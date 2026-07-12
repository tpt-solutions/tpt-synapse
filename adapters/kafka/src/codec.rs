//! Kafka wire protocol codec (spec.txt §3.3, §6 Phase 3).
//!
//! [`decode_request`] is the untrusted-input entry point: it parses one
//! length-prefixed Kafka request from the front of `&[u8]`, returning
//! `Ok(None)` when more bytes are needed or `Err` on malformed input. The
//! per-API request-body parsers and response encoders implement the subset of
//! the Kafka protocol the adapter speaks — produce/fetch/log-offsets/metadata
//! plus consumer-group coordination (FindCoordinator, OffsetCommit/Fetch,
//! Join/Sync/Heartbeat/LeaveGroup) — so existing Kafka clients can talk to the
//! core [`Log`] primitive and [`StreamRouter`] with no migration.
//!
//! Header versions: v0 has no `client_id`; v1 adds it; v2+ add a compact
//! tagged-field section that we skip rather than reject. We advertise v0/v1 in
//! `ApiVersions` so well-behaved clients negotiate a version we parse.

use std::fmt;

const MAX_FRAME: usize = 256 * 1024 * 1024;

/// Error code returned by Kafka responses (`0` = none).
pub const ERR_NONE: i16 = 0;
pub const ERR_UNKNOWN: i16 = -1;
pub const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;

/// Why a frame could not be decoded. `Incomplete` is not an error: it signals
/// "read more bytes and try again".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Incomplete,
    Malformed(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Incomplete => f.write_str("incomplete frame"),
            ProtocolError::Malformed(r) => write!(f, "malformed kafka frame: {r}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Kafka API keys the adapter implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    OffsetCommit,
    OffsetFetch,
    FindCoordinator,
    JoinGroup,
    Heartbeat,
    LeaveGroup,
    SyncGroup,
    ApiVersions,
    Unknown(i16),
}

impl ApiKey {
    pub fn from_i16(v: i16) -> ApiKey {
        match v {
            0 => ApiKey::Produce,
            1 => ApiKey::Fetch,
            2 => ApiKey::ListOffsets,
            3 => ApiKey::Metadata,
            8 => ApiKey::OffsetCommit,
            9 => ApiKey::OffsetFetch,
            10 => ApiKey::FindCoordinator,
            11 => ApiKey::JoinGroup,
            12 => ApiKey::Heartbeat,
            13 => ApiKey::LeaveGroup,
            14 => ApiKey::SyncGroup,
            18 => ApiKey::ApiVersions,
            other => ApiKey::Unknown(other),
        }
    }

    pub fn as_i16(self) -> i16 {
        match self {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ListOffsets => 2,
            ApiKey::Metadata => 3,
            ApiKey::OffsetCommit => 8,
            ApiKey::OffsetFetch => 9,
            ApiKey::FindCoordinator => 10,
            ApiKey::JoinGroup => 11,
            ApiKey::Heartbeat => 12,
            ApiKey::LeaveGroup => 13,
            ApiKey::SyncGroup => 14,
            ApiKey::ApiVersions => 18,
            ApiKey::Unknown(v) => v,
        }
    }
}

/// A fully decoded Kafka request frame (header + body slice).
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub body: Vec<u8>,
}

// --- low-level readers/writers -------------------------------------------

pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn u8(&mut self) -> Result<u8, ProtocolError> {
        if self.remaining() < 1 {
            return Err(ProtocolError::Incomplete);
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn i8(&mut self) -> Result<i8, ProtocolError> {
        Ok(self.u8()? as i8)
    }

    fn i16(&mut self) -> Result<i16, ProtocolError> {
        if self.remaining() < 2 {
            return Err(ProtocolError::Incomplete);
        }
        let v = i16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn i32(&mut self) -> Result<i32, ProtocolError> {
        if self.remaining() < 4 {
            return Err(ProtocolError::Incomplete);
        }
        let v = i32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn i64(&mut self) -> Result<i64, ProtocolError> {
        if self.remaining() < 8 {
            return Err(ProtocolError::Incomplete);
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(i64::from_be_bytes(b))
    }

    /// Decode a Kafka string (int16 length, -1 => null).
    fn str_opt(&mut self) -> Result<Option<String>, ProtocolError> {
        let len = self.i16()?;
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        if self.remaining() < len {
            return Err(ProtocolError::Incomplete);
        }
        let raw = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        std::str::from_utf8(raw)
            .map(|s| Some(s.to_string()))
            .map_err(|_| ProtocolError::Malformed("invalid utf8 string"))
    }

    /// Decode a non-null Kafka string (errors if length is -1).
    fn str_nonnull(&mut self) -> Result<String, ProtocolError> {
        self.str_opt()?
            .ok_or(ProtocolError::Malformed("expected non-null string"))
    }

    /// Decode a Kafka bytes field (int32 length, -1 => null).
    fn bytes_opt(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        if self.remaining() < len {
            return Err(ProtocolError::Incomplete);
        }
        let raw = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(Some(raw))
    }

    /// Decode an array header. Returns `None` for the `-1` (null) sentinel,
    /// otherwise the element count.
    fn array_len_opt(&mut self) -> Result<Option<usize>, ProtocolError> {
        let len = self.i32()?;
        if len < 0 {
            Ok(None)
        } else {
            Ok(Some(len as usize))
        }
    }

    fn array_len(&mut self) -> Result<usize, ProtocolError> {
        Ok(self.array_len_opt()?.unwrap_or(0))
    }

    fn skip_tags(&mut self) -> Result<(), ProtocolError> {
        let count = self.uvarint()?;
        for _ in 0..count {
            self.uvarint()?; // tag id
            let size = self.uvarint()? as usize;
            if self.remaining() < size {
                return Err(ProtocolError::Incomplete);
            }
            self.pos += size;
        }
        Ok(())
    }

    fn uvarint(&mut self) -> Result<u64, ProtocolError> {
        let mut shift = 0u32;
        let mut result = 0u64;
        loop {
            let b = self.u8()?;
            result |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift = shift
                .checked_add(7)
                .ok_or(ProtocolError::Malformed("uvarint overflow"))?;
            if shift >= 64 {
                return Err(ProtocolError::Malformed("uvarint overflow"));
            }
        }
        Ok(result)
    }

    fn remaining_bytes(&self) -> Vec<u8> {
        self.buf[self.pos..].to_vec()
    }
}

pub(crate) struct Writer {
    pub(crate) buf: Vec<u8>,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Writer { buf: Vec::new() }
    }

    #[allow(dead_code)]
    pub(crate) fn i8(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    pub(crate) fn i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub(crate) fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub(crate) fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub(crate) fn bool(&mut self, b: bool) {
        self.buf.push(if b { 1 } else { 0 });
    }

    pub(crate) fn str_opt(&mut self, s: Option<&str>) {
        match s {
            None => self.i16(-1),
            Some(s) => {
                let b = s.as_bytes();
                self.i16(b.len() as i16);
                self.buf.extend_from_slice(b);
            }
        }
    }

    pub(crate) fn bytes_opt(&mut self, b: Option<&[u8]>) {
        match b {
            None => self.i32(-1),
            Some(b) => {
                self.i32(b.len() as i32);
                self.buf.extend_from_slice(b);
            }
        }
    }

    /// Write an array header. We never emit the `-1` null sentinel.
    pub(crate) fn array_len(&mut self, n: usize) {
        self.i32(n as i32);
    }
}

// --- request decoding ----------------------------------------------------

/// Parse one Kafka request frame from the front of `buf`.
///
/// * `Ok(Some((frame, n)))` — a complete frame spanning `n` bytes.
/// * `Ok(None)` — `buf` is a prefix; read more and retry.
/// * `Err(_)` — malformed, must close the connection.
pub fn decode_request(buf: &[u8]) -> Result<Option<(Frame, usize)>, ProtocolError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if size < 0 {
        return Err(ProtocolError::Malformed("negative frame size"));
    }
    let size = size as usize;
    if size > MAX_FRAME {
        return Err(ProtocolError::Malformed("frame too large"));
    }
    if buf.len() < 4 + size {
        return Ok(None);
    }
    let mut r = Reader::new(&buf[4..4 + size]);
    let api_raw = r.i16()?;
    let api_key = ApiKey::from_i16(api_raw);
    let api_version = r.i16()?;
    let correlation_id = r.i32()?;
    let client_id = if api_version >= 1 {
        r.str_opt()?
    } else {
        None
    };
    if api_version >= 2 {
        r.skip_tags()?;
    }
    let body = r.remaining_bytes();
    Ok(Some((
        Frame {
            api_key,
            api_version,
            correlation_id,
            client_id,
            body,
        },
        4 + size,
    )))
}

/// Convenience entry point used by the fuzz targets: decode one request frame
/// from untrusted bytes. Never panics on malformed input.
pub fn parse(input: &[u8]) -> Result<Option<(Frame, usize)>, ProtocolError> {
    decode_request(input)
}

// --- typed request bodies ------------------------------------------------

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<(String, Vec<(i32, Vec<u8>)>)>,
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub topics: Vec<(String, Vec<(i32, i64, i32)>)>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub topics: Vec<(String, Vec<(i32, i64)>)>,
}

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub topics: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub topics: Vec<(String, Vec<(i32, i64, String)>)>,
}

#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    pub topics: Vec<(String, Vec<i32>)>,
}

#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    pub key: String,
    pub key_type: i8,
}

#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub member_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: Option<String>,
    pub assignment: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct MemberRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: Option<String>,
}

pub fn parse_produce(body: &[u8]) -> Result<ProduceRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let acks = r.i16()?;
    let timeout_ms = r.i32()?;
    let mut topics = Vec::new();
    if let Some(nt) = r.array_len_opt()? {
        for _ in 0..nt {
            let topic = r.str_nonnull()?;
            let mut parts = Vec::new();
            let np = r.array_len()?;
            for _ in 0..np {
                let partition = r.i32()?;
                let data = r.bytes_opt()?.unwrap_or_default();
                parts.push((partition, data));
            }
            topics.push((topic, parts));
        }
    }
    Ok(ProduceRequest {
        acks,
        timeout_ms,
        topics,
    })
}

pub fn parse_fetch(body: &[u8]) -> Result<FetchRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let replica_id = r.i32()?;
    let max_wait_ms = r.i32()?;
    let min_bytes = r.i32()?;
    let mut topics = Vec::new();
    if let Some(nt) = r.array_len_opt()? {
        for _ in 0..nt {
            let topic = r.str_nonnull()?;
            let mut parts = Vec::new();
            let np = r.array_len()?;
            for _ in 0..np {
                let partition = r.i32()?;
                let fetch_offset = r.i64()?;
                let max_bytes = r.i32()?;
                parts.push((partition, fetch_offset, max_bytes));
            }
            topics.push((topic, parts));
        }
    }
    Ok(FetchRequest {
        replica_id,
        max_wait_ms,
        min_bytes,
        topics,
    })
}

pub fn parse_list_offsets(body: &[u8]) -> Result<ListOffsetsRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let replica_id = r.i32()?;
    let mut topics = Vec::new();
    if let Some(nt) = r.array_len_opt()? {
        for _ in 0..nt {
            let topic = r.str_nonnull()?;
            let mut parts = Vec::new();
            let np = r.array_len()?;
            for _ in 0..np {
                let partition = r.i32()?;
                let timestamp = r.i64()?;
                parts.push((partition, timestamp));
            }
            topics.push((topic, parts));
        }
    }
    Ok(ListOffsetsRequest { replica_id, topics })
}

pub fn parse_metadata(body: &[u8]) -> Result<MetadataRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let topics = match r.array_len_opt()? {
        None => None,
        Some(n) => {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push(r.str_nonnull()?);
            }
            Some(v)
        }
    };
    Ok(MetadataRequest { topics })
}

pub fn parse_offset_commit(body: &[u8]) -> Result<OffsetCommitRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let group_id = r.str_nonnull()?;
    let mut topics = Vec::new();
    if let Some(nt) = r.array_len_opt()? {
        for _ in 0..nt {
            let topic = r.str_nonnull()?;
            let mut parts = Vec::new();
            let np = r.array_len()?;
            for _ in 0..np {
                let partition = r.i32()?;
                let offset = r.i64()?;
                let metadata = r.str_opt()?.unwrap_or_default();
                parts.push((partition, offset, metadata));
            }
            topics.push((topic, parts));
        }
    }
    Ok(OffsetCommitRequest { group_id, topics })
}

pub fn parse_offset_fetch(body: &[u8]) -> Result<OffsetFetchRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let group_id = r.str_nonnull()?;
    let mut topics = Vec::new();
    if let Some(nt) = r.array_len_opt()? {
        for _ in 0..nt {
            let topic = r.str_nonnull()?;
            let mut parts = Vec::new();
            let np = r.array_len()?;
            for _ in 0..np {
                parts.push(r.i32()?);
            }
            topics.push((topic, parts));
        }
    }
    Ok(OffsetFetchRequest { group_id, topics })
}

pub fn parse_find_coordinator(body: &[u8]) -> Result<FindCoordinatorRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let key = r.str_nonnull()?;
    let key_type = r.i8()?;
    Ok(FindCoordinatorRequest { key, key_type })
}

pub fn parse_join_group(body: &[u8]) -> Result<JoinGroupRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let group_id = r.str_nonnull()?;
    let session_timeout_ms = r.i32()?;
    let member_id = r.str_opt()?;
    let protocol_type = r.str_nonnull()?;
    let mut protocols = Vec::new();
    if let Some(np) = r.array_len_opt()? {
        for _ in 0..np {
            let name = r.str_nonnull()?;
            let meta = r.bytes_opt()?.unwrap_or_default();
            protocols.push((name, meta));
        }
    }
    Ok(JoinGroupRequest {
        group_id,
        session_timeout_ms,
        member_id,
        protocol_type,
        protocols,
    })
}

pub fn parse_sync_group(body: &[u8]) -> Result<SyncGroupRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let group_id = r.str_nonnull()?;
    let generation_id = r.i32()?;
    let member_id = r.str_opt()?;
    let mut assignment = Vec::new();
    if let Some(na) = r.array_len_opt()? {
        for _ in 0..na {
            let member_id = r.str_nonnull()?;
            let data = r.bytes_opt()?.unwrap_or_default();
            assignment.push((member_id, data));
        }
    }
    Ok(SyncGroupRequest {
        group_id,
        generation_id,
        member_id,
        assignment,
    })
}

pub fn parse_member_request(body: &[u8]) -> Result<MemberRequest, ProtocolError> {
    let mut r = Reader::new(body);
    let group_id = r.str_nonnull()?;
    let generation_id = r.i32()?;
    let member_id = r.str_opt()?;
    Ok(MemberRequest {
        group_id,
        generation_id,
        member_id,
    })
}

// --- response encoding ---------------------------------------------------

pub fn encode_api_versions() -> Vec<u8> {
    let versions: &[(i16, i16, i16)] = &[
        (0, 0, 1),  // Produce
        (1, 0, 1),  // Fetch
        (2, 0, 1),  // ListOffsets
        (3, 0, 1),  // Metadata
        (8, 0, 1),  // OffsetCommit
        (9, 0, 1),  // OffsetFetch
        (10, 0, 1), // FindCoordinator
        (11, 0, 1), // JoinGroup
        (12, 0, 1), // Heartbeat
        (13, 0, 1), // LeaveGroup
        (14, 0, 1), // SyncGroup
        (18, 0, 1), // ApiVersions
    ];
    let mut w = Writer::new();
    w.i16(ERR_NONE);
    w.array_len(versions.len());
    for (key, min, max) in versions {
        w.i16(*key);
        w.i16(*min);
        w.i16(*max);
    }
    w.buf
}

pub fn encode_metadata(broker: &str, port: i32, topics: &[(String, u32)]) -> Vec<u8> {
    let mut w = Writer::new();
    w.array_len(1); // one broker
    w.i32(0); // node id
    w.str_opt(Some(broker));
    w.i32(port);
    w.array_len(topics.len());
    for (name, partitions) in topics {
        w.i16(ERR_NONE);
        w.str_opt(Some(name));
        w.bool(false); // is_internal
        w.array_len(*partitions as usize);
        for p in 0..*partitions {
            w.i16(ERR_NONE);
            w.i32(p as i32);
            w.i32(0); // leader
            w.array_len(1);
            w.i32(0); // replica
            w.array_len(1);
            w.i32(0); // isr
        }
    }
    w.buf
}

pub fn encode_produce(topics: &[(String, Vec<(i32, i16, i64)>)]) -> Vec<u8> {
    let mut w = Writer::new();
    w.array_len(topics.len());
    for (name, parts) in topics {
        w.str_opt(Some(name));
        w.array_len(parts.len());
        for (partition, error, offset) in parts {
            w.i32(*partition);
            w.i16(*error);
            w.i64(*offset);
        }
    }
    w.buf
}

pub fn encode_fetch(topics: &[(String, Vec<(i32, i16, i64, Vec<u8>)>)]) -> Vec<u8> {
    let mut w = Writer::new();
    w.array_len(topics.len());
    for (name, parts) in topics {
        w.str_opt(Some(name));
        w.array_len(parts.len());
        for (partition, error, hw, data) in parts {
            w.i32(*partition);
            w.i16(*error);
            w.i64(*hw);
            w.bytes_opt(Some(data));
        }
    }
    w.buf
}

pub fn encode_list_offsets(topics: &[(String, Vec<(i32, i16, i64)>)]) -> Vec<u8> {
    let mut w = Writer::new();
    w.array_len(topics.len());
    for (name, parts) in topics {
        w.str_opt(Some(name));
        w.array_len(parts.len());
        for (partition, error, offset) in parts {
            w.i32(*partition);
            w.i64(*offset);
            w.i16(*error);
        }
    }
    w.buf
}

pub fn encode_find_coordinator(host: &str, port: i32) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(ERR_NONE);
    w.i32(0); // node id
    w.str_opt(Some(host));
    w.i32(port);
    w.buf
}

pub fn encode_offset_commit(topics: &[(String, Vec<(i32, i16)>)]) -> Vec<u8> {
    let mut w = Writer::new();
    w.array_len(topics.len());
    for (name, parts) in topics {
        w.str_opt(Some(name));
        w.array_len(parts.len());
        for (partition, error) in parts {
            w.i32(*partition);
            w.i16(*error);
        }
    }
    w.buf
}

pub fn encode_offset_fetch(topics: &[(String, Vec<(i32, i64, String, i16)>)]) -> Vec<u8> {
    let mut w = Writer::new();
    w.array_len(topics.len());
    for (name, parts) in topics {
        w.str_opt(Some(name));
        w.array_len(parts.len());
        for (partition, offset, metadata, error) in parts {
            w.i32(*partition);
            w.i64(*offset);
            w.str_opt(Some(metadata.as_str()));
            w.i16(*error);
        }
    }
    w.buf
}

pub fn encode_join_group(
    generation_id: i32,
    protocol: &str,
    leader_id: &str,
    member_id: &str,
    members: &[(String, Vec<u8>)],
) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(ERR_NONE);
    w.i32(generation_id);
    w.str_opt(Some(protocol));
    w.str_opt(Some(leader_id));
    w.str_opt(Some(member_id));
    w.array_len(members.len());
    for (id, meta) in members {
        w.str_opt(Some(id));
        w.bytes_opt(Some(meta));
    }
    w.buf
}

pub fn encode_sync_group(assignment: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(ERR_NONE);
    w.bytes_opt(Some(assignment));
    w.buf
}

pub fn encode_error_only(error: i16) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(error);
    w.buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(api: ApiKey, body: &[u8]) {
        let mut buf = Vec::new();
        let header_len = 2 + 2 + 4 + 2 + 1; // api + version + correlation + client_id len + "x"
        buf.extend_from_slice(&((header_len + body.len()) as i32).to_be_bytes());
        buf.extend_from_slice(&api.as_i16().to_be_bytes());
        buf.extend_from_slice(&1i16.to_be_bytes()); // version
        buf.extend_from_slice(&7i32.to_be_bytes()); // correlation
        buf.extend_from_slice(&1i16.to_be_bytes()); // client_id len
        buf.extend_from_slice(b"x");
        buf.extend_from_slice(body);

        let (frame, n) = decode_request(&buf).unwrap().unwrap();
        assert_eq!(frame.api_key, api);
        assert_eq!(frame.correlation_id, 7);
        assert_eq!(frame.client_id.as_deref(), Some("x"));
        assert_eq!(n, buf.len());
    }

    #[test]
    fn decode_produce_request() {
        let mut b = Vec::new();
        let mut w = Writer::new();
        w.i16(1); // acks
        w.i32(1000); // timeout
        w.array_len(1);
        w.str_opt(Some("t"));
        w.array_len(1);
        w.i32(0);
        w.bytes_opt(Some(b"hello"));
        b.extend_from_slice(&w.buf);
        roundtrip_request(ApiKey::Produce, &b);
        let req = parse_produce(&b).unwrap();
        assert_eq!(req.topics[0].0, "t");
        assert_eq!(req.topics[0].1[0].1, b"hello");
    }

    #[test]
    fn incomplete_returns_none() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1000i32.to_be_bytes()); // claims 1000 bytes
        assert_eq!(decode_request(&buf).unwrap(), None);
    }

    #[test]
    fn api_versions_response_encodes() {
        let body = encode_api_versions();
        // error code + 12 api keys
        assert!(body.len() > 2 + 12 * 6);
    }
}
