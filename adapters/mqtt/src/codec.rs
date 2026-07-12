//! MQTT 3.1.1 wire codec (spec.txt §3.3, §6 Phase 2).
//!
//! [`decode_packet`] is the untrusted-input entry point: it parses a single
//! `&[u8]` frame and returns the number of bytes consumed, or [`None`] when the
//! buffer is a prefix of a larger frame (caller should read more bytes and
//! retry). [`encode_packet`] renders a [`Packet`] back to bytes for the wire.

use std::fmt;

/// Quality of service level for a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    pub fn from_u8(v: u8) -> Option<QoS> {
        match v {
            0 => Some(QoS::AtMostOnce),
            1 => Some(QoS::AtLeastOnce),
            2 => Some(QoS::ExactlyOnce),
            _ => None,
        }
    }
}

/// Why a packet could not be decoded. `Incomplete` is not an error: it signals
/// "read more bytes and try again".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Incomplete,
    Malformed(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Incomplete => f.write_str("incomplete packet"),
            ProtocolError::Malformed(r) => write!(f, "malformed packet: {r}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// MQTT 3.1.1 SUBACK return codes. `Failure` is `0x80`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubAckCode {
    Qos0,
    Qos1,
    Qos2,
    Failure,
}

impl SubAckCode {
    fn to_u8(self) -> u8 {
        match self {
            SubAckCode::Qos0 => 0,
            SubAckCode::Qos1 => 1,
            SubAckCode::Qos2 => 2,
            SubAckCode::Failure => 0x80,
        }
    }

    fn from_u8(v: u8) -> SubAckCode {
        match v {
            0 => SubAckCode::Qos0,
            1 => SubAckCode::Qos1,
            2 => SubAckCode::Qos2,
            _ => SubAckCode::Failure,
        }
    }
}

/// A decoded MQTT control packet.
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Connect(Connect),
    ConnAck { session_present: bool, code: u8 },
    Publish(Publish),
    PubAck { packet_id: u16 },
    PubRec { packet_id: u16 },
    PubRel { packet_id: u16 },
    PubComp { packet_id: u16 },
    Subscribe { packet_id: u16, topics: Vec<(String, QoS)> },
    SubAck { packet_id: u16, codes: Vec<SubAckCode> },
    Unsubscribe { packet_id: u16, topics: Vec<String> },
    UnsubAck { packet_id: u16 },
    PingReq,
    PingResp,
    Disconnect,
}

/// CONNECT variable header + payload (3.1.1).
#[derive(Debug, Clone, PartialEq)]
pub struct Connect {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<Will>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

/// PUBLISH payload + metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

impl Publish {
    /// Re-publish this message to a downstream subscriber, assigning a fresh
    /// `packet_id` (for QoS > 0) and clearing the DUP flag.
    pub fn to_delivery(&self, packet_id: Option<u16>) -> Publish {
        Publish {
            dup: false,
            qos: self.qos,
            retain: false,
            topic: self.topic.clone(),
            packet_id,
            payload: self.payload.clone(),
        }
    }
}

// --- low-level readers/writers -------------------------------------------

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn u8(&mut self) -> Result<u8, ProtocolError> {
        if self.remaining() < 1 {
            return Err(ProtocolError::Incomplete);
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn u16(&mut self) -> Result<u16, ProtocolError> {
        if self.remaining() < 2 {
            return Err(ProtocolError::Incomplete);
        }
        let v = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        if self.remaining() < n {
            return Err(ProtocolError::Incomplete);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn string(&mut self) -> Result<String, ProtocolError> {
        let len = self.u16()? as usize;
        let raw = self.bytes(len)?;
        std::str::from_utf8(raw)
            .map(|s| s.to_string())
            .map_err(|_| ProtocolError::Malformed("invalid utf8 string"))
    }

    /// Decode a variable-byte integer; returns the value and bytes consumed.
    fn varint(&mut self) -> Result<(u32, usize), ProtocolError> {
        let mut multiplier = 1u32;
        let mut value = 0u32;
        let mut consumed = 0;
        loop {
            let b = self.u8()?;
            consumed += 1;
            value += ((b & 0x7F) as u32) * multiplier;
            if b & 0x80 == 0 {
                break;
            }
            multiplier = multiplier.checked_mul(128).ok_or(ProtocolError::Malformed("varint overflow"))?;
            if consumed >= 4 {
                return Err(ProtocolError::Malformed("varint too long"));
            }
        }
        Ok((value, consumed))
    }
}

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Writer { buf: Vec::new() }
    }

    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    fn string(&mut self, s: &str) {
        let b = s.as_bytes();
        self.u16(b.len() as u16);
        self.buf.extend_from_slice(b);
    }

    fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    fn varint(&mut self, mut v: u32) {
        loop {
            let mut b = (v & 0x7F) as u8;
            v >>= 7;
            if v > 0 {
                b |= 0x80;
            }
            self.buf.push(b);
            if v == 0 {
                break;
            }
        }
    }
}

const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const PUBREC: u8 = 5;
const PUBREL: u8 = 6;
const PUBCOMP: u8 = 7;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const UNSUBSCRIBE: u8 = 10;
const UNSUBACK: u8 = 11;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

/// Parse one MQTT packet from the front of `buf`.
///
/// * `Ok(Some((packet, n)))` — a complete packet spanning `n` bytes.
/// * `Ok(None)` — `buf` is a prefix; read more and retry.
/// * `Err(_)` — malformed, must close the connection.
pub fn decode_packet(buf: &[u8]) -> Result<Option<(Packet, usize)>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let mut r = Reader::new(buf);
    let first = r.u8()?;
    let packet_type = first >> 4;
    let flags = first & 0x0F;
    // Reading the remaining-length varint may run past the end of a partial
    // frame; treat that as "incomplete", not malformed.
    let (remaining_len, varint_len) = match r.varint() {
        Ok(v) => v,
        Err(ProtocolError::Incomplete) => return Ok(None),
        Err(e) => return Err(e),
    };
    let header_len = 1 + varint_len;
    let total = header_len + remaining_len as usize;
    if buf.len() < total {
        return Ok(None); // incomplete: not enough bytes yet
    }
    let body = &buf[header_len..total];

    let packet = match packet_type {
        CONNECT => decode_connect(body, flags)?,
        CONNACK => decode_connack(body)?,
        PUBLISH => decode_publish(body, flags)?,
        PUBACK => Packet::PubAck { packet_id: parse_id(body)? },
        PUBREC => Packet::PubRec { packet_id: parse_id(body)? },
        PUBREL => {
            if flags != 0x02 {
                return Err(ProtocolError::Malformed("PUBREL must use flags 0x02"));
            }
            Packet::PubRel { packet_id: parse_id(body)? }
        }
        PUBCOMP => Packet::PubComp { packet_id: parse_id(body)? },
        SUBSCRIBE => {
            if flags != 0x02 {
                return Err(ProtocolError::Malformed("SUBSCRIBE must use flags 0x02"));
            }
            decode_subscribe(body)?
        }
        SUBACK => decode_suback(body)?,
        UNSUBSCRIBE => {
            if flags != 0x02 {
                return Err(ProtocolError::Malformed("UNSUBSCRIBE must use flags 0x02"));
            }
            decode_unsubscribe(body)?
        }
        UNSUBACK => Packet::UnsubAck { packet_id: parse_id(body)? },
        PINGREQ => Packet::PingReq,
        PINGRESP => Packet::PingResp,
        DISCONNECT => Packet::Disconnect,
        _ => return Err(ProtocolError::Malformed("unknown packet type")),
    };

    Ok(Some((packet, total)))
}

fn parse_id(body: &[u8]) -> Result<u16, ProtocolError> {
    if body.len() < 2 {
        return Err(ProtocolError::Malformed("expected packet id"));
    }
    Ok(u16::from_be_bytes([body[0], body[1]]))
}

fn decode_connect(body: &[u8], _flags: u8) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let proto = r.string()?;
    if proto != "MQTT" {
        return Err(ProtocolError::Malformed("bad protocol name"));
    }
    let level = r.u8()?;
    if level != 4 {
        return Err(ProtocolError::Malformed("only MQTT 3.1.1 (level 4) supported"));
    }
    let connect_flags = r.u8()?;
    let keep_alive = r.u16()?;
    let client_id = r.string()?;

    let clean_session = connect_flags & 0x02 != 0;
    let will_flag = connect_flags & 0x04 != 0;
    let will_retain = connect_flags & 0x20 != 0;
    let will_qos = QoS::from_u8((connect_flags >> 3) & 0x03)
        .ok_or(ProtocolError::Malformed("bad will qos"))?;
    let username_flag = connect_flags & 0x80 != 0;
    let password_flag = connect_flags & 0x40 != 0;

    let will = if will_flag {
        let topic = r.string()?;
        let payload = r.bytes(r.remaining())?.to_vec();
        Some(Will { topic, payload, qos: will_qos, retain: will_retain })
    } else {
        None
    };
    let username = if username_flag {
        Some(r.string()?)
    } else {
        None
    };
    let password = if password_flag {
        let len = r.u16()?;
        Some(r.bytes(len as usize)?.to_vec())
    } else {
        None
    };

    Ok(Packet::Connect(Connect {
        client_id,
        keep_alive,
        clean_session,
        will,
        username,
        password,
    }))
}

fn decode_connack(body: &[u8]) -> Result<Packet, ProtocolError> {
    if body.len() < 2 {
        return Err(ProtocolError::Malformed("connack too short"));
    }
    Ok(Packet::ConnAck {
        session_present: body[0] & 0x01 != 0,
        code: body[1],
    })
}

fn decode_publish(body: &[u8], flags: u8) -> Result<Packet, ProtocolError> {
    let dup = flags & 0x08 != 0;
    let qos = QoS::from_u8((flags >> 1) & 0x03)
        .ok_or(ProtocolError::Malformed("bad publish qos"))?;
    let retain = flags & 0x01 != 0;

    let mut r = Reader::new(body);
    let topic = r.string()?;
    if topic.is_empty() {
        return Err(ProtocolError::Malformed("empty publish topic"));
    }
    let packet_id = if qos != QoS::AtMostOnce {
        Some(r.u16()?)
    } else {
        None
    };
    let payload = r.bytes(r.remaining())?.to_vec();
    Ok(Packet::Publish(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
    }))
}

fn decode_subscribe(body: &[u8]) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    let mut topics = Vec::new();
    while r.remaining() >= 3 {
        let filter = r.string()?;
        let qos = QoS::from_u8(r.u8()? & 0x03)
            .ok_or(ProtocolError::Malformed("bad subscribe qos"))?;
        topics.push((filter, qos));
    }
    if topics.is_empty() {
        return Err(ProtocolError::Malformed("subscribe has no topics"));
    }
    Ok(Packet::Subscribe { packet_id, topics })
}

fn decode_suback(body: &[u8]) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    let mut codes = Vec::new();
    while r.remaining() > 0 {
        codes.push(SubAckCode::from_u8(r.u8()?));
    }
    Ok(Packet::SubAck { packet_id, codes })
}

fn decode_unsubscribe(body: &[u8]) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    let mut topics = Vec::new();
    while r.remaining() >= 2 {
        topics.push(r.string()?);
    }
    Ok(Packet::Unsubscribe { packet_id, topics })
}

/// Encode `packet` into `out`.
pub fn encode_packet(packet: &Packet, out: &mut Vec<u8>) {
    match packet {
        Packet::Connect(c) => encode_connect(c, out),
        Packet::ConnAck { session_present, code } => {
            let mut w = Writer::new();
            w.u8(if *session_present { 0x01 } else { 0x00 });
            w.u8(*code);
            fixed(CONNACK, 0, &w.buf, out);
        }
        Packet::Publish(p) => encode_publish(p, out),
        Packet::PubAck { packet_id } => fixed_id(PUBACK, packet_id, out),
        Packet::PubRec { packet_id } => fixed_id(PUBREC, packet_id, out),
        Packet::PubRel { packet_id } => fixed_id_flags(PUBREL, packet_id, out),
        Packet::PubComp { packet_id } => fixed_id(PUBCOMP, packet_id, out),
        Packet::Subscribe { packet_id, topics } => {
            let mut w = Writer::new();
            w.u16(*packet_id);
            for (f, q) in topics {
                w.string(f);
                w.u8(*q as u8);
            }
            fixed(SUBSCRIBE, 0x02, &w.buf, out);
        }
        Packet::SubAck { packet_id, codes } => {
            let mut w = Writer::new();
            w.u16(*packet_id);
            for c in codes {
                w.u8(c.to_u8());
            }
            fixed(SUBACK, 0, &w.buf, out);
        }
        Packet::Unsubscribe { packet_id, topics } => {
            let mut w = Writer::new();
            w.u16(*packet_id);
            for t in topics {
                w.string(t);
            }
            fixed(UNSUBSCRIBE, 0x02, &w.buf, out);
        }
        Packet::UnsubAck { packet_id } => fixed_id(UNSUBACK, packet_id, out),
        Packet::PingReq => fixed(PINGREQ, 0, &[], out),
        Packet::PingResp => fixed(PINGRESP, 0, &[], out),
        Packet::Disconnect => fixed(DISCONNECT, 0, &[], out),
    }
}

fn encode_connect(c: &Connect, out: &mut Vec<u8>) {
    let mut w = Writer::new();
    w.string("MQTT");
    w.u8(4); // level 3.1.1
    let mut cf = 0u8;
    if c.clean_session {
        cf |= 0x02;
    }
    if c.will.is_some() {
        cf |= 0x04;
    }
    if c.password.is_some() {
        cf |= 0x40;
    }
    if c.username.is_some() {
        cf |= 0x80;
    }
    if let Some(will) = &c.will {
        if will.retain {
            cf |= 0x20;
        }
        cf |= (will.qos as u8) << 3;
    }
    w.u8(cf);
    w.u16(c.keep_alive);
    w.string(&c.client_id);
    if let Some(will) = &c.will {
        w.string(&will.topic);
        w.string_bytes(&will.payload);
    }
    if let Some(u) = &c.username {
        w.string(u);
    }
    if let Some(p) = &c.password {
        w.u16(p.len() as u16);
        w.bytes(p);
    }
    fixed(CONNECT, 0, &w.buf, out);
}

fn encode_publish(p: &Publish, out: &mut Vec<u8>) {
    let mut w = Writer::new();
    w.string(&p.topic);
    if p.qos != QoS::AtMostOnce {
        w.u16(p.packet_id.unwrap_or(0));
    }
    w.bytes(&p.payload);
    let mut flags = (p.qos as u8) << 1;
    if p.dup {
        flags |= 0x08;
    }
    if p.retain {
        flags |= 0x01;
    }
    fixed(PUBLISH, flags, &w.buf, out);
}

fn fixed(typ: u8, flags: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push((typ << 4) | flags);
    let mut w = Writer::new();
    w.varint(body.len() as u32);
    out.extend_from_slice(&w.buf);
    out.extend_from_slice(body);
}

fn fixed_id(typ: u8, id: &u16, out: &mut Vec<u8>) {
    let mut w = Writer::new();
    w.u16(*id);
    fixed(typ, 0, &w.buf, out);
}

fn fixed_id_flags(typ: u8, id: &u16, out: &mut Vec<u8>) {
    let mut w = Writer::new();
    w.u16(*id);
    fixed(typ, 0x02, &w.buf, out);
}

impl Writer {
    fn string_bytes(&mut self, b: &[u8]) {
        self.u16(b.len() as u16);
        self.buf.extend_from_slice(b);
    }
}

/// Convenience entry point used by the fuzz targets: decode a single packet
/// from untrusted bytes. Never panics on malformed input.
pub fn parse(input: &[u8]) -> Result<Option<(Packet, usize)>, ProtocolError> {
    decode_packet(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(p: Packet) {
        let mut buf = Vec::new();
        encode_packet(&p, &mut buf);
        let (decoded, n) = decode_packet(&buf).unwrap().unwrap();
        assert_eq!(decoded, p);
        assert_eq!(n, buf.len());
    }

    #[test]
    fn connect_roundtrip() {
        roundtrip(Packet::Connect(Connect {
            client_id: "client-1".into(),
            keep_alive: 60,
            clean_session: true,
            will: None,
            username: Some("u".into()),
            password: Some(b"p".to_vec()),
        }));
    }

    #[test]
    fn publish_qos1_roundtrip() {
        roundtrip(Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: "a/b/c".into(),
            packet_id: Some(42),
            payload: b"hello".to_vec(),
        }));
    }

    #[test]
    fn subscribe_suback_roundtrip() {
        roundtrip(Packet::Subscribe {
            packet_id: 7,
            topics: vec![("sensors/#".into(), QoS::AtMostOnce), ("x/+".into(), QoS::ExactlyOnce)],
        });
        roundtrip(Packet::SubAck {
            packet_id: 7,
            codes: vec![SubAckCode::Qos0, SubAckCode::Failure],
        });
    }

    #[test]
    fn incomplete_returns_none() {
        let mut buf = Vec::new();
        encode_packet(&Packet::PingReq, &mut buf);
        let partial = &buf[..buf.len() - 1];
        assert_eq!(decode_packet(partial).unwrap(), None);
    }

    #[test]
    fn varint_roundtrip() {
        for v in [0u32, 127, 128, 16383, 16384, 268435455] {
            let mut w = Writer::new();
            w.varint(v);
            let mut r = Reader::new(&w.buf);
            let (got, _) = r.varint().unwrap();
            assert_eq!(got, v);
        }
    }
}
