//! AMQP 0-9-1 wire codec (spec.txt §3.3, §6 Phase 3 "Lite" adapter).
//!
//! [`decode_frame`] is the untrusted-input entry point: it parses one AMQP
//! frame (method / content-header / content-body / heartbeat) from the front
//! of `&[u8]`, returning `Ok(None)` when more bytes are needed or `Err` on
//! malformed input. The adapter speaks a deliberately narrowed subset of
//! AMQP — exchanges/bindings/queues, `basic.publish`/`consume`/`get`/`ack` —
//! so existing AMQP 0-9-1 clients (e.g. `pika`, `amqp`) can talk to the core
//! [`Queue`] primitive and [`GraphRouter`] with no migration. The out-of-process
//! `pika` conformance suite (`conformance/harness`) remains the canonical
//! end-goal; the in-repo TCP tests in `tests/integration.rs` stand in for it.
//!
//! [`parse`] is the untrusted-input entry point fuzzed by
//! `fuzz/fuzz_targets/parse.rs`.

use std::fmt;

// AMQP 0-9-1 protocol header the client sends to begin the handshake.
pub const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

// Frame types.
pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

// Class ids.
pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_EXCHANGE: u16 = 40;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;

// Connection method ids.
pub const METHOD_CONNECTION_START: u16 = 10;
pub const METHOD_CONNECTION_START_OK: u16 = 11;
pub const METHOD_CONNECTION_TUNE: u16 = 30;
pub const METHOD_CONNECTION_TUNE_OK: u16 = 31;
pub const METHOD_CONNECTION_OPEN: u16 = 40;
pub const METHOD_CONNECTION_OPEN_OK: u16 = 41;
pub const METHOD_CONNECTION_CLOSE: u16 = 50;
pub const METHOD_CONNECTION_CLOSE_OK: u16 = 51;

// Channel method ids.
pub const METHOD_CHANNEL_OPEN: u16 = 10;
pub const METHOD_CHANNEL_OPEN_OK: u16 = 11;
pub const METHOD_CHANNEL_CLOSE: u16 = 40;
pub const METHOD_CHANNEL_CLOSE_OK: u16 = 41;

// Exchange method ids.
pub const METHOD_EXCHANGE_DECLARE: u16 = 10;
pub const METHOD_EXCHANGE_DECLARE_OK: u16 = 11;

// Queue method ids.
pub const METHOD_QUEUE_DECLARE: u16 = 10;
pub const METHOD_QUEUE_DECLARE_OK: u16 = 11;
pub const METHOD_QUEUE_BIND: u16 = 20;
pub const METHOD_QUEUE_BIND_OK: u16 = 21;

// Basic method ids.
pub const METHOD_BASIC_QOS: u16 = 10;
pub const METHOD_BASIC_QOS_OK: u16 = 11;
pub const METHOD_BASIC_CONSUME: u16 = 20;
pub const METHOD_BASIC_CONSUME_OK: u16 = 21;
pub const METHOD_BASIC_PUBLISH: u16 = 40;
pub const METHOD_BASIC_DELIVER: u16 = 60;
pub const METHOD_BASIC_GET: u16 = 70;
pub const METHOD_BASIC_GET_OK: u16 = 71;
pub const METHOD_BASIC_GET_EMPTY: u16 = 72;
pub const METHOD_BASIC_ACK: u16 = 80;

// Soft AMQP error codes used in connection.close replies.
pub const REPLY_SUCCESS: u16 = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Incomplete,
    Malformed(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Incomplete => f.write_str("incomplete frame"),
            ProtocolError::Malformed(r) => write!(f, "malformed amqp frame: {r}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// A decoded AMQP frame. Method `args`, header `properties`, and body `data`
/// are kept raw; the broker parses them per-method.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Method {
        channel: u16,
        class: u16,
        method: u16,
        args: Vec<u8>,
    },
    Header {
        channel: u16,
        class: u16,
        weight: u16,
        body_size: u64,
        properties: Vec<u8>,
    },
    Body {
        channel: u16,
        data: Vec<u8>,
    },
    Heartbeat,
}

// --- low-level reader (AMQP type system + bit packing) -------------------

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    bit_byte: u8,
    bit_left: u8,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader {
            buf,
            pos: 0,
            bit_byte: 0,
            bit_left: 0,
        }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn flush_bits(&mut self) {
        self.bit_left = 0;
    }

    pub fn u8(&mut self) -> Result<u8, ProtocolError> {
        self.flush_bits();
        if self.remaining() < 1 {
            return Err(ProtocolError::Incomplete);
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn i8(&mut self) -> Result<i8, ProtocolError> {
        Ok(self.u8()? as i8)
    }

    pub fn u16(&mut self) -> Result<u16, ProtocolError> {
        self.flush_bits();
        if self.remaining() < 2 {
            return Err(ProtocolError::Incomplete);
        }
        let v = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn u32(&mut self) -> Result<u32, ProtocolError> {
        self.flush_bits();
        if self.remaining() < 4 {
            return Err(ProtocolError::Incomplete);
        }
        let mut b = [0u8; 4];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(u32::from_be_bytes(b))
    }

    pub fn u64(&mut self) -> Result<u64, ProtocolError> {
        self.flush_bits();
        if self.remaining() < 8 {
            return Err(ProtocolError::Incomplete);
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_be_bytes(b))
    }

    /// Read one packed boolean bit (LSB-first within the current octet),
    /// matching AMQP's bit-packing convention.
    pub fn bit(&mut self) -> Result<bool, ProtocolError> {
        if self.bit_left == 0 {
            self.bit_byte = self.u8()?;
            self.bit_left = 8;
        }
        let bit = self.bit_byte & 1;
        self.bit_byte >>= 1;
        self.bit_left -= 1;
        Ok(bit == 1)
    }

    /// Read an AMQP short-string (1-byte length, Latin-1/UTF-8 body).
    pub fn short_str(&mut self) -> Result<String, ProtocolError> {
        self.flush_bits();
        let len = self.u8()? as usize;
        if self.remaining() < len {
            return Err(ProtocolError::Incomplete);
        }
        let raw = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(String::from_utf8_lossy(raw).into_owned())
    }

    /// Read an AMQP long-string (4-byte length, body).
    pub fn long_str(&mut self) -> Result<String, ProtocolError> {
        self.flush_bits();
        let len = self.u32()? as usize;
        if self.remaining() < len {
            return Err(ProtocolError::Incomplete);
        }
        let raw = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(String::from_utf8_lossy(raw).into_owned())
    }

    /// Read an AMQP long-string as raw bytes.
    pub fn long_str_raw(&mut self) -> Result<Vec<u8>, ProtocolError> {
        self.flush_bits();
        let len = self.u32()? as usize;
        if self.remaining() < len {
            return Err(ProtocolError::Incomplete);
        }
        let raw = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(raw)
    }

    /// Read and discard a field table (4-byte length prefix).
    pub fn skip_table(&mut self) -> Result<(), ProtocolError> {
        self.flush_bits();
        let len = self.u32()? as usize;
        if self.remaining() < len {
            return Err(ProtocolError::Incomplete);
        }
        let mut table = Reader::new(&self.buf[self.pos..self.pos + len]);
        while table.remaining() > 0 {
            let _key = table.short_str()?;
            skip_field_value(&mut table)?;
        }
        self.pos += len;
        Ok(())
    }

    pub fn remaining_bytes(&self) -> Vec<u8> {
        self.buf[self.pos..].to_vec()
    }
}

/// Skip one AMQP field-table value given the type byte already consumed.
fn skip_field_value(r: &mut Reader<'_>) -> Result<(), ProtocolError> {
    let ty = r.u8()?;
    match ty as char {
        't' => {
            r.u8()?;
        }
        'b' | 'B' => {
            r.u8()?;
        }
        's' => {
            r.u16()?;
        }
        'I' => {
            r.u32()?;
        }
        'l' => {
            r.u64()?;
        }
        'f' => {
            r.u32()?;
        }
        'd' => {
            r.u64()?;
        }
        'D' => {
            r.u8()?;
            r.u32()?;
        }
        'S' => {
            r.short_str()?;
        }
        'x' => {
            let len = r.u32()? as usize;
            if r.remaining() < len {
                return Err(ProtocolError::Incomplete);
            }
            r.pos += len;
        }
        'L' => {
            r.long_str()?;
        }
        'T' => {
            r.u64()?;
        }
        'F' => {
            r.skip_table()?;
        }
        'A' => {
            let count = r.u32()? as usize;
            for _ in 0..count {
                skip_field_value(r)?;
            }
        }
        'V' => {}
        _ => return Err(ProtocolError::Malformed("unknown field-table type")),
    }
    Ok(())
}

// --- low-level writer ----------------------------------------------------

pub struct Writer {
    pub(crate) buf: Vec<u8>,
    bit_byte: u8,
    bit_used: u8,
}

impl Writer {
    pub fn new() -> Self {
        Writer {
            buf: Vec::new(),
            bit_byte: 0,
            bit_used: 0,
        }
    }

    fn flush_bits(&mut self) {
        if self.bit_used > 0 {
            self.buf.push(self.bit_byte);
            self.bit_byte = 0;
            self.bit_used = 0;
        }
    }

    pub fn u8(&mut self, v: u8) {
        self.flush_bits();
        self.buf.push(v);
    }

    pub fn u16(&mut self, v: u16) {
        self.flush_bits();
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn u32(&mut self, v: u32) {
        self.flush_bits();
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.flush_bits();
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn bit(&mut self, b: bool) {
        self.bit_byte |= (b as u8) << self.bit_used;
        self.bit_used += 1;
        if self.bit_used == 8 {
            self.buf.push(self.bit_byte);
            self.bit_byte = 0;
            self.bit_used = 0;
        }
    }

    pub fn short_str(&mut self, s: &str) {
        self.flush_bits();
        let b = s.as_bytes();
        self.buf.push(b.len() as u8);
        self.buf.extend_from_slice(b);
    }

    pub fn long_str(&mut self, s: &str) {
        self.flush_bits();
        let b = s.as_bytes();
        self.u32(b.len() as u32);
        self.buf.extend_from_slice(b);
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.bit_used > 0 {
            self.buf.push(self.bit_byte);
            self.bit_byte = 0;
            self.bit_used = 0;
        }
    }
}

impl Writer {
    /// Consume the writer and return the encoded bytes (needed because `Writer`
    /// implements `Drop`; the trailing partial bit-byte is flushed first).
    pub fn into_bytes(mut self) -> Vec<u8> {
        self.flush_bits();
        std::mem::take(&mut self.buf)
    }
}

// --- frame (de)serialization --------------------------------------------

/// Wrap `payload` in an AMQP frame of `frame_type` for `channel`.
fn frame(frame_type: u8, channel: u16, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + payload.len());
    out.push(frame_type);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out.push(FRAME_END);
    out
}

/// Encode a method frame.
pub fn encode_method(channel: u16, class: u16, method: u16, args: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + args.len());
    payload.extend_from_slice(&class.to_be_bytes());
    payload.extend_from_slice(&method.to_be_bytes());
    payload.extend_from_slice(args);
    frame(FRAME_METHOD, channel, &payload)
}

/// Encode a content-header frame (no properties in the Lite subset).
pub fn encode_header(channel: u16, class: u16, body_size: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(14);
    payload.extend_from_slice(&class.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes()); // weight
    payload.extend_from_slice(&body_size.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes()); // property flags
    frame(FRAME_HEADER, channel, &payload)
}

/// Encode a content-body frame.
pub fn encode_body(channel: u16, data: &[u8]) -> Vec<u8> {
    frame(FRAME_BODY, channel, data)
}

/// Encode a heartbeat frame.
pub fn encode_heartbeat() -> Vec<u8> {
    frame(FRAME_HEARTBEAT, 0, &[])
}

/// Parse one AMQP frame from the front of `buf`.
pub fn decode_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, ProtocolError> {
    if buf.len() < 7 {
        return Ok(None);
    }
    let frame_type = buf[0];
    let channel = u16::from_be_bytes([buf[1], buf[2]]);
    let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
    if size > 512 * 1024 * 1024 {
        return Err(ProtocolError::Malformed("frame too large"));
    }
    if buf.len() < 7 + size + 1 {
        return Ok(None);
    }
    let end = buf[7 + size];
    if end != FRAME_END {
        return Err(ProtocolError::Malformed("bad frame-end"));
    }
    let payload = &buf[7..7 + size];
    let frame = match frame_type {
        FRAME_HEARTBEAT => Frame::Heartbeat,
        FRAME_METHOD => {
            if payload.len() < 4 {
                return Err(ProtocolError::Malformed("short method frame"));
            }
            let class = u16::from_be_bytes([payload[0], payload[1]]);
            let method = u16::from_be_bytes([payload[2], payload[3]]);
            Frame::Method {
                channel,
                class,
                method,
                args: payload[4..].to_vec(),
            }
        }
        FRAME_HEADER => {
            if payload.len() < 14 {
                return Err(ProtocolError::Malformed("short header frame"));
            }
            let class = u16::from_be_bytes([payload[0], payload[1]]);
            let weight = u16::from_be_bytes([payload[2], payload[3]]);
            let body_size = u64::from_be_bytes([
                payload[4], payload[5], payload[6], payload[7], payload[8], payload[9], payload[10],
                payload[11],
            ]);
            Frame::Header {
                channel,
                class,
                weight,
                body_size,
                properties: payload[12..].to_vec(),
            }
        }
        FRAME_BODY => Frame::Body {
            channel,
            data: payload.to_vec(),
        },
        _other => return Err(ProtocolError::Malformed("unknown frame type")),
    };
    Ok(Some((frame, 8 + size)))
}

/// Convenience entry point used by the fuzz targets: decode one frame from
/// untrusted bytes. Never panics on malformed input.
pub fn parse(input: &[u8]) -> Result<Option<(Frame, usize)>, ProtocolError> {
    decode_frame(input)
}

/// Skip the property section of a content-header frame, returning the number
/// of property bytes consumed (used so callers can bounds-check).
pub fn skip_properties(reader: &mut Reader<'_>) -> Result<(), ProtocolError> {
    let mut flags = reader.u16()?;
    loop {
        for i in 0..15 {
            if flags & (1 << i) != 0 {
                read_property(i, reader)?;
            }
        }
        if flags & (1 << 15) != 0 {
            flags = reader.u16()?;
            continue;
        }
        break;
    }
    Ok(())
}

fn read_property(i: usize, r: &mut Reader<'_>) -> Result<(), ProtocolError> {
    match i {
        0 | 1 | 5 | 6 | 7 | 8 | 10 | 11 | 12 | 13 => {
            r.short_str()?;
        }
        2 => r.skip_table()?,
        3 | 4 => {
            r.u8()?;
        }
        9 => {
            r.u64()?;
        }
        _ => {}
    }
    Ok(())
}

// --- response encoders (server -> client) --------------------------------

/// `connection.start` — first handshake response after the protocol header.
pub fn encode_connection_start() -> Vec<u8> {
    let mut w = Writer::new();
    w.u8(0); // version-major
    w.u8(9); // version-minor
    // server-properties table (empty)
    w.u32(0);
    w.long_str("PLAIN"); // mechanisms
    w.short_str("en_US"); // locales
    encode_method(0, CLASS_CONNECTION, METHOD_CONNECTION_START, &w.buf)
}

pub fn encode_connection_tune() -> Vec<u8> {
    let mut w = Writer::new();
    w.u16(0); // channel-max (0 = unlimited)
    w.u32(0); // frame-max (0 = no limit)
    w.u16(0); // heartbeat (0 = disabled)
    encode_method(0, CLASS_CONNECTION, METHOD_CONNECTION_TUNE, &w.buf)
}

pub fn encode_connection_open_ok() -> Vec<u8> {
    let mut w = Writer::new();
    w.short_str(""); // reserved
    encode_method(0, CLASS_CONNECTION, METHOD_CONNECTION_OPEN_OK, &w.buf)
}

pub fn encode_connection_close_ok() -> Vec<u8> {
    encode_method(0, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE_OK, &[])
}

pub fn encode_channel_open_ok(channel: u16) -> Vec<u8> {
    let mut w = Writer::new();
    w.long_str(""); // channel-id
    encode_method(channel, CLASS_CHANNEL, METHOD_CHANNEL_OPEN_OK, &w.buf)
}

pub fn encode_channel_close_ok(channel: u16) -> Vec<u8> {
    encode_method(channel, CLASS_CHANNEL, METHOD_CHANNEL_CLOSE_OK, &[])
}

pub fn encode_exchange_declare_ok(channel: u16) -> Vec<u8> {
    encode_method(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK, &[])
}

pub fn encode_queue_declare_ok(channel: u16, name: &str, message_count: u32, consumer_count: u32) -> Vec<u8> {
    let mut w = Writer::new();
    w.short_str(name);
    w.u32(message_count);
    w.u32(consumer_count);
    encode_method(channel, CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK, &w.buf)
}

pub fn encode_queue_bind_ok(channel: u16) -> Vec<u8> {
    encode_method(channel, CLASS_QUEUE, METHOD_QUEUE_BIND_OK, &[])
}

pub fn encode_basic_qos_ok(channel: u16) -> Vec<u8> {
    encode_method(channel, CLASS_BASIC, METHOD_BASIC_QOS_OK, &[])
}

pub fn encode_basic_consume_ok(channel: u16, tag: &str) -> Vec<u8> {
    let mut w = Writer::new();
    w.short_str(tag);
    encode_method(channel, CLASS_BASIC, METHOD_BASIC_CONSUME_OK, &w.buf)
}

pub fn encode_basic_get_empty(channel: u16) -> Vec<u8> {
    let mut w = Writer::new();
    w.short_str(""); // cluster-id
    encode_method(channel, CLASS_BASIC, METHOD_BASIC_GET_EMPTY, &w.buf)
}

pub fn encode_basic_get_ok(
    channel: u16,
    delivery_tag: u64,
    redelivered: bool,
    exchange: &str,
    routing_key: &str,
    message_count: u32,
) -> Vec<u8> {
    let mut w = Writer::new();
    w.u64(delivery_tag);
    w.bit(redelivered);
    w.short_str(exchange);
    w.short_str(routing_key);
    w.u32(message_count);
    encode_method(channel, CLASS_BASIC, METHOD_BASIC_GET_OK, &w.buf)
}

/// Encode a server-initiated `basic.deliver` push (method + caller appends
/// header + body frames).
pub fn encode_basic_deliver(
    channel: u16,
    consumer_tag: &str,
    delivery_tag: u64,
    redelivered: bool,
    exchange: &str,
    routing_key: &str,
) -> Vec<u8> {
    let mut w = Writer::new();
    w.short_str(consumer_tag);
    w.u64(delivery_tag);
    w.bit(redelivered);
    w.short_str(exchange);
    w.short_str(routing_key);
    encode_method(channel, CLASS_BASIC, METHOD_BASIC_DELIVER, &w.buf)
}

/// Encode a `basic.ack` (client -> broker).
pub fn parse_basic_ack(args: &[u8]) -> Result<(u64, bool), ProtocolError> {
    let mut r = Reader::new(args);
    let delivery_tag = r.u64()?;
    let multiple = r.bit()?;
    Ok((delivery_tag, multiple))
}

/// Parse `basic.publish` args.
pub fn parse_basic_publish(args: &[u8]) -> Result<(String, String), ProtocolError> {
    let mut r = Reader::new(args);
    r.u16()?; // reserved-1
    let exchange = r.long_str()?;
    let routing_key = r.long_str()?;
    // mandatory + immediate bits
    r.bit()?;
    r.bit()?;
    Ok((exchange, routing_key))
}

/// Parse `basic.consume` args.
pub fn parse_basic_consume(args: &[u8]) -> Result<(String, String, bool), ProtocolError> {
    let mut r = Reader::new(args);
    r.u16()?; // reserved-1
    let queue = r.long_str()?;
    let tag = r.long_str()?;
    let no_local = r.bit()?;
    let no_ack = r.bit()?;
    r.bit()?; // exclusive
    r.bit()?; // nowait
    r.skip_table()?;
    let _ = no_local;
    Ok((queue, tag, no_ack))
}

/// Parse `basic.get` args.
pub fn parse_basic_get(args: &[u8]) -> Result<(String, bool), ProtocolError> {
    let mut r = Reader::new(args);
    r.u16()?; // reserved-1
    let queue = r.long_str()?;
    let no_ack = r.bit()?;
    Ok((queue, no_ack))
}

/// Parse `exchange.declare` args.
pub fn parse_exchange_declare(args: &[u8]) -> Result<(String, String, bool, bool), ProtocolError> {
    let mut r = Reader::new(args);
    r.u16()?; // reserved-1
    let exchange = r.long_str()?;
    let kind = r.long_str()?;
    let passive = r.bit()?;
    let durable = r.bit()?;
    r.bit()?; // auto-delete
    r.bit()?; // internal
    r.bit()?; // nowait
    r.skip_table()?;
    Ok((exchange, kind, passive, durable))
}

/// Parse `queue.declare` args.
pub fn parse_queue_declare(args: &[u8]) -> Result<(String, bool, bool), ProtocolError> {
    let mut r = Reader::new(args);
    r.u16()?; // reserved-1
    let queue = r.long_str()?;
    let passive = r.bit()?;
    let durable = r.bit()?;
    r.bit()?; // exclusive
    r.bit()?; // auto-delete
    r.bit()?; // nowait
    r.skip_table()?;
    Ok((queue, passive, durable))
}

/// Parse `queue.bind` args.
pub fn parse_queue_bind(args: &[u8]) -> Result<(String, String, String), ProtocolError> {
    let mut r = Reader::new(args);
    r.u16()?; // reserved-1
    let queue = r.long_str()?;
    let exchange = r.long_str()?;
    let routing_key = r.long_str()?;
    r.bit()?; // nowait
    r.skip_table()?;
    Ok((queue, exchange, routing_key))
}

/// Parse `connection.start-ok` args.
pub fn parse_connection_start_ok(args: &[u8]) -> Result<(), ProtocolError> {
    let mut r = Reader::new(args);
    r.skip_table()?; // client-properties
    r.long_str()?; // mechanism
    r.long_str_raw()?; // response
    r.short_str()?; // locale
    Ok(())
}

/// Parse `connection.tune-ok` args.
pub fn parse_connection_tune_ok(args: &[u8]) -> Result<(), ProtocolError> {
    let mut r = Reader::new(args);
    r.u16()?; // channel-max
    r.u32()?; // frame-max
    r.u16()?; // heartbeat
    Ok(())
}

/// Parse `connection.open` args.
pub fn parse_connection_open(args: &[u8]) -> Result<(), ProtocolError> {
    let mut r = Reader::new(args);
    let _vhost = r.long_str()?;
    r.short_str()?; // reserved-1
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_method_frame() {
        let bytes = encode_method(3, CLASS_BASIC, METHOD_BASIC_ACK, b"abc");
        let (frame, n) = decode_frame(&bytes).unwrap().unwrap();
        match frame {
            Frame::Method { channel, class, method, args } => {
                assert_eq!(channel, 3);
                assert_eq!(class, CLASS_BASIC);
                assert_eq!(method, METHOD_BASIC_ACK);
                assert_eq!(args, b"abc");
            }
            _ => panic!("expected method"),
        }
        assert_eq!(n, bytes.len());
    }

    #[test]
    fn incomplete_returns_none() {
        let mut bytes = encode_method(1, CLASS_CONNECTION, METHOD_CONNECTION_OPEN_OK, &[]);
        bytes.truncate(bytes.len() - 2);
        assert_eq!(decode_frame(&bytes).unwrap(), None);
    }

    #[test]
    fn bad_frame_end_is_malformed() {
        let mut bytes = encode_method(1, CLASS_CONNECTION, METHOD_CONNECTION_OPEN_OK, &[]);
        let last = bytes.len() - 1;
        bytes[last] = 0x00;
        assert!(decode_frame(&bytes).is_err());
    }

    #[test]
    fn publish_args_parse() {
        let mut w = Writer::new();
        w.u16(0);
        w.long_str("amq.direct");
        w.long_str("key.1");
        w.bit(false); // mandatory
        w.bit(false); // immediate
        let (ex, rk) = parse_basic_publish(&w.into_bytes()).unwrap();
        assert_eq!(ex, "amq.direct");
        assert_eq!(rk, "key.1");
    }

    #[test]
    fn consume_args_parse() {
        let mut w = Writer::new();
        w.u16(0);
        w.long_str("jobs");
        w.long_str("c1");
        w.bit(false); // no-local
        w.bit(true); // no-ack
        w.bit(false); // exclusive
        w.bit(false); // nowait
        w.u32(0); // arguments table (empty)
        let (q, tag, no_ack) = parse_basic_consume(&w.buf).unwrap();
        assert_eq!(q, "jobs");
        assert_eq!(tag, "c1");
        assert!(no_ack);
    }
}
