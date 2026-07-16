//! MQTT 3.1.1 and 5.0 wire codec (spec.txt §3.3, §6 Phase 2).
//!
//! [`decode_packet`] is the untrusted-input entry point: it parses a single
//! `&[u8]` frame and returns the number of bytes consumed, or [`None`] when the
//! buffer is a prefix of a larger frame (caller should read more bytes and
//! retry). [`encode_packet`] renders a [`Packet`] back to bytes for the wire.
//!
//! Protocol version is negotiated once, on CONNECT (the only self-describing
//! packet — its `level` byte is 4 for v3.1.1 or 5 for v5.0). Every packet
//! after that on the same connection must be decoded/encoded according to
//! that negotiated [`ProtocolVersion`], which the caller (`server.rs`) threads
//! through as an explicit parameter. For `ProtocolVersion::V311`, all v5-only
//! fields (`properties`, `reason` codes) are `None`/`Success` and produce
//! byte-identical wire output to the pre-v5 codec.

use std::fmt;

/// Negotiated MQTT protocol version for a connection, learned from CONNECT's
/// protocol level byte (4 = v3.1.1, 5 = v5.0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    V311,
    V5,
}

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

/// MQTT 3.1.1 SUBACK return codes. `Failure` is `0x80`. Retained (rather than
/// replaced by [`ReasonCode`]) so v3.1.1 SUBACK framing is untouched; v5
/// SUBACK carries the richer [`ReasonCode`] set separately in `v5_reasons`.
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

    fn to_reason(self) -> ReasonCode {
        match self {
            SubAckCode::Qos0 => ReasonCode::Success,
            SubAckCode::Qos1 => ReasonCode::GrantedQoS1,
            SubAckCode::Qos2 => ReasonCode::GrantedQoS2,
            SubAckCode::Failure => ReasonCode::UnspecifiedError,
        }
    }
}

/// MQTT v5 reason codes (union of the CONNACK / PUBACK-family / SUBACK /
/// UNSUBACK / DISCONNECT / AUTH code spaces). Not every variant is legal in
/// every packet type — the encode/decode function for each packet only
/// accepts the subset the spec allows there; the caller is trusted to pick a
/// legal value for what it's sending, matching this crate's existing
/// `SubAckCode` style.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasonCode {
    Success,
    GrantedQoS1,
    GrantedQoS2,
    DisconnectWithWillMessage,
    NoMatchingSubscribers,
    NoSubscriptionExisted,
    ContinueAuthentication,
    ReAuthenticate,
    UnspecifiedError,
    MalformedPacket,
    ProtocolError,
    ImplementationSpecificError,
    UnsupportedProtocolVersion,
    ClientIdentifierNotValid,
    BadUsernameOrPassword,
    NotAuthorized,
    ServerUnavailable,
    ServerBusy,
    Banned,
    ServerShuttingDown,
    BadAuthenticationMethod,
    KeepAliveTimeout,
    SessionTakenOver,
    TopicFilterInvalid,
    TopicNameInvalid,
    PacketIdentifierInUse,
    PacketIdentifierNotFound,
    ReceiveMaximumExceeded,
    TopicAliasInvalid,
    PacketTooLarge,
    MessageRateTooHigh,
    QuotaExceeded,
    AdministrativeAction,
    PayloadFormatInvalid,
    RetainNotSupported,
    QoSNotSupported,
    UseAnotherServer,
    ServerMoved,
    SharedSubscriptionsNotSupported,
    ConnectionRateExceeded,
    MaximumConnectTime,
    SubscriptionIdentifiersNotSupported,
    WildcardSubscriptionsNotSupported,
}

impl ReasonCode {
    pub fn to_u8(self) -> u8 {
        use ReasonCode::*;
        match self {
            Success => 0x00,
            GrantedQoS1 => 0x01,
            GrantedQoS2 => 0x02,
            DisconnectWithWillMessage => 0x04,
            NoMatchingSubscribers => 0x10,
            NoSubscriptionExisted => 0x11,
            ContinueAuthentication => 0x18,
            ReAuthenticate => 0x19,
            UnspecifiedError => 0x80,
            MalformedPacket => 0x81,
            ProtocolError => 0x82,
            ImplementationSpecificError => 0x83,
            UnsupportedProtocolVersion => 0x84,
            ClientIdentifierNotValid => 0x85,
            BadUsernameOrPassword => 0x86,
            NotAuthorized => 0x87,
            ServerUnavailable => 0x88,
            ServerBusy => 0x89,
            Banned => 0x8A,
            ServerShuttingDown => 0x8B,
            BadAuthenticationMethod => 0x8C,
            KeepAliveTimeout => 0x8D,
            SessionTakenOver => 0x8E,
            TopicFilterInvalid => 0x8F,
            TopicNameInvalid => 0x90,
            PacketIdentifierInUse => 0x91,
            PacketIdentifierNotFound => 0x92,
            ReceiveMaximumExceeded => 0x93,
            TopicAliasInvalid => 0x94,
            PacketTooLarge => 0x95,
            MessageRateTooHigh => 0x96,
            QuotaExceeded => 0x97,
            AdministrativeAction => 0x98,
            PayloadFormatInvalid => 0x99,
            RetainNotSupported => 0x9A,
            QoSNotSupported => 0x9B,
            UseAnotherServer => 0x9C,
            ServerMoved => 0x9D,
            SharedSubscriptionsNotSupported => 0x9E,
            ConnectionRateExceeded => 0x9F,
            MaximumConnectTime => 0xA0,
            SubscriptionIdentifiersNotSupported => 0xA1,
            WildcardSubscriptionsNotSupported => 0xA2,
        }
    }

    pub fn from_u8(v: u8) -> Result<ReasonCode, ProtocolError> {
        use ReasonCode::{
            AdministrativeAction, BadAuthenticationMethod, BadUsernameOrPassword, Banned,
            ClientIdentifierNotValid, ConnectionRateExceeded, ContinueAuthentication,
            DisconnectWithWillMessage, GrantedQoS1, GrantedQoS2, ImplementationSpecificError,
            KeepAliveTimeout, MalformedPacket, MaximumConnectTime, MessageRateTooHigh,
            NoMatchingSubscribers, NoSubscriptionExisted, NotAuthorized, PacketIdentifierInUse,
            PacketIdentifierNotFound, PacketTooLarge, PayloadFormatInvalid, QoSNotSupported,
            QuotaExceeded, ReAuthenticate, ReceiveMaximumExceeded, RetainNotSupported, ServerBusy,
            ServerMoved, ServerShuttingDown, ServerUnavailable, SessionTakenOver,
            SharedSubscriptionsNotSupported, SubscriptionIdentifiersNotSupported, Success,
            TopicAliasInvalid, TopicFilterInvalid, TopicNameInvalid, UnspecifiedError,
            UnsupportedProtocolVersion, UseAnotherServer, WildcardSubscriptionsNotSupported,
        };
        // `ReasonCode::ProtocolError` shares a name with this module's
        // `ProtocolError` error type; qualify it explicitly instead of
        // glob-importing to avoid shadowing.
        Ok(match v {
            0x00 => Success,
            0x01 => GrantedQoS1,
            0x02 => GrantedQoS2,
            0x04 => DisconnectWithWillMessage,
            0x10 => NoMatchingSubscribers,
            0x11 => NoSubscriptionExisted,
            0x18 => ContinueAuthentication,
            0x19 => ReAuthenticate,
            0x80 => UnspecifiedError,
            0x81 => MalformedPacket,
            0x82 => ReasonCode::ProtocolError,
            0x83 => ImplementationSpecificError,
            0x84 => UnsupportedProtocolVersion,
            0x85 => ClientIdentifierNotValid,
            0x86 => BadUsernameOrPassword,
            0x87 => NotAuthorized,
            0x88 => ServerUnavailable,
            0x89 => ServerBusy,
            0x8A => Banned,
            0x8B => ServerShuttingDown,
            0x8C => BadAuthenticationMethod,
            0x8D => KeepAliveTimeout,
            0x8E => SessionTakenOver,
            0x8F => TopicFilterInvalid,
            0x90 => TopicNameInvalid,
            0x91 => PacketIdentifierInUse,
            0x92 => PacketIdentifierNotFound,
            0x93 => ReceiveMaximumExceeded,
            0x94 => TopicAliasInvalid,
            0x95 => PacketTooLarge,
            0x96 => MessageRateTooHigh,
            0x97 => QuotaExceeded,
            0x98 => AdministrativeAction,
            0x99 => PayloadFormatInvalid,
            0x9A => RetainNotSupported,
            0x9B => QoSNotSupported,
            0x9C => UseAnotherServer,
            0x9D => ServerMoved,
            0x9E => SharedSubscriptionsNotSupported,
            0x9F => ConnectionRateExceeded,
            0xA0 => MaximumConnectTime,
            0xA1 => SubscriptionIdentifiersNotSupported,
            0xA2 => WildcardSubscriptionsNotSupported,
            _ => return Err(ProtocolError::Malformed("unknown reason code")),
        })
    }

    fn to_subacklike(self) -> SubAckCode {
        match self {
            ReasonCode::Success => SubAckCode::Qos0,
            ReasonCode::GrantedQoS1 => SubAckCode::Qos1,
            ReasonCode::GrantedQoS2 => SubAckCode::Qos2,
            _ => SubAckCode::Failure,
        }
    }
}

/// A single MQTT v5 user property. Repeatable; order is preserved for
/// echo-back fidelity though the spec doesn't require it.
pub type UserProperty = (String, String);

/// v5 Properties, decoded into a flat struct rather than a generic id->value
/// map: every property ID has a fixed type and (except User Property) occurs
/// at most once, so a typed struct gives compile-time field access in
/// `broker.rs`/`server.rs` instead of stringly-typed lookups.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Properties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub subscription_identifier: Option<u32>,
    pub session_expiry_interval: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub server_keep_alive: Option<u16>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
    pub request_problem_information: Option<u8>,
    pub will_delay_interval: Option<u32>,
    pub request_response_information: Option<u8>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub reason_string: Option<String>,
    pub receive_maximum: Option<u16>,
    pub topic_alias_maximum: Option<u16>,
    pub topic_alias: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub user_properties: Vec<UserProperty>,
    pub maximum_packet_size: Option<u32>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifier_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
}

impl Properties {
    pub fn is_empty(&self) -> bool {
        *self == Properties::default()
    }
}

mod prop_id {
    pub const PAYLOAD_FORMAT_INDICATOR: u8 = 0x01;
    pub const MESSAGE_EXPIRY_INTERVAL: u8 = 0x02;
    pub const CONTENT_TYPE: u8 = 0x03;
    pub const RESPONSE_TOPIC: u8 = 0x08;
    pub const CORRELATION_DATA: u8 = 0x09;
    pub const SUBSCRIPTION_IDENTIFIER: u8 = 0x0B;
    pub const SESSION_EXPIRY_INTERVAL: u8 = 0x11;
    pub const ASSIGNED_CLIENT_IDENTIFIER: u8 = 0x12;
    pub const SERVER_KEEP_ALIVE: u8 = 0x13;
    pub const AUTHENTICATION_METHOD: u8 = 0x15;
    pub const AUTHENTICATION_DATA: u8 = 0x16;
    pub const REQUEST_PROBLEM_INFORMATION: u8 = 0x17;
    pub const WILL_DELAY_INTERVAL: u8 = 0x18;
    pub const REQUEST_RESPONSE_INFORMATION: u8 = 0x19;
    pub const RESPONSE_INFORMATION: u8 = 0x1A;
    pub const SERVER_REFERENCE: u8 = 0x1C;
    pub const REASON_STRING: u8 = 0x1F;
    pub const RECEIVE_MAXIMUM: u8 = 0x21;
    pub const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
    pub const TOPIC_ALIAS: u8 = 0x23;
    pub const MAXIMUM_QOS: u8 = 0x24;
    pub const RETAIN_AVAILABLE: u8 = 0x25;
    pub const USER_PROPERTY: u8 = 0x26;
    pub const MAXIMUM_PACKET_SIZE: u8 = 0x27;
    pub const WILDCARD_SUBSCRIPTION_AVAILABLE: u8 = 0x28;
    pub const SUBSCRIPTION_IDENTIFIER_AVAILABLE: u8 = 0x29;
    pub const SHARED_SUBSCRIPTION_AVAILABLE: u8 = 0x2A;
}

/// Retain-handling option for a v5 SUBSCRIBE topic filter (bits 4-5 of the
/// subscribe options byte). v3.1.1 has no equivalent; always
/// `SendAtSubscribe` there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainHandling {
    SendAtSubscribe,
    SendIfNewSubscription,
    DoNotSend,
}

impl RetainHandling {
    fn from_bits(b: u8) -> Result<Self, ProtocolError> {
        match b {
            0 => Ok(RetainHandling::SendAtSubscribe),
            1 => Ok(RetainHandling::SendIfNewSubscription),
            2 => Ok(RetainHandling::DoNotSend),
            _ => Err(ProtocolError::Malformed("bad retain handling")),
        }
    }

    fn to_bits(self) -> u8 {
        match self {
            RetainHandling::SendAtSubscribe => 0,
            RetainHandling::SendIfNewSubscription => 1,
            RetainHandling::DoNotSend => 2,
        }
    }
}

/// One SUBSCRIBE topic filter with its v5 subscribe options (No Local,
/// Retain As Published, Retain Handling default to "off"/3.1.1 behavior when
/// decoded from a v3.1.1 SUBSCRIBE).
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeTopic {
    pub filter: String,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

/// A decoded MQTT control packet.
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Connect(Connect),
    ConnAck {
        session_present: bool,
        code: u8,
        properties: Option<Properties>,
    },
    Publish(Publish),
    PubAck {
        packet_id: u16,
        reason: ReasonCode,
        properties: Option<Properties>,
    },
    PubRec {
        packet_id: u16,
        reason: ReasonCode,
        properties: Option<Properties>,
    },
    PubRel {
        packet_id: u16,
        reason: ReasonCode,
        properties: Option<Properties>,
    },
    PubComp {
        packet_id: u16,
        reason: ReasonCode,
        properties: Option<Properties>,
    },
    Subscribe {
        packet_id: u16,
        topics: Vec<SubscribeTopic>,
        properties: Option<Properties>,
    },
    SubAck {
        packet_id: u16,
        codes: Vec<SubAckCode>,
        v5_reasons: Option<Vec<ReasonCode>>,
        properties: Option<Properties>,
    },
    Unsubscribe {
        packet_id: u16,
        topics: Vec<String>,
        properties: Option<Properties>,
    },
    UnsubAck {
        packet_id: u16,
        v5_reasons: Option<Vec<ReasonCode>>,
        properties: Option<Properties>,
    },
    PingReq,
    PingResp,
    Disconnect {
        reason: ReasonCode,
        properties: Option<Properties>,
    },
    /// v5-only enhanced-auth packet (type 15); absent from v3.1.1.
    Auth {
        reason: ReasonCode,
        properties: Option<Properties>,
    },
}

/// CONNECT variable header + payload (3.1.1 and 5.0).
#[derive(Debug, Clone, PartialEq)]
pub struct Connect {
    pub protocol: ProtocolVersion,
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<Will>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub properties: Option<Properties>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    pub properties: Option<Properties>,
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
    pub properties: Option<Properties>,
}

impl Publish {
    /// Re-publish this message to a downstream subscriber, assigning a fresh
    /// `packet_id` (for QoS > 0) and clearing the DUP flag. Properties are
    /// intentionally dropped: per-subscriber concerns (like a topic alias
    /// established by the *publisher*) must not leak into another
    /// connection's independent alias table; the broker/server re-populates
    /// whatever v5 properties are relevant to this specific delivery.
    pub fn to_delivery(&self, packet_id: Option<u16>) -> Publish {
        Publish {
            dup: false,
            qos: self.qos,
            retain: false,
            topic: self.topic.clone(),
            packet_id,
            payload: self.payload.clone(),
            properties: None,
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

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        if self.remaining() < 4 {
            return Err(ProtocolError::Incomplete);
        }
        let v = u32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
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

    /// Length-prefixed binary data (u16 length + raw bytes) — the MQTT
    /// "Binary Data" wire type used for will payloads, correlation data, and
    /// authentication data.
    fn binary(&mut self) -> Result<Vec<u8>, ProtocolError> {
        let len = self.u16()? as usize;
        Ok(self.bytes(len)?.to_vec())
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

    /// Decode a v5 Properties block: `[varint length][repeated: id, value]`.
    fn properties(&mut self) -> Result<Properties, ProtocolError> {
        let (len, _) = self.varint()?;
        let len = len as usize;
        if self.remaining() < len {
            return Err(ProtocolError::Incomplete);
        }
        let end = self.pos + len;
        let mut props = Properties::default();
        while self.pos < end {
            let id = self.u8()?;
            match id {
                prop_id::PAYLOAD_FORMAT_INDICATOR => props.payload_format_indicator = Some(self.u8()?),
                prop_id::MESSAGE_EXPIRY_INTERVAL => props.message_expiry_interval = Some(self.u32()?),
                prop_id::CONTENT_TYPE => props.content_type = Some(self.string()?),
                prop_id::RESPONSE_TOPIC => props.response_topic = Some(self.string()?),
                prop_id::CORRELATION_DATA => props.correlation_data = Some(self.binary()?),
                prop_id::SUBSCRIPTION_IDENTIFIER => {
                    let (v, _) = self.varint()?;
                    props.subscription_identifier = Some(v);
                }
                prop_id::SESSION_EXPIRY_INTERVAL => props.session_expiry_interval = Some(self.u32()?),
                prop_id::ASSIGNED_CLIENT_IDENTIFIER => props.assigned_client_identifier = Some(self.string()?),
                prop_id::SERVER_KEEP_ALIVE => props.server_keep_alive = Some(self.u16()?),
                prop_id::AUTHENTICATION_METHOD => props.authentication_method = Some(self.string()?),
                prop_id::AUTHENTICATION_DATA => props.authentication_data = Some(self.binary()?),
                prop_id::REQUEST_PROBLEM_INFORMATION => props.request_problem_information = Some(self.u8()?),
                prop_id::WILL_DELAY_INTERVAL => props.will_delay_interval = Some(self.u32()?),
                prop_id::REQUEST_RESPONSE_INFORMATION => props.request_response_information = Some(self.u8()?),
                prop_id::RESPONSE_INFORMATION => props.response_information = Some(self.string()?),
                prop_id::SERVER_REFERENCE => props.server_reference = Some(self.string()?),
                prop_id::REASON_STRING => props.reason_string = Some(self.string()?),
                prop_id::RECEIVE_MAXIMUM => props.receive_maximum = Some(self.u16()?),
                prop_id::TOPIC_ALIAS_MAXIMUM => props.topic_alias_maximum = Some(self.u16()?),
                prop_id::TOPIC_ALIAS => props.topic_alias = Some(self.u16()?),
                prop_id::MAXIMUM_QOS => props.maximum_qos = Some(self.u8()?),
                prop_id::RETAIN_AVAILABLE => props.retain_available = Some(self.u8()?),
                prop_id::USER_PROPERTY => {
                    let k = self.string()?;
                    let v = self.string()?;
                    props.user_properties.push((k, v));
                }
                prop_id::MAXIMUM_PACKET_SIZE => props.maximum_packet_size = Some(self.u32()?),
                prop_id::WILDCARD_SUBSCRIPTION_AVAILABLE => props.wildcard_subscription_available = Some(self.u8()?),
                prop_id::SUBSCRIPTION_IDENTIFIER_AVAILABLE => {
                    props.subscription_identifier_available = Some(self.u8()?)
                }
                prop_id::SHARED_SUBSCRIPTION_AVAILABLE => props.shared_subscription_available = Some(self.u8()?),
                _ => return Err(ProtocolError::Malformed("unknown property id")),
            }
        }
        Ok(props)
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

    fn u32(&mut self, v: u32) {
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

    /// Length-prefixed binary data — mirrors [`Reader::binary`].
    fn binary(&mut self, b: &[u8]) {
        self.u16(b.len() as u16);
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

    fn properties(&mut self, p: &Properties) {
        let mut body = Writer::new();
        if let Some(v) = p.payload_format_indicator {
            body.u8(prop_id::PAYLOAD_FORMAT_INDICATOR);
            body.u8(v);
        }
        if let Some(v) = p.message_expiry_interval {
            body.u8(prop_id::MESSAGE_EXPIRY_INTERVAL);
            body.u32(v);
        }
        if let Some(v) = &p.content_type {
            body.u8(prop_id::CONTENT_TYPE);
            body.string(v);
        }
        if let Some(v) = &p.response_topic {
            body.u8(prop_id::RESPONSE_TOPIC);
            body.string(v);
        }
        if let Some(v) = &p.correlation_data {
            body.u8(prop_id::CORRELATION_DATA);
            body.binary(v);
        }
        if let Some(v) = p.subscription_identifier {
            body.u8(prop_id::SUBSCRIPTION_IDENTIFIER);
            body.varint(v);
        }
        if let Some(v) = p.session_expiry_interval {
            body.u8(prop_id::SESSION_EXPIRY_INTERVAL);
            body.u32(v);
        }
        if let Some(v) = &p.assigned_client_identifier {
            body.u8(prop_id::ASSIGNED_CLIENT_IDENTIFIER);
            body.string(v);
        }
        if let Some(v) = p.server_keep_alive {
            body.u8(prop_id::SERVER_KEEP_ALIVE);
            body.u16(v);
        }
        if let Some(v) = &p.authentication_method {
            body.u8(prop_id::AUTHENTICATION_METHOD);
            body.string(v);
        }
        if let Some(v) = &p.authentication_data {
            body.u8(prop_id::AUTHENTICATION_DATA);
            body.binary(v);
        }
        if let Some(v) = p.request_problem_information {
            body.u8(prop_id::REQUEST_PROBLEM_INFORMATION);
            body.u8(v);
        }
        if let Some(v) = p.will_delay_interval {
            body.u8(prop_id::WILL_DELAY_INTERVAL);
            body.u32(v);
        }
        if let Some(v) = p.request_response_information {
            body.u8(prop_id::REQUEST_RESPONSE_INFORMATION);
            body.u8(v);
        }
        if let Some(v) = &p.response_information {
            body.u8(prop_id::RESPONSE_INFORMATION);
            body.string(v);
        }
        if let Some(v) = &p.server_reference {
            body.u8(prop_id::SERVER_REFERENCE);
            body.string(v);
        }
        if let Some(v) = &p.reason_string {
            body.u8(prop_id::REASON_STRING);
            body.string(v);
        }
        if let Some(v) = p.receive_maximum {
            body.u8(prop_id::RECEIVE_MAXIMUM);
            body.u16(v);
        }
        if let Some(v) = p.topic_alias_maximum {
            body.u8(prop_id::TOPIC_ALIAS_MAXIMUM);
            body.u16(v);
        }
        if let Some(v) = p.topic_alias {
            body.u8(prop_id::TOPIC_ALIAS);
            body.u16(v);
        }
        if let Some(v) = p.maximum_qos {
            body.u8(prop_id::MAXIMUM_QOS);
            body.u8(v);
        }
        if let Some(v) = p.retain_available {
            body.u8(prop_id::RETAIN_AVAILABLE);
            body.u8(v);
        }
        for (k, v) in &p.user_properties {
            body.u8(prop_id::USER_PROPERTY);
            body.string(k);
            body.string(v);
        }
        if let Some(v) = p.maximum_packet_size {
            body.u8(prop_id::MAXIMUM_PACKET_SIZE);
            body.u32(v);
        }
        if let Some(v) = p.wildcard_subscription_available {
            body.u8(prop_id::WILDCARD_SUBSCRIPTION_AVAILABLE);
            body.u8(v);
        }
        if let Some(v) = p.subscription_identifier_available {
            body.u8(prop_id::SUBSCRIPTION_IDENTIFIER_AVAILABLE);
            body.u8(v);
        }
        if let Some(v) = p.shared_subscription_available {
            body.u8(prop_id::SHARED_SUBSCRIPTION_AVAILABLE);
            body.u8(v);
        }
        self.varint(body.buf.len() as u32);
        self.buf.extend_from_slice(&body.buf);
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
const AUTH: u8 = 15;

/// Parse one MQTT packet from the front of `buf`, according to `version`
/// (the version negotiated on this connection's CONNECT — ignored for
/// CONNECT itself, which is self-describing via its own protocol-level byte).
///
/// * `Ok(Some((packet, n)))` — a complete packet spanning `n` bytes.
/// * `Ok(None)` — `buf` is a prefix; read more and retry.
/// * `Err(_)` — malformed, must close the connection.
pub fn decode_packet(buf: &[u8], version: ProtocolVersion) -> Result<Option<(Packet, usize)>, ProtocolError> {
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
        CONNACK => decode_connack(body, version)?,
        PUBLISH => decode_publish(body, flags, version)?,
        PUBACK => {
            let (packet_id, reason, properties) = decode_simple_ack(body, version)?;
            Packet::PubAck { packet_id, reason, properties }
        }
        PUBREC => {
            let (packet_id, reason, properties) = decode_simple_ack(body, version)?;
            Packet::PubRec { packet_id, reason, properties }
        }
        PUBREL => {
            if flags != 0x02 {
                return Err(ProtocolError::Malformed("PUBREL must use flags 0x02"));
            }
            let (packet_id, reason, properties) = decode_simple_ack(body, version)?;
            Packet::PubRel { packet_id, reason, properties }
        }
        PUBCOMP => {
            let (packet_id, reason, properties) = decode_simple_ack(body, version)?;
            Packet::PubComp { packet_id, reason, properties }
        }
        SUBSCRIBE => {
            if flags != 0x02 {
                return Err(ProtocolError::Malformed("SUBSCRIBE must use flags 0x02"));
            }
            decode_subscribe(body, version)?
        }
        SUBACK => decode_suback(body, version)?,
        UNSUBSCRIBE => {
            if flags != 0x02 {
                return Err(ProtocolError::Malformed("UNSUBSCRIBE must use flags 0x02"));
            }
            decode_unsubscribe(body, version)?
        }
        UNSUBACK => {
            if version == ProtocolVersion::V5 {
                let mut r = Reader::new(body);
                let packet_id = r.u16()?;
                let properties = Some(r.properties()?);
                let mut reasons = Vec::new();
                while r.remaining() > 0 {
                    reasons.push(ReasonCode::from_u8(r.u8()?)?);
                }
                Packet::UnsubAck { packet_id, v5_reasons: Some(reasons), properties }
            } else {
                Packet::UnsubAck { packet_id: parse_id(body)?, v5_reasons: None, properties: None }
            }
        }
        PINGREQ => Packet::PingReq,
        PINGRESP => Packet::PingResp,
        DISCONNECT => {
            if version == ProtocolVersion::V5 {
                let (reason, properties) = decode_reason_properties(body)?;
                Packet::Disconnect { reason, properties }
            } else {
                Packet::Disconnect { reason: ReasonCode::Success, properties: None }
            }
        }
        AUTH => {
            let (reason, properties) = decode_reason_properties(body)?;
            Packet::Auth { reason, properties }
        }
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

/// Decode the shared `[packet_id][reason?][properties?]` shape used by
/// PUBACK/PUBREC/PUBREL/PUBCOMP: under v5, a 2-byte body implies
/// `(Success, None)`, a 3-byte body carries just a reason, and anything
/// longer carries a reason plus a properties block. Under v3.1.1 the body is
/// always just the 2-byte packet id.
fn decode_simple_ack(body: &[u8], version: ProtocolVersion) -> Result<(u16, ReasonCode, Option<Properties>), ProtocolError> {
    if version != ProtocolVersion::V5 {
        return Ok((parse_id(body)?, ReasonCode::Success, None));
    }
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    if r.remaining() == 0 {
        return Ok((packet_id, ReasonCode::Success, None));
    }
    let reason = ReasonCode::from_u8(r.u8()?)?;
    if r.remaining() == 0 {
        return Ok((packet_id, reason, None));
    }
    Ok((packet_id, reason, Some(r.properties()?)))
}

fn encode_simple_ack(
    typ: u8,
    flags: u8,
    version: ProtocolVersion,
    packet_id: u16,
    reason: ReasonCode,
    properties: &Option<Properties>,
    out: &mut Vec<u8>,
) {
    let mut w = Writer::new();
    w.u16(packet_id);
    if version == ProtocolVersion::V5 {
        let has_props = properties.as_ref().is_some_and(|p| !p.is_empty());
        if reason == ReasonCode::Success && !has_props {
            // 2-byte short form: nothing more to write.
        } else if !has_props {
            w.u8(reason.to_u8());
        } else {
            w.u8(reason.to_u8());
            w.properties(properties.as_ref().unwrap());
        }
    }
    fixed(typ, flags, &w.buf, out);
}

/// Decode the `[reason?][properties?]` shape shared by v5 DISCONNECT and
/// AUTH (no packet id): an empty body implies `(Success, None)`, a 1-byte
/// body carries just a reason, anything longer carries a reason plus
/// properties.
fn decode_reason_properties(body: &[u8]) -> Result<(ReasonCode, Option<Properties>), ProtocolError> {
    if body.is_empty() {
        return Ok((ReasonCode::Success, None));
    }
    let mut r = Reader::new(body);
    let reason = ReasonCode::from_u8(r.u8()?)?;
    if r.remaining() == 0 {
        return Ok((reason, None));
    }
    Ok((reason, Some(r.properties()?)))
}

fn encode_reason_properties(typ: u8, flags: u8, reason: ReasonCode, properties: &Option<Properties>, out: &mut Vec<u8>) {
    let has_props = properties.as_ref().is_some_and(|p| !p.is_empty());
    let mut w = Writer::new();
    if reason == ReasonCode::Success && !has_props {
        // Empty body.
    } else if !has_props {
        w.u8(reason.to_u8());
    } else {
        w.u8(reason.to_u8());
        w.properties(properties.as_ref().unwrap());
    }
    fixed(typ, flags, &w.buf, out);
}

fn decode_connect(body: &[u8], _flags: u8) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let proto = r.string()?;
    if proto != "MQTT" {
        return Err(ProtocolError::Malformed("bad protocol name"));
    }
    let level = r.u8()?;
    let version = match level {
        4 => ProtocolVersion::V311,
        5 => ProtocolVersion::V5,
        _ => return Err(ProtocolError::Malformed("unsupported protocol level")),
    };
    let connect_flags = r.u8()?;
    let keep_alive = r.u16()?;
    let properties = if version == ProtocolVersion::V5 {
        Some(r.properties()?)
    } else {
        None
    };
    let client_id = r.string()?;

    let clean_session = connect_flags & 0x02 != 0;
    let will_flag = connect_flags & 0x04 != 0;
    let will_retain = connect_flags & 0x20 != 0;
    let will_qos = QoS::from_u8((connect_flags >> 3) & 0x03)
        .ok_or(ProtocolError::Malformed("bad will qos"))?;
    let username_flag = connect_flags & 0x80 != 0;
    let password_flag = connect_flags & 0x40 != 0;

    let will = if will_flag {
        let will_properties = if version == ProtocolVersion::V5 {
            Some(r.properties()?)
        } else {
            None
        };
        let topic = r.string()?;
        // Will Payload is length-prefixed ("Binary Data") in both versions.
        let payload = r.binary()?;
        Some(Will { topic, payload, qos: will_qos, retain: will_retain, properties: will_properties })
    } else {
        None
    };
    let username = if username_flag {
        Some(r.string()?)
    } else {
        None
    };
    let password = if password_flag { Some(r.binary()?) } else { None };

    Ok(Packet::Connect(Connect {
        protocol: version,
        client_id,
        keep_alive,
        clean_session,
        will,
        username,
        password,
        properties,
    }))
}

fn decode_connack(body: &[u8], version: ProtocolVersion) -> Result<Packet, ProtocolError> {
    if body.len() < 2 {
        return Err(ProtocolError::Malformed("connack too short"));
    }
    let session_present = body[0] & 0x01 != 0;
    let code = body[1];
    let properties = if version == ProtocolVersion::V5 {
        let mut r = Reader::new(&body[2..]);
        Some(r.properties()?)
    } else {
        None
    };
    Ok(Packet::ConnAck { session_present, code, properties })
}

fn decode_publish(body: &[u8], flags: u8, version: ProtocolVersion) -> Result<Packet, ProtocolError> {
    let dup = flags & 0x08 != 0;
    let qos = QoS::from_u8((flags >> 1) & 0x03)
        .ok_or(ProtocolError::Malformed("bad publish qos"))?;
    let retain = flags & 0x01 != 0;

    let mut r = Reader::new(body);
    let topic = r.string()?;
    let packet_id = if qos != QoS::AtMostOnce {
        Some(r.u16()?)
    } else {
        None
    };
    let properties = if version == ProtocolVersion::V5 {
        Some(r.properties()?)
    } else {
        None
    };
    // A v5 PUBLISH may carry an empty topic name when a Topic Alias supplies
    // the real topic; v3.1.1 has no such mechanism, so keep that check strict.
    if version == ProtocolVersion::V311 && topic.is_empty() {
        return Err(ProtocolError::Malformed("empty publish topic"));
    }
    let payload = r.bytes(r.remaining())?.to_vec();
    Ok(Packet::Publish(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
        properties,
    }))
}

fn decode_subscribe(body: &[u8], version: ProtocolVersion) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    let properties = if version == ProtocolVersion::V5 {
        Some(r.properties()?)
    } else {
        None
    };
    let mut topics = Vec::new();
    while r.remaining() >= 3 {
        let filter = r.string()?;
        let opts = r.u8()?;
        let qos = QoS::from_u8(opts & 0x03)
            .ok_or(ProtocolError::Malformed("bad subscribe qos"))?;
        let (no_local, retain_as_published, retain_handling) = if version == ProtocolVersion::V5 {
            (
                opts & 0x04 != 0,
                opts & 0x08 != 0,
                RetainHandling::from_bits((opts >> 4) & 0x03)?,
            )
        } else {
            (false, false, RetainHandling::SendAtSubscribe)
        };
        topics.push(SubscribeTopic { filter, qos, no_local, retain_as_published, retain_handling });
    }
    if topics.is_empty() {
        return Err(ProtocolError::Malformed("subscribe has no topics"));
    }
    Ok(Packet::Subscribe { packet_id, topics, properties })
}

fn decode_suback(body: &[u8], version: ProtocolVersion) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    if version == ProtocolVersion::V5 {
        let properties = Some(r.properties()?);
        let mut reasons = Vec::new();
        while r.remaining() > 0 {
            reasons.push(ReasonCode::from_u8(r.u8()?)?);
        }
        let codes = reasons.iter().map(|rc| rc.to_subacklike()).collect();
        Ok(Packet::SubAck { packet_id, codes, v5_reasons: Some(reasons), properties })
    } else {
        let mut codes = Vec::new();
        while r.remaining() > 0 {
            codes.push(SubAckCode::from_u8(r.u8()?));
        }
        Ok(Packet::SubAck { packet_id, codes, v5_reasons: None, properties: None })
    }
}

fn decode_unsubscribe(body: &[u8], version: ProtocolVersion) -> Result<Packet, ProtocolError> {
    let mut r = Reader::new(body);
    let packet_id = r.u16()?;
    let properties = if version == ProtocolVersion::V5 {
        Some(r.properties()?)
    } else {
        None
    };
    let mut topics = Vec::new();
    while r.remaining() >= 2 {
        topics.push(r.string()?);
    }
    Ok(Packet::Unsubscribe { packet_id, topics, properties })
}

/// Encode `packet` into `out`, framed according to `version` (ignored for
/// `Packet::Connect`, which carries its own `protocol` field).
pub fn encode_packet(packet: &Packet, version: ProtocolVersion, out: &mut Vec<u8>) {
    match packet {
        Packet::Connect(c) => encode_connect(c, out),
        Packet::ConnAck { session_present, code, properties } => {
            let mut w = Writer::new();
            w.u8(if *session_present { 0x01 } else { 0x00 });
            w.u8(*code);
            if version == ProtocolVersion::V5 {
                w.properties(&properties.clone().unwrap_or_default());
            }
            fixed(CONNACK, 0, &w.buf, out);
        }
        Packet::Publish(p) => encode_publish(p, version, out),
        Packet::PubAck { packet_id, reason, properties } => {
            encode_simple_ack(PUBACK, 0, version, *packet_id, *reason, properties, out)
        }
        Packet::PubRec { packet_id, reason, properties } => {
            encode_simple_ack(PUBREC, 0, version, *packet_id, *reason, properties, out)
        }
        Packet::PubRel { packet_id, reason, properties } => {
            encode_simple_ack(PUBREL, 0x02, version, *packet_id, *reason, properties, out)
        }
        Packet::PubComp { packet_id, reason, properties } => {
            encode_simple_ack(PUBCOMP, 0, version, *packet_id, *reason, properties, out)
        }
        Packet::Subscribe { packet_id, topics, properties } => {
            let mut w = Writer::new();
            w.u16(*packet_id);
            if version == ProtocolVersion::V5 {
                w.properties(&properties.clone().unwrap_or_default());
            }
            for t in topics {
                w.string(&t.filter);
                let mut opts = t.qos as u8;
                if version == ProtocolVersion::V5 {
                    if t.no_local {
                        opts |= 0x04;
                    }
                    if t.retain_as_published {
                        opts |= 0x08;
                    }
                    opts |= t.retain_handling.to_bits() << 4;
                }
                w.u8(opts);
            }
            fixed(SUBSCRIBE, 0x02, &w.buf, out);
        }
        Packet::SubAck { packet_id, codes, v5_reasons, properties } => {
            let mut w = Writer::new();
            w.u16(*packet_id);
            if version == ProtocolVersion::V5 {
                w.properties(&properties.clone().unwrap_or_default());
                let reasons: Vec<ReasonCode> = v5_reasons
                    .clone()
                    .unwrap_or_else(|| codes.iter().map(|c| c.to_reason()).collect());
                for rc in reasons {
                    w.u8(rc.to_u8());
                }
            } else {
                for c in codes {
                    w.u8(c.to_u8());
                }
            }
            fixed(SUBACK, 0, &w.buf, out);
        }
        Packet::Unsubscribe { packet_id, topics, properties } => {
            let mut w = Writer::new();
            w.u16(*packet_id);
            if version == ProtocolVersion::V5 {
                w.properties(&properties.clone().unwrap_or_default());
            }
            for t in topics {
                w.string(t);
            }
            fixed(UNSUBSCRIBE, 0x02, &w.buf, out);
        }
        Packet::UnsubAck { packet_id, v5_reasons, properties } => {
            if version == ProtocolVersion::V5 {
                let mut w = Writer::new();
                w.u16(*packet_id);
                w.properties(&properties.clone().unwrap_or_default());
                for rc in v5_reasons.clone().unwrap_or_default() {
                    w.u8(rc.to_u8());
                }
                fixed(UNSUBACK, 0, &w.buf, out);
            } else {
                fixed_id(UNSUBACK, packet_id, out);
            }
        }
        Packet::PingReq => fixed(PINGREQ, 0, &[], out),
        Packet::PingResp => fixed(PINGRESP, 0, &[], out),
        Packet::Disconnect { reason, properties } => {
            if version == ProtocolVersion::V5 {
                encode_reason_properties(DISCONNECT, 0, *reason, properties, out);
            } else {
                fixed(DISCONNECT, 0, &[], out);
            }
        }
        Packet::Auth { reason, properties } => encode_reason_properties(AUTH, 0, *reason, properties, out),
    }
}

fn encode_connect(c: &Connect, out: &mut Vec<u8>) {
    let mut w = Writer::new();
    w.string("MQTT");
    w.u8(if c.protocol == ProtocolVersion::V5 { 5 } else { 4 });
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
    if c.protocol == ProtocolVersion::V5 {
        w.properties(&c.properties.clone().unwrap_or_default());
    }
    w.string(&c.client_id);
    if let Some(will) = &c.will {
        if c.protocol == ProtocolVersion::V5 {
            w.properties(&will.properties.clone().unwrap_or_default());
        }
        w.string(&will.topic);
        w.binary(&will.payload);
    }
    if let Some(u) = &c.username {
        w.string(u);
    }
    if let Some(p) = &c.password {
        w.binary(p);
    }
    fixed(CONNECT, 0, &w.buf, out);
}

fn encode_publish(p: &Publish, version: ProtocolVersion, out: &mut Vec<u8>) {
    let mut w = Writer::new();
    w.string(&p.topic);
    if p.qos != QoS::AtMostOnce {
        w.u16(p.packet_id.unwrap_or(0));
    }
    if version == ProtocolVersion::V5 {
        w.properties(&p.properties.clone().unwrap_or_default());
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

/// Convenience entry point used by the fuzz targets: decode a single packet
/// from untrusted bytes, assuming v3.1.1 framing (the fuzz target has no
/// connection state to learn a negotiated version from). Never panics on
/// malformed input.
pub fn parse(input: &[u8]) -> Result<Option<(Packet, usize)>, ProtocolError> {
    decode_packet(input, ProtocolVersion::V311)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(p: Packet) {
        roundtrip_version(p, ProtocolVersion::V311);
    }

    fn roundtrip_version(p: Packet, version: ProtocolVersion) {
        let mut buf = Vec::new();
        encode_packet(&p, version, &mut buf);
        let (decoded, n) = decode_packet(&buf, version).unwrap().unwrap();
        assert_eq!(decoded, p);
        assert_eq!(n, buf.len());
    }

    #[test]
    fn connect_roundtrip() {
        roundtrip(Packet::Connect(Connect {
            protocol: ProtocolVersion::V311,
            client_id: "client-1".into(),
            keep_alive: 60,
            clean_session: true,
            will: None,
            username: Some("u".into()),
            password: Some(b"p".to_vec()),
            properties: None,
        }));
    }

    #[test]
    fn connect_with_will_and_credentials_roundtrip() {
        roundtrip(Packet::Connect(Connect {
            protocol: ProtocolVersion::V311,
            client_id: "client-2".into(),
            keep_alive: 30,
            clean_session: false,
            will: Some(Will {
                topic: "status/offline".into(),
                payload: b"bye".to_vec(),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: None,
            }),
            username: Some("u".into()),
            password: Some(b"p".to_vec()),
            properties: None,
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
            properties: None,
        }));
    }

    #[test]
    fn subscribe_suback_roundtrip() {
        roundtrip(Packet::Subscribe {
            packet_id: 7,
            topics: vec![
                SubscribeTopic {
                    filter: "sensors/#".into(),
                    qos: QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: RetainHandling::SendAtSubscribe,
                },
                SubscribeTopic {
                    filter: "x/+".into(),
                    qos: QoS::ExactlyOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: RetainHandling::SendAtSubscribe,
                },
            ],
            properties: None,
        });
        roundtrip(Packet::SubAck {
            packet_id: 7,
            codes: vec![SubAckCode::Qos0, SubAckCode::Failure],
            v5_reasons: None,
            properties: None,
        });
    }

    #[test]
    fn incomplete_returns_none() {
        let mut buf = Vec::new();
        encode_packet(&Packet::PingReq, ProtocolVersion::V311, &mut buf);
        let partial = &buf[..buf.len() - 1];
        assert_eq!(decode_packet(partial, ProtocolVersion::V311).unwrap(), None);
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

    fn sample_properties() -> Properties {
        Properties {
            payload_format_indicator: Some(1),
            message_expiry_interval: Some(3600),
            content_type: Some("text/plain".into()),
            response_topic: Some("resp/1".into()),
            correlation_data: Some(b"corr".to_vec()),
            subscription_identifier: Some(42),
            session_expiry_interval: Some(120),
            assigned_client_identifier: Some("gen-1".into()),
            server_keep_alive: Some(30),
            authentication_method: Some("SCRAM".into()),
            authentication_data: Some(b"auth".to_vec()),
            request_problem_information: Some(1),
            will_delay_interval: Some(5),
            request_response_information: Some(1),
            response_information: Some("resp-info".into()),
            server_reference: Some("other-server".into()),
            reason_string: Some("because".into()),
            receive_maximum: Some(10),
            topic_alias_maximum: Some(16),
            topic_alias: Some(2),
            maximum_qos: Some(1),
            retain_available: Some(1),
            user_properties: vec![("k1".into(), "v1".into()), ("k2".into(), "v2".into())],
            maximum_packet_size: Some(65536),
            wildcard_subscription_available: Some(1),
            subscription_identifier_available: Some(1),
            shared_subscription_available: Some(1),
        }
    }

    #[test]
    fn properties_roundtrip() {
        let props = sample_properties();
        let mut w = Writer::new();
        w.properties(&props);
        let mut r = Reader::new(&w.buf);
        let decoded = r.properties().unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn empty_properties_roundtrip() {
        let props = Properties::default();
        let mut w = Writer::new();
        w.properties(&props);
        assert_eq!(w.buf, vec![0x00]); // just the zero-length varint
        let mut r = Reader::new(&w.buf);
        let decoded = r.properties().unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn unknown_property_id_rejected() {
        let mut r = Reader::new(&[0x02, 0xFF, 0x00]); // len=2, unknown id 0xFF
        assert_eq!(r.properties(), Err(ProtocolError::Malformed("unknown property id")));
    }

    #[test]
    fn varint_property_length_incomplete() {
        let mut r = Reader::new(&[0x05, 0x01]); // declares 5 bytes, only 1 present
        assert_eq!(r.properties(), Err(ProtocolError::Incomplete));
    }

    #[test]
    fn connect_v5_roundtrip() {
        roundtrip_version(
            Packet::Connect(Connect {
                protocol: ProtocolVersion::V5,
                client_id: "v5-client".into(),
                keep_alive: 60,
                clean_session: true,
                will: Some(Will {
                    topic: "status/offline".into(),
                    payload: b"bye".to_vec(),
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    properties: Some(Properties {
                        will_delay_interval: Some(10),
                        ..Default::default()
                    }),
                }),
                username: Some("u".into()),
                password: Some(b"p".to_vec()),
                properties: Some(Properties {
                    session_expiry_interval: Some(60),
                    receive_maximum: Some(20),
                    ..Default::default()
                }),
            }),
            ProtocolVersion::V5,
        );
    }

    #[test]
    fn connack_v5_roundtrip() {
        roundtrip_version(
            Packet::ConnAck {
                session_present: false,
                code: ReasonCode::Success.to_u8(),
                properties: Some(Properties {
                    retain_available: Some(1),
                    ..Default::default()
                }),
            },
            ProtocolVersion::V5,
        );
        // A v5 CONNACK's properties block is always present on the wire (its
        // length prefix is unconditional, unlike the PUBACK-family's 2-byte
        // short form), so `None` on encode input normalizes to
        // `Some(Properties::default())` on decode — assert that directly
        // rather than expecting a roundtrip back to `None`.
        let mut buf = Vec::new();
        encode_packet(
            &Packet::ConnAck { session_present: false, code: ReasonCode::Success.to_u8(), properties: None },
            ProtocolVersion::V5,
            &mut buf,
        );
        let (decoded, _) = decode_packet(&buf, ProtocolVersion::V5).unwrap().unwrap();
        assert_eq!(
            decoded,
            Packet::ConnAck {
                session_present: false,
                code: ReasonCode::Success.to_u8(),
                properties: Some(Properties::default())
            }
        );
    }

    #[test]
    fn puback_v5_success_short_form() {
        let pkt = Packet::PubAck { packet_id: 5, reason: ReasonCode::Success, properties: None };
        let mut buf = Vec::new();
        encode_packet(&pkt, ProtocolVersion::V5, &mut buf);
        // type/flags byte + remaining-length(1) + 2-byte packet id == 4 bytes total.
        assert_eq!(buf.len(), 4);
        roundtrip_version(pkt, ProtocolVersion::V5);
    }

    #[test]
    fn puback_v5_with_reason_and_properties() {
        roundtrip_version(
            Packet::PubAck {
                packet_id: 9,
                reason: ReasonCode::NoMatchingSubscribers,
                properties: Some(Properties { reason_string: Some("no subs".into()), ..Default::default() }),
            },
            ProtocolVersion::V5,
        );
    }

    #[test]
    fn subscribe_v5_options_roundtrip() {
        roundtrip_version(
            Packet::Subscribe {
                packet_id: 3,
                topics: vec![SubscribeTopic {
                    filter: "$share/g/sensors/#".into(),
                    qos: QoS::AtLeastOnce,
                    no_local: true,
                    retain_as_published: true,
                    retain_handling: RetainHandling::DoNotSend,
                }],
                properties: Some(Properties { subscription_identifier: Some(7), ..Default::default() }),
            },
            ProtocolVersion::V5,
        );
    }

    #[test]
    fn suback_v5_reasons_roundtrip() {
        // As with CONNACK (see `connack_v5_roundtrip`), a v5 SUBACK's
        // properties block is unconditionally present, so `None` normalizes
        // to `Some(Properties::default())` through the wire.
        roundtrip_version(
            Packet::SubAck {
                packet_id: 3,
                codes: vec![SubAckCode::Qos1, SubAckCode::Failure],
                v5_reasons: Some(vec![ReasonCode::GrantedQoS1, ReasonCode::SharedSubscriptionsNotSupported]),
                properties: Some(Properties::default()),
            },
            ProtocolVersion::V5,
        );
    }

    #[test]
    fn disconnect_v5_with_reason_roundtrip() {
        roundtrip_version(
            Packet::Disconnect {
                reason: ReasonCode::SessionTakenOver,
                properties: Some(Properties { reason_string: Some("took over".into()), ..Default::default() }),
            },
            ProtocolVersion::V5,
        );
    }

    #[test]
    fn disconnect_v311_bare() {
        let pkt = Packet::Disconnect { reason: ReasonCode::Success, properties: None };
        let mut buf = Vec::new();
        encode_packet(&pkt, ProtocolVersion::V311, &mut buf);
        assert_eq!(buf, vec![DISCONNECT << 4, 0x00]);
        roundtrip_version(pkt, ProtocolVersion::V311);
    }

    #[test]
    fn auth_roundtrip() {
        roundtrip_version(
            Packet::Auth {
                reason: ReasonCode::ContinueAuthentication,
                properties: Some(Properties {
                    authentication_method: Some("SCRAM".into()),
                    authentication_data: Some(b"challenge".to_vec()),
                    ..Default::default()
                }),
            },
            ProtocolVersion::V5,
        );
    }

    #[test]
    fn v311_wire_output_unchanged() {
        // Byte-for-byte fixture guarding backward compatibility: this is the
        // exact encoding the pre-v5 codec produced for this packet.
        let pkt = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a".into(),
            packet_id: Some(1),
            payload: b"x".to_vec(),
            properties: None,
        });
        let mut buf = Vec::new();
        encode_packet(&pkt, ProtocolVersion::V311, &mut buf);
        assert_eq!(buf, vec![0x32, 0x06, 0x00, 0x01, b'a', 0x00, 0x01, b'x']);
    }
}
