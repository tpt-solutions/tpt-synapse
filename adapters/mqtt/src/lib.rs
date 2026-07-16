//! MQTT v3.1.1 and v5.0 wire protocol adapter (spec.txt §3.3, §6 Phase 2).
//!
//! Turns the unified storage core + routing engine into a wire-compatible
//! MQTT broker: publishers write to concrete topics, subscribers register
//! wildcard filters matched by the routing engine, and QoS 1/2 publishes are
//! durably recorded on the core [`Log`] primitive. Run it with
//! [`server::serve`] over TCP.
//!
//! [`parse`] is the untrusted-input entry point fuzzed by
//! `fuzz/fuzz_targets/parse.rs`.

pub mod broker;
pub mod codec;
pub mod server;

pub use broker::Broker;
pub use codec::{
    decode_packet, encode_packet, parse, Connect, Packet, Properties, ProtocolError,
    ProtocolVersion, Publish, QoS, ReasonCode, RetainHandling, SubAckCode, SubscribeTopic, Will,
};
pub use server::serve;
