//! Kafka wire protocol adapter: produce/fetch → Log writes/reads and consumer
//! group management (spec.txt §6 Phase 3). Turns the unified storage core into
//! a wire-compatible Kafka broker. Run it with [`server::serve`] over TCP.
//!
//! [`parse`] is the untrusted-input entry point fuzzed by
//! `fuzz/fuzz_targets/parse.rs`.

pub mod broker;
pub mod codec;
pub mod server;

pub use broker::Broker;
pub use codec::{decode_request, parse, ApiKey, Frame, ProtocolError};
pub use server::serve;
