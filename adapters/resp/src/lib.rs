//! RESP (Redis) adapter: GET/SET/PUBLISH/XADD mapped to Map/Log operations
//! (spec.txt §6 Phase 2).
//!
//! Turns the unified storage core into a wire-compatible Redis broker. Run it
//! with [`server::serve`] over TCP. [`parse`] is the untrusted-input entry
//! point fuzzed by `fuzz/fuzz_targets/parse.rs`.

pub mod broker;
pub mod codec;
pub mod server;

pub use broker::RespBroker;
pub use codec::{decode_value, encode_value, parse, Value, RespError};
pub use server::serve;
