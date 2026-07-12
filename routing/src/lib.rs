//! Unified Routing & Compute Engine (spec.txt §3.2, §6 Phase 1).
//!
//! Four cooperating pieces sit between the protocol adapters and the storage
//! core:
//! * [`topic`] — hierarchical pub/sub matching (MQTT), with `+`/`#` wildcards.
//! * [`stream`] — consumer groups, partition assignment, and offset tracking
//!   (Kafka).
//! * [`graph`] — exchange/binding/queue routing (AMQP "Lite").
//! * [`rule`] — an embedded SQL-like rule engine for filter/route/transform.
//! * [`wasm_transform`] — sandboxed WASM transform plugins for untrusted
//!   per-tenant logic that doesn't fit the rule engine's `WHERE` predicates.
//!
//! One shared [`backpressure`] signal gives every adapter a single internal
//! representation to translate to/from (MQTT inflight windows, Kafka
//! fetch/produce quotas, AMQP prefetch/credit) — see TODO.md.

pub mod backpressure;
pub mod graph;
pub mod rule;
pub mod stream;
pub mod topic;
pub mod wasm_transform;

use std::fmt;

/// Error type for routing-layer operations (rule parse errors, unknown
/// exchanges, etc.).
#[derive(Debug, Clone)]
pub struct RoutingError(pub String);

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RoutingError {}

impl From<&str> for RoutingError {
    fn from(s: &str) -> Self {
        RoutingError(s.to_string())
    }
}

impl From<String> for RoutingError {
    fn from(s: String) -> Self {
        RoutingError(s)
    }
}

impl RoutingError {
    pub fn new(s: impl Into<String>) -> Self {
        RoutingError(s.into())
    }
}

pub type RoutingResult<T> = Result<T, RoutingError>;
