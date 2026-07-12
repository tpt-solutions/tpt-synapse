//! # tpt-synapse core storage engine
//!
//! Unified storage primitives — [`Log`], [`Queue`], [`Map`] — sharing one
//! tiered, append-only physical layer ([`storage`]), with multi-tenant
//! isolation ([`tenant`]) and Prometheus metrics ([`metrics`]). The
//! [`SynapseCore`] engine is the in-process surface the routing engine and the
//! protocol adapters build on (spec.txt §3.1, §6 Phase 1).
//!
//! ## Consistency & durability model (pre-Phase-4, single node)
//!
//! Written down *before* the storage engine was built, per TODO.md, because it
//! constrains the write path rather than being an afterthought.
//!
//! * **Writes:** every mutation (log append, queue enqueue, map set) is
//!   durably written to the shared [`storage::TieredSegmentedLog`] WAL before
//!   it is acknowledged to the caller. On this single-node target the WAL
//!   *is* the source of truth; in-memory indexes (queue ready/inflight,
//!   map hashmap) are rebuildable hot structures over the WAL.
//! * **Durability:** `committed` == the record has been appended to the
//!   in-memory segment and (for the hot tier) is not at risk of loss on
//!   process crash because the segment lives in the process. Object-tier
//!   offload ([`storage::TieredSegmentedLog::offload_sealed`]) is *best-effort
//!   capacity management*: a sealed segment is copied to object storage and
//!   then dropped from resident memory, so a crash between copy and drop could
//!   lose that segment until Phase 4 adds fsync/replication. This is
//!   acceptable for the Phase 1 single-node milestone and is called out
//!   explicitly so Phase 4's Raft replication closes the gap (the WAL becomes
//!   the Raft log; `committed` then means "replicated to a majority").
//! * **Ordering:** a single global, monotonic offset space gives the `Log`
//!   strict append-ordering; the `Queue` is FIFO with at-least-once delivery
//!   (unacked messages are redelivered via [`queue::Queue::nack`]); `Map`
//!   offers last-writer-wins per key with optional TTL.
//! * **Reads:** `Log::read` and the queue/map indexes are satisfied from
//!   memory; offloaded segments are fetched back transparently, preserving the
//!   same offset API.
//! * **Isolation:** all resources are namespaced by [`tenant::TenantId`]; a
//!   tenant cannot address another tenant's primitives, and per-tenant quota
//!   accounting (bytes, object counts, ops/sec) is enforced at creation and
//!   on every charge.

pub mod engine;
pub mod error;
pub mod http;
pub mod log;
pub mod map;
pub mod metrics;
pub mod queue;
pub mod storage;
pub mod tenant;

pub use engine::SynapseCore;
pub use error::{EngineError, EngineResult, ErrorKind};
pub use http::spawn_metrics_server;
pub use log::Log;
pub use map::Map;
pub use metrics::Metrics;
pub use queue::Queue;
pub use tenant::{Tenant, TenantId, TenantQuota, TenantRegistry};
