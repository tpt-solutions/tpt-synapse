//! # tpt-workflow
//!
//! A gRPC **task-queue facade over the `Queue` primitive** — a Temporal
//! "matching service"-style dispatch layer for tpt-synapse. It lets a scheduler
//! push workflow activity tasks onto a task queue and workers pull them with a
//! long-poll or streaming RPC, receiving completions via an idempotent ack.
//!
//! The facade is intentionally thin: all durability and FIFO ordering come from
//! [`synapse_core::Queue`]; this crate layers on the workflow-specific leasing
//! semantics a raw queue lacks:
//!
//! * **Per-task-queue routing** — `(tenant, task_queue, task_type)` maps to one
//!   isolated FIFO queue.
//! * **Visibility timeout + redelivery** — a dispatched task is invisible until
//!   acked; if its lease lapses (worker crash), a sweeper re-enqueues it.
//! * **Idempotent ack** — each delivery carries an opaque task token; a repeat
//!   response for an already-responded token is a successful duplicate.
//! * **Long-poll / streaming pull** — `PollTask` blocks for a task; `StreamTasks`
//!   keeps a stream open and pushes tasks as they arrive.
//!
//! See `proto/workflow.proto` for the wire contract and `MatchingService`.

pub mod dispatch;
pub mod error;
pub mod metrics;
pub mod proto;
pub mod server;
pub mod service;

pub use dispatch::{AckResult, TaskQueueManager};
pub use error::{WorkflowError, WorkflowResult};
pub use server::{spawn, ServerHandle};
pub use service::MatchingServiceImpl;
