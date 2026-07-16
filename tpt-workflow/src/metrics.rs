//! Prometheus metrics for the workflow matching service.
//!
//! Kept as a standalone registry (not wired into the engine's `/metrics`
//! listener) so the facade is observable without coupling to the core HTTP
//! server; `Metrics::encode` renders the standard text exposition format.

use std::sync::Arc;

use prometheus::{IntCounter, IntCounterVec, Registry, TextEncoder, Opts, register_int_counter_vec_with_registry, register_int_counter_with_registry};

/// Counters tracked by the matching service.
#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    tasks_enqueued: IntCounter,
    tasks_dispatched: IntCounter,
    tasks_acked: IntCounter,
    tasks_failed: IntCounter,
    /// Tasks that were redelivered after their visibility timeout elapsed.
    tasks_timed_out: IntCounter,
    long_poll_timeouts: IntCounter,
    /// Add calls partitioned by tenant for per-tenant accounting.
    enqueue_by_tenant: IntCounterVec,
    dispatch_by_tenant: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let tasks_enqueued =
            register_int_counter_with_registry!(Opts::new("wf_tasks_enqueued", "Tasks enqueued by schedulers"), registry).unwrap();
        let tasks_dispatched =
            register_int_counter_with_registry!(Opts::new("wf_tasks_dispatched", "Tasks dispatched to workers"), registry).unwrap();
        let tasks_acked =
            register_int_counter_with_registry!(Opts::new("wf_tasks_acked", "Tasks acknowledged (completed) by workers"), registry).unwrap();
        let tasks_failed =
            register_int_counter_with_registry!(Opts::new("wf_tasks_failed", "Tasks failed by workers"), registry).unwrap();
        let tasks_timed_out =
            register_int_counter_with_registry!(Opts::new("wf_tasks_timed_out", "Tasks redelivered after visibility timeout"), registry).unwrap();
        let long_poll_timeouts =
            register_int_counter_with_registry!(Opts::new("wf_long_poll_timeouts", "Long-poll pulls that timed out empty"), registry).unwrap();
        let enqueue_by_tenant = register_int_counter_vec_with_registry!(
            Opts::new("wf_enqueue_by_tenant", "Tasks enqueued per tenant"),
            &["tenant"],
            registry
        )
        .unwrap();
        let dispatch_by_tenant = register_int_counter_vec_with_registry!(
            Opts::new("wf_dispatch_by_tenant", "Tasks dispatched per tenant"),
            &["tenant"],
            registry
        )
        .unwrap();

        Self {
            registry: Arc::new(registry),
            tasks_enqueued,
            tasks_dispatched,
            tasks_acked,
            tasks_failed,
            tasks_timed_out,
            long_poll_timeouts,
            enqueue_by_tenant,
            dispatch_by_tenant,
        }
    }

    pub fn enqueued(&self, tenant: &str) {
        self.tasks_enqueued.inc();
        self.enqueue_by_tenant.with_label_values(&[tenant]).inc();
    }

    pub fn dispatched(&self, tenant: &str) {
        self.tasks_dispatched.inc();
        self.dispatch_by_tenant.with_label_values(&[tenant]).inc();
    }

    pub fn acked(&self) {
        self.tasks_acked.inc();
    }

    pub fn failed(&self) {
        self.tasks_failed.inc();
    }

    pub fn timed_out(&self) {
        self.tasks_timed_out.inc();
    }

    pub fn long_poll_timeout(&self) {
        self.long_poll_timeouts.inc();
    }

    /// Render the metrics registry in Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let mfs = self.registry.gather();
        let mut out = String::new();
        let encoder = TextEncoder::new();
        let _ = encoder.encode_utf8(&mfs, &mut out);
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
