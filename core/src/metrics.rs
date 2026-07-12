//! Prometheus metrics registry (TODO.md Phase 1).
//!
//! Exposes the baseline production-trust metrics (throughput, latency, queue
//! depth, map size, retained log bytes) via the Prometheus text exposition
//! format. This module owns the registry and renders the body; the HTTP
//! `/metrics` listener that serves it lives in [`crate::http`].

use crate::error::EngineResult;
use prometheus::{Encoder, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};

/// Counter/gauged metric labels. `tenant` and `name` provide per-resource
/// dimensionality without exploding cardinality.
#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    routing_ops: IntCounterVec,
    log_appends: IntCounterVec,
    log_bytes: IntGaugeVec,
    queue_enqueues: IntCounterVec,
    queue_depth: IntGaugeVec,
    map_sets: IntCounterVec,
    map_size: IntGaugeVec,
    op_latency: HistogramVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let routing_ops = IntCounterVec::new(
            Opts::new(
                "synapse_routing_ops_total",
                "Total internal routing operations, by tenant and router kind.",
            ),
            &["tenant", "router"],
        )
        .unwrap();
        let log_appends = IntCounterVec::new(
            Opts::new(
                "synapse_log_appends_total",
                "Total log appends, by tenant and log.",
            ),
            &["tenant", "name"],
        )
        .unwrap();
        let log_bytes = IntGaugeVec::new(
            Opts::new(
                "synapse_log_bytes",
                "Retained log bytes, by tenant and log.",
            ),
            &["tenant", "name"],
        )
        .unwrap();
        let queue_enqueues = IntCounterVec::new(
            Opts::new(
                "synapse_queue_enqueues_total",
                "Total queue enqueues, by tenant and queue.",
            ),
            &["tenant", "name"],
        )
        .unwrap();
        let queue_depth = IntGaugeVec::new(
            Opts::new(
                "synapse_queue_depth",
                "Outstanding queue messages (ready + in-flight).",
            ),
            &["tenant", "name"],
        )
        .unwrap();
        let map_sets = IntCounterVec::new(
            Opts::new(
                "synapse_map_sets_total",
                "Total map sets, by tenant and map.",
            ),
            &["tenant", "name"],
        )
        .unwrap();
        let map_size = IntGaugeVec::new(
            Opts::new(
                "synapse_map_size",
                "Live map entries, by tenant and map.",
            ),
            &["tenant", "name"],
        )
        .unwrap();
        let op_latency = HistogramVec::new(
            prometheus::histogram_opts!(
                "synapse_op_latency_seconds",
                "Internal operation latency distribution.",
                vec![
                    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
                ]
            ),
            &["router"],
        )
        .unwrap();

        registry.register(Box::new(routing_ops.clone())).unwrap();
        registry.register(Box::new(log_appends.clone())).unwrap();
        registry.register(Box::new(log_bytes.clone())).unwrap();
        registry.register(Box::new(queue_enqueues.clone())).unwrap();
        registry.register(Box::new(queue_depth.clone())).unwrap();
        registry.register(Box::new(map_sets.clone())).unwrap();
        registry.register(Box::new(map_size.clone())).unwrap();
        registry.register(Box::new(op_latency.clone())).unwrap();

        Self {
            registry,
            routing_ops,
            log_appends,
            log_bytes,
            queue_enqueues,
            queue_depth,
            map_sets,
            map_size,
            op_latency,
        }
    }

    pub fn routing_op(&self, tenant: &str, router: &str) {
        self.routing_ops.with_label_values(&[tenant, router]).inc();
    }

    pub fn log_append(&self, tenant: &str, name: &str, bytes: u64) {
        self.log_appends.with_label_values(&[tenant, name]).inc();
        self.log_bytes
            .with_label_values(&[tenant, name])
            .add(bytes as i64);
    }

    pub fn queue_enqueue(&self, tenant: &str, name: &str, depth: i64) {
        self.queue_enqueues.with_label_values(&[tenant, name]).inc();
        self.queue_depth
            .with_label_values(&[tenant, name])
            .set(depth);
    }

    pub fn map_set(&self, tenant: &str, name: &str, size: i64) {
        self.map_sets.with_label_values(&[tenant, name]).inc();
        self.map_size.with_label_values(&[tenant, name]).set(size);
    }

    pub fn observe_latency(&self, router: &str, seconds: f64) {
        self.op_latency.with_label_values(&[router]).observe(seconds);
    }

    /// Render the Prometheus text exposition format for the `/metrics` endpoint.
    pub fn render(&self) -> EngineResult<String> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        encoder
            .encode(&metric_families, &mut buf)
            .map_err(|e| crate::error::EngineError::internal(format!("encode metrics: {e}")))?;
        String::from_utf8(buf).map_err(|e| crate::error::EngineError::internal(format!("utf8: {e}")))
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_counters() {
        let m = Metrics::new();
        m.routing_op("acme", "topic");
        m.log_append("acme", "orders", 12);
        let out = m.render().unwrap();
        assert!(out.contains("synapse_routing_ops_total"));
        assert!(out.contains("synapse_log_bytes"));
        assert!(out.contains("acme"));
    }
}
