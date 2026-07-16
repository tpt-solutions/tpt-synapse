# synapse-routing

Unified routing engine for **[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)** — the
shared dispatch layer all protocol adapters route through.

## Components

- **`topic.rs`** — MQTT-style wildcard topic matching via a pre-compiled zero-heap
  `CompiledFilter`; perf-gated at ≥ 1 000 000 matches/sec
- **`stream.rs`** — Kafka-style stream routing (consumer groups, partition assignment)
- **`graph.rs`** — AMQP exchange/binding graph routing (direct, fanout, topic, headers)
- **`rule.rs`** — embedded SQL-like rule engine for content-based routing
- **`wasm_transform.rs`** — sandboxed per-tenant WASM transforms via `wasmtime`,
  fuel-limited to prevent runaway transforms
- **`backpressure.rs`** — shared backpressure signal that each adapter maps its own
  flow-control concept onto (MQTT inflight windows, Kafka fetch/produce quotas,
  AMQP prefetch/credit)

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
