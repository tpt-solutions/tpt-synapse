# synapse-core

Unified storage engine for **[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)** — a
single broker that natively speaks MQTT, Kafka, AMQP, and Redis RESP wire protocols over one
shared storage and routing core.

## What this crate provides

Three access patterns over one shared tiered append-only physical layer:

| Primitive | Semantics | Wire analogue |
|-----------|-----------|---------------|
| `Log` | Strictly ordered, append-only sequence | Kafka topic partition |
| `Queue` | At-least-once FIFO with acknowledgement | AMQP queue / MQTT inflight |
| `Map` | Last-writer-wins KV store with optional TTL | Redis string/hash |

All three share one `SegmentedLog` / `TieredSegmentedLog` storage backend. Protocol adapters
don't own storage — they translate wire semantics onto these primitives via `synapse-routing`.

## Features

- `io-uring` — Linux io_uring network I/O backend (requires Linux; off by default)
- `consensus` — embedded Raft consensus via `openraft` for multi-node HA (off by default)

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
