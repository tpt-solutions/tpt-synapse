# Protocol Conformance Harness

Runs real client libraries against a running `tpt-synapse` broker to verify
each wire adapter is a drop-in replacement for the protocol it implements.
This is the harness referenced in [TODO.md](../TODO.md) Phase 0, populated
per-adapter as Phases 2-3 land:

| Adapter | Client library | Status |
|---|---|---|
| MQTT    | `paho-mqtt`  | not yet populated (Phase 2) |
| RESP    | `redis-rs`   | not yet populated (Phase 2) |
| Kafka   | `librdkafka` | not yet populated (Phase 3) |
| AMQP    | `pika`       | not yet populated (Phase 3) |

## Structure

`harness/` is a Rust crate holding the (currently empty/ignored) test modules,
one per adapter. `redis-rs` and `paho-mqtt` are Rust crates and slot in as
regular `dev-dependencies`. `librdkafka` (C) and `pika` (Python) don't have
idiomatic Rust bindings usable as test dependencies here, so their suites will
run as separate out-of-process test runners invoked by `scripts/ci.sh`/
`scripts/ci.ps1` once populated — not as `cargo test` targets.

Each populated suite should also produce a compatibility matrix + a
migration-checker doc/tool flagging unsupported features, per TODO.md
Phase 2/3.
