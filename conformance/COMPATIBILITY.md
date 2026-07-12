# tpt-synapse Compatibility Matrix & Migration Checker

Wire-compatibility status for replacing incumbent brokers with tpt-synapse,
per `TODO.md` Phase 2–3. "Wire-compatible" means an *existing* client library
for the incumbent protocol speaks to a tpt-synapse adapter without code
changes — the fastest path to a zero-migration-cost milestone.

Status legend: ✅ shipped & in-repo TCP-tested · 🟡 partial / tracked follow-up
· ⬜ not started.

| Protocol | Adapter | Incumbent | Baseline client | Wire compat | Conformance harness | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| MQTT 3.1.1 | `adapters/mqtt` | Mosquitto | paho-mqtt | ✅ | 🟡 (in-repo `tests/integration.rs`; paho-mqtt suite tracked) | QoS 1/2, wildcards, clean sessions, keep-alives |
| MQTT 5.0 | — | Mosquitto | paho-mqtt | ⬜ | ⬜ | reason codes, shared subs, enhanced auth — tracked follow-up |
| RESP (Redis) | `adapters/resp` | Redis | redis-rs | ✅ | 🟡 (in-repo `tests/integration.rs`; redis-rs suite tracked) | GET/SET/DEL/EXISTS/PUBLISH/XADD/XRANGE |
| Kafka | `adapters/kafka` | Kafka | librdkafka | ✅ | 🟡 (in-repo `tests/integration.rs` + harness `kafka_wire_roundtrip`; librdkafka suite tracked) | produce/fetch, consumer groups, offsets, metadata |
| AMQP 0-9-1 "Lite" | `adapters/amqp` | RabbitMQ | lapin / pika | 🟡 | 🟡 (in-repo `tests/integration.rs` + harness `amqp_wire_roundtrip`; lapin/pika suite tracked) | exchanges/bindings/queues, basic.publish/consume/get/ack |
| Native (tpt-synapse) | `adapters/native` | — | synapse SDKs | 🟡 | 🟡 | from-scratch protocol, Phase 4-adjacent — see `TODO.md` Parallel Track |

## Migration checker

Run before cutover. Each check is a binary pass/fail against a running
tpt-synapse node; the harness in this crate (`cargo test -p
synapse-conformance-harness`) executes the wire-level ones.

1. **Client library smoke** — point the incumbent's *unmodified* client at the
   tpt-synapse adapter port and perform one produce + one consume round-trip.
   Adapter port is the only config change.
2. **Semantics parity** — for each primitive the client uses:
   - pub/sub: wildcard matching (`+`, `#`) and QoS delivery guarantees.
   - log/stream: offset monotonicity, consumer-group rebalance behaviour.
   - queue: at-least-once redelivery on `nack`/disconnect.
   - KV: TTL expiry window and last-writer-wins semantics.
3. **Limits & quotas** — tenant quotas (throughput/storage) do not silently
   drop messages the incumbent would accept.
4. **Durability gate** — single-node `committed` == WAL-resident (Phase 1);
   multi-node `committed` == replicated to a majority (Phase 4, pending).

## Known gaps blocking each "replace incumbent" milestone

- **Mosquitto + Redis**: MQTT 5.0 parity and a *published* compatibility matrix
  are the remaining gaps (TODO.md Phase 2 milestone).
- **Kafka + RabbitMQ**: the out-of-process `librdkafka` / `lapin`(/`pika`)
  conformance suites are the remaining gap (TODO.md Phase 3 milestone). The
  in-repo TCP integration tests already prove the wire path.
- **Clustering**: none of the above milestones are "HA" until Phase 4's Raft
  replication lands (TODO.md Phase 4).

## Enabling the canonical third-party suites

```sh
# Kafka: real librdkafka client (needs C toolchain to build)
cargo test -p synapse-conformance-harness --features rdkafka -- --ignored

# AMQP: real lapin client (pure Rust)
cargo test -p synapse-conformance-harness --features lapin -- --ignored
```

The default `cargo test` runs the portable in-repo baseline
(`*_wire_roundtrip`) that reuses each adapter's public codec over a real TCP
socket.
