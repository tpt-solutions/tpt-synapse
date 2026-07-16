# synapse-adapter-kafka

Kafka wire protocol adapter for
**[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)**.

Translates Kafka produce/fetch/consumer-group semantics onto the `synapse-core` Log primitive
via the `synapse-routing` stream router, providing a drop-in replacement for a Kafka broker over
the same shared storage layer used by the MQTT, AMQP, and Redis adapters.

## Crate shape

| File | Role |
|------|------|
| `codec.rs` | Wire framing / (de)serialization |
| `broker.rs` | Protocol semantics against core primitives + routing |
| `server.rs` | TCP accept loop / connection handling |

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
