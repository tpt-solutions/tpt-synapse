# synapse-adapter-amqp

AMQP 0-9-1 wire protocol adapter for
**[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)**.

Translates AMQP exchange/queue/binding semantics onto the `synapse-core` Queue primitive via
the `synapse-routing` graph router, providing a drop-in replacement for RabbitMQ over the same
shared storage layer used by the MQTT, Kafka, and Redis adapters.

## Crate shape

| File | Role |
|------|------|
| `codec.rs` | Wire framing / (de)serialization |
| `broker.rs` | Protocol semantics against core primitives + routing |
| `server.rs` | TCP accept loop / connection handling |

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
