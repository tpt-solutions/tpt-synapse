# synapse-adapter-resp

Redis RESP wire protocol adapter for
**[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)**.

Translates Redis command semantics onto the `synapse-core` Map primitive (GET/SET/DEL/TTL/etc.),
providing a drop-in replacement for Redis over the same shared storage layer used by the MQTT,
Kafka, and AMQP adapters.

## Crate shape

| File | Role |
|------|------|
| `codec.rs` | Wire framing / (de)serialization |
| `broker.rs` | Protocol semantics against core primitives + routing |
| `server.rs` | TCP accept loop / connection handling |

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
