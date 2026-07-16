# synapse-adapter-mqtt

MQTT v3.1.1 and v5.0 wire protocol adapter for
**[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)**.

Both versions share one `Packet` enum and codec — the protocol version is negotiated once on
`CONNECT`. The adapter translates MQTT publish/subscribe semantics onto the `synapse-core` Log and
Queue primitives via the `synapse-routing` topic router.

## Crate shape

| File | Role |
|------|------|
| `codec.rs` | Wire framing / (de)serialization |
| `broker.rs` | Protocol semantics against core primitives + routing |
| `server.rs` | TCP accept loop / connection handling |

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
