# synapse-native-client

Client SDK for the **[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)** native
encrypted protocol.

Wraps `synapse-adapter-native` with a high-level async API — connect to a tpt-synapse broker
and use pub/sub, log tailing, consumer groups, queue+ack, and key-value operations over a
single encrypted connection (ChaCha20-Poly1305 AEAD, X25519 key exchange).

## Quick start

```rust
use synapse_native_client::Client;

let client = Client::connect("127.0.0.1:7171").await?;
client.publish("sensors/temp", b"23.4").await?;
```

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
