# synapse-adapter-native

Native encrypted protocol adapter for
**[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)** — a from-scratch,
non-blocking parallel protocol track designed for first-class security.

## Security model

- Mandatory **ChaCha20-Poly1305 AEAD** on every frame — no unencrypted carve-out
- **CRC pre-auth filter** to reject corrupt frames before decryption
- **AAD-bound header fields** — authenticated additional data covers all header metadata
- **Boot-salt + counter nonces** with epoch-based key rotation
- **X25519 asymmetric rekey handshake** for forward secrecy

## Multiplexed primitives

One connection carries all four core primitives over a single `Opcode` enum — no per-protocol
translation layer needed:

| Operation | Maps to |
|-----------|---------|
| Pub/sub | `synapse-core` Log |
| Log tail / consumer groups | `synapse-core` Log |
| Queue + ack | `synapse-core` Queue |
| Key-value | `synapse-core` Map |

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
