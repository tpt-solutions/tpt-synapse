//! Standalone client SDK for the native tpt-synapse protocol (TODO.md "Parallel
//! Track — Native tpt-synapse Protocol": *Native client SDK: Rust, plus at
//! least one other language binding*).
//!
//! The wire codec, AEAD frame encryption, CRC pre-auth filter, replay/counter
//! check, and the asymmetric X25519 session handshake all live in
//! `synapse-adapter-native` (the broker side). This crate is the client-facing
//! surface: it drives all four data primitives — pub/sub, log tailing /
//! consumer groups, queue + ack, and KV with TTL — over a single connection,
//! reusing the exact same [`Codec`] a real client would, so the SDK exercises
//! the identical AEAD + replay path as the broker's own tests.
//!
//! The server loop used by the integration test is the broker's own
//! [`native_broker::serve`], so the SDK is verified end-to-end against the real
//! `Log`/`Queue`/`Map` primitives and routing engines.

use std::io;
use std::net::SocketAddr;

use synapse_adapter_native::{Codec, Frame, NativeError, Opcode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Errors surfaced by the client: either a raw I/O failure on the socket or a
/// protocol-level rejection from the [`Codec`] (bad magic, CRC mismatch, auth
/// failure, replay, unknown key).
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] NativeError),
}

/// A connected native client. One instance owns one TCP connection; all four
/// primitive families are multiplexed over it.
pub struct NativeClient {
    stream: tokio::net::TcpStream,
    codec: Codec,
    counter: u32,
    buf: Vec<u8>,
}

impl NativeClient {
    /// Connect to a broker at `addr` using the shared `codec` (which carries
    /// the boot-salt and `KeyRing` agreed out-of-band with the server — the
    /// SDK itself does not provision keys).
    pub async fn connect(addr: SocketAddr, codec: Codec) -> Result<Self, ClientError> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            codec,
            counter: 0,
            buf: Vec::new(),
        })
    }

    /// Send one frame and return the broker's direct response frame.
    pub async fn exchange(&mut self, opcode: Opcode, payload: Vec<u8>) -> Result<Frame, ClientError> {
        self.counter = self.counter.wrapping_add(1);
        let mut out = Vec::new();
        self.codec.encode(
            &Frame {
                key_id: 0,
                opcode,
                counter: self.counter,
                payload,
            },
            &mut out,
        );
        self.stream.write_all(&out).await?;
        self.stream.flush().await?;
        self.recv().await
    }

    /// Read the next inbound frame. Used for unsolicited pub/sub deliveries and
    /// any response not tied to a just-sent request. The read buffer persists
    /// across calls so pipelined frames coalesced into one TCP read aren't lost.
    pub async fn recv(&mut self) -> Result<Frame, ClientError> {
        let mut tmp = [0u8; 8192];
        loop {
            if let Some(f) = self.codec.decode(&mut self.buf)? {
                return Ok(f);
            }
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(ClientError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed connection",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    // --- KV (Map) --------------------------------------------------------

    /// Set `key` to `value` with an optional TTL in milliseconds (0 = no TTL).
    pub async fn kv_set(&mut self, key: &str, ttl_ms: u64, value: &[u8]) -> Result<bool, ClientError> {
        let r = self
            .exchange(Opcode::KvSet, encode_kv(key, ttl_ms, value))
            .await?;
        Ok(r.payload.first() == Some(&1))
    }

    /// Get `key`. Returns `None` on miss; otherwise the stored value.
    pub async fn kv_get(&mut self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        let r = self.exchange(Opcode::KvGet, encode_kv(key, 0, &[])).await?;
        if r.payload.first() == Some(&1) {
            let len = u32::from_be_bytes(r.payload[1..5].try_into().unwrap()) as usize;
            Ok(Some(r.payload[5..5 + len].to_vec()))
        } else {
            Ok(None)
        }
    }

    // --- Queue + Ack (Queue) --------------------------------------------

    /// Enqueue `payload` onto `name`. Returns the assigned sequence number.
    pub async fn enqueue(&mut self, name: &str, payload: &[u8]) -> Result<u64, ClientError> {
        let r = self
            .exchange(Opcode::Queue, encode_named(0, name, payload))
            .await?;
        if r.payload.first() == Some(&1) {
            Ok(u64::from_be_bytes(r.payload[1..9].try_into().unwrap()))
        } else {
            Err(ClientError::Protocol(NativeError::Truncated))
        }
    }

    /// Dequeue the next item from `name`, if any.
    pub async fn dequeue(&mut self, name: &str) -> Result<Option<(u64, Vec<u8>)>, ClientError> {
        let r = self.exchange(Opcode::Queue, encode_named(1, name, &[])).await?;
        if r.payload.first() == Some(&1) {
            let seq = u64::from_be_bytes(r.payload[1..9].try_into().unwrap());
            let len = u32::from_be_bytes(r.payload[9..13].try_into().unwrap()) as usize;
            Ok(Some((seq, r.payload[13..13 + len].to_vec())))
        } else {
            Ok(None)
        }
    }

    /// Acknowledge a previously dequeued item by `seq`.
    pub async fn ack(&mut self, name: &str, seq: u64) -> Result<bool, ClientError> {
        let rest = seq.to_be_bytes().to_vec();
        let r = self
            .exchange(Opcode::Ack, encode_named(0, name, &rest))
            .await?;
        Ok(r.payload.first() == Some(&1))
    }

    // --- Pub/Sub (TopicRouter) ------------------------------------------

    /// Subscribe this connection to `filter` (MQTT wildcard contract).
    pub async fn subscribe(&mut self, filter: &str) -> Result<(), ClientError> {
        self.exchange(Opcode::PubSub, encode_named(1, filter, &[]))
            .await?;
        Ok(())
    }

    /// Publish `payload` to `topic`. Returns once the broker acks; any matching
    /// delivery frames arrive separately via [`NativeClient::recv`].
    pub async fn publish(&mut self, topic: &str, payload: &[u8]) -> Result<(), ClientError> {
        self.exchange(Opcode::PubSub, encode_named(0, topic, payload))
            .await?;
        Ok(())
    }

    // --- Log tailing / consumer groups (Log + StreamRouter) ------------

    /// Create a log `topic` with `partitions` partitions.
    pub async fn log_create(&mut self, topic: &str, partitions: u32) -> Result<bool, ClientError> {
        let r = self
            .exchange(Opcode::LogTail, encode_named(2, topic, &partitions.to_be_bytes()))
            .await?;
        Ok(r.payload.first() == Some(&1))
    }

    /// Append `payload` to `topic`. Returns the assigned offset.
    pub async fn log_append(&mut self, topic: &str, payload: &[u8]) -> Result<u64, ClientError> {
        let r = self
            .exchange(Opcode::LogTail, encode_named(0, topic, payload))
            .await?;
        if r.payload.first() == Some(&1) {
            Ok(u64::from_be_bytes(r.payload[1..9].try_into().unwrap()))
        } else {
            Err(ClientError::Protocol(NativeError::Truncated))
        }
    }

    /// Consume up to `max` records from `topic` as consumer `group`.
    pub async fn log_consume(
        &mut self,
        topic: &str,
        group: &str,
        max: u32,
    ) -> Result<Vec<(u64, Vec<u8>)>, ClientError> {
        let mut rest = Vec::new();
        rest.extend_from_slice(&(group.len() as u16).to_be_bytes());
        rest.extend_from_slice(group.as_bytes());
        rest.extend_from_slice(&max.to_be_bytes());
        let r = self
            .exchange(Opcode::LogTail, encode_named(1, topic, &rest))
            .await?;
        if r.payload.first() != Some(&1) {
            return Ok(Vec::new());
        }
        let count = u32::from_be_bytes(r.payload[1..5].try_into().unwrap()) as usize;
        let mut out = Vec::with_capacity(count);
        let mut cur = 5;
        for _ in 0..count {
            let off = u64::from_be_bytes(r.payload[cur..cur + 8].try_into().unwrap());
            let len = u32::from_be_bytes(r.payload[cur + 8..cur + 12].try_into().unwrap()) as usize;
            let data = r.payload[cur + 12..cur + 12 + len].to_vec();
            out.push((off, data));
            cur += 12 + len;
        }
        Ok(out)
    }
}

// --- wire payload helpers (mirror the broker's encodings) ----------------

/// `action || name_len(2) || name || rest`.
fn encode_named(action: u8, name: &str, rest: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.push(action);
    p.extend_from_slice(&(name.len() as u16).to_be_bytes());
    p.extend_from_slice(name.as_bytes());
    p.extend_from_slice(rest);
    p
}

/// `key_len(2) || key || ttl_millis(8) || value`.
fn encode_kv(key: &str, ttl_ms: u64, value: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&(key.len() as u16).to_be_bytes());
    p.extend_from_slice(key.as_bytes());
    p.extend_from_slice(&ttl_ms.to_be_bytes());
    p.extend_from_slice(value);
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use synapse_adapter_native::native_broker;
    use synapse_adapter_native::KeyRing;
    use synapse_core::SynapseCore;
    use tokio::net::TcpListener;

    fn test_codec() -> (Codec, Codec) {
        let salt = [3u8, 1, 4, 1];
        let mut kr = KeyRing::new();
        kr.insert(0, [2u8; 32]);
        (Codec::new(salt, kr.clone()), Codec::new(salt, kr))
    }

    #[tokio::test]
    async fn sdk_drives_all_four_primitives_over_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (srv, cli) = test_codec();
        let core = Arc::new(SynapseCore::new());

        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            native_broker::serve(sock, srv, core, 0).await.unwrap();
        });

        let mut c = NativeClient::connect(addr, cli).await.unwrap();

        // --- KV (Map) -----------------------------------------------------
        assert!(c.kv_set("hello", 0, b"world").await.unwrap());
        assert_eq!(c.kv_get("hello").await.unwrap().unwrap(), b"world");
        assert_eq!(c.kv_get("missing").await.unwrap(), None);

        // --- Queue + Ack (Queue) -----------------------------------------
        let seq = c.enqueue("jobs", b"task-a").await.unwrap();
        let (got_seq, got_payload) = c.dequeue("jobs").await.unwrap().unwrap();
        assert_eq!(got_seq, seq);
        assert_eq!(got_payload, b"task-a");
        assert!(c.ack("jobs", seq).await.unwrap());

        // --- Pub/Sub (TopicRouter) ---------------------------------------
        c.subscribe("sensors/#").await.unwrap();
        c.publish("sensors/temp", b"21.5").await.unwrap();
        let delivery = c.recv().await.unwrap();
        assert_eq!(delivery.opcode, Opcode::PubSub);
        assert_eq!(delivery.payload[0], 2); // delivery marker
        let topic_len = u16::from_be_bytes([delivery.payload[1], delivery.payload[2]]) as usize;
        let topic = &delivery.payload[3..3 + topic_len];
        assert_eq!(topic, b"sensors/temp");
        assert_eq!(&delivery.payload[3 + topic_len..], b"21.5");

        // --- Log tailing + consumer group (Log + StreamRouter) -----------
        assert!(c.log_create("events", 1).await.unwrap());
        // The Log primitive shares one global WAL across all primitives, so the
        // assigned offset is not fixed; verify the record round-trips through a
        // consumer group instead.
        c.log_append("events", b"evt-1").await.unwrap();
        let recs = c.log_consume("events", "g1", 10).await.unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].1, b"evt-1".to_vec());
    }
}
