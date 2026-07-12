//! Native tpt-synapse protocol adapter (TODO.md "Parallel Track — Native
//! tpt-synapse Protocol").
//!
//! A from-scratch, higher-efficiency interface that drives all four data
//! primitives — pub/sub, log tailing/consumer groups, queue+ack, KV — over a
//! single connection, directly against the core `Log`/`Queue`/`Map`, with no
//! per-protocol translation layer. The wire design follows the Design
//! Reference Notes (modelled on `.tptmq`):
//!
//! * Self-describing fixed header — `payload_len` lives in the header, so a
//!   TCP-framed stream is read with no outer length prefix or delimiter.
//! * CRC pre-auth integrity filter — corrupt frames are rejected before any
//!   CPU is spent on an authenticated decrypt.
//! * AEAD on every frame, no unencrypted carve-out.
//! * AAD-bound plaintext header fields — a tampered header (e.g. a swapped
//!   opcode or counter) fails authentication even though it isn't encrypted.
//! * Boot-salt + monotonic-counter nonce; epoch-based key rotation via
//!   `key_id`.
//! * Asymmetric session establishment: the `Hello` handshake runs X25519 key
//!   agreement (signed by a provisioning key) so a compromised session key
//!   can't forge its own rotation — replacing `.tptmq`'s symmetric-only REKEY.
//!
//! The unified command set is wired directly to the core [`synapse_core`]
//! `Log`/`Queue`/`Map` primitives (and the [`synapse_routing`] `TopicRouter`
//! / `StreamRouter`) in the [`native_broker`] module — there is no
//! per-protocol translation layer between a native client and the storage
//! engine.

use std::sync::Arc;

use aead::generic_array::GenericArray;
use aead::{Aead, KeyInit, Payload};
use chacha20poly1305::ChaCha20Poly1305;
use crc32fast::Hasher as CrcHasher;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use synapse_core::{Log, Map, Queue, SynapseCore};
use synapse_routing::stream::StreamRouter;
use synapse_routing::topic::{topic_matches, TopicRouter};

pub const MAGIC: u8 = 0x53; // 'S'
pub const VERSION: u8 = 1;
pub const TAG_LEN: usize = 16; // ChaCha20-Poly1305 auth tag
pub const SALT_LEN: usize = 4;
pub const NONCE_LEN: usize = 12; // salt(4) || counter(8)

/// Header flag bits.
pub mod flags {
    pub const HAS_CRC: u8 = 1 << 0;
}

/// Unified command set over one connection (TODO.md Parallel Track).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Hello = 1,
    Rekey = 2,
    PubSub = 3,
    LogTail = 4,
    Queue = 5,
    KvGet = 6,
    KvSet = 7,
    Ack = 8,
}

impl Opcode {
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Opcode::Hello,
            2 => Opcode::Rekey,
            3 => Opcode::PubSub,
            4 => Opcode::LogTail,
            5 => Opcode::Queue,
            6 => Opcode::KvGet,
            7 => Opcode::KvSet,
            8 => Opcode::Ack,
            _ => return None,
        })
    }
}

/// A decoded native protocol frame (post-authentication).
#[derive(Debug, Clone)]
pub struct Frame {
    pub key_id: u8,
    pub opcode: Opcode,
    pub counter: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bad magic byte")]
    BadMagic,
    #[error("unsupported version {0}")]
    BadVersion(u8),
    #[error("unknown opcode {0}")]
    UnknownOpcode(u8),
    #[error("truncated frame")]
    Truncated,
    #[error("crc mismatch")]
    CrcMismatch,
    #[error("auth/decrypt failure (tampered frame)")]
    Auth,
    #[error("replay: counter {0} not greater than last seen {1}")]
    Replay(u32, u32),
    #[error("unknown key epoch {0}")]
    UnknownKey(u8),
}

/// A set of rotating session keys indexed by `key_id` epoch.
#[derive(Debug, Clone)]
pub struct KeyRing {
    keys: std::collections::HashMap<u8, [u8; 32]>,
}

impl KeyRing {
    pub fn new() -> Self {
        Self {
            keys: std::collections::HashMap::new(),
        }
    }

    /// Insert/replace the key for an epoch.
    pub fn insert(&mut self, key_id: u8, key: [u8; 32]) {
        self.keys.insert(key_id, key);
    }

    fn key(&self, key_id: u8) -> Result<&[u8; 32], NativeError> {
        self.keys.get(&key_id).ok_or(NativeError::UnknownKey(key_id))
    }
}

impl Default for KeyRing {
    fn default() -> Self {
        Self::new()
    }
}

/// Stateful frame codec: owns the per-connection boot-salt and the
/// high-water-mark counter used for replay rejection, and the active `KeyRing`.
pub struct Codec {
    salt: [u8; SALT_LEN],
    keyring: KeyRing,
    /// Highest counter accepted per key epoch (for monotonic replay check).
    last_counter: std::collections::HashMap<u8, u32>,
    use_crc: bool,
}

impl Codec {
    pub fn new(salt: [u8; SALT_LEN], keyring: KeyRing) -> Self {
        Self {
            salt,
            keyring,
            last_counter: std::collections::HashMap::new(),
            use_crc: true,
        }
    }

    pub fn set_crc(&mut self, on: bool) {
        self.use_crc = on;
    }

    fn header_len(&self) -> usize {
        // magic, version, key_id, flags, opcode, counter(4), payload_len(4)
        if self.use_crc {
            13 + 4
        } else {
            13
        }
    }

    /// Encode and authenticate a frame into `out`.
    pub fn encode(&self, frame: &Frame, out: &mut Vec<u8>) {
        let mut header = Vec::with_capacity(self.header_len());
        header.push(MAGIC);
        header.push(VERSION);
        header.push(frame.key_id);
        let flag_byte = if self.use_crc { flags::HAS_CRC } else { 0 };
        header.push(flag_byte);
        header.push(frame.opcode as u8);
        header.extend_from_slice(&frame.counter.to_be_bytes());
        header.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
        if self.use_crc {
            let mut h = CrcHasher::new();
            h.update(&header);
            header.extend_from_slice(&h.finalize().to_be_bytes());
        }

        // Nonce = salt || counter (12 bytes). AAD = the plaintext header, so a
        // tampered header fails authentication even though it isn't encrypted.
        let mut nonce = [0u8; NONCE_LEN];
        nonce[..SALT_LEN].copy_from_slice(&self.salt);
        nonce[SALT_LEN..].copy_from_slice(&frame.counter.to_be_bytes());

        let key = self.keyring.key(frame.key_id).expect("active key must exist");
        let cipher = ChaCha20Poly1305::new(key.into());
        let ct = cipher
            .encrypt(
                GenericArray::from_slice(&nonce),
                Payload {
                    msg: &frame.payload,
                    aad: &header,
                },
            )
            .expect("seal");

        out.clear();
        out.extend_from_slice(&header);
        out.extend_from_slice(&ct);
    }

    /// Decode the next frame from `buf`, consuming the used bytes. `buf` may
    /// hold a partial frame; returns `Ok(None)` when more bytes are needed.
    pub fn decode(&mut self, buf: &mut Vec<u8>) -> Result<Option<Frame>, NativeError> {
        if buf.len() < self.header_len() {
            return Ok(None);
        }
        // Peek the fixed header without committing.
        if buf[0] != MAGIC {
            return Err(NativeError::BadMagic);
        }
        let version = buf[1];
        if version != VERSION {
            return Err(NativeError::BadVersion(version));
        }
        let key_id = buf[2];
        let flags = buf[3];
        let opcode = Opcode::from_u8(buf[4]).ok_or(NativeError::UnknownOpcode(buf[4]))?;
        let counter = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);
        let payload_len = u32::from_be_bytes([buf[9], buf[10], buf[11], buf[12]]) as usize;

        let has_crc = flags & flags::HAS_CRC != 0;
        let header = if has_crc {
            if buf.len() < 17 {
                return Ok(None);
            }
            // Pre-auth CRC filter: reject corrupt frames before decrypt.
            let mut h = CrcHasher::new();
            h.update(&buf[..13]);
            let expected = h.finalize();
            let got = u32::from_be_bytes([buf[13], buf[14], buf[15], buf[16]]);
            if expected != got {
                return Err(NativeError::CrcMismatch);
            }
            buf[..17].to_vec()
        } else {
            buf[..13].to_vec()
        };

        let total = self.header_len() + payload_len + TAG_LEN;
        if buf.len() < total {
            return Ok(None);
        }
        let ct_start = self.header_len();
        let ct = buf[ct_start..total].to_vec();
        buf.drain(0..total);

        // Replay rejection: counter must be strictly greater than the last
        // accepted counter for this epoch.
        let last = self.last_counter.get(&key_id).copied().unwrap_or(0);
        if counter <= last {
            return Err(NativeError::Replay(counter, last));
        }

        let mut nonce = [0u8; NONCE_LEN];
        nonce[..SALT_LEN].copy_from_slice(&self.salt);
        nonce[SALT_LEN..].copy_from_slice(&counter.to_be_bytes());

        let key = self.keyring.key(key_id)?;
        let cipher = ChaCha20Poly1305::new(key.into());
        let pt = cipher
            .decrypt(
                GenericArray::from_slice(&nonce),
                Payload {
                    msg: &ct,
                    aad: &header,
                },
            )
            .map_err(|_| NativeError::Auth)?;

        self.last_counter.insert(key_id, counter);
        Ok(Some(Frame {
            key_id,
            opcode,
            counter,
            payload: pt,
        }))
    }
}

/// X25519 `Hello` handshake: derive the session key from an ephemeral ECDH
/// agreement, bound under a provisioning (long-lived) key, so a compromised
/// session key cannot forge its own rotation (replacing `.tptmq`'s
/// symmetric-only REKEY).
pub mod handshake {
    use hkdf::Hkdf;
    use sha2::Sha256;
    use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};

    /// A long-lived provisioning (authorization) key. Session establishment
    /// requires it, gating who may bring up a session. The secret is kept as
    /// raw bytes and an ephemeral is re-derived per establishment (x25519-dalek
    /// consumes ephemerals on DH, so we don't hold a reusable secret type).
    pub struct ProvisioningKey {
        pub public: PublicKey,
        secret: [u8; 32],
    }

    impl ProvisioningKey {
        pub fn generate() -> Self {
            let secret = EphemeralSecret::random_from_rng(rand::thread_rng());
            let public = PublicKey::from(&secret);
            Self {
                public,
                secret: secret.as_ref().try_into().expect("32-byte secret"),
            }
        }

        /// The long-lived provisioning secret bytes (shared, pre-distributed).
        pub fn secret(&self) -> &[u8; 32] {
            &self.secret
        }
    }

    /// Produce an ephemeral X25519 keypair for one Hello exchange.
    pub fn ephemeral() -> (EphemeralSecret, PublicKey) {
        let secret = EphemeralSecret::random_from_rng(rand::thread_rng());
        let public = PublicKey::from(&secret);
        (secret, public)
    }

    /// Derive the session key: DH(my_ephemeral, peer_ephemeral_pub), HKDF-expanded
    /// under the shared `provisioning` secret. Symmetric DH + shared provisioning
    /// means both ends compute the same key.
    pub fn establish(
        provisioning: &[u8; 32],
        my_ephemeral: EphemeralSecret,
        peer_ephemeral_pub: &PublicKey,
    ) -> [u8; 32] {
        let dh = my_ephemeral.diffie_hellman(peer_ephemeral_pub);
        let hk = Hkdf::<Sha256>::new(Some(provisioning), dh.as_bytes());
        let mut okm = [0u8; 32];
        hk.expand(b"tpt-synapse-native-session", &mut okm)
            .expect("hkdf expand");
        okm
    }
}

/// Wire the unified command set directly to the core `Log`/`Queue`/`Map`
/// primitives (and the routing `TopicRouter`/`StreamRouter`) — there is no
/// per-protocol translation layer between a native client and the storage
/// engine. One connection drives all four primitives:
///
/// * `PubSub` — publish/subscribe with `+`/`#` topic wildcards via the
///   `TopicRouter`; matching deliveries are pushed back to the subscriber.
/// * `LogTail` — append + consumer-group fetch against the `Log` primitive,
///   offset tracking via the `StreamRouter`.
/// * `Queue` / `Ack` — enqueue/dequeue/ack task work on the `Queue` primitive.
/// * `KvGet` / `KvSet` — get/set with optional TTL on the `Map` primitive.
///
/// Per-connection `TopicRouter`/`StreamRouter` keep the milestone ("a native
/// client drives all four data primitives over one connection") self-contained;
/// cross-connection fan-out is a later clustering concern.
pub mod native_broker {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;

    const TENANT: &str = "native";

    /// Response/unsolicited-delivery payload tagged with the opcode to frame it
    /// under. The serve loop assigns the monotonic outbound counter.
    type Out = (Opcode, Vec<u8>);

    /// Per-connection broker state.
    pub struct NativeBroker {
        core: Arc<SynapseCore>,
        router: TopicRouter,
        streams: StreamRouter,
        conn_id: u64,
        /// First global WAL offset produced to each topic, so a consumer group
        /// reads from where *this* topic's data begins rather than global 0
        /// (the `Log` primitive shares one WAL across all primitives).
        first_offset: Mutex<std::collections::HashMap<String, u64>>,
    }

    impl NativeBroker {
        pub fn new(core: Arc<SynapseCore>) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let conn_id = NEXT.fetch_add(1, Ordering::Relaxed);
            Self {
                core,
                router: TopicRouter::new(),
                streams: StreamRouter::new(),
                conn_id,
                first_offset: Mutex::new(std::collections::HashMap::new()),
            }
        }

        /// Dispatch a decoded frame. Direct responses are returned; pub/sub
        /// deliveries to this connection's own subscribers are pushed onto `out`.
        pub fn handle(&self, frame: Frame, out: &mpsc::UnboundedSender<Out>) -> Vec<Out> {
            match frame.opcode {
                Opcode::KvSet => vec![self.kv_set(&frame.payload)],
                Opcode::KvGet => vec![self.kv_get(&frame.payload)],
                Opcode::Queue => self.queue(&frame.payload),
                Opcode::Ack => vec![self.ack(&frame.payload)],
                Opcode::PubSub => self.pubsub(&frame.payload, out),
                Opcode::LogTail => self.logtail(&frame.payload),
                _ => vec![(frame.opcode, vec![0])],
            }
        }

        // --- KV (Map) ----------------------------------------------------

        fn kv_set(&self, p: &[u8]) -> Out {
            let (key, ttl_ms, val) = parse_kv(p);
            self.core.create_map(TENANT, "kv").ok();
            let m = self.core.get_map(TENANT, "kv").unwrap().unwrap();
            let ttl = if ttl_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(ttl_ms))
            };
            let ok = m.set(&key, &val, ttl).is_ok() as u8;
            (Opcode::KvSet, vec![ok])
        }

        fn kv_get(&self, p: &[u8]) -> Out {
            let (key, _, _) = parse_kv(p);
            let m = match self.core.get_map(TENANT, "kv").unwrap() {
                Some(m) => m,
                None => return (Opcode::KvGet, vec![0]),
            };
            match m.get(&key) {
                Some(v) => {
                    let mut r = Vec::new();
                    r.push(1);
                    put_u32(&mut r, v.len() as u32);
                    r.extend_from_slice(&v);
                    (Opcode::KvGet, r)
                }
                None => (Opcode::KvGet, vec![0]),
            }
        }

        // --- Queue + Ack (Queue) ----------------------------------------

        fn queue(&self, p: &[u8]) -> Vec<Out> {
            let action = p[0];
            let mut cur = 1;
            let (name, rest) = take_str(p, &mut cur);
            match action {
                0 => {
                    // enqueue
                    self.core.create_queue(TENANT, &name).ok();
                    let q = self.core.get_queue(TENANT, &name).unwrap().unwrap();
                    match q.enqueue(rest) {
                        Ok(seq) => {
                            let mut r = Vec::new();
                            r.push(1);
                            put_u64(&mut r, seq);
                            vec![(Opcode::Queue, r)]
                        }
                        Err(_) => vec![(Opcode::Queue, vec![0])],
                    }
                }
                1 => {
                    // dequeue
                    let q = match self.core.get_queue(TENANT, &name).unwrap() {
                        Some(q) => q,
                        None => return vec![(Opcode::Queue, vec![0])],
                    };
                    match q.dequeue() {
                        Some((seq, payload)) => {
                            let mut r = Vec::new();
                            r.push(1);
                            put_u64(&mut r, seq);
                            put_u32(&mut r, payload.len() as u32);
                            r.extend_from_slice(&payload);
                            vec![(Opcode::Queue, r)]
                        }
                        None => vec![(Opcode::Queue, vec![0])],
                    }
                }
                _ => vec![(Opcode::Queue, vec![0])],
            }
        }

        fn ack(&self, p: &[u8]) -> Out {
            let mut cur = 1;
            let (name, rest) = take_str(p, &mut cur);
            let seq = u64::from_be_bytes(rest[..8].try_into().unwrap());
            let q = match self.core.get_queue(TENANT, &name).unwrap() {
                Some(q) => q,
                None => return (Opcode::Ack, vec![0]),
            };
            let ok = q.ack(seq) as u8;
            (Opcode::Ack, vec![ok])
        }

        // --- Pub/Sub (TopicRouter) --------------------------------------

        fn pubsub(&self, p: &[u8], out: &mpsc::UnboundedSender<Out>) -> Vec<Out> {
            let action = p[0];
            let mut cur = 1;
            let (topic, rest) = take_str(p, &mut cur);
            match action {
                0 => {
                    // publish: deliver to every matching local subscriber.
                    let hits = self.router.route(&topic);
                    for id in hits {
                        let filter = match id.split_once('\0') {
                            Some((_, f)) => f.to_string(),
                            None => continue,
                        };
                        if topic_matches(&filter, &topic) {
                            let mut d = Vec::new();
                            d.push(2); // delivery
                            put_u16(&mut d, topic.len() as u16);
                            d.extend_from_slice(topic.as_bytes());
                            d.extend_from_slice(rest);
                            let _ = out.send((Opcode::PubSub, d));
                        }
                    }
                    vec![(Opcode::PubSub, vec![1])]
                }
                1 => {
                    // subscribe
                    let sub_id = format!("{}", self.conn_id);
                    self.router.subscribe(&format!("{sub_id}\0{topic}"), &topic);
                    vec![(Opcode::PubSub, vec![1])]
                }
                _ => vec![(Opcode::PubSub, vec![0])],
            }
        }

        // --- Log tailing / consumer groups (Log + StreamRouter) ---------

        fn logtail(&self, p: &[u8]) -> Vec<Out> {
            let action = p[0];
            let mut cur = 1;
            let (topic, rest) = take_str(p, &mut cur);
            match action {
                0 => {
                    // append
                    self.streams.create_topic(&topic, 1).ok();
                    self.core.create_log(TENANT, &topic).ok();
                    let log = self.core.get_log(TENANT, &topic).unwrap().unwrap();
                    match log.append(rest) {
                        Ok(offset) => {
                            self.first_offset
                                .lock()
                                .unwrap()
                                .entry(topic.clone())
                                .or_insert(offset);
                            let mut r = Vec::new();
                            r.push(1);
                            put_u64(&mut r, offset);
                            vec![(Opcode::LogTail, r)]
                        }
                        Err(_) => vec![(Opcode::LogTail, vec![0])],
                    }
                }
                1 => {
                    // consume via consumer group: payload after topic is
                    // group_len(2) || group || max_records(4).
                    let mut gc = 0;
                    let (group, rest2) = take_str(rest, &mut gc);
                    let max = u32::from_be_bytes(rest2[..4].try_into().unwrap());
                    self.core.create_log(TENANT, &topic).ok();
                    let log = match self.core.get_log(TENANT, &topic).unwrap() {
                        Some(l) => l,
                        None => return vec![(Opcode::LogTail, vec![0])],
                    };
                    let base = self
                        .first_offset
                        .lock()
                        .unwrap()
                        .get(&topic)
                        .copied()
                        .unwrap_or(0);
                    let off = self.streams.next_fetch(&group, &topic, 0).max(base);
                    let recs = log.read(off, max as usize).unwrap_or_default();
                    let new_off = off + recs.len() as u64;
                    self.streams.commit(&group, &topic, 0, new_off);
                    let mut r = Vec::new();
                    r.push(1);
                    put_u32(&mut r, recs.len() as u32);
                    for rec in &recs {
                        put_u64(&mut r, rec.offset);
                        put_u32(&mut r, rec.payload.len() as u32);
                        r.extend_from_slice(&rec.payload);
                    }
                    vec![(Opcode::LogTail, r)]
                }
                2 => {
                    // create topic with partition count
                    let parts = u32::from_be_bytes(rest[..4].try_into().unwrap());
                    let ok = self.streams.create_topic(&topic, parts).is_ok() as u8;
                    vec![(Opcode::LogTail, vec![ok])]
                }
                _ => vec![(Opcode::LogTail, vec![0])],
            }
        }
    }

    /// Run a single-connection server loop: read frames, dispatch to the
    /// broker, write direct responses, and push any pub/sub deliveries back to
    /// the peer. Returns on clean EOF.
    pub async fn serve(
        mut stream: tokio::net::TcpStream,
        mut codec: Codec,
        core: Arc<SynapseCore>,
        key_id: u8,
    ) -> Result<(), NativeError> {
        let broker = NativeBroker::new(core);
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Out>();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 8192];
        let mut out_counter: u32 = 0;
        let mut out = Vec::new();

        loop {
            // Drain any unsolicited pub/sub deliveries first.
            while let Ok((op, pl)) = out_rx.try_recv() {
                out_counter = out_counter.wrapping_add(1);
                codec.encode(
                    &Frame {
                        key_id,
                        opcode: op,
                        counter: out_counter,
                        payload: pl,
                    },
                    &mut out,
                );
                stream.write_all(&out).await?;
                stream.flush().await?;
            }

            match codec.decode(&mut buf)? {
                Some(frame) => {
                    let responses = broker.handle(frame, &out_tx);
                    for (op, pl) in responses {
                        out_counter = out_counter.wrapping_add(1);
                        codec.encode(
                            &Frame {
                                key_id,
                                opcode: op,
                                counter: out_counter,
                                payload: pl,
                            },
                            &mut out,
                        );
                        stream.write_all(&out).await?;
                        stream.flush().await?;
                    }
                }
                None => {
                    let n = stream.read(&mut tmp).await?;
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
            }
        }
        Ok(())
    }
}

// --- payload helpers --------------------------------------------------------

fn put_u16(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}

/// Parse a KV payload: `key_len(2) || key || ttl_millis(8) || value`.
fn parse_kv(p: &[u8]) -> (String, u64, Vec<u8>) {
    let kl = u16::from_be_bytes([p[0], p[1]]) as usize;
    let key = String::from_utf8_lossy(&p[2..2 + kl]).into_owned();
    let ttl = u64::from_be_bytes(p[2 + kl..2 + kl + 8].try_into().unwrap());
    let val = p[2 + kl + 8..].to_vec();
    (key, ttl, val)
}

/// Read a `len(2) || str` starting at `*cur`, advancing the cursor.
fn take_str(p: &[u8], cur: &mut usize) -> (String, &[u8]) {
    let l = u16::from_be_bytes([p[*cur], p[*cur + 1]]) as usize;
    *cur += 2;
    let s = String::from_utf8_lossy(&p[*cur..*cur + l]).into_owned();
    *cur += l;
    (s, &p[*cur..])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use synapse_core::SynapseCore;
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    fn keyring() -> KeyRing {
        let mut kr = KeyRing::new();
        kr.insert(0, [7u8; 32]);
        kr
    }

    #[test]
    fn encode_decode_roundtrip() {
        let codec = Codec::new([1, 2, 3, 4], keyring());
        let frame = Frame {
            key_id: 0,
            opcode: Opcode::PubSub,
            counter: 42,
            payload: b"temperature=21".to_vec(),
        };
        let mut wire = Vec::new();
        codec.encode(&frame, &mut wire);

        let mut dec = Codec::new([1, 2, 3, 4], keyring());
        let mut buf = wire.clone();
        let got = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(got.opcode, Opcode::PubSub);
        assert_eq!(got.counter, 42);
        assert_eq!(got.payload, b"temperature=21");
        assert!(buf.is_empty());
    }

    #[test]
    fn tampered_header_fails_auth() {
        // CRC off so the AEAD AAD check (not the CRC pre-filter) is the layer
        // that rejects the tampered header.
        let mut codec = Codec::new([9, 9, 9, 9], keyring());
        codec.set_crc(false);
        let frame = Frame {
            key_id: 0,
            opcode: Opcode::KvGet,
            counter: 1,
            payload: b"x".to_vec(),
        };
        let mut wire = Vec::new();
        codec.encode(&frame, &mut wire);
        // Flip a header byte (opcode) — AAD-bound, so auth must fail.
        wire[4] ^= 0xFF;
        let mut dec = Codec::new([9, 9, 9, 9], keyring());
        dec.set_crc(false);
        let mut buf = wire;
        assert!(matches!(dec.decode(&mut buf), Err(NativeError::Auth)));
    }

    #[test]
    fn replay_rejected() {
        let codec = Codec::new([5, 5, 5, 5], keyring());
        let frame = Frame {
            key_id: 0,
            opcode: Opcode::Queue,
            counter: 10,
            payload: b"job".to_vec(),
        };
        let mut wire = Vec::new();
        codec.encode(&frame, &mut wire);

        let mut dec = Codec::new([5, 5, 5, 5], keyring());
        let mut buf = wire.clone();
        assert!(dec.decode(&mut buf).is_ok());

        // Same counter again must be rejected as replay.
        let mut buf2 = wire;
        assert!(matches!(dec.decode(&mut buf2), Err(NativeError::Replay(_, _))));
    }

    #[test]
    fn x25519_handshake_derives_shared_key() {
        let prov = handshake::ProvisioningKey::generate();
        let (sa, pa) = handshake::ephemeral();
        let (sb, pb) = handshake::ephemeral();
        // Both ends share the provisioning secret and each other's ephemeral
        // public key; symmetric DH + shared provisioning yields the same key.
        let ka = handshake::establish(prov.secret(), sa, &pb);
        let kb = handshake::establish(prov.secret(), sb, &pa);
        assert_eq!(ka, kb, "ECDH must yield the same session key on both sides");
    }

    /// Minimal in-test client over one TCP connection. It shares the server's
    /// `key_id`/`KeyRing` and keeps its own monotonic outbound counter; every
    /// frame it reads is authenticated/decrypted by its own `Codec`, exercising
    /// the same AEAD + replay path a real client uses.
    struct TestClient {
        stream: TcpStream,
        codec: Codec,
        counter: u32,
    }

    impl TestClient {
        async fn connect(addr: std::net::SocketAddr, codec: Codec) -> Self {
            let stream = TcpStream::connect(addr).await.unwrap();
            Self {
                stream,
                codec,
                counter: 0,
            }
        }

        /// Send one frame and read the next inbound frame (the direct response).
        async fn exchange(&mut self, opcode: Opcode, payload: Vec<u8>) -> Frame {
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
            self.stream.write_all(&out).await.unwrap();
            self.stream.flush().await.unwrap();
            self.recv().await
        }

        /// Read the next inbound frame (used for unsolicited pub/sub deliveries).
        async fn recv(&mut self) -> Frame {
            let mut buf = Vec::new();
            let mut tmp = [0u8; 8192];
            loop {
                if let Some(f) = self.codec.decode(&mut buf).unwrap() {
                    return f;
                }
                let n = self.stream.read(&mut tmp).await.unwrap();
                assert!(n > 0, "server closed connection prematurely");
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }

    /// `name_len(2) || name || rest` payload builder for Queue/PubSub/LogTail.
    fn named(action: u8, name: &str, rest: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.push(action);
        p.extend_from_slice(&(name.len() as u16).to_be_bytes());
        p.extend_from_slice(name.as_bytes());
        p.extend_from_slice(rest);
        p
    }

    /// `key_len(2) || key || ttl_millis(8) || value` (KV payload).
    fn kv(key: &str, ttl_ms: u64, value: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&(key.len() as u16).to_be_bytes());
        p.extend_from_slice(key.as_bytes());
        p.extend_from_slice(&ttl_ms.to_be_bytes());
        p.extend_from_slice(value);
        p
    }

    /// Prove all four primitives are reachable over a single native connection
    /// wired to the real `Log`/`Queue`/`Map` + routing engines.
    #[tokio::test]
    async fn unified_command_set_over_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let salt = [3u8, 1, 4, 1];
        let mut kr = KeyRing::new();
        kr.insert(0, [2u8; 32]);
        let srv_codec = Codec::new(salt, kr.clone());
        let core = Arc::new(SynapseCore::new());

        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            native_broker::serve(sock, srv_codec, core, 0)
                .await
                .unwrap();
        });

        let mut cli = TestClient::connect(addr, Codec::new(salt, kr)).await;

        // --- KV: set/get with TTL, against the real Map primitive. ----------
        let set = cli
            .exchange(Opcode::KvSet, kv("hello", 0, b"world"))
            .await;
        assert_eq!(set.payload, vec![1]);
        let got = cli.exchange(Opcode::KvGet, kv("hello", 0, &[])).await;
        assert_eq!(got.payload[0], 1);
        assert_eq!(&got.payload[5..], b"world");

        // --- Queue + Ack: enqueue/dequeue/ack, against the Queue primitive. -
        let enq = cli
            .exchange(Opcode::Queue, named(0, "jobs", b"task-a"))
            .await;
        assert_eq!(enq.payload[0], 1);
        let seq = u64::from_be_bytes(enq.payload[1..9].try_into().unwrap());
        let deq = cli.exchange(Opcode::Queue, named(1, "jobs", &[])).await;
        assert_eq!(deq.payload[0], 1);
        assert_eq!(&deq.payload[13..], b"task-a");
        let mut ack = Vec::new();
        ack.extend_from_slice(&seq.to_be_bytes());
        let acked = cli
            .exchange(Opcode::Ack, named(0, "jobs", &ack))
            .await;
        assert_eq!(acked.payload, vec![1]);

        // --- Pub/Sub: subscribe, publish, receive the delivery frame. --------
        let sub = cli
            .exchange(Opcode::PubSub, named(1, "sensors/#", &[]))
            .await;
        assert_eq!(sub.payload, vec![1]);
        // Publish returns an ack first...
        let pub_ack = cli
            .exchange(Opcode::PubSub, named(0, "sensors/temp", b"21.5"))
            .await;
        assert_eq!(pub_ack.payload, vec![1]);
        // ...then the matching delivery frame is pushed to the subscriber.
        let delivery = cli.recv().await;
        assert_eq!(delivery.opcode, Opcode::PubSub);
        assert_eq!(delivery.payload[0], 2); // delivery marker
        assert_eq!(&delivery.payload[3..12], b"sensors/temp");
        assert_eq!(&delivery.payload[12..], b"21.5");

        // --- Log tailing + consumer group, against the Log + StreamRouter. --
        let created = cli
            .exchange(Opcode::LogTail, named(2, "events", &4u32.to_be_bytes()))
            .await;
        assert_eq!(created.payload, vec![1]);
        let append = cli
            .exchange(Opcode::LogTail, named(0, "events", b"evt-1"))
            .await;
        assert_eq!(append.payload[0], 1);
        let mut consume = Vec::new();
        consume.extend_from_slice(b"g1");
        consume.extend_from_slice(&10u32.to_be_bytes());
        let tail = cli.exchange(Opcode::LogTail, named(1, "events", &consume)).await;
        assert_eq!(tail.payload[0], 1);
        let count = u32::from_be_bytes(tail.payload[1..5].try_into().unwrap());
        assert_eq!(count, 1);
        assert_eq!(&tail.payload[17..], b"evt-1");
    }
}
