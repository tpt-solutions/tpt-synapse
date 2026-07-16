//! Real TCP transport + apply-loop for the embedded Raft core (TODO.md Phase 4
//! "Milestone").
//!
//! [`consensus`] is a pure state machine: it answers `RequestVote` /
//! `AppendEntries` and tracks replication progress, but the caller owns the
//! network transport, election timers, and the apply-loop. This module closes
//! that gap with a length-delimited JSON wire protocol, an async [`RaftServer`]
//! that services peer RPCs on a TCP listener, a driver loop that runs elections
//! and heartbeats, and an apply-loop that replays committed [`ReplicatedCommand`]
//! entries onto the local [`SynapseCore`] — so a multi-node cluster converges
//! end-to-end over real sockets, not an in-memory shim.
//!
//! This is the embeddable building block; the `openraft` production library
//! (named in the Phase 0 decision) can later replace the wire codec/state
//! machine while keeping this transport + apply-loop shape.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::cluster::{apply_command, decode_command, encode_command, ReplicatedCommand};
use crate::consensus::{Entry, MemoryStore, RaftNode, Role};
use crate::engine::SynapseCore;
use crate::error::{EngineError, EngineResult};

/// Length prefix on every wire frame (4-byte big-endian byte count).
const FRAME_LEN: usize = 4;

/// A request or response exchanged between Raft peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMessage {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        /// Highest log index the follower reports having (lets the leader
        /// fast-forward `match_index` without a separate RPC).
        match_index: u64,
    },
}

/// A running Raft node bound to a TCP listener, wired to a local engine.
pub struct RaftServer {
    node: Arc<Mutex<RaftNode>>,
    core: Arc<SynapseCore>,
    peer_addrs: Vec<(String, String)>,
    /// Our advertised listen address, so peers can reach us.
    pub addr: String,
    /// The inbound peer-RPC listener, bound once at construction and consumed
    /// by [`RaftServer::serve`] so the port is never re-bound (which could let
    /// another node claim it between binds).
    listener: std::sync::Mutex<Option<TcpListener>>,
}

impl RaftServer {
    /// Create a server with `id`, `peers` (other node ids), and a map from peer
    /// id to its `host:port`. Binds `listen_addr` for inbound peer RPCs and
    /// keeps the listener until [`RaftServer::serve`] starts accepting on it.
    pub async fn bind(
        id: String,
        peers: Vec<String>,
        peer_addrs: Vec<(String, String)>,
        listen_addr: &str,
        core: Arc<SynapseCore>,
    ) -> EngineResult<Self> {
        let listener = TcpListener::bind(listen_addr).await.map_err(|e| {
            EngineError::internal(format!("bind raft listener {listen_addr}: {e}"))
        })?;
        let store: Arc<dyn crate::consensus::StateStore> = MemoryStore::new();
        let node = Arc::new(Mutex::new(RaftNode::new(id.clone(), peers, store)));
        Ok(Self {
            node,
            core,
            peer_addrs,
            addr: listen_addr.to_string(),
            listener: std::sync::Mutex::new(Some(listener)),
        })
    }

    /// Append a command to the local log *if we are the leader*, returning its
    /// Raft index. Non-leaders reject the write (clients should be redirected to
    /// the leader in a full deployment).
    pub async fn propose(&self, cmd: &ReplicatedCommand) -> EngineResult<u64> {
        let mut node = self.node.lock().await;
        if node.role() != Role::Leader {
            return Err(EngineError::internal("not leader"));
        }
        Ok(node.leader_append(encode_command(cmd)))
    }

    /// True when this node currently holds leadership.
    pub async fn is_leader(&self) -> bool {
        self.node.lock().await.role() == Role::Leader
    }

    /// Apply every committed-but-not-yet-applied entry to the local engine. This
    /// is the apply-loop body, driven concurrently by [`RaftServer::apply_loop`].
    pub async fn apply_pending(&self) -> EngineResult<()> {
        let to_apply: Vec<Entry> = {
            let mut node = self.node.lock().await;
            let mut out = Vec::new();
            node.apply_committed(|e: &Entry| out.push(e.clone()));
            out
        };
        for e in &to_apply {
            let cmd = decode_command(&e.data)?;
            apply_command(&self.core, &cmd)?;
        }
        Ok(())
    }

    /// Spawn the apply-loop: continuously applies newly committed entries to the
    /// local engine. Returns the task handle.
    pub fn apply_loop(&self) -> JoinHandle<()> {
        let server = self.clone_for_task();
        tokio::spawn(async move {
            loop {
                if server.apply_pending().await.is_err() {
                    // Best-effort; a real deployment would surface this.
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
    }

    /// Spawn the inbound RPC listener: each accepted connection is served by
    /// [`handle_peer`]. Consumes the listener bound in [`RaftServer::bind`].
    /// Returns the task handle.
    pub fn serve(&self) -> JoinHandle<()> {
        let listener = self.listener.lock().unwrap().take();
        let listener = match listener {
            Some(l) => l,
            None => return tokio::spawn(async {}),
        };
        let server = self.clone_for_task();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((sock, _)) => {
                        let s = server.clone_for_task();
                        tokio::spawn(async move {
                            let _ = s.handle_peer(sock).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        })
    }

    /// Spawn the driver loop: runs election timeouts, heartbeats, and log
    /// replication. Returns the task handle.
    pub fn drive(&self) -> JoinHandle<()> {
        let server = self.clone_for_task();
        tokio::spawn(async move {
            server.drive_loop().await;
        })
    }

    async fn handle_peer(&self, mut sock: TcpStream) -> EngineResult<()> {
        // A peer connection carries exactly one RPC + its response.
        let msg = read_message(&mut sock).await?;
        let reply = match msg {
            WireMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                let (term, granted) = {
                    let mut node = self.node.lock().await;
                    node.handle_request_vote(term, &candidate_id, last_log_index, last_log_term)
                };
                WireMessage::RequestVoteResponse {
                    term,
                    vote_granted: granted,
                }
            }
            WireMessage::AppendEntries {
                term,
                leader_id: _,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                let match_index = if entries.is_empty() {
                    prev_log_index
                } else {
                    entries.last().map(|e| e.index).unwrap_or(0)
                };
                let (term, success) = {
                    let mut node = self.node.lock().await;
                    node.handle_append_entries(
                        term,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    )
                };
                let match_index = if success { match_index } else { 0 };
                WireMessage::AppendEntriesResponse {
                    term,
                    success,
                    match_index,
                }
            }
            other => {
                return Err(EngineError::internal(format!(
                    "unexpected peer message: {other:?}"
                )))
            }
        };
        write_message(&mut sock, &reply).await?;
        Ok(())
    }

    async fn drive_loop(&self) {
        let base = Duration::from_millis(150);
        let jitter = Duration::from_millis(150);
        let heartbeat = Duration::from_millis(50);
        let mut rng = 0x9E3779B97F4A7C15u64;
        loop {
            let role = self.node.lock().await.role();
            match role {
                Role::Follower | Role::Candidate => {
                    let extra = (rng % jitter.as_millis() as u64) as u64;
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    tokio::time::sleep(base + Duration::from_millis(extra)).await;
                    self.try_elect().await;
                }
                Role::Leader => {
                    self.heartbeat_and_replicate().await;
                    tokio::time::sleep(heartbeat).await;
                }
            }
        }
    }

    /// Run one election round from this node. Updates this node's role to Leader
    /// internally if a majority grants the vote.
    async fn try_elect(&self) {
        let ids = {
            let node = self.node.lock().await;
            node.peers().to_vec()
        };
        // Snapshot election args with the lock held ONLY for the brief
        // begin_election call, then release it for the network RPCs. Holding
        // the lock across call_peer would deadlock the cluster: the peer
        // answers our RPC by locking ITS OWN node, and we'd be holding ours
        // while waiting on it (lock-ordering cycle across nodes).
        let (term, last_index, last_term) = {
            let mut node = self.node.lock().await;
            let (li, lt) = node.begin_election();
            (node.current_term(), li, lt)
        };
        let self_id = self.node.lock().await.id().to_string();
        let mut votes = 1u64; // self
        for peer in &ids {
            if peer == &self_id {
                continue;
            }
            let peer_id = peer.clone();
            let r = self
                .call_peer(
                    peer,
                    WireMessage::RequestVote {
                        term,
                        candidate_id: peer_id,
                        last_log_index: last_index,
                        last_log_term: last_term,
                    },
                )
                .await;
            if let Ok(WireMessage::RequestVoteResponse { vote_granted, .. }) = r {
                if vote_granted {
                    votes += 1;
                }
            }
        }
        let mut node = self.node.lock().await;
        node.finalize_election(votes);
    }

    /// As leader: send heartbeats / log suffix to every peer, then advance the
    /// commit index once a majority has acknowledged.
    async fn heartbeat_and_replicate(&self) {
        let leader_id = self.node.lock().await.id().to_string();
        let peers = self.node.lock().await.peers().to_vec();
        for peer in &peers {
            if peer == &leader_id {
                continue;
            }
            // Snapshot the slice to send and the prev-log pointers under the lock.
            let (prev_log_index, prev_log_term, entries, leader_commit, term) = {
                let n = self.node.lock().await;
                let start = n.next_index_of(peer).max(1);
                let entries = n.entries_from(start);
                let (prev_log_index, prev_log_term) = if let Some(first) = entries.first() {
                    let idx = first.index.saturating_sub(1);
                    let term = n
                        .log_entries()
                        .iter()
                        .find(|e| e.index == idx)
                        .map(|e| e.term)
                        .unwrap_or(0);
                    (idx, term)
                } else {
                    n.last_log()
                };
                (
                    prev_log_index,
                    prev_log_term,
                    entries,
                    n.commit_index(),
                    n.current_term(),
                )
            };
            let resp = self
                .call_peer(
                    peer,
                    WireMessage::AppendEntries {
                        term,
                        leader_id: leader_id.clone(),
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    },
                )
                .await;
            if let Ok(WireMessage::AppendEntriesResponse { success, match_index, .. }) = resp {
                let mut n = self.node.lock().await;
                if success {
                    n.advance_next(peer, match_index + 1);
                }
                n.record_peer_ack(peer, match_index);
            }
        }
    }

    /// Send `msg` to a peer by id and read its single response.
    async fn call_peer(&self, peer: &str, msg: WireMessage) -> EngineResult<WireMessage> {
        let addr = self
            .peer_addrs
            .iter()
            .find(|(id, _)| id == peer)
            .map(|(_, a)| a.clone())
            .ok_or_else(|| EngineError::internal(format!("no address for peer {peer}")))?;
        let connect = TcpStream::connect(&addr);
        let mut sock = tokio::time::timeout(Duration::from_secs(2), connect)
            .await
            .map_err(|_| EngineError::internal(format!("connect to {addr} timed out")))?
            .map_err(|e| EngineError::internal(format!("connect to {addr}: {e}")))?;
        write_message(&mut sock, &msg).await?;
        read_message(&mut sock).await
    }

    fn clone_for_task(&self) -> Arc<RaftServer> {
        // Cheap clones: the heavy state is behind Arc. The listener is consumed
        // by the first `serve()` call; clones keep a taken (None) slot.
        Arc::new(RaftServer {
            node: self.node.clone(),
            core: self.core.clone(),
            peer_addrs: self.peer_addrs.clone(),
            addr: self.addr.clone(),
            listener: std::sync::Mutex::new(None),
        })
    }
}

// --- helpers --------------------------------------------------------------

async fn read_message(sock: &mut TcpStream) -> EngineResult<WireMessage> {
    let mut len_buf = [0u8; FRAME_LEN];
    sock.read_exact(&mut len_buf)
        .await
        .map_err(|e| EngineError::internal(format!("read frame len: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(EngineError::internal("frame too large"));
    }
    let mut buf = vec![0u8; len];
    sock.read_exact(&mut buf)
        .await
        .map_err(|e| EngineError::internal(format!("read frame body: {e}")))?;
    serde_json::from_slice(&buf).map_err(|e| EngineError::internal(format!("decode wire msg: {e}")))
}

async fn write_message(sock: &mut TcpStream, msg: &WireMessage) -> EngineResult<()> {
    let body = serde_json::to_vec(msg).map_err(|e| EngineError::internal(format!("encode: {e}")))?;
    let mut frame = Vec::with_capacity(FRAME_LEN + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    sock.write_all(&frame)
        .await
        .map_err(|e| EngineError::internal(format!("write frame: {e}")))?;
    sock.flush()
        .await
        .map_err(|e| EngineError::internal(format!("flush: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_loopback_works() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            s.write_all(b"hi").await.unwrap();
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 2];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hi");
    }

    // Minimal election-only check: three nodes with serve+drive; a leader must
    // emerge. Isolates the election/transport path from apply-loop concerns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn election_emerges_leader() {
        let probe = tokio::spawn(async {
        let ids = ["a", "b", "c"];
        let mut addrs = Vec::new();
        for _ in 0..3 {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(l.local_addr().unwrap().to_string());
            drop(l);
        }
        let peers: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let peer_addrs: Vec<(String, String)> = ids
            .iter()
            .zip(addrs.iter())
            .map(|(id, a)| (id.to_string(), a.clone()))
            .collect();
        let mut servers = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let mut pa = peer_addrs.clone();
            pa.remove(i);
            let core = Arc::new(SynapseCore::new());
            let s = RaftServer::bind(id.to_string(), peers.clone(), pa, &addrs[i], core)
                .await
                .unwrap();
            servers.push(s);
        }
        for s in &servers {
            s.serve();
            s.drive();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let mut any = false;
            for s in &servers {
                if s.is_leader().await {
                    any = true;
                }
            }
            if any {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("no leader emerged");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        });
        tokio::time::timeout(Duration::from_secs(20), probe)
            .await
            .expect("election test timed out")
            .expect("election test panicked");
    }

    // Spin up a 3-node cluster over real loopback TCP and verify a proposed
    // command converges to every node's engine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_reaches_consensus_over_tcp() {
        // Overall guard so a hang fails loudly instead of timing out the runner.
        let probe = tokio::spawn(async {
            let ids = ["a", "b", "c"];
            let mut addrs = Vec::new();
            for _ in 0..3 {
                let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
                addrs.push(l.local_addr().unwrap().to_string());
                drop(l);
            }
            let peers: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
            let peer_addrs: Vec<(String, String)> = ids
                .iter()
                .zip(addrs.iter())
                .map(|(id, a)| (id.to_string(), a.clone()))
                .collect();

            let mut servers = Vec::new();
            for (i, id) in ids.iter().enumerate() {
                let mut pa = peer_addrs.clone();
                pa.remove(i);
                let core = Arc::new(SynapseCore::new());
                let s = RaftServer::bind(id.to_string(), peers.clone(), pa, &addrs[i], core)
                    .await
                    .unwrap();
                servers.push(s);
            }
            eprintln!("[test] bound all servers");

            for s in &servers {
                s.apply_loop();
                s.serve();
                s.drive();
            }
            eprintln!("[test] loops spawned");

            let mut leader: Option<Arc<RaftServer>> = None;
            for _ in 0..100 {
                for s in &servers {
                    if s.is_leader().await {
                        leader = Some(s.clone_for_task());
                        break;
                    }
                }
                if leader.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            eprintln!("[test] leader? {}", leader.is_some());
            let leader = leader.expect("a leader should be elected");

            let _idx = leader
                .propose(&ReplicatedCommand::MapSet {
                    tenant: "acme".into(),
                    name: "cache".into(),
                    key: "k".into(),
                    value: b"v".to_vec(),
                    ttl_ms: None,
                })
                .await
                .expect("leader accepts proposal");
            eprintln!("[test] proposed idx {}", _idx);

            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let mut converged = false;
            while tokio::time::Instant::now() < deadline {
                let mut all = true;
                for s in &servers {
                    s.apply_pending().await.unwrap();
                    match s.core.get_map("acme", "cache").unwrap() {
                        Some(m) if m.get("k") == Some(b"v".to_vec()) => {}
                        _ => all = false,
                    }
                }
                if all {
                    converged = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            eprintln!("[test] converged = {}", converged);
            assert!(converged, "command did not converge to all nodes");
        });
        tokio::time::timeout(Duration::from_secs(30), probe)
            .await
            .expect("test timed out (deadlock?)")
            .expect("test task panicked");
    }
}
