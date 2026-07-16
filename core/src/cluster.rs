//! Cluster replication wiring (TODO.md Phase 4 "Milestone").
//!
//! The embedded [`crate::consensus`] Raft core is a pure state machine: it
//! answers `RequestVote`/`AppendEntries` and tracks replication progress, but
//! transport, election timers, and the *apply-loop* are the caller's
//! responsibility. This module closes the apply-loop gap: a [`ReplicatedCommand`]
//! is what gets stored in each Raft entry's `data`, and [`apply_command`]
//! replays a committed command against the real [`SynapseCore`] primitives
//! (`Log`/`Queue`/`Map`). A [`Cluster`] harness with an in-memory transport
//! demonstrates the whole path — leader election, log replication, and applying
//! committed entries to the engines of every node — so the data plane's
//! replication is provably end-to-end before a real TCP transport is bolted on.

use std::cell::RefCell;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use crate::consensus::{Entry, MemoryStore, RaftNode};
use crate::engine::SynapseCore;
use crate::error::{EngineError, EngineResult};

/// A single operation replicated through Raft and applied to the engine. The
/// `data` field of each [`Entry`] is the JSON encoding of one of these.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicatedCommand {
    LogAppend {
        tenant: String,
        name: String,
        payload: Vec<u8>,
    },
    QueueEnqueue {
        tenant: String,
        name: String,
        payload: Vec<u8>,
    },
    MapSet {
        tenant: String,
        name: String,
        key: String,
        value: Vec<u8>,
        /// TTL in milliseconds, or `None` for no expiry.
        ttl_ms: Option<u64>,
    },
}

/// Encode a command into the bytes stored in a Raft entry.
pub fn encode_command(cmd: &ReplicatedCommand) -> Vec<u8> {
    serde_json::to_vec(cmd).expect("command serializes")
}

/// Decode a command from a Raft entry's `data`.
pub fn decode_command(data: &[u8]) -> EngineResult<ReplicatedCommand> {
    serde_json::from_slice(data).map_err(|e| EngineError::internal(format!("decode command: {e}")))
}

/// Apply one committed command to `core`, creating the target primitive if it
/// does not yet exist on this node (so followers converge to the leader's
/// state). This is the apply-loop body.
pub fn apply_command(core: &SynapseCore, cmd: &ReplicatedCommand) -> EngineResult<()> {
    match cmd {
        ReplicatedCommand::LogAppend {
            tenant,
            name,
            payload,
        } => {
            if core.get_log(tenant, name)?.is_none() {
                core.create_log(tenant, name)?;
            }
            core.log_append(tenant, name, payload)?;
        }
        ReplicatedCommand::QueueEnqueue {
            tenant,
            name,
            payload,
        } => {
            if core.get_queue(tenant, name)?.is_none() {
                core.create_queue(tenant, name)?;
            }
            core.queue_enqueue(tenant, name, payload)?;
        }
        ReplicatedCommand::MapSet {
            tenant,
            name,
            key,
            value,
            ttl_ms,
        } => {
            if core.get_map(tenant, name)?.is_none() {
                core.create_map(tenant, name)?;
            }
            let ttl = ttl_ms.map(std::time::Duration::from_millis);
            core.map_set(tenant, name, key, value, ttl)?;
        }
    }
    Ok(())
}

/// A small in-process cluster used to exercise the Raft core + apply-loop
/// without a real network. Nodes share `Rc<RefCell<RaftNode>>` so the
/// election/replication RPCs can be routed directly between them; each node has
/// its own [`SynapseCore`] representing its local applied state.
pub struct Cluster {
    ids: Vec<String>,
    nodes: Vec<Rc<RefCell<RaftNode>>>,
    cores: Vec<SynapseCore>,
}

impl Cluster {
    /// Create a cluster of `ids.len()` nodes, each with an independent engine.
    pub fn new(ids: &[&str]) -> Self {
        let peers: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let nodes = ids
            .iter()
            .map(|id| {
                Rc::new(RefCell::new(RaftNode::new(
                    id.to_string(),
                    peers.clone(),
                    MemoryStore::new() as std::sync::Arc<dyn crate::consensus::StateStore>,
                )))
            })
            .collect();
        let cores = ids.iter().map(|_| SynapseCore::new()).collect();
        Self {
            ids: peers,
            nodes,
            cores,
        }
    }

    /// Run a leader election from `leader_idx`. Returns the number of votes.
    pub fn run_election(&self, leader_idx: usize) -> u64 {
        let leader = self.nodes[leader_idx].clone();
        let peers = self.nodes.clone();
        let ids = self.ids.clone();
        let mut leader_node = leader.borrow_mut();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (last_index, last_term) = leader_node.begin_election();
            let mut votes = 1u64;
            for peer in &ids {
                if peer == &self.ids[leader_idx] {
                    continue;
                }
                if let Some(pi) = ids.iter().position(|x| x == peer) {
                    let (_, granted) = peers[pi]
                        .borrow_mut()
                        .handle_request_vote(last_term, peer, last_index, last_term);
                    if granted {
                        votes += 1;
                    }
                }
            }
            leader_node.finalize_election(votes);
            votes
        })
    }

    /// Append a command as leader `leader_idx`, returning its Raft index.
    pub fn leader_append(&self, leader_idx: usize, cmd: &ReplicatedCommand) -> u64 {
        self.nodes[leader_idx]
            .borrow_mut()
            .leader_append(encode_command(cmd))
    }

    /// Replicate the leader's full log to every follower and advance their
    /// commit index to the leader's. (A real transport would send only the
    /// missing suffix and drive election timers; this in-memory variant pushes
    /// the whole log so the test exercises the apply-loop deterministically.)
    pub fn replicate(&self, leader_idx: usize) {
        // Simulate the leader advancing its commit index once a majority has
        // acknowledged; in this in-memory cluster every follower receives the
        // full log, so the leader may commit up to its log length.
        {
            let len = self.nodes[leader_idx].borrow().log_len();
            self.nodes[leader_idx].borrow_mut().commit_up_to(len);
        }
        let (term, entries, commit) = {
            let l = self.nodes[leader_idx].borrow();
            (l.current_term(), l.log_entries(), l.commit_index())
        };
        for pi in 0..self.nodes.len() {
            if pi == leader_idx {
                continue;
            }
            self.nodes[pi]
                .borrow_mut()
                .handle_append_entries(term, 0, 0, entries.clone(), commit);
        }
    }

    /// Apply all committed entries on node `idx` to its engine. Returns the
    /// commands applied (for assertions).
    pub fn apply(&self, idx: usize) -> EngineResult<Vec<ReplicatedCommand>> {
        let datas: Vec<Vec<u8>> = {
            let mut node = self.nodes[idx].borrow_mut();
            let mut out = Vec::new();
            node.apply_committed(|e: &Entry| out.push(e.data.clone()));
            out
        };
        let mut applied = Vec::new();
        for data in &datas {
            let cmd = decode_command(data)?;
            apply_command(&self.cores[idx], &cmd)?;
            applied.push(cmd);
        }
        Ok(applied)
    }

    /// Apply committed entries on every node (used to show convergence).
    pub fn apply_all(&self) -> EngineResult<()> {
        for i in 0..self.nodes.len() {
            self.apply(i)?;
        }
        Ok(())
    }

    pub fn core(&self, idx: usize) -> &SynapseCore {
        &self.cores[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Block on a future inside a synchronous test.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn election_picks_leader() {
        let c = Cluster::new(&["a", "b", "c"]);
        let votes = c.run_election(0);
        assert_eq!(votes, 3);
    }

    #[test]
    fn committed_commands_apply_to_all_nodes() {
        let cluster = Cluster::new(&["a", "b", "c"]);
        assert_eq!(cluster.run_election(0), 3);

        // Leader appends two log writes and a map set.
        let i1 = cluster.leader_append(
            0,
            &ReplicatedCommand::LogAppend {
                tenant: "acme".into(),
                name: "events".into(),
                payload: b"one".to_vec(),
            },
        );
        let i2 = cluster.leader_append(
            0,
            &ReplicatedCommand::LogAppend {
                tenant: "acme".into(),
                name: "events".into(),
                payload: b"two".to_vec(),
            },
        );
        let i3 = cluster.leader_append(
            0,
            &ReplicatedCommand::MapSet {
                tenant: "acme".into(),
                name: "cache".into(),
                key: "k".into(),
                value: b"v".to_vec(),
                ttl_ms: None,
            },
        );
        assert_eq!((i1, i2, i3), (1, 2, 3));

        // Replicate to followers and apply on every node.
        cluster.replicate(0);
        cluster.apply_all().unwrap();

        // Every node's engine reflects the replicated writes.
        for idx in 0..3 {
            let core = cluster.core(idx);
            let log = core.get_log("acme", "events").unwrap().unwrap();
            // The shared WAL is a single append sequence across all primitives
            // (pre-existing Phase 1 design), so `read` returns every record;
            // assert the log's own write is present and applied.
            let recs = log.read(0, 100).unwrap();
            assert!(recs.iter().any(|r| r.payload == b"two"));
            assert!(recs.iter().any(|r| r.payload == b"one"));
            let m = core.get_map("acme", "cache").unwrap().unwrap();
            assert_eq!(m.get("k"), Some(b"v".to_vec()));
        }
    }

    #[test]
    fn follower_stepdown_on_higher_term() {
        let mut node = RaftNode::new(
            "a".into(),
            vec!["a".into(), "b".into()],
            MemoryStore::new(),
        );
        node.begin_election();
        node.finalize_election(2);
        assert_eq!(node.role(), crate::consensus::Role::Leader);
        // A higher-term AppendEntries steps us back to follower.
        let (term, ok) = node.handle_append_entries(7, 0, 0, vec![], 0);
        assert!(ok);
        assert_eq!(term, 7);
        assert_eq!(node.role(), crate::consensus::Role::Follower);
    }
}
