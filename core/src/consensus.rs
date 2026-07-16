//! Embedded Raft consensus for multi-node HA and log replication (TODO.md
//! Phase 4). Feature-gated behind `consensus` so the crate builds without it.
//!
//! This is a self-contained, dependency-free Raft core implementing the two
//! fundamental RPCs (`RequestVote` / `AppendEntries`) and the
//! follower/candidate/leader state machine, so a cluster can agree on a
//! replicated log without an external ZooKeeper-style coordinator. The Phase 0
//! decision named `openraft` as the eventual production library; this module is
//! the embeddable building block the data plane drives until that integration
//! lands, and it implements the same algorithm (term monotonicity, voted-for
//! tracking, log matching, commit via majority).

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

/// A node's Raft role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// One replicated log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub term: u64,
    /// 1-based position in the log.
    pub index: u64,
    pub data: Vec<u8>,
}

/// Durable node state that must survive restarts: the current term and who
/// this node voted for in it. The log itself is held in memory here; a
/// production store would persist it too.
pub trait StateStore: Send + Sync {
    fn save_term(&self, term: u64);
    fn load_term(&self) -> u64;
    fn save_voted_for(&self, id: Option<String>);
    fn load_voted_for(&self) -> Option<String>;
}

/// In-memory [`StateStore`] for tests and single-process clusters.
#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<(u64, Option<String>)>,
}

impl MemoryStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl StateStore for MemoryStore {
    fn save_term(&self, term: u64) {
        self.inner.lock().unwrap().0 = term;
    }
    fn load_term(&self) -> u64 {
        self.inner.lock().unwrap().0
    }
    fn save_voted_for(&self, id: Option<String>) {
        self.inner.lock().unwrap().1 = id;
    }
    fn load_voted_for(&self) -> Option<String> {
        self.inner.lock().unwrap().1.clone()
    }
}

/// A single Raft node. Network transport, election timers, and apply-loop are
/// the caller's responsibility; this type is the pure state machine that
/// answers the RPCs and tracks replication progress.
pub struct RaftNode {
    id: String,
    peers: Vec<String>,
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<Entry>,
    commit_index: u64,
    last_applied: u64,
    /// Next index to send each peer (leader only).
    next_index: Vec<u64>,
    /// Highest index replicated on each peer (leader only), used to advance the
    /// commit index once a majority has acknowledged.
    match_index: Vec<u64>,
    store: Arc<dyn StateStore>,
}

impl RaftNode {
    /// Create a node with `id`, cluster `peers`, and a durable `store`.
    pub fn new(id: String, peers: Vec<String>, store: Arc<dyn StateStore>) -> Self {
        let current_term = store.load_term();
        let voted_for = store.load_voted_for();
        let majority = peers.len() / 2 + 1;
        let next_index = vec![1; majority.max(peers.len())];
        Self {
            id,
            peers: peers.clone(),
            role: Role::Follower,
            current_term,
            voted_for,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index,
            match_index: vec![0; peers.len()],
            store,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn current_term(&self) -> u64 {
        self.current_term
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }
    pub fn peers(&self) -> &[String] {
        &self.peers
    }

    /// Total cluster size including this node (used for majority arithmetic).
    /// `peers` already includes this node's own id (see [`RaftNode::new`]).
    pub fn cluster_size(&self) -> u64 {
        self.peers.len() as u64
    }

    /// The next index the leader should send to `peer` (1-based).
    pub fn next_index_of(&self, peer: &str) -> u64 {
        match self.peers.iter().position(|p| p == peer) {
            Some(i) if i < self.next_index.len() => self.next_index[i],
            _ => self.log_len() + 1,
        }
    }

    /// Advance a peer's `next_index` (typically to `match_index + 1` after a
    /// successful AppendEntries).
    pub fn advance_next(&mut self, peer: &str, next: u64) {
        if let Some(i) = self.peers.iter().position(|p| p == peer) {
            if i < self.next_index.len() {
                self.next_index[i] = next;
            }
        }
    }

    /// The `(index, term)` pair of the last entry in the log (used as the
    /// `prev_log_*` arguments when the leader ships entries 1..N).
    pub fn last_log(&self) -> (u64, u64) {
        match self.log.last() {
            Some(e) => (e.index, e.term),
            None => (0, 0),
        }
    }

    /// Entries from `start` (1-based, inclusive) to the end of the log.
    pub fn entries_from(&self, start: u64) -> Vec<Entry> {
        if start == 0 {
            return self.log.clone();
        }
        self.log
            .iter()
            .skip_while(|e| e.index < start)
            .cloned()
            .collect()
    }

    /// True when `candidate`'s log is at least as up-to-date as ours, per the
    /// Raft log-comparison rule (last term, then last index).
    pub fn is_log_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let (my_index, my_term) = self.last_log();
        last_term > my_term || (last_term == my_term && last_index >= my_index)
    }

    /// Advance `next_index`/`match_index` for a peer after an AppendEntries
    /// response, then advance the leader's commit index to the highest index
    /// replicated on a majority of nodes.
    pub fn record_peer_ack(&mut self, peer: &str, peer_match: u64) {
        if let Some(pi) = self.peers.iter().position(|p| p == peer) {
            if pi < self.match_index.len() {
                self.match_index[pi] = self.match_index[pi].max(peer_match);
            }
        }
        // Count nodes (self + peers) that have replicated at least `idx`.
        let majority = self.peers.len() as u64 / 2 + 1;
        let mut idx = self.commit_index;
        while idx < self.log_len() {
            let next = idx + 1;
            let replicated = 1u64
                + self
                    .match_index
                    .iter()
                    .filter(|m| **m >= next)
                    .count() as u64;
            if replicated >= majority && self.log[(next - 1) as usize].term == self.current_term {
                idx = next;
            } else {
                break;
            }
        }
        self.commit_index = idx;
    }

    /// Advance the commit index up to `idx` (clamped to the log length). A real
    /// leader does this once a majority has acknowledged; the in-memory
    /// [`crate::cluster::Cluster`] test drives it directly after replication.
    pub fn commit_up_to(&mut self, idx: u64) {
        let max = self.log_len();
        self.commit_index = idx.min(max);
    }
    pub fn log_len(&self) -> u64 {
        self.log.len() as u64
    }

    /// Clone of the replicated log, used by the apply-loop / transport
    /// (TODO.md Phase 4 `cluster.rs`) to push entries to followers.
    pub fn log_entries(&self) -> Vec<Entry> {
        self.log.clone()
    }

    /// Begin an election: bump the term, become candidate, grant our own vote.
    /// Returns `(last_index, last_term)` for the request-vote RPCs. Caller must
    /// NOT hold the node lock while awaiting the votes (see `collect_votes`) so
    /// peers can answer without deadlock.
    pub fn begin_election(&mut self) -> (u64, u64) {
        self.current_term += 1;
        self.store.save_term(self.current_term);
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.store.save_voted_for(Some(self.id.clone()));
        self.last_log()
    }

    /// Tally `votes` (including our own). Becomes Leader and resets per-peer
    /// next indices on a majority. Caller must not hold the node lock across the
    /// vote collection so peers can answer without deadlock.
    pub fn finalize_election(&mut self, votes: u64) {
        let majority = self.peers.len() as u64 / 2 + 1;
        if votes >= majority {
            self.role = Role::Leader;
            let next = self.log_len() + 1;
            for n in self.next_index.iter_mut() {
                *n = next;
            }
        }
    }

    /// Handle an incoming `RequestVote` RPC. Returns `(term, vote_granted)`.
    pub fn handle_request_vote(
        &mut self,
        candidate_term: u64,
        candidate_id: &str,
        last_log_index: u64,
        last_log_term: u64,
    ) -> (u64, bool) {
        if candidate_term < self.current_term {
            return (self.current_term, false);
        }
        // Newer term: step down and persist it.
        if candidate_term > self.current_term {
            self.current_term = candidate_term;
            self.store.save_term(self.current_term);
            self.voted_for = None;
            self.store.save_voted_for(None);
            self.role = Role::Follower;
        }
        let (my_last_index, my_last_term) = self.last_log();
        let log_up_to_date =
            last_log_term > my_last_term || (last_log_term == my_last_term && last_log_index >= my_last_index);
        let can_vote = self.voted_for.is_none() || self.voted_for.as_deref() == Some(candidate_id);
        if can_vote && log_up_to_date {
            self.voted_for = Some(candidate_id.to_string());
            self.store.save_voted_for(Some(candidate_id.to_string()));
            (self.current_term, true)
        } else {
            (self.current_term, false)
        }
    }

    /// Handle an incoming `AppendEntries` RPC (heartbeat when `entries` is
    /// empty). Returns `(term, success)`.
    pub fn handle_append_entries(
        &mut self,
        leader_term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) -> (u64, bool) {
        if leader_term < self.current_term {
            return (self.current_term, false);
        }
        // Newer term: step down to follower. A leader receiving its own
        // heartbeat (leader_term == current_term) must NOT demote itself.
        if leader_term > self.current_term {
            self.current_term = leader_term;
            self.store.save_term(self.current_term);
            self.role = Role::Follower;
        }

        // Log consistency check at prev_log_index.
        let prev_ok = if prev_log_index == 0 {
            true
        } else {
            match self.log.get((prev_log_index - 1) as usize) {
                Some(e) => e.term == prev_log_term,
                None => false,
            }
        };
        if !prev_ok {
            return (self.current_term, false);
        }

        // Append new entries, truncating any conflicting suffix.
        let mut start = prev_log_index as usize;
        for e in entries {
            if let Some(existing) = self.log.get(start) {
                if existing.term != e.term {
                    self.log.truncate(start);
                }
            }
            if start >= self.log.len() {
                self.log.push(e);
            }
            start += 1;
        }

        // Advance commit index.
        if leader_commit > self.commit_index {
            let last = self.log_len();
            self.commit_index = leader_commit.min(last);
        }
        (self.current_term, true)
    }

    /// Append a new entry as leader (assumes this node is leader) and return
    /// its assigned index. Replication to followers happens via
    /// `handle_append_entries` on the peer side.
    pub fn leader_append(&mut self, data: Vec<u8>) -> u64 {
        let term = self.current_term;
        let index = self.log_len() + 1;
        self.log.push(Entry { term, index, data });
        index
    }

    /// Apply every committed-but-not-yet-applied entry to the state machine via
    /// `apply`. Advances `last_applied` as entries are applied. This is the
    /// missing wiring called out in TODO.md Phase 4: the caller drives the real
    /// `Log`/`Queue`/`Map` primitives from here (see `cluster.rs`).
    pub fn apply_committed<F>(&mut self, mut apply: F)
    where
        F: FnMut(&Entry),
    {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            match self.log.get((next - 1) as usize) {
                Some(e) => {
                    apply(e);
                    self.last_applied = next;
                }
                None => break,
            }
        }
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

    fn node(id: &str, peers: &[&str]) -> RaftNode {
        let ps: Vec<String> = peers.iter().map(|s| s.to_string()).collect();
        RaftNode::new(id.to_string(), ps, MemoryStore::new())
    }

    #[test]
    fn single_node_elects_itself_leader() {
        let mut n = node("a", &["a"]);
        n.begin_election();
        n.finalize_election(1);
        assert_eq!(n.role(), Role::Leader);
    }

    #[test]
    fn follower_grants_vote_to_up_to_date_candidate() {
        let mut f = node("b", &["a", "b"]);
        // Candidate "a" with an empty log, term 2, asks "b" for a vote.
        let (term, granted) = f.handle_request_vote(2, "a", 0, 0);
        assert_eq!(term, 2);
        assert!(granted);
        assert_eq!(f.current_term(), 2);
    }

    #[test]
    fn follower_rejects_stale_term() {
        let mut f = node("b", &["a", "b"]);
        f.current_term = 5;
        f.store.save_term(5);
        let (term, granted) = f.handle_request_vote(3, "a", 0, 0);
        assert_eq!(term, 5);
        assert!(!granted);
    }

    #[test]
    fn follower_rejects_candidate_with_behind_log() {
        let mut f = node("b", &["a", "b"]);
        // f has a newer log (term 3 at index 1); candidate is behind.
        f.log.push(Entry { term: 3, index: 1, data: vec![1] });
        let (_, granted) = f.handle_request_vote(4, "a", 0, 0);
        assert!(!granted);
    }

    #[test]
    fn higher_term_forces_step_down() {
        let mut n = node("a", &["a", "b", "c"]);
        // Win an election first (3-node cluster, all 3 grant).
        n.begin_election();
        n.finalize_election(3);
        assert_eq!(n.role(), Role::Leader);
        // A newer-term AppendEntries from another leader steps us down.
        let (term, ok) = n.handle_append_entries(9, 0, 0, vec![], 0);
        assert!(ok);
        assert_eq!(term, 9);
        assert_eq!(n.role(), Role::Follower);
    }

    #[test]
    fn append_entries_replicates_and_commits() {
        let mut leader = node("a", &["a", "b"]);
        leader.begin_election();
        leader.finalize_election(1);
        let idx1 = leader.leader_append(b"one".to_vec());
        let idx2 = leader.leader_append(b"two".to_vec());
        assert_eq!((idx1, idx2), (1, 2));

        // Replicate to follower "b".
        let mut follower = node("b", &["a", "b"]);
        let entries = leader.log.clone();
        let (term, ok) = follower.handle_append_entries(
            leader.current_term(),
            0,
            0,
            entries,
            /* leader_commit = */ 2,
        );
        assert!(ok);
        assert_eq!(term, leader.current_term());
        assert_eq!(follower.log_len(), 2);
        assert_eq!(follower.commit_index(), 2);
        assert_eq!(follower.log[1].data, b"two");
    }

    #[test]
    fn append_entries_rejects_inconsistent_prefix() {
        let mut follower = node("b", &["a", "b"]);
        follower.log.push(Entry { term: 1, index: 1, data: vec![9] });
        // Leader claims prev_log_index=1, prev_log_term=2 — but follower's
        // entry 1 is term 1, so the prefix check must fail.
        let (_, ok) = follower.handle_append_entries(2, 1, 2, vec![], 0);
        assert!(!ok);
    }
}
