//! Stream Router: consumer groups, partition assignment, and offset tracking
//! (spec.txt §3.2, Kafka). Backs the Kafka adapter and unified log tailing.
//!
//! Each topic has a fixed number of partitions (each partition maps onto a
//! [`core::Log`] at the engine layer). A consumer group tracks one committed
//! offset per `(topic, partition)`; `fetch` resumes from the committed offset,
//! and `rebalance` assigns partitions to live members round-robin.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic: String,
    partition: u32,
}

#[derive(Debug, Default)]
struct Group {
    members: Vec<String>,
    offsets: HashMap<PartitionKey, u64>,
}

/// Manages topics, consumer groups, and committed offsets for stream routing.
#[derive(Debug, Default)]
pub struct StreamRouter {
    partitions: Mutex<HashMap<String, u32>>,
    groups: Mutex<HashMap<String, Group>>,
}

impl StreamRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a topic with a partition count (idempotent if unchanged).
    pub fn create_topic(&self, topic: &str, partitions: u32) -> RoutingResult<()> {
        if partitions == 0 {
            return Err(RoutingError::new("partitions must be > 0"));
        }
        let mut ps = self.partitions.lock().unwrap();
        match ps.get(topic) {
            Some(&existing) if existing != partitions => {
                return Err(RoutingError::new("topic partition count changed"));
            }
            _ => ps.insert(topic.to_string(), partitions),
        };
        Ok(())
    }

    pub fn partition_count(&self, topic: &str) -> Option<u32> {
        self.partitions.lock().unwrap().get(topic).copied()
    }

    /// Add a member to a consumer group (creating the group if needed).
    pub fn join_group(&self, group: &str, member: &str) {
        let mut groups = self.groups.lock().unwrap();
        groups
            .entry(group.to_string())
            .or_default()
            .members
            .push(member.to_string());
    }

    pub fn leave_group(&self, group: &str, member: &str) {
        let mut groups = self.groups.lock().unwrap();
        if let Some(g) = groups.get_mut(group) {
            g.members.retain(|m| m != member);
            if g.members.is_empty() {
                groups.remove(group);
            }
        }
    }

    /// Commit the consumed offset for a `(topic, partition)` in a group.
    pub fn commit(&self, group: &str, topic: &str, partition: u32, offset: u64) {
        let mut groups = self.groups.lock().unwrap();
        let g = groups.entry(group.to_string()).or_default();
        g.offsets.insert(PartitionKey { topic: topic.to_string(), partition }, offset);
    }

    /// The next offset a group should fetch from for a partition (0 if none).
    pub fn next_fetch(&self, group: &str, topic: &str, partition: u32) -> u64 {
        let groups = self.groups.lock().unwrap();
        groups
            .get(group)
            .and_then(|g| g.offsets.get(&PartitionKey { topic: topic.to_string(), partition }))
            .copied()
            .unwrap_or(0)
    }

    /// Rebalance: assign each partition of each topic to one live member,
    /// round-robin, returning `member -> [(topic, partition)]`.
    pub fn rebalance(&self, group: &str) -> HashMap<String, Vec<(String, u32)>> {
        let groups = self.groups.lock().unwrap();
        let topics = self.partitions.lock().unwrap();
        let empty = Group::default();
        let g = groups.get(group).unwrap_or(&empty);
        let members: Vec<&String> = g.members.iter().collect();
        let mut assignment: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        if members.is_empty() {
            return assignment;
        }
        let mut idx = 0;
        for (topic, &count) in topics.iter() {
            for p in 0..count {
                let m = members[idx % members.len()].clone();
                assignment.entry(m).or_default().push((topic.clone(), p));
                idx += 1;
            }
        }
        assignment
    }

    /// Live members of a group (for introspection).
    pub fn members(&self, group: &str) -> HashSet<String> {
        self.groups
            .lock()
            .unwrap()
            .get(group)
            .map(|g| g.members.iter().cloned().collect())
            .unwrap_or_default()
    }
}

use crate::RoutingError;
use crate::RoutingResult;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_and_fetch() {
        let r = StreamRouter::new();
        r.create_topic("events", 3).unwrap();
        r.join_group("g1", "c1");
        r.join_group("g1", "c2");
        assert_eq!(r.next_fetch("g1", "events", 0), 0);
        r.commit("g1", "events", 0, 42);
        assert_eq!(r.next_fetch("g1", "events", 0), 42);
    }

    #[test]
    fn rebalance_splits_partitions() {
        let r = StreamRouter::new();
        r.create_topic("events", 4).unwrap();
        r.join_group("g1", "c1");
        r.join_group("g1", "c2");
        let a = r.rebalance("g1");
        let total: usize = a.values().map(|v| v.len()).sum();
        assert_eq!(total, 4);
        assert_eq!(a.len(), 2);
    }
}
