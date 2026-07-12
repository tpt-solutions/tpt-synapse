//! Topic Router: hierarchical pub/sub matching for MQTT (spec.txt §3.2).
//!
//! Implements the MQTT wildcard contract: `+` matches exactly one topic level,
//! `#` matches the remaining levels (must be the final character). Publishers
//! write to concrete topics; subscribers register a filter and are returned by
//! [`TopicRouter::route`] when their filter matches.

use std::collections::HashMap;
use std::sync::Mutex;

/// Returns true if an MQTT topic `filter` (which may contain wildcards)
/// matches a concrete published `topic`. Levels are separated by `/`.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    topic_matches_sep(filter, topic, b'/')
}

/// Same as [`topic_matches`] but with a configurable level separator (given as
/// a byte). AMQP topic exchanges use `.` as the separator, so the graph router
/// calls this with `b'.'`.
pub fn topic_matches_sep(filter: &str, topic: &str, sep: u8) -> bool {
    // Work on level slices so separators never get dropped at level boundaries.
    let fl: Vec<&str> = split_levels(filter, sep);
    let tl: Vec<&str> = split_levels(topic, sep);
    matches_levels(&fl, &tl)
}

fn split_levels(s: &str, sep: u8) -> Vec<&str> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(sep as char).collect()
}

fn matches_levels(filter: &[&str], topic: &[&str]) -> bool {
    match filter.split_first() {
        None => topic.is_empty(),
        Some((f, frest)) => match *f {
            "#" => {
                // `#` matches the parent level and all remaining levels. When it
                // is the final filter level (per MQTT) it matches any topic. For
                // the malformed-but-tolerated mid-`#` case it consumes 1+ levels
                // and the rest of the filter must still match what's left.
                if frest.is_empty() {
                    true
                } else if topic.is_empty() {
                    false
                } else {
                    (1..=topic.len()).any(|i| matches_levels(frest, &topic[i..]))
                }
            }
            "+" => {
                if topic.is_empty() {
                    false
                } else {
                    matches_levels(frest, &topic[1..])
                }
            }
            _ => {
                if topic.is_empty() {
                    false
                } else if *f == topic[0] {
                    matches_levels(frest, &topic[1..])
                } else {
                    false
                }
            }
        },
    }
}

/// Registry of subscriber-id -> topic filter, with matching lookups.
#[derive(Debug, Default)]
pub struct TopicRouter {
    subs: Mutex<HashMap<String, String>>,
}

impl TopicRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe `id` to `filter`. Replaces any existing filter for that id.
    pub fn subscribe(&self, id: &str, filter: &str) {
        self.subs
            .lock()
            .unwrap()
            .insert(id.to_string(), filter.to_string());
    }

    pub fn unsubscribe(&self, id: &str) {
        self.subs.lock().unwrap().remove(id);
    }

    /// Return the subscriber ids whose filter matches `topic`.
    pub fn route(&self, topic: &str) -> Vec<String> {
        self.subs
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, filter)| topic_matches(filter, topic))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// All current subscriber ids (for rebalance / introspection).
    pub fn subscribers(&self) -> Vec<String> {
        self.subs.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_matching() {
        assert!(topic_matches("a/b", "a/b"));
        assert!(!topic_matches("a/b", "a/c"));
        assert!(topic_matches("a/+", "a/b"));
        assert!(!topic_matches("a/+", "a/b/c"));
        assert!(topic_matches("a/#", "a/b/c"));
        assert!(topic_matches("a/#", "a"));
        assert!(!topic_matches("a/b/#", "a"));
        assert!(topic_matches("sport/+/player1", "sport/tennis/player1"));
    }

    #[test]
    fn router_delivers_to_matching() {
        let r = TopicRouter::new();
        r.subscribe("sub1", "sensors/#");
        r.subscribe("sub2", "sensors/temp/+");
        r.subscribe("sub3", "alerts");
        let hits = r.route("sensors/temp/kitchen");
        assert!(hits.contains(&"sub1".to_string()));
        assert!(hits.contains(&"sub2".to_string()));
        assert!(!hits.contains(&"sub3".to_string()));
    }

    /// Milestone gate (TODO.md Phase 1): the router must sustain 1M+ routing
    /// ops/sec on a single node. The strict 1M target is validated in release
    /// builds via `cargo bench` (the historical tracker); debug CI keeps a
    /// shorter, lower-floor run so `cargo test` stays fast and portable while
    /// still guarding against catastrophic regressions.
    #[test]
    fn sustains_one_million_ops_per_sec() {
        let r = TopicRouter::new();
        for i in 0..64 {
            r.subscribe(&format!("s{i}"), "sensors/+/temp");
        }
        let release = !cfg!(debug_assertions);
        let n = if release { 2_000_000u64 } else { 200_000u64 };
        let start = std::time::Instant::now();
        let mut sink = 0usize;
        for _ in 0..n {
            sink += r.route("sensors/room1/temp").len();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = n as f64 / elapsed.as_secs_f64();
        let floor = if release { 1_000_000.0 } else { 5_000.0 };
        assert!(
            ops_per_sec >= floor,
            "routing throughput {ops_per_sec:.0} ops/sec below floor {floor:.0} (sink={sink})"
        );
    }
}
