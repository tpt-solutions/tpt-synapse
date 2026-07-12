//! Graph Router: exchange/binding/queue routing for the AMQP "Lite" adapter
//! (spec.txt §3.2, §6 Phase 3). We model the simplified internal graph:
//! exchanges of kind Direct/Topic/Fanout, bindings that connect an exchange to
//! a queue under a routing-key pattern, and the resulting queue set for a
//! publish.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::topic::topic_matches_sep;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeKind {
    /// Routing key must equal the binding pattern exactly.
    Direct,
    /// Routing key is matched against the binding pattern as an MQTT-style
    /// topic filter (`+`/`#` wildcards).
    Topic,
    /// Pattern is ignored; every bound queue receives the message.
    Fanout,
}

#[derive(Debug, Clone)]
struct Binding {
    queue: String,
    pattern: String,
}

#[derive(Debug)]
struct Exchange {
    kind: ExchangeKind,
    bindings: Vec<Binding>,
}

/// The routing graph for AMQP-style message delivery.
#[derive(Debug, Default)]
pub struct GraphRouter {
    exchanges: Mutex<HashMap<String, Exchange>>,
    queues: Mutex<HashSet<String>>,
}

impl GraphRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_exchange(&self, name: &str, kind: ExchangeKind) {
        self.exchanges
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| Exchange {
                kind,
                bindings: Vec::new(),
            });
    }

    pub fn create_queue(&self, name: &str) {
        self.queues.lock().unwrap().insert(name.to_string());
    }

    pub fn bind(&self, exchange: &str, queue: &str, pattern: &str) -> RoutingResult<()> {
        if !self.queues.lock().unwrap().contains(queue) {
            return Err(RoutingError::new("unknown queue"));
        }
        let mut exchanges = self.exchanges.lock().unwrap();
        let ex = exchanges
            .get_mut(exchange)
            .ok_or_else(|| RoutingError::new("unknown exchange"))?;
        if !ex.bindings.iter().any(|b| b.queue == queue && b.pattern == pattern) {
            ex.bindings.push(Binding {
                queue: queue.to_string(),
                pattern: pattern.to_string(),
            });
        }
        Ok(())
    }

    /// Resolve the set of queues a publish to `exchange` with `routing_key`
    /// should be delivered to.
    pub fn route(&self, exchange: &str, routing_key: &str) -> RoutingResult<Vec<String>> {
        let exchanges = self.exchanges.lock().unwrap();
        let ex = exchanges
            .get(exchange)
            .ok_or_else(|| RoutingError::new("unknown exchange"))?;
        let mut out: Vec<String> = ex
            .bindings
            .iter()
            .filter(|b| match ex.kind {
                ExchangeKind::Direct => b.pattern == routing_key,
                ExchangeKind::Topic => topic_matches_sep(&b.pattern, routing_key, b'.'),
                ExchangeKind::Fanout => true,
            })
            .map(|b| b.queue.clone())
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub fn queues(&self) -> Vec<String> {
        self.queues.lock().unwrap().iter().cloned().collect()
    }
}

use crate::RoutingError;
use crate::RoutingResult;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fanout_delivers_to_all() {
        let g = GraphRouter::new();
        g.create_exchange("logs", ExchangeKind::Fanout);
        g.create_queue("q1");
        g.create_queue("q2");
        g.bind("logs", "q1", "").unwrap();
        g.bind("logs", "q2", "").unwrap();
        let out = g.route("logs", "anything").unwrap();
        assert_eq!(out, vec!["q1".to_string(), "q2".to_string()]);
    }

    #[test]
    fn topic_routing_key_match() {
        let g = GraphRouter::new();
        g.create_exchange("events", ExchangeKind::Topic);
        g.create_queue("orders");
        g.bind("events", "orders", "shop.#.order").unwrap();
        assert!(g.route("events", "shop.us.order").unwrap().contains(&"orders".to_string()));
        assert!(!g.route("events", "shop.us.cancel").unwrap().contains(&"orders".to_string()));
    }

    #[test]
    fn bind_unknown_queue_errors() {
        let g = GraphRouter::new();
        g.create_exchange("e", ExchangeKind::Fanout);
        assert!(g.bind("e", "missing", "").is_err());
    }
}
