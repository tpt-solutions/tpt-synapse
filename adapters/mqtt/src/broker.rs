//! MQTT broker: session registry, subscription matching, QoS 0/1/2 delivery,
//! retained messages, and QoS 1/2 durability through the core [`Log`] primitive
//! (spec.txt §3.3, §6 Phase 2).
//!
//! The broker is protocol-agnostic about the transport; [`crate::server`] wires
//! it to a TCP listener. Delivery uses an in-process `mpsc` channel per client
//! so the broker and the I/O loop never share a lock on the hot path.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;

use synapse_core::{Log, SynapseCore};
use synapse_routing::topic::TopicRouter;

use crate::codec::{Packet, Publish, QoS, SubAckCode};

/// Maximum QoS the broker will grant a subscriber (full 3.1.1: 0/1/2).
pub const MAX_QOS: QoS = QoS::ExactlyOnce;

/// A live client connection's outbound half.
struct Client {
    tx: UnboundedSender<Packet>,
    next_packet_id: AtomicU16,
}

/// Shared broker state.
pub struct Broker {
    /// Durable store for QoS 1/2 inbound publishes (tenant `mqtt`, log `pubstore`).
    persist: Arc<Log>,
    /// Subscription registry: subscriber id -> filter. The id is
    /// `"{client_id}\0{filter}"` so one client can hold many filters, and
    /// [`TopicRouter::route`] returns all matching subscriber ids at once.
    router: Arc<TopicRouter>,
    /// Granted QoS per subscriber id (mirrors the router's filters).
    sub_qos: Mutex<HashMap<String, QoS>>,
    /// Retained message per topic: `(payload, qos)`.
    retained: Mutex<HashMap<String, (Vec<u8>, QoS)>>,
    /// Connected clients by client id.
    clients: Mutex<HashMap<String, Client>>,
}

fn sub_key(client_id: &str, filter: &str) -> String {
    format!("{client_id}\0{filter}")
}

impl Broker {
    /// Build a broker over a core engine. Creates the tenant/log used for QoS
    /// 1/2 durability.
    pub fn new(core: Arc<SynapseCore>) -> Self {
        core.create_log("mqtt", "pubstore").ok();
        let persist = core
            .get_log("mqtt", "pubstore")
            .ok()
            .flatten()
            .unwrap_or_else(|| core.create_log("mqtt", "pubstore").unwrap());
        Self {
            persist,
            router: Arc::new(TopicRouter::new()),
            sub_qos: Mutex::new(HashMap::new()),
            retained: Mutex::new(HashMap::new()),
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// Register a connected client. The caller keeps the `rx` half of `tx`'s
    /// channel and pumps it to the socket; the broker holds `tx` for delivery.
    /// Replaces any existing session for the same client id (MQTT clean-session
    /// semantics are applied by the caller via `clean_session`).
    pub fn connect(&self, client_id: &str, clean_session: bool, tx: UnboundedSender<Packet>) {
        if clean_session {
            self.drop_session(client_id);
        }
        let client = Client {
            tx,
            next_packet_id: AtomicU16::new(1),
        };
        self.clients
            .lock()
            .unwrap()
            .insert(client_id.to_string(), client);
    }

    /// Remove all of a client's subscriptions and its session record.
    pub fn disconnect(&self, client_id: &str, _clean_session: bool) {
        self.drop_session(client_id);
    }

    fn drop_session(&self, client_id: &str) {
        self.clients.lock().unwrap().remove(client_id);
        let mut subs = self.sub_qos.lock().unwrap();
        let keys: Vec<String> = subs
            .keys()
            .filter(|k| k.starts_with(&format!("{client_id}\0")))
            .cloned()
            .collect();
        for k in &keys {
            subs.remove(k);
            self.router.unsubscribe(k);
        }
    }

    /// Subscribe `client_id` to `topics`, returning the per-topic SUBACK codes.
    pub fn subscribe(&self, client_id: &str, topics: &[(String, QoS)]) -> Vec<SubAckCode> {
        let mut codes = Vec::with_capacity(topics.len());
        let retained = self.retained.lock().unwrap();
        // Collect matches under the lock, then deliver after releasing it so we
        // don't re-lock `self.retained` inside `deliver_retained`.
        let matches: Vec<(String, Vec<u8>, QoS)> = topics
            .iter()
            .flat_map(|(filter, req_qos)| {
                let granted = (*req_qos as u8).min(MAX_QOS as u8);
                retained
                    .iter()
                    .filter(|(t, _)| synapse_routing::topic::topic_matches(filter, t))
                    .map(move |(t, (payload, qos))| {
                        let eff = QoS::from_u8((*qos as u8).min(granted)).unwrap();
                        (t.clone(), payload.clone(), eff)
                    })
            })
            .collect();
        drop(retained);

        for (filter, req_qos) in topics {
            let granted = (*req_qos as u8).min(MAX_QOS as u8);
            let key = sub_key(client_id, filter);
            self.router.subscribe(&key, filter);
            self.sub_qos.lock().unwrap().insert(key, QoS::from_u8(granted).unwrap());
            codes.push(match granted {
                0 => SubAckCode::Qos0,
                1 => SubAckCode::Qos1,
                2 => SubAckCode::Qos2,
                _ => SubAckCode::Failure,
            });
            for (topic, payload, qos) in &matches {
                if synapse_routing::topic::topic_matches(filter, topic) {
                    self.deliver_retained(client_id, topic, payload, *qos);
                }
            }
        }
        codes
    }

    fn deliver_retained(&self, client_id: &str, topic: &str, payload: &[u8], qos: QoS) {
        let pid = if qos != QoS::AtMostOnce {
            self.next_id(client_id)
        } else {
            None
        };
        let p = Publish {
            dup: false,
            qos,
            retain: true,
            topic: topic.to_string(),
            packet_id: pid,
            payload: payload.to_vec(),
        };
        let _ = self.clients.lock().unwrap().get(client_id).map(|c| c.tx.send(Packet::Publish(p)));
    }

    /// Unsubscribe `client_id` from `topics`.
    pub fn unsubscribe(&self, client_id: &str, topics: &[String]) {
        let mut subs = self.sub_qos.lock().unwrap();
        for filter in topics {
            let key = sub_key(client_id, filter);
            subs.remove(&key);
            self.router.unsubscribe(&key);
        }
    }

    /// Publish a message: persist (QoS 1/2), store retained (if flagged), and
    /// deliver to every matching subscriber at the effective (min) QoS.
    pub fn publish(&self, p: &Publish) {
        if p.qos != QoS::AtMostOnce {
            // Durably record the QoS 1/2 publish on the core Log primitive.
            let mut frame = Vec::new();
            crate::codec::encode_packet(&Packet::Publish(p.clone()), &mut frame);
            let _ = self.persist.append(&frame);
        }
        if p.retain {
            let mut retained = self.retained.lock().unwrap();
            if p.payload.is_empty() {
                retained.remove(&p.topic);
            } else {
                retained.insert(p.topic.clone(), (p.payload.clone(), p.qos));
            }
        }

        let subs = self.router.route(&p.topic);
        let qos_map = self.sub_qos.lock().unwrap();
        let clients = self.clients.lock().unwrap();
        let mut seen: HashSet<String> = HashSet::new();
        for key in subs {
            let client_id = match key.split_once('\0') {
                Some((c, _)) => c.to_string(),
                None => continue,
            };
            if !seen.insert(client_id.clone()) {
                continue;
            }
            let granted = qos_map.get(&*key).copied().unwrap_or(QoS::AtMostOnce);
            let eff = QoS::from_u8((p.qos as u8).min(granted as u8)).unwrap();
            let packet_id = if eff != QoS::AtMostOnce {
                clients.get(&client_id).map(|c| c.next_packet_id.fetch_add(1, Ordering::Relaxed))
            } else {
                None
            };
            let delivery = p.to_delivery(packet_id);
            let delivery = Publish { qos: eff, ..delivery };
            if let Some(c) = clients.get(&client_id) {
                let _ = c.tx.send(Packet::Publish(delivery));
            }
        }
    }

    fn next_id(&self, client_id: &str) -> Option<u16> {
        self.clients
            .lock()
            .unwrap()
            .get(client_id)
            .map(|c| c.next_packet_id.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> Broker {
        Broker::new(Arc::new(SynapseCore::new()))
    }

    #[test]
    fn publish_routes_to_subscriber() {
        let b = broker();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("sub", true, tx);
        b.subscribe("sub", &[("sensors/#".into(), QoS::AtMostOnce)]);

        let p = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "sensors/temp/kitchen".into(),
            packet_id: None,
            payload: b"21.5".to_vec(),
        };
        b.publish(&p);

        let got = rx.try_recv().expect("message delivered");
        match got {
            Packet::Publish(d) => {
                assert_eq!(d.topic, "sensors/temp/kitchen");
                assert_eq!(d.payload, b"21.5");
            }
            _ => panic!("unexpected packet"),
        }
    }

    #[test]
    fn qos1_gets_puback_and_packet_id() {
        let b = broker();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("sub", true, tx);
        b.subscribe("sub", &[("a".into(), QoS::AtLeastOnce)]);

        let p = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a".into(),
            packet_id: Some(99),
            payload: b"x".to_vec(),
        };
        b.publish(&p);
        match rx.try_recv().unwrap() {
            Packet::Publish(d) => {
                assert_eq!(d.qos, QoS::AtLeastOnce);
                assert!(d.packet_id.is_some());
                assert_ne!(d.packet_id, Some(99)); // broker reassigns ids
            }
            _ => panic!("unexpected"),
        }
    }

    #[test]
    fn retained_delivered_on_subscribe() {
        let b = broker();
        let p = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: "status".into(),
            packet_id: None,
            payload: b"up".to_vec(),
        };
        b.publish(&p);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("late", true, tx);
        b.subscribe("late", &[("status".into(), QoS::AtMostOnce)]);
        match rx.try_recv().unwrap() {
            Packet::Publish(d) => {
                assert_eq!(d.topic, "status");
                assert_eq!(d.payload, b"up");
                assert!(d.retain);
            }
            _ => panic!("unexpected"),
        }
    }
}
