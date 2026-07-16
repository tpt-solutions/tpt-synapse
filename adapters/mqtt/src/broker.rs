//! MQTT broker: session registry, subscription matching, QoS 0/1/2 delivery,
//! retained messages, and QoS 1/2 durability through the core [`Log`] primitive
//! (spec.txt §3.3, §6 Phase 2).
//!
//! The broker is protocol-agnostic about the transport; [`crate::server`] wires
//! it to a TCP listener. Delivery uses an in-process `mpsc` channel per client
//! so the broker and the I/O loop never share a lock on the hot path.
//!
//! The broker itself is also agnostic about MQTT *wire* version (3.1.1 vs
//!5.0) — it works purely in terms of [`Publish`]/[`SubscribeTopic`] values;
//! `server.rs` is responsible for encoding/decoding those according to the
//! negotiated [`crate::codec::ProtocolVersion`] for a given connection. v5
//! features that need broker-side behavior (No Local, Retain As Published,
//! Retain Handling, message expiry, subscription identifiers, shared
//! subscriptions) are implemented here since they affect routing/delivery
//! rather than wire framing.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::UnboundedSender;

use synapse_core::{Log, SynapseCore};
use synapse_routing::topic::TopicRouter;

use crate::codec::{Packet, Properties, ProtocolVersion, Publish, QoS, RetainHandling, SubAckCode, SubscribeTopic};

/// Maximum QoS the broker will grant a subscriber (full 3.1.1/5.0: 0/1/2).
pub const MAX_QOS: QoS = QoS::ExactlyOnce;

/// A live client connection's outbound half.
struct Client {
    tx: UnboundedSender<Packet>,
    next_packet_id: AtomicU16,
}

/// Per-subscription state, keyed the same way as the router (`sub_key`).
/// Carries the v5 subscribe options that affect delivery: granted QoS,
/// whether the publisher's own messages should be suppressed (No Local),
/// whether the original publish's retain flag should be preserved on
/// delivery (Retain As Published), and the subscription identifier (if any)
/// to echo back on delivered messages.
#[derive(Debug, Clone, Copy)]
struct SubState {
    qos: QoS,
    no_local: bool,
    retain_as_published: bool,
    subscription_identifier: Option<u32>,
}

/// A retained message, with an optional absolute expiry derived from the v5
/// `message_expiry_interval` property (v3.1.1 retained messages never
/// expire).
#[derive(Clone)]
struct RetainedMessage {
    payload: Vec<u8>,
    qos: QoS,
    expires_at: Option<Instant>,
}

impl RetainedMessage {
    fn expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|t| now >= t)
    }
}

/// Shared broker state.
pub struct Broker {
    /// Durable store for QoS 1/2 inbound publishes (tenant `mqtt`, log `pubstore`).
    persist: Arc<Log>,
    /// Subscription registry: subscriber id -> filter. The id is
    /// `"{client_id}\0{filter}"` so one client can hold many filters, and
    /// [`TopicRouter::route`] returns all matching subscriber ids at once.
    /// For shared subscriptions, `filter` is the full `$share/{group}/{real}`
    /// string, but the *router* is subscribed under the real filter only
    /// (see [`parse_shared`]) so topic matching ignores the share prefix.
    router: Arc<TopicRouter>,
    /// Per-subscriber state (granted QoS + v5 subscribe options), mirroring
    /// the router's filters.
    sub_state: Mutex<HashMap<String, SubState>>,
    /// Retained message per topic.
    retained: Mutex<HashMap<String, RetainedMessage>>,
    /// Connected clients by client id.
    clients: Mutex<HashMap<String, Client>>,
    /// Shared-subscription group membership: `(group, real_filter) ->
    /// (member client ids, round-robin cursor)`.
    shared_members: Mutex<HashMap<(String, String), (Vec<String>, usize)>>,
}

fn sub_key(client_id: &str, filter: &str) -> String {
    format!("{client_id}\0{filter}")
}

/// Parse a shared-subscription filter (`$share/{group}/{real_filter}`),
/// returning `(group, real_filter)`. Ordinary filters (including other
/// `$`-prefixed topics) return `None`.
fn parse_shared(filter: &str) -> Option<(&str, &str)> {
    let rest = filter.strip_prefix("$share/")?;
    let (group, real_filter) = rest.split_once('/')?;
    if group.is_empty() || real_filter.is_empty() {
        return None;
    }
    Some((group, real_filter))
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
            sub_state: Mutex::new(HashMap::new()),
            retained: Mutex::new(HashMap::new()),
            clients: Mutex::new(HashMap::new()),
            shared_members: Mutex::new(HashMap::new()),
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
        let mut subs = self.sub_state.lock().unwrap();
        let prefix = format!("{client_id}\0");
        let keys: Vec<String> = subs.keys().filter(|k| k.starts_with(&prefix)).cloned().collect();
        for k in &keys {
            subs.remove(k);
            self.router.unsubscribe(k);
            if let Some(filter) = k.strip_prefix(&prefix) {
                if let Some((group, real_filter)) = parse_shared(filter) {
                    self.remove_shared_member(group, real_filter, client_id);
                }
            }
        }
    }

    fn remove_shared_member(&self, group: &str, real_filter: &str, client_id: &str) {
        let mut groups = self.shared_members.lock().unwrap();
        if let Some((members, cursor)) = groups.get_mut(&(group.to_string(), real_filter.to_string())) {
            if let Some(pos) = members.iter().position(|m| m == client_id) {
                members.remove(pos);
                if members.is_empty() {
                    *cursor = 0;
                } else {
                    *cursor %= members.len();
                }
            }
        }
    }

    /// Subscribe `client_id` to `topics`, returning the per-topic SUBACK codes.
    /// `subscription_identifier` is the v5 packet-level Subscription
    /// Identifier property (if any), applied to every filter in this
    /// SUBSCRIBE.
    pub fn subscribe(
        &self,
        client_id: &str,
        topics: &[SubscribeTopic],
        subscription_identifier: Option<u32>,
    ) -> Vec<SubAckCode> {
        let mut codes = Vec::with_capacity(topics.len());
        let now = Instant::now();
        let retained = self.retained.lock().unwrap();
        // Collect matches under the lock, then deliver after releasing it so we
        // don't re-lock `self.retained` inside `deliver_retained`.
        let matches: Vec<(String, Vec<u8>, QoS)> = topics
            .iter()
            .flat_map(|t| {
                let granted = (t.qos as u8).min(MAX_QOS as u8);
                let real_filter = parse_shared(&t.filter).map(|(_, f)| f).unwrap_or(&t.filter);
                retained
                    .iter()
                    .filter(move |(topic, r)| !r.expired(now) && synapse_routing::topic::topic_matches(real_filter, topic))
                    .map(move |(topic, r)| {
                        let eff = QoS::from_u8((r.qos as u8).min(granted)).unwrap();
                        (topic.clone(), r.payload.clone(), eff)
                    })
            })
            .collect();
        drop(retained);

        for t in topics {
            let granted = (t.qos as u8).min(MAX_QOS as u8);
            let shared = parse_shared(&t.filter);
            let real_filter = shared.map(|(_, f)| f).unwrap_or(&t.filter);
            let key = sub_key(client_id, &t.filter);
            let already_subscribed = self.sub_state.lock().unwrap().contains_key(&key);
            self.router.subscribe(&key, real_filter);
            self.sub_state.lock().unwrap().insert(
                key,
                SubState {
                    qos: QoS::from_u8(granted).unwrap(),
                    no_local: t.no_local,
                    retain_as_published: t.retain_as_published,
                    subscription_identifier,
                },
            );
            if let Some((group, filter)) = shared {
                let mut members = self.shared_members.lock().unwrap();
                let entry = members
                    .entry((group.to_string(), filter.to_string()))
                    .or_insert_with(|| (Vec::new(), 0));
                if !entry.0.iter().any(|m| m == client_id) {
                    entry.0.push(client_id.to_string());
                }
            }
            codes.push(match granted {
                0 => SubAckCode::Qos0,
                1 => SubAckCode::Qos1,
                2 => SubAckCode::Qos2,
                _ => SubAckCode::Failure,
            });

            // Shared subscriptions never receive retained messages (spec
            // §4.8.2), regardless of Retain Handling.
            if shared.is_none() {
                let skip_retained = match t.retain_handling {
                    RetainHandling::DoNotSend => true,
                    RetainHandling::SendIfNewSubscription => already_subscribed,
                    RetainHandling::SendAtSubscribe => false,
                };
                if !skip_retained {
                    for (topic, payload, qos) in &matches {
                        if synapse_routing::topic::topic_matches(real_filter, topic) {
                            self.deliver_retained(client_id, topic, payload, *qos);
                        }
                    }
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
            properties: None,
        };
        let _ = self.clients.lock().unwrap().get(client_id).map(|c| c.tx.send(Packet::Publish(p)));
    }

    /// Unsubscribe `client_id` from `topics`.
    pub fn unsubscribe(&self, client_id: &str, topics: &[String]) {
        let mut subs = self.sub_state.lock().unwrap();
        for filter in topics {
            let key = sub_key(client_id, filter);
            subs.remove(&key);
            self.router.unsubscribe(&key);
            if let Some((group, real_filter)) = parse_shared(filter) {
                self.remove_shared_member(group, real_filter, client_id);
            }
        }
    }

    /// Publish a message: persist (QoS 1/2), store retained (if flagged), and
    /// deliver to every matching subscriber at the effective (min) QoS.
    /// `publisher_id` (if known) is used to enforce v5 No Local subscriptions.
    pub fn publish(&self, publisher_id: Option<&str>, p: &Publish) {
        if p.qos != QoS::AtMostOnce {
            // Durably record the QoS 1/2 publish on the core Log primitive.
            // Internal storage format only — always encoded as v3.1.1 framing
            // since this is never sent over a real wire.
            let mut frame = Vec::new();
            crate::codec::encode_packet(&Packet::Publish(p.clone()), ProtocolVersion::V311, &mut frame);
            let _ = self.persist.append(&frame);
        }
        if p.retain {
            let mut retained = self.retained.lock().unwrap();
            if p.payload.is_empty() {
                retained.remove(&p.topic);
            } else {
                let expires_at = p
                    .properties
                    .as_ref()
                    .and_then(|props| props.message_expiry_interval)
                    .map(|secs| Instant::now() + Duration::from_secs(secs as u64));
                retained.insert(p.topic.clone(), RetainedMessage { payload: p.payload.clone(), qos: p.qos, expires_at });
            }
        }

        let subs = self.router.route(&p.topic);
        let state_map = self.sub_state.lock().unwrap();
        let clients = self.clients.lock().unwrap();

        // Partition matches into direct (deliver to every matching client)
        // and shared-subscription (deliver to exactly one round-robin member
        // per matching group).
        let mut seen: HashSet<String> = HashSet::new();
        let mut direct_keys: Vec<Arc<str>> = Vec::new();
        let mut shared_groups_matched: HashSet<(String, String)> = HashSet::new();
        for key in &subs {
            let (client_id, filter) = match key.split_once('\0') {
                Some(v) => v,
                None => continue,
            };
            if let Some((group, real_filter)) = parse_shared(filter) {
                shared_groups_matched.insert((group.to_string(), real_filter.to_string()));
            } else if seen.insert(client_id.to_string()) {
                direct_keys.push(key.clone());
            }
        }

        let mut delivery_keys: Vec<Arc<str>> = direct_keys;
        if !shared_groups_matched.is_empty() {
            let mut groups = self.shared_members.lock().unwrap();
            for (group, filter) in shared_groups_matched {
                if let Some((members, cursor)) = groups.get_mut(&(group.clone(), filter.clone())) {
                    let n = members.len();
                    if n == 0 {
                        continue;
                    }
                    for i in 0..n {
                        let idx = (*cursor + i) % n;
                        let candidate = &members[idx];
                        if clients.contains_key(candidate) {
                            let key_str = sub_key(candidate, &format!("$share/{group}/{filter}"));
                            delivery_keys.push(Arc::from(key_str));
                            *cursor = (idx + 1) % n;
                            break;
                        }
                    }
                }
            }
        }

        for key in delivery_keys {
            let (client_id, _filter) = match key.split_once('\0') {
                Some(v) => v,
                None => continue,
            };
            let state = state_map.get(&*key).copied();
            if let Some(publisher) = publisher_id {
                if client_id == publisher && state.is_some_and(|s| s.no_local) {
                    continue;
                }
            }
            let granted = state.map(|s| s.qos).unwrap_or(QoS::AtMostOnce);
            let eff = QoS::from_u8((p.qos as u8).min(granted as u8)).unwrap();
            let packet_id = if eff != QoS::AtMostOnce {
                clients.get(client_id).map(|c| c.next_packet_id.fetch_add(1, Ordering::Relaxed))
            } else {
                None
            };
            let mut delivery = p.to_delivery(packet_id);
            delivery.qos = eff;
            if let Some(state) = state {
                delivery.retain = if state.retain_as_published { p.retain } else { false };
                if let Some(sub_id) = state.subscription_identifier {
                    delivery.properties = Some(Properties {
                        subscription_identifier: Some(sub_id),
                        ..Default::default()
                    });
                }
            }
            if let Some(c) = clients.get(client_id) {
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
    use crate::codec::QoS;

    fn broker() -> Broker {
        Broker::new(Arc::new(SynapseCore::new()))
    }

    fn topic(filter: &str, qos: QoS) -> SubscribeTopic {
        SubscribeTopic {
            filter: filter.to_string(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAtSubscribe,
        }
    }

    fn publish(topic: &str, qos: QoS, retain: bool, payload: &[u8]) -> Publish {
        Publish {
            dup: false,
            qos,
            retain,
            topic: topic.to_string(),
            // 99 is deliberately far from the broker's own packet-id
            // sequence (which starts at 1), so tests can assert the broker
            // reassigns a fresh id rather than reusing the publisher's.
            packet_id: if qos != QoS::AtMostOnce { Some(99) } else { None },
            payload: payload.to_vec(),
            properties: None,
        }
    }

    #[test]
    fn publish_routes_to_subscriber() {
        let b = broker();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("sub", true, tx);
        b.subscribe("sub", &[topic("sensors/#", QoS::AtMostOnce)], None);

        let p = publish("sensors/temp/kitchen", QoS::AtMostOnce, false, b"21.5");
        b.publish(None, &p);

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
        b.subscribe("sub", &[topic("a", QoS::AtLeastOnce)], None);

        let p = publish("a", QoS::AtLeastOnce, false, b"x");
        b.publish(None, &p);
        match rx.try_recv().unwrap() {
            Packet::Publish(d) => {
                assert_eq!(d.qos, QoS::AtLeastOnce);
                assert!(d.packet_id.is_some());
                assert_ne!(d.packet_id, Some(99)); // broker reassigns ids, not reusing the publisher's
            }
            _ => panic!("unexpected"),
        }
    }

    #[test]
    fn retained_delivered_on_subscribe() {
        let b = broker();
        let p = publish("status", QoS::AtMostOnce, true, b"up");
        b.publish(None, &p);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("late", true, tx);
        b.subscribe("late", &[topic("status", QoS::AtMostOnce)], None);
        match rx.try_recv().unwrap() {
            Packet::Publish(d) => {
                assert_eq!(d.topic, "status");
                assert_eq!(d.payload, b"up");
                assert!(d.retain);
            }
            _ => panic!("unexpected"),
        }
    }

    #[test]
    fn no_local_suppresses_self_publish() {
        let b = broker();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("me", true, tx);
        b.subscribe(
            "me",
            &[SubscribeTopic { no_local: true, ..topic("a", QoS::AtMostOnce) }],
            None,
        );

        let p = publish("a", QoS::AtMostOnce, false, b"x");
        b.publish(Some("me"), &p);
        assert!(rx.try_recv().is_err(), "no_local subscriber must not receive its own publish");

        b.publish(Some("someone-else"), &p);
        assert!(rx.try_recv().is_ok(), "no_local only suppresses the publisher's own messages");
    }

    #[test]
    fn retain_as_published_preserves_flag() {
        let b = broker();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("sub", true, tx);
        b.subscribe(
            "sub",
            &[SubscribeTopic { retain_as_published: true, ..topic("a", QoS::AtMostOnce) }],
            None,
        );

        let p = publish("a", QoS::AtMostOnce, true, b"x");
        b.publish(None, &p);
        match rx.try_recv().unwrap() {
            Packet::Publish(d) => assert!(d.retain, "retain_as_published must preserve the original retain flag"),
            _ => panic!("unexpected"),
        }
    }

    #[test]
    fn retain_handling_do_not_send_skips_retained() {
        let b = broker();
        let p = publish("status", QoS::AtMostOnce, true, b"up");
        b.publish(None, &p);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("late", true, tx);
        b.subscribe(
            "late",
            &[SubscribeTopic { retain_handling: RetainHandling::DoNotSend, ..topic("status", QoS::AtMostOnce) }],
            None,
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn retain_handling_send_if_new_skips_on_resubscribe() {
        let b = broker();
        let p = publish("status", QoS::AtMostOnce, true, b"up");
        b.publish(None, &p);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("client", true, tx);
        let opts = SubscribeTopic { retain_handling: RetainHandling::SendIfNewSubscription, ..topic("status", QoS::AtMostOnce) };
        b.subscribe("client", &[opts.clone()], None);
        assert!(rx.try_recv().is_ok(), "first subscription is new, retained should be sent");

        b.subscribe("client", &[opts], None);
        assert!(rx.try_recv().is_err(), "resubscribing to the same filter is not a new subscription");
    }

    #[test]
    fn message_expiry_interval_expires_retained() {
        let b = broker();
        let mut p = publish("status", QoS::AtMostOnce, true, b"up");
        p.properties = Some(Properties { message_expiry_interval: Some(0), ..Default::default() });
        b.publish(None, &p);
        std::thread::sleep(Duration::from_millis(5));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("late", true, tx);
        b.subscribe("late", &[topic("status", QoS::AtMostOnce)], None);
        assert!(rx.try_recv().is_err(), "expired retained message must not be delivered");
    }

    #[test]
    fn subscription_identifier_echoed_on_delivery() {
        let b = broker();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("sub", true, tx);
        b.subscribe("sub", &[topic("a", QoS::AtMostOnce)], Some(42));

        let p = publish("a", QoS::AtMostOnce, false, b"x");
        b.publish(None, &p);
        match rx.try_recv().unwrap() {
            Packet::Publish(d) => {
                assert_eq!(d.properties.and_then(|p| p.subscription_identifier), Some(42));
            }
            _ => panic!("unexpected"),
        }
    }

    #[test]
    fn shared_subscription_round_robins_across_members() {
        let b = broker();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
        b.connect("w1", true, tx1);
        b.connect("w2", true, tx2);
        b.subscribe("w1", &[topic("$share/g/work", QoS::AtMostOnce)], None);
        b.subscribe("w2", &[topic("$share/g/work", QoS::AtMostOnce)], None);

        for i in 0..4 {
            let p = publish("work", QoS::AtMostOnce, false, format!("job{i}").as_bytes());
            b.publish(None, &p);
        }

        let c1 = std::iter::from_fn(|| rx1.try_recv().ok()).count();
        let c2 = std::iter::from_fn(|| rx2.try_recv().ok()).count();
        assert_eq!(c1 + c2, 4, "every message delivered exactly once across the group");
        assert_eq!(c1, 2);
        assert_eq!(c2, 2);
    }

    #[test]
    fn shared_subscription_no_retained_delivery() {
        let b = broker();
        let p = publish("work", QoS::AtMostOnce, true, b"retained");
        b.publish(None, &p);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        b.connect("w1", true, tx);
        b.subscribe("w1", &[topic("$share/g/work", QoS::AtMostOnce)], None);
        assert!(rx.try_recv().is_err(), "shared subscriptions must never receive retained messages");
    }

    #[test]
    fn shared_subscription_removes_dead_member_on_disconnect() {
        let b = broker();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
        b.connect("w1", true, tx1);
        b.connect("w2", true, tx2);
        b.subscribe("w1", &[topic("$share/g/work", QoS::AtMostOnce)], None);
        b.subscribe("w2", &[topic("$share/g/work", QoS::AtMostOnce)], None);
        b.disconnect("w1", true);

        for i in 0..3 {
            let p = publish("work", QoS::AtMostOnce, false, format!("job{i}").as_bytes());
            b.publish(None, &p);
        }
        let c1 = std::iter::from_fn(|| rx1.try_recv().ok()).count();
        let c2 = std::iter::from_fn(|| rx2.try_recv().ok()).count();
        assert_eq!(c1, 0);
        assert_eq!(c2, 3);
    }
}
