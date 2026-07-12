//! AMQP broker: exchange/queue/binding declarations, `basic.publish` routing
//! through the [`GraphRouter`] into the core [`Queue`] primitive, and
//! consumer push-delivery / `basic.get` / `basic.ack` (spec.txt §3.2, §6 Phase
//! 3 "Lite"). `basic.publish`/`get`/`ack` are at-least-once: a delivered-but-
//! unacked message is held in-flight in the [`Queue`] until acked or the
//! connection drops (the server-side broker can then redeliver via
//! [`Queue::nack`] on reconnect — out of scope for the Lite subset, which
//! leaves unacked messages in-flight).
//!
//! The broker is transport-agnostic; [`crate::server`] wires it to a TCP
//! listener, reusing the per-connection `mpsc` model of the other adapters.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;

use synapse_core::{Queue, SynapseCore};
use synapse_routing::graph::{ExchangeKind, GraphRouter};

use crate::codec::{
    parse_basic_ack, parse_basic_consume, parse_basic_get, parse_connection_open,
    parse_connection_start_ok, parse_connection_tune_ok, parse_exchange_declare, parse_queue_bind,
    parse_queue_declare, encode_basic_consume_ok, encode_basic_get_empty, encode_basic_get_ok,
    encode_basic_qos_ok, encode_channel_close_ok, encode_channel_open_ok, encode_connection_close_ok,
    encode_connection_open_ok, encode_connection_tune, encode_exchange_declare_ok,
    encode_header, encode_body, encode_method, encode_queue_bind_ok, encode_queue_declare_ok,
    CLASS_BASIC, CLASS_CHANNEL, CLASS_CONNECTION, CLASS_EXCHANGE, CLASS_QUEUE,
    METHOD_BASIC_ACK, METHOD_BASIC_CONSUME, METHOD_BASIC_GET, METHOD_BASIC_QOS,
    METHOD_CHANNEL_CLOSE, METHOD_CHANNEL_OPEN, METHOD_CONNECTION_CLOSE, METHOD_CONNECTION_OPEN,
    METHOD_CONNECTION_START_OK, METHOD_CONNECTION_TUNE_OK, METHOD_EXCHANGE_DECLARE,
    METHOD_QUEUE_BIND, METHOD_QUEUE_DECLARE,
};
use crate::codec::ProtocolError;

const TENANT: &str = "amqp";

/// An event the server writes to the socket for one connection.
pub enum ServerEvent {
    /// One or more already-encoded frames to write contiguously.
    Bytes(Vec<u8>),
    /// A server-initiated `basic.deliver` push (server appends header + body).
    Deliver(DeliverMsg),
}

/// Payload of a `basic.deliver` push.
pub struct DeliverMsg {
    pub channel: u16,
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub exchange: String,
    pub routing_key: String,
    pub body: Vec<u8>,
}

#[derive(Default)]
struct ConnState {
    tx: Option<UnboundedSender<ServerEvent>>,
    delivery_tags: Mutex<HashMap<u16, u64>>,
}

struct Consumer {
    conn_id: u64,
    channel: u16,
    tag: String,
    no_ack: bool,
}

#[derive(Default)]
struct ConsumerGroup {
    consumers: Vec<Consumer>,
    rr: usize,
}

/// Shared AMQP broker state.
pub struct Broker {
    core: Arc<SynapseCore>,
    graph: Arc<GraphRouter>,
    queues: Mutex<HashMap<String, Arc<Queue>>>,
    connections: Mutex<HashMap<u64, ConnState>>,
    consumers: Mutex<HashMap<String, ConsumerGroup>>,
    inflight: Mutex<HashMap<(u64, u16, u64), (String, u64)>>,
    next_conn: AtomicU64,
    next_name: AtomicU64,
}

impl Broker {
    /// Build a broker over a core engine, pre-declaring the default topic/
    /// direct/fanout exchanges AMQP clients expect.
    pub fn new(core: Arc<SynapseCore>) -> Self {
        let graph = Arc::new(GraphRouter::new());
        graph.create_exchange("amq.direct", ExchangeKind::Direct);
        graph.create_exchange("amq.topic", ExchangeKind::Topic);
        graph.create_exchange("amq.fanout", ExchangeKind::Fanout);
        Self {
            core,
            graph,
            queues: Mutex::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
            consumers: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            next_conn: AtomicU64::new(1),
            next_name: AtomicU64::new(1),
        }
    }

    /// Register a new connection (called after the protocol header). Returns
    /// the connection id used for subsequent per-method dispatch.
    pub fn connect(&self, tx: UnboundedSender<ServerEvent>) -> u64 {
        let id = self.next_conn.fetch_add(1, Ordering::Relaxed);
        self.connections
            .lock()
            .unwrap()
            .insert(id, ConnState { tx: Some(tx), delivery_tags: Mutex::new(HashMap::new()) });
        id
    }

    /// Tear down a connection: drop its consumers and in-flight deliveries.
    pub fn disconnect(&self, conn_id: u64) {
        self.connections.lock().unwrap().remove(&conn_id);
        // Remove consumers owned by this connection.
        let mut groups = self.consumers.lock().unwrap();
        for group in groups.values_mut() {
            group.consumers.retain(|c| c.conn_id != conn_id);
        }
        groups.retain(|_, g| !g.consumers.is_empty());
        drop(groups);
        self.inflight.lock().unwrap().retain(|(c, _, _), _| *c != conn_id);
    }

    fn ensure_queue(&self, name: &str) -> Option<Arc<Queue>> {
        if let Some(q) = self.queues.lock().unwrap().get(name) {
            return Some(q.clone());
        }
        let q = self.core.create_queue(TENANT, name).ok()?;
        self.graph.create_queue(name);
        self.queues.lock().unwrap().insert(name.to_string(), q.clone());
        Some(q)
    }

    fn next_delivery_tag(&self, conn_id: u64, channel: u16) -> u64 {
        let conns = self.connections.lock().unwrap();
        let c = conns.get(&conn_id).expect("live connection");
        let mut tags = c.delivery_tags.lock().unwrap();
        let entry = tags.entry(channel).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Dispatch one decoded method frame. Returns the encoded response frames
    /// (handshake replies, declare-ok, get-ok/empty, etc.). `basic.publish` is
    /// handled separately by the server (it spans header + body frames).
    pub fn handle_method(
        &self,
        conn_id: u64,
        channel: u16,
        class: u16,
        method: u16,
        args: &[u8],
    ) -> Result<Vec<ServerEvent>, ProtocolError> {
        match (class, method) {
            (CLASS_CONNECTION, METHOD_CONNECTION_START_OK) => {
                parse_connection_start_ok(args)?;
                Ok(vec![ServerEvent::Bytes(encode_connection_tune())])
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_TUNE_OK) => {
                parse_connection_tune_ok(args)?;
                Ok(vec![])
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_OPEN) => {
                parse_connection_open(args)?;
                Ok(vec![ServerEvent::Bytes(encode_connection_open_ok())])
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE) => {
                Ok(vec![ServerEvent::Bytes(encode_connection_close_ok())])
            }
            (CLASS_CHANNEL, METHOD_CHANNEL_OPEN) => {
                Ok(vec![ServerEvent::Bytes(encode_channel_open_ok(channel))])
            }
            (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE) => {
                Ok(vec![ServerEvent::Bytes(encode_channel_close_ok(channel))])
            }
            (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE) => {
                let (exchange, kind, passive, _durable) = parse_exchange_declare(args)?;
                if passive && self.graph.route(&exchange, "").is_err() {
                    return Ok(vec![ServerEvent::Bytes(encode_connection_close(
                        channel, 404, 0,
                    ))]);
                }
                let ek = match kind.as_str() {
                    "topic" => ExchangeKind::Topic,
                    "fanout" => ExchangeKind::Fanout,
                    _ => ExchangeKind::Direct,
                };
                self.graph.create_exchange(&exchange, ek);
                Ok(vec![ServerEvent::Bytes(encode_exchange_declare_ok(channel))])
            }
            (CLASS_QUEUE, METHOD_QUEUE_DECLARE) => {
                let (queue, _passive, _durable) = parse_queue_declare(args)?;
                let name = if queue.is_empty() {
                    let n = self.next_name.fetch_add(1, Ordering::Relaxed);
                    format!("synapse.gen.{n}")
                } else {
                    queue
                };
                let q = self
                    .ensure_queue(&name)
                    .ok_or_else(|| ProtocolError::Malformed("queue create failed"))?;
                let consumers = self
                    .consumers
                    .lock()
                    .unwrap()
                    .get(&name)
                    .map(|g| g.consumers.len() as u32)
                    .unwrap_or(0);
                Ok(vec![ServerEvent::Bytes(encode_queue_declare_ok(
                    channel,
                    &name,
                    q.depth() as u32,
                    consumers,
                ))])
            }
            (CLASS_QUEUE, METHOD_QUEUE_BIND) => {
                let (queue, exchange, routing_key) = parse_queue_bind(args)?;
                // The default (nameless) direct exchange routes by exact
                // routing-key == queue-name, so binding to it is implicit.
                if !exchange.is_empty() {
                    self.ensure_queue(&queue);
                    self.graph.bind(&exchange, &queue, &routing_key).ok();
                }
                Ok(vec![ServerEvent::Bytes(encode_queue_bind_ok(channel))])
            }
            (CLASS_BASIC, METHOD_BASIC_QOS) => {
                Ok(vec![ServerEvent::Bytes(encode_basic_qos_ok(channel))])
            }
            (CLASS_BASIC, METHOD_BASIC_CONSUME) => {
                let (queue, tag, no_ack) = parse_basic_consume(args)?;
                let name = if queue.is_empty() {
                    let n = self.next_name.fetch_add(1, Ordering::Relaxed);
                    format!("synapse.gen.{n}")
                } else {
                    queue
                };
                let consumer_tag = if tag.is_empty() {
                    let n = self.next_name.fetch_add(1, Ordering::Relaxed);
                    format!("synapse.ctag.{n}")
                } else {
                    tag
                };
                self.ensure_queue(&name);
                let mut groups = self.consumers.lock().unwrap();
                let group = groups.entry(name.clone()).or_default();
                group.consumers.push(Consumer {
                    conn_id,
                    channel,
                    tag: consumer_tag.clone(),
                    no_ack,
                });
                drop(groups);
                Ok(vec![ServerEvent::Bytes(encode_basic_consume_ok(
                    channel,
                    &consumer_tag,
                ))])
            }
            (CLASS_BASIC, METHOD_BASIC_GET) => {
                let (queue, no_ack) = parse_basic_get(args)?;
                let q = self.ensure_queue(&queue);
                let (seq, body) = match q.and_then(|q| q.dequeue()) {
                    Some((seq, body)) => (seq, body),
                    None => {
                        return Ok(vec![ServerEvent::Bytes(encode_basic_get_empty(channel))]);
                    }
                };
                let tag = self.next_delivery_tag(conn_id, channel);
                if no_ack {
                    if let Some(q) = self.queues.lock().unwrap().get(&queue) {
                        q.ack(seq);
                    }
                } else {
                    self.inflight
                        .lock()
                        .unwrap()
                        .insert((conn_id, channel, tag), (queue.clone(), seq));
                }
                let mut out = encode_basic_get_ok(
                    channel,
                    tag,
                    false,
                    "",
                    &queue,
                    self.queues
                        .lock()
                        .unwrap()
                        .get(&queue)
                        .map(|q| q.depth() as u32)
                        .unwrap_or(0),
                );
                out.extend_from_slice(&encode_header(channel, CLASS_BASIC, body.len() as u64));
                out.extend_from_slice(&encode_body(channel, &body));
                Ok(vec![ServerEvent::Bytes(out)])
            }
            (CLASS_BASIC, METHOD_BASIC_ACK) => {
                let (tag, _multiple) = parse_basic_ack(args)?;
                if let Some((queue, seq)) =
                    self.inflight.lock().unwrap().remove(&(conn_id, channel, tag))
                {
                    if let Some(q) = self.queues.lock().unwrap().get(&queue) {
                        q.ack(seq);
                    }
                }
                Ok(vec![])
            }
            _ => Ok(vec![]),
        }
    }

    /// Publish `body` to `exchange` under `routing_key`, routing through the
    /// [`GraphRouter`] and enqueuing into each matched queue, then pushing to
    /// any consumers.
    pub fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
    ) -> Result<(), ProtocolError> {
        let targets: Vec<String> = if exchange.is_empty() {
            // Default direct exchange: routing key names the queue.
            vec![routing_key.to_string()]
        } else {
            self.graph.route(exchange, routing_key).map_err(|_| {
                ProtocolError::Malformed("unknown exchange")
            })?
        };

        for queue_name in targets {
            let q = match self.ensure_queue(&queue_name) {
                Some(q) => q,
                None => continue,
            };
            let seq = match self.core.queue_enqueue(TENANT, &queue_name, body) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Deliver to a consumer if one exists; otherwise leave queued for
            // basic.get.
            let consumer = {
                let mut groups = self.consumers.lock().unwrap();
                match groups.get_mut(&queue_name) {
                    Some(g) if !g.consumers.is_empty() => {
                        let idx = g.rr % g.consumers.len();
                        g.rr += 1;
                        Some(g.consumers[idx].clone())
                    }
                    _ => None,
                }
            };
            if let Some(c) = consumer {
                // Dequeue to mark in-flight, then push to the consumer's connection.
                let (delivered_seq, payload) = match q.dequeue() {
                    Some(d) => d,
                    None => (seq, body.to_vec()),
                };
                let _ = delivered_seq;
                let tag = self.next_delivery_tag(c.conn_id, c.channel);
                let conns = self.connections.lock().unwrap();
                let tx = conns.get(&c.conn_id).and_then(|c| c.tx.clone());
                drop(conns);
                if c.no_ack {
                    q.ack(seq);
                } else {
                    self.inflight
                        .lock()
                        .unwrap()
                        .insert((c.conn_id, c.channel, tag), (queue_name.clone(), seq));
                }
                if let Some(tx) = tx {
                    let _ = tx.send(ServerEvent::Deliver(DeliverMsg {
                        channel: c.channel,
                        consumer_tag: c.tag.clone(),
                        delivery_tag: tag,
                        exchange: exchange.to_string(),
                        routing_key: routing_key.to_string(),
                        body: payload,
                    }));
                }
            }
        }
        Ok(())
    }
}

impl Clone for Consumer {
    fn clone(&self) -> Self {
        Consumer {
            conn_id: self.conn_id,
            channel: self.channel,
            tag: self.tag.clone(),
            no_ack: self.no_ack,
        }
    }
}

/// Helper to build a `connection.close` frame (used for hard errors). Kept here
/// so the broker can reject invalid passive declarations uniformly.
fn encode_connection_close(channel: u16, _reply_code: u16, _class: u16) -> Vec<u8> {
    let mut w = crate::codec::Writer::new();
    w.u16(404); // reply-code: not found
    w.short_str("not found");
    w.u16(0); // failing-class
    w.u16(0); // failing-method
    encode_method(channel, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE, &w.buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> Broker {
        Broker::new(Arc::new(SynapseCore::new()))
    }

    #[test]
    fn declare_exchange_queue_and_bind() {
        let b = broker();
        let _ = b.handle_method(
            1,
            1,
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DECLARE,
            &exchange_declare_args("orders", "topic"),
        );
        let resp = b.handle_method(
            1,
            1,
            CLASS_QUEUE,
            METHOD_QUEUE_DECLARE,
            &queue_declare_args("jobs"),
        );
        assert!(matches!(resp, Ok(_)));
        let _ = b.handle_method(
            1,
            1,
            CLASS_QUEUE,
            METHOD_QUEUE_BIND,
            &queue_bind_args("jobs", "orders", "job.#"),
        );
        // Publishing to orders/job.x should route into jobs.
        b.publish("orders", "job.x", b"work").unwrap();
        let q = b.queues.lock().unwrap().get("jobs").cloned().unwrap();
        assert_eq!(q.depth(), 1);
    }

    #[test]
    fn fanout_delivers_to_all_bound_queues() {
        let b = broker();
        let _ = b.handle_method(
            1,
            1,
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DECLARE,
            &exchange_declare_args("logs", "fanout"),
        );
        let _ = b.handle_method(1, 1, CLASS_QUEUE, METHOD_QUEUE_DECLARE, &queue_declare_args("q1"));
        let _ = b.handle_method(1, 1, CLASS_QUEUE, METHOD_QUEUE_DECLARE, &queue_declare_args("q2"));
        let _ = b.handle_method(
            1,
            1,
            CLASS_QUEUE,
            METHOD_QUEUE_BIND,
            &queue_bind_args("q1", "logs", ""),
        );
        let _ = b.handle_method(
            1,
            1,
            CLASS_QUEUE,
            METHOD_QUEUE_BIND,
            &queue_bind_args("q2", "logs", ""),
        );
        b.publish("logs", "anything", b"hi").unwrap();
        assert_eq!(b.queues.lock().unwrap().get("q1").unwrap().depth(), 1);
        assert_eq!(b.queues.lock().unwrap().get("q2").unwrap().depth(), 1);
    }

    // --- arg builders mirroring the codec for broker unit tests -----------

    fn exchange_declare_args(name: &str, kind: &str) -> Vec<u8> {
        let mut w = crate::codec::Writer::new();
        w.u16(0);
        w.long_str(name);
        w.long_str(kind);
        w.bit(false); // passive
        w.bit(true); // durable
        w.bit(false); // auto-delete
        w.bit(false); // internal
        w.bit(false); // nowait
        w.u32(0); // arguments table
        w.into_bytes()
    }

    fn queue_declare_args(name: &str) -> Vec<u8> {
        let mut w = crate::codec::Writer::new();
        w.u16(0);
        w.long_str(name);
        w.bit(false);
        w.bit(true);
        w.bit(false);
        w.bit(false);
        w.bit(false);
        w.u32(0);
        w.into_bytes()
    }

    fn queue_bind_args(queue: &str, exchange: &str, key: &str) -> Vec<u8> {
        let mut w = crate::codec::Writer::new();
        w.u16(0);
        w.long_str(queue);
        w.long_str(exchange);
        w.long_str(key);
        w.bit(false);
        w.u32(0);
        w.into_bytes()
    }
}
