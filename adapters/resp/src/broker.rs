//! RESP (Redis) broker: maps the Redis command surface onto the unified core
//! primitives (spec.txt §6 Phase 2).
//!
//! * `GET`/`SET`/`DEL`/`EXISTS` -> the [`Map`] primitive (tenant `redis`, map
//!   `kv`), with `SET` TTL honoured via the map's per-key expiry.
//! * `PUBLISH` -> the routing [`TopicRouter`] (Redis pub/sub); subscribers
//!   receive pushed `message` frames over their connection channel.
//! * `XADD`/`XRANGE` -> the [`Log`] primitive (Redis streams).
//!
//! The broker is transport-agnostic; [`crate::server`] wires it to a TCP
//! listener, reusing the same `mpsc`-per-connection delivery model as the MQTT
//! adapter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;

use synapse_core::SynapseCore;
use synapse_routing::topic::TopicRouter;

use crate::codec::Value;

const TENANT: &str = "redis";
const KV_MAP: &str = "kv";

/// Shared RESP broker state.
pub struct RespBroker {
    core: Arc<SynapseCore>,
    router: Arc<TopicRouter>,
    clients: Mutex<std::collections::HashMap<String, UnboundedSender<Value>>>,
    next_id: AtomicU64,
}

impl RespBroker {
    pub fn new(core: Arc<SynapseCore>) -> Self {
        Self {
            core,
            router: Arc::new(TopicRouter::new()),
            clients: Mutex::new(std::collections::HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    fn kv(&self) -> Result<Arc<synapse_core::Map>, Value> {
        if self.core.get_map(TENANT, KV_MAP).unwrap().is_none() {
            self.core.create_map(TENANT, KV_MAP).map_err(|e| Value::err(format!("ERR {e}")))?;
        }
        Ok(self.core.get_map(TENANT, KV_MAP).unwrap().unwrap())
    }

    /// Execute a non-pub/sub command, returning the RESP response value.
    pub fn exec(&self, cmd: &[Value]) -> Value {
        let name = cmd.first().and_then(|v| v.as_str()).unwrap_or("").to_uppercase();
        match name.as_str() {
            "PING" => {
                if cmd.len() > 1 {
                    cmd[1].clone()
                } else {
                    Value::SimpleString("PONG".to_string())
                }
            }
            "ECHO" => cmd.get(1).cloned().unwrap_or_else(Value::null),
            "COMMAND" => Value::Array(vec![Value::bulk(b"GET".to_vec()), Value::bulk(b"SET".to_vec()), Value::bulk(b"DEL".to_vec()), Value::bulk(b"EXISTS".to_vec()), Value::bulk(b"PUBLISH".to_vec()), Value::bulk(b"SUBSCRIBE".to_vec()), Value::bulk(b"UNSUBSCRIBE".to_vec()), Value::bulk(b"XADD".to_vec()), Value::bulk(b"XRANGE".to_vec()), Value::bulk(b"PING".to_vec()), Value::bulk(b"ECHO".to_vec()), Value::bulk(b"QUIT".to_vec())]),
            "GET" => {
                let key = match cmd.get(1).and_then(|v| v.as_str()) {
                    Some(k) => k.to_string(),
                    None => return Value::err("ERR wrong number of arguments for 'get'"),
                };
                match self.kv().and_then(|m| Ok::<_, Value>(m.get(&key))) {
                    Ok(Some(v)) => Value::bulk(v),
                    Ok(None) => Value::null(),
                    Err(e) => e,
                }
            }
            "SET" => {
                let (key, val) = match (cmd.get(1).and_then(|v| v.as_str()), cmd.get(2)) {
                    (Some(k), Some(v)) => (k.to_string(), v.clone()),
                    _ => return Value::err("ERR wrong number of arguments for 'set'"),
                };
                let mut ttl = None;
                let mut i = 3;
                while i < cmd.len() {
                    let opt = cmd.get(i).and_then(|v| v.as_str()).unwrap_or("").to_uppercase();
                    if opt == "EX" {
                        match cmd.get(i + 1).and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok()) {
                            Some(secs) => ttl = Some(Duration::from_secs(secs)),
                            None => return Value::err("ERR invalid EX value"),
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                let payload = match &val {
                    Value::BulkString(b) => b.clone(),
                    _ => return Value::err("ERR value must be a bulk string"),
                };
                match self.kv().and_then(|m| m.set(&key, &payload, ttl).map_err(|e| Value::err(format!("ERR {e}")))) {
                    Ok(()) => Value::ok(),
                    Err(e) => e,
                }
            }
            "DEL" => {
                let m = match self.kv() {
                    Ok(m) => m,
                    Err(e) => return e,
                };
                let mut count = 0i64;
                for c in &cmd[1..] {
                    if let Some(k) = c.as_str() {
                        if m.delete(k) {
                            count += 1;
                        }
                    }
                }
                Value::int(count)
            }
            "EXISTS" => {
                let m = match self.kv() {
                    Ok(m) => m,
                    Err(e) => return e,
                };
                let mut count = 0i64;
                for c in &cmd[1..] {
                    if let Some(k) = c.as_str() {
                        if m.get(k).is_some() {
                            count += 1;
                        }
                    }
                }
                Value::int(count)
            }
            "XADD" => {
                let (key, rest) = match (cmd.get(1).and_then(|v| v.as_str()), cmd.get(2)) {
                    (Some(k), Some(_id)) => (k.to_string(), &cmd[3..]),
                    _ => return Value::err("ERR wrong number of arguments for 'xadd'"),
                };
                if self.core.get_log(TENANT, &key).unwrap().is_none() {
                    if let Err(e) = self.core.create_log(TENANT, &key) {
                        return Value::err(format!("ERR {e}"));
                    }
                }
                // Serialize field/value pairs as `field\0value\n` lines.
                let mut payload = Vec::new();
                let mut i = 0;
                while i + 1 < rest.len() {
                    let f = rest[i].as_str().unwrap_or("").as_bytes();
                    let v = match &rest[i + 1] {
                        Value::BulkString(b) => b.clone(),
                        _ => Vec::new(),
                    };
                    payload.extend_from_slice(f);
                    payload.push(0);
                    payload.extend_from_slice(&v);
                    payload.push(b'\n');
                    i += 2;
                }
                match self.core.log_append(TENANT, &key, &payload) {
                    Ok(off) => Value::bulk(off.to_string().into_bytes()),
                    Err(e) => Value::err(format!("ERR {e}")),
                }
            }
            "XRANGE" => {
                let (key, start, end) = match (cmd.get(1).and_then(|v| v.as_str()), cmd.get(2).and_then(|v| v.as_str()), cmd.get(3).and_then(|v| v.as_str())) {
                    (Some(k), Some(s), Some(e)) => (k.to_string(), s.to_string(), e.to_string()),
                    _ => return Value::err("ERR wrong number of arguments for 'xrange'"),
                };
                let log = match self.core.get_log(TENANT, &key).unwrap() {
                    Some(l) => l,
                    None => return Value::Array(vec![]),
                };
                let from = if start == "-" {
                    0
                } else {
                    start.parse::<u64>().unwrap_or(0)
                };
                let to = if end == "+" {
                    log.len().saturating_sub(1)
                } else {
                    end.parse::<u64>().unwrap_or(u64::MAX)
                };
                let max = (to - from + 1) as usize;
                match log.read(from, max) {
                    Ok(recs) => {
                        let entries: Vec<Value> = recs
                            .into_iter()
                            .map(|r| {
                                Value::Array(vec![
                                    Value::bulk(r.offset.to_string().into_bytes()),
                                    Value::bulk(r.payload.clone()),
                                ])
                            })
                            .collect();
                        Value::Array(entries)
                    }
                    Err(e) => Value::err(format!("ERR {e}")),
                }
            }
            "" => Value::err("ERR empty command"),
            other => Value::err(format!("ERR unknown command '{other}'")),
        }
    }

    /// Register `channels` as subscriptions for `conn_id`, returning the
    /// per-channel confirmation frames.
    pub fn subscribe(&self, conn_id: &str, tx: UnboundedSender<Value>, channels: &[Value]) -> Vec<Value> {
        self.clients.lock().unwrap().insert(conn_id.to_string(), tx);
        let mut confirmations = Vec::new();
        for ch in channels {
            if let Some(channel) = ch.as_str() {
                self.router.subscribe(conn_id, channel);
                let count = self.router.route(channel).len() as i64;
                confirmations.push(Value::Array(vec![
                    Value::bulk(b"subscribe".to_vec()),
                    Value::bulk(channel.as_bytes().to_vec()),
                    Value::int(count),
                ]));
            }
        }
        confirmations
    }

    /// Remove `channels` for `conn_id`, returning confirmations. An empty
    /// `channels` list unsubscribes from everything.
    pub fn unsubscribe(&self, conn_id: &str, channels: &[Value]) -> Vec<Value> {
        let to_remove: Vec<String> = if channels.is_empty() {
            self.router
                .subscribers()
                .into_iter()
                .filter(|s| &**s == conn_id)
                .map(|s| (*s).to_string())
                .collect()
        } else {
            channels
                .iter()
                .filter_map(|c| c.as_str().map(|s| s.to_string()))
                .collect()
        };
        let mut confirmations = Vec::new();
        for channel in &to_remove {
            self.router.unsubscribe(conn_id);
            let count = self.router.route(channel).len() as i64;
            confirmations.push(Value::Array(vec![
                Value::bulk(b"unsubscribe".to_vec()),
                Value::bulk(channel.as_bytes().to_vec()),
                Value::int(count),
            ]));
        }
        if to_remove.is_empty() {
            confirmations.push(Value::Array(vec![
                Value::bulk(b"unsubscribe".to_vec()),
                Value::null(),
                Value::int(0),
            ]));
        }
        confirmations
    }

    /// Publish `payload` to `channel`, returning the number of receiving
    /// subscribers.
    pub fn publish(&self, channel: &str, payload: &[u8]) -> i64 {
        let subs = self.router.route(channel);
        let clients = self.clients.lock().unwrap();
        let mut seen = std::collections::HashSet::new();
        let mut delivered = 0i64;
        for sub in subs {
            if !seen.insert(sub.to_string()) {
                continue;
            }
            if let Some(tx) = clients.get(&*sub) {
                let frame = Value::Array(vec![
                    Value::bulk(b"message".to_vec()),
                    Value::bulk(channel.as_bytes().to_vec()),
                    Value::bulk(payload.to_vec()),
                ]);
                if tx.send(frame).is_ok() {
                    delivered += 1;
                }
            }
        }
        delivered
    }

    /// Allocate a fresh connection id for pub/sub tracking.
    pub fn next_conn_id(&self) -> String {
        format!("conn-{}", self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Drop a connection's pub/sub registrations.
    pub fn disconnect(&self, conn_id: &str) {
        self.clients.lock().unwrap().remove(conn_id);
        self.router.unsubscribe(conn_id);
    }
}
