//! `SynapseCore`: the unified engine tying storage, tenants, primitives, and
//! metrics together (spec.txt §3, TODO.md Phase 1). This is the in-process
//! surface the protocol adapters and the routing engine build on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use base64::Engine as _;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::error::{EngineError, EngineResult};
use crate::log::Log;
use crate::map::Map;
use crate::metrics::Metrics;
use crate::queue::Queue;
use crate::storage::{MemoryObjectStore, ObjectStore, SegmentedLog, TieredSegmentedLog};
use crate::tenant::{Tenant, TenantId, TenantRegistry};

type Key = (TenantId, String);

/// A kind of resource a [`CoreEvent`] pertains to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ResourceKind {
    Log,
    Queue,
    Map,
}

/// One mutation observed by the broker, broadcast to subscribers (the admin
/// API's live tail, future replication hooks) so they can react to traffic
/// without polling the engine.
#[derive(Debug, Clone, Serialize)]
pub struct CoreEvent {
    pub tenant: String,
    pub kind: ResourceKind,
    pub name: String,
    /// For `Map` writes, the affected key.
    pub key: Option<String>,
    /// A base64 preview of the written payload (capped) for live tail display.
    pub preview: String,
}

/// The unified data engine: one WAL, tenant isolation, and the three
/// primitives addressed by `(tenant, name)`.
pub struct SynapseCore {
    tenants: TenantRegistry,
    wal: Arc<TieredSegmentedLog>,
    metrics: Arc<Metrics>,
    logs: Mutex<HashMap<Key, Arc<Log>>>,
    queues: Mutex<HashMap<Key, Arc<Queue>>>,
    maps: Mutex<HashMap<Key, Arc<Map>>>,
    /// Broadcast channel of mutations; capacity-bounded ring so a slow
    /// consumer only ever sees recent events.
    events: broadcast::Sender<CoreEvent>,
}

impl SynapseCore {
    /// Create a core with an in-memory object tier (dev / single node).
    pub fn new() -> Self {
        Self::with_object_store(Arc::new(MemoryObjectStore::new()))
    }

    /// Create a core with a specific object store for tiered offload.
    pub fn with_object_store(object: Arc<dyn ObjectStore>) -> Self {
        let wal = Arc::new(TieredSegmentedLog::new(
            SegmentedLog::new(64 * 1024 * 1024),
            object,
        ));
        Self {
            tenants: TenantRegistry::new(),
            wal,
            metrics: Arc::new(Metrics::new()),
            logs: Mutex::new(HashMap::new()),
            queues: Mutex::new(HashMap::new()),
            maps: Mutex::new(HashMap::new()),
            events: broadcast::channel(1024).0,
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Subscribe to the mutation event stream (for live tail / replication).
    pub fn subscribe(&self) -> broadcast::Receiver<CoreEvent> {
        self.events.subscribe()
    }

    pub fn tenant(&self, name: &str) -> EngineResult<Arc<Tenant>> {
        Ok(self.tenants.get_or_create(TenantId::new(name)?))
    }

    fn emit(&self, kind: ResourceKind, tenant: &str, name: &str, key: Option<&str>, payload: &[u8]) {
        let preview = preview_b64(payload);
        let _ = self.events.send(CoreEvent {
            tenant: tenant.to_string(),
            kind,
            name: name.to_string(),
            key: key.map(|k| k.to_string()),
            preview,
        });
    }

    // --- Log -------------------------------------------------------------

    pub fn create_log(&self, tenant: &str, name: &str) -> EngineResult<Arc<Log>> {
        let t = self.tenant(tenant)?;
        let log = Arc::new(Log::new(name, t, self.wal.clone())?);
        self.logs
            .lock()
            .unwrap()
            .insert((TenantId::new(tenant)?, name.to_string()), log.clone());
        Ok(log)
    }

    pub fn get_log(&self, tenant: &str, name: &str) -> EngineResult<Option<Arc<Log>>> {
        let key = (TenantId::new(tenant)?, name.to_string());
        Ok(self.logs.lock().unwrap().get(&key).cloned())
    }

    /// Append through the engine, updating metrics. Adapter-facing helper.
    pub fn log_append(&self, tenant: &str, name: &str, payload: &[u8]) -> EngineResult<u64> {
        let start = Instant::now();
        let log = self
            .get_log(tenant, name)?
            .ok_or_else(|| EngineError::not_found(format!("log {}/{}", tenant, name)))?;
        let off = log.append(payload)?;
        self.metrics.log_append(tenant, name, payload.len() as u64);
        self.metrics.observe_latency("log", start.elapsed().as_secs_f64());
        self.emit(ResourceKind::Log, tenant, name, None, payload);
        Ok(off)
    }

    // --- Queue -----------------------------------------------------------

    pub fn create_queue(&self, tenant: &str, name: &str) -> EngineResult<Arc<Queue>> {
        let t = self.tenant(tenant)?;
        let q = Arc::new(Queue::new(name, t, self.wal.clone())?);
        self.queues
            .lock()
            .unwrap()
            .insert((TenantId::new(tenant)?, name.to_string()), q.clone());
        Ok(q)
    }

    pub fn get_queue(&self, tenant: &str, name: &str) -> EngineResult<Option<Arc<Queue>>> {
        let key = (TenantId::new(tenant)?, name.to_string());
        Ok(self.queues.lock().unwrap().get(&key).cloned())
    }

    pub fn queue_enqueue(&self, tenant: &str, name: &str, payload: &[u8]) -> EngineResult<u64> {
        let start = Instant::now();
        let q = self
            .get_queue(tenant, name)?
            .ok_or_else(|| EngineError::not_found(format!("queue {}/{}", tenant, name)))?;
        let seq = q.enqueue(payload)?;
        let depth = q.depth() as i64;
        self.metrics.queue_enqueue(tenant, name, depth);
        self.metrics.observe_latency("queue", start.elapsed().as_secs_f64());
        self.emit(ResourceKind::Queue, tenant, name, None, payload);
        Ok(seq)
    }

    // --- Map -------------------------------------------------------------

    pub fn create_map(&self, tenant: &str, name: &str) -> EngineResult<Arc<Map>> {
        let t = self.tenant(tenant)?;
        let m = Arc::new(Map::new(name, t, self.wal.clone())?);
        self.maps
            .lock()
            .unwrap()
            .insert((TenantId::new(tenant)?, name.to_string()), m.clone());
        Ok(m)
    }

    pub fn get_map(&self, tenant: &str, name: &str) -> EngineResult<Option<Arc<Map>>> {
        let key = (TenantId::new(tenant)?, name.to_string());
        Ok(self.maps.lock().unwrap().get(&key).cloned())
    }

    pub fn map_set(
        &self,
        tenant: &str,
        name: &str,
        key: &str,
        value: &[u8],
        ttl: Option<std::time::Duration>,
    ) -> EngineResult<()> {
        let start = Instant::now();
        let m = self
            .get_map(tenant, name)?
            .ok_or_else(|| EngineError::not_found(format!("map {}/{}", tenant, name)))?;
        m.set(key, value, ttl)?;
        self.metrics.map_set(tenant, name, m.len() as i64);
        self.metrics.observe_latency("map", start.elapsed().as_secs_f64());
        self.emit(ResourceKind::Map, tenant, name, Some(key), value);
        Ok(())
    }

    // --- Lifecycle -------------------------------------------------------

    /// Offload sealed WAL segments to the object tier, freeing resident memory.
    pub fn offload(&self) -> EngineResult<usize> {
        self.wal.offload_sealed()
    }

    pub fn routing_op(&self, tenant: &str, router: &str) {
        self.metrics.routing_op(tenant, router);
    }

    /// A read-only snapshot of every tenant and its logs/queues/maps, with a
    /// small sample of recent contents, for the admin API (TODO.md "Adoption &
    /// Tooling"). Iterates the engine's registries under their locks; cheap
    /// enough for an ops dashboard that polls at human cadence.
    pub fn snapshot(&self) -> crate::admin::CoreSnapshot {
        use crate::admin::{LogInfo, MapInfo, QueueInfo, ResourceSnapshot, TenantSnapshot};
        let mut tenants = Vec::new();
        {
            let logs = self.logs.lock().unwrap();
            let queues = self.queues.lock().unwrap();
            let maps = self.maps.lock().unwrap();
            let mut ids: Vec<TenantId> = logs.keys().map(|(t, _)| t.clone()).collect();
            for (t, _) in queues.keys() {
                if !ids.contains(t) {
                    ids.push(t.clone());
                }
            }
            for (t, _) in maps.keys() {
                if !ids.contains(t) {
                    ids.push(t.clone());
                }
            }
            ids.sort();
            ids.dedup();
            for id in ids {
                let tid = id.clone();
                let log_list: Vec<LogInfo> = logs
                    .iter()
                    .filter(|((t, _), _)| *t == tid)
                    .map(|((_, name), log)| {
                        let len = log.len();
                        let start = len.saturating_sub(10);
                        let sample = log
                            .read(start, 10)
                            .map(|recs| recs.iter().map(|r| preview_b64(&r.payload)).collect())
                            .unwrap_or_default();
                        LogInfo {
                            name: name.clone(),
                            len,
                            sample,
                        }
                    })
                    .collect();
                let queue_list: Vec<QueueInfo> = queues
                    .iter()
                    .filter(|((t, _), _)| *t == tid)
                    .map(|((_, name), q)| QueueInfo {
                        name: name.clone(),
                        depth: q.depth() as u64,
                        sample: q
                            .snapshot()
                            .into_iter()
                            .map(|(seq, p)| (seq, preview_b64(&p)))
                            .collect(),
                    })
                    .collect();
                let map_list: Vec<MapInfo> = maps
                    .iter()
                    .filter(|((t, _), _)| *t == tid)
                    .map(|((_, name), m)| MapInfo {
                        name: name.clone(),
                        size: m.len() as u64,
                        keys: m
                            .snapshot()
                            .into_iter()
                            .map(|(k, v)| (k, preview_b64(&v)))
                            .collect(),
                    })
                    .collect();
                tenants.push(TenantSnapshot {
                    tenant: id.to_string(),
                    resources: ResourceSnapshot {
                        logs: log_list,
                        queues: queue_list,
                        maps: map_list,
                    },
                });
            }
        }
        crate::admin::CoreSnapshot { tenants }
    }
}

/// A base64 preview of the first up-to-64 bytes of a payload, for the admin
/// live tail.
fn preview_b64(payload: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(&payload[..payload.len().min(64)])
}

impl Default for SynapseCore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_roundtrip_all_primitives() {
        let core = SynapseCore::new();
        let log = core.create_log("acme", "events").unwrap();
        let off = log.append(b"hello").unwrap();
        assert_eq!(core.log_append("acme", "events", b"world").unwrap(), off + 1);

        core.create_queue("acme", "jobs").unwrap();
        core.queue_enqueue("acme", "jobs", b"task").unwrap();

        core.create_map("acme", "cache").unwrap();
        core.map_set("acme", "cache", "k", b"v", None).unwrap();
        let m = core.get_map("acme", "cache").unwrap().unwrap();
        assert_eq!(m.get("k"), Some(b"v".to_vec()));
    }

    #[test]
    fn tenant_isolation_enforced() {
        let core = SynapseCore::new();
        core.create_log("acme", "events").unwrap();
        assert!(core.get_log("other", "events").unwrap().is_none());
    }
}
