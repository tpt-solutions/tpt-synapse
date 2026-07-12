//! `SynapseCore`: the unified engine tying storage, tenants, primitives, and
//! metrics together (spec.txt §3, TODO.md Phase 1). This is the in-process
//! surface the protocol adapters and the routing engine build on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::error::{EngineError, EngineResult};
use crate::log::Log;
use crate::map::Map;
use crate::metrics::Metrics;
use crate::queue::Queue;
use crate::storage::{MemoryObjectStore, ObjectStore, SegmentedLog, TieredSegmentedLog};
use crate::tenant::{Tenant, TenantId, TenantRegistry};

type Key = (TenantId, String);

/// The unified data engine: one WAL, tenant isolation, and the three
/// primitives addressed by `(tenant, name)`.
pub struct SynapseCore {
    tenants: TenantRegistry,
    wal: Arc<TieredSegmentedLog>,
    metrics: Arc<Metrics>,
    logs: Mutex<HashMap<Key, Arc<Log>>>,
    queues: Mutex<HashMap<Key, Arc<Queue>>>,
    maps: Mutex<HashMap<Key, Arc<Map>>>,
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
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn tenant(&self, name: &str) -> EngineResult<Arc<Tenant>> {
        Ok(self.tenants.get_or_create(TenantId::new(name)?))
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
