//! The `Map` primitive: concurrent in-memory KV store with TTL (spec.txt §3.1).
//! Backs Redis. Sets are durable via the shared tiered segmented log (WAL);
//! reads serve from the hot in-memory index.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{EngineError, EngineResult};
use crate::storage::TieredSegmentedLog;
use crate::tenant::Tenant;

#[derive(Debug, Clone)]
struct Entry {
    value: Vec<u8>,
    /// Absolute expiry in millis since epoch, or `None` for no expiry.
    expires_at: Option<u64>,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Concurrent key-value store with per-key TTL. Backs the RESP adapter.
pub struct Map {
    name: String,
    tenant: Arc<Tenant>,
    _wal: Arc<TieredSegmentedLog>,
    data: Mutex<HashMap<String, Entry>>,
}

impl Map {
    pub fn new(
        name: impl Into<String>,
        tenant: Arc<Tenant>,
        wal: Arc<TieredSegmentedLog>,
    ) -> EngineResult<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(EngineError::invalid_argument("map name required"));
        }
        tenant.charge_map()?;
        Ok(Self {
            name,
            tenant,
            _wal: wal,
            data: Mutex::new(HashMap::new()),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set `key`, optionally expiring after `ttl`. Durable via WAL.
    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> EngineResult<()> {
        if key.is_empty() {
            return Err(EngineError::invalid_argument("empty key"));
        }
        self._wal.append(value)?;
        let expires_at = ttl.map(|t| now_millis().saturating_add(t.as_millis() as u64));
        self.data.lock().unwrap().insert(
            key.to_string(),
            Entry {
                value: value.to_vec(),
                expires_at,
            },
        );
        Ok(())
    }

    /// Get `key`, returning `None` if absent or expired (expired keys are
    /// lazily evicted on access).
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut data = self.data.lock().unwrap();
        if let Some(entry) = data.get(key) {
            if let Some(exp) = entry.expires_at {
                if exp <= now_millis() {
                    data.remove(key);
                    return None;
                }
            }
            return Some(entry.value.clone());
        }
        None
    }

    pub fn delete(&self, key: &str) -> bool {
        self.data.lock().unwrap().remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.data.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for Map {
    fn drop(&mut self) {
        self.tenant.release_map();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryObjectStore;
    use crate::tenant::{TenantId, TenantRegistry};

    fn make_map() -> Map {
        let reg = TenantRegistry::new();
        let t = reg.get_or_create(TenantId::new("t").unwrap());
        let wal = Arc::new(TieredSegmentedLog::new(
            crate::storage::SegmentedLog::new(1 << 20),
            Arc::new(MemoryObjectStore::new()),
        ));
        Map::new("cache", t, wal).unwrap()
    }

    #[test]
    fn set_get_delete() {
        let m = make_map();
        m.set("k", b"v", None).unwrap();
        assert_eq!(m.get("k"), Some(b"v".to_vec()));
        assert!(m.delete("k"));
        assert_eq!(m.get("k"), None);
    }

    #[test]
    fn ttl_expiry() {
        let m = make_map();
        m.set("k", b"v", Some(Duration::from_millis(1))).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(m.get("k"), None);
    }
}
