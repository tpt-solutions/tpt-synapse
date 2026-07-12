//! The `Log` primitive: immutable append-only record sequence (spec.txt §3.1).
//!
//! Backs Kafka partitions and MQTT QoS 1/2. Durable via the shared tiered
//! segmented log; retention is accounted against the owning tenant's byte quota.

use std::sync::Arc;

use crate::error::EngineResult;
use crate::storage::{Record, TieredSegmentedLog};
use crate::tenant::Tenant;

/// An append-only log owned by a tenant.
pub struct Log {
    name: String,
    tenant: Arc<Tenant>,
    storage: Arc<TieredSegmentedLog>,
}

impl Log {
    pub fn new(
        name: impl Into<String>,
        tenant: Arc<Tenant>,
        storage: Arc<TieredSegmentedLog>,
    ) -> EngineResult<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(crate::error::EngineError::invalid_argument("log name required"));
        }
        tenant.charge_log()?;
        Ok(Self {
            name,
            tenant,
            storage,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Append a payload, returning its global offset. Charges the tenant's
    /// retention quota.
    pub fn append(&self, payload: &[u8]) -> EngineResult<u64> {
        let offset = self.storage.append(payload)?;
        // Charge committed bytes; immutable log retains them.
        self.tenant.charge_log_bytes(payload.len() as u64)?;
        Ok(offset)
    }

    /// Read up to `max_records` starting at `from_offset` (inclusive).
    pub fn read(&self, from_offset: u64, max_records: usize) -> EngineResult<Vec<Record>> {
        self.storage.read(from_offset, max_records)
    }

    /// Number of records currently stored.
    pub fn len(&self) -> u64 {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        self.tenant.release_log();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryObjectStore;
    use crate::tenant::{TenantId, TenantRegistry};

    fn make_log() -> Log {
        let reg = TenantRegistry::new();
        let t = reg.get_or_create(TenantId::new("t").unwrap());
        let storage = Arc::new(TieredSegmentedLog::new(
            crate::storage::SegmentedLog::new(1 << 20),
            Arc::new(MemoryObjectStore::new()),
        ));
        Log::new("orders", t, storage).unwrap()
    }

    #[test]
    fn append_then_read() {
        let log = make_log();
        let o0 = log.append(b"one").unwrap();
        let o1 = log.append(b"two").unwrap();
        assert_eq!((o0, o1), (0, 1));
        let recs = log.read(0, 10).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].payload, b"one");
    }

    #[test]
    fn empty_name_rejected() {
        let reg = TenantRegistry::new();
        let t = reg.get_or_create(TenantId::new("t").unwrap());
        let storage = Arc::new(TieredSegmentedLog::new(
            crate::storage::SegmentedLog::new(1 << 20),
            Arc::new(MemoryObjectStore::new()),
        ));
        assert!(Log::new("", t, storage).is_err());
    }
}
