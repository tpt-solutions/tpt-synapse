//! Multi-tenancy: namespace isolation and per-tenant quotas (TODO.md Phase 1).
//!
//! Tenants are the isolation boundary for the storage and routing primitives.
//! Every log/queue/map is created *within* a tenant, and quota accounting
//! (storage bytes, object counts, throughput) is enforced at the engine layer
//! so it is cheaper to enforce up front than to retrofit after the adapters
//! land (see TODO.md note on building multi-tenancy in during Phase 1).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::error::{EngineError, EngineResult};

/// A tenant identifier. Names are non-empty and restricted to a safe charset so
/// they can be used directly as filesystem namespace segments later.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(name: impl Into<String>) -> EngineResult<Self> {
        let s: String = name.into();
        if s.is_empty() {
            return Err(EngineError::invalid_argument("tenant id must not be empty"));
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            return Err(EngineError::invalid_argument(
                "tenant id may only contain [A-Za-z0-9._-]",
            ));
        }
        Ok(TenantId(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

use std::fmt;

/// Per-tenant resource limits. `u64::MAX` fields mean "unbounded".
#[derive(Debug, Clone, Copy)]
pub struct TenantQuota {
    /// Maximum bytes retained across all of the tenant's logs.
    pub max_log_bytes: u64,
    /// Maximum number of queues the tenant may create.
    pub max_queues: usize,
    /// Maximum number of maps the tenant may create.
    pub max_maps: usize,
    /// Maximum number of logs the tenant may create.
    pub max_logs: usize,
    /// Maximum sustained operations per second (admission-control hint).
    pub max_ops_per_sec: u64,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self {
            max_log_bytes: 1024 * 1024 * 1024,
            max_queues: 1024,
            max_maps: 1024,
            max_logs: 1024,
            max_ops_per_sec: 100_000,
        }
    }
}

/// A live tenant with mutable accounting counters guarded by the registry lock.
#[derive(Debug)]
pub struct Tenant {
    id: TenantId,
    quota: TenantQuota,
    used_log_bytes: Mutex<u64>,
    used_queues: Mutex<usize>,
    used_maps: Mutex<usize>,
    used_logs: Mutex<usize>,
}

impl Tenant {
    pub fn id(&self) -> &TenantId {
        &self.id
    }

    pub fn quota(&self) -> TenantQuota {
        self.quota
    }

    pub fn used_log_bytes(&self) -> u64 {
        *self.used_log_bytes.lock().unwrap()
    }

    pub fn used_queues(&self) -> usize {
        *self.used_queues.lock().unwrap()
    }

    pub fn used_maps(&self) -> usize {
        *self.used_maps.lock().unwrap()
    }

    pub fn used_logs(&self) -> usize {
        *self.used_logs.lock().unwrap()
    }

    /// Account for `bytes` of new log retention. Returns an error if it would
    /// exceed the quota.
    pub fn charge_log_bytes(&self, bytes: u64) -> EngineResult<()> {
        let mut used = self.used_log_bytes.lock().unwrap();
        let next = used.saturating_add(bytes);
        if next > self.quota.max_log_bytes {
            return Err(EngineError::quota_exceeded(format!(
                "tenant {} would exceed log byte quota ({} > {})",
                self.id, next, self.quota.max_log_bytes
            )));
        }
        *used = next;
        Ok(())
    }

    pub fn release_log_bytes(&self, bytes: u64) {
        let mut used = self.used_log_bytes.lock().unwrap();
        *used = used.saturating_sub(bytes);
    }

    pub fn charge_log(&self) -> EngineResult<()> {
        let mut used = self.used_logs.lock().unwrap();
        if *used >= self.quota.max_logs {
            return Err(EngineError::quota_exceeded(format!(
                "tenant {} log count quota exceeded",
                self.id
            )));
        }
        *used += 1;
        Ok(())
    }

    pub fn release_log(&self) {
        let mut used = self.used_logs.lock().unwrap();
        *used = used.saturating_sub(1);
    }

    pub fn charge_queue(&self) -> EngineResult<()> {
        let mut used = self.used_queues.lock().unwrap();
        if *used >= self.quota.max_queues {
            return Err(EngineError::quota_exceeded(format!(
                "tenant {} queue count quota exceeded",
                self.id
            )));
        }
        *used += 1;
        Ok(())
    }

    pub fn release_queue(&self) {
        let mut used = self.used_queues.lock().unwrap();
        *used = used.saturating_sub(1);
    }

    pub fn charge_map(&self) -> EngineResult<()> {
        let mut used = self.used_maps.lock().unwrap();
        if *used >= self.quota.max_maps {
            return Err(EngineError::quota_exceeded(format!(
                "tenant {} map count quota exceeded",
                self.id
            )));
        }
        *used += 1;
        Ok(())
    }

    pub fn release_map(&self) {
        let mut used = self.used_maps.lock().unwrap();
        *used = used.saturating_sub(1);
    }
}

/// Registry of known tenants and their quotas (spec.txt §3.1, TODO Phase 1).
#[derive(Debug, Default)]
pub struct TenantRegistry {
    tenants: Mutex<HashMap<TenantId, Arc<Tenant>>>,
}

impl TenantRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a tenant, returning [`ErrorKind::AlreadyExists`] if present.
    pub fn create(
        &self,
        id: TenantId,
        quota: TenantQuota,
    ) -> EngineResult<Arc<Tenant>> {
        let mut tenants = self.tenants.lock().unwrap();
        if tenants.contains_key(&id) {
            return Err(EngineError::already_exists(format!(
                "tenant {} already exists",
                id
            )));
        }
        let tenant = Arc::new(Tenant {
            id,
            quota,
            used_log_bytes: Mutex::new(0),
            used_queues: Mutex::new(0),
            used_maps: Mutex::new(0),
            used_logs: Mutex::new(0),
        });
        tenants.insert(tenant.id().clone(), tenant.clone());
        Ok(tenant)
    }

    /// Get a tenant by id, or create it with the default quota if missing.
    pub fn get_or_create(&self, id: TenantId) -> Arc<Tenant> {
        let mut tenants = self.tenants.lock().unwrap();
        tenants
            .entry(id.clone())
            .or_insert_with(|| {
                Arc::new(Tenant {
                    id,
                    quota: TenantQuota::default(),
                    used_log_bytes: Mutex::new(0),
                    used_queues: Mutex::new(0),
                    used_maps: Mutex::new(0),
                    used_logs: Mutex::new(0),
                })
            })
            .clone()
    }

    pub fn get(&self, id: &TenantId) -> EngineResult<Arc<Tenant>> {
        let tenants = self.tenants.lock().unwrap();
        tenants
            .get(id)
            .cloned()
            .ok_or_else(|| EngineError::not_found(format!("tenant {}", id)))
    }

    pub fn list(&self) -> Vec<TenantId> {
        let tenants = self.tenants.lock().unwrap();
        tenants.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_rejects_bad_chars() {
        assert!(TenantId::new("ok-name.1").is_ok());
        assert!(TenantId::new("").is_err());
        assert!(TenantId::new("bad name").is_err());
        assert!(TenantId::new("bad/name").is_err());
    }

    #[test]
    fn quota_create_and_charge() {
        let reg = TenantRegistry::new();
        let t = reg
            .create(
                TenantId::new("acme").unwrap(),
                TenantQuota {
                    max_logs: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(t.charge_log().is_ok());
        assert!(t.charge_log().is_err());
        t.release_log();
        assert!(t.charge_log().is_ok());
    }

    #[test]
    fn log_byte_quota_enforced() {
        let reg = TenantRegistry::new();
        let t = reg
            .create(
                TenantId::new("acme").unwrap(),
                TenantQuota {
                    max_log_bytes: 100,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(t.charge_log_bytes(60).is_ok());
        assert!(t.charge_log_bytes(60).is_err());
        t.release_log_bytes(60);
        assert!(t.charge_log_bytes(60).is_ok());
    }
}
