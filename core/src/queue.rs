//! The `Queue` primitive: mutable FIFO with acknowledgment tracking
//! (spec.txt §3.1). Backs AMQP and task queues.
//!
//! Enqueues are durable via the shared tiered segmented log (WAL). In-memory
//! structures track the ready queue, in-flight (delivered, awaiting ack)
//! messages, and redelivery on negative acknowledgment.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{EngineError, EngineResult};
use crate::storage::TieredSegmentedLog;
use crate::tenant::Tenant;

#[derive(Debug, Clone)]
struct Entry {
    seq: u64,
    payload: Vec<u8>,
}

/// A FIFO work queue with ack/nack semantics. Durable through the shared WAL.
pub struct Queue {
    name: String,
    tenant: Arc<Tenant>,
    _wal: Arc<TieredSegmentedLog>,
    ready: Mutex<VecDeque<Entry>>,
    inflight: Mutex<HashMap<u64, Entry>>,
    seq: AtomicU64,
}

impl Queue {
    pub fn new(
        name: impl Into<String>,
        tenant: Arc<Tenant>,
        wal: Arc<TieredSegmentedLog>,
    ) -> EngineResult<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(EngineError::invalid_argument("queue name required"));
        }
        tenant.charge_queue()?;
        Ok(Self {
            name,
            tenant,
            _wal: wal,
            ready: Mutex::new(VecDeque::new()),
            inflight: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Enqueue a payload. Returns the assigned sequence number. Durable.
    /// Empty payloads are permitted (e.g. AMQP allows zero-length bodies).
    pub fn enqueue(&self, payload: &[u8]) -> EngineResult<u64> {
        // WAL write provides durability; the in-memory deque is the hot index.
        self._wal.append(payload)?;
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        self.ready.lock().unwrap().push_back(Entry {
            seq,
            payload: payload.to_vec(),
        });
        Ok(seq)
    }

    /// Dequeue the next ready message, marking it in-flight until ack/nack.
    pub fn dequeue(&self) -> Option<(u64, Vec<u8>)> {
        let mut ready = self.ready.lock().unwrap();
        let entry = ready.pop_front()?;
        self.inflight
            .lock()
            .unwrap()
            .insert(entry.seq, entry.clone());
        Some((entry.seq, entry.payload))
    }

    /// Acknowledge a previously dequeued message; removes it for good.
    pub fn ack(&self, seq: u64) -> bool {
        self.inflight.lock().unwrap().remove(&seq).is_some()
    }

    /// Negative-ack: return a message to the front of the ready queue for
    /// redelivery (e.g. a worker crashed).
    pub fn nack(&self, seq: u64) -> bool {
        let entry = self.inflight.lock().unwrap().remove(&seq);
        if let Some(entry) = entry {
            self.ready.lock().unwrap().push_front(entry);
            true
        } else {
            false
        }
    }

    /// Total outstanding messages: ready + in-flight.
    pub fn depth(&self) -> usize {
        self.ready.lock().unwrap().len() + self.inflight.lock().unwrap().len()
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        self.tenant.release_queue();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryObjectStore;
    use crate::tenant::{TenantId, TenantRegistry};

    fn make_queue() -> Queue {
        let reg = TenantRegistry::new();
        let t = reg.get_or_create(TenantId::new("t").unwrap());
        let wal = Arc::new(TieredSegmentedLog::new(
            crate::storage::SegmentedLog::new(1 << 20),
            Arc::new(MemoryObjectStore::new()),
        ));
        Queue::new("jobs", t, wal).unwrap()
    }

    #[test]
    fn fifo_enqueue_dequeue() {
        let q = make_queue();
        q.enqueue(b"a").unwrap();
        q.enqueue(b"b").unwrap();
        assert_eq!(q.depth(), 2);
        let (s0, p0) = q.dequeue().unwrap();
        let (s1, p1) = q.dequeue().unwrap();
        assert_eq!((p0, p1), (b"a".to_vec(), b"b".to_vec()));
        assert_eq!(q.depth(), 2); // still in-flight
        assert!(q.ack(s0));
        assert!(q.ack(s1));
        assert_eq!(q.depth(), 0);
    }

    #[test]
    fn nack_redelivers() {
        let q = make_queue();
        let s = q.enqueue(b"x").unwrap();
        let (d, p) = q.dequeue().unwrap();
        assert_eq!(p, b"x");
        assert_eq!(d, s);
        assert!(q.nack(d));
        let (d2, p2) = q.dequeue().unwrap();
        assert_eq!(p2, b"x");
        assert_eq!(d2, s);
        assert!(q.ack(d2));
        assert_eq!(q.depth(), 0);
    }
}
