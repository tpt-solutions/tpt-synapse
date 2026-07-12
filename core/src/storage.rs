//! Shared physical storage layer under Log/Queue/Map (TODO.md Phase 1).
//!
//! All three primitives are backed by one segmented, append-only log. Local
//! "hot" segments live in memory; once a segment is sealed it can be offloaded
//! transparently to an S3-compatible object store while the same global offset
//! read API is preserved (`TieredSegmentedLog`). This is the cost/scale
//! differentiator called out in TODO.md ("Tiered storage for the Log primitive").
//!
//! The async I/O path is abstracted behind the [`Persistence`] trait so a
//! `tokio-uring`/`monoio` Linux backend (picked in Phase 0) can be slotted in
//! without changing the primitives. The default [`MemoryPersistence`] keeps the
//! engine cross-platform and testable today.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{EngineError, EngineResult};

/// A single stored record. `offset` is the global, monotonically increasing
/// sequence number assigned at append time and stable across tier moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub payload: Vec<u8>,
    pub timestamp: u64,
}

/// One contiguous run of records. Sealed segments are immutable and are the
/// unit of offload to object storage.
#[derive(Debug)]
struct Segment {
    id: u32,
    base_offset: u64,
    records: Vec<Record>,
    bytes: usize,
    sealed: bool,
}

impl Segment {
    fn new(id: u32, base_offset: u64) -> Self {
        Self {
            id,
            base_offset,
            records: Vec::new(),
            bytes: 0,
            sealed: false,
        }
    }

    fn append(&mut self, rec: Record) {
        self.bytes += rec.payload.len();
        self.records.push(rec);
    }
}

/// In-memory segmented append log. Thread-safe. Offers a stable global offset
/// namespace so readers (and the tiered wrapper) can address records uniformly.
#[derive(Debug)]
pub struct SegmentedLog {
    segments: Mutex<Vec<Segment>>,
    next_offset: AtomicU64,
    next_segment_id: AtomicU64,
    max_segment_bytes: usize,
    now: fn() -> u64,
}

impl SegmentedLog {
    pub fn new(max_segment_bytes: usize) -> Self {
        Self::with_clock(max_segment_bytes, default_now)
    }

    pub fn with_clock(max_segment_bytes: usize, now: fn() -> u64) -> Self {
        let mut segments = Vec::new();
        segments.push(Segment::new(0, 0));
        Self {
            segments: Mutex::new(segments),
            next_offset: AtomicU64::new(0),
            next_segment_id: AtomicU64::new(1),
            max_segment_bytes,
            now,
        }
    }

    /// Append a payload; returns the global offset it was assigned.
    pub fn append(&self, payload: &[u8]) -> EngineResult<u64> {
        if payload.is_empty() {
            return Err(EngineError::invalid_argument("empty payload"));
        }
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let rec = Record {
            offset,
            payload: payload.to_vec(),
            timestamp: (self.now)(),
        };
        let mut segments = self.segments.lock().unwrap();
        let last = segments.last_mut().unwrap();
        let would_exceed = last.bytes + payload.len() > self.max_segment_bytes && !last.records.is_empty();
        if would_exceed {
            last.sealed = true;
            let id = self.next_segment_id.fetch_add(1, Ordering::SeqCst) as u32;
            let base = offset;
            segments.push(Segment::new(id, base));
        }
        segments.last_mut().unwrap().append(rec);
        Ok(offset)
    }

    /// Read up to `max_records` starting at (and including) `from_offset`.
    pub fn read(&self, from_offset: u64, max_records: usize) -> EngineResult<Vec<Record>> {
        let segments = self.segments.lock().unwrap();
        let mut out = Vec::with_capacity(max_records.min(64));
        for seg in segments.iter() {
            if seg.records.is_empty() {
                continue;
            }
            let seg_end = seg.base_offset + seg.records.len() as u64;
            if from_offset >= seg_end {
                continue;
            }
            let start = (from_offset.saturating_sub(seg.base_offset)) as usize;
            for rec in seg.records.iter().skip(start) {
                if out.len() >= max_records {
                    return Ok(out);
                }
                out.push(rec.clone());
            }
        }
        Ok(out)
    }

    /// Total number of records stored (local only).
    pub fn len(&self) -> u64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Immutable view of sealed segments for offload (excludes the active one).
    pub fn sealed_segments(&self) -> Vec<(u32, u64, u64)> {
        let segments = self.segments.lock().unwrap();
        segments
            .iter()
            .filter(|s| s.sealed)
            .map(|s| (s.id, s.base_offset, s.records.len() as u64))
            .collect()
    }

    /// Base offset of the currently active (last) segment.
    pub fn active_base(&self) -> u64 {
        self.segments.lock().unwrap().last().unwrap().base_offset
    }

    /// `(base_offset, record_count)` for every segment, in append order.
    pub fn segment_bounds(&self) -> Vec<(u64, u64)> {
        self.segments
            .lock()
            .unwrap()
            .iter()
            .map(|s| (s.base_offset, s.records.len() as u64))
            .collect()
    }

    /// All records of a specific segment by base offset (clone).
    pub fn read_segment_records(&self, base: u64) -> Option<Vec<Record>> {
        let segments = self.segments.lock().unwrap();
        segments
            .iter()
            .find(|s| s.base_offset == base)
            .map(|s| s.records.clone())
    }

    fn drop_segment(&self, id: u32) {
        let mut segments = self.segments.lock().unwrap();
        if let Some(pos) = segments.iter().position(|s| s.id == id && s.sealed) {
            segments.remove(pos);
        }
    }

    /// Read a specific sealed segment's raw serialized form (used by offload).
    pub fn segment_payloads(&self, id: u32) -> EngineResult<Vec<Vec<u8>>> {
        let segments = self.segments.lock().unwrap();
        segments
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.records.iter().map(|r| r.payload.clone()).collect())
            .ok_or_else(|| EngineError::not_found(format!("segment {}", id)))
    }
}

fn default_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Object-storage backend for tiered offload (S3-compatible API shape).
pub trait ObjectStore: Send + Sync {
    /// Store `data` under `key`. Idempotent.
    fn put(&self, key: &str, data: &[u8]) -> EngineResult<()>;
    /// Fetch `key`, or [`ErrorKind::NotFound`] if absent.
    fn get(&self, key: &str) -> EngineResult<Vec<u8>>;
    /// Whether `key` is currently stored.
    fn exists(&self, key: &str) -> bool;
}

/// In-memory [`ObjectStore`] used for tests and single-node dev. The same trait
/// is implemented by the S3 adapter (see [`S3ObjectStore`]) so offload is a
/// drop-in.
#[derive(Debug, Default)]
pub struct MemoryObjectStore {
    inner: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ObjectStore for MemoryObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> EngineResult<()> {
        self.inner
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> EngineResult<Vec<u8>> {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| EngineError::not_found(format!("object {}", key)))
    }

    fn exists(&self, key: &str) -> bool {
        self.inner.lock().unwrap().contains_key(key)
    }
}

/// S3-compatible object store placeholder.
///
/// The real implementation lives behind the `s3` feature and uses
/// `aws-sdk-s3`; until then every operation returns a clear error so callers
/// fail loudly rather than silently losing segments. The trait contract is
/// identical to [`MemoryObjectStore`], so enabling the feature is a no-op at
/// the call site.
#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    pub endpoint: String,
    pub bucket: String,
}

impl S3ObjectStore {
    pub fn new(endpoint: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
        }
    }
}

impl ObjectStore for S3ObjectStore {
    fn put(&self, _key: &str, _data: &[u8]) -> EngineResult<()> {
        Err(EngineError::internal(
            "S3ObjectStore requires the `s3` feature (aws-sdk-s3) to be enabled",
        ))
    }

    fn get(&self, _key: &str) -> EngineResult<Vec<u8>> {
        Err(EngineError::internal(
            "S3ObjectStore requires the `s3` feature (aws-sdk-s3) to be enabled",
        ))
    }

    fn exists(&self, _key: &str) -> bool {
        false
    }
}

fn segment_key(base_offset: u64) -> String {
    format!("segments/{}.log", base_offset)
}

/// Segmented log with transparent offload of sealed segments to object storage.
/// Reads consult local memory first, then fall back to the object store, so the
/// global-offset read API is identical whether a segment is hot or offloaded.
pub struct TieredSegmentedLog {
    local: SegmentedLog,
    object: Arc<dyn ObjectStore>,
    /// Set of segment base-offsets currently living only in the object store.
    offloaded: Mutex<std::collections::HashSet<u64>>,
}

impl TieredSegmentedLog {
    pub fn new(local: SegmentedLog, object: Arc<dyn ObjectStore>) -> Self {
        Self {
            local,
            object,
            offloaded: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Append a record; returns its global offset.
    pub fn append(&self, payload: &[u8]) -> EngineResult<u64> {
        self.local.append(payload)
    }

    /// Offload every sealed local segment that isn't already offloaded,
    /// freeing resident memory. Returns the number of segments moved.
    pub fn offload_sealed(&self) -> EngineResult<usize> {
        let sealed = self.local.sealed_segments();
        let mut moved = 0;
        for (id, base, _len) in sealed {
            let key = segment_key(base);
            if self.object.exists(&key) {
                self.local.drop_segment(id);
                self.offloaded.lock().unwrap().insert(base);
                moved += 1;
                continue;
            }
            let payloads = self.local.segment_payloads(id)?;
            let mut buf = Vec::new();
            for p in &payloads {
                // length-prefixed framing so a fetched segment can be re-read.
                buf.extend_from_slice(&(p.len() as u32).to_le_bytes());
                buf.extend_from_slice(p);
            }
            self.object.put(&key, &buf)?;
            self.local.drop_segment(id);
            self.offloaded.lock().unwrap().insert(base);
            moved += 1;
        }
        Ok(moved)
    }

    /// Read records by global offset. Falls back to the object store when the
    /// segment has been offloaded, transparently to the caller. Segments are
    /// visited in global-offset order so results are always correctly sorted.
    pub fn read(&self, from_offset: u64, max_records: usize) -> EngineResult<Vec<Record>> {
        let offloaded = self.offloaded.lock().unwrap().iter().copied().collect::<Vec<_>>();
        let mut bases: Vec<u64> = offloaded.clone();
        for (base, _) in self.local.segment_bounds() {
            bases.push(base);
        }
        bases.sort_unstable();
        bases.dedup();

        let mut out = Vec::with_capacity(max_records.min(64));
        for base in bases {
            if out.len() >= max_records {
                break;
            }
            let offloaded_here = offloaded.contains(&base);
            let mut recs: Vec<Record> = if offloaded_here {
                let buf = self.object.get(&segment_key(base))?;
                decode_segment(&buf)
            } else {
                match self.local.read_segment_records(base) {
                    Some(r) => r,
                    None => continue,
                }
            };
            for (i, r) in recs.iter_mut().enumerate() {
                r.offset = base + i as u64;
            }
            let seg_end = base + recs.len() as u64;
            if seg_end <= from_offset {
                continue;
            }
            let start = (from_offset.saturating_sub(base)) as usize;
            for r in recs.into_iter().skip(start) {
                if out.len() >= max_records {
                    break;
                }
                out.push(r);
            }
        }
        Ok(out)
    }

    pub fn len(&self) -> u64 {
        self.local.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn decode_segment(buf: &[u8]) -> Vec<Record> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor + 4 <= buf.len() {
        let len = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + len > buf.len() {
            break;
        }
        let payload = buf[cursor..cursor + len].to_vec();
        cursor += len;
        out.push(Record {
            offset: out.len() as u64,
            payload,
            timestamp: 0,
        });
    }
    out
}

impl std::fmt::Debug for TieredSegmentedLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredSegmentedLog")
            .field("local_len", &self.local.len())
            .field("offloaded", &self.offloaded.lock().unwrap().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read_by_offset() {
        let log = SegmentedLog::new(1024);
        let o0 = log.append(b"a").unwrap();
        let o1 = log.append(b"b").unwrap();
        let o2 = log.append(b"c").unwrap();
        assert_eq!((o0, o1, o2), (0, 1, 2));
        let recs = log.read(0, 10).unwrap();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[2].payload, b"c");
        let recs = log.read(1, 10).unwrap();
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn segment_rolls_when_full() {
        let log = SegmentedLog::with_clock(10, || 0);
        for _ in 0..5 {
            log.append(b"payload!!").unwrap(); // 9 bytes each -> rolls after 1
        }
        let sealed = log.sealed_segments();
        assert!(sealed.len() >= 4, "expected several sealed segments, got {}", sealed.len());
    }

    #[test]
    fn tiered_offload_and_read() {
        let local = SegmentedLog::with_clock(10, || 0);
        for _ in 0..5 {
            local.append(b"payload!!").unwrap();
        }
        let tiered = TieredSegmentedLog::new(local, Arc::new(MemoryObjectStore::new()));
        let moved = tiered.offload_sealed().unwrap();
        assert!(moved >= 4);
        // Reading across offloaded segments still returns everything.
        let recs = tiered.read(0, 100).unwrap();
        assert_eq!(recs.len(), 5);
        assert_eq!(recs[4].payload, b"payload!!");
    }
}
