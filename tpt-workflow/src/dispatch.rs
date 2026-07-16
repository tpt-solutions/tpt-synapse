//! The matching-service dispatch layer over the `Queue` primitive.
//!
//! This is the Temporal "matching service" equivalent: a thin, workflow-aware
//! facade on top of [`synapse_core::Queue`] that adds the semantics a task queue
//! needs beyond plain FIFO + ack/nack:
//!
//! * **Per-task-queue routing** — a `(tenant, task_queue, task_type)` tuple maps
//!   to one isolated FIFO [`Queue`], so activity tasks and workflow tasks (and
//!   different tenants) never bleed into one another.
//! * **Visibility timeout + redelivery** — a dispatched task is invisible to
//!   other pollers until it is acked; if its visibility window elapses without
//!   an ack (worker crash / stall), a background sweeper re-enqueues it.
//! * **Idempotent ack** — every delivery carries an opaque task token; a second
//!   response for an already-responded token is reported as a duplicate
//!   (success) rather than an error, so at-least-once redelivery never double-
//!   applies a completion.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use synapse_core::EngineError;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::{WorkflowError, WorkflowResult};
use crate::metrics::Metrics;

/// How long a responded task token stays remembered for idempotent re-acks.
const COMPLETED_TTL: Duration = Duration::from_secs(600);
/// Default visibility timeout when a request does not specify one.
const DEFAULT_VISIBILITY: Duration = Duration::from_secs(30);
/// Default long-poll ceiling when a poll request omits a timeout.
const DEFAULT_LONG_POLL: Duration = Duration::from_secs(10);
/// How often the redelivery sweeper scans in-flight tasks.
const SWEEP_INTERVAL: Duration = Duration::from_millis(250);

/// Outcome of an idempotent ack/fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckResult {
    /// Token was in-flight and is now acknowledged.
    Accepted,
    /// Token was already responded to before — safe duplicate, treat as success.
    Duplicate,
    /// Token is unknown (never dispatched, or already redelivered under a new
    /// token after a visibility timeout).
    Unknown,
}

/// One task pulled off a queue, before the manager wraps it into a gRPC `Task`.
pub(crate) struct Dispatch {
    pub token: Vec<u8>,
    pub payload: Vec<u8>,
    pub attempt: u64,
}

/// A single routed FIFO queue: the `Queue` primitive plus the matching-service
/// bookkeeping (in-flight visibility deadlines, attempt counters, wakeups).
pub(crate) struct TaskQueue {
    inner: Arc<synapse_core::Queue>,
    notify: Notify,
    inflight: Mutex<HashMap<u64, Inflight>>,
    attempts: Mutex<HashMap<u64, u64>>,
    visibility: Mutex<Duration>,
}

struct Inflight {
    token: Vec<u8>,
    expires_at: Instant,
}

impl TaskQueue {
    fn new(inner: Arc<synapse_core::Queue>, default_visibility: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            notify: Notify::new(),
            inflight: Mutex::new(HashMap::new()),
            attempts: Mutex::new(HashMap::new()),
            visibility: Mutex::new(default_visibility),
        })
    }

    fn set_visibility(&self, d: Duration) {
        *self.visibility.lock().unwrap() = d;
    }

    fn enqueue(&self, payload: &[u8]) -> Result<u64, EngineError> {
        self.inner.enqueue(payload)
    }

    /// Dispatch the next ready task, marking it in-flight with a fresh task
    /// token and a visibility deadline. Returns `None` if the queue is empty.
    fn dispatch(&self) -> Result<Option<Dispatch>, EngineError> {
        let (seq, payload) = match self.inner.dequeue() {
            Some(v) => v,
            None => return Ok(None),
        };
        let attempt = {
            let mut a = self.attempts.lock().unwrap();
            let e = a.entry(seq).or_insert(0);
            *e += 1;
            *e
        };
        let token = Uuid::new_v4().into_bytes().to_vec();
        let vis = *self.visibility.lock().unwrap();
        self.inflight.lock().unwrap().insert(
            seq,
            Inflight {
                token: token.clone(),
                expires_at: Instant::now() + vis,
            },
        );
        Ok(Some(Dispatch {
            token,
            payload,
            attempt,
        }))
    }

    fn seq_for_token(&self, token: &[u8]) -> Option<u64> {
        let inflight = self.inflight.lock().unwrap();
        inflight
            .iter()
            .find(|(_, v)| v.token == token)
            .map(|(seq, _)| *seq)
    }

    /// Acknowledge a previously dispatched task by token.
    fn ack(&self, token: &[u8]) -> bool {
        match self.seq_for_token(token) {
            Some(s) => {
                self.inner.ack(s);
                self.inflight.lock().unwrap().remove(&s);
                self.attempts.lock().unwrap().remove(&s);
                true
            }
            None => false,
        }
    }

    /// Fail a previously dispatched task: re-enqueue it for redelivery
    /// (preserving its attempt counter) and drop it from in-flight.
    fn nack(&self, token: &[u8]) -> bool {
        match self.seq_for_token(token) {
            Some(s) => {
                let ok = self.inner.nack(s);
                self.inflight.lock().unwrap().remove(&s);
                // Keep `attempts[s]` so the next dispatch reports an increment.
                ok
            }
            None => false,
        }
    }

    /// Re-enqueue any in-flight task whose visibility deadline elapsed.
    /// Returns the number of tasks redelivered.
    fn sweep(&self) -> usize {
        let now = Instant::now();
        let mut inflight = self.inflight.lock().unwrap();
        let expired: Vec<u64> = inflight
            .iter()
            .filter(|(_, v)| v.expires_at <= now)
            .map(|(seq, _)| *seq)
            .collect();
        let mut redelivered = 0;
        for seq in expired {
            inflight.remove(&seq);
            if self.inner.nack(seq) {
                redelivered += 1;
            }
        }
        redelivered
    }

    fn notify_waiters(&self) {
        self.notify.notify_one();
    }

    fn notified(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.notify.notified()
    }

    #[allow(dead_code)]
    fn depth(&self) -> usize {
        self.inner.depth()
    }
}

/// A routed queue plus the labels needed to build a delivered `Task`.
#[derive(Clone)]
struct RoutedQueue {
    tq: Arc<TaskQueue>,
    task_queue: String,
    task_type: String,
}

/// Owns every routed task queue and the cross-queue bookkeeping (task-token →
/// queue index, idempotency set) plus the redelivery sweeper.
///
/// Construct via [`TaskQueueManager::new`], which returns an `Arc<Self>` and
/// starts the background sweeper. The service holds an `Arc<TaskQueueManager>`.
pub struct TaskQueueManager {
    core: Arc<synapse_core::SynapseCore>,
    queues: Mutex<HashMap<String, RoutedQueue>>,
    /// task token → owning queue key, for O(1) idempotent ack routing.
    token_index: Mutex<HashMap<Vec<u8>, String>>,
    /// responded task tokens retained for idempotent re-ack detection (TTL).
    completed: Mutex<HashMap<Vec<u8>, Instant>>,
    metrics: Arc<Metrics>,
    default_visibility: Duration,
    shutdown: watch::Sender<bool>,
    sweeper: Mutex<Option<JoinHandle<()>>>,
}

impl TaskQueueManager {
    /// Create the manager and start the redelivery sweeper.
    pub fn new(core: Arc<synapse_core::SynapseCore>) -> Arc<Self> {
        let (shutdown, rx) = watch::channel(false);
        let metrics = Arc::new(Metrics::new());
        let mgr = Arc::new(Self {
            core,
            queues: Mutex::new(HashMap::new()),
            token_index: Mutex::new(HashMap::new()),
            completed: Mutex::new(HashMap::new()),
            metrics,
            default_visibility: DEFAULT_VISIBILITY,
            shutdown,
            sweeper: Mutex::new(None),
        });

        let weak = Arc::downgrade(&mgr);
        let handle = tokio::spawn(async move {
            loop {
                if *rx.borrow() {
                    break;
                }
                tokio::time::sleep(SWEEP_INTERVAL).await;
                if *rx.borrow() {
                    break;
                }
                if let Some(m) = weak.upgrade() {
                    let mut redelivered = 0usize;
                    {
                        let qs = m.queues.lock().unwrap();
                        for rq in qs.values() {
                            redelivered += rq.tq.sweep();
                        }
                    }
                    for _ in 0..redelivered {
                        m.metrics.timed_out();
                    }
                } else {
                    break;
                }
            }
        });
        *mgr.sweeper.lock().unwrap() = Some(handle);
        mgr
    }

    /// Stop the sweeper and release resources. Safe to call multiple times.
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        if let Some(h) = self.sweeper.lock().unwrap().take() {
            h.abort();
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    fn queue_key(tenant: &str, task_queue: &str, task_type: Option<&str>) -> String {
        format!("{}|{}|{}", tenant, task_queue, task_type.unwrap_or(""))
    }

    fn get_or_create(
        &self,
        tenant: &str,
        task_queue: &str,
        task_type: Option<&str>,
    ) -> WorkflowResult<Arc<TaskQueue>> {
        let key = Self::queue_key(tenant, task_queue, task_type);
        if let Some(rq) = self.queues.lock().unwrap().get(&key) {
            return Ok(rq.tq.clone());
        }
        let inner = match self.core.get_queue(tenant, &key)? {
            Some(q) => q,
            None => self.core.create_queue(tenant, &key)?,
        };
        let tq = TaskQueue::new(inner, self.default_visibility);
        self.queues.lock().unwrap().insert(
            key,
            RoutedQueue {
                tq: tq.clone(),
                task_queue: task_queue.to_string(),
                task_type: task_type.unwrap_or("").to_string(),
            },
        );
        Ok(tq)
    }

    /// Set the visibility-timeout default for a routed queue (idempotent create
    /// if the queue does not yet exist). Used by `StreamTasks` to honor a
    /// per-request visibility override.
    pub fn set_visibility(
        &self,
        tenant: &str,
        task_queue: &str,
        task_type: Option<&str>,
        vis: Duration,
    ) -> WorkflowResult<()> {
        let tq = self.get_or_create(tenant, task_queue, task_type)?;
        tq.set_visibility(vis);
        Ok(())
    }

    /// Scheduler side: enqueue a task onto a (tenant, task_queue, task_type)
    /// routed queue. Returns the underlying `Queue` sequence number.
    pub fn add_task(
        &self,
        tenant: &str,
        task_queue: &str,
        task_type: Option<&str>,
        payload: &[u8],
        visibility: Option<Duration>,
    ) -> WorkflowResult<u64> {
        if task_queue.is_empty() {
            return Err(WorkflowError::InvalidArgument("task_queue required".into()));
        }
        let tq = self.get_or_create(tenant, task_queue, task_type)?;
        if let Some(v) = visibility {
            tq.set_visibility(v);
        }
        let seq = tq.enqueue(payload)?;
        self.metrics.enqueued(tenant);
        tq.notify_waiters();
        Ok(seq)
    }

    /// Worker side: dispatch the next ready task from the routed queue,
    /// registering its token for idempotent ack routing. Returns `None` if the
    /// queue is empty (caller decides whether to wait).
    fn take(&self, key: &str, tenant: &str) -> WorkflowResult<Option<crate::proto::synapse::workflow::v1::Task>> {
        let rq = match self.queues.lock().unwrap().get(key).cloned() {
            Some(rq) => rq,
            None => return Ok(None),
        };
        match rq.tq.dispatch()? {
            Some(d) => {
                self.token_index
                    .lock()
                    .unwrap()
                    .insert(d.token.clone(), key.to_string());
                self.metrics.dispatched(tenant);
                Ok(Some(crate::proto::synapse::workflow::v1::Task {
                    task_token: d.token,
                    task_queue: rq.task_queue,
                    task_type: rq.task_type,
                    payload: d.payload,
                    attempt: d.attempt as i64,
                }))
            }
            None => Ok(None),
        }
    }

    /// Worker side: long-poll pull a single task. Blocks until a task is
    /// available or `long_poll_timeout` elapses (whichever comes first).
    pub async fn poll(
        &self,
        tenant: &str,
        task_queue: &str,
        task_type: Option<&str>,
        long_poll_timeout: Duration,
    ) -> WorkflowResult<Option<crate::proto::synapse::workflow::v1::Task>> {
        if task_queue.is_empty() {
            return Err(WorkflowError::InvalidArgument("task_queue required".into()));
        }
        let key = Self::queue_key(tenant, task_queue, task_type);
        let tq = self.get_or_create(tenant, task_queue, task_type)?;
        let timeout = if long_poll_timeout > Duration::ZERO {
            long_poll_timeout
        } else {
            DEFAULT_LONG_POLL
        };
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if let Some(t) = self.take(&key, tenant)? {
                return Ok(Some(t));
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                // Final re-check closes the notify lost-wakeup window.
                if let Some(t) = self.take(&key, tenant)? {
                    return Ok(Some(t));
                }
                self.metrics.long_poll_timeout();
                return Ok(None);
            }
            let remaining = deadline - now;
            let n = tq.notified();
            tokio::select! {
                _ = n => {}
                _ = tokio::time::sleep(remaining) => {
                    if let Some(t) = self.take(&key, tenant)? {
                        return Ok(Some(t));
                    }
                    self.metrics.long_poll_timeout();
                    return Ok(None);
                }
            }
        }
    }

    /// Worker side: idempotent completion/failure by task token.
    pub fn respond(&self, token: &[u8], failed: bool) -> WorkflowResult<AckResult> {
        self.prune_completed();
        if self.completed.lock().unwrap().contains_key(token) {
            return Ok(AckResult::Duplicate);
        }
        let key = match self.token_index.lock().unwrap().get(token).cloned() {
            Some(k) => k,
            None => return Ok(AckResult::Unknown),
        };
        let rq = match self.queues.lock().unwrap().get(&key).cloned() {
            Some(rq) => rq,
            None => return Ok(AckResult::Unknown),
        };
        let ok = if failed {
            rq.tq.nack(token)
        } else {
            rq.tq.ack(token)
        };
        if ok {
            self.completed
                .lock()
                .unwrap()
                .insert(token.to_vec(), Instant::now());
            self.token_index.lock().unwrap().remove(token);
            if failed {
                self.metrics.failed();
            } else {
                self.metrics.acked();
            }
            Ok(AckResult::Accepted)
        } else {
            Ok(AckResult::Unknown)
        }
    }

    fn prune_completed(&self) {
        let mut c = self.completed.lock().unwrap();
        let now = Instant::now();
        c.retain(|_, t| now.duration_since(*t) < COMPLETED_TTL);
    }
}

impl Drop for TaskQueueManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn manager() -> Arc<TaskQueueManager> {
        TaskQueueManager::new(Arc::new(synapse_core::SynapseCore::new()))
    }

    #[tokio::test]
    async fn enqueue_dispatch_ack() {
        let mgr = manager();
        mgr.add_task("t", "q", Some("activity"), b"do-it", None)
            .unwrap();
        let t = mgr.poll("t", "q", Some("activity"), Duration::from_millis(500)).await.unwrap();
        let t = t.expect("task dispatched");
        assert_eq!(t.payload, b"do-it");
        assert_eq!(t.attempt, 1);
        assert_eq!(t.task_type, "activity");

        let res = mgr.respond(&t.task_token, false).unwrap();
        assert_eq!(res, AckResult::Accepted);
    }

    #[tokio::test]
    async fn idempotent_duplicate_ack() {
        let mgr = manager();
        mgr.add_task("t", "q", None, b"x", None).unwrap();
        let t = mgr
            .poll("t", "q", None, Duration::from_millis(500))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(mgr.respond(&t.task_token, false).unwrap(), AckResult::Accepted);
        // A second completion for the same token is an idempotent success.
        assert_eq!(mgr.respond(&t.task_token, false).unwrap(), AckResult::Duplicate);
    }

    #[tokio::test]
    async fn unknown_token_is_rejected() {
        let mgr = manager();
        assert_eq!(
            mgr.respond(b"not-a-real-token", false).unwrap(),
            AckResult::Unknown
        );
    }

    #[tokio::test]
    async fn per_task_queue_isolation() {
        let mgr = manager();
        mgr.add_task("t", "q1", None, b"one", None).unwrap();
        mgr.add_task("t", "q2", None, b"two", None).unwrap();

        let t1 = mgr
            .poll("t", "q1", None, Duration::from_millis(200))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t1.payload, b"one");

        let t2 = mgr
            .poll("t", "q2", None, Duration::from_millis(200))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t2.payload, b"two");

        // q1 is now empty; polling it times out.
        assert!(mgr
            .poll("t", "q1", None, Duration::from_millis(100))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn visibility_timeout_redelivers() {
        let mgr = manager();
        // Tiny visibility window so the background sweeper redelivers quickly.
        mgr.add_task("t", "q", None, b"retry", Some(Duration::from_millis(1)))
            .unwrap();

        let first = mgr
            .poll("t", "q", None, Duration::from_millis(200))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.attempt, 1);

        // Don't ack; let the visibility window elapse and the sweeper re-enqueue.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let second = mgr
            .poll("t", "q", None, Duration::from_millis(200))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.payload, b"retry");
        assert_eq!(second.attempt, 2);

        // The original (stale) token is no longer valid after redelivery.
        assert_eq!(
            mgr.respond(&first.task_token, false).unwrap(),
            AckResult::Unknown
        );
        assert_eq!(
            mgr.respond(&second.task_token, false).unwrap(),
            AckResult::Accepted
        );
    }

    #[tokio::test]
    async fn failed_task_is_redelivered() {
        let mgr = manager();
        mgr.add_task("t", "q", None, b"boom", None).unwrap();
        let t = mgr
            .poll("t", "q", None, Duration::from_millis(200))
            .await
            .unwrap()
            .unwrap();
        // Worker reports failure -> re-enqueued for redelivery.
        assert_eq!(mgr.respond(&t.task_token, true).unwrap(), AckResult::Accepted);

        let again = mgr
            .poll("t", "q", None, Duration::from_millis(200))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.payload, b"boom");
        assert_eq!(again.attempt, 2);
    }
}
