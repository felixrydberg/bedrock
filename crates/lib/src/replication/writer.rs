//! Single-node ordered commits (#9 / B-09, #11 / B-11).
//! Each SlateDB batch atomically stores original events, resolved values, retry results
//! and progress. Only remote durability allows a response or subscription publication.
//! Future B-09b commits through Raft first and uses the same persistence operation.
//!
//! [`Writer::open`] is `bedrock-api`'s writer for pushed datasets: it recovers every namespace at startup and feeds the
//! relay log. [`Writer::owner`] is a customer-KV node's writer for the namespaces it owns (#58 / B-26): it opens each on
//! first use, keeps at most `max_open` open, publishes no relay log, serves reads from the same handle
//! ([`Writer::reader`]) and, after another node fenced it, waits [`FENCE_HOLD`] before taking a namespace back.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family},
};
use tokio::sync::oneshot;

use super::log::{Log, MemoryLog};
use super::materialized::{
    Committed, Materialized, Persisted, Reader, Retention, Retries, RetryRecord, Snapshot, View,
    validate_scope,
};
use super::{Event, Operation, stream};
use crate::admin::Admin;
use crate::storage::namespace::NamespaceFactory;
use crate::storage::{ErrorContext, StorageError, StorageResult};

pub const EPOCH: u64 = 1;
/// A recovery lease lapses after this long without transfer progress.
const RECOVERY_LEASE: std::time::Duration = std::time::Duration::from_secs(60);
/// Renewal never extends a recovery lease past this age.
const RECOVERY_MAX: std::time::Duration = std::time::Duration::from_secs(600);
/// Maximum writes waiting for the next namespace batch.
pub const MAX_QUEUED: usize = 1024;
/// How long an owner writer leaves a namespace alone after another writer fenced it. Membership views converge within
/// this, so two nodes that both think they own a namespace stop taking it from each other.
pub const FENCE_HOLD: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub dataset: String,
    pub namespace: String,
    pub key: Vec<u8>,
    pub operation: Operation,
    /// Until fixed expiry, a retry returns its original sequence without applying again.
    pub idempotency_key: Option<String>,
    /// Commit only as the revision after this one: the write was planned from it, and anything newer invalidates the plan.
    pub expected: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    Invalid(String),
    Busy,
    /// The namespace moved past [`Write::expected`] to `head`; nothing was written. A caller that planned from a
    /// lagging replica re-reads at `head` and plans again (#56 / B-25).
    Conflict {
        head: u64,
    },
    Fenced,
    /// Persistence may have completed; retry with the same idempotency key.
    Unavailable(String),
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid write: {reason}"),
            Self::Busy => f.write_str("too many writes waiting in this namespace"),
            Self::Conflict { head } => {
                write!(
                    f,
                    "the namespace moved past the expected revision to {head}"
                )
            }
            Self::Fenced => f.write_str("another writer owns this namespace"),
            Self::Unavailable(detail) => write!(f, "commit outcome unknown: {detail}"),
        }
    }
}
impl std::error::Error for CommitError {}

pub struct Persistence {
    pub factory: Arc<dyn NamespaceFactory>,
    pub retention: Retention,
    pub metrics: PersistenceMetrics,
}

#[derive(Clone, Default)]
pub struct PersistenceMetrics {
    runs: Family<PersistenceLabel, Counter>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PersistenceLabel {
    dataset: String,
    outcome: String,
}

impl PersistenceMetrics {
    pub fn new(admin: &Admin) -> Self {
        let metrics = Self::default();
        admin.register(
            "retention_runs",
            "Event-triggered namespace expiry cleanup runs by outcome",
            metrics.runs.clone(),
        );
        metrics
    }
    fn record(&self, dataset: &str, failed: bool) {
        self.runs
            .get_or_create(&PersistenceLabel {
                dataset: dataset.into(),
                outcome: if failed { "failed" } else { "cleaned" }.into(),
            })
            .inc();
    }
}

pub struct Writer {
    clock: fn() -> u64,
    log: Arc<MemoryLog>,
    namespaces: Mutex<BTreeMap<(String, String), Arc<Namespace>>>,
    persistence: Persistence,
    slots: tokio::sync::Semaphore,
    closed: AtomicBool,
    /// Set for an owner writer ([`Writer::owner`]).
    owner: Option<Owner>,
}

struct Owner {
    max_open: usize,
    /// Bounds concurrent cold opens, which read a manifest, write a fence and start engine tasks.
    // ponytail: one process-wide bound; per-cell fairness (`health::protection::Limiter`) if cross-cell surges show.
    opening: tokio::sync::Semaphore,
}

#[derive(Default)]
struct Namespace {
    queue: Mutex<Vec<Queued>>,
    state: tokio::sync::Mutex<State>,
    flow: Mutex<CleanupFlow>,
    /// Whether the namespace has an open handle, and when it was last used: what an owner writer evicts by.
    open: AtomicBool,
    used: Mutex<Option<tokio::time::Instant>>,
}

#[derive(Default)]
struct CleanupFlow {
    running: bool,
    requested: u64,
}

struct Queued {
    write: Write,
    done: oneshot::Sender<Result<u64, CommitError>>,
}

#[derive(Default)]
struct State {
    source: Option<Arc<Materialized>>,
    head: u64,
    committed_at: u64,
    retries: Retries,
    unresolved: Option<Vec<Committed>>,
    fenced: bool,
    /// When this handle was fenced; an owner writer may take the namespace back [`FENCE_HOLD`] later.
    fenced_at: Option<tokio::time::Instant>,
    recovery: Vec<std::sync::Weak<RecoveryLease>>,
}

/// Keeps the committed tail after a coherent view available while it is transferred.
pub struct RecoveryLease {
    dataset: String,
    namespace: String,
    boundary: u64,
    expires: Mutex<tokio::time::Instant>,
    deadline: tokio::time::Instant,
}

impl RecoveryLease {
    pub fn valid(&self) -> bool {
        crate::monotonic_now() < *self.expires.lock().unwrap()
    }

    /// Transfer progress extends a still-valid lease, never past its absolute deadline.
    pub fn renew(&self) {
        let now = crate::monotonic_now();
        let mut expires = self.expires.lock().unwrap();
        if now < *expires {
            *expires = (now + RECOVERY_LEASE).min(self.deadline);
        }
    }
}

fn unavailable(detail: &str) -> StorageError {
    StorageError::Unavailable(ErrorContext {
        engine: "writer",
        instance: "api".into(),
        file: None,
        partition: None,
        detail: detail.into(),
    })
}

impl Writer {
    pub fn log(&self) -> Arc<MemoryLog> {
        self.log.clone()
    }

    fn new(
        clock: fn() -> u64,
        config: Persistence,
        owner: Option<Owner>,
    ) -> StorageResult<Arc<Self>> {
        config.retention.validate()?;
        let log = Arc::new(MemoryLog::with_retention(
            clock,
            config.retention.event_retention_ms,
        ));
        Ok(Arc::new(Self {
            clock,
            log,
            namespaces: Mutex::default(),
            persistence: config,
            slots: tokio::sync::Semaphore::new(4),
            closed: AtomicBool::new(false),
            owner,
        }))
    }

    /// A customer-KV owner's writer (#58 / B-26): nothing is opened until used, at most `max_open` namespaces stay open,
    /// the least recently used closing first, and no relay log is kept. The caller routes each namespace to one owner;
    /// a second writer opening it fences this one, as SlateDB guarantees and `docs/spec/materialized.tla` models.
    pub fn owner(
        clock: fn() -> u64,
        config: Persistence,
        max_open: usize,
    ) -> StorageResult<Arc<Self>> {
        Self::new(
            clock,
            config,
            Some(Owner {
                max_open: max_open.max(1),
                opening: tokio::sync::Semaphore::new(
                    crate::health::protection::NAMESPACE_OPENS.total,
                ),
            }),
        )
    }

    pub async fn open(clock: fn() -> u64, config: Persistence) -> StorageResult<Arc<Self>> {
        let writer = Self::new(clock, config, None)?;
        let result = async {
            for (dataset, namespace) in writer.persistence.factory.namespaces().await? {
                validate_scope(&dataset, &namespace)?;
                let ns = writer.namespace(&dataset, &namespace);
                let mut state = ns.state.lock().await;
                writer.source(&mut state, &dataset, &namespace).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = writer.close().await;
            return Err(error);
        }
        Ok(writer)
    }

    async fn source(
        &self,
        state: &mut State,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<Materialized>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(unavailable("writer closed"));
        }
        if let Some(source) = &state.source {
            return Ok(source.clone());
        }
        let _permit = match &self.owner {
            Some(owner) => Some(
                owner
                    .opening
                    .acquire()
                    .await
                    .map_err(|_| unavailable("writer closed"))?,
            ),
            None => None,
        };
        let source = Arc::new(
            Materialized::open(self.persistence.factory.as_ref(), dataset, namespace).await?,
        );
        let restored = async {
            let progress = source.progress().await?;
            if progress.raft.is_some() {
                return Err(unavailable(
                    "Raft-backed namespace requires the Raft writer",
                ));
            }
            if self.owner.is_some() {
                // No relay log; a reopen may only find what this handle already made durable, or more that a writer
                // that took the namespace meanwhile added.
                if progress.sequence < state.head {
                    return Err(StorageError::Corruption(ErrorContext {
                        engine: "writer",
                        instance: format!("{dataset}/{namespace}"),
                        file: None,
                        partition: None,
                        detail: format!(
                            "reopened store is at {} behind durable head {}",
                            progress.sequence, state.head
                        ),
                    }));
                }
                // The store moved past this handle's head: a failed batch either landed or another owner took its
                // revisions meanwhile. Its callers were told the outcome is unknown and retry with their idempotency keys,
                // which find a batch that landed; replaying it now would collide with the other owner's revisions.
                if progress.sequence > state.head {
                    state.unresolved = None;
                }
                state.head = progress.sequence;
                state.committed_at = progress.committed_at;
                state.retries = progress.retries.into();
                return Ok(());
            }
            // A reopen after `discard` keeps the log, which holds what was durable. It gains what became durable since,
            // such as a batch whose flush landed but reported failure, and may lack nothing.
            let head = self.log.head(dataset, namespace).get();
            if progress.sequence < head {
                return Err(StorageError::Corruption(ErrorContext {
                    engine: "writer",
                    instance: format!("{dataset}/{namespace}"),
                    file: None,
                    partition: None,
                    detail: format!(
                        "reopened store is at {} behind durable head {head}",
                        progress.sequence
                    ),
                }));
            }
            let entries = source.events().await?;
            if head == 0 {
                self.log
                    .reset(dataset, namespace, progress.pruned_through)
                    .map_err(|detail| unavailable(&detail))?;
            }
            for entry in entries
                .into_iter()
                .filter(|entry| entry.event.sequence > head)
            {
                self.log
                    .append(entry.event)
                    .map_err(|detail| unavailable(&detail))?;
            }
            if self.log.head(dataset, namespace).get() != progress.sequence {
                return Err(unavailable("restored events do not reach the durable head"));
            }
            state.head = progress.sequence;
            state.committed_at = progress.committed_at;
            state.retries = progress.retries.into();
            Ok(())
        }
        .await;
        if let Err(error) = restored {
            let _ = source.close().await;
            return Err(error);
        }
        state.source = Some(source.clone());
        if self.owner.is_some() {
            let ns = self.namespace(dataset, namespace);
            ns.open.store(true, Ordering::SeqCst);
            *ns.used.lock().unwrap() = Some(crate::monotonic_now());
            self.evict(Some(&ns)).await;
        }
        Ok(source)
    }

    /// Closes the least recently used idle handles while more than `max_open` are open, never `keep`'s. A namespace
    /// in use, or with a batch whose flush failed, stays open until a later pass: the next open or finished batch.
    async fn evict(&self, keep: Option<&Arc<Namespace>>) {
        let Some(owner) = &self.owner else {
            return;
        };
        let mut open: Vec<(Option<tokio::time::Instant>, Arc<Namespace>)> = self
            .namespaces
            .lock()
            .unwrap()
            .values()
            .filter(|ns| ns.open.load(Ordering::SeqCst))
            .map(|ns| (*ns.used.lock().unwrap(), ns.clone()))
            .collect();
        let mut count = open.len();
        // ponytail: sorts every open namespace on each cold open; an ordered index if `max_open` grows large.
        open.sort_by_key(|(used, _)| *used);
        for (_, ns) in open {
            if count <= owner.max_open {
                return;
            }
            if keep.is_some_and(|keep| Arc::ptr_eq(&ns, keep))
                || !ns.queue.lock().unwrap().is_empty()
            {
                continue;
            }
            let Ok(mut state) = ns.state.try_lock() else {
                continue;
            };
            if state.unresolved.is_some() {
                continue;
            }
            Self::shut(&ns, &mut state).await;
            count -= 1;
        }
    }

    /// Closes the namespace's handle and forgets its state; the next use reopens it from the store.
    async fn shut(ns: &Namespace, state: &mut State) {
        if let Some(source) = state.source.take()
            && let Err(error) = source.close().await
        {
            tracing::debug!(%error, "closing a namespace handle failed");
        }
        *state = State::default();
        ns.open.store(false, Ordering::SeqCst);
    }

    /// Whether this handle is fenced. An owner writer takes the namespace back after [`FENCE_HOLD`]: a fresh open
    /// recovers the store and fences whoever had it.
    fn held(&self, state: &mut State) -> bool {
        if !state.fenced {
            return false;
        }
        let expired = state
            .fenced_at
            .is_some_and(|at| crate::monotonic_now().duration_since(at) >= FENCE_HOLD);
        if self.owner.is_some() && expired {
            state.fenced = false;
            state.fenced_at = None;
            state.source = None;
            return false;
        }
        true
    }

    /// SlateDB closes a database for good when one of its background tasks fails, which a single failed S3 request can
    /// cause. So a failed handle is dropped, and the next use reopens the namespace from the store, as a restart would,
    /// which fences the old handle. Closing it instead could flush the failed batch. A fenced one stays fenced: another
    /// writer owns the namespace.
    fn discard(state: &mut State, error: &StorageError) {
        if matches!(error, StorageError::Fenced(_)) {
            if !state.fenced {
                state.fenced_at = Some(crate::monotonic_now());
            }
            state.fenced = true;
        } else {
            state.source = None;
        }
    }

    /// A coherent view, discarding the handle if it fails.
    async fn read_view(
        &self,
        state: &mut State,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<View> {
        let view = self.source(state, dataset, namespace).await?.view().await;
        if let Err(error) = &view {
            Self::discard(state, error);
        }
        view
    }

    /// Owns a complete coherent view before releasing the namespace gate.
    pub async fn bootstrap(&self, dataset: &str, namespace: &str) -> StorageResult<View> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        self.settle(&mut state, dataset, namespace).await?;
        self.read_view(&mut state, dataset, namespace).await
    }

    /// Settles a failed flush before a read that needs a coherent view. Waiting for a later commit could wait forever: a
    /// membership publisher writes only once its own bootstrap succeeded.
    async fn settle(&self, state: &mut State, dataset: &str, namespace: &str) -> StorageResult<()> {
        if self.held(state) {
            return Err(unavailable("another writer owns this namespace"));
        }
        self.source(state, dataset, namespace).await?;
        if let Some(entries) = state.unresolved.take() {
            self.persist(state, entries)
                .await
                .map_err(|error| unavailable(&error.to_string()))?;
        }
        Ok(())
    }

    pub async fn begin_recovery(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<(Snapshot, Arc<RecoveryLease>)> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        self.settle(&mut state, dataset, namespace).await?;
        state
            .recovery
            .retain(|lease| lease.upgrade().is_some_and(|l| l.valid()));
        if state.recovery.len() >= 4 {
            return Err(unavailable("namespace recovery capacity exhausted"));
        }
        let source = self.source(&mut state, dataset, namespace).await?;
        // File protection outlasts bounded capture plus the absolute tail lease.
        let view = tokio::time::timeout(
            RECOVERY_LEASE,
            source.snapshot(RECOVERY_MAX + 2 * RECOVERY_LEASE),
        )
        .await
        .map_err(|_| unavailable("snapshot capture timed out"))
        .and_then(|view| view);
        if let Err(error) = &view {
            Self::discard(&mut state, error);
        }
        let view = view?;
        let now = crate::monotonic_now();
        let lease = Arc::new(RecoveryLease {
            dataset: dataset.into(),
            namespace: namespace.into(),
            boundary: view.sequence,
            expires: Mutex::new(now + RECOVERY_LEASE),
            deadline: now + RECOVERY_MAX,
        });
        state.recovery.push(Arc::downgrade(&lease));
        Ok((view, lease))
    }

    pub async fn recovery_events(
        &self,
        dataset: &str,
        namespace: &str,
        from: u64,
        lease: &RecoveryLease,
    ) -> StorageResult<Vec<Event>> {
        if !lease.valid()
            || from <= lease.boundary
            || lease.dataset != dataset
            || lease.namespace != namespace
        {
            return Err(unavailable(
                "recovery lease expired or invalid scope/cursor",
            ));
        }
        let (source, durable) = {
            let ns = self.namespace(dataset, namespace);
            let mut state = ns.state.lock().await;
            let source = self.source(&mut state, dataset, namespace).await?;
            (source, state.head)
        };
        // The namespace gate is released so commits continue during the bounded read. The store can also hold batches
        // whose flush has not succeeded, and a crash may yet erase those, so nothing past the durable head goes out.
        Ok(source
            .events_from(from, 256)
            .await?
            .into_iter()
            .map(|entry| entry.event)
            .take_while(|event| event.sequence <= durable)
            .collect())
    }

    /// Cleanup is the only post-response flow. It never decides commit success.
    pub async fn cleanup(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        if self.held(&mut state) || state.unresolved.is_some() {
            return Ok(());
        }
        let source = self.source(&mut state, dataset, namespace).await?;
        let _slot = self
            .slots
            .acquire()
            .await
            .map_err(|_| unavailable("persistence closed"))?;
        let now = (self.clock)().max(state.committed_at);
        state
            .recovery
            .retain(|lease| lease.upgrade().is_some_and(|l| l.valid()));
        let through = state
            .recovery
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .map(|lease| lease.boundary)
            .min()
            .unwrap_or(u64::MAX);
        let removed = match source
            .cleanup_through(now, &self.persistence.retention, through)
            .await
        {
            Ok(removed) => removed,
            Err(error) => {
                Self::discard(&mut state, &error);
                return Err(error);
            }
        };
        assert!(
            removed <= through,
            "cleanup removed through {removed}, past recovery boundary {through}"
        );
        if self.owner.is_none() {
            self.log
                .prune(dataset, namespace, removed)
                .map_err(|detail| unavailable(&detail))?;
        }
        state.retries.expire(now);
        Ok(())
    }

    pub async fn close(&self) -> StorageResult<()> {
        self.closed.store(true, Ordering::SeqCst);
        let namespaces: Vec<_> = self.namespaces.lock().unwrap().values().cloned().collect();
        let mut failure = None;
        for ns in namespaces {
            let mut state = ns.state.lock().await;
            if let Some(source) = state.source.take()
                && let Err(error) = source.close().await
            {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub async fn commit(self: &Arc<Self>, write: Write) -> Result<u64, CommitError> {
        let mut committed = self.commit_all(vec![write]).await?;
        Ok(committed.pop().expect("one write, one revision"))
    }

    /// Commits writes to one namespace in order and in one atomic batch: they enter the queue together, and a drain
    /// takes the whole queue. Returns each write's revision, or the first error, which then applies to all of them.
    pub async fn commit_all(self: &Arc<Self>, writes: Vec<Write>) -> Result<Vec<u64>, CommitError> {
        let Some(first) = writes.first() else {
            return Ok(Vec::new());
        };
        for write in &writes {
            validate(write)?;
            if write.dataset != first.dataset || write.namespace != first.namespace {
                return Err(CommitError::Invalid(
                    "one batch writes one namespace".into(),
                ));
            }
        }
        if self.closed.load(Ordering::SeqCst) {
            return Err(CommitError::Unavailable("writer closed".into()));
        }
        let ns = self.namespace(&first.dataset, &first.namespace);
        let mut waiting = Vec::with_capacity(writes.len());
        {
            let mut queue = ns.queue.lock().unwrap();
            // A batch larger than the bound is admitted into an empty queue, so it is bounded by its caller's request size.
            if queue.len() >= MAX_QUEUED
                || (!queue.is_empty() && queue.len() + writes.len() > MAX_QUEUED)
            {
                return Err(CommitError::Busy);
            }
            for write in writes {
                let (done, committed) = oneshot::channel();
                queue.push(Queued { write, done });
                waiting.push(committed);
            }
        }
        // Caller cancellation cannot interrupt persistence between buffering and durability.
        tokio::spawn(self.clone().drain(ns));
        let mut revisions = Vec::with_capacity(waiting.len());
        let mut failure = None;
        for committed in waiting {
            match committed.await {
                Ok(Ok(revision)) => revisions.push(revision),
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                }
                Err(_) => {
                    failure.get_or_insert(CommitError::Unavailable("writer stopped".into()));
                }
            }
        }
        failure.map_or(Ok(revisions), Err)
    }

    /// Reads of namespace `dataset/namespace` from this owner's handle, opening it if needed (#58 / B-26). Only what
    /// a flush made durable is visible. The reader fails once the handle closes; ask again for a fresh one.
    // ponytail: takes the namespace gate, so a read waits out a flush in progress; a lock-free handle if that shows.
    pub async fn reader(&self, dataset: &str, namespace: &str) -> StorageResult<Reader> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        self.settle(&mut state, dataset, namespace).await?;
        let source = self.source(&mut state, dataset, namespace).await?;
        *ns.used.lock().unwrap() = Some(crate::monotonic_now());
        Ok(source.reader())
    }

    /// Takes a failure seen by a [`Writer::reader`] into account, as a failed write would be: a fenced handle waits
    /// out [`FENCE_HOLD`], any other failed one is reopened by the next use.
    pub async fn report(&self, dataset: &str, namespace: &str, error: &StorageError) {
        if !matches!(
            error,
            StorageError::Fenced(_)
                | StorageError::Corruption(_)
                | StorageError::IoError(_)
                | StorageError::Unavailable(_)
        ) {
            return;
        }
        let Some(ns) = self
            .namespaces
            .lock()
            .unwrap()
            .get(&(dataset.into(), namespace.into()))
            .cloned()
        else {
            return;
        };
        let mut state = ns.state.lock().await;
        if state.source.is_some() && state.unresolved.is_none() {
            Self::discard(&mut state, error);
        }
    }

    /// The namespace's live key and value bytes as of its durable head, for storage quotas.
    pub async fn live_bytes(&self, dataset: &str, namespace: &str) -> StorageResult<u64> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        self.settle(&mut state, dataset, namespace).await?;
        let source = self.source(&mut state, dataset, namespace).await?;
        source.live_bytes().await
    }

    /// Closes the namespace's handle, e.g. once this node no longer owns it. A batch whose flush failed keeps it open,
    /// so it is retried rather than lost; the next owner's open recovers whatever did land.
    pub async fn release(&self, dataset: &str, namespace: &str) {
        let Some(ns) = self
            .namespaces
            .lock()
            .unwrap()
            .get(&(dataset.into(), namespace.into()))
            .cloned()
        else {
            return;
        };
        let mut state = ns.state.lock().await;
        if state.unresolved.is_none() {
            Self::shut(&ns, &mut state).await;
        }
    }

    /// Closes the handles idle for at least `idle`, with what [`Writer::release`] keeps open.
    pub async fn sweep(&self, idle: std::time::Duration) {
        let now = crate::monotonic_now();
        let idle: Vec<(String, String)> = self
            .namespaces
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, ns)| {
                ns.open.load(Ordering::SeqCst)
                    && ns
                        .used
                        .lock()
                        .unwrap()
                        .is_none_or(|used| now.duration_since(used) >= idle)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for (dataset, namespace) in idle {
            self.release(&dataset, &namespace).await;
        }
    }

    /// Open handles with their durable head and idle time, for introspection.
    pub fn open_namespaces(&self) -> Vec<(String, String, std::time::Duration)> {
        let now = crate::monotonic_now();
        self.namespaces
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, ns)| ns.open.load(Ordering::SeqCst))
            .map(|((dataset, namespace), ns)| {
                let used = ns.used.lock().unwrap().unwrap_or(now);
                (dataset.clone(), namespace.clone(), now.duration_since(used))
            })
            .collect()
    }

    /// Deletes every stored object of the namespace after closing this writer's handle and fencing any other: the open
    /// here takes the namespace from whichever writer still had it, so nothing writes back under the deleted prefix.
    pub async fn destroy(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        Self::shut(&ns, &mut state).await;
        let factory = self.persistence.factory.as_ref();
        factory.open(dataset, namespace).await?.close().await?;
        factory.destroy(dataset, namespace).await
    }

    fn namespace(&self, dataset: &str, namespace: &str) -> Arc<Namespace> {
        self.namespaces
            .lock()
            .unwrap()
            .entry((dataset.into(), namespace.into()))
            .or_default()
            .clone()
    }

    async fn drain(self: Arc<Self>, ns: Arc<Namespace>) {
        let mut state = ns.state.lock().await;
        let queued = std::mem::take(&mut *ns.queue.lock().unwrap());
        if queued.is_empty() {
            return;
        }
        let dataset = queued[0].write.dataset.clone();
        let name = queued[0].write.namespace.clone();
        if self.owner.is_some() {
            *ns.used.lock().unwrap() = Some(crate::monotonic_now());
        }
        let setup = if self.held(&mut state) {
            Err(CommitError::Fenced)
        } else {
            self.source(&mut state, &dataset, &name)
                .await
                .map(|_| ())
                .map_err(commit_error)
        };
        if let Err(error) = setup {
            for item in queued {
                let _ = item.done.send(Err(error.clone()));
            }
            return;
        }
        if let Some(entries) = state.unresolved.take()
            && let Err(error) = self.persist(&mut state, entries).await
        {
            for item in queued {
                let _ = item.done.send(Err(error.clone()));
            }
            return;
        }
        let committed_at = (self.clock)().max(state.committed_at);
        let Some(retry_expires_at) =
            committed_at.checked_add(self.persistence.retention.retry_retention_ms)
        else {
            for item in queued {
                let _ = item.done.send(Err(CommitError::Unavailable(
                    "retry expiry exceeds timestamp range".into(),
                )));
            }
            return;
        };
        state.retries.expire(committed_at);
        let mut entries = Vec::new();
        let mut in_batch = BTreeMap::new();
        let mut waiting = Vec::new();
        // Answered after persistence: `head` may count this batch's earlier writes, which a failed flush leaves unpublished.
        let mut conflicts = Vec::new();
        for Queued { write, done } in queued {
            let key = write.idempotency_key;
            if let Some(sequence) = key
                .as_ref()
                .and_then(|key| state.retries.get(key))
                .map(|retry| retry.sequence)
            {
                let _ = done.send(Ok(sequence));
                continue;
            }
            if let Some(&sequence) = key.as_ref().and_then(|key| in_batch.get(key)) {
                waiting.push((done, sequence));
                continue;
            }
            let head = state.head + entries.len() as u64;
            if write.expected.is_some_and(|expected| expected != head) {
                conflicts.push((done, head));
                continue;
            }
            let Some(sequence) = state
                .head
                .checked_add(entries.len() as u64)
                .and_then(|n| n.checked_add(1))
            else {
                let _ = done.send(Err(CommitError::Unavailable(
                    "namespace revision exhausted".into(),
                )));
                continue;
            };
            if let Some(key) = &key {
                in_batch.insert(key.clone(), sequence);
            }
            entries.push(Committed {
                event: Event {
                    stream: stream(&write.dataset, 0),
                    dataset: write.dataset,
                    namespace: write.namespace,
                    sequence,
                    epoch: EPOCH,
                    committed_at,
                    key: write.key,
                    operation: write.operation,
                },
                retry_expires_at: if key.is_some() { retry_expires_at } else { 0 },
                retry_key: key,
            });
            waiting.push((done, sequence));
        }
        let result = if entries.is_empty() {
            Ok(())
        } else {
            self.persist(&mut state, entries).await
        };
        drop(state);
        for (done, sequence) in waiting {
            let _ = done.send(result.clone().map(|()| sequence));
        }
        for (done, head) in conflicts {
            let _ = done.send(result.clone().and(Err(CommitError::Conflict { head })));
        }
        if result.is_ok() {
            self.schedule_cleanup(ns, dataset, name);
        }
        // Opens that happened while this batch held its namespace could not close it.
        self.evict(None).await;
    }

    async fn persist(&self, state: &mut State, entries: Vec<Committed>) -> Result<(), CommitError> {
        let _slot = self
            .slots
            .acquire()
            .await
            .map_err(|_| CommitError::Unavailable("persistence closed".into()))?;
        let source = state
            .source
            .as_ref()
            .expect("source opened before persistence");
        match source.persist(&entries, None).await {
            Ok(Persisted {
                sequence,
                committed_at,
                ..
            }) => {
                assert!(sequence >= state.head, "the namespace head moved backwards");
                let last = entries.last().map(|entry| entry.event.clone());
                for entry in entries {
                    if entry.event.sequence > state.head {
                        if let Some(key) = entry.retry_key {
                            let retry = RetryRecord {
                                sequence: entry.event.sequence,
                                expires_at: entry.retry_expires_at,
                            };
                            state.retries.insert(key, retry);
                        }
                        if self.owner.is_none() {
                            self.log
                                .append(entry.event)
                                .expect("batch continues durable log");
                        }
                    }
                }
                if let Some(last) = last.filter(|_| self.owner.is_none()) {
                    assert_eq!(sequence, last.sequence, "persisted a different head");
                    let log = self.log.head(&last.dataset, &last.namespace).get();
                    assert_eq!(log, sequence, "the log and the durable head disagree");
                }
                state.head = sequence;
                state.committed_at = committed_at;
                Ok(())
            }
            Err(error) => {
                if !matches!(error, StorageError::Fenced(_)) {
                    state.unresolved = Some(entries);
                }
                Self::discard(state, &error);
                Err(commit_error(error))
            }
        }
    }

    fn schedule_cleanup(self: &Arc<Self>, ns: Arc<Namespace>, dataset: String, name: String) {
        {
            let mut flow = ns.flow.lock().unwrap();
            flow.requested = flow.requested.saturating_add(1);
            if flow.running {
                return;
            }
            flow.running = true;
        }
        let writer = self.clone();
        tokio::spawn(async move {
            loop {
                let requested = ns.flow.lock().unwrap().requested;
                let result = writer.cleanup(&dataset, &name).await;
                writer.persistence.metrics.record(&dataset, result.is_err());
                let mut flow = ns.flow.lock().unwrap();
                if flow.requested <= requested {
                    flow.running = false;
                    return;
                }
            }
        });
    }
}

fn commit_error(error: StorageError) -> CommitError {
    match error {
        StorageError::Fenced(_) => CommitError::Fenced,
        error => CommitError::Unavailable(error.to_string()),
    }
}

fn validate(write: &Write) -> Result<(), CommitError> {
    validate_scope(&write.dataset, &write.namespace)
        .map_err(|error| CommitError::Invalid(error.to_string()))?;
    if write.key.is_empty() {
        return Err(CommitError::Invalid("key must not be empty".into()));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/replication/materialized_writer.rs"]
mod materialized_tests;
#[cfg(test)]
#[path = "../../tests/unit/replication/writer.rs"]
mod tests;
