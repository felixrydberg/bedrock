//! Single-node ordered commits (#9 / B-09, #11 / B-11).
//! Each SlateDB batch atomically stores original events, resolved values, retry results
//! and progress. Only remote durability allows a response or subscription publication.
//! Future B-09b commits through Raft first and uses the same persistence operation.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family},
};
use tokio::sync::oneshot;

use super::log::MemoryLog;
use super::materialized::{
    Committed, Materialized, Progress, Retention, RetryRecord, View, validate_scope,
};
use super::{Event, Operation, stream};
use crate::storage::namespace::NamespaceFactory;
use crate::storage::{ErrorContext, StorageError, StorageResult};
use crate::transport::admin::Admin;

pub const EPOCH: u64 = 1;
/// Maximum writes waiting for the next namespace batch.
pub const MAX_QUEUED: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    pub dataset: String,
    pub namespace: String,
    pub key: Vec<u8>,
    pub operation: Operation,
    /// Until fixed expiry, a retry returns its original sequence without applying again.
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    Invalid(String),
    Busy,
    Fenced,
    /// Persistence may have completed; retry with the same idempotency key.
    Unavailable(String),
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid write: {reason}"),
            Self::Busy => f.write_str("too many writes waiting in this namespace"),
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
}

#[derive(Default)]
struct Namespace {
    queue: Mutex<Vec<Queued>>,
    state: tokio::sync::Mutex<State>,
    flow: Mutex<CleanupFlow>,
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
    retries: BTreeMap<String, RetryRecord>,
    unresolved: Option<Vec<Committed>>,
    fenced: bool,
    recovery: Vec<std::sync::Weak<RecoveryLease>>,
}

/// Keeps the committed tail after a coherent view available while it is transferred.
pub struct RecoveryLease {
    dataset: String,
    namespace: String,
    boundary: u64,
    expires: tokio::time::Instant,
}

impl RecoveryLease {
    pub fn valid(&self) -> bool {
        crate::monotonic_now() < self.expires
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

    pub async fn open(clock: fn() -> u64, config: Persistence) -> StorageResult<Arc<Self>> {
        config.retention.validate()?;
        let log = Arc::new(MemoryLog::with_retention(
            clock,
            config.retention.event_retention_ms,
        ));
        let writer = Arc::new(Self {
            clock,
            log,
            namespaces: Mutex::default(),
            persistence: config,
            slots: tokio::sync::Semaphore::new(4),
            closed: AtomicBool::new(false),
        });
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
            let entries = source.events().await?;
            self.log
                .reset(dataset, namespace, progress.pruned_through)
                .map_err(|detail| unavailable(&detail))?;
            for entry in entries {
                self.log
                    .append(entry.event)
                    .map_err(|detail| unavailable(&detail))?;
            }
            state.head = progress.sequence;
            state.committed_at = progress.committed_at;
            state.retries = progress.retries;
            Ok(())
        }
        .await;
        if let Err(error) = restored {
            let _ = source.close().await;
            return Err(error);
        }
        state.source = Some(source.clone());
        Ok(source)
    }

    /// Owns a complete coherent view before releasing the namespace gate.
    pub async fn bootstrap(&self, dataset: &str, namespace: &str) -> StorageResult<View> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        if state.fenced || state.unresolved.is_some() {
            return Err(unavailable("unresolved persistence outcome"));
        }
        self.source(&mut state, dataset, namespace)
            .await?
            .view()
            .await
    }

    pub async fn begin_recovery(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<(View, Arc<RecoveryLease>)> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        if state.fenced || state.unresolved.is_some() {
            return Err(unavailable("unresolved persistence outcome"));
        }
        state
            .recovery
            .retain(|lease| lease.upgrade().is_some_and(|l| l.valid()));
        if state.recovery.len() >= 4 {
            return Err(unavailable("namespace recovery capacity exhausted"));
        }
        let view = self
            .source(&mut state, dataset, namespace)
            .await?
            .view()
            .await?;
        let lease = Arc::new(RecoveryLease {
            dataset: dataset.into(),
            namespace: namespace.into(),
            boundary: view.sequence,
            expires: crate::monotonic_now() + std::time::Duration::from_secs(60),
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
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        Ok(self
            .source(&mut state, dataset, namespace)
            .await?
            .events()
            .await?
            .into_iter()
            .filter(|entry| entry.event.sequence >= from)
            .take(256)
            .map(|entry| entry.event)
            .collect())
    }

    /// Cleanup is the only post-response flow. It never decides commit success.
    pub async fn cleanup(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        validate_scope(dataset, namespace)?;
        let ns = self.namespace(dataset, namespace);
        let mut state = ns.state.lock().await;
        if state.fenced || state.unresolved.is_some() {
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
        let removed = source
            .cleanup_through(now, &self.persistence.retention, through)
            .await?;
        self.log
            .prune(dataset, namespace, removed)
            .map_err(|detail| unavailable(&detail))?;
        state.retries.retain(|_, retry| retry.expires_at > now);
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
        validate(&write)?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(CommitError::Unavailable("writer closed".into()));
        }
        let ns = self.namespace(&write.dataset, &write.namespace);
        let (done, committed) = oneshot::channel();
        {
            let mut queue = ns.queue.lock().unwrap();
            if queue.len() >= MAX_QUEUED {
                return Err(CommitError::Busy);
            }
            queue.push(Queued { write, done });
        }
        // Caller cancellation cannot interrupt persistence between buffering and durability.
        tokio::spawn(self.clone().drain(ns));
        committed
            .await
            .unwrap_or_else(|_| Err(CommitError::Unavailable("writer stopped".into())))
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
        let setup = if state.fenced {
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
        state
            .retries
            .retain(|_, retry| retry.expires_at > committed_at);
        let mut entries = Vec::new();
        let mut in_batch = BTreeMap::new();
        let mut waiting = Vec::new();
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
        if result.is_ok() {
            self.schedule_cleanup(ns, dataset, name);
        }
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
            Ok(Progress {
                sequence,
                committed_at,
                retries,
                ..
            }) => {
                for entry in entries {
                    if entry.event.sequence > state.head {
                        self.log
                            .append(entry.event)
                            .expect("batch continues durable log");
                    }
                }
                state.head = sequence;
                state.committed_at = committed_at;
                state.retries = retries;
                Ok(())
            }
            Err(error) => {
                if matches!(error, StorageError::Fenced(_)) {
                    state.fenced = true;
                } else {
                    state.unresolved = Some(entries);
                }
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
