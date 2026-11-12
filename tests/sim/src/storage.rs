//! All simulated storage (#25 / B-23): the in-memory namespace store that stands in for S3, and wrappers around it and
//! [`MemoryEngine`] that own faults, deliberate bugs and restart persistence.
//!
//! `bedrock-lib` compiles this file as `storage::sim` for its tests and simulation builds, because its in-crate unit
//! tests cannot depend on another crate without a second copy of its traits. Production builds never contain it. Fault
//! decisions come from the closures a harness passes in, so the seeded randomness stays in the harness.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;

use super::memory::MemoryEngine;
use super::namespace::{
    Changes, NamespaceDatabase, NamespaceFactory, NamespaceReader, NamespaceScan, valid_identity,
};
use super::{
    Entry, ErrorContext, KeyRange, Scan, Snapshot, StorageEngine, StorageError, StorageResult,
    StorageStats, WriteBatch, WriteOp,
};

/// A running hash of everything a simulation decided and did, each with its simulated time. Two runs match only if every
/// fault decision and every write matched in content, order and time, so a match means the run really was reproduced.
#[derive(Clone)]
pub struct Fingerprint {
    hasher: Arc<Mutex<DefaultHasher>>,
    started: tokio::time::Instant,
}

impl Fingerprint {
    pub fn new() -> Self {
        Fingerprint {
            hasher: Arc::default(),
            started: crate::monotonic_now(),
        }
    }

    pub fn mix(&self, event: impl Hash) {
        let mut hasher = self.hasher.lock().unwrap();
        self.started.elapsed().as_nanos().hash(&mut *hasher);
        event.hash(&mut *hasher);
    }

    pub fn value(&self) -> u64 {
        self.hasher.lock().unwrap().finish()
    }
}

impl Default for Fingerprint {
    fn default() -> Self {
        Self::new()
    }
}

type Identity = (String, String);
type Entries = BTreeMap<Vec<u8>, Vec<u8>>;
type State = BTreeMap<Identity, DurableNamespace>;

#[derive(Default)]
struct DurableNamespace {
    entries: Entries,
    // Identity is used only for equality, never message ordering or output.
    generation: Arc<()>,
}

/// Deterministic namespace storage with separate buffered and durable state (#11 / B-11). Opening a new writer or
/// crashing invalidates old handles; only explicit flush or clean close persists. Clones share durable state, including
/// across modeled process crashes.
#[derive(Clone, Default)]
pub struct MemoryFactory {
    state: Arc<Mutex<State>>,
}

struct View {
    entries: Entries,
    /// Buffered changes, so a flush costs what was written rather than the size of the namespace.
    unflushed: Changes,
    closed: bool,
}

struct MemoryDatabase {
    state: Arc<Mutex<State>>,
    identity: Identity,
    instance: String,
    generation: Arc<()>,
    view: Mutex<View>,
}

// The deterministic model copies its baseline; the native adapter pins MVCC versions instead.
struct MemoryScan(std::vec::IntoIter<Entry>);

#[async_trait]
impl NamespaceScan for MemoryScan {
    async fn next(&mut self) -> StorageResult<Option<Entry>> {
        Ok(self.0.next())
    }
}

fn context(engine: &'static str, instance: &str, detail: &str) -> ErrorContext {
    ErrorContext {
        engine,
        instance: instance.into(),
        file: None,
        partition: None,
        detail: detail.into(),
    }
}

fn unavailable(instance: &str, detail: &str) -> StorageError {
    StorageError::Unavailable(context("namespace-memory", instance, detail))
}

fn fenced(instance: &str) -> StorageError {
    StorageError::Fenced(context(
        "namespace-memory",
        instance,
        "newer writer or crash",
    ))
}

impl MemoryFactory {
    /// Discards every handle's unflushed state without changing any durable namespace.
    pub fn crash(&self) -> StorageResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?;
        for namespace in state.values_mut() {
            namespace.generation = Arc::new(());
        }
        Ok(())
    }
}

fn apply(entries: &mut Entries, changes: Changes) {
    for (key, value) in changes {
        match value {
            Some(value) => {
                entries.insert(key, value);
            }
            None => {
                entries.remove(&key);
            }
        }
    }
}

impl MemoryDatabase {
    fn with_active<T>(
        &self,
        operation: impl FnOnce(&mut DurableNamespace, &mut View) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable(&self.instance, "state lock poisoned"))?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| unavailable(&self.instance, "view lock poisoned"))?;
        let namespace = state
            .get_mut(&self.identity)
            .ok_or_else(|| unavailable(&self.instance, "namespace is unavailable"))?;
        if !Arc::ptr_eq(&namespace.generation, &self.generation) {
            return Err(fenced(&self.instance));
        }
        if view.closed {
            return Err(unavailable(&self.instance, "database closed"));
        }
        operation(namespace, &mut view)
    }
}

#[async_trait]
impl NamespaceFactory for MemoryFactory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?
            .keys()
            .cloned()
            .collect())
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        if !valid_identity(dataset) || !valid_identity(namespace) {
            return Err(StorageError::Unsupported(context(
                "namespace-memory",
                "namespace",
                "dataset and namespace must be nonempty single path components",
            )));
        }
        let identity = (dataset.to_owned(), namespace.to_owned());
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?;
        let durable = state.entry(identity.clone()).or_default();
        durable.generation = Arc::new(());
        Ok(Arc::new(MemoryDatabase {
            state: self.state.clone(),
            identity,
            instance: format!("{dataset}/{namespace}"),
            generation: durable.generation.clone(),
            view: Mutex::new(View {
                entries: durable.entries.clone(),
                unflushed: Changes::new(),
                closed: false,
            }),
        }))
    }

    /// Follows the durable state at once: the model has no publication lag, and never sees a buffered write.
    async fn open_reader(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Option<Arc<dyn NamespaceReader>>> {
        if !valid_identity(dataset) || !valid_identity(namespace) {
            return Err(StorageError::Unsupported(context(
                "namespace-memory",
                "namespace",
                "dataset and namespace must be nonempty single path components",
            )));
        }
        let identity = (dataset.to_owned(), namespace.to_owned());
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?;
        if !state.contains_key(&identity) {
            return Ok(None);
        }
        Ok(Some(Arc::new(MemoryReader {
            state: self.state.clone(),
            identity,
            instance: format!("{dataset}/{namespace}"),
            closed: AtomicBool::new(false),
        })))
    }

    async fn destroy(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        self.state
            .lock()
            .map_err(|_| unavailable("namespace", "state lock poisoned"))?
            .remove(&(dataset.to_owned(), namespace.to_owned()));
        Ok(())
    }
}

struct MemoryReader {
    state: Arc<Mutex<State>>,
    identity: Identity,
    instance: String,
    closed: AtomicBool,
}

impl MemoryReader {
    fn with_durable<T>(&self, read: impl FnOnce(&Entries) -> T) -> StorageResult<T> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(unavailable(&self.instance, "reader closed"));
        }
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable(&self.instance, "state lock poisoned"))?;
        // A destroyed database's objects are gone; a reader still holding it fails rather than reading nothing.
        let namespace = state
            .get(&self.identity)
            .ok_or_else(|| unavailable(&self.instance, "namespace objects are missing"))?;
        Ok(read(&namespace.entries))
    }
}

#[async_trait]
impl NamespaceReader for MemoryReader {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.with_durable(|entries| entries.get(key).cloned())
    }

    async fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StorageResult<Vec<Entry>> {
        let range = KeyRange::prefix(prefix);
        self.with_durable(|entries| {
            range.bounds_after(after).map_or_else(Vec::new, |bounds| {
                entries
                    .range::<[u8], _>(bounds)
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
        })
    }

    async fn close(&self) -> StorageResult<()> {
        self.closed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl NamespaceDatabase for MemoryDatabase {
    async fn snapshot(
        &self,
        prefix: &[u8],
        _lifetime: std::time::Duration,
    ) -> StorageResult<Box<dyn NamespaceScan>> {
        Ok(Box::new(MemoryScan(self.scan(prefix).await?.into_iter())))
    }

    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.with_active(|_, view| Ok(view.entries.get(key).cloned()))
    }

    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        let range = KeyRange::prefix(prefix);
        self.with_active(|_, view| {
            Ok(view
                .entries
                .range::<[u8], _>(range.bounds().expect("prefix range is never empty"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        })
    }

    async fn write(&self, changes: Changes) -> StorageResult<()> {
        self.with_active(|_, view| {
            if changes.iter().any(|(key, value)| {
                key.is_empty()
                    || u32::try_from(key.len()).is_err()
                    || value
                        .as_ref()
                        .is_some_and(|value| u32::try_from(value.len()).is_err())
            }) {
                return Err(StorageError::Unsupported(context(
                    "namespace-memory",
                    &self.instance,
                    "SlateDB keys must be nonempty and keys/values must fit u32 lengths",
                )));
            }
            view.unflushed.extend(changes.iter().cloned());
            apply(&mut view.entries, changes);
            Ok(())
        })
    }

    async fn flush(&self) -> StorageResult<()> {
        self.with_active(|namespace, view| {
            apply(&mut namespace.entries, std::mem::take(&mut view.unflushed));
            Ok(())
        })
    }

    async fn close(&self) -> StorageResult<()> {
        self.with_active(|namespace, view| {
            apply(&mut namespace.entries, std::mem::take(&mut view.unflushed));
            view.closed = true;
            Ok(())
        })
    }
}

/// What a call into a namespace database does, as a fault hook sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreAccess {
    Open,
    Read,
    Write,
    Flush,
    Close,
}

/// An injected object-store failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreFault {
    Unavailable,
    /// The call takes effect, but the caller sees `IoError`: the store's response was lost.
    Ambiguous,
    /// The store answers late. A crash during the wait loses the call.
    Delay(Duration),
    /// A conditional write finds a newer writer. `landed` is the ambiguous case: this write got in first and only its
    /// response was lost, so the caller hears `Fenced` for a write that took effect.
    Fenced {
        landed: bool,
    },
    /// A deliberate bug: the flush reports success and persists nothing.
    LostFlush,
    /// A deliberate bug: the write loses its retry records.
    DropRetries,
}

/// Decides each call into a database of a [`SimFactory`].
pub type StoreFaults = Arc<dyn Fn(StoreAccess) -> Option<StoreFault> + Send + Sync>;

/// A [`MemoryFactory`] whose calls fail as `faults` decides. Clones share state, as API processes share S3.
#[derive(Clone)]
pub struct SimFactory {
    inner: MemoryFactory,
    faults: StoreFaults,
    fingerprint: Option<Fingerprint>,
    observer: Option<StoreObserver>,
}

/// Sees every batch written to a namespace, as `dataset/namespace`. Checkers use it for what sampling would miss.
pub type StoreObserver = Arc<dyn Fn(&str, &Changes) + Send + Sync>;

struct SimDatabase {
    inner: Arc<dyn NamespaceDatabase>,
    faults: StoreFaults,
    instance: String,
    fingerprint: Option<Fingerprint>,
    observer: Option<StoreObserver>,
    /// Writes the observer has not seen, because they may yet be lost.
    unflushed: Mutex<Vec<Changes>>,
}

impl SimDatabase {
    fn durable(&self) {
        if let Some(observer) = &self.observer {
            for changes in self.unflushed.lock().unwrap().drain(..) {
                observer(&self.instance, &changes);
            }
        }
    }
}

/// Waits out or fails `access`. `Ok` carries a fault the call has to finish applying first.
async fn inject(
    faults: &StoreFaults,
    access: StoreAccess,
    instance: &str,
) -> StorageResult<Option<StoreFault>> {
    match faults(access) {
        Some(StoreFault::Delay(delay)) => {
            tokio::time::sleep(delay).await;
            Ok(None)
        }
        Some(StoreFault::Unavailable) => Err(unavailable(instance, "injected fault")),
        Some(StoreFault::Fenced { landed: false }) => Err(fenced(instance)),
        fault => Ok(fault),
    }
}

/// `result`, unless `fault` says the caller never hears of it.
fn finish<T>(
    fault: Option<StoreFault>,
    instance: &str,
    result: StorageResult<T>,
) -> StorageResult<T> {
    match fault {
        Some(StoreFault::Ambiguous) => result.and(Err(StorageError::IoError(context(
            "namespace-memory",
            instance,
            "injected fault",
        )))),
        Some(StoreFault::Fenced { landed: true }) => result.and(Err(fenced(instance))),
        _ => result,
    }
}

impl SimFactory {
    pub fn new(faults: StoreFaults) -> Self {
        SimFactory {
            inner: MemoryFactory::default(),
            faults,
            fingerprint: None,
            observer: None,
        }
    }

    /// Mixes every write into `fingerprint`.
    pub fn recorded(mut self, fingerprint: Fingerprint) -> Self {
        self.fingerprint = Some(fingerprint);
        self
    }

    /// Shows every write to `observer` once it is durable, in commit order.
    pub fn observed(mut self, observer: StoreObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Discards every handle's unflushed state without changing any durable namespace.
    pub fn crash(&self) -> StorageResult<()> {
        self.inner.crash()
    }
}

#[async_trait]
impl NamespaceFactory for SimFactory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        self.inner.namespaces().await
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        inject(&self.faults, StoreAccess::Open, "namespace").await?;
        Ok(Arc::new(SimDatabase {
            inner: self.inner.open(dataset, namespace).await?,
            faults: self.faults.clone(),
            instance: format!("{dataset}/{namespace}"),
            fingerprint: self.fingerprint.clone(),
            observer: self.observer.clone(),
            unflushed: Mutex::default(),
        }))
    }

    async fn open_reader(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Option<Arc<dyn NamespaceReader>>> {
        inject(&self.faults, StoreAccess::Open, "namespace").await?;
        let Some(inner) = self.inner.open_reader(dataset, namespace).await? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(SimReader {
            inner,
            faults: self.faults.clone(),
            instance: format!("{dataset}/{namespace}"),
        })))
    }

    async fn destroy(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        let instance = format!("{dataset}/{namespace}");
        let fault = inject(&self.faults, StoreAccess::Write, &instance).await?;
        let destroyed = self.inner.destroy(dataset, namespace).await;
        finish(fault, &instance, destroyed)
    }
}

/// A reader whose calls fail as the factory's faults decide. Its reads take the `Read` faults, like a writer's.
struct SimReader {
    inner: Arc<dyn NamespaceReader>,
    faults: StoreFaults,
    instance: String,
}

#[async_trait]
impl NamespaceReader for SimReader {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        inject(&self.faults, StoreAccess::Read, &self.instance).await?;
        self.inner.get(key).await
    }

    async fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StorageResult<Vec<Entry>> {
        inject(&self.faults, StoreAccess::Read, &self.instance).await?;
        self.inner.scan(prefix, after, limit).await
    }

    async fn close(&self) -> StorageResult<()> {
        inject(&self.faults, StoreAccess::Close, &self.instance).await?;
        self.inner.close().await
    }
}

#[async_trait]
impl NamespaceDatabase for SimDatabase {
    async fn snapshot(
        &self,
        prefix: &[u8],
        lifetime: std::time::Duration,
    ) -> StorageResult<Box<dyn NamespaceScan>> {
        inject(&self.faults, StoreAccess::Read, &self.instance).await?;
        Ok(Box::new(SimScan {
            inner: self.inner.snapshot(prefix, lifetime).await?,
            faults: self.faults.clone(),
            instance: self.instance.clone(),
        }))
    }

    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        inject(&self.faults, StoreAccess::Read, &self.instance).await?;
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        inject(&self.faults, StoreAccess::Read, &self.instance).await?;
        self.inner.scan(prefix).await
    }

    async fn write(&self, mut changes: Changes) -> StorageResult<()> {
        let fault = inject(&self.faults, StoreAccess::Write, &self.instance).await?;
        if fault == Some(StoreFault::DropRetries) {
            changes.retain(|(key, _)| !key.starts_with(b"r"));
        }
        if let Some(fingerprint) = &self.fingerprint {
            fingerprint.mix((&self.instance, &changes));
        }
        let written = self.inner.write(changes.clone()).await;
        if written.is_ok() {
            self.unflushed.lock().unwrap().push(changes);
        }
        finish(fault, &self.instance, written)
    }

    async fn flush(&self) -> StorageResult<()> {
        let fault = inject(&self.faults, StoreAccess::Flush, &self.instance).await?;
        if fault == Some(StoreFault::LostFlush) {
            return Ok(());
        }
        let flushed = self.inner.flush().await;
        if flushed.is_ok() {
            self.durable();
        }
        finish(fault, &self.instance, flushed)
    }

    async fn close(&self) -> StorageResult<()> {
        let fault = inject(&self.faults, StoreAccess::Close, &self.instance).await?;
        let closed = self.inner.close().await;
        if closed.is_ok() {
            self.durable();
        }
        finish(fault, &self.instance, closed)
    }
}

struct SimScan {
    inner: Box<dyn NamespaceScan>,
    faults: StoreFaults,
    instance: String,
}

#[async_trait]
impl NamespaceScan for SimScan {
    async fn next(&mut self) -> StorageResult<Option<Entry>> {
        inject(&self.faults, StoreAccess::Read, &self.instance).await?;
        self.inner.next().await
    }
}

/// What a call into a [`SimEngine`] does, as a fault hook sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Access {
    /// Reads, on the engine or through a snapshot, and health checks.
    Read,
    /// `write` and `restore`.
    Write,
}

/// An injected local disk failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Fault {
    Io,
    /// A read fails verification.
    Corruption,
    Unavailable,
    /// The write commits, but the caller sees `IoError`, as after a failed sync. Fails plainly elsewhere.
    Ambiguous,
    /// A scan yields this many entries and then an error. Fails plainly elsewhere.
    PartialScan(usize),
}

/// Decides each call into a [`SimEngine`].
pub type Faults = Arc<dyn Fn(Access) -> Option<Fault> + Send + Sync>;

/// A deliberate bug in a dataset engine's apply path, for proving the checkers fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bug {
    /// Drops deletes but still advances the revision.
    SwallowDelete,
    /// Keeps writing a key's first value.
    StaleValue,
    /// Advances the revision without the data.
    DropData,
    /// Keeps the data a revoke deletes.
    KeepRange,
}

/// Committed batches [`SimEngine::crash`] can still take back.
const LOSABLE_BATCHES: usize = 32;

fn l2_op(op: &WriteOp) -> bool {
    matches!(op, WriteOp::Put { key, .. } | WriteOp::Delete { key } if key.starts_with(b"l2/"))
}

/// A state copy that owns its data, so it can outlive the engine's borrow and be restored later.
#[derive(Clone, Debug, PartialEq)]
pub struct Frozen {
    entries: Entries,
    revisions: BTreeMap<String, u64>,
    damaged: BTreeSet<Vec<u8>>,
}

impl Snapshot for Frozen {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.entries.get(key).cloned())
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        let entries: Vec<_> = range
            .bounds()
            .into_iter()
            .flat_map(|bounds| self.entries.range::<[u8], _>(bounds))
            .map(|(key, value)| Ok((key.clone(), value.clone())))
            .collect();
        Ok(Box::new(entries.into_iter()))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        Ok(self.revisions.get(name).copied())
    }

    fn revisions(&self) -> StorageResult<BTreeMap<String, u64>> {
        Ok(self.revisions.clone())
    }

    fn revisions_in(&self, ranges: &[super::Names]) -> StorageResult<BTreeMap<String, u64>> {
        Ok(super::names_in(&self.revisions, ranges))
    }
}

impl Frozen {
    /// Whether a record in this copy failed verification, so reads of it could not be trusted.
    pub fn damaged(&self) -> bool {
        !self.damaged.is_empty()
    }
}

/// A [`MemoryEngine`] on a simulated disk: reads and writes fail as `faults` decides, records can be damaged, and a
/// crash can take back acknowledged batches.
pub struct SimEngine {
    name: String,
    inner: MemoryEngine,
    faults: Faults,
    bug: Option<Bug>,
    /// The first value of each key, for [`Bug::StaleValue`].
    first: Mutex<Entries>,
    /// How to undo each recent commit, newest last. Only an engine that may lose commits keeps them.
    committed: Option<Mutex<VecDeque<Undo>>>,
    damaged: Mutex<BTreeSet<Vec<u8>>>,
    /// Every call fails from now on.
    wedged: AtomicBool,
    fingerprint: Option<Fingerprint>,
}

impl SimEngine {
    pub fn new(name: &str, faults: Faults, bug: Option<Bug>, losable: bool) -> Self {
        SimEngine {
            name: name.to_owned(),
            inner: MemoryEngine::new(name),
            faults,
            bug,
            first: Mutex::default(),
            committed: losable.then(Mutex::default),
            damaged: Mutex::default(),
            wedged: AtomicBool::new(false),
            fingerprint: None,
        }
    }

    /// Mixes every commit into `fingerprint`.
    pub fn recorded(mut self, fingerprint: Fingerprint) -> Self {
        self.fingerprint = Some(fingerprint);
        self
    }

    fn mix_batch(&self, batch: &WriteBatch) {
        let Some(fingerprint) = &self.fingerprint else {
            return;
        };
        let ops: Vec<_> = batch
            .ops
            .iter()
            .map(|op| match op {
                WriteOp::Put { key, value } => (key, Some(value), None),
                WriteOp::Delete { key } => (key, None, None),
                WriteOp::DeleteRange(range) => (&range.start, None, range.end.as_ref()),
            })
            .collect();
        fingerprint.mix((&self.name, ops, &batch.revisions));
    }

    fn failure(
        &self,
        make: fn(ErrorContext) -> StorageError,
        partition: Option<String>,
    ) -> StorageError {
        make(ErrorContext {
            engine: "sim",
            instance: self.name.clone(),
            file: None,
            partition,
            detail: "injected fault".into(),
        })
    }

    fn injected(&self, fault: Fault) -> StorageError {
        match fault {
            Fault::Unavailable => self.failure(StorageError::Unavailable, None),
            Fault::Corruption => self.failure(StorageError::Corruption, None),
            _ => self.failure(StorageError::IoError, None),
        }
    }

    fn damage_error(&self, key: &[u8]) -> StorageError {
        let key = format!("key {}", key.escape_ascii());
        self.failure(StorageError::Corruption, Some(key))
    }

    /// Fails a read as faults decide; `Ok` may carry a partial scan.
    fn read_fault(&self) -> StorageResult<Option<usize>> {
        self.check_wedged()?;
        match (self.faults)(Access::Read) {
            None => Ok(None),
            Some(Fault::PartialScan(after)) => Ok(Some(after)),
            Some(fault) => Err(self.injected(fault)),
        }
    }

    fn check_wedged(&self) -> StorageResult<()> {
        if self.wedged.load(Ordering::SeqCst) {
            return Err(self.failure(StorageError::Unavailable, None));
        }
        Ok(())
    }

    /// The disk stops answering for good.
    pub fn wedge(&self) {
        self.wedged.store(true, Ordering::SeqCst);
    }

    fn check_read(&self) -> StorageResult<()> {
        self.read_fault().map(|_| ())
    }

    /// Damages `key`, which then fails verification on every read of it. Nothing else shows the damage.
    pub fn damage(&self, key: &[u8]) {
        self.lock_damaged().insert(key.to_vec());
    }

    fn lock_damaged(&self) -> std::sync::MutexGuard<'_, BTreeSet<Vec<u8>>> {
        self.damaged.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The current state with faults off, for checkers.
    pub fn frozen(&self) -> Frozen {
        let snapshot = self
            .inner
            .snapshot()
            .expect("memory snapshots are infallible");
        let entries = snapshot
            .scan(&KeyRange::all())
            .expect("memory scans are infallible")
            .map(|entry| entry.expect("damage is tracked by this wrapper"))
            .collect();
        Frozen {
            entries,
            revisions: snapshot.revisions().expect("memory reads are infallible"),
            damaged: self.lock_damaged().clone(),
        }
    }

    /// A power failure on a disk that acknowledged writes it had not synced: the last `lose` committed batches vanish,
    /// data and revisions together, as a write-ahead log truncates at a batch boundary. Returns how many were lost.
    pub fn crash(&self, lose: usize) -> usize {
        let Some(committed) = &self.committed else {
            return 0;
        };
        let mut committed = committed.lock().unwrap();
        let lost = lose.min(committed.len());
        let kept = committed.len() - lost;
        if lost > 0 {
            let mut survivor = self.frozen();
            for undo in committed.drain(kept..).rev() {
                undo.apply(&mut survivor);
            }
            self.inner
                .restore(&survivor)
                .expect("memory restores are infallible");
            *self.lock_damaged() = survivor.damaged;
        }
        committed.clear();
        lost
    }

    /// Keeps what undoes `batch`, before it is written.
    fn remember(&self, batch: &WriteBatch) {
        self.keep(|| {
            let read = "memory reads are infallible";
            let mut keys = Vec::new();
            for op in &batch.ops {
                match op {
                    WriteOp::Put { key, .. } | WriteOp::Delete { key } => {
                        keys.push((key.clone(), self.inner.get(key).expect(read)));
                    }
                    WriteOp::DeleteRange(range) => keys.extend(
                        self.inner
                            .scan(range)
                            .expect(read)
                            .map(|entry| entry.map(|(key, value)| (key, Some(value))).expect(read)),
                    ),
                }
            }
            let revisions = batch
                .revisions
                .keys()
                .map(|name| (name.clone(), self.inner.revision(name).expect(read)))
                .collect();
            Undo::Batch {
                keys,
                revisions,
                damaged: self.lock_damaged().clone(),
            }
        });
    }

    fn keep(&self, undo: impl FnOnce() -> Undo) {
        if let Some(committed) = &self.committed {
            let undo = undo();
            let mut committed = committed.lock().unwrap();
            if committed.len() == LOSABLE_BATCHES {
                committed.pop_front();
            }
            committed.push_back(undo);
        }
    }

    /// `batch` with this engine's deliberate bug applied.
    fn mutated(&self, mut batch: WriteBatch) -> WriteBatch {
        match self.bug {
            None => {}
            Some(Bug::SwallowDelete) => batch
                .ops
                .retain(|op| !(l2_op(op) && matches!(op, WriteOp::Delete { .. }))),
            Some(Bug::DropData) => batch.ops.retain(|op| !l2_op(op)),
            Some(Bug::KeepRange) => batch
                .ops
                .retain(|op| !matches!(op, WriteOp::DeleteRange(_))),
            Some(Bug::StaleValue) => {
                let mut first = self.first.lock().unwrap();
                for op in &mut batch.ops {
                    if let WriteOp::Put { key, value } = op
                        && key.starts_with(b"l2/")
                    {
                        *value = first.entry(key.clone()).or_insert(value.clone()).clone();
                    }
                }
            }
        }
        batch
    }

    fn scanned<'a>(
        &self,
        damaged: &BTreeSet<Vec<u8>>,
        partial: Option<usize>,
        scan: Scan<'a>,
    ) -> Scan<'a> {
        let mut entries: Vec<_> = scan
            .map(|entry| match entry {
                Ok((key, _)) if damaged.contains(&key) => Err(self.damage_error(&key)),
                entry => entry,
            })
            .collect();
        if let Some(after) = partial {
            entries.truncate(after);
            entries.push(Err(self.injected(Fault::Io)));
        }
        Box::new(entries.into_iter())
    }
}

/// What takes one commit back. A batch keeps only what it overwrote, since copying the whole engine on every commit was
/// the largest cost in long runs (B-61).
enum Undo {
    /// Earlier values of the keys and revisions a batch wrote, `None` where there was none, and the damage before it.
    Batch {
        keys: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        revisions: Vec<(String, Option<u64>)>,
        damaged: BTreeSet<Vec<u8>>,
    },
    /// A restore replaces everything, so it keeps everything.
    Whole(Frozen),
}

impl Undo {
    fn apply(self, state: &mut Frozen) {
        match self {
            Undo::Whole(earlier) => *state = earlier,
            Undo::Batch {
                keys,
                revisions,
                damaged,
            } => {
                // Every earlier value was read before the batch, so the order they go back in does not matter.
                for (key, value) in keys {
                    match value {
                        Some(value) => state.entries.insert(key, value),
                        None => state.entries.remove(&key),
                    };
                }
                for (name, revision) in revisions {
                    match revision {
                        Some(revision) => state.revisions.insert(name, revision),
                        None => state.revisions.remove(&name),
                    };
                }
                state.damaged = damaged;
            }
        }
    }
}

/// A snapshot that fails like its engine. Damage is fixed at the time it was taken.
struct SimSnapshot<'a> {
    owner: &'a SimEngine,
    inner: Box<dyn Snapshot + 'a>,
    damaged: BTreeSet<Vec<u8>>,
}

impl Snapshot for SimSnapshot<'_> {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.owner.check_read()?;
        if self.damaged.contains(key) {
            return Err(self.owner.damage_error(key));
        }
        self.inner.get(key)
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        let partial = self.owner.read_fault()?;
        let scan = self.inner.scan(range)?;
        Ok(self.owner.scanned(&self.damaged, partial, scan))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.owner.check_read()?;
        self.inner.revision(name)
    }

    fn revisions(&self) -> StorageResult<BTreeMap<String, u64>> {
        self.owner.check_read()?;
        self.inner.revisions()
    }

    fn revisions_in(&self, ranges: &[super::Names]) -> StorageResult<BTreeMap<String, u64>> {
        self.owner.check_read()?;
        self.inner.revisions_in(ranges)
    }
}

impl StorageEngine for SimEngine {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.check_read()?;
        if self.lock_damaged().contains(key) {
            return Err(self.damage_error(key));
        }
        self.inner.get(key)
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        let partial = self.read_fault()?;
        let damaged = self.lock_damaged().clone();
        let scan = self.inner.scan(range)?;
        Ok(self.scanned(&damaged, partial, scan))
    }

    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        self.check_wedged()?;
        let fault = (self.faults)(Access::Write);
        if let Some(fault) = fault.filter(|fault| *fault != Fault::Ambiguous) {
            return Err(self.injected(fault));
        }
        let batch = self.mutated(batch);
        self.mix_batch(&batch);
        self.remember(&batch);
        {
            // Overwriting a damaged record repairs it.
            let mut damaged = self.lock_damaged();
            for op in &batch.ops {
                match op {
                    WriteOp::Put { key, .. } | WriteOp::Delete { key } => {
                        damaged.remove(key);
                    }
                    WriteOp::DeleteRange(range) => damaged.retain(|key| !range.contains(key)),
                }
            }
        }
        self.inner.write(batch)?;
        fault.map_or(Ok(()), |fault| Err(self.injected(fault)))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.check_read()?;
        self.inner.revision(name)
    }

    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
        Ok(Box::new(SimSnapshot {
            owner: self,
            inner: self.inner.snapshot()?,
            damaged: self.lock_damaged().clone(),
        }))
    }

    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
        self.check_wedged()?;
        // Failing plainly keeps the contract that a failed restore changes nothing.
        if let Some(fault) = (self.faults)(Access::Write) {
            return Err(self.injected(fault));
        }
        self.keep(|| Undo::Whole(self.frozen()));
        self.inner.restore(from)?;
        self.lock_damaged().clear();
        Ok(())
    }

    fn stats(&self) -> StorageResult<StorageStats> {
        self.inner.stats()
    }

    fn disk_usage(&self) -> StorageResult<u64> {
        self.inner.disk_usage()
    }

    fn health(&self) -> StorageResult<()> {
        self.check_read()
    }

    fn compact(&self, range: &KeyRange) -> StorageResult<()> {
        self.inner.compact(range)
    }
}

/// Every node's disks: `(data_dir, name)` always opens the same engine, so a restarted node finds its data and checkers
/// can read every node.
pub struct Disks {
    engines: Mutex<BTreeMap<(PathBuf, String), Arc<SimEngine>>>,
    faults: Faults,
    bug: Option<Bug>,
    losable: bool,
    fingerprint: Fingerprint,
}

impl Disks {
    pub fn new(faults: Faults, bug: Option<Bug>, losable: bool, fingerprint: Fingerprint) -> Self {
        Disks {
            engines: Mutex::default(),
            faults,
            bug,
            losable,
            fingerprint,
        }
    }

    pub fn open(&self, data_dir: &Path, name: &str) -> Arc<SimEngine> {
        let mut engines = self.engines.lock().unwrap();
        let key = (data_dir.to_owned(), name.to_owned());
        engines
            .entry(key)
            .or_insert_with(|| {
                // Applier bugs and cache-tail loss target disposable replicas, not the cell's fsynced Raft authority.
                let bug = self.bug.filter(|_| name != "replay" && name != "control");
                let engine = SimEngine::new(
                    name,
                    self.faults.clone(),
                    bug,
                    self.losable && name != "control",
                );
                Arc::new(engine.recorded(self.fingerprint.clone()))
            })
            .clone()
    }

    /// The engines opened under `data_dir`, by name.
    pub fn of(&self, data_dir: &Path) -> Vec<Arc<SimEngine>> {
        let engines = self.engines.lock().unwrap();
        engines
            .iter()
            .filter(|((dir, _), _)| dir == data_dir)
            .map(|(_, engine)| engine.clone())
            .collect()
    }

    pub fn get(&self, data_dir: &Path, name: &str) -> Option<Arc<SimEngine>> {
        let engines = self.engines.lock().unwrap();
        engines
            .get(&(data_dir.to_owned(), name.to_owned()))
            .cloned()
    }
}

#[cfg(test)]
#[path = "../../../crates/lib/tests/unit/storage/namespace_memory.rs"]
mod tests;
