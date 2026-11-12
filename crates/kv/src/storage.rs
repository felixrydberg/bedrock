//! Customer-KV reader residency and value caching (#57 / B-39).
//!
//! [`Readers`] keeps one read-only reader per active namespace whose home region is this node's, opened on demand and
//! closed under handle pressure or after [`IDLE`] unused. Closing a reader never touches S3 data. [`ValueCache`] holds
//! values a home reader returned, for at most [`CACHE_TTL_SECONDS`], under one process-wide byte budget that no single
//! namespace can take over. Neither keeps anything for a dormant namespace, and neither polls one.
//!
//! Which node holds a namespace's reader, and which serves a key from its cache, is routing (B-27).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection::{self, NamespaceOpens};
use bedrock_lib::replication::materialized::{Position, Reader};
use bedrock_lib::storage::namespace::NamespaceFactory;
use bedrock_lib::storage::{Entry, ErrorContext, StorageError, StorageResult};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use tokio::time::Instant;

use crate::namespaces::{self, CACHE_TTL_SECONDS, Record};

/// The dataset customer namespaces are stored under (`kv/{id}/`).
pub const DATASET: &str = "kv";
/// A reader unused this long is closed; the next read reopens it.
pub const IDLE: Duration = Duration::from_secs(15 * 60);
/// How long a minimum-revision read waits for the reader to see a revision its writer already acknowledged.
pub const MIN_REVISION_WAIT: Duration = Duration::from_secs(2);
/// A namespace may fill at most this fraction of the value cache, so one hot namespace cannot evict the rest.
const SHARE: u64 = 8;

/// Why a read was refused before reaching storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// Only the namespace's home region opens its database; other regions ask the home.
    NotHome,
    /// The namespace is being deleted: it has no data to read.
    Deleted,
    Storage(StorageError),
}

impl From<StorageError> for ReadError {
    fn from(error: StorageError) -> Self {
        ReadError::Storage(error)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabels {
    outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ReasonLabels {
    reason: &'static str,
}

struct Open {
    reader: Arc<Reader>,
    used: Instant,
    /// What the last read through this reader saw, for introspection.
    position: Position,
}

/// One logical reader per active home namespace, bounded in number.
pub struct Readers {
    factory: Arc<dyn NamespaceFactory>,
    opens: NamespaceOpens,
    region: String,
    cell: String,
    max_open: usize,
    open: Mutex<BTreeMap<String, Open>>,
    /// Readers taken out of service, by namespace; each closes once no read still holds it.
    retired: Mutex<Vec<(String, Arc<Reader>, &'static str)>>,
    opened: Family<OutcomeLabels, Counter>,
    closed: Family<ReasonLabels, Counter>,
    waits: Family<OutcomeLabels, Counter>,
    gauge: Gauge,
}

fn storage_error(id: &str, detail: &str) -> StorageError {
    StorageError::Unavailable(ErrorContext {
        engine: "kv",
        instance: format!("{DATASET}/{id}"),
        file: None,
        partition: None,
        detail: detail.into(),
    })
}

impl Readers {
    pub fn new(
        admin: &Admin,
        factory: Arc<dyn NamespaceFactory>,
        region: String,
        cell: String,
        max_open: usize,
    ) -> Arc<Self> {
        let opened = Family::default();
        admin.register(
            "kv_reader_opens",
            "Namespace reader opens by outcome",
            opened.clone(),
        );
        let closed = Family::default();
        admin.register(
            "kv_reader_closes",
            "Namespace reader closes by reason",
            closed.clone(),
        );
        let waits = Family::default();
        admin.register(
            "kv_reader_min_revision",
            "Minimum-revision reads: met at once, met after waiting for publication, or still behind",
            waits.clone(),
        );
        let gauge = Gauge::default();
        admin.register("kv_readers_open", "Open namespace readers", gauge.clone());
        let readers = Arc::new(Readers {
            opens: NamespaceOpens::new(factory.clone(), &protection::Metrics::new(admin)),
            factory,
            region,
            cell,
            max_open,
            open: Mutex::default(),
            retired: Mutex::default(),
            opened,
            closed,
            waits,
            gauge,
        });
        let weak = Arc::downgrade(&readers);
        admin.introspect(move |view, _| match (view, weak.upgrade()) {
            ("readers", Some(readers)) => Some(readers.introspect()),
            _ => None,
        });
        readers
    }

    /// Open readers with what they last saw: namespace, revision, its commit time and idle time.
    fn introspect(&self) -> String {
        let now = Instant::now();
        let open = self.open.lock().unwrap();
        let rows: Vec<_> = open
            .iter()
            .map(|(id, open)| {
                serde_json::json!({
                    "namespace": id,
                    "revision": open.position.sequence,
                    "committed_at": open.position.committed_at,
                    "idle_ms": now.duration_since(open.used).as_millis() as u64,
                })
            })
            .collect();
        serde_json::json!({ "readers": rows }).to_string()
    }

    fn admit(&self, record: &Record) -> Result<(), ReadError> {
        if namespaces::admit_open(record).is_err() {
            return Err(ReadError::Deleted);
        }
        if record.home_region != self.region {
            return Err(ReadError::NotHome);
        }
        Ok(())
    }

    /// The namespace's reader, opening it if needed; `None` while no writer has created its database.
    async fn reader(&self, id: &str) -> StorageResult<Option<Arc<Reader>>> {
        if let Some(open) = self.open.lock().unwrap().get_mut(id) {
            open.used = Instant::now();
            return Ok(Some(open.reader.clone()));
        }
        let opened = self.opens.open(&self.cell, DATASET, id).await;
        let outcome = match &opened {
            Ok(Some(_)) => "opened",
            Ok(None) => "not_created",
            Err(StorageError::Overloaded(_)) => "overloaded",
            Err(_) => "error",
        };
        self.opened.get_or_create(&OutcomeLabels { outcome }).inc();
        let Some(reader) = opened? else {
            // Not kept: the database may be created any moment, and an empty view would never see it.
            return Ok(None);
        };
        let (reader, evicted) = self.install(id, reader);
        // Pressure closes what it evicted now, not at the next sweep. A reader a read still holds closes once released,
        // so beyond `max_open` there are at most as many readers as reads in flight.
        if evicted {
            self.release().await;
        }
        Ok(Some(reader))
    }

    /// Keeps `reader` open for `id` unless another already is, evicting the least recently used readers beyond
    /// `max_open`. Returns the reader to use and whether any was evicted.
    fn install(&self, id: &str, reader: Arc<Reader>) -> (Arc<Reader>, bool) {
        let mut open = self.open.lock().unwrap();
        // A coalesced open hands every caller the same reader; the first to finish installs it.
        let installed = open.entry(id.to_owned()).or_insert_with(|| Open {
            reader: reader.clone(),
            used: Instant::now(),
            position: Position::default(),
        });
        installed.used = Instant::now();
        let installed = installed.reader.clone();
        // An open that started after another finished loses the install; every caller it served retires it once.
        if !Arc::ptr_eq(&installed, &reader) {
            let mut retired = self.retired.lock().unwrap();
            if !retired
                .iter()
                .any(|(_, held, _)| Arc::ptr_eq(held, &reader))
            {
                retired.push((id.to_owned(), reader, "duplicate"));
            }
        }
        let reader = installed;
        let mut evicted = false;
        // ponytail: least recently used is found by a scan over at most `max_open` readers; an ordered index if that
        // grows large.
        while open.len() > self.max_open {
            evicted = true;
            let oldest = open
                .iter()
                .min_by_key(|(_, open)| open.used)
                .map(|(id, _)| id.clone())
                .expect("over the bound means not empty");
            let evicted = open.remove(&oldest).expect("just found");
            self.retired
                .lock()
                .unwrap()
                .push((oldest, evicted.reader, "pressure"));
        }
        self.gauge.set(open.len() as i64);
        (reader, evicted)
    }

    /// Takes a reader that failed out of service, so the next read opens a fresh one.
    fn discard(&self, id: &str, reader: &Arc<Reader>, error: &StorageError) {
        let broken = matches!(
            error,
            StorageError::Corruption(_) | StorageError::IoError(_) | StorageError::Fenced(_)
        );
        if !broken {
            return;
        }
        let mut open = self.open.lock().unwrap();
        if open
            .get(id)
            .is_some_and(|open| Arc::ptr_eq(&open.reader, reader))
        {
            let removed = open.remove(id).expect("just checked");
            self.retired
                .lock()
                .unwrap()
                .push((id.to_owned(), removed.reader, "error"));
            self.gauge.set(open.len() as i64);
        }
    }

    fn record(&self, id: &str, reader: &Arc<Reader>, position: Position) {
        if let Some(open) = self.open.lock().unwrap().get_mut(id)
            && Arc::ptr_eq(&open.reader, reader)
            && position.sequence >= open.position.sequence
        {
            open.position = position;
        }
    }

    /// Whether the reader's view is behind `min_revision`, so the read waits for publication.
    async fn behind(&self, reader: &Reader, min_revision: u64) -> bool {
        min_revision > 0
            && !reader
                .position()
                .await
                .is_ok_and(|position| position.sequence >= min_revision)
    }

    /// Counts a minimum-revision read and takes a broken reader out of service.
    fn finish<T>(
        &self,
        id: &str,
        reader: &Arc<Reader>,
        min_revision: u64,
        behind: bool,
        result: StorageResult<(T, Position)>,
    ) -> Result<(T, Position), ReadError> {
        if min_revision > 0 {
            let outcome = match (&result, behind) {
                (Ok(_), false) => "met",
                (Ok(_), true) => "waited",
                (Err(_), true) => "behind",
                (Err(_), false) => "failed",
            };
            self.waits.get_or_create(&OutcomeLabels { outcome }).inc();
        }
        match result {
            Ok((found, position)) => {
                self.record(id, reader, position);
                Ok((found, position))
            }
            Err(error) => {
                self.discard(id, reader, &error);
                Err(ReadError::Storage(error))
            }
        }
    }

    /// `key`'s value or proven absence in the home namespace `record`, at a position at least `min_revision`. The
    /// caller passes a fresh directory read, so a deleted namespace is never served from a reader left open.
    pub async fn get(
        &self,
        record: &Record,
        key: &[u8],
        min_revision: u64,
    ) -> Result<(Option<Vec<u8>>, Position), ReadError> {
        self.admit(record)?;
        let id = &record.id;
        let Some(reader) = self.reader(id).await? else {
            return absent(id, min_revision).map(|position| (None, position));
        };
        let behind = self.behind(&reader, min_revision).await;
        let read = reader.get(key, min_revision, MIN_REVISION_WAIT).await;
        self.finish(id, &reader, min_revision, behind, read)
    }

    /// Up to `limit` keys under `prefix` after `after`, as [`Readers::get`] reads one.
    pub async fn list(
        &self,
        record: &Record,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
        min_revision: u64,
    ) -> Result<(Vec<Entry>, Position), ReadError> {
        self.admit(record)?;
        let id = &record.id;
        let Some(reader) = self.reader(id).await? else {
            return absent(id, min_revision).map(|position| (Vec::new(), position));
        };
        let behind = self.behind(&reader, min_revision).await;
        let read = reader
            .list(prefix, after, limit, min_revision, MIN_REVISION_WAIT)
            .await;
        self.finish(id, &reader, min_revision, behind, read)
    }

    /// Retires readers idle for [`IDLE`] and closes every retired reader no read still holds. The caller runs this on
    /// a process-wide timer; it costs nothing per namespace.
    pub async fn sweep(&self) {
        let now = Instant::now();
        {
            let mut open = self.open.lock().unwrap();
            let idle: Vec<String> = open
                .iter()
                .filter(|(_, open)| now.duration_since(open.used) >= IDLE)
                .map(|(id, _)| id.clone())
                .collect();
            let mut retired = self.retired.lock().unwrap();
            for id in idle {
                let removed = open.remove(&id).expect("just listed");
                retired.push((id, removed.reader, "idle"));
            }
            self.gauge.set(open.len() as i64);
        }
        self.release().await;
    }

    /// Closes every retired reader no read still holds.
    async fn release(&self) {
        let closable: Vec<_> = {
            let mut retired = self.retired.lock().unwrap();
            let (closable, held) = std::mem::take(&mut *retired)
                .into_iter()
                .partition(|(_, reader, _)| Arc::strong_count(reader) == 1);
            *retired = held;
            closable
        };
        for (_, reader, reason) in closable {
            self.stop(&reader, reason).await;
        }
    }

    async fn stop(&self, reader: &Reader, reason: &'static str) {
        if let Err(error) = reader.close().await {
            tracing::debug!(%error, "closing a namespace reader failed");
        }
        self.closed.get_or_create(&ReasonLabels { reason }).inc();
    }

    /// Deletes every stored object of namespace `id`, after closing each of its local readers and fencing its writer, so
    /// nothing still running writes a checkpoint or publication back under the deleted prefix. Readers are closed at
    /// once, not when idle: the namespace is deleted, so a read still holding one fails rather than being waited for.
    pub async fn destroy(&self, id: &str) -> StorageResult<()> {
        let mut stopping = Vec::new();
        {
            let mut open = self.open.lock().unwrap();
            stopping.extend(open.remove(id).map(|open| open.reader));
            self.gauge.set(open.len() as i64);
            let mut retired = self.retired.lock().unwrap();
            let (theirs, others) = std::mem::take(&mut *retired)
                .into_iter()
                .partition(|(namespace, _, _)| namespace == id);
            *retired = others;
            stopping.extend(theirs.into_iter().map(|(_, reader, _)| reader));
        }
        for reader in stopping {
            self.stop(&reader, "purge").await;
        }
        // Opening a writer fences any `bedrock-api` writer still open on the namespace. Refusing it a new one is write
        // admission's (B-26): a `Deleting` namespace takes no writes.
        self.factory.open(DATASET, id).await?.close().await?;
        self.factory.destroy(DATASET, id).await
    }
}

/// A namespace no write has reached holds nothing, at revision 0; a later revision has not been published yet.
fn absent(id: &str, min_revision: u64) -> Result<Position, ReadError> {
    if min_revision > 0 {
        return Err(ReadError::Storage(storage_error(
            id,
            "namespace database not created yet",
        )));
    }
    Ok(Position::default())
}

struct Cached {
    value: Vec<u8>,
    revision: u64,
    expires: Instant,
}

impl Cached {
    fn bytes(key: &(String, Vec<u8>), value: &[u8]) -> u64 {
        (key.0.len() + key.1.len() + value.len()) as u64
    }
}

/// Values a home reader returned, by namespace and key, under one byte budget for the process.
pub struct ValueCache {
    budget: u64,
    inner: Mutex<CacheState>,
    bytes: Gauge,
    skipped: Counter,
}

struct CacheState {
    entries: lru::LruCache<(String, Vec<u8>), Cached>,
    total: u64,
    by_namespace: BTreeMap<String, u64>,
}

impl CacheState {
    fn remove(&mut self, key: &(String, Vec<u8>)) {
        if let Some(cached) = self.entries.pop(key) {
            self.forget(key, &cached);
        }
    }

    fn forget(&mut self, key: &(String, Vec<u8>), cached: &Cached) {
        let bytes = Cached::bytes(key, &cached.value);
        self.total -= bytes;
        let namespace = self
            .by_namespace
            .get_mut(&key.0)
            .expect("cached namespaces are counted");
        *namespace -= bytes;
        if *namespace == 0 {
            self.by_namespace.remove(&key.0);
        }
    }
}

impl ValueCache {
    pub fn new(admin: &Admin, budget: u64) -> Self {
        let bytes = Gauge::default();
        admin.register(
            "kv_value_cache_bytes",
            "Bytes in the value cache",
            bytes.clone(),
        );
        let skipped = Counter::default();
        admin.register(
            "kv_value_cache_skipped",
            "Values not cached because their namespace had used its share",
            skipped.clone(),
        );
        ValueCache {
            budget,
            inner: Mutex::new(CacheState {
                entries: lru::LruCache::unbounded(),
                total: 0,
                by_namespace: BTreeMap::new(),
            }),
            bytes,
            skipped,
        }
    }

    /// The cached value of `key` in `namespace` and its revision, if one is cached, unexpired and at least at
    /// `min_revision`. Absences are never cached, so a miss proves nothing.
    pub fn get(&self, namespace: &str, key: &[u8], min_revision: u64) -> Option<(Vec<u8>, u64)> {
        let mut state = self.inner.lock().unwrap();
        let id = (namespace.to_owned(), key.to_vec());
        let cached = state.entries.get(&id)?;
        if cached.expires <= Instant::now() {
            state.remove(&id);
            self.bytes.set(state.total as i64);
            return None;
        }
        (cached.revision >= min_revision).then(|| (cached.value.clone(), cached.revision))
    }

    /// Keeps `value` for [`CACHE_TTL_SECONDS`]. A value from an older revision than the one cached is ignored. A
    /// namespace over its share caches nothing more until its entries leave, so it never evicts other namespaces.
    pub fn fill(&self, namespace: &str, key: &[u8], value: &[u8], revision: u64) {
        let id = (namespace.to_owned(), key.to_vec());
        let size = Cached::bytes(&id, value);
        let share = self.budget / SHARE;
        let mut state = self.inner.lock().unwrap();
        if state
            .entries
            .peek(&id)
            .is_some_and(|cached| cached.revision > revision)
        {
            return;
        }
        state.remove(&id);
        let over = |state: &CacheState| {
            state.by_namespace.get(namespace).copied().unwrap_or(0) + size > share
        };
        if over(&state) {
            // Expired entries leave only when read or under budget pressure, so reclaim this namespace's before refusing.
            // ponytail: a scan of the whole cache, only for a namespace at its share; a per-namespace index if it shows.
            let now = Instant::now();
            let expired: Vec<_> = state
                .entries
                .iter()
                .filter(|(key, cached)| key.0 == namespace && cached.expires <= now)
                .map(|(key, _)| key.clone())
                .collect();
            for key in expired {
                state.remove(&key);
            }
        }
        if over(&state) {
            self.skipped.inc();
            self.bytes.set(state.total as i64);
            return;
        }
        let expires = Instant::now() + Duration::from_secs(CACHE_TTL_SECONDS.into());
        state.total += size;
        *state.by_namespace.entry(namespace.to_owned()).or_default() += size;
        state.entries.put(
            id,
            Cached {
                value: value.to_vec(),
                revision,
                expires,
            },
        );
        while state.total > self.budget {
            let (key, cached) = state
                .entries
                .pop_lru()
                .expect("over budget means not empty");
            state.forget(&key, &cached);
        }
        self.bytes.set(state.total as i64);
    }
}

/// How many readers a node with `memory_mb` of budget keeps open.
pub fn max_open(memory_mb: u64) -> usize {
    // ponytail: one reader per 8 MiB of the node's memory budget (a reader's WAL memtable is bounded at 16 MiB but
    // rarely full); a measured bound once B-53's benchmarks exist.
    (memory_mb / 8).max(1) as usize
}

#[cfg(test)]
#[path = "../tests/unit/storage.rs"]
mod tests;
