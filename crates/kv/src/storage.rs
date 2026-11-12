//! Customer-KV namespace ownership, owner reads and writes, and value caching (#57 / B-39, #58 / B-26).
//!
//! Each namespace has one owner: the first of its home cell's ready `kv` members by rendezvous hash of the namespace ID
//! ([`Owners::route`]). The owner holds the namespace's only writer handle, embedded here as an owner
//! [`Writer`], and serves reads from that same handle, so no reader polls S3 and a read sees every acknowledged write.
//! Handles open on first use, close under pressure or after [`IDLE`], and close when membership moves the namespace to
//! another member. Closing never touches S3 data. A node that disagrees about ownership for a moment is fenced by the
//! other's open and leaves the namespace alone for `FENCE_HOLD`. [`ValueCache`] holds values an owner returned, for at
//! most [`CACHE_TTL_SECONDS`], under one process-wide byte budget that no single namespace can take over.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::placement;
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::Position;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{CommitError, Write, Writer};
use bedrock_lib::storage::{StorageError, StorageResult};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prost::Message;
use tokio::time::Instant;

use crate::namespaces::{self, CACHE_TTL_SECONDS, Record};

/// The dataset customer namespaces are stored under (`kv/{id}/`).
pub const DATASET: &str = "kv";
/// A handle unused this long is closed; the next request reopens it.
pub const IDLE: Duration = Duration::from_secs(15 * 60);
/// A namespace may fill at most this fraction of the value cache, so one hot namespace cannot evict the rest.
const SHARE: u64 = 8;

/// A stored customer value: Cloudflare's value, metadata and absolute expiration together, so they change atomically.
#[derive(Clone, PartialEq, Message)]
pub struct Stored {
    #[prost(bytes = "vec", tag = "1")]
    pub value: Vec<u8>,
    /// The caller's metadata, as the JSON text it sent.
    #[prost(string, optional, tag = "2")]
    pub metadata: Option<String>,
    /// Seconds since the Unix epoch after which the key reads as absent; 0 never expires.
    #[prost(uint64, tag = "3")]
    pub expiration: u64,
}

impl Stored {
    /// Whether the key reads as absent at wall-clock `now_s`.
    pub fn expired(&self, now_s: u64) -> bool {
        self.expiration != 0 && self.expiration <= now_s
    }
}

/// One key's change in a write: a value to store, or `None` to delete it.
pub type Change = (Vec<u8>, Option<Stored>);

/// Where a namespace's requests go from this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// This node owns it.
    Local,
    /// Another member of this cell owns it; its address.
    Peer(String),
    /// Another cell is its home: (region, cell).
    Cell(String, String),
}

/// Why a read or write was refused before reaching storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerError {
    /// This node does not own the namespace; send the request where [`Owners::route`] says.
    NotOwner(Route),
    /// The namespace is being deleted: it has no data and takes no writes.
    Deleted,
    /// A write would exceed the namespace's storage quota.
    Quota,
    Commit(CommitError),
    Storage(StorageError),
}

impl From<StorageError> for OwnerError {
    fn from(error: StorageError) -> Self {
        OwnerError::Storage(error)
    }
}

impl From<CommitError> for OwnerError {
    fn from(error: CommitError) -> Self {
        OwnerError::Commit(error)
    }
}

/// This cell's members of service `kv`, as the node's membership view holds them.
pub type Members = Arc<dyn Fn() -> BTreeMap<String, pb::Member> + Send + Sync>;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabels {
    outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RouteLabels {
    route: &'static str,
}

/// The namespaces this node owns, and where the others' requests go.
pub struct Owners {
    writer: Arc<Writer>,
    node_id: String,
    region: String,
    cell: String,
    members: Members,
    routes: Family<RouteLabels, Counter>,
    writes: Family<OutcomeLabels, Counter>,
    released: Counter,
    commit_seconds: Histogram,
    pending_bytes: Gauge,
    open: Gauge,
}

impl Owners {
    pub fn new(
        admin: &Admin,
        writer: Arc<Writer>,
        node_id: String,
        region: String,
        cell: String,
        members: Members,
    ) -> Arc<Self> {
        let routes = Family::default();
        admin.register(
            "kv_routes",
            "Customer-KV requests by where they were sent: local, a peer in this cell, or another cell",
            routes.clone(),
        );
        let writes = Family::default();
        admin.register(
            "kv_writes",
            "Customer-KV write requests by outcome",
            writes.clone(),
        );
        let released = Counter::default();
        admin.register(
            "kv_owner_released",
            "Namespace handles closed because membership moved the namespace to another member",
            released.clone(),
        );
        let commit_seconds = Histogram::new([0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]);
        admin.register(
            "kv_commit_seconds",
            "Customer-KV write latency from admission to durable acknowledgement",
            commit_seconds.clone(),
        );
        let pending_bytes = Gauge::default();
        admin.register(
            "kv_pending_bytes",
            "Customer-KV bytes admitted and not yet durable",
            pending_bytes.clone(),
        );
        let open = Gauge::default();
        admin.register("kv_owner_open", "Open namespace handles", open.clone());
        let owners = Arc::new(Owners {
            writer,
            node_id,
            region,
            cell,
            members,
            routes,
            writes,
            released,
            commit_seconds,
            pending_bytes,
            open,
        });
        let weak = Arc::downgrade(&owners);
        admin.introspect(move |view, _| match (view, weak.upgrade()) {
            ("owners", Some(owners)) => Some(owners.introspect()),
            _ => None,
        });
        owners
    }

    /// Open handles: namespace and idle time. The durable head is in each read's answer.
    fn introspect(&self) -> String {
        let rows: Vec<_> = self
            .writer
            .open_namespaces()
            .into_iter()
            .map(|(_, namespace, idle)| {
                serde_json::json!({ "namespace": namespace, "idle_ms": idle.as_millis() as u64 })
            })
            .collect();
        serde_json::json!({ "owners": rows }).to_string()
    }

    /// The owner of namespace `id` among this cell's ready, healthy members, if any.
    fn owner(&self, id: &str) -> Option<pb::Member> {
        let members = (self.members)();
        let eligible = members
            .values()
            .filter(|m| m.status == pb::member::Status::Ready as i32 && m.unhealthy.is_empty());
        placement::rank(placement::hash_of(DATASET, id, b""), eligible)
            .first()
            .map(|m| (*m).clone())
    }

    /// Where `record`'s requests go. Only its home cell's owner serves them; with no ready member known in this cell,
    /// this node serves them, since it is a member itself.
    pub fn route(&self, record: &Record) -> Route {
        if record.home_region != self.region || record.home_cell != self.cell {
            return Route::Cell(record.home_region.clone(), record.home_cell.clone());
        }
        match self.owner(&record.id) {
            Some(owner) if owner.node_id != self.node_id => Route::Peer(owner.address),
            _ => Route::Local,
        }
    }

    /// Counts where a request was sent.
    pub fn count_route(&self, route: &Route) {
        let route = match route {
            Route::Local => "local",
            Route::Peer(_) => "peer",
            Route::Cell(..) => "cell",
        };
        self.routes.get_or_create(&RouteLabels { route }).inc();
    }

    /// Refuses `record` unless it is active and owned here. The caller passes a fresh directory read, so a deleted
    /// namespace is never served from a handle left open.
    fn admit(&self, record: &Record) -> Result<(), OwnerError> {
        if namespaces::admit_open(record).is_err() {
            return Err(OwnerError::Deleted);
        }
        match self.route(record) {
            Route::Local => Ok(()),
            route => Err(OwnerError::NotOwner(route)),
        }
    }

    /// `key`'s live value, or its proven absence, at a position at least `min_revision`. An acknowledged revision is
    /// durable on its owner before the answer, so a view behind `min_revision` is a node that lost the namespace: the
    /// read fails at once and the caller routes again.
    pub async fn get(
        &self,
        record: &Record,
        key: &[u8],
        min_revision: u64,
    ) -> Result<(Option<Stored>, Position), OwnerError> {
        self.admit(record)?;
        let reader = self.writer.reader(DATASET, &record.id).await?;
        let read = reader.get(key, min_revision, Duration::ZERO).await;
        let (raw, position) = self.checked(&record.id, read).await?;
        Ok((live(raw, &record.id)?, position))
    }

    /// Passes a failed read to the writer, so a fenced or broken handle is not used again.
    async fn checked<T>(&self, id: &str, read: StorageResult<T>) -> StorageResult<T> {
        if let Err(error) = &read {
            // A view behind the caller's revision is not a broken handle.
            let behind = matches!(error, StorageError::Unavailable(context) if context.engine == "namespace");
            if !behind {
                self.writer.report(DATASET, id, error).await;
            }
        }
        read
    }

    /// Up to `limit` live keys under `prefix` after `after`, as [`Owners::get`] reads one. Expired keys are left out, so
    /// a page may hold fewer than `limit` while more follow.
    pub async fn list(
        &self,
        record: &Record,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
        min_revision: u64,
    ) -> Result<(Vec<(Vec<u8>, Stored)>, Position), OwnerError> {
        self.admit(record)?;
        let reader = self.writer.reader(DATASET, &record.id).await?;
        let read = reader
            .list(prefix, after, limit, min_revision, Duration::ZERO)
            .await;
        let (entries, position) = self.checked(&record.id, read).await?;
        let mut page = Vec::with_capacity(entries.len());
        for (key, raw) in entries {
            if let Some(stored) = live(Some(raw), &record.id)? {
                page.push((key, stored));
            }
        }
        Ok((page, position))
    }

    /// Commits `changes` to `record` in order and in one atomic batch, after its quota admits them, and returns the
    /// revision of the last. With `idempotency`, a retry returns the original revision without writing again.
    pub async fn write(
        &self,
        record: &Record,
        changes: Vec<Change>,
        idempotency: Option<&str>,
    ) -> Result<u64, OwnerError> {
        let written = self.try_write(record, changes, idempotency).await;
        let outcome = match &written {
            Ok(_) => "committed",
            Err(OwnerError::NotOwner(_)) => "not_owner",
            Err(OwnerError::Deleted) => "deleted",
            Err(OwnerError::Quota) => "quota",
            Err(OwnerError::Commit(CommitError::Busy)) => "busy",
            Err(OwnerError::Commit(CommitError::Fenced)) => "fenced",
            Err(_) => "failed",
        };
        self.writes.get_or_create(&OutcomeLabels { outcome }).inc();
        written
    }

    async fn try_write(
        &self,
        record: &Record,
        changes: Vec<Change>,
        idempotency: Option<&str>,
    ) -> Result<u64, OwnerError> {
        self.admit(record)?;
        // ponytail: counts each stored value in full, ignoring what it replaces, so an overwrite at the limit is refused;
        // concurrent requests can overshoot by one batch. Exact accounting needs the old sizes before admission.
        let added: u64 = changes
            .iter()
            .filter_map(|(key, stored)| {
                stored
                    .as_ref()
                    .map(|s| (key.len() + s.encoded_len()) as u64)
            })
            .sum();
        if record.storage_bytes.is_some() {
            let current = self.writer.live_bytes(DATASET, &record.id).await?;
            if namespaces::admit_write(record, current, added).is_err() {
                return Err(OwnerError::Quota);
            }
        }
        let single = changes.len() == 1;
        let writes = changes
            .into_iter()
            .enumerate()
            .map(|(index, (key, stored))| Write {
                dataset: DATASET.into(),
                namespace: record.id.clone(),
                key,
                operation: match stored {
                    Some(stored) => Operation::Set(stored.encode_to_vec()),
                    None => Operation::Delete,
                },
                idempotency_key: idempotency.map(|key| {
                    if single {
                        key.to_owned()
                    } else {
                        format!("{key}#{index}")
                    }
                }),
                expected: None,
            })
            .collect();
        let started = bedrock_lib::monotonic_now();
        self.pending_bytes.inc_by(added as i64);
        let committed = self.writer.commit_all(writes).await;
        self.pending_bytes.dec_by(added as i64);
        self.commit_seconds.observe(
            bedrock_lib::monotonic_now()
                .duration_since(started)
                .as_secs_f64(),
        );
        let revisions = committed?;
        Ok(*revisions.last().expect("a write has at least one change"))
    }

    /// Closes the handles of namespaces membership has moved to another member, so this node stops serving them.
    /// The caller runs it whenever its membership view advances.
    pub async fn release_moved(&self) {
        for (_, id, _) in self.writer.open_namespaces() {
            let moved = self
                .owner(&id)
                .is_some_and(|owner| owner.node_id != self.node_id);
            if moved {
                self.writer.release(DATASET, &id).await;
                self.released.inc();
            }
        }
        self.open.set(self.writer.open_namespaces().len() as i64);
    }

    /// Closes handles idle for [`IDLE`]. The caller runs this on a process-wide timer; it costs nothing per namespace.
    pub async fn sweep(&self) {
        self.writer.sweep(IDLE).await;
        self.open.set(self.writer.open_namespaces().len() as i64);
    }

    /// Deletes every stored object of namespace `id`, after closing this node's handle and fencing any other writer, so
    /// nothing still running writes back under the deleted prefix.
    pub async fn destroy(&self, id: &str) -> StorageResult<()> {
        self.writer.destroy(DATASET, id).await
    }
}

/// A stored value as the caller sees it: absent once expired, and corruption if it does not decode.
fn live(raw: Option<Vec<u8>>, id: &str) -> StorageResult<Option<Stored>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let stored = Stored::decode(raw.as_slice()).map_err(|_| {
        StorageError::Corruption(bedrock_lib::storage::ErrorContext {
            engine: "kv",
            instance: format!("{DATASET}/{id}"),
            file: None,
            partition: None,
            detail: "stored value is not a valid envelope".into(),
        })
    })?;
    Ok((!stored.expired(namespaces::wall_ms() / 1000)).then_some(stored))
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

/// Values an owner returned, by namespace and key, under one byte budget for the process.
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
        if cached.expires <= bedrock_lib::monotonic_now() {
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
            let now = bedrock_lib::monotonic_now();
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
        let expires = bedrock_lib::monotonic_now() + Duration::from_secs(CACHE_TTL_SECONDS.into());
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

/// How many namespace handles a node with `memory_mb` of budget keeps open. A quarter of the budget is the shared block
/// cache and a quarter is headroom for the value cache and the process; the other half holds handles at their worst
/// case, 16 MiB of unflushed memtable each (the writer's L0 settings in `SlateFactory`).
pub fn max_open(memory_mb: u64) -> usize {
    // ponytail: sized for every reader full at once; a measured bound once B-53's benchmarks exist.
    (memory_mb / 2 / 16).max(1) as usize
}

#[cfg(test)]
#[path = "../tests/unit/storage.rs"]
mod tests;
