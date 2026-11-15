//! Range databases: one SlateDB per `volume` range per cell (#113 / S-05), or an in-memory model in simulation builds.
//!
//! The writer runs with SlateDB's WAL off: the slots' quorum log already is the write-ahead log, so [`Writer::commit`]
//! writes one batch to the memtable and flushes it to an L0 file and a manifest update. That is about two S3 writes per
//! commit, whatever the number of actors in the batch. Opening a writer fences the previous one.
//!
//! Each cell's [`Catalog`] names the database serving each span. Split and merge build a new database by clone, sharing
//! the sources' files, and commit it by replacing the catalog with a conditional PUT: of concurrent clones, one wins
//! and the others are never written.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bedrock_lib::storage::namespace::Changes;
use bedrock_lib::storage::{Entry, StorageResult};

/// A read view: the writer's own, or a read-only reader that never fences it.
#[async_trait]
pub trait Database: Send + Sync {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    /// Up to `limit` entries of `[start, end)`, in key order.
    async fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> StorageResult<Vec<Entry>>;
    /// Closes the handle. Closing never deletes database data.
    async fn close(&self) -> StorageResult<()>;
}

#[async_trait]
pub trait Writer: Database {
    /// Writes `changes` as one atomic batch and returns once it is durable in S3. On an error, the batch may or may not
    /// be durable; the caller writes it again.
    async fn commit(&self, changes: Changes) -> StorageResult<()>;
    /// Bytes in the database's files, from the manifest the writer holds: no S3 request.
    fn size(&self) -> u64;
}

/// Where range databases live.
#[async_trait]
pub trait Databases: Send + Sync {
    /// Opens `name` for writing, creating it if absent, and fences any other writer.
    async fn open(&self, name: &str) -> StorageResult<Arc<dyn Writer>>;
    /// A read-only handle, or `None` while no writer has created `name`.
    async fn open_reader(&self, name: &str) -> StorageResult<Option<Arc<dyn Database>>>;
    /// `cell`'s catalog (empty when none is stored) and the version [`Databases::put_catalog`] must match: one GET.
    async fn catalog(&self, cell: &str) -> StorageResult<(Catalog, Option<String>)>;
    /// Stores `catalog` if the stored one is still at `version` (`None`: none stored); `Ok(false)` when it moved.
    async fn put_catalog(
        &self,
        cell: &str,
        catalog: &Catalog,
        version: Option<String>,
    ) -> StorageResult<bool>;
    /// Creates `name` from `sources`, each a database and the hash span `[start, end)` of it to take, sharing their
    /// files. Spans must not overlap. Repeating the same clone is a no-op.
    async fn create_clone(&self, name: &str, sources: &[(String, u64, u64)]) -> StorageResult<()>;
    /// Cells that have range databases: one LIST.
    async fn cells(&self) -> StorageResult<Vec<String>>;
}

/// A range database: the span it was created for (`end` 0 meaning 2⁶⁴) and its generation, unique in its cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Generation {
    pub start: u64,
    pub end: u64,
    pub generation: u64,
}

/// A cell's range databases: which one serves each span, and the next generation. Spans never overlap. A span may be
/// narrower than its database's: after a split, the half not yet cloned is still read from the old database, which no
/// one writes any more.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    pub next: u64,
    /// By span start: the span's end (0 meaning 2⁶⁴) and the database serving it.
    pub spans: BTreeMap<u64, (u64, Generation)>,
}

impl Catalog {
    /// One line with `next`, then one per span: `start end database-start database-end generation`.
    pub fn encode(&self) -> Vec<u8> {
        let mut text = format!("next {}\n", self.next);
        for (start, (end, db)) in &self.spans {
            text.push_str(&format!(
                "{start:016x} {end:016x} {:016x} {:016x} {}\n",
                db.start, db.end, db.generation
            ));
        }
        text.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        let next = lines.next()?.strip_prefix("next ")?.parse().ok()?;
        let mut spans = BTreeMap::new();
        for line in lines {
            let words: Vec<&str> = line.split(' ').collect();
            let [start, end, db_start, db_end, generation] = words[..] else {
                return None;
            };
            let hex = |word: &str| u64::from_str_radix(word, 16).ok();
            let db = Generation {
                start: hex(db_start)?,
                end: hex(db_end)?,
                generation: generation.parse().ok()?,
            };
            spans.insert(hex(start)?, (hex(end)?, db));
        }
        Some(Catalog { next, spans })
    }
}

/// The name of `cell`'s range database `db`: `{cell}/{start:016x}-{end:016x}/{generation}`.
pub fn database(cell: &str, db: &Generation) -> String {
    format!("{cell}/{:016x}-{:016x}/{}", db.start, db.end, db.generation)
}

/// The keys of hash span `[start, end)` (`end` 0 meaning 2⁶⁴): range database keys start with the actor's hash.
pub fn keys(start: u64, end: u64) -> (Vec<u8>, Option<Vec<u8>>) {
    (
        start.to_be_bytes().to_vec(),
        (end != 0).then(|| end.to_be_bytes().to_vec()),
    )
}

/// A key below every hash, so no clone's projection takes it: present once a clone took this database as a source.
/// A writer that finds it writes nothing, since the clone may not see what it would write.
pub const RETIRED: &[u8] = &[0];

#[cfg(not(madsim))]
pub use slate::{Slate, ssd_cache};

#[cfg(not(madsim))]
mod slate {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bedrock_lib::storage::namespace::Changes;
    use bedrock_lib::storage::{Entry, ErrorContext, StorageError, StorageResult};
    use std::ops::Bound;

    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};
    use slatedb::admin::Admin;
    use slatedb::bytes::Bytes;
    use slatedb::config::{DbReaderOptions, WriteOptions};
    use slatedb::db_cache::DbCache;
    use slatedb::{
        CloneSourceSpec, CloseReason, Db, DbReader, DbReaderMode, ErrorKind, Settings, WriteBatch,
    };

    use super::{Catalog, Database, Databases, Writer, keys};

    /// How often a reader looks for a newer manifest: one GET per poll and open reader.
    pub const READER_POLL: Duration = Duration::from_secs(1);

    /// Range databases under `volume/` in one bucket. Readers share one block cache (the SSD cache, B-30).
    pub struct Slate {
        store: Arc<dyn ObjectStore>,
        cache: Option<Arc<dyn DbCache>>,
    }

    impl Slate {
        pub fn new(store: Arc<dyn ObjectStore>, cache: Option<Arc<dyn DbCache>>) -> Self {
            Slate { store, cache }
        }
    }

    fn path(name: &str) -> Path {
        Path::from(format!("volume/{name}"))
    }

    fn catalog_path(cell: &str) -> Path {
        Path::from(format!("volume/{cell}/catalog"))
    }

    fn store_error(name: &str, operation: &str, error: object_store::Error) -> StorageError {
        StorageError::Unavailable(ErrorContext {
            engine: "object_store",
            instance: format!("volume/{name}"),
            file: None,
            partition: None,
            detail: format!("{operation}: {error}"),
        })
    }

    fn error(name: &str, operation: &str, error: slatedb::Error) -> StorageError {
        let kind = error.kind();
        let context = ErrorContext {
            engine: "slatedb",
            instance: format!("volume/{name}"),
            file: None,
            partition: None,
            detail: format!("{operation}: {kind:?}"),
        };
        match kind {
            ErrorKind::Closed(CloseReason::Fenced) => StorageError::Fenced(context),
            ErrorKind::Data => StorageError::Corruption(context),
            ErrorKind::Closed(_) | ErrorKind::Unavailable => StorageError::Unavailable(context),
            _ => StorageError::IoError(context),
        }
    }

    async fn collect(
        name: &str,
        mut iter: slatedb::DbIterator,
        limit: usize,
    ) -> StorageResult<Vec<Entry>> {
        let mut entries = Vec::new();
        while entries.len() < limit
            && let Some(entry) = iter.next().await.map_err(|e| error(name, "scan", e))?
        {
            entries.push((entry.key.to_vec(), entry.value.to_vec()));
        }
        Ok(entries)
    }

    struct SlateWriter {
        db: Db,
        name: String,
    }

    struct SlateReader {
        reader: DbReader,
        name: String,
    }

    #[async_trait]
    impl Database for SlateWriter {
        async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            let value = self.db.get(key).await;
            Ok(value
                .map_err(|e| error(&self.name, "get", e))?
                .map(|v| v.to_vec()))
        }

        async fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> StorageResult<Vec<Entry>> {
            let iter = self.db.scan(start.to_vec()..end.to_vec()).await;
            collect(
                &self.name,
                iter.map_err(|e| error(&self.name, "scan", e))?,
                limit,
            )
            .await
        }

        async fn close(&self) -> StorageResult<()> {
            self.db
                .close()
                .await
                .map_err(|e| error(&self.name, "close", e))
        }
    }

    #[async_trait]
    impl Writer for SlateWriter {
        async fn commit(&self, changes: Changes) -> StorageResult<()> {
            if changes.is_empty() {
                return Ok(());
            }
            let mut batch = WriteBatch::new();
            for (key, value) in changes {
                match value {
                    Some(value) => batch.put(key, value),
                    None => batch.delete(key),
                }
            }
            // With the WAL off, a write lands in the memtable and `flush` makes it durable in one L0 file.
            self.db
                .write_with_options(batch, &WriteOptions::default())
                .await
                .map_err(|e| error(&self.name, "write", e))?;
            self.db
                .flush()
                .await
                .map_err(|e| error(&self.name, "flush", e))
        }

        fn size(&self) -> u64 {
            let manifest = self.db.manifest();
            let l0: u64 = manifest.l0().iter().map(|sst| sst.estimate_size()).sum();
            let runs: u64 = manifest
                .compacted()
                .iter()
                .map(|run| run.estimate_size())
                .sum();
            l0 + runs
        }
    }

    #[async_trait]
    impl Database for SlateReader {
        async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            let value = self.reader.get(key).await;
            Ok(value
                .map_err(|e| error(&self.name, "reader get", e))?
                .map(|v| v.to_vec()))
        }

        async fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> StorageResult<Vec<Entry>> {
            let iter = self.reader.scan(start.to_vec()..end.to_vec()).await;
            collect(
                &self.name,
                iter.map_err(|e| error(&self.name, "reader scan", e))?,
                limit,
            )
            .await
        }

        async fn close(&self) -> StorageResult<()> {
            self.reader
                .close()
                .await
                .map_err(|e| error(&self.name, "reader close", e))
        }
    }

    #[async_trait]
    impl Databases for Slate {
        async fn open(&self, name: &str) -> StorageResult<Arc<dyn Writer>> {
            let settings = Settings {
                wal_enabled: false,
                // Memtables flush on the maintainer's timer, not on a WAL timer.
                flush_interval: None,
                // The object store client retries already.
                object_store_max_retries: Some(0),
                ..Settings::default()
            };
            let db = Db::builder(path(name), self.store.clone())
                .with_settings(settings)
                .build()
                .await
                .map_err(|e| error(name, "open", e))?;
            Ok(Arc::new(SlateWriter {
                db,
                name: name.to_owned(),
            }))
        }

        async fn open_reader(&self, name: &str) -> StorageResult<Option<Arc<dyn Database>>> {
            // A writer's open writes the first manifest, so no manifest means no database yet.
            let manifests = path(name).join("manifest");
            {
                use tokio_stream::StreamExt;
                if self.store.list(Some(&manifests)).next().await.is_none() {
                    return Ok(None);
                }
            }
            let options = DbReaderOptions {
                manifest_poll_interval: READER_POLL,
                object_store_max_retries: Some(0),
                ..DbReaderOptions::default()
            };
            let mut builder = DbReader::builder(path(name), self.store.clone())
                .with_reader_mode(DbReaderMode::ManagedCheckpoint)
                .with_options(options);
            if let Some(cache) = &self.cache {
                builder = builder.with_db_cache(cache.clone(), scope(name));
            }
            let reader = builder
                .build()
                .await
                .map_err(|e| error(name, "open reader", e))?;
            Ok(Some(Arc::new(SlateReader {
                reader,
                name: name.to_owned(),
            })))
        }

        async fn catalog(&self, cell: &str) -> StorageResult<(Catalog, Option<String>)> {
            self.get_catalog(cell).await
        }

        async fn put_catalog(
            &self,
            cell: &str,
            catalog: &Catalog,
            version: Option<String>,
        ) -> StorageResult<bool> {
            let mode = match version {
                Some(version) => PutMode::Update(decode_version(&version)),
                None => PutMode::Create,
            };
            match self
                .store
                .put_opts(&catalog_path(cell), catalog.encode().into(), mode.into())
                .await
            {
                Ok(_) => Ok(true),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
                Err(e) => Err(store_error(cell, "put catalog", e)),
            }
        }

        async fn create_clone(
            &self,
            name: &str,
            sources: &[(String, u64, u64)],
        ) -> StorageResult<()> {
            let spec = |(source, start, end): &(String, u64, u64)| {
                CloneSourceSpec::new(path(source)).with_projection_range(projection(*start, *end))
            };
            let mut sources = sources.iter();
            let first = sources.next().expect("a clone has a source");
            let admin = Admin::builder(path(name), self.store.clone()).build();
            let mut builder = admin.create_clone_builder_from_source(spec(first));
            for source in sources {
                builder = builder.with_source(spec(source));
            }
            builder.build().await.map_err(|e| error(name, "clone", e))
        }

        async fn cells(&self) -> StorageResult<Vec<String>> {
            let listed = self
                .store
                .list_with_delimiter(Some(&Path::from("volume")))
                .await
                .map_err(|e| store_error("", "list cells", e))?;
            Ok(listed
                .common_prefixes
                .iter()
                .filter_map(|p| p.filename().map(str::to_owned))
                .collect())
        }
    }

    impl Slate {
        async fn get_catalog(&self, cell: &str) -> StorageResult<(Catalog, Option<String>)> {
            let at = catalog_path(cell);
            let result = match self.store.get(&at).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok((Catalog::default(), None)),
                Err(e) => return Err(store_error(cell, "get catalog", e)),
            };
            let version = encode_version(result.meta.e_tag.clone(), result.meta.version.clone());
            let bytes = result
                .bytes()
                .await
                .map_err(|e| store_error(cell, "read catalog", e))?;
            let catalog = Catalog::decode(&bytes).ok_or_else(|| {
                StorageError::Corruption(ErrorContext {
                    engine: "volume",
                    instance: format!("volume/{cell}/catalog"),
                    file: None,
                    partition: None,
                    detail: "the catalog does not decode".into(),
                })
            })?;
            Ok((catalog, Some(version)))
        }
    }

    /// An object version as one string: the e-tag and the store's version, either possibly absent.
    fn encode_version(e_tag: Option<String>, version: Option<String>) -> String {
        format!(
            "{}\n{}",
            e_tag.unwrap_or_default(),
            version.unwrap_or_default()
        )
    }

    fn decode_version(text: &str) -> UpdateVersion {
        let (e_tag, version) = text.split_once('\n').unwrap_or((text, ""));
        let some = |s: &str| (!s.is_empty()).then(|| s.to_owned());
        UpdateVersion {
            e_tag: some(e_tag),
            version: some(version),
        }
    }

    fn projection(start: u64, end: u64) -> (Bound<Bytes>, Bound<Bytes>) {
        let (start, end) = keys(start, end);
        (
            Bound::Included(Bytes::from(start)),
            end.map_or(Bound::Unbounded, |end| Bound::Excluded(Bytes::from(end))),
        )
    }

    /// The SSD cache readers share: Foyer's memory and disk tiers, `mb` MiB on disk under `dir`. It is droppable: a
    /// cache lost with its disk only costs reads from S3.
    pub async fn ssd_cache(dir: &std::path::Path, mb: u64) -> Result<Arc<dyn DbCache>, String> {
        use foyer::{
            BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder,
            PsyncIoEngineConfig,
        };
        use slatedb::db_cache::CachedEntry;
        use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;

        let device = FsDeviceBuilder::new(dir)
            .with_capacity((mb << 20) as usize)
            .build()
            .map_err(|e| e.to_string())?;
        let cache = HybridCacheBuilder::new()
            .with_name("volume_ssd_cache")
            .memory(64 << 20)
            .with_weighter(|_, v: &CachedEntry| v.size())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(64 << 10))
            .build()
            .await
            .map_err(|e| e.to_string())?;
        Ok(Arc::new(FoyerHybridCache::new_with_cache(cache)))
    }

    /// A database's scope in the shared cache, stable across reopens (FNV-1a; cache keys also carry each SST's ID).
    fn scope(name: &str) -> u64 {
        name.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
    }
}

#[cfg(any(test, madsim))]
pub use memory::Memory;

/// An in-memory bucket: committed batches only, with writer fencing. Simulation builds use it instead of S3; it models
/// a commit whose answer is lost after it became durable.
#[cfg(any(test, madsim))]
mod memory {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, PoisonError};

    use async_trait::async_trait;
    use bedrock_lib::storage::namespace::Changes;
    use bedrock_lib::storage::{Entry, ErrorContext, StorageError, StorageResult};

    use super::{Catalog, Database, Databases, Writer, keys};

    #[derive(Default)]
    struct Stored {
        epoch: u64,
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    #[derive(Default)]
    struct Bucket {
        databases: BTreeMap<String, Stored>,
        /// Each cell's catalog and its version.
        catalogs: BTreeMap<String, (Catalog, u64)>,
        /// Commits so far, as a stand-in for S3 writes.
        commits: u64,
        /// The next commits fail; with `durable`, after they were applied.
        failing: u64,
        durable: bool,
    }

    /// Clones share one bucket, like nodes sharing S3.
    #[derive(Clone, Default)]
    pub struct Memory(Arc<Mutex<Bucket>>);

    #[cfg(madsim)]
    thread_local! {
        static SHARED: Memory = Memory::default();
    }

    impl Memory {
        /// The bucket every node of this simulation shares, as they would share S3. Each seed runs on a fresh thread.
        #[cfg(madsim)]
        pub fn shared() -> Self {
            SHARED.with(Clone::clone)
        }

        fn bucket(&self) -> std::sync::MutexGuard<'_, Bucket> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// Commits made so far.
        pub fn commits(&self) -> u64 {
            self.bucket().commits
        }

        /// Fails the next `count` commits; `durable` applies them first, as a lost answer would.
        pub fn fail(&self, count: u64, durable: bool) {
            let mut bucket = self.bucket();
            bucket.failing = count;
            bucket.durable = durable;
        }

        /// The name of every database, in order.
        pub fn names(&self) -> Vec<String> {
            self.bucket().databases.keys().cloned().collect()
        }

        /// Every key of `name`, as S3 holds it.
        pub fn contents(&self, name: &str) -> BTreeMap<Vec<u8>, Vec<u8>> {
            self.bucket()
                .databases
                .get(name)
                .map(|db| db.data.clone())
                .unwrap_or_default()
        }
    }

    fn context(name: &str, detail: &str) -> ErrorContext {
        ErrorContext {
            engine: "memory",
            instance: format!("volume/{name}"),
            file: None,
            partition: None,
            detail: detail.into(),
        }
    }

    struct Handle {
        bucket: Memory,
        name: String,
        /// The writer's epoch; `None` for a reader.
        epoch: Option<u64>,
    }

    impl Handle {
        fn check(&self, bucket: &Bucket) -> StorageResult<()> {
            match (self.epoch, bucket.databases.get(&self.name)) {
                (Some(mine), Some(db)) if db.epoch != mine => Err(StorageError::Fenced(context(
                    &self.name,
                    "a newer writer opened",
                ))),
                _ => Ok(()),
            }
        }
    }

    #[async_trait]
    impl Database for Handle {
        async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            let bucket = self.bucket.bucket();
            self.check(&bucket)?;
            Ok(bucket
                .databases
                .get(&self.name)
                .and_then(|db| db.data.get(key).cloned()))
        }

        async fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> StorageResult<Vec<Entry>> {
            let bucket = self.bucket.bucket();
            self.check(&bucket)?;
            if start >= end {
                return Ok(Vec::new());
            }
            Ok(bucket
                .databases
                .get(&self.name)
                .map_or_else(Vec::new, |db| {
                    db.data
                        .range(start.to_vec()..end.to_vec())
                        .take(limit)
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                }))
        }

        async fn close(&self) -> StorageResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl Writer for Handle {
        async fn commit(&self, changes: Changes) -> StorageResult<()> {
            let mut bucket = self.bucket.bucket();
            self.check(&bucket)?;
            let failed = bucket.failing > 0;
            if failed {
                bucket.failing -= 1;
                if !bucket.durable {
                    return Err(StorageError::Unavailable(context(&self.name, "S3 failed")));
                }
            }
            bucket.commits += 1;
            let db = bucket.databases.entry(self.name.clone()).or_default();
            for (key, value) in changes {
                match value {
                    Some(value) => db.data.insert(key, value),
                    None => db.data.remove(&key),
                };
            }
            if failed {
                return Err(StorageError::Unavailable(context(
                    &self.name,
                    "answer lost",
                )));
            }
            Ok(())
        }

        fn size(&self) -> u64 {
            let bucket = self.bucket.bucket();
            bucket.databases.get(&self.name).map_or(0, |db| {
                db.data
                    .iter()
                    .map(|(k, v)| (k.len() + v.len()) as u64)
                    .sum()
            })
        }
    }

    #[async_trait]
    impl Databases for Memory {
        async fn open(&self, name: &str) -> StorageResult<Arc<dyn Writer>> {
            let mut bucket = self.bucket();
            let db = bucket.databases.entry(name.to_owned()).or_default();
            db.epoch += 1;
            Ok(Arc::new(Handle {
                bucket: self.clone(),
                name: name.to_owned(),
                epoch: Some(db.epoch),
            }))
        }

        async fn open_reader(&self, name: &str) -> StorageResult<Option<Arc<dyn Database>>> {
            if !self.bucket().databases.contains_key(name) {
                return Ok(None);
            }
            Ok(Some(Arc::new(Handle {
                bucket: self.clone(),
                name: name.to_owned(),
                epoch: None,
            })))
        }

        async fn catalog(&self, cell: &str) -> StorageResult<(Catalog, Option<String>)> {
            Ok(self
                .bucket()
                .catalogs
                .get(cell)
                .map_or_else(Default::default, |(catalog, version)| {
                    (catalog.clone(), Some(version.to_string()))
                }))
        }

        async fn put_catalog(
            &self,
            cell: &str,
            catalog: &Catalog,
            version: Option<String>,
        ) -> StorageResult<bool> {
            let mut bucket = self.bucket();
            let stored = bucket.catalogs.get(cell).map(|(_, v)| v.to_string());
            if stored != version {
                return Ok(false);
            }
            let next = bucket.catalogs.get(cell).map_or(1, |(_, v)| v + 1);
            bucket
                .catalogs
                .insert(cell.to_owned(), (catalog.clone(), next));
            Ok(true)
        }

        async fn create_clone(
            &self,
            name: &str,
            sources: &[(String, u64, u64)],
        ) -> StorageResult<()> {
            let mut bucket = self.bucket();
            if bucket.databases.contains_key(name) {
                return Ok(());
            }
            let mut data = BTreeMap::new();
            for (source, start, end) in sources {
                let (start, end) = keys(*start, *end);
                let Some(db) = bucket.databases.get(source) else {
                    return Err(StorageError::Corruption(context(source, "no clone source")));
                };
                data.extend(
                    db.data
                        .range(start..)
                        .take_while(|(k, _)| end.as_ref().is_none_or(|end| *k < end))
                        .map(|(k, v)| (k.clone(), v.clone())),
                );
            }
            bucket
                .databases
                .insert(name.to_owned(), Stored { epoch: 0, data });
            Ok(())
        }

        async fn cells(&self) -> StorageResult<Vec<String>> {
            Ok(self.bucket().catalogs.keys().cloned().collect())
        }
    }
}
