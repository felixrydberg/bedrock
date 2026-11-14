//! RocksDB [`StorageEngine`] (B-05). Compiled out under `cfg(madsim)`, where nodes use the in-memory engine.
//!
//! Data lives in the `data` column family, every key stored behind a `0x00` byte so the key space ends at `[0x01]`
//! and unbounded range deletes become one RocksDB range tombstone. Applied revisions live in `revisions`, keyed by
//! name with a big-endian `u64` value. Every batch is synced before `write` returns.
//!
//! [`StorageEngine::restore`] moves to a new generation of both column families, because a damaged table file can only leave
//! with its column family: range deletes hide its records, and every later compaction would read the bad block and fail.
//! Dropping the old `data` column family is the commit, so the lowest generation with both column families is current.

use std::collections::BTreeSet;
use std::ffi::CStr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use ::rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor, DBCompressionType,
    DBWithThreadMode, Direction, ErrorKind, IteratorMode, MultiThreaded, Options, ReadOptions,
    SnapshotWithThreadMode, WriteOptions, properties,
};
use serde::Deserialize;

use super::{
    ErrorContext, KeyRange, Scan, Snapshot, StorageEngine, StorageError, StorageResult,
    StorageStats, WriteBatch, WriteOp,
};

const ENGINE: &str = "rocksdb";
const DATA: &str = "data";
const REVISIONS: &str = "revisions";
/// The first byte after every stored data key.
const KEY_END: [u8; 1] = [0x01];

/// Memory and compaction budgets for one instance. Unset fields keep RocksDB's defaults.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RocksDbOptions {
    /// Block cache shared by both column families.
    pub block_cache_mb: Option<u64>,
    /// Memtable memory across column families; reaching it forces a flush.
    pub write_buffer_mb: Option<u64>,
    /// Concurrent flushes and compactions.
    pub max_background_jobs: Option<i32>,
}

type Db = DBWithThreadMode<MultiThreaded>;

pub struct RocksDbAdapter {
    instance: String,
    db: Db,
    cf_options: Options,
    generation: RwLock<u64>,
    /// Background errors counted before the last restore, which removed whatever caused them.
    restored_errors: AtomicU64,
}

impl RocksDbAdapter {
    /// Opens or creates the instance stored in `path`; `instance` names it in errors, normally the dataset.
    pub fn open(
        path: &Path,
        instance: impl Into<String>,
        options: &RocksDbOptions,
    ) -> StorageResult<Self> {
        let instance = instance.into();
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        // With paranoid checks, a damaged block read by compaction stops every write until a reopen, so the restore that
        // removes the block could never refill the instance. Without them the error still counts in `health`.
        db_options.set_paranoid_checks(false);
        if let Some(mb) = options.write_buffer_mb {
            db_options.set_db_write_buffer_size(mb_to_bytes(mb));
        }
        if let Some(jobs) = options.max_background_jobs {
            db_options.set_max_background_jobs(jobs);
        }

        let mut table = BlockBasedOptions::default();
        if let Some(mb) = options.block_cache_mb {
            table.set_block_cache(&Cache::new_lru_cache(mb_to_bytes(mb)));
        }
        let mut cf_options = Options::default();
        cf_options.set_compression_type(DBCompressionType::Lz4);
        cf_options.set_block_based_table_factory(&table);
        // Listing fails only for an instance that does not exist yet, or one that opening reports properly.
        let existing: BTreeSet<String> = Db::list_cf(&db_options, path)
            .unwrap_or_default()
            .into_iter()
            .collect();
        let generation = current(&existing);
        let current = [name(DATA, generation), name(REVISIONS, generation)];
        let cfs = existing
            .iter()
            .filter(|cf| *cf != "default")
            .chain(&current)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf, cf_options.clone()));

        let db =
            Db::open_cf_descriptors(&db_options, path, cfs).map_err(|e| error(&instance, e))?;
        // What a restore left behind, before compaction can read a damaged file in it.
        for cf in existing
            .iter()
            .filter(|cf| *cf != "default" && !current.contains(cf))
        {
            db.drop_cf(cf).map_err(|e| error(&instance, e))?;
        }
        Ok(RocksDbAdapter {
            instance,
            db,
            cf_options,
            generation: RwLock::new(generation),
            restored_errors: AtomicU64::new(0),
        })
    }

    fn background_errors(&self) -> StorageResult<u64> {
        let errors = self
            .db
            .property_int_value(&**properties::BACKGROUND_ERRORS)
            .map_err(|e| error(&self.instance, e))?;
        Ok(errors.unwrap_or(0))
    }

    fn handle(&self, cf: &str) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(cf).expect("opened or created above")
    }

    fn data(&self) -> Arc<BoundColumnFamily<'_>> {
        self.handle(&name(DATA, *self.generation.read().unwrap()))
    }

    fn view(&self) -> View<'_> {
        let generation = self.generation.read().unwrap();
        View {
            engine: self,
            data: self.handle(&name(DATA, *generation)),
            revisions: self.handle(&name(REVISIONS, *generation)),
            snapshot: None,
        }
    }

    /// Creates generation `next` holding `from`'s contents. An empty `from` writes nothing, so an engine whose writes
    /// stopped can still be emptied.
    fn fill(&self, next: u64, from: &dyn Snapshot) -> StorageResult<()> {
        let (data, revisions) = (name(DATA, next), name(REVISIONS, next));
        for cf in [&data, &revisions] {
            if self.db.cf_handle(cf).is_some() {
                self.db.drop_cf(cf).map_err(|e| error(&self.instance, e))?;
            }
            self.db
                .create_cf(cf, &self.cf_options)
                .map_err(|e| error(&self.instance, e))?;
        }
        let (data, revisions) = (self.handle(&data), self.handle(&revisions));
        // ponytail: the whole image goes into one batch, so it must fit in memory; ingest SST files when it doesn't.
        let mut rocks = ::rocksdb::WriteBatch::default();
        for entry in from.scan(&KeyRange::all())? {
            let (key, value) = entry?;
            rocks.put_cf(&data, data_key(&key), value);
        }
        for (name, revision) in from.revisions()? {
            rocks.put_cf(&revisions, name, revision.to_be_bytes());
        }
        if rocks.is_empty() {
            return Ok(());
        }
        self.commit(rocks)
    }

    fn commit(&self, batch: ::rocksdb::WriteBatch) -> StorageResult<()> {
        // ponytail: one fsync per batch; group concurrent batches if write throughput matters.
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db
            .write_opt(batch, &options)
            .map_err(|e| error(&self.instance, e))
    }

    fn property(&self, name: &CStr) -> StorageResult<u64> {
        let value = self
            .db
            .property_int_value_cf(&self.data(), name)
            .map_err(|e| error(&self.instance, e))?;
        Ok(value.unwrap_or(0))
    }
}

/// Reads at the latest state, or through `snapshot`.
struct View<'a> {
    engine: &'a RocksDbAdapter,
    /// Held so a restore during this view leaves it reading what it started on.
    data: Arc<BoundColumnFamily<'a>>,
    revisions: Arc<BoundColumnFamily<'a>>,
    snapshot: Option<SnapshotWithThreadMode<'a, Db>>,
}

impl<'a> View<'a> {
    fn options(&self) -> ReadOptions {
        let mut options = ReadOptions::default();
        if let Some(snapshot) = &self.snapshot {
            options.set_snapshot(snapshot);
        }
        options
    }

    /// The scan reads through `self.snapshot`, so only the lifetime-bound [`Snapshot::scan`] hands out a snapshot
    /// scan; the engine scans without one.
    fn scan_unbound(&self, range: &KeyRange) -> Scan<'a> {
        if range.bounds().is_none() {
            return Box::new(std::iter::empty());
        }
        let mut options = self.options();
        options.set_iterate_upper_bound(range.end.as_deref().map_or(KEY_END.to_vec(), data_key));
        let start = data_key(&range.start);
        let engine = self.engine;
        let iter = engine.db.iterator_cf_opt(
            &self.data,
            options,
            IteratorMode::From(&start, Direction::Forward),
        );
        Box::new(iter.map(move |item| {
            let (key, value) = item.map_err(|e| error(&engine.instance, e))?;
            Ok((key[1..].to_vec(), value.into_vec()))
        }))
    }

    fn decode_revision(&self, name: &str, value: &[u8]) -> StorageResult<u64> {
        let bytes = value.try_into().map_err(|_| {
            StorageError::Corruption(ErrorContext {
                engine: ENGINE,
                instance: self.engine.instance.clone(),
                file: None,
                partition: Some(format!("revision {name}")),
                detail: format!("revision value is {} bytes, not 8", value.len()),
            })
        })?;
        Ok(u64::from_be_bytes(bytes))
    }
}

impl Snapshot for View<'_> {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let engine = self.engine;
        engine
            .db
            .get_cf_opt(&self.data, data_key(key), &self.options())
            .map_err(|e| error(&engine.instance, e))
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        Ok(self.scan_unbound(range))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        let engine = self.engine;
        let value = engine
            .db
            .get_cf_opt(&self.revisions, name, &self.options())
            .map_err(|e| error(&engine.instance, e))?;
        value
            .map(|value| self.decode_revision(name, &value))
            .transpose()
    }

    fn revisions(&self) -> StorageResult<std::collections::BTreeMap<String, u64>> {
        self.revisions_between("", None)
    }

    fn revisions_in(
        &self,
        ranges: &[super::Names],
    ) -> StorageResult<std::collections::BTreeMap<String, u64>> {
        let mut found = std::collections::BTreeMap::new();
        for (from, to) in ranges {
            found.extend(self.revisions_between(from, Some(to))?);
        }
        Ok(found)
    }
}

impl View<'_> {
    /// The revisions named in `[from, to)`, reading only those; no `to` reads to the end.
    fn revisions_between(
        &self,
        from: &str,
        to: Option<&str>,
    ) -> StorageResult<std::collections::BTreeMap<String, u64>> {
        let engine = self.engine;
        engine
            .db
            .iterator_cf_opt(
                &self.revisions,
                self.options(),
                IteratorMode::From(from.as_bytes(), Direction::Forward),
            )
            .take_while(|item| match (item, to) {
                (Ok((name, _)), Some(to)) => **name < *to.as_bytes(),
                _ => true,
            })
            .map(|item| {
                let (name, value) = item.map_err(|e| error(&engine.instance, e))?;
                let name = String::from_utf8(name.into_vec()).map_err(|e| {
                    StorageError::Corruption(ErrorContext {
                        engine: ENGINE,
                        instance: engine.instance.clone(),
                        file: None,
                        partition: Some("revisions".into()),
                        detail: format!("revision name is not UTF-8: {e}"),
                    })
                })?;
                let revision = self.decode_revision(&name, &value)?;
                Ok((name, revision))
            })
            .collect()
    }
}

impl StorageEngine for RocksDbAdapter {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.view().get(key)
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        Ok(self.view().scan_unbound(range))
    }

    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        let generation = self.generation.read().unwrap();
        let (data, revisions) = (
            self.handle(&name(DATA, *generation)),
            self.handle(&name(REVISIONS, *generation)),
        );
        let mut rocks = ::rocksdb::WriteBatch::default();
        for op in batch.ops {
            match op {
                WriteOp::Put { key, value } => rocks.put_cf(&data, data_key(&key), value),
                WriteOp::Delete { key } => rocks.delete_cf(&data, data_key(&key)),
                WriteOp::DeleteRange(range) => {
                    if range.bounds().is_some() {
                        let end = range.end.as_deref().map_or(KEY_END.to_vec(), data_key);
                        rocks.delete_range_cf(&data, data_key(&range.start), end);
                    }
                }
            }
        }
        for (name, revision) in batch.revisions {
            match revision {
                Some(revision) => rocks.put_cf(&revisions, name, revision.to_be_bytes()),
                None => rocks.delete_cf(&revisions, name),
            }
        }
        let result = self.commit(rocks);
        drop(generation);
        result
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.view().revision(name)
    }

    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
        // The column families first: a sequence number means nothing to ones created after it.
        let view = self.view();
        Ok(Box::new(View {
            snapshot: Some(self.db.snapshot()),
            ..view
        }))
    }

    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
        let mut generation = self.generation.write().unwrap();
        // Read first: an error during the restore may come from the new generation.
        let errors = self.background_errors()?;
        let (old, next) = (*generation, *generation + 1);
        let committed = self.fill(next, from).and_then(|()| {
            self.db
                .drop_cf(&name(DATA, old))
                .map_err(|e| error(&self.instance, e))
        });
        if let Err(failed) = committed {
            // Opening drops these too, so only the first error matters.
            let _ = self.db.drop_cf(&name(DATA, next));
            let _ = self.db.drop_cf(&name(REVISIONS, next));
            return Err(failed);
        }
        *generation = next;
        self.restored_errors.store(errors, Ordering::Relaxed);
        if let Err(error) = self.db.drop_cf(&name(REVISIONS, old)) {
            tracing::warn!(%error, "old revisions stay until the next open");
        }
        Ok(())
    }

    fn stats(&self) -> StorageResult<StorageStats> {
        Ok(StorageStats {
            keys: self.property(properties::ESTIMATE_NUM_KEYS)?,
            live_bytes: self.property(properties::ESTIMATE_LIVE_DATA_SIZE)?
                + self.property(properties::CUR_SIZE_ALL_MEM_TABLES)?,
        })
    }

    #[allow(clippy::disallowed_methods)] // adapter code: sums the instance's files on disk
    fn disk_usage(&self) -> StorageResult<u64> {
        let path = self.db.path();
        let io_error = |e: std::io::Error| {
            StorageError::IoError(ErrorContext {
                engine: ENGINE,
                instance: self.instance.clone(),
                file: Some(path.display().to_string()),
                partition: None,
                detail: e.to_string(),
            })
        };
        let mut bytes = 0;
        for entry in std::fs::read_dir(path).map_err(io_error)? {
            bytes += entry.and_then(|e| e.metadata()).map_err(io_error)?.len();
        }
        Ok(bytes)
    }

    /// RocksDB drops range tombstones and the keys under them only when compaction rewrites their files.
    fn compact(&self, range: &KeyRange) -> StorageResult<()> {
        if range.bounds().is_none() {
            return Ok(());
        }
        let end = range.end.as_deref().map_or(KEY_END.to_vec(), data_key);
        self.db
            .compact_range_cf(&self.data(), Some(data_key(&range.start)), Some(end));
        self.health()
    }

    /// From the SST index: memtables are not counted, and compressed files count as stored.
    fn approximate_size(&self, range: &KeyRange) -> StorageResult<u64> {
        let start = data_key(&range.start);
        let end = range.end.as_deref().map_or(KEY_END.to_vec(), data_key);
        let sizes = self
            .db
            .get_approximate_sizes_cf(&self.data(), &[rocksdb::Range::new(&start, &end)]);
        Ok(sizes[0])
    }

    fn health(&self) -> StorageResult<()> {
        // RocksDB's counter never falls, so only errors since the last restore count. ponytail: one transient flush error
        // keeps the instance unhealthy until its rebuild; read the background error status if that rebuilds too much.
        let n = self.background_errors()? - self.restored_errors.load(Ordering::Relaxed);
        if n == 0 {
            return Ok(());
        }
        Err(StorageError::Unavailable(ErrorContext {
            engine: ENGINE,
            instance: self.instance.clone(),
            file: Some(self.db.path().display().to_string()),
            partition: None,
            detail: format!("{n} background errors since the last restore"),
        }))
    }
}

/// The column family `base` of `generation`. Generation 0 keeps the original names.
fn name(base: &str, generation: u64) -> String {
    match generation {
        0 => base.to_owned(),
        n => format!("{base}.{n}"),
    }
}

/// The lowest generation with both column families, or 0 for a new instance.
fn current(existing: &BTreeSet<String>) -> u64 {
    existing
        .iter()
        .filter_map(|cf| match cf.split_once('.') {
            Some((DATA, n)) => n.parse().ok(),
            None if cf == DATA => Some(0),
            _ => None,
        })
        .filter(|generation| existing.contains(&name(REVISIONS, *generation)))
        .min()
        .unwrap_or(0)
}

fn data_key(key: &[u8]) -> Vec<u8> {
    let mut stored = Vec::with_capacity(key.len() + 1);
    stored.push(0x00);
    stored.extend_from_slice(key);
    stored
}

fn mb_to_bytes(mb: u64) -> usize {
    usize::try_from(mb.saturating_mul(1 << 20)).unwrap_or(usize::MAX)
}

fn error(instance: &str, e: ::rocksdb::Error) -> StorageError {
    let kind = e.kind();
    let detail = e.into_string();
    let context = ErrorContext {
        engine: ENGINE,
        instance: instance.to_owned(),
        file: file_in(&detail),
        // RocksDB does not say which keys a damaged block held; `health::storage` quarantines the whole instance.
        partition: None,
        detail,
    };
    match kind {
        ErrorKind::Corruption => StorageError::Corruption(context),
        ErrorKind::NotSupported => StorageError::Unsupported(context),
        ErrorKind::ShutdownInProgress
        | ErrorKind::Busy
        | ErrorKind::TimedOut
        | ErrorKind::TryAgain
        | ErrorKind::Aborted
        | ErrorKind::ColumnFamilyDropped => StorageError::Unavailable(context),
        _ => StorageError::IoError(context),
    }
}

/// The RocksDB file a message names, e.g. `.../000012.sst` in a checksum mismatch.
fn file_in(message: &str) -> Option<String> {
    message
        .split_whitespace()
        .map(|word| word.trim_end_matches([',', ':', ';', ')']))
        .find(|word| {
            [".sst", ".log", ".blob"]
                .iter()
                .any(|ext| word.ends_with(ext))
                || word.contains("MANIFEST-")
        })
        .map(str::to_owned)
}

#[cfg(test)]
#[path = "../../tests/unit/storage/rocksdb.rs"]
mod tests;
