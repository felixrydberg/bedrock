//! RocksDB [`StorageEngine`] (B-05). Compiled out under `cfg(madsim)`, where nodes use the in-memory engine.
//!
//! Data lives in the `data` column family, every key stored behind a `0x00` byte so the key space ends at `[0x01]`
//! and unbounded range deletes become one RocksDB range tombstone. Applied revisions live in `revisions`, keyed by
//! name with a big-endian `u64` value. Every batch is synced before `write` returns.

use std::ffi::CStr;
use std::path::Path;

use ::rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType,
    Direction, ErrorKind, IteratorMode, Options, ReadOptions, SnapshotWithThreadMode, WriteOptions,
    properties,
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
/// No UTF-8 revision name contains `0xFF`, so this ends the revision space.
const REVISION_END: [u8; 1] = [0xFF];

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

pub struct RocksDbAdapter {
    instance: String,
    db: DB,
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
        let cfs =
            [DATA, REVISIONS].map(|name| ColumnFamilyDescriptor::new(name, cf_options.clone()));

        let db =
            DB::open_cf_descriptors(&db_options, path, cfs).map_err(|e| error(&instance, e))?;
        Ok(RocksDbAdapter { instance, db })
    }

    fn data(&self) -> &ColumnFamily {
        self.db
            .cf_handle(DATA)
            .expect("opened with the data column family")
    }

    fn revisions(&self) -> &ColumnFamily {
        self.db
            .cf_handle(REVISIONS)
            .expect("opened with the revisions column family")
    }

    fn view(&self) -> View<'_> {
        View {
            engine: self,
            snapshot: None,
        }
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
            .property_int_value_cf(self.data(), name)
            .map_err(|e| error(&self.instance, e))?;
        Ok(value.unwrap_or(0))
    }
}

/// Reads at the latest state, or through `snapshot`.
struct View<'a> {
    engine: &'a RocksDbAdapter,
    snapshot: Option<SnapshotWithThreadMode<'a, DB>>,
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
            engine.data(),
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
            .get_cf_opt(engine.data(), data_key(key), &self.options())
            .map_err(|e| error(&engine.instance, e))
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        Ok(self.scan_unbound(range))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        let engine = self.engine;
        let value = engine
            .db
            .get_cf_opt(engine.revisions(), name, &self.options())
            .map_err(|e| error(&engine.instance, e))?;
        value
            .map(|value| self.decode_revision(name, &value))
            .transpose()
    }

    fn revisions(&self) -> StorageResult<std::collections::BTreeMap<String, u64>> {
        let engine = self.engine;
        engine
            .db
            .iterator_cf_opt(engine.revisions(), self.options(), IteratorMode::Start)
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
        let (data, revisions) = (self.data(), self.revisions());
        let mut rocks = ::rocksdb::WriteBatch::default();
        for op in batch.ops {
            match op {
                WriteOp::Put { key, value } => rocks.put_cf(data, data_key(&key), value),
                WriteOp::Delete { key } => rocks.delete_cf(data, data_key(&key)),
                WriteOp::DeleteRange(range) => {
                    if range.bounds().is_some() {
                        let end = range.end.as_deref().map_or(KEY_END.to_vec(), data_key);
                        rocks.delete_range_cf(data, data_key(&range.start), end);
                    }
                }
            }
        }
        for (name, revision) in batch.revisions {
            rocks.put_cf(revisions, name, revision.to_be_bytes());
        }
        self.commit(rocks)
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.view().revision(name)
    }

    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
        Ok(Box::new(View {
            engine: self,
            snapshot: Some(self.db.snapshot()),
        }))
    }

    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
        // ponytail: the whole image goes into one batch, so it must fit in memory; ingest SST files when it doesn't.
        let (data, revisions) = (self.data(), self.revisions());
        let mut rocks = ::rocksdb::WriteBatch::default();
        rocks.delete_range_cf(data, &[0x00][..], &KEY_END[..]);
        rocks.delete_range_cf(revisions, &[][..], &REVISION_END[..]);
        for entry in from.scan(&KeyRange::all())? {
            let (key, value) = entry?;
            rocks.put_cf(data, data_key(&key), value);
        }
        for (name, revision) in from.revisions()? {
            rocks.put_cf(revisions, name, revision.to_be_bytes());
        }
        self.commit(rocks)
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

    fn health(&self) -> StorageResult<()> {
        // ponytail: the counter never resets, so an error RocksDB recovered from keeps the instance unhealthy until
        // restart; read the background error status instead if that quarantines too much.
        let errors = self
            .db
            .property_int_value(&**properties::BACKGROUND_ERRORS)
            .map_err(|e| error(&self.instance, e))?;
        match errors {
            Some(n) if n > 0 => Err(StorageError::Unavailable(ErrorContext {
                engine: ENGINE,
                instance: self.instance.clone(),
                file: Some(self.db.path().display().to_string()),
                partition: None,
                detail: format!("{n} background errors; RocksDB may have stopped writes"),
            })),
            _ => Ok(()),
        }
    }
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
        // RocksDB does not say which keys a damaged block held, so B-14 quarantines the whole instance.
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
mod tests {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;

    use tempfile::TempDir;

    use super::*;
    use crate::storage::contract::{Harness, storage_contract};

    #[derive(Default)]
    struct Rocks {
        dirs: Vec<TempDir>,
    }

    impl Harness for Rocks {
        type Engine = RocksDbAdapter;

        fn open(&mut self) -> RocksDbAdapter {
            let dir = tempfile::tempdir().unwrap();
            let engine = RocksDbAdapter::open(dir.path(), "test", &RocksDbOptions::default());
            self.dirs.push(dir);
            engine.unwrap()
        }

        fn corrupt(&mut self, engine: RocksDbAdapter, key: &[u8]) -> RocksDbAdapter {
            corrupt_sst(engine, key)
        }
    }

    storage_contract!(Rocks::default());

    /// Flushes `key` into an SST file, then flips bytes in the file's first data block while the DB is closed.
    #[allow(clippy::disallowed_methods)] // test fault injection rewrites the instance's files
    fn corrupt_sst(engine: RocksDbAdapter, key: &[u8]) -> RocksDbAdapter {
        assert!(engine.get(key).unwrap().is_some());
        engine.db.flush_cf(engine.data()).unwrap();
        let path = engine.db.path().to_path_buf();
        drop(engine);
        let mut corrupted = 0;
        for entry in std::fs::read_dir(&path).unwrap() {
            let file = entry.unwrap().path();
            if file.extension().is_some_and(|ext| ext == "sst") {
                let mut bytes = std::fs::read(&file).unwrap();
                bytes[..8].iter_mut().for_each(|b| *b ^= 0xFF);
                std::fs::write(&file, bytes).unwrap();
                corrupted += 1;
            }
        }
        assert!(corrupted > 0, "flush wrote no SST file");
        RocksDbAdapter::open(&path, "test", &RocksDbOptions::default()).unwrap()
    }

    #[test]
    fn corruption_names_the_file() {
        let mut harness = Rocks::default();
        let engine = harness.open();
        engine.put(b"k", b"v").unwrap();
        let engine = harness.corrupt(engine, b"k");
        match engine.get(b"k") {
            Err(StorageError::Corruption(context)) => {
                let file = context.file.expect("file in context");
                assert!(file.ends_with(".sst"), "{file}");
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn data_and_revisions_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let options = RocksDbOptions {
            block_cache_mb: Some(8),
            write_buffer_mb: Some(8),
            max_background_jobs: Some(2),
        };
        let engine = RocksDbAdapter::open(dir.path(), "test", &options).unwrap();
        let mut batch = WriteBatch::default();
        batch.put(*b"a", *b"1").set_revision("ns", 4);
        engine.write(batch).unwrap();
        drop(engine);

        let engine = RocksDbAdapter::open(dir.path(), "test", &options).unwrap();
        assert_eq!(engine.get(b"a"), Ok(Some(b"1".to_vec())));
        assert_eq!(engine.revision("ns"), Ok(Some(4)));
        assert!(engine.disk_usage().unwrap() > 0);
        assert_eq!(engine.health(), Ok(()));
    }

    #[test]
    fn file_in_message() {
        let message = "Corruption: block checksum mismatch: stored = 1, computed = 2, type = 4  in /d/000009.sst offset 0 size 77";
        assert_eq!(file_in(message).as_deref(), Some("/d/000009.sst"));
        assert_eq!(
            file_in("Corruption: bad record in C:\\d\\MANIFEST-000005:").as_deref(),
            Some("C:\\d\\MANIFEST-000005")
        );
        assert_eq!(file_in("IO error: No space left on device"), None);
    }

    const CRASH_DIR: &str = "BEDROCK_ROCKSDB_CRASH_DIR";
    const ACKED: u64 = 100;

    /// Child of `acknowledged_writes_survive_kill`: writes batches forever, printing each acknowledged revision.
    #[test]
    #[ignore = "run by acknowledged_writes_survive_kill"]
    fn crash_writer() {
        let Ok(dir) = std::env::var(CRASH_DIR) else {
            return;
        };
        let engine = RocksDbAdapter::open(Path::new(&dir), "crash", &RocksDbOptions::default());
        let engine = engine.unwrap();
        for revision in 1u64..=u64::MAX {
            let mut batch = WriteBatch::default();
            batch
                .put(format!("ns/{revision}"), revision.to_string())
                .set_revision("ns", revision);
            engine.write(batch).unwrap();
            println!("ack {revision}");
        }
    }

    #[test]
    #[allow(clippy::disallowed_types)] // a real process kill needs a child process
    fn acknowledged_writes_survive_kill() {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::rocksdb::tests::crash_writer",
                "--ignored",
                "--nocapture",
            ])
            .env(CRASH_DIR, dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut acked = 0;
        for line in BufReader::new(child.stdout.take().unwrap()).lines() {
            if let Some(revision) = line.unwrap().strip_prefix("ack ") {
                acked = revision.parse().unwrap();
                if acked == ACKED {
                    break;
                }
            }
        }
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(acked, ACKED, "writer exited early");

        let engine = RocksDbAdapter::open(dir.path(), "crash", &RocksDbOptions::default()).unwrap();
        let revision = engine.revision("ns").unwrap().unwrap();
        assert!(revision >= ACKED, "lost acknowledged revision {revision}");
        for r in 1..=revision {
            let value = engine.get(format!("ns/{r}").as_bytes()).unwrap();
            assert_eq!(value, Some(r.to_string().into_bytes()), "revision {r}");
        }
        let next = engine.get(format!("ns/{}", revision + 1).as_bytes());
        assert_eq!(next, Ok(None), "data committed without its revision");
    }
}
