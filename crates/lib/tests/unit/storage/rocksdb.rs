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

#[test]
#[allow(clippy::disallowed_methods)] // real threads exercise RocksDB's native write/restore race
fn writes_remain_atomic_while_restore_changes_generations() {
    let mut harness = Rocks::default();
    let engine = harness.open();
    let start = std::sync::Barrier::new(2);
    for revision in 1..=4 {
        std::thread::scope(|scope| {
            let mut batch = WriteBatch::default();
            for key in 0u32..262_144 {
                batch.put(key.to_be_bytes(), [42; 64]);
            }
            batch.set_revision("ns", revision);
            let writer = scope.spawn(|| {
                start.wait();
                engine.write(batch)
            });
            start.wait();
            // Let the write start assembling its batch before restore changes generations.
            std::thread::sleep(std::time::Duration::from_millis(10));
            engine.restore(&*empty().snapshot().unwrap()).unwrap();
            writer.join().unwrap().unwrap();
        });
        let has_data = engine.get(&0u32.to_be_bytes()).unwrap().is_some();
        assert_eq!(has_data, engine.revision("ns").unwrap().is_some());
    }
}

/// Flushes `key` into an SST file, then flips bytes in the file's first data block while the DB is closed.
#[allow(clippy::disallowed_methods)] // test fault injection rewrites the instance's files
fn corrupt_sst(engine: RocksDbAdapter, key: &[u8]) -> RocksDbAdapter {
    assert!(engine.get(key).unwrap().is_some());
    engine.db.flush_cf(&engine.data()).unwrap();
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
fn compact_reclaims_a_deleted_range() {
    let mut harness = Rocks::default();
    let engine = harness.open();
    // Incompressible values, so the SST files hold about 8 MiB.
    let mut state = 1u64;
    for i in 0..2048u32 {
        let value: Vec<u8> = (0..4096)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                (state >> 56) as u8
            })
            .collect();
        engine
            .put(&[b"r/".as_slice(), &i.to_be_bytes()].concat(), &value)
            .unwrap();
    }
    engine.put(b"s/kept", b"v").unwrap();
    engine.db.flush_cf(&engine.data()).unwrap();
    let full = engine.disk_usage().unwrap();
    engine.delete_range(KeyRange::prefix(*b"r/")).unwrap();
    engine.compact(&KeyRange::prefix(*b"r/")).unwrap();
    let after = engine.disk_usage().unwrap();
    assert!(
        after + 4 * 1024 * 1024 < full,
        "{after} bytes left of {full}"
    );
    assert_eq!(engine.get(b"s/kept"), Ok(Some(b"v".to_vec())));
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

fn reopened(engine: RocksDbAdapter) -> RocksDbAdapter {
    let path = engine.db.path().to_path_buf();
    drop(engine);
    RocksDbAdapter::open(&path, "test", &RocksDbOptions::default()).unwrap()
}

fn empty() -> crate::storage::memory::MemoryEngine {
    crate::storage::memory::MemoryEngine::new("empty")
}

const FIRST: &[u8] = b"k00000000";
const LAST: &[u8] = b"k00039999";

/// An engine with one table file large enough to have several blocks, with a block in the middle damaged.
#[allow(clippy::disallowed_methods)] // test fault injection rewrites the instance's files
fn damaged(harness: &mut Rocks) -> RocksDbAdapter {
    let engine = harness.open();
    for chunk in 0..40 {
        let mut batch = WriteBatch::default();
        for i in chunk * 1_000..(chunk + 1) * 1_000 {
            batch.put(format!("k{i:08}").into_bytes(), [b'v'; 200].to_vec());
        }
        engine.write(batch).unwrap();
    }
    engine.db.flush_cf(&engine.data()).unwrap();
    let path = engine.db.path().to_path_buf();
    drop(engine);
    let file = std::fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|file| file.extension().is_some_and(|ext| ext == "sst"))
        .unwrap();
    let mut bytes = std::fs::read(&file).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle..middle + 64]
        .iter_mut()
        .for_each(|b| *b ^= 0xFF);
    std::fs::write(&file, bytes).unwrap();
    RocksDbAdapter::open(&path, "test", &RocksDbOptions::default()).unwrap()
}

/// Writes across the whole key space and compacts it, which rewrites any table file left there.
fn compact_over(engine: &RocksDbAdapter) -> StorageResult<()> {
    engine.put(FIRST, b"newer")?;
    engine.put(LAST, b"newer")?;
    engine.db.flush_cf(&engine.data()).unwrap();
    engine.compact(&KeyRange::new(*b"a", *b"z"))
}

/// A range delete would leave the damaged file, and scans would pass until compaction read it and stopped all writes.
#[test]
fn a_restore_removes_a_damaged_file() {
    let mut harness = Rocks::default();
    let engine = damaged(&mut harness);

    engine.restore(&*empty().snapshot().unwrap()).unwrap();

    compact_over(&engine).unwrap();
    engine.put(b"later", b"1").unwrap();
    assert_eq!(engine.get(FIRST), Ok(Some(b"newer".to_vec())));
}

/// Found by the crash tests (#25 / B-23 Layer 3): a compaction that read the damaged block stopped every write, and its
/// error kept `health` failing, so the rebuild could neither refill the instance nor pass validation until a restart.
#[test]
fn a_rebuild_after_a_failed_compaction_needs_no_reopen() {
    let mut harness = Rocks::default();
    let engine = damaged(&mut harness);
    assert!(matches!(
        compact_over(&engine),
        Err(StorageError::Unavailable(_))
    ));
    assert!(engine.health().is_err());

    let source = harness.open();
    source.put(b"fresh", b"1").unwrap();
    engine.restore(&*source.snapshot().unwrap()).unwrap();
    engine.put(b"later", b"1").unwrap();

    compact_over(&engine).unwrap();
    engine.health().unwrap();
    assert_eq!(engine.get(b"fresh"), Ok(Some(b"1".to_vec())));
    assert_eq!(engine.get(b"k00020000"), Ok(None));
}

#[test]
fn a_restore_lasts_through_a_reopen() {
    let mut harness = Rocks::default();
    let source = harness.open();
    source.put(b"new", b"1").unwrap();
    let engine = harness.open();
    engine.put(b"old", b"1").unwrap();

    for _ in 0..2 {
        engine.restore(&*source.snapshot().unwrap()).unwrap();
    }
    let engine = reopened(engine);

    assert_eq!(engine.get(b"old"), Ok(None));
    assert_eq!(engine.get(b"new"), Ok(Some(b"1".to_vec())));
}

#[test]
fn a_restore_retries_with_leftover_column_families() {
    let mut harness = Rocks::default();
    let source = empty();
    let mut batch = WriteBatch::default();
    batch.put(*b"new", *b"1").set_revision("ns", 4);
    source.write(batch).unwrap();

    for leftovers in [&[DATA, REVISIONS][..], &[DATA], &[REVISIONS]] {
        let engine = harness.open();
        engine.put(b"old", b"1").unwrap();
        // A failed restore can leave either or both families when its cleanup also fails.
        for base in leftovers {
            let cf = name(base, 1);
            engine.db.create_cf(&cf, &Options::default()).unwrap();
            let cf = engine.handle(&cf);
            if *base == DATA {
                engine.db.put_cf(&cf, data_key(b"stale"), b"2").unwrap();
            } else {
                engine.db.put_cf(&cf, "stale", 99u64.to_be_bytes()).unwrap();
            }
        }

        engine.restore(&*source.snapshot().unwrap()).unwrap();

        assert_eq!(engine.get(b"new"), Ok(Some(b"1".to_vec())));
        assert_eq!(engine.get(b"old"), Ok(None));
        assert_eq!(engine.get(b"stale"), Ok(None));
        assert_eq!(engine.revision("ns"), Ok(Some(4)));
        assert_eq!(engine.revision("stale"), Ok(None));
    }
}

/// A crash before the old `data` is dropped has not committed, even with the new generation complete.
#[test]
fn a_restore_stopped_before_its_commit_changes_nothing() {
    let mut harness = Rocks::default();
    let engine = harness.open();
    engine.put(b"old", b"1").unwrap();
    for cf in [name(DATA, 1), name(REVISIONS, 1)] {
        engine.db.create_cf(cf, &Options::default()).unwrap();
    }

    let engine = reopened(engine);

    assert_eq!(engine.get(b"old"), Ok(Some(b"1".to_vec())));
    assert!(engine.db.cf_handle(&name(DATA, 1)).is_none());
}

/// A crash after dropping the old `data` has committed, even with the old revisions still there.
#[test]
fn a_restore_stopped_after_its_commit_finishes_on_open() {
    let mut harness = Rocks::default();
    let engine = harness.open();
    let mut batch = WriteBatch::default();
    batch.put(*b"old", *b"1").set_revision("ns", 3);
    engine.write(batch).unwrap();
    for cf in [name(DATA, 1), name(REVISIONS, 1)] {
        engine.db.create_cf(cf, &Options::default()).unwrap();
    }
    engine.db.drop_cf(DATA).unwrap();
    *engine.generation.write().unwrap() = 1;

    let engine = reopened(engine);

    assert_eq!(engine.get(b"old"), Ok(None));
    assert_eq!(engine.revision("ns"), Ok(None));
    assert!(engine.db.cf_handle(REVISIONS).is_none());
}

#[test]
fn a_snapshot_keeps_reading_through_a_restore() {
    let mut harness = Rocks::default();
    let engine = harness.open();
    engine.put(b"old", b"1").unwrap();
    let snapshot = engine.snapshot().unwrap();

    engine.restore(&*empty().snapshot().unwrap()).unwrap();

    assert_eq!(snapshot.get(b"old"), Ok(Some(b"1".to_vec())));
    assert_eq!(engine.get(b"old"), Ok(None));
}
