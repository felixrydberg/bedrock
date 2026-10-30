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
