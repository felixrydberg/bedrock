//! #11 / B-11: atomic durable commits, crash recovery and retry replay on the seeded runtime.
//! Replay: `BEDROCK_MATERIALIZED_SEED=11 RUSTFLAGS="--cfg madsim" cargo test -p bedrock-sim --test materialized -- --nocapture`.
#![cfg(madsim)]

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::{DEFAULT_RETENTION_MS, Retention};
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::namespace::memory::MemoryFactory;
use bedrock_lib::storage::namespace::{Changes, NamespaceDatabase, NamespaceFactory};
use bedrock_lib::storage::{Entry, KeyRange, StorageEngine, StorageResult};
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Runtime;
use tokio::time::{Instant, sleep};

const SEEDS: &[u64] = &[1, 2, 3, 11, 26, 42];
const COMMITTED_AT: u64 = 1_000;

#[derive(Clone)]
struct Trace(Arc<Mutex<Vec<(u128, String)>>>, Instant);

impl Trace {
    #[allow(clippy::disallowed_methods)] // Instant is simulated by madsim.
    fn new() -> Self {
        Self(Arc::default(), Instant::now())
    }

    fn record(&self, event: String) {
        self.0
            .lock()
            .unwrap()
            .push((self.1.elapsed().as_nanos(), event));
    }

    fn hash(&self) -> u64 {
        fingerprint(&*self.0.lock().unwrap())
    }
}

fn fingerprint(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone)]
struct Factory {
    inner: MemoryFactory,
    trace: Trace,
}

struct Database {
    inner: Arc<dyn NamespaceDatabase>,
    trace: Trace,
}

async fn jitter() {
    sleep(Duration::from_millis(thread_rng().gen_range(1..5))).await;
}

#[tonic::async_trait]
impl NamespaceFactory for Factory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        jitter().await;
        let result = self.inner.namespaces().await;
        self.trace.record(format!("namespaces {result:?}"));
        result
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        jitter().await;
        let inner = self.inner.open(dataset, namespace).await?;
        self.trace.record(format!("open {dataset}/{namespace}"));
        Ok(Arc::new(Database {
            inner,
            trace: self.trace.clone(),
        }))
    }
}

#[tonic::async_trait]
impl NamespaceDatabase for Database {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        jitter().await;
        let result = self.inner.get(key).await;
        self.trace.record(format!("get {key:?} {result:?}"));
        result
    }

    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        jitter().await;
        let result = self.inner.scan(prefix).await;
        self.trace.record(format!("scan {prefix:?} {result:?}"));
        result
    }

    async fn write(&self, changes: Changes) -> StorageResult<()> {
        jitter().await;
        let hash = fingerprint(&changes);
        let result = self.inner.write(changes).await;
        self.trace.record(format!("write {hash} {result:?}"));
        result
    }

    async fn flush(&self) -> StorageResult<()> {
        jitter().await;
        let result = self.inner.flush().await;
        self.trace.record(format!("flush {result:?}"));
        result
    }

    async fn close(&self) -> StorageResult<()> {
        jitter().await;
        let result = self.inner.close().await;
        self.trace.record(format!("close {result:?}"));
        result
    }
}

fn write(key: Vec<u8>, operation: Operation, sequence: u64) -> Write {
    Write {
        dataset: "kv".into(),
        namespace: "n".into(),
        key,
        operation,
        idempotency_key: Some(format!("retry-{sequence}")),
    }
}

async fn scenario() -> u64 {
    let trace = Trace::new();
    let factory = Factory {
        inner: MemoryFactory::default(),
        trace: trace.clone(),
    };
    let writer = Writer::open(
        || COMMITTED_AT,
        Persistence {
            factory: Arc::new(factory.clone()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    let mut reference = BTreeMap::new();
    for sequence in 1..=32 {
        let key = vec![b'a' + thread_rng().gen_range(0..8)];
        let operation = if thread_rng().gen_bool(0.3) {
            reference.remove(&key);
            Operation::Delete
        } else {
            let value = thread_rng().gen_range(0..1_000_u64).to_be_bytes().to_vec();
            reference.insert(key.clone(), value.clone());
            Operation::Set(value)
        };
        assert_eq!(
            writer
                .commit(write(key, operation, sequence))
                .await
                .unwrap(),
            sequence
        );
        if thread_rng().gen_bool(0.5) {
            // Cleanup shares the namespace gate with durable commits.
            writer.cleanup("kv", "n").await.unwrap();
        }
        let view = writer.bootstrap("kv", "n").await.unwrap();
        assert_eq!(view.sequence, sequence);
        assert_eq!(view.committed_at, COMMITTED_AT);
        assert_eq!(view.values, reference);
        assert_eq!(view.retries.len(), sequence as usize);
        for revision in 1..=sequence {
            let retry = &view.retries[&format!("retry-{revision}")];
            assert_eq!(retry.sequence, revision);
            assert_eq!(retry.expires_at, COMMITTED_AT + DEFAULT_RETENTION_MS);
        }
        trace.record(format!(
            "view {sequence} {:?} {:?}",
            view.values, view.retries
        ));
    }

    assert_eq!(
        writer
            .commit(write(
                b"resurrection".to_vec(),
                Operation::Set(b"old".to_vec()),
                33
            ))
            .await
            .unwrap(),
        33
    );
    writer.cleanup("kv", "n").await.unwrap();
    assert_eq!(
        writer
            .commit(write(b"resurrection".to_vec(), Operation::Delete, 34))
            .await
            .unwrap(),
        34
    );
    assert_eq!(
        writer
            .commit(write(
                b"marker".to_vec(),
                Operation::Set(b"live".to_vec()),
                35
            ))
            .await
            .unwrap(),
        35
    );
    reference.insert(b"marker".to_vec(), b"live".to_vec());
    writer.cleanup("kv", "n").await.unwrap();
    // A retry of an overwritten/deleted operation returns its original result without restoring data.
    assert_eq!(
        writer
            .commit(write(
                b"resurrection".to_vec(),
                Operation::Set(b"wrong".to_vec()),
                33
            ))
            .await
            .unwrap(),
        33
    );
    assert_eq!(
        writer
            .commit(write(
                b"wrong".to_vec(),
                Operation::Set(b"wrong".to_vec()),
                1
            ))
            .await
            .unwrap(),
        1
    );
    let view = writer.bootstrap("kv", "n").await.unwrap();
    assert_eq!(view.sequence, 35);
    assert_eq!(view.values, reference);
    assert!(!view.values.contains_key(b"resurrection".as_slice()));

    let local = MemoryEngine::new("kv");
    local.put(b"n/stale", b"old").unwrap();
    local.put(b"other/keep", b"keep").unwrap();
    view.install(&local, "n", KeyRange::prefix(b"n/")).unwrap();
    assert_eq!(local.revision("n").unwrap(), Some(35));
    assert_eq!(local.get(b"n/stale").unwrap(), None);
    assert_eq!(local.get(b"n/resurrection").unwrap(), None);
    assert_eq!(local.get(b"other/keep").unwrap(), Some(b"keep".to_vec()));
    for (key, value) in reference {
        assert_eq!(
            local
                .get(&[b"n/".as_slice(), key.as_slice()].concat())
                .unwrap(),
            Some(value)
        );
    }

    // No graceful close: discard buffered state and invalidate every old handle.
    let expected = writer.bootstrap("kv", "n").await.unwrap();
    factory.inner.crash().unwrap();
    drop(writer);
    let recovered = Writer::open(
        || COMMITTED_AT,
        Persistence {
            factory: Arc::new(factory),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    assert_eq!(recovered.bootstrap("kv", "n").await.unwrap(), expected);
    assert_eq!(
        recovered
            .commit(write(b"wrong".to_vec(), Operation::Delete, 33))
            .await
            .unwrap(),
        33
    );
    assert_eq!(
        recovered
            .commit(write(
                b"after-crash".to_vec(),
                Operation::Set(b"durable".to_vec()),
                36
            ))
            .await
            .unwrap(),
        36
    );
    let final_view = recovered.bootstrap("kv", "n").await.unwrap();
    assert_eq!(final_view.sequence, 36);
    assert_eq!(
        final_view.values.get(b"after-crash".as_slice()),
        Some(&b"durable".to_vec())
    );
    trace.record(format!("recovered {final_view:?}"));
    recovered.close().await.unwrap();
    trace.hash()
}

#[allow(clippy::disallowed_methods)] // A fresh OS thread resets madsim's seeded std map hashing.
fn run(seed: u64) -> u64 {
    std::thread::spawn(move || Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario()))
        .join()
        .unwrap_or_else(|failure| {
            eprintln!("Replay: BEDROCK_MATERIALIZED_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test materialized -- --nocapture");
            std::panic::resume_unwind(failure)
        })
}

#[test]
fn same_seed_replays_materialized_trace_and_results() {
    let seeds = std::env::var("BEDROCK_MATERIALIZED_SEED")
        .map(|seed| {
            vec![
                seed.parse::<u64>()
                    .expect("BEDROCK_MATERIALIZED_SEED is a u64"),
            ]
        })
        .unwrap_or_else(|_| SEEDS.to_vec());
    let mut hashes = BTreeSet::new();
    for seed in seeds {
        let first = run(seed);
        assert_eq!(
            first,
            run(seed),
            "Replay: BEDROCK_MATERIALIZED_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test materialized -- --nocapture"
        );
        hashes.insert(first);
    }
    if std::env::var_os("BEDROCK_MATERIALIZED_SEED").is_none() {
        assert!(
            hashes.len() > 1,
            "materialized trace must depend on the seed"
        );
    }
}
