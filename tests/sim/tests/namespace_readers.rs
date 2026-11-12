//! #57 / B-39: customer-KV readers under seeded store faults, crashes and writer restarts.
//! Replay: `BEDROCK_READERS_SEED=7 RUSTFLAGS="--cfg madsim" cargo test -p bedrock-sim --test namespace_readers -- --nocapture`;
//! add `BEDROCK_READERS_TRACE=1` to print the trace.
//!
//! A writer publishes revisions while readers ask for keys at minimum revisions they have seen. Every answer must
//! be at least the revision asked for, never a revision no write reached the store with, never older than an earlier
//! answer, and its value must be one the namespace held at or after the answer's revision.
#![cfg(madsim)]

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_kv::namespaces::{
    CACHE_TTL_SECONDS, FORMAT_VERSION, Lifecycle, Record, storage as storage_of,
};
use bedrock_kv::storage::{DATASET, ReadError, Readers};
use bedrock_lib::admin::Admin;
use bedrock_lib::replication::materialized::{Committed, Materialized};
use bedrock_lib::replication::{Event, Operation};
use bedrock_lib::storage::sim::{SimFactory, StoreAccess, StoreFault};
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Runtime;
use tokio::time::sleep;

const SEEDS: &[u64] = &[1, 2, 3, 7, 42];
const NAMESPACE: &str = "0123456789abcdef0123456789abcdef";
const KEYS: u64 = 4;
const REVISIONS: u64 = 60;

fn record() -> Record {
    Record {
        format_version: FORMAT_VERSION,
        id: NAMESPACE.into(),
        tenant: "tenant".into(),
        title: "readers".into(),
        home_region: "eu".into(),
        storage: storage_of(NAMESPACE),
        storage_format: bedrock_lib::replication::materialized::FORMAT_VERSION,
        storage_bytes: None,
        permissions: Vec::new(),
        cache_ttl_seconds: CACHE_TTL_SECONDS,
        state: Lifecycle::Active as i32,
        created_at: 1,
        deleting_at: 0,
    }
}

/// Revision `sequence` always writes the same key and value, so a write retried after a crash is the same write.
fn write(sequence: u64) -> Committed {
    Committed {
        event: Event {
            dataset: DATASET.into(),
            namespace: NAMESPACE.into(),
            stream: "kv-0".into(),
            sequence,
            epoch: 1,
            committed_at: sequence * 10,
            key: format!("k{}", sequence % KEYS).into_bytes(),
            operation: Operation::Set(sequence.to_be_bytes().to_vec()),
        },
        retry_key: None,
        retry_expires_at: 0,
    }
}

#[derive(Default)]
struct Observed {
    /// The highest revision any write attempt carried: a reader may see an ambiguous write that landed, never more.
    attempted: u64,
    /// The highest revision any reader answered with.
    answered: u64,
    trace: Vec<String>,
}

async fn writer(factory: SimFactory, observed: Arc<Mutex<Observed>>) {
    let mut next = 1;
    while next <= REVISIONS {
        let Ok(writer) = Materialized::open(&factory, DATASET, NAMESPACE).await else {
            sleep(Duration::from_millis(50)).await;
            continue;
        };
        // Recovery resumes after what is durable; anything lost is written again, identically.
        match writer.progress().await {
            Ok(progress) => next = progress.sequence + 1,
            Err(_) => continue,
        }
        while next <= REVISIONS {
            {
                let mut observed = observed.lock().unwrap();
                observed.attempted = observed.attempted.max(next);
            }
            let published = match writer.apply(&[write(next)]).await {
                Ok(()) => writer.publish().await,
                Err(error) => Err(error),
            };
            if published.is_err() {
                break;
            }
            observed
                .lock()
                .unwrap()
                .trace
                .push(format!("published {next}"));
            next += 1;
            sleep(Duration::from_millis(thread_rng().gen_range(1..40))).await;
        }
        let _ = writer.close().await;
    }
}

async fn reader(n: usize, readers: Arc<Readers>, observed: Arc<Mutex<Observed>>) {
    let namespace = record();
    let mut seen = 0;
    for _ in 0..80 {
        let key = thread_rng().gen_range(0..KEYS);
        let min_revision = if thread_rng().gen_bool(0.5) { seen } else { 0 };
        let read = readers
            .get(&namespace, format!("k{key}").as_bytes(), min_revision)
            .await;
        match read {
            Ok((value, position)) => {
                let mut observed = observed.lock().unwrap();
                assert!(
                    position.sequence >= min_revision,
                    "below its minimum revision"
                );
                assert!(
                    position.sequence >= seen,
                    "went back from {seen} to {}",
                    position.sequence
                );
                assert!(
                    position.sequence <= observed.attempted,
                    "revision {} was never written",
                    position.sequence
                );
                // The last write to this key at or before the answer's revision, which the value must not predate.
                let floor = (1..=position.sequence).rev().find(|s| s % KEYS == key);
                // The answer's revision is a lower bound: the value may come from a later write, never an earlier one.
                match (value, floor) {
                    (None, None) => {}
                    (Some(bytes), floor) => {
                        let written =
                            u64::from_be_bytes(bytes.try_into().expect("an 8-byte revision"));
                        assert_eq!(written % KEYS, key, "a value from another key");
                        assert!(
                            floor.is_none_or(|floor| written >= floor),
                            "value from {written} predates revision {floor:?}"
                        );
                        assert!(
                            written <= observed.attempted,
                            "value {written} was never written"
                        );
                    }
                    (None, Some(floor)) => {
                        panic!("k{key} at {position:?} is absent but was written at {floor}")
                    }
                }
                seen = position.sequence;
                observed.answered = observed.answered.max(seen);
                observed.trace.push(format!("reader {n} k{key} at {seen}"));
            }
            Err(ReadError::Storage(error)) => {
                observed
                    .lock()
                    .unwrap()
                    .trace
                    .push(format!("reader {n} failed: {error}"));
            }
            Err(other) => panic!("a home, active namespace was refused: {other:?}"),
        }
        sleep(Duration::from_millis(thread_rng().gen_range(1..60))).await;
    }
}

async fn scenario() -> u64 {
    let faults = Arc::new(|access: StoreAccess| {
        let mut rng = thread_rng();
        match access {
            StoreAccess::Read if rng.gen_bool(0.05) => Some(StoreFault::Unavailable),
            StoreAccess::Read if rng.gen_bool(0.05) => Some(StoreFault::Delay(
                Duration::from_millis(rng.gen_range(1..200)),
            )),
            StoreAccess::Open if rng.gen_bool(0.1) => Some(StoreFault::Unavailable),
            StoreAccess::Flush if rng.gen_bool(0.05) => Some(StoreFault::Ambiguous),
            StoreAccess::Write if rng.gen_bool(0.03) => Some(StoreFault::Unavailable),
            _ => None,
        }
    });
    let factory = SimFactory::new(faults);
    let observed = Arc::new(Mutex::new(Observed::default()));
    let readers = Readers::new(
        &Admin::new(),
        Arc::new(factory.clone()),
        "eu".into(),
        "eu-1a".into(),
        // One namespace, so this never evicts: open races and idle closes are what it exercises. Eviction and reopen are
        // the kv unit tests'.
        1,
    );

    let writing = tokio::spawn(writer(factory.clone(), observed.clone()));
    let reading: Vec<_> = (0..3)
        .map(|n| tokio::spawn(reader(n, readers.clone(), observed.clone())))
        .collect();
    let crashing = {
        let (factory, observed) = (factory.clone(), observed.clone());
        tokio::spawn(async move {
            for _ in 0..4 {
                sleep(Duration::from_millis(thread_rng().gen_range(100..900))).await;
                factory.crash().unwrap();
                observed.lock().unwrap().trace.push("crash".into());
            }
        })
    };
    let sweeping = {
        let readers = readers.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(300)).await;
                readers.sweep().await;
            }
        })
    };
    for reading in reading {
        reading.await.unwrap();
    }
    crashing.await.unwrap();
    writing.await.unwrap();
    sweeping.abort();

    let observed = observed.lock().unwrap();
    assert!(observed.answered > 0, "no read ever succeeded");
    if std::env::var_os("BEDROCK_READERS_TRACE").is_some() {
        for line in &observed.trace {
            eprintln!("TRACE {line}");
        }
        eprintln!("TRACE END");
    }
    let mut hasher = DefaultHasher::new();
    observed.trace.hash(&mut hasher);
    hasher.finish()
}

#[allow(clippy::disallowed_methods)] // A fresh OS thread resets madsim's seeded std map hashing.
fn run(seed: u64) -> u64 {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario())
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: BEDROCK_READERS_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test namespace_readers -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

/// Runs `seed` in a child process and returns its trace hash. Replays are compared across processes: tokio seeds its
/// `watch` waker shards from a process-wide counter, so a second run inside one process may wake waiters differently.
#[allow(clippy::disallowed_types)] // This launcher isolates seeds before simulation starts.
fn in_child(seed: u64) -> u64 {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "readers_hold_their_invariants_and_each_seed_replays",
            "--nocapture",
        ])
        .env("BEDROCK_READERS_SEED", seed.to_string())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "seed {seed} failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
        .lines()
        .find_map(|line| line.split_once("READERS "))
        .and_then(|(_, hash)| hash.trim().parse().ok())
        .expect("the child prints its trace hash")
}

#[test]
fn readers_hold_their_invariants_and_each_seed_replays() {
    match std::env::var("BEDROCK_READERS_SEED") {
        Ok(seed) => println!(
            "READERS {}",
            run(seed.parse().expect("seed must be a number"))
        ),
        Err(_) => {
            for &seed in SEEDS {
                assert_eq!(in_child(seed), in_child(seed), "seed {seed} did not replay");
            }
        }
    }
}
