//! #24 / B-22b: read protection under seeded schedules and source latency.
//! Replay: `BEDROCK_READ_PROTECTION_SEED=11 RUSTFLAGS="--cfg madsim" cargo test -p bedrock-sim --test read_protection -- --nocapture`.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::health::fallback::{self, Found, Limited, Read, ReadKey, SourceChain, Tier};
use bedrock_lib::health::protection::{self, Limiter, S3_FETCHES};
use bedrock_lib::storage::{StorageError, StorageResult};
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Runtime;
use tokio::time::sleep;

const SEEDS: &[u64] = &[1, 2, 3, 11, 24, 42];

struct Get {
    cell: String,
    key: String,
}

impl ReadKey for Get {
    fn cell(&self) -> &str {
        &self.cell
    }

    fn identity(&self) -> (String, Vec<u8>) {
        ("ns".into(), self.key.clone().into_bytes())
    }
}

/// S3 with seeded latency. Records what it served and how many reads ran at once.
#[derive(Default)]
struct S3 {
    fetched: Mutex<BTreeMap<String, usize>>,
    running: Mutex<BTreeMap<String, usize>>,
    trace: Mutex<Vec<String>>,
}

#[tonic::async_trait]
impl Tier<Get> for S3 {
    fn name(&self) -> &'static str {
        "s3"
    }

    async fn read(&self, key: &Get, _: u64) -> StorageResult<Read> {
        *self
            .fetched
            .lock()
            .unwrap()
            .entry(key.key.clone())
            .or_default() += 1;
        {
            let mut running = self.running.lock().unwrap();
            let cell = running.entry(key.cell.clone()).or_default();
            *cell += 1;
            assert!(
                *cell <= S3_FETCHES.per_cell,
                "cell {} over its limit",
                key.cell
            );
            assert!(running.values().sum::<usize>() <= S3_FETCHES.total);
        }
        self.trace
            .lock()
            .unwrap()
            .push(format!("{} {}", key.cell, key.key));
        sleep(Duration::from_millis(thread_rng().gen_range(5..50))).await;
        *self.running.lock().unwrap().get_mut(&key.cell).unwrap() -= 1;
        Ok(Read::Found(Found {
            value: Some(key.key.clone().into_bytes()),
            revision: 1,
        }))
    }
}

/// One noisy cell and two quiet ones reading overlapping keys; returns a hash of the source's trace.
async fn scenario() -> u64 {
    let admin = Admin::new();
    let metrics = protection::Metrics::new(&admin);
    let limiter = Arc::new(Limiter::new("s3", S3_FETCHES, &metrics));
    let s3 = Arc::new(S3::default());
    let source: Arc<dyn Tier<Get>> = s3.clone();
    let tiers: Vec<Arc<dyn Tier<Get>>> = vec![Arc::new(Limited::new(source, limiter))];
    let chain = Arc::new(SourceChain::new(
        "cdn",
        tiers,
        &fallback::Metrics::new(&admin),
        &metrics,
    ));

    let mut requests = Vec::new();
    for tick in 0..300 {
        let quiet = tick % 10 == 0;
        for cell in ["noisy", "quiet-0", "quiet-1"] {
            if cell != "noisy" && !quiet {
                continue;
            }
            let chain = chain.clone();
            let key = Get {
                cell: cell.to_owned(),
                key: format!("key-{}", thread_rng().gen_range(0..200)),
            };
            requests.push(tokio::spawn(async move {
                let answer = chain.read(&key, 0).await;
                (key.cell, answer)
            }));
        }
        sleep(Duration::from_millis(1)).await;
    }
    let mut rejected = BTreeMap::<String, usize>::new();
    for request in requests {
        let (cell, answer) = request.await.unwrap();
        match answer {
            Ok(_) => {}
            Err(StorageError::Overloaded(_)) => *rejected.entry(cell).or_default() += 1,
            Err(error) => panic!("overload must stay retryable: {error}"),
        }
    }
    assert!(!rejected.contains_key("quiet-0"), "{rejected:?}");
    assert!(!rejected.contains_key("quiet-1"), "{rejected:?}");

    let mut hasher = DefaultHasher::new();
    s3.trace.lock().unwrap().hash(&mut hasher);
    rejected.hash(&mut hasher);
    hasher.finish()
}

#[allow(clippy::disallowed_methods)] // A fresh OS thread resets madsim's seeded std map hashing.
fn run(seed: u64) -> u64 {
    std::thread::spawn(move || Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario()))
        .join()
        .unwrap_or_else(|failure| {
            eprintln!("Replay: BEDROCK_READ_PROTECTION_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test read_protection -- --nocapture");
            std::panic::resume_unwind(failure)
        })
}

#[test]
fn one_seed_replays_one_schedule_and_limits_hold_on_every_seed() {
    let replay = std::env::var("BEDROCK_READ_PROTECTION_SEED")
        .ok()
        .map(|seed| vec![seed.parse().expect("seed must be a number")]);
    for &seed in replay.as_deref().unwrap_or(SEEDS) {
        assert_eq!(run(seed), run(seed), "seed {seed} did not replay");
    }
}
