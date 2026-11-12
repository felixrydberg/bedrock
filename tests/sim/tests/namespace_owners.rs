//! #58 / B-26: customer-KV owners under seeded membership changes, store faults and crashes.
//! Replay: `BEDROCK_OWNERS_SEED=7 RUSTFLAGS="--cfg madsim" cargo test -p bedrock-sim --test namespace_owners -- --nocapture`;
//! add `BEDROCK_OWNERS_TRACE=1` to print the trace.
//!
//! Three nodes share one store. Membership keeps moving the namespace's ownership between them, and each node's view
//! of it lags on its own, so two nodes sometimes both believe they own it: the store's fencing must keep them apart.
//! Clients write through any node, following "not owner" answers, and read back at revisions they were given.
//!
//! What must hold (`docs/spec/materialized.tla`, `NoLostAck` across `Move`):
//! - One revision is acknowledged for one write only: a fenced old owner never acknowledges a revision the new one
//!   also gave out.
//! - A read at a minimum revision answers at that revision or later, with a value some write to that key carried.
//! - At the end, the owner holds, for every key, a write no older than the last one acknowledged for it.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_kv::namespaces::{
    CACHE_TTL_SECONDS, FORMAT_VERSION, Lifecycle, Record, storage as storage_of,
};
use bedrock_kv::storage::{Members, OwnerError, Owners, Route, Stored};
use bedrock_lib::admin::Admin;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{Persistence, Writer};
use bedrock_lib::storage::sim::{SimFactory, StoreAccess, StoreFault};
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Runtime;
use tokio::time::sleep;

const SEEDS: &[u64] = &[1, 2, 3, 7, 42];
const NAMESPACE: &str = "0123456789abcdef0123456789abcdef";
const NODES: [&str; 3] = ["kv-1", "kv-2", "kv-3"];
const KEYS: u64 = 4;
const WRITES: u64 = 300;
const CLIENTS: u64 = 3;

fn record() -> Record {
    Record {
        format_version: FORMAT_VERSION,
        id: NAMESPACE.into(),
        tenant: "tenant".into(),
        title: "owners".into(),
        home_region: "eu".into(),
        storage: storage_of(NAMESPACE),
        storage_format: bedrock_lib::replication::materialized::FORMAT_VERSION,
        storage_bytes: None,
        permissions: Vec::new(),
        cache_ttl_seconds: CACHE_TTL_SECONDS,
        state: Lifecycle::Active as i32,
        created_at: 1,
        deleting_at: 0,
        home_cell: "eu-1a".into(),
    }
}

fn member(node: &str) -> pb::Member {
    pb::Member {
        node_id: node.into(),
        service: "kv".into(),
        region: "eu".into(),
        cell: "eu-1a".into(),
        status: pb::member::Status::Ready as i32,
        address: node.into(),
        ..Default::default()
    }
}

/// One node's view of membership: which nodes it believes are ready.
type View = Arc<Mutex<Vec<&'static str>>>;

fn members(view: &View) -> Members {
    let view = view.clone();
    Arc::new(move || {
        view.lock()
            .unwrap()
            .iter()
            .map(|node| (node.to_string(), member(node)))
            .collect()
    })
}

/// Write `id` stores `id` under key `id % KEYS`, so a value names the write that carried it.
fn value(id: u64) -> Stored {
    Stored {
        value: id.to_be_bytes().to_vec(),
        metadata: None,
        expiration: 0,
    }
}

#[derive(Default)]
struct Observed {
    /// Revision → the write acknowledged with it.
    acked: BTreeMap<u64, u64>,
    /// Key → (revision, write) of its last acknowledged write.
    last: BTreeMap<u64, (u64, u64)>,
    trace: Vec<String>,
}

impl Observed {
    fn ack(&mut self, write: u64, revision: u64) {
        let earlier = *self.acked.entry(revision).or_insert(write);
        assert_eq!(
            earlier, write,
            "revision {revision} acknowledged for writes {earlier} and {write}"
        );
        let last = self.last.entry(write % KEYS).or_insert((0, 0));
        if revision > last.0 {
            *last = (revision, write);
        }
        self.trace.push(format!("ack {write} at {revision}"));
    }
}

/// Sends a write to `node`, following "not owner" answers between nodes, until it is acknowledged; a retry carries the
/// same idempotency key, so a write that landed unacknowledged is answered, not applied twice.
async fn client(
    n: u64,
    nodes: Arc<BTreeMap<&'static str, Arc<Owners>>>,
    observed: Arc<Mutex<Observed>>,
) {
    let namespace = record();
    let mut seen = 0;
    for write in (0..WRITES).filter(|w| w % CLIENTS == n) {
        let key = (write % KEYS).to_be_bytes().to_vec();
        let mut node = NODES[thread_rng().gen_range(0..NODES.len())];
        let mut hops = 0;
        let revision = loop {
            let attempt = nodes[node]
                .write(
                    &namespace,
                    vec![(key.clone(), Some(value(write)))],
                    Some(&format!("w{write}")),
                )
                .await;
            match attempt {
                Ok(revision) => break revision,
                // Follow the owner as `operations` forwards, at most `MAX_HOPS` times; nodes whose views disagree can point
                // at each other, and only waiting lets their views catch up.
                Err(OwnerError::NotOwner(Route::Peer(owner)))
                    if hops < bedrock_kv::operations::MAX_HOPS =>
                {
                    hops += 1;
                    node = NODES.iter().find(|n| **n == owner).expect("a known node");
                }
                Err(error) => {
                    hops = 0;
                    observed
                        .lock()
                        .unwrap()
                        .trace
                        .push(format!("write {write} on {node}: {error:?}"));
                    sleep(Duration::from_millis(thread_rng().gen_range(50..500))).await;
                    node = NODES[thread_rng().gen_range(0..NODES.len())];
                }
            }
        };
        observed.lock().unwrap().ack(write, revision);
        seen = seen.max(revision);

        // Read some key back at a revision this client was given, from whichever node owns it now.
        let read_key = thread_rng().gen_range(0..KEYS);
        for node in NODES {
            match nodes[node]
                .get(&namespace, &read_key.to_be_bytes(), seen)
                .await
            {
                Ok((found, position)) => {
                    assert!(
                        position.sequence >= seen,
                        "{node} answered below revision {seen}"
                    );
                    if let Some(found) = found {
                        let carried = u64::from_be_bytes(found.value.try_into().expect("8 bytes"));
                        assert_eq!(carried % KEYS, read_key, "a value from another key");
                        assert!(carried < WRITES, "value {carried} was never written");
                    }
                    observed.lock().unwrap().trace.push(format!(
                        "read k{read_key} on {node} at {}",
                        position.sequence
                    ));
                    break;
                }
                Err(error) => observed
                    .lock()
                    .unwrap()
                    .trace
                    .push(format!("read on {node}: {error:?}")),
            }
        }
        sleep(Duration::from_millis(thread_rng().gen_range(1..200))).await;
    }
}

/// The run's trace hash, and how many writes a fenced owner refused.
async fn scenario(observed: Arc<Mutex<Observed>>) -> (u64, usize) {
    let faults = Arc::new(|access: StoreAccess| {
        let mut rng = thread_rng();
        match access {
            StoreAccess::Read if rng.gen_bool(0.03) => Some(StoreFault::Unavailable),
            StoreAccess::Read if rng.gen_bool(0.03) => Some(StoreFault::Delay(
                Duration::from_millis(rng.gen_range(1..200)),
            )),
            StoreAccess::Open if rng.gen_bool(0.05) => Some(StoreFault::Unavailable),
            StoreAccess::Flush if rng.gen_bool(0.03) => Some(StoreFault::Ambiguous),
            StoreAccess::Write if rng.gen_bool(0.02) => Some(StoreFault::Unavailable),
            _ => None,
        }
    });
    let factory = SimFactory::new(faults);
    let views: BTreeMap<&str, View> = NODES
        .iter()
        .map(|node| (*node, Arc::new(Mutex::new(NODES.to_vec()))))
        .collect();
    let nodes: Arc<BTreeMap<&'static str, Arc<Owners>>> = Arc::new(
        NODES
            .iter()
            .map(|node| {
                let writer = Writer::owner(
                    || 1_700_000_000_000,
                    Persistence {
                        factory: Arc::new(factory.clone()),
                        retention: Default::default(),
                        metrics: Default::default(),
                    },
                    4,
                )
                .unwrap();
                let owners = Owners::new(
                    &Admin::new(),
                    writer,
                    node.to_string(),
                    "eu".into(),
                    "eu-1a".into(),
                    members(&views[node]),
                );
                (*node, owners)
            })
            .collect(),
    );

    let clients: Vec<_> = (0..CLIENTS)
        .map(|n| tokio::spawn(client(n, nodes.clone(), observed.clone())))
        .collect();
    // Membership: a node leaves or returns, and each node's view catches up after its own delay.
    let churning = {
        let (views, nodes, observed) = (views.clone(), nodes.clone(), observed.clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(thread_rng().gen_range(200..1500))).await;
                let mut ready: Vec<&'static str> = NODES
                    .iter()
                    .copied()
                    .filter(|_| thread_rng().gen_bool(0.7))
                    .collect();
                if ready.is_empty() {
                    ready.push(NODES[thread_rng().gen_range(0..NODES.len())]);
                }
                observed
                    .lock()
                    .unwrap()
                    .trace
                    .push(format!("members {ready:?}"));
                for node in NODES {
                    let (views, nodes, ready) = (views.clone(), nodes.clone(), ready.clone());
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(thread_rng().gen_range(0..1500))).await;
                        *views[node].lock().unwrap() = ready;
                        nodes[node].release_moved().await;
                    });
                }
            }
        })
    };
    let crashing = {
        let (factory, observed) = (factory.clone(), observed.clone());
        tokio::spawn(async move {
            for _ in 0..5 {
                sleep(Duration::from_millis(thread_rng().gen_range(500..4000))).await;
                factory.crash().unwrap();
                observed.lock().unwrap().trace.push("crash".into());
            }
        })
    };
    for client in clients {
        client.await.unwrap();
    }
    churning.abort();
    crashing.abort();
    // View updates the churn already scheduled land within 1.5 s; let them, so none overrides the settled view.
    sleep(Duration::from_secs(2)).await;

    // Settle on one owner; it must hold every key's last acknowledged write or a later one.
    for view in views.values() {
        *view.lock().unwrap() = NODES.to_vec();
    }
    let namespace = record();
    let owner = NODES
        .iter()
        .find(|node| nodes[*node].route(&namespace) == Route::Local)
        .expect("one node owns it");
    for node in NODES {
        nodes[node].release_moved().await;
    }
    sleep(bedrock_lib::replication::writer::FENCE_HOLD).await;
    // Every client has finished, so nothing else touches it.
    let observed = std::mem::take(&mut *observed.lock().unwrap());
    let max = *observed
        .acked
        .keys()
        .last()
        .expect("writes were acknowledged");
    for (key, (revision, write)) in &observed.last {
        // The settled owner may first wait out a fence hold; it must then serve.
        let mut tries = 0;
        let found = loop {
            match nodes[owner].get(&namespace, &key.to_be_bytes(), max).await {
                Ok((found, _)) => break found,
                Err(error) if tries < 100 => {
                    tries += 1;
                    let _ = error;
                    sleep(Duration::from_millis(100)).await;
                }
                Err(error) => panic!("the settled owner {owner} cannot serve k{key}: {error:?}"),
            }
        };
        let found = found.unwrap_or_else(|| panic!("k{key} lost its write {write} at {revision}"));
        let carried = u64::from_be_bytes(found.value.try_into().expect("8 bytes"));
        let carried_at = observed
            .acked
            .iter()
            .find_map(|(revision, w)| (*w == carried).then_some(*revision));
        assert!(
            carried_at.is_none_or(|at| at >= *revision),
            "k{key} holds write {carried} at {carried_at:?}, older than acknowledged {write} at {revision}"
        );
    }
    if std::env::var_os("BEDROCK_OWNERS_TRACE").is_some() {
        for line in &observed.trace {
            eprintln!("TRACE {line}");
        }
        eprintln!("TRACE END");
    }
    let mut hasher = DefaultHasher::new();
    observed.trace.hash(&mut hasher);
    let fenced = observed
        .trace
        .iter()
        .filter(|line| line.contains("Fenced"))
        .count();
    (hasher.finish(), fenced)
}

#[allow(clippy::disallowed_methods)] // A fresh OS thread resets madsim's seeded std map hashing.
fn run(seed: u64) -> (u64, usize) {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(async {
            // Simulated time: a run that cannot finish fails here with its seed rather than spinning.
            let observed = Arc::new(Mutex::new(Observed::default()));
            match tokio::time::timeout(Duration::from_secs(3600), scenario(observed.clone())).await {
                Ok(done) => done,
                Err(_) => {
                    let observed = observed.lock().unwrap();
                    for line in observed.trace.iter().rev().take(40).rev() {
                        eprintln!("TRACE {line}");
                    }
                    panic!("the scenario did not finish within an hour of simulated time");
                }
            }
        })
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: BEDROCK_OWNERS_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test namespace_owners -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

/// Runs `seed` in a child process and returns its trace hash and fenced writes. Replays are compared across processes: tokio seeds its
/// `watch` waker shards from a process-wide counter, so a second run inside one process may wake waiters differently.
#[allow(clippy::disallowed_types)] // This launcher isolates seeds before simulation starts.
fn in_child(seed: u64) -> (u64, usize) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "owners_hold_their_invariants_and_each_seed_replays",
            "--nocapture",
        ])
        .env("BEDROCK_OWNERS_SEED", seed.to_string())
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
        .find_map(|line| line.split_once("OWNERS "))
        .and_then(|(_, line)| {
            let (hash, fenced) = line.trim().split_once(' ')?;
            Some((hash.parse().ok()?, fenced.parse().ok()?))
        })
        .expect("the child prints its trace hash and fenced writes")
}

#[test]
fn owners_hold_their_invariants_and_each_seed_replays() {
    match std::env::var("BEDROCK_OWNERS_SEED") {
        Ok(seed) => {
            let (hash, fenced) = run(seed.parse().expect("seed must be a number"));
            println!("OWNERS {hash} {fenced}");
        }
        Err(_) => {
            let mut fenced = 0;
            for &seed in SEEDS {
                let run = in_child(seed);
                assert_eq!(run, in_child(seed), "seed {seed} did not replay");
                fenced += run.1;
            }
            // Otherwise the seeds never made two owners meet, and nothing above tested fencing.
            assert!(fenced > 0, "no seed fenced an old owner");
        }
    }
}
