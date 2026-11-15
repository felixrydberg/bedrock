//! #113 / S-05: the cold tier under seeded faults. A holder commits to an actor's three slots while the range maintainer
//! flushes to the simulated bucket; S3 commits fail, some after they became durable (a lost answer), and the maintainer
//! restarts. The range splits and merges by clone while a second maintainer, which never sees the directory change,
//! races the first. S3 never holds more than the announced commit point, every slot reads the committed state through
//! its overlay and the range database, and a seed replays the same trace.
//!
//! The slots are in-process stores: the slot protocol over the network is `volume.rs`'s; this checks flush, truncation
//! and cold reads.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection::Metrics;
use bedrock_lib::placement::{Directory, Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::sim::Fingerprint;
use bedrock_lib::volume::{self as pb, Refusal};
use bedrock_volume::cold::db::{Databases, Memory, database};
use bedrock_volume::cold::readers::Readers;
use bedrock_volume::cold::{self, Cold, Meta, Placement};
use bedrock_volume::store::{Actor, Store};
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Runtime;
use tokio::time::sleep;
use tonic::Status;

const COMMITS: u64 = 60;
const KEYS: u64 = 5;

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "c".repeat(25)),
        kind: 0,
    }
}

/// The actor's first slot is here (slot 0); the others truncate in process, refusing while behind.
struct Slots([Arc<Store>; 3]);

/// What `#[async_trait]` expands to; this crate does not depend on `async-trait`.
type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, Status>> + Send + 'a>>;

impl Placement for Slots {
    fn first_here<'a, 'b, 'c>(&'a self, _: &'b pb::Actor) -> Answer<'c, bool>
    where
        'a: 'c,
        'b: 'c,
    {
        Box::pin(async { Ok(true) })
    }

    fn truncate_others<'a, 'b, 'c>(&'a self, actor: &'b pb::Actor, through: u64) -> Answer<'c, bool>
    where
        'a: 'c,
        'b: 'c,
    {
        Box::pin(async move {
            let actor = Actor::new(actor).unwrap();
            let mut done = true;
            for store in &self.0[1..] {
                let answer = store.truncate(&actor, through).unwrap().answer;
                done &= answer.slot.unwrap().flushed >= through;
            }
            Ok(done)
        })
    }

    fn first_cell<'a, 'b, 'c>(&'a self, _: &'b pb::Actor) -> Answer<'c, String>
    where
        'a: 'c,
        'b: 'c,
    {
        Box::pin(async { Ok("cell-1".into()) })
    }
}

fn maintainer(stores: &[Arc<Store>; 3], bucket: &Memory) -> Cold {
    let databases = Arc::new(bucket.clone());
    let readers = Readers::new(databases.clone(), 4, &Metrics::new(&Admin::new()));
    Cold::new(
        stores[0].clone(),
        databases,
        Arc::new(Slots(stores.clone())),
        readers,
        "cell-1",
        "volume-0",
    )
}

struct Holder {
    log: Vec<pb::Entry>,
    next: [u64; 3],
    commit: u64,
}

impl Holder {
    /// Sends the slot its missing entries; whether it holds the whole log now.
    fn send(&mut self, store: &Store, i: usize) -> bool {
        loop {
            let from = self.next[i].max(1);
            let request = pb::AppendRequest {
                holder: Some(pb::Holder {
                    cell: "cell-1".into(),
                    host: "h".into(),
                }),
                epoch: 1,
                prev_epoch: u64::from(from > 1),
                entries: self.log[from as usize - 1..].to_vec(),
                commit: self.commit,
                ..Default::default()
            };
            // Backpressure refuses the append; the holder tries again later.
            let Ok(outcome) = store.append(&Actor::new(&actor()).unwrap(), &request) else {
                return false;
            };
            let slot = outcome.answer.slot.unwrap();
            match Refusal::try_from(outcome.answer.refused).unwrap() {
                Refusal::None => {
                    self.next[i] = self.log.len() as u64 + 1;
                    return true;
                }
                Refusal::LogGap => {
                    let end = slot.end.unwrap_or_default().seq.max(slot.flushed);
                    self.next[i] = (from - 1).min(end + 1);
                }
                other => panic!("a slot refused the holder: {other:?}"),
            }
        }
    }
}

/// The value each key holds after the first `seq` entries.
fn state(log: &[pb::Entry], seq: u64) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut keys = BTreeMap::new();
    for entry in &log[..seq as usize] {
        for change in &entry.changes {
            keys.insert(change.key.clone(), change.value.clone());
        }
    }
    keys
}

/// The actor's flushed sequence in the database the catalog names for it.
async fn stored_meta(bucket: &Memory) -> u64 {
    let actor = Actor::new(&actor()).unwrap();
    let (catalog, _) = bucket.catalog("cell-1").await.unwrap();
    let Some((_, (_, db))) = catalog.spans.range(..=actor.hash()).next_back() else {
        return 0;
    };
    bucket
        .contents(&database("cell-1", db))
        .get(&cold::meta_key(actor.name()))
        .and_then(|bytes| Meta::decode(bytes))
        .map_or(0, |meta| meta.seq)
}

/// Every database of the bucket holds at most `committed` for the actor: no clone or stale maintainer wrote past it.
fn assert_committed_only(bucket: &Memory, committed: u64) {
    let actor = Actor::new(&actor()).unwrap();
    for name in bucket.names() {
        let seq = bucket
            .contents(&name)
            .get(&cold::meta_key(actor.name()))
            .and_then(|bytes| Meta::decode(bytes))
            .map_or(0, |meta| meta.seq);
        assert!(
            seq <= committed,
            "{name} holds sequence {seq}, past the commit point"
        );
    }
}

/// The trace and how many range databases the cell created: more than one means clones ran.
async fn scenario() -> (u64, u64) {
    let fingerprint = Fingerprint::new();
    let bucket = Memory::shared();
    let stores: [Arc<Store>; 3] = std::array::from_fn(|_| {
        // A small bound, so a long S3 outage reaches backpressure.
        Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))).with_max_unflushed(1_500))
    });
    let claim = Actor::new(&actor()).unwrap();
    for store in &stores {
        let holder = pb::Holder {
            cell: "cell-1".into(),
            host: "h".into(),
        };
        store.claim(&claim, &holder, 1, 0, |_| true).unwrap();
    }
    let whole: Directory = BTreeMap::from([(
        0,
        vec![Owner {
            node: "volume-0".into(),
            state: State::Ready,
        }],
    )]);
    // The split puts the actor in the lower or the upper half, by seed.
    let at = if thread_rng().gen_bool(0.5) {
        claim.hash()
    } else {
        claim.hash().saturating_add(1)
    };
    let mut split = whole.clone();
    split.insert(at, whole[&0].clone());
    let directory = Arc::new(Mutex::new(whole.clone()));
    let cold = Arc::new(Mutex::new(Arc::new(maintainer(&stores, &bucket))));
    let committed = Arc::new(Mutex::new(0u64));

    // The maintainer: flushes about every second, and S3 never holds more than the holder announced.
    let flushing = {
        let (cold, bucket, committed, fingerprint) = (
            cold.clone(),
            bucket.clone(),
            committed.clone(),
            fingerprint.clone(),
        );
        let directory = directory.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(thread_rng().gen_range(200..1500))).await;
                let current = cold.lock().unwrap().clone();
                let seen = directory.lock().unwrap().clone();
                current.tick(&seen).await;
                assert_committed_only(&bucket, *committed.lock().unwrap());
                let meta = stored_meta(&bucket).await;
                fingerprint.mix(("flush", meta, bucket.commits()));
            }
        })
    };
    // A maintainer that never sees the split: it merges the range back whenever it flushes, racing the first.
    let stale = {
        let (stores, bucket, committed, whole) = (
            stores.clone(),
            bucket.clone(),
            committed.clone(),
            whole.clone(),
        );
        tokio::spawn(async move {
            let stale = maintainer(&stores, &bucket);
            loop {
                sleep(Duration::from_millis(thread_rng().gen_range(1000..4000))).await;
                stale.tick(&whole).await;
                assert_committed_only(&bucket, *committed.lock().unwrap());
            }
        })
    };

    let mut holder = Holder {
        log: Vec::new(),
        next: [1; 3],
        commit: 0,
    };
    for n in 0..COMMITS {
        match thread_rng().gen_range(0..10) {
            0 => bucket.fail(thread_rng().gen_range(1..4), thread_rng().gen_bool(0.5)),
            1 => *cold.lock().unwrap() = Arc::new(maintainer(&stores, &bucket)),
            2 => {
                let mut current = directory.lock().unwrap();
                *current = if current.len() == 1 {
                    split.clone()
                } else {
                    whole.clone()
                };
            }
            _ => {}
        }
        // A new entry once the last one committed; until then the holder resends it, as its window would.
        let seq = holder.log.len() as u64 + 1;
        if holder.commit == seq - 1 {
            let key = format!("k{}", thread_rng().gen_range(0..KEYS)).into_bytes();
            let value = (n % 7 != 6).then(|| n.to_be_bytes().to_vec());
            holder.log.push(pb::Entry {
                seq,
                epoch: 1,
                committed_at: seq,
                changes: vec![pb::Change { key, value }],
                ..Default::default()
            });
        }
        let seq = holder.log.len() as u64;
        // One slot at random lags behind this round.
        let lagging = thread_rng().gen_range(0..4);
        let mut acks = 0;
        for i in (0..3).filter(|i| *i != lagging) {
            acks += usize::from(holder.send(&stores[i], i));
        }
        if acks >= 2 {
            holder.commit = seq;
            *committed.lock().unwrap() = seq;
        }
        fingerprint.mix(("commit", n, lagging, acks));
        sleep(Duration::from_millis(thread_rng().gen_range(10..300))).await;
    }

    stale.abort();
    // Heal: S3 answers again and every slot catches up. Two no-ops: all three slots take the first, and the second
    // announces its commit, so every slot may truncate through it.
    bucket.fail(0, false);
    for _ in 0..2 {
        let seq = holder.log.len() as u64 + 1;
        holder.log.push(pb::Entry {
            seq,
            epoch: 1,
            committed_at: seq,
            kind: pb::entry::Kind::Noop as i32,
            ..Default::default()
        });
        for (i, store) in stores.iter().enumerate() {
            // A slot full of unflushed log waits for the maintainer.
            let mut tries = 0;
            while !holder.send(store, i) {
                tries += 1;
                assert!(tries < 30, "slot {i} never caught up");
                sleep(Duration::from_secs(1)).await;
            }
        }
        holder.commit = seq;
        *committed.lock().unwrap() = seq;
    }
    sleep(Duration::from_secs(10)).await;
    flushing.abort();
    let current = cold.lock().unwrap().clone();
    let directory = directory.lock().unwrap().clone();
    for _ in 0..3 {
        current.tick(&directory).await;
    }
    // The slots know the first no-op committed; the second is still unannounced.
    let commit = holder.commit - 1;
    // Backpressure may hold the holder back through an outage, but some entries commit.
    assert!(commit > 2, "the scenario committed entries");
    assert_eq!(stored_meta(&bucket).await, commit);
    let expected = state(&holder.log, commit);
    let keys: Vec<Vec<u8>> = (0..KEYS).map(|k| format!("k{k}").into_bytes()).collect();
    for (i, store) in stores.iter().enumerate() {
        let (answer, mut values, missing) = store.read(&claim, &keys, commit).unwrap();
        let slot = answer.slot.unwrap();
        assert_eq!(slot.flushed, commit, "slot {i} truncated through the flush");
        let cold_keys: Vec<_> = missing.iter().map(|&m| keys[m].clone()).collect();
        let found = current
            .get("cell-1", &actor(), slot.flushed, &cold_keys)
            .await
            .unwrap();
        for (m, value) in missing.into_iter().zip(found) {
            values[m].value = value;
        }
        for value in values {
            let want = expected.get(&value.key).cloned().flatten();
            assert_eq!(value.value, want, "slot {i} reads {:?}", value.key);
        }
        let end = slot.end.unwrap_or_default().seq;
        assert_eq!(end, commit + 1, "slot {i} keeps only the unannounced no-op");
    }
    let created = bucket.catalog("cell-1").await.unwrap().0.next;
    fingerprint.mix(("end", commit, bucket.commits(), created));
    (fingerprint.value(), created)
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> (u64, u64) {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario())
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: seed {seed}, RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test volume_cold"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn flushes_under_s3_faults_hold_only_committed_state_and_seeds_replay() {
    let traces: Vec<(u64, u64)> = (1..=8).map(run).collect();
    for (seed, trace) in (1..=8).zip(&traces) {
        assert_eq!(run(seed), *trace, "seed {seed} did not replay its trace");
    }
    assert!(
        traces.windows(2).any(|w| w[0] != w[1]),
        "seeds should differ"
    );
    assert!(
        traces.iter().any(|(_, created)| *created > 3),
        "some seeds split, merge and race clones: {traces:?}"
    );
}
