//! #125 / S-17: restore after a DC is lost for good. Holders commit to three actors' slots while the range maintainer
//! flushes to the simulated bucket under S3 faults and restarts. At a seeded moment the DC is lost: its slots and
//! maintainer are gone, and only the bucket is left. The restore lists every actor from the lost cell's databases, and
//! each recorded actor is exactly its committed state at the recorded sequence, at or past what its slots had already
//! truncated: the loss is only the unflushed tail. A seed replays the same trace.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::actors::{Dc, Topology};
use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection::Metrics;
use bedrock_lib::placement::{Directory, Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::sim::Fingerprint;
use bedrock_lib::volume::{self as pb, Refusal};
use bedrock_volume::cold::db::{Databases, Memory, database};
use bedrock_volume::cold::readers::Readers;
use bedrock_volume::cold::{self, Cold, Placement, restore};
use bedrock_volume::store::{Actor, Store};
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Runtime;
use tokio::time::sleep;
use tonic::Status;

const ACTORS: usize = 3;
const ROUNDS: u64 = 80;
const KEYS: u64 = 4;

fn actor(i: usize) -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}{}a", (b'b' + i as u8) as char, "c".repeat(24)),
        kind: 0,
    }
}

/// Every actor's first slot is here (slot 0); the others truncate in process.
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

/// One actor's holder: its log, what each slot holds, and its commit point.
struct Holder {
    actor: pb::Actor,
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
            let Ok(outcome) = store.append(&Actor::new(&self.actor).unwrap(), &request) else {
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

/// The value each key holds after the first `seq` entries; deletes are absent.
fn state(log: &[pb::Entry], seq: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut keys = BTreeMap::new();
    for entry in &log[..seq as usize] {
        for change in &entry.changes {
            match &change.value {
                Some(value) => keys.insert(change.key.clone(), value.clone()),
                None => keys.remove(&change.key),
            };
        }
    }
    keys
}

/// The data keys the lost cell's database serving `actor` holds for it.
async fn stored(bucket: &Memory, actor: &pb::Actor) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let parsed = Actor::new(actor).unwrap();
    let (catalog, _) = bucket.catalog("cell-1").await.unwrap();
    let (_, (_, db)) = catalog.spans.range(..=parsed.hash()).next_back().unwrap();
    let prefix = cold::data_key(parsed.name(), b"");
    bucket
        .contents(&database("cell-1", db))
        .into_iter()
        .filter_map(|(k, v)| Some((k.strip_prefix(prefix.as_slice())?.to_vec(), v)))
        .collect()
}

/// The trace, and how many entries the restore lost across actors.
async fn scenario() -> (u64, u64) {
    let fingerprint = Fingerprint::new();
    let bucket = Memory::shared();
    let stores: [Arc<Store>; 3] = std::array::from_fn(|_| {
        Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))).with_max_unflushed(1_500))
    });
    let directory: Directory = BTreeMap::from([(
        0,
        vec![Owner {
            node: "volume-0".into(),
            state: State::Ready,
        }],
    )]);
    let mut holders: Vec<Holder> = (0..ACTORS)
        .map(|i| {
            let claim = Actor::new(&actor(i)).unwrap();
            for store in &stores {
                let holder = pb::Holder {
                    cell: "cell-1".into(),
                    host: "h".into(),
                };
                store.claim(&claim, &holder, 1, 0, |_| true).unwrap();
            }
            Holder {
                actor: actor(i),
                log: Vec::new(),
                next: [1; 3],
                commit: 0,
            }
        })
        .collect();

    // The maintainer flushes about every second; S3 faults and restarts come from the commit loop.
    let cold = Arc::new(std::sync::Mutex::new(Arc::new(maintainer(
        &stores, &bucket,
    ))));
    let flushing = {
        let (cold, directory) = (cold.clone(), directory.clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(thread_rng().gen_range(200..1500))).await;
                let current = cold.lock().unwrap().clone();
                current.tick(&directory).await;
            }
        })
    };

    // The DC is lost after a seeded number of rounds, possibly mid-flush.
    let lost_at = thread_rng().gen_range(ROUNDS / 2..ROUNDS);
    for n in 0..lost_at {
        match thread_rng().gen_range(0..12) {
            0 => bucket.fail(thread_rng().gen_range(1..4), thread_rng().gen_bool(0.5)),
            1 => *cold.lock().unwrap() = Arc::new(maintainer(&stores, &bucket)),
            _ => {}
        }
        let holder = &mut holders[thread_rng().gen_range(0..ACTORS)];
        let seq = holder.log.len() as u64 + 1;
        if holder.commit == seq - 1 {
            let key = format!("k{}", thread_rng().gen_range(0..KEYS)).into_bytes();
            let value = (n % 5 != 4).then(|| n.to_be_bytes().to_vec());
            holder.log.push(pb::Entry {
                seq,
                epoch: 1,
                committed_at: seq,
                changes: vec![pb::Change { key, value }],
                ..Default::default()
            });
        }
        let seq = holder.log.len() as u64;
        let lagging = thread_rng().gen_range(0..4);
        let mut acks = 0;
        for i in (0..3).filter(|i| *i != lagging) {
            acks += usize::from(holder.send(&stores[i], i));
        }
        if acks >= 2 {
            holder.commit = seq;
        }
        fingerprint.mix(("commit", n, lagging, acks));
        sleep(Duration::from_millis(thread_rng().gen_range(10..300))).await;
    }
    // The DC is gone: no slot or maintainer runs again. What each slot had truncated is what S3 must hold.
    flushing.abort();
    bucket.fail(0, false);
    let truncated: Vec<u64> = holders
        .iter()
        .map(|h| {
            let parsed = Actor::new(&h.actor).unwrap();
            stores
                .iter()
                .map(|s| s.read(&parsed, &[], 0).unwrap().0.slot.unwrap().flushed)
                .max()
                .unwrap()
        })
        .collect();

    let topology = Topology {
        region: "eu".into(),
        dcs: vec![Dc {
            name: "dc-1".into(),
            cells: vec!["cell-1".into()],
        }],
        ..Default::default()
    };
    let count = restore::restore(&bucket, &topology, "dc-1", "r1")
        .await
        .unwrap();
    let recorded = restore::restored(&bucket, &topology, "dc-1", "r1", None, 100)
        .await
        .unwrap();
    assert_eq!(count, recorded.len() as u64, "the record reads back");
    let recorded: BTreeMap<String, u64> = recorded
        .into_iter()
        .map(|(a, meta)| (a.id, meta.seq))
        .collect();
    let mut lost = 0;
    for (holder, truncated) in holders.iter().zip(truncated) {
        let seq = recorded.get(&holder.actor.id).copied().unwrap_or(0);
        assert!(
            seq >= truncated,
            "{}: restored at {seq}, but its slots had truncated through {truncated}",
            holder.actor.id
        );
        assert!(seq <= holder.commit, "only committed entries reach S3");
        if seq > 0 {
            assert_eq!(
                stored(&bucket, &holder.actor).await,
                state(&holder.log, seq),
                "{} restores its state at {seq}",
                holder.actor.id
            );
        }
        lost += holder.commit - seq;
        fingerprint.mix(("restored", seq, holder.commit));
    }
    (fingerprint.value(), lost)
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> (u64, u64) {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario())
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: seed {seed}, RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test actor_dc_restore"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn a_restore_after_dc_loss_loses_only_the_unflushed_tail_and_seeds_replay() {
    let traces: Vec<(u64, u64)> = (1..=8).map(run).collect();
    for (seed, trace) in (1..=8).zip(&traces) {
        assert_eq!(run(seed), *trace, "seed {seed} did not replay its trace");
    }
    assert!(
        traces.windows(2).any(|w| w[0] != w[1]),
        "seeds should differ"
    );
    assert!(
        traces.iter().any(|(_, lost)| *lost > 0),
        "some seeds lose the DC with an unflushed tail: {traces:?}"
    );
}
