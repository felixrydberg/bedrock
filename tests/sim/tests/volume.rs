//! #111 / S-03: one actor's three slots on `bedrock-volume` services over the simulated network and disks. A holder
//! commits while seeded faults crash and restart one slot at a time: between appends, after a slot's fsync but before
//! its answer, or with the append still in flight, so the disk loses it. Every acknowledged commit survives into the
//! next holder's recovered log, and a seed replays the same trace.
//!
//! The simulated network has no TLS, so calls carry no `Caller`; `crates/volume/tests/unit/serve.rs` checks callers.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::sim::{Fingerprint, SimEngine};
use bedrock_lib::volume::volume_client::VolumeClient;
use bedrock_lib::volume::volume_server::VolumeServer;
use bedrock_lib::volume::{self as pb, Refusal};
use bedrock_volume::serve::{LEASE, Service};
use bedrock_volume::store::Store;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::time::{sleep, timeout};

const SLOTS: [&str; 3] = ["10.0.1.1", "10.0.1.2", "10.0.1.3"];
const COMMITS: u64 = 24;
const CALL: Duration = Duration::from_millis(500);

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "c".repeat(25)),
        kind: 0,
    }
}

/// A slot node whose simulated disk outlives its restarts. The store syncs every batch, so what the disk holds survives
/// a restart; a crash with an append in flight takes that batch back ([`SimEngine::crash`]).
fn slot(i: usize, engine: Arc<dyn StorageEngine>) -> NodeHandle {
    let name = format!("volume-{i}");
    Handle::current()
        .create_node()
        .name(&name)
        .ip(SLOTS[i].parse().unwrap())
        .init(move || {
            let (engine, name) = (engine.clone(), name.clone());
            async move {
                let store = Arc::new(Store::new(engine));
                let service = Service::new(&Admin::new(), store, &name, "cell-1", "dc1").unwrap();
                let owner = Owner {
                    node: name,
                    state: State::Ready,
                };
                *service.directory.write().unwrap() = BTreeMap::from([(0, vec![owner])]);
                tonic::transport::Server::builder()
                    .add_service(VolumeServer::new(service))
                    .serve(format!("{}:7000", SLOTS[i]).parse().unwrap())
                    .await
                    .unwrap();
            }
        })
        .build()
}

async fn client(i: usize) -> Option<VolumeClient<tonic::transport::Channel>> {
    let address = format!("http://{}:7000", SLOTS[i]);
    timeout(CALL, VolumeClient::connect(address))
        .await
        .ok()?
        .ok()
}

fn refused(answer: &pb::Answer) -> Refusal {
    Refusal::try_from(answer.refused).unwrap()
}

fn slot_of(answer: &pb::Answer) -> pb::Slot {
    answer.slot.clone().unwrap()
}

struct Holder {
    name: pb::Holder,
    epoch: u64,
    log: Vec<pb::Entry>,
    next: [u64; 3],
    commit: u64,
}

impl Holder {
    /// Sends the log from the slot's next entry, backing off on a gap; `None` when the slot did not answer.
    async fn send(&mut self, i: usize) -> Option<pb::Answer> {
        loop {
            let from = self.next[i].max(1);
            let request = pb::AppendRequest {
                actor: Some(actor()),
                holder: Some(self.name.clone()),
                epoch: self.epoch,
                version: 0,
                prev_epoch: if from > 1 {
                    self.log[from as usize - 2].epoch
                } else {
                    0
                },
                entries: self.log[from as usize - 1..].to_vec(),
                commit: self.commit,
            };
            let answer = timeout(CALL, client(i).await?.append(request))
                .await
                .ok()?
                .ok()?
                .into_inner();
            match refused(&answer) {
                Refusal::LogGap => {
                    let end = slot_of(&answer).end.unwrap_or_default().seq;
                    self.next[i] = (from - 1).min(end + 1);
                }
                Refusal::None => {
                    self.next[i] = self.log.len() as u64 + 1;
                    return Some(answer);
                }
                _ => return Some(answer),
            }
        }
    }

    /// Sends to every slot not in `skip`; commits once two voting slots hold the whole log.
    async fn replicate(&mut self, skip: Option<usize>) -> bool {
        let mut acks = 0;
        for i in (0..3).filter(|i| Some(*i) != skip) {
            if let Some(answer) = self.send(i).await
                && refused(&answer) == Refusal::None
                && slot_of(&answer).voting
            {
                acks += 1;
            }
        }
        if acks >= 2 {
            self.commit = self.log.len() as u64;
        }
        acks >= 2
    }
}

/// Reserve at a quorum, claim at a quorum, recover the most advanced log and commit a no-op. Retries while slots are
/// down or still treat the last holder's lease as live.
async fn open(name: &str, fingerprint: &Fingerprint) -> Holder {
    let holder = pb::Holder {
        cell: "cell-1".into(),
        host: name.into(),
    };
    let mut seen = 0;
    loop {
        let epoch = seen + 1;
        let mut reserved = 0;
        for i in 0..3 {
            let Some(mut c) = client(i).await else {
                continue;
            };
            let request = pb::ReserveRequest {
                actor: Some(actor()),
                epoch,
                version: 0,
            };
            if let Ok(Ok(answer)) = timeout(CALL, c.reserve(request)).await {
                let answer = answer.into_inner();
                seen = seen.max(slot_of(&answer).reserved);
                reserved += usize::from(refused(&answer) == Refusal::None);
            }
        }
        let mut granted = Vec::new();
        if reserved >= 2 {
            for i in 0..3 {
                let Some(mut c) = client(i).await else {
                    continue;
                };
                let request = pb::ClaimRequest {
                    actor: Some(actor()),
                    holder: Some(holder.clone()),
                    epoch,
                    version: 0,
                };
                if let Ok(Ok(answer)) = timeout(CALL, c.claim(request)).await {
                    let answer = answer.into_inner();
                    if refused(&answer) == Refusal::None && slot_of(&answer).voting {
                        let end = slot_of(&answer).end.unwrap_or_default();
                        granted.push((end.epoch, end.seq, i));
                    }
                }
            }
        }
        fingerprint.mix(("open", name, epoch, reserved, &granted));
        if granted.len() >= 2 {
            let (_, end, from) = granted.into_iter().max().unwrap();
            // The claimant reads the most advanced log through `Log`, which only the promised holder may read.
            let mut c = client(from).await.expect("a granting slot answers");
            let reply = c
                .log(pb::LogRequest {
                    actor: Some(actor()),
                    holder: Some(holder.clone()),
                    epoch,
                    from: 1,
                    limit: 1000,
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(refused(&reply.answer.unwrap()), Refusal::None);
            assert!(!reply.more);
            let mut log = reply.entries;
            assert_eq!(log.len() as u64, end, "the log ends where the claim said");
            let mut noop = pb::Entry {
                seq: log.len() as u64 + 1,
                epoch,
                kind: pb::entry::Kind::Noop as i32,
                ..Default::default()
            };
            noop.committed_at = noop.seq;
            log.push(noop);
            let mut opened = Holder {
                name: holder.clone(),
                epoch,
                log,
                next: [1; 3],
                commit: 0,
            };
            if opened.replicate(None).await {
                return opened;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn scenario() -> u64 {
    let fingerprint = Fingerprint::new();
    // Fault-free disks that can lose their last batches in a crash.
    let engines: Vec<Arc<SimEngine>> = (0..3)
        .map(|i| {
            let disk = SimEngine::new(&format!("volume-{i}"), Arc::new(|_| None), None, true);
            Arc::new(disk.recorded(fingerprint.clone()))
        })
        .collect();
    let nodes: Vec<NodeHandle> = (0..3)
        .map(|i| slot(i, engines[i].clone() as Arc<dyn StorageEngine>))
        .collect();
    let handle = Handle::current();
    let holder = handle
        .create_node()
        .name("compute")
        .ip("10.0.0.9".parse().unwrap())
        .build();
    let check = fingerprint.clone();
    let ids: Vec<_> = nodes.iter().map(NodeHandle::id).collect();
    holder
        .spawn(async move {
            let fingerprint = check;
            let handle = Handle::current();
            sleep(Duration::from_millis(100)).await;
            let mut h1 = open("h1", &fingerprint).await;
            let mut acked = h1.log.clone();
            let mut down: Option<usize> = None;
            for n in 0..COMMITS {
                // Seeded faults: restart the slot that is down, or crash one, before or after its fsync.
                let roll = thread_rng().gen_range(0..6);
                match (down, roll) {
                    (Some(i), 0 | 1) => {
                        handle.restart(ids[i]);
                        down = None;
                    }
                    (None, 0) => {
                        let i = thread_rng().gen_range(0..3);
                        handle.kill(ids[i]);
                        down = Some(i);
                    }
                    _ => {}
                }
                let seq = h1.log.len() as u64 + 1;
                h1.log.push(pb::Entry {
                    seq,
                    epoch: h1.epoch,
                    committed_at: seq,
                    changes: vec![pb::Change {
                        key: format!("k{n}").into_bytes(),
                        value: Some(n.to_be_bytes().to_vec()),
                    }],
                    ..Default::default()
                });
                if down.is_none() && roll >= 4 {
                    let i = thread_rng().gen_range(0..3);
                    let written = h1
                        .send(i)
                        .await
                        .is_some_and(|answer| refused(&answer) == Refusal::None);
                    // Roll 5: the slot fsyncs the append and dies before its answer counts. Roll 4: it dies with the
                    // append still in flight, so the disk loses the batch and the answer never counts.
                    if roll == 4 && written {
                        engines[i].crash(1);
                    }
                    handle.kill(ids[i]);
                    down = Some(i);
                }
                let committed = h1.replicate(down).await;
                fingerprint.mix(("commit", n, down, committed));
                if committed {
                    acked = h1.log.clone();
                }
            }
            // h1 dies. The down slot restarts; the next holder waits out h1's lease and recovers.
            if let Some(i) = down {
                handle.restart(ids[i]);
            }
            sleep(LEASE + Duration::from_secs(1)).await;
            let h2 = open("h2", &fingerprint).await;
            assert!(acked.len() > 1, "the scenario committed something");
            assert_eq!(
                &h2.log[..acked.len()],
                acked.as_slice(),
                "an acknowledged commit was lost"
            );
            fingerprint.mix(("recovered", h2.epoch, h2.log.len()));
        })
        .await
        .unwrap();
    for id in nodes.iter().map(NodeHandle::id) {
        handle.kill(id);
    }
    fingerprint.value()
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> u64 {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario())
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!("Replay: seed {seed}, RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test volume");
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn slot_crashes_lose_no_acknowledged_commit_and_seeds_replay() {
    let traces: Vec<u64> = (1..=6).map(run).collect();
    for (seed, trace) in (1..=6).zip(&traces) {
        assert_eq!(run(seed), *trace, "seed {seed} did not replay its trace");
    }
    assert!(
        traces.windows(2).any(|w| w[0] != w[1]),
        "seeds should differ"
    );
}
