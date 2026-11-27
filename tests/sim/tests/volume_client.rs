//! #112 / S-04: the volume client in Compute against three `bedrock-volume` slots over the simulated network.
//!
//! - Killing the holder at every append boundary, between commits or with one in flight, loses no acknowledged commit:
//!   the next holder waits out the lease and reads every acknowledged entry.
//! - A holder cut off from its slots stops serving within 4 s, and no other holder opens before it stops.
//! - A claim at an epoch not reserved at a quorum never opens: with one slot repaired and one down, no slot is claimed.
//! - Reads never return state older than the commit point, while the nearest slot falls behind.
//! - Deleting an actor whose holder crashed mid-commit waits out its lease, and no later claim opens it (#137 / S-05b).
//! - A seed replays its trace.
//!
//! The simulated network has no TLS, so calls carry no `Caller`; `crates/volume/tests/unit/serve.rs` checks callers.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_compute::actors::volume::{Error, FENCE, Host, Slot, Writer};
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::sim::{Fingerprint, SimEngine};
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume::volume_client::VolumeClient;
use bedrock_lib::volume::volume_server::VolumeServer;
use bedrock_lib::volume::{self as pb};
use bedrock_volume::serve::Service;
use bedrock_volume::store::{self, Store};
use madsim::net::NetSim;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle, Runtime};
use prost::Message;
use tokio::sync::oneshot;
use tokio::time::sleep;

const SLOT_IPS: [&str; 3] = ["10.0.1.1", "10.0.1.2", "10.0.1.3"];
const CELLS: [&str; 3] = ["cell-1", "cell-2", "cell-3"];
const COMMITS: u64 = 6;
const LETTERS: &[u8] = b"bcdefghijklmnopqrstuvwxyz";

fn actor(case: usize) -> pb::Actor {
    let letter = (LETTERS[case] as char).to_string();
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", letter.repeat(25)),
        kind: 0,
    }
}

fn slot_node(i: usize, engine: Arc<dyn StorageEngine>) -> NodeHandle {
    let name = format!("volume-{i}");
    Handle::current()
        .create_node()
        .name(&name)
        .ip(SLOT_IPS[i].parse().unwrap())
        .init(move || {
            let (engine, name) = (engine.clone(), name.clone());
            async move {
                let store = Arc::new(Store::new(engine));
                let service = Service::new(&Admin::new(), store, &name, CELLS[i], "dc1").unwrap();
                let owner = Owner {
                    node: name,
                    state: State::Ready,
                };
                *service.directory.write().unwrap() = BTreeMap::from([(0, vec![owner])]);
                tonic::transport::Server::builder()
                    .add_service(VolumeServer::new(service))
                    .serve(format!("{}:7000", SLOT_IPS[i]).parse().unwrap())
                    .await
                    .unwrap();
            }
        })
        .build()
}

fn slots() -> [Slot; 3] {
    std::array::from_fn(|i| Slot {
        cell: CELLS[i].into(),
        address: format!("{}:7000", SLOT_IPS[i]),
        identity: tls::Identity {
            service: "volume".into(),
            cell: CELLS[i].into(),
            node_id: format!("volume-{i}"),
            host: SLOT_IPS[i].into(),
        },
    })
}

/// A Compute node in the first slot's cell.
fn compute(n: usize) -> NodeHandle {
    Handle::current()
        .create_node()
        .name(format!("compute-{n}"))
        .ip(format!("10.0.0.{}", n + 10).parse().unwrap())
        .build()
}

fn host(name: &str) -> Arc<Host> {
    host_in(name, CELLS[0])
}

fn host_in(name: &str, cell: &str) -> Arc<Host> {
    let holder = pb::Holder {
        cell: cell.into(),
        host: name.into(),
    };
    Host::start(Arc::new(Peers::new(&Admin::new())), holder)
}

/// Opens, retrying while a previous holder's lease is live or a quorum is unreachable.
async fn open(host: &Arc<Host>, actor: pb::Actor) -> Writer {
    for _ in 0..60 {
        match Writer::open(host.clone(), actor.clone(), slots(), 0).await {
            Ok(writer) => return writer,
            Err(Error::Held(_) | Error::Unavailable(_)) => sleep(Duration::from_millis(500)).await,
            Err(error) => panic!("open failed: {error}"),
        }
    }
    panic!("no holder opened within the retries");
}

fn change(key: &str, n: u64) -> Vec<pb::Change> {
    vec![pb::Change {
        key: key.as_bytes().to_vec(),
        value: Some(n.to_be_bytes().to_vec()),
    }]
}

fn number(value: &pb::Value) -> Option<u64> {
    value
        .value
        .as_ref()
        .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
}

/// Kills the holder at every append boundary: after `boundary` commits, or with commit `boundary` in flight.
async fn holder_crashes(fingerprint: &Fingerprint, next: &mut usize) {
    let handle = Handle::current();
    for boundary in 0..=COMMITS {
        for in_flight in [false, true] {
            if in_flight && boundary == COMMITS {
                continue;
            }
            let case = *next;
            *next += 1;
            let acked = Arc::new(Mutex::new(Vec::new()));
            let (tx, rx) = oneshot::channel();
            let node = compute(2 * case);
            let recorded = acked.clone();
            node.spawn(async move {
                let host = host(&format!("h1-{case}"));
                let writer = open(&host, actor(case)).await;
                let mut tx = Some(tx);
                for n in 0..COMMITS {
                    let commit = async {
                        match writer.commit(change(&format!("k{n}"), n), None, n).await {
                            Ok(_) => recorded.lock().unwrap().push(n),
                            // Only the commit the crash interrupts may fail.
                            Err(error) => {
                                assert_eq!(
                                    n, boundary,
                                    "commit {n} before the crash failed: {error}"
                                )
                            }
                        }
                    };
                    if n == boundary {
                        if in_flight {
                            let delay = thread_rng().gen_range(0..20);
                            let crash = async {
                                sleep(Duration::from_millis(delay)).await;
                                let _ = tx.take().map(|tx| tx.send(()));
                                std::future::pending::<()>().await;
                            };
                            tokio::select! {
                                biased;
                                () = commit => {}
                                () = crash => {}
                            }
                        }
                        // The commit may have finished before the crash.
                        let _ = tx.take().map(|tx| tx.send(()));
                        loop {
                            std::future::pending::<()>().await;
                        }
                    }
                    commit.await;
                }
                let _ = tx.take().map(|tx| tx.send(()));
                std::future::pending::<()>().await;
            });
            rx.await.unwrap();
            handle.kill(node.id());
            let acked: Vec<u64> = acked.lock().unwrap().clone();
            assert!(acked.len() as u64 <= boundary + 1);

            let next_node = compute(2 * case + 1);
            let (epoch, committed) = next_node
                .spawn(async move {
                    let host = host(&format!("h2-{case}"));
                    let writer = open(&host, actor(case)).await;
                    let keys = (0..COMMITS).map(|n| format!("k{n}").into_bytes()).collect();
                    let values = writer.read(keys).await.unwrap();
                    for &n in &acked {
                        assert_eq!(
                            number(&values[n as usize]),
                            Some(n),
                            "case {case}: acknowledged commit {n} was lost"
                        );
                    }
                    assert!(
                        writer.committed() > acked.len() as u64,
                        "the no-op follows them"
                    );
                    (writer.epoch(), writer.committed())
                })
                .await
                .unwrap();
            handle.kill(next_node.id());
            fingerprint.mix(("holder crash", boundary, in_flight, epoch, committed));
        }
    }
}

/// A holder cut off from its slots stops serving within [`FENCE`], before any other holder opens.
async fn cut_off(fingerprint: &Fingerprint, next: &mut usize) {
    let case = *next;
    *next += 1;
    let first = compute(2 * case);
    let (ready, cut) = oneshot::channel();
    let fenced = first.spawn(async move {
        let host = host("cut-1");
        let writer = open(&host, actor(case)).await;
        writer.commit(change("k", 1), None, 1).await.unwrap();
        let _ = ready.send(());
        while writer.serving() {
            sleep(Duration::from_millis(50)).await;
        }
        let fenced = bedrock_lib::monotonic_now();
        let late = writer.commit(change("k", 2), None, 2).await;
        assert!(
            matches!(late, Err(Error::Fenced(_))),
            "a fenced writer commits nothing"
        );
        let read = writer.read(vec![b"k".to_vec()]).await;
        assert!(
            matches!(read, Err(Error::Fenced(_))),
            "a fenced writer reads nothing"
        );
        fenced
    });
    cut.await.unwrap();
    let cut_at = bedrock_lib::monotonic_now();
    NetSim::current().clog_node(first.id());
    let second = compute(2 * case + 1);
    let opened = second
        .spawn(async move {
            let host = host("cut-2");
            let writer = open(&host, actor(case)).await;
            let values = writer.read(vec![b"k".to_vec()]).await.unwrap();
            assert_eq!(number(&values[0]), Some(1));
            bedrock_lib::monotonic_now()
        })
        .await
        .unwrap();
    let fenced = fenced.await.unwrap();
    assert!(
        fenced.duration_since(cut_at) <= FENCE + Duration::from_millis(100),
        "stopped serving {:?} after the cut",
        fenced.duration_since(cut_at)
    );
    assert!(
        opened >= fenced,
        "a second holder opened while the first still served"
    );
    fingerprint.mix((
        "cut off",
        fenced.duration_since(cut_at),
        opened.duration_since(cut_at),
    ));
    NetSim::current().unclog_node(first.id());
    Handle::current().kill(first.id());
    Handle::current().kill(second.id());
}

/// With one slot repaired and one down, no epoch is reserved at a quorum, so nothing is claimed. Once the slot is back,
/// a claim opens above every reservation the failed attempts left.
async fn unreserved(
    fingerprint: &Fingerprint,
    next: &mut usize,
    engines: &[Arc<SimEngine>],
    nodes: &[NodeHandle],
) {
    let case = *next;
    *next += 1;
    let key = store::claim_key(&store::Actor::new(&actor(case)).unwrap());
    let repaired = pb::ClaimRecord {
        repaired: true,
        ..Default::default()
    };
    engines[1].put(&key, &repaired.encode_to_vec()).unwrap();
    let handle = Handle::current();
    handle.kill(nodes[2].id());
    let node = compute(2 * case);
    let failed = node
        .spawn(async move {
            Writer::open(host("unreserved"), actor(case), slots(), 0)
                .await
                .err()
        })
        .await
        .unwrap();
    assert!(matches!(failed, Some(Error::Unavailable(_))), "{failed:?}");
    let claim = |i: usize| {
        let bytes = engines[i].get(&key).unwrap().unwrap_or_default();
        pb::ClaimRecord::decode(bytes.as_slice()).unwrap()
    };
    assert_eq!(
        (claim(0).epoch, claim(1).epoch),
        (0, 0),
        "a slot promised an unreserved epoch"
    );
    let reserved = claim(0).reserved;
    assert!(reserved > 0);
    handle.restart(nodes[2].id());
    let epoch = node
        .spawn(async move { open(&host("unreserved"), actor(case)).await.epoch() })
        .await
        .unwrap();
    assert!(
        epoch > reserved,
        "claimed {epoch} at or below a reservation {reserved}"
    );
    fingerprint.mix(("unreserved", reserved, epoch));
    handle.kill(node.id());
}

/// A slot still promised to an older epoch can hold that epoch's uncommitted tail at sequences at or past the new
/// holder's commit point. A read from it would return a write that never committed, so the reader skips it.
async fn stale_epoch(
    fingerprint: &Fingerprint,
    next: &mut usize,
    engines: &[Arc<SimEngine>],
    nodes: &[NodeHandle],
) {
    let case = *next;
    *next += 1;
    let (first, second) = (compute(2 * case), compute(2 * case + 1));
    let slot_ids: Vec<_> = nodes.iter().map(NodeHandle::id).collect();
    let cut = |a, b| {
        NetSim::current().clog_link(a, b);
        NetSim::current().clog_link(b, a);
    };
    let (opened, wait_opened) = oneshot::channel();
    let (id, ids) = (first.id(), slot_ids.clone());
    first.spawn(async move {
        let writer = open(&host("old"), actor(case)).await;
        // Only slot 2 still hears this holder, so its next entry reaches that slot alone and never commits.
        for &slot in &ids[..2] {
            NetSim::current().clog_link(id, slot);
            NetSim::current().clog_link(slot, id);
        }
        let _ = opened.send(writer.epoch());
        let _ = writer.commit(change("ghost", 1), None, 1).await;
        std::future::pending::<()>().await;
    });
    let old_epoch = wait_opened.await.unwrap();
    sleep(Duration::from_secs(1)).await;
    Handle::current().kill(first.id());
    // The next holder, in slot 2's cell, claims without slot 2.
    cut(second.id(), slot_ids[2]);
    let (id, slot) = (second.id(), slot_ids[2]);
    let (committed, read) = second
        .spawn(async move {
            let writer = open(&host_in("new", CELLS[2]), actor(case)).await;
            // Past every call that was cut, so slot 2 never takes the claim.
            sleep(Duration::from_secs(3)).await;
            NetSim::current().unclog_link(id, slot);
            NetSim::current().unclog_link(slot, id);
            let read = writer.read(vec![b"ghost".to_vec()]).await.unwrap();
            (writer.committed(), number(&read[0]))
        })
        .await
        .unwrap();
    let key = store::claim_key(&store::Actor::new(&actor(case)).unwrap());
    let bytes = engines[2].get(&key).unwrap().unwrap();
    let claim = pb::ClaimRecord::decode(bytes.as_slice()).unwrap();
    let end = claim.end.unwrap_or_default();
    assert_eq!(claim.epoch, old_epoch, "slot 2 kept the old promise");
    assert!(
        end.seq >= committed,
        "slot 2's old tail reaches the new commit point"
    );
    assert_eq!(read, None, "a read returned a write that never committed");
    fingerprint.mix(("stale epoch", old_epoch, committed, end.seq));
    Handle::current().kill(second.id());
}

/// Two slots truncated through `flushed` (S-05's maintainer) and a third whose log ends exactly there: the next holder
/// recovers a log that starts after `flushed`, and must send the lagging slot the entry after it, which needs the
/// epoch of entry `flushed` from the recovery slot's answer.
async fn flushed_point(
    fingerprint: &Fingerprint,
    next: &mut usize,
    engines: &[Arc<SimEngine>],
    nodes: &[NodeHandle],
) {
    let case = *next;
    *next += 1;
    let first = compute(2 * case);
    let (id, lagging) = (first.id(), nodes[2].id());
    let (truncated, wait_truncated) = oneshot::channel();
    first.spawn(async move {
        let writer = open(&host("flush-1"), actor(case)).await;
        writer.commit(change("a", 1), None, 1).await.unwrap();
        let flushed = writer.committed();
        // Let slot 2 store entry `flushed` too, then cut it off.
        sleep(Duration::from_millis(200)).await;
        NetSim::current().clog_link(id, lagging);
        NetSim::current().clog_link(lagging, id);
        for n in 2..=4 {
            writer.commit(change("a", n), None, n).await.unwrap();
        }
        for ip in &SLOT_IPS[..2] {
            let mut client = VolumeClient::connect(format!("http://{ip}:7000"))
                .await
                .unwrap();
            let request = pb::TruncateRequest {
                actor: Some(actor(case)),
                through: flushed,
            };
            let answer = client.truncate(request).await.unwrap().into_inner();
            assert_eq!(answer.refused, pb::Refusal::None as i32);
        }
        let _ = truncated.send(flushed);
        std::future::pending::<()>().await;
    });
    let flushed = wait_truncated.await.unwrap();
    Handle::current().kill(first.id());
    NetSim::current().unclog_link(id, lagging);
    NetSim::current().unclog_link(lagging, id);
    let second = compute(2 * case + 1);
    let committed = second
        .spawn(async move {
            let writer = open(&host("flush-2"), actor(case)).await;
            writer.commit(change("a", 5), None, 5).await.unwrap();
            // Long enough for slot 2 to catch up and learn the commit point.
            sleep(Duration::from_secs(2)).await;
            writer.committed()
        })
        .await
        .unwrap();
    let key = store::claim_key(&store::Actor::new(&actor(case)).unwrap());
    let claim = |i: usize| {
        let bytes = engines[i].get(&key).unwrap().unwrap();
        pb::ClaimRecord::decode(bytes.as_slice()).unwrap()
    };
    assert!(
        flushed > 0 && claim(0).flushed == flushed,
        "slot 0 truncated"
    );
    assert_eq!(
        claim(2).end.unwrap_or_default().seq,
        committed,
        "the slot that ended at the flushed point caught up"
    );
    fingerprint.mix(("flushed point", flushed, committed));
    Handle::current().kill(second.id());
}

/// Reads at the commit point while seeded cuts leave the nearest slot behind; the SQLite chunk read and scan follow.
async fn reads(fingerprint: &Fingerprint, next: &mut usize, nodes: &[NodeHandle]) {
    let case = *next;
    *next += 1;
    let node = compute(2 * case);
    let (id, nearest) = (node.id(), nodes[0].id());
    let trace = node
        .spawn(async move {
            let host = host("reader");
            let writer = open(&host, actor(case)).await;
            let mut trace = Vec::new();
            for n in 1..=12u64 {
                let cut = thread_rng().gen_bool(0.3);
                if cut {
                    NetSim::current().clog_link(id, nearest);
                    NetSim::current().clog_link(nearest, id);
                }
                writer.commit(change("x", n), None, n).await.unwrap();
                NetSim::current().unclog_link(id, nearest);
                NetSim::current().unclog_link(nearest, id);
                let values = writer.read(vec![b"x".to_vec()]).await.unwrap();
                assert_eq!(
                    number(&values[0]),
                    Some(n),
                    "a read returned state older than min_seq"
                );
                trace.push((cut, writer.committed()));
            }
            let pages: Vec<pb::Change> = (64u32..70)
                .map(|p| pb::Change {
                    key: p.to_be_bytes().to_vec(),
                    value: Some(vec![p as u8]),
                })
                .collect();
            writer.commit(pages, None, 13).await.unwrap();
            let chunk = writer.read_chunk(66).await.unwrap();
            assert_eq!(chunk.len(), 64);
            assert_eq!(chunk[0].key, 64u32.to_be_bytes());
            let present: Vec<bool> = chunk.iter().map(|v| v.value.is_some()).collect();
            assert!(present[..6].iter().all(|p| *p) && present[6..].iter().all(|p| !*p));
            let (values, more) = writer.scan(Vec::new(), 3).await.unwrap();
            assert!(more);
            assert!(values.windows(2).all(|w| w[0].key < w[1].key));
            trace
        })
        .await
        .unwrap();
    fingerprint.mix(("reads", trace));
    Handle::current().kill(node.id());
}

/// The `committed_at` of every `DELETE` here.
const DELETED_AT: u64 = 77;

/// Deletes `actor`, retrying while a previous holder's lease is live or a quorum is unreachable.
async fn delete(host: &Arc<Host>, actor: pb::Actor) {
    for _ in 0..60 {
        match Writer::delete(host.clone(), actor.clone(), slots(), 0, DELETED_AT).await {
            Ok(()) => return,
            Err(Error::Held(_) | Error::Unavailable(_)) => sleep(Duration::from_millis(500)).await,
            Err(error) => panic!("delete failed: {error}"),
        }
    }
    panic!("no delete went through within the retries");
}

/// S-05b: a holder commits until it crashes at a seeded point, possibly mid-commit; another host deletes the actor once
/// the lease ran out. After that no claim opens it, deleting it again succeeds, and the tombstones carry the `DELETE`'s
/// `committed_at`, where the grace period starts.
async fn deleted(fingerprint: &Fingerprint, next: &mut usize, engines: &[Arc<SimEngine>]) {
    let case = *next;
    *next += 1;
    let first = compute(2 * case);
    first.spawn(async move {
        let host = host("del-1");
        let writer = open(&host, actor(case)).await;
        for n in 1.. {
            if writer.commit(change("k", n), None, n).await.is_err() {
                return;
            }
        }
    });
    sleep(Duration::from_millis(thread_rng().gen_range(500..1500))).await;
    Handle::current().kill(first.id());
    let second = compute(2 * case + 1);
    let opened = second
        .spawn(async move {
            let host = host("del-2");
            delete(&host, actor(case)).await;
            delete(&host, actor(case)).await;
            for _ in 0..60 {
                match Writer::open(host.clone(), actor(case), slots(), 0).await {
                    Err(Error::Deleted) => return false,
                    Err(Error::Held(_) | Error::Unavailable(_)) => {
                        sleep(Duration::from_millis(500)).await
                    }
                    Ok(_) => return true,
                    Err(error) => panic!("open failed: {error}"),
                }
            }
            panic!("no claim was answered within the retries");
        })
        .await
        .unwrap();
    assert!(!opened, "a claim opened a deleted actor");
    let tombstones = engines
        .iter()
        .map(|engine| {
            let store = Store::new(engine.clone() as Arc<dyn StorageEngine>);
            let at = |before| store.expired(0, 0, before).unwrap().len();
            (at(DELETED_AT), at(DELETED_AT + 1))
        })
        .filter(|counts| *counts == (0, 1))
        .count();
    assert!(
        tombstones >= 2,
        "two slots hold the tombstone from DELETED_AT"
    );
    fingerprint.mix(("deleted", opened));
    Handle::current().kill(second.id());
}

async fn scenario() -> u64 {
    let fingerprint = Fingerprint::new();
    let engines: Vec<Arc<SimEngine>> = (0..3)
        .map(|i| {
            let disk = SimEngine::new(&format!("volume-{i}"), Arc::new(|_| None), None, false);
            Arc::new(disk.recorded(fingerprint.clone()))
        })
        .collect();
    let nodes: Vec<NodeHandle> = (0..3)
        .map(|i| slot_node(i, engines[i].clone() as Arc<dyn StorageEngine>))
        .collect();
    // Slots treat every holder as live for their first lease after starting.
    sleep(Duration::from_secs(6)).await;
    let mut next = 0;
    holder_crashes(&fingerprint, &mut next).await;
    cut_off(&fingerprint, &mut next).await;
    reads(&fingerprint, &mut next, &nodes).await;
    stale_epoch(&fingerprint, &mut next, &engines, &nodes).await;
    flushed_point(&fingerprint, &mut next, &engines, &nodes).await;
    unreserved(&fingerprint, &mut next, &engines, &nodes).await;
    deleted(&fingerprint, &mut next, &engines).await;
    for node in &nodes {
        Handle::current().kill(node.id());
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
        eprintln!(
            "Replay: seed {seed}, RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test volume_client"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn holder_crashes_cut_offs_and_reads_keep_every_acknowledged_commit_and_seeds_replay() {
    let traces: Vec<u64> = (1..=3).map(run).collect();
    for (seed, trace) in (1..=3).zip(&traces) {
        assert_eq!(run(seed), *trace, "seed {seed} did not replay its trace");
    }
    assert!(
        traces.windows(2).any(|w| w[0] != w[1]),
        "seeds should differ"
    );
}
