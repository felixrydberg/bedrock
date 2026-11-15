//! #112 / S-04: the volume client in Compute against three `bedrock-volume` slots over the simulated network.
//!
//! - Killing the holder at every append boundary, between commits or with one in flight, loses no acknowledged commit:
//!   the next holder waits out the lease and reads every acknowledged entry.
//! - A holder cut off from its slots stops serving within 4 s, and no other holder opens before it stops.
//! - A claim at an epoch not reserved at a quorum never opens: with one slot repaired and one down, no slot is claimed.
//! - Reads never return state older than the commit point, while the nearest slot falls behind.
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
    let holder = pb::Holder {
        cell: CELLS[0].into(),
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
                        if writer
                            .commit(change(&format!("k{n}"), n), None, n)
                            .await
                            .is_ok()
                        {
                            recorded.lock().unwrap().push(n);
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
            matches!(late, Err(Error::Fenced)),
            "a fenced writer commits nothing"
        );
        let read = writer.read(vec![b"k".to_vec()]).await;
        assert!(
            matches!(read, Err(Error::Fenced)),
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
    unreserved(&fingerprint, &mut next, &engines, &nodes).await;
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
