//! #125 / S-17: a planned DC change under load (`docs/spec/volume_dc.tla`). A holder in the old DC commits while a
//! mover on another Compute node hands the actor over: the holder releases or crashes at a seeded moment, the mover
//! commits `MOVE` in the old DC, waits until the old slots truncated through it (a stand-in maintainer truncates through
//! each slot's commit point, as it does once a flush is durable), and imports the new DC's slots there.
//!
//! - A claim in the new DC before the import is refused.
//! - The `MOVE` follows every acknowledged commit, so nothing acknowledged is lost, and S3 holds it.
//! - No claim in the old DC serves after the `MOVE`, and the new DC opens above it and commits: one DC serves.
//! - A seed replays its trace.
//!
//! S3's contents are not modelled here (`actor_dc_restore.rs` and `volume_cold.rs` check them), and the simulated
//! network has no TLS, so calls carry no `Caller`.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bedrock_compute::actors::volume::{self as client, Error, Host, Slot, Writer};
use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection::Metrics;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::sim::{Fingerprint, SimEngine};
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume::volume_client::VolumeClient;
use bedrock_lib::volume::volume_server::VolumeServer;
use bedrock_lib::volume::{self as pb};
use bedrock_volume::cold::db::Memory;
use bedrock_volume::cold::readers::Readers;
use bedrock_volume::cold::{Cold, Placement};
use bedrock_volume::serve::Service;
use bedrock_volume::store::Store;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle, Runtime};
use tokio::sync::oneshot;
use tokio::time::sleep;

const OLD: [&str; 3] = ["10.0.1.1", "10.0.1.2", "10.0.1.3"];
const NEW: [&str; 3] = ["10.0.2.1", "10.0.2.2", "10.0.2.3"];

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "m".repeat(25)),
        kind: 0,
    }
}

/// What `#[async_trait]` expands to; this crate does not depend on `async-trait`.
type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, tonic::Status>> + Send + 'a>>;

/// The new DC's placement: every actor is moving in. Nothing here flushes.
struct MovingIn;

impl Placement for MovingIn {
    fn first_here<'a, 'b, 'c>(&'a self, _: &'b pb::Actor) -> Answer<'c, bool>
    where
        'a: 'c,
        'b: 'c,
    {
        Box::pin(async { Ok(false) })
    }

    fn truncate_others<'a, 'b, 'c>(&'a self, _: &'b pb::Actor, _: u64) -> Answer<'c, bool>
    where
        'a: 'c,
        'b: 'c,
    {
        Box::pin(async { Ok(true) })
    }

    fn first_cell<'a, 'b, 'c>(&'a self, _: &'b pb::Actor) -> Answer<'c, String>
    where
        'a: 'c,
        'b: 'c,
    {
        Box::pin(async { Ok("new-0".into()) })
    }

    fn moving_in<'a, 'b, 'c>(&'a self, _: &'b pb::Actor, _: u64) -> Answer<'c, bool>
    where
        'a: 'c,
        'b: 'c,
    {
        Box::pin(async { Ok(true) })
    }
}

/// Slot `i` of the old DC, or of the new one, which knows the actor is moving in.
fn slot_node(new: bool, i: usize, engine: Arc<dyn StorageEngine>) -> NodeHandle {
    let (ip, side) = if new {
        (NEW[i], "new")
    } else {
        (OLD[i], "old")
    };
    let (name, cell) = (format!("{side}-volume-{i}"), format!("{side}-{i}"));
    Handle::current()
        .create_node()
        .name(&name)
        .ip(ip.parse().unwrap())
        .init(move || {
            let (engine, name, cell) = (engine.clone(), name.clone(), cell.clone());
            async move {
                let store = Arc::new(Store::new(engine));
                let dc = if new { "dc-new" } else { "dc-old" };
                let mut service =
                    Service::new(&Admin::new(), store.clone(), &name, &cell, dc).unwrap();
                if new {
                    let databases = Arc::new(Memory::shared());
                    let readers = Readers::new(databases.clone(), 4, &Metrics::new(&Admin::new()));
                    let cold =
                        Cold::new(store, databases, Arc::new(MovingIn), readers, &cell, &name);
                    service = service.with_cold(Arc::new(cold));
                }
                let owner = Owner {
                    node: name,
                    state: State::Ready,
                };
                *service.directory.write().unwrap() = BTreeMap::from([(0, vec![owner])]);
                tonic::transport::Server::builder()
                    .add_service(VolumeServer::new(service))
                    .serve(format!("{ip}:7000").parse().unwrap())
                    .await
                    .unwrap();
            }
        })
        .build()
}

fn slots(new: bool) -> [Slot; 3] {
    let (ips, side) = if new { (NEW, "new") } else { (OLD, "old") };
    std::array::from_fn(|i| Slot {
        cell: format!("{side}-{i}"),
        address: format!("{}:7000", ips[i]),
        identity: tls::Identity {
            service: "volume".into(),
            cell: format!("{side}-{i}"),
            node_id: format!("{side}-volume-{i}"),
            host: ips[i].into(),
        },
    })
}

fn compute(n: usize) -> NodeHandle {
    Handle::current()
        .create_node()
        .name(format!("compute-{n}"))
        .ip(format!("10.0.0.{}", n + 10).parse().unwrap())
        .build()
}

fn host(name: &str, cell: &str) -> Arc<Host> {
    let holder = pb::Holder {
        cell: cell.into(),
        host: name.into(),
    };
    Host::start(Arc::new(Peers::new(&Admin::new())), holder)
}

fn change(n: u64) -> Vec<pb::Change> {
    vec![pb::Change {
        key: b"k".to_vec(),
        value: Some(n.to_be_bytes().to_vec()),
    }]
}

/// Opens on `slots`, retrying while a lease is live or a quorum is unreachable.
async fn open(host: &Arc<Host>, new: bool) -> Writer {
    for _ in 0..60 {
        match Writer::open(host.clone(), actor(), slots(new), 0).await {
            Ok(writer) => return writer,
            Err(Error::Held(_) | Error::Unavailable(_)) => sleep(Duration::from_millis(500)).await,
            Err(error) => panic!("open failed: {error}"),
        }
    }
    panic!("no holder opened within the retries");
}

/// Truncates each old slot through its commit point every few hundred milliseconds, as the maintainer does once a
/// flush is durable.
async fn maintain() {
    loop {
        for ip in OLD {
            let Ok(mut client) = VolumeClient::connect(format!("http://{ip}:7000")).await else {
                continue;
            };
            let read = pb::ReadRequest {
                actor: Some(actor()),
                ..Default::default()
            };
            let Ok(reply) = client.read(read).await else {
                continue;
            };
            let committed = reply
                .into_inner()
                .answer
                .and_then(|a| a.slot)
                .map_or(0, |s| s.committed);
            let request = pb::TruncateRequest {
                actor: Some(actor()),
                through: committed,
            };
            let _ = client.truncate(request).await;
        }
        sleep(Duration::from_millis(300)).await;
    }
}

async fn scenario() -> (u64, bool) {
    let fingerprint = Fingerprint::new();
    let mut nodes = Vec::new();
    for new in [false, true] {
        for i in 0..3 {
            let side = if new { "new" } else { "old" };
            let disk = SimEngine::new(&format!("{side}-{i}"), Arc::new(|_| None), None, false);
            let engine = Arc::new(disk.recorded(fingerprint.clone()));
            nodes.push(slot_node(new, i, engine as Arc<dyn StorageEngine>));
        }
    }
    // Slots treat every holder as live for their first lease after starting.
    sleep(Duration::from_secs(6)).await;
    let maintainer = compute(9);
    maintainer.spawn(maintain());

    // The holder in the old DC commits until it is told to stop; it then releases, unless the seed crashes it.
    let acked = Arc::new(Mutex::new(Vec::<u64>::new()));
    let (stop, stopped) = oneshot::channel::<()>();
    let (released, wait_released) = oneshot::channel::<()>();
    let crash = thread_rng().gen_bool(0.5);
    let holding = compute(0);
    {
        let acked = acked.clone();
        holding.spawn(async move {
            let writer = open(&host("old-holder", "old-0"), false).await;
            let mut stopped = std::pin::pin!(stopped);
            for n in 1.. {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    result = writer.commit(change(n), None, n) => match result {
                        Ok(seq) => acked.lock().unwrap().push(seq),
                        Err(_) => break,
                    },
                }
                sleep(Duration::from_millis(thread_rng().gen_range(10..200))).await;
            }
            let _ = writer.release(0).await;
            let _ = released.send(());
            std::future::pending::<()>().await;
        });
    }

    // A claim in the new DC before the import is refused.
    let early = compute(1)
        .spawn(async move { Writer::open(host("early", "new-0"), actor(), slots(true), 0).await })
        .await
        .unwrap();
    assert!(
        matches!(&early, Err(Error::Unavailable(detail)) if detail.contains("not imported")),
        "a new-DC claim before the import: {:?}",
        early.as_ref().err()
    );

    sleep(Duration::from_millis(thread_rng().gen_range(500..3000))).await;
    if crash {
        Handle::current().kill(holding.id());
    } else {
        let _ = stop.send(());
        let _ = wait_released.await;
    }
    let acked = acked.lock().unwrap().clone();
    assert!(!acked.is_empty(), "the holder committed under load");

    // The mover retries while the crashed holder's lease is live.
    let (epoch, seq) = compute(2)
        .spawn(async move {
            let mover = host("mover", "old-1");
            for _ in 0..60 {
                match client::handover(mover.clone(), &actor(), slots(false), &slots(true), 0).await
                {
                    Ok(point) => return point,
                    Err(error @ (Error::Held(_) | Error::Unavailable(_))) => {
                        eprintln!("handover retries: {error}");
                        sleep(Duration::from_millis(500)).await
                    }
                    Err(error) => panic!("handover failed: {error}"),
                }
            }
            panic!("the handover never finished");
        })
        .await
        .unwrap();
    let last = *acked.last().unwrap();
    assert!(
        seq > last,
        "MOVE at {seq} follows every acknowledged commit, the last at {last}"
    );

    // The old DC never serves again; the new DC opens above the MOVE and commits.
    let again = compute(3)
        .spawn(
            async move { Writer::open(host("old-again", "old-2"), actor(), slots(false), 0).await },
        )
        .await
        .unwrap();
    assert!(
        matches!(again, Err(Error::Moved)),
        "an old-DC claim after the move: {:?}",
        again.err()
    );
    let (new_epoch, committed) = compute(4)
        .spawn(async move {
            let writer = open(&host("new-holder", "new-0"), true).await;
            let committed = writer.commit(change(1000), None, 1000).await.unwrap();
            (writer.epoch(), committed)
        })
        .await
        .unwrap();
    assert!(
        new_epoch > epoch,
        "the new DC claims above the MOVE's epoch"
    );
    assert!(
        committed > seq + 1,
        "the new DC continues after the MOVE and its no-op"
    );
    // Moving back to the old DC is refused before any MOVE commits, and the new DC still serves.
    let back = compute(5)
        .spawn(async move {
            client::handover(
                host("back", "new-1"),
                &actor(),
                slots(true),
                &slots(false),
                0,
            )
            .await
        })
        .await
        .unwrap();
    assert!(
        matches!(&back, Err(Error::Rpc(s)) if s.code() == tonic::Code::FailedPrecondition),
        "a return to the old DC: {back:?}"
    );
    let after = compute(6)
        .spawn(async move {
            let writer = open(&host("new-again", "new-1"), true).await;
            writer.commit(change(1001), None, 1001).await.unwrap()
        })
        .await
        .unwrap();
    assert!(
        after > committed,
        "the new DC still serves after the refusal"
    );
    fingerprint.mix((
        "handover",
        crash,
        acked.len(),
        epoch,
        seq,
        new_epoch,
        committed,
    ));
    for node in &nodes {
        Handle::current().kill(node.id());
    }
    (fingerprint.value(), crash)
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64) -> (u64, bool) {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario())
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: seed {seed}, RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test actor_dc_change"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn a_planned_dc_change_under_load_keeps_every_acknowledged_commit_and_one_serving_dc() {
    let traces: Vec<(u64, bool)> = (1..=4).map(run).collect();
    for (seed, trace) in (1..=4).zip(&traces) {
        assert_eq!(run(seed), *trace, "seed {seed} did not replay its trace");
    }
    assert!(
        traces.windows(2).any(|w| w[0] != w[1]),
        "seeds should differ"
    );
    assert!(
        traces.iter().any(|t| t.1) && traces.iter().any(|t| !t.1),
        "some seeds crash the holder and some release it: {traces:?}"
    );
}
