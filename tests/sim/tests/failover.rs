//! #41 / B-12b acceptance: a relay's subscribers move to a sibling that is already warm, at every tier. Real relay
//! sessions and sibling streams, with one leaf following a namespace and a hash range. Replay one seed with
//! `BEDROCK_FAILOVER_SEED=42` and the documented simulation flags.
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::placement::hash_of;
use bedrock_lib::relays::client::{Selection, Session, Target};
use bedrock_lib::relays::replay::{Replay, ReplayConfig};
use bedrock_lib::relays::siblings::{self, Siblings};
use bedrock_lib::relays::{Hub, IDLE};
use bedrock_lib::replication::Operation;
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::namespace::memory::MemoryFactory;
use bedrock_lib::storage::{StorageEngine, l2_key};
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use madsim::runtime::{Handle, NodeHandle, Runtime};
use madsim::task::NodeId;
use tokio::time::sleep;

const WRITER: &str = "10.0.3.1:7000";
const GRACE: Duration = Duration::from_secs(20);
const DEADLINE: Duration = Duration::from_secs(120);
/// The leaf's range: the lower half of the hash space.
const HALF: u64 = 1 << 63;
const CELL: u32 = 1;
const REGION: u32 = 3;

fn peers() -> Arc<Peers> {
    Arc::new(Peers::new(&Admin::new()))
}

fn node(name: &str, address: &str) -> NodeHandle {
    let socket: std::net::SocketAddr = address.parse().unwrap();
    Handle::current()
        .create_node()
        .name(name)
        .ip(socket.ip())
        .build()
}

#[derive(Clone)]
struct Relay {
    id: NodeId,
    hub: Hub,
    replay: Arc<Replay>,
}

impl Relay {
    fn start(name: &str, address: &str, tier: u32, upstream: &str, siblings: &[String]) -> Self {
        let replay = Arc::new(
            Replay::open(Arc::new(MemoryEngine::new(name)), ReplayConfig::default()).unwrap(),
        );
        let others: Vec<String> = siblings.iter().filter(|s| *s != address).cloned().collect();
        let peers = peers();
        let hub = Hub::relay(
            name.into(),
            tier,
            peers.clone(),
            vec![upstream.into()],
            others.clone(),
            Some(replay.clone()),
        );
        let handle = node(name, address);
        let socket = address.parse().unwrap();
        let serving = hub.clone();
        handle.spawn(async move {
            let upstream = serving.start();
            let (_siblings, watching) = Siblings::start(serving.clone(), peers, others, GRACE);
            tonic::transport::Server::builder()
                .add_service(pb::relay_server::RelayServer::new(serving.clone()))
                .add_service(pb::siblings_server::SiblingsServer::new(siblings::Server(
                    serving,
                )))
                .serve(socket)
                .await
                .unwrap();
            upstream.abort();
            watching.abort();
        });
        Self {
            id: handle.id(),
            hub,
            replay,
        }
    }

    /// What its replay log advertises for each stream the leaf follows.
    fn ranges(&self) -> Vec<Vec<(u64, u64)>> {
        [("auth", "keys"), ("cdn", "pages")]
            .map(|(dataset, namespace)| self.replay.ranges(dataset, namespace))
            .to_vec()
    }
}

/// Live follows and recovery requests a hub has received for what the leaf follows.
fn requests(hub: &Hub) -> [(u64, u64); 2] {
    ["auth", "cdn"].map(|dataset| hub.requests(dataset))
}

async fn until(what: &str, within: Duration, done: impl Fn() -> bool) {
    tokio::time::timeout(within, async {
        while !done() {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// A `cdn` key inside the leaf's range.
fn inside(revision: u64) -> Vec<u8> {
    (0u64..)
        .map(|i| format!("{revision}-{i}").into_bytes())
        .find(|key| hash_of("cdn", "pages", key) < HALF)
        .unwrap()
}

/// Commits revisions `first..=last` to both streams.
async fn commit(writer: &Arc<Writer>, first: u64, last: u64) {
    for revision in first..=last {
        for (dataset, namespace, key) in [
            ("auth", "keys", b"key".to_vec()),
            ("cdn", "pages", inside(revision)),
        ] {
            let write = Write {
                dataset: dataset.into(),
                namespace: namespace.into(),
                key,
                operation: Operation::Set(revision.to_be_bytes().to_vec()),
                idempotency_key: None,
            };
            assert_eq!(writer.commit(write).await.unwrap(), revision);
        }
    }
}

/// The leaf applied both streams through `revision`.
async fn caught_up(session: &Session, engine: &MemoryEngine, revision: u64) {
    tokio::time::timeout(DEADLINE, session.wait_for("auth", "keys", revision))
        .await
        .unwrap_or_else(|_| panic!("revision {revision} stalled: {:?}", session.error()));
    let key = inside(revision);
    let stored = l2_key(hash_of("cdn", "pages", &key), "pages", &key);
    until("the range's latest key", DEADLINE, || {
        engine.get(&stored).unwrap().is_some()
    })
    .await;
}

/// Fails over between three siblings at `tier`, below one relay per tier above it, and returns what the leaf saw.
async fn scenario(tier: u32) -> Vec<String> {
    let writer = Writer::open(
        || 1_700_000_000_000,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    let source = Hub::writer("writer".into(), writer.clone(), peers());
    let serving = source.clone();
    let writer_node = node("writer", WRITER);
    writer_node.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(serving))
            .serve(WRITER.parse().unwrap())
            .await
            .unwrap();
    });
    let (mut upstream, mut parent) = (WRITER.to_owned(), source);
    for rank in (tier + 1..=REGION).rev() {
        let address = format!("10.0.3.{}:7000", 10 * rank);
        let relay = Relay::start(&format!("upper-{rank}"), &address, rank, &upstream, &[]);
        (upstream, parent) = (address, relay.hub);
    }
    let addresses: Vec<String> = (1..=3).map(|i| format!("10.0.3.{}:7000", 40 + i)).collect();
    let relays: Vec<Relay> = addresses
        .iter()
        .enumerate()
        .map(|(i, address)| {
            Relay::start(&format!("relay-{i}"), address, tier, &upstream, &addresses)
        })
        .collect();

    let leaf = node("leaf", "10.0.3.50:7000");
    let result = leaf.spawn(async move {
        let mut trace = Vec::new();
        let engine = Arc::new(MemoryEngine::new("leaf"));
        let session = Session::start(
            peers(),
            addresses.clone(),
            vec![
                Target {
                    selection: Selection {
                        dataset: "auth".into(),
                        namespace: "keys".into(),
                    },
                    engine: engine.clone(),
                    prefix: b"auth\0".to_vec(),
                    span: None,
                },
                Target {
                    selection: Selection {
                        dataset: "cdn".into(),
                        namespace: String::new(),
                    },
                    engine: engine.clone(),
                    prefix: Vec::new(),
                    span: Some((0, HALF)),
                },
            ],
        )
        .unwrap();

        // Subscribers sit on one relay, and its idle siblings follow the same streams.
        until("every sibling to follow", DEADLINE, || {
            requests(&parent).iter().all(|(follows, _)| *follows >= 3)
        })
        .await;
        let serving = |relay: &&Relay| !relay.hub.interests().is_empty();
        let first = relays
            .iter()
            .find(serving)
            .expect("a relay has the leaf")
            .clone();
        assert_eq!(relays.iter().filter(serving).count(), 1);
        commit(&writer, 1, 10).await;
        caught_up(&session, &engine, 10).await;
        until("every replay log to hold the same ranges", DEADLINE, || {
            relays.iter().all(|r| r.ranges() == vec![vec![(1, 10)]; 2])
        })
        .await;

        // Its subscribers move with no new upstream follow and no recovery request.
        let before = requests(&parent);
        let killed = tokio::time::Instant::now();
        Handle::current().kill(first.id);
        commit(&writer, 11, 20).await;
        caught_up(&session, &engine, 20).await;
        let took = killed.elapsed();
        assert_eq!(requests(&parent), before, "failover reached the upper tier");
        let alive = |relay: &&Relay| relay.id != first.id;
        let next = relays.iter().filter(alive).find(serving).unwrap().clone();
        assert!(
            requests(&next.hub)
                .iter()
                .all(|(_, recoveries)| *recoveries == 0),
            "the sibling's replay window covers the leaf's cursors",
        );
        let index = |relay: &Relay| relays.iter().position(|r| r.id == relay.id).unwrap();
        trace.push(format!(
            "relay {} to {} in {took:?}: {:?}",
            index(&first),
            index(&next),
            next.ranges(),
        ));

        // The dead relay's interests expired; those of the relay now serving keep the last sibling warm.
        let other = relays
            .iter()
            .find(|relay| relay.id != first.id && relay.id != next.id)
            .unwrap()
            .clone();
        sleep(IDLE + GRACE + Duration::from_secs(2)).await;
        assert!(
            !other.hub.desired().0.is_empty(),
            "the serving relay shares"
        );

        // A crashed sibling's interests stay for the grace period, however soon the crash is noticed, then go.
        Handle::current().kill(next.id);
        session.stop().await;
        sleep(GRACE - Duration::from_secs(1)).await;
        assert!(!other.hub.desired().0.is_empty(), "dropped within grace");
        until(
            "the crashed sibling's interests to expire",
            IDLE + Duration::from_secs(3),
            || {
                let (namespaces, datasets) = other.hub.desired();
                namespaces.is_empty() && datasets.is_empty()
            },
        )
        .await;
        trace.push(format!(
            "relay {} cold after {:?}",
            index(&other),
            killed.elapsed(),
        ));
        trace
    });
    result.await.unwrap()
}

#[allow(clippy::disallowed_methods)] // A fresh test thread resets the simulator's seeded std map hashing.
fn run(seed: u64, tier: u32) -> Vec<String> {
    std::thread::spawn(move || {
        Runtime::with_seed_and_config(seed, madsim::Config::default()).block_on(scenario(tier))
    })
    .join()
    .unwrap_or_else(|failure| {
        eprintln!(
            "Replay: BEDROCK_FAILOVER_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test failover -- --nocapture"
        );
        std::panic::resume_unwind(failure)
    })
}

#[test]
fn subscribers_fail_over_warm_at_every_tier_and_replay_per_seed() {
    let seeds = std::env::var("BEDROCK_FAILOVER_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 42]);
    for seed in seeds {
        for tier in CELL..=REGION {
            let first = run(seed, tier);
            assert_eq!(first, run(seed, tier), "seed {seed} tier {tier} changed");
            println!("failover seed {seed} tier {tier}: {first:?}");
        }
    }
}
