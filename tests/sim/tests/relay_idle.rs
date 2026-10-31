//! Shared relay sessions detect a one-way idle partition and resume at another source (#14/B-17).
#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::health::State;
use bedrock_lib::relays::{
    Hub,
    client::{Selection, Session, Target},
};
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb::relay_server::RelayServer;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::storage::namespace::memory::MemoryFactory;

use bedrock_lib::replication::Operation;
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::transport::Peers;
use bedrock_lib::transport::admin::Admin;
use madsim::runtime::{Handle, Runtime};
use tokio::time::{sleep, timeout};

const PRIMARY: &str = "10.0.0.1:7000";
const BACKUP: &str = "10.0.0.2:7000";

async fn writer() -> Arc<Writer> {
    Writer::open(
        || 700,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap()
}

async fn append(writer: &Arc<Writer>, sequence: u64) {
    assert_eq!(
        writer
            .commit(Write {
                dataset: "auth".into(),
                namespace: "ns".into(),
                key: b"k".to_vec(),
                operation: Operation::Set(sequence.to_be_bytes().to_vec()),
                idempotency_key: None,
            })
            .await
            .unwrap(),
        sequence
    );
}
async fn scenario() {
    let handle = Handle::current();
    let primary_log = writer().await;
    let backup_log = writer().await;
    append(&primary_log, 1).await;
    append(&backup_log, 1).await;
    let mut sources = Vec::new();
    for (name, address, log) in [
        ("primary", PRIMARY, primary_log.clone()),
        ("backup", BACKUP, backup_log.clone()),
    ] {
        let socket: std::net::SocketAddr = address.parse().unwrap();
        let node = handle.create_node().name(name).ip(socket.ip()).build();
        node.spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RelayServer::new(Hub::writer(
                    name.into(),
                    log,
                    Arc::new(Peers::new(&Admin::new())),
                )))
                .serve(socket)
                .await
                .unwrap();
        });
        sources.push(node);
    }
    let primary_id = sources[0].id();
    let leaf = handle
        .create_node()
        .name("leaf")
        .ip("10.0.0.3".parse().unwrap())
        .build();
    let leaf_id = leaf.id();
    leaf.spawn(async move {
        sleep(Duration::from_millis(20)).await;
        let admin = Admin::new();
        let peers = Arc::new(Peers::new(&admin));
        let engine = Arc::new(MemoryEngine::new("auth"));
        let subscription = Session::start(peers.clone(), vec![PRIMARY.into(), BACKUP.into()], vec![Target {
            selection: Selection { dataset: "auth".into(), namespace: "ns".into(), buckets: Vec::new() },
            engine: engine.clone(), prefix: Vec::new(),
        }]).unwrap();
        timeout(Duration::from_secs(10), subscription.wait_for("auth", "ns", 1)).await.unwrap();
        sleep(Duration::from_secs(35)).await;
        assert_eq!(peers.health().state(PRIMARY), State::Closed, "healthy idle acknowledgements must keep the source alive");

        // Incoming source progress remains reachable, so a client-side read timeout cannot detect this fault.
        madsim::net::NetSim::current().clog_link(leaf_id, primary_id);
        timeout(Duration::from_secs(45), async {
            while peers.health().state(PRIMARY) != State::Open { sleep(Duration::from_millis(100)).await; }
        }).await.expect("source did not expire missing incoming acknowledgements");
        append(&primary_log, 2).await;
        append(&backup_log, 2).await;
        timeout(Duration::from_secs(15), subscription.wait_for("auth", "ns", 2)).await.expect("subscriber did not fail over");
        assert_eq!(engine.get(b"k").unwrap(), Some(2_u64.to_be_bytes().to_vec()));
        assert!(subscription.ready());
        assert!(!admin.render().lines().any(|line| line.contains("outcome=\"Timeout\"") && !line.ends_with(" 0")),
            "source incoming deadline, rather than client read timeout, must detect the one-way fault");
        subscription.stop().await;
    }).await.unwrap();
    for source in sources {
        handle.kill(source.id());
    }
    handle.kill(leaf.id());
}

#[test]
fn one_way_idle_partition_expires_source_and_resumes_at_backup() {
    Runtime::with_seed_and_config(1, madsim::Config::default()).block_on(scenario());
}
