//! #82 / B-34 acceptance: the two owners of an artifact's range prewarm it from its ref key. Cold, together or with the
//! second asking the first before it starts, they read S3 once and end with two verified copies; lookups return only
//! holders that have the bytes; when the first owner is down or hangs, the second completes from S3 within B-31's
//! timeout. Runs over real sockets in normal builds and simulated under `--cfg madsim`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_cdn::artifact_storage::{Artifacts, Hash, Memory, Origin};
use bedrock_cdn::artifact_transfer::{ArtifactTransferServer, Transfer};
use bedrock_cdn::paths::ref_key;
use bedrock_cdn::retention::Prewarm;
use bedrock_lib::admin::Admin;
use bedrock_lib::health::Health;
use bedrock_lib::health::protection;
use bedrock_lib::health::storage::StorageHealth;
use bedrock_lib::placement::{self, Directory, Owner, State};
use bedrock_lib::reconciliation::{self, Reconciler};
use bedrock_lib::relays::sources::{RECHECK, Sources};
use bedrock_lib::replication::pb::Member;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::{StorageEngine, StorageResult, WriteBatch, l2_key};
use bedrock_lib::transport::Peers;
use bytes::Bytes;
use tokio::time::sleep;

fn artifact() -> Bytes {
    (0..1 << 20).map(|i: usize| (i * 13 % 251) as u8).collect()
}

/// S3 holding the artifact; every read counts and takes a while, so concurrent fetches overlap.
#[derive(Default)]
struct Counted {
    gets: AtomicUsize,
}

#[async_trait]
impl Origin for Counted {
    async fn put(&self, _: &Hash, _: Bytes) -> StorageResult<()> {
        unreachable!()
    }
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Bytes>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        sleep(Duration::from_millis(200)).await;
        Ok((*hash == Hash::of(&artifact())).then(artifact))
    }
}

/// A `cdn` owner: its engine holds the artifact's ref key, as its relay delivered it.
struct Holder {
    cache: Arc<Memory>,
    transfer: Arc<Transfer>,
    prewarm: Reconciler<Prewarm>,
}

/// The cell's members at their addresses and one range owned by `owners`, in order.
fn view(owners: &[(&str, String)]) -> (BTreeMap<String, Member>, Directory) {
    let directory: Directory = BTreeMap::from([(
        0,
        owners
            .iter()
            .map(|(node, _)| Owner {
                node: (*node).into(),
                state: State::Ready,
            })
            .collect(),
    )]);
    let members = owners
        .iter()
        .map(|(node, address)| {
            let member = Member {
                node_id: (*node).into(),
                address: address.clone(),
                status: bedrock_lib::replication::pb::member::Status::Ready as i32,
                ..Member::default()
            };
            ((*node).to_owned(), member)
        })
        .collect();
    (members, directory)
}

/// Owner `me` of the one range, listing `owners` in directory order at their addresses.
fn holder(me: &str, owners: &[(&str, String)], origin: Arc<Counted>) -> Holder {
    let admin = Admin::new();
    let cache = Arc::new(Memory::default());
    let artifacts = Arc::new(Artifacts::new(
        origin,
        cache.clone(),
        StorageHealth::new(&admin),
    ));
    let transfer = Arc::new(Transfer::new(
        artifacts.clone(),
        Arc::new(Peers::new(&admin)),
        &protection::Metrics::new(&admin),
    ));
    let sha256 = Hash::of(&artifact());
    let key = ref_key(&sha256, "d1").unwrap();
    let engine = Arc::new(MemoryEngine::new("cdn"));
    let mut batch = WriteBatch::default();
    batch.put(
        l2_key(placement::content(&sha256.0), "deployments", &key),
        Vec::new(),
    );
    engine.write(batch).unwrap();
    let (members, directory) = view(owners);
    let prewarm = Prewarm {
        engine: engine as Arc<dyn StorageEngine>,
        placement: Box::new(move || Ok((members.clone(), directory.clone()))),
        me: me.into(),
        lane: "cell-a/warm".into(),
        artifacts,
        transfer: transfer.clone(),
    };
    let config = reconciliation::Config {
        burst: 100,
        refill: Duration::from_millis(1),
        repair_timeout: Duration::from_secs(60),
        base_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(1),
        seed: 1,
    };
    let prewarm = Reconciler::new(
        "prewarm",
        Arc::new(prewarm),
        config,
        &reconciliation::Metrics::new(&admin),
    );
    Holder {
        cache,
        transfer,
        prewarm,
    }
}

fn holds(holder: &Holder) -> bool {
    holder
        .cache
        .0
        .lock()
        .unwrap()
        .get(&Hash::of(&artifact()))
        .is_some_and(|bytes| Hash::of(bytes) == Hash::of(&artifact()))
}

fn router(transfer: Arc<Transfer>) -> tonic::transport::server::Router {
    tonic::transport::Server::builder().add_service(ArtifactTransferServer::from_arc(transfer))
}

/// Two cold owners `a` (first) and `b`. With `together`, both prewarm at once; otherwise only the second does, asking
/// the first before it has started. Either way S3 is read once and both end with verified copies. Lookups skip a holder
/// whose held-only ask found nothing until [`RECHECK`] passes, and a held-only ask never reads S3.
async fn pair(together: bool, owners: &[(&str, String)], origin: Arc<Counted>) {
    let mut first = holder("a", owners, origin.clone());
    let mut second = holder("b", owners, origin.clone());
    let (serve_a, serve_b) = (first.transfer.clone(), second.transfer.clone());
    let _servers = (
        serve(owners[0].1.clone(), serve_a),
        serve(owners[1].1.clone(), serve_b),
    );
    sleep(Duration::from_millis(10)).await;
    let sha256 = Hash::of(&artifact());
    let hash = placement::content(&sha256.0);
    let (members, directory) = view(owners);
    let health = Health::new(&Admin::new());
    let sources = Sources::default();
    let lookup = || {
        let holders = sources.select(&members, &directory, &health, "", "cdn", "", hash, 0);
        holders
            .iter()
            .map(|m| m.node_id.clone())
            .collect::<Vec<_>>()
    };

    let first_address = &owners[0].1;
    assert_eq!(
        second.transfer.held(first_address, &sha256).await.unwrap(),
        None,
        "cold"
    );
    sources.missing("a", "cdn", hash);
    assert_eq!(lookup(), ["b"], "the cold first holder is skipped");
    assert_eq!(
        origin.gets.load(Ordering::SeqCst),
        0,
        "held-only asks read no S3"
    );

    if together {
        let (a, b) = tokio::join!(first.prewarm.pass(), second.prewarm.pass());
        assert_eq!((a.unwrap().repaired, b.unwrap().repaired), (1, 1));
    } else {
        assert_eq!(second.prewarm.pass().await.unwrap().repaired, 1);
        assert!(holds(&first), "the first fetched for the second's request");
        assert_eq!(first.prewarm.pass().await.unwrap().pending, 0);
    }
    assert_eq!(origin.gets.load(Ordering::SeqCst), 1, "one S3 download");
    assert!(holds(&first) && holds(&second), "two verified copies");

    sleep(RECHECK).await;
    assert_eq!(lookup(), ["a", "b"]);
    let held = second.transfer.held(first_address, &sha256).await.unwrap();
    assert_eq!(held, Some(artifact()));
    assert_eq!(origin.gets.load(Ordering::SeqCst), 1);
}

#[cfg(not(madsim))]
fn serve(address: String, transfer: Arc<Transfer>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
        router(transfer)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap()
    })
}

#[cfg(madsim)]
fn serve(address: String, transfer: Arc<Transfer>) {
    let address: std::net::SocketAddr = address.parse().unwrap();
    madsim::runtime::Handle::current()
        .create_node()
        .ip(address.ip())
        .build()
        .spawn(async move { router(transfer).serve(address).await.unwrap() });
}

/// A free local port.
#[cfg(not(madsim))]
async fn local() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread")]
async fn cold_holders_read_s3_once_whoever_starts() {
    for together in [true, false] {
        let owners = [("a", local().await), ("b", local().await)];
        pair(together, &owners, Arc::new(Counted::default())).await;
    }
}

#[cfg(madsim)]
#[madsim::test]
async fn cold_holders_read_s3_once_whoever_starts() {
    let handle = madsim::runtime::Handle::current();
    handle
        .create_node()
        .name("test")
        .ip("10.0.0.9".parse().unwrap())
        .build()
        .spawn(async move {
            for (together, ips) in [
                (true, ["10.0.1.1", "10.0.1.2"]),
                (false, ["10.0.2.1", "10.0.2.2"]),
            ] {
                let owners = [
                    ("a", format!("{}:7000", ips[0])),
                    ("b", format!("{}:7000", ips[1])),
                ];
                pair(together, &owners, Arc::new(Counted::default())).await;
            }
        })
        .await
        .unwrap();
}

/// A first owner that accepts transfers and never answers.
#[cfg(madsim)]
struct Hung;

#[cfg(madsim)]
#[tonic::async_trait]
impl bedrock_cdn::artifact_transfer::pb::artifact_transfer_server::ArtifactTransfer for Hung {
    type FetchStream =
        tokio_stream::Empty<Result<bedrock_cdn::artifact_transfer::pb::Chunk, tonic::Status>>;

    async fn fetch(
        &self,
        _: tonic::Request<bedrock_cdn::artifact_transfer::pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        std::future::pending().await
    }
}

/// The second owner prewarms from S3 when the first is down or hangs, within B-31's per-call timeout.
#[cfg(madsim)]
#[madsim::test]
async fn the_second_holder_falls_back_to_s3_when_the_first_fails() {
    use bedrock_cdn::artifact_transfer::TIMEOUT;

    let handle = madsim::runtime::Handle::current();
    let hung: std::net::SocketAddr = "10.0.0.1:7000".parse().unwrap();
    handle
        .create_node()
        .name("hung")
        .ip(hung.ip())
        .build()
        .spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    bedrock_cdn::artifact_transfer::pb::artifact_transfer_server::ArtifactTransferServer::new(Hung),
                )
                .serve(hung)
                .await
                .unwrap()
        });
    handle
        .create_node()
        .name("second")
        .ip("10.0.0.2".parse().unwrap())
        .build()
        .spawn(async move {
            sleep(Duration::from_millis(10)).await;
            // Down: nothing listens at the first owner's address.
            for first in [hung.to_string(), "10.0.0.3:7000".to_owned()] {
                let origin = Arc::new(Counted::default());
                let owners = [("a", first), ("b", "10.0.0.2:7000".to_owned())];
                let mut second = holder("b", &owners, origin.clone());
                let started = bedrock_lib::monotonic_now();
                assert_eq!(second.prewarm.pass().await.unwrap().repaired, 1);
                let took = bedrock_lib::monotonic_now() - started;
                assert!(took < TIMEOUT + Duration::from_secs(1), "{took:?}");
                assert!(holds(&second));
                assert_eq!(origin.gets.load(Ordering::SeqCst), 1);
            }
        })
        .await
        .unwrap();
}
