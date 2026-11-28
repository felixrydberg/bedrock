use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bedrock_lib::admin::Admin;
use bedrock_lib::health::storage::StorageHealth;
use tokio::sync::watch;
use tokio::time::sleep;

use super::pb::artifact_transfer_server::ArtifactTransfer;
use super::*;
use crate::artifact_storage::{CacheStorage, Memory, Origin};

/// S3 holding `artifacts`; reads count, then wait until `open` is set.
struct Gated {
    artifacts: BTreeMap<Hash, Bytes>,
    gets: AtomicUsize,
    open: watch::Sender<bool>,
}

impl Gated {
    fn new(artifacts: &[&Bytes], open: bool) -> Arc<Self> {
        Arc::new(Gated {
            artifacts: artifacts
                .iter()
                .map(|a| (Hash::of(a), (*a).clone()))
                .collect(),
            gets: AtomicUsize::new(0),
            open: watch::channel(open).0,
        })
    }
}

#[async_trait]
impl Origin for Gated {
    async fn put(&self, _: &Hash, _: Bytes) -> StorageResult<()> {
        unreachable!()
    }
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Bytes>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.open.subscribe().wait_for(|open| *open).await.unwrap();
        Ok(self.artifacts.get(hash).cloned())
    }
}

struct Node {
    admin: Arc<Admin>,
    cache: Arc<Memory>,
    transfer: Arc<Transfer>,
}

fn node(origin: Arc<Gated>) -> Node {
    let admin = Admin::new();
    let cache = Arc::new(Memory::default());
    let artifacts = Artifacts::new(origin, cache.clone(), StorageHealth::new(&admin));
    let transfer = Transfer::new(
        Arc::new(artifacts),
        Arc::new(Peers::new(&admin)),
        &protection::Metrics::new(&admin),
    );
    Node {
        admin,
        cache,
        transfer: Arc::new(transfer),
    }
}

/// Serves `service` on a local port and returns its address.
async fn serve<T: ArtifactTransfer>(service: Arc<T>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ArtifactTransferServer::from_arc(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    addr.to_string()
}

/// A peer's request, as another node sends it.
async fn peer_fetch(addr: String, hash: Hash) -> Bytes {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    let request = pb::FetchRequest {
        hash: Bytes::copy_from_slice(&hash.0),
        held_only: false,
    };
    let mut stream = ArtifactTransferClient::new(channel)
        .fetch(request)
        .await
        .unwrap()
        .into_inner();
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        body.extend_from_slice(&chunk.data);
    }
    body.freeze()
}

fn artifact(len: usize) -> Bytes {
    (0..len).map(|i| (i * 7 % 251) as u8).collect()
}

/// Awaits `request`, counting it in `joined` once its first poll returns. Nothing before the coalescer awaits, so by
/// then it leads the S3 fetch or waits on it.
async fn joining<T>(request: impl Future<Output = T>, joined: &AtomicUsize) -> T {
    let mut request = std::pin::pin!(request);
    let mut first = true;
    std::future::poll_fn(|cx| {
        let poll = request.as_mut().poll(cx);
        if std::mem::take(&mut first) {
            joined.fetch_add(1, Ordering::SeqCst);
        }
        poll
    })
    .await
}

/// The node's transfer service, counting the peer requests that joined its fetch.
struct Counting(Arc<Transfer>, Arc<AtomicUsize>);

#[tonic::async_trait]
impl ArtifactTransfer for Counting {
    type FetchStream = Chunks;

    async fn fetch(&self, request: Request<pb::FetchRequest>) -> Result<Response<Chunks>, Status> {
        joining(self.0.fetch(request), &self.1).await
    }
}

/// #79 acceptance: 100 concurrent requests for one missing hash, from peers and local prewarming, cause one S3 fetch,
/// and the peer request that arrives before prewarming starts leads it.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_peer_and_local_requests_share_one_s3_fetch() {
    let artifact = artifact(1 << 20);
    let hash = Hash::of(&artifact);
    let origin = Gated::new(&[&artifact], false);
    let server = node(origin.clone());
    let joined = Arc::new(AtomicUsize::new(0));
    let addr = serve(Arc::new(Counting(server.transfer.clone(), joined.clone()))).await;

    let mut requests = tokio::task::JoinSet::new();
    requests.spawn(peer_fetch(addr.clone(), hash));
    while origin.gets.load(Ordering::SeqCst) == 0 {
        sleep(Duration::from_millis(5)).await;
    }
    for i in 1..100 {
        if i % 2 == 0 {
            requests.spawn(peer_fetch(addr.clone(), hash));
        } else {
            let transfer = server.transfer.clone();
            let joined = joined.clone();
            requests.spawn(async move {
                let got = joining(transfer.get("cell-a/warm", &hash, &[]), &joined).await;
                got.unwrap().unwrap()
            });
        }
    }
    // Everyone joins the running fetch before S3 answers; a late joiner would hit the cache and miss the count below.
    while joined.load(Ordering::SeqCst) < 100 {
        sleep(Duration::from_millis(5)).await;
    }
    origin.open.send_replace(true);

    let got = requests.join_all().await;
    assert_eq!(got.len(), 100);
    assert!(got.iter().all(|bytes| *bytes == artifact));
    assert_eq!(origin.gets.load(Ordering::SeqCst), 1);
    let metrics = server.admin.render();
    assert!(
        metrics.contains(r#"bedrock_read_coalesced_total{source="artifact"} 99"#),
        "{metrics}"
    );
    assert!(server.cache.contains(&hash).await.unwrap());
}

/// A peer's copy is used and cached, and S3 is not read.
#[tokio::test(flavor = "multi_thread")]
async fn a_peers_copy_is_streamed_verified_and_cached() {
    let artifact = artifact(CHUNK * 3 + 5);
    let hash = Hash::of(&artifact);
    let holder = node(Gated::new(&[&artifact], true));
    let addr = serve(holder.transfer.clone()).await;
    let origin = Gated::new(&[], true);
    let receiver = node(origin.clone());

    let got = receiver.transfer.get("cell-a", &hash, &[addr]).await;
    assert_eq!(got.unwrap().unwrap(), artifact);
    assert_eq!(origin.gets.load(Ordering::SeqCst), 0);
    assert!(receiver.cache.contains(&hash).await.unwrap());
    // The call and its five chunk reads (four chunks and the end) ran on the bulk connection.
    let metrics = receiver.admin.render();
    assert!(
        metrics.contains(r#"bedrock_transport_calls_total{class="Bulk",outcome="Ok"} 6"#)
            && !metrics.contains(r#"class="Control""#),
        "{metrics}"
    );

    // Neither the peer nor S3 has it: a missing artifact, not an error.
    let missing = Hash::of(b"missing");
    let addr = serve(holder.transfer.clone()).await;
    let got = receiver.transfer.get("cell-a", &missing, &[addr]).await;
    assert_eq!(got.unwrap(), None);
}

/// Two nodes fetching one artifact from each other do not wait on each other: each peer's request reads S3 instead of
/// joining the fetch that is asking the other node, which would stall both for `TIMEOUT`.
#[tokio::test(flavor = "multi_thread")]
async fn nodes_fetching_from_each_other_do_not_wait_on_each_other() {
    let artifact = artifact(CHUNK + 1);
    let hash = Hash::of(&artifact);
    let (a_origin, b_origin) = (
        Gated::new(&[&artifact], false),
        Gated::new(&[&artifact], false),
    );
    let (a, b) = (node(a_origin.clone()), node(b_origin.clone()));
    let (a_addr, b_addr) = (
        serve(a.transfer.clone()).await,
        serve(b.transfer.clone()).await,
    );

    let a_get = tokio::spawn({
        let transfer = a.transfer.clone();
        async move { transfer.get("cell-a/warm", &hash, &[b_addr]).await }
    });
    let b_get = tokio::spawn({
        let transfer = b.transfer.clone();
        async move { transfer.get("cell-a/warm", &hash, &[a_addr]).await }
    });
    // Each node's S3 is read for the other's request, not held up behind its own fetch.
    let reached = async {
        while a_origin.gets.load(Ordering::SeqCst) + b_origin.gets.load(Ordering::SeqCst) < 2 {
            sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(TIMEOUT / 2, reached).await.unwrap();
    a_origin.open.send_replace(true);
    b_origin.open.send_replace(true);

    assert_eq!(a_get.await.unwrap().unwrap().unwrap(), artifact);
    assert_eq!(b_get.await.unwrap().unwrap().unwrap(), artifact);
    assert_eq!(a_origin.gets.load(Ordering::SeqCst), 1);
    assert_eq!(b_origin.gets.load(Ordering::SeqCst), 1);
}

/// A peer whose stream ends early, with a clean end of stream.
struct Truncating(Bytes);

#[tonic::async_trait]
impl ArtifactTransfer for Truncating {
    type FetchStream = Chunks;

    async fn fetch(&self, _: Request<pb::FetchRequest>) -> Result<Response<Chunks>, Status> {
        let half = pb::Chunk {
            data: self.0.slice(..self.0.len() / 2),
        };
        Ok(Response::new(Box::pin(tokio_stream::iter([Ok(half)]))))
    }
}

/// #79 acceptance: a truncated transfer is rejected, neither cached nor served, and the artifact comes from S3.
#[tokio::test(flavor = "multi_thread")]
async fn a_truncated_transfer_is_rejected() {
    let artifact = artifact(CHUNK + 1);
    let hash = Hash::of(&artifact);
    let addr = serve(Arc::new(Truncating(artifact.clone()))).await;
    let origin = Gated::new(&[&artifact], true);
    let receiver = node(origin.clone());

    let got = receiver.transfer.get("cell-a", &hash, &[addr]).await;
    assert_eq!(got.unwrap().unwrap(), artifact);
    assert_eq!(origin.gets.load(Ordering::SeqCst), 1);
    let cached = Origin::get(&*receiver.cache, &hash).await.unwrap();
    assert_eq!(cached.unwrap(), artifact);
}
