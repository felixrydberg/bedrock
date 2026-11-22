//! Artifact transfer between CDN nodes (#79 / B-31, `cdn.md` §3 and Core Principle 6). A node gets a missing artifact
//! from the peers its caller names, then from S3, and serves the artifacts it has, or fetches, to its peers.
//!
//! - **Coalescing.** Concurrent requests for one hash on a node share one S3 fetch, whether they come from local callers
//!   (prewarming, retrieval) or from peers, and whichever arrives first starts it (#24 / B-22b [`Coalescer`]).
//! - **No forwarding.** A peer's request is answered from the cache or S3, never from other peers, and never waits on a
//!   fetch that is asking peers, so two nodes fetching from each other never wait on each other. It joins the S3 stage
//!   of such a fetch, or starts one that the fetch then joins.
//! - **Verification.** Every artifact is checked against its SHA-256 before it is cached or served; a truncated or
//!   damaged transfer is rejected and the next source tried. Peer damage is not reported as local storage damage.
//! - **Traffic separation and backpressure.** Transfers run on the peer's [`Class::Bulk`] connection, so they fill its
//!   HTTP/2 flow-control windows rather than delaying control and relay traffic. S3 fetches pass the `S3_FETCHES`
//!   limiter; a full queue fails at once with a retryable `Overloaded`.
//! - **Retries and timeouts.** Peers are tried in order, skipping those with an open breaker (B-13), then S3, whose
//!   client retries on its own. Every call and every chunk is bounded by [`TIMEOUT`].
//!
//! Which peers hold an artifact (B-34), what to prewarm (B-45) and what to retain (B-32) are the caller's.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::health::protection::{self, Coalescer, Limiter, S3_FETCHES};
use bedrock_lib::storage::{StorageError, StorageResult};
use bedrock_lib::transport::{Caller, Class, Peers};
use bytes::{Bytes, BytesMut};
use tokio_stream::Stream;
use tonic::{Code, Request, Response, Status};

use crate::artifact_storage::{Artifacts, Hash};

pub mod pb {
    tonic::include_proto!("bedrock.artifact");
}
use pb::artifact_transfer_client::ArtifactTransferClient;
pub use pb::artifact_transfer_server::ArtifactTransferServer;

/// Bounds each peer call and each chunk read. A peer's first answer may wait on its own S3 fetch.
pub const TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds a whole peer transfer, so a peer trickling chunks cannot hold a fetch and everyone waiting on it.
// ponytail: one bound for every size; scale it with the artifact's size once callers know it (B-32 ref keys).
pub const DEADLINE: Duration = Duration::from_secs(120);
/// Well under gRPC's 4 MiB message limit.
const CHUNK: usize = 256 << 10;
/// The limiter lane of a peer's request when TLS does not name its cell.
const PEER_LANE: &str = "peer";

/// A finished fetch: the lane that led it, and its answer.
type Landed = (String, StorageResult<Option<Bytes>>);

pub struct Transfer {
    artifacts: Arc<Artifacts>,
    peers: Arc<Peers>,
    /// Fetches asking peers. Peers' requests never join these, so two nodes asking each other never wait on each other.
    /// Keyed by the sources too: a failure behind one caller's peers says nothing about another's.
    from_peers: Coalescer<(Hash, Vec<String>), Landed>,
    /// Fetches from the cache, then S3: every peer request, local requests naming no peers, and the last stage of
    /// those that do.
    from_s3: Coalescer<Hash, Landed>,
    s3: Limiter,
}

impl Transfer {
    pub fn new(
        artifacts: Arc<Artifacts>,
        peers: Arc<Peers>,
        metrics: &protection::Metrics,
    ) -> Self {
        Transfer {
            artifacts,
            peers,
            from_peers: Coalescer::new("artifact_peers", metrics),
            from_s3: Coalescer::new("artifact", metrics),
            s3: Limiter::new("artifact_s3", S3_FETCHES, metrics),
        }
    }

    /// The verified artifact from the local cache, then `sources` (peer addresses, in order), then S3; `Ok(None)` when
    /// S3 has no artifact by that hash. `lane` pays for an S3 fetch: the caller's cell, or its own lane for background
    /// work, e.g. `cell-a/warm` for prewarming.
    pub async fn get(
        &self,
        lane: &str,
        hash: &Hash,
        sources: &[String],
    ) -> StorageResult<Option<Bytes>> {
        if sources.is_empty() {
            return self.fetch_s3(lane, hash).await;
        }
        // ponytail: a peer's request arriving meanwhile reads S3 itself rather than wait, since this fetch may be waiting
        // on that peer, directly or through others. That costs one extra S3 read when a third node asks. The fix is
        // placement, not waiting: B-34 should have one owner per range read S3 and the other name it, never back.
        let fetch = || async {
            // Checked inside the fetch: a request that just missed the previous one finds what it cached.
            if let Some(bytes) = self.artifacts.cached(hash).await {
                return Ok(Some(bytes));
            }
            for peer in self.peers.health().route(sources) {
                match self.fetch_peer(peer, hash).await {
                    Ok(Some(bytes)) => {
                        self.artifacts.fill(hash, bytes.clone()).await;
                        return Ok(Some(bytes));
                    }
                    Ok(None) => {}
                    Err(status) => {
                        tracing::warn!(%peer, %hash, %status, "artifact transfer failed")
                    }
                }
            }
            self.fetch_s3(lane, hash).await
        };
        coalesced(&self.from_peers, (*hash, sources.to_vec()), lane, fetch).await
    }

    async fn fetch_s3(&self, lane: &str, hash: &Hash) -> StorageResult<Option<Bytes>> {
        let fetch = || async {
            if let Some(bytes) = self.artifacts.cached(hash).await {
                return Ok(Some(bytes));
            }
            let _permit = self.s3.acquire(lane).await?;
            self.artifacts.fetch_origin(hash).await
        };
        coalesced(&self.from_s3, *hash, lane, fetch).await
    }

    /// The artifact streamed from `peer` and verified, or `None` when the peer could not find it.
    async fn fetch_peer(&self, peer: &str, hash: &Hash) -> Result<Option<Bytes>, Status> {
        let channel = self
            .peers
            .channel(peer, Class::Bulk)
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let request = pb::FetchRequest {
            hash: Bytes::copy_from_slice(&hash.0),
        };
        let mut client = ArtifactTransferClient::new(channel);
        let call = client.fetch(request);
        let mut stream = match self.peers.call(peer, Class::Bulk, TIMEOUT, call).await {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(status),
        };
        // ponytail: whole artifacts are buffered, as in `artifact_storage::collect`.
        let read = async {
            let mut body = BytesMut::new();
            while let Some(chunk) = self
                .peers
                .call(peer, Class::Bulk, TIMEOUT, stream.message())
                .await?
            {
                body.extend_from_slice(&chunk.data);
            }
            Ok::<_, Status>(body.freeze())
        };
        let body = tokio::time::timeout(DEADLINE, read)
            .await
            .map_err(|_| Status::deadline_exceeded(format!("transfer took over {DEADLINE:?}")))??;
        if Hash::of(&body) != *hash {
            return Err(Status::data_loss(format!(
                "{} bytes received do not match the artifact's SHA-256",
                body.len()
            )));
        }
        Ok(Some(body))
    }
}

/// Runs `fetch` in `flights`, or joins the one running for `key`.
async fn coalesced<K: Ord + Clone, F: Future<Output = StorageResult<Option<Bytes>>>>(
    flights: &Coalescer<K, Landed>,
    key: K,
    lane: &str,
    fetch: impl FnOnce() -> F,
) -> StorageResult<Option<Bytes>> {
    // Only a leader from `lane` can pass on an overload: it is that lane's queue that is full.
    let accept =
        |(leader, got): &Landed| leader == lane || !matches!(got, Err(StorageError::Overloaded(_)));
    let fetch = || async { (lane.to_owned(), fetch().await) };
    flights.run(key, accept, fetch).await.1
}

type Chunks = Pin<Box<dyn Stream<Item = Result<pb::Chunk, Status>> + Send>>;

#[tonic::async_trait]
impl pb::artifact_transfer_server::ArtifactTransfer for Transfer {
    type FetchStream = Chunks;

    async fn fetch(&self, request: Request<pb::FetchRequest>) -> Result<Response<Chunks>, Status> {
        let lane = match Caller::of(&request) {
            Some(caller) => {
                caller.require(&["cdn"], None)?;
                caller.cell.clone()
            }
            None => PEER_LANE.to_owned(),
        };
        let hash = <[u8; 32]>::try_from(&request.get_ref().hash[..])
            .map(Hash)
            .map_err(|_| Status::invalid_argument("hash must be 32 bytes"))?;
        match self.get(&lane, &hash, &[]).await {
            // The stream is polled only as the bulk connection's flow control allows.
            Ok(Some(bytes)) => {
                let chunks = (0..bytes.len()).step_by(CHUNK).map(move |at| {
                    Ok(pb::Chunk {
                        data: bytes.slice(at..bytes.len().min(at + CHUNK)),
                    })
                });
                Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
            }
            Ok(None) => Err(Status::not_found(format!("no artifact {hash}"))),
            Err(error @ StorageError::Overloaded(_)) => {
                Err(Status::resource_exhausted(error.to_string()))
            }
            Err(error) => Err(Status::unavailable(error.to_string())),
        }
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "../tests/unit/artifact_transfer.rs"]
mod tests;
