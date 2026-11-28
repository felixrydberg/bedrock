//! Content-addressed artifact storage (#78 / B-30, `cdn.md` §3). S3 holds every artifact under its SHA-256 and is the
//! authority; a [`CacheStorage`] keeps local copies. [`Artifacts`] verifies each artifact's hash before it is cached and
//! before it is served.
//!
//! A cached artifact that fails verification is quarantined alone: evicted, refetched from S3 and reported to the
//! storage-health manager (B-14) without escalating. Only [`bedrock_lib::health::CONSECUTIVE_FAILURES`] failures with
//! no verified read between them quarantine the whole cache; reads then bypass it to S3. Nothing rebuilds a quarantined
//! cache yet, so it stays bypassed until restart.
//!
//! Foyer and native S3 I/O ([`S3`]) are compiled out of simulation builds, like RocksDB; simulation supplies its own
//! [`Origin`] and [`CacheStorage`].

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bedrock_lib::health::storage::{State, StorageHealth};
use bedrock_lib::storage::{StorageError, StorageResult};
use bytes::Bytes;
use tokio_stream::{Stream, StreamExt};

#[cfg(not(madsim))]
#[allow(clippy::disallowed_methods)] // adapter code: the inventory's files on disk
mod foyer;
#[cfg(not(madsim))]
pub use self::foyer::Foyer;

use bedrock_lib::cdn::error_context;
pub use bedrock_lib::cdn::{Hash, Origin};

/// An artifact's bytes as a stream of chunks.
pub type Body = Pin<Box<dyn Stream<Item = StorageResult<Bytes>> + Send>>;

/// A body of one chunk.
pub fn body(bytes: Bytes) -> Body {
    Box::pin(tokio_stream::once(Ok(bytes)))
}

/// Reads a whole body.
// ponytail: whole artifacts are buffered, because a hash is only known at the end and S3's checked reads and Foyer hold
// whole values. That bounds artifacts by memory (and Foyer's block size); verify per chunk with a manifest if they grow.
pub async fn collect(mut body: Body) -> StorageResult<Bytes> {
    let mut all = Vec::new();
    while let Some(chunk) = body.next().await {
        all.extend_from_slice(&chunk?);
    }
    Ok(all.into())
}

/// A local artifact cache, independent of KV's `StorageEngine` (`cdn.md` Core Principle 7). It may drop artifacts at
/// any time; S3 still has them. It stores what it is given: [`Artifacts`] verifies hashes on both sides.
#[async_trait]
pub trait CacheStorage: Send + Sync {
    async fn put(&self, hash: &Hash, body: Body) -> StorageResult<()>;
    /// The artifact, or `Ok(None)` when it is not cached.
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Body>>;
    async fn contains(&self, hash: &Hash) -> StorageResult<bool>;
    /// Removes the artifact; removing an absent one succeeds.
    async fn delete(&self, hash: &Hash) -> StorageResult<()>;
    /// The local artifact inventory, with sizes, in hash order. It survives restarts.
    fn inventory(&self) -> StorageResult<Vec<(Hash, u64)>>;
    /// Bytes of artifacts in the inventory.
    fn usage(&self) -> StorageResult<u64>;
}

#[cfg(not(madsim))]
pub use bedrock_lib::cdn::S3;

#[cfg(any(test, feature = "test-support"))]
pub use memory::Memory;

#[cfg(any(test, feature = "test-support"))]
#[path = "../tests/support/memory.rs"]
mod memory;

/// The health manager's source for the artifact cache.
pub const DATASET: &str = bedrock_lib::datasets::capabilities::CDN;
pub const LAYER: &str = "artifacts";

/// Artifacts in their [`Origin`] by SHA-256, read through a local cache.
pub struct Artifacts {
    origin: Arc<dyn Origin>,
    cache: Arc<dyn CacheStorage>,
    health: Arc<StorageHealth>,
}

impl Artifacts {
    pub fn new(
        origin: Arc<dyn Origin>,
        cache: Arc<dyn CacheStorage>,
        health: Arc<StorageHealth>,
    ) -> Self {
        Artifacts {
            origin,
            cache,
            health,
        }
    }

    /// Stores `body` in the origin under its SHA-256, which it returns. Uploading an artifact that exists succeeds.
    pub async fn upload(&self, body: Body) -> StorageResult<Hash> {
        let body = collect(body).await?;
        let hash = Hash::of(&body);
        self.origin.put(&hash, body).await?;
        Ok(hash)
    }

    /// The verified artifact, or `Ok(None)` when the origin has no artifact by that hash.
    pub async fn fetch(&self, hash: &Hash) -> StorageResult<Option<Body>> {
        if let Some(bytes) = self.cached(hash).await {
            return Ok(Some(body(bytes)));
        }
        Ok(self.fetch_origin(hash).await?.map(body))
    }

    /// The verified artifact from the origin, which then fills the cache.
    pub(crate) async fn fetch_origin(&self, hash: &Hash) -> StorageResult<Option<Bytes>> {
        let Some(bytes) = self.origin.get(hash).await? else {
            return Ok(None);
        };
        // Origin damage is not local damage.
        if Hash::of(&bytes) != *hash {
            return Err(StorageError::Corruption(error_context(
                "origin",
                hash,
                "SHA-256 differs from the artifact's name",
            )));
        }
        self.fill(hash, bytes.clone()).await;
        Ok(Some(bytes))
    }

    /// Caches `bytes`, which the caller verified against `hash`. A failure is reported, not returned: the cache may
    /// drop artifacts anyway.
    pub(crate) async fn fill(&self, hash: &Hash, bytes: Bytes) {
        // Rechecked: other fetches may have quarantined the cache meanwhile.
        if self.healthy()
            && let Err(error) = self.cache.put(hash, body(bytes)).await
        {
            self.health.report_contained(DATASET, LAYER, &error);
        }
    }

    /// Whether the cache is in service; a quarantined one is bypassed.
    fn healthy(&self) -> bool {
        self.health
            .get(DATASET, LAYER)
            .is_none_or(|report| report.state == State::Healthy)
    }

    /// The cached artifact if the cache is in service and the artifact verifies. Otherwise a damaged one is evicted
    /// and reported, and the caller refetches it.
    pub(crate) async fn cached(&self, hash: &Hash) -> Option<Bytes> {
        if !self.healthy() {
            return None;
        }
        let error = match self.cache.get(hash).await {
            Ok(None) => return None,
            Ok(Some(body)) => match collect(body).await {
                Ok(bytes) if Hash::of(&bytes) == *hash => {
                    self.health.verified(DATASET, LAYER);
                    return Some(bytes);
                }
                Ok(_) => StorageError::Corruption(error_context(
                    "cache",
                    hash,
                    "cached artifact failed SHA-256 verification",
                )),
                Err(error) => error,
            },
            Err(error) => error,
        };
        tracing::warn!(%hash, %error, "evicting a damaged cached artifact");
        self.health.report_contained(DATASET, LAYER, &error);
        if let Err(error) = self.cache.delete(hash).await {
            tracing::warn!(%hash, %error, "could not evict a damaged cached artifact");
        }
        None
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "../tests/unit/artifact_storage.rs"]
mod tests;
