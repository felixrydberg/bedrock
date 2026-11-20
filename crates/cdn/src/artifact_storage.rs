//! Content-addressed artifact storage (#78 / B-30, `cdn.md` §3). S3 holds every artifact under its SHA-256 and is the
//! authority; a [`CacheStorage`] keeps local copies. [`Artifacts`] verifies each artifact's hash before it is cached and
//! before it is served.
//!
//! A cached artifact that fails verification is quarantined alone: evicted, refetched from S3 and reported to the
//! storage-health manager (B-14) without escalating. Only [`bedrock_lib::health::CONSECUTIVE_FAILURES`] failures with
//! no verified read between them quarantine the whole cache; reads then bypass it to S3. Nothing rebuilds a quarantined
//! cache yet, so it stays bypassed until restart.
//!
//! Foyer and native S3 I/O are compiled out of simulation builds, like RocksDB; the contract and hash type remain.

use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use bedrock_lib::storage::StorageResult;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio_stream::{Stream, StreamExt};

#[cfg(not(madsim))]
#[allow(clippy::disallowed_methods)] // adapter code: the inventory's files on disk
mod foyer;
#[cfg(not(madsim))]
pub use self::foyer::Foyer;

/// An artifact's name: the SHA-256 of its bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    pub fn of(body: &[u8]) -> Self {
        Hash(Sha256::digest(body).into())
    }

    /// Parses 64 lowercase hex digits, as [`Hash`] displays, S3 keys and inventories spell it.
    pub fn parse(hex: &str) -> Option<Self> {
        let digit = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        };
        if hex.len() != 64 {
            return None;
        }
        let mut hash = [0; 32];
        for (byte, pair) in hash.iter_mut().zip(hex.as_bytes().chunks(2)) {
            *byte = digit(pair[0])? << 4 | digit(pair[1])?;
        }
        Some(Hash(hash))
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

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
pub use store::Artifacts;

#[cfg(not(madsim))]
mod store {
    use std::sync::Arc;

    use bedrock_lib::health::storage::{State, StorageHealth};
    use bedrock_lib::s3::{get_checked, put_checked};
    use bedrock_lib::storage::{ErrorContext, StorageError, StorageResult};
    use bytes::Bytes;
    use object_store::path::Path;
    use object_store::{ObjectStore, PutMode};

    use super::{Body, CacheStorage, Hash, body, collect};

    /// The health manager's source for the artifact cache.
    pub const DATASET: &str = bedrock_lib::datasets::capabilities::CDN;
    pub const LAYER: &str = "artifacts";

    /// Artifacts in S3 by SHA-256, read through a local cache.
    pub struct Artifacts {
        s3: Arc<dyn ObjectStore>,
        cache: Arc<dyn CacheStorage>,
        health: Arc<StorageHealth>,
    }

    fn path(hash: &Hash) -> Path {
        Path::from(format!("artifacts/{hash}"))
    }

    fn context(engine: &'static str, hash: &Hash, detail: &str) -> ErrorContext {
        ErrorContext {
            engine,
            instance: DATASET.into(),
            file: Some(path(hash).to_string()),
            partition: None,
            detail: detail.into(),
        }
    }

    impl Artifacts {
        pub fn new(
            s3: Arc<dyn ObjectStore>,
            cache: Arc<dyn CacheStorage>,
            health: Arc<StorageHealth>,
        ) -> Self {
            Artifacts { s3, cache, health }
        }

        /// Stores `body` in S3 under its SHA-256, which it returns. Uploading an artifact that exists succeeds.
        pub async fn upload(&self, body: Body) -> StorageResult<Hash> {
            let body = collect(body).await?;
            let hash = Hash::of(&body);
            match put_checked(&*self.s3, &path(&hash), body.into(), PutMode::Create).await {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(hash),
                // Backend errors can carry configured URLs, so only the classification is kept.
                Err(_) => Err(StorageError::Unavailable(context(
                    "object_store",
                    &hash,
                    "artifact upload failed",
                ))),
            }
        }

        /// The verified artifact, or `Ok(None)` when S3 has no artifact by that hash.
        pub async fn fetch(&self, hash: &Hash) -> StorageResult<Option<Body>> {
            if self.healthy()
                && let Some(bytes) = self.cached(hash).await
            {
                return Ok(Some(body(bytes)));
            }
            let Some(bytes) = get_checked(&*self.s3, &path(hash)).await? else {
                return Ok(None);
            };
            // `get_checked` matched the body to its metadata; the name must match too. S3 damage is not local damage.
            let bytes = Bytes::from(bytes);
            if Hash::of(&bytes) != *hash {
                return Err(StorageError::Corruption(context(
                    "object_store",
                    hash,
                    "SHA-256 differs from the artifact's name",
                )));
            }
            // Rechecked: other fetches may have quarantined the cache meanwhile.
            if self.healthy()
                && let Err(error) = self.cache.put(hash, body(bytes.clone())).await
            {
                self.health.report_contained(DATASET, LAYER, &error);
            }
            Ok(Some(body(bytes)))
        }

        /// Whether the cache is in service; a quarantined one is bypassed.
        fn healthy(&self) -> bool {
            self.health
                .get(DATASET, LAYER)
                .is_none_or(|report| report.state == State::Healthy)
        }

        /// The cached artifact if it verifies. Otherwise it is evicted and reported, and the caller refetches it.
        async fn cached(&self, hash: &Hash) -> Option<Bytes> {
            let error = match self.cache.get(hash).await {
                Ok(None) => return None,
                Ok(Some(body)) => match collect(body).await {
                    Ok(bytes) if Hash::of(&bytes) == *hash => {
                        self.health.verified(DATASET, LAYER);
                        return Some(bytes);
                    }
                    Ok(_) => StorageError::Corruption(context(
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
}

#[cfg(all(test, not(madsim)))]
#[path = "../tests/unit/artifact_storage.rs"]
mod tests;
