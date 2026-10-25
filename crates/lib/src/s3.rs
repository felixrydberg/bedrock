//! L3 object store for KV snapshots and log archives and for CDN artifact bytes (B-07, `kv.md` §14, `cdn.md` §3).
//!
//! Callers use [`ObjectStore`]. Normal builds back it with S3 ([`aws::S3Store`]); simulation builds use
//! [`memory::MemoryStore`], because the simulated S3 SDK has no conditional writes or checksums. Both pass the suite
//! in `s3/contract.rs`.
//!
//! Every object is stored with the SHA-256 of its body. S3 checks it on write and the store checks it again on every
//! read, so a damaged object is an error, never data.

#[cfg(not(madsim))]
pub mod aws;
pub mod memory;

#[cfg(test)]
mod contract;

use std::fmt;

use serde::Deserialize;
use sha2::{Digest, Sha256};

pub type S3Result<T> = Result<T, S3Error>;

/// An object store failure. A missing key is not one: reads return `Ok(None)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3Error {
    /// A [`Put::Create`] found the key already holding a different body.
    AlreadyExists { key: String },
    /// The body does not match the checksum it was stored with, or has none.
    Corruption { key: String, detail: String },
    /// Network failure, timeout or server error that outlasted the retries. Retrying later may succeed.
    Unavailable { detail: String },
    /// Any other rejection, e.g. bad credentials or a missing bucket.
    Failed { detail: String },
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3Error::AlreadyExists { key } => write!(f, "object {key} already exists"),
            S3Error::Corruption { key, detail } => write!(f, "corrupt object {key}: {detail}"),
            S3Error::Unavailable { detail } => write!(f, "object store unavailable: {detail}"),
            S3Error::Failed { detail } => write!(f, "object store request failed: {detail}"),
        }
    }
}

impl std::error::Error for S3Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Put {
    Overwrite,
    /// Write only if the key is absent. Finding the same body already there also succeeds, so a retried create
    /// (whose first attempt landed but whose response was lost) is not mistaken for losing a race.
    Create,
}

/// One bucket of objects. Implementations pass the contract suite in `s3/contract.rs`.
pub trait ObjectStore: Send + Sync {
    /// The verified body, or `Ok(None)` when the key does not exist.
    fn get(&self, key: &str) -> impl Future<Output = S3Result<Option<Vec<u8>>>> + Send;

    fn put(&self, key: &str, body: Vec<u8>, mode: Put)
    -> impl Future<Output = S3Result<()>> + Send;

    /// Every key starting with `prefix`, in byte order.
    fn list(&self, prefix: &str) -> impl Future<Output = S3Result<Vec<String>>> + Send;

    /// Deleting a missing key succeeds.
    fn delete(&self, key: &str) -> impl Future<Output = S3Result<()>> + Send;
}

/// Connection settings for [`aws::S3Store`]. The endpoint is configurable so SeaweedFS (or another S3-compatible store) can stand in for S3.
#[derive(Clone, Deserialize)]
pub struct S3Config {
    /// e.g. `http://s3:8333`. Requests use path-style addressing, which S3-compatible stores need.
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Attempts per request, including the first; the SDK backs off exponentially, with jitter, between them.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Limit per attempt, so a stalled connection is retried instead of hanging.
    #[serde(default = "default_attempt_timeout_ms")]
    pub attempt_timeout_ms: u64,
}

fn default_region() -> String {
    "us-east-1".into()
}

fn default_max_attempts() -> u32 {
    3
}

fn default_attempt_timeout_ms() -> u64 {
    30_000
}

// Keeps the secret out of logs.
impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("max_attempts", &self.max_attempts)
            .field("attempt_timeout_ms", &self.attempt_timeout_ms)
            .finish()
    }
}

/// One version of a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub version: u64,
    pub body: Vec<u8>,
}

/// Writes manifest `version` under `{prefix}/manifests/`. Every version is its own write-once object, so this is a
/// compare-and-swap with every other writer of `version`: `AlreadyExists` means one got there first and the caller
/// should re-read [`latest_manifest`].
pub async fn put_manifest(
    store: &impl ObjectStore,
    prefix: &str,
    version: u64,
    body: Vec<u8>,
) -> S3Result<()> {
    // Zero-padded to u64's 20 digits, so byte order is version order.
    let key = format!("{prefix}/manifests/{version:020}");
    store.put(&key, body, Put::Create).await
}

/// The highest manifest version under `prefix`, or `None` if none was written.
// ponytail: lists every version; prune old ones with snapshot retention (B-11) or list from the end if this grows.
pub async fn latest_manifest(store: &impl ObjectStore, prefix: &str) -> S3Result<Option<Manifest>> {
    let dir = format!("{prefix}/manifests/");
    let Some(key) = store.list(&dir).await?.pop() else {
        return Ok(None);
    };
    let version = key[dir.len()..].parse().map_err(|_| S3Error::Corruption {
        key: key.clone(),
        detail: "manifest key is not a version number".into(),
    })?;
    match store.get(&key).await? {
        Some(body) => Ok(Some(Manifest { version, body })),
        None => Err(S3Error::Unavailable {
            detail: format!("{key} was deleted while being read"),
        }),
    }
}

fn sha256(body: &[u8]) -> [u8; 32] {
    Sha256::digest(body).into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Returns `body` if it matches `expected`, the hex SHA-256 it was stored with.
fn verify(key: &str, body: Vec<u8>, expected: Option<&str>) -> S3Result<Vec<u8>> {
    let corruption = |detail: String| S3Error::Corruption {
        key: key.into(),
        detail,
    };
    let expected = expected.ok_or_else(|| corruption("no stored checksum".into()))?;
    let actual = hex(&sha256(&body));
    if actual != expected {
        return Err(corruption(format!("sha256 {actual}, stored {expected}")));
    }
    Ok(body)
}
