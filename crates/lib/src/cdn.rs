//! The `cdn` dataset's records and its publish client, shared by `bedrock-cdn` and its callers (`cdn.md` §3,
//! `feature-drafts.md` §4 decisions 14 and 17). They live here because the deploy control plane (B-50) cannot depend on
//! `bedrock-cdn`.
//!
//! | Key | Value | Placed by |
//! |---|---|---|
//! | `path/{deployment_id}/{path}` | [`PathRow`] | the normal `(dataset, namespace, key)` hash |
//! | `ref/{sha256}/{deployment_id}` | empty | the artifact's content hash ([`crate::placement::hash_of`]) |
//!
//! Namespaces and deployment IDs are opaque to the CDN. A deployment ID is any non-empty string without `/`, so a key
//! has one parse; the path is the rest of the key.
//!
//! [`Publisher`] (#81 / B-33) uploads a deployment's artifacts, then writes its ref keys and path rows in bulks (B-09c).
//! Ref keys commit before path rows and path rows are deleted before ref keys, so a visible path row's artifact is always
//! stored and retained. Every step writes the same objects and keys on a rerun, so a killed publish or release converges
//! when run again.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::StatusCode;
use prost::Message;
use sha2::{Digest, Sha256};

use crate::datasets::capabilities::CDN;
use crate::replication::materialized::validate_scope;
use crate::replication::writer::{CommitError, Write, Writer, encoded_change_len};
use crate::replication::{Operation, pb};
use crate::storage::{ErrorContext, StorageResult};
use crate::transport::tls::Tls;

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

/// Where artifacts live for good: S3, or a model of it in simulation. It returns what it holds; readers check that the
/// bytes match their name.
#[async_trait]
pub trait Origin: Send + Sync {
    /// Stores `bytes` under `hash`; storing an artifact that exists succeeds.
    async fn put(&self, hash: &Hash, bytes: Bytes) -> StorageResult<()>;
    /// The bytes stored under `hash`, or `Ok(None)`.
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Bytes>>;
}

/// An artifact's object name in S3.
pub fn object_path(hash: &Hash) -> String {
    format!("artifacts/{hash}")
}

/// Names the artifact in a storage error, for the storage-health manager.
pub fn error_context(engine: &'static str, hash: &Hash, detail: &str) -> ErrorContext {
    ErrorContext {
        engine,
        instance: CDN.into(),
        file: Some(object_path(hash)),
        partition: None,
        detail: detail.into(),
    }
}

// Native S3 I/O stays outside simulation (B-23a); simulation supplies its own [`Origin`].
#[cfg(not(madsim))]
pub use s3::S3;

#[cfg(not(madsim))]
mod s3 {
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::body::Bytes;
    use object_store::{ObjectStore, PutMode};

    use super::{Hash, Origin, error_context, object_path};
    use crate::s3::{get_checked, put_checked};
    use crate::storage::{StorageError, StorageResult};

    /// Artifacts in S3 under `artifacts/{sha256}`.
    pub struct S3(pub Arc<dyn ObjectStore>);

    #[async_trait]
    impl Origin for S3 {
        async fn put(&self, hash: &Hash, bytes: Bytes) -> StorageResult<()> {
            let path = object_path(hash).into();
            match put_checked(&*self.0, &path, bytes.into(), PutMode::Create).await {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
                // Backend errors can carry configured URLs, so only the classification is kept.
                Err(_) => Err(StorageError::Unavailable(error_context(
                    "object_store",
                    hash,
                    "artifact upload failed",
                ))),
            }
        }

        async fn get(&self, hash: &Hash) -> StorageResult<Option<Bytes>> {
            // `get_checked` matches the body to its metadata.
            let bytes = get_checked(&*self.0, &object_path(hash).into()).await?;
            Ok(bytes.map(Bytes::from))
        }
    }
}

/// The value-encoding version. A row with another version is [`Malformed`].
const VERSION: u8 = 1;
/// Version, SHA-256 and size; the content type follows.
const HEADER: usize = 1 + 32 + 8;

/// Which artifact a URL path serves, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathRow {
    pub sha256: Hash,
    /// The `Content-Type` header value: visible ASCII, spaces and tabs.
    pub content_type: String,
    /// The artifact's size in bytes.
    pub size: u64,
}

/// A key or value that is not in the canonical form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Malformed(pub &'static str);

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed cdn record: {}", self.0)
    }
}

impl std::error::Error for Malformed {}

impl PathRow {
    /// `{version}{sha256}{size as u64 BE}{content type}`.
    pub fn encode(&self) -> Result<Vec<u8>, Malformed> {
        header_value(&self.content_type)?;
        let mut out = Vec::with_capacity(HEADER + self.content_type.len());
        out.push(VERSION);
        out.extend_from_slice(&self.sha256.0);
        out.extend_from_slice(&self.size.to_be_bytes());
        out.extend_from_slice(self.content_type.as_bytes());
        Ok(out)
    }

    pub fn decode(value: &[u8]) -> Result<Self, Malformed> {
        if value.len() < HEADER {
            return Err(Malformed("path row too short"));
        }
        if value[0] != VERSION {
            return Err(Malformed("unknown path row version"));
        }
        let content_type = std::str::from_utf8(&value[HEADER..])
            .map_err(|_| Malformed("content type is not UTF-8"))?;
        header_value(content_type)?;
        Ok(PathRow {
            sha256: Hash(value[1..33].try_into().unwrap()),
            size: u64::from_be_bytes(value[33..HEADER].try_into().unwrap()),
            content_type: content_type.to_owned(),
        })
    }
}

/// `path/{deployment_id}/{path}`.
pub fn path_key(deployment_id: &str, path: &str) -> Result<Vec<u8>, Malformed> {
    deployment(deployment_id)?;
    Ok(format!("path/{deployment_id}/{path}").into_bytes())
}

/// `ref/{sha256}/{deployment_id}`: keeps the artifact while it exists.
pub fn ref_key(sha256: &Hash, deployment_id: &str) -> Result<Vec<u8>, Malformed> {
    deployment(deployment_id)?;
    Ok(format!("{}{deployment_id}", ref_prefix(sha256)).into_bytes())
}

/// `ref/{sha256}/`: the prefix of every ref key of one artifact, whatever its deployment.
pub fn ref_prefix(sha256: &Hash) -> String {
    format!("ref/{sha256}/")
}

fn deployment(id: &str) -> Result<(), Malformed> {
    if id.is_empty() || id.contains('/') {
        return Err(Malformed("deployment ID is empty or contains '/'"));
    }
    Ok(())
}

/// A valid HTTP field value, so the Gateway can serve it as `Content-Type`.
fn header_value(value: &str) -> Result<(), Malformed> {
    if value
        .bytes()
        .all(|b| b == b'\t' || (b' '..=b'~').contains(&b))
    {
        Ok(())
    } else {
        Err(Malformed("content type is not a valid header value"))
    }
}

/// Changes of one bulk: a key and its value, or `None` to delete it.
pub type Changes = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Why a publish or release stopped. Nothing it wrote needs undoing: rerun it, or release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The request can never succeed: a malformed key or row, a duplicate path, or a change over the bulk cap.
    Invalid(String),
    /// S3 or the write API failed after every attempt; a rerun may succeed.
    Unavailable(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Invalid(reason) => write!(f, "invalid cdn publish: {reason}"),
            Error::Unavailable(reason) => write!(f, "cdn publish unavailable: {reason}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<Malformed> for Error {
    fn from(error: Malformed) -> Self {
        Error::Invalid(error.to_string())
    }
}

/// Commits one bulk to a namespace of the `cdn` dataset and returns its revision.
#[async_trait]
pub trait Bulks: Send + Sync {
    async fn commit(&self, namespace: &str, changes: Changes) -> Result<u64, Error>;
}

/// An in-process writer, as in simulation and on the API node itself.
#[async_trait]
impl Bulks for Arc<Writer> {
    async fn commit(&self, namespace: &str, changes: Changes) -> Result<u64, Error> {
        let write = Write {
            dataset: CDN.into(),
            namespace: namespace.into(),
            key: Vec::new(),
            operation: Operation::Bulk(changes),
            // A bulk sets or deletes fixed values, so committing it twice is harmless. An idempotency key would make
            // a republish after a release return the old revision and write nothing.
            idempotency_key: None,
            expected: None,
        };
        Writer::commit(self, write)
            .await
            .map_err(|error| match error {
                CommitError::Invalid(reason) => Error::Invalid(reason),
                error => Error::Unavailable(error.to_string()),
            })
    }
}

/// `bedrock-api`'s bulk endpoint, `POST /v1/cdn/{namespace}`.
pub struct WriteApi {
    pub address: String,
    pub token: String,
    pub tls: Option<Arc<Tls>>,
}

const API_TIMEOUT: Duration = Duration::from_secs(10);

#[async_trait]
impl Bulks for WriteApi {
    async fn commit(&self, namespace: &str, changes: Changes) -> Result<u64, Error> {
        // Also keeps the namespace one URI path segment.
        scope(namespace)?;
        let changes = changes
            .into_iter()
            .map(|(key, value)| pb::Change { key, value })
            .collect();
        let request = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("/v1/{CDN}/{namespace}"))
            .header("host", crate::transport::host_of(&self.address))
            .header("authorization", format!("Bearer {}", self.token))
            .body(http_body_util::Full::new(Bytes::from(
                pb::Bulk { changes }.encode_to_vec(),
            )))
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let (status, body) =
            crate::http::send(&self.address, self.tls.as_deref(), request, API_TIMEOUT)
                .await
                .map_err(Error::Unavailable)?;
        let body = String::from_utf8_lossy(&body);
        match status {
            StatusCode::OK => body
                .trim()
                .parse()
                .map_err(|_| Error::Unavailable(format!("200 without a revision: {body}"))),
            StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE => {
                Err(Error::Invalid(format!("api answered {status}: {body}")))
            }
            status => Err(Error::Unavailable(format!("api answered {status}: {body}"))),
        }
    }
}

/// One artifact of a deployment: a static asset served at `path`, or the Worker bundle (`path: None`), which gets a ref
/// key and no path row.
#[derive(Debug, Clone)]
pub struct File {
    pub path: Option<String>,
    pub content_type: String,
    pub bytes: Bytes,
}

/// Attempts per upload or bulk before the step fails.
const ATTEMPTS: u32 = 4;

/// Publishes and releases deployments in the `cdn` dataset.
///
/// Callers run at most one publish or release of a deployment at a time, and release a deployment only once no publish
/// of it can still be running. Otherwise a publish's path rows can commit after a release deleted their ref keys, and
/// the rows would point at artifacts nothing retains.
pub struct Publisher {
    pub origin: Arc<dyn Origin>,
    pub bulks: Arc<dyn Bulks>,
    /// The write API's `bulk_max_bytes`; changes are split into bulks no larger.
    pub bulk_max_bytes: usize,
}

impl Publisher {
    /// Uploads every artifact, then writes its ref keys and the path rows. Returns the last bulk's revision: from it on,
    /// every row is readable and every artifact retained until [`Publisher::release`].
    pub async fn publish(
        &self,
        namespace: &str,
        deployment_id: &str,
        files: &[File],
    ) -> Result<u64, Error> {
        scope(namespace)?;
        let mut artifacts = BTreeMap::new();
        let mut paths = BTreeMap::new();
        for file in files {
            let sha256 = Hash::of(&file.bytes);
            artifacts.insert(sha256, file.bytes.clone());
            let Some(path) = &file.path else { continue };
            let row = PathRow {
                sha256,
                content_type: file.content_type.clone(),
                size: file.bytes.len() as u64,
            };
            let value = Some(row.encode()?);
            if paths
                .insert(path_key(deployment_id, path)?, value)
                .is_some()
            {
                return Err(Error::Invalid(format!("path {path:?} appears twice")));
            }
        }
        let refs = artifacts
            .keys()
            .map(|sha256| Ok((ref_key(sha256, deployment_id)?, Some(Vec::new()))))
            .collect::<Result<Changes, Malformed>>()?;
        // Checked before the first upload, so a publish that can never succeed stores nothing.
        let bulks = split(refs.into_iter().chain(paths).collect(), self.bulk_max_bytes)?;
        // Never skipped because the object exists: that would tell one tenant another stored the file (decision 14).
        // ponytail: one upload at a time; bound concurrency if large deployments publish too slowly.
        for (sha256, bytes) in &artifacts {
            retry(|| async {
                self.origin
                    .put(sha256, bytes.clone())
                    .await
                    .map_err(|e| Error::Unavailable(e.to_string()))
            })
            .await?;
        }
        self.write(namespace, bulks).await
    }

    /// Deletes the path rows of `paths`, then the ref keys of `artifacts`. Callers first remove their references to the
    /// deployment. Returns the last bulk's revision.
    pub async fn release(
        &self,
        namespace: &str,
        deployment_id: &str,
        paths: &[String],
        artifacts: &[Hash],
    ) -> Result<u64, Error> {
        scope(namespace)?;
        let paths = paths
            .iter()
            .map(|path| path_key(deployment_id, path))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let refs = artifacts
            .iter()
            .map(|sha256| ref_key(sha256, deployment_id))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let changes = paths.into_iter().chain(refs).map(|key| (key, None));
        let bulks = split(changes.collect(), self.bulk_max_bytes)?;
        self.write(namespace, bulks).await
    }

    /// Commits `bulks` in order, each retried; returns the last revision.
    async fn write(&self, namespace: &str, bulks: Vec<Changes>) -> Result<u64, Error> {
        let mut revision = 0;
        for bulk in bulks {
            revision = retry(|| self.bulks.commit(namespace, bulk.clone())).await?;
        }
        Ok(revision)
    }
}

/// The writer's namespace check, run before anything is uploaded or sent.
fn scope(namespace: &str) -> Result<(), Error> {
    validate_scope(CDN, namespace).map_err(|e| Error::Invalid(e.to_string()))
}

/// Consecutive runs of `changes`, each of at most `max_bytes` as the writer counts them; at least one.
fn split(changes: Changes, max_bytes: usize) -> Result<Vec<Changes>, Error> {
    let mut bulks: Vec<Changes> = Vec::new();
    let mut bytes = 0;
    for (key, value) in changes {
        let size = encoded_change_len(&key, value.as_deref());
        if size > max_bytes {
            return Err(Error::Invalid(format!(
                "a change of {size} bytes exceeds the {max_bytes}-byte bulk cap"
            )));
        }
        match bulks.last_mut() {
            Some(bulk) if bytes + size <= max_bytes => bulk.push((key, value)),
            _ => {
                bulks.push(vec![(key, value)]);
                bytes = 0;
            }
        }
        bytes += size;
    }
    if bulks.is_empty() {
        return Err(Error::Invalid(
            "a deployment has at least one artifact".into(),
        ));
    }
    Ok(bulks)
}

/// Runs `step` until it succeeds, it is [`Error::Invalid`], or [`ATTEMPTS`] have failed.
async fn retry<T, F: Future<Output = Result<T, Error>>>(step: impl Fn() -> F) -> Result<T, Error> {
    let mut attempt = 1;
    loop {
        match step().await {
            Err(Error::Unavailable(reason)) if attempt < ATTEMPTS => {
                tracing::warn!(attempt, %reason, "cdn publish step failed; retrying");
                tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/cdn.rs"]
mod tests;
