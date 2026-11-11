//! The customer-KV namespace directory (#56 / B-25, §4 decision 11). Each namespace is one record at
//! `kv/namespaces/{id}` in Quicksilver's `infrastructure` namespace, so every region resolves the same storage identity
//! and home region from the same replicated bytes.
//!
//! - **IDs** are 32 lowercase hex characters, generated here and never reused. They are valid writer and storage
//!   identities, so a namespace's database lives at `kv/{id}/` (the writer's `{dataset}/{namespace}` layout). Bindings
//!   reference the ID; `account` and `title` are descriptive only.
//! - **Nothing locational** is recorded: no cell, origin or cache holder. Adding a cell changes no record.
//! - **Create** writes only the record. The writer opens the namespace's database on its first data write, so an empty
//!   namespace has no engine objects.
//! - **Delete** marks the record `Deleting`, which closes it to new opens and writes at once. Removing its S3 objects
//!   waits for B-39's reader lifecycle ([`retire_storage`]).
//! - Writes are planned from a read and committed with `If-Match` on the `infrastructure` head. The answering range may
//!   lag the head; a 412 carries the head, and the operation re-reads at it and plans again.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bedrock_lib::admin::Admin;
use bedrock_lib::client::Client;
use bedrock_lib::transport::tls::Tls;
use http_body_util::Full;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prost::Message;
use serde::Deserialize;

/// Where the directory lives.
pub const SERVICE: &str = "quicksilver";
pub const DATASET: &str = bedrock_lib::datasets::capabilities::QUICKSILVER;
pub const NAMESPACE: &str = "infrastructure";
pub const PREFIX: &str = "kv/namespaces/";
/// Records this code writes; any other version is refused rather than misread.
pub const FORMAT_VERSION: u32 = 1;
/// Remote regions cache values returned by the home reader for this long (fixed, B-27).
pub const CACHE_TTL_SECONDS: u32 = 60;
/// Reads, writes and conflicts one operation may go through before giving up.
const ATTEMPTS: usize = 4;
const API_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest `account`, `title` and `home_region`.
const MAX_LABEL: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum Lifecycle {
    Active = 0,
    /// Closed to new opens and writes; its storage is retired once B-39 accounts for readers and checkpoints.
    Deleting = 1,
}

#[derive(Clone, PartialEq, Message)]
pub struct Record {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(string, tag = "2")]
    pub id: String,
    #[prost(string, tag = "3")]
    pub account: String,
    #[prost(string, tag = "4")]
    pub title: String,
    /// The only region that opens the namespace's database; its writer is that region's API writer.
    #[prost(string, tag = "5")]
    pub home_region: String,
    /// The namespace's S3 database prefix, `kv/{id}`.
    #[prost(string, tag = "6")]
    pub storage: String,
    /// The materialized-state format the database was created for.
    #[prost(uint32, tag = "7")]
    pub storage_format: u32,
    /// Total persistent size limit; unset means none.
    #[prost(uint64, optional, tag = "8")]
    pub storage_bytes: Option<u64>,
    // ponytail: opaque grants until Auth defines their shape (B-42).
    #[prost(string, repeated, tag = "9")]
    pub permissions: Vec<String>,
    #[prost(uint32, tag = "10")]
    pub cache_ttl_seconds: u32,
    #[prost(enumeration = "Lifecycle", tag = "11")]
    pub state: i32,
    /// Wall-clock milliseconds since the Unix epoch.
    #[prost(uint64, tag = "12")]
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Invalid(String),
    /// Quota or deletion refused the operation.
    Rejected(Rejected),
    /// The directory kept moving under this operation; retry later.
    Contended,
    Unavailable(String),
    /// A stored record could not be decoded or failed validation.
    Corrupt(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejected {
    Deleted,
    StorageQuota,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("namespace not found"),
            Self::Invalid(detail) => write!(f, "invalid request: {detail}"),
            Self::Rejected(Rejected::Deleted) => f.write_str("namespace is being deleted"),
            Self::Rejected(Rejected::StorageQuota) => {
                f.write_str("namespace storage quota exceeded")
            }
            Self::Contended => f.write_str("the directory changed during the operation; retry"),
            Self::Unavailable(detail) => write!(f, "directory unavailable: {detail}"),
            Self::Corrupt(detail) => write!(f, "directory record is corrupt: {detail}"),
        }
    }
}

/// The directory key of namespace `id`.
pub fn key(id: &str) -> Vec<u8> {
    [PREFIX.as_bytes(), id.as_bytes()].concat()
}

/// The S3 database prefix of namespace `id`: the writer stores dataset `kv`, namespace `id` under `kv/{id}/`.
pub fn storage(id: &str) -> String {
    format!("kv/{id}")
}

pub fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A fresh namespace ID from this node, a wall-clock time and a per-process counter. Collisions are caught by the
/// conditional create, so this need not be secret, only spread out.
pub fn new_id(node: &str, at: u64, counter: u64) -> String {
    let input = [at.to_be_bytes(), counter.to_be_bytes()].concat();
    let high = bedrock_lib::placement::hash_of("kv-namespace-id", node, &input);
    let low = bedrock_lib::placement::hash_of("kv-namespace-id-low", node, &input);
    format!("{high:016x}{low:016x}")
}

/// A stored record, validated: unknown versions, mismatched keys and identities, and other cache policies are
/// corruption, never a namespace.
pub fn decode(id: &str, bytes: &[u8]) -> Result<Record, Error> {
    let record = Record::decode(bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
    let problem = if record.format_version != FORMAT_VERSION {
        Some("unknown format version")
    } else if record.id != id || !valid_id(id) {
        Some("record ID does not match its key")
    } else if record.storage != storage(id) {
        Some("storage identity is not derived from the ID")
    } else if record.cache_ttl_seconds != CACHE_TTL_SECONDS {
        Some("unsupported cache policy")
    } else if Lifecycle::try_from(record.state).is_err() {
        Some("unknown lifecycle state")
    } else {
        None
    };
    match problem {
        Some(problem) => Err(Error::Corrupt(format!("{id}: {problem}"))),
        None => Ok(record),
    }
}

/// Whether a new reader or write may open the namespace. Callers check a fresh directory read, so a warm cache or
/// reader never bypasses deletion.
pub fn admit_open(record: &Record) -> Result<(), Rejected> {
    match record.state() {
        Lifecycle::Active => Ok(()),
        Lifecycle::Deleting => Err(Rejected::Deleted),
    }
}

/// Whether a write adding `added` bytes to a namespace holding `current` fits its storage quota.
pub fn admit_write(record: &Record, current: u64, added: u64) -> Result<(), Rejected> {
    admit_open(record)?;
    match record.storage_bytes {
        Some(limit) if current.saturating_add(added) > limit => Err(Rejected::StorageQuota),
        _ => Ok(()),
    }
}

/// Deletes namespace `id`'s S3 database under `kv/{id}/` once its record is `Deleting`.
pub async fn retire_storage(_id: &str) {
    todo!("#57 / B-39: account for open readers and checkpoints, then delete kv/{{id}}/ only")
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OperationLabels {
    operation: &'static str,
    outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QuotaLabels {
    quota: &'static str,
}

/// `bedrock-api`'s write endpoint.
pub struct Api {
    pub address: String,
    pub token: String,
    pub tls: Option<Arc<Tls>>,
}

enum Put {
    Committed { revision: u64 },
    Conflict { head: u64 },
}

impl Api {
    async fn put(
        &self,
        id: &str,
        record: &Record,
        expected: u64,
        idempotency: &str,
    ) -> Result<Put, Error> {
        let path = format!(
            "/v1/{DATASET}/{NAMESPACE}/{}",
            String::from_utf8(key(id)).expect("hex IDs are UTF-8")
        );
        let request = hyper::Request::put(path)
            .header("host", bedrock_lib::transport::host_of(&self.address))
            .header("authorization", format!("Bearer {}", self.token))
            .header("if-match", expected.to_string())
            .header("idempotency-key", idempotency)
            .body(Full::new(Bytes::from(record.encode_to_vec())))
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let (status, body) =
            bedrock_lib::http::send(&self.address, self.tls.as_deref(), request, API_TIMEOUT)
                .await
                .map_err(Error::Unavailable)?;
        let body = String::from_utf8_lossy(&body);
        match status {
            StatusCode::OK => body
                .trim()
                .parse()
                .map(|revision| Put::Committed { revision })
                .map_err(|_| Error::Unavailable(format!("200 without a revision: {body}"))),
            StatusCode::PRECONDITION_FAILED => body
                .trim()
                .parse()
                .map(|head| Put::Conflict { head })
                .map_err(|_| Error::Unavailable(format!("412 without a head: {body}"))),
            status => Err(Error::Unavailable(format!("api answered {status}: {body}"))),
        }
    }
}

pub struct Directory {
    client: Arc<Client>,
    api: Api,
    node: String,
    ids: AtomicU64,
    operations: Family<OperationLabels, Counter>,
    quota_rejections: Family<QuotaLabels, Counter>,
}

/// What a caller asks for when creating a namespace.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Create {
    pub account: String,
    pub title: String,
    pub home_region: String,
    #[serde(default)]
    pub storage_bytes: Option<u64>,
    #[serde(default)]
    pub permissions: Vec<String>,
}

impl Create {
    fn validate(&self) -> Result<(), Error> {
        for (field, value) in [
            ("account", &self.account),
            ("title", &self.title),
            ("home_region", &self.home_region),
        ] {
            if value.is_empty() || value.len() > MAX_LABEL || value.chars().any(char::is_control) {
                return Err(Error::Invalid(format!(
                    "{field} must be 1 to {MAX_LABEL} bytes without control characters"
                )));
            }
        }
        Ok(())
    }
}

impl Directory {
    pub fn new(admin: &Admin, client: Arc<Client>, api: Api, node: String) -> Self {
        let operations = Family::default();
        admin.register(
            "kv_namespace_operations",
            "Namespace directory operations by outcome",
            operations.clone(),
        );
        let quota_rejections = Family::default();
        admin.register(
            "kv_quota_rejections",
            "Writes refused by a namespace quota or deletion",
            quota_rejections.clone(),
        );
        Self {
            client,
            api,
            node,
            ids: AtomicU64::new(0),
            operations,
            quota_rejections,
        }
    }

    fn count<T>(&self, operation: &'static str, result: &Result<T, Error>) {
        let outcome = match result {
            Ok(_) => "ok",
            Err(Error::NotFound) => "not_found",
            Err(Error::Invalid(_)) => "invalid",
            Err(Error::Rejected(_)) => "rejected",
            Err(Error::Contended) => "contended",
            Err(Error::Unavailable(_)) => "unavailable",
            Err(Error::Corrupt(_)) => "corrupt",
        };
        self.operations
            .get_or_create(&OperationLabels { operation, outcome })
            .inc();
    }

    /// [`admit_write`], counted for the quota metrics. Only B-26's write path calls it.
    pub fn admit_write(&self, record: &Record, current: u64, added: u64) -> Result<(), Rejected> {
        let admitted = admit_write(record, current, added);
        if let Err(rejected) = admitted {
            let quota = match rejected {
                Rejected::Deleted => "deleted",
                Rejected::StorageQuota => "storage_bytes",
            };
            self.quota_rejections
                .get_or_create(&QuotaLabels { quota })
                .inc();
        }
        admitted
    }

    /// Namespace `id` and the directory revision that proves it, from a holder at least at `min_revision`.
    pub async fn get(&self, id: &str, min_revision: u64) -> Result<(Record, u64), Error> {
        let found = self.read(id, min_revision).await;
        self.count("get", &found);
        let (record, revision) = found?;
        Ok((record.ok_or(Error::NotFound)?, revision))
    }

    async fn read(&self, id: &str, min_revision: u64) -> Result<(Option<Record>, u64), Error> {
        if !valid_id(id) {
            return Err(Error::NotFound);
        }
        let found = self
            .client
            .get(SERVICE, DATASET, NAMESPACE, &key(id), min_revision)
            .await
            .map_err(|status| Error::Unavailable(status.message().to_owned()))?;
        let record = found.value.map(|bytes| decode(id, &bytes)).transpose()?;
        Ok((record, found.revision))
    }

    /// Creates a namespace and returns it with the directory revision that holds it.
    pub async fn create(&self, request: Create) -> Result<(Record, u64), Error> {
        let created = self.try_create(request).await;
        self.count("create", &created);
        created
    }

    async fn try_create(&self, request: Create) -> Result<(Record, u64), Error> {
        request.validate()?;
        let mut floor = 0;
        for _ in 0..ATTEMPTS {
            let at = wall_ms();
            let id = new_id(&self.node, at, self.ids.fetch_add(1, Ordering::Relaxed));
            let (existing, revision) = self.read(&id, floor).await?;
            if existing.is_some() {
                continue;
            }
            let record = Record {
                format_version: FORMAT_VERSION,
                id: id.clone(),
                account: request.account.clone(),
                title: request.title.clone(),
                home_region: request.home_region.clone(),
                storage: storage(&id),
                storage_format: bedrock_lib::replication::materialized::FORMAT_VERSION,
                storage_bytes: request.storage_bytes,
                permissions: request.permissions.clone(),
                cache_ttl_seconds: CACHE_TTL_SECONDS,
                state: Lifecycle::Active as i32,
                created_at: at,
            };
            match self
                .api
                .put(&id, &record, revision, &format!("create:{id}"))
                .await?
            {
                Put::Committed { revision } => return Ok((record, revision)),
                Put::Conflict { head } => floor = head,
            }
        }
        Err(Error::Contended)
    }

    /// Marks namespace `id` `Deleting` and returns it with the directory revision that holds that; deleting one that already
    /// is changes nothing. A read at that revision or later, in any region, sees the deletion.
    pub async fn delete(&self, id: &str) -> Result<(Record, u64), Error> {
        let deleted = self.try_delete(id).await;
        self.count("delete", &deleted);
        deleted
    }

    async fn try_delete(&self, id: &str) -> Result<(Record, u64), Error> {
        let mut floor = 0;
        for _ in 0..ATTEMPTS {
            let (record, revision) = self.read(id, floor).await?;
            let mut record = record.ok_or(Error::NotFound)?;
            if record.state() == Lifecycle::Deleting {
                return Ok((record, revision));
            }
            record.set_state(Lifecycle::Deleting);
            match self
                .api
                .put(id, &record, revision, &format!("delete:{id}"))
                .await?
            {
                // Storage stays until B-39 can retire it safely (`retire_storage`).
                Put::Committed { revision } => return Ok((record, revision)),
                Put::Conflict { head } => floor = head,
            }
        }
        Err(Error::Contended)
    }

    /// Up to `limit` namespaces after ID `after`, in ID order, optionally of one account, with the cursor for the next
    /// page and the lowest revision among the ranges read. ponytail: the account filter applies after paging, so a page may come back short.
    pub async fn list(
        &self,
        account: Option<&str>,
        after: &str,
        limit: u32,
    ) -> Result<(Vec<Record>, Option<String>, u64), Error> {
        let listed = self.try_list(account, after, limit).await;
        self.count("list", &listed);
        listed
    }

    async fn try_list(
        &self,
        account: Option<&str>,
        after: &str,
        limit: u32,
    ) -> Result<(Vec<Record>, Option<String>, u64), Error> {
        let start_after = if after.is_empty() {
            Vec::new()
        } else {
            key(after)
        };
        let page = self
            .client
            .list(
                SERVICE,
                DATASET,
                NAMESPACE,
                PREFIX.as_bytes(),
                &start_after,
                limit,
                0,
            )
            .await
            .map_err(|status| Error::Unavailable(status.message().to_owned()))?;
        let mut records = Vec::new();
        let mut last = None;
        for (key, value) in page.entries {
            let id = String::from_utf8(key[PREFIX.len()..].to_vec())
                .map_err(|_| Error::Corrupt("directory key is not UTF-8".into()))?;
            let record = decode(&id, &value)?;
            if account.is_none_or(|account| record.account == account) {
                records.push(record);
            }
            last = Some(id);
        }
        Ok((records, last.filter(|_| page.more), page.revision))
    }
}

/// Wall time is the point here (IDs and `created_at`); madsim simulates it.
#[allow(clippy::disallowed_methods)]
fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// `POST/GET /v1/namespaces` and `GET/DELETE /v1/namespaces/{id}`. Callers authenticate in front of these.
pub fn routes(directory: Arc<Directory>) -> Router {
    Router::new()
        .route("/v1/namespaces", get(list).post(create))
        .route("/v1/namespaces/{id}", get(read).delete(delete))
        .with_state(directory)
}

#[derive(Deserialize)]
struct GetQuery {
    #[serde(default)]
    min_revision: u64,
}

#[derive(Deserialize)]
struct ListQuery {
    account: Option<String>,
    #[serde(default)]
    cursor: String,
    #[serde(default)]
    limit: u32,
}

async fn create(State(directory): State<Arc<Directory>>, body: Bytes) -> Response {
    let request: Create = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => return failure(Error::Invalid(error.to_string())),
    };
    match directory.create(request).await {
        Ok((record, revision)) => {
            (StatusCode::CREATED, Json(json(&record, revision))).into_response()
        }
        Err(error) => failure(error),
    }
}

async fn read(
    State(directory): State<Arc<Directory>>,
    Path(id): Path<String>,
    Query(query): Query<GetQuery>,
) -> Response {
    match directory.get(&id, query.min_revision).await {
        Ok((record, revision)) => Json(json(&record, revision)).into_response(),
        Err(error) => failure(error),
    }
}

async fn delete(State(directory): State<Arc<Directory>>, Path(id): Path<String>) -> Response {
    match directory.delete(&id).await {
        Ok((record, revision)) => Json(json(&record, revision)).into_response(),
        Err(error) => failure(error),
    }
}

async fn list(State(directory): State<Arc<Directory>>, Query(query): Query<ListQuery>) -> Response {
    match directory
        .list(query.account.as_deref(), &query.cursor, query.limit)
        .await
    {
        Ok((records, cursor, revision)) => Json(serde_json::json!({
            "namespaces": records.iter().map(|r| json(r, revision)).collect::<Vec<_>>(),
            "cursor": cursor,
        }))
        .into_response(),
        Err(error) => failure(error),
    }
}

/// A record as the management API shows it, with the directory `revision` it was read or written at.
fn json(record: &Record, revision: u64) -> serde_json::Value {
    serde_json::json!({
        "revision": revision,
        "id": record.id,
        "account": record.account,
        "title": record.title,
        "home_region": record.home_region,
        "storage": record.storage,
        "storage_format": record.storage_format,
        "storage_bytes": record.storage_bytes,
        "permissions": record.permissions,
        "cache_ttl_seconds": record.cache_ttl_seconds,
        "state": match record.state() {
            Lifecycle::Active => "active",
            Lifecycle::Deleting => "deleting",
        },
        "created_at": record.created_at,
    })
}

fn failure(error: Error) -> Response {
    let status = match error {
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::Rejected(_) | Error::Contended => StatusCode::CONFLICT,
        Error::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        Error::Corrupt(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}

#[cfg(test)]
#[path = "../tests/unit/namespaces.rs"]
mod tests;
