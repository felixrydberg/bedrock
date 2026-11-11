//! The customer-KV namespace directory (#56 / B-25, §4 decision 11). Each namespace is one record at
//! `kv/namespaces/{id}` in Quicksilver's `infrastructure` namespace, so every region resolves the same storage identity
//! and home region from the same replicated bytes.
//!
//! - **IDs** are random version 4 UUIDs as 32 lowercase hex characters (Cloudflare's namespace ID shape), never
//!   reused. They are valid writer and storage identities, so a namespace's database lives at `kv/{id}/` (the writer's
//!   `{dataset}/{namespace}` layout). Bindings reference the ID; `tenant` and `title` are descriptive only.
//! - **Nothing locational** is recorded: no cell, origin or cache holder. Adding a cell changes no record.
//! - **Create** writes only the record. The writer opens the namespace's database on its first data write, so an empty
//!   namespace has no engine objects.
//! - **Delete** marks the record `Deleting`, which closes it to new opens and writes at once. Removing its S3 objects
//!   waits for B-39's reader lifecycle ([`retire_storage`]).
//! - Writes are planned from a read and committed with `If-Match` on the `infrastructure` head. The answering range may
//!   lag the head; a 412 carries the head, and the operation re-reads at it and plans again.
//! - **The management API** is Cloudflare's namespace API (`/client/v4/accounts/{tenant}/storage/kv/namespaces`): the
//!   same paths, bodies, envelope and namespace object, so its clients work against a `bedrock-kv` host. Its additions
//!   are each answer's directory `revision`, `?min_revision=` on reads and lists, and quota fields on create.

use std::sync::Arc;
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
use serde_json::{Value, json};

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
/// Cloudflare's limits: a tenant is an account ID, a title at most 512 bytes.
const MAX_TENANT: usize = 32;
const MAX_TITLE: usize = 512;
const MAX_REGION: usize = 64;

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
    pub tenant: String,
    #[prost(string, tag = "4")]
    pub title: String,
    /// The only region that opens the namespace's database; its writer is that region's API writer. Cloudflare's
    /// `jurisdiction`.
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
    /// The tenant already has an active namespace with this title.
    Duplicate,
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
            Self::Duplicate => f.write_str("a namespace with this title already exists"),
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

/// What a caller asks for when creating a namespace.
#[derive(Debug, Clone, Default)]
pub struct Create {
    pub tenant: String,
    pub title: String,
    pub home_region: String,
    pub storage_bytes: Option<u64>,
    pub permissions: Vec<String>,
}

impl Create {
    fn validate(&self) -> Result<(), Error> {
        label("tenant", &self.tenant, MAX_TENANT)?;
        label("title", &self.title, MAX_TITLE)?;
        label("home_region", &self.home_region, MAX_REGION)
    }
}

fn label(field: &str, value: &str, max: usize) -> Result<(), Error> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(Error::Invalid(format!(
            "{field} must be 1 to {max} bytes without control characters"
        )));
    }
    Ok(())
}

/// Whether an active namespace of `tenant` other than `id` is titled `title`. Cloudflare refuses a duplicate title
/// until its holder is deleted.
fn taken(records: &[Record], tenant: &str, title: &str, id: &str) -> bool {
    records
        .iter()
        .any(|r| r.tenant == tenant && r.title == title && r.id != id && admit_open(r).is_ok())
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
    async fn put(&self, record: &Record, expected: u64, idempotency: &str) -> Result<Put, Error> {
        let path = format!(
            "/v1/{DATASET}/{NAMESPACE}/{}",
            String::from_utf8(key(&record.id)).expect("hex IDs are UTF-8")
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

/// A planned write: the record to commit, or the unchanged record when there is nothing to write.
enum Plan {
    Write(Record),
    Unchanged(Record),
}

pub struct Directory {
    client: Arc<Client>,
    api: Api,
    /// Home region of namespaces created without a jurisdiction: this node's region.
    region: String,
    operations: Family<OperationLabels, Counter>,
    quota_rejections: Family<QuotaLabels, Counter>,
}

impl Directory {
    pub fn new(admin: &Admin, client: Arc<Client>, api: Api, region: String) -> Self {
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
            region,
            operations,
            quota_rejections,
        }
    }

    fn count<T>(&self, operation: &'static str, result: Result<T, Error>) -> Result<T, Error> {
        let outcome = match &result {
            Ok(_) => "ok",
            Err(Error::NotFound) => "not_found",
            Err(Error::Duplicate) => "duplicate",
            Err(Error::Invalid(_)) => "invalid",
            Err(Error::Rejected(_)) => "rejected",
            Err(Error::Contended) => "contended",
            Err(Error::Unavailable(_)) => "unavailable",
            Err(Error::Corrupt(_)) => "corrupt",
        };
        self.operations
            .get_or_create(&OperationLabels { operation, outcome })
            .inc();
        result
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

    /// Namespace `id` in any state and the directory revision that proves it, from a holder at least at
    /// `min_revision`.
    pub async fn get(&self, id: &str, min_revision: u64) -> Result<(Record, u64), Error> {
        let found = self
            .read(id, min_revision)
            .await
            .and_then(|(record, revision)| Ok((record.ok_or(Error::NotFound)?, revision)));
        self.count("get", found)
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

    /// Every record, and the lowest revision among the ranges read, each at least at `min_revision`. A commit with
    /// `If-Match` on that revision succeeds only if every range was at the head, so nothing read has changed.
    /// ponytail: create, rename and list read the whole directory; a per-tenant title index when that gets slow.
    async fn scan(&self, min_revision: u64) -> Result<(Vec<Record>, u64), Error> {
        let mut records = Vec::new();
        let mut revision = u64::MAX;
        let mut start_after = Vec::new();
        loop {
            let page = self
                .client
                .list(
                    SERVICE,
                    DATASET,
                    NAMESPACE,
                    PREFIX.as_bytes(),
                    &start_after,
                    0,
                    min_revision,
                )
                .await
                .map_err(|status| Error::Unavailable(status.message().to_owned()))?;
            revision = revision.min(page.revision);
            for (key, value) in page.entries {
                let id = String::from_utf8(key[PREFIX.len()..].to_vec())
                    .map_err(|_| Error::Corrupt("directory key is not UTF-8".into()))?;
                records.push(decode(&id, &value)?);
                start_after = key;
            }
            if !page.more {
                return Ok((records, revision));
            }
        }
    }

    /// Plans a write from a [`scan`](Self::scan) and commits it, scanning again at the head after a conflict.
    async fn commit(
        &self,
        operation: &str,
        plan: impl Fn(Vec<Record>) -> Result<Plan, Error>,
    ) -> Result<(Record, u64), Error> {
        let mut floor = 0;
        for _ in 0..ATTEMPTS {
            let (records, revision) = self.scan(floor).await?;
            let record = match plan(records)? {
                Plan::Write(record) => record,
                Plan::Unchanged(record) => return Ok((record, revision)),
            };
            let idempotency = format!("{operation}:{}:{revision}", record.id);
            match self.api.put(&record, revision, &idempotency).await? {
                Put::Committed { revision } => return Ok((record, revision)),
                Put::Conflict { head } => floor = head,
            }
        }
        Err(Error::Contended)
    }

    /// Creates a namespace and returns it with the directory revision that holds it.
    pub async fn create(&self, request: Create) -> Result<(Record, u64), Error> {
        let created = match request.validate() {
            Ok(()) => {
                self.commit("create", |records| {
                    if taken(&records, &request.tenant, &request.title, "") {
                        return Err(Error::Duplicate);
                    }
                    let id = bedrock_lib::uuid();
                    if records.iter().any(|r| r.id == id) {
                        return Err(Error::Contended);
                    }
                    Ok(Plan::Write(Record {
                        format_version: FORMAT_VERSION,
                        storage: storage(&id),
                        id,
                        tenant: request.tenant.clone(),
                        title: request.title.clone(),
                        home_region: request.home_region.clone(),
                        storage_format: bedrock_lib::replication::materialized::FORMAT_VERSION,
                        storage_bytes: request.storage_bytes,
                        permissions: request.permissions.clone(),
                        cache_ttl_seconds: CACHE_TTL_SECONDS,
                        state: Lifecycle::Active as i32,
                        created_at: wall_ms(),
                    }))
                })
                .await
            }
            Err(error) => Err(error),
        };
        self.count("create", created)
    }

    /// Retitles the tenant's active namespace `id`; its ID and storage stay.
    pub async fn rename(
        &self,
        tenant: &str,
        id: &str,
        title: &str,
    ) -> Result<(Record, u64), Error> {
        let renamed = match label("title", title, MAX_TITLE) {
            Ok(()) => {
                self.commit("rename", |records| {
                    if taken(&records, tenant, title, id) {
                        return Err(Error::Duplicate);
                    }
                    let mut record = records
                        .into_iter()
                        .find(|r| r.id == id && r.tenant == tenant && admit_open(r).is_ok())
                        .ok_or(Error::NotFound)?;
                    if record.title == title {
                        return Ok(Plan::Unchanged(record));
                    }
                    record.title = title.to_owned();
                    Ok(Plan::Write(record))
                })
                .await
            }
            Err(error) => Err(error),
        };
        self.count("rename", renamed)
    }

    /// Marks the tenant's namespace `id` `Deleting` and returns it with the directory revision that holds that;
    /// deleting one that already is changes nothing. A read at that revision or later, in any region, sees the
    /// deletion.
    pub async fn delete(&self, tenant: &str, id: &str) -> Result<(Record, u64), Error> {
        let deleted = self.try_delete(tenant, id).await;
        self.count("delete", deleted)
    }

    async fn try_delete(&self, tenant: &str, id: &str) -> Result<(Record, u64), Error> {
        let mut floor = 0;
        for _ in 0..ATTEMPTS {
            let (record, revision) = self.read(id, floor).await?;
            let mut record = record
                .filter(|r| r.tenant == tenant)
                .ok_or(Error::NotFound)?;
            if record.state() == Lifecycle::Deleting {
                return Ok((record, revision));
            }
            record.set_state(Lifecycle::Deleting);
            match self
                .api
                .put(&record, revision, &format!("delete:{id}"))
                .await?
            {
                // Storage stays until B-39 can retire it safely (`retire_storage`).
                Put::Committed { revision } => return Ok((record, revision)),
                Put::Conflict { head } => floor = head,
            }
        }
        Err(Error::Contended)
    }

    /// The tenant's active namespaces in ID order, with the lowest revision among the ranges read, each at least at
    /// `min_revision`.
    pub async fn list(&self, tenant: &str, min_revision: u64) -> Result<(Vec<Record>, u64), Error> {
        let listed = self
            .scan(min_revision)
            .await
            .map(|(mut records, revision)| {
                records.retain(|r| r.tenant == tenant && admit_open(r).is_ok());
                (records, revision)
            });
        self.count("list", listed)
    }
}

/// Wall time is the point here (`created_at`); madsim simulates it.
#[allow(clippy::disallowed_methods)]
fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Cloudflare's KV namespace routes, at the same paths as under `api.cloudflare.com`. Callers authenticate in front of
/// these.
pub fn routes(directory: Arc<Directory>) -> Router {
    Router::new()
        .route(
            "/client/v4/accounts/{tenant}/storage/kv/namespaces",
            get(list).post(create),
        )
        .route(
            "/client/v4/accounts/{tenant}/storage/kv/namespaces/{id}",
            get(read).put(rename).delete(delete),
        )
        .with_state(directory)
}

#[derive(Deserialize)]
struct CreateBody {
    title: String,
    /// The home region; the serving node's region when absent.
    jurisdiction: Option<String>,
    /// KV Instant is not offered.
    mode: Option<String>,
    storage_bytes: Option<u64>,
    #[serde(default)]
    permissions: Vec<String>,
}

#[derive(Deserialize)]
struct RenameBody {
    title: String,
}

#[derive(Deserialize)]
struct GetQuery {
    #[serde(default)]
    min_revision: u64,
}

#[derive(Deserialize)]
struct ListQuery {
    page: Option<usize>,
    per_page: Option<usize>,
    order: Option<String>,
    direction: Option<String>,
    #[serde(default)]
    min_revision: u64,
}

fn body<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
}

async fn create(
    State(directory): State<Arc<Directory>>,
    Path(tenant): Path<String>,
    bytes: Bytes,
) -> Response {
    let created = match body::<CreateBody>(&bytes) {
        Ok(CreateBody { mode: Some(_), .. }) => Err(Error::Invalid("mode is not supported".into())),
        Ok(request) => {
            let request = Create {
                tenant,
                title: request.title,
                home_region: request
                    .jurisdiction
                    .unwrap_or_else(|| directory.region.clone()),
                storage_bytes: request.storage_bytes,
                permissions: request.permissions,
            };
            directory.create(request).await
        }
        Err(error) => Err(error),
    };
    reply(
        created.map(|(record, revision)| namespace(&record, revision)),
        None,
    )
}

async fn read(
    State(directory): State<Arc<Directory>>,
    Path((tenant, id)): Path<(String, String)>,
    Query(query): Query<GetQuery>,
) -> Response {
    let found = directory
        .get(&id, query.min_revision)
        .await
        .and_then(|(record, revision)| {
            if record.tenant == tenant && admit_open(&record).is_ok() {
                Ok(namespace(&record, revision))
            } else {
                Err(Error::NotFound)
            }
        });
    reply(found, None)
}

async fn rename(
    State(directory): State<Arc<Directory>>,
    Path((tenant, id)): Path<(String, String)>,
    bytes: Bytes,
) -> Response {
    let renamed = match body::<RenameBody>(&bytes) {
        Ok(request) => directory.rename(&tenant, &id, &request.title).await,
        Err(error) => Err(error),
    };
    reply(
        renamed.map(|(record, revision)| namespace(&record, revision)),
        None,
    )
}

/// Cloudflare answers an empty result; the revision lets a caller read its own deletion.
async fn delete(
    State(directory): State<Arc<Directory>>,
    Path((tenant, id)): Path<(String, String)>,
) -> Response {
    let deleted = directory.delete(&tenant, &id).await;
    reply(
        deleted.map(|(_, revision)| json!({ "revision": revision })),
        None,
    )
}

async fn list(
    State(directory): State<Arc<Directory>>,
    Path(tenant): Path<String>,
    Query(query): Query<ListQuery>,
) -> Response {
    let page = query.page.unwrap_or(1);
    let per_page = query.per_page.unwrap_or(20);
    let invalid = |detail: &str| reply(Err(Error::Invalid(detail.into())), None);
    let by_title = match query.order.as_deref() {
        None | Some("id") => false,
        Some("title") => true,
        Some(_) => return invalid("order must be id or title"),
    };
    let descending = match query.direction.as_deref() {
        None | Some("asc") => false,
        Some("desc") => true,
        Some(_) => return invalid("direction must be asc or desc"),
    };
    if page == 0 || !(1..=1000).contains(&per_page) {
        return invalid("page starts at 1 and per_page is 1 to 1000");
    }
    let (mut records, revision) = match directory.list(&tenant, query.min_revision).await {
        Ok(listed) => listed,
        Err(error) => return reply(Err(error), None),
    };
    if by_title {
        records.sort_by(|a, b| a.title.cmp(&b.title));
    }
    if descending {
        records.reverse();
    }
    let shown: Vec<_> = records
        .iter()
        .skip((page - 1).saturating_mul(per_page))
        .take(per_page)
        .map(|record| namespace(record, revision))
        .collect();
    let info = json!({
        "page": page,
        "per_page": per_page,
        "count": shown.len(),
        "total_count": records.len(),
    });
    reply(Ok(Value::Array(shown)), Some(info))
}

/// A namespace as Cloudflare shows it, with the directory `revision` it was read or written at.
fn namespace(record: &Record, revision: u64) -> Value {
    json!({
        "id": record.id,
        "title": record.title,
        "jurisdiction": record.home_region,
        "revision": revision,
    })
}

/// Cloudflare's response envelope. ponytail: every error has code 10000; distinct codes when a client needs them.
fn reply(result: Result<Value, Error>, result_info: Option<Value>) -> Response {
    match result {
        Ok(result) => {
            let mut body =
                json!({ "success": true, "errors": [], "messages": [], "result": result });
            if let Some(info) = result_info {
                body["result_info"] = info;
            }
            Json(body).into_response()
        }
        Err(error) => {
            let status = match error {
                Error::NotFound => StatusCode::NOT_FOUND,
                Error::Invalid(_) | Error::Duplicate => StatusCode::BAD_REQUEST,
                Error::Rejected(_) | Error::Contended => StatusCode::CONFLICT,
                Error::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
                Error::Corrupt(_) => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let body = json!({
                "success": false,
                "errors": [{ "code": 10000, "message": error.to_string() }],
                "messages": [],
                "result": null,
            });
            (status, Json(body)).into_response()
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/namespaces.rs"]
mod tests;
