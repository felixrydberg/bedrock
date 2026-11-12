//! Customer-KV writes (#58 / B-26): Cloudflare's Workers KV value API for `put`, `delete`, bulk writes and bulk deletes,
//! at the same paths, parameters, limits and response envelope.
//!
//! Any `bedrock-kv` node takes a write. It reads the namespace's record and sends the request where
//! [`Owners::route`] says: the owner in this cell, another cell's entry address (`cells` in the config), or its own
//! owner writer. Forwarding passes the request on unchanged with a hop count, so a request whose routes disagree for a
//! moment stops after [`MAX_HOPS`] with a retryable 503 rather than circling. The answer carries the committed
//! namespace `revision`; a read with that minimum revision on the owner sees the write, whether or not anything has
//! been published yet.
//!
//! No customer-KV event reaches a relay stream: the owner writer keeps no relay log.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, FromRequest, Multipart, Path, Query, State};
use axum::http::{HeaderMap, Method, Request, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use base64::Engine;
use bedrock_lib::replication::writer::CommitError;
use bedrock_lib::storage::StorageError;
use bedrock_lib::transport::tls::Tls;
use http_body_util::Full;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::namespaces::{self, Directory, Error, Record, Rejected, reply};
use crate::storage::{Change, OwnerError, Owners, Route, Stored};

/// Cloudflare's limits.
pub const MAX_KEY: usize = 512;
pub const MAX_VALUE: usize = 25 * 1024 * 1024;
pub const MAX_METADATA: usize = 1024;
pub const MAX_BULK: usize = 10_000;
pub const MAX_REQUEST: usize = 100 * 1024 * 1024;
/// The shortest expiration Cloudflare accepts, from now.
pub const MIN_TTL_SECONDS: u64 = 60;
/// Forwards one request may take: to the home cell, then to its owner there.
pub const MAX_HOPS: u32 = 2;
const HOPS: &str = "x-bedrock-hops";
const FORWARD_TIMEOUT: Duration = Duration::from_secs(30);

/// What serves the value routes.
pub struct Values {
    pub directory: Arc<Directory>,
    pub owners: Arc<Owners>,
    /// Sent on forwarded requests; every `bedrock-kv` node accepts the same write token.
    pub token: String,
    pub tls: Option<Arc<Tls>>,
    /// Entry address of each other cell, by cell name.
    pub cells: BTreeMap<String, String>,
}

/// Cloudflare's value write routes. Callers authenticate in front of these.
pub fn routes(values: Arc<Values>) -> Router {
    const NAMESPACE: &str = "/client/v4/accounts/{tenant}/storage/kv/namespaces/{id}";
    Router::new()
        .route(
            &format!("{NAMESPACE}/values/{{*key}}"),
            put(put_value).delete(delete_value),
        )
        .route(&format!("{NAMESPACE}/bulk"), put(bulk_put))
        .route(&format!("{NAMESPACE}/bulk/delete"), post(bulk_delete))
        // ponytail: one request may hold up to 100 MiB in memory; bound concurrent writes if that shows.
        .layer(DefaultBodyLimit::max(MAX_REQUEST))
        .with_state(values)
}

/// One write request as it arrived, kept whole so it can be forwarded unchanged.
struct Incoming {
    tenant: String,
    id: String,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Deserialize, Default)]
struct Expiry {
    expiration: Option<u64>,
    expiration_ttl: Option<u64>,
}

async fn put_value(
    State(values): State<Arc<Values>>,
    Path((tenant, id, key)): Path<(String, String, String)>,
    Query(expiry): Query<Expiry>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let incoming = Incoming {
        tenant,
        id,
        method,
        uri,
        headers,
        body,
    };
    values
        .handle(incoming, |incoming| async move {
            let key = valid_key(&key)?;
            let expiration = expiration(expiry.expiration, expiry.expiration_ttl)?;
            let (value, metadata) = value_and_metadata(&incoming.headers, incoming.body).await?;
            Ok(vec![(key, Some(stored(value, metadata, expiration)?))])
        })
        .await
}

async fn delete_value(
    State(values): State<Arc<Values>>,
    Path((tenant, id, key)): Path<(String, String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let incoming = Incoming {
        tenant,
        id,
        method,
        uri,
        headers,
        body: Bytes::new(),
    };
    values
        .handle(
            incoming,
            |_| async move { Ok(vec![(valid_key(&key)?, None)]) },
        )
        .await
}

#[derive(Deserialize)]
struct BulkPair {
    key: String,
    value: String,
    #[serde(default)]
    base64: bool,
    expiration: Option<u64>,
    expiration_ttl: Option<u64>,
    metadata: Option<Value>,
}

async fn bulk_put(
    State(values): State<Arc<Values>>,
    Path((tenant, id)): Path<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let incoming = Incoming {
        tenant,
        id,
        method,
        uri,
        headers,
        body,
    };
    values
        .handle(incoming, |incoming| async move {
            let pairs: Vec<BulkPair> = json_body(&incoming.body)?;
            bulk_size(pairs.len())?;
            pairs
                .into_iter()
                .map(|pair| {
                    let value = if pair.base64 {
                        base64::engine::general_purpose::STANDARD
                            .decode(&pair.value)
                            .map_err(|_| {
                                Error::Invalid(format!("{}: value is not base64", pair.key))
                            })?
                    } else {
                        pair.value.into_bytes()
                    };
                    let metadata = pair.metadata.map(|m| m.to_string());
                    let expiration = expiration(pair.expiration, pair.expiration_ttl)?;
                    Ok((
                        valid_key(&pair.key)?,
                        Some(stored(value, metadata, expiration)?),
                    ))
                })
                .collect()
        })
        .await
}

async fn bulk_delete(
    State(values): State<Arc<Values>>,
    Path((tenant, id)): Path<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let incoming = Incoming {
        tenant,
        id,
        method,
        uri,
        headers,
        body,
    };
    values
        .handle(incoming, |incoming| async move {
            let keys: Vec<String> = json_body(&incoming.body)?;
            bulk_size(keys.len())?;
            keys.iter().map(|key| Ok((valid_key(key)?, None))).collect()
        })
        .await
}

impl Values {
    /// Routes `incoming` and, where this node owns the namespace, parses it with `parse` and commits it.
    async fn handle<F, Fut>(&self, incoming: Incoming, parse: F) -> Response
    where
        F: FnOnce(Incoming) -> Fut,
        Fut: Future<Output = Result<Vec<Change>, Error>>,
    {
        let hops = match incoming
            .headers
            .get(HOPS)
            .map(|h| h.to_str().ok().and_then(|h| h.parse().ok()))
        {
            None => 0,
            Some(Some(hops)) => hops,
            Some(None) => return reply(Err(Error::Invalid(format!("invalid {HOPS}"))), None),
        };
        let record = match self.record(&incoming.tenant, &incoming.id).await {
            Ok(record) => record,
            Err(error) => return reply(Err(error), None),
        };
        let route = self.owners.route(&record);
        self.owners.count_route(&route);
        let address = match &route {
            Route::Local => None,
            Route::Peer(address) => Some(address.clone()),
            Route::Cell(region, cell) => match self.cells.get(cell) {
                Some(address) => Some(address.clone()),
                None => {
                    let detail =
                        format!("no address configured for cell {cell} in region {region}");
                    return reply(Err(Error::Unavailable(detail)), None);
                }
            },
        };
        if let Some(address) = address {
            return self.forward(&address, incoming, hops).await;
        }
        let idempotency = match incoming.headers.get("idempotency-key").map(|k| k.to_str()) {
            None => None,
            Some(Ok(key)) if !key.is_empty() => Some(key.to_owned()),
            Some(_) => return reply(Err(Error::Invalid("invalid Idempotency-Key".into())), None),
        };
        let bulk = incoming.uri.path().contains("/bulk");
        let changes = match parse(incoming).await {
            Ok(changes) => changes,
            Err(error) => return reply(Err(error), None),
        };
        let count = changes.len();
        let written = self
            .owners
            .write(&record, changes, idempotency.as_deref())
            .await
            .map_err(write_error);
        reply(
            written.map(|revision| {
                if bulk {
                    json!({ "successful_key_count": count, "unsuccessful_keys": [], "revision": revision })
                } else {
                    json!({ "revision": revision })
                }
            }),
            None,
        )
    }

    /// The tenant's active namespace `id`, from a fresh directory read so a deletion is never missed.
    async fn record(&self, tenant: &str, id: &str) -> Result<Record, Error> {
        let (record, _) = self.directory.get(id, 0).await?;
        if record.tenant != tenant || namespaces::admit_open(&record).is_err() {
            return Err(Error::NotFound);
        }
        Ok(record)
    }

    /// Sends `incoming` on to `address` unchanged, one hop further, and relays the answer.
    async fn forward(&self, address: &str, incoming: Incoming, hops: u32) -> Response {
        if hops >= MAX_HOPS {
            let detail = "the namespace's owner is moving; retry".to_owned();
            return reply(Err(Error::Unavailable(detail)), None);
        }
        let path = incoming
            .uri
            .path_and_query()
            .map_or("/", |path| path.as_str())
            .to_owned();
        let mut request = hyper::Request::builder()
            .method(incoming.method)
            .uri(path)
            .header("host", bedrock_lib::transport::host_of(address))
            .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
            .header(HOPS, (hops + 1).to_string());
        for name in [
            header::CONTENT_TYPE,
            "idempotency-key".parse().expect("valid header name"),
        ] {
            if let Some(value) = incoming.headers.get(&name) {
                request = request.header(name, value);
            }
        }
        let request = match request.body(Full::new(incoming.body)) {
            Ok(request) => request,
            Err(error) => return reply(Err(Error::Invalid(error.to_string())), None),
        };
        match bedrock_lib::http::send(address, self.tls.as_deref(), request, FORWARD_TIMEOUT).await
        {
            Ok((status, body)) => {
                (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
            }
            // Sent or not, the outcome is unknown; the caller retries with its idempotency key.
            Err(detail) => reply(Err(Error::Unavailable(detail)), None),
        }
    }
}

fn write_error(error: OwnerError) -> Error {
    match error {
        OwnerError::NotOwner(_) => {
            Error::Unavailable("the namespace's owner is moving; retry".into())
        }
        OwnerError::Deleted => Error::NotFound,
        OwnerError::Quota => Error::Rejected(Rejected::StorageQuota),
        OwnerError::Commit(CommitError::Busy) => Error::Busy,
        OwnerError::Commit(CommitError::Invalid(detail)) => Error::Invalid(detail),
        OwnerError::Commit(error) => Error::Unavailable(error.to_string()),
        OwnerError::Storage(StorageError::Corruption(context)) => Error::Corrupt(context.detail),
        OwnerError::Storage(error) => Error::Unavailable(error.to_string()),
    }
}

fn json_body<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
}

fn bulk_size(count: usize) -> Result<(), Error> {
    if count == 0 {
        return Err(Error::Invalid(
            "a bulk request needs at least one key".into(),
        ));
    }
    if count > MAX_BULK {
        return Err(Error::TooLarge(format!(
            "at most {MAX_BULK} keys per bulk request"
        )));
    }
    Ok(())
}

/// A key as Cloudflare accepts it: 1 to 512 bytes, and not `.` or `..`.
pub fn valid_key(key: &str) -> Result<Vec<u8>, Error> {
    if key.is_empty() || key == "." || key == ".." {
        return Err(Error::Invalid("key must not be empty, . or ..".into()));
    }
    if key.len() > MAX_KEY {
        return Err(Error::TooLarge(format!("key longer than {MAX_KEY} bytes")));
    }
    Ok(key.as_bytes().to_vec())
}

/// The absolute expiration, in seconds since the Unix epoch, or 0 for none. Cloudflare refuses either form less than
/// 60 seconds away.
pub fn expiration(expiration: Option<u64>, ttl: Option<u64>) -> Result<u64, Error> {
    let now = namespaces::wall_ms() / 1000;
    let at = match (expiration, ttl) {
        (None, None) => return Ok(0),
        (Some(_), Some(_)) => {
            return Err(Error::Invalid(
                "give expiration or expiration_ttl, not both".into(),
            ));
        }
        (Some(at), None) => at,
        (None, Some(ttl)) => now.saturating_add(ttl),
    };
    if at < now.saturating_add(MIN_TTL_SECONDS) {
        return Err(Error::Invalid(format!(
            "expiration must be at least {MIN_TTL_SECONDS} seconds in the future"
        )));
    }
    Ok(at)
}

/// A value within Cloudflare's limits, with metadata that is JSON of at most 1024 bytes.
pub fn stored(value: Vec<u8>, metadata: Option<String>, expiration: u64) -> Result<Stored, Error> {
    if value.len() > MAX_VALUE {
        return Err(Error::TooLarge(format!(
            "value longer than {MAX_VALUE} bytes"
        )));
    }
    if let Some(metadata) = &metadata {
        if metadata.len() > MAX_METADATA {
            return Err(Error::TooLarge(format!(
                "metadata longer than {MAX_METADATA} bytes"
            )));
        }
        serde_json::from_str::<Value>(metadata)
            .map_err(|_| Error::Invalid("metadata must be JSON".into()))?;
    }
    Ok(Stored {
        value,
        metadata,
        expiration,
    })
}

/// A single put's value: the raw body, or the `value` and `metadata` fields of a `multipart/form-data` body.
async fn value_and_metadata(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<(Vec<u8>, Option<String>), Error> {
    let multipart = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("multipart/form-data"));
    if !multipart {
        return Ok((body.to_vec(), None));
    }
    let mut request = Request::builder();
    if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        request = request.header(header::CONTENT_TYPE, content_type);
    }
    let request = request
        .body(Body::from(body))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut form = Multipart::from_request(request, &())
        .await
        .map_err(|error| Error::Invalid(error.body_text()))?;
    let (mut value, mut metadata) = (None, None);
    while let Some(field) = form
        .next_field()
        .await
        .map_err(|error| Error::Invalid(error.body_text()))?
    {
        let name = field.name().unwrap_or_default().to_owned();
        let bytes = field
            .bytes()
            .await
            .map_err(|error| Error::Invalid(error.body_text()))?;
        match name.as_str() {
            "value" => value = Some(bytes.to_vec()),
            "metadata" => {
                let text = String::from_utf8(bytes.to_vec())
                    .map_err(|_| Error::Invalid("metadata must be UTF-8 JSON".into()))?;
                metadata = Some(text);
            }
            _ => {}
        }
    }
    let value = value.ok_or_else(|| Error::Invalid("multipart body has no value field".into()))?;
    Ok((value, metadata))
}

#[cfg(test)]
#[path = "../tests/unit/operations.rs"]
mod tests;
