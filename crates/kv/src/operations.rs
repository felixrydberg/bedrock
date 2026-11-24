//! Customer KV REST and per-cell value-cache front (#126 / S-18).

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, FromRequest, Multipart, Path, Query, State};
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine;
use bedrock_lib::client::Client;
use bedrock_lib::health::protection::{self, Coalescer, Limiter};
use bedrock_lib::kv::{
    self, Get, Item, List, Mutation, Record, Reply, Stored, Write, request::Operation,
};
use bedrock_lib::membership;
use bedrock_lib::replication::pb;
use bedrock_lib::transport::{Caller, Peers};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use tonic::Status;

use crate::namespaces;
use crate::storage::ValueCache;

type Landed = (u64, Result<pb::GetReply, Status>);

#[derive(Clone)]
pub struct Values {
    client: Arc<Client>,
    members: Arc<membership::View>,
    peers: Arc<Peers>,
    node_id: String,
    cell: String,
    cache: Arc<ValueCache>,
    misses: Arc<Coalescer<(String, Vec<u8>), Landed>>,
    limiter: Arc<Limiter>,
}

impl Values {
    pub fn new(
        admin: &bedrock_lib::admin::Admin,
        client: Arc<Client>,
        members: Arc<membership::View>,
        peers: Arc<Peers>,
        node_id: String,
        cell: String,
        budget_bytes: u64,
    ) -> Self {
        let metrics = protection::Metrics::new(admin);
        Self {
            client,
            members,
            peers,
            node_id,
            cell,
            cache: Arc::new(ValueCache::new(admin, budget_bytes)),
            misses: Arc::new(Coalescer::new("kv_holder", &metrics)),
            limiter: Arc::new(Limiter::new(
                "kv_holder",
                protection::RECENT_STATE,
                &metrics,
            )),
        }
    }

    async fn record(&self, tenant: &str, id: &str) -> Result<Record, Status> {
        let record = kv::record(&self.client, id).await?;
        if record.tenant != tenant || namespaces::admit_open(&record).is_err() {
            return Err(Status::not_found("namespace not found"));
        }
        Ok(record)
    }

    async fn call(&self, record: &Record, operation: Operation) -> Result<Reply, Status> {
        let _permit =
            if matches!(operation, Operation::Get(_) | Operation::List(_)) {
                Some(self.limiter.acquire(&self.cell).await.map_err(|error| {
                    Status::unavailable(format!("holder read capacity: {error}"))
                })?)
            } else {
                None
            };
        kv::call(
            &self.client,
            record,
            kv::Request {
                tenant: record.tenant.clone(),
                operation: Some(operation),
            },
        )
        .await
    }

    /// The peer endpoint never forwards to another cache: a miss goes straight to the actor.
    async fn local(
        &self,
        record: &Record,
        key: &[u8],
        minimum: u64,
    ) -> Result<pb::GetReply, Status> {
        if let Some((bytes, revision)) = self.cache.get(&record.id, key, minimum) {
            let value = decode(&bytes)?;
            if !value.expired(namespaces::wall_ms() / 1000) {
                return Ok(pb::GetReply {
                    revision,
                    value: Some(bytes),
                });
            }
        }
        let accept = |(asked, answer): &Landed| match answer {
            Ok(reply) => reply.revision >= minimum,
            Err(_) => *asked <= minimum,
        };
        let mut reply = self
            .misses
            .run((record.id.clone(), key.to_vec()), accept, || async {
                (minimum, self.fetch(record, key, minimum).await)
            })
            .await
            .1?;
        if let Some(bytes) = &reply.value
            && decode(bytes)?.expired(namespaces::wall_ms() / 1000)
        {
            reply.value = None;
        }
        Ok(reply)
    }

    async fn fetch(
        &self,
        record: &Record,
        key: &[u8],
        minimum: u64,
    ) -> Result<pb::GetReply, Status> {
        // A delayed fetch must not start a new 60-second freshness window on arrival.
        let started = bedrock_lib::monotonic_now();
        let reply = self
            .call(
                record,
                Operation::Get(Get {
                    keys: vec![key.to_vec()],
                    min_revision: minimum,
                }),
            )
            .await?;
        let item = reply
            .items
            .into_iter()
            .next()
            .ok_or_else(|| Status::data_loss("holder omitted requested key"))?;
        if item.key != key || reply.revision < minimum {
            return Err(Status::data_loss(
                "holder returned another key or an old revision",
            ));
        }
        let value = item
            .value
            .filter(|value| !value.expired(namespaces::wall_ms() / 1000));
        let value = value.map(|value| value.encode_to_vec());
        if let Some(bytes) = &value {
            self.cache
                .fill_at(&record.id, key, bytes, reply.revision, started);
        }
        Ok(pb::GetReply {
            revision: reply.revision,
            value,
        })
    }

    async fn read(
        &self,
        record: &Record,
        key: &[u8],
        minimum: u64,
    ) -> Result<pb::GetReply, Status> {
        let members = self.members.members()?;
        let hash = bedrock_lib::placement::hash_of("kv", &record.id, key);
        let candidates =
            bedrock_lib::relays::sources::readers(members.values(), self.peers.health(), hash);
        let mut last = Status::unavailable("no healthy KV cache in this cell");
        for member in candidates {
            let result = if member.node_id == self.node_id {
                self.local(record, key, minimum).await
            } else {
                let request = pb::GetRequest {
                    dataset: "kv".into(),
                    namespace: record.id.clone(),
                    key: key.to_vec(),
                    min_revision: minimum,
                };
                self.client
                    .call_member(member, |channel| async move {
                        pb::reads_client::ReadsClient::new(channel)
                            .max_decoding_message_size(kv::MAX_MESSAGE_BYTES)
                            .max_encoding_message_size(kv::MAX_MESSAGE_BYTES)
                            .get(request)
                            .await
                    })
                    .await
            };
            match result {
                Ok(reply) if reply.revision >= minimum => {
                    if let Some(bytes) = &reply.value
                        && decode(bytes)?.expired(namespaces::wall_ms() / 1000)
                    {
                        return Ok(pb::GetReply {
                            value: None,
                            ..reply
                        });
                    }
                    // Do not cache a peer answer: it would reset the source cache's TTL.
                    return Ok(reply);
                }
                Ok(_) => last = Status::data_loss("cache returned an old revision"),
                Err(error) if kv::terminal_read_error(&error) => return Err(error),
                Err(error) => last = error,
            }
        }
        Err(last)
    }
}

#[tonic::async_trait]
impl pb::reads_server::Reads for Values {
    async fn get(
        &self,
        request: tonic::Request<pb::GetRequest>,
    ) -> Result<tonic::Response<pb::GetReply>, Status> {
        if let Some(caller) = Caller::of(&request) {
            caller.require(&["kv", "compute", "gateway"], Some(&self.cell))?;
        }
        let request = request.into_inner();
        if request.dataset != "kv" {
            return Err(Status::failed_precondition(
                "only customer KV is hosted here",
            ));
        }
        valid_key_bytes(&request.key)?;
        let record = kv::record(&self.client, &request.namespace).await?;
        if namespaces::admit_open(&record).is_err() {
            return Err(Status::not_found("namespace not found"));
        }
        self.local(&record, &request.key, request.min_revision)
            .await
            .map(tonic::Response::new)
    }

    async fn list(
        &self,
        _: tonic::Request<pb::ListRequest>,
    ) -> Result<tonic::Response<pb::ListReply>, Status> {
        Err(Status::unimplemented("KV LIST uses the namespace holder"))
    }
}

pub fn routes(values: Arc<Values>) -> Router {
    const NS: &str = "/client/v4/accounts/{tenant}/storage/kv/namespaces/{id}";
    Router::new()
        .route(
            &format!("{NS}/values/{{*key}}"),
            get(get_value).put(put_value).delete(delete_value),
        )
        .route(&format!("{NS}/metadata/{{*key}}"), get(get_metadata))
        .route(&format!("{NS}/keys"), get(list))
        .route(&format!("{NS}/bulk"), put(bulk_put))
        .route(&format!("{NS}/bulk/delete"), post(bulk_delete))
        .route(&format!("{NS}/bulk/get"), post(bulk_get))
        .layer(DefaultBodyLimit::max(kv::MAX_REQUEST))
        .with_state(values)
}

#[derive(Default, Deserialize)]
struct ReadQuery {
    #[serde(default)]
    min_revision: u64,
}

async fn get_value(
    State(values): State<Arc<Values>>,
    Path((tenant, id, key)): Path<(String, String, String)>,
    Query(query): Query<ReadQuery>,
) -> Response {
    let result = async {
        valid_key_bytes(key.as_bytes())?;
        let record = values.record(&tenant, &id).await?;
        let reply = values
            .read(&record, key.as_bytes(), query.min_revision)
            .await?;
        let stored = reply
            .value
            .as_deref()
            .map(decode)
            .transpose()?
            .ok_or_else(|| Status::not_found("key not found"))?;
        Ok::<_, Status>((reply.revision, stored.value))
    }
    .await;
    match result {
        Ok((revision, body)) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
                (
                    header::HeaderName::from_static("x-bedrock-revision"),
                    revision.to_string(),
                ),
            ],
            body,
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

async fn get_metadata(
    State(values): State<Arc<Values>>,
    Path((tenant, id, key)): Path<(String, String, String)>,
    Query(query): Query<ReadQuery>,
) -> Response {
    let result = async {
        valid_key_bytes(key.as_bytes())?;
        let record = values.record(&tenant, &id).await?;
        let reply = values
            .read(&record, key.as_bytes(), query.min_revision)
            .await?;
        let stored = reply
            .value
            .as_deref()
            .map(decode)
            .transpose()?
            .ok_or_else(|| Status::not_found("key not found"))?;
        stored
            .metadata
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_| Status::data_loss("stored metadata is not JSON"))
            .map(|v| v.unwrap_or(Value::Null))
    }
    .await;
    response(result)
}

#[derive(Default, Deserialize)]
struct Expiry {
    expiration: Option<u64>,
    expiration_ttl: Option<u64>,
}

async fn put_value(
    State(values): State<Arc<Values>>,
    Path((tenant, id, key)): Path<(String, String, String)>,
    Query(expiry): Query<Expiry>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        let (value, metadata) = value_and_metadata(&headers, body).await?;
        let mut value = stored(
            value,
            metadata,
            expiration(expiry.expiration, expiry.expiration_ttl)?,
        )?;
        if expiry.expiration_ttl.is_some() {
            value.expiration = 0;
        }
        write(
            &values,
            &tenant,
            &id,
            &headers,
            vec![Mutation {
                key: valid_key(&key)?,
                value: Some(value),
                expiration_ttl: expiry.expiration_ttl,
            }],
            false,
        )
        .await
    }
    .await;
    response(result)
}

async fn delete_value(
    State(values): State<Arc<Values>>,
    Path((tenant, id, key)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        write(
            &values,
            &tenant,
            &id,
            &headers,
            vec![Mutation {
                key: valid_key(&key)?,
                value: None,
                expiration_ttl: None,
            }],
            false,
        )
        .await
    }
    .await;
    response(result)
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
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        let pairs: Vec<BulkPair> = json_body(&body)?;
        bulk_size(pairs.len(), kv::MAX_BULK)?;
        let changes = pairs
            .into_iter()
            .map(|pair| {
                let value = if pair.base64 {
                    base64::engine::general_purpose::STANDARD
                        .decode(pair.value)
                        .map_err(|_| Status::invalid_argument("value is not base64"))?
                } else {
                    pair.value.into_bytes()
                };
                let expires = expiration(pair.expiration, pair.expiration_ttl)?;
                Ok(Mutation {
                    key: valid_key(&pair.key)?,
                    value: Some(stored(
                        value,
                        pair.metadata.map(|m| m.to_string()),
                        if pair.expiration_ttl.is_some() {
                            0
                        } else {
                            expires
                        },
                    )?),
                    expiration_ttl: pair.expiration_ttl,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        write(&values, &tenant, &id, &headers, changes, true).await
    }
    .await;
    response(result)
}

async fn bulk_delete(
    State(values): State<Arc<Values>>,
    Path((tenant, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        let keys: Vec<String> = json_body(&body)?;
        bulk_size(keys.len(), kv::MAX_BULK)?;
        let changes = keys
            .iter()
            .map(|key| {
                Ok(Mutation {
                    key: valid_key(key)?,
                    value: None,
                    expiration_ttl: None,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        write(&values, &tenant, &id, &headers, changes, true).await
    }
    .await;
    response(result)
}

async fn write(
    values: &Values,
    tenant: &str,
    id: &str,
    headers: &HeaderMap,
    changes: Vec<Mutation>,
    bulk: bool,
) -> Result<Value, Status> {
    let retry = text_header(headers, "idempotency-key")?;
    let expected = text_header(headers, "if-match")?
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| Status::invalid_argument("If-Match must be a revision"))
        })
        .transpose()?;
    let record = values.record(tenant, id).await?;
    let reply = values
        .call(
            &record,
            Operation::Write(Write {
                changes,
                retry,
                expected,
            }),
        )
        .await?;
    Ok(if bulk {
        json!({"successful_key_count": reply.written_keys, "unsuccessful_keys": [], "revision": reply.revision, "committed_at": reply.committed_at})
    } else {
        json!({"revision": reply.revision, "committed_at": reply.committed_at})
    })
}

#[derive(Deserialize)]
struct BulkGet {
    keys: Vec<String>,
    #[serde(default)]
    with_metadata: bool,
    #[serde(default)]
    min_revision: u64,
    #[serde(default = "text_type")]
    r#type: String,
}
fn text_type() -> String {
    "text".into()
}

async fn bulk_get(
    State(values): State<Arc<Values>>,
    Path((tenant, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let result = async {
        let query: BulkGet = json_body(&body)?;
        bulk_size(query.keys.len(), 100)?;
        if query.r#type != "text" && query.r#type != "json" {
            return Err(Status::invalid_argument("type must be text or json"));
        }
        let keys = query
            .keys
            .iter()
            .map(|key| valid_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let record = values.record(&tenant, &id).await?;
        // One holder snapshot preserves bulk atomicity; independent cache hits need not share a revision.
        let reply = values
            .call(
                &record,
                Operation::Get(Get {
                    keys: keys.clone(),
                    min_revision: query.min_revision,
                }),
            )
            .await?;
        if reply.items.len() != keys.len()
            || reply
                .items
                .iter()
                .zip(&keys)
                .any(|(item, key)| item.key != *key)
        {
            return Err(Status::data_loss("holder omitted requested keys"));
        }
        let mut found = serde_json::Map::new();
        for item in reply.items {
            let key =
                String::from_utf8(item.key).map_err(|_| Status::data_loss("key is not UTF-8"))?;
            let value = item
                .value
                .filter(|v| !v.expired(namespaces::wall_ms() / 1000));
            let entry = match value {
                None => Value::Null,
                Some(value) => {
                    let text = String::from_utf8(value.value)
                        .map_err(|_| Status::invalid_argument("value is not UTF-8"))?;
                    let data = if query.r#type == "json" {
                        serde_json::from_str(&text)
                            .map_err(|_| Status::invalid_argument("value is not JSON"))?
                    } else {
                        Value::String(text)
                    };
                    if query.with_metadata {
                        json!({"value":data, "metadata": metadata(&value.metadata)?})
                    } else {
                        data
                    }
                }
            };
            found.insert(key, entry);
        }
        Ok(json!({"values": found, "revision": reply.revision}))
    }
    .await;
    response(result)
}

#[derive(Default, Deserialize)]
struct ListQuery {
    #[serde(default)]
    prefix: String,
    cursor: Option<String>,
    limit: Option<u32>,
    #[serde(default)]
    min_revision: u64,
}

async fn list(
    State(values): State<Arc<Values>>,
    Path((tenant, id)): Path<(String, String)>,
    Query(query): Query<ListQuery>,
) -> Response {
    let result = async {
        let limit = query.limit.unwrap_or(1000);
        if !(1..=1000).contains(&limit) || query.prefix.len() > kv::MAX_KEY { return Err(Status::invalid_argument("limit must be 1 to 1000 and prefix at most 512 bytes")); }
        let after = query.cursor.filter(|c| !c.is_empty()).map(|c| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(c).map_err(|_| Status::invalid_argument("invalid cursor"))).transpose()?;
        if let Some(after) = &after { valid_key_bytes(after)?; }
        let record = values.record(&tenant, &id).await?;
        let reply = values.call(&record, Operation::List(List { prefix: query.prefix.into_bytes(), after, limit, min_revision: query.min_revision })).await?;
        let keys = reply.items.into_iter().map(listed).collect::<Result<Vec<_>, _>>()?;
        let cursor = reply.cursor.map(|c| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(c)).unwrap_or_default();
        let info = json!({"count": keys.len(), "cursor":cursor, "list_complete":cursor.is_empty(), "revision":reply.revision});
        Ok((Value::Array(keys), info))
    }.await;
    match result {
        Ok((keys, info)) => {
            Json(json!({"success":true,"errors":[],"messages":[],"result":keys,"result_info":info}))
                .into_response()
        }
        Err(e) => error_response(e),
    }
}

fn listed(item: Item) -> Result<Value, Status> {
    let key = String::from_utf8(item.key).map_err(|_| Status::data_loss("key is not UTF-8"))?;
    let stored = item
        .value
        .ok_or_else(|| Status::data_loss("LIST returned an absent item"))?;
    let mut value = json!({"name":key, "metadata":metadata(&stored.metadata)?});
    if stored.expiration != 0 {
        value["expiration"] = json!(stored.expiration);
    }
    Ok(value)
}

fn metadata(value: &Option<String>) -> Result<Value, Status> {
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map(|v| v.unwrap_or(Value::Null))
        .map_err(|_| Status::data_loss("stored metadata is not JSON"))
}
fn decode(bytes: &[u8]) -> Result<Stored, Status> {
    let value = Stored::decode(bytes).map_err(|e| Status::data_loss(e.to_string()))?;
    stored(value.value, value.metadata, value.expiration)
        .map_err(|_| Status::data_loss("invalid stored value"))
}
fn json_body<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(bytes).map_err(|e| Status::invalid_argument(e.to_string()))
}
fn text_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, Status> {
    headers
        .get(name)
        .map(|v| {
            v.to_str()
                .ok()
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| Status::invalid_argument(format!("invalid {name}")))
        })
        .transpose()
}
fn bulk_size(count: usize, max: usize) -> Result<(), Status> {
    if count == 0 || count > max {
        Err(Status::invalid_argument(format!(
            "expected 1 to {max} keys"
        )))
    } else {
        Ok(())
    }
}
fn valid_key_bytes(key: &[u8]) -> Result<(), Status> {
    if key.is_empty()
        || key == b"."
        || key == b".."
        || key.len() > kv::MAX_KEY
        || std::str::from_utf8(key).is_err()
    {
        Err(Status::invalid_argument(
            "key must be 1 to 512 UTF-8 bytes, other than . or ..",
        ))
    } else {
        Ok(())
    }
}
fn valid_key(key: &str) -> Result<Vec<u8>, Status> {
    valid_key_bytes(key.as_bytes())?;
    Ok(key.as_bytes().to_vec())
}
fn expiration(at: Option<u64>, ttl: Option<u64>) -> Result<u64, Status> {
    let now = namespaces::wall_ms() / 1000;
    let at = match (at, ttl) {
        (None, None) => return Ok(0),
        (Some(at), None) => at,
        (None, Some(ttl)) => now
            .checked_add(ttl)
            .ok_or_else(|| Status::invalid_argument("expiration overflow"))?,
        _ => {
            return Err(Status::invalid_argument(
                "give expiration or expiration_ttl, not both",
            ));
        }
    };
    if at < now.saturating_add(60) {
        return Err(Status::invalid_argument(
            "expiration must be at least 60 seconds in the future",
        ));
    }
    Ok(at)
}
fn stored(value: Vec<u8>, metadata: Option<String>, expiration: u64) -> Result<Stored, Status> {
    if value.len() > kv::MAX_VALUE
        || metadata
            .as_ref()
            .is_some_and(|m| m.len() > kv::MAX_METADATA)
    {
        return Err(Status::resource_exhausted(
            "value or metadata exceeds KV limit",
        ));
    }
    if let Some(metadata) = &metadata {
        serde_json::from_str::<Value>(metadata)
            .map_err(|_| Status::invalid_argument("metadata must be JSON"))?;
    }
    Ok(Stored {
        value,
        metadata,
        expiration,
    })
}

async fn value_and_metadata(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<(Vec<u8>, Option<String>), Status> {
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("multipart/form-data"))
    {
        return Ok((body.to_vec(), None));
    }
    let mut request = Request::builder();
    request = request.header(
        header::CONTENT_TYPE,
        headers.get(header::CONTENT_TYPE).expect("checked above"),
    );
    let mut request = request
        .body(Body::from(body))
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    DefaultBodyLimit::max(kv::MAX_REQUEST).apply(&mut request);
    let mut form = Multipart::from_request(request, &())
        .await
        .map_err(|e| Status::invalid_argument(e.body_text()))?;
    let (mut value, mut metadata) = (None, None);
    while let Some(field) = form
        .next_field()
        .await
        .map_err(|e| Status::invalid_argument(e.body_text()))?
    {
        let name = field.name().unwrap_or_default().to_owned();
        let bytes = field
            .bytes()
            .await
            .map_err(|e| Status::invalid_argument(e.body_text()))?;
        match name.as_str() {
            "value" if value.is_none() => value = Some(bytes.to_vec()),
            "metadata" if metadata.is_none() => {
                metadata = Some(
                    String::from_utf8(bytes.to_vec())
                        .map_err(|_| Status::invalid_argument("metadata must be UTF-8 JSON"))?,
                )
            }
            "value" | "metadata" => {
                return Err(Status::invalid_argument("duplicate multipart field"));
            }
            _ => {}
        }
    }
    Ok((
        value.ok_or_else(|| Status::invalid_argument("multipart body has no value field"))?,
        metadata,
    ))
}

fn response(result: Result<Value, Status>) -> Response {
    match result {
        Ok(value) => {
            Json(json!({"success":true,"errors":[],"messages":[],"result":value})).into_response()
        }
        Err(error) => error_response(error),
    }
}
fn error_response(error: Status) -> Response {
    let status = match error.code() {
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::ResourceExhausted => StatusCode::PAYLOAD_TOO_LARGE,
        tonic::Code::Aborted | tonic::Code::FailedPrecondition | tonic::Code::AlreadyExists => {
            StatusCode::CONFLICT
        }
        tonic::Code::DataLoss | tonic::Code::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status,Json(json!({"success":false,"errors":[{"code":10000,"message":error.message()}],"messages":[],"result":null}))).into_response()
}

#[cfg(test)]
#[path = "../tests/unit/operations.rs"]
mod tests;
