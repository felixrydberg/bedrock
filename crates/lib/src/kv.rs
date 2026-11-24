//! Customer-KV binding and built-in actor wire contract (#126 / S-18).
//! Requests use the existing Gateway `Actors.Invoke` path; the Worker sandbox consumes this contract in S-10.

use crate::actors::{ActorId, Kind, Regions};
use crate::volume;
use prost::Message;

pub const MAX_KEY: usize = 512;
pub const MAX_VALUE: usize = 25 << 20;
pub const MAX_METADATA: usize = 1024;
pub const MAX_BULK: usize = 10_000;
pub const MAX_REQUEST: usize = 100 << 20;
/// Room for protobuf envelopes, keys, metadata, quota/revision, retry and retention records.
// Each atomic entry includes both current values and the original retained event payload.
pub const MAX_ENTRY_BYTES: usize = 224 << 20;
pub const MAX_MESSAGE_BYTES: usize = 256 << 20;
pub const RETRY_MS: u64 = 24 * 60 * 60 * 1000;
pub const EVENT_MS: u64 = 60 * 1000;

/// Worker binding read contract: select this cell's KV value cache by HRW; misses reach the actor through that cache.
/// The host validates the binding's namespace before calling; the KV service rechecks directory deletion on every read.
pub async fn get(
    client: &crate::client::Client,
    record: &Record,
    key: &[u8],
    min_revision: u64,
) -> Result<(Option<Stored>, u64), tonic::Status> {
    let mut last = tonic::Status::unavailable("no KV cache answered");
    let mut seen = None;
    for _ in 0..2 {
        let table = client.routes("kv", seen).await?;
        seen = Some(table.revision);
        let ranked = crate::placement::rank(
            crate::placement::hash_of("kv", &record.id, key),
            table
                .members
                .values()
                .filter(|m| m.status == crate::replication::pb::member::Status::Ready as i32),
        );
        let addresses: Vec<_> = ranked.iter().map(|m| m.address.clone()).collect();
        let usable = client.route(&addresses);
        for member in ranked.iter().filter(|m| usable.contains(&&m.address)) {
            let request = crate::replication::pb::GetRequest {
                dataset: "kv".into(),
                namespace: record.id.clone(),
                key: key.to_vec(),
                min_revision,
            };
            match client
                .call_member(member, |channel| async move {
                    crate::replication::pb::reads_client::ReadsClient::new(channel)
                        .max_decoding_message_size(MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(MAX_MESSAGE_BYTES)
                        .get(request)
                        .await
                })
                .await
            {
                Ok(reply) => {
                    if reply.revision < min_revision {
                        return Err(tonic::Status::data_loss(
                            "KV cache returned an old revision",
                        ));
                    }
                    let value = reply
                        .value
                        .map(|bytes| {
                            Stored::decode(bytes.as_slice())
                                .map_err(|e| tonic::Status::data_loss(e.to_string()))
                        })
                        .transpose()?;
                    return Ok((
                        value.filter(|value| !value.expired(crate::wall_ms() / 1000)),
                        reply.revision,
                    ));
                }
                Err(error) if terminal_read_error(&error) => return Err(error),
                Err(error) => last = error,
            }
        }
    }
    Err(last)
}

/// A newer deletion or an explicit validation/corruption failure must not fall back to another cached answer.
pub fn terminal_read_error(error: &tonic::Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::NotFound
            | tonic::Code::PermissionDenied
            | tonic::Code::InvalidArgument
            | tonic::Code::DataLoss
            | tonic::Code::FailedPrecondition
    )
}

/// Delete a retired namespace actor through its normal Gateway route.
pub async fn purge(client: &crate::client::Client, record: &Record) -> Result<(), tonic::Status> {
    call(
        client,
        record,
        Request {
            tenant: record.tenant.clone(),
            operation: Some(request::Operation::Purge(true)),
        },
    )
    .await?;
    Ok(())
}

/// Shared binding transport. No write is automatically retried after an ambiguous outcome.
pub async fn call(
    client: &crate::client::Client,
    record: &Record,
    request: Request,
) -> Result<Reply, tonic::Status> {
    let (topology, _) = crate::actors::Topology::read(client, &record.home_region, 0)
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let regions = std::collections::BTreeMap::from([(record.home_region.clone(), topology)]);
    let actor = actor(&regions, &record.home_region, &record.id)
        .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
    let mut table = client.routes("gateway", None).await?;
    if table.members.is_empty() {
        table = client.routes("gateway", Some(table.revision)).await?;
    }
    let addresses: Vec<_> = table.members.values().map(|m| m.address.clone()).collect();
    let routed = client.route(&addresses);
    let address = routed
        .first()
        .ok_or_else(|| tonic::Status::unavailable("no Gateway in this cell"))?;
    let member = table
        .members
        .values()
        .find(|m| m.address.as_str() == address.as_str())
        .expect("address came from table");
    let reply = client
        .call_member(member, |channel| async move {
            volume::actors_client::ActorsClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES)
                .invoke(volume::InvokeRequest {
                    actor: Some(actor),
                    payload: request.encode_to_vec(),
                    ..Default::default()
                })
                .await
        })
        .await?;
    if reply.code != 0 {
        return Err(tonic::Status::new(reply.code.into(), reply.message));
    }
    let reply = Reply::decode(reply.payload.as_slice())
        .map_err(|e| tonic::Status::data_loss(e.to_string()))?;
    if reply.code != 0 {
        return Err(tonic::Status::new(reply.code.into(), reply.message));
    }
    Ok(reply)
}

/// A fresh directory lookup, shared by the REST front and the built-in holder.
pub async fn record(client: &crate::client::Client, id: &str) -> Result<Record, tonic::Status> {
    let found = client
        .get(
            "quicksilver",
            "quicksilver",
            "infrastructure",
            format!("kv/namespaces/{id}").as_bytes(),
            0,
        )
        .await?;
    let bytes = found
        .value
        .ok_or_else(|| tonic::Status::not_found("namespace not found"))?;
    let record =
        Record::decode(bytes.as_slice()).map_err(|e| tonic::Status::data_loss(e.to_string()))?;
    if record.id != id
        || id.len() != 32
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || record.format_version != 1
        || record.storage != format!("kv/{id}")
        || record.cache_ttl_seconds != 60
        || Lifecycle::try_from(record.state).is_err()
    {
        return Err(tonic::Status::data_loss(
            "invalid namespace directory record",
        ));
    }
    Ok(record)
}

/// Namespace identity is also the actor namespace; only the ID's hash supplies placement.
pub fn actor(
    regions: &Regions,
    region: &str,
    namespace: &str,
) -> Result<volume::Actor, crate::actors::Error> {
    Ok(volume::Actor {
        namespace: namespace.into(),
        id: ActorId::from_name(regions, region, namespace, "")?.to_string(),
        kind: Kind::Kv as i32,
    })
}

#[derive(Clone, PartialEq, Message)]
pub struct Stored {
    #[prost(bytes = "vec", tag = "1")]
    pub value: Vec<u8>,
    #[prost(string, optional, tag = "2")]
    pub metadata: Option<String>,
    /// Unix seconds, zero for no expiration.
    #[prost(uint64, tag = "3")]
    pub expiration: u64,
}

impl Stored {
    pub fn expired(&self, now_s: u64) -> bool {
        self.expiration != 0 && self.expiration <= now_s
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct Mutation {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub value: Option<Stored>,
    /// Relative expiry is resolved once by the holder, after routing and queueing, never on replay or retry.
    #[prost(uint64, optional, tag = "3")]
    pub expiration_ttl: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Write {
    #[prost(message, repeated, tag = "1")]
    pub changes: Vec<Mutation>,
    #[prost(string, optional, tag = "2")]
    pub retry: Option<String>,
    #[prost(uint64, optional, tag = "3")]
    pub expected: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Get {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub keys: Vec<Vec<u8>>,
    #[prost(uint64, tag = "2")]
    pub min_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct List {
    #[prost(bytes = "vec", tag = "1")]
    pub prefix: Vec<u8>,
    /// Exclusive last key; the next page reads a current view at least at `min_revision`.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub after: Option<Vec<u8>>,
    #[prost(uint32, tag = "3")]
    pub limit: u32,
    #[prost(uint64, tag = "4")]
    pub min_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Request {
    /// Checked against the authoritative directory by the holder on every event.
    #[prost(string, tag = "1")]
    pub tenant: String,
    #[prost(oneof = "request::Operation", tags = "2, 3, 4, 5, 6")]
    pub operation: Option<request::Operation>,
}

pub mod request {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "2")]
        Write(super::Write),
        #[prost(message, tag = "3")]
        Get(super::Get),
        #[prost(message, tag = "4")]
        List(super::List),
        /// Event-triggered expiry/retry/event reclamation, without changing the namespace revision.
        #[prost(bool, tag = "5")]
        Compact(bool),
        /// Administrative lifecycle operation; Compute authorizes it before acquiring an instance.
        #[prost(bool, tag = "6")]
        Purge(bool),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct Item {
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub value: Option<Stored>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Reply {
    #[prost(uint64, tag = "1")]
    pub revision: u64,
    #[prost(uint64, tag = "2")]
    pub committed_at: u64,
    #[prost(message, repeated, tag = "3")]
    pub items: Vec<Item>,
    #[prost(bytes = "vec", optional, tag = "4")]
    pub cursor: Option<Vec<u8>>,
    /// gRPC code, zero on success; application errors stay answers so Gateways never resend a write.
    #[prost(int32, tag = "5")]
    pub code: i32,
    #[prost(string, tag = "6")]
    pub message: String,
    /// Original successful write count, retained with the retry result.
    #[prost(uint32, tag = "7")]
    pub written_keys: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum Lifecycle {
    Active = 0,
    /// Closed to new reads and writes; namespace purge later tombstones its actor.
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
    /// The built-in actor's home region, Cloudflare's `jurisdiction`.
    #[prost(string, tag = "5")]
    pub home_region: String,
    /// The stable logical storage identity, `kv/{id}`; actor bytes live in volume range databases.
    #[prost(string, tag = "6")]
    pub storage: String,
    /// Legacy directory compatibility field. Actor metadata and value envelopes carry their own format versions.
    #[prost(uint32, tag = "7")]
    pub storage_format: u32,
    /// Customer key and encoded-value bytes, excluding internal retention and replicas; unset means no limit.
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
    /// When the namespace became `Deleting`, in wall-clock milliseconds; 0 while active, and for namespaces deleted
    /// before this was recorded.
    #[prost(uint64, tag = "13")]
    pub deleting_at: u64,
}

#[cfg(test)]
#[path = "../tests/unit/kv.rs"]
mod tests;
