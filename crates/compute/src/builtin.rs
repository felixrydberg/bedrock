//! Built-in customer-KV namespace actor (#126 / S-18), over the shared execution and volume seams.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bedrock_lib::actors::{Kind, Topology};
use bedrock_lib::kv::{self, Item, Lifecycle, Reply, Request, Stored, Write, request::Operation};
use bedrock_lib::volume as pb;
use prometheus_client::metrics::counter::Counter;
use prost::Message;
use tonic::Status;

use crate::actors::storage::{Storage, now_ms};
use crate::actors::volume;
use crate::runtime::{self, ExecutionSpec, Instance};

const META: &[u8] = b"m";
const DATA: &[u8] = b"d/";
const RETRY: &[u8] = b"r/";
const EVENT: &[u8] = b"e/";
const CLEAN_PAGE: u32 = 32;

/// Adds KV to an existing executor; every other execution spec keeps its original executor.
pub struct KvExecutor {
    directory: Arc<kv::DirectoryView>,
    fallback: Arc<dyn runtime::Executor>,
    quota_rejections: Counter,
}

impl KvExecutor {
    pub fn new(
        admin: &bedrock_lib::admin::Admin,
        directory: Arc<kv::DirectoryView>,
        fallback: Arc<dyn runtime::Executor>,
    ) -> Self {
        let quota_rejections = Counter::default();
        admin.register(
            "kv_quota_rejections",
            "KV actor writes refused by the storage quota",
            quota_rejections.clone(),
        );
        Self {
            directory,
            fallback,
            quota_rejections,
        }
    }
}

#[async_trait]
impl runtime::Executor for KvExecutor {
    async fn start(
        &self,
        spec: ExecutionSpec,
    ) -> std::result::Result<Box<dyn Instance>, runtime::Error> {
        if spec.builtin() != Some(Kind::Kv) {
            return self.fallback.start(spec).await;
        }
        let ExecutionSpec::Actor { actor, storage } = spec else {
            unreachable!("builtin identified an actor")
        };
        Ok(Box::new(Kv {
            directory: self.directory.clone(),
            actor,
            storage,
            quota_rejections: self.quota_rejections.clone(),
            cleanup: [DATA.to_vec(), EVENT.to_vec(), RETRY.to_vec()],
        }))
    }
}

struct Kv {
    directory: Arc<kv::DirectoryView>,
    actor: pb::Actor,
    storage: Storage,
    quota_rejections: Counter,
    cleanup: [Vec<u8>; 3],
}

#[derive(Clone, PartialEq, Message)]
struct Meta {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(uint64, tag = "3")]
    committed_at: u64,
    #[prost(uint64, tag = "4")]
    bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
struct Row {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(message, optional, tag = "3")]
    value: Option<Stored>,
}

#[derive(Clone, PartialEq, Message)]
struct Retained {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(uint64, tag = "3")]
    committed_at: u64,
    #[prost(uint64, tag = "4")]
    expires_at: u64,
    #[prost(message, optional, tag = "5")]
    write: Option<Write>,
    #[prost(uint32, tag = "6")]
    written_keys: u32,
}

enum Failure {
    Application(Status),
    Volume(volume::Error),
}

impl From<Status> for Failure {
    fn from(value: Status) -> Self {
        Self::Application(value)
    }
}

impl From<volume::Error> for Failure {
    fn from(value: volume::Error) -> Self {
        Self::Volume(value)
    }
}

type Result<T> = std::result::Result<T, Failure>;

#[async_trait]
impl Instance for Kv {
    async fn invoke(&mut self, payload: Vec<u8>) -> std::result::Result<Vec<u8>, runtime::Error> {
        let result = self.handle(&payload).await;
        let reply = match result {
            Ok(reply) => reply,
            Err(Failure::Application(status)) => Reply {
                code: status.code() as i32,
                message: status.message().to_owned(),
                ..Default::default()
            },
            Err(Failure::Volume(error)) => return Err(error.into()),
        };
        let reply = Reply {
            directory_revision: self.directory.minimum(),
            ..reply
        };
        Ok(reply.encode_to_vec())
    }
}

impl Kv {
    async fn handle(&mut self, payload: &[u8]) -> Result<Reply> {
        let request =
            Request::decode(payload).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let record = self
            .directory
            .record(&self.actor.namespace, request.directory_min_revision)
            .await?;
        if request.tenant != record.tenant {
            return Err(Status::permission_denied("namespace belongs to another tenant").into());
        }
        if record.state != Lifecycle::Active as i32 {
            return Err(Status::failed_precondition("namespace is deleting").into());
        }
        let (topology, _) = Topology::read(&self.directory.client, &record.home_region, 0)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let regions = BTreeMap::from([(record.home_region.clone(), topology)]);
        let canonical = kv::actor(&regions, &record.home_region, &record.id)
            .map_err(|e| Status::data_loss(e.to_string()))?;
        if self.actor != canonical {
            return Err(Status::permission_denied("noncanonical KV actor identity").into());
        }
        let meta = self.meta().await?;
        let now = now_ms();
        match request
            .operation
            .ok_or_else(|| Status::invalid_argument("missing KV operation"))?
        {
            Operation::Write(write) => self.write(write, meta, &record, now).await,
            Operation::Get(get) => {
                minimum(&meta, get.min_revision)?;
                if get.keys.is_empty() || get.keys.len() > 100 {
                    return Err(Status::invalid_argument("GET requires 1 to 100 keys").into());
                }
                let mut reply = answer(&meta);
                let mut size = 0;
                for key in get.keys {
                    valid_key(&key)?;
                    let value = self
                        .row(&key, &meta)
                        .await?
                        .filter(|v| !v.expired(now / 1000));
                    size += value.as_ref().map_or(0, Message::encoded_len);
                    if size > kv::MAX_REQUEST {
                        return Err(
                            Status::resource_exhausted("GET response exceeds 100 MiB").into()
                        );
                    }
                    reply.items.push(Item { key, value });
                }
                Ok(reply)
            }
            Operation::List(list) => self.list(list, &meta, now).await,
            Operation::Compact(_) => {
                let mut meta = meta;
                let changes = self.cleanup(&mut meta, now, &BTreeMap::new()).await?;
                if !changes.is_empty() {
                    let mut changes = changes;
                    changes.push(change(META.to_vec(), Some(meta.encode_to_vec())));
                    self.storage.commit_at(changes, now).await?;
                }
                Ok(answer(&meta))
            }
            Operation::Purge(_) => {
                Err(Status::unimplemented("purge is handled by Compute admission").into())
            }
        }
    }

    async fn bytes(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let values = self.storage.get(vec![key.clone()]).await?;
        if values.len() != 1 || values[0].key != key {
            return Err(Status::data_loss("volume GET did not return the requested key").into());
        }
        Ok(values.into_iter().next().and_then(|v| v.value))
    }

    async fn meta(&self) -> Result<Meta> {
        match self.bytes(META.to_vec()).await? {
            None => Ok(Meta {
                version: 1,
                ..Default::default()
            }),
            Some(bytes) => {
                let meta = Meta::decode(bytes.as_slice()).map_err(corrupt)?;
                if meta.version != 1 || meta.revision == 0 {
                    return Err(Status::data_loss("invalid KV namespace metadata").into());
                }
                Ok(meta)
            }
        }
    }

    async fn row(&self, key: &[u8], meta: &Meta) -> Result<Option<Stored>> {
        Ok(self
            .bytes(prefixed(DATA, key))
            .await?
            .map(|bytes| decode_row(&bytes, meta))
            .transpose()?)
    }

    async fn write(
        &mut self,
        write: Write,
        mut meta: Meta,
        record: &kv::Record,
        now: u64,
    ) -> Result<Reply> {
        if let Some(id) = &write.retry {
            if id.is_empty() || id.len() > 512 {
                return Err(
                    Status::invalid_argument("retry identity must contain 1 to 512 bytes").into(),
                );
            }
            if let Some(bytes) = self.bytes(prefixed(RETRY, id.as_bytes())).await? {
                let retained = decode_retained(&bytes, &meta, kv::RETRY_MS, false)?;
                if retained.expires_at > now {
                    return Ok(Reply {
                        revision: retained.revision,
                        committed_at: retained.committed_at,
                        written_keys: retained.written_keys,
                        ..Default::default()
                    });
                }
            }
        }
        let final_values = validate_write(&write, now)?;
        let written_keys = write.changes.len() as u32;
        if write
            .expected
            .is_some_and(|expected| expected != meta.revision)
        {
            return Err(Status::aborted("namespace revision changed").into());
        }
        let mut changes = self.cleanup(&mut meta, now, &final_values).await?;
        let after_cleanup = meta.bytes;
        // Old values can be much larger than their replacements; bound each collected batch too.
        let requested: Vec<_> = final_values.keys().collect();
        for batch in requested.chunks(8) {
            let keys: Vec<_> = batch.iter().map(|key| prefixed(DATA, key)).collect();
            let old = previous_rows(&keys, self.storage.get(keys.clone()).await?, &meta)?;
            for key in batch {
                let value = &final_values[*key];
                let previous = &old[&prefixed(DATA, key)];
                meta.bytes = replace_bytes(meta.bytes, key, previous.as_ref(), value.as_ref())?;
            }
        }
        if record
            .storage_bytes
            .is_some_and(|limit| meta.bytes > limit && meta.bytes > after_cleanup)
        {
            self.quota_rejections.inc();
            if !changes.is_empty() {
                meta.bytes = after_cleanup;
                changes.push(change(META.to_vec(), Some(meta.encode_to_vec())));
                self.storage.commit_at(changes, now).await?;
            }
            return Err(Status::resource_exhausted("namespace storage quota exceeded").into());
        }
        meta.revision = meta
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("namespace revision exhausted"))?;
        meta.committed_at = now;
        for (key, value) in final_values {
            changes.push(change(
                prefixed(DATA, &key),
                value.map(|value| {
                    Row {
                        version: 1,
                        revision: meta.revision,
                        value: Some(value),
                    }
                    .encode_to_vec()
                }),
            ));
        }
        if let Some(id) = &write.retry {
            changes.push(change(
                prefixed(RETRY, id.as_bytes()),
                Some(
                    Retained {
                        version: 1,
                        revision: meta.revision,
                        committed_at: now,
                        expires_at: now
                            .checked_add(kv::RETRY_MS)
                            .ok_or_else(|| Status::out_of_range("retry expiry"))?,
                        write: None,
                        written_keys,
                    }
                    .encode_to_vec(),
                ),
            ));
        }
        changes.push(change(
            prefixed(EVENT, &meta.revision.to_be_bytes()),
            Some(
                Retained {
                    version: 1,
                    revision: meta.revision,
                    committed_at: now,
                    expires_at: now
                        .checked_add(kv::EVENT_MS)
                        .ok_or_else(|| Status::out_of_range("event expiry"))?,
                    write: Some(write),
                    written_keys,
                }
                .encode_to_vec(),
            ),
        ));
        changes.push(change(META.to_vec(), Some(meta.encode_to_vec())));
        self.storage.commit_at(changes, now).await?;
        Ok(Reply {
            written_keys,
            ..answer(&meta)
        })
    }

    async fn list(&self, list: kv::List, meta: &Meta, now: u64) -> Result<Reply> {
        minimum(meta, list.min_revision)?;
        if list.limit == 0
            || list.limit > 1000
            || list.prefix.len() > kv::MAX_KEY
            || std::str::from_utf8(&list.prefix).is_err()
        {
            return Err(
                Status::invalid_argument("invalid LIST prefix or limit (1 to 1000)").into(),
            );
        }
        let mut start = prefixed(DATA, &list.prefix);
        if let Some(after) = list.after {
            valid_key(&after)?;
            start = start.max(exclusive(prefixed(DATA, &after)));
        }
        let mut reply = answer(meta);
        loop {
            let (page, more) = self.storage.scan(start.clone(), 128).await?;
            if more && page.is_empty() {
                return Err(Status::data_loss("volume scan made no progress").into());
            }
            for entry in &page {
                if entry.key < start {
                    return Err(Status::data_loss("volume scan regressed").into());
                }
                let Some(key) = entry.key.strip_prefix(DATA) else {
                    return Ok(reply);
                };
                if !key.starts_with(&list.prefix) {
                    return Ok(reply);
                }
                valid_key(key).map_err(|_| Status::data_loss("invalid stored customer key"))?;
                if let Some(bytes) = &entry.value {
                    let mut value = decode_row(bytes, meta)?;
                    if value.expired(now / 1000) {
                        continue;
                    }
                    if reply.items.len() == list.limit as usize {
                        reply.cursor = reply.items.last().map(|item| item.key.clone());
                        return Ok(reply);
                    }
                    value.value.clear();
                    reply.items.push(Item {
                        key: key.to_vec(),
                        value: Some(value),
                    });
                }
            }
            if !more {
                return Ok(reply);
            }
            start = exclusive(
                page.last()
                    .expect("nonempty page checked above")
                    .key
                    .clone(),
            );
        }
    }

    async fn cleanup(
        &mut self,
        meta: &mut Meta,
        now: u64,
        replacing: &BTreeMap<Vec<u8>, Option<Stored>>,
    ) -> Result<Vec<pb::Change>> {
        let mut changes = Vec::new();
        for (index, prefix) in [DATA, EVENT, RETRY].into_iter().enumerate() {
            let start = self.cleanup[index].clone();
            let (page, more) = self.storage.scan(start.clone(), CLEAN_PAGE).await?;
            if more && page.is_empty() {
                return Err(Status::data_loss("volume cleanup scan made no progress").into());
            }
            let mut within = true;
            for entry in &page {
                if entry.key < start {
                    return Err(Status::data_loss("volume cleanup scan regressed").into());
                }
                let Some(key) = entry.key.strip_prefix(prefix) else {
                    within = false;
                    break;
                };
                let Some(bytes) = &entry.value else { continue };
                let expired = if prefix == DATA {
                    valid_key(key).map_err(|_| Status::data_loss("invalid stored customer key"))?;
                    let value = decode_row(bytes, meta)?;
                    if replacing.contains_key(key) {
                        continue;
                    }
                    if value.expired(now / 1000) {
                        meta.bytes = replace_bytes(meta.bytes, key, Some(&value), None)?;
                        true
                    } else {
                        false
                    }
                } else {
                    let window = if prefix == EVENT {
                        kv::EVENT_MS
                    } else {
                        kv::RETRY_MS
                    };
                    let retained = decode_retained(bytes, meta, window, prefix == EVENT)?;
                    if prefix == EVENT && key != retained.revision.to_be_bytes() {
                        return Err(Status::data_loss("event revision does not match key").into());
                    }
                    retained.expires_at <= now
                };
                if expired {
                    changes.push(change(entry.key.clone(), None));
                }
            }
            self.cleanup[index] = if more && within {
                exclusive(
                    page.last()
                        .expect("nonempty page checked above")
                        .key
                        .clone(),
                )
            } else {
                prefix.to_vec()
            };
        }
        Ok(changes)
    }
}

fn answer(meta: &Meta) -> Reply {
    Reply {
        revision: meta.revision,
        committed_at: meta.committed_at,
        ..Default::default()
    }
}

fn minimum(meta: &Meta, minimum: u64) -> std::result::Result<(), Status> {
    if meta.revision < minimum {
        Err(Status::unavailable("namespace is behind minimum revision"))
    } else {
        Ok(())
    }
}

fn prefixed(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    [prefix, key].concat()
}
fn exclusive(mut key: Vec<u8>) -> Vec<u8> {
    key.push(0);
    key
}
fn change(key: Vec<u8>, value: Option<Vec<u8>>) -> pb::Change {
    pb::Change { key, value }
}
fn corrupt(error: prost::DecodeError) -> Status {
    Status::data_loss(error.to_string())
}

fn valid_key(key: &[u8]) -> std::result::Result<(), Status> {
    if key.is_empty()
        || key.len() > kv::MAX_KEY
        || matches!(key, b"." | b"..")
        || std::str::from_utf8(key).is_err()
    {
        Err(Status::invalid_argument(
            "key must be 1 to 512 UTF-8 bytes, excluding . and ..",
        ))
    } else {
        Ok(())
    }
}

fn validate_value(value: &Stored, now: Option<u64>) -> std::result::Result<(), Status> {
    if value.value.len() > kv::MAX_VALUE {
        return Err(Status::invalid_argument("value exceeds 25 MiB"));
    }
    if let Some(metadata) = &value.metadata
        && (metadata.len() > kv::MAX_METADATA
            || serde_json::from_str::<serde_json::Value>(metadata).is_err())
    {
        return Err(Status::invalid_argument(
            "metadata must be JSON of at most 1 KiB",
        ));
    }
    if let Some(now) = now
        && value.expiration != 0
        && value.expiration < now / 1000 + 60
    {
        return Err(Status::invalid_argument(
            "expiration must be at least 60 seconds ahead",
        ));
    }
    Ok(())
}

fn validate_write(
    write: &Write,
    now: u64,
) -> std::result::Result<BTreeMap<Vec<u8>, Option<Stored>>, Status> {
    if write.changes.is_empty() || write.changes.len() > kv::MAX_BULK {
        return Err(Status::invalid_argument(
            "write requires 1 to 10000 mutations",
        ));
    }
    let mut bytes = 0usize;
    let mut final_values = BTreeMap::new();
    for mutation in &write.changes {
        valid_key(&mutation.key)?;
        let mut resolved = mutation.value.clone();
        if let Some(ttl) = mutation.expiration_ttl {
            let value = resolved
                .as_mut()
                .ok_or_else(|| Status::invalid_argument("a delete has no expiration"))?;
            if ttl < 60 || value.expiration != 0 {
                return Err(Status::invalid_argument(
                    "relative expiration must be at least 60 seconds and exclude absolute expiration",
                ));
            }
            value.expiration = (now / 1000)
                .checked_add(ttl)
                .ok_or_else(|| Status::invalid_argument("expiration overflow"))?;
        }
        if let Some(value) = &resolved {
            validate_value(value, Some(now))?;
        }
        bytes = bytes
            .checked_add(mutation.key.len())
            .and_then(|n| {
                n.checked_add(mutation.value.as_ref().map_or(0, |v| {
                    v.value.len() + v.metadata.as_ref().map_or(0, String::len)
                }))
            })
            .ok_or_else(|| Status::invalid_argument("request size overflow"))?;
        if bytes > kv::MAX_REQUEST {
            return Err(Status::invalid_argument("request exceeds 100 MiB"));
        }
        final_values.insert(mutation.key.clone(), resolved);
    }
    Ok(final_values)
}

fn decode_row(bytes: &[u8], meta: &Meta) -> std::result::Result<Stored, Status> {
    let row = Row::decode(bytes).map_err(corrupt)?;
    if row.version != 1 || row.revision == 0 || row.revision > meta.revision {
        return Err(Status::data_loss("invalid KV value envelope"));
    }
    let value = row
        .value
        .ok_or_else(|| Status::data_loss("missing KV value"))?;
    validate_value(&value, None).map_err(|e| Status::data_loss(e.message().to_owned()))?;
    Ok(value)
}

fn previous_rows(
    requested: &[Vec<u8>],
    values: Vec<pb::Value>,
    meta: &Meta,
) -> std::result::Result<BTreeMap<Vec<u8>, Option<Stored>>, Status> {
    let mut rows = BTreeMap::new();
    for value in values {
        if !requested.contains(&value.key) || rows.contains_key(&value.key) {
            return Err(Status::data_loss(
                "volume GET returned an unexpected or duplicate key",
            ));
        }
        rows.insert(
            value.key,
            value
                .value
                .map(|bytes| decode_row(&bytes, meta))
                .transpose()?,
        );
    }
    if rows.len() != requested.len() {
        return Err(Status::data_loss("volume GET omitted a requested key"));
    }
    Ok(rows)
}

fn decode_retained(
    bytes: &[u8],
    meta: &Meta,
    window: u64,
    event: bool,
) -> std::result::Result<Retained, Status> {
    let row = Retained::decode(bytes).map_err(corrupt)?;
    if row.version != 1
        || row.revision == 0
        || row.revision > meta.revision
        || row.committed_at.checked_add(window) != Some(row.expires_at)
        || row.write.is_some() != event
        || row.written_keys == 0
        || row.written_keys as usize > kv::MAX_BULK
        || row
            .write
            .as_ref()
            .is_some_and(|write| write.changes.len() != row.written_keys as usize)
    {
        return Err(Status::data_loss("invalid KV retained record"));
    }
    Ok(row)
}

fn replace_bytes(
    current: u64,
    key: &[u8],
    old: Option<&Stored>,
    new: Option<&Stored>,
) -> std::result::Result<u64, Status> {
    let size = |value: &Stored| (key.len() + value.encoded_len()) as u64;
    current
        .checked_sub(old.map_or(0, size))
        .and_then(|n| n.checked_add(new.map_or(0, size)))
        .ok_or_else(|| Status::data_loss("KV quota accounting overflow or underflow"))
}

#[cfg(test)]
#[path = "../tests/unit/kv.rs"]
mod tests;
