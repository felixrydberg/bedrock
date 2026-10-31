//! Atomic event/state persistence (#11 / B-11). Standalone acknowledgement follows
//! remote durability; future Raft supplies committed batches through the same path.

use std::collections::BTreeMap;
use std::sync::Arc;

use prost::Message;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::pb;
use super::{Event, Operation};
use crate::storage::namespace::{Changes, NamespaceDatabase, NamespaceFactory, valid_identity};
use crate::storage::{
    Entry, ErrorContext, KeyRange, StorageEngine, StorageError, StorageResult, WriteBatch,
};

pub const DEFAULT_RETENTION_MS: u64 = 86_400_000;
const FORMAT_VERSION: u32 = 2;
const META_KEY: &[u8] = b"m";

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Retention {
    pub event_retention_ms: u64,
    pub retry_retention_ms: u64,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            event_retention_ms: DEFAULT_RETENTION_MS,
            retry_retention_ms: DEFAULT_RETENTION_MS,
        }
    }
}

impl Retention {
    pub fn validate(&self) -> StorageResult<()> {
        if self.event_retention_ms == 0 || self.retry_retention_ms == 0 {
            return Err(StorageError::Unsupported(context(
                "retention",
                "event and retry retention must be positive",
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub event: Event,
    pub retry_key: Option<String>,
    pub retry_expires_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryRecord {
    pub sequence: u64,
    pub expires_at: u64,
}

#[derive(Clone, Debug)]
pub struct Progress {
    pub sequence: u64,
    pub committed_at: u64,
    pub retries: BTreeMap<String, RetryRecord>,
    pub pruned_through: u64,
    pub raft: Option<RaftPosition>,
}

/// Covered committed Raft log position, independent of namespace/engine sequences.
/// A native recovery checkpoint must also retain Raft membership before log purge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftPosition {
    pub index: u64,
    pub term: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
    pub dataset: String,
    pub namespace: String,
    pub sequence: u64,
    pub committed_at: u64,
    pub retries: BTreeMap<String, RetryRecord>,
    pub values: BTreeMap<Vec<u8>, Vec<u8>>,
    pub pruned_through: u64,
    pub raft: Option<RaftPosition>,
}

impl View {
    /// Installs a coherent view into one physical namespace range in a pushed dataset's engine.
    pub fn install(
        &self,
        engine: &(impl StorageEngine + ?Sized),
        namespace: &str,
        prefix: KeyRange,
    ) -> StorageResult<()> {
        let invalid = |detail: &str| {
            StorageError::Unavailable(ErrorContext {
                engine: "materialized",
                instance: format!("{}/{}", self.dataset, self.namespace),
                file: None,
                partition: None,
                detail: detail.into(),
            })
        };
        if namespace != self.namespace
            || engine
                .revision(namespace)?
                .is_some_and(|revision| revision > self.sequence)
        {
            return Err(invalid(
                "namespace mismatch or target older than installed state",
            ));
        }
        let mut batch = WriteBatch::default();
        batch.delete_range(prefix.clone());
        for (key, value) in &self.values {
            let physical = [prefix.start.as_slice(), key.as_slice()].concat();
            if !prefix.contains(&physical) {
                return Err(invalid(
                    "namespace key is outside supplied installation range",
                ));
            }
            batch.put(physical, value.clone());
        }
        batch.set_revision(namespace, self.sequence);
        engine.write(batch)
    }
}

#[derive(Clone, PartialEq, Message)]
struct StateRecord {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    dataset: String,
    #[prost(string, tag = "3")]
    namespace: String,
    #[prost(uint64, tag = "4")]
    sequence: u64,
    #[prost(uint64, tag = "5")]
    committed_at: u64,
    #[prost(uint64, optional, tag = "6")]
    raft_index: Option<u64>,
    #[prost(uint64, optional, tag = "7")]
    raft_term: Option<u64>,
    #[prost(uint64, tag = "8")]
    pruned_through: u64,
}

#[derive(Clone, PartialEq, Message)]
struct StoredEvent {
    #[prost(message, optional, tag = "1")]
    event: Option<pb::Event>,
    #[prost(string, optional, tag = "2")]
    retry_key: Option<String>,
    #[prost(uint64, tag = "3")]
    retry_expires_at: u64,
}

#[derive(Clone, PartialEq, Message)]
struct StoredRetry {
    #[prost(uint64, tag = "1")]
    sequence: u64,
    #[prost(uint64, tag = "2")]
    expires_at: u64,
}

pub struct Materialized {
    db: Arc<dyn NamespaceDatabase>,
    dataset: String,
    namespace: String,
    lock: Mutex<()>,
}

fn context(instance: &str, detail: impl Into<String>) -> ErrorContext {
    ErrorContext {
        engine: "namespace",
        instance: instance.into(),
        file: None,
        partition: None,
        detail: detail.into(),
    }
}

pub(crate) fn validate_scope(dataset: &str, namespace: &str) -> StorageResult<()> {
    if !valid_identity(dataset) || !valid_identity(namespace) {
        return Err(StorageError::Unsupported(context(
            "namespace",
            "invalid dataset or namespace identity",
        )));
    }
    Ok(())
}

fn key(prefix: u8, raw: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(raw.len() + 1);
    encoded.push(prefix);
    encoded.extend_from_slice(raw);
    encoded
}

impl Materialized {
    pub async fn open(
        factory: &dyn NamespaceFactory,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Self> {
        let materialized = Self {
            db: factory.open(dataset, namespace).await?,
            dataset: dataset.into(),
            namespace: namespace.into(),
            lock: Mutex::new(()),
        };
        if let Err(error) = materialized.read_progress().await {
            let _ = materialized.db.close().await;
            return Err(error);
        }
        Ok(materialized)
    }

    fn corrupt(&self, detail: impl Into<String>) -> StorageError {
        StorageError::Corruption(context(
            &format!("{}/{}", self.dataset, self.namespace),
            detail,
        ))
    }

    async fn read_view(&self) -> StorageResult<View> {
        let entries = self.db.scan(b"").await?;
        let view = self.decode_view(entries)?;
        self.events_locked(&view).await?;
        Ok(view)
    }

    // Applying/publishing need revision and retry metadata, not all namespace values.
    async fn read_progress(&self) -> StorageResult<View> {
        let Some(metadata) = self.db.get(META_KEY).await? else {
            // A genuinely new database is empty; data without atomic metadata is corruption.
            return self.read_view().await;
        };
        let mut entries = self.db.scan(b"r").await?;
        entries.push((META_KEY.to_vec(), metadata));
        self.decode_view(entries)
    }

    fn decode_view(&self, entries: Vec<Entry>) -> StorageResult<View> {
        let mut has_events = false;
        let mut view = View {
            dataset: self.dataset.clone(),
            namespace: self.namespace.clone(),
            sequence: 0,
            committed_at: 0,
            retries: BTreeMap::new(),
            values: BTreeMap::new(),
            pruned_through: 0,
            raft: None,
        };
        let mut metadata = None;
        for (encoded, value) in entries {
            match encoded.as_slice() {
                [b'm'] => {
                    if metadata.is_some() {
                        return Err(self.corrupt("duplicate namespace metadata"));
                    }
                    metadata =
                        Some(StateRecord::decode(value.as_slice()).map_err(|_| {
                            self.corrupt("namespace metadata is not valid protobuf")
                        })?);
                }
                [b'v', raw @ ..] if !raw.is_empty() => {
                    if view.values.insert(raw.to_vec(), value).is_some() {
                        return Err(self.corrupt("duplicate value key"));
                    }
                }
                [b'e', raw @ ..] if raw.len() == 8 => {
                    has_events = true;
                    self.decode_event(&encoded, &value)?;
                }
                [b'r', raw @ ..] => {
                    let retry_key = String::from_utf8(raw.to_vec())
                        .map_err(|_| self.corrupt("retry key is not UTF-8"))?;
                    let retry = StoredRetry::decode(value.as_slice())
                        .map_err(|_| self.corrupt("retry record is not valid protobuf"))?;
                    if view
                        .retries
                        .insert(
                            retry_key,
                            RetryRecord {
                                sequence: retry.sequence,
                                expires_at: retry.expires_at,
                            },
                        )
                        .is_some()
                    {
                        return Err(self.corrupt("duplicate retry key"));
                    }
                }
                _ => return Err(self.corrupt("invalid materialized record key")),
            }
        }
        match metadata {
            Some(record) => {
                if record.version != FORMAT_VERSION
                    || record.dataset != self.dataset
                    || record.namespace != self.namespace
                    || record.sequence == 0
                {
                    return Err(
                        self.corrupt("invalid namespace metadata version, scope or revision")
                    );
                }
                view.sequence = record.sequence;
                view.committed_at = record.committed_at;
                view.pruned_through = record.pruned_through;
                view.raft = match (record.raft_index, record.raft_term) {
                    (None, None) => None,
                    (Some(index), Some(term)) if index > 0 => Some(RaftPosition { index, term }),
                    _ => return Err(self.corrupt("invalid represented Raft position")),
                };
                if view.pruned_through > view.sequence {
                    return Err(self.corrupt("retained-event boundary exceeds revision"));
                }
            }
            None if has_events || !view.values.is_empty() || !view.retries.is_empty() => {
                return Err(self.corrupt("namespace data has no atomic revision metadata"));
            }
            None => {}
        }
        if (view.sequence == 0 && (!view.values.is_empty() || !view.retries.is_empty()))
            || view.retries.values().any(|retry| {
                retry.sequence == 0
                    || retry.sequence > view.sequence
                    || retry.expires_at <= view.committed_at
            })
        {
            return Err(self.corrupt("invalid retry revision/expiry or zero-revision data"));
        }
        Ok(view)
    }

    pub async fn view(&self) -> StorageResult<View> {
        let _guard = self.lock.lock().await;
        self.read_view().await
    }

    pub async fn progress(&self) -> StorageResult<Progress> {
        let _guard = self.lock.lock().await;
        let view = self.read_progress().await?;
        Ok(Progress {
            sequence: view.sequence,
            committed_at: view.committed_at,
            retries: view.retries,
            pruned_through: view.pruned_through,
            raft: view.raft,
        })
    }

    pub async fn apply(&self, entries: &[Committed]) -> StorageResult<()> {
        let _guard = self.lock.lock().await;
        self.apply_locked(entries, None).await
    }

    /// The common standalone/future-Raft persistence operation. All events, values,
    /// retry results and progress enter ONE atomic write, followed by remote durability.
    /// A returned position is not a native checkpoint or permission to purge Raft.
    pub async fn persist(
        &self,
        entries: &[Committed],
        raft: Option<RaftPosition>,
    ) -> StorageResult<Progress> {
        let _guard = self.lock.lock().await;
        self.apply_locked(entries, raft).await?;
        self.db.flush().await?;
        let view = self.read_progress().await?;
        Ok(Progress {
            sequence: view.sequence,
            committed_at: view.committed_at,
            retries: view.retries,
            pruned_through: view.pruned_through,
            raft: view.raft,
        })
    }

    async fn apply_locked(
        &self,
        entries: &[Committed],
        raft: Option<RaftPosition>,
    ) -> StorageResult<()> {
        let mut view = self.read_progress().await?;
        if view.sequence == 0 && entries.is_empty() && raft.is_some() {
            return Err(StorageError::Unsupported(context(
                "namespace",
                "cannot advance Raft position without namespace events",
            )));
        }
        if let Some(position) = raft {
            if position.index == 0
                || view.raft.is_some_and(|old| {
                    position.index < old.index
                        || position.term < old.term
                        || (position.index == old.index && position.term != old.term)
                })
            {
                return Err(self.corrupt("represented Raft position regressed"));
            }
        } else if view.raft.is_some()
            && entries
                .iter()
                .any(|entry| entry.event.sequence > view.sequence)
        {
            return Err(self.corrupt("Raft-backed state requires a committed Raft position"));
        }
        let original_retries = view.retries.clone();
        let original_sequence = view.sequence;
        let mut changes: Changes = Vec::new();
        for entry in entries {
            let event = &entry.event;
            if event.dataset != self.dataset
                || event.namespace != self.namespace
                || event.sequence == 0
                || event.key.is_empty()
            {
                return Err(self.corrupt("committed event has invalid scope, sequence or key"));
            }
            if event.sequence <= view.sequence {
                if event.sequence > view.pruned_through {
                    let encoded = key(b'e', &event.sequence.to_be_bytes());
                    let stored = self
                        .db
                        .get(&encoded)
                        .await?
                        .ok_or_else(|| self.corrupt("retained event is missing"))?;
                    if self.decode_event(&encoded, &stored)? != *entry {
                        return Err(self.corrupt("duplicate revision has different contents"));
                    }
                }
                continue;
            }
            if view.sequence.checked_add(1) != Some(event.sequence) {
                return Err(self.corrupt("gap in committed materialization events"));
            }
            if event.committed_at < view.committed_at {
                return Err(self.corrupt("commit timestamp regressed"));
            }
            if (entry.retry_key.is_some() && entry.retry_expires_at <= event.committed_at)
                || (entry.retry_key.is_none() && entry.retry_expires_at != 0)
            {
                return Err(
                    self.corrupt("committed retry expiry must follow its fixed commit time")
                );
            }
            let value = match &event.operation {
                Operation::Set(value) => Some(value.clone()),
                Operation::Delete => None,
            };
            changes.push((key(b'v', &event.key), value));
            changes.push((
                key(b'e', &event.sequence.to_be_bytes()),
                Some(
                    StoredEvent {
                        event: Some(event.clone().into()),
                        retry_key: entry.retry_key.clone(),
                        retry_expires_at: entry.retry_expires_at,
                    }
                    .encode_to_vec(),
                ),
            ));
            view.sequence = event.sequence;
            view.committed_at = event.committed_at;
            view.retries
                .retain(|_, retry| retry.expires_at > event.committed_at);
            if let Some(retry_key) = &entry.retry_key {
                view.retries.insert(
                    retry_key.clone(),
                    RetryRecord {
                        sequence: event.sequence,
                        expires_at: entry.retry_expires_at,
                    },
                );
            }
        }
        if view.sequence == original_sequence && (raft.is_none() || view.raft == raft) {
            return Ok(());
        }
        view.raft = raft.or(view.raft);
        for retry_key in original_retries.keys() {
            if !view.retries.contains_key(retry_key) {
                changes.push((key(b'r', retry_key.as_bytes()), None));
            }
        }
        for (retry_key, retry) in &view.retries {
            if original_retries.get(retry_key) != Some(retry) {
                changes.push((
                    key(b'r', retry_key.as_bytes()),
                    Some(
                        StoredRetry {
                            sequence: retry.sequence,
                            expires_at: retry.expires_at,
                        }
                        .encode_to_vec(),
                    ),
                ));
            }
        }
        changes.push((
            META_KEY.to_vec(),
            Some(
                StateRecord {
                    version: FORMAT_VERSION,
                    dataset: self.dataset.clone(),
                    namespace: self.namespace.clone(),
                    sequence: view.sequence,
                    committed_at: view.committed_at,
                    raft_index: view.raft.map(|position| position.index),
                    raft_term: view.raft.map(|position| position.term),
                    pruned_through: view.pruned_through,
                }
                .encode_to_vec(),
            ),
        ));
        self.db.write(changes).await
    }

    fn decode_event(&self, encoded: &[u8], value: &[u8]) -> StorageResult<Committed> {
        let raw = encoded
            .strip_prefix(b"e")
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
            .ok_or_else(|| self.corrupt("invalid retained event key"))?;
        let stored = StoredEvent::decode(value)
            .map_err(|_| self.corrupt("invalid retained event protobuf"))?;
        let event = Event::from(
            stored
                .event
                .ok_or_else(|| self.corrupt("missing retained event"))?,
        );
        if event.sequence == 0
            || event.sequence != u64::from_be_bytes(raw)
            || event.dataset != self.dataset
            || event.namespace != self.namespace
            || event.key.is_empty()
            || (stored.retry_key.is_some() && stored.retry_expires_at <= event.committed_at)
            || (stored.retry_key.is_none() && stored.retry_expires_at != 0)
        {
            return Err(self.corrupt("invalid retained event scope, revision or expiry"));
        }
        Ok(Committed {
            event,
            retry_key: stored.retry_key,
            retry_expires_at: stored.retry_expires_at,
        })
    }

    async fn events_locked(&self, view: &View) -> StorageResult<Vec<Committed>> {
        let mut events = Vec::new();
        let mut expected = view.pruned_through;
        let mut timestamp = 0;
        for (encoded, value) in self.db.scan(b"e").await? {
            let entry = self.decode_event(&encoded, &value)?;
            if expected.checked_add(1) != Some(entry.event.sequence)
                || entry.event.sequence > view.sequence
                || entry.event.committed_at < timestamp
                || entry.event.committed_at > view.committed_at
            {
                return Err(self.corrupt("retained event history has a gap or invalid timestamp"));
            }
            expected = entry.event.sequence;
            timestamp = entry.event.committed_at;
            events.push(entry);
        }
        if expected != view.sequence {
            return Err(self.corrupt("retained event history does not reach namespace revision"));
        }
        Ok(events)
    }

    pub async fn events(&self) -> StorageResult<Vec<Committed>> {
        let _guard = self.lock.lock().await;
        self.events_locked(&self.read_progress().await?).await
    }

    /// Reads at most `limit` retained events from `from` by sequence key, never the whole history.
    pub async fn events_from(&self, from: u64, limit: u64) -> StorageResult<Vec<Committed>> {
        let _guard = self.lock.lock().await;
        let view = self.read_progress().await?;
        if from <= view.pruned_through {
            return Err(StorageError::Unavailable(context(
                &format!("{}/{}", self.dataset, self.namespace),
                "requested events were pruned",
            )));
        }
        let through = view
            .sequence
            .min(from.saturating_add(limit).saturating_sub(1));
        let mut events = Vec::new();
        for sequence in from..=through {
            let encoded = key(b'e', &sequence.to_be_bytes());
            let stored = self
                .db
                .get(&encoded)
                .await?
                .ok_or_else(|| self.corrupt("retained event is missing"))?;
            events.push(self.decode_event(&encoded, &stored)?);
        }
        Ok(events)
    }

    /// Event-triggered expiry cleanup. Progress and event removals are atomic;
    /// pinned native views remain protected by the engine's own MVCC/checkpoint GC.
    pub async fn cleanup(&self, now: u64, retention: &Retention) -> StorageResult<u64> {
        self.cleanup_through(now, retention, u64::MAX).await
    }

    /// A recovery lease protects events after its coherent bootstrap boundary.
    pub async fn cleanup_through(
        &self,
        now: u64,
        retention: &Retention,
        through: u64,
    ) -> StorageResult<u64> {
        retention.validate()?;
        let _guard = self.lock.lock().await;
        let mut view = self.read_progress().await?;
        let mut changes = Vec::new();
        // Read only the expired prefix, not the whole retained history on every write.
        // Bound one pass; further namespace activity can continue physical reclamation.
        let limit = view
            .sequence
            .min(view.pruned_through.saturating_add(1024))
            .min(through);
        while view.pruned_through < limit {
            let sequence = view.pruned_through + 1;
            let encoded = key(b'e', &sequence.to_be_bytes());
            let stored = self
                .db
                .get(&encoded)
                .await?
                .ok_or_else(|| self.corrupt("retained event is missing during cleanup"))?;
            let entry = self.decode_event(&encoded, &stored)?;
            if entry.event.committed_at > view.committed_at {
                return Err(self.corrupt("retained event timestamp exceeds namespace progress"));
            }
            if now.saturating_sub(entry.event.committed_at) < retention.event_retention_ms {
                break;
            }
            view.pruned_through = entry.event.sequence;
            changes.push((encoded, None));
        }
        for (retry_key, retry) in &view.retries {
            if retry.expires_at <= now {
                changes.push((key(b'r', retry_key.as_bytes()), None));
            }
        }
        if changes.is_empty() {
            return Ok(view.pruned_through);
        }
        changes.push((
            META_KEY.to_vec(),
            Some(
                StateRecord {
                    version: FORMAT_VERSION,
                    dataset: self.dataset.clone(),
                    namespace: self.namespace.clone(),
                    sequence: view.sequence,
                    committed_at: view.committed_at,
                    raft_index: view.raft.map(|p| p.index),
                    raft_term: view.raft.map(|p| p.term),
                    pruned_through: view.pruned_through,
                }
                .encode_to_vec(),
            ),
        ));
        self.db.write(changes).await?;
        self.db.flush().await?;
        Ok(view.pruned_through)
    }

    pub async fn publish(&self) -> StorageResult<u64> {
        let _guard = self.lock.lock().await;
        let sequence = self.read_progress().await?.sequence;
        self.db.flush().await?;
        Ok(sequence)
    }

    pub async fn close(&self) -> StorageResult<()> {
        let _guard = self.lock.lock().await;
        self.db.close().await
    }
}

#[cfg(test)]
#[path = "../../tests/unit/replication/materialized.rs"]
mod tests;
