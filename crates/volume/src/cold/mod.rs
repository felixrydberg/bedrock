//! The cold tier (#113 / S-05, `statefull/v2.md` §5 Cold tier).
//!
//! Each `volume` range of each cell has one range database ([`db`]) holding the actors in that range whose first slot is
//! in that cell. Its maintainer is the first owner of the range that is not populating: it opens the database as
//! writer, which fences any other, and every [`FLUSH_INTERVAL`] (sooner after [`FLUSH_BYTES`]) writes every changed
//! actor's keys up to its commit point in one batch with each actor's `meta`. Once that batch is durable, the actor's
//! slots truncate their log and overlay through it. Other slots read the database through [`readers`].
//!
//! Range database keys are `d/{actor}{key}` for values and `m/{actor}` for the actor's [`Meta`], where `{actor}` is
//! `{hash}{namespace}\0{id}\0` as on the slots. Meta keys sit together, so a writer loads every actor's flushed point
//! with one scan when it opens and never reads meta again.
//!
//! - **Exactly once.** An actor is written only beyond the sequence its `meta` holds, and the batch carries the keys as
//!   of its commit point, rebuilt from the log. A crash between a flush and its truncation flushes the same keys again
//!   (the same values), or, past them, only what is newer; a slot truncating twice is a no-op.
//! - **Commit point.** Only what a slot knows is committed is flushed, which is at most what the holder announced.
//! - **Backpressure.** While a range cannot flush, its slots' logs grow until the store refuses appends.

pub mod db;
pub mod placement;
pub mod readers;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_lib::placement::{Directory, State};
use bedrock_lib::storage::namespace::Changes;
use bedrock_lib::storage::{KeyRange, StorageError};
use bedrock_lib::volume as pb;
use tokio::time::Instant;
use tonic::Status;

use crate::store::{Actor, Error, Pending, Store};
use db::{Database, Databases, Writer};
use readers::Readers;

/// How often a range flushes.
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// Unflushed log bytes on the node that start a flush before the interval ends, and the most one batch carries.
pub const FLUSH_BYTES: u64 = 4 << 20;
/// How long a fenced maintainer waits before opening the database again.
pub const FENCED_WAIT: Duration = Duration::from_secs(5);
/// How long a cold read waits for its reader to see the slot's flushed point.
pub const BEHIND_WAIT: Duration = Duration::from_secs(2);

/// What a range database holds for an actor besides its keys: the sequence they are at, its epoch, and the alarm.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Meta {
    pub seq: u64,
    pub epoch: u64,
    pub alarm: Option<u64>,
}

impl Meta {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = [self.seq.to_be_bytes(), self.epoch.to_be_bytes()].concat();
        if let Some(alarm) = self.alarm {
            bytes.extend_from_slice(&alarm.to_be_bytes());
        }
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let word = |at: usize| Some(u64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?));
        let alarm = match bytes.len() {
            16 => None,
            24 => Some(word(16)?),
            _ => return None,
        };
        Some(Meta {
            seq: word(0)?,
            epoch: word(8)?,
            alarm,
        })
    }
}

pub fn data_key(actor: &[u8], key: &[u8]) -> Vec<u8> {
    [b"d/", actor, key].concat()
}

pub fn meta_key(actor: &[u8]) -> Vec<u8> {
    [b"m/", actor].concat()
}

/// The range database of `cell`'s range `[start, end)`. Split and merge (S-05 part 2) clone into a higher generation.
pub fn database(cell: &str, start: u64, end: u64) -> String {
    format!("{cell}/{start:016x}-{end:016x}/0")
}

/// The maintainer of a range: its first owner that is not populating.
pub fn maintainer(owners: &[bedrock_lib::placement::Owner]) -> Option<&str> {
    owners
        .iter()
        .find(|o| o.state != State::Populating)
        .map(|o| o.node.as_str())
}

/// Where an actor's slots and range database are, across cells. Implemented over the topology record and sibling cells'
/// routes; tests stub it.
#[async_trait]
pub trait Placement: Send + Sync {
    /// Whether this node's cell holds the actor's first slot.
    async fn first_here(&self, actor: &pb::Actor) -> Result<bool, Status>;
    /// Asks the actor's slots on other nodes to truncate through `through`. `Ok(false)` while one has not yet.
    async fn truncate_others(&self, actor: &pb::Actor, through: u64) -> Result<bool, Status>;
    /// The range database holding the actor: its first slot cell's range holding its hash.
    async fn database_of(&self, actor: &pb::Actor) -> Result<String, Status>;
}

/// An open writer and every actor's flushed point in it.
struct Maintained {
    db: Arc<dyn Writer>,
    known: BTreeMap<Vec<u8>, Meta>,
}

pub struct Cold {
    store: Arc<Store>,
    databases: Arc<dyn Databases>,
    placement: Arc<dyn Placement>,
    pub readers: Readers,
    cell: String,
    node: String,
    /// Range databases this node writes, by name.
    writers: tokio::sync::Mutex<BTreeMap<String, Maintained>>,
    /// When a fenced database may be opened again.
    fenced: Mutex<BTreeMap<String, Instant>>,
    /// Truncations other slots still owe, by actor prefix.
    owed: Mutex<BTreeMap<Vec<u8>, (pb::Actor, u64)>>,
    /// Unflushed bytes at the last flush, for the early trigger.
    mark: Mutex<i64>,
}

fn status(error: Error) -> Status {
    crate::serve::status(error)
}

fn storage(error: StorageError) -> Status {
    status(Error::Storage(error))
}

impl Cold {
    pub fn new(
        store: Arc<Store>,
        databases: Arc<dyn Databases>,
        placement: Arc<dyn Placement>,
        readers: Readers,
        cell: &str,
        node: &str,
    ) -> Self {
        Cold {
            store,
            databases,
            placement,
            readers,
            cell: cell.to_owned(),
            node: node.to_owned(),
            writers: Default::default(),
            fenced: Mutex::default(),
            owed: Mutex::default(),
            mark: Mutex::new(0),
        }
    }

    /// Whether enough was appended since the last flush to flush before the interval ends.
    pub fn due(&self) -> bool {
        self.store.unflushed() - *self.mark.lock().unwrap_or_else(PoisonError::into_inner)
            >= FLUSH_BYTES as i64
    }

    /// Bytes of the range database this node writes for `[start, end)`, from its manifest; 0 when it writes none.
    pub async fn size(&self, start: u64, end: u64) -> u64 {
        let writers = self.writers.lock().await;
        writers
            .get(&database(&self.cell, start, end))
            .map_or(0, |m| m.db.size())
    }

    /// One flush round: every range this node maintains, then the truncations other slots still owe. Ranges it no
    /// longer maintains are closed. Errors are logged per range; one range's trouble does not hold up the others.
    pub async fn tick(&self, directory: &Directory) {
        *self.mark.lock().unwrap_or_else(PoisonError::into_inner) = self.store.unflushed();
        let mut ranges = Vec::new();
        let mut entries = directory.iter().peekable();
        while let Some((&start, owners)) = entries.next() {
            let end = entries.peek().map_or(0, |(next, _)| **next);
            if maintainer(owners) == Some(self.node.as_str()) {
                ranges.push((start, end));
            }
        }
        let names: Vec<String> = ranges
            .iter()
            .map(|(s, e)| database(&self.cell, *s, *e))
            .collect();
        let stale: Vec<(String, Maintained)> = {
            let mut writers = self.writers.lock().await;
            let gone: Vec<String> = writers
                .keys()
                .filter(|n| !names.contains(n))
                .cloned()
                .collect();
            gone.into_iter()
                .filter_map(|n| writers.remove(&n).map(|m| (n, m)))
                .collect()
        };
        for (name, maintained) in stale {
            if let Err(error) = maintained.db.close().await {
                tracing::debug!(%error, database = %name, "closing a range database writer failed");
            }
        }
        for ((start, end), name) in ranges.into_iter().zip(names) {
            loop {
                match self.flush(&name, start, end).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(error) => {
                        tracing::warn!(%error, database = %name, "range flush failed");
                        break;
                    }
                }
            }
        }
        self.settle().await;
        self.readers.sweep().await;
    }

    /// Opens `name` as writer, unless it was fenced less than [`FENCED_WAIT`] ago, and loads every actor's meta.
    async fn open(&self, name: &str) -> Result<Option<Maintained>, StorageError> {
        let now = bedrock_lib::monotonic_now();
        if self
            .fenced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .is_some_and(|until| now < *until)
        {
            return Ok(None);
        }
        let db = self.databases.open(name).await?;
        let mut known = BTreeMap::new();
        let end = KeyRange::prefix(*b"m/").end.expect("`m/` has an end");
        let mut from = b"m/".to_vec();
        loop {
            let page = db.scan(&from, &end, 1000).await?;
            let Some((last, _)) = page.last() else { break };
            from = [last.as_slice(), &[0]].concat();
            for (key, value) in page {
                let meta = Meta::decode(&value).ok_or_else(|| {
                    StorageError::Corruption(bedrock_lib::storage::ErrorContext {
                        engine: "volume",
                        instance: name.to_owned(),
                        file: None,
                        partition: None,
                        detail: "an actor meta does not decode".into(),
                    })
                })?;
                known.insert(key[2..].to_vec(), meta);
            }
        }
        Ok(Some(Maintained { db, known }))
    }

    /// Flushes one batch of range `[start, end)` into `name` and truncates what it holds. Returns whether more actors
    /// wait than one batch took.
    async fn flush(&self, name: &str, start: u64, end: u64) -> Result<bool, Status> {
        let mut writers = self.writers.lock().await;
        if !writers.contains_key(name) {
            match self.open(name).await.map_err(storage)? {
                Some(maintained) => writers.insert(name.to_owned(), maintained),
                None => return Ok(false),
            };
        }
        let maintained = writers.get_mut(name).expect("just opened");
        let mut batch: Changes = Vec::new();
        let mut bytes = 0;
        let mut written = Vec::new();
        let mut truncate = Vec::new();
        let mut more = false;
        for actor in self.store.dirty(start, end) {
            if bytes >= FLUSH_BYTES {
                more = true;
                break;
            }
            if !self.placement.first_here(&actor).await? {
                continue;
            }
            let parsed = Actor::new(&actor).map_err(status)?;
            let store = self.store.clone();
            let pending = {
                let parsed = Actor::new(&actor).map_err(status)?;
                crate::serve::blocking(move || store.pending(&parsed))
                    .await?
                    .map_err(status)?
            };
            let Some(pending) = pending else { continue };
            let known = maintained
                .known
                .get(parsed.name())
                .copied()
                .unwrap_or_default();
            if pending.flushed > known.seq {
                // The slot dropped entries this database does not hold: never write over that gap.
                tracing::error!(database = %name, "a slot is flushed beyond its range database");
                continue;
            }
            if pending.through <= known.seq {
                truncate.push((actor, pending.through));
                continue;
            }
            bytes += pending.bytes();
            add(&mut batch, parsed.name(), &pending);
            written.push((
                parsed.name().to_vec(),
                meta(&pending),
                actor,
                pending.through,
            ));
        }
        if !batch.is_empty() {
            if let Err(error) = maintained.db.commit(batch).await {
                if matches!(error, StorageError::Fenced(_)) {
                    writers.remove(name);
                    self.fenced
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(name.to_owned(), bedrock_lib::monotonic_now() + FENCED_WAIT);
                }
                return Err(storage(error));
            }
            for (key, meta, actor, through) in written {
                maintained.known.insert(key, meta);
                truncate.push((actor, through));
            }
        }
        drop(writers);
        for (actor, through) in truncate {
            self.truncate(actor, through).await?;
        }
        Ok(more)
    }

    /// Truncates the actor's own slot here, and asks its other slots to, through `through`.
    async fn truncate(&self, actor: pb::Actor, through: u64) -> Result<(), Status> {
        let parsed = Actor::new(&actor).map_err(status)?;
        let store = self.store.clone();
        crate::serve::blocking(move || store.truncate(&parsed, through))
            .await?
            .map_err(status)?;
        let prefix = Actor::new(&actor).map_err(status)?.name().to_vec();
        self.owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(prefix, (actor, through));
        Ok(())
    }

    /// Retries the truncations other slots still owe.
    async fn settle(&self) {
        let owed: Vec<_> = self
            .owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (prefix, (actor, through)) in owed {
            match self.placement.truncate_others(&actor, through).await {
                Ok(true) => {
                    let mut owed = self.owed.lock().unwrap_or_else(PoisonError::into_inner);
                    // A newer flush may have raised it meanwhile.
                    if owed.get(&prefix).is_some_and(|(_, t)| *t == through) {
                        owed.remove(&prefix);
                    }
                }
                Ok(false) => {}
                Err(error) => tracing::debug!(%error, "a slot truncation is still owed"),
            }
        }
    }

    /// The database handle for `actor`'s reads: this node's writer when it maintains it, a reader otherwise.
    async fn handle(
        &self,
        cell: &str,
        actor: &pb::Actor,
    ) -> Result<(String, Arc<dyn Database>), Status> {
        let name = self.placement.database_of(actor).await?;
        if let Some(maintained) = self.writers.lock().await.get(&name) {
            return Ok((name, maintained.db.clone() as Arc<dyn Database>));
        }
        match self.readers.get(cell, &name).await.map_err(storage)? {
            Some(db) => Ok((name, db)),
            None => Err(Status::unavailable("the range database is not created yet")),
        }
    }

    /// Waits up to [`BEHIND_WAIT`] for `db` to hold the actor at `flushed` or later.
    async fn caught_up(&self, db: &dyn Database, actor: &[u8], flushed: u64) -> Result<(), Status> {
        let deadline = bedrock_lib::monotonic_now() + BEHIND_WAIT;
        loop {
            let seq = match db.get(&meta_key(actor)).await.map_err(storage)? {
                Some(bytes) => {
                    Meta::decode(&bytes)
                        .ok_or_else(|| Status::data_loss("an actor meta does not decode"))?
                        .seq
                }
                None => 0,
            };
            if seq >= flushed {
                return Ok(());
            }
            if bedrock_lib::monotonic_now() >= deadline {
                return Err(Status::unavailable(
                    "the range database reader is behind this slot",
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Values of `keys` from the range database, at the slot's flushed point or later, for a caller in `cell`.
    pub async fn get(
        &self,
        cell: &str,
        actor: &pb::Actor,
        flushed: u64,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, Status> {
        let parsed = Actor::new(actor).map_err(status)?;
        let (name, db) = self.handle(cell, actor).await?;
        let read = async {
            self.caught_up(db.as_ref(), parsed.name(), flushed).await?;
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(
                    db.get(&data_key(parsed.name(), key))
                        .await
                        .map_err(storage)?,
                );
            }
            Ok(values)
        };
        let result = read.await;
        if result.is_err() {
            self.readers.discard(&name, &db);
        }
        result
    }

    /// Up to `limit` live entries of the actor from `start` in the range database, and whether more follow.
    pub async fn scan(
        &self,
        cell: &str,
        actor: &pb::Actor,
        flushed: u64,
        start: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool), Status> {
        let parsed = Actor::new(actor).map_err(status)?;
        let (name, db) = self.handle(cell, actor).await?;
        let prefix = data_key(parsed.name(), b"");
        let read = async {
            self.caught_up(db.as_ref(), parsed.name(), flushed).await?;
            let end = KeyRange::prefix(prefix.clone())
                .end
                .expect("an actor prefix ends in NUL, so it has an end");
            let page = db
                .scan(&data_key(parsed.name(), start), &end, limit + 1)
                .await
                .map_err(storage)?;
            let more = page.len() > limit;
            Ok((
                page.into_iter()
                    .take(limit)
                    .map(|(k, v)| (k[prefix.len()..].to_vec(), v))
                    .collect(),
                more,
            ))
        };
        let result = read.await;
        if result.is_err() {
            self.readers.discard(&name, &db);
        }
        result
    }
}

/// Merges a page of overlay records (deletes included) over a page of range database entries, both from the same start:
/// the overlay wins. Only keys up to the end of a page with more after it are certain, so the answer stops there.
/// Returns up to `limit` live values, whether more follow, and where the next page starts.
pub fn merge(
    overlay: Vec<pb::Value>,
    overlay_more: bool,
    stored: Vec<(Vec<u8>, Vec<u8>)>,
    stored_more: bool,
    limit: usize,
) -> (Vec<pb::Value>, bool, Vec<u8>) {
    let cutoff = [
        overlay
            .last()
            .filter(|_| overlay_more)
            .map(|v| v.key.clone()),
        stored
            .last()
            .filter(|_| stored_more)
            .map(|(k, _)| k.clone()),
    ]
    .into_iter()
    .flatten()
    .min();
    let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
        stored.into_iter().map(|(k, v)| (k, Some(v))).collect();
    merged.extend(overlay.into_iter().map(|v| (v.key, v.value)));
    let mut more = cutoff.is_some();
    let mut values = Vec::new();
    for (key, value) in merged {
        if cutoff.as_ref().is_some_and(|c| key > *c) {
            break;
        }
        let Some(value) = value else { continue };
        if values.len() == limit {
            more = true;
            break;
        }
        values.push(pb::Value {
            key,
            value: Some(value),
        });
    }
    let next = cutoff.map_or_else(Vec::new, |c| [c.as_slice(), &[0]].concat());
    (values, more, next)
}

fn meta(pending: &Pending) -> Meta {
    Meta {
        seq: pending.through,
        epoch: pending.epoch,
        alarm: pending.alarm,
    }
}

fn add(batch: &mut Changes, actor: &[u8], pending: &Pending) {
    for (key, value) in &pending.changes {
        batch.push((data_key(actor, key), value.clone()));
    }
    batch.push((meta_key(actor), Some(meta(pending).encode())));
}

#[cfg(test)]
#[path = "../../tests/unit/cold.rs"]
mod tests;
