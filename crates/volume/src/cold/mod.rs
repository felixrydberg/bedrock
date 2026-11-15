//! The cold tier (#113 / S-05, `statefull/v2.md` §5 Cold tier).
//!
//! Each `volume` range of each cell has one range database ([`db`]) holding the actors in that range whose first slot is
//! in that cell. Its maintainer is the first owner of the range that is not populating: it opens the database as
//! writer, which fences any other, and every [`FLUSH_INTERVAL`] (sooner after [`FLUSH_BYTES`]) writes every changed
//! actor's keys up to its commit point in one batch with each actor's `meta`. Once that batch is durable, the actor's
//! slots truncate their log and overlay through it. Other slots read the database through [`readers`].
//!
//! Range database keys are `{actor}d{key}` for values and `{actor}m` for the actor's [`Meta`], where `{actor}` is
//! `{hash}{namespace}\0{id}\0` as on the slots. Keys start with the hash, so a hash span is one key range, which a
//! clone can take (SlateDB takes one range per source). A writer reads an actor's meta the first time it flushes it and
//! keeps it.
//!
//! - **Split and merge.** The cell's [`db::Catalog`] names the database serving each span. A maintainer whose range has
//!   no database of its own fences and retires each database serving part of it, clones a new one from them, and
//!   commits it by replacing the catalog conditionally. A retired database is never written again, so the clone holds
//!   everything flushed to it; of concurrent clones only the one that replaced the catalog is ever used.
//! - **First-slot moves.** When a topology change moves an actor's first slot to this cell, the actor's slot is flushed
//!   beyond what this cell's database holds. The maintainer then copies the actor's keys from the cell database that
//!   holds it at the slot's flushed point or later, in the same batch as its pending changes.
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
use bedrock_lib::storage::{Entry, KeyRange, StorageError};
use bedrock_lib::volume as pb;
use tokio::time::Instant;
use tonic::Status;

use crate::store::{Actor, Error, Pending, Store};
use db::{Catalog, Database, Databases, Generation, RETIRED, Writer, database};
use readers::Readers;

/// How often a range flushes.
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// Unflushed log bytes on the node that start a flush before the interval ends, and the most one batch carries.
pub const FLUSH_BYTES: u64 = 4 << 20;
/// Batches one range flushes per round at most; what is left stays dirty for the next round, so a busy range cannot
/// hold up the others, pending truncations or reader sweeps.
pub const FLUSH_ROUNDS: usize = 8;
/// How long a fenced maintainer waits before opening the database again.
pub const FENCED_WAIT: Duration = Duration::from_secs(5);
/// How long a cold read waits for its reader to see the slot's flushed point.
pub const BEHIND_WAIT: Duration = Duration::from_secs(2);
/// How often one open tries again after a concurrent catalog change.
const OPEN_ATTEMPTS: usize = 3;
/// How often a first-slot move rereads a source whose actor changed during the copy.
const COPY_ATTEMPTS: usize = 3;

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
    [actor, b"d", key].concat()
}

pub fn meta_key(actor: &[u8]) -> Vec<u8> {
    [actor, b"m"].concat()
}

/// What opening the database of span `[start, end)` takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// The span has its own database.
    Open(Generation),
    /// A new database `db`, cloned from `sources` (each a database and the part of the span it serves; none for a new
    /// span), committed by storing `catalog`.
    Create {
        catalog: Catalog,
        db: Generation,
        sources: Vec<(Generation, u64, u64)>,
    },
}

/// A span end as a number, 0 meaning 2⁶⁴.
fn top(end: u64) -> u128 {
    if end == 0 { 1 << 64 } else { end.into() }
}

/// How to open span `[start, end)` given `catalog`. `retired` means its own database was found retired, so it gets a
/// new one even though the catalog still names it.
pub fn plan(catalog: &Catalog, start: u64, end: u64, retired: bool) -> Plan {
    let overlapping: Vec<(u64, u64, Generation)> = catalog
        .spans
        .iter()
        .filter(|(s, (e, _))| u128::from(**s) < top(end) && top(*e) > u128::from(start))
        .map(|(s, (e, db))| (*s, *e, *db))
        .collect();
    if let [(s, e, db)] = overlapping[..]
        && !retired
        && (s, e, db.start, db.end) == (start, end, start, end)
    {
        return Plan::Open(db);
    }
    let db = Generation {
        start,
        end,
        generation: catalog.next,
    };
    let mut next = catalog.clone();
    next.next += 1;
    let mut sources = Vec::new();
    for (s, e, served) in overlapping {
        next.spans.remove(&s);
        // What lies outside the new span keeps reading the old database until its own range clones it.
        if s < start {
            next.spans.insert(s, (start, served));
        }
        if top(e) > top(end) {
            next.spans.insert(end, (e, served));
        }
        let to = if top(e) < top(end) { e } else { end };
        sources.push((served, s.max(start), to));
    }
    next.spans.insert(start, (end, db));
    Plan::Create {
        catalog: next,
        db,
        sources,
    }
}

/// The database of `catalog` serving `hash`.
fn serving(catalog: &Catalog, hash: u64) -> Option<Generation> {
    let (_, (end, db)) = catalog.spans.range(..=hash).next_back()?;
    (u128::from(hash) < top(*end)).then_some(*db)
}

async fn close(name: &str, db: &dyn Database) {
    if let Err(error) = db.close().await {
        tracing::debug!(%error, database = %name, "closing a range database handle failed");
    }
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
    /// The cell of the actor's first slot, whose range databases hold it.
    async fn first_cell(&self, actor: &pb::Actor) -> Result<String, Status>;
}

/// An open writer and the flushed point of every actor it has flushed or read. `known` is locked for a whole flush,
/// which serializes the flushes of one database without holding up other ranges or reads.
struct Maintained {
    name: String,
    db: Arc<dyn Writer>,
    known: tokio::sync::Mutex<BTreeMap<Vec<u8>, Meta>>,
}

pub struct Cold {
    store: Arc<Store>,
    databases: Arc<dyn Databases>,
    placement: Arc<dyn Placement>,
    pub readers: Readers,
    cell: String,
    node: String,
    /// Range databases this node writes, by span. Locked only to look one up, never across range I/O.
    writers: Mutex<BTreeMap<(u64, u64), Arc<Maintained>>>,
    /// When a fenced span may be opened again.
    fenced: Mutex<BTreeMap<(u64, u64), Instant>>,
    /// Catalogs cold reads and copies used, by cell; dropped when a read through one fails.
    catalogs: Mutex<BTreeMap<String, Arc<Catalog>>>,
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
            writers: Mutex::default(),
            fenced: Mutex::default(),
            catalogs: Mutex::default(),
            mark: Mutex::new(0),
        }
    }

    /// Whether enough was appended since the last flush to flush before the interval ends.
    pub fn due(&self) -> bool {
        self.store.unflushed() - *self.mark.lock().unwrap_or_else(PoisonError::into_inner)
            >= FLUSH_BYTES as i64
    }

    fn writer(&self, span: (u64, u64)) -> Option<Arc<Maintained>> {
        self.writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&span)
            .cloned()
    }

    /// Bytes of the range database this node writes for `[start, end)`, from its manifest; 0 when it writes none. It
    /// never waits for a flush.
    pub fn size(&self, start: u64, end: u64) -> u64 {
        self.writer((start, end)).map_or(0, |m| m.db.size())
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
        let stale: Vec<Arc<Maintained>> = {
            let mut writers = self.writers.lock().unwrap_or_else(PoisonError::into_inner);
            let gone: Vec<(u64, u64)> = writers
                .keys()
                .filter(|span| !ranges.contains(span))
                .copied()
                .collect();
            gone.iter()
                .filter_map(|span| writers.remove(span))
                .collect()
        };
        for maintained in stale {
            close(&maintained.name, maintained.db.as_ref()).await;
        }
        for (start, end) in ranges {
            for _ in 0..FLUSH_ROUNDS {
                match self.flush(start, end).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(error) => {
                        tracing::warn!(%error, start, end, "range flush failed");
                        break;
                    }
                }
            }
        }
        self.settle().await;
        self.readers.sweep().await;
    }

    /// Opens the database of span `[start, end)` as writer, unless it was fenced less than [`FENCED_WAIT`] ago. When
    /// the span has no database of its own, creates one from the databases serving it now (a split or merge).
    async fn open(&self, start: u64, end: u64) -> Result<Option<Maintained>, StorageError> {
        let now = bedrock_lib::monotonic_now();
        if self
            .fenced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(start, end))
            .is_some_and(|until| now < *until)
        {
            return Ok(None);
        }
        let mut retired = false;
        for _ in 0..OPEN_ATTEMPTS {
            let (catalog, version) = self.databases.catalog(&self.cell).await?;
            let db = match plan(&catalog, start, end, retired) {
                Plan::Open(db) => db,
                Plan::Create {
                    catalog,
                    db,
                    sources,
                } => {
                    let name = database(&self.cell, &db);
                    self.retire(&name, &sources).await?;
                    if !sources.is_empty() {
                        let sources: Vec<_> = sources
                            .iter()
                            .map(|(source, s, e)| (database(&self.cell, source), *s, *e))
                            .collect();
                        self.databases.create_clone(&name, &sources).await?;
                    }
                    if !self
                        .databases
                        .put_catalog(&self.cell, &catalog, version)
                        .await?
                    {
                        // Another change won; this clone is never written.
                        continue;
                    }
                    db
                }
            };
            let name = database(&self.cell, &db);
            let writer = self.databases.open(&name).await?;
            if writer.get(RETIRED).await?.is_some() {
                // A clone that took it never replaced the catalog: take it over with a clone of its own.
                close(&name, writer.as_ref()).await;
                retired = true;
                continue;
            }
            return Ok(Some(Maintained {
                name,
                db: writer,
                known: tokio::sync::Mutex::default(),
            }));
        }
        Err(StorageError::Unavailable(
            bedrock_lib::storage::ErrorContext {
                engine: "volume",
                instance: format!("{}/{start:016x}-{end:016x}", self.cell),
                file: None,
                partition: None,
                detail: "the catalog kept changing while opening".into(),
            },
        ))
    }

    /// Fences every source and marks it retired, so nothing written to it after this point can be missing from a clone:
    /// a writer that opened before the mark makes the mark fail, and one that opens after it finds it.
    async fn retire(
        &self,
        clone: &str,
        sources: &[(Generation, u64, u64)],
    ) -> Result<(), StorageError> {
        for (source, _, _) in sources {
            let name = database(&self.cell, source);
            let writer = self.databases.open(&name).await?;
            let marked = writer
                .commit(vec![(RETIRED.to_vec(), Some(clone.as_bytes().to_vec()))])
                .await;
            close(&name, writer.as_ref()).await;
            marked?;
        }
        Ok(())
    }

    /// The actor's flushed point in `maintained`'s database, read once.
    async fn known(
        db: &dyn Writer,
        known: &mut BTreeMap<Vec<u8>, Meta>,
        actor: &[u8],
    ) -> Result<Meta, Status> {
        if let Some(meta) = known.get(actor) {
            return Ok(*meta);
        }
        let meta = match db.get(&meta_key(actor)).await.map_err(storage)? {
            Some(bytes) => Meta::decode(&bytes)
                .ok_or_else(|| Status::data_loss("an actor meta does not decode"))?,
            None => Meta::default(),
        };
        known.insert(actor.to_vec(), meta);
        Ok(meta)
    }

    /// The database of `cell` serving `hash`, from the catalog this node last read for it.
    async fn serving(&self, cell: &str, hash: u64) -> Result<Option<String>, StorageError> {
        let cached = self
            .catalogs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cell)
            .cloned();
        let catalog = match cached {
            Some(catalog) => catalog,
            None => {
                let catalog = Arc::new(self.databases.catalog(cell).await?.0);
                self.catalogs
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(cell.to_owned(), catalog.clone());
                catalog
            }
        };
        Ok(serving(&catalog, hash).map(|db| database(cell, &db)))
    }

    /// Forgets `cell`'s catalog and `name`'s reader after a read through them failed.
    fn forget(&self, cell: &str, name: &str, db: &Arc<dyn Database>) {
        self.catalogs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(cell);
        self.readers.discard(name, db);
    }

    /// The actor's keys and meta from the database of another cell that holds it at `flushed` or later, as changes that
    /// make `target`'s keys of the actor equal to them. `None` when no cell holds it that far.
    async fn adopt(
        &self,
        target: &dyn Writer,
        actor: &Actor,
        flushed: u64,
    ) -> Result<Option<(Meta, Changes)>, Status> {
        let mut best: Option<(Meta, Vec<Entry>)> = None;
        for cell in self.databases.cells().await.map_err(storage)? {
            if cell == self.cell {
                continue;
            }
            let Some(name) = self.serving(&cell, actor.hash()).await.map_err(storage)? else {
                continue;
            };
            let Some(db) = self.readers.get(&self.cell, &name).await.map_err(storage)? else {
                continue;
            };
            let copied = copy(db.as_ref(), actor.name()).await;
            match copied {
                Ok(Some((meta, keys)))
                    if meta.seq >= flushed
                        && best.as_ref().is_none_or(|(b, _)| meta.seq > b.seq) =>
                {
                    best = Some((meta, keys));
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::debug!(%error, database = %name, "reading a moved actor failed");
                    self.forget(&cell, &name, &db);
                }
            }
        }
        let Some((meta, keys)) = best else {
            // A catalog read before the move may name a database the actor has left: read them again next time.
            self.catalogs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clear();
            return Ok(None);
        };
        // Start from deletes of what the target holds, so keys the source no longer has go.
        let mut changes: BTreeMap<Vec<u8>, Option<Vec<u8>>> = all(target, actor.name())
            .await
            .map_err(storage)?
            .into_iter()
            .map(|(key, _)| (key, None))
            .collect();
        changes.extend(keys.into_iter().map(|(key, value)| (key, Some(value))));
        changes.insert(meta_key(actor.name()), Some(meta.encode()));
        Ok(Some((meta, changes.into_iter().collect())))
    }

    /// Flushes one batch of range `[start, end)` into `name` and truncates what it holds. Returns whether more actors
    /// wait than one batch took.
    async fn flush(&self, start: u64, end: u64) -> Result<bool, Status> {
        let span = (start, end);
        // Only the flush loop opens writers, so no other open of the span races this one.
        let maintained = match self.writer(span) {
            Some(maintained) => maintained,
            None => match self.open(start, end).await.map_err(storage)? {
                Some(opened) => {
                    let opened = Arc::new(opened);
                    self.writers
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(span, opened.clone());
                    opened
                }
                None => return Ok(false),
            },
        };
        let mut known_points = maintained.known.lock().await;
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
            let mut known =
                Self::known(maintained.db.as_ref(), &mut known_points, parsed.name()).await?;
            if pending.flushed > known.seq {
                // Flushed in another cell's database: its first slot moved here. Copy it from there first.
                match self
                    .adopt(maintained.db.as_ref(), &parsed, pending.flushed)
                    .await?
                {
                    Some((meta, copied)) => {
                        bytes += copied
                            .iter()
                            .map(|(k, v)| (k.len() + v.as_ref().map_or(0, Vec::len)) as u64)
                            .sum::<u64>();
                        batch.extend(copied);
                        known = meta;
                        written.push((
                            parsed.name().to_vec(),
                            meta,
                            actor.clone(),
                            meta.seq.min(pending.through),
                        ));
                    }
                    None => {
                        // The slot dropped entries no database holds: never write over that gap.
                        tracing::error!(database = %maintained.name, "a slot is flushed beyond its range database");
                        continue;
                    }
                }
            }
            if pending.through <= known.seq {
                truncate.push((actor, pending.through));
                continue;
            }
            bytes += pending.bytes();
            // Later writes of a key in one batch win, so these go over a copy.
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
                    self.writers
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&span);
                    self.fenced
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(span, bedrock_lib::monotonic_now() + FENCED_WAIT);
                }
                return Err(storage(error));
            }
            for (key, meta, actor, through) in written {
                known_points.insert(key, meta);
                truncate.push((actor, through));
            }
            // An adopted actor is listed twice when its pending changes went over the copy: truncate once, the furthest.
            truncate.sort_by(|a, b| a.0.id.cmp(&b.0.id).then(b.1.cmp(&a.1)));
            truncate.dedup_by(|a, b| a.0 == b.0);
        }
        drop(known_points);
        for (actor, through) in truncate {
            self.truncate(actor, through).await?;
        }
        Ok(more)
    }

    /// Truncates the actor's own slot here and records that its other slots owe the same, which [`Cold::settle`] asks of
    /// them. The record is stored with the truncation, so a restart or a range handoff keeps it.
    async fn truncate(&self, actor: pb::Actor, through: u64) -> Result<(), Status> {
        let parsed = Actor::new(&actor).map_err(status)?;
        let store = self.store.clone();
        crate::serve::blocking(move || store.truncate_owing(&parsed, through))
            .await?
            .map_err(status)?;
        Ok(())
    }

    /// Retries the truncations other slots still owe; one done is settled unless a newer flush raised it meanwhile.
    async fn settle(&self) {
        for (actor, through) in self.store.owed() {
            match self.placement.truncate_others(&actor, through).await {
                Ok(true) => {
                    let Ok(parsed) = Actor::new(&actor) else {
                        continue;
                    };
                    let store = self.store.clone();
                    let settled =
                        crate::serve::blocking(move || store.settle(&parsed, through)).await;
                    if let Ok(Err(error)) = settled {
                        tracing::warn!(error = ?error, "settling a slot truncation failed");
                    }
                }
                Ok(false) => {}
                Err(error) => tracing::debug!(%error, "a slot truncation is still owed"),
            }
        }
    }

    /// The database handle for `actor`'s reads: this node's writer when it maintains it, a reader otherwise. Returns the
    /// first slot's cell too.
    async fn handle(
        &self,
        cell: &str,
        actor: &pb::Actor,
    ) -> Result<(String, String, Arc<dyn Database>), Status> {
        let first = self.placement.first_cell(actor).await?;
        let hash = Actor::new(actor).map_err(status)?.hash();
        if first == self.cell {
            let maintained = self
                .writers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .range(..=(hash, u64::MAX))
                .next_back()
                .filter(|((_, end), _)| u128::from(hash) < top(*end))
                .map(|(_, maintained)| maintained.clone());
            if let Some(maintained) = maintained {
                return Ok((
                    first,
                    maintained.name.clone(),
                    maintained.db.clone() as Arc<dyn Database>,
                ));
            }
        }
        let not_yet = || Status::unavailable("the range database is not created yet");
        let name = self
            .serving(&first, hash)
            .await
            .map_err(storage)?
            .ok_or_else(not_yet)?;
        match self.readers.get(cell, &name).await.map_err(storage)? {
            Some(db) => Ok((first, name, db)),
            None => Err(not_yet()),
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
        let (first, name, db) = self.handle(cell, actor).await?;
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
            self.forget(&first, &name, &db);
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
        let (first, name, db) = self.handle(cell, actor).await?;
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
            self.forget(&first, &name, &db);
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
    // The last key consumed, deletes included: where the next page starts when the limit stops this one.
    let mut consumed = None;
    for (key, value) in merged {
        if cutoff.as_ref().is_some_and(|c| key > *c) {
            break;
        }
        if value.is_some() && values.len() == limit {
            more = true;
            break;
        }
        consumed = Some(key.clone());
        if let Some(value) = value {
            values.push(pb::Value {
                key,
                value: Some(value),
            });
        }
    }
    let after = |key: Vec<u8>| [key.as_slice(), &[0]].concat();
    let next = if values.len() == limit && more {
        consumed.map_or_else(Vec::new, after)
    } else {
        cutoff.map_or_else(Vec::new, after)
    };
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

/// Every key of `actor` in `db`.
async fn all(db: &dyn Database, actor: &[u8]) -> Result<Vec<Entry>, StorageError> {
    let end = KeyRange::prefix(actor.to_vec())
        .end
        .expect("an actor prefix ends in NUL, so it has an end");
    let mut from = actor.to_vec();
    let mut entries = Vec::new();
    loop {
        let page = db.scan(&from, &end, 1000).await?;
        let Some((last, _)) = page.last() else {
            return Ok(entries);
        };
        from = [last.as_slice(), &[0]].concat();
        entries.extend(page);
    }
}

/// The actor's meta and other keys in `db`, as of one flush: the meta read before and after the keys agree, and every
/// flush of the actor changes it.
async fn copy(db: &dyn Database, actor: &[u8]) -> Result<Option<(Meta, Vec<Entry>)>, StorageError> {
    let meta = meta_key(actor);
    for _ in 0..COPY_ATTEMPTS {
        let Some(before) = db.get(&meta).await? else {
            return Ok(None);
        };
        let keys = all(db, actor).await?;
        if db.get(&meta).await?.as_ref() == Some(&before) {
            let decoded = Meta::decode(&before).ok_or_else(|| {
                StorageError::Corruption(bedrock_lib::storage::ErrorContext {
                    engine: "volume",
                    instance: String::new(),
                    file: None,
                    partition: None,
                    detail: "an actor meta does not decode".into(),
                })
            })?;
            return Ok(Some((
                decoded,
                keys.into_iter().filter(|(k, _)| *k != meta).collect(),
            )));
        }
    }
    Err(StorageError::Unavailable(
        bedrock_lib::storage::ErrorContext {
            engine: "volume",
            instance: String::new(),
            file: None,
            partition: None,
            detail: "the actor kept changing while it was copied".into(),
        },
    ))
}

#[cfg(test)]
#[path = "../../tests/unit/cold.rs"]
mod tests;
