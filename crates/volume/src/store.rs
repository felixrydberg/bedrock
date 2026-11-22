//! Slot records and their rules (#111 / S-03, `statefull/v2.md` §5, `docs/spec/volume.tla`).
//!
//! Records live in a [`StorageEngine`]: RocksDB, or the in-memory engine in simulation builds. Every actor key starts
//! with `s/` and the actor's big-endian hash, so a hash range is one scan and dropping it one `delete_range`:
//! `s/{hash}{namespace}\0{id}\0` followed by `claim`, `log/{seq}`, `data/{key}` (the overlay) or `owed` (a flush's
//! truncation the actor's other slots have not confirmed, kept by its maintainer). Released actors with an
//! alarm are also indexed by time, `alarm/{when}{hash}{namespace}\0{id}`, removed with the claim's release, and
//! tombstones of deleted actors (S-05b) by their `DELETE`'s time under `gone/`, removed with the tombstone.
//!
//! Each request is one atomic batch: log entries, overlay changes and the claim together. The engine syncs every batch
//! before it returns, and RocksDB group-commits concurrent batches into one fsync, so an answer always follows its fsync.
//!
//! The rules are the model's: [`Store::reserve`] is `Reserve`, [`Store::claim`] is `Grant` and [`Store::append`] is
//! `Replicate`. A `repaired` slot takes claims and appends, but its answers say it does not vote; it votes again once an
//! append shows that a claim above its reservation has committed.
//!
//! Range marks (#114 / S-06, `crate::repair`) sit outside the actor keys, each one record of hash spans: `m/held` (spans
//! this node holds a complete copy of), `m/sealed` (spans a range handoff sealed: no reservation, claim or append),
//! `m/repaired` (spans where an actor with no claim record here starts `repaired`, with floor 0) and `m/fresh` (the
//! store started empty while this node owned no range, so a range it is first given `ready` has nothing to copy).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bedrock_lib::actors::ActorId;
use bedrock_lib::replication::pb::{RangeSize, Span};
use bedrock_lib::storage::{
    ErrorContext, KeyRange, StorageEngine, StorageError, StorageResult, WriteBatch,
};
use bedrock_lib::volume as pb;
use pb::Refusal;
use pb::entry::Kind as EntryKind;
use prost::Message;

/// Striped locks: each actor's read-modify-write is serialized, and actors in other stripes write concurrently.
// ponytail: 64 stripes; per-actor locks if stripes contend.
const STRIPES: u64 = 64;
/// The most entries one `Scan` or `Repair` answer carries.
pub const MAX_LIMIT: u32 = 1000;

/// Log entries an actor may hold beyond its flushed point before its appends are refused (S-05 backpressure). It keeps
/// one actor whose range database cannot flush from filling the node.
pub const MAX_UNFLUSHED_ENTRIES: u64 = 100_000;
/// Log bytes the node may hold beyond the flushed points before every append is refused.
pub const MAX_UNFLUSHED_BYTES: u64 = 16 << 30;

#[derive(Debug)]
pub enum Error {
    Storage(StorageError),
    Invalid(String),
    /// Too much is not yet flushed to S3; the holder retries later. Nothing was written.
    Backpressure,
    /// A range handoff sealed the actor's range here (S-06); its new owner serves it once ready.
    Sealed,
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        Error::Storage(error)
    }
}

fn invalid<T>(detail: impl Into<String>) -> Result<T, Error> {
    Err(Error::Invalid(detail.into()))
}

fn corrupt(what: &str, error: prost::DecodeError) -> Error {
    Error::Storage(StorageError::Corruption(ErrorContext {
        engine: "volume",
        instance: "volume".into(),
        file: None,
        partition: None,
        detail: format!("{what} does not decode: {error}"),
    }))
}

fn missing(seq: u64) -> Error {
    Error::Storage(StorageError::Corruption(ErrorContext {
        engine: "volume",
        instance: "volume".into(),
        file: None,
        partition: Some(format!("log {seq}")),
        detail: "an entry below the log end is missing".into(),
    }))
}

/// An actor's key prefix, hash and metrics kind.
pub struct Actor {
    hash: u64,
    prefix: Vec<u8>,
    kind: i32,
}

impl Actor {
    /// Checks the ID's format (not its region, which the holder checked) and the namespace.
    pub fn new(actor: &pb::Actor) -> Result<Self, Error> {
        let hash = ActorId::hash_of(&actor.id).map_err(|e| Error::Invalid(e.to_string()))?;
        if actor.namespace.is_empty() || actor.namespace.contains('\0') {
            return invalid("an actor namespace is not empty and has no NUL");
        }
        let mut prefix = Vec::with_capacity(12 + actor.namespace.len() + actor.id.len());
        prefix.extend_from_slice(b"s/");
        prefix.extend_from_slice(&hash.to_be_bytes());
        for part in [&actor.namespace, &actor.id] {
            prefix.extend_from_slice(part.as_bytes());
            prefix.push(0);
        }
        Ok(Actor {
            hash,
            prefix,
            kind: actor.kind,
        })
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn kind(&self) -> i32 {
        self.kind
    }

    /// `{hash}{namespace}\0{id}\0`: the actor's key prefix without the slot store's `s/`, as the range database keys it.
    pub fn name(&self) -> &[u8] {
        &self.prefix[2..]
    }

    /// The actor whose slot-store prefix `prefix` is, back from its bytes.
    pub fn parse(prefix: &[u8], kind: i32) -> Option<pb::Actor> {
        let rest = prefix.strip_prefix(b"s/")?.get(8..)?;
        let mut parts = rest.split(|b| *b == 0);
        let namespace = String::from_utf8(parts.next()?.to_vec()).ok()?;
        let id = String::from_utf8(parts.next()?.to_vec()).ok()?;
        Some(pb::Actor {
            namespace,
            id,
            kind,
        })
    }

    fn key(&self, suffix: &[u8]) -> Vec<u8> {
        [self.prefix.as_slice(), suffix].concat()
    }
}

pub fn claim_key(actor: &Actor) -> Vec<u8> {
    actor.key(b"claim")
}

fn log_key(actor: &Actor, seq: u64) -> Vec<u8> {
    actor.key(&[b"log/".as_slice(), &seq.to_be_bytes()].concat())
}

fn data_key(actor: &Actor, key: &[u8]) -> Vec<u8> {
    actor.key(&[b"data/".as_slice(), key].concat())
}

fn owed_key(actor: &Actor) -> Vec<u8> {
    actor.key(b"owed")
}

/// The alarm index entry of a released actor with an alarm.
fn alarm_key(actor: &Actor, claim: &pb::ClaimRecord) -> Option<Vec<u8>> {
    let at = claim.alarm_ms.filter(|_| claim.released)?;
    Some([b"alarm/".as_slice(), &at.to_be_bytes(), &actor.prefix[2..]].concat())
}

/// The tombstone index entry of a deleted actor (S-05b).
fn gone_key(actor: &Actor, claim: &pb::ClaimRecord) -> Option<Vec<u8>> {
    claim.deleted?;
    Some(
        [
            b"gone/".as_slice(),
            &claim.deleted_at_ms.to_be_bytes(),
            &actor.prefix[2..],
        ]
        .concat(),
    )
}

/// Every actor key whose hash lies in `[start, end)`, `end` 0 meaning 2⁶⁴.
pub fn span(start: u64, end: u64) -> KeyRange {
    let at = |hash: u64| [b"s/".as_slice(), &hash.to_be_bytes()].concat();
    match end {
        0 => KeyRange {
            start: at(start),
            end: KeyRange::prefix(*b"s/").end,
        },
        end => KeyRange::new(at(start), at(end)),
    }
}

/// Hash spans `[start, end)`, each with a generation (0 where it has none): start → (end, generation), with 2⁶⁴ as the
/// end of the hash space. Touching spans of one generation coalesce.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Spans(BTreeMap<u64, (u128, u64)>);

fn end_of(end: u64) -> u128 {
    if end == 0 { 1 << 64 } else { u128::from(end) }
}

impl Spans {
    pub fn contains(&self, hash: u64) -> bool {
        self.0
            .range(..=hash)
            .next_back()
            .is_some_and(|(_, (end, _))| u128::from(hash) < *end)
    }

    /// The generation of the one span that covers `[start, end)` (`end` 0 meaning 2⁶⁴).
    pub fn generation(&self, start: u64, end: u64) -> Option<u64> {
        self.0
            .range(..=start)
            .next_back()
            .filter(|(_, (e, _))| end_of(end) <= *e)
            .map(|(_, (_, generation))| *generation)
    }

    /// Whether `[start, end)` lies inside one span.
    pub fn covers(&self, start: u64, end: u64) -> bool {
        self.generation(start, end).is_some()
    }

    pub fn overlaps(&self, start: u64, end: u64) -> bool {
        self.0
            .iter()
            .any(|(s, (e, _))| u128::from(*s) < end_of(end) && *e > u128::from(start))
    }

    pub fn add(&mut self, start: u64, end: u64) {
        self.add_generation(start, end, 0);
    }

    /// Adds `[start, end)` for `generation`, replacing what it overlaps.
    pub fn add_generation(&mut self, start: u64, end: u64, generation: u64) {
        self.remove(start, end);
        let (mut from, mut to) = (u128::from(start), end_of(end));
        let touching: Vec<u64> = self
            .0
            .iter()
            .filter(|(s, (e, g))| *g == generation && (u128::from(**s) == to || *e == from))
            .map(|(s, _)| *s)
            .collect();
        for s in touching {
            let (e, _) = self.0.remove(&s).unwrap();
            from = from.min(u128::from(s));
            to = to.max(e);
        }
        self.0.insert(from as u64, (to, generation));
    }

    pub fn remove(&mut self, start: u64, end: u64) {
        let (from, to) = (u128::from(start), end_of(end));
        let cut: Vec<(u64, (u128, u64))> = self
            .0
            .iter()
            .filter(|(s, (e, _))| u128::from(**s) < to && *e > from)
            .map(|(s, e)| (*s, *e))
            .collect();
        for (s, (e, g)) in cut {
            self.0.remove(&s);
            if u128::from(s) < from {
                self.0.insert(s, (from, g));
            }
            if e > to {
                self.0.insert(to as u64, (e, g));
            }
        }
    }

    /// The spans as `(start, end)`, `end` 0 meaning 2⁶⁴.
    pub fn list(&self) -> Vec<(u64, u64)> {
        self.entries().into_iter().map(|(s, e, _)| (s, e)).collect()
    }

    /// The spans as `(start, end, generation)`.
    pub fn entries(&self) -> Vec<(u64, u64, u64)> {
        self.0
            .iter()
            .map(|(s, (e, g))| (*s, if *e == 1 << 64 { 0 } else { *e as u64 }, *g))
            .collect()
    }

    /// The parts of these spans inside `[start, end)`.
    pub fn within(&self, start: u64, end: u64) -> Spans {
        let mut inside = self.clone();
        if start > 0 {
            inside.remove(0, start);
        }
        if end != 0 {
            inside.remove(end, 0);
        }
        inside
    }

    fn encode(&self) -> Vec<u8> {
        self.entries()
            .into_iter()
            .flat_map(|(s, e, g)| [s.to_be_bytes(), e.to_be_bytes(), g.to_be_bytes()].concat())
            .collect()
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (triples, rest) = bytes.as_chunks::<24>();
        if !rest.is_empty() {
            return Err(Error::Storage(StorageError::Corruption(ErrorContext {
                engine: "volume",
                instance: "volume".into(),
                file: None,
                partition: None,
                detail: "a range mark is not whole spans".into(),
            })));
        }
        let mut spans = Spans::default();
        for triple in triples {
            let word = |i: usize| u64::from_be_bytes(triple[i * 8..i * 8 + 8].try_into().unwrap());
            if word(1) != 0 && word(1) <= word(0) {
                return Err(Error::Storage(StorageError::Corruption(ErrorContext {
                    engine: "volume",
                    instance: "volume".into(),
                    file: None,
                    partition: None,
                    detail: "a range mark holds an empty span".into(),
                })));
            }
            spans.add_generation(word(0), word(1), word(2));
        }
        Ok(spans)
    }
}

/// The length of an actor key's prefix, `s/{hash}{namespace}\0{id}\0`, or `None` for another key.
pub fn prefix_len(key: &[u8]) -> Option<usize> {
    let rest = key.strip_prefix(b"s/")?.get(8..)?;
    let first = rest.iter().position(|b| *b == 0)?;
    let second = rest[first + 1..].iter().position(|b| *b == 0)?;
    Some(10 + first + second + 2)
}

fn spans_of(kept: &[(u64, u64, u64)]) -> Spans {
    let mut spans = Spans::default();
    for (start, end, generation) in kept {
        spans.add_generation(*start, *end, *generation);
    }
    spans
}

/// The parts of `held` inside `inside` with the same generation.
fn kept_of(held: &Spans, inside: &Spans) -> Spans {
    let mut stays = Spans::default();
    for (start, end, generation) in held.entries() {
        for (s, e, g) in inside.within(start, end).entries() {
            if g == generation {
                stays.add_generation(s, e, g);
            }
        }
    }
    stays
}

/// The range marks, as stored under `m/`.
#[derive(Default)]
struct Marks {
    held: Spans,
    sealed: Spans,
    repaired: Spans,
    initialized: bool,
    fresh: bool,
    /// Bumped whenever a held span, or one being copied, is forgotten: a copy that saw it change may be stale.
    forgotten: u64,
    /// Spans a copy is filling (in memory only: a copy cut short starts over).
    copying: Spans,
}

const HELD: &[u8] = b"m/held";
const SEALED: &[u8] = b"m/sealed";
const REPAIRED: &[u8] = b"m/repaired";
/// Present once the node has decided whether its store started fresh; holds 1 while it is fresh.
const FRESH: &[u8] = b"m/fresh";

/// What an append or truncation did besides its answer, for metrics.
pub struct Outcome {
    pub answer: pb::Answer,
    /// Change in overlay bytes (keys plus stored records).
    pub overlay: i64,
    /// How long the synced batch took, when one was written.
    pub fsync: Option<Duration>,
}

impl Outcome {
    fn new(answer: pb::Answer) -> Self {
        Outcome {
            answer,
            overlay: 0,
            fsync: None,
        }
    }
}

pub struct Store {
    engine: Arc<dyn StorageEngine>,
    locks: Vec<Mutex<()>>,
    /// Actors with committed entries not yet flushed, by slot-store prefix: the maintainer's flush candidates.
    dirty: Mutex<BTreeMap<Vec<u8>, pb::Actor>>,
    /// Log bytes beyond the flushed points, for backpressure.
    unflushed: AtomicI64,
    max_unflushed: u64,
    /// Truncations the actors' other slots owe after a flush, by slot-store prefix; stored as `owed` records too.
    owed: Mutex<BTreeMap<Vec<u8>, (pb::Actor, u64)>>,
    marks: Mutex<Marks>,
}

/// What a flush writes for one actor: its keys as of `through` (its commit point), rebuilt from the log.
#[derive(Debug, Clone, PartialEq)]
pub struct Pending {
    pub actor: pb::Actor,
    /// The slot's flushed point: the changes start after it.
    pub flushed: u64,
    pub through: u64,
    pub epoch: u64,
    pub alarm: Option<u64>,
    /// The log's first `MOVE`, when it is at or before `through` (S-17).
    pub moved: Option<pb::LogEnd>,
    /// The last write of each key in `(flushed, through]`, in key order; `None` deletes it.
    pub changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl Pending {
    pub fn bytes(&self) -> u64 {
        self.changes
            .iter()
            .map(|(k, v)| (k.len() + v.as_ref().map_or(0, Vec::len)) as u64)
            .sum()
    }
}

fn log_bytes(actor: &Actor, entry: &pb::Entry) -> i64 {
    (log_key(actor, entry.seq).len() + entry.encoded_len()) as i64
}

fn answer(refused: Refusal, claim: &pb::ClaimRecord, voting: bool) -> pb::Answer {
    pb::Answer {
        refused: refused as i32,
        slot: Some(pb::Slot {
            voting,
            reserved: claim.reserved,
            epoch: claim.epoch,
            holder: claim.holder.clone(),
            version: claim.version,
            end: claim.end,
            committed: claim.committed,
            flushed: claim.flushed,
            released: claim.released,
            flushed_epoch: claim.flushed_epoch,
            moved: claim.moved,
            flushed_alarm_ms: claim.flushed_alarm_ms,
            imported: claim.imported,
            deleted: claim.deleted,
        }),
    }
}

/// The `DELETED` refusal of a slot holding the actor's tombstone (S-05b).
fn buried(claim: &pb::ClaimRecord) -> Option<pb::Answer> {
    claim
        .deleted
        .is_some()
        .then(|| answer(Refusal::Deleted, claim, !claim.repaired))
}

/// Whether the claim knows its `DELETE` committed and is not a tombstone yet.
fn deletes(claim: &pb::ClaimRecord) -> bool {
    claim.deleted.is_none() && claim.deleting.is_some_and(|d| claim.committed >= d.seq)
}

fn end(claim: &pb::ClaimRecord) -> pb::LogEnd {
    claim.end.unwrap_or_default()
}

fn overlay_size(key: &[u8], record: Option<&pb::OverlayRecord>) -> i64 {
    record.map_or(0, |r| (key.len() + r.encoded_len()) as i64)
}

impl Store {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Store {
            engine,
            locks: (0..STRIPES).map(|_| Mutex::new(())).collect(),
            dirty: Mutex::default(),
            unflushed: AtomicI64::new(0),
            max_unflushed: MAX_UNFLUSHED_BYTES,
            owed: Mutex::default(),
            marks: Mutex::default(),
        }
    }

    fn marks(&self) -> MutexGuard<'_, Marks> {
        self.marks.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The spans this node holds a complete copy of.
    pub fn held(&self) -> Spans {
        self.marks().held.clone()
    }

    pub fn sealed(&self) -> Spans {
        self.marks().sealed.clone()
    }

    /// Spans where an actor with no claim record here starts `repaired`.
    pub fn repaired(&self) -> Spans {
        self.marks().repaired.clone()
    }

    /// Whether the node decided yet if its store started fresh, and whether it did.
    pub fn fresh(&self) -> Option<bool> {
        let marks = self.marks();
        marks.initialized.then_some(marks.fresh)
    }

    /// How often a held span was forgotten.
    pub fn forgotten(&self) -> u64 {
        self.marks().forgotten
    }

    /// Changes the marks durably, then in memory: `change` edits a copy, which is written in one batch.
    fn remark(&self, change: impl FnOnce(&mut Marks)) -> Result<(), Error> {
        let mut marks = self.marks();
        let mut next = Marks {
            held: marks.held.clone(),
            sealed: marks.sealed.clone(),
            repaired: marks.repaired.clone(),
            copying: marks.copying.clone(),
            ..*marks
        };
        change(&mut next);
        let mut batch = WriteBatch::default();
        let mut written = false;
        for (key, old, new) in [
            (HELD, &marks.held, &next.held),
            (SEALED, &marks.sealed, &next.sealed),
            (REPAIRED, &marks.repaired, &next.repaired),
        ] {
            if old != new {
                batch.put(key.to_vec(), new.encode());
                written = true;
            }
        }
        if (marks.initialized, marks.fresh) != (next.initialized, next.fresh) {
            batch.put(FRESH.to_vec(), vec![u8::from(next.fresh)]);
            written = true;
        }
        if written {
            self.commit(batch)?;
        }
        *marks = next;
        Ok(())
    }

    /// Records whether the store started fresh, once.
    pub fn initialize(&self, fresh: bool) -> Result<(), Error> {
        self.remark(|m| {
            m.initialized = true;
            m.fresh = fresh;
        })
    }

    /// Holds a complete copy of `[start, end)`, made for the assignment `generation`.
    pub fn hold(&self, start: u64, end: u64, generation: u64) -> Result<(), Error> {
        self.remark(|m| m.held.add_generation(start, end, generation))
    }

    /// Ends the copy of `[start, end)` that [`Store::start_copy`] began, and holds it for `generation` if it `copied` and
    /// no held span or copy was forgotten since `start_copy` returned `forgotten`: the copy may then be stale. Both happen
    /// under the marks lock, so a [`Store::keep`] either sees the copy being made or the span held. A copy it does not
    /// hold may have left part of a stale copy behind, so the store is no longer fresh. Returns whether it holds.
    pub fn finish_copy(
        &self,
        start: u64,
        end: u64,
        generation: u64,
        forgotten: u64,
        copied: bool,
    ) -> Result<bool, Error> {
        let mut held = false;
        self.remark(|m| {
            m.copying.remove(start, end);
            if copied && m.forgotten == forgotten {
                m.held.add_generation(start, end, generation);
                held = true;
            } else {
                m.fresh = false;
            }
        })?;
        Ok(held)
    }

    /// The held spans inside `kept` (a pushed range set, each span with this node's generation of it), held for that
    /// generation. Changes nothing.
    pub fn holding(&self, kept: &[(u64, u64, u64)]) -> Spans {
        kept_of(&self.marks().held, &spans_of(kept))
    }

    /// Forgets held spans outside `kept` (a pushed range set, each span with this node's generation of it) or held for
    /// another generation, and returns what stays held. A store that forgets one is
    /// no longer fresh: a range it is given `ready` again needs a repair, not trust. It reads and writes the marks
    /// under their lock, so a copy held meanwhile is judged against this set too.
    pub fn keep(&self, kept: &[(u64, u64, u64)]) -> Result<Spans, Error> {
        let inside = spans_of(kept);
        let mut stays = Spans::default();
        self.remark(|m| {
            stays = kept_of(&m.held, &inside);
            if m.copying
                .entries()
                .iter()
                .any(|(s, e, g)| inside.generation(*s, *e) != Some(*g))
            {
                m.forgotten += 1;
            }
            if stays != m.held {
                m.held = stays.clone();
                m.fresh = false;
                m.forgotten += 1;
            }
        })?;
        Ok(stays)
    }

    /// Notes that a copy is filling `[start, end)` for `generation`, and returns how often a held span was forgotten, read
    /// under the same lock: a pushed set without the span from now on makes the copy stale ([`Store::finish_copy`]).
    pub fn start_copy(&self, start: u64, end: u64, generation: u64) -> u64 {
        let mut marks = self.marks();
        marks.copying.add_generation(start, end, generation);
        marks.forgotten
    }

    /// Seals `[start, end)` for a range handoff: once this returns, no reservation, claim or append there is in flight or
    /// accepted later, so a copy made after it holds every promise this slot gave.
    pub fn seal(&self, start: u64, end: u64) -> Result<(), Error> {
        if !self.marks().sealed.covers(start, end) {
            self.remark(|m| m.sealed.add(start, end))?;
        }
        // Writes check the seal under their actor's stripe lock: taking each lock once waits out those already past it.
        // Every call waits, even one that finds the span sealed: a concurrent first seal may still be waiting.
        for lock in &self.locks {
            drop(lock.lock().unwrap_or_else(PoisonError::into_inner));
        }
        Ok(())
    }

    pub fn mark_repaired(&self, start: u64, end: u64) -> Result<(), Error> {
        self.remark(|m| m.repaired.add(start, end))
    }

    fn check_sealed(&self, actor: &Actor) -> Result<(), Error> {
        if self.marks().sealed.contains(actor.hash) {
            return Err(Error::Sealed);
        }
        Ok(())
    }

    /// Drops every record and mark of `[start, end)`, in one batch: a range this node no longer owns, or one it is about
    /// to copy again. It holds every stripe lock meanwhile, so no write admitted before it lands after the delete.
    pub fn clear(&self, start: u64, end: u64) -> Result<(), Error> {
        // ponytail: blocks every actor's writes for one clear; lock only the span's stripes if clears get frequent.
        let _stripes: Vec<_> = self
            .locks
            .iter()
            .map(|l| l.lock().unwrap_or_else(PoisonError::into_inner))
            .collect();
        let mut batch = WriteBatch::default();
        batch.delete_range(span(start, end));
        let (freed, _) = self.tally(start, end)?;
        // ponytail: scans the whole alarm and tombstone indexes; keep them per range if clears get frequent.
        for (index, at) in [(b"alarm/".as_slice(), 14), (b"gone/".as_slice(), 13)] {
            for item in self.engine.scan(&KeyRange::prefix(index.to_vec()))? {
                let (key, _) = item?;
                let hash = key
                    .get(at..at + 8)
                    .map(|h| u64::from_be_bytes(h.try_into().unwrap()));
                if hash.is_some_and(|h| h >= start && (end == 0 || h < end)) {
                    batch.delete(key);
                }
            }
        }
        let mut marks = self.marks();
        let mut next = Marks {
            held: marks.held.clone(),
            sealed: marks.sealed.clone(),
            repaired: marks.repaired.clone(),
            copying: marks.copying.clone(),
            ..*marks
        };
        next.held.remove(start, end);
        next.sealed.remove(start, end);
        next.repaired.remove(start, end);
        batch.put(HELD.to_vec(), next.held.encode());
        batch.put(SEALED.to_vec(), next.sealed.encode());
        batch.put(REPAIRED.to_vec(), next.repaired.encode());
        self.commit(batch)?;
        *marks = next;
        drop(marks);
        self.unflushed.fetch_sub(freed, Ordering::Relaxed);
        let outside = |prefix: &Vec<u8>| {
            let hash = u64::from_be_bytes(prefix[2..10].try_into().unwrap());
            hash < start || (end != 0 && hash >= end)
        };
        self.dirty
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|k, _| outside(k));
        self.owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|k, _| outside(k));
        Ok(())
    }

    /// The log bytes of `[start, end)` and its claims and owed truncations, by actor prefix.
    #[allow(clippy::type_complexity)]
    fn tally(
        &self,
        start: u64,
        end: u64,
    ) -> Result<(i64, Vec<(Vec<u8>, Option<pb::ClaimRecord>, Option<u64>)>), Error> {
        let mut logged = 0;
        let mut actors: Vec<(Vec<u8>, Option<pb::ClaimRecord>, Option<u64>)> = Vec::new();
        for item in self.engine.scan(&span(start, end))? {
            let (key, value) = item?;
            let Some(at) = prefix_len(&key) else {
                continue;
            };
            if actors.last().is_none_or(|(p, _, _)| p[..] != key[..at]) {
                actors.push((key[..at].to_vec(), None, None));
            }
            let actor = actors.last_mut().unwrap();
            let name = &key[at..];
            if name == b"claim" {
                actor.1 = Some(
                    pb::ClaimRecord::decode(value.as_slice()).map_err(|e| corrupt("a claim", e))?,
                );
            } else if name.starts_with(b"log/") {
                logged += (key.len() + value.len()) as i64;
            } else if name == b"owed" {
                actor.2 = <[u8; 8]>::try_from(value.as_slice())
                    .ok()
                    .map(u64::from_be_bytes);
            }
        }
        Ok((logged, actors))
    }

    /// Takes a copied span into the flush state: its actors with committed entries not yet flushed and its owed
    /// truncations. [`Store::install`] already counted its log bytes. The span is not served while it is copied, so
    /// nothing else changes it meanwhile.
    pub fn adopt(&self, start: u64, end: u64) -> Result<(), Error> {
        let (_, actors) = self.tally(start, end)?;
        for (prefix, claim, owed) in actors {
            let Some(claim) = claim else { continue };
            self.mark(&prefix, &claim);
            if let Some(through) = owed {
                self.owe(&prefix, claim.kind, through);
            }
        }
        Ok(())
    }

    /// Installs one actor's records, copied from another slot. A range handoff (`repair` false) copies the sealed leaving
    /// owner as it is. A repair keeps the more advanced log of the slot here and the copy, with its overlay, and the
    /// higher promise and reservation, and marks the actor `repaired`; a copy that is itself repaired is no voting peer
    /// and is skipped, as are its maintainer obligations (`owed`), which belong to the cell copied from. Call
    /// [`Store::adopt`] once the copy is done, to rebuild flush state.
    pub fn install(&self, records: &[pb::Record], repair: bool) -> Result<(), Error> {
        let Some(at) = records.first().and_then(|r| prefix_len(&r.key)) else {
            return Ok(());
        };
        let prefix = &records[0].key[..at];
        let claim_at = [prefix, b"claim".as_slice()].concat();
        let Some(claim) = records.iter().find(|r| r.key == claim_at) else {
            return Ok(());
        };
        if records.iter().any(|r| !r.key.starts_with(prefix)) {
            return invalid("copied records span more than one actor");
        }
        let copied =
            pb::ClaimRecord::decode(claim.value.as_slice()).map_err(|e| corrupt("a claim", e))?;
        // A tombstone is a committed fact, so it is copied even from a repaired slot.
        if repair && copied.repaired && copied.deleted.is_none() {
            return Ok(());
        }
        let parsed = Actor::parse(prefix, copied.kind)
            .ok_or_else(|| Error::Invalid("a copied actor prefix does not parse".into()))?;
        let actor = Actor::new(&parsed)?;
        let _lock = self.lock(&actor);
        let here = self
            .engine
            .get(&claim_key(&actor))?
            .map(|bytes| {
                pb::ClaimRecord::decode(bytes.as_slice()).map_err(|e| corrupt("a claim", e))
            })
            .transpose()?;
        if let Some(tomb) = here
            .as_ref()
            .filter(|h| h.deleted.is_some())
            .or(copied.deleted.is_some().then_some(&copied))
        {
            return self.install_tombstone(&actor, here.as_ref(), &copied, tomb.clone(), repair);
        }
        let last = |c: &pb::ClaimRecord| {
            let end = end(c);
            (end.epoch, end.seq)
        };
        let mut batch = WriteBatch::default();
        let take = here.as_ref().is_none_or(|h| last(&copied) > last(h));
        // Log bytes count towards backpressure from when they are on disk, as `recover` counts them: those replaced
        // leave, those copied enter. A copy cut short is then cleared without miscounting.
        let mut logged = 0;
        let mut new = if take {
            let is_log = |key: &[u8]| key[at..].starts_with(b"log/");
            for item in self.engine.scan(&KeyRange::prefix(prefix.to_vec()))? {
                let (key, value) = item?;
                if is_log(&key) {
                    logged -= (key.len() + value.len()) as i64;
                }
            }
            logged += records
                .iter()
                .filter(|r| is_log(&r.key))
                .map(|r| (r.key.len() + r.value.len()) as i64)
                .sum::<i64>();
            batch.delete_range(KeyRange::prefix(prefix.to_vec()));
            for record in records {
                if record.key != claim.key && !(repair && record.key[at..] == *b"owed") {
                    batch.put(record.key.clone(), record.value.clone());
                }
            }
            copied.clone()
        } else {
            let mut kept = here.clone().unwrap();
            // Logs that end at the same entry are the same log (Raft's log matching), so the further commit point holds.
            if last(&copied) == last(&kept) {
                kept.committed = kept.committed.max(copied.committed);
            }
            kept
        };
        if let Some(here) = &here {
            let promise = if copied.epoch > here.epoch {
                &copied
            } else {
                here
            };
            new.epoch = promise.epoch;
            new.holder = promise.holder.clone();
            new.released = promise.released && new.end == promise.end;
            new.reserved = here.reserved.max(copied.reserved);
            new.version = here.version.max(copied.version);
        }
        if repair {
            new.repaired = true;
        }
        let old = here.unwrap_or_default();
        Self::save(&mut batch, &actor, &old, &new);
        self.commit(batch)?;
        self.unflushed.fetch_add(logged, Ordering::Relaxed);
        // The copy's commit point can reach the kept log's `DELETE`.
        if deletes(&new) {
            self.bury(&actor, &new, new.clone(), &[])?;
        }
        Ok(())
    }

    /// Installs a tombstone (S-05b): the one here, or `tomb` copied from another slot. The actor's other records here
    /// are dropped, and the higher promise and reservation kept.
    fn install_tombstone(
        &self,
        actor: &Actor,
        here: Option<&pb::ClaimRecord>,
        copied: &pb::ClaimRecord,
        mut tomb: pb::ClaimRecord,
        repair: bool,
    ) -> Result<(), Error> {
        let old = here.cloned().unwrap_or_default();
        for other in [&old, copied] {
            if other.epoch > tomb.epoch {
                tomb.epoch = other.epoch;
                tomb.holder = other.holder.clone();
            }
            tomb.reserved = tomb.reserved.max(other.reserved);
            tomb.version = tomb.version.max(other.version);
        }
        tomb.repaired |= repair;
        let mut logged = 0;
        for item in self.engine.scan(&KeyRange::prefix(actor.prefix.clone()))? {
            let (key, value) = item?;
            if key[actor.prefix.len()..].starts_with(b"log/") {
                logged += (key.len() + value.len()) as i64;
            }
        }
        let mut batch = WriteBatch::default();
        batch.delete_range(KeyRange::prefix(actor.prefix.clone()));
        Self::save(&mut batch, actor, &old, &tomb);
        self.commit(batch)?;
        self.unflushed.fetch_sub(logged, Ordering::Relaxed);
        self.mark(&actor.prefix, &tomb);
        self.owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&actor.prefix);
        Ok(())
    }

    /// Spans as protocol messages.
    pub fn messages(spans: &Spans) -> Vec<pb::HashSpan> {
        spans
            .list()
            .into_iter()
            .map(|(start, end)| pb::HashSpan { start, end })
            .collect()
    }

    /// The truncations other slots still owe, in key order.
    pub fn owed(&self) -> Vec<(pb::Actor, u64)> {
        self.owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn owe(&self, prefix: &[u8], kind: i32, through: u64) {
        if let Some(parsed) = Actor::parse(prefix, kind) {
            self.owed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(prefix.to_vec(), (parsed, through));
        }
    }

    /// Drops the obligation to have `actor`'s other slots truncate through `through`, once they have; a newer one
    /// raised meanwhile stays.
    pub fn settle(&self, actor: &Actor, through: u64) -> Result<(), Error> {
        let _lock = self.lock(actor);
        if self
            .owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&actor.prefix)
            .is_none_or(|(_, owed)| *owed != through)
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        batch.delete(owed_key(actor));
        self.commit(batch)?;
        self.owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&actor.prefix);
        Ok(())
    }

    /// Refuses appends once the node holds `bytes` of unflushed log.
    pub fn with_max_unflushed(mut self, bytes: u64) -> Self {
        self.max_unflushed = bytes;
        self
    }

    /// Log bytes beyond the flushed points.
    pub fn unflushed(&self) -> i64 {
        self.unflushed.load(Ordering::Relaxed)
    }

    /// Notes whether `actor` has committed entries beyond its flushed point.
    fn mark(&self, prefix: &[u8], claim: &pb::ClaimRecord) {
        let mut dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);
        if claim.committed <= claim.flushed {
            dirty.remove(prefix);
        } else if !dirty.contains_key(prefix)
            && let Some(parsed) = Actor::parse(prefix, claim.kind)
        {
            dirty.insert(prefix.to_vec(), parsed);
        }
    }

    /// The actors of `[start, end)` (`end` 0 meaning 2⁶⁴) with committed entries not yet flushed, in key order.
    pub fn dirty(&self, start: u64, end: u64) -> Vec<pb::Actor> {
        let span = span(start, end);
        let bounds = span.bounds().expect("a hash span is never empty");
        self.dirty
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .range::<[u8], _>(bounds)
            .map(|(_, actor)| actor.clone())
            .collect()
    }

    /// What a flush of `actor` writes: its keys as of its commit point, or `None` when nothing committed is unflushed.
    /// Keys come from the log, not the overlay, which may already hold a newer, uncommitted value.
    pub fn pending(&self, actor: &Actor) -> Result<Option<Pending>, Error> {
        let _lock = self.lock(actor);
        let claim = self.load(actor)?;
        if claim.committed <= claim.flushed {
            return Ok(None);
        }
        let log = self.entries(actor, claim.flushed + 1, claim.committed)?;
        let mut changes = BTreeMap::new();
        let mut alarm = claim.flushed_alarm_ms;
        for entry in &log {
            for change in &entry.changes {
                changes.insert(change.key.clone(), change.value.clone());
            }
            if let Some(at) = &entry.alarm {
                alarm = at.at_ms;
            }
        }
        Ok(Some(Pending {
            actor: Actor::parse(&actor.prefix, claim.kind)
                .ok_or_else(|| Error::Invalid("an actor prefix does not parse".into()))?,
            flushed: claim.flushed,
            through: claim.committed,
            epoch: self.epoch_at(actor, &claim, claim.committed)?,
            alarm,
            moved: claim.moved.or_else(|| {
                log.iter()
                    .find(|e| e.kind == EntryKind::Move as i32)
                    .map(|e| pb::LogEnd {
                        seq: e.seq,
                        epoch: e.epoch,
                    })
            }),
            changes: changes.into_iter().collect(),
        }))
    }

    fn lock(&self, actor: &Actor) -> MutexGuard<'_, ()> {
        self.locks[(actor.hash % STRIPES) as usize]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The actor's claim; without one, an empty claim, `repaired` where a repair left the range marked so (S-06).
    fn load(&self, actor: &Actor) -> Result<pb::ClaimRecord, Error> {
        match self.engine.get(&claim_key(actor))? {
            Some(bytes) => {
                pb::ClaimRecord::decode(bytes.as_slice()).map_err(|e| corrupt("a claim", e))
            }
            None => Ok(pb::ClaimRecord {
                repaired: self.marks().repaired.contains(actor.hash),
                ..Default::default()
            }),
        }
    }

    fn entry(&self, actor: &Actor, seq: u64) -> Result<pb::Entry, Error> {
        let bytes = self
            .engine
            .get(&log_key(actor, seq))?
            .ok_or_else(|| missing(seq))?;
        pb::Entry::decode(bytes.as_slice()).map_err(|e| corrupt("a log entry", e))
    }

    /// The log from `from` through `through`, in order. One scan, failing like [`Store::entry`] on a missing entry.
    fn entries(&self, actor: &Actor, from: u64, through: u64) -> Result<Vec<pb::Entry>, Error> {
        if through < from {
            return Ok(Vec::new());
        }
        let range = KeyRange::new(log_key(actor, from), log_key(actor, through + 1));
        let mut log = Vec::new();
        for item in self.engine.scan(&range)? {
            let (_, bytes) = item?;
            let entry =
                pb::Entry::decode(bytes.as_slice()).map_err(|e| corrupt("a log entry", e))?;
            let expected = from + log.len() as u64;
            if entry.seq != expected {
                return Err(missing(expected));
            }
            log.push(entry);
        }
        let next = from + log.len() as u64;
        if next <= through {
            return Err(missing(next));
        }
        Ok(log)
    }

    /// The epoch of entry `seq`, which lies between `claim.flushed` and the log end.
    fn epoch_at(&self, actor: &Actor, claim: &pb::ClaimRecord, seq: u64) -> Result<u64, Error> {
        let end = end(claim);
        Ok(if seq == 0 {
            0
        } else if seq == claim.flushed {
            claim.flushed_epoch
        } else if seq == end.seq {
            end.epoch
        } else {
            self.entry(actor, seq)?.epoch
        })
    }

    fn overlay(&self, key: &[u8]) -> Result<Option<pb::OverlayRecord>, Error> {
        self.engine
            .get(key)?
            .map(|bytes| {
                pb::OverlayRecord::decode(bytes.as_slice())
                    .map_err(|e| corrupt("an overlay record", e))
            })
            .transpose()
    }

    fn commit(&self, batch: WriteBatch) -> Result<Duration, Error> {
        let started = bedrock_lib::monotonic_now();
        self.engine.write(batch)?;
        Ok(started.elapsed())
    }

    /// Adds the claim, and its alarm and tombstone index entries if they changed, to `batch`.
    fn save(batch: &mut WriteBatch, actor: &Actor, old: &pb::ClaimRecord, new: &pb::ClaimRecord) {
        Self::index(batch, alarm_key(actor, old), alarm_key(actor, new));
        Self::index(batch, gone_key(actor, old), gone_key(actor, new));
        batch.put(claim_key(actor), new.encode_to_vec());
    }

    fn index(batch: &mut WriteBatch, was: Option<Vec<u8>>, is: Option<Vec<u8>>) {
        if was != is {
            if let Some(was) = was {
                batch.delete(was);
            }
            if let Some(is) = is {
                batch.put(is, Vec::new());
            }
        }
    }

    /// The tombstones of `[start, end)` (`end` 0 meaning 2⁶⁴) whose `DELETE` committed before `before_ms`, oldest
    /// first, at most [`MAX_LIMIT`]: each actor and its `DELETE` (S-05b).
    pub fn expired(
        &self,
        start: u64,
        end: u64,
        before_ms: u64,
    ) -> Result<Vec<(pb::Actor, pb::LogEnd)>, Error> {
        let range = KeyRange::new(
            b"gone/".to_vec(),
            [b"gone/".as_slice(), &before_ms.to_be_bytes()].concat(),
        );
        let mut expired = Vec::new();
        // ponytail: one index for every range; key it by hash first if many tombstones wait at once.
        for item in self.engine.scan(&range)? {
            let (key, _) = item?;
            let prefix = [b"s/".as_slice(), &key[13..]].concat();
            let Some(hash) = prefix.get(2..10) else {
                continue;
            };
            let hash = u64::from_be_bytes(hash.try_into().unwrap());
            if hash < start || (end != 0 && hash >= end) {
                continue;
            }
            let Some(parsed) = Actor::parse(&prefix, 0) else {
                continue;
            };
            let claim = self.load(&Actor::new(&parsed)?)?;
            if let Some(deleted) = claim.deleted {
                expired.push((
                    pb::Actor {
                        kind: claim.kind,
                        ..parsed
                    },
                    deleted,
                ));
                if expired.len() == MAX_LIMIT as usize {
                    break;
                }
            }
        }
        Ok(expired)
    }

    /// Drops the tombstone index entry of `deleted` (S-05b): its range database no longer holds the actor, or this node
    /// never purges it (its first slot is in another cell). The tombstone itself stays, so the actor's ID is never
    /// claimed again.
    pub fn unindex(&self, actor: &Actor, deleted: pb::LogEnd) -> Result<(), Error> {
        let _lock = self.lock(actor);
        let old = self.load(actor)?;
        if old.deleted == Some(deleted) {
            let mut batch = WriteBatch::default();
            Self::index(&mut batch, gone_key(actor, &old), None);
            self.commit(batch)?;
        }
        Ok(())
    }

    /// Reserves `epoch` for a claim: only above the slot's highest reservation, and never on a repaired slot (#131 /
    /// S-02b). The reservation is synced before the answer.
    pub fn reserve(&self, actor: &Actor, epoch: u64, version: u64) -> Result<pb::Answer, Error> {
        let _lock = self.lock(actor);
        self.check_sealed(actor)?;
        let old = self.load(actor)?;
        if let Some(refused) = buried(&old) {
            return Ok(refused);
        }
        let voting = !old.repaired;
        let refused = if !voting {
            Refusal::NotVoting
        } else if version < old.version {
            Refusal::StaleVersion
        } else if epoch <= old.reserved {
            Refusal::StaleEpoch
        } else {
            Refusal::None
        };
        if refused != Refusal::None {
            return Ok(answer(refused, &old, voting));
        }
        let new = pb::ClaimRecord {
            reserved: epoch,
            version,
            kind: actor.kind,
            ..old.clone()
        };
        let mut batch = WriteBatch::default();
        Self::save(&mut batch, actor, &old, &new);
        self.commit(batch)?;
        Ok(answer(refused, &new, voting))
    }

    /// Starts the actor on this slot of its new DC at `seq`, whose entry has `epoch`, with `alarm_ms` as of there: the
    /// flushed `MOVE` of a DC change or a restore's point (S-17, `Import` in `volume_dc.tla`). The slot holds no log; S3
    /// holds everything through `seq`. Repeating the same import, alarm included, is a no-op, also after claims moved the
    /// slot on; a slot holding anything else of the actor refuses with `STALE_EPOCH`. Synced before the answer.
    pub fn import(
        &self,
        actor: &Actor,
        epoch: u64,
        seq: u64,
        alarm_ms: Option<u64>,
        version: u64,
    ) -> Result<pb::Answer, Error> {
        let _lock = self.lock(actor);
        self.check_sealed(actor)?;
        let old = self.load(actor)?;
        if let Some(refused) = buried(&old) {
            return Ok(refused);
        }
        let voting = !old.repaired;
        let point = pb::Import {
            epoch,
            seq,
            alarm_ms,
        };
        if old.imported == Some(point) {
            return Ok(answer(Refusal::None, &old, voting));
        }
        // A slot in a repaired span starts repaired; the import keeps that, and the slot votes once a claim commits.
        let blank = pb::ClaimRecord {
            repaired: false,
            ..old.clone()
        };
        if blank != pb::ClaimRecord::default() {
            return Ok(answer(Refusal::StaleEpoch, &old, voting));
        }
        let new = pb::ClaimRecord {
            epoch,
            reserved: epoch,
            released: true,
            version,
            committed: seq,
            end: Some(pb::LogEnd { seq, epoch }),
            flushed: seq,
            flushed_epoch: epoch,
            alarm_ms,
            flushed_alarm_ms: alarm_ms,
            kind: actor.kind,
            imported: Some(point),
            ..old.clone()
        };
        let mut batch = WriteBatch::default();
        Self::save(&mut batch, actor, &old, &new);
        self.commit(batch)?;
        Ok(answer(Refusal::None, &new, voting))
    }

    /// The `NOT_IMPORTED` refusal while this slot has not been imported: for an actor moving into this DC (S-17).
    pub fn unimported(&self, actor: &Actor) -> Result<Option<pb::Answer>, Error> {
        let old = self.load(actor)?;
        Ok(old
            .imported
            .is_none()
            .then(|| answer(Refusal::NotImported, &old, !old.repaired)))
    }

    /// Promises `epoch` to `holder`: only above the slot's promise, at its topology version or newer, and while no other
    /// holder's lease is `live`. Returns whether this took over from a holder whose lease expired.
    pub fn claim(
        &self,
        actor: &Actor,
        holder: &pb::Holder,
        epoch: u64,
        version: u64,
        live: impl Fn(&pb::Holder) -> bool,
    ) -> Result<(pb::Answer, bool), Error> {
        let _lock = self.lock(actor);
        self.check_sealed(actor)?;
        let old = self.load(actor)?;
        if let Some(refused) = buried(&old) {
            return Ok((refused, false));
        }
        let voting = !old.repaired;
        let other = old
            .holder
            .as_ref()
            .filter(|h| *h != holder && !old.released);
        let refused = if version < old.version {
            Refusal::StaleVersion
        } else if epoch <= old.epoch {
            Refusal::StaleEpoch
        } else if other.is_some_and(&live) {
            Refusal::LeaseLive
        } else {
            Refusal::None
        };
        if refused != Refusal::None {
            return Ok((answer(refused, &old, voting), false));
        }
        let new = pb::ClaimRecord {
            epoch,
            holder: Some(holder.clone()),
            released: false,
            version,
            kind: actor.kind,
            ..old.clone()
        };
        let mut batch = WriteBatch::default();
        Self::save(&mut batch, actor, &old, &new);
        self.commit(batch)?;
        Ok((answer(refused, &new, voting), other.is_some()))
    }

    /// Writes entries at the slot's promise (epoch and holder), once its log holds the holder's entry before them. An
    /// entry the slot already has at the same epoch is skipped; one at another epoch is an old epoch's uncommitted tail,
    /// which is truncated with the overlay changes it made.
    pub fn append(&self, actor: &Actor, request: &pb::AppendRequest) -> Result<Outcome, Error> {
        let Some(holder) = &request.holder else {
            return invalid("an append names its holder");
        };
        let Some(first) = request.entries.first() else {
            return invalid("an append carries entries");
        };
        let mut epoch = request.prev_epoch;
        for (i, entry) in request.entries.iter().enumerate() {
            if first.seq == 0 || entry.seq != first.seq + i as u64 {
                return invalid("entries are consecutive from sequence 1 or later");
            }
            if entry.epoch < epoch || entry.epoch > request.epoch {
                return invalid("entry epochs rise from prev_epoch to the request's epoch");
            }
            epoch = entry.epoch;
        }
        let last = first.seq + request.entries.len() as u64 - 1;

        let _lock = self.lock(actor);
        self.check_sealed(actor)?;
        let old = self.load(actor)?;
        if let Some(refused) = buried(&old) {
            return Ok(Outcome::new(refused));
        }
        let voting = !old.repaired;
        if old.epoch != request.epoch || old.holder.as_ref() != Some(holder) {
            return Ok(Outcome::new(answer(Refusal::NotPromised, &old, voting)));
        }
        if request.version < old.version {
            return Ok(Outcome::new(answer(Refusal::StaleVersion, &old, voting)));
        }
        let old_end = end(&old);
        // S3 holds everything through `flushed`, which is committed, so the holder's copies of it match.
        let (prev, prev_epoch) = if first.seq - 1 < old.flushed {
            (old.flushed, old.flushed_epoch)
        } else {
            (first.seq - 1, request.prev_epoch)
        };
        if prev > old_end.seq || self.epoch_at(actor, &old, prev)? != prev_epoch {
            return Ok(Outcome::new(answer(Refusal::LogGap, &old, voting)));
        }
        let mut entries = request.entries.iter().filter(|e| e.seq > old.flushed);
        // Skip what the slot already has; the first entry it lacks or holds at another epoch starts the write.
        let mut fresh = Vec::new();
        let mut conflict = None;
        for entry in entries.by_ref() {
            if entry.seq > old_end.seq {
                fresh.push(entry);
                break;
            }
            if self.epoch_at(actor, &old, entry.seq)? != entry.epoch {
                conflict = Some(entry.seq);
                fresh.push(entry);
                break;
            }
        }
        fresh.extend(entries);

        // Only growth past the bounds waits for a flush: a new epoch's recovery that replaces an old tail with as much
        // or less still goes through.
        let added: i64 = fresh.iter().map(|e| log_bytes(actor, e)).sum();
        let replaced: i64 = match conflict {
            Some(from) => self
                .entries(actor, from, old_end.seq)?
                .iter()
                .map(|e| log_bytes(actor, e))
                .sum(),
            None => 0,
        };
        let full = added > replaced
            && fresh.last().is_some_and(|last| {
                last.seq - old.flushed > MAX_UNFLUSHED_ENTRIES
                    || self.unflushed().max(0) as u64 >= self.max_unflushed
            });
        if full {
            // The log matches the holder's up to its own end, or up to the conflict, so it still learns the commit
            // point there: without it the slot could never truncate, and would stay full.
            let matched = conflict.map_or(old_end.seq, |from| from - 1);
            let commit = request.commit.min(last).min(matched);
            if commit > old.committed {
                let new = pb::ClaimRecord {
                    committed: commit,
                    ..old.clone()
                };
                if deletes(&new) {
                    return self.bury(actor, &old, new, &[]);
                }
                let mut batch = WriteBatch::default();
                Self::save(&mut batch, actor, &old, &new);
                self.commit(batch)?;
                self.mark(&actor.prefix, &new);
            }
            return Err(Error::Backpressure);
        }
        let mut new = pb::ClaimRecord {
            version: request.version,
            kind: actor.kind,
            ..old.clone()
        };
        let mut batch = WriteBatch::default();
        let mut logged = 0;
        // Overlay records to write (`None` removes one, so reads fall back to S3).
        let mut overlay: BTreeMap<Vec<u8>, Option<pb::OverlayRecord>> = BTreeMap::new();
        if let Some(from) = conflict {
            logged -= self.truncate_tail(actor, &mut new, from, &mut batch, &mut overlay)?;
        }
        for entry in &fresh {
            logged += log_bytes(actor, entry);
            batch.put(log_key(actor, entry.seq), entry.encode_to_vec());
            for change in &entry.changes {
                overlay.insert(
                    data_key(actor, &change.key),
                    Some(pb::OverlayRecord {
                        seq: entry.seq,
                        value: change.value.clone(),
                    }),
                );
            }
            if let Some(alarm) = &entry.alarm {
                new.alarm_ms = alarm.at_ms;
            }
            // A `MOVE` (S-17) or `DELETE` (S-05b) ends the holder's claim like a release.
            let ends = [EntryKind::Release, EntryKind::Move, EntryKind::Delete]
                .map(|k| k as i32)
                .contains(&entry.kind);
            if ends && entry.epoch == request.epoch {
                new.released = true;
            }
            if entry.kind == EntryKind::Delete as i32 && new.deleting.is_none() {
                new.deleting = Some(pb::LogEnd {
                    seq: entry.seq,
                    epoch: entry.epoch,
                });
            }
            new.end = Some(pb::LogEnd {
                seq: entry.seq,
                epoch: entry.epoch,
            });
        }
        // The slot's log matches the holder's through `last`, so it may learn the commit point up to there.
        let commit = request.commit.min(last);
        new.committed = new.committed.max(commit.min(end(&new).seq));
        // A repaired slot votes again once a claim above its reservation has committed an entry (`Replicate`).
        if new.repaired && request.epoch > new.reserved && commit > old.flushed {
            let epoch_of = |seq: u64| match fresh.iter().find(|e| e.seq == seq) {
                Some(entry) => Ok(entry.epoch),
                None => self.epoch_at(actor, &new, seq),
            };
            if epoch_of(commit)? == request.epoch {
                new.repaired = false;
            }
        }
        if deletes(&new) {
            return self.bury(actor, &old, new, &fresh);
        }
        let mut delta = 0;
        for (key, record) in overlay {
            delta += overlay_size(&key, record.as_ref())
                - overlay_size(&key, self.overlay(&key)?.as_ref());
            match record {
                Some(record) => batch.put(key, record.encode_to_vec()),
                None => batch.delete(key),
            };
        }
        Self::save(&mut batch, actor, &old, &new);
        let fsync = self.commit(batch)?;
        self.unflushed.fetch_add(logged, Ordering::Relaxed);
        self.mark(&actor.prefix, &new);
        Ok(Outcome {
            answer: answer(Refusal::None, &new, voting),
            overlay: delta,
            fsync: Some(fsync),
        })
    }

    /// Drops the log from `from` (an old epoch's uncommitted tail) and restores the overlay and alarm it changed from
    /// the rest of the log, or from S3's state where the log no longer has them. Returns the log bytes dropped.
    fn truncate_tail(
        &self,
        actor: &Actor,
        claim: &mut pb::ClaimRecord,
        from: u64,
        batch: &mut WriteBatch,
        overlay: &mut BTreeMap<Vec<u8>, Option<pb::OverlayRecord>>,
    ) -> Result<i64, Error> {
        let log = self.entries(actor, claim.flushed + 1, end(claim).seq)?;
        let (kept, dropped) = log.split_at(log.partition_point(|e| e.seq < from));
        batch.delete_range(KeyRange::new(
            log_key(actor, from),
            log_key(actor, end(claim).seq + 1),
        ));
        // The last value the kept log wrote to each key, deletes included.
        let mut last = BTreeMap::new();
        for entry in kept {
            for change in &entry.changes {
                last.insert(change.key.as_slice(), (entry.seq, &change.value));
            }
        }
        for change in dropped.iter().flat_map(|e| &e.changes) {
            let restored = last
                .get(change.key.as_slice())
                .map(|(seq, value)| pb::OverlayRecord {
                    seq: *seq,
                    value: (*value).clone(),
                });
            overlay.insert(data_key(actor, &change.key), restored);
        }
        if claim.deleting.is_some_and(|d| d.seq >= from) {
            claim.deleting = None;
        }
        if dropped.iter().any(|e| e.alarm.is_some()) {
            claim.alarm_ms = kept
                .iter()
                .rev()
                .find_map(|e| e.alarm.as_ref())
                .map_or(claim.flushed_alarm_ms, |alarm| alarm.at_ms);
        }
        claim.end = Some(match kept.last() {
            Some(e) => pb::LogEnd {
                seq: e.seq,
                epoch: e.epoch,
            },
            None => pb::LogEnd {
                seq: claim.flushed,
                epoch: claim.flushed_epoch,
            },
        });
        Ok(dropped.iter().map(|e| log_bytes(actor, e)).sum())
    }

    /// Writes `new`, whose `DELETE` committed, as the actor's tombstone (S-05b, `Replicate` in `volume.tla`): one batch
    /// drops its log, overlay, alarm and owed truncation, and keeps only the claim, which then refuses reservations,
    /// claims, appends and reads with `DELETED`. `fresh` holds entries of the request not written yet; the `DELETE` is
    /// there or in the log. Answers `DELETED`.
    fn bury(
        &self,
        actor: &Actor,
        old: &pb::ClaimRecord,
        new: pb::ClaimRecord,
        fresh: &[&pb::Entry],
    ) -> Result<Outcome, Error> {
        let delete = new.deleting.expect("a claim that deletes has its DELETE");
        let at = match fresh.iter().find(|e| e.seq == delete.seq) {
            Some(entry) => entry.committed_at,
            None => self.entry(actor, delete.seq)?.committed_at,
        };
        let (mut logged, mut overlay) = (0, 0);
        for item in self.engine.scan(&KeyRange::prefix(actor.prefix.clone()))? {
            let (key, value) = item?;
            let name = &key[actor.prefix.len()..];
            let size = (key.len() + value.len()) as i64;
            if name.starts_with(b"log/") {
                logged += size;
            } else if name.starts_with(b"data/") {
                overlay += size;
            }
        }
        let end = end(&new);
        let tomb = pb::ClaimRecord {
            released: true,
            committed: end.seq,
            flushed: end.seq,
            flushed_epoch: end.epoch,
            alarm_ms: None,
            flushed_alarm_ms: None,
            deleting: None,
            deleted: Some(delete),
            deleted_at_ms: at,
            ..new
        };
        let mut batch = WriteBatch::default();
        batch.delete_range(KeyRange::prefix(actor.prefix.clone()));
        Self::save(&mut batch, actor, old, &tomb);
        let fsync = self.commit(batch)?;
        self.unflushed.fetch_sub(logged, Ordering::Relaxed);
        self.mark(&actor.prefix, &tomb);
        self.owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&actor.prefix);
        Ok(Outcome {
            answer: answer(Refusal::Deleted, &tomb, !tomb.repaired),
            overlay: -overlay,
            fsync: Some(fsync),
        })
    }

    /// Drops the log and overlay through `through`, which S3 now holds (S-05's maintainer calls this). Only committed
    /// entries can be dropped, so a slot that knows less is committed truncates that far and answers `Behind`; its
    /// `flushed` says how far it got. Without that, a lagging slot whose log is full could never shrink.
    pub fn truncate(&self, actor: &Actor, through: u64) -> Result<Outcome, Error> {
        self.truncate_and(actor, through, false)
    }

    /// [`Store::truncate`] by the actor's maintainer, which also records, in the same batch, that the actor's other
    /// slots owe a truncation through `through` until [`Store::settle`]: the obligation survives a restart and moves
    /// with the range. An obligation only rises: a lower one leaves a higher one in place.
    pub fn truncate_owing(&self, actor: &Actor, through: u64) -> Result<Outcome, Error> {
        self.truncate_and(actor, through, true)
    }

    fn truncate_and(&self, actor: &Actor, owed: u64, owe: bool) -> Result<Outcome, Error> {
        let _lock = self.lock(actor);
        let old = self.load(actor)?;
        let voting = !old.repaired;
        let refused = if owed > old.committed {
            Refusal::Behind
        } else {
            Refusal::None
        };
        let through = owed.min(old.committed);
        // The actor's stripe lock is held, so the obligation cannot change under this check.
        let owed = self
            .owed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&actor.prefix)
            .map_or(owed, |(_, held)| owed.max(*held));
        let mut batch = WriteBatch::default();
        if owe {
            batch.put(owed_key(actor), owed.to_be_bytes().to_vec());
        }
        if through <= old.flushed {
            if owe {
                self.commit(batch)?;
                self.owe(&actor.prefix, old.kind, owed);
            }
            return Ok(Outcome::new(answer(refused, &old, voting)));
        }
        let log = self.entries(actor, old.flushed + 1, through)?;
        let mut new = old.clone();
        batch.delete_range(KeyRange::new(
            log_key(actor, old.flushed + 1),
            log_key(actor, through + 1),
        ));
        let mut delta = 0;
        let mut seen = std::collections::BTreeSet::new();
        for entry in &log {
            for change in &entry.changes {
                let key = data_key(actor, &change.key);
                if !seen.insert(key.clone()) {
                    continue;
                }
                let record = self.overlay(&key)?;
                if record.as_ref().is_some_and(|r| r.seq <= through) {
                    delta -= overlay_size(&key, record.as_ref());
                    batch.delete(key);
                }
            }
            if let Some(alarm) = &entry.alarm {
                new.flushed_alarm_ms = alarm.at_ms;
            }
            // Only committed entries are truncated, in order, so the first `MOVE` truncated is the log's first (S-17).
            if entry.kind == EntryKind::Move as i32 && new.moved.is_none() {
                new.moved = Some(pb::LogEnd {
                    seq: entry.seq,
                    epoch: entry.epoch,
                });
            }
        }
        new.flushed = through;
        new.flushed_epoch = self.epoch_at(actor, &old, through)?;
        Self::save(&mut batch, actor, &old, &new);
        let fsync = self.commit(batch)?;
        let freed: i64 = log.iter().map(|e| log_bytes(actor, e)).sum();
        self.unflushed.fetch_sub(freed, Ordering::Relaxed);
        self.mark(&actor.prefix, &new);
        if owe {
            self.owe(&actor.prefix, new.kind, owed);
        }
        Ok(Outcome {
            answer: answer(refused, &new, voting),
            overlay: delta,
            fsync: Some(fsync),
        })
    }

    /// Takes S3's state of the actor at `seq`, whose entry has `epoch`, with the alarm `alarm` there, as this slot's
    /// flushed point when its log ends below it (S-06, `Skip` in `volume.tla`). A holder that recovered from a slot
    /// that truncated cannot resend what S3 holds, while this slot kept every promise and reservation, so it keeps its
    /// vote. S3 holds only committed entries, so what the slot had up to there is either the same or could never commit.
    /// Only to an S3 point no newer than the slot's promise: the promised holder's log then holds it, so its appends
    /// continue S3's state, while a holder a newer one fenced may lack it. The answer is `None` when the log already
    /// reaches `seq` or `epoch` is above the promise. `moved` is the first `MOVE` S3 holds (S-17), which the slot keeps
    /// as its own, so a later `MOVE` never takes its place.
    #[allow(clippy::too_many_arguments)]
    pub fn skip(
        &self,
        actor: &Actor,
        holder: &pb::Holder,
        promise: u64,
        seq: u64,
        epoch: u64,
        alarm: Option<u64>,
        moved: Option<pb::LogEnd>,
    ) -> Result<Option<Outcome>, Error> {
        let _lock = self.lock(actor);
        self.check_sealed(actor)?;
        let old = self.load(actor)?;
        let old_end = end(&old);
        // Only for the holder whose append found the gap, while the slot still promises it: another claim may have come
        // in since.
        let promised = old.holder.as_ref() == Some(holder) && old.epoch == promise;
        if !promised || old_end.seq >= seq || epoch > old.epoch || old.deleted.is_some() {
            return Ok(None);
        }
        let log = self.entries(actor, old.flushed + 1, old_end.seq)?;
        let mut batch = WriteBatch::default();
        batch.delete_range(KeyRange::new(
            log_key(actor, old.flushed + 1),
            log_key(actor, old_end.seq + 1),
        ));
        // Every overlay record is older than `seq`: reads of those keys go to S3.
        let mut delta = 0;
        let mut seen = std::collections::BTreeSet::new();
        for entry in &log {
            for change in &entry.changes {
                let key = data_key(actor, &change.key);
                if seen.insert(key.clone())
                    && let Some(record) = self.overlay(&key)?
                {
                    delta -= overlay_size(&key, Some(&record));
                    batch.delete(key);
                }
            }
        }
        let new = pb::ClaimRecord {
            end: Some(pb::LogEnd { seq, epoch }),
            committed: old.committed.max(seq),
            flushed: seq,
            flushed_epoch: epoch,
            alarm_ms: alarm,
            flushed_alarm_ms: alarm,
            moved: old.moved.or(moved),
            // The dropped log ends below S3, which holds only committed entries and never a `DELETE`: a slot that knows
            // its `DELETE` committed is a tombstone, so it never flushes one.
            deleting: None,
            ..old.clone()
        };
        Self::save(&mut batch, actor, &old, &new);
        let fsync = self.commit(batch)?;
        let freed: i64 = log.iter().map(|e| log_bytes(actor, e)).sum();
        self.unflushed.fetch_sub(freed, Ordering::Relaxed);
        self.mark(&actor.prefix, &new);
        Ok(Some(Outcome {
            answer: answer(Refusal::None, &new, !new.repaired),
            overlay: delta,
            fsync: Some(fsync),
        }))
    }

    /// The slot can answer for `min_seq` when it knows that far is committed, or its log ends in the promise's epoch
    /// (so it matches the holder's) at `min_seq` or later.
    fn caught_up(claim: &pb::ClaimRecord, min_seq: u64) -> bool {
        let end = end(claim);
        claim.committed >= min_seq || (end.epoch == claim.epoch && end.seq >= min_seq)
    }

    /// Values from the overlay, and the positions of keys it lacks while S3 holds the actor (`answer.slot.flushed` > 0):
    /// those are read from the range database at the flushed point or later. Without S3 state a missing key is `None`.
    pub fn read(
        &self,
        actor: &Actor,
        keys: &[Vec<u8>],
        min_seq: u64,
    ) -> Result<(pb::Answer, Vec<pb::Value>, Vec<usize>), Error> {
        let _lock = self.lock(actor);
        let claim = self.load(actor)?;
        if let Some(refused) = buried(&claim) {
            return Ok((refused, Vec::new(), Vec::new()));
        }
        let voting = !claim.repaired;
        if !Self::caught_up(&claim, min_seq) {
            return Ok((
                answer(Refusal::Behind, &claim, voting),
                Vec::new(),
                Vec::new(),
            ));
        }
        let mut values = Vec::with_capacity(keys.len());
        let mut cold = Vec::new();
        for key in keys {
            let value = match self.overlay(&data_key(actor, key))? {
                Some(record) => record.value,
                None if claim.flushed == 0 => None,
                None => {
                    cold.push(values.len());
                    None
                }
            };
            values.push(pb::Value {
                key: key.clone(),
                value,
            });
        }
        Ok((answer(Refusal::None, &claim, voting), values, cold))
    }

    /// Up to `limit` live keys from `start`, in order, and whether more follow. While S3 holds the actor
    /// (`answer.slot.flushed` > 0) the overlay is only part of the answer, so its records come back with deletes
    /// (`value` absent) for the caller to merge over the range database, `limit` counting both.
    pub fn scan(
        &self,
        actor: &Actor,
        start: &[u8],
        limit: u32,
        min_seq: u64,
    ) -> Result<(pb::Answer, Vec<pb::Value>, bool), Error> {
        let _lock = self.lock(actor);
        let claim = self.load(actor)?;
        if let Some(refused) = buried(&claim) {
            return Ok((refused, Vec::new(), false));
        }
        let voting = !claim.repaired;
        if !Self::caught_up(&claim, min_seq) {
            return Ok((answer(Refusal::Behind, &claim, voting), Vec::new(), false));
        }
        let cold = claim.flushed > 0;
        let limit = limit.clamp(1, MAX_LIMIT) as usize;
        let prefix = data_key(actor, b"");
        let range = KeyRange {
            start: data_key(actor, start),
            end: KeyRange::prefix(prefix.clone()).end,
        };
        let mut values = Vec::new();
        for item in self.engine.scan(&range)? {
            let (key, bytes) = item?;
            let record = pb::OverlayRecord::decode(bytes.as_slice())
                .map_err(|e| corrupt("an overlay record", e))?;
            if record.value.is_none() && !cold {
                continue;
            }
            if values.len() == limit {
                return Ok((answer(Refusal::None, &claim, voting), values, true));
            }
            values.push(pb::Value {
                key: key[prefix.len()..].to_vec(),
                value: record.value,
            });
        }
        Ok((answer(Refusal::None, &claim, voting), values, false))
    }

    /// The claimant's recovery read: up to `limit` log entries from `from`, or from the first after the flushed point,
    /// and whether more follow. Only the holder the slot promised at `epoch` may read; S3 holds the rest.
    pub fn log(
        &self,
        actor: &Actor,
        holder: &pb::Holder,
        epoch: u64,
        from: u64,
        limit: u32,
    ) -> Result<(pb::Answer, Vec<pb::Entry>, bool), Error> {
        let _lock = self.lock(actor);
        let claim = self.load(actor)?;
        if let Some(refused) = buried(&claim) {
            return Ok((refused, Vec::new(), false));
        }
        let voting = !claim.repaired;
        if claim.epoch != epoch || claim.holder.as_ref() != Some(holder) {
            return Ok((
                answer(Refusal::NotPromised, &claim, voting),
                Vec::new(),
                false,
            ));
        }
        let from = from.max(claim.flushed + 1);
        let end = end(&claim).seq;
        let through = end.min(from.saturating_add(u64::from(limit.clamp(1, MAX_LIMIT)) - 1));
        let entries = self.entries(actor, from, through)?;
        Ok((
            answer(Refusal::None, &claim, voting),
            entries,
            through < end,
        ))
    }

    /// Up to `limit` raw records of `[start, end)` after `after`, for a peer's repair copy (S-06), and whether more
    /// follow.
    pub fn records(
        &self,
        start: u64,
        end: u64,
        after: &[u8],
        limit: u32,
    ) -> StorageResult<(Vec<pb::Record>, bool)> {
        let range = span(start, end);
        let after = (!after.is_empty()).then_some(after);
        let Some(bounds) = range.bounds_after(after) else {
            return Ok((Vec::new(), false));
        };
        let range = KeyRange {
            start: match bounds.0 {
                std::ops::Bound::Included(k) => k.to_vec(),
                // The first key after `after`.
                _ => [after.unwrap_or_default(), &[0]].concat(),
            },
            end: range.end.clone(),
        };
        let limit = limit.clamp(1, MAX_LIMIT) as usize;
        let mut records = Vec::new();
        for item in self.engine.scan(&range)? {
            let (key, value) = item?;
            if records.len() == limit {
                return Ok((records, true));
            }
            records.push(pb::Record { key, value });
        }
        Ok((records, false))
    }

    /// About how many bytes the slots of `[start, end)` hold (`end` 0 meaning 2⁶⁴), and a hash with about half of them
    /// below it, or 0 when the range is empty or one hash wide. Placement balances ranges on these (#111 / S-03).
    pub fn size(&self, start: u64, end: u64) -> StorageResult<RangeSize> {
        let bytes = self.engine.approximate_size(&span(start, end))?;
        let to = if end == 0 { 1 << 64 } else { u128::from(end) };
        let (mut low, mut high) = (u128::from(start), to);
        // Twenty halvings put the middle within a millionth of the span.
        for _ in 0..20 {
            if bytes == 0 || high - low < 2 {
                break;
            }
            let mid = (low + high) / 2;
            if self.engine.approximate_size(&span(start, mid as u64))? * 2 < bytes {
                low = mid;
            } else {
                high = mid;
            }
        }
        let inside = bytes > 0 && high > u128::from(start) && high < to;
        Ok(RangeSize {
            span: Some(Span { start, end }),
            bytes,
            middle: if inside { high as u64 } else { 0 },
        })
    }

    /// Rebuilds the in-memory flush state (unflushed actors, log bytes, owed truncations) and returns overlay bytes per
    /// actor kind, for
    /// the gauge: one pass over every slot record at startup.
    // ponytail: a full scan at startup; keep running totals in a record if startup time matters.
    pub fn recover(&self) -> Result<BTreeMap<i32, i64>, Error> {
        {
            let mut guard = self.marks();
            let marks = &mut *guard;
            for (key, spans) in [
                (HELD, &mut marks.held),
                (SEALED, &mut marks.sealed),
                (REPAIRED, &mut marks.repaired),
            ] {
                if let Some(bytes) = self.engine.get(key)? {
                    *spans = Spans::decode(&bytes)?;
                }
            }
            if let Some(fresh) = self.engine.get(FRESH)? {
                marks.initialized = true;
                marks.fresh = fresh == [1];
            }
        }
        let mut bytes = BTreeMap::new();
        let mut kind = 0;
        let mut logged = 0;
        for item in self.engine.scan(&KeyRange::prefix(*b"s/"))? {
            let (key, value) = item?;
            // `s/`, the hash, then `{namespace}\0{id}\0` and the record's name.
            let Some(at) = prefix_len(&key) else {
                continue;
            };
            let name = &key[at..];
            if name == b"claim" {
                let claim =
                    pb::ClaimRecord::decode(value.as_slice()).map_err(|e| corrupt("a claim", e))?;
                kind = claim.kind;
                self.mark(&key[..at], &claim);
            } else if name.starts_with(b"data/") {
                *bytes.entry(kind).or_default() += (key.len() + value.len()) as i64;
            } else if name.starts_with(b"log/") {
                logged += (key.len() + value.len()) as i64;
            } else if name == b"owed" {
                let through = <[u8; 8]>::try_from(value.as_slice()).map_err(|_| {
                    Error::Storage(StorageError::Corruption(ErrorContext {
                        engine: "volume",
                        instance: "volume".into(),
                        file: None,
                        partition: None,
                        detail: "an owed truncation is not 8 bytes".into(),
                    }))
                })?;
                self.owe(&key[..at], kind, u64::from_be_bytes(through));
            }
        }
        self.unflushed.store(logged, Ordering::Relaxed);
        Ok(bytes)
    }
}

/// Whether `new` sizes are worth a membership write over the `published` ones: the ranges differ, or one range's bytes
/// moved by more than an eighth and 1 MiB. Placement's own band absorbs smaller drift.
pub fn worth_publishing(published: &[RangeSize], new: &[RangeSize]) -> bool {
    published.len() != new.len()
        || published.iter().zip(new).any(|(old, new)| {
            let moved = old.bytes.abs_diff(new.bytes);
            old.span != new.span || (moved > old.bytes / 8 && moved > 1 << 20)
        })
}

#[cfg(test)]
#[path = "../tests/unit/store.rs"]
mod tests;
