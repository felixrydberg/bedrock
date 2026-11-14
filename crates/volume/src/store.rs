//! Slot records and their rules (#111 / S-03, `statefull/v2.md` §5, `docs/spec/volume.tla`).
//!
//! Records live in a [`StorageEngine`]: RocksDB, or the in-memory engine in simulation builds. Every actor key starts
//! with `s/` and the actor's big-endian hash, so a hash range is one scan and dropping it one `delete_range`:
//! `s/{hash}{namespace}\0{id}\0` followed by `claim`, `log/{seq}` or `data/{key}` (the overlay). Released actors with an
//! alarm are also indexed by time, `alarm/{when}{hash}{namespace}\0{id}`, removed with the claim's release.
//!
//! Each request is one atomic batch: log entries, overlay changes and the claim together. The engine syncs every batch
//! before it returns, and RocksDB group-commits concurrent batches into one fsync, so an answer always follows its fsync.
//!
//! The rules are the model's: [`Store::reserve`] is `Reserve`, [`Store::claim`] is `Grant` and [`Store::append`] is
//! `Replicate`. A `repaired` slot (S-06 marks it) takes claims and appends, but its answers say it does not vote; it
//! votes again once an append shows that a claim above its reservation has committed.

use std::collections::BTreeMap;
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

#[derive(Debug)]
pub enum Error {
    Storage(StorageError),
    Invalid(String),
    /// The overlay does not hold the key, and S3 might: cold reads arrive with S-05.
    Cold,
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

/// The alarm index entry of a released actor with an alarm.
fn alarm_key(actor: &Actor, claim: &pb::ClaimRecord) -> Option<Vec<u8>> {
    let at = claim.alarm_ms.filter(|_| claim.released)?;
    Some([b"alarm/".as_slice(), &at.to_be_bytes(), &actor.prefix[2..]].concat())
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
        }),
    }
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
        }
    }

    fn lock(&self, actor: &Actor) -> MutexGuard<'_, ()> {
        self.locks[(actor.hash % STRIPES) as usize]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn load(&self, actor: &Actor) -> Result<pb::ClaimRecord, Error> {
        match self.engine.get(&claim_key(actor))? {
            Some(bytes) => {
                pb::ClaimRecord::decode(bytes.as_slice()).map_err(|e| corrupt("a claim", e))
            }
            None => Ok(pb::ClaimRecord::default()),
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

    /// Adds the claim, and its alarm index entry if that changed, to `batch`.
    fn save(batch: &mut WriteBatch, actor: &Actor, old: &pb::ClaimRecord, new: &pb::ClaimRecord) {
        let (was, is) = (alarm_key(actor, old), alarm_key(actor, new));
        if was != is {
            if let Some(was) = was {
                batch.delete(was);
            }
            if let Some(is) = is {
                batch.put(is, Vec::new());
            }
        }
        batch.put(claim_key(actor), new.encode_to_vec());
    }

    /// Reserves `epoch` for a claim: only above the slot's highest reservation, and never on a repaired slot (#131 /
    /// S-02b). The reservation is synced before the answer.
    pub fn reserve(&self, actor: &Actor, epoch: u64, version: u64) -> Result<pb::Answer, Error> {
        let _lock = self.lock(actor);
        let old = self.load(actor)?;
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
        let old = self.load(actor)?;
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
        let old = self.load(actor)?;
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

        let mut new = pb::ClaimRecord {
            version: request.version,
            kind: actor.kind,
            ..old.clone()
        };
        let mut batch = WriteBatch::default();
        // Overlay records to write (`None` removes one, so reads fall back to S3).
        let mut overlay: BTreeMap<Vec<u8>, Option<pb::OverlayRecord>> = BTreeMap::new();
        if let Some(from) = conflict {
            self.truncate_tail(actor, &mut new, from, &mut batch, &mut overlay)?;
        }
        for entry in &fresh {
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
            if entry.kind == EntryKind::Release as i32 && entry.epoch == request.epoch {
                new.released = true;
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
        Ok(Outcome {
            answer: answer(Refusal::None, &new, voting),
            overlay: delta,
            fsync: Some(fsync),
        })
    }

    /// Drops the log from `from` (an old epoch's uncommitted tail) and restores the overlay and alarm it changed from
    /// the rest of the log, or from S3's state where the log no longer has them.
    fn truncate_tail(
        &self,
        actor: &Actor,
        claim: &mut pb::ClaimRecord,
        from: u64,
        batch: &mut WriteBatch,
        overlay: &mut BTreeMap<Vec<u8>, Option<pb::OverlayRecord>>,
    ) -> Result<(), Error> {
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
        Ok(())
    }

    /// Drops the log and overlay through `through`, which S3 now holds (S-05's maintainer calls this). Only committed
    /// entries can be dropped.
    pub fn truncate(&self, actor: &Actor, through: u64) -> Result<Outcome, Error> {
        let _lock = self.lock(actor);
        let old = self.load(actor)?;
        let voting = !old.repaired;
        if through <= old.flushed {
            return Ok(Outcome::new(answer(Refusal::None, &old, voting)));
        }
        if through > old.committed {
            return Ok(Outcome::new(answer(Refusal::Behind, &old, voting)));
        }
        let log = self.entries(actor, old.flushed + 1, through)?;
        let mut new = old.clone();
        let mut batch = WriteBatch::default();
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
        }
        new.flushed = through;
        new.flushed_epoch = self.epoch_at(actor, &old, through)?;
        Self::save(&mut batch, actor, &old, &new);
        let fsync = self.commit(batch)?;
        Ok(Outcome {
            answer: answer(Refusal::None, &new, voting),
            overlay: delta,
            fsync: Some(fsync),
        })
    }

    /// The slot can answer for `min_seq` when it knows that far is committed, or its log ends in the promise's epoch
    /// (so it matches the holder's) at `min_seq` or later.
    fn caught_up(claim: &pb::ClaimRecord, min_seq: u64) -> bool {
        let end = end(claim);
        claim.committed >= min_seq || (end.epoch == claim.epoch && end.seq >= min_seq)
    }

    /// Values from the overlay. A key the overlay lacks is `None` while S3 holds nothing of the actor, and a cold read
    /// otherwise.
    pub fn read(
        &self,
        actor: &Actor,
        keys: &[Vec<u8>],
        min_seq: u64,
    ) -> Result<(pb::Answer, Vec<pb::Value>), Error> {
        let _lock = self.lock(actor);
        let claim = self.load(actor)?;
        let voting = !claim.repaired;
        if !Self::caught_up(&claim, min_seq) {
            return Ok((answer(Refusal::Behind, &claim, voting), Vec::new()));
        }
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = match self.overlay(&data_key(actor, key))? {
                Some(record) => record.value,
                None if claim.flushed == 0 => None,
                None => return Err(Error::Cold),
            };
            values.push(pb::Value {
                key: key.clone(),
                value,
            });
        }
        Ok((answer(Refusal::None, &claim, voting), values))
    }

    /// Up to `limit` live keys from `start`, in order, and whether more follow.
    pub fn scan(
        &self,
        actor: &Actor,
        start: &[u8],
        limit: u32,
        min_seq: u64,
    ) -> Result<(pb::Answer, Vec<pb::Value>, bool), Error> {
        let _lock = self.lock(actor);
        let claim = self.load(actor)?;
        let voting = !claim.repaired;
        if !Self::caught_up(&claim, min_seq) {
            return Ok((answer(Refusal::Behind, &claim, voting), Vec::new(), false));
        }
        if claim.flushed > 0 {
            return Err(Error::Cold);
        }
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
            let Some(value) = record.value else { continue };
            if values.len() == limit {
                return Ok((answer(Refusal::None, &claim, voting), values, true));
            }
            values.push(pb::Value {
                key: key[prefix.len()..].to_vec(),
                value: Some(value),
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

    /// Overlay bytes per actor kind, for the gauge at startup: one pass over every slot record.
    // ponytail: a full scan at startup; keep running totals in a record if startup time matters.
    pub fn overlay_bytes(&self) -> Result<BTreeMap<i32, i64>, Error> {
        let mut bytes = BTreeMap::new();
        let mut kind = 0;
        for item in self.engine.scan(&KeyRange::prefix(*b"s/"))? {
            let (key, value) = item?;
            // `s/`, the hash, then `{namespace}\0{id}\0` and the record's name.
            let Some(name) = key.get(10..).and_then(|rest| {
                let first = rest.iter().position(|b| *b == 0)?;
                let second = rest[first + 1..].iter().position(|b| *b == 0)?;
                rest.get(first + second + 2..)
            }) else {
                continue;
            };
            if name == b"claim" {
                kind = pb::ClaimRecord::decode(value.as_slice())
                    .map_err(|e| corrupt("a claim", e))?
                    .kind;
            } else if name.starts_with(b"data/") {
                *bytes.entry(kind).or_default() += (key.len() + value.len()) as i64;
            }
        }
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
