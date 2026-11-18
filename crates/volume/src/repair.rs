//! Range copies and slot repair (#114 / S-06, `statefull/v2.md` §5 "Slot repair and range handoff",
//! `docs/spec/volume.tla` `Copy` and `Ready`, `docs/spec/handoff.tla` with `Reported`).
//!
//! [`Repair::tick`] walks this node's view of its cell's `volume` directory:
//! - **Range handoff.** A `populating` owner whose range has a `leaving` owner copies the range from it with one scan
//!   that first seals it there, so the copy holds every promise the leaving owner gave. It then holds the range, and its
//!   relay counts it ready from its report ([`Held`]).
//! - **Repair.** A `populating` owner with no leaving owner (the last one was removed), or a `ready` or `leaving` owner
//!   whose store started empty (it lost its disk), copies the range from the actor's other slots: one scan of each
//!   owner of the range in every cell of the DC, keeping per actor the more advanced log and the higher promise and
//!   reservation. Every copied actor, and every actor no slot had a record of, is then `repaired`: it takes claims and
//!   appends but counts towards no quorum until a claim above the reservation it copied commits (#131 / S-02b).
//! - **Bootstrap.** A store that started empty while the node owned no range is `fresh`: a range it is then given
//!   `ready` (a cell's first owners, `crates/relay` placement) has nothing anywhere to copy.
//! - **Revoke.** A range the node no longer owns, once another owner is `ready`, is deleted with its marks.
//!
//! A copy holds the range only if no held span was forgotten meanwhile ([`Store::keep`]): a range revoked and given
//! back during the copy may have been written elsewhere, so it is copied again.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bedrock_lib::actors;
use bedrock_lib::placement::{Directory, State};
use bedrock_lib::relays::client::Holdings;
use bedrock_lib::replication::pb::Span;
use bedrock_lib::volume as pb;
use prost::Message;
use tonic::Status;

use crate::serve::{blocking, status};
use crate::store::{self, Store};

/// Where a copy reads from: one owner's part `[start, end)` of the range (`end` 0 meaning 2⁶⁴).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub cell: String,
    pub node: String,
    pub start: u64,
    pub end: u64,
}

/// The actor's other slots, across cells. Implemented over the topology record and sibling cells' routes
/// ([`crate::cold::placement::Cells`]); tests stub it.
#[async_trait]
pub trait Peers: Send + Sync {
    /// Every `ready` or `leaving` owner of a part of `[start, end)` in each cell of this DC, this node excepted, each
    /// with its part. Fails while a part of another cell has no such owner.
    async fn sources(&self, start: u64, end: u64) -> Result<Vec<Source>, Status>;
    /// The cells of the actor's three slots.
    async fn slots(&self, actor: &pb::Actor) -> Result<[String; actors::SLOTS], Status>;
    /// One page of a source's records.
    async fn page(
        &self,
        source: &Source,
        request: pb::RepairRequest,
    ) -> Result<pb::RepairReply, Status>;
}

pub struct Repair {
    store: Arc<Store>,
    peers: Arc<dyn Peers>,
    node: String,
    cell: String,
}

impl Repair {
    pub fn new(store: Arc<Store>, peers: Arc<dyn Peers>, node: &str, cell: &str) -> Self {
        Repair {
            store,
            peers,
            node: node.to_owned(),
            cell: cell.to_owned(),
        }
    }

    /// One pass over `directory`, which must be a validated view, and its assignment `generations` (start → owner →
    /// generation). Returns whether a range became held, which the relay learns from a new report. A range that fails is
    /// logged and tried again next pass. The store must have decided whether it started fresh ([`decide`]).
    pub async fn tick(
        &self,
        directory: &Directory,
        generations: &BTreeMap<u64, BTreeMap<String, u64>>,
    ) -> Result<bool, Status> {
        if self.store.fresh().is_none() {
            return Err(Status::failed_precondition(
                "the store has not decided whether it started fresh",
            ));
        }
        let mut held = false;
        let mut entries = directory.iter().peekable();
        while let Some((&start, owners)) = entries.next() {
            let end = entries.peek().map_or(0, |(next, _)| **next);
            let generation = generations
                .get(&start)
                .and_then(|g| g.get(&self.node))
                .copied()
                .unwrap_or(0);
            match self.range(start, end, owners, generation).await {
                Ok(now) => held |= now,
                Err(error) => {
                    tracing::warn!(%error, start = %format!("{start:016x}"), "volume range copy failed; retrying");
                }
            }
        }
        Ok(held)
    }

    async fn range(
        &self,
        start: u64,
        end: u64,
        owners: &[bedrock_lib::placement::Owner],
        generation: u64,
    ) -> Result<bool, Status> {
        let held = self.store.held();
        let mine = owners.iter().find(|o| o.node == self.node).map(|o| o.state);
        match mine {
            None => {
                let marked = held.overlaps(start, end) || self.store.sealed().overlaps(start, end);
                if marked && owners.iter().any(|o| o.state == State::Ready) {
                    let store = self.store.clone();
                    blocking(move || store.clear(start, end))
                        .await?
                        .map_err(status)?;
                }
                return Ok(false);
            }
            // A copy made for another assignment of the range is stale: others may have written since.
            Some(_) if held.generation(start, end) == Some(generation) => {
                if mine == Some(State::Ready) && self.store.sealed().overlaps(start, end) {
                    let store = self.store.clone();
                    blocking(move || store.unseal(start, end))
                        .await?
                        .map_err(status)?;
                }
                return Ok(false);
            }
            _ => {}
        }
        self.store.copying(start, end, generation, true);
        let forgotten = self.store.forgotten();
        let copied = self.fill(start, end, owners, mine).await;
        self.store.copying(start, end, generation, false);
        copied?;
        let store = self.store.clone();
        let held = blocking(move || store.hold_unless_forgotten(start, end, generation, forgotten))
            .await?
            .map_err(status)?;
        if !held {
            return Ok(false);
        }
        tracing::info!(start = %format!("{start:016x}"), "holding a volume range");
        Ok(true)
    }

    /// Fills a range this node owns but does not hold yet.
    async fn fill(
        &self,
        start: u64,
        end: u64,
        owners: &[bedrock_lib::placement::Owner],
        mine: Option<State>,
    ) -> Result<(), Status> {
        match mine {
            Some(State::Populating) => {
                let leaving = owners
                    .iter()
                    .find(|o| o.state == State::Leaving && o.node != self.node);
                match leaving {
                    Some(leaving) => {
                        let source = Source {
                            cell: self.cell.clone(),
                            node: leaving.node.clone(),
                            start,
                            end,
                        };
                        self.copy(&[source], false, start, end).await?;
                    }
                    None => self.repair(start, end).await?,
                }
            }
            _ => {
                if self.store.fresh() != Some(true) || self.store.repaired().overlaps(start, end) {
                    self.repair(start, end).await?;
                }
            }
        }
        Ok(())
    }

    /// Copies `[start, end)` from the actor's other slots and leaves it repaired.
    async fn repair(&self, start: u64, end: u64) -> Result<(), Status> {
        let sources = self.peers.sources(start, end).await?;
        self.copy(&sources, true, start, end).await
    }

    /// Replaces what this node has of `[start, end)` with a copy of `sources`, one hash-ordered scan each.
    async fn copy(
        &self,
        sources: &[Source],
        repair: bool,
        start: u64,
        end: u64,
    ) -> Result<(), Status> {
        let store = self.store.clone();
        blocking(move || {
            store.clear(start, end)?;
            if repair {
                store.mark_repaired(start, end)?;
            }
            Ok::<_, store::Error>(())
        })
        .await?
        .map_err(status)?;
        for source in sources {
            let mut after = Vec::new();
            let mut actor: Vec<pb::Record> = Vec::new();
            loop {
                let request = pb::RepairRequest {
                    start: source.start,
                    end: source.end,
                    after: after.clone(),
                    limit: store::MAX_LIMIT,
                    seal: !repair,
                };
                let reply = self.peers.page(source, request).await?;
                if !repair {
                    for span in &reply.repaired {
                        let (store, span) = (self.store.clone(), *span);
                        blocking(move || store.mark_repaired(span.start, span.end))
                            .await?
                            .map_err(status)?;
                    }
                }
                if let Some(last) = reply.records.last() {
                    after = last.key.clone();
                }
                for record in reply.records {
                    let same = match (actor.first(), store::prefix_len(&record.key)) {
                        (Some(first), Some(at)) => first.key.get(..at) == record.key.get(..at),
                        _ => actor.is_empty(),
                    };
                    if !same {
                        self.take(source, std::mem::take(&mut actor), repair)
                            .await?;
                    }
                    actor.push(record);
                }
                if !reply.more {
                    break;
                }
            }
            self.take(source, actor, repair).await?;
        }
        let store = self.store.clone();
        blocking(move || store.adopt(start, end))
            .await?
            .map_err(status)?;
        Ok(())
    }

    /// Installs one actor's records, read again while they are torn: the source kept appending or truncating between
    /// the pages they came in. Gives up after a few tries; the range is copied again next pass.
    async fn take(
        &self,
        source: &Source,
        mut records: Vec<pb::Record>,
        repair: bool,
    ) -> Result<(), Status> {
        for _ in 0..8 {
            if consistent(&records)? {
                return self.install(source, records, repair).await;
            }
            records = self.reread(source, &records[0].key, repair).await?;
        }
        Err(Status::unavailable(
            "an actor kept changing while it was copied",
        ))
    }

    /// Every record of the actor whose key `first` is, from `source`.
    async fn reread(
        &self,
        source: &Source,
        first: &[u8],
        repair: bool,
    ) -> Result<Vec<pb::Record>, Status> {
        let at = store::prefix_len(first).expect("a copied record is an actor key");
        let prefix = first[..at].to_vec();
        let hash = u64::from_be_bytes(prefix[2..10].try_into().unwrap());
        let mut records = Vec::new();
        let mut after = prefix.clone();
        loop {
            let request = pb::RepairRequest {
                start: hash,
                end: hash.wrapping_add(1),
                after: after.clone(),
                limit: store::MAX_LIMIT,
                seal: !repair,
            };
            let reply = self.peers.page(source, request).await?;
            let more = reply.more;
            for record in reply.records {
                if !record.key.starts_with(&prefix) {
                    return Ok(records);
                }
                after = record.key.clone();
                records.push(record);
            }
            if !more {
                return Ok(records);
            }
        }
    }

    /// Installs one actor's records from `source`. A repair takes only actors that have a slot both here and there.
    async fn install(
        &self,
        source: &Source,
        records: Vec<pb::Record>,
        repair: bool,
    ) -> Result<(), Status> {
        let Some(at) = records.first().and_then(|r| store::prefix_len(&r.key)) else {
            return Ok(());
        };
        if repair {
            let Some(actor) = store::Actor::parse(&records[0].key[..at], 0) else {
                return Ok(());
            };
            let slots = self.peers.slots(&actor).await?;
            if !slots.contains(&source.cell) || !slots.contains(&self.cell) {
                return Ok(());
            }
        }
        let store = self.store.clone();
        blocking(move || store.install(&records, repair))
            .await?
            .map_err(status)
    }
}

/// Whether one actor's records agree with its claim: its log holds exactly the entries after the flushed point through
/// the log end, and no overlay record is newer than the log end.
fn consistent(records: &[pb::Record]) -> Result<bool, Status> {
    let Some(at) = records.first().and_then(|r| store::prefix_len(&r.key)) else {
        return Ok(true);
    };
    let corrupt =
        |e: prost::DecodeError| Status::data_loss(format!("a copied record does not decode: {e}"));
    let Some(claim) = records.iter().find(|r| r.key[at..] == *b"claim") else {
        return Ok(true);
    };
    let claim = pb::ClaimRecord::decode(claim.value.as_slice()).map_err(corrupt)?;
    let end = claim.end.map_or(claim.flushed, |e| e.seq);
    let mut seqs = Vec::new();
    for record in records {
        let name = &record.key[at..];
        if let Some(seq) = name.strip_prefix(b"log/") {
            seqs.push(u64::from_be_bytes(seq.try_into().unwrap_or_default()));
        } else if name.starts_with(b"data/") {
            let overlay = pb::OverlayRecord::decode(record.value.as_slice()).map_err(corrupt)?;
            if overlay.seq > end {
                return Ok(false);
            }
        }
    }
    Ok(seqs.into_iter().eq(claim.flushed + 1..=end))
}

/// What this node holds of the `volume` dataset, for the reports its relay counts readiness from.
pub struct Held(pub Arc<Store>);

impl Holdings for Held {
    fn settle(
        &self,
        _dataset: &str,
        spans: &[Span],
        generations: &[u64],
    ) -> Result<Vec<(Span, u64)>, Status> {
        let held = self.0.keep(&kept(spans, generations)).map_err(status)?;
        Ok(report(&held))
    }

    fn held(&self, _dataset: &str, spans: &[Span], generations: &[u64]) -> Vec<(Span, u64)> {
        report(&self.0.holding(&kept(spans, generations)))
    }
}

fn kept(spans: &[Span], generations: &[u64]) -> Vec<(u64, u64, u64)> {
    spans
        .iter()
        .enumerate()
        .map(|(i, s)| (s.start, s.end, generations.get(i).copied().unwrap_or(0)))
        .collect()
}

fn report(held: &store::Spans) -> Vec<(Span, u64)> {
    held.entries()
        .into_iter()
        .map(|(start, end, generation)| (Span { start, end }, generation))
        .collect()
}

/// Decides, once, whether the store started fresh: empty while `directory` (its cell's `volume` directory, read before
/// this node joins, when nothing can be given to it) lists the node nowhere. A store that starts empty while the node
/// owns a range lost its disk, and repairs it.
pub fn decide(store: &Store, directory: &Directory, node: &str) -> Result<(), store::Error> {
    if store.fresh().is_some() {
        return Ok(());
    }
    let owns = directory.values().flatten().any(|o| o.node == node);
    store.initialize(!owns)
}

#[cfg(test)]
#[path = "../tests/unit/repair.rs"]
mod tests;
