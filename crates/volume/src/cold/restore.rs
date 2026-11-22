//! Restore after a DC is lost for good (#125 / S-17, `statefull/v2.md` §5 Cold tier and §7).
//!
//! The lost DC's range databases stay in the region's bucket. [`restore`] reads every one of its cells' databases in
//! hash order at once and records each actor once, with the newest meta any of them holds, in
//! `{first cell}/restored/{id}`: a database of meta keys, which [`restored`] reads back a page at a time. Memory stays a
//! few keys per cell whatever the number of actors.
//!
//! - **Older copies.** Range databases keep an actor's copy after it moves (S-05b purges a deleted actor only from its
//!   current first-slot database): a first-slot move inside the DC leaves an older copy in another of its cells, so only
//!   the highest sequence counts, and an actor that
//!   moved to another DC before this DC change is skipped, since the lost DC is neither its `dc_of` nor its
//!   `dc_before`.
//! - **Unfinished moves.** While a DC change runs, an actor leaving the lost DC (`dc_before`) is recorded too: its
//!   handover may not have imported its new slots. Its meta's `moved` is the first `MOVE` when S3 holds it, which is the
//!   point the handover imports at, so importing there finishes the move; without it the actor is imported at its
//!   flushed point like any other. A move that did finish imported that same point already, and repeating it changes
//!   nothing.
//! - **Nothing is copied here.** Once an actor's new slots start at its recorded sequence, its maintainer copies its keys
//!   from the lost cell's database on its next flush (the first-slot move in [`super::Cold`]), and cold reads find it
//!   there until then. Each actor loses what its slots had committed beyond the recorded sequence: its unflushed tail.

use std::collections::BTreeMap;
use std::sync::Arc;

use bedrock_lib::actors::{self, ActorId, Topology};
use bedrock_lib::storage::{ErrorContext, StorageError, StorageResult};
use bedrock_lib::volume as pb;

use super::db::{Database, Databases, database, keys};
use super::{Meta, meta_key, undecodable};

/// Actors recorded per commit, so a large DC's record is many small commits; a rerun writes the same keys again.
const RECORD_BATCH: usize = 10_000;

/// Restores lost DC `dc` of `topology` (the region's record, which still lists it, or lists it as a previous DC while a
/// DC change retires it) as restore `id`, and returns how many actors it recorded.
pub async fn restore(
    databases: &dyn Databases,
    topology: &Topology,
    dc: &str,
    id: &str,
) -> StorageResult<u64> {
    let cells = cells(topology, dc)?;
    let regions = BTreeMap::from([(topology.region.clone(), topology.clone())]);
    let mut cursors = Vec::with_capacity(cells.len());
    for cell in &cells {
        let mut cursor = Cursor::new(databases, cell).await?;
        cursor.advance(databases).await?;
        cursors.push(cursor);
    }
    let writer = databases.open(&record(&cells, id)).await?;
    let mut batch = Vec::new();
    let mut count = 0;
    // The lowest actor any cell holds next, with the newest meta among its copies.
    while let Some(lowest) = cursors
        .iter()
        .filter_map(|c| c.head.as_ref())
        .map(|h| h.0.clone())
        .min()
    {
        let mut newest: Option<(pb::Actor, Meta)> = None;
        for cursor in &mut cursors {
            if cursor.head.as_ref().is_some_and(|h| h.0 == lowest) {
                let (_, actor, meta) = cursor.head.take().expect("checked above");
                if newest
                    .as_ref()
                    .is_none_or(|(_, n)| (meta.seq, meta.epoch) > (n.seq, n.epoch))
                {
                    newest = Some((actor, meta));
                }
                cursor.advance(databases).await?;
            }
        }
        let (actor, meta) = newest.expect("some cursor held the lowest actor");
        let parsed = ActorId::parse(&regions, &actor.id)
            .map_err(|e| corrupt(format!("actor {}: {e}", actor.id)))?;
        let home = actors::dc_of(&parsed, topology).map_err(|e| corrupt(e.to_string()))?;
        let before = actors::dc_before(&parsed, topology).map_err(|e| corrupt(e.to_string()))?;
        if home.name != dc && before.is_none_or(|b| b.name != dc) {
            continue;
        }
        batch.push((meta_key(&lowest), Some(meta.encode())));
        count += 1;
        if batch.len() == RECORD_BATCH {
            writer.commit(std::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        writer.commit(batch).await?;
    }
    let _ = writer.close().await;
    for cursor in cursors {
        cursor.close().await;
    }
    Ok(count)
}

/// Up to `limit` actors restore `id` of `dc` recorded after `after` (`None`: from the first), in hash order.
pub async fn restored(
    databases: &dyn Databases,
    topology: &Topology,
    dc: &str,
    id: &str,
    after: Option<&pb::Actor>,
    limit: usize,
) -> StorageResult<Vec<(pb::Actor, Meta)>> {
    let Some(reader) = databases
        .open_reader(&record(&cells(topology, dc)?, id))
        .await?
    else {
        return Ok(Vec::new());
    };
    let mut from = after.map_or_else(Vec::new, |a| [name(a).as_slice(), b"n"].concat());
    let mut page = Vec::new();
    let mut listed = Ok(());
    while page.len() < limit {
        match next_meta(reader.as_ref(), &mut from, &[0xff; 9]).await {
            Ok(Some((_, actor, meta))) => page.push((actor, meta)),
            Ok(None) => break,
            Err(error) => {
                listed = Err(error);
                break;
            }
        }
    }
    let _ = reader.close().await;
    listed.map(|()| page)
}

/// The lost DC's cells, sorted: from its entries among the current DCs and the previous ones, which differ while a
/// change of its cells runs.
fn cells(topology: &Topology, dc: &str) -> StorageResult<Vec<String>> {
    let cells: std::collections::BTreeSet<String> = topology
        .dcs
        .iter()
        .chain(&topology.previous_dcs)
        .filter(|d| d.name == dc)
        .flat_map(|d| d.cells.iter().cloned())
        .collect();
    if cells.is_empty() {
        return Err(StorageError::Unsupported(context(format!(
            "the topology of {} lists no DC {dc}",
            topology.region
        ))));
    }
    Ok(cells.into_iter().collect())
}

/// The record of restore `id` of the DC with these (sorted) cells: under its first cell, whose name no other cell or
/// DC prefix of the bucket can take.
fn record(cells: &[String], id: &str) -> String {
    format!("{}/restored/{id}", cells[0])
}

/// A database being read and the keys `[from, to)` left to read in it.
type Reading = (Arc<dyn Database>, Vec<u8>, Vec<u8>);

/// One cell's actors in hash order: its catalog's spans in order, each read from the database serving it.
struct Cursor {
    spans: std::vec::IntoIter<(String, Vec<u8>, Vec<u8>)>,
    open: Option<Reading>,
    head: Option<(Vec<u8>, pb::Actor, Meta)>,
}

impl Cursor {
    async fn new(databases: &dyn Databases, cell: &str) -> StorageResult<Self> {
        let (catalog, _) = databases.catalog(cell).await?;
        let mut spans = Vec::new();
        for (start, (end, db)) in &catalog.spans {
            let (from, to) = keys(*start, *end);
            // Above every key of the last span: no hash is followed by 0xff, which UTF-8 never has.
            spans.push((
                database(cell, db),
                from,
                to.unwrap_or_else(|| vec![0xff; 9]),
            ));
        }
        Ok(Cursor {
            spans: spans.into_iter(),
            open: None,
            head: None,
        })
    }

    /// Moves `head` to the cell's next actor, opening each span's database in turn; `None` once there is no more.
    async fn advance(&mut self, databases: &dyn Databases) -> StorageResult<()> {
        self.head = None;
        loop {
            if self.open.is_none() {
                let Some((name, from, to)) = self.spans.next() else {
                    return Ok(());
                };
                // A span whose database was never created holds no actor.
                if let Some(reader) = databases.open_reader(&name).await? {
                    self.open = Some((reader, from, to));
                }
                continue;
            }
            let (reader, from, to) = self.open.as_mut().expect("opened above");
            if let Some(found) = next_meta(reader.as_ref(), from, to).await? {
                self.head = Some(found);
                return Ok(());
            }
            let (reader, _, _) = self.open.take().expect("opened above");
            let _ = reader.close().await;
        }
    }

    async fn close(self) {
        if let Some((reader, _, _)) = self.open {
            let _ = reader.close().await;
        }
    }
}

/// The next actor with a meta at or after `from` and before `to`, moving `from` past it; each actor's data keys are
/// skipped with one more scan.
// ponytail: one scan per actor (two when it has data keys); page the scans if restores of large DCs are too slow.
async fn next_meta(
    db: &dyn Database,
    from: &mut Vec<u8>,
    to: &[u8],
) -> StorageResult<Option<(Vec<u8>, pb::Actor, Meta)>> {
    loop {
        let Some((key, value)) = db.scan(from, to, 1).await?.into_iter().next() else {
            return Ok(None);
        };
        let (actor, prefix) =
            parse(&key).ok_or_else(|| corrupt("a range database key is not an actor key"))?;
        let actor_name = key[..prefix].to_vec();
        match &key[prefix..] {
            [b'm'] => {
                *from = [actor_name.as_slice(), b"n"].concat();
                let meta = Meta::decode(&value).ok_or_else(undecodable)?;
                return Ok(Some((actor_name, actor, meta)));
            }
            [b'd', ..] => *from = meta_key(&actor_name),
            _ => return Err(corrupt("a range database key is not an actor key")),
        }
    }
}

/// `{hash}{namespace}\0{id}\0`, as range databases key the actor.
fn name(actor: &pb::Actor) -> Vec<u8> {
    let hash = ActorId::hash_of(&actor.id).unwrap_or_default();
    [
        &hash.to_be_bytes()[..],
        actor.namespace.as_bytes(),
        &[0],
        actor.id.as_bytes(),
        &[0],
    ]
    .concat()
}

/// The actor a range database key belongs to and the length of its prefix. Range databases do not keep actor kinds,
/// so the kind is 0.
fn parse(key: &[u8]) -> Option<(pb::Actor, usize)> {
    let mut parts = key.get(8..)?.splitn(3, |b| *b == 0);
    let namespace = std::str::from_utf8(parts.next()?).ok()?;
    let id = std::str::from_utf8(parts.next()?).ok()?;
    parts.next()?;
    let actor = pb::Actor {
        namespace: namespace.into(),
        id: id.into(),
        kind: 0,
    };
    (name(&actor).get(..8) == key.get(..8)).then_some(())?;
    Some((actor, 8 + namespace.len() + id.len() + 2))
}

fn context(detail: String) -> ErrorContext {
    ErrorContext {
        engine: "volume",
        instance: String::new(),
        file: None,
        partition: None,
        detail,
    }
}

fn corrupt(detail: impl Into<String>) -> StorageError {
    StorageError::Corruption(context(detail.into()))
}
