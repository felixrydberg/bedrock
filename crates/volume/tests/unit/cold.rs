use std::collections::BTreeMap;

use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection::Metrics;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;

use super::db::Memory;
use super::*;
use pb::Refusal;

const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

/// Actor `i`: a valid ID whose first four characters spell `i`.
fn actor(i: usize) -> pb::Actor {
    let digits: String = (0..4)
        .map(|d| ALPHABET[(i >> (5 * (3 - d))) & 31] as char)
        .collect();
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{digits}{}a", "c".repeat(21)),
        kind: 0,
    }
}

/// Every actor's first slot is in `cell`, this node's; truncations other slots owe are recorded.
#[derive(Default)]
struct Here {
    cell: String,
    truncated: Mutex<Vec<(String, u64)>>,
}

#[async_trait]
impl Placement for Here {
    async fn first_here(&self, _: &pb::Actor) -> Result<bool, Status> {
        Ok(true)
    }

    async fn truncate_others(&self, actor: &pb::Actor, through: u64) -> Result<bool, Status> {
        self.truncated
            .lock()
            .unwrap()
            .push((actor.id.clone(), through));
        Ok(true)
    }

    async fn first_cell(&self, _: &pb::Actor) -> Result<String, Status> {
        Ok(self.cell.clone())
    }
}

/// The first database of a cell's first range.
fn first_db() -> String {
    database(
        "cell-1",
        &Generation {
            start: 0,
            end: 0,
            generation: 0,
        },
    )
}

struct Node {
    store: Arc<Store>,
    cold: Cold,
    here: Arc<Here>,
    directory: Directory,
}

fn node(bucket: &Memory, name: &str, max_unflushed: u64) -> Node {
    node_in(bucket, "cell-1", name, max_unflushed)
}

fn node_in(bucket: &Memory, cell: &str, name: &str, max_unflushed: u64) -> Node {
    let store = Arc::new(
        Store::new(Arc::new(MemoryEngine::new("volume"))).with_max_unflushed(max_unflushed),
    );
    let here = Arc::new(Here {
        cell: cell.into(),
        ..Here::default()
    });
    let admin = Admin::new();
    let databases: Arc<dyn Databases> = Arc::new(bucket.clone());
    let readers = Readers::new(databases.clone(), 4, &Metrics::new(&admin));
    let cold = Cold::new(store.clone(), databases, here.clone(), readers, cell, name);
    let directory = BTreeMap::from([(
        0,
        vec![Owner {
            node: name.into(),
            state: State::Ready,
        }],
    )]);
    Node {
        store,
        cold,
        here,
        directory,
    }
}

fn holder() -> pb::Holder {
    pb::Holder {
        cell: "cell-1".into(),
        host: "h".into(),
    }
}

fn entry(seq: u64, changes: &[(&str, Option<&str>)]) -> pb::Entry {
    pb::Entry {
        seq,
        epoch: 1,
        changes: changes
            .iter()
            .map(|(k, v)| pb::Change {
                key: k.as_bytes().to_vec(),
                value: v.map(|v| v.as_bytes().to_vec()),
            })
            .collect(),
        ..Default::default()
    }
}

fn claim(store: &Store, i: usize) {
    let actor = Actor::new(&actor(i)).unwrap();
    let (answer, _) = store.claim(&actor, &holder(), 1, 0, |_| true).unwrap();
    assert_eq!(answer.refused, Refusal::None as i32);
}

/// Appends `entries` after `prev` and announces `commit`.
fn append(
    store: &Store,
    i: usize,
    prev: u64,
    entries: Vec<pb::Entry>,
    commit: u64,
) -> Result<pb::Answer, Error> {
    let actor = Actor::new(&actor(i)).unwrap();
    store
        .append(
            &actor,
            &pb::AppendRequest {
                holder: Some(holder()),
                epoch: 1,
                prev_epoch: u64::from(prev > 0),
                entries,
                commit,
                ..Default::default()
            },
        )
        .map(|outcome| outcome.answer)
}

fn flushed(store: &Store, i: usize) -> u64 {
    let actor = Actor::new(&actor(i)).unwrap();
    let (answer, _, _) = store.read(&actor, &[], 0).unwrap();
    answer.slot.unwrap().flushed
}

fn stored(bucket: &Memory, i: usize, key: &str) -> Option<Vec<u8>> {
    let name = Actor::new(&actor(i)).unwrap().name().to_vec();
    bucket
        .contents(&first_db())
        .get(&data_key(&name, key.as_bytes()))
        .cloned()
}

fn meta_of(bucket: &Memory, i: usize) -> Option<Meta> {
    let name = Actor::new(&actor(i)).unwrap().name().to_vec();
    bucket
        .contents(&first_db())
        .get(&meta_key(&name))
        .and_then(|bytes| Meta::decode(bytes))
}

#[test]
fn meta_round_trips_and_rejects_other_lengths() {
    for meta in [
        Meta::default(),
        Meta {
            seq: 7,
            epoch: 2,
            alarm: Some(99),
        },
    ] {
        assert_eq!(Meta::decode(&meta.encode()), Some(meta));
    }
    assert_eq!(Meta::decode(&[0; 17]), None);
}

/// One flush writes every changed actor of a range in one batch, so S3 writes per interval do not grow with actors.
#[tokio::test]
async fn one_commit_per_range_flush_whatever_the_number_of_actors() {
    for actors in [1, 50] {
        let bucket = Memory::default();
        let n = node(&bucket, "n1", u64::MAX);
        for i in 0..actors {
            claim(&n.store, i);
            append(&n.store, i, 0, vec![entry(1, &[("k", Some("v"))])], 1).unwrap();
        }
        n.cold.tick(&n.directory).await;
        assert_eq!(bucket.commits(), 1, "{actors} actors");
        for i in 0..actors {
            assert_eq!(flushed(&n.store, i), 1);
            assert_eq!(stored(&bucket, i, "k"), Some(b"v".to_vec()));
        }
        assert_eq!(n.here.truncated.lock().unwrap().len(), actors);
        // Nothing changed since: the next round writes nothing.
        n.cold.tick(&n.directory).await;
        assert_eq!(bucket.commits(), 1);
    }
}

/// Only committed entries reach S3, as of the commit point, even when the overlay already holds a newer value.
#[tokio::test]
async fn nothing_past_the_commit_point_reaches_s3() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    let entries = vec![
        entry(1, &[("a", Some("1")), ("b", Some("1"))]),
        entry(2, &[("a", Some("2"))]),
        entry(3, &[("a", Some("3")), ("c", Some("3"))]),
    ];
    append(&n.store, 0, 0, entries, 2).unwrap();
    n.cold.tick(&n.directory).await;
    assert_eq!(stored(&bucket, 0, "a"), Some(b"2".to_vec()));
    assert_eq!(stored(&bucket, 0, "c"), None);
    assert_eq!(meta_of(&bucket, 0).unwrap().seq, 2);
    assert_eq!(flushed(&n.store, 0), 2);
    // The overlay keeps entry 3's value until it is flushed.
    let actor0 = Actor::new(&actor(0)).unwrap();
    let (_, values, missing) = n
        .store
        .read(&actor0, &[b"a".to_vec(), b"b".to_vec()], 0)
        .unwrap();
    assert_eq!(values[0].value, Some(b"3".to_vec()));
    assert_eq!(missing, vec![1]);
    // A delete reaches S3 as a delete.
    append(&n.store, 0, 3, vec![entry(4, &[("b", None)])], 4).unwrap();
    n.cold.tick(&n.directory).await;
    assert_eq!(stored(&bucket, 0, "b"), None);
    assert_eq!(stored(&bucket, 0, "c"), Some(b"3".to_vec()));
    assert_eq!(meta_of(&bucket, 0).unwrap().seq, 4);
}

/// A flush whose answer is lost after it became durable is written again unchanged, and the slot truncates once.
#[tokio::test]
async fn a_crash_between_flush_and_truncation_applies_nothing_twice() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    append(&n.store, 0, 0, vec![entry(1, &[("a", Some("1"))])], 1).unwrap();
    bucket.fail(1, true);
    n.cold.tick(&n.directory).await;
    assert_eq!(
        flushed(&n.store, 0),
        0,
        "no truncation without a durable answer"
    );
    let before = bucket.contents(&first_db());
    // A restarted maintainer knows only what S3 holds.
    let restarted = Cold::new(
        n.store.clone(),
        Arc::new(bucket.clone()),
        n.here.clone(),
        Readers::new(Arc::new(bucket.clone()), 4, &Metrics::new(&Admin::new())),
        "cell-1",
        "n1",
    );
    restarted.tick(&n.directory).await;
    assert_eq!(flushed(&n.store, 0), 1);
    // Its meta already holds the entry, so the slot only truncates: nothing is written again.
    assert_eq!(bucket.commits(), 1);
    assert_eq!(bucket.contents(&first_db()), before);
    // The entry is gone from the log, so nothing applies it again.
    restarted.tick(&n.directory).await;
    assert_eq!(bucket.contents(&first_db()), before);
    assert_eq!(bucket.commits(), 1);
}

/// While S3 fails, the log grows only to its bound and then appends are refused; nothing acknowledged is lost, and
/// appends resume once a flush lands.
#[tokio::test]
async fn a_slow_s3_bounds_the_log_with_backpressure() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", 2_000);
    claim(&n.store, 0);
    bucket.fail(u64::MAX, false);
    let mut seq = 0;
    let refused = loop {
        let value = format!("{seq:0100}");
        match append(
            &n.store,
            0,
            seq,
            vec![entry(seq + 1, &[("k", Some(&value))])],
            seq + 1,
        ) {
            Ok(_) => seq += 1,
            Err(error) => break error,
        }
        n.cold.tick(&n.directory).await;
        assert!(seq < 100, "the log was not bounded");
    };
    assert!(matches!(refused, Error::Backpressure));
    assert!(n.store.unflushed() >= 2_000);
    bucket.fail(0, false);
    n.cold.tick(&n.directory).await;
    assert_eq!(flushed(&n.store, 0), seq);
    assert_eq!(n.store.unflushed(), 0);
    let last = format!("{:0100}", seq - 1);
    assert_eq!(stored(&bucket, 0, "k"), Some(last.into_bytes()));
    append(&n.store, 0, seq, vec![entry(seq + 1, &[])], seq + 1).unwrap();
}

/// After truncation a slot reads flushed keys from the range database, and scans merge it under the overlay.
#[tokio::test]
async fn cold_reads_and_scans_merge_the_range_database_under_the_overlay() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    let first = [
        ("a", Some("1")),
        ("b", Some("1")),
        ("c", Some("1")),
        ("d", Some("1")),
    ];
    append(&n.store, 0, 0, vec![entry(1, &first)], 1).unwrap();
    n.cold.tick(&n.directory).await;
    append(
        &n.store,
        0,
        1,
        vec![entry(2, &[("b", None), ("e", Some("2"))])],
        2,
    )
    .unwrap();
    let got = n
        .cold
        .get("cell-1", &actor(0), 1, &[b"a".to_vec()])
        .await
        .unwrap();
    assert_eq!(got, vec![Some(b"1".to_vec())]);
    // A reader behind the slot's flushed point refuses rather than answering old state.
    let behind = n.cold.get("cell-1", &actor(0), 5, &[b"a".to_vec()]).await;
    assert_eq!(behind.unwrap_err().code(), tonic::Code::Unavailable);

    let actor0 = Actor::new(&actor(0)).unwrap();
    let mut keys = Vec::new();
    let mut start = Vec::new();
    loop {
        let (_, overlay, more) = n.store.scan(&actor0, &start, 2, 0).unwrap();
        let (stored, stored_more) = n
            .cold
            .scan("cell-1", &actor(0), 1, &start, 2)
            .await
            .unwrap();
        let (values, more, next) = merge(overlay, more, stored, stored_more, 2);
        keys.extend(
            values
                .iter()
                .map(|v| String::from_utf8(v.key.clone()).unwrap()),
        );
        if !more {
            break;
        }
        start = match values.last() {
            Some(last) => [last.key.as_slice(), &[0]].concat(),
            None => next,
        };
    }
    assert_eq!(keys, ["a", "c", "d", "e"]);
}

#[test]
fn a_merge_stops_where_a_page_with_more_ends() {
    let value = |k: &str, v: Option<&str>| pb::Value {
        key: k.into(),
        value: v.map(Into::into),
    };
    let stored = |keys: &[&str]| {
        keys.iter()
            .map(|k| (k.as_bytes().to_vec(), b"s".to_vec()))
            .collect()
    };
    // The overlay page ends at "b" with more after it: "c" from S3 is not yet certain.
    let (values, more, next) = merge(
        vec![value("a", None), value("b", Some("o"))],
        true,
        stored(&["a", "c"]),
        false,
        10,
    );
    assert_eq!(values, vec![value("b", Some("o"))]);
    assert!(more);
    assert_eq!(next, b"b\0");
    // Deletes hide every certain key: nothing to return, but the next page starts past them.
    let (values, more, next) = merge(vec![value("a", None)], true, stored(&["a"]), false, 10);
    assert!(values.is_empty() && more);
    assert_eq!(next, b"a\0");
    let (values, more, _) = merge(Vec::new(), false, stored(&["a", "b", "c"]), false, 2);
    assert_eq!((values.len(), more), (2, true));
}

/// Opening the database elsewhere fences this maintainer, which then waits before opening it again.
#[tokio::test]
async fn a_fenced_maintainer_stops_writing_and_waits() {
    let bucket = Memory::default();
    let old = node(&bucket, "n1", u64::MAX);
    let new = node(&bucket, "n2", u64::MAX);
    for n in [&old, &new] {
        claim(&n.store, 0);
        append(&n.store, 0, 0, vec![entry(1, &[("a", Some("1"))])], 1).unwrap();
    }
    old.cold.tick(&old.directory).await;
    assert_eq!(bucket.commits(), 1);
    new.cold.tick(&new.directory).await;
    // The new owner rewrote nothing it already held.
    assert_eq!(bucket.commits(), 1);
    append(&old.store, 0, 1, vec![entry(2, &[("a", Some("2"))])], 2).unwrap();
    old.cold.tick(&old.directory).await;
    assert_eq!(stored(&bucket, 0, "a"), Some(b"1".to_vec()));
    assert_eq!(flushed(&old.store, 0), 1);
    // Within the wait it does not reopen, so it cannot fence the new owner back.
    old.cold.tick(&old.directory).await;
    assert_eq!(stored(&bucket, 0, "a"), Some(b"1".to_vec()));
}

/// A range this node no longer maintains is not flushed; its writer is closed.
#[tokio::test]
async fn only_the_maintainer_flushes() {
    let bucket = Memory::default();
    let mut n = node(&bucket, "n1", u64::MAX);
    n.directory = BTreeMap::from([(
        0,
        vec![
            Owner {
                node: "n0".into(),
                state: State::Populating,
            },
            Owner {
                node: "n2".into(),
                state: State::Leaving,
            },
            Owner {
                node: "n1".into(),
                state: State::Ready,
            },
        ],
    )]);
    claim(&n.store, 0);
    append(&n.store, 0, 0, vec![entry(1, &[("a", Some("1"))])], 1).unwrap();
    n.cold.tick(&n.directory).await;
    assert_eq!(bucket.commits(), 0);
    assert_eq!(maintainer(&n.directory[&0]), Some("n2"));
}

/// Against real SlateDB over an in-memory bucket that counts requests.
fn generation(start: u64, end: u64, generation: u64) -> Generation {
    Generation {
        start,
        end,
        generation,
    }
}

fn create(plan: Plan) -> (Catalog, Generation, Vec<(Generation, u64, u64)>) {
    match plan {
        Plan::Create {
            catalog,
            db,
            sources,
        } => (catalog, db, sources),
        Plan::Open(db) => panic!("expected a new database, got {db:?}"),
    }
}

/// A span opens its own database, or clones one from the databases serving it: a split leaves the other half reading
/// the old one, a merge takes every part, and a retired database is cloned onto itself.
#[test]
fn plans_open_split_merge_and_takeover() {
    let middle = 1 << 63;
    let (one, db, sources) = create(plan(&Catalog::default(), 0, 0, false));
    assert_eq!((db, sources), (generation(0, 0, 0), Vec::new()));
    assert_eq!(plan(&one, 0, 0, false), Plan::Open(db));
    assert_eq!(Catalog::decode(&one.encode()), Some(one.clone()));

    let (split, db, sources) = create(plan(&one, 0, middle, false));
    assert_eq!(db, generation(0, middle, 1));
    assert_eq!(sources, vec![(generation(0, 0, 0), 0, middle)]);
    assert_eq!(
        split.spans,
        BTreeMap::from([
            (0, (middle, generation(0, middle, 1))),
            (middle, (0, generation(0, 0, 0))),
        ])
    );
    assert_eq!(Catalog::decode(&split.encode()), Some(split.clone()));
    // The upper half reads the old database until it clones its own.
    let (halves, _, sources) = create(plan(&split, middle, 0, false));
    assert_eq!(sources, vec![(generation(0, 0, 0), middle, 0)]);

    let (merged, db, sources) = create(plan(&halves, 0, 0, false));
    assert_eq!(db, generation(0, 0, 3));
    assert_eq!(
        sources,
        vec![
            (generation(0, middle, 1), 0, middle),
            (generation(middle, 0, 2), middle, 0),
        ]
    );
    assert_eq!(merged.spans, BTreeMap::from([(0, (0, db))]));

    let (_, db, sources) = create(plan(&merged, 0, 0, true));
    assert_eq!(
        (db, sources),
        (generation(0, 0, 4), vec![(generation(0, 0, 3), 0, 0)])
    );
}

/// Retries a cold read once: the first read after a split may go to the database a cached catalog named, which is
/// behind; it fails, and the retry reads the new catalog.
async fn read(cold: &Cold, i: usize, flushed: u64) -> Option<Vec<u8>> {
    let keys = [b"k".to_vec()];
    let got = match cold.get("cell-1", &actor(i), flushed, &keys).await {
        Ok(got) => got,
        Err(_) => cold.get("cell-1", &actor(i), flushed, &keys).await.unwrap(),
    };
    got.into_iter().next().unwrap()
}

/// The acceptance check: a split and a merge by clone leave every actor readable at its flushed sequence, through the
/// maintainer and through another node's readers, and later flushes land in the clones.
#[tokio::test(start_paused = true)]
async fn a_split_or_merge_by_clone_leaves_every_actor_readable() {
    let bucket = Memory::default();
    let mut n = node(&bucket, "n1", u64::MAX);
    let reader = node(&bucket, "n2", u64::MAX);
    let actors = 8;
    let whole = n.directory.clone();
    let middle = Actor::new(&actor(actors / 2)).unwrap().hash();
    let mut split = whole.clone();
    split.insert(middle, whole[&0].clone());
    for i in 0..actors {
        claim(&n.store, i);
    }
    for (round, directory) in [whole.clone(), split, whole].into_iter().enumerate() {
        n.directory = directory;
        let seq = round as u64;
        for i in 0..actors {
            let value = format!("{i}-{round}");
            let entries = vec![entry(seq + 1, &[("k", Some(&value))])];
            append(&n.store, i, seq, entries, seq + 1).unwrap();
        }
        n.cold.tick(&n.directory).await;
        for i in 0..actors {
            assert_eq!(flushed(&n.store, i), seq + 1);
            let value = Some(format!("{i}-{round}").into_bytes());
            assert_eq!(read(&n.cold, i, seq + 1).await, value, "round {round}");
            assert_eq!(read(&reader.cold, i, seq + 1).await, value, "round {round}");
        }
    }
    // Clones copy no data: each flush is one commit per range, plus one retire mark per source.
    assert_eq!(bucket.commits(), 1 + (2 + 2) + (2 + 1));
}

/// A maintainer that has not seen a split yet cannot lose a flush: the clone fenced and retired its database, so its
/// flush fails and its slot keeps the entry until a database that every read finds holds it.
#[tokio::test(start_paused = true)]
async fn a_late_flush_by_the_old_maintainer_is_fenced_not_lost() {
    let bucket = Memory::default();
    let old = node(&bucket, "n1", u64::MAX);
    let mut new = node(&bucket, "n2", u64::MAX);
    let middle = Actor::new(&actor(4)).unwrap().hash();
    new.directory.insert(middle, new.directory[&0].clone());
    for n in [&old, &new] {
        claim(&n.store, 0);
        append(&n.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    }
    old.cold.tick(&old.directory).await;
    append(&old.store, 0, 1, vec![entry(2, &[("k", Some("2"))])], 2).unwrap();
    new.cold.tick(&new.directory).await;
    old.cold.tick(&old.directory).await;
    assert_eq!(flushed(&old.store, 0), 1, "the late flush failed");
    // Restarted with the same stale directory, it does not write the retired database: it merges the halves into a
    // new one, which holds the entry.
    let restarted = Cold::new(
        old.store.clone(),
        Arc::new(bucket.clone()),
        old.here.clone(),
        Readers::new(Arc::new(bucket.clone()), 4, &Metrics::new(&Admin::new())),
        "cell-1",
        "n1",
    );
    restarted.tick(&old.directory).await;
    assert_eq!(flushed(&old.store, 0), 2);
    let reader = node(&bucket, "n3", u64::MAX);
    assert_eq!(read(&reader.cold, 0, 2).await, Some(b"2".to_vec()));
}

/// When an actor's first slot moves to another cell, that cell's maintainer copies the actor's keys from the database
/// that held it, under its pending changes, and its slot then truncates as usual.
#[tokio::test(start_paused = true)]
async fn a_first_slot_move_copies_the_actor_into_the_new_cell() {
    let bucket = Memory::default();
    let before = node_in(&bucket, "cell-1", "n1", u64::MAX);
    let after = node_in(&bucket, "cell-2", "n2", u64::MAX);
    let first = [("k", Some("1")), ("x", Some("1")), ("y", Some("1"))];
    for n in [&before, &after] {
        claim(&n.store, 0);
        append(&n.store, 0, 0, vec![entry(1, &first)], 1).unwrap();
    }
    before.cold.tick(&before.directory).await;
    // The other slots truncate when the maintainer asks.
    let actor0 = Actor::new(&actor(0)).unwrap();
    after.store.truncate(&actor0, 1).unwrap();
    // The topology changes: the first slot is now in cell-2, whose database does not hold the actor.
    for n in [&before, &after] {
        append(
            &n.store,
            0,
            1,
            vec![entry(2, &[("k", Some("2")), ("x", None)])],
            2,
        )
        .unwrap();
    }
    after.cold.tick(&after.directory).await;
    assert_eq!(flushed(&after.store, 0), 2);
    let cell2 = bucket.contents(&database("cell-2", &generation(0, 0, 0)));
    let name = actor0.name();
    assert_eq!(cell2.get(&data_key(name, b"k")), Some(&b"2".to_vec()));
    assert_eq!(cell2.get(&data_key(name, b"x")), None);
    assert_eq!(cell2.get(&data_key(name, b"y")), Some(&b"1".to_vec()));
    assert_eq!(Meta::decode(&cell2[&meta_key(name)]).unwrap().seq, 2);
    let got = after
        .cold
        .get("cell-2", &actor(0), 2, &[b"y".to_vec()])
        .await
        .unwrap();
    assert_eq!(got, vec![Some(b"1".to_vec())]);
}

#[cfg(not(madsim))]
mod slate {
    use std::fmt;
    use std::ops::Range;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, Ordering};

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    };
    use tokio_stream::Stream;

    use bedrock_lib::storage::namespace::Changes;

    use super::super::db::Slate;
    use super::*;

    type Listing = Pin<Box<dyn Stream<Item = Result<ObjectMeta>> + Send>>;

    /// Requests by kind: puts, gets, lists, copies, deletes.
    #[derive(Debug, Default)]
    struct Counting {
        inner: InMemory,
        puts: AtomicU64,
        gets: AtomicU64,
        lists: AtomicU64,
        copies: AtomicU64,
        deletes: AtomicU64,
    }

    impl Counting {
        fn requests(&self) -> [u64; 5] {
            [
                &self.puts,
                &self.gets,
                &self.lists,
                &self.copies,
                &self.deletes,
            ]
            .map(|c| c.load(Ordering::SeqCst))
        }
    }

    impl fmt::Display for Counting {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Counting")
        }
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(
            &self,
            at: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.put_opts(at, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            at: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(at, opts).await
        }

        async fn get_opts(&self, at: &Path, options: GetOptions) -> Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(at, options).await
        }

        async fn get_ranges(
            &self,
            at: &Path,
            ranges: &[Range<u64>],
        ) -> Result<Vec<slatedb::bytes::Bytes>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get_ranges(at, ranges).await
        }

        fn delete_stream(
            &self,
            locations: Pin<Box<dyn Stream<Item = Result<Path>> + Send>>,
        ) -> Pin<Box<dyn Stream<Item = Result<Path>> + Send>> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> Listing {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.copies.fetch_add(1, Ordering::SeqCst);
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn slate_node(store: &Arc<Counting>, name: &str) -> Node {
        let databases: Arc<dyn Databases> = Arc::new(Slate::new(store.clone(), None));
        let n = node(&Memory::default(), name, u64::MAX);
        let readers = Readers::new(databases.clone(), 4, &Metrics::new(&Admin::new()));
        Node {
            cold: Cold::new(
                n.store.clone(),
                databases,
                n.here.clone(),
                readers,
                "cell-1",
                name,
            ),
            ..n
        }
    }

    /// Writes one change to each of `actors` actors (after `seq` earlier ones) and flushes; returns the requests the
    /// flush made.
    async fn round(n: &Node, store: &Counting, actors: usize, seq: u64) -> [u64; 5] {
        for i in 0..actors {
            if seq == 0 {
                claim(&n.store, i);
            }
            let value = format!("{i}-{seq}");
            append(
                &n.store,
                i,
                seq,
                vec![entry(seq + 1, &[("k", Some(&value))])],
                seq + 1,
            )
            .unwrap();
        }
        let before = store.requests();
        n.cold.tick(&n.directory).await;
        let after = store.requests();
        std::array::from_fn(|k| after[k] - before[k])
    }

    /// On SlateDB, a split and a merge by clone copy no object and keep every actor readable at its flushed sequence,
    /// through the maintainer and through another node's readers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_split_and_merge_by_clone_on_slatedb_keep_every_actor_readable() {
        let store = Arc::new(Counting::default());
        let mut n = slate_node(&store, "n1");
        let actors = 4;
        let whole = n.directory.clone();
        let mut split = whole.clone();
        split.insert(
            Actor::new(&actor(actors / 2)).unwrap().hash(),
            whole[&0].clone(),
        );
        for (step, directory) in [whole.clone(), split, whole].into_iter().enumerate() {
            n.directory = directory;
            let seq = step as u64;
            round(&n, &store, actors, seq).await;
            let reader = slate_node(&store, "n2");
            for i in 0..actors {
                let want = Some(format!("{i}-{seq}").into_bytes());
                assert_eq!(read(&n.cold, i, seq + 1).await, want, "step {step}");
                assert_eq!(read(&reader.cold, i, seq + 1).await, want, "step {step}");
            }
        }
        assert_eq!(store.requests()[3], 0, "a clone copies no object");
    }

    /// The acceptance check: S3 requests per flush do not grow with the actors in a range. One flush is one L0 file
    /// and one manifest update; measuring sizes reads the manifest the writer holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_requests_per_flush_do_not_grow_with_actors() {
        let mut writes = Vec::new();
        for actors in [2, 200] {
            let store = Arc::new(Counting::default());
            let n = slate_node(&store, "n1");
            round(&n, &store, actors, 0).await;
            let steady = round(&n, &store, actors, 1).await;
            assert_eq!(steady[3], 0, "no copies");
            writes.push(steady[0]);
            assert!(
                steady.iter().sum::<u64>() <= 8,
                "{actors} actors made {steady:?}"
            );
            for i in 0..actors {
                assert_eq!(flushed(&n.store, i), 2);
            }
            let before = store.requests();
            assert!(n.cold.size(0, 0) > 0);
            assert_eq!(store.requests(), before, "sizes come from the manifest");
        }
        assert_eq!(writes[0], writes[1]);
    }

    /// Measurements for `statefull/v2.md` section 12 questions 2 and 3, per range database: S3 requests to open a writer
    /// and a reader, per flush, and per cold read without a cache; and bytes written per flush when a SQLite actor
    /// stores 4 KiB pages or 256 KiB chunks. Run with `--ignored --nocapture`; the numbers are published on #113.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "a measurement, not a check"]
    async fn measure_range_database_costs() {
        let delta = |before: [u64; 5], after: [u64; 5]| -> [u64; 5] {
            std::array::from_fn(|k| after[k] - before[k])
        };
        println!("requests as [puts, gets, lists, copies, deletes]");
        let store = Arc::new(Counting::default());
        let databases = Slate::new(store.clone(), None);
        let name = first_db();
        let before = store.requests();
        let writer = databases.open(&name).await.unwrap();
        println!(
            "open writer (new database): {:?}",
            delta(before, store.requests())
        );
        for actors in [1usize, 100, 10_000] {
            let changes: Changes = (0..actors)
                .flat_map(|i| {
                    let actor = Actor::new(&actor(i % 1024)).unwrap();
                    let name = [actor.name(), &(i as u64).to_be_bytes()].concat();
                    [
                        (data_key(&name, b"k"), Some(vec![7; 100])),
                        (meta_key(&name), Some(Meta::default().encode())),
                    ]
                })
                .collect();
            let before = store.requests();
            writer.commit(changes).await.unwrap();
            println!(
                "flush of {actors} actors with one 100-byte key each: {:?}",
                delta(before, store.requests())
            );
        }
        writer.close().await.unwrap();
        let before = store.requests();
        let writer = databases.open(&name).await.unwrap();
        println!(
            "open writer (existing database, fences the last): {:?}",
            delta(before, store.requests())
        );
        let before = store.requests();
        let reader = databases.open_reader(&name).await.unwrap().unwrap();
        println!(
            "open reader (managed checkpoint): {:?}",
            delta(before, store.requests())
        );
        let key = data_key(Actor::new(&actor(5)).unwrap().name(), b"missing");
        let before = store.requests();
        reader.get(&key).await.unwrap();
        println!(
            "cold get, no block cache: {:?}",
            delta(before, store.requests())
        );
        let before = store.requests();
        reader.close().await.unwrap();
        println!("close reader: {:?}", delta(before, store.requests()));
        writer.close().await.unwrap();

        // Question 3: one SQLite transaction touching 16 random 4 KiB pages of a 64 MiB database.
        for (label, unit) in [("4 KiB pages", 4usize << 10), ("256 KiB chunks", 256 << 10)] {
            let store = Arc::new(Counting::default());
            let databases = Slate::new(store.clone(), None);
            let writer = databases.open(&name).await.unwrap();
            let name = Actor::new(&actor(1)).unwrap().name().to_vec();
            let pages: Vec<u64> = (0..16u64)
                .map(|i| (i * 2_654_435_761) % (64 << 20 >> 12))
                .collect();
            let units: std::collections::BTreeSet<u64> =
                pages.iter().map(|p| p * 4096 / unit as u64).collect();
            let changes: Changes = units
                .iter()
                .map(|u| (data_key(&name, &u.to_be_bytes()), Some(vec![1; unit])))
                .collect();
            let bytes: usize = changes
                .iter()
                .map(|(_, v)| v.as_ref().map_or(0, Vec::len))
                .sum();
            let before = store.requests();
            writer.commit(changes).await.unwrap();
            println!(
                "{label}: 16 dirty pages write {} keys, {bytes} value bytes, requests {:?}",
                units.len(),
                delta(before, store.requests())
            );
            writer.close().await.unwrap();
        }
    }

    /// A new range owner opens the same database: no object is copied, the old writer is fenced, and readers see every
    /// actor at its flushed sequence. Closing a reader deletes no data.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handoff_copies_nothing_and_closing_a_reader_deletes_nothing() {
        let store = Arc::new(Counting::default());
        let old = slate_node(&store, "n1");
        round(&old, &store, 3, 0).await;
        let new = slate_node(&store, "n2");
        let before = store.requests();
        new.cold.tick(&new.directory).await;
        let moved = store.requests();
        assert_eq!(moved[3], before[3], "a handoff copies no S3 data");

        let databases = Slate::new(store.clone(), None);
        let name = first_db();
        let reader = databases.open_reader(&name).await.unwrap().unwrap();
        let key = |i: usize| data_key(Actor::new(&actor(i)).unwrap().name(), b"k");
        assert_eq!(reader.get(&key(1)).await.unwrap(), Some(b"1-0".to_vec()));
        reader.close().await.unwrap();
        let again = databases.open_reader(&name).await.unwrap().unwrap();
        for i in 0..3 {
            assert_eq!(
                again.get(&key(i)).await.unwrap(),
                Some(format!("{i}-0").into_bytes())
            );
        }
        again.close().await.unwrap();

        // The old writer is fenced and its next flush fails without truncating.
        append(&old.store, 0, 1, vec![entry(2, &[("k", Some("late"))])], 2).unwrap();
        old.cold.tick(&old.directory).await;
        assert_eq!(flushed(&old.store, 0), 1);
    }
}

/// A coalesced open that finishes after its handle was evicted reads through it, but never reinstalls it.
#[tokio::test]
async fn a_late_coalesced_open_does_not_reinstall_a_retired_handle() {
    let bucket = Memory::default();
    for name in ["a", "b"] {
        bucket.open(name).await.unwrap();
    }
    let readers = Readers::new(Arc::new(bucket.clone()), 1, &Metrics::new(&Admin::new()));
    let first = readers.get("cell-1", "a").await.unwrap().unwrap();
    // Opening `b` evicts `a`, whose handle the late caller still holds.
    readers.get("cell-1", "b").await.unwrap().unwrap();
    let (late, evicted) = readers.install("a", first.clone());
    assert!(Arc::ptr_eq(&late, &first) && !evicted);
    assert_eq!(readers.len(), 1);
    let fresh = readers.get("cell-1", "a").await.unwrap().unwrap();
    assert!(!Arc::ptr_eq(&fresh, &first), "a retired handle came back");
}

/// A range busier than [`FLUSH_ROUNDS`] batches leaves the rest dirty for the next round.
#[tokio::test]
async fn one_round_flushes_at_most_flush_rounds_batches_per_range() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    let big = "v".repeat(FLUSH_BYTES as usize);
    let actors = FLUSH_ROUNDS + 2;
    for i in 0..actors {
        claim(&n.store, i);
        append(&n.store, i, 0, vec![entry(1, &[("k", Some(&big))])], 1).unwrap();
    }
    n.cold.tick(&n.directory).await;
    assert_eq!(bucket.commits(), FLUSH_ROUNDS as u64);
    assert_eq!(n.store.dirty(0, 0).len(), actors - FLUSH_ROUNDS);
    n.cold.tick(&n.directory).await;
    assert!(n.store.dirty(0, 0).is_empty());
}

#[test]
fn a_merge_stopped_by_its_limit_resumes_after_the_last_key_it_consumed() {
    let stored = vec![
        (b"a".to_vec(), b"s".to_vec()),
        (b"b".to_vec(), b"s".to_vec()),
        (b"c".to_vec(), b"s".to_vec()),
    ];
    let deleted = pb::Value {
        key: b"b".to_vec(),
        value: None,
    };
    let (values, more, next) = merge(vec![deleted], false, stored, false, 1);
    assert_eq!(values.len(), 1);
    assert!(more);
    // `b` was consumed (a delete); `c` was not returned, so the next page starts right after `b`.
    assert_eq!(next, b"b\0");
}
