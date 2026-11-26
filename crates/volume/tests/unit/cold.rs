use std::collections::BTreeMap;

use bedrock_lib::admin::Admin;
use bedrock_lib::health::protection::Metrics;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::StorageResult;
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

/// The `i` [`actor`] was built from.
fn index(actor: &pb::Actor) -> usize {
    actor.id[3..7].bytes().fold(0, |i, byte| {
        (i << 5) | ALPHABET.iter().position(|c| *c == byte).unwrap()
    })
}

/// Every actor's first slot is here; the truncations of the first [`SETTLE_CONCURRENCY`] actors never finish.
#[derive(Default)]
struct Hanging {
    cell: String,
}

#[async_trait]
impl Placement for Hanging {
    async fn first_here(&self, _: &pb::Actor) -> Result<bool, Status> {
        Ok(true)
    }

    async fn truncate_others(&self, actor: &pb::Actor, _: u64) -> Result<bool, Status> {
        if index(actor) < SETTLE_CONCURRENCY {
            std::future::pending().await
        }
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
            moved: None,
        },
        Meta {
            seq: 7,
            epoch: 2,
            alarm: None,
            moved: Some((5, 2)),
        },
        Meta {
            seq: 7,
            epoch: 2,
            alarm: Some(99),
            moved: Some((5, 2)),
        },
    ] {
        assert_eq!(Meta::decode(&meta.encode()), Some(meta));
    }
    assert_eq!(Meta::decode(&[0; 17]), None);
}

/// S-17: a restore merges every cell of the lost DC, keeps each actor's newest copy (an older one stays behind after a
/// first-slot move), skips actors whose `dc_of` is another DC, tells data keys from metas (also data keys ending in
/// `m`), and records the actors so they read back a page at a time. An actor never flushed is not in S3.
#[tokio::test]
async fn a_restore_records_each_homed_actor_once_at_its_newest_copy() {
    use bedrock_lib::actors::{ActorId, Dc, Topology, dc_of};
    let bucket = Memory::default();
    let one = node_in(&bucket, "cell-1", "n1", u64::MAX);
    let two = node_in(&bucket, "cell-2", "n2", u64::MAX);
    for i in 0..6 {
        claim(&one.store, i);
        claim(&two.store, i);
    }
    for i in 0..5 {
        append(&one.store, i, 0, vec![entry(1, &[("km", Some("v"))])], 1).unwrap();
    }
    one.cold.tick(&one.directory).await;
    // Actor 0 moved to cell-2 and flushed further there; cell-1 keeps its older copy.
    let entries = vec![entry(1, &[("a", Some("1"))]), entry(2, &[("m", None)])];
    append(&two.store, 0, 0, entries, 2).unwrap();
    two.cold.tick(&two.directory).await;

    let topology = Topology {
        region: "eu".into(),
        dcs: vec![
            Dc {
                name: "dc-1".into(),
                cells: vec!["cell-2".into(), "cell-1".into()],
            },
            Dc {
                name: "dc-2".into(),
                cells: vec!["cell-9".into()],
            },
        ],
        ..Default::default()
    };
    let regions = BTreeMap::from([("eu".to_string(), topology.clone())]);
    let homed = |i: usize| {
        let id = ActorId::parse(&regions, &actor(i).id).unwrap();
        dc_of(&id, &topology).unwrap().name == "dc-1"
    };
    let mut want: Vec<(String, u64)> = (0..5)
        .filter(|i| homed(*i))
        .map(|i| (actor(i).id, if i == 0 { 2 } else { 1 }))
        .collect();
    want.sort_by_key(|(id, _)| ActorId::hash_of(id).unwrap());
    assert!(
        want.len() > 1 && want.len() < 5,
        "the filter keeps some actors: {want:?}"
    );

    let count = restore::restore(&bucket, &topology, "dc-1", "r1")
        .await
        .unwrap();
    assert_eq!(count, want.len() as u64);
    let all = restore::restored(&bucket, &topology, "dc-1", "r1", None, 100)
        .await
        .unwrap();
    let got: Vec<(String, u64)> = all.iter().map(|(a, m)| (a.id.clone(), m.seq)).collect();
    assert_eq!(got, want);
    assert!(all.iter().all(|(a, _)| a.namespace == actor(0).namespace));
    // Paged: one at a time after the last one read.
    let mut paged = Vec::new();
    let mut after = None;
    loop {
        let page = restore::restored(&bucket, &topology, "dc-1", "r1", after.as_ref(), 1)
            .await
            .unwrap();
        let Some((actor, meta)) = page.into_iter().next() else {
            break;
        };
        paged.push((actor.id.clone(), meta.seq));
        after = Some(actor);
    }
    assert_eq!(paged, want);
    assert!(
        restore::restored(&bucket, &topology, "dc-1", "r2", None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        restore::restore(&bucket, &topology, "dc-3", "r1")
            .await
            .is_err()
    );

    // While a DC change moves the other actors out of dc-1, their handovers may not have finished: dc-1's restore
    // records them too, at their flushed meta, with the first MOVE S3 holds.
    let leaving = (0..5).find(|i| !homed(*i)).unwrap();
    let mut moving = entry(2, &[]);
    moving.kind = pb::entry::Kind::Move as i32;
    append(&one.store, leaving, 1, vec![moving], 2).unwrap();
    one.cold.tick(&one.directory).await;
    let changing = Topology {
        previous_dcs: vec![topology.dcs[0].clone()],
        ..topology.clone()
    };
    // Each actor's ID, recorded sequence and first MOVE.
    type Recorded = (String, u64, Option<(u64, u64)>);
    let mut want: Vec<Recorded> = (0..5)
        .map(|i| {
            let seq = if i == 0 || i == leaving { 2 } else { 1 };
            (actor(i).id, seq, (i == leaving).then_some((2, 1)))
        })
        .collect();
    want.sort_by_key(|(id, _, _)| ActorId::hash_of(id).unwrap());
    assert_eq!(
        restore::restore(&bucket, &changing, "dc-1", "r3")
            .await
            .unwrap(),
        5
    );
    let got: Vec<Recorded> = restore::restored(&bucket, &changing, "dc-1", "r3", None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|(a, m)| (a.id, m.seq, m.moved))
        .collect();
    assert_eq!(got, want);
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

/// The response page budget applies across both tiers, with duplicate requested keys counted each time.
#[tokio::test]
async fn a_read_limits_the_combined_hot_and_cold_values() {
    use bedrock_lib::volume::volume_server::Volume;
    use tonic::Request;

    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    let large = "v".repeat(bedrock_lib::kv::MAX_VALUE);
    append(&n.store, 0, 0, vec![entry(1, &[("cold", Some(&large))])], 1).unwrap();
    n.cold.tick(&n.directory).await;
    append(&n.store, 0, 1, vec![entry(2, &[("hot", Some(&large))])], 2).unwrap();
    drop(large);
    let service = crate::serve::Service::new(&Admin::new(), n.store, "n1", "cell-1", "dc1")
        .unwrap()
        .with_cold(Arc::new(n.cold));
    *service.directory.write().unwrap() = n.directory;
    let keys = |count| {
        (0..count)
            .map(|i| {
                if i % 2 == 0 {
                    b"cold".to_vec()
                } else {
                    b"hot".to_vec()
                }
            })
            .collect()
    };
    // Every value is valid and each tier alone fits. Together ten 25 MiB values exceed PAGE_BYTES.
    let Err(error) = service
        .read(Request::new(pb::ReadRequest {
            actor: Some(actor(0)),
            keys: keys(10),
            min_seq: 2,
        }))
        .await
    else {
        panic!("combined hot and cold values must exceed the reply budget");
    };
    assert_eq!(error.code(), tonic::Code::OutOfRange);
    let reply = service
        .read(Request::new(pb::ReadRequest {
            actor: Some(actor(0)),
            keys: keys(2),
            min_seq: 2,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        reply
            .values
            .iter()
            .map(|value| value.key.as_slice())
            .collect::<Vec<_>>(),
        [b"cold".as_slice(), b"hot".as_slice()]
    );
    assert!(
        reply
            .values
            .iter()
            .all(|value| value.value.as_ref().unwrap().len() == bedrock_lib::kv::MAX_VALUE)
    );
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
        .get(
            "cell-1",
            &actor(0),
            1,
            &[b"a".to_vec()],
            crate::store::PAGE_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(got, vec![Some(b"1".to_vec())]);
    // A reader behind the slot's flushed point refuses rather than answering old state.
    let behind = n
        .cold
        .get(
            "cell-1",
            &actor(0),
            5,
            &[b"a".to_vec()],
            crate::store::PAGE_BYTES,
        )
        .await;
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
        let (values, more, next) = merge(
            overlay,
            more,
            stored,
            stored_more,
            2,
            bedrock_lib::kv::MAX_MESSAGE_BYTES,
        );
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
        bedrock_lib::kv::MAX_MESSAGE_BYTES,
    );
    assert_eq!(values, vec![value("b", Some("o"))]);
    assert!(more);
    assert_eq!(next, b"b\0");
    // Deletes hide every certain key: nothing to return, but the next page starts past them.
    let (values, more, next) = merge(
        vec![value("a", None)],
        true,
        stored(&["a"]),
        false,
        10,
        bedrock_lib::kv::MAX_MESSAGE_BYTES,
    );
    assert!(values.is_empty() && more);
    assert_eq!(next, b"a\0");
    let (values, more, _) = merge(
        Vec::new(),
        false,
        stored(&["a", "b", "c"]),
        false,
        2,
        bedrock_lib::kv::MAX_MESSAGE_BYTES,
    );
    assert_eq!((values.len(), more), (2, true));
}

#[test]
fn a_merged_scan_page_fits_its_encoded_reply_and_continues() {
    let answer = pb::Answer {
        slot: Some(pb::Slot {
            holder: Some(holder()),
            committed: 123,
            ..Default::default()
        }),
        ..Default::default()
    };
    let expected = pb::Value {
        key: b"a".to_vec(),
        value: Some(b"cold".to_vec()),
    };
    let envelope = pb::ScanReply {
        answer: Some(answer.clone()),
        more: true,
        ..Default::default()
    }
    .encoded_len();
    let budget = pb::ScanReply {
        answer: Some(answer.clone()),
        values: vec![expected.clone()],
        more: true,
    }
    .encoded_len();
    let overlay = pb::Value {
        key: b"b".to_vec(),
        value: Some(b"hot".to_vec()),
    };
    let (values, more, next) = merge(
        vec![overlay.clone()],
        false,
        vec![(b"a".to_vec(), b"cold".to_vec())],
        false,
        10,
        budget - envelope,
    );
    assert_eq!(values, vec![expected]);
    assert!(more);
    assert_eq!(next, b"a\0");
    assert_eq!(
        pb::ScanReply {
            answer: Some(answer),
            values,
            more
        }
        .encoded_len(),
        budget
    );
    let (rest, more, _) = merge(
        vec![overlay.clone()],
        false,
        Vec::new(),
        false,
        10,
        budget - envelope,
    );
    assert_eq!(rest, vec![overlay]);
    assert!(!more);

    // One maximum KV value proves the real wire sizes without retaining eleven copies: each tier's six/five
    // values fit PAGE_BYTES, while their combined reply exceeds the actual tonic limit.
    let value = pb::Value {
        key: b"k".to_vec(),
        value: Some(vec![0; bedrock_lib::kv::MAX_VALUE]),
    };
    let encoded = prost::encoding::message::encoded_len(2, &value);
    assert!(6 * (value.encoded_len() + 10) < crate::store::PAGE_BYTES);
    assert!(5 * (value.encoded_len() + 10) < crate::store::PAGE_BYTES);
    assert!(envelope + 11 * encoded > bedrock_lib::kv::MAX_MESSAGE_BYTES);
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
            rest: None,
        } => (catalog, db, sources),
        other => panic!("expected a new database alone, got {other:?}"),
    }
}

/// No database serves two spans of `catalog`, so a merge takes each source once.
fn each_once(catalog: &Catalog) {
    let mut seen = Vec::new();
    for (_, db) in catalog.spans.values() {
        assert!(!seen.contains(db), "{db:?} serves two spans of {catalog:?}");
        seen.push(*db);
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

/// A split inside one database's span clones the part above into its own database too, so a later merge of the whole
/// span takes three distinct sources instead of one database twice.
#[test]
fn an_interior_split_leaves_no_database_serving_two_spans() {
    let (low, high) = (1 << 62, 1 << 63);
    let (one, ..) = create(plan(&Catalog::default(), 0, 0, false));
    let Plan::Create {
        catalog: inner,
        db,
        sources,
        rest,
    } = plan(&one, low, high, false)
    else {
        panic!("a split creates a database");
    };
    assert_eq!(db, generation(low, high, 1));
    assert_eq!(sources, vec![(generation(0, 0, 0), low, high)]);
    assert_eq!(
        rest,
        Some((generation(high, 0, 2), (generation(0, 0, 0), high, 0)))
    );
    each_once(&inner);
    let (merged, db, sources) = create(plan(&inner, 0, 0, false));
    assert_eq!(db, generation(0, 0, 3));
    assert_eq!(
        sources,
        vec![
            (generation(0, 0, 0), 0, low),
            (generation(low, high, 1), low, high),
            (generation(high, 0, 2), high, 0),
        ]
    );
    each_once(&merged);
}

/// Retries a cold read once: the first read after a split may go to the database a cached catalog named, which is
/// behind; it fails, and the retry reads the new catalog.
async fn read(cold: &Cold, i: usize, flushed: u64) -> Option<Vec<u8>> {
    let keys = [b"k".to_vec()];
    let got = match cold
        .get(
            "cell-1",
            &actor(i),
            flushed,
            &keys,
            crate::store::PAGE_BYTES,
        )
        .await
    {
        Ok(got) => got,
        Err(_) => cold
            .get(
                "cell-1",
                &actor(i),
                flushed,
                &keys,
                crate::store::PAGE_BYTES,
            )
            .await
            .unwrap(),
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

/// A cached catalog can name a database a split has already retired; it never catches up, so a read reloads the
/// catalog and reads the database that serves the range now instead of waiting on the retired one.
#[tokio::test(start_paused = true)]
async fn a_stale_catalog_after_a_split_is_reloaded_not_waited_on() {
    let bucket = Memory::default();
    let mut n = node(&bucket, "n1", u64::MAX);
    let reader = node(&bucket, "n2", u64::MAX);
    let keys = [b"k".to_vec()];
    claim(&n.store, 0);
    append(&n.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    n.cold.tick(&n.directory).await;
    // The reader caches the whole range's catalog.
    assert_eq!(
        reader
            .cold
            .get("cell-1", &actor(0), 1, &keys, crate::store::PAGE_BYTES)
            .await
            .unwrap(),
        vec![Some(b"1".to_vec())]
    );
    // A split retires that database while the reader keeps its catalog: later flushes go to the clone.
    let middle = Actor::new(&actor(4)).unwrap().hash();
    let mut split = n.directory.clone();
    split.insert(middle, n.directory[&0].clone());
    n.directory = split;
    n.cold.tick(&n.directory).await;
    append(&n.store, 0, 1, vec![entry(2, &[("k", Some("2"))])], 2).unwrap();
    n.cold.tick(&n.directory).await;
    let started = bedrock_lib::monotonic_now();
    assert_eq!(
        reader
            .cold
            .get("cell-1", &actor(0), 2, &keys, crate::store::PAGE_BYTES)
            .await
            .unwrap(),
        vec![Some(b"2".to_vec())]
    );
    assert!(
        started.elapsed() < BEHIND_WAIT,
        "the read waited on the retired database"
    );
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
        .get(
            "cell-2",
            &actor(0),
            2,
            &[b"y".to_vec()],
            crate::store::PAGE_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(got, vec![Some(b"1".to_vec())]);
}

/// The in-memory bucket with one cell's catalog unreadable.
struct Broken {
    bucket: Memory,
    cell: &'static str,
}

#[async_trait]
impl Databases for Broken {
    async fn open(&self, name: &str) -> StorageResult<Arc<dyn Writer>> {
        self.bucket.open(name).await
    }

    async fn open_reader(&self, name: &str) -> StorageResult<Option<Arc<dyn Database>>> {
        self.bucket.open_reader(name).await
    }

    async fn catalog(&self, cell: &str) -> StorageResult<(Catalog, Option<String>)> {
        if cell == self.cell {
            return Err(StorageError::Unavailable(
                bedrock_lib::storage::ErrorContext {
                    engine: "test",
                    instance: cell.into(),
                    file: None,
                    partition: None,
                    detail: "the catalog is unreadable".into(),
                },
            ));
        }
        self.bucket.catalog(cell).await
    }

    async fn put_catalog(
        &self,
        cell: &str,
        catalog: &Catalog,
        version: Option<String>,
    ) -> StorageResult<bool> {
        self.bucket.put_catalog(cell, catalog, version).await
    }

    async fn create_clone(&self, name: &str, sources: &[(String, u64, u64)]) -> StorageResult<()> {
        self.bucket.create_clone(name, sources).await
    }

    async fn cells(&self) -> StorageResult<Vec<String>> {
        self.bucket.cells().await
    }
}

/// A cell that cannot be read does not hide another cell's copy of a moved actor; the failure only matters when no
/// cell qualified, so the caller retries instead of concluding that none holds the actor.
#[tokio::test]
async fn adopt_skips_a_cell_it_cannot_read_and_fails_only_when_no_cell_qualified() {
    let bucket = Memory::default();
    let parsed = Actor::new(&actor(0)).unwrap();
    let db = generation(0, 0, 0);
    let name = database("cell-3", &db);
    let meta = Meta {
        seq: 1,
        epoch: 1,
        alarm: None,
        moved: None,
    };
    let changes = vec![
        (meta_key(parsed.name()), Some(meta.encode())),
        (data_key(parsed.name(), b"k"), Some(b"v".to_vec())),
    ];
    let writer = bucket.open(&name).await.unwrap();
    writer.commit(changes).await.unwrap();
    writer.close().await.unwrap();
    // cell-2's catalog cannot be read; cell-3 holds the actor at sequence 1.
    bucket
        .put_catalog("cell-2", &Catalog::default(), None)
        .await
        .unwrap();
    let catalog = Catalog {
        next: 1,
        spans: BTreeMap::from([(0, (0, db))]),
    };
    bucket.put_catalog("cell-3", &catalog, None).await.unwrap();
    let databases: Arc<dyn Databases> = Arc::new(Broken {
        bucket: bucket.clone(),
        cell: "cell-2",
    });
    let readers = Readers::new(databases.clone(), 4, &Metrics::new(&Admin::new()));
    let cold = Cold::new(
        Arc::new(Store::new(Arc::new(MemoryEngine::new("volume")))),
        databases,
        Arc::new(Here::default()),
        readers,
        "cell-1",
        "n1",
    );
    let (found, keys) = cold.adopt(&parsed, 1).await.unwrap().unwrap();
    assert_eq!(found, meta);
    assert_eq!(keys, vec![(data_key(parsed.name(), b"k"), b"v".to_vec())]);
    // Nothing holds it at sequence 2: whether the unreadable cell does cannot be decided, so the caller retries.
    let missed = cold.adopt(&parsed, 2).await.unwrap_err();
    assert_eq!(missed.code(), tonic::Code::Unavailable);
}

/// The same for a read: a cell that cannot be read does not hide another cell's copy of a moved actor; the failure
/// only matters when no cell qualified, so the caller retries instead of concluding that none holds the actor.
#[tokio::test]
async fn a_moved_actor_read_skips_a_cell_it_cannot_read() {
    let bucket = Memory::default();
    let parsed = Actor::new(&actor(0)).unwrap();
    let db = generation(0, 0, 0);
    let name = database("cell-3", &db);
    let meta = Meta {
        seq: 1,
        epoch: 1,
        alarm: None,
        moved: None,
    };
    let writer = bucket.open(&name).await.unwrap();
    writer
        .commit(vec![
            (meta_key(parsed.name()), Some(meta.encode())),
            (data_key(parsed.name(), b"k"), Some(b"v".to_vec())),
        ])
        .await
        .unwrap();
    writer.close().await.unwrap();
    // cell-2's catalog cannot be read; cell-3 holds the actor at sequence 1.
    bucket
        .put_catalog("cell-2", &Catalog::default(), None)
        .await
        .unwrap();
    let catalog = Catalog {
        next: 1,
        spans: BTreeMap::from([(0, (0, db))]),
    };
    bucket.put_catalog("cell-3", &catalog, None).await.unwrap();
    let databases: Arc<dyn Databases> = Arc::new(Broken {
        bucket: bucket.clone(),
        cell: "cell-2",
    });
    let readers = Readers::new(databases.clone(), 4, &Metrics::new(&Admin::new()));
    let here = Arc::new(Here {
        cell: "cell-1".into(),
        ..Here::default()
    });
    let cold = Cold::new(
        Arc::new(Store::new(Arc::new(MemoryEngine::new("volume")))),
        databases,
        here,
        readers,
        "cell-1",
        "n1",
    );
    let keys = [b"k".to_vec()];
    let got = cold
        .get("cell-1", &actor(0), 1, &keys, crate::store::PAGE_BYTES)
        .await
        .unwrap();
    assert_eq!(got, vec![Some(b"v".to_vec())]);
    // Nothing holds it at sequence 2: whether the unreadable cell does cannot be decided, so the caller retries.
    let missed = cold
        .get("cell-1", &actor(0), 2, &keys, crate::store::PAGE_BYTES)
        .await
        .unwrap_err();
    assert_eq!(missed.code(), tonic::Code::Unavailable);
}

/// The same through the maintainer on the in-memory bucket, which refuses a repeated clone source as SlateDB does: an
/// interior split, then a merge of the whole range, keep every actor readable.
#[tokio::test(start_paused = true)]
async fn an_interior_split_then_a_full_merge_keep_every_actor_readable() {
    let bucket = Memory::default();
    let mut n = node(&bucket, "n1", u64::MAX);
    let actors = 6;
    let whole = n.directory.clone();
    let inner = interior(&whole, actors);
    for i in 0..actors {
        claim(&n.store, i);
    }
    for (round, directory) in [whole.clone(), inner, whole].into_iter().enumerate() {
        n.directory = directory;
        let seq = round as u64;
        for i in 0..actors {
            let value = format!("{i}-{round}");
            let entries = vec![entry(seq + 1, &[("k", Some(&value))])];
            append(&n.store, i, seq, entries, seq + 1).unwrap();
        }
        n.cold.tick(&n.directory).await;
        for i in 0..actors {
            // In the interior round only the middle range has a maintainer.
            let middle = (2..4).contains(&i);
            let at = if round == 1 && !middle { seq } else { seq + 1 };
            assert_eq!(flushed(&n.store, i), at, "round {round}, actor {i}");
        }
    }
    for i in 0..actors {
        assert_eq!(
            read(&n.cold, i, 3).await,
            Some(format!("{i}-2").into_bytes())
        );
    }
}

/// `whole` split at actors `actors / 3` and `2 * actors / 3`, with a maintainer for the middle range only: the outer
/// two keep reading the first database.
fn interior(whole: &Directory, actors: usize) -> Directory {
    let hash = |i: usize| Actor::new(&actor(i)).unwrap().hash();
    let (low, high) = (hash(actors / 3), hash(2 * actors / 3));
    let mut inner = whole.clone();
    inner.insert(low, whole[&0].clone());
    inner.insert(high, whole[&0].clone());
    for (start, owners) in inner.iter_mut() {
        if *start != low {
            owners[0].state = State::Populating;
        }
    }
    inner
}

/// A fenced writer found by reading an actor's meta, not only by a commit, leaves service and waits before reopening.
#[tokio::test(start_paused = true)]
async fn a_writer_fenced_on_a_meta_read_waits_before_reopening() {
    let bucket = Memory::default();
    let old = node(&bucket, "n1", u64::MAX);
    let new = node(&bucket, "n2", u64::MAX);
    claim(&old.store, 0);
    append(&old.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    old.cold.tick(&old.directory).await;
    // Another owner opens the database; then an actor this writer has not read yet changes.
    new.cold.tick(&new.directory).await;
    claim(&old.store, 1);
    append(&old.store, 1, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    old.cold.tick(&old.directory).await;
    assert_eq!(old.cold.size(0, 0), 0, "the fenced writer left service");
    assert_eq!(flushed(&old.store, 1), 0);
    tokio::time::sleep(FENCED_WAIT / 2).await;
    old.cold.tick(&old.directory).await;
    assert_eq!(old.cold.size(0, 0), 0, "no reopen within the wait");
    tokio::time::sleep(FENCED_WAIT).await;
    old.cold.tick(&old.directory).await;
    assert_eq!(flushed(&old.store, 1), 1, "reopened after the wait");
}

/// A read before the range database exists fails; once the maintainer creates it, a retry reads it, because the
/// empty catalog read first is not kept.
#[tokio::test(start_paused = true)]
async fn a_read_before_the_database_exists_succeeds_on_retry_once_it_does() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    let reader = node(&bucket, "n2", u64::MAX);
    let keys = [b"k".to_vec()];
    let early = reader
        .cold
        .get("cell-1", &actor(0), 1, &keys, crate::store::PAGE_BYTES)
        .await;
    assert_eq!(early.unwrap_err().code(), tonic::Code::Unavailable);
    claim(&n.store, 0);
    append(&n.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    n.cold.tick(&n.directory).await;
    let got = reader
        .cold
        .get("cell-1", &actor(0), 1, &keys, crate::store::PAGE_BYTES)
        .await
        .unwrap();
    assert_eq!(got, vec![Some(b"1".to_vec())]);
}

/// An actor that moved to another cell while idle has nothing to flush, so the new cell's database never copies it;
/// cold reads find it in the database of the cell that holds it.
#[tokio::test(start_paused = true)]
async fn an_idle_actor_whose_first_slot_moved_stays_readable() {
    let bucket = Memory::default();
    let before = node_in(&bucket, "cell-1", "n1", u64::MAX);
    let after = node_in(&bucket, "cell-2", "n2", u64::MAX);
    for n in [&before, &after] {
        claim(&n.store, 0);
        append(&n.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    }
    before.cold.tick(&before.directory).await;
    let actor0 = Actor::new(&actor(0)).unwrap();
    after.store.truncate(&actor0, 1).unwrap();
    // The first slot is now in cell-2, and no entry 2 follows: cell-2 has no database for the actor.
    after.cold.tick(&after.directory).await;
    let got = after
        .cold
        .get(
            "cell-2",
            &actor(0),
            1,
            &[b"k".to_vec()],
            crate::store::PAGE_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(got, vec![Some(b"1".to_vec())]);
    let (page, more) = after
        .cold
        .scan("cell-2", &actor(0), 1, b"", 10)
        .await
        .unwrap();
    assert_eq!((page, more), (vec![(b"k".to_vec(), b"1".to_vec())], false));
}

/// The cell of the actor's first slot, which the test moves.
struct Moving {
    cell: Mutex<String>,
}

#[async_trait]
impl Placement for Moving {
    async fn first_here(&self, _: &pb::Actor) -> Result<bool, Status> {
        Ok(true)
    }

    async fn truncate_others(&self, _: &pb::Actor, _: u64) -> Result<bool, Status> {
        Ok(true)
    }

    async fn first_cell(&self, _: &pb::Actor) -> Result<String, Status> {
        Ok(self.cell.lock().unwrap().clone())
    }
}

/// A catalog cached before a split names the database that split retired, which never catches up. When an idle actor's
/// first slot then moves, the new cell holds no copy: a read has to reread the source cell's catalog and find the clone
/// the split left holding the newer sequence, not give up on the retired database.
#[tokio::test(start_paused = true)]
async fn a_read_after_an_idle_move_rereads_a_catalog_cached_before_a_split() {
    let bucket = Memory::default();
    let mut before = node_in(&bucket, "cell-1", "n1", u64::MAX);
    claim(&before.store, 0);
    append(&before.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    before.cold.tick(&before.directory).await;
    // A read in cell-2 caches cell-1's catalog while the actor's first slot is still there.
    let placement = Arc::new(Moving {
        cell: Mutex::new("cell-1".into()),
    });
    let databases: Arc<dyn Databases> = Arc::new(bucket.clone());
    let readers = Readers::new(databases.clone(), 4, &Metrics::new(&Admin::new()));
    let moved = Cold::new(
        Arc::new(Store::new(Arc::new(MemoryEngine::new("volume")))),
        databases,
        placement.clone(),
        readers,
        "cell-2",
        "n2",
    );
    let keys = [b"k".to_vec()];
    assert_eq!(
        moved
            .get("cell-2", &actor(0), 1, &keys, crate::store::PAGE_BYTES)
            .await
            .unwrap(),
        vec![Some(b"1".to_vec())]
    );
    // The source range splits and its clone flushes a newer sequence.
    let middle = Actor::new(&actor(4)).unwrap().hash();
    let mut split = before.directory.clone();
    split.insert(middle, before.directory[&0].clone());
    before.directory = split;
    before.cold.tick(&before.directory).await;
    append(&before.store, 0, 1, vec![entry(2, &[("k", Some("2"))])], 2).unwrap();
    before.cold.tick(&before.directory).await;
    // The first slot moves while the actor is idle, so cell-2 holds no copy: only cell-1's clone now has it, while
    // the catalog cell-2 cached still names the database the split retired.
    *placement.cell.lock().unwrap() = "cell-2".into();
    assert_eq!(
        moved
            .get("cell-2", &actor(0), 2, &keys, crate::store::PAGE_BYTES)
            .await
            .unwrap(),
        vec![Some(b"2".to_vec())]
    );
}

/// Configured sibling relays must name exactly the other cells of this node's DC.
#[test]
fn sibling_relays_must_match_the_dc() {
    use bedrock_lib::actors::{Dc, Topology};
    let topology = Topology {
        dcs: vec![Dc {
            name: "dc-1".into(),
            cells: vec!["cell-1".into(), "cell-2".into(), "cell-3".into()],
        }],
        ..Default::default()
    };
    let check = |dc: &str, cells: &[&str]| {
        let configured = cells.iter().map(|c| c.to_string()).collect();
        super::placement::sibling_mismatch(&topology, dc, "cell-1", &configured)
    };
    assert_eq!(check("dc-1", &["cell-2", "cell-3"]), None);
    assert!(check("dc-1", &["cell-2"]).unwrap().contains("cell-3"));
    assert!(
        check("dc-1", &["cell-2", "cell-3", "cell-9"])
            .unwrap()
            .contains("cell-9")
    );
    assert!(check("dc-2", &[]).is_some());
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_maximum_kv_value_survives_flush_truncation_and_cold_page_reads() {
        use bedrock_lib::kv;
        use prost::Message;
        let objects = Arc::new(Counting::default());
        let n = slate_node(&objects, "n1");
        claim(&n.store, 0);
        let key = b"d/large".to_vec();
        let stored = kv::Stored {
            value: vec![0x5a; kv::MAX_VALUE],
            metadata: Some("{\"large\":true}".into()),
            expiration: 0,
        };
        let bytes = stored.encode_to_vec();
        append(
            &n.store,
            0,
            0,
            vec![pb::Entry {
                seq: 1,
                epoch: 1,
                committed_at: 1234,
                changes: vec![pb::Change {
                    key: key.clone(),
                    value: Some(bytes),
                }],
                ..Default::default()
            }],
            1,
        )
        .unwrap();
        n.cold.tick(&n.directory).await;
        assert_eq!(flushed(&n.store, 0), 1);
        let keys = vec![key.clone()];
        let values = n
            .cold
            .get("cell-1", &actor(0), 1, &keys, crate::store::PAGE_BYTES)
            .await
            .unwrap();
        let restored = kv::Stored::decode(values[0].as_deref().unwrap()).unwrap();
        assert_eq!(restored, stored);
        let (page, more) = n.cold.scan("cell-1", &actor(0), 1, b"d/", 1).await.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0, key);
        assert_eq!(kv::Stored::decode(page[0].1.as_slice()).unwrap(), stored);
        assert!(!more);
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

    /// On SlateDB, which refuses a repeated clone source: a split inside the range, then a merge of the whole range.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_interior_split_then_a_full_merge_on_slatedb() {
        let store = Arc::new(Counting::default());
        let mut n = slate_node(&store, "n1");
        let actors = 6;
        let whole = n.directory.clone();
        round(&n, &store, actors, 0).await;
        n.directory = interior(&whole, actors);
        n.cold.tick(&n.directory).await;
        n.directory = whole;
        round(&n, &store, actors, 1).await;
        for i in 0..actors {
            assert_eq!(flushed(&n.store, i), 2, "actor {i}");
            assert_eq!(
                read(&n.cold, i, 2).await,
                Some(format!("{i}-1").into_bytes())
            );
        }
    }

    /// The acceptance check: S3 requests per flush do not grow with the actors in a range. One flush is one L0 file
    /// and one manifest update; measuring sizes reads the manifest the writer holds. SlateDB's background tasks (the
    /// manifest poll, compactor and garbage collector) may land inside one flush on a slow machine, so the quietest of
    /// a few steady flushes is measured.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_requests_per_flush_do_not_grow_with_actors() {
        let mut writes = Vec::new();
        for actors in [2, 200] {
            let store = Arc::new(Counting::default());
            let n = slate_node(&store, "n1");
            round(&n, &store, actors, 0).await;
            let mut quietest: Option<[u64; 5]> = None;
            for seq in 1..=3 {
                let flush = round(&n, &store, actors, seq).await;
                if quietest.is_none_or(|q| flush.iter().sum::<u64>() < q.iter().sum::<u64>()) {
                    quietest = Some(flush);
                }
            }
            let steady = quietest.expect("three flushes");
            assert_eq!(steady[3], 0, "no copies");
            writes.push(steady[0]);
            assert!(
                steady.iter().sum::<u64>() <= 8,
                "{actors} actors made {steady:?}"
            );
            for i in 0..actors {
                assert_eq!(flushed(&n.store, i), 4);
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

/// Repeated overwrites collapse to one small database value; the remaining committed log still needs another round.
#[tokio::test]
async fn a_bounded_flush_continues_after_an_overwrite_prefix() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    let value = "v".repeat(1024);
    let count = FLUSH_BYTES as usize / value.len() + 1;
    let mut entries: Vec<_> = (1..=count)
        .map(|seq| entry(seq as u64, &[("k", Some(&value))]))
        .collect();
    entries.push(entry(count as u64 + 1, &[("k", Some("last"))]));
    append(&n.store, 0, 0, entries, count as u64 + 1).unwrap();
    n.cold.tick(&n.directory).await;
    assert_eq!(flushed(&n.store, 0), count as u64 + 1);
    assert_eq!(stored(&bucket, 0, "k"), Some(b"last".to_vec()));
    assert!(n.store.dirty(0, 0).is_empty());
    assert_eq!(
        bucket.commits(),
        2,
        "only the remaining committed prefix starts another batch"
    );
}

/// A hanging slot owner holds a tick up for at most the settlement budget, and the next tick starts after the hung
/// actors, so the ones behind them still get settled.
#[tokio::test(start_paused = true)]
async fn a_hanging_owner_is_bounded_and_later_actors_are_still_settled() {
    let bucket = Memory::default();
    let store =
        Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))).with_max_unflushed(u64::MAX));
    let databases: Arc<dyn Databases> = Arc::new(bucket.clone());
    let readers = Readers::new(databases.clone(), 4, &Metrics::new(&Admin::new()));
    let cold = Cold::new(
        store.clone(),
        databases,
        Arc::new(Hanging {
            cell: "cell-1".into(),
        }),
        readers,
        "cell-1",
        "n1",
    );
    let directory = BTreeMap::from([(
        0,
        vec![Owner {
            node: "n1".into(),
            state: State::Ready,
        }],
    )]);
    let actors: usize = 1_000;
    for i in 0..actors {
        claim(&store, i);
        append(&store, i, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    }
    let budget = SETTLE_WAIT + Duration::from_secs(1);
    assert!(
        tokio::time::timeout(budget, cold.tick(&directory))
            .await
            .is_ok(),
        "a hanging owner held up the tick"
    );
    assert_eq!(store.owed().len(), actors, "the hung ones are still owed");
    // `actor(i)` puts `i` in the hash's most significant digits, so key order is index order: the hung actors are the
    // first ones owed, and the next tick's batch is exactly the ones after them.
    cold.tick(&directory).await;
    assert_eq!(store.owed().len(), actors - SETTLE_ACTORS);
    assert!(
        store.owed().iter().any(|(owed, _)| owed.id == actor(0).id),
        "the hung actors are still retried"
    );
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
    let (values, more, next) = merge(
        vec![deleted],
        false,
        stored,
        false,
        1,
        bedrock_lib::kv::MAX_MESSAGE_BYTES,
    );
    assert_eq!(values.len(), 1);
    assert!(more);
    // `b` was consumed (a delete); `c` was not returned, so the next page starts right after `b`.
    assert_eq!(next, b"b\0");
}

/// S-05b: once a deleted actor's grace period ends, its maintainer deletes every key of it from the range database in
/// the range's one flush batch, with the other actors' changes. The tombstone stays and leaves the expiry index, so later
/// ticks neither claim nor rescan the actor.
#[tokio::test]
async fn a_deleted_actor_is_purged_in_the_normal_flush_batch() {
    let bucket = Memory::default();
    let Node {
        store,
        cold,
        directory,
        ..
    } = node(&bucket, "n1", u64::MAX);
    let cold = cold.with_grace(Duration::ZERO);
    for i in 0..2 {
        claim(&store, i);
        append(
            &store,
            i,
            0,
            vec![entry(1, &[("k", Some("v")), ("j", Some("w"))])],
            1,
        )
        .unwrap();
    }
    cold.tick(&directory).await;
    assert_eq!(bucket.commits(), 1);
    let gone = Actor::new(&actor(0)).unwrap();
    let delete = pb::Entry {
        kind: pb::entry::Kind::Delete as i32,
        ..entry(2, &[])
    };
    append(&store, 0, 1, vec![delete.clone()], 1).unwrap();
    let answer = append(&store, 0, 1, vec![delete], 2).unwrap();
    assert_eq!(answer.refused, Refusal::Deleted as i32);
    // The grace period runs from the DELETE's committed_at (0 here).
    assert!(store.expired(0, 0, 0).unwrap().is_empty());
    assert_eq!(store.expired(0, 0, 1).unwrap().len(), 1);
    append(&store, 1, 1, vec![entry(2, &[("k", Some("x"))])], 2).unwrap();

    cold.tick(&directory).await;
    assert_eq!(
        bucket.commits(),
        2,
        "one batch for the deletes and the other actor's changes"
    );
    let contents = bucket.contents(&first_db());
    assert!(
        !contents.keys().any(|k| k.starts_with(gone.name())),
        "no key of the deleted actor remains"
    );
    assert_eq!(stored(&bucket, 1, "k"), Some(b"x".to_vec()));
    assert_eq!(stored(&bucket, 1, "j"), Some(b"w".to_vec()));
    assert!(store.expired(0, 0, u64::MAX).unwrap().is_empty());
    assert_eq!(
        store.reserve(&gone, 5, 0).unwrap().refused,
        Refusal::Deleted as i32
    );
    cold.tick(&directory).await;
    assert_eq!(bucket.commits(), 2, "a purged actor costs nothing more");
}

/// A deleted actor with more keys than [`PURGE_PAGE`] is purged one page per flush batch, and leaves the expiry index
/// only with the batch that empties its prefix.
#[tokio::test]
async fn a_large_deleted_actor_is_purged_one_page_per_batch() {
    let bucket = Memory::default();
    let Node {
        store,
        cold,
        directory,
        ..
    } = node(&bucket, "n1", u64::MAX);
    let cold = cold.with_grace(Duration::ZERO);
    claim(&store, 0);
    let keys: Vec<String> = (0..PURGE_PAGE).map(|i| format!("k{i:05}")).collect();
    let changes: Vec<(&str, Option<&str>)> = keys.iter().map(|k| (k.as_str(), Some("v"))).collect();
    append(&store, 0, 0, vec![entry(1, &changes)], 1).unwrap();
    cold.tick(&directory).await;
    assert_eq!(bucket.commits(), 1);
    let delete = pb::Entry {
        kind: pb::entry::Kind::Delete as i32,
        ..entry(2, &[])
    };
    append(&store, 0, 1, vec![delete.clone()], 1).unwrap();
    append(&store, 0, 1, vec![delete], 2).unwrap();

    // The PURGE_PAGE data keys and the meta: a full page, then the meta alone.
    cold.tick(&directory).await;
    assert_eq!(bucket.commits(), 3, "one batch per page");
    let gone = Actor::new(&actor(0)).unwrap();
    assert!(
        !bucket
            .contents(&first_db())
            .keys()
            .any(|k| k.starts_with(gone.name()))
    );
    assert!(store.expired(0, 0, u64::MAX).unwrap().is_empty());
}

/// A tombstone younger than the grace period keeps the actor's keys in the range database.
#[tokio::test]
async fn a_deleted_actor_keeps_its_keys_during_the_grace_period() {
    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    append(&n.store, 0, 0, vec![entry(1, &[("k", Some("v"))])], 1).unwrap();
    n.cold.tick(&n.directory).await;
    #[allow(clippy::disallowed_methods)]
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let delete = pb::Entry {
        kind: pb::entry::Kind::Delete as i32,
        committed_at: now,
        ..entry(2, &[])
    };
    append(&n.store, 0, 1, vec![delete.clone()], 1).unwrap();
    append(&n.store, 0, 1, vec![delete], 2).unwrap();
    n.cold.tick(&n.directory).await;
    assert_eq!(bucket.commits(), 1);
    assert_eq!(stored(&bucket, 0, "k"), Some(b"v".to_vec()));
    assert_eq!(
        n.store.expired(0, 0, u64::MAX).unwrap().len(),
        1,
        "still waiting"
    );
}

/// #124 / S-16 seed 2: an append past a slot's log end that S3 does not hold either is answered with its gap well
/// within the holder's call timeout, so the holder resends from the slot's end. Waiting out [`BEHIND_WAIT`] instead
/// timed the call out every time, and a slot behind the other voter never caught up.
#[tokio::test(start_paused = true)]
async fn an_append_past_what_s3_holds_answers_its_gap_at_once() {
    use bedrock_lib::volume::volume_server::Volume;
    use tonic::Request;

    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    append(&n.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    n.cold.tick(&n.directory).await;
    let service = crate::serve::Service::new(&Admin::new(), n.store, "n1", "cell-1", "dc1")
        .unwrap()
        .with_cold(Arc::new(n.cold));
    *service.directory.write().unwrap() = n.directory;
    let started = bedrock_lib::monotonic_now();
    let answer = service
        .append(Request::new(pb::AppendRequest {
            actor: Some(actor(0)),
            holder: Some(holder()),
            epoch: 1,
            prev_epoch: 1,
            entries: vec![entry(3, &[])],
            commit: 2,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(answer.refused, Refusal::LogGap as i32);
    assert_eq!(answer.slot.unwrap().end.unwrap().seq, 1);
    assert!(
        started.elapsed() <= crate::serve::GAP_LOOKUP,
        "the gap was answered after {:?}",
        started.elapsed()
    );
}

/// When S3 is too slow to say whether it holds what the slot lacks, the append is retryable, not a gap: a holder sending
/// the first entry it still keeps would take a gap for one it cannot fill and drop the slot.
#[tokio::test(start_paused = true)]
async fn an_append_gap_s3_is_too_slow_to_answer_is_retryable() {
    use bedrock_lib::volume::volume_server::Volume;
    use tonic::{Code, Request};

    let bucket = Memory::default();
    let n = node(&bucket, "n1", u64::MAX);
    claim(&n.store, 0);
    append(&n.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    n.cold.tick(&n.directory).await;
    let service = crate::serve::Service::new(&Admin::new(), n.store, "n1", "cell-1", "dc1")
        .unwrap()
        .with_cold(Arc::new(n.cold));
    *service.directory.write().unwrap() = n.directory;
    bucket.slow(crate::serve::GAP_LOOKUP);
    let refused = service
        .append(Request::new(pb::AppendRequest {
            actor: Some(actor(0)),
            holder: Some(holder()),
            epoch: 1,
            prev_epoch: 1,
            entries: vec![entry(3, &[])],
            commit: 2,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable, "{refused}");
}

/// The other half of [`an_append_past_what_s3_holds_answers_its_gap_at_once`]: when the flush that holds what the slot
/// lacks lands while the append waits, the slot skips to it and takes the append, as before the bound.
#[tokio::test(start_paused = true)]
async fn an_append_past_a_flush_still_landing_is_taken() {
    use bedrock_lib::volume::volume_server::Volume;
    use tonic::Request;

    let bucket = Memory::default();
    let behind = node(&bucket, "n1", u64::MAX);
    let ahead = node(&bucket, "n2", u64::MAX);
    claim(&behind.store, 0);
    claim(&ahead.store, 0);
    append(&behind.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    behind.cold.tick(&behind.directory).await;
    append(&ahead.store, 0, 0, vec![entry(1, &[("k", Some("1"))])], 1).unwrap();
    append(&ahead.store, 0, 1, vec![entry(2, &[("k", Some("2"))])], 2).unwrap();
    let service = crate::serve::Service::new(&Admin::new(), behind.store, "n1", "cell-1", "dc1")
        .unwrap()
        .with_cold(Arc::new(behind.cold));
    *service.directory.write().unwrap() = behind.directory;
    let request = Request::new(pb::AppendRequest {
        actor: Some(actor(0)),
        holder: Some(holder()),
        epoch: 1,
        prev_epoch: 1,
        entries: vec![entry(3, &[])],
        commit: 2,
        ..Default::default()
    });
    let flush = async {
        tokio::time::sleep(crate::serve::GAP_WAIT / 2).await;
        ahead.cold.tick(&ahead.directory).await;
    };
    let (answer, ()) = tokio::join!(service.append(request), flush);
    let answer = answer.unwrap().into_inner();
    assert_eq!(answer.refused, Refusal::None as i32, "{answer:?}");
    assert_eq!(answer.slot.unwrap().end.unwrap().seq, 3);
}
