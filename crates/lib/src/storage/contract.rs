//! Contract suite every [`StorageEngine`] adapter must pass (B-04; used by B-05 and B-39).
//!
//! An adapter implements [`Harness`] in its test module and calls `storage_contract!(harness_expr)`, which defines
//! one `#[test]` per case with a fresh harness each.

use std::collections::BTreeMap;

use super::{Entry, KeyRange, Scan, StorageEngine, StorageError, StorageResult, WriteBatch};

pub trait Harness {
    type Engine: StorageEngine;

    /// A new, empty engine.
    fn open(&mut self) -> Self::Engine;

    /// Damages the stored record at `key` so that reading it must fail with `Corruption`. May reopen the engine.
    fn corrupt(&mut self, engine: Self::Engine, key: &[u8]) -> Self::Engine;
}

macro_rules! storage_contract {
    ($harness:expr) => {
        $crate::storage::contract::storage_contract!(
            $harness;
            missing_key_is_none_not_error,
            put_overwrite_delete,
            batch_applies_in_order,
            data_and_revision_commit_together,
            revisions_are_per_name_and_outside_key_space,
            scans_in_key_order,
            snapshot_is_consistent,
            restore_replaces_everything,
            failed_restore_leaves_engine_unchanged,
            corruption_is_an_error_not_a_miss,
            stats_disk_usage_and_health,
        );
    };
    ($harness:expr; $($case:ident),* $(,)?) => {
        $(
            #[test]
            fn $case() {
                $crate::storage::contract::$case(&mut $harness);
            }
        )*
    };
}
pub(crate) use storage_contract;

fn entries(scan: StorageResult<Scan<'_>>) -> Vec<Entry> {
    scan.unwrap().collect::<StorageResult<_>>().unwrap()
}

fn keys(scan: StorageResult<Scan<'_>>) -> Vec<Vec<u8>> {
    entries(scan).into_iter().map(|(key, _)| key).collect()
}

fn get(engine: &impl StorageEngine, key: &[u8]) -> Option<Vec<u8>> {
    engine.get(key).unwrap()
}

pub fn missing_key_is_none_not_error(h: &mut impl Harness) {
    let engine = h.open();
    assert_eq!(engine.get(b"absent"), Ok(None));
    assert_eq!(engine.revision("absent"), Ok(None));
    engine.delete(b"absent").unwrap();
    engine.put(b"k", b"v").unwrap();
    engine.delete(b"k").unwrap();
    assert_eq!(engine.get(b"k"), Ok(None));
    assert_eq!(engine.snapshot().unwrap().get(b"k"), Ok(None));
}

pub fn put_overwrite_delete(h: &mut impl Harness) {
    let engine = h.open();
    engine.put(b"k", b"1").unwrap();
    assert_eq!(get(&engine, b"k"), Some(b"1".to_vec()));
    engine.put(b"k", b"2").unwrap();
    assert_eq!(get(&engine, b"k"), Some(b"2".to_vec()));
    engine.put(b"empty", b"").unwrap();
    assert_eq!(
        get(&engine, b"empty"),
        Some(Vec::new()),
        "an empty value is not a miss"
    );
    engine.delete(b"k").unwrap();
    assert_eq!(get(&engine, b"k"), None);
}

pub fn batch_applies_in_order(h: &mut impl Harness) {
    let engine = h.open();
    engine.put(b"ns1/old", b"x").unwrap();
    let mut batch = WriteBatch::default();
    batch
        .put(*b"a", *b"1")
        .delete(*b"a")
        .put(*b"b", *b"1")
        .put(*b"ns1/new", *b"1")
        .delete_range(KeyRange::prefix(*b"ns1/"))
        .put(*b"ns1/after", *b"1")
        .delete(*b"never-written");
    engine.write(batch).unwrap();
    assert_eq!(
        entries(engine.scan(&KeyRange::all())),
        vec![
            (b"b".to_vec(), b"1".to_vec()),
            (b"ns1/after".to_vec(), b"1".to_vec())
        ]
    );
}

pub fn data_and_revision_commit_together(h: &mut impl Harness) {
    let engine = h.open();
    let before = engine.snapshot().unwrap();
    let mut batch = WriteBatch::default();
    batch.put(*b"ns/k", *b"1").set_revision("ns", 1);
    engine.write(batch).unwrap();
    assert_eq!(get(&engine, b"ns/k"), Some(b"1".to_vec()));
    assert_eq!(engine.revision("ns"), Ok(Some(1)));
    assert_eq!(before.get(b"ns/k"), Ok(None));
    assert_eq!(before.revision("ns"), Ok(None));
    drop(before);

    // A reader must never see a revision without its data, or the reverse.
    const WRITES: u64 = 200;
    std::thread::scope(|s| {
        s.spawn(|| {
            for revision in 2..=WRITES {
                let mut batch = WriteBatch::default();
                batch
                    .put(*b"ns/k", revision.to_string())
                    .set_revision("ns", revision);
                engine.write(batch).unwrap();
            }
        });
        loop {
            let snapshot = engine.snapshot().unwrap();
            let revision = snapshot.revision("ns").unwrap().unwrap();
            let value = snapshot.get(b"ns/k").unwrap().unwrap();
            assert_eq!(value, revision.to_string().into_bytes());
            if revision == WRITES {
                break;
            }
        }
    });
}

pub fn revisions_are_per_name_and_outside_key_space(h: &mut impl Harness) {
    let engine = h.open();
    let mut batch = WriteBatch::default();
    batch.set_revision("a", 1).set_revision("b", 5);
    engine.write(batch).unwrap();
    let mut batch = WriteBatch::default();
    batch.set_revision("a", 2);
    engine.write(batch).unwrap();
    assert_eq!(engine.revision("a"), Ok(Some(2)));
    assert_eq!(engine.revision("b"), Ok(Some(5)));

    assert_eq!(keys(engine.scan(&KeyRange::all())), Vec::<Vec<u8>>::new());
    engine.delete_range(KeyRange::all()).unwrap();
    assert_eq!(
        engine.snapshot().unwrap().revisions(),
        Ok(BTreeMap::from([("a".into(), 2), ("b".into(), 5)]))
    );
}

pub fn scans_in_key_order(h: &mut impl Harness) {
    let engine = h.open();
    let stored: [&[u8]; 6] = [b"b", b"ab\xff", b"a", b"ac", b"ab", b"\xff\xff"];
    for key in stored {
        engine.put(key, b"v").unwrap();
    }
    let k = |keys: &[&[u8]]| keys.iter().map(|k| k.to_vec()).collect::<Vec<_>>();
    assert_eq!(
        keys(engine.scan(&KeyRange::all())),
        k(&[b"a", b"ab", b"ab\xff", b"ac", b"b", b"\xff\xff"])
    );
    assert_eq!(
        keys(engine.scan(&KeyRange::prefix(*b"ab"))),
        k(&[b"ab", b"ab\xff"])
    );
    assert_eq!(
        keys(engine.scan(&KeyRange::prefix(*b"\xff"))),
        k(&[b"\xff\xff"])
    );
    assert_eq!(
        keys(engine.scan(&KeyRange::new(*b"a", *b"ac"))),
        k(&[b"a", b"ab", b"ab\xff"])
    );
    assert_eq!(keys(engine.scan(&KeyRange::new(*b"b", *b"a"))), k(&[]));
    assert_eq!(keys(engine.scan(&KeyRange::prefix(*b"z"))), k(&[]));
    assert_eq!(
        engine.scan(&KeyRange::all()).unwrap().next().unwrap(),
        Ok((b"a".to_vec(), b"v".to_vec()))
    );
}

pub fn snapshot_is_consistent(h: &mut impl Harness) {
    let engine = h.open();
    let mut batch = WriteBatch::default();
    batch
        .put(*b"a", *b"1")
        .put(*b"b", *b"1")
        .set_revision("ns", 1);
    engine.write(batch).unwrap();
    let snapshot = engine.snapshot().unwrap();

    let mut batch = WriteBatch::default();
    batch
        .put(*b"a", *b"2")
        .delete(*b"b")
        .put(*b"c", *b"2")
        .set_revision("ns", 2);
    engine.write(batch).unwrap();
    engine.delete_range(KeyRange::prefix(*b"a")).unwrap();

    assert_eq!(snapshot.get(b"a"), Ok(Some(b"1".to_vec())));
    assert_eq!(snapshot.get(b"c"), Ok(None));
    assert_eq!(snapshot.revision("ns"), Ok(Some(1)));
    assert_eq!(
        entries(snapshot.scan(&KeyRange::all())),
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"1".to_vec())
        ]
    );
    assert_eq!(keys(engine.scan(&KeyRange::all())), vec![b"c".to_vec()]);
    assert_eq!(engine.revision("ns"), Ok(Some(2)));
}

pub fn restore_replaces_everything(h: &mut impl Harness) {
    let source = h.open();
    let mut batch = WriteBatch::default();
    batch
        .put(*b"a", *b"src")
        .put(*b"b", *b"src")
        .set_revision("ns", 7);
    source.write(batch).unwrap();

    let target = h.open();
    let mut batch = WriteBatch::default();
    batch
        .put(*b"a", *b"old")
        .put(*b"stale", *b"old")
        .set_revision("ns", 9)
        .set_revision("other", 1);
    target.write(batch).unwrap();

    let snapshot = source.snapshot().unwrap();
    target.restore(&*snapshot).unwrap();
    assert_eq!(
        entries(target.scan(&KeyRange::all())),
        entries(snapshot.scan(&KeyRange::all()))
    );
    assert_eq!(
        target.snapshot().unwrap().revisions(),
        Ok(BTreeMap::from([("ns".into(), 7)]))
    );
}

pub fn failed_restore_leaves_engine_unchanged(h: &mut impl Harness) {
    let source = h.open();
    source.put(b"a", b"src").unwrap();
    source.put(b"bad", b"src").unwrap();
    let source = h.corrupt(source, b"bad");

    let target = h.open();
    let mut batch = WriteBatch::default();
    batch.put(*b"keep", *b"old").set_revision("ns", 3);
    target.write(batch).unwrap();

    let result = target.restore(&*source.snapshot().unwrap());
    assert!(
        matches!(result, Err(StorageError::Corruption(_))),
        "{result:?}"
    );
    assert_eq!(
        entries(target.scan(&KeyRange::all())),
        vec![(b"keep".to_vec(), b"old".to_vec())]
    );
    assert_eq!(target.revision("ns"), Ok(Some(3)));
}

pub fn corruption_is_an_error_not_a_miss(h: &mut impl Harness) {
    let engine = h.open();
    engine.put(b"a", b"1").unwrap();
    engine.put(b"bad", b"1").unwrap();
    let engine = h.corrupt(engine, b"bad");

    match engine.get(b"bad") {
        Err(StorageError::Corruption(context)) => assert!(!context.engine.is_empty()),
        other => panic!("expected Corruption, got {other:?}"),
    }
    let scanned: Vec<_> = engine.scan(&KeyRange::all()).unwrap().collect();
    assert!(
        scanned
            .iter()
            .any(|entry| matches!(entry, Err(StorageError::Corruption(_)))),
        "a scan must not skip a corrupted record: {scanned:?}"
    );
    assert!(matches!(
        engine.snapshot().unwrap().get(b"bad"),
        Err(StorageError::Corruption(_))
    ));
}

pub fn stats_disk_usage_and_health(h: &mut impl Harness) {
    let engine = h.open();
    engine.put(b"k", b"v").unwrap();
    assert_eq!(engine.health(), Ok(()));
    engine.stats().unwrap();
    let usage = engine.disk_usage();
    assert!(
        matches!(usage, Ok(_) | Err(StorageError::Unsupported(_))),
        "{usage:?}"
    );
}
