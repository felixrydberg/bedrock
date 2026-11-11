use super::*;
use bedrock_lib::placement::{Owner, State};
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::{StorageEngine, WriteBatch};

const NS: &str = "infrastructure";
const DS: &str = "quicksilver";
const HALF: u64 = 1 << 63;

/// `qs-1` owns `[0, HALF)` in state `state`; `qs-2` owns the rest.
fn directory(state: State) -> Directory {
    let owner = |node: &str, state| Owner {
        node: node.into(),
        state,
    };
    [
        (0, vec![owner("qs-1", state)]),
        (HALF, vec![owner("qs-2", State::Ready)]),
    ]
    .into_iter()
    .collect()
}

/// Keys whose hashes fall in the low half, with values.
fn engine(keys: &[&[u8]]) -> MemoryEngine {
    let engine = MemoryEngine::new(DS);
    let mut batch = WriteBatch::default();
    for key in keys {
        let hash = placement::hash_of(DS, NS, key);
        assert!(hash < HALF, "pick keys in the low half");
        batch.put(l2_key(hash, NS, key), [b"v:".as_slice(), key].concat());
    }
    // Another namespace in the same span is never listed.
    let hash = placement::hash_of(DS, "other", b"p/x");
    batch.put(l2_key(hash % HALF, "other", b"p/x"), b"no".to_vec());
    engine.write(batch).unwrap();
    engine
}

/// Keys with the prefix that hash into the low half.
fn low_keys(prefix: &str, count: usize) -> Vec<Vec<u8>> {
    (0..)
        .map(|i| format!("{prefix}{i:03}").into_bytes())
        .filter(|key| placement::hash_of(DS, NS, key) < HALF)
        .take(count)
        .collect()
}

fn get_request(key: &[u8], min_revision: u64) -> pb::GetRequest {
    pb::GetRequest {
        dataset: DS.into(),
        namespace: NS.into(),
        key: key.to_vec(),
        min_revision,
    }
}

#[test]
fn a_get_answers_with_the_value_or_absence_at_its_ranges_revision() {
    let keys = low_keys("k", 2);
    let engine = engine(&[&keys[0]]);
    let snapshot = engine.snapshot().unwrap();
    let applied = vec![(NS.to_owned(), (0, HALF), 5)];
    let dir = directory(State::Ready);
    let view = View {
        snapshot: &*snapshot,
        applied: &applied,
        directory: &dir,
        node: "qs-1",
    };
    let found = get(&view, &get_request(&keys[0], 5)).unwrap();
    assert_eq!(found.value, Some([b"v:".as_slice(), &keys[0]].concat()));
    assert_eq!(found.revision, 5);
    let absent = get(&view, &get_request(&keys[1], 0)).unwrap();
    assert_eq!((absent.value, absent.revision), (None, 5));
}

#[test]
fn a_get_is_turned_away_by_a_non_owner_and_by_a_behind_owner() {
    let keys = low_keys("k", 1);
    let engine = engine(&[&keys[0]]);
    let snapshot = engine.snapshot().unwrap();
    let applied = vec![(NS.to_owned(), (0, HALF), 5)];
    for (node, state) in [("qs-2", State::Ready), ("qs-1", State::Populating)] {
        let dir = directory(state);
        let view = View {
            snapshot: &*snapshot,
            applied: &applied,
            directory: &dir,
            node,
        };
        let error = get(&view, &get_request(&keys[0], 0)).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{node}");
    }
    let dir = directory(State::Leaving);
    let view = View {
        snapshot: &*snapshot,
        applied: &applied,
        directory: &dir,
        node: "qs-1",
    };
    assert_eq!(
        get(&view, &get_request(&keys[0], 6)).unwrap_err().code(),
        tonic::Code::Aborted
    );
    // No validated progress for the range at all is behind too, never an absence.
    let view = View {
        snapshot: &*snapshot,
        applied: &[],
        directory: &dir,
        node: "qs-1",
    };
    assert_eq!(
        get(&view, &get_request(&keys[0], 0)).unwrap_err().code(),
        tonic::Code::Aborted
    );
}

#[test]
fn a_list_returns_one_namespaces_prefix_in_key_order_and_pages() {
    let mut keys = low_keys("p/", 4);
    keys.extend(low_keys("q/", 1));
    let engine = engine(&keys.iter().map(Vec::as_slice).collect::<Vec<_>>());
    let snapshot = engine.snapshot().unwrap();
    let applied = vec![(NS.to_owned(), (0, HALF), 8)];
    let dir = directory(State::Ready);
    let view = View {
        snapshot: &*snapshot,
        applied: &applied,
        directory: &dir,
        node: "qs-1",
    };
    let request = |start_after: &[u8]| pb::ListRequest {
        dataset: DS.into(),
        namespace: NS.into(),
        prefix: b"p/".to_vec(),
        start_after: start_after.to_vec(),
        limit: 3,
        range_start: 0,
        min_revision: 8,
    };
    let page = list(&view, &request(b"")).unwrap();
    let listed: Vec<_> = page.entries.iter().map(|e| e.key.clone()).collect();
    assert_eq!(listed, keys[..3]);
    assert!(page.more);
    assert_eq!(page.revision, 8);
    let rest = list(&view, &request(&keys[2])).unwrap();
    assert_eq!(rest.entries.len(), 1);
    assert_eq!(rest.entries[0].key, keys[3]);
    assert!(!rest.more);
    let mut not_a_start = request(b"");
    not_a_start.range_start = 1;
    assert_eq!(
        list(&view, &not_a_start).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}
