use super::*;
use crate::paths::{PathRow, path_key, ref_key};
use bedrock_lib::placement::hash_of;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::{WriteBatch, l2_key};

/// Stores `key` where a range holder applies it: the hash-first layout at its placement hash.
fn apply(engine: &MemoryEngine, namespace: &str, key: &[u8], value: Option<&[u8]>) {
    let physical = l2_key(hash_of("cdn", namespace, key), namespace, key);
    let mut batch = WriteBatch::default();
    match value {
        Some(value) => batch.put(physical, value.to_vec()),
        None => batch.delete(physical),
    };
    engine.write(batch).unwrap();
}

fn sha(first: u64, last: u8) -> Hash {
    let mut sha = [last; 32];
    sha[..8].copy_from_slice(&first.to_be_bytes());
    Hash(sha)
}

#[test]
fn a_shared_artifact_stays_retained_until_its_last_ref_key_is_deleted() {
    let engine = MemoryEngine::new("cdn");
    let shared = sha(7, 1);
    for deployment in ["d1", "d2"] {
        apply(
            &engine,
            "deployments",
            &ref_key(&shared, deployment).unwrap(),
            Some(b"".as_slice()),
        );
    }
    assert!(retained(&engine, &shared).unwrap());
    apply(
        &engine,
        "deployments",
        &ref_key(&shared, "d1").unwrap(),
        None,
    );
    assert!(retained(&engine, &shared).unwrap(), "d2 still refers to it");
    apply(
        &engine,
        "deployments",
        &ref_key(&shared, "d2").unwrap(),
        None,
    );
    assert!(!retained(&engine, &shared).unwrap());
}

#[test]
fn any_namespace_retains_and_neighbours_at_the_same_position_do_not() {
    let engine = MemoryEngine::new("cdn");
    // The last position in the hash space: the scan's end wraps to 2⁶⁴.
    let artifact = sha(u64::MAX, 1);
    let neighbour = sha(u64::MAX, 2);
    apply(
        &engine,
        "other",
        &ref_key(&artifact, "d1").unwrap(),
        Some(b"".as_slice()),
    );
    assert!(retained(&engine, &artifact).unwrap());
    assert!(
        !retained(&engine, &neighbour).unwrap(),
        "same 8 bytes, another artifact"
    );
    // A path row naming an artifact keeps nothing; only ref keys do.
    let row = PathRow {
        sha256: neighbour,
        content_type: String::new(),
        size: 0,
    };
    apply(
        &engine,
        "deployments",
        &path_key("d1", "/").unwrap(),
        Some(&row.encode().unwrap()),
    );
    assert!(!retained(&engine, &neighbour).unwrap());
    assert!(!retained(&engine, &sha(0, 1)).unwrap());
}
