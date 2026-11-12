use std::time::Duration;

use bedrock_lib::replication::writer::{FENCE_HOLD, Persistence};
use bedrock_lib::storage::sim::MemoryFactory;
use tokio::time::sleep;

use super::*;
use crate::namespaces::{FORMAT_VERSION, Lifecycle, storage};

const REGION: &str = "eu";
const CELL: &str = "eu-1a";

fn record(id: &str) -> Record {
    Record {
        format_version: FORMAT_VERSION,
        id: id.into(),
        tenant: "tenant".into(),
        title: id.into(),
        home_region: REGION.into(),
        storage: storage(id),
        storage_format: bedrock_lib::replication::materialized::FORMAT_VERSION,
        storage_bytes: None,
        permissions: Vec::new(),
        cache_ttl_seconds: CACHE_TTL_SECONDS,
        state: Lifecycle::Active as i32,
        created_at: 1,
        deleting_at: 0,
        home_cell: CELL.into(),
    }
}

fn id(n: u8) -> String {
    format!("{n:032x}")
}

fn member(node: &str) -> pb::Member {
    pb::Member {
        node_id: node.into(),
        service: "kv".into(),
        region: REGION.into(),
        cell: CELL.into(),
        status: pb::member::Status::Ready as i32,
        address: format!("{node}:7100"),
        ..Default::default()
    }
}

/// A membership view the test can change.
#[derive(Clone, Default)]
struct View(Arc<Mutex<BTreeMap<String, pb::Member>>>);

impl View {
    fn of(nodes: &[&str]) -> Self {
        let view = View::default();
        view.set(nodes);
        view
    }

    fn set(&self, nodes: &[&str]) {
        *self.0.lock().unwrap() = nodes.iter().map(|n| (n.to_string(), member(n))).collect();
    }

    fn members(&self) -> Members {
        let view = self.0.clone();
        Arc::new(move || view.lock().unwrap().clone())
    }
}

fn owners(factory: &MemoryFactory, node: &str, view: &View, max_open: usize) -> Arc<Owners> {
    owners_on(Arc::new(factory.clone()), node, view, max_open)
}

fn owners_on(
    factory: Arc<dyn bedrock_lib::storage::namespace::NamespaceFactory>,
    node: &str,
    view: &View,
    max_open: usize,
) -> Arc<Owners> {
    let writer = Writer::owner(
        || 1_700_000_000_000,
        Persistence {
            factory,
            retention: Default::default(),
            metrics: Default::default(),
        },
        max_open,
    )
    .unwrap();
    Owners::new(
        &Admin::new(),
        writer,
        node.into(),
        REGION.into(),
        CELL.into(),
        view.members(),
    )
}

fn set(value: &[u8]) -> Option<Stored> {
    Some(Stored {
        value: value.to_vec(),
        metadata: None,
        expiration: 0,
    })
}

async fn value(owners: &Owners, record: &Record, key: &[u8], min_revision: u64) -> Option<Vec<u8>> {
    let (stored, _) = owners.get(record, key, min_revision).await.unwrap();
    stored.map(|stored| stored.value)
}

/// A namespace no single node owns in every test: the first of `nodes` it ranks.
fn owned_by(nodes: &[&str], n: u8) -> String {
    let view = View::of(nodes);
    let members = (view.members())();
    placement::rank(placement::hash_of(DATASET, &id(n), b""), members.values())[0]
        .node_id
        .clone()
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn only_the_owner_in_the_home_cell_serves_and_never_a_deleted_namespace() {
    let factory = MemoryFactory::default();
    let nodes = ["kv-1", "kv-2", "kv-3"];
    let view = View::of(&nodes);
    let owner = owned_by(&nodes, 1);
    let other = nodes.iter().find(|n| **n != owner).unwrap();
    let here = owners(&factory, &owner, &view, 4);
    let there = owners(&factory, other, &view, 4);
    let home = record(&id(1));

    assert_eq!(here.route(&home), Route::Local);
    assert_eq!(there.route(&home), Route::Peer(format!("{owner}:7100")));
    assert_eq!(
        there
            .write(&home, vec![(b"k".to_vec(), set(b"v"))], None)
            .await,
        Err(OwnerError::NotOwner(Route::Peer(format!("{owner}:7100"))))
    );
    let mut elsewhere = record(&id(1));
    elsewhere.home_cell = "eu-1b".into();
    assert_eq!(
        here.route(&elsewhere),
        Route::Cell(REGION.into(), "eu-1b".into())
    );
    let mut deleted = record(&id(1));
    deleted.set_state(Lifecycle::Deleting);
    assert_eq!(here.get(&deleted, b"k", 0).await, Err(OwnerError::Deleted));
    assert!(
        here.writer.open_namespaces().is_empty(),
        "nothing was opened"
    );

    assert_eq!(value(&here, &home, b"k", 0).await, None);
}

/// #58 / B-26's acceptance: a read with the revision a write returned sees it, with no publication to wait for.
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_write_is_read_back_at_its_revision_with_metadata_and_expiry() {
    let factory = MemoryFactory::default();
    let owners = owners(&factory, "kv-1", &View::of(&["kv-1"]), 4);
    let home = record(&id(1));
    let stored = Stored {
        value: b"v".to_vec(),
        metadata: Some(r#"{"m":1}"#.into()),
        expiration: u64::MAX,
    };
    let revision = owners
        .write(&home, vec![(b"k".to_vec(), Some(stored.clone()))], None)
        .await
        .unwrap();
    assert_eq!(revision, 1);
    let (found, position) = owners.get(&home, b"k", revision).await.unwrap();
    assert_eq!((found, position.sequence), (Some(stored), 1));

    // An expired value reads as absent and is left out of lists.
    let expired = Stored {
        value: b"old".to_vec(),
        metadata: None,
        expiration: 1,
    };
    let revision = owners
        .write(&home, vec![(b"gone".to_vec(), Some(expired))], None)
        .await
        .unwrap();
    assert_eq!(value(&owners, &home, b"gone", revision).await, None);
    let (page, _) = owners.list(&home, b"", None, 10, revision).await.unwrap();
    assert_eq!(
        page.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
        vec![b"k".to_vec()]
    );

    // Namespaces are isolated.
    assert_eq!(value(&owners, &record(&id(2)), b"k", 0).await, None);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_bulk_write_commits_in_order_in_one_batch_and_retries_return_its_revision() {
    let factory = MemoryFactory::default();
    let owners = owners(&factory, "kv-1", &View::of(&["kv-1"]), 4);
    let home = record(&id(1));
    let bulk = vec![
        (b"a".to_vec(), set(b"1")),
        (b"b".to_vec(), set(b"2")),
        (b"a".to_vec(), None),
    ];
    let revision = owners
        .write(&home, bulk.clone(), Some("bulk-1"))
        .await
        .unwrap();
    assert_eq!(revision, 3);
    assert_eq!(value(&owners, &home, b"a", 3).await, None);
    assert_eq!(value(&owners, &home, b"b", 3).await, Some(b"2".to_vec()));

    // The retry writes nothing again and answers the original revision.
    assert_eq!(owners.write(&home, bulk, Some("bulk-1")).await.unwrap(), 3);
    let single = vec![(b"c".to_vec(), set(b"3"))];
    assert_eq!(
        owners
            .write(&home, single.clone(), Some("one"))
            .await
            .unwrap(),
        4
    );
    assert_eq!(owners.write(&home, single, Some("one")).await.unwrap(), 4);
    let (_, position) = owners.get(&home, b"c", 0).await.unwrap();
    assert_eq!(position.sequence, 4);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn the_storage_quota_refuses_writes_past_it() {
    let factory = MemoryFactory::default();
    let owners = owners(&factory, "kv-1", &View::of(&["kv-1"]), 4);
    let mut home = record(&id(1));
    home.storage_bytes = Some(64);
    owners
        .write(&home, vec![(b"k".to_vec(), set(&[0; 20]))], None)
        .await
        .unwrap();
    assert_eq!(
        owners
            .write(&home, vec![(b"big".to_vec(), set(&[0; 60]))], None)
            .await,
        Err(OwnerError::Quota)
    );
    // Deleting frees room.
    owners
        .write(&home, vec![(b"k".to_vec(), None)], None)
        .await
        .unwrap();
    owners
        .write(&home, vec![(b"big".to_vec(), set(&[0; 50]))], None)
        .await
        .unwrap();
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn pressure_and_idleness_close_handles_and_the_next_request_reopens_them() {
    let factory = MemoryFactory::default();
    let owners = owners(&factory, "kv-1", &View::of(&["kv-1"]), 2);
    for n in 1..=3 {
        owners
            .write(&record(&id(n)), vec![(b"k".to_vec(), set(&[n]))], None)
            .await
            .unwrap();
        sleep(Duration::from_secs(1)).await;
    }
    let open: Vec<String> = owners
        .writer
        .open_namespaces()
        .into_iter()
        .map(|(_, namespace, _)| namespace)
        .collect();
    assert_eq!(open, vec![id(2), id(3)], "the least recently used closed");
    assert_eq!(
        value(&owners, &record(&id(1)), b"k", 1).await,
        Some(vec![1])
    );

    sleep(IDLE).await;
    owners.sweep().await;
    assert!(owners.writer.open_namespaces().is_empty());
    assert_eq!(
        value(&owners, &record(&id(3)), b"k", 1).await,
        Some(vec![3])
    );
}

/// An ownership move (#58 / B-26, `docs/spec/materialized.tla` `Move`): the new owner's open fences the old one, every
/// acknowledged write survives, the old owner acknowledges nothing more, and it takes the namespace back only after
/// `FENCE_HOLD` once membership says so again.
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn an_ownership_move_fences_the_old_owner_and_loses_no_acknowledged_write() {
    let factory = MemoryFactory::default();
    let home = record(&id(1));
    let (a_view, b_view) = (View::of(&["kv-a"]), View::of(&["kv-b"]));
    let a = owners(&factory, "kv-a", &a_view, 4);
    let b = owners(&factory, "kv-b", &b_view, 4);
    assert_eq!(
        a.write(&home, vec![(b"k".to_vec(), set(b"1"))], Some("w1"))
            .await,
        Ok(1)
    );

    // B takes the namespace while A's view still says A owns it.
    assert_eq!(
        b.write(&home, vec![(b"k".to_vec(), set(b"2"))], None).await,
        Ok(2)
    );
    assert!(matches!(
        a.write(&home, vec![(b"k".to_vec(), set(b"lost"))], None)
            .await,
        Err(OwnerError::Commit(CommitError::Fenced))
    ));
    assert!(
        a.get(&home, b"k", 2).await.is_err(),
        "A cannot prove revision 2"
    );
    assert_eq!(value(&b, &home, b"k", 2).await, Some(b"2".to_vec()));
    // A retry of A's acknowledged write, sent to B, finds its result.
    assert_eq!(
        b.write(&home, vec![(b"k".to_vec(), set(b"1"))], Some("w1"))
            .await,
        Ok(1)
    );

    // A's view catches up: it closes its handle. Membership later moves the namespace back to A, which waits out the
    // fence hold, then reopens it with everything B acknowledged.
    a_view.set(&["kv-a", "kv-b"]);
    b_view.set(&["kv-a", "kv-b"]);
    let back_to_a = owned_by(&["kv-a", "kv-b"], 1) == "kv-a";
    let (owner, other) = if back_to_a { (&a, &b) } else { (&b, &a) };
    other.release_moved().await;
    assert!(other.writer.open_namespaces().is_empty());
    sleep(FENCE_HOLD).await;
    assert_eq!(
        owner
            .write(&home, vec![(b"k".to_vec(), set(b"3"))], None)
            .await,
        Ok(3)
    );
    assert_eq!(value(owner, &home, b"k", 3).await, Some(b"3".to_vec()));
}

/// Regression for `namespace_owners` seed 42: an owner whose flush failed keeps the batch to retry, but if another
/// owner took the namespace and used those revisions meanwhile, the first owner's reopen must drop it rather than
/// replay it into a revision that is no longer its own, which wedged the namespace as corrupt.
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_failed_batch_superseded_by_another_owner_is_dropped_on_reopen() {
    use bedrock_lib::storage::sim::{SimFactory, StoreAccess, StoreFault};
    use std::sync::atomic::{AtomicBool, Ordering};

    let fail_flush = Arc::new(AtomicBool::new(false));
    let faults = {
        let fail_flush = fail_flush.clone();
        Arc::new(move |access: StoreAccess| {
            (access == StoreAccess::Flush && fail_flush.swap(false, Ordering::SeqCst))
                .then_some(StoreFault::Unavailable)
        })
    };
    let factory: Arc<dyn bedrock_lib::storage::namespace::NamespaceFactory> =
        Arc::new(SimFactory::new(faults));
    let home = record(&id(1));
    let a = owners_on(factory.clone(), "kv-a", &View::of(&["kv-a"]), 4);
    let b = owners_on(factory, "kv-b", &View::of(&["kv-b"]), 4);
    let one = |value: &[u8]| vec![(b"k".to_vec(), set(value))];

    assert_eq!(a.write(&home, one(b"a1"), None).await, Ok(1));
    fail_flush.store(true, Ordering::SeqCst);
    assert!(matches!(
        a.write(&home, one(b"unknown"), None).await,
        Err(OwnerError::Commit(CommitError::Unavailable(_)))
    ));
    // B takes the namespace and uses revision 2 for its own write.
    assert_eq!(b.write(&home, one(b"b2"), None).await, Ok(2));
    // A owns it again: its reopen fences B and writes on from B's head.
    assert_eq!(a.write(&home, one(b"a3"), None).await, Ok(3));
    assert_eq!(value(&a, &home, b"k", 3).await, Some(b"a3".to_vec()));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn destroy_removes_only_that_namespace() {
    let factory = MemoryFactory::default();
    let owners = owners(&factory, "kv-1", &View::of(&["kv-1"]), 4);
    for n in 1..=2 {
        owners
            .write(&record(&id(n)), vec![(b"k".to_vec(), set(&[n]))], None)
            .await
            .unwrap();
    }
    owners.destroy(&id(1)).await.unwrap();
    assert!(
        owners
            .writer
            .open_namespaces()
            .iter()
            .all(|(_, namespace, _)| *namespace != id(1))
    );
    assert_eq!(value(&owners, &record(&id(1)), b"k", 0).await, None);
    assert_eq!(
        value(&owners, &record(&id(2)), b"k", 1).await,
        Some(vec![2])
    );
}

/// Owners on real SlateDB that each believe they own the namespace, as during a membership change.
#[cfg(not(madsim))]
fn slate_owner(store: &Arc<object_store::memory::InMemory>, node: &str) -> Arc<Owners> {
    let writer = Writer::owner(
        || 1_700_000_000_000,
        Persistence {
            factory: Arc::new(bedrock_lib::storage::slate::SlateFactory::new(
                store.clone(),
            )),
            retention: Default::default(),
            metrics: Default::default(),
        },
        4,
    )
    .unwrap();
    Owners::new(
        &Admin::new(),
        writer,
        node.into(),
        REGION.into(),
        CELL.into(),
        View::of(&[node]).members(),
    )
}

/// An ownership move on real SlateDB (#58 / B-26), not the model: `docs/spec/materialized.tla` trusts SlateDB's writer
/// fencing, and this checks it. A new owner's open fences the old one, which never acknowledges again; everything the
/// old owner acknowledged survives; a retry sent to the new owner finds its result; the old owner cannot prove a
/// revision it does not hold; and a third owner takes the namespace from the second the same way.
#[cfg(not(madsim))]
#[tokio::test]
async fn slatedb_fences_the_old_owner_when_a_namespace_moves() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let home = record(&id(1));
    let (a, b) = (slate_owner(&store, "kv-a"), slate_owner(&store, "kv-b"));
    let one = |value: &[u8]| vec![(b"k".to_vec(), set(value))];

    assert_eq!(a.write(&home, one(b"a1"), Some("a1")).await, Ok(1));
    assert_eq!(a.write(&home, one(b"a2"), None).await, Ok(2));

    // B opens the namespace while A still has it open.
    assert_eq!(b.write(&home, one(b"b3"), None).await, Ok(3));
    assert_eq!(value(&b, &home, b"k", 2).await, Some(b"b3".to_vec()));
    let (found, _) = b.get(&home, b"k", 0).await.unwrap();
    assert_eq!(
        found.unwrap().value,
        b"b3",
        "B recovered A's acknowledged writes"
    );

    // A's next write is fenced and acknowledged to no one.
    assert_eq!(
        a.write(&home, one(b"lost"), None).await,
        Err(OwnerError::Commit(CommitError::Fenced))
    );
    assert_eq!(value(&b, &home, b"k", 3).await, Some(b"b3".to_vec()));
    // A cannot prove B's revision, so a minimum-revision read there fails rather than answering stale.
    assert!(a.get(&home, b"k", 3).await.is_err());
    // A retry of A's acknowledged write, sent to B, finds its original revision and writes nothing.
    assert_eq!(b.write(&home, one(b"a1"), Some("a1")).await, Ok(1));
    assert_eq!(value(&b, &home, b"k", 3).await, Some(b"b3".to_vec()));

    // A third owner takes it from B the same way.
    let c = slate_owner(&store, "kv-c");
    assert_eq!(c.write(&home, one(b"c4"), None).await, Ok(4));
    assert_eq!(
        b.write(&home, one(b"lost"), None).await,
        Err(OwnerError::Commit(CommitError::Fenced))
    );
    assert_eq!(value(&c, &home, b"k", 4).await, Some(b"c4".to_vec()));
}

/// The race behind `StaleFlush` in `docs/spec/materialized.tla`, on real SlateDB: A's batch is in flight while B opens.
/// Whichever wins, a write acknowledged to anyone is in the store B serves, and no revision is acknowledged twice.
#[cfg(not(madsim))]
#[tokio::test]
async fn slatedb_never_loses_a_write_acknowledged_while_the_namespace_moves() {
    for round in 0..8u8 {
        let store = Arc::new(object_store::memory::InMemory::new());
        let home = record(&id(round + 1));
        let (a, b) = (slate_owner(&store, "kv-a"), slate_owner(&store, "kv-b"));
        a.write(&home, vec![(b"warm".to_vec(), set(b"0"))], None)
            .await
            .unwrap();
        let racing: Vec<_> = (0..4u8)
            .map(|n| {
                let (a, home) = (a.clone(), home.clone());
                tokio::spawn(async move {
                    let key = vec![b'a', n];
                    let written = a.write(&home, vec![(key.clone(), set(&[n]))], None).await;
                    (key, written)
                })
            })
            .collect();
        // B opens, fencing A, while A's batches are being written and flushed.
        let taken = b
            .write(&home, vec![(b"b".to_vec(), set(b"b"))], None)
            .await
            .unwrap();
        let mut acknowledged = BTreeMap::new();
        acknowledged.insert(taken, b"b".to_vec());
        for racing in racing {
            let (key, written) = racing.await.unwrap();
            match written {
                Ok(revision) => {
                    assert!(
                        acknowledged.insert(revision, key.clone()).is_none(),
                        "round {round}: revision {revision} acknowledged twice"
                    );
                    let (found, _) = b.get(&home, &key, 0).await.unwrap();
                    assert!(
                        found.is_some(),
                        "round {round}: acknowledged {key:?} at {revision} was lost"
                    );
                }
                // Fenced, or an outcome A never learned: not acknowledged, so nothing to keep.
                Err(OwnerError::Commit(_)) => {}
                Err(other) => panic!("round {round}: {other:?}"),
            }
        }
        // B keeps writing past everything, at revisions no one else was given.
        let next = b
            .write(&home, vec![(b"after".to_vec(), set(b"x"))], None)
            .await
            .unwrap();
        assert!(acknowledged.keys().all(|revision| *revision < next));
    }
}

/// SlateDB, because the model removes a destroyed namespace's state and so fails its writer whether fenced or not.
#[cfg(not(madsim))]
#[tokio::test]
async fn destroy_fences_a_writer_still_open_so_nothing_comes_back() {
    use bedrock_lib::replication::materialized::{Committed, Materialized};
    use bedrock_lib::replication::{Event, Operation};
    use bedrock_lib::storage::slate::SlateFactory;
    use object_store::ObjectStore;

    let store = Arc::new(object_store::memory::InMemory::new());
    let factory = Arc::new(SlateFactory::new(store.clone()));
    let stale = Materialized::open(factory.as_ref(), DATASET, &id(1))
        .await
        .unwrap();
    let writer = Writer::owner(
        || 1_700_000_000_000,
        Persistence {
            factory,
            retention: Default::default(),
            metrics: Default::default(),
        },
        4,
    )
    .unwrap();
    let owners = Owners::new(
        &Admin::new(),
        writer,
        "kv-1".into(),
        REGION.into(),
        CELL.into(),
        View::of(&["kv-1"]).members(),
    );
    owners.destroy(&id(1)).await.unwrap();
    let event = Event {
        dataset: DATASET.into(),
        namespace: id(1),
        stream: "kv-0".into(),
        sequence: 1,
        epoch: 1,
        committed_at: 100,
        key: b"k".to_vec(),
        operation: Operation::Set(b"v".to_vec()),
    };
    let late = stale
        .persist(
            &[Committed {
                event,
                retry_key: None,
                retry_expires_at: 0,
            }],
            None,
        )
        .await;
    assert!(matches!(late, Err(StorageError::Fenced(_))), "{late:?}");
    let left = store.list_with_delimiter(None).await.unwrap();
    assert!(
        left.objects.is_empty() && left.common_prefixes.is_empty(),
        "nothing was written back under the deleted prefix: {left:?}"
    );
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_list_cursor_outside_its_prefix_never_leaves_the_prefix() {
    let factory = MemoryFactory::default();
    let owners = owners(&factory, "kv-1", &View::of(&["kv-1"]), 4);
    let home = record(&id(1));
    let keys: Vec<Change> = [b"aa", b"ba", b"ca"]
        .into_iter()
        .map(|key| (key.to_vec(), set(b"v")))
        .collect();
    owners.write(&home, keys, None).await.unwrap();
    let page = |after: &'static [u8]| {
        let (owners, home) = (owners.clone(), home.clone());
        async move {
            owners
                .list(&home, b"b", Some(after), 10, 0)
                .await
                .unwrap()
                .0
                .len()
        }
    };
    assert_eq!(
        page(b"a").await,
        1,
        "a cursor before the prefix starts at it"
    );
    assert_eq!(page(b"b").await, 1);
    assert_eq!(page(b"ba").await, 0);
    assert_eq!(
        page(b"c").await,
        0,
        "a cursor past the prefix leaves nothing"
    );
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn cached_values_expire_and_never_answer_a_newer_minimum_revision() {
    let cache = ValueCache::new(&Admin::new(), 1 << 20);
    cache.fill("a", b"k", b"v", 5);
    assert_eq!(cache.get("a", b"k", 5), Some((b"v".to_vec(), 5)));
    assert_eq!(cache.get("a", b"k", 6), None);
    assert_eq!(cache.get("b", b"k", 0), None, "namespaces are separate");
    cache.fill("a", b"k", b"older", 4);
    assert_eq!(cache.get("a", b"k", 0), Some((b"v".to_vec(), 5)));

    sleep(Duration::from_secs(CACHE_TTL_SECONDS.into())).await;
    assert_eq!(cache.get("a", b"k", 0), None);
    assert_eq!(cache.inner.lock().unwrap().total, 0);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn expired_values_free_their_namespace_share() {
    let budget = 8 * 1024;
    let cache = ValueCache::new(&Admin::new(), budget);
    for n in 0..100u32 {
        cache.fill("hot", &n.to_be_bytes(), &[0; 100], 1);
    }
    sleep(Duration::from_secs(CACHE_TTL_SECONDS.into())).await;
    cache.fill("hot", b"new", &[0; 100], 2);
    assert!(cache.get("hot", b"new", 0).is_some());
}

#[test]
fn one_namespace_cannot_take_over_the_value_cache() {
    let budget = 8 * 1024;
    let cache = ValueCache::new(&Admin::new(), budget);
    cache.fill("quiet", b"k", &[0; 100], 1);
    for n in 0..100u32 {
        cache.fill("hot", &n.to_be_bytes(), &[0; 100], 1);
    }
    let state = cache.inner.lock().unwrap();
    assert!(state.by_namespace["hot"] <= budget / SHARE);
    assert!(state.total <= budget);
    drop(state);
    assert!(
        cache.get("quiet", b"k", 0).is_some(),
        "the hot namespace evicted nothing"
    );
}

#[test]
fn the_value_cache_stays_within_its_budget() {
    let budget = 64 * 1024;
    let cache = ValueCache::new(&Admin::new(), budget);
    for namespace in 0..64u32 {
        for key in 0..8u32 {
            cache.fill(&namespace.to_string(), &key.to_be_bytes(), &[0; 200], 1);
        }
    }
    let state = cache.inner.lock().unwrap();
    assert!(state.total <= budget);
    assert_eq!(state.total, state.by_namespace.values().sum::<u64>());
    // The oldest fills went first.
    assert!(
        state
            .entries
            .peek(&("0".into(), 0u32.to_be_bytes().to_vec()))
            .is_none()
    );
    assert!(
        state
            .entries
            .peek(&("63".into(), 7u32.to_be_bytes().to_vec()))
            .is_some()
    );
}
