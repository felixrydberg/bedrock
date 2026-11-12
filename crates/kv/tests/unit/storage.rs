use std::time::Duration;

use bedrock_lib::replication::materialized::{Committed, Materialized};
use bedrock_lib::replication::{Event, Operation};
use bedrock_lib::storage::sim::MemoryFactory;
use tokio::time::sleep;

use super::*;
use crate::namespaces::{FORMAT_VERSION, Lifecycle, storage};

const REGION: &str = "eu";

fn record(id: &str, home_region: &str) -> Record {
    Record {
        format_version: FORMAT_VERSION,
        id: id.into(),
        tenant: "tenant".into(),
        title: id.into(),
        home_region: home_region.into(),
        storage: storage(id),
        storage_format: bedrock_lib::replication::materialized::FORMAT_VERSION,
        storage_bytes: None,
        permissions: Vec::new(),
        cache_ttl_seconds: CACHE_TTL_SECONDS,
        state: Lifecycle::Active as i32,
        created_at: 1,
        deleting_at: 0,
    }
}

fn id(n: u8) -> String {
    format!("{n:032x}")
}

/// Publishes `key = value` at `revision` into namespace `id`.
async fn publish(factory: &MemoryFactory, id: &str, revision: u64, key: &[u8], value: &[u8]) {
    let writer = Materialized::open(factory, DATASET, id).await.unwrap();
    let event = Event {
        dataset: DATASET.into(),
        namespace: id.into(),
        stream: "kv-0".into(),
        sequence: revision,
        epoch: 1,
        committed_at: revision * 100,
        key: key.to_vec(),
        operation: Operation::Set(value.to_vec()),
    };
    writer
        .apply(&[Committed {
            event,
            retry_key: None,
            retry_expires_at: 0,
        }])
        .await
        .unwrap();
    writer.publish().await.unwrap();
    writer.close().await.unwrap();
}

fn readers(factory: &MemoryFactory, max_open: usize) -> Arc<Readers> {
    Readers::new(
        &Admin::new(),
        Arc::new(factory.clone()),
        REGION.into(),
        "eu-1a".into(),
        max_open,
    )
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn only_the_home_region_reads_and_never_a_deleted_namespace() {
    let factory = MemoryFactory::default();
    publish(&factory, &id(1), 1, b"k", b"v").await;
    let readers = readers(&factory, 4);

    let elsewhere = record(&id(1), "us");
    assert_eq!(
        readers.get(&elsewhere, b"k", 0).await,
        Err(ReadError::NotHome)
    );
    let mut deleted = record(&id(1), REGION);
    deleted.set_state(Lifecycle::Deleting);
    assert_eq!(
        readers.get(&deleted, b"k", 0).await,
        Err(ReadError::Deleted)
    );
    assert!(
        readers.open.lock().unwrap().is_empty(),
        "nothing was opened"
    );

    let (value, position) = readers.get(&record(&id(1), REGION), b"k", 1).await.unwrap();
    assert_eq!((value, position.sequence), (Some(b"v".to_vec()), 1));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_namespace_no_write_reached_is_empty_and_never_kept_open() {
    let factory = MemoryFactory::default();
    let readers = readers(&factory, 4);
    let fresh = record(&id(1), REGION);
    assert_eq!(
        readers.get(&fresh, b"k", 0).await.unwrap(),
        (None, Position::default())
    );
    assert!(matches!(
        readers.get(&fresh, b"k", 1).await,
        Err(ReadError::Storage(StorageError::Unavailable(_)))
    ));
    assert!(readers.open.lock().unwrap().is_empty());

    // Once the writer creates it, the next read sees it.
    publish(&factory, &id(1), 1, b"k", b"v").await;
    let (value, _) = readers.get(&fresh, b"k", 1).await.unwrap();
    assert_eq!(value, Some(b"v".to_vec()));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn handle_pressure_closes_the_least_recently_used_reader_once_no_read_holds_it() {
    let factory = MemoryFactory::default();
    for n in 1..=3 {
        publish(&factory, &id(n), 1, b"k", &[n]).await;
    }
    let readers = readers(&factory, 2);
    readers.get(&record(&id(1), REGION), b"k", 0).await.unwrap();
    // A read still in flight on namespace 1 holds its reader.
    let in_flight = readers.reader(&id(1)).await.unwrap().unwrap();
    sleep(Duration::from_secs(1)).await;
    readers.get(&record(&id(2), REGION), b"k", 0).await.unwrap();
    sleep(Duration::from_secs(1)).await;
    readers.get(&record(&id(3), REGION), b"k", 0).await.unwrap();
    let open: Vec<_> = readers.open.lock().unwrap().keys().cloned().collect();
    assert_eq!(open, vec![id(2), id(3)]);

    readers.sweep().await;
    assert_eq!(readers.retired.lock().unwrap().len(), 1, "still held");
    let (value, _) = in_flight.get(b"k", 0, Duration::ZERO).await.unwrap();
    assert_eq!(value, Some(vec![1]), "an evicted reader finishes its read");
    drop(in_flight);
    readers.sweep().await;
    assert!(readers.retired.lock().unwrap().is_empty());

    // Namespace 1 reopens on demand.
    let (value, _) = readers.get(&record(&id(1), REGION), b"k", 0).await.unwrap();
    assert_eq!(value, Some(vec![1]));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn an_idle_reader_closes_and_the_next_read_reopens_it() {
    let factory = MemoryFactory::default();
    publish(&factory, &id(1), 1, b"k", b"v").await;
    let readers = readers(&factory, 4);
    let home = record(&id(1), REGION);
    readers.get(&home, b"k", 0).await.unwrap();
    sleep(IDLE - Duration::from_secs(1)).await;
    readers.sweep().await;
    assert_eq!(readers.open.lock().unwrap().len(), 1);
    sleep(Duration::from_secs(1)).await;
    readers.sweep().await;
    assert!(readers.open.lock().unwrap().is_empty());
    assert!(readers.retired.lock().unwrap().is_empty());

    publish(&factory, &id(1), 2, b"k", b"newer").await;
    let (value, position) = readers.get(&home, b"k", 2).await.unwrap();
    assert_eq!((value, position.sequence), (Some(b"newer".to_vec()), 2));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn one_failed_namespace_leaves_the_others_readable() {
    let factory = MemoryFactory::default();
    publish(&factory, &id(1), 1, b"k", b"one").await;
    publish(&factory, &id(2), 1, b"k", b"two").await;
    let readers = readers(&factory, 4);
    readers.get(&record(&id(1), REGION), b"k", 0).await.unwrap();
    factory.destroy(DATASET, &id(1)).await.unwrap();
    assert!(matches!(
        readers.get(&record(&id(1), REGION), b"k", 0).await,
        Err(ReadError::Storage(_))
    ));
    let (value, _) = readers.get(&record(&id(2), REGION), b"k", 0).await.unwrap();
    assert_eq!(value, Some(b"two".to_vec()));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn destroy_retires_the_reader_and_removes_only_that_namespace() {
    let factory = MemoryFactory::default();
    publish(&factory, &id(1), 1, b"k", b"one").await;
    publish(&factory, &id(2), 1, b"k", b"two").await;
    let readers = readers(&factory, 4);
    let first = record(&id(1), REGION);
    readers.get(&first, b"k", 0).await.unwrap();
    readers.destroy(&id(1)).await.unwrap();
    assert!(readers.open.lock().unwrap().is_empty());
    assert_eq!(
        readers.get(&first, b"k", 0).await.unwrap(),
        (None, Position::default())
    );
    let (value, _) = readers.get(&record(&id(2), REGION), b"k", 0).await.unwrap();
    assert_eq!(value, Some(b"two".to_vec()));
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
