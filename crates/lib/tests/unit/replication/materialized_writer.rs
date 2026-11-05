use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::tests::Factory;
use crate::replication::log::Log;
use crate::storage::StorageError;
use crate::storage::sim::{MemoryFactory, SimFactory, StoreAccess, StoreFault};

fn early() -> u64 {
    1_000
}
fn later() -> u64 {
    1_100
}
fn expired() -> u64 {
    2_000
}

fn write(value: Option<&str>, retry: Option<&str>) -> Write {
    Write {
        dataset: "kv".into(),
        namespace: "one".into(),
        key: b"key".to_vec(),
        operation: value.map_or(Operation::Delete, |value| {
            Operation::Set(value.as_bytes().to_vec())
        }),
        idempotency_key: retry.map(str::to_owned),
        expected: None,
    }
}

fn config(
    factory: Arc<dyn NamespaceFactory>,
    retention: Retention,
    metrics: PersistenceMetrics,
) -> Persistence {
    Persistence {
        factory,
        retention,
        metrics,
    }
}

async fn idle(writer: &Writer) {
    let namespace = writer.namespace("kv", "one");
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::task::yield_now().await;
        while namespace.flow.lock().unwrap().running {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup flow must finish");
}

#[tokio::test]
async fn retry_keeps_original_result_through_overwrite_delete_and_crash() {
    let factory = Arc::new(MemoryFactory::default());
    let writer = Writer::open(
        early,
        config(
            factory.clone(),
            Retention::default(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        writer.commit(write(Some("first"), Some("original"))).await,
        Ok(1)
    );
    assert_eq!(writer.commit(write(Some("overwrite"), None)).await, Ok(2));
    assert_eq!(writer.commit(write(None, Some("delete"))).await, Ok(3));
    idle(&writer).await;
    factory.crash().unwrap();
    let reopened = Writer::open(
        early,
        config(factory, Retention::default(), PersistenceMetrics::default()),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .commit(write(Some("wrong"), Some("original")))
            .await,
        Ok(1)
    );
    assert_eq!(
        reopened.commit(write(Some("wrong"), Some("delete"))).await,
        Ok(3)
    );
    assert!(
        reopened
            .bootstrap("kv", "one")
            .await
            .unwrap()
            .values
            .is_empty()
    );
    assert_eq!(
        reopened
            .log()
            .read("kv", "one", 1, usize::MAX)
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn retry_expiry_stays_fixed_and_expired_identity_becomes_a_new_write() {
    let factory = Arc::new(MemoryFactory::default());
    let retention = Retention {
        event_retention_ms: 5_000,
        retry_retention_ms: 500,
    };
    let writer = Writer::open(
        early,
        config(
            factory.clone(),
            retention.clone(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    writer
        .commit(write(Some("original"), Some("retry")))
        .await
        .unwrap();
    idle(&writer).await;
    factory.crash().unwrap();
    let writer = Writer::open(
        later,
        config(
            factory.clone(),
            retention.clone(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        writer.commit(write(Some("wrong"), Some("retry"))).await,
        Ok(1)
    );
    assert_eq!(
        writer.bootstrap("kv", "one").await.unwrap().retries["retry"].expires_at,
        1_500
    );
    idle(&writer).await;
    factory.crash().unwrap();
    let writer = Writer::open(
        expired,
        config(factory, retention, PersistenceMetrics::default()),
    )
    .await
    .unwrap();
    assert_eq!(
        writer.commit(write(Some("new"), Some("retry"))).await,
        Ok(2)
    );
    let view = writer.bootstrap("kv", "one").await.unwrap();
    assert_eq!(view.values[b"key".as_slice()], b"new");
    assert_eq!(view.retries["retry"].expires_at, 2_500);
}

#[tokio::test]
async fn event_and_retry_retention_windows_are_independent_and_survive_crash() {
    for (event_ms, retry_ms, expected_pruned, expected_retry) in
        [(50, 500, 1, true), (500, 50, 0, false)]
    {
        let factory = Arc::new(MemoryFactory::default());
        let retention = Retention {
            event_retention_ms: event_ms,
            retry_retention_ms: retry_ms,
        };
        let writer = Writer::open(
            early,
            config(
                factory.clone(),
                retention.clone(),
                PersistenceMetrics::default(),
            ),
        )
        .await
        .unwrap();
        writer
            .commit(write(Some("value"), Some("retry")))
            .await
            .unwrap();
        idle(&writer).await;
        factory.crash().unwrap();
        let writer = Writer::open(
            later,
            config(
                factory.clone(),
                retention.clone(),
                PersistenceMetrics::default(),
            ),
        )
        .await
        .unwrap();
        writer.cleanup("kv", "one").await.unwrap();
        let view = writer.bootstrap("kv", "one").await.unwrap();
        assert_eq!(view.pruned_through, expected_pruned);
        assert_eq!(view.retries.contains_key("retry"), expected_retry);
        assert_eq!(view.values[b"key".as_slice()], b"value");
        factory.crash().unwrap();
        let reopened = Writer::open(
            later,
            config(factory, retention, PersistenceMetrics::default()),
        )
        .await
        .unwrap();
        assert_eq!(reopened.bootstrap("kv", "one").await.unwrap(), view);
    }
}

#[tokio::test]
async fn expiry_cleanup_failure_is_metered_after_durable_acknowledgement() {
    static NOW: AtomicU64 = AtomicU64::new(1_000);
    fn now() -> u64 {
        NOW.load(Ordering::SeqCst)
    }
    let factory = Factory::default();
    let metrics = PersistenceMetrics::default();
    let writer = Writer::open(
        now,
        config(
            Arc::new(factory.clone()),
            Retention {
                event_retention_ms: 10,
                retry_retention_ms: 5_000,
            },
            metrics.clone(),
        ),
    )
    .await
    .unwrap();
    writer
        .commit(write(Some("old"), Some("first")))
        .await
        .unwrap();
    idle(&writer).await;
    factory.controls.fail_cleanup.store(true, Ordering::SeqCst);
    NOW.store(2_000, Ordering::SeqCst);
    assert_eq!(
        writer.commit(write(Some("durable"), Some("second"))).await,
        Ok(2)
    );
    idle(&writer).await;
    assert!(
        metrics
            .runs
            .get_or_create(&PersistenceLabel {
                dataset: "kv".into(),
                outcome: "failed".into()
            })
            .get()
            > 0
    );
    factory.inner.crash().unwrap();
    factory.controls.fail_cleanup.store(false, Ordering::SeqCst);
    let reopened = Writer::open(
        now,
        config(
            Arc::new(factory),
            Retention {
                event_retention_ms: 10,
                retry_retention_ms: 5_000,
            },
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    let view = reopened.bootstrap("kv", "one").await.unwrap();
    assert_eq!(view.sequence, 2);
    assert_eq!(view.values[b"key".as_slice()], b"durable");
    assert_eq!(view.retries["second"].sequence, 2);
}

#[tokio::test]
async fn missing_or_corrupt_retained_event_prevents_startup() {
    for value in [None, Some(vec![255])] {
        let factory = Arc::new(MemoryFactory::default());
        let writer = Writer::open(
            early,
            config(
                factory.clone(),
                Retention::default(),
                PersistenceMetrics::default(),
            ),
        )
        .await
        .unwrap();
        writer.commit(write(Some("value"), None)).await.unwrap();
        idle(&writer).await;
        let raw = factory.open("kv", "one").await.unwrap();
        let event_key = [b"e".as_slice(), &1_u64.to_be_bytes()].concat();
        raw.write(vec![(event_key, value)]).await.unwrap();
        raw.flush().await.unwrap();
        let result = Writer::open(
            early,
            config(factory, Retention::default(), PersistenceMetrics::default()),
        )
        .await;
        assert!(matches!(result, Err(StorageError::Corruption(_))));
    }
}

#[cfg(not(madsim))]
#[tokio::test]
async fn real_engine_recovers_encoded_namespace_identity() {
    let factory = Arc::new(crate::storage::slate::SlateFactory::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let writer = Writer::open(
        early,
        config(
            factory.clone(),
            Retention::default(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    let mut entry = write(Some("value"), None);
    entry.namespace = "space and % sign".into();
    assert_eq!(writer.commit(entry).await, Ok(1));
    writer.close().await.unwrap();

    let reopened = Writer::open(
        early,
        config(factory, Retention::default(), PersistenceMetrics::default()),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .log()
            .read("kv", "space and % sign", 1, usize::MAX)
            .await
            .unwrap()
            .len(),
        1
    );
    let view = reopened.bootstrap("kv", "space and % sign").await.unwrap();
    assert_eq!(view.sequence, 1);
    assert_eq!(view.values.get(b"key".as_slice()), Some(&b"value".to_vec()));
    reopened.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn real_engine_recovers_atomic_state_retries_and_retained_events() {
    let factory = Arc::new(crate::storage::slate::SlateFactory::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let retention = Retention {
        event_retention_ms: 50,
        retry_retention_ms: 500,
    };
    let writer = Writer::open(
        early,
        config(
            factory.clone(),
            retention.clone(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    writer
        .commit(write(Some("first"), Some("first")))
        .await
        .unwrap();
    writer.commit(write(None, Some("delete"))).await.unwrap();
    writer.close().await.unwrap();
    let writer = Writer::open(
        later,
        config(
            factory.clone(),
            retention.clone(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        writer.commit(write(Some("wrong"), Some("first"))).await,
        Ok(1)
    );
    writer.cleanup("kv", "one").await.unwrap();
    let view = writer.bootstrap("kv", "one").await.unwrap();
    assert_eq!(view.sequence, 2);
    assert_eq!(view.pruned_through, 2);
    assert!(view.values.is_empty());
    assert_eq!(view.retries["first"].expires_at, 1_500);
    writer.close().await.unwrap();
    let reopened = Writer::open(
        later,
        config(factory, retention, PersistenceMetrics::default()),
    )
    .await
    .unwrap();
    assert_eq!(reopened.bootstrap("kv", "one").await.unwrap(), view);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn idle_stream_expiry_is_immediate_and_live_retry_triggers_physical_cleanup() {
    static NOW: AtomicU64 = AtomicU64::new(1_000);
    fn now() -> u64 {
        NOW.load(Ordering::SeqCst)
    }
    let writer = Writer::open(
        now,
        config(
            Arc::new(MemoryFactory::default()),
            Retention {
                event_retention_ms: 50,
                retry_retention_ms: 500,
            },
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        writer.commit(write(Some("value"), Some("retry"))).await,
        Ok(1)
    );
    idle(&writer).await;
    let source = writer
        .namespace("kv", "one")
        .state
        .lock()
        .await
        .source
        .as_ref()
        .unwrap()
        .clone();
    assert_eq!(source.events().await.unwrap().len(), 1);
    NOW.store(1_100, Ordering::SeqCst);
    let result = writer.log().read("kv", "one", 1, 1).await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::OutOfRange);
    // Stream expiry is enforced before any physical database cleanup is requested.
    assert_eq!(source.events().await.unwrap().len(), 1);
    assert_eq!(source.progress().await.unwrap().pruned_through, 0);
    assert_eq!(
        writer.commit(write(Some("wrong"), Some("retry"))).await,
        Ok(1)
    );
    idle(&writer).await;
    assert!(source.events().await.unwrap().is_empty());
    assert_eq!(source.progress().await.unwrap().pruned_through, 1);
    let view = writer.bootstrap("kv", "one").await.unwrap();
    assert_eq!(view.values[b"key".as_slice()], b"value");
    assert_eq!(view.retries["retry"].expires_at, 1_500);
}

/// A store whose flushes fail while `failing` is set.
fn flaky_store(failing: &Arc<AtomicBool>) -> Arc<SimFactory> {
    let failing = failing.clone();
    Arc::new(SimFactory::new(Arc::new(move |access| {
        (access == StoreAccess::Flush && failing.load(Ordering::SeqCst))
            .then_some(StoreFault::Unavailable)
    })))
}

/// Found by the simulation (#25 / B-23): a membership publisher waits for a bootstrap before it writes, so waiting for a
/// later commit to settle a failed flush deadlocked them.
#[tokio::test]
async fn bootstrap_settles_a_failed_flush_without_a_later_commit() {
    let failing = Arc::new(AtomicBool::new(true));
    let writer = Writer::open(
        early,
        config(
            flaky_store(&failing),
            Retention::default(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    assert!(writer.commit(write(Some("v"), Some("r"))).await.is_err());
    assert!(writer.bootstrap("kv", "one").await.is_err());
    failing.store(false, Ordering::SeqCst);
    let view = writer.bootstrap("kv", "one").await.unwrap();
    assert_eq!(view.sequence, 1);
    assert_eq!(view.values[b"key".as_slice()], b"v");
    // The retry finds what the settled flush committed.
    assert_eq!(writer.commit(write(Some("v"), Some("r"))).await.unwrap(), 1);
}

/// A subscriber must not apply an event that a crash could still erase.
#[tokio::test]
async fn recovery_never_serves_past_the_durable_head() {
    let failing = Arc::new(AtomicBool::new(false));
    let writer = Writer::open(
        early,
        config(
            flaky_store(&failing),
            Retention::default(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        writer.commit(write(Some("durable"), None)).await.unwrap(),
        1
    );
    let (view, lease) = writer.begin_recovery("kv", "one").await.unwrap();
    assert_eq!(view.sequence, 1);
    failing.store(true, Ordering::SeqCst);
    assert!(writer.commit(write(Some("buffered"), None)).await.is_err());
    let events = writer
        .recovery_events("kv", "one", 2, &lease)
        .await
        .unwrap();
    assert!(events.is_empty());
}

/// Found by the simulation (#25 / B-23): a publisher's delayed write landed after newer ones and undid them.
#[tokio::test]
async fn a_write_planned_from_an_old_revision_is_refused() {
    let writer = Writer::open(
        early,
        config(
            Arc::new(MemoryFactory::default()),
            Retention::default(),
            PersistenceMetrics::default(),
        ),
    )
    .await
    .unwrap();
    let planned = |value, expected| Write {
        expected: Some(expected),
        ..write(Some(value), None)
    };
    assert_eq!(writer.commit(planned("first", 0)).await, Ok(1));
    assert_eq!(writer.commit(planned("second", 1)).await, Ok(2));
    assert_eq!(
        writer.commit(planned("late", 1)).await,
        Err(CommitError::Conflict)
    );
    let view = writer.bootstrap("kv", "one").await.unwrap();
    assert_eq!(view.sequence, 2);
    assert_eq!(view.values[b"key".as_slice()], b"second");
    assert_eq!(writer.commit(planned("third", 2)).await, Ok(3));
}
