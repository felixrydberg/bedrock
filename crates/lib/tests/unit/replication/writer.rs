use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::replication::log::Log;
use crate::storage::namespace::memory::MemoryFactory;
use crate::storage::namespace::{Changes, NamespaceDatabase, NamespaceFactory};
use crate::storage::{Entry, ErrorContext, StorageError, StorageResult};
use async_trait::async_trait;

#[derive(Clone, Default)]
pub(super) struct Factory {
    pub(super) inner: MemoryFactory,
    pub(super) controls: Arc<Controls>,
}

#[derive(Default)]
pub(super) struct Controls {
    pub(super) flush_fault: AtomicUsize,
    pub(super) block_flush: AtomicBool,
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
    pub(super) writes: Mutex<Vec<Changes>>,
    pub(super) fail_cleanup: AtomicBool,
}

struct Database {
    inner: Arc<dyn NamespaceDatabase>,
    controls: Arc<Controls>,
}

fn failed() -> StorageError {
    StorageError::Unavailable(ErrorContext {
        engine: "fault-test",
        instance: "namespace".into(),
        file: None,
        partition: None,
        detail: "injected storage failure".into(),
    })
}

#[async_trait]
impl NamespaceFactory for Factory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        self.inner.namespaces().await
    }
    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        Ok(Arc::new(Database {
            inner: self.inner.open(dataset, namespace).await?,
            controls: self.controls.clone(),
        }))
    }
}

#[async_trait]
impl NamespaceDatabase for Database {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        self.inner.scan(prefix).await
    }
    async fn write(&self, changes: Changes) -> StorageResult<()> {
        tokio::time::sleep(Duration::from_millis(1)).await;
        if changes
            .iter()
            .any(|(key, value)| key.starts_with(b"e") && value.is_none())
            && self.controls.fail_cleanup.load(Ordering::SeqCst)
        {
            return Err(failed());
        }
        self.controls.writes.lock().unwrap().push(changes.clone());
        self.inner.write(changes).await
    }
    async fn flush(&self) -> StorageResult<()> {
        if self.controls.block_flush.swap(false, Ordering::SeqCst) {
            self.controls.entered.notify_one();
            self.controls.release.notified().await;
        }
        let fault = self.controls.flush_fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(failed());
        }
        self.inner.flush().await?;
        if fault == 2 {
            return Err(failed());
        }
        Ok(())
    }
    async fn close(&self) -> StorageResult<()> {
        self.inner.close().await
    }
}

fn clock() -> u64 {
    1_700_000_000_000
}

pub(super) async fn open(factory: Factory) -> Arc<Writer> {
    Writer::open(
        clock,
        Persistence {
            factory: Arc::new(factory),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap()
}

fn set(namespace: &str, key: &str, value: &str, idempotency_key: Option<&str>) -> Write {
    Write {
        dataset: "kv".into(),
        namespace: namespace.into(),
        key: key.into(),
        operation: Operation::Set(value.into()),
        idempotency_key: idempotency_key.map(Into::into),
    }
}

async fn events(writer: &Writer, namespace: &str) -> Vec<Event> {
    writer
        .log
        .read("kv", namespace, 1, usize::MAX)
        .await
        .unwrap()
}

fn sequences(events: &[Event]) -> Vec<u64> {
    events.iter().map(|e| e.sequence).collect()
}

#[tokio::test]
async fn concurrent_writes_get_gap_free_sequences_in_batches() {
    let store = Factory::default();
    let writer = open(store.clone()).await;
    let tasks: Vec<_> = (0..300)
        .map(|i| {
            let writer = writer.clone();
            let namespace = ["a", "b", "c"][i % 3];
            tokio::spawn(async move {
                let sequence = writer
                    .commit(set(namespace, "k", &i.to_string(), None))
                    .await;
                (namespace, i, sequence.unwrap())
            })
        })
        .collect();
    let mut committed = BTreeMap::<&str, Vec<(u64, usize)>>::new();
    for task in tasks {
        let (namespace, i, sequence) = task.await.unwrap();
        committed.entry(namespace).or_default().push((sequence, i));
    }

    for (namespace, mut acked) in committed {
        acked.sort();
        let logged = events(&writer, namespace).await;
        assert_eq!(
            sequences(&logged),
            (1..=100).collect::<Vec<_>>(),
            "{namespace}"
        );
        for ((sequence, i), event) in acked.iter().zip(&logged) {
            assert_eq!(event.sequence, *sequence);
            assert_eq!(event.operation, Operation::Set(i.to_string().into()));
        }
    }
    let batches = store
        .controls
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter(|changes| {
            changes
                .iter()
                .any(|(key, value)| key.starts_with(b"e") && value.is_some())
        })
        .count();
    assert!(batches < 300, "{batches} batches for 300 writes");
}

#[tokio::test]
async fn acknowledged_writes_and_keys_survive_a_restart() {
    let store = Factory::default();
    let writer = open(store.clone()).await;
    writer.commit(set("a", "x", "1", Some("w1"))).await.unwrap();
    writer.commit(set("b", "x", "1", None)).await.unwrap();
    writer.commit(set("a", "y", "2", None)).await.unwrap();
    let before = events(&writer, "a").await;
    store.inner.crash().unwrap();
    drop(writer);

    let writer = open(store).await;
    assert_eq!(events(&writer, "a").await, before);
    assert_eq!(writer.commit(set("a", "x", "1", Some("w1"))).await, Ok(1));
    assert_eq!(writer.commit(set("a", "z", "3", None)).await, Ok(3));
    assert_eq!(writer.commit(set("b", "z", "3", None)).await, Ok(2));
}

#[tokio::test]
async fn a_new_engine_handle_fences_the_old_writer_without_duplicate_acknowledgements() {
    let factory = Factory::default();
    let old = open(factory.clone()).await;
    assert_eq!(old.commit(set("a", "x", "old", None)).await, Ok(1));
    let new = open(factory.clone()).await;
    assert_eq!(
        old.commit(set("a", "x", "wrong", None)).await,
        Err(CommitError::Fenced)
    );
    assert_eq!(new.commit(set("a", "x", "new", None)).await, Ok(2));
    factory.inner.crash().unwrap();
    let reopened = open(factory).await;
    assert_eq!(sequences(&events(&reopened, "a").await), [1, 2]);
    assert_eq!(
        reopened.bootstrap("kv", "a").await.unwrap().values[b"x".as_slice()],
        b"new"
    );
}

#[tokio::test]
async fn retries_with_the_same_key_apply_once() {
    let writer = open(Factory::default()).await;
    let (a, b) = tokio::join!(
        writer.commit(set("a", "x", "1", Some("k"))),
        writer.commit(set("a", "x", "1", Some("k"))),
    );
    assert_eq!((a, b), (Ok(1), Ok(1)));
    assert_eq!(writer.commit(set("a", "x", "1", Some("k"))).await, Ok(1));
    // Keys are per namespace.
    assert_eq!(writer.commit(set("b", "x", "1", Some("k"))).await, Ok(1));
    assert_eq!(sequences(&events(&writer, "a").await), [1]);
}

#[tokio::test]
async fn flush_failures_before_and_after_durability_preserve_the_original_batch() {
    static NOW: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1_000);
    fn now() -> u64 {
        NOW.load(Ordering::SeqCst)
    }
    for fault in [1, 2] {
        let factory = Factory::default();
        NOW.store(1_000, Ordering::SeqCst);
        let writer = Writer::open(
            now,
            Persistence {
                factory: Arc::new(factory.clone()),
                retention: Retention::default(),
                metrics: PersistenceMetrics::default(),
            },
        )
        .await
        .unwrap();
        factory.controls.flush_fault.store(fault, Ordering::SeqCst);
        let result = writer
            .commit(set("a", "x", "original", Some("retry")))
            .await;
        assert!(matches!(result, Err(CommitError::Unavailable(_))));
        assert!(events(&writer, "a").await.is_empty());
        let writes = factory.controls.writes.lock().unwrap().clone();
        assert_eq!(writes.len(), 1);
        NOW.store(2_000, Ordering::SeqCst);
        assert_eq!(
            writer
                .commit(set("a", "x", "replacement", Some("retry")))
                .await,
            Ok(1)
        );
        // Retrying an uncertain flush reuses exactly the buffered event and fixed expiry.
        assert_eq!(*factory.controls.writes.lock().unwrap(), writes);
        let before = writer.bootstrap("kv", "a").await.unwrap();
        assert_eq!(before.values[b"x".as_slice()], b"original");
        assert_eq!(
            before.retries["retry"].expires_at,
            1_000 + Retention::default().retry_retention_ms
        );
        factory.inner.crash().unwrap();
        let reopened = Writer::open(
            now,
            Persistence {
                factory: Arc::new(factory),
                retention: Retention::default(),
                metrics: PersistenceMetrics::default(),
            },
        )
        .await
        .unwrap();
        assert_eq!(reopened.bootstrap("kv", "a").await.unwrap(), before);
        assert_eq!(sequences(&events(&reopened, "a").await), [1]);
    }
}

#[tokio::test]
async fn acknowledgement_waits_for_flush_of_one_atomic_event_state_retry_batch() {
    let factory = Factory::default();
    let writer = open(factory.clone()).await;
    factory.controls.block_flush.store(true, Ordering::SeqCst);
    let task = tokio::spawn({
        let writer = writer.clone();
        async move { writer.commit(set("a", "x", "value", Some("retry"))).await }
    });
    tokio::time::timeout(Duration::from_secs(5), factory.controls.entered.notified())
        .await
        .unwrap();
    assert!(!task.is_finished());
    assert!(events(&writer, "a").await.is_empty());
    let changes = factory.controls.writes.lock().unwrap().clone();
    assert_eq!(changes.len(), 1);
    let keys: Vec<_> = changes[0].iter().map(|(key, _)| key.as_slice()).collect();
    assert_eq!(keys.len(), 4);
    assert!(keys.contains(&b"vx".as_slice()));
    assert!(keys.contains(&b"rretry".as_slice()));
    assert!(keys.contains(&b"m".as_slice()));
    assert!(keys.iter().any(|key| key.len() == 9 && key[0] == b'e'));
    factory.controls.release.notify_one();
    assert_eq!(task.await.unwrap(), Ok(1));
    factory.inner.crash().unwrap();
    let reopened = open(factory).await;
    let view = reopened.bootstrap("kv", "a").await.unwrap();
    assert_eq!(view.sequence, 1);
    assert_eq!(view.values[b"x".as_slice()], b"value");
    assert_eq!(view.retries["retry"].sequence, 1);
}

#[tokio::test]
async fn full_queues_and_malformed_writes_are_refused() {
    let writer = open(Factory::default()).await;
    // Holding the namespace as a batch in flight would, so the commits below queue.
    let namespace = writer.namespace("kv", "a");
    let in_flight = namespace.state.lock().await;
    let queued: Vec<_> = (0..MAX_QUEUED)
        .map(|i| {
            let writer = writer.clone();
            tokio::spawn(async move { writer.commit(set("a", "k", &i.to_string(), None)).await })
        })
        .collect();
    while namespace.queue.lock().unwrap().len() < MAX_QUEUED {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        writer.commit(set("a", "k", "v", None)).await,
        Err(CommitError::Busy)
    );
    drop(in_flight);
    for commit in queued {
        commit.await.unwrap().unwrap();
    }

    for write in [
        set("a/b", "k", "v", None),
        set("", "k", "v", None),
        set("a", "", "v", None),
    ] {
        assert!(matches!(
            writer.commit(write).await,
            Err(CommitError::Invalid(_))
        ));
    }
}

#[tokio::test]
async fn crashing_after_unknown_flush_outcome_recovers_only_remotely_durable_batches() {
    for fault in [1, 2] {
        let factory = Factory::default();
        let writer = open(factory.clone()).await;
        factory.controls.flush_fault.store(fault, Ordering::SeqCst);
        assert!(matches!(
            writer
                .commit(set("a", "x", "original", Some("retry")))
                .await,
            Err(CommitError::Unavailable(_))
        ));
        factory.inner.crash().unwrap();
        let reopened = open(factory).await;
        let view = reopened.bootstrap("kv", "a").await.unwrap();
        assert_eq!(view.sequence, if fault == 1 { 0 } else { 1 });
        assert_eq!(view.retries.contains_key("retry"), fault == 2);
        assert_eq!(
            reopened
                .commit(set("a", "x", "replacement", Some("retry")))
                .await,
            Ok(1)
        );
        let expected = if fault == 1 {
            b"replacement".as_slice()
        } else {
            b"original".as_slice()
        };
        assert_eq!(
            reopened.bootstrap("kv", "a").await.unwrap().values[b"x".as_slice()],
            expected
        );
        assert_eq!(sequences(&events(&reopened, "a").await), [1]);
    }
}
