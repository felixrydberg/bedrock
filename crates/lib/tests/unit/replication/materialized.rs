use super::*;
use crate::storage::StorageEngine;
use crate::storage::memory::MemoryEngine;
use crate::storage::namespace::memory::MemoryFactory;

#[test]
fn namespace_metadata_and_retry_format_v2_golden_bytes() {
    let state = StateRecord {
        version: 2,
        dataset: "kv".into(),
        namespace: "one".into(),
        sequence: 3,
        committed_at: 400,
        raft_index: Some(9),
        raft_term: Some(2),
        pruned_through: 1,
    };
    assert_eq!(
        state.encode_to_vec(),
        vec![
            8, 2, 18, 2, b'k', b'v', 26, 3, b'o', b'n', b'e', 32, 3, 40, 144, 3, 48, 9, 56, 2, 64,
            1
        ]
    );
    let retry = StoredRetry {
        sequence: 3,
        expires_at: 700,
    };
    assert_eq!(retry.encode_to_vec(), vec![8, 3, 16, 188, 5]);
}

fn committed(
    sequence: u64,
    timestamp: u64,
    raw_key: &[u8],
    value: Option<&[u8]>,
    retry: Option<(&str, u64)>,
) -> Committed {
    Committed {
        event: Event {
            dataset: "kv".into(),
            namespace: "one".into(),
            stream: "kv-0".into(),
            sequence,
            epoch: 1,
            committed_at: timestamp,
            key: raw_key.to_vec(),
            operation: value.map_or(Operation::Delete, |value| Operation::Set(value.to_vec())),
        },
        retry_key: retry.map(|(key, _)| key.into()),
        retry_expires_at: retry.map_or(0, |(_, expires)| expires),
    }
}

async fn storage_contract(factory: &dyn NamespaceFactory) {
    let view = Materialized::open(factory, "kv", "one").await.unwrap();
    let first = committed(1, 100, b"m", Some(b"first"), Some(("m", 400)));
    let second = committed(2, 200, b"m", None, Some(("delete", 500)));
    view.apply(&[first.clone(), second.clone()]).await.unwrap();
    let before = view.view().await.unwrap();
    assert_eq!(before.sequence, 2);
    assert_eq!(before.committed_at, 200);
    assert!(before.values.is_empty());
    assert_eq!(
        before.retries["m"],
        RetryRecord {
            sequence: 1,
            expires_at: 400
        }
    );
    assert_eq!(
        before.retries["delete"],
        RetryRecord {
            sequence: 2,
            expires_at: 500
        }
    );

    let third = committed(3, 400, b"rdelete", Some(b"third"), Some(("v", 700)));
    let gap = committed(5, 450, b"bad", Some(b"bad"), None);
    assert!(view.apply(&[third.clone(), gap]).await.is_err());
    assert_eq!(view.view().await.unwrap(), before);
    view.apply(std::slice::from_ref(&third)).await.unwrap();
    view.apply(&[first, second]).await.unwrap();
    let after = view.view().await.unwrap();
    assert_eq!(after.sequence, 3);
    assert!(!after.retries.contains_key("m"));
    assert_eq!(after.retries["delete"].expires_at, 500);
    assert_eq!(after.retries["v"].expires_at, 700);
    assert_eq!(
        after.values.get(b"rdelete".as_slice()),
        Some(&b"third".to_vec())
    );
    assert_eq!(view.publish().await.unwrap(), 3);
    view.close().await.unwrap();
    let recovered = Materialized::open(factory, "kv", "one").await.unwrap();
    assert_eq!(recovered.view().await.unwrap(), after);
    let other = Materialized::open(factory, "auth", "one").await.unwrap();
    assert!(other.view().await.unwrap().values.is_empty());
    other.close().await.unwrap();
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn model_materialization_contract() {
    storage_contract(&MemoryFactory::default()).await;
}

#[cfg(not(madsim))]
#[tokio::test]
async fn actual_slatedb_materialization_contract() {
    let factory =
        crate::storage::slate::SlateFactory::new(Arc::new(object_store::memory::InMemory::new()));
    storage_contract(&factory).await;
}

#[tokio::test]
async fn crash_before_publish_loses_whole_batch_and_published_state_recovers() {
    let factory = MemoryFactory::default();
    let initial = committed(1, 100, b"m", Some(b"first"), Some(("r", 400)));
    let view = Materialized::open(&factory, "kv", "one").await.unwrap();
    view.apply(std::slice::from_ref(&initial)).await.unwrap();
    factory.crash().unwrap();
    assert!(view.publish().await.is_err());
    let recovered = Materialized::open(&factory, "kv", "one").await.unwrap();
    assert_eq!(recovered.view().await.unwrap().sequence, 0);
    assert!(recovered.view().await.unwrap().retries.is_empty());
    recovered.apply(&[initial]).await.unwrap();
    recovered.publish().await.unwrap();
    let durable = recovered.view().await.unwrap();
    recovered
        .apply(&[committed(2, 200, b"m", None, Some(("delete", 900)))])
        .await
        .unwrap();
    factory.crash().unwrap();
    let recovered = Materialized::open(&factory, "kv", "one").await.unwrap();
    assert_eq!(recovered.view().await.unwrap(), durable);
}

#[tokio::test]
async fn malformed_batch_does_not_buffer_valid_prefix() {
    let factory = MemoryFactory::default();
    let view = Materialized::open(&factory, "kv", "one").await.unwrap();
    let first = committed(1, 100, b"key", Some(b"first"), None);
    let mut wrong_scope = committed(2, 200, b"key", Some(b"wrong"), None);
    wrong_scope.event.dataset = "auth".into();
    assert!(view.apply(&[first.clone(), wrong_scope]).await.is_err());
    assert_eq!(view.view().await.unwrap().sequence, 0);
    let invalid_expiry = committed(2, 200, b"key", None, Some(("retry", 200)));
    assert!(view.apply(&[first, invalid_expiry]).await.is_err());
    assert_eq!(view.view().await.unwrap().sequence, 0);
}

#[tokio::test]
async fn open_validates_metadata_record_keys_and_retry_expiry() {
    let factory = MemoryFactory::default();
    let state = StateRecord {
        version: FORMAT_VERSION,
        dataset: "kv".into(),
        namespace: "one".into(),
        sequence: 1,
        committed_at: 100,
        raft_index: None,
        raft_term: None,
        pruned_through: 0,
    };
    let cases = vec![
        vec![(b"m".to_vec(), Some(vec![255]))],
        vec![(b"vkey".to_vec(), Some(b"orphan".to_vec()))],
        vec![
            (b"m".to_vec(), Some(state.encode_to_vec())),
            (b"unknown".to_vec(), Some(vec![1])),
        ],
        vec![
            (b"m".to_vec(), Some(state.encode_to_vec())),
            (b"v".to_vec(), Some(vec![1])),
        ],
        vec![
            (b"m".to_vec(), Some(state.encode_to_vec())),
            (
                b"rretry".to_vec(),
                Some(
                    StoredRetry {
                        sequence: 1,
                        expires_at: 100,
                    }
                    .encode_to_vec(),
                ),
            ),
        ],
    ];
    for (index, changes) in cases.into_iter().enumerate() {
        let raw = factory.open("kv", "one").await.unwrap();
        let old = raw.scan(b"").await.unwrap();
        raw.write(old.into_iter().map(|(key, _)| (key, None)).collect())
            .await
            .unwrap();
        raw.write(changes).await.unwrap();
        raw.flush().await.unwrap();
        raw.close().await.unwrap();
        if index == 2 || index == 3 {
            // Metadata-only opens avoid values; a full view still validates every record key.
            let opened = Materialized::open(&factory, "kv", "one").await.unwrap();
            assert!(matches!(
                opened.view().await,
                Err(StorageError::Corruption(_))
            ));
            opened.close().await.unwrap();
        } else {
            assert!(matches!(
                Materialized::open(&factory, "kv", "one").await,
                Err(StorageError::Corruption(_))
            ));
        }
    }
    let raw = factory.open("kv", "one").await.unwrap();
    raw.write(
        raw.scan(b"")
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| (key, None))
            .collect(),
    )
    .await
    .unwrap();
    let mut wrong_scope = state;
    wrong_scope.dataset = "auth".into();
    raw.write(vec![(b"m".to_vec(), Some(wrong_scope.encode_to_vec()))])
        .await
        .unwrap();
    raw.close().await.unwrap();
    assert!(matches!(
        Materialized::open(&factory, "kv", "one").await,
        Err(StorageError::Corruption(_))
    ));
}

#[tokio::test]
async fn install_is_namespace_scoped_atomic_and_never_goes_backwards() {
    let factory = MemoryFactory::default();
    let materialized = Materialized::open(&factory, "kv", "one").await.unwrap();
    materialized
        .apply(&[committed(1, 100, b"key", Some(b"value"), None)])
        .await
        .unwrap();
    let view = materialized.view().await.unwrap();
    let engine = MemoryEngine::new("kv");
    engine.put(b"one/old", b"old").unwrap();
    engine.put(b"two/keep", b"keep").unwrap();
    view.install(&engine, "one", KeyRange::prefix(b"one/"))
        .unwrap();
    assert_eq!(engine.get(b"one/key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(engine.get(b"one/old").unwrap(), None);
    assert_eq!(engine.get(b"two/keep").unwrap(), Some(b"keep".to_vec()));
    assert_eq!(engine.revision("one").unwrap(), Some(1));
    assert!(
        view.install(&engine, "two", KeyRange::prefix(b"two/"))
            .is_err()
    );
    let mut newer = WriteBatch::default();
    newer.set_revision("one", 2);
    engine.write(newer).unwrap();
    assert!(
        view.install(&engine, "one", KeyRange::prefix(b"one/"))
            .is_err()
    );
    assert_eq!(engine.get(b"one/key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn retention_defaults_and_validation() {
    let mut retention = Retention::default();
    assert_eq!(retention.event_retention_ms, DEFAULT_RETENTION_MS);
    assert_eq!(retention.retry_retention_ms, DEFAULT_RETENTION_MS);
    retention.validate().unwrap();
    retention.event_retention_ms = 0;
    assert!(retention.validate().is_err());
    retention.event_retention_ms = 1;
    retention.retry_retention_ms = 0;
    assert!(retention.validate().is_err());
}

#[derive(Default)]
struct ScanCountingFactory {
    inner: MemoryFactory,
    value_scans: Arc<std::sync::atomic::AtomicUsize>,
    event_scans: Arc<std::sync::atomic::AtomicUsize>,
}

struct ScanCountingDatabase {
    inner: Arc<dyn NamespaceDatabase>,
    value_scans: Arc<std::sync::atomic::AtomicUsize>,
    event_scans: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl NamespaceFactory for ScanCountingFactory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        self.inner.namespaces().await
    }
    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        Ok(Arc::new(ScanCountingDatabase {
            inner: self.inner.open(dataset, namespace).await?,
            value_scans: self.value_scans.clone(),
            event_scans: self.event_scans.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl NamespaceDatabase for ScanCountingDatabase {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        if prefix == b"e" {
            self.event_scans
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if prefix.is_empty() || prefix == b"v" {
            self.value_scans
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.scan(prefix).await
    }

    async fn write(&self, changes: Changes) -> StorageResult<()> {
        self.inner.write(changes).await
    }

    async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }

    async fn close(&self) -> StorageResult<()> {
        self.inner.close().await
    }
}

#[tokio::test]
async fn ordinary_reopen_apply_and_publish_do_not_scan_namespace_values() {
    use std::sync::atomic::Ordering;

    let factory = ScanCountingFactory::default();
    let materialized = Materialized::open(&factory, "kv", "one").await.unwrap();
    materialized
        .apply(&[committed(
            1,
            100,
            b"key",
            Some(b"value"),
            Some(("retry", 500)),
        )])
        .await
        .unwrap();
    materialized.publish().await.unwrap();
    materialized.close().await.unwrap();
    factory.value_scans.store(0, Ordering::SeqCst);
    factory.event_scans.store(0, Ordering::SeqCst);

    let materialized = Materialized::open(&factory, "kv", "one").await.unwrap();
    let progress = materialized.progress().await.unwrap();
    assert_eq!(progress.sequence, 1);
    assert_eq!(progress.committed_at, 100);
    assert_eq!(progress.retries["retry"].expires_at, 500);
    materialized
        .apply(&[committed(2, 200, b"key", Some(b"new"), None)])
        .await
        .unwrap();
    materialized.publish().await.unwrap();
    materialized
        .cleanup(200, &Retention::default())
        .await
        .unwrap();
    assert_eq!(factory.value_scans.load(Ordering::SeqCst), 0);
    assert_eq!(
        factory.event_scans.load(Ordering::SeqCst),
        0,
        "ordinary commits and young-history cleanup must not scan retained event payloads"
    );
    assert_eq!(
        materialized.view().await.unwrap().values[b"key".as_slice()],
        b"new"
    );
    assert_eq!(factory.value_scans.load(Ordering::SeqCst), 1);
    materialized.close().await.unwrap();
}

#[tokio::test]
async fn committed_raft_position_is_atomic_monotonic_and_recovers_after_crash() {
    let factory = MemoryFactory::default();
    let source = Materialized::open(&factory, "kv", "one").await.unwrap();
    let first = committed(1, 100, b"key", Some(b"value"), Some(("retry", 500)));
    let position = RaftPosition { index: 10, term: 2 };
    let progress = source
        .persist(std::slice::from_ref(&first), Some(position))
        .await
        .unwrap();
    assert_eq!(progress.sequence, 1);
    assert_eq!(progress.raft, Some(position));
    let before = source.view().await.unwrap();
    for position in [
        RaftPosition { index: 0, term: 0 },
        RaftPosition { index: 9, term: 2 },
        RaftPosition { index: 10, term: 3 },
        RaftPosition { index: 11, term: 1 },
    ] {
        assert!(matches!(
            source.persist(&[], Some(position)).await,
            Err(StorageError::Corruption(_))
        ));
        assert_eq!(source.view().await.unwrap(), before);
    }
    let mut conflicting = first.clone();
    conflicting.event.operation = Operation::Delete;
    assert!(
        source
            .persist(&[conflicting], Some(position))
            .await
            .is_err()
    );
    assert_eq!(source.view().await.unwrap(), before);
    assert!(
        source
            .persist(&[committed(2, 200, b"key", None, None)], None)
            .await
            .is_err()
    );
    let second = committed(2, 200, b"key", None, Some(("delete", 600)));
    let position = RaftPosition { index: 11, term: 3 };
    source
        .persist(std::slice::from_ref(&second), Some(position))
        .await
        .unwrap();
    let durable = source.view().await.unwrap();
    factory.crash().unwrap();
    let recovered = Materialized::open(&factory, "kv", "one").await.unwrap();
    assert_eq!(recovered.view().await.unwrap(), durable);
    assert_eq!(recovered.progress().await.unwrap().raft, Some(position));
    assert_eq!(recovered.events().await.unwrap(), [first, second]);
    assert!(durable.values.is_empty());
}

#[tokio::test]
async fn event_validation_rejects_missing_invalid_and_conflicting_retained_history() {
    for corruption in 0..3 {
        let factory = MemoryFactory::default();
        let source = Materialized::open(&factory, "kv", "one").await.unwrap();
        let first = committed(1, 100, b"key", Some(b"value"), None);
        source
            .persist(std::slice::from_ref(&first), None)
            .await
            .unwrap();
        let raw = factory.open("kv", "one").await.unwrap();
        let encoded = key(b'e', &1_u64.to_be_bytes());
        let value = match corruption {
            0 => None,
            1 => Some(vec![255]),
            _ => Some(
                StoredEvent {
                    event: Some(committed(2, 100, b"key", Some(b"wrong"), None).event.into()),
                    retry_key: None,
                    retry_expires_at: 0,
                }
                .encode_to_vec(),
            ),
        };
        raw.write(vec![(encoded, value)]).await.unwrap();
        raw.flush().await.unwrap();
        let reopened = Materialized::open(&factory, "kv", "one").await.unwrap();
        assert!(matches!(
            reopened.events().await,
            Err(StorageError::Corruption(_))
        ));
    }
}

#[tokio::test]
async fn raft_position_without_any_namespace_event_leaves_empty_storage_healthy() {
    let factory = MemoryFactory::default();
    let source = Materialized::open(&factory, "kv", "one").await.unwrap();
    assert!(matches!(
        source
            .persist(&[], Some(RaftPosition { index: 1, term: 1 }))
            .await,
        Err(StorageError::Unsupported(_))
    ));
    assert_eq!(source.progress().await.unwrap().sequence, 0);
    assert_eq!(source.progress().await.unwrap().raft, None);
    assert!(source.events().await.unwrap().is_empty());
    let raw = factory.open("kv", "one").await.unwrap();
    assert!(raw.scan(b"").await.unwrap().is_empty());
    assert_eq!(raw.get(META_KEY).await.unwrap(), None);
    raw.close().await.unwrap();
    let reopened = Materialized::open(&factory, "kv", "one").await.unwrap();
    assert_eq!(reopened.view().await.unwrap().sequence, 0);
    assert!(reopened.events().await.unwrap().is_empty());
}
