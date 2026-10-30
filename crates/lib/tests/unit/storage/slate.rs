use super::*;
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, path::Path,
};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

type ObjectStream<T> =
    Pin<Box<dyn tokio_stream::Stream<Item = object_store::Result<T>> + Send + 'static>>;

/// Fails one nonempty native WAL PUT, optionally after the remote object exists.
#[derive(Debug, Default)]
struct WalFaultStore {
    inner: InMemory,
    fault: AtomicUsize,
    failures: AtomicUsize,
}

impl std::fmt::Display for WalFaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WAL fault store")
    }
}

#[async_trait]
impl ObjectStore for WalFaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let fault = if location.as_ref().contains("/wal/") && payload.content_length() > 0 {
            self.fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == 0 {
            return self.inner.put_opts(location, payload, options).await;
        }
        if fault == 2 {
            self.inner.put_opts(location, payload, options).await?;
        }
        self.failures.fetch_add(1, Ordering::SeqCst);
        Err(object_store::Error::Generic {
            store: "wal-fault-test",
            source: Box::new(std::io::Error::other("injected WAL response failure")),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: ObjectStream<Path>) -> ObjectStream<Path> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> ObjectStream<ObjectMeta> {
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> ObjectStream<ObjectMeta> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[tokio::test]
async fn failed_native_wal_flush_requires_restart_and_recovers_only_remote_batches() {
    for fault in [1, 2] {
        let store = Arc::new(WalFaultStore::default());
        let factory = SlateFactory::new(store.clone());
        let db = factory.open("kv", "one").await.unwrap();
        db.write(vec![(b"baseline".to_vec(), Some(b"durable".to_vec()))])
            .await
            .unwrap();
        db.flush().await.unwrap();
        db.write(vec![
            (b"value".to_vec(), Some(b"original".to_vec())),
            (b"revision".to_vec(), Some(b"2".to_vec())),
            (b"retry".to_vec(), Some(b"fixed-result-expiry".to_vec())),
        ])
        .await
        .unwrap();
        store.fault.store(fault, Ordering::SeqCst);
        // No successful durability result may escape, even when the PUT actually landed.
        assert!(db.flush().await.is_err());
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        assert_eq!(store.fault.load(Ordering::SeqCst), 0);
        // Restoring S3 does not revive SlateDB's terminated WAL task.
        assert!(db.flush().await.is_err());
        let _ = db.close().await;
        drop(db);

        let recovered = factory.open("kv", "one").await.unwrap();
        assert_eq!(
            recovered.get(b"baseline").await.unwrap(),
            Some(b"durable".to_vec())
        );
        for (key, value) in [
            (b"value".as_slice(), b"original".as_slice()),
            (b"revision".as_slice(), b"2".as_slice()),
            (b"retry".as_slice(), b"fixed-result-expiry".as_slice()),
        ] {
            assert_eq!(
                recovered.get(key).await.unwrap(),
                (fault == 2).then(|| value.to_vec())
            );
        }
        recovered
            .write(vec![(b"resumed".to_vec(), Some(b"new".to_vec()))])
            .await
            .unwrap();
        recovered.flush().await.unwrap();
        recovered.close().await.unwrap();
    }
}

fn factory() -> SlateFactory {
    SlateFactory::new(Arc::new(InMemory::new()))
}

#[tokio::test]
async fn durable_batch_values_metadata_and_deletes_recover_together() {
    let factory = factory();
    let db = factory.open("kv", "one").await.unwrap();
    assert_eq!(db.get(b"missing").await.unwrap(), None);
    db.write(vec![
        (b"data/a".to_vec(), Some(b"value".to_vec())),
        (b"meta/revision".to_vec(), Some(b"7".to_vec())),
        (
            b"retry/request".to_vec(),
            Some(b"fixed-result-expiry".to_vec()),
        ),
    ])
    .await
    .unwrap();
    assert_eq!(db.get(b"data/a").await.unwrap(), Some(b"value".to_vec()));
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let db = factory.open("kv", "one").await.unwrap();
    assert_eq!(
        db.scan(b"data/").await.unwrap(),
        vec![(b"data/a".to_vec(), b"value".to_vec())]
    );
    assert_eq!(db.get(b"meta/revision").await.unwrap(), Some(b"7".to_vec()));
    assert_eq!(
        db.get(b"retry/request").await.unwrap(),
        Some(b"fixed-result-expiry".to_vec())
    );
    db.write(vec![
        (b"data/a".to_vec(), None),
        (b"meta/revision".to_vec(), Some(b"8".to_vec())),
    ])
    .await
    .unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let db = factory.open("kv", "one").await.unwrap();
    assert_eq!(db.get(b"data/a").await.unwrap(), None);
    assert_eq!(db.get(b"meta/revision").await.unwrap(), Some(b"8".to_vec()));
    db.close().await.unwrap();
}

#[tokio::test]
async fn namespace_and_dataset_identity_isolate_state() {
    let factory = factory();
    let first = factory.open("kv", "one").await.unwrap();
    let other_namespace = factory.open("kv", "two").await.unwrap();
    let other_dataset = factory.open("auth", "one").await.unwrap();
    first
        .write(vec![(b"key".to_vec(), Some(b"value".to_vec()))])
        .await
        .unwrap();
    first.flush().await.unwrap();
    assert_eq!(other_namespace.get(b"key").await.unwrap(), None);
    assert_eq!(other_dataset.get(b"key").await.unwrap(), None);
    for db in [first, other_namespace, other_dataset] {
        db.close().await.unwrap();
    }
    for identity in [
        "",
        "../escape",
        "nested/path",
        "back\\slash",
        "nul\0key",
        ".",
        "..",
    ] {
        assert!(factory.open("kv", identity).await.is_err());
        assert!(factory.open(identity, "one").await.is_err());
    }
}

#[tokio::test]
async fn newer_writer_fences_old_publication() {
    let factory = factory();
    let old = factory.open("kv", "one").await.unwrap();
    old.write(vec![(b"key".to_vec(), Some(b"first".to_vec()))])
        .await
        .unwrap();
    old.flush().await.unwrap();
    let new = factory.open("kv", "one").await.unwrap();
    assert_eq!(new.get(b"key").await.unwrap(), Some(b"first".to_vec()));
    // The old writer may accept into memory before observing the newer epoch, but cannot publish.
    let stale = old
        .write(vec![(b"key".to_vec(), Some(b"stale".to_vec()))])
        .await;
    if stale.is_ok() {
        assert!(old.flush().await.is_err());
    }
    new.write(vec![(b"key".to_vec(), Some(b"new".to_vec()))])
        .await
        .unwrap();
    new.flush().await.unwrap();
    new.close().await.unwrap();
    let _ = old.close().await;
    let reopened = factory.open("kv", "one").await.unwrap();
    assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"new".to_vec()));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn invalid_batch_returns_error_without_applying_valid_rows() {
    let factory = factory();
    let db = factory.open("kv", "one").await.unwrap();
    assert!(
        db.write(vec![
            (b"valid".to_vec(), Some(b"value".to_vec())),
            (Vec::new(), Some(b"invalid".to_vec())),
        ])
        .await
        .is_err()
    );
    assert_eq!(db.get(b"valid").await.unwrap(), None);
    db.close().await.unwrap();
}
