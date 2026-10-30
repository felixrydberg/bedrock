use super::*;

fn materialized_batch(value: &[u8], revision: &[u8]) -> Changes {
    vec![
        (b"data/key".to_vec(), Some(value.to_vec())),
        (b"meta/revision".to_vec(), Some(revision.to_vec())),
        (b"retry/id".to_vec(), Some(b"result/fixed-expiry".to_vec())),
    ]
}

#[tokio::test]
async fn crash_loses_unflushed_value_revision_and_retry_together() {
    let factory = MemoryFactory::default();
    let db = factory.open("kv", "one").await.unwrap();
    db.write(materialized_batch(b"first", b"1")).await.unwrap();
    assert_eq!(db.scan(b"").await.unwrap().len(), 3);
    factory.crash().unwrap();
    assert!(matches!(
        db.get(b"data/key").await,
        Err(StorageError::Fenced(_))
    ));
    assert!(db.flush().await.is_err());
    assert!(db.close().await.is_err());
    let reopened = factory.open("kv", "one").await.unwrap();
    assert_eq!(reopened.scan(b"").await.unwrap(), Vec::<Entry>::new());
}

#[tokio::test]
async fn crash_keeps_flushed_batch_and_loses_buffered_overwrite_and_delete() {
    let factory = MemoryFactory::default();
    let db = factory.open("kv", "one").await.unwrap();
    db.write(materialized_batch(b"first", b"1")).await.unwrap();
    db.flush().await.unwrap();
    let expected = db.scan(b"").await.unwrap();
    db.write(vec![
        (b"data/key".to_vec(), Some(b"second".to_vec())),
        (b"meta/revision".to_vec(), Some(b"2".to_vec())),
        (b"retry/id".to_vec(), None),
    ])
    .await
    .unwrap();
    factory.crash().unwrap();
    let reopened = factory.clone().open("kv", "one").await.unwrap();
    assert_eq!(reopened.scan(b"").await.unwrap(), expected);
    reopened
        .write(vec![(b"data/key".to_vec(), None)])
        .await
        .unwrap();
    reopened.close().await.unwrap();
    assert!(matches!(
        reopened.get(b"data/key").await,
        Err(StorageError::Unavailable(_))
    ));
    let reopened = factory.open("kv", "one").await.unwrap();
    assert_eq!(reopened.get(b"data/key").await.unwrap(), None);
    assert_eq!(
        reopened.get(b"retry/id").await.unwrap(),
        Some(b"result/fixed-expiry".to_vec())
    );
}

#[tokio::test]
async fn new_handle_fences_old_flush_and_close_and_namespaces_are_isolated() {
    let factory = MemoryFactory::default();
    let old = factory.open("kv", "one").await.unwrap();
    old.write(materialized_batch(b"old", b"1")).await.unwrap();
    let new = factory.open("kv", "one").await.unwrap();
    assert_eq!(new.get(b"data/key").await.unwrap(), None);
    new.write(materialized_batch(b"new", b"2")).await.unwrap();
    new.flush().await.unwrap();
    assert!(matches!(old.flush().await, Err(StorageError::Fenced(_))));
    assert!(matches!(old.close().await, Err(StorageError::Fenced(_))));
    assert_eq!(new.get(b"data/key").await.unwrap(), Some(b"new".to_vec()));
    let other_namespace = factory.open("kv", "two").await.unwrap();
    let other_dataset = factory.open("auth", "one").await.unwrap();
    assert_eq!(
        other_namespace.scan(b"").await.unwrap(),
        Vec::<Entry>::new()
    );
    assert_eq!(other_dataset.scan(b"").await.unwrap(), Vec::<Entry>::new());
}

#[tokio::test]
async fn scan_orders_bytes_and_handles_prefix_boundaries() {
    let factory = MemoryFactory::default();
    let db = factory.open("kv", "one").await.unwrap();
    db.write(vec![
        (vec![255, 255], Some(vec![4])),
        (b"ab".to_vec(), Some(vec![2])),
        (vec![255], Some(vec![3])),
        (b"aa".to_vec(), Some(vec![1])),
        (b"b".to_vec(), Some(vec![9])),
    ])
    .await
    .unwrap();
    assert_eq!(
        db.scan(b"a").await.unwrap(),
        vec![(b"aa".to_vec(), vec![1]), (b"ab".to_vec(), vec![2])]
    );
    assert_eq!(
        db.scan(&[255]).await.unwrap(),
        vec![(vec![255], vec![3]), (vec![255, 255], vec![4])]
    );
    assert_eq!(
        db.scan(b"")
            .await
            .unwrap()
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>(),
        vec![
            b"aa".to_vec(),
            b"ab".to_vec(),
            b"b".to_vec(),
            vec![255],
            vec![255, 255]
        ]
    );
}

#[tokio::test]
async fn invalid_batch_never_partially_mutates_and_invalid_identities_fail() {
    let factory = MemoryFactory::default();
    let db = factory.open("kv", "one").await.unwrap();
    assert!(matches!(
        db.write(vec![
            (b"valid".to_vec(), Some(vec![1])),
            (Vec::new(), Some(vec![2])),
        ])
        .await,
        Err(StorageError::Unsupported(_))
    ));
    assert_eq!(db.get(b"valid").await.unwrap(), None);
    for identity in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
        assert!(factory.open("kv", identity).await.is_err());
        assert!(factory.open(identity, "one").await.is_err());
    }
}
