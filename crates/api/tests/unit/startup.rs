use super::*;

#[tokio::test]
async fn customer_kv_writes_never_create_a_dev_api_database() {
    use bedrock_lib::storage::namespace::NamespaceFactory;
    let factory = Arc::new(bedrock_lib::storage::sim::MemoryFactory::default());
    let writer = Writer::open(
        || 0,
        Persistence {
            factory: factory.clone(),
            retention: Default::default(),
            metrics: Default::default(),
        },
    )
    .await
    .unwrap();
    for method in [Method::PUT, Method::DELETE] {
        let (status, _) = write(
            State(writer.clone()),
            Path(("kv".into(), "customer".into(), "key".into())),
            method,
            HeaderMap::new(),
            Bytes::from_static(b"value"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    assert!(factory.namespaces().await.unwrap().is_empty());
}

#[tokio::test]
async fn rejects_unusable_write_tokens() {
    let writer = Writer::open(
        || 0,
        Persistence {
            factory: Arc::new(bedrock_lib::storage::sim::MemoryFactory::default()),
            retention: Default::default(),
            metrics: Default::default(),
        },
    )
    .await
    .unwrap();
    for token in ["", " ", "with space", "with\nnewline", "non-ascii-å"] {
        assert!(app(writer.clone(), token.into(), 1).is_err());
    }
}
