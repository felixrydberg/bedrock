use super::*;

#[tokio::test]
async fn rejects_unusable_write_tokens() {
    let writer = Writer::open(
        || 0,
        Persistence {
            factory: Arc::new(bedrock_lib::storage::namespace::memory::MemoryFactory::default()),
            retention: Default::default(),
            metrics: Default::default(),
        },
    )
    .await
    .unwrap();
    for token in ["", " ", "with space", "with\nnewline", "non-ascii-å"] {
        assert!(app(writer.clone(), token.into()).is_err());
    }
}
