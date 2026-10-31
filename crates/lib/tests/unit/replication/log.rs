use super::*;
use crate::replication::Operation;

#[tokio::test]
async fn pruned_log_keeps_head_and_requires_bootstrap_for_old_cursor() {
    let log = MemoryLog::default();
    log.reset("kv", "ns", 4).unwrap();
    assert_eq!(log.head("kv", "ns").get(), 4);
    assert!(matches!(
        log.read("kv", "ns", 1, 10).await,
        Err(status) if status.code() == tonic::Code::OutOfRange
    ));
    log.append(Event {
        dataset: "kv".into(),
        namespace: "ns".into(),
        stream: "kv-0".into(),
        sequence: 5,
        epoch: 1,
        committed_at: 0,
        key: b"k".to_vec(),
        operation: Operation::Delete,
    })
    .unwrap();
    assert_eq!(log.head("kv", "ns").get(), 5);
    assert_eq!(log.read("kv", "ns", 5, 10).await.unwrap()[0].sequence, 5);
    log.prune("kv", "ns", 5).unwrap();
    assert_eq!(log.head("kv", "ns").get(), 5);
    assert!(log.read("kv", "ns", 6, 10).await.unwrap().is_empty());
    assert!(
        log.append(Event {
            dataset: "kv".into(),
            namespace: "ns".into(),
            stream: "kv-0".into(),
            sequence: 7,
            epoch: 1,
            committed_at: 0,
            key: b"k".to_vec(),
            operation: Operation::Delete,
        })
        .is_err()
    );
}
