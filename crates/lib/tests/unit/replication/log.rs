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

#[tokio::test]
async fn namespace_discovery_tracks_first_creation_per_dataset() {
    let log = MemoryLog::default();
    assert_eq!(log.added().get(), 0);
    assert!(log.read("auth", "missing", 1, 10).await.unwrap().is_empty());
    assert_eq!(log.added().get(), 0, "reading must not create a namespace");

    log.head("auth", "z");
    log.reset("auth", "a", 5).unwrap();
    log.head("other", "a");
    assert_eq!(log.added().get(), 3);
    assert_eq!(log.namespaces("auth"), vec!["a", "z"]);
    assert_eq!(log.namespaces("other"), vec!["a"]);
    assert!(log.namespaces("missing").is_empty());

    // A rejected first append still creates a discoverable namespace; retries do not add it again.
    let event = Event {
        dataset: "auth".into(),
        namespace: "m".into(),
        stream: "auth-0".into(),
        sequence: 2,
        epoch: 1,
        committed_at: 0,
        key: b"k".to_vec(),
        operation: Operation::Delete,
    };
    assert!(log.append(event.clone()).is_err());
    assert_eq!(log.added().get(), 4);
    assert_eq!(log.namespaces("auth"), vec!["a", "m", "z"]);
    assert_eq!(log.head("auth", "m").get(), 0);
    assert!(log.append(event).is_err());
    assert!(log.reset("auth", "a", 6).is_err());
    assert_eq!(log.added().get(), 4);
}

#[tokio::test]
async fn namespace_creation_wakes_discovery_waiters() {
    let log = std::sync::Arc::new(MemoryLog::default());
    let waiting = log.clone();
    let waiter = tokio::spawn(async move { waiting.added().wait_until(|count| *count > 0).await });
    tokio::task::yield_now().await;
    log.reset("auth", "new", 0).unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap(),
        1
    );
}
