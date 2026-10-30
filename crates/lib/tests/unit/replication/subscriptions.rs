use super::*;
use crate::replication::ApplierMetrics;
use crate::storage::memory::MemoryEngine;
use crate::storage::{StorageEngine, StorageResult, WriteBatch};

struct Hook;

impl ApplyHook for Hook {
    fn apply(&mut self, _: &dyn StorageEngine, _: &Event, _: &mut WriteBatch) -> StorageResult<()> {
        Ok(())
    }
}

fn batch(sequences: &[u64]) -> pb::Batch {
    pb::Batch {
        events: sequences
            .iter()
            .map(|&sequence| pb::Event {
                dataset: "kv".into(),
                namespace: "ns".into(),
                sequence,
                ..Default::default()
            })
            .collect(),
    }
}

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
async fn gaps_publish_progress_and_retry_only_the_remaining_gap() {
    let engine = Arc::new(MemoryEngine::new("kv"));
    let mut applier = Applier::new("kv", engine.clone(), Hook, &ApplierMetrics::default());
    applier
        .offer(batch(&[5]).events.pop().unwrap().into())
        .unwrap();
    let applied = Arc::<Applied>::default();
    let subscription = Subscription {
        applied: applied.clone(),
        recovery: Arc::new(Watched::new(None)),
        task: tokio::spawn(std::future::pending()),
    };
    let (requests, mut rx) = mpsc::unbounded_channel();

    apply_batch(&mut applier, &applied, batch(&[1, 2]), &requests, "source").unwrap();
    assert_eq!(engine.revision("ns"), Ok(Some(2)));
    tokio::time::timeout(Duration::from_secs(1), subscription.wait_for("ns", 2))
        .await
        .unwrap();
    assert_eq!(rx.try_recv().unwrap(), subscribe("kv", "ns", 3));
    assert_eq!(rx.try_recv().unwrap(), ack("kv", "ns", 2));
    assert!(rx.try_recv().is_err());

    // An earlier request must not suppress a retry after the missing cursor advances.
    apply_batch(&mut applier, &applied, batch(&[3]), &requests, "source").unwrap();
    assert_eq!(rx.try_recv().unwrap(), subscribe("kv", "ns", 4));
    assert_eq!(rx.try_recv().unwrap(), ack("kv", "ns", 3));
    assert!(rx.try_recv().is_err());

    // A gap resolved within a batch needs no replay.
    apply_batch(
        &mut applier,
        &applied,
        batch(&[7, 4, 6]),
        &requests,
        "source",
    )
    .unwrap();
    assert_eq!(applied.get("ns").get(), 7);
    assert_eq!(rx.try_recv().unwrap(), ack("kv", "ns", 7));
    assert!(rx.try_recv().is_err());
    subscription.stop().await;
}

#[tokio::test]
async fn stop_waits_until_the_task_releases_its_resources() {
    let (started, ready) = tokio::sync::oneshot::channel();
    let (held, mut released) = tokio::sync::oneshot::channel::<()>();
    let subscription = Subscription {
        applied: Arc::default(),
        recovery: Arc::new(Watched::new(None)),
        task: tokio::spawn(async move {
            let _held = held;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        }),
    };
    ready.await.unwrap();
    subscription.stop().await;
    assert_eq!(
        released.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
}
