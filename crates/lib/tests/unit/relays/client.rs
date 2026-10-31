use super::*;
use crate::storage::memory::MemoryEngine;

#[tokio::test]
async fn stop_joins_the_task_before_local_storage_can_be_replaced() {
    let (started, ready) = tokio::sync::oneshot::channel();
    let (held, mut released) = tokio::sync::oneshot::channel::<()>();
    let session = Session {
        progress: Arc::default(),
        changes: Arc::new(Watched::new(0)),
        task: tokio::spawn(async move {
            let _held = held;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        }),
        draining: None,
    };
    ready.await.unwrap();
    session.stop().await;
    assert_eq!(
        released.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
}

fn target(engine: Arc<MemoryEngine>, buckets: Vec<u32>) -> Target {
    Target {
        selection: Selection {
            dataset: "auth".into(),
            namespace: "users".into(),
            buckets,
        },
        engine,
        prefix: b"users\0".to_vec(),
    }
}

fn event(sequence: u64, key: &[u8], value: Option<&[u8]>) -> pb::Event {
    pb::Event {
        dataset: "auth".into(),
        namespace: "users".into(),
        stream: "auth-0".into(),
        sequence,
        epoch: 1,
        committed_at: 700,
        key: key.to_vec(),
        value: value.map(<[u8]>::to_vec),
    }
}

fn delivery(from: u64, through: u64, events: Vec<pb::Event>) -> pb::Delivery {
    pb::Delivery {
        dataset: "auth".into(),
        namespace: "users".into(),
        from,
        through,
        events: events
            .into_iter()
            .map(|event| event.encode_to_vec().into())
            .collect(),
    }
}

fn bootstrap(boundary: u64, entries: Vec<(&[u8], &[u8])>) -> pb::Bootstrap {
    let mut digest = bootstrap_digest("auth", "users", boundary, 700);
    let entries = entries
        .into_iter()
        .map(|(key, value)| {
            for part in [key, value] {
                digest.update((part.len() as u64).to_be_bytes());
                digest.update(part);
            }
            pb::StateEntry {
                key: key.to_vec(),
                value: value.to_vec(),
            }
        })
        .collect();
    pb::Bootstrap {
        dataset: "auth".into(),
        namespace: "users".into(),
        boundary,
        committed_at: 700,
        begin: true,
        end: true,
        entries,
        checksum: digest.finalize().to_vec(),
    }
}

#[test]
fn filtered_coverage_commits_empty_intervals_and_resumes_only_its_scope() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let bucket = bucket_of("auth", "users", b"k");
    let mut consumer = Consumer::new(target(engine.clone(), vec![bucket])).unwrap();
    assert!(
        consumer
            .delivery(delivery(1, 5, vec![event(3, b"k", Some(b"v"))]))
            .unwrap()
    );
    assert_eq!(consumer.applied, 5);
    assert_eq!(engine.get(b"users\0k").unwrap(), Some(b"v".to_vec()));
    assert!(consumer.delivery(delivery(6, 9, vec![])).unwrap());
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(9));
    let resumed = Consumer::new(target(engine.clone(), vec![bucket, bucket])).unwrap();
    assert_eq!(resumed.applied, 9);
    assert!(!resumed.recover);
    let widened = Consumer::new(target(engine, vec![])).unwrap();
    assert_eq!(widened.applied, 0);
    assert!(widened.recover);
}

#[test]
fn gap_and_malformed_delivery_never_advance_or_partially_apply() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone(), vec![])).unwrap();
    assert!(
        !consumer
            .delivery(delivery(2, 2, vec![event(2, b"gap", Some(b"v"))]))
            .unwrap()
    );
    assert!(engine.get(b"users\0gap").unwrap().is_none());
    let mut wrong = event(2, b"wrong", Some(b"v"));
    wrong.namespace = "other".into();
    assert!(
        consumer
            .delivery(delivery(1, 2, vec![event(1, b"k", Some(b"v")), wrong]))
            .is_err()
    );
    assert!(engine.get(b"users\0k").unwrap().is_none());
    assert_eq!(consumer.applied, 0);
    assert!(
        consumer
            .delivery(delivery(1, 2, vec![event(1, b"k", Some(b"v"))]))
            .is_err()
    );
    assert!(
        consumer
            .delivery(delivery(1, 1, vec![event(1, b"k", Some(b"v"))]))
            .unwrap()
    );
    assert!(
        consumer
            .delivery(delivery(2, 2, vec![event(2, b"k", None)]))
            .unwrap()
    );
    assert!(engine.get(b"users\0k").unwrap().is_none());
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(2));
}

#[test]
fn duplicate_coverage_does_not_reapply_old_values() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone(), vec![])).unwrap();
    consumer
        .delivery(delivery(1, 1, vec![event(1, b"k", Some(b"first"))]))
        .unwrap();
    consumer
        .delivery(delivery(2, 2, vec![event(2, b"k", Some(b"second"))]))
        .unwrap();
    consumer
        .delivery(delivery(1, 1, vec![event(1, b"k", Some(b"first"))]))
        .unwrap();
    assert_eq!(engine.get(b"users\0k").unwrap(), Some(b"second".to_vec()));
}

#[test]
fn filtered_delivery_rejects_other_bucket_even_in_an_old_interval() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let bucket = bucket_of("auth", "users", b"k");
    assert_ne!(bucket, bucket_of("auth", "users", b"other"));
    let mut consumer = Consumer::new(target(engine, vec![bucket])).unwrap();
    consumer.delivery(delivery(1, 1, vec![])).unwrap();
    assert!(
        consumer
            .delivery(delivery(1, 1, vec![event(1, b"other", Some(b"v"))]))
            .is_err()
    );
    assert_eq!(consumer.applied, 1);
}

#[test]
fn customer_kv_and_invalid_bucket_are_not_pushed_selections() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut kv = target(engine.clone(), vec![]);
    kv.selection.dataset = "kv".into();
    assert!(Consumer::new(kv).is_err());
    assert!(Consumer::new(target(engine, vec![COUNT])).is_err());
}

#[test]
fn corrupt_or_unsorted_bootstrap_leaves_data_and_revision_untouched() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone(), vec![])).unwrap();
    consumer
        .delivery(delivery(1, 1, vec![event(1, b"old", Some(b"value"))]))
        .unwrap();
    let mut corrupt = bootstrap(9, vec![(b"new", b"value")]);
    corrupt.checksum[0] ^= 1;
    assert!(consumer.bootstrap(corrupt).is_err());
    assert_eq!(engine.get(b"users\0old").unwrap(), Some(b"value".to_vec()));
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(1));
    assert!(engine.get(b"users\0new").unwrap().is_none());
    assert!(
        consumer
            .bootstrap(bootstrap(9, vec![(b"z", b"v"), (b"a", b"v")]))
            .is_err()
    );
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(1));
}

#[test]
fn bootstrap_replaces_only_selected_scope_and_commits_its_boundary() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    engine.put(b"users\0old", b"stale").unwrap();
    engine.put(b"elsewhere\0old", b"unrelated").unwrap();
    let mut consumer = Consumer::new(target(engine.clone(), vec![])).unwrap();
    assert!(consumer.recover);
    assert!(
        consumer
            .bootstrap(bootstrap(9, vec![(b"new", b"value")]))
            .unwrap()
    );
    assert!(engine.get(b"users\0old").unwrap().is_none());
    assert_eq!(engine.get(b"users\0new").unwrap(), Some(b"value".to_vec()));
    assert_eq!(
        engine.get(b"elsewhere\0old").unwrap(),
        Some(b"unrelated".to_vec())
    );
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(9));
    assert!(!consumer.recover);
    assert!(
        consumer
            .delivery(delivery(10, 10, vec![event(10, b"new", None)]))
            .unwrap()
    );
}

#[test]
fn bootstrap_header_is_checksummed_and_chunks_are_invisible_until_end() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone(), vec![])).unwrap();
    let complete = bootstrap(9, vec![(b"k", b"v")]);
    let begin = pb::Bootstrap {
        end: false,
        checksum: vec![],
        ..complete.clone()
    };
    assert!(!consumer.bootstrap(begin).unwrap());
    assert!(engine.get(b"users\0k").unwrap().is_none());
    assert_eq!(engine.revision(&consumer.revision).unwrap(), None);
    let end = pb::Bootstrap {
        begin: false,
        entries: vec![],
        ..complete
    };
    assert!(consumer.bootstrap(end).unwrap());
    assert_eq!(engine.get(b"users\0k").unwrap(), Some(b"v".to_vec()));
    let mut changed = bootstrap(10, vec![(b"k", b"changed")]);
    changed.committed_at += 1;
    assert!(consumer.bootstrap(changed).is_err());
    assert_eq!(engine.get(b"users\0k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn selector_change_bootstraps_before_reusing_progress_and_preserves_other_buckets() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let first = bucket_of("auth", "users", b"first");
    let second = bucket_of("auth", "users", b"second");
    assert_ne!(first, second);
    let mut original = Consumer::new(target(engine.clone(), vec![])).unwrap();
    original
        .delivery(delivery(
            1,
            2,
            vec![
                event(1, b"first", Some(b"old")),
                event(2, b"second", Some(b"keep")),
            ],
        ))
        .unwrap();
    let mut filtered = Consumer::new(target(engine.clone(), vec![first])).unwrap();
    assert!(filtered.recover);
    assert!(matches!(
        filtered.request().kind,
        Some(pb::relay_request::Kind::Recover(pb::Recover {
            follow: Some(pb::Follow { from: 0, .. }),
            ..
        }))
    ));
    assert!(filtered.delivery(delivery(1, 5, vec![])).is_err());
    filtered
        .bootstrap(bootstrap(9, vec![(b"first", b"new")]))
        .unwrap();
    assert_eq!(engine.get(b"users\0first").unwrap(), Some(b"new".to_vec()));
    assert_eq!(
        engine.get(b"users\0second").unwrap(),
        Some(b"keep".to_vec())
    );
    let widened = Consumer::new(target(engine, vec![])).unwrap();
    assert!(widened.recover);
    assert_eq!(
        widened.applied, 0,
        "old full-scope cursor cannot become the new baseline"
    );
}

fn range(head: u64, recovered: bool) -> pb::ReplayRange {
    pb::ReplayRange {
        dataset: "auth".into(),
        namespace: "users".into(),
        first: 0,
        last: 0,
        head,
        recovered,
    }
}

#[tokio::test]
async fn installed_bootstrap_and_tail_wait_for_source_confirmation_before_reads_are_ready() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone(), vec![])).unwrap();
    assert!(
        consumer
            .bootstrap(bootstrap(9, vec![(b"k", b"v")]))
            .unwrap()
    );
    assert_eq!(engine.get(b"users\0k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(9));
    assert_eq!(
        engine.revision(&consumer.pending_confirmation).unwrap(),
        Some(1)
    );
    assert!(consumer.awaiting_confirmation);
    assert!(!consumer.validated());
    consumer.range(&range(9, false)).unwrap();
    assert!(
        !consumer.validated(),
        "ordinary range cannot confirm recovery"
    );
    consumer.range(&range(10, true)).unwrap();
    assert!(
        !consumer.validated(),
        "confirmation beyond committed coverage cannot satisfy readiness"
    );
    let scope = consumer.scope();
    let progress = Arc::new(Mutex::new(Progress {
        connected: true,
        scopes: BTreeMap::from([(scope.clone(), (9, Some(10), consumer.validated()))]),
        error: None,
    }));
    let changes = Arc::new(Watched::new(0));
    let session = Session {
        progress: progress.clone(),
        changes: changes.clone(),
        task: tokio::spawn(std::future::pending()),
        draining: None,
    };
    assert!(!session.ready());
    assert!(
        tokio::time::timeout(
            Duration::from_millis(1),
            session.wait_for("auth", "users", 9)
        )
        .await
        .is_err()
    );
    consumer
        .delivery(delivery(10, 10, vec![event(10, b"k", Some(b"tail"))]))
        .unwrap();
    assert!(!consumer.validated());
    consumer.range(&range(10, true)).unwrap();
    assert!(consumer.validated());
    assert_eq!(
        engine.revision(&consumer.pending_confirmation).unwrap(),
        Some(0)
    );
    changed(&progress, &changes, |state| {
        state
            .scopes
            .insert(scope, (10, Some(10), consumer.validated()));
    });
    assert!(session.ready());
    tokio::time::timeout(
        Duration::from_millis(1),
        session.wait_for("auth", "users", 10),
    )
    .await
    .unwrap();
    let reopened = Consumer::new(target(engine, vec![])).unwrap();
    assert!(reopened.validated());
}

#[test]
fn missing_confirmation_forces_fresh_recovery_after_disconnect_or_process_restart() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone(), vec![])).unwrap();
    consumer
        .bootstrap(bootstrap(9, vec![(b"k", b"v")]))
        .unwrap();
    consumer.reconnect();
    assert!(!consumer.validated());
    assert!(matches!(
        consumer.request().kind,
        Some(pb::relay_request::Kind::Recover(pb::Recover {
            follow: Some(pb::Follow { from: 0, .. }),
            ..
        }))
    ));
    consumer.range(&range(9, false)).unwrap();
    consumer.range(&range(9, true)).unwrap();
    assert!(
        !consumer.validated(),
        "reconnect requires a fresh bootstrap before confirming"
    );
    let mut reopened = Consumer::new(target(engine, vec![])).unwrap();
    assert!(reopened.awaiting_confirmation);
    assert!(reopened.recover);
    assert!(!reopened.validated());
    reopened.range(&range(9, true)).unwrap();
    assert!(!reopened.validated());
    reopened
        .bootstrap(bootstrap(9, vec![(b"k", b"fresh")]))
        .unwrap();
    reopened.range(&range(9, true)).unwrap();
    assert!(reopened.validated());
}

#[test]
fn early_confirmation_never_confirms_an_incomplete_bootstrap() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine, vec![])).unwrap();
    let complete = bootstrap(0, vec![]);
    consumer
        .bootstrap(pb::Bootstrap {
            end: false,
            checksum: vec![],
            ..complete.clone()
        })
        .unwrap();
    consumer.range(&range(0, true)).unwrap();
    assert!(!consumer.validated());
    consumer
        .bootstrap(pb::Bootstrap {
            begin: false,
            ..complete
        })
        .unwrap();
    assert!(!consumer.validated());
    consumer.range(&range(0, false)).unwrap();
    assert!(!consumer.validated());
    consumer.range(&range(0, true)).unwrap();
    assert!(consumer.validated());
}

#[tokio::test]
async fn scoped_remote_error_keeps_other_dataset_delivering_and_minimum_reads_available() {
    let mut auth = Consumer::new(target(Arc::new(MemoryEngine::new("auth")), vec![])).unwrap();
    let healthy_engine = Arc::new(MemoryEngine::new("quicksilver"));
    let mut healthy_target = target(healthy_engine.clone(), vec![]);
    healthy_target.selection.dataset = "quicksilver".into();
    let mut healthy = Consumer::new(healthy_target).unwrap();
    let mut intake = Intake::default();
    let auth_scope = auth.scope();
    let healthy_scope = healthy.scope();
    let failed = intake
        .apply(
            &auth_scope,
            &mut auth,
            Action::Error(pb::RelayError {
                dataset: "auth".into(),
                namespace: "users".into(),
                code: tonic::Code::Unavailable as u32,
                detail: "auth storage unavailable".into(),
            }),
        )
        .unwrap();
    assert!(failed.error.is_some());
    assert!(!auth.validated());
    let mut event = event(1, b"k", Some(b"configuration"));
    event.dataset = "quicksilver".into();
    let mut batch = delivery(1, 1, vec![event]);
    batch.dataset = "quicksilver".into();
    assert!(
        intake
            .apply(&healthy_scope, &mut healthy, Action::Delivery(batch))
            .unwrap()
            .acknowledge
    );
    assert_eq!(
        healthy_engine.get(b"users\0k").unwrap(),
        Some(b"configuration".to_vec())
    );
    let progress = Arc::new(Mutex::new(Progress {
        connected: true,
        scopes: BTreeMap::from([
            (auth_scope.clone(), (0, Some(1), auth.validated())),
            (healthy_scope, (1, Some(1), healthy.validated())),
        ]),
        error: failed.error,
    }));
    let session = Session {
        progress,
        changes: Arc::new(Watched::new(0)),
        task: tokio::spawn(std::future::pending()),
        draining: None,
    };
    assert!(!session.ready());
    assert!(session.error().is_some());
    tokio::time::timeout(
        Duration::from_millis(1),
        session.wait_for("quicksilver", "users", 1),
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(1),
            session.wait_for("auth", "users", 0)
        )
        .await
        .is_err()
    );

    // Dispatch each queued recovery once; another tick does not restart an in-progress bootstrap.
    assert_eq!(
        std::mem::take(&mut intake.retries),
        BTreeSet::from([auth_scope.clone()])
    );
    assert!(matches!(
        auth.request().kind,
        Some(pb::relay_request::Kind::Recover(pb::Recover {
            follow: Some(pb::Follow { from: 0, .. }),
            ..
        }))
    ));
    let complete = bootstrap(1, vec![(b"k", b"recovered")]);
    intake
        .apply(
            &auth_scope,
            &mut auth,
            Action::Bootstrap(pb::Bootstrap {
                end: false,
                checksum: vec![],
                ..complete.clone()
            }),
        )
        .unwrap();
    assert!(std::mem::take(&mut intake.retries).is_empty());
    assert!(auth.bootstrap.is_some());
    assert!(
        intake
            .apply(
                &auth_scope,
                &mut auth,
                Action::Bootstrap(pb::Bootstrap {
                    begin: false,
                    entries: vec![],
                    ..complete
                })
            )
            .unwrap()
            .acknowledge
    );
    intake
        .apply(&auth_scope, &mut auth, Action::Range(range(1, true)))
        .unwrap();
    assert!(auth.validated());
    assert!(intake.failed.is_empty());
}

#[test]
fn local_storage_error_is_scoped_but_malformed_drain_coverage_is_still_fatal() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    engine.put(b"users\0broken", b"old").unwrap();
    let bucket = bucket_of("auth", "users", b"k");
    let mut consumer = Consumer::new(target(engine.clone(), vec![bucket])).unwrap();
    engine.corrupt(b"users\0broken");
    let scope = consumer.scope();
    let mut intake = Intake::default();
    let effect = intake
        .apply(
            &scope,
            &mut consumer,
            Action::Bootstrap(bootstrap(9, vec![(b"k", b"v")])),
        )
        .unwrap();
    assert!(
        effect
            .error
            .unwrap()
            .metadata()
            .get("bedrock-local-error")
            .is_some()
    );
    assert!(!consumer.validated());
    assert!(intake.retries.contains(&scope));
    assert!(
        intake
            .apply(
                &scope,
                &mut consumer,
                Action::Delivery(delivery(1, 1, vec![event(1, b"k", Some(b"old-stream"))]))
            )
            .is_ok()
    );
    assert_eq!(
        consumer.applied, 0,
        "drained data must not advance a failed scope"
    );
    let mut malformed = delivery(2, 1, vec![]);
    malformed.dataset = "auth".into();
    assert!(
        intake
            .apply(&scope, &mut consumer, Action::Delivery(malformed))
            .is_err()
    );
}
