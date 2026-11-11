use super::*;
use crate::placement::{Owner, State};
use crate::storage::memory::MemoryEngine;

#[tokio::test]
async fn stop_joins_the_task_before_local_storage_can_be_replaced() {
    let (started, ready) = tokio::sync::oneshot::channel();
    let (held, mut released) = tokio::sync::oneshot::channel::<()>();
    let session = Session {
        progress: Arc::default(),
        changes: Arc::new(Watched::new(0)),
        rebuilds: Rebuilds::default(),
        task: tokio::spawn(async move {
            let _held = held;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        }),
        member: None,
    };
    ready.await.unwrap();
    session.stop().await;
    assert_eq!(
        released.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
}

#[tokio::test]
async fn drain_returns_once_the_draining_task_has_finished() {
    let session = Session {
        progress: Arc::default(),
        changes: Arc::new(Watched::new(0)),
        rebuilds: Rebuilds::default(),
        task: tokio::spawn(async {}),
        member: Some(Arc::new(Watched::new(pb::Member::default()))),
    };
    // Polling the finished task again would panic.
    session.drain().await;
}

#[tokio::test]
async fn storage_health_is_announced_and_quarantined_scopes_do_not_block_readiness() {
    let member = Arc::new(Watched::new(pb::Member {
        datasets: vec!["auth".into(), "quicksilver".into()],
        ..Default::default()
    }));
    let progress = Progress {
        connected: true,
        scopes: BTreeMap::from([
            (("auth".into(), "users".into(), None), (0, None, false)),
            (
                ("quicksilver".into(), "routes".into(), None),
                (5, Some(5), true),
            ),
        ]),
        error: None,
        ..Default::default()
    };
    let session = Session {
        progress: Arc::new(Mutex::new(progress)),
        changes: Arc::new(Watched::new(0)),
        rebuilds: Rebuilds::default(),
        task: tokio::spawn(async {}),
        member: Some(member.clone()),
    };
    let quarantined = BTreeSet::from(["auth".to_string(), "cdn".to_string()]);
    // A dataset the member never announced is dropped, or the relay would refuse the update.
    session.set_unhealthy(&quarantined);
    assert_eq!(member.get().unhealthy, ["auth"]);
    assert!(!session.ready());
    assert!(session.ready_except(&quarantined));
    session.set_unhealthy(&BTreeSet::new());
    assert!(member.get().unhealthy.is_empty());
}

fn target(engine: Arc<MemoryEngine>) -> Target {
    Target {
        selection: Selection {
            dataset: "auth".into(),
            namespace: "users".into(),
        },
        engine,
        prefix: b"users\0".to_vec(),
        span: None,
    }
}

/// The consumer of namespace `users` in a range of `auth`.
fn ranged(engine: Arc<dyn StorageEngine>, range: (u64, u64)) -> Consumer {
    let group = Target {
        selection: Selection {
            dataset: "auth".into(),
            namespace: String::new(),
        },
        engine,
        prefix: Vec::new(),
        span: Some(range),
    };
    Consumer::in_range(&group, "users", range).unwrap()
}

/// Exactly the hash of `key` in `users`.
fn only(key: &[u8]) -> (u64, u64) {
    let hash = hash_of("auth", "users", key);
    (hash, hash + 1)
}

fn stored_at(key: &[u8]) -> Vec<u8> {
    l2_key(hash_of("auth", "users", key), "users", key)
}

fn in_range<T>(mut frame: T, range: (u64, u64), set: impl FnOnce(&mut T, Option<pb::Span>)) -> T {
    set(&mut frame, span(Some(range)));
    frame
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
        span: None,
        ..Default::default()
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
        span: None,
        ..Default::default()
    }
}

#[test]
fn range_coverage_commits_empty_intervals_and_resumes_only_its_scope() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let range = only(b"k");
    let set = |d: &mut pb::Delivery, s| d.span = s;
    let mut consumer = ranged(engine.clone(), range);
    assert!(
        consumer
            .delivery(in_range(
                delivery(1, 5, vec![event(3, b"k", Some(b"v"))]),
                range,
                set
            ))
            .unwrap()
    );
    assert_eq!(consumer.applied, 5);
    assert_eq!(engine.get(&stored_at(b"k")).unwrap(), Some(b"v".to_vec()));
    assert!(
        consumer
            .delivery(in_range(delivery(6, 9, vec![]), range, set))
            .unwrap()
    );
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(9));
    let resumed = ranged(engine.clone(), range);
    assert_eq!(resumed.applied, 9);
    assert!(!resumed.recover);
    let widened = ranged(engine, (range.0, 0));
    assert_eq!(widened.applied, 0);
    assert!(widened.recover);
}

#[test]
fn a_range_consumer_marks_its_namespace_so_a_restart_resumes_it() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let range = only(b"k");
    let mut consumer = ranged(engine.clone(), range);
    consumer
        .delivery(in_range(delivery(1, 2, vec![]), range, |d, s| d.span = s))
        .unwrap();
    let group = Target {
        selection: Selection {
            dataset: "auth".into(),
            namespace: String::new(),
        },
        engine,
        prefix: Vec::new(),
        span: Some(range),
    };
    let resumed = Consumer::resumed(&group, range).unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(
        resumed[0].scope(),
        ("auth".into(), "users".into(), Some(range))
    );
    assert_eq!(resumed[0].applied, 2);
    assert_eq!(resumed[0].resume(), 3);
    assert!(Consumer::resumed(&group, (range.0, 0)).unwrap().is_empty());
}

#[test]
fn restart_ignores_inactive_and_other_dataset_markers_but_rejects_corrupt_active_markers() {
    let engine = MemoryEngine::new("ranges");
    let mut batch = WriteBatch::default();
    batch
        .set_revision(marker_prefix("auth", (10, 20)) + "users", 1)
        .set_revision(marker_prefix("auth", (10, 20)) + "admins", 1)
        .set_revision(marker_prefix("auth", (20, 0)) + "retired", 0)
        .set_revision("relay:range:cdn:malformed", 1)
        .set_revision("relay:range:auth:malformed", 0);
    engine.write(batch).unwrap();
    assert_eq!(held(&engine, "auth").unwrap(), BTreeSet::from([(10, 20)]));

    let mut batch = WriteBatch::default();
    batch.set_revision("relay:range:auth:malformed", 1);
    engine.write(batch).unwrap();
    assert_eq!(
        held(&engine, "auth").unwrap_err().code(),
        tonic::Code::DataLoss
    );
}

#[test]
fn gap_and_malformed_delivery_never_advance_or_partially_apply() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
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
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
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
fn range_delivery_rejects_keys_outside_it_even_in_an_old_interval() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let range = only(b"k");
    let set = |d: &mut pb::Delivery, s| d.span = s;
    let mut consumer = ranged(engine, range);
    consumer
        .delivery(in_range(delivery(1, 1, vec![]), range, set))
        .unwrap();
    assert!(
        consumer
            .delivery(in_range(
                delivery(1, 1, vec![event(1, b"other", Some(b"v"))]),
                range,
                set
            ))
            .is_err()
    );
    assert!(
        consumer.delivery(delivery(2, 2, vec![])).is_err(),
        "a whole-namespace frame is another stream"
    );
    assert_eq!(consumer.applied, 1);
}

#[test]
fn customer_kv_is_not_a_pushed_selection() {
    let mut kv = target(Arc::new(MemoryEngine::new("auth")));
    kv.selection.dataset = "kv".into();
    assert!(Consumer::new(kv).is_err());
}

#[test]
fn corrupt_or_unsorted_bootstrap_leaves_data_and_revision_untouched() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
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
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
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
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
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
fn range_change_bootstraps_before_reusing_progress_and_preserves_keys_outside_it() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let whole = (0, 0);
    let first = only(b"first");
    assert_ne!(first, only(b"second"));
    let mut original = ranged(engine.clone(), whole);
    original
        .delivery(in_range(
            delivery(
                1,
                2,
                vec![
                    event(1, b"first", Some(b"old")),
                    event(2, b"second", Some(b"keep")),
                ],
            ),
            whole,
            |d, s| d.span = s,
        ))
        .unwrap();
    let mut narrowed = ranged(engine.clone(), first);
    assert!(narrowed.recover);
    assert_eq!(narrowed.resume(), 0);
    assert!(matches!(
        narrowed.request().kind,
        Some(pb::relay_request::Kind::Recover(pb::Recover {
            follow: Some(pb::Follow {
                from: 0,
                span: Some(_),
                ..
            }),
            ..
        }))
    ));
    assert!(
        narrowed
            .delivery(in_range(delivery(1, 5, vec![]), first, |d, s| d.span = s))
            .is_err()
    );
    narrowed
        .bootstrap(in_range(
            bootstrap(9, vec![(b"first", b"new")]),
            first,
            |b, s| b.span = s,
        ))
        .unwrap();
    assert_eq!(
        engine.get(&stored_at(b"first")).unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        engine.get(&stored_at(b"second")).unwrap(),
        Some(b"keep".to_vec())
    );
}

#[test]
fn range_bootstrap_preserves_other_namespaces_in_the_same_span() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let whole = (0, 0);
    let users_old = stored_at(b"old");
    let admins_old = l2_key(hash_of("auth", "admins", b"old"), "admins", b"old");
    engine.put(&users_old, b"stale").unwrap();
    engine.put(&admins_old, b"keep").unwrap();
    let mut consumer = ranged(engine.clone(), whole);
    assert!(consumer.recover);
    consumer
        .bootstrap(in_range(
            bootstrap(9, vec![(b"new", b"value")]),
            whole,
            |b, s| b.span = s,
        ))
        .unwrap();
    assert_eq!(engine.get(&users_old).unwrap(), None);
    assert_eq!(
        engine.get(&stored_at(b"new")).unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(engine.get(&admins_old).unwrap(), Some(b"keep".to_vec()));
    assert_eq!(engine.revision(&consumer.revision).unwrap(), Some(9));
}

#[test]
fn splitting_unconfirmed_coverage_requires_the_kept_range_to_recover() {
    let data = Arc::new(MemoryEngine::new("auth"));
    let whole = (0, 0);
    let kept = only(b"k");
    let mut consumer = ranged(data.clone(), whole);
    consumer
        .bootstrap(in_range(
            bootstrap(9, vec![(b"k", b"value")]),
            whole,
            |b, s| b.span = s,
        ))
        .unwrap();
    assert!(!consumer.validated());

    let engine: Arc<dyn StorageEngine> = data.clone();
    let mut consumers = BTreeMap::from([(consumer.scope(), consumer)]);
    let mut pushed = Pushed {
        groups: Groups::from([(("auth".into(), whole), engine.clone())]),
        ranged: BTreeMap::from([("auth".into(), engine.clone())]),
        me: None,
        pending: BTreeMap::new(),
        retired: BTreeSet::new(),
        merges: BTreeMap::new(),
    };
    let progress = Mutex::new(Progress::default());
    let changes = Watched::new(0);
    let change = ranges::Change {
        held: whole,
        kept: vec![kept],
        dropped: vec![(0, kept.0), (kept.1, 0)],
    };
    let owners = vec![Owner {
        node: "other".into(),
        state: State::Ready,
    }];
    let directory = BTreeMap::from([(0, owners)]);
    let requests = narrow(
        "auth",
        &engine,
        ranges::confirm(Some(&directory), "me", &change, &BTreeSet::new()).unwrap(),
        &mut consumers,
        &mut pushed,
        &progress,
        &changes,
    )
    .unwrap();
    match requests.as_slice() {
        [
            pb::relay_request::Kind::Unfollow(_),
            pb::relay_request::Kind::Follow(follow),
        ] => {
            assert_eq!(
                follow.from_namespaces,
                BTreeMap::from([("users".into(), 0)])
            );
        }
        other => panic!("expected unfollow and recovery follow, got {other:?}"),
    }
    let replacement = &consumers[&("auth".into(), "users".into(), Some(kept))];
    assert!(!replacement.validated());
    assert_eq!(replacement.resume(), 0);
    assert_eq!(data.get(&stored_at(b"k")).unwrap(), Some(b"value".to_vec()));
    // Found by the simulation (#25 / B-23): the data stays, so a restart has to know the part is held or a revoke misses it.
    assert_eq!(held(data.as_ref(), "auth").unwrap(), BTreeSet::from([kept]));
    let resumed = Consumer::resumed(&replacement.target, kept).unwrap();
    assert!(!resumed[0].validated());
    assert_eq!(resumed[0].resume(), 0);
}

fn range(head: u64, recovered: bool) -> pb::ReplayRange {
    pb::ReplayRange {
        dataset: "auth".into(),
        namespace: "users".into(),
        first: 0,
        last: 0,
        head,
        recovered,
        span: None,
    }
}

#[tokio::test]
async fn installed_bootstrap_and_tail_wait_for_source_confirmation_before_reads_are_ready() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
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
        ..Default::default()
    }));
    let changes = Arc::new(Watched::new(0));
    let session = Session {
        progress: progress.clone(),
        changes: changes.clone(),
        rebuilds: Rebuilds::default(),
        task: tokio::spawn(std::future::pending()),
        member: None,
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
        None
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
    let reopened = Consumer::new(target(engine)).unwrap();
    assert!(reopened.validated());
}

#[test]
fn missing_confirmation_forces_fresh_recovery_after_disconnect_or_process_restart() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
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
    let mut reopened = Consumer::new(target(engine)).unwrap();
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
    let mut consumer = Consumer::new(target(engine)).unwrap();
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
    let mut auth = Consumer::new(target(Arc::new(MemoryEngine::new("auth")))).unwrap();
    let healthy_engine = Arc::new(MemoryEngine::new("quicksilver"));
    let mut healthy_target = target(healthy_engine.clone());
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
                span: None,
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
        ..Default::default()
    }));
    let session = Session {
        progress,
        changes: Arc::new(Watched::new(0)),
        rebuilds: Rebuilds::default(),
        task: tokio::spawn(std::future::pending()),
        member: None,
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
fn a_quarantined_scope_asks_for_no_recovery_until_its_rebuild() {
    let health = crate::health::storage::StorageHealth::new(&crate::admin::Admin::new());
    let engine = health.wrap_replication("auth", "engine", Arc::new(MemoryEngine::new("auth")));
    let mut consumer = Consumer::new(Target {
        engine,
        ..target(Arc::new(MemoryEngine::new("unused")))
    })
    .unwrap();
    let scope = consumer.scope();
    let mut intake = Intake::default();
    intake.fail(&scope, &mut consumer, Status::unavailable("busy"));
    let mut consumers = BTreeMap::from([(scope.clone(), consumer)]);
    assert_eq!(
        intake.due(&consumers),
        BTreeSet::from([scope.clone()]),
        "a healthy engine retries at once"
    );

    let damage = crate::storage::StorageError::Corruption(crate::storage::ErrorContext {
        engine: "memory",
        instance: "auth".into(),
        file: None,
        partition: None,
        detail: "damaged".into(),
    });
    health.report("auth", "engine", &damage);
    let failed = consumers.get_mut(&scope).unwrap();
    intake.fail(&scope, failed, Status::internal("quarantined"));
    assert!(intake.due(&consumers).is_empty());
    assert!(intake.due(&consumers).is_empty(), "it keeps waiting");

    // Starting the rebuild requests again by itself; a request that then fails retries as usual.
    assert!(health.rebuilding("auth", "engine"));
    assert_eq!(intake.due(&consumers), BTreeSet::from([scope]));
}

#[test]
fn local_storage_error_is_scoped_but_malformed_drain_coverage_is_still_fatal() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let whole = (0, 0);
    engine.put(&stored_at(b"broken"), b"old").unwrap();
    let mut consumer = ranged(engine.clone(), whole);
    engine.corrupt(&stored_at(b"broken"));
    let scope = consumer.scope();
    let mut intake = Intake::default();
    let effect = intake
        .apply(
            &scope,
            &mut consumer,
            Action::Bootstrap(in_range(bootstrap(9, vec![(b"k", b"v")]), whole, |b, s| {
                b.span = s
            })),
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
                Action::Delivery(in_range(
                    delivery(1, 1, vec![event(1, b"k", Some(b"old-stream"))]),
                    whole,
                    |d, s| d.span = s
                ))
            )
            .is_ok()
    );
    assert_eq!(
        consumer.applied, 0,
        "drained data must not advance a failed scope"
    );
    let malformed = in_range(delivery(2, 1, vec![]), whole, |d, s| d.span = s);
    assert!(
        intake
            .apply(&scope, &mut consumer, Action::Delivery(malformed))
            .is_err()
    );
}

/// Node `n1`'s membership view in cell `c1`, with `quicksilver` directory entries `(start, [(node, state)])`.
fn view(applied: u64, entries: &[(u64, &[(&str, pb::range::owner::State)])]) -> Consumer {
    let engine = Arc::new(MemoryEngine::new("membership"));
    for (start, owners) in entries {
        let range = pb::Range {
            owners: owners
                .iter()
                .map(|(node, state)| pb::range::Owner {
                    node_id: (*node).into(),
                    state: *state as i32,
                })
                .collect(),
        };
        let key = format!("quicksilver.c1\0range/quicksilver/{start:016x}");
        engine.put(key.as_bytes(), &range.encode_to_vec()).unwrap();
    }
    let mut consumer = Consumer::new(Target {
        selection: Selection {
            dataset: "membership".into(),
            namespace: "quicksilver.c1".into(),
        },
        engine,
        prefix: b"quicksilver.c1\0".to_vec(),
        span: None,
    })
    .unwrap();
    // A validated view at `applied`, as if its subscription had installed these entries.
    consumer.recover = false;
    consumer.applied = applied;
    consumer
}

fn pushed_ranges(revision: u64, spans: &[(u64, u64)]) -> pb::Ranges {
    pb::Ranges {
        dataset: "quicksilver".into(),
        revision,
        spans: spans
            .iter()
            .map(|&(start, end)| pb::Span { start, end })
            .collect(),
    }
}

#[tokio::test]
async fn unreadable_held_ranges_refuse_the_set_without_stopping_other_datasets() {
    use crate::storage::sim::{Access, Fault, SimEngine};
    use pb::relay_request::Kind;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let whole = (0, 0);
    let marker = marker_prefix("auth", whole) + "users";
    // Quarantine and both narrowing reads are isolated; malformed persisted state remains fatal.
    for (fail_at, marker, expected_error, spans, range) in [
        (None, marker.clone(), None, Vec::new(), whole),
        (Some(1), marker.clone(), None, Vec::new(), whole),
        (Some(2), marker.clone(), None, Vec::new(), whole),
        (
            Some(1),
            marker,
            None,
            vec![(0, 1 << 63), (1 << 63, 0)],
            whole,
        ),
        // A refused cut also blocks a merged range's bootstrap over the unreadable part.
        (
            Some(0),
            marker_prefix("auth", (0, 1 << 62)) + "users",
            None,
            vec![(1, 0)],
            (0, 1 << 62),
        ),
        (
            Some(1),
            marker_prefix("auth", (0, 1 << 62)) + "users",
            None,
            vec![(1, 0)],
            (0, 1 << 62),
        ),
        (
            Some(usize::MAX),
            "relay:range:auth:broken".into(),
            Some(tonic::Code::DataLoss),
            Vec::new(),
            whole,
        ),
        (
            Some(usize::MAX),
            marker_prefix("auth", whole) + "bad\0name",
            Some(tonic::Code::InvalidArgument),
            Vec::new(),
            whole,
        ),
    ] {
        let reads = Arc::new(AtomicUsize::new(usize::MAX / 2));
        let observed = reads.clone();
        let data = Arc::new(SimEngine::new(
            "auth",
            Arc::new(move |access| {
                (access == Access::Read && Some(observed.fetch_add(1, Ordering::SeqCst)) == fail_at)
                    .then_some(Fault::Unavailable)
            }),
            None,
            false,
        ));
        data.put(&stored_at(b"k"), b"value").unwrap();
        let mut batch = WriteBatch::default();
        batch.set_revision(marker, 1);
        data.write(batch).unwrap();
        let health = crate::health::storage::StorageHealth::new(&crate::admin::Admin::new());
        let engine = health.wrap_replication("auth", "engine", data.clone());
        let me: Scope = ("membership".into(), "quicksilver.c1".into(), None);
        let membership = view(7, &[]);
        let moved = pb::Range {
            owners: vec![pb::range::Owner {
                node_id: "other".into(),
                state: pb::range::owner::State::Ready as i32,
            }],
        };
        membership
            .target
            .engine
            .put(
                b"quicksilver.c1\0range/auth/0000000000000000",
                &moved.encode_to_vec(),
            )
            .unwrap();
        let mut consumers = BTreeMap::from([(me.clone(), membership)]);
        let mut pushed = revoke_state(&engine, range);
        if range != whole {
            let other = (range.1, 1 << 63);
            pushed.groups.insert(("auth".into(), other), engine.clone());
            pushed.merges.insert(
                "auth".into(),
                vec![ranges::Merge {
                    into: (range.0, other.1),
                    parts: vec![range, other],
                }],
            );
        }
        pushed.me = Some(("n1".into(), me));
        pushed.ranged.insert(
            "quicksilver".into(),
            Arc::new(MemoryEngine::new("quicksilver")),
        );
        pushed.pending.insert(
            "auth".into(),
            pb::Ranges {
                dataset: "auth".into(),
                ..pushed_ranges(7, &spans)
            },
        );
        pushed
            .pending
            .insert("quicksilver".into(), pushed_ranges(7, &[whole]));
        if fail_at.is_none() {
            health.report(
                "auth",
                "engine",
                &crate::storage::StorageError::Corruption(crate::storage::ErrorContext {
                    engine: "memory",
                    instance: "auth".into(),
                    file: None,
                    partition: None,
                    detail: "damaged".into(),
                }),
            );
        }
        reads.store(0, Ordering::SeqCst);
        let (tx, mut rx) = mpsc::channel(8);
        let result = settle(
            &tx,
            &mut consumers,
            &mut pushed,
            &Mutex::new(Progress::default()),
            &Watched::new(0),
        )
        .await;
        if let Some(code) = expected_error {
            assert_eq!(result.unwrap_err().code(), code);
            assert!(rx.try_recv().is_err());
            continue;
        }
        result.unwrap();

        match rx.try_recv().unwrap().kind.unwrap() {
            Kind::Ranges(ack) => {
                assert_eq!(ack.dataset, "auth");
                let dropped = if range == whole || fail_at == Some(0) {
                    whole
                } else {
                    (0, 1)
                };
                assert_eq!(ack.refused, [span(Some(dropped)).unwrap()]);
                assert!(!ack.reason.is_empty());
            }
            other => panic!("expected a refusing acknowledgement, got {other:?}"),
        }
        assert!(matches!(
            rx.try_recv().unwrap().kind,
            Some(Kind::Follow(follow)) if follow.dataset == "quicksilver"
        ));
        assert!(matches!(
            rx.try_recv().unwrap().kind,
            Some(Kind::Ranges(ack)) if ack.dataset == "quicksilver" && ack.refused.is_empty()
        ));
        assert!(rx.try_recv().is_err());
        assert!(pushed.groups.contains_key(&("auth".into(), range)));
        assert!(!pushed.retired.contains(&("auth".into(), range)));
        assert!(pushed.merges.get("auth").is_none_or(Vec::is_empty));
        assert_eq!(data.get(&stored_at(b"k")).unwrap(), Some(b"value".to_vec()));
        assert_eq!(
            held(data.as_ref(), "auth").unwrap(),
            BTreeSet::from([range])
        );
    }
}

#[tokio::test]
async fn pushed_revokes_wait_for_the_view_are_refused_unless_it_confirms_then_drop_and_keep_coverage()
 {
    use pb::range::owner::State::{Leaving, Ready};
    use pb::relay_request::Kind;
    const HALF: u64 = 1 << 63;
    let whole = (0, 0);
    let data = Arc::new(MemoryEngine::new("quicksilver"));
    let group = Target {
        selection: Selection {
            dataset: "quicksilver".into(),
            namespace: String::new(),
        },
        engine: data.clone(),
        prefix: Vec::new(),
        span: Some(whole),
    };
    let mut site = Consumer::in_range(&group, "site", whole).unwrap();
    let key = |high: bool| {
        (0u8..)
            .map(|i| vec![b'k', i])
            .find(|k| (hash_of("quicksilver", "site", k) >= HALF) == high)
            .unwrap()
    };
    let (low, high) = (key(false), key(true));
    let mut low_event = event(7, &low, Some(b"low"));
    let mut high_event = event(6, &high, Some(b"high"));
    for e in [&mut high_event, &mut low_event] {
        e.dataset = "quicksilver".into();
        e.namespace = "site".into();
    }
    let mut batch = in_range(
        delivery(1, 7, vec![high_event, low_event]),
        whole,
        |d, s| d.span = s,
    );
    batch.dataset = "quicksilver".into();
    batch.namespace = "site".into();
    site.delivery(batch).unwrap();
    let stored = |key: &[u8]| {
        data.get(&l2_key(hash_of("quicksilver", "site", key), "site", key))
            .unwrap()
    };

    let me: Scope = ("membership".into(), "quicksilver.c1".into(), None);
    let mut consumers = BTreeMap::from([(site.scope(), site)]);
    let engine: Arc<dyn StorageEngine> = data.clone();
    let mut pushed = Pushed {
        groups: Groups::from([(("quicksilver".into(), whole), engine.clone())]),
        ranged: BTreeMap::from([("quicksilver".into(), engine)]),
        me: Some(("n1".into(), me.clone())),
        pending: BTreeMap::new(),
        retired: BTreeSet::new(),
        merges: BTreeMap::new(),
    };
    let progress = Mutex::new(Progress::default());
    let changes = Watched::new(0);
    let (tx, mut rx) = mpsc::channel::<pb::RelayRequest>(16);
    let mut sent = || {
        let mut out = Vec::new();
        while let Ok(request) = rx.try_recv() {
            out.push(request.kind.unwrap());
        }
        out
    };

    // The view has not reached the pushed revision: nothing happens yet.
    consumers.insert(me.clone(), view(5, &[(0, &[("n1", Ready)])]));
    pushed
        .pending
        .insert("quicksilver".into(), pushed_ranges(6, &[(0, HALF)]));
    settle(&tx, &mut consumers, &mut pushed, &progress, &changes)
        .await
        .unwrap();
    assert!(sent().is_empty());
    assert_eq!(pushed.pending.len(), 1);

    // The view lists the node as leaving the dropped half: refused, reported, data kept.
    let leaving: &[(&str, pb::range::owner::State)] = &[("n1", Leaving), ("n2", Ready)];
    consumers.insert(
        me.clone(),
        view(6, &[(0, &[("n1", Ready)]), (HALF, leaving)]),
    );
    settle(&tx, &mut consumers, &mut pushed, &progress, &changes)
        .await
        .unwrap();
    match sent().as_slice() {
        [Kind::Ranges(ack)] => {
            assert_eq!(ack.revision, 6);
            assert_eq!(
                ack.refused,
                [pb::Span {
                    start: HALF,
                    end: 0
                }]
            );
            assert!(ack.reason.contains("still an owner"), "{}", ack.reason);
        }
        other => panic!("expected a refusing acknowledgement, got {other:?}"),
    }
    assert_eq!(stored(&high), Some(b"high".to_vec()));
    assert!(pushed.groups.contains_key(&("quicksilver".into(), whole)));

    // Once the view confirms it, the dropped half is deleted and the kept half keeps its coverage.
    let moved: &[(&str, pb::range::owner::State)] = &[("n2", Ready)];
    consumers.insert(me.clone(), view(7, &[(0, &[("n1", Ready)]), (HALF, moved)]));
    pushed
        .pending
        .insert("quicksilver".into(), pushed_ranges(7, &[(0, HALF)]));
    settle(&tx, &mut consumers, &mut pushed, &progress, &changes)
        .await
        .unwrap();
    match sent().as_slice() {
        [
            Kind::Unfollow(unfollow),
            Kind::Follow(follow),
            Kind::Ranges(ack),
        ] => {
            assert_eq!(unfollow.span, Some(pb::Span { start: 0, end: 0 }));
            assert_eq!(
                follow.span,
                Some(pb::Span {
                    start: 0,
                    end: HALF
                })
            );
            assert_eq!(follow.from_namespaces, BTreeMap::from([("site".into(), 8)]));
            assert_eq!((ack.revision, ack.refused.len()), (7, 0));
        }
        other => panic!("expected the narrowing requests, got {other:?}"),
    }
    assert_eq!(stored(&high), None);
    assert_eq!(stored(&low), Some(b"low".to_vec()));
    // Frames of the unfollowed range still in flight are dropped, not fatal.
    assert!(pushed.retired.contains(&("quicksilver".into(), whole)));
    let kept = &consumers[&("quicksilver".into(), "site".into(), Some((0, HALF)))];
    assert_eq!((kept.applied, kept.validated()), (7, true));
    assert_eq!(
        held(data.as_ref(), "quicksilver").unwrap(),
        BTreeSet::from([(0, HALF)])
    );

    // A new range is followed from scratch; a dataset this process does not carry is only acknowledged.
    consumers.insert(me.clone(), view(8, &[(0, &[("n1", Ready)])]));
    pushed.pending.insert(
        "quicksilver".into(),
        pushed_ranges(8, &[(0, HALF), (HALF, 0)]),
    );
    pushed.pending.insert(
        "auth".into(),
        pb::Ranges {
            dataset: "auth".into(),
            ..pushed_ranges(8, &[(0, 0)])
        },
    );
    settle(&tx, &mut consumers, &mut pushed, &progress, &changes)
        .await
        .unwrap();
    match sent().as_slice() {
        [Kind::Ranges(auth), Kind::Follow(follow), Kind::Ranges(qs)] => {
            assert_eq!((auth.dataset.as_str(), auth.refused.len()), ("auth", 0));
            assert_eq!(
                follow.span,
                Some(pb::Span {
                    start: HALF,
                    end: 0
                })
            );
            assert!(follow.from_namespaces.is_empty());
            assert_eq!(qs.dataset, "quicksilver");
        }
        other => panic!("expected a follow and two acknowledgements, got {other:?}"),
    }
}

#[tokio::test]
async fn rebuild_counts_only_after_a_fresh_verified_bootstrap() {
    let engine = Arc::new(MemoryEngine::new("auth"));
    let mut consumer = Consumer::new(target(engine.clone())).unwrap();
    consumer
        .bootstrap(bootstrap(5, vec![(b"k", b"v")]))
        .unwrap();
    consumer.range(&range(5, true)).unwrap();
    assert!(consumer.validated());
    let scope = consumer.scope();
    let progress = Arc::new(Mutex::new(Progress {
        connected: true,
        scopes: BTreeMap::from([(scope.clone(), (5, Some(5), true))]),
        ..Default::default()
    }));
    let changes = Arc::new(Watched::new(0));
    let session = Session {
        progress: progress.clone(),
        changes: changes.clone(),
        rebuilds: Rebuilds::default(),
        task: tokio::spawn(std::future::pending()),
        member: None,
    };
    let rebuilder = session.rebuilder("auth");
    let rebuilding = tokio::spawn(async move { rebuilder.rebuild().await });
    let requested = session
        .rebuilds
        .wait_until(|requested| requested.get("auth") == Some(&1))
        .await;
    assert!(consumer.start_rebuild(&requested));
    assert!(
        !consumer.start_rebuild(&requested),
        "one request starts one recovery"
    );
    assert!(matches!(
        consumer.request().kind,
        Some(pb::relay_request::Kind::Recover(pb::Recover {
            follow: Some(pb::Follow { from: 0, .. }),
            ..
        }))
    ));
    tokio::task::yield_now().await;
    assert!(!rebuilding.is_finished(), "the old coverage does not count");

    // The instance was recreated empty; the old subscription's tail cannot rebuild it.
    engine
        .restore(&*MemoryEngine::new("empty").snapshot().unwrap())
        .unwrap();
    assert!(
        consumer
            .delivery(delivery(6, 6, vec![event(6, b"k", Some(b"tail"))]))
            .is_err()
    );
    consumer
        .bootstrap(bootstrap(6, vec![(b"k", b"fresh")]))
        .unwrap();
    assert!(
        !consumer.validated(),
        "a bootstrap still needs confirmation"
    );
    consumer.range(&range(6, true)).unwrap();
    assert!(consumer.validated());
    changed(&progress, &changes, |state| {
        state.scopes.insert(scope.clone(), (6, Some(6), true));
        state.rebuilt.insert(scope, consumer.rebuild);
    });
    tokio::time::timeout(Duration::from_secs(1), rebuilding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(engine.get(b"users\0k").unwrap(), Some(b"fresh".to_vec()));
}

#[tokio::test]
async fn a_member_announces_only_the_ranged_datasets_it_follows() {
    let peers = Arc::new(Peers::new(&crate::admin::Admin::new()));
    let member = pb::Member {
        node_id: "q1".into(),
        service: "quicksilver".into(),
        cell: "c1".into(),
        status: pb::member::Status::Ready as i32,
        datasets: vec!["auth".into(), "quicksilver".into()],
        ..Default::default()
    };
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new("quicksilver"));
    let ranged = BTreeMap::from([("quicksilver".to_string(), engine)]);
    let relays = vec!["10.0.0.1:7000".to_string()];
    // Without its own membership view the member could never confirm a revoke.
    let refused = Session::join(
        peers.clone(),
        relays.clone(),
        Vec::new(),
        member.clone(),
        ranged.clone(),
    )
    .err()
    .unwrap();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    // `auth` did not open, so placement must not give this member its ranges.
    let (_, view) = crate::membership::View::new("quicksilver", "c1");
    let session = Session::join(peers, relays, vec![view], member, ranged).unwrap();
    assert_eq!(
        session.member.as_ref().unwrap().get().datasets,
        ["quicksilver"]
    );
}

/// Found by the simulation (#25 / B-23): the session refused to start on a damaged record, so the process exited on every
/// restart before the rebuild that repairs it could run.
#[tokio::test]
async fn a_damaged_engine_does_not_stop_the_session_starting() {
    let engine = Arc::new(crate::storage::sim::SimEngine::new(
        "quicksilver",
        Arc::new(|_| None),
        None,
        false,
    ));
    let key = l2_key(1, "infrastructure", b"k");
    let mut batch = WriteBatch::default();
    batch
        .put(key.clone(), "v")
        .set_revision(marker_prefix("quicksilver", (0, 0)) + "infrastructure", 1);
    engine.write(batch).unwrap();
    engine.damage(&key);
    let member = pb::Member {
        node_id: "q1".into(),
        service: "quicksilver".into(),
        cell: "c1".into(),
        status: pb::member::Status::Ready as i32,
        datasets: vec!["quicksilver".into()],
        ..Default::default()
    };
    let engine: Arc<dyn StorageEngine> = engine;
    let ranged = BTreeMap::from([("quicksilver".to_string(), engine)]);
    let (_, view) = crate::membership::View::new("quicksilver", "c1");
    let peers = Arc::new(Peers::new(&crate::admin::Admin::new()));
    let relays = vec!["10.0.0.1:7000".to_string()];
    Session::join(peers, relays, vec![view], member, ranged).unwrap();
}

/// Found by the crash tests (#25 / B-23 Layer 3): damage in a later range quarantined the engine, and the earlier range's
/// second read then failed and ended the session, so the process exited once before running quarantined.
#[tokio::test]
async fn damage_in_a_later_range_does_not_stop_the_session_starting() {
    let sim = Arc::new(crate::storage::sim::SimEngine::new(
        "quicksilver",
        Arc::new(|_| None),
        None,
        false,
    ));
    let middle = u64::MAX / 2;
    let late = l2_key(middle + 1, "infrastructure", b"k");
    let mut batch = WriteBatch::default();
    batch
        .put(l2_key(1, "infrastructure", b"k"), "v")
        .put(late.clone(), "v")
        .set_revision(
            marker_prefix("quicksilver", (0, middle)) + "infrastructure",
            1,
        )
        .set_revision(
            marker_prefix("quicksilver", (middle, 0)) + "infrastructure",
            1,
        );
    sim.write(batch).unwrap();
    sim.damage(&late);
    let health = crate::health::storage::StorageHealth::new(&crate::admin::Admin::new());
    let engine = health.wrap_replication("quicksilver", "engine", sim);
    let member = pb::Member {
        node_id: "q1".into(),
        service: "quicksilver".into(),
        cell: "c1".into(),
        status: pb::member::Status::Ready as i32,
        datasets: vec!["quicksilver".into()],
        ..Default::default()
    };
    let ranged = BTreeMap::from([("quicksilver".to_string(), engine)]);
    let (_, view) = crate::membership::View::new("quicksilver", "c1");
    let peers = Arc::new(Peers::new(&crate::admin::Admin::new()));
    let relays = vec!["10.0.0.1:7000".to_string()];
    Session::join(peers, relays, vec![view], member, ranged).unwrap();
    assert!(health.unhealthy().contains("quicksilver"));
}

/// `narrow`'s other arguments, for a member holding `whole` of `auth`, with `engine` as its storage.
fn revoke_state(engine: &Arc<dyn StorageEngine>, whole: Range) -> Pushed {
    Pushed {
        groups: Groups::from([(("auth".into(), whole), engine.clone())]),
        ranged: BTreeMap::from([("auth".into(), engine.clone())]),
        me: None,
        pending: BTreeMap::new(),
        retired: BTreeSet::new(),
        merges: BTreeMap::new(),
    }
}

/// A directory in which another node holds everything, so a revoke is allowed.
fn replaced() -> crate::placement::Directory {
    let owner = Owner {
        node: "other".into(),
        state: State::Ready,
    };
    BTreeMap::from([(0, vec![owner])])
}

/// Found by the simulation (#25 / B-23): a revoke's write can commit and still report failure. The consumer must not keep
/// trusting a cursor whose data may be gone, or it applies deliveries over the hole and claims full coverage.
#[test]
fn a_revoke_with_an_unknown_outcome_stops_trusting_the_consumer() {
    use crate::storage::sim::{Access, Fault, SimEngine};
    use std::sync::atomic::{AtomicBool, Ordering};

    let failing = Arc::new(AtomicBool::new(false));
    let write_fails = failing.clone();
    let data = Arc::new(SimEngine::new(
        "auth",
        Arc::new(move |access| {
            (access == Access::Write && write_fails.load(Ordering::SeqCst))
                .then_some(Fault::Ambiguous)
        }),
        None,
        false,
    ));
    let whole = (0, 0);
    let mut consumer = ranged(data.clone(), whole);
    consumer
        .bootstrap(in_range(
            bootstrap(9, vec![(b"k", b"value")]),
            whole,
            |b, s| b.span = s,
        ))
        .unwrap();
    consumer.awaiting_confirmation = false;
    assert!(consumer.validated());
    let scope = consumer.scope();
    let mut consumers = BTreeMap::from([(scope.clone(), consumer)]);
    let engine: Arc<dyn StorageEngine> = data.clone();
    let mut pushed = revoke_state(&engine, whole);
    let change = ranges::Change {
        held: whole,
        kept: Vec::new(),
        dropped: vec![whole],
    };
    failing.store(true, Ordering::SeqCst);
    let revoked = narrow(
        "auth",
        &engine,
        ranges::confirm(Some(&replaced()), "me", &change, &BTreeSet::new()).unwrap(),
        &mut consumers,
        &mut pushed,
        &Mutex::new(Progress::default()),
        &Watched::new(0),
    );
    assert!(revoked.is_err());
    assert!(!consumers[&scope].validated());
}

/// A range can be revoked before its consumer exists, when its progress could not be read at startup. Deleting the data
/// must retire that stored progress too, or a later push resumes the cursor over nothing.
#[test]
fn a_revoke_retires_stored_progress_no_consumer_holds() {
    let data = Arc::new(MemoryEngine::new("auth"));
    let whole = (0, 0);
    let mut consumer = ranged(data.clone(), whole);
    consumer
        .bootstrap(in_range(
            bootstrap(9, vec![(b"k", b"value")]),
            whole,
            |b, s| b.span = s,
        ))
        .unwrap();
    let active = consumer.active.clone();
    assert_eq!(data.revision(&active).unwrap(), Some(1));
    let engine: Arc<dyn StorageEngine> = data.clone();
    let mut pushed = revoke_state(&engine, whole);
    let change = ranges::Change {
        held: whole,
        kept: Vec::new(),
        dropped: vec![whole],
    };
    narrow(
        "auth",
        &engine,
        ranges::confirm(Some(&replaced()), "me", &change, &BTreeSet::new()).unwrap(),
        &mut BTreeMap::new(),
        &mut pushed,
        &Mutex::new(Progress::default()),
        &Watched::new(0),
    )
    .unwrap();
    assert_eq!(data.revision(&active).unwrap(), None);
    assert!(held(data.as_ref(), "auth").unwrap().is_empty());
    // B-61: retiring deletes the names, so a node's revisions do not grow with its range history.
    let left: Vec<_> = data
        .snapshot()
        .unwrap()
        .revisions()
        .unwrap()
        .into_keys()
        .collect();
    assert!(
        left.iter().all(|name| !name.starts_with("relay:")),
        "{left:?}"
    );
}

/// Found by the simulation (#25 / B-23): a split whose write committed but reported failure left progress on the parts. A
/// later revoke of one part deleted its data and retired only the whole range, so a later push resumed the part's cursor
/// over nothing.
#[test]
fn a_revoke_retires_progress_stored_for_part_of_the_range() {
    let data = Arc::new(MemoryEngine::new("auth"));
    let (whole, part) = ((0, 0), only(b"k"));
    let mut consumer = ranged(data.clone(), part);
    consumer
        .bootstrap(in_range(
            bootstrap(9, vec![(b"k", b"value")]),
            part,
            |b, s| b.span = s,
        ))
        .unwrap();
    let active = consumer.active.clone();
    let engine: Arc<dyn StorageEngine> = data.clone();
    let mut pushed = revoke_state(&engine, whole);
    let change = ranges::Change {
        held: whole,
        kept: Vec::new(),
        dropped: vec![whole],
    };
    narrow(
        "auth",
        &engine,
        ranges::confirm(Some(&replaced()), "me", &change, &BTreeSet::new()).unwrap(),
        &mut BTreeMap::new(),
        &mut pushed,
        &Mutex::new(Progress::default()),
        &Watched::new(0),
    )
    .unwrap();
    assert_eq!(data.revision(&active).unwrap(), None);
    assert!(held(data.as_ref(), "auth").unwrap().is_empty());
}

/// Found by the simulation (#25 / B-23): a read failure while creating a split part's consumer left a group with none. A
/// further split then retired the stored marker and left the parts it kept unmarked, so a restart or revoke missed them.
#[test]
fn a_split_without_a_consumer_still_marks_what_it_keeps() {
    let data = Arc::new(MemoryEngine::new("auth"));
    let (whole, kept) = ((0, 0), only(b"k"));
    let mut consumer = ranged(data.clone(), whole);
    consumer
        .bootstrap(in_range(
            bootstrap(9, vec![(b"k", b"value")]),
            whole,
            |b, s| b.span = s,
        ))
        .unwrap();
    let engine: Arc<dyn StorageEngine> = data.clone();
    let mut pushed = revoke_state(&engine, whole);
    let change = ranges::Change {
        held: whole,
        kept: vec![kept],
        dropped: vec![(0, kept.0), (kept.1, 0)],
    };
    narrow(
        "auth",
        &engine,
        ranges::confirm(Some(&replaced()), "me", &change, &BTreeSet::new()).unwrap(),
        &mut BTreeMap::new(),
        &mut pushed,
        &Mutex::new(Progress::default()),
        &Watched::new(0),
    )
    .unwrap();
    assert_eq!(held(data.as_ref(), "auth").unwrap(), BTreeSet::from([kept]));
}

/// Found by the simulation (#25 / B-23): a split that committed but reported failure left a trusted cursor on a part. The
/// retry dropped nothing, so that cursor stayed active over data newer bootstraps replaced.
#[test]
fn a_retried_split_retires_progress_an_earlier_attempt_left_on_a_part() {
    let data = Arc::new(MemoryEngine::new("auth"));
    let whole = (0, 0);
    let (first, part) = ((0, 1 << 63), (1 << 63, 0));
    let parts = [(1 << 63, 3 << 62), (3 << 62, 0)];
    let mut consumer = ranged(data.clone(), whole);
    consumer
        .bootstrap(in_range(
            bootstrap(9, vec![(b"k", b"value")]),
            whole,
            |b, s| b.span = s,
        ))
        .unwrap();
    consumer
        .range(&in_range(range(9, true), whole, |r, s| r.span = s))
        .unwrap();
    assert!(consumer.validated());
    let mut left = WriteBatch::default();
    consumer.carry(&mut left, part);
    data.write(left).unwrap();
    consumer.distrust();

    let engine: Arc<dyn StorageEngine> = data.clone();
    let mut consumers = BTreeMap::from([(consumer.scope(), consumer)]);
    let mut pushed = revoke_state(&engine, whole);
    let change = ranges::Change {
        held: whole,
        kept: vec![first, parts[0], parts[1]],
        dropped: Vec::new(),
    };
    narrow(
        "auth",
        &engine,
        ranges::confirm(Some(&replaced()), "me", &change, &BTreeSet::new()).unwrap(),
        &mut consumers,
        &mut pushed,
        &Mutex::new(Progress::default()),
        &Watched::new(0),
    )
    .unwrap();
    assert_eq!(
        held(data.as_ref(), "auth").unwrap(),
        BTreeSet::from([first, parts[0], parts[1]])
    );
}

/// Found by the simulation (#25 / B-23): after a split that committed but reported failure, the whole range's consumer
/// installed newer data while the cursors left on its parts still claimed the old revision.
#[test]
fn a_commit_retires_cursors_an_unknown_split_left_on_parts_of_its_range() {
    let data = Arc::new(MemoryEngine::new("auth"));
    let (whole, part) = ((0, 0), (1 << 63, 0));
    let mut consumer = ranged(data.clone(), whole);
    let baseline = |boundary| {
        in_range(
            bootstrap(boundary, vec![(b"k", b"value")]),
            whole,
            |b, s| b.span = s,
        )
    };
    consumer.bootstrap(baseline(9)).unwrap();
    consumer
        .range(&in_range(range(9, true), whole, |r, s| r.span = s))
        .unwrap();
    let mut left = WriteBatch::default();
    consumer.carry(&mut left, part);
    data.write(left).unwrap();
    consumer.distrust();
    assert!(held(data.as_ref(), "auth").unwrap().contains(&part));

    consumer.bootstrap(baseline(12)).unwrap();
    assert_eq!(
        held(data.as_ref(), "auth").unwrap(),
        BTreeSet::from([whole])
    );
}

/// #66 / B-61: a member joins the consumers of merged ranges only once they are at one revision, and then in one batch
/// that leaves the merged range holding that coverage (`docs/spec/merge.tla`).
#[test]
fn merged_ranges_join_only_at_one_revision() {
    const HALF: u64 = 1 << 63;
    let data = Arc::new(MemoryEngine::new("auth"));
    let engine: Arc<dyn StorageEngine> = data.clone();
    let group = Target {
        selection: Selection {
            dataset: "auth".into(),
            namespace: String::new(),
        },
        engine: engine.clone(),
        prefix: Vec::new(),
        span: None,
    };
    let (low, high, whole) = ((0, HALF), (HALF, 0), (0, 0));
    let at = |range: Range, applied: u64| {
        let names = Names::of("auth", "keys", &[], Some(range));
        let mut batch = WriteBatch::default();
        batch
            .set_revision(names.marker.as_ref().unwrap(), 1)
            .set_revision(&names.active, 1)
            .set_revision(&names.revision, applied);
        data.write(batch).unwrap();
        Consumer::in_range(&group, "keys", range).unwrap()
    };
    let mut consumers = BTreeMap::new();
    for consumer in [at(low, 5), at(high, 4)] {
        consumers.insert(consumer.scope(), consumer);
    }
    let merge = ranges::Merge {
        into: whole,
        parts: vec![low, high],
    };
    let mut pushed = Pushed {
        groups: Groups::from([
            (("auth".into(), low), engine.clone()),
            (("auth".into(), high), engine.clone()),
        ]),
        ranged: BTreeMap::from([("auth".into(), engine.clone())]),
        me: None,
        pending: BTreeMap::new(),
        retired: BTreeSet::new(),
        merges: BTreeMap::from([("auth".into(), vec![merge])]),
    };
    let progress = Mutex::new(Progress::default());
    let changes = Watched::new(0);

    // One part behind: joining at either revision would roll a part back or skip its events.
    assert!(
        join(&mut consumers, &mut pushed, &progress, &changes)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        held(data.as_ref(), "auth").unwrap(),
        BTreeSet::from([low, high])
    );

    let caught_up = at(high, 5);
    consumers.insert(caught_up.scope(), caught_up);
    match join(&mut consumers, &mut pushed, &progress, &changes)
        .unwrap()
        .as_slice()
    {
        [
            pb::relay_request::Kind::Unfollow(a),
            pb::relay_request::Kind::Unfollow(b),
            pb::relay_request::Kind::Follow(follow),
        ] => {
            assert_eq!((a.span, b.span), (span(Some(low)), span(Some(high))));
            assert_eq!(follow.span, span(Some(whole)));
            assert_eq!(follow.from_namespaces, BTreeMap::from([("keys".into(), 6)]));
        }
        other => panic!("expected the joining requests, got {other:?}"),
    }
    assert_eq!(
        held(data.as_ref(), "auth").unwrap(),
        BTreeSet::from([whole])
    );
    let joined = &consumers[&("auth".into(), "keys".into(), Some(whole))];
    assert_eq!((joined.applied, joined.validated()), (5, true));
    assert_eq!(consumers.len(), 1);
    assert!(pushed.retired.contains(&("auth".into(), low)) && pushed.merges.is_empty());
    assert_eq!(
        pushed.groups.keys().collect::<Vec<_>>(),
        [&("auth".into(), whole)]
    );
}

/// Found by #56 / B-25's simulation: a range stream opens a namespace's consumer on its first frame, so a namespace
/// nothing was written to never had stored progress, and its owners answered every read, even at revision 0, as behind.
#[test]
fn a_range_holds_a_never_written_namespace_at_revision_zero() {
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new("quicksilver"));
    let range = (0, 1 << 63);
    let opened = Consumer::missing("quicksilver", range, &engine, |_| false).unwrap();
    let scopes: Vec<_> = opened.iter().map(Consumer::scope).collect();
    assert_eq!(
        scopes,
        [("quicksilver".into(), "infrastructure".into(), Some(range))]
    );
    assert!(opened[0].validated());
    let snapshot = engine.snapshot().unwrap();
    assert_eq!(
        applied_ranges(&*snapshot, "quicksilver").unwrap(),
        [("infrastructure".to_owned(), range, 0)]
    );
    // A restart resumes it from the first revision; a namespace already open is not opened twice.
    let group = Target {
        selection: Selection {
            dataset: "quicksilver".into(),
            namespace: String::new(),
        },
        engine: engine.clone(),
        prefix: Vec::new(),
        span: Some(range),
    };
    let resumed = Consumer::resumed(&group, range).unwrap();
    assert_eq!(
        (resumed.len(), resumed[0].applied, resumed[0].resume()),
        (1, 0, 1)
    );
    let held: BTreeSet<Scope> = resumed.iter().map(Consumer::scope).collect();
    assert!(
        Consumer::missing("quicksilver", range, &engine, |s| held.contains(s))
            .unwrap()
            .is_empty()
    );
}

/// Data in a range without stored progress may be anything, so it is never claimed as revision 0.
#[test]
fn unexplained_range_data_is_never_held_at_revision_zero() {
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new("auth"));
    let range = only(b"k");
    let mut batch = WriteBatch::default();
    batch.put(stored_at(b"k"), b"v".to_vec());
    engine.write(batch).unwrap();
    let consumer = ranged(engine.clone(), range);
    assert!(!consumer.validated());
    assert!(
        applied_ranges(&*engine.snapshot().unwrap(), "auth")
            .unwrap()
            .is_empty()
    );
}
