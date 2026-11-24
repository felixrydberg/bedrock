use bedrock_lib::storage::memory::MemoryEngine;

use super::*;

/// A valid ID: 26 base32 characters whose last two padding bits are zero.
fn id(c: char) -> String {
    format!("eu_{}a", c.to_string().repeat(25))
}

fn actor() -> Actor {
    Actor::new(&pb::Actor {
        namespace: "ns".into(),
        id: id('c'),
        kind: 0,
    })
    .unwrap()
}

fn store() -> Store {
    Store::new(Arc::new(MemoryEngine::new("volume")))
}

fn host(name: &str) -> pb::Holder {
    pb::Holder {
        cell: "cell-1".into(),
        host: name.into(),
    }
}

fn refused(answer: &pb::Answer) -> Refusal {
    Refusal::try_from(answer.refused).unwrap()
}

fn slot(answer: &pb::Answer) -> &pb::Slot {
    answer.slot.as_ref().unwrap()
}

fn entry(seq: u64, epoch: u64, changes: &[(&str, Option<&str>)]) -> pb::Entry {
    pb::Entry {
        seq,
        epoch,
        committed_at: 1000 + seq,
        kind: EntryKind::Data as i32,
        changes: changes
            .iter()
            .map(|(k, v)| pb::Change {
                key: k.as_bytes().to_vec(),
                value: v.map(|v| v.as_bytes().to_vec()),
            })
            .collect(),
        alarm: None,
    }
}

fn append(
    store: &Store,
    holder: &pb::Holder,
    epoch: u64,
    prev_epoch: u64,
    entries: Vec<pb::Entry>,
    commit: u64,
) -> pb::Answer {
    store
        .append(
            &actor(),
            &pb::AppendRequest {
                actor: None,
                holder: Some(holder.clone()),
                epoch,
                version: 0,
                prev_epoch,
                entries,
                commit,
            },
        )
        .unwrap()
        .answer
}

fn claimed(store: &Store, holder: &pb::Holder, epoch: u64) {
    let (answer, _) = store.claim(&actor(), holder, epoch, 0, |_| false).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
}

fn read(store: &Store, key: &str) -> Option<Vec<u8>> {
    let (answer, values, _) = store.read(&actor(), &[key.as_bytes().to_vec()], 0).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
    values[0].value.clone()
}

#[test]
fn a_reservation_must_rise_and_a_refusal_names_the_highest() {
    let store = store();
    let actor = actor();
    assert_eq!(
        refused(&store.reserve(&actor, 2, 0).unwrap()),
        Refusal::None
    );
    for epoch in [1, 2] {
        let answer = store.reserve(&actor, epoch, 0).unwrap();
        assert_eq!(refused(&answer), Refusal::StaleEpoch);
        assert_eq!(slot(&answer).reserved, 2);
    }
    assert_eq!(
        refused(&store.reserve(&actor, 3, 1).unwrap()),
        Refusal::None
    );
    // The reservation adopted version 1: the slot refuses an older version, for reservations and claims.
    assert_eq!(
        refused(&store.reserve(&actor, 4, 0).unwrap()),
        Refusal::StaleVersion
    );
    let (answer, _) = store.claim(&actor, &host("a"), 3, 0, |_| false).unwrap();
    assert_eq!(refused(&answer), Refusal::StaleVersion);
    assert_eq!(slot(&answer).reserved, 3);
}

#[test]
fn claims_need_a_higher_epoch_a_current_version_and_no_other_live_lease() {
    let store = store();
    let actor = actor();
    let (a, b) = (host("a"), host("b"));
    let claim = |holder: &pb::Holder, epoch, version, live: bool| {
        store
            .claim(&actor, holder, epoch, version, |_| live)
            .unwrap()
    };
    assert_eq!(refused(&claim(&a, 1, 1, true).0), Refusal::None);
    let (answer, _) = claim(&b, 1, 1, false);
    assert_eq!(refused(&answer), Refusal::StaleEpoch);
    assert_eq!(refused(&claim(&b, 2, 0, false).0), Refusal::StaleVersion);
    let (answer, _) = claim(&b, 2, 1, true);
    assert_eq!(refused(&answer), Refusal::LeaseLive);
    assert_eq!(
        slot(&answer).holder,
        Some(a.clone()),
        "a refusal names the live holder"
    );
    // The same holder may claim again whatever its lease.
    assert_eq!(claim(&a, 2, 1, true), (answer_of(&store, &a, 2, 1), false));
    // Once a's lease expired, b takes over, and that counts as an expiry.
    let (answer, expired) = claim(&b, 3, 1, false);
    assert_eq!(refused(&answer), Refusal::None);
    assert!(expired);
}

/// What the slot answers now, for comparing whole answers.
fn answer_of(store: &Store, holder: &pb::Holder, epoch: u64, version: u64) -> pb::Answer {
    let claim = store.load(&actor()).unwrap();
    assert_eq!((claim.epoch, claim.holder.as_ref()), (epoch, Some(holder)));
    assert_eq!(claim.version, version);
    answer(Refusal::None, &claim, true)
}

#[test]
fn a_release_lets_the_next_claim_skip_the_lease_wait() {
    let store = store();
    let (a, b) = (host("a"), host("b"));
    claimed(&store, &a, 1);
    let mut release = entry(1, 1, &[]);
    release.kind = EntryKind::Release as i32;
    release.alarm = Some(pb::Alarm { at_ms: Some(42) });
    assert!(slot(&append(&store, &a, 1, 0, vec![release], 1)).released);
    // The released actor's alarm is indexed by time.
    let index: Vec<_> = store
        .engine
        .scan(&KeyRange::prefix(*b"alarm/"))
        .unwrap()
        .map(|e| e.unwrap().0)
        .collect();
    assert_eq!(
        index,
        vec![
            [
                b"alarm/".as_slice(),
                &42u64.to_be_bytes(),
                &actor().prefix[2..]
            ]
            .concat()
        ]
    );
    let (answer, expired) = store.claim(&actor(), &b, 2, 0, |_| true).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
    assert!(!expired && !slot(&answer).released);
    // Held again, so out of the index.
    assert_eq!(
        store
            .engine
            .scan(&KeyRange::prefix(*b"alarm/"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn due_alarms_include_held_claim_hints_without_changing_the_release_index() {
    let store = store();
    let holder = host("a");
    claimed(&store, &holder, 1);
    let mut event = entry(1, 1, &[]);
    event.alarm = Some(pb::Alarm { at_ms: Some(42) });
    append(&store, &holder, 1, 0, vec![event], 1);
    assert!(store.due_alarms(41).unwrap().is_empty());
    let due = store.due_alarms(42).unwrap();
    assert_eq!(due.len(), 1);
    assert!(!due[0].released);
    assert_eq!(due[0].holder.as_ref(), Some(&holder));
    assert_eq!(due[0].at_ms, 42);
    assert_eq!(
        store
            .engine
            .scan(&KeyRange::prefix(*b"alarm/"))
            .unwrap()
            .count(),
        0
    );
    let mut release = entry(2, 1, &[]);
    release.kind = EntryKind::Release as i32;
    append(&store, &holder, 1, 1, vec![release], 2);
    let due = store.due_alarms(42).unwrap();
    assert_eq!(due.len(), 1);
    assert!(due[0].released);
    assert!(store.alarms.lock().unwrap().is_empty());
    // Reading candidates never acknowledges or removes an alarm.
    assert_eq!(store.due_alarms(42).unwrap().len(), 1);
}

#[test]
fn a_pending_alarm_change_does_not_suppress_the_committed_alarm() {
    let store = store();
    let holder = host("a");
    claimed(&store, &holder, 1);
    let mut event = entry(1, 1, &[]);
    event.alarm = Some(pb::Alarm { at_ms: Some(42) });
    assert_eq!(slot_end(&append(&store, &holder, 1, 0, vec![event], 1)), 1);

    // A clear above the commit point has not taken effect yet.
    let mut clear = entry(2, 1, &[]);
    clear.alarm = Some(pb::Alarm { at_ms: None });
    append(&store, &holder, 1, 1, vec![clear.clone()], 1);
    let due = store.due_alarms(42).unwrap();
    assert_eq!(
        (due.len(), due[0].at_ms),
        (1, 42),
        "a pending clear hid the committed alarm"
    );

    // Neither has a reschedule into the future.
    let mut later = entry(3, 1, &[]);
    later.alarm = Some(pb::Alarm { at_ms: Some(100) });
    append(&store, &holder, 1, 1, vec![later.clone()], 1);
    let due = store.due_alarms(42).unwrap();
    assert_eq!((due.len(), due[0].at_ms), (1, 42));

    // Once the clear commits, the pending reschedule is not an alarm yet.
    assert_eq!(slot_end(&append(&store, &holder, 1, 1, vec![clear], 2)), 3);
    assert!(store.due_alarms(99).unwrap().is_empty());

    // Committing the reschedule makes it the alarm.
    append(&store, &holder, 1, 1, vec![later], 3);
    assert_eq!(store.due_alarms(100).unwrap()[0].at_ms, 100);
}

#[test]
fn an_uncommitted_tail_keeps_the_hint_until_its_alarm_commits() {
    let store = store();
    let holder = host("a");
    claimed(&store, &holder, 1);
    // The alarm and its clear are both above the commit point.
    let mut set = entry(1, 1, &[]);
    set.alarm = Some(pb::Alarm { at_ms: Some(42) });
    append(&store, &holder, 1, 0, vec![set], 0);
    let mut clear = entry(2, 1, &[]);
    clear.alarm = Some(pb::Alarm { at_ms: None });
    append(&store, &holder, 1, 1, vec![clear.clone()], 0);

    // Nothing is committed, so nothing is due, but the hint must stay for the alarm the tail may commit.
    assert!(store.due_alarms(42).unwrap().is_empty());
    assert!(!store.alarms.lock().unwrap().is_empty());

    // The commit point reaches the alarm while the clear is still pending: still no alarm, but still wakeable.
    append(&store, &holder, 1, 1, vec![clear], 1);
    assert_eq!(
        store.load(&actor()).unwrap().alarm_ms,
        None,
        "the pending clear is still the tail"
    );
    let due = store.due_alarms(42).unwrap();
    assert_eq!((due.len(), due[0].at_ms), (1, 42));
}

#[test]
fn an_uncommitted_delete_leaves_the_committed_alarm_wakeable() {
    let store = store();
    let holder = host("a");
    claimed(&store, &holder, 1);
    let mut event = entry(1, 1, &[]);
    event.alarm = Some(pb::Alarm { at_ms: Some(42) });
    append(&store, &holder, 1, 0, vec![event], 1);
    // A `DELETE` above the commit point ends the claim but has not deleted the actor: its alarm still fires.
    append(&store, &holder, 1, 1, vec![delete(2, 1)], 1);
    let claim = store.load(&actor()).unwrap();
    assert!(claim.deleting.is_some() && claim.deleted.is_none());
    let due = store.due_alarms(42).unwrap();
    assert_eq!((due.len(), due[0].at_ms, due[0].released), (1, 42, true));
}

#[test]
fn due_alarm_scan_reports_corrupt_keys() {
    let store = store();
    let mut batch = WriteBatch::default();
    batch.put(b"alarm/bad".to_vec(), Vec::new());
    store.commit(batch).unwrap();
    assert!(matches!(
        store.due_alarms(u64::MAX),
        Err(Error::Storage(StorageError::Corruption(_)))
    ));
}

#[test]
fn a_pending_clear_keeps_a_released_alarm_indexed_until_commit_or_truncation() {
    for kind in [EntryKind::Release, EntryKind::Move, EntryKind::Delete] {
        for batched in [false, true] {
            for truncate in [false, true] {
                let store = store();
                let holder = host("a");
                claimed(&store, &holder, 1);
                let mut set = entry(1, 1, &[]);
                set.alarm = Some(pb::Alarm { at_ms: Some(42) });
                let mut clear = entry(2, 1, &[]);
                clear.alarm = Some(pb::Alarm { at_ms: None });
                let mut finish = entry(3, 1, &[]);
                finish.kind = kind as i32;
                if batched {
                    append(&store, &holder, 1, 0, vec![set, clear, finish.clone()], 1);
                } else {
                    append(&store, &holder, 1, 0, vec![set], 1);
                    append(&store, &holder, 1, 1, vec![clear, finish.clone()], 1);
                }
                let due = store.due_alarms(42).unwrap();
                assert_eq!(
                    due.len(),
                    1,
                    "{kind:?}, batched={batched}, truncate={truncate}"
                );
                assert_eq!((due[0].at_ms, due[0].released), (42, true));
                let indexed: Vec<_> = store
                    .engine
                    .scan(&KeyRange::prefix(*b"alarm/"))
                    .unwrap()
                    .map(|row| row.unwrap().0)
                    .collect();
                assert_eq!(indexed.len(), 1);
                assert_eq!(&indexed[0][6..14], &42u64.to_be_bytes());

                let copied = Store::new(Arc::new(MemoryEngine::new("copied")));
                copied.install(&records(&store), false).unwrap();
                let copied_due = copied.due_alarms(42).unwrap();
                assert_eq!(copied_due.len(), 1);
                assert_eq!((copied_due[0].at_ms, copied_due[0].released), (42, true));

                if truncate {
                    let next = host("b");
                    claimed(&store, &next, 2);
                    let mut noop = entry(2, 2, &[]);
                    noop.kind = EntryKind::Noop as i32;
                    append(&store, &next, 2, 1, vec![noop], 2);
                    let due = store.due_alarms(42).unwrap();
                    assert_eq!((due.len(), due[0].at_ms, due[0].released), (1, 42, false));
                } else {
                    // Only the clear commits; the ending entry remains a pending tail.
                    append(&store, &holder, 1, 1, vec![finish], 2);
                    assert!(store.due_alarms(42).unwrap().is_empty());
                }
                assert_eq!(
                    store
                        .engine
                        .scan(&KeyRange::prefix(*b"alarm/"))
                        .unwrap()
                        .count(),
                    0
                );
            }
        }
    }
}

#[test]
fn recovery_restores_a_pending_alarm_hint_before_any_alarm_has_committed() {
    let original = store();
    let holder = host("a");
    claimed(&original, &holder, 1);
    let mut set = entry(1, 1, &[]);
    set.alarm = Some(pb::Alarm { at_ms: Some(42) });
    let mut clear = entry(2, 1, &[]);
    clear.alarm = Some(pb::Alarm { at_ms: None });
    append(&original, &holder, 1, 0, vec![set, clear.clone()], 0);
    let reopened = Store::new(original.engine.clone());
    reopened.recover().unwrap();
    assert!(reopened.due_alarms(42).unwrap().is_empty());
    append(&reopened, &holder, 1, 1, vec![clear.clone()], 1);
    let due = reopened.due_alarms(42).unwrap();
    assert_eq!(
        due.len(),
        1,
        "the restored tail's alarm became committed without a hint"
    );
    assert_eq!(due[0].at_ms, 42);
    append(&reopened, &holder, 1, 1, vec![clear], 2);
    let cleared = Store::new(original.engine.clone());
    cleared.recover().unwrap();
    assert!(cleared.alarms.lock().unwrap().is_empty());
}

#[test]
fn installing_a_held_alarm_tail_restores_discovery_without_registering_cleared_claims() {
    for repair in [false, true] {
        for committed in 0..=2 {
            let source = store();
            let holder = host("a");
            claimed(&source, &holder, 1);
            let mut set = entry(1, 1, &[]);
            set.alarm = Some(pb::Alarm { at_ms: Some(42) });
            let mut clear = entry(2, 1, &[]);
            clear.alarm = Some(pb::Alarm { at_ms: None });
            append(&source, &holder, 1, 0, vec![set, clear.clone()], committed);
            let target = store();
            target.install(&records(&source), repair).unwrap();
            if committed == 2 {
                assert!(target.alarms.lock().unwrap().is_empty());
                continue;
            }
            if committed == 0 {
                assert!(target.due_alarms(42).unwrap().is_empty());
                append(&target, &holder, 1, 1, vec![clear], 1);
            }
            let due = target.due_alarms(42).unwrap();
            assert_eq!(due.len(), 1, "repair={repair}, committed={committed}");
            assert_eq!(due[0].at_ms, 42);
        }
    }
}

#[test]
fn held_alarm_hints_recover_after_reopen_and_prune_after_clear_delete_and_range_removal() {
    let original = store();
    let holder = host("a");
    claimed(&original, &holder, 1);
    let mut event = entry(1, 1, &[]);
    event.alarm = Some(pb::Alarm { at_ms: Some(42) });
    append(&original, &holder, 1, 0, vec![event], 1);
    let repaired = store();
    repaired.install(&records(&original), true).unwrap();
    assert_eq!(repaired.due_alarms(42).unwrap().len(), 1);
    let reopened = Store::new(original.engine.clone());
    reopened.recover().unwrap();
    assert_eq!(reopened.due_alarms(42).unwrap().len(), 1);
    let mut clear = entry(2, 1, &[]);
    clear.alarm = Some(pb::Alarm { at_ms: None });
    append(&reopened, &holder, 1, 1, vec![clear], 2);
    assert!(reopened.due_alarms(42).unwrap().is_empty());
    assert!(reopened.alarms.lock().unwrap().is_empty());

    let mut again = entry(3, 1, &[]);
    again.alarm = Some(pb::Alarm { at_ms: Some(42) });
    append(&reopened, &holder, 1, 1, vec![again], 3);
    assert_eq!(reopened.due_alarms(42).unwrap().len(), 1);
    let mut delete = entry(4, 1, &[]);
    delete.kind = EntryKind::Delete as i32;
    append(&reopened, &holder, 1, 1, vec![delete], 4);
    assert!(reopened.due_alarms(42).unwrap().is_empty());
    assert!(reopened.alarms.lock().unwrap().is_empty());

    // A removed range can later be copied again; a stale hint must not hide its new alarm.
    reopened.clear(0, 0).unwrap();
    assert!(reopened.due_alarms(42).unwrap().is_empty());
    reopened.import(&actor(), 2, 1, Some(42), 0).unwrap();
    assert_eq!(reopened.due_alarms(42).unwrap().len(), 1);
    reopened.claim(&actor(), &holder, 3, 0, |_| false).unwrap();
    reopened.clear(0, 0).unwrap();
    assert!(reopened.due_alarms(42).unwrap().is_empty());
    assert!(reopened.alarms.lock().unwrap().is_empty());
}

#[test]
fn appends_are_written_only_at_the_promise() {
    let store = store();
    let (a, b) = (host("a"), host("b"));
    claimed(&store, &a, 1);
    assert_eq!(
        refused(&append(
            &store,
            &a,
            1,
            0,
            vec![entry(1, 1, &[("k", Some("1"))])],
            0
        )),
        Refusal::None
    );
    claimed(&store, &b, 2);
    // a's epoch is stale now; neither its epoch nor b's epoch under a's name is the promise.
    for epoch in [1, 2] {
        let answer = append(
            &store,
            &a,
            epoch,
            1,
            vec![entry(2, epoch, &[("k", Some("2"))])],
            0,
        );
        assert_eq!(refused(&answer), Refusal::NotPromised);
    }
    assert_eq!(read(&store, "k"), Some(b"1".to_vec()));
}

#[test]
fn a_gap_or_a_different_previous_epoch_is_refused_with_the_log_end() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 2);
    assert_eq!(
        refused(&append(&store, &a, 2, 0, vec![entry(1, 2, &[])], 0)),
        Refusal::None
    );
    let gap = append(&store, &a, 2, 2, vec![entry(3, 2, &[])], 0);
    assert_eq!(refused(&gap), Refusal::LogGap);
    assert_eq!(slot(&gap).end, Some(pb::LogEnd { seq: 1, epoch: 2 }));
    assert_eq!(
        refused(&append(&store, &a, 2, 1, vec![entry(2, 2, &[])], 0)),
        Refusal::LogGap
    );
    // A duplicate is skipped, not refused.
    assert_eq!(
        refused(&append(
            &store,
            &a,
            2,
            0,
            vec![entry(1, 2, &[]), entry(2, 2, &[])],
            0
        )),
        Refusal::None
    );
    assert_eq!(
        slot(&append(&store, &a, 2, 2, vec![entry(2, 2, &[])], 0))
            .end
            .unwrap()
            .seq,
        2
    );
}

#[test]
fn a_new_epoch_truncates_an_old_uncommitted_tail_and_its_overlay() {
    let store = store();
    let (a, b) = (host("a"), host("b"));
    claimed(&store, &a, 1);
    let old = vec![
        entry(1, 1, &[("kept", Some("1")), ("both", Some("1"))]),
        entry(2, 1, &[("both", Some("2")), ("gone", Some("2"))]),
        entry(3, 1, &[("gone", None)]),
    ];
    append(&store, &a, 1, 0, old, 1);
    // b recovered a log with entry 1 only and writes its own entry 2.
    claimed(&store, &b, 2);
    let answer = append(
        &store,
        &b,
        2,
        1,
        vec![entry(2, 2, &[("new", Some("b"))])],
        0,
    );
    assert_eq!(refused(&answer), Refusal::None);
    assert_eq!(slot(&answer).end, Some(pb::LogEnd { seq: 2, epoch: 2 }));
    assert_eq!(read(&store, "kept"), Some(b"1".to_vec()));
    assert_eq!(
        read(&store, "both"),
        Some(b"1".to_vec()),
        "restored from entry 1"
    );
    assert_eq!(read(&store, "gone"), None);
    assert_eq!(read(&store, "new"), Some(b"b".to_vec()));
    let log = store.entries(&actor(), 1, 2).unwrap();
    assert_eq!(
        log.iter().map(|e| (e.seq, e.epoch)).collect::<Vec<_>>(),
        [(1, 1), (2, 2)]
    );
    // No entry left writes `gone`, so reads fall back to S3 for it.
    assert_eq!(
        store.engine.get(&data_key(&actor(), b"gone")).unwrap(),
        None
    );
}

#[test]
fn a_repaired_slot_takes_claims_and_appends_without_voting_until_a_newer_claim_commits() {
    let store = store();
    let actor = actor();
    let a = host("a");
    // What S-06 leaves after copying: the peers' highest reservation (3) as the floor.
    let repaired = pb::ClaimRecord {
        reserved: 3,
        repaired: true,
        ..Default::default()
    };
    store
        .engine
        .put(&claim_key(&actor), &repaired.encode_to_vec())
        .unwrap();
    assert_eq!(
        refused(&store.reserve(&actor, 4, 0).unwrap()),
        Refusal::NotVoting
    );
    // A claim at the floor or below commits nothing that re-enables the vote.
    let (answer, _) = store.claim(&actor, &a, 3, 0, |_| false).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
    assert!(!slot(&answer).voting);
    let answer = append(&store, &a, 3, 0, vec![entry(1, 3, &[])], 1);
    assert!(!slot(&answer).voting);
    assert!(!slot(&append(&store, &a, 3, 3, vec![entry(2, 3, &[])], 2)).voting);
    claimed(&store, &a, 4);
    // The no-op at 4 is not committed yet. The append that learns it committed still answers as non-voting; the next
    // one votes.
    assert!(!slot(&append(&store, &a, 4, 3, vec![entry(3, 4, &[])], 2)).voting);
    assert!(!slot(&append(&store, &a, 4, 4, vec![entry(4, 4, &[])], 3)).voting);
    assert!(slot(&append(&store, &a, 4, 4, vec![entry(5, 4, &[])], 4)).voting);
    assert_eq!(
        refused(&store.reserve(&actor, 5, 0).unwrap()),
        Refusal::None
    );
}

#[test]
fn the_promised_holder_reads_the_log_and_a_missing_entry_is_corruption() {
    let store = store();
    let (a, b) = (host("a"), host("b"));
    claimed(&store, &a, 1);
    let log: Vec<_> = (1..=3).map(|seq| entry(seq, 1, &[])).collect();
    append(&store, &a, 1, 0, log.clone(), 0);
    let (answer, entries, more) = store.log(&actor(), &a, 1, 1, 2).unwrap();
    assert_eq!(
        (refused(&answer), entries.as_slice(), more),
        (Refusal::None, &log[..2], true)
    );
    let (_, entries, more) = store.log(&actor(), &a, 1, 3, 10).unwrap();
    assert_eq!((entries.as_slice(), more), (&log[2..], false));
    // Only the promise reads: another holder, or the right holder at another epoch, gets nothing.
    for (holder, epoch) in [(&b, 1), (&a, 2)] {
        let (answer, entries, _) = store.log(&actor(), holder, epoch, 1, 10).unwrap();
        assert_eq!((refused(&answer), entries.len()), (Refusal::NotPromised, 0));
    }
    // A hole below the log end fails as a point read of it does.
    store.engine.delete(&log_key(&actor(), 2)).unwrap();
    for result in [
        store.log(&actor(), &a, 1, 1, 10).map(|_| ()),
        store.entry(&actor(), 2).map(|_| ()),
    ] {
        let Err(Error::Storage(StorageError::Corruption(context))) = result else {
            panic!("a missing entry is corruption");
        };
        assert_eq!(context.partition.as_deref(), Some("log 2"));
    }
}

#[test]
fn reads_refuse_a_slot_behind_min_seq_and_a_flushed_miss_is_cold() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 1);
    append(&store, &a, 1, 0, vec![entry(1, 1, &[("k", Some("1"))])], 0);
    let (answer, _, _) = store.read(&actor(), &[b"k".to_vec()], 2).unwrap();
    assert_eq!(refused(&answer), Refusal::Behind);
    assert_eq!(read(&store, "missing"), None);
    // Truncation needs the commit point.
    assert_eq!(
        refused(&store.truncate(&actor(), 1).unwrap().answer),
        Refusal::Behind
    );
    append(&store, &a, 1, 1, vec![entry(2, 1, &[("j", Some("2"))])], 2);
    let truncated = store.truncate(&actor(), 1).unwrap();
    assert_eq!(slot(&truncated.answer).flushed, 1);
    assert!(truncated.overlay < 0);
    let (_, _, cold) = store
        .read(&actor(), &[b"k".to_vec(), b"j".to_vec()], 0)
        .unwrap();
    assert_eq!(cold, vec![0]);
    assert_eq!(read(&store, "j"), Some(b"2".to_vec()));
    // The log continues after the flushed point.
    assert_eq!(
        refused(&append(
            &store,
            &a,
            1,
            1,
            vec![entry(2, 1, &[]), entry(3, 1, &[])],
            2
        )),
        Refusal::None
    );
}

#[test]
fn a_slot_behind_s3_skips_to_it_and_keeps_its_vote() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 1);
    append(&store, &a, 1, 0, vec![entry(1, 1, &[("k", Some("1"))])], 0);
    // The holder recovered from a slot that flushed through 5, so it cannot resend 2..=5.
    let gap = append(&store, &a, 1, 1, vec![entry(6, 1, &[])], 6);
    assert_eq!(refused(&gap), Refusal::LogGap);
    // Not to a point a newer holder wrote: this slot's holder may not have it.
    assert!(
        store
            .skip(&actor(), &a, 1, 5, 2, None, None)
            .unwrap()
            .is_none()
    );
    // Not for another holder, or for an epoch the slot no longer promises.
    assert!(
        store
            .skip(&actor(), &host("b"), 1, 5, 1, None, None)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .skip(&actor(), &a, 2, 5, 1, None, None)
            .unwrap()
            .is_none()
    );
    let skipped = store
        .skip(&actor(), &a, 1, 5, 1, Some(42), None)
        .unwrap()
        .unwrap();
    let after = slot(&skipped.answer);
    assert!(after.voting);
    assert_eq!((after.flushed, after.committed), (5, 5));
    assert_eq!(after.end.unwrap().seq, 5);
    assert!(skipped.overlay < 0, "the overlay record of entry 1 is gone");
    // `k` is read from S3 now.
    let (_, _, cold) = store.read(&actor(), &[b"k".to_vec()], 0).unwrap();
    assert_eq!(cold, vec![0]);
    assert_eq!(
        refused(&append(&store, &a, 1, 1, vec![entry(6, 1, &[])], 6)),
        Refusal::None
    );
    // A log that already reaches S3's point stays.
    assert!(
        store
            .skip(&actor(), &a, 1, 5, 1, None, None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn scans_list_live_keys_in_order() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 1);
    let changes = [
        ("b", Some("2")),
        ("a", Some("1")),
        ("c", None),
        ("d", Some("4")),
    ];
    append(&store, &a, 1, 0, vec![entry(1, 1, &changes)], 0);
    let (_, values, more) = store.scan(&actor(), b"a", 2, 0).unwrap();
    let keys: Vec<_> = values.iter().map(|v| v.key.as_slice()).collect();
    assert_eq!((keys, more), (vec![b"a".as_slice(), b"b"], true));
    let (_, values, more) = store.scan(&actor(), b"b\0", 10, 0).unwrap();
    assert_eq!((values.len(), more), (1, false));
}

#[test]
fn repair_records_cover_one_hash_span() {
    let store = store();
    let other = Actor::new(&pb::Actor {
        namespace: "ns".into(),
        id: id('z'),
        kind: 0,
    })
    .unwrap();
    store.reserve(&actor(), 1, 0).unwrap();
    store.reserve(&other, 1, 0).unwrap();
    let (records, more) = store.records(0, actor().hash() + 1, b"", 10).unwrap();
    assert_eq!((records.len(), more), (1, false));
    // The reservation lives in the claim record, so a repair copy carries it.
    let claim = pb::ClaimRecord::decode(records[0].value.as_slice()).unwrap();
    assert_eq!(claim.reserved, 1);
    let (records, more) = store.records(0, 0, b"", 1).unwrap();
    assert_eq!((records.len(), more), (1, true));
    let (records, _) = store.records(0, 0, &records[0].key, 10).unwrap();
    assert_eq!(records[0].key, claim_key(&other));
}

/// A holder over three slots, enough of S-04's client to test the slot side: reserve and claim at a quorum, recover the
/// most advanced log, append a no-op, then commits, each acknowledged once two voting slots answered.
struct Holder {
    name: pb::Holder,
    epoch: u64,
    log: Vec<pb::Entry>,
    /// Per slot: the next sequence to send.
    next: [u64; 3],
    commit: u64,
}

fn send(store: &Store, holder: &mut Holder, slot: usize) -> Option<pb::Answer> {
    loop {
        let from = holder.next[slot].max(1);
        let entries = holder.log[from as usize - 1..].to_vec();
        if entries.is_empty() {
            return None;
        }
        let prev_epoch = if from > 1 {
            holder.log[from as usize - 2].epoch
        } else {
            0
        };
        let answer = append(
            store,
            &holder.name,
            holder.epoch,
            prev_epoch,
            entries,
            holder.commit,
        );
        match refused(&answer) {
            Refusal::LogGap => {
                let end = slot_end(&answer);
                holder.next[slot] = (from - 1).min(end + 1);
            }
            Refusal::None => {
                holder.next[slot] = holder.log.len() as u64 + 1;
                return Some(answer);
            }
            _ => return Some(answer),
        }
    }
}

fn slot_end(answer: &pb::Answer) -> u64 {
    slot(answer).end.unwrap_or_default().seq
}

/// Sends the holder's log to every slot `up` allows; returns whether two voting slots hold all of it.
fn replicate(slots: &[Store; 3], holder: &mut Holder, up: impl Fn(usize) -> bool) -> bool {
    let mut acks = 0;
    for (i, store) in slots.iter().enumerate() {
        if !up(i) {
            continue;
        }
        if let Some(answer) = send(store, holder, i)
            && refused(&answer) == Refusal::None
            && slot(&answer).voting
        {
            acks += 1;
        }
    }
    if acks >= 2 {
        holder.commit = holder.log.len() as u64;
    }
    acks >= 2
}

/// Reserve, claim, recover and commit a no-op, through the slots `up` allows.
fn open(slots: &[Store; 3], name: &str, up: impl Fn(usize) -> bool + Copy) -> Holder {
    let seen = slots
        .iter()
        .map(|s| s.load(&actor()).unwrap().reserved)
        .max()
        .unwrap();
    let epoch = seen + 1;
    let reserved = (0..3)
        .filter(|&i| {
            up(i) && refused(&slots[i].reserve(&actor(), epoch, 0).unwrap()) == Refusal::None
        })
        .count();
    assert!(reserved >= 2, "a quorum reserves {epoch}");
    let name = host(name);
    let mut best: Option<(u64, u64, usize)> = None;
    let mut granted = 0;
    for i in (0..3).filter(|&i| up(i)) {
        let (answer, _) = slots[i]
            .claim(&actor(), &name, epoch, 0, |_| false)
            .unwrap();
        if refused(&answer) == Refusal::None && slot(&answer).voting {
            granted += 1;
            let end = slot(&answer).end.unwrap_or_default();
            best = best.max(Some((end.epoch, end.seq, i)));
        }
    }
    assert!(granted >= 2, "a quorum grants {epoch}");
    let (_, end, from) = best.unwrap();
    let (_, mut log, more) = slots[from].log(&actor(), &name, epoch, 1, 1000).unwrap();
    assert!(!more && log.len() as u64 == end);
    let mut noop = entry(log.len() as u64 + 1, epoch, &[]);
    noop.kind = EntryKind::Noop as i32;
    log.push(noop);
    let mut holder = Holder {
        name,
        epoch,
        log,
        next: [1; 3],
        commit: 0,
    };
    assert!(replicate(slots, &mut holder, up), "the no-op commits");
    holder
}

/// #111 acceptance: killing any one slot at any append boundary, before or after its fsync, loses no acknowledged
/// commit, and each commit is acknowledged after two slots' synced writes with no S3 call. The next holder recovers
/// every acknowledged entry from any two slots.
#[test]
fn killing_one_slot_at_every_append_boundary_loses_no_acknowledged_commit() {
    const COMMITS: u64 = 4;
    // The holder commits while the victim dies at the boundary; returns the slots after the crash and the acked log.
    let crash = |victim: usize, boundary: u64, after_write: bool| {
        let slots = [store(), store(), store()];
        let mut holder = open(&slots, "h1", |_| true);
        let mut acked = holder.log.clone();
        for n in 0..COMMITS {
            let seq = holder.log.len() as u64 + 1;
            let key = format!("k{n}");
            holder
                .log
                .push(entry(seq, holder.epoch, &[(&key, Some(&key))]));
            let down = |i: usize| i == victim && n >= boundary;
            if after_write && n == boundary {
                // The victim fsyncs this append, then dies before answering.
                send(&slots[victim], &mut holder, victim);
            }
            if replicate(&slots, &mut holder, |i| !down(i)) {
                acked = holder.log.clone();
            }
        }
        (slots, acked)
    };
    for victim in 0..3 {
        for boundary in 0..=COMMITS {
            for after_write in [false, true] {
                // The holder dies too. The victim restarts with what it synced; the next holder may use it and
                // either survivor. Each quorum recovers from the same post-crash slots, rebuilt for it.
                for lost in 0..3 {
                    let (slots, acked) = crash(victim, boundary, after_write);
                    let up = |i: usize| i != lost;
                    let next = open(&slots, &format!("h2-{lost}"), up);
                    for (i, entry) in acked.iter().enumerate() {
                        assert_eq!(
                            next.log.get(i),
                            Some(entry),
                            "victim {victim}, boundary {boundary}, after write {after_write}, without slot {lost}"
                        );
                    }
                    for i in (0..3).filter(|&i| up(i)) {
                        assert_eq!(
                            read(&slots[i], "k0"),
                            Some(b"k0".to_vec()),
                            "slot {i} serves the recovered overlay"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn a_range_size_reports_its_bytes_and_a_middle_between_them() {
    let store = store();
    for hash in [1u64 << 60, 3 << 60] {
        let key = [b"s/".as_slice(), &hash.to_be_bytes(), b"ns\0x\0claim"].concat();
        store.engine.put(&key, &[0; 1000]).unwrap();
    }
    let size = store.size(0, 0).unwrap();
    assert!(size.bytes > 2000);
    assert!(
        size.middle > 1 << 60 && size.middle <= 3 << 60,
        "{:x}",
        size.middle
    );
    let empty = store.size(4 << 60, 5 << 60).unwrap();
    assert_eq!((empty.bytes, empty.middle), (0, 0));
}

#[test]
fn sizes_are_republished_only_for_new_ranges_or_large_changes() {
    let size = |start, bytes| RangeSize {
        span: Some(Span { start, end: 0 }),
        bytes,
        middle: 0,
    };
    let published = [size(0, 100 << 20)];
    assert!(!worth_publishing(&published, &[size(0, 110 << 20)]));
    assert!(worth_publishing(&published, &[size(0, 120 << 20)]));
    assert!(worth_publishing(&published, &[size(1, 100 << 20)]));
    assert!(worth_publishing(&published, &[]));
    // Small ranges wait for a mebibyte of change.
    assert!(!worth_publishing(&[size(0, 1000)], &[size(0, 3000)]));
}

/// A full slot refuses new entries, but still learns the commit point for the log it holds, so a flush can truncate it
/// (seed 4 of `tests/sim/tests/volume_cold.rs`: otherwise a lagging slot stays full for good).
#[test]
fn a_full_slot_still_learns_the_commit_point_and_can_be_truncated() {
    let store = Store::new(Arc::new(MemoryEngine::new("volume"))).with_max_unflushed(100);
    let a = host("a");
    claimed(&store, &a, 1);
    let request = |entries: Vec<pb::Entry>, prev_epoch: u64, commit: u64| pb::AppendRequest {
        actor: None,
        holder: Some(a.clone()),
        epoch: 1,
        version: 0,
        prev_epoch,
        entries,
        commit,
    };
    let big = "v".repeat(200);
    store
        .append(
            &actor(),
            &request(vec![entry(1, 1, &[("k", Some(&big))])], 0, 0),
        )
        .unwrap();
    let full = store.append(
        &actor(),
        &request(
            vec![entry(1, 1, &[("k", Some(&big))]), entry(2, 1, &[])],
            0,
            1,
        ),
    );
    assert!(matches!(full, Err(Error::Backpressure)));
    // S3 holds more than this slot knows is committed: it truncates what it can and says it is behind.
    let truncated = store.truncate(&actor(), 5).unwrap();
    assert_eq!(refused(&truncated.answer), Refusal::Behind);
    assert_eq!(slot(&truncated.answer).flushed, 1);
    assert_eq!(store.unflushed(), 0);
    store
        .append(&actor(), &request(vec![entry(2, 1, &[])], 1, 1))
        .unwrap();
}

/// While the node is full, a new epoch may replace an old uncommitted tail with as much or less, but not grow the log.
#[test]
fn a_full_slot_takes_a_replaced_tail_only_if_the_log_does_not_grow() {
    let store = Store::new(Arc::new(MemoryEngine::new("volume"))).with_max_unflushed(100);
    let (a, b) = (host("a"), host("b"));
    claimed(&store, &a, 1);
    let big = "v".repeat(200);
    let tail = vec![entry(1, 1, &[("k", Some(&big))]), entry(2, 1, &[])];
    append(&store, &a, 1, 0, tail, 0);
    claimed(&store, &b, 2);
    let request = |entries: Vec<pb::Entry>| pb::AppendRequest {
        actor: None,
        holder: Some(b.clone()),
        epoch: 2,
        version: 0,
        prev_epoch: 1,
        entries,
        commit: 0,
    };
    let growing = vec![entry(2, 2, &[("k", Some(&big))]), entry(3, 2, &[])];
    let refused = store.append(&actor(), &request(growing));
    assert!(matches!(refused, Err(Error::Backpressure)));
    let same = store
        .append(&actor(), &request(vec![entry(2, 2, &[])]))
        .unwrap();
    assert_eq!(slot(&same.answer).end.unwrap().epoch, 2);
}

/// A maintainer's truncation records what the actor's other slots owe; the record survives a restart, and only the
/// obligation it was raised for settles it.
#[test]
fn owed_truncations_survive_a_restart_until_settled() {
    let engine = Arc::new(MemoryEngine::new("volume"));
    let store = Store::new(engine.clone());
    let kept = engine.clone();
    let a = host("a");
    claimed(&store, &a, 1);
    append(&store, &a, 1, 0, vec![entry(1, 1, &[("k", Some("1"))])], 1);
    store.truncate_owing(&actor(), 1).unwrap();
    let restarted = Store::new(engine);
    restarted.recover().unwrap();
    let owed = restarted.owed();
    assert_eq!(owed.len(), 1);
    assert_eq!((owed[0].0.id.as_str(), owed[0].1), (id('c').as_str(), 1));
    // A settle for another point leaves the obligation.
    restarted.settle(&actor(), 7).unwrap();
    assert_eq!(restarted.owed().len(), 1);
    restarted.settle(&actor(), 1).unwrap();
    assert!(restarted.owed().is_empty());
    // The settle's deletion is durable: a second restart finds nothing owed.
    let again = Store::new(kept);
    again.recover().unwrap();
    assert!(again.owed().is_empty());
}

/// A lower truncation leaves a higher obligation in place, in memory and on disk, so settling the lower one keeps it.
#[test]
fn an_owed_truncation_only_rises() {
    let engine = Arc::new(MemoryEngine::new("volume"));
    let store = Store::new(engine.clone());
    let a = host("a");
    claimed(&store, &a, 1);
    store.truncate_owing(&actor(), 10).unwrap();
    store.truncate_owing(&actor(), 5).unwrap();
    assert_eq!(store.owed()[0].1, 10);
    store.settle(&actor(), 5).unwrap();
    assert_eq!(store.owed()[0].1, 10);
    let restarted = Store::new(engine);
    restarted.recover().unwrap();
    assert_eq!(restarted.owed()[0].1, 10);
}

fn records(store: &Store) -> Vec<pb::Record> {
    store.records(0, 0, &[], MAX_LIMIT).unwrap().0
}

#[test]
fn a_sealed_range_refuses_reservations_claims_and_appends() {
    let store = store();
    let actor = actor();
    let a = host("a");
    claimed(&store, &a, 1);
    store.seal(0, 0).unwrap();
    assert!(matches!(store.reserve(&actor, 2, 0), Err(Error::Sealed)));
    assert!(matches!(
        store.claim(&actor, &a, 2, 0, |_| false),
        Err(Error::Sealed)
    ));
    let request = pb::AppendRequest {
        holder: Some(a.clone()),
        epoch: 1,
        entries: vec![entry(1, 1, &[])],
        ..Default::default()
    };
    assert!(matches!(store.append(&actor, &request), Err(Error::Sealed)));
    // The seal is durable.
    let reopened = Store::new(store.engine.clone());
    reopened.recover().unwrap();
    assert!(matches!(reopened.reserve(&actor, 2, 0), Err(Error::Sealed)));
}

#[test]
fn an_actor_with_no_record_in_a_repaired_range_starts_repaired() {
    let store = store();
    let actor = actor();
    assert!(slot(&store.reserve(&actor, 1, 0).unwrap()).voting);
    store.mark_repaired(0, 0).unwrap();
    assert_eq!(
        refused(&store.reserve(&actor, 2, 0).unwrap()),
        Refusal::None,
        "a record written before the mark keeps its own state"
    );
    let other = Actor::new(&pb::Actor {
        namespace: "ns".into(),
        id: id('d'),
        kind: 0,
    })
    .unwrap();
    assert_eq!(
        refused(&store.reserve(&other, 1, 0).unwrap()),
        Refusal::NotVoting
    );
}

#[test]
fn a_repair_keeps_the_more_advanced_log_and_the_higher_promise_and_reservation() {
    let a = host("a");
    // One peer has the log; the other only a higher reservation.
    let logged = store();
    logged.reserve(&actor(), 1, 0).unwrap();
    claimed(&logged, &a, 1);
    append(
        &logged,
        &a,
        1,
        0,
        vec![entry(1, 1, &[("k", Some("v"))]), entry(2, 1, &[])],
        0,
    );
    let reserved = store();
    reserved.reserve(&actor(), 3, 0).unwrap();
    // A peer that is itself repaired is no voting copy, however far its log goes.
    let repaired = store();
    claimed(&repaired, &a, 5);
    append(&repaired, &a, 5, 0, vec![entry(1, 5, &[])], 0);
    repaired.mark_repaired(0, 0).unwrap();
    let marked = pb::ClaimRecord {
        repaired: true,
        ..repaired.load(&actor()).unwrap()
    };
    repaired
        .engine
        .put(&claim_key(&actor()), &marked.encode_to_vec())
        .unwrap();

    let target = store();
    for source in [&reserved, &logged, &repaired] {
        target.install(&records(source), true).unwrap();
    }
    let claim = target.load(&actor()).unwrap();
    assert_eq!((claim.epoch, claim.holder.as_ref()), (1, Some(&a)));
    assert_eq!(claim.reserved, 3);
    assert!(claim.repaired);
    assert_eq!(end(&claim), pb::LogEnd { seq: 2, epoch: 1 });
    assert_eq!(read(&target, "k"), Some(b"v".to_vec()));
    // A range handoff copies a slot as it is.
    let handed = store();
    handed.install(&records(&reserved), false).unwrap();
    assert!(!handed.load(&actor()).unwrap().repaired);
}

#[test]
fn a_pushed_set_without_a_held_range_forgets_it_and_clearing_drops_its_records() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 1);
    store.initialize(true).unwrap();
    store.hold(0, 0, 0).unwrap();
    let held = store.keep(&[(0, 1 << 63, 0)]).unwrap();
    assert_eq!(held.list(), [(0, 1 << 63)]);
    assert_eq!(store.fresh(), Some(false));
    assert_eq!(store.forgotten(), 1);
    assert_eq!(
        store.keep(&[(0, 0, 0)]).unwrap(),
        held,
        "nothing new is held"
    );
    // A pushed set that gives the range anew, at another generation, forgets the copy.
    assert_eq!(store.keep(&[(0, 0, 7)]).unwrap(), Spans::default());
    store.hold(0, 0, 7).unwrap();
    store.seal(0, 0).unwrap();
    store.clear(0, 0).unwrap();
    assert!(records(&store).is_empty());
    assert_eq!(store.held(), Spans::default());
    assert_eq!(store.sealed(), Spans::default());
}

#[test]
fn spans_coalesce_and_cut() {
    let mut spans = Spans::default();
    spans.add(10, 20);
    spans.add(20, 30);
    spans.add(40, 0);
    assert_eq!(spans.list(), [(10, 30), (40, 0)]);
    assert!(spans.covers(12, 30) && !spans.covers(12, 31) && spans.covers(50, 0));
    assert!(spans.contains(u64::MAX) && !spans.contains(35));
    spans.remove(15, 45);
    assert_eq!(spans.list(), [(10, 15), (45, 0)]);
    assert!(spans.overlaps(0, 11) && !spans.overlaps(15, 45));
    assert_eq!(spans.within(12, 50).list(), [(12, 15), (45, 50)]);
}

#[test]
fn copied_log_bytes_count_once_and_a_copy_cut_short_clears_to_zero() {
    let a = host("a");
    let source = store();
    claimed(&source, &a, 1);
    append(
        &source,
        &a,
        1,
        0,
        vec![entry(1, 1, &[("k", Some("v"))]), entry(2, 1, &[])],
        0,
    );
    let logged = source.unflushed();
    assert!(logged > 0);
    let target = store();
    target.install(&records(&source), true).unwrap();
    assert_eq!(target.unflushed(), logged, "counted when installed");
    target.install(&records(&source), true).unwrap();
    assert_eq!(
        target.unflushed(),
        logged,
        "a log no more advanced is not taken"
    );
    target.adopt(0, 0).unwrap();
    assert_eq!(target.unflushed(), logged, "adopting counts nothing again");
    let cut = store();
    cut.install(&records(&source), true).unwrap();
    cut.clear(0, 0).unwrap();
    assert_eq!(
        cut.unflushed(),
        0,
        "a copy cut short and cleared leaves nothing counted"
    );
}

#[test]
fn a_copy_finished_after_a_set_forgot_it_is_not_held_and_reports_forget_nothing() {
    let store = store();
    store.initialize(true).unwrap();
    let forgotten = store.start_copy(0, 0, 3);
    store.keep(&[(0, 0, 4)]).unwrap();
    assert!(!store.finish_copy(0, 0, 3, forgotten, true).unwrap());
    assert!(store.held().list().is_empty());
    // A copy that failed is no longer being made, so a set without it forgets nothing.
    let forgotten = store.start_copy(0, 0, 4);
    assert!(!store.finish_copy(0, 0, 4, forgotten, false).unwrap());
    let before = store.forgotten();
    store.keep(&[]).unwrap();
    assert_eq!(store.forgotten(), before);
    let forgotten = store.start_copy(0, 0, 4);
    assert!(store.finish_copy(0, 0, 4, forgotten, true).unwrap());
    assert_eq!(store.holding(&[(0, 0, 4)]).list(), [(0, 0)]);
    assert!(store.holding(&[(0, 0, 5)]).list().is_empty());
    assert_eq!(
        store.held().list(),
        [(0, 0)],
        "reading a report forgets nothing"
    );
}

#[test]
fn a_sealed_slot_does_not_skip_to_s3() {
    let store = store();
    claimed(&store, &host("a"), 1);
    store.seal(0, 0).unwrap();
    // A second seal of the same span still waits for writes in flight, and the seal refuses the skip.
    store.seal(0, 0).unwrap();
    assert!(matches!(
        store.skip(&actor(), &host("a"), 1, 5, 1, None, None),
        Err(Error::Sealed)
    ));
}

#[test]
fn a_clear_waits_for_writes_already_admitted() {
    let store = store();
    // A write holds the last stripe; the clear takes the stripes in order, so it is waiting once it holds the first.
    let last = store.locks.len() - 1;
    let (done, cleared) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        // Taken inside the scope, so a failed assertion releases it before the clear's thread is joined.
        let admitted = store.locks[last].lock().unwrap();
        s.spawn(|| {
            store.clear(0, 0).unwrap();
            done.send(()).unwrap();
        });
        let mut tries = 0;
        while store.locks[0].try_lock().is_ok() {
            tries += 1;
            assert!(tries < 100_000_000, "the clear never started");
            std::thread::yield_now();
        }
        assert!(
            cleared.try_recv().is_err(),
            "the clear waits while a write holds its stripe"
        );
        drop(admitted);
        cleared
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the clear finishes once the write does");
    });
}

#[test]
fn a_range_mark_with_an_empty_span_is_corruption() {
    let engine = Arc::new(MemoryEngine::new("volume"));
    let bad = [5u64.to_be_bytes(), 3u64.to_be_bytes(), 0u64.to_be_bytes()].concat();
    engine.put(HELD, &bad).unwrap();
    assert!(matches!(
        Store::new(engine).recover(),
        Err(Error::Storage(StorageError::Corruption(_)))
    ));
}

#[test]
fn copies_of_the_same_log_keep_the_further_commit_point_in_either_order() {
    let a = host("a");
    let log = vec![entry(1, 1, &[("k", Some("v"))]), entry(2, 1, &[])];
    let at = |commit| {
        let store = store();
        claimed(&store, &a, 1);
        append(&store, &a, 1, 0, log.clone(), commit);
        store
    };
    let (behind, ahead) = (at(0), at(2));
    for order in [[&behind, &ahead], [&ahead, &behind]] {
        let target = store();
        for source in order {
            target.install(&records(source), true).unwrap();
        }
        assert_eq!(target.load(&actor()).unwrap().committed, 2);
        target.adopt(0, 0).unwrap();
        assert_eq!(
            target.dirty(0, 0).len(),
            1,
            "the committed entries wait for a flush"
        );
    }
    // A longer log is taken whole, with its own commit point.
    let longer = store();
    claimed(&longer, &a, 1);
    append(
        &longer,
        &a,
        1,
        0,
        [log.clone(), vec![entry(3, 1, &[])]].concat(),
        1,
    );
    let target = store();
    target.install(&records(&ahead), true).unwrap();
    target.install(&records(&longer), true).unwrap();
    assert_eq!(target.load(&actor()).unwrap().committed, 1);
}

/// S-17: a committed `MOVE` ends the claim like a release, and truncating it leaves `moved` set, so a claimant still
/// learns of it once the entry is gone from the log.
#[test]
fn a_truncated_move_stays_known_and_ends_the_claim() {
    let store = store();
    let (a, b) = (host("a"), host("b"));
    claimed(&store, &a, 1);
    let mut moving = entry(2, 1, &[]);
    moving.kind = EntryKind::Move as i32;
    let answer = append(
        &store,
        &a,
        1,
        0,
        vec![entry(1, 1, &[("k", Some("v"))]), moving],
        2,
    );
    assert_eq!(refused(&answer), Refusal::None);
    assert!(slot(&answer).released, "MOVE ends the claim like a release");
    assert_eq!(slot(&answer).moved, None, "not truncated yet");
    let first = Some(pb::LogEnd { seq: 2, epoch: 1 });
    assert_eq!(
        store.pending(&actor()).unwrap().unwrap().moved,
        first,
        "a flush carries the MOVE to S3"
    );
    let answer = store.truncate(&actor(), 2).unwrap().answer;
    assert_eq!(slot(&answer).moved, first);
    // A new claimant needs no lease wait and sees where the actor moved; its own MOVE keeps the first one.
    let (answer, _) = store.claim(&actor(), &b, 2, 0, |_| true).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
    assert_eq!(slot(&answer).moved, first);
    let mut again = entry(3, 2, &[]);
    again.kind = EntryKind::Move as i32;
    assert_eq!(
        refused(&append(&store, &b, 2, 1, vec![again], 3)),
        Refusal::None
    );
    let answer = store.truncate(&actor(), 3).unwrap().answer;
    assert_eq!(
        slot(&answer).moved,
        first,
        "the first MOVE stays the import point"
    );
}

/// S-17: an import starts the actor at the given point on a slot that holds nothing of it, is idempotent, and is
/// refused on a slot holding other state; a claim then goes above its epoch.
#[test]
fn an_import_starts_an_empty_slot_at_the_moved_point() {
    let store = store();
    assert_eq!(
        refused(&store.unimported(&actor()).unwrap().unwrap()),
        Refusal::NotImported
    );
    let answer = store.import(&actor(), 3, 7, Some(99), 1).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
    let s = slot(&answer);
    assert_eq!(
        (s.epoch, s.flushed, s.committed, s.flushed_epoch),
        (3, 7, 7, 3)
    );
    assert_eq!(s.end, Some(pb::LogEnd { seq: 7, epoch: 3 }));
    assert_eq!(s.flushed_alarm_ms, Some(99));
    assert!(store.unimported(&actor()).unwrap().is_none());
    // The same import again is a no-op; another one is refused.
    assert_eq!(
        refused(&store.import(&actor(), 3, 7, Some(99), 1).unwrap()),
        Refusal::None
    );
    assert_eq!(
        refused(&store.import(&actor(), 3, 8, None, 1).unwrap()),
        Refusal::StaleEpoch
    );
    let (answer, _) = store.claim(&actor(), &host("a"), 3, 1, |_| true).unwrap();
    assert_eq!(
        refused(&answer),
        Refusal::StaleEpoch,
        "a claim goes above the MOVE's epoch"
    );
    let (answer, _) = store.claim(&actor(), &host("a"), 4, 1, |_| true).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
    // After the claim moved the slot on, the same import is still acknowledged and changes nothing; the same point with
    // another alarm is refused.
    let answer = store.import(&actor(), 3, 7, Some(99), 1).unwrap();
    assert_eq!(refused(&answer), Refusal::None);
    assert_eq!(
        slot(&answer).epoch,
        4,
        "a repeated import leaves the live state"
    );
    assert_eq!(
        refused(&store.import(&actor(), 3, 7, None, 1).unwrap()),
        Refusal::StaleEpoch
    );

    let other = Store::new(Arc::new(MemoryEngine::new("volume")));
    claimed(&other, &host("a"), 1);
    assert_eq!(
        refused(&other.import(&actor(), 3, 7, None, 1).unwrap()),
        Refusal::StaleEpoch,
        "a slot holding a claim is never imported"
    );
}

/// S-17: a slot that skips to S3 past the first `MOVE` keeps it from S3's meta, so a later `MOVE` truncated there never
/// becomes its import point.
#[test]
fn a_skip_past_the_first_move_keeps_it() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 2);
    let first = Some(pb::LogEnd { seq: 4, epoch: 1 });
    let skipped = store
        .skip(&actor(), &a, 2, 5, 2, None, first)
        .unwrap()
        .unwrap();
    assert_eq!(slot(&skipped.answer).moved, first);
    let mut again = entry(6, 2, &[]);
    again.kind = EntryKind::Move as i32;
    assert_eq!(
        refused(&append(&store, &a, 2, 2, vec![again], 6)),
        Refusal::None
    );
    let answer = store.truncate(&actor(), 6).unwrap().answer;
    assert_eq!(slot(&answer).moved, first);
}

fn delete(seq: u64, epoch: u64) -> pb::Entry {
    pb::Entry {
        kind: EntryKind::Delete as i32,
        ..entry(seq, epoch, &[])
    }
}

/// Every refusal a tombstone gives: reservations, claims, appends, reads, scans and the recovery log.
fn assert_buried(store: &Store, holder: &pb::Holder) {
    let actor = actor();
    assert_eq!(
        refused(&store.reserve(&actor, 9, 0).unwrap()),
        Refusal::Deleted
    );
    let (answer, _) = store.claim(&actor, holder, 9, 0, |_| false).unwrap();
    assert_eq!(refused(&answer), Refusal::Deleted);
    assert_eq!(
        refused(&append(store, holder, 1, 0, vec![entry(1, 1, &[])], 0)),
        Refusal::Deleted
    );
    let (answer, values, _) = store.read(&actor, &[b"k".to_vec()], 0).unwrap();
    assert_eq!((refused(&answer), values.len()), (Refusal::Deleted, 0));
    assert_eq!(
        refused(&store.scan(&actor, b"", 10, 0).unwrap().0),
        Refusal::Deleted
    );
    assert_eq!(
        refused(&store.log(&actor, holder, 1, 0, 10).unwrap().0),
        Refusal::Deleted
    );
    assert_eq!(
        refused(&store.import(&actor, 1, 1, None, 0).unwrap()),
        Refusal::Deleted
    );
}

/// S-05b: a slot that learns its DELETE committed keeps only the claim as a tombstone, in one batch: no log, overlay,
/// alarm or flush state is left, and every later request is refused.
#[test]
fn a_committed_delete_leaves_only_a_tombstone_that_refuses_everything() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 1);
    let mut first = entry(1, 1, &[("k", Some("v"))]);
    first.alarm = Some(pb::Alarm { at_ms: Some(5000) });
    let answer = append(&store, &a, 1, 0, vec![first, delete(2, 1)], 1);
    assert_eq!(refused(&answer), Refusal::None);
    assert!(
        slot(&answer).released,
        "DELETE ends the claim like a release"
    );
    assert_eq!(slot(&answer).deleted, None, "not known committed yet");
    assert_eq!(read(&store, "k"), Some(b"v".to_vec()));
    assert!(
        store
            .engine
            .scan(&KeyRange::prefix(*b"alarm/"))
            .unwrap()
            .next()
            .is_some()
    );
    assert!(store.unflushed() > 0);

    // The commit point reaches the DELETE with the next append.
    let answer = append(&store, &a, 1, 1, vec![delete(2, 1)], 2);
    assert_eq!(refused(&answer), Refusal::Deleted);
    assert_eq!(slot(&answer).deleted, Some(pb::LogEnd { seq: 2, epoch: 1 }));
    let claim = store.load(&actor()).unwrap();
    assert_eq!(
        claim.deleted_at_ms, 1002,
        "the grace period starts at the DELETE's committed_at"
    );
    assert_eq!((claim.alarm_ms, claim.deleting), (None, None));
    let kept: Vec<_> = records(&store).into_iter().map(|r| r.key).collect();
    assert_eq!(kept, vec![claim_key(&actor())], "only the tombstone stays");
    assert!(
        store
            .engine
            .scan(&KeyRange::prefix(*b"alarm/"))
            .unwrap()
            .next()
            .is_none()
    );
    assert_eq!(store.unflushed(), 0);
    assert!(store.dirty(0, 0).is_empty());
    assert_eq!(store.pending(&actor()).unwrap(), None);
    assert_buried(&store, &a);
    // A restart finds no flush work in a tombstone.
    let engine = store.engine.clone();
    let restarted = Store::new(engine);
    restarted.recover().unwrap();
    assert!(restarted.dirty(0, 0).is_empty());
    assert_buried(&restarted, &host("b"));
}

/// A DELETE that never committed is an old epoch's tail like any other: a newer claim truncates it.
#[test]
fn an_uncommitted_delete_is_truncated_with_an_old_tail() {
    let store = store();
    let (a, b) = (host("a"), host("b"));
    claimed(&store, &a, 1);
    append(
        &store,
        &a,
        1,
        0,
        vec![entry(1, 1, &[("k", Some("v"))]), delete(2, 1)],
        1,
    );
    claimed(&store, &b, 2);
    let answer = append(&store, &b, 2, 1, vec![entry(2, 2, &[])], 2);
    assert_eq!(refused(&answer), Refusal::None);
    let claim = store.load(&actor()).unwrap();
    assert_eq!((claim.deleting, claim.deleted), (None, None));
    assert_eq!(read(&store, "k"), Some(b"v".to_vec()));
}

/// S-05b with S-06: a repair or a handoff copies a tombstone over any log, even from a repaired slot, keeping the higher
/// promise; and a copy whose commit point reaches the kept log's DELETE buries it.
#[test]
fn a_repair_copies_a_tombstone_and_a_copied_commit_point_can_bury() {
    let a = host("a");
    let buried = store();
    claimed(&buried, &a, 1);
    append(&buried, &a, 1, 0, vec![delete(1, 1)], 0);
    append(&buried, &a, 1, 0, vec![delete(1, 1)], 1);
    buried.mark_repaired(0, 0).unwrap();
    let marked = pb::ClaimRecord {
        repaired: true,
        ..buried.load(&actor()).unwrap()
    };
    buried
        .engine
        .put(&claim_key(&actor()), &marked.encode_to_vec())
        .unwrap();

    for repair in [true, false] {
        // A slot with a longer log and a higher promise of its own.
        let target = store();
        claimed(&target, &a, 3);
        append(
            &target,
            &a,
            3,
            0,
            vec![entry(1, 3, &[("k", Some("v"))]), entry(2, 3, &[])],
            2,
        );
        target.install(&records(&buried), repair).unwrap();
        let claim = target.load(&actor()).unwrap();
        assert_eq!(claim.deleted, Some(pb::LogEnd { seq: 1, epoch: 1 }));
        assert_eq!(claim.epoch, 3, "the higher promise stays");
        assert_eq!(records(&target).len(), 1);
        assert_eq!(target.unflushed(), 0);
        assert_buried(&target, &a);
    }

    // The same log on both slots: the copy knows the DELETE committed, the slot here does not.
    let knows = store();
    claimed(&knows, &a, 1);
    append(&knows, &a, 1, 0, vec![entry(1, 1, &[]), delete(2, 1)], 1);
    let target = store();
    claimed(&target, &a, 1);
    append(&target, &a, 1, 0, vec![entry(1, 1, &[]), delete(2, 1)], 1);
    let committed = pb::ClaimRecord {
        committed: 2,
        ..knows.load(&actor()).unwrap()
    };
    knows
        .engine
        .put(&claim_key(&actor()), &committed.encode_to_vec())
        .unwrap();
    target.install(&records(&knows), true).unwrap();
    assert_eq!(
        target.load(&actor()).unwrap().deleted,
        Some(pb::LogEnd { seq: 2, epoch: 1 })
    );
    assert_buried(&target, &a);
}
