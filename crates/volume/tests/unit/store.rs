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
    let (answer, values) = store.read(&actor(), &[key.as_bytes().to_vec()], 0).unwrap();
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
    // A slot refuses an older topology version once it has seen a newer one.
    assert_eq!(
        refused(&store.reserve(&actor, 4, 0).unwrap()),
        Refusal::StaleVersion
    );
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
        vec![alarm_key(&actor(), &store.load(&actor()).unwrap()).unwrap()]
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
    let log = store.entries(&actor(), 1, 3).unwrap();
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
fn reads_refuse_a_slot_behind_min_seq_and_a_flushed_miss_is_cold() {
    let store = store();
    let a = host("a");
    claimed(&store, &a, 1);
    append(&store, &a, 1, 0, vec![entry(1, 1, &[("k", Some("1"))])], 0);
    let (answer, _) = store.read(&actor(), &[b"k".to_vec()], 2).unwrap();
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
    assert!(matches!(
        store.read(&actor(), &[b"k".to_vec()], 0),
        Err(Error::Cold)
    ));
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
    let mut log = slots[from].entries(&actor(), 1, end).unwrap();
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
    for victim in 0..3 {
        for boundary in 0..=COMMITS {
            for after_write in [false, true] {
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
                // The holder dies too. The victim restarts with what it synced; the next holder may use it and
                // either survivor.
                for lost in 0..3 {
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
