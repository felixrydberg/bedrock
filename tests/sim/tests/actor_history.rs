//! #124 / S-16: every check of the actor workload's history can fire. Runs in the normal build.

use std::time::Duration;

use bedrock_sim::actors::{Answer, Call, History, Op, Violation};

fn call(op: Op, start: u64, end: u64, answer: Option<(u64, u64)>) -> Call {
    Call {
        actor: "a".into(),
        op,
        start: Duration::from_secs(start),
        end: Duration::from_secs(end),
        answer: answer.map(|(instance, value)| Answer { instance, value }),
    }
}

/// Instance 1 counts to 2, instance 2 takes over at 3 and counts on, with a failed call between.
fn clean() -> History {
    History {
        calls: vec![
            call(Op::Incr, 0, 1, Some((1, 1))),
            call(Op::Incr, 1, 2, Some((1, 2))),
            call(Op::Peek, 2, 3, Some((1, 2))),
            call(Op::Incr, 3, 9, None),
            call(Op::Get, 10, 11, Some((2, 3))),
            call(Op::Incr, 11, 12, Some((2, 4))),
        ],
        alarms: [("a".to_owned(), 5)].into(),
        fired: [("a".to_owned(), 5)].into(),
        ..History::default()
    }
}

#[test]
fn a_clean_history_passes_with_an_unknown_call_between() {
    assert_eq!(clean().check(), Ok(()));
}

#[test]
fn an_increment_counted_twice_or_backwards_is_a_lost_commit() {
    let mut history = clean();
    history.calls.push(call(Op::Incr, 13, 14, Some((2, 4))));
    assert!(matches!(history.check(), Err(Violation::LostCommit { .. })));
    let mut history = clean();
    history.calls.push(call(Op::Incr, 13, 14, Some((2, 3))));
    assert!(matches!(history.check(), Err(Violation::LostCommit { .. })));
    // Concurrent increments may land in either order.
    let mut history = clean();
    history.calls.push(call(Op::Incr, 11, 14, Some((2, 5))));
    history.calls.push(call(Op::Incr, 11, 13, Some((2, 6))));
    assert_eq!(history.check(), Ok(()));
}

#[test]
fn an_older_instance_answering_after_a_newer_one_is_two_serving() {
    let mut history = clean();
    history.calls.push(call(Op::Peek, 12, 13, Some((1, 2))));
    assert_eq!(
        history.check(),
        Err(Violation::TwoServing {
            actor: "a".into(),
            older: 1,
            newer: 2,
        })
    );
}

#[test]
fn a_read_below_an_acknowledged_increment_is_stale() {
    let mut history = clean();
    history.calls.push(call(Op::Get, 13, 14, Some((2, 3))));
    assert_eq!(
        history.check(),
        Err(Violation::StaleRead {
            actor: "a".into(),
            read: 3,
            acknowledged: 4,
        })
    );
}

#[test]
fn an_answer_after_an_acknowledged_deletion_is_served_deleted() {
    let mut history = clean();
    history.deleted.insert("a".into(), Duration::from_secs(12));
    assert_eq!(history.check(), Ok(()));
    history.calls.push(call(Op::Get, 13, 14, Some((3, 4))));
    assert_eq!(
        history.check(),
        Err(Violation::DeletedServed {
            actor: "a".into(),
            instance: 3,
        })
    );
}

#[test]
fn an_acknowledged_alarm_that_never_fired_is_lost() {
    let mut history = clean();
    history.alarms.insert(("a".into(), 7));
    assert_eq!(
        history.check(),
        Err(Violation::AlarmLost {
            actor: "a".into(),
            at_ms: 7,
        })
    );
}
