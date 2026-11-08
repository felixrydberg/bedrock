//! #25 / B-23: every invariant the simulation checks must be able to fire. Runs in the normal build.

use std::collections::{BTreeMap, BTreeSet};

use bedrock_sim::history::{
    History, KEPT, OwnerState, Placement, Stream, Violation, check_coverage, check_handoff,
    check_held, check_routing,
};

fn stream() -> Stream {
    ("cdn".into(), "pages".into())
}

/// set a=1 at 1, set b=2 at 2, set a=3 at 3, delete b at 4.
fn history() -> History {
    let mut history = History::default();
    let s = stream();
    history.acknowledged(&s, 1, "w1", b"a", Some(b"1")).unwrap();
    history.acknowledged(&s, 2, "w2", b"b", Some(b"2")).unwrap();
    history.acknowledged(&s, 3, "w3", b"a", Some(b"3")).unwrap();
    history.acknowledged(&s, 4, "w4", b"b", None).unwrap();
    history
}

fn copy(entries: &[(&str, &str)]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    entries
        .iter()
        .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect()
}

fn invariant<T: std::fmt::Debug>(result: Result<T, Violation>) -> &'static str {
    result.unwrap_err().invariant
}

fn all(_: &[u8]) -> bool {
    true
}

#[test]
fn a_faithful_copy_passes_at_every_revision() {
    let history = history();
    let s = stream();
    for (revision, entries) in [
        (0, vec![]),
        (1, vec![("a", "1")]),
        (2, vec![("a", "1"), ("b", "2")]),
        (3, vec![("a", "3"), ("b", "2")]),
        (4, vec![("a", "3")]),
    ] {
        history
            .check_copy(&s, revision, all, &copy(&entries))
            .unwrap();
    }
    history.gap_free().unwrap();
    history.check_head(&s, 4).unwrap();
}

#[test]
fn selection_restricts_the_copy() {
    history()
        .check_copy(&stream(), 4, |key| key != b"a", &copy(&[]))
        .unwrap();
}

#[test]
fn the_same_answer_may_be_repeated() {
    let mut history = history();
    history
        .acknowledged(&stream(), 3, "w3", b"a", Some(b"3"))
        .unwrap();
}

#[test]
fn no_lost_ack_fires_on_a_reused_sequence() {
    // A writer that lost an acknowledged write gives its sequence to the next one.
    let result = history().acknowledged(&stream(), 3, "w9", b"a", Some(b"9"));
    assert_eq!(invariant(result), "NoLostAck");
}

#[test]
fn no_lost_ack_fires_when_the_durable_head_is_behind() {
    assert_eq!(invariant(history().check_head(&stream(), 3)), "NoLostAck");
}

#[test]
fn no_lost_ack_fires_on_a_copy_past_the_head() {
    let result = history().check_copy(&stream(), 5, all, &copy(&[("a", "3")]));
    assert_eq!(invariant(result), "NoLostAck");
}

#[test]
fn retry_protection_fires_when_a_retry_applies_twice() {
    let result = history().acknowledged(&stream(), 5, "w1", b"a", Some(b"1"));
    assert_eq!(invariant(result), "RetryProtection");
}

#[test]
fn no_resurrection_fires_on_a_deleted_key() {
    let result = history().check_copy(&stream(), 4, all, &copy(&[("a", "3"), ("b", "2")]));
    assert_eq!(invariant(result), "NoResurrection");
}

#[test]
fn no_stale_overwrite_fires_on_an_older_value() {
    let result = history().check_copy(&stream(), 3, all, &copy(&[("a", "1"), ("b", "2")]));
    assert_eq!(invariant(result), "NoStaleOverwrite");
}

#[test]
fn gap_free_apply_fires_on_missing_data() {
    let result = history().check_copy(&stream(), 3, all, &copy(&[("a", "3")]));
    assert_eq!(invariant(result), "GapFreeApply");
}

#[test]
fn gap_free_apply_fires_on_data_ahead_of_progress() {
    let result = history().check_copy(&stream(), 2, all, &copy(&[("a", "3"), ("b", "2")]));
    assert_eq!(invariant(result), "GapFreeApply");
}

#[test]
fn gap_free_apply_fires_on_a_missing_sequence() {
    let mut history = history();
    history
        .acknowledged(&stream(), 6, "w6", b"a", Some(b"6"))
        .unwrap();
    assert_eq!(invariant(history.gap_free()), "GapFreeApply");
}

/// [`history`], then `extra` more writes past what it keeps, all of key `c`.
fn outgrown(extra: u64) -> History {
    let mut history = history();
    let s = stream();
    for sequence in 5..=4 + KEPT + extra {
        let id = format!("w{sequence}");
        let value = sequence.to_string();
        let value = Some(value.as_bytes());
        history
            .acknowledged(&s, sequence, &id, b"c", value)
            .unwrap();
    }
    history
}

#[test]
fn a_forgotten_write_still_decides_its_keys_state() {
    let history = outgrown(10);
    let s = stream();
    let head = history.head(&s);
    let held = copy(&[("a", "3"), ("c", &head.to_string())]);
    history.check_copy(&s, head, all, &held).unwrap();
    history.gap_free().unwrap();
    history
        .check_read(&s, head, head, b"a", Some(b"3"))
        .unwrap();

    let stale = copy(&[("a", "1"), ("c", &head.to_string())]);
    let result = history.check_copy(&s, head, all, &stale);
    assert_eq!(invariant(result), "NoStaleOverwrite");
    let resurrected = copy(&[("a", "3"), ("b", "2"), ("c", &head.to_string())]);
    let result = history.check_copy(&s, head, all, &resurrected);
    assert_eq!(invariant(result), "NoResurrection");
}

#[test]
fn the_oldest_kept_revision_still_checks_and_older_ones_fail() {
    let history = outgrown(10);
    let s = stream();
    let oldest = history.head(&s) - KEPT;
    let held = copy(&[("a", "3"), ("c", &oldest.to_string())]);
    history.check_copy(&s, oldest, all, &held).unwrap();

    let result = history.check_copy(&s, oldest - 1, all, &held);
    assert_eq!(invariant(result), "HistoryKept");
    let result = history.check_read(&s, 1, oldest - 1, b"a", Some(b"3"));
    assert_eq!(invariant(result), "HistoryKept");
}

#[test]
fn an_answer_at_a_forgotten_sequence_fails_but_a_kept_one_may_repeat() {
    let mut history = outgrown(10);
    let s = stream();
    let oldest = history.head(&s) - KEPT;
    let result = history.acknowledged(&s, oldest, "new", b"a", Some(b"9"));
    assert_eq!(invariant(result), "HistoryKept");

    let kept = oldest + 1;
    let value = kept.to_string();
    let id = format!("w{kept}");
    history
        .acknowledged(&s, kept, &id, b"c", Some(value.as_bytes()))
        .unwrap();
    let result = history.acknowledged(&s, kept, "new", b"c", Some(b"9"));
    assert_eq!(invariant(result), "NoLostAck");
}

#[test]
fn a_retry_within_what_is_kept_cannot_apply_twice() {
    let mut history = outgrown(10);
    let s = stream();
    let kept = history.head(&s) - KEPT + 1;
    let id = format!("w{kept}");
    let result = history.acknowledged(&s, kept + 5, &id, b"c", Some(b"9"));
    assert_eq!(invariant(result), "RetryProtection");
}

#[test]
fn only_the_contiguous_prefix_counts_as_known() {
    let mut history = history();
    let s = stream();
    assert_eq!(history.known_through(&s), 4);
    history.acknowledged(&s, 6, "w6", b"a", Some(b"6")).unwrap();
    assert_eq!(history.known_through(&s), 4);
    history.acknowledged(&s, 5, "w5", b"a", Some(b"5")).unwrap();
    assert_eq!(history.known_through(&s), 6);
}

#[test]
fn min_revision_reads_prove_values_and_absence() {
    let history = history();
    let s = stream();
    history.check_read(&s, 3, 4, b"a", Some(b"3")).unwrap();
    history.check_read(&s, 4, 4, b"b", None).unwrap();
    history.check_read(&s, 0, 4, b"never", None).unwrap();
}

#[test]
fn min_revision_reads_fire_below_the_minimum() {
    let result = history().check_read(&stream(), 4, 3, b"a", Some(b"3"));
    assert_eq!(invariant(result), "MinRevisionReads");
}

#[test]
fn min_revision_reads_fire_on_wrong_absence_and_wrong_value() {
    let history = history();
    let s = stream();
    assert_eq!(
        invariant(history.check_read(&s, 3, 3, b"a", None)),
        "MinRevisionReads"
    );
    assert_eq!(
        invariant(history.check_read(&s, 4, 4, b"b", Some(b"2"))),
        "MinRevisionReads"
    );
}

const HALF: u64 = 1 << 63;

fn owners(list: &[(&str, OwnerState)]) -> Vec<(String, OwnerState)> {
    list.iter()
        .map(|(node, state)| ((*node).to_owned(), *state))
        .collect()
}

fn placement(low: &[(&str, OwnerState)], high: &[(&str, OwnerState)]) -> Placement {
    vec![((0, HALF), owners(low)), ((HALF, 0), owners(high))]
}

fn members(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

use OwnerState::{Leaving, Populating, Ready};

#[test]
fn coverage_passes_a_full_tiling_and_an_empty_directory() {
    check_coverage(&placement(&[("a", Ready)], &[("a", Ready)])).unwrap();
    check_coverage(&vec![((0, 0), owners(&[("a", Ready)]))]).unwrap();
    check_coverage(&Vec::new()).unwrap();
}

#[test]
fn coverage_fires_on_a_gap_an_overlap_and_a_short_end() {
    let a = owners(&[("a", Ready)]);
    for placement in [
        vec![((0, 10), a.clone()), ((20, 0), a.clone())],
        vec![((0, 20), a.clone()), ((10, 0), a.clone())],
        vec![((5, 0), a.clone())],
        vec![((0, 10), a.clone())],
    ] {
        assert_eq!(invariant(check_coverage(&placement)), "DirectoryCoverage");
    }
}

#[test]
fn handoff_passes_while_a_leaving_owner_still_serves() {
    let before = placement(&[("a", Ready)], &[("a", Ready)]);
    let after = placement(&[("a", Leaving), ("b", Populating)], &[("a", Ready)]);
    check_handoff(&before, &after, &members(&["a", "b"])).unwrap();
}

#[test]
fn losing_the_only_owner_to_a_crash_is_not_a_handoff() {
    let before = placement(&[("a", Ready)], &[("b", Ready)]);
    let after = placement(&[("b", Populating)], &[("b", Ready)]);
    check_handoff(&before, &after, &members(&["b"])).unwrap();
}

#[test]
fn no_drop_before_ready_fires_when_only_populating_owners_remain() {
    let before = placement(&[("a", Ready)], &[("a", Ready)]);
    let after = placement(&[("b", Populating)], &[("a", Ready)]);
    assert_eq!(
        invariant(check_handoff(&before, &after, &members(&["a", "b"]))),
        "NoDropBeforeReady"
    );
}

#[test]
fn no_drop_before_ready_checks_every_part_of_a_split() {
    let before = vec![((0, 0), owners(&[("a", Ready)]))];
    let healthy = members(&["a", "b"]);
    for split in [1, HALF, u64::MAX] {
        let mut after = vec![
            ((0, split), owners(&[("a", Ready)])),
            ((split, 0), owners(&[("b", Populating)])),
        ];
        assert_eq!(
            invariant(check_handoff(&before, &after, &healthy)),
            "NoDropBeforeReady"
        );

        after[1].1.push(("a".into(), Leaving));
        check_handoff(&before, &after, &healthy).unwrap();
    }
}

#[test]
fn owned_or_leaving_passes_for_any_listed_state() {
    let placement = placement(&[("a", Leaving)], &[("a", Populating)]);
    check_held(&placement, "a", [1, HALF + 1]).unwrap();
}

#[test]
fn owned_or_leaving_fires_outside_the_nodes_ranges() {
    let placement = placement(&[("a", Ready)], &[("b", Ready)]);
    assert_eq!(
        invariant(check_held(&placement, "a", [HALF + 1])),
        "OwnedOrLeaving"
    );
}

#[test]
fn routing_passes_for_ranges_an_unhealthy_member_already_had() {
    let unhealthy = BTreeSet::from(["a".to_owned()]);
    let before = placement(&[("a", Ready)], &[("b", Ready)]);
    check_routing(&before, &before, &unhealthy).unwrap();
}

#[test]
fn no_unhealthy_routing_fires_when_an_unhealthy_member_gains_a_range() {
    let unhealthy = BTreeSet::from(["a".to_owned()]);
    let before = placement(&[("a", Ready)], &[("b", Ready)]);
    let after = placement(&[("a", Ready)], &[("a", Populating), ("b", Ready)]);
    assert_eq!(
        invariant(check_routing(&before, &after, &unhealthy)),
        "NoUnhealthyRouting"
    );
}

#[test]
fn no_unhealthy_routing_checks_every_part_of_a_merge() {
    let unhealthy = members(&["a"]);
    for split in [1, HALF, u64::MAX] {
        let before = vec![
            ((0, split), owners(&[("a", Ready)])),
            ((split, 0), owners(&[("b", Ready)])),
        ];
        let after = vec![((0, 0), owners(&[("a", Populating), ("b", Ready)]))];
        assert_eq!(
            invariant(check_routing(&before, &after, &unhealthy)),
            "NoUnhealthyRouting"
        );
    }
}
