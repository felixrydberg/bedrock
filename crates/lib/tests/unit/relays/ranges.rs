use super::*;
use crate::placement::Owner;

const HALF: u64 = 1 << 63;

fn spans(ranges: &[Range]) -> Vec<pb::Span> {
    ranges
        .iter()
        .map(|&(start, end)| pb::Span { start, end })
        .collect()
}

#[test]
fn a_pushed_set_keeps_splits_drops_and_adds() {
    let held = [(0, 100), (200, 300), (HALF, 0)];
    let plan = plan(
        &held,
        &[],
        &spans(&[(0, 100), (200, 250), (400, 500), (HALF, 0)]),
    )
    .unwrap();
    assert_eq!(plan.added, [(400, 500)]);
    assert_eq!(
        plan.changed,
        [Change {
            held: (200, 300),
            kept: vec![(200, 250)],
            dropped: vec![(250, 300)],
        }]
    );
    let revoked = super::plan(&held, &[], &spans(&[(0, 100), (200, 300)])).unwrap();
    assert_eq!(
        revoked.changed,
        [Change {
            held: (HALF, 0),
            kept: vec![],
            dropped: vec![(HALF, 0)],
        }]
    );
    // Keeping both ends of a held range drops the middle; the top end is 2⁶⁴.
    let middle = super::plan(
        &[(HALF, 0)],
        &[],
        &spans(&[(HALF, HALF + 10), (HALF + 20, 0)]),
    )
    .unwrap();
    assert_eq!(middle.changed[0].dropped, [(HALF + 10, HALF + 20)]);
}

#[test]
fn malformed_or_straddling_pushed_sets_are_protocol_errors() {
    for pushed in [vec![(5, 5)], vec![(0, 10), (5, 20)], vec![(50, 150)]] {
        assert_eq!(
            plan(&[(0, 100)], &[], &spans(&pushed)).unwrap_err().code(),
            tonic::Code::DataLoss,
            "{pushed:?}"
        );
    }
}

#[test]
fn adjacent_unsorted_ranges_keep_all_coverage_and_the_top_boundary() {
    let plan = plan(&[(0, 0)], &[], &spans(&[(HALF, 0), (0, HALF)])).unwrap();
    assert_eq!(plan.added, []);
    assert_eq!(
        plan.changed,
        [Change {
            held: (0, 0),
            kept: vec![(0, HALF), (HALF, 0)],
            dropped: vec![],
        }]
    );
    assert!(within((u64::MAX, 0), (HALF, 0)));
    assert!(!within((HALF - 1, HALF), (HALF, 0)));
}

fn owner(node: &str, state: State) -> Owner {
    Owner {
        node: node.into(),
        state,
    }
}

#[test]
fn a_drop_is_confirmed_only_when_the_view_dropped_the_node_and_another_owner_is_ready() {
    let directory = Directory::from([
        (
            0,
            vec![owner("a", State::Ready), owner("b", State::Leaving)],
        ),
        (100, vec![owner("a", State::Ready)]),
        (200, vec![owner("c", State::Populating)]),
    ]);
    assert_eq!(
        confirms(Some(&directory), "b", (100, 200), &BTreeSet::new()),
        Ok(())
    );
    assert!(
        confirms(Some(&directory), "b", (50, 150), &BTreeSet::new())
            .unwrap_err()
            .contains("still an owner"),
        "a leaving owner keeps its range"
    );
    assert!(
        confirms(Some(&directory), "b", (200, 0), &BTreeSet::new())
            .unwrap_err()
            .contains("no ready owner")
    );
    assert!(confirms(None, "b", (0, 0), &BTreeSet::new()).is_err());
    assert!(confirms(Some(&Directory::new()), "b", (0, 0), &BTreeSet::new()).is_err());
}

#[test]
fn a_revoke_checks_every_overlapping_directory_range_but_not_adjacent_ranges() {
    let directory = Directory::from([
        (0, vec![owner("replacement", State::Ready)]),
        (100, vec![owner("replacement", State::Ready)]),
        (200, vec![owner("old", State::Leaving)]),
    ]);
    assert_eq!(
        confirms(Some(&directory), "old", (0, 200), &BTreeSet::new()),
        Ok(())
    );
    assert!(
        confirms(Some(&directory), "old", (99, 201), &BTreeSet::new())
            .unwrap_err()
            .contains("still an owner")
    );
}

#[test]
fn an_unhealthy_ready_owner_does_not_confirm_a_revoke() {
    let directory = Directory::from([(0, vec![owner("sick", State::Ready)])]);
    let unhealthy = BTreeSet::from(["sick".to_string()]);
    assert!(
        confirms(Some(&directory), "b", (0, 0), &unhealthy)
            .unwrap_err()
            .contains("no ready owner")
    );
    assert_eq!(
        confirms(Some(&directory), "b", (0, 0), &BTreeSet::new()),
        Ok(())
    );
}

/// Found by the simulation (#25 / B-23): a stored range that could not be read when the session started has no consumer,
/// so nothing else would ever drop it.
#[test]
fn a_stored_range_without_a_consumer_is_dropped_where_the_set_no_longer_has_it() {
    let span = |start, end| pb::Span { start, end };
    let revoked = plan(&[], &[(0, 100)], &[span(200, 300)]).unwrap();
    assert_eq!(revoked.added, [(200, 300)]);
    assert_eq!(
        revoked.changed,
        [Change {
            held: (0, 100),
            kept: Vec::new(),
            dropped: vec![(0, 100)],
        }]
    );

    // Found by the simulation too: followed as new, the part had no marker until its first commit, so a crash left its data
    // unknown. Kept, it is marked in the batch that retires the whole.
    let split = plan(&[], &[(0, 100)], &[span(0, 40)]).unwrap();
    assert!(split.added.is_empty());
    assert_eq!(split.changed[0].kept, [(0, 40)]);
    assert_eq!(split.changed[0].dropped, [(40, 100)]);

    let kept = plan(&[], &[(0, 100)], &[span(0, 100)]).unwrap();
    assert_eq!(kept.added, [(0, 100)]);
    assert!(kept.changed.is_empty());
    assert!(plan(&[], &[(0, 100)], &[span(50, 150)]).is_err());
}

/// Found by the simulation (#25 / B-23): a refused drop left the stored whole beside a part already followed, and treating
/// that as held forgot the rest of it.
#[test]
fn a_stored_range_around_a_held_part_still_drops_the_rest() {
    let span = |start, end| pb::Span { start, end };
    let plan = plan(&[(0, 40)], &[(0, 40), (0, 100)], &[span(0, 40)]).unwrap();
    assert!(plan.added.is_empty());
    assert_eq!(
        plan.changed,
        [Change {
            held: (0, 100),
            kept: vec![(0, 40)],
            dropped: vec![(40, 100)],
        }]
    );
}

/// An attempt that committed but reported failure leaves parts stored inside the range still held; retrying it covers them.
#[test]
fn a_stored_part_of_a_held_range_changes_nothing() {
    let span = |start, end| pb::Span { start, end };
    let plan = plan(&[(0, 100)], &[(0, 40)], &[span(0, 100)]).unwrap();
    assert_eq!(plan, Plan::default());
}
