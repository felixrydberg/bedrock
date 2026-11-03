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
    let revoked = super::plan(&held, &spans(&[(0, 100), (200, 300)])).unwrap();
    assert_eq!(
        revoked.changed,
        [Change {
            held: (HALF, 0),
            kept: vec![],
            dropped: vec![(HALF, 0)],
        }]
    );
    // Keeping both ends of a held range drops the middle; the top end is 2⁶⁴.
    let middle = super::plan(&[(HALF, 0)], &spans(&[(HALF, HALF + 10), (HALF + 20, 0)])).unwrap();
    assert_eq!(middle.changed[0].dropped, [(HALF + 10, HALF + 20)]);
}

#[test]
fn malformed_or_straddling_pushed_sets_are_protocol_errors() {
    for pushed in [vec![(5, 5)], vec![(0, 10), (5, 20)], vec![(50, 150)]] {
        assert_eq!(
            plan(&[(0, 100)], &spans(&pushed)).unwrap_err().code(),
            tonic::Code::DataLoss,
            "{pushed:?}"
        );
    }
}

#[test]
fn adjacent_unsorted_ranges_keep_all_coverage_and_the_top_boundary() {
    let plan = plan(&[(0, 0)], &spans(&[(HALF, 0), (0, HALF)])).unwrap();
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
    assert_eq!(confirms(Some(&directory), "b", (100, 200)), Ok(()));
    assert!(
        confirms(Some(&directory), "b", (50, 150))
            .unwrap_err()
            .contains("still an owner"),
        "a leaving owner keeps its range"
    );
    assert!(
        confirms(Some(&directory), "b", (200, 0))
            .unwrap_err()
            .contains("no ready owner")
    );
    assert!(confirms(None, "b", (0, 0)).is_err());
    assert!(confirms(Some(&Directory::new()), "b", (0, 0)).is_err());
}

#[test]
fn a_revoke_checks_every_overlapping_directory_range_but_not_adjacent_ranges() {
    let directory = Directory::from([
        (0, vec![owner("replacement", State::Ready)]),
        (100, vec![owner("replacement", State::Ready)]),
        (200, vec![owner("old", State::Leaving)]),
    ]);
    assert_eq!(confirms(Some(&directory), "old", (0, 200)), Ok(()));
    assert!(
        confirms(Some(&directory), "old", (99, 201))
            .unwrap_err()
            .contains("still an owner")
    );
}
