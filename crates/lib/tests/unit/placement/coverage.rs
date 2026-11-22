use super::*;
use crate::placement::{Owner, State};

fn owner(node: &str, state: State) -> Owner {
    Owner {
        node: node.into(),
        state,
    }
}

const HALF: u64 = 1 << 63;

#[test]
fn counts_only_accepted_holders_by_hash_space() {
    let directory = Directory::from([
        (
            0,
            vec![
                owner("a", State::Ready),
                owner("b", State::Leaving),
                owner("c", State::Populating),
            ],
        ),
        (
            HALF,
            vec![owner("a", State::Ready), owner("c", State::Populating)],
        ),
    ]);
    let all = coverage(&directory, 2, |_| true);
    assert_eq!(all.complete, SPACE);
    assert_eq!(all.redundant, SPACE / 2);
    assert!(all.degraded());

    // A disconnected or unhealthy `a` leaves the second half without a holder.
    let without_a = coverage(&directory, 2, |node| node != "a");
    assert_eq!(without_a.complete, SPACE / 2);
    assert_eq!(without_a.redundant, 0);

    let one = coverage(&directory, 1, |_| true);
    assert_eq!(one.redundant, SPACE);
    assert!(!one.degraded());
}

#[test]
fn an_empty_or_gapped_directory_is_missing_space() {
    assert_eq!(coverage(&Directory::new(), 1, |_| true).complete, 0);
    let gapped = Directory::from([(HALF, vec![owner("a", State::Ready)])]);
    assert_eq!(coverage(&gapped, 1, |_| true).complete, SPACE / 2);
}
