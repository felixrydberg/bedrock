use super::*;
use crate::placement::{Owner, State as Range};

fn drain(scope: Scope) -> Drain {
    Drain {
        scope,
        datasets: Vec::new(),
        namespace: None,
        state: State::Draining,
        rate: 2,
    }
}

fn ranged(nodes: &[&[&str]]) -> Ranged {
    let directory: Directory = nodes
        .iter()
        .enumerate()
        .map(|(i, owners)| {
            let owners = owners
                .iter()
                .map(|n| Owner {
                    node: (*n).into(),
                    state: Range::Ready,
                })
                .collect();
            (i as u64, owners)
        })
        .collect();
    Ranged::new("quicksilver", move || Ok(directory.clone()))
}

#[test]
fn a_record_round_trips_and_corruption_is_an_error() {
    let record = drain(Scope::Node("n1".into()));
    assert_eq!(Drain::decode(&record.encode()), Ok(record));
    assert_eq!(Drain::key("d1"), "topology/drains/d1");
    assert!(matches!(Drain::decode(b"{"), Err(Error::Corrupt(_))));
    let mut zero = drain(Scope::Cell("c".into()));
    zero.rate = 0;
    assert!(matches!(
        Drain::decode(&zero.encode()),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn ranged_datasets_move_a_nodes_ranges_and_leave_a_cell_alone() {
    let hook = ranged(&[&["a", "b"], &["b"], &["c"]]);
    let node = |n: &str| Scope::Node(n.into());
    assert_eq!(hook.drain(&node("b"), None), Ok(Outcome::Moves(2)));
    assert_eq!(hook.drain(&node("zzz"), None), Ok(Outcome::NothingToMove));
    assert_eq!(
        hook.drain(&Scope::Cell("c1".into()), None),
        Ok(Outcome::NothingToMove)
    );
    assert!(matches!(
        hook.drain(&node("a"), Some("infrastructure")),
        Err(Error::Unsupported { .. })
    ));
}

#[test]
fn an_unreadable_directory_is_an_error_not_zero_units() {
    let hook = Ranged::new("cdn", || Err("scan failed".into()));
    assert_eq!(
        hook.drain(&Scope::Node("a".into()), None),
        Err(Error::Unavailable("scan failed".into()))
    );
}

#[test]
fn left_counts_selected_datasets_and_refuses_unsupported_ones() {
    let hooks: Vec<Arc<dyn Hook>> = vec![Arc::new(ranged(&[&["a"]])), Arc::new(Everywhere("kv"))];
    let mut d = drain(Scope::Node("a".into()));
    let units = left(&hooks, &d).unwrap();
    assert_eq!(
        units,
        BTreeMap::from([("kv".into(), 0), ("quicksilver".into(), 1)])
    );
    assert!(!complete(&units));
    d.datasets = vec!["kv".into()];
    assert!(complete(&left(&hooks, &d).unwrap()));
    // A dataset with no hook is never skipped, and a namespace drain reaches the datasets that cannot do it.
    d.datasets = vec!["cdn".into()];
    assert!(matches!(left(&hooks, &d), Err(Error::Unsupported { .. })));
    d.datasets.clear();
    d.namespace = Some("n".into());
    assert!(matches!(left(&hooks, &d), Err(Error::Unsupported { .. })));
}

#[test]
fn apply_marks_the_scope_draining_and_only_while_it_runs() {
    let member = |id: &str, cell: &str| pb::Member {
        node_id: id.into(),
        cell: cell.into(),
        ..Default::default()
    };
    let members = BTreeMap::from([
        ("a".to_string(), member("a", "c1")),
        ("b".to_string(), member("b", "c2")),
    ]);
    let status = |m: &BTreeMap<String, pb::Member>, id: &str| m[id].status;
    let draining = pb::member::Status::Draining as i32;
    let node = apply(&members, &[drain(Scope::Node("a".into()))], "cdn");
    assert_eq!((status(&node, "a"), status(&node, "b")), (draining, 0));
    let cell = apply(&members, &[drain(Scope::Cell("c2".into()))], "cdn");
    assert_eq!((status(&cell, "a"), status(&cell, "b")), (0, draining));
    let mut other = drain(Scope::Node("a".into()));
    other.datasets = vec!["auth".into()];
    assert_eq!(status(&apply(&members, &[other], "cdn"), "a"), 0);
    let mut done = drain(Scope::Node("a".into()));
    done.state = State::Drained;
    assert_eq!(status(&apply(&members, &[done], "cdn"), "a"), 0);
}

#[test]
fn the_smallest_rate_bounds_the_handoffs() {
    let mut slow = drain(Scope::Node("a".into()));
    slow.rate = 1;
    assert_eq!(moving(&[], 16), 16);
    assert_eq!(moving(&[drain(Scope::Node("a".into())), slow], 16), 1);
    assert_eq!(moving(&[drain(Scope::Node("a".into()))], 1), 1);
}
