use super::*;
use crate::relays::{Interest, Subscriber};
use crate::transport::admin::Admin;

const LONG: Duration = Duration::from_secs(3600);

fn hub() -> Hub {
    Hub::relay(
        "relay".into(),
        2,
        Arc::new(Peers::new(&Admin::new())),
        Vec::new(),
        Vec::new(),
        None,
    )
}

/// `(dataset, namespace, range, from)`.
type Followed<'a> = (&'a str, &'a str, Option<(u64, u64)>, u64);

/// A subscriber with `follows` that follows `ranges` of a dataset.
fn subscribe(
    hub: &Hub,
    id: u64,
    follows: &[Followed],
    ranges: &[(&str, (u64, u64))],
) -> mpsc::Receiver<Result<pb::RelayFrame, Status>> {
    let (tx, rx) = mpsc::channel(1);
    let follows = follows.iter().map(|(dataset, namespace, range, from)| {
        let key = (((*dataset).to_owned(), (*namespace).to_owned()), *range);
        let interest = Interest {
            from: *from,
            acked: from - 1,
            generation: id,
            baseline: from - 1,
            confirming: false,
            target: None,
        };
        (key, interest)
    });
    hub.0.subscribers.lock().unwrap().insert(
        id,
        Subscriber {
            tx,
            bytes: Default::default(),
            events: Default::default(),
            closed: Arc::new(Watched::new(false)),
            follows: follows.collect(),
            ranges: ranges
                .iter()
                .map(|(dataset, range)| ((*dataset).to_owned(), *range))
                .collect(),
            changed: Arc::new(Watched::new(0)),
            member: None,
            pushed: BTreeMap::new(),
            ranges_acked: BTreeMap::new(),
            moved: None,
        },
    );
    rx
}

fn interests(follows: impl IntoIterator<Item = pb::Follow>) -> Interests {
    follows.into_iter().map(|f| (selector(&f), f)).collect()
}

fn namespace(dataset: &str, namespace: &str, from: u64) -> pb::Follow {
    pb::Follow {
        dataset: dataset.into(),
        namespace: namespace.into(),
        from,
        ..Default::default()
    }
}

fn dataset(dataset: &str, from: &[(&str, u64)]) -> pb::Follow {
    pb::Follow {
        dataset: dataset.into(),
        span: span(Some(WHOLE)),
        from_namespaces: from.iter().map(|(n, f)| ((*n).to_owned(), *f)).collect(),
        ..Default::default()
    }
}

fn key(dataset: &str, namespace: &str) -> Key {
    (dataset.into(), namespace.into())
}

#[test]
fn every_range_of_a_dataset_shares_as_one_whole_dataset_interest() {
    let hub = hub();
    let (low, high) = ((0, 1 << 32), (1 << 32, 0));
    let _a = subscribe(
        &hub,
        1,
        &[
            ("cdn", "pages", Some(low), 7),
            ("cdn", "workers", Some(low), 3),
            ("membership", "kv.c1", None, 5),
        ],
        &[("cdn", low)],
    );
    let _b = subscribe(
        &hub,
        2,
        &[
            ("cdn", "pages", Some(high), 4),
            ("cdn", "workers", Some(high), 9),
        ],
        &[("cdn", high)],
    );
    assert_eq!(
        hub.interests(),
        interests([
            dataset("cdn", &[("pages", 4), ("workers", 3)]),
            namespace("membership", "kv.c1", 5),
        ])
    );
}

#[test]
fn what_a_sibling_shares_is_followed_but_never_shared_onward() {
    let hub = hub();
    hub.shared(
        "b",
        Some(interests([
            dataset("cdn", &[("pages", 4)]),
            namespace("membership", "kv.c1", 5),
        ])),
        LONG,
    );
    let (namespaces, datasets) = hub.desired();
    assert_eq!(datasets, BTreeSet::from(["cdn".to_string()]));
    assert_eq!(namespaces[&key("cdn", "pages")], 4);
    assert_eq!(namespaces[&key("cdn", "workers")], 1, "a cursor is missing");
    assert_eq!(namespaces[&key("membership", "kv.c1")], 5);
    assert!(hub.interests().is_empty());

    hub.shared("b", Some(Interests::new()), LONG);
    assert!(
        hub.idle(),
        "a sibling that stops sharing keeps nothing alive"
    );
}

#[test]
fn a_shared_stream_starts_after_this_relays_history_or_at_the_siblings_cursor() {
    let hub = hub();
    hub.observe_head(&key("membership", "behind"), 3);
    hub.observe_head(&key("membership", "ahead"), 30);
    hub.shared(
        "b",
        Some(interests([
            namespace("membership", "behind", 10),
            namespace("membership", "ahead", 10),
        ])),
        LONG,
    );
    let (namespaces, _) = hub.desired();
    assert_eq!(namespaces[&key("membership", "behind")], 10);
    assert_eq!(namespaces[&key("membership", "ahead")], 31);
}

#[test]
fn a_subscribers_own_cursor_wins_over_a_siblings() {
    let hub = hub();
    let _own = subscribe(&hub, 1, &[("membership", "kv.c1", None, 2)], &[]);
    hub.shared(
        "b",
        Some(interests([namespace("membership", "kv.c1", 9)])),
        LONG,
    );
    assert_eq!(hub.desired().0[&key("membership", "kv.c1")], 2);
    let (followed, cold) = hub.sibling_interests();
    assert!(followed.is_empty() && cold.is_empty(), "already held");
}

#[test]
fn a_lost_siblings_interests_outlive_its_stream_for_the_grace_period() {
    let hub = hub();
    let shared = interests([namespace("membership", "kv.c1", 1)]);
    hub.shared("b", Some(shared.clone()), LONG);

    hub.shared("b", None, LONG);
    hub.expire_shared();
    assert!(!hub.idle(), "within grace");
    // It restarted without its subscribers, which are still failing over.
    hub.shared("b", Some(Interests::new()), LONG);
    hub.expire_shared();
    assert!(!hub.idle(), "a quick restart keeps the grace period");

    hub.shared("b", None, Duration::ZERO);
    hub.expire_shared();
    assert!(hub.idle(), "gone after grace");
}

#[test]
fn a_sibling_lost_before_it_synced_leaves_nothing() {
    let hub = hub();
    hub.shared("b", None, LONG);
    assert!(hub.idle());
    assert!(hub.0.shared.lock().unwrap()["b"].until.is_none());
}

#[test]
fn the_cap_keeps_range_follows_and_leaves_the_excess_cold() {
    let hub = hub();
    let many = (0..MAX_INTERESTS + 10).map(|i| namespace("membership", &format!("n{i:05}"), 1));
    hub.shared(
        "b",
        Some(interests(many.chain([dataset("quicksilver", &[])]))),
        LONG,
    );
    let (followed, cold) = hub.sibling_interests();
    assert_eq!(followed.len(), MAX_INTERESTS);
    assert_eq!(cold.len(), 11);
    assert_eq!(followed[0].dataset, "quicksilver", "range follows go first");
    assert_eq!(hub.desired().0.len(), MAX_INTERESTS);

    // The cap binds only sibling interests: an own subscriber follows a cold namespace, and frees a place.
    let excess = cold[0].namespace.clone();
    let _own = subscribe(&hub, 1, &[("membership", &excess, None, 1)], &[]);
    let (followed, cold) = hub.sibling_interests();
    assert_eq!((followed.len(), cold.len()), (MAX_INTERESTS, 10));
    assert!(hub.desired().0.contains_key(&key("membership", &excess)));
}

#[test]
fn siblings_must_share_a_selector_a_relay_would_serve() {
    let valid = namespace("membership", "kv.c1", 1);
    assert!(crate::relays::selectable(&valid));
    assert!(crate::relays::selectable(&dataset("cdn", &[])));
    for invalid in [
        namespace("kv", "customer", 1),
        namespace("membership", "a\0b", 1),
        namespace("membership", "", 1),
        pb::Follow {
            span: Some(pb::Span { start: 9, end: 3 }),
            ..valid.clone()
        },
    ] {
        assert!(!crate::relays::selectable(&invalid), "{invalid:?}");
    }
}
