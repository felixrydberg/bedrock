use super::*;
use crate::admin::Admin;
use crate::relays::{Interest, RECOVERY_WAIT, Subscriber};

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
            follow: 0,
            baseline: from - 1,
            confirming: false,
            target: None,
            woken: Default::default(),
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
fn siblings_sharing_a_selector_keep_the_lowest_cursor() {
    let hub = hub();
    let partial = |from| pb::Follow {
        span: span(Some((0, 1 << 32))),
        ..dataset("auth", &[("keys", from)])
    };
    hub.shared(
        "a",
        Some(interests([
            namespace("membership", "kv.c1", 9),
            dataset("cdn", &[("pages", 8), ("workers", 3)]),
            partial(7),
        ])),
        LONG,
    );
    hub.shared(
        "b",
        Some(interests([
            namespace("membership", "kv.c1", 5),
            dataset("cdn", &[("pages", 4)]),
            partial(2),
        ])),
        LONG,
    );
    hub.shared(
        "c",
        Some(interests([dataset(
            "quicksilver",
            &[("infrastructure", 6)],
        )])),
        LONG,
    );
    hub.shared(
        "d",
        Some(interests([dataset(
            "quicksilver",
            &[("infrastructure", 2), ("unseen", 6)],
        )])),
        LONG,
    );
    let (followed, _) = hub.sibling_interests();
    let cursors = |dataset: &str| {
        let follow = followed.iter().find(|f| f.dataset == dataset).unwrap();
        follow.from_namespaces.clone()
    };
    assert_eq!(
        cursors("cdn"),
        BTreeMap::from([("pages".into(), 4), ("workers".into(), 1)]),
        "a sibling without a namespace's cursor needs it from the start"
    );
    assert_eq!(
        cursors("quicksilver"),
        BTreeMap::from([("infrastructure".into(), 2), ("unseen".into(), 1)])
    );
    assert_eq!(
        cursors("auth"),
        BTreeMap::from([("keys".into(), 7)]),
        "partial ranges keep the first follow"
    );
    assert_eq!(hub.desired().0[&key("membership", "kv.c1")], 5);
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

/// Found by the simulation (#25 / B-23): a recovery source was lost after a bootstrap reached the subscriber but before its
/// confirmation. The next source was asked for a plain catch-up, which never confirms, so the subscriber waited forever.
#[test]
fn a_bootstrap_still_unconfirmed_makes_the_next_source_a_full_recovery() {
    let hub = hub();
    let _own = subscribe(&hub, 1, &[("auth", "keys", None, 13)], &[]);
    let stream = (key("auth", "keys"), None);
    assert_eq!(hub.recovery_start(1, &stream, 1, false), Some((13, false)));
    assert_eq!(hub.recovery_start(1, &stream, 1, true), Some((0, true)));

    let mut subscribers = hub.0.subscribers.lock().unwrap();
    let interest = subscribers.get_mut(&1).unwrap().follows.get_mut(&stream);
    interest.unwrap().confirming = true;
    drop(subscribers);
    assert_eq!(hub.recovery_start(1, &stream, 1, false), Some((0, true)));
    assert_eq!(
        hub.recovery_start(1, &stream, 2, false),
        None,
        "a newer follow"
    );
}

/// Found by the simulation (#25 / B-23): a refused recovery came back only on the subscriber's 10 second retry, so a node with
/// many ranges took minutes to recover, however quick each recovery was.
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_recovery_waits_for_a_slot_to_free() {
    let hub = hub();
    let held = hub.0.recoveries.clone().acquire_many_owned(4).await;
    let waiting = {
        let hub = hub.clone();
        tokio::spawn(async move { hub.recovery_permit().await.map(drop) })
    };
    tokio::time::sleep(RECOVERY_WAIT / 2).await;
    assert!(!waiting.is_finished());
    drop(held);
    waiting.await.unwrap().unwrap();
}

/// Found by the simulation (#25 / B-23): a recovery took a slot on every relay it passed through, so sibling relays ran about
/// as many recoveries as one of them, and waited on each other's slots until they timed out.
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn only_the_relay_a_recovery_entered_needs_a_slot() {
    let hub = hub();
    let stream = (key("cdn", "pages"), None);
    let _held = hub.0.recoveries.clone().acquire_many_owned(4).await;
    let entered = hub.proxy(1, &stream, 1, &[], false).await.unwrap_err();
    assert_eq!(entered.code(), tonic::Code::ResourceExhausted);
    let visited = ["sibling".to_owned()];
    let forwarded = hub.proxy(1, &stream, 1, &visited, false).await.unwrap_err();
    assert_eq!(forwarded.code(), tonic::Code::Unavailable);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn a_recovery_is_refused_when_no_slot_frees_in_time() {
    let hub = hub();
    let _held = hub.0.recoveries.clone().acquire_many_owned(4).await;
    let refused = hub.recovery_permit().await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
async fn sibling_handshake_requires_the_same_cell_authority_before_sharing_members() {
    use crate::membership::control::{Config, Control};
    use crate::replication::pb::siblings_server::Siblings as _;
    use crate::storage::memory::MemoryEngine;
    use tokio_stream::StreamExt;

    let peers = Arc::new(Peers::new(&Admin::new()));
    let (control, running) = Control::start(
        Arc::new(MemoryEngine::new("control")),
        peers.clone(),
        Config {
            region: "eu".into(),
            dc: "sto".into(),
            cell: "sto-1".into(),
            me: "a:7000".into(),
            voters: vec!["a:7000".into(), "b:7000".into(), "c:7000".into()],
            voter_ids: ["a", "b", "c"]
                .into_iter()
                .map(|id| (format!("{id}:7000"), id.into()))
                .collect(),
            clock: || 1000,
            operator_token: None,
            bootstrap: true,
        },
    )
    .unwrap();
    running.abort();
    let identity = control.identity();
    let cell = Hub::relay("relay".into(), 1, peers, Vec::new(), Vec::new(), None)
        .with_cell("sto-1".into())
        .with_control(control);
    let server = Server(cell);
    let mut mismatches = vec![None];
    for field in ["region", "dc", "cell", "voters", "voter_ids"] {
        let mut peer = identity.clone();
        match field {
            "region" => peer.region = "other".into(),
            "dc" => peer.dc = "other".into(),
            "cell" => peer.cell = "other".into(),
            "voters" => peer.voters[2] = "replacement:7000".into(),
            _ => {
                peer.voter_ids.insert("b:7000".into(), "other".into());
            }
        }
        mismatches.push(Some(peer));
    }
    for control in mismatches {
        let error = server
            .watch(Request::new(pb::SiblingWatch { control }))
            .await
            .expect_err("mismatched peer must be refused before snapshot");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    let mut stream = server
        .watch(Request::new(pb::SiblingWatch {
            control: Some(identity.clone()),
        }))
        .await
        .unwrap()
        .into_inner();
    let snapshot = stream.next().await.unwrap().unwrap();
    assert!(snapshot.synced);
    assert_eq!(snapshot.control, Some(identity.clone()));
    drop(stream);

    let upper = Server(hub());
    let error = upper
        .watch(Request::new(pb::SiblingWatch {
            control: Some(identity),
        }))
        .await
        .expect_err("authority peer cannot join an upper-tier sibling stream");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    let mut stream = upper
        .watch(Request::new(pb::SiblingWatch { control: None }))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.next().await.unwrap().unwrap().control.is_none());
}
