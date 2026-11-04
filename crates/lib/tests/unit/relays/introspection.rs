use super::*;
use crate::relays::Subscriber;
use crate::relays::replay::Replay;
use crate::replication::log::Watched;
use crate::replication::{Event, Operation};
use crate::storage::memory::MemoryEngine;
use crate::transport::Peers;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;

const COMMITTED: u64 = 1_700_000_000_000;

fn clock() -> u64 {
    COMMITTED + 250
}

fn relay() -> Hub {
    let replay = Replay::open(Arc::new(MemoryEngine::new("replay")), Default::default()).unwrap();
    Hub::relay(
        "cell".into(),
        1,
        Arc::new(Peers::new(&Admin::new())),
        Vec::new(),
        Vec::new(),
        Some(Arc::new(replay)),
    )
    .with_clock(clock)
}

fn event(namespace: &str, sequence: u64) -> Event {
    Event {
        dataset: "auth".into(),
        namespace: namespace.into(),
        stream: "auth-0".into(),
        sequence,
        epoch: 1,
        committed_at: COMMITTED,
        key: b"key".to_vec(),
        operation: Operation::Set(vec![7; 32]),
    }
}

/// Series, not descriptor lines, in the `/metrics` body.
fn series(admin: &Admin) -> usize {
    admin
        .render()
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .count()
}

fn top(admin: &Admin, name: &str) -> Vec<String> {
    admin
        .render()
        .lines()
        .filter(|line| line.starts_with(&format!("bedrock_{name}{{")))
        .map(str::to_owned)
        .collect()
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn series_count_does_not_grow_with_namespaces() {
    let hub = relay();
    let admin = Admin::new();
    register(&admin, &hub, "cell", "c1", None);
    let publish = |from: usize, to: usize| {
        for n in from..to {
            let key = ("auth".to_owned(), format!("ns-{n:06}"));
            hub.publish(event(&key.1, 1));
            hub.0.stats.upstream_head(&key, 1 + (n % 7) as u64);
        }
    };
    publish(0, 100);
    for sequence in 2..=5 {
        hub.publish(event("hot", sequence));
    }
    tokio::time::sleep(RATE_WINDOW + Duration::from_secs(1)).await;
    let small = series(&admin);
    let rates = top(&admin, "relay_top_namespace_events_per_second");
    assert_eq!(rates.len(), TOP);
    assert!(rates[0].contains("namespace=\"hot\""), "{rates:?}");
    assert_eq!(top(&admin, "relay_top_namespace_lag_revisions").len(), TOP);

    publish(100, 100_000);
    tokio::time::sleep(RATE_WINDOW + Duration::from_secs(1)).await;
    assert_eq!(series(&admin), small, "{}", admin.render());
    let lag = top(&admin, "relay_lag_seconds_count");
    assert_eq!(lag.len(), 1);
    assert!(lag[0].ends_with(" 100004"), "{lag:?}");
    let sum = top(&admin, "relay_lag_seconds_sum");
    assert!(
        sum[0].contains("tier=\"cell\"") && sum[0].contains("cell=\"c1\""),
        "{sum:?}"
    );
}

/// A subscriber whose queue tonic never drains and that never acknowledges.
fn stalled(hub: &Hub, id: u64) -> mpsc::Receiver<Result<pb::RelayFrame, tonic::Status>> {
    let (tx, rx) = mpsc::channel(4096);
    hub.0.subscribers.lock().unwrap().insert(
        id,
        Subscriber {
            tx,
            bytes: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(AtomicUsize::new(0)),
            closed: Arc::new(Watched::new(false)),
            follows: BTreeMap::new(),
            ranges: Default::default(),
            changed: Arc::new(Watched::new(0)),
            member: None,
            pushed: BTreeMap::new(),
            ranges_acked: BTreeMap::new(),
            moved: None,
        },
    );
    rx
}

async fn subscriber_state(hub: &Hub, queued: u64) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let view = subscribers(hub, &Query::parse("dataset=auth"));
            if view["items"][0]["queued_events"].as_u64() >= Some(queued) {
                return view["items"][0].clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("deliveries reach the stalled queue")
}

#[tokio::test]
async fn stalled_subscriber_shows_rising_lag_and_queue_depth() {
    let hub = relay();
    let _queue = stalled(&hub, 1);
    for sequence in 1..=10 {
        hub.publish(event("users", sequence));
    }
    let follow = pb::Follow {
        dataset: "auth".into(),
        namespace: "users".into(),
        from: 1,
        span: None,
        from_namespaces: Default::default(),
    };
    hub.follow(1, follow, Vec::new(), false).await;
    let before = subscriber_state(&hub, 10).await;
    for sequence in 11..=30 {
        hub.publish(event("users", sequence));
    }
    hub.changed();
    let after = subscriber_state(&hub, 30).await;
    assert_eq!(before["follows"][0]["lag"], 10);
    assert_eq!(after["follows"][0]["lag"], 30);
    assert!(after["queued_bytes"].as_u64() > before["queued_bytes"].as_u64());
    assert_eq!(after["service"], "none");
    assert_eq!(
        subscribers(&hub, &Query::parse("dataset=cdn"))["items"],
        json!([])
    );
}

#[test]
fn query_filters_and_pages_namespaces() {
    let query = Query::parse("dataset=auth&prefix=a%2Fb+c&limit=0&after=auth/x&service=cdn");
    assert_eq!(
        query,
        Query {
            dataset: Some("auth".into()),
            prefix: "a/b c".into(),
            service: Some("cdn".into()),
            after: Some("auth/x".into()),
            limit: 1,
        }
    );
    assert_eq!(decode("%zz%4"), "%zz%4");
}

#[tokio::test]
async fn namespaces_page_by_cursor() {
    let hub = relay();
    for namespace in ["a/1", "a/2", "a/3", "b/1"] {
        hub.publish(event(namespace, 1));
    }
    let first = namespaces(&hub, &Query::parse("prefix=a%2F&limit=2"));
    let names: Vec<_> = first["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["namespace"].clone())
        .collect();
    assert_eq!(names, [json!("a/1"), json!("a/2")]);
    assert_eq!(first["next"], "auth/a/2");
    let rest = namespaces(
        &hub,
        &Query::parse("prefix=a%2F&limit=2&after=auth%2Fa%2F2"),
    );
    assert_eq!(rest["items"][0]["namespace"], "a/3");
    assert_eq!(rest["items"][0]["latest"], 1);
    assert_eq!(rest["items"][0]["lag_ms"], 250);
    assert_eq!(rest["next"], Value::Null);
}

#[test]
fn seek_matches_a_filtered_scan_across_datasets() {
    let map: BTreeMap<Key, ()> = ["auth", "cdn", "quicksilver"]
        .into_iter()
        .flat_map(|d| ["a", "b/1", "b/2", "c"].map(|n| ((d.to_owned(), n.to_owned()), ())))
        .collect();
    for query in [
        "prefix=b",
        "prefix=b&after=auth%2Fb%2F2",
        "dataset=cdn&prefix=b",
        "dataset=cdn",
        "prefix=z",
        "dataset=zzz",
        "",
    ] {
        let query = Query::parse(query);
        let after = query.after_key();
        let scanned: Vec<Key> = map
            .keys()
            .filter(|k| query.matches(k) && after.as_ref().is_none_or(|a| *k > a))
            .take(3)
            .cloned()
            .collect();
        assert_eq!(seek(&map, &query, 3), scanned, "{query:?}");
    }
}
