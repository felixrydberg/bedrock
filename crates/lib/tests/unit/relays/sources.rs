use tokio::time::sleep;

use super::*;
use crate::buckets::hash_of;
use crate::health::CONSECUTIVE_FAILURES;
use crate::placement::Owner;
use crate::transport::admin::Admin;

fn member(id: &str, domain: &str) -> pb::Member {
    pb::Member {
        node_id: id.into(),
        failure_domain: domain.into(),
        address: format!("{id}:7000"),
        status: pb::member::Status::Ready as i32,
        ..Default::default()
    }
}

fn members(ids: &[(&str, &str)]) -> BTreeMap<String, pb::Member> {
    ids.iter()
        .map(|(id, domain)| ((*id).into(), member(id, domain)))
        .collect()
}

fn owner(node: &str, state: State) -> Owner {
    Owner {
        node: node.into(),
        state,
    }
}

/// One range covering the whole hash space.
fn directory(owners: Vec<Owner>) -> Directory {
    Directory::from([(0, owners)])
}

fn ids(selected: &[&pb::Member]) -> Vec<String> {
    selected.iter().map(|m| m.node_id.clone()).collect()
}

fn open(health: &Health, node: &str) {
    health.trip(&format!("{node}:7000"));
}

const H: u64 = 42;

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn ranks_ready_before_leaving_and_never_returns_populating() {
    let health = Health::new(&Admin::new());
    let members = members(&[("a", "d1"), ("b", "d1"), ("c", "d1"), ("d", "d1")]);
    let mut dir = directory(vec![
        owner("a", State::Leaving),
        owner("b", State::Populating),
        owner("c", State::Ready),
        owner("d", State::Ready),
    ]);
    let sources = Sources::default();
    let pick = |dir: &Directory| {
        ids(&sources.select(&members, dir, &health, "", "quicksilver", "ns", H, 0))
    };
    assert_eq!(pick(&dir), ["c", "d", "a"]);

    // Revoked: the leaving owner is gone from the entry, and with it from the results.
    dir.insert(
        0,
        vec![
            owner("b", State::Ready),
            owner("c", State::Ready),
            owner("d", State::Ready),
        ],
    );
    assert_eq!(pick(&dir), ["b", "c", "d"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn prefers_the_callers_failure_domain_then_draining_last() {
    let health = Health::new(&Admin::new());
    let mut members = members(&[("a", "d1"), ("b", "d2"), ("c", "d2")]);
    let dir = directory(vec![
        owner("a", State::Ready),
        owner("b", State::Ready),
        owner("c", State::Ready),
    ]);
    let sources = Sources::default();
    let select = |members: &BTreeMap<String, pb::Member>, domain| {
        ids(&sources.select(members, &dir, &health, domain, "quicksilver", "ns", H, 0))
    };
    assert_eq!(select(&members, "d2"), ["b", "c", "a"]);
    // An unknown domain prefers nothing.
    assert_eq!(select(&members, ""), ["a", "b", "c"]);

    members.get_mut("b").unwrap().status = pb::member::Status::Draining as i32;
    assert_eq!(select(&members, "d2"), ["c", "a", "b"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn failed_holder_disappears() {
    let health = Health::new(&Admin::new());
    let mut members = members(&[("a", ""), ("b", ""), ("c", "")]);
    let dir = directory(vec![
        owner("a", State::Ready),
        owner("b", State::Ready),
        owner("c", State::Ready),
    ]);
    let sources = Sources::default();
    let select = |members: &BTreeMap<String, pb::Member>| {
        ids(&sources.select(members, &dir, &health, "", "quicksilver", "ns", H, 0))
    };

    // Its breaker opens on real traffic first.
    open(&health, "a");
    assert_eq!(select(&members), ["b", "c"]);
    // Removed from membership after the grace period, it is never returned, even before the directory moves its ranges.
    members.remove("a");
    sleep(crate::health::BASE_BACKOFF).await;
    assert_eq!(select(&members), ["b", "c"]);

    // With every breaker open, all are still tried.
    for node in ["b", "c"] {
        open(&health, node);
    }
    assert_eq!(select(&members), ["b", "c"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn stale_holder_is_skipped_for_newer_reads_until_recheck() {
    let health = Health::new(&Admin::new());
    let members = members(&[("a", ""), ("b", "")]);
    let dir = directory(vec![owner("a", State::Ready), owner("b", State::Ready)]);
    let sources = Sources::default();
    let select = |namespace, min| {
        ids(&sources.select(
            &members,
            &dir,
            &health,
            "",
            "quicksilver",
            namespace,
            H,
            min,
        ))
    };

    // Unknown revisions are tried: the holder checks freshness when it serves.
    assert_eq!(select("ns", 10), ["a", "b"]);
    sources.behind("a", "quicksilver", "ns", 7);
    assert_eq!(select("ns", 10), ["b"]);
    // Reads it can serve, and other namespaces, still go to it.
    assert_eq!(select("ns", 7), ["a", "b"]);
    assert_eq!(select("other", 10), ["a", "b"]);

    // After RECHECK it is tried again, since it may have caught up.
    sleep(RECHECK).await;
    assert_eq!(select("ns", 10), ["a", "b"]);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn cdn_artifacts_resolve_by_content_hash() {
    let health = Health::new(&Admin::new());
    let members = members(&[("a", ""), ("b", ""), ("c", "")]);
    let split = 1 << 63;
    let dir = Directory::from([
        (0, vec![owner("a", State::Ready), owner("b", State::Ready)]),
        (
            split,
            vec![owner("b", State::Ready), owner("c", State::Ready)],
        ),
    ]);
    let sources = Sources::default();
    let select = |hash| ids(&sources.select(&members, &dir, &health, "", "cdn", "", hash, 0));
    assert_eq!(select(split - 1), ["a", "b"]);
    assert_eq!(select(split), ["b", "c"]);
    // Before the directory's first write there are no holders.
    assert!(
        sources
            .select(&members, &Directory::new(), &health, "", "cdn", "", H, 0)
            .is_empty()
    );
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn kv_readers_fail_over_to_the_next_ranked() {
    let health = Health::new(&Admin::new());
    let mut members = members(&[("a", ""), ("b", ""), ("c", ""), ("d", "")]);
    let hash = hash_of("kv", "ns", b"key");
    let ranked = ids(&placement::rank(hash, members.values()));
    assert_eq!(ids(&readers(members.values(), &health, hash)), ranked);

    // The preferred reader's breaker opens: the next-ranked reader serves.
    open(&health, &ranked[0]);
    assert_eq!(ids(&readers(members.values(), &health, hash)), ranked[1..]);

    // A draining reader gets no new reads.
    members.get_mut(&ranked[1]).unwrap().status = pb::member::Status::Draining as i32;
    assert_eq!(ids(&readers(members.values(), &health, hash)), ranked[2..]);

    // Repeated failures behave the same way as a trip.
    for _ in 0..CONSECUTIVE_FAILURES {
        let peer = format!("{}:7000", ranked[2]);
        let probe = health.begin(&peer, Duration::from_millis(100));
        health.record(&peer, probe, false);
    }
    assert_eq!(ids(&readers(members.values(), &health, hash)), ranked[3..]);
}
