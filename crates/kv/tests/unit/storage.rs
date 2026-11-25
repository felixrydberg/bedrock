use super::*;
use tokio::time::sleep;
#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn cached_values_expire_and_never_answer_a_newer_minimum_revision() {
    let cache = ValueCache::new(&Admin::new(), 1 << 20);
    cache.fill_at("a", b"k", b"v", 5, bedrock_lib::monotonic_now());
    assert_eq!(cache.get("a", b"k", 5), Some((b"v".to_vec(), 5)));
    assert_eq!(cache.get("a", b"k", 6), None);
    assert_eq!(cache.get("b", b"k", 0), None, "namespaces are separate");
    cache.fill_at("a", b"k", b"older", 4, bedrock_lib::monotonic_now());
    assert_eq!(cache.get("a", b"k", 0), Some((b"v".to_vec(), 5)));

    sleep(Duration::from_secs(CACHE_TTL_SECONDS.into())).await;
    assert_eq!(cache.get("a", b"k", 0), None);
    assert_eq!(cache.inner.lock().unwrap().total, 0);
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn delayed_fills_keep_the_source_fetch_deadline_and_value_expiration() {
    use prost::Message;
    let cache = ValueCache::new(&Admin::new(), 1 << 20);
    let started = bedrock_lib::monotonic_now();
    sleep(Duration::from_secs(59)).await;
    cache.fill_at("a", b"late", b"v", 1, started);
    assert!(cache.get("a", b"late", 0).is_some());
    sleep(Duration::from_secs(1)).await;
    assert!(cache.get("a", b"late", 0).is_none());
    cache.fill_at("a", b"too-late", b"v", 1, started);
    assert!(cache.get("a", b"too-late", 0).is_none());

    sleep(Duration::from_millis(250)).await;
    let expiration = namespaces::wall_ms() / 1000 + 2;
    let remaining = expiration * 1000 - namespaces::wall_ms();
    let value = bedrock_lib::kv::Stored {
        value: b"expires sooner".to_vec(),
        metadata: None,
        expiration,
    }
    .encode_to_vec();
    cache.fill_at("a", b"expiring", &value, 2, bedrock_lib::monotonic_now());
    assert!(cache.get("a", b"expiring", 0).is_some());
    sleep(Duration::from_millis(remaining)).await;
    assert!(cache.get("a", b"expiring", 0).is_none());
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn expired_values_free_their_namespace_share() {
    let budget = 8 * 1024;
    let cache = ValueCache::new(&Admin::new(), budget);
    for n in 0..100u32 {
        cache.fill_at(
            "hot",
            &n.to_be_bytes(),
            &[0; 100],
            1,
            bedrock_lib::monotonic_now(),
        );
    }
    sleep(Duration::from_secs(CACHE_TTL_SECONDS.into())).await;
    cache.fill_at("hot", b"new", &[0; 100], 2, bedrock_lib::monotonic_now());
    assert!(cache.get("hot", b"new", 0).is_some());
}

#[test]
fn one_namespace_cannot_take_over_the_value_cache() {
    let budget = 8 * 1024;
    let cache = ValueCache::new(&Admin::new(), budget);
    cache.fill_at("quiet", b"k", &[0; 100], 1, bedrock_lib::monotonic_now());
    for n in 0..100u32 {
        cache.fill_at(
            "hot",
            &n.to_be_bytes(),
            &[0; 100],
            1,
            bedrock_lib::monotonic_now(),
        );
    }
    let state = cache.inner.lock().unwrap();
    assert!(state.by_namespace["hot"] <= budget / SHARE);
    assert!(state.total <= budget);
    drop(state);
    assert!(
        cache.get("quiet", b"k", 0).is_some(),
        "the hot namespace evicted nothing"
    );
}

#[test]
fn the_value_cache_stays_within_its_budget() {
    let budget = 64 * 1024;
    let cache = ValueCache::new(&Admin::new(), budget);
    for namespace in 0..64u32 {
        for key in 0..8u32 {
            cache.fill_at(
                &namespace.to_string(),
                &key.to_be_bytes(),
                &[0; 200],
                1,
                bedrock_lib::monotonic_now(),
            );
        }
    }
    let state = cache.inner.lock().unwrap();
    assert!(state.total <= budget);
    assert_eq!(state.total, state.by_namespace.values().sum::<u64>());
    // The oldest fills went first.
    assert!(
        state
            .entries
            .peek(&("0".into(), 0u32.to_be_bytes().to_vec()))
            .is_none()
    );
    assert!(
        state
            .entries
            .peek(&("63".into(), 7u32.to_be_bytes().to_vec()))
            .is_some()
    );
}
