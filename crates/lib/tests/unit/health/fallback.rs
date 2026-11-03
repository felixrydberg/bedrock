use std::collections::BTreeMap;
use std::time::Duration;

use super::*;
use crate::health::storage::State;
use crate::reconciliation::rebuild::{Rebuild, Reinitialise, Restore};
use crate::reconciliation::{Config, Metrics as ReconcileMetrics, Reconciler};
use crate::storage::memory::MemoryEngine;

/// A scripted tier that records the fills it is offered.
struct Fake {
    name: &'static str,
    answer: StorageResult<Read>,
    fills: Mutex<Vec<Found>>,
}

#[async_trait::async_trait]
impl Tier<str> for Fake {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn read(&self, _: &str, _: u64) -> StorageResult<Read> {
        self.answer.clone()
    }

    async fn fill(&self, _: &str, found: &Found) -> StorageResult<()> {
        self.fills.lock().unwrap().push(found.clone());
        Ok(())
    }
}

fn fake(name: &'static str, answer: StorageResult<Read>) -> Arc<Fake> {
    Arc::new(Fake {
        name,
        answer,
        fills: Mutex::default(),
    })
}

fn found(value: Option<&[u8]>, revision: u64) -> StorageResult<Read> {
    Ok(Read::Found(Found {
        value: value.map(<[u8]>::to_vec),
        revision,
    }))
}

fn down(what: &str) -> StorageResult<Read> {
    Err(StorageError::Unavailable(ErrorContext {
        engine: "test",
        instance: what.into(),
        file: None,
        partition: None,
        detail: format!("{what} is down"),
    }))
}

fn chain(admin: &Admin, tiers: &[&Arc<Fake>]) -> SourceChain<str> {
    let tiers = tiers
        .iter()
        .map(|tier| Arc::clone(*tier) as Arc<dyn Tier<str>>)
        .collect();
    SourceChain::new("cdn", tiers, &Metrics::new(admin))
}

fn fills(tier: &Fake) -> Vec<Found> {
    tier.fills.lock().unwrap().clone()
}

#[tokio::test]
async fn broken_tiers_are_bypassed_and_only_tiers_that_missed_are_filled() {
    let admin = Admin::new();
    // L1 broken: L2 answers.
    let l1 = fake("l1", down("l1"));
    let l2 = fake("l2", found(Some(b"v"), 7));
    let answer = chain(&admin, &[&l1, &l2]).read("k", 0).await.unwrap();
    assert_eq!(answer.value.as_deref(), Some(&b"v"[..]));
    assert!(fills(&l1).is_empty(), "a failed tier is not filled");

    // L2 broken: the next tier (peers) answers, and the L1 that missed is filled.
    let admin = Admin::new();
    let l1 = fake("l1", Ok(Read::Miss));
    let l2 = fake("l2", down("l2"));
    let peer = fake("peer", found(Some(b"v"), 7));
    let answer = chain(&admin, &[&l1, &l2, &peer])
        .read("k", 0)
        .await
        .unwrap();
    assert_eq!(answer.revision, 7);
    assert_eq!(fills(&l1), [answer]);
    assert!(fills(&l2).is_empty());
    let metrics = admin.render();
    assert!(
        metrics.contains(r#"fallback_tier_reads_total{dataset="cdn",tier="l2",outcome="error"} 1"#),
        "{metrics}"
    );
}

#[tokio::test]
async fn stale_peer_is_skipped_for_a_minimum_revision_read() {
    let admin = Admin::new();
    let stale = fake("peer", found(Some(b"old"), 3));
    let fresh = fake("peer", found(Some(b"new"), 9));
    let peers = chain(&admin, &[&stale, &fresh]);
    let answer = peers.read("k", 5).await.unwrap();
    assert_eq!(answer.value.as_deref(), Some(&b"new"[..]));
    // Offered to the stale peer, which is behind the answer.
    assert_eq!(fills(&stale), [answer]);
    // Without a minimum, the stale peer's answer stands.
    assert_eq!(peers.read("k", 0).await.unwrap().revision, 3);
}

#[tokio::test]
async fn customer_kv_reader_loss_publication_lag_and_s3_outage() {
    let admin = Admin::new();
    let l1 = fake("l1", Ok(Read::Miss));
    let preferred = fake("reader", down("preferred reader"));
    // The next reader's S3 view lags the write the caller already saw (revision 6).
    let lagging = fake("reader", found(Some(b"v1"), 4));
    let recent = fake("recent", found(Some(b"v2"), 6));
    let s3 = fake("s3", found(Some(b"v1"), 4));
    let answer = chain(&admin, &[&l1, &preferred, &lagging, &recent, &s3])
        .read("k", 6)
        .await
        .unwrap();
    assert_eq!(
        (answer.value.as_deref(), answer.revision),
        (Some(&b"v2"[..]), 6)
    );

    // An absence proven by a sufficiently new view is an answer.
    let gone = fake("s3", found(None, 8));
    let answer = chain(&admin, &[&l1, &preferred, &gone])
        .read("k", 6)
        .await
        .unwrap();
    assert_eq!(
        answer,
        Found {
            value: None,
            revision: 8
        }
    );

    // With S3 down and only a lagging reader left, the read fails instead of claiming the key is missing.
    let s3_down = fake("s3", down("s3"));
    let error = chain(&admin, &[&l1, &preferred, &lagging, &s3_down])
        .read("k", 6)
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::Unavailable(_)), "{error}");
    let error = chain(&admin, &[&l1]).read("k", 0).await.unwrap_err();
    assert!(
        matches!(error, StorageError::Unavailable(_)),
        "a miss everywhere proves nothing"
    );
}

fn cache(admin: &Admin) -> (Arc<StorageHealth>, Arc<MemoryEngine>, Arc<CacheTier<str>>) {
    let health = StorageHealth::new(admin);
    let raw = Arc::new(MemoryEngine::new("cdn"));
    let tier = CacheTier::new(
        "cdn",
        "l1",
        health.wrap("cdn", "l1", raw.clone()),
        health.clone(),
        Box::new(|key: &str| ("artifacts".to_owned(), key.as_bytes().to_vec())),
    );
    (health, raw, Arc::new(tier))
}

async fn cached(tier: &CacheTier<str>) -> Read {
    tier.read("k", 0).await.unwrap()
}

fn at(value: Option<&[u8]>, revision: u64) -> Found {
    Found {
        value: value.map(<[u8]>::to_vec),
        revision,
    }
}

#[tokio::test]
async fn cache_fills_never_go_backwards_or_resurrect_deletes() {
    let admin = Admin::new();
    let (_, _, tier) = cache(&admin);
    tier.fill("k", &at(Some(b"x"), 10)).await.unwrap();
    assert_eq!(cached(&tier).await, Read::Found(at(Some(b"x"), 10)));
    // An older source never replaces a newer value.
    tier.fill("k", &at(Some(b"y"), 5)).await.unwrap();
    assert_eq!(cached(&tier).await, Read::Found(at(Some(b"x"), 10)));
    // A newer absence removes it, and a late fill from before the delete cannot bring it back.
    tier.fill("k", &at(None, 12)).await.unwrap();
    assert_eq!(cached(&tier).await, Read::Miss);
    tier.fill("k", &at(Some(b"x"), 10)).await.unwrap();
    assert_eq!(cached(&tier).await, Read::Miss);
    tier.fill("k", &at(Some(b"z"), 12)).await.unwrap();
    assert_eq!(cached(&tier).await, Read::Found(at(Some(b"z"), 12)));
}

#[tokio::test]
async fn stale_cache_entry_is_refetched_for_a_minimum_revision_read() {
    let admin = Admin::new();
    let (_, _, tier) = cache(&admin);
    tier.fill("k", &at(Some(b"x"), 10)).await.unwrap();
    let origin = fake("s3", found(Some(b"new"), 20));
    let tiers: Vec<Arc<dyn Tier<str>>> = vec![tier.clone(), origin];
    let reads = SourceChain::new("cdn", tiers, &Metrics::new(&admin));
    assert_eq!(reads.read("k", 0).await.unwrap().revision, 10);
    assert_eq!(reads.read("k", 15).await.unwrap().revision, 20);
    assert_eq!(cached(&tier).await, Read::Found(at(Some(b"new"), 20)));
}

#[cfg_attr(not(madsim), tokio::test(start_paused = true))]
#[cfg_attr(madsim, tokio::test)]
async fn corrupted_cache_entry_is_quarantined_refetched_and_rebuilt_before_use() {
    let admin = Admin::new();
    let (health, raw, tier) = cache(&admin);
    tier.fill("k", &at(Some(b"x"), 10)).await.unwrap();
    // Damage the entry below the engine's own checks: the bytes read back cleanly but fail the checksum.
    let mut damaged = raw.get(b"k").unwrap().unwrap();
    *damaged.last_mut().unwrap() ^= 1;
    raw.put(b"k", &damaged).unwrap();

    let origin = fake("s3", found(Some(b"x"), 10));
    let tiers: Vec<Arc<dyn Tier<str>>> = vec![tier.clone(), origin.clone()];
    let reads = SourceChain::new("cdn", tiers, &Metrics::new(&admin));
    let answer = reads.read("k", 0).await.unwrap();
    assert_eq!(
        answer.value.as_deref(),
        Some(&b"x"[..]),
        "refetched from the next tier"
    );
    assert_eq!(health.get("cdn", "l1").unwrap().state, State::Unhealthy);
    // The quarantined cache refuses reads and fills without touching its storage.
    assert!(matches!(
        tier.read("k", 0).await,
        Err(StorageError::Unavailable(_))
    ));
    assert!(matches!(
        tier.fill("k", &at(Some(b"x"), 10)).await,
        Err(StorageError::Unavailable(_))
    ));

    let restore: Arc<dyn Restore> = Arc::new(Reinitialise {
        engine: health.wrap_replication("cdn", "l1", raw.clone()),
        resubscribe: None,
    });
    let rebuild = Rebuild::new(
        health.clone(),
        BTreeMap::from([(("cdn".to_owned(), "l1"), restore)]),
    );
    let config = Config {
        burst: 1,
        refill: Duration::from_millis(1),
        repair_timeout: Duration::from_secs(1),
        base_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
        seed: 1,
    };
    let mut reconciler = Reconciler::new(
        "rebuild",
        Arc::new(rebuild),
        config,
        &ReconcileMetrics::new(&admin),
    );
    assert_eq!(reconciler.pass().await.unwrap().repaired, 1);
    assert_eq!(health.get("cdn", "l1").unwrap().state, State::Healthy);
    assert_eq!(
        raw.get(b"k").unwrap(),
        None,
        "the damaged instance was recreated empty"
    );

    // Back in service, the cache refills from the next read and then answers on its own.
    reads.read("k", 0).await.unwrap();
    assert_eq!(cached(&tier).await, Read::Found(at(Some(b"x"), 10)));
}
