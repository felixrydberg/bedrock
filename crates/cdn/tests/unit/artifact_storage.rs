use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bedrock_lib::admin::Admin;
use bedrock_lib::health::CONSECUTIVE_FAILURES;
use bedrock_lib::health::storage::{State, StorageHealth};
use bedrock_lib::storage::StorageError;
use object_store::memory::InMemory;
use object_store::path::Path;

use super::store::{DATASET, LAYER};
use super::*;

/// A cache that flips a byte of every artifact it returns while `damaged` is set, and counts reads.
#[derive(Default)]
struct Damaged {
    artifacts: Mutex<BTreeMap<Hash, Bytes>>,
    damaged: std::sync::atomic::AtomicBool,
    reads: AtomicUsize,
}

#[async_trait]
impl CacheStorage for Damaged {
    async fn put(&self, hash: &Hash, body: Body) -> StorageResult<()> {
        let bytes = collect(body).await?;
        self.artifacts.lock().unwrap().insert(*hash, bytes);
        Ok(())
    }
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Body>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let Some(bytes) = self.artifacts.lock().unwrap().get(hash).cloned() else {
            return Ok(None);
        };
        let mut bytes = bytes.to_vec();
        if self.damaged.load(Ordering::Relaxed) {
            bytes[0] ^= 1;
        }
        Ok(Some(body(bytes.into())))
    }
    async fn contains(&self, hash: &Hash) -> StorageResult<bool> {
        Ok(self.artifacts.lock().unwrap().contains_key(hash))
    }
    async fn delete(&self, hash: &Hash) -> StorageResult<()> {
        self.artifacts.lock().unwrap().remove(hash);
        Ok(())
    }
    fn inventory(&self) -> StorageResult<Vec<(Hash, u64)>> {
        unreachable!()
    }
    fn usage(&self) -> StorageResult<u64> {
        unreachable!()
    }
}

async fn read(artifacts: &Artifacts, hash: &Hash) -> Option<Bytes> {
    Some(
        collect(artifacts.fetch(hash).await.unwrap()?)
            .await
            .unwrap(),
    )
}

#[test]
fn hashes_round_trip_through_hex() {
    let hash = Hash::of(b"artifact");
    assert_eq!(Hash::parse(&hash.to_string()), Some(hash));
    assert_eq!(Hash::parse(&hash.to_string().to_uppercase()), None);
    assert_eq!(Hash::parse("00"), None);
}

#[tokio::test]
async fn upload_then_fetch_round_trips_through_foyer() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(Foyer::open(dir.path(), 4, 16).await.unwrap());
    let artifacts = Artifacts::new(
        Arc::new(InMemory::new()),
        cache.clone(),
        StorageHealth::new(&Admin::new()),
    );

    let hash = artifacts
        .upload(body(Bytes::from_static(b"bundle")))
        .await
        .unwrap();
    assert_eq!(hash, Hash::of(b"bundle"));
    // Content addressing makes a repeated upload a no-op.
    assert_eq!(
        artifacts
            .upload(body(Bytes::from_static(b"bundle")))
            .await
            .unwrap(),
        hash
    );
    assert!(!cache.contains(&hash).await.unwrap());

    assert_eq!(
        read(&artifacts, &hash).await.as_deref(),
        Some(&b"bundle"[..])
    );
    assert!(
        cache.contains(&hash).await.unwrap(),
        "a fetch fills the cache"
    );
    assert_eq!(
        read(&artifacts, &hash).await.as_deref(),
        Some(&b"bundle"[..])
    );
    assert_eq!(read(&artifacts, &Hash::of(b"missing")).await, None);
}

#[tokio::test]
async fn a_corrupted_cached_artifact_is_evicted_reported_and_refetched() {
    let cache = Arc::new(Damaged::default());
    let health = StorageHealth::new(&Admin::new());
    let artifacts = Artifacts::new(Arc::new(InMemory::new()), cache.clone(), health.clone());
    let hash = artifacts
        .upload(body(Bytes::from_static(b"page")))
        .await
        .unwrap();
    read(&artifacts, &hash).await;

    cache.damaged.store(true, Ordering::Relaxed);
    assert_eq!(read(&artifacts, &hash).await.as_deref(), Some(&b"page"[..]));
    let report = health.get(DATASET, LAYER).unwrap();
    assert_eq!(
        report.state,
        State::Healthy,
        "one bad artifact does not quarantine the cache"
    );
    assert_eq!(report.errors, 1);
    assert!(matches!(report.last, Some(StorageError::Corruption(_))));
    assert!(health.unhealthy().is_empty());
    // Evicted, then refilled from S3.
    assert!(cache.contains(&hash).await.unwrap());
}

#[tokio::test]
async fn repeated_corruption_quarantines_the_cache_and_reads_bypass_it() {
    let cache = Arc::new(Damaged::default());
    let health = StorageHealth::new(&Admin::new());
    let artifacts = Artifacts::new(Arc::new(InMemory::new()), cache.clone(), health.clone());
    let hash = artifacts
        .upload(body(Bytes::from_static(b"page")))
        .await
        .unwrap();
    read(&artifacts, &hash).await;
    cache.damaged.store(true, Ordering::Relaxed);
    for _ in 0..CONSECUTIVE_FAILURES {
        assert_eq!(read(&artifacts, &hash).await.as_deref(), Some(&b"page"[..]));
    }
    assert_eq!(health.get(DATASET, LAYER).unwrap().state, State::Unhealthy);

    let reads = cache.reads.load(Ordering::Relaxed);
    assert_eq!(read(&artifacts, &hash).await.as_deref(), Some(&b"page"[..]));
    assert_eq!(
        cache.reads.load(Ordering::Relaxed),
        reads,
        "a quarantined cache is not read"
    );
}

#[tokio::test]
async fn an_s3_object_that_does_not_match_its_name_is_corruption() {
    let s3 = Arc::new(InMemory::new());
    let health = StorageHealth::new(&Admin::new());
    let cache = Arc::new(Damaged::default());
    let artifacts = Artifacts::new(s3.clone(), cache.clone(), health.clone());
    let hash = Hash::of(b"expected");
    let path = Path::from(format!("artifacts/{hash}"));
    bedrock_lib::s3::put_checked(
        &*s3,
        &path,
        b"other".to_vec(),
        object_store::PutMode::Create,
    )
    .await
    .unwrap();
    assert!(matches!(
        artifacts.fetch(&hash).await,
        Err(StorageError::Corruption(_))
    ));
    assert_eq!(
        health.get(DATASET, LAYER),
        None,
        "S3 damage is not local damage"
    );
    assert!(!cache.contains(&hash).await.unwrap(), "nor cached");
}

#[tokio::test]
async fn the_foyer_inventory_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (Hash::of(b"a"), Hash::of(b"bb"));
    {
        let cache = Foyer::open(dir.path(), 4, 16).await.unwrap();
        cache.put(&a, body(Bytes::from_static(b"a"))).await.unwrap();
        cache
            .put(&b, body(Bytes::from_static(b"bb")))
            .await
            .unwrap();
        cache.close().await.unwrap();
    }
    // A record torn by a crash is dropped.
    #[allow(clippy::disallowed_methods)] // the test tears an adapter file
    std::fs::write(
        dir.path()
            .join("inventory")
            .join(Hash::of(b"c").to_string()),
        "",
    )
    .unwrap();

    let cache = Foyer::open(dir.path(), 4, 16).await.unwrap();
    let mut expected = vec![(a, 1), (b, 2)];
    expected.sort();
    assert_eq!(cache.inventory().unwrap(), expected);
    assert_eq!(cache.usage().unwrap(), 3);
    let got = collect(cache.get(&b).await.unwrap().unwrap())
        .await
        .unwrap();
    assert_eq!(&got[..], b"bb");

    cache.delete(&a).await.unwrap();
    assert!(!cache.contains(&a).await.unwrap());
    assert_eq!(cache.get(&a).await.unwrap().map(|_| ()), None);
    assert_eq!(cache.inventory().unwrap(), vec![(b, 2)]);
}

/// Foyer checks its own disk blocks, so damage there reads as a miss and the artifact is refetched from S3.
#[tokio::test]
#[allow(clippy::disallowed_methods)] // the test damages the adapter's files
async fn damage_on_foyers_disk_is_refetched_from_s3() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = Arc::new(InMemory::new());
    let artifact: Vec<u8> = (0..64 << 10).map(|i| (i * 7 % 251) as u8).collect();
    let hash = {
        let cache = Arc::new(Foyer::open(dir.path(), 4, 16).await.unwrap());
        let artifacts =
            Artifacts::new(s3.clone(), cache.clone(), StorageHealth::new(&Admin::new()));
        let hash = artifacts
            .upload(body(artifact.clone().into()))
            .await
            .unwrap();
        read(&artifacts, &hash).await;
        cache.close().await.unwrap();
        hash
    };
    let mut damaged = 0;
    for entry in std::fs::read_dir(dir.path().join("foyer")).unwrap() {
        let path = entry.unwrap().path();
        let mut bytes = std::fs::read(&path).unwrap();
        if let Some(at) = bytes.windows(4096).position(|w| w == &artifact[..4096]) {
            bytes[at + 100] ^= 1;
            std::fs::write(&path, bytes).unwrap();
            damaged += 1;
        }
    }
    assert_eq!(damaged, 1, "the artifact reached Foyer's disk");

    let cache = Arc::new(Foyer::open(dir.path(), 4, 16).await.unwrap());
    let health = StorageHealth::new(&Admin::new());
    let artifacts = Artifacts::new(s3, cache.clone(), health.clone());
    assert!(cache.get(&hash).await.unwrap().is_none());
    assert_eq!(
        read(&artifacts, &hash).await.as_deref(),
        Some(&artifact[..])
    );
    assert!(cache.contains(&hash).await.unwrap(), "refilled from S3");
    // ponytail: Foyer cannot tell damage from eviction, so the health manager never hears of it.
    assert_eq!(health.get(DATASET, LAYER), None);
}

/// Foyer keeps an artifact larger than its 16 MiB block in memory only, without an error.
#[tokio::test]
async fn an_artifact_larger_than_a_foyer_block_does_not_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let big = Bytes::from(vec![7u8; (16 << 20) + 1]);
    let hash = Hash::of(&big);
    {
        let cache = Foyer::open(dir.path(), 64, 128).await.unwrap();
        cache.put(&hash, body(big.clone())).await.unwrap();
        assert!(cache.get(&hash).await.unwrap().is_some());
        cache.close().await.unwrap();
    }
    let cache = Foyer::open(dir.path(), 64, 128).await.unwrap();
    assert!(cache.get(&hash).await.unwrap().is_none());
    assert!(
        cache.inventory().unwrap().is_empty(),
        "the miss drops it from the inventory"
    );
}
