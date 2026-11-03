//! Tiered read fallback (#23 / B-22, `kv.md` §15.3, §15.8). A product supplies its tiers in order, e.g. CDN local
//! cache → L2 holders → peers → S3, or customer KV L1 → preferred reader → next reader → S3 (B-27/B-39), and every
//! read walks the same [`SourceChain`].
//!
//! - **Skipping.** A tier that errors is skipped. Known-unhealthy sources are skipped without I/O until recheck by the
//!   machinery that already tracks them: a quarantined engine refuses at once (B-14), and peer tiers pick holders with
//!   [`crate::relays::sources`], which routes around open breakers (B-13) and holders that reported being behind.
//! - **Freshness.** Every answer names the namespace view revision that proves it, including an absence. An answer
//!   older than the read's minimum revision is skipped: a healthy but stale source is no replacement.
//! - **No false misses.** Only a tier's [`Found`] answers a read. When every tier missed, failed or was stale, the read
//!   fails `Unavailable`; an unavailable source never proves a key missing.
//! - **Fills.** The answer is offered to the tiers above it that missed or were stale. A receiver accepts it only if it
//!   is not further along itself ([`CacheTier`] does this atomically).
//!
//! Tiers verify what they read (checksums, content hashes) and report corruption to storage health themselves.

use std::sync::{Arc, Mutex};

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use sha2::{Digest, Sha256};

use super::storage::StorageHealth;
use crate::storage::{ErrorContext, StorageEngine, StorageError, StorageResult, WriteBatch};
use crate::transport::admin::Admin;

/// A tier's answer: the value, or `None` for a proven absence, as of namespace view `revision`. A key last changed at
/// revision 5 may be answered at view revision 100.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub value: Option<Vec<u8>>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// This tier does not hold the key; it proves nothing.
    Miss,
    Found(Found),
}

/// One tier of a product's read path. `K` is whatever the product reads by.
#[async_trait::async_trait]
pub trait Tier<K: ?Sized + Sync>: Send + Sync {
    /// Low-cardinality metric label, e.g. `l1`, `l2`, `peer`, `reader`, `s3`.
    fn name(&self) -> &'static str;

    /// `min_revision` lets a remote holder reject a read it is behind on. Corruption and unavailability are errors.
    async fn read(&self, key: &K, min_revision: u64) -> StorageResult<Read>;

    /// Offers an answer from a tier below. Tiers that cache nothing keep this default.
    async fn fill(&self, _key: &K, _found: &Found) -> StorageResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct Labels {
    dataset: String,
    tier: &'static str,
    outcome: &'static str,
}

/// Registered once per process and shared by every chain.
#[derive(Clone)]
pub struct Metrics {
    reads: Family<Labels, Counter>,
}

impl Metrics {
    pub fn new(admin: &Admin) -> Self {
        let reads = Family::default();
        admin.register(
            "fallback_tier_reads",
            "Tier outcomes of tiered reads (hit, miss, stale, error, fill_error)",
            reads.clone(),
        );
        Metrics { reads }
    }
}

pub struct SourceChain<K: ?Sized> {
    dataset: String,
    tiers: Vec<Arc<dyn Tier<K>>>,
    metrics: Metrics,
}

impl<K: ?Sized + Sync> SourceChain<K> {
    pub fn new(dataset: &str, tiers: Vec<Arc<dyn Tier<K>>>, metrics: &Metrics) -> Self {
        SourceChain {
            dataset: dataset.to_owned(),
            tiers,
            metrics: metrics.clone(),
        }
    }

    /// The first answer at least `min_revision` new, after filling the tiers above it that missed or were stale.
    pub async fn read(&self, key: &K, min_revision: u64) -> StorageResult<Found> {
        let mut behind: Vec<&Arc<dyn Tier<K>>> = Vec::new();
        let mut failures = Vec::new();
        for tier in &self.tiers {
            match tier.read(key, min_revision).await {
                Ok(Read::Found(found)) if found.revision >= min_revision => {
                    self.count(tier.name(), "hit");
                    for receiver in behind {
                        if let Err(error) = receiver.fill(key, &found).await {
                            self.count(receiver.name(), "fill_error");
                            tracing::warn!(dataset = %self.dataset, tier = receiver.name(), %error, "fill failed");
                        }
                    }
                    return Ok(found);
                }
                Ok(Read::Found(found)) => {
                    self.count(tier.name(), "stale");
                    failures.push(format!("{} is at revision {}", tier.name(), found.revision));
                    behind.push(tier);
                }
                Ok(Read::Miss) => {
                    self.count(tier.name(), "miss");
                    behind.push(tier);
                }
                Err(error) => {
                    self.count(tier.name(), "error");
                    failures.push(format!("{}: {error}", tier.name()));
                }
            }
        }
        Err(StorageError::Unavailable(ErrorContext {
            engine: "fallback",
            instance: self.dataset.clone(),
            file: None,
            partition: None,
            detail: format!(
                "no tier answered at revision {min_revision} or newer ({})",
                failures.join("; ")
            ),
        }))
    }

    fn count(&self, tier: &'static str, outcome: &'static str) {
        self.metrics
            .reads
            .get_or_create(&Labels {
                dataset: self.dataset.clone(),
                tier,
                outcome,
            })
            .inc();
    }
}

/// Where a key lives in a [`CacheTier`]'s engine: its namespace and physical key.
pub type Locate<K> = Box<dyn Fn(&K) -> (String, Vec<u8>) + Send + Sync>;

const HEADER: usize = 8 + 32;

/// SHA-256 over an entry's big-endian revision and value, so a damaged revision is caught like a damaged value.
fn checksum(revision: &[u8], value: &[u8]) -> [u8; 32] {
    Sha256::new()
        .chain_update(revision)
        .chain_update(value)
        .finalize()
        .into()
}

/// An on-demand local cache over a [`StorageHealth::wrap`]ped engine, e.g. an L1 keyspace. Each entry stores the view
/// revision it was answered at and a SHA-256 of that revision and its value, so a read verifies both and a
/// minimum-revision read can reject it as stale. Absences are not cached: not-found records belong to the KV read path (B-27).
///
/// A fill is accepted only from a source at least as far along as the namespace's newest accepted fill, kept as the
/// `fill:{namespace}` revision in the same batch. So an old fill never replaces a newer value, and never resurrects a
/// key a newer answer proved absent.
pub struct CacheTier<K: ?Sized> {
    dataset: String,
    layer: &'static str,
    engine: Arc<dyn StorageEngine>,
    health: Arc<StorageHealth>,
    locate: Locate<K>,
    // ponytail: one fill at a time per tier; stripe by namespace if fills contend.
    fills: Mutex<()>,
}

impl<K: ?Sized> CacheTier<K> {
    /// `layer` names the tier and must be the layer `engine` was wrapped as, so corruption quarantines that source.
    pub fn new(
        dataset: &str,
        layer: &'static str,
        engine: Arc<dyn StorageEngine>,
        health: Arc<StorageHealth>,
        locate: Locate<K>,
    ) -> Self {
        CacheTier {
            dataset: dataset.to_owned(),
            layer,
            engine,
            health,
            locate,
            fills: Mutex::new(()),
        }
    }
}

#[async_trait::async_trait]
impl<K: ?Sized + Sync> Tier<K> for CacheTier<K> {
    fn name(&self) -> &'static str {
        self.layer
    }

    async fn read(&self, key: &K, _min_revision: u64) -> StorageResult<Read> {
        let (_, physical) = (self.locate)(key);
        let Some(stored) = self.engine.get(&physical)? else {
            return Ok(Read::Miss);
        };
        let verified = (stored.len() >= HEADER)
            .then(|| stored.split_at(HEADER))
            .filter(|(header, value)| header[8..] == checksum(&header[..8], value));
        let Some((header, value)) = verified else {
            // The engine read cleanly, so the damage is past its own checks; quarantine as B-14 does for any
            // corruption, and the chain refetches from the next tier.
            let error = StorageError::Corruption(ErrorContext {
                engine: "cache",
                instance: self.dataset.clone(),
                file: None,
                partition: Some(format!("key {}", physical.escape_ascii())),
                detail: "cached entry failed checksum verification".into(),
            });
            self.health.report(&self.dataset, self.layer, &error);
            return Err(error);
        };
        Ok(Read::Found(Found {
            value: Some(value.to_vec()),
            revision: u64::from_be_bytes(header[..8].try_into().unwrap()),
        }))
    }

    async fn fill(&self, key: &K, found: &Found) -> StorageResult<()> {
        let (namespace, physical) = (self.locate)(key);
        let newest = format!("fill:{namespace}");
        let _fills = self.fills.lock().unwrap();
        if self
            .engine
            .revision(&newest)?
            .is_some_and(|seen| seen > found.revision)
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        match &found.value {
            Some(value) => {
                let revision = found.revision.to_be_bytes();
                let entry = [&revision[..], &checksum(&revision, value), value].concat();
                batch.put(physical, entry)
            }
            None => batch.delete(physical),
        };
        batch.set_revision(newest, found.revision);
        self.engine.write(batch)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/health/fallback.rs"]
mod tests;
