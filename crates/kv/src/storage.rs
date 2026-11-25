//! Customer-KV value caching (#126 / S-18). Cache misses read from the namespace actor.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use bedrock_lib::admin::Admin;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use tokio::time::Instant;

use crate::namespaces::{self, CACHE_TTL_SECONDS};

/// A namespace may fill at most this fraction of the process value cache.
const SHARE: u64 = 8;

struct Cached {
    value: Vec<u8>,
    revision: u64,
    expires: Instant,
}

impl Cached {
    fn bytes(key: &(String, Vec<u8>), value: &[u8]) -> u64 {
        (key.0.len() + key.1.len() + value.len()) as u64
    }
}

/// Encoded values returned by the namespace actor, under one process-wide byte budget.
pub struct ValueCache {
    budget: u64,
    inner: Mutex<CacheState>,
    bytes: Gauge,
    skipped: Counter,
}

struct CacheState {
    entries: lru::LruCache<(String, Vec<u8>), Cached>,
    total: u64,
    by_namespace: BTreeMap<String, u64>,
}

impl CacheState {
    fn remove(&mut self, key: &(String, Vec<u8>)) {
        if let Some(cached) = self.entries.pop(key) {
            self.forget(key, &cached);
        }
    }

    fn forget(&mut self, key: &(String, Vec<u8>), cached: &Cached) {
        let bytes = Cached::bytes(key, &cached.value);
        self.total -= bytes;
        let namespace = self
            .by_namespace
            .get_mut(&key.0)
            .expect("cached namespaces are counted");
        *namespace -= bytes;
        if *namespace == 0 {
            self.by_namespace.remove(&key.0);
        }
    }
}

impl ValueCache {
    pub fn new(admin: &Admin, budget: u64) -> Self {
        let bytes = Gauge::default();
        admin.register(
            "kv_value_cache_bytes",
            "Bytes in the value cache",
            bytes.clone(),
        );
        let skipped = Counter::default();
        admin.register(
            "kv_value_cache_skipped",
            "Values not cached because their namespace had used its share",
            skipped.clone(),
        );
        ValueCache {
            budget,
            inner: Mutex::new(CacheState {
                entries: lru::LruCache::unbounded(),
                total: 0,
                by_namespace: BTreeMap::new(),
            }),
            bytes,
            skipped,
        }
    }

    /// The cached value of `key` in `namespace` and its revision, if one is cached, unexpired and at least at
    /// `min_revision`. Absences are never cached, so a miss proves nothing.
    pub fn get(&self, namespace: &str, key: &[u8], min_revision: u64) -> Option<(Vec<u8>, u64)> {
        let mut state = self.inner.lock().unwrap();
        let id = (namespace.to_owned(), key.to_vec());
        let cached = state.entries.get(&id)?;
        if cached.expires <= bedrock_lib::monotonic_now() {
            state.remove(&id);
            self.bytes.set(state.total as i64);
            return None;
        }
        (cached.revision >= min_revision).then(|| (cached.value.clone(), cached.revision))
    }

    /// TTL starts when the source fetch began, rather than when a delayed response arrives.
    /// Older revisions never replace newer ones; each namespace stays within its share of the byte budget.
    pub fn fill_at(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        revision: u64,
        started: Instant,
    ) {
        let now = bedrock_lib::monotonic_now();
        let mut expires = started + Duration::from_secs(CACHE_TTL_SECONDS.into());
        if let Ok(stored) = <bedrock_lib::kv::Stored as prost::Message>::decode(value)
            && stored.expiration != 0
        {
            let remaining = stored
                .expiration
                .saturating_mul(1000)
                .saturating_sub(namespaces::wall_ms());
            expires = expires.min(now + Duration::from_millis(remaining));
        }
        if expires <= now {
            return;
        }
        let id = (namespace.to_owned(), key.to_vec());
        let size = Cached::bytes(&id, value);
        let share = self.budget / SHARE;
        let mut state = self.inner.lock().unwrap();
        if state
            .entries
            .peek(&id)
            .is_some_and(|cached| cached.revision > revision)
        {
            return;
        }
        state.remove(&id);
        let over = |state: &CacheState| {
            state.by_namespace.get(namespace).copied().unwrap_or(0) + size > share
        };
        if over(&state) {
            // Expired entries leave only when read or under budget pressure, so reclaim this namespace's before refusing.
            // ponytail: a scan of the whole cache, only for a namespace at its share; a per-namespace index if it shows.
            let now = bedrock_lib::monotonic_now();
            let expired: Vec<_> = state
                .entries
                .iter()
                .filter(|(key, cached)| key.0 == namespace && cached.expires <= now)
                .map(|(key, _)| key.clone())
                .collect();
            for key in expired {
                state.remove(&key);
            }
        }
        if over(&state) {
            self.skipped.inc();
            self.bytes.set(state.total as i64);
            return;
        }
        state.total += size;
        *state.by_namespace.entry(namespace.to_owned()).or_default() += size;
        state.entries.put(
            id,
            Cached {
                value: value.to_vec(),
                revision,
                expires,
            },
        );
        while state.total > self.budget {
            let (key, cached) = state
                .entries
                .pop_lru()
                .expect("over budget means not empty");
            state.forget(&key, &cached);
        }
        self.bytes.set(state.total as i64);
    }
}

#[cfg(test)]
#[path = "../tests/unit/storage.rs"]
mod tests;
