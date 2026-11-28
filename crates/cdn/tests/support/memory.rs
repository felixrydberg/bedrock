//! Artifacts in memory, as a cache or an origin, for tests and simulation.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bedrock_lib::storage::StorageResult;
use bytes::Bytes;

use super::{Body, CacheStorage, Hash, Origin, body, collect};

#[derive(Default)]
pub struct Memory(pub Mutex<BTreeMap<Hash, Bytes>>);

#[cfg(madsim)]
thread_local! {
    static SHARED: std::sync::Arc<Memory> = std::sync::Arc::default();
}

impl Memory {
    /// The bucket every node of this simulation shares, as they would share S3. Each seed runs on a fresh thread.
    #[cfg(madsim)]
    pub fn shared() -> std::sync::Arc<Self> {
        SHARED.with(Clone::clone)
    }

    /// Holding `artifacts`, each under its SHA-256.
    pub fn with(artifacts: &[&Bytes]) -> Self {
        let artifacts = artifacts.iter().map(|a| (Hash::of(a), (*a).clone()));
        Memory(Mutex::new(artifacts.collect()))
    }
}

#[async_trait]
impl Origin for Memory {
    async fn put(&self, hash: &Hash, bytes: Bytes) -> StorageResult<()> {
        self.0.lock().unwrap().insert(*hash, bytes);
        Ok(())
    }
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Bytes>> {
        Ok(self.0.lock().unwrap().get(hash).cloned())
    }
}

#[async_trait]
impl CacheStorage for Memory {
    async fn put(&self, hash: &Hash, body: Body) -> StorageResult<()> {
        let bytes = collect(body).await?;
        self.0.lock().unwrap().insert(*hash, bytes);
        Ok(())
    }
    async fn get(&self, hash: &Hash) -> StorageResult<Option<Body>> {
        Ok(self.0.lock().unwrap().get(hash).cloned().map(body))
    }
    async fn contains(&self, hash: &Hash) -> StorageResult<bool> {
        Ok(self.0.lock().unwrap().contains_key(hash))
    }
    async fn delete(&self, hash: &Hash) -> StorageResult<()> {
        self.0.lock().unwrap().remove(hash);
        Ok(())
    }
    fn inventory(&self) -> StorageResult<Vec<(Hash, u64)>> {
        let artifacts = self.0.lock().unwrap();
        Ok(artifacts
            .iter()
            .map(|(h, b)| (*h, b.len() as u64))
            .collect())
    }
    fn usage(&self) -> StorageResult<u64> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum())
    }
}
