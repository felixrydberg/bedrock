//! The Foyer [`CacheStorage`]: Foyer's memory and disk tiers hold artifact bytes, and one small file per artifact under
//! `inventory/` lists them, since Foyer cannot list its keys. Foyer recovers its disk tier on open and the inventory is
//! reread, so both survive a restart.
//!
//! Foyer may drop an entry at any time (eviction, a full flush queue, a crash before its write); a miss then drops the
//! artifact from the inventory too. An entry Foyer kept whose inventory file was lost is still served, just not listed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use bedrock_lib::storage::{ErrorContext, StorageError, StorageResult};
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    PsyncIoEngineConfig, RecoverMode,
};

use super::{Body, CacheStorage, Hash, body, collect};

pub struct Foyer {
    cache: HybridCache<Vec<u8>, Bytes>,
    dir: PathBuf,
    inventory: Mutex<BTreeMap<Hash, u64>>,
}

fn failed(hash: Option<&Hash>, error: impl std::fmt::Display) -> StorageError {
    StorageError::IoError(ErrorContext {
        engine: "foyer",
        instance: "cdn".into(),
        file: None,
        partition: hash.map(|hash| format!("artifact {hash}")),
        detail: error.to_string(),
    })
}

impl Foyer {
    /// Opens the cache under `dir`, with `memory_mb` MiB in memory and `disk_mb` MiB on disk.
    // ponytail: Foyer's 16 MiB default block size bounds what reaches disk. Larger artifacts silently stay in memory
    // only, so they are refetched after eviction or a restart; raise the block size with the largest artifact.
    pub async fn open(dir: &Path, memory_mb: u64, disk_mb: u64) -> StorageResult<Self> {
        let inventory = dir.join("inventory");
        let data = dir.join("foyer");
        for dir in [&inventory, &data] {
            fs::create_dir_all(dir).map_err(|e| failed(None, e))?;
        }
        let device = FsDeviceBuilder::new(&data)
            .with_capacity((disk_mb << 20) as usize)
            .build()
            .map_err(|e| failed(None, e))?;
        let cache = HybridCacheBuilder::new()
            .with_name("cdn_artifacts")
            .memory((memory_mb << 20) as usize)
            .with_weighter(|_, value: &Bytes| value.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(device))
            .with_recover_mode(RecoverMode::Quiet)
            .build()
            .await
            .map_err(|e| failed(None, e))?;
        let mut listed = BTreeMap::new();
        for entry in fs::read_dir(&inventory).map_err(|e| failed(None, e))? {
            let path = entry.map_err(|e| failed(None, e))?.path();
            let record = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(Hash::parse);
            let size = fs::read_to_string(&path)
                .ok()
                .and_then(|size| size.parse().ok());
            match record.zip(size) {
                Some((hash, size)) => {
                    listed.insert(hash, size);
                }
                // A record torn by a crash; the artifact is refetched if Foyer lost it too.
                None => fs::remove_file(&path).map_err(|e| failed(None, e))?,
            }
        }
        Ok(Foyer {
            cache,
            dir: inventory,
            inventory: Mutex::new(listed),
        })
    }

    /// Flushes the memory tier to disk, so a reopened cache still has it.
    pub async fn close(&self) -> StorageResult<()> {
        self.cache.close().await.map_err(|e| failed(None, e))
    }

    fn forget(&self, hash: &Hash) -> StorageResult<()> {
        if self.inventory.lock().unwrap().remove(hash).is_some() {
            match fs::remove_file(self.dir.join(hash.to_string())) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    return Err(failed(Some(hash), e));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl CacheStorage for Foyer {
    async fn put(&self, hash: &Hash, body: Body) -> StorageResult<()> {
        let bytes = collect(body).await?;
        let size = bytes.len() as u64;
        self.cache.insert(hash.0.to_vec(), bytes);
        // ponytail: not fsynced; a crash loses only the listing, and Foyer may lose the bytes anyway.
        fs::write(self.dir.join(hash.to_string()), size.to_string())
            .map_err(|e| failed(Some(hash), e))?;
        self.inventory.lock().unwrap().insert(*hash, size);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> StorageResult<Option<Body>> {
        match self.cache.get(&hash.0[..]).await {
            Ok(Some(entry)) => Ok(Some(body(entry.value().clone()))),
            Ok(None) => {
                self.forget(hash)?;
                Ok(None)
            }
            Err(e) => Err(failed(Some(hash), e)),
        }
    }

    async fn contains(&self, hash: &Hash) -> StorageResult<bool> {
        Ok(self.cache.contains(&hash.0[..]))
    }

    async fn delete(&self, hash: &Hash) -> StorageResult<()> {
        self.forget(hash)?;
        self.cache.remove(&hash.0[..]);
        Ok(())
    }

    fn inventory(&self) -> StorageResult<Vec<(Hash, u64)>> {
        Ok(self
            .inventory
            .lock()
            .unwrap()
            .iter()
            .map(|(h, s)| (*h, *s))
            .collect())
    }

    fn usage(&self) -> StorageResult<u64> {
        Ok(self.inventory.lock().unwrap().values().sum())
    }
}
