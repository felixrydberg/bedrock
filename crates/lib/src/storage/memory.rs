//! In-memory [`StorageEngine`] for tests and for simulation builds, where RocksDB is compiled out (B-23a).
//!
//! Only committed batches reach the engine. A simulated crash keeps the engine and drops the node's tasks, so it
//! loses exactly the batches that were still being built.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use super::{
    ErrorContext, KeyRange, Scan, Snapshot, StorageEngine, StorageError, StorageResult,
    StorageStats, WriteBatch, WriteOp,
};

const ENGINE: &str = "memory";

pub struct MemoryEngine {
    view: Mutex<View>,
}

/// The engine state, which doubles as its snapshot: cloning shares the maps and the next write copies them.
#[derive(Clone)]
struct View {
    instance: String,
    // `None` marks a record damaged by `MemoryEngine::corrupt`.
    data: Arc<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    revisions: Arc<BTreeMap<String, u64>>,
}

impl MemoryEngine {
    pub fn new(instance: impl Into<String>) -> Self {
        MemoryEngine {
            view: Mutex::new(View {
                instance: instance.into(),
                data: Arc::default(),
                revisions: Arc::default(),
            }),
        }
    }

    /// Fault injection: reads of `key` fail with `Corruption` until it is written or deleted again.
    pub fn corrupt(&self, key: &[u8]) {
        Arc::make_mut(&mut self.lock().data).insert(key.to_vec(), None);
    }

    fn lock(&self) -> MutexGuard<'_, View> {
        self.view.lock().unwrap()
    }
}

impl View {
    fn corruption(&self, key: &[u8]) -> StorageError {
        StorageError::Corruption(ErrorContext {
            engine: ENGINE,
            instance: self.instance.clone(),
            file: None,
            partition: Some(format!("key {}", key.escape_ascii())),
            detail: "record failed verification".into(),
        })
    }

    // ponytail: copies the range up front so no lock is held while the caller iterates; fine for tests and simulation.
    fn scan_owned(&self, range: &KeyRange) -> Scan<'static> {
        let entries: Vec<_> = range
            .bounds()
            .into_iter()
            .flat_map(|bounds| self.data.range::<[u8], _>(bounds))
            .map(|(key, value)| match value {
                Some(value) => Ok((key.clone(), value.clone())),
                None => Err(self.corruption(key)),
            })
            .collect();
        Box::new(entries.into_iter())
    }
}

impl Snapshot for View {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        match self.data.get(key) {
            None => Ok(None),
            Some(Some(value)) => Ok(Some(value.clone())),
            Some(None) => Err(self.corruption(key)),
        }
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        Ok(self.scan_owned(range))
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        Ok(self.revisions.get(name).copied())
    }

    fn revisions(&self) -> StorageResult<BTreeMap<String, u64>> {
        Ok((*self.revisions).clone())
    }
}

impl StorageEngine for MemoryEngine {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.lock().get(key)
    }

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>> {
        Ok(self.lock().scan_owned(range))
    }

    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        // Every step below is infallible and runs under the lock, so readers see all of the batch or none of it.
        let mut view = self.lock();
        let data = Arc::make_mut(&mut view.data);
        for op in batch.ops {
            match op {
                WriteOp::Put { key, value } => {
                    data.insert(key, Some(value));
                }
                WriteOp::Delete { key } => {
                    data.remove(&key);
                }
                // ponytail: O(all keys); split_off around the range if eviction tests get slow.
                WriteOp::DeleteRange(range) => data.retain(|key, _| !range.contains(key)),
            }
        }
        Arc::make_mut(&mut view.revisions).extend(batch.revisions);
        Ok(())
    }

    fn revision(&self, name: &str) -> StorageResult<Option<u64>> {
        self.lock().revision(name)
    }

    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>> {
        Ok(Box::new(self.lock().clone()))
    }

    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()> {
        // Read everything first, so a failing source leaves this engine untouched.
        let data = from
            .scan(&KeyRange::all())?
            .map(|entry| entry.map(|(key, value)| (key, Some(value))))
            .collect::<StorageResult<BTreeMap<_, _>>>()?;
        let revisions = from.revisions()?;
        let mut view = self.lock();
        view.data = Arc::new(data);
        view.revisions = Arc::new(revisions);
        Ok(())
    }

    fn stats(&self) -> StorageResult<StorageStats> {
        let view = self.lock();
        let live_bytes = view
            .data
            .iter()
            .map(|(key, value)| key.len() + value.as_ref().map_or(0, Vec::len))
            .sum::<usize>();
        Ok(StorageStats {
            keys: view.data.len() as u64,
            live_bytes: live_bytes as u64,
        })
    }

    fn disk_usage(&self) -> StorageResult<u64> {
        Err(StorageError::Unsupported(ErrorContext {
            engine: ENGINE,
            instance: self.lock().instance.clone(),
            file: None,
            partition: None,
            detail: "in-memory engine has no disk".into(),
        }))
    }

    fn health(&self) -> StorageResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/storage/memory.rs"]
mod tests;
