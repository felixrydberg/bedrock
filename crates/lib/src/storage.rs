//! Local storage contract shared by every engine adapter (B-04, `kv.md` §6).
//!
//! Replication, caches, relays, recovery and S3 persistence use [`StorageEngine`] and never an adapter directly.
//! Each dataset gets its own engine instance per node (`kv.md` §7).
//!
//! Applied replication revisions live beside the data, outside the key space, and are committed in the same
//! [`WriteBatch`] as the data they describe, so after a crash both are either there or not.

pub mod memory;

#[cfg(test)]
mod contract;

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound;

pub type StorageResult<T> = Result<T, StorageError>;

/// A stored key and value.
pub type Entry = (Vec<u8>, Vec<u8>);

/// Entries in key order. A record that cannot be read yields an error item instead of being skipped.
pub type Scan<'a> = Box<dyn Iterator<Item = StorageResult<Entry>> + 'a>;

/// Key bounds as `BTreeMap::range` takes them.
pub type Bounds<'a> = (Bound<&'a [u8]>, Bound<&'a [u8]>);

/// A storage failure. A missing key is not one: reads return `Ok(None)`.
///
/// A miss falls back to other sources. `Corruption` must also reach storage-health handling (B-14), because the
/// local copy can no longer be trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    IoError(ErrorContext),
    Corruption(ErrorContext),
    Unavailable(ErrorContext),
    /// The adapter does not provide this capability. Adapters report it rather than approximate it.
    Unsupported(ErrorContext),
}

/// Where a storage error happened, as far as the adapter can tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorContext {
    /// Adapter name, e.g. `memory` or `rocksdb`.
    pub engine: &'static str,
    /// Engine instance, normally the dataset it stores.
    pub instance: String,
    pub file: Option<String>,
    /// Affected key or key range, when the adapter can narrow it down.
    pub partition: Option<String>,
    pub detail: String,
}

impl StorageError {
    pub fn context(&self) -> &ErrorContext {
        match self {
            StorageError::IoError(c)
            | StorageError::Corruption(c)
            | StorageError::Unavailable(c)
            | StorageError::Unsupported(c) => c,
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            StorageError::IoError(_) => "I/O error",
            StorageError::Corruption(_) => "corruption",
            StorageError::Unavailable(_) => "unavailable",
            StorageError::Unsupported(_) => "unsupported",
        };
        let c = self.context();
        write!(f, "{kind} in {} instance {}", c.engine, c.instance)?;
        if let Some(file) = &c.file {
            write!(f, ", file {file}")?;
        }
        if let Some(partition) = &c.partition {
            write!(f, ", partition {partition}")?;
        }
        write!(f, ": {}", c.detail)
    }
}

impl std::error::Error for StorageError {}

/// Keys from `start` (inclusive) to `end` (exclusive; `None` is unbounded). A range with `end <= start` is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        KeyRange {
            start: start.into(),
            end: Some(end.into()),
        }
    }

    pub fn all() -> Self {
        KeyRange {
            start: Vec::new(),
            end: None,
        }
    }

    /// Every key that starts with `prefix`.
    pub fn prefix(prefix: impl Into<Vec<u8>>) -> Self {
        let start = prefix.into();
        // The first key after the prefix: drop trailing 0xFF bytes and increment the last remaining byte.
        let mut end = start.clone();
        while end.pop_if(|b| *b == 0xFF).is_some() {}
        let end = match end.last_mut() {
            Some(last) => {
                *last += 1;
                Some(end)
            }
            None => None,
        };
        KeyRange { start, end }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_deref().is_none_or(|end| key < end)
    }

    /// Bounds for `BTreeMap::range`, or `None` when the range is empty (where `range` would panic).
    pub fn bounds(&self) -> Option<Bounds<'_>> {
        match self.end.as_deref() {
            Some(end) if end <= self.start.as_slice() => None,
            end => Some((
                Bound::Included(self.start.as_slice()),
                end.map_or(Bound::Unbounded, Bound::Excluded),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Namespace eviction and other bulk removal. Revisions are not keys and stay; set them in the same batch.
    DeleteRange(KeyRange),
}

/// Writes committed together by [`StorageEngine::write`]. Operations apply in order; nothing is visible or durable
/// until the batch commits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    pub ops: Vec<WriteOp>,
    /// Applied replication revision per name (namespace or stream), set together with the data.
    pub revisions: BTreeMap<String, u64>,
}

impl WriteBatch {
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(WriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(WriteOp::Delete { key: key.into() });
        self
    }

    pub fn delete_range(&mut self, range: KeyRange) -> &mut Self {
        self.ops.push(WriteOp::DeleteRange(range));
        self
    }

    pub fn set_revision(&mut self, name: impl Into<String>, revision: u64) -> &mut Self {
        self.revisions.insert(name.into(), revision);
        self
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageStats {
    /// Stored keys; adapters may estimate.
    pub keys: u64,
    /// Key plus value bytes before engine overhead; adapters may estimate.
    pub live_bytes: u64,
}

/// A point-in-time read view. Writes committed after it was taken are not visible through it.
pub trait Snapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;
    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>>;
    fn revision(&self, name: &str) -> StorageResult<Option<u64>>;
    fn revisions(&self) -> StorageResult<BTreeMap<String, u64>>;
}

/// One local storage instance. Adapters pass the contract suite in `storage/contract.rs`.
///
/// Calls are synchronous and short; once `write` or `restore` returns `Ok`, the change survives a crash.
pub trait StorageEngine: Send + Sync {
    /// `Ok(None)` when the key is not stored. Never an error.
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    fn scan(&self, range: &KeyRange) -> StorageResult<Scan<'_>>;

    /// Atomically commits every operation and revision in `batch`, or none of them.
    fn write(&self, batch: WriteBatch) -> StorageResult<()>;

    /// The applied revision last committed for `name`, or `None` if none was.
    fn revision(&self, name: &str) -> StorageResult<Option<u64>>;

    fn snapshot(&self) -> StorageResult<Box<dyn Snapshot + '_>>;

    /// Atomically replaces all data and revisions with those of `from`. On error the engine is unchanged.
    fn restore(&self, from: &dyn Snapshot) -> StorageResult<()>;

    fn stats(&self) -> StorageResult<StorageStats>;

    /// Bytes on disk, or `Unsupported` for engines without a disk.
    fn disk_usage(&self) -> StorageResult<u64>;

    /// `Ok` when the instance can serve reads and writes; otherwise the error that stops it.
    fn health(&self) -> StorageResult<()>;

    fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let mut batch = WriteBatch::default();
        batch.put(key, value);
        self.write(batch)
    }

    fn delete(&self, key: &[u8]) -> StorageResult<()> {
        let mut batch = WriteBatch::default();
        batch.delete(key);
        self.write(batch)
    }

    fn delete_range(&self, range: KeyRange) -> StorageResult<()> {
        let mut batch = WriteBatch::default();
        batch.delete_range(range);
        self.write(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_range_end() {
        assert_eq!(KeyRange::prefix(*b"ab").end, Some(b"ac".to_vec()));
        assert_eq!(KeyRange::prefix([b'a', 0xFF]).end, Some(b"b".to_vec()));
        assert_eq!(KeyRange::prefix([0xFF, 0xFF]).end, None);
        assert_eq!(KeyRange::prefix(Vec::new()), KeyRange::all());

        let r = KeyRange::prefix([b'a', 0xFF]);
        assert!(r.contains(&[b'a', 0xFF]) && r.contains(&[b'a', 0xFF, 0xFF, 7]));
        assert!(!r.contains(b"a") && !r.contains(b"b"));
    }

    #[test]
    fn inverted_range_is_empty() {
        assert!(KeyRange::new(*b"b", *b"a").bounds().is_none());
        assert!(KeyRange::new(*b"a", *b"a").bounds().is_none());
        assert!(!KeyRange::new(*b"b", *b"a").contains(b"b"));
    }
}
