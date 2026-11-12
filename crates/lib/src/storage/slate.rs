//! Real SlateDB materialized namespace storage (#11 / B-11).
//!
//! Original events, resolved values, progress and retry records share one atomic batch.
//! A successful explicit remote flush is the standalone acknowledgement boundary.
//!
//! Open handles retain upstream manifest, compaction and GC timers. Automatic WAL flushing is
//! disabled; closing inactive handles ends their tasks. Removing all idle engine timers requires
//! a scoped upstream change rather than disabling compaction or pretending these tasks are absent.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions};
use slatedb::db_cache::DbCache;
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::{CloseReason, Db, ErrorKind, Settings, WriteBatch};
use tokio_stream::StreamExt;

use super::namespace::{
    Changes, NamespaceDatabase, NamespaceFactory, NamespaceReader, NamespaceScan, valid_identity,
};
use super::{Entry, ErrorContext, KeyRange, StorageError, StorageResult};

/// How often an open writer reads the manifest for compaction results and for a newer writer that fenced it. The handle
/// sees its own writes without polling and learns of fencing on its next write, so this only bounds how long a node that
/// lost a customer-KV namespace keeps serving its last view (#58 / B-26). SlateDB's default is 1 s.
// ponytail: fixed; tune from the stale-read and S3 request metrics.
pub const MANIFEST_POLL: Duration = Duration::from_secs(10);
/// The writer's memtable size before it flushes to L0, bounding each open handle's memory (`kv` `max_open`).
const WRITER_L0_BYTES: usize = 8 * 1024 * 1024;

pub struct SlateFactory {
    store: Arc<dyn ObjectStore>,
    /// Blocks, indexes and filters of every database the process opens, under one budget.
    cache: Arc<dyn DbCache>,
}

/// The shared block cache's default budget. Entries are allocated as they are filled.
pub const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;

impl SlateFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            cache: cache(DEFAULT_CACHE_BYTES),
        }
    }

    /// Bounds the block cache that all of this factory's databases share. Without one, each database would get
    /// SlateDB's own 512 MiB default.
    pub fn with_cache_bytes(mut self, bytes: u64) -> Self {
        self.cache = cache(bytes);
        self
    }
}

fn cache(bytes: u64) -> Arc<dyn DbCache> {
    Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: bytes,
        ..MokaCacheOptions::default()
    }))
}

/// One database's scope in the shared cache: stable across reopens, so a reopened handle finds its blocks again.
fn cache_scope(instance: &str) -> u64 {
    // FNV-1a. Cache keys also carry each SST's random ULID, so a scope collision cannot mix namespaces' blocks.
    instance.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn invalid_scope() -> StorageError {
    StorageError::Unsupported(context(
        "namespace",
        "dataset and namespace must be nonempty single path components".into(),
    ))
}

/// Reads of a writer's handle, filtered to what is durable in the store.
struct SlateDurable {
    db: Db,
    instance: String,
}

struct SlateDatabase {
    db: Db,
    instance: String,
    snapshot_lifetime: Duration,
}

struct SlateScan {
    // Pins historical versions; native compactor checkpoints protect the iterator's files.
    _snapshot: Arc<slatedb::DbSnapshot>,
    iter: slatedb::DbIterator,
    instance: String,
}

#[async_trait]
impl NamespaceScan for SlateScan {
    async fn next(&mut self) -> StorageResult<Option<Entry>> {
        self.iter
            .next()
            .await
            .map(|entry| entry.map(|entry| (entry.key.to_vec(), entry.value.to_vec())))
            .map_err(|error| engine_error(&self.instance, "snapshot next", error))
    }
}

fn context(instance: &str, detail: String) -> ErrorContext {
    ErrorContext {
        engine: "slatedb",
        instance: instance.into(),
        file: None,
        partition: None,
        detail,
    }
}

fn engine_error(instance: &str, operation: &str, error: slatedb::Error) -> StorageError {
    let kind = error.kind();
    // Public classifications retain the actionable cause without leaking backend URLs or secrets.
    let context = context(instance, format!("{operation}: {kind:?}"));
    match kind {
        ErrorKind::Closed(CloseReason::Fenced) => StorageError::Fenced(context),
        ErrorKind::Closed(_) => StorageError::Unavailable(ErrorContext {
            detail: format!("{operation}: engine closed; restart writer to recover ({kind:?})"),
            ..context
        }),
        ErrorKind::Data => StorageError::Corruption(context),
        ErrorKind::Unavailable => StorageError::Unavailable(context),
        _ => StorageError::IoError(context),
    }
}

#[async_trait]
impl NamespaceFactory for SlateFactory {
    async fn namespaces(&self) -> StorageResult<Vec<(String, String)>> {
        let mut scopes = BTreeSet::new();
        let listing_error =
            |_| StorageError::Unavailable(context("namespace", "namespace listing failed".into()));
        let mut datasets = self
            .store
            .list_with_delimiter(None)
            .await
            .map_err(listing_error)?
            .common_prefixes;
        datasets.sort();
        for dataset in datasets {
            let mut namespaces = self
                .store
                .list_with_delimiter(Some(&dataset))
                .await
                .map_err(listing_error)?
                .common_prefixes;
            namespaces.sort();
            for namespace in namespaces {
                let children = self
                    .store
                    .list_with_delimiter(Some(&namespace))
                    .await
                    .map_err(listing_error)?;
                if !children
                    .common_prefixes
                    .contains(&namespace.clone().join("state"))
                {
                    continue;
                }
                let dataset_name = dataset.as_ref();
                let namespace_name = namespace
                    .as_ref()
                    .strip_prefix(dataset_name)
                    .and_then(|suffix| suffix.strip_prefix('/'))
                    .unwrap_or_default();
                let invalid_identity = || {
                    StorageError::Corruption(context(
                        "namespace",
                        "invalid stored namespace identity".into(),
                    ))
                };
                let dataset_name = percent_encoding::percent_decode_str(dataset_name)
                    .decode_utf8()
                    .map_err(|_| invalid_identity())?;
                let namespace_name = percent_encoding::percent_decode_str(namespace_name)
                    .decode_utf8()
                    .map_err(|_| invalid_identity())?;
                if !valid_identity(&dataset_name) || !valid_identity(&namespace_name) {
                    return Err(invalid_identity());
                }
                scopes.insert((dataset_name.into_owned(), namespace_name.into_owned()));
            }
        }
        Ok(scopes.into_iter().collect())
    }

    async fn open(
        &self,
        dataset: &str,
        namespace: &str,
    ) -> StorageResult<Arc<dyn NamespaceDatabase>> {
        if !valid_identity(dataset) || !valid_identity(namespace) {
            return Err(StorageError::Unsupported(context(
                "namespace",
                "dataset and namespace must be nonempty single path components".into(),
            )));
        }
        let instance: String = format!("{dataset}/{namespace}");
        let settings = Settings {
            flush_interval: None,
            object_store_max_retries: Some(0),
            // Memtables flush at 8 MiB, and writes wait once 16 MiB is unflushed, bounding each open handle.
            l0_sst_size_bytes: WRITER_L0_BYTES,
            max_unflushed_bytes: 2 * WRITER_L0_BYTES,
            manifest_poll_interval: MANIFEST_POLL,
            ..Settings::default()
        };
        let snapshot_lifetime = settings
            .compactor_options
            .as_ref()
            .map_or(Duration::MAX, |options| options.checkpoint_lifetime);
        let db = Db::builder(format!("{instance}/state"), self.store.clone())
            .with_settings(settings)
            .with_db_cache(self.cache.clone(), cache_scope(&instance))
            .build()
            .await
            .map_err(|error| engine_error(&instance, "open", error))?;
        Ok(Arc::new(SlateDatabase {
            db,
            instance,
            snapshot_lifetime,
        }))
    }

    async fn destroy(&self, dataset: &str, namespace: &str) -> StorageResult<()> {
        if !valid_identity(dataset) || !valid_identity(namespace) {
            return Err(invalid_scope());
        }
        let instance = format!("{dataset}/{namespace}");
        let failed = |operation: &str| {
            StorageError::Unavailable(context(&instance, format!("destroy: {operation} failed")))
        };
        // A `Path` prefix matches whole segments, so `kv/ab` never lists `kv/abc/...`.
        let prefix = Path::from(dataset).join(namespace);
        let mut listing = self.store.list(Some(&prefix));
        while let Some(object) = listing.next().await {
            let object = object.map_err(|_| failed("listing"))?;
            match self.store.delete(&object.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(_) => return Err(failed("delete")),
            }
        }
        Ok(())
    }
}

fn durable_reads() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..ReadOptions::default()
    }
}

#[async_trait]
impl NamespaceReader for SlateDurable {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.db
            .get_with_options(key, &durable_reads())
            .await
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(|error| engine_error(&self.instance, "durable get", error))
    }

    async fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StorageResult<Vec<Entry>> {
        let range = KeyRange::prefix(prefix);
        let Some(bounds) = range.bounds_after(after) else {
            return Ok(Vec::new());
        };
        let options = ScanOptions {
            durability_filter: DurabilityLevel::Remote,
            ..ScanOptions::default()
        };
        let mut iter = self
            .db
            .scan_with_options(bounds, &options)
            .await
            .map_err(|error| engine_error(&self.instance, "durable scan", error))?;
        let mut entries = Vec::new();
        while entries.len() < limit
            && let Some(entry) = iter
                .next()
                .await
                .map_err(|error| engine_error(&self.instance, "durable scan next", error))?
        {
            entries.push((entry.key.to_vec(), entry.value.to_vec()));
        }
        Ok(entries)
    }
}

#[async_trait]
impl NamespaceDatabase for SlateDatabase {
    async fn snapshot(
        &self,
        prefix: &[u8],
        lifetime: Duration,
    ) -> StorageResult<Box<dyn NamespaceScan>> {
        if lifetime >= self.snapshot_lifetime {
            return Err(StorageError::Unsupported(context(
                &self.instance,
                "snapshot lifetime exceeds native compaction protection".into(),
            )));
        }
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(|error| engine_error(&self.instance, "snapshot", error))?;
        let range = KeyRange::prefix(prefix);
        let iter = snapshot
            .scan(range.bounds().expect("prefix range is never empty"))
            .await
            .map_err(|error| engine_error(&self.instance, "snapshot scan", error))?;
        Ok(Box::new(SlateScan {
            _snapshot: snapshot,
            iter,
            instance: self.instance.clone(),
        }))
    }

    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.db
            .get(key)
            .await
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(|error| engine_error(&self.instance, "get", error))
    }

    async fn scan(&self, prefix: &[u8]) -> StorageResult<Vec<Entry>> {
        let range = KeyRange::prefix(prefix);
        let mut iter = self
            .db
            .scan(range.bounds().expect("prefix range is never empty"))
            .await
            .map_err(|error| engine_error(&self.instance, "scan", error))?;
        let mut entries = Vec::new();
        while let Some(entry) = iter
            .next()
            .await
            .map_err(|error| engine_error(&self.instance, "scan next", error))?
        {
            entries.push((entry.key.to_vec(), entry.value.to_vec()));
        }
        Ok(entries)
    }

    async fn write(&self, changes: Changes) -> StorageResult<()> {
        if changes.is_empty() {
            return Ok(());
        }
        // SlateDB's batch builder panics on invalid lengths; reject before constructing any rows.
        if changes.iter().any(|(key, value)| {
            key.is_empty()
                || u32::try_from(key.len()).is_err()
                || value
                    .as_ref()
                    .is_some_and(|value| u32::try_from(value.len()).is_err())
        }) {
            return Err(StorageError::Unsupported(context(
                &self.instance,
                "SlateDB keys must be nonempty and keys/values must fit u32 lengths".into(),
            )));
        }
        let mut batch = WriteBatch::new();
        for (key, value) in changes {
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        self.db
            .write(batch)
            .await
            .map(|_| ())
            .map_err(|error| engine_error(&self.instance, "write", error))
    }

    async fn flush(&self) -> StorageResult<()> {
        self.db
            .flush()
            .await
            .map_err(|error| engine_error(&self.instance, "flush", error))
    }

    async fn close(&self) -> StorageResult<()> {
        self.db
            .close()
            .await
            .map_err(|error| engine_error(&self.instance, "close", error))
    }

    fn durable(&self) -> Arc<dyn NamespaceReader> {
        Arc::new(SlateDurable {
            db: self.db.clone(),
            instance: self.instance.clone(),
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/storage/slate.rs"]
mod tests;
