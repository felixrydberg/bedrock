//! Dataset → namespace model, policy resolution and per-dataset engine instances (B-06, `kv.md` §2–§4, §7).
//!
//! Each binary declares the datasets it hosts with their default policies; `lib` stays generic over datasets.
//! `bedrock-quicksilver` hosts `quicksilver` and `auth`, `bedrock-cdn` hosts `cdn`, and neither opens the other's.
//! `bedrock-kv` (customer KV) hosts no relay-fed dataset.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::node::{Budget, ConfigError};
use crate::storage::{StorageEngine, StorageResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationPolicy {
    /// Every eligible datacenter receives updates; L2 holds the working set and may evict.
    Global,
    /// Only regions with active subscribers receive updates.
    OnDemand,
    /// Stays in the home region; remote reads are forwarded there.
    HomeOnly,
    /// Every eligible datacenter retains the complete namespace.
    FullReplica,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachePolicy {
    /// How long L1 keeps recent updates; 0 disables the window.
    pub l1_recent_window_seconds: u64,
    pub l2_eviction: bool,
    /// Keys with these prefixes are protected from normal eviction.
    pub pinned_prefixes: Vec<String>,
}

/// The policy a dataset defaults to, and after [`resolve`] the policy in effect for one namespace on this node.
#[derive(Debug, Clone, PartialEq)]
pub struct Policy {
    pub replication: ReplicationPolicy,
    pub cache: CachePolicy,
    /// Budget of the dataset's engine instance; unset means no limit.
    pub budget: Budget,
}

/// What a namespace may change about its dataset's policy. The instance budget belongs to the dataset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyOverrides {
    pub replication: Option<ReplicationPolicy>,
    pub l1_recent_window_seconds: Option<u64>,
    pub l2_eviction: Option<bool>,
    pub pinned_prefixes: Option<Vec<String>>,
}

/// Effective policy: dataset defaults, then namespace overrides, then the node's resource budget, which caps the
/// dataset budget.
pub fn resolve(defaults: &Policy, overrides: &PolicyOverrides, node: &Budget) -> Policy {
    let o = overrides;
    let d = &defaults.cache;
    Policy {
        replication: o.replication.unwrap_or(defaults.replication),
        cache: CachePolicy {
            l1_recent_window_seconds: o
                .l1_recent_window_seconds
                .unwrap_or(d.l1_recent_window_seconds),
            l2_eviction: o.l2_eviction.unwrap_or(d.l2_eviction),
            pinned_prefixes: o
                .pinned_prefixes
                .clone()
                .unwrap_or_else(|| d.pinned_prefixes.clone()),
        },
        budget: Budget {
            cpus: cap(defaults.budget.cpus, node.cpus),
            memory_mb: cap(defaults.budget.memory_mb, node.memory_mb),
            disk_mb: cap(defaults.budget.disk_mb, node.disk_mb),
        },
    }
}

/// The smaller limit; `None` is no limit.
fn cap<T: PartialOrd>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if b < a { b } else { a }),
        (a, b) => a.or(b),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub name: &'static str,
    pub defaults: Policy,
}

/// An isolated store inside a dataset. Namespaces share their dataset's engine instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    id: String,
    pub home_region: String,
    pub quotas: Quotas,
    // ponytail: opaque grants until Auth (B-37) defines their shape.
    pub permissions: Vec<String>,
    pub overrides: PolicyOverrides,
}

impl Namespace {
    /// Creates a namespace whose ID cannot contain the keyspace separator, NUL.
    pub fn new(id: impl Into<String>, home_region: impl Into<String>) -> Result<Self, ConfigError> {
        let id = id.into();
        if id.contains('\0') {
            return Err(ConfigError("namespace ID contains NUL".into()));
        }
        Ok(Self {
            id,
            home_region: home_region.into(),
            quotas: Quotas::default(),
            permissions: Vec::new(),
            overrides: PolicyOverrides::default(),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Unset means no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Quotas {
    pub storage_bytes: Option<u64>,
    pub api_requests_per_second: Option<u64>,
}

/// One engine instance per enabled dataset hosted by this process. The relay's replay instance is not in here.
pub struct Registry {
    instances: BTreeMap<&'static str, Instance>,
}

pub struct Instance {
    pub dataset: Dataset,
    /// A dataset whose engine failed to open keeps its error here; the other datasets still open.
    pub engine: StorageResult<Arc<dyn StorageEngine>>,
}

impl Registry {
    /// Opens `enabled` datasets, each of which must be in `hosted`. An empty list, or a dataset hosted by another
    /// binary, is a config error and nothing is opened.
    pub fn open(
        hosted: &[Dataset],
        enabled: &[String],
        mut open: impl FnMut(&Dataset) -> StorageResult<Arc<dyn StorageEngine>>,
    ) -> Result<Registry, ConfigError> {
        if enabled.is_empty() {
            return Err(ConfigError("datasets must not be empty".into()));
        }
        let datasets = enabled
            .iter()
            .map(|name| {
                hosted.iter().find(|d| d.name == name).ok_or_else(|| {
                    let names: Vec<_> = hosted.iter().map(|d| d.name).collect();
                    ConfigError(format!(
                        "dataset {name:?} is not hosted by this binary (hosted: {names:?})"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut instances = BTreeMap::new();
        for dataset in datasets {
            instances.entry(dataset.name).or_insert_with(|| {
                let engine = open(dataset);
                if let Err(error) = &engine {
                    tracing::error!(dataset = dataset.name, %error, "storage engine failed to open");
                }
                Instance {
                    dataset: dataset.clone(),
                    engine,
                }
            });
        }
        Ok(Registry { instances })
    }

    /// `None` when this process does not have `dataset` enabled.
    pub fn get(&self, dataset: &str) -> Option<&Instance> {
        self.instances.get(dataset)
    }

    /// Instances in dataset name order.
    pub fn iter(&self) -> impl Iterator<Item = &Instance> {
        self.instances.values()
    }

    /// Ready only when every enabled dataset opened successfully and can serve reads and writes.
    pub fn is_healthy(&self) -> bool {
        self.iter().all(|instance| {
            instance
                .engine
                .as_ref()
                .is_ok_and(|engine| engine.health().is_ok())
        })
    }
}

/// The node's engine for `dataset`: RocksDB in `data_dir/{dataset}`, or in memory in simulation builds, which compile
/// RocksDB out (B-23a).
pub fn open_engine(data_dir: &Path, dataset: &Dataset) -> StorageResult<Arc<dyn StorageEngine>> {
    // ponytail: RocksDB defaults; map the resolved dataset budget onto `RocksDbOptions` when budgets are enforced.
    #[cfg(not(madsim))]
    let engine = crate::storage::rocksdb::RocksDbAdapter::open(
        &data_dir.join(dataset.name),
        dataset.name,
        &Default::default(),
    )?;
    #[cfg(madsim)]
    let engine = {
        let _ = data_dir;
        crate::storage::memory::MemoryEngine::new(dataset.name)
    };
    Ok(Arc::new(engine))
}

#[cfg(test)]
#[path = "../tests/unit/datasets.rs"]
mod tests;
