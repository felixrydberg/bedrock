//! Dataset → namespace model, policy resolution and per-dataset engine instances (B-06, `kv.md` §2–§4, §7).
//!
//! Each binary declares the datasets it hosts with their default policies; `lib` stays generic over datasets.
//! `bedrock-kv` hosts `quicksilver`, `kv` and `auth`, `bedrock-cdn` hosts `cdn`, and neither opens the other's.

use std::collections::BTreeMap;
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
    pub id: String,
    pub home_region: String,
    pub quotas: Quotas,
    // ponytail: opaque grants until Auth (B-37) defines their shape.
    pub permissions: Vec<String>,
    pub overrides: PolicyOverrides,
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
    /// Opens `enabled` datasets, each of which must be in `hosted`. A dataset hosted by another binary is a
    /// config error and nothing is opened.
    pub fn open(
        hosted: &[Dataset],
        enabled: &[String],
        mut open: impl FnMut(&Dataset) -> StorageResult<Arc<dyn StorageEngine>>,
    ) -> Result<Registry, ConfigError> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryEngine;
    use crate::storage::{ErrorContext, StorageError};

    fn dataset(name: &'static str) -> Dataset {
        Dataset {
            name,
            defaults: Policy {
                replication: ReplicationPolicy::Global,
                cache: CachePolicy {
                    l1_recent_window_seconds: 60,
                    l2_eviction: true,
                    pinned_prefixes: vec!["critical/".into()],
                },
                budget: Budget {
                    cpus: Some(2.0),
                    memory_mb: Some(1024),
                    disk_mb: None,
                },
            },
        }
    }

    #[test]
    fn dataset_defaults_apply_without_overrides() {
        let d = dataset("quicksilver");
        assert_eq!(
            resolve(&d.defaults, &PolicyOverrides::default(), &Budget::default()),
            d.defaults
        );
    }

    #[test]
    fn namespace_overrides_replace_defaults() {
        let d = dataset("kv");
        let overrides = PolicyOverrides {
            replication: Some(ReplicationPolicy::HomeOnly),
            l1_recent_window_seconds: Some(0),
            l2_eviction: Some(false),
            pinned_prefixes: Some(vec![]),
        };
        let p = resolve(&d.defaults, &overrides, &Budget::default());
        assert_eq!(p.replication, ReplicationPolicy::HomeOnly);
        assert_eq!(
            p.cache,
            CachePolicy {
                l1_recent_window_seconds: 0,
                l2_eviction: false,
                pinned_prefixes: vec![],
            }
        );
        assert_eq!(p.budget, d.defaults.budget);

        // A partial override keeps the other defaults.
        let p = resolve(
            &d.defaults,
            &PolicyOverrides {
                l2_eviction: Some(false),
                ..Default::default()
            },
            &Budget::default(),
        );
        assert!(!p.cache.l2_eviction);
        assert_eq!(p.replication, ReplicationPolicy::Global);
        assert_eq!(p.cache.pinned_prefixes, ["critical/"]);
    }

    #[test]
    fn node_budget_caps_dataset_budget() {
        let d = dataset("kv");
        let node = Budget {
            cpus: Some(4.0),
            memory_mb: Some(512),
            disk_mb: Some(10_000),
        };
        let p = resolve(&d.defaults, &PolicyOverrides::default(), &node);
        assert_eq!(
            p.budget,
            Budget {
                cpus: Some(2.0),
                memory_mb: Some(512),
                disk_mb: Some(10_000),
            }
        );
    }

    fn memory(d: &Dataset) -> StorageResult<Arc<dyn StorageEngine>> {
        Ok(Arc::new(MemoryEngine::new(d.name)))
    }

    #[test]
    fn opens_one_isolated_instance_per_enabled_dataset() {
        let hosted = [dataset("auth"), dataset("kv"), dataset("quicksilver")];
        let enabled = ["quicksilver".to_string(), "kv".into(), "kv".into()];
        let r = Registry::open(&hosted, &enabled, memory).unwrap();
        let names: Vec<_> = r.iter().map(|i| i.dataset.name).collect();
        assert_eq!(names, ["kv", "quicksilver"]);
        assert!(r.get("auth").is_none());

        let engine = |name| r.get(name).unwrap().engine.as_ref().unwrap();
        engine("kv").put(b"k", b"v").unwrap();
        assert_eq!(engine("quicksilver").get(b"k").unwrap(), None);
    }

    #[test]
    fn failing_instance_does_not_stop_others() {
        let hosted = [dataset("kv"), dataset("quicksilver")];
        let enabled = ["kv".to_string(), "quicksilver".into()];
        let r = Registry::open(&hosted, &enabled, |d| {
            if d.name == "kv" {
                return Err(StorageError::IoError(ErrorContext {
                    engine: "test",
                    instance: d.name.into(),
                    file: None,
                    partition: None,
                    detail: "disk gone".into(),
                }));
            }
            memory(d)
        })
        .unwrap();
        assert!(r.get("kv").unwrap().engine.is_err());
        assert!(r.get("quicksilver").unwrap().engine.is_ok());
    }

    #[test]
    fn rejects_dataset_hosted_elsewhere() {
        let mut opened = 0;
        let err = Registry::open(&[dataset("kv")], &["kv".to_string(), "cdn".into()], |d| {
            opened += 1;
            memory(d)
        });
        assert!(err.is_err());
        assert_eq!(opened, 0);
    }
}
