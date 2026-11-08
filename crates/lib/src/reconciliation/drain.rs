//! Drain nodes, cells and single namespaces (#68 / B-21b, `feature-drafts.md` §4 decision 11).
//!
//! A drain is a [`Drain`] record at `topology/drains/{id}` in Quicksilver, changed by ordinary ordered writes; deleting
//! it cancels the drain. There is no membership or placement epoch: the record only changes what the desired state
//! excludes. Each dataset's [`Hook`] says what that means for the units it holds on the target: moves still needed,
//! nothing to move, or an explicit [`Error::Unsupported`], never a silent no-op.
//!
//! - **Placement.** [`apply`] marks drained members `draining`, so range assignment gives them nothing new and
//!   hands their ranges off ([`crate::placement::assign`], #22 / B-21). Customer-KV ranking already skips them.
//! - **Progress.** [`left`] sums the hooks' units; the drain is complete when every selected dataset reports zero.
//! - **Pace.** [`Drain::rate`] bounds the ranges in handoff at once ([`crate::membership::place`]).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::membership::{self, View};
use crate::placement::Directory;
use crate::replication::pb;

/// Drain records are keys of Quicksilver's `infrastructure` namespace: `topology/drains/{id}`.
pub const PREFIX: &str = "topology/drains/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Node(String),
    Cell(String),
}

impl Scope {
    /// The label value for metrics: kind only, never the target.
    pub fn kind(&self) -> &'static str {
        match self {
            Scope::Node(_) => "node",
            Scope::Cell(_) => "cell",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Draining,
    /// Every selected dataset reported zero units left; the node can stop or the cell be removed.
    Drained,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drain {
    pub scope: Scope,
    /// Empty is every dataset.
    pub datasets: Vec<String>,
    /// Narrows the drain to one namespace.
    pub namespace: Option<String>,
    pub state: State,
    /// Most ranges in handoff at once while this drain runs, at least 1.
    pub rate: u32,
}

impl Drain {
    pub fn key(id: &str) -> String {
        format!("{PREFIX}{id}")
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a drain record serializes")
    }

    /// A record that does not decode is corruption, never a missing drain.
    pub fn decode(value: &[u8]) -> Result<Self, Error> {
        let drain: Drain =
            serde_json::from_slice(value).map_err(|e| Error::Corrupt(e.to_string()))?;
        if drain.rate == 0 {
            return Err(Error::Corrupt("drain rate must be at least 1".into()));
        }
        Ok(drain)
    }

    pub fn selects(&self, dataset: &str) -> bool {
        self.datasets.is_empty() || self.datasets.iter().any(|d| d == dataset)
    }

    fn covers(&self, member: &pb::Member) -> bool {
        match &self.scope {
            Scope::Node(node) => member.node_id == *node,
            Scope::Cell(cell) => member.cell == *cell,
        }
    }
}

/// What a dataset's hook reports for a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// This many units still sit on the target and need to move or be released.
    Moves(u64),
    NothingToMove,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Unsupported {
        dataset: String,
        scope: &'static str,
        namespace: Option<String>,
    },
    Unavailable(String),
    Corrupt(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Unsupported {
                dataset,
                scope,
                namespace,
            } => write!(
                f,
                "drain of a {scope}{} is unsupported for {dataset}",
                namespace
                    .as_ref()
                    .map_or(String::new(), |n| format!(" namespace {n}"))
            ),
            Error::Unavailable(detail) => write!(f, "drain state unavailable: {detail}"),
            Error::Corrupt(detail) => write!(f, "corrupt drain record: {detail}"),
        }
    }
}

/// One dataset's meaning of a drain, implemented beside the dataset.
pub trait Hook: Send + Sync {
    fn dataset(&self) -> &str;

    /// The units still on the target for `scope`, narrowed to `namespace` when given.
    fn drain(&self, scope: &Scope, namespace: Option<&str>) -> Result<Outcome, Error>;
}

/// `dataset` is unsupported for `scope` with `namespace`.
pub fn unsupported(dataset: &str, scope: &Scope, namespace: Option<&str>) -> Error {
    Error::Unsupported {
        dataset: dataset.into(),
        scope: scope.kind(),
        namespace: namespace.map(Into::into),
    }
}

/// An L2 dataset placed in hash ranges (`quicksilver`, `auth`, `cdn`): a node drain is the ranges it is still listed in,
/// a cell drain has nothing to move because every cell holds its own copy, and one namespace cannot leave a cell
/// because a range covers all of the dataset's namespaces.
pub struct Ranged {
    dataset: &'static str,
    /// The dataset's published directory.
    directory: Box<dyn Fn() -> Result<Directory, String> + Send + Sync>,
}

impl Ranged {
    pub fn new(
        dataset: &'static str,
        directory: impl Fn() -> Result<Directory, String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            dataset,
            directory: Box::new(directory),
        }
    }
}

impl Hook for Ranged {
    fn dataset(&self) -> &str {
        self.dataset
    }

    fn drain(&self, scope: &Scope, namespace: Option<&str>) -> Result<Outcome, Error> {
        if namespace.is_some() {
            return Err(unsupported(self.dataset, scope, namespace));
        }
        let Scope::Node(node) = scope else {
            return Ok(Outcome::NothingToMove);
        };
        let directory = (self.directory)().map_err(Error::Unavailable)?;
        let left = directory
            .values()
            .filter(|owners| owners.iter().any(|o| o.node == *node))
            .count() as u64;
        Ok(if left == 0 {
            Outcome::NothingToMove
        } else {
            Outcome::Moves(left)
        })
    }
}

/// A dataset every node or cell holds whole and independently: nothing to move for a node or a cell, and one namespace
/// cannot be removed from it.
pub struct Everywhere(pub &'static str);

impl Hook for Everywhere {
    fn dataset(&self) -> &str {
        self.0
    }

    fn drain(&self, scope: &Scope, namespace: Option<&str>) -> Result<Outcome, Error> {
        match namespace {
            Some(_) => Err(unsupported(self.0, scope, namespace)),
            None => Ok(Outcome::NothingToMove),
        }
    }
}

/// `members` with those `drains` cover marked `draining`, so placement treats them as it does a node shutting down.
/// Only drains still in progress count, and only for a dataset the member carries when the drain selects datasets.
pub fn apply(
    members: &BTreeMap<String, pb::Member>,
    drains: &[Drain],
    dataset: &str,
) -> BTreeMap<String, pb::Member> {
    members
        .iter()
        .map(|(id, member)| {
            let mut member = member.clone();
            let drained = drains.iter().any(|d| {
                d.state == State::Draining
                    && d.namespace.is_none()
                    && d.selects(dataset)
                    && d.covers(&member)
            });
            if drained {
                member.status = pb::member::Status::Draining as i32;
            }
            (id.clone(), member)
        })
        .collect()
}

/// The ranges in handoff at once under `drains`: the smallest `rate` among them, never above `limit`.
pub fn moving(drains: &[Drain], limit: usize) -> usize {
    drains
        .iter()
        .filter(|d| d.state == State::Draining)
        .map(|d| d.rate as usize)
        .fold(limit, usize::min)
}

/// Units left per selected dataset. A selected dataset without a hook is `Unsupported`, as is any the hook refuses.
pub fn left(hooks: &[Arc<dyn Hook>], drain: &Drain) -> Result<BTreeMap<String, u64>, Error> {
    let mut left = BTreeMap::new();
    for hook in hooks.iter().filter(|h| drain.selects(h.dataset())) {
        let units = match hook.drain(&drain.scope, drain.namespace.as_deref())? {
            Outcome::Moves(units) => units,
            Outcome::NothingToMove => 0,
        };
        left.insert(hook.dataset().to_owned(), units);
    }
    for dataset in &drain.datasets {
        if !left.contains_key(dataset) {
            return Err(unsupported(
                dataset,
                &drain.scope,
                drain.namespace.as_deref(),
            ));
        }
    }
    Ok(left)
}

/// Whether the drain is complete: every selected dataset reports zero units left.
pub fn complete(left: &BTreeMap<String, u64>) -> bool {
    left.values().all(|units| *units == 0)
}

/// The hash-range datasets a service places, for building its [`Ranged`] hooks.
pub fn ranged(service: &str, view: &Arc<View>) -> Vec<Arc<dyn Hook>> {
    membership::ranged_datasets(service)
        .iter()
        .map(|&dataset| {
            let view = view.clone();
            let directory = move || view.directory(dataset).map_err(|e| e.to_string());
            Arc::new(Ranged::new(dataset, directory)) as Arc<dyn Hook>
        })
        .collect()
}

#[cfg(test)]
#[path = "../../tests/unit/reconciliation/drain.rs"]
mod tests;
