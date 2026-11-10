//! Drain nodes, cells and single namespaces (#68 / B-21b, `feature-drafts.md` §4 decision 11).
//!
//! A drain is a [`Drain`] record at `drain/{id}` in the cell's membership namespace, changed by ordinary ordered writes;
//! deleting it cancels the drain. There is no membership or placement epoch: the record only changes what the desired state
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

use crate::placement::Directory;
use crate::replication::pb;

/// Drain records are keys of a cell's membership namespace `{service}.{cell}`: `drain/{id}`. A cell drain is written to
/// each ranged service's namespace in the cell (`docs/spec/membership.md`, Drain records).
pub const PREFIX: &str = "drain/";

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

    /// Whether this dataset can act on the requested target before any record is committed.
    fn validate(&self, scope: &Scope, namespace: Option<&str>) -> Result<(), Error>;

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

/// An L2 dataset placed in hash ranges (`quicksilver`, `cdn`): a node drain is the ranges it is still listed in,
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

    fn validate(&self, scope: &Scope, namespace: Option<&str>) -> Result<(), Error> {
        validate_ranged_scope(self.dataset, scope, namespace)
    }

    fn drain(&self, scope: &Scope, namespace: Option<&str>) -> Result<Outcome, Error> {
        self.validate(scope, namespace)?;
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

fn validate_ranged_scope(
    dataset: &str,
    scope: &Scope,
    namespace: Option<&str>,
) -> Result<(), Error> {
    if namespace.is_some() {
        return Err(unsupported(dataset, scope, namespace));
    }
    Ok(())
}

/// Validate the relay's supported ranged hooks before a control record is committed.
pub(crate) fn validate_ranged_selection(service: &str, drain: &Drain) -> Result<(), Error> {
    let available = || crate::datasets::capabilities::ranged_datasets(service);
    if available().next().is_none() {
        return Err(unsupported(
            "any dataset",
            &drain.scope,
            drain.namespace.as_deref(),
        ));
    }
    let mut selected = std::collections::BTreeSet::new();
    for dataset in &drain.datasets {
        if !selected.insert(dataset.as_str()) || !available().any(|allowed| allowed == dataset) {
            return Err(unsupported(
                dataset,
                &drain.scope,
                drain.namespace.as_deref(),
            ));
        }
    }
    for dataset in available().filter(|dataset| drain.selects(dataset)) {
        validate_ranged_scope(dataset, &drain.scope, drain.namespace.as_deref())?;
    }
    Ok(())
}

/// A dataset every node or cell holds whole and independently: nothing to move for a node or a cell, and one namespace
/// cannot be removed from it.
pub struct Everywhere(pub &'static str);

impl Hook for Everywhere {
    fn dataset(&self) -> &str {
        self.0
    }

    fn validate(&self, scope: &Scope, namespace: Option<&str>) -> Result<(), Error> {
        if namespace.is_some() {
            return Err(unsupported(self.0, scope, namespace));
        }
        Ok(())
    }

    fn drain(&self, scope: &Scope, namespace: Option<&str>) -> Result<Outcome, Error> {
        self.validate(scope, namespace)?;
        match namespace {
            None => Ok(Outcome::NothingToMove),
            Some(_) => unreachable!("validated above"),
        }
    }
}

/// `members` with those `drains` cover marked `draining`, so placement treats them as it does a node shutting down.
/// Every committed drain, including `Drained`, excludes its target from new placement. Only `Draining` records pace
/// handoffs, and a dataset applies a record only when it selects that dataset.
pub fn apply(
    members: &BTreeMap<String, pb::Member>,
    drains: &[Drain],
    dataset: &str,
) -> BTreeMap<String, pb::Member> {
    members
        .iter()
        .map(|(id, member)| {
            let mut member = member.clone();
            let drained = drains
                .iter()
                .any(|d| d.namespace.is_none() && d.selects(dataset) && d.covers(&member));
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
    validate(hooks, drain)?;
    let mut left = BTreeMap::new();
    for hook in hooks.iter().filter(|h| drain.selects(h.dataset())) {
        let units = match hook.drain(&drain.scope, drain.namespace.as_deref())? {
            Outcome::Moves(units) => units,
            Outcome::NothingToMove => 0,
        };
        left.insert(hook.dataset().to_owned(), units);
    }
    Ok(left)
}

/// Validates every selected dataset without reading its state.
pub fn validate(hooks: &[Arc<dyn Hook>], drain: &Drain) -> Result<(), Error> {
    let mut selected = BTreeMap::new();
    for hook in hooks.iter().filter(|h| drain.selects(h.dataset())) {
        hook.validate(&drain.scope, drain.namespace.as_deref())?;
        if selected.insert(hook.dataset(), ()).is_some() {
            return Err(Error::Corrupt(format!(
                "duplicate drain hook for {}",
                hook.dataset()
            )));
        }
    }
    let mut requested = std::collections::BTreeSet::new();
    for dataset in &drain.datasets {
        if !requested.insert(dataset.as_str()) {
            return Err(Error::Corrupt(format!("duplicate drain dataset {dataset}")));
        }
        if !selected.contains_key(dataset.as_str()) {
            return Err(unsupported(
                dataset,
                &drain.scope,
                drain.namespace.as_deref(),
            ));
        }
    }
    if selected.is_empty() {
        return Err(unsupported(
            "any dataset",
            &drain.scope,
            drain.namespace.as_deref(),
        ));
    }
    Ok(())
}

/// Whether the drain is complete: every selected dataset reports zero units left.
pub fn complete(left: &BTreeMap<String, u64>) -> bool {
    left.values().all(|units| *units == 0)
}

/// Returns the completed record only after every selected dataset reports zero units left.
pub fn completed(hooks: &[Arc<dyn Hook>], drain: &Drain) -> Result<Option<Drain>, Error> {
    if drain.state == State::Drained {
        return Ok(None);
    }
    let left = left(hooks, drain)?;
    if complete(&left) {
        let mut completed = drain.clone();
        completed.state = State::Drained;
        Ok(Some(completed))
    } else {
        Ok(None)
    }
}

/// Builds ranged hooks from a service-specific directory reader.
pub fn ranged_with(
    service: &str,
    directory: impl Fn(&str) -> Result<Directory, String> + Send + Sync + 'static,
) -> Vec<Arc<dyn Hook>> {
    let directory = Arc::new(directory);
    crate::datasets::capabilities::ranged_datasets(service)
        .map(|dataset| {
            let directory = directory.clone();
            Arc::new(Ranged::new(dataset, move || directory(dataset))) as Arc<dyn Hook>
        })
        .collect()
}

#[cfg(test)]
#[path = "../../tests/unit/reconciliation/drain.rs"]
mod tests;
