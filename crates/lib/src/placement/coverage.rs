//! `full_replica` completeness (#87 / B-40, `kv.md` §15.5): how much of a dataset's hash space a cell holds, and with
//! how many copies. An owner counts only if it [`holds`] its range, the rule a node serves by, and `holder` accepts it:
//! the cell relays pass members that are connected and whose storage for the dataset is healthy. A cell is degraded
//! until every range has its configured number of holders again.

use std::sync::atomic::AtomicU64;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;

use super::{Directory, end_of, holds};
use crate::admin::Admin;

/// The size of the 64-bit hash space.
pub const SPACE: u128 = 1 << 64;

/// Amounts of hash space, out of [`SPACE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coverage {
    /// Ranges with at least one holder: every key is still served in the cell.
    pub complete: u128,
    /// Ranges with at least the configured number of holders.
    pub redundant: u128,
}

impl Coverage {
    /// Degraded until rebuilding restores both completeness and redundancy.
    pub fn degraded(&self) -> bool {
        self.redundant < SPACE
    }
}

/// The coverage of `directory` by owners that hold their range and that `holder` accepts. Space no range covers, as in
/// an empty directory, counts as missing.
pub fn coverage(directory: &Directory, replicas: usize, holder: impl Fn(&str) -> bool) -> Coverage {
    let mut coverage = Coverage {
        complete: 0,
        redundant: 0,
    };
    for (start, owners) in directory {
        let end = match end_of(directory, *start) {
            0 => SPACE,
            end => u128::from(end),
        };
        let width = end - u128::from(*start);
        let held = owners
            .iter()
            .filter(|o| holds(o) && holder(&o.node))
            .count();
        if held > 0 {
            coverage.complete += width;
        }
        if held >= replicas.max(1) {
            coverage.redundant += width;
        }
    }
    coverage
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct Labels {
    dataset: String,
}

/// Completeness gauges per `full_replica` dataset. Per-range detail stays in the relay's directory introspection.
#[derive(Clone, Default)]
pub struct Report {
    complete: Family<Labels, Gauge<f64, AtomicU64>>,
    redundant: Family<Labels, Gauge<f64, AtomicU64>>,
    degraded: Family<Labels, Gauge>,
}

impl Report {
    pub fn register(admin: &Admin, cell: &str) -> Self {
        let report = Self::default();
        admin.register_labelled(vec![("cell", cell.into())], |registry| {
            registry.register(
                "full_replica_complete_ratio",
                "Share of the hash space with at least one connected, healthy ready or leaving owner",
                report.complete.clone(),
            );
            registry.register(
                "full_replica_redundant_ratio",
                "Share of the hash space with the configured number of connected, healthy ready or leaving owners",
                report.redundant.clone(),
            );
            registry.register(
                "full_replica_degraded",
                "1 until every range is complete and redundant again",
                report.degraded.clone(),
            );
        });
        report
    }

    pub fn set(&self, dataset: &str, coverage: Coverage) {
        let labels = Labels {
            dataset: dataset.to_owned(),
        };
        let ratio = |space: u128| space as f64 / SPACE as f64;
        self.complete
            .get_or_create(&labels)
            .set(ratio(coverage.complete));
        self.redundant
            .get_or_create(&labels)
            .set(ratio(coverage.redundant));
        self.degraded
            .get_or_create(&labels)
            .set(i64::from(coverage.degraded()));
    }
}

#[cfg(test)]
#[path = "../../tests/unit/placement/coverage.rs"]
mod tests;
