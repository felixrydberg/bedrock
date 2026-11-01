//! Source selection on a cache miss (#20 / B-19). Each node chooses from its own view of its cell: its service's members
//! and placement directory ([`crate::membership::View`]), its circuit breakers (#12 / B-13) and the revisions holders
//! reported when they turned down a read. Relays stay off the read path.
//!
//! Freshness is checked by the holder when it serves. A holder behind the requested minimum revision rejects the read
//! with its applied revision; the caller reports it through [`Sources::behind`] and tries the next candidate. A report
//! only saves later callers that round trip and never proves a holder fresh. Customer KV has no ranges: [`readers`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use crate::health::Health;
use crate::placement::{self, Directory, State};
use crate::replication::pb;

/// How long a holder's reported revision keeps it out of reads above that revision; then it is tried again.
pub const RECHECK: Duration = Duration::from_secs(1);

/// `(node, dataset, namespace)` → applied revision the holder reported, and when.
type Behind = BTreeMap<(String, String, String), (u64, Instant)>;

/// Holders that recently turned down a read for being behind.
#[derive(Default)]
pub struct Sources {
    behind: Mutex<Behind>,
}

impl Sources {
    /// `node` rejected a read of `dataset`/`namespace` because it has only applied `applied`.
    pub fn behind(&self, node: &str, dataset: &str, namespace: &str, applied: u64) {
        let now = crate::monotonic_now();
        let mut behind = self.behind.lock().unwrap();
        behind.retain(|_, (_, at)| now.duration_since(*at) < RECHECK);
        behind.insert(
            (node.into(), dataset.into(), namespace.into()),
            (applied, now),
        );
    }

    /// Holders of `hash` in a ranged service (`quicksilver`, `cdn`), best first: `ready` owners before `leaving` or
    /// draining ones, then those in the caller's failure `domain`. Never `populating` owners, owners that are no longer
    /// members, or holders that reported being behind `min_revision` within [`RECHECK`]. Open breakers are skipped
    /// unless every candidate's is open ([`Health::route`] fails open). Keys hash with
    /// [`crate::buckets::hash_of`]; CDN artifacts use their content hash and minimum revision 0.
    #[allow(clippy::too_many_arguments)]
    pub fn select<'a>(
        &self,
        members: &'a BTreeMap<String, pb::Member>,
        directory: &Directory,
        health: &Health,
        domain: &str,
        dataset: &str,
        namespace: &str,
        hash: u64,
        min_revision: u64,
    ) -> Vec<&'a pb::Member> {
        let now = crate::monotonic_now();
        let behind = self.behind.lock().unwrap();
        let mut candidates: Vec<(&State, &pb::Member)> = placement::owners(directory, hash)
            .iter()
            .filter(|o| o.state != State::Populating)
            .filter(|o| {
                let key = (o.node.clone(), dataset.to_owned(), namespace.to_owned());
                behind.get(&key).is_none_or(|(applied, at)| {
                    *applied >= min_revision || now.duration_since(*at) >= RECHECK
                })
            })
            .filter_map(|o| Some((&o.state, members.get(&o.node)?)))
            .collect();
        drop(behind);
        // Stable, so equal candidates keep directory order.
        candidates.sort_by_key(|(state, m)| {
            (
                **state == State::Leaving || m.status != pb::member::Status::Ready as i32,
                domain.is_empty() || m.failure_domain != domain,
            )
        });
        healthy(health, candidates.into_iter().map(|(_, m)| m).collect())
    }
}

/// Customer-KV readers for `hash` (`hash_of("kv", namespace, key)`), preferred first: rendezvous order over ready
/// members ([`placement::rank`]), skipping open breakers. Draining members get no new reads. Every reader may serve: it
/// proves freshness from its own namespace view when serving, so none is excluded by revision.
pub fn readers<'a>(
    members: impl IntoIterator<Item = &'a pb::Member>,
    health: &Health,
    hash: u64,
) -> Vec<&'a pb::Member> {
    let ready = members
        .into_iter()
        .filter(|m| m.status == pb::member::Status::Ready as i32);
    healthy(health, placement::rank(hash, ready))
}

/// `candidates` in order without open breakers, or all of them when every breaker is open.
fn healthy<'a>(health: &Health, candidates: Vec<&'a pb::Member>) -> Vec<&'a pb::Member> {
    let addresses: Vec<&str> = candidates.iter().map(|m| m.address.as_str()).collect();
    let usable: BTreeSet<&str> = health.route(&addresses).into_iter().copied().collect();
    candidates
        .into_iter()
        .filter(|m| usable.contains(m.address.as_str()))
        .collect()
}

#[cfg(test)]
#[path = "../../tests/unit/relays/sources.rs"]
mod tests;
