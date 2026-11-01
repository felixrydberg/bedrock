//! L2 range assignment and customer-KV reader ranking (#18 / B-15, `feature-drafts.md` §4 decisions 12 and 13).
//!
//! Both are pure and deterministic, so every relay and node computes the same result from the same inputs.
//! - [`assign`] is run by the cell relays' majority membership publisher. From the published placement directory and
//!   membership it returns the directory entries to write: ranges of removed members get new owners, and members far from
//!   their capacity-weighted share take ranges from the most loaded, splitting where needed. Handoff (marking owners
//!   `ready`, revoking `leaving` ones) belongs to the relays (#22 / B-21); until then the publisher [`promote`]s.
//! - [`owners`] and [`serves`] read the published directory (#19 / B-16, `docs/spec/membership.md`).
//! - [`rank`] orders customer-KV readers by rendezvous hash. Customer KV has no ranges.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use crate::replication::pb;

/// The hash space is `[0, 2⁶⁴)`.
const SPACE: u128 = 1 << 64;
/// A move fills a member to at most this far above its target and leaves a donor at least this far below its own.
const FILL: f64 = 0.1;
/// Members within this of their target are left alone, so a join or removal moves nothing else.
const TRIGGER: f64 = 0.15;
/// Donors tried for an under-loaded member before it waits for the next plan.
const TRIES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Populating,
    Ready,
    Leaving,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub node: String,
    pub state: State,
}

impl Owner {
    /// Populating and ready owners count towards a range's replicas; a leaving one is on its way out.
    fn holds(&self) -> bool {
        self.state != State::Leaving
    }
}

/// Range start → owners. A range ends where the next starts and the last at 2⁶⁴, so ranges never gap or overlap.
pub type Directory = BTreeMap<u64, Vec<Owner>>;

/// The entries to write so every range has `replicas` owners, in distinct failure domains when possible, and members
/// hold their capacity-weighted share. At most `max_moving` ranges are in handoff at once, counting those already moving.
/// Ranges of members missing from `members` (removed after the grace period) are reassigned; draining, unhealthy and
/// non-`l2` members get nothing new. Decreasing `replicas` removes excess owners through bounded leaving handoffs.
pub fn assign(
    directory: &Directory,
    members: &BTreeMap<String, pb::Member>,
    unhealthy: &BTreeSet<String>,
    replicas: usize,
    max_moving: usize,
) -> Directory {
    let mut plan = Plan::new(directory, members, unhealthy, replicas);
    plan.repair(replicas, max_moving);
    plan.rebalance(max_moving);
    plan.ranges
        .into_iter()
        .filter(|(start, owners)| directory.get(start) != Some(owners))
        .collect()
}

struct Plan<'a> {
    members: &'a BTreeMap<String, pb::Member>,
    ranges: Directory,
    /// Member → starts of the ranges it holds.
    held: BTreeMap<&'a str, BTreeSet<u64>>,
    load: BTreeMap<&'a str, u128>,
    /// Eligible members only.
    target: BTreeMap<&'a str, f64>,
    /// Eligible members by load / target, as `f64` bits: non-negative floats order like their bit patterns.
    order: BTreeSet<(u64, &'a str)>,
    /// Ranges with an owner not yet `ready`.
    moving: usize,
}

impl<'a> Plan<'a> {
    fn new(
        directory: &Directory,
        members: &'a BTreeMap<String, pb::Member>,
        unhealthy: &BTreeSet<String>,
        replicas: usize,
    ) -> Self {
        let mut ranges = directory.clone();
        // An empty directory is one unowned range.
        ranges.entry(0).or_default();
        for owners in ranges.values_mut() {
            owners.retain(|o| members.contains_key(&o.node));
        }
        let mut plan = Plan {
            members,
            ranges,
            held: BTreeMap::new(),
            load: BTreeMap::new(),
            target: BTreeMap::new(),
            order: BTreeSet::new(),
            moving: 0,
        };
        let starts: Vec<u64> = plan.ranges.keys().copied().collect();
        for start in starts {
            let size = plan.size(start);
            let owners = &plan.ranges[&start];
            plan.moving += usize::from(owners.iter().any(|o| o.state != State::Ready));
            for owner in owners.iter().filter(|o| o.holds()) {
                let node = members.get_key_value(&owner.node).unwrap().0.as_str();
                *plan.load.entry(node).or_default() += size;
                plan.held.entry(node).or_default().insert(start);
            }
        }

        let eligible: Vec<(&'a str, &pb::Member)> = members
            .iter()
            .filter(|(id, m)| {
                m.status == pb::member::Status::Ready as i32
                    && m.roles.iter().any(|r| r == "l2")
                    && !unhealthy.contains(*id)
            })
            .map(|(id, m)| (id.as_str(), m))
            .collect();
        let eligible_ids: BTreeSet<&str> = eligible.iter().map(|(id, _)| *id).collect();
        // Ineligible members keep what they hold; the eligible ones share the rest.
        let kept: u128 = plan
            .load
            .iter()
            .filter(|(n, _)| !eligible_ids.contains(*n))
            .map(|(_, l)| l)
            .sum();
        let demand = replicas as f64 * SPACE as f64 - kept as f64;
        // Each member's share of its scarcest resource. A resource some member leaves unset (0) is ignored.
        let mut weight = vec![f64::INFINITY; eligible.len()];
        let resources: [fn(&pb::Member) -> f64; 2] = [|m| m.cpus, |m| m.disk_mb as f64];
        for resource in resources {
            let values: Vec<f64> = eligible.iter().map(|(_, m)| resource(m)).collect();
            if values.iter().all(|v| *v > 0.0) {
                let total: f64 = values.iter().sum();
                for (w, v) in weight.iter_mut().zip(&values) {
                    *w = w.min(v / total);
                }
            }
        }
        let weight: Vec<f64> = weight
            .into_iter()
            .map(|w| if w.is_finite() { w } else { 1.0 })
            .collect();
        let total: f64 = weight.iter().sum();
        for ((node, _), w) in eligible.iter().zip(weight) {
            let target = (w / total * demand).max(1.0);
            let load = *plan.load.entry(node).or_default();
            plan.target.insert(node, target);
            plan.order.insert((ratio(load, target), node));
        }
        plan
    }

    /// Gives every range missing owners new `populating` ones, fewest owners first. A ready range is split so the new
    /// owners take only what they have room for; a moving range is not split, since its handoff is already under way.
    fn repair(&mut self, replicas: usize, max_moving: usize) {
        let mut todo: Vec<(usize, u64)> = self
            .ranges
            .iter()
            .map(|(start, owners)| (owners.iter().filter(|o| o.holds()).count(), *start))
            .filter(|(have, _)| *have < replicas)
            .collect();
        todo.sort();
        for (_, start) in todo {
            loop {
                let owners = &self.ranges[&start];
                let have = owners.iter().filter(|o| o.holds()).count();
                let moving = owners.iter().any(|o| o.state != State::Ready);
                if have >= replicas || (!moving && self.moving >= max_moving) {
                    break;
                }
                let picks = self.pick(start, replicas - have);
                if picks.is_empty() {
                    break;
                }
                let size = self.size(start);
                let piece = if moving {
                    size
                } else {
                    // Redundancy comes before balance: with no room left anywhere, the whole range goes.
                    picks
                        .iter()
                        .map(|n| self.room(n))
                        .min()
                        .filter(|room| *room > 0)
                        .map_or(size, |room| room.min(size))
                };
                let at = self.split(start, piece);
                for node in picks {
                    self.add(at, node);
                }
                if !moving {
                    self.moving += 1;
                }
                if at == start {
                    break;
                }
            }
        }

        for start in self.ranges.keys().copied().collect::<Vec<_>>() {
            if self.moving >= max_moving {
                break;
            }
            let owners = &self.ranges[&start];
            if owners.iter().any(|o| o.state != State::Ready) {
                continue;
            }
            let held: Vec<_> = owners.iter().filter(|o| o.holds()).collect();
            if held.len() > replicas {
                let old = held[0].node.clone();
                self.mark_leaving(start, &old);
                self.moving += 1;
                continue;
            }
            if held.len() != replicas {
                continue;
            }

            let mut domain_counts = BTreeMap::<&str, usize>::new();
            for owner in &held {
                if let Some(domain) = self.domain(&owner.node) {
                    *domain_counts.entry(domain).or_default() += 1;
                }
            }
            let Some(old) = held
                .iter()
                .find(|o| {
                    self.domain(&o.node)
                        .is_some_and(|domain| domain_counts[domain] > 1)
                })
                .map(|o| o.node.clone())
            else {
                continue;
            };
            let Some(new) = self.pick(start, 1).first().copied() else {
                continue;
            };
            let Some(domain) = self.domain(new) else {
                continue;
            };
            if domain_counts.contains_key(domain) {
                continue;
            }

            self.mark_leaving(start, &old);
            self.add(start, new);
            self.moving += 1;
        }
    }

    /// The `missing` least-loaded eligible members to add to a range, in failure domains its owners lack when possible.
    fn pick(&self, start: u64, missing: usize) -> Vec<&'a str> {
        let owners = &self.ranges[&start];
        let mut domains: Vec<Option<&str>> = owners
            .iter()
            .filter(|o| o.holds())
            .map(|o| self.domain(&o.node))
            .collect();
        let mut picks = Vec::new();
        // ponytail: scans every member when too few domains exist; index members by domain if large cells need it.
        for distinct in [true, false] {
            for &(_, node) in &self.order {
                if picks.len() == missing {
                    return picks;
                }
                let domain = self.domain(node);
                if owners.iter().any(|o| o.node == node)
                    || picks.contains(&node)
                    || (distinct && domain.is_some() && domains.contains(&domain))
                {
                    continue;
                }
                picks.push(node);
                domains.push(domain);
            }
        }
        picks
    }

    /// Moves ranges from the most to the least loaded until every member is within [`TRIGGER`] of its target, or nothing
    /// more can move.
    fn rebalance(&mut self, max_moving: usize) {
        let mut stuck = BTreeSet::new();
        while self.moving < max_moving {
            let Some(&(under, u)) = self.order.iter().find(|(_, n)| !stuck.contains(n)) else {
                return;
            };
            let under = f64::from_bits(under);
            let donors: Vec<&'a str> = self
                .order
                .iter()
                .rev()
                .take(TRIES)
                .map(|(r, d)| (f64::from_bits(*r), *d))
                .take_while(|(over, _)| {
                    *over > 1.0 && (under < 1.0 - TRIGGER || *over > 1.0 + TRIGGER)
                })
                .map(|(_, d)| d)
                .collect();
            if under >= 1.0 || donors.is_empty() {
                return;
            }
            match donors
                .into_iter()
                .find_map(|d| self.donor_range(d, u).map(|start| (d, start)))
            {
                Some((d, start)) => self.give(start, d, u),
                None => {
                    stuck.insert(u);
                }
            }
        }
    }

    /// `d`'s ready range for `u` to take without losing failure-domain spread, preferring ranges `u` spreads further,
    /// then the largest.
    fn donor_range(&self, d: &str, u: &str) -> Option<u64> {
        let mut best: Option<((usize, u128), u64)> = None;
        for &start in self.held.get(d)? {
            let owners = &self.ranges[&start];
            if owners
                .iter()
                .any(|o| o.state != State::Ready || o.node == u)
            {
                continue;
            }
            let others = owners.iter().map(|o| o.node.as_str()).filter(|n| *n != d);
            let before = self.spread(others.clone().chain([d]));
            let after = self.spread(others.chain([u]));
            if after < before {
                continue;
            }
            let key = (after - before, self.size(start));
            if best.is_none_or(|(k, _)| key > k) {
                best = Some((key, start));
            }
        }
        best.map(|(_, start)| start)
    }

    /// Hands the tail of `d`'s range to `u`: as much as `u` has room for and `d` can spare.
    fn give(&mut self, start: u64, d: &'a str, u: &'a str) {
        let need = (1.0 + FILL) * self.target[u] - self.load[u] as f64;
        let spare = self.load[d] as f64 - (1.0 - FILL) * self.target[d];
        let at = self.split(start, need.min(spare).max(1.0) as u128);
        self.mark_leaving(at, d);
        self.add(at, u);
        self.moving += 1;
    }

    fn mark_leaving(&mut self, start: u64, node: &str) {
        let node = self.members.get_key_value(node).unwrap().0.as_str();
        self.ranges
            .get_mut(&start)
            .unwrap()
            .iter_mut()
            .filter(|o| o.node == node)
            .for_each(|o| o.state = State::Leaving);
        self.held.get_mut(node).unwrap().remove(&start);
        self.set_load(node, self.load[node] - self.size(start));
    }

    /// Splits off the last `piece` hashes of a range and returns where they start; the whole range if it is no larger.
    fn split(&mut self, start: u64, piece: u128) -> u64 {
        let size = self.size(start);
        if piece >= size {
            return start;
        }
        let at = start + (size - piece) as u64;
        let owners = self.ranges[&start].clone();
        for owner in owners.iter().filter(|o| o.holds()) {
            let node = self.members.get_key_value(&owner.node).unwrap().0.as_str();
            self.held.get_mut(node).unwrap().insert(at);
        }
        self.ranges.insert(at, owners);
        at
    }

    fn add(&mut self, start: u64, node: &'a str) {
        self.ranges.get_mut(&start).unwrap().push(Owner {
            node: node.into(),
            state: State::Populating,
        });
        self.held.entry(node).or_default().insert(start);
        self.set_load(node, self.load[node] + self.size(start));
    }

    fn set_load(&mut self, node: &'a str, load: u128) {
        if let Some(&target) = self.target.get(node) {
            self.order.remove(&(ratio(self.load[node], target), node));
            self.order.insert((ratio(load, target), node));
        }
        self.load.insert(node, load);
    }

    /// How much more `node` may take before it is [`FILL`] over its target.
    fn room(&self, node: &str) -> u128 {
        ((1.0 + FILL) * self.target[node] - self.load[node] as f64).max(0.0) as u128
    }

    fn size(&self, start: u64) -> u128 {
        let end = self
            .ranges
            .range((Excluded(start), Unbounded))
            .next()
            .map_or(SPACE, |(next, _)| u128::from(*next));
        end - u128::from(start)
    }

    /// An empty failure domain is the node's own.
    fn domain(&self, node: &str) -> Option<&'a str> {
        let domain = self.members[node].failure_domain.as_str();
        (!domain.is_empty()).then_some(domain)
    }

    /// How many failure domains `nodes` span.
    fn spread<'b>(&self, nodes: impl Iterator<Item = &'b str>) -> usize {
        let mut named = BTreeSet::new();
        nodes
            .filter(|n| self.domain(n).is_none_or(|d| named.insert(d)))
            .count()
    }
}

fn ratio(load: u128, target: f64) -> u64 {
    (load as f64 / target).to_bits()
}

/// The entries to write so `populating` owners become `ready` and `leaving` ones go. A range whose owners are all
/// leaving keeps them, so no range loses its last holder.
// ponytail: claims readiness without a bootstrap, which is safe only while nothing reads L2 by range (quicksilver is
// full_replica, auth on-demand); #22 / B-21 replaces it with the relays' bootstrap plus acknowledgement.
pub fn promote(directory: &Directory, members: &BTreeMap<String, pb::Member>) -> Directory {
    directory
        .iter()
        .filter(|(_, owners)| {
            owners
                .iter()
                .any(|o| o.state != State::Ready && members.contains_key(&o.node))
                && owners
                    .iter()
                    .any(|o| o.holds() && members.contains_key(&o.node))
        })
        .map(|(start, owners)| {
            let ready = owners
                .iter()
                .filter(|o| o.holds() && members.contains_key(&o.node))
                .map(|o| Owner {
                    node: o.node.clone(),
                    state: State::Ready,
                });
            (*start, ready.collect())
        })
        .collect()
}

/// The owners of the range holding `hash`; none before the directory's first write.
pub fn owners(directory: &Directory, hash: u64) -> &[Owner] {
    directory
        .range(..=hash)
        .next_back()
        .map_or(&[], |(_, owners)| owners)
}

/// The owner check: a node serves only ranges it owns or is leaving in its own view. A caller routed by a stale view
/// is rejected and retries the next owner.
pub fn serves(directory: &Directory, node: &str, hash: u64) -> bool {
    owners(directory, hash).iter().any(|o| o.node == node)
}

/// Customer-KV readers for a key, most preferred first: rendezvous hashing of `hash`
/// (`buckets::hash_of("kv", namespace, key)`) with each member's node ID. The same members give the same order on every
/// node, and removing a member moves only the keys it ranked first. Pass the readers that may serve, in their order, to
/// [`crate::health::Health::route`] to fall back to the next-ranked healthy one.
// ponytail: O(N) hashes plus a sort per request, about 185 µs at 10,000 readers (release, 2026-10-31); add a precomputed
// table only if that cost shows up in request latency.
pub fn rank<'a>(
    hash: u64,
    members: impl IntoIterator<Item = &'a pb::Member>,
) -> Vec<&'a pb::Member> {
    let mut scored: Vec<_> = members
        .into_iter()
        .map(|m| (score(hash, &m.node_id), m))
        .collect();
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.node_id.cmp(&b.1.node_id)));
    scored.into_iter().map(|(_, m)| m).collect()
}

/// FNV-1a over the node ID, then the SplitMix64 finalizer: stable across builds and platforms, unlike `DefaultHasher`.
fn score(hash: u64, node: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325_u64;
    for byte in node.bytes() {
        h = (h ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut z = h ^ hash;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
#[path = "../tests/unit/placement.rs"]
mod tests;
