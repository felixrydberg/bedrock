//! Customer-KV reader ranking and the L2 placement directory's read side (#18 / B-15, `feature-drafts.md` §4 decisions 12
//! and 13).
//!
//! All of it is pure and deterministic, so every relay and node computes the same result from the same inputs. The write
//! side (`assign`, `handoff`, `merges`) runs only in the cell relays' membership publisher and lives in `bedrock-relay`.
//! - [`owners`] and [`serves`] read the published directory (#19 / B-16, `docs/spec/membership.md`).
//! - [`rank`] orders customer-KV readers by rendezvous hash. Customer KV has no ranges.
//! - [`hash_of`] places a key in the hash space; relay range streams select keys with [`in_span`] (#22 / B-21).
//! - [`namespaces`] is the fixed namespace set of each ranged dataset, so relays know every namespace a range covers.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::replication::pb;

/// The namespaces of a dataset placed in L2 ranges, fixed in code; empty for every other dataset. The writer accepts no
/// other namespace of a ranged dataset, and a member is ready for a range only once it holds all of them (#22 / B-21).
pub fn namespaces(dataset: &str) -> &'static [&'static str] {
    crate::datasets::capabilities::namespaces(dataset)
}

/// A key's position in the 64-bit L2 hash space (#18 / B-15); customer KV ranks its readers by it.
pub fn hash_of(dataset: &str, namespace: &str, key: &[u8]) -> u64 {
    let mut hash = Sha256::new();
    for part in [dataset.as_bytes(), namespace.as_bytes(), key] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    u64::from_be_bytes(hash.finalize()[..8].try_into().unwrap())
}

/// Whether `hash` lies in `span`: `[start, end)`, where `end` 0 means 2⁶⁴.
pub fn in_span(span: &pb::Span, hash: u64) -> bool {
    hash >= span.start && (span.end == 0 || hash < span.end)
}

/// A span is valid when it is not empty.
pub fn valid_span(span: &pb::Span) -> bool {
    span.end == 0 || span.end > span.start
}

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

/// Range start → owners. A range ends where the next starts and the last at 2⁶⁴, so ranges never gap or overlap.
pub type Directory = BTreeMap<u64, Vec<Owner>>;

/// The owners of the range holding `hash`; none before the directory's first write.
pub fn owners(directory: &Directory, hash: u64) -> &[Owner] {
    directory
        .range(..=hash)
        .next_back()
        .map_or(&[], |(_, owners)| owners)
}

/// The start of the range holding `hash`; 0 before the directory's first write.
pub fn start_of(directory: &Directory, hash: u64) -> u64 {
    directory
        .range(..=hash)
        .next_back()
        .map_or(0, |(start, _)| *start)
}

/// Where the range starting at `start` ends: the next range's start, or 0 at the end of the ring.
pub fn end_of(directory: &Directory, start: u64) -> u64 {
    start
        .checked_add(1)
        .and_then(|after| directory.range(after..).next())
        .map_or(0, |(next, _)| *next)
}

/// The owner check: a node serves only ranges it holds as `ready` or `leaving` in its own view; a `populating` owner
/// may not have the data yet (#22 / B-21). A caller routed by a stale view is rejected and retries the next owner.
pub fn serves(directory: &Directory, node: &str, hash: u64) -> bool {
    owners(directory, hash)
        .iter()
        .any(|o| o.node == node && o.state != State::Populating)
}

/// Customer-KV readers for a key, most preferred first: rendezvous hashing of `hash`
/// (`hash_of("kv", namespace, key)`) with each member's node ID. The same members give the same order on every
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
