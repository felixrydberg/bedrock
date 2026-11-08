//! Pushed L2 ranges on a member (#22 / B-21, `docs/spec/handoff.tla`): what a pushed set means for the ranges it
//! holds, and whether its own directory view confirms a revoke.
//!
//! A relay pushes the member's whole range set per dataset. A held range the set still contains is unchanged. One it
//! contains only parts of was split or re-cut: the member keeps those parts with their coverage and drops the rest. One
//! the set no longer touches is revoked. A pushed range inside no held range is new. Every drop needs confirmation.
//! Ranges also merge (#66 / B-61, `docs/spec/merge.tla`): a pushed range made of several kept parts joins their consumers
//! once each namespace's are at the same revision, and one with gaps bootstraps whole over them.

use std::collections::BTreeSet;

use tonic::Status;

use crate::placement::{Directory, State, valid_span};
use crate::replication::pb;

/// `(start, end)`, `end` 0 meaning 2⁶⁴.
pub type Range = (u64, u64);

fn end(range: Range) -> u128 {
    if range.1 == 0 {
        1 << 64
    } else {
        u128::from(range.1)
    }
}

/// Whether `inner` lies within `outer`, including equal bounds. Ranges are half-open; neither range is validated.
pub fn within(inner: Range, outer: Range) -> bool {
    inner.0 >= outer.0 && end(inner) <= end(outer)
}

pub(super) fn overlaps(a: Range, b: Range) -> bool {
    u128::from(a.0) < end(b) && u128::from(b.0) < end(a)
}

/// The parts of `held` outside the sorted, disjoint `kept` ranges inside it.
fn minus(held: Range, kept: &[Range]) -> Vec<Range> {
    let mut out = Vec::new();
    let mut at = u128::from(held.0);
    for k in kept {
        if u128::from(k.0) > at {
            out.push((at as u64, k.0));
        }
        at = end(*k);
    }
    if at < end(held) {
        out.push((at as u64, held.1));
    }
    out
}

/// A held range the pushed set changed: the parts kept, each continuing the held range's coverage, and the parts to
/// drop.
#[derive(Debug, PartialEq, Eq)]
pub struct Change {
    pub held: Range,
    pub kept: Vec<Range>,
    pub dropped: Vec<Range>,
}

/// Held ranges a pushed range is exactly made of, to join once their consumers agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merge {
    pub into: Range,
    pub parts: Vec<Range>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub changed: Vec<Change>,
    pub added: Vec<Range>,
    pub merged: Vec<Merge>,
    /// Pushed ranges holding held ones that do not fill them: a merged range given to a member that kept only some of its
    /// parts. The parts' consumers retire and the range bootstraps whole, as a new one does; nothing is dropped, since the
    /// member is a populating owner and its bootstrap replaces the span.
    pub absorbed: Vec<Merge>,
}

/// What the pushed `spans` mean for the `held` ranges and the durably `stored` ones. A stored range outside every held one
/// has data but no consumer. Merges move range boundaries (#66 / B-61), so a pushed set may cut any held range anywhere:
///
/// 1. Each held or stored range keeps its parts inside pushed ranges, each with its coverage, and drops the rest. A stored
///    range the set still has whole is new, since nothing follows it.
/// 2. A pushed range made of more than one whole kept part merges them; one with parts and gaps absorbs its parts and
///    bootstraps whole; one with no parts is new.
///
/// Malformed or overlapping spans return a data-loss status.
pub fn plan(held: &[Range], stored: &[Range], spans: &[pb::Span]) -> Result<Plan, Status> {
    if spans.iter().any(|s| !valid_span(s)) {
        return Err(Status::data_loss("invalid pushed range"));
    }
    let mut pushed: Vec<Range> = spans.iter().map(|s| (s.start, s.end)).collect();
    pushed.sort_unstable();
    if pushed.windows(2).any(|w| overlaps(w[0], w[1])) {
        return Err(Status::data_loss("overlapping pushed ranges"));
    }
    let size = |range: &Range| end(*range) - u128::from(range.0);
    let mut plan = Plan::default();
    let stored: Vec<Range> = stored
        .iter()
        .copied()
        .filter(|s| !held.iter().any(|h| within(*s, *h)))
        .collect();
    // Kept parts, and whether something follows them: a stored range kept whole has no consumer.
    let mut pieces: Vec<(Range, bool)> = Vec::new();
    for (&h, followed) in held
        .iter()
        .map(|h| (h, true))
        .chain(stored.iter().map(|s| (s, false)))
    {
        let kept: Vec<Range> = pushed
            .iter()
            .filter(|p| overlaps(h, **p))
            .map(|p| intersection(h, *p))
            .collect();
        if kept == [h] {
            pieces.push((h, followed));
            continue;
        }
        let dropped = minus(h, &kept);
        assert_eq!(
            kept.iter().chain(&dropped).map(size).sum::<u128>(),
            size(&h),
            "kept and dropped parts must exactly fill the held range"
        );
        pieces.extend(kept.iter().map(|k| (*k, true)));
        plan.changed.push(Change {
            held: h,
            dropped,
            kept,
        });
    }
    for &p in &pushed {
        let inside: Vec<(Range, bool)> = pieces
            .iter()
            .copied()
            .filter(|(r, _)| within(*r, p))
            .collect();
        let mut parts: Vec<Range> = inside.iter().map(|(r, _)| *r).collect();
        parts.sort_unstable();
        // A held part may also be a kept part of a stored range around it.
        parts.dedup();
        if parts.is_empty() || inside == [(p, false)] {
            plan.added.push(p);
            continue;
        }
        if parts == [p] {
            continue;
        }
        let whole = parts.iter().map(size).sum::<u128>() == size(&p);
        let merge = Merge { into: p, parts };
        if whole && inside.iter().all(|(_, followed)| *followed) {
            plan.merged.push(merge);
        } else {
            plan.absorbed.push(merge);
            plan.added.push(p);
        }
    }
    Ok(plan)
}

/// The part of `a` inside `b`; they must overlap.
fn intersection(a: Range, b: Range) -> Range {
    let start = a.0.max(b.0);
    let stop = if end(a) <= end(b) { a.1 } else { b.1 };
    (start, stop)
}

/// Whether `node`'s own directory view confirms dropping `range`: every directory range overlapping it no longer lists
/// the node, in any state, and has another owner `ready`. Returns a reason to refuse if the directory is missing,
/// no directory range overlaps, or an overlapping range fails those checks.
fn confirms(
    directory: Option<&Directory>,
    node: &str,
    range: Range,
    unhealthy: &BTreeSet<String>,
) -> Result<(), String> {
    let directory = directory.ok_or("the directory view has no ranges for the dataset")?;
    let mut entries = directory.iter().peekable();
    let mut covered = false;
    while let Some((&start, owners)) = entries.next() {
        let entry = (start, entries.peek().map_or(0, |(next, _)| **next));
        if !overlaps(entry, range) {
            continue;
        }
        covered = true;
        if owners.iter().any(|o| o.node == node) {
            return Err(format!("still an owner of range {start:016x}"));
        }
        // An unhealthy owner's copy may be the damaged one, so it cannot be why this one goes.
        if !owners
            .iter()
            .any(|o| o.state == State::Ready && !unhealthy.contains(&o.node))
        {
            return Err(format!("range {start:016x} has no ready owner"));
        }
    }
    if covered {
        Ok(())
    } else {
        Err("the directory view does not cover the range".into())
    }
}

/// Proof that the directory view allows `change`'s deletions. Only [`confirm`] makes one, and deleting a range requires it.
pub struct Confirmed<'a>(&'a Change);

impl<'a> Confirmed<'a> {
    pub fn change(&self) -> &'a Change {
        self.0
    }
}

/// Whether the directory view allows every deletion in `change`; the error says why it does not.
pub fn confirm<'a>(
    directory: Option<&Directory>,
    node: &str,
    change: &'a Change,
    unhealthy: &BTreeSet<String>,
) -> Result<Confirmed<'a>, String> {
    change
        .dropped
        .iter()
        .try_for_each(|dropped| confirms(directory, node, *dropped, unhealthy))?;
    Ok(Confirmed(change))
}

#[cfg(test)]
#[path = "../../tests/unit/relays/ranges.rs"]
mod tests;
