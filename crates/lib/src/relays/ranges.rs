//! Pushed L2 ranges on a member (#22 / B-21, `docs/spec/handoff.tla`): what a pushed set means for the ranges it
//! holds, and whether its own directory view confirms a revoke.
//!
//! A relay pushes the member's whole range set per dataset. A held range the set still contains is unchanged. One it
//! contains only parts of was split: the member keeps those parts with their coverage and drops the rest. One the set
//! no longer touches is revoked. A pushed range inside no held range is new. Every drop needs confirmation.

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

pub fn within(inner: Range, outer: Range) -> bool {
    inner.0 >= outer.0 && end(inner) <= end(outer)
}

fn overlaps(a: Range, b: Range) -> bool {
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

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub changed: Vec<Change>,
    pub added: Vec<Range>,
}

/// What the pushed `spans` mean for the `held` ranges. Malformed or overlapping spans, and a pushed range that
/// straddles a held one, are protocol errors: assignment only ever splits ranges.
pub fn plan(held: &[Range], spans: &[pb::Span]) -> Result<Plan, Status> {
    if spans.iter().any(|s| !valid_span(s)) {
        return Err(Status::data_loss("invalid pushed range"));
    }
    let mut pushed: Vec<Range> = spans.iter().map(|s| (s.start, s.end)).collect();
    pushed.sort_unstable();
    if pushed.windows(2).any(|w| overlaps(w[0], w[1])) {
        return Err(Status::data_loss("overlapping pushed ranges"));
    }
    let mut plan = Plan::default();
    for &p in &pushed {
        match held.iter().find(|h| overlaps(p, **h)) {
            Some(h) if !within(p, *h) => {
                return Err(Status::data_loss("pushed range straddles a held range"));
            }
            Some(_) => {}
            None => plan.added.push(p),
        }
    }
    for &h in held {
        let kept: Vec<Range> = pushed.iter().copied().filter(|p| within(*p, h)).collect();
        if kept != [h] {
            plan.changed.push(Change {
                held: h,
                dropped: minus(h, &kept),
                kept,
            });
        }
    }
    Ok(plan)
}

/// Whether `node`'s own directory view confirms dropping `range`: every directory range overlapping it no longer lists
/// the node, in any state, and has another owner `ready`. Otherwise the reason to refuse.
pub fn confirms(directory: Option<&Directory>, node: &str, range: Range) -> Result<(), String> {
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
        if !owners.iter().any(|o| o.state == State::Ready) {
            return Err(format!("range {start:016x} has no ready owner"));
        }
    }
    if covered {
        Ok(())
    } else {
        Err("the directory view does not cover the range".into())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/relays/ranges.rs"]
mod tests;
