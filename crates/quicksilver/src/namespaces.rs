//! Quicksilver namespaces (B-24, `kv.md` §4). `infrastructure` holds generic routing/ownership/config metadata; typed
//! schemas belong to their consumers.

use bedrock_lib::datasets::{Namespace, PolicyOverrides};

pub const INFRASTRUCTURE: &str = "infrastructure";
/// Kept in L1; L2 never evicts the namespace (`full_replica`).
pub const PINNED_PREFIXES: [&str; 2] = ["critical/ownership/", "critical/routing/"];

/// The `infrastructure` namespace with its pinned prefixes.
pub fn infrastructure(home_region: &str) -> Namespace {
    let mut ns = Namespace::new(INFRASTRUCTURE, home_region).expect("static ID has no NUL");
    ns.overrides = PolicyOverrides {
        pinned_prefixes: Some(PINNED_PREFIXES.map(String::from).to_vec()),
        ..PolicyOverrides::default()
    };
    ns
}

#[cfg(test)]
#[path = "../tests/unit/namespaces.rs"]
mod tests;
