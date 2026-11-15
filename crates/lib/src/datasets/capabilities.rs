//! Relay-visible dataset capabilities. Service identity and local storage policy are separate.

pub const QUICKSILVER: &str = "quicksilver";
pub const AUTH: &str = "auth";
pub const CDN: &str = "cdn";
pub const KV: &str = "kv";
pub const MEMBERSHIP: &str = "membership";
pub const VOLUME: &str = "volume";

/// Services with a membership namespace in each cell, including services without ranged datasets.
pub const SERVICES: [&str; 4] = [QUICKSILVER, KV, CDN, VOLUME];

struct Capability {
    dataset: &'static str,
    pushed: bool,
    namespaces: &'static [&'static str],
    ranged_service: Option<&'static str>,
}

// Auth's generic relay and writer contracts remain available; no service places it until Gateway.
const CAPABILITIES: &[Capability] = &[
    Capability {
        dataset: QUICKSILVER,
        pushed: true,
        namespaces: &["infrastructure"],
        ranged_service: Some(QUICKSILVER),
    },
    Capability {
        dataset: AUTH,
        pushed: true,
        namespaces: &["keys"],
        ranged_service: None,
    },
    Capability {
        dataset: CDN,
        pushed: true,
        namespaces: &["pages", "workers"],
        ranged_service: Some(CDN),
    },
    Capability {
        dataset: KV,
        pushed: false,
        namespaces: &[],
        ranged_service: None,
    },
    // Actor slots (#111 / S-03): relay-assigned hash ranges with no relay data. The owner, not a relay stream, decides
    // when it is ready (S-06).
    Capability {
        dataset: VOLUME,
        pushed: false,
        namespaces: &[],
        ranged_service: Some(VOLUME),
    },
    Capability {
        dataset: MEMBERSHIP,
        pushed: true,
        namespaces: &[],
        ranged_service: None,
    },
];

pub fn pushed(dataset: &str) -> bool {
    CAPABILITIES
        .iter()
        .any(|capability| capability.dataset == dataset && capability.pushed)
}

pub fn namespaces(dataset: &str) -> &'static [&'static str] {
    CAPABILITIES
        .iter()
        .find(|capability| capability.dataset == dataset)
        .map_or(&[], |capability| capability.namespaces)
}

pub fn ranged_datasets(service: &str) -> impl Iterator<Item = &'static str> + '_ {
    CAPABILITIES.iter().filter_map(move |capability| {
        (capability.ranged_service == Some(service)).then_some(capability.dataset)
    })
}

#[cfg(test)]
#[path = "../../tests/unit/datasets/capabilities.rs"]
mod tests;
