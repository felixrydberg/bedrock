//! Shared distributed core linked by every Bedrock binary.

pub mod admin;
pub mod client;
pub mod datasets;
pub mod health;
pub mod http;
pub mod membership;
pub mod node;
pub mod placement;
pub(crate) mod raft_runtime;
pub mod reconciliation;
pub mod relays;
pub mod replication;
pub mod s3;
#[cfg(madsim)]
pub mod sim;
pub mod storage;
pub mod transport;

// madsim's Tokio clock uses `std::time::Instant`; scope the lint exception to this adapter.
#[cfg_attr(madsim, allow(clippy::disallowed_methods))]
/// Returns the runtime's monotonic clock, including simulated time under madsim.
pub fn monotonic_now() -> tokio::time::Instant {
    tokio::time::Instant::now()
}

/// A random version 4 UUID as 32 lowercase hex digits without dashes, the shape of a Cloudflare namespace ID. The
/// runtime's RNG draws it: seeded under madsim, so a seed replays the same IDs.
// madsim's `rand` re-exports the real crate in normal builds; this is the one place that reads it.
#[allow(clippy::disallowed_methods)]
pub fn uuid() -> String {
    let bits: u128 = madsim::rand::random();
    // RFC 9562: version 4 in bits 76..80, variant `10` in bits 62..64.
    let bits = (bits & !(0xf << 76) & !(0x3 << 62)) | (0x4 << 76) | (0x2 << 62);
    format!("{bits:032x}")
}

#[cfg(test)]
#[path = "../tests/unit/lib.rs"]
mod tests;
