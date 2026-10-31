//! Shared distributed core linked by every Bedrock binary.

pub mod buckets;
pub mod datasets;
pub mod health;
pub mod membership {}
pub mod node;
pub mod placement {}
pub mod reconciliation {}
pub mod relays;
pub mod replication;
pub mod s3;
pub mod storage;
pub mod transport;

// madsim's Tokio clock uses `std::time::Instant`; scope the lint exception to this adapter.
#[cfg_attr(madsim, allow(clippy::disallowed_methods))]
pub(crate) fn monotonic_now() -> tokio::time::Instant {
    tokio::time::Instant::now()
}
