//! Object I/O configuration and whole-object integrity helpers (#7 / B-07).
//!
//! Native clients use `object_store::ObjectStore` directly. Whole artifact reads
//! verify SHA-256 metadata; SlateDB retains native ranged I/O and block checksums.
//! Simulation uses the deterministic namespace-storage model rather than native I/O.

#[cfg(not(madsim))]
mod object;
#[cfg(not(madsim))]
pub use object::{get_checked, object_store, put_checked};

#[cfg(all(test, not(madsim)))]
#[path = "../tests/support/s3_contract.rs"]
mod contract;

use std::fmt;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::storage::{ErrorContext, StorageError, StorageResult};

/// S3 connection settings for the upstream object-store backend.
#[derive(Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Attempts per request, including the first.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_attempt_timeout_ms")]
    pub attempt_timeout_ms: u64,
}

fn default_region() -> String {
    "us-east-1".into()
}
fn default_max_attempts() -> u32 {
    3
}
fn default_attempt_timeout_ms() -> u64 {
    30_000
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("max_attempts", &self.max_attempts)
            .field("attempt_timeout_ms", &self.attempt_timeout_ms)
            .finish()
    }
}

fn checksum(body: &[u8]) -> String {
    Sha256::digest(body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn context(key: &str, detail: &str) -> ErrorContext {
    ErrorContext {
        engine: "object_store",
        instance: "s3".into(),
        file: Some(key.into()),
        partition: None,
        detail: detail.into(),
    }
}

/// Whole-object SHA-256 validation, also usable by simulated artifact sources.
/// Missing or invalid integrity metadata is corruption, never an absent key.
pub fn verify_checksum(key: &str, body: &[u8], expected: Option<&str>) -> StorageResult<()> {
    let expected = expected
        .ok_or_else(|| StorageError::Corruption(context(key, "missing SHA-256 metadata")))?;
    if checksum(body) != expected {
        return Err(StorageError::Corruption(context(key, "SHA-256 mismatch")));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/s3.rs"]
mod tests;
