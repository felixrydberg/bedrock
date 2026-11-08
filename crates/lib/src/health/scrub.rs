//! Background verification of local storage (#25 / B-23). A read finds damage only in the record it touches, so a cold
//! record could stay damaged forever while its node keeps claiming the copy. One low-rate task per process walks its
//! engines in turn, reading every record through the [`super::storage::StorageHealth::wrap`] handles, so a failed read
//! quarantines and rebuilds like any other. A quarantined engine refuses the walk until it is rebuilt.
//!
//! The walk goes by hash range, so a failure already names the part of the engine it hit, which partial quarantine can
//! use once adapters can prove damage is limited to it.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::storage::{KeyRange, StorageEngine, StorageResult, l2_span};

/// Hash ranges per engine. A power of two, so they meet exactly at 2⁶⁴.
const SLICES: u64 = 64;
/// The least time between two ranges, which sets the walk's speed on small datasets.
const PAUSE: Duration = Duration::from_secs(1);
/// The walk's read rate on large ones.
const BYTES_PER_SECOND: f64 = 8.0 * 1024.0 * 1024.0;

/// How long a walk of `engines` near-empty engines takes.
pub const fn pass(engines: u64) -> Duration {
    Duration::from_secs(engines * (SLICES + 2) * PAUSE.as_secs())
}

/// Everything an engine can hold, by hash range and then the keys outside L2.
fn ranges() -> Vec<KeyRange> {
    let width = u64::MAX / SLICES + 1;
    let mut ranges: Vec<KeyRange> = (0..SLICES)
        .map(|slice| l2_span(slice * width, (slice + 1).wrapping_mul(width)))
        .collect();
    let l2 = KeyRange::prefix(*b"l2/");
    ranges.push(KeyRange::new(Vec::new(), l2.start));
    ranges.push(KeyRange {
        start: l2.end.expect("the prefix has an end"),
        end: None,
    });
    ranges
}

/// Reads every record of `range` and returns the bytes read. The first failure ends the read; the handle reports it.
fn verify(engine: &dyn StorageEngine, range: &KeyRange) -> StorageResult<u64> {
    let mut bytes = 0;
    for entry in engine.scan(range)? {
        let (key, value) = entry?;
        bytes += (key.len() + value.len()) as u64;
    }
    Ok(bytes)
}

/// Walks `engines`, the process's wrapped handles, until aborted.
// ponytail: a restart begins a new walk, so a process restarting faster than a pass never reaches the last ranges; keep a
// durable position if that shows up. Reads also go through the block cache; bypass it if the walk evicts hot data.
pub fn start(engines: Vec<(String, Arc<dyn StorageEngine>)>) -> JoinHandle<()> {
    tokio::spawn(async move {
        if engines.is_empty() {
            return;
        }
        let ranges = ranges();
        loop {
            for (dataset, engine) in &engines {
                for range in &ranges {
                    #[cfg(not(madsim))]
                    let result = {
                        let (engine, range) = (engine.clone(), range.clone());
                        tokio::task::spawn_blocking(move || verify(engine.as_ref(), &range))
                            .await
                            .expect("the walk does not panic")
                    };
                    #[cfg(madsim)]
                    let result = verify(engine.as_ref(), range);
                    let wait = match result {
                        Ok(bytes) => {
                            PAUSE.max(Duration::from_secs_f64(bytes as f64 / BYTES_PER_SECOND))
                        }
                        Err(error) => {
                            tracing::debug!(%dataset, ?range, %error, "scrub read failed");
                            PAUSE
                        }
                    };
                    tokio::time::sleep(wait).await;
                }
            }
        }
    })
}

#[cfg(test)]
#[path = "../../tests/unit/health/scrub.rs"]
mod tests;
