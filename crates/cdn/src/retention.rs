//! Declarative retention (#80 / B-32, `cdn.md` §3, decision 14): an artifact is kept while any
//! `ref/{sha256}/{deployment_id}` key exists in any namespace. Ref keys are ordinary L2 range data, placed by the
//! artifact's content hash, so this is one scan of that hash in the hash-first layout: no counts and no derived state,
//! and range bootstrap rebuilds it like any other range.

use bedrock_lib::placement;
use bedrock_lib::storage::{StorageEngine, StorageResult, l2_parts, l2_span};

use crate::artifact_storage::Hash;
use crate::paths::ref_prefix;

/// Whether `engine` holds a ref key of `sha256` in any namespace. Only meaningful on a holder of the artifact's range.
pub fn retained(engine: &dyn StorageEngine, sha256: &Hash) -> StorageResult<bool> {
    let hash = placement::content(&sha256.0);
    let prefix = ref_prefix(sha256);
    // The same 8 bytes may also place other artifacts' ref keys and path rows; only this artifact's ref keys count.
    for entry in engine.scan(&l2_span(hash, hash.wrapping_add(1)))? {
        let (physical, _) = entry?;
        if l2_parts(&physical).is_some_and(|(_, key)| key.starts_with(prefix.as_bytes())) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
#[path = "../tests/unit/retention.rs"]
mod tests;
