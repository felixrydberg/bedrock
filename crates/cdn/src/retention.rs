//! Declarative retention (#80 / B-32, `cdn.md` §3, decision 14): an artifact is kept while any
//! `ref/{sha256}/{deployment_id}` key exists in any namespace. Ref keys are ordinary L2 range data, placed by the
//! artifact's content hash, so this is one scan of that hash in the hash-first layout: no counts and no derived state,
//! and range bootstrap rebuilds it like any other range.
//!
//! Regional L2 (#82 / B-34, `cdn.md` §3): the two owners of an artifact's range in the cell's placement directory hold
//! its bytes. [`Prewarm`] keeps them there: desired is every retained artifact in this node's ranges, actual is its
//! local inventory, and a repair fetches through B-31 from the owners ahead of this one ([`sources`]), then S3. The same
//! pass prewarms an artifact whose first ref key just arrived and repopulates a range this node was just given (B-21):
//! both show up as retained artifacts missing locally.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bedrock_lib::placement::{self, Directory};
use bedrock_lib::reconciliation::Reconcile;
use bedrock_lib::replication::pb::Member;
use bedrock_lib::storage::{StorageEngine, StorageResult, l2_parts, l2_span};

use crate::artifact_storage::{Artifacts, Hash};
use crate::artifact_transfer::Transfer;
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

/// The artifact a well-formed `ref/{sha256}/{deployment_id}` key retains.
fn ref_hash(key: &[u8]) -> Option<Hash> {
    let rest = key.strip_prefix(b"ref/")?;
    (rest.get(64) == Some(&b'/'))
        .then(|| Hash::parse(std::str::from_utf8(&rest[..64]).ok()?))
        .flatten()
}

/// The addresses `me` fetches `sha256` from before S3: the owners ahead of it in its range's directory order, or
/// `None` when `me` does not own the range. The first owner names none and reads S3; each later one names those ahead,
/// so no two holders name each other and a cold artifact is read from S3 once per cell, the others joining that
/// fetch through B-31 (which never forwards a peer's request). Views differing between holders only cost a second S3
/// read. Owners missing from `members` are skipped.
pub fn sources(
    directory: &Directory,
    members: &BTreeMap<String, Member>,
    me: &str,
    sha256: &Hash,
) -> Option<Vec<String>> {
    let owners = placement::owners(directory, placement::content(&sha256.0));
    let at = owners.iter().position(|owner| owner.node == me)?;
    Some(
        owners[..at]
            .iter()
            .filter_map(|owner| Some(members.get(&owner.node)?.address.clone()))
            .collect(),
    )
}

/// This cell's members and the `cdn` placement directory, as the node's membership view has them.
pub type Placement =
    Box<dyn Fn() -> Result<(BTreeMap<String, Member>, Directory), String> + Send + Sync>;

/// Prewarming and replica repair of the artifacts this node holds, run by a [`bedrock_lib::reconciliation::Reconciler`].
pub struct Prewarm {
    /// The `cdn` engine holding this node's ranges.
    pub engine: Arc<dyn StorageEngine>,
    pub placement: Placement,
    /// This node's ID, as the directory names it.
    pub me: String,
    /// The limiter lane S3 fetches pay from, e.g. `{cell}/warm`, so prewarming never takes readers' share.
    pub lane: String,
    pub artifacts: Arc<Artifacts>,
    pub transfer: Arc<Transfer>,
}

#[async_trait::async_trait]
impl Reconcile for Prewarm {
    type Key = Hash;
    /// The owners ahead of this one.
    type Desired = Vec<String>;
    type Actual = ();
    type Repair = Vec<String>;
    type Error = String;

    // ponytail: scans every key this node holds each pass, and keeps every retained artifact until B-44 (#92) adds the
    // idle window and its read times. Track ref keys as they apply if scans grow slow.
    async fn desired(&self) -> Result<BTreeMap<Hash, Vec<String>>, String> {
        let (members, directory) = (self.placement)()?;
        let mut desired = BTreeMap::new();
        for entry in self
            .engine
            .scan(&l2_span(0, 0))
            .map_err(|e| e.to_string())?
        {
            let (physical, _) = entry.map_err(|e| e.to_string())?;
            let Some(sha256) = l2_parts(&physical).and_then(|(_, key)| ref_hash(key)) else {
                continue;
            };
            if !desired.contains_key(&sha256)
                && let Some(sources) = sources(&directory, &members, &self.me, &sha256)
            {
                desired.insert(sha256, sources);
            }
        }
        Ok(desired)
    }

    async fn actual(&self) -> Result<BTreeMap<Hash, ()>, String> {
        let held: BTreeSet<Hash> = self.artifacts.inventory().map_err(|e| e.to_string())?;
        Ok(held.into_iter().map(|hash| (hash, ())).collect())
    }

    /// Fetches what is missing. What is held but no longer desired is left for eviction (B-44).
    fn plan(
        &self,
        _: &Hash,
        desired: Option<&Vec<String>>,
        actual: Option<&()>,
    ) -> Option<(u8, Vec<String>)> {
        match (desired, actual) {
            (Some(sources), None) => Some((0, sources.clone())),
            _ => None,
        }
    }

    async fn repair(&self, sha256: &Hash, sources: &Vec<String>) -> Result<(), String> {
        match self.transfer.get(&self.lane, sha256, sources).await {
            Ok(Some(_)) => Ok(()),
            // Publishing uploads before it writes ref keys (B-33), so this is retried rather than trusted.
            Ok(None) => Err(format!("retained artifact {sha256} is not in S3")),
            Err(error) => Err(error.to_string()),
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/retention.rs"]
mod tests;
