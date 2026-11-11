//! Reads served by L2 range owners (#56 / B-25): the `Reads` service the shared client calls
//! ([`bedrock_lib::client`]). A node answers only for ranges it serves in its own placement view, and only when its
//! applied revision of the range reaches the caller's minimum, so a stale caller is turned away instead of served old
//! data. The value and the revision come from one engine snapshot.

use std::collections::BTreeMap;
use std::sync::Arc;

use bedrock_lib::membership;
use bedrock_lib::placement::{self, Directory};
use bedrock_lib::relays::client::applied_ranges;
use bedrock_lib::relays::ranges::Range;
use bedrock_lib::replication::pb;
use bedrock_lib::storage::{Snapshot, StorageEngine, l2_key, l2_span};
use bedrock_lib::transport::Caller;
use tonic::{Request, Response, Status};

pub struct Reads {
    pub node_id: String,
    pub cell: String,
    pub members: Arc<membership::View>,
    /// Read handles of the hosted datasets.
    pub engines: BTreeMap<String, Arc<dyn StorageEngine>>,
}

impl Reads {
    /// Runs `read` over one snapshot of `dataset`, with the ranges applied in it and this node's placement view.
    fn read<T>(
        &self,
        caller: Option<&Caller>,
        dataset: &str,
        read: impl FnOnce(&View) -> Result<T, Status>,
    ) -> Result<Response<T>, Status> {
        if let Some(caller) = caller {
            caller.require(&membership::SERVICES, Some(&self.cell))?;
        }
        let engine = self
            .engines
            .get(dataset)
            .ok_or_else(|| Status::failed_precondition("dataset is not hosted here"))?;
        let directory = self.members.directory(dataset)?;
        let snapshot = engine.snapshot().map_err(unavailable)?;
        let applied = applied_ranges(&*snapshot, dataset).map_err(unavailable)?;
        read(&View {
            snapshot: &*snapshot,
            applied: &applied,
            directory: &directory,
            node: &self.node_id,
        })
        .map(Response::new)
    }
}

#[tonic::async_trait]
impl pb::reads_server::Reads for Reads {
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetReply>, Status> {
        let request_ref = request.get_ref();
        self.read(Caller::of(&request), &request_ref.dataset, |view| {
            get(view, request_ref)
        })
    }

    async fn list(
        &self,
        request: Request<pb::ListRequest>,
    ) -> Result<Response<pb::ListReply>, Status> {
        let request_ref = request.get_ref();
        self.read(Caller::of(&request), &request_ref.dataset, |view| {
            list(view, request_ref)
        })
    }
}

/// The value of `request.key`, or its absence, at this node's applied revision of the key's range.
pub fn get(view: &View, request: &pb::GetRequest) -> Result<pb::GetReply, Status> {
    let hash = placement::hash_of(&request.dataset, &request.namespace, &request.key);
    let revision = view
        .serving(&request.namespace, hash, request.min_revision)?
        .1;
    let value = view
        .snapshot
        .get(&l2_key(hash, &request.namespace, &request.key))
        .map_err(unavailable)?;
    Ok(pb::GetReply { revision, value })
}

/// Keys of the range starting at `request.range_start` with the prefix, after `start_after`, in key order.
pub fn list(view: &View, request: &pb::ListRequest) -> Result<pb::ListReply, Status> {
    let ((start, end), revision) = view.serving(
        &request.namespace,
        request.range_start,
        request.min_revision,
    )?;
    if start != request.range_start {
        return Err(Status::failed_precondition("not a range start"));
    }
    let mut marker = request.namespace.as_bytes().to_vec();
    marker.push(0);
    let mut entries = Vec::new();
    // ponytail: the span is hash-ordered, so every match is collected before sorting; a key-ordered index would stop
    // at `limit`.
    for entry in view
        .snapshot
        .scan(&l2_span(start, end))
        .map_err(unavailable)?
    {
        let (key, value) = entry.map_err(unavailable)?;
        // `l2/{8-byte hash}/{namespace}\0{key}`
        let Some(key) = key
            .get(12..)
            .and_then(|rest| rest.strip_prefix(marker.as_slice()))
        else {
            continue;
        };
        if key.starts_with(&request.prefix) && key > request.start_after.as_slice() {
            entries.push(pb::StateEntry {
                key: key.to_vec(),
                value,
            });
        }
    }
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    let limit = match request.limit {
        0 => bedrock_lib::client::LIST_LIMIT,
        limit => limit,
    } as usize;
    let more = entries.len() > limit;
    entries.truncate(limit);
    Ok(pb::ListReply {
        revision,
        entries,
        more,
    })
}

/// `(namespace, range, revision)` of every range this node has validated progress for ([`applied_ranges`]).
pub type Applied = (String, Range, u64);

/// What one read sees: an engine snapshot, the ranges applied in it, and this node's placement view.
pub struct View<'a> {
    pub snapshot: &'a dyn Snapshot,
    pub applied: &'a [Applied],
    pub directory: &'a Directory,
    pub node: &'a str,
}

impl View<'_> {
    /// The followed range holding `hash` and its applied revision, when this node serves it at `min_revision`:
    /// `FAILED_PRECONDITION` when its view does not let it serve the range, `ABORTED` when it is behind.
    fn serving(
        &self,
        namespace: &str,
        hash: u64,
        min_revision: u64,
    ) -> Result<(Range, u64), Status> {
        if !placement::serves(self.directory, self.node, hash) {
            return Err(Status::failed_precondition("not an owner of the range"));
        }
        let applied = self.applied.iter().find(|(ns, (start, end), _)| {
            ns == namespace && hash >= *start && (*end == 0 || hash < *end)
        });
        match applied {
            Some((_, range, revision)) if *revision >= min_revision => Ok((*range, *revision)),
            _ => Err(Status::aborted("range is behind the requested revision")),
        }
    }
}

fn unavailable(error: bedrock_lib::storage::StorageError) -> Status {
    Status::unavailable(error.to_string())
}

#[cfg(test)]
#[path = "../tests/unit/operations.rs"]
mod tests;
