# Future development

These are possible directions for later work, not accepted scope or implemented features. Existing issue requirements,
the feature draft and recorded decisions remain authoritative until explicitly amended.

## Horizontally scaled API writers

B-09b starts with one Raft group for the core region. A future multi-group API could distribute write ordering and
materialization across API nodes while keeping KV nodes focused on reads. This is a scaling direction to evaluate after
the single-group protocol is implemented and hardened, not a prerequisite for the current B-11 or B-23 work.

Use a bounded number of writer shards, each backed by a Raft group, and assign each `(dataset, namespace)` to one shard.
Different groups can have leaders on different API nodes, and each namespace keeps its own SlateDB database. A group
can serve many namespaces, avoiding the cost of a separate consensus group for every idle namespace. Group count,
placement and the need for dedicated groups for hot namespaces require measurement. One hot namespace still has a
single ordered writer; splitting a namespace is a separate design.

The design must address:

- **Routing and ownership:** route all mutations for a namespace to its current group leader. Membership changes or
  a stale routing view cannot authorize a second group to write the namespace. Namespace-to-group routing is distinct
  from `(namespace, key)` cache affinity for KV reads.
- **Failover and durability:** acknowledge only after the group's configured durable quorum commits the write. Recover
  acknowledged but not yet SlateDB-durable entries, namespace revisions and retry records before serving as required.
  A group without a quorum cannot commit writes.
- **Storage fencing:** fence stale SlateDB publishers across leadership changes; Raft terms alone do not fence object
  storage. Preserve atomic state, revision and retry persistence and the separate committed, durable and reader-visible
  positions. Minimum-revision reads still require a sufficiently current view or coherent recent-state path.
- **Rebalancing:** moving a namespace between groups needs an ordered handoff, a single-authority cutover, fencing and
  recovery. Changing a hash or adding an API node alone does not safely move ownership. Define log truncation and
  checkpoint rules for namespaces shared by a group, including closed namespaces.
- **Scope and validation:** define batch/transaction guarantees across namespaces and groups explicitly; independent
  groups do not provide cross-group atomicity. Specify resource budgets, isolation, placement and failure domains.
  Update the TLA+ protocol before implementation and rerun the applicable B-23 checks and nightly history.

The shared interfaces may eventually need a group identity and routing contract, but their design is not a requirement
to add speculative multi-group machinery to the initial single-group implementation.

## Partial storage quarantine

B-14 (#13) records the affected partition with each storage error but always quarantines the whole engine instance,
because no adapter can yet prove that damage is limited. Narrower quarantine would shorten rebuilds once instances are
large; L2 is never the only copy, so today whole-instance quarantine costs only rebuild time on one node.

- **CDN artifacts first.** Artifacts are content-addressed, so `CacheStorage` (B-30) can verify the hash on every read
  and quarantine one artifact: evict it, refetch from S3 and report it to the health manager without escalating. Only
  repeated failures quarantine the whole cache. This fits the existing per-source model and needs no protocol change;
  record it as a B-30 or B-22 amendment.
- **Detection for RocksDB.** RocksDB names the damaged file (`file_in` already extracts it) but not its keys. The file's
  smallest and largest keys narrow a checksum failure to one key span. Quicksilver L2 keys start with the big-endian
  hash, so that span is a hash range, the unit placement assigns. L1 spans map to namespaces. Damage to revisions or
  metadata still quarantines the whole instance, since they describe every key's progress.
- **Routing.** The node can reject reads for a quarantined range through the owner check (`placement::serves`); callers
  already retry the next owner, so this needs no protocol change. A short-lived per-range skip, like `Sources::behind`,
  avoids repeated round trips. Publishing quarantined ranges in `Member` or the directory is a membership protocol
  change, needs the specification updated first, and pays off only if relays should re-replicate the range.
- **Repair.** Rebuild only that hash range without moving the dataset's applied revision backwards, reusing the range
  bootstrap from #22 / B-21 handoffs.
- **Containment.** A compaction that reads a corrupt SST fails and stops RocksDB writes, which brings back
  whole-instance quarantine. The quarantined span must also be removed from compaction (delete the range and compact
  around it, or drop the file).

## Customer-KV S3 request cost (follow-up to #57 / B-39)

Each open namespace reader polls S3 about once a second (a LIST and a GET of the manifest, then a WAL probe), even
when nothing in the namespace has changed. LISTs are the expensive request class. The cost lands on namespaces that
are read often but rarely written, and on the 15 minutes before an idle reader closes. Namespaces nobody reads already
cost nothing once their reader closes.

Proceed in order and stop when the measurements show the cost is low enough:

1. **Measure.** Count S3 requests by operation (LIST, GET, PUT) and by component (writer, reader, GC, purge). Add the
   range-bytes metric requested by #57. Decide what to change only once the bill shows which requests dominate.
2. **Cache immutable objects on the read side.** SST and WAL objects never change once written, so a read-through
   cache needs no invalidation and no part in the write path. Options are a larger shared block cache, or SlateDB's
   object-store cache if a local disk cache is acceptable.
3. **Cut idle polling.** Make the reader poll slower, or close idle readers sooner than 15 minutes. Optionally, have
   the writer send “namespace N reached revision R” notices so readers refresh only when something changed. SlateDB
   0.17's `DbReader` has no manual refresh, so a notice means reopening the reader or changing SlateDB upstream.
4. **Consider a write-path S3 gateway only if steps 1–3 aren't enough.** Put a proxy in front of the bucket, limited
   to the `kv/` prefix, that answers manifest/WAL LISTs and GETs from memory and invalidates on writes it sees.
   - Conditional writes pass straight through. Create-if-absent and compare-and-swap go to S3 unchanged, and the cache
     changes only after S3 confirms. A write whose outcome is unknown invalidates the cache instead of updating it.
   - It must be the only path to the bucket. Writers, GC, reader checkpoints, purges and operators all go through it,
     or its cache goes stale. A stale “this exists” after GC deleted an object makes readers fail.
   - Choose either one gateway (readers in other regions then call it across regions) or one per region with coherent
     invalidation between instances, which turns it into a distributed cache protocol.
   - Writes would depend on the gateway as well as on S3.
   - This touches writer fencing, so model it in TLA+ before building it.

For namespaces that are read but not changing, steady-state S3 requests should drop close to zero: one real poll,
then cached until the writer publishes again. Namespaces nobody reads cost nothing already.

**Depends on:** #57 / B-39 (readers), B-28 (residency policy), and a metrics baseline from step 1.

## Customer-KV namespace writer/reader colocation

**Accepted 2026-11-12 for #58 / B-26** (writer and reads on the rendezvous owner in the home region; see the B-26
amendment in `feature-drafts.md`). The text below is the original evaluation.

Evaluate placing a namespace's writer and its active KV readers in the same cell, and where practical the same host
or nearby hosts. This may reduce cross-region S3 traffic and read latency, especially for frequently read namespaces,
but should be justified by measured request, latency and cache-residency data. The API writer and `bedrock-kv` remain
separate services; colocation is a placement and routing choice, not a shared-process design.

For a colocated reader, remove the idle timeout and rely on the local writer to notify it when the namespace advances.
The reader can then stay open without periodic S3 polling while the writer is reachable. On writer or cell loss, the
reader must fall back to the existing refresh/reopen or S3 path; this avoids polling during normal colocated operation,
not the need to recover and catch up after failover.

- **Ownership and routing:** keep one fenced writer for each namespace. Route writes to its authoritative writer and
  prefer eligible local readers for reads, with the existing S3/recent-state fallback when no suitable reader is
  available. Reader affinity must not imply that a reader is authoritative or sufficiently current for a
  minimum-revision read.
- **Placement inputs:** account for writer region, active read demand, cache residency, capacity and failure domains.
  Avoid moving namespaces based on noisy short-term demand, and retain a clear fallback when a cell or host is
  unavailable.
- **Handoff:** moving a writer requires the existing ordered handoff and storage fencing guarantees. Moving or
  shedding readers must preserve readiness, freshness and bounded resource use. Do not add a separate membership or
  placement epoch for this optimization.
- **Validation:** first measure cross-region request cost, read latency and cache hit/residency rates. Prototype with
  a small set of namespaces and compare against current placement, including failover and rebalancing behavior.
  Define the placement policy only if the measured gains justify its operational and routing complexity.

**Depends on:** #57 / B-39 (readers), B-28 (residency policy), and the existing writer-fencing and handoff guarantees.
