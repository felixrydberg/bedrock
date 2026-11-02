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
