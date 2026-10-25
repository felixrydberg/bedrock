# Unified Distributed Metadata & KV Architecture

## 1. Overview

A globally distributed, S3-backed key-value platform inspired by Cloudflare Quicksilver v2.

The platform initially supports two datasets:

- **Quicksilver:** Critical infrastructure metadata, including deployment routing, D1 ownership, Durable Object placement, KV namespace ownership, and system configuration.
- **KV:** Customer key-value stores with home-region writes, on-demand caching, and automatic eviction.

Both datasets run on the **same Rust binary, replication protocol, sequence system, regional relays, and L1/L2 caching architecture**.

Storage engines are interchangeable through a common Rust abstraction.

### High-Level Architecture

```text
          Authoritative Home Writers
                     |
              Durable Write Logs
                     |
          Global Replication Network
                     |
         +-----------+-----------+
         |           |           |
       EU DC       US DC       APAC DC
         |           |           |
      Relays      Relays       Relays
         |           |           |
      L1 + L2     L1 + L2      L1 + L2
      Cluster     Cluster      Cluster
         |           |           |
       StorageEngine Abstraction
         |                       |
      RocksDB                SurrealKV

              L3: S3 Storage
          Snapshots + Log Archive
```

Each cell independently manages its caches, shard assignments, and node health. A datacenter is one cell by default; large sites run several (see §9 Cells).

## 2. Logical Data Model

The logical hierarchy is:

**Dataset → Namespace**

```text
Dataset: quicksilver
  Namespace: infrastructure

Dataset: kv
  Namespace: customer-123/store-A
  Namespace: customer-123/store-B
  Namespace: customer-456/store-A
```

### Dataset

Defines default policies for:

- Replication
- Cache retention
- Durability
- Priority
- Resource budgets
- Storage engine selection

Each dataset uses its own storage-engine instance on each applicable node.

### Namespace

Represents an isolated KV store with:

- Unique identifier
- Home region
- Storage and API quotas
- Permissions
- Optional policy overrides

Namespaces share their dataset's local storage instance.

Application bindings reference existing namespace IDs.

There are no exposed application-level shards. Regional L2 bucket partitioning remains an internal implementation detail.

## 3. Replication Policies

| Policy | Propagation | Regional L2 storage |
|---|---|---|
| `global` | All eligible datacenters | Working set + recent changes |
| `on_demand` | Active subscribers only | Requested data |
| `home_only` | Home region only | No remote requirement |
| `full_replica` | All eligible datacenters | Complete namespace |

**Global** means all eligible datacenters receive updates, but older, unpinned data may be evicted.

**On-demand** means only active regions receive updates, allowing customer data to be cached according to demand.

**Home-only** forwards remote requests to the authoritative home region.

**Full-replica** requires every eligible datacenter to retain the entire namespace distributed across L2 nodes.

Global propagation and complete regional retention are different guarantees.

## 4. Dataset and Namespace Policies

Effective policies are resolved through:

1. Dataset defaults
2. Optional namespace overrides
3. Node safety and resource constraints

Both L1 and L2 respect these policies.

Example:

```json
{
  "dataset": "quicksilver",
  "namespace": "infrastructure",
  "replication": "full_replica",
  "cache": {
    "l1_recent_window_seconds": 60,
    "l2_eviction": false,
    "pinned_prefixes": [
      "critical/ownership/",
      "critical/routing/"
    ]
  }
}
```

Pinned records are protected against normal cache eviction. Under `full_replica` nothing is evicted from L2, so pins keep
critical records in each node's L1.

## 5. Docker Deployment

Each component is its own binary, all built on the shared `lib` crate.

| Binary / role | Responsibility |
|---|---|
| `bedrock-api` | Authoritative writes and sequence ordering |
| `bedrock-relay` | Replication fan-out, replay and cell membership |
| `bedrock-kv`, role `l1` | Per-node local cache |
| `bedrock-kv`, role `l2` | Cell-wide distributed cache |

`l1` and `l2` can coexist in one `bedrock-kv` process. Relays run as their own process on a few machines per cell.

### Requirements

- Docker deployment by default.
- `network_mode: host` for low-latency networking.
- Host NVMe bind mounts for database storage.
- ext4 or XFS filesystems.
- Configurable CPU, memory, and disk budgets.
- Database files survive container restarts.
- Bare-metal deployment remains an option.

Example:

```yaml
services:
  platform:
    image: platform:latest
    network_mode: host
    volumes:
      - /mnt/nvme/platform:/data
    restart: unless-stopped
```

## 6. Pluggable Storage Engine

All local storage is accessed through a Rust `StorageEngine` abstraction.

The following components must remain independent of the chosen adapter:

- Replication protocol
- L1/L2 cache management
- Regional relays
- Recovery orchestration
- S3 persistence

### StorageEngine Interface

The interface provides:

- `get`, `put`, `delete`
- Atomic batches and transactions
- Prefix and range iteration
- Consistent snapshots and restoration
- Namespace eviction and range deletion
- Atomic replication revision tracking
- Storage statistics and disk usage
- Structured I/O and corruption errors
- Storage health and availability information

### Storage Adapters

| Adapter | Implementation | Advantages |
|---|---|---|
| `RocksDbAdapter` | Rust `rocksdb` crate | Mature LSM implementation, configurable compaction |
| `SurrealKvAdapter` | SurrealKV | Pure Rust, async commit pipeline, MVCC |

Both must implement the same logical correctness guarantees.

Adapter-specific operations may use different implementation strategies. Unsupported capabilities must be explicitly reported rather than silently approximated.

### Storage Error Contract

A missing key is a normal lookup outcome, not a storage failure.

A suitable Rust interface is:

```rust
enum StorageError {
    IoError,
    Corruption,
    Unavailable,
    Unsupported,
}

type StorageResult<T> = Result<T, StorageError>;

// Ok(Some(value)) = key found
// Ok(None)        = key not present locally
// Err(error)      = storage failure
```

Production errors should also carry sufficient diagnostic information to identify the affected engine, database, file, or partition when available.

This distinction is important because a cache miss should trigger normal fallback, while corruption should additionally trigger storage-health handling.

## 7. Storage Instance Layout

Each dataset has its own storage-engine instance per applicable node.

```text
Machine A (L1 + L2 + Relay)

StorageEngine: quicksilver
  l1/{namespace}/{key}
  l2/{bucket}/{namespace}/{key}
  meta/{namespace}/...

StorageEngine: kv
  l1/{namespace}/{key}
  l2/{bucket}/{namespace}/{key}
  meta/{namespace}/...

StorageEngine: relay
  replay/{dataset}/{namespace}/{revision}
  cursor/{stream}/...
```

L1 and L2 share their dataset's local engine instance but use different logical keyspaces.

Relay replay history uses a separate instance.

This provides:

- Independent dataset compaction.
- Separate memory and storage budgets.
- Easier monitoring and eviction.
- Isolation between customer KV and Quicksilver.
- Fewer instances than one database per customer.

L1 and L2 within one dataset still share that instance's compaction and I/O resources.

**Failure isolation:** A corrupted database instance must be isolated from other dataset instances where possible. Since L1 and L2 share an instance, a database-level failure may temporarily affect both layers on that machine.

Internal cache partitioning or additional engine instances can be introduced later to reduce this failure domain.

## 8. Shared Replication and Sequences

Both datasets use the same event format.

**Sequence:** the only replication counter. It orders authoritative changes within one namespace, starting at 1, and
reflects authoritative commit order. Other sections call it the namespace revision, or the revision.

There is no dataset-wide or stream-wide counter. Each dataset has separate logical stream pools, but streams are only
roads: a node merges every stream it receives, and ordering, gap detection and resume all use the per-namespace
sequence.

```text
quicksilver-0
  infrastructure seq 408 → SET placement/A
  infrastructure seq 409 → DELETE placement/B

kv-0
  store-A seq 550 → SET foo
  store-B seq 12  → SET bar
```

An event contains:

```json
{
  "dataset": "quicksilver",
  "namespace": "infrastructure",
  "stream": "quicksilver-0",
  "sequence": 408,
  "epoch": 7,
  "committed_at": 1761400000000,
  "operation": "SET",
  "key": "d1/123/owner",
  "value": "eu-node-7"
}
```

Replicas must:

- Detect missing sequences.
- Ignore duplicates.
- Apply updates in order.
- Atomically persist applied sequences with data changes.
- Recover missing history from relays or authoritative storage.

A subscriber resumes by exchanging heads: on reconnect the relay sends the latest sequence per namespace in the
subscribed buckets, and the node requests what it has not applied. This also covers namespaces created while it was
disconnected.

A delete travels as an ordered `DELETE` event like any other change. The stream needs no separate tombstone records,
because events apply strictly in order. Deletes are recorded locally only where a node must answer "not found"
without a network round trip (the KV read path's negative caching).

Independent namespaces do not require shared total ordering.

## 9. Datacenter L1/L2 Architecture

Each datacenter operates an independent caching cluster.

### L1 — Per-Node Cache

Every leaf has a local cache containing:

- Recently requested records.
- Recent replication updates.
- Explicitly pinned records.

An optional memory layer sits above the local storage engine.

For globally replicated datasets, leaves maintain a recent-update window before ordinary eviction rules apply.

### L2 — Datacenter-Wide Cache

L2 is distributed across participating machines using internal hash buckets.

```text
US Datacenter

Regional Relays
      |
      +-- Bucket Group A → Node 1
      +-- Bucket Group B → Node 2
      +-- Bucket Group C → Node 3
      +-- Bucket Group D → Node 4
```

Each node may serve its own L1 cache and assigned L2 buckets.

Bucket assignments differ independently between datacenters.

Optional replicas provide regional redundancy.

For `global`, L2 holds the regional working set.

For `full_replica`, L2 must retain complete namespace coverage.

### Failure-Aware Placement

Regional L2 placement tracks:

- Node availability.
- Storage health.
- Bucket ownership.
- Available replica copies.
- Applied namespace revisions.

An unhealthy node must not remain advertised as a valid source for unavailable data.

Membership comes from the streams nodes hold to their cell relays: relays merge their connected-node lists and publish them as a
replicated namespace, with a grace period so a relay failover is not seen as nodes leaving. A member is a node that is receiving
updates; one cut off from every relay is not advertised as a source. Each node also keeps a local per-peer circuit breaker
fed by the outcome of real requests, and routes around peers that are failing it. Views converge but can briefly disagree,
so there is no global placement epoch: a node rejects requests for buckets it does not own, and the caller tries the next candidate.
When every candidate looks unhealthy, requests are still attempted (fail open).

### Cells

A cell is a self-contained cluster: its own membership, virtual buckets, L2 and relays. It is the unit of scaling and failure isolation.

- A datacenter is one cell by default. A cell's maximum size is set by its bucket count (2²⁰ buckets balance well up to about
  10,000 nodes). The actual size is chosen for blast radius, since a bad deploy or relay bug takes down one cell. Large sites split into several cells.
- Relays per cell grow with the node count (minimum 2), so connections and egress never sit on one machine.
- Each cell has its own bucket ownership table. Cells do not know each other's buckets.
- Reads stay inside the cell: L1 → cell L2 → cell peers → home region → S3.
- Cells apply revisions independently. A client moved between cells uses minimum-revision reads so it never sees an older value.
- Losing a cell moves its traffic to cold caches elsewhere. The home region coalesces concurrent misses and rate-limits per cell to absorb it.
- Moving traffic between cells is out of scope for now.

```text
            Home writer
                 |
           EU top relays
          /             \
   FRA DC relays      STO cell relays   (single-cell DC: no DC tier)
    /         \             |
cell-a relays  cell-b relays   L1/L2
    |              |
 L1 + L2        L1 + L2
 (own buckets)  (own buckets)
```

## 10. Regional Relays

Regional relays serve both Quicksilver and KV.

Responsibilities:

- Persistent upstream connections.
- Downstream replication fan-out.
- Namespace subscription tracking.
- L2 bucket discovery.
- Node health and replica revisions.
- Recent event replay.
- Cache-miss coordination.
- Locality-aware source selection.
- Failed-node detection and cache redistribution.

Multiple relays run in each datacenter for redundancy.

### Relay Tiers

Relays form a tree: home writer → region top relays → optional DC relays → cell relays → nodes.
The writer sends each event once per region; each cell receives it over a short link.

- Tiers are static config. Each relay has a `tier` and an ordered list of upstreams; an upstream must be in a higher tier, so there are no loops.
  A DC tier is only needed when several cells share a building, so events cross the WAN once.
- The downstream dials and the upstream pushes. A subscriber sends its subscriptions and cursor, and live events are pushed over that
  connection. Gaps and reconnects pull the missing range from the replay log. Upstreams keep no list of who is below them.
- Every node and every CDN process holds one long-lived connection to a relay in its cell, with one logical stream per channel.
- Failover walks the upstream list and resumes from the cursor. Duplicates and gaps are handled by the ordered applier.
- Subscriptions propagate up the tree with counts, so a tier only receives namespaces that someone below it wants.
- Fan-out is routed by `(namespace, bucket)`: L2 events go to the bucket's owners, not to every node. This includes `global` data and
  `full_replica` data such as Quicksilver, which is spread across each cell's L2 buckets (§3), never held whole on one node.
- Each event is encoded once and shared by all subscribers. A subscriber whose bounded queue fills is disconnected and catches up from replay.

### Two Independent Relay Channels

#### A. Replication Replay Log

A bounded event cache indexed by:

`dataset / namespace / revision`

Example:

```text
quicksilver/infrastructure
  rev 408 → SET d1/123/owner
  rev 409 → DELETE node/456

kv/customer-123/store-A
  rev 550 → SET foo
  rev 551 → DELETE bar
```

Lagging nodes request missing revisions from nearby relays before falling back to authoritative history.

Retention depends on:

- Available disk space.
- Dataset priority.
- Event age.
- Preferred replay window.
- Configured storage budgets.

Old history may be evicted when durable recovery remains possible.

Relays must accurately advertise which contiguous revision ranges are available.

#### B. Cache-Fill Broadcast Queue

A best-effort channel shares resolved cache misses.

1. A node requests an uncached value.
2. A peer or authoritative source retrieves it.
3. The relay sends the value and revision to the key's bucket owners and the namespace's L1 subscribers in the same cell.
4. Interested leaves optionally cache it.

Fills are never broadcast to every node (that grows as O(N²) with cell size) and never travel up the relay tree.

Cache-fill messages may be dropped under load.

An older cache fill must never overwrite a newer value or deletion tombstone.

### Relay Failure Handling

If a relay's local storage becomes unavailable:

- Continue forwarding live updates when safe.
- Disable unavailable local replay ranges.
- Redirect catch-up requests to other relays.
- Rebuild its replay cache from future updates or valid retained history.

A relay must not acknowledge durable ownership of data based solely on its disposable replay cache.

## 11. Quicksilver Global Update Sequence

Example: a D1 database changes ownership.

1. The authoritative writer commits the change.
2. A namespace revision is assigned in commit order.
3. The update enters the durable Quicksilver stream.
4. The replication network distributes it globally.
5. Regional relays cache recent events.
6. Relays fan out updates to eligible leaves.
7. Leaves apply changes through their storage adapter.
8. Applied revisions are committed atomically.
9. Leaves acknowledge progress.

Quicksilver is `full_replica`: every datacenter keeps the complete namespace across its L2 nodes, so a lookup that misses
L1 is one hop inside the cell while local L2 is healthy. If local L2 is unavailable, the lookup can fall through to healthy
peers, the home region, or L3.

Critical ownership transitions use fencing epochs.

## 12. Customer KV Write Sequence

Example: a namespace homed in Europe is cached in America.

1. The request reaches any API node.
2. The namespace directory identifies its home writer.
3. The request is forwarded to the authoritative region.
4. The writer durably commits the operation.
5. A namespace revision is assigned.
6. The event enters the KV stream.
7. Active subscriber regions receive the update.
8. The US relay temporarily caches the event.
9. Relevant L1/L2 nodes receive it.
10. Replicas atomically apply the update and revision.

Regions without active subscriptions do not continuously receive customer updates.

## 13. Customer KV Read Sequence

Example: a US node requests a European customer's key.

1. Check local L1.
2. On a miss, determine the regional L2 bucket owner.
3. Fetch from L2 when sufficiently fresh.
4. If L2 misses or is unavailable, select another healthy peer.
5. If no regional source is suitable, use the home-region service.
6. The home region can retrieve cold state from S3-backed L3.
7. Validate the returned value and revision.
8. Populate healthy caches according to policy.
9. Establish necessary update subscriptions and synchronize revisions.
10. Optionally broadcast the cache fill.

Subscriptions and snapshot reads must have a consistent revision boundary to prevent missed concurrent updates.

## 14. L3 — Engine-Independent S3 Persistence

S3 provides long-term recoverable state.

It stores:

- Immutable namespace snapshots.
- Archived ordered change logs.
- Versioned manifests.
- Recovery checkpoints.

Example:

```text
s3://platform/
  quicksilver/
    infrastructure/
      manifest.json
      snapshots/
      logs/

  kv/
    store-A/
      manifest.json
      snapshots/
      logs/
```

### Portable Snapshot Format

Snapshots use an engine-neutral logical format containing:

- Keys and values.
- Revisions.
- Tombstones.
- Snapshot boundaries.
- Required recovery metadata.
- Integrity checksums.

RocksDB and SurrealKV must be able to restore equivalent logical state.

Engine-native checkpoints can optionally accelerate local recovery.

### Recovery Sequence

1. Obtain the required authoritative revision.
2. Locate the latest valid snapshot.
3. Download from S3 or a suitable healthy peer.
4. Validate its integrity.
5. Restore through the configured storage adapter.
6. Replay missing durable changes.
7. Atomically mark restored data ready.
8. Resume replication.

S3 snapshots may lag acknowledged writes. Any changes after the snapshot must remain recoverable from durable logs.

For cold single-key reads, authoritative storage should provide indexed or independently retrievable objects rather than requiring an entire large namespace download.

## 15. Storage-Agnostic Fault Tolerance

The platform must recover from local storage failures independently of RocksDB or SurrealKV.

**Core principle:** A failed L1/L2 cache should reduce performance or local cache availability, not disable the entire node.

Routing, network replication, peer discovery, and recovery should continue operating whenever the process and network remain healthy.

### 15.1 Failure Detection

Storage adapters distinguish:

- Successful reads.
- Keys not present locally.
- I/O failures.
- Corruption.
- Storage unavailability.

Errors are reported to a shared storage-health manager.

The health manager tracks:

- Dataset and engine instance.
- Affected cache layer.
- Affected bucket or key range, if identifiable.
- Error frequency.
- Current health state.
- Recovery attempts.

### 15.2 Failure Isolation

When a storage failure occurs:

1. Identify the affected storage instance or partition.
2. Mark the affected data source unhealthy.
3. Stop routing requests to the damaged cache.
4. Continue operating unaffected dataset instances.
5. Redirect affected requests to healthy sources.
6. Schedule cache rebuilding.

Prefer the smallest safe isolation boundary.

If the storage adapter cannot determine whether corruption is limited to an individual key, the system must conservatively quarantine the affected database instance or a larger verified failure domain.

Corruption must not be treated as an ordinary cache miss.

### 15.3 Automatic Read Fallback

Normal read hierarchy:

```text
Local L1
   ↓
Regional L2
   ↓
Healthy Regional Peers
   ↓
L3: Home Region / S3-Backed Storage
```

When a tier fails, bypass it and continue through the hierarchy.

The system must avoid repeated attempts against known-unhealthy sources until they are eligible for health rechecks.

Fallback sources must satisfy the request's revision and freshness requirements.

A healthy but outdated replica is not automatically a valid replacement.

### 15.4 Regional Relay Recovery

Regional relays maintain node health and cache-location information.

When a leaf or L2 holder becomes unavailable:

1. Mark its affected assignments unhealthy.
2. Identify healthy nodes containing the data.
3. Redirect requests to those nodes.
4. Update regional cache placement.
5. Schedule rebuilding or repopulation.

Directory entries use health information to avoid routing to failed holders. A request that still reaches an outdated owner is rejected by that owner and retried elsewhere.

### 15.5 L2 Bucket Redistribution

If an L2 node fails:

1. Detect the failed holder.
2. Select replacement nodes using regional placement rules.
3. Assign the missing buckets to healthy nodes.
4. Repopulate from existing replicas or authoritative storage.
5. Verify integrity and required revisions.
6. Mark the replacement buckets ready.

For ordinary caches, missing buckets may temporarily fall through to L3.

For `full_replica`, loss of required coverage must be reported as degraded until rebuilding restores the configured redundancy and completeness.

### 15.6 Automatic Cache Rebuilding

Affected caches can be discarded and recreated.

Recovery may involve:

- Reinitializing a corrupted storage instance.
- Restoring individual keys.
- Rebuilding individual L2 bucket ranges.
- Replaying recent namespace revisions.
- Restoring snapshots from S3.

The recovery coordinator operates above the storage engine, using the common storage abstraction.

Rebuilt entries are not made available until they pass integrity and freshness validation.

### 15.7 Granular Recovery

Prefer recovery at the smallest practical logical level.

| Failure | Preferred recovery |
|---|---|
| Missing cached key | Fetch key from healthy source |
| Corrupted cached entry | Quarantine and refetch if safely identifiable |
| Failed L2 bucket | Reassign and repopulate bucket |
| Corrupted storage instance | Recreate instance and restore required cache state |
| Failed local disk | Bypass disk and reconstruct on replacement storage |

Physical storage-block recovery is optional and engine-specific.

The generic replication layer should not depend on the ability to repair RocksDB SST blocks or SurrealKV internal pages.

### 15.8 Data Integrity

Recovered records must be validated using:

- Checksums.
- Namespace revisions.
- Writer epochs.
- Versioned tombstones.
- Verified snapshot boundaries.
- Authoritative freshness requirements.

A corrupted or stale value must never overwrite a newer, verified value.

Deleted records must not reappear because an outdated peer supplied an earlier snapshot.

Checksums detect many forms of corruption, but do not by themselves prove that a value is the latest authoritative version.

### 15.9 Durability Separation

L1 and L2 caches are disposable.

They can be recreated from healthy peers and authoritative history.

However, **authoritative writers and durable logs are not disposable caches**.

A home-writer storage failure must trigger protected recovery or failover using:

- Durable committed history.
- Writer epochs.
- Leadership fencing.
- Verified recovery state.

No replacement writer may acknowledge new writes until authoritative ownership and required committed history are established.

### 15.10 Service Availability

If the KV dataset's local storage fails, the Quicksilver dataset should continue operating when its own storage and dependencies remain healthy.

Similarly, a failed relay replay database should not unnecessarily stop the relay's network-forwarding functionality.

The system should isolate recoverable component failures without assuming that all engine failures can be contained inside a running process.

## 16. Cache Lifecycle and Eviction

Every datacenter independently manages cache retention.

| State | Meaning |
|---|---|
| HOT | Cached in memory and local storage |
| WARM | Retained locally, not in memory |
| COLD | Evicted locally; recoverable elsewhere |
| UNHEALTHY | Local data cannot currently be trusted |
| REBUILDING | Cache is being restored and validated |

Eviction considers:

- Access frequency.
- Record age.
- Storage pressure.
- Dataset priority.
- Namespace policy.
- Active subscriptions.

Unhealthy data is excluded from routing until verified recovery succeeds.

`global` permits ordinary eviction, while `full_replica` requires complete regional coverage.

## 17. Consistency and Failure Handling

The system provides:

- Ordered namespace revisions.
- Duplicate-safe replication.
- Missing-revision detection.
- Atomic data and cursor commits.
- Writer epochs and fencing.
- Versioned tombstones.
- Durable snapshot and log recovery.
- Independent downstream progress.
- Minimum-revision read options.
- Automatic healthy-source fallback.
- L2 placement recovery.
- Storage-integrity validation.

### Freshness Guarantees

Global propagation remains asynchronous.

A node may temporarily observe older metadata.

Security-sensitive actions such as ownership changes and access revocations require fencing, authoritative validation, or fail-closed behavior.

Storage failure fallback must not weaken these guarantees.

## 18. Observability

Monitor:

- Replication latency and revision lag.
- L1/L2 cache hit rates.
- Relay replay coverage.
- L2 bucket placement and redundancy.
- Engine-specific I/O errors.
- Corruption detections.
- Storage-instance health.
- Quarantined cache entries or partitions.
- Cache fallback rates and latency.
- Automatic recovery success rates.
- Rebuild duration.
- S3 recovery latency.
- Compaction and write stalls.
- Per-dataset storage capacity.
- Full-replica completeness.
- Node and relay health.
- End-to-end replication lag: every event carries `committed_at`, and every hop reports `now - committed_at`.

Rules:

- Metrics use only low-cardinality labels (dataset, tier, cell, service, node). Per-namespace, per-key and per-subscriber state
  is exposed through introspection APIs (relays: per-namespace revision, sequence, subscribers and replay range), or as top-N metrics.
- Relays expose an admin introspection API for upstream state, namespaces, subscribers and the published membership view.

## 19. Implementation Phases

### Phase 1 — Shared Engine and Storage Abstraction

Build the single Rust binary, datasets, namespaces, policy resolution, Docker deployment, RocksDB adapter, authoritative writes, ordered replication, and S3-backed durability.

Define structured storage errors and atomic replication revision updates.

### Phase 2 — Storage Fault Tolerance

Implement the health manager, unhealthy-cache bypass, source fallback, integrity verification, and cache rebuilding.

Add fault-injection and crash-recovery testing.

### Phase 3 — SurrealKV Adapter

Implement the same storage contract using SurrealKV.

Validate transaction semantics, recovery behavior, corruption reporting, and performance against RocksDB.

### Phase 4 — Hierarchical Replication

Introduce regional relays, persistent update streams, bounded replay history, and independent cache-fill channels.

### Phase 5 — Regional Caching

Implement per-node L1, distributed L2 buckets, healthy-peer discovery, on-demand subscriptions, and automatic redistribution.

### Phase 6 — Advanced Optimization

Add `full_replica` placement, adaptive cache promotion, regional load balancing, improved recovery, and optional engine-specific repair mechanisms.

---

## Core Architectural Principles

1. **One Rust binary** runs Quicksilver and customer KV.
2. **Dataset → Namespace** is the logical data hierarchy.
3. **RocksDB and SurrealKV** implement a shared storage abstraction.
4. **Each dataset has its own storage-engine instance** on applicable nodes.
5. **L1 and L2 share a dataset's local engine instance** but use separate logical keyspaces.
6. **Regional relays use separate storage** for bounded replay history.
7. **Docker with host networking and NVMe bind mounts** is the default deployment.
8. **Global, on-demand, home-only, and full-replica policies** control replication and retention.
9. **Every cell independently operates an L1/L2 cluster.** A datacenter is one or more cells.
10. **A tree of relays coordinates replication, cache discovery, and recent event replay.**
11. **Cache-fill broadcasts remain separate from authoritative replication.**
12. **S3 persists engine-independent snapshots and archived logs.**
13. **Local cache failures trigger automatic fallback and rebuilding.**
14. **Unhealthy storage is isolated without unnecessarily disabling unrelated services.**
15. **L2 bucket ownership is redistributed when cache nodes fail.**
16. **Recovered data is validated against revisions, checksums, epochs, and tombstones.**
17. **L1/L2 caches are disposable; authoritative durable history is not.**
18. **Replication correctness and recovery guarantees remain independent of the storage adapter.**

**Result:** A unified, globally distributed KV platform with interchangeable Rust storage engines, S3-backed durability, Quicksilver-style replication, regional caching, and automatic recovery from local storage failures.
