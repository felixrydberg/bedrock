# CDN Implementation Plan

Build CDN on top of the existing Bedrock KV infrastructure. Reuse distributed functionality rather than implementing it again.

## 1. KV Prerequisites

The following KV implementation phases must be completed before the corresponding CDN functionality:

- **KV Phase 1:** Dataset model, replication sequences, S3 integration, and service infrastructure.
- **KV Phase 2:** Storage health, failure detection, and recovery infrastructure.
- **KV Phase 4:** Regional relays, replication streams, subscriptions, and replay.
- **KV Phase 5:** Regional bucket distribution, node discovery, placement, and redistribution.

KV Phase 3 (SurrealKV) and Phase 6 (advanced optimization) are not prerequisites.

The CDN will reuse KV's distributed functionality under `bedrock/lib/distributed/`, including membership, health, buckets, placement, replication, relays, and reconciliation.

## 2. Code Structure

```text
bedrock/
├── lib/distributed/     # Shared with KV
├── kv/                  # Existing KV
└── cdn/
    ├── artifact_storage
    ├── artifact_transfer
    ├── manifests
    └── retention
```

## 3. CDN-Specific Implementation

### Artifact Storage

- Store immutable artifacts in S3 using SHA-256 hashes.
- Implement a pluggable `CacheStorage` abstraction.
- Implement Foyer as the initial cache backend.
- Support filesystem storage as an alternative.
- Maintain local artifact inventories and verify checksums.
- Support protected L2 storage and optional unreplicated L1 caching.

### Artifact Transfer

- Implement streaming artifact transfers between storage nodes.
- Retrieve missing artifacts from S3.
- Coalesce concurrent requests for identical artifacts.
- Support retries, timeouts, and transfer backpressure.
- Keep bulk transfers separate from relay traffic.

### Regional L2

Reuse KV's regional buckets and placement system (§9–10).

- Map artifact hashes to virtual buckets.
- Maintain two verified replicas per retained artifact.
- Use existing placement and health infrastructure to select replica holders.
- Track local artifact availability independently of bucket ownership.
- Implement CDN-specific replica repair and bucket migration.
- Allow L1 to cache requests independently, with automatic eviction.

### Declarative Retention

Reuse KV's replication and sequencing system (§8).

- Introduce a `cdn` dataset with regional retention namespaces.
- Implement `RETAIN_MANIFEST`, `RELEASE_MANIFEST`, `RETAIN_ARTIFACT`, and `RELEASE_ARTIFACT`.
- Expand manifests into artifact retention requirements.
- Track shared artifact references across deployments.
- Reconcile desired replica counts against local inventories.
- Recover retention state using existing replay and snapshot mechanisms.
- Avoid globally tracking individual artifact locations.

### Deployment Integration

The CDN is a backing service and knows nothing about deployments. It provides:

- Immutable manifests mapping paths to artifact hashes, and a publish client that returns a `manifest_hash` only once it is stored and retained.
- Retrieval by manifest and artifact hash.
- Prewarming on retain, readiness per manifest, and artifact garbage collection.

Workers, Pages, deployments, activation and rollbacks belong to the Compute and deploy control plane, a client of the CDN and of
Quicksilver. It publishes before it activates, and resolves which manifest is live itself.

## 4. Implementation Phases

### Phase 1 — Artifact Foundation

*Requires KV Phase 1.*

- S3 content-addressed artifact storage.
- `CacheStorage` abstraction and Foyer adapter.
- Artifact transfer and checksum verification.
- Manifests and the publish client.
- Hash-addressed artifact retrieval (Worker and Pages front ends are part of the deploy control plane).

### Phase 2 — Distributed CDN

*Requires KV Phases 2, 4, and 5.*

- Register CDN with shared relays.
- Implement regional retention streams.
- Reuse bucket placement for artifact distribution.
- Implement two-replica L2 storage.
- Implement peer-to-peer transfers and fallback.
- Track replica availability.

### Phase 3 — Retention and Recovery

*Reuses KV replication, fault tolerance, and reconciliation infrastructure.*

- Manifest-based retention.
- Automatic replica repair.
- Bucket migration and rebalancing.
- Coordinated eviction.
- Recovery after node and relay failures.
- Reconciliation without per-artifact global metadata.

### Phase 4 — Optimization

- Optional unreplicated L1 caching.
- Prewarming and manifest readiness.
- Popularity-based retention.
- Compiled Worker artifact caching moves to Compute; it uses the CDN as ordinary artifacts.
- Large-object and range transfer optimization.
- Additional storage adapters.

---

## Core Principles

1. **Reuse KV's distributed infrastructure** for membership, health, relays, placement, and replication.
2. **S3 remains authoritative** for artifact bytes.
3. **L2 maintains two regional replicas** of retained artifacts.
4. **L1 is optional and unreplicated.**
5. **Replication streams describe desired retention**, not artifact bytes.
6. **Relays coordinate communication**, while storage nodes transfer artifacts directly.
7. **Cache storage is pluggable**, independently of KV's storage engines.
8. **Only CDN-specific behavior belongs under `bedrock/cdn/`.**
9. **Shared infrastructure, separate data.** The CDN subscribes only to its own `cdn` dataset, never to KV datasets such as
   Quicksilver, and KV never subscribes to `cdn`.