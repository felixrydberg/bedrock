# Bedrock — Feature Implementation Drafts

Draft GitHub issues for building Bedrock, in the order they should be done. Every draft uses
`.github/ISSUE_TEMPLATE/feature_request.md` (title prefix `Feature 🚀:`, label `enhancement`).
The **Additional context** section of each draft says which implementation it belongs to,
which module it lives in, which doc section it comes from, what it depends on, and when it counts as done.

Sources: `docs/implementation-prototype/kv.md`, `cdn.md`, `code-structure.md`, `notes.md`.

> Status: B-01 to B-23 (including B-18b, B-22b and B-23a, but not B-09b) are created as GitHub issues (linked below, the issue is the source of truth). Later drafts are still drafts. When a draft is created, replace its body here with a link to the issue.

---

## 1. Strategy

The docs describe two products, KV and CDN, built on one shared distributed layer (`lib/`).
The CDN doc depends on KV phases 1, 2, 4 and 5. Read closely, almost all of those phases are
**shared behaviour** (membership, health, buckets, placement, replication, relays, reconciliation),
not KV-specific behaviour. So the order is:

1. **Foundation.** Workspace, the `bedrock-kv` and `bedrock-cdn` binaries, roles, transport.
2. **Shared core (`lib/`).** Everything KV *and* CDN need. It has to be built and hardened
   first, with its own test harness, before either product uses it.
3. **First KV** (`kv/`) and **first CDN** (`cdn/`). Thin product layers on the hardened core.
   The two tracks can run in parallel.
4. **KV refinements** and **CDN refinements**. Product-specific depth: optimisations,
   extra adapters, advanced retention. These can also run in parallel.

Things deliberately left out of the shared core because only one product uses them:
the cache-fill broadcast channel (KV only), SurrealKV (KV phase 3), `full_replica` (KV phase 6),
and every CDN phase 3/4 item. Performance and protocol constraints such as sequence compression
are out of scope for this plan.

### Code structure followed

```text
crates/
├── lib/          # bedrock-lib — shared distributed core (library only)
│   ├── membership  health  buckets  placement  replication  relays  reconciliation
│   └── storage  datasets  s3  transport                                              (decided in B-01, see §4)
├── api/          # bedrock/api — write API (B-09)                                         → binary `bedrock-api`
├── relay/        # bedrock/relay — relay process: tiers, fan-out, replay, membership      → binary `bedrock-relay`
├── kv/           # bedrock/kv  — storage (keyspaces/eviction), namespaces, operations  → binary `bedrock-kv`
└── cdn/          # bedrock/cdn — artifact_storage, artifact_transfer, manifests, retention → binary `bedrock-cdn`
```

### Deployment model: separate binaries, one shared library

Each service ships as **its own binary** linking `lib`. Every machine runs `bedrock-kv` and `bedrock-cdn`, as separate
processes (containers) with their own resource limits, deploys and failure domains. `bedrock-relay` runs on a few machines
per cell (minimum 2, more as the cell grows) and per upper tier, and `bedrock-api` in the core region. This matches the public Cloudflare model: every
server runs every service, each service is its own process, and code is shared as libraries.

| Binary | Hosts | Roles |
|---|---|---|
| `bedrock-api` | Authoritative writes and revision ordering (B-09) | — |
| `bedrock-relay` | Relay tree, fan-out, replay log, cell membership, regional directory | — (`tier` and `upstreams` in config) |
| `bedrock-kv` | Datasets `quicksilver`, `kv`, `auth` | `l1`, `l2` |
| `bedrock-cdn` | Dataset `cdn` (retention metadata) and artifact bytes: storage, transfer, regional L2 placement, serving | `l1`, `l2` (artifact cache roles) |

Rules this implies:

- **Relays are their own binary.** `bedrock-relay` holds no product logic: it is generic over datasets and uses `lib`
  for everything (`lib/relays` for fan-out and tiers, `lib/replication` for streams, `lib/storage` RocksDB for its replay log,
  `lib/membership` for the cell view). A KV or CDN crash never takes a relay with it, and relays deploy on their own schedule.
- **`kv`, `cdn`, `relay` and `api` depend on `lib` only, never on each other.**
- **KV and CDN share infrastructure, not data.** Both write through `bedrock-api` and subscribe through `bedrock-relay`,
  reusing one replication and sequencing system. `bedrock-kv` subscribes only to KV datasets (`quicksilver`, `kv`, `auth`), and
  `bedrock-cdn` only to the `cdn` dataset. Neither ever subscribes to the other's datasets.
- **Retention operations are plain `SET`/`DELETE` writes** to keys in the `cdn` dataset. The write API and relays are generic,
  and only `bedrock-cdn` interprets them when it applies them (manifest expansion, reference counts).
- **The CDN serves by hash and knows nothing about deployments.** Workers, Pages, deployments, revisions and which version is live
  belong to the Compute and deploy control plane (track L), a client of KV and the CDN (B-50, B-51).
- **Each binary has its own service view.** Membership, health, placement, buckets and the regional directory run
  inside each binary for that service's nodes. A node with a broken CDN cache disk is still a healthy KV node, and the reverse.
- **Both binaries need a durable local engine**, so the `StorageEngine` adapters live in `lib/storage`, not `kv/storage`.

---

## 2. Implementation order

| # | Draft | Implementation | Module | Depends on |
|---|---|---|---|---|
| **A — Foundation** |||||
| [B-01](https://github.com/felixrydberg/bedrock/issues/1) | Workspace restructure into `lib` plus `bedrock-kv`, `bedrock-cdn`, `bedrock-relay` and `bedrock-api` binaries | Foundation | workspace | — |
| [B-02](https://github.com/felixrydberg/bedrock/issues/2) | Node configuration, roles and Docker deployment for `kv`, `cdn` and `relay` | Foundation | `kv` + `cdn` + `relay` bins | B-01 |
| [B-23a](https://github.com/felixrydberg/bedrock/issues/26) | Deterministic simulation runtime (`madsim`) and I/O rules, before any I/O code | Shared core | workspace + `lib` (test support) | B-01 |
| [B-03](https://github.com/felixrydberg/bedrock/issues/3) | Node transport and observability baseline | Shared core | `lib/transport` | B-01, B-23a |
| **B — Shared core: local state** |||||
| [B-04](https://github.com/felixrydberg/bedrock/issues/4) | `StorageEngine` contract and error model | Shared core | `lib/storage` | B-01, B-23a |
| [B-05](https://github.com/felixrydberg/bedrock/issues/5) | RocksDB storage adapter | Shared core | `lib/storage` | B-04 |
| [B-06](https://github.com/felixrydberg/bedrock/issues/6) | Datasets, namespaces and policy resolution | Shared core | `lib/datasets` | B-04 |
| [B-07](https://github.com/felixrydberg/bedrock/issues/7) | L3 S3 client | Shared core | `lib/s3` | B-01, B-23a |
| **C — Shared core: replication** |||||
| [B-08](https://github.com/felixrydberg/bedrock/issues/8) | Replication event model and ordered applier | Shared core | `lib/replication` | B-04, B-06 |
| [B-09](https://github.com/felixrydberg/bedrock/issues/9) | Dev write API (Axum, single node) batching writes to S3 | Shared core | `lib/replication` + `api` bin | B-08, B-02, B-07 |
| [B-10](https://github.com/felixrydberg/bedrock/issues/10) | Stream subscriptions with consistent revision boundary | Shared core | `lib/replication` | B-09, B-03 |
| [B-11](https://github.com/felixrydberg/bedrock/issues/11) | Portable snapshots, log archive and recovery | Shared core | `lib/replication` | B-09, B-07 |
| B-09b | Raft write quorum for the API (later) | Shared core | `lib/replication` + `api` bin | B-09, B-10, B-11, B-13 |
| **D — Shared core: cluster** |||||
| [B-13](https://github.com/felixrydberg/bedrock/issues/12) | Peer reachability from real traffic | Shared core | `lib/health` | B-03 |
| [B-14](https://github.com/felixrydberg/bedrock/issues/13) | Storage-health manager and failure isolation | Shared core | `lib/health` | B-04, B-13 |
| **F — Shared core: relays** (before E: membership comes from relay streams) |||||
| [B-17](https://github.com/felixrydberg/bedrock/issues/14) | `bedrock-relay` binary: tiers, subscriber-initiated streams, bucket-routed fan-out | Shared core | `lib/relays` + `relay` bin | B-10, B-13 |
| [B-12](https://github.com/felixrydberg/bedrock/issues/15) | Membership from relay streams, published as a dataset | Shared core | `lib/membership` + `lib/relays` | B-10, B-17 |
| [B-18](https://github.com/felixrydberg/bedrock/issues/16) | Relay replay log | Shared core | `lib/relays` | B-17, B-05 |
| [B-18b](https://github.com/felixrydberg/bedrock/issues/17) | Relay introspection and metrics | Shared core | `lib/relays` + `relay` bin | B-17, B-18, B-12 |
| **E — Shared core: regional distribution** |||||
| [B-15](https://github.com/felixrydberg/bedrock/issues/18) | Placement: rendezvous hashing and replica selection | Shared core | `lib/placement` | B-12, B-13 |
| [B-16](https://github.com/felixrydberg/bedrock/issues/19) | Virtual buckets and ownership | Shared core | `lib/buckets` | B-15 |
| [B-19](https://github.com/felixrydberg/bedrock/issues/20) | Regional directory and source selection | Shared core | `lib/relays` | B-16, B-12 |
| **G — Shared core: reconciliation, recovery, hardening** |||||
| [B-20](https://github.com/felixrydberg/bedrock/issues/21) | Reconciliation framework | Shared core | `lib/reconciliation` | B-16 |
| [B-21](https://github.com/felixrydberg/bedrock/issues/22) | L2 bucket redistribution on node failure | Shared core | `lib/buckets` + `lib/reconciliation` | B-19, B-20 |
| [B-22](https://github.com/felixrydberg/bedrock/issues/23) | Tiered read fallback and cache rebuild | Shared core | `lib/health` + `lib/reconciliation` | B-14, B-19, B-11 |
| [B-22b](https://github.com/felixrydberg/bedrock/issues/24) | Home-region read protection: coalescing and per-cell limits | Shared core | `lib/reconciliation` + home-region service | B-22 |
| [B-23](https://github.com/felixrydberg/bedrock/issues/25) | Hardening gate: TLA+ model, deterministic simulation, real-binary crash tests | Shared core | `lib` (test support), `docs/spec/` | B-23a, B-01 … B-22b |
| **H — First KV** |||||
| B-24 | Quicksilver dataset | KV | `kv/namespaces` | B-23 |
| B-25 | KV namespace directory and management | KV | `kv/namespaces` | B-24 |
| B-26 | KV write path | KV | `kv/operations` | B-25 |
| B-27 | KV read path through L1/L2 | KV | `kv/operations`, `kv/storage` | B-26 |
| B-28 | Replication policies and basic eviction | KV | `kv/storage`, `kv/namespaces` | B-27 |
| B-29 | KV end-to-end acceptance | KV | `kv` | B-24 … B-28 |
| **I — First CDN** (parallel with H; no dependency on KV drafts) |||||
| B-30 | Artifact storage: S3 content addressing, `CacheStorage`, Foyer | CDN | `cdn/artifact_storage` | B-23 |
| B-31 | Artifact transfer | CDN | `cdn/artifact_transfer` | B-30 |
| B-32 | `cdn` dataset and retention operations | CDN | `cdn/retention` | B-23 |
| B-33 | Manifests and publish client | CDN | `cdn/manifests` | B-30, B-32 |
| B-34 | Regional L2 with two-replica artifact placement | CDN | `cdn/retention`, `cdn/artifact_storage` | B-31, B-32 |
| B-35 | Artifact retrieval interface | CDN | `cdn/artifact_transfer` | B-33, B-34 |
| B-36 | CDN end-to-end acceptance | CDN | `cdn` | B-30 … B-35 |
| **J — KV refinements** |||||
| B-37 | Cache-fill broadcast queue | KV | `lib/relays`, `kv/operations` | B-29 |
| B-38 | Full cache lifecycle and eviction | KV | `kv/storage` | B-29 |
| B-39 | SurrealKV storage adapter | KV | `lib/storage` | B-29 |
| B-40 | `full_replica` placement and completeness | KV | `kv/storage`, `lib/buckets` | B-29 |
| B-41 | Adaptive cache promotion and regional load balancing | KV | `kv/operations`, `lib/relays` | B-38 |
| B-42 | Auth dataset | KV | `kv/namespaces` | B-29 |
| **K — CDN refinements** (parallel with J) |||||
| B-43 | Artifact replica repair and bucket migration | CDN | `cdn/retention` | B-36 |
| B-44 | Coordinated eviction, GC and failure recovery | CDN | `cdn/retention` | B-43 |
| B-45 | Prewarming and manifest readiness | CDN | `cdn/manifests` | B-36 |
| B-46 | Optional unreplicated L1 and popularity-based retention | CDN | `cdn/artifact_storage`, `cdn/retention` | B-44 |
| B-47 | Filesystem `CacheStorage` adapter | CDN | `cdn/artifact_storage` | B-36 |
| B-49 | Large-object and range transfers | CDN | `cdn/artifact_transfer` | B-36 |
| **L — Compute and deploy control plane (future)** |||||
| B-50 | Deploy control plane: publish, activate, roll back, delete | Compute | not decided | B-24, B-33, B-45 |
| B-51 | Worker and Pages request front ends | Compute | not decided | B-35, B-50 |
| B-48 | Compiled Worker artifact caching | Compute | not decided | B-51 |
| B-52 | Deploy end-to-end acceptance | Compute | not decided | B-29, B-36, B-50, B-51 |

### Parallel work inside the shared core

- B-04/B-05/B-06 and B-07 have no dependency on each other.
- Track D (B-13, B-14) can start as soon as B-03 and B-04 are done, alongside track C.
- Track F (B-17, B-12, B-18) needs B-10 and B-13. Track E (B-15, B-16, B-19) needs B-12. Track G needs everything before it.

### Mapping to the doc phases

| Doc phase | Where it lands |
|---|---|
| KV Phase 1 (engine, datasets, writes, replication, S3) | B-01 … B-11, plus KV parts in B-24 … B-26 |
| KV Phase 2 (storage fault tolerance) | B-14, B-22, B-23a, B-23 |
| KV Phase 3 (SurrealKV) | B-39 |
| KV Phase 4 (relays, streams, replay) | B-17 … B-19 (cache-fill moved to B-37) |
| KV Phase 5 (L1/L2, buckets, discovery, redistribution) | B-15, B-16, B-19, B-21, B-27, B-28 |
| KV Phase 6 (optimisation) | B-38, B-40, B-41 |
| CDN Phase 1 (artifact foundation) | B-30, B-31, B-33, B-35 |
| CDN Phase 2 (distributed CDN) | B-32, B-34 |
| CDN Phase 3 (retention and recovery) | B-43, B-44 |
| CDN Phase 4 (optimisation) | B-45, B-46, B-47, B-49 (compiled Worker caching moved to B-48 in track L) |
| Deployment parts of the CDN doc (activation, rollbacks, Worker/Pages retrieval) | B-50, B-51, B-48, B-52 |

---

## 3. Drafts

### A — Foundation

---

#### B-01 — Workspace restructure into `lib` plus `bedrock-kv` and `bedrock-cdn` binaries

Created: [#1](https://github.com/felixrydberg/bedrock/issues/1) — Feature 🚀: Restructure workspace into a shared lib crate and separate bedrock-kv / bedrock-cdn / bedrock-relay / bedrock-api binaries

---

#### B-02 — Node configuration, roles and Docker deployment

Created: [#2](https://github.com/felixrydberg/bedrock/issues/2) — Feature 🚀: Node configuration, role selection and Docker deployment for bedrock-kv, bedrock-cdn and bedrock-relay

---

#### B-03 — Node transport and observability baseline

Created: [#3](https://github.com/felixrydberg/bedrock/issues/3) — Feature 🚀: Node-to-node transport and observability baseline

---

### B — Shared core: local state

---

#### B-04 — `StorageEngine` contract and error model

Created: [#4](https://github.com/felixrydberg/bedrock/issues/4) — Feature 🚀: StorageEngine abstraction, structured storage errors and contract test suite

---

#### B-05 — RocksDB storage adapter

Created: [#5](https://github.com/felixrydberg/bedrock/issues/5) — Feature 🚀: RocksDB adapter implementing the StorageEngine contract

---

#### B-06 — Datasets, namespaces and policy resolution

Created: [#6](https://github.com/felixrydberg/bedrock/issues/6) — Feature 🚀: Dataset → Namespace model, policy resolution and per-dataset engine instances

---

#### B-07 — L3 S3 client

Created: [#7](https://github.com/felixrydberg/bedrock/issues/7) — Feature 🚀: Shared S3 (L3) client with checksums and versioned manifests

---

### C — Shared core: replication

---

#### B-08 — Replication event model and ordered applier

Created: [#8](https://github.com/felixrydberg/bedrock/issues/8) — Feature 🚀: Shared replication event format and ordered, duplicate-safe applier

---

#### B-09 — Dev write API: single node, batched writes to S3

Created: [#9](https://github.com/felixrydberg/bedrock/issues/9) — Feature 🚀: Dev write API (Axum, single node) batching ordered writes to S3 and emitting them to the stream

---

#### B-09b — Replicated write quorum (Raft) for the write API

```
Title:  Feature 🚀: Raft quorum for the write API with S3 archive and automatic failover
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The dev API (B-09) is a single point of failure and waits on S3 for every acknowledgement.
At scale the write path needs automatic failover, fencing of old leaders and low commit latency.

**Describe the solution you'd like**
- Run `bedrock-api` as a 3 or 5 node Raft group (`openraft`) in the core region. One leader is the only writer, the rest are followers.
- Acknowledge a write once a majority has fsynced it. The state machine assigns per-namespace revisions when entries are applied. The Raft term fences old leaders.
- The leader batches committed entries and archives them to S3 with deterministic keys (`If-None-Match: *`). After a leader change the new leader reads the last archived batch from S3 and continues. A repeated upload is a harmless 412.
- Raft log truncation only after the batch is archived and snapshotted.
- Committed entries are published to the dataset stream (B-10), from the leader or a caught-up follower.
- Start with one group for the whole core region. Splitting into groups per namespace range is a later step.

**Additional context**
- **Implementation:** Shared core.
- **Module:** `lib/replication` plus the `api` crate.
- **Doc reference:** `kv.md` §8, §11, §12, §15.9.
- **Depends on:** B-09, B-10, B-11, B-13.
- **Known limits:** writes stop without a majority. Entries not yet archived are lost if a majority of nodes is lost at once, so run nodes in separate failure domains and archive frequently.
- **Acceptance criteria:** Killing the leader loses no acknowledged write and a new leader accepts writes within the election timeout. An old-term leader cannot commit. The new leader does not duplicate or skip archived batches. Revisions stay gap-free across failover.

---

#### B-10 — Stream subscriptions with consistent revision boundary

Created: [#10](https://github.com/felixrydberg/bedrock/issues/10) — Feature 🚀: Replication stream subscriptions and catch-up from a consistent revision boundary

---

#### B-11 — Portable snapshots, log archive and recovery

Created: [#11](https://github.com/felixrydberg/bedrock/issues/11) — Feature 🚀: Engine-neutral S3 snapshots, archived logs and snapshot+replay recovery

---

### D — Shared core: cluster

---

#### B-12 — Membership from relay streams, published as a dataset

Created: [#15](https://github.com/felixrydberg/bedrock/issues/15) — Feature 🚀: Cell membership derived from relay subscriber streams and published as a replicated namespace

---

#### B-13 — Peer reachability from real traffic

Created: [#12](https://github.com/felixrydberg/bedrock/issues/12) — Feature 🚀: Per-peer circuit breaker driven by real traffic, with fail-open routing

---

#### B-14 — Storage-health manager and failure isolation

Created: [#13](https://github.com/felixrydberg/bedrock/issues/13) — Feature 🚀: Storage-health manager with error tracking, quarantine and isolation

---

### E — Shared core: regional distribution

---

#### B-15 — Placement: rendezvous hashing and replica selection

Created: [#18](https://github.com/felixrydberg/bedrock/issues/18) — Feature 🚀: Rendezvous-hash placement with health-aware replica selection and failure domains

---

#### B-16 — Virtual buckets and ownership

Created: [#19](https://github.com/felixrydberg/bedrock/issues/19) — Feature 🚀: Virtual bucket mapping and per-cell ownership table with owner-side checks

---

### F — Shared core: relays

---

#### B-17 — `bedrock-relay` binary: tiers, fan-out, subscriptions

Created: [#14](https://github.com/felixrydberg/bedrock/issues/14) — Feature 🚀: bedrock-relay binary with relay tiers, subscriber-initiated streams, bucket-routed fan-out and subscription tracking

---

#### B-18 — Relay replay log

Created: [#16](https://github.com/felixrydberg/bedrock/issues/16) — Feature 🚀: Bounded relay replay log with advertised revision ranges

---

#### B-18b — Relay introspection and metrics

Created: [#17](https://github.com/felixrydberg/bedrock/issues/17) — Feature 🚀: Relay introspection API and low-cardinality relay metrics with end-to-end lag

---

#### B-19 — Regional directory and source selection

Created: [#20](https://github.com/felixrydberg/bedrock/issues/20) — Feature 🚀: Regional directory for bucket discovery, replica revisions and locality-aware source selection

---

### G — Shared core: reconciliation, recovery and hardening

---

#### B-20 — Reconciliation framework

Created: [#21](https://github.com/felixrydberg/bedrock/issues/21) — Feature 🚀: Generic desired-vs-actual reconciliation loop with repair scheduling

---

#### B-21 — L2 bucket redistribution on node failure

Created: [#22](https://github.com/felixrydberg/bedrock/issues/22) — Feature 🚀: Automatic L2 bucket reassignment and repopulation after node failure

---

#### B-22 — Tiered read fallback and cache rebuild

Created: [#23](https://github.com/felixrydberg/bedrock/issues/23) — Feature 🚀: Generic tiered read fallback and validated cache rebuilding

---

#### B-22b — Home-region read protection: coalescing and per-cell limits

Created: [#24](https://github.com/felixrydberg/bedrock/issues/24) — Feature 🚀: Request coalescing and per-cell rate limits on the home-region read service

---

#### B-23a — Deterministic simulation runtime and I/O rules

Created: [#26](https://github.com/felixrydberg/bedrock/issues/26) — Feature 🚀: Deterministic simulation runtime and I/O rules for the shared core

Done right after B-01, before any I/O code (B-03, B-04, B-07 onwards follow its rules). The ID stays next to B-23 because it exists for B-23.

---

#### B-23 — Shared-core hardening gate: TLA+ model, deterministic simulation and crash testing

Created: [#25](https://github.com/felixrydberg/bedrock/issues/25) — Feature 🚀: Shared-core hardening gate: TLA+ model, deterministic simulation and crash testing

---

### H — First KV implementation

---

#### B-24 — Quicksilver dataset

```
Title:  Feature 🚀: Quicksilver infrastructure metadata dataset
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Infrastructure metadata needs a globally replicated, pinned store: routing, ownership (D1/DO/KV), system configuration,
and later deployment records written by the deploy control plane (B-50). The KV namespace directory (B-25) reads from it.
Quicksilver is a KV dataset only: the CDN never reads it.

**Describe the solution you'd like**
- A `quicksilver` dataset with an `infrastructure` namespace and the `global` policy.
- Pinned prefixes (`critical/ownership/`, `critical/routing/`) protected from eviction.
- Generic metadata keys and values. Typed schemas for specific consumers (for example Worker deployment config) belong to
  those consumers, not to this dataset.
- Ownership transitions use fencing epochs (B-09).

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/namespaces`.
- **Doc reference:** `kv.md` §1, §4, §11; `notes.md` (Quicksilver).
- **Depends on:** B-23.
- **Acceptance criteria:** A metadata write in one region is visible in every region. Pinned keys survive eviction pressure. Stale-epoch ownership changes are rejected.

---

#### B-25 — KV namespace directory and management

```
Title:  Feature 🚀: Customer KV namespace directory, creation and quotas
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Customer KV stores are namespaces with a home region, quotas and permissions.
Any API node must be able to find a namespace's home writer.

**Describe the solution you'd like**
- A `kv` dataset with the default `on_demand` policy.
- Namespace directory stored in Quicksilver: ID → home region, quotas, permissions, policy overrides.
- Create, delete and list namespaces. Deletion uses range deletion.
- Application bindings reference existing namespace IDs.

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/namespaces`.
- **Doc reference:** `kv.md` §2 Namespace, §12 step 2; `notes.md` (KV dataset).
- **Depends on:** B-24.
- **Acceptance criteria:** A created namespace is resolvable from every region. Quota limits are enforced. A deleted namespace frees its storage.

---

#### B-26 — KV write path

```
Title:  Feature 🚀: Customer KV write path with home-region forwarding
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Writes can arrive at any API node. They must be forwarded to the namespace's home writer, committed durably and replicated to subscriber regions.

**Describe the solution you'd like**
- Operations: `PUT`, `DELETE` (and optionally batch).
- Look up the home writer through the directory, then forward to it.
- Commit through B-09, which assigns the revision and publishes the event.
- Return the committed revision to the client.
- Values are validated against quota and size limits.

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/operations`.
- **Doc reference:** `kv.md` §12.
- **Depends on:** B-25.
- **Acceptance criteria:** A write sent to a non-home region commits in the home region. Active subscriber regions receive it. Regions with no subscription do not.

---

#### B-27 — KV read path through L1/L2

```
Title:  Feature 🚀: Customer KV read path through L1, L2, peers and home region
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Reads must be served locally whenever possible and still meet freshness requirements, falling back through the regional hierarchy to the home region and S3.

**Describe the solution you'd like**
- `GET` and `LIST` (prefix) using the B-22 `SourceChain` with KV tiers:
  - L1: `l1/{ns}/{key}`
  - L2 bucket owner in this cell: `l2/{bucket}/{ns}/{key}`
  - healthy peers in this cell
  - home region
  - S3
- Reads stay inside the cell. There is no sibling-cell tier; it can be added to the `SourceChain` later if miss rates justify it.
- Validate the returned value against revision and tombstone.
- Fill caches according to policy.
- **Missing keys:** cache "not found at revision N" entries, overwritten by later writes, so repeated lookups of a key that doesn't
  exist don't reach L2, the home region or S3. Storing all keys and only some values (Quicksilver v2 style) is the stronger option,
  worth it only if such lookups turn out to be common.
- On first read, establish an `on_demand` subscription at a consistent boundary (B-10).
- Minimum-revision read option. Cells apply revisions independently, so a client moved between cells can see an older revision;
  passing the last revision it saw prevents its reads from going backwards.

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/operations`, `kv/storage` (keyspaces).
- **Doc reference:** `kv.md` §9, §13.
- **Depends on:** B-26.
- **Acceptance criteria:** A cold read in a remote region succeeds and populates L1/L2. A second read is a local hit. Writes after the first read reach the subscribed region. A deleted key never reappears. A repeated read of a missing key is answered locally until the key is written. A minimum-revision read never returns an older revision, even from another cell.

---

#### B-28 — Replication policies and basic eviction

```
Title:  Feature 🚀: Apply global / on_demand / home_only policies and basic L1/L2 eviction
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Namespaces need their configured policy to take effect. Caches also need a bound so a node's disk does not fill up.

**Describe the solution you'd like**
- `global`: propagate to every eligible DC; L2 holds the working set.
- `on_demand`: propagate only to active subscribers.
- `home_only`: forward remote reads to the home region.
- Basic eviction based on storage pressure and access recency, respecting pinned prefixes.
- Unsubscribe idle `on_demand` namespaces.
- `full_replica` is out of scope here (B-40).

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/storage`, `kv/namespaces`.
- **Doc reference:** `kv.md` §3, §16.
- **Depends on:** B-27.
- **Acceptance criteria:** Each policy behaves as documented in an integration test. Disk usage stays within budget under load. Evicted data is still readable through fallback.

---

#### B-29 — KV end-to-end acceptance

```
Title:  Feature 🚀: KV end-to-end acceptance suite on the multi-region deployment
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
"KV works" has to be shown on the real binary and Docker deployment, not only in unit tests.

**Describe the solution you'd like**
- Scenarios on the B-02 compose profile:
  - write in EU, read in US
  - kill an L2 node mid-traffic
  - kill a relay
  - corrupt a KV instance while Quicksilver keeps serving
  - restart a writer
- Run the B-23 invariants against the deployed cluster.
- Basic KV dashboards: hit rates, replication lag, fallback rate.

**Additional context**
- **Implementation:** KV.
- **Doc reference:** `kv.md` §12, §13, §15, §18.
- **Depends on:** B-24 … B-28.
- **Acceptance criteria:** Every scenario passes in CI or in a nightly job.

---

### I — First CDN implementation

---

#### B-30 — Artifact storage: S3 content addressing, `CacheStorage`, Foyer

```
Title:  Feature 🚀: Content-addressed artifact storage with pluggable CacheStorage and Foyer backend
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
CDN artifacts are immutable and identified by SHA-256.
They need authoritative storage in S3 plus a local cache backend that is separate from KV's storage engines.

**Describe the solution you'd like**
- Store and fetch artifacts in S3 by SHA-256, using `lib/s3`.
- A `CacheStorage` trait: put/get (streaming), contains, delete, list inventory, usage. Separate from `StorageEngine`.
- A Foyer adapter as the first backend.
- A local artifact inventory. Checksums are verified on write and on read.
- Corrupted artifacts are reported to the storage-health manager (B-14).

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/artifact_storage`.
- **Doc reference:** `cdn.md` §3 Artifact Storage, §4 Phase 1, Core Principles 2 and 7.
- **Depends on:** B-23.
- **Acceptance criteria:** Upload, then fetch by hash, round-trips. A corrupted local artifact is detected and refetched from S3. The inventory survives a restart.

---

#### B-31 — Artifact transfer

```
Title:  Feature 🚀: Streaming peer-to-peer artifact transfer with S3 fallback and request coalescing
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Artifacts need to move between storage nodes directly, with S3 as the fallback.
This must not swamp relay traffic and must not duplicate fetches when many requests ask for the same hash.

**Describe the solution you'd like**
- Streaming transfers between nodes over the B-03 bulk channel.
- Fetch from S3 when no peer has the artifact.
- Coalesce concurrent requests for the same hash.
- Retries, timeouts and backpressure.
- Checksum verified before an artifact is stored or served.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/artifact_transfer`.
- **Doc reference:** `cdn.md` §3 Artifact Transfer, Core Principle 6.
- **Depends on:** B-30.
- **Acceptance criteria:** 100 concurrent requests for one missing hash cause exactly one upstream fetch. Bulk transfers do not push relay or control latency past its SLO. Truncated transfers are rejected.

---

#### B-32 — `cdn` dataset and retention operations

```
Title:  Feature 🚀: cdn dataset with regional retention namespaces and RETAIN/RELEASE operations
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Which artifacts each region keeps is declared through replicated retention state, not by tracking artifact locations globally.
This reuses the shared replication and sequencing infrastructure (`bedrock-api`, `bedrock-relay`), not KV itself.
The `cdn` dataset belongs to the CDN: `bedrock-kv` never hosts or subscribes to it, just as `bedrock-cdn` never subscribes to a KV dataset.

**Describe the solution you'd like**
- A `cdn` dataset (B-06), with one retention namespace per region. It holds metadata only, never artifact bytes.
- The four retention operations are plain writes through `bedrock-api` (B-09). The write API and relays are generic over datasets
  and need no CDN code:

  | Logical operation | Write |
  |---|---|
  | `RETAIN_MANIFEST` | `SET manifest/{manifest_hash}` |
  | `RELEASE_MANIFEST` | `DELETE manifest/{manifest_hash}` |
  | `RETAIN_ARTIFACT` | `SET artifact/{sha256}` |
  | `RELEASE_ARTIFACT` | `DELETE artifact/{sha256}` |

- A client in `cdn/retention` with typed `retain_manifest(...)` etc. that turns calls into those writes.
- `bedrock-cdn` subscribes to its region's retention namespace through the relays (B-10/B-17). Its apply hook (B-08):
  - expands manifests into artifact retention requirements
  - keeps reference counts shared across manifests
  - stores this derived state in a local `lib/storage` instance
- Derived state can be thrown away: on wipe or corruption it is rebuilt from the `cdn` dataset's snapshot and replay (B-11).
  No artifact locations are stored globally.

**Additional context**
- **Implementation:** CDN. Writes, ordering and fan-out come from the shared infrastructure. Interpretation lives in `bedrock-cdn`.
- **Module:** `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Declarative Retention, §4 Phase 2, Core Principle 5; §1 "Deployment model" in this file.
- **Depends on:** B-23.
- **Acceptance criteria:**
  - Retaining two manifests that share artifacts gives correct refcounts.
  - Releasing one keeps the shared artifacts.
  - Wiping `bedrock-cdn`'s local state rebuilds identical refcounts from the `cdn` dataset's snapshot and replay.
  - `bedrock-kv` receives no `cdn` events, and the `kv` and `cdn` crates have no dependency on each other.

---

#### B-33 — Manifests and publish client

```
Title:  Feature 🚀: Immutable, content-addressed manifests and a publish client
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A set of related artifacts (for example a Pages site with thousands of files) needs one handle: a mapping from paths to artifact hashes.
Publishing must guarantee that a returned handle points only at stored, retained content.
The CDN is a backing service: it knows manifests and artifacts, never deployments, Workers or which version is live.

**Describe the solution you'd like**
- An immutable manifest format (`path → sha256`, plus metadata), stored content-addressed in S3 and addressed by `manifest_hash`.
- A publish client in `cdn/manifests`:
  1. Upload the artifacts.
  2. Upload the manifest.
  3. `RETAIN_MANIFEST` (a write to the `cdn` dataset through `bedrock-api`, see B-32), so caching starts.
  4. Return `manifest_hash` only once steps 1-3 have succeeded.
- **Contract for callers:** a returned `manifest_hash` is stored and retained until the caller sends `RELEASE_MANIFEST`.
  Callers must write their own references to it (for example in Quicksilver) only after publishing, and remove them before
  releasing. The deploy control plane (B-50) is the first such caller.
- Publishing is idempotent: republishing the same content returns the same `manifest_hash` and retains it once.
- `bedrock-cdn` subscribes to the `cdn` dataset only, never to `quicksilver` or any other KV dataset.
- `bedrock-cdn` caches manifests locally by hash. They are immutable, so a cached manifest never needs invalidating.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/manifests`.
- **Doc reference:** `cdn.md` §3 Deployment Integration.
- **Depends on:** B-30, B-32.
- **Acceptance criteria:** The publish client never returns a `manifest_hash` whose artifacts or manifest are missing or not retained.
  A manifest fetched by hash always matches its hash. Killing the client between any two steps and rerunning it produces the
  same end state. `bedrock-cdn` receives no events from any KV dataset.

---

#### B-34 — Regional L2 with two-replica artifact placement

```
Title:  Feature 🚀: Regional L2 artifact placement with two verified replicas per retained artifact
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Retained artifacts must be held by two verified nodes per region, chosen with the shared bucket and placement system.
Local availability must be tracked separately from bucket ownership.

**Describe the solution you'd like**
- Map artifact hash → bucket (B-16). Pick 2 holders with B-15.
- A reconciler (B-20):
  - desired = retained artifacts in my buckets
  - actual = local inventory
  - repair = fetch via B-31
- Publish local artifact availability to the regional directory (B-19), separate from ownership.
- Bucket repopulation hook for B-21.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/retention`, `cdn/artifact_storage`.
- **Doc reference:** `cdn.md` §3 Regional L2, §4 Phase 2, Core Principle 3.
- **Depends on:** B-31, B-32.
- **Acceptance criteria:** After `RETAIN_MANIFEST`, every artifact has two verified holders in each region. Availability lookups return only holders that really have the bytes.

---

#### B-35 — Artifact retrieval interface

```
Title:  Feature 🚀: Hash-addressed CDN retrieval interface (streaming API and HTTP)
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Callers need artifacts from the nearest healthy copy through one retrieval API, addressed only by content hash.

**Describe the solution you'd like**
- `get(manifest_hash, path)` and `get_artifact(hash)`, both streaming. Everything is addressed by hash; the CDN never knows
  what a manifest belongs to. Callers resolve which `manifest_hash` they want themselves.
- Uses the B-22 `SourceChain` with CDN tiers: local cache → L2 holders → peers → S3.
- One HTTP surface (`/manifests/{manifest_hash}/{path}`, `/artifacts/{hash}`) and the same calls as an internal API.
- Correct content type and caching headers for immutable content.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/artifact_transfer` (retrieval), served by the `bedrock-cdn` binary.
- **Doc reference:** `cdn.md` §3 Deployment Integration, §4 Phase 1 ("Basic Worker and Pages artifact retrieval"; the
  Worker- and Pages-specific front ends are B-51).
- **Depends on:** B-33, B-34.
- **Acceptance criteria:** Requests are served from L2 in steady state. Requests still succeed when both L2 holders are down (via S3). The response hash always matches the manifest. An unknown `manifest_hash` returns not found.

---

#### B-36 — CDN end-to-end acceptance

```
Title:  Feature 🚀: CDN end-to-end acceptance suite on the multi-region deployment
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
"CDN works" has to be shown on the real deployment, including failures.

**Describe the solution you'd like**
- Scenarios:
  - publish a manifest, then fetch it and its artifacts by hash from every region
  - kill one L2 holder
  - kill a relay during publish
  - corrupt a cached artifact
  - many concurrent cold requests for the same artifact
  - kill `bedrock-cdn` on a machine: `bedrock-kv` on that machine is unaffected
  - kill `bedrock-kv` on a machine: `bedrock-cdn` there is unaffected, since it depends on no KV process or dataset
- Basic CDN dashboards: hit rates, S3 fallback rate, replica counts, transfer throughput.

**Additional context**
- **Implementation:** CDN.
- **Doc reference:** `cdn.md` §3, §4 Phases 1–2.
- **Depends on:** B-30 … B-35.
- **Acceptance criteria:** Every scenario passes in CI or in a nightly job.

---

### J — KV refinements

---

#### B-37 — Cache-fill broadcast queue

```
Title:  Feature 🚀: Best-effort relay cache-fill broadcast channel
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
When one node resolves a KV miss, other nodes in the region that want the same value fetch it again.
A best-effort broadcast lets interested nodes cache it for free.

**Describe the solution you'd like**
- A second relay channel, separate from replication, that broadcasts `(dataset, ns, key, value, revision)` after a resolved miss.
- Sent only to the owners of the key's bucket and that namespace's L1 subscribers in the same cell, never to every node.
  Broadcasting each miss to all N nodes grows as O(N²) with cell size. Fills never travel up the relay tree.
- Droppable under load.
- Receivers cache it only if interested, and an older fill never overwrites a newer value or tombstone.

**Additional context**
- **Implementation:** KV refinement. The code lives in `lib/relays`, but KV is its only user, so it is deferred.
- **Module:** `lib/relays`, `kv/operations`.
- **Doc reference:** `kv.md` §10 B, §13 step 10, principle 11.
- **Depends on:** B-29.
- **Acceptance criteria:** Fills are dropped under load without any correctness impact. A property test shows stale fills never overwrite newer data.

---

#### B-38 — Full cache lifecycle and eviction

```
Title:  Feature 🚀: HOT/WARM/COLD cache lifecycle, recent-update window and memory layer
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The basic eviction from B-28 does not model the documented lifecycle or the L1 recent-update window, and there is no memory layer.

**Describe the solution you'd like**
- Lifecycle states HOT / WARM / COLD / UNHEALTHY / REBUILDING.
- An optional memory layer above the engine.
- An L1 recent-update window for globally replicated datasets.
- Eviction scoring by frequency, age, pressure, dataset priority, policy and active subscriptions.

**Additional context**
- **Implementation:** KV refinement.
- **Module:** `kv/storage`.
- **Doc reference:** `kv.md` §9 L1, §16.
- **Depends on:** B-29.
- **Acceptance criteria:** Hit rate improves on a benchmark workload compared with B-28. State transitions are observable in metrics.

---

#### B-39 — SurrealKV storage adapter

```
Title:  Feature 🚀: SurrealKV adapter implementing the StorageEngine contract
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The architecture promises interchangeable engines. A second adapter proves the abstraction and gives us a pure-Rust option.

**Describe the solution you'd like**
- `SurrealKvAdapter` that passes the B-04 contract suite and the B-23 fault suite.
- Validate transaction semantics, recovery, corruption reporting and performance against RocksDB.
- Explicit `Unsupported` for any capabilities it lacks.

**Additional context**
- **Implementation:** KV refinement (KV Phase 3). It lives in `lib/storage` next to RocksDB, so `bedrock-cdn` could use it too.
- **Module:** `lib/storage`.
- **Doc reference:** `kv.md` §6, §19 Phase 3.
- **Depends on:** B-29.
- **Acceptance criteria:** All contract and fault tests pass. A benchmark comparison with RocksDB is written up. S3 snapshots restore equivalent state across both engines.

---

#### B-40 — `full_replica` placement and completeness

```
Title:  Feature 🚀: full_replica policy with complete regional coverage and completeness reporting
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Some namespaces must be retained in full in every eligible datacenter. The working-set behaviour of `global` does not guarantee that.

**Describe the solution you'd like**
- `full_replica` placement so that every key of the namespace is held across the L2 nodes in each DC.
- Eviction of covered keys is forbidden.
- Completeness tracking, with degraded status until redundancy and completeness are restored.

**Additional context**
- **Implementation:** KV refinement (KV Phase 6).
- **Module:** `kv/storage`, `lib/buckets`.
- **Doc reference:** `kv.md` §3, §9, §15.5, §16.
- **Depends on:** B-29.
- **Acceptance criteria:** Killing a node reports degraded until it is rebuilt. Coverage reaches 100% again afterwards.

---

#### B-41 — Adaptive cache promotion and regional load balancing

```
Title:  Feature 🚀: Adaptive cache promotion and regional load balancing
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Hot keys and uneven bucket load cause hotspots that static placement cannot fix.

**Describe the solution you'd like**
- Promote hot keys to more nodes, or to L1 everywhere.
- Load-aware source selection in the regional directory.
- Optional engine-specific repair mechanisms (`kv.md` §15.7) can be scoped here if needed.

**Additional context**
- **Implementation:** KV refinement (KV Phase 6).
- **Module:** `kv/operations`, `lib/relays`.
- **Doc reference:** `kv.md` §19 Phase 6.
- **Depends on:** B-38.
- **Acceptance criteria:** A skewed benchmark shows lower p99 latency and more even node load.

---

#### B-42 — Auth dataset

```
Title:  Feature 🚀: Auth dataset for API key hashes, permissions and status
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
`notes.md` lists an Auth dataset (API key hashes, permissions, status) replicated `on_demand`. It is not in the main architecture doc yet.

**Describe the solution you'd like**
- An `auth` dataset using the `on_demand` policy.
- Fail-closed behaviour for revocations: revocation must be enforced authoritatively, not only eventually.

**Additional context**
- **Implementation:** KV refinement (new dataset). Needs scoping before work starts.
- **Module:** `kv/namespaces`.
- **Doc reference:** `notes.md`; `kv.md` §17 Freshness Guarantees ("access revocations require fencing… or fail-closed").
- **Depends on:** B-29.
- **Acceptance criteria:** A revoked key is rejected in every region within the agreed bound, or the check fails closed.

---

### K — CDN refinements

---

#### B-43 — Artifact replica repair and bucket migration

```
Title:  Feature 🚀: Automatic artifact replica repair and bucket migration
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The first CDN keeps two replicas, but repair and rebalancing after failures or membership changes only use the generic paths.
CDN-specific repair is needed: verified copies, peer-first sourcing, throttling.

**Describe the solution you'd like**
- Replica repair: restore 2 verified holders after a holder is lost, sourcing from peers first and S3 second.
- Bucket migration during rebalance, with handoff and no gap in availability.
- Repair throttling that respects transfer backpressure.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 3). **Do this first among the CDN refinements.**
- **Module:** `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Regional L2, §4 Phase 3.
- **Depends on:** B-36.
- **Acceptance criteria:** Killing a holder restores two replicas automatically. Rebalancing never drops an artifact below one available replica.

---

#### B-44 — Coordinated eviction, GC and failure recovery

```
Title:  Feature 🚀: Coordinated artifact eviction, garbage collection and node/relay failure recovery
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Released artifacts need to be removed safely across replicas and from S3.
Retention state must also recover cleanly after node and relay failures, without any per-artifact global metadata.

**Describe the solution you'd like**
- Coordinated eviction when a refcount reaches zero, across both replicas.
- S3 garbage collection with a grace period. GC deletes an S3 object only when:
  - it was released (its refcount reached zero), not merely never retained. Objects uploaded by a publish that has not reached
    `RETAIN_MANIFEST` yet (B-33 steps 1-2) are never candidates, and
  - it is older than the grace period, and no retained manifest references it at deletion time (re-checked, since a new deploy
    can retain the same content hash again).
- Orphans from failed publishes (uploaded, never retained) are swept separately, only after a much longer grace period than any publish takes.
- Recovery of retention state after relay and node loss, using replay and snapshots.
- Reconciliation without per-artifact global location tracking.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 3).
- **Module:** `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Declarative Retention, §4 Phase 3.
- **Depends on:** B-43.
- **Acceptance criteria:** An artifact that is still referenced is never deleted. Released artifacts are reclaimed within the grace window. State converges after a relay failure. An artifact uploaded by an in-progress publish is never deleted before its `RETAIN_MANIFEST` arrives. An artifact released by one manifest and retained by another during GC is kept.

---

#### B-45 — Prewarming and manifest readiness

```
Title:  Feature 🚀: Regional prewarming on retain and per-manifest readiness reporting
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Newly published content should not start cold in every region, and callers need to know when it is warm.

**Describe the solution you'd like**
- Prewarm: when `bedrock-cdn` applies `RETAIN_MANIFEST`, its L2 holders fetch every artifact of the manifest.
- Readiness: each cell reports per `manifest_hash` whether both L2 holders have every artifact, readable through the retrieval
  API (B-35). Callers can wait for all cells, or a threshold with a timeout, before they start sending traffic.
- Waiting is optional: using a manifest early is still correct, since misses fall back to S3, just cold.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 4).
- **Module:** `cdn/manifests`.
- **Doc reference:** `cdn.md` §3 Deployment Integration, §4 Phase 4.
- **Depends on:** B-36.
- **Acceptance criteria:** A manifest reported ready in a cell gets close to 100% L2 hits there on its first requests.
  Readiness turns false when a holder is lost and true again after repair (B-43).

---

#### B-46 — Optional unreplicated L1 and popularity-based retention

```
Title:  Feature 🚀: Optional unreplicated L1 artifact cache and popularity-based retention
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Hot artifacts gain from per-node caching. Retention driven only by manifests ignores real demand.

**Describe the solution you'd like**
- An optional per-node L1 that caches independently, evicts automatically and is never replicated.
- Separate protected L2 and evictable L1 storage areas.
- Popularity signals feed into retention decisions.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 4).
- **Module:** `cdn/artifact_storage`, `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Artifact Storage and Regional L2, §4 Phase 4, Core Principle 4.
- **Depends on:** B-44.
- **Acceptance criteria:** L1 eviction never touches protected L2 data. The hit rate for popular assets improves.

---

#### B-47 — Filesystem `CacheStorage` adapter

```
Title:  Feature 🚀: Filesystem CacheStorage adapter
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Some deployments want plain files instead of Foyer.

**Describe the solution you'd like**
- A filesystem adapter implementing `CacheStorage`: atomic writes, checksum verification, inventory scan on startup.
- Shares the `CacheStorage` contract tests with Foyer.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 4).
- **Module:** `cdn/artifact_storage`.
- **Doc reference:** `cdn.md` §3 Artifact Storage, §4 Phase 4.
- **Depends on:** B-36.
- **Acceptance criteria:** Passes the same `CacheStorage` tests as Foyer. Can be selected by config.

---

#### B-49 — Large-object and range transfers

```
Title:  Feature 🚀: Large-object and HTTP range transfer support
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Large assets and partial (range) requests should not need the whole object transferred or buffered.

**Describe the solution you'd like**
- Range reads through the retrieval interface and the peer/S3 transfers.
- Chunked verification for large objects.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 4).
- **Module:** `cdn/artifact_transfer`.
- **Doc reference:** `cdn.md` §4 Phase 4.
- **Depends on:** B-36.
- **Acceptance criteria:** Range requests return the correct bytes without fetching the full object. Large uploads and downloads stay within the memory budget.

---

### L — Compute and deploy control plane (future)

KV, CDN and (later) Compute are backing services. Everything that knows about Workers, Pages, deployments or revisions lives here,
as a client of those services. Nothing in tracks A-K depends on this track. The crate layout is decided when the track starts.

---

#### B-50 — Deploy control plane: publish, activate, roll back, delete

```
Title:  Feature 🚀: Deploy control plane orchestrating S3, CDN and Quicksilver for Worker and Pages deployments
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A Worker or Pages deployment touches three backing services (S3, the `cdn` dataset, Quicksilver) that cannot be written atomically together.
Something has to own the order, so that every intermediate state is safe and a failed deploy can simply be rerun.

**Describe the solution you'd like**
- Every revision of a Worker or Pages site is its own deployment with its own `deployment_id`.
- Quicksilver schema, owned by this draft (typed accessors included): `deployment/{deployment_id} → {runtime settings, bindings,
  env/secret references, manifest_hash}` and `worker/{worker_id}/active → deployment_id`.
- **Publish and activate**, in this order:
  1. Publish the artifacts and manifest with the CDN publish client (B-33). It returns `manifest_hash` once uploaded and retained.
  2. *(Optional)* wait for manifest readiness (B-45).
  3. Write `deployment/{deployment_id}` to Quicksilver.
  4. Write `worker/{worker_id}/active` to make it live. Can be batched with step 3, or written later.
- **Why this order:** Quicksilver only ever names manifests that exist and are retained, so CDN GC (B-44) cannot delete artifacts
  a deployment points at, and Compute never sees a deployment whose artifacts are not cached yet. A deploy that fails after step 1
  leaves a retained manifest that nothing uses (harmless, cleaned up by a retry or a release).
- **Rollback:** write a previous `deployment_id` to `worker/{worker_id}/active`. Keep the N previous deployments' manifests retained,
  so a rollback is served from L2.
- **Delete** runs in reverse: remove `deployment/{deployment_id}` (it must not be active), then `RELEASE_MANIFEST`.
- Every step is idempotent, keyed by `deployment_id`.

**Additional context**
- **Implementation:** Compute / deploy control plane.
- **Doc reference:** `notes.md` (Worker Execution); `cdn.md` §3 Deployment Integration.
- **Depends on:** B-24, B-33, B-45.
- **Acceptance criteria:** Killing the control plane between any two steps and rerunning the deploy produces the same end state.
  No Quicksilver deployment ever references an unretained manifest. A rollback serves the old version with no S3 fallback.

---

#### B-51 — Worker and Pages request front ends

```
Title:  Feature 🚀: Pages host routing and Worker bundle fetch on top of the hash-addressed CDN
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A Pages request arrives with a host and a path, and a Worker cold start needs its bundle. Both must be turned into CDN hash lookups.

**Describe the solution you'd like**
- Pages front end: host → site → `worker/{id}/active` → `deployment/{id}.manifest_hash` (Quicksilver) → `get(manifest_hash, path)` (B-35).
- Worker bundle fetch: active deployment → `manifest_hash` → `get_artifact` for the bundle.
- Content type, caching headers and not-found handling for deployments.

**Additional context**
- **Implementation:** Compute / deploy control plane.
- **Doc reference:** `notes.md` (Worker Execution); `cdn.md` §4 Phase 1 ("Basic Worker and Pages artifact retrieval").
- **Depends on:** B-35, B-50.
- **Acceptance criteria:** A Pages request resolves to the active deployment's file. Switching `active` changes the served
  version in every region once Quicksilver has propagated.

---

#### B-48 — Compiled Worker artifact caching

```
Title:  Feature 🚀: Cache compiled Worker artifacts
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Recompiling Worker bundles on each cold VM start wastes time against the < 100 ms startup target.

**Describe the solution you'd like**
- Store compiled outputs as ordinary CDN artifacts, keyed by `(bundle hash, runtime version, target)`. The CDN needs no
  Worker-specific code: Compute publishes and fetches them through B-33 and B-35.

**Additional context**
- **Implementation:** Compute (moved from the CDN refinements).
- **Doc reference:** `cdn.md` §4 Phase 4; `notes.md` (Worker Execution).
- **Depends on:** B-51.
- **Acceptance criteria:** A warm compiled-cache hit skips compilation. Changing the runtime version invalidates the entry.

---

#### B-52 — Deploy end-to-end acceptance

```
Title:  Feature 🚀: End-to-end deploy suite across KV, CDN and the control plane
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
"Deploys work" spans three services and has to be shown on the real multi-region deployment, including failures.

**Describe the solution you'd like**
- Scenarios:
  - deploy, activate and serve a Worker and a Pages site from every region
  - roll back and delete a deployment
  - kill the control plane mid-deploy and rerun it
  - kill a relay during a deploy
  - kill `bedrock-kv` on a machine: served deployments keep working from the CDN and other KV nodes

**Additional context**
- **Implementation:** Compute / deploy control plane.
- **Depends on:** B-29, B-36, B-50, B-51.
- **Acceptance criteria:** Every scenario passes in CI or in a nightly job.

---

## 4. Open decisions

### Decided

1. **Separate binaries.** `bedrock-kv` (Quicksilver, KV, Auth), `bedrock-cdn` (`cdn` retention metadata and artifacts),
   `bedrock-relay` (relay tree and membership) and `bedrock-api` (writes), all linking `lib` and never each other.
   KV and CDN run as separate processes on every machine; relays run on a few machines per cell. See §1 "Deployment model".
2. **`StorageEngine` trait and adapters live in `lib/storage`.** Both binaries need a durable local engine, and
   `cdn` must not depend on `kv`. `kv/storage` keeps only KV keyspaces and eviction.
3. **KV and CDN share infrastructure, not datasets.** Retention operations are plain writes to the `cdn` dataset through the
   generic `bedrock-api` and `bedrock-relay`; only `bedrock-cdn` interprets them (B-32). `bedrock-kv` never subscribes to `cdn`,
   and `bedrock-cdn` never subscribes to `quicksilver`, `kv` or `auth`. The CDN serves by `manifest_hash`.
4. **No heartbeat or gossip protocol.** Membership comes from the subscriber streams nodes already hold to their cell relays (B-12):
   relays merge their connected lists and publish them as a replicated namespace, with a grace period on disconnect.
   A member is a node that is receiving updates. Routing decisions use a local per-peer circuit breaker fed by real traffic (B-13).
   There is no separate membership or placement epoch; bucket owners reject requests for buckets they don't own (B-16).
5. **Cells.** A cell (one DC by default) has its own membership, buckets, L2 and relays. Its maximum size is set by the
   bucket count (about 10,000 nodes with 2²⁰ buckets); the actual size is chosen for blast radius. Large sites run several cells. Reads stay inside the cell. Moving traffic between cells is out of scope for now.
6. **Relay tree.** Relays form tiers in static config (region top → optional DC → cell). The downstream dials and the upstream
   pushes; gaps are pulled from the replay log. Fan-out is routed by `(namespace, bucket)`.
7. **KV and CDN are backing services.** Nothing in their tracks knows about Workers, Pages or deployments. That lives in
   track L (Compute and deploy control plane), which uses KV and the CDN as clients and owns the publish/activate/rollback order.
8. **Observability policy.**
   - **Cardinality:** metrics are labelled only by low-cardinality dimensions (dataset, tier, cell, service, node). Anything per
     namespace, key, hash or subscriber goes through an introspection API, or a top-N metric.
   - **End-to-end lag:** every event carries `committed_at`, set once by `bedrock-api` (B-08). Every hop reports `now - committed_at`
     as a histogram, so write-to-anywhere latency needs no tracing infrastructure.
   - **Every draft names its metrics** in its solution or acceptance criteria, on top of the B-03 baseline (logging, metrics endpoint, health).
   - Relays expose per-namespace state through the B-18b introspection API.
9. **`lib/` holds `storage`, `datasets`, `s3` and `transport`** alongside the seven documented modules, since every binary
   needs them. `code-structure.md` reflects this, including `storage` moving out of `kv/` (B-01).
10. **Crate naming.** The shared crate is the package `bedrock-lib` (`use bedrock_lib::…`) in `crates/lib`; the binary
    crates are `bedrock-kv`, `bedrock-cdn`, `bedrock-relay` and `bedrock-api` (B-01).

### Still open

1. **Auth dataset scope (B-42).** It is only mentioned in `notes.md`, so it needs a short design before it becomes an issue.
