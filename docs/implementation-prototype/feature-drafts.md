# Bedrock — Feature Implementation Drafts

Draft GitHub issues for building Bedrock, in the order they should be done. Every draft uses
`.github/ISSUE_TEMPLATE/feature_request.md` (title prefix `Feature 🚀:`, label `enhancement`).
The **Additional context** section of each draft says which implementation it belongs to,
which module it lives in, which doc section it comes from, what it depends on, and when it counts as done.

Sources: `docs/implementation-prototype/kv.md`, `cdn.md`, `code-structure.md`, `notes.md`.

> Status: B-01 to B-23 (including B-12b, B-18b, B-22b and B-23a, but not B-09b or B-21b) are created as GitHub issues (linked below, the issue is the source of truth). Later drafts are still drafts. When a draft is created, replace its body here with a link to the issue.

> Architecture update, 2026-10-29: decision 11 replaces customer-KV origin cells with per-namespace S3 databases,
> an API writer and read-only KV caches. This document records the new decision; the older customer-KV origin text in
> `kv.md` and `code-structure.md` is superseded and still needs synchronization. Section 5 audits all 26 GitHub issues:
> eight open issues now have dated scope-amendment comments, linked there, while their original descriptions are preserved.
> A second amendment below assigns generic writer materialization/recovery to #11 / B-11 before #25 / B-23;
> B-39 is later customer-KV reader/cache integration. Seven additional materialized-state issue amendments are posted and linked in §5.

---

## 1. Strategy

The docs describe two products, KV and CDN, built on one shared distributed layer (`lib/`).
The CDN doc depends on KV phases 1, 2, 4 and 5. Read closely, almost all of those phases are
**shared behaviour** (membership, health, buckets, placement, replication, relays, reconciliation),
not KV-specific behaviour. So the order is:

1. **Foundation.** Workspace, the `bedrock-quicksilver`, `bedrock-kv` and `bedrock-cdn` binaries, roles, transport.
2. **Shared core (`lib/`).** Everything KV *and* CDN need. It has to be built and hardened
   first, with its own test harness, before either product uses it.
3. **First KV** (`quicksilver/` for Quicksilver/Auth, `kv/` for customer KV) and **first CDN** (`cdn/`). Thin product layers on the hardened core.
   The two tracks can run in parallel.
4. **KV refinements** and **CDN refinements**. Product-specific depth: optimisations,
   extra adapters, advanced retention. These can also run in parallel.

**Hardening order:** B-11 provides the generic API-writer atomic persistence and recovery contract before the initial
B-23 gate. That gate covers its real backend, atomic event/state/revision/retry records, durable database recovery, publication
fencing and protected-view GC alongside the retained shared core and minimal reference workload. It does not depend on
B-39. Only after that gate passes does B-39 integrate customer-KV readers/caches on the existing B-11 store, extend the
model/checkers and rerun every applicable test layer and the seven-night history before B-26/customer-KV acceptance.
B-09b is later work with the same protocol-first/revalidation requirement.
Simulation execution follows the latest #26/B-23a discussion: run locally in WSL/Linux/macOS before merging; normal CI
does not imply a simulation pass. Amend #25's older CI wording accordingly while retaining the required seed/replay and
seven-night evidence. This document does not enable a simulation CI job or claim an unrun gate passed.

B-11 adds an asynchronous, SlateDB-based atomic S3 persistence contract in `lib/storage` for the generic API writer across datasets.
B-39 later integrates customer-KV read-only namespace handles and caches on that store; namespace residency and cache policy stay in `kv/storage`.
Quicksilver, Auth, relay replay and CDN retention continue using the local `StorageEngine` and pushed replication.
`full_replica` completeness and CDN phase 3/4 items remain later work. Sequence compression is out of scope.

### Code structure followed

```text
crates/
├── lib/          # bedrock-lib — shared distributed core (library only)
│   ├── membership  health  buckets  placement  replication  relays  reconciliation
│   └── storage  datasets  s3  transport                                              (decided in B-01, see §4)
├── api/          # bedrock/api — write API (B-09)                                         → binary `bedrock-api`
├── relay/        # bedrock/relay — relay process: tiers, fan-out, replay, membership      → binary `bedrock-relay`
├── quicksilver/  # bedrock/quicksilver — quicksilver + auth: storage (hash-range keyspaces), namespaces → binary `bedrock-quicksilver`
├── kv/           # bedrock/kv  — customer KV: S3 namespace readers, cache residency, operations → binary `bedrock-kv`
└── cdn/          # bedrock/cdn — artifact_storage, artifact_transfer, manifests, retention → binary `bedrock-cdn`
```

### Deployment model: separate binaries, one shared library

Each service ships as **its own binary** linking `lib`. Every machine runs `bedrock-quicksilver`, `bedrock-kv` and `bedrock-cdn`, as separate
processes (containers) with their own resource limits, deploys and failure domains. `bedrock-relay` runs on a few machines
per cell (minimum 3, since membership and placement publication need a majority; more as the cell grows) and per upper tier, and `bedrock-api` in the core region. This matches the public Cloudflare model: every
server runs every service, each service is its own process, and code is shared as libraries.

| Binary | Hosts | Roles |
|---|---|---|
| `bedrock-api` | Authoritative writes and revisions; generic per-namespace materialized state across datasets (B-09, B-11) | — |
| `bedrock-relay` | Relay tree, fan-out, replay log, cell membership, L2 placement directory and handoffs (§4 decision 13), regional directory | — (`tier` and `upstreams` in config) |
| `bedrock-quicksilver` | Pushed `quicksilver` and `auth` over relay-assigned hash ranges (§4 decision 13) | `l1`, `l2` |
| `bedrock-kv` | Customer `kv`: S3-backed namespace readers and caches; no hash ranges or buckets (§4 decision 12) | `l1`, `l2` (only logged until B-39) |
| `bedrock-cdn` | Dataset `cdn` (retention metadata) and artifact bytes: storage, transfer, regional L2 placement, serving | `l1`, `l2` (artifact cache roles) |

Rules this implies:

- **Relays are their own binary.** `bedrock-relay` holds no product logic: it is generic over datasets and uses `lib`
  for everything (`lib/relays` for fan-out and tiers, `lib/replication` for streams, `lib/storage` RocksDB for its replay log,
  `lib/membership` for the cell view). A Quicksilver, KV or CDN crash never takes a relay with it, and relays deploy on their own schedule.
- **`quicksilver`, `kv`, `cdn`, `relay` and `api` depend on `lib` only, never on each other.**
- **KV and CDN share infrastructure, not data.** Writes use `bedrock-api` and its sequencing/durability system.
  `bedrock-quicksilver` subscribes to `quicksilver` and `auth`; `bedrock-cdn` subscribes only to `cdn` retention metadata.
  Customer `kv` has no relay subscriptions, replay queues or subscription-driven placement. Its readers use S3-backed
  namespace databases. `bedrock-kv` holds a relay connection only for its own `kv` membership, without another heartbeat,
  and reads Quicksilver/Auth (namespace directory, credentials) from `bedrock-quicksilver` nodes, not from a local copy.
- **Customer KV is isolated from the control plane.** Tenant load (hot namespaces, cold-open storms, slow S3) can exhaust
  `bedrock-kv` without taking down Quicksilver/Auth, which Workers, CDN and deploy tooling depend on (§4 decision 12).
- **Retention operations are plain `SET`/`DELETE` writes** to keys in the `cdn` dataset. The write API and relays are generic,
  and only `bedrock-cdn` interprets them when it applies them (manifest expansion, reference counts).
- **The CDN serves by hash and knows nothing about deployments.** Workers, Pages, deployments, revisions and which version is live
  belong to the Compute and deploy control plane (track L), a client of KV and the CDN (B-50, B-51).
- **Each binary has its own service view.** Membership, health, the placement directory and the regional directory are
  per service, and each binary reads only its own service's views. Relays assign L2 ranges per service (§4 decision 13). A node with a broken CDN cache disk is still a healthy KV node, and the reverse.
- **Shared storage implementations live in `lib/storage`.** Keep the existing synchronous local `StorageEngine` contract
  for pushed datasets. B-11 adds a separate asynchronous namespace persistence/recovery contract; B-39 adds customer-KV readers. Do not block on S3 inside
  the local trait or pretend a read-only handle can write/restore. Dataset identity and permissions remain independent of
  whether a dataset has a relay-fed local engine. API materialization spans datasets; B-39 replaces the KV-reader
  one-local-engine-per-dataset assumption only for customer KV. `kv/storage` owns handle retention, memory/disk budgets and cache eviction;
  `quicksilver/storage` owns the pushed-data L1/L2 hash-range keyspaces; CDN keeps `CacheStorage`.

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
| [B-11](https://github.com/felixrydberg/bedrock/issues/11) | Generic API atomic materialized state, retained events and recovery | Shared core | `lib/storage`, `lib/replication`, `lib/s3`, `api` wiring | B-09, B-07, B-04, B-06, B-23a |
| B-09b | Raft write quorum for the API (later) | Shared core | `lib/replication` + `api` bin | B-09, B-10, B-11, B-13 |
| **D — Shared core: cluster** |||||
| [B-13](https://github.com/felixrydberg/bedrock/issues/12) | Peer reachability from real traffic | Shared core | `lib/health` | B-03 |
| [B-14](https://github.com/felixrydberg/bedrock/issues/13) | Storage-health manager and failure isolation | Shared core | `lib/health` | B-04, B-13 |
| **F — Shared core: relays** (before E: membership comes from relay streams) |||||
| [B-17](https://github.com/felixrydberg/bedrock/issues/14) | `bedrock-relay` binary: tiers, subscriber-initiated streams, bucket-routed fan-out | Shared core | `lib/relays` + `relay` bin | B-10, B-13 |
| [B-12](https://github.com/felixrydberg/bedrock/issues/15) | Membership from relay streams, published as a dataset | Shared core | `lib/membership` + `lib/relays` | B-10, B-17 |
| [B-12b](https://github.com/felixrydberg/bedrock/issues/41) | Warm relay failover: sibling relays share subscriber interests | Shared core | `lib/relays` | B-12, B-17, B-18 |
| [B-18](https://github.com/felixrydberg/bedrock/issues/16) | Relay replay log | Shared core | `lib/relays` | B-17, B-05 |
| [B-18b](https://github.com/felixrydberg/bedrock/issues/17) | Relay introspection and metrics | Shared core | `lib/relays` + `relay` bin | B-17, B-18, B-12 |
| **E — Shared core: regional distribution** |||||
| [B-15](https://github.com/felixrydberg/bedrock/issues/18) | Placement: relay range assignment, replica selection and customer-KV rendezvous | Shared core | `lib/placement` | B-12, B-13 |
| [B-16](https://github.com/felixrydberg/bedrock/issues/19) | Hash ranges, placement directory and owner checks | Shared core | `lib/buckets` + `lib/membership` | B-15 |
| [B-19](https://github.com/felixrydberg/bedrock/issues/20) | Regional directory and source selection | Shared core | `lib/relays` | B-16, B-12 |
| **G — Shared core: reconciliation, recovery, hardening** |||||
| [B-20](https://github.com/felixrydberg/bedrock/issues/21) | Reconciliation framework | Shared core | `lib/reconciliation` | B-16 |
| [B-21](https://github.com/felixrydberg/bedrock/issues/22) | Relay-driven L2 range handoff: assign, bootstrap, ready, revoke | Shared core | `lib/relays` + `lib/buckets` + `lib/reconciliation` | B-19, B-20 |
| B-21b | Drain nodes, cells and namespaces for every dataset | Shared core | `lib/reconciliation` + `lib/placement` | B-15, B-20, B-21 |
| [B-22](https://github.com/felixrydberg/bedrock/issues/23) | Tiered read fallback and cache rebuild | Shared core | `lib/health` + `lib/reconciliation` | B-14, B-19, B-11 |
| [B-22b](https://github.com/felixrydberg/bedrock/issues/24) | Read protection: coalescing and per-cell limits (amendment pending, §5) | Shared core | `lib/reconciliation` + callers | B-22 |
| [B-23](https://github.com/felixrydberg/bedrock/issues/25) | Hardening gate: TLA+ model, deterministic simulation, real-binary crash tests | Shared core | `lib` (test support), `docs/spec/` | B-23a, created B-01 … B-22b; excludes later B-09b/B-21b |
| **H — First KV** |||||
| B-24 | Quicksilver dataset | KV | `quicksilver/namespaces` | B-23 |
| B-25 | KV namespace directory and management | KV | `kv/namespaces` | B-24 |
| B-39 | Customer-KV readers and caches on B-11 materialized storage | KV storage integration | `lib/storage`, `lib/s3`, `kv/storage` | B-23, B-04, B-06, B-07, B-11 |
| B-26 | KV write path and durable namespace publication | KV | `kv/operations`, `api` wiring | B-25, B-39 |
| B-27 | KV reads through cache-affinity routing and S3 namespace readers | KV | `kv/operations`, `kv/storage` | B-26 |
| B-28 | Namespace reader residency, cache budgets and basic eviction | KV | `kv/storage`, `kv/namespaces` | B-27 |
| B-29 | KV end-to-end acceptance | KV | `kv`, `api`, storage integration tests | B-24 … B-28, B-39 |
| **I — First CDN** (parallel with H; no dependency on KV drafts) |||||
| B-30 | Artifact storage: S3 content addressing, `CacheStorage`, Foyer | CDN | `cdn/artifact_storage` | B-23 |
| B-31 | Artifact transfer | CDN | `cdn/artifact_transfer` | B-30 |
| B-32 | `cdn` retention and shared manifest format/reader | CDN | `cdn/retention`, `cdn/manifests` | B-23, B-30 |
| B-33 | Publish client using shared manifests/retention | CDN | `cdn/manifests` | B-30, B-32 |
| B-34 | Regional L2 with two-replica artifact placement | CDN | `cdn/retention`, `cdn/artifact_storage` | B-31, B-32 |
| B-35 | Artifact retrieval interface | CDN | `cdn/artifact_transfer` | B-33, B-34 |
| B-36 | CDN end-to-end acceptance | CDN | `cdn` | B-30 … B-35 |
| **J — KV refinements** |||||
| ~~B-37~~ | ~~Cache-fill broadcast queue~~ (dropped, decision 11) | — | — | — |
| B-38 | Full cache lifecycle and eviction | KV | `kv/storage` | B-29 |
| B-40 | `full_replica` placement and completeness | KV | `quicksilver/storage`, `lib/buckets` | B-24, B-21 |
| B-41 | Adaptive cache promotion and regional load balancing | KV | `kv/operations`, `lib/relays` | B-38 |
| B-42 | Auth dataset | KV | `quicksilver/namespaces` | B-24, B-21b |
| B-53 | Namespace residency fairness and cold-open tuning at scale | KV | `kv/storage`, `kv/namespaces` | B-29 |
| **K — CDN refinements** (parallel with J) |||||
| B-43 | Artifact replica repair and range migration | CDN | `cdn/retention` | B-36 |
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
| Generic materialized namespace engine; later customer-KV readers/caches | B-11; B-39 after B-23 and before B-26 |
| KV Phase 4 (relays, streams, replay) | B-17 … B-19 (cache-fill broadcast dropped, decision 11) |
| KV Phase 5 (cache routing and residency; pushed-data redistribution stays shared) | B-15, B-16, B-19, B-27, B-28 |
| KV Phase 6 (optimisation) | B-38, B-40, B-41, B-53 |
| CDN Phase 1 (artifact storage, transfer, manifests + publish, hash-addressed retrieval) | B-30, B-31, B-33, B-35 (dependency order still applies: B-33 needs B-32's manifest codec, B-35 needs B-34's L2) |
| CDN Phase 2 (global retention streams, regional placement) | B-32, B-34 |
| CDN Phase 3 (manifest-based retention and recovery) | B-32 (manifest refcounts), B-43, B-44 |
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

Object-I/O cleanup, 2026-10-30 (explicit user decision): standardize on the upstream
`object_store::ObjectStore` interface and configured S3 backend. Remove the custom trait,
duplicate AWS/memory clients and identical-body create behavior. Full-object checksum
helpers retain artifact SHA-256 verification and distinguish absent keys from a missing
bucket; native engine reads retain ranged I/O and block integrity. Namespace simulation
continues through its deterministic storage model. Run the same native contract against
InMemory and the configured live S3 provider, including strict create, CAS and range reads.

---

### C — Shared core: replication

---

#### B-08 — Replication event model and ordered applier

Created: [#8](https://github.com/felixrydberg/bedrock/issues/8) — Feature 🚀: Shared replication event format and ordered, duplicate-safe applier

---

#### B-09 — Single-node write API with ordered, SlateDB-durable commits

Created: [#9](https://github.com/felixrydberg/bedrock/issues/9) — Feature 🚀: Single-node write API with ordered, SlateDB-durable commits

---

#### B-09b — Replicated write quorum (Raft) for the write API

```
Title:  Feature 🚀: Raft quorum for the write API with SlateDB persistence and automatic failover
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The dev API (B-09) is a single point of failure and waits on S3 for every acknowledgement.
At scale the write path needs automatic failover, fencing of old leaders and low commit latency.

**Describe the solution you'd like**
- Run `bedrock-api` as a 3 or 5 node Raft group (`openraft`) in the core region. One leader is the only writer, the rest are followers.
- Acknowledge a write once a majority has fsynced it. The state machine assigns per-namespace revisions when entries are applied. The Raft term fences old leaders.
- Reuse B-11's existing `Materialized::persist(entries, Some(raft_position))` operation for batched database
  persistence. B-11 owns its atomic records, remote flush and engine format; do not implement another backend,
  persistence protocol or parallel raw S3 event journal. Retain the majority-committed but not yet SlateDB-durable
  Raft tail for recovery and retry lookup, preserving original retry expiry.
- Keep Raft-committed, applied, SlateDB-durable and reader-visible positions separate. Truncate only after a verified
  recoverable native checkpoint covers every namespace affected by the global prefix, including closed/lazy namespaces.
  Store the cutoff's index, term and membership. A namespace progress record is not a native checkpoint.
- Materialize all datasets through B-11; publish pushed datasets through the existing `Relay.Session`. Customer `kv` uses B-39 readers and is
  never sent through relays. The generic writer selects a storage/delivery contract, not product-specific KV/CDN logic.
- Only one leader publishes each namespace database. Fence stale S3 publishers as well as stale Raft proposals; a term
  change alone must not let an old process publish an older manifest. Integrate existing B-11 fencing with leadership;
  do not create a second storage-publisher implementation.
- Own the buffered/durable API consolidation when this first Raft caller lands: identify which `apply`/`publish`
  entry points are actually needed beside `persist`, migrate their callers and remove superseded wrappers in the
  same change. `apply` buffers; `persist` applies and flushes; `publish` flushes prior buffered work. Preserve the
  adapter's buffered-versus-durable model and crash/flush/acknowledgement tests; test-only use alone is not proof of dead code.
- Start with one group for the whole core region. Assigning namespaces across multiple groups is later work; see
  [future API writer scaling](future-development.md#horizontally-scaled-api-writers) for the proposed direction.
- Metrics: quorum commit latency, failover/recovery duration, persistence lag and retained tail/backpressure,
  with namespace detail in introspection rather than metric labels.

**Additional context**
- **Implementation:** Shared core.
- **Module:** `lib/replication` plus the `api` crate.
- **Doc reference:** `kv.md` §8, §11, §12, §15.9.
- **Depends on:** B-09, B-10, B-11, B-13.
- **Known limits:** writes stop without a majority. Committed entries not yet SlateDB-durable can be lost if a majority of nodes is lost at once; separate failure domains and persist frequently.
- **Acceptance criteria:** Killing the leader loses no acknowledged write, including a write committed before S3 publication.
  A new leader accepts writes within the election timeout plus documented recovery time. An old-term leader cannot commit
  or publish stale storage state. Persistence/replay retries do not double-apply data or skip revisions. Raft truncation cannot
  strand a closed namespace. Update TLA+ before this protocol change, then rerun the applicable B-23 gates and nightly history.

---

#### B-10 — Stream subscriptions with consistent revision boundary

Created: [#10](https://github.com/felixrydberg/bedrock/issues/10) — Feature 🚀: Replication stream subscriptions and catch-up from a consistent revision boundary

The logical B-10 boundary/cursor/minimum-revision contracts now use B-17's sole `Relay.Session` transport.
The former `Source`/`Subscription`/`Replication.Subscribe` path was removed; no compatibility implementation is required.

---

#### B-11 — Generic API atomic materialized state, retained events and recovery

Created: [#11](https://github.com/felixrydberg/bedrock/issues/11) — Feature 🚀: Generic API materialized state, retained events and recovery

Alpha cleanup, 2026-10-30: materialized storage is the sole write/recovery path; no archive compatibility or migration code is retained.

Protocol replacement, 2026-10-30 (explicit user decision): the standalone API writer commits directly to SlateDB.
One atomic engine WriteBatch stores the original retained events, resolved SET/DELETE state and metadata,
namespace revision and original committed_at, retry key/result with fixed expiry, and optional represented Raft
index/term. Await remote durable flush before acknowledging standalone writes. Buffered apply is not durable commit.
Remove the separate raw S3 journal and published/pruned object protocol, with no legacy archive compatibility.
Startup opens and validates durable database state and recent retained events; it never rebuilds current state from a raw journal.
Engine sequence numbers are not Bedrock namespace revisions. Namespace progress, including the event trim boundary,
is stored in the database; it is not a native SlateDB checkpoint.

Pin an Apache-2.0 SlateDB source/format baseline, preserve attribution, and take upstream correctness fixes. Keep atomic
batches, consistent views, native checkpoint/reference protection, compaction and GC. Later reader replacements must
preserve the storage protocol or explicitly migrate it. Route range/conditional object I/O through lib/s3 using the
existing S3Config and official object_store AWS client. Native create and CAS remain strict; request retries use
atomic namespace retry records rather than identical-body object creates. Verify engine block checksums on ranged SST reads; do not fetch a whole SST to
serve a block. Missing referenced objects and ambiguous conditional outcomes are explicit errors. A faithful separate
adapter under cfg(madsim) models buffered versus remote-durable batches.

Event retention and retry protection are independently configurable, both defaulting to 24 hours. Preserve original
events and fixed retry expiry; neither retry nor restart extends expiry. Enforce logical expiry on reads even when
physical rows remain. After successful namespace activity, schedule bounded asynchronous cleanup and meter failures
independently of the completed write. There is no idle namespace polling; physical cleanup may wait for another event
or explicit operation. Current resolved state survives event-history deletion. Retain fenced single-publisher engine
handles for the writer lifetime. SlateDB's own maintenance timers, compaction and protected-view GC remain; removing
all internal engine timers is not claimed by the Bedrock scheduling rule.

Future B-09b is not implemented here. It may acknowledge after majority-fsynced Raft commit before batched SlateDB
persistence, using the same atomic persistence operation. Keep committed not-yet-durable entries for retry lookup and
recovery, with their original expiry. A native SlateDB checkpoint is separate from a progress record. Truncate a Raft
prefix only after a recoverable checkpoint covers every affected namespace, including closed ones, and records the
cutoff index, term and membership. Fence stale storage publishers in addition to stale Raft proposals.

A subscriber whose cursor predates retained events bootstraps a consistent live-key state/revision view then resumes
strictly after its boundary. Validate integrity and revision before readiness. Portable live-key export remains optional
separate work. Keep pushed local StorageEngine and CDN artifact CacheStorage contracts unchanged; B-11 does not depend
on later B-39 readers/caches.

Scope ownership, 2026-10-30: B-11 owns this common persistence/view provider; B-18 owns the existing protected
state-plus-tail transfer and recovery confirmation over `Relay.Session`. B-32 consumes that path for CDN rebuilds,
and B-09b adds quorum/retained-tail/checkpoint integration around the existing persistence operation. Neither track
reimplements the backend, a raw archive, or a product-specific subscription/recovery loop. When an integrating
track supersedes a helper, it owns caller/test migration and deletion in the same change. Unleased `bootstrap`
and `View::install` utilities must not be mistaken for protected production scan-plus-tail recovery.

Acceptance covers normal and cfg(madsim) atomic-batch/flush recovery, acknowledgement crash boundaries, retry after
restart or ambiguous completion, fixed expiry, separate logical retention, namespace isolation, corruption, stale handles,
protected native views, obsolete-cursor bootstrap and asynchronous cleanup failure. Follow B-23a runtime/I/O seams and
update TLA+ before implementation. The bounded standalone model does not establish the full B-23 gate or nightly history.

---

### D — Shared core: cluster

---

#### B-12 — Membership from relay streams, published as a dataset

Created: [#15](https://github.com/felixrydberg/bedrock/issues/15) — Feature 🚀: Cell membership derived from relay subscriber streams and published as a replicated namespace

---

#### B-12b — Warm relay failover: sibling relays share subscriber interests

Created: [#41](https://github.com/felixrydberg/bedrock/issues/41) — Feature 🚀: Warm relay failover by sharing subscriber interests between sibling relays

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

Amended 2026-10-31 by §4 decision 13: L2 placement becomes the relays' range-assignment function. Rendezvous over
`(namespace, key)` remains only for customer-KV cache affinity.

---

#### B-16 — Virtual buckets and ownership

Created: [#19](https://github.com/felixrydberg/bedrock/issues/19) — Feature 🚀: Virtual bucket mapping and per-cell ownership table with owner-side checks

Amended 2026-10-31 by §4 decision 13: 64-bit hash-space ranges replace the 2²⁰ buckets, and the relay-published placement
directory in the membership namespace replaces the per-node ownership table.

---

### F — Shared core: relays

---

#### B-17 — `bedrock-relay` binary: tiers, fan-out, subscriptions

Created: [#14](https://github.com/felixrydberg/bedrock/issues/14) — Feature 🚀: bedrock-relay binary with relay tiers, subscriber-initiated streams, bucket-routed fan-out and subscription tracking

One `Relay.Session` transport serves direct and relayed pushed subscriptions. No preserved B-10 wire/API wrapper,
per-product client or separate heartbeat is required. B-32 integrates richer application behavior at that same boundary.

---

#### B-18 — Relay replay log

Created: [#16](https://github.com/felixrydberg/bedrock/issues/16) — Feature 🚀: Bounded relay replay log with advertised revision ranges

The shared session recovery path reuses B-11 coherent views and bounded tail-protection leases. Future consumers,
including B-32 metadata rebuilds, reuse it rather than adding their own bootstrap transport or recovery engine.

---

#### B-18b — Relay introspection and metrics

Created: [#17](https://github.com/felixrydberg/bedrock/issues/17) — Feature 🚀: Relay introspection API and low-cardinality relay metrics with end-to-end lag

---

#### B-19 — Regional directory and source selection

Created: [#20](https://github.com/felixrydberg/bedrock/issues/20) — Feature 🚀: Regional directory for bucket discovery, replica revisions and locality-aware source selection

Amended 2026-10-31 by §4 decision 13: L2 owners and their readiness come from the placement directory.

---

### G — Shared core: reconciliation, recovery and hardening

---

#### B-20 — Reconciliation framework

Created: [#21](https://github.com/felixrydberg/bedrock/issues/21) — Feature 🚀: Generic desired-vs-actual reconciliation loop with repair scheduling

---

#### B-21 — L2 bucket redistribution on node failure

Created: [#22](https://github.com/felixrydberg/bedrock/issues/22) — Feature 🚀: Automatic L2 bucket reassignment and repopulation after node failure

Amended 2026-10-31 by §4 decision 13: relays carry out range handoffs (push interests, bootstrap, mark ready, revoke), and
nodes confirm a revoke against their directory view before deleting.

---

#### B-21b — Drain nodes, cells and namespaces

```
Title:  Feature 🚀: Generic drain for nodes, cells and single namespaces, with a per-dataset drain hook
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Retiring a node or a cell means stopping new assignments, releasing local work and moving only state that requires another
holder. Customer KV readers own no persistent namespace state: finish reads, close handles and drop cached bytes.
Quicksilver is retained in every cell, while CDN artifacts have their own holders. One drain coordinates these different hooks.

**Describe the solution you'd like**
- **One operation:** `drain(scope, datasets, namespace?)`, through an operator API and CLI.
  - `scope` is a node or a cell. `datasets` is all of them (the default) or a list. `namespace` narrows the drain to one namespace.
  - The drain is a record in Quicksilver (`topology/drains/{id}`: scope, datasets, namespace, state, rate), changed by
    ordinary ordered writes. Deleting the record cancels the drain; it does not introduce a membership/placement epoch.
- **Placement:** relay range assignment (B-15, decision 13) gives draining members nothing new, and customer-KV cache
  affinity skips them. Their existing L2 ranges hand off through B-21; customer-KV cache traffic reroutes without moving an
  authoritative dataset.
- **Execution** runs in each binary's B-20 reconciliation: the desired state excludes the draining target, and the dataset's
  drain hook says what that means for each unit it holds. Moves are rate-limited and limited in concurrency per drain.
- **Per-dataset drain hook:** for a scope and optional namespace, return the moves needed, "nothing to move", or `Unsupported`
  (an explicit error, never a silent no-op):

  | Dataset | Drain a node | Drain a cell | Drain one namespace from a cell |
  |---|---|---|---|
  | `quicksilver` (`full_replica`) | Ranges move to other nodes in the cell (B-21) | Nothing to move: every cell holds it | `Unsupported`: it must be in every cell |
  | `kv` | Stop new reads, finish in-flight reads, close readers and release caches | Apply the same reader/cache drain on its nodes; S3 data stays in place | Close that namespace's readers and release its local cache |
  | `auth` (`on_demand`) | Ranges move to other nodes | Nothing to move: each cell subscribes on its own | `Unsupported` |
  | `cdn` | Artifact holders move within the cell (B-21, B-43) | Nothing to move: each cell keeps its own holders; the cells taking its traffic can prewarm (B-45) | `Unsupported` until the CDN defines one |

- **Progress and completion:** each dataset reports the units left on the target. The drain is complete when every selected
  dataset reports zero; then the node can be stopped, or the cell removed from the region's cell list. Taking client traffic away
  from a drained cell belongs to the front door, outside this issue.
- **Metrics** (labelled by dataset and scope kind only): units left, moves started, completed and failed. Per-drain detail goes
  through introspection.

**Additional context**
- **Implementation:** Shared core. The engine, the drain record and placement eligibility live in `lib`; each dataset's hook lives
  with its dataset: KV in B-28, Quicksilver in B-24, Auth in B-42, CDN in B-43.
- **Module:** `lib/reconciliation`, `lib/placement`.
- **Doc reference:** `kv.md` §9 Cells and Customer KV: Origin Cells and Tiered Caching, §15.5; `cdn.md` §3; `feature-drafts.md` §4 decision 11.
- **Depends on:** B-15, B-20, B-21.
- **Acceptance criteria:**
  - Draining a node under load leaves it holding nothing for any dataset, with no read going backwards.
  - Draining a cell releases its customer-KV readers/caches and reports "nothing to move" across cells for Quicksilver,
    Auth and the CDN; the cell gets no new assignments while it drains. No customer-KV S3 data is copied or deleted.
  - Draining one KV namespace affects only its local handles/caches; unsupported dataset operations return `Unsupported`.
  - An interrupted drain resumes after a restart. Cancelling a drain makes the target eligible again and leaves no partial copies.
  - Moves never exceed the drain's rate and concurrency limits.

---

#### B-22 — Tiered read fallback and cache rebuild

Created: [#23](https://github.com/felixrydberg/bedrock/issues/23) — Feature 🚀: Generic tiered read fallback and validated cache rebuilding

---

#### B-22b — Origin read protection: coalescing and per-cell limits

Created: [#24](https://github.com/felixrydberg/bedrock/issues/24) — Feature 🚀: Request coalescing at every tier and per-cell rate limits on origin reads

---

#### B-23a — Deterministic simulation runtime and I/O rules

Created: [#26](https://github.com/felixrydberg/bedrock/issues/26) — Feature 🚀: Deterministic simulation runtime and I/O rules for the shared core

Done right after B-01, before any I/O code (B-03, B-04, B-07 onwards follow its rules). The ID stays next to B-23 because it exists for B-23.

---

#### B-23 — Shared-core hardening gate: TLA+ model, deterministic simulation and crash testing

Created: [#25](https://github.com/felixrydberg/bedrock/issues/25) — Feature 🚀: Shared-core hardening gate: TLA+ model, deterministic simulation and crash testing

B-23 integrates and extends the existing models, production `run(config)` paths, adapter contracts, workload/fault
scenarios and retained seeds. It does not reimplement B-11 storage or B-17/B-18 subscription/recovery as a second
reference stack. Reuse scenario/checker code across layers while retaining real-engine and real-socket evidence.
The 500-seed PR checks, safety/liveness modes, replay hashes, mutation tests, real-binary faults and seven consecutive
passing nights remain required. Later B-09b/B-32/B-39 work is not an initial prerequisite; applicable integrations
extend these same checks and rerun the required gate/history.

---

### H — First KV implementation

---

#### B-24 — Quicksilver dataset

```
Title:  Feature 🚀: Quicksilver infrastructure metadata dataset
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Infrastructure metadata needs a store that is complete in every datacenter: routing, ownership (D1/DO/KV), system configuration,
and later deployment records written by the deploy control plane (B-50). The KV namespace directory (B-25) reads from it.
Quicksilver is hosted by `bedrock-quicksilver` (decision 12): the CDN never reads it, and customer KV reads it from those nodes.

**Describe the solution you'd like**
- A `quicksilver` dataset with an `infrastructure` namespace and the `full_replica` policy: L2 never evicts it.
  Coverage tracking and rebuild after node loss come with B-40.
- Pinned prefixes (`critical/ownership/`, `critical/routing/`) kept in L1.
- Generic metadata keys and values. Typed schemas for specific consumers (for example Worker deployment config) belong to
  those consumers, not to this dataset.
- Ownership transitions use fencing epochs (B-09).
- Drain hook (B-21b): a node drain moves its ranges; a cell drain has nothing to move; a namespace drain is `Unsupported`.

**Additional context**
- **Implementation:** KV.
- **Module:** `quicksilver/namespaces`.
- **Doc reference:** `kv.md` §1, §4, §11; `notes.md` (Quicksilver).
- **Depends on:** B-23.
- **Acceptance criteria:** A metadata write in one region is visible in every region. No Quicksilver key is evicted from L2, and pinned keys stay in L1, under eviction pressure. Stale-epoch ownership changes are rejected.

---

#### B-25 — KV namespace directory and management

```
Title:  Feature 🚀: Customer KV namespace directory, creation and quotas
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Customer KV stores are namespaces with a home writer, quotas, permissions and their own S3-backed database. A namespace
directory must locate that storage without assigning an origin cell or tracking which nodes happen to cache it.

**Describe the solution you'd like**
- Customer `kv` is a logical dataset using B-11 materialized storage through B-39 readers, not a relay subscription or a local replicated engine instance.
- Namespace directory in Quicksilver: stable ID → home region/writer, canonical S3 storage identity and format version,
  quotas, permissions, cache/read-residency policy and deletion state. Writer fencing is separate from cache placement.
- Create, delete and list namespaces. An empty namespace needs only its directory entry; create engine objects on first write.
  No `origin_cell`, global active/dormant state, cached-key inventory or per-namespace membership heartbeat.
- Deletion first prevents new authorized opens through the directory, then retires the namespace through B-39's safe
  lifecycle. Existing reads/checkpoints must be accounted for before deleting S3 objects. Never confuse namespace deletion
  with evicting a local reader or its cached bytes.
- Application bindings reference existing namespace IDs.
- Metrics: namespace operations by outcome; quota rejection counts. Namespace IDs/details use introspection, not labels.

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/namespaces`.
- **Doc reference:** §4 decision 11; `kv.md` §2 Namespace; `notes.md` (KV dataset). Older origin-cell text is superseded.
- **Depends on:** B-24.
- **Acceptance criteria:** Every region resolves the same namespace/storage identity and home writer. Empty namespaces
  create no engine objects. Adding a cell changes no storage identity. Quotas and deletion checks are enforced even when a
  reader/cache entry is warm. Deleting one namespace cannot delete a sibling namespace's objects.

---

#### B-39 — Customer-KV readers and caches on B-11 materialized storage

```
Title:  Feature 🚀: Customer-KV namespace readers and caches on the shared materialized store
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Customer KV needs read-only access to B-11's durable namespace state with bounded local working sets. Its readers must
preserve coherent views and freshness while fetching blocks on demand. The generic API-writer engine and its recovery
protocol already belong to B-11; customer-KV reader integration follows the full B-23 gate.

**Describe the solution you'd like**
- Reuse B-11's pinned SlateDB source/format baseline, attribution, asynchronous storage contract and checkpoint/recovery
  protocol. Add read-only get/metadata-aware reads, prefix scan, open/close, health and position reporting as needed.
  Keep consistent views, atomic batches, checkpoint protection, compaction and GC. Later replacement readers must preserve
  SST/manifest/recovery compatibility or explicitly migrate; matching method names alone is insufficient.
  Reuse structured error semantics: absent key is `Ok(None)`; corruption/unavailability/unsupported operations are errors.
  Preserve the local `StorageEngine`, its existing adapters and their contract suite for pushed datasets.
- One engine database per customer namespace under a canonical namespace-derived S3 prefix. No per-node authoritative
  shard, database per virtual bucket or segment per namespace inside one global database. Readers load only databases
  requested on that node. Shared caches have process-wide budgets and namespace isolation; no cache allocation per dormant
  namespace. A single large namespace remains one writer/database; splitting it is future work, not claimed linear scaling.
- Consume B-11's generic materialized namespace state without changing its commit authority. Preserve namespace revisions,
  deletes, metadata, expiration and `committed_at`; engine sequence numbers are not Bedrock namespace revisions.
- Respect separate committed, applied, S3-published and reader-visible positions. Use a coherent recent-state read/wait
  path for minimum-revision requests while S3 readers lag. B-11 owns validated durable-database recovery, retry expiry
  and stale-publisher fencing; future Raft additionally retains its committed not-yet-durable tail. Native checkpoints are
  distinct from namespace progress; portable export is optional separate work.
- Reuse B-11's `lib/s3` range-I/O and conditional publication adapter, extending only for demonstrated reader needs. Do not implement a block read by
  fetching the whole namespace/SST. Specify range integrity explicitly: preserve full-object SHA-256 checks for existing
  archives/exports and verify the engine's checksummed blocks for ranged SST reads. Account for codecs, ETags/conditional
  operations, retry outcome ambiguity, missing referenced objects and atomic manifest visibility.
- Keep compaction, checkpoint/reference protection and GC. Reuse B-11's bounded event-triggered namespace flows;
  no idle namespace polling. Logical expiry is enforced even when physical cleanup waits for a later event/manual operation.
  Keep S3 data retention distinct from reader/cache residency. Changing/removing a segment or expiring S3 objects is not
  cache eviction. Reader WAL replay, if enabled, consumes the native engine WAL, never a separate Bedrock journal or relay subscription.
- All time, tasks, RNG, network/S3 and filesystem work follows B-23a. Either port the actual engine to those seams or
  compile it out in `cfg(madsim)` and validate a faithful buffered/committed storage model plus real-engine crash tests.
  Upstream deterministic tests do not by themselves establish Bedrock simulation compatibility. Update TLA+ before the
  publication/recovery protocol and run the applicable B-23 checks/nightly history before dependent product acceptance.
- Metrics: commit-to-publication lag, reader lag, engine opens/closes, range bytes, cache bytes, compaction/GC outcomes
  and backlog, using low-cardinality labels. Expose namespace positions and resource usage through introspection/top-N.

**Additional context**
- **Implementation:** Customer-KV reader/cache integration after the full shared-core gate; this reuses the uncreated
  B-39 ID and replaces its former engine-creation scope. B-11 is its prerequisite, never its dependent. CDN remains independent.
- **Module:** `lib/storage`, `lib/s3`, `kv/storage` and KV reader wiring; generic writer/recovery remains B-11.
- **Doc reference:** §1 storage boundary and §4 decision 11; [SlateDB](https://github.com/slatedb/slatedb),
  [custom WAL boundary](https://docs.rs/slatedb/0.17.0/slatedb/wal/index.html).
- **Depends on:** B-23, B-04, B-06, B-07, B-11. B-09b is a later integration, not a prerequisite for S3-durable operation.
- **Known limits:** many small namespaces create many manifests/SSTs; open handles and maintenance must be bounded.
  Cold opens may require several object requests. The Frankfurt-to-R2-EU 100–300 ms cold-read goal is a benchmark target,
  not a latency guarantee. No petabyte capacity or automatic writer scaling is established.
- **Acceptance criteria:** normal and simulated reader contract/fault checks cover coherent views, minimum revisions,
  publication lag, corruption, safe GC with live readers, pressure/idle closure and reopen. The readers reopen B-11 format
  fixtures and preserve its crash/recovery guarantees. A cold point read fetches bounded metadata/blocks rather than a full namespace. Unrelated namespaces and local
  Quicksilver/Auth stores remain usable after one namespace fails. The current local engines still pass their own contracts.

---

#### B-26 — KV write path

```
Title:  Feature 🚀: Customer KV write path with home-region forwarding
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Writes can arrive in any region. They must reach the home writer, commit durably and become visible through the namespace's
S3 database without customer-KV relay delivery.

**Describe the solution you'd like**
- Operations: `PUT`, `DELETE` and bulk/batch operations; preserve value metadata and TTL/expiration.
- Look up the home writer through the directory, then forward to it.
- Commit through B-11 atomic persistence and remote flush (or majority-fsynced B-09b when implemented); B-39 integrates KV readers. The write API remains generic over
  datasets and opaque payloads; it never imports the `kv` binary. No customer `kv` event is published to a relay stream.
- Return the committed namespace revision. S3 publication and reader visibility can lag a future Raft acknowledgement;
  expose the recent-state read/wait path rather than weakening minimum-revision reads.
- Values are validated against quota and size limits.
- Bound the active writer pool and persistence work; open its engine before standalone persistence. Future Raft may
  commit before namespace persistence, but must bound and backpressure its committed not-yet-durable tail.
- Metrics: commit latency, publication lag, forwarding, rejected/busy writes and pending materialization bytes.

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/operations`, generic storage/commit interfaces in `lib`, `api` wiring.
- **Doc reference:** §4 decision 11; `kv.md` §12's former origin delivery is superseded.
- **Depends on:** B-25, B-39.
- **Acceptance criteria:** Cross-region writes commit in the home region and survive the configured failure scenarios.
  They produce no customer-KV relay subscription/event. A minimum-revision read sees a write even while its S3 publication
  is delayed. Deletes, expiration, metadata, retries and batched writes preserve ordering and namespace isolation.

---

#### B-27 — KV reads through cache-affinity routing and S3 namespace readers

```
Title:  Feature 🚀: Customer KV get and list through bounded caches and S3 namespace readers
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Reads should reuse cached blocks and open namespace readers without loading a whole namespace or subscribing to its writes.
Every eligible KV reader can reach the same S3 state; hashing chooses cache affinity, not ownership of authoritative data.

**Describe the solution you'd like**
- `GET`, metadata-aware get and bulk reads: optional local L1 → preferred healthy KV cache reader in the cell → that reader's
  B-39 namespace view and block cache → S3 range reads. Coalesce identical misses and namespace opens. No mandatory regional
  cache tier, origin cell or cold namespace restore. B-22's generic freshness/fallback helpers remain reusable after §5 amendments.
- Reuse B-15/B-16 hashing over `(namespace, original key)` for cache affinity if useful. A routing change only changes the
  preferred cache: another eligible reader may serve, and no bucket copy, replica-completeness check or relay handoff is needed.
  Keep original keys in the namespace database; hashes must not replace the index needed for prefix listing.
- `LIST` scans the namespace database's complete ordered view, never the incomplete entry/block caches. One reader can
  execute a list without scattering to cache holders. Use an opaque cursor bound to namespace, prefix and last returned
  original key; it must not encode a physical node. Default pages do not promise a snapshot spanning multiple requests.
- Keep key revision, namespace view revision, observation/freshness information and expiration distinct. A key last changed
  at revision 5 may be valid in a namespace view at revision 100. Enforce minimum revision against the view that proves the
  result, including an absence, not merely against the key's last mutation. Never let an older fill replace a newer verified
  result; a stale reader must not reset cache freshness indefinitely by re-serving old state.
- Preserve configurable entry `cache_ttl` (default 60 seconds, with the agreed floor). Immutable SST blocks are retained by
  cache budgets; a block's age is not proof that the namespace view is current. Publication/reader-refresh limits are separate.
- Refresh/pin namespace views through B-11's manifest/checkpoint protocol integrated by B-39. For data newer than the S3 view, use a coherent
  committed recent-state read path or wait for publication; this includes updates, deletes and listing, not just misses.
  No customer-KV relay subscription, Raft feed to every reader, or cache-fill broadcast is created.
- Cache not-found results at a proven namespace view revision with a bounded TTL. Enforce key expiration at read time
  using the recorded expiration, rather than waiting for compaction. Authorization/deletion checks precede serving caches.
- Metrics: cache hit/miss and negative-hit rates, source-read latency/bytes, coalescing, reader lag, freshness waits,
  errors and reopen latency. Namespace/key/cursor details stay in introspection/top-N.

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/operations`, `kv/storage`, B-39's shared asynchronous read contract.
- **Doc reference:** §4 decision 11; the origin/tier portions of `kv.md` §9 and §13 are superseded.
- **Depends on:** B-26.
- **Acceptance criteria:** A cold cross-region read opens only its namespace and fetches relevant blocks; a repeat hits
  cache. Killing the preferred cache reader succeeds through another reader without authoritative data movement.
  Minimum-revision get/list/not-found checks work across delayed S3 publication, reopen and node changes. Deletes cannot
  be resurrected by an old fill; expired data is not served. Pagination survives cache eviction and routing changes.
  Concurrent identical misses/opens cause one operation per receiving coalescer, and freshness bounds include publication
  and reader-refresh lag rather than claiming cache TTL alone bounds all staleness.

---

#### B-28 — Namespace reader residency, cache budgets and basic eviction

```
Title:  Feature 🚀: Per-node namespace reader retention and bounded customer-KV caching
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Open readers retain namespace metadata and cached blocks consume memory/disk. Bound both independently, including in the
home region, so many dormant applications cost only directory metadata and S3 objects instead of permanent local state.

**Describe the solution you'd like**
- Customer-KV cache/residency policy is independent of `ReplicationPolicy`. There are no customer-KV `global`,
  `full_replica` or `on_demand` push overrides. Shared policies for Quicksilver/Auth/CDN stay intact. Any home-region-only
  access restriction, if exposed, is a read-routing policy and must not recreate an origin cell.
- Hard node/process-wide limits for cached disk bytes, RAM (blocks plus reader/manifest overhead), open namespace readers
  and concurrent opens/fetches. Use soft per-namespace cached-byte shares with borrowing of unused capacity and explicit
  optional caps. These limit local residency, not a namespace's total persistent size; total-size quotas belong to B-25.
- Reclaim cold blocks under disk pressure. Close least-recently-used idle readers under metadata/handle pressure, with a
  configurable idle timeout even below the pressure threshold. Reader and block lifetimes are separate: closing a reader
  need not discard useful blocks, and evicting blocks need not close a reader. Quicksilver/Auth local stores have separate budgets.
- Finish in-flight operations before closing a reader and release its checkpoint/reference protection safely. Coalesce
  concurrent reopens. Failure to open one namespace is isolated; process readiness does not require opening every namespace.
- All KV nodes, including home-region nodes, make these decisions locally. No global dormant/rehydrating directory state,
  eviction announcement, customer-KV subscription, whole-database restore or authoritative data migration. Persistent TTL
  and S3 GC must never be triggered merely because a cache/reader became idle.
- B-21b drain hook stops new cache routing, lets reads finish, closes the selected readers and releases local cache state.
  It never deletes the S3 namespace database. API writer-handle retention is separate and must preserve the committed tail.
- Metrics: open readers, opens/evictions by reason, coalesced reopens, disk/RAM usage against budgets and cache churn.
  Per-namespace shares, last access and handle state use introspection/top-N, not namespace metric labels.

**Additional context**
- **Implementation:** KV.
- **Module:** `kv/storage`, `kv/namespaces`.
- **Doc reference:** §4 decision 11; supersedes customer-KV origin/residency behavior in `kv.md` §3, §9 and §16.
- **Depends on:** B-27.
- **Acceptance criteria:** More namespaces than the open-reader limit do not produce unbounded handles, background tasks
  or memory. Disk and RAM stay within documented budgets, accounting for bounded in-flight work. One busy namespace cannot
  starve others. Evicting one node's copy leaves other readers and S3 unchanged. A closed namespace reopens without a full
  restore or lost minimum-revision guarantee. Draining readers does not lose data or interrupt in-flight operations.

---

#### B-29 — KV end-to-end acceptance

```
Title:  Feature 🚀: KV end-to-end acceptance suite on the multi-region deployment
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
"KV works" has to be shown on the real binary and Docker deployment, not only in unit tests.

**Describe the solution you'd like**
- Scenarios on the B-02 compose profile: EU write/US read; hot and cold namespace reads; reader loss and cache-affinity
  changes under load; namespace eviction/reopen; bounded handles across many inactive namespaces; delayed/failing S3;
  writer crash before/after checkpoint publication; and one namespace's corruption while Quicksilver and other namespaces serve.
- Kill a relay: Quicksilver/Auth recovery still works, while already authorized customer-KV S3 reads do not depend on a
  customer-KV relay stream. Authorization/control-plane outages follow their own fail-closed policy, not a cache bypass.
- Exercise get/metadata, bulk operations, TTL/expiration, deletes, prefix list and pagination across node/revision changes.
  Apply the B-23 safety checks to the new storage protocol as well as the retained pushed-data paths. When B-09b lands,
  add leader changes and minimum-revision reads during committed-but-unpublished writes before claiming Raft durability.
- Dashboards: cache hit rates, namespace open/close and resource budgets, publication/reader lag, S3 bytes/requests,
  freshness waits and errors. Record cold-open/key-read p50/p95/p99, sustained writes, compaction throughput and object growth;
  report Frankfurt-to-R2-EU results against the 100–300 ms goal without claiming an unmeasured SLA.

**Additional context**
- **Implementation:** KV.
- **Doc reference:** §4 decision 11 and B-39's storage contract; legacy origin scenarios are superseded.
- **Depends on:** B-24 … B-28, B-39.
- **Acceptance criteria:** Every applicable scenario passes in CI or a nightly job. Adding a node/cell needs no customer-KV
  data migration. No minimum-revision read goes backwards; no uncommitted, unverified or expired result is served as fresh.
  No customer-KV relay subscription is created, and no test requires a complete namespace on a KV node before serving.

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
- Coalesce concurrent requests for the same hash on each node, including local prewarming and incoming peer requests.
  A peer request can start a missing artifact's fetch or join an existing fetch, even before local prewarming starts.
- Retries, timeouts and backpressure.
- Checksum verified before an artifact is stored or served.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/artifact_transfer`.
- **Doc reference:** `cdn.md` §3 Artifact Transfer, Core Principle 6.
- **Depends on:** B-30.
- **Acceptance criteria:** 100 concurrent requests for one missing hash on one node, mixing local prewarming and peer requests,
  cause exactly one upstream fetch. A peer request arriving before local prewarming starts also shares that fetch.
  Bulk transfers do not push relay or control latency past its SLO. Truncated transfers are rejected.

---

#### B-32 — `cdn` retention and shared manifest format/reader

```
Title:  Feature 🚀: CDN retention operations with shared manifest format and verified reads
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Which artifacts the CDN keeps is declared through replicated retention state, not by tracking artifact locations globally.
This reuses the shared replication and sequencing infrastructure (`bedrock-api`, `bedrock-relay`), not KV itself.
The `cdn` dataset belongs to the CDN: `bedrock-quicksilver` and `bedrock-kv` never host or subscribe to it, just as `bedrock-cdn` never subscribes to a KV dataset.

**Describe the solution you'd like**
- A `cdn` dataset (B-06) with global retention namespaces, replicated to every region. The deploy control plane (B-50) uses
  `workers` and `pages`; the CDN treats namespace names as opaque. It holds metadata only, never artifact bytes, and its
  records are never evicted.
- The four retention operations are plain writes through `bedrock-api` (B-09). The write API and relays are generic over datasets
  and need no CDN code:

  | Logical operation | Write |
  |---|---|
  | `RETAIN_MANIFEST` | `SET manifest/{manifest_hash}` |
  | `RELEASE_MANIFEST` | `DELETE manifest/{manifest_hash}` |
  | `RETAIN_ARTIFACT` | `SET artifact/{sha256}` |
  | `RELEASE_ARTIFACT` | `DELETE artifact/{sha256}` |

- A client in `cdn/retention` with typed `retain_manifest(...)` etc. that turns calls into those writes.
- Implement the canonical immutable manifest format (`path → sha256`, plus metadata), encoding/decoding and
  hash-verified reader in `cdn/manifests` here: retention is its first consumer. Reuse B-30's content-addressed
  storage/cache for manifest bytes. B-33 consumes this implementation; do not create a provisional retention codec
  that B-33 later replaces, or make B-32 depend on B-33.
- `bedrock-cdn` uses the existing B-17 `Relay.Session` client for every retention namespace. Integrate richer
  application behavior once at its existing data/covered-progress boundary, reusing or consolidating B-08 hook
  semantics instead of retaining a parallel applier/subscriber. CDN application:
  - expands manifests into artifact retention requirements
  - keeps reference counts shared across manifests
  - stores this derived state in a local `lib/storage` instance
- Derived state can be thrown away: rebuild using B-18's existing session recovery over B-11's coherent state
  and lease-protected tail, then the same CDN application behavior. Reuse the transfer, checksum validation,
  exact-boundary resumption and recovery confirmation; no second bootstrap protocol or mandatory portable archive.
  State replacement/derived writes and represented covered progress must remain atomic and unready until validated.
  No artifact locations are stored globally.
- Remove superseded application/rebuild wrappers and migrate their useful tests in this integration. Keep shared
  log/view/storage primitives and actual inspection/local-install utilities with identified uses. Do not prebuild
  this product hook framework in earlier core issues merely to replace it here.

**Additional context**
- **Implementation:** CDN. Writes, ordering and fan-out come from the shared infrastructure. Interpretation lives in `bedrock-cdn`.
- **Module:** `cdn/retention`, shared codec/verified reader in `cdn/manifests`.
- **Doc reference:** `cdn.md` §3 Declarative Retention, §4 Phase 2, Core Principle 5; §1 "Deployment model" in this file.
- **Depends on:** B-23 and B-30; reuse created B-08/B-11/B-17/B-18 contracts.
- **Acceptance criteria:**
  - Manifest encoding/decoding and verified reads use one canonical implementation shared with B-33. A bad hash or missing manifest is an explicit error.
  - Retaining two manifests that share artifacts gives correct refcounts.
  - Releasing one keeps the shared artifacts.
  - Live delivery, duplicate/gap handling, restart and obsolete-cursor bootstrap run through the shared session; derived state and covered cursor cannot diverge on a crash.
  - Wiping `bedrock-cdn`'s local state rebuilds identical refcounts through protected recovery under concurrent writes/GC; corruption, interruption and lease expiry cannot establish readiness.
  - `bedrock-quicksilver` and `bedrock-kv` receive no `cdn` events, and the `quicksilver`, `kv` and `cdn` crates have no dependency on each other.

---

#### B-33 — Publish client using shared manifests and retention

```
Title:  Feature 🚀: Idempotent publish client using the shared manifest and retention contracts
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A set of related artifacts (for example a Pages site with thousands of files) needs one handle: a mapping from paths to artifact hashes.
Publishing must guarantee that a returned handle points only at stored, retained content.
The CDN is a backing service: it knows manifests and artifacts, never deployments, Workers or which version is live.

**Describe the solution you'd like**
- Reuse B-32's canonical manifest format, codec and hash-verified reader, and B-30's content-addressed storage/cache.
  This draft owns publish orchestration, not another manifest implementation or cache backend.
- A publish client in `cdn/manifests`:
  1. Upload the artifacts.
  2. Upload the manifest.
  3. `RETAIN_MANIFEST` in the caller's retention namespace (a write to the `cdn` dataset through `bedrock-api`, see B-32),
     so caching starts in every region.
  4. Return `manifest_hash` only once steps 1-3 have succeeded.
- **Contract for callers:** a returned `manifest_hash` is stored and retained until the caller sends `RELEASE_MANIFEST`.
  Callers must write their own references to it (for example in Quicksilver) only after publishing, and remove them before
  releasing. The deploy control plane (B-50) is the first such caller.
- Publishing is idempotent: republishing the same content returns the same `manifest_hash` and retains it once.
- `bedrock-cdn` subscribes to the `cdn` dataset only, never to `quicksilver` or any other KV dataset.
- Reuse the shared verified manifest cache by hash. Manifests are immutable, so a cached manifest never needs invalidating.
- Compose B-30 storage and B-32 retention calls directly; no temporary publisher/manifest facade or second subscription/recovery loop.

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
Retained artifacts must be held by two verified nodes per cell, chosen with the shared range placement (decision 13).
Local availability must be tracked separately from range ownership.
Applying `RETAIN_MANIFEST` must not make both cold holders independently download the same artifact from S3.

**Describe the solution you'd like**
- The artifact hash falls in one hash range (B-16); that range's two owners in the placement directory (B-15) are the holders.
  Relays deliver its retention metadata only to them.
- Only the selected holders initiate prewarming for their artifacts when retention is applied.
- Reuse verified local or peer copies first. On a completely cold miss, the first holder fetches from S3.
  The second requests the artifact from the first through B-31, starting or joining its coalesced fetch.
- If the first holder fails or times out, the second falls back to S3 using B-31's retries and timeouts.
  Artifact bytes use the existing bulk channel; relays continue carrying metadata.
- A reconciler (B-20):
  - desired = retained artifacts in my ranges only when read within the idle window (B-44); initial retention prewarming remains separate
  - actual = local inventory
  - repair = fetch via B-31
- Publish local artifact availability to the regional directory (B-19), separate from ownership.
- Range repopulation hook for B-21.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/retention`, `cdn/artifact_storage`.
- **Doc reference:** `cdn.md` §3 Regional L2, §4 Phase 2, Core Principle 3.
- **Depends on:** B-31, B-32.
- **Known limits:** Download suppression is per cell and best-effort. Separate cells fetch independently; failover or
  differing membership views can cause duplicate S3 downloads.
- **Acceptance criteria:**
  - After `RETAIN_MANIFEST`, every artifact has two verified holders in each cell. Availability lookups return only holders that really have the bytes.
  - With stable placement and healthy nodes, concurrent prewarming on both cold holders causes one S3 download and produces two verified copies.
  - The second holder can request the first before it starts prewarming; concurrent requests on the first share one fetch.
  - Failure or timeout of the first holder lets the second complete from S3 without waiting indefinitely.

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
  - kill `bedrock-cdn` on a machine: `bedrock-quicksilver` and `bedrock-kv` on that machine are unaffected
  - kill `bedrock-quicksilver` or `bedrock-kv` on a machine: `bedrock-cdn` there is unaffected, since it depends on no KV process or dataset
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

Dropped (§4 decision 11). Customer KV uses read-driven caches over S3 namespace databases. Cache fills/evictions are local,
and neither a customer-KV relay subscription nor a separate fill-broadcast channel is required.

---

#### B-38 — Full cache lifecycle and eviction

```
Title:  Feature 🚀: Adaptive KV cache admission and lifecycle across memory and disk
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The bounded reader/block residency from B-28 needs workload-driven admission and eviction tuning. Pushed datasets still
need their own recent-update behavior; customer KV receives no push stream.

**Describe the solution you'd like**
- Tune hot/warm/cold admission and pressure behavior using measured cache reuse; keep reader-handle and block lifetimes separate.
- Reuse the selected engine cache rather than introducing a second memory/disk layer for the same blocks without evidence.
- Preserve the L1 recent-update window for pushed datasets only. Customer KV admission follows reads and freshness checks.
- Score by frequency, age, pressure and policy, respecting B-28's hard budgets and namespace fairness.
- Metrics: hit rate, admission/eviction reasons, cache churn and p99 read latency; namespace details remain introspection/top-N.

**Additional context**
- **Implementation:** KV refinement.
- **Module:** `kv/storage`; the pushed-data L1 recent-update window lives in `quicksilver/storage`.
- **Doc reference:** `kv.md` §9 L1, §16.
- **Depends on:** B-29.
- **Acceptance criteria:** Hit rate improves on a benchmark workload compared with B-28. State transitions are observable in metrics.

---

#### B-40 — `full_replica` placement and completeness

```
Title:  Feature 🚀: full_replica policy with complete regional coverage and completeness reporting
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Some namespaces, starting with Quicksilver (B-24), must be retained in full in every eligible datacenter. Disabling L2 eviction
keeps what arrived, but nothing yet guarantees coverage or rebuilds it after a node is lost.

**Describe the solution you'd like**
- `full_replica` placement so that every key of the namespace is held across the L2 nodes in each DC: every hash range in
  the placement directory has a ready owner (decision 13).
- Eviction of covered keys is forbidden.
- Completeness tracking, with degraded status until redundancy and completeness are restored.
- This applies to pushed namespaces such as Quicksilver, not customer-KV cache readers. No customer-KV replica-completeness
  or range-rebuild requirement is reintroduced. Metrics cover retained-data coverage/degradation with low-cardinality labels.

**Additional context**
- **Implementation:** KV refinement (KV Phase 6).
- **Module:** `quicksilver/storage`, `lib/buckets`.
- **Doc reference:** `kv.md` §3, §9, §15.5, §16.
- **Depends on:** B-24, B-21.
- **Acceptance criteria:** Killing a node reports degraded until it is rebuilt. Coverage reaches 100% again afterwards.

---

#### B-41 — Adaptive cache promotion and regional load balancing

```
Title:  Feature 🚀: Adaptive cache promotion and regional load balancing
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Many hot keys can be spread by cache affinity, but a single hot key can still overload its preferred reader.

**Describe the solution you'd like**
- Promote hot customer-KV keys to additional cache readers or L1s using measured load; no durable shard movement or push feed.
- Load-aware selection among eligible readers, with misses coalesced and S3/recent-state requests bounded.
- Measure metadata duplication and cache reuse as readers spread; do not imply read-side scaling increases one writer's capacity.
- Metrics: source load, promotions, cache-hit distribution and p99 latency; hot-key/namespace details use top-N/introspection.

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
- An `auth` dataset using the `on_demand` policy (push on first read). It never uses the `cached` TTL model: a revocation must not wait for a TTL.
- Fail-closed behaviour for revocations: revocation must be enforced authoritatively, not only eventually.
- Drain hook (B-21b): a node drain moves its ranges; a cell drain has nothing to move; a namespace drain is `Unsupported`.

**Additional context**
- **Implementation:** KV refinement (new dataset). Needs scoping before work starts.
- **Module:** `quicksilver/namespaces` (binary `bedrock-quicksilver`, decision 12).
- **Doc reference:** `notes.md`; `kv.md` §17 Freshness Guarantees ("access revocations require fencing… or fail-closed").
- **Depends on:** B-24, B-21b.
- **Acceptance criteria:** A revoked key is rejected in every region within the agreed bound, or the check fails closed.

---

#### B-53 — Namespace residency fairness and cold-open tuning at scale

```
Title:  Feature 🚀: Tune namespace reader residency, fairness and cold-open cost at scale
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Basic namespace residency is already mandatory in B-28. At large namespace counts, cold-open churn, tiny S3 objects and
uneven cache use need measured tuning without introducing a global namespace dormancy protocol.

**Describe the solution you'd like**
- Benchmark many mostly idle namespaces, bursts across more namespaces than fit in the handle pool, one large/hot tenant,
  cache-node replacement and concurrent opens. Vary idle timeout, per-namespace shares and open/fetch concurrency.
- Tune pressure watermarks, borrowing and admission using observed memory/manifest overhead as well as cached block bytes.
  Preserve node hard limits; do not pre-open every namespace, restore full databases or announce evictions to other regions.
- Measure the number/size of namespace manifests/SSTs, request cost, cold-open metadata requests, replay tail and GC backlog.
  Coordinate bounded maintenance with B-39. Namespace grouping/segmentation remains a separate future decision if evidence
  warrants it, not an automatic rewrite of the per-namespace storage contract.
- Report cold namespace-open plus key-read p50/p95/p99 for the actual Frankfurt/R2-EU path against the 100–300 ms target.
  Warm-block, cold-block and cold-handle results must be separate. No global latency or petabyte-capacity guarantee.
- Metrics: reader churn, open/fetch queues, budget pressure, fairness and maintenance backlog with low-cardinality labels.
  Namespace sizes, last access and allocations are introspection/top-N only.

**Additional context**
- **Implementation:** KV refinement, replacing the former origin dormancy/rehydration proposal.
- **Module:** `kv/storage`, `kv/namespaces`, shared engine maintenance from B-11 via B-39.
- **Doc reference:** §4 decision 11; B-28's mandatory residency policy.
- **Depends on:** B-29.
- **Known limits:** storage capacity, one writer's throughput, manifest growth, maintenance and cold-open costs are separate
  limits. Closing local readers saves no S3 storage; persistent retention/deletion is an explicit data-lifecycle decision.
- **Acceptance criteria:**
  - The documented workload stays within bounded memory/disk/handle budgets and avoids starvation between namespaces.
  - Reopening a closed namespace reads relevant metadata/blocks without a full restore or a new customer-KV subscription.
  - Minimum revisions, expiration, delete safety and namespace isolation survive eviction, reopen and node replacement.
  - Concurrent misses share one open; in-flight reads finish safely during eviction and checkpoint release.
  - Publish reproducible latency/resource/object-count results and any limits found; an unmet target is reported as such.

---

### K — CDN refinements

---

#### B-43 — Artifact replica repair and range migration

```
Title:  Feature 🚀: Automatic artifact replica repair and range migration
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The first CDN keeps two replicas, but repair and rebalancing after failures or membership changes only use the generic paths.
CDN-specific repair is needed: verified copies, peer-first sourcing, throttling.

**Describe the solution you'd like**
- Replica repair: restore 2 verified holders after a holder is lost, sourcing from peers first and S3 second.
- Range migration during rebalance through the relay-driven handoff (B-21), with no gap in availability.
- Repair throttling that respects transfer backpressure.
- Drain hook (B-21b): a node drain moves its artifact holders within the cell; a cell drain has nothing to move, and the cells
  taking its traffic can prewarm (B-45).

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
- Idle eviction: both L2 holders evict a retained artifact that has not been read within a configurable idle window, and
  under storage pressure the least recently read first. S3 keeps it, it stays retained, and the next miss fetches it again.
- S3 garbage collection with a grace period. GC deletes an S3 object only when:
  - it was released (its refcount reached zero), not merely never retained. Objects uploaded by a publish that has not reached
    `RETAIN_MANIFEST` yet (B-33 steps 1-2) are never candidates, and
  - it is older than the grace period, and no retained manifest references it at deletion time (re-checked, since a new deploy
    can retain the same content hash again).
- Orphans from failed publishes (uploaded, never retained) are swept separately, only after a much longer grace period than any publish takes.
- Recovery of retention state after relay and node loss, using B-11's consistent materialized-state view and subsequent retained database events, resuming the stream boundary.
- Reconciliation without per-artifact global location tracking.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 3).
- **Module:** `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Declarative Retention, §4 Phase 3.
- **Depends on:** B-43.
- **Acceptance criteria:** An artifact that is still referenced is never deleted from S3. An idle retained artifact leaves L2
  after the idle window and is served from S3 on the next request. Released artifacts are reclaimed within the grace window. State converges after a relay failure. An artifact uploaded by an in-progress publish is never deleted before its `RETAIN_MANIFEST` arrives. An artifact released by one manifest and retained by another during GC is kept.

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
- Separate L2 and L1 storage areas; L1 evicts on its own, independently of L2 idle eviction (B-44).
- Popularity signals feed into retention decisions.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 4).
- **Module:** `cdn/artifact_storage`, `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Artifact Storage and Regional L2, §4 Phase 4, Core Principle 4.
- **Depends on:** B-44.
- **Acceptance criteria:** L1 eviction never touches L2 data. The hit rate for popular assets improves.

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
  1. Publish the artifacts and manifest with the CDN publish client (B-33), into the `workers` or `pages` retention namespace.
     It returns `manifest_hash` once uploaded and retained.
  2. *(Optional)* wait for manifest readiness (B-45).
  3. Write `deployment/{deployment_id}` to Quicksilver.
  4. Write `worker/{worker_id}/active` to make it live. Can be batched with step 3, or written later.
- **Why this order:** Quicksilver only ever names manifests that exist and are retained, so CDN GC (B-44) cannot delete artifacts
  a deployment points at. A deploy that fails after step 1
  leaves a retained manifest that nothing uses (harmless, cleaned up by a retry or a release).
- **Rollback:** write a previous `deployment_id` to `worker/{worker_id}/active`. Keep the N previous deployments' manifests retained.
  Only requested artifacts that remain warm within the idle window (B-44) are guaranteed to be served from L2; cold artifacts may
  fetch from S3 unless the full manifest was prewarmed (B-45). Reading the deployment record does not warm its full manifest.
- **Delete** runs in reverse: remove `deployment/{deployment_id}` (it must not be active), then `RELEASE_MANIFEST`.
- Every step is idempotent, keyed by `deployment_id`.

**Additional context**
- **Implementation:** Compute / deploy control plane.
- **Doc reference:** `notes.md` (Worker Execution); `cdn.md` §3 Deployment Integration.
- **Depends on:** B-24, B-33, B-45.
- **Acceptance criteria:** Killing the control plane between any two steps and rerunning the deploy produces the same end state.
  No Quicksilver deployment ever references an unretained manifest. Rollback requests for artifacts that remain warm within the
  idle window need no S3 fallback; cold artifacts may require S3 unless the full manifest is prewarmed.

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
  - kill `bedrock-quicksilver` on a machine: served deployments keep working from the CDN and other Quicksilver nodes

**Additional context**
- **Implementation:** Compute / deploy control plane.
- **Depends on:** B-29, B-36, B-50, B-51.
- **Acceptance criteria:** Every scenario passes in CI or in a nightly job.

---

## 4. Open decisions

### Decided

1. **Separate binaries.** `bedrock-quicksilver` (Quicksilver, Auth), `bedrock-kv` (customer KV), `bedrock-cdn` (`cdn` retention
   metadata and artifacts), `bedrock-relay` (relay tree and membership) and `bedrock-api` (writes), all linking `lib` and never
   each other. Quicksilver, KV and CDN run as separate processes on every machine; relays run on a few machines per cell.
   See §1 "Deployment model" and decision 12.
2. **Storage contracts and adapters live in `lib/storage`.** The synchronous local `StorageEngine` remains for pushed
   datasets, relay replay and CDN metadata. B-11 adds asynchronous S3 materialized storage for generic API writers across
   datasets; B-39 later integrates KV readers. `kv/storage` owns reader retention/cache policy; `quicksilver/storage` owns
   the pushed L1/L2 keyspaces; CDN retains `CacheStorage`.
3. **KV and CDN share infrastructure, not datasets.** Retention operations are plain writes to the `cdn` dataset through the
   generic `bedrock-api` and `bedrock-relay`; only `bedrock-cdn` interprets them (B-32). `bedrock-quicksilver` and `bedrock-kv` never
   subscribe to `cdn`, and `bedrock-cdn` never subscribes to `quicksilver`, `kv` or `auth`. Customer `kv` has no relay delivery at all;
   Quicksilver carries its directory metadata. The CDN serves by `manifest_hash`. Shared writers/storage remain generic
   and never import another binary's product code.
4. **No heartbeat or gossip protocol.** Membership comes from the subscriber streams nodes already hold to their cell relays (B-12):
   relays merge their connected lists and publish them as a replicated namespace, with a grace period on disconnect.
   A member is a node that is receiving updates. Routing decisions use a local per-peer circuit breaker fed by real traffic (B-13).
   There is no separate membership or placement epoch; owners reject requests for L2 ranges they don't own (B-16, decision 13).
   Customer-KV hash routing only selects a preferred cache; another eligible reader can serve without owning persistent data.
   It hashes `(namespace, key)` directly and uses no ranges or buckets (decisions 12, 13).
5. **Cells.** A cell (one DC by default) has its own membership, placement directory, L2 and relays. No fixed bucket count
   caps its size (decision 13); the actual size is chosen for blast radius. Large sites run several cells.
   Customer-KV misses read shared S3 namespace state; minimum-revision reads can use the home writer's recent-state path.
   There is no mandatory regional KV tier or origin cell. Moving client traffic between cells is out of scope for now.
6. **Relay tree.** Relays form tiers in static config (region top → optional DC → cell). The downstream dials and the upstream
   pushes; gaps are pulled from the replay log. Fan-out for pushed datasets is routed by `(namespace, hash range)`.
   `quicksilver`, `auth` and `cdn` continue using this path; customer-KV values/revisions are not published through it.
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
    crates are `bedrock-quicksilver`, `bedrock-kv`, `bedrock-cdn`, `bedrock-relay` and `bedrock-api` (B-01, decision 12).
11. **Customer KV is per-namespace S3 storage with one logical API writer and read-only KV caches.** Decided 2026-10-29;
    replaces the 2026-10-26 origin-cell design. B-11 supplies a scoped SlateDB-derived generic API-writer engine across datasets;
    B-39 later integrates customer-KV readers/caches on that engine embedded in the `bedrock-kv` binary (customer KV only
    since decision 12), with no separate database service. This decision does not claim that the engine, Raft or the latency targets are implemented.
    - **Persistent layout:** each namespace has its own stable S3 database identity, manifest, indexed SSTs and recoverable
      committed tail. Empty namespaces need only Quicksilver directory metadata. Open writer/reader handles on demand;
      no LSM per virtual bucket, global per-namespace segment manifest or whole-namespace local disk requirement.
      S3 is on the normal read path. Many small databases have object/open/maintenance costs that B-39/B-53 must measure.
    - **Writer and durability:** the home bedrock-api writer orders mutations with per-namespace revisions through
      B-11's shared lib/storage across datasets. One atomic SlateDB batch stores original retained events, resolved
      values/deletes/metadata, namespace revision and original commit time, retry result/fixed expiry, and optional
      represented Raft index/term. Standalone acknowledgement awaits remote durable flush. Startup validates the durable
      database and recent events; no separate raw S3 journal or published/pruned-object protocol is retained.
      Future B-09b acknowledges majority-fsynced Raft entries before batched SlateDB persistence and retains the committed
      not-yet-durable tail for retry/recovery. Truncation requires a recoverable native checkpoint covering every namespace
      in the prefix plus cutoff index/term/membership; a progress record alone is insufficient. Keep committed, buffered,
      durable and reader-visible positions distinct. Fence stale engine publishers. Portable live-key export is optional.
    - **Readers and routing:** ordinary `bedrock-kv` nodes read the same namespace databases and cache hot blocks/entries.
      HRW over `(namespace, key)` selects a preferred reader to reuse cache (no ranges or buckets, decisions 12, 13); it does not shard persistent
      data or impose durable ownership. Another eligible reader may serve. Adding/removing a cache node needs no namespace
      move, replica rebuild or customer-KV relay subscription. Membership still comes from the Quicksilver/Auth connection.
      One logical writer remains a write-throughput limit even when readers scale out.
    - **Read semantics:** optional L1 → preferred healthy cell reader → local blocks → S3. No mandatory regional tier or
      origin cell. `LIST` scans one namespace's full ordered database view; pagination uses original keys and a logical cursor,
      not cache-node locations or a scatter/merge over cache holders. A minimum-revision read requires a sufficiently new
      namespace view, including for an absence. Wait for publication or read coherent recent committed state if the S3 view
      lags; do not fetch only not-found cases. Cache TTL alone does not bound publication/reader-refresh lag.
    - **Local residency:** every KV node, including the home region, independently bounds disk cache, memory/manifest
      overhead, open readers and in-flight opens. Soft per-namespace cached-byte shares may borrow free capacity; quotas on
      total persistent namespace size are separate. Disk pressure evicts cold blocks; handle/memory pressure and idle timeout
      close idle readers. Finish in-flight reads, coalesce reopen and release checkpoint protection safely. An open reader
      need not keep all blocks, and useful blocks can outlive a reader. No global dormant/rehydrating state or eviction broadcasts.
    - **Lifecycle and retention:** closing/evicting a local reader never deletes S3 data or expires customer keys. TTL and
      namespace deletion are explicit data semantics; KV enforces expiration on reads, and background compaction/GC reclaims
      obsolete physical data with reader protection. API writer handles have a separate retention policy and recovery duty.
      B-11 runs bounded, event-triggered asynchronous cleanup, with failures metered independently of completed HTTP responses
      and no idle polling. Event history and retry records have separate configurable time-based retention, both default 24 hours;
      recorded retry expiry never moves on retry/restart. Expired event rows can be removed without losing resolved state;
      preserve native read/checkpoint protection. Idle expiry is logical; physical cleanup may wait for activity. SlateDB's
      internal compaction/GC remain. Node/cell drains release caches/handles;
      they do not move an authoritative KV namespace. B-28 is baseline; B-53 tunes fairness and cold-open costs.
    - **Storage boundary:** preserve the existing synchronous local `StorageEngine` for pushed datasets, and add a shared
      asynchronous namespace contract in `lib/storage`. Range/conditional object I/O and simulation seams live in `lib/s3`;
      KV residency stays in `kv/storage`. Dataset identity/authz must not imply opening a relay-fed local engine. Fork only
      needed capabilities while retaining SST/manifest/recovery compatibility and tests; keep compaction, GC, atomic batches,
      read views and fencing. Later custom readers/writers must preserve that storage protocol or migrate it explicitly.
    - **Push remains separate:** Quicksilver, Auth and CDN retention continue their agreed relay policies. Customer KV has
      no `global`, `full_replica` or `on_demand` push override. B-37 stays dropped. Source WAL replay from S3, when used, is
      a storage recovery/reader mechanism, not a customer-KV relay stream. This does not change CDN byte placement or Auth policy.
12. **Customer KV is its own binary and uses no virtual buckets.** Decided 2026-10-31.
    - **Split:** `bedrock-quicksilver` (`crates/quicksilver`) hosts the pushed `quicksilver` and `auth` datasets with today's
      L1/L2 hash-range keyspaces (decision 13), relay subscriptions and `full_replica` work (B-24, B-40, B-42). `bedrock-kv` (`crates/kv`) is
      customer KV only: S3 namespace readers and caches (B-27, B-28, B-39, B-53). Each publishes its own membership
      (`quicksilver`, `kv`); `bedrock-kv` subscribes to no dataset.
    - **Why:** Quicksilver/Auth are the control plane that Workers, CDN and deploy tooling depend on; customer KV is tenant
      traffic. A hot namespace, cold-open storm or slow S3 must not exhaust the process serving Quicksilver/Auth. The two
      also share no storage engine, replication path or scaling signal.
    - **Routing:** a customer-KV request picks its preferred reader by rendezvous hash of `(namespace, key)` over the
      healthy `bedrock-kv` members of the cell (B-15), without a placement directory, owner checks or range moves. The
      relay-assigned ranges of decision 13 (B-16) serve Quicksilver/Auth and CDN.
    - **Cost:** a fifth binary, one more relay stream per machine (membership only), and cell-local lookups from
      `bedrock-kv` to `bedrock-quicksilver` for the namespace directory and credentials, cached with minimum-revision checks.
13. **L2 placement: relays assign hash-space ranges; nodes subscribe to the directory.** Decided 2026-10-31.
    Replaces the fixed 2²⁰ virtual buckets and per-node rendezvous ownership (B-15, B-16) for the pushed `quicksilver` and
    `auth` datasets and the CDN. Customer-KV cache affinity hashes `(namespace, key)` with rendezvous directly and uses no
    ranges (decision 11's readers own no persistent data). Chosen over a hash ring and over simplified buckets.
    - **Key space:** an L2 key maps to a 64-bit hash of `(dataset, namespace, key)`; CDN artifacts use their content hash.
      Ownership is assigned in contiguous ranges of that hash space, not raw key order, so sequential keys do not concentrate.
      L2 storage keys start with the big-endian hash: a range is one contiguous scan, and dropping it is one `delete_range`
      followed by compaction of that span (RocksDB reclaims range tombstones only on compaction). No bucket count caps a cell.
    - **Directory:** the cell relays' membership publisher (B-12) also writes the placement directory into the same
      `membership/{service}.{cell}` namespace, under `range/{start}` keys (node IDs cannot contain `/`). Membership,
      assignment and readiness therefore share one revision order. An entry holds the range bounds and its owners, each
      `populating`, `ready` or `leaving`. Every node subscribes to it, as it already does to membership; it stays small
      (a few ranges per node). Nodes route locally from that view; relays are never on the read path.
    - **Assignment (B-15):** a pure, deterministic function of the published directory and membership, run only by the
      majority publisher, which waits until its subscription applies each write before planning again. A joining member takes
      ranges, splitting the largest where needed, until its share matches its capacity weight. A member removed after the
      grace period has its ranges reassigned. Each range has R owners in distinct failure domains when possible. Draining
      members get nothing new. A bounded number of ranges move at once. Load-based splitting waits for measurements.
    - **Handoff (B-21):** relays push interests to their own leaf subscribers instead of nodes choosing what to `Follow`.
      When the directory names a subscriber as a new owner, its relay starts the range, bootstrapping where needed. The relay
      derives readiness from its completed bootstrap plus the subscriber's acknowledgement through the head, and the
      publisher marks the owner `ready`. A `leaving` owner keeps following and serving until every new owner is `ready`;
      then its relay sends a revoke. The node confirms the revoke against its own directory view, stops serving and following
      the range, deletes it and compacts the span. Relays act only on published state, never on a sibling's local view.
    - **Owner checks:** a node rejects requests for a range it neither owns nor is leaving, and the caller retries the next
      owner. Directory revisions are ordinary dataset revisions, not a placement epoch.
    - **Failure behavior:** L2 is never the only copy (B-11 materialized state rebuilds Quicksilver/Auth; S3 holds artifact
      bytes), so an early drop costs availability, not data. A leaving owner keeps its copy until the new owners are ready
      and drops early only under disk pressure. Without a relay majority, placement freezes: nothing moves or is revoked, and
      nodes keep serving what they hold.
    - **Why:** a ring or per-bucket rendezvous makes every node derive ownership from membership (about 16 MB of ring tokens
      or a 12 MB bucket table at 10,000 nodes, plus incremental-update logic). Relay assignment gives exact balance, a few
      ranges per node and one component that decides and executes moves, reusing the existing majority publisher and the
      #41 / B-12b sibling stream.
    - **Cost:** relays become the most critical cell component; a wrong assignment affects the whole cell; placement freezes
      without a majority; asymmetric relay partitions can flap assignments as they can flap membership. Relay-pushed interests
      and revokes change `Relay.Session`. Estimated 2026-10-31 at about 10–14 weeks for one developer including
      specification and simulation; load-based splitting adds 3–4 weeks.
    - **Specification first:** `membership.tla` gains the directory writes and a single assigner; a handoff model checks that
      a range is never dropped before its new owners are ready and that no node serves a range it neither owns nor is leaving;
      `relays.tla` covers relay-pushed interests and revokes. Update them before implementation.

### Still open

1. **Auth dataset scope (B-42).** It is only mentioned in `notes.md`, so it needs a short design before it becomes an issue.
2. **Publication and reader-refresh budgets (B-39/B-27).** Choose measured bounds and overload behavior for publication lag,
   manifest refresh, recent-state reads/waits and cold namespace opens. The Frankfurt/R2-EU 100–300 ms goal is a measurement
   target, not a guaranteed S3 latency. An unavailable source is an error, never proof that a key is missing.
3. **Fork and runtime integration (B-11, reader extension B-39).** Pin the source/format baseline and upstream-update policy;
   prove Bedrock's I/O, simulation, range-integrity, checkpoint and recovery contracts before selecting engine features to remove.
4. **Namespace scale (B-53).** Measure per-open metadata, small-object cost, writer/maintenance backlog and cache fairness.
   Grouping namespaces, paged metadata or partitioning a single large namespace requires a later explicit decision.

---

## 5. GitHub issue compatibility audit — 2026-10-29

Scope consolidation, 2026-10-30: descriptions for #8/#9/#10/#11/#14/#16/#25 now incorporate the approved persistence,
single-session and recovery contracts. #9/#11 titles match that scope; issue states, labels and discussion remain.
Drafts B-09b/B-32/B-33 assign reuse and removal ownership, and B-23 reuses the existing validation stack without reducing
its gate. See [the scope follow-through](temporary-paths-audit.md#scope-follow-through--2026-10-30).

The explicit user decision in B-11 and decision 11 above supersedes older linked comments requiring a separate raw
S3 journal or asynchronous standalone commit materialization. The audit counts, preserved-body statements and table
below describe the historical 2026-10-29 review; current descriptions contain the 2026-10-30 consolidation.

Fetched all **26 issues (14 open, 12 closed)** and all issue comments with the GitHub CLI. Each open issue was also fetched
with `gh issue view <number> --repo felixrydberg/bedrock --json number,title,state,body,comments,url`.
The initial audit was read only. The user-authorized follow-up below records the amendments as GitHub comments;
original issue descriptions, titles, labels and states are preserved.

The initial result was **eight required narrow amendments and six compatible open issues**. The shared mechanisms remain
useful; the old customer-KV origin requirements cannot be implemented unchanged or silently waived by a new adapter.
The dated comments below now supersede those specific clauses for affected work. Read each original issue together with
its amendment; unrelated shared-core work can continue. The table below incorporates the second materialized-state scope
review; original issue descriptions and prior customer-KV amendment comments remain preserved.

| Issue / plan ID | Result | Required clarification or retained scope |
|---|---|---|
| [#9 / B-09](https://github.com/felixrydberg/bedrock/issues/9) (closed) | Follow-up | Preserve S3-durable acknowledgement and completed issue state. B-11 adds generic materialization, separate positions, atomic retry/state records and committed-tail retry lookup; future Raft reuses its seam. |
| [#10 / B-10](https://github.com/felixrydberg/bedrock/issues/10) (closed) | Follow-up | Preserve exact stream boundaries and completed issue state. B-11 adds consistent materialized-state bootstrap/resume when a cursor predates retained authoritative history, with view/tail GC protection. |
| [#11 / B-11](https://github.com/felixrydberg/bedrock/issues/11) | Amend | Own generic SlateDB API materialization across datasets, atomic event/state/revision/retry records, direct remote-durable SlateDB commits, time-based retention and durable-database recovery. Future Raft keeps its own committed tail. Replace mandatory portable snapshots/segment compactor with optional separate export. Real engine precedes B-23; no B-39 dependency. |
| [#13 / B-14](https://github.com/felixrydberg/bedrock/issues/13) | Compatible | Keep local `StorageEngine`/CDN health reporting and failure isolation. B-11 adds generic async namespace errors/health integration; B-39 later integrates KV readers without changing completed local-engine contracts. |
| [#14 / B-17](https://github.com/felixrydberg/bedrock/issues/14) | Amend | Remove the clause that a cached KV namespace is followed by its origin cell and the requirement to deliver customer `kv` through relays. The KV process still connects for Quicksilver/Auth, so one connection per process and relay-based membership remain valid. |
| [#15 / B-12](https://github.com/felixrydberg/bedrock/issues/15) | Compatible | Keep membership from existing subscriber streams and service-local views. No additional heartbeat or customer-KV subscription is needed. |
| [#16 / B-18](https://github.com/felixrydberg/bedrock/issues/16) | Amend | Bounded replay remains for pushed datasets. Beyond retained authoritative history, bootstrap B-11 consistent materialized state then resume after its boundary; do not assume all old events remain. Customer KV has no relay replay queue. |
| [#17 / B-18b](https://github.com/felixrydberg/bedrock/issues/17) | Compatible | Retain generic relay introspection, `committed_at` and low-cardinality metrics for streams that exist. Customer-KV storage lag is added by B-39/B-27. |
| [#18 / B-15](https://github.com/felixrydberg/bedrock/issues/18) | Compatible | The pure HRW selector and failure-domain checks still serve replicated data and cache affinity. They do not require customer-KV persistent replicas. |
| [#19 / B-16](https://github.com/felixrydberg/bedrock/issues/19) | Amend | Scope relay subscribe/unsubscribe handoff, owner-only serving and population readiness to replicated buckets. Customer-KV buckets are cache affinity only; any eligible reader can serve without loading a complete bucket. Keep the shared bucket count/table algorithms. |
| [#20 / B-19](https://github.com/felixrydberg/bedrock/issues/20) | Amend | Replace mandatory regional-tier/origin-cell resolution and its acceptance test with eligible cache-reader routing plus S3/recent-state fallback. Do not treat cache readers as full namespace replicas. Retain shared locality/health/freshness discovery. |
| [#21 / B-20](https://github.com/felixrydberg/bedrock/issues/21) | Amend | Remove the required first KV user that moves origins, unsubscribes and deletes an authoritative local range. Retain generic reconciliation, idempotent repair, pushed-data and CDN users. Customer-KV drain only releases local readers/caches. |
| [#22 / B-21](https://github.com/felixrydberg/bedrock/issues/22) | Amend | Remove mandatory complete customer-KV bucket repopulation/redundancy and origin readiness. New cache targets fetch on demand. Retained-data rebuilding uses B-11 consistent materialized state plus committed tail, with integrity/revision validation before readiness. Keep CDN's existing requirements. |
| [#23 / B-22](https://github.com/felixrydberg/bedrock/issues/23) | Amend | Replace customer-KV origins with namespace reader/S3 fallback and publication-lag handling. B-11 durable-database recovery and consistent state bootstrap replace mandatory portable-snapshot recovery; B-39 integrates KV readers. Keep generic source-chain and local `StorageEngine` contracts. |
| [#24 / B-22b](https://github.com/felixrydberg/bedrock/issues/24) | Amend | Protect cache-reader opens, S3 fetches and recent-state reads instead of an origin cell. Preserve bounded queues, per-cell fairness and coalescing; specify its scope per coalescer rather than promising global cross-node deduplication. |
| [#25 / B-23](https://github.com/felixrydberg/bedrock/issues/25) | Amend | Replace origin moves/`CompleteOrigin` with coherent-view rules. Gate the real B-11 engine, atomic state/revision/retry expiry, committed-tail recovery, safe time-based GC and fencing before product tracks; no B-39 dependency. Later B-39 reader/cache integration reruns applicable layers and nightly history. Preserve retained ownership/redundancy, TLC, DST, mutation/crash checks and #26's local-simulation decision. |

Closed issues [#1–#10 / B-01–B-10](https://github.com/felixrydberg/bedrock/issues?q=is%3Aissue+is%3Aclosed),
[#12 / B-13](https://github.com/felixrydberg/bedrock/issues/12) and
[#26 / B-23a](https://github.com/felixrydberg/bedrock/issues/26) were reviewed as prerequisites, not rewritten as new scope.
In particular, B-11 now owns follow-up to #4/B-04 (generic async materialization contract), #6/B-06 (API per-namespace handles),
#7/B-07 (engine range/conditional I/O) and #9/B-09 (direct atomic namespace persistence and durable retry lookup).
B-39 later integrates customer-KV read-only handles/cache policy on those contracts. Closed #9/B-09 and #10/B-10 receive
narrow follow-up comments without reopening or rewriting their original descriptions; seven materialized-state amendments
are posted for #9/#10/#11/#16/#22/#23/#25 and linked below. They supersede only changed clauses and preserve the prior customer-KV amendments.
The #26 simulation seam and runtime rules remain mandatory; upstream SlateDB tests are not a substitute for them.
Its final discussion comment supersedes the earlier CI simulation requirement. The existing #3/B-03 stream-liveness
question (simulated HTTP/2 keepalive versus progress/idle handling) remains for #14/B-17 and #15/B-12; removing customer-KV
subscriptions does not solve or authorize a separate heartbeat protocol for the remaining pushed datasets.

### Amendments recorded in comments — 2026-10-29

Posted through `gh issue comment --body-file` and read back to verify the exact text, one amendment per issue, and unchanged
original descriptions/titles/states. No implementation or hardening gate is claimed complete by these comments.

| Issue / plan ID | Amendment comment |
|---|---|
| #14 / B-17 | [Relay delivery scope](https://github.com/felixrydberg/bedrock/issues/14#issuecomment-6494996597) |
| #19 / B-16 | [Replicated ownership versus cache affinity](https://github.com/felixrydberg/bedrock/issues/19#issuecomment-6494997449) |
| #20 / B-19 | [Reader discovery and freshness](https://github.com/felixrydberg/bedrock/issues/20#issuecomment-6494998291) |
| #21 / B-20 | [Reconciliation and cache drains](https://github.com/felixrydberg/bedrock/issues/21#issuecomment-6494999081) |
| #22 / B-21 | [Retained-data repair and demand-driven KV fills](https://github.com/felixrydberg/bedrock/issues/22#issuecomment-6495003564) |
| #23 / B-22 | [S3 reader fallback and revision guarantees](https://github.com/felixrydberg/bedrock/issues/23#issuecomment-6495004223) |
| #24 / B-22b | [Coalescing and shared read-capacity protection](https://github.com/felixrydberg/bedrock/issues/24#issuecomment-6495004948) |
| #25 / B-23 | [Protocol invariants and staged hardening gates](https://github.com/felixrydberg/bedrock/issues/25#issuecomment-6495005633) |

### Materialized-state amendments recorded in comments — 2026-10-29

The authorized follow-up adds these comments while preserving the original issue descriptions, states and earlier
customer-KV amendments. This records the changed scope and acceptance checks; it does not claim implementation or gate completion.

| Issue / plan ID | Amendment comment |
|---|---|
| #9 / B-09 (closed) | [Generic writer materialization and committed-tail retries](https://github.com/felixrydberg/bedrock/issues/9#issuecomment-6507616173) |
| #10 / B-10 (closed) | [Retained-history bootstrap and stream boundary](https://github.com/felixrydberg/bedrock/issues/10#issuecomment-6507616930) |
| #11 / B-11 | [Generic materialized store, retention and recovery](https://github.com/felixrydberg/bedrock/issues/11#issuecomment-6507617950) |
| #16 / B-18 | [Relay fallback beyond retained history](https://github.com/felixrydberg/bedrock/issues/16#issuecomment-6507618650) |
| #22 / B-21 | [Verified materialized-state recovery sources](https://github.com/felixrydberg/bedrock/issues/22#issuecomment-6507619378) |
| #23 / B-22 | [Materialized recovery and reader integration](https://github.com/felixrydberg/bedrock/issues/23#issuecomment-6507620366) |
| #25 / B-23 | [Real B-11 engine before the initial hardening gate](https://github.com/felixrydberg/bedrock/issues/25#issuecomment-6507621085) |

### Binary-split amendments recorded in comments — 2026-10-31

§4 decision 12 (separate `bedrock-quicksilver` and `bedrock-kv`, no virtual buckets for customer KV) was posted to the
affected open issues only; closed issues (#1/#2 four-binary wording, #15 membership services) keep their history. Each
comment was read back and matched the posted text. Original descriptions and earlier amendments are preserved.
#13, #17, #24 and #41 are unaffected: they name no binary split or customer-KV bucket behavior that changes.

| Issue / plan ID | Amendment comment |
|---|---|
| #18 / B-15 | [Customer-KV routing hashes `(namespace, key)` directly](https://github.com/felixrydberg/bedrock/issues/18#issuecomment-6545940022) |
| #19 / B-16 | [Buckets serve Quicksilver/Auth and CDN only](https://github.com/felixrydberg/bedrock/issues/19#issuecomment-6545940212) |
| #20 / B-19 | [Directory per bucketed service](https://github.com/felixrydberg/bedrock/issues/20#issuecomment-6545940438) |
| #21 / B-20 | [Reconciliation users per binary](https://github.com/felixrydberg/bedrock/issues/21#issuecomment-6545940614) |
| #22 / B-21 | [Reassignment for Quicksilver/Auth and CDN buckets](https://github.com/felixrydberg/bedrock/issues/22#issuecomment-6545940809) |
| #23 / B-22 | [Preferred reader without buckets](https://github.com/felixrydberg/bedrock/issues/23#issuecomment-6545941037) |
| #25 / B-23 | [Five binaries in the hardening gate](https://github.com/felixrydberg/bedrock/issues/25#issuecomment-6545941217) |

### L2 placement amendments recorded in comments — 2026-10-31

§4 decision 13 (relay-assigned hash-space ranges, placement directory in the membership namespace, relay-driven handoffs)
was posted to the affected open issues; closed issues keep their history. Each comment was read back and matched the posted
text. Original descriptions and earlier amendments are preserved. #44 is unaffected. The comments name
`bedrock-quicksilver` and decision 12; read them as the Quicksilver/Auth side of the binary split.

| Issue / plan ID | Amendment comment |
|---|---|
| #13 / B-14 | [Storage health reaches relay assignment](https://github.com/felixrydberg/bedrock/issues/13#issuecomment-6546264337) |
| #17 / B-18b | [Placement and handoff introspection](https://github.com/felixrydberg/bedrock/issues/17#issuecomment-6546264612) |
| #18 / B-15 | [Relay range assignment](https://github.com/felixrydberg/bedrock/issues/18#issuecomment-6546264830) |
| #19 / B-16 | [Hash ranges and the placement directory](https://github.com/felixrydberg/bedrock/issues/19#issuecomment-6546265100) |
| #20 / B-19 | [Sources from the placement directory](https://github.com/felixrydberg/bedrock/issues/20#issuecomment-6546265354) |
| #21 / B-20 | [Handoff reconciliation in the relays](https://github.com/felixrydberg/bedrock/issues/21#issuecomment-6546265598) |
| #22 / B-21 | [Relay-driven range handoff](https://github.com/felixrydberg/bedrock/issues/22#issuecomment-6546265819) |
| #23 / B-22 | [Range-level rebuilds](https://github.com/felixrydberg/bedrock/issues/23#issuecomment-6546266055) |
| #24 / B-22b | [Bounded range moves](https://github.com/felixrydberg/bedrock/issues/24#issuecomment-6546266248) |
| #25 / B-23 | [Placement and handoff invariants](https://github.com/felixrydberg/bedrock/issues/25#issuecomment-6546266463) |
| #41 / B-12b | [Hash-range interest selectors](https://github.com/felixrydberg/bedrock/issues/41#issuecomment-6546266677) |
