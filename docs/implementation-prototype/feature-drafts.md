# Bedrock — Feature Implementation Drafts

Draft GitHub issues for building Bedrock, in the order they should be done. Every draft uses
`.github/ISSUE_TEMPLATE/feature_request.md` (title prefix `Feature 🚀:`, label `enhancement`).
The **Additional context** section of each draft says which implementation it belongs to,
which module it lives in, which doc section it comes from, what it depends on, and when it counts as done.

Sources: `docs/implementation-prototype/kv.md`, `cdn.md`, `code-structure.md`, `notes.md`.

> Status: B-01 to B-29 (including B-12b, B-18b, B-22b and B-23a, but not B-09b), plus B-21b, B-39 and B-61, are created as
> GitHub issues (linked below; the issue is the source of truth). B-26, B-27, B-28 and B-53 are replaced by track O.
> Track O (S-01 … S-19) and the other later drafts are still drafts.

> Architecture updates, newest first:
> - **2026-11-14: stateful actor platform (decision 16, track O).** Durable Objects, D1, Queues, Cron, Workflows and
>   customer-KV writers run as actors on two new binaries, `bedrock-compute` and `bedrock-volume`. The plan is
>   [`statefull/v2.md`](statefull/v2.md). Customer-KV writers are `kv` actors acknowledged by an SSD quorum, so B-26's
>   owner path is not built (PR #74 closed, #58 amended).
> - **2026-11-09:** Quicksilver no longer hosts or places the Auth dataset. Auth serving waits for the Gateway binary.
> - **2026-10-29:** decision 11 replaces customer-KV origin cells with per-namespace S3 databases. §5 audits the issues of
>   that time and links their amendment comments. `kv.md` and `code-structure.md` still describe the older origin design.

---

## 1. Strategy

The docs describe two products, KV and CDN, built on one shared distributed layer (`lib/`).
The CDN doc depends on KV phases 1, 2, 4 and 5. Read closely, almost all of those phases are
**shared behaviour** (membership, health, buckets, placement, replication, relays, reconciliation),
not KV-specific behaviour. So the order is:

1. **Foundation.** Workspace, the `bedrock-quicksilver`, `bedrock-kv` and `bedrock-cdn` binaries, roles, transport.
2. **Shared core (`lib/`).** Everything KV *and* CDN need. It has to be built and hardened
   first, with its own test harness, before either product uses it.
3. **First KV** (`quicksilver/` for Quicksilver, `kv/` for customer KV) and **first CDN** (`cdn/`). Thin product layers on the hardened core.
   The two tracks can run in parallel.
4. **KV refinements** and **CDN refinements**. Product-specific depth: optimisations,
   extra adapters, advanced retention. These can also run in parallel.
5. **Stateful actors** (track O, decision 16). Actor storage and hosting, then the products built on them, including
   customer-KV writes. Product drafts wait for the #25 / B-23 gate like the other product tracks.

**Hardening order:** B-11 provides the generic API-writer atomic persistence and recovery contract before the initial
B-23 gate. That gate covers its real backend, atomic event/state/revision/retry records, durable database recovery, publication
fencing and protected-view GC alongside the retained shared core and minimal reference workload. B-39 (#57) then added
the read-only namespace `Reader`, the value cache and purge on that store. Track O's volume protocol (S-02) and B-09b are
later work with the same protocol-first/revalidation requirement.
Simulation execution follows the latest #26/B-23a discussion: run locally in WSL/Linux/macOS before merging; normal CI
does not imply a simulation pass. Amend #25's older CI wording accordingly while retaining the required seed/replay and
seven-night evidence. This document does not enable a simulation CI job or claim an unrun gate passed.

B-11 adds an asynchronous, SlateDB-based atomic S3 persistence contract in `lib/storage` for the generic API writer across datasets.
Track O reuses it for each actor's S3 database: a volume slot flushes to it and other slots read it through B-39's `Reader`.
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
├── quicksilver/  # bedrock/quicksilver — quicksilver storage and namespaces → binary `bedrock-quicksilver`
├── kv/           # bedrock/kv  — customer KV: value caches, REST API, namespace management → binary `bedrock-kv`
├── cdn/          # bedrock/cdn — artifact_storage, artifact_transfer, paths, retention → binary `bedrock-cdn`
├── compute/      # bedrock/compute — actor host, sandbox, built-in d1/kv/queue/cron     → binary `bedrock-compute` (track O)
└── volume/       # bedrock/volume  — actor claims, quorum log, overlay, cold tier, alarms → binary `bedrock-volume` (track O)
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
| `bedrock-quicksilver` | Pushed `quicksilver` over relay-assigned hash ranges (§4 decision 13) | `l1`, `l2` |
| `bedrock-kv` | Customer `kv` read side: value caches, the REST API and namespace management; no hash ranges or buckets (§4 decisions 12, 16) | `l1`, `l2` (only logged until B-39) |
| `bedrock-cdn` | Dataset `cdn` (path rows and ref keys) and artifact bytes: storage, transfer, regional L2 placement, serving | `l1`, `l2` (artifact cache roles) |
| `bedrock-gateway` (planned, B-57) | Public entry point, internal routing between cells, API-key checks and the local `auth` copy (§4 decision 15); actor routing (S-08) | — (follows its relay's tier) |
| `bedrock-compute` (planned, S-07) | Actor host: Workers sandbox, Durable Objects and the built-in `d1`, `kv`, `queue` and `cron` handlers (§4 decision 16) | — |
| `bedrock-volume` (planned, S-03) | Actor storage: claims, leases, quorum log, overlay, flushes to S3, alarm index; `volume` hash ranges per cell (§4 decision 16) | — |

Rules this implies:

- **Relays are their own binary.** `bedrock-relay` holds no product logic: it is generic over datasets and uses `lib`
  for everything (`lib/relays` for fan-out and tiers, `lib/replication` for streams, `lib/storage` RocksDB for its replay log,
  `lib/membership` for the cell view). A Quicksilver, KV or CDN crash never takes a relay with it, and relays deploy on their own schedule.
- **`quicksilver`, `kv`, `cdn`, `relay`, `api`, `gateway`, `compute` and `volume` depend on `lib` only, never on each other.**
- **KV and CDN share infrastructure, not data.** Writes use `bedrock-api` and its sequencing/durability system.
  `bedrock-quicksilver` subscribes to `quicksilver`; `bedrock-cdn` follows only the `cdn` ranges its relay pushes.
  Customer `kv` has no relay subscriptions, replay queues or subscription-driven placement. Its writes go to the
  namespace's `kv` actor (decision 16). `bedrock-kv` holds a relay connection only for its own `kv` membership,
  and reads the namespace directory from `bedrock-quicksilver` nodes, not from a local copy. Gateways check credentials (§4 decision 15).
- **Customer KV is isolated from the control plane.** Tenant load (hot namespaces, cold-open storms, slow S3) can exhaust
  `bedrock-kv` without taking down Quicksilver/Auth, which Workers, CDN and deploy tooling depend on (§4 decision 12).
- **CDN path rows and ref keys are plain `SET`/`DELETE` writes** to the `cdn` dataset, sent in bulk (B-09c). The write API and
  relays are generic; the only CDN rule in the shared core is that `ref/{sha256}/…` keys are placed by their content hash.
  `bedrock-cdn` keeps them as ordinary range data: no manifest expansion and no reference counts (§4 decision 14).
- **The CDN serves by key and hash and knows nothing about deployments.** A deployment ID is an opaque part of a key. Workers, Pages, deployments, revisions and which version is live
  belong to the Compute and deploy control plane (track L), a client of KV and the CDN (B-50, B-51).
- **Each binary has its own service view.** Membership, health, the placement directory and the regional directory are
  per service, and each binary reads only its own service's views, except that a Gateway and the shared client read the views of the services they call (§4 decision 15). Relays assign L2 ranges per service (§4 decision 13). A node with a broken CDN cache disk is still a healthy KV node, and the reverse.
- **Shared storage implementations live in `lib/storage`.** Keep the existing synchronous local `StorageEngine` contract
  for pushed datasets. B-11 adds a separate asynchronous namespace persistence/recovery contract; B-39 adds read-only
  handles. Do not block on S3 inside the local trait or pretend a read-only handle can write/restore. Dataset identity
  and permissions remain independent of whether a dataset has a relay-fed local engine. `kv/storage` owns the value
  cache budgets; `volume/cold` owns S3 reader handles for actor databases (S-05); `quicksilver/storage` owns the
  pushed-data L1/L2 hash-range keyspaces; CDN keeps `CacheStorage`.

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
| B-09c | Bulk writes: many keys of one namespace in one revision | Shared core | `lib/replication`, `lib/relays` + `api` bin | B-11, B-23 |
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
| [B-21b](https://github.com/felixrydberg/bedrock/issues/68) | Drain nodes, cells and namespaces for every dataset | Shared core | `lib/reconciliation` + `lib/placement` | B-15, B-20, B-21 |
| [B-22](https://github.com/felixrydberg/bedrock/issues/23) | Tiered read fallback and cache rebuild | Shared core | `lib/health` + `lib/reconciliation` | B-14, B-19, B-11 |
| [B-22b](https://github.com/felixrydberg/bedrock/issues/24) | Read protection: coalescing and per-cell limits | Shared core | `lib/health` + callers | B-22 |
| [B-23](https://github.com/felixrydberg/bedrock/issues/25) | Hardening gate: TLA+ model, deterministic simulation, real-binary crash tests | Shared core | `lib` (test support), `docs/spec/` | B-23a, created B-01 … B-22b; excludes later B-09b/B-21b |
| **H — First KV** |||||
| [B-24](https://github.com/felixrydberg/bedrock/issues/55) | Quicksilver dataset | KV | `quicksilver/namespaces` | B-23 |
| [B-25](https://github.com/felixrydberg/bedrock/issues/56) | KV namespace directory and management | KV | `kv/namespaces` | B-24 |
| [B-39](https://github.com/felixrydberg/bedrock/issues/57) | Customer-KV readers and caches on B-11 materialized storage | KV storage integration | `lib/storage`, `lib/s3`, `kv/storage` | B-23, B-04, B-06, B-07, B-11 |
| ~~[B-26](https://github.com/felixrydberg/bedrock/issues/58)~~ | ~~KV write path~~ (replaced by S-18, decision 16) | — | — | — |
| ~~[B-27](https://github.com/felixrydberg/bedrock/issues/59)~~ | ~~KV reads through home-region S3 readers~~ (replaced by S-18, decision 16) | — | — | — |
| ~~[B-28](https://github.com/felixrydberg/bedrock/issues/60)~~ | ~~Namespace reader residency~~ (value-cache budgets landed in B-39; reader handles move to S-05) | — | — | — |
| [B-29](https://github.com/felixrydberg/bedrock/issues/61) | KV end-to-end acceptance | KV | `kv`, `compute`, `volume`, integration tests | B-24, B-25, B-39, S-18 |
| B-58 | Simulation load: KV reads, listing and namespaces | KV | `tests/sim` | B-23, B-29 |
| **I — First CDN** (parallel with H; no dependency on KV drafts) |||||
| B-30 | Artifact storage: S3 content addressing, `CacheStorage`, Foyer | CDN | `cdn/artifact_storage` | B-23 |
| B-31 | Artifact transfer | CDN | `cdn/artifact_transfer` | B-30 |
| B-32 | `cdn` path rows and ref keys (§4 decision 14) | CDN | `cdn/retention`, `cdn/paths` | B-23, B-30, B-09c |
| B-33 | Publish client writing path rows and ref keys in bulk | CDN | `cdn/paths` | B-30, B-32 |
| B-34 | Regional L2 with two-replica artifact placement | CDN | `cdn/retention`, `cdn/artifact_storage` | B-31, B-32 |
| B-35 | Artifact retrieval interface | CDN | `cdn/artifact_transfer` | B-33, B-34 |
| B-36 | CDN end-to-end acceptance | CDN | `cdn` | B-30 … B-35 |
| B-59 | Simulation load: publishing and artifact retrieval | CDN | `tests/sim` | B-23, B-36 |
| **J — KV refinements** |||||
| ~~B-37~~ | ~~Cache-fill broadcast queue~~ (dropped, decision 11) | — | — | — |
| B-38 | Full cache lifecycle and eviction | KV | `kv/storage` | B-29 |
| B-40 | `full_replica` placement and completeness | KV | `quicksilver/storage`, `lib/buckets` | B-24, B-21 |
| B-41 | Adaptive cache promotion and regional load balancing | KV | `kv/operations`, `lib/relays` | B-38 |
| B-42 | Auth dataset | Shared infrastructure | `gateway` | B-57 |
| ~~B-53~~ | ~~Namespace residency fairness and cold-open tuning~~ (replaced by S-05's per-actor database measurements) | — | — | — |
| **K — CDN refinements** (parallel with J) |||||
| B-43 | Artifact replica repair and range migration | CDN | `cdn/retention` | B-36 |
| B-44 | Coordinated eviction, GC and failure recovery | CDN | `cdn/retention` | B-43 |
| B-45 | Prewarming and artifact-set readiness | CDN | `cdn/retention` | B-36 |
| B-46 | Optional unreplicated L1 and popularity-based retention | CDN | `cdn/artifact_storage`, `cdn/retention` | B-44 |
| B-47 | Filesystem `CacheStorage` adapter | CDN | `cdn/artifact_storage` | B-36 |
| B-49 | Large-object and range transfers | CDN | `cdn/artifact_transfer` | B-36 |
| **L — Compute and deploy control plane (future)** |||||
| B-50 | Deploy control plane: publish, activate, roll back, delete | Compute | not decided | B-24, B-33, B-45 |
| B-51 | Gateway: Worker and Pages request routing | Compute | `gateway` | B-35, B-50, B-57 |
| B-48 | Compiled Worker artifact caching | Compute | not decided | B-51 |
| B-52 | Deploy end-to-end acceptance | Compute | not decided | B-29, B-36, B-50, B-51 |
| B-60 | Simulation load: Workers, Pages and deploys | Compute | `tests/sim` | B-23, B-52 |
| **M — Node security** (parent [#44](https://github.com/felixrydberg/bedrock/issues/44); scope amended 2026-11-03, drafts not yet issues) |||||
| B-55 | Mutual TLS and caller identity for node-to-node traffic, enabled by config | Shared core | `lib/transport` + every bin | B-03, B-12, B-17 |
| B-54 | Deployment guide: private networking and node certificates (Kubernetes, Docker, bare metal) | Foundation (docs) | `docs/deployment/` + `deploy/` | B-02, B-55 |
| **N — Shared platform infrastructure (future)** |||||
| B-56 | Shared API rate-limit infrastructure | Shared infrastructure | shared contracts in `lib`; API integration | To be agreed; does not gate initial KV |
| B-57 | Gateway binary and shared cluster client | Shared infrastructure | `gateway`, `lib` client, `lib/relays` | B-19, B-22, B-55 |
| [B-61](https://github.com/felixrydberg/bedrock/issues/66) | Simulation speed: more simulated seconds per real second | Shared infrastructure | `tests/sim`, `scripts/vopr.sh` | B-23 |
| B-62 | Partial quarantine: rebuild only the damaged range | Shared core | `lib/health` + `lib/reconciliation` + `lib/relays` | B-14, B-21, B-22, B-23 |
| B-63 | Operational scenarios: scale out, drain, cancel and rolling restart under load | Shared core | `tests/sim`, `tests/crash` | B-23, B-21b |
| B-64 | Appointed writer per namespace group, with a small controller log (design and model first) | Shared core | `docs/spec/`, later `lib/replication` | B-09b, B-11, B-23 |
| B-65 | Shared durable log store with one group-committed fsync for every writer group on a node | Shared core | `lib/replication` log store | B-09b; B-64 for more than one group |
| B-66 | Safety versus discovery: stale or unreachable discovery inputs may only cost availability | Shared core | `docs/spec/`, `tests/sim`, `tests/crash` | B-23; B-09b and B-57 for their parts |
| B-67 | Explicit operator-controlled membership changes for the API Raft group | Shared core | `lib/replication` + `api` bin | B-09b |
| **O — Stateful actors** (decision 16; scope and acceptance in [`statefull/v2.md`](statefull/v2.md) §11) |||||
| S-01 | Actor IDs, topology record and placement functions | Actors | `lib/actors`, Quicksilver schema | B-24 |
| S-02 | Volume protocol in TLA+: claim, append, lease, release, repair, topology change | Actors | `docs/spec/volume.tla` | decision 16 |
| S-03 | `bedrock-volume` binary: records, gRPC service, membership, ranges, owner checks | Actors | `volume` bin | S-01, S-02, B-21 |
| S-04 | Volume client in Compute: claim, recovery, append window, reads | Actors | `compute/actors` | S-03 |
| S-05 | Cold tier: maintainer, SlateDB flush, truncation, SSD cache, S3 reader handles | Actors | `volume/cold` | S-03, B-11, B-39 |
| S-06 | Slot repair and node-reported range readiness | Actors | `volume/repair`, `lib/relays` | S-02, S-03 |
| S-07 | `bedrock-compute` actor host: acquire, wake, sleep, leases, SQLite VFS, gates | Actors | `compute` bin | S-04 |
| S-08 | Gateway actor routing and location cache | Actors | `gateway/actors` | B-57, S-07 |
| S-09 | Alarms: holder timers and volume index wake | Actors | `compute/actors`, `volume/alarms` | S-07, S-08 |
| S-10 | Durable Objects API | Compute | `compute` | S-07 … S-09, sandbox decision |
| S-11 | D1 | Compute | `compute/builtin` | S-10 |
| S-12 | Queues | Compute | `compute/builtin` | S-09, S-10 |
| S-13 | Cron Triggers | Compute | `compute/builtin` | S-09, B-50 |
| S-14 | Workflows | Compute | `compute` (runtime library) | S-10 |
| S-15 | Hibernatable WebSockets held at the Gateway | Compute | `gateway/actors` | S-08 |
| S-16 | Simulation and crash load: actor workload, kills mid-append, slot loss, cell loss | Actors | `tests/sim`, `tests/crash` | S-07, B-23 |
| S-17 | Planned DC changes and manual DC-loss restore | Actors | `lib/actors`, operator tooling | S-05 |
| S-18 | Customer-KV writers as `kv` actors, with reads through the value cache to the holder (replaces B-26, B-27) | KV | `compute/builtin`, `kv` | S-05, S-08, B-25 |
| S-19 | Load-based range splitting for `volume` ranges (later, gated on measurements) | Actors | `lib/placement`, `volume` | S-03, S-16 |

### Parallel work inside the shared core

- B-04/B-05/B-06 and B-07 have no dependency on each other.
- Track D (B-13, B-14) can start as soon as B-03 and B-04 are done, alongside track C.
- Track F (B-17, B-12, B-18) needs B-10 and B-13. Track E (B-15, B-16, B-19) needs B-12. Track G needs everything before it.
- Track O: S-01 and S-02 can run in parallel. After S-03, S-04 and S-05 and S-06 are independent of each other.

### Mapping to the doc phases

| Doc phase | Where it lands |
|---|---|
| KV Phase 1 (engine, datasets, writes, replication, S3) | B-01 … B-11, plus KV parts in B-24, B-25 and S-18 |
| KV Phase 2 (storage fault tolerance) | B-14, B-22, B-23a, B-23 |
| Generic materialized namespace engine; read-only handles and value caches | B-11; B-39 |
| KV Phase 4 (relays, streams, replay) | B-17 … B-19 (cache-fill broadcast dropped, decision 11) |
| KV Phase 5 (cache routing and residency; pushed-data redistribution stays shared) | B-15, B-16, B-19, B-39, S-18 |
| KV Phase 6 (optimisation) | B-38, B-40, B-41 |
| CDN Phase 1 (artifact storage, transfer, path rows + publish, key- and hash-addressed retrieval) | B-30, B-31, B-33, B-35 (dependency order still applies: B-33 needs B-32's row codec, B-35 needs B-34's L2) |
| CDN Phase 2 (global retention streams, regional placement) | B-32, B-34 |
| CDN Phase 3 (ref-key retention and recovery) | B-32 (ref keys), B-43, B-44 |
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

#### B-09c — Bulk writes: many keys of one namespace in one revision

```
Title:  Feature 🚀: Bulk writes that commit many keys of one namespace in one revision and fan out per range
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A Pages deployment with 10,000 files writes 20,000 CDN keys (§4 decision 14). The write API takes one key per request, and a
deployment must become visible all at once, never half written.

**Describe the solution you'd like**
- A generic bulk route in `bedrock-api`: many `SET`/`DELETE` operations for one dataset and namespace, committed atomically as
  one revision, so every key of a bulk is visible or none is.
- One replication event carries the operations. Relays filter it per key for range streams: each range receives the operations
  it owns, and a range with none notes coverage for that revision only, as for any filtered event today.
- Nodes apply their share and the revision atomically, duplicate-safe, like a single-key event.
- A configured size cap per bulk; larger writes are split by the caller, which records the last revision.
- Reuse the existing event format, sequencing, applier and B-11 materialization; the write API stays free of product code.

**Additional context**
- **Implementation:** Shared core.
- **Module:** `lib/replication`, `lib/relays`, plus the `api` crate.
- **Doc reference:** §4 decision 14; `kv.md` §8.
- **Depends on:** B-11, B-23. Update the TLA+ specification first: one revision covering many keys, and per-range filtering.
- **Acceptance criteria:** A crash mid-bulk never exposes part of it after recovery. A range stream receives exactly its own
  keys of a bulk and coverage for the rest. Replaying a bulk is duplicate-safe. A bulk over the cap is rejected explicitly.
  Normal and `cfg(madsim)` builds pass the applicable B-23 checks.

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

Decided 2026-11-01 while implementing #20: each node selects sources itself (`lib/relays/sources`) from its
`membership::View`, its breakers and holders' rejections. A holder checks the minimum revision when it serves and rejects
a read it is behind on with its applied revision; callers skip it for newer reads for one second. Relay acknowledgements
stay relay-local for B-21 readiness. "Nearby" means the caller's failure domain within its cell; there is no cross-cell
peer discovery. Customer-KV writer resolution moves to S-18 (it needs B-25), and CDN availability filtering to B-34.

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

Full replication simulation, requested 2026-11-01 and deferred from #19 / B-16, where nodes do not yet follow their
ranges. It is a madsim scenario through the real `bedrock-api`, `bedrock-relay`, `bedrock-quicksilver` and `bedrock-cdn`
entry points, with writes running throughout. Each seed must replay to the same trace. It checks:
- **Data follows ownership.** After joins, removals and rebalancing, every `ready` owner holds exactly the committed data
  of its ranges, checked against the writer. No owner is `ready` before its bootstrap is verified.
- **Disconnection.** Relay failover and a restart within the grace period move nothing. A node cut off from every relay
  loses its ranges after the grace period and takes ranges again once healed. A killed node's ranges are rebuilt
  elsewhere.
- **Drain (B-21b).** A draining node gets nothing new, and its ranges hand off.
- **Relay faults.** A sibling continues a handoff whose relay died, working from published state (#41 / B-12b). Without
  a relay majority nothing moves or is revoked.
- **Revoke.** A leaving owner serves until every new owner is `ready`. A revoke that does not match the node's view is
  refused. After deletion, no keys remain in the range's span.
- **One revision order.** No node sees a directory owner that its membership view has never listed.

Membership-only cases that need no data (failover, restart, cut-off, drain and frozen directory) can go into
`tests/sim/tests/membership.rs` earlier.

---

#### B-21b — Drain nodes, cells and namespaces

Created: [#68](https://github.com/felixrydberg/bedrock/issues/68) — Feature 🚀: Generic drain for nodes, cells and single namespaces, with a per-dataset drain hook

Amended 2026-11-08 in an issue comment: the drain record is `drain/{id}` in the cell's membership namespace `{service}.{cell}`
(a cell drain is written to each ranged service's namespace), not `topology/drains/{id}` in Quicksilver. It shares the
namespace's revision order, so it needs no Quicksilver dependency; `docs/spec/membership.md` has the model.

---

#### B-22 — Tiered read fallback and cache rebuild

Created: [#23](https://github.com/felixrydberg/bedrock/issues/23) — Feature 🚀: Generic tiered read fallback and validated cache rebuilding

---

#### B-22b — Origin read protection: coalescing and per-cell limits

Created: [#24](https://github.com/felixrydberg/bedrock/issues/24) — Feature 🚀: Request coalescing at every tier and per-cell rate limits on origin reads

Limits per source on each node (`lib/health/protection`). They are starting values, not measurements; S-05 and S-16 measure them.

| Source | Total running | Running per cell | Queued per cell |
|---|---|---|---|
| S3 fetches | 64 | 16 | 64 |
| Namespace opens | 8 | 2 | 8 |
| Recent committed state | 32 | 8 | 32 |

A request over its cell's queue fails at once with a retryable `Overloaded`. Fairness comes from the first-come shared
queue: a cell can have at most its per-cell share in it. New read paths must wrap these sources: S-05 (S3 reads and reader opens on volume slots) and B-31/B-34 (artifact fetches; warming uses its own lane, e.g. `{cell}/warm`).

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

Created: [#55](https://github.com/felixrydberg/bedrock/issues/55) — Feature 🚀: Quicksilver infrastructure metadata dataset

---

#### B-25 — KV namespace directory and management

Created: [#56](https://github.com/felixrydberg/bedrock/issues/56) — Feature 🚀: Customer KV namespace directory, creation and quotas

Amended 2026-11-11 (user decision): B-25 also builds Quicksilver reads (`Reads` on range owners) and the shared client's
routing-table slice (§4 decision 15 amendment). Records live at `kv/namespaces/{id}` in `quicksilver/infrastructure`; IDs
are random version 4 UUIDs as 32 hex characters (`bedrock_lib::uuid`, seeded under madsim), so storage is `kv/{id}/`.
Deletion marks the record `Deleting`; removing S3 objects waits for B-39. Records name their `tenant` (Cloudflare's
account). `bedrock-kv` serves Cloudflare's namespace API at `/client/v4/accounts/{tenant}/storage/kv/namespaces` (create,
get, rename, delete, paged list, response envelope, per-tenant unique titles); `jurisdiction` is the home region.
The home stays a region; a region-tier router picks the DC (decision 15). DCs are named by the IATA code of the nearest
airport and a mandatory number (`arn1`), checked when node configs load. Open question for B-26/B-39: a home DC, cell or
node that owns a namespace's L3 reads and writes, and whether SlateDB writer fencing supports that.
The dated scope amendment comment on #56 is pending; add its GitHub comment link here after it is posted.

---

#### B-39 — Customer-KV readers and caches on B-11 materialized storage

Created: [#57](https://github.com/felixrydberg/bedrock/issues/57) — Feature 🚀: Customer-KV namespace readers and caches on the shared materialized store

---

#### B-26 — KV write path

Created: [#58](https://github.com/felixrydberg/bedrock/issues/58) — Feature 🚀: Customer KV write path with home-region forwarding

Replaced by S-18 (decision 16, user decision 2026-11-14). PR #74 is closed unmerged; its retry results, event retention,
limit checks and quota accounting are reused in S-18. Amendment:
[#58 comment](https://github.com/felixrydberg/bedrock/issues/58#issuecomment-6867978192).

---

#### B-27 — KV reads through cache-affinity routing and S3 namespace readers

Created: [#59](https://github.com/felixrydberg/bedrock/issues/59) — Feature 🚀: Customer KV get and list through bounded caches and S3 namespace readers

Replaced by S-18 (decision 16). Reads still go L1 → the cell value cache by HRW over `(namespace, key)` with a 60-second
TTL, but a miss goes to the namespace's `kv` actor instead of a home-region S3 reader. The holder answers
minimum-revision reads and `LIST`. The issue amendment is pending.

---

#### B-28 — Namespace reader residency, cache budgets and basic eviction

Created: [#60](https://github.com/felixrydberg/bedrock/issues/60) — Feature 🚀: Per-node namespace reader retention and bounded customer-KV caching

Replaced (decision 16). The value cache budget and per-namespace share landed in B-39 (#73). Reader handle residency
moves to `volume/cold` in S-05, keyed by actor database. The issue amendment is pending.

---

#### B-29 — KV end-to-end acceptance

Created: [#61](https://github.com/felixrydberg/bedrock/issues/61) — Feature 🚀: KV end-to-end acceptance suite on the multi-region deployment

Depends on S-18 now (decision 16). Writer-crash scenarios become holder and slot kills; "home-region reader" scenarios
become value-cache misses to the holder. The issue amendment is pending.

---

#### B-58 — Simulation load: KV reads, listing and namespaces

```
Title:  Feature 🚀: Add customer KV traffic to the simulation's load
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The simulation (#25 / B-23) only sends `PUT` and `DELETE`, because reads do not exist yet, so it proves `MinRevisionReads` by
inspecting engine state rather than through a served read.

**Describe the solution you'd like**
- Add to the fixed load: KV reads with minimum revisions, listing, and namespace create and delete.
- Namespace create, get, list and delete go through `bedrock-kv`'s management endpoint. Give each operation an expected
  result in `tests/sim/src/history.rs`: returned ID/record/list, the `Active` to `Deleting` transition, and rejection of
  opens and writes after deletion. Include a deliberate bug for each request type so its expectation demonstrably fails.
- Quicksilver `Get`/`List` reads may join the fixed load, with expected values, absence and minimum-revision outcomes.
- Prove `MinRevisionReads` through served reads, including absence and listing as #25 requires.
- Run real `bedrock-kv` value caches and `kv` actors under faults: cache loss and pressure, holder sleep and wake, and
  slot loss.
- Add real reads to the simulation. Damage is found today by those and by the scrubber (`health::scrub`, #25 / B-23).

**Additional context**
The simulation sends the same load for every seed, and the seed decides only how it arrives: timing, bursts at the same
average rate, which keys are hot, and value sizes. That keeps the load realistic, so long runs (`scripts/vopr.sh soak`) can
find what only appears under real traffic over time. It keeps working without this task, but cannot reach what the load
does not include. Add to the fixed load, never as a per-seed option, and give each request type its own expected result in
`tests/sim/src/history.rs`: a request nothing checks only creates load.
- **Implementation:** KV.
- **Depends on:** B-23, B-29.
- **Acceptance criteria:** Every new request type has an expected result and a deliberate bug that breaks it. The sweep and a
  long soak pass with the new load.

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

#### B-32 — `cdn` path rows and ref keys

```
Title:  Feature 🚀: CDN path rows and ref keys in the `cdn` dataset
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Which artifacts the CDN keeps, and which artifact a URL path serves, is declared through replicated keys, not by tracking
artifact locations globally (§4 decision 14). This reuses the shared replication and sequencing infrastructure
(`bedrock-api`, `bedrock-relay`), not KV itself. The `cdn` dataset belongs to the CDN: `bedrock-quicksilver` and `bedrock-kv`
never host or subscribe to it, just as `bedrock-cdn` never subscribes to a KV dataset.

**Describe the solution you'd like**
- A `cdn` dataset (B-06) with global namespaces, replicated to every region. The deploy control plane (B-50) uses `workers` and
  `pages`; the CDN treats namespace names and deployment IDs as opaque. It holds metadata only, never artifact bytes, and its
  records are never evicted.
- Two key kinds, written in bulk through `bedrock-api` (B-09c). The write API and relays are generic over datasets:

  | Key | Value | Placed by |
  |---|---|---|
  | `path/{deployment_id}/{path}` | `{sha256, content type, size}` | the normal `(dataset, namespace, key)` hash |
  | `ref/{sha256}/{deployment_id}` | empty | the sha256's first 8 bytes, ignoring the namespace |

- The one shared-core change: `placement::hash_of` places `cdn` keys of the form `ref/{64 hex}/…` by that hash, so relays route
  them, and nodes store them in the hash-first L2 layout, by content. Every other key, including a malformed `cdn` key, keeps the
  normal hash.
- One canonical codec for path-row values in `cdn/paths`, shared with B-33 and B-35. A malformed value is an explicit error.
- `bedrock-cdn` follows the `cdn` ranges its relay pushes (#22 / B-21) and keeps both key kinds as ordinary L2 range data in
  `lib/storage`. An artifact is retained while any `ref/{sha256}/…` key exists in any namespace: one prefix scan, no counts.
- No derived state: range bootstrap (B-18, B-21) rebuilds everything a holder needs, with its existing validation,
  exact-boundary resumption and readiness. No manifest expansion, reference-count table or second recovery path.
  No artifact locations are stored globally.

**Additional context**
- **Implementation:** CDN, plus the `hash_of` rule in the shared core. Writes, ordering and fan-out come from the shared infrastructure.
- **Module:** `cdn/retention`, row codec in `cdn/paths`, `lib/placement`.
- **Doc reference:** `cdn.md` §3 Declarative Retention, §4 Phase 2, Core Principle 5; §4 decision 14 in this file.
- **Depends on:** B-23, B-30 and B-09c; reuse created B-08/B-11/B-17/B-18/B-21 contracts.
- **Acceptance criteria:**
  - A `ref/` key reaches exactly the owners of its sha256's range in every cell, whatever its namespace.
  - Two deployments sharing an artifact keep it retained; releasing one keeps the shared artifact.
  - Wiping `bedrock-cdn`'s local state rebuilds identical rows and ref keys through range bootstrap under concurrent writes;
    corruption, interruption and lease expiry cannot establish readiness.
  - `bedrock-quicksilver` and `bedrock-kv` receive no `cdn` events, and the `quicksilver`, `kv` and `cdn` crates have no dependency on each other.

---

#### B-33 — Publish client writing path rows and ref keys in bulk

```
Title:  Feature 🚀: Idempotent publish and release client for path rows and ref keys
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A set of related artifacts (for example a Pages site with thousands of files) is published under one deployment ID.
Publishing must guarantee that once it returns, every path row points at stored, retained content.
The CDN is a backing service: it knows keys and artifacts, never deployments, Workers or which version is live.

**Describe the solution you'd like**
- Reuse B-32's row codec and B-30's content-addressed storage. This draft owns publish orchestration only.
- `publish(namespace, deployment_id, files)` in `cdn/paths`:
  1. Upload every artifact to S3 by sha256. Uploads are idempotent; never skip one because another tenant stored it.
  2. Bulk-write (B-09c) every `path/{deployment_id}/{path}` row and one `ref/{sha256}/{deployment_id}` per distinct sha256,
     split into bulks under the size cap. A Worker writes only the ref key of its bundle.
  3. Return the revision of the last bulk once steps 1-2 have succeeded.
- `release(namespace, deployment_id, files)` deletes the same keys in bulks under the size cap; a failed bulk is retried.
- **Contract for callers:** after `publish` returns, the deployment's rows are readable at that revision and their artifacts are
  retained until `release`. Callers write their own references to the deployment (for example in Quicksilver) only after
  publishing, and remove them before releasing. The deploy control plane (B-50) is the first such caller.
- Publishing is idempotent: rerunning it with the same files writes the same keys.
- `bedrock-cdn` follows only `cdn` ranges, never `quicksilver` or any other KV dataset.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/paths`.
- **Doc reference:** `cdn.md` §3 Deployment Integration; §4 decision 14.
- **Depends on:** B-30, B-32.
- **Acceptance criteria:** `publish` never returns while a row's artifact is missing from S3 or unretained. Killing the client
  between any two steps and rerunning it produces the same end state. A crash never makes part of one bulk visible.
  `bedrock-cdn` receives no events from any KV dataset.

---

#### B-34 — Regional L2 with two-replica artifact placement

```
Title:  Feature 🚀: Regional L2 artifact placement with two verified replicas per retained artifact
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Retained artifacts must be held by two verified nodes per cell, chosen with the shared range placement (decision 13).
Local availability must be tracked separately from range ownership.
A publish must not make both cold holders independently download the same artifact from S3.

**Describe the solution you'd like**
- The artifact hash falls in one `cdn` hash range (B-16); that range's two owners in the placement directory (B-15) are the
  holders. Relays deliver its `ref/` keys only to them (§4 decision 14).
- Only the selected holders initiate prewarming for an artifact, when its first `ref/` key arrives.
- Reuse verified local or peer copies first. On a completely cold miss, the first holder fetches from S3.
  The second requests the artifact from the first through B-31, starting or joining its coalesced fetch.
- If the first holder fails or times out, the second falls back to S3 using B-31's retries and timeouts.
  Artifact bytes use the existing bulk channel; relays continue carrying metadata.
- A reconciler (B-20):
  - desired = retained artifacts in my ranges only when read within the idle window (B-44); initial retention prewarming remains separate
  - actual = local inventory
  - repair = fetch via B-31
- Track artifact availability separately from ownership and filter `relays::sources::Sources::select` by it (deferred from #20 / B-19, which returns range owners only).
- Range repopulation hook for B-21.
- Per-service owner count: B-16 made the owners per range (`REPLICAS = 3`) and the ranges moving at once
  (`MAX_MOVING = 16`) constants in `crates/lib/src/membership.rs`. Make them per-service settings that the relay reads
  at startup, from an environment variable or `[membership]`, with `cdn` set to 2.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/retention`, `cdn/artifact_storage`.
- **Doc reference:** `cdn.md` §3 Regional L2, §4 Phase 2, Core Principle 3.
- **Depends on:** B-31, B-32.
- **Known limits:** Download suppression is per cell and best-effort. Separate cells fetch independently; failover or
  differing membership views can cause duplicate S3 downloads.
- **Acceptance criteria:**
  - After a publish, every artifact has two verified holders in each cell. Availability lookups return only holders that really have the bytes.
  - With stable placement and healthy nodes, concurrent prewarming on both cold holders causes one S3 download and produces two verified copies.
  - The second holder can request the first before it starts prewarming; concurrent requests on the first share one fetch.
  - Failure or timeout of the first holder lets the second complete from S3 without waiting indefinitely.

---

#### B-35 — Artifact retrieval interface

```
Title:  Feature 🚀: Key- and hash-addressed CDN retrieval interface (streaming API and HTTP)
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Callers need a path row and then the artifact from the nearest healthy copy, both by point lookup.

**Describe the solution you'd like**
- `get_path(namespace, deployment_id, path, min_revision)` returns the row; `get_artifact(sha256)` streams the bytes. The CDN
  never knows what a deployment is; callers (the Gateway, B-51) resolve which deployment they want themselves.
- A path row is read from its key's owners at `min_revision` or later; an owner that is behind waits or the caller retries the next.
- Artifacts use the B-22 `SourceChain` with CDN tiers: local cache → L2 holders of the sha256 → peers → S3.
- One HTTP surface (`/paths/{namespace}/{deployment_id}/{path}`, `/artifacts/{sha256}`) and the same calls as an internal API.
- Correct content type and caching headers for immutable content.

**Additional context**
- **Implementation:** CDN.
- **Module:** `cdn/artifact_transfer` (retrieval), served by the `bedrock-cdn` binary.
- **Doc reference:** `cdn.md` §3 Deployment Integration, §4 Phase 1 ("Basic Worker and Pages artifact retrieval"; the
  Gateway is B-51).
- **Depends on:** B-33, B-34.
- **Acceptance criteria:** Requests are served from L2 in steady state. Artifact requests still succeed when both L2 holders are
  down (via S3). The returned bytes always match the row's sha256. A missing row returns not found; a row behind `min_revision`
  is never returned.

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
  - publish a deployment, then fetch its path rows and artifacts from every region
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

#### B-59 — Simulation load: publishing and artifact retrieval

```
Title:  Feature 🚀: Add CDN traffic to the simulation's load
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
The simulation (#25 / B-23) writes keys to the `pages` and `workers` namespaces of the `cdn` dataset. Those are plain key
writes, so nothing exercises publishing, path rows or artifact bytes.

**Describe the solution you'd like**
- Add to the fixed load: bulk publishes writing path rows and ref keys, and artifact retrieval, including many concurrent cold
  requests for one artifact.
- Add `CacheStorage` and S3 artifact faults to `tests/sim/src/storage.rs`.
- Check that a retrieved artifact matches what was published, and that retention never removes a referenced artifact.

**Additional context**
The simulation sends the same load for every seed, and the seed decides only how it arrives: timing, bursts at the same
average rate, which keys are hot, and value sizes. That keeps the load realistic, so long runs (`scripts/vopr.sh soak`) can
find what only appears under real traffic over time. It keeps working without this task, but cannot reach what the load
does not include. Add to the fixed load, never as a per-seed option, and give each request type its own expected result in
`tests/sim/src/history.rs`: a request nothing checks only creates load.
- **Implementation:** CDN.
- **Depends on:** B-23, B-36.
- **Acceptance criteria:** Every new request type has an expected result and a deliberate bug that breaks it. The sweep and a
  long soak pass with the new load.

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
The bounded value cache from B-39 needs workload-driven admission and eviction tuning. Pushed datasets still
need their own recent-update behavior; customer KV receives no push stream.

**Describe the solution you'd like**
- Tune hot/warm/cold admission and pressure behavior using measured cache reuse.
- Preserve the L1 recent-update window for pushed datasets only. Customer KV admission follows reads and freshness checks.
- Score by frequency, age, pressure and policy, respecting B-39's byte budget and per-namespace share.
- Metrics: hit rate, admission/eviction reasons, cache churn and p99 read latency; namespace details remain introspection/top-N.

**Additional context**
- **Implementation:** KV refinement.
- **Module:** `kv/storage`; the pushed-data L1 recent-update window lives in `quicksilver/storage`.
- **Doc reference:** `kv.md` §9 L1, §16.
- **Depends on:** B-29.
- **Acceptance criteria:** Hit rate improves on a benchmark workload compared with B-39's cache. State transitions are observable in metrics.

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
- The Quicksilver read server answers only for ranges its node holds in `ready` or `leaving`; it checks that state and its
  applied revision for each request. Completeness reporting must use the same held-range/readiness definition, not count
  assigned or populating ranges as complete.
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
`notes.md` lists an Auth dataset (API key hashes, permissions, status). §4 decision 15 places it on the Gateways.

**Describe the solution you'd like**
- An `auth` dataset with a `keys` namespace (§4 decision 13, fixed namespaces). Every cell-tier Gateway follows the whole
  namespace into local durable storage and is not ready until it has caught up. It never uses the `cached` TTL model: a
  revocation must not wait for a TTL.
- Fail-closed behaviour for revocations: a Gateway rejects keys once it has gone 30 seconds without proof that its copy is
  current (decision 15).
- Each key record holds its permissions, status, allowed IP ranges and expiry, evaluated locally on the Gateway.
- Proposed key format, to settle before the first key is issued: a recognizable prefix and a checksum, so secret scanners can
  find leaked keys and malformed ones are rejected without a lookup.

**Additional context**
- **Implementation:** Shared infrastructure (new dataset).
- **Module:** `crates/gateway` (binary `bedrock-gateway`, decision 15).
- **Doc reference:** `notes.md`; `kv.md` §17 Freshness Guarantees ("access revocations require fencing… or fail-closed").
- **Depends on:** B-57.
- **Acceptance criteria:** A revoked key is rejected in every region within 30 seconds, or the check fails closed.

---

#### B-53 — Namespace residency fairness and cold-open tuning at scale

Replaced (decision 16). Customer-KV writes and reads now go through `kv` actors, so the cost of many small S3 databases
is measured once, for all actors, in S-05 (`statefull/v2.md` §12 question 2). The fallback named there is a chunk store
with a conditional-PUT manifest.

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
- Coordinated eviction when an artifact's last `ref/` key is deleted, across both holders.
- Idle eviction: both L2 holders evict a retained artifact that has not been read within a configurable idle window, and
  under storage pressure the least recently read first. S3 keeps it, it stays retained, and the next miss fetches it again.
- S3 garbage collection with a grace period. GC deletes an S3 object only when:
  - it was released (its last `ref/` key was deleted), not merely never retained. Objects uploaded by a publish that has not
    written its ref keys yet (B-33 step 1) are never candidates, and
  - it is older than the grace period, and no `ref/` key for it exists at deletion time (re-checked by the holders, since a
    new deploy can retain the same content hash again).
- Deletion is version-protected: the bucket is versioned, and GC deletes only the object version it found released and past
  the grace period. A publish always uploads (B-33 step 1), creating a newer version that a concurrent GC never deletes, so
  re-checking ref keys is not the only guard.
- S3 GC runs in one designated cell: its holders of a range see every ref key of the artifacts in it.
- Orphans from failed publishes (uploaded, never retained) are swept separately, only after a much longer grace period than any publish takes.
- Recovery of ref keys after relay and node loss through range bootstrap (B-21), using B-11's consistent materialized-state view and subsequent retained database events, resuming the stream boundary.
- Reconciliation without per-artifact global location tracking.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 3).
- **Module:** `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Declarative Retention, §4 Phase 3.
- **Depends on:** B-43.
- **Acceptance criteria:** An artifact that is still referenced is never deleted from S3. An idle retained artifact leaves L2
  after the idle window and is served from S3 on the next request. Released artifacts are reclaimed within the grace window. State converges after a relay failure. An artifact uploaded by an in-progress publish is never deleted before its ref keys arrive. An artifact released by one deployment and retained by another during GC is kept.

---

#### B-45 — Prewarming and artifact-set readiness

```
Title:  Feature 🚀: Regional prewarming on publish and readiness reporting for a set of artifacts
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Newly published content should not start cold in every region, and callers need to know when it is warm.

**Describe the solution you'd like**
- Prewarm: when a holder applies an artifact's first `ref/` key, it fetches the artifact (B-34).
- Readiness: given a set of sha256s (for example a deployment's files, which the caller published), each cell groups them by
  owner from its placement directory, asks each owner which it holds verified, and reports whether both holders have every
  artifact, through the retrieval API (B-35). Callers can wait for all cells, or a threshold with a timeout, before they
  start sending traffic.
- Waiting is optional: using a deployment early is still correct, since misses fall back to S3, just cold.

**Additional context**
- **Implementation:** CDN refinement (CDN Phase 4).
- **Module:** `cdn/retention`.
- **Doc reference:** `cdn.md` §3 Deployment Integration, §4 Phase 4; §4 decision 14.
- **Depends on:** B-36.
- **Acceptance criteria:** A set reported ready in a cell gets close to 100% L2 hits there on its first requests.
  Readiness turns false when a holder is lost and true again after repair (B-43).

---

#### B-46 — Optional unreplicated L1 and popularity-based retention

```
Title:  Feature 🚀: Optional unreplicated L1 artifact cache and popularity-based retention
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Hot artifacts gain from per-node caching. Retention driven only by ref keys ignores real demand.

**Describe the solution you'd like**
- An optional per-node L1 that caches path rows and artifact bytes by sha256 in memory, evicts automatically and is never replicated.
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
as a client of those services. Nothing in tracks A-K depends on this track. Worker execution runs in `bedrock-compute`
(track O); the deploy control plane's crate is decided when the track starts.

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
- Read `deployment/{id}` and active-deployment records through the shared client and Quicksilver `Reads` API; do not add a
  separate deployment-record read path.
- Quicksilver schema, owned by this draft (typed accessors included): `deployment/{deployment_id} → {runtime settings, bindings,
  env/secret references, cdn_revision, bundle sha256 for Workers}` and `worker/{worker_id}/active → deployment_id`. A project
  name or host resolves to a `deployment_id` here; the CDN only ever sees the UUID.
- **Publish and activate**, in this order:
  1. Publish the artifacts with the CDN publish client (B-33) under the new `deployment_id`, into the `workers` or `pages`
     namespace. It returns `cdn_revision` once the bytes are uploaded and the path rows and ref keys are written.
  2. *(Optional)* wait for readiness of the deployment's artifacts (B-45).
  3. Write `deployment/{deployment_id}` to Quicksilver.
  4. Write `worker/{worker_id}/active` to make it live. Can be batched with step 3, or written later.
  5. Send the deployment's cron schedules to the Worker's cron actor (S-13). Rerunning sends the same schedules again.
- **Why this order:** Quicksilver only ever names deployments whose rows exist and whose artifacts are retained, so CDN GC
  (B-44) cannot delete artifacts a deployment points at. A deploy that fails after step 1
  leaves retained rows that nothing uses (harmless, cleaned up by a retry or a release).
- **Rollback:** write a previous `deployment_id` to `worker/{worker_id}/active`. Keep the N previous deployments' rows and ref keys.
  Only requested artifacts that remain warm within the idle window (B-44) are guaranteed to be served from L2; cold artifacts may
  fetch from S3 unless the deployment was prewarmed (B-45). Reading the deployment record does not warm its artifacts.
- **Delete** runs in reverse: remove `deployment/{deployment_id}` (it must not be active), then `release` (B-33).
- Every step is idempotent, keyed by `deployment_id`.

**Additional context**
- **Implementation:** Compute / deploy control plane.
- **Doc reference:** `notes.md` (Worker Execution); `cdn.md` §3 Deployment Integration.
- **Depends on:** B-24, B-33, B-45.
- **Acceptance criteria:** Killing the control plane between any two steps and rerunning the deploy produces the same end state.
  No Quicksilver deployment ever references unretained artifacts. Rollback requests for artifacts that remain warm within the
  idle window need no S3 fallback; cold artifacts may require S3 unless the deployment is prewarmed.

---

#### B-51 — Gateway: Worker and Pages request routing

```
Title:  Feature 🚀: Gateway routing Worker and Pages requests through Quicksilver and the CDN
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A request arrives with a host and a path. Something must decide whether it is a Worker or a Pages deployment and turn it into
point lookups; the CDN and Quicksilver only answer for keys and artifacts.

**Describe the solution you'd like**
- The Gateway resolves the host (or project name) to the active `deployment_id` in Quicksilver, then reads
  `deployment/{deployment_id}`, which says Worker or Pages.
- Use the shared client and Quicksilver `Reads` API for both records.
- Worker: route the request to a node that invokes it. A cold start fetches the bundle with `get_artifact(bundle sha256)` (B-35).
- Pages: `get_path("pages", deployment_id, path, cdn_revision)` gives the sha256, then `get_artifact(sha256)` from its holders.
  Index files, SPA fallbacks, redirects and the 404 page are further path rows.
- Path rows and artifacts are immutable, so the Gateway caches both in memory without invalidation; a warm request makes no
  CDN lookup.
- Content type, caching headers and not-found handling for deployments.

**Additional context**
- **Implementation:** Compute / deploy control plane, running in `bedrock-gateway` (B-57).
- **Doc reference:** `notes.md` (Worker Execution); `cdn.md` §4 Phase 1 ("Basic Worker and Pages artifact retrieval"); §4 decisions 14, 15.
- **Depends on:** B-35, B-50, B-57.
- **Acceptance criteria:** A Pages request resolves to the active deployment's file. Switching `active` changes the served
  version in every region once Quicksilver has propagated. A Pages request never sees a row behind its deployment's
  `cdn_revision`.

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

#### B-60 — Simulation load: Workers, Pages and deploys

```
Title:  Feature 🚀: Add Worker, Pages and deploy traffic to the simulation's load
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Deploys span KV, CDN and the control plane, and a Worker or Pages site is made of several writes. Random key writes cannot
show that a deployment is ever half-visible.

**Describe the solution you'd like**
- Add to the fixed load: create, activate, roll back and delete for Workers and Pages sites, alongside the KV and CDN traffic.
- Run the Gateway and control plane binaries in the simulation with the others.
- Check that a request is served by exactly one complete deployment, and that a rollback restores the previous one.

**Additional context**
The simulation sends the same load for every seed, and the seed decides only how it arrives: timing, bursts at the same
average rate, which keys are hot, and value sizes. That keeps the load realistic, so long runs (`scripts/vopr.sh soak`) can
find what only appears under real traffic over time. It keeps working without this task, but cannot reach what the load
does not include. Add to the fixed load, never as a per-seed option, and give each request type its own expected result in
`tests/sim/src/history.rs`: a request nothing checks only creates load.
- **Implementation:** Compute / deploy control plane.
- **Depends on:** B-23, B-52.
- **Acceptance criteria:** Every new request type has an expected result and a deliberate bug that breaks it. The sweep and a
  long soak pass with the new load.

---

### M — Node security (proposal for #44)

> Proposal, 2026-11-03. [#44](https://github.com/felixrydberg/bedrock/issues/44) asks for a Nebula overlay between all
> nodes, mTLS on node-to-node RPC, and certificates issued and stored in OpenBao. Scope update, 2026-11-03 (user
> decision): Bedrock runs on Kubernetes, customer Docker hosts and bare metal, so the binaries implement only mTLS,
> enabled by configuration. Private networking (Nebula or the platform's own) and certificate issuance (OpenBao or any
> other CA) are deployment choices, documented per deployment type. OpenBao is the reference issuer, not a requirement.
> This narrows #44; the [scope amendment](https://github.com/felixrydberg/bedrock/issues/44#issuecomment-6598728634)
> is recorded there.

Today every listener binds a public address in `network_mode: host` and carries plaintext: relay and writer `Relay`/`Siblings`
gRPC (`crates/relay/src/lib.rs`, `crates/api/src/lib.rs`), the write API over HTTP/1 with a shared bearer token, and the
admin endpoint (`/metrics`, `/health`, `/ready`). Any host that can reach a relay can open a leaf session and announce
any `Member` (`node_id`, `service`, `cell`, `address`, budgets), which places it in membership and L2 range assignment.

- **mTLS (B-55, binaries)** authenticates each process: an X.509 certificate per binary instance names its service, cell
  and node. Servers authorize callers by that identity, and relays bind the announced `Member` to it. It must hold on its
  own over public links, because a private network is optional.
- **Deployment guide (B-54, docs)** covers how certificates reach the configured paths and how the network is made
  private on each deployment type. Nebula is one of those options, for machines spanning several networks. It needs no
  Bedrock code: `listen` and `advertise` point at the overlay address.

---

#### B-55 — Mutual TLS and caller identity between nodes

```
Title:  Feature 🚀: Mutual TLS and caller identity for node-to-node traffic
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Node-to-node gRPC and the write API are plaintext. A relay accepts any leaf session and trusts the `Member` it
announces, so a host that reaches a relay can join membership under another node's `node_id` and receive L2 ranges.

**Describe the solution you'd like**
- **Enabled by configuration, fail closed.** `NodeConfig` gains an optional `tls = { cert, key, ca }` (file paths).
  - Absent: plaintext with a startup warning and the gauge `transport_tls_enabled 0`, except production cell relays
    with membership enabled, which require TLS and fail startup without it.
  - Present: mTLS is required on every node-to-node listener and connection. Until the files load, `/ready` stays false;
    if they do not load within a fixed startup timeout, the process exits with an error. A configured node never falls
    back to plaintext. A file appearing at a path does not by itself enable TLS.
  - A TLS server rejects plaintext clients and the reverse, so a cell runs one mode. Moving an existing cell to TLS is a
    cutover; there is no mixed mode.
- **Certificate contract** (what any issuer must produce, documented in B-54): a PEM private key, a PEM certificate chain
  and a PEM CA bundle. The leaf certificate has extended key usages `serverAuth` and `clientAuth`, a URI SAN
  `bedrock://{service}/{cell}/{node_id}` (`service` ∈ `quicksilver`, `kv`, `cdn`, `relay`, `api`; `node_id` and `cell`
  already exclude `/`), and a DNS or IP SAN for the host in `advertise`. Startup rejects a certificate whose URI SAN
  does not match the node's own `service`, `cell` and `node_id`.
- **Identity.** One parser in `lib/transport` turns a verified peer chain into `Caller { service, cell, node_id }`.
- **Transport.** `Peers::channel` dials `https://` with the client certificate and verifies the server chain against the
  CA bundle and the dialed host against the server's SANs. Every server requires a client certificate from that bundle.
  TLS 1.3 only, through rustls/tokio-rustls (already in the lockfile) on tonic's `serve_with_incoming` and
  `connect_with_connector_lazy`, so certificates can be swapped (see Rotation).
- **Authorization** in the RPC handlers, from `Caller`, never from fields in the request:

  | Server | Accepts |
  |---|---|
  | api `Relay.Session` | `relay` |
  | relay `Relay.Session`, leaf (`tier` 0) | `quicksilver`, `kv`, `cdn` in the relay's own cell |
  | relay `Relay.Session`, lower relay tier | `relay` |
  | relay `Siblings.Watch` | `relay` in the same cell |
  | api write HTTP (HTTPS when `tls` is set) | Any caller; a client certificate is optional and every caller keeps the bearer token (open question 1) |

  Clients check the server too: a leaf or relay session only accepts frames from a `relay` or `api` peer.
- **Membership binding.** A leaf `Hello`'s `Member` must match the caller: same `service`, `cell` and `node_id`, and
  the host in `address` must be one of the caller certificate's SANs. Otherwise the relay rejects the session with
  `PERMISSION_DENIED`, which is a caller error and does not trip the caller's breaker for the relay (B-13). Budgets and
  roles stay self-reported.
- **Rotation without restart.** Every handshake uses the current rustls configs, on the server through the TLS
  acceptor and on the client through the custom connector, so new handshakes pick up renewed files. Files are polled
  every 10 seconds and compared by content, which also catches Kubernetes' atomic symlink swap of mounted Secrets. A
  renewal that fails to parse, has expired or does not match the node keeps the previous certificate and raises an
  error metric. Existing connections keep their handshake identity: gRPC servers begin a graceful close after 24
  hours (`max_connection_age`, no grace limit), so long-lived relay sessions keep running, and the write listener has
  no age limit.
- **Admin endpoint** stays plain HTTP without client certificates. Deployments bind it to loopback or a private
  interface (B-54).
- **Metrics.** `transport_tls_enabled`, inbound handshake failures, reload failures and seconds until the local
  certificate expires, all without labels. Rejected sessions are logged with the caller's identity.
- **Simulation.** madsim-tonic ignores TLS and has no `serve_with_incoming`, so the TLS adapter is compiled out under
  `cfg(madsim)`, as RocksDB is: simulated nodes run plaintext, and configuring `tls` there fails at startup.
  Authorization is a pure function of the `Caller` (unit-tested in both builds); real-socket tests with a generated CA
  cover the wiring.
- **Specification.** Authorization only removes callers the protocol already treats as failed peers, and adds no
  messages or states; `relays.tla` and `membership.tla` stay unchanged. Revisit if review shows otherwise.

**Additional context**
- **Implementation:** Shared core (`lib/transport`) plus wiring in every binary.
- **Module:** `lib/transport`, `lib/node` (config), `lib/relays` (session authorization and membership binding), `api`.
- **Depends on:** B-03 (transport), B-12 (membership entries), B-17 (relay sessions).
- **Doc reference:** #44; `kv.md` §17 (security-sensitive actions fail closed).
- **Acceptance criteria:**
  - Real-binary test with a CA generated in the test: a session without a client certificate, with a certificate from
    another CA, with an expired certificate, or as the wrong service is rejected; a valid one succeeds.
  - A leaf announcing another `node_id`, another cell or an address outside its SANs is rejected before it joins
    membership.
  - With `tls` set and the files missing, the node stays unready and then exits; it never serves plaintext. Without
    `tls`, non-membership nodes serve plaintext and report `transport_tls_enabled 0`; production cell membership
    rejects startup without TLS.
  - Replacing the files in place and by symlink swap changes the certificate on new handshakes without dropping
    sessions; a corrupt or mismatched renewal keeps the old certificate and is reported.
  - Normal and `cfg(madsim)` builds pass the existing seeds; bulk-transfer throughput with TLS is measured against
    plaintext and recorded.

---

#### B-54 — Deployment guide: private networking and certificate issuance

```
Title:  Feature 🚀: Deployment guide for private networking and node certificates
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
B-55 makes the binaries require certificates at configured paths but does not say where they come from. Bedrock runs on
Kubernetes, customer Docker hosts and bare metal at several providers, and each has its own way to issue certificates and
keep traffic private.

**Describe the solution you'd like**
- A guide under `docs/deployment/` with the B-55 certificate contract, and per deployment type how the files reach the
  configured paths, how they renew before expiry, how the network is restricted, and where the admin endpoint binds:

  | Deployment | Certificates | Private network |
  |---|---|---|
  | Kubernetes | cert-manager `Certificate` per pod → Secret mounted read-only. With OpenBao, cert-manager's Vault issuer (compatibility to be verified) | The cluster's CNI (for example WireGuard encryption) and NetworkPolicies limiting each port to the callers in B-55's table |
  | Docker / compose | OpenBao Agent container writing into a volume shared with the services, or any script that writes the files | Host firewall; Nebula when hosts span networks |
  | Bare metal across providers | OpenBao Agent as a systemd unit | Nebula, below |
  | Local / tests | No `tls` section, or a test CA from a script | — |

- **OpenBao reference setup**: a PKI intermediate with one role per service (`client_flag`,
  `server_flag`, URI SANs limited by `allowed_uri_sans_template` to the machine's own `cell` and `node_id`, DNS SANs
  by `allowed_domains_template` to its own host, IP SANs off, 7-day TTL, `generate_lease` set). One AppRole per
  machine, whose response-wrapped `secret_id` is delivered before every agent start, with a policy allowing only `issue/{service}` for the services it
  runs. Agent templates use `secret`: OpenBao's docs describe `pkiCert` re-fetching only at startup or after expiry,
  and leased secrets at 85% of their TTL. The setup is run once against an OpenBao dev server to confirm this.
- **Nebula**, for machines spanning several networks:
  - Nebula 1.10+ with v2 certificates on a dedicated IPv4 /16, as a host service outside the Bedrock containers.
  - At least three lighthouses at different providers; Nebula's own relay feature stays off for hosts with public addresses.
  - Host firewall on the public interface: Nebula UDP 4242 and SSH from the operator network only.
  - Nebula groups (`service`, `relay`, `api`, `monitoring`, `ops`) limit ports: relay `listen` ← `service`, `relay`;
    api `listen` ← `relay`; api `write_listen` ← `relay`, `ops`; `admin_listen` ← `monitoring`, `ops`.
  - Host keys are generated on the machine; only the public key is signed (open question 2 covers where the CA key lives).
  - Removing a machine: its fingerprint goes into `pki.blocklist` on every host, with `pki.disconnect_invalid: true`.
    Its TLS certificate then expires within its lifetime because the machine can no longer renew.
- Non-membership examples may leave `[tls]` commented out. Production cell membership examples require TLS and
  an explicit `membership.voter_ids` mapping; simulations retain their plaintext exception.

**Additional context**
- **Implementation:** Foundation (deployment). Documentation and example configuration; no Rust changes.
- **Module:** `docs/deployment/security.md`, `deploy/config/`.
- **Depends on:** B-02, B-55.
- **Acceptance criteria:**
  - The compose + OpenBao path is run end to end once: certificates are issued, a cell starts with TLS, renewal swaps the
    certificate without dropping sessions. Recorded in the guide.
  - The Nebula path is run once on machines in two networks: Bedrock ports answer only on overlay addresses and a
    blocklisted machine loses its tunnels. Recorded in the guide.
  - Steps not run end to end (for example the Kubernetes/cert-manager path) are marked untested in the guide.

---

#### Open questions for #44

1. **Write API callers that are not nodes** (deploy tooling, future Compute). Implemented for now: the write listener
   uses TLS with an optional client certificate, and every caller, relays included, keeps the bearer token until the
   Auth dataset (B-42) exists. Moving relays' membership writes to certificate identity only is a later choice.
2. **Nebula CA key in the reference setup.** OpenBao has no built-in Nebula engine. Options: (a) the community
   `openbao-plugin-secrets-nebula` (MIT, Nebula v2 support, one maintainer; whether the CA key stays inside OpenBao and
   whether it signs a supplied public key must be checked before use), or (b) the CA key in OpenBao KV, signed with
   `nebula-cert sign -in-pub` on an operator machine. Proposal: (b).
3. **Ordering with the hardening gate (B-23).** Proposal: neither blocks the other. B-55 changes the transport, so when it
   lands it reruns the madsim seeds and real-binary crash tests; B-23 does not wait for it.
4. **Lifetimes in the reference setup.** 7-day node certificates and 90-day Nebula certificates. Shorter node certificates
   shrink the revocation window and the tolerated issuer outage at the same time.

---

### N — Shared platform infrastructure (future)

#### B-56 — Shared API rate-limit infrastructure

```
Title:  Feature 🚀: Shared API rate-limit infrastructure across services
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Request-rate limits should use shared platform functionality that services can integrate, rather than separate product
implementations. Customer KV request-rate limiting is deferred until this infrastructure is available; it does not block
initial KV delivery.

**Describe the solution you'd like**
- Provide reusable rate-limit contracts and implementation in `lib`, with service API integration. KV consumes the shared
  functionality later; it does not implement its own rate-limit system in B-25 through B-29 or B-39.
- Agree policy configuration, caller/scope identity, accounting scope and behavior during failures before implementation.
  The algorithm, deployment topology and distributed coordination remain design decisions for this draft.
- Keep customer request-rate policies separate from storage/size quotas and existing overload protection such as bounded
  queues, coalescing, source-capacity limits and backpressure.
- Use low-cardinality metrics for allowed/rejected requests and limiter failures; expose caller/namespace details through
  introspection rather than metric labels.

**Additional context**
- **Implementation:** Future shared infrastructure; draft only, no GitHub issue created.
- **Module:** Shared contracts in `lib` and service API integration; final placement to be agreed.
- **Depends on:** To be agreed before implementation. This draft is not a prerequisite for initial KV acceptance.
- **Acceptance criteria:** The same limiter supports multiple service integrations with isolated scopes and consistent
  decisions/errors. Validate policy enforcement, failure behavior and scope isolation through shared contract tests and
  applicable normal/simulation checks when implemented. KV adoption follows this shared work.

---

#### B-57 — Gateway binary and shared cluster client

```
Title:  Feature 🚀: Gateway binary and shared client for routing between nodes and cells
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Services have no standard way to reach each other. Each binary wires up its own member view and source selection, a node
cannot reach another cell, and public requests have no entry point.

**Describe the solution you'd like**
- #56 / B-25 ships the shared client's first slice: callers fetch `Relay.Routes` from their cell relay on first use, cache
  members and placement directory for that service, and drop the table after 10 idle minutes (`IDLE_TTL`). Reads go directly
  to holders; owner/freshness rejection tries the next owner, then fetches a newer table, with bounded retries. Compute holds
  tables only for services it calls. B-57 adds `bytes()`, Gateway/Compute wiring, tier fallback and customer-KV rendezvous
  (`Sources::readers`).
- `bedrock-gateway`, with its role taken from the tier of the relay it subscribes to (§4 decision 15): a cell tier authenticates
  and routes to nodes, a DC or region tier picks a cell. Separate public and internal listeners with separate limits.
- A `gateway` service identity in `authorize`, including a way to read from upper-tier relays, which accept only `relay` today.
- Reading other services' views: `{service}.{cell}` for each service the Gateway calls. This is untested.
- The list of cells below an upper tier, which relays do not know today.
- The freshness request of decision 15 on the relay session, with its reply's age accumulated at each tier.

**Additional context**
- **Implementation:** Shared infrastructure; draft only, no GitHub issue created.
- **Module:** new `crates/gateway`; the client in `lib`; `lib/relays` and `replication.proto` for the freshness request.
- **Doc reference:** §4 decision 15.
- **Depends on:** B-19, B-22, B-55. The freshness request updates the TLA+ specification first and meets the B-23 checks.
- **Acceptance criteria:** A read inside a cell takes no extra hop. A node reaches another cell with no member list for it. A
  relay cut off from its parent cannot make a Gateway look fresh: simulation shows the Gateway failing closed within 30 seconds
  and recovering afterwards, replayable per seed. Metric labels stay low-cardinality.

---

#### B-61 — Simulation speed: more simulated seconds per real second

Created: [#66](https://github.com/felixrydberg/bedrock/issues/66) — Feature 🚀: Run more simulated seconds per real second

#### B-62 — Partial quarantine: rebuild only the damaged range

```
Title:  Feature 🚀: Rebuild only the damaged range of a quarantined engine
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
One bad record quarantines a whole engine instance, and rebuilding it re-bootstraps every scope of that dataset. Ranges only
split, so that cost grows with a node's range count: `quicksilver-3` held 45 `keys` ranges at the end of a simulated day. While
it rebuilds, the node serves nothing from that dataset and takes no new ranges for it, even though almost all of its data is
good. `kv.md` §15.7 prefers the smallest practical recovery, and a failed L2 range is one of the levels it lists.

**Describe the solution you'd like**
Verify first, then narrow:
1. Damage quarantines the whole instance, as it does today, so nothing is served while the extent is unknown.
2. A verification pass checks the whole engine with its own verifier where it has one, then reads every record by range. It is
   a local read, far cheaper than bootstrapping over the network.
3. The adapter reports the span of storage it must discard. On RocksDB that is the damaged table file's key span, not the hash
   range the failure hit. If it can name no span, the whole instance rebuilds.
4. Every L2 range overlapping that span stays quarantined and the rest serve again.
5. The adapter physically discards the span, and each of those ranges re-bootstraps through its own scope, then validates as a
   bootstrap does today.

The client already recovers one scope alone (`Intake::fail`), and the scrubber's walk already names the part of the engine it
hit. New work:
- Per-range health. `StorageHealth` tracks quarantine per range, and `Monitored` refuses calls by range. Whole-instance stays
  the default state. `Member` names the damaged ranges, and `select`, `confirms` and `handoff` filter by range. The owner stays
  `ready` in the directory, as a rebuilding owner does today, so this needs no directory write and no relay majority. This
  changes the protocol, so `handoff.tla` and `membership.tla` come first, with `Sicken` per point.
- The quarantine marker names its ranges, so a restart comes back with the same ranges refused. A marker that cannot be read as
  ranges means the whole instance.
- `StorageEngine` gains a way to report and discard a span, and an optional whole-engine verifier. `Reinitialise` gets a range
  form that uses them and removes only those ranges' records and revisions.

Falls back to the whole instance when:
- a failure names no key or file, such as a manifest error;
- the adapter cannot discard the span;
- a failure is outside the L2 ranges, such as a revision or a namespace-prefixed record;
- the verification pass fails or is interrupted;
- more ranges are damaged than it is worth rebuilding one at a time.

**RocksDB**
Measured with throwaway probes against RocksDB 11.8 on 2026-11-06:
- `delete_range` does not remove damage. After deleting the range around a bad block, scans are clean, but the next compaction
  reads the block, sets a background error and stops all writes. Reopening does not help, because the next compaction fails the
  same way. Only the block's file leaving the engine fixes it. Whole-instance `restore` therefore replaces both column families.
- `delete_file_in_range` over the damaged file's span works: with 10 table files, one was dropped, the other 9 stayed, and after
  refilling the span a full compaction and later writes succeeded. The corruption error names the file, and `live_files` gives
  its key span. Keys are hash-first, so a file maps to a contiguous part of the hash space.
  RocksDB's header says this leaves level 0 files, so damage in one means a whole-instance rebuild. Compacting it first cannot help, since compaction reads the bad block.
- A scan cannot prove an engine clean: the same probe shows a clean scan over a block that still stops the engine. The proof
  needs `VerifyChecksum`, which reads table files directly. It is in RocksDB's C API but not in the `rocksdb` crate, whose raw
  handle is also private, so it needs an upstream change or a patched crate.

**Open decisions**
- Whether `VerifyChecksum` reads past the block cache, and what it reports with several bad files. It may stop at the first, so
  the pass would repeat after each discard.
- What SurrealKV needs when its adapter lands. Its `skv scrub` sweeps SSTable data blocks, but its README names no library API
  for that and it skips the log and value log.

**Acceptance**
- A record damaged in one range rebuilds that range only, and every other range keeps serving throughout.
- A range under rebuild serves nothing, and a restart during its rebuild does not serve it.
- Each fallback above rebuilds the whole instance.
- On a real RocksDB engine, a damaged table file discarded this way leaves compaction and writes working.
- Deletes stay deleted and no older value replaces a newer one across a range rebuild.
- `SimEngine` learns damage wider than one record, such as neighbouring keys and metadata. Damage is exactly one record today,
  so the simulation would otherwise only ever take the narrow path and overstate the gain.
- Compare rebuild time and time spent unhealthy before and after, using a soak of a seed with the corruption class.

**Describe alternatives you've considered**
- Trust the failing key's range without a verification pass: rejected. A bad RocksDB block can hold many keys, so neighbouring
  ranges may be damaged too.
- Demote the owner to `populating` instead of per-range health: rejected on 2026-11-06. It would reuse `select`, `confirms` and
  the handoff, but a plain demotion breaks `NoEarlyDrop` and `ReadyHasData` in TLC. It passes a small model (two nodes, one start,
  `MaxRev = 4`) only with three new rules: another healthy `ready` owner must remain, no promotion while sick, and the node wipes
  only on proof the demotion is current. It would still need a member signal, a relay majority and a fallback for the last
  holder, and it would count against the moving-range limit.
- Repair engine blocks or pages: rejected by `kv.md` §15.7, since replication must not depend on engine internals.
- Keep rebuilding whole instances and make them faster: helps, but the cost still grows with range count.

**Additional context**
This shortens rebuilds, not detection. Damage is found by the scrubber's walk, which starts over on every restart, so a durable
walk position is separate work (`health::scrub`).

Measured on seed 90003 over two simulated hours, with the seed's heavy fault load: 207 quarantines, and quicksilver nodes
unhealthy for 28–49% of samples. One quicksilver rebuild took about 80 seconds. Seed 90004 reached 60–79%. The simulation's
damage is one record, so these show the cost of whole-instance rebuilds, not what a real disk would produce.

#### B-63 — Operational scenarios: scale out, drain, cancel and rolling restart under load

```
Title:  Feature 🚀: End-to-end operational scenarios: scale out, drain, cancel and rolling restart under load
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Simulation covers membership events on the shared core (join, relay failover, restart within grace, a cut-off node, a killed
node, and a graceful `draining` leave, checked against the writer), and the crash harness covers kills and disk faults on a
fixed cluster. Nothing exercises operating a live cluster: adding nodes, draining a node or cell, cancelling, and restarting
nodes in turn, with real binaries and writes running.

**Describe the solution you'd like**
- **Simulation (madsim)**, extending `tests/sim/tests/membership.rs`, seeded, each seed replaying to the same trace:
  - Held node drain (B-21b): a node stays `draining`, its ranges hand off within the drain rate, every `ready` owner holds the
    committed data of its ranges, and nothing is revoked before the new owners are `ready`.
  - Cancel: deleting the drain record mid-handoff makes the node eligible again and leaves no partial copies.
  - Cell drain: nothing moves across cells for quicksilver and cdn, and the cell gets no new assignments.
  - Scale out, then scale in: joins and rebalancing, then drains below the replica count, which must keep redundancy.
  - Rolling restart of every node, one at a time, with no write lost and no read going backwards.
  - Keep clients reading during range handoffs, drains and rolling restarts. Exercise stale `Relay.Routes` tables, owner
    rejection, bounded next-owner retry and table refresh while reads continue.
- **Real binaries** (`tests/crash`, `scripts/crash.sh`): a dynamic cluster where the harness starts a new node, drains one
  through the operator path and removes it, and does a rolling restart. The existing invariants and disk checks apply.
- Failing seeds go to OPEN, then REGRESSIONS, with a focused test.

**Additional context**
- **Implementation:** Test support only; no product code.
- **Module:** `tests/sim`, `tests/crash`.
- **Depends on:** B-23, B-21b. The drain record is `drain/{id}` in the cell's membership namespace (amendment on #68), so no
  other dataset is needed. Amendment 2026-11-09: authenticated cell-local `CellControl.Write` now provides ordered drain
  writes and cancellation; the operator CLI remains deferred. This draft still owns the proposed real-binary held-drain
  scenario; the existing real-binary drain announces `draining` from the node.
- **Acceptance criteria:**
  - Every scenario above passes for the required seeds and replays to the same trace.
  - A drain leaves the target holding nothing for any dataset, with no read going backwards.
  - Cancelling a drain leaves no partial copies and makes the target eligible again.
  - A drain never takes a range below its replica count.
  - The real-binary run completes a scale-out, a drain and a rolling restart with every invariant intact.

#### B-64 — Appointed writer per namespace group, with a small controller log

```
Title:  Feature 🚀: Appointed writer per namespace group, controlled by a small root log (design and model first)
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
B-09b starts with one Raft group for the whole core region, so one leader orders every namespace. `future-development.md`
proposes several Raft groups, each owning a set of namespaces. Independent election groups pay timers, heartbeats and
elections per group, and moving a namespace between groups needs a single authority to decide it anyway. Enroute v0.4.0
(`docs/internals/consensus.md`, `ledger-and-compute.md`) runs one log per repository without any of that: a small root
Raft log is the controller and appoints each log's sequencer. This draft evaluates that shape against the plain
multi-group design before anyone builds either. It is not needed until B-09b is implemented and hardened.

**Describe the solution you'd like**
Write and model-check the alternative, then decide:
- **Controller.** The B-09b Raft group is the root. It records each writer group's replicas, sequencer and term, and the
  namespace-to-group assignment. Only it changes them.
- **Appointed sequencer.** A group's log has no timers, heartbeats or elections of its own; only its appointed sequencer
  appends. Failure detection is per node, from real traffic (B-13), not per group.
- **Failover.** The controller seals the group at term + 1 on its replicas; a seal raises the replica's fence and reports
  where its log ends. After a majority answered, it appoints one of the most up-to-date answers with a compare-and-swap on the
  root. A seal majority meets every commit majority, so the new sequencer holds every committed entry.
- **Commit rule.** The commit point moves to the highest position a majority holds, only for a position of the
  sequencer's own term. A new term starts with a no-op (Raft's rule against its Figure 8).
- **Reads** that need the latest state check with a majority that the sequencer's term is still open.
- **Fencing.** The B-11 publisher fence follows the appointed term, so a stale sequencer cannot publish an older manifest.
- Output is a TLA+ specification and a written decision against N openraft groups (idle cost, failover time, what
  `openraft` already gives, implementation risk). If appointed sequencers win, the implementation sits behind the write contract
  B-09b defines. If not, record why and leave `future-development.md` as it is.

**Describe alternatives you've considered**
- Keep one group: simplest, and still the starting point.
- N `openraft` groups as in `future-development.md`: reuses a library, but each group elects on its own.

**Additional context**
- **Implementation:** Design draft; no product code until the decision is accepted. Draft only, no GitHub issue created.
  **Deferred:** this stays a draft and is not started until a GitHub issue is created for it. B-09b's single group comes
  first, and B-65's multi-group support waits for this.
- **Module:** `docs/spec/` first; later `lib/replication`.
- **Reference:** Enroute v0.4.0 docs (reference only, no dependency). Namespace handoff between groups stays the separate
  design in `future-development.md`.
- **Depends on:** B-09b, B-11, B-23. A protocol change updates the TLA+ specification first.
- **Acceptance criteria:**
  - The specification checks failover, a stale sequencer, reads and fencing in TLC.
  - Negative checks fail as stated: committing without a seal majority, appointing a replica that is not among the most up
    to date, and counting a previous-term entry as committed each violate an invariant.
  - A written decision exists. Nothing else is built before it.

#### B-65 — Shared durable log store with one group-committed fsync

```
Title:  Feature 🚀: One durable log store per API node with a group-committed fsync for all writer groups
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
B-09b acknowledges a write once a majority has fsynced it, so fsync time is commit time and throughput depends on how
many writes share one fsync. With one log file per group, many busy groups on a node would each sync on their own and
destroy that batching. Enroute keeps one physical log store per node, with many logical logs, so one fsync covers every
group written in a batch.

**Describe the solution you'd like**
- One log store per API node. Each writer group is a logical log under its group id: entries, hard state (term, fence,
  vote, commit point) and where the log starts.
- Concurrent writes from all groups join one batch and one fsync; a write is acknowledged only after the sync that covers it.
- Recovery returns every group to its last durable position; a torn tail is discarded, never an acknowledged entry.
- Truncation and purge per group follow B-09b's rule: only after a verified recoverable native checkpoint covers every affected
  namespace.
- Start with the single group of B-09b (concurrent writes of one group share an fsync). Support several group ids once
  B-64 accepts them. If B-09b's log store already batches that way, this draft reduces to adding the group id.
- Metrics: fsync batch size and fsync latency, with no group or namespace label.

**Additional context**
- **Implementation:** Shared core; draft only, no GitHub issue created.
- **Module:** `lib/replication` log store, with direct filesystem access only in its adapter (B-23a). The simulation
  adapter models buffered versus committed state.
- **Depends on:** B-09b; B-64 for more than one group.
- **Acceptance criteria:**
  - Writes from several groups in one batch cost one fsync, measured.
  - Crash tests kill during a batch and during a torn write; every acknowledged entry survives and each group recovers to a
    consistent position.
  - Deterministic simulation drives buffered-versus-committed loss with replayable seeds.
  - Groups stay isolated: one group's truncation or damage does not alter another's log.

#### B-66 — Safety versus discovery: stale or unreachable inputs may only cost availability

```
Title:  Feature 🚀: Audit and test that stale or unreachable discovery never breaks safety
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
Several parts learn who owns something, or where to send it, from sources that can be stale or unreachable: the cell
membership view and regional directory (published by the relay majority), the relay peer lists in config, S3 for namespace
opens and recent state, the current-leader view that clients and relays hold after a B-09b failover, and the Gateway's auth
copy. The specifications already freeze without a relay majority (`Freeze` in `membership.tla` and `handoff.tla`) and
decision 15 makes the Gateway fail closed within 30 seconds. There is no one statement of which inputs are safety-critical,
and no test that every one failing costs only availability. Enroute states this separation explicitly: its object-store
`cluster/` listing is only discovery, and safety lives in the root log.

**Describe the solution you'd like**
- A table in `docs/spec/`: each discovery input, its source, who uses it, what stale or unreachable can cause, and the
  mechanism that keeps it safe (owner check, fence, term, freeze, fail-closed).
- The rule: a stale or unreachable input may cause a retry, an error or a freeze. It never causes a second writer, a write
  accepted by a non-owner, a read below its minimum revision, or a revoked key accepted past its bound.
- Relay-fetched routing tables are discovery inputs that may cost availability only. A holder's owner check, followed by
  rejection handling and table refresh, preserves the safety rule.
- Simulation scenarios in `tests/sim`, seeded and replayable, each checked against the writer:
  - the regional directory stale on callers during a handoff;
  - S3 unreachable on open and recent-state paths while relays are healthy;
  - the old leader still addressed after a B-09b failover (its writes are rejected and never applied);
  - a relay cut off from its parent while a Gateway depends on it.
- Fix what this finds, each as a focused regression test. A protocol change updates the TLA+ specification first.

**Additional context**
- **Implementation:** Shared core audit plus tests; draft only, no GitHub issue created.
- **Module:** `docs/spec/`, `tests/sim`, `tests/crash`.
- **Depends on:** B-23. The leader scenario needs B-09b and the Gateway scenario needs B-57.
- **Acceptance criteria:**
  - Every input in the table has a mechanism, and a scenario or existing test that makes it fail.
  - No scenario produces a second writer, a non-owner write, a read below its minimum revision or a stale key accepted.
  - Failing seeds go to OPEN, then REGRESSIONS, with a focused test.

#### B-67 — Explicit operator-controlled membership changes for the API Raft group

```
Title:  Feature 🚀: API Raft membership changes only by explicit operator command, with safety checks
Labels: enhancement
```

**Is your feature request related to a problem? Please describe.**
A node that is down looks the same as a node that is gone. Relay-derived membership removes a missing Quicksilver, KV or CDN
node after a grace period, which is safe because their L2 and cache data is never the only copy. A B-09b voter is
different: it may hold committed entries not yet SlateDB-durable (see B-09b's known limits), and removing a partitioned voter
automatically would shrink the quorum. Enroute makes removal an explicit operator act for the same reason.

**Describe the solution you'd like**
- Voters are added, promoted and removed only by explicit commands on the API's authenticated control endpoint. No code path
  changes voters because a node is unreachable or absent.
- A new voter joins as a learner and is promoted only after it has caught up.
- A removal is refused when it would leave fewer voters than the configured minimum, drop below distinct failure domains
  (`node.location.failure_domain`, for example one availability zone per voter), or remove a voter that is still reachable
  and has not been stopped or drained. A `force` flag overrides the last two for a lost node and is logged.
- A removed voter that restarts cannot start an election, serve reads or publish.
- A partitioned voter stays a voter: writes stop without a majority instead of the group shrinking itself.
- The command-line front end is shared with the deferred operator CLI for drains; only the control endpoint is in scope here.

**Describe alternatives you've considered**
- Remove an absent voter after a grace period, as relay membership does: rejected for voters, as above.
- Fixed voters in config only: simpler, but replacing a lost node would need a restart of the whole group.

**Additional context**
- **Implementation:** Shared core; draft only, no GitHub issue created.
- **Module:** `lib/replication` plus the `api` crate.
- **Depends on:** B-09b. Failure-domain checks reuse the placement helpers where they fit.
- **Acceptance criteria:**
  - No membership change happens without a command; simulation shows a long partition changes no voters.
  - Each refusal above is tested, and `force` is logged.
  - A learner is never promoted before it has caught up, and a removed voter cannot disturb the group after it restarts.
  - A removal while the leader crashes leaves exactly one consistent membership, replayable per seed.

---

### O — Stateful actor platform

Decision 16. The drafts' scope, dependencies and acceptance live in [`statefull/v2.md`](statefull/v2.md): §4–§8 for the
design, §11 for the drafts S-01 … S-19 and their acceptance highlights, §12 for open questions. When a draft becomes an
issue, its body is written from those sections in the usual template, and its entry here becomes a `Created:` link.

- **Storage first.** S-01 … S-06 build actor storage. S-02 updates the specification before any protocol code, and
  S-03 waits for it.
- **Hosting.** S-07 … S-09 host actors in `bedrock-compute`, route to them through the Gateway (needs B-57) and fire
  alarms.
- **Products.** S-10 … S-15 and S-18 wait for the #25 / B-23 gate like the other product tracks. S-10 also waits for
  the sandbox runtime decision (leaning towards sandboxed workerd).
- **Hardening and operations.** S-16 adds the actor workload to the simulation and crash tests; S-17 covers planned DC
  changes and DC-loss restore.
- **Later.** S-19 (load-based splitting) starts only once measurements show hot volume nodes with similar range sizes.

---

## 4. Open decisions

### Decided

1. **Separate binaries.** `bedrock-quicksilver` (Quicksilver, Auth), `bedrock-kv` (customer KV), `bedrock-cdn` (`cdn` retention
   metadata and artifacts), `bedrock-relay` (relay tree and membership) and `bedrock-api` (writes), all linking `lib` and never
   each other. Quicksilver, KV and CDN run as separate processes on every machine; relays run on a few machines per cell.
   See §1 "Deployment model" and decision 12.
2. **Storage contracts and adapters live in `lib/storage`.** The synchronous local `StorageEngine` remains for pushed
   datasets, relay replay and CDN metadata. B-11 adds asynchronous S3 materialized storage for generic API writers across
   datasets; B-39 adds read-only handles. `kv/storage` owns the value cache; `volume/cold` owns S3 reader handles for
   actor databases (decision 16); `quicksilver/storage` owns the pushed L1/L2 keyspaces; CDN retains `CacheStorage`.
3. **KV and CDN share infrastructure, not datasets.** CDN path rows and ref keys are plain writes to the `cdn` dataset through the
   generic `bedrock-api` and `bedrock-relay`; only `bedrock-cdn` reads them (B-32, decision 14). `bedrock-quicksilver` and `bedrock-kv` never
   subscribe to `cdn`, and `bedrock-cdn` never subscribes to `quicksilver`, `kv` or `auth`. Customer `kv` has no relay delivery at all;
   Quicksilver carries its directory metadata. The CDN serves path rows by key and artifacts by content hash (decision 14). Shared writers/storage remain generic
   and never import another binary's product code.
4. **No heartbeat or gossip protocol.** Membership comes from the subscriber streams nodes already hold to their cell relays (B-12):
   relays merge their connected lists and publish them as a replicated namespace, with a grace period on disconnect.
   A member is a node that is receiving updates. Routing decisions use a local per-peer circuit breaker fed by real traffic (B-13).
   There is no separate membership or placement epoch; owners reject requests for L2 ranges they don't own (B-16, decision 13).
   Customer-KV hash routing only selects a preferred cache; another eligible cache node can serve without owning persistent
   data. It hashes `(namespace, key)` directly and uses no ranges or buckets (decisions 12, 13).
   Amended 2026-11-14 (decision 16): actor hosts send lease heartbeats to the volume nodes of the actors they hold. They
   are for actors only; membership is unchanged.
5. **Cells.** A cell (one DC by default) has its own membership, placement directory, L2 and relays. No fixed bucket count
   caps its size (decision 13); the actual size is chosen for blast radius. Large sites run several cells.
   Amended 2026-11-09: the cell's relays own authoritative membership, placement and drain state. Publishing, reading,
   bootstrapping and recovering this control state must not require the central write API, upstream relays, Quicksilver,
   S3 or another service. Acknowledged control writes must survive relay process restarts and publisher failover through
   durable replication within the cell. A healthy cell-relay quorum can change membership and placement and accept or
   cancel drains while every external service is unreachable. Without that quorum, new control writes fail; nodes keep
   serving their last committed view. Already committed changes may still reach lagging nodes. Membership observations
   continue to come from existing subscriber streams, and there is no new membership or placement epoch. Handoffs
   still require verified range data and the existing readiness and revoke checks. The existing cell relays embed
   a Raft quorum, using a separate durable local control store; no separate service is deployed. Internal Raft terms
   fence leadership and are not membership or placement epochs. The protocol and implementation are described in
   `docs/spec/membership.md` and `cell.tla`. This supersedes decision 13's API-backed publication path and B-21b's allowance for API drain
   writes without a cell-relay quorum.
   A cell behaves as an independent cluster. Its identity and local membership, placement, readiness and maintenance
   decisions do not depend on the identities, number or arrangement of external relays and gateways between it and
   the Writer. The cell boundary needs configured immediate upstream endpoints; intermediate routing and failover
   belong to the transport layer. Replacing an external path must not reconfigure the cell's Raft group or reset its
   directory, drain intent or applied product-data revisions. External gateways route requests to the cell; they do
   not participate in its control quorum or decide its internal placement.
   Product-data writes and upstream replication retain their existing contracts. Loss of every external path can
   delay new product data and leave reads unable to meet a requested revision or freshness bound; it must not stop
   local cluster management. Recovery through another path resumes from validated revisions, preserving the
   Writer's order and original commit times. Cell consensus cannot substitute for missing product data or proof
   of Writer freshness.
   Customer-KV cache misses go to the namespace's `kv` actor, which also answers minimum-revision reads (decision 16).
   There is no mandatory regional KV tier or origin cell. Moving client traffic between cells is out of scope for now.
   Amended 2026-11-14 (decision 16): an actor's three volume slots span the cells of one DC. Each cell still decides its
   own placement; only the data plane crosses cells, and each slot's owner comes from its own cell's directory.
6. **Relay tree.** Relays form tiers in static config (region top → optional DC → cell). The downstream dials and the upstream
   pushes; gaps are pulled from the replay log. Fan-out for pushed datasets is routed by `(namespace, hash range)`.
   `quicksilver`, `auth` and `cdn` continue using this path; customer-KV values/revisions are not published through it.
7. **KV and CDN are backing services.** Nothing in their tracks knows about Workers, Pages or deployments. That lives in
   track L (Compute and deploy control plane), which uses KV and the CDN as clients and owns the publish/activate/rollback order.
8. **Observability policy.**
   - **Cardinality:** metrics are labelled only by low-cardinality dimensions (dataset, tier, cell, service, node). Anything per
     namespace, key, hash or subscriber goes through an introspection API, or a top-N metric.
   - **End-to-end lag:** every event carries `committed_at`, set once by its authoritative writer (`bedrock-api` for product data,
     the cell Raft leader for control state). Every hop reports `now - committed_at`
     as a histogram, so write-to-anywhere latency needs no tracing infrastructure.
   - **Every draft names its metrics** in its solution or acceptance criteria, on top of the B-03 baseline (logging, metrics endpoint, health).
   - Relays expose per-namespace state through the B-18b introspection API.
9. **`lib/` holds `storage`, `datasets`, `s3` and `transport`** alongside the seven documented modules, since every binary
   needs them. `code-structure.md` reflects this, including `storage` moving out of `kv/` (B-01).
10. **Crate naming.** The shared crate is the package `bedrock-lib` (`use bedrock_lib::…`) in `crates/lib`; the binary
    crates are `bedrock-quicksilver`, `bedrock-kv`, `bedrock-cdn`, `bedrock-relay` and `bedrock-api` (B-01, decision 12).
11. **Customer KV is per-namespace S3 storage behind one writer, with read-only caches.** Decided 2026-10-29; replaces
    the 2026-10-26 origin-cell design. Amended 2026-11-14 by decision 16: the writer is the namespace's `kv` actor, not
    `bedrock-api` or a B-26 owner node, and the home-region S3 readers are replaced by the holder. What still holds:
    - **Persistent layout:** each namespace has its own S3 database `kv/{id}` (SlateDB through B-11's namespace contract).
      Empty namespaces need only Quicksilver directory metadata. No LSM per virtual bucket and no whole-namespace local
      disk requirement.
    - **One writer per namespace,** which orders mutations with per-namespace revisions and keeps retry results and
      events. One logical writer remains a write-throughput limit even when caches scale out.
    - **Reads:** optional local L1 → the cell value cache, chosen by HRW over `(namespace, key)` with no ranges or buckets
      (decisions 12, 13) → on a miss, the writer. The cache keeps a value for at most 60 seconds; the TTL bounds cache
      residence, not freshness. A minimum-revision read, including one for an absence, needs a source that can prove that
      revision; a cache must not satisfy one it cannot prove. `LIST` runs against the namespace's full ordered view with a
      logical cursor over original keys, never a scatter over cache holders.
    - **Local residency:** KV nodes bound value-cache bytes and in-flight fetches, with a per-namespace share. Closing a
      cache or handle never deletes S3 data or expires keys. No global dormant state or eviction broadcasts.
    - **Lifecycle:** TTL and namespace deletion are explicit data semantics. Expiration is enforced on read; compaction
      and GC reclaim obsolete data. Deleting a namespace purges its S3 database after a grace period (B-39).
    - **Push remains separate:** customer KV has no relay delivery and no `global`, `full_replica` or `on_demand` push
      override. B-37 stays dropped. Quicksilver, Auth and CDN keep their relay policies.
12. **Customer KV is its own binary and uses no virtual buckets.** Decided 2026-10-31.
    - **Split:** `bedrock-quicksilver` (`crates/quicksilver`) hosts the pushed `quicksilver` and `auth` datasets with today's
      L1/L2 hash-range keyspaces (decision 13), relay subscriptions and `full_replica` work (B-24, B-40, B-42). `bedrock-kv` (`crates/kv`) is
      the customer-KV read side: value caches, the REST API and namespace management (B-25, B-39); its writers are `kv`
      actors in `bedrock-compute` (decision 16). Each publishes its own membership
      (`quicksilver`, `kv`); `bedrock-kv` subscribes to no dataset.
    - **Why:** Quicksilver/Auth are the control plane that Workers, CDN and deploy tooling depend on; customer KV is tenant
      traffic. A hot namespace, cold-open storm or slow S3 must not exhaust the process serving Quicksilver/Auth. The two
      also share no storage engine, replication path or scaling signal.
    - **Routing:** a customer-KV read picks its preferred cache node by rendezvous hash of `(namespace, key)` over the
      healthy `bedrock-kv` members of the cell (B-15), without a placement directory, owner checks or range moves. The
      relay-assigned ranges of decision 13 (B-16) serve Quicksilver/Auth and CDN.
    - **Cost:** a fifth binary, one more relay stream per machine (membership only), and cell-local lookups from
      `bedrock-kv` to `bedrock-quicksilver` for the namespace directory and credentials, cached with minimum-revision checks.
13. **L2 placement: relays assign hash-space ranges; nodes subscribe to the directory.** Decided 2026-10-31.
    Replaces the fixed 2²⁰ virtual buckets and per-node rendezvous ownership (B-15, B-16) for the pushed `quicksilver` and
    `auth` datasets and the CDN. Customer-KV cache affinity hashes `(namespace, key)` with rendezvous directly and uses no
    ranges (decision 11's readers own no persistent data). Chosen over a hash ring and over simplified buckets.
    - **Key space:** an L2 key maps to a 64-bit hash of `(dataset, namespace, key)`; CDN `ref/{sha256}/…` keys use the
      artifact's content hash instead (decision 14).
      Ownership is assigned in contiguous ranges of that hash space, not raw key order, so sequential keys do not concentrate.
      L2 storage keys start with the big-endian hash: a range is one contiguous scan, and dropping it is one `delete_range`
      followed by compaction of that span (RocksDB reclaims range tombstones only on compaction). No bucket count caps a cell.
    - **Directory:** the cell relays' membership publisher (B-12) also writes the placement directory into the same
      `membership/{service}.{cell}` namespace, under `range/{start}` keys (node IDs cannot contain `/`). Membership,
      assignment and readiness therefore share one revision order. An entry holds the range bounds and its owners, each
      `populating`, `ready` or `leaving`. Every node subscribes to it, as it already does to membership; it stays small
      (a few ranges per node). `Relay.Routes` looks up this already-published state; keys and bytes never pass through
      relays. Nodes route locally from a table, and relays are never on the read path.
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
      without a majority. Amendment 2026-11-09: embedded cell Raft serializes directory changes even with asymmetric relay
      partitions; member observations can still differ during failover. Relay-pushed interests
      and revokes change `Relay.Session`. Estimated 2026-10-31 at about 10–14 weeks for one developer including
      specification and simulation. Load-based splitting is S-19, re-estimated 2026-11-14 at 2–3 weeks now that
      splitting and merging exist.
    - **Specification first:** `membership.tla` gains the directory writes and a single assigner; a handoff model checks that
      a range is never dropped before its new owners are ready and that no node serves a range it neither owns nor is leaving;
      `relays.tla` covers relay-pushed interests and revokes. Update them before implementation.
    - **Refined 2026-11-02 for #22 / B-21:**
      - *Ranges per dataset.* Each dataset has its own ranges under `range/{dataset}/{start}`, given only to members whose
        `datasets` include it. A node carrying `auth` but not `quicksilver` owns only `auth` ranges.
      - *One stream per dataset and range.* A node follows `(dataset, range)`, and that stream carries every namespace of the
        dataset. Events keep their namespace and per-namespace `sequence`; the node keeps one applied sequence per namespace,
        and deliveries note coverage only for namespaces that changed, so an idle namespace costs one number per stream. A
        subscription selects either a namespace (membership) or a dataset range (L2), never both.
      - *Fixed namespaces.* Decided 2026-11-03. Each ranged dataset's namespaces are fixed in code (`placement::namespaces`):
        `quicksilver` has `infrastructure`, `auth` has `keys`, `cdn` has `pages` and `workers`. The writer's scope validation is
        the one place that rejects any other namespace of these datasets. Relays count a node ready only once it holds every
        listed namespace, so a namespace a relay has not seen a write for yet cannot be skipped. Adding a namespace means
        shipping new code.
      - *One pushed frame.* A relay sends each leaf its full owned range set per dataset, tagged with the directory revision it
        was derived from. Ranges it adds are followed; ranges it drops are the revoke. The node confirms a revoke only once its
        own view has reached that revision and shows it without the range while another owner is `ready`; otherwise it
        refuses and reports. A relay drops a range from the set only after the published directory removed the node.
      - *Readiness.* The holding relay derives it from the confirmed bootstrap (or a stream from revision 0) plus
        acknowledgements through the head it saw at confirmation, counting only acknowledgements sent after the node applied
        the frame that assigned the range. It shares a ready bit, not acknowledgements, over the sibling stream; the publisher
        writes `ready` and then removes `leaving` owners once every other owner is `ready`.
      - *Serving.* A node serves only ranges it holds as `ready` or `leaving` in its own view; `populating` owners do not serve.
      - *Storage.* L2 data uses the hash-first layout, so a range is one scan across namespaces and one `delete_range`. Startup
        rejects the earlier alpha layout instead of migrating it.
      - *Deferred.* Dropping a `leaving` range early under disk pressure waits for the storage-health work (B-14).
      - *Specification.* The handoff model is its own module, `docs/spec/handoff.tla`, over an abstract membership:
        combined with `membership.tla`'s relay and clock state it was too large for TLC.
      - *CDN.* The `cdn` dataset is ranged like `quicksilver` and `auth`: CDN members carry `cdn` in their `datasets`, own
        `range/cdn/{start}` entries and follow the ranges their relay pushes. This replaces the earlier `artifacts` label
        and the whole-range follow of `cdn` (decision 14).
14. **CDN: path rows and ref keys instead of manifests.** Decided 2026-11-02.
    Replaces the manifest file, the `RETAIN_*`/`RELEASE_*` operations and the manifest reference counts in B-32, B-33 and B-35.
    - **Records** in the `cdn` dataset. The namespace is the caller's retention namespace (`workers`, `pages`), opaque to the CDN.
      - `path/{deployment_id}/{path}` → `{sha256, content type, size}`: which bytes a URL path serves. Pages only.
      - `ref/{sha256}/{deployment_id}` → empty: keeps the artifact. An artifact is retained while any `ref/{sha256}/…` key exists
        in any namespace; two deployments sharing a file write two ref keys and hold one copy.
    - **Deployment IDs** are immutable UUIDs. A project name resolves to a UUID in Quicksilver, never in a CDN key, so rows never
      change and every cache keeps them without invalidation. To the CDN the ID is an opaque string.
    - **Placement:** `ref/` keys by the first 8 bytes of their sha256, ignoring the namespace, so an artifact's holders receive
      exactly its ref keys and `workers` and `pages` share one holder set. Path rows use the normal `(dataset, namespace, key)` hash.
    - **Writes:** a publish uploads the bytes to S3 by sha256, then writes every path row and ref key of the deployment in bulk
      (B-09c): one revision for all of them. A deployment larger than one bulk is split; the caller records the last bulk's
      revision. Release starts only after the control plane removed its references to the deployment, then deletes the same
      keys in bulks under the cap, retrying any that fail. Rerunning either converges.
    - **Reads (Gateway, B-51):** host or project → `deployment_id` → `deployment/{id}` in Quicksilver says Worker or Pages. A Worker
      goes to compute, and its record holds the bundle's sha256. Pages reads `path/{id}/{path}` at the recorded minimum revision,
      then the artifact from its sha256's holders. Both are point reads of immutable data, so a warm Gateway does neither.
    - **No derived state.** Holders keep path rows and ref keys as ordinary L2 range data, rebuilt by range bootstrap (B-21).
      Ref keys arriving trigger prewarming (B-34); the last ref key of an artifact leaving makes it a GC candidate (B-44).
    - **Storage tiers.** L2 owners keep path rows and ref keys on disk and never evict them; they hold artifact bytes until idle
      (B-44). L1 is an unreplicated memory cache of rows and of bytes by sha256 (B-46). S3 holds the bytes authoritatively; the
      replication log and B-11 state hold the rows.
    - **Tenant isolation.** A publish never skips an upload because another tenant already stored the same sha256 (that would
      reveal the file exists). Uploads are idempotent by sha256; L2 deduplicates across tenants.
    - **Why:** a manifest file has to be fetched and parsed by every node that expands it (with every CDN node following all of
      `cdn`, about 1 GB per cell for a 10,000-file publish on 1,000 nodes) or on the request path. Per-file rows make every lookup
      a point read and send each record only to its 2 owners per cell.
    - **Cost:** two keys per file in the log, and bulk writes become a shared-core prerequisite (B-09c).
15. **Gateway, routing between nodes and cells, and API-key checks.** Decided 2026-11-04. Replaces decision 11's regional value
    cache with a per-cell one, moves `auth` out of `bedrock-quicksilver` (decisions 1, 6, 12 and 13 still describe it there) and
    replaces B-42's `on_demand` policy.
    - **One Gateway binary.** `bedrock-gateway` (B-57) is both the public entry point and the internal routing layer. It runs on
      its own machines, not beside relays, so an attack from the internet never lands on the hosts that replicate data, and it
      subscribes to a relay like any other process. Its role follows that relay's tier: at the cell tier it authenticates and
      routes to nodes; at a DC or region tier it picks a cell, with no authentication and no Quicksilver. Public and internal
      (mTLS) traffic use separate listeners with separate limits, so a public flood cannot starve internal reads.
    - **Reads inside a cell go direct.** The caller hashes the key against routing tables fetched from its cell relay on
      first use, then connects directly to a node. The shared client in `lib` caches each service table and drops it after
      10 idle minutes. Compute holds tables only for services it calls. Owner rejection and refresh protect correctness;
      a stale/unavailable table can only cost availability. One shared client hides tables, hashing, connections, breakers
      and tier fallback, so a service only says what it wants.
    - **Reads between cells go through a Gateway.** A node needs its own cell's list and the target's Gateway address, not
      another cell's members.
    - **Public entry.** Anycast reaches a DC; a connection-level layer that never sees keys spreads connections across Gateways.
      Off-the-shelf ECMP or L4 balancing comes first. It is not designed yet (Still open).
    - **Caches and coalescing.** Customer-KV value caches are per cell, matching decision 12's routing. A read never searches
      other cells. Coalescing stays on the serving node and S3 and open limits stay on the reader (B-22b), because that is where
      a key's requests meet and where a miss is known. An upper-tier Gateway only sees misses, so it may also coalesce across
      cells, and one hash-picked DC or region stop may become an extra `SourceChain` tier. Neither is required (decision 5).
    - **Authentication.** Cell-tier Gateways check API keys and pass the tenant with the request. `bedrock-kv` accepts reads only
      from Gateway and Compute identities and still checks namespace deletion. Compute's host runtime, outside the Worker
      sandbox, checks bindings, and a Worker's KV reads skip the Gateway.
    - **Auth data.** Every cell-tier Gateway follows the whole `auth/keys` namespace into local durable storage and is not ready
      until it has loaded and caught up. A revocation never waits for confirmation from every Gateway. Quicksilver reads go
      straight to Quicksilver nodes, never through another Gateway. If anything else must check keys, revisit where `auth` lives.
    - **Freshness.** A Gateway accepts keys for at most 30 seconds without proof that its copy is current, then fails closed. The
      writer stays event driven, with no timed writes. A Gateway pulls the latest `auth/keys` sequence from its relay, and each
      relay asks upward to the writer's live head, because a relay's own head cannot reveal a stopped parent. Relays merge and
      briefly reuse answers, and each answer carries its age. The Gateway accepts while it has applied that sequence and the
      confirmation is under 30 seconds old. Starting values: a 5-second poll and up to 5 seconds of reuse per tier. A head that
      moves while the Gateway's stream does not also sends it to a sibling relay.
    - **Amendment 2026-11-11: discover routing tables on demand.** Callers hash locally against service routing tables fetched
      from their cell relay on first use, then drop each table after 10 idle minutes. They do not subscribe to every service
      they call; Compute holds tables only for services it uses. Gateway entry routing for Worker and Pages traffic remains
      as described above.
    - **Cost:** a writer outage over 30 seconds rejects API-key requests everywhere, until B-09b replicates the writer; losing
      every route through the relays does the same. Public Pages and Worker traffic needs no API key. The freshness request is a
      relay protocol change, so the TLA+ specification comes first, and like `Tick` it can never advance applied state.
    - **Amendment 2026-11-11 (#56 / B-25): routing tables on demand.** A caller does not subscribe to the member lists of the
      services it calls. The shared client (`lib/client`, the first slice of B-57's client) asks a cell relay for a service's
      published membership namespace (`Relay.Routes`: members and placement directories at one revision), caches it, and
      reads from the holder directly. A holder that does not serve the range or is behind turns the read away and the
      client tries the next owner, then a newer table, a bounded number of times. A table unused for 10 minutes is dropped.
      Relays answer only table fetches, never reads, so decision 13 still holds. `Routes` and the owners' `Reads` service are
      read-only and change no replication state; no TLA+ change. B-25 wires Quicksilver `Get`/`List`; `bytes()`, customer-KV
      rendezvous and Gateway/Compute use stay with S-18 and B-57.
    - **Amendment 2026-11-14 (decision 16): sibling cells in one DC.** Compute and volume nodes may fetch the `volume.{cell}`
      routing tables of sibling cells in the same DC through `Relay.Routes`, for direct quorum traffic. Going through a
      Gateway would add a hop to every commit. Reads and requests between cells otherwise still go through a Gateway.
16. **Stateful actors on `bedrock-compute` and `bedrock-volume`.** Decided 2026-11-13; KV part decided 2026-11-14. The
    plan is [`statefull/v2.md`](statefull/v2.md), implemented by track O (S-01 … S-19).
    - **Actors** are the single primitive for Durable Objects, D1, Queues, Cron, Workflows and customer-KV writers. An
      actor ID is `{region}_{128 bits}`. The DC, cells and wake host are computed from it, so no directory lookup is needed.
    - **Storage:** `bedrock-volume` keeps each actor's claim, quorum log and key/value overlay on three slots in different
      cells of one DC. A commit is acknowledged at 2 of 3 fsyncs; S3 is never on the write path. A maintainer flushes to
      the actor's SlateDB database every second or 4 MiB. Epochs fence writers, and a host lease protects in-memory state.
    - **Hosting:** `bedrock-compute` holds awake actors, sleeps idle ones and wakes them on request or alarm. Gateways
      route region → DC → cell → host with a location cache.
    - **Failure model:** losing a cell loses no acknowledged write. Losing a whole DC means downtime for its actors, and
      losing it for good can lose up to about 1 second of acknowledged writes.
    - **Changes to earlier decisions:** 4 (lease heartbeats for actors), 5 (slots span a DC's cells), 11 (the writer is a
      `kv` actor; PR #74 closed, #58 amended), 12 (`bedrock-kv` is the read side), 15 (sibling-cell routing tables), plus
      B-21 (node-reported readiness for `volume` ranges, a protocol change) and B-50 (cron schedules on activation).

### Still open

1. **Gateway entry and placement (B-57).** The DNS, anycast and L4 layer in front of Gateways, how traffic moves between cells,
   and how a Worker is assigned to a Compute node. Decision 15 assumes these and does not design them.
2. **SlateDB fork and patch set (B-11, S-05).** Pin the source/format baseline and upstream-update policy; prove
   Bedrock's I/O, simulation, range-integrity, checkpoint and recovery contracts before selecting engine features to
   remove. Which of Rivet's vendored SlateDB patches to carry is not decided.
3. **Stateful actor questions.** Sandbox runtime, per-actor SlateDB cost, page or chunk granularity, DC choice, a KV
   durability option, large KV values and heartbeat volume: see `statefull/v2.md` §12. An unavailable source is an
   error, never proof that a key is missing.

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

### CDN placement amendment recorded in comments — 2026-11-02

§4 decision 14 (path rows and ref keys instead of manifests; the CDN follows pushed `cdn` ranges) was posted to the one
affected open issue and read back. B-32 to B-36, B-44 to B-46, B-50, B-51 and the new B-09c are still drafts and were edited here.

| Issue / plan ID | Amendment comment |
|---|---|
| #22 / B-21 | [CDN follows pushed `cdn` ranges](https://github.com/felixrydberg/bedrock/issues/22#issuecomment-6588674643) |
