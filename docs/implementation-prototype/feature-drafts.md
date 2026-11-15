# Bedrock — Feature Implementation Drafts

Draft GitHub issues for building Bedrock, in the order they should be done. Every draft uses
`.github/ISSUE_TEMPLATE/feature_request.md` (title prefix `Feature 🚀:`, label `enhancement`).
The **Additional context** section of each draft says which implementation it belongs to,
which module it lives in, which doc section it comes from, what it depends on, and when it counts as done.

Sources: `docs/implementation-prototype/kv.md`, `cdn.md`, `code-structure.md`, `notes.md`.

> Status: every draft is a GitHub issue (linked below; the issue is the source of truth). B-26, B-27 and B-28
> (#58–#60) and B-53 are replaced by track O; B-37 is dropped; B-54 and B-55 shipped in #44. Track O is #109–#127,
> the remaining B-drafts #75–#108, and B-68 is #70 (created 2026-11-14 unless noted).

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
Track O reuses it for one S3 database per `volume` range, shared by many actors (amended 2026-11-14): the range's
maintainer flushes to it and other slots read it through B-39's `Reader`.
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
| `bedrock-volume` (S-03; cold tier S-05) | Actor storage: claims, leases, quorum log, overlay, flushes to S3, alarm index; `volume` hash ranges per cell (§4 decision 16) | — |

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
| [B-09b](https://github.com/felixrydberg/bedrock/issues/75) | Raft write quorum for the API (later) | Shared core | `lib/replication` + `api` bin | B-09, B-10, B-11, B-13 |
| [B-09c](https://github.com/felixrydberg/bedrock/issues/76) | Bulk writes: many keys of one namespace in one revision | Shared core | `lib/replication`, `lib/relays` + `api` bin | B-11, B-23 |
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
| [B-58](https://github.com/felixrydberg/bedrock/issues/77) | Simulation load: KV reads, listing and namespaces | KV | `tests/sim` | B-23, B-29 |
| **I — First CDN** (parallel with H; no dependency on KV drafts) |||||
| [B-30](https://github.com/felixrydberg/bedrock/issues/78) | Artifact storage: S3 content addressing, `CacheStorage`, Foyer | CDN | `cdn/artifact_storage` | B-23 |
| [B-31](https://github.com/felixrydberg/bedrock/issues/79) | Artifact transfer | CDN | `cdn/artifact_transfer` | B-30 |
| [B-32](https://github.com/felixrydberg/bedrock/issues/80) | `cdn` path rows and ref keys (§4 decision 14) | CDN | `cdn/retention`, `cdn/paths` | B-23, B-30, B-09c |
| [B-33](https://github.com/felixrydberg/bedrock/issues/81) | Publish client writing path rows and ref keys in bulk | CDN | `cdn/paths` | B-30, B-32 |
| [B-34](https://github.com/felixrydberg/bedrock/issues/82) | Regional L2 with two-replica artifact placement | CDN | `cdn/retention`, `cdn/artifact_storage` | B-31, B-32 |
| [B-35](https://github.com/felixrydberg/bedrock/issues/83) | Artifact retrieval interface | CDN | `cdn/artifact_transfer` | B-33, B-34 |
| [B-36](https://github.com/felixrydberg/bedrock/issues/84) | CDN end-to-end acceptance | CDN | `cdn` | B-30 … B-35 |
| [B-59](https://github.com/felixrydberg/bedrock/issues/85) | Simulation load: publishing and artifact retrieval | CDN | `tests/sim` | B-23, B-36 |
| **J — KV refinements** |||||
| ~~B-37~~ | ~~Cache-fill broadcast queue~~ (dropped, decision 11) | — | — | — |
| [B-38](https://github.com/felixrydberg/bedrock/issues/86) | Full cache lifecycle and eviction | KV | `kv/storage` | B-29 |
| [B-40](https://github.com/felixrydberg/bedrock/issues/87) | `full_replica` placement and completeness | KV | `quicksilver/storage`, `lib/buckets` | B-24, B-21 |
| [B-41](https://github.com/felixrydberg/bedrock/issues/88) | Adaptive cache promotion and regional load balancing | KV | `kv/operations`, `lib/relays` | B-38 |
| [B-42](https://github.com/felixrydberg/bedrock/issues/90) | Auth dataset | Shared infrastructure | `gateway` | B-57 |
| ~~B-53~~ | ~~Namespace residency fairness and cold-open tuning~~ (replaced by S-05's per-actor database measurements) | — | — | — |
| **K — CDN refinements** (parallel with J) |||||
| [B-43](https://github.com/felixrydberg/bedrock/issues/91) | Artifact replica repair and range migration | CDN | `cdn/retention` | B-36 |
| [B-44](https://github.com/felixrydberg/bedrock/issues/92) | Coordinated eviction, GC and failure recovery | CDN | `cdn/retention` | B-43 |
| [B-45](https://github.com/felixrydberg/bedrock/issues/93) | Prewarming and artifact-set readiness | CDN | `cdn/retention` | B-36 |
| [B-46](https://github.com/felixrydberg/bedrock/issues/94) | Optional unreplicated L1 and popularity-based retention | CDN | `cdn/artifact_storage`, `cdn/retention` | B-44 |
| [B-47](https://github.com/felixrydberg/bedrock/issues/95) | Filesystem `CacheStorage` adapter | CDN | `cdn/artifact_storage` | B-36 |
| [B-49](https://github.com/felixrydberg/bedrock/issues/96) | Large-object and range transfers | CDN | `cdn/artifact_transfer` | B-36 |
| **L — Compute and deploy control plane (future)** |||||
| [B-50](https://github.com/felixrydberg/bedrock/issues/97) | Deploy control plane: publish, activate, roll back, delete | Compute | not decided | B-24, B-33, B-45 |
| [B-51](https://github.com/felixrydberg/bedrock/issues/98) | Gateway: Worker and Pages request routing | Compute | `gateway` | B-35, B-50, B-57 |
| [B-48](https://github.com/felixrydberg/bedrock/issues/99) | Compiled Worker artifact caching | Compute | not decided | B-51 |
| [B-52](https://github.com/felixrydberg/bedrock/issues/100) | Deploy end-to-end acceptance | Compute | not decided | B-29, B-36, B-50, B-51 |
| [B-60](https://github.com/felixrydberg/bedrock/issues/101) | Simulation load: Workers, Pages and deploys | Compute | `tests/sim` | B-23, B-52 |
| **M — Node security** (parent [#44](https://github.com/felixrydberg/bedrock/issues/44); scope amended 2026-11-03, drafts not yet issues) |||||
| [B-55](https://github.com/felixrydberg/bedrock/issues/44) | Mutual TLS and caller identity for node-to-node traffic, enabled by config | Shared core | `lib/transport` + every bin | B-03, B-12, B-17 |
| [B-54](https://github.com/felixrydberg/bedrock/issues/44) | Deployment guide: private networking and node certificates (Kubernetes, Docker, bare metal) | Foundation (docs) | `docs/deployment/` + `deploy/` | B-02, B-55 |
| **N — Shared platform infrastructure (future)** |||||
| [B-56](https://github.com/felixrydberg/bedrock/issues/102) | Shared API rate-limit infrastructure | Shared infrastructure | shared contracts in `lib`; API integration | To be agreed; does not gate initial KV |
| [B-57](https://github.com/felixrydberg/bedrock/issues/89) | Gateway binary and shared cluster client | Shared infrastructure | `gateway`, `lib` client, `lib/relays` | B-19, B-22, B-55 |
| [B-61](https://github.com/felixrydberg/bedrock/issues/66) | Simulation speed: more simulated seconds per real second | Shared infrastructure | `tests/sim`, `scripts/vopr.sh` | B-23 |
| [B-62](https://github.com/felixrydberg/bedrock/issues/103) | Partial quarantine: rebuild only the damaged range | Shared core | `lib/health` + `lib/reconciliation` + `lib/relays` | B-14, B-21, B-22, B-23 |
| [B-63](https://github.com/felixrydberg/bedrock/issues/104) | Operational scenarios: scale out, drain, cancel and rolling restart under load | Shared core | `tests/sim`, `tests/crash` | B-23, B-21b |
| [B-64](https://github.com/felixrydberg/bedrock/issues/105) | Appointed writer per namespace group, with a small controller log (design and model first) | Shared core | `docs/spec/`, later `lib/replication` | B-09b, B-11, B-23 |
| [B-65](https://github.com/felixrydberg/bedrock/issues/106) | Shared durable log store with one group-committed fsync for every writer group on a node | Shared core | `lib/replication` log store | B-09b; B-64 for more than one group |
| [B-66](https://github.com/felixrydberg/bedrock/issues/107) | Safety versus discovery: stale or unreachable discovery inputs may only cost availability | Shared core | `docs/spec/`, `tests/sim`, `tests/crash` | B-23; B-09b and B-57 for their parts |
| [B-67](https://github.com/felixrydberg/bedrock/issues/108) | Explicit operator-controlled membership changes for the API Raft group | Shared core | `lib/replication` + `api` bin | B-09b |
| [B-68](https://github.com/felixrydberg/bedrock/issues/70) | Move relay-only placement and membership publisher code from `crates/lib` into `crates/relay` | Shared core | `lib` → `relay` bin | B-21b |
| **O — Stateful actors** (decision 16; scope and acceptance in [`statefull/v2.md`](statefull/v2.md) §11) |||||
| [S-01](https://github.com/felixrydberg/bedrock/issues/109) | Actor IDs, topology record and placement functions | Actors | `lib/actors`, Quicksilver schema | B-24 |
| [S-02](https://github.com/felixrydberg/bedrock/issues/110) | Volume protocol in TLA+: claim, append, lease, release, repair, topology change | Actors | `docs/spec/volume.tla` | decision 16 |
| [S-02b](https://github.com/felixrydberg/bedrock/issues/131) | Volume protocol fix in TLA+: epoch reservation and repair after a lost disk | Actors | `docs/spec/volume.tla` | S-02 |
| [S-03](https://github.com/felixrydberg/bedrock/issues/111) | `bedrock-volume` binary: records, gRPC service, membership, ranges, owner checks | Actors | `volume` bin | S-01, S-02b, B-21, B-68 |
| [S-04](https://github.com/felixrydberg/bedrock/issues/112) | Volume client in Compute: claim, recovery, append window, reads | Actors | `compute/actors` | S-03 |
| [S-05](https://github.com/felixrydberg/bedrock/issues/113) | Cold tier: one SlateDB per range, maintainer flush, truncation, split and merge by clone, SSD cache, S3 reader handles | Actors | `volume/cold` | S-03, B-11, B-39 |
| [S-06](https://github.com/felixrydberg/bedrock/issues/114) | Slot repair and node-reported range readiness | Actors | `volume/repair`, `lib/relays` | S-02b, S-03, B-68 |
| [S-07](https://github.com/felixrydberg/bedrock/issues/115) | `bedrock-compute` actor host: acquire, wake, sleep, leases, SQLite VFS, gates | Actors | `compute` bin | S-04 |
| [S-08](https://github.com/felixrydberg/bedrock/issues/116) | Gateway actor routing and location cache | Actors | `gateway/actors` | B-57, S-07 |
| [S-09](https://github.com/felixrydberg/bedrock/issues/117) | Alarms: holder timers and volume index wake | Actors | `compute/actors`, `volume/alarms` | S-07, S-08 |
| [S-10](https://github.com/felixrydberg/bedrock/issues/118) | Durable Objects API | Compute | `compute` | S-07 … S-09, sandbox decision |
| [S-11](https://github.com/felixrydberg/bedrock/issues/119) | D1 | Compute | `compute/builtin` | S-10 |
| [S-12](https://github.com/felixrydberg/bedrock/issues/120) | Queues | Compute | `compute/builtin` | S-09, S-10 |
| [S-13](https://github.com/felixrydberg/bedrock/issues/121) | Cron Triggers | Compute | `compute/builtin` | S-09, B-50 |
| [S-14](https://github.com/felixrydberg/bedrock/issues/122) | Workflows | Compute | `compute` (runtime library) | S-10 |
| [S-15](https://github.com/felixrydberg/bedrock/issues/123) | Hibernatable WebSockets held at the Gateway | Compute | `gateway/actors` | S-08 |
| [S-16](https://github.com/felixrydberg/bedrock/issues/124) | Simulation and crash load: actor workload, kills mid-append, slot loss, cell loss | Actors | `tests/sim`, `tests/crash` | S-07, B-23 |
| [S-17](https://github.com/felixrydberg/bedrock/issues/125) | Planned DC changes and manual DC-loss restore | Actors | `lib/actors`, operator tooling | S-05 |
| [S-18](https://github.com/felixrydberg/bedrock/issues/126) | Customer-KV writers as `kv` actors, with reads through the value cache to the holder (replaces B-26, B-27) | KV | `compute/builtin`, `kv` | S-05, S-08, B-25 |
| [S-19](https://github.com/felixrydberg/bedrock/issues/127) | Load- and flush-lag-based range splitting for `volume` ranges (later, gated on measurements; bytes balancing is in S-03) | Actors | `lib/placement`, `volume` | S-03, S-16 |

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

Created: [#75](https://github.com/felixrydberg/bedrock/issues/75) — Feature 🚀: Raft quorum for the write API with SlateDB persistence and automatic failover

---

#### B-09c — Bulk writes: many keys of one namespace in one revision

Created: [#76](https://github.com/felixrydberg/bedrock/issues/76) — Feature 🚀: Bulk writes that commit many keys of one namespace in one revision and fan out per range

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
minimum-revision reads and `LIST`. Amendment: [#59 comment](https://github.com/felixrydberg/bedrock/issues/59#issuecomment-6876204015).

---

#### B-28 — Namespace reader residency, cache budgets and basic eviction

Created: [#60](https://github.com/felixrydberg/bedrock/issues/60) — Feature 🚀: Per-node namespace reader retention and bounded customer-KV caching

Replaced (decision 16). The value cache budget and per-namespace share landed in B-39 (#73). Reader handle residency
moves to `volume/cold` in S-05, keyed by actor database. Amendment: [#60 comment](https://github.com/felixrydberg/bedrock/issues/60#issuecomment-6876205788).

---

#### B-29 — KV end-to-end acceptance

Created: [#61](https://github.com/felixrydberg/bedrock/issues/61) — Feature 🚀: KV end-to-end acceptance suite on the multi-region deployment

Depends on S-18 now (decision 16). Writer-crash scenarios become holder and slot kills; "home-region reader" scenarios
become value-cache misses to the holder. Amendment: [#61 comment](https://github.com/felixrydberg/bedrock/issues/61#issuecomment-6876208017).

---

#### B-58 — Simulation load: KV reads, listing and namespaces

Created: [#77](https://github.com/felixrydberg/bedrock/issues/77) — Feature 🚀: Add customer KV traffic to the simulation's load

---

### I — First CDN implementation

---

#### B-30 — Artifact storage: S3 content addressing, `CacheStorage`, Foyer

Created: [#78](https://github.com/felixrydberg/bedrock/issues/78) — Feature 🚀: Content-addressed artifact storage with pluggable CacheStorage and Foyer backend

---

#### B-31 — Artifact transfer

Created: [#79](https://github.com/felixrydberg/bedrock/issues/79) — Feature 🚀: Streaming peer-to-peer artifact transfer with S3 fallback and request coalescing

---

#### B-32 — `cdn` path rows and ref keys

Created: [#80](https://github.com/felixrydberg/bedrock/issues/80) — Feature 🚀: CDN path rows and ref keys in the `cdn` dataset

---

#### B-33 — Publish client writing path rows and ref keys in bulk

Created: [#81](https://github.com/felixrydberg/bedrock/issues/81) — Feature 🚀: Idempotent publish and release client for path rows and ref keys

---

#### B-34 — Regional L2 with two-replica artifact placement

Created: [#82](https://github.com/felixrydberg/bedrock/issues/82) — Feature 🚀: Regional L2 artifact placement with two verified replicas per retained artifact

---

#### B-35 — Artifact retrieval interface

Created: [#83](https://github.com/felixrydberg/bedrock/issues/83) — Feature 🚀: Key- and hash-addressed CDN retrieval interface (streaming API and HTTP)

---

#### B-36 — CDN end-to-end acceptance

Created: [#84](https://github.com/felixrydberg/bedrock/issues/84) — Feature 🚀: CDN end-to-end acceptance suite on the multi-region deployment

---

#### B-59 — Simulation load: publishing and artifact retrieval

Created: [#85](https://github.com/felixrydberg/bedrock/issues/85) — Feature 🚀: Add CDN traffic to the simulation's load

---

### J — KV refinements

---

#### B-37 — Cache-fill broadcast queue

Dropped (§4 decision 11). Customer KV uses read-driven caches over S3 namespace databases. Cache fills/evictions are local,
and neither a customer-KV relay subscription nor a separate fill-broadcast channel is required.

---

#### B-38 — Full cache lifecycle and eviction

Created: [#86](https://github.com/felixrydberg/bedrock/issues/86) — Feature 🚀: Adaptive KV cache admission and lifecycle across memory and disk

---

#### B-40 — `full_replica` placement and completeness

Created: [#87](https://github.com/felixrydberg/bedrock/issues/87) — Feature 🚀: full_replica policy with complete regional coverage and completeness reporting

---

#### B-41 — Adaptive cache promotion and regional load balancing

Created: [#88](https://github.com/felixrydberg/bedrock/issues/88) — Feature 🚀: Adaptive cache promotion and regional load balancing

---

#### B-42 — Auth dataset

Created: [#90](https://github.com/felixrydberg/bedrock/issues/90) — Feature 🚀: Auth dataset for API key hashes, permissions and status

---

#### B-53 — Namespace residency fairness and cold-open tuning at scale

Replaced (decision 16). Customer-KV writes and reads now go through `kv` actors, so the cost of many small S3 databases
was handled once, for all actors, in S-05. Amendment 2026-11-14: actors now share one S3 database per `volume` range
(`statefull/v2.md` §5 Cold tier), so there are no per-actor databases to tune.

---

### K — CDN refinements

---

#### B-43 — Artifact replica repair and range migration

Created: [#91](https://github.com/felixrydberg/bedrock/issues/91) — Feature 🚀: Automatic artifact replica repair and range migration

---

#### B-44 — Coordinated eviction, GC and failure recovery

Created: [#92](https://github.com/felixrydberg/bedrock/issues/92) — Feature 🚀: Coordinated artifact eviction, garbage collection and node/relay failure recovery

---

#### B-45 — Prewarming and artifact-set readiness

Created: [#93](https://github.com/felixrydberg/bedrock/issues/93) — Feature 🚀: Regional prewarming on publish and readiness reporting for a set of artifacts

---

#### B-46 — Optional unreplicated L1 and popularity-based retention

Created: [#94](https://github.com/felixrydberg/bedrock/issues/94) — Feature 🚀: Optional unreplicated L1 artifact cache and popularity-based retention

---

#### B-47 — Filesystem `CacheStorage` adapter

Created: [#95](https://github.com/felixrydberg/bedrock/issues/95) — Feature 🚀: Filesystem CacheStorage adapter

---

#### B-49 — Large-object and range transfers

Created: [#96](https://github.com/felixrydberg/bedrock/issues/96) — Feature 🚀: Large-object and HTTP range transfer support

---

### L — Compute and deploy control plane (future)

KV, CDN and (later) Compute are backing services. Everything that knows about Workers, Pages, deployments or revisions lives here,
as a client of those services. Nothing in tracks A-K depends on this track. Worker execution runs in `bedrock-compute`
(track O); the deploy control plane's crate is decided when the track starts.

---

#### B-50 — Deploy control plane: publish, activate, roll back, delete

Created: [#97](https://github.com/felixrydberg/bedrock/issues/97) — Feature 🚀: Deploy control plane orchestrating S3, CDN and Quicksilver for Worker and Pages deployments

---

#### B-51 — Gateway: Worker and Pages request routing

Created: [#98](https://github.com/felixrydberg/bedrock/issues/98) — Feature 🚀: Gateway routing Worker and Pages requests through Quicksilver and the CDN

---

#### B-48 — Compiled Worker artifact caching

Created: [#99](https://github.com/felixrydberg/bedrock/issues/99) — Feature 🚀: Cache compiled Worker artifacts

---

#### B-52 — Deploy end-to-end acceptance

Created: [#100](https://github.com/felixrydberg/bedrock/issues/100) — Feature 🚀: End-to-end deploy suite across KV, CDN and the control plane

---

#### B-60 — Simulation load: Workers, Pages and deploys

Created: [#101](https://github.com/felixrydberg/bedrock/issues/101) — Feature 🚀: Add Worker, Pages and deploy traffic to the simulation's load

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

Shipped in [#44](https://github.com/felixrydberg/bedrock/issues/44) (scope amendment 2026-11-03: B-55 binaries, B-54 deployment guide `docs/deployment/security.md`).

---

#### B-54 — Deployment guide: private networking and certificate issuance

Shipped in [#44](https://github.com/felixrydberg/bedrock/issues/44) (scope amendment 2026-11-03: B-55 binaries, B-54 deployment guide `docs/deployment/security.md`).

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

Created: [#102](https://github.com/felixrydberg/bedrock/issues/102) — Feature 🚀: Shared API rate-limit infrastructure across services

---

#### B-57 — Gateway binary and shared cluster client

Created: [#89](https://github.com/felixrydberg/bedrock/issues/89) — Feature 🚀: Gateway binary and shared client for routing between nodes and cells

---

#### B-61 — Simulation speed: more simulated seconds per real second

Created: [#66](https://github.com/felixrydberg/bedrock/issues/66) — Feature 🚀: Run more simulated seconds per real second

#### B-62 — Partial quarantine: rebuild only the damaged range

Created: [#103](https://github.com/felixrydberg/bedrock/issues/103) — Feature 🚀: Rebuild only the damaged range of a quarantined engine

---

#### B-63 — Operational scenarios: scale out, drain, cancel and rolling restart under load

Created: [#104](https://github.com/felixrydberg/bedrock/issues/104) — Feature 🚀: End-to-end operational scenarios: scale out, drain, cancel and rolling restart under load

---

#### B-64 — Appointed writer per namespace group, with a small controller log

Created: [#105](https://github.com/felixrydberg/bedrock/issues/105) — Feature 🚀: Appointed writer per namespace group, controlled by a small root log (design and model first)

---

#### B-65 — Shared durable log store with one group-committed fsync

Created: [#106](https://github.com/felixrydberg/bedrock/issues/106) — Feature 🚀: One durable log store per API node with a group-committed fsync for all writer groups

---

#### B-66 — Safety versus discovery: stale or unreachable inputs may only cost availability

Created: [#107](https://github.com/felixrydberg/bedrock/issues/107) — Feature 🚀: Audit and test that stale or unreachable discovery never breaks safety

---

#### B-67 — Explicit operator-controlled membership changes for the API Raft group

Created: [#108](https://github.com/felixrydberg/bedrock/issues/108) — Feature 🚀: API Raft membership changes only by explicit operator command, with safety checks

---

### O — Stateful actor platform

Decision 16. The drafts' scope, dependencies and acceptance live in [`statefull/v2.md`](statefull/v2.md): §4–§8 for the
design, §11 for the drafts S-01 … S-19 and their acceptance highlights, §12 for open questions. When a draft becomes an
issue, its body is written from those sections in the usual template. All are created (2026-11-14):

- S-01: [#109](https://github.com/felixrydberg/bedrock/issues/109) — Feature 🚀: Actor IDs, topology record and placement functions
- S-02: [#110](https://github.com/felixrydberg/bedrock/issues/110) — Feature 🚀: Volume protocol in TLA+: claim, append, lease, release, repair and topology change
- S-03: [#111](https://github.com/felixrydberg/bedrock/issues/111) — Feature 🚀: bedrock-volume binary: actor records, gRPC service, membership, ranges and owner checks
- S-04: [#112](https://github.com/felixrydberg/bedrock/issues/112) — Feature 🚀: Volume client in Compute: claim, recovery, append window and reads
- S-05: [#113](https://github.com/felixrydberg/bedrock/issues/113) — Feature 🚀: Volume cold tier: maintainer flush to SlateDB, truncation, SSD cache and S3 reader handles
- S-06: [#114](https://github.com/felixrydberg/bedrock/issues/114) — Feature 🚀: Volume slot repair and node-reported range readiness
- S-07: [#115](https://github.com/felixrydberg/bedrock/issues/115) — Feature 🚀: bedrock-compute actor host: acquire, wake, sleep, leases, SQLite VFS and gates
- S-08: [#116](https://github.com/felixrydberg/bedrock/issues/116) — Feature 🚀: Gateway actor routing and location cache
- S-09: [#117](https://github.com/felixrydberg/bedrock/issues/117) — Feature 🚀: Actor alarms: holder timers and volume index wake
- S-10: [#118](https://github.com/felixrydberg/bedrock/issues/118) — Feature 🚀: Durable Objects API on the actor host
- S-11: [#119](https://github.com/felixrydberg/bedrock/issues/119) — Feature 🚀: D1 on actors
- S-12: [#120](https://github.com/felixrydberg/bedrock/issues/120) — Feature 🚀: Queues on actors
- S-13: [#121](https://github.com/felixrydberg/bedrock/issues/121) — Feature 🚀: Cron Triggers on actors
- S-14: [#122](https://github.com/felixrydberg/bedrock/issues/122) — Feature 🚀: Workflows on actors
- S-15: [#123](https://github.com/felixrydberg/bedrock/issues/123) — Feature 🚀: Hibernatable WebSockets held at the Gateway
- S-16: [#124](https://github.com/felixrydberg/bedrock/issues/124) — Feature 🚀: Actor workload in the simulation and crash tests
- S-17: [#125](https://github.com/felixrydberg/bedrock/issues/125) — Feature 🚀: Planned DC changes and manual DC-loss restore for actors
- S-18: [#126](https://github.com/felixrydberg/bedrock/issues/126) — Feature 🚀: Customer-KV namespace writers as kv actors, with reads through the value cache
- S-19: [#127](https://github.com/felixrydberg/bedrock/issues/127) — Feature 🚀: Load-based range splitting for volume ranges

- **Storage first.** S-01 … S-06 build actor storage. S-02 updates the specification before any protocol code, and
  S-03 waits for it.
- **Hosting.** S-07 … S-09 host actors in `bedrock-compute`, route to them through the Gateway (needs B-57) and fire
  alarms.
- **Products.** S-10 … S-15 and S-18 wait for the #25 / B-23 gate like the other product tracks. S-10 also waits for
  the sandbox runtime decision (leaning towards sandboxed workerd).
- **Hardening and operations.** S-16 adds the actor workload to the simulation and crash tests; S-17 covers planned DC
  changes and DC-loss restore.
- **Later.** S-19's load-based splitting starts only once measurements show hot volume nodes with similar range sizes.
  Balancing on stored bytes moved into S-03 (amended 2026-11-14).

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
    - **Amendment 2026-11-14: one SlateDB per range.** Actors share one S3 database per `volume` range, written by the
      owner of each actor's first slot. A range flushes every second or 4 MiB, so S3 requests grow with ranges and
      bytes, not with the number of actors. SlateDB's own WAL is off. Splits and merges use SlateDB clones. Balancing
      on stored bytes moves into S-03; S-19 adds load and flush-lag signals. Details in `statefull/v2.md` §5 Cold tier
      and §11.
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
3. **Stateful actor questions.** Sandbox runtime, range-database details (clone API, prefix delete, naming, KV),
   page or chunk granularity, DC choice, a KV
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
