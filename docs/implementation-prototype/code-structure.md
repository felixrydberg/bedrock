bedrock/crates/
├── lib/                  # bedrock-lib: shared distributed core (library only)
│   ├── membership
│   ├── health
│   ├── buckets
│   ├── placement
│   ├── replication
│   ├── relays
│   ├── reconciliation
│   ├── storage
│   ├── datasets
│   ├── node
│   ├── s3
│   └── transport
│
├── api/                  # bedrock-api: write API
├── relay/                # bedrock-relay: relay process, built on lib
│
├── quicksilver/          # bedrock-quicksilver: quicksilver and auth datasets
│   ├── storage
│   └── namespaces
│
├── kv/                   # bedrock-kv: customer KV readers and caches (no virtual buckets)
│   ├── storage
│   ├── namespaces
│   └── operations
│
└── cdn/                  # bedrock-cdn: cdn dataset and artifact bytes
    ├── artifact_storage
    ├── artifact_transfer
    ├── paths
    └── retention

Each binary (`bedrock-api`, `bedrock-relay`, `bedrock-quicksilver`, `bedrock-kv`, `bedrock-cdn`) runs as its own process, depends on
`bedrock-lib` and never on another binary crate. Each binary crate is a library with a `run(config)` entry point
(service-specific code) plus a `main.rs` that only initialises logging, loads config and calls `run`.

`bedrock/tests/sim` (`bedrock-sim`, B-23a) holds the deterministic simulation tests. It boots those same `run`
functions as simulated nodes, so tests and production execute the same code. Most only compile under
`RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim"`, where `tokio` and `tonic` resolve to their madsim
simulators, and only on Linux or macOS (on Windows, use WSL). A normal `cargo test` lists them as one ignored test.
The exception is `tests/transport.rs` (B-03), which runs in both builds: over real HTTP/2 normally, where it also proves
bulk/control isolation, and simulated under madsim, where tonic has no flow control. Its `build.rs` compiles
`proto/sim.proto` with the vendored `protoc` in every build.
CI does not run the madsim builds (#26), so normal CI is not simulation evidence. Run them locally before merging
shared-core changes: `RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim" cargo test --workspace`, and lint the sim code
with `cargo clippy --workspace --all-targets -- -D warnings -A clippy::disallowed_methods -A clippy::disallowed_types`
under the same flags (madsim re-exports std's `Instant`, so simulation code reads time through `monotonic_now`).

`tests/sim/tests/vopr.rs` (#25 / B-23) is the fault simulation, after TigerBeetle's VOPR: one seed drives all five real
`run` functions through storage, network and process faults while clients write. A safety phase runs the faults, then a
liveness phase must converge within a bound, and `tests/sim/src/history.rs` checks everything observed against what the
writer acknowledged. Every seed sends the same load; the seed decides only how it arrives and which faults run.

| Suite | Command | Runs | For |
| --- | --- | --- | --- |
| Quick | `scripts/vopr.sh quick` | Retained seeds and deliberate bugs | Every change |
| Sweep | `scripts/vopr.sh sweep 500` | Many 60-second runs | Before merging |
| Soak | `scripts/vopr.sh soak 3600 8` | 8 seeds for a simulated hour each | What only appears over time |
| Watch | `scripts/vopr.sh watch main` | Sweeps and soaks of the branch's latest commit, forever, from a dedicated clone | Always-on testing of latest builds |
| Profile | `BEDROCK_VOPR_SECONDS=3600 scripts/vopr.sh profile 42` | One seed sampled by the kernel, listing its busiest functions and writing stacks for speedscope.app | Where a slow seed spends its CPU time |

Everything except the code under test is deterministic: faults, event order, keys, values and time all come from the seed.
A seed and commit reproduce the exact run, including a failure, in another process, with logging on, and in a debug or
release build. `scripts/vopr.sh replay 42` proves that by running a seed in two processes and comparing a fingerprint of
every fault decision and write with its simulated time. Each failure is rerun with logs and a stack trace and written to a
report with its seed and commit in `VOPR_REPORTS`, by default `~/bedrock-vopr/reports`. Runs long enough to report progress
also leave a `.metrics` file there, one line a minute per seed with real seconds, seed, simulated seconds and memory in MB.
The script prints from it whether real time per simulated hour and memory climbed. Real time also falls as other seeds finish,
so memory is the cleaner signal of anything accumulating. A panic or assertion names the failing line; an
invariant violation names what broke, and the replay with `RUST_LOG=bedrock_lib=debug` shows how it got there. A failing
seed goes in `OPEN` in `vopr.rs`, which lists what is left to fix, then moves to `REGRESSIONS` with its fix. If the mistake was
the harness's, the harness is fixed and the seed leaves the list. A seed reproduces only at the commit and length that found it, so each bug also gets a directed
regression test. Retained seeds run one process each, like a sweep, because madsim keeps state across a process and a seed
run after others can take a different path.

Replica copies are sampled and checked against the history. The directory invariants (`NoDropBeforeReady`,
`NoUnhealthyRouting`, `DirectoryCoverage`) are checked at the store instead, on durable writes in commit order, because a
sampled view can stall across a whole departure or recovery.

Simulation code stays out of production builds. Fault logic lives only in `tests/sim`: `src/storage.rs` wraps the storage
traits and owns faults, deliberate bugs and restart persistence, and `src/rpc.rs` owns RPC failures and the scheduling of
madsim's clogs, kills and pauses. `crates/lib/src/sim.rs`, compiled only under `cfg(madsim)`, is the seam: the storage
`cfg` switches and one line in each of `Peers::call` and `Peers::dial` go through thread-local hooks the harness installs.
`bedrock-lib` includes `tests/sim/src/storage.rs` as `storage::sim` for its tests and simulation builds, because its
in-crate tests cannot depend on another crate without a second copy of its traits. Tests outside the crate enable it with
the `test-support` feature.

`tests/crash` (`bedrock-crash`, #25 / B-23 Layer 3) runs the same cluster as the five release binaries on loopback, with
RocksDB, the SlateDB namespace engine and SeaweedFS as S3 in Docker. `scripts/crash.sh run` does one run of
`BEDROCK_CRASH_SECONDS` of faults, then waits for convergence and checks every node's disk; `scripts/crash.sh sweep 10`
does ten. The faults, picked by `BEDROCK_CRASH_SEED` and limited by `BEDROCK_CRASH_FAULTS`:

- `kill`: SIGKILL a process, then restart it.
- `disk`: while a storage node is down, cut its WAL tail, flip a byte in an SST file, or wipe a dataset.
- `network`: every process dials the others through a TCP proxy per node, which refuses, stalls, stalls replies or slows
  that node's inbound traffic. A partition is therefore one-way.
- `s3`: the API's S3 proxy refuses, slows, stalls replies, or forwards a request and cuts the reply.

It reuses `tests/sim/src/history.rs`. A running node holds its RocksDB lock, so its copies are read after a kill, which
makes each kill a crash-consistency check too. At the end, each storage node is killed again until its copies reach the
head, and copies of one node's disk, damaged on purpose, must break `GapFreeApply`, `NoStaleOverwrite`, `NoResurrection`
and `NoLostAck`. Unlike the simulation a run is not deterministic: the kernel decides the interleaving, so a failing seed
may not repeat; its configs, data, logs and `faults.log` stay in `/tmp/bedrock-crash-<seed>`. SIGKILL keeps the page
cache, so power loss is not covered. Stale writers are not covered yet.

Assertions check only this process's own memory, and stay on in release: a release build aborts on panic and on integer
overflow, so containers must restart on exit. Data from disk or the network is never asserted on, because one bad event
reaches every relay; it fails with an explicit error.
The I/O rules in `clippy.toml` apply to every crate; adapters opt out with an explicit `#[allow]`.

| Module           | Responsibility                                                        |
| ---------------- | --------------------------------------------------------------------- |
| `membership`     | Cell membership and the L2 placement directory from relay streams, published as one namespace |
| `health`         | Per-peer reachability from real traffic, storage health and scrubbing, availability |
| `buckets`        | 64-bit key hash, hash ranges and owner checks (`feature-drafts.md` §4 decision 13) |
| `placement`      | Relay range assignment, failure domains; customer-KV rendezvous       |
| `replication`    | Ordered streams, namespace revisions, replay, snapshots, and recovery |
| `relays`         | Relay tiers, event fan-out, peer discovery and range handoffs (run by `bedrock-relay`) |
| `reconciliation` | Compare desired state with actual state and schedule repair           |
| `storage`        | `StorageEngine` contract and adapters (RocksDB), shared by all binaries |
| `datasets`       | Dataset → namespace model and policy resolution                       |
| `node`           | Node config (file + `BEDROCK_*` env), roles, logging and graceful shutdown |
| `s3`             | Namespace-engine object I/O and checksummed CDN artifact storage     |
| `transport`      | Node-to-node transport and observability baseline                     |
| `quicksilver/storage` | Quicksilver/Auth L1/L2 hash-range keyspaces and eviction, on top of `lib/storage` |
| `kv/storage`     | Customer-KV reader residency and cache eviction (B-28, B-39)          |

