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
├── kv/                   # bedrock-kv: quicksilver, kv and auth datasets
│   ├── storage
│   ├── namespaces
│   └── operations
│
└── cdn/                  # bedrock-cdn: cdn dataset and artifact bytes
    ├── artifact_storage
    ├── artifact_transfer
    ├── manifests
    └── retention

Each binary (`bedrock-api`, `bedrock-relay`, `bedrock-kv`, `bedrock-cdn`) runs as its own process, depends on
`bedrock-lib` and never on another binary crate. Each binary crate is a library with a `run(config)` entry point
(service-specific code) plus a `main.rs` that only initialises logging, loads config and calls `run`.

`bedrock/tests/sim` (`bedrock-sim`, B-23a) holds the deterministic simulation tests. It boots those same `run`
functions as simulated nodes, so tests and production execute the same code. Most only compile under
`RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim"`, where `tokio`, `tonic` and `aws-sdk-s3` resolve to their madsim
simulators, and only on Linux or macOS (on Windows, use WSL). A normal `cargo test` lists them as one ignored test.
The exception is `tests/transport.rs` (B-03), which runs in both builds: over real HTTP/2 normally, where it also proves
bulk/control isolation, and simulated under madsim, where tonic has no flow control. Its `build.rs` compiles
`proto/sim.proto` with the vendored `protoc` in every build.
CI does not run the madsim builds; run them locally before merging shared-core changes:
`RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim" cargo test --workspace`, and lint the sim code with
`cargo clippy --workspace --all-targets -- -D warnings -A clippy::disallowed_methods -A clippy::disallowed_types`
under the same flags (madsim re-exports std's `Instant`, so the I/O rules only hold in the normal build).
The I/O rules in `clippy.toml` apply to every crate; adapters opt out with an explicit `#[allow]`.

| Module           | Responsibility                                                        |
| ---------------- | --------------------------------------------------------------------- |
| `membership`     | Cell membership and the L2 placement directory from relay streams, published as one namespace |
| `health`         | Per-peer reachability from real traffic, storage health, availability |
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
| `kv/storage`     | KV hash-range keyspaces and eviction only, on top of `lib/storage`    |

