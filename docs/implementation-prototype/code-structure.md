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
`bedrock-lib` and never on another binary crate.

| Module           | Responsibility                                                        |
| ---------------- | --------------------------------------------------------------------- |
| `membership`     | Cell membership from relay streams, published as a namespace          |
| `health`         | Per-peer reachability from real traffic, storage health, availability |
| `buckets`        | Virtual bucket assignment, ownership, and rebalancing                 |
| `placement`      | Rendezvous hashing, replica selection, and failure-domain awareness   |
| `replication`    | Ordered streams, namespace revisions, replay, snapshots, and recovery |
| `relays`         | Relay tiers, event fan-out and peer discovery (run by `bedrock-relay`) |
| `reconciliation` | Compare desired state with actual state and schedule repair           |
| `storage`        | `StorageEngine` contract and adapters (RocksDB), shared by all binaries |
| `datasets`       | Dataset → namespace model and policy resolution                       |
| `s3`             | L3 S3 client with checksums and versioned manifests                   |
| `transport`      | Node-to-node transport and observability baseline                     |
| `kv/storage`     | KV keyspaces and eviction only, on top of `lib/storage`               |

