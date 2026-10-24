bedrock/
├── lib/
│   ├── membership
│   ├── health
│   ├── buckets
│   ├── placement
│   ├── replication
│   ├── relays
│   └── reconciliation
│
├── api/                  # bedrock-api: write API
├── relay/                # bedrock-relay: relay process, built on lib
│
├── kv/
│   ├── storage
│   ├── namespaces
│   └── operations
│
└── cdn/
    ├── artifact_storage
    ├── artifact_transfer
    ├── manifests
    └── retention

| Module           | Responsibility                                                        |
| ---------------- | --------------------------------------------------------------------- |
| `membership`     | Cell membership from relay streams, published as a namespace          |
| `health`         | Per-peer reachability from real traffic, storage health, availability |
| `buckets`        | Virtual bucket assignment, ownership, and rebalancing                 |
| `placement`      | Rendezvous hashing, replica selection, and failure-domain awareness   |
| `replication`    | Ordered streams, namespace revisions, replay, snapshots, and recovery |
| `relays`         | Relay tiers, event fan-out and peer discovery (run by `bedrock-relay`) |
| `reconciliation` | Compare desired state with actual state and schedule repair           |

