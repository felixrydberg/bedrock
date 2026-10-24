KV Datasets:
- Quicksilver
    - Stores
        - Stores routing information for D1, Workers
        - Ownership
        - Configurations
    - Replicated Globally
- KV
    - Stores
        - Customer KV stores usable through Workers
    - Replicated on_demand
- Auth
    - Stores
        - API Key hashes, permissions, status
    - Replicated on_demand


Worker Execution:
- Requests enter the nearest available region.
- Quicksilver provides Worker deployment configuration:
  - Active deployment version and code artifact location (S3 / CDN).
  - Runtime settings (CPU, memory, timeout).
  - Bindings to KV namespaces, D1 databases, and other Workers.
  - Environment variables and secret references.
- Regional Rendezvous Hashing deterministically selects an execution host.
- Existing warm VMs are reused; otherwise, a new VM starts (under 100 ms).
- VM placement, health, and capacity are managed locally.

