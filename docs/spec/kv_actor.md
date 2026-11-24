# S-18 KV actor safety model

`kv_actor.tla` models #126 / S-18's namespace semantics on the existing volume
protocol. `Commit` abstracts a complete entry replicated to the fenced holder's
SSD quorum, not an S3 flush. `Move` discards an uncommitted candidate and opens the
committed state. The volume models remain responsible for claims, fencing,
quorum intersection, recovery, cold storage and handoff; this model introduces no
new quorum protocol and does not re-prove those properties.

One namespace has two keys and two retry identities. Each accepted single-key or
bulk mutation creates one namespace revision with a complete reference snapshot,
fixed original commit time, event and retry result. Buffered and committed state
must match that snapshot, including exact physical byte accounting. Replacing a
key subtracts its old size; deletion subtracts its full size. Expired physical
rows remain charged until removed. Oversized requests are disabled. Byte sizes
are small abstract units; real API limits, checked arithmetic, namespace/tenant
authorization and wire validation require Rust tests.
The event snapshot is an oracle abstraction of the original mutation payload;
production retains the mutation, not a full copy of the namespace.

Only a data mutation allocates a namespace revision. Claims and recovery do not.
Acknowledgement requires committed progress. The model can commit without
acknowledging, recover on another holder, commit another request, and retry the
first identity. Its response must retain the original revision and original fixed
expiry. Expired retry identities may be reused. Event cleanup and retry cleanup
use separate windows, can lag indefinitely, and must preserve young records.
The configured units compress the real fixed 24-hour retry and 60-second event
windows while preserving their relative order; changing the model constants does
not configure production retention. Data-row compaction is omitted: it may remove
expired physical rows and adjust bytes atomically without allocating a revision.

Holder reads prove the namespace revision for both presence and absence. Cache
fetches carry that proof and their original fetch time through a delayed network
response. A later-arriving response cannot regress a key's cache revision, even
after expiry. Cache deadlines are bounded by both the original fetch time plus
the cache window (60 seconds in production), and the value's expiry. Reads never
return expired values and a minimum-revision cache read requires its proof to meet
the minimum. Ordinary cache reads may return a still-live older version.
Read and retry responses update Boolean safety monitors instead of retaining all
response tuples, keeping observations from multiplying the reachable state space.

`kv_actor.cfg` bounds revisions at 3, logical time at 4, and holders at 2. Safety
invariants cover both sides of the atomic commit, revision ordering, committed
acknowledgement, quota, retry retention/results, event retention, cache monotonicity
and lifetime, minimum reads and value/absence visibility. There is no fairness or
liveness claim. Listing pagination, directory deletion, multiple namespaces,
production resource limits and the runtime/sandbox binding contract require
implementation acceptance checks.

Every deliberate bug is registered in `scripts/tlc.sh`: partial bulk application,
torn commit, early acknowledgement, claim-induced revision, double-counted bytes,
quota bypass, lost/renewed retry, retry returning the latest revision, early event
cleanup, stale cache overwrite, extended cache TTL, unproved minimum reads and
expired-value reads. Run `scripts/tlc.sh kv_actor` with a fixed worker cap and heap;
it checks the normal configuration and each bug against its named invariant alone.
The local run passed on 2026-11-23: 10,188,440 distinct states, depth 16, in 12 minutes 46 seconds with one worker and
a 4 GiB heap. All fourteen local deliberate bugs broke their named invariants. This is a bounded local result.

`kv_actor-local.cfg` is the separate local check: two revisions, four time units
and two holders, with the same invariants and all fourteen deliberate bugs. The
full three-revision configuration is unchanged. The initial full run used two
workers and a 4 GiB heap and was stopped after 24,061,202 distinct states with
10,318,417 still queued; it did not finish. Subsequent local runs use one worker.
