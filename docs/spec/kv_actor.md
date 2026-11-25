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
Quota is fixed throughout a trace, starting from an empty namespace. External
quota decreases and the resulting already-over-quota state are outside this
model: production permits a write in that state only when its physical bytes do
not increase after expiry cleanup. Rust acceptance checks cover that admission
rule; this model introduces no dynamic quota or control-plane protocol.
The event snapshot is an oracle abstraction of the original mutation payload;
production retains the mutation, not a full copy of the namespace.
While an entry is staged but not committed, `AtomicBuffered` requires both its
exact retry result and its event, rather than merely checking any records that
happen to be present. Cleanup is still legal after commitment and acknowledgement.

Only a data mutation allocates a namespace revision. Claims and recovery do not.
Acknowledgement requires committed progress. The model can commit without
acknowledging, recover on another holder, commit another request, and retry the
first identity. Its response must retain the original revision and commit time,
checked against the original history entry independently of the stored retry
expiry. Retention must preserve that expiry too. Expired retry identities may be
reused. Event cleanup and retry cleanup
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

All configurations use the same safety invariants and seventeen deliberate bugs:

| Configuration | Revisions | Time | Holders | Quota |
| --- | ---: | ---: | ---: | ---: |
| `kv_actor.cfg` | 3 | 4 | 2 | 5 |
| `kv_actor-local.cfg` | 2 | 4 | 2 | 5 |
| `kv_actor-overwrite.cfg` | 3 | 4 | 2 | 7 |
| `kv_actor-review.cfg` | 3 | 3 | 2 | 7 |

The original full and local configurations retain their committed bounds. With
quota 5 a bulk at revision 1 fills both keys, but a bulk at revision 2 costs 7 and
cannot be accepted. The additional quota-7 configurations allow a committed row
to be overwritten by an accepted two-key bulk, including replacement-byte
accounting. The separate review configuration limits time to 3. It keeps three
revisions so the quota-bypass bug can exceed quota 7, and two holders to exercise
recovery and the deliberate claim bug. Three time units allow a retry identity to
expire before its reuse for the third revision.

Safety invariants cover both sides of the atomic commit, revision ordering, committed
acknowledgement, quota, retry retention/results, event retention, cache monotonicity
and lifetime, minimum reads and value/absence visibility. There is no fairness or
liveness claim. Listing pagination, directory deletion, multiple namespaces,
production resource limits and the runtime/sandbox binding contract require
implementation acceptance checks.

Every deliberate bug is registered in `scripts/tlc.sh`: partial bulk application,
torn commit, early acknowledgement, claim-induced revision, double-counted bytes,
quota bypass, lost/renewed retry, retry returning the latest revision, early event
cleanup, stale cache overwrite, extended cache TTL, unproved minimum reads,
expired-value reads, missing staged retry/event records, and a retry reporting a
later commit time while retaining the original expiry. Each bug checks its named
invariant alone.

Historical validation before these model changes: on 2026-11-23 the local command
`TLC_WORKERS=1 TLC_HEAP=4g scripts/tlc.sh kv_actor-local` completed with 10,188,440
distinct states, depth 16, in 12 minutes 46 seconds; its then-fourteen bugs all
broke their named invariants. That result does not validate the strengthened
model. The separate full `scripts/tlc.sh kv_actor` run used two workers and a
4 GiB heap and was stopped after 24,061,202 distinct states with 10,318,417 still
queued; it did not finish. Subsequent runs use one worker.

Reproduce the strengthened local check with
`TLC_WORKERS=1 TLC_HEAP=4g scripts/tlc.sh kv_actor-local`; use
`TLC_WORKERS=1 TLC_HEAP=4g scripts/tlc.sh kv_actor-review` for the additional
bounded overwrite coverage. Full commands are
`TLC_WORKERS=1 TLC_HEAP=4g scripts/tlc.sh kv_actor` and
`TLC_WORKERS=1 TLC_HEAP=4g scripts/tlc.sh kv_actor-overwrite`.
Each command checks its normal configuration and all seventeen bugs.

On 2026-11-24 the strengthened `kv_actor-local` passed 10,188,440 distinct states,
depth 16, in 801 seconds, with one worker and a 4 GiB heap; all seventeen bugs
broke their named invariants. A separate temporary quota-7 smoke config kept two
revisions, one time unit and two holders: all invariants passed 215,752 states in
19 seconds. An independent negative reachability assertion found an accepted
second bulk overwriting a committed first-revision row. This supplements the
quota-5 check without pretending the larger quota-7 configs finished. Neither
`kv_actor-review` nor either full three-revision config was run in this review.
