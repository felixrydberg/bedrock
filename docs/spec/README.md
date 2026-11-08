# Storage recovery model (#11 / B-11)

Relay protocol and recovery: [#14 / B-17 and #16 / B-18](relays.md).
Cell membership from relay streams, the L2 placement directory and range handoff: [#15 / B-12, #19 / B-16 and #22 / B-21](membership.md).

The standalone writer persists directly to SlateDB in S3. See
[the atomic namespace protocol model](materialized.md) for its bounded checks and
explicit limits. There is no separate raw S3 journal, published-object boundary,
pruned-object watermark or legacy archive compatibility path.

## Atomic writer integration

`[s3]` in `deploy/config/api.toml` supplies the namespace engine's object-store configuration.
`event_retention_ms` and `retry_retention_ms` are independently configurable under
`[retention]` and each default to `86400000` (24 hours).

The real engine is pinned to SlateDB **0.17.0**, published source revision
`c1e36fc07fb9b759d31484dda18c1c1aad8a2853`, with default features disabled.
Bedrock-owned database rows use a versioned protobuf format. Engine manifests,
WAL and SSTs live under `{dataset}/{namespace}/state/`; no Bedrock `log/`,
`published/` or `pruned/` objects accompany them.
The alpha row format is now version 2 and deliberately rejects the previous
layout. Existing alpha databases need a reset or an external migration before
use; startup does not silently reset data.

`Materialized::persist(entries, represented_raft_position)` writes one atomic
engine batch containing original retained events, resolved values/deletes,
namespace revision and original `committed_at`, retry key/result/fixed expiry,
and the optional represented Raft index/term. It then explicitly awaits remote
durable flush and returns durable `Progress`. `Materialized::apply` remains a
buffered helper for adapter tests; buffered apply alone cannot authorize an
acknowledgement. Engine sequence IDs are not Bedrock revisions. Progress,
including event `pruned_through`, is database metadata, not a native checkpoint.

`Writer::open(clock, Persistence { factory, retention, metrics })` uses that
persistence path as the standalone commit authority. It returns a successful
write only after remote durability. A failure after durable flush can have an
ambiguous client outcome; the fixed retry result prevents duplicate application.
SlateDB 0.17 closes a database for good when a background task fails, which a
single failed object-store request causes. So the writer drops any handle whose
call failed, and the next use reopens it from durable state, as a restart would.
A handle closed by fencing is never reopened. A reopen can fence a
newer writer, as restarting an old one can, but cannot undo its commits: the
writer takes its head from the reopened store, never from memory. Unacknowledged
buffered writes may be absent after a reopen, while a lost durable response may
already be present. Retrying the original identity resolves that ambiguity.
Startup opens and validates durable database state and recent retained events;
it never reconstructs current values by replaying a separate raw S3 journal.

The writer retains fenced engine handles for its lifetime. The native
`lib/s3::object_store` factory supplies the upstream `object_store::ObjectStore`
interface with strict create, CAS, ranges and multipart I/O. The former custom
trait and AWS/memory implementations are removed. Whole-object `put_checked`
and `get_checked` helpers preserve SHA-256 artifact validation without wrapping
SlateDB's ranged reads. Native Create rejects identical existing bodies too;
request idempotency belongs to the atomic namespace retry records.
Missing referenced objects, corruption and unsupported operations are errors;
only an absent application key is `Ok(None)`.

## Retention and maintenance

Event history and retry protection have separate logical expiry. Lookups reject
expired rows immediately, even if physical cleanup has not run. Cleanup preserves
original timestamps and fixed expiry, runs asynchronously in response to namespace
activity, and meters failures without changing already successful write results.
Bedrock never polls idle namespaces for cleanup. Physical reclamation may wait
for another event or an explicit operation, and failure can retain old rows beyond
their nominal age. Current resolved state survives event-history deletion.

Preserve native engine compaction, checkpoint/reference protection and GC; native
LSM tombstones are implementation details. The Bedrock no-idle-polling rule does
not remove SlateDB's internal timers while a handle is open. Later B-39 readers
must protect native read/checkpoint views rather than infer protection from a
namespace progress record. Upstream attribution remains in the dependency; these
contracts do not claim a custom timer-free fork.

## Later Raft integration

B-09b is not implemented. It will acknowledge majority-fsynced entries before
batched SlateDB persistence, retain the committed not-yet-durable tail for retry
lookup and recovery, and call the same persistence operation with its represented
Raft position. A native SlateDB checkpoint is separate from progress metadata.
Raft truncation requires a recoverable checkpoint covering every namespace in
the truncated prefix, with Raft index, term and membership. Closed namespaces
must remain recoverable, and stale publishers must remain fenced.

## Model checks (#25 / B-23 Layer 1)

`scripts/tlc.sh` runs TLC on each model's config, then checks every deliberate bug. Each model has a `Bug`
constant, `"none"` in its config. Every other value is a bug that must break the invariant the script names beside
it, checked with that invariant alone so no other one fires first. `scripts/tlc.sh bugs` runs only the bugs, in
under a minute, and `scripts/tlc.sh relays` runs one model. It needs Java 11 or later and the
[TLA+ tools jar](https://github.com/tlaplus/tlaplus/releases), by default at `.codex-tmp/tla2tools.jar`.
Every invariant and property has a bug except `TypeOK`, which only catches mistakes in a model.

On 2026-11-08, TLC 1.8.0 passed every config and caught all 38 bugs:

| Model | Generated states | Distinct states | Time |
|---|---:|---:|---:|
| `relays` | 26,865,595 | 2,705,822 | 10 s |
| `follows` | 2,399 | 713 | 1 s |
| `merge` | 262 | 112 | 1 s |
| `materialized` | 2,207 | 888 | 1 s |
| `publisher` | 2,965 | 436 | 1 s |
| `membership` | 17,874,929 | 1,437,600 | 37 s |
| `handoff` | 396,310,318 | 34,143,510 | 8 min |

The models and the simulation (`tests/sim`) use the same names for the shared invariants:

| Invariant | Models | Simulation |
|---|---|---|
| `NoLostAck`, `RetryProtection` | `materialized` | yes |
| `GapFreeApply` | `relays`, `merge` | yes |
| `NoResurrection`, `NoStaleOverwrite` | within `relays` `GapFreeApply` and `materialized` `AtomicDurable` | yes |
| `DirectoryCoverage`, `NoDropBeforeReady` | `membership`, `handoff` | yes |
| `NoUnhealthyRouting` | `membership` | yes |
| `OwnedOrLeaving` | `handoff` | yes |
| `MinRevisionReads` | none: no model has a read path | yes |
| `Converged`, `RedundancyRestored` | none: liveness, and the models have no fairness | yes |

The models track payload as the set of revisions applied, so a resurrected or stale value shows as a wrong set,
which `GapFreeApply` catches. The other invariants are each model's own. On 2026-11-07, `Covered` became
`DirectoryCoverage`, `NoEarlyDrop` became `NoDropBeforeReady` and `FilteredCoverage` became `GapFreeApply`; older
check records in the model reports use the old names. `membership`'s `NoUnhealthyRouting` was split out of `NewOwnersPublished`,
and `handoff` gained `OwnedOrLeaving`.

## Validation boundaries

The [model report](materialized.md) records checks actually run for this protocol.
Normal adapter, writer and `cfg(madsim)` tests are separate evidence; run the
current focused simulation test with the repository's documented simulation flags.
Neither the model nor focused tests establish the full B-23 multi-layer and
seven-night gate or real-S3 crash/fault acceptance.
