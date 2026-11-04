# Relay protocol (#14 / B-17, #16 / B-18)

The Relay.Session protocol multiplexes pushed datasets over one persistent
subscription. Hello negotiates version 1 and the downstream tier (leaf, cell, DC,
region); Welcome identifies the source (writer, region, DC, cell). Live edges must
strictly increase tier. Recovery requests may visit siblings, carry a visited path,
and never create a live edge. This is the sole subscription transport; the
superseded B-10 Replication.Subscribe client, service and relay bridge were removed.
The writer's ordered log and change notifications remain shared primitives.

Each follow selects a namespace or a dataset hash range ([range streams](#range-streams)).
Delivery covers an inclusive consecutive revision interval per namespace, including
revisions whose payload lies outside the range. Matching original events and the
covered cursors commit atomically. Selector identity is persisted with the cursors.
Coverage never renumbers events or changes committed_at. Tick proves
connection liveness only and cannot advance applied state. Customer kv is rejected.

Replay is a disposable checksummed cache, never commit authority. Advertised ranges
are contiguous validated committed records. Eviction or disk failure removes range
claims but cannot block live fan-out. Catch-up may read peers and upstream sources.
Beyond authoritative retention, a coherent writer view is staged and checksummed;
only successful validation permits atomic installation. A bounded lease acquired
with the view protects the subsequent tail from GC through catch-up. Expiry or
disconnection aborts recovery. No readiness follows a partial or corrupt transfer.

## Range streams

#22 / B-21 (§4 decision 13, refined 2026-11-02; [membership.md](membership.md#range-handoff)).

- **Selector.** A follow selects either a namespace (membership, whole) or a dataset hash range
  `[start, end)` (`end` 0 meaning 2⁶⁴), never both. A range follow is one stream carrying every namespace of the
  dataset. Events keep their namespace and per-namespace revision. Coverage is per namespace: a delivery notes how
  far each namespace it touches is covered, so namespaces without writes add nothing. The subscriber keeps one
  covered revision per namespace and resumes with that table. Each ranged dataset has a fixed namespace set in code
  (`placement::namespaces`): `quicksilver` has `infrastructure`, `auth` has `keys` and `cdn` has `pages` and
  `workers`. The writer rejects any other namespace of these datasets, so the list is complete. This is the
  model's constant `Namespaces`.
- **Pushed ranges.** A relay pushes each leaf subscriber its full owned range set per dataset, tagged with the
  directory revision it was derived from. The subscriber follows the ranges added. A range dropped from the set is
  the revoke: the subscriber confirms it against its own directory view, then unfollows and deletes the range, or
  refuses and reports a mismatch. Narrowing a held range after a split keeps its coverage; a new range starts from
  nothing and bootstraps where retained history does not reach.
- **Bootstrap.** Each namespace of the range bootstraps at its own boundary under its own lease and installs
  atomically. Readiness requires every namespace confirmed and caught up.
- **Relays.** A relay follows a dataset whole (`[0, 2⁶⁴)`) from its upstream as soon as one subscriber follows it
  by range, and never also by namespace. It starts each subscriber's range in every namespace of the dataset. A range
  follow's `from_namespaces` entry of 0 asks for a fresh baseline; a namespace without an entry starts at 1.
- **Readiness.** The relay reports a subscriber ready for a range only after it processed every pushed set and
  then acknowledged through the head, so an acknowledgement from before a pending revoke never counts.
- **Relay side.** On every reconciliation pass each cell relay pushes its own leaf members their ranges per dataset
  from the published directory, only when a set changed. `Hub::ready` counts a member ready for a range once its
  `RangesAck` covers the latest pushed set and, in every namespace of the dataset, the range's stream finished any
  bootstrap (its recovered confirmation was sent) and was acknowledged through the head recorded when it first
  qualified. Relays share these ready bits with their siblings each second; the publisher writes `ready` from its own
  and its live siblings' bits (`placement::handoff`), then drops `leaving` owners. A namespace with no writes yet
  counts too, so a member cannot be ready while it still lacks a namespace.
- **Member side.** A member keeps only the latest pushed set per dataset and applies it once its own membership
  view has reached the set's revision (`relays::ranges`): it follows added ranges, carries a split range's coverage
  to the parts it keeps, and drops a revoked range only when its view no longer lists it there and lists another
  `ready` owner. A drop deletes the range's span and its consumers' progress in one atomic batch, then compacts the
  span. It acknowledges every set it applied with `RangesAck`, listing the revokes it refused and why; the relay
  then forgets that set and pushes it again on its next publish pass. A range
  held before a restart resumes until the relays push the current set.

## Model

relays.tla is a bounded safety abstraction: coverage is represented one revision at
a time per namespace, with alternating revisions modeling range matches and omissions.
One stream interleaves the namespaces' coverage. Its
projection invariant requires all selected payloads through covered progress,
while unselected revisions still advance coverage. `projected` is ghost history,
not a physical snapshot log or tombstone set. Cached events are disposable and the
writer history is abstracted as recoverable while pinned. It checks consecutive
coverage, queue bounds and protected bootstrap installation through disconnects.
`Assign` and `Revoke` push the range and its removal as ordered frames, which a
disconnect loses and a reconnect resends as the current set. `Receive` follows the
range or deletes it, restarting coverage from nothing. `Report` is the relay's
readiness, kept by the directory until a revoke: `ReportedHolds` checks that a
reported subscriber still follows the range.
It does not prove payload routing, Rust correctness, disk checksums, fairness,
resource limits, or the complete B-23 gate. Implementation tests must cover those.
Run TLC with relays.cfg; record actual results separately.

The model has one subscriber stream and no relay subscription set, so [sibling interests](#sibling-interests) add no
state or action to it (checked against the model, 2026-11-03; TLC not rerun, the model is unchanged). A sibling
interest only makes a relay follow more, and a move to a warm sibling is `Disconnect` then `Reconnect` with a
non-empty `cached`.

## Deployment and recovery

`bedrock-relay` serves Relay.Session on `listen`. `upstreams` retains preference order;
the peer's negotiated tier must be higher. `replay_peers` names same-tier siblings:
recovery sources and the [sibling stream](membership.md), not live parents. Recovery carries a visited path capped at eight relays.
There is one persistent subscription per process; temporary recovery sessions are
bounded and may use other peers. Control traffic remains separate from artifact
bulk connections.

Deploy at least three relays per cell (membership publication needs a majority), adding approximately
one relay per 500–1,000 nodes. Rotate the configured relay preference across nodes to distribute load;
upper tiers need only a few relays. Provisioning is static configuration here.

No node configures subscriptions. `bedrock-quicksilver` and `bedrock-cdn` follow the ranges the
relays push of each dataset they carry (`cdn` for the CDN, whose `ref/{sha256}/…` keys are placed
by content hash, `feature-drafts.md` §4 decision 14); `Session::join` maps those datasets to their engines.
Every node also follows its own membership namespace whole. Customer
`kv` opens/reads do not create interests. Leaf sessions also carry [cell membership](membership.md)
(#15 / B-12). The leaf client stores a whole namespace under its namespace-NUL prefix and a
range in the hash-first L2 layout (`l2/{hash}/{namespace}\0{key}`, `storage::l2_key`), and
persists its selector identity and covered revision in the same local atomic batch. A range
consumer also marks its namespace, so a restart resumes every namespace it held. Changing a
selector restarts recovery from validated state rather than reusing coverage.
Call `Session::stop().await` before replacing or reusing its local storage; it
aborts and joins the applying task. Dropping the session requests cancellation.

Both directions send progress every 10 seconds on the existing session. A fixed
30-second incoming deadline detects one-way partitions even while outgoing progress
continues. Tick/progress alone never advances data or a revision. A receiver queue
is capped at 1,024 records and 4 MiB of serialized frames; control frames also have
a bounded channel. Outstanding revision coverage has a separate 1,024 window.
Full queues disconnect only that subscriber; exhausted acknowledgement windows
wait up to the idle deadline before disconnection. Encoded events and key hashes
are shared across recipients in a bounded 4 MiB/4,096-record recent cache.

Replay persistence has its own 1,024-record/4 MiB input queue and commits batches
of at most 256 records. A normal build runs blocking adapter writes on the runtime's
blocking pool; simulation uses the deterministic adapter directly. Overflow drops
disposable cache entries, leaving accurate gaps for upstream recovery, while live
forwarding continues. The worker holds at most one additional bounded batch.
Scoped recovery/storage errors gate only the affected namespace; healthy datasets
keep receiving updates and satisfying revision waits. Failed scopes retry recovery
on the existing progress interval rather than restarting healthy streams.
A storage rebuild (#23 / B-22) recreates a quarantined dataset's engine empty and asks
the session to recover that dataset's scopes at once (`Rebuilder`). Like a local failure,
the scope drains its old subscription; it is ready again only after a fresh validated
bootstrap and its recovered confirmation (`LoseLocal`, `ReadyNotLost`).

### Sibling interests

#41 / B-12b. Every relay shares its subscribers' interests with its `replay_peers`, so a sibling keeps the same streams
live and in its replay log. When a relay dies, its subscribers arrive at a warm sibling: no new upstream follow and no
recovery request, as long as their cursors fall within the sibling's replay window.

- **Shape.** An interest is the `Follow` the relay would send upstream for its own subscribers alone, and its removal
  the matching `Unfollow`. Whatever hash ranges subscribers follow, a relay follows that dataset whole, so one
  interest (`[0, 2⁶⁴)`, every fixed namespace) stands for all of them. A cell shares at most six: three datasets and
  three membership namespaces.
- **Own only.** A relay shares what its own sessions need, never what a sibling shared. Upstream it follows the union
  of the two, as one more subscriber, so the tier above sees nothing new. When the last real subscriber leaves, the
  interest leaves with it.
- **Where a shared stream starts.** After the relay's own history, or at the cursor in the interest if that is later.
  An idle relay with no history therefore starts where its sibling's subscribers are, not at revision 1, which could
  ask the writer for a bootstrap nobody uses. Cursors are sent once and not updated as they advance.
- **Every tier.** Cell siblings share too, rather than deriving interests from the placement directory: DC and region
  need sharing anyway, relays without `[membership]` never read the directory, and it holds no membership namespaces.
  Above the cell tier the stream carries interests only, and the same-cell check on `Siblings.Watch` applies only at
  the cell tier, because only its relays share one cell.
- **Lost sibling.** Interests a sibling shared when its stream dropped stay for the grace period (`membership.grace_ms`,
  otherwise its 20-second default), even if it returns sooner without them, because its subscribers are still moving.
- **Cap.** A relay follows at most 4,096 sibling interests beyond its own, range follows first, then by selector. The
  rest stay cold and are reported; they fail over as before. Own subscribers are never capped.

A relay that cannot reach a sibling falls back to cold failover. Shared state changes within about a second.

`[replay]` overrides `max_bytes`, `window_ms`, and dataset `priorities` (higher values
retain data longer). Defaults are 1 GiB and 5/15/60 minutes at cell/DC/region tiers;
a smaller node disk budget caps the replay budget. The store validates checksummed
records before advertising contiguous ranges. After restart, validated persisted
history is available even while parents are unavailable. Live reconnects start
after the contiguous prefix covered by disk and recent memory, without skipping
gaps. Range head is the greatest observed source revision (initially a lower bound
from validated disk history), separate from cached first/last; a stale parent
cannot reduce it. Zero/zero advertises no local cached range. A range is a
point-in-time hint and reads revalidate it before serving. A recovery candidate
behind the requested cursor is skipped in favor of the next source. Age pruning is
triggered by append/read activity; idle physical cleanup may wait for activity.

Storage failures disable cache ranges while the in-memory live path continues.
After a successfully opened cache fails, future events retry rebuilding its
disposable prefix at most once per second. If initial open/validation fails, the
node runs without disk replay until the cache is repaired or cleared and restarted.
The logical byte budget is strict; physical RocksDB usage includes WAL/SST overhead
and is checked, where supported, against twice the logical budget. Exceeding that
disables caching. The storage contract
cannot guarantee filesystem compaction or prevent temporary physical overshoot;
deployment disk quotas remain an adapter/host responsibility.

Expired authoritative cursors use a writer-owned coherent view, never a relay's
replay records as a substitute for current state. The header and sorted live entries
are SHA-256 checksummed. A subscriber stages and validates all chunks before atomic
installation; deletes after the boundary are ordinary ordered events. Capture and
lease registration share the writer namespace gate; GC cannot prune the protected
tail. Bootstrap End permits atomic installation and acknowledgement, but readiness
also requires a recovered Range confirmation from the source after the installed
boundary and catch-up were acknowledged under a valid lease. A proxy preserves that
confirmation; it cannot invent one. Leases expire after 60 seconds without progress;
sent bootstrap chunks and advancing acknowledgements renew them, never beyond
10 minutes from acquisition. They are scoped
to dataset/namespace. There are at most four per namespace and four concurrent
generated/proxied recoveries per source Hub. Pending confirmation is persisted with
the installed state, so a restart cannot treat an unconfirmed baseline as ready.
Disconnect, expiry and corruption leave a partial view unready. A full owned B-11
view is retained during generation, so bootstrap memory scales with namespace size.
The current B-11 view/tail APIs also materialize retained history; this is not a
constant-memory bootstrap implementation.

## Introspection and metrics (#17 / B-18b)

`bedrock-relay` serves read-only JSON on its admin listener at `/introspect/{view}`. Every list view is
paginated (`limit`, default 100, at most 1,000; `after` is the `next` cursor of the previous page) and filtered
by `dataset`, namespace `prefix` and `service`, so one namespace can be inspected without listing the rest:

| View | Contents |
|---|---|
| `upstream` | connected upstream; per namespace the sequence received through (`cursor`), the upstream's announced head, revisions `behind`, the latest `committed_at` and its lag on arrival |
| `namespaces` | per `(dataset, namespace)`: subscriber count, latest sequence received, head, replay ranges, events/s and bytes/s over 10-second windows, lag on arrival |
| `subscribers` | per session: service, node and address of its member, follows with acknowledged revision and lag (head minus acknowledged), queued events and bytes, the member's disconnect count |
| `membership` | the published members of each service with their status, the placement directory with every owner's state, and how long publication has been frozen |
| `siblings` | the interests siblings share beyond this relay's own, each `followed` or `cold`, and per sibling (`streams`) whether it is connected and how many interests it shares or left lingering |

The admin listener is unauthenticated and these views expose node IDs, addresses and namespace names, so
`admin_listen` belongs on a trusted management network. A page seeks through the sorted namespace map, so its cost
does not grow with namespaces the filter excludes. Sessions without a member (downstream relays, recovery
sessions, member-less clients) report service `none`.
Events carry one counter, so "revision" and "sequence" are the same number.

Metrics carry the process's `tier` and `cell` plus at most `dataset`, `service` or placement `state`:
events and bytes in (from upstream) and out (queued delivery frames), full-queue disconnects, the
`relay_lag_seconds` histogram, subscribers by service, upstream revision lag (max and p99 across
namespaces), replay coverage, placement ranges by state, directory writes and frozen seconds, live follows and
recovery requests received, sibling streams connected, sibling interests followed and left cold, and failover
catch-up time: from a session announcing `Hello.moved` to each of its streams reaching the head. The only
per-namespace series are the top 20 namespaces by event rate and by revision lag, so the series count does
not grow with namespaces. Gauges are computed from the hub at scrape time.

- **End-to-end lag** is `now - committed_at` on the hub's injected runtime clock, observed once per event as it
  enters a relay, at every tier. `committed_at` is never rewritten. Per-subscriber lag is in revisions.
- **Revision lag behind upstream** compares the relay's received sequence with the head the upstream announces in
  `Range` frames, which it sends at follow and recovery. It shows catch-up, not a parent that stops mid-stream;
  the arrival lag and `committed_at` in `upstream` show that staleness.
- **Replay coverage** counts a subscribed namespace as covered when the cache is healthy, holds one contiguous
  range, and lost nothing that arrived within the window to capacity eviction or a rebuild. Age pruning is not a
  loss. Losses are tracked in memory: after a restart, a namespace whose cache evicted window records before the
  restart can count as covered until those records would have aged out.
- **Placement:** a range is `leaving` if any owner is, else `populating` if any owner is, else `ready`.
  Pushed interests, handoff and revoke counts wait for #22 / B-21's relay-driven handoffs.

Unit tests cover a constant series count at 100 and 100,000 namespaces, a stalled subscriber's rising lag and
queue depth, paging and filters, and window coverage. `tests/relays.rs` reads `/introspect` and `/metrics` over
admin HTTP through a writer → region → cell chain in both builds: the cell's namespace view matches what the
leaf applied, and lag is reported at both tiers. On 2026-11-02 in WSL Ubuntu every check below passed: native
tests 235 passed in 28 suites, simulation tests 197 passed in 28 suites.

## Checks and load measurement

Workspace checks run in WSL Ubuntu (native RocksDB builds use
`LIBCLANG_PATH=/usr/lib/llvm-18/lib`):

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim" cargo test --workspace
RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim" cargo clippy --workspace --all-targets -- -D warnings -A clippy::disallowed_methods -A clippy::disallowed_types
```

The simulation lint allowances match the workspace's documented harness checks;
normal lint retains the disallowed API checks. All checks above passed on
2026-10-30, including 165 native / 118 simulation shared-core unit tests and the
ten relay protocol integration tests in both builds. Opt-in load and real-S3
tests remain ignored by the ordinary workspace test command. With range streams
(2026-11-02, #22 / B-21 phase 2), every check above passed again: 235 native and
195 simulation tests in 28 suites each, including the range-follow integration test
in both builds. With member-side pushed ranges and per-dataset directories (phase 3,
same day), they passed again: 239 native and 199 simulation tests. With relay-side pushes and readiness (phase 4,
same day): 240 native and 200 simulation tests.
With sibling interests (#41 / B-12b, 2026-11-04), every check above passed: 307 native and 260 simulation tests in
29 suites each. In a temporary copy, each of these breaks failed at least one test: dropping the union, re-sharing
learned interests, no grace period (the failover then reaches the upper tier), a grace period that never expires, no
cap, and ignoring shared cursors.
For cleanup validation, native lint reused a cached RocksDB archive through the
dependency's supported `ROCKSDB_LIB_DIR` / `ROCKSDB_STATIC` settings; native tests
built RocksDB normally. This avoids rebuilding C++ for the Rust-only lint step.

TLC 1.8.0 checked `MaxRevision=4`, `QueueLimit=2` on 2026-11-03, with local storage loss: 776,387
generated states, 95,010 distinct states, depth 24, no invariant violation. The model separates
validated installation from the source's recovered confirmation, keeps pending
confirmation across disconnects and lease expiry, and checks `ReadyAfterConfirmation`:
readiness requires confirmation and coverage through head, and a new commit clears it.
This is the bounded safety model above, not a proof of the Rust implementation or a
liveness proof. As negative checks in ignored temporary copies, replacing
selected-payload application with a no-op makes `FilteredCoverage` fail at covered
revision 2 (2026-10-30), and dropping `~pending` from `BecomeReady` makes
`ReadyAfterConfirmation` fail at depth 4 (2026-10-31), as does letting `Commit` keep
readiness, at depth 3 (2026-10-31). Dropping `~lost` from `Confirm`, so a lost receiver
could confirm without a fresh install, makes `ReadyNotLost` fail at depth 11 (2026-11-03).

With range streams, pushed ranges and revokes, TLC `2026.10.28.014557` (rev `56d37a9`)
checked on 2026-11-02 with `QueueLimit=2` and every invariant above plus `ReportedHolds`:

| Namespaces | `MaxRevision` | Generated states | Distinct states | Depth | Result |
|---|---:|---:|---:|---:|---|
| `{"a", "b"}` (`relays.cfg`) | 2 | 15,397,970 | 1,679,867 | 28 | no violation (11 s) |
| `{"a"}` | 4 | 3,218,130 | 368,329 | 28 | no violation (3 s) |

Negative checks in ignored temporary copies, 2026-11-02, with `relays.cfg`: letting
`Report` count acknowledgements while pushed frames are unprocessed violates
`ReportedHolds` at depth 7, as does letting `Revoke` keep the reported readiness. The
earlier checks still fail on the new model: a no-op selected-payload application
violates `FilteredCoverage` at depth 10, dropping `~pending` from `BecomeReady`
violates `ReadyAfterConfirmation` at depth 9, and letting `Commit` keep readiness
violates it at depth 8.

With local storage loss merged into the range model (#23 / B-22, 2026-11-03), TLC 1.8.0 checked `relays.cfg` with
every invariant above plus `ReadyNotLost`: 26,865,595 generated states, 2,705,822 distinct, depth 28, no violation
(31 s). A revoke deletes the range, so it also clears `lost`. Letting `LoseLocal` keep `pending` unchanged violates
`ReadyNotLost` (840 distinct states).

With the [official TLA+ release](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0)
jar saved as `.codex-tmp/tla2tools.jar`, run from `docs/spec`:

```sh
java -XX:+UseParallelGC -cp ../../.codex-tmp/tla2tools.jar tlc2.TLC -workers 1 -metadir ../../.codex-tmp/tlc-relays -config relays.cfg relays.tla
```

The focused suites cover selector changes, filtered coverage, corruption, restart,
eviction, storage failure/rebuild, subscription cancellation, two-tier forwarding,
pruned-cursor bootstrap, and GC protection. A range follow through a writer, a top
relay and a cell relay receives only its hashes in every namespace, picks up a
namespace created after it followed, and resumes after a restart
(`range_follow_streams_every_namespace_and_only_its_hashes_through_two_tiers`).
`tests/sim/tests/relays.rs` runs seeds
1, 2 and 42 twice and compares committed application traces through idle partitions,
node kills, all-open routing and a KV-only process-task abort on a relay machine.
Replay one seed with `BEDROCK_RELAY_SEED=42` and the documented simulation flags.
`tests/sim/tests/failover.rs` kills relays among three siblings at cell, DC and region tiers (seeds 1, 2 and 42, each
twice), checks the upper tier sees no new follow or recovery, and that a crashed sibling's interests last the grace
period. Replay one seed with `BEDROCK_FAILOVER_SEED=42`.
`relay_idle.rs` separately verifies healthy idle operation and one-way request
partition failover through the shared session client. The snapshot/concurrent-write
and HTTP write suites also use this client; they do not keep a second test transport.

Run the opt-in native benchmark with:

```sh
BEDROCK_RELAY_LOAD_SECONDS=60 cargo test --release -p bedrock-lib --test relay_load -- --ignored --nocapture
```

It uses 1,000 independent loopback subscribers, each following one of 1,000 equal hash ranges,
256-byte values, a committed-log fixture paced at 10,000 events/second, and a real
RocksDB replay store capped at 16 MiB. This isolates relay capacity from writer
persistence. `BEDROCK_RELAY_LOAD_WRITER=1` instead exercises the materialized memory
writer with at most 256 pending commits; `BEDROCK_RELAY_LOAD_MEMORY=1` substitutes
the memory replay adapter. `BEDROCK_RELAY_LOAD_RATE` changes the offered rate for
overload probes. It reports actual rates, lag, RSS, catch-up and errors.
Neither mode establishes real-S3 writer throughput or WAN capacity. Normal tests
leave this hardware measurement ignored.

Measured on 2026-10-30 in WSL Ubuntu, optimized build, eight runtime workers, with the earlier bucket selectors (#22 /
B-21 replaced them with hash ranges on 2026-11-02; the benchmark has not been rerun since):

| Source | Injection window | Source events/s | Delivered events/s, including final drain | p99 lag | Subscriber errors |
|---|---:|---:|---:|---:|---:|
| Committed-log fixture | 60 s | 9,999.6 | 9,925.4 | 580 ms | 0 |
| Materialized memory writer | 10 s | 6,540.6 | 6,517.2 | 115 ms | 0 |
| Committed log, 40k/s overload | 10 s | 39,989.1 | 10,689.4 | ≥5,000 ms (clipped) | 0 |

The 60-second relay run delivered all 599,979 events to their selected recipients;
all 1,000 cursors reached the same final revision. Final drain took 0.449 seconds,
p50/p95 lag was 126/510 ms, retained replay was 16,503,077 bytes, and peak process
RSS was 522,540 KiB. RSS includes the authoritative fixture's growing event history
and all 1,000 clients, so it is not an isolated relay RSS or a memory-plateau proof.
Relay queues/recent history/replay have the independent bounds above. These results
demonstrate approximately 10k relay events/s; the separate memory-writer path was
source-limited and does not establish a 10k write-API rate.
The overload probe delivered all 399,928 events but needed another 27.412 seconds
to drain after injection stopped. This shows saturation around 10.7k delivered
events/s in this fixture, rather than sustainable 40k/s capacity. Peak process RSS
was 419,420 KiB and replay remained below its 16 MiB limit. The five-second latency
histogram clipped all reported overload percentiles; they are lower bounds. A finer
search between the stable 10k/s run and the overloaded 40k/s run was not performed.

The full B-23 gate, nightly history and real deployed crash/fault acceptance remain
separate work.
