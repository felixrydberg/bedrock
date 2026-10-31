# Relay protocol (#14 / B-17, #16 / B-18)

The Relay.Session protocol multiplexes pushed datasets over one persistent
subscription. Hello negotiates version 1 and the downstream tier (leaf, cell, DC,
region); Welcome identifies the source (writer, region, DC, cell). Live edges must
strictly increase tier. Recovery requests may visit siblings, carry a visited path,
and never create a live edge. This is the sole subscription transport; the
superseded B-10 Replication.Subscribe client, service and relay bridge were removed.
The writer's ordered log and change notifications remain shared primitives.

Each follow is a dataset/namespace plus a canonical bucket selector. An empty
selector means the whole namespace. Delivery covers an inclusive consecutive
namespace revision interval, including revisions whose payload belongs to another
bucket. Matching original events and the covered cursor commit atomically.
Selector identity is persisted with the cursor; changing it requires a new validated
baseline. Coverage never renumbers events or changes committed_at. Tick proves
connection liveness only and cannot advance applied state. Customer kv is rejected.

Replay is a disposable checksummed cache, never commit authority. Advertised ranges
are contiguous validated committed records. Eviction or disk failure removes range
claims but cannot block live fan-out. Catch-up may read peers and upstream sources.
Beyond authoritative retention, a coherent writer view is staged and checksummed;
only successful validation permits atomic installation. A bounded lease acquired
with the view protects the subsequent tail from GC through catch-up. Expiry or
disconnection aborts recovery. No readiness follows a partial or corrupt transfer.

relays.tla is a bounded safety abstraction: coverage is represented one revision at
a time, with alternating revisions modeling selector matches and omissions. Its
projection invariant requires all selected payloads through covered progress,
while unselected revisions still advance coverage. `projected` is ghost history,
not a physical snapshot log or tombstone set. Cached events are disposable and the
writer history is abstracted as recoverable while pinned. It checks consecutive
coverage, queue bounds and protected bootstrap installation through disconnects.
It does not prove payload routing, Rust correctness, disk checksums, fairness,
resource limits, or the complete B-23 gate. Implementation tests must cover those.
Run TLC with relays.cfg; record actual results separately.

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

KV/CDN `[[subscriptions]]` entries explicitly name dataset, namespace and optional
bucket IDs. Omitted/empty buckets follow the namespace. The KV binary permits
Quicksilver/Auth; CDN permits only CDN metadata. Customer `kv` opens/reads do not
create interests. Automatic ownership remains #19 / B-16 work; namespace interests here are explicit
configuration. Leaf sessions also carry [cell membership](membership.md) (#15 / B-12).
The leaf client stores values under namespace-NUL prefixes and persists its
selector identity and covered revision in the same local atomic batch. Changing a
selector restarts recovery from validated state rather than reusing coverage.
Call `Session::stop().await` before replacing or reusing its local storage; it
aborts and joins the applying task. Dropping the session requests cancellation.

Both directions send progress every 10 seconds on the existing session. A fixed
30-second incoming deadline detects one-way partitions even while outgoing progress
continues. Tick/progress alone never advances data or a revision. A receiver queue
is capped at 1,024 records and 4 MiB of serialized frames; control frames also have
a bounded channel. Outstanding revision coverage has a separate 1,024 window.
Full queues disconnect only that subscriber; exhausted acknowledgement windows
wait up to the idle deadline before disconnection. Encoded events and bucket hashes
are shared across recipients in a bounded 4 MiB/4,096-record recent cache.

Replay persistence has its own 1,024-record/4 MiB input queue and commits batches
of at most 256 records. A normal build runs blocking adapter writes on the runtime's
blocking pool; simulation uses the deterministic adapter directly. Overflow drops
disposable cache entries, leaving accurate gaps for upstream recovery, while live
forwarding continues. The worker holds at most one additional bounded batch.
Scoped recovery/storage errors gate only the affected namespace; healthy datasets
keep receiving updates and satisfying revision waits. Failed scopes retry recovery
on the existing progress interval rather than restarting healthy streams.

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
tests remain ignored by the ordinary workspace test command.
For cleanup validation, native lint reused a cached RocksDB archive through the
dependency's supported `ROCKSDB_LIB_DIR` / `ROCKSDB_STATIC` settings; native tests
built RocksDB normally. This avoids rebuilding C++ for the Rust-only lint step.

TLC 1.8.0 checked `MaxRevision=4`, `QueueLimit=2` on 2026-10-31: 436,101 generated
states, 59,694 distinct states, depth 24, no invariant violation. The model separates
validated installation from the source's recovered confirmation, keeps pending
confirmation across disconnects and lease expiry, and checks `ReadyAfterConfirmation`:
readiness requires confirmation and coverage through head, and a new commit clears it.
This is the bounded safety model above, not a proof of the Rust implementation or a
liveness proof. As negative checks in ignored temporary copies, replacing
selected-payload application with a no-op makes `FilteredCoverage` fail at covered
revision 2 (2026-10-30), and dropping `~pending` from `BecomeReady` makes
`ReadyAfterConfirmation` fail at depth 4 (2026-10-31), as does letting `Commit` keep
readiness, at depth 3 (2026-10-31).

With the [official TLA+ release](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0)
jar saved as `.codex-tmp/tla2tools.jar`, run from `docs/spec`:

```sh
java -XX:+UseParallelGC -cp ../../.codex-tmp/tla2tools.jar tlc2.TLC -workers 1 -metadir ../../.codex-tmp/tlc-relays -config relays.cfg relays.tla
```

The focused suites cover selector changes, filtered coverage, corruption, restart,
eviction, storage failure/rebuild, subscription cancellation, two-tier forwarding,
pruned-cursor bootstrap, and GC protection. `tests/sim/tests/relays.rs` runs seeds
1, 2 and 42 twice and compares committed application traces through idle partitions,
node kills, all-open routing and a KV-only process-task abort on a relay machine.
Replay one seed with `BEDROCK_RELAY_SEED=42` and the documented simulation flags.
`relay_idle.rs` separately verifies healthy idle operation and one-way request
partition failover through the shared session client. The snapshot/concurrent-write
and HTTP write suites also use this client; they do not keep a second test transport.

Run the opt-in native benchmark with:

```sh
BEDROCK_RELAY_LOAD_SECONDS=60 cargo test --release -p bedrock-lib --test relay_load -- --ignored --nocapture
```

It uses 1,000 independent loopback subscribers, exactly distributed bucket selectors,
256-byte values, a committed-log fixture paced at 10,000 events/second, and a real
RocksDB replay store capped at 16 MiB. This isolates relay capacity from writer
persistence. `BEDROCK_RELAY_LOAD_WRITER=1` instead exercises the materialized memory
writer with at most 256 pending commits; `BEDROCK_RELAY_LOAD_MEMORY=1` substitutes
the memory replay adapter. `BEDROCK_RELAY_LOAD_RATE` changes the offered rate for
overload probes. It reports actual rates, lag, RSS, catch-up and errors.
Neither mode establishes real-S3 writer throughput or WAN capacity. Normal tests
leave this hardware measurement ignored.

Measured on 2026-10-30 in WSL Ubuntu, optimized build, eight runtime workers:

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
