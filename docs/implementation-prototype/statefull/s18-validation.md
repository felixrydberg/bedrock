# Customer KV actors — #126 / S-18

## Implementation and scope

Each namespace has one built-in Rust `kv` actor. Its ID is the home region plus the first 128 bits of SHA-256 of its
namespace ID in the existing base32 actor format. It uses `Executor`/`Instance` and volume key/value storage, with no
SQLite or sandbox. A write or bulk is one quorum-acknowledged volume entry containing final values, a separate namespace
revision, exact customer-storage accounting, the original retained event, and an optional retry result. Claim, release,
and compaction entries do not allocate namespace revisions. Retry results retain the original revision, timestamp and
successful-key count for 24 hours; events retain their payload for 60 seconds. Expired values read as absent and bounded
event-triggered cleanup reclaims them without advancing the namespace revision.
The storage quota charges each customer key and its encoded value/metadata/expiration envelope once, matching #74's
live-byte contract. Overwrites subtract the old charge; expired rows remain charged until removed. Internal retries,
events and replicated copies are excluded from that customer quota.

`bedrock-kv` serves the existing bearer-authenticated namespace API plus value, metadata, bulk and key-list routes.
Single-key reads use the cell's HRW-selected cache; cache misses go through the Gateway to the holder. Concurrent misses
coalesce on that serving cache node, using the existing bounded recent-state limiter. Cache TTL starts before the source
fetch and is capped by value expiration. Absence is not cached. Bulk reads use one holder event so independently cached
keys cannot expose half of an atomic bulk. Minimum revisions prove namespace progress for values, absence and listing.

The obsolete per-namespace S3 reader path and its dedicated KV reader tests are retired. Generic materialized-reader
tests and volume range-reader tests remain. Namespace `storage = kv/{id}` stays a logical directory identity; actual
actor data joins volume range databases. Purge tombstones the actor before deleting its directory record, and volume's
own deletion grace governs physical reclamation. The dev API refuses direct `kv` writes.

The shared binding contract supplies actor writes through `lib::kv::call` and HRW cache reads through `lib::kv::get`.
Actual Worker sandbox invocation remains S-10, as agreed for this issue. The S-20 binary fixture uses a test D1 handler
instead of its former dummy KV counter, because the Compute binary now registers the real KV handler.

## Byte bounds and measurement

| Boundary | Limit |
| --- | --- |
| Customer value | 25 MiB |
| REST body / decoded binding mutation payload | 100 MiB each |
| Mutations per bulk | 10,000 |
| Encoded volume entry | 224 MiB |
| Uncommitted append window | 256 MiB and 128 entries |
| Volume recovery/scan/repair response page | 224 MiB + 64 KiB, with an atomic first record |
| Actor/volume RPC encoding and decoding | 256 MiB |
| Retained committed client log | 64 MiB, except its last complete committed entry |
| Cold flush selection | 4 MiB of complete log entries, except one larger atomic entry |

The retained event contains the original mutation payload in addition to current values, so the largest atomic entry
is about twice the customer payload. The former 3 MiB entry cap and 8 MiB append window could not support the agreed KV
limits. Keeping the latest committed entry is necessary even when it exceeds the retained-log budget: a commit-only
append must still announce that commit to idle slots. Cold flush now streams a bounded prefix rather than eagerly
copying the entire unflushed tail before applying its batch budget.

The normal HTTP/2 size test sends a full 25 MiB value within a nearly 100 MiB bulk plus its retained event, reads the
values back from real volume services, and checks paging across two such log entries. On 2026-11-24 the entry encoded
to 209,709,211 bytes and the recovery page to 209,709,276 bytes. The first quorum commit took 1,404 ms and the four-value
read took 514 ms (debug build, loopback HTTP/2, three in-memory slot stores, two runtime workers). Both entries were
recovered across separate pages. This is a local adapter measurement, not a latency SLA or Frankfurt-to-R2 result.

**Existing resource limit:** append and RPC-page bounds do not bound a whole actor's recovery or range repair. Those
paths still accumulate an entire actor across pages; a long S3 outage can leave a log approaching the existing 16 GiB
node backpressure limit, and a large flushed actor can make repair require substantial memory. Streaming/spooling those
paths is separate shared-core work. This change does not claim a whole-actor memory bound.

## Validation and remaining deployment checks

The results in this section describe the original implementation at `6352e93`, before the review corrections below.

The focused simulation runs the real API, relays, Quicksilver, Gateway and KV nodes with Compute actor hosts and three
durable simulated volume services. The slot placement is fixed and cold maintenance is omitted to isolate write-path
S3 dependence. It covers retries after holder movement, ambiguous minority writes, bulk/quota isolation, minimum reads,
listing, TTL, cache/REST operations, and idempotent namespace actor deletion. Its customer S3 adapter is explicitly
failed after bootstrap; control-plane storage remains available.

On 2026-11-24, the eleven focused madsim integration targets passed (sixteen tests): `kv_actor`, `namespaces`,
`compute_binary`, `actor_host`, `actor_routing`, `actor_dc_change`, `actor_dc_restore`, `volume`, `volume_client`,
`volume_cold`, and `volume_repair`. Each target's documented seed/replay cases ran; the new KV scenario checks seeds 1
and 2 twice each. The large debug fixture uses a 64 MiB orchestration stack, like the existing volume-cell scenario.
The host rollback fixture now exceeds the actual 224 MiB entry cap rather than the retired 3 MiB cap.

Simulation-compatible unit checks also passed: eight built-in KV contracts, eighteen KV frontend/cache/directory
contracts, the latest-large-entry retention regression, complete-entry cold-flush selection, and the independent
actor-ID vector. `cargo tree -p bedrock-sim -e normal` under `--cfg madsim` contains no RocksDB dependency.
An attempted full Compute unit run under madsim found four existing lease tests that construct a blocking Tokio
runtime, unsupported by madsim; those tests run in the normal workspace suite, and their simulated I/O coverage is in
the passing host/client integration targets. This is not a claim that every workspace unit test supports madsim.

Both normal HTTP/2 size contracts passed, as did the real SlateDB/object-store adapter contract that flushes,
truncates, gets and scans a full 25 MiB encoded KV value. All Rust builds use two jobs; focused simulation tests use one
test thread, and normal tests use at most two. Normal and madsim targets are separate and unique to this checkout.

The final normal checks passed on 2026-11-24: `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -j 2 -- -D warnings`, and `cargo test --workspace -j 2`.
Workspace/all-target Clippy also passed with `RUSTFLAGS="--cfg madsim" RUSTDOCFLAGS="--cfg madsim"`.
Cleanup removed this checkout's `~/bedrock-126-b840` and `~/bedrock-126-b840-madsim` targets (about 35 GiB), its TLC
state directories and its temporary validation logs/helpers. No sibling targets, Docker artifacts or sccache data were
removed; no VHDX compaction was performed.

The shared-core VOPR checks passed on 2026-11-24: `scripts/vopr.sh quick` (six tests, including retained seeds and
deliberate bugs), a 500-seed sweep (seeds 1–500, two CPU-bound workers, 141 seconds wall time), and replay of seeds 1 and
42 in two processes each. The sweep covered 41,356.963 simulated seconds and 828,093 acknowledgements with no failed
seed. These are the existing shared-core workloads, not the unavailable S-16 actor crash matrix. The single-threaded
test harness prefixes its VOPR marker, so the script's sweep summary printed zero simulated seconds; all 500 logs were
independently checked for success and their markers aggregated. Replay passed with `RUST_TEST_THREADS=2`, which keeps
markers on their own lines. A temporary Git command wrapper translated this Windows-created worktree's metadata path
for Linux Git; neither shared harness files nor Git metadata were changed.

Full B-29 deployment acceptance remains blocked by [#148 / S-20b](https://github.com/felixrydberg/bedrock/issues/148)
(sibling-cell volume tables, dependent on #103 / B-62) and
[#124 / S-16](https://github.com/felixrydberg/bedrock/issues/124) (actor simulation/crash harness). This includes the full
three-cell loss and real deployment crash matrix, dashboards and remote latency/compaction measurements. The user
explicitly kept S-18 focused on the implementation and three-slot checks pending those issues.

The full TLA+ configuration remains `kv_actor.cfg`: three revisions, four time units and two holders. Its initial
two-worker, 4 GiB run was stopped at 24,061,202 distinct states with 10,318,417 queued; it did not finish. The separate
`kv_actor-local.cfg` checks two revisions at the same time/holder bounds and carries all fourteen deliberate bugs.
Subsequent local model checks use one worker after that interruption. No full-bound result is claimed.
The local run passed on 2026-11-23 with 10,188,440 distinct states at depth 16 in 12 minutes 46 seconds; all fourteen
deliberate bugs broke their named invariants (one worker, 4 GiB heap).

## Review corrections

The namespace-management bearer credential is the existing administration authority; data-plane tenant text is not
permission. Purge now publishes only a random grant's SHA-256 hash through the management API's authenticated CAS.
The secret grant travels through the existing actor request, and Compute checks its preimage, tenant, canonical actor,
Deleting state and grace before deletion. It is never stored in the readable directory. Direct bindings cannot issue
grants. The same secret can retry an ambiguous deletion safely against the permanent actor tombstone.

The user selected known-revision fencing rather than a new authoritative read barrier. A shared `kv::DirectoryView`
retains observed infrastructure progress per process, including management publications; actor requests and cache RPCs
propagate that floor. REST reads/list/bulk get accept `directory_min_revision`, and value writes accept the
`bedrock-kv-directory-revision` header. A supplied future floor cannot poison other requests: only successful reads
and authenticated publications raise observed progress. A deletion is enforced at its publication revision; a process
that has not observed it and a caller without its floor still has ordinary control-plane propagation delay.

Lowering storage quota now permits nonincreasing writes while usage is above the limit. Rejected growth still commits
expired-row/event/retry cleanup with no namespace revision or committed-time change. Old-row reads are matched by key
in batches of eight: the requested single lookup for all 10,000 replacements could accumulate about 250 GiB of old
values even for a small incoming bulk. These batches bound collected old values while avoiding one RPC per key.

The review also fixes cancellation during actor deletion, case-insensitive multipart detection, combined hot/cold
reply budgets, encoded merged-scan paging, and continued cold flush of complete committed prefixes. Strengthened
acceptance probes held S3 handles after fault activation, actual purge of an unpurged actor, retained directory state
after actor-purge failure, warm-cache deletion before TTL, ambiguous-bulk values after retry, and both recovered bulk
entries with their retained events. The models now check staged retention completeness and retry response timestamps,
permit bounded flush prefixes, and separately model the grant/floor admission policy. Original model results above
are historical; updated review validation is recorded after the fresh checks.

Fresh model checks on 2026-11-24 passed with one worker and a 4 GiB heap: strengthened KV local (10,188,440 states,
17 bugs), admission (8 states, 2 bugs), and cold move (400 states, 2 bugs). Separate temporary smoke bounds passed
quota-7 overwrite (2 revisions / 1 time / 2 holders, 215,752 states) and cold split (2 maintainers / 2 sequences /
1 generation / 1 epoch, 174,573 states). A negative reachability check proved an accepted bulk can overwrite committed
data. All three cold-split bugs broke at the original full bounds. The full split base and the larger quota-7/three-
revision configurations were not run. See the model documents for exact commands and historical results.

The review's normal `cargo fmt --all -- --check`, workspace/all-target Clippy with `-D warnings`, and full
`cargo test --workspace` passed on 2026-11-24 (two jobs). The first madsim run could not be verified after the approval
service hit a usage limit and WSL restarted, clearing `/tmp`; it is not claimed as passing. Following the interruption
rule, the repeat on 2026-11-25 used one build job and one test thread. Madsim workspace/all-target Clippy passed,
as did 82 volume unit tests, 18 KV unit tests, nine built-in KV contracts and all thirteen tests across eight focused
integration targets (`actor_host`, `actor_routing`, `compute_binary`, `kv_actor`, `namespaces`, `volume_client`,
`volume_cold`, `volume_repair`). Their seed/replay cases ran; KV seeds 1–2 and namespace seeds 1, 2 and 42 replayed.
The madsim dependency tree again excludes RocksDB. Review targets remain separate from normal builds and every other
checkout; subsequent heavy checks use the reduced one-job/one-worker cap.

Eight mutation negative controls on 2026-11-25 failed for their intended regression assertions when the corresponding
fix was temporarily disabled: combined read budget, merged scan budget, partial-flush continuation, multipart media
case, purge grant, reduced quota, rejected-write cleanup and delete cancellation. Each repaired contract passed after
restoring the exact source bytes. These are controlled mutations of the integrated patch, not a claim that the new
tests compiled against the old APIs unchanged. No production test-only flags or alternate runtime seams were added.

The final shared-core VOPR checks passed on 2026-11-25: all six quick checks (retained seeds and deliberate bugs),
two-process replay of seeds 1 and 42, and all 500 seeds 1–500, covering 41,356 reported simulated seconds with no
failure. The run used one CPU, one build job, one test thread and one seed worker after the interruption. It reused
the checked-in harness functions with libtest's terse output solely to keep replay markers parseable at one thread;
the shared harness itself was not edited. The 717-second sweep wall time includes a hold between seeds while another
checkout ran workspace checks. No seed was interrupted during that hold. Full deployment and larger-model limits
remain as recorded above.

Cleanup on 2026-11-25 removed the review's two unique target directories (`~/bedrock-126-b840-review1` and
`~/bedrock-126-b840-review1-madsim`, about 36 GiB), temporary seed reports/logs/helpers and generated TLC states.
Sibling targets, shared harness files, Docker artifacts and sccache were left untouched. No VHDX compaction ran.
