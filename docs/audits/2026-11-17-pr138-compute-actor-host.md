# Audit — #138 Implement compute actor host with SQLite storage and execution seam

    grep -rn "^status: open" docs/audits      # what still needs an answer
    grep -rn "pr: felixrydberg/bedrock#138" docs/audits

pr: felixrydberg/bedrock#138
issue: #115 / S-07 (depends on #112 / S-04)
base: f7aa02b4e017 (main; merge-base 011ebd7c with the PR head)
head: 0fc59a37d0d4
audited: 2026-11-17
kind: technical-decision audit — read-only, independent; not a code review, no verdict
open: 9
principles: ARCHITECTURAL-PRINCIPLES.md, suggested defaults (unchanged); P-* ids cited below
not-verified: madsim scenario never run by me (not buildable on this Windows host; CI sets no --cfg madsim), cargo fmt/clippy/test not run locally, no production wiring traced (no binary/relay/Gateway exists), S-05 flush/truncation behaviour for page loss

## What the PR does

Builds the Compute-side actor host on top of the existing volume client: one instance per actor, on-demand
claim/wake, sleep by Release after 10 s idle, cancellation-aware gating of storage and responses, key/value plus
SQLite-over-volume storage, and the Executor/Instance execution seam. It adds no binary and no relay membership,
so nothing outside tests constructs the host. Everything is confined to crates/compute, manifests and one madsim
test.

## Ledger — decisions made

C1  An event-level failure escalates to actor-level teardown; a cancelled commit fences the storage   host.rs:322-345,358-378; storage.rs:54-61,97-103@0fc59a3
C2  One transaction is one volume entry, awaited, never pipelined; ~750-page ceiling                  storage.rs:9-14,166-174; volume.rs:56,855-912@0fc59a3
C3  SQLite pages are plain volume keys in the same key space as key/value                             storage.rs:4,37-40,301-321@0fc59a3
C4  Sync-VFS-over-async-client bridge: page cache + rerun the transaction after a 64-page fetch      storage.rs:5-8,107-178@0fc59a3
C5  The execution seam lands in S-07 and carries actor storage; Worker arm has no consumer           runtime.rs:13-14,52-58,76-84; host.rs:33@0fc59a3
C6  Host ships as a library behind a Placement port; binary and relay membership deferred            host.rs:38-43,191-199; lib.rs:1-6@0fc59a3

## Assumptions — what breaks if wrong

D1  A handler's Storage clone never outlives its instance, so Release is appended
status: open
    where: host.rs:240-246,348-355; storage.rs:85-87; volume.rs:301-341@0fc59a3
    failure: no Release is written and other hosts' claims are refused as a live lease while the clone lives; actor never sleeps or migrates, and the omission is logged at debug only
    tag: INFERRED

D2  Nothing uncommitted is read or cached: one event at a time plus an awaited commit
status: open
    where: storage.rs:126-167; storage.rs:13-14@0fc59a3
    failure: a handler-spawned second sql call can commit pages from another attempt, and a later failure drops the database after the fact
    tag: INFERRED

D3  Every page below the size key is readable from the slots
status: open
    where: storage.rs:159-163@0fc59a3
    failure: one lost page makes the actor unreadable forever; every wake fails with DataLoss
    tag: VERIFIED code / UNKNOWN reachability (depends on S-05)

D4  The SQL closure may run more than once and does nothing but SQL
status: open
    where: storage.rs:6-8,107@0fc59a3
    failure: side effects inside the closure happen twice; mutated captured state is observed across attempts
    tag: VERIFIED (doc) / INFERRED (consequence)

D5  KV keys and SQL keys cannot collide because an actor uses one dialect
status: open
    where: storage.rs:21,37-40@0fc59a3
    failure: a customer key of sqlite/size or a page number aliases the database; nothing enforces the split
    tag: VERIFIED

D6  SystemTime::now() is simulated under madsim, so committed_at replays
status: open
    where: storage.rs:196-202; vendor/madsim/src/sim/time/system_time.rs:3-20@f7aa02b
    failure: seeds stop replaying where madsim does not override the clock
    tag: VERIFIED on unix targets

## Challenges — decisions worth a conversation

F1 — A cancelled request can take down a healthy actor
status: open
decision: C1
defensible: an instance whose memory may differ from the log is the failure the lease exists to prevent; fencing is conservative and recorded in commit 8cde4c3
contestable: the guard is created before the instance gate, so a request cancelled while queued fences shared storage and converts a running event's success into Fenced (P-PAT-5, P-CPLX-1)
cost: every cancellation costs a full re-claim and re-runs the event; concurrent callers can fail each other; no Release on the cancel path, so another host is refused until the old host stops being seen (~5 s if this was its last writer there, longer otherwise)
settled-by: a madsim test that cancels a queued request while another event is mid-commit and asserts the running response survives; a two-host test where the cancelled actor shares a node
question: should an event that never entered Instance::invoke fence shared storage, and should the teardown path append Release?

F2 — Durable layout with no version, no owner, and a shared key space
status: open
decision: C3
defensible: minimal keys aligned for 64-page chunk reads; the volume treats keys as opaque
contestable: no version marker, migration story or owner anywhere in the PR, commits, issue or docs; the KV/SQL split is a code comment and sqlite/size is an obvious colliding KV key (P-DATA-2, P-MAINT-1)
cost: the first encoding change is a migration across every actor in every range database; S-10's hidden-table KV will mix dialects in one actor
settled-by: a recorded owner plus a version or prefix decision, and a test opening a database written by an older encoding
question: who owns the actor storage format, and what is the migration path when S-10's KV API becomes a hidden SQLite table?

F3 — One quorum round trip per transaction and a ~750-page ceiling
status: answered
answer: 2026-11-24 — #126 / S-18 preserves one atomic entry per transaction and raises its cap to 224 MiB, with a 256 MiB byte-bounded append window. The user approved retaining 25 MiB values and 100 MiB bulks. Real HTTP/2 committed a 209,709,211-byte bulk-plus-event entry in 1,404 ms locally; an oversized SQLite transaction still rolls back in the host simulation. See [S-18 validation](../implementation-prototype/statefull/s18-validation.md); remote latency remains pending deployment checks.
decision: C2
defensible: simplest construction that satisfies "no response precedes its commit"; limits recorded as known limits in the issue amendment
contestable: the ceiling lands before D1/Queues exist; TooLarge is a permanent error, not backpressure (P-CPLX-2, P-COMPAT-1)
cost: one commit per quorum RTT per actor; large batches fail rather than split; a protocol change is far cheaper now than after customers exist
settled-by: a measurement of S-18-scale bulk writes against the 3 MiB entry limit (v2.md:586-587 already flags it)
question: is one entry per transaction a product limit we accept, or an implementation detail to revisit before S-11?

F4 — The execution seam is fixed before its owner exists
status: open
decision: C5
defensible: S-10 cannot start without an interface; the amendment assigns it to S-07; RustExecutor plus a stub sandbox proves the seam works
contestable: the seam carries a concrete Storage and a Worker arm with no consumer, so S-10 inherits a coupling and a placeholder; AGENTS.md:21-22 names a Sandbox contract this PR does not define (P-PAT-2, P-MAINT-3)
cost: if the sandbox needs streaming, WebSockets, alarms or a different lifetime model, S-10 bends the trait or adds the second path AGENTS.md forbids
settled-by: S-10's design sketch showing Instance::invoke carrying a DO WebSocket event and a Worker request without a second method
question: is this trait the shape S-10 will keep, and should Storage leave ExecutionSpec before more code depends on it?

F5 — Library-only landing with the issue's binary and AGENTS.md items deferred
status: answered
answer: 2026-11-22 — #147 / S-20 adds the `bedrock-compute` binary and its `compute` relay membership and updates AGENTS.md; sibling cells' `volume` tables are #148 / S-20b (`crates/compute/src/lib.rs`, `tests/sim/tests/compute_binary.rs`)
decision: C6
defensible: deferral is recorded with its reason (files S-06 owns) and its exact item list; the library boundary made the host testable
contestable: AGENTS.md:14 still reads "compute (planned, track O)" and was not touched, and the only consumer of the new public API is a test (P-DOC-4)
cost: the one-instance guarantee is proven in simulation only; the first production wiring will make placement and routing choices with no second review of host integration
settled-by: a follow-up issue naming the binary, relay membership and the AGENTS.md line, with a date rather than a sentence
question: where is the issue for the binary and the AGENTS.md update, and will it land before S-08 needs to route?

## Behavioural changes (for future archaeology)

- No customer-visible behaviour: nothing outside tests constructs ActorHost; no binary, relay membership or Gateway.
- Actors (future): wake on request; sleep after 10 s idle with a Release; stop answering within 4 s of losing two slots; a cancelled or panicking event stops the actor and the event re-runs on the next Acquire.
- Storage: actor SQLite databases are volume keys (page numbers + sqlite/size); a transaction over ~750 pages fails rather than splitting.
- Metrics: compute_actor_claim_seconds, compute_actor_wake_seconds, compute_actor_lease_expiries; labels cell, dc, kind; expiries conflates a lease lapse with a storage failure.
- Builds: rusqlite (bundled C SQLite) and sqlite-plugin (bindgen, libclang) reach bedrock-compute; tests/sim gains async-trait.
- Tests: three crate test files, the third authored and committed by coderabbitai[bot] (beta test generation, committed from the PR checkbox); the madsim acceptance test is not run by CI (ci.yml:10,50-52 vs tests/sim/tests/actor_host.rs:10).
- Unchanged: volume protocol, TLA+ spec, lib, relays and every other crate.

## Update convention

Answer an item by changing its `status:` line — open → answered / accepted / moot — and adding an
`answer:` line with date, decision and where the reasoning lives. Never renumber ids; a superseded item
gets `status: superseded` and a `superseded-by:` line. Keep the `open:` count truthful, and append new
audits to `docs/audits/` with this shape so one grep spans all of them.
Not verified here: the madsim scenario's pass (not runnable on this host, not run by CI), local
fmt/clippy/test, and any production wiring. The principles file's state is unchanged.
