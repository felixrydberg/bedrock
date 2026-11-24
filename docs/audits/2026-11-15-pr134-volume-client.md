# Audit — #134 volume client in Compute (S-04)

    grep -rn "^status: open" docs/audits      # what still needs an answer
    grep -rn "pr: 134" docs/audits            # everything about this PR
    grep -rn "F3" docs/audits                 # one item and everything citing it

pr: 134
issue: 112 / S-04
base: 24581fc
head: b9c95015
audited: 2026-11-15
kind: technical-decision audit — read-only, independent; not a code review, no verdict
open: 5
principles: ARCHITECTURAL-PRINCIPLES.md, still on its suggested defaults; P-* ids cited below
not-verified: whether the madsim acceptance scenarios were run; whether the four "user decision 2026-11-15" calls were actually made; any deployment or traffic behaviour

## What the PR does

The holder side of a stateful actor: per-host heartbeat set and permanent self-fence; open as
reserve → claim → recover the most advanced log → commit a no-op at the new epoch; one append task
per slot with a 128-entry / 8 MiB in-flight window and a 2-of-3 commit point gated on the no-op
(Raft Fig. 8); reads and scans at the commit point from the nearest eligible slot; one additive wire
field (`Slot.flushed_epoch = 10`) so a claimant can still tell a lagging slot what epoch precedes
the entry it resends after truncation. New library crate `bedrock-compute`, workspace dependencies
only, no persisted format change, no runtime change to existing binaries.

## Ledger — decisions made

C1  New crate `bedrock-compute`, `bedrock-lib` only, no binary until S-07.      crates/compute/Cargo.toml:9-12
C2  One heartbeat set per host, refcounted per writer; fence is permanent at 4 s
    against the server's 5 s lease.                                             crates/compute/src/actors/volume.rs:47-48,246-339,520-533
C3  Commit on two voting slots gated on the claim's no-op; per-slot tasks;
    128 entries / 8 MiB in flight; 4,096 entries / 64 MiB retained.             crates/compute/src/actors/volume.rs:51-64,229-243,457-489
C4  Reads at the commit point from the nearest slot; may return the writer's
    own uncommitted entries; hiding them is S-07's output gate.                 crates/compute/src/actors/volume.rs:17-21,914-946
C5  Oversized replies handled in the client: split read, halve scan, raised
    decode limit only for recovery (~192 MiB worst case).                       crates/compute/src/actors/volume.rs:69-72,918-928,960-969
C6  Additive `Slot.flushed_epoch = 10`; TLA model deliberately unchanged.       crates/lib/proto/volume.proto:103-105; docs/spec/volume.md:91-113

## Assumptions — what breaks if wrong

D1  A read may expose this writer's own uncommitted values.
    where: crates/compute/src/actors/volume.rs:17-21
    failure: a caller reads a value a later takeover truncates away; nothing in this crate reports it.
    tag: VERIFIED (declined CodeRabbit 4578654954)

D2  No slot is truncated while this client serves reads.
    where: crates/compute/src/actors/volume.rs:1030 vs crates/volume/src/store.rs:611-613,637-639
    failure: after S-05 flushes, `read` (any key in the batch) and `scan` (always) return
             Unimplemented through that slot; `nearest()` treats it as terminal, so the read fails
             even when a sibling slot still holds the value.
    tag: VERIFIED

D3  FENCE (4 s) < server LEASE (5 s).
    where: crates/compute/src/actors/volume.rs:48 vs crates/volume/src/serve.rs:32
    failure: inverted, a new holder may claim while the old still serves; its appends fail
             NOT_PROMISED but reads can hit a stale slot until the fence lands.
    tag: VERIFIED, checked only by a madsim scenario CI does not run

D4  Exactly three slots and a majority of two.
    where: crates/compute/src/actors/volume.rs:236-243, 529, 692, 717, 747, 821
    failure: `SLOTS` is imported but every quorum decision hardcodes 2; a different slot count
             silently computes the wrong commit point.
    tag: VERIFIED

D5  A lagging slot is worth at most 64 MiB of retained log per open actor.
    where: crates/compute/src/actors/volume.rs:61-64
    failure: holder memory is bounded only by actor count when entries are large or a slot lags.
    tag: VERIFIED

D6  The caller supplies every slot cell's `volume` routing table.
    where: crates/compute/src/actors/volume.rs:120-156
    failure: partial map yields `Error::Unavailable` (no silent guess), but nothing here fetches it.
    tag: VERIFIED

## Challenges — decisions worth a conversation

## F1 — Reads may return data that later never commits, and the fix belongs to an unbuilt component
status: open
decision: C4
defensible: the overlay is where a slot's truth is; serialising reads against pending commits would couple read latency to commit latency; the design already puts an output gate in the actor host (docs/implementation-prototype/statefull/v2.md:182).
contestable: the client is the only component that knows which sequences are uncommitted and it drops that knowledge at the API boundary — values come back without sequences, so nothing downstream can separate committed from pending. Two owners for one question (P-DATA-1, P-PAT-5).
cost: a read during an in-flight commit can return a value a takeover discards, silently; the only accepted evidence is a code comment naming S-07.
settled-by: a test that reads concurrently with a commit and states what a caller is entitled to see; or an S-07 note showing its cache can reconstruct "pending" for keys this writer never wrote (a previous holder's pending entries are invisible to it).
question: until S-07 lands, is there any consumer of `Writer::read`, and if not should it refuse to answer above the commit point?

## F2 — Oversized replies are managed in the client, not the server
status: answered
answer: 2026-11-24 — #126 / S-18 adds server byte bounds and matching 256 MiB transport limits. A real HTTP/2 check recovered two 209,709,211-byte entries across pages; the measured page was 209,709,276 bytes. Whole-actor recovery memory remains a separate limit. See [S-18 validation](../implementation-prototype/statefull/s18-validation.md).
decision: C5
defensible: keeps the freshly merged server untouched; splitting was CodeRabbit's own stated fallback.
contestable: the sizing knowledge ends up on the wrong side — `MAX_ENTRY_BYTES = 3 MiB` exists only to stay under tonic's 4 MiB default, the recovery decode limit is ~192 MiB, and a split read is two snapshots, not one. Diverges from the pattern the server already uses for `Scan`, clamped to 1,000 keys (P-CPLX-2, P-PAT-1).
cost: recovery of an actor with 3 MiB entries can buffer ~192 MiB in one message at each end; read/scan can add up to log2(n) round trips under load; the server may still emit unbounded `Log` replies to any caller.
settled-by: the measured largest recovery page from S-18's size work; or a server-side byte bound plus one matching client limit.
question: what is the largest single message you are willing to have buffered on a holder?

## F3 — The writer-lease relation is prose between two crates
status: open
decision: C2
defensible: 1 s beats, 4 s fence, 5 s server lease is the design's own number (docs/implementation-prototype/statefull/v2.md:149-152), and the madsim scenario asserts that a second holder opens only after the first stops serving.
contestable: `FENCE` and `LEASE` are `pub const`s in crates that cannot import each other, the doc comment repeats the literal "5 s", there is no compile-time assertion, and the only behavioural check runs where CI does not (P-CPLX-4, P-MAINT-1).
cost: tuning either constant can invert the ordering with a green CI run; the consequence is two live holders for a read window, not lost commits.
settled-by: `const _: () = assert!(FENCE < LEASE);` in `bedrock-lib`, with both crates reading the budget from there.
question: should the lease budget live in `bedrock-lib` as a protocol constant instead of twice as prose?

## F4 — The acceptance evidence lives outside CI and outside the PR
status: open
decision: C3 (and the test strategy)
defensible: madsim does not build on Windows and the repository runs it manually (scripts/vopr.sh:2); CI has no TLC step either.
contestable: all four #112 acceptance criteria sit in one `#![cfg(madsim)]` file (tests/sim/tests/volume_client.rs:15), the PR body and all six commit messages record no command or check result, and a future break there never turns anything red. The normal-build test CI does run covers only reply splitting (P-MAINT-4, P-DOC-1; house rule AGENTS.md:31).
cost: the strongest guarantees in the PR are unpaid for by any gate, and the next person to touch the lease or window logic has no way to know what they broke.
settled-by: one seed sweep recorded against the PR or in the branch; or a scheduled workflow.
question: were the sim scenarios run on this branch, and where is the seed output?

## F5 — A wire field ships with the model unchanged
status: open
decision: C6
defensible: the argument is in the specification doc rather than a review thread and is substantially right — the model never lets a slot lose log entries, so the field adds no algorithm. It also surfaces a silent debt: truncation shipped in #133 with no model coverage, and the new section says so.
contestable: two overreaches. `docs/spec/volume.md:108` claims a slot "still refuses the append with LOG_GAP, as in the model" — the model has no append refusal; `Replicate` overwrites the slot log whenever the promise matches (docs/spec/volume.tla:130-141). And the recovery path the field enables (holder log starting at flushed+1, carrying base/base_epoch) is outside the model's state space, whose log is indexed by absolute sequence (docs/spec/volume.tla:217-219) — "its checks are unchanged" is true because the checks never reached it (P-MAINT-2, P-DOC-2 against AGENTS.md:30).
cost: whoever models truncation in S-05 inherits a doc claiming the model already covers the refusal, plus an invariant set never shown to hold over a truncated log.
settled-by: a `Truncate` action and a holder base offset in volume.tla, plus a TLC run of volume-base and volume-wipe.
question: is S-05's model work scheduled before anything relies on truncated-slot recovery?

## F6 — Two scope items are reduced and the only record is a code comment
status: open
decision: C1 (placement) and C3 (topology transitions)
defensible: sibling `volume.{cell}` tables need a relay session S-07 owns; joint-majority transitions only matter when a cell is added or removed (docs/implementation-prototype/statefull/v2.md:204-211).
contestable: the attribution is thin. "user decision 2026-11-15" appears four times in the new code (crates/compute/src/actors/volume.rs:21,23,26,57) and nowhere else — not in #112, whose own bullet list includes fetching sibling-cell tables through `Relay.Routes`; not in #115; not in docs/. AGENTS.md:8 wants contradictions with recorded decisions reported before affected behaviour is built; AGENTS.md:71 wants issue scope changes as an amendment comment (P-DOC-2).
cost: a later reader finds an issue whose acceptance list is unmet and a client that skips two items, with no record of who decided or where the work went.
settled-by: an amendment on #112 naming both deferrals and the issues that receive them.
question: were those four decisions taken on 2026-11-15, and should they be amended onto #112?

## Behavioural changes (for future archaeology)

- Volume answers now carry field 10; additive, ignored by older readers.
- `tests/sim` compiles `bedrock-volume` and `bedrock-compute` in normal builds too, so `cargo test --workspace` runs one real-tonic loopback test.
- A read of a flushed slot returns `Error::Rpc(Unimplemented)`; nothing falls through to a sibling slot.
- No new config keys, no migrations, no metrics or logs added, no runtime change to existing binaries.

## Update convention

Answer an item by changing its `status:` line — `open` → `answered` / `accepted` / `moot` — and adding
an `answer:` line with date, decision and where the reasoning lives. Never renumber ids; a superseded
item gets `status: superseded` and a `superseded-by:` line. Keep the `open:` count in the header
truthful, and append new audits to `docs/audits/` with the same shape so one grep spans all of them.
