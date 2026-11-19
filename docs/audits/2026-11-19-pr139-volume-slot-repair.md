# Audit — #139 Repair volume slots and enhance node reporting for readiness

    grep -rn "^status: open" docs/audits      # what still needs an answer
    grep -rn "pr: 139" docs/audits            # everything about this PR
    grep -rn "F1" docs/audits                 # one item and everything citing it

pr: 139
issue: 114 / S-06
base: 011ebd7
head: 6d11b0af
audited: 2026-11-19
kind: technical-decision audit — read-only, independent; not a code review, no verdict
open: 6
principles: ARCHITECTURAL-PRINCIPLES.md, suggested defaults (unchanged by this audit); P-CPLX-1/3, P-PAT-5, P-COMPAT-1, P-DATA-2/3, P-DOC-1/2/4, P-ORG-4, P-AVOID-1 cited below
not-verified: no TLC run (logs absent from the repo), no test or build executed, whether any environment holds volume data written by a pre-PR build, whether pre-existing owners acquire a generation during rollout

## What the PR does

Gives a `volume` node the job of copying its own hash ranges and reporting what it holds,
so its relay can count it ready; adds an assignment generation to the published directory
record, slot repair after a wipe, a non-lifting seal during handoff, and a skip-to-S3 path
for a voting slot left behind by truncation. It does not touch the cold tier's flush logic,
the compute crate, or the wire compat of existing fields.

## Ledger — decisions made

C1  Readiness is a node report keyed to an assignment generation     crates/lib/src/relays.rs:527
C2  Generation = directory revision + 1, stored in the owner record   crates/relay/src/membership.rs:311
C3  Leaving owner seals on the copier's request; a seal is never lifted  crates/volume/src/store.rs:596
C4  Repair clears the range, then copies from peers                  crates/volume/src/repair.rs:201
C5  Fresh-vs-wiped decided once, before joining, from a relay majority  crates/volume/src/lib.rs:113
C6  A lagging voting slot skips to S3's flushed point                crates/volume/src/serve.rs:335

## Assumptions — what breaks if wrong

D1  An owner with no held mark has lost its data
    where: crates/volume/src/repair.rs:411-417, :179
    failure: a healthy store is cleared and re-copied; on a cell-wide first boot the range is left cleared, unheld and unreported (F1)
    tag: VERIFIED

D2  A peer that answers Repair holds the data a repair needs
    where: crates/volume/src/serve.rs:534; crates/volume/src/store.rs:736
    failure: a peer whose claims are all repaired is skipped per actor, yet the span is still held and reported ready — ready holding nothing
    tag: VERIFIED (path exists); reachability INFERRED (needs a second repair)

D3  S3's flushed point is a prefix of the committed log, with the epoch Meta records
    where: crates/volume/src/serve.rs:352; crates/volume/src/store.rs:1392
    failure: skip installs an entry under a covering watermark while the slot keeps its vote — the skip-any failure
    tag: UNKNOWN (rests on S-05; not established here)

D4  A pushed generation describes the node's current assignment, and 0 means none
    where: crates/lib/src/membership.rs:359-361; crates/relay/src/membership.rs:316-321
    failure: the check is inert for pre-existing owners and for ranges an old relay rewrites; a spurious 0 makes a node forget a good copy and repair destructively
    tag: VERIFIED

D5  The first write of a cell's volume directory means no data exists anywhere
    where: crates/relay/src/membership.rs:283-289; crates/volume/src/repair.rs:178-182
    failure: a new cell in an existing DC, or a rebuilt control namespace, marks owners ready with no data; the node reports a range it never copied
    tag: INFERRED

D6  A copy whose seqs and epochs line up is a correct copy
    where: crates/volume/src/repair.rs:340-372
    failure: a torn log that keeps seqs and epochs is installed; no checksum, at most 8 re-reads
    tag: INFERRED

## Challenges — decisions worth a conversation

## F1 — Repair destroys the range before it can prove a copy, keyed on a missing mark rather than a lost disk
status: open
decision: C4
defensible: a slot cannot vouch for data it did not copy for this assignment; clearing first makes the copy unambiguously the truth
contestable: the destructive step precedes any evidence a peer can supply the data, and fresh = !owns never asks the store whether it is empty (P-CPLX-3, P-DATA-3, P-MAINT-4)
cost: on the first boot of this version every owning node clears its ranges, and because serving a repair page is also gated on the held mark, no upgraded node can serve a peer's copy; a cell-wide roll leaves every range cleared, unheld, repaired and unreported, with S3 holding logs but not claims
settled-by: boot the new binary against a store written at 011ebd7 and assert what the store holds and reports after one tick
question: should repair ever delete before it holds a verified copy, and should the trigger ask the store whether it is empty?

## F2 — The generation's lifecycle and an undocumented upgrade order
status: open
decision: C2
defensible: stored with the owner, so it survives relay state loss and is identical on every relay
contestable: 0 means none, it is never migrated, and a base relay re-encodes an entry it touches with generation 0 (P-COMPAT-1, P-DATA-2, P-DOC-4)
cost: no upgrade procedure exists; relays upgraded before their cell's nodes strip generations on every touched entry, making nodes forget and re-copy; pre-existing owners never acquire a generation
settled-by: decode a base-written record with the new code and write down the intended order
question: should a kept owner with generation 0 be re-stamped, and what is the documented upgrade order?

## F3 — A seal that is never lifted, expressed as a transport error
status: open
decision: C3
defensible: the promise set must be frozen at copy time; unsealing was removed for a concrete race
contestable: a durable refusal state outside the Refusal vocabulary, with no exit for a cancelled handoff (P-PAT-5, P-COMPAT-1)
cost: the holder retries a sealed slot forever instead of dropping it; a mid-copy handoff abandonment leaves a span that refuses writes but still votes
settled-by: kill the copier mid-copy and assert what the leaving owner does with the next reserve and on the next boot
question: should a sealed slot answer Refusal::Sealed, and what ends a seal when nobody is copying?

## F4 — Skip-to-S3 arrived inside this PR, in the append hot path
status: open
decision: C6
defensible: without it the actor strands after handoff plus truncation; the guard is what volume.tla checks
contestable: outside #114's stated scope, puts the cold tier on the write path, and rests on an S-05 invariant (P-CPLX-1, P-DOC-1)
cost: a new way for a slot to rewrite its own log, with no test of the orchestration — Cold::meta has no caller and no unit test installs a cold tier
settled-by: a test that installs a cold tier and drives log-gap → skip → retry
question: why was skip not a separate PR with its own acceptance test, and is the epoch guard right given S-05's flush semantics?

## F5 — A separate binary's startup depends on a majority of relays
status: open
decision: C5
defensible: the classification cannot be revisited once ranges are assigned
contestable: no timeout, no failure mode, no metric — one warn per second (P-PAT-5, P-CPLX-4)
cost: a volume node started before its relays never joins, and waiting looks identical to broken
settled-by: start the volume tier before the relays and record what an operator sees
question: is a hard dependency better than a persisted install id, given the store already carries durable marks?

## F6 — The evidence for the safety claims is weaker than the PR says
status: open
decision: C1
defensible: TLC carries the model proofs with recorded runs; the readiness comparison and store-level seal/skip are unit-tested in CI
contestable: three of the four replayed counterexample scripts assert less than their names, the bug directions need hand edits, the sweep is four seeds, and the simulation suite is excluded from CI (P-MAINT-4, P-DOC-2)
cost: composite safety rests on a model plus scenarios that cannot fail for the reason claimed; the split-generation path is untested in both model and unit tests
settled-by: delete each production rule in turn and record which script fails
question: which of the four scripts fails if you delete the corresponding rule, and should the simulation suite run on this PR?

## Behavioural changes (for future archaeology)

- A range the node does not hold is refused with failed_precondition; previously served from whatever the store had
- Sealed and repairing slots surface as transport errors the holder retries, not as refusals it can act on
- A repaired slot accepts claims and appends but answers voting = false, and refuses reservations
- Volume startup waits for a majority of relays before joining membership
- An append may read the cold tier and rewrite the slot's log and overlay inside the same RPC
- Volume ranges merge only when their owners' generations agree, so some previously-merging ranges stay split
- volume-wipe is recorded as passing; the README's open design finding is gone
- scripts/tlc.sh defaults now include handoff-volume-split, which has never finished
- The simulation tests remain outside CI; m/* store keys remain invisible to older binaries

## Update convention

Answer an item by changing its `status:` line — `open` → `answered` / `accepted` / `moot` — and adding
an `answer:` line with date, decision and where the reasoning lives. Never renumber ids; a superseded
item gets `status: superseded` and a `superseded-by:` line. Keep the `open:` count in the header
truthful, and append new audits to `docs/audits/` with the same shape so one grep spans all of them.

Closing limits: no TLC model was run and its logs are not in the repository; no test, build or lint was
executed; whether any environment holds volume data written before this PR is unknown and is what makes
F1 live rather than theoretical. ARCHITECTURAL-PRINCIPLES.md is unchanged and still on its suggested
defaults.