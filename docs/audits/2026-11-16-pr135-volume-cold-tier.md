# PR 135 audit — volume cold tier (part 1)

pr: felixrydberg/bedrock#135
issue: #113 / S-05 (part 1)
base: 27bb81b6cffd1b86dea6abf7735e9ca4bca280e9
head: 09c90f184c267ad2f526b79a8e79b76ba4e36b55
audited: 2026-11-16
kind: technical-decision audit (read-only; no verdict)
open: 6
principles: suggested defaults (unchanged)
not-verified: cargo fmt/clippy/test, madsim seeds, TLC runs, SlateDB internals, compute's actor host (S-07), relay size weighting

## Decisions

### C1 — One database per hash range instead of one per actor
status: recorded (user decision 2026-11-14, v2.md §5, §12 Q2)
where: crates/volume/src/cold/mod.rs:92-102@09c90f1
rationale: docs/implementation-prototype/statefull/v2.md:216-219@09c90f1

### C2 — Flush rebuilt from the log as absolute values, WAL off
status: open
where: crates/volume/src/store.rs:348-374@09c90f1; crates/volume/src/cold/db.rs:1-5@09c90f1
rationale: code comment only; not in PR body or commits

### C3 — Bound the log by refusing appends; a refusal teaches the commit point
status: open
where: crates/volume/src/store.rs:37-41,609-629,765-791@09c90f1
rationale: docs/spec/volume.md:28-32; the constants 100_000 and 16 GiB are unargued

### C4 — Cold reads merged in the RPC handler behind a 2 s freshness wait
status: open
where: crates/volume/src/serve.rs:348-420@09c90f1; cold/mod.rs:410-432@09c90f1
rationale: undocumented

### C5 — Cross-cell placement through sibling relays
status: recorded (v2.md §10 decision 15) / enforcement undocumented
where: crates/lib/src/relays.rs:2286-2294@09c90f1; crates/lib/src/client.rs:99-125@09c90f1

## Assumptions

### D1 — sibling_relays names every other cell
where: crates/volume/src/cold/placement.rs:87-98@09c90f1
failure: S3-backed reads fail FAILED_PRECONDITION; truncations retry forever, logged at debug only
tag: VERIFIED

### D2 — Maintainer's directory and readers' published table name the same database
where: cold/mod.rs:191-204 vs cold/placement.rs:184-195@09c90f1
failure: reader opens a database nobody writes; reads fail UNAVAILABLE during the window
tag: INFERRED

### D3 — A slot never ends up flushed beyond its range database
where: cold/mod.rs:318-322@09c90f1
failure: that actor is never flushed or truncated again and its reads time out
tag: VERIFIED code / UNKNOWN reachability (needs part 2's naming change)

### D4 — Per-window deltas can replace an untruncated tail safely
where: store.rs:699-730; cold/mod.rs:564-566@09c90f1
failure: a key written only by the replaced tail keeps a stale S3 value
tag: INFERRED

### D5 — 2 s is enough for a reader to see a flush
where: cold/mod.rs:48-49,411-431@09c90f1
failure: reads right after truncation fail UNAVAILABLE under S3 latency
tag: INFERRED

### D6 — One maintainer writes a range at a time
where: cold/mod.rs:46-47,238-248@09c90f1
failure: alternating fencing between two live writers
tag: UNKNOWN

## Challenges

### F1 — Backpressure's status vs the caller that treats it as fatal
decision: C3
status: open
defensible: refusing is the requirement; no new protocol field
contestable: RESOURCE_EXHAUSTED fences the whole holder in the sole caller (crates/compute/src/actors/volume.rs:597-609@09c90f1), unlike DataLoss; crates/compute unchanged (P-COMPAT-1, P-PAT-5, P-DATA-4)
cost: one full slot stops an actor with a healthy quorum; wake/reopen can fence repeatedly
settled-by: a VolumeClient-level test with one full slot and two healthy ones
question: should a full slot be dropped like DataLoss, or is actor-level fencing intended — and where is that written?

### F2 — Cross-cell volume routes with no DC check and outside the security contract
decision: C5
status: open
defensible: narrowest widening (volume caller, volume routes), four-direction test, decided in v2 §10
contestable: certificate carries no DC; security.md lists neither volume certs nor a Routes row (P-DOC-4, P-DATA-1)
cost: an authorization change auditable only by reading code
settled-by: a multi-DC deployment, or a doc saying DC separation is a network property
question: is the DC boundary enforced by certificates, and if not, which document says so?

### F3 — The range is now a shared failure and repair unit
decision: C1
status: open
defensible: only shape that keeps S3 cost flat in actors; settled 2026-11-14
contestable: blast radius, purge, restore and first-slot moves all become range-level, all deferred to part 2 (P-CPLX-2, P-DATA-1, P-COMPAT-3)
cost: 1/N of a cell's actors per unfetchable database; no repair path in part 1
settled-by: a measurement or written procedure for restoring one actor in a 10,000-actor range
question: if one range database is unrecoverable, what is lost and how is it rebuilt — before part 2 lands?

### F4 — sibling_relays is a silent runtime dependency
decision: C5
status: open
defensible: reuses Client's relay plumbing, no topology logic in volume
contestable: missing [s3] is a startup error, missing cell is a debug log (P-COMPAT-2, P-DOC-4)
cost: a node that writes but cannot serve S3-backed reads for two thirds of its actors
settled-by: ServiceConfig::validate checking the map against the topology record
question: why is one new config key fatal at startup and the other a runtime surprise?

### F5 — Cold-tier disk footprint ignores budget.disk_mb
decision: C3
status: open
defensible: constants are simple and the SSD cache is droppable
contestable: 16 GiB + 10 GiB defaults vs a published disk budget nothing enforces (P-AVOID-4, P-COMPAT-2)
cost: ~28 GiB a node's budget never sees; rediscovered by whoever fills a disk
settled-by: any deployment with disk_mb under 26 GiB
question: should both bounds come from budget, and what is 16 GiB today?

### F6 — A second SlateDB adapter and a second cache backend
decision: C1 / C4
status: open
defensible: lib's adapter cannot express WAL-off, manifest size, three-segment names or a Foyer tier
contestable: the read-only path is re-implemented with no recorded reason (P-PAT-4, P-ORG-3, P-DOC-2)
cost: two reader implementations to fix, two caches to tune
settled-by: a diff of the two reader paths beyond naming, or a written decision to fork
question: was sharing the reader path considered, and what would it have cost?

## Behaviour changes
- Append/Release can answer RESOURCE_EXHAUSTED (new for volume); a refusal still advances the commit point.
- Truncate may truncate less than asked and answer Behind; each truncation writes an `owed` record.
- Reads of overlay-missing keys come from S3; may fail UNAVAILABLE or FAILED_PRECONDITION.
- Scan merges overlay and S3, counting tombstones against the limit.
- Node needs [s3]; default 10 GiB cache dir; 100 ms flush loop; readers capped by memory_mb.
- Maintainers publish range sizes including manifest bytes; co-owners do not.
- Relays serve volume routes to volume callers from any cell.

## Update convention
Answer an item by changing `status:` from `open` to `answered` and adding `answer: <date> — <decision> — <where the reasoning lives>`.
A superseded item becomes `status: superseded` plus `superseded-by: <id>`. Keep `open:` in the header truthful.

## Limits
Checks not run: cargo fmt/clippy/test, madsim seeds, TLC. Not traced: SlateDB internals, compute's actor host (S-07), relay placement weighting. Access: authenticated local clone, git show/diff/grep at fetched refs plus read-only gh; the working tree was never used as the PR. ARCHITECTURAL-PRINCIPLES.md state: suggested defaults, unchanged by this audit.