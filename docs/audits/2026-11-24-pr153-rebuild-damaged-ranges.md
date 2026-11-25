# PR #153 — Rebuild only the damaged ranges of a quarantined engine (#103 / B-62)

pr: felixrydberg/bedrock#153
issue: #103 / B-62
base: 56789df (main at a01d636)
head: a615994
audited: 2026-11-24
kind: pr-technical-decision-audit
open: 5
principles: suggested defaults, unchanged
not-verified: cargo fmt/clippy/test, VOPR seeds, all TLC configs, deployment behaviour

## Decisions

- C1 — Model per-point health in handoff.tla and membership.tla before the code (sick/bad sets,
  Sicken/Narrow/Heal, DamageRefused, KeepsHealthyHolder per point; membership Sicken per point,
  bug range-only). docs/spec/handoff.tla:98-112,277@a615994; scripts/tlc.sh:57,71-74.
  Alternatives: per-node boolean plus an "extent known" flag (cheaper, cannot express a ready
  owner damaged in part); models untouched, simulation only. Blast radius: system.
- C2 — Prove the damage extent by a verification pass; never narrow from the reported failure.
  crates/lib/src/reconciliation/rebuild.rs:221-265@a615994. The rejected variant is modelled as
  the deliberate bug `unverified`. Blast radius: system.
- C3 — The adapter names the damaged file's key span and the engine drops that file in place,
  rather than moving the range. crates/lib/src/storage/rocksdb.rs:486-523@a615994. Alternatives:
  kv.md §15.7 reassign-and-repopulate (rejected in the issue with TLC evidence), delete_range only
  (measured not to work), whole-instance rebuild. Blast radius: contract.
- C4 — Direct librocksdb-sys dependency (=0.19.0, not(madsim)) to reach VerifyChecksum.
  Cargo.toml:30-32; crates/lib/Cargo.toml:36-37. Alternatives: vendor/patch rocksdb (precedent:
  the madsim patch), upstream PR. Lead decision, 2026-11-22. Blast radius: contract+dependency.
- C5 — The quarantine extent is durable in the engine as revisions: health:quarantined = 1 whole
  or 2 narrowed, plus health:quarantined:{start:016x}-{end:016x} per range; unparseable ⇒ whole.
  crates/lib/src/health/storage.rs:400-455@a615994. Blast radius: contract (durable format).
- C6 — A partly damaged member stays ready in the directory and unhealthy in membership, gets no
  new ranges, and the wire gains a range list instead of a new health state.
  crates/lib/src/health/storage.rs:203-268; docs/spec/membership.md:83-85. User decision
  2026-11-22; demotion to populating rejected in the issue. Blast radius: system.

## Assumptions

- D1 | tag: INFERRED | where: crates/lib/src/storage/rocksdb.rs:481-484,600-660@a615994
  VerifyChecksum over a hard-linked checkpoint sees what the live engine would hit, and names only
  the first bad file.
  failure: the narrowed range list understates the damage; the node serves damaged hashes and the
  next compaction wedges the engine, silently until health() flips.
- D2 | tag: VERIFIED | where: crates/lib/src/reconciliation/rebuild.rs:268-278@a615994
  A damaged file's key span is one contiguous L2 hash range; anything else falls back whole.
  failure: none for safety; cost only (whole rebuild when the span is not L2-only).
- D3 | tag: UNKNOWN | where: crates/lib/src/storage/rocksdb.rs:503-523@a615994
  DeleteFilesInRange on an open DB cannot stall or race concurrent reads/writes of other ranges.
  failure: the "good" ranges stall during a narrowed repair, defeating the feature.
- D4 | tag: VERIFIED (code) | where: crates/lib/src/relays/client.rs:615-650@a615994
  A range-scoped re-bootstrap keeps deletes deleted and no older value wins; a discarded range's
  cursors are forgotten so a later consumer bootstraps.
  failure: a consumer resumes into a hole (found by seed 67, now pinned).
- D5 | tag: VERIFIED | where: crates/lib/src/membership.rs:60-73, relays/ranges.rs:176-181@a615994
  Every reader treats an empty damaged list as the whole copy, never as healthy.
  failure: reads routed to damaged hashes.
- D6 | tag: VERIFIED | where: crates/lib/src/health/storage.rs:535-541@a615994
  The quarantine record write is best effort; a disk refusing it loses the extent across a restart.
  failure: a restarted node serves the damage until the scrubber's walk finds it.

## Challenges

- F1 | decision: C1 | defensible: per-point health is required for the new revoke and routing rules,
  and the models are the pre-agreed protocol gate. | contestable: P-CPLX-1, P-DOC-3, AGENTS.md
  "every config and bug is registered".
  cost: handoff 34.1M→124.3M distinct states and ~24 min→1 h 56; handoff-volume-faulty stays in the
  default list without finishing; handoff-ranges and its two bugs are never executed by any
  scripts/tlc.sh mode, so the two-node shape is unverified; no TLC step in CI.
  settled-by: a finishable-config decision, or an explicit README statement that the two-node shape
  is unchecked. question: is a registered-but-unexecuted config a check?
  status: open
- F2 | decision: C2 | defensible: quantifying the gain was asked for; the mechanism count is real.
  | contestable: P-MAINT-1, P-DOC-1.
  cost: the outcome (rebuild duration, unhealthy share) is unmeasured, and 131 vs the baseline 207
  quarantines on the same seed is unexplained.
  settled-by: a soak reporting both outcome numbers. question: what were the unhealthy-sample share
  and median rebuild duration on seed 90003 after the change?
  status: open
- F3 | decision: C3 | defensible: optional defaults, whole-instance fallback, §15.7:812 sanctions
  engine-specific physical recovery. | contestable: kv.md §15.7:814, P-AVOID-2, P-ORG-2.
  cost: the shared contract carries a filesystem capability and the rule that forbids it is
  unamended; the next adapter inherits an unexplained obligation.
  settled-by: one sentence in kv.md §15.7, or moving damaged/discard out of StorageEngine.
  question: is discard-and-re-bootstrap the engine-specific option the doc allows, or the engine
  repair it forbids?
  status: open
- F4 | decision: C6 | defensible: the relay's placement check is stricter, so the per-part gate
  only matters between the relay's write and the node's check. | contestable: P-MAINT-4.
  cost: no test exercises a directory entry narrower than the dropped range with a non-empty
  quarantine, so the intersection can be removed without a failure; a wrong span list could drop
  the last healthy copy.
  settled-by: a multi-entry test with a non-empty damaged list driven end to end through confirm.
  question: which test fails today if sick(part) becomes sick(range)?
  status: open
- F5 | decision: C2 | defensible: a cap on worst-case damage is better than none.
  | contestable: P-CPLX-1.
  cost: 8 spans and ½ the hash space are hand-picked, unconfigurable, absent from the model (which
  cannot reach them at two points), and a fragmented instance pays 8 full verifications then
  rebuilds whole anyway.
  settled-by: the damaged-span distribution from a corruption soak.
  question: where is the crossover between range rebuild and whole rebuild?
  status: open

## Observable behaviour

- Member.damaged on the wire; unhealthy datasets without it stay quarantined whole.
- While a range rebuild runs, non-damaged ranges serve; unconfined calls and new failures refuse
  the whole dataset.
- A quarantine no longer necessarily wipes the instance; surviving revisions and ranges are kept.
- RocksDB: a <instance>.verify checkpoint directory appears during a rebuild; files are dropped
  mid-rebuild; a discard resets the background-error baseline.
- The scrubber also reads revisions each pass; corruption injection in the simulation is wider.
- scripts/tlc.sh moves from about 2 hours to many hours.
- No new metric; the refused ranges are visible in logs and the member announcement only.

## Updating this file

Answer an item by changing `status:` to `answered` and adding an `answer:` line with the date, the
decision and where the reasoning lives. A superseded item gets `status: superseded` plus
`superseded-by:`. Keep the header `open:` count truthful.

Not verified: no build, test, VOPR or TLC run was performed for this audit (a PR-head checkout was
out of scope and the host runs one heavy job at a time); the PR's check and state-count claims are
the author's. Deployment behaviour is outside the repository. ARCHITECTURAL-PRINCIPLES.md was not
modified and remains on its suggested defaults.
