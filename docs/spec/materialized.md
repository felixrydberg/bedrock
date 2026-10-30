# B-11 atomic namespace persistence safety model

`materialized.tla` models the standalone #11 / B-11 protocol selected on
2026-10-30. A single SlateDB atomic batch stores original retained events,
resolved SET/DELETE state, namespace revision and original commit time, retry
result with its fixed expiry, and an optional represented log position. The
standalone writer acknowledges only after the batch's remote durable flush.
There is no separate raw S3 event journal or published/pruned object protocol.

## State and actions

The bounded model has one namespace, one key and one retry identity. `history`
is a reference oracle for expected event content and fixed expiry. It includes a
staged candidate until crash; neither recovery nor retry lookup consults it.
`buffered` and `durable` contain complete database records. `Stage` writes the
entire batch atomically to buffered state. `Flush` makes that complete state
remote-durable. `Ack` requires the pending revision to be durable. A flush may
complete without an acknowledgement; retry still returns its original result.
`Crash` discards only buffered state and reopens the durable database.

Retry lookup uses the durable retry record, with expiry interpreted at lookup
time. `RetryProtection` ensures an unexpired reference result is still present;
`Stage` cannot allocate another revision while that result is live. Expiry is
chosen once in `Stage`, never renewed on flush, lookup or restart. `Retry` returns
the stored revision by adding that original response to `acknowledged`.

`Tick` advances logical time without scheduling work. Successful requests schedule
cleanup; `StageCleanup` removes expired event and retry rows independently in one
batch, and `Flush` makes that cleanup durable. `CleanupFailure` permits scheduled
work to fail without changing durable data. Idle physical cleanup may be delayed
indefinitely. The model checks young-event retention separately from retry
protection using different retention windows.

`bufferEpoch = epoch` on `Flush` abstracts single-publisher engine fencing. The
model assumes that a closed or superseded handle cannot publish; it does not prove
SlateDB's lease implementation, model competing writers, or retain outstanding
I/O from stale handles. `WithPosition` exercises optional progress metadata in the
same atomic batch. It does not implement a Raft group or prove Raft recovery.

The seven invariants cover coherent buffered/durable state and fixed expiry,
acknowledgements bounded by durable progress, ordered revisions, young retained
events, live retry protection, and retention of a pending event until flush.
A namespace revision is a Bedrock revision, independent of engine sequence IDs.

## Checks run on 2026-10-30

TLC 2.19 with explicit Java 17, two workers, `MemFPSet` and breadth-first search
completed all seven invariants with `MaxSeq=3`, `MaxTime=4`, `MaxEpoch=2`:

| Event window | Retry window | Optional position | Generated | Distinct | Depth | Result |
|---|---|---|---|---|---|---|
| 1 | 2 | absent | 2,207 | 888 | 18 | Passed |
| 2 | 1 | present | 4,270 | 1,966 | 19 | Passed |

A scratch mutation allowed `Ack` against buffered rather than durable progress.
TLC rejected `NoLostAck` with the trace `Stage -> Ack` (16 generated / 15 distinct
states). The checked-in source was never mutated.
The old journal/publication model's results do not validate this replacement.

Run the checked-in configuration from `docs/spec` in PowerShell. Put the TLC
tools JAR at `./tools/tla2tools.jar` or replace that relative classpath with its
location; Java 17 must be available on `PATH`:

```powershell
java `
  '-XX:+UseParallelGC' `
  '-Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.MemFPSet' `
  -cp './tools/tla2tools.jar' `
  tlc2.TLC -workers 2 `
  -metadir './states/materialized' `
  -config materialized.cfg materialized.tla
```

No fairness or liveness is specified. This does not establish actual SlateDB
internals, multi-namespace/key isolation, native checkpoints, range checksums,
corruption handling, ambiguous I/O, stale asynchronous publishers, real S3 crash
acceptance, bounded resource usage, relay streams or the full #25 / B-23 gate and
seven-night history. Those require implementation tests and separate model work.

## Future Raft boundary

B-09b is future work. A majority-fsynced committed entry may then be acknowledged
before SlateDB persistence. The leader must retain the committed but not yet
SlateDB-durable tail and include it in retry lookup and recovery. It batches those
entries through the same atomic persistence operation, with represented Raft
index/term. A native SlateDB checkpoint is a separate recoverability object, not
the progress record or a successful buffered apply.

Truncation requires a verified recoverable checkpoint covering **every namespace**
affected by the truncated global prefix, including namespaces whose engines are
closed. Its recovery metadata includes the Raft cutoff index, term and membership.
A per-namespace progress record alone never authorizes global truncation. Extend
the protocol model before implementing quorum acknowledgement or truncation.
