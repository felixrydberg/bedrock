# Actor volume protocol model (S-02 / #110, S-02b / #131)

`volume.tla` models one actor's quorum log on `bedrock-volume`, as planned in
[statefull/v2.md §5](../implementation-prototype/statefull/v2.md#5-storage-bedrock-volume): an appointed holder claims
with epochs and appends, three slots in different cells promise and fsync, and a maintainer flushes committed entries
to S3. It also models the three changes that move votes. S-02b (#131) adds epoch reservation and the repaired state
after a disk wipe. S-03 (#111), S-04 (#112) and S-06 (#114) implement it.

## What is modeled

- **Slots** `a`, `b`, `c` hold a promise (epoch and holder), a highest reserved epoch, a log of entries (epoch and
  holder), a state (`vote`, `leave`, `seal`, `pop`, `repaired`, `off`) and the topology version they accept. `d` is
  the node a change brings in.
- **Holders** `h1`, `h2` compete. `Start` picks the highest epoch seen plus one, and `Reserve` records it at slots: a
  slot accepts only an epoch above its highest reservation. Once a quorum has reserved it (`Reserved`), the holder
  claims at that epoch. `Grant` accepts a claim only above the slot's promise, at its version or newer, and not while
  another holder's lease is live there. With a quorum,
  `Recover` fixes the most advanced log the slots sent (highest last-entry epoch, then sequence). `Write` appends the
  no-op and later entries. `Release` is a last entry after which no lease is live. `Fence` is a self-fence or a crash.
- **Appends.** `Replicate` sends the holder's log to a slot whose promise is the holder's ballot. The slot takes the log,
  which copies the missing tail and truncates an old epoch's uncommitted tail, and fsyncs. The holder acknowledges in
  the same step when that fsync completes a quorum. It counts only an entry of its own epoch, which commits the
  entries before it (Raft's Figure 8 rule). The slot learns the commit point with the append.
- **Lease.** Under the bounded-drift assumption a holder self-fences before any slot sees its lease expire, so a slot
  sees a live lease exactly while its promised holder is between `Recover` and `Fence` or `Release`.
- **Maintainer.** Two slots (`a`, `b`) can each believe they are the maintainer. `OpenMaint` is SlateDB writer fencing:
  the newest opener fences the other. `Flush` appends one atomic batch to S3, up to the announced commit point.
- **Backpressure and truncation** (S-05, #113; config `volume-truncate` only, so the other configs keep their state
  spaces). Every slot tracks the commit point it knows. `Learn` is an append refused under backpressure: the slot
  writes nothing, as with a lost message, but learns the announced commit point for the part of its log that matches
  the holder's, so a full slot can still truncate. `Truncate` drops a slot's log through what S3 holds, but never
  past the commit point the slot knows; the dropped prefix is then read from S3. `Skip` (S-06, #114) lets a slot
  whose log ends below S3 take S3's state as its log, keeping its vote, but only when S3's last entry is no newer than
  the slot's promise.
- **Changes**, one per config (`Change`):
  - `handoff`: `c` becomes the leaving owner and seals. The populating owner `d` copies from the sealed `c`, reports
    ready, then votes in `c`'s slot position; `c` drops the range. Readiness is reported by the node and checked by
    its relay (the B-21 change for `volume` ranges).
  - `wipe`: `c` loses its promise, reservation and log, and comes back populating. It copies from both peers, keeping
    the higher promise and reservation and the more advanced log. It then waits as `repaired`: it takes claims and
    appends, but counts towards no reservation, claim or commit quorum. It votes again once an append tells it that a
    claim above the highest reservation it copied has committed.
  - `topology`: version 1 is `{a, b, c}`, version 2 needs majorities of both `{a, b, c}` and `{a, b, d}`, version 3 is
    `{a, b, d}`. Claims and appends carry the version the holder read, and a slot refuses an older one.

Messages are abstract. A grant the holder has received stays counted after the slot that sent it is wiped or hands
off, and a fenced holder's appends still arrive.

## Invariants

| Invariant | Meaning |
| --- | --- |
| `NoLostAck` | No two entries commit at one sequence. Every acknowledged entry is in the recovered log of every holder that won a later claim, and of any other holder that won the same epoch. |
| `OneWriter` | At most one holder serves the actor (is open). |
| `NoStaleAppend` | Once a newer holder serves, an older epoch commits nothing that holder lacks. A late fsync may still complete the quorum of an entry the newer holder recovered. |
| `S3Committed` | Every entry in S3 is committed, at its sequence. |
| `TruncatedInS3` | What a slot truncated is what S3 holds at those sequences, so reads of it from S3 are the slot's log. |
| `TypeOK` | Catches mistakes in the model only. |

Each deliberate bug in `scripts/tlc.sh` breaks the invariant named beside it:

| Config | `Bug` | Breaks | What it does |
| --- | --- | --- | --- |
| `volume-base` | `appoint` | `NoLostAck` | Recovery takes the last slot's log, not the most advanced |
| `volume-base` | `previous` | `NoLostAck` | A previous-epoch entry counts as committed once on a quorum |
| `volume-base` | `flush` | `S3Committed` | The maintainer flushes past the announced commit point |
| `volume-handoff` | `populating` | `NoLostAck` | A populating slot votes |
| `volume-handoff` | `unsealed` | `NoLostAck` | The populating owner copies from an unsealed leaving owner, which keeps voting |
| `volume-wipe` | `self-epoch` | `NoLostAck` | A holder claims at its own highest seen + 1, without reserving it |
| `volume-wipe` | `early` | `NoLostAck` | A repaired slot votes at once |
| `volume-topology` | `new-only` | `NoLostAck`, `OneWriter`, `NoStaleAppend` | During the transition a quorum of only the new slot set counts |
| `volume-truncate` | `learn` | `S3Committed` | A refused append teaches the commit point past where the slot's log matches the holder's |
| `volume-truncate` | `truncate` | `TruncatedInS3` | A slot truncates through what S3 holds, past the commit point it knows |
| `volume-truncate` | `skip-keep` | `TruncatedInS3` | A slot behind S3 keeps its own prefix and only appends S3's tail |
| `volume-truncate` | `skip-any` | `TruncatedInS3` | A slot skips to an S3 point newer than its promise, so its fenced holder's appends land on it |

## Bounds

The third epoch multiplies the state space, so each config gets the bounds its checks need:

| Config | Change | `MaxEpoch` | `MaxLen` | Why |
| --- | --- | ---: | ---: | --- |
| `volume-base` | none | 4 | 1 | Raft's Figure 8 needs four claims; single-entry logs suffice because the bug commits before the no-op |
| `volume-handoff` | handoff | 2 | 2 | Two competing claims around the handoff |
| `volume-wipe` | wipe | 2 | 2 | Two competing claims around the wipe |
| `volume-topology` | topology | 2 | 1 | One claim per version is enough for the joint-majority checks |
| `volume-truncate` | truncate | 2 | 2 | Two entries let a slot match the holder's log in part and truncate in part |

Two hosts, symmetric. `MaxEpoch 3` with `MaxLen 2` grew past 38 million distinct states for `handoff` without
finishing.

## Model choices to confirm in S-03

- **Appends check the holder too.** A slot writes an append only when its claim's epoch and holder both match. v2.md
  says "the epoch matches its claim"; the claim record holds both. It matters only if two holders win one epoch.
- **The topology transition ends after a joint commit.** v2.md does not say when version 3 starts. The model allows it
  only after an entry has committed under joint majorities, as Raft commits `C_old,new` before `C_new`. Without such
  a rule, a version-1 commit on `{a, c}` and a version-3 claim on `{b, d}` share no slot.
- **Slots learn a version from claims and appends.** Reading it from the topology record earlier only adds refusals,
  which a lost message already covers, so the model leaves that out.
- **Log truncation after a flush** is modeled as a truncation point (`volume-truncate`); slots keep their whole log
  beside it, and `TruncatedInS3` checks that the truncated prefix is S3's. Alarms, reads and the overlay are out of
  scope. The next section says what the code needs so that this abstraction stays sound.

## Truncation and `Slot.flushed_epoch` (#112 / S-04)

The model's `Replicate` sends a slot the holder's whole log (`hlog`), so the holder always knows the epoch of the
entry before anything it sends. That is how a slot's log is checked against the holder's at the append point, and
how a slot holding an old epoch's tail gets it truncated. In the code, a slot answers `Log` only with entries after
its flushed point (S-05 truncates the rest once S3 holds it). A holder that recovers from such a slot therefore lacks
the entry at the flushed point, and without its epoch it cannot send a lagging slot the entry after it.

`Slot.flushed_epoch` (`crates/lib/proto/volume.proto`, field 10) closes that gap. Every slot answer carries the epoch
of the entry at its flushed point, from `ClaimRecord.flushed_epoch`. The claimant's recovery reads it with the first
`Log` page and uses it as the epoch before its first recovered entry (`recover` in
`crates/compute/src/actors/volume.rs`). The field adds no step and no state the model lacks: `Truncate` drops only
what S3 already holds, so the epoch at the flushed point is known before a slot truncates, and the specification
keeps no field for it. Two facts make it safe:

- An entry at a slot's flushed point is committed, because S3 holds only committed entries (`S3Committed`). Its epoch
  is the same on every slot that holds it.
- A slot without that entry still refuses the append with `LOG_GAP`, as in the model. The holder then stops sending
  to it until S-06 repairs it.

The `flushed_point` scenario in `tests/sim/tests/volume_client.rs` checks this. Two slots are truncated, and a third
whose log ends exactly at the flushed point catches up through the next holder. Without the field, it is stuck.
S-05 models truncation itself with `Learn`, `Truncate` and `TruncatedInS3` (above); the code reads the dropped prefix
from S3.

## Resolved: slot repair after a disk wipe (S-02b / #131)

A slot that loses its data also loses the promises it gave. The rule is:

1. **Epochs are reserved at a quorum.** Before claiming, a holder reserves `highest seen + 1` at two slots. A slot
   accepts only an epoch above its highest reservation, and fsyncs it. A holder never claims at an epoch it picked
   alone, as Rivet's Depot volume never lets a writer pick its own epoch.
2. **A repaired slot is a learner.** It copies claims, reservations and logs from both peers and keeps the highest
   reservation as its floor. It then takes claims and appends, but counts towards no reservation, claim or commit
   quorum.
3. **It votes again once a claim above its floor has committed its no-op.** Every promise it gave before the wipe was
   for an epoch reserved at a quorum, so one of its peers holds that reservation and the floor covers it.

The cost is one round trip and one fsync per claim (each wake and failover). Durable safety still uses no clock. Range
handoff needs no such rule: the populating owner copies from the sealed leaving owner, which has given every promise
and reservation it will give.

### Why: copying from both peers is not enough

`volume-wipe` failed `NoLostAck` when the repaired slot voted after copying (16 states, `MaxEpoch 2`, `MaxLen 2`):

1. `h1` and `h2` both claim epoch 1.
2. `c` grants `h1`. Then `c` is wiped and copies from `b`, which has promised nothing yet.
3. `b` grants `h1`: `h1` wins epoch 1 with the old `c` and `b`.
4. `c` copies from `a` (nothing promised yet), and `a` grants `h2`. `c` becomes ready with no promise and grants `h2`:
   `h2` also wins epoch 1, with `a` and the new `c`.
5. `h2` commits its no-op on `a` and `c`. Two holders hold epoch 1.

### Why: waiting for a newer claim is not enough on its own

Rule 3 alone, with holders still picking their own epoch, also fails `NoLostAck` (22 states):

1. `h1` claims epoch 1, fences, then claims epoch 2. Only `c` grants `(2, h1)`.
2. `c` is wiped. `a` and `b` have promised nothing.
3. `h2` starts after the wipe and picks epoch 1, because epoch 2 existed only on `c`. It commits its no-op on `a` and
   `b`, opens and appends a Release entry.
4. `b` grants `(2, h1)`. `h1` recovers with the old `c` and `b`, whose log holds only the no-op.
5. `c` finishes copying, learns that `h2`'s no-op committed and votes again. The Release entry commits on `a` and `c`,
   and `h1` lacks it.

A claim that starts after the repair need not have a higher epoch. With rule 1, epoch 2 is reserved on `a` or `b`, so
`h2` picks a higher epoch and the repaired `c` copies 2 as its floor.

## Slot repair and range handoff in the code (S-06, #114)

`crates/volume/src/repair.rs` implements `Copy` and `Ready`. The relay side, where the node reports readiness and its
relay checks it, is `handoff.tla` with `Reported` ([membership.md](membership.md#node-reported-readiness-volume-114--s-06)).

- **Seal.** A new owner copies its range from the `leaving` owner with `Repair` requests that carry `seal`. The first
  one seals the span durably on the leaving owner, which then refuses `Reserve`, `Claim` and `Append`. Each write checks
  the seal under its actor's lock, and sealing takes every lock once, so no write is in flight when the copy starts.
  The leaving owner thus has given every promise it will give (`Copy` in a handoff requires `st[m] = "seal"`).
- **Which path.** A `populating` owner copies from the range's `leaving` owner when there is one. Otherwise the last
  owner was removed, and it repairs as after a wipe. A `ready` or `leaving` owner whose store started empty while it
  owned ranges lost its disk, and it also repairs. A store that started empty while its node owned nothing is `fresh`:
  a range it is first given `ready` (a cell's first owners) has nothing to copy.
- **Repair** copies from every `ready` or `leaving` owner of the range in every cell of the DC, keeping only actors
  with a slot both there and here. Per actor it keeps the more advanced log with its overlay, and the higher promise
  and reservation, which is `Copy`'s rule. It then marks the actor `repaired`. A source copy that is itself repaired
  is skipped, since it is no voting peer.
- **Unknown actors.** The range is also marked repaired, so an actor that neither peer had a record of when the copy
  ran starts `repaired` with floor 0. A reservation still in flight to a peer can reach it after the copy, which is the
  `early` trace. The mark stays until the range leaves the node.
- **Readiness.** The node holds a range only once the copy is complete, and only for the assignment generation it
  copied for. It reports what it holds. A pushed set that no longer lists the range, or lists it for another
  generation, makes the node forget the copy, and a copy that saw such a set while it ran is not held. This makes the
  copy atomic in the sense of `handoff.tla`'s `Copy` and `Receive`. Until it holds a range, the node refuses every call
  for it.
- **A slot left behind.** A new writer keeps the log only from the flushed point of the slot it recovered from, so it
  cannot resend what S3 already holds. A voting slot whose log ends below that, such as one that copied a sealed
  owner's log while the holder kept committing on the other two, finds the gap when it applies the append. Before it
  answers, it reads the actor's meta from the range database and skips to it (`Store::skip`, `Skip` in the model): it
  drops its log and overlay there and keeps its vote, since it lost no promise. It then applies the append again, and
  the holder gets that answer. It skips only when S3's point is no newer than its promise and its range is not
  sealed. A slot behind entries that S3 does not hold yet answers `LOG_GAP` and stays behind until a flush reaches
  them.
  `tests/sim/tests/volume_cell.rs` runs this through the real relays and binaries: without it, the actor's next
  claim can never commit once another slot is wiped.
- **Merges.** A merge keeps the earlier entry's generation for every owner, so the publisher merges two `volume`
  ranges only when each owner has the same generation in both. Otherwise that owner would forget its copy of the
  later part, and a node that is not fresh would repair it, as `repaired`, from its peers. Ranges whose generations
  differ stay apart. `handoff.tla` has splits but no merges.
- **Fresh or wiped.** Before it joins its cell, a node whose store has not decided reads its cell's `volume`
  directory. It is fresh if the directory lists it nowhere; otherwise its disk was lost.

`tests/sim/tests/volume_repair.rs` replays the four TLC counterexamples (`populating`, `unsealed`, `early`,
`self-epoch`) against the real services and repair, and checks that none loses an acknowledged commit. Its seeded
scenario wipes a slot and then hands its range off while a holder commits, and checks that the slot votes only once a
newer claim has committed.

## Checks run

On 2026-11-14 with TLC 2026.11.06 from `scripts/tla2tools.jar`, 2 workers and a 4 GB heap (`scripts/tlc.sh`):

| Config | Generated states | Distinct states | Depth | Time | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| `volume-base` | 309,236,801 | 73,871,268 | 49 | 1,591 s | Passed |
| `volume-handoff` | 223,648,667 | 54,367,452 | 42 | 1,258 s | Passed |
| `volume-topology` | 136,316,807 | 32,069,792 | 40 | 648 s | Passed |
| `volume-wipe` | 149,000,043 | 33,777,832 | 42 | 1,005 s | Passed |
| `volume-truncate` | 99,206,671 | 20,565,946 | 40 | 725 s | Passed |

All 10 volume bugs broke their invariants, and the other 50 still did (`scripts/tlc.sh bugs`, 2,231 s). Reservation
adds a step to every claim, so the passing configs have 11 to 21 times as many distinct states as in S-02
(`volume-base` had 3.5 million). Runs used 2 workers; an earlier S-02 run with 4 workers was interrupted when the machine went down.

On 2026-11-18, after `Skip` (S-06), with 3 workers and an 8 GB heap: `volume-truncate` passed 108,935,493 generated and
22,458,014 distinct states, depth 39 (486 s), and `learn`, `truncate`, `skip-keep` and `skip-any` broke their
invariants. Two earlier `Skip` rules failed `TruncatedInS3`. Without the promise check, a holder fenced by a newer one
appended over a skip to the newer holder's flush. Without the recovered-holder check, a holder fenced before it
recovered did the same. The model lets such a holder replicate, while the code's holder appends only once recovered.
The same day `volume-base`, `volume-handoff`, `volume-wipe` and `volume-topology` passed again with the distinct state
counts in the table, since `Skip` runs only in `volume-truncate`, and each of their bugs broke its invariants.

On 2026-11-16, after S-05 (#113) added `Learn`, `Truncate` and `TruncatedInS3`, every volume config was checked again
with 2 workers and a 4 GB heap. The four earlier configs found exactly the state counts above, since the new actions
run only in `volume-truncate`; all 12 volume bugs broke their invariants, including the new `learn` and `truncate`.

## Cold tier: range databases (S-05, #113)

`volume.tla` keeps one S3 sequence per actor, with SlateDB fencing between maintainers. `volume_cold.tla` checks that
the range databases behind it keep that sequence across splits, merges and first-slot moves (statefull/v2.md §5 Cold
tier). It is a separate model because adding databases and catalogs to `volume.tla` would multiply a state space that
already takes about 25 minutes per config.

One actor sits in the middle of three hash points. Each cell's catalog names the database serving each point.

- **Maintainers** see their cell's directory and the actor's first cell, each possibly stale.
- **Open:** a span that the catalog serves with one database of exactly that span, not retired, is opened as writer.
  This fences any other writer.
- **Create:** any other span gets a new database in steps that interleave with everything else. The maintainer fences
  and marks retired every database serving part of the span, clones the new database from them, and replaces the
  catalog only if it is unchanged since it read it.
- **Flush:** writes the slot's entries after its flushed point, up to the commit point. Once the flush is durable, the
  slots truncate. A database behind the slot first copies the actor from another cell's database that holds it that
  far.

| Invariant | Meaning |
| --- | --- |
| `Consistent` | Every database holds the actor without gaps, up to its meta. |
| `S3Committed` | No database holds past the commit point. |
| `NoLostFlush` | Everything the slots dropped is in a database that a read finds through a catalog. |

Each guard has a bug that must break an invariant:

| Config | Bug | Breaks | What the bug does |
| --- | --- | --- | --- |
| `volume_cold-split` | `no-retire` | `NoLostFlush` | A clone does not fence its sources, so a stale maintainer flushes after the clone's snapshot. |
| `volume_cold-split` | `no-cas` | `NoLostFlush` | The catalog is replaced without the version check, so a concurrent clone's database is dropped after it was written. |
| `volume_cold-move` | `no-adopt` | `Consistent` | The new first cell flushes without copying the actor, leaving a gap. |

The model takes each clone source per point, so it does not check SlateDB's rule that a clone takes each source path
once. The unit tests cover that rule (the interior split and full merge in `crates/volume/tests/unit/cold.rs`). Relay
`Routes` access for sibling cells is an authorization rule and is not modeled.

### Checks run

On 2026-11-16 with `scripts/tla2tools.jar`, 4 workers and a 4 GB heap:

| Config | Constants | Distinct states | Depth | Time | Result |
| --- | --- | ---: | ---: | ---: | --- |
| `volume_cold-split` | 2 maintainers in one cell, 3 directories, `MaxSeq` 2, `MaxGen` 2, `MaxEp` 2 | 23,844,105 | 38 | 785 s | Passed |
| `volume_cold-move` | 2 cells with one maintainer each, `MaxSeq` 2, `MaxGen` 2, `MaxEp` 2 | 322 | 14 | 1 s | Passed |

All three bugs broke their invariants (66,738, 12,287 and 314 distinct states).
