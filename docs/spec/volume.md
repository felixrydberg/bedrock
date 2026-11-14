# Actor volume protocol model (S-02 / #110, S-02b / #131)

`volume.tla` models one actor's quorum log on `bedrock-volume`, as planned in
[statefull/v2.md §5](../implementation-prototype/statefull/v2.md#5-storage-bedrock-volume): an appointed holder claims
with epochs and appends, three slots in different cells promise and fsync, and a maintainer flushes committed entries
to S3. It also models the three changes that move votes. S-02b (#131) adds epoch reservation and the repaired state
after a disk wipe. Nothing here is implemented yet; S-03 (#111), S-04 (#112) and S-06 (#114) build on it.

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

## Bounds

The third epoch multiplies the state space, so each config gets the bounds its checks need:

| Config | Change | `MaxEpoch` | `MaxLen` | Why |
| --- | --- | ---: | ---: | --- |
| `volume-base` | none | 4 | 1 | Raft's Figure 8 needs four claims; single-entry logs suffice because the bug commits before the no-op |
| `volume-handoff` | handoff | 2 | 2 | Two competing claims around the handoff |
| `volume-wipe` | wipe | 2 | 2 | Two competing claims around the wipe |
| `volume-topology` | topology | 2 | 1 | One claim per version is enough for the joint-majority checks |

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
- **Log truncation after a flush** is not modeled; slots keep their whole log. Alarms, reads and the overlay are out
  of scope.

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

## Checks run

On 2026-11-14 with TLC 2026.11.06 from `scripts/tla2tools.jar`, 2 workers and a 4 GB heap (`scripts/tlc.sh`):

| Config | Generated states | Distinct states | Depth | Time | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| `volume-base` | 309,236,801 | 73,871,268 | 49 | 1,591 s | Passed |
| `volume-handoff` | 223,648,667 | 54,367,452 | 42 | 1,258 s | Passed |
| `volume-topology` | 136,316,807 | 32,069,792 | 40 | 648 s | Passed |
| `volume-wipe` | 149,000,043 | 33,777,832 | 42 | 1,005 s | Passed |

All 10 volume bugs broke their invariants, and the other 50 still did (`scripts/tlc.sh bugs`, 2,231 s). Reservation
adds a step to every claim, so the passing configs have 11 to 21 times as many distinct states as in S-02
(`volume-base` had 3.5 million). Runs used 2 workers; an earlier S-02 run with 4 workers was interrupted when the machine went down.
