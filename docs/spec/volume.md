# Actor volume protocol model (S-02 / #110)

`volume.tla` models one actor's quorum log on `bedrock-volume`, as planned in
[statefull/v2.md §5](../implementation-prototype/statefull/v2.md#5-storage-bedrock-volume): an appointed holder claims
with epochs and appends, three slots in different cells promise and fsync, and a maintainer flushes committed entries
to S3. It also models the three changes that move votes. Nothing here is implemented yet; S-03 (#111) and S-06 (#114)
build on it.

## What is modeled

- **Slots** `a`, `b`, `c` hold a promise (epoch and holder), a log of entries (epoch and holder), a state (`vote`,
  `leave`, `seal`, `pop`, `off`) and the topology version they accept. `d` is the node a change brings in.
- **Holders** `h1`, `h2` compete. `Start` claims at the highest epoch seen plus one. `Grant` accepts a claim only above
  the slot's promise, at its version or newer, and not while another holder's lease is live there. With a quorum,
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
  - `wipe`: `c` loses its promise and log and comes back populating. It copies from both peers (keeping the higher
    promise and the more advanced log) before voting again.
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
  says "the epoch matches its claim"; the claim record holds both. It matters only if two holders win one epoch, which
  the wipe finding below allows.
- **The topology transition ends after a joint commit.** v2.md does not say when version 3 starts. The model allows it
  only after an entry has committed under joint majorities, as Raft commits `C_old,new` before `C_new`. Without such
  a rule, a version-1 commit on `{a, c}` and a version-3 claim on `{b, d}` share no slot.
- **Slots learn a version from claims and appends.** Reading it from the topology record earlier only adds refusals,
  which a lost message already covers, so the model leaves that out.
- **Log truncation after a flush** is not modeled; slots keep their whole log. Alarms, reads and the overlay are out
  of scope.

## Finding: a wiped slot's promise is lost

`volume-wipe` fails `NoLostAck`. Copying from both peers before voting restores the peers' promises, but not a
promise the wiped slot gave just before the wipe, while that grant is still counted by its claimant. Shortest trace
(16 states, `MaxEpoch 2`, `MaxLen 2`):

1. `h1` and `h2` both claim epoch 1.
2. `c` grants `h1`. Then `c` is wiped and copies from `b`, which has promised nothing yet.
3. `b` grants `h1`: `h1` wins epoch 1 with the old `c` and `b`.
4. `c` copies from `a` (nothing promised yet), and `a` grants `h2`. `c` becomes ready with no promise and grants `h2`:
   `h2` also wins epoch 1, with `a` and the new `c`.
5. `h2` commits its no-op on `a` and `c`. Two holders hold epoch 1, which v2.md says cannot happen.

The same race also loses an acknowledged write across different epochs. `h1` wins epoch 1 with `a` and `b`. `h2` wins
epoch 2 with the old `c` and `b` and recovers an empty log. `c` is wiped, copies epoch 1 from `a` and `b`, and accepts
`h1`'s no-op, which commits on `a` and the new `c` although two slots had promised epoch 2. Once `h1` fences, `h2`
takes `a`'s promise and overwrites the acknowledged entry.

The fix is a design decision and is not in the model. Options:

1. Treat a repaired slot as a new member: a version change with joint majorities, as for topology (above).
2. Bound claims in time: a claimant drops a claim that has not reached a quorum within a bound, and the repaired slot
   waits that bound before copying. This puts a clock on durable safety, which epochs avoid today.
3. Do not vote until a claim started after the repair has committed (Paxos Made Live §5.1).

## Checks run

On 2026-11-14 with TLC 2026.11.06 from `scripts/tla2tools.jar`, 2 workers and a 4 GB heap (`scripts/tlc.sh`):

| Config | Generated states | Distinct states | Depth | Time | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| `volume-base` | 15,170,591 | 3,477,058 | 36 | 67 s | Passed |
| `volume-handoff` | 20,452,175 | 4,979,696 | 34 | 103 s | Passed |
| `volume-topology` | 9,059,601 | 2,098,058 | 32 | 38 s | Passed |
| `volume-wipe` | 652,317 | 196,344 | 16 | under 1 min | Fails `NoLostAck` (finding above) |

All eight deliberate bugs broke their invariants, and the existing 50 still did (`scripts/tlc.sh bugs`, 2 min 16 s).
An earlier run with 4 workers was interrupted when the machine went down; these runs used 2.
