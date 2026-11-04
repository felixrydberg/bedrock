# Cell membership (#15 / B-12), L2 placement directory (#19 / B-16), storage health (#13 / B-14) and range handoff (#22 / B-21)

A member is a `bedrock-quicksilver`, `bedrock-kv` or `bedrock-cdn` process that is receiving updates: it holds a leaf
`Relay.Session` to a relay of its own cell. There is no heartbeat or gossip protocol and no epoch.

## Protocol

- **Handshake.** A leaf `Hello` carries a `Member`: node ID, service, region, DC, cell, roles, datasets,
  CPU/memory/disk budget, failure domain, advertised address, status (`ready`/`draining`) and unhealthy
  datasets. Later `RelayRequest.member` messages may change only the status and unhealthy datasets of the same
  `(service, node_id)`. Relay-to-relay and recovery sessions carry no member. Identities are ASCII letters,
  digits, `-`, `_` and `.`.
- **Storage health.** `Member.unhealthy` lists, in order, the member's own datasets whose local storage its
  process has quarantined or is rebuilding (`health::storage`). The session sends a member update when the list
  changes and the current list on every `Hello`. It is published like any other change to the entry. It is not a
  heartbeat: it is sent only on change, on the stream the node already holds.
- **Relay view.** Each relay lists the members on its leaf sessions, the newest session winning a duplicate
  identity, with each subscription's acknowledged revision for range readiness (#22 / B-21). When a
  draining member's stream closes, the relay keeps it as relay-local `LEFT` for 30 seconds.
- **Siblings.** A cell's relays are each relay plus its `replay_peers`, listed by advertised address. Every relay
  serves `Siblings.Watch` and watches each sibling: a snapshot completed by `synced`, then changes, with progress
  ticks every 10 seconds and the 30-second stream deadline. A relay shares only its own leaf members, never what it
  learned from a sibling. A sibling counts as live only while its synced stream is up. The stream also carries
  subscriber interests, at every tier ([relays.md](relays.md#sibling-interests)); above the cell tier it carries only
  those.
- **Publication.** Dataset `membership`, namespace `{service}.{cell}`, key node ID, value the encoded `Member`.
  Cell relays subscribe to every service namespace (`quicksilver`, `kv`, `cdn`) through their upstreams like any subscriber. Once that view is
  current, the relay with the lowest address among itself and its live siblings publishes, but only while they
  are a strict majority of the configured relays. It writes the diff between the union of live lists and the
  published view through `bedrock-api` with the relay's `write_token`. It then waits until its subscription
  applies the returned revisions before planning again.
- **Grace.** Every relay keeps its own absence clock for each published member that no live list holds, and
  resets it when one does. The publisher deletes after `grace_ms` (default 20 s) of absence, or immediately
  for a live `LEFT` marker. Failover to another cell relay within the grace period therefore writes nothing:
  the entry is unchanged. A restarted node rejoins under the same node ID.
- **Freeze.** Without a majority, or before the relay's subscription is current, nothing is written. With every
  cell relay down, nodes keep their last view. A node cut off from all relays is removed even though its peers
  may still reach it.
- **Isolation.** Quicksilver, KV and CDN, and different cells, use separate namespaces. Relays ignore entries for another
  cell; such a node can still receive data through a DC-tier fallback relay but is not a member of that cell.
- **Nodes.** `bedrock-quicksilver`, `bedrock-kv` and `bedrock-cdn` subscribe to their own `{service}.{cell}` namespace in memory
  (`membership::View`). When the view advances, they drop the connections and circuit breakers of members
  that left. Shutdown drains the session: `draining`, half-close, then wait for the relay to end the stream.

A relay-to-relay split leaves at most one publishing side, and its view decides: nodes held only by the minority's
relays are removed after the grace period. Run at least three relays per cell; with two, one relay down freezes
all membership changes until it returns.

## Placement directory

§4 decision 13 of `feature-drafts.md`. Applies to the `quicksilver` and `cdn` services; customer KV ranks its readers instead
(#18 / B-15).

- **Entries.** Each dataset has its own ranges: key `range/{dataset}/{start}` (16 lowercase hex digits) in the same
  `{service}.{cell}` namespace, value an encoded `Range`: owners with state `populating`, `ready` or `leaving`. A
  range ends where the dataset's next starts, or at 2⁶⁴, so every single write leaves the hash space covered with no
  gaps or overlaps. Node IDs cannot contain `/`, so entries never collide with members. Membership and placement
  share the namespace's one revision order.
- **Assignment.** Only the membership publisher writes, and only once membership has nothing left to write, so it
  plans from applied membership. Each pass runs `placement::assign` (#18 / B-15) per dataset, over the members whose
  `datasets` include it, with 3 replicas and at most 16 ranges moving, and applies the handoff steps below. It then
  waits for its subscription to apply the writes, as for membership. `bedrock-quicksilver` places `quicksilver` and
  `auth`; `bedrock-cdn` places `cdn`, whose ref keys sit at their artifact's content hash (B-34, §4 decision 14). A
  member announcing a dataset as unhealthy (#13 / B-14) gets no new ranges of it. It keeps what it holds, for B-22
  to rebuild, and source selection skips it for the unhealthy datasets only.
- **Owner checks.** Nodes read the directory from `membership::View::directory`. `placement::owners` lists the owners
  of a hash; a node serves a hash only if it holds it as `ready` or `leaving` in its own view. A caller routed by a
  stale view is rejected and retries the next owner. Revisions are ordinary dataset revisions, not a placement
  epoch.
- **Freeze.** Without a majority nothing is written, so placement freezes with membership and nodes keep serving
  what they hold.

## Range handoff

#22 / B-21, §4 decision 13 as refined on 2026-11-02. Joins, failures and drains all use the same steps:

1. Assignment writes new owners as `populating`.
2. Every relay pushes each of its leaf members the full set of ranges it owns per dataset, in any state, tagged
   with the directory revision it read. The member follows each new range as one stream that carries every
   namespace of the dataset ([relays.md](relays.md#range-streams)), bootstrapping where needed.
3. The holding relay counts the member ready for a range once its bootstrap is confirmed and it has acknowledged
   through the head the relay saw then. It counts only acknowledgements sent after the member processed every
   pushed set. It shares that ready bit with its siblings, never the acknowledgements; the publisher writes the
   owner `ready`.
4. A `leaving` owner keeps following and serving. Once every other owner of the entry is `ready`, the publisher
   removes it.
5. The relay then pushes a set without the range: that is the revoke. The member waits until its own view has
   reached the pushed revision, then confirms that the view no longer lists it for the range and lists another
   `ready` owner. It stops serving and following the range, deletes it with one `delete_range` over the hash span
   and compacts that span. A revoke that does not match its view is refused and reported, and the relay pushes the
   set again on its next pass.

Relays act only on the published directory. A relay that dies mid-handoff loses nothing: the member's next relay
pushes the full set again and the publisher continues from published state. Without a majority nothing is
written, so no range becomes `ready` or is revoked. A member cut off past the grace period is removed from its
ranges; when it reconnects, the pushed set no longer lists them and it drops them.

## Model

`membership.tla` abstracts the mesh and the reconciliation pass as atomic. Relays are numbers and the lowest live
relay of a majority side publishes. Relay partitions are two transitive sides. `Tick` advances absence clocks:
one per relay and node (`gone`), plus the node's real absence (`gap`). Nodes connect, disconnect, drain, leave
gracefully and restart; relays crash, recover and change sides.

Invariants and properties:

- `OnePublisher`: two relays never publish at once.
- `ClockBound`: without a past split, no relay's clock runs ahead of the node's real absence.
- `NoEarlyRemoval`: an entry is deleted only after a graceful leave, a full grace period of real absence, or a
  relay split.
- `Freeze`: with no majority anywhere, the published view and directory do not change.

The directory adds `Points`, the possible range starts, and `Write(r, p, o)`, which writes owners `o` at start
`p` (a new start splits the covering range). `Write` allows any plan within the assigner's rules, so the model
checks those rules rather than `assign`'s balance:

- `Covered`: start 0 always exists, so the implicit range ends cover the hash space.
- `NewOwnersPublished`: only published, non-draining members whose published storage is healthy become new
  owners. `Sicken` toggles a node's storage health (`sick`); `Set` publishes it with the status (`pubSick`).
- `NoEarlyDrop`: a range with a holder (`ready` or `leaving`) still in membership keeps a holder. A `leaving` owner
  goes only when every other owner of the entry is ready.

Owners are never demoted from `ready` to `populating`.

The model is one dataset's directory; datasets have independent directories. `Carriers` are the nodes carrying
that dataset, and `NewOwnersPublished` also requires a new owner to be one.

`handoff.tla` checks the handoff on the same directory rules, with membership and the relay mesh abstract: a member
is published while connected and removed only while disconnected, and `quorum` stands for a relay majority. Combined
with this model's relay and clock state, the handoff was too large for TLC: a two-node check with three directory
revisions passed 132 million distinct states without finishing. Per node and point it adds what relays last pushed
(`told`), unprocessed pushed frames in order (`chan`), whether the node follows and holds the point's data (`follow`,
`copy`), and what the node's own, possibly lagging, directory view says about the point (`view`): its own state
and whether another owner is ready, the only facts the node reads from it. Each frame records whether the node's view has
reached the revision the frame was derived from (`ok`), which is all a revoke's revision is used for; recording
the revision itself made the state space too large. `Push` sends a change of the published directory, `Receive`
follows or confirms and drops, `Copy` abstracts bootstrap and catch-up (relays.tla checks those), and `Observe`
catches the node's view up. A disconnect loses the node's frames, and its
next relay pushes everything again. `Write` marks an owner `ready` only from `ReadyFor`: the node is connected,
processed every pushed frame, follows the range and holds it. With `Faulty`, `BadRevoke` lets a relay revoke on its
own unpublished view. `Bound` limits directory revisions to `MaxRev` and each frame queue to two. With `Capped`, it
also limits a run to `MaxCuts` disconnects and `MaxFlips` quorum changes, so TLC reaches deeper directory histories;
uncapped configs keep both unlimited at shallower depth.

- `ReadyHasData`: every `ready` or `leaving` owner holds the data, so no range is dropped before its new owners
  are ready, even after a faulty revoke.
- `ServesHeld`: a node that serves a point from its own view (`ready` or `leaving` there) holds its data.
- `NewOwnersPublished`, `NoEarlyDrop` and `Freeze` as above, against the abstract membership.

Neither model covers dropping a `leaving` range early under disk pressure (deferred to B-14).

`membership.tla` does not model message delays or a sibling whose stream fails silently: a node that moves to a relay whose
mesh stream to the publisher is dead but not yet detected can be removed and re-added once. Asymmetric
partitions are not modeled. If relays A and B cannot reach each other but both reach C, both can publish
and flap the entries they disagree on. The model does not prove liveness, the Rust implementation or the
complete B-23 gate.

## Checks

With the [official TLA+ release](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0) jar saved as
`.codex-tmp/tla2tools.jar`, run from `docs/spec`:

```sh
java -XX:+UseParallelGC -cp ../../.codex-tmp/tla2tools.jar tlc2.TLC -workers 1 -metadir ../../.codex-tmp/tlc-membership -config membership.cfg membership.tla
```

TLC 1.8.0 on 2026-10-31 with `Relays = {1, 2, 3}` and `Grace = 2`:

| Nodes | Generated states | Distinct states | Depth | Result |
|---|---:|---:|---:|---|
| `{1}` (`membership.cfg`) | 215,721 | 21,564 | 16 | no violation |
| `{1, 2}` | 71,697,001 | 5,180,040 | 22 | no violation (9 min 51 s) |

Negative checks in ignored temporary copies, 2026-10-31: publishing without a majority violates `OnePublisher`
at depth 2. Deleting on a draining status instead of a live `LEFT` marker violates `NoEarlyRemoval` at depth 6.
An earlier draft also restarted every clock when a relay's live set grew. Removing that rule violated nothing,
because a sibling counts as live only once its snapshot is synced, so the rule was dropped from the model and the
implementation.

With the directory, TLC 1.8.0 on 2026-11-01 with `Grace = 2` and `Points = {0, 1}`:

| Relays | Nodes | Generated states | Distinct states | Depth | Result |
|---|---|---:|---:|---:|---|
| `{1, 2, 3}` | `{1}` (`membership.cfg`) | 4,924,193 | 431,280 | 21 | no violation (57 s) |
| `{1}` | `{1, 2}` | 19,265,153 | 1,664,640 | 25 | no violation (2 min 8 s) |

The first draft of `Write` let a `ready` owner become `populating` again, which violated `NoEarlyDrop` at depth 6.
Negative checks in ignored temporary copies, 2026-11-01: revoking a `leaving` owner without a `ready` one violates
`NoEarlyDrop` at depth 8. Dropping owners still in membership violates it at depth 9. Giving ranges to draining
members violates `NewOwnersPublished` at depth 6. Writing from a relay without a majority violates `Freeze` at
depth 5.

With storage health (#13 / B-14), TLC 1.8.0 on 2026-11-01 with `Grace = 2` and `Points = {0, 1}`:

| Relays | Nodes | Generated states | Distinct states | Depth | Result |
|---|---|---:|---:|---:|---|
| `{1, 2, 3}` | `{1}` (`membership.cfg`) | 17,684,369 | 1,437,600 | 23 | no violation (1 min 17 s) |
| `{1}` | `{1, 2}` | 237,939,713 | 18,496,000 | 28 | no violation (24 min 11 s) |

Negative check in an ignored temporary copy, 2026-11-01: letting `Write` give ranges to a member whose published
storage is unhealthy violates `NewOwnersPublished` at depth 6.

After merging storage health with per-dataset `Carriers` (2026-11-03), TLC `2026.10.28.014557` checked
`membership.cfg`: 17,684,369 generated and 1,437,600 distinct states, depth 24, no violation (1 min 12 s). With one
node carrying the dataset the counts match the storage-health run; this TLC build reports the depth as 24.

With `Carriers`, TLC `2026.10.28.014557` (rev `56d37a9`) on 2026-11-02 checked `membership.cfg`
(`Carriers = {1}`): 4,924,193 generated and 431,280 distinct states, depth 22, no violation (12 s). The state
counts match the 2026-11-01 run; this TLC build reports the depth as 22.

The handoff model, run from `docs/spec`:

```sh
java -XX:+UseParallelGC -cp ../../.codex-tmp/tla2tools.jar tlc2.TLC -workers 6 -checkpoint 5 -metadir ../../.codex-tmp/tlc-handoff -config handoff.cfg handoff.tla
```

The same TLC build on 2026-11-02, with `Nodes = {n1, n2}` under symmetry, both carrying the dataset, faulty
revokes enabled and no caps:

| Points | `MaxRev` | Generated states | Distinct states | Depth | Result |
|---|---:|---:|---:|---:|---|
| `{0}` | 4 | 28,163,121 | 2,782,754 | 40 | no violation (28 s) |
| `{0}` (`handoff.cfg`) | 6 | 83,417,417 | 8,237,636 | 46 | no violation (1 min 37 s) |
| `{0, 1}`, no faulty revokes | 3 | 965,875,582 | 88,204,865 | 48 | no violation (51 min 50 s after resuming from a checkpoint) |

With a refused revoke making the relay push again (2026-11-03), TLC 1.8.0 checked `handoff.cfg` with 4 workers:
124,714,070 generated and 11,769,576 distinct states, depth 49, no violation (6 min 32 s).

With two points ranges split. Faulty revokes act per point, so the one-point runs cover them; leaving them out keeps
the two-point run small enough to finish. Capped two-point runs with deeper revisions have not finished on this machine.

Negative checks in ignored temporary copies, 2026-11-02, with `Points = {0}` and `MaxRev = 4` unless stated:

- Confirming a revoke without checking the node's view violates `ReadyHasData` at depth 15.
- Marking an owner `ready` without its data violates `ReadyHasData` at depth 12.
- Giving a range to a node that does not carry the dataset (`Carriers = {n1}`) violates `NewOwnersPublished` at
  depth 8.
- Writing the directory without a quorum violates `Freeze` at depth 9.
- Confirming a revoke before the node's view reached its revision violates `ReadyHasData` at depth 20.
- Counting readiness before the node processed every pushed frame violates `ReadyHasData` at depth 22
  (`MaxRev = 6`, no faulty revokes). The trace: the range is revoked and assigned again, the relay counts the old
  stream's acknowledgements, the owner is published `ready`, and the node then processes the queued revoke and
  deletes the range.

The first attempt combined the handoff with this model. Even with three directory revisions it passed 132 million
distinct states without finishing, and an exact revision on every frame kept the separate model too large until
frames recorded only whether the node's view had reached it.

Implementation evidence is separate. Unit tests cover `membership::plan`, the relay member list and
configuration validation. `tests/sim/tests/membership.rs` runs the real `bedrock-api` and `bedrock-relay` entry
points for seeds 1, 2 and 42, twice each, comparing traces. It covers join, relay failover, restart within grace,
drain, a cut-off node, a killed node, all relays down, and service and cell isolation. Its placement scenario
starts four CDN members in one cell and one in another. Every node sees three ready owners per range in its own
cell, and a killed member's ranges move to the others only after the grace period. Unit tests cover
`membership::place`, directory decoding, `placement::handoff` and the stale-view owner check. Replay one seed with
`BEDROCK_MEMBERSHIP_SEED=42` and the simulation flags in [relays.md](relays.md#checks-and-load-measurement).

On 2026-10-31 in WSL Ubuntu, every check listed in [relays.md](relays.md#checks-and-load-measurement) passed:
format, native and simulation clippy, native tests (176 shared-core unit tests) and simulation tests (129
shared-core unit tests). That includes the membership scenario, whose seeds 1, 2 and 42 each produced the same
trace twice. In that scenario, a drained member left within 0.1 simulated seconds. A member cut off from every
relay left about 50 s after the cut: the 30 s stream deadline plus the 20 s grace period. A killed member left
20.1 s after its process stopped. The full B-23 gate, nightly history and real deployed crash tests remain
separate work.

On 2026-11-01 in WSL Ubuntu, with the placement directory, the same checks passed. Native tests: 217 passed in 34
suites. Simulation tests: 176 passed in 28 suites. Each placement seed (1, 2 and 42) produced the same trace twice:
three ready owners per range 1.3 simulated seconds after start, and a killed member's ranges reassigned about
20 s after it stopped.

On 2026-11-01 in WSL Ubuntu, with storage health, the same checks passed. Native tests: 235 passed. Simulation
tests: 197 passed in 28 suites. The placement scenario now also corrupts one member's wrapped engine: every node's
view shows the dataset as unhealthy 0.1 simulated seconds after the corrupt read, and healthy again 0.1 s after the
recovery. Seeds 1, 2 and 42 each produced the same trace twice. Unit tests cover quarantine and isolation in
`health::storage`, readiness with a quarantined dataset, entry validation, relay member updates, session updates,
placement without new ranges for unhealthy members, and source selection skipping them per dataset.

On 2026-11-02, with relay-derived readiness (#22 / B-21 phase 4), owners become `ready` only after their relays
report them so, and `placement::promote` is gone. The placement scenario now waits until the directory has not
changed for 5 s before killing a member, since rebalancing passes through placed states between handoffs. Seeds 1, 2
and 42 each produced the same trace twice: placement settled 8.4–9.3 simulated seconds after start, and the killed
member's ranges were reassigned about 21 s after it stopped. Native tests: 240 passed; simulation tests: 200 passed,
28 suites each.

On 2026-11-03, with each ranged dataset's namespaces fixed in code and the CDN placing and following `cdn` ranges
(the `artifacts` label is gone), the same checks passed in WSL Ubuntu: format, native and simulation clippy, native
tests (256 passed) and simulation tests (217 passed). A relay now counts every fixed namespace toward readiness, even
one no write has reached yet, so a fresh `cdn` dataset still gets ready owners in the placement scenario. The
handoff scenario (`data_follows_ownership_through_faults_and_replays_per_seed`) writes to both `cdn` namespaces
through `bedrock-api` while members join, fail over, restart, are cut off, are killed and drain, and the relays lose
their majority. It checks that every holder keeps exactly the data of its own ranges. Seeds 1, 2 and 42 each
produced the same trace twice in all three membership scenarios.
