# Cell membership (#15 / B-12) and L2 placement directory (#19 / B-16)

A member is a `bedrock-quicksilver`, `bedrock-kv` or `bedrock-cdn` process that is receiving updates: it holds a leaf
`Relay.Session` to a relay of its own cell. There is no heartbeat or gossip protocol and no epoch.

## Protocol

- **Handshake.** A leaf `Hello` carries a `Member`: node ID, service, region, DC, cell, roles, datasets,
  CPU/memory/disk budget, failure domain, advertised address and status (`ready`/`draining`). Later
  `RelayRequest.member` messages may change only the status of the same `(service, node_id)`. Relay-to-relay
  and recovery sessions carry no member. Identities are ASCII letters, digits, `-`, `_` and `.`.
- **Relay view.** Each relay lists the members on its leaf sessions, the newest session winning a duplicate
  identity, with each subscription's acknowledged revision for the regional directory (#20 / B-19). When a
  draining member's stream closes, the relay keeps it as relay-local `LEFT` for 30 seconds.
- **Siblings.** A cell's relays are each relay plus its `replay_peers`, listed by advertised address. Every relay
  serves `Siblings.Watch`, and a cell relay with `[membership]` watches each sibling: a snapshot completed by
  `synced`, then changes, with progress ticks every 10 seconds and the 30-second stream deadline. A relay shares
  only its own leaf members, never what it learned from a sibling. A sibling counts as live only while its synced
  stream is up. #41 / B-12b reuses this stream for subscriber interests at every tier.
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

- **Entries.** Key `range/{start}` (16 lowercase hex digits) in the same `{service}.{cell}` namespace, value an
  encoded `Range`: owners with state `populating`, `ready` or `leaving`. A range ends where the next starts, or at
  2⁶⁴, so every single write leaves the hash space covered with no gaps or overlaps. Node IDs cannot contain `/`,
  so entries never collide with members. Membership and placement share the namespace's one revision order.
- **Assignment.** Only the membership publisher writes, and only once membership has nothing left to write, so it
  plans from applied membership. Each pass either promotes the last pass's handoffs or runs `placement::assign`
  (#18 / B-15) with 3 replicas and at most 16 ranges moving. It then waits for its subscription to apply the writes,
  as for membership.
- **Interim readiness.** Until #22 / B-21 derives readiness from the relays' bootstrap, promotion marks
  `populating` owners `ready` and removes `leaving` ones on the next pass. Nothing reads L2 by range yet
  (`quicksilver` is `full_replica`, `auth` on-demand), so no reader relies on the claim. A range whose owners are
  all leaving keeps them.
- **Owner checks.** Nodes read the directory from `membership::View::directory`. `placement::owners` lists the owners
  of a hash; a node serves a hash only if `placement::serves` says it owns or is leaving it in its own view. A caller
  routed by a stale view is rejected and retries the next owner. Revisions are ordinary dataset revisions, not a
  placement epoch.
- **Freeze.** Without a majority nothing is written, so placement freezes with membership and nodes keep serving
  what they hold.

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
- `NewOwnersPublished`: only published, non-draining members become new owners.
- `NoEarlyDrop`: a range with a holder (`ready` or `leaving`) still in membership keeps a holder. A `leaving` owner
  goes only when every other owner of the entry is ready.

Owners are never demoted from `ready` to `populating`.

It does not model message delays or a sibling whose stream fails silently: a node that moves to a relay whose
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

Implementation evidence is separate. Unit tests cover `membership::plan`, the relay member list and
configuration validation. `tests/sim/tests/membership.rs` runs the real `bedrock-api` and `bedrock-relay` entry
points for seeds 1, 2 and 42, twice each, comparing traces. It covers join, relay failover, restart within grace,
drain, a cut-off node, a killed node, all relays down, and service and cell isolation. Its placement scenario
starts four CDN members in one cell and one in another. Every node sees three ready owners per range in its own
cell, and a killed member's ranges move to the others only after the grace period. Unit tests cover
`membership::place`, directory decoding, `placement::promote` and the stale-view owner check. Replay one seed with
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
