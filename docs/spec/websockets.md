# Hibernatable WebSockets (#123 / S-15)

`websockets.tla` checks the socket lifecycle and event output protocol before its
runtime implementation. It composes with `volume.tla`: a roster write is an
acknowledged, recovered actor-volume commit, and `actor = "serving"` means the
single holder still has serving permission. It adds no quorum, membership,
placement epoch, heartbeat or replay protocol.

The finite bounds are two Gateways, two event scopes (one socket input followed
by one alarm), three lifetime-unique connection IDs, one restart per Gateway,
one holder replacement, and one queued frame per socket. The third ID belongs
to the first Gateway's replacement incarnation. Sleep and holder replacement preserve the durable
roster and the Gateway's sockets and attachments. The model's `boot` records
process lifetime only; it is not a wire membership epoch. Runtime IDs are unique
random IDs, generated with `bedrock_lib::uuid()`.

The full bounds are explicit in `websockets.cfg` and unchanged by the separate
`websockets-local.cfg` diagnostic. The latter has two Gateways, two original
socket IDs, one input event, two possible holder epochs and capacity one. Run
`TLC_WORKERS=1 TLC_HEAP=2g scripts/tlc.sh websockets-local` for a quicker
diagnostic. It does not check replacement IDs, an alarm event, inter-event
reordering/epoch admission, or queue overflow between events; its registered
negative controls exclude those cases. A local pass is not a full-model pass.

The roster holds connection references and their owning Gateway. Attachments
exist only at that Gateway and disappear on close or restart. Acceptance follows
the roster commit. A delivered close removes the reference; a lost close leaves
a candidate. A certified owner's NotFound can remove it; a timeout preserves it
as unavailable. Wake recovers the candidate roster, not a claim that every
candidate is still live.

An explicit actor close retires its ID even while the owner is unreachable.
The physical socket may remain open temporarily, but later input for that ID
is refused. IDs are never reused, including after Gateway restart.

Each event stages output in its own scope. Emission requires successful handler
completion, the required writes' committed acknowledgements, the same active
scope and a currently serving holder. Failure, cancellation, fencing and scope
expiry cannot emit effects. An alarm has the same gate even though it has no
response payload or incoming socket. Callback acceptance checks actor identity,
owning Gateway and the lifetime-unique socket ID. Output stays ordered for each
socket. Callback emission and arrival are separate actions: an already emitted
packet may arrive after the sender loses its lease. The Gateway remembers the
latest `(Writer.epoch, event number)` per socket and refuses duplicate or older
tuples, including an old packet delayed past a newer holder's output. The event
number resets on wake; `EpochAdmission` checks that the greater writer epoch
still admits the new event after that reset. This is volatile ordering and
fencing, not durable deduplication. An ambiguous inbound dispatch closes its socket; neither inbound nor
outbound ambiguity permits automatic replay. A full queue closes only that
socket. A full unrelated queue cannot disable a healthy socket's callback.

The model stages at most one abstract frame per `(event, socket)`. The runtime
can batch multiple sends, attachment changes and close effects under one event
tuple; transport tests must check within-batch ordering, overflow and outcomes.
The queue bound models per-socket messages, not byte/global memory accounting.

The invariants have deliberate negative controls registered in `scripts/tlc.sh`:

| Bug | Required failure |
| --- | --- |
| `accept-before-commit` | `AcceptedRecorded` |
| `forget-roster` | `RecoveredRoster` |
| `keep-attachment` | `AttachmentLifetime` |
| `reuse-id` | `IncarnationSafe` |
| `ungated`, `old-effects` | `OutputGated` |
| `unchecked-owner` | `OwnerBound` |
| `timeout-prune` | `UnknownRetained` |
| `retired-input` | `RetiredInputRefused` |
| `replay-input`, `replay-output` | `NoReplay` |
| `stale-callback` | `OutputOrdered` |
| `event-only` | `EpochAdmission` |
| `overflow-all` | `OverflowIsolated` |
| `unbounded` | `QueueBound` |
| `slow-stalls` | `HealthyNotStalled` |

Run `TLC_WORKERS=1 TLC_HEAP=2g scripts/tlc.sh websockets` for the model and all
its deliberate bugs. Local validation on 2026-11-27 used one worker and a 2 GiB
heap: the diagnostic config completed 69,546 distinct states at depth 27, and
all 16 full-bounds deliberate bugs broke their intended invariants. The full
baseline exploration was stopped after reporting 32,309,237 distinct states at
depth 32; it has no completed passing result. The full config remains unchanged
for longer runs. `HealthyNotStalled` is an enabledness
check; this model does not prove network progress, callback deadlines, exact live
enumeration during owner outages, permanently lost Gateway garbage collection,
WebSocket framing, TLS validation or real-socket behavior. Runtime/transport
tests must cover those boundaries. No exactly-once or client-delivery guarantee
is implied by callback acceptance into the Gateway's bounded queue.

## Runtime limits and mounting

Both Gateway and Compute configurations accept a `sockets` section. Unspecified
fields use `bedrock_lib::websockets::Limits` defaults:

| Budget | Default |
| --- | --- |
| Message / attachment | 64 KiB / 2 KiB |
| Pending per connection | 64 messages, 1 MiB (including in-flight input) |
| Gateway | 4,096 connections, 64 MiB pending across connections |
| Actor roster / event effects | 1,024 references / 128 effects, 1 MiB |
| Owner callbacks / input recovery | 4 concurrent, 1 s total callback budget / 15 s |

The Gateway exposes `Sockets::upgrade` as a mountable HTTP handler, plus internal
`Sockets.Lookup` and `Sockets.Apply` RPCs. The public listener remains B-57b / #140.
`Instance::event` carries typed socket events, ordinary requests and alarms through
the existing actor gate; its borrowed socket context stages outgoing effects.
The production customer Sandbox adapter remains S-10 / #118. The initial
multi-service contract uses a test executor over real actor/volume services.

`tests/sim/tests/websockets.rs` runs in normal and `cfg(madsim)` builds. Set
`BEDROCK_WEBSOCKETS_SEED` to replay one seed; the default checks seeds 1 and 2
twice and compares ordered observed execution, connection IDs and owner results.
Gateway owner contracts live under `crates/gateway/tests/unit/`. Connection
enumeration reports unavailable owners and lazily prunes certified missing IDs;
it does not infer permanent loss from an expired RPC deadline.

## Runtime validation (2026-11-27)

Validation includes the merged S-16 / #124 changes at main commit
`e1d5c3c7d763b7f15de10ea90cd1ea0eec6b74bd`.

| Check | Result |
| --- | --- |
| Formatting; normal and madsim workspace/all-target Clippy with `-D warnings` | Passed |
| Normal `cargo test --workspace` | 778 passed, 7 ignored |
| Real-socket S-15 contracts, normal and madsim | All three passed in each mode |
| WebSocket seeds 1–500 | Every seed passed and matched its second run |
| Independent-process WebSocket replay | Seeds 1, 42 and 500 matched observed trace fingerprints |
| Runtime negative controls | In-flight input accounting, stopped-host output, ambiguous replay and stalled-owner isolation each caught its intended regression |
| Core VOPR quick checks | All six passed, including retained seeds and deliberate bugs |
| Independent-process core VOPR replay | Seed 42 passed twice with trace `7fe29d61de904db1` |

The full madsim workspace run used `--no-fail-fast` and remains blocked by eight
upstream failures: seven unchanged Compute tests construct a blocking Tokio
runtime, which madsim rejects before they exercise production code; `actor_routing`
seed 1 fails at `alarm redelivery after holder crash within 20s`. A pristine
archive of the main commit above, built in its own fresh target directory,
reproduced the same routing assertion and seed. All those Compute tests passed
in the normal suite. The VOPR replay script did not parse libtest-prefixed
fingerprints; the successful two-process comparison used the test binary's
output directly.

Builds used one Cargo job and one test thread; TLC used one worker and a 2 GiB
heap. The core VOPR quick launcher's internal subprocess count followed the
host's available CPUs despite the test-thread limit; subsequent VOPR replay
and pristine-main checks were pinned to one CPU. The opt-in real-binary S3
crash suite and project-wide nightly history were not run.

Cleanup removed this task's `~/bedrock-123-db1e`,
`~/bedrock-123-db1e-madsim`, `~/bedrock-123-baseline-src` and
`~/bedrock-123-baseline-target` directories (about 49 GiB), plus temporary
archives, validation helpers and TLC state files. Validation logs and the
shared sccache were retained. No Docker artifacts or VHDX compaction were involved.
