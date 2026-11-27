# Actor alarm delivery (#117 / S-09)

`alarms.tla` composes with `volume.tla`'s acknowledged atomic commits, recovery and
single-writer lease proof. It checks one actor with two successive alarm times,
one delayed or duplicated wake, a holder timer, a primary slot and late fallback
slots. Time is bounded to seven seconds; fallback starts five seconds late.
Normal requests can acquire an actor while an alarm hint is delayed; that race
also exercises delivery after the holder has cleared or rescheduled the alarm.

Only the handler's commit changes the alarm. Sending, losing a reply and crashing
before that commit retain it. Delivery checks the current alarm under the input
gate. A released actor is scanned through its durable index; a crashed held
actor is eligible after lease expiry (user decision 2026-11-23). Retry state is
volatile: loss re-enables scanning without consuming the durable alarm.

A quorum acknowledges a commit, but the slots learn it only from the holder's next
append or heartbeat (`known`, `learned`), so a holder can commit an alarm and crash
before any slot knows. The slots' hint is their committed alarm, or, when they know
none, the alarm in the tail past their commit point (`Store::due_alarms`, PR #155).
The model starts with alarm 1 either known or only in such a tail. A hint whose tail
never committed is not modeled; delivery and Compute's recovery reject it like any
stale hint.

`CommitOnly`, `CurrentAlarmOnly`, `LateFallbackOnly` and `CrashRecoverable` have
deliberate bugs `consume`, `stale`, `early-fallback` and `released-only`, registered
in `scripts/tlc.sh`. `committed-only`, which hints only committed alarms the slots
know, also breaks `CrashRecoverable`. Existing volume configurations and bounds are unchanged.
This safety/enabledness abstraction does not prove network progress, retry delay
bounds or the one-second scan latency; runtime and simulation tests cover them.

Run `scripts/tlc.sh alarms` for this model and its five deliberate bugs. The
normal transport contract is `cargo test -p bedrock-sim --test actor_alarms`.
With `RUSTFLAGS="--cfg madsim"`, run `actor_alarms`, `actor_routing`, `actor_host`
and `compute_binary` in that crate. The alarm contract replays seeds 1–3 and the
Gateway scenario replays seeds 1–2; set `BEDROCK_ACTOR_ALARMS_SEED` or
`BEDROCK_ACTOR_ROUTING_SEED` to replay one seed. Use separate per-checkout target
directories for normal and simulation builds as required by `AGENTS.md`.
