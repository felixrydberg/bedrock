------------------------------ MODULE alarms ------------------------------
EXTENDS Naturals, TLC
\* #117 / S-09. Quorum commits and single-writer leases compose with volume.tla.
\* One actor, two successive alarm times, one delayed Wake, three abstract slots.
CONSTANT Bug
VARIABLES clock, alarm, durable, holder, expired, wake, running, valid, lateOnly
vars == <<clock, alarm, durable, holder, expired, wake, running, valid, lateOnly>>
Times == {1, 2}
Due(t) == t # 0 /\ clock >= t
Eligible == holder = "released" \/ (holder = "crashed" /\ expired /\ Bug # "released-only")
Init == /\ clock = 0 /\ alarm = 1 /\ durable = 1
        /\ holder = "live" /\ expired = FALSE
        /\ wake = 0 /\ running = 0 /\ valid = TRUE /\ lateOnly = TRUE
Tick == /\ clock < 7 /\ clock' = clock + 1
        /\ UNCHANGED <<alarm, durable, holder, expired, wake, running, valid, lateOnly>>
Release == /\ holder = "live" /\ running = 0 /\ holder' = "released"
           /\ UNCHANGED <<clock, alarm, durable, expired, wake, running, valid, lateOnly>>
Crash == /\ holder = "live" /\ holder' = "crashed" /\ running' = 0 /\ expired' = FALSE
         /\ UNCHANGED <<clock, alarm, durable, wake, valid, lateOnly>>
Expire == /\ holder = "crashed" /\ ~expired /\ expired' = TRUE
          /\ UNCHANGED <<clock, alarm, durable, holder, wake, running, valid, lateOnly>>
\* A normal request can wake the actor while an older alarm hint remains in flight.
Acquire == /\ holder = "released" \/ (holder = "crashed" /\ expired)
           /\ holder' = "live" /\ expired' = FALSE
           /\ UNCHANGED <<clock, alarm, durable, wake, running, valid, lateOnly>>
Scan(fallback) ==
    /\ Eligible /\ Due(alarm)
    /\ (~fallback \/ clock >= alarm + 5 \/ Bug = "early-fallback")
    /\ wake' = alarm
    /\ lateOnly' = (lateOnly /\ (~fallback \/ clock >= alarm + 5))
    /\ alarm' = IF Bug = "consume" THEN 0 ELSE alarm
    /\ UNCHANGED <<clock, durable, holder, expired, running, valid>>
Primary == Scan(FALSE)
Fallback == Scan(TRUE)
\* Loss, failure or a lost success reply consumes only the volatile send. A scan retries.
Lost == /\ wake # 0 /\ wake' = 0
        /\ UNCHANGED <<clock, alarm, durable, holder, expired, running, valid, lateOnly>>
Deliver ==
    /\ wake # 0 /\ running = 0 /\ (holder # "crashed" \/ expired)
    /\ (Bug = "stale" \/ (wake = alarm /\ Due(alarm)))
    /\ running' = wake /\ holder' = "live" /\ expired' = FALSE /\ wake' = 0
    /\ valid' = (valid /\ (wake = alarm /\ Due(alarm)))
    /\ UNCHANGED <<clock, alarm, durable, lateOnly>>
Timer == /\ holder = "live" /\ running = 0 /\ Due(alarm) /\ running' = alarm
         /\ UNCHANGED <<clock, alarm, durable, holder, expired, wake, valid, lateOnly>>
\* Only an acknowledged handler commit clears or reschedules; a crash beforehand keeps it.
Commit(t) == /\ holder = "live" /\ running # 0 /\ t \in {0, 2}
             /\ alarm' = t /\ durable' = t /\ running' = 0
             /\ UNCHANGED <<clock, holder, expired, wake, valid, lateOnly>>
Next == Tick \/ Release \/ Crash \/ Expire \/ Acquire \/ Primary \/ Fallback \/ Lost \/ Deliver \/ Timer
        \/ (\E t \in {0, 2} : Commit(t))
Spec == Init /\ [][Next]_vars
TypeOK == /\ clock \in 0..7 /\ alarm \in {0, 1, 2} /\ durable \in {0, 1, 2}
          /\ holder \in {"live", "released", "crashed"} /\ expired \in BOOLEAN
          /\ wake \in {0, 1, 2} /\ running \in {0, 1, 2} /\ valid \in BOOLEAN /\ lateOnly \in BOOLEAN
CommitOnly == alarm = durable
CurrentAlarmOnly == valid
LateFallbackOnly == lateOnly
\* Recovery remains enabled after a process crash even without a released-index row.
CrashRecoverable == (holder = "crashed" /\ expired /\ Due(alarm)) => ENABLED Primary
=============================================================================
