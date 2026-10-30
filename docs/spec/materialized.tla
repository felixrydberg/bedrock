---------------------------- MODULE materialized ----------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS MaxSeq, MaxTime, EventWindow, RetryWindow, MaxEpoch, WithPosition
VARIABLES history, buffered, durable, now, epoch, bufferEpoch, pending,
          acknowledged, cleanupRequested
vars == <<history, buffered, durable, now, epoch, bufferEpoch, pending,
          acknowledged, cleanupRequested>>

State(s) == IF s = 0 THEN FALSE ELSE s % 2 = 1
EmptyRetry == [sequence |-> 0, expiry |-> 0]
EmptyDB == [revision |-> 0, at |-> 0, value |-> FALSE,
            events |-> [s \in {} |-> s], retry |-> EmptyRetry, position |-> 0]
At(s) == IF s = 0 THEN 0 ELSE history[s].at
Expiry(s) == IF s = 0 THEN 0 ELSE history[s].expiry
Position(s) == IF WithPosition /\ s > 0 THEN [index |-> s, term |-> 1] ELSE 0
LiveRetry == durable.retry.sequence > 0 /\ durable.retry.expiry > now

Init == /\ history = <<>> /\ buffered = EmptyDB /\ durable = EmptyDB
        /\ now = 0 /\ epoch = 1 /\ bufferEpoch = 1 /\ pending = 0
        /\ acknowledged = {} /\ cleanupRequested = FALSE

\* One namespace, key and retry identity. Stage is one engine WriteBatch:
\* original event + resolved SET/DELETE + revision/time + retry + optional position.
Stage == /\ buffered = durable /\ pending = 0
         /\ ~LiveRetry /\ durable.revision < MaxSeq
         /\ LET s == durable.revision + 1
                row == [at |-> now, expiry |-> now + RetryWindow,
                        value |-> State(s)]
            IN /\ history' = Append(history, row)
               /\ buffered' = [revision |-> s, at |-> now, value |-> State(s),
                    events |-> [i \in DOMAIN durable.events \cup {s} |->
                                  IF i = s THEN row ELSE durable.events[i]],
                    retry |-> [sequence |-> s, expiry |-> row.expiry],
                    position |-> Position(s)]
               /\ pending' = s
         /\ bufferEpoch' = epoch
         /\ UNCHANGED <<durable, now, epoch, acknowledged, cleanupRequested>>

\* Only completion of remote durable flush makes this batch recoverable.
Flush == /\ buffered # durable /\ bufferEpoch = epoch
         /\ durable' = buffered
         /\ UNCHANGED <<history, buffered, now, epoch, bufferEpoch, pending,
                         acknowledged, cleanupRequested>>
Ack == /\ pending > 0 /\ pending <= durable.revision
       /\ acknowledged' = acknowledged \cup {pending} /\ pending' = 0
       /\ cleanupRequested' = TRUE
       /\ UNCHANGED <<history, buffered, durable, now, epoch, bufferEpoch>>
Retry == /\ pending = 0 /\ LiveRetry
         /\ acknowledged' = acknowledged \cup {durable.retry.sequence}
         /\ cleanupRequested' = TRUE
         /\ UNCHANGED <<history, buffered, durable, now, epoch, bufferEpoch,
                         pending>>

\* Restart opens durable engine state; there is no separate journal to replay.
\* The epoch guard on Flush abstracts fenced single-publisher engine handles.
Crash == /\ epoch < MaxEpoch /\ epoch' = epoch + 1
         /\ buffered' = durable /\ bufferEpoch' = epoch + 1 /\ pending' = 0
         /\ history' = SubSeq(history, 1, durable.revision)
         /\ UNCHANGED <<durable, now, acknowledged, cleanupRequested>>
Tick == /\ now < MaxTime /\ now' = now + 1
        /\ UNCHANGED <<history, buffered, durable, epoch, bufferEpoch, pending,
                        acknowledged, cleanupRequested>>

\* Expiry is logical at now; physical cleanup is event-triggered and may fail.
\* It uses a separate atomic batch, never rewriting surviving fixed expiries.
StageCleanup == /\ cleanupRequested /\ pending = 0 /\ buffered = durable
                /\ buffered' = [durable EXCEPT
                     !.events = [s \in {i \in DOMAIN durable.events :
                                         durable.events[i].at + EventWindow > now}
                                  |-> durable.events[s]],
                     !.retry = IF LiveRetry THEN durable.retry ELSE EmptyRetry]
                /\ bufferEpoch' = epoch /\ cleanupRequested' = FALSE
                /\ UNCHANGED <<history, durable, now, epoch, pending, acknowledged>>
CleanupFailure == /\ cleanupRequested /\ cleanupRequested' = FALSE
                  /\ UNCHANGED <<history, buffered, durable, now, epoch,
                                  bufferEpoch, pending, acknowledged>>

Next == Stage \/ Flush \/ Ack \/ Retry \/ Crash \/ Tick
        \/ StageCleanup \/ CleanupFailure
Spec == Init /\ [][Next]_vars

Consistent(db) == /\ db.value = State(db.revision) /\ db.at = At(db.revision)
                  /\ db.position = Position(db.revision)
                  /\ \A s \in DOMAIN db.events :
                       s <= db.revision /\ db.events[s] = history[s]
                  /\ db.retry = EmptyRetry \/
                       (db.retry.sequence = db.revision /\
                        db.retry.expiry = Expiry(db.revision))
AtomicBuffered == Consistent(buffered)
AtomicDurable == Consistent(durable)
NoLostAck == acknowledged \subseteq 1..durable.revision
OrderedPositions == durable.revision <= buffered.revision
                    /\ buffered.revision = Len(history)
                    /\ buffered.revision <= durable.revision + 1
YoungEventsSurvive == \A s \in 1..durable.revision :
                        At(s) + EventWindow > now => s \in DOMAIN durable.events
RetryProtection == durable.revision > 0 /\ Expiry(durable.revision) > now =>
                     durable.retry = [sequence |-> durable.revision,
                                       expiry |-> Expiry(durable.revision)]
PendingRetained == pending > durable.revision =>
                     pending \in DOMAIN buffered.events
=============================================================================
