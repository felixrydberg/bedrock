------------------------------ MODULE kv_actor ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* #126 / S-18: KV semantics above the existing fenced volume quorum protocol.
CONSTANTS MaxSeq, MaxTime, MaxHolders, Quota, RetryWindow, EventWindow,
          CacheWindow, ValueWindow, Bug
Keys == {1, 2}
Ids == {1, 2}
EmptyRow == [value |-> 0, expiry |-> 0, bytes |-> 0]
EmptyRows == [k \in Keys |-> EmptyRow]
EmptyRetry == [revision |-> 0, expiry |-> 0]
EmptyDB == [revision |-> 0, rows |-> EmptyRows, bytes |-> 0,
            retries |-> [id \in Ids |-> EmptyRetry], events |-> [s \in {} |-> s]]
EmptyFill == [key |-> 1, row |-> EmptyRow, revision |-> 0, at |-> 0]
EmptyCache == [row |-> EmptyRow, revision |-> 0, until |-> 0, at |-> 0]
VARIABLES history, buffered, durable, pending, acknowledged, now, holder,
          flight, cache, highWater, minimumOK, visibilityOK, retryOK
vars == <<history, buffered, durable, pending, acknowledged, now, holder,
          flight, cache, highWater, minimumOK, visibilityOK, retryOK>>

RowsAt(s) == IF s = 0 THEN EmptyRows ELSE history[s].rows
Bytes(rows) == rows[1].bytes + rows[2].bytes
Visible(row, at) == IF row.value # 0 /\ row.expiry > at THEN row.value ELSE 0
Latest(id, s) == LET matches == {i \in 1..s : history[i].id = id}
                 IN IF matches = {} THEN 0
                    ELSE CHOOSE i \in matches : \A j \in matches : j <= i
RetryAt(id, s) == LET i == Latest(id, s)
                 IN IF i = 0 THEN EmptyRetry
                    ELSE [revision |-> i, expiry |-> history[i].at + RetryWindow]
LiveRetry(id) == durable.retries[id].revision > 0 /\ durable.retries[id].expiry > now
NewRow(k, s) == [value |-> s, expiry |-> now + ValueWindow, bytes |-> k + s]
Mutate(rows, mode, s) == CASE mode = "put" -> [rows EXCEPT ![1] = NewRow(1, s)]
                           [] mode = "bulk" -> [k \in Keys |-> NewRow(k, s)]
                           [] OTHER -> [rows EXCEPT ![1] = EmptyRow]

Init == /\ history = <<>> /\ buffered = EmptyDB /\ durable = EmptyDB
        /\ pending = 0 /\ acknowledged = {} /\ now = 0 /\ holder = 1
        /\ flight = EmptyFill /\ cache = [k \in Keys |-> EmptyCache]
        /\ highWater = [k \in Keys |-> 0]
        /\ minimumOK = TRUE /\ visibilityOK = TRUE /\ retryOK = TRUE

\* Data, namespace revision, exact physical bytes, retry and event form one entry.
Stage(id, mode) ==
    /\ pending = 0 /\ buffered = durable /\ ~LiveRetry(id)
    /\ durable.revision < MaxSeq
    /\ LET s == durable.revision + 1
           rows == Mutate(durable.rows, mode, s)
           event == [id |-> id, rows |-> rows, at |-> now]
       IN /\ (Bytes(rows) <= Quota \/ Bug = "quota")
          /\ history' = Append(history, event)
          /\ buffered' = [revision |-> s,
                rows |-> IF Bug = "bulk" /\ mode = "bulk"
                         THEN [rows EXCEPT ![2] = durable.rows[2]] ELSE rows,
                bytes |-> IF Bug = "bytes" THEN durable.bytes + Bytes(rows) ELSE Bytes(rows),
                retries |-> [durable.retries EXCEPT
                    ![id] = [revision |-> s, expiry |-> now + RetryWindow]],
                events |-> [i \in DOMAIN durable.events \cup {s} |->
                              IF i = s THEN event ELSE durable.events[i]]]
          /\ pending' = s
    /\ UNCHANGED <<durable, acknowledged, now, holder, flight, cache,
                    highWater, minimumOK, visibilityOK, retryOK>>

\* Commit abstracts the existing majority-fsynced, single-writer volume entry.
Commit == /\ pending > durable.revision
          /\ durable' = IF Bug = "torn" THEN [buffered EXCEPT !.rows = durable.rows]
                        ELSE buffered
          /\ UNCHANGED <<history, buffered, pending, acknowledged, now, holder,
                          flight, cache, highWater, minimumOK, visibilityOK, retryOK>>
Ack == /\ pending > 0
       /\ pending <= (IF Bug = "ack" THEN buffered.revision ELSE durable.revision)
       /\ acknowledged' = acknowledged \cup {pending} /\ pending' = 0
       /\ UNCHANGED <<history, buffered, durable, now, holder, flight, cache,
                       highWater, minimumOK, visibilityOK, retryOK>>

\* A claim no-op/holder move opens only committed state, without a KV revision.
Move == /\ holder < MaxHolders /\ holder' = holder + 1
        /\ LET recovered == IF Bug = "claim"
                            THEN [durable EXCEPT !.revision = @ + 1] ELSE durable
           IN /\ durable' = recovered /\ buffered' = recovered
        /\ history' = SubSeq(history, 1, durable.revision) /\ pending' = 0
        /\ UNCHANGED <<acknowledged, now, flight, cache, highWater,
                        minimumOK, visibilityOK, retryOK>>

Retry(id) == /\ pending = 0 /\ buffered = durable /\ LiveRetry(id)
             /\ retryOK' = (retryOK /\
                    ((IF Bug = "retry-result" THEN durable.revision
                      ELSE durable.retries[id].revision) = RetryAt(id, durable.revision).revision))
             /\ durable' = IF Bug = "retry-expiry" THEN
                   [durable EXCEPT !.retries[id].expiry = now + RetryWindow] ELSE durable
             /\ buffered' = durable'
             /\ UNCHANGED <<history, pending, acknowledged, now, holder, flight,
                             cache, highWater, minimumOK, visibilityOK>>

\* Cleanup may lag arbitrarily; expiry is enforced logically even without it.
Cleanup == /\ pending = 0 /\ buffered = durable
           /\ durable' = [durable EXCEPT
                !.events = [s \in {i \in DOMAIN durable.events :
                    history[i].at + EventWindow > now /\ Bug # "events"} |-> durable.events[s]],
                !.retries = [id \in Ids |-> IF LiveRetry(id) /\ Bug # "retry-drop"
                                          THEN durable.retries[id] ELSE EmptyRetry]]
           /\ buffered' = durable'
           /\ UNCHANGED <<history, pending, acknowledged, now, holder, flight,
                           cache, highWater, minimumOK, visibilityOK, retryOK>>

\* Network responses can be delayed across writes, expiry and holder changes.
Fetch(k) == /\ flight' = [key |-> k, row |-> durable.rows[k],
                         revision |-> durable.revision, at |-> now]
            /\ UNCHANGED <<history, buffered, durable, pending, acknowledged,
                            now, holder, cache, highWater, minimumOK, visibilityOK, retryOK>>
Fill(response) == /\ (response.revision >= highWater[response.key] \/ Bug = "cache-regress")
        /\ LET deadline == response.at + CacheWindow + (IF Bug = "cache-ttl" THEN 1 ELSE 0)
               until == IF response.row.value = 0 THEN deadline
                        ELSE IF deadline < response.row.expiry THEN deadline ELSE response.row.expiry
           IN cache' = [cache EXCEPT ![response.key] =
                 [row |-> response.row, revision |-> response.revision,
                  until |-> until, at |-> response.at]]
        /\ highWater' = [highWater EXCEPT ![response.key] =
             IF @ > response.revision THEN @ ELSE response.revision]
        /\ UNCHANGED <<history, buffered, durable, pending, acknowledged, now,
                        holder, flight, minimumOK, visibilityOK, retryOK>>
HolderRead(k, minimum) ==
    /\ durable.revision >= minimum
    /\ minimumOK' = (minimumOK /\ durable.revision >= minimum)
    /\ visibilityOK' = (visibilityOK /\
          ((IF Bug = "expired" THEN durable.rows[k].value ELSE Visible(durable.rows[k], now))
           = Visible(RowsAt(durable.revision)[k], now)))
    /\ UNCHANGED <<history, buffered, durable, pending, acknowledged, now,
                    holder, flight, cache, highWater, retryOK>>
CacheRead(k, minimum) ==
    /\ cache[k].until > now
    /\ (cache[k].revision >= minimum \/ Bug = "minimum")
    /\ minimumOK' = (minimumOK /\ cache[k].revision >= minimum)
    /\ visibilityOK' = (visibilityOK /\
          (Visible(cache[k].row, now) = Visible(RowsAt(cache[k].revision)[k], now)))
    /\ UNCHANGED <<history, buffered, durable, pending, acknowledged, now,
                    holder, flight, cache, highWater, retryOK>>
Tick == /\ now < MaxTime /\ now' = now + 1
        /\ UNCHANGED <<history, buffered, durable, pending, acknowledged, holder,
                        flight, cache, highWater, minimumOK, visibilityOK, retryOK>>

Next == \/ \E id \in Ids, mode \in {"put", "bulk", "delete"} : Stage(id, mode)
        \/ Commit \/ Ack \/ Move \/ Cleanup \/ Tick \/ Fill(flight)
        \/ \E id \in Ids : Retry(id)
        \/ \E k \in Keys : Fetch(k) \/ Fill([key |-> k, row |-> durable.rows[k],
                                             revision |-> durable.revision, at |-> now])
        \/ \E k \in Keys, minimum \in 0..MaxSeq : HolderRead(k, minimum) \/ CacheRead(k, minimum)
Spec == Init /\ [][Next]_vars

Consistent(db) == /\ db.revision \in 0..Len(history)
                  /\ db.rows = RowsAt(db.revision)
                  /\ db.bytes = Bytes(db.rows)
                  /\ \A id \in Ids : db.retries[id] \in {EmptyRetry, RetryAt(id, db.revision)}
                  /\ \A s \in DOMAIN db.events : s \in 1..db.revision /\ db.events[s] = history[s]
AtomicBuffered == Consistent(buffered)
AtomicCommitted == Consistent(durable)
OrderedRevisions == /\ durable.revision <= buffered.revision
                    /\ buffered.revision = Len(history)
                    /\ buffered.revision <= durable.revision + 1
NoLostAck == acknowledged \subseteq 1..durable.revision
ExactQuota == /\ buffered.bytes = Bytes(buffered.rows) /\ durable.bytes = Bytes(durable.rows)
              /\ buffered.bytes <= Quota /\ durable.bytes <= Quota
RetryProtection == \A id \in Ids :
    RetryAt(id, durable.revision).expiry > now => durable.retries[id] = RetryAt(id, durable.revision)
RetryOriginalResult == retryOK
YoungEventsSurvive == \A s \in 1..durable.revision :
    history[s].at + EventWindow > now => s \in DOMAIN durable.events
CacheMonotonic == \A k \in Keys : cache[k].revision = highWater[k]
CacheLifetime == \A k \in Keys :
    /\ cache[k].until <= cache[k].at + CacheWindow
    /\ cache[k].row.value # 0 => cache[k].until <= cache[k].row.expiry
ReadMinimum == minimumOK
ReadVisibility == visibilityOK
=============================================================================
