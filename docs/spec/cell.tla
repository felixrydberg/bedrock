------------------------------- MODULE cell -------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* Cell-local authority, 2026-11-09 (#15 / B-12, #19 / B-16, #68 / B-21b).
\* Raft supplies a single chosen log and leader completeness. This model checks Bedrock's
\* integration boundary: a quorum persists before commitment/acknowledgement; only the
\* committed prefix applies; data and applied position persist together; CAS is checked
\* during ordered application. It abstracts Raft's voting/log-matching algorithm, which
\* is implemented by the embedded library, not by Bedrock's membership observations.
\* Buffered suffixes and local application may lag; crashes discard only volatile state.
\* Relays denotes the fixed, persisted voter configuration. A transport peer is admitted
\* only after its configured region, DC, cell and voter addresses match this group's identity.
\* Each address additionally pins a distinct certificate node identity, persisted and exchanged
\* with the group configuration. Both inbound sender and outbound destination must match that
\* identity over mutual TLS; host SANs alone cannot distinguish processes sharing a host.
CONSTANTS Relays, Limit, Bug
VARIABLES chosen, durable, pending, up, applied, state, acked
vars == <<chosen, durable, pending, up, applied, state, acked>>
Empty == [revision |-> 0, stage |-> 0]
Commands == {[base |-> b, stage |-> b + 1] : b \in 0..(Limit - 1)}
Prefix(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a
Quorum(q) == q \subseteq Relays /\ 2 * Cardinality(q) > Cardinality(Relays)
Init == /\ chosen = <<>> /\ acked = 0
        /\ durable = [r \in Relays |-> <<>>] /\ pending = [r \in Relays |-> <<>>]
        /\ up = Relays /\ applied = [r \in Relays |-> 0]
        /\ state = [r \in Relays |-> Empty]
\* Replication can deliver the chosen prefix plus one uncommitted proposal. Raft's
\* log matching prevents replacement of a chosen prefix; it may replace an unchosen suffix.
Replicate(r, c) == /\ r \in up /\ Len(chosen) < Limit
                   /\ pending' = [pending EXCEPT ![r] = Append(chosen, c)]
                   /\ UNCHANGED <<chosen, durable, up, applied, state, acked>>
Persist(r) == /\ r \in up /\ pending[r] # durable[r] /\ Prefix(chosen, pending[r])
              /\ durable' = [durable EXCEPT ![r] = pending[r]]
              /\ UNCHANGED <<chosen, pending, up, applied, state, acked>>
Choose(c, q) == LET next == Append(chosen, c) IN
               /\ Len(chosen) < Limit /\ Quorum(q) /\ q \subseteq up
               /\ \A r \in q : IF Bug = "buffered" THEN Prefix(next, pending[r])
                                    ELSE Prefix(next, durable[r])
               /\ chosen' = next
               /\ UNCHANGED <<durable, pending, up, applied, state, acked>>
Ack == /\ acked < Len(chosen) /\ acked' = Len(chosen)
       /\ UNCHANGED <<chosen, durable, pending, up, applied, state>>
\* A stale planner can propose a command, but its expected revision is checked at apply.
Apply(r) == LET n == applied[r] + 1
                source == IF Bug = "uncommitted" THEN durable[r] ELSE chosen
                c == source[n]
                accept == c.base = state[r].revision \/ Bug = "cas"
            IN /\ r \in up /\ n <= Len(source) /\ n <= Len(durable[r])
               /\ state' = [state EXCEPT ![r] = IF accept
                    THEN [revision |-> @.revision + 1, stage |-> c.stage] ELSE @]
               /\ applied' = IF Bug = "torn" THEN applied ELSE [applied EXCEPT ![r] = n]
               /\ UNCHANGED <<chosen, durable, pending, up, acked>>
Crash(r) == /\ r \in up /\ up' = up \ {r}
            /\ pending' = [pending EXCEPT ![r] = durable[r]]
            /\ UNCHANGED <<chosen, durable, applied, state, acked>>
Restart(r) == /\ r \notin up /\ up' = up \cup {r}
              /\ UNCHANGED <<chosen, durable, pending, applied, state, acked>>
Next == \/ Ack
        \/ \E r \in Relays : Persist(r) \/ Apply(r) \/ Crash(r) \/ Restart(r)
        \/ \E r \in Relays, c \in Commands : Replicate(r, c)
        \/ \E c \in Commands, q \in SUBSET Relays : Choose(c, q)
Spec == Init /\ [][Next]_vars
TypeOK == /\ Len(chosen) <= Limit /\ acked \in 0..Limit
          /\ applied \in [Relays -> 0..Limit] /\ up \subseteq Relays
\* Chosen/acknowledged entries remain on a durable majority, including after crashes.
DurableCommit == \E q \in SUBSET Relays : Quorum(q) /\ \A r \in q : Prefix(chosen, durable[r])
CommittedApply == \A r \in Relays : applied[r] <= Len(chosen)
\* Each command deliberately carries its planner's next stage; accepted stale bases break this.
OrderedCAS == \A r \in Relays : state[r].stage = state[r].revision
AtomicApply == \A r \in Relays : state[r].revision <= applied[r]
=============================================================================
