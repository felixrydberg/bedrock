------------------------ MODULE service_isolation ------------------------
EXTENDS Naturals, FiniteSets
\* #15 / B-12, #19 / B-16, #68 / B-21b: a failed service read must not
\* stop another service's publisher. Each service has one modeled member.
\* membership.tla checks the mesh/range rules; cell.tla checks durable ordered
\* application. Publish here is one atomic, quorum-durable CAS application.
\* Fault covers malformed/unsupported drain records and unavailable snapshots.
\* It never silently deletes a committed member or repairs an operator record.
CONSTANTS Services, Relays, Grace, MaxRevision, Bug
VARIABLES up, unreadable, validated, connected, gap, gone, current, view, lastBase
vars == <<up, unreadable, validated, connected, gap, gone, current, view, lastBase>>
Quorum == 2 * Cardinality(up) > Cardinality(Relays)
Empty == [revision |-> 0, member |-> FALSE]
Desired(s) == connected[s] \/ (view[s].member /\ gone[s] < Grace)
Eligible(s) == ((s \in validated /\ s \notin unreadable) \/ (Bug = "unreadable-write" /\ s \in unreadable))
              /\ (Bug # "global-freeze" \/ unreadable = {})
Min(a, b) == IF a < b THEN a ELSE b

Init == /\ up = Relays /\ unreadable = {} /\ validated = {}
        /\ connected = [s \in Services |-> FALSE]
        /\ gap = [s \in Services |-> 0] /\ gone = [s \in Services |-> 0]
        /\ current = [s \in Services |-> Empty]
        /\ view = [s \in Services |-> Empty]
        /\ lastBase = [s \in Services |-> 0]

\* A failed read invalidates this service's entire planning input. Reconnects
\* may pass unseen until its next successful read, so its old clock is unsafe.
Fault(s) == /\ s \notin unreadable
            /\ unreadable' = unreadable \cup {s}
            /\ validated' = validated \ {s}
            /\ gone' = IF Bug = "stale-clock" THEN gone ELSE [gone EXCEPT ![s] = 0]
            /\ UNCHANGED <<up, connected, gap, current, view, lastBase>>
Recover(s) == /\ s \in unreadable /\ unreadable' = unreadable \ {s}
              /\ UNCHANGED <<up, validated, connected, gap, gone, current, view, lastBase>>
Read(s) == /\ s \notin unreadable
           /\ view' = [view EXCEPT ![s] = current[s]]
           /\ gone' = [gone EXCEPT ![s] = IF connected[s] \/
                       (s \notin validated /\ Bug # "stale-clock") THEN 0 ELSE @]
           /\ validated' = validated \cup {s}
           /\ UNCHANGED <<up, unreadable, connected, gap, current, lastBase>>
Connect(s) == /\ ~connected[s] /\ connected' = [connected EXCEPT ![s] = TRUE]
              /\ gap' = [gap EXCEPT ![s] = 0]
              /\ gone' = IF s \in validated THEN [gone EXCEPT ![s] = 0] ELSE gone
              /\ UNCHANGED <<up, unreadable, validated, current, view, lastBase>>
Disconnect(s) == /\ connected[s] /\ connected' = [connected EXCEPT ![s] = FALSE]
                 /\ UNCHANGED <<up, unreadable, validated, gap, gone, current, view, lastBase>>
Tick == /\ gap' = [s \in Services |-> IF connected[s] THEN 0 ELSE Min(gap[s] + 1, Grace)]
        /\ gone' = [s \in Services |-> IF s \notin validated THEN gone[s]
                      ELSE IF connected[s] THEN 0 ELSE Min(gone[s] + 1, Grace)]
        /\ UNCHANGED <<up, unreadable, validated, connected, current, view, lastBase>>

\* A stale local subscription may plan, but service-local revision CAS rejects
\* it at ordered application. No service's eligibility changes the voter set.
Publish(s) == /\ Eligible(s) /\ (Quorum \/ Bug = "minority-write") /\ current[s].revision < MaxRevision
              /\ Desired(s) # view[s].member
              /\ (view[s].revision = current[s].revision \/ Bug = "cas")
              /\ current' = [current EXCEPT ![s] =
                    [revision |-> @.revision + 1, member |-> Desired(s)]]
              /\ lastBase' = [lastBase EXCEPT ![s] = view[s].revision]
              /\ UNCHANGED <<up, unreadable, validated, connected, gap, gone, view>>
\* An authorized operator can cancel the offending drain while its publisher
\* is frozen. This is an ordinary durable control write, preserving members;
\* it invalidates old service revisions and still needs the unchanged quorum.
Cancel(s) == /\ s \in unreadable /\ Quorum /\ current[s].revision < MaxRevision
             /\ unreadable' = unreadable \ {s}
             /\ current' = [current EXCEPT ![s].revision = @ + 1]
             /\ lastBase' = [lastBase EXCEPT ![s] = current[s].revision]
             /\ UNCHANGED <<up, validated, connected, gap, gone, view>>
Voters(q) == /\ up' = q
             /\ UNCHANGED <<unreadable, validated, connected, gap, gone, current, view, lastBase>>
Next == \/ Tick
        \/ \E s \in Services : Fault(s) \/ Recover(s) \/ Read(s) \/ Connect(s) \/ Disconnect(s) \/ Publish(s) \/ Cancel(s)
        \/ \E q \in SUBSET Relays : Voters(q)
Spec == Init /\ [][Next]_vars

TypeOK == /\ up \subseteq Relays /\ unreadable \subseteq Services /\ validated \subseteq Services
          /\ connected \in [Services -> BOOLEAN]
          /\ gap \in [Services -> 0..Grace] /\ gone \in [Services -> 0..Grace]
          /\ current \in [Services -> [revision : 0..MaxRevision, member : BOOLEAN]]
          /\ view \in [Services -> [revision : 0..MaxRevision, member : BOOLEAN]]
          /\ lastBase \in [Services -> 0..MaxRevision]
ClockBound == \A s \in validated : gone[s] <= gap[s]
OrderedCAS == \A s \in Services : current[s].revision = 0 \/ lastBase[s] + 1 = current[s].revision
\* A healthy service's next diff stays enabled even if every other service
\* fails validation. This is independence of enabling, not a fairness claim.
HealthyServiceEnabled == \A s \in Services :
    (s \in validated /\ s \notin unreadable /\ Quorum
     /\ current[s].revision < MaxRevision /\ view[s].revision = current[s].revision
     /\ Desired(s) # view[s].member) => ENABLED Publish(s)
ServiceFreeze == [][\A s \in unreadable : current'[s].member = current[s].member]_vars
NoEarlyRemoval == [][\A s \in Services :
    (current[s].member /\ ~current'[s].member) => gap[s] >= Grace]_vars
Freeze == [][~Quorum => current' = current]_vars
=============================================================================
