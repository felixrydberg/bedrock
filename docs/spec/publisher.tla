---------------------------- MODULE publisher ----------------------------
EXTENDS Naturals, FiniteSets
\* #25 / B-23: directory writes are planned from a relay's view and land later, so a slow write can
\* arrive after newer ones. membership.tla writes atomically and cannot show that. Here any relay may
\* plan at any time, which also covers two relays that each believe they are the publisher while
\* sibling state is still syncing. A write carries the revision it was planned from.
\* `stage` stands for directory progress: a write moves one step on from what its planner saw.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
\* "unconditional" lands writes regardless of the revision; "ahead" lets a view claim an uncommitted revision.
CONSTANTS Relays, MaxRevision, Bug
VARIABLES rev, stage, view, inflight
vars == <<rev, stage, view, inflight>>

Init == /\ rev = 0 /\ stage = 0
        /\ view = [r \in Relays |-> [rev |-> 0, stage |-> 0]]
        /\ inflight = {}

\* The relay's subscription applies the namespace; views can lag it by any amount.
Refresh(r) == /\ view' = [view EXCEPT ![r] = [rev |-> rev + IF Bug = "ahead" THEN 1 ELSE 0, stage |-> stage]]
              /\ UNCHANGED <<rev, stage, inflight>>
\* The relay plans from its view and sends the write.
Plan(r) == /\ rev < MaxRevision
           /\ inflight' = inflight \cup {[base |-> view[r].rev, stage |-> view[r].stage + 1]}
           /\ UNCHANGED <<rev, stage, view>>
\* The writer accepts a write only at the revision it was planned from.
Land(w) == /\ inflight' = inflight \ {w}
           /\ IF Bug = "unconditional" \/ w.base = rev
                 THEN /\ rev < MaxRevision /\ rev' = rev + 1 /\ stage' = w.stage
                 ELSE UNCHANGED <<rev, stage>>
           /\ UNCHANGED view
\* The request is lost, or its caller gave up.
Drop(w) == /\ inflight' = inflight \ {w}
           /\ UNCHANGED <<rev, stage, view>>

Next == \/ \E r \in Relays : Refresh(r) \/ Plan(r)
        \/ \E w \in inflight : Land(w) \/ Drop(w)
Spec == Init /\ [][Next]_vars

TypeOK == /\ rev \in 0..MaxRevision
          /\ stage \in 0..(MaxRevision + 1)
          /\ view \in [Relays -> [rev : 0..MaxRevision, stage : 0..(MaxRevision + 1)]]
\* Every accepted write built on the one before it: nothing was planned from older state.
NoLostUpdate == stage = rev
\* Directory progress is never undone by a write that arrives late.
NoRegression == [][stage' >= stage]_vars
\* A view never claims more than was committed.
ViewsLag == \A r \in Relays : view[r].rev <= rev
=============================================================================
