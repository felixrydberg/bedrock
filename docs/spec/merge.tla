------------------------------ MODULE merge ------------------------------
EXTENDS Naturals
\* #66 / B-61: a member joins the consumers of two adjacent ranges, one namespace each, when the directory merges the
\* ranges (membership.tla, Merge). Every range stream of a namespace carries all its revisions, payload selected or not
\* (relays.tla), so the two consumers follow one revision sequence, each at its own position. Each half holds one key here,
\* written by alternate revisions, so data a position has not reached, or one it has moved past, shows. The merged
\* consumer resumes from one position for both halves, so the member merges only once they are equal.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS MaxRevision, Bug
VARIABLES head, at, merged, value
vars == <<head, at, merged, value>>
Halves == {"a", "b"}
\* The half whose key revision r writes.
Writes(r) == IF r % 2 = 1 THEN "a" ELSE "b"
\* The value a half's key has once every revision through n is applied: the last revision that wrote it.
Expected(h, n) == IF \E r \in 1..n : Writes(r) = h
                  THEN CHOOSE r \in 1..n : Writes(r) = h /\ \A s \in 1..n : Writes(s) = h => s <= r
                  ELSE 0
Init == /\ head = 0 /\ at = [h \in Halves |-> 0] /\ merged = FALSE /\ value = [h \in Halves |-> 0]
Commit == /\ head < MaxRevision /\ head' = head + 1
          /\ UNCHANGED <<at, merged, value>>
\* One consumer applies its next revision: before the merge each half's own, after it the merged one, for both halves.
Apply(h) == LET r == at[h] + 1
                Applied == IF merged /\ Bug # "drift" THEN Halves ELSE {h} IN
            /\ at[h] < head
            /\ at' = [g \in Halves |-> IF g \in Applied THEN r ELSE at[g]]
            /\ value' = [value EXCEPT ![Writes(r)] = IF Writes(r) \in Applied THEN r ELSE @]
            /\ UNCHANGED <<head, merged>>
\* One atomic batch replaces both consumers with the merged one, resuming from a single position. The data stays.
Merge == LET from == CASE Bug = "lower" -> IF at["a"] < at["b"] THEN at["a"] ELSE at["b"]
                       [] Bug = "higher" -> IF at["a"] > at["b"] THEN at["a"] ELSE at["b"]
                       [] OTHER -> at["a"] IN
         /\ ~merged /\ (at["a"] = at["b"] \/ Bug \in {"lower", "higher"})
         /\ merged' = TRUE /\ at' = [h \in Halves |-> from]
         /\ UNCHANGED <<head, value>>
Next == Commit \/ Merge \/ \E h \in Halves : Apply(h)
Spec == Init /\ [][Next]_vars
TypeOK == /\ head \in 0..MaxRevision /\ merged \in BOOLEAN
          /\ at \in [Halves -> 0..MaxRevision] /\ value \in [Halves -> 0..MaxRevision]
\* Each half holds exactly the state its position says: nothing missing, stale or ahead (the shared GapFreeApply).
GapFreeApply == \A h \in Halves : value[h] = Expected(h, at[h])
\* After a merge the halves move together, so one cursor describes both.
OneCursor == merged => at["a"] = at["b"]
=============================================================================
