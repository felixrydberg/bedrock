---------------------------- MODULE kv_admission ----------------------------
EXTENDS Naturals, TLC
\* #126 / S-18: admission above existing authorized directory CAS and Actors routing.
\* Only management learns the secret grant; observing its published hash does not reveal it.
CONSTANTS Clients, MaxRevision, Bug
VARIABLES head, grantIssued, gracePassed, knownFloor, purgeOK, fencesOK
vars == <<head, grantIssued, gracePassed, knownFloor, purgeOK, fencesOK>>

Max(a, b) == IF a > b THEN a ELSE b
StateAt(revision) == IF revision = 0 THEN "Active" ELSE "Deleting"
GrantAt(revision) == revision = 2

Init == /\ head = 0 /\ grantIssued = FALSE /\ gracePassed = FALSE
        /\ knownFloor = 0 /\ purgeOK = TRUE /\ fencesOK = TRUE

\* Revisions 1 and 2 respectively publish Deleting and the management-issued hash.
MarkDeleting == /\ head = 0 /\ head' = 1
                /\ UNCHANGED <<grantIssued, gracePassed, knownFloor, purgeOK, fencesOK>>
PassGrace == /\ head > 0 /\ ~gracePassed /\ gracePassed' = TRUE
             /\ UNCHANGED <<head, grantIssued, knownFloor, purgeOK, fencesOK>>
IssueGrant(client) == /\ client = "management" /\ head = 1 /\ gracePassed
                      /\ head' = 2 /\ grantIssued' = TRUE
                      /\ UNCHANGED <<gracePassed, knownFloor, purgeOK, fencesOK>>

\* Each attempt includes its successful directory read and resulting admission.
\* A failed read has no observed progress and changes none of these variables.
Attempt(client, mode, revision, minimum, tenantOK, canonical) ==
    LET required == Max(knownFloor, minimum)
        possessesGrant == grantIssued /\ client = "management"
        matchesGrant == possessesGrant /\ GrantAt(revision)
        purgeAccepted == mode = "purge" /\ tenantOK /\ canonical
                         /\ StateAt(revision) = "Deleting" /\ gracePassed
                         /\ (matchesGrant \/ Bug = "admin-bypass")
    IN /\ revision \in 0..head
       \* Management forwards the grant's publication revision through Actors.
       /\ (mode = "purge" /\ possessesGrant) => minimum >= 2
       /\ (revision >= required \/ Bug = "stale-floor")
       /\ knownFloor' = Max(knownFloor, revision)
       /\ fencesOK' = (fencesOK /\ revision >= required)
       /\ purgeOK' = (purgeOK /\ (purgeAccepted =>
             possessesGrant /\ GrantAt(revision) /\ minimum >= 2
             /\ tenantOK /\ canonical /\ StateAt(revision) = "Deleting" /\ gracePassed))
       /\ UNCHANGED <<head, grantIssued, gracePassed>>

Next == \/ MarkDeleting \/ PassGrace
        \/ \E client \in Clients : IssueGrant(client)
        \/ \E client \in Clients, mode \in {"open", "purge"},
              revision \in 0..MaxRevision, minimum \in 0..MaxRevision,
              tenantOK \in BOOLEAN, canonical \in BOOLEAN :
               Attempt(client, mode, revision, minimum, tenantOK, canonical)
Spec == Init /\ [][Next]_vars

TypeOK == /\ head \in 0..MaxRevision /\ knownFloor \in 0..head
          /\ grantIssued \in BOOLEAN /\ gracePassed \in BOOLEAN
          /\ purgeOK \in BOOLEAN /\ fencesOK \in BOOLEAN
PurgeAuthorized == purgeOK
KnownDeletionFences == fencesOK
=============================================================================
