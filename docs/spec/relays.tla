------------------------------ MODULE relays ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* The single shared Relay.Session transport implements these transitions.
\* Removing the superseded B-10 wire service does not add a second delivery path.
CONSTANTS MaxRevision, QueueLimit
VARIABLES head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected,
          pending, ready
vars == <<head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected,
          pending, ready>>
\* Alternating revisions stand for payloads matching/not matching a fixed selector.
Selected(r) == r % 2 = 0
\* pinned: the source holds a valid recovery lease. pending: an installed baseline awaits the
\* source's recovered confirmation; it is persisted with the installed state.
Init == /\ head = 0 /\ applied = 0 /\ queued = <<>> /\ connected = TRUE
        /\ cached = {} /\ retained = {} /\ projected = {}
        /\ boundary = 0 /\ pinned = FALSE /\ staging = FALSE
        /\ pending = FALSE /\ ready = FALSE
Commit == /\ head < MaxRevision /\ head' = head + 1
          /\ cached' = cached \cup {head + 1}
          /\ retained' = retained \cup {head + 1}
          /\ ready' = FALSE
          /\ UNCHANGED <<applied, queued, connected, boundary, pinned, staging, projected, pending>>
Send == /\ connected /\ ~staging /\ applied + Len(queued) < head
        /\ Len(queued) < QueueLimit
        /\ applied + Len(queued) + 1 \in cached \cup retained
        /\ queued' = Append(queued, applied + Len(queued) + 1)
        /\ UNCHANGED <<head, applied, connected, cached, boundary, pinned, staging, retained, projected, pending, ready>>
ApplyCoverage == /\ Len(queued) > 0 /\ Head(queued) = applied + 1
                 /\ applied' = Head(queued) /\ queued' = Tail(queued)
                 /\ projected' = IF Selected(Head(queued)) THEN projected \cup {Head(queued)} ELSE projected
                 /\ UNCHANGED <<head, connected, cached, boundary, pinned, staging, retained, pending, ready>>
\* Disconnect aborts staging and releases the lease; an unconfirmed install stays pending.
Disconnect == /\ connected /\ connected' = FALSE /\ queued' = <<>>
              /\ pinned' = FALSE /\ staging' = FALSE /\ ready' = FALSE
              /\ UNCHANGED <<head, applied, cached, boundary, retained, projected, pending>>
Reconnect == /\ ~connected /\ connected' = TRUE
             /\ UNCHANGED <<head, applied, queued, cached, boundary, pinned, staging, retained, projected, pending, ready>>
\* The lease expires without a disconnect; an unfinished transfer aborts.
ExpireLease == /\ pinned /\ pinned' = FALSE /\ staging' = FALSE
               /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, retained, projected, pending, ready>>
Evict == /\ \E r \in cached: cached' = cached \ {r}
         /\ UNCHANGED <<head, applied, queued, connected, boundary, pinned, staging, retained, projected, pending, ready>>
GC == /\ \E r \in retained: /\ (~pinned \/ r <= boundary)
                              /\ retained' = retained \ {r}
      /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, projected, pending, ready>>
BeginBootstrap == /\ connected /\ ~staging /\ boundary' = head
                  /\ staging' = TRUE /\ pinned' = TRUE /\ queued' = <<>> /\ ready' = FALSE
                  /\ UNCHANGED <<head, applied, cached, retained, connected, projected, pending>>
\* Validated atomic installation. It does not confirm recovery.
InstallValidated == /\ staging /\ pinned /\ boundary >= applied
                    /\ applied' = boundary /\ staging' = FALSE /\ pending' = TRUE
                    /\ projected' = {r \in 1..boundary : Selected(r)}
                    /\ UNCHANGED <<head, queued, connected, cached, boundary, pinned, retained, ready>>
\* The source's recovered Range: catch-up through head was acknowledged under a valid lease.
Confirm == /\ connected /\ pinned /\ pending /\ ~staging /\ applied = head
           /\ pending' = FALSE /\ pinned' = FALSE
           /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, staging, retained, projected, ready>>
\* The receiver's local readiness check.
BecomeReady == /\ connected /\ ~staging /\ ~pending /\ applied = head /\ ~ready /\ ready' = TRUE
               /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected, pending>>
Next == Commit \/ Send \/ ApplyCoverage \/ Disconnect \/ Reconnect \/ ExpireLease \/ Evict
        \/ BeginBootstrap \/ InstallValidated \/ Confirm \/ BecomeReady \/ GC
Spec == Init /\ [][Next]_vars
TypeOK == /\ applied \in 0..head /\ head \in 0..MaxRevision
          /\ cached \subseteq 1..head /\ Len(queued) <= QueueLimit
          /\ pending \in BOOLEAN /\ ready \in BOOLEAN
OrderedCoverage == \A i \in 1..Len(queued): queued[i] = applied + i
ProtectedBootstrap == staging => pinned
ProtectedTail == pinned => (boundary+1..head) \subseteq retained
FilteredCoverage == projected = {r \in 1..applied : Selected(r)}
ReadyAfterConfirmation == ready => (connected /\ ~staging /\ ~pending /\ applied = head)
=============================================================================
