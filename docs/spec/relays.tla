------------------------------ MODULE relays ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* The single shared Relay.Session transport implements these transitions.
\* Removing the superseded B-10 wire service does not add a second delivery path.
CONSTANTS MaxRevision, QueueLimit
VARIABLES head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected,
          pending, ready, lost
vars == <<head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected,
          pending, ready, lost>>
\* Alternating revisions stand for payloads matching/not matching a fixed selector.
Selected(r) == r % 2 = 0
\* pinned: the source holds a valid recovery lease. pending: an installed baseline awaits the
\* source's recovered confirmation; it is persisted with the installed state. lost: local storage was recreated
\* empty (#23 / B-22 rebuild) and no validated baseline has been installed since.
Init == /\ head = 0 /\ applied = 0 /\ queued = <<>> /\ connected = TRUE
        /\ cached = {} /\ retained = {} /\ projected = {}
        /\ boundary = 0 /\ pinned = FALSE /\ staging = FALSE
        /\ pending = FALSE /\ ready = FALSE /\ lost = FALSE
Commit == /\ head < MaxRevision /\ head' = head + 1
          /\ cached' = cached \cup {head + 1}
          /\ retained' = retained \cup {head + 1}
          /\ ready' = FALSE
          /\ UNCHANGED <<applied, queued, connected, boundary, pinned, staging, projected, pending, lost>>
Send == /\ connected /\ ~staging /\ applied + Len(queued) < head
        /\ Len(queued) < QueueLimit
        /\ applied + Len(queued) + 1 \in cached \cup retained
        /\ queued' = Append(queued, applied + Len(queued) + 1)
        /\ UNCHANGED <<head, applied, connected, cached, boundary, pinned, staging, retained, projected, pending, ready, lost>>
\* A receiver whose storage was lost drains coverage until a fresh baseline installs.
ApplyCoverage == /\ ~lost /\ Len(queued) > 0 /\ Head(queued) = applied + 1
                 /\ applied' = Head(queued) /\ queued' = Tail(queued)
                 /\ projected' = IF Selected(Head(queued)) THEN projected \cup {Head(queued)} ELSE projected
                 /\ UNCHANGED <<head, connected, cached, boundary, pinned, staging, retained, pending, ready, lost>>
\* Disconnect aborts staging and releases the lease; an unconfirmed install stays pending.
Disconnect == /\ connected /\ connected' = FALSE /\ queued' = <<>>
              /\ pinned' = FALSE /\ staging' = FALSE /\ ready' = FALSE
              /\ UNCHANGED <<head, applied, cached, boundary, retained, projected, pending, lost>>
Reconnect == /\ ~connected /\ connected' = TRUE
             /\ UNCHANGED <<head, applied, queued, cached, boundary, pinned, staging, retained, projected, pending, ready, lost>>
\* The lease expires without a disconnect; an unfinished transfer aborts.
ExpireLease == /\ pinned /\ pinned' = FALSE /\ staging' = FALSE
               /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, retained, projected, pending, ready, lost>>
Evict == /\ \E r \in cached: cached' = cached \ {r}
         /\ UNCHANGED <<head, applied, queued, connected, boundary, pinned, staging, retained, projected, pending, ready, lost>>
GC == /\ \E r \in retained: /\ (~pinned \/ r <= boundary)
                              /\ retained' = retained \ {r}
      /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, projected, pending, ready, lost>>
BeginBootstrap == /\ connected /\ ~staging /\ boundary' = head
                  /\ staging' = TRUE /\ pinned' = TRUE /\ queued' = <<>> /\ ready' = FALSE
                  /\ UNCHANGED <<head, applied, cached, retained, connected, projected, pending, lost>>
\* Validated atomic installation. It does not confirm recovery.
InstallValidated == /\ staging /\ pinned /\ boundary >= applied
                    /\ applied' = boundary /\ staging' = FALSE /\ pending' = TRUE
                    /\ projected' = {r \in 1..boundary : Selected(r)} /\ lost' = FALSE
                    /\ UNCHANGED <<head, queued, connected, cached, boundary, pinned, retained, ready>>
\* The source's recovered Range: catch-up through head was acknowledged under a valid lease.
Confirm == /\ connected /\ pinned /\ pending /\ ~staging /\ ~lost /\ applied = head
           /\ pending' = FALSE /\ pinned' = FALSE
           /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, staging, retained, projected, ready, lost>>
\* Local storage is recreated empty for a rebuild. The receiver keeps its cursor but abandons any transfer and
\* must install and confirm a fresh validated baseline before it is ready again.
LoseLocal == /\ lost' = TRUE /\ projected' = {} /\ queued' = <<>> /\ staging' = FALSE
             /\ pending' = TRUE /\ ready' = FALSE
             /\ UNCHANGED <<head, applied, connected, cached, boundary, pinned, retained>>
\* The receiver's local readiness check.
BecomeReady == /\ connected /\ ~staging /\ ~pending /\ applied = head /\ ~ready /\ ready' = TRUE
               /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected, pending, lost>>
Next == Commit \/ Send \/ ApplyCoverage \/ Disconnect \/ Reconnect \/ ExpireLease \/ Evict
        \/ BeginBootstrap \/ InstallValidated \/ Confirm \/ BecomeReady \/ GC \/ LoseLocal
Spec == Init /\ [][Next]_vars
TypeOK == /\ applied \in 0..head /\ head \in 0..MaxRevision
          /\ cached \subseteq 1..head /\ Len(queued) <= QueueLimit
          /\ pending \in BOOLEAN /\ ready \in BOOLEAN /\ lost \in BOOLEAN
OrderedCoverage == \A i \in 1..Len(queued): queued[i] = applied + i
ProtectedBootstrap == staging => pinned
ProtectedTail == pinned => (boundary+1..head) \subseteq retained
FilteredCoverage == ~lost => projected = {r \in 1..applied : Selected(r)}
ReadyAfterConfirmation == ready => (connected /\ ~staging /\ ~pending /\ applied = head)
\* Nothing is served from lost storage before its fresh baseline is installed and confirmed.
ReadyNotLost == ready => ~lost
=============================================================================
