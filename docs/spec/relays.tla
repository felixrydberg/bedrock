------------------------------ MODULE relays ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* The single shared Relay.Session transport implements these transitions.
\* Removing the superseded B-10 wire service does not add a second delivery path.
\* #22 / B-21: one stream per dataset and range carries every namespace of the dataset, each
\* with its own revisions. The relay pushes whether the subscriber owns the range; dropping it
\* is the revoke, and the subscriber deletes the range.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS MaxRevision, QueueLimit, Namespaces, Bug
VARIABLES head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected,
          pending, ready, assigned, frames, following, reported, lost
vars == <<head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected,
          pending, ready, assigned, frames, following, reported, lost>>
\* Alternating revisions stand for payloads inside/outside the range.
Selected(r) == r % 2 = 0
\* pinned: the source holds valid recovery leases. pending: an installed baseline awaits the
\* source's recovered confirmation; it is persisted with the installed state.
\* assigned: what the relay last pushed. frames: pushes the subscriber has not processed.
\* following: the subscriber follows the range. reported: the relay told the publisher the
\* subscriber is ready, which the directory keeps until the range is revoked. lost: local storage
\* was recreated empty (#23 / B-22 rebuild) and no validated baseline has been installed since.
Caught == \A n \in Namespaces : applied[n] = head[n]
Sent(n) == applied[n] + Cardinality({i \in 1..Len(queued) : queued[i].ns = n})
Init == /\ head = [n \in Namespaces |-> 0] /\ applied = [n \in Namespaces |-> 0]
        /\ queued = <<>> /\ connected = TRUE
        /\ cached = [n \in Namespaces |-> {}] /\ retained = [n \in Namespaces |-> {}]
        /\ projected = [n \in Namespaces |-> {}]
        /\ boundary = [n \in Namespaces |-> 0] /\ pinned = FALSE /\ staging = FALSE
        /\ pending = FALSE /\ ready = FALSE
        /\ assigned = FALSE /\ frames = <<>> /\ following = FALSE /\ reported = FALSE
        /\ lost = FALSE
Commit(n) == /\ head[n] < MaxRevision /\ head' = [head EXCEPT ![n] = @ + 1]
             /\ cached' = [cached EXCEPT ![n] = @ \cup {head[n] + 1}]
             /\ retained' = [retained EXCEPT ![n] = @ \cup {head[n] + 1}]
             /\ ready' = FALSE
             /\ UNCHANGED <<applied, queued, connected, boundary, pinned, staging, projected, pending,
                            assigned, frames, following, reported, lost>>
\* Coverage of each namespace travels in order; namespaces interleave.
Send(n) == /\ connected /\ following /\ ~staging /\ Sent(n) < head[n]
           /\ Len(queued) < QueueLimit
           /\ Sent(n) + 1 \in cached[n] \cup retained[n]
           /\ queued' = Append(queued, [ns |-> n, r |-> Sent(n) + IF Bug = "skip" THEN 2 ELSE 1])
           /\ UNCHANGED <<head, applied, connected, cached, boundary, pinned, staging, retained, projected,
                          pending, ready, assigned, frames, following, reported, lost>>
\* A receiver whose storage was lost drains coverage until a fresh baseline installs.
ApplyCoverage == LET q == Head(queued) IN
                 /\ ~lost /\ Len(queued) > 0 /\ q.r = applied[q.ns] + 1
                 /\ applied' = [applied EXCEPT ![q.ns] = q.r] /\ queued' = Tail(queued)
                 /\ projected' = IF Selected(q.r) /\ Bug # "payload" THEN [projected EXCEPT ![q.ns] = @ \cup {q.r}]
                                 ELSE projected
                 /\ UNCHANGED <<head, connected, cached, boundary, pinned, staging, retained, pending, ready,
                                assigned, frames, following, reported, lost>>
\* Disconnect aborts staging and releases the leases; an unconfirmed install stays pending.
\* Unprocessed pushes are lost; the next session starts with the full pushed set.
Disconnect == /\ connected /\ connected' = FALSE /\ queued' = <<>>
              /\ pinned' = FALSE /\ staging' = FALSE /\ ready' = FALSE /\ frames' = <<>>
              /\ UNCHANGED <<head, applied, cached, boundary, retained, projected, pending,
                             assigned, following, reported, lost>>
Reconnect == /\ ~connected /\ connected' = TRUE /\ frames' = <<assigned>>
             /\ UNCHANGED <<head, applied, queued, cached, boundary, pinned, staging, retained, projected,
                            pending, ready, assigned, following, reported, lost>>
\* The leases expire without a disconnect; an unfinished transfer aborts.
ExpireLease == /\ pinned /\ pinned' = FALSE /\ staging' = (Bug = "lease" /\ staging)
               /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, retained, projected, pending,
                              ready, assigned, frames, following, reported, lost>>
Evict(n) == /\ \E r \in cached[n] : cached' = [cached EXCEPT ![n] = @ \ {r}]
            /\ UNCHANGED <<head, applied, queued, connected, boundary, pinned, staging, retained, projected,
                           pending, ready, assigned, frames, following, reported, lost>>
GC(n) == /\ \E r \in retained[n] : /\ (~pinned \/ r <= boundary[n] \/ Bug = "gc")
                                    /\ retained' = [retained EXCEPT ![n] = @ \ {r}]
         /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, projected,
                        pending, ready, assigned, frames, following, reported, lost>>
\* One transfer covers the range in every namespace, each at its own boundary.
BeginBootstrap == /\ connected /\ following /\ ~staging /\ boundary' = head
                  /\ staging' = TRUE /\ pinned' = TRUE /\ queued' = <<>> /\ ready' = FALSE
                  /\ UNCHANGED <<head, applied, cached, retained, connected, projected, pending,
                                 assigned, frames, following, reported, lost>>
\* Validated atomic installation of every namespace. It does not confirm recovery.
InstallValidated == /\ staging /\ pinned /\ \A n \in Namespaces : boundary[n] >= applied[n]
                    /\ applied' = boundary /\ staging' = FALSE /\ pending' = TRUE
                    /\ projected' = [n \in Namespaces |-> {r \in 1..boundary[n] : Selected(r)}] /\ lost' = FALSE
                    /\ UNCHANGED <<head, queued, connected, cached, boundary, pinned, retained, ready,
                                   assigned, frames, following, reported>>
\* The source's recovered Range: catch-up through head was acknowledged under valid leases.
Confirm == /\ connected /\ pinned /\ pending /\ ~staging /\ ~lost /\ Caught
           /\ pending' = FALSE /\ pinned' = FALSE
           /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, staging, retained, projected, ready,
                          assigned, frames, following, reported, lost>>
\* Local storage is recreated empty for a rebuild. The receiver keeps its cursors but abandons any transfer
\* and must install and confirm a fresh validated baseline before it is ready again.
LoseLocal == /\ lost' = TRUE /\ projected' = [n \in Namespaces |-> {}] /\ queued' = <<>>
             /\ staging' = FALSE /\ pending' = (Bug # "lost" \/ pending) /\ ready' = FALSE
             /\ UNCHANGED <<head, applied, connected, cached, boundary, pinned, retained,
                            assigned, frames, following, reported>>
\* The receiver's local readiness check.
BecomeReady == /\ connected /\ following /\ ~staging /\ (~pending \/ Bug = "unconfirmed") /\ Caught /\ ~ready /\ ready' = TRUE
               /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, retained,
                              projected, pending, assigned, frames, following, reported, lost>>
\* The relay pushes the range (the directory made the subscriber an owner) or drops it (the
\* directory removed it). Either clears the reported readiness of the earlier assignment.
Assign == /\ connected /\ ~assigned /\ Len(frames) < QueueLimit
          /\ assigned' = TRUE /\ frames' = Append(frames, TRUE) /\ reported' = FALSE
          /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, retained,
                         projected, pending, ready, following, lost>>
Revoke == /\ connected /\ assigned /\ Len(frames) < QueueLimit
          /\ assigned' = FALSE /\ frames' = Append(frames, FALSE) /\ reported' = (Bug = "report" /\ reported)
          /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, retained,
                         projected, pending, ready, following, lost>>
\* The subscriber processes pushes in order. A revoke (confirmed against its own directory
\* view, membership.tla) deletes the range: its coverage restarts from nothing.
Receive == /\ frames # <<>> /\ frames' = Tail(frames)
           /\ IF Head(frames)
              THEN /\ following' = TRUE
                   /\ UNCHANGED <<applied, queued, pinned, staging, projected, pending, ready, lost>>
              ELSE /\ following' = FALSE /\ queued' = <<>>
                   /\ applied' = [n \in Namespaces |-> 0] /\ projected' = [n \in Namespaces |-> {}]
                   /\ pinned' = FALSE /\ staging' = FALSE /\ pending' = FALSE /\ ready' = FALSE
                   /\ lost' = FALSE
           /\ UNCHANGED <<head, connected, cached, boundary, retained, assigned, reported>>
\* The relay's readiness, shared with the publisher: confirmed and caught up, counting only
\* acknowledgements sent after the subscriber processed every push.
Report == /\ connected /\ assigned /\ frames = <<>> /\ ~staging /\ ~pending /\ Caught /\ ~reported
          /\ reported' = TRUE
          /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, retained,
                         projected, pending, ready, assigned, frames, following, lost>>
Next == \/ \E n \in Namespaces : Commit(n) \/ Send(n) \/ Evict(n) \/ GC(n)
        \/ ApplyCoverage \/ Disconnect \/ Reconnect \/ ExpireLease
        \/ BeginBootstrap \/ InstallValidated \/ Confirm \/ BecomeReady
        \/ Assign \/ Revoke \/ Receive \/ Report \/ LoseLocal
Spec == Init /\ [][Next]_vars
TypeOK == /\ \A n \in Namespaces : applied[n] \in 0..head[n] /\ head[n] \in 0..MaxRevision
                                   /\ cached[n] \subseteq 1..head[n]
          /\ Len(queued) <= QueueLimit /\ Len(frames) <= QueueLimit
          /\ pending \in BOOLEAN /\ ready \in BOOLEAN /\ reported \in BOOLEAN /\ lost \in BOOLEAN
OrderedCoverage == \A i \in 1..Len(queued) :
                    queued[i].r = applied[queued[i].ns]
                                  + Cardinality({j \in 1..i : queued[j].ns = queued[i].ns})
ProtectedBootstrap == staging => pinned
ProtectedTail == pinned => \A n \in Namespaces : (boundary[n]+1..head[n]) \subseteq retained[n]
\* Applied payload matches covered progress exactly: nothing missing, stale or resurrected.
GapFreeApply == ~lost => \A n \in Namespaces : projected[n] = {r \in 1..applied[n] : Selected(r)}
ReadyAfterConfirmation == ready => (connected /\ following /\ ~staging /\ ~pending /\ Caught)
\* A reported subscriber still follows the range it was assigned: it has not deleted it.
ReportedHolds == reported => (assigned /\ following)
\* Nothing is served from lost storage before its fresh baseline is installed and confirmed.
ReadyNotLost == ready => ~lost
=============================================================================
