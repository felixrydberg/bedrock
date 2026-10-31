------------------------------ MODULE relays ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* The single shared Relay.Session transport implements these transitions.
\* Removing the superseded B-10 wire service does not add a second delivery path.
CONSTANTS MaxRevision, QueueLimit
VARIABLES head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected
vars == <<head, applied, queued, connected, cached, boundary, pinned, staging, retained, projected>>
\* Alternating revisions stand for payloads matching/not matching a fixed selector.
Selected(r) == r % 2 = 0
Init == /\ head = 0 /\ applied = 0 /\ queued = <<>> /\ connected = TRUE
        /\ cached = {} /\ retained = {} /\ projected = {}
        /\ boundary = 0 /\ pinned = FALSE /\ staging = FALSE
Commit == /\ head < MaxRevision /\ head' = head + 1
          /\ cached' = cached \cup {head + 1}
          /\ retained' = retained \cup {head + 1}
          /\ UNCHANGED <<applied, queued, connected, boundary, pinned, staging, projected>>
Send == /\ connected /\ ~staging /\ applied + Len(queued) < head
        /\ Len(queued) < QueueLimit
        /\ applied + Len(queued) + 1 \in cached \cup retained
        /\ queued' = Append(queued, applied + Len(queued) + 1)
        /\ UNCHANGED <<head, applied, connected, cached, boundary, pinned, staging, retained, projected>>
ApplyCoverage == /\ Len(queued) > 0 /\ Head(queued) = applied + 1
                 /\ applied' = Head(queued) /\ queued' = Tail(queued)
                 /\ projected' = IF Selected(Head(queued)) THEN projected \cup {Head(queued)} ELSE projected
                 /\ UNCHANGED <<head, connected, cached, boundary, pinned, staging, retained>>
Disconnect == /\ connected /\ connected' = FALSE /\ queued' = <<>>
              /\ pinned' = FALSE /\ staging' = FALSE
              /\ UNCHANGED <<head, applied, cached, boundary, retained, projected>>
Reconnect == /\ ~connected /\ connected' = TRUE
             /\ UNCHANGED <<head, applied, queued, cached, boundary, pinned, staging, retained, projected>>
Evict == /\ \E r \in cached: cached' = cached \ {r}
         /\ UNCHANGED <<head, applied, queued, connected, boundary, pinned, staging, retained, projected>>
GC == /\ \E r \in retained: /\ (~pinned \/ r <= boundary)
                              /\ retained' = retained \ {r}
      /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, pinned, staging, projected>>
BeginBootstrap == /\ connected /\ ~staging /\ boundary' = head
                  /\ staging' = TRUE /\ pinned' = TRUE /\ queued' = <<>>
                  /\ UNCHANGED <<head, applied, cached, retained, connected, projected>>
\* Abstracts validated installation plus the source's recovered confirmation;
\* a concrete receiver may persist staging first, but cannot become ready yet.
InstallValidated == /\ staging /\ pinned /\ boundary >= applied
                    /\ applied' = boundary /\ staging' = FALSE
                    /\ projected' = {r \in 1..boundary : Selected(r)}
                    /\ UNCHANGED <<head, queued, connected, cached, boundary, pinned, retained>>
FinishRecovery == /\ pinned /\ ~staging /\ applied = head /\ pinned' = FALSE
                  /\ UNCHANGED <<head, applied, queued, connected, cached, boundary, staging, retained, projected>>
Next == Commit \/ Send \/ ApplyCoverage \/ Disconnect \/ Reconnect \/ Evict
        \/ BeginBootstrap \/ InstallValidated \/ FinishRecovery \/ GC
Spec == Init /\ [][Next]_vars
TypeOK == /\ applied \in 0..head /\ head \in 0..MaxRevision
          /\ cached \subseteq 1..head /\ Len(queued) <= QueueLimit
OrderedCoverage == \A i \in 1..Len(queued): queued[i] = applied + i
ProtectedBootstrap == staging => pinned
ProtectedTail == pinned => (boundary+1..head) \subseteq retained
FilteredCoverage == projected = {r \in 1..applied : Selected(r)}
=============================================================================
