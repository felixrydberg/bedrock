------------------------------ MODULE volume_dc ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* S-17 (#125): one actor moves from its old DC to a new one (statefull/v2.md §5 Single writer).
\* It has three slots in the old DC (Old) and three in the new one (New). The two slot sets share no
\* slot, so epochs and quorums alone cannot fence the move:
\*   1. A holder in the old DC (the mover) commits a MOVE entry. A later claimant there that
\*      recovers a MOVE never serves; it may only commit MOVE again, at its own epoch.
\*   2. The maintainer flushes the old DC's log to S3 through the MOVE entry.
\*   3. A new-DC slot is imported from S3 once S3 holds a MOVE: it takes S3's log and the MOVE's
\*      epoch as its promise. Only imported slots take claims and appends in the new DC.
\* Any host may claim in its DC at any time: routing and topology are only hints. Reservations,
\* repair, handoff and truncation are volume.tla's; here a claim promises directly, which is the
\* same protocol without lost disks.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS OldHosts, NewHosts, NoHost, Maint, OldOthers, New, MaxEpoch, MaxLen, Bug
VARIABLES promise, log, imported, cp, phase, epoch, granted, hlog, ackd, acked, s3
slots == <<promise, log, imported, cp>>
holders == <<phase, epoch, granted, hlog, ackd>>
vars == <<slots, holders, acked, s3>>

Hosts == OldHosts \cup NewHosts
\* Slots are model values: Maint is the old DC's maintainer slot, and the other old slots and the new
\* slots are each interchangeable, which the symmetry uses.
Old == {Maint} \cup OldOthers
Nodes == Old \cup New
SlotsOf(h) == IF h \in OldHosts THEN Old ELSE New
SameDC(h, g) == (h \in OldHosts) = (g \in OldHosts)
None == [e |-> 0, h |-> NoHost]
Ballot(h) == [e |-> epoch[h], h |-> h]
Entry(h, k) == [e |-> epoch[h], h |-> h, k |-> k]
Quorum(h, Q) == Cardinality(Q \cap SlotsOf(h)) >= 2
LastE(L) == IF L = <<>> THEN 0 ELSE L[Len(L)].e
Adv(L, M) == LastE(L) > LastE(M) \/ (LastE(L) = LastE(M) /\ Len(L) > Len(M))
HasMove(L) == \E i \in DOMAIN L : L[i].k = "move"
Min(x, y) == IF x < y THEN x ELSE y
\* The lease, under bounded clock drift, as in volume.tla.
Live(h) == h # NoHost /\ phase[h] \in {"recover", "claimed", "open"}
\* Old-DC slots always take claims; a new-DC slot only once imported. Bug "no-import" lets an
\* empty new-DC slot take a claim.
Takes(n) == n \in Old \/ imported[n] \/ Bug = "no-import"
\* The holder's commit point: its last entry of its own epoch acknowledged at a quorum.
CommitPoint(h, acks) ==
    LET ok(i) == Quorum(h, {n \in Nodes : acks[n] >= i}) /\ hlog[h][i].e = epoch[h]
        C == {i \in 1..Len(hlog[h]) : ok(i)}
    IN IF C = {} THEN 0 ELSE CHOOSE i \in C : \A j \in C : j <= i

Init == /\ promise = [n \in Nodes |-> None] /\ log = [n \in Nodes |-> <<>>]
        /\ imported = [n \in Nodes |-> FALSE] /\ cp = 0
        /\ phase = [h \in Hosts |-> "idle"] /\ epoch = [h \in Hosts |-> 0]
        /\ granted = [h \in Hosts |-> {}] /\ hlog = [h \in Hosts |-> <<>>]
        /\ ackd = [h \in Hosts |-> [n \in Nodes |-> 0]]
        /\ acked = {} /\ s3 = <<>>

\* ---- Holder ----
Start(h) == LET seen == {promise[n].e : n \in SlotsOf(h)} \cup {epoch[h]}
                e == 1 + CHOOSE x \in seen : \A y \in seen : y <= x IN
            /\ phase[h] \in {"idle", "fenced", "moved"} /\ e <= MaxEpoch
            /\ phase' = [phase EXCEPT ![h] = "claiming"] /\ epoch' = [epoch EXCEPT ![h] = e]
            /\ hlog' = [hlog EXCEPT ![h] = <<>>] /\ granted' = [granted EXCEPT ![h] = {}]
            /\ ackd' = [ackd EXCEPT ![h] = [n \in Nodes |-> 0]]
            /\ UNCHANGED <<slots, acked, s3>>
Grant(n, h) == /\ n \in SlotsOf(h) /\ Takes(n)
               /\ phase[h] \in {"claiming", "recover", "claimed", "open", "moving"}
               /\ epoch[h] > promise[n].e
               /\ promise[n].h = h \/ ~Live(promise[n].h)
               /\ promise' = [promise EXCEPT ![n] = Ballot(h)]
               /\ IF phase[h] = "claiming"
                  THEN /\ granted' = [granted EXCEPT ![h] = @ \cup {n}]
                       /\ hlog' = [hlog EXCEPT ![h] = IF Adv(log[n], @) THEN log[n] ELSE @]
                  ELSE UNCHANGED <<granted, hlog>>
               /\ UNCHANGED <<log, imported, cp, phase, epoch, ackd, acked, s3>>
Recover(h) == /\ phase[h] = "claiming" /\ Quorum(h, granted[h])
              /\ phase' = [phase EXCEPT ![h] = "recover"]
              /\ UNCHANGED <<slots, epoch, granted, hlog, ackd, acked, s3>>
\* The no-op after recovery, then data. A claimant that recovered a MOVE never serves (bug
\* "serve-moved" lets it).
Write(h) == /\ phase[h] \in {"recover", "open"} /\ Len(hlog[h]) < MaxLen
            /\ phase[h] = "open" \/ ~HasMove(hlog[h]) \/ Bug = "serve-moved"
            /\ hlog' = [hlog EXCEPT ![h] = Append(@, Entry(h, "data"))]
            /\ phase' = [phase EXCEPT ![h] = IF @ = "recover" THEN "claimed" ELSE @]
            /\ UNCHANGED <<slots, epoch, granted, ackd, acked, s3>>
\* The mover: an old-DC holder appends MOVE and stops serving. A claimant that recovered a MOVE
\* commits it again at its own epoch, the only way an earlier epoch's entry commits.
Move(h) == /\ h \in OldHosts /\ phase[h] \in {"recover", "open"} /\ Len(hlog[h]) < MaxLen
           /\ hlog' = [hlog EXCEPT ![h] = Append(@, Entry(h, "move"))]
           /\ phase' = [phase EXCEPT ![h] = "moving"]
           /\ UNCHANGED <<slots, epoch, granted, ackd, acked, s3>>
\* Self-fence or a crash.
Fence(h) == /\ phase[h] \in {"claiming", "recover", "claimed", "open", "moving"}
            /\ phase' = [phase EXCEPT ![h] = "fenced"]
            /\ UNCHANGED <<slots, epoch, granted, hlog, ackd, acked, s3>>

\* ---- Slot ----
\* An append at the slot's promise: the slot takes the holder's log, fsyncs and acknowledges. A
\* fenced holder's late appends still arrive but commit nothing new.
Replicate(n, h) ==
    LET fenced == phase[h] = "fenced"
        acks == IF fenced THEN ackd[h] ELSE [ackd[h] EXCEPT ![n] = Len(hlog[h])]
        point == IF fenced THEN 0 ELSE CommitPoint(h, acks)
    IN /\ n \in SlotsOf(h) /\ Takes(n)
       /\ phase[h] \in {"recover", "claimed", "open", "moving", "moved", "fenced"}
       /\ promise[n] = Ballot(h)
       /\ log[n] # hlog[h] \/ acks # ackd[h]
       /\ log' = [log EXCEPT ![n] = hlog[h]] /\ ackd' = [ackd EXCEPT ![h] = acks]
       /\ acked' = acked \cup {<<j, hlog[h][j], Ballot(h)>> :
                                 j \in {i \in 1..point : ~\E x \in acked : x[1] = i /\ x[2] = hlog[h][i]}}
       /\ phase' = [phase EXCEPT ![h] =
                        IF point = Len(hlog[h]) /\ @ = "claimed" THEN "open"
                        ELSE IF point = Len(hlog[h]) /\ @ = "moving" THEN "moved" ELSE @]
       /\ cp' = IF n = Maint /\ point > cp THEN point ELSE cp
       /\ UNCHANGED <<promise, imported, epoch, granted, hlog, s3>>

\* ---- Maintainer and import ----
\* One atomic batch through at most the announced commit point (bug "flush": through the log).
Flush == /\ \E k \in Len(s3) + 1..(IF Bug = "flush" THEN Len(log[Maint]) ELSE Min(cp, Len(log[Maint]))) :
              s3' = s3 \o SubSeq(log[Maint], Len(s3) + 1, k)
         /\ UNCHANGED <<slots, holders, acked>>
\* A new-DC slot imports S3's log once it holds a MOVE, promising the MOVE's epoch (bug
\* "early-import": whatever S3 holds).
Import(n) == /\ n \in New /\ ~imported[n]
             /\ HasMove(s3) \/ Bug = "early-import"
             /\ imported' = [imported EXCEPT ![n] = TRUE]
             /\ log' = [log EXCEPT ![n] = s3]
             /\ promise' = [promise EXCEPT ![n] = [e |-> LastE(s3), h |-> NoHost]]
             /\ UNCHANGED <<cp, holders, acked, s3>>

Next == \/ \E h \in Hosts : Start(h) \/ Recover(h) \/ Write(h) \/ Move(h) \/ Fence(h)
        \/ \E n \in Nodes, h \in Hosts : Grant(n, h) \/ Replicate(n, h)
        \/ Flush \/ \E n \in New : Import(n)
Spec == Init /\ [][Next]_vars
Perms == Permutations(OldHosts) \cup Permutations(OldOthers) \cup Permutations(New)

Entries == [e : 0..MaxEpoch, h : Hosts \cup {NoHost}, k : {"data", "move"}]
TypeOK == /\ \A n \in Nodes : promise[n].e \in 0..MaxEpoch
          /\ \A n \in Nodes : Len(log[n]) <= MaxLen /\ \A i \in DOMAIN log[n] : log[n][i] \in Entries
          /\ imported \in [Nodes -> BOOLEAN] /\ cp \in 0..MaxLen
          /\ phase \in [Hosts -> {"idle", "claiming", "recover", "claimed", "open", "moving", "moved", "fenced"}]
          /\ \A h \in Hosts : Len(hlog[h]) <= MaxLen
\* acked holds <<sequence, entry, first ballot that committed it>>. No two entries commit at one
\* sequence. Every acknowledged entry is in the recovered log of every later holder of its DC, and
\* every acknowledged data entry of the old DC is in that of every new-DC holder that recovered:
\* the move loses nothing that was acknowledged. After the first MOVE commits, the old DC commits
\* only MOVE entries again (a mover that recovered it), which the import need not carry.
Later(h, x) == IF h \in NewHosts /\ x[3].h \in OldHosts THEN x[2].k = "data"
               ELSE SameDC(h, x[3].h) /\ (epoch[h] > x[3].e \/ (epoch[h] = x[3].e /\ h # x[3].h))
NoLostAck == /\ \A x, y \in acked : x[1] = y[1] /\ SameDC(x[3].h, y[3].h) => x[2] = y[2]
             /\ \A x \in acked, h \in Hosts :
                 (phase[h] \in {"recover", "claimed", "open", "moving", "moved"} /\ Later(h, x))
                     => Len(hlog[h]) >= x[1] /\ hlog[h][x[1]] = x[2]
\* At most one holder serves the actor, across both DCs.
OneWriter == Cardinality({h \in Hosts : phase[h] = "open"}) <= 1
\* S3 holds only committed entries, in order.
S3Committed == \A i \in 1..Len(s3) : \E x \in acked : x[1] = i /\ x[2] = s3[i]
=============================================================================
