------------------------------ MODULE volume ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
\* S-02 (#110), S-02b (#131): one actor's quorum log on bedrock-volume (statefull/v2.md §5).
\* Holders (Compute hosts) reserve an epoch at a quorum, claim with it and append; slots promise,
\* fsync and learn the commit point; a maintainer flushes to S3. a, b and c are the actor's three
\* slots and d is the node a change brings in. Change picks the one change that moves votes in a
\* run (one config each, or "none"):
\*   "handoff"  : the range at slot c moves to d in c's cell (c seals, d copies from c, then votes);
\*   "wipe"     : c loses its data, copies from both peers, then stays repaired (no vote) until a
\*                claim above the highest reservation it copied has committed;
\*   "topology" : the slot set {a, b, c} becomes {a, b, d} through a joint version.
\* Messages are abstract. A grant the holder has received stays counted after the slot that sent
\* it changes; a fenced holder's appends still arrive. A commit is acknowledged in the step whose
\* fsync completes its quorum.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS Hosts, NoHost, MaxEpoch, MaxLen, Change, Bug
VARIABLES promise, log, st, sv, copied, reserved, \* slots
          phase, epoch, hv, granted, hlog, ackd, \* holders
          acked, cp, s3, maint, tv, jointDone
slots == <<promise, log, st, sv, copied, reserved>>
holders == <<phase, epoch, hv, granted, hlog, ackd>>
vars == <<slots, holders, acked, cp, s3, maint, tv, jointDone>>

Nodes == {"a", "b", "c", "d"}
\* Two slots that can both believe they are the maintainer; they track the commit point.
Maintainers == {"a", "b"}
None == [e |-> 0, h |-> NoHost]
Ballot(h) == [e |-> epoch[h], h |-> h]
\* Slot positions: in a handoff or a wipe, d (or the repaired c) takes c's place.
Pos(n) == CASE n = "a" -> 1 [] n = "b" -> 2 [] n = "c" -> 3
                [] n = "d" -> IF Change = "topology" THEN 4 ELSE 3
\* Version 1 is the old slot set, 2 the joint transition, 3 the new set.
Configs(v) == CASE v = 1 -> {{1, 2, 3}}
                [] v = 2 -> IF Bug = "new-only" THEN {{1, 2, 4}} ELSE {{1, 2, 3}, {1, 2, 4}}
                [] v = 3 -> {{1, 2, 4}}
Quorum(v, Q) == \A S \in Configs(v) : Cardinality({Pos(n) : n \in Q} \cap S) >= 2
\* A leaving owner votes until it seals; a populating or repaired slot never votes.
Voting(n) == \/ st[n] \in {"vote", "leave"} \/ (Bug = "populating" /\ st[n] = "pop")
             \/ (Bug = "early" /\ st[n] = "repaired")
\* A repaired slot still takes claims and appends, without counting towards a quorum.
Takes(n) == Voting(n) \/ st[n] = "repaired"
\* Most advanced log: highest last-entry epoch, then highest sequence.
LastE(L) == IF L = <<>> THEN 0 ELSE L[Len(L)].e
Adv(L, M) == LastE(L) > LastE(M) \/ (LastE(L) = LastE(M) /\ Len(L) > Len(M))
\* The lease, under bounded clock drift: a holder self-fences before any slot sees its lease
\* expire, so a slot sees a live lease exactly while its promised holder still runs the actor.
Live(h) == h # NoHost /\ phase[h] \in {"recover", "claimed", "open"}
\* What a populating node copies from before it is ready.
Sources == IF Change = "handoff" THEN {"c"} ELSE {"a", "b"}
Min(x, y) == IF x < y THEN x ELSE y
\* The holder's commit point: its last entry of its own epoch acknowledged at a quorum. Earlier
\* entries commit only through it (Raft's Figure 8 rule).
CommitPoint(h, acks) ==
    LET ok(i) == /\ Quorum(hv[h], {n \in Nodes : acks[n] >= i})
                 /\ hlog[h][i].e = epoch[h] \/ Bug = "previous"
        C == {i \in 1..Len(hlog[h]) : ok(i)}
    IN IF C = {} THEN 0 ELSE CHOOSE i \in C : \A j \in C : j <= i
Committed(h) == {<<j, hlog[h][j]>> : j \in 1..CommitPoint(h, ackd[h])}

Init == /\ promise = [n \in Nodes |-> None] /\ log = [n \in Nodes |-> <<>>]
        /\ sv = [n \in Nodes |-> 1] /\ copied = [n \in Nodes |-> {}] /\ reserved = [n \in Nodes |-> 0]
        /\ st = [n \in Nodes |-> IF n = "d" THEN "off" ELSE "vote"]
        /\ phase = [h \in Hosts |-> "idle"] /\ epoch = [h \in Hosts |-> 0] /\ hv = [h \in Hosts |-> 1]
        /\ granted = [h \in Hosts |-> {}] /\ hlog = [h \in Hosts |-> <<>>]
        /\ ackd = [h \in Hosts |-> [n \in Nodes |-> 0]]
        /\ acked = {} /\ cp = [n \in Maintainers |-> 0] /\ s3 = <<>> /\ maint = "a"
        /\ tv = 1 /\ jointDone = FALSE

\* ---- Holder ----
\* Reserve(actor, host, epoch = highest seen + 1) at every slot, at the topology version the host
\* read; the claim at that epoch follows once a quorum has reserved it. A claimant with an older view
\* is the same reservation started earlier, its replies delayed. Bug "self-epoch" claims at once.
Start(h) == LET seen == {promise[n].e : n \in Nodes} \cup {reserved[n] : n \in Nodes} \cup {epoch[h]}
                e == 1 + CHOOSE x \in seen : \A y \in seen : y <= x IN
            /\ phase[h] \in {"idle", "fenced", "released"} /\ e <= MaxEpoch
            /\ phase' = [phase EXCEPT ![h] = IF Bug = "self-epoch" THEN "claiming" ELSE "reserving"]
            /\ epoch' = [epoch EXCEPT ![h] = e]
            /\ hv' = [hv EXCEPT ![h] = tv] /\ hlog' = [hlog EXCEPT ![h] = <<>>]
            /\ ackd' = [ackd EXCEPT ![h] = [n \in Nodes |-> 0]] /\ granted' = [granted EXCEPT ![h] = {}]
            /\ UNCHANGED <<slots, acked, cp, s3, maint, tv, jointDone>>
\* A voting slot reserves an epoch above its highest reservation and fsyncs it, so no two holders
\* reserve one epoch at a quorum. The holder collects the reservations in granted.
Reserve(n, h) == /\ phase[h] = "reserving" /\ Voting(n)
                 /\ epoch[h] > reserved[n] /\ hv[h] >= sv[n]
                 /\ reserved' = [reserved EXCEPT ![n] = epoch[h]] /\ sv' = [sv EXCEPT ![n] = hv[h]]
                 /\ granted' = [granted EXCEPT ![h] = @ \cup {n}]
                 /\ UNCHANGED <<promise, log, st, copied, phase, epoch, hv, hlog, ackd,
                                acked, cp, s3, maint, tv, jointDone>>
Reserved(h) == /\ phase[h] = "reserving" /\ Quorum(hv[h], granted[h])
               /\ phase' = [phase EXCEPT ![h] = "claiming"] /\ granted' = [granted EXCEPT ![h] = {}]
               /\ UNCHANGED <<slots, epoch, hv, hlog, ackd, acked, cp, s3, maint, tv, jointDone>>
\* A slot accepts a claim above its promise, at its version or newer, unless another holder's
\* lease is live there. While the claim is open, the holder keeps the most advanced log it was sent.
\* A late acceptance only moves the promise, so that slot can take the holder's appends.
Grant(n, h) == /\ phase[h] \notin {"idle", "reserving", "fenced"} /\ Takes(n)
               /\ epoch[h] > promise[n].e /\ hv[h] >= sv[n]
               /\ promise[n].h = h \/ ~Live(promise[n].h)
               /\ promise' = [promise EXCEPT ![n] = Ballot(h)] /\ sv' = [sv EXCEPT ![n] = hv[h]]
               /\ IF phase[h] = "claiming" /\ Voting(n)
                  THEN /\ granted' = [granted EXCEPT ![h] = @ \cup {n}]
                       /\ hlog' = [hlog EXCEPT ![h] =
                              IF Bug = "appoint" \/ Adv(log[n], hlog[h]) THEN log[n] ELSE @]
                  ELSE UNCHANGED <<granted, hlog>>
               /\ UNCHANGED <<log, st, copied, reserved, phase, epoch, hv, ackd, acked, cp, s3, maint, tv, jointDone>>
\* Acceptances at a quorum of every config of the version: the recovered log is fixed.
Recover(h) == /\ phase[h] = "claiming" /\ Quorum(hv[h], granted[h])
              /\ phase' = [phase EXCEPT ![h] = "recover"] /\ granted' = [granted EXCEPT ![h] = {}]
              /\ UNCHANGED <<slots, epoch, hv, hlog, ackd, acked, cp, s3, maint, tv, jointDone>>
\* After the tail copy (Replicate) the holder appends a no-op at its epoch; once that commits the
\* actor is open and appends commits. Release is a last entry, after which no lease is live.
Write(h) == /\ phase[h] \in {"recover", "open"} /\ Len(hlog[h]) < MaxLen
            /\ hlog' = [hlog EXCEPT ![h] = Append(@, Ballot(h))]
            /\ phase' = [phase EXCEPT ![h] = IF @ = "recover" THEN "claimed" ELSE @]
            /\ UNCHANGED <<slots, epoch, hv, granted, ackd, acked, cp, s3, maint, tv, jointDone>>
Release(h) == /\ phase[h] = "open" /\ Len(hlog[h]) < MaxLen
              /\ hlog' = [hlog EXCEPT ![h] = Append(@, Ballot(h))]
              /\ phase' = [phase EXCEPT ![h] = "released"]
              /\ UNCHANGED <<slots, epoch, hv, granted, ackd, acked, cp, s3, maint, tv, jointDone>>
\* Self-fence (fewer than two heartbeats acknowledged within 4 s) or a crash.
Fence(h) == /\ phase[h] \in {"reserving", "claiming", "recover", "claimed", "open"}
            /\ phase' = [phase EXCEPT ![h] = "fenced"]
            /\ UNCHANGED <<slots, epoch, hv, granted, hlog, ackd, acked, cp, s3, maint, tv, jointDone>>

\* ---- Slot ----
\* An append (or the recovery tail copy) at the slot's promise: the slot takes the holder's log,
\* truncating an old epoch's uncommitted tail, fsyncs and acknowledges. The holder commits what
\* that completes, and the slot learns the commit point announced with the append. A repaired
\* slot's acknowledgement counts for nothing; it votes again once it learns that a claim above the
\* highest reservation it copied has committed.
Replicate(n, h) ==
    LET fenced == phase[h] = "fenced"
        acks == IF fenced \/ ~Voting(n) THEN ackd[h] ELSE [ackd[h] EXCEPT ![n] = Len(hlog[h])]
        point == IF fenced THEN 0 ELSE CommitPoint(h, acks)
    IN /\ phase[h] \notin {"idle", "reserving", "claiming"} /\ Takes(n)
       /\ promise[n] = Ballot(h) /\ hv[h] >= sv[n]
       /\ log[n] # hlog[h] \/ acks # ackd[h] \/ (st[n] = "repaired" /\ epoch[h] > reserved[n] /\ point > 0)
       /\ log' = [log EXCEPT ![n] = hlog[h]] /\ sv' = [sv EXCEPT ![n] = hv[h]]
       /\ ackd' = [ackd EXCEPT ![h] = acks]
       /\ st' = IF st[n] = "repaired" /\ epoch[h] > reserved[n] /\ point > 0
                THEN [st EXCEPT ![n] = "vote"] ELSE st
       /\ acked' = acked \cup {<<j, hlog[h][j], Ballot(h)>> :
                                 j \in {i \in 1..point : ~\E x \in acked : x[1] = i /\ x[2] = hlog[h][i]}}
       /\ phase' = [phase EXCEPT ![h] = IF @ = "claimed" /\ point = Len(hlog[h]) THEN "open" ELSE @]
       /\ cp' = IF n \in Maintainers /\ point > cp[n] THEN [cp EXCEPT ![n] = point] ELSE cp
       /\ jointDone' = (jointDone \/ (hv[h] = 2 /\ point > 0))
       /\ UNCHANGED <<promise, copied, reserved, epoch, hv, granted, hlog, s3, maint, tv>>

\* ---- Maintainer ----
\* SlateDB writer fencing: opening the database fences the other maintainer.
OpenMaint(n) == /\ Voting(n) /\ maint # n /\ maint' = n
                /\ UNCHANGED <<slots, holders, acked, cp, s3, tv, jointDone>>
\* One atomic batch through S, at most the announced commit point.
Flush(n) == /\ maint = n /\ Voting(n)
            /\ \E k \in Len(s3) + 1..(IF Bug = "flush" THEN Len(log[n]) ELSE Min(cp[n], Len(log[n]))) :
                 s3' = s3 \o SubSeq(log[n], Len(s3) + 1, k)
            /\ UNCHANGED <<slots, holders, acked, cp, maint, tv, jointDone>>

\* ---- Changes that move votes ----
\* Handoff: c becomes the leaving owner and d the populating one.
StartHandoff == /\ Change = "handoff" /\ st["d"] = "off" /\ st["c"] = "vote"
                /\ st' = [st EXCEPT !["c"] = "leave", !["d"] = "pop"]
                /\ UNCHANGED <<promise, log, sv, copied, reserved, holders, acked, cp, s3, maint, tv, jointDone>>
\* The leaving owner seals: from now on it refuses claims and appends.
Seal == /\ st["c"] = "leave" /\ st' = [st EXCEPT !["c"] = "seal"]
        /\ UNCHANGED <<promise, log, sv, copied, reserved, holders, acked, cp, s3, maint, tv, jointDone>>
\* The slot loses its data (crash or wipe), reservations included, and comes back populating.
Wipe == /\ Change = "wipe" /\ st["c"] = "vote" /\ copied["c"] = {}
        /\ st' = [st EXCEPT !["c"] = "pop"] /\ promise' = [promise EXCEPT !["c"] = None]
        /\ log' = [log EXCEPT !["c"] = <<>>] /\ reserved' = [reserved EXCEPT !["c"] = 0]
        /\ UNCHANGED <<sv, copied, holders, acked, cp, s3, maint, tv, jointDone>>
\* A populating node copies the claim and log of one source: the sealed leaving owner, or a
\* voting peer. It keeps the higher promise and reservation, and the more advanced log.
Copy(n, m) == /\ st[n] = "pop" /\ m \in Sources \ copied[n]
              /\ IF Change = "handoff" THEN st[m] = "seal" \/ (Bug = "unsealed" /\ st[m] = "leave")
                                       ELSE Voting(m)
              /\ promise' = [promise EXCEPT ![n] = IF promise[m].e > @.e THEN promise[m] ELSE @]
              /\ log' = [log EXCEPT ![n] = IF Adv(log[m], @) THEN log[m] ELSE @]
              /\ reserved' = [reserved EXCEPT ![n] = IF reserved[m] > @ THEN reserved[m] ELSE @]
              /\ copied' = [copied EXCEPT ![n] = @ \cup {m}]
              /\ UNCHANGED <<st, sv, holders, acked, cp, s3, maint, tv, jointDone>>
\* The node reports ready after copying from every source, and its relay checks the report
\* (the B-21 change for volume ranges). In a handoff the leaving owner then drops the range and the
\* new owner votes: the sealed source gave no promise or reservation after the copy. A repaired
\* slot waits (see Replicate).
Ready(n) == /\ st[n] = "pop" /\ copied[n] = Sources
            /\ st' = IF Change = "handoff" THEN [st EXCEPT ![n] = "vote", !["c"] = "off"]
                     ELSE [st EXCEPT ![n] = "repaired"]
            /\ UNCHANGED <<promise, log, sv, copied, reserved, holders, acked, cp, s3, maint, tv, jointDone>>
\* Topology: the version moves to the joint sets, then to the new set once an entry has committed
\* under joint majorities. A slot learns a newer version from a claim or append and then refuses
\* older ones. Reading it from the topology record sooner only adds refusals, which a lost message
\* already covers.
Begin == /\ Change = "topology" /\ tv = 1 /\ tv' = 2 /\ st' = [st EXCEPT !["d"] = "vote"]
         /\ UNCHANGED <<promise, log, sv, copied, reserved, holders, acked, cp, s3, maint, jointDone>>
End == /\ tv = 2 /\ jointDone /\ tv' = 3
       /\ UNCHANGED <<slots, holders, acked, cp, s3, maint, jointDone>>

Next == \/ \E h \in Hosts : \/ Start(h) \/ Reserved(h)
                            \/ Recover(h) \/ Write(h) \/ Release(h) \/ Fence(h)
        \/ \E n \in Nodes, h \in Hosts : Reserve(n, h) \/ Grant(n, h) \/ Replicate(n, h)
        \/ \E n \in Maintainers : OpenMaint(n) \/ Flush(n)
        \/ \E n \in Nodes : Ready(n) \/ \E m \in Nodes : Copy(n, m)
        \/ StartHandoff \/ Seal \/ Wipe \/ Begin \/ End
Spec == Init /\ [][Next]_vars
HostPerms == Permutations(Hosts)

Entries == [e : 0..MaxEpoch, h : Hosts \cup {NoHost}]
TypeOK == /\ promise \in [Nodes -> Entries] /\ reserved \in [Nodes -> 0..MaxEpoch]
          /\ \A n \in Nodes : Len(log[n]) <= MaxLen /\ \A i \in DOMAIN log[n] : log[n][i] \in Entries
          /\ st \in [Nodes -> {"vote", "leave", "seal", "pop", "repaired", "off"}] /\ sv \in [Nodes -> 1..3]
          /\ phase \in [Hosts -> {"idle", "reserving", "claiming", "recover", "claimed", "open", "released", "fenced"}]
          /\ \A h \in Hosts : Len(hlog[h]) <= MaxLen
\* acked holds <<sequence, entry, first ballot that committed it>>. No two entries commit at one
\* sequence, and every acknowledged entry is in the recovered log of every holder that won a later
\* claim, and of any other holder that won the same epoch (which must not happen).
Later(h, b) == epoch[h] > b.e \/ (epoch[h] = b.e /\ h # b.h)
NoLostAck == /\ \A x, y \in acked : x[1] = y[1] => x[2] = y[2]
             /\ \A x \in acked, h \in Hosts :
                 (phase[h] \in {"recover", "claimed", "open", "released"} /\ Later(h, x[3]))
                     => Len(hlog[h]) >= x[1] /\ hlog[h][x[1]] = x[2]
\* At most one holder serves the actor (epochs fence writes; the lease fences in-memory answers).
OneWriter == Cardinality({h \in Hosts : phase[h] = "open"}) <= 1
\* Once a newer holder serves, an older epoch commits nothing that holder lacks. A late fsync can
\* still complete the quorum of an entry the newer holder recovered.
NoStaleAppend == [][\A x \in acked' \ acked, h \in Hosts :
                       (phase[h] = "open" /\ epoch[h] > x[3].e)
                           => Len(hlog[h]) >= x[1] /\ hlog[h][x[1]] = x[2]]_vars
\* S3 holds only committed entries, in order.
S3Committed == \A i \in 1..Len(s3) : \E x \in acked : x[1] = i /\ x[2] = s3[i]
=============================================================================
