------------------------------ MODULE handoff ------------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC
\* #22 / B-21 L2 range handoff (membership.md, "Range handoff") over one dataset's directory.
\* Membership and the relay mesh are abstract: membership.tla checks who is published, that
\* one majority publisher writes, and that draining members get nothing new. Here a member
\* is published while its stream is up and removed only while disconnected; `quorum` stands
\* for a relay majority. Each connected node has one holding relay; failover is a disconnect
\* and a connect. Points are possible range starts; a range ends where the next start begins.
\* With Capped, a run has at most MaxCuts disconnects and MaxFlips quorum changes, which lets
\* TLC reach deeper directory histories; uncapped configs check unlimited ones.
CONSTANTS Nodes, Points, Carriers, MaxRev, Faulty, Capped, MaxCuts, MaxFlips
VARIABLES pub, conn, quorum, starts, own, rev, told, chan, follow, copy, view, cuts, flips
\* rev: directory revision. told: what the relays last pushed a node for a point
\* ("unknown" after its session ends). chan: pushed frames the node has not processed; `ok`
\* records that the node's view has reached the revision the frame was derived from.
\* follow/copy: the node follows the point / holds its data. view: what the node's own,
\* possibly lagging, directory view says about each point: its own state (`mine`) and
\* whether another owner is ready (`other`), the only facts the node reads from it.
nvars == <<told, chan, follow, copy, view>>
vars == <<pub, conn, quorum, starts, own, rev, nvars, cuts, flips>>
Holds == {"ready", "leaving"}
\* The range covering point h: the greatest start at or below it.
Cover(h) == CHOOSE s \in starts : s <= h /\ \A t \in starts : t <= h => t <= s
Entry(h) == own[Cover(h)]
\* The points of the range starting at p when the starts are S.
Span(S, p) == {h \in Points : h >= p /\ \A s \in S : s <= h => s <= p}
\* Node n's view of the current directory.
Current(n) == [h \in Points |-> [mine |-> Entry(h)[n],
                                 other |-> \E m \in Nodes \ {n} : Entry(h)[m] = "ready"]]
Count(c) == IF Capped THEN c + 1 ELSE c

Init == /\ pub = [n \in Nodes |-> "absent"] /\ conn = [n \in Nodes |-> FALSE] /\ quorum = TRUE
        /\ starts = {0} /\ own = [p \in Points |-> [n \in Nodes |-> "none"]] /\ rev = 0
        /\ told = [n \in Nodes |-> [h \in Points |-> "unknown"]]
        /\ chan = [n \in Nodes |-> [h \in Points |-> <<>>]]
        /\ follow = [n \in Nodes |-> [h \in Points |-> FALSE]]
        /\ copy = [n \in Nodes |-> [h \in Points |-> FALSE]]
        /\ view = [n \in Nodes |-> [h \in Points |-> [mine |-> "none", other |-> FALSE]]]
        /\ cuts = 0 /\ flips = 0

Connect(n) == /\ ~conn[n] /\ conn' = [conn EXCEPT ![n] = TRUE]
              /\ UNCHANGED <<pub, quorum, starts, own, rev, nvars, cuts, flips>>
\* The node keeps its data on disk. Its unprocessed frames are lost, and its next relay
\* pushes everything again.
Disconnect(n) == /\ conn[n] /\ conn' = [conn EXCEPT ![n] = FALSE]
                 /\ chan' = [chan EXCEPT ![n] = [h \in Points |-> <<>>]]
                 /\ told' = [told EXCEPT ![n] = [h \in Points |-> "unknown"]]
                 /\ cuts' = Count(cuts)
                 /\ UNCHANGED <<pub, quorum, starts, own, rev, follow, copy, view, flips>>
Publish(n) == /\ quorum /\ conn[n] /\ pub[n] = "absent" /\ pub' = [pub EXCEPT ![n] = "ready"]
              /\ UNCHANGED <<conn, quorum, starts, own, rev, nvars, cuts, flips>>
\* After the grace period, or at once after a graceful leave.
Remove(n) == /\ quorum /\ ~conn[n] /\ pub[n] # "absent" /\ pub' = [pub EXCEPT ![n] = "absent"]
             /\ UNCHANGED <<conn, quorum, starts, own, rev, nvars, cuts, flips>>
Partition == /\ quorum' = ~quorum /\ flips' = Count(flips)
             /\ UNCHANGED <<pub, conn, starts, own, rev, nvars, cuts>>

\* The holding relay's readiness for n over points S, shared over the sibling stream: n
\* processed every pushed frame (the last one assigned S), follows and holds S.
ReadyFor(n, S) == /\ conn[n]
                  /\ \A h \in S : /\ told[n][h] = "on" /\ chan[n][h] = <<>>
                                  /\ follow[n][h] /\ copy[n][h]
\* The publisher writes one range entry `o` at start p, splitting the covering range when p
\* is new. Any plan within these rules: new owners are published ready members carrying the
\* dataset; a populating owner becomes ready only from its relay's readiness; owners leave
\* the directory only once removed from membership, or when leaving and every other owner
\* of the entry is ready.
Write(p, o) ==
    LET cur == Entry(p)
        S == Span(starts \cup {p}, p) IN
    /\ quorum /\ (p \notin starts \/ o # own[p])
    /\ \A n \in Nodes :
        /\ o[n] = "populating" => \/ cur[n] = "populating"
                                  \/ cur[n] = "none" /\ pub[n] = "ready" /\ n \in Carriers
        /\ o[n] = "ready" => \/ cur[n] = "ready"
                             \/ cur[n] = "populating" /\ ReadyFor(n, S)
        /\ o[n] = "leaving" => cur[n] \in Holds
        /\ (o[n] = "none" /\ cur[n] # "none") =>
            \/ pub[n] = "absent"
            \/ cur[n] = "leaving" /\ (\A m \in Nodes : o[m] # "populating")
                                  /\ (\E m \in Nodes : o[m] = "ready")
    /\ starts' = starts \cup {p}
    /\ own' = [own EXCEPT ![p] = o]
    /\ rev' = rev + 1
    /\ UNCHANGED <<pub, conn, quorum, nvars, cuts, flips>>

\* The node's relay pushes, from the published directory, whether it owns h in any state.
\* Dropping h from the pushed set is the revoke.
Push(n, h) == LET on == IF Entry(h)[n] # "none" THEN "on" ELSE "off" IN
              /\ conn[n] /\ told[n][h] # on
              /\ told' = [told EXCEPT ![n][h] = on]
              /\ chan' = [chan EXCEPT ![n][h] = Append(@, [ok |-> view[n] = Current(n), on |-> on = "on"])]
              /\ UNCHANGED <<pub, conn, quorum, starts, own, rev, follow, copy, view, cuts, flips>>
\* A faulty relay revokes h on its own, unpublished view.
BadRevoke(n, h) == /\ Faulty /\ conn[n]
                   /\ chan' = [chan EXCEPT ![n][h] = Append(@, [ok |-> view[n] = Current(n), on |-> FALSE])]
                   /\ UNCHANGED <<pub, conn, quorum, starts, own, rev, told, follow, copy, view, cuts, flips>>
\* The node's own directory subscription catches up, past every pushed frame's revision.
Observe(n) == /\ conn[n] /\ view[n] # Current(n)
              /\ view' = [view EXCEPT ![n] = Current(n)]
              /\ chan' = [chan EXCEPT ![n] = [h \in Points |->
                     [i \in 1..Len(chan[n][h]) |-> [chan[n][h][i] EXCEPT !.ok = TRUE]]]]
              /\ UNCHANGED <<pub, conn, quorum, starts, own, rev, told, follow, copy, cuts, flips>>
\* A revoke is confirmed only once the node's view has reached the frame's revision and
\* shows the node without h while another owner of h is ready; otherwise it is refused.
Confirms(n, h) == view[n][h].mine = "none" /\ view[n][h].other
\* The node processes its next frame for h: follow, drop (delete_range), or refuse. A refusal
\* reaches the relay in the acknowledgement, which forgets what it pushed so it pushes again.
Receive(n, h) == LET f == Head(chan[n][h]) IN
                 /\ chan[n][h] # <<>>
                 /\ f.on \/ f.ok
                 /\ chan' = [chan EXCEPT ![n][h] = Tail(@)]
                 /\ IF f.on THEN /\ follow' = [follow EXCEPT ![n][h] = TRUE]
                                 /\ UNCHANGED <<copy, told>>
                    ELSE IF Confirms(n, h)
                         THEN /\ follow' = [follow EXCEPT ![n][h] = FALSE]
                              /\ copy' = [copy EXCEPT ![n][h] = FALSE]
                              /\ UNCHANGED told
                         ELSE /\ told' = [told EXCEPT ![n][h] = "unknown"]
                              /\ UNCHANGED <<follow, copy>>
                 /\ UNCHANGED <<pub, conn, quorum, starts, own, rev, view, cuts, flips>>
\* Bootstrap and catch-up of a followed point (relays.tla checks how).
Copy(n, h) == /\ conn[n] /\ follow[n][h] /\ ~copy[n][h]
              /\ copy' = [copy EXCEPT ![n][h] = TRUE]
              /\ UNCHANGED <<pub, conn, quorum, starts, own, rev, told, chan, follow, view, cuts, flips>>

Next == \/ Partition
        \/ \E n \in Nodes : Connect(n) \/ Disconnect(n) \/ Publish(n) \/ Remove(n)
                            \/ Observe(n)
        \/ \E p \in Points, o \in [Nodes -> {"none", "populating"} \cup Holds] : Write(p, o)
        \/ \E n \in Nodes, h \in Points : Push(n, h) \/ BadRevoke(n, h) \/ Receive(n, h) \/ Copy(n, h)
Spec == Init /\ [][Next]_vars
\* Bounds the directory revisions, pushed frames, disconnects and quorum changes TLC explores.
Bound == /\ rev <= MaxRev /\ cuts <= MaxCuts /\ flips <= MaxFlips
         /\ \A n \in Nodes, h \in Points : Len(chan[n][h]) <= 2
\* Nodes are interchangeable model values when every node carries the dataset.
NodePerms == Permutations(Nodes)

TypeOK == /\ pub \in [Nodes -> {"absent", "ready"}] /\ quorum \in BOOLEAN
          /\ starts \subseteq Points
          /\ own \in [Points -> [Nodes -> {"none", "populating"} \cup Holds]]
          /\ told \in [Nodes -> [Points -> {"on", "off", "unknown"}]]
          /\ follow \in [Nodes -> [Points -> BOOLEAN]] /\ copy \in [Nodes -> [Points -> BOOLEAN]]
\* Ends are implicit, so the directory covers the hash space after every write.
Covered == 0 \in starts
\* Every ready or leaving owner holds the data: no owner is ready before its copy, and no
\* node drops data the directory still has it holding, even after a faulty revoke.
ReadyHasData == \A h \in Points, n \in Nodes : Entry(h)[n] \in Holds => copy[n][h]
\* A node serves only from its own view (ready or leaving), and then it holds the data.
ServesHeld == \A h \in Points, n \in Nodes : view[n][h].mine \in Holds => copy[n][h]
\* Only published, non-draining members carrying the dataset are given new ranges.
NewOwnersPublished == [][\A h \in Points, n \in Nodes :
                            (Entry(h)'[n] = "populating" /\ Entry(h)[n] # "populating")
                                => pub[n] = "ready" /\ n \in Carriers]_vars
\* A range that has a holder still in membership keeps a holder.
NoEarlyDrop == [][\A h \in Points :
                    (\E n \in Nodes : Entry(h)[n] \in Holds /\ pub[n] # "absent")
                        => \E n \in Nodes : Entry(h)'[n] \in Holds]_vars
\* Without a relay majority nothing becomes ready, moves or is revoked by the directory.
Freeze == [][~quorum => <<starts, own>>' = <<starts, own>>]_vars
=============================================================================
