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
\* sick is the set of points a member's storage refuses and reports unhealthy (#13 / B-14): all of them
\* while the whole instance is quarantined, and after a verification pass only the damaged ones
\* (#103 / B-62). A copy at a sick point may be damaged, so it never counts as the ready owner that lets
\* another copy go there. bad is a ghost: the points whose local copy really is damaged. The quarantine
\* marker in the engine keeps sick across a restart; one that cannot be read as points means all of them.
\* With Reported (#114 / S-06, the `volume` dataset) no relay stream carries the data: the node
\* copies a range itself, reports that it holds it, and its relay checks the report. Nothing keeps
\* such a copy current once the node stops owning the range, so each assignment has a generation
\* (the revision that gave it) and a report counts only for the generation it was copied for. A
\* node can also lose its disk while it is down (Wipe).
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS Nodes, Points, Carriers, MaxRev, Faulty, Capped, MaxCuts, MaxFlips, Reported, Bug
VARIABLES pub, sick, bad, conn, quorum, starts, own, gen, rev, told, chan, follow, copy, view, rep, sent, known,
          held, cuts, flips
\* rev: directory revision. told: what the relays last pushed a node for a point
\* ("unknown" after its session ends). chan: pushed frames the node has not processed; `ok`
\* records that the node's view has reached the revision the frame was derived from.
\* follow/copy: the node follows the point / holds its data. view: what the node's own,
\* possibly lagging, directory view says about each point: its own state (`mine`) and
\* whether another owner is ready (`other`), the only facts the node reads from it.
\* With Reported (all stay 0 otherwise): gen is each owner's generation per entry. sent: the
\* generation the relay last pushed the node for a point; known: the one in the last frame the node
\* processed; held: the one the node holds a copy for (0: none). rep: what the holding relay keeps
\* of the node's latest report (a generation, 0 for none); it lives in the node's session, and every
\* acknowledgement replaces it. copy is then a ghost: the node's data is current, which it stops
\* being once the directory drops the node, whatever the node believes.
nvars == <<told, chan, follow, copy, view, rep, sent, known, held>>
vars == <<pub, sick, bad, conn, quorum, starts, own, gen, rev, nvars, cuts, flips>>
Holds == {"ready", "leaving"}
\* The range covering point h: the greatest start at or below it.
Cover(h) == CHOOSE s \in starts : s <= h /\ \A t \in starts : t <= h => t <= s
Entry(h) == own[Cover(h)]
Gen(h) == gen[Cover(h)]
\* The points of the range starting at p when the starts are S.
Span(S, p) == {h \in Points : h >= p /\ \A s \in S : s <= h => s <= p}
\* Whether m's copy of points S counts as a healthy ready owner's. Bug "start-only" checks only the range's
\* start, as if a damaged range were named by the start it had when it was damaged.
Min(S) == CHOOSE p \in S : \A q \in S : p <= q
Counts(m, S) == \/ Bug = "any-ready"
                \/ pub[m] = "ready" /\ (IF Bug = "start-only" THEN Min(S) \notin sick[m] ELSE S \cap sick[m] = {})
\* Node n's view of the current directory.
Current(n) == [h \in Points |-> [mine |-> Entry(h)[n],
                                 other |-> \E m \in Nodes \ {n} :
                                     Entry(h)[m] = "ready" /\ Counts(m, {h})]]
Count(c) == IF Capped THEN c + 1 ELSE c

Init == /\ pub = [n \in Nodes |-> "absent"] /\ sick = [n \in Nodes |-> {}] /\ bad = sick /\ conn = [n \in Nodes |-> FALSE] /\ quorum = TRUE
        /\ starts = {0} /\ own = [p \in Points |-> [n \in Nodes |-> "none"]] /\ rev = 0
        /\ gen = [p \in Points |-> [n \in Nodes |-> 0]]
        /\ told = [n \in Nodes |-> [h \in Points |-> "unknown"]]
        /\ chan = [n \in Nodes |-> [h \in Points |-> <<>>]]
        /\ follow = [n \in Nodes |-> [h \in Points |-> FALSE]]
        /\ copy = [n \in Nodes |-> [h \in Points |-> FALSE]]
        /\ view = [n \in Nodes |-> [h \in Points |-> [mine |-> "none", other |-> FALSE]]]
        /\ rep = [n \in Nodes |-> [h \in Points |-> 0]] /\ sent = rep /\ known = rep /\ held = rep
        /\ cuts = 0 /\ flips = 0

\* A connect may follow a restart, which reads sick back from the quarantine marker: the same points, or
\* all of them if the marker cannot be read as points. Bug "forget" comes back healthy.
Connect(n) == /\ ~conn[n] /\ conn' = [conn EXCEPT ![n] = TRUE]
              /\ \E s \in IF sick[n] = {} THEN {{}} ELSE {sick[n], Points} :
                     sick' = [sick EXCEPT ![n] = IF Bug = "forget" THEN {} ELSE s]
              /\ UNCHANGED <<pub, bad, quorum, starts, own, gen, rev, nvars, cuts, flips>>
\* The node keeps its data on disk. Its unprocessed frames are lost, and its next relay
\* pushes everything again. Its report ends with the session, as a relay keeps it per session.
Disconnect(n) == /\ conn[n] /\ conn' = [conn EXCEPT ![n] = FALSE]
                 /\ chan' = [chan EXCEPT ![n] = [h \in Points |-> <<>>]]
                 /\ told' = [told EXCEPT ![n] = [h \in Points |-> "unknown"]]
                 /\ rep' = [rep EXCEPT ![n] = [h \in Points |-> 0]]
                 /\ sent' = [sent EXCEPT ![n] = [h \in Points |-> 0]]
                 /\ cuts' = Count(cuts)
                 /\ UNCHANGED <<pub, sick, bad, quorum, starts, own, gen, rev, follow, copy, view, known, held, flips>>
\* A node that is down loses its disk and restarts holding nothing (Reported only; relay-fed
\* datasets rebuild through relays.tla's LoseLocal).
Wipe(n) == /\ Reported /\ ~conn[n] /\ \E h \in Points : held[n][h] # 0
           /\ copy' = [copy EXCEPT ![n] = [h \in Points |-> FALSE]]
           /\ follow' = [follow EXCEPT ![n] = [h \in Points |-> FALSE]]
           /\ known' = [known EXCEPT ![n] = [h \in Points |-> 0]]
           /\ held' = [held EXCEPT ![n] = [h \in Points |-> 0]]
           /\ UNCHANGED <<pub, sick, bad, conn, quorum, starts, own, gen, rev, told, chan, view, rep, sent, cuts, flips>>
Publish(n) == /\ quorum /\ conn[n] /\ pub[n] = "absent" /\ pub' = [pub EXCEPT ![n] = "ready"]
              /\ UNCHANGED <<sick, bad, conn, quorum, starts, own, gen, rev, nvars, cuts, flips>>
\* After the grace period, or at once after a graceful leave.
Remove(n) == /\ quorum /\ ~conn[n] /\ pub[n] # "absent" /\ pub' = [pub EXCEPT ![n] = "absent"]
             /\ UNCHANGED <<sick, bad, conn, quorum, starts, own, gen, rev, nvars, cuts, flips>>
\* A member's storage fails at points D: damage there, or an I/O failure that damages nothing (D empty).
\* The whole instance is quarantined at once, since the extent is unknown.
Sicken(n, D) == /\ pub[n] # "absent" /\ (D # {} \/ sick[n] # Points)
                /\ bad' = [bad EXCEPT ![n] = @ \cup D] /\ sick' = [sick EXCEPT ![n] = Points]
                /\ UNCHANGED <<pub, conn, quorum, starts, own, gen, rev, nvars, cuts, flips>>
\* A verification pass found every damaged point and the adapter named a span to discard that covers them,
\* so only the points D overlapping it stay sick. Bug "unverified" trusts the failure that was reported and
\* may leave other damaged points out.
Narrow(n, D) == /\ D # {} /\ D \subseteq sick[n] /\ D # sick[n]
                /\ (bad[n] \subseteq D \/ Bug = "unverified")
                /\ sick' = [sick EXCEPT ![n] = D]
                /\ UNCHANGED <<pub, bad, conn, quorum, starts, own, gen, rev, nvars, cuts, flips>>
\* Point h's rebuild discarded its span, re-bootstrapped and validated it (relays.tla checks how). A whole
\* instance's rebuild heals every point; the model may heal them one at a time.
Heal(n, h) == /\ h \in sick[n]
              /\ sick' = [sick EXCEPT ![n] = @ \ {h}] /\ bad' = [bad EXCEPT ![n] = @ \ {h}]
              /\ UNCHANGED <<pub, conn, quorum, starts, own, gen, rev, nvars, cuts, flips>>
Partition == /\ quorum' = ~quorum /\ flips' = Count(flips)
             /\ UNCHANGED <<pub, sick, bad, conn, starts, own, gen, rev, nvars, cuts>>

\* The holding relay's readiness for n over points S, shared over the sibling stream: n
\* processed every pushed frame (the last one assigned S), follows and holds S. With Reported the
\* relay cannot see the copy and checks the node's report instead, which must name the current
\* assignment's generation (bug "no-gen" takes any report).
ReadyFor(n, S) == /\ conn[n]
                  /\ \A h \in S : /\ told[n][h] = "on" /\ chan[n][h] = <<>>
                                  /\ IF Reported THEN rep[n][h] # 0 /\ (rep[n][h] = Gen(h)[n] \/ Bug = "no-gen")
                                     ELSE follow[n][h] /\ copy[n][h]
\* The publisher writes one range entry `o` at start p, splitting the covering range when p
\* is new. Any plan within these rules: new owners are published ready members carrying the
\* dataset; a populating owner becomes ready only from its relay's readiness; owners leave
\* the directory only once removed from membership, or when leaving and every other owner
\* of the entry is ready. With Reported, the first write of a directory may make new owners ready
\* at once: no data exists anywhere yet. A new owner's generation is the revision written, and a
\* node the write drops, or gives the range anew, no longer has current data there.
Write(p, o) ==
    LET cur == Entry(p)
        S == Span(starts \cup {p}, p)
        New(n) == cur[n] = "none" /\ ((pub[n] = "ready" /\ sick[n] = {}) \/ Bug = "absent") /\ n \in Carriers
        Next(n) == IF o[n] = "none" THEN 0 ELSE IF cur[n] = "none" THEN rev + 1 ELSE Gen(p)[n] IN
    /\ (quorum \/ Bug = "minority-write") /\ (p \notin starts \/ o # own[p])
    /\ \A n \in Nodes :
        /\ o[n] = "populating" => \/ cur[n] = "populating"
                                  \/ New(n)
        /\ o[n] = "ready" => \/ cur[n] = "ready"
                             \/ cur[n] = "populating" /\ (ReadyFor(n, S) \/ Bug = "unready")
                             \/ Reported /\ rev = 0 /\ New(n)
        /\ o[n] = "leaving" => cur[n] \in Holds
        /\ (o[n] = "none" /\ cur[n] # "none") =>
            \/ pub[n] = "absent"
            \/ cur[n] = "leaving" /\ (\A m \in Nodes : o[m] # "populating")
                                  /\ (Bug = "early-drop" \/ \E m \in Nodes : o[m] = "ready" /\ Counts(m, S))
    /\ starts' = starts \cup {p}
    /\ own' = [own EXCEPT ![p] = o]
    /\ gen' = IF Reported THEN [gen EXCEPT ![p] = [n \in Nodes |-> Next(n)]] ELSE gen
    /\ copy' = IF Reported
               THEN [m \in Nodes |-> [h \in Points |->
                        IF h \in S /\ (o[m] = "none" \/ cur[m] = "none") THEN FALSE ELSE copy[m][h]]]
               ELSE copy
    /\ rev' = rev + 1
    /\ UNCHANGED <<pub, sick, bad, conn, quorum, told, chan, follow, view, rep, sent, known, held, cuts, flips>>

\* The node's relay pushes, from the published directory, whether it owns h in any state, and the
\* generation. Dropping h from the pushed set is the revoke. A relay may skip revisions: a revoke
\* and a new assignment of the same range can reach a node as one frame, or (bug "no-gen") none.
Push(n, h) == LET on == IF Entry(h)[n] # "none" THEN "on" ELSE "off"
                  g == IF on = "on" THEN Gen(h)[n] ELSE 0 IN
              /\ conn[n] /\ (told[n][h] # on \/ (Bug # "no-gen" /\ sent[n][h] # g))
              /\ told' = [told EXCEPT ![n][h] = on]
              /\ sent' = [sent EXCEPT ![n][h] = g]
              /\ chan' = [chan EXCEPT ![n][h] = Append(@, [ok |-> view[n] = Current(n), on |-> on = "on", gen |-> g])]
              /\ UNCHANGED <<pub, sick, bad, conn, quorum, starts, own, gen, rev, follow, copy, view, rep, known, held, cuts, flips>>
\* A faulty relay revokes h on its own, unpublished view.
BadRevoke(n, h) == /\ Faulty /\ conn[n]
                   /\ chan' = [chan EXCEPT ![n][h] = Append(@, [ok |-> view[n] = Current(n), on |-> FALSE, gen |-> 0])]
                   /\ UNCHANGED <<pub, sick, bad, conn, quorum, starts, own, gen, rev, told, follow, copy, view, rep, sent, known,
                                  held, cuts, flips>>
\* The node's own directory subscription catches up, past every pushed frame's revision.
Observe(n) == /\ conn[n] /\ view[n] # Current(n)
              /\ view' = [view EXCEPT ![n] = [h \in Points |->
                     IF Bug = "view" /\ Entry(h)[n] = "populating" THEN [Current(n)[h] EXCEPT !.mine = "ready"]
                     ELSE Current(n)[h]]]
              /\ chan' = [chan EXCEPT ![n] = [h \in Points |->
                     [i \in 1..Len(chan[n][h]) |-> [chan[n][h][i] EXCEPT !.ok = TRUE]]]]
              /\ UNCHANGED <<pub, sick, bad, conn, quorum, starts, own, gen, rev, told, follow, copy, rep, sent, known, held,
                             cuts, flips>>
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
                                 /\ known' = [known EXCEPT ![n][h] = f.gen]
                                 /\ UNCHANGED <<copy, told, held>>
                    ELSE IF Confirms(n, h)
                         THEN /\ follow' = [follow EXCEPT ![n][h] = FALSE]
                              /\ copy' = IF Bug = "keep" THEN copy ELSE [copy EXCEPT ![n][h] = FALSE]
                              /\ held' = [held EXCEPT ![n][h] = 0]
                              /\ UNCHANGED <<told, known>>
                         ELSE /\ told' = [told EXCEPT ![n][h] = "unknown"]
                              /\ UNCHANGED <<follow, copy, known, held>>
                 \* The acknowledgement carries the node's report as of after the frame.
                 /\ rep' = IF Reported THEN [rep EXCEPT ![n][h] = IF follow'[n][h] THEN held'[n][h] ELSE 0]
                           ELSE rep
                 /\ UNCHANGED <<pub, sick, bad, conn, quorum, starts, own, gen, rev, view, sent, cuts, flips>>
\* Bootstrap and catch-up of a followed point (relays.tla checks how). With Reported, the node's own
\* copy of a range for the generation it was last pushed (volume.tla checks from where): current if
\* that is still its assignment.
Copy(n, h) == /\ conn[n] /\ follow[n][h]
              /\ IF Reported
                 THEN /\ held[n][h] # known[n][h]
                      /\ held' = [held EXCEPT ![n][h] = known[n][h]]
                      /\ copy' = [copy EXCEPT ![n][h] = Entry(h)[n] # "none" /\ Gen(h)[n] = known[n][h]]
                 ELSE /\ ~copy[n][h] /\ copy' = [copy EXCEPT ![n][h] = TRUE]
                      /\ UNCHANGED held
              /\ UNCHANGED <<pub, sick, bad, conn, quorum, starts, own, gen, rev, told, chan, follow, view, rep, sent, known,
                             cuts, flips>>
\* The node reports the generation it holds a copy for, on the stream that carries its
\* acknowledgements. Bug "early-report" reports the one it is still copying.
Report(n, h) == LET r == IF ~follow[n][h] THEN 0 ELSE IF Bug = "early-report" THEN known[n][h] ELSE held[n][h] IN
                /\ Reported /\ conn[n] /\ rep[n][h] # r
                /\ rep' = [rep EXCEPT ![n][h] = r]
                /\ UNCHANGED <<pub, sick, bad, conn, quorum, starts, own, gen, rev, told, chan, follow, copy, view, sent, known,
                               held, cuts, flips>>

Next == \/ Partition
        \/ \E n \in Nodes : Connect(n) \/ Disconnect(n) \/ Publish(n) \/ Remove(n)
                            \/ Observe(n) \/ Wipe(n)
                            \/ \E D \in SUBSET Points : Sicken(n, D) \/ Narrow(n, D)
        \/ \E p \in Points, o \in [Nodes -> {"none", "populating"} \cup Holds] : Write(p, o)
        \/ \E n \in Nodes, h \in Points : Push(n, h) \/ BadRevoke(n, h) \/ Receive(n, h) \/ Copy(n, h)
                                         \/ Report(n, h) \/ Heal(n, h)
Spec == Init /\ [][Next]_vars
\* Bounds the directory revisions, pushed frames, disconnects and quorum changes TLC explores.
Bound == /\ rev <= MaxRev /\ cuts <= MaxCuts /\ flips <= MaxFlips
         /\ \A n \in Nodes, h \in Points : Len(chan[n][h]) <= 2
\* Nodes are interchangeable model values when every node carries the dataset.
NodePerms == Permutations(Nodes)

TypeOK == /\ pub \in [Nodes -> {"absent", "ready"}] /\ quorum \in BOOLEAN
          /\ sick \in [Nodes -> SUBSET Points] /\ bad \in [Nodes -> SUBSET Points]
          /\ starts \subseteq Points
          /\ own \in [Points -> [Nodes -> {"none", "populating"} \cup Holds]]
          /\ told \in [Nodes -> [Points -> {"on", "off", "unknown"}]]
          /\ follow \in [Nodes -> [Points -> BOOLEAN]] /\ copy \in [Nodes -> [Points -> BOOLEAN]]
          /\ \A f \in {rep, sent, known, held} : f \in [Nodes -> [Points -> 0..MaxRev + 1]]
          /\ gen \in [Points -> [Nodes -> 0..MaxRev + 1]]
\* Ends are implicit, so the directory covers the hash space after every write.
DirectoryCoverage == 0 \in starts
\* Every ready or leaving owner holds the data: no owner is ready before its copy, and no
\* node drops data the directory still has it holding, even after a faulty revoke.
ReadyHasData == \A h \in Points, n \in Nodes : Entry(h)[n] \in Holds => copy[n][h]
\* An owner becomes ready only while it holds the data. ReadyHasData says more, but a wipe breaks
\* that on purpose: a wiped ready owner repairs before it serves (volume.tla's `repaired`).
\* The first write of a directory is exempt: nothing exists to copy.
ReadyCopied == [][\A h \in Points, n \in Nodes :
                     (rev # 0 /\ Entry(h)'[n] = "ready" /\ Entry(h)[n] # "ready") => copy[n][h]]_vars
\* A node serves only from its own view (ready or leaving), and then it holds the data.
ServesHeld == \A h \in Points, n \in Nodes : view[n][h].mine \in Holds => copy[n][h]
\* A node holds no data its relay revoked once it has processed that revoke: a confirmed revoke deletes, and a
\* refused one makes the relay push again.
OwnedOrLeaving == \A h \in Points, n \in Nodes : told[n][h] = "off" /\ chan[n][h] = <<>> => ~copy[n][h]
\* Only published, non-draining members carrying the dataset are given new ranges.
NewOwnersPublished == [][\A h \in Points, n \in Nodes :
                            (Entry(h)'[n] \in {"populating", "ready"} /\ Entry(h)[n] = "none")
                                => pub[n] = "ready" /\ sick[n] = {} /\ n \in Carriers]_vars
\* A range that has a holder still in membership keeps a holder.
NoDropBeforeReady == [][\A h \in Points :
                    (\E n \in Nodes : Entry(h)[n] \in Holds /\ pub[n] # "absent")
                        => \E n \in Nodes : Entry(h)'[n] \in Holds]_vars
Healthy(n, h) == pub[n] = "ready" /\ h \notin sick[n]
\* A directory write never lets go of a range's last healthy holder, even when an unhealthy owner
\* is listed as ready.
KeepsHealthyHolder == [][(<<pub, sick>>' = <<pub, sick>>) => \A h \in Points :
                            (\E n \in Nodes : Entry(h)[n] \in Holds /\ Healthy(n, h))
                                => \E n \in Nodes : Entry(h)'[n] \in Holds /\ Healthy(n, h)]_vars
\* A damaged point is refused, from its damage until its rebuild, across restarts (#103 / B-62).
DamageRefused == \A n \in Nodes : bad[n] \subseteq sick[n]
\* Without a relay majority nothing becomes ready, moves or is revoked by the directory.
Freeze == [][~quorum => <<starts, own, gen>>' = <<starts, own, gen>>]_vars
=============================================================================
