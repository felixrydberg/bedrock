---------------------------- MODULE membership ----------------------------
EXTENDS Naturals, FiniteSets
\* #15 / B-12 cell membership. Relays are numbered; the lowest relay of a majority
\* side publishes. Mesh exchange and reconciliation are atomic in this abstraction.
\* #19 / B-16: the publisher also writes the L2 placement directory into the same namespace.
\* Points are possible range starts; a range ends where the next start begins.
CONSTANTS Relays, Nodes, Grace, Points
None == 0
VARIABLES up, side, conn, drain, leftAt, gone, gap, pub, split, starts, own
vars == <<up, side, conn, drain, leftAt, gone, gap, pub, split, starts, own>>
Holds == {"ready", "leaving"}
\* The range covering point h: the greatest start at or below it.
Cover(h) == CHOOSE s \in starts : s <= h /\ \A t \in starts : t <= h => t <= s
Entry(h) == own[Cover(h)]

Live(r) == {s \in Relays : up[s] /\ side[s] = side[r]}
Quorum(r) == up[r] /\ 2 * Cardinality(Live(r)) > Cardinality(Relays)
Publisher(r) == Quorum(r) /\ \A s \in Live(r) : r <= s
Held(r, n) == conn[n] \in Live(r)
Status(n) == IF drain[n] THEN "draining" ELSE "ready"
Min(a, b) == IF a < b THEN a ELSE b
Parted(u, sd) == \E a, b \in Relays : u[a] /\ u[b] /\ sd[a] # sd[b]

\* Each relay's absence clock: down relays lose it and a held node resets it. A sibling counts
\* as live only once its snapshot is synced, so its nodes are held as soon as it rejoins.
Clocks(u, sd, c) ==
    [r \in Relays |-> [n \in Nodes |->
        IF ~u[r] \/ (c[n] # None /\ u[c[n]] /\ sd[c[n]] = sd[r]) THEN 0 ELSE gone[r][n]]]

Init == /\ up = [r \in Relays |-> TRUE] /\ side = [r \in Relays |-> 1]
        /\ conn = [n \in Nodes |-> None] /\ drain = [n \in Nodes |-> FALSE]
        /\ leftAt = [n \in Nodes |-> None]
        /\ gone = [r \in Relays |-> [n \in Nodes |-> 0]]
        /\ gap = [n \in Nodes |-> 0] /\ pub = [n \in Nodes |-> "absent"] /\ split = FALSE
        /\ starts = {0} /\ own = [p \in Points |-> [n \in Nodes |-> "none"]]

\* A node's stream reaches a relay; a new session clears that node's graceful-leave marker.
Connect(n, r) == /\ up[r] /\ conn[n] = None
                 /\ conn' = [conn EXCEPT ![n] = r]
                 /\ leftAt' = [leftAt EXCEPT ![n] = None]
                 /\ gap' = [gap EXCEPT ![n] = 0]
                 /\ gone' = Clocks(up, side, conn')
                 /\ UNCHANGED <<up, side, drain, pub, split, starts, own>>
\* Node crash, cut-off stream or failover start.
Disconnect(n) == /\ conn[n] # None /\ conn' = [conn EXCEPT ![n] = None]
                 /\ UNCHANGED <<up, side, drain, leftAt, gone, gap, pub, split, starts, own>>
Drain(n) == /\ conn[n] # None /\ ~drain[n] /\ drain' = [drain EXCEPT ![n] = TRUE]
            /\ UNCHANGED <<up, side, conn, leftAt, gone, gap, pub, split, starts, own>>
\* Graceful shutdown: the holding relay remembers that the draining node left.
Leave(n) == /\ conn[n] # None /\ drain[n]
            /\ leftAt' = [leftAt EXCEPT ![n] = conn[n]]
            /\ conn' = [conn EXCEPT ![n] = None]
            /\ UNCHANGED <<up, side, drain, gone, gap, pub, split, starts, own>>
\* The process restarts with the same identity and is ready again.
Restart(n) == /\ conn[n] = None /\ drain[n] /\ drain' = [drain EXCEPT ![n] = FALSE]
              /\ UNCHANGED <<up, side, conn, leftAt, gone, gap, pub, split, starts, own>>
Crash(r) == /\ up[r] /\ up' = [up EXCEPT ![r] = FALSE]
            /\ conn' = [n \in Nodes |-> IF conn[n] = r THEN None ELSE conn[n]]
            /\ leftAt' = [n \in Nodes |-> IF leftAt[n] = r THEN None ELSE leftAt[n]]
            /\ gone' = Clocks(up', side, conn')
            /\ UNCHANGED <<side, drain, gap, pub, split, starts, own>>
Recover(r) == /\ ~up[r] /\ up' = [up EXCEPT ![r] = TRUE]
              /\ split' = (split \/ Parted(up', side))
              /\ gone' = Clocks(up', side, conn)
              /\ UNCHANGED <<side, conn, drain, leftAt, gap, pub, starts, own>>
\* Relay-to-relay partitions are two transitive sides.
Move(r) == /\ side' = [side EXCEPT ![r] = 3 - side[r]]
           /\ split' = (split \/ Parted(up, side'))
           /\ gone' = Clocks(up, side', conn)
           /\ UNCHANGED <<up, conn, drain, leftAt, gap, pub, starts, own>>
Tick == /\ gap' = [n \in Nodes |-> IF conn[n] # None THEN 0 ELSE Min(gap[n] + 1, Grace)]
        /\ gone' = [r \in Relays |-> [n \in Nodes |->
                        IF ~up[r] \/ Held(r, n) THEN 0 ELSE Min(gone[r][n] + 1, Grace)]]
        /\ UNCHANGED <<up, side, conn, drain, leftAt, pub, split, starts, own>>
\* Reconciliation diffs the desired view against the subscribed namespace.
Set(r, n) == /\ Publisher(r) /\ Held(r, n) /\ pub[n] # Status(n)
             /\ pub' = [pub EXCEPT ![n] = Status(n)]
             /\ UNCHANGED <<up, side, conn, drain, leftAt, gone, gap, split, starts, own>>
Delete(r, n) == /\ Publisher(r) /\ pub[n] # "absent" /\ ~Held(r, n)
                /\ (leftAt[n] \in Live(r) \/ gone[r][n] >= Grace)
                /\ pub' = [pub EXCEPT ![n] = "absent"]
                /\ UNCHANGED <<up, side, conn, drain, leftAt, gone, gap, split, starts, own>>
\* The assigner (B-15 `assign` plus the interim promotion) writes one range entry `o` at
\* start p, splitting the covering range when p is new. Any plan within these rules:
\* new owners are published ready members; owners leave the directory only once removed
\* from membership, or when leaving and every other owner of the entry is ready.
Write(r, p, o) ==
    LET cur == Entry(p) IN
    /\ Publisher(r) /\ (p \notin starts \/ o # own[p])
    /\ \A n \in Nodes :
        /\ o[n] = "populating" => \/ cur[n] = "populating"
                                  \/ cur[n] = "none" /\ pub[n] = "ready"
        /\ o[n] = "ready" => cur[n] \in {"ready", "populating"}
        /\ o[n] = "leaving" => cur[n] \in Holds
        /\ (o[n] = "none" /\ cur[n] # "none") =>
            \/ pub[n] = "absent"
            \/ cur[n] = "leaving" /\ (\A m \in Nodes : o[m] # "populating")
                                  /\ (\E m \in Nodes : o[m] = "ready")
    /\ starts' = starts \cup {p}
    /\ own' = [own EXCEPT ![p] = o]
    /\ UNCHANGED <<up, side, conn, drain, leftAt, gone, gap, pub, split>>

Next == \/ Tick
        \/ \E n \in Nodes : Disconnect(n) \/ Drain(n) \/ Leave(n) \/ Restart(n)
        \/ \E n \in Nodes, r \in Relays : Connect(n, r) \/ Set(r, n) \/ Delete(r, n)
        \/ \E r \in Relays : Crash(r) \/ Recover(r) \/ Move(r)
        \/ \E r \in Relays, p \in Points, o \in [Nodes -> {"none", "populating"} \cup Holds] :
            Write(r, p, o)
Spec == Init /\ [][Next]_vars

TypeOK == /\ conn \in [Nodes -> Relays \cup {None}] /\ leftAt \in [Nodes -> Relays \cup {None}]
          /\ pub \in [Nodes -> {"absent", "ready", "draining"}]
          /\ gone \in [Relays -> [Nodes -> 0..Grace]] /\ gap \in [Nodes -> 0..Grace]
          /\ starts \subseteq Points
          /\ own \in [Points -> [Nodes -> {"none", "populating"} \cup Holds]]
\* Ends are implicit, so the directory covers the hash space after every write.
Covered == 0 \in starts
\* Only published, non-draining members are given new ranges.
NewOwnersPublished == [][\A h \in Points, n \in Nodes :
                            (Entry(h)'[n] = "populating" /\ Entry(h)[n] # "populating")
                                => pub[n] = "ready"]_vars
\* A range that has a holder still in membership keeps a holder: a leaving owner goes only
\* once a new owner is ready.
NoEarlyDrop == [][\A h \in Points :
                    (\E n \in Nodes : Entry(h)[n] \in Holds /\ pub[n] # "absent")
                        => \E n \in Nodes : Entry(h)'[n] \in Holds]_vars
\* Majority sides cannot both elect a publisher.
OnePublisher == \A r, s \in Relays : Publisher(r) /\ Publisher(s) => r = s
\* Without relay partitions, no relay's clock runs ahead of the node's real absence.
ClockBound == ~split => \A r \in Relays, n \in Nodes : gone[r][n] <= gap[n]
\* Failover inside the grace period changes nothing; removal needs a graceful leave,
\* a full grace period of absence, or a relay partition (the majority side wins).
NoEarlyRemoval == [][\A n \in Nodes : (pub[n] # "absent" /\ pub'[n] = "absent")
                        => (split \/ leftAt[n] # None \/ gap[n] >= Grace)]_vars
\* With no majority anywhere, the published view and directory are frozen.
Freeze == [][(\A r \in Relays : ~Quorum(r)) => <<pub, starts, own>>' = <<pub, starts, own>>]_vars
=============================================================================
