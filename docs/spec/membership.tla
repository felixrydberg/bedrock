---------------------------- MODULE membership ----------------------------
EXTENDS Naturals, FiniteSets
\* #15 / B-12 cell membership. Relays are numbered; the lowest relay of a majority
\* side publishes. Mesh exchange and reconciliation are atomic in this abstraction.
CONSTANTS Relays, Nodes, Grace
None == 0
VARIABLES up, side, conn, drain, leftAt, gone, gap, pub, split
vars == <<up, side, conn, drain, leftAt, gone, gap, pub, split>>

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

\* A node's stream reaches a relay; a new session clears that node's graceful-leave marker.
Connect(n, r) == /\ up[r] /\ conn[n] = None
                 /\ conn' = [conn EXCEPT ![n] = r]
                 /\ leftAt' = [leftAt EXCEPT ![n] = None]
                 /\ gap' = [gap EXCEPT ![n] = 0]
                 /\ gone' = Clocks(up, side, conn')
                 /\ UNCHANGED <<up, side, drain, pub, split>>
\* Node crash, cut-off stream or failover start.
Disconnect(n) == /\ conn[n] # None /\ conn' = [conn EXCEPT ![n] = None]
                 /\ UNCHANGED <<up, side, drain, leftAt, gone, gap, pub, split>>
Drain(n) == /\ conn[n] # None /\ ~drain[n] /\ drain' = [drain EXCEPT ![n] = TRUE]
            /\ UNCHANGED <<up, side, conn, leftAt, gone, gap, pub, split>>
\* Graceful shutdown: the holding relay remembers that the draining node left.
Leave(n) == /\ conn[n] # None /\ drain[n]
            /\ leftAt' = [leftAt EXCEPT ![n] = conn[n]]
            /\ conn' = [conn EXCEPT ![n] = None]
            /\ UNCHANGED <<up, side, drain, gone, gap, pub, split>>
\* The process restarts with the same identity and is ready again.
Restart(n) == /\ conn[n] = None /\ drain[n] /\ drain' = [drain EXCEPT ![n] = FALSE]
              /\ UNCHANGED <<up, side, conn, leftAt, gone, gap, pub, split>>
Crash(r) == /\ up[r] /\ up' = [up EXCEPT ![r] = FALSE]
            /\ conn' = [n \in Nodes |-> IF conn[n] = r THEN None ELSE conn[n]]
            /\ leftAt' = [n \in Nodes |-> IF leftAt[n] = r THEN None ELSE leftAt[n]]
            /\ gone' = Clocks(up', side, conn')
            /\ UNCHANGED <<side, drain, gap, pub, split>>
Recover(r) == /\ ~up[r] /\ up' = [up EXCEPT ![r] = TRUE]
              /\ split' = (split \/ Parted(up', side))
              /\ gone' = Clocks(up', side, conn)
              /\ UNCHANGED <<side, conn, drain, leftAt, gap, pub>>
\* Relay-to-relay partitions are two transitive sides.
Move(r) == /\ side' = [side EXCEPT ![r] = 3 - side[r]]
           /\ split' = (split \/ Parted(up, side'))
           /\ gone' = Clocks(up, side', conn)
           /\ UNCHANGED <<up, conn, drain, leftAt, gap, pub>>
Tick == /\ gap' = [n \in Nodes |-> IF conn[n] # None THEN 0 ELSE Min(gap[n] + 1, Grace)]
        /\ gone' = [r \in Relays |-> [n \in Nodes |->
                        IF ~up[r] \/ Held(r, n) THEN 0 ELSE Min(gone[r][n] + 1, Grace)]]
        /\ UNCHANGED <<up, side, conn, drain, leftAt, pub, split>>
\* Reconciliation diffs the desired view against the subscribed namespace.
Set(r, n) == /\ Publisher(r) /\ Held(r, n) /\ pub[n] # Status(n)
             /\ pub' = [pub EXCEPT ![n] = Status(n)]
             /\ UNCHANGED <<up, side, conn, drain, leftAt, gone, gap, split>>
Delete(r, n) == /\ Publisher(r) /\ pub[n] # "absent" /\ ~Held(r, n)
                /\ (leftAt[n] \in Live(r) \/ gone[r][n] >= Grace)
                /\ pub' = [pub EXCEPT ![n] = "absent"]
                /\ UNCHANGED <<up, side, conn, drain, leftAt, gone, gap, split>>

Next == \/ Tick
        \/ \E n \in Nodes : Disconnect(n) \/ Drain(n) \/ Leave(n) \/ Restart(n)
        \/ \E n \in Nodes, r \in Relays : Connect(n, r) \/ Set(r, n) \/ Delete(r, n)
        \/ \E r \in Relays : Crash(r) \/ Recover(r) \/ Move(r)
Spec == Init /\ [][Next]_vars

TypeOK == /\ conn \in [Nodes -> Relays \cup {None}] /\ leftAt \in [Nodes -> Relays \cup {None}]
          /\ pub \in [Nodes -> {"absent", "ready", "draining"}]
          /\ gone \in [Relays -> [Nodes -> 0..Grace]] /\ gap \in [Nodes -> 0..Grace]
\* Majority sides cannot both elect a publisher.
OnePublisher == \A r, s \in Relays : Publisher(r) /\ Publisher(s) => r = s
\* Without relay partitions, no relay's clock runs ahead of the node's real absence.
ClockBound == ~split => \A r \in Relays, n \in Nodes : gone[r][n] <= gap[n]
\* Failover inside the grace period changes nothing; removal needs a graceful leave,
\* a full grace period of absence, or a relay partition (the majority side wins).
NoEarlyRemoval == [][\A n \in Nodes : (pub[n] # "absent" /\ pub'[n] = "absent")
                        => (split \/ leftAt[n] # None \/ gap[n] >= Grace)]_vars
\* With no majority anywhere, the published view is frozen.
Freeze == [][(\A r \in Relays : ~Quorum(r)) => pub' = pub]_vars
=============================================================================
