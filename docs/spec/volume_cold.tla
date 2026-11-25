---------------------------- MODULE volume_cold ----------------------------
EXTENDS Naturals, FiniteSets, TLC
\* S-05 (#113): the cold tier under volume.tla's `s3`. volume.tla checks one S3 sequence per actor
\* with SlateDB fencing between maintainers. Here that sequence lives in range databases that split
\* and merge by clone, and moves between cells with the actor's first slot (statefull/v2.md §5).
\*
\* One actor sits at hash point P of the points 1..3. A cell's catalog names the database serving
\* each point; a database is <<cell, lo, hi, generation>>. Maintainers each see their cell's
\* directory (a partition of the points) and the actor's first cell, possibly stale. Opening a span
\* the catalog serves with one database of exactly that span opens it as writer, fencing any other.
\* Otherwise a maintainer creates one, in steps: it fences and marks retired every database serving
\* part of the span, clones the new database from them, and replaces the catalog if it has not
\* changed since it read it. A writer that finds its database retired writes nothing; one fenced
\* fails its flush. A flush writes any complete prefix of committed entries after its flushed point;
\* once durable, the slots truncate through that prefix only. A database behind the slot first copies
\* the actor from another cell's database that holds it that far (a first-slot move).
\*
\* Database contents are the entry sequences they hold, so a flush over a gap shows.
\* Two configurations keep the state space small: volume_cold-split.cfg has two maintainers of one
\* cell, one of them stale, splitting and merging; volume_cold-move.cfg moves the first slot between
\* two cells with one maintainer each.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS Cells, Maint, Moves, MaxSeq, MaxGen, MaxEp, Bug
VARIABLES com, fl, first, moved, fv, view, cat, cv, nxt, dbs, wep, ret, meta, content, handle, pend, op
vars == <<com, fl, first, moved, fv, view, cat, cv, nxt, dbs, wep, ret, meta, content, handle, pend, op>>

P == 2
Points == 1..3
\* Maintainers 1 and 2 are in c1, where the actor's first slot starts, so one can be stale while the
\* other splits or merges; maintainer 3 is in c2, where the first slot moves.
Cell(m) == IF m = 3 THEN "c2" ELSE "c1"
\* The directories a cell goes through: whole, split with the actor above, and split around it (an
\* interior range). A first-slot move keeps one range per cell.
Partitions == IF Moves THEN {{<<1, 3>>}}
              ELSE {{<<1, 3>>}, {<<1, 1>>, <<2, 3>>}, {<<1, 1>>, <<2, 2>>, <<3, 3>>}}
Span(s) == s[1]..s[2]
Block(v) == CHOOSE s \in v : P \in Span(s)
Dbs == {<<c, lo, hi, g>> : c \in Cells, lo \in Points, hi \in Points, g \in 0..MaxGen}
NoDb == <<"none", 0, 0, 0>>
DbSpan(d) == <<d[2], d[3]>>
NoOp == [ph |-> "idle", s |-> <<0, 0>>, todo |-> {}, src |-> NoDb, ver |-> 0, gen |-> 0]
Max(x, y) == IF x > y THEN x ELSE y
Min(x, y) == IF x < y THEN x ELSE y

Init == /\ com = 0 /\ fl = 0 /\ first = "c1" /\ moved = FALSE
        /\ fv = [m \in Maint |-> "c1"] /\ view = [m \in Maint |-> {<<1, 3>>}]
        /\ cat = [c \in Cells |-> [p \in Points |-> NoDb]] /\ cv = [c \in Cells |-> 0]
        /\ nxt = [c \in Cells |-> 0] /\ dbs = {}
        /\ wep = [d \in Dbs |-> 0] /\ ret = [d \in Dbs |-> FALSE]
        /\ meta = [d \in Dbs |-> 0] /\ content = [d \in Dbs |-> {}]
        /\ handle = [m \in Maint |-> [d |-> NoDb, ep |-> 0]]
        /\ pend = [m \in Maint |-> 0] /\ op = [m \in Maint |-> NoOp]

\* ---- Environment ----
Commit == /\ com < MaxSeq /\ com' = com + 1
          /\ UNCHANGED <<fl, first, moved, fv, view, cat, cv, nxt, dbs, wep, ret, meta, content, handle, pend, op>>
\* The directory changes, and each maintainer learns it at its own pace.
See(m) == /\ \E v \in Partitions : v # view[m] /\ view' = [view EXCEPT ![m] = v]
          /\ UNCHANGED <<com, fl, first, moved, fv, cat, cv, nxt, dbs, wep, ret, meta, content, handle, pend, op>>
\* A topology change moves the actor's first slot to the other cell once; maintainers learn it late.
Move == /\ Moves /\ ~moved /\ moved' = TRUE /\ first' = "c2"
        /\ UNCHANGED <<com, fl, fv, view, cat, cv, nxt, dbs, wep, ret, meta, content, handle, pend, op>>
Learn(m) == /\ fv[m] # first /\ fv' = [fv EXCEPT ![m] = first]
            /\ UNCHANGED <<com, fl, first, moved, view, cat, cv, nxt, dbs, wep, ret, meta, content, handle, pend, op>>

\* ---- Maintainer ----
\* A range it no longer maintains is closed.
Close(m) == /\ handle[m].d # NoDb /\ DbSpan(handle[m].d) # Block(view[m])
            /\ handle' = [handle EXCEPT ![m] = [d |-> NoDb, ep |-> 0]]
            /\ UNCHANGED <<com, fl, first, moved, fv, view, cat, cv, nxt, dbs, wep, ret, meta, content, pend, op>>
\* Opening span s: its own unretired database, or a plan to create one.
Plan(m, s) ==
    LET c == Cell(m)
        over == {cat[c][p] : p \in Span(s)} \ {NoDb}
        own == over = {cat[c][s[1]]} /\ cat[c][s[1]] # NoDb /\ DbSpan(cat[c][s[1]]) = s
                /\ \A p \in Span(s) : cat[c][p] = cat[c][s[1]]
    IN /\ op[m].ph = "idle" /\ s \in view[m]
       /\ IF own /\ ~ret[cat[c][s[1]]]
          THEN LET d == cat[c][s[1]] IN
               /\ s = Block(view[m]) /\ handle[m].d # d
               /\ wep' = [wep EXCEPT ![d] = @ + 1]
               /\ handle' = [handle EXCEPT ![m] = [d |-> d, ep |-> wep[d] + 1]]
               /\ UNCHANGED op
          ELSE /\ nxt[c] <= MaxGen
               /\ op' = [op EXCEPT ![m] = [ph |-> IF over = {} THEN "cas" ELSE "retire", s |-> s, todo |-> over,
                                            src |-> cat[c][P], ver |-> cv[c], gen |-> nxt[c]]]
               /\ UNCHANGED <<wep, handle>>
       /\ UNCHANGED <<com, fl, first, moved, fv, view, cat, cv, nxt, dbs, ret, meta, content, pend>>
\* Fence a source and mark it retired, in one commit that fails if another writer opened it since.
Retire(m) == /\ op[m].ph = "retire" /\ op[m].todo # {}
             /\ \E d \in op[m].todo :
                  /\ IF Bug = "no-retire" THEN UNCHANGED <<wep, ret>>
                     ELSE /\ wep' = [wep EXCEPT ![d] = @ + 1] /\ ret' = [ret EXCEPT ![d] = TRUE]
                  /\ LET rest == op[m].todo \ {d} IN
                     op' = [op EXCEPT ![m].todo = rest, ![m].ph = IF rest = {} THEN "clone" ELSE "retire"]
             /\ UNCHANGED <<com, fl, first, moved, fv, view, cat, cv, nxt, dbs, meta, content, handle, pend>>
\* The clone shares its sources' files as they are now; repeating a clone of the same name is a no-op.
Clone(m) == LET o == op[m]
                nd == <<Cell(m), o.s[1], o.s[2], o.gen>>
                takes == P \in Span(o.s) /\ o.src # NoDb
            IN /\ o.ph = "clone"
               /\ IF nd \in dbs THEN UNCHANGED <<dbs, meta, content>>
                  ELSE /\ dbs' = dbs \cup {nd}
                       /\ meta' = [meta EXCEPT ![nd] = IF takes THEN meta[o.src] ELSE 0]
                       /\ content' = [content EXCEPT ![nd] = IF takes THEN content[o.src] ELSE {}]
               /\ op' = [op EXCEPT ![m].ph = "cas"]
               /\ UNCHANGED <<com, fl, first, moved, fv, view, cat, cv, nxt, wep, ret, handle, pend>>
\* Replace the catalog if unchanged since the plan read it; otherwise the clone is never written.
Cas(m) == LET o == op[m]
              c == Cell(m)
              nd == <<c, o.s[1], o.s[2], o.gen>>
          IN /\ o.ph = "cas"
             /\ IF cv[c] = o.ver \/ Bug = "no-cas"
                THEN /\ cat' = [cat EXCEPT ![c] = [p \in Points |-> IF p \in Span(o.s) THEN nd ELSE @[p]]]
                     /\ cv' = [cv EXCEPT ![c] = @ + 1] /\ nxt' = [nxt EXCEPT ![c] = o.gen + 1]
                     /\ dbs' = dbs \cup {nd}
                     /\ IF o.s = Block(view[m])
                        THEN /\ wep' = [wep EXCEPT ![nd] = @ + 1]
                             /\ handle' = [handle EXCEPT ![m] = [d |-> nd, ep |-> wep[nd] + 1]]
                        ELSE UNCHANGED <<wep, handle>>
                ELSE UNCHANGED <<cat, cv, nxt, dbs, wep, handle>>
             /\ op' = [op EXCEPT ![m] = NoOp]
             /\ UNCHANGED <<com, fl, first, moved, fv, view, ret, meta, content, pend>>
\* A flush by a maintainer that believes the first slot is in its cell. Behind the slot, it copies
\* the actor first from another cell's database holding it that far, or does not flush.
Flush(m) ==
    LET d == handle[m].d
        holders == {cat[c][P] : c \in Cells \ {Cell(m)}} \ {NoDb}
        ahead == {x \in holders : meta[x] >= fl}
    IN /\ d # NoDb /\ wep[d] = handle[m].ep /\ fv[m] = Cell(m) /\ com > fl
       /\ meta[d] >= fl \/ ahead # {} \/ Bug = "no-adopt"
       /\ \E through \in (fl + 1)..com :
            /\ \E base \in IF meta[d] >= fl \/ Bug = "no-adopt" THEN {d} ELSE ahead :
                 /\ meta' = [meta EXCEPT ![d] = Max(meta[base], through)]
                 /\ content' = [content EXCEPT ![d] = content[base] \cup (fl + 1)..through]
            /\ pend' = [pend EXCEPT ![m] = IF Bug = "prefix-truncate" THEN com ELSE through]
       /\ UNCHANGED <<com, fl, first, moved, fv, view, cat, cv, nxt, dbs, wep, ret, handle, op>>
\* Once the flush is durable, the slots drop the log through it.
Truncate(m) == /\ pend[m] > fl /\ fl' = pend[m] /\ pend' = [pend EXCEPT ![m] = 0]
               /\ UNCHANGED <<com, first, moved, fv, view, cat, cv, nxt, dbs, wep, ret, meta, content, handle, op>>

Next == \/ Commit \/ Move
        \/ \E m \in Maint : See(m) \/ Learn(m) \/ Close(m) \/ Retire(m) \/ Clone(m) \/ Cas(m)
                            \/ Flush(m) \/ Truncate(m) \/ \E s \in view[m] : Plan(m, s)
Spec == Init /\ [][Next]_vars
Bounded == \A d \in Dbs : wep[d] <= MaxEp

TypeOK == /\ com \in 0..MaxSeq /\ fl \in 0..com /\ dbs \subseteq Dbs
          /\ \A m \in Maint : op[m].ph \in {"idle", "retire", "clone", "cas"}
\* Every database holds the actor without gaps: everything up to its meta.
Consistent == \A d \in dbs : content[d] = 1..meta[d]
\* No database holds past the commit point.
S3Committed == \A d \in dbs : meta[d] <= com
\* Everything the slots dropped is in a database a read finds through a catalog (cold reads fall
\* back from the first cell to the others).
NoLostFlush == fl = 0 \/ \E c \in Cells : cat[c][P] # NoDb /\ meta[cat[c][P]] >= fl
=============================================================================
