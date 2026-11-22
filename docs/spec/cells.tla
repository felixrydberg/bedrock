------------------------------ MODULE cells ------------------------------
EXTENDS Naturals, FiniteSets
\* #89 / B-57: how an upper-tier Gateway learns the cells below it. A child relay's Hello names its cell and address;
\* the parent relay checks both against the child's certificate and lists its live child sessions in Relay.Cells. A
\* Gateway caches a listing, resolves the cell's Gateways at a listed relay address and forwards. An address can be
\* reprovisioned for another cell after its session ends, so a cached listing may be stale or wrong. The cell Gateway
\* serves only requests for its own cell, so a wrong listing can only cost availability (#107 / B-66). The Gateway
\* caches each cell's gateway table; a relay's Routes reply names the relay's own cell, and the Gateway keeps it only
\* under that cell, so a reprovisioned relay's table (empty or not) never stands in for another cell's.
\* One parent tier is modeled; a region Gateway's walk (region relays list DCs, DC relays list cells) repeats it.
\* Relay.Cells reads the parent's sessions and changes no replication state: like Tick, it never advances applied state.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS Addrs, Cells, Bug
None == "none"
VARIABLES cellOf, session, listed, cache, answered, tables
vars == <<cellOf, session, listed, cache, answered, tables>>
\* cellOf: the cell the relay and Gateways at an address serve, as their certificates name it. session: the address
\* holds a live child session to the parent. listed: what the parent's Relay.Cells returns. cache: the Gateway's
\* copy of an earlier listing. answered: forwarded requests that were served, with the cell that served them.
\* tables: for each cell, whose gateway table the Gateway caches for it (None before its first fetch).
Entry == [cell : Cells, addr : Addrs]
Init == /\ cellOf \in [Addrs -> Cells \cup {None}] /\ session = [a \in Addrs |-> FALSE]
        /\ listed = {} /\ cache = {} /\ answered = {} /\ tables = [c \in Cells |-> None]
\* A child relay opens its session, announcing `claim`. The parent accepts only the cell its certificate names.
Connect(a, claim) == /\ ~session[a] /\ cellOf[a] # None
                     /\ (claim = cellOf[a] \/ Bug = "unchecked")
                     /\ session' = [session EXCEPT ![a] = TRUE]
                     /\ listed' = {e \in listed : e.addr # a} \cup {[cell |-> claim, addr |-> a]}
                     /\ UNCHANGED <<cellOf, cache, answered, tables>>
Disconnect(a) == /\ session[a] /\ session' = [session EXCEPT ![a] = FALSE]
                 /\ listed' = IF Bug = "sticky" THEN listed ELSE {e \in listed : e.addr # a}
                 /\ UNCHANGED <<cellOf, cache, answered, tables>>
\* The host is reprovisioned for another cell (or none); its old session has ended.
Reassign(a, c) == /\ ~session[a] /\ cellOf' = [cellOf EXCEPT ![a] = c]
                  /\ UNCHANGED <<session, listed, cache, answered, tables>>
\* The Gateway fetches a listing (on a miss or after every forward to a cell failed).
Fetch == /\ cache' = listed /\ UNCHANGED <<cellOf, session, listed, answered, tables>>
\* The Gateway fetches `target`'s gateway table from a cached entry's relay, which answers with its own cell's table,
\* naming that cell. A reply naming another cell is refused.
Resolve(target) == /\ \E e \in cache :
                        /\ e.cell = target /\ cellOf[e.addr] # None
                        /\ (cellOf[e.addr] = target \/ Bug = "unattributed")
                        /\ tables' = [tables EXCEPT ![target] = cellOf[e.addr]]
                   /\ UNCHANGED <<cellOf, session, listed, cache, answered>>
\* The Gateway forwards a request for `target` through a cached entry. The cell Gateway at that address serves it
\* only for its own cell; otherwise the request fails and the Gateway tries another entry or fetches again.
Forward(target) == /\ \E e \in cache :
                        /\ e.cell = target /\ cellOf[e.addr] # None
                        /\ (cellOf[e.addr] = target \/ Bug = "unscoped")
                        /\ answered' = answered \cup {[target |-> target, by |-> cellOf[e.addr]]}
                   /\ UNCHANGED <<cellOf, session, listed, cache, tables>>
Next == \/ \E a \in Addrs : \/ Disconnect(a)
                            \/ \E c \in Cells : Connect(a, c)
                            \/ \E c \in Cells \cup {None} : Reassign(a, c)
        \/ Fetch
        \/ \E c \in Cells : Forward(c) \/ Resolve(c)
Spec == Init /\ [][Next]_vars
TypeOK == /\ cellOf \in [Addrs -> Cells \cup {None}] /\ session \in [Addrs -> BOOLEAN]
          /\ listed \subseteq Entry /\ cache \subseteq Entry
          /\ answered \subseteq [target : Cells, by : Cells]
          /\ tables \in [Cells -> Cells \cup {None}]
\* The parent lists exactly its live child sessions, each under the cell its certificate names.
ListedLive == listed = {[cell |-> cellOf[a], addr |-> a] : a \in {x \in Addrs : session[x]}}
\* Every forwarded request was served in the cell it targeted, however stale the Gateway's listing.
ServedInTarget == \A r \in answered : r.by = r.target
\* A cell's cached gateway table is that cell's own, however stale the listing that led to its relay.
TablesOfTarget == \A c \in Cells : tables[c] \in {None, c}
=============================================================================
