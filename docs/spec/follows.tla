------------------------------ MODULE follows ------------------------------
EXTENDS Naturals, Sequences
\* One relay's upstream session for one namespace (#25 / B-23 Layer 3). The relay ends its follow and starts another,
\* as when it switches from namespace follows to a dataset follow or a subscriber leaves and returns. Requests reach
\* the upstream in order and deliveries return in order, but deliveries already sent for the ended follow still arrive
\* after the new one began. Each Follow carries an id the upstream stamps on its deliveries, and the relay drops
\* deliveries of follows it ended.
\* Bug names a deliberate bug for the check that must catch it (scripts/tlc.sh); "none" is the protocol.
CONSTANTS MaxRevision, MaxFollows, QueueLimit, Bug
VARIABLES head, next, follow, requests, serving, frames, held
vars == <<head, next, follow, requests, serving, frames, held>>
\* next: the revision the relay expects of its current follow. follow: that follow's id. requests: Follows on their
\* way upstream. serving: the follow the upstream serves and the next revision it sends. frames: deliveries on their
\* way back. held: the revisions the relay published, which a new follow resumes after.
Init == /\ head = 0 /\ next = 1 /\ follow = 1 /\ requests = <<>>
        /\ serving = [id |-> 1, pos |-> 1] /\ frames = <<>> /\ held = {}
Commit == /\ head < MaxRevision /\ head' = head + 1
          /\ UNCHANGED <<next, follow, requests, serving, frames, held>>
Send == /\ serving.pos <= head /\ Len(frames) < QueueLimit
        /\ frames' = Append(frames, [id |-> serving.id, r |-> serving.pos])
        /\ serving' = [serving EXCEPT !.pos = @ + 1]
        /\ UNCHANGED <<head, next, follow, requests, held>>
\* A new follow resumes after the revisions the relay holds; a pruned cache can make it start earlier.
Refollow == /\ follow < MaxFollows /\ Len(requests) < QueueLimit
            /\ \E from \in 1..(next + IF Bug = "ahead" THEN 1 ELSE 0) :
                  /\ next' = from
                  /\ requests' = Append(requests, [id |-> follow + 1, from |-> from])
            /\ follow' = follow + 1
            /\ UNCHANGED <<head, serving, frames, held>>
Upstream == /\ requests # <<>> /\ serving' = [id |-> Head(requests).id, pos |-> Head(requests).from]
            /\ requests' = Tail(requests)
            /\ UNCHANGED <<head, next, follow, frames, held>>
\* A delivery of the current follow applies; the receiver checks it starts at next (NoFalseGap). Others are dropped.
Receive == /\ frames # <<>> /\ frames' = Tail(frames)
           /\ IF Head(frames).id = follow \/ Bug = "untagged"
              THEN /\ next' = Head(frames).r + 1 /\ held' = held \cup {Head(frames).r}
              ELSE UNCHANGED <<next, held>>
           /\ UNCHANGED <<head, follow, requests, serving>>
\* A new session loses everything in flight and follows again from next.
Reconnect == /\ frames' = <<>> /\ requests' = <<>> /\ serving' = [id |-> follow, pos |-> next]
             /\ UNCHANGED <<head, next, follow, held>>
Next == Commit \/ Send \/ Refollow \/ Upstream \/ Receive \/ Reconnect
Spec == Init /\ [][Next]_vars
TypeOK == /\ head \in 0..MaxRevision /\ next \in 1..(MaxRevision + 2) /\ follow \in 1..MaxFollows
          /\ Len(frames) <= QueueLimit /\ Len(requests) <= QueueLimit /\ held \subseteq 1..MaxRevision
\* Every delivery the relay applies continues its follow exactly, so the gap check never fires falsely.
NoFalseGap == (frames # <<>> /\ (Head(frames).id = follow \/ Bug = "untagged")) => Head(frames).r = next
\* The relay holds everything before the revision it expects.
HeldBeforeNext == 1..(next - 1) \subseteq held
=============================================================================
