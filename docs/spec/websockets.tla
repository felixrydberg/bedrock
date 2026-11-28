---------------------------- MODULE websockets ----------------------------
EXTENDS Naturals, Sequences, FiniteSets
\* #123 / S-15. Quorum commits, recovery and single-serving-holder leases
\* compose with volume.tla. Epoch is the existing actor Writer.epoch, not a
\* membership epoch. An emitted callback may arrive after its holder stops.
CONSTANTS Gateways, Connections, Events, MaxEpoch, Capacity, Bug
Owner(c) == IF c = "c2" THEN "g2" ELSE "g1"
Birth(c) == IF c = "c3" THEN 1 ELSE 0
Effect == [event : Events, socket : Connections]
Pair(e, c) == [event |-> e, socket |-> c]
VARIABLE s
vars == <<s>>
Init == s = [boot |-> [g \in Gateways |-> 0], allocated |-> Connections \ {"c3"},
    live |-> Connections \ {"c3"}, accepted |-> {}, attachments |-> Connections \ {"c3"},
    roster |-> {}, retired |-> {}, view |-> {}, actor |-> "serving", epoch |-> 1,
    scope |-> 0, serial |-> 0, phase |-> [e \in Events |-> "new"], at |-> [e \in Events |-> 0],
    number |-> [e \in Events |-> 0],
    committed |-> {}, effects |-> {}, inflight |-> {},
    attempts |-> [p \in Effect |-> 0], inputs |-> [e \in Events |-> 0],
    unknownInputs |-> {}, unknownOutputs |-> {}, highest |-> [c \in Connections |-> 0],
    queues |-> [c \in Connections |-> <<>>], delivered |-> [c \in Connections |-> <<>>],
    gated |-> TRUE, bound |-> TRUE, retained |-> TRUE, isolated |-> TRUE, validInput |-> TRUE, admitted |-> TRUE]

\* A roster write abstracts one acknowledged, recovered actor-volume commit.
Register(c) == /\ s.actor = "serving" /\ s.scope = 0
               /\ c \in s.live /\ c \notin s.roster /\ c \notin s.retired
               /\ s' = [s EXCEPT !.roster = @ \cup {c}, !.view = @ \cup {c}]
Accept(c) == /\ c \in s.live /\ c \notin s.accepted /\ c \notin s.retired
             /\ (c \in s.roster \/ Bug = "accept-before-commit")
             /\ s' = [s EXCEPT !.accepted = @ \cup {c}]
\* A delivered close retires the ID. A lost close retains the candidate.
Close(c, lost) == /\ c \in s.live /\ s.scope = 0
                 /\ (lost \/ s.actor = "serving")
                 /\ s' = [s EXCEPT !.live = @ \ {c}, !.accepted = @ \ {c},
                      !.attachments = @ \ {c}, !.queues[c] = <<>>,
                      !.roster = IF lost THEN @ ELSE @ \ {c},
                      !.view = IF lost THEN @ ELSE @ \ {c},
                      !.retired = IF lost THEN @ ELSE @ \cup {c}]
\* Retire even when the owner is unreachable; later input must be refused.
Retire(c) == /\ s.actor = "serving" /\ s.scope = 0 /\ c \in s.roster
             /\ s' = [s EXCEPT !.roster = @ \ {c}, !.view = @ \ {c}, !.retired = @ \cup {c}]
Sleep == /\ s.actor = "serving" /\ s.scope = 0
         /\ s' = [s EXCEPT !.actor = "sleeping", !.view = {}]
Wake == /\ s.actor \in {"sleeping", "fenced"} /\ s.scope = 0 /\ s.epoch < MaxEpoch
        /\ s' = [s EXCEPT !.actor = "serving", !.epoch = @ + 1, !.serial = 0,
             !.view = IF Bug = "forget-roster" THEN {} ELSE s.roster]
Fence == /\ s.actor = "serving"
         /\ s' = [s EXCEPT !.actor = "fenced", !.view = {}]
Restart(g) == /\ s.boot[g] = 0
              /\ s' = [s EXCEPT !.boot[g] = 1,
                   !.live = {c \in @ : Owner(c) # g},
                   !.accepted = {c \in @ : Owner(c) # g},
                   !.attachments = IF Bug = "keep-attachment" THEN @ ELSE {c \in @ : Owner(c) # g},
                   !.queues = [c \in Connections |-> IF Owner(c) = g THEN <<>> ELSE s.queues[c]]]
\* Replacement gets a fresh lifetime ID, not a new membership epoch.
Replacement == /\ "c3" \in Connections /\ s.boot["g1"] = 1 /\ "c3" \notin s.allocated
               /\ LET c == IF Bug = "reuse-id" THEN "c1" ELSE "c3"
                  IN s' = [s EXCEPT !.allocated = @ \cup {"c3"}, !.live = @ \cup {c}, !.attachments = @ \cup {c}]
Timeout(c) == /\ c \in s.roster
              /\ s' = [s EXCEPT !.roster = IF Bug = "timeout-prune" THEN @ \ {c} ELSE @,
                   !.view = IF Bug = "timeout-prune" THEN @ \ {c} ELSE @,
                   !.retained = @ /\ Bug # "timeout-prune"]
\* Only a certified owner's NotFound proves this exact ID absent.
NotFound(c) == /\ c \in s.roster /\ c \notin s.live /\ s.scope = 0
               /\ s.actor = "serving"
               /\ s' = [s EXCEPT !.roster = @ \ {c}, !.view = @ \ {c}, !.retired = @ \cup {c}]

\* One input followed by an alarm, sharing the same instance and input gate.
Begin(e) == /\ s.actor = "serving" /\ s.scope = 0 /\ s.phase[e] = "new"
            /\ IF e = 1 THEN /\ "c1" \in s.accepted
                               /\ ("c1" \in s.roster \/ Bug = "retired-input")
                           ELSE s.phase[1] \in {"ok", "failed", "cancelled"}
            /\ s' = [s EXCEPT !.scope = e, !.phase[e] = "running", !.at[e] = s.epoch,
                 !.number[e] = s.serial + 1, !.serial = @ + 1,
                 !.inputs[e] = @ + 1, !.validInput = @ /\ (e # 1 \/ "c1" \in s.roster)]
Stage(e, c) == /\ s.scope = e /\ s.phase[e] = "running" /\ c \in s.view
               /\ Pair(e, c) \notin s.effects
               /\ s' = [s EXCEPT !.effects = @ \cup {Pair(e, c)}]
Commit(e) == /\ s.actor = "serving" /\ s.scope = e /\ s.phase[e] = "running"
             /\ e \notin s.committed /\ s' = [s EXCEPT !.committed = @ \cup {e}]
Finish(e, result) == /\ s.scope = e /\ s.phase[e] = "running"
                     /\ s' = [s EXCEPT !.phase[e] = result]
End(e) == /\ s.scope = e /\ s.phase[e] \in {"ok", "failed", "cancelled"}
          /\ s' = [s EXCEPT !.scope = 0,
               !.effects = IF Bug = "old-effects" THEN @ ELSE {p \in @ : p.event # e}]
Eligible(e, c) == /\ Pair(e, c) \in s.effects /\ s.attempts[Pair(e, c)] = 0
                  /\ s.scope = e /\ s.phase[e] = "ok" /\ e \in s.committed /\ s.actor = "serving"
\* Gate emission; an already emitted packet may arrive after lease loss.
Emit(e, c) == /\ Pair(e, c) \in s.effects /\ s.attempts[Pair(e, c)] = 0
              /\ Eligible(e, c) \/ Bug = "ungated"
                 \/ (Bug = "old-effects" /\ s.scope # e /\ s.phase[e] = "ok" /\ e \in s.committed /\ s.actor = "serving")
              /\ Bug # "slow-stalls" \/ \A other \in Connections : Len(s.queues[other]) < Capacity
              /\ s' = [s EXCEPT !.attempts[Pair(e, c)] = @ + 1,
                   !.inflight = @ \cup {Pair(e, c)}, !.gated = @ /\ Eligible(e, c)]
\* Logical event identity is distinct from the per-awake number: wake resets
\* the latter, so an epoch-2/event-1 callback must outrank epoch-1/event-1.
Stamp(p) == s.at[p.event] * 3 + s.number[p.event]
LastNumber(c) == IF s.highest[c] = 0 THEN 0 ELSE IF s.highest[c] \in {4, 7} THEN 1 ELSE 2
\* The (Writer.epoch,event) tuple is volatile ordering, never a replay log.
Receive(p, g, claim) ==
    /\ p \in s.inflight
    /\ LET c == p.socket
           correctOwner == g = Owner(c) /\ claim = "target" /\ Birth(c) = s.boot[g]
           newer == Stamp(p) > s.highest[c]
           accepts == /\ c \in s.accepted
                      /\ (Bug = "unchecked-owner" \/ correctOwner)
                      /\ (IF Bug = "event-only" THEN s.number[p.event] > LastNumber(c)
                          ELSE newer \/ Bug = "stale-callback")
           full == accepts /\ Len(s.queues[c]) = Capacity /\ Bug # "unbounded"
           close == IF full THEN (IF Bug = "overflow-all" THEN s.live ELSE {c}) ELSE {}
       IN s' = [s EXCEPT !.inflight = @ \ {p},
            !.queues = [other \in Connections |-> IF other \in close THEN <<>>
                         ELSE IF other = c /\ accepts THEN Append(s.queues[other], p.event) ELSE s.queues[other]],
            !.highest[c] = IF accepts THEN Stamp(p) ELSE @,
            !.live = @ \ close, !.accepted = @ \ close, !.attachments = @ \ close,
            !.bound = @ /\ (~accepts \/ (g = Owner(c) /\ claim = "target" /\ Birth(c) = s.boot[g])),
            !.admitted = @ /\ (~(c \in s.accepted /\ correctOwner /\ newer) \/ accepts),
            !.isolated = @ /\ close \subseteq {c}]
Lost(p) == /\ p \in s.inflight /\ s' = [s EXCEPT !.inflight = @ \ {p}]
UnknownInput == /\ s.inputs[1] = 1 /\ 1 \notin s.unknownInputs
                /\ s' = [s EXCEPT !.unknownInputs = @ \cup {1}, !.live = @ \ {"c1"},
                     !.accepted = @ \ {"c1"}, !.attachments = @ \ {"c1"}, !.queues["c1"] = <<>>]
ReplayInput == /\ Bug = "replay-input" /\ 1 \in s.unknownInputs /\ s.inputs[1] = 1
               /\ s.scope = 0 /\ s.actor = "serving"
               /\ s' = [s EXCEPT !.inputs[1] = @ + 1, !.scope = 1, !.phase[1] = "running", !.at[1] = s.epoch]
UnknownOutput(p) == /\ s.attempts[p] = 1 /\ p \notin s.unknownOutputs
                    /\ s' = [s EXCEPT !.unknownOutputs = @ \cup {p}]
ReplayOutput(p) == /\ Bug = "replay-output" /\ p \in s.unknownOutputs /\ s.attempts[p] = 1
                   /\ s' = [s EXCEPT !.attempts[p] = @ + 1, !.inflight = @ \cup {p}]
Deliver(c) == /\ Len(s.queues[c]) > 0
              /\ s' = [s EXCEPT !.delivered[c] = Append(@, Head(s.queues[c])), !.queues[c] = Tail(@)]
Next == \/ \E c \in Connections : Register(c) \/ Accept(c) \/ Timeout(c) \/ NotFound(c) \/ Retire(c) \/ Deliver(c)
                                      \/ (\E lost \in BOOLEAN : Close(c, lost))
        \/ Sleep \/ Wake \/ Fence \/ Replacement \/ UnknownInput \/ ReplayInput
        \/ \E g \in Gateways : Restart(g)
        \/ \E e \in Events : Begin(e) \/ Commit(e) \/ End(e)
             \/ (\E result \in {"ok", "failed", "cancelled"} : Finish(e, result))
             \/ (\E c \in Connections : Stage(e, c) \/ Emit(e, c))
        \/ \E p \in Effect : Lost(p) \/ UnknownOutput(p) \/ ReplayOutput(p)
             \/ (\E g \in Gateways, claim \in {"target", "other"} : Receive(p, g, claim))
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.boot \in [Gateways -> 0..1] /\ s.allocated \subseteq Connections
          /\ s.live \subseteq s.allocated /\ s.accepted \subseteq s.live /\ s.attachments \subseteq Connections
          /\ s.roster \subseteq Connections /\ s.retired \subseteq Connections /\ s.view \subseteq Connections
          /\ s.actor \in {"serving", "sleeping", "fenced"} /\ s.epoch \in 1..MaxEpoch
          /\ s.scope \in {0} \cup Events /\ s.serial \in 0..(Cardinality(Events) + 1)
          /\ s.phase \in [Events -> {"new", "running", "ok", "failed", "cancelled"}]
          /\ s.at \in [Events -> 0..MaxEpoch] /\ s.number \in [Events -> 0..(Cardinality(Events) + 1)]
          /\ s.committed \subseteq Events /\ s.effects \subseteq Effect /\ s.inflight \subseteq Effect
          /\ s.attempts \in [Effect -> 0..2] /\ s.inputs \in [Events -> 0..2]
          /\ s.unknownInputs \subseteq Events /\ s.unknownOutputs \subseteq Effect
          /\ s.highest \in [Connections -> 0..(MaxEpoch * 3 + 2)]
          /\ s.queues \in [Connections -> Seq(Events)] /\ s.delivered \in [Connections -> Seq(Events)]
          /\ s.gated \in BOOLEAN /\ s.bound \in BOOLEAN /\ s.retained \in BOOLEAN /\ s.isolated \in BOOLEAN /\ s.validInput \in BOOLEAN /\ s.admitted \in BOOLEAN
AcceptedRecorded == s.accepted \ s.retired \subseteq s.roster
RecoveredRoster == s.actor = "serving" => s.view = s.roster
AttachmentLifetime == s.attachments \subseteq s.live
IncarnationSafe == \A c \in s.live : Birth(c) = s.boot[Owner(c)]
OutputGated == s.gated
OwnerBound == s.bound
UnknownRetained == s.retained
RetiredInputRefused == s.validInput
EpochAdmission == s.admitted
NoReplay == /\ \A e \in Events : s.inputs[e] <= 1
            /\ \A p \in Effect : s.attempts[p] <= 1
QueueBound == \A c \in Connections : Len(s.queues[c]) <= Capacity
OverflowIsolated == s.isolated
OutputOrdered == \A c \in Connections : \A i, j \in 1..Len(s.delivered[c]) : i < j => s.delivered[c][i] < s.delivered[c][j]
\* Enabledness only: a full unrelated socket cannot block this event's emission.
HealthyNotStalled == \A e \in Events, c \in Connections :
    (Eligible(e, c) /\ c \in s.accepted /\ Len(s.queues[c]) < Capacity)
       => ENABLED Emit(e, c)
=============================================================================
