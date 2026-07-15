---- MODULE EPaxosCompactionFencing ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite model of compaction and admission fencing.  The positive *)
(* execution intentionally stages both lane folds, deferred late-message   *)
(* handling, incarnation pinning before stale arrival, and config closure  *)
(* before non-control arrival.                                             *)
(***************************************************************************)

CONSTANTS MutateFoldWithoutDurable, MutateFoldNoncontiguous,
          MutateDuplicateApplyAfterLoad, MutateAcceptStaleIncarnation,
          MutateAcceptWhileFenced, MutatePayloadDropRewritesTuple

Lanes == {"lane1", "lane2"}
Instances == 1..2
InitialDecision == <<"A", 1>>
InitialChecksum == "checksum-A-1"

ASSUME
    /\ MutateFoldWithoutDurable \in BOOLEAN
    /\ MutateFoldNoncontiguous \in BOOLEAN
    /\ MutateDuplicateApplyAfterLoad \in BOOLEAN
    /\ MutateAcceptStaleIncarnation \in BOOLEAN
    /\ MutateAcceptWhileFenced \in BOOLEAN
    /\ MutatePayloadDropRewritesTuple \in BOOLEAN

VARIABLE state

vars == state

EmptyLaneNat == [lane \in Lanes |-> 0]
EmptyLaneSet == [lane \in Lanes |-> {}]

TypeOK ==
    /\ state.stage \in 0..10
    /\ state.folded \in [Lanes -> 0..2]
    /\ state.durable \in [Lanes -> SUBSET Instances]
    /\ state.resident \in [Lanes -> SUBSET Instances]
    /\ state.decision \in {InitialDecision, <<"B", 1>>}
    /\ state.durableDecision = InitialDecision
    /\ state.loadedDecision \in {InitialDecision, <<"B", 1>>}
    /\ state.checksum \in {InitialChecksum, "checksum-B-1"}
    /\ state.applied \in 0..2
    /\ state.pinnedIncarnation \in 1..2
    /\ state.arrivalIncarnation \in 0..2
    /\ state.lateDeferred \in BOOLEAN
    /\ state.loaded \in BOOLEAN
    /\ state.payloadDropped \in BOOLEAN
    /\ state.staleMessageSeen \in BOOLEAN
    /\ state.staleAccepted \in BOOLEAN
    /\ state.closed \in BOOLEAN
    /\ state.nonControlSeen \in BOOLEAN
    /\ state.closedAccepted \in BOOLEAN

Init ==
    state =
        [stage |-> 0,
         folded |-> EmptyLaneNat,
         durable |-> EmptyLaneSet,
         resident |-> [lane \in Lanes |-> {1}],
         decision |-> InitialDecision,
         durableDecision |-> InitialDecision,
         loadedDecision |-> InitialDecision,
         checksum |-> InitialChecksum,
         applied |-> 0,
         lateDeferred |-> FALSE,
         loaded |-> FALSE,
         payloadDropped |-> FALSE,
         pinnedIncarnation |-> 1,
         arrivalIncarnation |-> 0,
         staleMessageSeen |-> FALSE,
         staleAccepted |-> FALSE,
         closed |-> FALSE,
         nonControlSeen |-> FALSE,
         closedAccepted |-> FALSE]

FoldLane1 ==
    /\ state.stage = 0
    /\ state' =
        [state EXCEPT
            !.stage = 1,
            !.folded["lane1"] =
                IF MutateFoldNoncontiguous THEN 2 ELSE 1,
            !.durable["lane1"] =
                IF MutateFoldWithoutDurable THEN {}
                ELSE IF MutateFoldNoncontiguous THEN {1, 2} ELSE {1},
            !.resident["lane1"] =
                IF MutateFoldNoncontiguous THEN {2} ELSE {}]

FoldLane2 ==
    /\ state.stage = 1
    /\ state' =
        [state EXCEPT
            !.stage = 2,
            !.folded["lane2"] = 1,
            !.durable["lane2"] = {1},
            !.resident["lane2"] = {}]

DeferLateMessage ==
    /\ state.stage = 2
    /\ state.folded["lane2"] = 1
    /\ 1 \in state.durable["lane2"]
    /\ 1 \notin state.resident["lane2"]
    /\ state' =
        [state EXCEPT
            !.stage = 3,
            !.lateDeferred = TRUE]

LoadDurableRecord ==
    /\ state.stage = 3
    /\ state.lateDeferred
    /\ ~state.loaded
    /\ state' =
        [state EXCEPT
            !.stage = 4,
            !.loaded = TRUE,
            !.loadedDecision = @]

ReplayLoadedRecord ==
    /\ state.stage = 4
    /\ state.lateDeferred
    /\ state.loaded
    /\ state.applied = 0
    /\ state' =
        [state EXCEPT
            !.stage = 5,
            !.applied = IF MutateDuplicateApplyAfterLoad THEN 2 ELSE 1]

DropPayload ==
    /\ state.stage = 5
    /\ state' =
        [state EXCEPT
            !.stage = 6,
            !.payloadDropped = TRUE,
            !.decision =
                IF MutatePayloadDropRewritesTuple THEN <<"B", 1>> ELSE @,
            !.checksum =
                IF MutatePayloadDropRewritesTuple THEN "checksum-B-1" ELSE @]

AdvanceIncarnation ==
    /\ state.stage = 6
    /\ state.pinnedIncarnation = 1
    /\ state' =
        [state EXCEPT
            !.stage = 7,
            !.pinnedIncarnation = 2]

RejectStaleIncarnation ==
    /\ state.stage = 7
    /\ state.pinnedIncarnation = 2
    /\ state.arrivalIncarnation = 0
    /\ ~state.staleMessageSeen
    /\ state' =
        [state EXCEPT
            !.stage = 8,
            !.arrivalIncarnation = 1,
            !.staleMessageSeen = TRUE,
            !.staleAccepted = MutateAcceptStaleIncarnation]

CloseConfig ==
    /\ state.stage = 8
    /\ ~state.closed
    /\ state' =
        [state EXCEPT
            !.stage = 9,
            !.closed = TRUE]

RejectClosedConfigTraffic ==
    /\ state.stage = 9
    /\ state.closed
    /\ ~state.nonControlSeen
    /\ state' =
        [state EXCEPT
            !.stage = 10,
            !.nonControlSeen = TRUE,
            !.closedAccepted = MutateAcceptWhileFenced]

Next ==
    \/ FoldLane1
    \/ FoldLane2
    \/ DeferLateMessage
    \/ LoadDurableRecord
    \/ ReplayLoadedRecord
    \/ DropPayload
    \/ AdvanceIncarnation
    \/ RejectStaleIncarnation
    \/ CloseConfig
    \/ RejectClosedConfigTraffic

Spec == Init /\ [][Next]_vars

FoldRequiresDurableExecuted ==
    /\ TypeOK
    /\ \A lane \in Lanes:
        \A instance \in 1..state.folded[lane]: instance \in state.durable[lane]

FoldedAbsentResident ==
    /\ TypeOK
    /\ \A lane \in Lanes:
        \A instance \in state.resident[lane]: instance > state.folded[lane]

LateMessageRematerializes ==
    /\ TypeOK
    /\ state.loaded =>
        /\ state.lateDeferred
        /\ 1 \in state.durable["lane2"]
        /\ state.loadedDecision = state.durableDecision
        /\ state.applied \in {0, 1}

PayloadDropPreservesAuthority ==
    /\ TypeOK
    /\ state.payloadDropped =>
        /\ state.checksum = InitialChecksum
        /\ state.decision = state.durableDecision

StaleIncarnationFenced ==
    /\ TypeOK
    /\ state.staleMessageSeen =>
        /\ state.pinnedIncarnation = 2
        /\ state.arrivalIncarnation = 1
        /\ state.arrivalIncarnation < state.pinnedIncarnation
        /\ ~state.staleAccepted

ClosedConfigFenced ==
    /\ TypeOK
    /\ state.closed /\ state.nonControlSeen => ~state.closedAccepted

====
