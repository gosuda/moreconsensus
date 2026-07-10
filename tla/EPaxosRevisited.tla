---- MODULE EPaxosRevisited ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Finite EPaxos Revisited model for two independently checked contracts.   *)
(* The chain cases retain the strict known-after pruning regressions.  The  *)
(* TOQ half models a concrete zero-attribute request, receiver processing,   *)
(* durable reply creation, delivery validation, owner assignment, carried   *)
(* evidence, and the optimized three-replica fast or slow decision.  This    *)
(* finite scope has no crashes, restarts, reconfiguration, clock             *)
(* synchronization, or delay measurement.  Its tick orders modeled events;  *)
(* it is not the legacy logical-time fast-wait optimization.                 *)
(***************************************************************************)

CONSTANTS A, B, C, D, ModelScenario,
          OmitEvidenceCoverage, RemoteOnlySlowTuple

ASSUME Cardinality({A, B, C, D}) = 4

VARIABLES chainCase, chainExecuted, tick, timingCase, toqRequest,
          receiverDurable, toqReplies, ownerPending, ownerAssignedAt,
          ownerTuple, candidateTuple, commitEvidence, prefixCommitHistory,
          replyDeliveryHistory, phase, commitReason, decisionTuple

Instances == {A, B, C, D}
Replicas == {1, 2, 3}
Owner == 1
Followers == Replicas \ {Owner}
\* Explicit TOQ at N = 3 decides with the owner and one qualifying remote.
SlowQuorum == 2
OptimizedFastQuorum == 2
ProcessAt == 1
DelayedProcessAt == 2
TickLimit == 3
Unprocessed == TickLimit + 1
CurrentConfig == "cfg-current"
OtherConfig == "cfg-other"
Configs == {CurrentConfig, OtherConfig}

ChainCases == {
    "safe-chain",
    "uncommitted-safe",
    "lower-seq",
    "equal-seq",
    "missing-reverse",
    "unknown-dependency"
}
SafeChainCases == {"safe-chain", "uncommitted-safe"}
UnsafeChainCases == ChainCases \ SafeChainCases
TimingCases == {
    "remote-fast",
    "delayed-owner-superset",
    "delayed-owner-incomparable",
    "delayed-receiver",
    "duplicate-reply",
    "stale-process-at",
    "config-mismatch",
    "malformed-reply",
    "covering-superset",
    "missing-prefix"
}
ModelScenarios == TimingCases \cup {"all"}
ASSUME ModelScenario \in ModelScenarios
ASSUME OmitEvidenceCoverage \in BOOLEAN
ASSUME RemoteOnlySlowTuple \in BOOLEAN
SelectedTimingCases ==
    IF ModelScenario = "all" THEN TimingCases ELSE {ModelScenario}
Statuses == {"none", "preaccepted", "accepted", "committed"}
Phases == {"preaccept", "accepted", "committed"}
CommitReasons == {"none", "remote-fast", "slow-accept"}
ReplyKinds == {"valid", "duplicate", "stale-process-at", "config-mismatch", "malformed"}

TupleSet == [seq : 0..TickLimit, deps : SUBSET Instances]
ZeroTuple == [seq |-> 0, deps |-> {}]
RequestRecords ==
    [present : BOOLEAN,
     inst : Instances,
     conf : Configs,
     processAt : 0..TickLimit,
     toq : BOOLEAN,
     tuple : TupleSet]
ReplyRecords ==
    [present : BOOLEAN,
     from : Followers,
     inst : Instances,
     conf : Configs,
     processAt : 0..TickLimit,
     tuple : TupleSet,
     evidence : SUBSET Instances]
DurableRecords ==
    [processed : BOOLEAN,
     persisted : BOOLEAN,
     processedAt : 0..Unprocessed,
     tuple : TupleSet,
     conf : Configs,
     requestProcessAt : 0..TickLimit,
     evidence : SUBSET Instances]
HistoryFacts ==
    {<<r, d, c, p>> :
        r \in Followers, d \in Instances, c \in Configs, p \in 0..TickLimit}
DeliveryFacts == {<<k, r>> : k \in ReplyKinds, r \in Followers}

EmptyRequest ==
    [present |-> FALSE,
     inst |-> A,
     conf |-> CurrentConfig,
     processAt |-> ProcessAt,
     toq |-> TRUE,
     tuple |-> ZeroTuple]
QueuedRequest ==
    [present |-> TRUE,
     inst |-> A,
     conf |-> CurrentConfig,
     processAt |-> ProcessAt,
     toq |-> TRUE,
     tuple |-> ZeroTuple]
EmptyDurable ==
    [processed |-> FALSE,
     persisted |-> FALSE,
     processedAt |-> Unprocessed,
     tuple |-> ZeroTuple,
     conf |-> CurrentConfig,
     requestProcessAt |-> ProcessAt,
     evidence |-> {}]
EmptyReply(r) ==
    [present |-> FALSE,
     from |-> r,
     inst |-> A,
     conf |-> CurrentConfig,
     processAt |-> ProcessAt,
     tuple |-> ZeroTuple,
     evidence |-> {}]

Vars ==
    <<chainCase, chainExecuted, tick, timingCase, toqRequest,
      receiverDurable, toqReplies, ownerPending, ownerAssignedAt,
      ownerTuple, candidateTuple, commitEvidence, prefixCommitHistory,
      replyDeliveryHistory, phase, commitReason, decisionTuple>>

StatusRank(s) ==
    CASE s = "none" -> 0
      [] s = "preaccepted" -> 1
      [] s = "accepted" -> 2
      [] s = "committed" -> 3

Seq(i) ==
    CASE i = A -> IF chainCase = "lower-seq" THEN 2 ELSE 1
      [] i = B -> IF chainCase = "lower-seq" THEN 1
                    ELSE IF chainCase = "equal-seq" THEN 1 ELSE 2
      [] i = C -> 3
      [] i = D -> 0

Deps(i) ==
    CASE chainCase = "safe-chain" ->
            CASE i = A -> {B}
              [] i = B -> {A, C}
              [] i = C -> {D}
              [] OTHER -> {}
      [] chainCase = "uncommitted-safe" ->
            CASE i = B -> {A}
              [] OTHER -> {}
      [] chainCase = "missing-reverse" ->
            {}
      [] chainCase = "unknown-dependency" ->
            CASE i = A -> {B}
              [] OTHER -> {}
      [] OTHER ->
            CASE i = B -> {A}
              [] OTHER -> {}

RecStatus(i) ==
    CASE i = A -> "committed"
      [] i = B -> IF chainCase = "safe-chain" THEN "committed"
                    ELSE IF chainCase = "unknown-dependency" THEN "none"
                    ELSE "accepted"
      [] i = C -> IF chainCase = "safe-chain" THEN "committed" ELSE "none"
      [] OTHER -> "none"

KnownAfter(base, other, minStatus) ==
    /\ RecStatus(other) \in Statuses
    /\ StatusRank(RecStatus(other)) >= StatusRank(minStatus)
    /\ Seq(other) > Seq(base)
    /\ base \in Deps(other)

UnresolvedKnownConflict(base) ==
    \E other \in Instances \ {base} :
        /\ other \notin chainExecuted
        /\ RecStatus(other) \in {"preaccepted", "accepted"}
        /\ ~KnownAfter(base, other, "preaccepted")

Ready(base) ==
    /\ RecStatus(base) = "committed"
    /\ base \notin chainExecuted
    /\ ~UnresolvedKnownConflict(base)
    /\ \A dep \in Deps(base) :
        \/ dep \in chainExecuted
        \/ KnownAfter(base, dep, "committed")

OwnerProcessedTuple ==
    CASE timingCase = "delayed-owner-superset" ->
            [seq |-> 3, deps |-> {B, C, D}]
      [] timingCase = "delayed-owner-incomparable" ->
            [seq |-> 2, deps |-> {B, D}]
      [] OTHER ->
            [seq |-> 1, deps |-> {B}]

ReceiverProcessedTuple(r) ==
    IF timingCase = "covering-superset"
    THEN IF r = 2
         THEN [seq |-> 2, deps |-> {B, C}]
         ELSE [seq |-> 3, deps |-> {B, C, D}]
    ELSE IF timingCase = "delayed-owner-incomparable"
    THEN IF r = 2
         THEN [seq |-> 3, deps |-> {B, C}]
         ELSE [seq |-> 1, deps |-> {B, C, D}]
    ELSE IF r = 2
         THEN [seq |-> 1, deps |-> {B}]
         ELSE [seq |-> 2, deps |-> {B, C}]

InitialEvidence(r) ==
    IF timingCase = "missing-prefix"
    THEN {}
    ELSE ReceiverProcessedTuple(r).deps

PrefixHistoryForScenario ==
    UNION {{<<r, dep, CurrentConfig, ProcessAt>> :
                dep \in InitialEvidence(r)} : r \in Followers}

HistoricalEvidenceFor(r) ==
    {dep \in Instances :
        <<r, dep, CurrentConfig, ProcessAt>> \in prefixCommitHistory}

ReceiverEarliestProcess(r) ==
    IF r = 3 /\ timingCase = "delayed-receiver"
    THEN DelayedProcessAt
    ELSE ProcessAt

OwnerEarliestProcess ==
    IF timingCase \in
        {"delayed-owner-superset", "delayed-owner-incomparable"}
    THEN DelayedProcessAt
    ELSE ProcessAt

MaxNat(a, b) == IF a >= b THEN a ELSE b

JoinTuple(left, right) ==
    [seq |-> MaxNat(left.seq, right.seq),
     deps |-> left.deps \cup right.deps]

TupleLeq(left, right) ==
    /\ left.seq <= right.seq
    /\ left.deps \subseteq right.deps

ReplyTupleOrZero(replies, r) ==
    IF replies[r].present THEN replies[r].tuple ELSE ZeroTuple

CandidateFor(owner, replies) ==
    JoinTuple(
        JoinTuple(owner, ReplyTupleOrZero(replies, 2)),
        ReplyTupleOrZero(replies, 3))

AcceptedSenders == {r \in Followers : toqReplies[r].present}
TotalTOQQuorumSize == Cardinality(AcceptedSenders) + 1
CarriedEvidence == UNION {commitEvidence[r] : r \in AcceptedSenders}
CarriedEvidenceCoversCandidate == candidateTuple.deps \subseteq CarriedEvidence

FastTupleWitness(r) ==
    /\ AcceptedSenders = {r}
    /\ TupleLeq(ownerTuple, toqReplies[r].tuple)
    /\ candidateTuple = JoinTuple(ownerTuple, toqReplies[r].tuple)

SafeOptimizedFastEligible ==
    /\ TotalTOQQuorumSize = OptimizedFastQuorum
    /\ \E r \in Followers :
        /\ FastTupleWitness(r)
        /\ candidateTuple.deps \subseteq commitEvidence[r]

ModeledOptimizedFastEligible ==
    /\ TotalTOQQuorumSize = OptimizedFastQuorum
    /\ \E r \in Followers :
        /\ FastTupleWitness(r)
        /\ \/ candidateTuple.deps \subseteq commitEvidence[r]
           \/ OmitEvidenceCoverage

AcceptedRemote ==
    CHOOSE r \in Followers : AcceptedSenders = {r}

SlowDecisionTuple ==
    IF RemoteOnlySlowTuple
    THEN toqReplies[AcceptedRemote].tuple
    ELSE candidateTuple

DurableReply(r) ==
    [present |-> TRUE,
     from |-> r,
     inst |-> toqRequest.inst,
     conf |-> receiverDurable[r].conf,
     processAt |-> receiverDurable[r].requestProcessAt,
     tuple |-> receiverDurable[r].tuple,
     evidence |-> receiverDurable[r].evidence]

AttemptReply(r, kind) ==
    CASE kind \in {"valid", "duplicate"} ->
            DurableReply(r)
      [] kind = "stale-process-at" ->
            [present |-> TRUE,
             from |-> r,
             inst |-> toqRequest.inst,
             conf |-> receiverDurable[r].conf,
             processAt |-> ProcessAt + 1,
             tuple |-> receiverDurable[r].tuple,
             evidence |-> receiverDurable[r].evidence]
      [] kind = "config-mismatch" ->
            [present |-> TRUE,
             from |-> r,
             inst |-> toqRequest.inst,
             conf |-> OtherConfig,
             processAt |-> receiverDurable[r].requestProcessAt,
             tuple |-> receiverDurable[r].tuple,
             evidence |-> receiverDurable[r].evidence]
      [] OTHER ->
            [present |-> TRUE,
             from |-> r,
             inst |-> toqRequest.inst,
             conf |-> receiverDurable[r].conf,
             processAt |-> receiverDurable[r].requestProcessAt,
             tuple |-> ZeroTuple,
             evidence |-> receiverDurable[r].evidence]

ReplyMatchesDurableRequest(r, reply) ==
    /\ receiverDurable[r].persisted
    /\ reply = DurableReply(r)
    /\ reply.inst = toqRequest.inst
    /\ reply.conf = toqRequest.conf
    /\ reply.processAt = toqRequest.processAt

RequiredRejectedKind ==
    CASE timingCase = "stale-process-at" -> "stale-process-at"
      [] timingCase = "config-mismatch" -> "config-mismatch"
      [] timingCase = "malformed-reply" -> "malformed"
      [] OTHER -> "none"

CanDeliverReplyKind(r, kind) ==
    /\ r \in Followers
    /\ kind \in ReplyKinds
    /\ receiverDurable[r].persisted
    /\ CASE kind = "valid" ->
            /\ AcceptedSenders = {}
            /\ ~toqReplies[r].present
            /\ phase = "preaccept"
            /\ IF r = 2 /\ RequiredRejectedKind # "none"
               THEN <<RequiredRejectedKind, r>> \in replyDeliveryHistory
               ELSE TRUE
            /\ IF r = 3 /\ timingCase = "duplicate-reply"
               THEN <<"duplicate", 2>> \in replyDeliveryHistory
               ELSE TRUE
          [] kind = "duplicate" ->
            /\ timingCase = "duplicate-reply"
            /\ r = 2
            /\ toqReplies[r].present
            /\ <<kind, r>> \notin replyDeliveryHistory
          [] kind = "stale-process-at" ->
            /\ timingCase = "stale-process-at"
            /\ r = 2
            /\ <<kind, r>> \notin replyDeliveryHistory
          [] kind = "config-mismatch" ->
            /\ timingCase = "config-mismatch"
            /\ r = 2
            /\ <<kind, r>> \notin replyDeliveryHistory
          [] OTHER ->
            /\ timingCase = "malformed-reply"
            /\ r = 2
            /\ <<kind, r>> \notin replyDeliveryHistory

TypeOK ==
    /\ chainCase \in ChainCases
    /\ chainExecuted \subseteq Instances
    /\ tick \in 0..TickLimit
    /\ timingCase \in TimingCases
    /\ toqRequest \in RequestRecords
    /\ receiverDurable \in [Followers -> DurableRecords]
    /\ toqReplies \in [Followers -> ReplyRecords]
    /\ ownerPending \in BOOLEAN
    /\ ownerAssignedAt \in 0..Unprocessed
    /\ ownerTuple \in TupleSet
    /\ candidateTuple \in TupleSet
    /\ commitEvidence \in [Followers -> SUBSET Instances]
    /\ prefixCommitHistory \subseteq HistoryFacts
    /\ replyDeliveryHistory \subseteq DeliveryFacts
    /\ phase \in Phases
    /\ commitReason \in CommitReasons
    /\ decisionTuple \in TupleSet

Init ==
    /\ chainCase \in ChainCases
    /\ chainExecuted = {}
    /\ tick = 0
    /\ timingCase \in SelectedTimingCases
    /\ toqRequest = EmptyRequest
    /\ receiverDurable = [r \in Followers |-> EmptyDurable]
    /\ toqReplies = [r \in Followers |-> EmptyReply(r)]
    /\ ownerPending = TRUE
    /\ ownerAssignedAt = Unprocessed
    /\ ownerTuple = ZeroTuple
    /\ candidateTuple = ZeroTuple
    /\ commitEvidence = [r \in Followers |-> {}]
    /\ prefixCommitHistory = PrefixHistoryForScenario
    /\ replyDeliveryHistory = {}
    /\ phase = "preaccept"
    /\ commitReason = "none"
    /\ decisionTuple = ZeroTuple

ExecuteA ==
    /\ Ready(A)
    /\ chainExecuted' = chainExecuted \cup {A}
    /\ UNCHANGED <<chainCase, tick, timingCase, toqRequest,
                  receiverDurable, toqReplies, ownerPending, ownerAssignedAt,
                  ownerTuple, candidateTuple, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory, phase, commitReason, decisionTuple>>

Tick ==
    /\ tick < TickLimit
    /\ tick' = tick + 1
    /\ UNCHANGED <<chainCase, chainExecuted, timingCase, toqRequest,
                  receiverDurable, toqReplies, ownerPending, ownerAssignedAt,
                  ownerTuple, candidateTuple, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory, phase, commitReason, decisionTuple>>

QueueTOQPreAccept ==
    /\ ~toqRequest.present
    /\ toqRequest' = QueuedRequest
    /\ UNCHANGED <<chainCase, chainExecuted, tick, timingCase,
                  receiverDurable, toqReplies, ownerPending, ownerAssignedAt,
                  ownerTuple, candidateTuple, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory, phase, commitReason, decisionTuple>>

ProcessTOQAt(r) ==
    /\ r \in Followers
    /\ toqRequest.present
    /\ ~receiverDurable[r].processed
    /\ tick >= toqRequest.processAt
    /\ tick >= ReceiverEarliestProcess(r)
    /\ receiverDurable' =
        [receiverDurable EXCEPT
            ![r] =
                [processed |-> TRUE,
                 persisted |-> FALSE,
                 processedAt |-> tick,
                 tuple |-> ReceiverProcessedTuple(r),
                 conf |-> toqRequest.conf,
                 requestProcessAt |-> toqRequest.processAt,
                 evidence |-> HistoricalEvidenceFor(r)]]
    /\ UNCHANGED <<chainCase, chainExecuted, tick, timingCase, toqRequest,
                  toqReplies, ownerPending, ownerAssignedAt, ownerTuple,
                  candidateTuple, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory, phase, commitReason, decisionTuple>>

PersistTOQReply(r) ==
    /\ r \in Followers
    /\ receiverDurable[r].processed
    /\ ~receiverDurable[r].persisted
    /\ receiverDurable' = [receiverDurable EXCEPT ![r].persisted = TRUE]
    /\ UNCHANGED <<chainCase, chainExecuted, tick, timingCase, toqRequest,
                  toqReplies, ownerPending, ownerAssignedAt, ownerTuple,
                  candidateTuple, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory, phase, commitReason, decisionTuple>>

DeliverTOQReply(r, kind) ==
    /\ CanDeliverReplyKind(r, kind)
    /\ LET attempted == AttemptReply(r, kind)
           accepted ==
                /\ ReplyMatchesDurableRequest(r, attempted)
                /\ ~toqReplies[r].present
           newReplies ==
                IF accepted
                THEN [toqReplies EXCEPT ![r] = attempted]
                ELSE toqReplies
       IN
       /\ toqReplies' = newReplies
       /\ commitEvidence' =
            IF accepted
            THEN [commitEvidence EXCEPT ![r] = attempted.evidence]
            ELSE commitEvidence
       /\ candidateTuple' =
            IF accepted /\ ~ownerPending
            THEN CandidateFor(ownerTuple, newReplies)
            ELSE candidateTuple
       /\ replyDeliveryHistory' =
            replyDeliveryHistory \cup {<<kind, r>>}
    /\ UNCHANGED <<chainCase, chainExecuted, tick, timingCase, toqRequest,
                  receiverDurable, ownerPending, ownerAssignedAt, ownerTuple,
                  prefixCommitHistory, phase, commitReason, decisionTuple>>

AssignOwnerAtProcessAt ==
    /\ toqRequest.present
    /\ ownerPending
    /\ tick >= toqRequest.processAt
    /\ tick >= OwnerEarliestProcess
    /\ ownerPending' = FALSE
    /\ ownerAssignedAt' = tick
    /\ ownerTuple' = OwnerProcessedTuple
    /\ candidateTuple' = CandidateFor(OwnerProcessedTuple, toqReplies)
    /\ UNCHANGED <<chainCase, chainExecuted, tick, timingCase, toqRequest,
                  receiverDurable, toqReplies, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory, phase, commitReason, decisionTuple>>

FinalizeTOQFast ==
    /\ ~ownerPending
    /\ phase = "preaccept"
    /\ ModeledOptimizedFastEligible
    /\ phase' = "committed"
    /\ commitReason' = "remote-fast"
    /\ decisionTuple' = candidateTuple
    /\ UNCHANGED <<chainCase, chainExecuted, tick, timingCase, toqRequest,
                  receiverDurable, toqReplies, ownerPending, ownerAssignedAt,
                  ownerTuple, candidateTuple, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory>>

StartTOQSlowAccept ==
    /\ ~ownerPending
    /\ phase = "preaccept"
    /\ TotalTOQQuorumSize >= SlowQuorum
    /\ ~ModeledOptimizedFastEligible
    /\ phase' = "accepted"
    /\ commitReason' = "slow-accept"
    /\ decisionTuple' = SlowDecisionTuple
    /\ UNCHANGED <<chainCase, chainExecuted, tick, timingCase, toqRequest,
                  receiverDurable, toqReplies, ownerPending, ownerAssignedAt,
                  ownerTuple, candidateTuple, commitEvidence, prefixCommitHistory,
                  replyDeliveryHistory>>

Next ==
    \/ ExecuteA
    \/ Tick
    \/ QueueTOQPreAccept
    \/ \E r \in Followers : ProcessTOQAt(r)
    \/ \E r \in Followers : PersistTOQReply(r)
    \/ \E r \in Followers, kind \in ReplyKinds : DeliverTOQReply(r, kind)
    \/ AssignOwnerAtProcessAt
    \/ FinalizeTOQFast
    \/ StartTOQSlowAccept

SafeChainExecutesOnlySafeCases ==
    A \in chainExecuted => chainCase \in SafeChainCases

UnsafeChainsNeverExecuteBase ==
    chainCase \in UnsafeChainCases => A \notin chainExecuted

TOQRequestIsZeroAttrs ==
    toqRequest.present =>
        /\ toqRequest = QueuedRequest
        /\ toqRequest.toq
        /\ toqRequest.tuple.seq = 0
        /\ toqRequest.tuple.deps = {}

TOQProcessedAtOrAfterProcessAt ==
    \A r \in Followers :
        receiverDurable[r].processed =>
            /\ receiverDurable[r].processedAt >= receiverDurable[r].requestProcessAt
            /\ receiverDurable[r].processedAt >= ReceiverEarliestProcess(r)
            /\ receiverDurable[r].requestProcessAt = toqRequest.processAt
            /\ receiverDurable[r].conf = toqRequest.conf

TOQPersistenceFollowsProcessing ==
    \A r \in Followers :
        receiverDurable[r].persisted => receiverDurable[r].processed

TOQReplyAfterDurableRecord ==
    \A r \in Followers :
        toqReplies[r].present =>
            /\ receiverDurable[r].persisted
            /\ <<"valid", r>> \in replyDeliveryHistory

TOQReplyCarriesProcessedTuple ==
    \A r \in Followers :
        toqReplies[r].present =>
            toqReplies[r].tuple = receiverDurable[r].tuple

TOQReplyCarriesCommittedPrefixEvidence ==
    \A r \in Followers :
        toqReplies[r].present =>
            toqReplies[r].evidence = receiverDurable[r].evidence

TOQReplyEvidenceSound ==
    \A r \in Followers :
        \A dep \in toqReplies[r].evidence :
            toqReplies[r].present =>
                <<r, dep, toqReplies[r].conf, toqReplies[r].processAt>>
                    \in prefixCommitHistory

TOQReplyTupleAndEvidenceProvenance ==
    \A r \in Followers :
        toqReplies[r].present => toqReplies[r] = DurableReply(r)

TOQResponseConfigAndProcessAtMatchRequest ==
    \A r \in Followers :
        toqReplies[r].present =>
            /\ toqReplies[r].from = r
            /\ toqReplies[r].inst = toqRequest.inst
            /\ toqReplies[r].conf = toqRequest.conf
            /\ toqReplies[r].processAt = toqRequest.processAt

TOQCommitEvidenceIsCarried ==
    \A r \in Followers :
        IF toqReplies[r].present
        THEN commitEvidence[r] = toqReplies[r].evidence
        ELSE commitEvidence[r] = {}

OwnerAssignedOnlyAtOrAfterProcessAt ==
    ownerAssignedAt # Unprocessed =>
        /\ ownerAssignedAt >= toqRequest.processAt
        /\ ownerAssignedAt >= OwnerEarliestProcess

OwnerPendingBlocksDecision ==
    ownerPending =>
        /\ ownerAssignedAt = Unprocessed
        /\ ownerTuple = ZeroTuple
        /\ candidateTuple = ZeroTuple
        /\ phase = "preaccept"
        /\ commitReason = "none"
        /\ decisionTuple = ZeroTuple

TOQCandidateIsJoinOfCarriedReplies ==
    IF ownerPending
    THEN candidateTuple = ZeroTuple
    ELSE candidateTuple = CandidateFor(ownerTuple, toqReplies)

TOQAtMostOneRemoteCounts ==
    Cardinality(AcceptedSenders) <= 1

TOQFastCommitRequiresCoveringCarriedEvidence ==
    commitReason = "remote-fast" =>
        /\ phase = "committed"
        /\ TotalTOQQuorumSize = OptimizedFastQuorum
        /\ Cardinality(AcceptedSenders) = 1
        /\ decisionTuple = candidateTuple
        /\ SafeOptimizedFastEligible
        /\ candidateTuple.deps \subseteq
            UNION {toqReplies[r].evidence : r \in AcceptedSenders}

TOQDecisionPreservesOwnerAndRemoteTuple ==
    commitReason # "none" =>
        /\ decisionTuple = candidateTuple
        /\ TupleLeq(ownerTuple, decisionTuple)
        /\ \A r \in AcceptedSenders :
            TupleLeq(toqReplies[r].tuple, decisionTuple)

TOQSlowAcceptIsUnsafeFastFallback ==
    commitReason = "slow-accept" =>
        /\ phase = "accepted"
        /\ TotalTOQQuorumSize = SlowQuorum
        /\ ~SafeOptimizedFastEligible
        /\ decisionTuple = candidateTuple

TOQHasNoFastWaitPhase ==
    phase \in {"preaccept", "accepted", "committed"}
HistoricalEvidenceIsScenarioFixed ==
    prefixCommitHistory = PrefixHistoryForScenario

RejectedAndDuplicateRepliesFailClosed ==
    /\ <<"stale-process-at", 2>> \in replyDeliveryHistory =>
        timingCase = "stale-process-at"
    /\ <<"config-mismatch", 2>> \in replyDeliveryHistory =>
        timingCase = "config-mismatch"
    /\ <<"malformed", 2>> \in replyDeliveryHistory =>
        timingCase = "malformed-reply"
    /\ <<"duplicate", 2>> \in replyDeliveryHistory =>
        /\ timingCase = "duplicate-reply"
        /\ <<"valid", 2>> \in replyDeliveryHistory

Safety ==
    /\ SafeChainExecutesOnlySafeCases
    /\ UnsafeChainsNeverExecuteBase
    /\ TOQRequestIsZeroAttrs
    /\ TOQProcessedAtOrAfterProcessAt
    /\ TOQPersistenceFollowsProcessing
    /\ TOQReplyAfterDurableRecord
    /\ TOQReplyCarriesProcessedTuple
    /\ TOQReplyCarriesCommittedPrefixEvidence
    /\ TOQReplyEvidenceSound
    /\ TOQReplyTupleAndEvidenceProvenance
    /\ TOQResponseConfigAndProcessAtMatchRequest
    /\ TOQCommitEvidenceIsCarried
    /\ OwnerAssignedOnlyAtOrAfterProcessAt
    /\ OwnerPendingBlocksDecision
    /\ TOQCandidateIsJoinOfCarriedReplies
    /\ TOQAtMostOneRemoteCounts
    /\ TOQFastCommitRequiresCoveringCarriedEvidence
    /\ TOQDecisionPreservesOwnerAndRemoteTuple
    /\ TOQSlowAcceptIsUnsafeFastFallback
    /\ TOQHasNoFastWaitPhase
    /\ HistoricalEvidenceIsScenarioFixed
    /\ RejectedAndDuplicateRepliesFailClosed

EventuallySafeChainBaseExecutes ==
    [](chainCase \in SafeChainCases => <> (A \in chainExecuted))

EventuallyTimedDecision ==
    <>(phase \in {"accepted", "committed"})

EventuallyTOQProcessingCompletes ==
    <>(/\ ~ownerPending
       /\ \A r \in Followers : receiverDurable[r].persisted)

EventuallyRequiredReplyBranch ==
    <>(CASE timingCase = "stale-process-at" ->
                <<"stale-process-at", 2>> \in replyDeliveryHistory
         [] timingCase = "config-mismatch" ->
                <<"config-mismatch", 2>> \in replyDeliveryHistory
         [] timingCase = "malformed-reply" ->
                <<"malformed", 2>> \in replyDeliveryHistory
         [] timingCase = "duplicate-reply" ->
                <<"duplicate", 2>> \in replyDeliveryHistory
         [] OTHER -> TRUE)

EventuallyOptimizedFastCommit ==
    timingCase = "remote-fast" =>
        <>(/\ phase = "committed"
           /\ commitReason = "remote-fast"
           /\ TotalTOQQuorumSize = OptimizedFastQuorum
           /\ Cardinality(AcceptedSenders) = 1
           /\ CarriedEvidenceCoversCandidate)

EventuallyEvidenceInsufficientFallback ==
    timingCase = "missing-prefix" =>
        <>(/\ phase = "accepted"
           /\ commitReason = "slow-accept"
           /\ ~CarriedEvidenceCoversCandidate)

EventuallyDelayedOwnerSupersetFallback ==
    timingCase = "delayed-owner-superset" =>
        <>(/\ phase = "accepted"
           /\ commitReason = "slow-accept"
           /\ decisionTuple = candidateTuple
           /\ TupleLeq(ownerTuple, decisionTuple)
           /\ \A r \in AcceptedSenders :
                TupleLeq(toqReplies[r].tuple, decisionTuple))

EventuallyDelayedOwnerIncomparableFallback ==
    timingCase = "delayed-owner-incomparable" =>
        <>(/\ phase = "accepted"
           /\ commitReason = "slow-accept"
           /\ decisionTuple = candidateTuple
           /\ TupleLeq(ownerTuple, decisionTuple)
           /\ \A r \in AcceptedSenders :
                TupleLeq(toqReplies[r].tuple, decisionTuple))

QuorumReachedLeadsToTOQDecision ==
    []( (/\ ~ownerPending
         /\ phase = "preaccept"
         /\ TotalTOQQuorumSize >= SlowQuorum)
        ~> (phase \in {"accepted", "committed"}) )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(ExecuteA)
    /\ WF_Vars(Tick)
    /\ WF_Vars(QueueTOQPreAccept)
    /\ \A r \in Followers : WF_Vars(ProcessTOQAt(r))
    /\ \A r \in Followers : WF_Vars(PersistTOQReply(r))
    /\ \A r \in Followers :
        \A kind \in ReplyKinds : WF_Vars(DeliverTOQReply(r, kind))
    /\ WF_Vars(AssignOwnerAtProcessAt)
    /\ WF_Vars(FinalizeTOQFast)
    /\ WF_Vars(StartTOQSlowAccept)

====
