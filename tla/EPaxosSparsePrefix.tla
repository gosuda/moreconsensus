---- MODULE EPaxosSparsePrefix ----
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A focused finite model of compact dependency-prefix execution.  Slot   *)
(* labels are ordered symbols, not bounded natural instance numbers: High *)
(* and Max are adjacent model positions even though their implementation   *)
(* magnitudes may be arbitrarily far apart.                                *)
(*                                                                         *)
(* The model deliberately separates:                                       *)
(*   - compact prefix meaning from sparse materialization,                  *)
(*   - graph construction from exact readiness,                            *)
(*   - an exact pruning witness from an interval waiver,                    *)
(*   - per-drive recovery work from total finite-prefix work, and           *)
(*   - tentative Ready execution from Advance-durable execution.           *)
(***************************************************************************)

CONSTANTS RecoveryBudget, RecoveryCap, MaxCrashes

ASSUME /\ RecoveryBudget = 1
       /\ RecoveryCap = 2
       /\ MaxCrashes = 2

Low  == "slot-low"
NextLow == "slot-next"
High == "slot-high"
Max  == "slot-max"
NoSlot == "no-slot"

SlotOrder == <<Low, NextLow, High, Max>>
Slots == {Low, NextLow, High, Max}
Endpoints == Slots \cup {NoSlot}

SlotRank(s) == CHOOSE i \in DOMAIN SlotOrder : SlotOrder[i] = s
EndpointRank(e) == IF e = NoSlot THEN 0 ELSE SlotRank(e)
EndpointLE(left, right) == EndpointRank(left) <= EndpointRank(right)
PrefixSlots(endpoint) ==
    IF endpoint = NoSlot
    THEN {}
    ELSE {s \in Slots : SlotRank(s) <= SlotRank(endpoint)}
SymbolicSuccessor(s) ==
    IF SlotRank(s) < Len(SlotOrder)
    THEN SlotOrder[SlotRank(s) + 1]
    ELSE NoSlot

ConfOld == "conf-old"
ConfNew == "conf-new"
LaneOld1 == "old-replica-1"
LaneOld2 == "old-replica-2"
LaneNew1 == "new-replica-1"
Lanes == {LaneOld1, LaneOld2, LaneNew1}

LaneConf(lane) ==
    IF lane = LaneNew1 THEN ConfNew ELSE ConfOld
LaneReplica(lane) ==
    CASE lane = LaneOld1 -> 1
      [] lane = LaneOld2 -> 2
      [] OTHER -> 1

NoLane == "no-lane"
NoName == "no-name"

BaseRef(name) ==
    [kind |-> "base", name |-> name, lane |-> NoLane, slot |-> NoSlot]
Ref(lane, slot) ==
    [kind |-> "slot", name |-> NoName, lane |-> lane, slot |-> slot]

A == BaseRef("base-a")
B == BaseRef("base-b")
NoBase == BaseRef("no-base")
Bases == {A, B}
BaseOrder == <<A, B>>
BaseConf(base) == IF base = A THEN ConfOld ELSE ConfNew

SlotRefs == {Ref(lane, slot) : lane \in Lanes, slot \in Slots}
Nodes == Bases \cup SlotRefs
NoRef == Ref(NoLane, NoSlot)

Old1Low == Ref(LaneOld1, Low)
Old1Next == Ref(LaneOld1, NextLow)
Old1High == Ref(LaneOld1, High)
Old1Max == Ref(LaneOld1, Max)
Old2Low == Ref(LaneOld2, Low)
Old2Next == Ref(LaneOld2, NextLow)
New1Low == Ref(LaneNew1, Low)

NodeConf(node) ==
    IF node \in Bases THEN BaseConf(node) ELSE LaneConf(node.lane)

CompactEndpoint ==
    [base \in Bases |->
        [lane \in Lanes |->
            CASE base = A /\ lane = LaneOld1 -> Max
              [] base = A /\ lane = LaneOld2 -> NextLow
              [] base = B /\ lane = LaneNew1 -> High
              [] OTHER -> NoSlot]]

PrefixRefs(lane, endpoint) ==
    {Ref(lane, slot) : slot \in PrefixSlots(endpoint)}

CompactExactDeps(base) ==
    UNION {PrefixRefs(lane, CompactEndpoint[base][lane]) : lane \in Lanes}

(***************************************************************************)
(* A and B form an internal execution SCC.  Old1Max is the sole exact      *)
(* higher-sequence reverse-dependency witness.  Its witness is in A's old  *)
(* configuration; the same replica number in LaneNew1 is a different,     *)
(* configuration-pinned lane.                                              *)
(***************************************************************************)
DirectDeps ==
    [node \in Nodes |->
        CASE node = A -> {B}
          [] node = B -> {A}
          [] node = Old1Max -> {A}
          [] OTHER -> {}]

ExpandedDeps(node) ==
    DirectDeps[node] \cup
        (IF node \in Bases THEN CompactExactDeps(node) ELSE {})

SeqNum(node) ==
    CASE node = A -> 1
      [] node = B -> 1
      [] node = Old1Max -> 2
      [] OTHER -> 0

Unknown == "unknown"
PreAccepted == "preaccepted"
Accepted == "accepted"
Committed == "committed"
Executed == "executed"
Statuses == {Unknown, PreAccepted, Accepted, Committed, Executed}
KnownStatuses == Statuses \ {Unknown}
AtLeastCommitted == {Committed, Executed}
RecoveryPhases == {"none", "prepare", "accept", "commit"}

Source(base, lane) == [base |-> base, lane |-> lane]
SourceAOld1 == Source(A, LaneOld1)
SourceAOld2 == Source(A, LaneOld2)
SourceBNew1 == Source(B, LaneNew1)
Sources == {SourceAOld1, SourceAOld2, SourceBNew1}
SourceOrder == <<SourceAOld1, SourceAOld2, SourceBNew1>>
NoSource == Source(NoBase, NoLane)

SourcePos(source) ==
    CHOOSE i \in DOMAIN SourceOrder : SourceOrder[i] = source

RefOrder ==
    <<Ref(LaneOld1, Low), Ref(LaneOld1, NextLow),
      Ref(LaneOld1, High), Ref(LaneOld1, Max),
      Ref(LaneOld2, Low), Ref(LaneOld2, NextLow),
      Ref(LaneOld2, High), Ref(LaneOld2, Max),
      Ref(LaneNew1, Low), Ref(LaneNew1, NextLow),
      Ref(LaneNew1, High), Ref(LaneNew1, Max)>>
RefPos(ref) == CHOOSE i \in DOMAIN RefOrder : RefOrder[i] = ref

RecoveryKey(source, target) == [source |-> source, target |-> target]
RecoveryKeys ==
    {RecoveryKey(source, target) : source \in Sources, target \in SlotRefs}

ReadySeqs == {<<>>, <<A>>, <<B>>, <<A, B>>}
SeqSet(seq) == {seq[i] : i \in 1..Len(seq)}
Take(seq, count) ==
    IF count = 0 THEN <<>> ELSE SubSeq(seq, 1, count)
Drop(seq, count) ==
    IF count = 0 THEN seq
    ELSE IF count = Len(seq) THEN <<>>
    ELSE SubSeq(seq, count + 1, Len(seq))

CoverageNames ==
    {"sparse-hole", "high-max-endpoint", "exact-pruning",
     "recovery-saturation", "restart-before-advance",
     "restart-after-advance", "execution-after-finite-closure"}

VARIABLES
    materialized,
    status,
    durableExecuted,
    volatileExecuted,
    committedThrough,
    executedThrough,
    activeRecovery,
    recoverySource,
    recoveryPhase,
    lastSource,
    driveOpen,
    startsThisDrive,
    scheduledSources,
    startedRecoveries,
    pendingReady,
    readyFrozen,
    awaitAdvance,
    appliedReadyPrefix,
    appliedHistory,
    advancedHistory,
    running,
    crashCount,
    crashPoint,
    crashBlockers,
    crashActive,
    crashRecoverySource,
    crashRecoveryPhase,
    restartObserved,
    coverage

Vars ==
    <<materialized, status, durableExecuted, volatileExecuted,
      committedThrough, executedThrough, activeRecovery, recoverySource,
      recoveryPhase, lastSource, driveOpen, startsThisDrive,
      scheduledSources, startedRecoveries, pendingReady, readyFrozen,
      awaitAdvance, appliedReadyPrefix, appliedHistory, advancedHistory,
      running, crashCount, crashPoint, crashBlockers, crashActive,
      crashRecoverySource, crashRecoveryPhase, restartObserved, coverage>>

InitialStatus ==
    [node \in Nodes |->
        CASE node \in Bases -> Committed
          [] node \in {Old1Low, Old1High} -> Executed
          [] node = Old1Max -> PreAccepted
          [] node = Old2Low -> Committed
          [] OTHER -> Unknown]

StatusCommittedSet(statusMap) ==
    {node \in Nodes : statusMap[node] \in AtLeastCommitted}
StatusExecutedSet(statusMap) ==
    {node \in Nodes : statusMap[node] = Executed}

Frontier(lane, exactSet) ==
    CHOOSE endpoint \in Endpoints :
        /\ PrefixRefs(lane, endpoint) \subseteq exactSet
        /\ \A other \in Endpoints :
              PrefixRefs(lane, other) \subseteq exactSet =>
                  EndpointLE(other, endpoint)

Tracker(exactSet) ==
    [lane \in Lanes |-> Frontier(lane, exactSet)]

KnownAfter(base, other) ==
    /\ base \in Bases
    /\ other \in SlotRefs
    /\ other \in materialized
    /\ status[other] \in KnownStatuses
    /\ other \in ExpandedDeps(base)
    /\ NodeConf(base) = NodeConf(other)
    /\ SeqNum(other) > SeqNum(base)
    /\ base \in DirectDeps[other]

PrunedExact(base) ==
    {other \in CompactExactDeps(base) : KnownAfter(base, other)}

MaterializedGraphVertices ==
    {node \in materialized :
        status[node] \in AtLeastCommitted /\ node \notin volatileExecuted}

NodePairs == Nodes \X Nodes

ExpandedMaterializedEdges ==
    {edge \in NodePairs :
        /\ edge[1] \in MaterializedGraphVertices
        /\ edge[2] \in MaterializedGraphVertices
        /\ edge[2] \in ExpandedDeps(edge[1])
        /\ (edge[1] \notin Bases \/ edge[2] \notin PrunedExact(edge[1]))}

LazyMaterializedDeps(node) ==
    (DirectDeps[node] \cap materialized) \cup
        (IF node \in Bases
         THEN UNION {
                  PrefixRefs(lane, CompactEndpoint[node][lane]) \cap materialized
                  : lane \in Lanes}
         ELSE {})

LazyMaterializedEdges ==
    {edge \in NodePairs :
        /\ edge[1] \in MaterializedGraphVertices
        /\ edge[2] \in MaterializedGraphVertices
        /\ edge[2] \in LazyMaterializedDeps(edge[1])
        /\ (edge[1] \notin Bases \/ edge[2] \notin PrunedExact(edge[1]))}

(***************************************************************************)
(* This topology has only two possibly cyclic vertices (A and B); all slot *)
(* vertices are leaves while committed-unexecuted.  Thus a simple path has *)
(* at most two edges.  This avoids pretending the symbolic Max label means *)
(* a large enumerated path.                                                 *)
(***************************************************************************)
Reachable(edges, from, to) ==
    /\ from \in MaterializedGraphVertices
    /\ to \in MaterializedGraphVertices
    /\ \/ from = to
       \/ <<from, to>> \in edges
       \/ \E middle \in Nodes :
              <<from, middle>> \in edges /\ <<middle, to>> \in edges

SCCOf(edges, node) ==
    IF node \in MaterializedGraphVertices
    THEN {other \in MaterializedGraphVertices :
              Reachable(edges, node, other) /\ Reachable(edges, other, node)}
    ELSE {}

IsSCC(edges, component) ==
    /\ component # {}
    /\ \E node \in MaterializedGraphVertices :
           component = SCCOf(edges, node)

ExactReady(component) ==
    \A base \in component \cap Bases :
        \A dep \in ExpandedDeps(base) :
            \/ dep \in volatileExecuted
            \/ dep \in component
            \/ dep \in PrunedExact(base)

ExpandedReady(component) ==
    /\ IsSCC(ExpandedMaterializedEdges, component)
    /\ component \cap Bases # {}
    /\ ExactReady(component)

LazyReady(component) ==
    /\ IsSCC(LazyMaterializedEdges, component)
    /\ component \cap Bases # {}
    /\ ExactReady(component)

RecoverableSlots(source) ==
    {slot \in PrefixSlots(CompactEndpoint[source.base][source.lane]) :
        LET target == Ref(source.lane, slot)
        IN /\ target \notin volatileExecuted
           /\ target \notin PrunedExact(source.base)
           /\ status[target] \notin AtLeastCommitted}

FirstRecoverable(source) ==
    LET candidates == RecoverableSlots(source)
    IN IF candidates = {}
       THEN NoRef
       ELSE Ref(source.lane,
                CHOOSE slot \in candidates :
                    \A other \in candidates : SlotRank(slot) <= SlotRank(other))

SourceNeedsRecovery(source) == FirstRecoverable(source) # NoRef
ActiveTargets == activeRecovery
EligibleSources ==
    {source \in Sources :
        /\ SourceNeedsRecovery(source)
        /\ FirstRecoverable(source) \notin ActiveTargets}

CursorDistance(source) ==
    IF lastSource = NoSource
    THEN SourcePos(source)
    ELSE IF SourcePos(source) > SourcePos(lastSource)
         THEN SourcePos(source) - SourcePos(lastSource)
         ELSE Len(SourceOrder) - SourcePos(lastSource) + SourcePos(source)

SelectedSource ==
    CHOOSE source \in EligibleSources :
        \A other \in EligibleSources :
            CursorDistance(source) <= CursorDistance(other)

CanStartRecovery ==
    /\ running
    /\ driveOpen
    /\ startsThisDrive < RecoveryBudget
    /\ Cardinality(activeRecovery) < RecoveryCap
    /\ EligibleSources # {}

DurableBlockers(base) ==
    {dep \in ExpandedDeps(base) :
        /\ dep \notin durableExecuted
        /\ dep \notin PrunedExact(base)
        /\ ~(dep \in Bases /\ base \in DirectDeps[dep])}

DurableBlockerMap == [base \in Bases |-> DurableBlockers(base)]

FinitePrefixClosed ==
    \A base \in Bases :
        CompactExactDeps(base) \subseteq durableExecuted \cup PrunedExact(base)

SparseHoleState ==
    /\ status[Old1Next] = Unknown
    /\ Old1High \in durableExecuted
    /\ executedThrough[LaneOld1] = Low

HighMaxEndpointState ==
    /\ CompactEndpoint[A][LaneOld1] = Max
    /\ High # Max
    /\ SymbolicSuccessor(High) = Max
    /\ SymbolicSuccessor(Max) = NoSlot

ExactPruningState ==
    /\ Old1Max \in PrunedExact(A)
    /\ Old1Next \notin PrunedExact(A)
    /\ status[Old1Next] = Unknown

StaticCoverageMissing ==
    {"sparse-hole", "high-max-endpoint", "exact-pruning"} \ coverage

InitialMaterialized == {node \in Nodes : InitialStatus[node] # Unknown}
InitialDurableExecuted == StatusExecutedSet(InitialStatus)

Init ==
    /\ materialized = InitialMaterialized
    /\ status = InitialStatus
    /\ durableExecuted = InitialDurableExecuted
    /\ volatileExecuted = InitialDurableExecuted
    /\ committedThrough = Tracker(StatusCommittedSet(InitialStatus))
    /\ executedThrough = Tracker(InitialDurableExecuted)
    /\ activeRecovery = {}
    /\ recoverySource = [target \in SlotRefs |-> NoSource]
    /\ recoveryPhase = [target \in SlotRefs |-> "none"]
    /\ lastSource = NoSource
    /\ driveOpen = FALSE
    /\ startsThisDrive = 0
    /\ scheduledSources = {}
    /\ startedRecoveries = {}
    /\ pendingReady = <<>>
    /\ readyFrozen = <<>>
    /\ awaitAdvance = FALSE
    /\ appliedReadyPrefix = 0
    /\ appliedHistory = {}
    /\ advancedHistory = {}
    /\ running = TRUE
    /\ crashCount = 0
    /\ crashPoint = "none"
    /\ crashBlockers = [base \in Bases |-> {}]
    /\ crashActive = {}
    /\ crashRecoverySource = [target \in SlotRefs |-> NoSource]
    /\ crashRecoveryPhase = [target \in SlotRefs |-> "none"]
    /\ restartObserved = FALSE
    /\ coverage = {}

ObserveStaticCoverage ==
    /\ SparseHoleState
    /\ HighMaxEndpointState
    /\ ExactPruningState
    /\ StaticCoverageMissing # {}
    /\ coverage' = coverage \cup
           {"sparse-hole", "high-max-endpoint", "exact-pruning"}
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, activeRecovery, recoverySource,
          recoveryPhase, lastSource, driveOpen, startsThisDrive,
          scheduledSources, startedRecoveries, pendingReady, readyFrozen,
          awaitAdvance, appliedReadyPrefix, appliedHistory, advancedHistory,
          running, crashCount, crashPoint, crashBlockers, crashActive,
          crashRecoverySource, crashRecoveryPhase, restartObserved>>

OpenDrive ==
    /\ running
    /\ ~driveOpen
    /\ driveOpen' = TRUE
    /\ startsThisDrive' = 0
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, activeRecovery, recoverySource,
          recoveryPhase, lastSource, scheduledSources, startedRecoveries,
          pendingReady, readyFrozen, awaitAdvance, appliedReadyPrefix,
          appliedHistory, advancedHistory, running, crashCount, crashPoint,
          crashBlockers, crashActive, crashRecoverySource,
          crashRecoveryPhase, restartObserved, coverage>>

StartSelectedRecovery ==
    LET source == SelectedSource
        target == FirstRecoverable(source)
        newActive == activeRecovery \cup {target}
        newCoverage ==
            IF Cardinality(newActive) = RecoveryCap
            THEN coverage \cup {"recovery-saturation"}
            ELSE coverage
    IN
    /\ CanStartRecovery
    /\ activeRecovery' = newActive
    /\ recoverySource' = [recoverySource EXCEPT ![target] = source]
    /\ recoveryPhase' = [recoveryPhase EXCEPT ![target] = "prepare"]
    /\ lastSource' = source
    /\ startsThisDrive' = startsThisDrive + 1
    /\ scheduledSources' = scheduledSources \cup {source}
    /\ startedRecoveries' =
           startedRecoveries \cup {RecoveryKey(source, target)}
    /\ coverage' = newCoverage
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, driveOpen, pendingReady,
          readyFrozen, awaitAdvance, appliedReadyPrefix, appliedHistory,
          advancedHistory, running, crashCount, crashPoint, crashBlockers,
          crashActive, crashRecoverySource, crashRecoveryPhase>>

CloseDrive ==
    /\ running
    /\ driveOpen
    /\ \/ startsThisDrive = RecoveryBudget
       \/ EligibleSources = {}
       \/ Cardinality(activeRecovery) = RecoveryCap
    /\ driveOpen' = FALSE
    /\ startsThisDrive' = 0
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, activeRecovery, recoverySource,
          recoveryPhase, lastSource, scheduledSources, startedRecoveries,
          pendingReady, readyFrozen, awaitAdvance, appliedReadyPrefix,
          appliedHistory, advancedHistory, running, crashCount, crashPoint,
          crashBlockers, crashActive, crashRecoverySource,
          crashRecoveryPhase, restartObserved, coverage>>

PrepareRecovery(target) ==
    /\ running
    /\ target \in activeRecovery
    /\ recoveryPhase[target] = "prepare"
    /\ recoveryPhase' = [recoveryPhase EXCEPT ![target] = "accept"]
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, activeRecovery, recoverySource,
          lastSource, driveOpen, startsThisDrive, scheduledSources,
          startedRecoveries, pendingReady, readyFrozen, awaitAdvance,
          appliedReadyPrefix, appliedHistory, advancedHistory, running,
          crashCount, crashPoint, crashBlockers, crashActive,
          crashRecoverySource, crashRecoveryPhase, coverage>>

AcceptRecovery(target) ==
    /\ running
    /\ target \in activeRecovery
    /\ recoveryPhase[target] = "accept"
    /\ recoveryPhase' = [recoveryPhase EXCEPT ![target] = "commit"]
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, activeRecovery, recoverySource,
          lastSource, driveOpen, startsThisDrive, scheduledSources,
          startedRecoveries, pendingReady, readyFrozen, awaitAdvance,
          appliedReadyPrefix, appliedHistory, advancedHistory, running,
          crashCount, crashPoint, crashBlockers, crashActive,
          crashRecoverySource, crashRecoveryPhase, coverage>>

CommitRecovery(target) ==
    LET newStatus == [status EXCEPT ![target] = Executed]
        newDurable == durableExecuted \cup {target}
    IN
    /\ running
    /\ target \in activeRecovery
    /\ recoveryPhase[target] = "commit"
    /\ materialized' = materialized \cup {target}
    /\ status' = newStatus
    /\ durableExecuted' = newDurable
    /\ volatileExecuted' = volatileExecuted \cup {target}
    /\ committedThrough' = Tracker(StatusCommittedSet(newStatus))
    /\ executedThrough' = Tracker(newDurable)
    /\ activeRecovery' = activeRecovery \ {target}
    /\ recoverySource' = [recoverySource EXCEPT ![target] = NoSource]
    /\ recoveryPhase' = [recoveryPhase EXCEPT ![target] = "none"]
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<lastSource, driveOpen, startsThisDrive, scheduledSources,
          startedRecoveries, pendingReady, readyFrozen, awaitAdvance,
          appliedReadyPrefix, appliedHistory, advancedHistory, running,
          crashCount, crashPoint, crashBlockers, crashActive,
          crashRecoverySource, crashRecoveryPhase, coverage>>

ExecuteCommittedInternal(target) ==
    LET newStatus == [status EXCEPT ![target] = Executed]
        newDurable == durableExecuted \cup {target}
    IN
    /\ running
    /\ target \in SlotRefs
    /\ status[target] = Committed
    /\ DirectDeps[target] \subseteq volatileExecuted
    /\ status' = newStatus
    /\ durableExecuted' = newDurable
    /\ volatileExecuted' = volatileExecuted \cup {target}
    /\ committedThrough' = Tracker(StatusCommittedSet(newStatus))
    /\ executedThrough' = Tracker(newDurable)
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<materialized, activeRecovery, recoverySource, recoveryPhase,
          lastSource, driveOpen, startsThisDrive, scheduledSources,
          startedRecoveries, pendingReady, readyFrozen, awaitAdvance,
          appliedReadyPrefix, appliedHistory, advancedHistory, running,
          crashCount, crashPoint, crashBlockers, crashActive,
          crashRecoverySource, crashRecoveryPhase, coverage>>

BatchFor(component) ==
    CASE component \cap Bases = {A, B} -> <<A, B>>
      [] component \cap Bases = {A} -> <<A>>
      [] component \cap Bases = {B} -> <<B>>
      [] OTHER -> <<>>

EmitReady(component) ==
    LET batch == BatchFor(component)
    IN
    /\ running
    /\ ~awaitAdvance
    /\ pendingReady = <<>>
    /\ component \subseteq Bases
    /\ ExpandedReady(component)
    /\ batch # <<>>
    /\ pendingReady' = batch
    /\ volatileExecuted' = volatileExecuted \cup SeqSet(batch)
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<materialized, status, durableExecuted, committedThrough,
          executedThrough, activeRecovery, recoverySource, recoveryPhase,
          lastSource, driveOpen, startsThisDrive, scheduledSources,
          startedRecoveries, readyFrozen, awaitAdvance, appliedReadyPrefix,
          appliedHistory, advancedHistory, running, crashCount, crashPoint,
          crashBlockers, crashActive, crashRecoverySource,
          crashRecoveryPhase, coverage>>

EmitAnyReady == \E component \in SUBSET Bases : EmitReady(component)

ExposeReady ==
    /\ running
    /\ ~awaitAdvance
    /\ pendingReady # <<>>
    /\ readyFrozen' = pendingReady
    /\ awaitAdvance' = TRUE
    /\ appliedReadyPrefix' = 0
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, activeRecovery, recoverySource,
          recoveryPhase, lastSource, driveOpen, startsThisDrive,
          scheduledSources, startedRecoveries, pendingReady, appliedHistory,
          advancedHistory, running, crashCount, crashPoint, crashBlockers,
          crashActive, crashRecoverySource, crashRecoveryPhase,
          restartObserved, coverage>>

ApplyOne ==
    LET next == appliedReadyPrefix + 1
        command == readyFrozen[next]
    IN
    /\ running
    /\ awaitAdvance
    /\ appliedReadyPrefix < Len(readyFrozen)
    /\ appliedReadyPrefix' = next
    /\ appliedHistory' = appliedHistory \cup {command}
    /\ UNCHANGED
        <<materialized, status, durableExecuted, volatileExecuted,
          committedThrough, executedThrough, activeRecovery, recoverySource,
          recoveryPhase, lastSource, driveOpen, startsThisDrive,
          scheduledSources, startedRecoveries, pendingReady, readyFrozen,
          awaitAdvance, advancedHistory, running, crashCount, crashPoint,
          crashBlockers, crashActive, crashRecoverySource,
          crashRecoveryPhase, restartObserved, coverage>>

AdvancePrefix(count) ==
    LET acknowledged == SeqSet(Take(readyFrozen, count))
        newStatus ==
            [node \in Nodes |->
                IF node \in acknowledged THEN Executed ELSE status[node]]
        newDurable == durableExecuted \cup acknowledged
        newCoverage ==
            IF Bases \subseteq newDurable /\ FinitePrefixClosed
            THEN coverage \cup {"execution-after-finite-closure"}
            ELSE coverage
    IN
    /\ running
    /\ awaitAdvance
    /\ count \in 1..appliedReadyPrefix
    /\ status' = newStatus
    /\ durableExecuted' = newDurable
    /\ committedThrough' = Tracker(StatusCommittedSet(newStatus))
    /\ executedThrough' = Tracker(newDurable)
    /\ pendingReady' = Drop(pendingReady, count)
    /\ readyFrozen' = <<>>
    /\ awaitAdvance' = FALSE
    /\ appliedReadyPrefix' = 0
    /\ advancedHistory' = advancedHistory \cup acknowledged
    /\ coverage' = newCoverage
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<materialized, volatileExecuted, activeRecovery, recoverySource,
          recoveryPhase, lastSource, driveOpen, startsThisDrive,
          scheduledSources, startedRecoveries, appliedHistory, running,
          crashCount, crashPoint, crashBlockers, crashActive,
          crashRecoverySource, crashRecoveryPhase>>

AdvanceAny == \E count \in 1..2 : AdvancePrefix(count)

CrashBeforeAdvanceEnabled ==
    /\ awaitAdvance
    /\ advancedHistory = {}
    /\ "restart-before-advance" \notin coverage

CrashAfterAdvanceEnabled ==
    /\ advancedHistory # {}
    /\ "restart-before-advance" \in coverage
    /\ "restart-after-advance" \notin coverage

Crash ==
    /\ running
    /\ crashCount < MaxCrashes
    /\ CrashBeforeAdvanceEnabled \/ CrashAfterAdvanceEnabled
    /\ running' = FALSE
    /\ crashCount' = crashCount + 1
    /\ crashPoint' =
           IF CrashBeforeAdvanceEnabled THEN "before-advance" ELSE "after-advance"
    /\ crashBlockers' = DurableBlockerMap
    /\ crashActive' = activeRecovery
    /\ crashRecoverySource' = recoverySource
    /\ crashRecoveryPhase' = recoveryPhase
    /\ volatileExecuted' = durableExecuted
    /\ pendingReady' = <<>>
    /\ readyFrozen' = <<>>
    /\ awaitAdvance' = FALSE
    /\ appliedReadyPrefix' = 0
    /\ lastSource' = NoSource
    /\ driveOpen' = FALSE
    /\ startsThisDrive' = 0
    /\ restartObserved' = FALSE
    /\ UNCHANGED
        <<materialized, status, durableExecuted, committedThrough,
          executedThrough, activeRecovery, recoverySource, recoveryPhase,
          scheduledSources, startedRecoveries, appliedHistory,
          advancedHistory, coverage>>

Restart ==
    LET restartTag ==
            IF crashPoint = "before-advance"
            THEN "restart-before-advance"
            ELSE "restart-after-advance"
    IN
    /\ ~running
    /\ running' = TRUE
    /\ volatileExecuted' = durableExecuted
    /\ committedThrough' = Tracker(StatusCommittedSet(status))
    /\ executedThrough' = Tracker(durableExecuted)
    /\ lastSource' = NoSource
    /\ driveOpen' = FALSE
    /\ startsThisDrive' = 0
    /\ pendingReady' = <<>>
    /\ readyFrozen' = <<>>
    /\ awaitAdvance' = FALSE
    /\ appliedReadyPrefix' = 0
    /\ restartObserved' = TRUE
    /\ coverage' = coverage \cup {restartTag}
    /\ UNCHANGED
        <<materialized, status, durableExecuted, activeRecovery,
          recoverySource, recoveryPhase, scheduledSources,
          startedRecoveries, appliedHistory, advancedHistory, crashCount,
          crashPoint, crashBlockers, crashActive, crashRecoverySource,
          crashRecoveryPhase>>

SafetyNext ==
    \/ ObserveStaticCoverage
    \/ OpenDrive
    \/ StartSelectedRecovery
    \/ CloseDrive
    \/ \E target \in SlotRefs : PrepareRecovery(target)
    \/ \E target \in SlotRefs : AcceptRecovery(target)
    \/ \E target \in SlotRefs : CommitRecovery(target)
    \/ \E target \in SlotRefs : ExecuteCommittedInternal(target)
    \/ EmitAnyReady
    \/ ExposeReady
    \/ ApplyOne
    \/ AdvanceAny
    \/ Crash
    \/ Restart

SafetySpec == Init /\ [][SafetyNext]_Vars

(***************************************************************************)
(* Safety invariants requested by the compact-prefix contract.             *)
(***************************************************************************)
TypeOK ==
    /\ materialized \subseteq Nodes
    /\ status \in [Nodes -> Statuses]
    /\ durableExecuted \subseteq Nodes
    /\ volatileExecuted \subseteq Nodes
    /\ committedThrough \in [Lanes -> Endpoints]
    /\ executedThrough \in [Lanes -> Endpoints]
    /\ activeRecovery \subseteq SlotRefs
    /\ recoverySource \in [SlotRefs -> Sources \cup {NoSource}]
    /\ recoveryPhase \in [SlotRefs -> RecoveryPhases]
    /\ lastSource \in Sources \cup {NoSource}
    /\ driveOpen \in BOOLEAN
    /\ startsThisDrive \in 0..RecoveryBudget
    /\ scheduledSources \subseteq Sources
    /\ startedRecoveries \subseteq RecoveryKeys
    /\ pendingReady \in ReadySeqs
    /\ readyFrozen \in ReadySeqs
    /\ awaitAdvance \in BOOLEAN
    /\ appliedReadyPrefix \in 0..2
    /\ appliedHistory \subseteq Bases
    /\ advancedHistory \subseteq Bases
    /\ running \in BOOLEAN
    /\ crashCount \in 0..MaxCrashes
    /\ crashPoint \in {"none", "before-advance", "after-advance"}
    /\ crashBlockers \in [Bases -> SUBSET Nodes]
    /\ crashActive \subseteq SlotRefs
    /\ crashRecoverySource \in [SlotRefs -> Sources \cup {NoSource}]
    /\ crashRecoveryPhase \in [SlotRefs -> RecoveryPhases]
    /\ restartObserved \in BOOLEAN
    /\ coverage \subseteq CoverageNames
    /\ materialized = {node \in Nodes : status[node] # Unknown}
    /\ \A target \in SlotRefs :
           (target \in activeRecovery <=> recoveryPhase[target] # "none")
    /\ \A target \in SlotRefs \ activeRecovery :
           recoverySource[target] = NoSource

PrefixMeaningExact ==
    /\ \A base \in Bases :
         ExpandedDeps(base) = DirectDeps[base] \cup
             UNION {PrefixRefs(lane, CompactEndpoint[base][lane]) :
                        lane \in Lanes}
    /\ \A base \in Bases, lane \in Lanes :
         CompactExactDeps(base) \cap PrefixRefs(lane, Max) =
             PrefixRefs(lane, CompactEndpoint[base][lane])
    /\ Low # High
    /\ High # Max
    /\ SymbolicSuccessor(Max) = NoSlot
    /\ LaneReplica(LaneOld1) = LaneReplica(LaneNew1)
    /\ LaneConf(LaneOld1) # LaneConf(LaneNew1)

ExecutedTrackerExact ==
    /\ durableExecuted = StatusExecutedSet(status)
    /\ executedThrough = Tracker(durableExecuted)
    /\ committedThrough = Tracker(StatusCommittedSet(status))
    /\ \A lane \in Lanes :
         /\ PrefixRefs(lane, executedThrough[lane]) \subseteq durableExecuted
         /\ \A endpoint \in Endpoints :
              PrefixRefs(lane, endpoint) \subseteq durableExecuted =>
                  EndpointLE(endpoint, executedThrough[lane])

LazyEdgesEqualExpandedMaterializedEdges ==
    LazyMaterializedEdges = ExpandedMaterializedEdges

LazyReadyEqualsExpandedReady ==
    /\ \A node \in Nodes :
         SCCOf(LazyMaterializedEdges, node) =
             SCCOf(ExpandedMaterializedEdges, node)
    /\ \A node \in Nodes :
         LET expandedComponent == SCCOf(ExpandedMaterializedEdges, node)
             lazyComponent == SCCOf(LazyMaterializedEdges, node)
         IN ExpandedReady(expandedComponent) <=> LazyReady(lazyComponent)

NoExecutionAcrossUnknownHole ==
    \A base \in Bases \cap volatileExecuted :
        \A dep \in CompactExactDeps(base) :
            status[dep] # Unknown \/ dep \in PrunedExact(base)

PruningIsPerExactWitness ==
    \A base \in Bases :
        /\ PrunedExact(base) =
             {target \in CompactExactDeps(base) : KnownAfter(base, target)}
        /\ \A witness \in PrunedExact(base) :
             \A lower \in CompactExactDeps(base) \cap SlotRefs :
                 lower.lane = witness.lane
                 /\ SlotRank(lower.slot) < SlotRank(witness.slot)
                 /\ ~KnownAfter(base, lower)
                 => lower \notin PrunedExact(base)

RecoveryTargetsAreExactAndUnresolved ==
    /\ \A target \in activeRecovery :
         LET source == recoverySource[target]
         IN /\ source \in Sources
            /\ target \in CompactExactDeps(source.base)
            /\ target = FirstRecoverable(source)
            /\ target \notin durableExecuted
            /\ target \notin PrunedExact(source.base)
            /\ status[target] \notin AtLeastCommitted
    /\ \A key \in startedRecoveries :
         key.target \in CompactExactDeps(key.source.base)

RecoveryStartsBounded ==
    /\ startsThisDrive <= RecoveryBudget
    /\ Cardinality(activeRecovery) <= RecoveryCap
    /\ (~driveOpen => startsThisDrive = 0)

ReadyStable ==
    /\ (awaitAdvance =>
          /\ readyFrozen # <<>>
          /\ readyFrozen = pendingReady
          /\ appliedReadyPrefix <= Len(readyFrozen))
    /\ (~awaitAdvance => readyFrozen = <<>> /\ appliedReadyPrefix = 0)

ReadyAdvanceExecutedDurability ==
    /\ ReadyStable
    /\ volatileExecuted = durableExecuted \cup SeqSet(pendingReady)
    /\ advancedHistory = durableExecuted \cap Bases
    /\ advancedHistory \subseteq appliedHistory
    /\ \A base \in Bases :
          status[base] = Executed <=> base \in advancedHistory

RestartReconstructsClosure ==
    restartObserved =>
        /\ running
        /\ volatileExecuted = durableExecuted
        /\ pendingReady = <<>>
        /\ readyFrozen = <<>>
        /\ ~awaitAdvance
        /\ lastSource = NoSource
        /\ startsThisDrive = 0
        /\ executedThrough = Tracker(durableExecuted)
        /\ committedThrough = Tracker(StatusCommittedSet(status))
        /\ DurableBlockerMap = crashBlockers
        /\ activeRecovery = crashActive
        /\ recoverySource = crashRecoverySource
        /\ recoveryPhase = crashRecoveryPhase

(***************************************************************************)
(* Coverage history and deterministic coverage driver.                     *)
(***************************************************************************)
SparseHoleCovered == "sparse-hole" \in coverage
HighMaxEndpointCovered == "high-max-endpoint" \in coverage
ExactPruningCovered == "exact-pruning" \in coverage
RecoverySaturationCovered == "recovery-saturation" \in coverage
RestartBeforeAdvanceCovered == "restart-before-advance" \in coverage
RestartAfterAdvanceCovered == "restart-after-advance" \in coverage
ExecutionAfterFiniteClosureCovered ==
    "execution-after-finite-closure" \in coverage

CoverageComplete == coverage = CoverageNames

FirstTargetInPhase(phaseName) ==
    CHOOSE target \in {ref \in activeRecovery : recoveryPhase[ref] = phaseName} :
        \A other \in {ref \in activeRecovery :
                         recoveryPhase[ref] = phaseName} :
            RefPos(target) <= RefPos(other)

FirstCommittedInternal ==
    CHOOSE target \in {ref \in SlotRefs : status[ref] = Committed} :
        \A other \in {ref \in SlotRefs : status[ref] = Committed} :
            RefPos(target) <= RefPos(other)

HasRecoveryPhase(phaseName) ==
    \E target \in activeRecovery : recoveryPhase[target] = phaseName
HasCommittedInternal == \E target \in SlotRefs : status[target] = Committed

PrepareFirstActive ==
    /\ HasRecoveryPhase("prepare")
    /\ PrepareRecovery(FirstTargetInPhase("prepare"))

AcceptFirstActive ==
    /\ HasRecoveryPhase("accept")
    /\ AcceptRecovery(FirstTargetInPhase("accept"))

CommitFirstActive ==
    /\ HasRecoveryPhase("commit")
    /\ CommitRecovery(FirstTargetInPhase("commit"))

ExecuteFirstCommittedInternal ==
    /\ HasCommittedInternal
    /\ ExecuteCommittedInternal(FirstCommittedInternal)

CoverageDriveStep ==
    IF ~driveOpen
    THEN OpenDrive
    ELSE IF CanStartRecovery
         THEN StartSelectedRecovery
         ELSE CloseDrive

CoverageRecoveryStep ==
    CASE HasRecoveryPhase("prepare") -> PrepareFirstActive
      [] HasRecoveryPhase("accept") -> AcceptFirstActive
      [] HasRecoveryPhase("commit") -> CommitFirstActive
      [] HasCommittedInternal -> ExecuteFirstCommittedInternal
      [] OTHER -> CoverageDriveStep

CoverageBeforeRestartStep ==
    IF ~running
    THEN Restart
    ELSE IF pendingReady = <<>> /\ ~awaitAdvance
         THEN EmitAnyReady
         ELSE IF ~awaitAdvance
              THEN ExposeReady
              ELSE Crash

CoverageAfterRestartStep ==
    IF ~running
    THEN Restart
    ELSE IF advancedHistory = {}
         THEN IF pendingReady = <<>> /\ ~awaitAdvance
              THEN EmitAnyReady
              ELSE IF ~awaitAdvance
                   THEN ExposeReady
                   ELSE IF appliedReadyPrefix = 0
                        THEN ApplyOne
                        ELSE AdvancePrefix(1)
         ELSE Crash

CoverageFinishExecutionStep ==
    IF pendingReady = <<>> /\ ~awaitAdvance
    THEN EmitAnyReady
    ELSE IF ~awaitAdvance
         THEN ExposeReady
         ELSE IF appliedReadyPrefix = 0
              THEN ApplyOne
              ELSE AdvancePrefix(1)

CoverageNext ==
    CASE StaticCoverageMissing # {} -> ObserveStaticCoverage
      [] ~RecoverySaturationCovered -> CoverageDriveStep
      [] ~FinitePrefixClosed -> CoverageRecoveryStep
      [] ~RestartBeforeAdvanceCovered -> CoverageBeforeRestartStep
      [] ~RestartAfterAdvanceCovered -> CoverageAfterRestartStep
      [] ~ExecutionAfterFiniteClosureCovered -> CoverageFinishExecutionStep
      [] OTHER -> FALSE

(***************************************************************************)
(* CoverageSpec is intentionally deterministic apart from the unique ready *)
(* SCC.  Its weak fairness is only a reachability harness for all named     *)
(* non-vacuity states; it is not a protocol liveness assumption.           *)
(***************************************************************************)
CoverageSpec ==
    /\ Init
    /\ [][CoverageNext]_Vars
    /\ WF_Vars(CoverageNext)

EventuallySparseHole == <>SparseHoleCovered
EventuallyHighMaxEndpoint == <>HighMaxEndpointCovered
EventuallyExactPruning == <>ExactPruningCovered
EventuallyRecoverySaturation == <>RecoverySaturationCovered
EventuallyRestartBeforeAdvance == <>RestartBeforeAdvanceCovered
EventuallyRestartAfterAdvance == <>RestartAfterAdvanceCovered
EventuallyExecutionAfterFiniteClosure ==
    <>ExecutionAfterFiniteClosureCovered
EventuallyCoverageComplete == <>CoverageComplete

(***************************************************************************)
(* The two liveness specifications below intentionally isolate deterministic *)
(* finite scenarios while reusing the exact SafetyNext phase actions.       *)
(* SchedulingFairSpec assumes weak fairness of one stable-cursor drive or   *)
(* exact prepare/accept/commit response step.  ExecutionFairSpec additionally*)
(* assumes weak fairness of the single Ready/apply/Advance step.  The fixed *)
(* scenarios are crash-free, representing the eventual crash-free suffix;   *)
(* prepare/accept/commit steps represent eventual responses from an available*)
(* configuration-pinned quorum.  No such fairness is used by SafetySpec.     *)
(***************************************************************************)
SchedulingProgress ==
    CASE HasRecoveryPhase("prepare") -> PrepareFirstActive
      [] HasRecoveryPhase("accept") -> AcceptFirstActive
      [] HasRecoveryPhase("commit") -> CommitFirstActive
      [] OTHER -> CoverageDriveStep

SchedulingFairSpec ==
    /\ Init
    /\ [][SchedulingProgress]_Vars
    /\ WF_Vars(SchedulingProgress)

ExecutionReadyProgress ==
    IF pendingReady = <<>> /\ ~awaitAdvance
    THEN EmitAnyReady
    ELSE IF ~awaitAdvance
         THEN ExposeReady
         ELSE IF appliedReadyPrefix < Len(readyFrozen)
              THEN ApplyOne
              ELSE AdvancePrefix(appliedReadyPrefix)

ExecutionProgress ==
    CASE HasRecoveryPhase("prepare") -> PrepareFirstActive
      [] HasRecoveryPhase("accept") -> AcceptFirstActive
      [] HasRecoveryPhase("commit") -> CommitFirstActive
      [] HasCommittedInternal -> ExecuteFirstCommittedInternal
      [] ~FinitePrefixClosed -> CoverageDriveStep
      [] ~(Bases \subseteq durableExecuted) -> ExecutionReadyProgress
      [] OTHER -> FALSE

ExecutionFairSpec ==
    /\ Init
    /\ [][ExecutionProgress]_Vars
    /\ WF_Vars(ExecutionProgress)

InitialRecoveryObligations ==
    {key \in RecoveryKeys :
        /\ key.target \in CompactExactDeps(key.source.base)
        /\ key.target.lane = key.source.lane
        /\ InitialStatus[key.target] \notin AtLeastCommitted
        /\ ~(key.source.base = A /\ key.target = Old1Max)}

EveryPersistentSourceEventuallyScheduled ==
    <> (InitialRecoveryObligations \subseteq startedRecoveries)

EventuallyFinitePrefixExecutes ==
    <> (FinitePrefixClosed /\ Bases \subseteq durableExecuted)

=============================================================================
