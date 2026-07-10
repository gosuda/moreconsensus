---- MODULE EPaxosTimingReady ----
EXTENDS Naturals, Integers, FiniteSets, Sequences

(***************************************************************************)
(* Finite model of the repaired deterministic logical-time and Ready       *)
(* durability boundary. MaxTick is deliberately small in every config.     *)
(* The model checks mathematical, non-wrapping addition; one recurring     *)
(* logical timer; logical TimeOptimization ProcessAt; frozen complete       *)
(* Ready hard state; atomic storage success or failure; and crash/restart   *)
(* reconstruction at a full interval. Physical time is a tagged external   *)
(* domain and never participates in logical scheduling or hard state.       *)
(* This is finite evidence for these contracts, not an unbounded proof or   *)
(* a model of the complete EPaxos protocol, network, quorum, or TOQ clocks. *)
(***************************************************************************)

CONSTANTS MaxTick, FullInterval, ProcessDelay, MaxCrashes,
          ConfID, V1, V2, V3

ASSUME
    /\ MaxTick \in Nat
    /\ MaxTick > 0
    /\ FullInterval \in 1..(MaxTick + 1)
    /\ ProcessDelay \in 1..(MaxTick + 1)
    /\ MaxCrashes \in 0..1
    /\ ConfID \in Nat
    /\ V1 # V2
    /\ V1 # V3
    /\ V2 # V3

Ticks == 0..MaxTick
MaxGeneration == (MaxCrashes + 1) * (MaxTick + 1)
Generations == 0..MaxGeneration
MutationCounts == 0..(MaxTick + 1)

CompleteConf == [id |-> ConfID, voters |-> <<V1, V2, V3>>]
EmptyConf == [id |-> 0, voters |-> <<>>]
HardState(t) == [conf |-> CompleteConf, tick |-> t]
NoHardState == [conf |-> EmptyConf, tick |-> 0]

LogicalTime(t) == [domain |-> "logical", value |-> t]
PhysicalTime(t) == [domain |-> "physical", value |-> t]
NoTime == [domain |-> "none", value |-> 0]
LogicalTimes == {LogicalTime(t) : t \in Ticks}
PhysicalTimes == {PhysicalTime(t) : t \in Ticks}

RecordState(timerCount, timerNeeded, proposal, at, atBase, processDone,
            processTick, mutationTick) ==
    [timerMutations |-> timerCount,
     timerRequired |-> timerNeeded,
     proposalDone |-> proposal,
     processAt |-> at,
     processAtBase |-> atBase,
     processAtProcessed |-> processDone,
     processAtProcessedTick |-> processTick,
     lastTimerMutationTick |-> mutationTick]

(* The finite slice begins with one durable unfinished instance whose soft   *)
(* recurring timer has not yet been inserted into the volatile timer heap.  *)
EmptyRecord == RecordState(0, TRUE, FALSE, NoTime, 0, FALSE, 0, 0)

RecordOK(r) ==
    /\ r.timerMutations \in MutationCounts
    /\ r.timerRequired \in BOOLEAN
    /\ r.proposalDone \in BOOLEAN
    /\ r.processAt \in LogicalTimes \cup {NoTime}
    /\ r.processAtBase \in Ticks
    /\ r.processAtProcessed \in BOOLEAN
    /\ r.processAtProcessedTick \in Ticks
    /\ r.lastTimerMutationTick \in Ticks
    /\ (r.proposalDone =>
            /\ r.processAt = LogicalTime(r.processAtBase + ProcessDelay)
            /\ r.processAtBase + ProcessDelay <= MaxTick)
    /\ (~r.proposalDone =>
            /\ r.processAt = NoTime
            /\ r.processAtBase = 0)
    /\ (r.processAtProcessed =>
            /\ r.proposalDone
            /\ r.processAtProcessedTick = r.processAt.value)
    /\ (~r.processAtProcessed => r.processAtProcessedTick = 0)
    /\ (r.timerMutations = 0 => r.lastTimerMutationTick = 0)

FireKey(g, t) == [generation |-> g, tick |-> t]
FireKeys == {FireKey(g, t) : g \in Generations, t \in Ticks}

VARIABLES
    currentTick,
    durableTick,
    acknowledgedTick,
    running,
    crashCount,

    timerActive,
    timerBase,
    timerDeadline,
    timerGeneration,
    timerExhausted,
    reconstructed,
    timerMutations,
    timerRequired,
    lastTimerMutationTick,
    fireHistory,
    fireAttempts,

    proposalDone,
    processAt,
    processAtBase,
    processAtProcessed,
    processAtProcessedTick,

    awaitAdvance,
    readyHardState,
    frozenHasRecord,
    frozenRecord,
    readyPersisted,
    durableRecord,
    acknowledgedRecord,
    releasedTimerMutations,
    releasedProcessAt,

    storageFailureObserved,
    tickAtMaxObserved,
    timerOverflowObserved,
    processOverflowObserved,
    restartOverflowObserved,

    physicalNow,

    lastAction,
    beforeTick,
    beforeDurableTick,
    beforeProtocolMutations,
    beforeTimerActive,
    beforeTimerBase,
    beforeTimerDeadline,
    beforeTimerGeneration,
    beforeProcessAt,
    beforeRecord,
    beforeDurableRecord,
    beforeReadyHardState,
    beforeFrozenHasRecord,
    beforeFrozenRecord,
    beforePhysicalNow

TimingState ==
    <<currentTick, timerActive, timerBase, timerDeadline, timerGeneration,
      timerExhausted, reconstructed, timerMutations, timerRequired,
      lastTimerMutationTick, fireHistory, fireAttempts>>

ProposalState ==
    <<proposalDone, processAt, processAtBase, processAtProcessed,
      processAtProcessedTick>>

ReadyState ==
    <<awaitAdvance, readyHardState, frozenHasRecord, frozenRecord,
      readyPersisted>>

DurableState ==
    <<durableTick, acknowledgedTick, durableRecord, acknowledgedRecord,
      releasedTimerMutations, releasedProcessAt>>

ControlState == <<running, crashCount>>

ObservationState ==
    <<storageFailureObserved, tickAtMaxObserved, timerOverflowObserved,
      processOverflowObserved, restartOverflowObserved, physicalNow>>

HistoryState ==
    <<lastAction, beforeTick, beforeDurableTick, beforeProtocolMutations,
      beforeTimerActive, beforeTimerBase, beforeTimerDeadline,
      beforeTimerGeneration, beforeProcessAt, beforeRecord,
      beforeDurableRecord, beforeReadyHardState, beforeFrozenHasRecord,
      beforeFrozenRecord, beforePhysicalNow>>

Vars ==
    <<TimingState, ProposalState, ReadyState, DurableState, ControlState,
      ObservationState, HistoryState>>

CurrentRecord ==
    RecordState(timerMutations, timerRequired, proposalDone, processAt,
                processAtBase, processAtProcessed, processAtProcessedTick,
                lastTimerMutationTick)

ProtocolMutations ==
    timerMutations + (IF proposalDone THEN 1 ELSE 0) +
        (IF processAtProcessed THEN 1 ELSE 0)

PendingReady ==
    currentTick # acknowledgedTick \/ CurrentRecord # acknowledgedRecord

SuccessfulTickActions ==
    { "tick-no-fire",
      "tick-fire-reschedule",
      "tick-fire-exhausted-before-max",
      "tick-fire-exhausted-at-max" }

FireActions ==
    { "tick-fire-reschedule",
      "tick-fire-exhausted-before-max",
      "tick-fire-exhausted-at-max" }

RejectedSchedulingActions ==
    {"timer-schedule-overflow", "process-at-overflow"}

Remember(actionName) ==
    /\ lastAction' = actionName
    /\ beforeTick' = currentTick
    /\ beforeDurableTick' = durableTick
    /\ beforeProtocolMutations' = ProtocolMutations
    /\ beforeTimerActive' = timerActive
    /\ beforeTimerBase' = timerBase
    /\ beforeTimerDeadline' = timerDeadline
    /\ beforeTimerGeneration' = timerGeneration
    /\ beforeProcessAt' = processAt
    /\ beforeRecord' = CurrentRecord
    /\ beforeDurableRecord' = durableRecord
    /\ beforeReadyHardState' = readyHardState
    /\ beforeFrozenHasRecord' = frozenHasRecord
    /\ beforeFrozenRecord' = frozenRecord
    /\ beforePhysicalNow' = physicalNow

TypeOK ==
    /\ currentTick \in Ticks
    /\ durableTick \in Ticks
    /\ acknowledgedTick \in Ticks
    /\ running \in BOOLEAN
    /\ crashCount \in 0..MaxCrashes
    /\ timerActive \in BOOLEAN
    /\ timerBase \in Ticks
    /\ timerDeadline \in Ticks
    /\ timerGeneration \in Generations
    /\ timerExhausted \in BOOLEAN
    /\ reconstructed \in BOOLEAN
    /\ timerMutations \in MutationCounts
    /\ timerRequired \in BOOLEAN
    /\ lastTimerMutationTick \in Ticks
    /\ fireHistory \subseteq FireKeys
    /\ fireAttempts \in 0..MaxGeneration
    /\ proposalDone \in BOOLEAN
    /\ processAt \in LogicalTimes \cup {NoTime}
    /\ processAtBase \in Ticks
    /\ processAtProcessed \in BOOLEAN
    /\ processAtProcessedTick \in Ticks
    /\ awaitAdvance \in BOOLEAN
    /\ readyHardState \in {NoHardState} \cup {HardState(t) : t \in Ticks}
    /\ frozenHasRecord \in BOOLEAN
    /\ RecordOK(frozenRecord)
    /\ readyPersisted \in BOOLEAN
    /\ RecordOK(durableRecord)
    /\ RecordOK(acknowledgedRecord)
    /\ releasedTimerMutations \in MutationCounts
    /\ releasedProcessAt \in BOOLEAN
    /\ storageFailureObserved \in BOOLEAN
    /\ tickAtMaxObserved \in BOOLEAN
    /\ timerOverflowObserved \in BOOLEAN
    /\ processOverflowObserved \in BOOLEAN
    /\ restartOverflowObserved \in BOOLEAN
    /\ physicalNow \in PhysicalTimes
    /\ lastAction \in {
           "init", "timer-schedule-fit-below-max",
           "timer-schedule-exact-max", "timer-schedule-overflow",
           "process-at-fit-below-max", "process-at-exact-max",
           "process-at-overflow", "process-at-due", "tick-no-fire",
           "tick-fire-reschedule", "tick-fire-exhausted-before-max",
           "tick-fire-exhausted-at-max", "tick-at-max-error",
           "ready-expose", "storage-failure", "storage-success",
           "release-durable-effect", "release-process-at-effect",
           "ready-advance", "crash-before-persist",
           "crash-after-persist", "restart-full-interval",
           "restart-without-timer", "restart-deadline-overflow",
           "physical-advance"}
    /\ beforeTick \in Ticks
    /\ beforeDurableTick \in Ticks
    /\ beforeProtocolMutations \in 0..(MaxTick + 3)
    /\ beforeTimerActive \in BOOLEAN
    /\ beforeTimerBase \in Ticks
    /\ beforeTimerDeadline \in Ticks
    /\ beforeTimerGeneration \in Generations
    /\ beforeProcessAt \in LogicalTimes \cup {NoTime}
    /\ RecordOK(beforeRecord)
    /\ RecordOK(beforeDurableRecord)
    /\ beforeReadyHardState \in {NoHardState} \cup {HardState(t) : t \in Ticks}
    /\ beforeFrozenHasRecord \in BOOLEAN
    /\ RecordOK(beforeFrozenRecord)
    /\ beforePhysicalNow \in PhysicalTimes
    /\ RecordOK(CurrentRecord)

Init ==
    /\ currentTick = 0
    /\ durableTick = 0
    /\ acknowledgedTick = 0
    /\ running = TRUE
    /\ crashCount = 0
    /\ timerActive = FALSE
    /\ timerBase = 0
    /\ timerDeadline = 0
    /\ timerGeneration = 0
    /\ timerExhausted = FALSE
    /\ reconstructed = FALSE
    /\ timerMutations = 0
    /\ timerRequired = TRUE
    /\ lastTimerMutationTick = 0
    /\ fireHistory = {}
    /\ fireAttempts = 0
    /\ proposalDone = FALSE
    /\ processAt = NoTime
    /\ processAtBase = 0
    /\ processAtProcessed = FALSE
    /\ processAtProcessedTick = 0
    /\ awaitAdvance = FALSE
    /\ readyHardState = NoHardState
    /\ frozenHasRecord = FALSE
    /\ frozenRecord = EmptyRecord
    /\ readyPersisted = FALSE
    /\ durableRecord = EmptyRecord
    /\ acknowledgedRecord = EmptyRecord
    /\ releasedTimerMutations = 0
    /\ releasedProcessAt = FALSE
    /\ storageFailureObserved = FALSE
    /\ tickAtMaxObserved = FALSE
    /\ timerOverflowObserved = FALSE
    /\ processOverflowObserved = FALSE
    /\ restartOverflowObserved = FALSE
    /\ physicalNow = PhysicalTime(0)
    /\ lastAction = "init"
    /\ beforeTick = 0
    /\ beforeDurableTick = 0
    /\ beforeProtocolMutations = 0
    /\ beforeTimerActive = FALSE
    /\ beforeTimerBase = 0
    /\ beforeTimerDeadline = 0
    /\ beforeTimerGeneration = 0
    /\ beforeProcessAt = NoTime
    /\ beforeRecord = EmptyRecord
    /\ beforeDurableRecord = EmptyRecord
    /\ beforeReadyHardState = NoHardState
    /\ beforeFrozenHasRecord = FALSE
    /\ beforeFrozenRecord = EmptyRecord
    /\ beforePhysicalNow = PhysicalTime(0)

DoScheduleTimerFit(actionName) ==
    /\ running
    /\ ~timerActive
    /\ ~timerExhausted
    /\ currentTick + FullInterval <= MaxTick
    /\ timerActive' = TRUE
    /\ timerBase' = currentTick
    /\ timerDeadline' = currentTick + FullInterval
    /\ Remember(actionName)
    /\ UNCHANGED <<currentTick, timerGeneration, timerExhausted,
                   reconstructed, timerMutations, timerRequired,
                   lastTimerMutationTick, fireHistory, fireAttempts,
                   ProposalState, ReadyState, DurableState, ControlState,
                   ObservationState>>

ScheduleTimerFitBelowMax ==
    /\ currentTick + FullInterval < MaxTick
    /\ DoScheduleTimerFit("timer-schedule-fit-below-max")

ScheduleTimerExactMax ==
    /\ currentTick + FullInterval = MaxTick
    /\ DoScheduleTimerFit("timer-schedule-exact-max")
ScheduleTimerOverflow ==
    /\ running
    /\ ~timerActive
    /\ ~timerExhausted
    /\ currentTick + FullInterval > MaxTick
    /\ ~timerOverflowObserved
    /\ timerExhausted' = TRUE
    /\ timerOverflowObserved' = TRUE
    /\ Remember("timer-schedule-overflow")
    /\ UNCHANGED <<currentTick, timerActive, timerBase, timerDeadline,
                   timerGeneration, reconstructed, timerMutations,
                   timerRequired, lastTimerMutationTick, fireHistory,
                   fireAttempts, ProposalState, ReadyState, DurableState,
                   ControlState, storageFailureObserved, tickAtMaxObserved,
                   processOverflowObserved, restartOverflowObserved,
                   physicalNow>>

DoScheduleProcessAtFit(actionName) ==
    /\ running
    /\ ~proposalDone
    /\ currentTick + ProcessDelay <= MaxTick
    /\ proposalDone' = TRUE
    /\ processAt' = LogicalTime(currentTick + ProcessDelay)
    /\ processAtBase' = currentTick
    /\ Remember(actionName)
    /\ UNCHANGED <<processAtProcessed, processAtProcessedTick,
                   TimingState, ReadyState, DurableState, ControlState,
                   ObservationState>>

ScheduleProcessAtFitBelowMax ==
    /\ currentTick + ProcessDelay < MaxTick
    /\ DoScheduleProcessAtFit("process-at-fit-below-max")

ScheduleProcessAtExactMax ==
    /\ currentTick + ProcessDelay = MaxTick
    /\ DoScheduleProcessAtFit("process-at-exact-max")
ScheduleProcessAtOverflow ==
    /\ running
    /\ ~proposalDone
    /\ currentTick + ProcessDelay > MaxTick
    /\ ~processOverflowObserved
    /\ processOverflowObserved' = TRUE
    /\ Remember("process-at-overflow")
    /\ UNCHANGED <<TimingState, ProposalState, ReadyState, DurableState,
                   ControlState, storageFailureObserved, tickAtMaxObserved,
                   timerOverflowObserved, restartOverflowObserved,
                   physicalNow>>

ProcessAtDue ==
    /\ running
    /\ proposalDone
    /\ ~processAtProcessed
    /\ currentTick = processAt.value
    /\ processAtProcessed' = TRUE
    /\ processAtProcessedTick' = currentTick
    /\ Remember("process-at-due")
    /\ UNCHANGED <<TimingState, proposalDone, processAt, processAtBase,
                   ReadyState, DurableState, ControlState, ObservationState>>

TickNoFire ==
    /\ running
    /\ currentTick < MaxTick
    /\ (~proposalDone \/ processAtProcessed \/
            currentTick + 1 <= processAt.value)
    /\ \/ ~timerActive
       \/ currentTick + 1 < timerDeadline
    /\ currentTick' = currentTick + 1
    /\ Remember("tick-no-fire")
    /\ UNCHANGED <<timerActive, timerBase, timerDeadline, timerGeneration,
                   timerExhausted, reconstructed, timerMutations,
                   timerRequired, lastTimerMutationTick, fireHistory,
                   fireAttempts, ProposalState, ReadyState, DurableState,
                   ControlState, ObservationState>>

TickFireAndReschedule ==
    LET nextTick == currentTick + 1
        fire == FireKey(timerGeneration, nextTick)
    IN
    /\ running
    /\ currentTick < MaxTick
    /\ (~proposalDone \/ processAtProcessed \/ nextTick <= processAt.value)
    /\ timerActive
    /\ nextTick = timerDeadline
    /\ nextTick + FullInterval <= MaxTick
    /\ timerGeneration < MaxGeneration
    /\ fire \notin fireHistory
    /\ currentTick' = nextTick
    /\ timerActive' = TRUE
    /\ timerBase' = nextTick
    /\ timerDeadline' = nextTick + FullInterval
    /\ timerGeneration' = timerGeneration + 1
    /\ timerExhausted' = FALSE
    /\ timerMutations' = timerMutations + 1
    /\ timerRequired' = TRUE
    /\ lastTimerMutationTick' = nextTick
    /\ fireHistory' = fireHistory \cup {fire}
    /\ fireAttempts' = fireAttempts + 1
    /\ Remember("tick-fire-reschedule")
    /\ UNCHANGED <<reconstructed, ProposalState, ReadyState, DurableState,
                   ControlState, ObservationState>>

DoTickFireWithoutSuccessor(actionName) ==
    LET nextTick == currentTick + 1
        fire == FireKey(timerGeneration, nextTick)
    IN
    /\ running
    /\ currentTick < MaxTick
    /\ (~proposalDone \/ processAtProcessed \/ nextTick <= processAt.value)
    /\ timerActive
    /\ nextTick = timerDeadline
    /\ nextTick + FullInterval > MaxTick
    /\ timerGeneration < MaxGeneration
    /\ fire \notin fireHistory
    /\ currentTick' = nextTick
    /\ timerActive' = FALSE
    /\ timerBase' = 0
    /\ timerDeadline' = 0
    /\ timerGeneration' = timerGeneration + 1
    /\ timerExhausted' = TRUE
    /\ timerMutations' = timerMutations + 1
    /\ timerRequired' = FALSE
    /\ lastTimerMutationTick' = nextTick
    /\ fireHistory' = fireHistory \cup {fire}
    /\ fireAttempts' = fireAttempts + 1
    /\ timerOverflowObserved' = TRUE
    /\ Remember(actionName)
    /\ UNCHANGED <<reconstructed, ProposalState, ReadyState, DurableState,
                   ControlState, storageFailureObserved, tickAtMaxObserved,
                   processOverflowObserved, restartOverflowObserved,
                   physicalNow>>

TickFireWithoutSuccessorBeforeMax ==
    /\ currentTick + 1 < MaxTick
    /\ DoTickFireWithoutSuccessor("tick-fire-exhausted-before-max")

TickFireAtMaxWithoutSuccessor ==
    /\ currentTick + 1 = MaxTick
    /\ DoTickFireWithoutSuccessor("tick-fire-exhausted-at-max")
TickAtMaxError ==
    /\ running
    /\ currentTick = MaxTick
    /\ ~tickAtMaxObserved
    /\ tickAtMaxObserved' = TRUE
    /\ Remember("tick-at-max-error")
    /\ UNCHANGED <<TimingState, ProposalState, ReadyState, DurableState,
                   ControlState, storageFailureObserved,
                   timerOverflowObserved, processOverflowObserved,
                   restartOverflowObserved, physicalNow>>

ExposeReady ==
    /\ running
    /\ ~awaitAdvance
    /\ PendingReady
    /\ awaitAdvance' = TRUE
    /\ readyHardState' = HardState(currentTick)
    /\ frozenHasRecord' = (CurrentRecord # acknowledgedRecord)
    /\ frozenRecord' = CurrentRecord
    /\ readyPersisted' = FALSE
    /\ Remember("ready-expose")
    /\ UNCHANGED <<TimingState, ProposalState, DurableState, ControlState,
                   ObservationState>>

PersistReadyFailure ==
    /\ running
    /\ awaitAdvance
    /\ ~readyPersisted
    /\ ~storageFailureObserved
    /\ storageFailureObserved' = TRUE
    /\ Remember("storage-failure")
    /\ UNCHANGED <<TimingState, ProposalState, ReadyState, DurableState,
                   ControlState, tickAtMaxObserved, timerOverflowObserved,
                   processOverflowObserved, restartOverflowObserved,
                   physicalNow>>

PersistReadySuccess ==
    /\ running
    /\ awaitAdvance
    /\ ~readyPersisted
    /\ readyHardState.tick >= durableTick
    /\ durableTick' = readyHardState.tick
    /\ durableRecord' =
           IF frozenHasRecord THEN frozenRecord ELSE durableRecord
    /\ readyPersisted' = TRUE
    /\ Remember("storage-success")
    /\ UNCHANGED <<TimingState, ProposalState, awaitAdvance,
                   readyHardState, frozenHasRecord, frozenRecord,
                   acknowledgedTick, acknowledgedRecord,
                   releasedTimerMutations, releasedProcessAt,
                   ControlState, ObservationState>>

ReleaseDurableTimerEffect ==
    /\ running
    /\ releasedTimerMutations < durableRecord.timerMutations
    /\ releasedTimerMutations' = releasedTimerMutations + 1
    /\ Remember("release-durable-effect")
    /\ UNCHANGED <<TimingState, ProposalState, ReadyState, durableTick,
                   acknowledgedTick, durableRecord, acknowledgedRecord,
                   releasedProcessAt, ControlState, ObservationState>>

ReleaseDurableProcessAtEffect ==
    /\ running
    /\ durableRecord.processAtProcessed
    /\ ~releasedProcessAt
    /\ releasedProcessAt' = TRUE
    /\ Remember("release-process-at-effect")
    /\ UNCHANGED <<TimingState, ProposalState, ReadyState, durableTick,
                   acknowledgedTick, durableRecord, acknowledgedRecord,
                   releasedTimerMutations, ControlState, ObservationState>>

AdvanceReady ==
    /\ running
    /\ awaitAdvance
    /\ readyPersisted
    /\ durableTick = readyHardState.tick
    /\ (~frozenHasRecord \/ durableRecord = frozenRecord)
    /\ acknowledgedTick' = readyHardState.tick
    /\ acknowledgedRecord' =
           IF frozenHasRecord THEN frozenRecord ELSE acknowledgedRecord
    /\ awaitAdvance' = FALSE
    /\ readyHardState' = NoHardState
    /\ frozenHasRecord' = FALSE
    /\ frozenRecord' = EmptyRecord
    /\ readyPersisted' = FALSE
    /\ Remember("ready-advance")
    /\ UNCHANGED <<TimingState, ProposalState, durableTick, durableRecord,
                   releasedTimerMutations, releasedProcessAt,
                   ControlState, ObservationState>>

DoCrash(actionName) ==
    /\ running
    /\ crashCount < MaxCrashes
    /\ running' = FALSE
    /\ crashCount' = crashCount + 1
    /\ currentTick' = durableTick
    /\ acknowledgedTick' = durableTick
    /\ timerActive' = FALSE
    /\ timerBase' = 0
    /\ timerDeadline' = 0
    /\ timerExhausted' = FALSE
    /\ reconstructed' = FALSE
    /\ timerMutations' = durableRecord.timerMutations
    /\ lastTimerMutationTick' = durableRecord.lastTimerMutationTick
    /\ timerRequired' = durableRecord.timerRequired
    /\ proposalDone' = durableRecord.proposalDone
    /\ processAt' = durableRecord.processAt
    /\ processAtBase' = durableRecord.processAtBase
    /\ processAtProcessed' = durableRecord.processAtProcessed
    /\ processAtProcessedTick' = durableRecord.processAtProcessedTick
    /\ acknowledgedRecord' = durableRecord
    /\ awaitAdvance' = FALSE
    /\ readyHardState' = NoHardState
    /\ frozenHasRecord' = FALSE
    /\ frozenRecord' = EmptyRecord
    /\ readyPersisted' = FALSE
    /\ Remember(actionName)
    /\ UNCHANGED <<timerGeneration, fireHistory, fireAttempts,
                   durableTick, durableRecord, releasedTimerMutations,
                   releasedProcessAt, ObservationState>>

CrashBeforePersistence ==
    /\ ~readyPersisted
    /\ (currentTick # durableTick \/ CurrentRecord # durableRecord)
    /\ DoCrash("crash-before-persist")

CrashAfterPersistence ==
    /\ awaitAdvance
    /\ readyPersisted
    /\ DoCrash("crash-after-persist")

RestartWithFullInterval ==
    /\ ~running
    /\ durableRecord.timerRequired
    /\ durableTick + FullInterval <= MaxTick
    /\ timerGeneration < MaxGeneration
    /\ running' = TRUE
    /\ currentTick' = durableTick
    /\ acknowledgedTick' = durableTick
    /\ timerActive' = TRUE
    /\ timerBase' = durableTick
    /\ timerDeadline' = durableTick + FullInterval
    /\ timerGeneration' = timerGeneration + 1
    /\ timerExhausted' = FALSE
    /\ reconstructed' = TRUE
    /\ timerMutations' = durableRecord.timerMutations
    /\ lastTimerMutationTick' = durableRecord.lastTimerMutationTick
    /\ timerRequired' = durableRecord.timerRequired
    /\ proposalDone' = durableRecord.proposalDone
    /\ processAt' = durableRecord.processAt
    /\ processAtBase' = durableRecord.processAtBase
    /\ processAtProcessed' = durableRecord.processAtProcessed
    /\ processAtProcessedTick' = durableRecord.processAtProcessedTick
    /\ acknowledgedRecord' = durableRecord
    /\ Remember("restart-full-interval")
    /\ UNCHANGED <<fireHistory, fireAttempts, ReadyState, durableTick,
                   durableRecord, releasedTimerMutations, releasedProcessAt,
                   crashCount, ObservationState>>

RestartWithoutTimer ==
    /\ ~running
    /\ ~durableRecord.timerRequired
    /\ running' = TRUE
    /\ currentTick' = durableTick
    /\ acknowledgedTick' = durableTick
    /\ timerActive' = FALSE
    /\ timerBase' = 0
    /\ timerDeadline' = 0
    /\ timerExhausted' = TRUE
    /\ reconstructed' = FALSE
    /\ timerMutations' = durableRecord.timerMutations
    /\ timerRequired' = FALSE
    /\ lastTimerMutationTick' = durableRecord.lastTimerMutationTick
    /\ proposalDone' = durableRecord.proposalDone
    /\ processAt' = durableRecord.processAt
    /\ processAtBase' = durableRecord.processAtBase
    /\ processAtProcessed' = durableRecord.processAtProcessed
    /\ processAtProcessedTick' = durableRecord.processAtProcessedTick
    /\ acknowledgedRecord' = durableRecord
    /\ Remember("restart-without-timer")
    /\ UNCHANGED <<timerGeneration, fireHistory, fireAttempts, ReadyState,
                   durableTick, durableRecord, releasedTimerMutations,
                   releasedProcessAt, crashCount, ObservationState>>

RestartDeadlineOverflow ==
    /\ ~running
    /\ durableRecord.timerRequired
    /\ durableTick + FullInterval > MaxTick
    /\ ~restartOverflowObserved
    /\ restartOverflowObserved' = TRUE
    /\ Remember("restart-deadline-overflow")
    /\ UNCHANGED <<TimingState, ProposalState, ReadyState, DurableState,
                   ControlState, storageFailureObserved, tickAtMaxObserved,
                   timerOverflowObserved, processOverflowObserved,
                   physicalNow>>

AdvancePhysicalClock ==
    /\ physicalNow.value < MaxTick
    /\ physicalNow' = PhysicalTime(physicalNow.value + 1)
    /\ Remember("physical-advance")
    /\ UNCHANGED <<TimingState, ProposalState, ReadyState, DurableState,
                   ControlState, storageFailureObserved, tickAtMaxObserved,
                   timerOverflowObserved, processOverflowObserved,
                   restartOverflowObserved>>

SuccessfulTick ==
    \/ TickNoFire
    \/ TickFireAndReschedule
    \/ TickFireWithoutSuccessorBeforeMax
    \/ TickFireAtMaxWithoutSuccessor

Next ==
    \/ ScheduleTimerFitBelowMax
    \/ ScheduleTimerExactMax
    \/ ScheduleTimerOverflow
    \/ ScheduleProcessAtFitBelowMax
    \/ ScheduleProcessAtExactMax
    \/ ScheduleProcessAtOverflow
    \/ ProcessAtDue
    \/ SuccessfulTick
    \/ TickAtMaxError
    \/ ExposeReady
    \/ PersistReadyFailure
    \/ PersistReadySuccess
    \/ ReleaseDurableTimerEffect
    \/ ReleaseDurableProcessAtEffect
    \/ AdvanceReady
    \/ CrashBeforePersistence
    \/ CrashAfterPersistence
    \/ RestartWithFullInterval
    \/ RestartWithoutTimer
    \/ RestartDeadlineOverflow
    \/ AdvancePhysicalClock

NoLogicalWrap ==
    /\ currentTick <= MaxTick
    /\ durableTick <= MaxTick
    /\ acknowledgedTick <= MaxTick
    /\ (~timerActive \/
            /\ timerBase < timerDeadline
            /\ timerDeadline <= MaxTick)
    /\ (~proposalDone \/ processAt.value <= MaxTick)
    /\ (~timerActive \/ timerRequired)

SuccessfulTickStrictlyIncreases ==
    lastAction \in SuccessfulTickActions =>
        /\ beforeTick < MaxTick
        /\ currentTick = beforeTick + 1
        /\ currentTick > beforeTick

CheckedDeadlineExactOrNoProtocolMutation ==
    /\ (timerActive =>
            /\ timerDeadline = timerBase + FullInterval
            /\ timerDeadline <= MaxTick)
    /\ (proposalDone =>
            /\ processAt = LogicalTime(processAtBase + ProcessDelay)
            /\ processAtBase + ProcessDelay <= MaxTick)
    /\ (processAtProcessed =>
            /\ proposalDone
            /\ processAtProcessedTick = processAt.value)
    /\ (lastAction \in RejectedSchedulingActions =>
            /\ currentTick = beforeTick
            /\ ProtocolMutations = beforeProtocolMutations
            /\ CurrentRecord = beforeRecord
            /\ timerActive = beforeTimerActive
            /\ timerBase = beforeTimerBase
            /\ timerDeadline = beforeTimerDeadline
            /\ timerGeneration = beforeTimerGeneration
            /\ processAt = beforeProcessAt)
    /\ (lastAction = "timer-schedule-overflow" =>
            /\ timerOverflowObserved
            /\ timerExhausted)
    /\ (lastAction = "process-at-overflow" => processOverflowObserved)

TimerNeverFiresEarly ==
    lastAction \in FireActions =>
        /\ beforeTimerActive
        /\ beforeTimerDeadline = beforeTick + 1
        /\ currentTick = beforeTimerDeadline

LogicalProcessAtNeverRunsEarly ==
    /\ (~proposalDone \/ processAtProcessed \/
            currentTick <= processAt.value)
    /\ (lastAction = "process-at-due" =>
            /\ beforeTick = beforeProcessAt.value
            /\ currentTick = beforeTick
            /\ processAtProcessed
            /\ processAtProcessedTick = currentTick)

AtMostOncePerGenerationAndTick ==
    /\ Cardinality(fireHistory) = fireAttempts
    /\ (lastAction \in FireActions =>
            FireKey(beforeTimerGeneration, currentTick) \in fireHistory)

NoSameTickRescheduleLoop ==
    /\ (timerActive => timerDeadline > currentTick)
    /\ (lastAction = "tick-fire-reschedule" =>
            /\ timerGeneration = beforeTimerGeneration + 1
            /\ timerBase = currentTick
            /\ timerDeadline = currentTick + FullInterval
            /\ timerDeadline > currentTick)

CounterExhaustionFailsClosed ==
    /\ (lastAction = "tick-at-max-error" =>
            /\ beforeTick = MaxTick
            /\ currentTick = beforeTick
            /\ ProtocolMutations = beforeProtocolMutations
            /\ CurrentRecord = beforeRecord
            /\ timerActive = beforeTimerActive
            /\ timerBase = beforeTimerBase
            /\ timerDeadline = beforeTimerDeadline
            /\ timerGeneration = beforeTimerGeneration
            /\ tickAtMaxObserved)
    /\ (lastAction \in
            {"tick-fire-exhausted-before-max",
             "tick-fire-exhausted-at-max"} =>
            /\ timerExhausted
            /\ ~timerRequired
            /\ ~timerActive
            /\ timerDeadline = 0
            /\ timerMutations = beforeRecord.timerMutations + 1)
    /\ (lastAction = "restart-deadline-overflow" =>
            /\ restartOverflowObserved
            /\ ~running
            /\ currentTick = durableTick
            /\ CurrentRecord = durableRecord
            /\ ProtocolMutations = beforeProtocolMutations)

RestartUsesDurableTickAndFullInterval ==
    /\ (~running =>
            /\ currentTick = durableTick
            /\ CurrentRecord = durableRecord
            /\ ~timerActive)
    /\ (lastAction = "restart-full-interval" =>
            /\ durableRecord.timerRequired
            /\ currentTick = durableTick
            /\ timerBase = durableTick
            /\ timerDeadline = durableTick + FullInterval
            /\ timerDeadline <= MaxTick
            /\ reconstructed)
    /\ (lastAction = "restart-without-timer" =>
            /\ ~timerActive
            /\ ~timerRequired
            /\ ~durableRecord.timerRequired)

CompleteReadyHardStateIsDurable ==
    /\ acknowledgedTick <= durableTick
    /\ durableTick <= currentTick
    /\ (awaitAdvance =>
            /\ readyHardState = HardState(readyHardState.tick)
            /\ acknowledgedTick <= readyHardState.tick
            /\ readyHardState.tick <= currentTick
            /\ (frozenHasRecord => RecordOK(frozenRecord)))
    /\ (~awaitAdvance =>
            /\ readyHardState = NoHardState
            /\ ~frozenHasRecord
            /\ ~readyPersisted)
    /\ (readyPersisted =>
            /\ durableTick = readyHardState.tick
            /\ (~frozenHasRecord \/ durableRecord = frozenRecord))
    /\ durableRecord.lastTimerMutationTick <= durableTick
    /\ acknowledgedRecord.lastTimerMutationTick <= acknowledgedTick
    /\ releasedTimerMutations <= durableRecord.timerMutations
    /\ durableRecord.processAtProcessedTick <= durableTick
    /\ acknowledgedRecord.processAtProcessedTick <= acknowledgedTick
    /\ (releasedProcessAt => durableRecord.processAtProcessed)

StorageFailureIsAtomic ==
    lastAction = "storage-failure" =>
        /\ durableTick = beforeDurableTick
        /\ durableRecord = beforeDurableRecord
        /\ CurrentRecord = beforeRecord
        /\ readyHardState = beforeReadyHardState
        /\ frozenHasRecord = beforeFrozenHasRecord
        /\ frozenRecord = beforeFrozenRecord
        /\ ~readyPersisted

ReadyRemainsFrozenWhileOutstanding ==
    lastAction \in SuccessfulTickActions \cup
                  {"timer-schedule-fit-below-max",
                   "timer-schedule-exact-max", "timer-schedule-overflow",
                   "process-at-fit-below-max", "process-at-exact-max",
                   "process-at-overflow", "process-at-due",
                   "physical-advance", "release-durable-effect",
                   "release-process-at-effect"} /\
    beforeReadyHardState # NoHardState =>
        /\ readyHardState = beforeReadyHardState
        /\ frozenHasRecord = beforeFrozenHasRecord
        /\ frozenRecord = beforeFrozenRecord

ClockDomainsAreNotConflated ==
    /\ physicalNow.domain = "physical"
    /\ currentTick \in Nat
    /\ durableTick \in Nat
    /\ (proposalDone => processAt.domain = "logical")
    /\ physicalNow # LogicalTime(currentTick)
    /\ (lastAction = "physical-advance" =>
            /\ currentTick = beforeTick
            /\ durableTick = beforeDurableTick
            /\ ProtocolMutations = beforeProtocolMutations
            /\ CurrentRecord = beforeRecord
            /\ processAt = beforeProcessAt)

Safety ==
    /\ NoLogicalWrap
    /\ SuccessfulTickStrictlyIncreases
    /\ CheckedDeadlineExactOrNoProtocolMutation
    /\ TimerNeverFiresEarly
    /\ LogicalProcessAtNeverRunsEarly
    /\ AtMostOncePerGenerationAndTick
    /\ NoSameTickRescheduleLoop
    /\ CounterExhaustionFailsClosed
    /\ RestartUsesDurableTickAndFullInterval
    /\ CompleteReadyHardStateIsDurable
    /\ StorageFailureIsAtomic
    /\ ReadyRemainsFrozenWhileOutstanding
    /\ ClockDomainsAreNotConflated

EventuallyTickExhaustionIsObserved == <>tickAtMaxObserved

\* This obligation begins only after a concrete timer generation is active.
ActiveRepresentableTimerEventuallyFires ==
    \A g \in Generations :
        \A d \in Ticks :
            (timerActive /\ timerGeneration = g /\ timerDeadline = d)
                ~> (FireKey(g, d) \in fireHistory)

LogicalProcessAtEventuallyRuns ==
    proposalDone ~> processAtProcessed

LogicalProcessAtEventuallyDurableAndReleased ==
    proposalDone ~> releasedProcessAt

Spec == Init /\ [][Next]_Vars

FairSpec ==
    /\ Spec
    /\ WF_Vars(SuccessfulTick)
    /\ WF_Vars(ProcessAtDue)
    /\ WF_Vars(ExposeReady)
    /\ WF_Vars(PersistReadySuccess)
    /\ WF_Vars(AdvanceReady)
    /\ WF_Vars(ReleaseDurableProcessAtEffect)
    /\ WF_Vars(TickAtMaxError)

====
