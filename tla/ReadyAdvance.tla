---- MODULE ReadyAdvance ----
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS R1, R2, X1, X2, M1, M2, C1, C2, MaxReadyMessages, MaxCrashes

VARIABLES
    records,
    hardStates,
    messages,
    committed,
    executedRecords,
    appliedCommitted,
    awaitAdvance,
    ready,
    durableRecords,
    durableHardState,
    persistedReadyPrefix,
    sentReadyPrefix,
    appliedReadyPrefix,
    sentHistory,
    durableApplied,
    running,
    crashCount,
    badAdvance,
    phase,
    lastCrashBoundary,
    crashSnapshotRecords,
    crashSnapshotHardState,
    crashSnapshotApplied,
    acknowledgedRecords,
    acknowledgedMessages,
    acknowledgedCommands

vars ==
    <<records, hardStates, messages, committed, executedRecords,
      appliedCommitted, awaitAdvance, ready, durableRecords,
      durableHardState, persistedReadyPrefix, sentReadyPrefix,
      appliedReadyPrefix, sentHistory, durableApplied, running,
      crashCount, badAdvance, phase, lastCrashBoundary,
      crashSnapshotRecords, crashSnapshotHardState,
      crashSnapshotApplied, acknowledgedRecords,
      acknowledgedMessages, acknowledgedCommands>>

RecordItems == {R1, R2, X1, X2}
MessageItems == {M1, M2}
CommandItems == {C1, C2}

OldHardState == [tick |-> 1, config |-> "old"]
NewHardState == [tick |-> 2, config |-> "new"]
EmptyHardState == [tick |-> 0, config |-> "none"]
HardStateItems == {OldHardState, NewHardState}

RecordSeqs == UNION {[1..n -> RecordItems] : n \in 0..4}
HardStateSeqs == UNION {[1..n -> HardStateItems] : n \in 0..4}
MessageSeqs == UNION {[1..n -> MessageItems] : n \in 0..2}
MessageHistorySeqs == UNION {[1..n -> MessageItems] : n \in 0..4}
CommandSeqs == UNION {[1..n -> CommandItems] : n \in 0..2}

InitialRecords == <<R1, R2>>
InitialHardStates == <<OldHardState, NewHardState>>
InitialMessages == <<M1, M2>>
InitialCommitted == <<C1, C2>>

ExecRecord(c) ==
    CASE c = C1 -> X1
      [] c = C2 -> X2

HardStateFor(r) ==
    CASE r = R1 -> OldHardState
      [] OTHER -> NewHardState

Drop(s, n) ==
    IF n = 0 THEN s
    ELSE IF n = Len(s) THEN <<>>
    ELSE SubSeq(s, n + 1, Len(s))

Take(s, n) ==
    IF n = 0 THEN <<>> ELSE SubSeq(s, 1, n)

SeqSet(s) == {s[i] : i \in 1..Len(s)}

ExecRecordsFor(cmdSeq) ==
    IF Len(cmdSeq) = 0 THEN <<>>
    ELSE [i \in 1..Len(cmdSeq) |-> ExecRecord(cmdSeq[i])]

VisibleMessages ==
    IF MaxReadyMessages = 0 \/ Len(messages) <= MaxReadyMessages
    THEN messages
    ELSE SubSeq(messages, 1, MaxReadyMessages)

PersistedItem(rec, hardState) ==
    [record |-> rec, hardState |-> hardState]

DurableItems ==
    UNION {{PersistedItem(rec, hardState) : hardState \in HardStateItems}
           : rec \in RecordItems}

InitialDurableRecords ==
    {PersistedItem(R1, OldHardState), PersistedItem(R2, NewHardState)}

ReadyRecordItems(rdy, n) ==
    {PersistedItem(rdy.recs[i], rdy.hardStates[i]) : i \in 1..n}

EmptyReady ==
    [recs |-> <<>>,
     hardStates |-> <<>>,
     msgs |-> <<>>,
     cmds |-> <<>>,
     mustSync |-> FALSE]

ReadyView ==
    [recs |-> records,
     hardStates |-> hardStates,
     msgs |-> VisibleMessages,
     cmds |-> committed,
     mustSync |-> Len(records) > 0]

PendingNonEmpty == Len(records) + Len(messages) + Len(committed) > 0

PersistedPrefixFor(recs, states) ==
    Cardinality(
        {i \in 1..Len(recs) :
            PersistedItem(recs[i], states[i]) \in durableRecords})

SentPrefixFor(msgs) ==
    Cardinality({i \in 1..Len(msgs) : msgs[i] \in SeqSet(sentHistory)})

AppliedPrefixFor(cmds) ==
    Cardinality({i \in 1..Len(cmds) : cmds[i] \in durableApplied})

ExposurePhase ==
    LET persisted == PersistedPrefixFor(records, hardStates)
        sent == SentPrefixFor(VisibleMessages)
        applied == AppliedPrefixFor(committed)
    IN
        IF Len(records) > 0 /\ persisted = 0
        THEN "before-persistence"
        ELSE IF persisted > 0 /\ persisted < Len(records)
        THEN "after-partial-persistence"
        ELSE IF Len(records) > 0
                /\ persisted = Len(records)
                /\ sent = 0
                /\ applied = 0
        THEN "after-full-persistence-before-send"
        ELSE "working"

CrashBoundaries ==
    {"before-persistence",
     "after-partial-persistence",
     "after-full-persistence-before-send",
     "after-send-before-apply",
     "after-apply-before-advance",
     "after-partial-advance"}

PhaseValues ==
    CrashBoundaries \cup
        {"idle", "working", "crashed", "restarted", "after-full-advance"}

TypeOK ==
    /\ MaxReadyMessages \in Nat
    /\ MaxCrashes \in 0..1
    /\ records \in RecordSeqs
    /\ hardStates \in HardStateSeqs
    /\ messages \in MessageSeqs
    /\ committed \in CommandSeqs
    /\ executedRecords \in RecordSeqs
    /\ appliedCommitted \in CommandSeqs
    /\ awaitAdvance \in BOOLEAN
    /\ ready \in
        [recs : RecordSeqs,
         hardStates : HardStateSeqs,
         msgs : MessageSeqs,
         cmds : CommandSeqs,
         mustSync : BOOLEAN]
    /\ durableRecords \in SUBSET DurableItems
    /\ durableHardState \in HardStateItems \cup {EmptyHardState}
    /\ persistedReadyPrefix \in 0..4
    /\ sentReadyPrefix \in 0..2
    /\ appliedReadyPrefix \in 0..2
    /\ sentHistory \in MessageHistorySeqs
    /\ durableApplied \in SUBSET CommandItems
    /\ running \in BOOLEAN
    /\ crashCount \in 0..MaxCrashes
    /\ badAdvance \in BOOLEAN
    /\ phase \in PhaseValues
    /\ lastCrashBoundary \in CrashBoundaries \cup {"none"}
    /\ crashSnapshotRecords \in SUBSET DurableItems
    /\ crashSnapshotHardState \in HardStateItems \cup {EmptyHardState}
    /\ crashSnapshotApplied \in SUBSET CommandItems
    /\ acknowledgedRecords \in SUBSET DurableItems
    /\ acknowledgedMessages \in SUBSET MessageItems
    /\ acknowledgedCommands \in SUBSET CommandItems

Init ==
    /\ records = InitialRecords
    /\ hardStates = InitialHardStates
    /\ messages = InitialMessages
    /\ committed = InitialCommitted
    /\ executedRecords = <<>>
    /\ appliedCommitted = <<>>
    /\ awaitAdvance = FALSE
    /\ ready = EmptyReady
    /\ durableRecords = {}
    /\ durableHardState = EmptyHardState
    /\ persistedReadyPrefix = 0
    /\ sentReadyPrefix = 0
    /\ appliedReadyPrefix = 0
    /\ sentHistory = <<>>
    /\ durableApplied = {}
    /\ running = TRUE
    /\ crashCount = 0
    /\ badAdvance = FALSE
    /\ phase = "idle"
    /\ lastCrashBoundary = "none"
    /\ crashSnapshotRecords = {}
    /\ crashSnapshotHardState = EmptyHardState
    /\ crashSnapshotApplied = {}
    /\ acknowledgedRecords = {}
    /\ acknowledgedMessages = {}
    /\ acknowledgedCommands = {}

ExposeReady ==
    /\ running
    /\ ~awaitAdvance
    /\ PendingNonEmpty
    /\ ready' = ReadyView
    /\ awaitAdvance' = TRUE
    /\ persistedReadyPrefix' = PersistedPrefixFor(records, hardStates)
    /\ sentReadyPrefix' = SentPrefixFor(VisibleMessages)
    /\ appliedReadyPrefix' = AppliedPrefixFor(committed)
    /\ phase' = ExposurePhase
    /\ UNCHANGED
        <<records, hardStates, messages, committed, executedRecords,
          appliedCommitted, durableRecords, durableHardState, sentHistory,
          durableApplied, running, crashCount, badAdvance,
          lastCrashBoundary, crashSnapshotRecords,
          crashSnapshotHardState, crashSnapshotApplied,
          acknowledgedRecords, acknowledgedMessages,
          acknowledgedCommands>>

PersistReadyRecord ==
    LET next == persistedReadyPrefix + 1
        item == PersistedItem(ready.recs[next], ready.hardStates[next])
    IN
        /\ running
        /\ awaitAdvance
        /\ persistedReadyPrefix < Len(ready.recs)
        /\ ready.hardStates[next].tick >= durableHardState.tick
        /\ durableRecords' = durableRecords \cup {item}
        /\ durableHardState' = ready.hardStates[next]
        /\ persistedReadyPrefix' = next
        /\ phase' =
            IF next < Len(ready.recs)
            THEN "after-partial-persistence"
            ELSE IF sentReadyPrefix = 0 /\ appliedReadyPrefix = 0
            THEN "after-full-persistence-before-send"
            ELSE "working"
        /\ UNCHANGED
            <<records, hardStates, messages, committed, executedRecords,
              appliedCommitted, awaitAdvance, ready, sentReadyPrefix,
              appliedReadyPrefix, sentHistory, durableApplied, running,
              crashCount, badAdvance, lastCrashBoundary,
              crashSnapshotRecords, crashSnapshotHardState,
              crashSnapshotApplied, acknowledgedRecords,
              acknowledgedMessages, acknowledgedCommands>>

SendReadyMessage ==
    LET next == sentReadyPrefix + 1
    IN
        /\ running
        /\ awaitAdvance
        /\ persistedReadyPrefix = Len(ready.recs)
        /\ sentReadyPrefix < Len(ready.msgs)
        /\ sentHistory' = Append(sentHistory, ready.msgs[next])
        /\ sentReadyPrefix' = next
        /\ phase' =
            IF appliedReadyPrefix = 0
            THEN "after-send-before-apply"
            ELSE "working"
        /\ UNCHANGED
            <<records, hardStates, messages, committed, executedRecords,
              appliedCommitted, awaitAdvance, ready, durableRecords,
              durableHardState, persistedReadyPrefix, appliedReadyPrefix,
              durableApplied, running, crashCount, badAdvance,
              lastCrashBoundary, crashSnapshotRecords,
              crashSnapshotHardState, crashSnapshotApplied,
              acknowledgedRecords, acknowledgedMessages,
              acknowledgedCommands>>

ApplyReadyCommand ==
    LET next == appliedReadyPrefix + 1
        command == ready.cmds[next]
        executionRecord == ExecRecord(command)
    IN
        /\ running
        /\ awaitAdvance
        /\ persistedReadyPrefix = Len(ready.recs)
        /\ appliedReadyPrefix < Len(ready.cmds)
        /\ command \notin durableApplied
        /\ records' = Append(records, executionRecord)
        /\ hardStates' = Append(hardStates, HardStateFor(executionRecord))
        /\ executedRecords' = Append(executedRecords, executionRecord)
        /\ appliedCommitted' = Append(appliedCommitted, command)
        /\ durableApplied' = durableApplied \cup {command}
        /\ appliedReadyPrefix' = next
        /\ phase' = "after-apply-before-advance"
        /\ UNCHANGED
            <<messages, committed, awaitAdvance, ready, durableRecords,
              durableHardState, persistedReadyPrefix, sentReadyPrefix,
              sentHistory, running, crashCount, badAdvance,
              lastCrashBoundary, crashSnapshotRecords,
              crashSnapshotHardState, crashSnapshotApplied,
              acknowledgedRecords, acknowledgedMessages,
              acknowledgedCommands>>

ValidAdvance(rn, mn, cn) ==
    /\ awaitAdvance
    /\ rn \in 0..Len(ready.recs)
    /\ mn \in 0..Len(ready.msgs)
    /\ cn \in 0..Len(ready.cmds)
    /\ rn + mn + cn > 0
    /\ rn <= persistedReadyPrefix
    /\ mn <= sentReadyPrefix
    /\ cn <= appliedReadyPrefix
    /\ (mn > 0 \/ cn > 0) => rn = Len(ready.recs)

Advance ==
    \E rn \in 0..Len(ready.recs),
       mn \in 0..Len(ready.msgs),
       cn \in 0..Len(ready.cmds) :
        /\ running
        /\ ValidAdvance(rn, mn, cn)
        /\ records' = Drop(records, rn)
        /\ hardStates' = Drop(hardStates, rn)
        /\ messages' = Drop(messages, mn)
        /\ committed' = Drop(committed, cn)
        /\ acknowledgedRecords' =
            acknowledgedRecords \cup ReadyRecordItems(ready, rn)
        /\ acknowledgedMessages' =
            acknowledgedMessages \cup SeqSet(Take(ready.msgs, mn))
        /\ acknowledgedCommands' =
            acknowledgedCommands \cup SeqSet(Take(ready.cmds, cn))
        /\ ready' = EmptyReady
        /\ awaitAdvance' = FALSE
        /\ persistedReadyPrefix' = 0
        /\ sentReadyPrefix' = 0
        /\ appliedReadyPrefix' = 0
        /\ phase' =
            IF rn < Len(ready.recs)
                \/ mn < Len(ready.msgs)
                \/ cn < Len(ready.cmds)
            THEN "after-partial-advance"
            ELSE "after-full-advance"
        /\ UNCHANGED
            <<executedRecords, appliedCommitted, durableRecords,
              durableHardState, sentHistory, durableApplied, running,
              crashCount, badAdvance, lastCrashBoundary,
              crashSnapshotRecords, crashSnapshotHardState,
              crashSnapshotApplied>>

RejectIncompleteAdvance ==
    \E rn \in 0..Len(ready.recs),
       mn \in 0..Len(ready.msgs),
       cn \in 0..Len(ready.cmds) :
        /\ running
        /\ awaitAdvance
        /\ rn + mn + cn > 0
        /\ ~ValidAdvance(rn, mn, cn)
        /\ UNCHANGED vars

Crash ==
    /\ running
    /\ crashCount < MaxCrashes
    /\ phase \in CrashBoundaries
    /\ running' = FALSE
    /\ crashCount' = crashCount + 1
    /\ ready' = EmptyReady
    /\ awaitAdvance' = FALSE
    /\ persistedReadyPrefix' = 0
    /\ sentReadyPrefix' = 0
    /\ appliedReadyPrefix' = 0
    /\ phase' = "crashed"
    /\ lastCrashBoundary' = phase
    /\ crashSnapshotRecords' = durableRecords
    /\ crashSnapshotHardState' = durableHardState
    /\ crashSnapshotApplied' = durableApplied
    /\ UNCHANGED
        <<records, hardStates, messages, committed, executedRecords,
          appliedCommitted, durableRecords, durableHardState, sentHistory,
          durableApplied, badAdvance, acknowledgedRecords,
          acknowledgedMessages, acknowledgedCommands>>

Restart ==
    /\ ~running
    /\ running' = TRUE
    /\ ready' = IF PendingNonEmpty THEN ReadyView ELSE EmptyReady
    /\ awaitAdvance' = PendingNonEmpty
    /\ persistedReadyPrefix' =
        IF PendingNonEmpty
        THEN PersistedPrefixFor(records, hardStates)
        ELSE 0
    /\ sentReadyPrefix' =
        IF PendingNonEmpty THEN SentPrefixFor(VisibleMessages) ELSE 0
    /\ appliedReadyPrefix' =
        IF PendingNonEmpty THEN AppliedPrefixFor(committed) ELSE 0
    /\ phase' = IF PendingNonEmpty THEN ExposurePhase ELSE "restarted"
    /\ UNCHANGED
        <<records, hardStates, messages, committed, executedRecords,
          appliedCommitted, durableRecords, durableHardState, sentHistory,
          durableApplied, crashCount, badAdvance, lastCrashBoundary,
          crashSnapshotRecords, crashSnapshotHardState,
          crashSnapshotApplied, acknowledgedRecords,
          acknowledgedMessages, acknowledgedCommands>>

Next ==
    ExposeReady
    \/ PersistReadyRecord
    \/ SendReadyMessage
    \/ ApplyReadyCommand
    \/ Advance
    \/ RejectIncompleteAdvance
    \/ Crash
    \/ Restart

PendingQueueWellFormed ==
    /\ Len(records) = Len(hardStates)
    /\ \A i \in 1..Len(records) :
        hardStates[i] = HardStateFor(records[i])

FrozenReadyMatchesPending ==
    /\ Len(ready.recs) <= Len(records)
    /\ Len(ready.hardStates) = Len(ready.recs)
    /\ ready.recs = Take(records, Len(ready.recs))
    /\ ready.hardStates = Take(hardStates, Len(ready.hardStates))
    /\ \A i \in 1..Len(ready.recs) :
        ready.hardStates[i] = HardStateFor(ready.recs[i])
    /\ Len(ready.msgs) <= Len(messages)
    /\ ready.msgs = Take(messages, Len(ready.msgs))
    /\ Len(ready.cmds) <= Len(committed)
    /\ ready.cmds = Take(committed, Len(ready.cmds))

MustSyncIffRecordsExposed ==
    ready.mustSync = (Len(ready.recs) > 0)

ReadySnapshotStableUntilAdvanceOrCrash ==
    IF awaitAdvance
    THEN FrozenReadyMatchesPending
    ELSE ready = EmptyReady

ReadyPersistenceIsPrefix ==
    \A i \in 1..Len(ready.recs), j \in 1..Len(ready.recs) :
        i < j
        /\ PersistedItem(ready.recs[j], ready.hardStates[j])
            \in durableRecords
        => PersistedItem(ready.recs[i], ready.hardStates[i])
            \in durableRecords

ReadySendIsPrefix ==
    \A i \in 1..Len(ready.msgs), j \in 1..Len(ready.msgs) :
        i < j /\ ready.msgs[j] \in SeqSet(sentHistory)
        => ready.msgs[i] \in SeqSet(sentHistory)

ReadyApplyIsPrefix ==
    \A i \in 1..Len(ready.cmds), j \in 1..Len(ready.cmds) :
        i < j /\ ready.cmds[j] \in durableApplied
        => ready.cmds[i] \in durableApplied

ValidCompletedPrefixes ==
    /\ persistedReadyPrefix <= Len(ready.recs)
    /\ sentReadyPrefix <= Len(ready.msgs)
    /\ appliedReadyPrefix <= Len(ready.cmds)
    /\ ReadyPersistenceIsPrefix
    /\ ReadySendIsPrefix
    /\ ReadyApplyIsPrefix
    /\ IF awaitAdvance
       THEN
            /\ persistedReadyPrefix =
                PersistedPrefixFor(ready.recs, ready.hardStates)
            /\ sentReadyPrefix = SentPrefixFor(ready.msgs)
            /\ appliedReadyPrefix = AppliedPrefixFor(ready.cmds)
       ELSE
            /\ persistedReadyPrefix = 0
            /\ sentReadyPrefix = 0
            /\ appliedReadyPrefix = 0

PersistenceBeforeSend ==
    Len(sentHistory) = 0 \/ InitialDurableRecords \subseteq durableRecords

PersistenceBeforeApply ==
    Len(appliedCommitted) = 0
    \/ InitialDurableRecords \subseteq durableRecords

AdvanceAcknowledgesOnlyCompletedPrefixes ==
    /\ ~badAdvance
    /\ acknowledgedRecords \subseteq durableRecords
    /\ acknowledgedMessages \subseteq SeqSet(sentHistory)
    /\ acknowledgedCommands \subseteq durableApplied

HardStateTickAndConfigDurable ==
    /\ \A item \in durableRecords :
        /\ item.hardState = HardStateFor(item.record)
        /\ item.hardState.tick <= durableHardState.tick
    /\ durableHardState = EmptyHardState
       <=> durableRecords = {}
    /\ durableHardState # EmptyHardState
       => \E item \in durableRecords :
            item.hardState = durableHardState

CrashPreservesDurableRecordsAndAppliedMarkers ==
    /\ crashSnapshotRecords \subseteq durableRecords
    /\ crashSnapshotApplied \subseteq durableApplied
    /\ crashSnapshotHardState.tick <= durableHardState.tick
    /\ crashCount = 0 => lastCrashBoundary = "none"
    /\ crashCount > 0 => lastCrashBoundary \in CrashBoundaries

DurableUnappliedCommands ==
    IF InitialDurableRecords \subseteq durableRecords
    THEN CommandItems \ durableApplied
    ELSE {}

RestartReexposesDurableUnappliedObligations ==
    /\ DurableUnappliedCommands \subseteq SeqSet(committed)
    /\ running /\ awaitAdvance
       => DurableUnappliedCommands \subseteq SeqSet(ready.cmds)

AppliedAtMostOnce ==
    /\ Len(appliedCommitted) = Cardinality(SeqSet(appliedCommitted))
    /\ durableApplied = SeqSet(appliedCommitted)

ExecutedRecordMatchesAppliedCommand ==
    executedRecords = ExecRecordsFor(appliedCommitted)

ExpectedRecordItems ==
    InitialDurableRecords \cup
        {PersistedItem(rec, HardStateFor(rec)) :
            rec \in SeqSet(executedRecords)}

NoHiddenBatchAfterPartialFailure ==
    /\ \A item \in ExpectedRecordItems :
        item \in acknowledgedRecords
        \/ item.record \in SeqSet(records)
    /\ \A message \in MessageItems :
        message \in acknowledgedMessages
        \/ message \in SeqSet(messages)
    /\ \A command \in CommandItems :
        command \in acknowledgedCommands
        \/ command \in SeqSet(committed)

Safety ==
    /\ PendingQueueWellFormed
    /\ MustSyncIffRecordsExposed
    /\ ReadySnapshotStableUntilAdvanceOrCrash
    /\ ValidCompletedPrefixes
    /\ PersistenceBeforeSend
    /\ PersistenceBeforeApply
    /\ AdvanceAcknowledgesOnlyCompletedPrefixes
    /\ HardStateTickAndConfigDurable
    /\ CrashPreservesDurableRecordsAndAppliedMarkers
    /\ RestartReexposesDurableUnappliedObligations
    /\ AppliedAtMostOnce
    /\ ExecutedRecordMatchesAppliedCommand
    /\ NoHiddenBatchAfterPartialFailure

Spec == Init /\ [][Next]_vars

====
