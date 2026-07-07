---- MODULE ReadyAdvance ----
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS R1, R2, X1, X2, M1, M2, C1, C2, MaxReadyMessages

VARIABLES records, messages, committed, executedRecords, appliedCommitted, awaitAdvance, ready, barrierOK

RecordItems == {R1, R2, X1, X2}
MessageItems == {M1, M2}
CommandItems == {C1, C2}

RecordSeqs == UNION {[1..n -> RecordItems] : n \in 0..4}
MessageSeqs == UNION {[1..n -> MessageItems] : n \in 0..2}
CommandSeqs == UNION {[1..n -> CommandItems] : n \in 0..2}

InitialRecords == <<R1, R2>>
InitialMessages == <<M1, M2>>
InitialCommitted == <<C1, C2>>

ExecRecord(c) ==
    CASE c = C1 -> X1
      [] c = C2 -> X2

Drop(s, n) ==
    IF n = 0 THEN s
    ELSE IF n = Len(s) THEN <<>>
    ELSE SubSeq(s, n + 1, Len(s))

Take(s, n) ==
    IF n = 0 THEN <<>> ELSE SubSeq(s, 1, n)

ExecRecordsFor(cmdSeq) ==
    IF Len(cmdSeq) = 0 THEN <<>>
    ELSE [i \in 1..Len(cmdSeq) |-> ExecRecord(cmdSeq[i])]

VisibleMessages ==
    IF MaxReadyMessages = 0 \/ Len(messages) <= MaxReadyMessages THEN messages
    ELSE SubSeq(messages, 1, MaxReadyMessages)

EmptyReady == [recs |-> <<>>, msgs |-> <<>>, cmds |-> <<>>, mustSync |-> FALSE]
ReadyView == [recs |-> records, msgs |-> VisibleMessages, cmds |-> committed, mustSync |-> Len(records) > 0]

PendingNonEmpty == Len(records) + Len(messages) + Len(committed) > 0

TypeOK ==
    /\ MaxReadyMessages \in Nat
    /\ records \in RecordSeqs
    /\ messages \in MessageSeqs
    /\ committed \in CommandSeqs
    /\ executedRecords \in RecordSeqs
    /\ appliedCommitted \in CommandSeqs
    /\ awaitAdvance \in BOOLEAN
    /\ ready \in [recs : RecordSeqs, msgs : MessageSeqs, cmds : CommandSeqs, mustSync : BOOLEAN]
    /\ barrierOK \in BOOLEAN

Init ==
    /\ records = InitialRecords
    /\ messages = InitialMessages
    /\ committed = InitialCommitted
    /\ executedRecords = <<>>
    /\ appliedCommitted = <<>>
    /\ awaitAdvance = FALSE
    /\ ready = EmptyReady
    /\ barrierOK = TRUE

Ready ==
    /\ ~awaitAdvance
    /\ PendingNonEmpty
    /\ ready' = ReadyView
    /\ awaitAdvance' = TRUE
    /\ UNCHANGED <<records, messages, committed, executedRecords, appliedCommitted, barrierOK>>

Advance ==
    \E rn \in 0..Len(ready.recs), mn \in 0..Len(ready.msgs), cn \in 0..Len(ready.cmds) :
        /\ awaitAdvance
        /\ rn + mn + cn > 0
        /\ (mn > 0 \/ cn > 0) => rn = Len(ready.recs)
        /\ records' = Drop(records, rn) \o ExecRecordsFor(Take(ready.cmds, cn))
        /\ messages' = Drop(messages, mn)
        /\ committed' = Drop(committed, cn)
        /\ executedRecords' = executedRecords \o ExecRecordsFor(Take(ready.cmds, cn))
        /\ appliedCommitted' = appliedCommitted \o Take(ready.cmds, cn)
        /\ ready' = EmptyReady
        /\ awaitAdvance' = FALSE
        /\ barrierOK' = barrierOK /\ ((mn = 0 /\ cn = 0) \/ rn = Len(ready.recs))

Next == Ready \/ Advance

ReadyStable ==
    IF awaitAdvance THEN ready = ReadyView ELSE ready = EmptyReady

ExecutedRecordsAfterCommittedAck ==
    executedRecords = ExecRecordsFor(appliedCommitted)

Safety == ReadyStable /\ ExecutedRecordsAfterCommittedAck /\ barrierOK

Spec == Init /\ [][Next]_<<records, messages, committed, executedRecords, appliedCommitted, awaitAdvance, ready, barrierOK>>

====
