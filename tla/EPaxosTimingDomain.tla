---- MODULE EPaxosTimingDomain ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite model of the durable ProcessAt timing-domain contract.   *)
(* Logical TimeOptimization and physical TOQ values are tagged and are     *)
(* compared only inside one domain. Legacy checksum migration, prepare     *)
(* recovery, Ready durability, deferred-queue ownership, crash/restart,    *)
(* and checked MaxTime boundaries are represented explicitly.              *)
(***************************************************************************)

CONSTANTS Scenario, MaxTime, MaxCrashes, DeferredCapacity,
          MutateRecoveryDomainCopy, MutateTOQLogicalFloor,
          MutateCrossDomainNumericCompare, MutateOmitHeapRemoval,
          MutateTOQCommitLogical, MutateTOQFloorBypass,
          MutateTimedNoop
Scenarios == {"MigrationRestart", "Recovery", "CrossDomain", "Deferred"}
Nodes == {"Owner", "Recovery", "Follower"}
Refs == {"Target", "TOQHistory", "LogicalHistory", "Boundary", "Replacement"}
Values == {"NoValue", "Noop", "A", "B"}
Statuses == {"Absent", "None", "PreAccepted", "Accepted", "Committed"}
Domains == {"Untimed", "Logical", "TOQ"}
TimedDomains == {"Logical", "TOQ"}
DomainTags == Domains \cup {"Missing"}
ChecksumTags == Domains \cup {"LegacyValid", "LegacyCorrupt"}
Modes == {"Ordinary", "Logical", "TOQ"}
ReadyStages == {"Idle", "Frozen", "Durable", "Released"}
Times == 0..MaxTime
Ballots == 0..2

MessageIDs ==
    {"ReadyNone", "UpgradeOut", "AcceptOut", "CommitOut",
     "PrepareOwner", "PrepareOwnerDup", "PrepareFollower", "PrepareStale",
     "DeferredOld", "DeferredDup", "DeferredStale", "DeferredReplacement"}
MessageKinds == {"Empty", "Upgrade", "PrepareResp", "Accept", "Commit", "PreAccept"}
PrepareIDs == {"PrepareOwner", "PrepareOwnerDup", "PrepareFollower", "PrepareStale"}
RecoveryOutputIDs == {"AcceptOut", "CommitOut"}
DeferredKeys == {"OldDeferred", "ReplacementDeferred"}
OldDeferred == "OldDeferred"
ReplacementDeferred == "ReplacementDeferred"

ASSUME
    /\ Scenario \in Scenarios
    /\ MaxTime \in Nat
    /\ MaxTime >= 3
    /\ MaxCrashes \in Nat
    /\ MaxCrashes >= 1
    /\ DeferredCapacity \in Nat
    /\ DeferredCapacity = 1
    /\ MutateRecoveryDomainCopy \in BOOLEAN
    /\ MutateTOQLogicalFloor \in BOOLEAN
    /\ MutateCrossDomainNumericCompare \in BOOLEAN
    /\ MutateOmitHeapRemoval \in BOOLEAN
    /\ MutateTOQCommitLogical \in BOOLEAN
    /\ MutateTOQFloorBypass \in BOOLEAN
    /\ MutateTimedNoop \in BOOLEAN

Record(value, status, domain, processAt, pending, checksum, ballot) ==
    [value |-> value,
     status |-> status,
     domain |-> domain,
     processAt |-> processAt,
     pending |-> pending,
     checksum |-> checksum,
     ballot |-> ballot]

NoRecord == Record("NoValue", "Absent", "Untimed", 0, FALSE, "Untimed", 0)

CanonicalRecord(value, status, domain, processAt, pending, ballot) ==
    Record(value, status, domain, processAt, pending, domain, ballot)

LegacyRecord(value, status, processAt, checksum, ballot) ==
    Record(value, status, "Missing", processAt, FALSE, checksum, ballot)

LegacyPendingRecord(value, status, processAt, checksum, ballot) ==
    Record(value, status, "Missing", processAt, TRUE, checksum, ballot)

RecordPresent(r) == r.value # "NoValue"
RecordCanonical(r) == r.checksum \in Domains
RecordLegacy(r) == r.checksum \in {"LegacyValid", "LegacyCorrupt"}
RecordTuple(r) == <<r.value, r.domain, r.processAt>>

EmptyMessage(id) ==
    [id |-> id,
     kind |-> "Empty",
     from |-> "Owner",
     to |-> "Owner",
     ref |-> "Target",
     value |-> "NoValue",
     status |-> "Absent",
     processAt |-> 0,
     toq |-> FALSE,
     ballot |-> 0,
     fresh |-> FALSE,
     requiresDurable |-> FALSE]

RecordMessage(id, kind, from, to, ref, rec, fresh, requiresDurable) ==
    [id |-> id,
     kind |-> kind,
     from |-> from,
     to |-> to,
     ref |-> ref,
     value |-> rec.value,
     status |-> rec.status,
     processAt |-> rec.processAt,
     toq |-> rec.domain = "TOQ",
     ballot |-> rec.ballot,
     fresh |-> fresh,
     requiresDurable |-> requiresDurable]

MessageDomain(m) ==
    IF m.toq THEN "TOQ"
    ELSE IF m.processAt = 0 THEN "Untimed" ELSE "Logical"

MessageTuple(m) == <<m.value, MessageDomain(m), m.processAt>>

EmptyTable == [n \in Nodes |-> [r \in Refs |-> NoRecord]]
EmptyMessages == [id \in MessageIDs |-> EmptyMessage(id)]
EmptyEvidence == [n \in Nodes |-> NoRecord]
EmptyChosen == [n \in Nodes |-> "NoValue"]

MigrationDisk ==
    [EmptyTable EXCEPT
        !["Recovery"]["Target"] =
            LegacyRecord("A", "PreAccepted", 0, "LegacyValid", 1),
        !["Recovery"]["Boundary"] =
            LegacyRecord("B", "PreAccepted", 1, "LegacyCorrupt", 1),
        !["Recovery"]["LogicalHistory"] =
            CanonicalRecord("A", "Committed", "Logical", MaxTime - 1, FALSE, 2),
        !["Owner"]["Replacement"] =
            LegacyPendingRecord("B", "None", 0, "LegacyValid", 0),
        !["Owner"]["TOQHistory"] =
            CanonicalRecord("B", "Committed", "TOQ", MaxTime, FALSE, 2)]

RecoveryDisk ==
    [EmptyTable EXCEPT
        !["Owner"]["Target"] =
            CanonicalRecord("A", "PreAccepted", "TOQ", MaxTime - 1, FALSE, 1),
        !["Recovery"]["Target"] =
            CanonicalRecord("A", "PreAccepted", "TOQ", MaxTime - 1, FALSE, 1),
        !["Follower"]["Target"] =
            CanonicalRecord("A", "Accepted", "TOQ", MaxTime - 1, FALSE, 2)]

ConflictLive ==
    [EmptyTable EXCEPT
        !["Recovery"]["TOQHistory"] =
            CanonicalRecord("A", "PreAccepted", "TOQ", MaxTime - 1, FALSE, 1),
        !["Owner"]["LogicalHistory"] =
            CanonicalRecord("B", "PreAccepted", "Logical", MaxTime, FALSE, 1)]

DeferredLive ==
    [EmptyTable EXCEPT
        !["Owner"]["Replacement"] =
            CanonicalRecord("B", "None", "TOQ", MaxTime, TRUE, 0)]

InitialDisk ==
    CASE Scenario = "MigrationRestart" -> MigrationDisk
      [] Scenario = "Recovery" -> RecoveryDisk
      [] OTHER -> EmptyTable

InitialLive ==
    CASE Scenario = "CrossDomain" -> ConflictLive
      [] Scenario = "Deferred" -> DeferredLive
      [] OTHER -> EmptyTable

InitialPC ==
    CASE Scenario = "MigrationRestart" -> "M0"
      [] Scenario = "Recovery" -> "R0"
      [] Scenario = "CrossDomain" -> "CN0"
      [] OTHER -> "D0"

InitialMode ==
    CASE Scenario = "CrossDomain" -> "TOQ"
      [] Scenario = "Deferred" -> "Logical"
      [] Scenario = "MigrationRestart" -> "Logical"
      [] OTHER -> "TOQ"

InitialHardTick(hardStatePresent) ==
    IF Scenario = "MigrationRestart" /\ hardStatePresent THEN 1 ELSE 0

VARIABLE state

ModeForNode(node) ==
    IF node = "Recovery" THEN state.mode
    ELSE IF Scenario = "Recovery" THEN "TOQ"
    ELSE IF node = "Owner" /\ Scenario \in {"MigrationRestart", "Deferred"} THEN "TOQ"
    ELSE IF node = "Owner" /\ Scenario = "CrossDomain" THEN "Logical"
    ELSE "Ordinary"

DomainEnabled(domain, mode) ==
    \/ domain = "Untimed"
    \/ domain = "Logical" /\ mode = "Logical"
    \/ domain = "TOQ" /\ mode = "TOQ"

vars == state

Init ==
    \E hardStatePresent \in
        (IF Scenario = "MigrationRestart" THEN BOOLEAN ELSE {TRUE}) :
        state =
            [pc |-> InitialPC,
             disk |-> InitialDisk,
             live |-> InitialLive,
             hardStatePresent |-> hardStatePresent,
             hardTick |-> InitialHardTick(hardStatePresent),
             tick |-> InitialHardTick(hardStatePresent),
             running |-> Nodes \ {"Owner"},
             crashCount |-> 0,
             mode |-> InitialMode,

             migrationDomain |-> "Missing",
             ambiguousAttempted |-> FALSE,
             ambiguousAccepted |-> FALSE,
             corruptAttempted |-> FALSE,
             corruptAccepted |-> FALSE,
             pendingInferenceObserved |-> FALSE,
             acceptedMigration |-> FALSE,
             migrationSourceAt |-> 0,
             migrationSourceValid |-> FALSE,
             migrationSourceDomain |-> "Missing",

             readyStage |-> "Idle",
             readyNode |-> "Recovery",
             readyRef |-> "Target",
             readyRecord |-> NoRecord,
             readyMessage |-> EmptyMessage("ReadyNone"),
             durableReadyPrefix |-> 0,
             releasedReadyPrefix |-> 0,
             durablyCovered |-> {},

             messages |-> EmptyMessages,
             network |-> {},
             delivered |-> {},
             rejectedMessages |-> {},
             prepareEvidence |-> EmptyEvidence,
             selectionMade |-> FALSE,
             selectedSource |-> NoRecord,
             recoveryRequested |-> FALSE,
             recoveryDone |-> FALSE,
             chosenBy |-> EmptyChosen,

             deferredIndex |-> {},
             logicalHeap |-> {},
             toqHeap |-> {},
             capacityWait |-> FALSE,
             replacementAdmitted |-> FALSE,
             supersessionObserved |-> FALSE,
             terminalSeen |-> FALSE,

             crossEval |-> FALSE,
             crossDomainSkipped |-> FALSE,
             sameLogicalSkipped |-> FALSE,
             sameTOQSkipped |-> FALSE,
             modeRejectionObserved |-> FALSE,
             wrongDomainRejected |-> FALSE,
             wrongDomainAdopted |-> FALSE,
             toqClosed |-> Scenario = "CrossDomain",
             toqClosedThrough |-> IF Scenario = "CrossDomain" THEN MaxTime - 1 ELSE 0,
             proposedTOQAt |-> 0,
             canonicalNoopObserved |-> FALSE,
             timedNoopRejected |-> FALSE,
             timedNoopAdopted |-> FALSE,

             restartChecked |-> FALSE,
             restartExpectedTick |-> 0,
             restartObservedTick |-> 0,
             restartHadHardState |-> FALSE,
             restartInitialHardTick |-> 0,
             overflowAttempted |-> FALSE,
             overflowRejected |-> FALSE,
             overflowBeforeRecord |-> NoRecord]

RecordType(r) ==
    /\ r.value \in Values
    /\ r.status \in Statuses
    /\ r.domain \in DomainTags
    /\ r.processAt \in Times
    /\ r.pending \in BOOLEAN
    /\ r.checksum \in ChecksumTags
    /\ r.ballot \in Ballots

MessageType(m) ==
    /\ m.id \in MessageIDs
    /\ m.kind \in MessageKinds
    /\ m.from \in Nodes
    /\ m.to \in Nodes
    /\ m.ref \in Refs
    /\ m.value \in Values
    /\ m.status \in Statuses
    /\ m.processAt \in Times
    /\ m.toq \in BOOLEAN
    /\ m.ballot \in Ballots
    /\ m.fresh \in BOOLEAN
    /\ m.requiresDurable \in BOOLEAN

TableType(table) ==
    /\ DOMAIN table = Nodes
    /\ \A n \in Nodes :
        /\ DOMAIN table[n] = Refs
        /\ \A ref \in Refs : RecordType(table[n][ref])

TypeOK ==
    /\ state.pc \in
        {"M0", "M1", "M2", "M2P", "M2PD", "M2PR", "M2C", "M2D",
         "M3", "M4", "M4O", "M5", "M6", "M7", "MDone",
         "R0", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "RDone",
         "CN0", "CN1", "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "CDone",
         "D0", "D1", "D2", "D3", "D4", "D5", "D6", "DDone"}
    /\ TableType(state.disk)
    /\ TableType(state.live)
    /\ state.hardStatePresent \in BOOLEAN
    /\ state.hardTick \in Times
    /\ state.tick \in Times
    /\ state.running \subseteq Nodes
    /\ state.crashCount \in 0..MaxCrashes
    /\ state.mode \in Modes
    /\ state.migrationDomain \in DomainTags
    /\ state.ambiguousAttempted \in BOOLEAN
    /\ state.ambiguousAccepted \in BOOLEAN
    /\ state.corruptAttempted \in BOOLEAN
    /\ state.corruptAccepted \in BOOLEAN
    /\ state.pendingInferenceObserved \in BOOLEAN
    /\ state.acceptedMigration \in BOOLEAN
    /\ state.migrationSourceAt \in Times
    /\ state.migrationSourceValid \in BOOLEAN
    /\ state.migrationSourceDomain \in DomainTags
    /\ state.readyNode \in Nodes
    /\ state.readyRef \in Refs
    /\ state.readyStage \in ReadyStages
    /\ RecordType(state.readyRecord)
    /\ MessageType(state.readyMessage)
    /\ state.durableReadyPrefix \in 0..1
    /\ state.releasedReadyPrefix \in 0..1
    /\ state.durablyCovered \subseteq MessageIDs
    /\ DOMAIN state.messages = MessageIDs
    /\ \A id \in MessageIDs : MessageType(state.messages[id])
    /\ state.network \subseteq MessageIDs
    /\ state.delivered \subseteq MessageIDs
    /\ state.rejectedMessages \subseteq MessageIDs
    /\ DOMAIN state.prepareEvidence = Nodes
    /\ \A n \in Nodes : RecordType(state.prepareEvidence[n])
    /\ state.selectionMade \in BOOLEAN
    /\ RecordType(state.selectedSource)
    /\ state.recoveryRequested \in BOOLEAN
    /\ state.recoveryDone \in BOOLEAN
    /\ DOMAIN state.chosenBy = Nodes
    /\ \A n \in Nodes : state.chosenBy[n] \in Values
    /\ state.deferredIndex \subseteq DeferredKeys
    /\ state.logicalHeap \subseteq DeferredKeys
    /\ state.toqHeap \subseteq DeferredKeys
    /\ Cardinality(state.deferredIndex) <= DeferredCapacity
    /\ state.capacityWait \in BOOLEAN
    /\ state.replacementAdmitted \in BOOLEAN
    /\ state.supersessionObserved \in BOOLEAN
    /\ state.terminalSeen \in BOOLEAN
    /\ state.crossEval \in BOOLEAN
    /\ state.crossDomainSkipped \in BOOLEAN
    /\ state.sameLogicalSkipped \in BOOLEAN
    /\ state.sameTOQSkipped \in BOOLEAN
    /\ state.modeRejectionObserved \in BOOLEAN
    /\ state.wrongDomainRejected \in BOOLEAN
    /\ state.wrongDomainAdopted \in BOOLEAN
    /\ state.toqClosed \in BOOLEAN
    /\ state.toqClosedThrough \in Times
    /\ state.proposedTOQAt \in Times
    /\ state.canonicalNoopObserved \in BOOLEAN
    /\ state.timedNoopRejected \in BOOLEAN
    /\ state.timedNoopAdopted \in BOOLEAN
    /\ state.restartChecked \in BOOLEAN
    /\ state.restartExpectedTick \in Times
    /\ state.restartObservedTick \in Times
    /\ state.restartHadHardState \in BOOLEAN
    /\ state.restartInitialHardTick \in Times
    /\ state.overflowAttempted \in BOOLEAN
    /\ state.overflowRejected \in BOOLEAN
    /\ RecordType(state.overflowBeforeRecord)

RecordDomainShape(r) ==
    IF ~RecordPresent(r)
    THEN /\ r.domain = "Untimed"
         /\ r.processAt = 0
         /\ ~r.pending
    ELSE IF RecordLegacy(r)
    THEN r.domain = "Missing"
    ELSE /\ r.domain \in Domains
         /\ (r.domain = "Untimed" => r.processAt = 0 /\ ~r.pending)
         /\ (r.domain = "Logical" => r.processAt > 0 /\ ~r.pending)
         /\ (r.pending => r.domain = "TOQ")
         /\ (r.value = "Noop" =>
                /\ RecordCanonical(r)
                /\ r.domain = "Untimed"
                /\ r.processAt = 0
                /\ ~r.pending)

MessageDomainShape(m) ==
    m.kind = "Empty" \/
    /\ (m.toq <=> MessageDomain(m) = "TOQ")
    /\ (~m.toq /\ m.processAt = 0 <=> MessageDomain(m) = "Untimed")
    /\ (~m.toq /\ m.processAt > 0 <=> MessageDomain(m) = "Logical")
    /\ (m.value = "Noop" =>
           /\ ~m.toq
           /\ m.processAt = 0
           /\ MessageDomain(m) = "Untimed")

DomainShape ==
    /\ \A n \in Nodes, ref \in Refs : RecordDomainShape(state.disk[n][ref])
    /\ \A n \in Nodes, ref \in Refs : RecordDomainShape(state.live[n][ref])
    /\ RecordDomainShape(state.readyRecord)
    /\ \A id \in MessageIDs :
        id \in state.rejectedMessages \/ MessageDomainShape(state.messages[id])

CanonicalChecksumAgreement(r) == RecordCanonical(r) => r.checksum = r.domain

ChecksumDomainAgreement ==
    /\ \A n \in Nodes, ref \in Refs : CanonicalChecksumAgreement(state.disk[n][ref])
    /\ \A n \in Nodes, ref \in Refs : CanonicalChecksumAgreement(state.live[n][ref])
    /\ CanonicalChecksumAgreement(state.readyRecord)

LegacyMigrationDomain(source, explicitDomain) ==
    IF source.pending THEN "TOQ" ELSE explicitDomain

LegacyMigrationAccepted(source, optionPresent, explicitDomain) ==
    /\ source.checksum = "LegacyValid"
    /\ (source.pending \/ optionPresent)
    /\ LET domain == LegacyMigrationDomain(source, explicitDomain)
           migrated == CanonicalRecord(source.value, source.status, domain,
                                       source.processAt, source.pending, source.ballot)
       IN RecordDomainShape(migrated)

NoAmbiguousLegacyAcceptance ==
    LET zero == LegacyRecord("A", "PreAccepted", 0, "LegacyValid", 1)
        nonzero == LegacyRecord("A", "PreAccepted", 1, "LegacyValid", 1)
        pending == LegacyPendingRecord("A", "None", 0, "LegacyValid", 0)
        corrupt == LegacyRecord("A", "PreAccepted", 0, "LegacyCorrupt", 1)
    IN /\ ~state.ambiguousAccepted
       /\ ~state.corruptAccepted
       /\ ~LegacyMigrationAccepted(zero, FALSE, "Untimed")
       /\ LegacyMigrationAccepted(zero, TRUE, "Untimed")
       /\ LegacyMigrationAccepted(zero, TRUE, "TOQ")
       /\ ~LegacyMigrationAccepted(zero, TRUE, "Logical")
       /\ LegacyMigrationAccepted(nonzero, TRUE, "Logical")
       /\ LegacyMigrationAccepted(nonzero, TRUE, "TOQ")
       /\ ~LegacyMigrationAccepted(nonzero, TRUE, "Untimed")
       /\ LegacyMigrationAccepted(pending, FALSE, "Untimed")
       /\ ~LegacyMigrationAccepted(corrupt, TRUE, "Untimed")
       /\ (state.acceptedMigration =>
              /\ state.migrationSourceValid
              /\ state.migrationSourceAt > 0
              /\ state.migrationDomain = state.migrationSourceDomain
              /\ state.migrationSourceDomain = "Logical")

CanonicalLogicalTimes(table, node) ==
    UNION {
        IF RecordPresent(table[node][ref]) /\
           RecordCanonical(table[node][ref]) /\
           table[node][ref].domain = "Logical"
        THEN {table[node][ref].processAt}
        ELSE {}
        : ref \in Refs}

CanonicalTimedTimes(table) ==
    UNION {UNION {
        IF RecordPresent(table[n][ref]) /\
           RecordCanonical(table[n][ref]) /\
           table[n][ref].domain \in TimedDomains
        THEN {table[n][ref].processAt}
        ELSE {}
        : ref \in Refs}
        : n \in Nodes}

MaxFinite(nonempty) == CHOOSE maximum \in nonempty : \A value \in nonempty : value <= maximum
LogicalFloor(table, node, hardTick) ==
    MaxFinite(CanonicalLogicalTimes(table, node) \cup {hardTick})
AllTimedFloor(table, hardTick) == MaxFinite(CanonicalTimedTimes(table) \cup {hardTick})

LogicalFloorOnly ==
    state.restartChecked =>
        /\ state.restartObservedTick = state.restartExpectedTick
        /\ (state.restartHadHardState =>
               state.restartExpectedTick = state.restartInitialHardTick)
        /\ (~state.restartHadHardState =>
               state.restartExpectedTick =
                   LogicalFloor(state.disk, "Recovery", state.restartInitialHardTick))

RecoveryPreservesTimingTuple ==
    state.selectionMade =>
        /\ RecordTuple(state.live["Recovery"]["Target"]) = RecordTuple(state.selectedSource)
        /\ (state.readyStage = "Idle" \/
              RecordTuple(state.readyRecord) = RecordTuple(state.selectedSource))

ValueBearingRecord(r) == RecordPresent(r) /\ r.value \in {"A", "B"}
ValueBearingMessage(m) == m.value \in {"A", "B"}

RecordModeAdmitted(node, rec) ==
    \/ ~RecordCanonical(rec)
    \/ ~ValueBearingRecord(rec)
    \/ DomainEnabled(rec.domain, ModeForNode(node))

TimingModeAdmission ==
    /\ \A node \in Nodes, ref \in Refs :
        /\ RecordModeAdmitted(node, state.disk[node][ref])
        /\ RecordModeAdmitted(node, state.live[node][ref])
    /\ (RecordPresent(state.readyRecord) =>
           DomainEnabled(state.readyRecord.domain, ModeForNode(state.readyNode)))
    /\ \A id \in state.delivered :
        \/ id \in state.rejectedMessages
        \/ ~ValueBearingMessage(state.messages[id])
        \/ DomainEnabled(MessageDomain(state.messages[id]),
                         ModeForNode(state.messages[id].to))

NoCrossDomainNumericSkip ==
    /\ ~state.crossDomainSkipped
    /\ ~state.wrongDomainAdopted
    /\ (state.pc \in {"C3", "C4", "C5", "C6", "C7", "CDone"} =>
           state.sameLogicalSkipped)
    /\ (state.pc = "CDone" => state.sameTOQSkipped)

DurableUpgradeBeforeOutput ==
    /\ state.releasedReadyPrefix <= state.durableReadyPrefix
    /\ (state.durableReadyPrefix = 1 =>
           state.disk[state.readyNode][state.readyRef] = state.readyRecord)
    /\ \A id \in state.network \cup state.delivered :
        state.messages[id].requiresDurable => id \in state.durablyCovered

MigrationUpgradeDisposition ==
    /\ ("UpgradeOut" \in state.delivered =>
           "UpgradeOut" \in state.rejectedMessages)
    /\ ("UpgradeOut" \in state.rejectedMessages =>
           /\ "UpgradeOut" \in state.delivered
           /\ ValueBearingMessage(state.messages["UpgradeOut"])
           /\ MessageDomain(state.messages["UpgradeOut"]) = "Logical"
           /\ ~DomainEnabled(MessageDomain(state.messages["UpgradeOut"]),
                             ModeForNode(state.messages["UpgradeOut"].to)))
    /\ (state.readyStage = "Released" /\
        state.pc \in {"M5", "M6", "M7", "MDone"} =>
           /\ "UpgradeOut" \in state.delivered
           /\ "UpgradeOut" \in state.rejectedMessages)

WrongDomainMessagesHaveNoEffect == ~state.wrongDomainAdopted

TOQClosedFloorPreserved ==
    state.proposedTOQAt = 0 \/ state.proposedTOQAt > MaxTime - 1

DeferredIndexHeapBijection ==
    /\ state.deferredIndex = state.logicalHeap \cup state.toqHeap
    /\ state.logicalHeap \cap state.toqHeap = {}

TerminalReleasesDeferredCapacity ==
    state.supersessionObserved =>
        OldDeferred \notin state.deferredIndex \cup state.logicalHeap \cup state.toqHeap

CanonicalNoopOnly ==
    /\ ~state.timedNoopAdopted
    /\ \A node \in Nodes, ref \in Refs :
        /\ (state.disk[node][ref].value = "Noop" =>
               RecordDomainShape(state.disk[node][ref]))
        /\ (state.live[node][ref].value = "Noop" =>
               RecordDomainShape(state.live[node][ref]))
    /\ \A id \in state.delivered \ state.rejectedMessages :
        state.messages[id].value = "Noop" =>
            /\ ~state.messages[id].toq
            /\ state.messages[id].processAt = 0

NoTickWrap ==
    /\ state.tick <= MaxTime
    /\ state.hardTick <= MaxTime
    /\ \A n \in Nodes, ref \in Refs : state.disk[n][ref].processAt <= MaxTime
    /\ \A n \in Nodes, ref \in Refs : state.live[n][ref].processAt <= MaxTime
    /\ (state.overflowAttempted =>
           /\ state.overflowRejected
           /\ state.tick = MaxTime
           /\ state.live["Recovery"]["Boundary"] = state.overflowBeforeRecord)

ChosenValueAgreement ==
    Cardinality({value \in Values \ {"NoValue"} :
        \E n \in Nodes : state.chosenBy[n] = value}) <= 1

RejectAmbiguousLegacy ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M0"
    /\ state.disk["Recovery"]["Target"].checksum = "LegacyValid"
    /\ state.disk["Recovery"]["Target"].processAt = 0
    /\ state.migrationDomain = "Missing"
    /\ state' = [state EXCEPT
        !.pc = "M1",
        !.ambiguousAttempted = TRUE,
        !.ambiguousAccepted = FALSE]

RejectCorruptLegacy ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M1"
    /\ state.disk["Recovery"]["Boundary"].checksum = "LegacyCorrupt"
    /\ state' = [state EXCEPT
        !.pc = "M2",
        !.corruptAttempted = TRUE,
        !.corruptAccepted = FALSE]

InferPendingLegacyTOQ ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2"
    /\ LET source == state.disk["Owner"]["Replacement"]
           inferredDomain == LegacyMigrationDomain(source, "Untimed")
           inferred == CanonicalRecord(source.value, source.status, inferredDomain,
                                       source.processAt, TRUE, source.ballot)
           zeroMessage == RecordMessage("DeferredReplacement", "PrepareResp",
                                        "Owner", "Owner", "Replacement",
                                        inferred, TRUE, TRUE)
       IN /\ LegacyMigrationAccepted(source, FALSE, "Untimed")
          /\ state' = [state EXCEPT
                !.pc = "M2P",
                !.readyNode = "Owner",
                !.readyRef = "Replacement",
                !.readyStage = "Frozen",
                !.readyRecord = inferred,
                !.readyMessage = zeroMessage,
                !.messages["DeferredReplacement"] = zeroMessage,
                !.durableReadyPrefix = 0,
                !.releasedReadyPrefix = 0]

CrashBeforePendingMigrationDurable ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2P"
    /\ state.crashCount < MaxCrashes
    /\ state' = [state EXCEPT
        !.pc = "M2",
        !.crashCount = @ + 1,
        !.readyStage = "Idle",
        !.readyRecord = NoRecord,
        !.readyMessage = EmptyMessage("ReadyNone"),
        !.durableReadyPrefix = 0,
        !.releasedReadyPrefix = 0]

PersistPendingMigrationReady ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2P"
    /\ state.readyStage = "Frozen"
    /\ state' = [state EXCEPT
        !.pc = "M2PD",
        !.disk[state.readyNode][state.readyRef] = state.readyRecord,
        !.durablyCovered = @ \cup {"DeferredReplacement"},
        !.pendingInferenceObserved = TRUE,
        !.readyStage = "Durable",
        !.durableReadyPrefix = 1]

CrashAfterPendingMigrationDurable ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2PD"
    /\ state.crashCount < MaxCrashes
    /\ state' = [state EXCEPT
        !.pc = "M2PR",
        !.crashCount = @ + 1,
        !.live["Owner"]["Replacement"] = state.disk["Owner"]["Replacement"],
        !.readyStage = "Idle",
        !.readyRecord = NoRecord,
        !.readyMessage = EmptyMessage("ReadyNone"),
        !.durableReadyPrefix = 0,
        !.releasedReadyPrefix = 0]

AdvancePendingMigrationReady ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2PD"
    /\ state' = [state EXCEPT
        !.pc = "M2C",
        !.live["Owner"]["Replacement"] = state.disk["Owner"]["Replacement"],
        !.delivered = @ \cup {"DeferredReplacement"},
        !.readyStage = "Released",
        !.releasedReadyPrefix = 1]

RestorePendingMigrationOutputAfterCrash ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2PR"
    /\ LET source == state.disk["Owner"]["Replacement"]
           output == RecordMessage("DeferredReplacement", "PrepareResp",
                                   "Owner", "Owner", "Replacement",
                                   source, TRUE, TRUE)
       IN /\ RecordCanonical(source)
          /\ source.domain = "TOQ"
          /\ state' = [state EXCEPT
                !.pc = "M2C",
                !.live["Owner"]["Replacement"] = source,
                !.messages["DeferredReplacement"] = output,
                !.delivered = @ \cup {"DeferredReplacement"}]

ConfigureExplicitMigrationDomain ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2C"
    /\ state' = [state EXCEPT
        !.pc = "M2D",
        !.disk["Recovery"]["Target"] =
            LegacyRecord("A", "PreAccepted", 1, "LegacyValid", 1),
        !.migrationDomain = "Logical"]

MigrateValidLegacy ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M2D"
    /\ LET source == state.disk["Recovery"]["Target"]
           domain == LegacyMigrationDomain(source, state.migrationDomain)
           upgraded == CanonicalRecord(source.value, source.status, domain,
                                       source.processAt, FALSE, source.ballot)
           output == RecordMessage("UpgradeOut", "Upgrade", "Recovery", "Follower",
                                   "Target", upgraded, TRUE, TRUE)
       IN /\ LegacyMigrationAccepted(source, TRUE, state.migrationDomain)
          /\ state' = [state EXCEPT
                !.pc = "M3",
                !.acceptedMigration = TRUE,
                !.migrationSourceAt = source.processAt,
                !.migrationSourceValid = source.checksum = "LegacyValid",
                !.migrationSourceDomain = domain,
                !.live["Recovery"]["Target"] = upgraded,
                !.readyNode = "Recovery",
                !.readyRef = "Target",
                !.readyStage = "Frozen",
                !.readyRecord = upgraded,
                !.readyMessage = output,
                !.messages["UpgradeOut"] = output,
                !.durableReadyPrefix = 0,
                !.releasedReadyPrefix = 0]

CrashBeforeMigrationDurable ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M3"
    /\ state.crashCount < MaxCrashes
    /\ state' = [state EXCEPT
        !.pc = "M2D",
        !.crashCount = @ + 1,
        !.live["Recovery"]["Target"] = NoRecord,
        !.acceptedMigration = FALSE,
        !.migrationSourceAt = 0,
        !.migrationSourceValid = FALSE,
        !.migrationSourceDomain = "Missing",
        !.readyStage = "Idle",
        !.readyRecord = NoRecord,
        !.readyMessage = EmptyMessage("ReadyNone"),
        !.durableReadyPrefix = 0,
        !.releasedReadyPrefix = 0]

PersistMigrationReady ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M3"
    /\ state.readyStage = "Frozen"
    /\ state' = [state EXCEPT
        !.pc = "M4",
        !.disk[state.readyNode][state.readyRef] = state.readyRecord,
        !.durablyCovered = @ \cup {"UpgradeOut"},
        !.readyStage = "Durable",
        !.durableReadyPrefix = 1]

CrashAfterMigrationDurable ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M4"
    /\ state.crashCount < MaxCrashes
    /\ state' = [state EXCEPT
        !.pc = "M5",
        !.crashCount = @ + 1,
        !.live["Recovery"]["Target"] = state.disk["Recovery"]["Target"],
        !.readyStage = "Idle",
        !.readyRecord = NoRecord,
        !.readyMessage = EmptyMessage("ReadyNone"),
        !.durableReadyPrefix = 0,
        !.releasedReadyPrefix = 0]

ReleaseMigrationOutput ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M4"
    /\ state.readyStage = "Durable"
    /\ state' = [state EXCEPT
        !.pc = "M4O",
        !.network = @ \cup {"UpgradeOut"},
        !.readyStage = "Released",
        !.releasedReadyPrefix = 1]

DeliverMigrationOutput ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M4O"
    /\ "UpgradeOut" \in state.network
    /\ LET output == state.messages["UpgradeOut"]
       IN /\ ValueBearingMessage(output)
          /\ MessageDomain(output) = "Logical"
          /\ ~DomainEnabled(MessageDomain(output), ModeForNode(output.to))
          /\ state' = [state EXCEPT
                !.pc = "M5",
                !.network = @ \ {"UpgradeOut"},
                !.delivered = @ \cup {"UpgradeOut"},
                !.rejectedMessages = @ \cup {"UpgradeOut"}]

RestartMigration ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M5"
    /\ LET expected == IF state.hardStatePresent
                       THEN state.hardTick
                       ELSE LogicalFloor(state.disk, "Recovery", state.hardTick)
           observed == IF MutateTOQLogicalFloor /\ ~state.hardStatePresent
                       THEN AllTimedFloor(state.disk, state.hardTick)
                       ELSE expected
       IN state' = [state EXCEPT
            !.pc = "M6",
            !.live["Recovery"] = state.disk["Recovery"],
            !.tick = observed,
            !.restartChecked = TRUE,
            !.restartExpectedTick = expected,
            !.restartHadHardState = state.hardStatePresent,
            !.restartInitialHardTick = state.hardTick,
            !.restartObservedTick = observed]

ProposeAtMaxBoundary ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M6"
    /\ state.tick < MaxTime
    /\ state' = [state EXCEPT
        !.pc = "M7",
        !.live["Recovery"]["Boundary"] =
            CanonicalRecord("B", "PreAccepted", "Logical", MaxTime, FALSE, 1),
        !.tick = MaxTime,
        !.hardTick = MaxTime,
        !.overflowBeforeRecord =
            CanonicalRecord("B", "PreAccepted", "Logical", MaxTime, FALSE, 1),
        !.hardStatePresent = TRUE]

RejectProcessAtOverflow ==
    /\ Scenario = "MigrationRestart"
    /\ state.pc = "M7"
    /\ state.tick = MaxTime
    /\ state' = [state EXCEPT
        !.pc = "MDone",
        !.overflowAttempted = TRUE,
        !.overflowRejected = TRUE]

PrepareMessage(id, from, rec) ==
    RecordMessage(id, "PrepareResp", from, "Recovery", "Target", rec, TRUE, FALSE)

BeginRecovery ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R0"
    /\ LET local == state.disk["Recovery"]["Target"]
           follower == state.disk["Follower"]["Target"]
           stale == CanonicalRecord("B", "PreAccepted", "Logical", 1, FALSE, 1)
           localMsg == PrepareMessage("PrepareOwner", "Recovery", local)
           duplicate == PrepareMessage("PrepareOwnerDup", "Recovery", local)
           followerMsg == PrepareMessage("PrepareFollower", "Follower", follower)
           staleMsg == RecordMessage("PrepareStale", "PrepareResp", "Follower", "Recovery",
                                     "Target", stale, FALSE, FALSE)
       IN state' = [state EXCEPT
            !.pc = "R1",
            !.recoveryRequested = TRUE,
            !.prepareEvidence = EmptyEvidence,
            !.selectionMade = FALSE,
            !.selectedSource = NoRecord,
            !.network = (@ \ RecoveryOutputIDs) \cup PrepareIDs,
            !.delivered = @ \ PrepareIDs,
            !.rejectedMessages = @ \ PrepareIDs,
            !.messages["PrepareOwner"] = localMsg,
            !.messages["PrepareOwnerDup"] = duplicate,
            !.messages["PrepareFollower"] = followerMsg,
            !.messages["PrepareStale"] = staleMsg]

DecodedEvidence(message) ==
    CanonicalRecord(message.value, message.status, MessageDomain(message),
                    message.processAt, FALSE, message.ballot)

DeliverPrepare(id) ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R1"
    /\ id \in PrepareIDs
    /\ id \in state.network
    /\ id \notin state.delivered
    /\ LET message == state.messages[id]
           oldEvidence == state.prepareEvidence[message.from]
           admitted == DomainEnabled(MessageDomain(message), ModeForNode(message.to))
           acceptFresh == message.fresh /\ admitted /\ ~RecordPresent(oldEvidence)
       IN state' = [state EXCEPT
            !.delivered = @ \cup {id},
            !.rejectedMessages =
                IF ValueBearingMessage(message) /\ ~admitted THEN @ \cup {id} ELSE @,
            !.prepareEvidence[message.from] =
                IF acceptFresh THEN DecodedEvidence(message) ELSE oldEvidence]

EvidenceSenders == {n \in Nodes : RecordPresent(state.prepareEvidence[n])}

SelectRecoveryValue ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R1"
    /\ PrepareIDs \subseteq state.delivered
    /\ Cardinality(EvidenceSenders) >= 2
    /\ LET selected == state.prepareEvidence["Follower"]
           copiedDomain == IF MutateRecoveryDomainCopy THEN "Logical" ELSE selected.domain
           adopted == CanonicalRecord(selected.value, "Accepted", copiedDomain,
                                      selected.processAt, FALSE, selected.ballot)
           acceptMessage == RecordMessage("AcceptOut", "Accept", "Recovery", "Follower",
                                          "Target", adopted, TRUE, TRUE)
       IN state' = [state EXCEPT
            !.pc = "R2",
            !.selectionMade = TRUE,
            !.selectedSource = selected,
            !.live["Recovery"]["Target"] = adopted,
            !.readyNode = "Recovery",
            !.readyRef = "Target",
            !.readyStage = "Frozen",
            !.readyRecord = adopted,
            !.readyMessage = acceptMessage,
            !.messages["AcceptOut"] = acceptMessage,
            !.durableReadyPrefix = 0,
            !.releasedReadyPrefix = 0]

CrashBeforeRecoveryDurable ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R2"
    /\ state.crashCount < MaxCrashes
    /\ state' = [state EXCEPT
        !.pc = "R0",
        !.crashCount = @ + 1,
        !.live["Recovery"]["Target"] = state.disk["Recovery"]["Target"],
        !.prepareEvidence = EmptyEvidence,
        !.selectionMade = FALSE,
        !.selectedSource = NoRecord,
        !.readyStage = "Idle",
        !.readyRecord = NoRecord,
        !.readyMessage = EmptyMessage("ReadyNone"),
        !.durableReadyPrefix = 0,
        !.releasedReadyPrefix = 0]

PersistRecoveredAccept ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R2"
    /\ state.readyStage = "Frozen"
    /\ state' = [state EXCEPT
        !.pc = "R3",
        !.disk[state.readyNode][state.readyRef] = state.readyRecord,
        !.durablyCovered = @ \cup {"AcceptOut"},
        !.readyStage = "Durable",
        !.durableReadyPrefix = 1]

CrashAfterRecoveryDurable ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R3"
    /\ state.crashCount < MaxCrashes
    /\ state' = [state EXCEPT
        !.pc = "R0",
        !.crashCount = @ + 1,
        !.live["Recovery"]["Target"] = state.disk["Recovery"]["Target"],
        !.prepareEvidence = EmptyEvidence,
        !.selectionMade = FALSE,
        !.selectedSource = NoRecord,
        !.readyStage = "Idle",
        !.readyRecord = NoRecord,
        !.readyMessage = EmptyMessage("ReadyNone"),
        !.durableReadyPrefix = 0,
        !.releasedReadyPrefix = 0]

ReleaseRecoveredAccept ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R3"
    /\ state.readyStage = "Durable"
    /\ state' = [state EXCEPT
        !.pc = "R4",
        !.network = (@ \ PrepareIDs) \cup {"AcceptOut"},
        !.readyStage = "Released",
        !.releasedReadyPrefix = 1]

AcceptRecoveredValue ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R4"
    /\ LET current == state.live["Recovery"]["Target"]
           committed == CanonicalRecord(current.value, "Committed", current.domain,
                                        current.processAt, FALSE, current.ballot)
           commitMessage == RecordMessage("CommitOut", "Commit", "Recovery", "Follower",
                                          "Target", committed, TRUE, TRUE)
       IN state' = [state EXCEPT
            !.pc = "R5",
            !.live["Recovery"]["Target"] = committed,
            !.readyNode = "Recovery",
            !.readyRef = "Target",
            !.readyStage = "Frozen",
            !.readyRecord = committed,
            !.readyMessage = commitMessage,
            !.messages["CommitOut"] = commitMessage,
            !.durableReadyPrefix = 0,
            !.releasedReadyPrefix = 0]

PersistRecoveredCommit ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R5"
    /\ state' = [state EXCEPT
        !.pc = "R6",
        !.disk[state.readyNode][state.readyRef] = state.readyRecord,
        !.chosenBy["Recovery"] = state.readyRecord.value,
        !.durablyCovered = @ \cup {"CommitOut"},
        !.readyStage = "Durable",
        !.durableReadyPrefix = 1]

ReleaseRecoveredCommit ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R6"
    /\ state' = [state EXCEPT
        !.pc = "R7",
        !.network = (@ \ {"AcceptOut"}) \cup {"CommitOut"},
        !.readyStage = "Released",
        !.releasedReadyPrefix = 1]

DeliverRecoveredCommit ==
    /\ Scenario = "Recovery"
    /\ state.pc = "R7"
    /\ "CommitOut" \in state.network
    /\ LET committed == state.live["Recovery"]["Target"]
       IN state' = [state EXCEPT
            !.pc = "RDone",
            !.disk["Follower"]["Target"] = committed,
            !.live["Follower"]["Target"] = committed,
            !.chosenBy["Follower"] = committed.value,
            !.delivered = @ \cup {"CommitOut"},
            !.recoveryDone = TRUE]

ShouldSkipTimed(candidateDomain, candidateAt, conflictRecord) ==
    /\ conflictRecord.status = "PreAccepted"
    /\ conflictRecord.domain \in TimedDomains
    /\ IF candidateDomain = conflictRecord.domain
       THEN candidateAt < conflictRecord.processAt
       ELSE MutateCrossDomainNumericCompare /\ candidateAt < conflictRecord.processAt

ExerciseCanonicalRecoveryNoop ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "CN0"
    /\ LET noop == CanonicalRecord("Noop", "Accepted", "Untimed", 0, FALSE, 1)
           message == RecordMessage("UpgradeOut", "PrepareResp", "Owner", "Recovery",
                                    "Boundary", noop, TRUE, FALSE)
       IN state' = [state EXCEPT
            !.pc = "CN1",
            !.messages["UpgradeOut"] = message,
            !.delivered = @ \cup {"UpgradeOut"},
            !.live["Recovery"]["Boundary"] = noop,
            !.canonicalNoopObserved = TRUE]

RejectTimedNoop ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "CN1"
    /\ LET timed == CanonicalRecord("Noop", "Accepted", "TOQ", 1, FALSE, 1)
           message == RecordMessage("AcceptOut", "Accept", "Owner", "Recovery",
                                    "Boundary", timed, TRUE, FALSE)
       IN state' = [state EXCEPT
            !.pc = "C0",
            !.messages["AcceptOut"] = message,
            !.delivered = @ \cup {"AcceptOut"},
            !.rejectedMessages =
                IF MutateTimedNoop THEN @ ELSE @ \cup {"AcceptOut"},
            !.timedNoopRejected = ~MutateTimedNoop,
            !.timedNoopAdopted = MutateTimedNoop,
            !.live["Recovery"]["Boundary"] =
                IF MutateTimedNoop THEN timed ELSE @]

DeliverMislabeledTOQCommit ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C0"
    /\ LET logical == CanonicalRecord("A", "Committed", "Logical",
                                      MaxTime - 1, FALSE, 2)
           message == RecordMessage("CommitOut", "Commit", "Owner", "Recovery",
                                    "Target", logical, TRUE, FALSE)
       IN state' = [state EXCEPT
            !.pc = "C1",
            !.messages["CommitOut"] = message,
            !.delivered = @ \cup {"CommitOut"},
            !.rejectedMessages =
                IF MutateTOQCommitLogical THEN @ ELSE @ \cup {"CommitOut"},
            !.wrongDomainRejected = ~MutateTOQCommitLogical,
            !.wrongDomainAdopted = MutateTOQCommitLogical,
            !.live["Recovery"]["Target"] =
                IF MutateTOQCommitLogical THEN logical ELSE @]

EvaluateLogicalCrossDomain ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C1"
    /\ LET admitted == DomainEnabled("Logical", state.mode)
           skipped == IF admitted
                      THEN ShouldSkipTimed("Logical", 1,
                                           state.live["Recovery"]["TOQHistory"])
                      ELSE MutateCrossDomainNumericCompare /\
                           1 < state.live["Recovery"]["TOQHistory"].processAt
       IN state' = [state EXCEPT
            !.pc = "C2",
            !.crossEval = TRUE,
            !.modeRejectionObserved = @ \/ ~admitted,
            !.crossDomainSkipped = @ \/ skipped]

EvaluateLogicalSameDomain ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C2"
    /\ LET skipped == ShouldSkipTimed("Logical", 1,
                                     state.live["Owner"]["LogicalHistory"])
       IN state' = [state EXCEPT
            !.pc = "C3",
            !.sameLogicalSkipped = skipped]

RejectIncompatibleModeSwitch ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C3"
    /\ state.mode = "TOQ"
    /\ RecordPresent(state.live["Recovery"]["TOQHistory"])
    /\ state' = [state EXCEPT
        !.pc = "C4",
        !.modeRejectionObserved = TRUE]

EvaluateTOQCrossDomain ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C4"
    /\ LET skipped == ShouldSkipTimed("TOQ", 1,
                                     state.live["Owner"]["LogicalHistory"])
       IN state' = [state EXCEPT
            !.pc = "C5",
            !.crossDomainSkipped = @ \/ skipped]

EvaluateTOQSameDomain ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C5"
    /\ LET skipped == ShouldSkipTimed("TOQ", 1,
                                     state.live["Recovery"]["TOQHistory"])
       IN state' = [state EXCEPT
            !.pc = "C6",
            !.sameTOQSkipped = skipped]

ProposeAfterClosedTOQFloor ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C6"
    /\ LET raw == 1
           proposed == IF MutateTOQFloorBypass
                       THEN raw
                       ELSE IF state.toqClosed /\ raw <= state.toqClosedThrough
                            THEN state.toqClosedThrough + 1
                            ELSE raw
       IN state' = [state EXCEPT
            !.pc = "C7",
            !.proposedTOQAt = proposed]

FinishCrossDomainScenario ==
    /\ Scenario = "CrossDomain"
    /\ state.pc = "C7"
    /\ state' = [state EXCEPT !.pc = "CDone"]

AdmitInitialDeferred ==
    /\ Scenario = "Deferred"
    /\ state.pc = "D0"
    /\ Cardinality(state.deferredIndex) < DeferredCapacity
    /\ LET rec == CanonicalRecord("A", "PreAccepted", "Logical", MaxTime, FALSE, 1)
           message == RecordMessage("DeferredOld", "PreAccept", "Owner", "Recovery",
                                    "Target", rec, TRUE, FALSE)
       IN state' = [state EXCEPT
            !.pc = "D1",
            !.messages["DeferredOld"] = message,
            !.deferredIndex = @ \cup {OldDeferred},
            !.logicalHeap = @ \cup {OldDeferred},
            !.capacityWait = TRUE]

DeliverDeferredDuplicate ==
    /\ Scenario = "Deferred"
    /\ state.pc = "D1"
    /\ LET original == state.messages["DeferredOld"]
           duplicate == [original EXCEPT !.id = "DeferredDup"]
       IN state' = [state EXCEPT
            !.pc = "D2",
            !.messages["DeferredDup"] = duplicate,
            !.delivered = @ \cup {"DeferredDup"}]

DeliverDeferredStale ==
    /\ Scenario = "Deferred"
    /\ state.pc = "D2"
    /\ LET rec == CanonicalRecord("A", "PreAccepted", "Logical", MaxTime, FALSE, 0)
           stale == RecordMessage("DeferredStale", "PreAccept", "Owner", "Recovery",
                                  "Target", rec, FALSE, FALSE)
       IN state' = [state EXCEPT
            !.pc = "D3",
            !.messages["DeferredStale"] = stale,
            !.delivered = @ \cup {"DeferredStale"}]

SupersedeDeferredWithHigherPhase ==
    \E status \in {"Accepted", "Committed"} :
        /\ Scenario = "Deferred"
        /\ state.pc = "D3"
        /\ LET rec == CanonicalRecord("A", status, "Logical", MaxTime, FALSE, 2)
           cleanedHeap == IF MutateOmitHeapRemoval
                          THEN state.logicalHeap
                          ELSE state.logicalHeap \ {OldDeferred}
           nextPC == IF status = "Committed" THEN "D5" ELSE "D4"
           IN state' = [state EXCEPT
                !.pc = nextPC,
                !.live["Recovery"]["Target"] = rec,
                !.deferredIndex = @ \ {OldDeferred},
                !.logicalHeap = cleanedHeap,
                !.supersessionObserved = TRUE,
                !.terminalSeen = status = "Committed"]

CommitAfterAcceptedSupersession ==
    /\ Scenario = "Deferred"
    /\ state.pc = "D4"
    /\ LET old == state.live["Recovery"]["Target"]
           committed == CanonicalRecord(old.value, "Committed", old.domain,
                                        old.processAt, FALSE, old.ballot)
           cleanedHeap == IF MutateOmitHeapRemoval
                          THEN state.logicalHeap
                          ELSE state.logicalHeap \ {OldDeferred}
       IN state' = [state EXCEPT
            !.pc = "D5",
            !.live["Recovery"]["Target"] = committed,
            !.deferredIndex = @ \ {OldDeferred},
            !.logicalHeap = cleanedHeap,
            !.terminalSeen = TRUE]

AdmitReplacementDeferred ==
    /\ Scenario = "Deferred"
    /\ state.pc = "D5"
    /\ Cardinality(state.deferredIndex) < DeferredCapacity
    /\ LET rec == CanonicalRecord("B", "PreAccepted", "Logical", MaxTime, FALSE, 1)
           message == RecordMessage("DeferredReplacement", "PreAccept", "Owner", "Recovery",
                                    "Replacement", rec, TRUE, FALSE)
       IN /\ DomainEnabled(MessageDomain(message), ModeForNode(message.to))
          /\ state' = [state EXCEPT
                !.pc = "D6",
                !.messages["DeferredReplacement"] = message,
                !.delivered = @ \cup {"DeferredReplacement"},
                !.deferredIndex = @ \cup {ReplacementDeferred},
                !.logicalHeap = @ \cup {ReplacementDeferred},
                !.capacityWait = FALSE,
                !.replacementAdmitted = TRUE]

ProcessReplacementDeferred ==
    /\ Scenario = "Deferred"
    /\ state.pc = "D6"
    /\ ReplacementDeferred \in state.deferredIndex
    /\ state' = [state EXCEPT
        !.pc = "DDone",
        !.deferredIndex = @ \ {ReplacementDeferred},
        !.logicalHeap = @ \ {ReplacementDeferred}]

MigrationStep ==
    RejectAmbiguousLegacy \/ RejectCorruptLegacy \/ InferPendingLegacyTOQ \/
    CrashBeforePendingMigrationDurable \/ PersistPendingMigrationReady \/
    CrashAfterPendingMigrationDurable \/ AdvancePendingMigrationReady \/
    RestorePendingMigrationOutputAfterCrash \/
    ConfigureExplicitMigrationDomain \/ MigrateValidLegacy \/
    CrashBeforeMigrationDurable \/ PersistMigrationReady \/
    CrashAfterMigrationDurable \/ ReleaseMigrationOutput \/
    DeliverMigrationOutput \/ RestartMigration \/
    ProposeAtMaxBoundary \/ RejectProcessAtOverflow

RecoveryStep ==
    BeginRecovery \/ (\E id \in PrepareIDs : DeliverPrepare(id)) \/
    SelectRecoveryValue \/ CrashBeforeRecoveryDurable \/ PersistRecoveredAccept \/
    CrashAfterRecoveryDurable \/ ReleaseRecoveredAccept \/ AcceptRecoveredValue \/
    PersistRecoveredCommit \/ ReleaseRecoveredCommit \/ DeliverRecoveredCommit

CrossDomainStep ==
    ExerciseCanonicalRecoveryNoop \/ RejectTimedNoop \/
    DeliverMislabeledTOQCommit \/ EvaluateLogicalCrossDomain \/
    EvaluateLogicalSameDomain \/ RejectIncompatibleModeSwitch \/
    EvaluateTOQCrossDomain \/ EvaluateTOQSameDomain \/
    ProposeAfterClosedTOQFloor \/ FinishCrossDomainScenario

DeferredStep ==
    AdmitInitialDeferred \/ DeliverDeferredDuplicate \/ DeliverDeferredStale \/
    SupersedeDeferredWithHigherPhase \/ CommitAfterAcceptedSupersession \/
    AdmitReplacementDeferred \/ ProcessReplacementDeferred
TerminalStutter ==
    /\ state.pc \in {"MDone", "RDone", "CDone", "DDone"}
    /\ UNCHANGED state

Next ==
    MigrationStep \/ RecoveryStep \/ CrossDomainStep \/ DeferredStep \/ TerminalStutter

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(MigrationStep)
    /\ WF_vars(RecoveryStep)
    /\ WF_vars(CrossDomainStep)
    /\ WF_vars(DeferredStep)

MigrationEventuallyCompletes ==
    Scenario # "MigrationRestart" \/
    <> /\ state.pc = "MDone"
       /\ state.ambiguousAttempted
       /\ state.corruptAttempted
       /\ state.pendingInferenceObserved
       /\ "DeferredReplacement" \in state.delivered
       /\ state.acceptedMigration
       /\ state.restartChecked
       /\ state.overflowRejected

MigrationOutputEventuallyDispositioned ==
    Scenario # "MigrationRestart" \/
    [] ("UpgradeOut" \in state.network =>
           <> /\ "UpgradeOut" \in state.delivered
              /\ "UpgradeOut" \in state.rejectedMessages)

CrossDomainEventuallyCompletes ==
    Scenario # "CrossDomain" \/
    <> /\ state.pc = "CDone"
       /\ state.crossEval
       /\ state.modeRejectionObserved
       /\ state.wrongDomainRejected
       /\ state.canonicalNoopObserved
       /\ state.timedNoopRejected
       /\ state.sameLogicalSkipped
       /\ state.sameTOQSkipped
       /\ state.proposedTOQAt > MaxTime - 1

RecoveryEventuallyCompletes ==
    Scenario # "Recovery" \/ <>state.recoveryDone

DeferredCapacityEventuallyReused ==
    Scenario # "Deferred" \/ [] (state.capacityWait => <>state.replacementAdmitted)

====
