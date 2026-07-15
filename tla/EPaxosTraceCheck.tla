---- MODULE EPaxosTraceCheck ----
(***************************************************************************)
(* Checked Go-trace-to-TLC action correspondence.                          *)
(*                                                                         *)
(* Each generated sequence of projected states is exported by              *)
(* tests/refinementtrace (CaptureTLA + cmd/tracetla) from a real captured   *)
(* RawNode execution. Each record binds the twelve mapped refinement-model  *)
(* variables and names the Go action label and semantic event kind that     *)
(* produced it. TraceNext requires the complete mapped-state action granted *)
(* to the audited raw (action, kind) pair; stuttering requires exact         *)
(* equality of all twelve mapped variables and is admitted only for audited *)
(* pairs listed in ALLOW_STUTTER.                                           *)
(***************************************************************************)
EXTENDS EPaxosRawNodeRefinement

CONSTANT TraceData

VARIABLES idx, traceAccepted

TraceVars == <<Vars, idx, traceAccepted>>

(***************************************************************************)
(* Decoders from the generated symbolic renderings to model values.  Roles  *)
(* rather than raw identifiers keep the projection independent of the       *)
(* CHOOSE-selected Leader/Peer values.                                      *)
(***************************************************************************)

InstOf(role) == IF role = "A" THEN A ELSE B
DecInstSet(roles) == {InstOf(r) : r \in roles}
DecInstSeq(roles) == [k \in 1..Len(roles) |-> InstOf(roles[k])]
DecConf(name) == IF name = "old" THEN OldConfig ELSE NewConfig
DecCmd(name) == IF name = "CmdA" THEN CmdA ELSE CmdB
DecNode(name) ==
    CASE name = "leader" -> Leader
      [] name = "peer1" -> Peer1
      [] name = "peer2" -> Peer2
      [] OTHER -> Spare
DecNodeSet(names) == {DecNode(n) : n \in names}
DecTuple(t) ==
    [present |-> t.present, cmd |-> DecCmd(t.cmd), seq |-> t.seq,
     deps |-> DecInstSet(t.deps), conf |-> DecConf(t.conf)]
DecRecord(role, r) ==
    InstanceRecord(InstOf(role), r.status, r.ballot, r.recordBallot,
                   DecTuple(r.tuple), Wire(0))
DecRecords(m) ==
    [i \in Instances |->
        IF i = A THEN DecRecord("A", m.instA) ELSE DecRecord("B", m.instB)]
DecEvidence(role, e) ==
    Evidence(InstOf(role), e.ballot, e.recordBallot,
             DecNodeSet(e.senders), DecTuple(e.tuple))
DecEvidences(m) ==
    [i \in Instances |->
        IF i = A THEN DecEvidence("A", m.instA) ELSE DecEvidence("B", m.instB)]
DecTuples(m) ==
    [i \in Instances |->
        IF i = A THEN DecTuple(m.instA) ELSE DecTuple(m.instB)]

(***************************************************************************)
(* This table is mirrored exactly by tracePaperPermissions in              *)
(* tests/refinementtrace/tlaexport.go and cross-checked by                  *)
(* TestTracePaperPermissionInventory. NonStutterPaper lists every pair that *)
(* may claim a paper action. NonPaperMapped lists every pair that may claim *)
(* a faithful non-paper mapped delta. ALLOW_STUTTER lists all 96 audited     *)
(* pairs; only exact twelve-variable equality may take its stutter arm.     *)
(***************************************************************************)

NonStutterPaper ==
    ("NormalPersistReady/persistence-complete" :> {"Choose", "Execute"}) @@
    ("RecoveryPersistBallot/persistence-complete" :> {"BeginRecovery"}) @@
    ("RecoveryPersistCommit/persistence-complete" :> {"Choose", "Execute"}) @@
    ("TOQPersistAllow/persistence-complete" :> {"Choose", "Execute"}) @@
    ("ConfigPersistTransition/persistence-complete" :> {"ChooseExecute"}) @@
    ("ConfigOldRefResponse/message-step" :> {"Reconfigure"}) @@
    ("ConfigPersistB/persistence-complete" :> {"Choose", "Execute"})

NonPaperMapped ==
    ("NormalPropose/propose-fast" :> {"ProposePreAccepted"}) @@
    ("NormalPersistReady/persistence-complete" :> {"PersistRecord"}) @@
    ("NormalRetrySend/message-step" :> {"CommitVolatile"}) @@
    ("RecoverySeedAccepted/message-step" :> {"SeedAccepted"}) @@
    ("RecoveryPersistAccepted/persistence-complete" :> {"PersistRecord"}) @@
    ("RecoveryStartBallot/logical-tick" :> {"TickOnly", "PromiseAndTick"}) @@
    ("RecoverySecondSender/message-step" :> {"AcceptRecoveryTuple", "CommitVolatile"}) @@
    ("RecoveryPersistCommit/persistence-complete" :> {"PersistRecord"}) @@
    ("TOQPersistPending/persistence-complete" :> {"SetTOQPending"}) @@
    ("TOQFirstTick/logical-tick" :> {"TickOnly"}) @@
    ("TOQDeadlineTick/logical-tick" :> {"TickOnly"}) @@
    ("TOQBuildAllowReady/process-toq-due" :> {"TOQCommitVolatile"}) @@
    ("ConfigProposeA/propose-config-a" :> {"ProposePreAccepted"}) @@
    ("ConfigPersistA/persistence-complete" :> {"PersistRecord"}) @@
    ("ConfigProposeB/propose-user-b" :> {"ProposePreAccepted"}) @@
    ("ConfigPersistB/persistence-complete" :> {"PersistRecord"}) @@
    ("ConfigBuildBReady/message-step" :> {"CommitVolatile"})

ALLOW_STUTTER == {
    "NormalCodecOnly/finite-scope",
    "RecoveryFrozenReadyProbe/finite-scope",
    "TOQMaxTickDrop/finite-scope",
    "ConfigDependencyBlocked/finite-scope",
    "NormalPropose/propose-fast",
    "NormalPropose/propose-conflicting",
    "NormalBuildReady/ready-observed",
    "NormalFrozenReadyProbe/frozen-ready-probe",
    "NormalPersistReady/persistence-complete",
    "NormalApply/application-acknowledged",
    "NormalAdvance/advance-complete",
    "NormalCodecOnly/canonical-message-roundtrip",
    "NormalRetrySend/message-step",
    "NormalValidationDrop/message-step",
    "NormalValidationDrop/wrong-target-step",
    "NormalDuplicateDrop/message-step",
    "RecoverySeedAccepted/seed-propose",
    "RecoverySeedAccepted/network-drop",
    "RecoverySeedAccepted/message-step",
    "RecoveryBuildAcceptedReady/ready-observed",
    "RecoveryBuildAcceptedReady/canonical-message-roundtrip",
    "RecoveryFrozenReadyProbe/frozen-ready-probe",
    "RecoveryPersistAccepted/persistence-complete",
    "RecoveryAdvanceAccepted/advance-complete",
    "RecoveryAdvanceAccepted/network-drop",
    "RecoveryCrash/crash-restart",
    "RecoveryStartBallot/logical-tick",
    "RecoveryBuildBallotReady/ready-observed",
    "RecoveryBuildBallotReady/canonical-message-roundtrip",
    "RecoveryBuildBallotReady/message-step",
    "RecoveryPersistBallot/persistence-complete",
    "RecoveryAdvanceBallot/advance-complete",
    "RecoveryAdvanceBallot/network-drop",
    "RecoveryFirstSender/message-step",
    "RecoveryDuplicateSender/message-step",
    "RecoveryStaleBallotDrop/message-step",
    "RecoveryWrongTargetDrop/message-step",
    "RecoverySecondSender/message-step",
    "RecoveryBuildCommitReady/ready-observed",
    "RecoveryBuildCommitReady/canonical-message-roundtrip",
    "RecoveryBuildCommitReady/message-step",
    "RecoveryPersistCommit/persistence-complete",
    "RecoveryPersistCommit/network-drop",
    "RecoveryAdvanceCommit/advance-complete",
    "RecoveryAdvanceCommit/network-drop",
    "RecoveryApply/application-acknowledged",
    "TOQPropose/toq-propose",
    "TOQBuildReady/ready-observed",
    "TOQFrozenReadyProbe/frozen-ready-probe",
    "TOQPersistPending/persistence-complete",
    "TOQAdvancePending/advance-complete",
    "TOQPendingApplyBlocked/pending-application-blocked",
    "TOQEarlyDecisionDrop/withhold-process-toq-before-due",
    "TOQFirstTick/logical-tick",
    "TOQFirstTick/ready-observed",
    "TOQFirstTick/persistence-complete",
    "TOQFirstTick/advance-complete",
    "TOQDeadlineTick/logical-tick",
    "TOQDeadlineTick/ready-observed",
    "TOQDeadlineTick/persistence-complete",
    "TOQDeadlineTick/advance-complete",
    "TOQMaxTickDrop/process-toq-closed-bucket",
    "TOQBuildAllowReady/process-toq-due",
    "TOQBuildAllowReady/ready-observed",
    "TOQPersistAllow/persistence-complete",
    "TOQAdvanceAllow/advance-complete",
    "TOQApply/application-acknowledged",
    "ConfigProposeA/message-step",
    "ConfigProposeA/propose-config-a",
    "ConfigBuildAReady/ready-observed",
    "ConfigBuildAReady/canonical-message-roundtrip",
    "ConfigPersistA/persistence-complete",
    "ConfigAdvanceA/advance-complete",
    "ConfigFrozenReadyProbe/frozen-ready-probe",
    "ConfigBuildTransitionReady/ready-observed",
    "ConfigBuildTransitionReady/canonical-message-roundtrip",
    "ConfigPersistTransition/persistence-complete",
    "ConfigAdvanceTransition/advance-complete",
    "ConfigStartOldRecovery/old-config-recovery-tick",
    "ConfigBuildOldBallotReady/ready-observed",
    "ConfigBuildOldBallotReady/canonical-message-roundtrip",
    "ConfigBuildOldBallotReady/message-step",
    "ConfigPersistOldBallot/persistence-complete",
    "ConfigAdvanceOldBallot/advance-complete",
    "ConfigAdvanceOldBallot/network-drop",
    "ConfigOldRefResponse/message-step",
    "ConfigWrongConfDrop/message-step",
    "ConfigProposeB/propose-user-b",
    "ConfigBuildBReady/ready-observed",
    "ConfigBuildBReady/message-step",
    "ConfigPersistB/persistence-complete",
    "ConfigAdvanceB/advance-complete",
    "ConfigDependencyBlocked/canonical-message-roundtrip",
    "ConfigApplyA/config-outcome-history-checkpoint",
    "ConfigApplyB/application-acknowledged",
    "ConfigApplyB/configuration-pinning-final"
}

PairOf(rec) == rec.lbl \o "/" \o rec.kind

(***************************************************************************)
(* Each replayed pair must satisfy a complete twelve-variable model action *)
(* selected purely by its audited raw (action, kind) key. Every action below *)
(* constrains its faithful delta and all eleven remaining mapped variables. *)
(* The rec.paper and rec.inst fields remain diagnostics and are not read.   *)
(***************************************************************************)

MappedPredicate(name) ==
    CASE name = "ProposePreAccepted" -> \E i \in Instances : TraceProposePreAccepted(i)
      [] name = "PersistRecord" -> \E i \in Instances : TracePersistRecord(i)
      [] name = "CommitVolatile" -> \E i \in Instances : TraceCommitVolatile(i)
      [] name = "SeedAccepted" -> \E i \in Instances : TraceSeedAccepted(i)
      [] name = "TickOnly" -> TraceTickOnly
      [] name = "PromiseAndTick" -> \E i \in Instances : TracePromiseAndTick(i)
      [] name = "AcceptRecoveryTuple" -> \E i \in Instances : TraceAcceptRecoveryTuple(i)
      [] name = "SetTOQPending" -> TraceSetTOQPending
      [] name = "TOQCommitVolatile" -> \E i \in Instances : TraceTOQCommitVolatile(i)
      [] OTHER -> FALSE

FullPaperPredicate(pair, name) ==
    CASE /\ pair = "NormalPersistReady/persistence-complete"
         /\ name = "Choose"
         -> \E i \in Instances : TracePersistChoose(i)
      [] /\ pair = "NormalPersistReady/persistence-complete"
         /\ name = "Execute"
         -> \E i \in Instances : TraceExecuteMapped(i)
      [] /\ pair = "RecoveryPersistBallot/persistence-complete"
         /\ name = "BeginRecovery"
         -> \E i \in Instances : TracePersistBeginRecovery(i)
      [] /\ pair = "RecoveryPersistCommit/persistence-complete"
         /\ name = "Choose"
         -> \E i \in Instances : TracePersistChoose(i)
      [] /\ pair = "RecoveryPersistCommit/persistence-complete"
         /\ name = "Execute"
         -> \E i \in Instances : TraceExecuteMapped(i)
      [] /\ pair = "TOQPersistAllow/persistence-complete"
         /\ name = "Choose"
         -> \E i \in Instances : TracePersistChooseAndClearTOQ(i)
      [] /\ pair = "TOQPersistAllow/persistence-complete"
         /\ name = "Execute"
         -> \E i \in Instances : TraceExecuteMapped(i)
      [] /\ pair = "ConfigPersistTransition/persistence-complete"
         /\ name = "ChooseExecute"
         -> \E i \in Instances : TracePersistChooseAndExecute(i)
      [] /\ pair = "ConfigOldRefResponse/message-step"
         /\ name = "Reconfigure"
         -> \E i \in Instances : TraceReconfigureCommit(i)
      [] /\ pair = "ConfigPersistB/persistence-complete"
         /\ name = "Choose"
         -> \E i \in Instances : TracePersistChoose(i)
      [] /\ pair = "ConfigPersistB/persistence-complete"
         /\ name = "Execute"
         -> \E i \in Instances : TraceExecuteMapped(i)
      [] OTHER -> FALSE

GrantedPaper(pair) ==
    IF pair \in DOMAIN NonStutterPaper THEN NonStutterPaper[pair] ELSE {}

GrantedMapped(pair) ==
    IF pair \in DOMAIN NonPaperMapped THEN NonPaperMapped[pair] ELSE {}

StepOK(rec) ==
    LET pair == PairOf(rec) IN
    /\ pair \in ALLOW_STUTTER \cup DOMAIN NonStutterPaper \cup DOMAIN NonPaperMapped
    /\ \/ /\ pair \in ALLOW_STUTTER
          /\ UNCHANGED TraceMappedVars
       \/ \E name \in GrantedMapped(pair) : MappedPredicate(name)
       \/ \E name \in GrantedPaper(pair) : FullPaperPredicate(pair, name)

InitOK ==
    /\ records = [i \in Instances |-> EmptyRecord(i)]
    /\ durableRecords = [i \in Instances |-> EmptyRecord(i)]
    /\ recoveryEvidence = [i \in Instances |-> EmptyEvidence(i)]
    /\ applied = {}
    /\ applyLog = <<>>
    /\ currentTick = 0
    /\ toqPending = FALSE
    /\ activeConfig = OldConfig
    /\ paperDecision = [i \in Instances |-> NoTuple]
    /\ paperExecuted = {}
    /\ paperEvidence = [i \in Instances |-> EmptyEvidence(i)]
    /\ paperConfig = OldConfig

TraceInit ==
    /\ idx = 1
    /\ LET rec == TraceData[1] IN
        /\ records = DecRecords(rec.records)
        /\ durableRecords = DecRecords(rec.durableRecords)
        /\ recoveryEvidence = DecEvidences(rec.recoveryEvidence)
        /\ applied = DecInstSet(rec.applied)
        /\ applyLog = DecInstSeq(rec.applyLog)
        /\ currentTick = rec.currentTick
        /\ toqPending = rec.toqPending
        /\ activeConfig = DecConf(rec.activeConfig)
        /\ paperDecision = DecTuples(rec.paperDecision)
        /\ paperExecuted = DecInstSet(rec.paperExecuted)
        /\ paperEvidence = DecEvidences(rec.paperEvidence)
        /\ paperConfig = DecConf(rec.paperConfig)
    /\ hardState = EmptyHard
    /\ durableHardState = EmptyHard
    /\ acknowledgedHardState = EmptyHard
    /\ ready = NoReady
    /\ frozenReady = NoReady
    /\ readyPersisted = FALSE
    /\ messages = {}
    /\ deadline = MaxTick
    /\ toqDecision = "none"
    /\ stage = 0
    /\ retryCount = 0
    /\ coverage = [b \in AllBranches |-> 0]
    /\ wireNonce = 0
    /\ traceAccepted = InitOK

TraceNext ==
    /\ idx < Len(TraceData)
    /\ idx' = idx + 1
    /\ LET rec == TraceData[idx + 1] IN
        /\ records' = DecRecords(rec.records)
        /\ durableRecords' = DecRecords(rec.durableRecords)
        /\ recoveryEvidence' = DecEvidences(rec.recoveryEvidence)
        /\ applied' = DecInstSet(rec.applied)
        /\ applyLog' = DecInstSeq(rec.applyLog)
        /\ currentTick' = rec.currentTick
        /\ toqPending' = rec.toqPending
        /\ activeConfig' = DecConf(rec.activeConfig)
        /\ paperDecision' = DecTuples(rec.paperDecision)
        /\ paperExecuted' = DecInstSet(rec.paperExecuted)
        /\ paperEvidence' = DecEvidences(rec.paperEvidence)
        /\ paperConfig' = DecConf(rec.paperConfig)
        /\ traceAccepted' =
               /\ traceAccepted
               /\ StepOK(rec)
               /\ (idx + 1 = Len(TraceData) =>
                       ScenarioInstances \subseteq paperExecuted')
    /\ UNCHANGED <<hardState, durableHardState, acknowledgedHardState,
         ready, frozenReady, readyPersisted, messages, deadline,
         toqDecision, stage, retryCount, coverage, wireNonce>>

TraceSpec == TraceInit /\ [][TraceNext]_TraceVars

TraceStepAccepted == traceAccepted

====
