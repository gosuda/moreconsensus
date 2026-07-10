---- MODULE EPaxosRawNodeRefinement ----
EXTENDS Naturals, Sequences, FiniteSets, TLC

(***************************************************************************)
(* This is a finite, implementation-shaped refinement slice.  It models   *)
(* semantic RawNode state and deliberately hides codec versions, checksum  *)
(* bytes, buffer ownership, and allocation epochs from the paper view.     *)
(* It is not a theorem that arbitrary Go executions refine the EPaxos      *)
(* paper.  The four configurations exercise separate bounded workflows.    *)
(***************************************************************************)

CONSTANTS Nodes, Commands, OldConfig, NewConfig, Mode, MaxTick

Configs == {OldConfig, NewConfig}
Modes == {"normal", "recovery", "toq", "config"}
Statuses == {"empty", "preaccepted", "accepted", "committed"}
Ballots == 0..2
TOQStates == {"none", "pending", "allow"}
MessageKinds == {"preaccept", "commit", "retry"}

Leader == CHOOSE n \in Nodes : TRUE
Peer1 == CHOOSE n \in Nodes \ {Leader} : TRUE
Peer2 == CHOOSE n \in Nodes \ {Leader, Peer1} : TRUE
Spare == IF Cardinality(Nodes) = 4
         THEN CHOOSE n \in Nodes \ {Leader, Peer1, Peer2} : TRUE
         ELSE Leader

A == [replica |-> Leader, instance |-> 1, conf |-> OldConfig]
B == [replica |-> Leader, instance |-> 2, conf |-> NewConfig]
Instances == {A, B}
ScenarioInstances == IF Mode = "config" THEN Instances ELSE {A}

CmdA == CHOOSE c \in Commands : TRUE
CmdB == CHOOSE c \in Commands \ {CmdA} : TRUE

OldVoters == IF Mode = "config" THEN Nodes \ {Spare} ELSE Nodes
NewVoters == IF Mode = "config" THEN Nodes \ {Peer1} ELSE Nodes
ConfigVoters(conf) == IF conf = OldConfig THEN OldVoters ELSE NewVoters

ASSUME
    /\ Mode \in Modes
    /\ OldConfig # NewConfig
    /\ Nodes \subseteq Nat
    /\ Commands \subseteq STRING
    /\ Cardinality(Commands) = 2
    /\ MaxTick = 2
    /\ IF Mode = "config"
       THEN Cardinality(Nodes) = 4
       ELSE Cardinality(Nodes) = 3

NoTuple ==
    [present |-> FALSE,
     cmd |-> CmdA,
     seq |-> 0,
     deps |-> {},
     conf |-> OldConfig]

Tuple(ref, cmd, seqNum, tupleDeps) ==
    [present |-> TRUE,
     cmd |-> cmd,
     seq |-> seqNum,
     deps |-> tupleDeps,
     conf |-> ref.conf]

TupleA == Tuple(A, CmdA, 1, {})
TupleB == Tuple(B, CmdB, 2, {A})

Wire(n) ==
    [codecVersion |-> IF n = 0 THEN 1 ELSE 2,
     checksumBit |-> IF n = 1 THEN 1 ELSE 0,
     allocationEpoch |-> n]

InstanceRecord(ref, status, ballot, recordBallot, tuple, wire) ==
    [ref |-> ref,
     status |-> status,
     ballot |-> ballot,
     recordBallot |-> recordBallot,
     tuple |-> tuple,
     wire |-> wire]

EmptyRecord(ref) == InstanceRecord(ref, "empty", 0, 0, NoTuple, Wire(0))

Hard(generation, ballot, conf, logicalTick, toq) ==
    [generation |-> generation,
     ballot |-> ballot,
     conf |-> conf,
     logicalTick |-> logicalTick,
     toq |-> toq]

EmptyHard == Hard(0, 0, OldConfig, 0, "none")

TransportPeer(ref) == IF ref.conf = NewConfig THEN Peer2 ELSE Peer1

Message(kind, attempt, targetRef, rec, hardGeneration) ==
    [kind |-> kind,
     from |-> Leader,
     to |-> TransportPeer(targetRef),
     targetRef |-> targetRef,
     ballot |-> rec.ballot,
     recordBallot |-> rec.recordBallot,
     conf |-> targetRef.conf,
     tuple |-> rec.tuple,
     hardGeneration |-> hardGeneration,
     wire |-> rec.wire,
     attempt |-> attempt]

RecoveryResponse(from, targetRef, conf, ballot, recordBallot, tuple) ==
    [from |-> from,
     targetRef |-> targetRef,
     conf |-> conf,
     ballot |-> ballot,
     recordBallot |-> recordBallot,
     tuple |-> tuple]

NoReady ==
    [present |-> FALSE,
     token |-> 0,
     hard |-> EmptyHard,
     writeRef |-> A,
     writeRecord |-> EmptyRecord(A),
     outbound |-> {},
     applySet |-> {},
     issuedAt |-> 0,
     allocationTag |-> 0]

ReadyValue(token, hs, ref, rec, outbound, applySet, issuedAt, allocationTag) ==
    [present |-> TRUE,
     token |-> token,
     hard |-> hs,
     writeRef |-> ref,
     writeRecord |-> rec,
     outbound |-> outbound,
     applySet |-> applySet,
     issuedAt |-> issuedAt,
     allocationTag |-> allocationTag]

EmptyEvidence(ref) ==
    [targetRef |-> ref,
     conf |-> ref.conf,
     ballot |-> 0,
     recordBallot |-> 0,
     senders |-> {},
     tuple |-> NoTuple]

Evidence(ref, ballot, recordBallot, senders, tuple) ==
    [targetRef |-> ref,
     conf |-> ref.conf,
     ballot |-> ballot,
     recordBallot |-> recordBallot,
     senders |-> senders,
     tuple |-> tuple]

NormalBranches ==
    {"normal-frozen-ready", "normal-retry-send", "normal-validation-drop",
     "normal-duplicate-drop", "normal-codec-hidden", "normal-advance"}
RecoveryBranches ==
    {"recovery-frozen-ready", "recovery-crash", "recovery-ballot-persisted",
     "recovery-first-sender", "recovery-duplicate-sender",
     "recovery-stale-ballot", "recovery-wrong-target",
     "recovery-second-sender", "recovery-advance"}
TOQBranches ==
    {"toq-frozen-ready", "toq-pending-apply-block",
     "toq-early-decision-drop", "toq-tick-advance",
     "toq-deadline-reached", "toq-max-tick-drop", "toq-advance"}
ConfigBranches ==
    {"config-frozen-ready", "config-transition-persisted",
     "config-old-ref-pin", "config-wrong-conf-drop",
     "config-new-ref-pin", "config-dependency-block", "config-advance"}
AllBranches == NormalBranches \cup RecoveryBranches \cup TOQBranches \cup ConfigBranches
RequiredBranches ==
    CASE Mode = "normal" -> NormalBranches
      [] Mode = "recovery" -> RecoveryBranches
      [] Mode = "toq" -> TOQBranches
      [] OTHER -> ConfigBranches

VARIABLES
    records,
    durableRecords,
    hardState,
    durableHardState,
    acknowledgedHardState,
    ready,
    frozenReady,
    readyPersisted,
    messages,
    recoveryEvidence,
    applied,
    applyLog,
    currentTick,
    deadline,
    toqPending,
    toqDecision,
    activeConfig,
    stage,
    retryCount,
    coverage,
    wireNonce,
    paperDecision,
    paperExecuted,
    paperEvidence,
    paperConfig

Vars ==
    <<records, durableRecords, hardState, durableHardState,
      acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
      recoveryEvidence, applied, applyLog, currentTick, deadline,
      toqPending, toqDecision, activeConfig, stage, retryCount, coverage,
      wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

PaperVars == <<paperDecision, paperExecuted, paperEvidence, paperConfig>>

MappedDecision ==
    [i \in Instances |->
        IF durableRecords[i].status = "committed"
        THEN durableRecords[i].tuple
        ELSE NoTuple]
MappedExecuted == applied
MappedEvidence ==
    [i \in Instances |->
        [targetRef |-> recoveryEvidence[i].targetRef,
         conf |-> recoveryEvidence[i].conf,
         ballot |-> recoveryEvidence[i].ballot,
         recordBallot |-> recoveryEvidence[i].recordBallot,
         senders |-> recoveryEvidence[i].senders,
         tuple |-> recoveryEvidence[i].tuple]]
MappedConfig == activeConfig

MappingInvariant ==
    /\ paperDecision = MappedDecision
    /\ paperExecuted = MappedExecuted
    /\ paperEvidence = MappedEvidence
    /\ paperConfig = MappedConfig

Hit(branch) == coverage' = [coverage EXCEPT ![branch] = 1]

Init ==
    /\ records = [i \in Instances |-> EmptyRecord(i)]
    /\ durableRecords = [i \in Instances |-> EmptyRecord(i)]
    /\ hardState = EmptyHard
    /\ durableHardState = EmptyHard
    /\ acknowledgedHardState = EmptyHard
    /\ ready = NoReady
    /\ frozenReady = NoReady
    /\ readyPersisted = FALSE
    /\ messages = {}
    /\ recoveryEvidence = [i \in Instances |-> EmptyEvidence(i)]
    /\ applied = {}
    /\ applyLog = <<>>
    /\ currentTick = 0
    /\ deadline = MaxTick
    /\ toqPending = FALSE
    /\ toqDecision = "none"
    /\ activeConfig = OldConfig
    /\ stage = 0
    /\ retryCount = 0
    /\ coverage = [b \in AllBranches |-> 0]
    /\ wireNonce = 0
    /\ paperDecision = [i \in Instances |-> NoTuple]
    /\ paperExecuted = {}
    /\ paperEvidence = [i \in Instances |-> EmptyEvidence(i)]
    /\ paperConfig = OldConfig

(***************************************************************************)
(* Paper-shaped action relation.  Every Progress transition below must be  *)
(* one of these abstract actions or a stutter of PaperVars.                 *)
(***************************************************************************)

PaperChoose(i) ==
    /\ paperDecision[i] = NoTuple
    /\ paperDecision'[i] # NoTuple
    /\ paperDecision'[i].conf = i.conf
    /\ \A other \in Instances \ {i} :
           paperDecision'[other] = paperDecision[other]
    /\ UNCHANGED <<paperExecuted, paperEvidence, paperConfig>>

PaperExecute(i) ==
    /\ paperDecision[i] # NoTuple
    /\ i \notin paperExecuted
    /\ paperExecuted' = paperExecuted \cup {i}
    /\ UNCHANGED <<paperDecision, paperEvidence, paperConfig>>

PaperBeginRecovery(i) ==
    /\ paperEvidence[i].senders = {}
    /\ paperEvidence'[i].targetRef = i
    /\ paperEvidence'[i].conf = i.conf
    /\ paperEvidence'[i].ballot > paperEvidence[i].ballot
    /\ paperEvidence'[i].recordBallot <= paperEvidence'[i].ballot
    /\ paperEvidence'[i].senders = {}
    /\ paperEvidence'[i].tuple # NoTuple
    /\ \A other \in Instances \ {i} :
           paperEvidence'[other] = paperEvidence[other]
    /\ UNCHANGED <<paperDecision, paperExecuted, paperConfig>>

PaperObserveRecovery(i) ==
    /\ paperEvidence'[i].targetRef = paperEvidence[i].targetRef
    /\ paperEvidence'[i].conf = paperEvidence[i].conf
    /\ paperEvidence'[i].ballot = paperEvidence[i].ballot
    /\ paperEvidence'[i].recordBallot = paperEvidence[i].recordBallot
    /\ paperEvidence'[i].tuple = paperEvidence[i].tuple
    /\ paperEvidence[i].senders \subseteq paperEvidence'[i].senders
    /\ Cardinality(paperEvidence'[i].senders) =
       Cardinality(paperEvidence[i].senders) + 1
    /\ \A other \in Instances \ {i} :
           paperEvidence'[other] = paperEvidence[other]
    /\ UNCHANGED <<paperDecision, paperExecuted, paperConfig>>

PaperReconfigure ==
    /\ paperConfig = OldConfig
    /\ paperConfig' = NewConfig
    /\ UNCHANGED <<paperDecision, paperExecuted, paperEvidence>>

PaperNext ==
    \/ \E i \in Instances : PaperChoose(i)
    \/ \E i \in Instances : PaperExecute(i)
    \/ \E i \in Instances : PaperBeginRecovery(i)
    \/ \E i \in Instances : PaperObserveRecovery(i)
    \/ PaperReconfigure

RefinementAction == PaperNext \/ UNCHANGED PaperVars
RefinementProperty == [][RefinementAction]_PaperVars

FrozenReadyAction ==
    ready.present /\ ready'.present => ready' = ready

FrozenReadyUntilAdvance == [][FrozenReadyAction]_Vars

AdvanceOnlyAcknowledgesAction ==
    ready.present /\ ~ready'.present =>
        /\ readyPersisted
        /\ acknowledgedHardState' = ready.hard
        /\ durableRecords' = durableRecords
        /\ durableHardState' = durableHardState
        /\ messages' = messages
        /\ applied' = applied
        /\ applyLog' = applyLog
        /\ UNCHANGED PaperVars

AdvanceOnlyAcknowledges == [][AdvanceOnlyAcknowledgesAction]_Vars

(***************************************************************************)
(* Normal workflow.                                                        *)
(***************************************************************************)

NormalPropose ==
    /\ Mode = "normal"
    /\ stage = 0
    /\ records' = [records EXCEPT
          ![A] = InstanceRecord(A, "preaccepted", 0, 0, TupleA, Wire(0))]
    /\ stage' = 1
    /\ UNCHANGED <<durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

NormalBuildReady ==
    /\ Mode = "normal"
    /\ stage = 1
    /\ LET committed == InstanceRecord(A, "committed", 0, 0, TupleA, records[A].wire)
           hs == Hard(1, 0, OldConfig, currentTick, "none")
           outbound == {Message("commit", 0, A, committed, hs.generation)}
           rdy == ReadyValue(1, hs, A, committed, outbound, {}, currentTick, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 2
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

NormalFrozenReadyProbe ==
    /\ Mode = "normal"
    /\ stage = 2
    /\ ready = frozenReady
    /\ Hit("normal-frozen-ready")
    /\ stage' = 3
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

NormalPersistReady ==
    /\ Mode = "normal"
    /\ stage = 3
    /\ ready.present
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ records' = [records EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ messages' = messages \cup ready.outbound
    /\ readyPersisted' = TRUE
    /\ paperDecision' = [paperDecision EXCEPT ![A] = ready.writeRecord.tuple]
    /\ stage' = 4
    /\ UNCHANGED <<hardState, acknowledgedHardState, ready, frozenReady,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperExecuted, paperEvidence, paperConfig>>

NormalRetrySend ==
    /\ Mode = "normal"
    /\ stage = 4
    /\ LET retry == Message("retry", 1, A, durableRecords[A],
                             durableHardState.generation)
       IN messages' = messages \cup {retry}
    /\ retryCount' = 1
    /\ Hit("normal-retry-send")
    /\ stage' = 5
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, wireNonce, paperDecision,
         paperExecuted, paperEvidence, paperConfig>>

NormalValidationDrop ==
    /\ Mode = "normal"
    /\ stage = 5
    /\ LET invalid == RecoveryResponse(Peer1, A, NewConfig, 0, 0, TupleA)
       IN invalid.conf # invalid.targetRef.conf
    /\ Hit("normal-validation-drop")
    /\ stage' = 6
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

NormalDuplicateDrop ==
    /\ Mode = "normal"
    /\ stage = 6
    /\ LET duplicate == Message("retry", 1, A, durableRecords[A],
                                 durableHardState.generation)
       IN duplicate \in messages
    /\ Hit("normal-duplicate-drop")
    /\ stage' = 7
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

NormalAdvance ==
    /\ Mode = "normal"
    /\ stage = 7
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ Hit("normal-advance")
    /\ stage' = 8
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

NormalCodecOnly ==
    /\ Mode = "normal"
    /\ stage = 8
    /\ wireNonce' = 1
    /\ records' = [records EXCEPT
          ![A] = InstanceRecord(A, @.status, @.ballot, @.recordBallot, @.tuple, Wire(1))]
    /\ Hit("normal-codec-hidden")
    /\ stage' = 9
    /\ UNCHANGED <<durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, paperDecision,
         paperExecuted, paperEvidence, paperConfig>>

NormalApply ==
    /\ Mode = "normal"
    /\ stage = 9
    /\ durableRecords[A].status = "committed"
    /\ applied' = applied \cup {A}
    /\ applyLog' = Append(applyLog, A)
    /\ paperExecuted' = paperExecuted \cup {A}
    /\ stage' = 10
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperEvidence, paperConfig>>

NormalProgress ==
    \/ NormalPropose
    \/ NormalBuildReady
    \/ NormalFrozenReadyProbe
    \/ NormalPersistReady
    \/ NormalRetrySend
    \/ NormalValidationDrop
    \/ NormalDuplicateDrop
    \/ NormalAdvance
    \/ NormalCodecOnly
    \/ NormalApply

(***************************************************************************)
(* Recovery workflow.  Sender evidence is a set, so the duplicate branch   *)
(* cannot count a sender twice.  Replies are admitted only after the new    *)
(* ballot and the old recordBallot have been made durable.                  *)
(***************************************************************************)

RecoverySeedAccepted ==
    /\ Mode = "recovery"
    /\ stage = 0
    /\ records' = [records EXCEPT
          ![A] = InstanceRecord(A, "accepted", 0, 0, TupleA, Wire(0))]
    /\ stage' = 1
    /\ UNCHANGED <<durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryBuildAcceptedReady ==
    /\ Mode = "recovery"
    /\ stage = 1
    /\ LET hs == Hard(1, 0, OldConfig, 0, "none")
           outbound == {Message("preaccept", 0, A, records[A], hs.generation)}
           rdy == ReadyValue(1, hs, A, records[A], outbound, {}, 0, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 2
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

RecoveryFrozenReadyProbe ==
    /\ Mode = "recovery"
    /\ stage = 2
    /\ ready = frozenReady
    /\ Hit("recovery-frozen-ready")
    /\ stage' = 3
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryPersistAccepted ==
    /\ Mode = "recovery"
    /\ stage = 3
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ messages' = messages \cup ready.outbound
    /\ readyPersisted' = TRUE
    /\ stage' = 4
    /\ UNCHANGED <<records, hardState, acknowledgedHardState, ready,
         frozenReady, recoveryEvidence, applied, applyLog, currentTick,
         deadline, toqPending, toqDecision, activeConfig, retryCount,
         coverage, wireNonce, paperDecision, paperExecuted, paperEvidence,
         paperConfig>>

RecoveryAdvanceAccepted ==
    /\ Mode = "recovery"
    /\ stage = 4
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ stage' = 5
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryCrash ==
    /\ Mode = "recovery"
    /\ stage = 5
    /\ records' = durableRecords
    /\ hardState' = durableHardState
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ Hit("recovery-crash")
    /\ stage' = 6
    /\ UNCHANGED <<durableRecords, durableHardState, acknowledgedHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryStartBallot ==
    /\ Mode = "recovery"
    /\ stage = 6
    /\ records' = [records EXCEPT
          ![A] = InstanceRecord(A, "accepted", 1, @.recordBallot, @.tuple, @.wire)]
    /\ hardState' = Hard(2, 1, OldConfig, 0, "none")
    /\ recoveryEvidence' = [recoveryEvidence EXCEPT
          ![A] = Evidence(A, 1, durableRecords[A].recordBallot, {},
                         durableRecords[A].tuple)]
    /\ paperEvidence' = [paperEvidence EXCEPT
          ![A] = Evidence(A, 1, durableRecords[A].recordBallot, {},
                         durableRecords[A].tuple)]
    /\ stage' = 7
    /\ UNCHANGED <<durableRecords, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         applied, applyLog, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperExecuted, paperConfig>>

RecoveryBuildBallotReady ==
    /\ Mode = "recovery"
    /\ stage = 7
    /\ LET rdy == ReadyValue(2, hardState, A, records[A], {}, {}, 0, wireNonce)
       IN /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 8
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

RecoveryPersistBallot ==
    /\ Mode = "recovery"
    /\ stage = 8
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ readyPersisted' = TRUE
    /\ Hit("recovery-ballot-persisted")
    /\ stage' = 9
    /\ UNCHANGED <<records, hardState, acknowledgedHardState, ready,
         frozenReady, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, wireNonce, paperDecision, paperExecuted, paperEvidence,
         paperConfig>>

RecoveryAdvanceBallot ==
    /\ Mode = "recovery"
    /\ stage = 9
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ stage' = 10
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryFirstSender ==
    /\ Mode = "recovery"
    /\ stage = 10
    /\ LET response == RecoveryResponse(Peer1, A, OldConfig, 1, 0, TupleA)
       IN /\ response.from \notin recoveryEvidence[A].senders
          /\ response.targetRef = recoveryEvidence[A].targetRef
          /\ response.conf = recoveryEvidence[A].conf
          /\ response.ballot = durableRecords[A].ballot
          /\ response.ballot = recoveryEvidence[A].ballot
          /\ response.recordBallot = recoveryEvidence[A].recordBallot
          /\ response.tuple = recoveryEvidence[A].tuple
    /\ recoveryEvidence' = [recoveryEvidence EXCEPT
          ![A].senders = @ \cup {Peer1}]
    /\ paperEvidence' = [paperEvidence EXCEPT
          ![A].senders = @ \cup {Peer1}]
    /\ Hit("recovery-first-sender")
    /\ stage' = 11
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         applied, applyLog, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, wireNonce, paperDecision, paperExecuted,
         paperConfig>>

RecoveryDuplicateSender ==
    /\ Mode = "recovery"
    /\ stage = 11
    /\ LET duplicate == RecoveryResponse(Peer1, A, OldConfig, 1, 0, TupleA)
       IN /\ duplicate.from \in recoveryEvidence[A].senders
          /\ duplicate.targetRef = recoveryEvidence[A].targetRef
          /\ duplicate.ballot = recoveryEvidence[A].ballot
    /\ Hit("recovery-duplicate-sender")
    /\ stage' = 12
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryStaleBallotDrop ==
    /\ Mode = "recovery"
    /\ stage = 12
    /\ LET stale == RecoveryResponse(Peer2, A, OldConfig, 0, 0, TupleA)
       IN /\ stale.targetRef = recoveryEvidence[A].targetRef
          /\ stale.conf = recoveryEvidence[A].conf
          /\ stale.ballot < recoveryEvidence[A].ballot
    /\ Hit("recovery-stale-ballot")
    /\ stage' = 13
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryWrongTargetDrop ==
    /\ Mode = "recovery"
    /\ stage = 13
    /\ LET wrongTarget == RecoveryResponse(Peer2, B, NewConfig, 1, 0, TupleB)
       IN /\ wrongTarget.targetRef # recoveryEvidence[A].targetRef
          /\ wrongTarget.ballot = recoveryEvidence[A].ballot
    /\ Hit("recovery-wrong-target")
    /\ stage' = 14
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoverySecondSender ==
    /\ Mode = "recovery"
    /\ stage = 14
    /\ LET response == RecoveryResponse(Peer2, A, OldConfig, 1, 0, TupleA)
       IN /\ response.from \notin recoveryEvidence[A].senders
          /\ response.targetRef = recoveryEvidence[A].targetRef
          /\ response.conf = recoveryEvidence[A].conf
          /\ response.ballot = recoveryEvidence[A].ballot
          /\ response.recordBallot = recoveryEvidence[A].recordBallot
          /\ response.tuple = recoveryEvidence[A].tuple
    /\ recoveryEvidence' = [recoveryEvidence EXCEPT
          ![A].senders = @ \cup {Peer2}]
    /\ paperEvidence' = [paperEvidence EXCEPT
          ![A].senders = @ \cup {Peer2}]
    /\ Hit("recovery-second-sender")
    /\ stage' = 15
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         applied, applyLog, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, wireNonce, paperDecision, paperExecuted,
         paperConfig>>

RecoveryBuildCommitReady ==
    /\ Mode = "recovery"
    /\ stage = 15
    /\ LET committed == InstanceRecord(A, "committed", 1, 1,
                                recoveryEvidence[A].tuple, records[A].wire)
           hs == Hard(3, 1, OldConfig, 0, "none")
           outbound == {Message("commit", 0, A, committed, hs.generation)}
           rdy == ReadyValue(3, hs, A, committed, outbound, {}, 0, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 16
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

RecoveryPersistCommit ==
    /\ Mode = "recovery"
    /\ stage = 16
    /\ records' = [records EXCEPT ![A] = ready.writeRecord]
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ messages' = messages \cup ready.outbound
    /\ readyPersisted' = TRUE
    /\ paperDecision' = [paperDecision EXCEPT ![A] = ready.writeRecord.tuple]
    /\ stage' = 17
    /\ UNCHANGED <<hardState, acknowledgedHardState, ready, frozenReady,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperExecuted, paperEvidence, paperConfig>>

RecoveryAdvanceCommit ==
    /\ Mode = "recovery"
    /\ stage = 17
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ Hit("recovery-advance")
    /\ stage' = 18
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

RecoveryApply ==
    /\ Mode = "recovery"
    /\ stage = 18
    /\ durableRecords[A].status = "committed"
    /\ applied' = applied \cup {A}
    /\ applyLog' = Append(applyLog, A)
    /\ paperExecuted' = paperExecuted \cup {A}
    /\ stage' = 19
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperEvidence, paperConfig>>

RecoveryProgress ==
    \/ RecoverySeedAccepted
    \/ RecoveryBuildAcceptedReady
    \/ RecoveryFrozenReadyProbe
    \/ RecoveryPersistAccepted
    \/ RecoveryAdvanceAccepted
    \/ RecoveryCrash
    \/ RecoveryStartBallot
    \/ RecoveryBuildBallotReady
    \/ RecoveryPersistBallot
    \/ RecoveryAdvanceBallot
    \/ RecoveryFirstSender
    \/ RecoveryDuplicateSender
    \/ RecoveryStaleBallotDrop
    \/ RecoveryWrongTargetDrop
    \/ RecoverySecondSender
    \/ RecoveryBuildCommitReady
    \/ RecoveryPersistCommit
    \/ RecoveryAdvanceCommit
    \/ RecoveryApply

(***************************************************************************)
(* TOQ workflow.  A chosen tuple is not application-ready while the durable *)
(* TOQ decision is pending.  Logical ticks and rejected early/overflow      *)
(* attempts refine stuttering steps.                                        *)
(***************************************************************************)

TOQPropose ==
    /\ Mode = "toq"
    /\ stage = 0
    /\ records' = [records EXCEPT
          ![A] = InstanceRecord(A, "preaccepted", 0, 0, TupleA, Wire(0))]
    /\ toqPending' = TRUE
    /\ toqDecision' = "pending"
    /\ stage' = 1
    /\ UNCHANGED <<durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperExecuted, paperEvidence, paperConfig>>

TOQBuildReady ==
    /\ Mode = "toq"
    /\ stage = 1
    /\ LET committed == InstanceRecord(A, "committed", 0, 0, TupleA, records[A].wire)
           hs == Hard(1, 0, OldConfig, currentTick, "pending")
           outbound == {Message("commit", 0, A, committed, hs.generation)}
           rdy == ReadyValue(1, hs, A, committed, outbound, {}, currentTick, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 2
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

TOQFrozenReadyProbe ==
    /\ Mode = "toq"
    /\ stage = 2
    /\ ready = frozenReady
    /\ Hit("toq-frozen-ready")
    /\ stage' = 3
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

TOQPersistPending ==
    /\ Mode = "toq"
    /\ stage = 3
    /\ records' = [records EXCEPT ![A] = ready.writeRecord]
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ messages' = messages \cup ready.outbound
    /\ readyPersisted' = TRUE
    /\ paperDecision' = [paperDecision EXCEPT ![A] = ready.writeRecord.tuple]
    /\ stage' = 4
    /\ UNCHANGED <<hardState, acknowledgedHardState, ready, frozenReady,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperExecuted, paperEvidence, paperConfig>>

TOQAdvancePending ==
    /\ Mode = "toq"
    /\ stage = 4
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ Hit("toq-advance")
    /\ stage' = 5
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

TOQPendingApplyBlocked ==
    /\ Mode = "toq"
    /\ stage = 5
    /\ durableHardState.toq = "pending"
    /\ A \notin applied
    /\ Hit("toq-pending-apply-block")
    /\ stage' = 6
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

TOQEarlyDecisionDrop ==
    /\ Mode = "toq"
    /\ stage = 6
    /\ currentTick < deadline
    /\ Hit("toq-early-decision-drop")
    /\ stage' = 7
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

TOQFirstTick ==
    /\ Mode = "toq"
    /\ stage = 7
    /\ currentTick = 0
    /\ currentTick' = 1
    /\ hardState' = [hardState EXCEPT !.logicalTick = 1]
    /\ Hit("toq-tick-advance")
    /\ stage' = 8
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, deadline, toqPending,
         toqDecision, activeConfig, retryCount, wireNonce, paperDecision,
         paperExecuted, paperEvidence, paperConfig>>

TOQDeadlineTick ==
    /\ Mode = "toq"
    /\ stage = 8
    /\ currentTick = 1
    /\ currentTick' = deadline
    /\ hardState' = [hardState EXCEPT !.logicalTick = deadline]
    /\ Hit("toq-deadline-reached")
    /\ stage' = 9
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, deadline, toqPending,
         toqDecision, activeConfig, retryCount, wireNonce, paperDecision,
         paperExecuted, paperEvidence, paperConfig>>

TOQMaxTickDrop ==
    /\ Mode = "toq"
    /\ stage = 9
    /\ currentTick = MaxTick
    /\ Hit("toq-max-tick-drop")
    /\ stage' = 10
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

TOQBuildAllowReady ==
    /\ Mode = "toq"
    /\ stage = 10
    /\ currentTick = deadline
    /\ LET hs == Hard(2, 0, OldConfig, currentTick, "allow")
           rdy == ReadyValue(2, hs, A, records[A], {}, {A}, currentTick, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ toqPending' = FALSE
    /\ toqDecision' = "allow"
    /\ stage' = 11
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, activeConfig, retryCount, coverage, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

TOQPersistAllow ==
    /\ Mode = "toq"
    /\ stage = 11
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ readyPersisted' = TRUE
    /\ stage' = 12
    /\ UNCHANGED <<records, hardState, acknowledgedHardState, ready,
         frozenReady, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

TOQAdvanceAllow ==
    /\ Mode = "toq"
    /\ stage = 12
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ stage' = 13
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

TOQApply ==
    /\ Mode = "toq"
    /\ stage = 13
    /\ durableRecords[A].status = "committed"
    /\ durableHardState.toq = "allow"
    /\ applied' = applied \cup {A}
    /\ applyLog' = Append(applyLog, A)
    /\ paperExecuted' = paperExecuted \cup {A}
    /\ stage' = 14
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperEvidence, paperConfig>>

TOQProgress ==
    \/ TOQPropose
    \/ TOQBuildReady
    \/ TOQFrozenReadyProbe
    \/ TOQPersistPending
    \/ TOQAdvancePending
    \/ TOQPendingApplyBlocked
    \/ TOQEarlyDecisionDrop
    \/ TOQFirstTick
    \/ TOQDeadlineTick
    \/ TOQMaxTickDrop
    \/ TOQBuildAllowReady
    \/ TOQPersistAllow
    \/ TOQAdvanceAllow
    \/ TOQApply

(***************************************************************************)
(* Configuration workflow.  A remains pinned to OldConfig after the active *)
(* configuration becomes NewConfig.  B is pinned to NewConfig and depends  *)
(* on A, so its early application attempt is blocked.                       *)
(***************************************************************************)

ConfigProposeA ==
    /\ Mode = "config"
    /\ stage = 0
    /\ records' = [records EXCEPT
          ![A] = InstanceRecord(A, "preaccepted", 0, 0, TupleA, Wire(0))]
    /\ stage' = 1
    /\ UNCHANGED <<durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigBuildAReady ==
    /\ Mode = "config"
    /\ stage = 1
    /\ LET committed == InstanceRecord(A, "committed", 0, 0, TupleA, records[A].wire)
           hs == Hard(1, 0, OldConfig, 0, "none")
           outbound == {Message("commit", 0, A, committed, hs.generation)}
           rdy == ReadyValue(1, hs, A, committed, outbound, {}, 0, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 2
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

ConfigPersistA ==
    /\ Mode = "config"
    /\ stage = 2
    /\ records' = [records EXCEPT ![A] = ready.writeRecord]
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ messages' = messages \cup ready.outbound
    /\ readyPersisted' = TRUE
    /\ paperDecision' = [paperDecision EXCEPT ![A] = ready.writeRecord.tuple]
    /\ stage' = 3
    /\ UNCHANGED <<hardState, acknowledgedHardState, ready, frozenReady,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperExecuted, paperEvidence, paperConfig>>

ConfigAdvanceA ==
    /\ Mode = "config"
    /\ stage = 3
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ stage' = 4
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigBuildTransitionReady ==
    /\ Mode = "config"
    /\ stage = 4
    /\ LET hs == Hard(2, 0, NewConfig, 0, "none")
           rdy == ReadyValue(2, hs, A, records[A], {}, {}, 0, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 5
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

ConfigFrozenReadyProbe ==
    /\ Mode = "config"
    /\ stage = 5
    /\ ready = frozenReady
    /\ Hit("config-frozen-ready")
    /\ stage' = 6
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigPersistTransition ==
    /\ Mode = "config"
    /\ stage = 6
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ readyPersisted' = TRUE
    /\ activeConfig' = NewConfig
    /\ paperConfig' = NewConfig
    /\ Hit("config-transition-persisted")
    /\ stage' = 7
    /\ UNCHANGED <<records, hardState, acknowledgedHardState, ready,
         frozenReady, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, retryCount,
         wireNonce, paperDecision, paperExecuted, paperEvidence>>

ConfigAdvanceTransition ==
    /\ Mode = "config"
    /\ stage = 7
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ Hit("config-advance")
    /\ stage' = 8
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigStartOldRecovery ==
    /\ Mode = "config"
    /\ stage = 8
    /\ activeConfig = NewConfig
    /\ records' = [records EXCEPT
          ![A] = InstanceRecord(A, "committed", 1, @.recordBallot, @.tuple, @.wire)]
    /\ hardState' = Hard(3, 1, NewConfig, 0, "none")
    /\ recoveryEvidence' = [recoveryEvidence EXCEPT
          ![A] = Evidence(A, 1, durableRecords[A].recordBallot, {},
                         durableRecords[A].tuple)]
    /\ paperEvidence' = [paperEvidence EXCEPT
          ![A] = Evidence(A, 1, durableRecords[A].recordBallot, {},
                         durableRecords[A].tuple)]
    /\ stage' = 9
    /\ UNCHANGED <<durableRecords, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         applied, applyLog, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperExecuted, paperConfig>>

ConfigBuildOldBallotReady ==
    /\ Mode = "config"
    /\ stage = 9
    /\ LET rdy == ReadyValue(3, hardState, A, records[A], {}, {}, 0, wireNonce)
       IN /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 10
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

ConfigPersistOldBallot ==
    /\ Mode = "config"
    /\ stage = 10
    /\ durableRecords' = [durableRecords EXCEPT ![A] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ readyPersisted' = TRUE
    /\ stage' = 11
    /\ UNCHANGED <<records, hardState, acknowledgedHardState, ready,
         frozenReady, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

ConfigAdvanceOldBallot ==
    /\ Mode = "config"
    /\ stage = 11
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ stage' = 12
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigOldRefResponse ==
    /\ Mode = "config"
    /\ stage = 12
    /\ LET response == RecoveryResponse(Peer1, A, OldConfig, 1, 0, TupleA)
       IN /\ response.targetRef = A
          /\ response.conf = A.conf
          /\ response.ballot = durableRecords[A].ballot
    /\ recoveryEvidence' = [recoveryEvidence EXCEPT
          ![A].senders = @ \cup {Peer1}]
    /\ paperEvidence' = [paperEvidence EXCEPT
          ![A].senders = @ \cup {Peer1}]
    /\ Hit("config-old-ref-pin")
    /\ stage' = 13
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         applied, applyLog, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, wireNonce, paperDecision, paperExecuted,
         paperConfig>>

ConfigWrongConfDrop ==
    /\ Mode = "config"
    /\ stage = 13
    /\ LET wrongConf == RecoveryResponse(Peer2, A, NewConfig, 1, 0, TupleA)
       IN /\ wrongConf.targetRef = recoveryEvidence[A].targetRef
          /\ wrongConf.ballot = recoveryEvidence[A].ballot
          /\ wrongConf.conf # wrongConf.targetRef.conf
    /\ Hit("config-wrong-conf-drop")
    /\ stage' = 14
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigProposeB ==
    /\ Mode = "config"
    /\ stage = 14
    /\ activeConfig = NewConfig
    /\ records' = [records EXCEPT
          ![B] = InstanceRecord(B, "preaccepted", 0, 0, TupleB, Wire(0))]
    /\ Hit("config-new-ref-pin")
    /\ stage' = 15
    /\ UNCHANGED <<durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigBuildBReady ==
    /\ Mode = "config"
    /\ stage = 15
    /\ LET committed == InstanceRecord(B, "committed", 0, 0, TupleB, records[B].wire)
           hs == Hard(4, hardState.ballot, NewConfig, 0, "none")
           outbound == {Message("commit", 0, B, committed, hs.generation)}
           rdy == ReadyValue(4, hs, B, committed, outbound, {}, 0, wireNonce)
       IN /\ hardState' = hs
          /\ ready' = rdy
          /\ frozenReady' = rdy
    /\ readyPersisted' = FALSE
    /\ stage' = 16
    /\ UNCHANGED <<records, durableRecords, durableHardState,
         acknowledgedHardState, messages, recoveryEvidence, applied, applyLog,
         currentTick, deadline, toqPending, toqDecision, activeConfig,
         retryCount, coverage, wireNonce, paperDecision, paperExecuted,
         paperEvidence, paperConfig>>

ConfigPersistB ==
    /\ Mode = "config"
    /\ stage = 16
    /\ records' = [records EXCEPT ![B] = ready.writeRecord]
    /\ durableRecords' = [durableRecords EXCEPT ![B] = ready.writeRecord]
    /\ durableHardState' = ready.hard
    /\ messages' = messages \cup ready.outbound
    /\ readyPersisted' = TRUE
    /\ paperDecision' = [paperDecision EXCEPT ![B] = ready.writeRecord.tuple]
    /\ stage' = 17
    /\ UNCHANGED <<hardState, acknowledgedHardState, ready, frozenReady,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperExecuted, paperEvidence, paperConfig>>

ConfigAdvanceB ==
    /\ Mode = "config"
    /\ stage = 17
    /\ readyPersisted
    /\ acknowledgedHardState' = ready.hard
    /\ ready' = NoReady
    /\ frozenReady' = NoReady
    /\ readyPersisted' = FALSE
    /\ stage' = 18
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         messages, recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, coverage,
         wireNonce, paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigDependencyBlocked ==
    /\ Mode = "config"
    /\ stage = 18
    /\ A \in durableRecords[B].tuple.deps
    /\ A \notin applied
    /\ Hit("config-dependency-block")
    /\ stage' = 19
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, applied, applyLog, currentTick, deadline,
         toqPending, toqDecision, activeConfig, retryCount, wireNonce,
         paperDecision, paperExecuted, paperEvidence, paperConfig>>

ConfigApplyA ==
    /\ Mode = "config"
    /\ stage = 19
    /\ durableRecords[A].status = "committed"
    /\ applied' = applied \cup {A}
    /\ applyLog' = Append(applyLog, A)
    /\ paperExecuted' = paperExecuted \cup {A}
    /\ stage' = 20
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperEvidence, paperConfig>>

ConfigApplyB ==
    /\ Mode = "config"
    /\ stage = 20
    /\ durableRecords[B].tuple.deps \subseteq applied
    /\ applied' = applied \cup {B}
    /\ applyLog' = Append(applyLog, B)
    /\ paperExecuted' = paperExecuted \cup {B}
    /\ stage' = 21
    /\ UNCHANGED <<records, durableRecords, hardState, durableHardState,
         acknowledgedHardState, ready, frozenReady, readyPersisted, messages,
         recoveryEvidence, currentTick, deadline, toqPending, toqDecision,
         activeConfig, retryCount, coverage, wireNonce, paperDecision,
         paperEvidence, paperConfig>>

ConfigProgress ==
    \/ ConfigProposeA
    \/ ConfigBuildAReady
    \/ ConfigPersistA
    \/ ConfigAdvanceA
    \/ ConfigBuildTransitionReady
    \/ ConfigFrozenReadyProbe
    \/ ConfigPersistTransition
    \/ ConfigAdvanceTransition
    \/ ConfigStartOldRecovery
    \/ ConfigBuildOldBallotReady
    \/ ConfigPersistOldBallot
    \/ ConfigAdvanceOldBallot
    \/ ConfigOldRefResponse
    \/ ConfigWrongConfDrop
    \/ ConfigProposeB
    \/ ConfigBuildBReady
    \/ ConfigPersistB
    \/ ConfigAdvanceB
    \/ ConfigDependencyBlocked
    \/ ConfigApplyA
    \/ ConfigApplyB

Progress ==
    CASE Mode = "normal" -> NormalProgress
      [] Mode = "recovery" -> RecoveryProgress
      [] Mode = "toq" -> TOQProgress
      [] OTHER -> ConfigProgress

Next == Progress

(***************************************************************************)
(* Types and checked invariants.                                            *)
(***************************************************************************)

TupleType ==
    [present : {TRUE}, cmd : Commands, seq : 0..2,
     deps : SUBSET Instances, conf : Configs]
WireType ==
    [codecVersion : 1..2, checksumBit : 0..1, allocationEpoch : 0..2]
RecordType ==
    [ref : Instances, status : Statuses, ballot : Ballots,
     recordBallot : Ballots, tuple : TupleType \cup {NoTuple}, wire : WireType]
HardType ==
    [generation : 0..4, ballot : Ballots, conf : Configs,
     logicalTick : 0..MaxTick, toq : TOQStates]
MessageType ==
    [kind : MessageKinds, from : Nodes, to : Nodes, targetRef : Instances,
     ballot : Ballots, recordBallot : Ballots,
     conf : Configs, tuple : TupleType \cup {NoTuple},
     hardGeneration : 0..4, wire : WireType, attempt : 0..1]
EvidenceType ==
    [targetRef : Instances, conf : Configs, ballot : Ballots,
     recordBallot : Ballots, senders : SUBSET Nodes,
     tuple : TupleType \cup {NoTuple}]
ReadyType ==
    [present : BOOLEAN, token : 0..4, hard : HardType,
     writeRef : Instances, writeRecord : RecordType,
     outbound : SUBSET MessageType, applySet : SUBSET Instances,
     issuedAt : 0..MaxTick, allocationTag : 0..2]
Orders == UNION {[1..n -> Instances] : n \in 0..Cardinality(Instances)}

TerminalStage ==
    CASE Mode = "normal" -> 10
      [] Mode = "recovery" -> 19
      [] Mode = "toq" -> 14
      [] OTHER -> 21

TypeOK ==
    /\ records \in [Instances -> RecordType]
    /\ durableRecords \in [Instances -> RecordType]
    /\ hardState \in HardType
    /\ durableHardState \in HardType
    /\ acknowledgedHardState \in HardType
    /\ ready \in ReadyType
    /\ frozenReady \in ReadyType
    /\ readyPersisted \in BOOLEAN
    /\ messages \in SUBSET MessageType
    /\ recoveryEvidence \in [Instances -> EvidenceType]
    /\ applied \in SUBSET Instances
    /\ applyLog \in Orders
    /\ currentTick \in 0..MaxTick
    /\ deadline \in 0..MaxTick
    /\ toqPending \in BOOLEAN
    /\ toqDecision \in TOQStates
    /\ activeConfig \in Configs
    /\ stage \in 0..TerminalStage
    /\ retryCount \in 0..1
    /\ coverage \in [AllBranches -> 0..1]
    /\ wireNonce \in 0..2
    /\ paperDecision \in [Instances -> TupleType \cup {NoTuple}]
    /\ paperExecuted \in SUBSET Instances
    /\ paperEvidence \in [Instances -> EvidenceType]
    /\ paperConfig \in Configs

TupleConfigMapping ==
    /\ \A i \in Instances :
           /\ records[i].ref = i
           /\ durableRecords[i].ref = i
           /\ (records[i].tuple # NoTuple => records[i].tuple.conf = i.conf)
           /\ (durableRecords[i].tuple # NoTuple =>
                  durableRecords[i].tuple.conf = i.conf)
           /\ (paperDecision[i] # NoTuple => paperDecision[i].conf = i.conf)
    /\ paperDecision = MappedDecision

EvidenceMapping ==
    /\ paperEvidence = MappedEvidence
    /\ \A i \in Instances :
           /\ recoveryEvidence[i].targetRef = i
           /\ recoveryEvidence[i].conf = i.conf
           /\ recoveryEvidence[i].recordBallot <= recoveryEvidence[i].ballot
           /\ recoveryEvidence[i].senders \subseteq Nodes \ {Leader}
           /\ (recoveryEvidence[i].tuple # NoTuple =>
                  recoveryEvidence[i].tuple.conf = i.conf)

DurabilityBeforeEffects ==
    /\ \A m \in messages :
           /\ durableRecords[m.targetRef].status # "empty"
           /\ durableRecords[m.targetRef].tuple = m.tuple
           /\ m.ballot <= durableRecords[m.targetRef].ballot
           /\ m.conf = m.targetRef.conf
           /\ m.hardGeneration <= durableHardState.generation
           /\ m.to \in ConfigVoters(m.conf)
    /\ \A i \in applied :
           /\ durableRecords[i].status = "committed"
           /\ paperDecision[i] = durableRecords[i].tuple

ReadyFrozen ==
    /\ ready = frozenReady
    /\ (ready.present =>
           /\ ready.writeRef = ready.writeRecord.ref
           /\ (ready.writeRecord.tuple # NoTuple =>
                  ready.writeRecord.tuple.conf = ready.writeRef.conf))

ExactConfigPinning ==
    /\ paperConfig = activeConfig
    /\ \A i \in Instances :
           /\ i \in DOMAIN records
           /\ ConfigVoters(i.conf) \subseteq Nodes
           /\ records[i].ref.conf = i.conf
           /\ durableRecords[i].ref.conf = i.conf
    /\ (Mode = "config" /\ activeConfig = NewConfig =>
           /\ durableRecords[A].ref.conf = OldConfig
           /\ A.conf = OldConfig
           /\ B.conf = NewConfig)

TOQPendingBlocksApplication ==
    Mode = "toq" /\ durableHardState.toq = "pending" => A \notin applied

RecoveryBallotAndUniqueSender ==
    /\ \A i \in Instances :
           /\ Cardinality(recoveryEvidence[i].senders) <=
              Cardinality(Nodes \ {Leader})
           /\ (recoveryEvidence[i].senders # {} =>
                  /\ durableRecords[i].ballot = recoveryEvidence[i].ballot
                  /\ recoveryEvidence[i].recordBallot <=
                     durableRecords[i].recordBallot
                  /\ recoveryEvidence[i].ballot <= hardState.ballot)
    /\ (Mode = "recovery" /\ stage >= 10 =>
           durableRecords[A].ballot = 1)

SeqSet(order) == {order[k] : k \in 1..Len(order)}
Position(order, ref) == CHOOSE k \in 1..Len(order) : order[k] = ref
Before(order, left, right) ==
    /\ left \in SeqSet(order)
    /\ right \in SeqSet(order)
    /\ Position(order, left) < Position(order, right)

ExecutionOrdering ==
    /\ Len(applyLog) = Cardinality(applied)
    /\ SeqSet(applyLog) = applied
    /\ \A i \in applied :
           \A dep \in paperDecision[i].deps :
               dep \in applied /\ Before(applyLog, dep, i)

CommittedMapsChosen ==
    \A i \in Instances :
        durableRecords[i].status = "committed" =>
            /\ paperDecision[i] = durableRecords[i].tuple
            /\ paperDecision[i] # NoTuple

AcknowledgementIsDurable ==
    acknowledgedHardState.generation <= durableHardState.generation

RefinementInvariant ==
    /\ MappingInvariant
    /\ TupleConfigMapping
    /\ EvidenceMapping
    /\ DurabilityBeforeEffects
    /\ ReadyFrozen
    /\ ExactConfigPinning
    /\ TOQPendingBlocksApplication
    /\ RecoveryBallotAndUniqueSender
    /\ ExecutionOrdering
    /\ CommittedMapsChosen
    /\ AcknowledgementIsDurable

CoverageComplete == \A b \in RequiredBranches : coverage[b] = 1
WorkflowComplete ==
    /\ stage = TerminalStage
    /\ CoverageComplete
    /\ ScenarioInstances \subseteq applied

EventuallyCoversRequiredBranches == <>CoverageComplete
EventuallyWorkflowCompletes == <>WorkflowComplete

FairSpec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(Progress)

(***************************************************************************)
(* A future Go trace/refinement theorem needs deterministic export/replay of *)
(* every action kind and node ID; Ref{Replica, Instance, Conf}; record       *)
(* Status, Ballot, RecordBallot, command identity, Seq, and dependency Refs; *)
(* message kind/from/to/target Ref/ballots/config/tuple/retry attempt and    *)
(* recovery-round sender admission result; Ready ID plus an immutable       *)
(* semantic payload of HardState, record writes, messages, and apply refs;  *)
(* persistence begin/success ordering and stable generation; Advance Ready  *)
(* ID; application Ref and order; logical tick/deadline/TOQ decision and its *)
(* durability; active config ID and exact voter set; crash/restart boundary; *)
(* and validation/drop reason.  Codec bytes, checksum bytes, ownership, and  *)
(* allocation events remain hidden, except that their semantic stuttering   *)
(* must be replayable.                                                       *)
(***************************************************************************)

====
