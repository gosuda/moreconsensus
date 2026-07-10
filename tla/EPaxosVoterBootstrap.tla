---- MODULE EPaxosVoterBootstrap ----
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* A finite safety and progress model for adding one staged EPaxos voter. *)
(* Old voters are 1..N and the target is N+1.  The target is a non-voter  *)
(* until an Activate control instance has been chosen under the old       *)
(* configuration, executed, and made durable.                              *)
(*                                                                         *)
(* This model checks the staged Prepare/Seal/ReadyProof/Activate-or-Abort *)
(* protocol.  It deliberately does not model or claim joint consensus.    *)
(* Slot values and digests are symbolic and collision-free in this finite *)
(* model.  The per-lane frontier covers two representative instance       *)
(* positions, so a large production instance number is not being bounded  *)
(* by the small natural numbers used here.                                 *)
(***************************************************************************)

CONSTANTS N, Mode, Mutation, MaxCrashes, MaxRef

Modes == {"size", "crash-prefix", "race", "fair"}
DeterministicMode == Mode \in Modes
Mutations ==
    {"none", "RemoveSealFence", "EarlyTargetVote",
     "IncompleteFrontier", "ReuseControlRef", "AcceptStaleProof"}

ASSUME /\ N \in 1..6
       /\ Mode \in Modes
       /\ Mutation \in Mutations
       /\ MaxCrashes \in 0..2
       /\ MaxRef = (2 * N) + 4

OldVoters == 1..N
Target == N + 1
Replicas == 1..Target
NewVoters == OldVoters \cup {Target}

SlowQuorum(voters) == (Cardinality(voters) \div 2) + 1
OldSlowQuorum == SlowQuorum(OldVoters)
NewSlowQuorum == SlowQuorum(NewVoters)
OldSlowWitness == 1..OldSlowQuorum
NewSlowWitness == (1..(NewSlowQuorum - 1)) \cup {Target}

Slots == 1..2
OldRef(lane, slot) == (2 * (lane - 1)) + slot
OldRefs == {OldRef(lane, slot) : lane \in OldVoters, slot \in Slots}
AttackRef == OldRef(1, 2)

PrepareRef == (2 * N) + 1
ActivateRef == (2 * N) + 2
AbortRef == IF Mutation = "ReuseControlRef" THEN ActivateRef ELSE (2 * N) + 3
ControlRefs == {PrepareRef, ActivateRef, AbortRef}
FenceExemptRefs == {ActivateRef, AbortRef}
AllocatorFloor == (2 * N) + 4
SuccessorRef == AllocatorFloor
Refs == 1..MaxRef

NoConfig == "no-config"
OldConfig == "old-config"
NewConfig == "new-config"

LaneOf(ref) ==
    CHOOSE lane \in OldVoters : \E slot \in Slots : ref = OldRef(lane, slot)
SlotOf(ref) ==
    CHOOSE slot \in Slots : \E lane \in OldVoters : ref = OldRef(lane, slot)

ZeroFrontier == [lane \in OldVoters |-> 0]
InitialFrontier ==
    [lane \in OldVoters |-> IF lane = 1 THEN 2 ELSE 1]
RefsThrough(frontier) ==
    {ref \in OldRefs : SlotOf(ref) <= frontier[LaneOf(ref)]}
PotentialRefs == RefsThrough(InitialFrontier)
LastPotentialRef == OldRef(N, InitialFrontier[N])

MinOfSet(set) == CHOOSE item \in set : \A other \in set : item <= other
MaxOfSet(set) == CHOOSE item \in set : \A other \in set : other <= item

VARIABLE state
vars == <<state>>

ReportUnion(signers) ==
    [lane \in OldVoters |->
        MaxOfSet({state.durableFrontier[voter][lane] : voter \in signers})]

CertificateDigest(frontier, coverage) ==
    [configDigest |-> [version |-> 1, voters |-> OldVoters],
     applicationDigest |-> "application-v1",
     timingFloor |-> 7,
     compactionFloor |-> 3,
     frontier |-> frontier,
     coverage |-> coverage]
NoDigest ==
    [configDigest |-> [version |-> 0, voters |-> {}],
     applicationDigest |-> "no-application",
     timingFloor |-> 0,
     compactionFloor |-> 0,
     frontier |-> ZeroFrontier,
     coverage |-> {}]
StaleDigest ==
    [configDigest |-> [version |-> 1, voters |-> OldVoters],
     applicationDigest |-> "stale-application",
     timingFloor |-> 6,
     compactionFloor |-> 2,
     frontier |-> ZeroFrontier,
     coverage |-> {}]

NoImage ==
    [installed |-> FALSE,
     snapshotDigest |-> NoDigest,
     snapshotConfig |-> {},
     deltas |-> {},
     nextInstance |-> [voter \in Replicas |-> 0],
     timingFloor |-> 0,
     compactionFloor |-> 0,
     allocatorFloor |-> 0]

ExpectedImage ==
    [installed |-> TRUE,
     snapshotDigest |-> state.certificateDigest,
     snapshotConfig |-> OldVoters,
     deltas |-> state.certificateCoverage,
     nextInstance |->
        [voter \in Replicas |->
            IF voter = Target
            THEN 1
            ELSE state.certificateFrontier[voter] + 1],
     timingFloor |-> 7,
     compactionFloor |-> 3,
     allocatorFloor |-> AllocatorFloor]

NoProof ==
    [prepare |-> 0, activate |-> 0, digest |-> NoDigest,
     target |-> 0, epoch |-> 0]
ExpectedProof ==
    [prepare |-> PrepareRef, activate |-> ActivateRef,
     digest |-> state.certificateDigest, target |-> Target, epoch |-> 1]
StaleProof ==
    [prepare |-> PrepareRef, activate |-> ActivateRef,
     digest |-> StaleDigest, target |-> Target, epoch |-> 0]
WrongProof ==
    [prepare |-> PrepareRef, activate |-> ActivateRef,
     digest |-> state.certificateDigest, target |-> 1, epoch |-> 1]

Chosen(ref, value) == [ref |-> ref, value |-> value]
InitialStatus ==
    [ref \in OldRefs |->
        IF ref \in PotentialRefs THEN "preaccepted" ELSE "none"]
InitialPins ==
    [ref \in Refs |-> IF ref \in OldRefs THEN OldConfig ELSE NoConfig]
PinnedAfterReservation ==
    [ref \in Refs |->
        IF ref \in ControlRefs THEN OldConfig ELSE state.pinned[ref]]


Init ==
    state =
        [phase |-> "idle",
         running |-> [voter \in Replicas |-> TRUE],
         crashCount |-> 0,

         ordinaryStatus |-> InitialStatus,
         ordinaryProposalSeen |-> FALSE,
         preSealVotes |-> {},
         postSealVotes |-> {},
         lateOldChosen |-> FALSE,

         reservationSucceeded |-> FALSE,
         exhaustionRejected |-> FALSE,
         allocatorNext |-> PrepareRef,
         pinned |-> InitialPins,
         prepareChosen |-> FALSE,
         durableReservation |-> {},
         volatileReservation |-> {},

         durableFence |-> [voter \in OldVoters |-> FALSE],
         volatileFence |-> [voter \in OldVoters |-> FALSE],
         durableFrontier |-> [voter \in OldVoters |-> ZeroFrontier],
         volatileFrontier |-> [voter \in OldVoters |-> ZeroFrontier],
         durableControls |-> [voter \in OldVoters |-> {}],
         volatileControls |-> [voter \in OldVoters |-> {}],
         pendingSeal |-> {},
         sealAcks |-> {},
         sealClosed |-> FALSE,
         unionFrontier |-> ZeroFrontier,
         admissionChecks |-> {},

         recovered |-> {},
         certificateIssued |-> FALSE,
         certificateCoverage |-> {},
         certificateFrontier |-> ZeroFrontier,
         certificateDigest |-> NoDigest,
         certificateSigners |-> {},
         certificateSignerDigest |-> [voter \in OldVoters |-> NoDigest],

         durableTargetImage |-> NoImage,
         volatileTargetImage |-> NoImage,
         readyProof |-> NoProof,
         readyProofEmitted |-> FALSE,
         pendingProof |-> {},
         proofReplicas |-> {},
         durableReadyProof |-> [voter \in OldVoters |-> NoProof],
         volatileReadyProof |-> [voter \in OldVoters |-> NoProof],
         acceptedProof |-> NoProof,

         activateRecoveryPhase |-> "none",
         activateBallot |-> 0,
         activatePrepareVotes |-> {},
         abortRecoveryPhase |-> "none",
         abortBallot |-> 0,
         abortPrepareVotes |-> {},
         activateVotes |-> {},
         abortVotes |-> {},
         chosenTerminals |-> {},
         chosenRecords |-> {},
         planExit |-> "none",
         exitExecuted |-> FALSE,

         durableActivation |-> FALSE,
         volatileActivation |-> FALSE,
         currentVoters |-> OldVoters,
         currentVersion |-> 1,

         successorProposed |-> FALSE,
         successorVotes |-> {},
         successorChosen |-> FALSE,

         networkLoss |-> FALSE,
         networkDuplicate |-> FALSE,
         networkReorder |-> FALSE,
         wrongProofRejected |-> FALSE,
         staleProofRejected |-> FALSE,
         rollbackRejected |-> FALSE,
         duplicateActivationSeen |-> FALSE]

(***************************************************************************)
(* Pre-seal ordinary work.  AttackRef is representative old in-flight     *)
(* work.  It may collect a genuine old slow quorum before Prepare, and is *)
(* still recovered through the certified frontier after Seal.             *)
(***************************************************************************)

ObserveOrdinaryProposal ==
    /\ state.phase = "idle"
    /\ ~state.ordinaryProposalSeen
    /\ state' =
        [state EXCEPT
            !.ordinaryProposalSeen = TRUE,
            !.ordinaryStatus[AttackRef] = "accepted"]

CastPreSealVote(voter) ==
    /\ state.phase = "idle"
    /\ state.running[voter]
    /\ voter \notin state.preSealVotes
    /\ Cardinality(state.preSealVotes) < OldSlowQuorum
    /\ \/ ~DeterministicMode
       \/ /\ voter \in OldSlowWitness
          /\ voter = MinOfSet(OldSlowWitness \ state.preSealVotes)
    /\ LET votes == state.preSealVotes \cup {voter}
       IN state' =
            [state EXCEPT
                !.preSealVotes = votes,
                !.ordinaryStatus[AttackRef] =
                    IF Cardinality(votes) >= OldSlowQuorum
                    THEN "terminal"
                    ELSE "accepted",
                !.chosenRecords =
                    IF Cardinality(votes) >= OldSlowQuorum
                    THEN @ \cup {Chosen(AttackRef, "old-user")}
                    ELSE @]

(***************************************************************************)
(* A failed allocator probe leaves the protocol untouched.  The real      *)
(* Prepare reservation succeeds only because all three adjacent control   *)
(* references fit below MaxRef; the mutation aliases Abort with Activate. *)
(***************************************************************************)

RejectExhaustedReservation ==
    /\ state.phase = "idle"
    /\ ~state.exhaustionRejected
    /\ (MaxRef - 1) + 2 > MaxRef
    /\ state' = [state EXCEPT !.exhaustionRejected = TRUE]

ChoosePrepareAndReserve ==
    /\ state.phase = "idle"
    /\ state.running[1]
    /\ (2 * N) + 3 < AllocatorFloor
    /\ AllocatorFloor <= MaxRef
    /\ state' =
        [state EXCEPT
            !.phase = "sealing",
            !.reservationSucceeded = TRUE,
            !.allocatorNext = AllocatorFloor,
            !.pinned = PinnedAfterReservation,
            !.prepareChosen = TRUE,
            !.durableReservation = ControlRefs,
            !.volatileReservation = ControlRefs,
            !.chosenRecords =
                @ \cup {Chosen(PrepareRef,
                                [kind |-> "prepare", config |-> OldConfig])}]

(***************************************************************************)
(* Each SealAck travels only after an atomic durable fence write.  The     *)
(* write rejects new initial-C work, rejects work above the reported lane *)
(* frontier, allows only retry/recovery at or below it until terminal, and *)
(* exempts only the exact matching Activate and Abort control refs.        *)
(***************************************************************************)

DurablyFence(voter) ==
    /\ state.phase = "sealing"
    /\ state.running[voter]
    /\ ~state.durableFence[voter]
    /\ \/ ~DeterministicMode
       \/ /\ voter \in OldSlowWitness
          /\ voter =
                 MinOfSet({member \in OldSlowWitness :
                              ~state.durableFence[member]})
    /\ state' =
        [state EXCEPT
            !.durableFence[voter] = TRUE,
            !.volatileFence[voter] = TRUE,
            !.durableFrontier[voter] = InitialFrontier,
            !.volatileFrontier[voter] = InitialFrontier,
            !.durableControls[voter] = FenceExemptRefs,
            !.volatileControls[voter] = FenceExemptRefs,
            !.pendingSeal = @ \cup {voter}]

SendSealWithoutFence ==
    /\ Mutation = "RemoveSealFence"
    /\ state.phase = "sealing"
    /\ state.running[1]
    /\ ~state.durableFence[1]
    /\ 1 \notin state.pendingSeal
    /\ 1 \notin state.sealAcks
    /\ state' = [state EXCEPT !.pendingSeal = @ \cup {1}]

RetrySeal(voter) ==
    /\ state.phase = "sealing"
    /\ state.running[voter]
    /\ voter \notin state.pendingSeal
    /\ voter \notin state.sealAcks
    /\ \/ state.durableFence[voter]
       \/ /\ Mutation = "RemoveSealFence"
          /\ voter = 1
    /\ state' = [state EXCEPT !.pendingSeal = @ \cup {voter}]

LoseSeal(voter) ==
    /\ Mode = "race"
    /\ voter \in state.pendingSeal
    /\ state' =
        [state EXCEPT
            !.pendingSeal = @ \ {voter},
            !.networkLoss = TRUE]

DeliverSeal(voter) ==
    /\ voter \in state.pendingSeal
    /\ \/ ~DeterministicMode
       \/ /\ voter \in OldSlowWitness
          /\ voter =
                 MinOfSet(state.pendingSeal \cap OldSlowWitness)
    /\ state' =
        [state EXCEPT
            !.pendingSeal = @ \ {voter},
            !.sealAcks = @ \cup {voter},
            !.networkReorder =
                @ \/ \E other \in state.pendingSeal : other < voter]

ObserveDuplicateSeal(voter) ==
    /\ voter \in state.sealAcks
    /\ ~state.networkDuplicate
    /\ state' = [state EXCEPT !.networkDuplicate = TRUE]

CloseSeal ==
    /\ state.phase = "sealing"
    /\ Cardinality(state.sealAcks) >= OldSlowQuorum
    /\ state' =
        [state EXCEPT
            !.phase = "recovering",
            !.sealClosed = TRUE,
            !.unionFrontier = ReportUnion(state.sealAcks),
            !.pendingSeal = {}]

ProbeDurableFenceAdmissions ==
    /\ state.sealClosed
    /\ state.admissionChecks = {}
    /\ \E voter \in state.sealAcks : state.durableFence[voter]
    /\ state' =
        [state EXCEPT
            !.admissionChecks =
                {"initial-preaccept-rejected", "initial-accept-rejected",
                 "initial-prepare-rejected",
                 "above-frontier-preaccept-rejected",
                 "above-frontier-accept-rejected",
                 "above-frontier-prepare-rejected",
                 "below-frontier-new-value-rejected",
                 "below-frontier-retry-or-recovery-only",
                 "activate-abort-control-exempt"}]

CastPostSealOldVote(voter) ==
    /\ state.sealClosed
    /\ state.planExit = "none"
    /\ state.running[voter]
    /\ ~state.durableFence[voter]
    /\ voter \notin state.postSealVotes
    /\ \/ ~DeterministicMode
       \/ voter =
              MinOfSet({member \in OldVoters :
                           ~state.durableFence[member]} \
                       state.postSealVotes)
    /\ LET votes == state.postSealVotes \cup {voter}
       IN state' =
            [state EXCEPT
                !.postSealVotes = votes,
                !.lateOldChosen =
                    @ \/ Cardinality(votes) >= OldSlowQuorum,
                !.chosenRecords =
                    IF Cardinality(votes) >= OldSlowQuorum
                    THEN @ \cup {Chosen(AttackRef, "late-old-user")}
                    ELSE @]

RecoveryGoal ==
    IF Mutation = "IncompleteFrontier"
    THEN RefsThrough(state.unionFrontier) \ {LastPotentialRef}
    ELSE RefsThrough(state.unionFrontier)

RecoverNextFrontierRef ==
    /\ state.phase = "recovering"
    /\ RecoveryGoal \ state.recovered # {}
    /\ LET ref == MinOfSet(RecoveryGoal \ state.recovered)
       IN state' =
            [state EXCEPT
                !.recovered = @ \cup {ref},
                !.ordinaryStatus[ref] = "terminal",
                !.chosenRecords = @ \cup {Chosen(ref, "old-user")}]

BuildCertificate ==
    /\ state.phase = "recovering"
    /\ RecoveryGoal \subseteq state.recovered
    /\ state' =
        [state EXCEPT
            !.phase = "certified",
            !.certificateIssued = TRUE,
            !.certificateCoverage = state.recovered,
            !.certificateFrontier = state.unionFrontier,
            !.certificateDigest =
                CertificateDigest(state.unionFrontier, state.recovered),
            !.certificateSigners = state.sealAcks,
            !.certificateSignerDigest =
                [voter \in OldVoters |->
                    IF voter \in state.sealAcks
                    THEN CertificateDigest(state.unionFrontier,
                                           state.recovered)
                    ELSE NoDigest]]

(***************************************************************************)
(* The target installation is one durable atomic image: snapshot, deltas, *)
(* per-replica allocator floors, timing floor, compaction floor, and the   *)
(* reserved-ref allocator floor.  ReadyProof is emitted only afterwards.  *)
(***************************************************************************)

InstallTargetAtomically ==
    /\ state.phase = "certified"
    /\ state.running[Target]
    /\ state.certificateIssued
    /\ state' =
        [state EXCEPT
            !.phase = "installed",
            !.durableTargetImage = ExpectedImage,
            !.volatileTargetImage = ExpectedImage]

RejectRollbackSnapshot ==
    /\ state.durableTargetImage.installed
    /\ ~state.rollbackRejected
    /\ state.planExit = "none"
    /\ state' = [state EXCEPT !.rollbackRejected = TRUE]

EmitReadyProof ==
    /\ state.phase = "installed"
    /\ state.durableTargetImage = ExpectedImage
    /\ state' =
        [state EXCEPT
            !.phase = "proof",
            !.readyProof = ExpectedProof,
            !.readyProofEmitted = TRUE,
            !.pendingProof = OldVoters]

LoseProof(voter) ==
    /\ Mode = "race"
    /\ state.phase = "proof"
    /\ voter \in state.pendingProof
    /\ state' =
        [state EXCEPT
            !.pendingProof = @ \ {voter},
            !.networkLoss = TRUE]

RetryProof(voter) ==
    /\ state.phase = "proof"
    /\ voter \notin state.pendingProof
    /\ voter \notin state.proofReplicas
    /\ state.readyProofEmitted
    /\ state' = [state EXCEPT !.pendingProof = @ \cup {voter}]

DeliverProof(voter) ==
    /\ state.phase = "proof"
    /\ voter \in state.pendingProof
    /\ state.running[voter]
    /\ \/ ~DeterministicMode
       \/ /\ voter \in OldSlowWitness
          /\ voter =
                 MinOfSet((state.pendingProof \cap OldSlowWitness) \
                          state.proofReplicas)
    /\ state' =
        [state EXCEPT
            !.pendingProof = @ \ {voter},
            !.proofReplicas = @ \cup {voter},
            !.durableReadyProof[voter] = state.readyProof,
            !.volatileReadyProof[voter] = state.readyProof,
            !.networkReorder =
                @ \/ \E other \in state.pendingProof : other < voter]

ObserveDuplicateProof(voter) ==
    /\ voter \in state.proofReplicas
    /\ ~state.networkDuplicate
    /\ state' = [state EXCEPT !.networkDuplicate = TRUE]

RejectWrongProof ==
    /\ state.phase = "proof"
    /\ ~state.wrongProofRejected
    /\ WrongProof # ExpectedProof
    /\ state' = [state EXCEPT !.wrongProofRejected = TRUE]

RejectStaleProof ==
    /\ state.phase = "proof"
    /\ Mutation # "AcceptStaleProof"
    /\ ~state.staleProofRejected
    /\ StaleProof # ExpectedProof
    /\ state' = [state EXCEPT !.staleProofRejected = TRUE]

AcceptStaleProofMutation ==
    /\ Mutation = "AcceptStaleProof"
    /\ state.phase = "proof"
    /\ state' =
        [state EXCEPT
            !.phase = "terminal-race",
            !.acceptedProof = StaleProof,
            !.proofReplicas = OldSlowWitness,
            !.durableReadyProof =
                [voter \in OldVoters |->
                    IF voter \in OldSlowWitness
                    THEN StaleProof
                    ELSE state.durableReadyProof[voter]],
            !.volatileReadyProof =
                [voter \in OldVoters |->
                    IF voter \in OldSlowWitness
                    THEN StaleProof
                    ELSE state.volatileReadyProof[voter]],
            !.pendingProof = {}]

FinishReadyProofReplication ==
    /\ state.phase = "proof"
    /\ Cardinality(state.proofReplicas) >= OldSlowQuorum
    /\ state' =
        [state EXCEPT
            !.phase = "terminal-race",
            !.acceptedProof = state.readyProof,
            !.pendingProof = {}]

(***************************************************************************)
(* Activate and Abort occupy different old-configuration control refs.     *)
(* Each exit uses a representative ordinary old-C Prepare/Accept ballot.   *)
(* The all-none Prepare result permits Abort; Activate additionally carries *)
(* the certified, old-slow-quorum-durable ReadyProof.  The first chosen     *)
(* terminal ref fixes the plan exit and excludes the other.                 *)
(***************************************************************************)

TerminalRecoveryPhase ==
    state.sealClosed /\
    state.phase \in
        {"recovering", "certified", "installed", "proof", "terminal-race"}

ObserveNetworkReorder ==
    /\ Mode = "race"
    /\ ~state.networkReorder
    /\ \/ Cardinality(state.pendingSeal) >= 2
       \/ Cardinality(state.pendingProof) >= 2
    /\ state' = [state EXCEPT !.networkReorder = TRUE]

StartAbortRecovery ==
    /\ TerminalRecoveryPhase
    /\ Mode = "race"
    /\ state.planExit = "none"
    /\ state.abortRecoveryPhase = "none"
    /\ state' =
        [state EXCEPT
            !.abortRecoveryPhase = "prepare",
            !.abortBallot = 1]

PrepareAbortVote(voter) ==
    /\ TerminalRecoveryPhase
    /\ state.planExit = "none"
    /\ state.abortRecoveryPhase = "prepare"
    /\ state.running[voter]
    /\ voter \notin state.abortPrepareVotes
    /\ voter \in OldSlowWitness
    /\ voter =
           MinOfSet(OldSlowWitness \ state.abortPrepareVotes)
    /\ state' = [state EXCEPT !.abortPrepareVotes = @ \cup {voter}]

BeginAbortAccept ==
    /\ TerminalRecoveryPhase
    /\ state.planExit = "none"
    /\ state.abortRecoveryPhase = "prepare"
    /\ Cardinality(state.abortPrepareVotes) >= OldSlowQuorum
    /\ state' = [state EXCEPT !.abortRecoveryPhase = "accept"]

RecoverAbortVote(voter) ==
    /\ TerminalRecoveryPhase
    /\ state.planExit = "none"
    /\ state.abortRecoveryPhase = "accept"
    /\ state.running[voter]
    /\ voter \notin state.abortVotes
    /\ voter \in OldSlowWitness
    /\ voter = MinOfSet(OldSlowWitness \ state.abortVotes)
    /\ state' = [state EXCEPT !.abortVotes = @ \cup {voter}]

StartActivateRecovery ==
    /\ state.phase = "terminal-race"
    /\ state.planExit = "none"
    /\ state.activateRecoveryPhase = "none"
    /\ state.acceptedProof # NoProof
    /\ state' =
        [state EXCEPT
            !.activateRecoveryPhase = "prepare",
            !.activateBallot = 1]

PrepareActivateVote(voter) ==
    /\ state.phase = "terminal-race"
    /\ state.planExit = "none"
    /\ state.activateRecoveryPhase = "prepare"
    /\ state.running[voter]
    /\ voter \notin state.activatePrepareVotes
    /\ \/ ~DeterministicMode
       \/ /\ voter \in OldSlowWitness
          /\ voter =
                 MinOfSet(OldSlowWitness \
                          state.activatePrepareVotes)
    /\ state' = [state EXCEPT !.activatePrepareVotes = @ \cup {voter}]

BeginActivateAccept ==
    /\ state.phase = "terminal-race"
    /\ state.planExit = "none"
    /\ state.activateRecoveryPhase = "prepare"
    /\ Cardinality(state.activatePrepareVotes) >= OldSlowQuorum
    /\ state' = [state EXCEPT !.activateRecoveryPhase = "accept"]

RecoverActivateVote(voter) ==
    /\ state.phase = "terminal-race"
    /\ state.planExit = "none"
    /\ state.activateRecoveryPhase = "accept"
    /\ state.running[voter]
    /\ voter \notin state.activateVotes
    /\ \/ ~DeterministicMode
       \/ /\ voter \in OldSlowWitness
          /\ voter =
                 MinOfSet(OldSlowWitness \ state.activateVotes)
    /\ state.acceptedProof # NoProof
    /\ state' = [state EXCEPT !.activateVotes = @ \cup {voter}]

ChooseAbort ==
    /\ TerminalRecoveryPhase
    /\ state.planExit = "none"
    /\ state.abortRecoveryPhase = "accept"
    /\ Cardinality(state.abortVotes) >= OldSlowQuorum
    /\ state' =
        [state EXCEPT
            !.phase = "chosen-exit",
            !.planExit = "abort",
            !.chosenTerminals = @ \cup {AbortRef},
            !.chosenRecords = @ \cup {Chosen(AbortRef, "abort")}]

ChooseActivate ==
    /\ state.phase = "terminal-race"
    /\ state.planExit = "none"
    /\ state.activateRecoveryPhase = "accept"
    /\ Cardinality(state.activateVotes) >= OldSlowQuorum
    /\ IF Mutation = "AcceptStaleProof"
       THEN state.acceptedProof # NoProof
       ELSE state.acceptedProof = ExpectedProof
    /\ state' =
        [state EXCEPT
            !.phase = "chosen-exit",
            !.planExit = "activate",
            !.chosenTerminals = @ \cup {ActivateRef},
            !.chosenRecords =
                @ \cup
                    {Chosen(ActivateRef,
                            [kind |-> "activate",
                             proof |-> state.acceptedProof])}]
ExecutePlanExit ==
    /\ state.phase = "chosen-exit"
    /\ ~state.exitExecuted
    /\ state.planExit \in {"activate", "abort"}
    /\ state.planExit = "abort" \/ state.running[Target]
    /\ state' =
        [state EXCEPT
            !.phase = IF state.planExit = "activate" THEN "active" ELSE "aborted",
            !.exitExecuted = TRUE,
            !.durableActivation = state.planExit = "activate",
            !.volatileActivation = state.planExit = "activate",
            !.currentVoters =
                IF state.planExit = "activate" THEN NewVoters ELSE OldVoters,
            !.currentVersion =
                IF state.planExit = "activate" THEN 2 ELSE 1]

ObserveDuplicateActivation ==
    /\ state.exitExecuted
    /\ state.planExit = "activate"
    /\ ~state.duplicateActivationSeen
    /\ state' = [state EXCEPT !.duplicateActivationSeen = TRUE]

CastEarlyTargetVoteMutation ==
    /\ Mutation = "EarlyTargetVote"
    /\ state.readyProofEmitted
    /\ ~state.durableActivation
    /\ Target \notin state.successorVotes
    /\ state' = [state EXCEPT !.successorVotes = @ \cup {Target}]

(***************************************************************************)
(* Successor ordinary work uses exactly the installed current membership. *)
(* On Activate this is C' and its fixed representative quorum includes the *)
(* target.  On Abort, ordinary work continues under the unchanged old C.  *)
(***************************************************************************)

ProposeSuccessor ==
    /\ state.phase \in {"active", "aborted"}
    /\ state.exitExecuted
    /\ ~state.successorProposed
    /\ state' =
        [state EXCEPT
            !.phase = "successor",
            !.successorProposed = TRUE,
            !.pinned[SuccessorRef] =
                IF state.currentVersion = 2 THEN NewConfig ELSE OldConfig]

SuccessorWitness ==
    IF state.currentVersion = 2 THEN NewSlowWitness ELSE OldSlowWitness

CastSuccessorVote(voter) ==
    /\ state.phase = "successor"
    /\ voter \in state.currentVoters
    /\ state.running[voter]
    /\ voter \notin state.successorVotes
    /\ voter # Target \/ state.durableActivation
    /\ \/ ~DeterministicMode
       \/ /\ voter \in SuccessorWitness
          /\ voter =
                 MinOfSet(SuccessorWitness \ state.successorVotes)
    /\ state' = [state EXCEPT !.successorVotes = @ \cup {voter}]

CurrentSlowQuorum ==
    IF state.currentVersion = 2 THEN NewSlowQuorum ELSE OldSlowQuorum

ChooseSuccessor ==
    /\ state.phase = "successor"
    /\ ~state.successorChosen
    /\ Cardinality(state.successorVotes \cap state.currentVoters) >=
           CurrentSlowQuorum
    /\ state' =
        [state EXCEPT
            !.phase = "done",
            !.successorChosen = TRUE,
            !.chosenRecords =
                @ \cup
                    {Chosen(SuccessorRef,
                            IF state.currentVersion = 2
                            THEN "successor-new"
                            ELSE "successor-old")}]

(***************************************************************************)
(* Crashes discard only volatile images.  Restart reconstructs every fence *)
(* and target image from the corresponding durable state.                  *)
(***************************************************************************)

Crash(voter) ==
    /\ Mode \in {"crash-prefix", "race"}
    /\ state.crashCount < MaxCrashes
    /\ state.running[voter]
    /\ state' =
        [state EXCEPT
            !.running[voter] = FALSE,
            !.crashCount = @ + 1,
            !.volatileReservation =
                IF voter = 1 THEN {} ELSE state.volatileReservation,
            !.volatileFence =
                IF voter \in OldVoters
                THEN [state.volatileFence EXCEPT ![voter] = FALSE]
                ELSE state.volatileFence,
            !.volatileFrontier =
                IF voter \in OldVoters
                THEN [state.volatileFrontier EXCEPT ![voter] = ZeroFrontier]
                ELSE state.volatileFrontier,
            !.volatileControls =
                IF voter \in OldVoters
                THEN [state.volatileControls EXCEPT ![voter] = {}]
                ELSE state.volatileControls,
            !.volatileReadyProof =
                IF voter \in OldVoters
                THEN [state.volatileReadyProof EXCEPT
                        ![voter] = NoProof]
                ELSE state.volatileReadyProof,
            !.volatileTargetImage =
                IF voter = Target THEN NoImage ELSE state.volatileTargetImage,
            !.volatileActivation =
                IF voter = Target THEN FALSE ELSE state.volatileActivation]

Restart(voter) ==
    /\ ~state.running[voter]
    /\ state' =
        [state EXCEPT
            !.running[voter] = TRUE,
            !.volatileReservation =
                IF voter = 1
                THEN state.durableReservation
                ELSE state.volatileReservation,
            !.volatileFence =
                IF voter \in OldVoters
                THEN [state.volatileFence EXCEPT
                        ![voter] = state.durableFence[voter]]
                ELSE state.volatileFence,
            !.volatileFrontier =
                IF voter \in OldVoters
                THEN [state.volatileFrontier EXCEPT
                        ![voter] = state.durableFrontier[voter]]
                ELSE state.volatileFrontier,
            !.volatileControls =
                IF voter \in OldVoters
                THEN [state.volatileControls EXCEPT
                        ![voter] = state.durableControls[voter]]
                ELSE state.volatileControls,
            !.volatileReadyProof =
                IF voter \in OldVoters
                THEN [state.volatileReadyProof EXCEPT
                        ![voter] = state.durableReadyProof[voter]]
                ELSE state.volatileReadyProof,
            !.volatileTargetImage =
                IF voter = Target
                THEN state.durableTargetImage
                ELSE state.volatileTargetImage,
            !.volatileActivation =
                IF voter = Target
                THEN state.durableActivation
                ELSE state.volatileActivation]

Next ==
    \/ ObserveOrdinaryProposal
    \/ \E voter \in OldVoters : CastPreSealVote(voter)
    \/ RejectExhaustedReservation
    \/ ChoosePrepareAndReserve
    \/ \E voter \in OldVoters : DurablyFence(voter)
    \/ SendSealWithoutFence
    \/ \E voter \in OldVoters : RetrySeal(voter)
    \/ \E voter \in OldVoters : LoseSeal(voter)
    \/ \E voter \in OldVoters : DeliverSeal(voter)
    \/ \E voter \in OldVoters : ObserveDuplicateSeal(voter)
    \/ ObserveNetworkReorder
    \/ CloseSeal
    \/ ProbeDurableFenceAdmissions
    \/ \E voter \in OldVoters : CastPostSealOldVote(voter)
    \/ RecoverNextFrontierRef
    \/ BuildCertificate
    \/ InstallTargetAtomically
    \/ RejectRollbackSnapshot
    \/ EmitReadyProof
    \/ \E voter \in OldVoters : LoseProof(voter)
    \/ \E voter \in OldVoters : RetryProof(voter)
    \/ \E voter \in OldVoters : DeliverProof(voter)
    \/ \E voter \in OldVoters : ObserveDuplicateProof(voter)
    \/ RejectWrongProof
    \/ RejectStaleProof
    \/ AcceptStaleProofMutation
    \/ FinishReadyProofReplication
    \/ StartAbortRecovery
    \/ \E voter \in OldVoters : PrepareAbortVote(voter)
    \/ BeginAbortAccept
    \/ StartActivateRecovery
    \/ \E voter \in OldVoters : PrepareActivateVote(voter)
    \/ BeginActivateAccept
    \/ \E voter \in OldVoters : RecoverAbortVote(voter)
    \/ \E voter \in OldVoters : RecoverActivateVote(voter)
    \/ ChooseAbort
    \/ ChooseActivate
    \/ ExecutePlanExit
    \/ ObserveDuplicateActivation
    \/ CastEarlyTargetVoteMutation
    \/ ProposeSuccessor
    \/ \E voter \in Replicas : CastSuccessorVote(voter)
    \/ ChooseSuccessor
    \/ \E voter \in Replicas : Crash(voter)
    \/ \E voter \in Replicas : Restart(voter)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* FairProgress is a deterministic live path embedded in Next.  Its weak  *)
(* fairness represents eventual delivery, a live old slow quorum, and     *)
(* checkpoint availability.  Fair configurations disable crashes/loss.   *)
(***************************************************************************)

MissingOldSealWitness == OldSlowWitness \ state.sealAcks
MissingProofWitness == OldSlowWitness \ state.proofReplicas
MissingActivateWitness == OldSlowWitness \ state.activateVotes
MissingActivatePrepareWitness ==
    OldSlowWitness \ state.activatePrepareVotes
MissingSuccessorWitness == SuccessorWitness \ state.successorVotes

FairSealStep ==
    /\ state.phase = "sealing"
    /\ IF \E voter \in OldSlowWitness : ~state.durableFence[voter]
       THEN DurablyFence(
                MinOfSet({voter \in OldSlowWitness :
                            ~state.durableFence[voter]}))
       ELSE IF MissingOldSealWitness # {}
            THEN DeliverSeal(MinOfSet(MissingOldSealWitness))
            ELSE CloseSeal

FairProofStep ==
    /\ state.phase = "proof"
    /\ IF MissingProofWitness # {}
       THEN DeliverProof(MinOfSet(MissingProofWitness))
       ELSE FinishReadyProofReplication

FairTerminalStep ==
    /\ state.phase = "terminal-race"
    /\ IF state.activateRecoveryPhase = "none"
       THEN StartActivateRecovery
       ELSE IF state.activateRecoveryPhase = "prepare"
            THEN IF MissingActivatePrepareWitness # {}
                 THEN PrepareActivateVote(
                          MinOfSet(MissingActivatePrepareWitness))
                 ELSE BeginActivateAccept
            ELSE IF MissingActivateWitness # {}
                 THEN RecoverActivateVote(MinOfSet(MissingActivateWitness))
                 ELSE ChooseActivate

FairSuccessorStep ==
    /\ state.phase = "successor"
    /\ IF MissingSuccessorWitness # {}
       THEN CastSuccessorVote(MinOfSet(MissingSuccessorWitness))
       ELSE ChooseSuccessor

FairProgress ==
    \/ ChoosePrepareAndReserve
    \/ FairSealStep
    \/ CloseSeal
    \/ RecoverNextFrontierRef
    \/ BuildCertificate
    \/ InstallTargetAtomically
    \/ EmitReadyProof
    \/ FairProofStep
    \/ FinishReadyProofReplication
    \/ FairTerminalStep
    \/ ChooseActivate
    \/ ExecutePlanExit
    \/ ProposeSuccessor
    \/ FairSuccessorStep
    \/ ChooseSuccessor

FairSpec == Spec /\ WF_vars(FairProgress)

(***************************************************************************)
(* Safety invariants requested for the staged protocol.                    *)
(***************************************************************************)

ReservedRefsUnique ==
    ~state.reservationSucceeded \/
    /\ PrepareRef # ActivateRef
    /\ PrepareRef # AbortRef
    /\ ActivateRef # AbortRef
    /\ ControlRefs \cap OldRefs = {}
    /\ Cardinality(ControlRefs) = 3
    /\ state.allocatorNext = AllocatorFloor
    /\ AllocatorFloor <= MaxRef
    /\ state.durableReservation = ControlRefs
    /\ (state.running[1] => state.volatileReservation = ControlRefs)

SealAckAfterDurableFence ==
    \A voter \in state.sealAcks :
        /\ state.durableFence[voter]
        /\ state.durableFrontier[voter] = InitialFrontier
        /\ state.durableControls[voter] = FenceExemptRefs

SealedQuorumBlocksOldChoice ==
    ~state.sealClosed \/
    /\ Cardinality(state.sealAcks) >= OldSlowQuorum
    /\ ~state.lateOldChosen
    /\ Cardinality(state.postSealVotes) < OldSlowQuorum

FrontierCompleteBeforeCertificate ==
    ~state.certificateIssued \/
    /\ state.certificateFrontier = state.unionFrontier
    /\ RefsThrough(state.certificateFrontier) \subseteq
           state.certificateCoverage
    /\ state.certificateCoverage \subseteq state.recovered
    /\ \A ref \in RefsThrough(state.certificateFrontier) :
           state.ordinaryStatus[ref] = "terminal"

SnapshotDigestAgreement ==
    /\ (~state.certificateIssued \/
        /\ state.certificateDigest =
               CertificateDigest(state.certificateFrontier,
                                 state.certificateCoverage)
        /\ Cardinality(state.certificateSigners) >= OldSlowQuorum
        /\ \A voter \in state.certificateSigners :
               state.certificateSignerDigest[voter] =
                   state.certificateDigest)
    /\ (~state.durableTargetImage.installed \/
        /\ state.durableTargetImage = ExpectedImage
        /\ state.durableTargetImage.snapshotDigest =
               state.certificateDigest)

ReadyProofAfterAtomicInstall ==
    ~state.readyProofEmitted \/
    /\ state.durableTargetImage.installed
    /\ state.durableTargetImage = ExpectedImage
    /\ state.readyProof = ExpectedProof

NoTargetVoteBeforeActivationDurable ==
    Target \notin state.successorVotes \/ state.durableActivation

AtMostOnePlanExit ==
    /\ Cardinality(state.chosenTerminals) <= 1
    /\ (state.planExit = "none" => state.chosenTerminals = {})
    /\ (state.planExit = "activate" =>
            state.chosenTerminals = {ActivateRef})
    /\ (state.planExit = "abort" =>
            state.chosenTerminals = {AbortRef})

ActivateProofValid ==
    ActivateRef \notin state.chosenTerminals \/
    /\ state.planExit = "activate"
    /\ state.certificateIssued
    /\ state.readyProof = ExpectedProof
    /\ state.acceptedProof = ExpectedProof
    /\ Cardinality(state.proofReplicas) >= OldSlowQuorum
    /\ \A voter \in state.proofReplicas :
           state.durableReadyProof[voter] = ExpectedProof

ConfigurationFunctional ==
    /\ state.currentVersion \in {1, 2}
    /\ (state.currentVersion = 1 <=> state.currentVoters = OldVoters)
    /\ (state.currentVersion = 2 <=> state.currentVoters = NewVoters)
    /\ (state.currentVersion = 2 <=> state.durableActivation)
    /\ (state.durableActivation =>
            /\ state.exitExecuted
            /\ state.planExit = "activate")
    /\ (state.exitExecuted /\ state.planExit = "abort" =>
            /\ state.currentVersion = 1
            /\ ~state.durableActivation)

OldRefsPinned ==
    /\ \A ref \in OldRefs : state.pinned[ref] = OldConfig
    /\ (~state.reservationSucceeded \/
        \A ref \in ControlRefs : state.pinned[ref] = OldConfig)
    /\ (~state.successorProposed \/
        state.pinned[SuccessorRef] =
            IF state.currentVersion = 2 THEN NewConfig ELSE OldConfig)

OldNewQuorumIntersection ==
    \A oldQuorum \in
        {quorum \in SUBSET OldVoters :
            Cardinality(quorum) >= OldSlowQuorum} :
      \A newQuorum \in
          {quorum \in SUBSET NewVoters :
              Cardinality(quorum) >= NewSlowQuorum} :
          oldQuorum \cap newQuorum # {}

(***************************************************************************)
(* The checked intersection is specific to the nested add C' = C \cup {x}. *)
(* It is not a joint-consensus theorem.  A non-nested replacement can have *)
(* disjoint majorities, as this explicit finite witness demonstrates.       *)
(***************************************************************************)
ReplacementOld == {"replacement-a", "replacement-b", "replacement-c"}
ReplacementNew == {"replacement-c", "replacement-d", "replacement-e"}
ReplacementOldQuorum == {"replacement-a", "replacement-b"}
ReplacementNewQuorum == {"replacement-d", "replacement-e"}
NonNestedReplacementDisjointWitness ==
    /\ ReplacementOldQuorum \subseteq ReplacementOld
    /\ ReplacementNewQuorum \subseteq ReplacementNew
    /\ Cardinality(ReplacementOldQuorum) =
           SlowQuorum(ReplacementOld)
    /\ Cardinality(ReplacementNewQuorum) =
           SlowQuorum(ReplacementNew)
    /\ ReplacementOldQuorum \cap ReplacementNewQuorum = {}

ChosenAgreement ==
    \A left \in state.chosenRecords :
      \A right \in state.chosenRecords :
        left.ref = right.ref => left.value = right.value

CrashReplayEquivalent ==
    /\ (state.running[1] =>
        state.volatileReservation = state.durableReservation)
    /\ \A voter \in OldVoters :
        state.running[voter] =>
            /\ state.volatileFence[voter] = state.durableFence[voter]
            /\ state.volatileFrontier[voter] =
                   state.durableFrontier[voter]
            /\ state.volatileControls[voter] =
                   state.durableControls[voter]
            /\ state.volatileReadyProof[voter] =
                   state.durableReadyProof[voter]
    /\ (state.running[Target] =>
        /\ state.volatileTargetImage = state.durableTargetImage
        /\ state.volatileActivation = state.durableActivation)

FiniteSealResolves ==
    state.sealClosed ~> state.planExit # "none"

SuccessorWorkProgresses ==
    state.sealClosed ~> state.successorChosen

=============================================================================
