---- MODULE EPaxosConfigHistory ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Finite configuration-history, durability, and pinned-recovery slice.   *)
(* N is fixed at four replicas.  Two same-base Conf1 configuration refs    *)
(* compete in EPaxos execution order: the earlier ref applies Add(4), and  *)
(* the later ref is durably rejected as superseded.  A third ref, pinned   *)
(* to Conf2, applies Remove(2), producing the exact finite chain            *)
(*   {1,2,3} -> {1,2,3,4} -> {1,3,4}.                                     *)
(* The model also retains old and mid-chain user records, persists an      *)
(* atomic HardState/history/outcome image, crashes and replays a pending   *)
(* loser, and races two recovery coordinators for the Conf2 user target.   *)
(* Recovery response sets are canonicalized by sender.  This is bounded   *)
(* evidence for this chain and these races; it is not an arbitrary-history *)
(* proof, a joint-consensus proof, or an unbounded EPaxos proof.            *)
(***************************************************************************)

CONSTANT Mode

Modes == {"crash-replay", "recovery-race"}
ASSUME Mode \in Modes

Conf1 == 1
Conf2 == 2
Conf3 == 3
NoConf == 0
ConfigIDs == {Conf1, Conf2, Conf3}
Bases == {Conf1, Conf2}

Replicas == {1, 2, 3, 4}
Voters1 == {1, 2, 3}
Voters2 == {1, 2, 3, 4}
Voters3 == {1, 3, 4}
RemovedVoter == 2
AddedVoter == 4

VotersFor(c) ==
    CASE c = Conf1 -> Voters1
      [] c = Conf2 -> Voters2
      [] c = Conf3 -> Voters3
      [] OTHER -> {}

SlowQuorum(c) == (Cardinality(VotersFor(c)) \div 2) + 1
ConfState(c) == [id |-> c, voters |-> VotersFor(c)]
EmptyConf == [id |-> NoConf, voters |-> {}]
ConfStates == {EmptyConf} \cup {ConfState(c) : c \in ConfigIDs}

Ref(replica, instance, conf) ==
    [replica |-> replica, instance |-> instance, conf |-> conf]
NoRef == Ref(0, 0, NoConf)

(* WinnerRef is deliberately later in raw replica order than LoserRef.     *)
(* Seq/dependency execution order, not raw ref order, selects the winner.  *)
WinnerRef == Ref(3, 1, Conf1)
LoserRef == Ref(1, 1, Conf1)
RemoveRef == Ref(4, 1, Conf2)
CompetingConfigRefs == {WinnerRef, LoserRef}
ConfigRefs == CompetingConfigRefs \cup {RemoveRef}

OldUser == Ref(2, 2, Conf1)
MidUser == Ref(2, 2, Conf2)
UserRefs == {OldUser, MidUser}
Target == MidUser
AllRefs == ConfigRefs \cup UserRefs

Add4 == "add-4"
Remove2 == "remove-2"
OldCommand == "old-user-command"
MidCommand == "mid-user-command"
Commands == {Add4, Remove2, OldCommand, MidCommand}

CommandFor(ref) ==
    CASE ref \in CompetingConfigRefs -> Add4
      [] ref = RemoveRef -> Remove2
      [] ref = OldUser -> OldCommand
      [] ref = MidUser -> MidCommand

SeqFor(ref) ==
    CASE ref = WinnerRef -> 1
      [] ref = LoserRef -> 2
      [] ref = RemoveRef -> 1
      [] ref = OldUser -> 2
      [] ref = MidUser -> 2

DepsFor(ref) ==
    CASE ref = WinnerRef -> [r \in Voters1 |-> 0]
      [] ref = LoserRef -> [r \in Voters1 |-> IF r = WinnerRef.replica THEN 1 ELSE 0]
      [] ref = RemoveRef -> [r \in Voters2 |-> 0]
      [] ref = OldUser -> [r \in Voters1 |-> IF r = WinnerRef.replica THEN 1 ELSE 0]
      [] ref = MidUser -> [r \in Voters2 |-> IF r = RemoveRef.replica THEN 1 ELSE 0]

TupleFor(ref) ==
    [cmd |-> CommandFor(ref),
     seq |-> SeqFor(ref),
     deps |-> DepsFor(ref),
     conf |-> ref.conf]
Tuples == {TupleFor(ref) : ref \in AllRefs}

ExecutionPrecedes(left, right) ==
    \/ SeqFor(left) < SeqFor(right)
    \/ /\ SeqFor(left) = SeqFor(right)
       /\ left.replica < right.replica
    \/ /\ SeqFor(left) = SeqFor(right)
       /\ left.replica = right.replica
       /\ left.instance < right.instance

ExpectedSuccessor(ref) ==
    CASE ref \in CompetingConfigRefs -> ConfState(Conf2)
      [] ref = RemoveRef -> ConfState(Conf3)

ConfigStatuses == {"none", "committed", "executed"}
Outcomes == {"none", "applied", "rejected-superseded"}
UserStatuses == {"none", "preaccepted", "accepted", "committed"}
Admissions == {"none", "admitted", "rejected"}
ReuseOutcomes == {"none", "rejected"}

EmptyConfigStatus == [ref \in ConfigRefs |-> "none"]
EmptyOutcome == [ref \in ConfigRefs |-> "none"]
EmptyResult == [ref \in ConfigRefs |-> EmptyConf]
EmptyApplied == [base \in Bases |-> NoRef]
EmptyUserStatus == [ref \in UserRefs |-> "none"]
EmptyUserPinned == [ref \in UserRefs |-> NoConf]
EmptyUserDeps == [ref \in UserRefs |-> {}]
EmptyAdmission == [ref \in UserRefs |-> "none"]
EmptyMessageSender == [ref \in UserRefs |-> 0]
InitialHistory == [c \in ConfigIDs |-> IF c = Conf1 THEN Voters1 ELSE {}]

Ballots == {0, 1, 2}
Coordinators == {1, 3}
RecoveryPhases == {"idle", "prepare", "accept", "committed"}

OtherCoordinator(k) == CHOOSE other \in Coordinators : other # k
Response(sender, coordinator, ballot, conf) ==
    [sender |-> sender, coordinator |-> coordinator,
     ballot |-> ballot, conf |-> conf]
ResponseKeys ==
    {Response(sender, coordinator, ballot, conf) :
        sender \in Replicas,
        coordinator \in Coordinators,
        ballot \in Ballots,
        conf \in ConfigIDs}

MatchingResponses(responses, coordinator, ballot, conf) ==
    {m \in responses :
        /\ m.coordinator = coordinator
        /\ m.ballot = ballot
        /\ m.conf = conf}

CountedSenders(responses, coordinator, ballot, conf) ==
    {m.sender : m \in MatchingResponses(responses, coordinator, ballot, conf)}

CoverageNames ==
    {"pending-blocked",
     "winner-applied",
     "storage-rejected",
     "pending-persisted",
     "crash",
     "replay",
     "loser-superseded",
     "remove-applied",
     "confid-reuse-rejected",
     "old-message-admitted",
     "mid-message-admitted",
     "race-started",
     "wrong-config-rejected",
     "stale-ballot-rejected",
     "current-prepare-insufficient",
     "duplicate-prepare",
     "removed-prepare-counted",
     "current-accept-insufficient",
     "duplicate-accept",
     "removed-accept-counted"}

CommonCoverage ==
    {"pending-blocked", "winner-applied", "storage-rejected",
     "pending-persisted", "loser-superseded", "remove-applied",
     "confid-reuse-rejected", "old-message-admitted",
     "mid-message-admitted"}
CrashCoverage == CommonCoverage \cup {"crash", "replay"}
RaceCoverage ==
    CommonCoverage \cup
    {"race-started", "wrong-config-rejected", "stale-ballot-rejected",
     "current-prepare-insufficient", "duplicate-prepare",
     "removed-prepare-counted", "current-accept-insufficient",
     "duplicate-accept", "removed-accept-counted"}

Phases ==
    {"init", "configs-pending", "pending-probed", "winner-executed",
     "winner-persist-failed", "winner-durable", "crashed",
     "after-replay", "loser-executed", "remove-pending",
     "chain-executed", "reuse-checked", "old-admitted",
     "mid-admitted", "chain-durable", "race-started",
     "race-wrong-rejected", "race-stale-rejected",
     "race-prepare-current", "race-prepare-dedup", "race-accept",
     "race-accept-current", "race-accept-dedup", "done"}

VARIABLES
    phase,
    running,
    currentConf,
    currentHardState,
    confHistory,
    configStatus,
    configOutcome,
    configResult,
    appliedByBase,
    pendingBarrier,

    userStatus,
    userPinned,
    userDepsDomain,
    messageAdmission,
    messageSender,
    blockedProposal,

    durableHardState,
    durableHistory,
    durableConfigStatus,
    durableOutcome,
    durableResult,
    durableAppliedByBase,
    durablePendingBarrier,
    durableUserStatus,
    durableUserPinned,
    durableUserDepsDomain,

    crashImage,
    replayImage,
    replayVerified,

    reuseOutcome,
    reuseCandidate,

    targetBallot,
    recoveryPhase,
    activeCoordinator,
    staleCoordinator,
    activeBallot,
    staleBallot,
    prepareResponses,
    acceptResponses,
    currentPrepareWitness,
    currentAcceptWitness,
    currentPrepareSnapshot,
    currentAcceptSnapshot,
    chosenTuples,

    coverage

VolatileConfigState ==
    <<running, currentConf, currentHardState, confHistory, configStatus,
      configOutcome, configResult, appliedByBase, pendingBarrier>>
VolatileUserState ==
    <<userStatus, userPinned, userDepsDomain, messageAdmission,
      messageSender, blockedProposal>>
DurableState ==
    <<durableHardState, durableHistory, durableConfigStatus,
      durableOutcome, durableResult, durableAppliedByBase,
      durablePendingBarrier, durableUserStatus, durableUserPinned,
      durableUserDepsDomain>>
CrashState == <<crashImage, replayImage, replayVerified>>
ReuseState == <<reuseOutcome, reuseCandidate>>
RecoveryState ==
    <<targetBallot, recoveryPhase, activeCoordinator, staleCoordinator,
      activeBallot, staleBallot, prepareResponses, acceptResponses,
      currentPrepareWitness, currentAcceptWitness,
      currentPrepareSnapshot, currentAcceptSnapshot, chosenTuples>>
Vars ==
    <<phase, VolatileConfigState, VolatileUserState, DurableState,
      CrashState, ReuseState, RecoveryState, coverage>>

MakeImage(hardState, history, statuses, outcomes, results, applied,
          pending, users, pins, domains) ==
    [hardState |-> hardState,
     history |-> history,
     statuses |-> statuses,
     outcomes |-> outcomes,
     results |-> results,
     applied |-> applied,
     pending |-> pending,
     users |-> users,
     pins |-> pins,
     domains |-> domains]

InitialImage ==
    MakeImage(ConfState(Conf1), InitialHistory, EmptyConfigStatus,
              EmptyOutcome, EmptyResult, EmptyApplied, FALSE,
              EmptyUserStatus, EmptyUserPinned, EmptyUserDeps)

DurableImage ==
    MakeImage(durableHardState, durableHistory, durableConfigStatus,
              durableOutcome, durableResult, durableAppliedByBase,
              durablePendingBarrier, durableUserStatus,
              durableUserPinned, durableUserDepsDomain)

ImageOK(image) ==
    /\ image.hardState \in ConfStates
    /\ image.history \in [ConfigIDs -> SUBSET Replicas]
    /\ image.statuses \in [ConfigRefs -> ConfigStatuses]
    /\ image.outcomes \in [ConfigRefs -> Outcomes]
    /\ image.results \in [ConfigRefs -> ConfStates]
    /\ image.applied \in [Bases -> ConfigRefs \cup {NoRef}]
    /\ image.pending \in BOOLEAN
    /\ image.users \in [UserRefs -> UserStatuses]
    /\ image.pins \in [UserRefs -> ConfigIDs \cup {NoConf}]
    /\ image.domains \in [UserRefs -> SUBSET Replicas]

Bump(name) ==
    /\ name \in CoverageNames
    /\ coverage[name] = 0
    /\ coverage' = [coverage EXCEPT ![name] = 1]

TypeOK ==
    /\ phase \in Phases
    /\ running \in BOOLEAN
    /\ currentConf \in ConfigIDs \cup {NoConf}
    /\ currentHardState \in ConfStates
    /\ confHistory \in [ConfigIDs -> SUBSET Replicas]
    /\ configStatus \in [ConfigRefs -> ConfigStatuses]
    /\ configOutcome \in [ConfigRefs -> Outcomes]
    /\ configResult \in [ConfigRefs -> ConfStates]
    /\ appliedByBase \in [Bases -> ConfigRefs \cup {NoRef}]
    /\ pendingBarrier \in BOOLEAN
    /\ userStatus \in [UserRefs -> UserStatuses]
    /\ userPinned \in [UserRefs -> ConfigIDs \cup {NoConf}]
    /\ userDepsDomain \in [UserRefs -> SUBSET Replicas]
    /\ messageAdmission \in [UserRefs -> Admissions]
    /\ messageSender \in [UserRefs -> 0..4]
    /\ blockedProposal \in BOOLEAN
    /\ durableHardState \in ConfStates
    /\ durableHistory \in [ConfigIDs -> SUBSET Replicas]
    /\ durableConfigStatus \in [ConfigRefs -> ConfigStatuses]
    /\ durableOutcome \in [ConfigRefs -> Outcomes]
    /\ durableResult \in [ConfigRefs -> ConfStates]
    /\ durableAppliedByBase \in [Bases -> ConfigRefs \cup {NoRef}]
    /\ durablePendingBarrier \in BOOLEAN
    /\ durableUserStatus \in [UserRefs -> UserStatuses]
    /\ durableUserPinned \in [UserRefs -> ConfigIDs \cup {NoConf}]
    /\ durableUserDepsDomain \in [UserRefs -> SUBSET Replicas]
    /\ ImageOK(crashImage)
    /\ ImageOK(replayImage)
    /\ replayVerified \in BOOLEAN
    /\ reuseOutcome \in ReuseOutcomes
    /\ reuseCandidate \in ConfStates \cup {[id |-> Conf2, voters |-> {1, 2, 4}]}
    /\ targetBallot \in Ballots
    /\ recoveryPhase \in RecoveryPhases
    /\ activeCoordinator \in Coordinators
    /\ staleCoordinator \in Coordinators
    /\ activeBallot \in Ballots
    /\ staleBallot \in Ballots
    /\ prepareResponses \subseteq ResponseKeys
    /\ acceptResponses \subseteq ResponseKeys
    /\ currentPrepareWitness \in BOOLEAN
    /\ currentAcceptWitness \in BOOLEAN
    /\ currentPrepareSnapshot \subseteq Replicas
    /\ currentAcceptSnapshot \subseteq Replicas
    /\ chosenTuples \subseteq Tuples
    /\ coverage \in [CoverageNames -> 0..1]

Init ==
    /\ phase = "init"
    /\ running = TRUE
    /\ currentConf = Conf1
    /\ currentHardState = ConfState(Conf1)
    /\ confHistory = InitialHistory
    /\ configStatus = EmptyConfigStatus
    /\ configOutcome = EmptyOutcome
    /\ configResult = EmptyResult
    /\ appliedByBase = EmptyApplied
    /\ pendingBarrier = FALSE
    /\ userStatus = EmptyUserStatus
    /\ userPinned = EmptyUserPinned
    /\ userDepsDomain = EmptyUserDeps
    /\ messageAdmission = EmptyAdmission
    /\ messageSender = EmptyMessageSender
    /\ blockedProposal = FALSE
    /\ durableHardState = ConfState(Conf1)
    /\ durableHistory = InitialHistory
    /\ durableConfigStatus = EmptyConfigStatus
    /\ durableOutcome = EmptyOutcome
    /\ durableResult = EmptyResult
    /\ durableAppliedByBase = EmptyApplied
    /\ durablePendingBarrier = FALSE
    /\ durableUserStatus = EmptyUserStatus
    /\ durableUserPinned = EmptyUserPinned
    /\ durableUserDepsDomain = EmptyUserDeps
    /\ crashImage = InitialImage
    /\ replayImage = InitialImage
    /\ replayVerified = FALSE
    /\ reuseOutcome = "none"
    /\ reuseCandidate = EmptyConf
    /\ targetBallot = 0
    /\ recoveryPhase = "idle"
    /\ activeCoordinator = 1
    /\ staleCoordinator = 3
    /\ activeBallot = 0
    /\ staleBallot = 0
    /\ prepareResponses = {}
    /\ acceptResponses = {}
    /\ currentPrepareWitness = FALSE
    /\ currentAcceptWitness = FALSE
    /\ currentPrepareSnapshot = {}
    /\ currentAcceptSnapshot = {}
    /\ chosenTuples = {}
    /\ coverage = [name \in CoverageNames |-> 0]

StartConfigWork ==
    /\ phase = "init"
    /\ running
    /\ phase' = "configs-pending"
    /\ configStatus' =
         [configStatus EXCEPT
            ![WinnerRef] = "committed",
            ![LoserRef] = "committed"]
    /\ pendingBarrier' = TRUE
    /\ userStatus' = [userStatus EXCEPT ![OldUser] = "committed"]
    /\ userPinned' = [userPinned EXCEPT ![OldUser] = Conf1]
    /\ userDepsDomain' = [userDepsDomain EXCEPT ![OldUser] = Voters1]
    /\ UNCHANGED <<running, currentConf, currentHardState, confHistory,
                   configOutcome, configResult, appliedByBase,
                   messageAdmission, messageSender, blockedProposal,
                   DurableState, CrashState, ReuseState, RecoveryState,
                   coverage>>

AttemptUserWhilePending ==
    /\ phase = "configs-pending"
    /\ running
    /\ pendingBarrier
    /\ ~blockedProposal
    /\ phase' = "pending-probed"
    /\ blockedProposal' = TRUE
    /\ Bump("pending-blocked")
    /\ UNCHANGED <<VolatileConfigState, userStatus, userPinned,
                   userDepsDomain, messageAdmission, messageSender,
                   DurableState, CrashState, ReuseState, RecoveryState>>

ExecuteWinner ==
    /\ phase = "pending-probed"
    /\ running
    /\ configStatus[WinnerRef] = "committed"
    /\ configStatus[LoserRef] = "committed"
    /\ ExecutionPrecedes(WinnerRef, LoserRef)
    /\ DepsFor(LoserRef)[WinnerRef.replica] >= WinnerRef.instance
    /\ appliedByBase[Conf1] = NoRef
    /\ phase' = "winner-executed"
    /\ currentConf' = Conf2
    /\ currentHardState' = ConfState(Conf2)
    /\ confHistory' = [confHistory EXCEPT ![Conf2] = Voters2]
    /\ configStatus' = [configStatus EXCEPT ![WinnerRef] = "executed"]
    /\ configOutcome' = [configOutcome EXCEPT ![WinnerRef] = "applied"]
    /\ configResult' = [configResult EXCEPT ![WinnerRef] = ConfState(Conf2)]
    /\ appliedByBase' = [appliedByBase EXCEPT ![Conf1] = WinnerRef]
    /\ pendingBarrier' = TRUE
    /\ Bump("winner-applied")
    /\ UNCHANGED <<running, VolatileUserState, DurableState, CrashState,
                   ReuseState, RecoveryState>>

RejectStorageBeforePendingPersist ==
    /\ phase = "winner-executed"
    /\ running
    /\ durableHardState = ConfState(Conf1)
    /\ phase' = "winner-persist-failed"
    /\ Bump("storage-rejected")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, RecoveryState>>

PersistWinnerAndPending ==
    /\ phase = "winner-persist-failed"
    /\ running
    /\ configStatus[WinnerRef] = "executed"
    /\ configStatus[LoserRef] = "committed"
    /\ pendingBarrier
    /\ phase' = IF Mode = "crash-replay" THEN "winner-durable" ELSE "after-replay"
    /\ durableHardState' = currentHardState
    /\ durableHistory' = confHistory
    /\ durableConfigStatus' = configStatus
    /\ durableOutcome' = configOutcome
    /\ durableResult' = configResult
    /\ durableAppliedByBase' = appliedByBase
    /\ durablePendingBarrier' = pendingBarrier
    /\ durableUserStatus' = userStatus
    /\ durableUserPinned' = userPinned
    /\ durableUserDepsDomain' = userDepsDomain
    /\ Bump("pending-persisted")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, CrashState,
                   ReuseState, RecoveryState>>

CrashWithPendingLoser ==
    /\ Mode = "crash-replay"
    /\ phase = "winner-durable"
    /\ running
    /\ durablePendingBarrier
    /\ phase' = "crashed"
    /\ running' = FALSE
    /\ currentConf' = NoConf
    /\ currentHardState' = EmptyConf
    /\ confHistory' = InitialHistory
    /\ configStatus' = EmptyConfigStatus
    /\ configOutcome' = EmptyOutcome
    /\ configResult' = EmptyResult
    /\ appliedByBase' = EmptyApplied
    /\ pendingBarrier' = FALSE
    /\ userStatus' = EmptyUserStatus
    /\ userPinned' = EmptyUserPinned
    /\ userDepsDomain' = EmptyUserDeps
    /\ messageAdmission' = EmptyAdmission
    /\ messageSender' = EmptyMessageSender
    /\ blockedProposal' = FALSE
    /\ crashImage' = DurableImage
    /\ Bump("crash")
    /\ UNCHANGED <<DurableState, replayImage, replayVerified,
                   ReuseState, RecoveryState>>

ReplayDurablePendingImage ==
    /\ Mode = "crash-replay"
    /\ phase = "crashed"
    /\ ~running
    /\ phase' = "after-replay"
    /\ running' = TRUE
    /\ currentConf' = crashImage.hardState.id
    /\ currentHardState' = crashImage.hardState
    /\ confHistory' = crashImage.history
    /\ configStatus' = crashImage.statuses
    /\ configOutcome' = crashImage.outcomes
    /\ configResult' = crashImage.results
    /\ appliedByBase' = crashImage.applied
    /\ pendingBarrier' = crashImage.pending
    /\ userStatus' = crashImage.users
    /\ userPinned' = crashImage.pins
    /\ userDepsDomain' = crashImage.domains
    /\ replayImage' = crashImage
    /\ replayVerified' = TRUE
    /\ Bump("replay")
    /\ UNCHANGED <<messageAdmission, messageSender, blockedProposal,
                   DurableState, crashImage, ReuseState, RecoveryState>>

ExecuteSupersededLoser ==
    /\ phase = "after-replay"
    /\ running
    /\ configStatus[WinnerRef] = "executed"
    /\ configOutcome[WinnerRef] = "applied"
    /\ appliedByBase[Conf1] = WinnerRef
    /\ configStatus[LoserRef] = "committed"
    /\ phase' = "loser-executed"
    /\ configStatus' = [configStatus EXCEPT ![LoserRef] = "executed"]
    /\ configOutcome' =
         [configOutcome EXCEPT ![LoserRef] = "rejected-superseded"]
    /\ configResult' = [configResult EXCEPT ![LoserRef] = EmptyConf]
    /\ pendingBarrier' = FALSE
    /\ Bump("loser-superseded")
    /\ UNCHANGED <<running, currentConf, currentHardState, confHistory,
                   appliedByBase, VolatileUserState, DurableState,
                   CrashState, ReuseState, RecoveryState>>

StartRemoveAndMidUser ==
    /\ phase = "loser-executed"
    /\ running
    /\ currentConf = Conf2
    /\ ~pendingBarrier
    /\ phase' = "remove-pending"
    /\ configStatus' = [configStatus EXCEPT ![RemoveRef] = "committed"]
    /\ pendingBarrier' = TRUE
    /\ userStatus' = [userStatus EXCEPT ![MidUser] = "accepted"]
    /\ userPinned' = [userPinned EXCEPT ![MidUser] = Conf2]
    /\ userDepsDomain' = [userDepsDomain EXCEPT ![MidUser] = Voters2]
    /\ UNCHANGED <<running, currentConf, currentHardState, confHistory,
                   configOutcome, configResult, appliedByBase,
                   messageAdmission, messageSender, blockedProposal,
                   DurableState, CrashState, ReuseState, RecoveryState,
                   coverage>>

ExecuteRemove ==
    /\ phase = "remove-pending"
    /\ running
    /\ configStatus[RemoveRef] = "committed"
    /\ appliedByBase[Conf2] = NoRef
    /\ phase' = "chain-executed"
    /\ currentConf' = Conf3
    /\ currentHardState' = ConfState(Conf3)
    /\ confHistory' = [confHistory EXCEPT ![Conf3] = Voters3]
    /\ configStatus' = [configStatus EXCEPT ![RemoveRef] = "executed"]
    /\ configOutcome' = [configOutcome EXCEPT ![RemoveRef] = "applied"]
    /\ configResult' = [configResult EXCEPT ![RemoveRef] = ConfState(Conf3)]
    /\ appliedByBase' = [appliedByBase EXCEPT ![Conf2] = RemoveRef]
    /\ pendingBarrier' = FALSE
    /\ Bump("remove-applied")
    /\ UNCHANGED <<running, VolatileUserState, DurableState, CrashState,
                   ReuseState, RecoveryState>>

RejectConflictingConfIDReuse ==
    /\ phase = "chain-executed"
    /\ running
    /\ confHistory[Conf2] = Voters2
    /\ phase' = "reuse-checked"
    /\ reuseOutcome' = "rejected"
    /\ reuseCandidate' = [id |-> Conf2, voters |-> {1, 2, 4}]
    /\ Bump("confid-reuse-rejected")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, RecoveryState>>

CanAdmitHistorical(ref, sender) ==
    /\ ref \in UserRefs
    /\ userPinned[ref] = ref.conf
    /\ userDepsDomain[ref] = VotersFor(ref.conf)
    /\ confHistory[ref.conf] = VotersFor(ref.conf)
    /\ ref.replica \in VotersFor(ref.conf)
    /\ sender \in VotersFor(ref.conf)

AdmitOldMessageAfterConf3 ==
    /\ phase = "reuse-checked"
    /\ running
    /\ currentConf = Conf3
    /\ RemovedVoter \notin Voters3
    /\ userStatus[OldUser] = "committed"
    /\ CanAdmitHistorical(OldUser, RemovedVoter)
    /\ phase' = "old-admitted"
    /\ messageAdmission' =
         [messageAdmission EXCEPT ![OldUser] = "admitted"]
    /\ messageSender' = [messageSender EXCEPT ![OldUser] = RemovedVoter]
    /\ Bump("old-message-admitted")
    /\ UNCHANGED <<VolatileConfigState, userStatus, userPinned,
                   userDepsDomain, blockedProposal, DurableState,
                   CrashState, ReuseState, RecoveryState>>

AdmitMidMessageAfterConf3 ==
    /\ phase = "old-admitted"
    /\ running
    /\ currentConf = Conf3
    /\ RemovedVoter \notin Voters3
    /\ userStatus[MidUser] = "accepted"
    /\ CanAdmitHistorical(MidUser, RemovedVoter)
    /\ phase' = "mid-admitted"
    /\ messageAdmission' =
         [messageAdmission EXCEPT ![MidUser] = "admitted"]
    /\ messageSender' = [messageSender EXCEPT ![MidUser] = RemovedVoter]
    /\ Bump("mid-message-admitted")
    /\ UNCHANGED <<VolatileConfigState, userStatus, userPinned,
                   userDepsDomain, blockedProposal, DurableState,
                   CrashState, ReuseState, RecoveryState>>

PersistCompleteChain ==
    /\ phase = "mid-admitted"
    /\ running
    /\ currentConf = Conf3
    /\ phase' = IF Mode = "crash-replay" THEN "done" ELSE "chain-durable"
    /\ durableHardState' = currentHardState
    /\ durableHistory' = confHistory
    /\ durableConfigStatus' = configStatus
    /\ durableOutcome' = configOutcome
    /\ durableResult' = configResult
    /\ durableAppliedByBase' = appliedByBase
    /\ durablePendingBarrier' = pendingBarrier
    /\ durableUserStatus' = userStatus
    /\ durableUserPinned' = userPinned
    /\ durableUserDepsDomain' = userDepsDomain
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, CrashState,
                   ReuseState, RecoveryState, coverage>>

StartRecoveryRace ==
    /\ Mode = "recovery-race"
    /\ phase = "chain-durable"
    /\ running
    /\ currentConf = Conf3
    /\ userPinned[Target] = Conf2
    /\ \E winner \in Coordinators :
         /\ phase' = "race-started"
         /\ activeCoordinator' = winner
         /\ staleCoordinator' = OtherCoordinator(winner)
         /\ activeBallot' = 2
         /\ staleBallot' = 1
         /\ targetBallot' = 0
         /\ recoveryPhase' = "prepare"
         /\ prepareResponses' = {Response(winner, winner, 2, Conf2)}
         /\ Bump("race-started")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, acceptResponses,
                   currentPrepareWitness, currentAcceptWitness,
                   currentPrepareSnapshot, currentAcceptSnapshot,
                   chosenTuples>>

RejectWrongCurrentConfigResponse ==
    /\ phase = "race-started"
    /\ recoveryPhase = "prepare"
    /\ currentConf = Conf3
    /\ Target.conf = Conf2
    /\ Conf3 # Target.conf
    /\ phase' = "race-wrong-rejected"
    /\ Bump("wrong-config-rejected")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, RecoveryState>>

RejectStaleCoordinatorResponse ==
    /\ phase = "race-wrong-rejected"
    /\ recoveryPhase = "prepare"
    /\ staleCoordinator # activeCoordinator
    /\ staleBallot = 1
    /\ activeBallot = 2
    /\ phase' = "race-stale-rejected"
    /\ Bump("stale-ballot-rejected")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, RecoveryState>>

ReceiveCurrentPrepareQuorum ==
    LET other == OtherCoordinator(activeCoordinator)
        response == Response(other, activeCoordinator, activeBallot, Conf2)
        newResponses == prepareResponses \cup {response}
        senders == CountedSenders(newResponses, activeCoordinator,
                                  activeBallot, Conf2)
    IN
    /\ phase = "race-stale-rejected"
    /\ recoveryPhase = "prepare"
    /\ senders = Coordinators
    /\ Cardinality(senders) = SlowQuorum(Conf3)
    /\ Cardinality(senders) < SlowQuorum(Conf2)
    /\ phase' = "race-prepare-current"
    /\ prepareResponses' = newResponses
    /\ currentPrepareWitness' = TRUE
    /\ currentPrepareSnapshot' = senders
    /\ Bump("current-prepare-insufficient")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, targetBallot, recoveryPhase,
                   activeCoordinator, staleCoordinator, activeBallot,
                   staleBallot, acceptResponses, currentAcceptWitness,
                   currentAcceptSnapshot, chosenTuples>>

ReceiveDuplicatePrepare ==
    LET sender == OtherCoordinator(activeCoordinator)
        duplicate == Response(sender, activeCoordinator, activeBallot, Conf2)
    IN
    /\ phase = "race-prepare-current"
    /\ duplicate \in prepareResponses
    /\ phase' = "race-prepare-dedup"
    /\ Bump("duplicate-prepare")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, RecoveryState>>

ReceiveRemovedPinnedPrepare ==
    LET response == Response(RemovedVoter, activeCoordinator,
                             activeBallot, Conf2)
        newResponses == prepareResponses \cup {response}
        senders == CountedSenders(newResponses, activeCoordinator,
                                  activeBallot, Conf2)
    IN
    /\ phase = "race-prepare-dedup"
    /\ RemovedVoter \in Voters2
    /\ RemovedVoter \notin Voters3
    /\ Cardinality(senders) = SlowQuorum(Conf2)
    /\ phase' = "race-accept"
    /\ prepareResponses' = newResponses
    /\ acceptResponses' =
         {Response(activeCoordinator, activeCoordinator,
                   activeBallot, Conf2)}
    /\ recoveryPhase' = "accept"
    /\ Bump("removed-prepare-counted")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, targetBallot,
                   activeCoordinator, staleCoordinator, activeBallot,
                   staleBallot, currentPrepareWitness,
                   currentAcceptWitness, currentPrepareSnapshot,
                   currentAcceptSnapshot, chosenTuples>>

ReceiveCurrentAcceptQuorum ==
    LET other == OtherCoordinator(activeCoordinator)
        response == Response(other, activeCoordinator, activeBallot, Conf2)
        newResponses == acceptResponses \cup {response}
        senders == CountedSenders(newResponses, activeCoordinator,
                                  activeBallot, Conf2)
    IN
    /\ phase = "race-accept"
    /\ recoveryPhase = "accept"
    /\ senders = Coordinators
    /\ Cardinality(senders) = SlowQuorum(Conf3)
    /\ Cardinality(senders) < SlowQuorum(Conf2)
    /\ phase' = "race-accept-current"
    /\ acceptResponses' = newResponses
    /\ currentAcceptWitness' = TRUE
    /\ currentAcceptSnapshot' = senders
    /\ Bump("current-accept-insufficient")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, targetBallot, recoveryPhase,
                   activeCoordinator, staleCoordinator, activeBallot,
                   staleBallot, prepareResponses, currentPrepareWitness,
                   currentPrepareSnapshot, chosenTuples>>

ReceiveDuplicateAccept ==
    LET sender == OtherCoordinator(activeCoordinator)
        duplicate == Response(sender, activeCoordinator, activeBallot, Conf2)
    IN
    /\ phase = "race-accept-current"
    /\ duplicate \in acceptResponses
    /\ phase' = "race-accept-dedup"
    /\ Bump("duplicate-accept")
    /\ UNCHANGED <<VolatileConfigState, VolatileUserState, DurableState,
                   CrashState, ReuseState, RecoveryState>>

ReceiveRemovedPinnedAccept ==
    LET response == Response(RemovedVoter, activeCoordinator,
                             activeBallot, Conf2)
        newResponses == acceptResponses \cup {response}
        senders == CountedSenders(newResponses, activeCoordinator,
                                  activeBallot, Conf2)
    IN
    /\ phase = "race-accept-dedup"
    /\ recoveryPhase = "accept"
    /\ RemovedVoter \in Voters2
    /\ RemovedVoter \notin Voters3
    /\ Cardinality(senders) = SlowQuorum(Conf2)
    /\ phase' = "done"
    /\ acceptResponses' = newResponses
    /\ recoveryPhase' = "committed"
    /\ chosenTuples' = {TupleFor(Target)}
    /\ userStatus' = [userStatus EXCEPT ![Target] = "committed"]
    /\ Bump("removed-accept-counted")
    /\ UNCHANGED <<VolatileConfigState, userPinned, userDepsDomain,
                   messageAdmission, messageSender, blockedProposal,
                   DurableState, CrashState, ReuseState, targetBallot,
                   activeCoordinator, staleCoordinator, activeBallot,
                   staleBallot, prepareResponses, currentPrepareWitness,
                   currentAcceptWitness, currentPrepareSnapshot,
                   currentAcceptSnapshot>>

Next ==
    \/ StartConfigWork
    \/ AttemptUserWhilePending
    \/ ExecuteWinner
    \/ RejectStorageBeforePendingPersist
    \/ PersistWinnerAndPending
    \/ CrashWithPendingLoser
    \/ ReplayDurablePendingImage
    \/ ExecuteSupersededLoser
    \/ StartRemoveAndMidUser
    \/ ExecuteRemove
    \/ RejectConflictingConfIDReuse
    \/ AdmitOldMessageAfterConf3
    \/ AdmitMidMessageAfterConf3
    \/ PersistCompleteChain
    \/ StartRecoveryRace
    \/ RejectWrongCurrentConfigResponse
    \/ RejectStaleCoordinatorResponse
    \/ ReceiveCurrentPrepareQuorum
    \/ ReceiveDuplicatePrepare
    \/ ReceiveRemovedPinnedPrepare
    \/ ReceiveCurrentAcceptQuorum
    \/ ReceiveDuplicateAccept
    \/ ReceiveRemovedPinnedAccept

HistoryExact(history) ==
    /\ history[Conf1] = Voters1
    /\ \A c \in ConfigIDs : history[c] \in {{}, VotersFor(c)}
    /\ history[Conf3] # {} => history[Conf2] # {}

SnapshotCausallyClosed(hardState, history, statuses, outcomes, results,
                       applied) ==
    /\ HistoryExact(history)
    /\ hardState.id = NoConf \/ history[hardState.id] = hardState.voters
    /\ \A ref \in ConfigRefs :
         /\ (outcomes[ref] = "none" => results[ref] = EmptyConf)
         /\ (outcomes[ref] = "applied" =>
                /\ statuses[ref] = "executed"
                /\ results[ref] = ExpectedSuccessor(ref)
                /\ applied[ref.conf] = ref
                /\ history[results[ref].id] = results[ref].voters)
         /\ (outcomes[ref] = "rejected-superseded" =>
                /\ statuses[ref] = "executed"
                /\ results[ref] = EmptyConf
                /\ ref = LoserRef
                /\ outcomes[WinnerRef] = "applied")
    /\ (hardState.id = Conf2 =>
           /\ outcomes[WinnerRef] = "applied"
           /\ results[WinnerRef] = ConfState(Conf2)
           /\ applied[Conf1] = WinnerRef)
    /\ (hardState.id = Conf3 =>
           /\ outcomes[WinnerRef] = "applied"
           /\ outcomes[RemoveRef] = "applied"
           /\ results[RemoveRef] = ConfState(Conf3)
           /\ applied[Conf2] = RemoveRef)
    /\ applied[Conf1] \in {NoRef, WinnerRef}
    /\ applied[Conf2] \in {NoRef, RemoveRef}

PendingMatchesStatuses ==
    /\ pendingBarrier =
         (running /\ \E ref \in ConfigRefs : configStatus[ref] = "committed")
    /\ durablePendingBarrier =
         (\E ref \in ConfigRefs : durableConfigStatus[ref] = "committed")

SerializedConfigExecution ==
    /\ ExecutionPrecedes(WinnerRef, LoserRef)
    /\ DepsFor(LoserRef)[WinnerRef.replica] >= WinnerRef.instance
    /\ (configStatus[LoserRef] = "executed" =>
           /\ configStatus[WinnerRef] = "executed"
           /\ configOutcome[WinnerRef] = "applied")
    /\ (configStatus[RemoveRef] # "none" =>
           /\ configStatus[WinnerRef] = "executed"
           /\ configStatus[LoserRef] = "executed"
           /\ configOutcome[LoserRef] = "rejected-superseded")
    /\ (configOutcome[LoserRef] = "rejected-superseded" =>
           /\ appliedByBase[Conf1] = WinnerRef
           /\ configResult[LoserRef] = EmptyConf)
    /\ (configOutcome[RemoveRef] = "applied" =>
           appliedByBase[Conf2] = RemoveRef)

ExactSuccessorsAndNoConflictingReuse ==
    /\ HistoryExact(confHistory)
    /\ HistoryExact(durableHistory)
    /\ \A ref \in ConfigRefs :
         configOutcome[ref] = "applied" =>
             /\ configResult[ref] = ExpectedSuccessor(ref)
             /\ configResult[ref].id = ref.conf + 1
    /\ reuseOutcome = "rejected" =>
         /\ reuseCandidate.id = Conf2
         /\ reuseCandidate.voters # Voters2
         /\ confHistory[Conf2] = Voters2
         /\ durableHistory[Conf2] = Voters2

CurrentAndDurableHardStateCausal ==
    /\ SnapshotCausallyClosed(currentHardState, confHistory, configStatus,
                              configOutcome, configResult, appliedByBase)
    /\ SnapshotCausallyClosed(durableHardState, durableHistory,
                              durableConfigStatus, durableOutcome,
                              durableResult, durableAppliedByBase)
    /\ (running => currentHardState = ConfState(currentConf))
    /\ (~running => /\ currentConf = NoConf
                    /\ currentHardState = EmptyConf)

ImageCausallyClosed(image) ==
    SnapshotCausallyClosed(image.hardState, image.history, image.statuses,
                           image.outcomes, image.results, image.applied)

ReplayIdentical ==
    /\ ImageCausallyClosed(crashImage)
    /\ ImageCausallyClosed(replayImage)
    /\ (replayVerified =>
           /\ replayImage = crashImage
           /\ crashImage.pending
           /\ crashImage.hardState = ConfState(Conf2)
           /\ crashImage.statuses[WinnerRef] = "executed"
           /\ crashImage.statuses[LoserRef] = "committed"
           /\ crashImage.outcomes[WinnerRef] = "applied"
           /\ crashImage.results[WinnerRef] = ConfState(Conf2))

HistoricalOwnerAndDomainPinned ==
    /\ \A ref \in UserRefs :
         userStatus[ref] # "none" =>
             /\ userPinned[ref] = ref.conf
             /\ userDepsDomain[ref] = VotersFor(ref.conf)
             /\ ref.replica \in VotersFor(ref.conf)
             /\ DOMAIN DepsFor(ref) = VotersFor(ref.conf)
    /\ \A ref \in UserRefs :
         durableUserStatus[ref] # "none" =>
             /\ durableUserPinned[ref] = ref.conf
             /\ durableUserDepsDomain[ref] = VotersFor(ref.conf)
    /\ (messageAdmission[OldUser] = "admitted" =>
           /\ messageSender[OldUser] = RemovedVoter
           /\ CanAdmitHistorical(OldUser, messageSender[OldUser]))
    /\ (messageAdmission[MidUser] = "admitted" =>
           /\ messageSender[MidUser] = RemovedVoter
           /\ CanAdmitHistorical(MidUser, messageSender[MidUser]))

OldCommittedWorkRemainsAdmissibleAfterConf3 ==
    running /\ currentConf = Conf3 /\ userStatus[OldUser] = "committed" =>
        /\ CanAdmitHistorical(OldUser, RemovedVoter)
        /\ RemovedVoter \notin Voters3

CanonicalBySender(responses) ==
    \A left, right \in responses :
        left.sender = right.sender => left = right

RecoveryRacePinnedAndUnmixed ==
    /\ Target.conf = Conf2
    /\ userStatus[Target] # "none" =>
         /\ userPinned[Target] = Conf2
         /\ userDepsDomain[Target] = Voters2
         /\ DOMAIN DepsFor(Target) = Voters2
    /\ CanonicalBySender(prepareResponses)
    /\ CanonicalBySender(acceptResponses)
    /\ (recoveryPhase # "idle" =>
         /\ activeCoordinator \in Coordinators
         /\ staleCoordinator = OtherCoordinator(activeCoordinator)
         /\ activeCoordinator # staleCoordinator
         /\ targetBallot = 0
         /\ staleBallot = 1
         /\ activeBallot = 2
         /\ \A response \in prepareResponses \cup acceptResponses :
              /\ response.conf = Target.conf
              /\ response.coordinator = activeCoordinator
              /\ response.ballot = activeBallot
              /\ response.sender \in Voters2)
    /\ (currentPrepareWitness =>
         /\ currentPrepareSnapshot = Coordinators
         /\ currentPrepareSnapshot \subseteq Voters3
         /\ Cardinality(currentPrepareSnapshot) = SlowQuorum(Conf3)
         /\ Cardinality(currentPrepareSnapshot) < SlowQuorum(Conf2))
    /\ (currentAcceptWitness =>
         /\ currentAcceptSnapshot = Coordinators
         /\ currentAcceptSnapshot \subseteq Voters3
         /\ Cardinality(currentAcceptSnapshot) = SlowQuorum(Conf3)
         /\ Cardinality(currentAcceptSnapshot) < SlowQuorum(Conf2))
    /\ (recoveryPhase \in {"accept", "committed"} =>
         /\ RemovedVoter \in
              CountedSenders(prepareResponses, activeCoordinator,
                             activeBallot, Conf2)
         /\ Cardinality(CountedSenders(prepareResponses,
                                       activeCoordinator, activeBallot,
                                       Conf2)) = SlowQuorum(Conf2))
    /\ (recoveryPhase = "committed" =>
         /\ RemovedVoter \in
              CountedSenders(acceptResponses, activeCoordinator,
                             activeBallot, Conf2)
         /\ Cardinality(CountedSenders(acceptResponses,
                                       activeCoordinator, activeBallot,
                                       Conf2)) = SlowQuorum(Conf2)
         /\ chosenTuples = {TupleFor(Target)})
    /\ Cardinality(chosenTuples) <= 1

Safety ==
    /\ PendingMatchesStatuses
    /\ SerializedConfigExecution
    /\ ExactSuccessorsAndNoConflictingReuse
    /\ CurrentAndDurableHardStateCausal
    /\ ReplayIdentical
    /\ HistoricalOwnerAndDomainPinned
    /\ RecoveryRacePinnedAndUnmixed
    /\ OldCommittedWorkRemainsAdmissibleAfterConf3

CoverageReached(names) == \A name \in names : coverage[name] = 1

EventuallyCoversCrashReplay ==
    <> ( /\ phase = "done"
         /\ Mode = "crash-replay"
         /\ CoverageReached(CrashCoverage)
         /\ replayVerified
         /\ replayImage = crashImage
         /\ durableHardState = ConfState(Conf3)
         /\ durableHistory = [c \in ConfigIDs |-> VotersFor(c)]
         /\ durableOutcome[WinnerRef] = "applied"
         /\ durableOutcome[LoserRef] = "rejected-superseded"
         /\ durableOutcome[RemoveRef] = "applied"
         /\ messageAdmission[OldUser] = "admitted"
         /\ messageAdmission[MidUser] = "admitted" )

EventuallyCoversRecoveryRace ==
    <> ( /\ phase = "done"
         /\ Mode = "recovery-race"
         /\ CoverageReached(RaceCoverage)
         /\ durableHardState = ConfState(Conf3)
         /\ recoveryPhase = "committed"
         /\ activeCoordinator # staleCoordinator
         /\ targetBallot = 0
         /\ staleBallot = 1
         /\ activeBallot = 2
         /\ currentPrepareSnapshot = Coordinators
         /\ currentAcceptSnapshot = Coordinators
         /\ RemovedVoter \in
              CountedSenders(prepareResponses, activeCoordinator,
                             activeBallot, Conf2)
         /\ RemovedVoter \in
              CountedSenders(acceptResponses, activeCoordinator,
                             activeBallot, Conf2)
         /\ chosenTuples = {TupleFor(Target)}
         /\ userStatus[Target] = "committed" )

CommonFairness ==
    /\ WF_Vars(StartConfigWork)
    /\ WF_Vars(AttemptUserWhilePending)
    /\ WF_Vars(ExecuteWinner)
    /\ WF_Vars(RejectStorageBeforePendingPersist)
    /\ WF_Vars(PersistWinnerAndPending)
    /\ WF_Vars(ExecuteSupersededLoser)
    /\ WF_Vars(StartRemoveAndMidUser)
    /\ WF_Vars(ExecuteRemove)
    /\ WF_Vars(RejectConflictingConfIDReuse)
    /\ WF_Vars(AdmitOldMessageAfterConf3)
    /\ WF_Vars(AdmitMidMessageAfterConf3)
    /\ WF_Vars(PersistCompleteChain)

CrashReplaySpec ==
    /\ Init
    /\ [][Next]_Vars
    /\ CommonFairness
    /\ WF_Vars(CrashWithPendingLoser)
    /\ WF_Vars(ReplayDurablePendingImage)

RecoveryRaceSpec ==
    /\ Init
    /\ [][Next]_Vars
    /\ CommonFairness
    /\ WF_Vars(StartRecoveryRace)
    /\ WF_Vars(RejectWrongCurrentConfigResponse)
    /\ WF_Vars(RejectStaleCoordinatorResponse)
    /\ WF_Vars(ReceiveCurrentPrepareQuorum)
    /\ WF_Vars(ReceiveDuplicatePrepare)
    /\ WF_Vars(ReceiveRemovedPinnedPrepare)
    /\ WF_Vars(ReceiveCurrentAcceptQuorum)
    /\ WF_Vars(ReceiveDuplicateAccept)
    /\ WF_Vars(ReceiveRemovedPinnedAccept)

====
