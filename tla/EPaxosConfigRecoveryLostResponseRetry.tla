---- MODULE EPaxosConfigRecoveryLostResponseRetry ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite old-config recovery lost-response retry model. It checks  *)
(* one old instance whose Ref.Conf is the four-voter configuration while    *)
(* the current configuration has already removed voter 4. The scenario makes*)
(* one old-voter prepare response and one old-voter accept response explicit*)
(* finite pre-retry losses: each response would complete the old 3/4 quorum *)
(* with the coordinator vote and voter 3, but it is not counted before the  *)
(* deterministic retry rebroadcast. The replacement responses delivered     *)
(* after retry complete recovery. This is not arbitrary message loss, an    *)
(* arbitrary retry history, joint consensus, arbitrary membership histories,*)
(* or an unbounded recovery proof.                                          *)
(***************************************************************************)
VARIABLES phase, prepareOK, acceptOK, recordStatus, retryTargets, depsWidth,
          prepareLossAttempted, acceptLossAttempted,
          prepareRetryBroadcasted, acceptRetryBroadcasted

Conf1 == 1
Conf2 == 2
Coordinator == 2
FirstResponder == 3
RemovedVoter == 4

OldVoters == {1, 2, 3, 4}
CurrentVoters == {1, 2, 3}
OldPeers == OldVoters \ {Coordinator}
OldSlowQuorum == 3
NewSlowQuorum == 2

Phases == {"current-after-removal", "prepare-sent", "prepare-first-ok",
           "prepare-lost-before-retry", "prepare-retried", "accept-sent",
           "accept-first-ok", "accept-lost-before-retry", "accept-retried",
           "committed-old", "executed-old"}
Statuses == {"preaccepted", "accepted_noop", "committed_noop", "executed_noop"}
AllVoters == {1, 2, 3, 4}

Vars == <<phase, prepareOK, acceptOK, recordStatus, retryTargets, depsWidth,
          prepareLossAttempted, acceptLossAttempted,
          prepareRetryBroadcasted, acceptRetryBroadcasted>>

TypeOK ==
    /\ phase \in Phases
    /\ prepareOK \subseteq OldVoters
    /\ acceptOK \subseteq OldVoters
    /\ recordStatus \in Statuses
    /\ retryTargets \subseteq AllVoters
    /\ depsWidth \in 0..4
    /\ prepareLossAttempted \in BOOLEAN
    /\ acceptLossAttempted \in BOOLEAN
    /\ prepareRetryBroadcasted \in BOOLEAN
    /\ acceptRetryBroadcasted \in BOOLEAN

Init ==
    /\ phase = "current-after-removal"
    /\ prepareOK = {}
    /\ acceptOK = {}
    /\ recordStatus = "preaccepted"
    /\ retryTargets = {}
    /\ depsWidth = 0
    /\ prepareLossAttempted = FALSE
    /\ acceptLossAttempted = FALSE
    /\ prepareRetryBroadcasted = FALSE
    /\ acceptRetryBroadcasted = FALSE

StartOldRecovery ==
    /\ phase = "current-after-removal"
    /\ phase' = "prepare-sent"
    /\ prepareOK' = {Coordinator}
    /\ retryTargets' = OldPeers
    /\ depsWidth' = Cardinality(OldVoters)
    /\ UNCHANGED <<acceptOK, recordStatus, prepareLossAttempted,
                  acceptLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted>>

FirstPrepareOK ==
    /\ phase = "prepare-sent"
    /\ prepareOK' = prepareOK \cup {FirstResponder}
    /\ Cardinality(prepareOK') = 2
    /\ phase' = "prepare-first-ok"
    /\ UNCHANGED <<acceptOK, recordStatus, retryTargets, depsWidth,
                  prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted>>

LostPrepareResponseBeforeRetry ==
    /\ phase = "prepare-first-ok"
    /\ RemovedVoter \notin prepareOK
    /\ prepareLossAttempted' = TRUE
    /\ phase' = "prepare-lost-before-retry"
    /\ UNCHANGED <<prepareOK, acceptOK, recordStatus, retryTargets, depsWidth,
                  acceptLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted>>

PrepareRetry ==
    /\ phase = "prepare-lost-before-retry"
    /\ prepareLossAttempted
    /\ Cardinality(prepareOK) = 2
    /\ phase' = "prepare-retried"
    /\ retryTargets' = OldPeers
    /\ depsWidth' = Cardinality(OldVoters)
    /\ prepareRetryBroadcasted' = TRUE
    /\ UNCHANGED <<prepareOK, acceptOK, recordStatus, prepareLossAttempted,
                  acceptLossAttempted, acceptRetryBroadcasted>>

LostPrepareReplacementOK ==
    /\ phase = "prepare-retried"
    /\ prepareRetryBroadcasted
    /\ prepareOK' = prepareOK \cup {RemovedVoter}
    /\ Cardinality(prepareOK') = OldSlowQuorum
    /\ phase' = "accept-sent"
    /\ recordStatus' = "accepted_noop"
    /\ acceptOK' = {Coordinator}
    /\ retryTargets' = OldPeers
    /\ depsWidth' = Cardinality(OldVoters)
    /\ UNCHANGED <<prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted>>

FirstAcceptOK ==
    /\ phase = "accept-sent"
    /\ acceptOK' = acceptOK \cup {FirstResponder}
    /\ Cardinality(acceptOK') = 2
    /\ phase' = "accept-first-ok"
    /\ UNCHANGED <<prepareOK, recordStatus, retryTargets, depsWidth,
                  prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted>>

LostAcceptResponseBeforeRetry ==
    /\ phase = "accept-first-ok"
    /\ RemovedVoter \notin acceptOK
    /\ acceptLossAttempted' = TRUE
    /\ phase' = "accept-lost-before-retry"
    /\ UNCHANGED <<prepareOK, acceptOK, recordStatus, retryTargets, depsWidth,
                  prepareLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted>>

AcceptRetry ==
    /\ phase = "accept-lost-before-retry"
    /\ acceptLossAttempted
    /\ Cardinality(acceptOK) = 2
    /\ phase' = "accept-retried"
    /\ retryTargets' = OldPeers
    /\ depsWidth' = Cardinality(OldVoters)
    /\ acceptRetryBroadcasted' = TRUE
    /\ UNCHANGED <<prepareOK, acceptOK, recordStatus, prepareLossAttempted,
                  acceptLossAttempted, prepareRetryBroadcasted>>

LostAcceptReplacementOK ==
    /\ phase = "accept-retried"
    /\ acceptRetryBroadcasted
    /\ acceptOK' = acceptOK \cup {RemovedVoter}
    /\ Cardinality(acceptOK') = OldSlowQuorum
    /\ phase' = "committed-old"
    /\ recordStatus' = "committed_noop"
    /\ UNCHANGED <<prepareOK, retryTargets, depsWidth, prepareLossAttempted,
                  acceptLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted>>

ExecuteOld ==
    /\ phase = "committed-old"
    /\ recordStatus = "committed_noop"
    /\ phase' = "executed-old"
    /\ recordStatus' = "executed_noop"
    /\ UNCHANGED <<prepareOK, acceptOK, retryTargets, depsWidth,
                  prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted>>

Next ==
    \/ StartOldRecovery
    \/ FirstPrepareOK
    \/ LostPrepareResponseBeforeRetry
    \/ PrepareRetry
    \/ LostPrepareReplacementOK
    \/ FirstAcceptOK
    \/ LostAcceptResponseBeforeRetry
    \/ AcceptRetry
    \/ LostAcceptReplacementOK
    \/ ExecuteOld

CurrentConfigExcludesRemovedVoter ==
    /\ Conf2 = 2
    /\ RemovedVoter \notin CurrentVoters

LostResponsesDoNotCountBeforeRetry ==
    /\ (prepareLossAttempted /\ ~prepareRetryBroadcasted) =>
        /\ RemovedVoter \notin prepareOK
        /\ Cardinality(prepareOK) = 2
        /\ recordStatus = "preaccepted"
    /\ (acceptLossAttempted /\ ~acceptRetryBroadcasted) =>
        /\ RemovedVoter \notin acceptOK
        /\ Cardinality(acceptOK) = 2
        /\ recordStatus = "accepted_noop"

RetryTargetsPinnedToOldConfig ==
    (prepareRetryBroadcasted \/ acceptRetryBroadcasted) =>
        /\ retryTargets = OldPeers
        /\ retryTargets \subseteq OldVoters
        /\ Coordinator \notin retryTargets
        /\ RemovedVoter \in retryTargets
        /\ RemovedVoter \notin CurrentVoters

RetryUsesOldDependencyWidth ==
    (prepareRetryBroadcasted \/ acceptRetryBroadcasted) => depsWidth = Cardinality(OldVoters)

RetryDoesNotAdvanceWithoutReplacement ==
    /\ phase = "prepare-retried" =>
        /\ Cardinality(prepareOK) = 2
        /\ recordStatus = "preaccepted"
    /\ phase = "accept-retried" =>
        /\ Cardinality(acceptOK) = 2
        /\ recordStatus = "accepted_noop"

OldQuorumAfterReplacementResponses ==
    recordStatus \in {"committed_noop", "executed_noop"} =>
        /\ Cardinality(prepareOK) = OldSlowQuorum
        /\ Cardinality(acceptOK) = OldSlowQuorum
        /\ RemovedVoter \in prepareOK
        /\ RemovedVoter \in acceptOK

NewQuorumWouldBeInsufficientForOldFourVoterInstance ==
    recordStatus \in {"committed_noop", "executed_noop"} =>
        Cardinality(acceptOK) > NewSlowQuorum

Safety ==
    /\ CurrentConfigExcludesRemovedVoter
    /\ LostResponsesDoNotCountBeforeRetry
    /\ RetryTargetsPinnedToOldConfig
    /\ RetryUsesOldDependencyWidth
    /\ RetryDoesNotAdvanceWithoutReplacement
    /\ OldQuorumAfterReplacementResponses
    /\ NewQuorumWouldBeInsufficientForOldFourVoterInstance

EventuallyCoversLostResponseRetry ==
    <> ( /\ phase = "executed-old"
         /\ recordStatus = "executed_noop"
         /\ prepareLossAttempted
         /\ acceptLossAttempted
         /\ prepareRetryBroadcasted
         /\ acceptRetryBroadcasted
         /\ RemovedVoter \in prepareOK
         /\ RemovedVoter \in acceptOK
         /\ Cardinality(prepareOK) = OldSlowQuorum
         /\ Cardinality(acceptOK) = OldSlowQuorum )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartOldRecovery)
    /\ WF_Vars(FirstPrepareOK)
    /\ WF_Vars(LostPrepareResponseBeforeRetry)
    /\ WF_Vars(PrepareRetry)
    /\ WF_Vars(LostPrepareReplacementOK)
    /\ WF_Vars(FirstAcceptOK)
    /\ WF_Vars(LostAcceptResponseBeforeRetry)
    /\ WF_Vars(AcceptRetry)
    /\ WF_Vars(LostAcceptReplacementOK)
    /\ WF_Vars(ExecuteOld)

====
