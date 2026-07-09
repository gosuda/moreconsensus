---- MODULE EPaxosConfigChainRecoveryLostResponseRetry ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite mid-chain recovery lost-response retry model. It checks   *)
(* one Conf2 instance after Conf1 added voter 4 and Conf2 later removed      *)
(* voter 2, leaving Conf3 current. Recovery must keep retries pinned to the  *)
(* Conf2 peers and dependency width even though the current Conf3 quorum     *)
(* {1,4} would be sufficient for new work. The modeled removed-voter prepare*)
(* response and accept response are lost before deterministic retry; only    *)
(* replacement responses after retry complete the old Conf2 3/4 quorum. This*)
(* is not arbitrary message loss, arbitrary retry histories, arbitrary       *)
(* membership histories, joint consensus, or an unbounded recovery proof.    *)
(***************************************************************************)

VARIABLES phase, currentConf, prepareOK, acceptOK, recordStatus, retryTargets,
          depsWidth, addedVoterCounted, removedVoterCounted,
          prepareLossAttempted, acceptLossAttempted,
          prepareRetryBroadcasted, acceptRetryBroadcasted,
          removedNewProposal

Conf1 == 1
Conf2 == 2
Conf3 == 3
Coordinator == 1
AddedVoter == 4
RemovedVoter == 2

Voters1 == {1, 2, 3}
Voters2 == Voters1 \cup {AddedVoter}
Voters3 == Voters2 \ {RemovedVoter}
MidPeers == Voters2 \ {Coordinator}
MidSlowQuorum == 3
CurrentSlowQuorum == 2

Phases == {"current-after-chain", "prepare-sent", "prepare-first-ok",
           "prepare-lost-before-retry", "prepare-retried", "accept-sent",
           "accept-first-ok", "accept-lost-before-retry", "accept-retried",
           "committed-mid", "executed-mid"}
Statuses == {"preaccepted", "accepted_noop", "committed_noop", "executed_noop"}
ProposalStatuses == {"none", "rejected", "accepted"}

Vars == <<phase, currentConf, prepareOK, acceptOK, recordStatus, retryTargets,
          depsWidth, addedVoterCounted, removedVoterCounted,
          prepareLossAttempted, acceptLossAttempted,
          prepareRetryBroadcasted, acceptRetryBroadcasted,
          removedNewProposal>>

TypeOK ==
    /\ phase \in Phases
    /\ currentConf \in {Conf1, Conf2, Conf3}
    /\ prepareOK \subseteq Voters2
    /\ acceptOK \subseteq Voters2
    /\ recordStatus \in Statuses
    /\ retryTargets \subseteq Voters2
    /\ depsWidth \in 0..4
    /\ addedVoterCounted \in BOOLEAN
    /\ removedVoterCounted \in BOOLEAN
    /\ prepareLossAttempted \in BOOLEAN
    /\ acceptLossAttempted \in BOOLEAN
    /\ prepareRetryBroadcasted \in BOOLEAN
    /\ acceptRetryBroadcasted \in BOOLEAN
    /\ removedNewProposal \in ProposalStatuses

Init ==
    /\ phase = "current-after-chain"
    /\ currentConf = Conf3
    /\ prepareOK = {}
    /\ acceptOK = {}
    /\ recordStatus = "preaccepted"
    /\ retryTargets = {}
    /\ depsWidth = 0
    /\ addedVoterCounted = FALSE
    /\ removedVoterCounted = FALSE
    /\ prepareLossAttempted = FALSE
    /\ acceptLossAttempted = FALSE
    /\ prepareRetryBroadcasted = FALSE
    /\ acceptRetryBroadcasted = FALSE
    /\ removedNewProposal = "none"

StartMidRecovery ==
    /\ phase = "current-after-chain"
    /\ phase' = "prepare-sent"
    /\ prepareOK' = {Coordinator}
    /\ retryTargets' = MidPeers
    /\ depsWidth' = Cardinality(Voters2)
    /\ UNCHANGED <<currentConf, acceptOK, recordStatus, addedVoterCounted,
                  removedVoterCounted, prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted,
                  removedNewProposal>>

FirstPrepareOK ==
    /\ phase = "prepare-sent"
    /\ prepareOK' = prepareOK \cup {AddedVoter}
    /\ Cardinality(prepareOK') = 2
    /\ phase' = "prepare-first-ok"
    /\ addedVoterCounted' = TRUE
    /\ UNCHANGED <<currentConf, acceptOK, recordStatus, retryTargets, depsWidth,
                  removedVoterCounted, prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted,
                  removedNewProposal>>

LostPrepareResponseBeforeRetry ==
    /\ phase = "prepare-first-ok"
    /\ RemovedVoter \notin prepareOK
    /\ prepareLossAttempted' = TRUE
    /\ phase' = "prepare-lost-before-retry"
    /\ UNCHANGED <<currentConf, prepareOK, acceptOK, recordStatus, retryTargets,
                  depsWidth, addedVoterCounted, removedVoterCounted,
                  acceptLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted, removedNewProposal>>

PrepareRetry ==
    /\ phase = "prepare-lost-before-retry"
    /\ prepareLossAttempted
    /\ Cardinality(prepareOK) = 2
    /\ phase' = "prepare-retried"
    /\ retryTargets' = MidPeers
    /\ depsWidth' = Cardinality(Voters2)
    /\ prepareRetryBroadcasted' = TRUE
    /\ UNCHANGED <<currentConf, prepareOK, acceptOK, recordStatus,
                  addedVoterCounted, removedVoterCounted, prepareLossAttempted,
                  acceptLossAttempted, acceptRetryBroadcasted,
                  removedNewProposal>>

LostPrepareReplacementOK ==
    /\ phase = "prepare-retried"
    /\ prepareRetryBroadcasted
    /\ prepareOK' = prepareOK \cup {RemovedVoter}
    /\ Cardinality(prepareOK') = MidSlowQuorum
    /\ phase' = "accept-sent"
    /\ recordStatus' = "accepted_noop"
    /\ acceptOK' = {Coordinator}
    /\ retryTargets' = MidPeers
    /\ depsWidth' = Cardinality(Voters2)
    /\ removedVoterCounted' = TRUE
    /\ UNCHANGED <<currentConf, addedVoterCounted, prepareLossAttempted,
                  acceptLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted, removedNewProposal>>

FirstAcceptOK ==
    /\ phase = "accept-sent"
    /\ acceptOK' = acceptOK \cup {AddedVoter}
    /\ Cardinality(acceptOK') = 2
    /\ phase' = "accept-first-ok"
    /\ addedVoterCounted' = TRUE
    /\ UNCHANGED <<currentConf, prepareOK, recordStatus, retryTargets, depsWidth,
                  removedVoterCounted, prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted,
                  removedNewProposal>>

LostAcceptResponseBeforeRetry ==
    /\ phase = "accept-first-ok"
    /\ RemovedVoter \notin acceptOK
    /\ acceptLossAttempted' = TRUE
    /\ phase' = "accept-lost-before-retry"
    /\ UNCHANGED <<currentConf, prepareOK, acceptOK, recordStatus, retryTargets,
                  depsWidth, addedVoterCounted, removedVoterCounted,
                  prepareLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted, removedNewProposal>>

AcceptRetry ==
    /\ phase = "accept-lost-before-retry"
    /\ acceptLossAttempted
    /\ Cardinality(acceptOK) = 2
    /\ phase' = "accept-retried"
    /\ retryTargets' = MidPeers
    /\ depsWidth' = Cardinality(Voters2)
    /\ acceptRetryBroadcasted' = TRUE
    /\ UNCHANGED <<currentConf, prepareOK, acceptOK, recordStatus,
                  addedVoterCounted, removedVoterCounted, prepareLossAttempted,
                  acceptLossAttempted, prepareRetryBroadcasted,
                  removedNewProposal>>

LostAcceptReplacementOK ==
    /\ phase = "accept-retried"
    /\ acceptRetryBroadcasted
    /\ acceptOK' = acceptOK \cup {RemovedVoter}
    /\ Cardinality(acceptOK') = MidSlowQuorum
    /\ phase' = "committed-mid"
    /\ recordStatus' = "committed_noop"
    /\ removedVoterCounted' = TRUE
    /\ UNCHANGED <<currentConf, prepareOK, retryTargets, depsWidth,
                  addedVoterCounted, prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted,
                  removedNewProposal>>

ExecuteMid ==
    /\ phase = "committed-mid"
    /\ recordStatus = "committed_noop"
    /\ phase' = "executed-mid"
    /\ recordStatus' = "executed_noop"
    /\ UNCHANGED <<currentConf, prepareOK, acceptOK, retryTargets, depsWidth,
                  addedVoterCounted, removedVoterCounted, prepareLossAttempted,
                  acceptLossAttempted, prepareRetryBroadcasted,
                  acceptRetryBroadcasted, removedNewProposal>>

RemovedAttemptsNewProposal ==
    /\ phase \in Phases
    /\ removedNewProposal = "none"
    /\ removedNewProposal' = IF RemovedVoter \in Voters3 THEN "accepted" ELSE "rejected"
    /\ UNCHANGED <<phase, currentConf, prepareOK, acceptOK, recordStatus,
                  retryTargets, depsWidth, addedVoterCounted, removedVoterCounted,
                  prepareLossAttempted, acceptLossAttempted,
                  prepareRetryBroadcasted, acceptRetryBroadcasted>>

Next ==
    \/ StartMidRecovery
    \/ FirstPrepareOK
    \/ LostPrepareResponseBeforeRetry
    \/ PrepareRetry
    \/ LostPrepareReplacementOK
    \/ FirstAcceptOK
    \/ LostAcceptResponseBeforeRetry
    \/ AcceptRetry
    \/ LostAcceptReplacementOK
    \/ ExecuteMid
    \/ RemovedAttemptsNewProposal

CurrentConfigIsFinalChain ==
    /\ currentConf = Conf3
    /\ AddedVoter \in Voters3
    /\ RemovedVoter \notin Voters3

LostResponsesDoNotCountBeforeRetry ==
    /\ (prepareLossAttempted /\ ~prepareRetryBroadcasted) =>
        /\ RemovedVoter \notin prepareOK
        /\ Cardinality(prepareOK) = 2
        /\ recordStatus = "preaccepted"
    /\ (acceptLossAttempted /\ ~acceptRetryBroadcasted) =>
        /\ RemovedVoter \notin acceptOK
        /\ Cardinality(acceptOK) = 2
        /\ recordStatus = "accepted_noop"

RetryTargetsPinnedToMidConfig ==
    (prepareRetryBroadcasted \/ acceptRetryBroadcasted) =>
        /\ retryTargets = MidPeers
        /\ retryTargets \subseteq Voters2
        /\ Coordinator \notin retryTargets
        /\ AddedVoter \in retryTargets
        /\ RemovedVoter \in retryTargets
        /\ RemovedVoter \notin Voters3

RetryUsesMidDependencyWidth ==
    (prepareRetryBroadcasted \/ acceptRetryBroadcasted) => depsWidth = Cardinality(Voters2)

RetryDoesNotAdvanceWithoutReplacement ==
    /\ phase = "prepare-retried" =>
        /\ Cardinality(prepareOK) = 2
        /\ recordStatus = "preaccepted"
    /\ phase = "accept-retried" =>
        /\ Cardinality(acceptOK) = 2
        /\ recordStatus = "accepted_noop"

CurrentQuorumCannotAdvanceMidRecovery ==
    Cardinality(prepareOK \cap Voters3) >= CurrentSlowQuorum /\ Cardinality(prepareOK) < MidSlowQuorum =>
        /\ phase \in {"prepare-first-ok", "prepare-lost-before-retry", "prepare-retried"}
        /\ recordStatus = "preaccepted"

CurrentQuorumCannotCommitMidRecovery ==
    Cardinality(acceptOK \cap Voters3) >= CurrentSlowQuorum /\ Cardinality(acceptOK) < MidSlowQuorum =>
        /\ phase \in {"accept-first-ok", "accept-lost-before-retry", "accept-retried"}
        /\ recordStatus = "accepted_noop"

MidQuorumAfterReplacementResponses ==
    recordStatus \in {"committed_noop", "executed_noop"} =>
        /\ Cardinality(prepareOK) = MidSlowQuorum
        /\ Cardinality(acceptOK) = MidSlowQuorum
        /\ AddedVoter \in prepareOK
        /\ AddedVoter \in acceptOK
        /\ RemovedVoter \in prepareOK
        /\ RemovedVoter \in acceptOK

RemovedVoterCannotProposeNewConfigWork ==
    removedNewProposal # "accepted"

Safety ==
    /\ CurrentConfigIsFinalChain
    /\ LostResponsesDoNotCountBeforeRetry
    /\ RetryTargetsPinnedToMidConfig
    /\ RetryUsesMidDependencyWidth
    /\ RetryDoesNotAdvanceWithoutReplacement
    /\ CurrentQuorumCannotAdvanceMidRecovery
    /\ CurrentQuorumCannotCommitMidRecovery
    /\ MidQuorumAfterReplacementResponses
    /\ RemovedVoterCannotProposeNewConfigWork

EventuallyCoversChainLostResponseRetry ==
    <> ( /\ phase = "executed-mid"
         /\ recordStatus = "executed_noop"
         /\ prepareLossAttempted
         /\ acceptLossAttempted
         /\ prepareRetryBroadcasted
         /\ acceptRetryBroadcasted
         /\ addedVoterCounted
         /\ removedVoterCounted
         /\ removedNewProposal = "rejected"
         /\ Cardinality(prepareOK) = MidSlowQuorum
         /\ Cardinality(acceptOK) = MidSlowQuorum )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartMidRecovery)
    /\ WF_Vars(FirstPrepareOK)
    /\ WF_Vars(LostPrepareResponseBeforeRetry)
    /\ WF_Vars(PrepareRetry)
    /\ WF_Vars(LostPrepareReplacementOK)
    /\ WF_Vars(FirstAcceptOK)
    /\ WF_Vars(LostAcceptResponseBeforeRetry)
    /\ WF_Vars(AcceptRetry)
    /\ WF_Vars(LostAcceptReplacementOK)
    /\ WF_Vars(ExecuteMid)
    /\ WF_Vars(RemovedAttemptsNewProposal)

====
