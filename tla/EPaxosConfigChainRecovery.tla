---- MODULE EPaxosConfigChainRecovery ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite recovery-under-configuration-chain model. It checks one   *)
(* mid-chain instance whose Ref.Conf is the four-voter configuration created*)
(* by an add-voter transition, while the current configuration has already  *)
(* removed voter 2. Recovery must keep the mid-chain instance pinned to     *)
(* Conf2: voter 4, which was added by the first transition, and voter 2,    *)
(* which was removed by the second transition, both count for that old      *)
(* instance's prepare/accept quorums. Removed voter 2 still cannot propose  *)
(* new current-config work. This does not model joint consensus, arbitrary  *)
(* membership histories, retries, message loss, or unbounded recovery.      *)
(***************************************************************************)

VARIABLES phase, currentConf, midPrepareOK, midAcceptOK, midRecordStatus,
          addedVoterCounted, removedVoterCounted, removedNewProposal

Conf1 == 1
Conf2 == 2
Conf3 == 3
AddedVoter == 4
RemovedVoter == 2
Coordinator == 3

Voters1 == {1, 2, 3}
Voters2 == Voters1 \cup {AddedVoter}
Voters3 == Voters2 \ {RemovedVoter}
AllReplicas == Voters2
MidSlowQuorum == 3
CurrentSlowQuorum == 2

Phases == {"current-after-chain", "prepare-mid", "accept-mid", "committed-mid", "executed-mid"}
Statuses == {"preaccepted", "accepted_noop", "committed_noop", "executed_noop"}
ProposalStatuses == {"none", "rejected", "accepted"}
Vars == <<phase, currentConf, midPrepareOK, midAcceptOK, midRecordStatus,
          addedVoterCounted, removedVoterCounted, removedNewProposal>>

TypeOK ==
    /\ phase \in Phases
    /\ currentConf \in {Conf1, Conf2, Conf3}
    /\ midPrepareOK \subseteq AllReplicas
    /\ midAcceptOK \subseteq AllReplicas
    /\ midRecordStatus \in Statuses
    /\ addedVoterCounted \in BOOLEAN
    /\ removedVoterCounted \in BOOLEAN
    /\ removedNewProposal \in ProposalStatuses

Init ==
    /\ phase = "current-after-chain"
    /\ currentConf = Conf3
    /\ midPrepareOK = {}
    /\ midAcceptOK = {}
    /\ midRecordStatus = "preaccepted"
    /\ addedVoterCounted = FALSE
    /\ removedVoterCounted = FALSE
    /\ removedNewProposal = "none"

StartMidRecovery ==
    /\ phase = "current-after-chain"
    /\ phase' = "prepare-mid"
    /\ midPrepareOK' = {Coordinator}
    /\ UNCHANGED <<currentConf, midAcceptOK, midRecordStatus,
                  addedVoterCounted, removedVoterCounted, removedNewProposal>>

PrepareMidOK(r) ==
    LET newPrepareOK == midPrepareOK \cup {r}
    IN
    /\ phase = "prepare-mid"
    /\ r \in Voters2 \ midPrepareOK
    /\ IF Cardinality(midPrepareOK) = 1 THEN r = AddedVoter
       ELSE IF Cardinality(midPrepareOK) = 2 THEN r = RemovedVoter
       ELSE TRUE
    /\ midPrepareOK' = newPrepareOK
    /\ addedVoterCounted' = (addedVoterCounted \/ (r = AddedVoter))
    /\ removedVoterCounted' = (removedVoterCounted \/ (r = RemovedVoter))
    /\ IF Cardinality(newPrepareOK) >= MidSlowQuorum
       THEN /\ phase' = "accept-mid"
            /\ midRecordStatus' = "accepted_noop"
            /\ midAcceptOK' = {Coordinator}
       ELSE /\ phase' = phase
            /\ midRecordStatus' = midRecordStatus
            /\ midAcceptOK' = midAcceptOK
    /\ UNCHANGED <<currentConf, removedNewProposal>>

AcceptMidOK(r) ==
    LET newAcceptOK == midAcceptOK \cup {r}
    IN
    /\ phase = "accept-mid"
    /\ r \in Voters2 \ midAcceptOK
    /\ IF Cardinality(midAcceptOK) = 1 THEN r = AddedVoter
       ELSE IF Cardinality(midAcceptOK) = 2 THEN r = RemovedVoter
       ELSE TRUE
    /\ midAcceptOK' = newAcceptOK
    /\ addedVoterCounted' = (addedVoterCounted \/ (r = AddedVoter))
    /\ removedVoterCounted' = (removedVoterCounted \/ (r = RemovedVoter))
    /\ IF Cardinality(newAcceptOK) >= MidSlowQuorum
       THEN /\ phase' = "committed-mid"
            /\ midRecordStatus' = "committed_noop"
       ELSE /\ phase' = phase
            /\ midRecordStatus' = midRecordStatus
    /\ UNCHANGED <<currentConf, midPrepareOK, removedNewProposal>>

ExecuteMid ==
    /\ phase = "committed-mid"
    /\ midRecordStatus = "committed_noop"
    /\ phase' = "executed-mid"
    /\ midRecordStatus' = "executed_noop"
    /\ UNCHANGED <<currentConf, midPrepareOK, midAcceptOK,
                  addedVoterCounted, removedVoterCounted, removedNewProposal>>

RemovedAttemptsNewProposal ==
    /\ phase \in Phases
    /\ removedNewProposal = "none"
    /\ removedNewProposal' = IF RemovedVoter \in Voters3 THEN "accepted" ELSE "rejected"
    /\ UNCHANGED <<phase, currentConf, midPrepareOK, midAcceptOK,
                  midRecordStatus, addedVoterCounted, removedVoterCounted>>

Next ==
    \/ StartMidRecovery
    \/ \E r \in Voters2 : PrepareMidOK(r)
    \/ \E r \in Voters2 : AcceptMidOK(r)
    \/ ExecuteMid
    \/ RemovedAttemptsNewProposal

CurrentConfigIsFinalChain ==
    /\ currentConf = Conf3
    /\ AddedVoter \in Voters3
    /\ RemovedVoter \notin Voters3

MidRecoveryUsesPinnedChainQuorum ==
    midRecordStatus \in {"accepted_noop", "committed_noop", "executed_noop"} =>
        /\ midPrepareOK \subseteq Voters2
        /\ AddedVoter \in midPrepareOK
        /\ RemovedVoter \in midPrepareOK
        /\ Cardinality(midPrepareOK) >= MidSlowQuorum

MidCommitUsesPinnedChainQuorum ==
    midRecordStatus \in {"committed_noop", "executed_noop"} =>
        /\ midAcceptOK \subseteq Voters2
        /\ AddedVoter \in midAcceptOK
        /\ RemovedVoter \in midAcceptOK
        /\ Cardinality(midAcceptOK) >= MidSlowQuorum

CurrentQuorumWouldBeInsufficientForMidInstance ==
    midRecordStatus \in {"committed_noop", "executed_noop"} =>
        Cardinality(midAcceptOK) > CurrentSlowQuorum


CurrentPrepareQuorumCannotAdvanceMidRecovery ==
    Cardinality(midPrepareOK \cap Voters3) >= CurrentSlowQuorum /\ Cardinality(midPrepareOK) < MidSlowQuorum =>
        /\ phase = "prepare-mid"
        /\ midRecordStatus = "preaccepted"

CurrentAcceptQuorumCannotCommitMidRecovery ==
    Cardinality(midAcceptOK \cap Voters3) >= CurrentSlowQuorum /\ Cardinality(midAcceptOK) < MidSlowQuorum =>
        /\ phase = "accept-mid"
        /\ midRecordStatus = "accepted_noop"

ChainVotersCountOnlyForPinnedMidInstance ==
    /\ addedVoterCounted => AddedVoter \in Voters2
    /\ removedVoterCounted => /\ RemovedVoter \in Voters2
                              /\ RemovedVoter \notin Voters3

RemovedVoterCannotProposeNewConfigWork ==
    removedNewProposal # "accepted"

Safety ==
    /\ CurrentConfigIsFinalChain
    /\ MidRecoveryUsesPinnedChainQuorum
    /\ MidCommitUsesPinnedChainQuorum
    /\ CurrentQuorumWouldBeInsufficientForMidInstance
    /\ CurrentPrepareQuorumCannotAdvanceMidRecovery
    /\ CurrentAcceptQuorumCannotCommitMidRecovery
    /\ ChainVotersCountOnlyForPinnedMidInstance
    /\ RemovedVoterCannotProposeNewConfigWork

EventuallyCoversConfigChainRecovery ==
    <> ( /\ phase = "executed-mid"
         /\ midRecordStatus = "executed_noop"
         /\ addedVoterCounted
         /\ removedVoterCounted
         /\ removedNewProposal = "rejected" )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartMidRecovery)
    /\ \A r \in Voters2 : WF_Vars(PrepareMidOK(r))
    /\ \A r \in Voters2 : WF_Vars(AcceptMidOK(r))
    /\ WF_Vars(ExecuteMid)
    /\ WF_Vars(RemovedAttemptsNewProposal)

====
