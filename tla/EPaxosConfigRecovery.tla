---- MODULE EPaxosConfigRecovery ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite recovery-under-configuration-change model. It checks one  *)
(* old instance whose Ref.Conf is the four-voter configuration while the    *)
(* current configuration has already removed voter 4. The scenario is       *)
(* intentionally staged so voter 4 answers old-instance prepare/accept      *)
(* before quorum, proving a removed current-config voter may still count    *)
(* for an old pinned instance. The removed voter still cannot propose new   *)
(* current-config work. This does not model joint consensus, arbitrary      *)
(* membership histories, message loss, or unbounded recovery.               *)
(***************************************************************************)
VARIABLES phase, currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
          removedVoterCounted, removedNewProposal

Conf1 == 1
Conf2 == 2
RemovedVoter == 4
Coordinator == 2

OldVoters == {1, 2, 3, 4}
NewVoters == {1, 2, 3}
AllReplicas == OldVoters
OldSlowQuorum == 3
NewSlowQuorum == 2

Phases == {"current-after-removal", "prepare-old", "accept-old", "committed-old", "executed-old"}
Statuses == {"preaccepted", "accepted_noop", "committed_noop", "executed_noop"}
ProposalStatuses == {"none", "rejected", "accepted"}
Vars == <<phase, currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
          removedVoterCounted, removedNewProposal>>

TypeOK ==
    /\ phase \in Phases
    /\ currentConf \in {Conf1, Conf2}
    /\ oldPrepareOK \subseteq OldVoters
    /\ oldAcceptOK \subseteq OldVoters
    /\ oldRecordStatus \in Statuses
    /\ removedVoterCounted \in BOOLEAN
    /\ removedNewProposal \in ProposalStatuses

Init ==
    /\ phase = "current-after-removal"
    /\ currentConf = Conf2
    /\ oldPrepareOK = {}
    /\ oldAcceptOK = {}
    /\ oldRecordStatus = "preaccepted"
    /\ removedVoterCounted = FALSE
    /\ removedNewProposal = "none"

StartOldRecovery ==
    /\ phase = "current-after-removal"
    /\ phase' = "prepare-old"
    /\ oldPrepareOK' = {Coordinator}
    /\ UNCHANGED <<currentConf, oldAcceptOK, oldRecordStatus,
                  removedVoterCounted, removedNewProposal>>

PrepareOldOK(r) ==
    LET newPrepareOK == oldPrepareOK \cup {r}
    IN
    /\ phase = "prepare-old"
    /\ r \in OldVoters \ oldPrepareOK
    /\ IF Cardinality(oldPrepareOK) = 1 THEN r = RemovedVoter ELSE TRUE
    /\ oldPrepareOK' = newPrepareOK
    /\ removedVoterCounted' = (removedVoterCounted \/ (r = RemovedVoter))
    /\ IF Cardinality(newPrepareOK) >= OldSlowQuorum
       THEN /\ phase' = "accept-old"
            /\ oldRecordStatus' = "accepted_noop"
            /\ oldAcceptOK' = {Coordinator}
       ELSE /\ phase' = phase
            /\ oldRecordStatus' = oldRecordStatus
            /\ oldAcceptOK' = oldAcceptOK
    /\ UNCHANGED <<currentConf, removedNewProposal>>

AcceptOldOK(r) ==
    LET newAcceptOK == oldAcceptOK \cup {r}
    IN
    /\ phase = "accept-old"
    /\ r \in OldVoters \ oldAcceptOK
    /\ IF Cardinality(oldAcceptOK) = 1 THEN r = RemovedVoter ELSE TRUE
    /\ oldAcceptOK' = newAcceptOK
    /\ removedVoterCounted' = (removedVoterCounted \/ (r = RemovedVoter))
    /\ IF Cardinality(newAcceptOK) >= OldSlowQuorum
       THEN /\ phase' = "committed-old"
            /\ oldRecordStatus' = "committed_noop"
       ELSE /\ phase' = phase
            /\ oldRecordStatus' = oldRecordStatus
    /\ UNCHANGED <<currentConf, oldPrepareOK, removedNewProposal>>

ExecuteOld ==
    /\ phase = "committed-old"
    /\ oldRecordStatus = "committed_noop"
    /\ phase' = "executed-old"
    /\ oldRecordStatus' = "executed_noop"
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldAcceptOK,
                  removedVoterCounted, removedNewProposal>>

RemovedAttemptsNewProposal ==
    /\ phase \in {"current-after-removal", "prepare-old", "accept-old", "committed-old", "executed-old"}
    /\ removedNewProposal = "none"
    /\ removedNewProposal' = IF RemovedVoter \in NewVoters THEN "accepted" ELSE "rejected"
    /\ UNCHANGED <<phase, currentConf, oldPrepareOK, oldAcceptOK,
                  oldRecordStatus, removedVoterCounted>>

Next ==
    \/ StartOldRecovery
    \/ \E r \in OldVoters : PrepareOldOK(r)
    \/ \E r \in OldVoters : AcceptOldOK(r)
    \/ ExecuteOld
    \/ RemovedAttemptsNewProposal

CurrentConfigExcludesRemovedVoter ==
    /\ currentConf = Conf2
    /\ RemovedVoter \notin NewVoters

OldRecoveryUsesOldQuorum ==
    oldRecordStatus \in {"accepted_noop", "committed_noop", "executed_noop"} =>
        /\ oldPrepareOK \subseteq OldVoters
        /\ Cardinality(oldPrepareOK) >= OldSlowQuorum

OldCommitUsesOldQuorum ==
    oldRecordStatus \in {"committed_noop", "executed_noop"} =>
        /\ oldAcceptOK \subseteq OldVoters
        /\ Cardinality(oldAcceptOK) >= OldSlowQuorum

NewQuorumWouldBeInsufficientForOldFourVoterInstance ==
    oldRecordStatus \in {"committed_noop", "executed_noop"} =>
        Cardinality(oldAcceptOK) > NewSlowQuorum

RemovedVoterMayCountOnlyForOldInstance ==
    removedVoterCounted => RemovedVoter \in OldVoters /\ RemovedVoter \notin NewVoters

RemovedVoterCannotProposeNewConfigWork ==
    removedNewProposal # "accepted"

Safety ==
    /\ CurrentConfigExcludesRemovedVoter
    /\ OldRecoveryUsesOldQuorum
    /\ OldCommitUsesOldQuorum
    /\ NewQuorumWouldBeInsufficientForOldFourVoterInstance
    /\ RemovedVoterMayCountOnlyForOldInstance
    /\ RemovedVoterCannotProposeNewConfigWork

EventuallyCoversConfigRecovery ==
    <> ( /\ phase = "executed-old"
         /\ oldRecordStatus = "executed_noop"
         /\ removedVoterCounted
         /\ removedNewProposal = "rejected" )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartOldRecovery)
    /\ \A r \in OldVoters : WF_Vars(PrepareOldOK(r))
    /\ \A r \in OldVoters : WF_Vars(AcceptOldOK(r))
    /\ WF_Vars(ExecuteOld)
    /\ WF_Vars(RemovedAttemptsNewProposal)

====
