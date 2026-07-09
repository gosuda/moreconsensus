---- MODULE EPaxosConfigAddRecovery ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite recovery-under-configuration-add model. It checks one old  *)
(* instance whose Ref.Conf is the three-voter configuration while the        *)
(* current configuration has already added voter 4. The scenario is staged   *)
(* so the added current-config voter attempts prepare/accept responses, but  *)
(* those attempts leave the old quorum sets unchanged; recovery finishes     *)
(* only with the old 2/3 slow quorum. This does not model joint consensus,   *)
(* arbitrary membership histories, message loss, arbitrary recovery under    *)
(* reconfiguration, or unbounded recovery.                                  *)
(***************************************************************************)
VARIABLES phase, currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
          addedPrepareAttempted, addedAcceptAttempted

Conf1 == 1
Conf2 == 2
AddedVoter == 4
Coordinator == 2

OldVoters == {1, 2, 3}
NewVoters == {1, 2, 3, 4}
AllReplicas == NewVoters
OldSlowQuorum == 2
NewSlowQuorum == 3

Phases == {"current-after-add", "prepare-old", "prepare-old-added-attempted",
           "accept-old", "accept-old-added-attempted", "committed-old", "executed-old"}
Statuses == {"preaccepted", "accepted_noop", "committed_noop", "executed_noop"}
Vars == <<phase, currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
          addedPrepareAttempted, addedAcceptAttempted>>

TypeOK ==
    /\ phase \in Phases
    /\ currentConf \in {Conf1, Conf2}
    /\ oldPrepareOK \subseteq OldVoters
    /\ oldAcceptOK \subseteq OldVoters
    /\ oldRecordStatus \in Statuses
    /\ addedPrepareAttempted \in BOOLEAN
    /\ addedAcceptAttempted \in BOOLEAN

Init ==
    /\ phase = "current-after-add"
    /\ currentConf = Conf2
    /\ oldPrepareOK = {}
    /\ oldAcceptOK = {}
    /\ oldRecordStatus = "preaccepted"
    /\ addedPrepareAttempted = FALSE
    /\ addedAcceptAttempted = FALSE

StartOldRecovery ==
    /\ phase = "current-after-add"
    /\ phase' = "prepare-old"
    /\ oldPrepareOK' = {Coordinator}
    /\ UNCHANGED <<currentConf, oldAcceptOK, oldRecordStatus,
                  addedPrepareAttempted, addedAcceptAttempted>>

AddedPrepareAttempt ==
    /\ phase = "prepare-old"
    /\ phase' = "prepare-old-added-attempted"
    /\ addedPrepareAttempted' = TRUE
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
                  addedAcceptAttempted>>

PrepareOldOK(r) ==
    LET newPrepareOK == oldPrepareOK \cup {r}
    IN
    /\ phase = "prepare-old-added-attempted"
    /\ r \in OldVoters \ oldPrepareOK
    /\ oldPrepareOK' = newPrepareOK
    /\ IF Cardinality(newPrepareOK) >= OldSlowQuorum
       THEN /\ phase' = "accept-old"
            /\ oldRecordStatus' = "accepted_noop"
            /\ oldAcceptOK' = {Coordinator}
       ELSE /\ phase' = phase
            /\ oldRecordStatus' = oldRecordStatus
            /\ oldAcceptOK' = oldAcceptOK
    /\ UNCHANGED <<currentConf, addedPrepareAttempted, addedAcceptAttempted>>

AddedAcceptAttempt ==
    /\ phase = "accept-old"
    /\ phase' = "accept-old-added-attempted"
    /\ addedAcceptAttempted' = TRUE
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
                  addedPrepareAttempted>>

AcceptOldOK(r) ==
    LET newAcceptOK == oldAcceptOK \cup {r}
    IN
    /\ phase = "accept-old-added-attempted"
    /\ r \in OldVoters \ oldAcceptOK
    /\ oldAcceptOK' = newAcceptOK
    /\ IF Cardinality(newAcceptOK) >= OldSlowQuorum
       THEN /\ phase' = "committed-old"
            /\ oldRecordStatus' = "committed_noop"
       ELSE /\ phase' = phase
            /\ oldRecordStatus' = oldRecordStatus
    /\ UNCHANGED <<currentConf, oldPrepareOK, addedPrepareAttempted,
                  addedAcceptAttempted>>

ExecuteOld ==
    /\ phase = "committed-old"
    /\ oldRecordStatus = "committed_noop"
    /\ phase' = "executed-old"
    /\ oldRecordStatus' = "executed_noop"
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldAcceptOK,
                  addedPrepareAttempted, addedAcceptAttempted>>

Next ==
    \/ StartOldRecovery
    \/ AddedPrepareAttempt
    \/ \E r \in OldVoters : PrepareOldOK(r)
    \/ AddedAcceptAttempt
    \/ \E r \in OldVoters : AcceptOldOK(r)
    \/ ExecuteOld

CurrentConfigIncludesAddedVoter ==
    /\ currentConf = Conf2
    /\ AddedVoter \in NewVoters
    /\ AddedVoter \notin OldVoters

OldRecoveryUsesOldQuorum ==
    oldRecordStatus \in {"accepted_noop", "committed_noop", "executed_noop"} =>
        /\ oldPrepareOK \subseteq OldVoters
        /\ AddedVoter \notin oldPrepareOK
        /\ Cardinality(oldPrepareOK) >= OldSlowQuorum

OldCommitUsesOldQuorum ==
    oldRecordStatus \in {"committed_noop", "executed_noop"} =>
        /\ oldAcceptOK \subseteq OldVoters
        /\ AddedVoter \notin oldAcceptOK
        /\ Cardinality(oldAcceptOK) >= OldSlowQuorum

NewQuorumNotRequiredForOldThreeVoterInstance ==
    oldRecordStatus \in {"committed_noop", "executed_noop"} =>
        Cardinality(oldAcceptOK) < NewSlowQuorum

AddedVoterAttemptsDoNotCount ==
    /\ addedPrepareAttempted => AddedVoter \notin oldPrepareOK
    /\ addedAcceptAttempted => AddedVoter \notin oldAcceptOK

Safety ==
    /\ CurrentConfigIncludesAddedVoter
    /\ OldRecoveryUsesOldQuorum
    /\ OldCommitUsesOldQuorum
    /\ NewQuorumNotRequiredForOldThreeVoterInstance
    /\ AddedVoterAttemptsDoNotCount

EventuallyCoversConfigAddRecovery ==
    <> ( /\ phase = "executed-old"
         /\ oldRecordStatus = "executed_noop"
         /\ Cardinality(oldPrepareOK) = OldSlowQuorum
         /\ Cardinality(oldAcceptOK) = OldSlowQuorum
         /\ addedPrepareAttempted
         /\ addedAcceptAttempted )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartOldRecovery)
    /\ WF_Vars(AddedPrepareAttempt)
    /\ \A r \in OldVoters : WF_Vars(PrepareOldOK(r))
    /\ WF_Vars(AddedAcceptAttempt)
    /\ \A r \in OldVoters : WF_Vars(AcceptOldOK(r))
    /\ WF_Vars(ExecuteOld)

====
