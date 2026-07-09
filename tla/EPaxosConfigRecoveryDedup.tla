---- MODULE EPaxosConfigRecoveryDedup ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite old-config recovery de-duplication model. It checks one   *)
(* old instance whose Ref.Conf is the four-voter configuration while the    *)
(* current configuration has already removed voter 4. The scenario is       *)
(* staged so a lost response and a duplicate response do not advance the    *)
(* old prepare/accept quorum; recovery finishes only after a distinct old   *)
(* voter response, including removed voter 4 for the old pinned instance.   *)
(* This does not model retries, joint consensus, arbitrary membership       *)
(* histories, arbitrary message loss, or unbounded recovery.                *)
(***************************************************************************)
VARIABLES phase, currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
          prepareLossAttempted, acceptLossAttempted,
          duplicatePrepareAttempted, duplicateAcceptAttempted

Conf1 == 1
Conf2 == 2
Coordinator == 2
FirstResponder == 3
RemovedVoter == 4
LostVoter == 1

OldVoters == {1, 2, 3, 4}
NewVoters == {1, 2, 3}
OldSlowQuorum == 3
NewSlowQuorum == 2

Phases == {"current-after-removal", "prepare-old", "prepare-old-first-ok",
           "prepare-old-loss-attempted", "prepare-old-duplicate-attempted",
           "accept-old", "accept-old-first-ok", "accept-old-loss-attempted",
           "accept-old-duplicate-attempted", "committed-old", "executed-old"}
Statuses == {"preaccepted", "accepted_noop", "committed_noop", "executed_noop"}
Vars == <<phase, currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
          prepareLossAttempted, acceptLossAttempted,
          duplicatePrepareAttempted, duplicateAcceptAttempted>>

TypeOK ==
    /\ phase \in Phases
    /\ currentConf \in {Conf1, Conf2}
    /\ oldPrepareOK \subseteq OldVoters
    /\ oldAcceptOK \subseteq OldVoters
    /\ oldRecordStatus \in Statuses
    /\ prepareLossAttempted \in BOOLEAN
    /\ acceptLossAttempted \in BOOLEAN
    /\ duplicatePrepareAttempted \in BOOLEAN
    /\ duplicateAcceptAttempted \in BOOLEAN

Init ==
    /\ phase = "current-after-removal"
    /\ currentConf = Conf2
    /\ oldPrepareOK = {}
    /\ oldAcceptOK = {}
    /\ oldRecordStatus = "preaccepted"
    /\ prepareLossAttempted = FALSE
    /\ acceptLossAttempted = FALSE
    /\ duplicatePrepareAttempted = FALSE
    /\ duplicateAcceptAttempted = FALSE

StartOldRecovery ==
    /\ phase = "current-after-removal"
    /\ phase' = "prepare-old"
    /\ oldPrepareOK' = {Coordinator}
    /\ UNCHANGED <<currentConf, oldAcceptOK, oldRecordStatus,
                  prepareLossAttempted, acceptLossAttempted,
                  duplicatePrepareAttempted, duplicateAcceptAttempted>>

FirstPrepareOK ==
    /\ phase = "prepare-old"
    /\ oldPrepareOK' = oldPrepareOK \cup {FirstResponder}
    /\ Cardinality(oldPrepareOK') = 2
    /\ phase' = "prepare-old-first-ok"
    /\ UNCHANGED <<currentConf, oldAcceptOK, oldRecordStatus,
                  prepareLossAttempted, acceptLossAttempted,
                  duplicatePrepareAttempted, duplicateAcceptAttempted>>

LostPrepareAttempt ==
    /\ phase = "prepare-old-first-ok"
    /\ prepareLossAttempted' = TRUE
    /\ phase' = "prepare-old-loss-attempted"
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
                  acceptLossAttempted, duplicatePrepareAttempted,
                  duplicateAcceptAttempted>>

DuplicatePrepareOK ==
    /\ phase = "prepare-old-loss-attempted"
    /\ FirstResponder \in oldPrepareOK
    /\ oldPrepareOK' = oldPrepareOK
    /\ duplicatePrepareAttempted' = TRUE
    /\ phase' = "prepare-old-duplicate-attempted"
    /\ UNCHANGED <<currentConf, oldAcceptOK, oldRecordStatus,
                  prepareLossAttempted, acceptLossAttempted,
                  duplicateAcceptAttempted>>

RemovedPrepareOK ==
    /\ phase = "prepare-old-duplicate-attempted"
    /\ oldPrepareOK' = oldPrepareOK \cup {RemovedVoter}
    /\ Cardinality(oldPrepareOK') = OldSlowQuorum
    /\ phase' = "accept-old"
    /\ oldRecordStatus' = "accepted_noop"
    /\ oldAcceptOK' = {Coordinator}
    /\ UNCHANGED <<currentConf, prepareLossAttempted, acceptLossAttempted,
                  duplicatePrepareAttempted, duplicateAcceptAttempted>>

FirstAcceptOK ==
    /\ phase = "accept-old"
    /\ oldAcceptOK' = oldAcceptOK \cup {FirstResponder}
    /\ Cardinality(oldAcceptOK') = 2
    /\ phase' = "accept-old-first-ok"
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldRecordStatus,
                  prepareLossAttempted, acceptLossAttempted,
                  duplicatePrepareAttempted, duplicateAcceptAttempted>>

LostAcceptAttempt ==
    /\ phase = "accept-old-first-ok"
    /\ acceptLossAttempted' = TRUE
    /\ phase' = "accept-old-loss-attempted"
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldAcceptOK, oldRecordStatus,
                  prepareLossAttempted, duplicatePrepareAttempted,
                  duplicateAcceptAttempted>>

DuplicateAcceptOK ==
    /\ phase = "accept-old-loss-attempted"
    /\ FirstResponder \in oldAcceptOK
    /\ oldAcceptOK' = oldAcceptOK
    /\ duplicateAcceptAttempted' = TRUE
    /\ phase' = "accept-old-duplicate-attempted"
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldRecordStatus,
                  prepareLossAttempted, acceptLossAttempted,
                  duplicatePrepareAttempted>>

RemovedAcceptOK ==
    /\ phase = "accept-old-duplicate-attempted"
    /\ oldAcceptOK' = oldAcceptOK \cup {RemovedVoter}
    /\ Cardinality(oldAcceptOK') = OldSlowQuorum
    /\ phase' = "committed-old"
    /\ oldRecordStatus' = "committed_noop"
    /\ UNCHANGED <<currentConf, oldPrepareOK, prepareLossAttempted,
                  acceptLossAttempted, duplicatePrepareAttempted,
                  duplicateAcceptAttempted>>

ExecuteOld ==
    /\ phase = "committed-old"
    /\ oldRecordStatus = "committed_noop"
    /\ phase' = "executed-old"
    /\ oldRecordStatus' = "executed_noop"
    /\ UNCHANGED <<currentConf, oldPrepareOK, oldAcceptOK,
                  prepareLossAttempted, acceptLossAttempted,
                  duplicatePrepareAttempted, duplicateAcceptAttempted>>

Next ==
    \/ StartOldRecovery
    \/ FirstPrepareOK
    \/ LostPrepareAttempt
    \/ DuplicatePrepareOK
    \/ RemovedPrepareOK
    \/ FirstAcceptOK
    \/ LostAcceptAttempt
    \/ DuplicateAcceptOK
    \/ RemovedAcceptOK
    \/ ExecuteOld

CurrentConfigExcludesRemovedVoter ==
    /\ currentConf = Conf2
    /\ RemovedVoter \notin NewVoters

LostResponsesDoNotCount ==
    /\ prepareLossAttempted => LostVoter \notin oldPrepareOK
    /\ acceptLossAttempted => LostVoter \notin oldAcceptOK

DuplicatePrepareDoesNotCount ==
    phase = "prepare-old-duplicate-attempted" =>
        /\ oldRecordStatus = "preaccepted"
        /\ Cardinality(oldPrepareOK) = 2
        /\ Cardinality(oldPrepareOK) < OldSlowQuorum

DuplicateAcceptDoesNotCount ==
    phase = "accept-old-duplicate-attempted" =>
        /\ oldRecordStatus = "accepted_noop"
        /\ Cardinality(oldAcceptOK) = 2
        /\ Cardinality(oldAcceptOK) < OldSlowQuorum

OldCommitUsesOldQuorum ==
    oldRecordStatus \in {"committed_noop", "executed_noop"} =>
        /\ oldPrepareOK \subseteq OldVoters
        /\ oldAcceptOK \subseteq OldVoters
        /\ Cardinality(oldPrepareOK) = OldSlowQuorum
        /\ Cardinality(oldAcceptOK) = OldSlowQuorum
        /\ RemovedVoter \in oldPrepareOK
        /\ RemovedVoter \in oldAcceptOK

NewQuorumWouldBeInsufficientForOldFourVoterInstance ==
    oldRecordStatus \in {"committed_noop", "executed_noop"} =>
        Cardinality(oldAcceptOK) > NewSlowQuorum

Safety ==
    /\ CurrentConfigExcludesRemovedVoter
    /\ LostResponsesDoNotCount
    /\ DuplicatePrepareDoesNotCount
    /\ DuplicateAcceptDoesNotCount
    /\ OldCommitUsesOldQuorum
    /\ NewQuorumWouldBeInsufficientForOldFourVoterInstance

EventuallyCoversConfigRecoveryDedup ==
    <> ( /\ phase = "executed-old"
         /\ oldRecordStatus = "executed_noop"
         /\ prepareLossAttempted
         /\ acceptLossAttempted
         /\ duplicatePrepareAttempted
         /\ duplicateAcceptAttempted
         /\ LostVoter \notin oldPrepareOK
         /\ LostVoter \notin oldAcceptOK
         /\ Cardinality(oldPrepareOK) = OldSlowQuorum
         /\ Cardinality(oldAcceptOK) = OldSlowQuorum )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartOldRecovery)
    /\ WF_Vars(FirstPrepareOK)
    /\ WF_Vars(LostPrepareAttempt)
    /\ WF_Vars(DuplicatePrepareOK)
    /\ WF_Vars(RemovedPrepareOK)
    /\ WF_Vars(FirstAcceptOK)
    /\ WF_Vars(LostAcceptAttempt)
    /\ WF_Vars(DuplicateAcceptOK)
    /\ WF_Vars(RemovedAcceptOK)
    /\ WF_Vars(ExecuteOld)

====
