----------------------------- MODULE EPaxosTryPreAcceptBranches -----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS ReplicaCount

VARIABLES scenario, stage, phase, restartedPrepare, evidenceStarted, ignoreResent,
          failClosed, accepted, dependencyAdded, deferred, recoveryStarted,
          duplicateRecoveryCount, okVotes

SlowQuorum == (ReplicaCount \div 2) + 1

Scenarios == {
    "stale_restarts_prepare",
    "committed_conflict_evidence_authorizes_ignore",
    "committed_conflict_evidence_fail_closed",
    "committed_conflict_direct_accept",
    "uncommitted_conflict_forced_accept",
    "uncommitted_conflict_defer_once",
    "ok_quorum_accept"
}

Stages == {"start", "evidence", "deferred", "done"}
Phases == {"try_pre_accept", "prepare", "accept"}

Vars == <<scenario, stage, phase, restartedPrepare, evidenceStarted, ignoreResent,
          failClosed, accepted, dependencyAdded, deferred, recoveryStarted,
          duplicateRecoveryCount, okVotes>>

TypeOK ==
    /\ ReplicaCount \in 1..7
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ phase \in Phases
    /\ restartedPrepare \in BOOLEAN
    /\ evidenceStarted \in BOOLEAN
    /\ ignoreResent \in BOOLEAN
    /\ failClosed \in BOOLEAN
    /\ accepted \in BOOLEAN
    /\ dependencyAdded \in BOOLEAN
    /\ deferred \in BOOLEAN
    /\ recoveryStarted \in BOOLEAN
    /\ duplicateRecoveryCount \in 0..1
    /\ okVotes \in 0..ReplicaCount

Init ==
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ phase = "try_pre_accept"
    /\ restartedPrepare = FALSE
    /\ evidenceStarted = FALSE
    /\ ignoreResent = FALSE
    /\ failClosed = FALSE
    /\ accepted = FALSE
    /\ dependencyAdded = FALSE
    /\ deferred = FALSE
    /\ recoveryStarted = FALSE
    /\ duplicateRecoveryCount = 0
    /\ okVotes = 0

StaleRejectRestartsPrepare ==
    /\ scenario = "stale_restarts_prepare"
    /\ stage = "start"
    /\ stage' = "done"
    /\ phase' = "prepare"
    /\ restartedPrepare' = TRUE
    /\ UNCHANGED <<scenario, evidenceStarted, ignoreResent, failClosed, accepted,
                  dependencyAdded, deferred, recoveryStarted, duplicateRecoveryCount, okVotes>>

StartCommittedEvidenceCheck ==
    /\ scenario \in {"committed_conflict_evidence_authorizes_ignore", "committed_conflict_evidence_fail_closed"}
    /\ stage = "start"
    /\ stage' = "evidence"
    /\ evidenceStarted' = TRUE
    /\ UNCHANGED <<scenario, phase, restartedPrepare, ignoreResent, failClosed,
                  accepted, dependencyAdded, deferred, recoveryStarted,
                  duplicateRecoveryCount, okVotes>>

EvidenceAuthorizesIgnoreResend ==
    /\ scenario = "committed_conflict_evidence_authorizes_ignore"
    /\ stage = "evidence"
    /\ stage' = "done"
    /\ ignoreResent' = TRUE
    /\ UNCHANGED <<scenario, phase, restartedPrepare, evidenceStarted, failClosed,
                  accepted, dependencyAdded, deferred, recoveryStarted,
                  duplicateRecoveryCount, okVotes>>

EvidenceFailsClosedToAccept ==
    /\ scenario = "committed_conflict_evidence_fail_closed"
    /\ stage = "evidence"
    /\ stage' = "done"
    /\ phase' = "accept"
    /\ failClosed' = TRUE
    /\ accepted' = TRUE
    /\ dependencyAdded' = TRUE
    /\ UNCHANGED <<scenario, restartedPrepare, evidenceStarted, ignoreResent,
                  deferred, recoveryStarted, duplicateRecoveryCount, okVotes>>

CommittedConflictDirectAccept ==
    /\ scenario = "committed_conflict_direct_accept"
    /\ stage = "start"
    /\ stage' = "done"
    /\ phase' = "accept"
    /\ accepted' = TRUE
    /\ dependencyAdded' = TRUE
    /\ UNCHANGED <<scenario, restartedPrepare, evidenceStarted, ignoreResent,
                  failClosed, deferred, recoveryStarted, duplicateRecoveryCount, okVotes>>

UncommittedConflictForcedAccept ==
    /\ scenario = "uncommitted_conflict_forced_accept"
    /\ stage = "start"
    /\ stage' = "done"
    /\ phase' = "accept"
    /\ accepted' = TRUE
    /\ dependencyAdded' = TRUE
    /\ UNCHANGED <<scenario, restartedPrepare, evidenceStarted, ignoreResent,
                  failClosed, deferred, recoveryStarted, duplicateRecoveryCount, okVotes>>

UncommittedConflictDefers ==
    /\ scenario = "uncommitted_conflict_defer_once"
    /\ stage = "start"
    /\ stage' = "deferred"
    /\ deferred' = TRUE
    /\ recoveryStarted' = TRUE
    /\ duplicateRecoveryCount' = 1
    /\ UNCHANGED <<scenario, phase, restartedPrepare, evidenceStarted, ignoreResent,
                  failClosed, accepted, dependencyAdded, okVotes>>

DuplicateUncommittedConflictDoesNotRestartRecovery ==
    /\ scenario = "uncommitted_conflict_defer_once"
    /\ stage = "deferred"
    /\ stage' = "done"
    /\ duplicateRecoveryCount' = 1
    /\ UNCHANGED <<scenario, phase, restartedPrepare, evidenceStarted, ignoreResent,
                  failClosed, accepted, dependencyAdded, deferred, recoveryStarted,
                  okVotes>>

OKSlowQuorumAccepts ==
    /\ scenario = "ok_quorum_accept"
    /\ stage = "start"
    /\ stage' = "done"
    /\ phase' = "accept"
    /\ okVotes' = SlowQuorum
    /\ accepted' = TRUE
    /\ UNCHANGED <<scenario, restartedPrepare, evidenceStarted, ignoreResent,
                  failClosed, dependencyAdded, deferred, recoveryStarted,
                  duplicateRecoveryCount>>

Next ==
    \/ StaleRejectRestartsPrepare
    \/ StartCommittedEvidenceCheck
    \/ EvidenceAuthorizesIgnoreResend
    \/ EvidenceFailsClosedToAccept
    \/ CommittedConflictDirectAccept
    \/ UncommittedConflictForcedAccept
    \/ UncommittedConflictDefers
    \/ DuplicateUncommittedConflictDoesNotRestartRecovery
    \/ OKSlowQuorumAccepts

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

StaleOnlyRestartsPrepare ==
    scenario = "stale_restarts_prepare" /\ stage = "done" =>
        /\ phase = "prepare"
        /\ restartedPrepare
        /\ ~accepted
        /\ ~deferred
        /\ ~ignoreResent

IgnoreRequiresAuthorizedEvidence ==
    ignoreResent =>
        /\ scenario = "committed_conflict_evidence_authorizes_ignore"
        /\ evidenceStarted
        /\ ~failClosed
        /\ ~accepted
        /\ ~dependencyAdded
        /\ phase = "try_pre_accept"

FailClosedAcceptsWithDependency ==
    failClosed =>
        /\ scenario = "committed_conflict_evidence_fail_closed"
        /\ evidenceStarted
        /\ accepted
        /\ dependencyAdded
        /\ ~ignoreResent
        /\ phase = "accept"

DirectCommittedConflictAcceptsWithDependency ==
    scenario = "committed_conflict_direct_accept" /\ stage = "done" =>
        /\ accepted
        /\ dependencyAdded
        /\ ~ignoreResent
        /\ ~evidenceStarted
        /\ phase = "accept"

UncommittedForcedAcceptDoesNotDefer ==
    scenario = "uncommitted_conflict_forced_accept" /\ stage = "done" =>
        /\ accepted
        /\ dependencyAdded
        /\ ~deferred
        /\ ~recoveryStarted
        /\ phase = "accept"

UncommittedDeferralStartsRecoveryOnce ==
    scenario = "uncommitted_conflict_defer_once" /\ stage \in {"deferred", "done"} =>
        /\ deferred
        /\ recoveryStarted
        /\ duplicateRecoveryCount = 1
        /\ ~accepted
        /\ ~dependencyAdded
        /\ phase = "try_pre_accept"

OKAcceptRequiresSlowQuorum ==
    scenario = "ok_quorum_accept" /\ stage = "done" =>
        /\ accepted
        /\ okVotes >= SlowQuorum
        /\ ~dependencyAdded
        /\ phase = "accept"

AcceptHasBranchJustification ==
    accepted =>
        \/ /\ scenario \in {"committed_conflict_evidence_fail_closed", "committed_conflict_direct_accept", "uncommitted_conflict_forced_accept"}
           /\ dependencyAdded
        \/ /\ scenario = "ok_quorum_accept"
           /\ okVotes >= SlowQuorum

Safety ==
    /\ StaleOnlyRestartsPrepare
    /\ IgnoreRequiresAuthorizedEvidence
    /\ FailClosedAcceptsWithDependency
    /\ DirectCommittedConflictAcceptsWithDependency
    /\ UncommittedForcedAcceptDoesNotDefer
    /\ UncommittedDeferralStartsRecoveryOnce
    /\ OKAcceptRequiresSlowQuorum
    /\ AcceptHasBranchJustification

BranchCovered ==
    CASE scenario = "stale_restarts_prepare" -> stage = "done" /\ restartedPrepare /\ phase = "prepare"
      [] scenario = "committed_conflict_evidence_authorizes_ignore" -> stage = "done" /\ evidenceStarted /\ ignoreResent /\ ~accepted
      [] scenario = "committed_conflict_evidence_fail_closed" -> stage = "done" /\ evidenceStarted /\ failClosed /\ accepted /\ dependencyAdded
      [] scenario = "committed_conflict_direct_accept" -> stage = "done" /\ accepted /\ dependencyAdded /\ ~evidenceStarted
      [] scenario = "uncommitted_conflict_forced_accept" -> stage = "done" /\ accepted /\ dependencyAdded /\ ~deferred
      [] scenario = "uncommitted_conflict_defer_once" -> stage = "done" /\ deferred /\ recoveryStarted /\ duplicateRecoveryCount = 1 /\ ~accepted
      [] scenario = "ok_quorum_accept" -> stage = "done" /\ accepted /\ okVotes >= SlowQuorum

EventuallyCoversTryPreAcceptBranches == <>BranchCovered

================================================================================
