--------------------------- MODULE EPaxosTryPreAcceptMessagePath ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS ReplicaCount

(*
This focused finite model covers concrete TryPreAccept request/response message
paths implemented in epaxos/node.go and exercised by epaxos/protocol_coverage_test.go:
  - follower MsgTryPreAccept handling for committed, stale-ballot, conflict,
    duplicate matching PreAccepted, and fresh PreAccepted records,
  - coordinator MsgTryPreAcceptResp handling for stale restart, committed and
    uncommitted conflict rejects, older-ballot ignore, duplicate OK ignore,
    first OK below quorum, and OK slow-quorum accept.
  - startTryPreAccept pre-seeded witness/local OK quorum that starts Accept
    immediately before broadcasting more TryPreAccept messages,
It is finite 3/5/7 message-path coverage, not a full network, evidence-query,
or unbounded optimized-recovery proof.
*)

VARIABLES scenario,
          stage,
          inputMsg,
          outputMsg,
          durableWrite,
          prepareStarted,
          evidenceRequested,
          acceptStarted,
          dependencyAdded,
          deferred,
          dependencyRecoveryStarted,
          existingOK,
          duplicateDropped,
          staleIgnored,
          committedReply,
          conflictReject

Replicas == 1..ReplicaCount
SlowQuorum == (ReplicaCount \div 2) + 1

Scenarios == {
    "follower_committed_sends_commit_only",
    "follower_stale_reject",
    "follower_committed_conflict_reject",
    "follower_uncommitted_conflict_reject",
    "follower_duplicate_preaccept_reack",
    "follower_fresh_preaccept_records_ack",
    "coordinator_preseed_quorum_immediate_accept",
    "coordinator_stale_reject_restarts_prepare",
    "coordinator_committed_conflict_starts_evidence",
    "coordinator_committed_conflict_direct_accept",
    "coordinator_uncommitted_conflict_forces_accept",
    "coordinator_uncommitted_conflict_defers",
    "coordinator_ok_old_ballot_ignored",
    "coordinator_ok_duplicate_ignored",
    "coordinator_ok_first_nonquorum",
    "coordinator_ok_quorum_accepts"
}

Stages == {"start", "done"}
InputMsgs == {"StartTryPreAccept", "MsgTryPreAccept", "MsgTryPreAcceptResp"}
OutputMsgs == {"none", "MsgCommit", "MsgTryPreAcceptRespOK", "MsgTryPreAcceptRespRejectStale", "MsgTryPreAcceptRespRejectCommitted", "MsgTryPreAcceptRespRejectUncommitted", "MsgEvidence", "MsgAccept", "MsgPrepare"}
ConflictRejects == {"none", "committed", "uncommitted"}

InitialOK ==
    CASE scenario = "coordinator_preseed_quorum_immediate_accept" -> SlowQuorum
      [] scenario = "coordinator_ok_quorum_accepts" -> SlowQuorum - 1
      [] scenario = "coordinator_ok_duplicate_ignored" -> 1
      [] OTHER -> 0

Vars == <<scenario,
          stage,
          inputMsg,
          outputMsg,
          durableWrite,
          prepareStarted,
          evidenceRequested,
          acceptStarted,
          dependencyAdded,
          deferred,
          dependencyRecoveryStarted,
          existingOK,
          duplicateDropped,
          staleIgnored,
          committedReply,
          conflictReject>>

Init ==
    /\ ReplicaCount \in {3, 5, 7}
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ inputMsg = IF scenario \in {
            "follower_committed_sends_commit_only",
            "follower_stale_reject",
            "follower_committed_conflict_reject",
            "follower_uncommitted_conflict_reject",
            "follower_duplicate_preaccept_reack",
            "follower_fresh_preaccept_records_ack"}
        THEN "MsgTryPreAccept"
        ELSE IF scenario = "coordinator_preseed_quorum_immediate_accept"
             THEN "StartTryPreAccept"
             ELSE "MsgTryPreAcceptResp"
    /\ outputMsg = "none"
    /\ durableWrite = FALSE
    /\ prepareStarted = FALSE
    /\ evidenceRequested = FALSE
    /\ acceptStarted = FALSE
    /\ dependencyAdded = FALSE
    /\ deferred = FALSE
    /\ dependencyRecoveryStarted = FALSE
    /\ existingOK = InitialOK
    /\ duplicateDropped = FALSE
    /\ staleIgnored = FALSE
    /\ committedReply = FALSE
    /\ conflictReject = "none"

TypeOK ==
    /\ ReplicaCount \in {3, 5, 7}
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ inputMsg \in InputMsgs
    /\ outputMsg \in OutputMsgs
    /\ durableWrite \in BOOLEAN
    /\ prepareStarted \in BOOLEAN
    /\ evidenceRequested \in BOOLEAN
    /\ acceptStarted \in BOOLEAN
    /\ dependencyAdded \in BOOLEAN
    /\ deferred \in BOOLEAN
    /\ dependencyRecoveryStarted \in BOOLEAN
    /\ existingOK \in 0..ReplicaCount
    /\ duplicateDropped \in BOOLEAN
    /\ staleIgnored \in BOOLEAN
    /\ committedReply \in BOOLEAN
    /\ conflictReject \in ConflictRejects

FollowerCommittedSendsCommitOnly ==
    /\ scenario = "follower_committed_sends_commit_only"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAccept"
    /\ stage' = "done"
    /\ outputMsg' = "MsgCommit"
    /\ committedReply' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  staleIgnored, conflictReject>>

FollowerStaleReject ==
    /\ scenario = "follower_stale_reject"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAccept"
    /\ stage' = "done"
    /\ outputMsg' = "MsgTryPreAcceptRespRejectStale"
    /\ staleIgnored' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  committedReply, conflictReject>>

FollowerConflictReject(kind, out) ==
    /\ scenario = IF kind = "committed" THEN "follower_committed_conflict_reject" ELSE "follower_uncommitted_conflict_reject"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAccept"
    /\ stage' = "done"
    /\ outputMsg' = out
    /\ conflictReject' = kind
    /\ UNCHANGED <<scenario, inputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  staleIgnored, committedReply>>

FollowerDuplicatePreacceptReack ==
    /\ scenario = "follower_duplicate_preaccept_reack"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAccept"
    /\ stage' = "done"
    /\ outputMsg' = "MsgTryPreAcceptRespOK"
    /\ UNCHANGED <<scenario, inputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  staleIgnored, committedReply, conflictReject>>

FollowerFreshPreacceptRecordsAck ==
    /\ scenario = "follower_fresh_preaccept_records_ack"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAccept"
    /\ stage' = "done"
    /\ outputMsg' = "MsgTryPreAcceptRespOK"
    /\ durableWrite' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, prepareStarted, evidenceRequested,
                  acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  staleIgnored, committedReply, conflictReject>>

CoordinatorStaleRejectRestartsPrepare ==
    /\ scenario = "coordinator_stale_reject_restarts_prepare"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ stage' = "done"
    /\ outputMsg' = "MsgPrepare"
    /\ prepareStarted' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, durableWrite, evidenceRequested,
                  acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  staleIgnored, committedReply, conflictReject>>

CoordinatorCommittedConflictStartsEvidence ==
    /\ scenario = "coordinator_committed_conflict_starts_evidence"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ stage' = "done"
    /\ outputMsg' = "MsgEvidence"
    /\ evidenceRequested' = TRUE
    /\ conflictReject' = "committed"
    /\ UNCHANGED <<scenario, inputMsg, durableWrite, prepareStarted,
                  acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  staleIgnored, committedReply>>

CoordinatorCommittedConflictDirectAccept ==
    /\ scenario = "coordinator_committed_conflict_direct_accept"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ stage' = "done"
    /\ outputMsg' = "MsgAccept"
    /\ acceptStarted' = TRUE
    /\ dependencyAdded' = TRUE
    /\ durableWrite' = TRUE
    /\ conflictReject' = "committed"
    /\ UNCHANGED <<scenario, inputMsg, prepareStarted, evidenceRequested,
                  deferred, dependencyRecoveryStarted, existingOK,
                  duplicateDropped, staleIgnored, committedReply>>

CoordinatorUncommittedConflictForcesAccept ==
    /\ scenario = "coordinator_uncommitted_conflict_forces_accept"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ stage' = "done"
    /\ outputMsg' = "MsgAccept"
    /\ acceptStarted' = TRUE
    /\ dependencyAdded' = TRUE
    /\ durableWrite' = TRUE
    /\ conflictReject' = "uncommitted"
    /\ UNCHANGED <<scenario, inputMsg, prepareStarted, evidenceRequested,
                  deferred, dependencyRecoveryStarted, existingOK,
                  duplicateDropped, staleIgnored, committedReply>>

CoordinatorUncommittedConflictDefers ==
    /\ scenario = "coordinator_uncommitted_conflict_defers"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ stage' = "done"
    /\ outputMsg' = "MsgPrepare"
    /\ deferred' = TRUE
    /\ dependencyRecoveryStarted' = TRUE
    /\ conflictReject' = "uncommitted"
    /\ UNCHANGED <<scenario, inputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded,
                  existingOK, duplicateDropped, staleIgnored, committedReply>>

CoordinatorOKOldBallotIgnored ==
    /\ scenario = "coordinator_ok_old_ballot_ignored"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ stage' = "done"
    /\ staleIgnored' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, outputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, duplicateDropped,
                  committedReply, conflictReject>>

CoordinatorOKDuplicateIgnored ==
    /\ scenario = "coordinator_ok_duplicate_ignored"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ existingOK > 0
    /\ stage' = "done"
    /\ duplicateDropped' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, outputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, existingOK, staleIgnored,
                  committedReply, conflictReject>>

CoordinatorOKFirstNonQuorum ==
    /\ scenario = "coordinator_ok_first_nonquorum"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ existingOK = 0
    /\ existingOK + 1 < SlowQuorum
    /\ stage' = "done"
    /\ existingOK' = existingOK + 1
    /\ UNCHANGED <<scenario, inputMsg, outputMsg, durableWrite, prepareStarted,
                  evidenceRequested, acceptStarted, dependencyAdded, deferred,
                  dependencyRecoveryStarted, duplicateDropped, staleIgnored,
                  committedReply, conflictReject>>

CoordinatorPreseedQuorumImmediateAccept ==
    /\ scenario = "coordinator_preseed_quorum_immediate_accept"
    /\ stage = "start"
    /\ inputMsg = "StartTryPreAccept"
    /\ existingOK >= SlowQuorum
    /\ stage' = "done"
    /\ outputMsg' = "MsgAccept"
    /\ durableWrite' = TRUE
    /\ acceptStarted' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, prepareStarted, evidenceRequested,
                  dependencyAdded, deferred, dependencyRecoveryStarted,
                  existingOK, duplicateDropped, staleIgnored, committedReply,
                  conflictReject>>

CoordinatorOKQuorumAccepts ==
    /\ scenario = "coordinator_ok_quorum_accepts"
    /\ stage = "start"
    /\ inputMsg = "MsgTryPreAcceptResp"
    /\ existingOK = SlowQuorum - 1
    /\ stage' = "done"
    /\ outputMsg' = "MsgAccept"
    /\ existingOK' = existingOK + 1
    /\ acceptStarted' = TRUE
    /\ durableWrite' = TRUE
    /\ UNCHANGED <<scenario, inputMsg, prepareStarted, evidenceRequested,
                  dependencyAdded, deferred, dependencyRecoveryStarted,
                  duplicateDropped, staleIgnored, committedReply,
                  conflictReject>>

Next ==
    \/ FollowerCommittedSendsCommitOnly
    \/ FollowerStaleReject
    \/ FollowerConflictReject("committed", "MsgTryPreAcceptRespRejectCommitted")
    \/ FollowerConflictReject("uncommitted", "MsgTryPreAcceptRespRejectUncommitted")
    \/ FollowerDuplicatePreacceptReack
    \/ FollowerFreshPreacceptRecordsAck
    \/ CoordinatorStaleRejectRestartsPrepare
    \/ CoordinatorCommittedConflictStartsEvidence
    \/ CoordinatorCommittedConflictDirectAccept
    \/ CoordinatorUncommittedConflictForcesAccept
    \/ CoordinatorUncommittedConflictDefers
    \/ CoordinatorOKOldBallotIgnored
    \/ CoordinatorOKDuplicateIgnored
    \/ CoordinatorOKFirstNonQuorum
    \/ CoordinatorPreseedQuorumImmediateAccept
    \/ CoordinatorOKQuorumAccepts

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

FollowerCommittedOnlySendsCommit ==
    scenario = "follower_committed_sends_commit_only" /\ stage = "done" =>
        /\ outputMsg = "MsgCommit"
        /\ committedReply
        /\ ~durableWrite
        /\ conflictReject = "none"

FollowerStaleRejectDoesNotWrite ==
    scenario = "follower_stale_reject" /\ stage = "done" =>
        /\ outputMsg = "MsgTryPreAcceptRespRejectStale"
        /\ staleIgnored
        /\ ~durableWrite

FollowerConflictRejectHasReasonAndNoWrite ==
    scenario \in {"follower_committed_conflict_reject", "follower_uncommitted_conflict_reject"} /\ stage = "done" =>
        /\ ~durableWrite
        /\ conflictReject \in {"committed", "uncommitted"}
        /\ (conflictReject = "committed" => outputMsg = "MsgTryPreAcceptRespRejectCommitted")
        /\ (conflictReject = "uncommitted" => outputMsg = "MsgTryPreAcceptRespRejectUncommitted")

FollowerDuplicateReackDoesNotWrite ==
    scenario = "follower_duplicate_preaccept_reack" /\ stage = "done" =>
        /\ outputMsg = "MsgTryPreAcceptRespOK"
        /\ ~durableWrite

FollowerFreshAckWritesOnce ==
    scenario = "follower_fresh_preaccept_records_ack" /\ stage = "done" =>
        /\ outputMsg = "MsgTryPreAcceptRespOK"
        /\ durableWrite

CoordinatorStaleRejectOnlyRestartsPrepare ==
    scenario = "coordinator_stale_reject_restarts_prepare" /\ stage = "done" =>
        /\ outputMsg = "MsgPrepare"
        /\ prepareStarted
        /\ ~acceptStarted
        /\ ~evidenceRequested

CoordinatorCommittedEvidenceDoesNotAccept ==
    scenario = "coordinator_committed_conflict_starts_evidence" /\ stage = "done" =>
        /\ outputMsg = "MsgEvidence"
        /\ evidenceRequested
        /\ conflictReject = "committed"
        /\ ~acceptStarted

CoordinatorCommittedDirectAcceptAddsDependency ==
    scenario = "coordinator_committed_conflict_direct_accept" /\ stage = "done" =>
        /\ outputMsg = "MsgAccept"
        /\ acceptStarted
        /\ dependencyAdded
        /\ durableWrite
        /\ conflictReject = "committed"
        /\ ~evidenceRequested

CoordinatorUncommittedForcedAcceptDoesNotRecover ==
    scenario = "coordinator_uncommitted_conflict_forces_accept" /\ stage = "done" =>
        /\ outputMsg = "MsgAccept"
        /\ acceptStarted
        /\ dependencyAdded
        /\ durableWrite
        /\ conflictReject = "uncommitted"
        /\ ~dependencyRecoveryStarted
        /\ ~deferred

CoordinatorUncommittedDefersAndRecovers ==
    scenario = "coordinator_uncommitted_conflict_defers" /\ stage = "done" =>
        /\ outputMsg = "MsgPrepare"
        /\ deferred
        /\ dependencyRecoveryStarted
        /\ conflictReject = "uncommitted"
        /\ ~acceptStarted

CoordinatorOldBallotOKIgnored ==
    scenario = "coordinator_ok_old_ballot_ignored" /\ stage = "done" =>
        /\ outputMsg = "none"
        /\ staleIgnored
        /\ existingOK = 0
        /\ ~acceptStarted

CoordinatorDuplicateOKIgnored ==
    scenario = "coordinator_ok_duplicate_ignored" /\ stage = "done" =>
        /\ outputMsg = "none"
        /\ duplicateDropped
        /\ existingOK = 1
        /\ ~acceptStarted

CoordinatorFirstOKBelowQuorumDoesNotAccept ==
    scenario = "coordinator_ok_first_nonquorum" /\ stage = "done" =>
        /\ outputMsg = "none"
        /\ existingOK = 1
        /\ existingOK < SlowQuorum
        /\ ~acceptStarted

CoordinatorPreseedQuorumAcceptsWithoutBroadcast ==
    scenario = "coordinator_preseed_quorum_immediate_accept" /\ stage = "done" =>
        /\ inputMsg = "StartTryPreAccept"
        /\ outputMsg = "MsgAccept"
        /\ durableWrite
        /\ acceptStarted
        /\ existingOK >= SlowQuorum
        /\ ~evidenceRequested
        /\ ~deferred

CoordinatorOKAcceptRequiresSlowQuorum ==
    scenario = "coordinator_ok_quorum_accepts" /\ stage = "done" =>
        /\ outputMsg = "MsgAccept"
        /\ acceptStarted
        /\ durableWrite
        /\ existingOK >= SlowQuorum

Safety ==
    /\ FollowerCommittedOnlySendsCommit
    /\ FollowerStaleRejectDoesNotWrite
    /\ FollowerConflictRejectHasReasonAndNoWrite
    /\ FollowerDuplicateReackDoesNotWrite
    /\ FollowerFreshAckWritesOnce
    /\ CoordinatorStaleRejectOnlyRestartsPrepare
    /\ CoordinatorCommittedEvidenceDoesNotAccept
    /\ CoordinatorCommittedDirectAcceptAddsDependency
    /\ CoordinatorUncommittedForcedAcceptDoesNotRecover
    /\ CoordinatorUncommittedDefersAndRecovers
    /\ CoordinatorOldBallotOKIgnored
    /\ CoordinatorDuplicateOKIgnored
    /\ CoordinatorFirstOKBelowQuorumDoesNotAccept
    /\ CoordinatorPreseedQuorumAcceptsWithoutBroadcast
    /\ CoordinatorOKAcceptRequiresSlowQuorum

PathCovered == stage = "done"

EventuallyCoversTryPreAcceptMessagePaths == <>PathCovered

================================================================================
