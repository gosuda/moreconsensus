--------------------- MODULE EPaxosOptimizedRecoveryDecisionTree ---------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite decision-table model for the implemented optimized         *)
(* recovery branch surface in F<=3 Accept-Deps mode. The table abstracts     *)
(* Section 6.2 recovery decisions into one deterministic resolution step per  *)
(* scenario and checks branch parity with epaxos/node.go's implemented        *)
(* prepare, TryPreAccept, committed-conflict evidence, and retry/duplicate   *)
(* handling. It is not an arbitrary-network, message-loss, reconfiguration,  *)
(* durable-history, operational-clock, or unbounded proof model.              *)
(***************************************************************************)

VARIABLES scenario, faultTolerance, stage, action, phase,
          evidenceStarted, failClosed, ignoreResent,
          targetDurableWritten, blockerRecoveryStarted, blockerDurableWritten,
          commandApplied, noopChosen, committedRecordUsed, acceptedRecordUsed,
          dependencyAdded, prepareStarted, staleOrOldDropped, duplicateDropped,
          deferred, okVotes, retryMessageSent, acceptMessages, commitMessages

FaultTolerances == 1..3
ReplicaCount == (2 * faultTolerance) + 1
RemoteCount == ReplicaCount - 1
SlowQuorum == faultTolerance + 1

Scenarios == {
    "no_information_noop_accept",
    "committed_record_commit",
    "accepted_record_slow_accept",
    "matching_preaccepted_witnesses_slow_accept",
    "committed_conflict_usable_evidence_resend_ignore",
    "committed_conflict_no_usable_evidence_slow_accept",
    "committed_conflict_outside_candidate_deps_slow_accept",
    "uncommitted_conflict_leader_required_slow_accept",
    "deferred_cycle_conflict_slow_accept",
    "uncommitted_optional_conflict_defer_recover",
    "stale_ballot_restart_prepare",
    "ok_trypreaccept_quorum_slow_accept",
    "duplicate_old_trypreaccept_response_ignored"
}

Stages == {"start", "done"}
Phases == {"try_pre_accept", "prepare", "accept", "committed"}
Actions == {
    "none",
    "choose_noop_accept",
    "commit",
    "slow_accept",
    "resend_try_ignore",
    "defer_recover",
    "restart_prepare",
    "ignore_response"
}
MessageCounts == 0..6
Vars == <<scenario, faultTolerance, stage, action, phase,
          evidenceStarted, failClosed, ignoreResent,
          targetDurableWritten, blockerRecoveryStarted, blockerDurableWritten,
          commandApplied, noopChosen, committedRecordUsed, acceptedRecordUsed,
          dependencyAdded, prepareStarted, staleOrOldDropped, duplicateDropped,
          deferred, okVotes, retryMessageSent, acceptMessages, commitMessages>>

ActionFor(s) ==
    CASE s = "no_information_noop_accept" -> "choose_noop_accept"
      [] s = "committed_record_commit" -> "commit"
      [] s = "accepted_record_slow_accept" -> "slow_accept"
      [] s = "matching_preaccepted_witnesses_slow_accept" -> "slow_accept"
      [] s = "committed_conflict_usable_evidence_resend_ignore" -> "resend_try_ignore"
      [] s = "committed_conflict_no_usable_evidence_slow_accept" -> "slow_accept"
      [] s = "committed_conflict_outside_candidate_deps_slow_accept" -> "slow_accept"
      [] s = "uncommitted_conflict_leader_required_slow_accept" -> "slow_accept"
      [] s = "deferred_cycle_conflict_slow_accept" -> "slow_accept"
      [] s = "uncommitted_optional_conflict_defer_recover" -> "defer_recover"
      [] s = "stale_ballot_restart_prepare" -> "restart_prepare"
      [] s = "ok_trypreaccept_quorum_slow_accept" -> "slow_accept"
      [] s = "duplicate_old_trypreaccept_response_ignored" -> "ignore_response"

PhaseFor(s) ==
    CASE s = "committed_record_commit" -> "committed"
      [] s = "stale_ballot_restart_prepare" -> "prepare"
      [] s \in {"committed_conflict_usable_evidence_resend_ignore",
                 "uncommitted_optional_conflict_defer_recover",
                 "duplicate_old_trypreaccept_response_ignored"} -> "try_pre_accept"
      [] OTHER -> "accept"

EvidenceStartedFor(s) ==
    s \in {"committed_conflict_usable_evidence_resend_ignore",
           "committed_conflict_no_usable_evidence_slow_accept"}

FailClosedFor(s) == s = "committed_conflict_no_usable_evidence_slow_accept"
IgnoreResentFor(s) == s = "committed_conflict_usable_evidence_resend_ignore"

TargetDurableFor(s) ==
    s \in {"no_information_noop_accept",
           "committed_record_commit",
           "accepted_record_slow_accept",
           "matching_preaccepted_witnesses_slow_accept",
           "committed_conflict_no_usable_evidence_slow_accept",
           "committed_conflict_outside_candidate_deps_slow_accept",
           "uncommitted_conflict_leader_required_slow_accept",
           "deferred_cycle_conflict_slow_accept",
           "stale_ballot_restart_prepare",
           "ok_trypreaccept_quorum_slow_accept"}

BlockerRecoveryFor(s) == s = "uncommitted_optional_conflict_defer_recover"
CommandAppliedFor(s) == s = "committed_record_commit"
NoopChosenFor(s) == s = "no_information_noop_accept"
CommittedRecordFor(s) == s = "committed_record_commit"
AcceptedRecordFor(s) == s = "accepted_record_slow_accept"

DependencyAddedFor(s) ==
    s \in {"committed_conflict_no_usable_evidence_slow_accept",
           "committed_conflict_outside_candidate_deps_slow_accept"}

PrepareStartedFor(s) == s = "stale_ballot_restart_prepare"
StaleOrOldDroppedFor(s) == s \in {"stale_ballot_restart_prepare", "duplicate_old_trypreaccept_response_ignored"}
DuplicateDroppedFor(s) == s = "duplicate_old_trypreaccept_response_ignored"
DeferredFor(s) == s = "uncommitted_optional_conflict_defer_recover"
RetrySentFor(s) == s = "committed_conflict_usable_evidence_resend_ignore"

OKVotesFor(s) ==
    IF s \in {"matching_preaccepted_witnesses_slow_accept",
              "ok_trypreaccept_quorum_slow_accept"}
    THEN SlowQuorum
    ELSE 0

AcceptMessagesFor(s) ==
    IF ActionFor(s) \in {"choose_noop_accept", "slow_accept"}
    THEN RemoteCount
    ELSE 0

CommitMessagesFor(s) == IF s = "committed_record_commit" THEN RemoteCount ELSE 0

TypeOK ==
    /\ faultTolerance \in FaultTolerances
    /\ ReplicaCount \in {3, 5, 7}
    /\ RemoteCount \in MessageCounts
    /\ SlowQuorum \in 2..4
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ action \in Actions
    /\ phase \in Phases
    /\ evidenceStarted \in BOOLEAN
    /\ failClosed \in BOOLEAN
    /\ ignoreResent \in BOOLEAN
    /\ targetDurableWritten \in BOOLEAN
    /\ blockerRecoveryStarted \in BOOLEAN
    /\ blockerDurableWritten \in BOOLEAN
    /\ commandApplied \in BOOLEAN
    /\ noopChosen \in BOOLEAN
    /\ committedRecordUsed \in BOOLEAN
    /\ acceptedRecordUsed \in BOOLEAN
    /\ dependencyAdded \in BOOLEAN
    /\ prepareStarted \in BOOLEAN
    /\ staleOrOldDropped \in BOOLEAN
    /\ duplicateDropped \in BOOLEAN
    /\ deferred \in BOOLEAN
    /\ okVotes \in MessageCounts
    /\ retryMessageSent \in BOOLEAN
    /\ acceptMessages \in MessageCounts
    /\ commitMessages \in MessageCounts

Init ==
    /\ faultTolerance \in FaultTolerances
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ action = "none"
    /\ phase = "try_pre_accept"
    /\ evidenceStarted = FALSE
    /\ failClosed = FALSE
    /\ ignoreResent = FALSE
    /\ targetDurableWritten = FALSE
    /\ blockerRecoveryStarted = FALSE
    /\ blockerDurableWritten = FALSE
    /\ commandApplied = FALSE
    /\ noopChosen = FALSE
    /\ committedRecordUsed = FALSE
    /\ acceptedRecordUsed = FALSE
    /\ dependencyAdded = FALSE
    /\ prepareStarted = FALSE
    /\ staleOrOldDropped = FALSE
    /\ duplicateDropped = FALSE
    /\ deferred = FALSE
    /\ okVotes = 0
    /\ retryMessageSent = FALSE
    /\ acceptMessages = 0
    /\ commitMessages = 0

ResolveDecision ==
    /\ stage = "start"
    /\ stage' = "done"
    /\ action' = ActionFor(scenario)
    /\ phase' = PhaseFor(scenario)
    /\ evidenceStarted' = EvidenceStartedFor(scenario)
    /\ failClosed' = FailClosedFor(scenario)
    /\ ignoreResent' = IgnoreResentFor(scenario)
    /\ targetDurableWritten' = TargetDurableFor(scenario)
    /\ blockerRecoveryStarted' = BlockerRecoveryFor(scenario)
    /\ blockerDurableWritten' = BlockerRecoveryFor(scenario)
    /\ commandApplied' = CommandAppliedFor(scenario)
    /\ noopChosen' = NoopChosenFor(scenario)
    /\ committedRecordUsed' = CommittedRecordFor(scenario)
    /\ acceptedRecordUsed' = AcceptedRecordFor(scenario)
    /\ dependencyAdded' = DependencyAddedFor(scenario)
    /\ prepareStarted' = PrepareStartedFor(scenario)
    /\ staleOrOldDropped' = StaleOrOldDroppedFor(scenario)
    /\ duplicateDropped' = DuplicateDroppedFor(scenario)
    /\ deferred' = DeferredFor(scenario)
    /\ okVotes' = OKVotesFor(scenario)
    /\ retryMessageSent' = RetrySentFor(scenario)
    /\ acceptMessages' = AcceptMessagesFor(scenario)
    /\ commitMessages' = CommitMessagesFor(scenario)
    /\ UNCHANGED <<scenario, faultTolerance>>

Next == ResolveDecision
Spec == Init /\ [][Next]_Vars /\ WF_Vars(ResolveDecision)

ChosenActions == {a \in Actions \ {"none"} : action = a}
ExactlyOneActionPerScenario ==
    stage = "done" => Cardinality(ChosenActions) = 1

NoInformationChoosesNoop ==
    scenario = "no_information_noop_accept" /\ stage = "done" =>
        /\ action = "choose_noop_accept"
        /\ phase = "accept"
        /\ noopChosen
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount
        /\ ~commandApplied

CommittedRecordCommits ==
    scenario = "committed_record_commit" /\ stage = "done" =>
        /\ action = "commit"
        /\ phase = "committed"
        /\ committedRecordUsed
        /\ targetDurableWritten
        /\ commandApplied
        /\ commitMessages = RemoteCount
        /\ acceptMessages = 0

AcceptedRecordUsesSlowAccept ==
    scenario = "accepted_record_slow_accept" /\ stage = "done" =>
        /\ action = "slow_accept"
        /\ phase = "accept"
        /\ acceptedRecordUsed
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount
        /\ ~commandApplied

MatchingPreAcceptedVotesUseSlowAccept ==
    scenario = "matching_preaccepted_witnesses_slow_accept" /\ stage = "done" =>
        /\ action = "slow_accept"
        /\ phase = "accept"
        /\ okVotes >= SlowQuorum
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount
        /\ ~commandApplied

UsableEvidenceOnlyResendsIgnore ==
    scenario = "committed_conflict_usable_evidence_resend_ignore" /\ stage = "done" =>
        /\ action = "resend_try_ignore"
        /\ phase = "try_pre_accept"
        /\ evidenceStarted
        /\ ignoreResent
        /\ retryMessageSent
        /\ ~failClosed
        /\ ~targetDurableWritten
        /\ acceptMessages = 0
        /\ commitMessages = 0
        /\ ~commandApplied

NoUsableEvidenceFailsClosedToSlowAccept ==
    scenario = "committed_conflict_no_usable_evidence_slow_accept" /\ stage = "done" =>
        /\ action = "slow_accept"
        /\ phase = "accept"
        /\ evidenceStarted
        /\ failClosed
        /\ ~ignoreResent
        /\ dependencyAdded
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount

CommittedConflictOutsideCandidateDepsAcceptsWithoutEvidence ==
    scenario = "committed_conflict_outside_candidate_deps_slow_accept" /\ stage = "done" =>
        /\ action = "slow_accept"
        /\ phase = "accept"
        /\ ~evidenceStarted
        /\ dependencyAdded
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount

UncommittedRequiredLeaderUsesSlowAccept ==
    scenario = "uncommitted_conflict_leader_required_slow_accept" /\ stage = "done" =>
        /\ action = "slow_accept"
        /\ phase = "accept"
        /\ ~deferred
        /\ ~blockerRecoveryStarted
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount

DeferredCycleUsesSlowAccept ==
    scenario = "deferred_cycle_conflict_slow_accept" /\ stage = "done" =>
        /\ action = "slow_accept"
        /\ phase = "accept"
        /\ ~deferred
        /\ ~blockerRecoveryStarted
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount

OptionalUncommittedConflictDefersAndRecoversBlocker ==
    scenario = "uncommitted_optional_conflict_defer_recover" /\ stage = "done" =>
        /\ action = "defer_recover"
        /\ phase = "try_pre_accept"
        /\ deferred
        /\ blockerRecoveryStarted
        /\ blockerDurableWritten
        /\ ~targetDurableWritten
        /\ acceptMessages = 0
        /\ commitMessages = 0
        /\ ~commandApplied

StaleBallotRestartsPrepare ==
    scenario = "stale_ballot_restart_prepare" /\ stage = "done" =>
        /\ action = "restart_prepare"
        /\ phase = "prepare"
        /\ prepareStarted
        /\ staleOrOldDropped
        /\ targetDurableWritten
        /\ ~commandApplied
        /\ acceptMessages = 0
        /\ commitMessages = 0

OKTryPreAcceptQuorumUsesSlowAccept ==
    scenario = "ok_trypreaccept_quorum_slow_accept" /\ stage = "done" =>
        /\ action = "slow_accept"
        /\ phase = "accept"
        /\ okVotes >= SlowQuorum
        /\ targetDurableWritten
        /\ acceptMessages = RemoteCount
        /\ ~commandApplied

DuplicateOrOldTryPreAcceptResponseDoesNotAdvance ==
    scenario = "duplicate_old_trypreaccept_response_ignored" /\ stage = "done" =>
        /\ action = "ignore_response"
        /\ phase = "try_pre_accept"
        /\ staleOrOldDropped
        /\ duplicateDropped
        /\ ~targetDurableWritten
        /\ acceptMessages = 0
        /\ commitMessages = 0
        /\ ~commandApplied

NoTargetDurableOrApplicationEffectForPureRetryDefer ==
    stage = "done" /\ action \in {"resend_try_ignore", "defer_recover", "ignore_response"} =>
        /\ ~targetDurableWritten
        /\ ~commandApplied
        /\ acceptMessages = 0
        /\ commitMessages = 0

SlowAcceptBranchesDoNotCommitApplication ==
    stage = "done" /\ action \in {"choose_noop_accept", "slow_accept"} =>
        /\ phase = "accept"
        /\ acceptMessages = RemoteCount
        /\ commitMessages = 0
        /\ ~commandApplied

RestartPrepareDoesNotAcceptOrCommit ==
    stage = "done" /\ action = "restart_prepare" =>
        /\ phase = "prepare"
        /\ acceptMessages = 0
        /\ commitMessages = 0
        /\ ~commandApplied

Safety ==
    /\ NoInformationChoosesNoop
    /\ CommittedRecordCommits
    /\ AcceptedRecordUsesSlowAccept
    /\ MatchingPreAcceptedVotesUseSlowAccept
    /\ UsableEvidenceOnlyResendsIgnore
    /\ NoUsableEvidenceFailsClosedToSlowAccept
    /\ CommittedConflictOutsideCandidateDepsAcceptsWithoutEvidence
    /\ UncommittedRequiredLeaderUsesSlowAccept
    /\ DeferredCycleUsesSlowAccept
    /\ OptionalUncommittedConflictDefersAndRecoversBlocker
    /\ StaleBallotRestartsPrepare
    /\ OKTryPreAcceptQuorumUsesSlowAccept
    /\ DuplicateOrOldTryPreAcceptResponseDoesNotAdvance
    /\ NoTargetDurableOrApplicationEffectForPureRetryDefer
    /\ SlowAcceptBranchesDoNotCommitApplication
    /\ RestartPrepareDoesNotAcceptOrCommit

DecisionCovered ==
    CASE scenario = "no_information_noop_accept" ->
            stage = "done" /\ action = "choose_noop_accept" /\ noopChosen
      [] scenario = "committed_record_commit" ->
            stage = "done" /\ action = "commit" /\ committedRecordUsed /\ commandApplied
      [] scenario = "accepted_record_slow_accept" ->
            stage = "done" /\ action = "slow_accept" /\ acceptedRecordUsed
      [] scenario = "matching_preaccepted_witnesses_slow_accept" ->
            stage = "done" /\ action = "slow_accept" /\ okVotes >= SlowQuorum
      [] scenario = "committed_conflict_usable_evidence_resend_ignore" ->
            stage = "done" /\ action = "resend_try_ignore" /\ evidenceStarted /\ ignoreResent /\ retryMessageSent
      [] scenario = "committed_conflict_no_usable_evidence_slow_accept" ->
            stage = "done" /\ action = "slow_accept" /\ evidenceStarted /\ failClosed /\ dependencyAdded
      [] scenario = "committed_conflict_outside_candidate_deps_slow_accept" ->
            stage = "done" /\ action = "slow_accept" /\ ~evidenceStarted /\ dependencyAdded
      [] scenario = "uncommitted_conflict_leader_required_slow_accept" ->
            stage = "done" /\ action = "slow_accept" /\ ~deferred /\ ~blockerRecoveryStarted
      [] scenario = "deferred_cycle_conflict_slow_accept" ->
            stage = "done" /\ action = "slow_accept" /\ ~deferred /\ ~blockerRecoveryStarted
      [] scenario = "uncommitted_optional_conflict_defer_recover" ->
            stage = "done" /\ action = "defer_recover" /\ deferred /\ blockerRecoveryStarted
      [] scenario = "stale_ballot_restart_prepare" ->
            stage = "done" /\ action = "restart_prepare" /\ prepareStarted
      [] scenario = "ok_trypreaccept_quorum_slow_accept" ->
            stage = "done" /\ action = "slow_accept" /\ okVotes >= SlowQuorum
      [] scenario = "duplicate_old_trypreaccept_response_ignored" ->
            stage = "done" /\ action = "ignore_response" /\ duplicateDropped

EventuallyCoversOptimizedRecoveryDecisionTree == <>DecisionCovered

================================================================================
