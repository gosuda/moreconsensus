---- MODULE EPaxosConfigRecoveryRetry ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite configuration-recovery retry model. It checks logical     *)
(* prepare/accept timer rebroadcasts for one old instance after removal and *)
(* one old instance after addition. In each case retry targets and deps      *)
(* width stay pinned to the instance Ref.Conf, not the current config. Pure  *)
(* retries reschedule logical timers without durable or application effects. *)
(* This does not model arbitrary recovery histories, message loss, joint     *)
(* consensus, or unbounded membership changes.                              *)
(***************************************************************************)

VARIABLES scenario, stage, phase, retryTargets, depsWidth,
          retryScheduled, durableRecordWritten, commandApplied

Coordinator == 2
RemovedVoter == 4
AddedVoter == 4
OldRemovalVoters == {1, 2, 3, 4}
CurrentAfterRemovalVoters == {1, 2, 3}
OldAdditionVoters == {1, 2, 3}
CurrentAfterAdditionVoters == {1, 2, 3, 4}

PrepareScenarios == {"removal_prepare_retry", "addition_prepare_retry"}
AcceptScenarios == {"removal_accept_retry", "addition_accept_retry"}
RemovalScenarios == {"removal_prepare_retry", "removal_accept_retry"}
AdditionScenarios == {"addition_prepare_retry", "addition_accept_retry"}
Scenarios == PrepareScenarios \cup AcceptScenarios
Stages == {"start", "done"}
Phases == {"prepare", "accept"}
AllVoters == {1, 2, 3, 4}

OldVoters(s) == IF s \in RemovalScenarios THEN OldRemovalVoters ELSE OldAdditionVoters
CurrentVoters(s) == IF s \in RemovalScenarios THEN CurrentAfterRemovalVoters ELSE CurrentAfterAdditionVoters
OldPeers(s) == OldVoters(s) \ {Coordinator}

Vars == <<scenario, stage, phase, retryTargets, depsWidth,
          retryScheduled, durableRecordWritten, commandApplied>>

TypeOK ==
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ phase \in Phases
    /\ retryTargets \subseteq AllVoters
    /\ depsWidth \in 0..4
    /\ retryScheduled \in BOOLEAN
    /\ durableRecordWritten \in BOOLEAN
    /\ commandApplied \in BOOLEAN

Init ==
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ phase = IF scenario \in PrepareScenarios THEN "prepare" ELSE "accept"
    /\ retryTargets = {}
    /\ depsWidth = 0
    /\ retryScheduled = FALSE
    /\ durableRecordWritten = FALSE
    /\ commandApplied = FALSE

PrepareRetry ==
    /\ scenario \in PrepareScenarios
    /\ stage = "start"
    /\ phase = "prepare"
    /\ stage' = "done"
    /\ retryTargets' = OldPeers(scenario)
    /\ depsWidth' = Cardinality(OldVoters(scenario))
    /\ retryScheduled' = TRUE
    /\ UNCHANGED <<scenario, phase, durableRecordWritten, commandApplied>>

AcceptRetry ==
    /\ scenario \in AcceptScenarios
    /\ stage = "start"
    /\ phase = "accept"
    /\ stage' = "done"
    /\ retryTargets' = OldPeers(scenario)
    /\ depsWidth' = Cardinality(OldVoters(scenario))
    /\ retryScheduled' = TRUE
    /\ UNCHANGED <<scenario, phase, durableRecordWritten, commandApplied>>

Next == PrepareRetry \/ AcceptRetry

RetryTargetsPinnedToOldConfig ==
    stage = "done" =>
        /\ retryTargets = OldPeers(scenario)
        /\ retryTargets \subseteq OldVoters(scenario)
        /\ Coordinator \notin retryTargets

RemovalRetryStillIncludesRemovedOldVoter ==
    scenario \in RemovalScenarios /\ stage = "done" =>
        /\ RemovedVoter \in retryTargets
        /\ RemovedVoter \notin CurrentVoters(scenario)

AdditionRetryExcludesNewVoterForOldInstance ==
    scenario \in AdditionScenarios /\ stage = "done" =>
        /\ AddedVoter \notin retryTargets
        /\ AddedVoter \in CurrentVoters(scenario)

RetryUsesOldDependencyWidth ==
    stage = "done" => depsWidth = Cardinality(OldVoters(scenario))

RetryDoesNotMutateDurableOrApplicationState ==
    retryScheduled =>
        /\ ~durableRecordWritten
        /\ ~commandApplied

TimerKindPreservesPhase ==
    stage = "done" =>
        /\ (scenario \in PrepareScenarios => phase = "prepare")
        /\ (scenario \in AcceptScenarios => phase = "accept")

Safety ==
    /\ RetryTargetsPinnedToOldConfig
    /\ RemovalRetryStillIncludesRemovedOldVoter
    /\ AdditionRetryExcludesNewVoterForOldInstance
    /\ RetryUsesOldDependencyWidth
    /\ RetryDoesNotMutateDurableOrApplicationState
    /\ TimerKindPreservesPhase

RetryScenarioCovered ==
    CASE scenario = "removal_prepare_retry" ->
            stage = "done" /\ phase = "prepare" /\ retryTargets = {1, 3, 4} /\ depsWidth = 4
      [] scenario = "removal_accept_retry" ->
            stage = "done" /\ phase = "accept" /\ retryTargets = {1, 3, 4} /\ depsWidth = 4
      [] scenario = "addition_prepare_retry" ->
            stage = "done" /\ phase = "prepare" /\ retryTargets = {1, 3} /\ depsWidth = 3
      [] scenario = "addition_accept_retry" ->
            stage = "done" /\ phase = "accept" /\ retryTargets = {1, 3} /\ depsWidth = 3

EventuallyCoversConfigRecoveryRetry == <>RetryScenarioCovered

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
