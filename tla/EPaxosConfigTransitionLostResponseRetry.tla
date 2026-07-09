---- MODULE EPaxosConfigTransitionLostResponseRetry ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite normal configuration-transition lost-response retry model. *)
(* It checks local-owner old instances after voter removal and voter        *)
(* addition. One needed response is explicitly lost before deterministic    *)
(* retry, so the old pinned quorum must not advance early; retry targets    *)
(* and dependency width remain pinned to Ref.Conf, and only the replacement *)
(* response after retry advances through the old quorum. This does not      *)
(* model arbitrary message loss, arbitrary retry histories, joint consensus,*)
(* arbitrary membership histories, or an unbounded proof.                   *)
(***************************************************************************)

VARIABLES scenario, stage, phase, votes, retryTargets, depsWidth,
          lostAttempted, retryBroadcasted, addedResponseRejected,
          advanced, committed, durableWrites, commandApplied

Owner == 1
RemovedVoter == 4
AddedVoter == 4
OldRemovalVoters == {1, 2, 3, 4}
CurrentAfterRemovalVoters == {1, 2, 3}
OldAdditionVoters == {1, 2, 3}
CurrentAfterAdditionVoters == {1, 2, 3, 4}

PreAcceptScenarios == {"removal_preaccept_loss", "addition_preaccept_loss"}
AcceptScenarios == {"removal_accept_loss", "addition_accept_loss"}
RemovalScenarios == {"removal_preaccept_loss", "removal_accept_loss"}
AdditionScenarios == {"addition_preaccept_loss", "addition_accept_loss"}
Scenarios == PreAcceptScenarios \cup AcceptScenarios
Stages == {"start", "below-quorum", "lost-before-retry", "retried", "done"}
Phases == {"preaccept", "accept"}
AllVoters == {1, 2, 3, 4}

OldVoters(s) == IF s \in RemovalScenarios THEN OldRemovalVoters ELSE OldAdditionVoters
CurrentVoters(s) == IF s \in RemovalScenarios THEN CurrentAfterRemovalVoters ELSE CurrentAfterAdditionVoters
OldPeers(s) == OldVoters(s) \ {Owner}
SlowQuorum(s) == (Cardinality(OldVoters(s)) \div 2) + 1
CurrentSlowQuorum(s) == (Cardinality(CurrentVoters(s)) \div 2) + 1
CountedVotes(s) == {Owner} \cup votes
LostResponder(s) == IF s \in RemovalScenarios THEN RemovedVoter ELSE 2

Vars == <<scenario, stage, phase, votes, retryTargets, depsWidth,
          lostAttempted, retryBroadcasted, addedResponseRejected,
          advanced, committed, durableWrites, commandApplied>>

TypeOK ==
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ phase \in Phases
    /\ votes \subseteq AllVoters
    /\ retryTargets \subseteq AllVoters
    /\ depsWidth \in 0..4
    /\ lostAttempted \in BOOLEAN
    /\ retryBroadcasted \in BOOLEAN
    /\ addedResponseRejected \in BOOLEAN
    /\ advanced \in BOOLEAN
    /\ committed \in BOOLEAN
    /\ durableWrites \in 0..2
    /\ commandApplied \in BOOLEAN

Init ==
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ phase = IF scenario \in PreAcceptScenarios THEN "preaccept" ELSE "accept"
    /\ votes = {}
    /\ retryTargets = {}
    /\ depsWidth = 0
    /\ lostAttempted = FALSE
    /\ retryBroadcasted = FALSE
    /\ addedResponseRejected = FALSE
    /\ advanced = FALSE
    /\ committed = FALSE
    /\ durableWrites = 0
    /\ commandApplied = FALSE

FirstBelowOldQuorum ==
    /\ stage = "start"
    /\ IF scenario \in RemovalScenarios THEN
          /\ votes' = {2}
          /\ addedResponseRejected' = FALSE
       ELSE
          /\ votes' = {}
          /\ AddedVoter \notin OldVoters(scenario)
          /\ addedResponseRejected' = TRUE
    /\ Cardinality(CountedVotes(scenario)') < SlowQuorum(scenario)
    /\ stage' = "below-quorum"
    /\ advanced' = FALSE
    /\ committed' = FALSE
    /\ durableWrites' = 0
    /\ commandApplied' = FALSE
    /\ UNCHANGED <<scenario, phase, retryTargets, depsWidth,
                  lostAttempted, retryBroadcasted>>

LostNeededResponseBeforeRetry ==
    /\ stage = "below-quorum"
    /\ LostResponder(scenario) \notin votes
    /\ lostAttempted' = TRUE
    /\ stage' = "lost-before-retry"
    /\ UNCHANGED <<scenario, phase, votes, retryTargets, depsWidth,
                  retryBroadcasted, addedResponseRejected,
                  advanced, committed, durableWrites, commandApplied>>

RetryOldTransition ==
    /\ stage = "lost-before-retry"
    /\ lostAttempted
    /\ Cardinality(CountedVotes(scenario)) < SlowQuorum(scenario)
    /\ stage' = "retried"
    /\ retryTargets' = OldPeers(scenario)
    /\ depsWidth' = Cardinality(OldVoters(scenario))
    /\ retryBroadcasted' = TRUE
    /\ advanced' = FALSE
    /\ committed' = FALSE
    /\ durableWrites' = 0
    /\ commandApplied' = FALSE
    /\ UNCHANGED <<scenario, phase, votes, lostAttempted,
                  addedResponseRejected>>

ReplacementResponseAfterRetry ==
    /\ stage = "retried"
    /\ retryBroadcasted
    /\ votes' = votes \cup {LostResponder(scenario)}
    /\ Cardinality(CountedVotes(scenario)') = SlowQuorum(scenario)
    /\ stage' = "done"
    /\ advanced' = TRUE
    /\ committed' = (scenario \in AcceptScenarios)
    /\ durableWrites' = IF scenario \in PreAcceptScenarios THEN 1 ELSE 2
    /\ commandApplied' = (scenario \in AcceptScenarios)
    /\ UNCHANGED <<scenario, phase, retryTargets, depsWidth,
                  lostAttempted, retryBroadcasted, addedResponseRejected>>

Next == FirstBelowOldQuorum \/ LostNeededResponseBeforeRetry \/ RetryOldTransition \/ ReplacementResponseAfterRetry

VotesStayOldPinned ==
    votes \subseteq OldPeers(scenario)

LostResponseDoesNotCountBeforeRetry ==
    lostAttempted /\ ~retryBroadcasted =>
        /\ LostResponder(scenario) \notin votes
        /\ Cardinality(CountedVotes(scenario)) < SlowQuorum(scenario)
        /\ ~advanced
        /\ ~committed
        /\ durableWrites = 0
        /\ ~commandApplied

RetryTargetsPinnedToOldConfig ==
    retryBroadcasted =>
        /\ retryTargets = OldPeers(scenario)
        /\ retryTargets \subseteq OldVoters(scenario)
        /\ Owner \notin retryTargets
        /\ depsWidth = Cardinality(OldVoters(scenario))

RemovalRetryStillIncludesRemovedOldVoter ==
    scenario \in RemovalScenarios /\ retryBroadcasted =>
        /\ RemovedVoter \in retryTargets
        /\ RemovedVoter \notin CurrentVoters(scenario)

AdditionRetryExcludesAddedCurrentVoter ==
    scenario \in AdditionScenarios =>
        /\ AddedVoter \notin votes
        /\ (retryBroadcasted => AddedVoter \notin retryTargets)
        /\ AddedVoter \in CurrentVoters(scenario)

CurrentRemovalQuorumCannotAdvanceOldInstance ==
    scenario \in RemovalScenarios
    /\ Cardinality(CountedVotes(scenario) \cap CurrentVoters(scenario)) >= CurrentSlowQuorum(scenario)
    /\ Cardinality(CountedVotes(scenario)) < SlowQuorum(scenario) =>
        /\ ~advanced
        /\ ~committed
        /\ durableWrites = 0
        /\ ~commandApplied

RetryDoesNotMutateDurableOrApplicationState ==
    stage = "retried" =>
        /\ ~advanced
        /\ ~committed
        /\ durableWrites = 0
        /\ ~commandApplied

ReplacementAdvancesOnlyAtOldQuorum ==
    stage = "done" =>
        /\ Cardinality(CountedVotes(scenario)) = SlowQuorum(scenario)
        /\ LostResponder(scenario) \in votes
        /\ advanced
        /\ (scenario \in PreAcceptScenarios =>
              /\ phase = "preaccept"
              /\ ~committed
              /\ durableWrites = 1
              /\ ~commandApplied)
        /\ (scenario \in AcceptScenarios =>
              /\ phase = "accept"
              /\ committed
              /\ durableWrites = 2
              /\ commandApplied)

Safety ==
    /\ VotesStayOldPinned
    /\ LostResponseDoesNotCountBeforeRetry
    /\ RetryTargetsPinnedToOldConfig
    /\ RemovalRetryStillIncludesRemovedOldVoter
    /\ AdditionRetryExcludesAddedCurrentVoter
    /\ CurrentRemovalQuorumCannotAdvanceOldInstance
    /\ RetryDoesNotMutateDurableOrApplicationState
    /\ ReplacementAdvancesOnlyAtOldQuorum

LossScenarioCovered ==
    CASE scenario = "removal_preaccept_loss" ->
            /\ stage = "done"
            /\ phase = "preaccept"
            /\ votes = {2, 4}
            /\ retryTargets = {2, 3, 4}
            /\ depsWidth = 4
            /\ lostAttempted
            /\ retryBroadcasted
            /\ advanced
            /\ ~committed
      [] scenario = "removal_accept_loss" ->
            /\ stage = "done"
            /\ phase = "accept"
            /\ votes = {2, 4}
            /\ retryTargets = {2, 3, 4}
            /\ depsWidth = 4
            /\ lostAttempted
            /\ retryBroadcasted
            /\ advanced
            /\ committed
      [] scenario = "addition_preaccept_loss" ->
            /\ stage = "done"
            /\ phase = "preaccept"
            /\ votes = {2}
            /\ retryTargets = {2, 3}
            /\ depsWidth = 3
            /\ lostAttempted
            /\ retryBroadcasted
            /\ addedResponseRejected
            /\ advanced
            /\ ~committed
      [] scenario = "addition_accept_loss" ->
            /\ stage = "done"
            /\ phase = "accept"
            /\ votes = {2}
            /\ retryTargets = {2, 3}
            /\ depsWidth = 3
            /\ lostAttempted
            /\ retryBroadcasted
            /\ addedResponseRejected
            /\ advanced
            /\ committed

EventuallyCoversConfigTransitionLostResponseRetry == <>LossScenarioCovered

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
