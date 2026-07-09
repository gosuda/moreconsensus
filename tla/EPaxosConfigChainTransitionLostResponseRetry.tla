---- MODULE EPaxosConfigChainTransitionLostResponseRetry ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite add-then-remove configuration-chain transition lost-       *)
(* response retry model. It checks normal local-owner old and mid-chain      *)
(* instances after Conf1 {1,2,3} adds voter 4 and Conf2 {1,2,3,4} later      *)
(* removes voter 2, leaving Conf3 {1,3,4} current. One needed old-config     *)
(* response is explicitly lost before deterministic retry; old Conf1         *)
(* PreAccept/Accept retries stay pinned to {2,3} with dependency width 3,    *)
(* while mid Conf2 retries stay pinned to {2,3,4} with dependency width 4    *)
(* including removed voter 2. This does not model arbitrary message loss,    *)
(* arbitrary retry histories, joint consensus, arbitrary membership          *)
(* histories, durable replay, or unbounded proof.                            *)
(***************************************************************************)

VARIABLES scenario, stage, phase, currentConf, votes, retryTargets, depsWidth,
          addedCurrentResponseRejected, lostAttempted, retryBroadcasted,
          advanced, committed, durableWrites, commandApplied

Conf1 == 1
Conf2 == 2
Conf3 == 3
Owner == 1
AddedVoter == 4
RemovedVoter == 2

Voters1 == {1, 2, 3}
Voters2 == Voters1 \cup {AddedVoter}
Voters3 == Voters2 \ {RemovedVoter}
AllVoters == Voters2

OldPreAcceptScenarios == {"old_preaccept_loss"}
OldAcceptScenarios == {"old_accept_loss"}
MidPreAcceptScenarios == {"mid_preaccept_loss"}
MidAcceptScenarios == {"mid_accept_loss"}
OldScenarios == OldPreAcceptScenarios \cup OldAcceptScenarios
MidScenarios == MidPreAcceptScenarios \cup MidAcceptScenarios
PreAcceptScenarios == OldPreAcceptScenarios \cup MidPreAcceptScenarios
AcceptScenarios == OldAcceptScenarios \cup MidAcceptScenarios
Scenarios == OldScenarios \cup MidScenarios
Stages == {"start", "below-quorum", "lost-before-retry", "retried", "done"}
Phases == {"preaccept", "accept"}

RefConf(s) == IF s \in OldScenarios THEN Conf1 ELSE Conf2
VotersFor(c) == CASE c = Conf1 -> Voters1 [] c = Conf2 -> Voters2 [] c = Conf3 -> Voters3
PeersFor(c) == VotersFor(c) \ {Owner}
SlowQuorum(c) == (Cardinality(VotersFor(c)) \div 2) + 1
CurrentSlowQuorum == (Cardinality(Voters3) \div 2) + 1
CountedVotes == {Owner} \cup votes
LostResponder == RemovedVoter
AttemptedCurrentVotes == IF scenario \in OldScenarios THEN {Owner, AddedVoter} ELSE CountedVotes

Vars == <<scenario, stage, phase, currentConf, votes, retryTargets, depsWidth,
          addedCurrentResponseRejected, lostAttempted, retryBroadcasted,
          advanced, committed, durableWrites, commandApplied>>

TypeOK ==
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ phase \in Phases
    /\ currentConf \in {Conf1, Conf2, Conf3}
    /\ votes \subseteq AllVoters
    /\ retryTargets \subseteq AllVoters
    /\ depsWidth \in 0..4
    /\ addedCurrentResponseRejected \in BOOLEAN
    /\ lostAttempted \in BOOLEAN
    /\ retryBroadcasted \in BOOLEAN
    /\ advanced \in BOOLEAN
    /\ committed \in BOOLEAN
    /\ durableWrites \in 0..2
    /\ commandApplied \in BOOLEAN

Init ==
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ phase = IF scenario \in PreAcceptScenarios THEN "preaccept" ELSE "accept"
    /\ currentConf = Conf3
    /\ votes = {}
    /\ retryTargets = {}
    /\ depsWidth = 0
    /\ addedCurrentResponseRejected = FALSE
    /\ lostAttempted = FALSE
    /\ retryBroadcasted = FALSE
    /\ advanced = FALSE
    /\ committed = FALSE
    /\ durableWrites = 0
    /\ commandApplied = FALSE

FirstBelowOldQuorum ==
    /\ stage = "start"
    /\ currentConf = Conf3
    /\ IF scenario \in OldScenarios THEN
          /\ votes' = {}
          /\ AddedVoter \notin VotersFor(RefConf(scenario))
          /\ addedCurrentResponseRejected' = TRUE
       ELSE
          /\ votes' = {AddedVoter}
          /\ AddedVoter \in VotersFor(RefConf(scenario))
          /\ addedCurrentResponseRejected' = FALSE
    /\ Cardinality({Owner} \cup votes') < SlowQuorum(RefConf(scenario))
    /\ stage' = "below-quorum"
    /\ advanced' = FALSE
    /\ committed' = FALSE
    /\ durableWrites' = 0
    /\ commandApplied' = FALSE
    /\ UNCHANGED <<scenario, phase, currentConf, retryTargets, depsWidth,
                  lostAttempted, retryBroadcasted>>

LostNeededResponseBeforeRetry ==
    /\ stage = "below-quorum"
    /\ LostResponder \notin votes
    /\ LostResponder \in PeersFor(RefConf(scenario))
    /\ lostAttempted' = TRUE
    /\ stage' = "lost-before-retry"
    /\ UNCHANGED <<scenario, phase, currentConf, votes, retryTargets, depsWidth,
                  addedCurrentResponseRejected, retryBroadcasted,
                  advanced, committed, durableWrites, commandApplied>>

RetryOldOrMidTransition ==
    /\ stage = "lost-before-retry"
    /\ lostAttempted
    /\ Cardinality(CountedVotes) < SlowQuorum(RefConf(scenario))
    /\ stage' = "retried"
    /\ retryTargets' = PeersFor(RefConf(scenario))
    /\ depsWidth' = Cardinality(VotersFor(RefConf(scenario)))
    /\ retryBroadcasted' = TRUE
    /\ advanced' = FALSE
    /\ committed' = FALSE
    /\ durableWrites' = 0
    /\ commandApplied' = FALSE
    /\ UNCHANGED <<scenario, phase, currentConf, votes,
                  addedCurrentResponseRejected, lostAttempted>>

ReplacementResponseAfterRetry ==
    /\ stage = "retried"
    /\ retryBroadcasted
    /\ votes' = votes \cup {LostResponder}
    /\ Cardinality({Owner} \cup votes') = SlowQuorum(RefConf(scenario))
    /\ stage' = "done"
    /\ advanced' = TRUE
    /\ committed' = (scenario \in AcceptScenarios)
    /\ durableWrites' = IF scenario \in PreAcceptScenarios THEN 1 ELSE 2
    /\ commandApplied' = (scenario \in AcceptScenarios)
    /\ UNCHANGED <<scenario, phase, currentConf, retryTargets, depsWidth,
                  addedCurrentResponseRejected, lostAttempted, retryBroadcasted>>

Next == FirstBelowOldQuorum \/ LostNeededResponseBeforeRetry \/ RetryOldOrMidTransition \/ ReplacementResponseAfterRetry

CurrentConfigIsFinalChain ==
    /\ currentConf = Conf3
    /\ AddedVoter \in Voters3
    /\ RemovedVoter \notin Voters3

VotesStayRefPinned ==
    votes \subseteq PeersFor(RefConf(scenario))

LostResponseDoesNotCountBeforeRetry ==
    lostAttempted /\ ~retryBroadcasted =>
        /\ LostResponder \notin votes
        /\ Cardinality(CountedVotes) < SlowQuorum(RefConf(scenario))
        /\ ~advanced
        /\ ~committed
        /\ durableWrites = 0
        /\ ~commandApplied

RetryTargetsPinnedToRefConf ==
    retryBroadcasted =>
        /\ retryTargets = PeersFor(RefConf(scenario))
        /\ retryTargets \subseteq VotersFor(RefConf(scenario))
        /\ Owner \notin retryTargets
        /\ depsWidth = Cardinality(VotersFor(RefConf(scenario)))

OldConf1RetryExcludesAddedVoter ==
    scenario \in OldScenarios /\ retryBroadcasted =>
        /\ retryTargets = {2, 3}
        /\ AddedVoter \notin retryTargets
        /\ RemovedVoter \in retryTargets
        /\ addedCurrentResponseRejected
        /\ depsWidth = Cardinality(Voters1)

MidConf2RetryIncludesAddedAndRemovedVoters ==
    scenario \in MidScenarios /\ retryBroadcasted =>
        /\ retryTargets = {2, 3, 4}
        /\ AddedVoter \in retryTargets
        /\ RemovedVoter \in retryTargets
        /\ RemovedVoter \notin Voters3
        /\ depsWidth = Cardinality(Voters2)

CurrentConfQuorumCannotAdvancePinnedInstance ==
    Cardinality(AttemptedCurrentVotes \cap Voters3) >= CurrentSlowQuorum /\
    Cardinality(CountedVotes) < SlowQuorum(RefConf(scenario)) =>
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

ReplacementAdvancesOnlyAtRefQuorum ==
    stage = "done" =>
        /\ Cardinality(CountedVotes) = SlowQuorum(RefConf(scenario))
        /\ LostResponder \in votes
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
    /\ CurrentConfigIsFinalChain
    /\ VotesStayRefPinned
    /\ LostResponseDoesNotCountBeforeRetry
    /\ RetryTargetsPinnedToRefConf
    /\ OldConf1RetryExcludesAddedVoter
    /\ MidConf2RetryIncludesAddedAndRemovedVoters
    /\ CurrentConfQuorumCannotAdvancePinnedInstance
    /\ RetryDoesNotMutateDurableOrApplicationState
    /\ ReplacementAdvancesOnlyAtRefQuorum

LossScenarioCovered ==
    CASE scenario = "old_preaccept_loss" ->
            /\ stage = "done"
            /\ phase = "preaccept"
            /\ votes = {2}
            /\ retryTargets = {2, 3}
            /\ depsWidth = 3
            /\ addedCurrentResponseRejected
            /\ lostAttempted
            /\ retryBroadcasted
            /\ advanced
            /\ ~committed
      [] scenario = "old_accept_loss" ->
            /\ stage = "done"
            /\ phase = "accept"
            /\ votes = {2}
            /\ retryTargets = {2, 3}
            /\ depsWidth = 3
            /\ addedCurrentResponseRejected
            /\ lostAttempted
            /\ retryBroadcasted
            /\ advanced
            /\ committed
      [] scenario = "mid_preaccept_loss" ->
            /\ stage = "done"
            /\ phase = "preaccept"
            /\ votes = {2, 4}
            /\ retryTargets = {2, 3, 4}
            /\ depsWidth = 4
            /\ ~addedCurrentResponseRejected
            /\ lostAttempted
            /\ retryBroadcasted
            /\ advanced
            /\ ~committed
      [] scenario = "mid_accept_loss" ->
            /\ stage = "done"
            /\ phase = "accept"
            /\ votes = {2, 4}
            /\ retryTargets = {2, 3, 4}
            /\ depsWidth = 4
            /\ ~addedCurrentResponseRejected
            /\ lostAttempted
            /\ retryBroadcasted
            /\ advanced
            /\ committed

EventuallyCoversConfigChainTransitionLostResponseRetry == <>LossScenarioCovered

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
