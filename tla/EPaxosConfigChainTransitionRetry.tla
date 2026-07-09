---- MODULE EPaxosConfigChainTransitionRetry ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite add-then-remove configuration-chain transition retry       *)
(* model. It checks normal local-owner old and mid-chain instances after    *)
(* Conf1 {1,2,3} adds voter 4 and Conf2 {1,2,3,4} later removes voter 2,    *)
(* leaving Conf3 {1,3,4} current. Logical PreAccept/Accept retries must     *)
(* stay pinned to each instance Ref.Conf: old Conf1 retries target {2,3}    *)
(* with dependency width 3, while mid Conf2 retries target {2,3,4} with     *)
(* dependency width 4 including removed voter 2. This does not model        *)
(* arbitrary retry histories, message loss, joint consensus, arbitrary      *)
(* membership histories, durable replay, or unbounded proof.                *)
(***************************************************************************)

VARIABLES scenario, stage, phase, currentConf, retryTargets, depsWidth,
          retryScheduled, durableRecordWritten, commandApplied

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

OldPreAcceptScenarios == {"old_preaccept_retry"}
OldAcceptScenarios == {"old_accept_retry"}
MidPreAcceptScenarios == {"mid_preaccept_retry"}
MidAcceptScenarios == {"mid_accept_retry"}
OldScenarios == OldPreAcceptScenarios \cup OldAcceptScenarios
MidScenarios == MidPreAcceptScenarios \cup MidAcceptScenarios
PreAcceptScenarios == OldPreAcceptScenarios \cup MidPreAcceptScenarios
AcceptScenarios == OldAcceptScenarios \cup MidAcceptScenarios
Scenarios == OldScenarios \cup MidScenarios
Stages == {"start", "done"}
Phases == {"preaccept", "accept"}

RefConf(s) == IF s \in OldScenarios THEN Conf1 ELSE Conf2
VotersFor(c) == CASE c = Conf1 -> Voters1 [] c = Conf2 -> Voters2 [] c = Conf3 -> Voters3
PeersFor(c) == VotersFor(c) \ {Owner}

Vars == <<scenario, stage, phase, currentConf, retryTargets, depsWidth,
          retryScheduled, durableRecordWritten, commandApplied>>

TypeOK ==
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ phase \in Phases
    /\ currentConf \in {Conf1, Conf2, Conf3}
    /\ retryTargets \subseteq AllVoters
    /\ depsWidth \in 0..4
    /\ retryScheduled \in BOOLEAN
    /\ durableRecordWritten \in BOOLEAN
    /\ commandApplied \in BOOLEAN

Init ==
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ phase = IF scenario \in PreAcceptScenarios THEN "preaccept" ELSE "accept"
    /\ currentConf = Conf3
    /\ retryTargets = {}
    /\ depsWidth = 0
    /\ retryScheduled = FALSE
    /\ durableRecordWritten = FALSE
    /\ commandApplied = FALSE

RetryOldOrMidTransition ==
    /\ stage = "start"
    /\ currentConf = Conf3
    /\ stage' = "done"
    /\ retryTargets' = PeersFor(RefConf(scenario))
    /\ depsWidth' = Cardinality(VotersFor(RefConf(scenario)))
    /\ retryScheduled' = TRUE
    /\ durableRecordWritten' = FALSE
    /\ commandApplied' = FALSE
    /\ UNCHANGED <<scenario, phase, currentConf>>

Next == RetryOldOrMidTransition

CurrentConfigIsFinalChain ==
    /\ currentConf = Conf3
    /\ AddedVoter \in Voters3
    /\ RemovedVoter \notin Voters3

RetryTargetsPinnedToRefConf ==
    retryScheduled =>
        /\ retryTargets = PeersFor(RefConf(scenario))
        /\ retryTargets \subseteq VotersFor(RefConf(scenario))
        /\ Owner \notin retryTargets
        /\ depsWidth = Cardinality(VotersFor(RefConf(scenario)))

OldConf1RetryExcludesAddedVoter ==
    scenario \in OldScenarios /\ retryScheduled =>
        /\ retryTargets = {2, 3}
        /\ AddedVoter \notin retryTargets
        /\ RemovedVoter \in retryTargets
        /\ depsWidth = Cardinality(Voters1)

MidConf2RetryIncludesAddedAndRemovedVoters ==
    scenario \in MidScenarios /\ retryScheduled =>
        /\ retryTargets = {2, 3, 4}
        /\ AddedVoter \in retryTargets
        /\ RemovedVoter \in retryTargets
        /\ RemovedVoter \notin Voters3
        /\ depsWidth = Cardinality(Voters2)

RetryDoesNotMutateDurableOrApplicationState ==
    retryScheduled =>
        /\ ~durableRecordWritten
        /\ ~commandApplied

TimerKindPreservesPhase ==
    stage = "done" =>
        /\ (scenario \in PreAcceptScenarios => phase = "preaccept")
        /\ (scenario \in AcceptScenarios => phase = "accept")

Safety ==
    /\ CurrentConfigIsFinalChain
    /\ RetryTargetsPinnedToRefConf
    /\ OldConf1RetryExcludesAddedVoter
    /\ MidConf2RetryIncludesAddedAndRemovedVoters
    /\ RetryDoesNotMutateDurableOrApplicationState
    /\ TimerKindPreservesPhase

RetryScenarioCovered ==
    CASE scenario = "old_preaccept_retry" ->
            /\ stage = "done"
            /\ phase = "preaccept"
            /\ retryTargets = {2, 3}
            /\ depsWidth = 3
      [] scenario = "old_accept_retry" ->
            /\ stage = "done"
            /\ phase = "accept"
            /\ retryTargets = {2, 3}
            /\ depsWidth = 3
      [] scenario = "mid_preaccept_retry" ->
            /\ stage = "done"
            /\ phase = "preaccept"
            /\ retryTargets = {2, 3, 4}
            /\ depsWidth = 4
      [] scenario = "mid_accept_retry" ->
            /\ stage = "done"
            /\ phase = "accept"
            /\ retryTargets = {2, 3, 4}
            /\ depsWidth = 4

EventuallyCoversConfigChainTransitionRetry == <>RetryScenarioCovered

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
