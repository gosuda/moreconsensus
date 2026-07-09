---- MODULE EPaxosConfigTransitionDedup ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite configuration-transition response de-duplication model.    *)
(* It checks normal local-owner old instances after voter removal/addition.  *)
(* The model records only remote responses; the local owner vote is always   *)
(* counted separately, matching the Go proposal/accept paths. Duplicate old  *)
(* responses and added-current-voter responses stay below old pinned quorums *)
(* until a second distinct old remote response is counted. This does not     *)
(* model arbitrary histories, joint consensus, or unbounded changes.         *)
(***************************************************************************)

VARIABLES scenario, stage, phase, votes, advanced, committed, durableWrites, commandApplied

Owner == 1
RemovedVoter == 4
AddedVoter == 4
OldRemovalVoters == {1, 2, 3, 4}
CurrentAfterRemovalVoters == {1, 2, 3}
OldAdditionVoters == {1, 2, 3}
CurrentAfterAdditionVoters == {1, 2, 3, 4}

PreAcceptScenarios == {"removal_preaccept_dedup", "addition_preaccept_dedup"}
AcceptScenarios == {"removal_accept_dedup", "addition_accept_dedup"}
RemovalScenarios == {"removal_preaccept_dedup", "removal_accept_dedup"}
AdditionScenarios == {"addition_preaccept_dedup", "addition_accept_dedup"}
Scenarios == PreAcceptScenarios \cup AcceptScenarios
Stages == {"start", "duplicate_seen", "below_quorum", "done"}
Phases == {"preaccept", "accept"}
AllVoters == {1, 2, 3, 4}

OldVoters(s) == IF s \in RemovalScenarios THEN OldRemovalVoters ELSE OldAdditionVoters
CurrentVoters(s) == IF s \in RemovalScenarios THEN CurrentAfterRemovalVoters ELSE CurrentAfterAdditionVoters
SlowQuorum(s) == (Cardinality(OldVoters(s)) \div 2) + 1
OldRemoteVotes(s) == OldVoters(s) \ {Owner}
CountedVotes(s) == {Owner} \cup votes

Vars == <<scenario, stage, phase, votes, advanced, committed, durableWrites, commandApplied>>

TypeOK ==
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ phase \in Phases
    /\ votes \subseteq AllVoters
    /\ advanced \in BOOLEAN
    /\ committed \in BOOLEAN
    /\ durableWrites \in 0..2
    /\ commandApplied \in BOOLEAN

Init ==
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ phase = IF scenario \in PreAcceptScenarios THEN "preaccept" ELSE "accept"
    /\ votes = {}
    /\ advanced = FALSE
    /\ committed = FALSE
    /\ durableWrites = 0
    /\ commandApplied = FALSE

DuplicateOrAddedRemoteBelowQuorum ==
    /\ stage = "start"
    /\ IF scenario \in RemovalScenarios THEN
          /\ votes' = {2}
          /\ Cardinality(CountedVotes(scenario)') < SlowQuorum(scenario)
       ELSE
          /\ votes' = {}
          /\ AddedVoter \notin OldVoters(scenario)
          /\ Cardinality(CountedVotes(scenario)') < SlowQuorum(scenario)
    /\ stage' = "duplicate_seen"
    /\ advanced' = FALSE
    /\ committed' = FALSE
    /\ durableWrites' = 0
    /\ commandApplied' = FALSE
    /\ UNCHANGED <<scenario, phase>>

StillBelowOldQuorum ==
    /\ stage = "duplicate_seen"
    /\ IF scenario \in RemovalScenarios THEN
          /\ votes' = {2}
          /\ RemovedVoter \notin votes'
          /\ Cardinality(CountedVotes(scenario)') < SlowQuorum(scenario)
       ELSE
          /\ votes' = {}
          /\ AddedVoter \notin votes'
          /\ Cardinality(CountedVotes(scenario)') < SlowQuorum(scenario)
    /\ stage' = "below_quorum"
    /\ advanced' = FALSE
    /\ committed' = FALSE
    /\ durableWrites' = 0
    /\ commandApplied' = FALSE
    /\ UNCHANGED <<scenario, phase>>

ReachOldQuorum ==
    /\ stage = "below_quorum"
    /\ IF scenario \in RemovalScenarios THEN
          /\ votes' = {2, 3}
          /\ RemovedVoter \notin votes'
       ELSE
          /\ votes' = {2}
          /\ AddedVoter \notin votes'
    /\ Cardinality(CountedVotes(scenario)') = SlowQuorum(scenario)
    /\ stage' = "done"
    /\ advanced' = TRUE
    /\ committed' = (scenario \in AcceptScenarios)
    /\ durableWrites' = IF scenario \in PreAcceptScenarios THEN 1 ELSE 2
    /\ commandApplied' = (scenario \in AcceptScenarios)
    /\ UNCHANGED <<scenario, phase>>

Next == DuplicateOrAddedRemoteBelowQuorum \/ StillBelowOldQuorum \/ ReachOldQuorum

VotesStayOldPinned ==
    votes \subseteq OldRemoteVotes(scenario)

DuplicateOrAddedVoteDoesNotAdvance ==
    stage \in {"duplicate_seen", "below_quorum"} =>
        /\ ~advanced
        /\ ~committed
        /\ durableWrites = 0
        /\ ~commandApplied

RemovalDuplicateRemoteStaysBelowOldQuorum ==
    scenario \in RemovalScenarios /\ stage = "below_quorum" =>
        /\ votes = {2}
        /\ Cardinality(CountedVotes(scenario)) < SlowQuorum(scenario)

RemovalSurvivingOldRemoteAdvancesOldQuorum ==
    scenario \in RemovalScenarios /\ stage = "done" =>
        /\ votes = {2, 3}
        /\ RemovedVoter \notin votes
        /\ advanced

AdditionAddedVoterNeverCounts ==
    scenario \in AdditionScenarios =>
        /\ AddedVoter \notin votes
        /\ AddedVoter \in CurrentVoters(scenario)

DistinctOldRemoteResponsesAdvanceOnlyAtOldQuorum ==
    stage = "done" =>
        /\ Cardinality(CountedVotes(scenario)) = SlowQuorum(scenario)
        /\ advanced
        /\ (scenario \in PreAcceptScenarios => phase = "preaccept" /\ ~committed /\ durableWrites = 1 /\ ~commandApplied)
        /\ (scenario \in AcceptScenarios => phase = "accept" /\ committed /\ durableWrites = 2 /\ commandApplied)

Safety ==
    /\ VotesStayOldPinned
    /\ DuplicateOrAddedVoteDoesNotAdvance
    /\ RemovalDuplicateRemoteStaysBelowOldQuorum
    /\ RemovalSurvivingOldRemoteAdvancesOldQuorum
    /\ AdditionAddedVoterNeverCounts
    /\ DistinctOldRemoteResponsesAdvanceOnlyAtOldQuorum

DedupScenarioCovered ==
    CASE scenario = "removal_preaccept_dedup" ->
            stage = "done" /\ phase = "preaccept" /\ votes = {2, 3} /\ advanced /\ ~committed
      [] scenario = "removal_accept_dedup" ->
            stage = "done" /\ phase = "accept" /\ votes = {2, 3} /\ advanced /\ committed
      [] scenario = "addition_preaccept_dedup" ->
            stage = "done" /\ phase = "preaccept" /\ votes = {2} /\ advanced /\ ~committed
      [] scenario = "addition_accept_dedup" ->
            stage = "done" /\ phase = "accept" /\ votes = {2} /\ advanced /\ committed

EventuallyCoversConfigTransitionDedup == <>DedupScenarioCovered

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
