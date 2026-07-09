---- MODULE EPaxosConfigRemoveTransition ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite remove-voter configuration-transition model. It checks one *)
(* old four-voter configuration, one chosen/executed configuration command   *)
(* that removes voter 4 and installs a three-voter successor, one user       *)
(* instance already in flight under the old configuration, and one later     *)
(* user instance under the new configuration. The old in-flight instance     *)
(* remains pinned to the old dependency domain and can still count the       *)
(* removed voter for its old quorum; the later instance excludes that voter  *)
(* and uses the smaller new quorum. It does not model joint consensus,       *)
(* concurrent configuration commands, recovery, message loss, durable replay,*)
(* or unbounded configuration histories.                                    *)
(***************************************************************************)

VARIABLES stage, currentConf, status, pinnedConf, quorumVotes, depsDomain

OldConf == 1
NewConf == 2
NoConf == 0

ConfigCmd == "config-cmd"
OldUser == "old-user"
NewUser == "new-user"
Instances == {ConfigCmd, OldUser, NewUser}

OldVoters == {1, 2, 3, 4}
RemovedVoter == 4
NewVoters == OldVoters \ {RemovedVoter}
AllReplicas == OldVoters
ConfigIDs == {OldConf, NewConf}
Statuses == {"none", "inflight", "chosen", "executed"}

Vars == <<stage, currentConf, status, pinnedConf, quorumVotes, depsDomain>>

VotersFor(c) ==
    CASE c = OldConf -> OldVoters
      [] c = NewConf -> NewVoters
      [] OTHER -> {}

SlowQuorum(c) == (Cardinality(VotersFor(c)) \div 2) + 1

QuorumFor(c, q) ==
    /\ c \in ConfigIDs
    /\ q \subseteq VotersFor(c)
    /\ Cardinality(q) = SlowQuorum(c)

QuorumSets(c) == {q \in SUBSET VotersFor(c) : Cardinality(q) = SlowQuorum(c)}

OldQuorumsWithRemoved == {q \in QuorumSets(OldConf) : RemovedVoter \in q}

TypeOK ==
    /\ stage \in 0..6
    /\ currentConf \in ConfigIDs
    /\ status \in [Instances -> Statuses]
    /\ pinnedConf \in [Instances -> ConfigIDs \cup {NoConf}]
    /\ quorumVotes \in [Instances -> SUBSET AllReplicas]
    /\ depsDomain \in [Instances -> SUBSET AllReplicas]

Init ==
    /\ stage = 0
    /\ currentConf = OldConf
    /\ status = [i \in Instances |-> "none"]
    /\ pinnedConf = [i \in Instances |-> NoConf]
    /\ quorumVotes = [i \in Instances |-> {}]
    /\ depsDomain = [i \in Instances |-> {}]

StartOldUser ==
    /\ stage = 0
    /\ status[OldUser] = "none"
    /\ stage' = 1
    /\ status' = [status EXCEPT ![OldUser] = "inflight"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![OldUser] = OldConf]
    /\ depsDomain' = [depsDomain EXCEPT ![OldUser] = OldVoters]
    /\ UNCHANGED <<currentConf, quorumVotes>>

ChooseConfig(q) ==
    /\ stage = 1
    /\ q \in QuorumSets(OldConf)
    /\ status[ConfigCmd] = "none"
    /\ stage' = 2
    /\ status' = [status EXCEPT ![ConfigCmd] = "chosen"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![ConfigCmd] = OldConf]
    /\ depsDomain' = [depsDomain EXCEPT ![ConfigCmd] = OldVoters]
    /\ quorumVotes' = [quorumVotes EXCEPT ![ConfigCmd] = q]
    /\ UNCHANGED currentConf

ExecuteConfig ==
    /\ stage = 2
    /\ status[ConfigCmd] = "chosen"
    /\ stage' = 3
    /\ currentConf' = NewConf
    /\ status' = [status EXCEPT ![ConfigCmd] = "executed"]
    /\ UNCHANGED <<pinnedConf, quorumVotes, depsDomain>>

ChooseOldUser(q) ==
    /\ stage = 3
    /\ currentConf = NewConf
    /\ status[OldUser] = "inflight"
    /\ q \in OldQuorumsWithRemoved
    /\ stage' = 4
    /\ status' = [status EXCEPT ![OldUser] = "chosen"]
    /\ quorumVotes' = [quorumVotes EXCEPT ![OldUser] = q]
    /\ UNCHANGED <<currentConf, pinnedConf, depsDomain>>

StartNewUser ==
    /\ stage = 4
    /\ currentConf = NewConf
    /\ status[ConfigCmd] = "executed"
    /\ status[NewUser] = "none"
    /\ stage' = 5
    /\ status' = [status EXCEPT ![NewUser] = "inflight"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![NewUser] = NewConf]
    /\ depsDomain' = [depsDomain EXCEPT ![NewUser] = NewVoters]
    /\ UNCHANGED <<currentConf, quorumVotes>>

ChooseNewUser(q) ==
    /\ stage = 5
    /\ currentConf = NewConf
    /\ status[NewUser] = "inflight"
    /\ q \in QuorumSets(NewConf)
    /\ stage' = 6
    /\ status' = [status EXCEPT ![NewUser] = "chosen"]
    /\ quorumVotes' = [quorumVotes EXCEPT ![NewUser] = q]
    /\ UNCHANGED <<currentConf, pinnedConf, depsDomain>>

Next ==
    \/ StartOldUser
    \/ \E q \in QuorumSets(OldConf) : ChooseConfig(q)
    \/ ExecuteConfig
    \/ \E q \in OldQuorumsWithRemoved : ChooseOldUser(q)
    \/ StartNewUser
    \/ \E q \in QuorumSets(NewConf) : ChooseNewUser(q)

InactiveInstancesAreEmpty ==
    \A i \in Instances :
        status[i] = "none" =>
            /\ pinnedConf[i] = NoConf
            /\ quorumVotes[i] = {}
            /\ depsDomain[i] = {}

ActiveInstancesUsePinnedDependencyDomain ==
    \A i \in Instances :
        status[i] # "none" =>
            /\ pinnedConf[i] \in ConfigIDs
            /\ depsDomain[i] = VotersFor(pinnedConf[i])

ChosenInstancesUsePinnedQuorum ==
    \A i \in Instances :
        status[i] \in {"chosen", "executed"} => QuorumFor(pinnedConf[i], quorumVotes[i])

ConfigCommandChosenAndExecutedUnderOldConfig ==
    status[ConfigCmd] # "none" =>
        /\ pinnedConf[ConfigCmd] = OldConf
        /\ depsDomain[ConfigCmd] = OldVoters
        /\ RemovedVoter \in depsDomain[ConfigCmd]

ExecutedConfigInstallsOnlyNewCurrentConfig ==
    /\ status[ConfigCmd] = "executed" => currentConf = NewConf
    /\ currentConf = NewConf => status[ConfigCmd] = "executed"

OldInFlightInstanceRemainsPinnedToOldConfig ==
    status[OldUser] # "none" =>
        /\ pinnedConf[OldUser] = OldConf
        /\ depsDomain[OldUser] = OldVoters
        /\ RemovedVoter \in depsDomain[OldUser]
        /\ (status[OldUser] = "chosen" => RemovedVoter \in quorumVotes[OldUser])

OldInstanceQuorumDoesNotUseGlobalNewConfig ==
    /\ currentConf = NewConf
    /\ status[OldUser] = "chosen"
    => /\ Cardinality(quorumVotes[OldUser]) = SlowQuorum(OldConf)
       /\ Cardinality(quorumVotes[OldUser]) > SlowQuorum(NewConf)
       /\ quorumVotes[OldUser] \subseteq OldVoters
       /\ ~(quorumVotes[OldUser] \subseteq NewVoters)

NewUserStartsOnlyAfterTransitionAndUsesNewConfig ==
    status[NewUser] # "none" =>
        /\ currentConf = NewConf
        /\ status[ConfigCmd] = "executed"
        /\ pinnedConf[NewUser] = NewConf
        /\ depsDomain[NewUser] = NewVoters
        /\ RemovedVoter \notin depsDomain[NewUser]

NewUserQuorumExcludesRemovedVoter ==
    status[NewUser] = "chosen" =>
        /\ Cardinality(quorumVotes[NewUser]) = SlowQuorum(NewConf)
        /\ RemovedVoter \notin quorumVotes[NewUser]
        /\ quorumVotes[NewUser] \subseteq NewVoters

Safety ==
    /\ InactiveInstancesAreEmpty
    /\ ActiveInstancesUsePinnedDependencyDomain
    /\ ChosenInstancesUsePinnedQuorum
    /\ ConfigCommandChosenAndExecutedUnderOldConfig
    /\ ExecutedConfigInstallsOnlyNewCurrentConfig
    /\ OldInFlightInstanceRemainsPinnedToOldConfig
    /\ OldInstanceQuorumDoesNotUseGlobalNewConfig
    /\ NewUserStartsOnlyAfterTransitionAndUsesNewConfig
    /\ NewUserQuorumExcludesRemovedVoter

EventuallyCoversConfigRemoveTransition ==
    <> ( /\ stage = 6
         /\ currentConf = NewConf
         /\ status[ConfigCmd] = "executed"
         /\ status[OldUser] = "chosen"
         /\ status[NewUser] = "chosen"
         /\ pinnedConf[ConfigCmd] = OldConf
         /\ pinnedConf[OldUser] = OldConf
         /\ pinnedConf[NewUser] = NewConf
         /\ RemovedVoter \in quorumVotes[OldUser]
         /\ RemovedVoter \notin quorumVotes[NewUser]
         /\ Cardinality(quorumVotes[OldUser]) = SlowQuorum(OldConf)
         /\ Cardinality(quorumVotes[NewUser]) = SlowQuorum(NewConf) )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartOldUser)
    /\ \A q \in QuorumSets(OldConf) : WF_Vars(ChooseConfig(q))
    /\ WF_Vars(ExecuteConfig)
    /\ \A q \in OldQuorumsWithRemoved : WF_Vars(ChooseOldUser(q))
    /\ WF_Vars(StartNewUser)
    /\ \A q \in QuorumSets(NewConf) : WF_Vars(ChooseNewUser(q))

====
