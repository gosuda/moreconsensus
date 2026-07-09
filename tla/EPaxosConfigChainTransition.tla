---- MODULE EPaxosConfigChainTransition ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite multi-step configuration-transition model. It checks one  *)
(* add-voter transition from {1,2,3} to {1,2,3,4}, followed by one          *)
(* remove-voter transition to {1,3,4}. Old and mid-flight user instances    *)
(* remain pinned to their original dependency domains and quorums after the *)
(* current configuration advances twice; the later user instance uses only  *)
(* the final voter set. It does not model joint consensus, concurrent       *)
(* configuration commands, recovery, message loss, durable replay, or       *)
(* unbounded configuration histories.                                      *)
(***************************************************************************)

VARIABLES stage, currentConf, status, pinnedConf, quorumVotes, depsDomain

Conf1 == 1
Conf2 == 2
Conf3 == 3
NoConf == 0

AddCmd == "add-cmd"
RemoveCmd == "remove-cmd"
OldUser == "old-user"
MidUser == "mid-user"
NewUser == "new-user"
Instances == {AddCmd, RemoveCmd, OldUser, MidUser, NewUser}

Voters1 == {1, 2, 3}
AddedVoter == 4
RemovedVoter == 2
Voters2 == Voters1 \cup {AddedVoter}
Voters3 == Voters2 \ {RemovedVoter}
AllReplicas == Voters2
ConfigIDs == {Conf1, Conf2, Conf3}
Statuses == {"none", "inflight", "chosen", "executed"}

Vars == <<stage, currentConf, status, pinnedConf, quorumVotes, depsDomain>>

VotersFor(c) ==
    CASE c = Conf1 -> Voters1
      [] c = Conf2 -> Voters2
      [] c = Conf3 -> Voters3
      [] OTHER -> {}

SlowQuorum(c) == (Cardinality(VotersFor(c)) \div 2) + 1

QuorumFor(c, q) ==
    /\ c \in ConfigIDs
    /\ q \subseteq VotersFor(c)
    /\ Cardinality(q) = SlowQuorum(c)

QuorumSets(c) == {q \in SUBSET VotersFor(c) : Cardinality(q) = SlowQuorum(c)}

Conf1QuorumsWithRemoved == {q \in QuorumSets(Conf1) : RemovedVoter \in q}
Conf2QuorumsWithAddedAndRemoved == {q \in QuorumSets(Conf2) : /\ AddedVoter \in q /\ RemovedVoter \in q}

TypeOK ==
    /\ stage \in 0..10
    /\ currentConf \in ConfigIDs
    /\ status \in [Instances -> Statuses]
    /\ pinnedConf \in [Instances -> ConfigIDs \cup {NoConf}]
    /\ quorumVotes \in [Instances -> SUBSET AllReplicas]
    /\ depsDomain \in [Instances -> SUBSET AllReplicas]

Init ==
    /\ stage = 0
    /\ currentConf = Conf1
    /\ status = [i \in Instances |-> "none"]
    /\ pinnedConf = [i \in Instances |-> NoConf]
    /\ quorumVotes = [i \in Instances |-> {}]
    /\ depsDomain = [i \in Instances |-> {}]

StartOldUser ==
    /\ stage = 0
    /\ status[OldUser] = "none"
    /\ stage' = 1
    /\ status' = [status EXCEPT ![OldUser] = "inflight"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![OldUser] = Conf1]
    /\ depsDomain' = [depsDomain EXCEPT ![OldUser] = Voters1]
    /\ UNCHANGED <<currentConf, quorumVotes>>

ChooseAdd(q) ==
    /\ stage = 1
    /\ q \in QuorumSets(Conf1)
    /\ status[AddCmd] = "none"
    /\ stage' = 2
    /\ status' = [status EXCEPT ![AddCmd] = "chosen"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![AddCmd] = Conf1]
    /\ depsDomain' = [depsDomain EXCEPT ![AddCmd] = Voters1]
    /\ quorumVotes' = [quorumVotes EXCEPT ![AddCmd] = q]
    /\ UNCHANGED currentConf

ExecuteAdd ==
    /\ stage = 2
    /\ status[AddCmd] = "chosen"
    /\ stage' = 3
    /\ currentConf' = Conf2
    /\ status' = [status EXCEPT ![AddCmd] = "executed"]
    /\ UNCHANGED <<pinnedConf, quorumVotes, depsDomain>>

StartMidUser ==
    /\ stage = 3
    /\ currentConf = Conf2
    /\ status[MidUser] = "none"
    /\ stage' = 4
    /\ status' = [status EXCEPT ![MidUser] = "inflight"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![MidUser] = Conf2]
    /\ depsDomain' = [depsDomain EXCEPT ![MidUser] = Voters2]
    /\ UNCHANGED <<currentConf, quorumVotes>>

ChooseRemove(q) ==
    /\ stage = 4
    /\ q \in QuorumSets(Conf2)
    /\ status[RemoveCmd] = "none"
    /\ stage' = 5
    /\ status' = [status EXCEPT ![RemoveCmd] = "chosen"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![RemoveCmd] = Conf2]
    /\ depsDomain' = [depsDomain EXCEPT ![RemoveCmd] = Voters2]
    /\ quorumVotes' = [quorumVotes EXCEPT ![RemoveCmd] = q]
    /\ UNCHANGED currentConf

ExecuteRemove ==
    /\ stage = 5
    /\ status[RemoveCmd] = "chosen"
    /\ stage' = 6
    /\ currentConf' = Conf3
    /\ status' = [status EXCEPT ![RemoveCmd] = "executed"]
    /\ UNCHANGED <<pinnedConf, quorumVotes, depsDomain>>

ChooseOldUser(q) ==
    /\ stage = 6
    /\ currentConf = Conf3
    /\ status[OldUser] = "inflight"
    /\ q \in Conf1QuorumsWithRemoved
    /\ stage' = 7
    /\ status' = [status EXCEPT ![OldUser] = "chosen"]
    /\ quorumVotes' = [quorumVotes EXCEPT ![OldUser] = q]
    /\ UNCHANGED <<currentConf, pinnedConf, depsDomain>>

ChooseMidUser(q) ==
    /\ stage = 7
    /\ currentConf = Conf3
    /\ status[MidUser] = "inflight"
    /\ q \in Conf2QuorumsWithAddedAndRemoved
    /\ stage' = 8
    /\ status' = [status EXCEPT ![MidUser] = "chosen"]
    /\ quorumVotes' = [quorumVotes EXCEPT ![MidUser] = q]
    /\ UNCHANGED <<currentConf, pinnedConf, depsDomain>>

StartNewUser ==
    /\ stage = 8
    /\ currentConf = Conf3
    /\ status[RemoveCmd] = "executed"
    /\ status[NewUser] = "none"
    /\ stage' = 9
    /\ status' = [status EXCEPT ![NewUser] = "inflight"]
    /\ pinnedConf' = [pinnedConf EXCEPT ![NewUser] = Conf3]
    /\ depsDomain' = [depsDomain EXCEPT ![NewUser] = Voters3]
    /\ UNCHANGED <<currentConf, quorumVotes>>

ChooseNewUser(q) ==
    /\ stage = 9
    /\ currentConf = Conf3
    /\ status[NewUser] = "inflight"
    /\ q \in QuorumSets(Conf3)
    /\ stage' = 10
    /\ status' = [status EXCEPT ![NewUser] = "chosen"]
    /\ quorumVotes' = [quorumVotes EXCEPT ![NewUser] = q]
    /\ UNCHANGED <<currentConf, pinnedConf, depsDomain>>

Next ==
    \/ StartOldUser
    \/ \E q \in QuorumSets(Conf1) : ChooseAdd(q)
    \/ ExecuteAdd
    \/ StartMidUser
    \/ \E q \in QuorumSets(Conf2) : ChooseRemove(q)
    \/ ExecuteRemove
    \/ \E q \in Conf1QuorumsWithRemoved : ChooseOldUser(q)
    \/ \E q \in Conf2QuorumsWithAddedAndRemoved : ChooseMidUser(q)
    \/ StartNewUser
    \/ \E q \in QuorumSets(Conf3) : ChooseNewUser(q)

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

ConfigCommandsUseGenerationPinnedConfig ==
    /\ status[AddCmd] # "none" => /\ pinnedConf[AddCmd] = Conf1 /\ depsDomain[AddCmd] = Voters1
    /\ status[RemoveCmd] # "none" => /\ pinnedConf[RemoveCmd] = Conf2 /\ depsDomain[RemoveCmd] = Voters2

CurrentConfigFollowsExecutedChain ==
    /\ status[AddCmd] = "executed" /\ status[RemoveCmd] # "executed" => currentConf = Conf2
    /\ status[RemoveCmd] = "executed" => currentConf = Conf3
    /\ currentConf = Conf3 => /\ status[AddCmd] = "executed" /\ status[RemoveCmd] = "executed"

OldInstanceRemainsPinnedThroughBothTransitions ==
    status[OldUser] # "none" =>
        /\ pinnedConf[OldUser] = Conf1
        /\ depsDomain[OldUser] = Voters1
        /\ AddedVoter \notin depsDomain[OldUser]
        /\ RemovedVoter \in depsDomain[OldUser]
        /\ (status[OldUser] = "chosen" => /\ RemovedVoter \in quorumVotes[OldUser]
                                           /\ AddedVoter \notin quorumVotes[OldUser])

MidInstanceRemainsPinnedAfterRemoval ==
    status[MidUser] # "none" =>
        /\ pinnedConf[MidUser] = Conf2
        /\ depsDomain[MidUser] = Voters2
        /\ AddedVoter \in depsDomain[MidUser]
        /\ RemovedVoter \in depsDomain[MidUser]
        /\ (status[MidUser] = "chosen" => /\ AddedVoter \in quorumVotes[MidUser]
                                           /\ RemovedVoter \in quorumVotes[MidUser]
                                           /\ Cardinality(quorumVotes[MidUser]) = SlowQuorum(Conf2)
                                           /\ Cardinality(quorumVotes[MidUser]) > SlowQuorum(Conf3)
                                           /\ ~(quorumVotes[MidUser] \subseteq Voters3))

NewUserUsesFinalConfigOnly ==
    status[NewUser] # "none" =>
        /\ currentConf = Conf3
        /\ pinnedConf[NewUser] = Conf3
        /\ depsDomain[NewUser] = Voters3
        /\ AddedVoter \in depsDomain[NewUser]
        /\ RemovedVoter \notin depsDomain[NewUser]
        /\ (status[NewUser] = "chosen" => /\ quorumVotes[NewUser] \subseteq Voters3
                                           /\ RemovedVoter \notin quorumVotes[NewUser]
                                           /\ Cardinality(quorumVotes[NewUser]) = SlowQuorum(Conf3))

Safety ==
    /\ InactiveInstancesAreEmpty
    /\ ActiveInstancesUsePinnedDependencyDomain
    /\ ChosenInstancesUsePinnedQuorum
    /\ ConfigCommandsUseGenerationPinnedConfig
    /\ CurrentConfigFollowsExecutedChain
    /\ OldInstanceRemainsPinnedThroughBothTransitions
    /\ MidInstanceRemainsPinnedAfterRemoval
    /\ NewUserUsesFinalConfigOnly

EventuallyCoversConfigChainTransition ==
    <> ( /\ stage = 10
         /\ currentConf = Conf3
         /\ status[AddCmd] = "executed"
         /\ status[RemoveCmd] = "executed"
         /\ status[OldUser] = "chosen"
         /\ status[MidUser] = "chosen"
         /\ status[NewUser] = "chosen"
         /\ RemovedVoter \in quorumVotes[OldUser]
         /\ AddedVoter \in quorumVotes[MidUser]
         /\ RemovedVoter \in quorumVotes[MidUser]
         /\ RemovedVoter \notin quorumVotes[NewUser]
         /\ Cardinality(quorumVotes[OldUser]) = SlowQuorum(Conf1)
         /\ Cardinality(quorumVotes[MidUser]) = SlowQuorum(Conf2)
         /\ Cardinality(quorumVotes[NewUser]) = SlowQuorum(Conf3) )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartOldUser)
    /\ \A q \in QuorumSets(Conf1) : WF_Vars(ChooseAdd(q))
    /\ WF_Vars(ExecuteAdd)
    /\ WF_Vars(StartMidUser)
    /\ \A q \in QuorumSets(Conf2) : WF_Vars(ChooseRemove(q))
    /\ WF_Vars(ExecuteRemove)
    /\ \A q \in Conf1QuorumsWithRemoved : WF_Vars(ChooseOldUser(q))
    /\ \A q \in Conf2QuorumsWithAddedAndRemoved : WF_Vars(ChooseMidUser(q))
    /\ WF_Vars(StartNewUser)
    /\ \A q \in QuorumSets(Conf3) : WF_Vars(ChooseNewUser(q))

====
