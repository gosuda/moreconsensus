---- MODULE EPaxosConfigReplay ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite durable configuration replay model. It checks the restart *)
(* contract implemented by RawNode: the node begins with the caller's       *)
(* initial configuration plus at most the last durable config state from    *)
(* storage, then replays executed configuration-change records in sorted    *)
(* instance order to reconstruct intermediate historical configs. It also   *)
(* keeps an unexecuted config command pending and rejects local proposals   *)
(* from a voter removed by replayed durable config commands. It does not    *)
(* model joint consensus, message loss, recovery under reconfiguration,     *)
(* arbitrary durable histories, or unbounded membership changes.            *)
(***************************************************************************)

VARIABLES stage, currentConf, confHistory, replayed, pendingConf,
          oldUserDomain, midUserDomain, removedUserProposal,
          removedConfProposal, survivingUserProposal

Conf1 == 1
Conf2 == 2
Conf3 == 3
NoConf == 0

InitialConf == Conf1
LastDurableConf == Conf3

AddCmd == "add-cmd"
RemoveCmd == "remove-cmd"
PendingCmd == "pending-cmd"
ExecutedConfigCmds == {AddCmd, RemoveCmd}

RemovedVoter == 2
SurvivingVoter == 1
AddedVoter == 4

Voters1 == {1, 2, 3}
Voters2 == Voters1 \cup {AddedVoter}
Voters3 == Voters2 \ {RemovedVoter}
AllReplicas == Voters2
ConfigIDs == {Conf1, Conf2, Conf3}
ProposalStatuses == {"none", "accepted", "blocked", "rejected"}

Vars == <<stage, currentConf, confHistory, replayed, pendingConf,
          oldUserDomain, midUserDomain, removedUserProposal,
          removedConfProposal, survivingUserProposal>>

VotersFor(c) ==
    CASE c = Conf1 -> Voters1
      [] c = Conf2 -> Voters2
      [] c = Conf3 -> Voters3
      [] OTHER -> {}

BaseConf(cmd) == IF cmd = AddCmd THEN Conf1 ELSE Conf2
ResultConf(cmd) == IF cmd = AddCmd THEN Conf2 ELSE Conf3

TypeOK ==
    /\ stage \in 0..5
    /\ currentConf \in ConfigIDs
    /\ confHistory \subseteq ConfigIDs
    /\ replayed \subseteq ExecutedConfigCmds
    /\ pendingConf \in BOOLEAN
    /\ oldUserDomain \subseteq AllReplicas
    /\ midUserDomain \subseteq AllReplicas
    /\ removedUserProposal \in ProposalStatuses
    /\ removedConfProposal \in ProposalStatuses
    /\ survivingUserProposal \in ProposalStatuses

Init ==
    /\ stage = 0
    /\ currentConf = LastDurableConf
    /\ confHistory = {InitialConf, LastDurableConf}
    /\ replayed = {}
    /\ pendingConf = TRUE
    /\ oldUserDomain = {}
    /\ midUserDomain = {}
    /\ removedUserProposal = "none"
    /\ removedConfProposal = "none"
    /\ survivingUserProposal = "none"

ReplayAdd ==
    /\ stage = 0
    /\ AddCmd \notin replayed
    /\ BaseConf(AddCmd) \in confHistory
    /\ stage' = 1
    /\ replayed' = replayed \cup {AddCmd}
    /\ confHistory' = confHistory \cup {ResultConf(AddCmd)}
    /\ currentConf' = LastDurableConf
    /\ UNCHANGED <<pendingConf, oldUserDomain, midUserDomain,
                  removedUserProposal, removedConfProposal,
                  survivingUserProposal>>

ReplayRemove ==
    /\ stage = 1
    /\ RemoveCmd \notin replayed
    /\ BaseConf(RemoveCmd) \in confHistory
    /\ stage' = 2
    /\ replayed' = replayed \cup {RemoveCmd}
    /\ confHistory' = confHistory \cup {ResultConf(RemoveCmd)}
    /\ currentConf' = LastDurableConf
    /\ UNCHANGED <<pendingConf, oldUserDomain, midUserDomain,
                  removedUserProposal, removedConfProposal,
                  survivingUserProposal>>

RefreshPendingAfterReplay ==
    /\ stage = 2
    /\ replayed = ExecutedConfigCmds
    /\ stage' = 3
    /\ pendingConf' = TRUE
    /\ UNCHANGED <<currentConf, confHistory, replayed, oldUserDomain,
                  midUserDomain, removedUserProposal, removedConfProposal,
                  survivingUserProposal>>

LoadPinnedUserDomains ==
    /\ stage = 3
    /\ Conf1 \in confHistory
    /\ Conf2 \in confHistory
    /\ stage' = 4
    /\ oldUserDomain' = VotersFor(Conf1)
    /\ midUserDomain' = VotersFor(Conf2)
    /\ UNCHANGED <<currentConf, confHistory, replayed, pendingConf,
                  removedUserProposal, removedConfProposal,
                  survivingUserProposal>>

AttemptLocalProposals ==
    /\ stage = 4
    /\ currentConf = Conf3
    /\ pendingConf
    /\ stage' = 5
    /\ removedUserProposal' = IF RemovedVoter \in VotersFor(currentConf) THEN "accepted" ELSE "rejected"
    /\ removedConfProposal' = IF RemovedVoter \in VotersFor(currentConf) THEN "accepted" ELSE "rejected"
    /\ survivingUserProposal' = IF pendingConf THEN "blocked" ELSE "accepted"
    /\ UNCHANGED <<currentConf, confHistory, replayed, pendingConf,
                  oldUserDomain, midUserDomain>>

Next ==
    \/ ReplayAdd
    \/ ReplayRemove
    \/ RefreshPendingAfterReplay
    \/ LoadPinnedUserDomains
    \/ AttemptLocalProposals

InitialAndLastDurableConfigsRemembered ==
    /\ InitialConf \in confHistory
    /\ LastDurableConf \in confHistory

IntermediateConfigReconstructedBeforeUse ==
    RemoveCmd \in replayed => Conf2 \in confHistory

CurrentConfigStaysAtLastDurableConfig ==
    currentConf = LastDurableConf

PendingConfigSurvivesRestartReplay ==
    pendingConf

HistoricalUsersKeepPinnedDomains ==
    stage = 5 =>
        /\ oldUserDomain = Voters1
        /\ RemovedVoter \in oldUserDomain
        /\ AddedVoter \notin oldUserDomain
        /\ midUserDomain = Voters2
        /\ RemovedVoter \in midUserDomain
        /\ AddedVoter \in midUserDomain

RemovedVoterCannotProposeAfterReplay ==
    stage = 5 =>
        /\ removedUserProposal = "rejected"
        /\ removedConfProposal = "rejected"

SurvivingVoterStillBlockedByPendingConfig ==
    stage = 5 => survivingUserProposal = "blocked"

Safety ==
    /\ InitialAndLastDurableConfigsRemembered
    /\ IntermediateConfigReconstructedBeforeUse
    /\ CurrentConfigStaysAtLastDurableConfig
    /\ PendingConfigSurvivesRestartReplay
    /\ HistoricalUsersKeepPinnedDomains
    /\ RemovedVoterCannotProposeAfterReplay
    /\ SurvivingVoterStillBlockedByPendingConfig

EventuallyCoversConfigReplay ==
    <> ( /\ stage = 5
         /\ confHistory = ConfigIDs
         /\ replayed = ExecutedConfigCmds
         /\ currentConf = Conf3
         /\ oldUserDomain = Voters1
         /\ midUserDomain = Voters2
         /\ removedUserProposal = "rejected"
         /\ removedConfProposal = "rejected"
         /\ survivingUserProposal = "blocked" )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(ReplayAdd)
    /\ WF_Vars(ReplayRemove)
    /\ WF_Vars(RefreshPendingAfterReplay)
    /\ WF_Vars(LoadPinnedUserDomains)
    /\ WF_Vars(AttemptLocalProposals)

====
