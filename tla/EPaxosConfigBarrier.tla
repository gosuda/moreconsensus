---- MODULE EPaxosConfigBarrier ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Small finite model of the implementation's configuration-change barrier. *)
(* It deliberately keeps two fixed configuration-command refs and one user  *)
(* command.  It models local pendingConf behavior and inbound user attrs,    *)
(* not dynamic membership reconfiguration, quorum changes, recovery, or an   *)
(* unbounded EPaxos instance space.                                          *)
(***************************************************************************)

CONSTANTS C1, C2

VARIABLES configStatus, pendingConf, userStatus, userDeps, userSeq,
          userSeenUnexecuted, localProposalStatus, pendingAtLocalAttempt

Replicas == {1, 2, 3}
ConfigRefs == {C1, C2}
ConfigStatuses == {"unknown", "preaccepted", "committed", "executed"}
UnexecutedKnownStatuses == {"preaccepted", "committed"}
UserStatuses == {"none", "preaccepted"}
LocalProposalStatuses == {"none", "blocked", "accepted"}

RefReplica == [c \in ConfigRefs |-> IF c = C1 THEN 1 ELSE 3]
RefInstance == [c \in ConfigRefs |-> 1]
RefSeq == [c \in ConfigRefs |-> 1]

Vars == <<configStatus, pendingConf, userStatus, userDeps, userSeq,
          userSeenUnexecuted, localProposalStatus, pendingAtLocalAttempt>>

IsUnexecutedKnown(statuses, c) == statuses[c] \in UnexecutedKnownStatuses
KnownUnexecuted(statuses) == {c \in ConfigRefs : IsUnexecutedKnown(statuses, c)}
HasPending(statuses) == KnownUnexecuted(statuses) # {}

ConfigDeps(configs) ==
    [r \in Replicas |-> IF \E c \in configs : RefReplica[c] = r
                      THEN 1
                      ELSE 0]

SeqAfter(configs) == IF configs = {} THEN 1 ELSE 2
DepsIncludeRef(deps, c) == deps[RefReplica[c]] >= RefInstance[c]

TypeOK ==
    /\ configStatus \in [ConfigRefs -> ConfigStatuses]
    /\ pendingConf \in BOOLEAN
    /\ userStatus \in UserStatuses
    /\ userDeps \in [Replicas -> 0..1]
    /\ userSeq \in 0..2
    /\ userSeenUnexecuted \subseteq ConfigRefs
    /\ localProposalStatus \in LocalProposalStatuses
    /\ pendingAtLocalAttempt \in BOOLEAN

Init ==
    /\ configStatus = [c \in ConfigRefs |-> "unknown"]
    /\ pendingConf = FALSE
    /\ userStatus = "none"
    /\ userDeps = [r \in Replicas |-> 0]
    /\ userSeq = 0
    /\ userSeenUnexecuted = {}
    /\ localProposalStatus = "none"
    /\ pendingAtLocalAttempt = FALSE

ObserveConfig(c) ==
    /\ configStatus[c] = "unknown"
    /\ LET newStatus == [configStatus EXCEPT ![c] = "preaccepted"]
       IN /\ configStatus' = newStatus
          /\ pendingConf' = HasPending(newStatus)
    /\ UNCHANGED <<userStatus, userDeps, userSeq, userSeenUnexecuted,
                  localProposalStatus, pendingAtLocalAttempt>>

CommitConfig(c) ==
    /\ configStatus[c] = "preaccepted"
    /\ LET newStatus == [configStatus EXCEPT ![c] = "committed"]
       IN /\ configStatus' = newStatus
          /\ pendingConf' = HasPending(newStatus)
    /\ UNCHANGED <<userStatus, userDeps, userSeq, userSeenUnexecuted,
                  localProposalStatus, pendingAtLocalAttempt>>

ExecuteConfig(c) ==
    /\ configStatus[c] \in UnexecutedKnownStatuses
    /\ LET newStatus == [configStatus EXCEPT ![c] = "executed"]
       IN /\ configStatus' = newStatus
          /\ pendingConf' = HasPending(newStatus)
    /\ UNCHANGED <<userStatus, userDeps, userSeq, userSeenUnexecuted,
                  localProposalStatus, pendingAtLocalAttempt>>

ReceiveUserPreAccept ==
    /\ userStatus = "none"
    /\ LET snapshot == KnownUnexecuted(configStatus)
          deps == ConfigDeps(snapshot)
          seq == SeqAfter(snapshot)
       IN /\ userSeenUnexecuted' = snapshot
          /\ userDeps' = deps
          /\ userSeq' = seq
          /\ userStatus' = "preaccepted"
    /\ UNCHANGED <<configStatus, pendingConf, localProposalStatus,
                  pendingAtLocalAttempt>>

LocalUserPropose ==
    /\ localProposalStatus = "none"
    /\ pendingAtLocalAttempt' = pendingConf
    /\ localProposalStatus' = IF pendingConf THEN "blocked" ELSE "accepted"
    /\ UNCHANGED <<configStatus, pendingConf, userStatus, userDeps, userSeq,
                  userSeenUnexecuted>>

Next ==
    \/ \E c \in ConfigRefs : ObserveConfig(c)
    \/ \E c \in ConfigRefs : CommitConfig(c)
    \/ \E c \in ConfigRefs : ExecuteConfig(c)
    \/ ReceiveUserPreAccept
    \/ LocalUserPropose

PendingMatchesKnownUnexecuted ==
    pendingConf = HasPending(configStatus)

UserAttrsIncludeKnownUnexecutedConfigs ==
    userStatus = "preaccepted" =>
        /\ \A c \in userSeenUnexecuted : DepsIncludeRef(userDeps, c)
        /\ \A c \in userSeenUnexecuted : userSeq > RefSeq[c]

UserAttrsOnlyNameSnapshotConfigs ==
    userStatus = "preaccepted" =>
        \A c \in ConfigRefs \ userSeenUnexecuted : ~DepsIncludeRef(userDeps, c)

LocalUserProposalBlockedWhilePending ==
    /\ pendingAtLocalAttempt => localProposalStatus = "blocked"
    /\ localProposalStatus = "accepted" => ~pendingAtLocalAttempt

ExecutingOneConfigKeepsBarrierForAnother ==
    (\E executed \in ConfigRefs :
        /\ configStatus[executed] = "executed"
        /\ \E remaining \in ConfigRefs : IsUnexecutedKnown(configStatus, remaining))
        => pendingConf

AllKnownConfigsExecutedClearsPending ==
    (\A c \in ConfigRefs : ~IsUnexecutedKnown(configStatus, c)) => ~pendingConf

Safety ==
    /\ PendingMatchesKnownUnexecuted
    /\ UserAttrsIncludeKnownUnexecutedConfigs
    /\ UserAttrsOnlyNameSnapshotConfigs
    /\ LocalUserProposalBlockedWhilePending
    /\ ExecutingOneConfigKeepsBarrierForAnother
    /\ AllKnownConfigsExecutedClearsPending

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
