---- MODULE KVOmissionRecovery ----
EXTENDS Naturals, FiniteSets

CONSTANTS ReplicaCount, SlowReplica

VARIABLES reachable, accepted, committed, learned

Replicas == 1..ReplicaCount
Leader == 1
SlowQuorum == (ReplicaCount \div 2) + 1
HealthyReplicas == Replicas \ {SlowReplica}
Vars == <<reachable, accepted, committed, learned>>

TypeOK ==
    /\ ReplicaCount \in 3..5
    /\ SlowReplica \in Replicas \ {Leader}
    /\ reachable \subseteq Replicas
    /\ accepted \subseteq Replicas
    /\ learned \subseteq Replicas
    /\ committed \in BOOLEAN
    /\ Leader \in accepted
    /\ accepted \subseteq reachable

Init ==
    /\ reachable = HealthyReplicas
    /\ accepted = {Leader}
    /\ committed = FALSE
    /\ learned = {}

Reply(r) ==
    /\ r \in reachable \ accepted
    /\ accepted' = accepted \cup {r}
    /\ UNCHANGED <<reachable, committed, learned>>

Commit ==
    /\ ~committed
    /\ Cardinality(accepted) >= SlowQuorum
    /\ committed' = TRUE
    /\ learned' = learned \cup accepted
    /\ UNCHANGED <<reachable, accepted>>

RecoverSlowReplica ==
    /\ SlowReplica \notin reachable
    /\ reachable' = reachable \cup {SlowReplica}
    /\ UNCHANGED <<accepted, committed, learned>>

Learn(r) ==
    /\ committed
    /\ r \in reachable \ learned
    /\ learned' = learned \cup {r}
    /\ UNCHANGED <<reachable, accepted, committed>>

HealthyReply == \E r \in HealthyReplicas : Reply(r)
SlowReplicaLearns == Learn(SlowReplica)

Next ==
    \/ HealthyReply
    \/ Reply(SlowReplica)
    \/ Commit
    \/ RecoverSlowReplica
    \/ \E r \in Replicas : Learn(r)

CommitRequiresQuorum == committed => Cardinality(accepted) >= SlowQuorum
CommittedReplicaCanLearnOnlyAfterRecovery == SlowReplica \in learned => SlowReplica \in reachable /\ committed
SingleReplicaOmissionLeavesQuorumReachable == Cardinality(HealthyReplicas) >= SlowQuorum

Safety ==
    /\ CommitRequiresQuorum
    /\ CommittedReplicaCanLearnOnlyAfterRecovery
    /\ SingleReplicaOmissionLeavesQuorumReachable

EventuallyCommitted == <>committed
EventuallySlowReplicaLearns == <>(SlowReplica \in learned)

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(HealthyReply)
    /\ WF_Vars(Commit)
    /\ WF_Vars(RecoverSlowReplica)
    /\ WF_Vars(SlowReplicaLearns)

====
