---- MODULE EPaxosRecovery ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused stopped-owner recovery model.  Owner is deliberately excluded    *)
(* from prepare and accept quorums so TLC checks the stronger case where a  *)
(* non-owner coordinator recovers a missing dependency while the original   *)
(* owner is absent.  This complements EPaxosResponses.tla; it is not a full *)
(* per-instance recovery model for every Go recovery branch.                *)
(***************************************************************************)

CONSTANTS ReplicaCount, Owner, Coordinator

VARIABLES phase, prepareOK, accOK, depStatus, dependentExecuted

Replicas == 1..ReplicaCount
SlowQuorum == (ReplicaCount \div 2) + 1

Phases == {"blocked", "prepare", "accept", "committed", "released"}
DepStatuses == {"missing", "accepted_noop", "committed_noop", "executed_noop"}
Vars == <<phase, prepareOK, accOK, depStatus, dependentExecuted>>

TypeOK ==
    /\ ReplicaCount \in 3..7
    /\ Owner \in Replicas
    /\ Coordinator \in Replicas
    /\ Coordinator # Owner
    /\ phase \in Phases
    /\ prepareOK \subseteq Replicas
    /\ accOK \subseteq Replicas
    /\ depStatus \in DepStatuses
    /\ dependentExecuted \in BOOLEAN

Init ==
    /\ phase = "blocked"
    /\ prepareOK = {}
    /\ accOK = {}
    /\ depStatus = "missing"
    /\ dependentExecuted = FALSE

StartRecovery ==
    /\ phase = "blocked"
    /\ prepareOK' = {Coordinator}
    /\ phase' = "prepare"
    /\ UNCHANGED <<accOK, depStatus, dependentExecuted>>

PrepareNone(r) ==
    LET newPrepareOK == prepareOK \cup {r}
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ r # Owner
    /\ prepareOK' = newPrepareOK
    /\ IF Cardinality(newPrepareOK) >= SlowQuorum
       THEN /\ phase' = "accept"
            /\ depStatus' = "accepted_noop"
            /\ accOK' = {Coordinator}
       ELSE /\ phase' = phase
            /\ depStatus' = depStatus
            /\ accOK' = accOK
    /\ UNCHANGED dependentExecuted

AcceptOK(r) ==
    LET newAccOK == accOK \cup {r}
    IN
    /\ phase = "accept"
    /\ r \in Replicas \ accOK
    /\ r # Owner
    /\ accOK' = newAccOK
    /\ IF Cardinality(newAccOK) >= SlowQuorum
       THEN /\ phase' = "committed"
            /\ depStatus' = "committed_noop"
       ELSE /\ phase' = phase
            /\ depStatus' = depStatus
    /\ UNCHANGED <<prepareOK, dependentExecuted>>

ExecuteNoop ==
    /\ phase = "committed"
    /\ depStatus = "committed_noop"
    /\ phase' = "released"
    /\ depStatus' = "executed_noop"
    /\ UNCHANGED <<prepareOK, accOK, dependentExecuted>>

ExecuteDependent ==
    /\ phase = "released"
    /\ depStatus = "executed_noop"
    /\ dependentExecuted = FALSE
    /\ dependentExecuted' = TRUE
    /\ UNCHANGED <<phase, prepareOK, accOK, depStatus>>

Next ==
    \/ StartRecovery
    \/ \E r \in Replicas : PrepareNone(r)
    \/ \E r \in Replicas : AcceptOK(r)
    \/ ExecuteNoop
    \/ ExecuteDependent

NoDependentBeforeRecoveredDependency ==
    dependentExecuted => depStatus = "executed_noop"

NoAcceptWithoutOwnerIndependentPrepareQuorum ==
    depStatus \in {"accepted_noop", "committed_noop", "executed_noop"} =>
        Cardinality(prepareOK) >= SlowQuorum /\ Owner \notin prepareOK

NoCommitWithoutOwnerIndependentAcceptQuorum ==
    depStatus \in {"committed_noop", "executed_noop"} =>
        Cardinality(accOK) >= SlowQuorum /\ Owner \notin accOK

Safety ==
    /\ NoDependentBeforeRecoveredDependency
    /\ NoAcceptWithoutOwnerIndependentPrepareQuorum
    /\ NoCommitWithoutOwnerIndependentAcceptQuorum

EventuallyDependentExecuted == <>dependentExecuted

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(StartRecovery)
    /\ \A r \in Replicas : WF_Vars(PrepareNone(r))
    /\ \A r \in Replicas : WF_Vars(AcceptOK(r))
    /\ WF_Vars(ExecuteNoop)
    /\ WF_Vars(ExecuteDependent)

====
