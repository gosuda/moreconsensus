----------------------------- MODULE EPaxosOptimizedRecovery -----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS ReplicaCount

VARIABLES phase, prepareWitnesses, tryOK, acceptOK, acceptReplyEvidenceRecorded, conflictAcceptDepsCoversTarget, chosenAttrsChanged, rejected

Replicas == 1..ReplicaCount
Coordinator == 1
SlowQuorum == (ReplicaCount \div 2) + 1
FastQuorum ==
    IF ReplicaCount \in {1, 3, 5, 7}
    THEN IF ReplicaCount = 1 THEN 1 ELSE (ReplicaCount \div 2) + (((ReplicaCount \div 2) + 1) \div 2)
    ELSE ReplicaCount - ((ReplicaCount - 1) \div 4)
TryWitnessQuorum == FastQuorum + SlowQuorum - ReplicaCount

Phases == {"prepare", "try_pre_accept", "accept", "committed", "deferred"}
Vars == <<phase, prepareWitnesses, tryOK, acceptOK, acceptReplyEvidenceRecorded, conflictAcceptDepsCoversTarget, chosenAttrsChanged, rejected>>

TypeOK ==
    /\ ReplicaCount \in 1..7
    /\ phase \in Phases
    /\ prepareWitnesses \subseteq Replicas
    /\ tryOK \subseteq Replicas
    /\ acceptOK \subseteq Replicas
    /\ acceptReplyEvidenceRecorded \in BOOLEAN
    /\ conflictAcceptDepsCoversTarget \in BOOLEAN
    /\ chosenAttrsChanged \in BOOLEAN
    /\ rejected \in BOOLEAN
    /\ Coordinator \in Replicas

Init ==
    /\ phase = "prepare"
    /\ prepareWitnesses = {}
    /\ tryOK = {}
    /\ acceptOK = {}
    /\ acceptReplyEvidenceRecorded = FALSE
    /\ conflictAcceptDepsCoversTarget \in BOOLEAN
    /\ chosenAttrsChanged = FALSE
    /\ rejected = FALSE

\* Abstracts receiving an AcceptReply carrying strict-superset Accept-Deps.
\* The evidence is recorded for recovery, but the chosen Seq/Deps are unchanged.
RecordAcceptReplyEvidence ==
    /\ ~acceptReplyEvidenceRecorded
    /\ acceptReplyEvidenceRecorded' = TRUE
    /\ chosenAttrsChanged' = FALSE
    /\ UNCHANGED <<phase, prepareWitnesses, tryOK, acceptOK, conflictAcceptDepsCoversTarget, rejected>>

PrepareFastWitness(r) ==
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareWitnesses
    /\ LET newPrepareWitnesses == prepareWitnesses \cup {r}
       IN /\ prepareWitnesses' = newPrepareWitnesses
          /\ phase' = IF Cardinality(newPrepareWitnesses) >= TryWitnessQuorum THEN "try_pre_accept" ELSE "prepare"
          /\ tryOK' = IF Cardinality(newPrepareWitnesses) >= TryWitnessQuorum THEN newPrepareWitnesses ELSE tryOK
    /\ UNCHANGED <<acceptOK, acceptReplyEvidenceRecorded, conflictAcceptDepsCoversTarget, chosenAttrsChanged, rejected>>

\* The target TryPreAccept candidate does not list the conflicting command in
\* its chosen deps. The conflict's ordinary chosen deps also omit the target.
\* Accept-Deps evidence is therefore the only recovery evidence that can prove
\* the conflict was already ordered after the target.
TryPreAcceptWitness(r) ==
    /\ phase = "try_pre_accept"
    /\ r \in Replicas \ tryOK
    /\ IF conflictAcceptDepsCoversTarget
       THEN LET newTryOK == tryOK \cup {r}
            IN /\ tryOK' = newTryOK
               /\ rejected' = FALSE
               /\ phase' = IF Cardinality(newTryOK) >= SlowQuorum THEN "accept" ELSE "try_pre_accept"
               /\ UNCHANGED <<prepareWitnesses, acceptOK, acceptReplyEvidenceRecorded, conflictAcceptDepsCoversTarget, chosenAttrsChanged>>
       ELSE /\ phase' = "deferred"
            /\ rejected' = TRUE
            /\ UNCHANGED <<prepareWitnesses, tryOK, acceptOK, acceptReplyEvidenceRecorded, conflictAcceptDepsCoversTarget, chosenAttrsChanged>>

AcceptOK(r) ==
    /\ phase = "accept"
    /\ r \in Replicas \ acceptOK
    /\ LET newAcceptOK == acceptOK \cup {r}
       IN /\ acceptOK' = newAcceptOK
          /\ phase' = IF Cardinality(newAcceptOK) >= SlowQuorum THEN "committed" ELSE "accept"
    /\ UNCHANGED <<prepareWitnesses, tryOK, acceptReplyEvidenceRecorded, conflictAcceptDepsCoversTarget, chosenAttrsChanged, rejected>>

Next ==
    \/ RecordAcceptReplyEvidence
    \/ \E r \in Replicas : PrepareFastWitness(r)
    \/ \E r \in Replicas : TryPreAcceptWitness(r)
    \/ \E r \in Replicas : AcceptOK(r)

Spec == Init /\ [][Next]_Vars

AcceptDepsPreventsUnsafeDeferral ==
    conflictAcceptDepsCoversTarget => ~rejected

DeferralRequiresMissingAcceptDepsEvidence ==
    rejected => ~conflictAcceptDepsCoversTarget

AcceptReplyEvidenceDoesNotChangeChosenAttrs ==
    acceptReplyEvidenceRecorded => ~chosenAttrsChanged

TryPreAcceptRequiresPrepareWitnessThreshold ==
    phase \in {"try_pre_accept", "accept", "committed", "deferred"} => Cardinality(prepareWitnesses) >= TryWitnessQuorum

AcceptRequiresTryQuorum ==
    phase \in {"accept", "committed"} => Cardinality(tryOK) >= SlowQuorum

CommitRequiresAcceptQuorum ==
    phase = "committed" => Cardinality(acceptOK) >= SlowQuorum

Safety ==
    /\ AcceptDepsPreventsUnsafeDeferral
    /\ DeferralRequiresMissingAcceptDepsEvidence
    /\ AcceptReplyEvidenceDoesNotChangeChosenAttrs
    /\ TryPreAcceptRequiresPrepareWitnessThreshold
    /\ AcceptRequiresTryQuorum
    /\ CommitRequiresAcceptQuorum

================================================================================
