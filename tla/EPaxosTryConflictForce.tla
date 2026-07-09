----------------------------- MODULE EPaxosTryConflictForce -----------------------------
EXTENDS Naturals

CONSTANTS ReplicaCount

(*
Finite model of the tryConflictForcesSlowAccept decision in epaxos/node.go.
It checks the quorum arithmetic behind uncommitted-conflict TryPreAccept
responses:
  - a conflict leader forces slow Accept only when the candidate does not
    already depend on that conflict and every possible candidate fast quorum
    must include that leader,
  - a recorded deferred-cycle leader can independently force slow Accept when
    every possible candidate fast quorum must include that deferred leader,
  - otherwise the conflict is deferred and blocker recovery is started.
This is finite 3/5/7 decision coverage, not a full recovery or network model.
*)

VARIABLES possible,
          conflictLeaderPossible,
          candidateDependsOnConflict,
          deferredCycle,
          deferredLeaderPossible,
          forceAccept,
          deferred,
          recoveryStarted,
          stage,
          reason

SlowQuorum == (ReplicaCount \div 2) + 1
FastQuorum ==
    IF ReplicaCount \in {1, 3, 5, 7}
    THEN IF ReplicaCount = 1 THEN 1 ELSE (ReplicaCount \div 2) + (((ReplicaCount \div 2) + 1) \div 2)
    ELSE ReplicaCount - ((ReplicaCount - 1) \div 4)

Stages == {"start", "done"}
Reasons == {"none", "conflict_leader_required", "deferred_leader_required", "defer"}

LeaderRequired(leaderPossible) == leaderPossible /\ possible - 1 < FastQuorum
ConflictLeaderForces == ~candidateDependsOnConflict /\ LeaderRequired(conflictLeaderPossible)
DeferredLeaderForces == deferredCycle /\ LeaderRequired(deferredLeaderPossible)
ShouldForce == ConflictLeaderForces \/ DeferredLeaderForces

Vars == <<possible,
          conflictLeaderPossible,
          candidateDependsOnConflict,
          deferredCycle,
          deferredLeaderPossible,
          forceAccept,
          deferred,
          recoveryStarted,
          stage,
          reason>>

Init ==
    /\ ReplicaCount \in {3, 5, 7}
    /\ possible \in 0..ReplicaCount
    /\ conflictLeaderPossible \in BOOLEAN
    /\ deferredLeaderPossible \in BOOLEAN
    /\ candidateDependsOnConflict \in BOOLEAN
    /\ deferredCycle \in BOOLEAN
    /\ conflictLeaderPossible => possible > 0
    /\ deferredLeaderPossible => possible > 0
    /\ stage = "start"
    /\ forceAccept = FALSE
    /\ deferred = FALSE
    /\ recoveryStarted = FALSE
    /\ reason = "none"

TypeOK ==
    /\ ReplicaCount \in {3, 5, 7}
    /\ possible \in 0..ReplicaCount
    /\ conflictLeaderPossible \in BOOLEAN
    /\ candidateDependsOnConflict \in BOOLEAN
    /\ deferredCycle \in BOOLEAN
    /\ deferredLeaderPossible \in BOOLEAN
    /\ conflictLeaderPossible => possible > 0
    /\ deferredLeaderPossible => possible > 0
    /\ forceAccept \in BOOLEAN
    /\ deferred \in BOOLEAN
    /\ recoveryStarted \in BOOLEAN
    /\ stage \in Stages
    /\ reason \in Reasons

ResolveDecision ==
    /\ stage = "start"
    /\ stage' = "done"
    /\ IF ShouldForce
       THEN /\ forceAccept' = TRUE
            /\ deferred' = FALSE
            /\ recoveryStarted' = FALSE
            /\ reason' = IF ConflictLeaderForces THEN "conflict_leader_required" ELSE "deferred_leader_required"
       ELSE /\ forceAccept' = FALSE
            /\ deferred' = TRUE
            /\ recoveryStarted' = TRUE
            /\ reason' = "defer"
    /\ UNCHANGED <<possible, conflictLeaderPossible, candidateDependsOnConflict,
                  deferredCycle, deferredLeaderPossible>>

Next == ResolveDecision

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

ConflictLeaderForceRequiresNoExistingDependency ==
    stage = "done" /\ reason = "conflict_leader_required" =>
        /\ ~candidateDependsOnConflict
        /\ conflictLeaderPossible
        /\ possible - 1 < FastQuorum
        /\ forceAccept
        /\ ~deferred

ExistingDependencyBlocksConflictLeaderOnlyForce ==
    stage = "done" /\ candidateDependsOnConflict /\ ~DeferredLeaderForces =>
        /\ ~forceAccept
        /\ deferred
        /\ recoveryStarted

OptionalConflictLeaderDoesNotForce ==
    stage = "done" /\ ~LeaderRequired(conflictLeaderPossible) /\ ~DeferredLeaderForces =>
        /\ ~forceAccept
        /\ deferred
        /\ recoveryStarted

DeferredCycleLeaderCanForce ==
    stage = "done" /\ DeferredLeaderForces =>
        /\ forceAccept
        /\ ~deferred
        /\ ~recoveryStarted

NoForceMeansRecoveryDeferral ==
    stage = "done" /\ ~ShouldForce =>
        /\ ~forceAccept
        /\ deferred
        /\ recoveryStarted
        /\ reason = "defer"

ForceIffLeaderRequiredBranch ==
    stage = "done" =>
        forceAccept = ShouldForce

Safety ==
    /\ ConflictLeaderForceRequiresNoExistingDependency
    /\ ExistingDependencyBlocksConflictLeaderOnlyForce
    /\ OptionalConflictLeaderDoesNotForce
    /\ DeferredCycleLeaderCanForce
    /\ NoForceMeansRecoveryDeferral
    /\ ForceIffLeaderRequiredBranch

EventuallyCoversForceDecision == <> (stage = "done")

================================================================================
