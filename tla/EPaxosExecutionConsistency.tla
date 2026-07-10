---- MODULE EPaxosExecutionConsistency ----
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* This finite model consumes immutable chosen tuples while separately      *)
(* tracking provisional PreAccepted and Accepted conflict evidence. Compact *)
(* dependency vectors are lane maxima: a maximum n denotes every logical    *)
(* instance 1..n in that lane, including slots with no materialized record.  *)
(* Consensus messages and ballots are intentionally outside this module.     *)
(***************************************************************************)

CONSTANTS Replicas, Instances, Commands, Conflicts, SeqNums, Scenario

VARIABLES caseKind, chosenTuple, learned, recordStatus, executed, execLog,
          recoveryRequested

Vars == <<caseKind, chosenTuple, learned, recordStatus, executed, execLog,
          recoveryRequested>>

Unknown == "unknown"
PreAccepted == "preaccepted"
Accepted == "accepted"
Committed == "committed"
Executed == "executed"
Statuses == {Unknown, PreAccepted, Accepted, Committed, Executed}
EvidenceStatuses == {PreAccepted, Accepted}
ChosenStatuses == {Committed, Executed}

A == <<1, 1>>
B == <<2, 1>>
C == <<3, 1>>
D == <<2, 2>>
SCCInstances == {A, B, C}
PruningInstances == {A, B, C, D}
SCCSeqNums == 1..3
PruningSeqNums == 0..3

CmdA == CHOOSE cmd \in Commands : TRUE
CmdB == CHOOSE cmd \in Commands \ {CmdA} : TRUE
CommandConflicts == {<<CmdA, CmdB>>, <<CmdB, CmdA>>}

MaxInstanceNum == IF Scenario = "scc" THEN 1 ELSE 2
DependencyVectors == [Replicas -> 0..MaxInstanceNum]
TupleType == [present : {TRUE}, cmd : Commands, seq : SeqNums,
             deps : DependencyVectors]
Orders == UNION {[1..n -> Instances] : n \in 0..Cardinality(Instances)}

DepVector(lane1, lane2, lane3) ==
    [replica \in Replicas |->
        CASE replica = 1 -> lane1
          [] replica = 2 -> lane2
          [] OTHER -> lane3]

ZeroDeps == DepVector(0, 0, 0)
Tuple(cmd, seqNum, compactDeps) ==
    [present |-> TRUE, cmd |-> cmd, seq |-> seqNum, deps |-> compactDeps]
NoTuple ==
    [present |-> FALSE, cmd |-> CmdA, seq |-> 0, deps |-> ZeroDeps]

DagChosen ==
    [i \in Instances |->
        CASE i = A -> Tuple(CmdA, 3, DepVector(0, 1, 0))
          [] i = B -> Tuple(CmdB, 2, DepVector(0, 0, 1))
          [] OTHER -> Tuple(CmdA, 1, ZeroDeps)]

CycleChosen ==
    [i \in Instances |->
        CASE i = A -> Tuple(CmdA, 1, DepVector(0, 1, 0))
          [] i = B -> Tuple(CmdB, 1, DepVector(1, 0, 0))
          [] OTHER -> Tuple(CmdA, 2, DepVector(0, 1, 0))]

(***************************************************************************)
(* SparseChosen gives A the compact lane-2 maximum 2. D is the materialized  *)
(* high witness and proves it is after A, but the lower logical slot B is an *)
(* independent prefix obligation. D also orders every conflicting CmdA.      *)
(***************************************************************************)
SparseChosen ==
    [i \in Instances |->
        CASE i = A -> Tuple(CmdA, 1, DepVector(0, 2, 0))
          [] i = B -> Tuple(CmdA, 1, ZeroDeps)
          [] i = C -> Tuple(CmdA, 1, ZeroDeps)
          [] OTHER -> Tuple(CmdB, 2, DepVector(1, 1, 1))]

(***************************************************************************)
(* EvidenceChosen is the immutable final graph for all provisional-conflict  *)
(* cases. B eventually resolves to a chosen tuple ordered after A, C, and D. *)
(***************************************************************************)
EvidenceChosen ==
    [i \in Instances |->
        CASE i = B -> Tuple(CmdB, 2, DepVector(1, 2, 1))
          [] OTHER -> Tuple(CmdA, 1, ZeroDeps)]

SCCCases == {"dag", "cycle"}
PruningCases == {
    "sparse-prefix",
    "higher-preaccepted",
    "higher-accepted",
    "equal-preaccepted",
    "missing-reverse-preaccepted"
}
CaseKinds == IF Scenario = "scc" THEN SCCCases ELSE PruningCases

ChosenFor(theCase) ==
    CASE theCase = "dag" -> DagChosen
      [] theCase = "cycle" -> CycleChosen
      [] theCase = "sparse-prefix" -> SparseChosen
      [] OTHER -> EvidenceChosen

EvidenceTuple(theCase, i) ==
    IF i # B
    THEN ChosenFor(theCase)[i]
    ELSE CASE theCase \in {"higher-preaccepted", "higher-accepted"} ->
                  Tuple(CmdB, 2, DepVector(1, 0, 0))
           [] theCase = "equal-preaccepted" ->
                  Tuple(CmdB, 1, DepVector(1, 0, 0))
           [] OTHER -> Tuple(CmdB, 2, ZeroDeps)

InitialStatus(theCase, i) ==
    IF theCase \in SCCCases
    THEN Unknown
    ELSE IF theCase = "sparse-prefix"
         THEN IF i \in {A, D} THEN Committed ELSE Unknown
         ELSE IF i = A
              THEN Committed
              ELSE IF i = B
                   THEN IF theCase = "higher-accepted" THEN Accepted
                        ELSE PreAccepted
                   ELSE Unknown

InitialLearned(theCase, i) ==
    CASE InitialStatus(theCase, i) = Unknown -> NoTuple
      [] InitialStatus(theCase, i) \in EvidenceStatuses -> EvidenceTuple(theCase, i)
      [] OTHER -> ChosenFor(theCase)[i]

ASSUME /\ Scenario \in {"scc", "pruning"}
       /\ Replicas = {1, 2, 3}
       /\ Cardinality(Commands) = 2
       /\ SeqNums \subseteq Nat
       /\ <<CmdA, CmdB>> \in Conflicts
       /\ <<CmdB, CmdA>> \in Conflicts
       /\ \A i \in Instances : i[1] \in Replicas /\ i[2] \in Nat \ {0}
       /\ (Scenario = "scc" =>
              /\ Instances = SCCInstances
              /\ {1, 2, 3} \subseteq SeqNums)
       /\ (Scenario = "pruning" =>
              /\ Instances = PruningInstances
              /\ {0, 1, 2} \subseteq SeqNums)

(***************************************************************************)
(* Cross-replica consistency is pairwise. Replicas 1 and 2 explore all      *)
(* independent local orders; replica 3 mirrors replica 1. This explicit      *)
(* finite symmetry reduction preserves every pair of independently evolving *)
(* replicas without using a TLC state constraint.                            *)
(***************************************************************************)
IndependentReplicas == {1, 2}
ReplicaGroup(r) == IF r = 1 THEN {1, 3} ELSE {2}

StatusRank(status) ==
    CASE status = Unknown -> 0
      [] status = PreAccepted -> 1
      [] status = Accepted -> 2
      [] status = Committed -> 3
      [] OTHER -> 4

StatusAtLeast(status, threshold) == StatusRank(status) >= StatusRank(threshold)
SeqSet(order) == {order[k] : k \in 1..Len(order)}
Materialized(r) == {i \in Instances : recordStatus[r][i] # Unknown}
ChosenVertices(r) == {i \in Instances : recordStatus[r][i] \in ChosenStatuses}
ConflictEvidence(r) == {i \in Instances : recordStatus[r][i] \in EvidenceStatuses}

ExpandDeps(i, tuple) ==
    {dep \in Instances :
        /\ dep # i
        /\ dep[2] <= tuple.deps[dep[1]]}

ChosenDeps(i) == ExpandDeps(i, chosenTuple[i])

RawDeps(r, i) ==
    IF i \notin Materialized(r) THEN {} ELSE ExpandDeps(i, learned[r][i])

KnownAfterAt(r, base, other, threshold) ==
    /\ base \in Materialized(r)
    /\ other \in Materialized(r)
    /\ StatusAtLeast(recordStatus[r][other], threshold)
    /\ base \in RawDeps(r, other)
    /\ learned[r][other].seq > learned[r][base].seq

KnownAfter(r, base, other) == KnownAfterAt(r, base, other, Committed)

PrunedDeps(r, i) ==
    RawDeps(r, i) \ {other \in RawDeps(r, i) : KnownAfter(r, i, other)}

Edge(r, from, to) ==
    /\ from \in ChosenVertices(r)
    /\ to \in ChosenVertices(r)
    /\ to \in PrunedDeps(r, from)

Paths == UNION {[1..n -> Instances] : n \in 1..Cardinality(Instances)}

Path(r, from, to, path) ==
    /\ path \in Paths
    /\ path[1] = from
    /\ path[Len(path)] = to
    /\ \A k \in 1..(Len(path) - 1) : Edge(r, path[k], path[k + 1])

Reachable(r, from, to) ==
    \E path \in Paths : Path(r, from, to, path)

SameSCC(r, left, right) ==
    /\ left \in ChosenVertices(r)
    /\ right \in ChosenVertices(r)
    /\ Reachable(r, left, right)
    /\ Reachable(r, right, left)

SCCOf(r, i) ==
    IF i \in ChosenVertices(r)
    THEN {other \in ChosenVertices(r) : SameSCC(r, i, other)}
    ELSE {}

StronglyConnected(r, comp) ==
    /\ comp # {}
    /\ comp \subseteq ChosenVertices(r)
    /\ \A left, right \in comp : Reachable(r, left, right)

MaximalSCC(r, comp) ==
    /\ StronglyConnected(r, comp)
    /\ \A larger \in SUBSET ChosenVertices(r) :
           comp \subseteq larger /\ StronglyConnected(r, larger) => larger = comp

IsSCC(r, comp) == \E i \in ChosenVertices(r) : comp = SCCOf(r, i)

MissingDeps(r, i) == RawDeps(r, i) \ Materialized(r)
OutstandingMissing(r) ==
    UNION {MissingDeps(r, i) : i \in ChosenVertices(r) \ executed[r]}
RequestsCurrent(r) == recoveryRequested[r] = OutstandingMissing(r)

ExternalDepsReady(r, comp) ==
    \A i \in comp :
        \A dep \in PrunedDeps(r, i) : dep \in comp \/ dep \in executed[r]

CommandsConflict(leftTuple, rightTuple) ==
    <<leftTuple.cmd, rightTuple.cmd>> \in Conflicts

UnresolvedConflict(r, base, comp) ==
    \E other \in ConflictEvidence(r) \ comp :
        /\ other \notin executed[r]
        /\ CommandsConflict(learned[r][base], learned[r][other])
        /\ ~KnownAfterAt(r, base, other, PreAccepted)

ReadySinkSCC(r, comp) ==
    /\ IsSCC(r, comp)
    /\ \A i \in comp : recordStatus[r][i] = Committed
    /\ RequestsCurrent(r)
    /\ ExternalDepsReady(r, comp)
    /\ \A i \in comp : ~UnresolvedConflict(r, i, comp)

RefLess(left, right) ==
    \/ left[1] < right[1]
    \/ left[1] = right[1] /\ left[2] < right[2]

TupleRefLess(left, right) ==
    \/ chosenTuple[left].seq < chosenTuple[right].seq
    \/ chosenTuple[left].seq = chosenTuple[right].seq /\ RefLess(left, right)

PermutationOf(order, comp) ==
    /\ order \in Orders
    /\ Len(order) = Cardinality(comp)
    /\ SeqSet(order) = comp

OrderedByTupleRef(order) ==
    \A leftPos, rightPos \in 1..Len(order) :
        leftPos < rightPos => TupleRefLess(order[leftPos], order[rightPos])

SortedSCC(r, comp) ==
    CHOOSE order \in Orders :
        /\ PermutationOf(order, comp)
        /\ OrderedByTupleRef(order)

LearnChosen(r, i) ==
    /\ recordStatus[r][i] = Unknown
    /\ RequestsCurrent(r)
    /\ learned' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN [learned[replica] EXCEPT ![i] = chosenTuple[i]]
               ELSE learned[replica]]
    /\ recordStatus' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN [recordStatus[replica] EXCEPT ![i] = Committed]
               ELSE recordStatus[replica]]
    /\ UNCHANGED <<caseKind, chosenTuple, executed, execLog, recoveryRequested>>

ResolveEvidence(r, i) ==
    /\ recordStatus[r][i] \in EvidenceStatuses
    /\ learned' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN [learned[replica] EXCEPT ![i] = chosenTuple[i]]
               ELSE learned[replica]]
    /\ recordStatus' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN [recordStatus[replica] EXCEPT ![i] = Committed]
               ELSE recordStatus[replica]]
    /\ UNCHANGED <<caseKind, chosenTuple, executed, execLog, recoveryRequested>>

RequestMissingDependency(r, i) ==
    /\ i \in ChosenVertices(r)
    /\ ~RequestsCurrent(r)
    /\ recoveryRequested' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN OutstandingMissing(replica)
               ELSE recoveryRequested[replica]]
    /\ UNCHANGED <<caseKind, chosenTuple, learned, recordStatus, executed, execLog>>

ExecuteSCC(r, comp) ==
    /\ ReadySinkSCC(r, comp)
    /\ executed' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN executed[replica] \cup comp
               ELSE executed[replica]]
    /\ execLog' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN execLog[replica] \o SortedSCC(replica, comp)
               ELSE execLog[replica]]
    /\ recordStatus' =
           [replica \in Replicas |->
               IF replica \in ReplicaGroup(r)
               THEN [i \in Instances |->
                       IF i \in comp THEN Executed ELSE recordStatus[replica][i]]
               ELSE recordStatus[replica]]
    /\ UNCHANGED <<caseKind, chosenTuple, learned, recoveryRequested>>

Init ==
    /\ caseKind \in CaseKinds
    /\ chosenTuple = ChosenFor(caseKind)
    /\ learned =
           [r \in Replicas |->
               [i \in Instances |-> InitialLearned(caseKind, i)]]
    /\ recordStatus =
           [r \in Replicas |->
               [i \in Instances |-> InitialStatus(caseKind, i)]]
    /\ executed = [r \in Replicas |-> {}]
    /\ execLog = [r \in Replicas |-> <<>>]
    /\ recoveryRequested = [r \in Replicas |-> {}]

Next ==
    \/ \E r \in IndependentReplicas, i \in Instances : LearnChosen(r, i)
    \/ \E r \in IndependentReplicas, i \in Instances : ResolveEvidence(r, i)
    \/ \E r \in IndependentReplicas, i \in Instances : RequestMissingDependency(r, i)
    \/ \E r \in IndependentReplicas, comp \in SUBSET Instances : ExecuteSCC(r, comp)

NoDuplicates(order) == Len(order) = Cardinality(SeqSet(order))

TypeOK ==
    /\ caseKind \in CaseKinds
    /\ chosenTuple = ChosenFor(caseKind)
    /\ chosenTuple \in [Instances -> TupleType]
    /\ learned \in [Replicas -> [Instances -> TupleType \cup {NoTuple}]]
    /\ recordStatus \in [Replicas -> [Instances -> Statuses]]
    /\ executed \in [Replicas -> SUBSET Instances]
    /\ execLog \in [Replicas -> Orders]
    /\ recoveryRequested \in [Replicas -> SUBSET Instances]
    /\ \A r \in Replicas :
           /\ NoDuplicates(execLog[r])
           /\ SeqSet(execLog[r]) = executed[r]
           /\ executed[r] = {i \in Instances : recordStatus[r][i] = Executed}
           /\ \A i \in Instances :
                  (recordStatus[r][i] = Unknown) <=> (learned[r][i] = NoTuple)

LearnedAgreesWithChosen ==
    \A r \in Replicas, i \in Instances :
        recordStatus[r][i] \in ChosenStatuses => learned[r][i] = chosenTuple[i]

ReplicaPairSymmetry ==
    /\ learned[1] = learned[3]
    /\ recordStatus[1] = recordStatus[3]
    /\ executed[1] = executed[3]
    /\ execLog[1] = execLog[3]
    /\ recoveryRequested[1] = recoveryRequested[3]

SCCPartition ==
    \A r \in Replicas :
      \A i \in ChosenVertices(r) :
        /\ i \in SCCOf(r, i)
        /\ MaximalSCC(r, SCCOf(r, i))
        /\ \A other \in SCCOf(r, i) : SCCOf(r, other) = SCCOf(r, i)

EqualLearnedGraphHasEqualSCCs ==
    \A leftReplica, rightReplica \in Replicas :
        /\ learned[leftReplica] = learned[rightReplica]
        /\ recordStatus[leftReplica] = recordStatus[rightReplica]
        => \A i \in Instances :
               SCCOf(leftReplica, i) = SCCOf(rightReplica, i)

ExecuteOnlyReadySinkSCC ==
    \A r \in Replicas :
      \A i \in executed[r] :
        /\ SCCOf(r, i) \subseteq executed[r]
        /\ PrunedDeps(r, i) \subseteq executed[r]

Position(order, i) == CHOOSE pos \in 1..Len(order) : order[pos] = i
Before(order, left, right) ==
    /\ left \in SeqSet(order)
    /\ right \in SeqSet(order)
    /\ Position(order, left) < Position(order, right)

DeterministicWithinSCC ==
    \A r \in Replicas :
      \A left, right \in executed[r] :
        left # right /\ SameSCC(r, left, right) =>
            (Before(execLog[r], left, right) <=> TupleRefLess(left, right))

DependencyOrder ==
    \A r \in Replicas :
      \A i \in executed[r] :
        \A dep \in PrunedDeps(r, i) :
          ~SameSCC(r, i, dep) => Before(execLog[r], dep, i)

PruneOnlyKnownAfter ==
    \A r \in Replicas :
      \A i \in ChosenVertices(r) :
        \A dep \in RawDeps(r, i) \ PrunedDeps(r, i) :
          /\ StatusAtLeast(recordStatus[r][dep], Committed)
          /\ i \in RawDeps(r, dep)
          /\ chosenTuple[dep].seq > chosenTuple[i].seq

PruningPreservesOrder ==
    \A r \in Replicas :
      \A i, dep \in executed[r] :
        dep \in RawDeps(r, i) \ PrunedDeps(r, i) =>
            Before(execLog[r], i, dep)

NoExecutionPastUnknownDependency ==
    \A r \in Replicas :
      \A i \in executed[r] :
        \A dep \in RawDeps(r, i) :
          dep \in executed[r] \/ KnownAfter(r, i, dep)

NoExecutionPastUnresolvedConflict ==
    \A r \in Replicas :
      \A base \in executed[r] :
        \A other \in Instances :
          /\ recordStatus[r][other] \in {PreAccepted, Accepted}
          /\ other \notin executed[r]
          /\ CommandsConflict(learned[r][base], learned[r][other])
          => KnownAfterAt(r, base, other, PreAccepted)

CommittedGraphExcludesUncommittedEvidence ==
    \A r \in Replicas, from, to \in Instances :
        Edge(r, from, to) =>
            /\ recordStatus[r][from] \in ChosenStatuses
            /\ recordStatus[r][to] \in ChosenStatuses

ConflictingExecutionConsistency ==
    \A leftReplica, rightReplica \in Replicas, left, right \in Instances :
        /\ left # right
        /\ CommandsConflict(chosenTuple[left], chosenTuple[right])
        /\ {left, right} \subseteq executed[leftReplica] \cap executed[rightReplica]
        => (Before(execLog[leftReplica], left, right) =
            Before(execLog[rightReplica], left, right))

RecoveryRequestsOnlyChosenDependencies ==
    \A r \in Replicas :
        recoveryRequested[r] \subseteq UNION {ChosenDeps(i) : i \in Instances}

ScenarioContracts ==
    /\ (caseKind = "sparse-prefix" =>
           /\ B \in ChosenDeps(A)
           /\ D \in ChosenDeps(A)
           /\ A \in ChosenDeps(D)
           /\ B[1] = D[1]
           /\ B[2] < D[2]
           /\ chosenTuple[D].seq > chosenTuple[A].seq)
    /\ (caseKind \in {"higher-preaccepted", "higher-accepted"} =>
           /\ A \in ExpandDeps(B, EvidenceTuple(caseKind, B))
           /\ EvidenceTuple(caseKind, B).seq > chosenTuple[A].seq)
    /\ (caseKind = "equal-preaccepted" =>
           /\ A \in ExpandDeps(B, EvidenceTuple(caseKind, B))
           /\ EvidenceTuple(caseKind, B).seq = chosenTuple[A].seq)
    /\ (caseKind = "missing-reverse-preaccepted" =>
           A \notin ExpandDeps(B, EvidenceTuple(caseKind, B)))

Safety ==
    /\ LearnedAgreesWithChosen
    /\ ReplicaPairSymmetry
    /\ SCCPartition
    /\ EqualLearnedGraphHasEqualSCCs
    /\ ExecuteOnlyReadySinkSCC
    /\ DeterministicWithinSCC
    /\ DependencyOrder
    /\ PruneOnlyKnownAfter
    /\ PruningPreservesOrder
    /\ NoExecutionPastUnknownDependency
    /\ NoExecutionPastUnresolvedConflict
    /\ CommittedGraphExcludesUncommittedEvidence
    /\ ConflictingExecutionConsistency
    /\ RecoveryRequestsOnlyChosenDependencies
    /\ ScenarioContracts

AllChosenLearned ==
    \A r \in Replicas, i \in Instances : learned[r][i] = chosenTuple[i]
AllExecutableExecuted == \A r \in Replicas : executed[r] = Instances
EventuallyLearnedExecutableCommandsExecute ==
    <> (AllChosenLearned /\ AllExecutableExecuted)

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(Next)

====
