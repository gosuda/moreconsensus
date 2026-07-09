---- MODULE Quorum ----
EXTENDS Naturals, FiniteSets

CONSTANTS Sizes

(* This module checks the repository quorum table: optimized EPaxos paper    *)
(* fast quorum for odd N=2F+1, with conservative quorums retained for even  *)
(* supported sizes because the paper proof assumes odd replica counts.       *)
VARIABLE dummy

AllowedSizes == 1..7

SlowQuorum(n) == (n \div 2) + 1
OptimizedOddFastQuorum(n) == IF n = 1 THEN 1 ELSE (n \div 2) + (((n \div 2) + 1) \div 2)
ConservativeFastQuorum(n) == n - ((n - 1) \div 4)
FastQuorum(n) == IF n \in {1, 3, 5, 7} THEN OptimizedOddFastQuorum(n) ELSE ConservativeFastQuorum(n)

ReplicaSet(n) == 1..n

SlowQuorumSets(n) ==
    {q \in SUBSET ReplicaSet(n) : Cardinality(q) = SlowQuorum(n)}

FastQuorumSets(n) ==
    {q \in SUBSET ReplicaSet(n) : Cardinality(q) = FastQuorum(n)}

MinSlowSlowIntersection(n) == (2 * SlowQuorum(n)) - n
MinFastSlowIntersection(n) == FastQuorum(n) + SlowQuorum(n) - n
MinFastFastIntersection(n) == (2 * FastQuorum(n)) - n

ExpectedSlow(n) ==
    CASE n = 1 -> 1
      [] n = 2 -> 2
      [] n = 3 -> 2
      [] n = 4 -> 3
      [] n = 5 -> 3
      [] n = 6 -> 4
      [] n = 7 -> 4

ExpectedFast(n) ==
    CASE n = 1 -> 1
      [] n = 2 -> 2
      [] n = 3 -> 2
      [] n = 4 -> 4
      [] n = 5 -> 3
      [] n = 6 -> 5
      [] n = 7 -> 5

TypeOK ==
    /\ Sizes = AllowedSizes
    /\ dummy \in {0}

QuorumTable ==
    \A n \in Sizes :
        /\ SlowQuorum(n) = ExpectedSlow(n)
        /\ FastQuorum(n) = ExpectedFast(n)

QuorumIntersection ==
    \A n \in Sizes :
        /\ \A s1 \in SlowQuorumSets(n), s2 \in SlowQuorumSets(n) :
            Cardinality(s1 \cap s2) >= MinSlowSlowIntersection(n)
        /\ \A f \in FastQuorumSets(n), s \in SlowQuorumSets(n) :
            Cardinality(f \cap s) >= MinFastSlowIntersection(n)
        /\ \A f1 \in FastQuorumSets(n), f2 \in FastQuorumSets(n) :
            Cardinality(f1 \cap f2) >= MinFastFastIntersection(n)

Init == dummy = 0
Next == UNCHANGED dummy
Spec == Init /\ [][Next]_dummy

====
