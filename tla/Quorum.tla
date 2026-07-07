---- MODULE Quorum ----
EXTENDS Naturals

CONSTANTS Sizes
VARIABLE dummy

AllowedSizes == 1..7

SlowQuorum(n) == (n \div 2) + 1
FastQuorum(n) == n - ((n - 1) \div 4)

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
      [] n = 3 -> 3
      [] n = 4 -> 4
      [] n = 5 -> 4
      [] n = 6 -> 5
      [] n = 7 -> 6

TypeOK ==
    /\ Sizes = AllowedSizes
    /\ dummy \in {0}

QuorumTable ==
    \A n \in Sizes :
        /\ SlowQuorum(n) = ExpectedSlow(n)
        /\ FastQuorum(n) = ExpectedFast(n)

Init == dummy = 0
Next == UNCHANGED dummy
Spec == Init /\ [][Next]_dummy

====
