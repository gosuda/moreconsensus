---- MODULE EPaxosRetirePrefix ----
EXTENDS Naturals, FiniteSets

\* Two lanes, instances 1..MaxI. Folding a present instance advances folded
\* and retains max seq. Invariant: folded instances are absent from present,
\* and retiredMaxSeq is monotonic and bounded.

CONSTANTS MaxI, MaxS, Lanes

VARIABLES seq, present, folded, retiredMaxSeq

vars == <<seq, present, folded, retiredMaxSeq>>

TypeOK ==
  /\ seq \in [Lanes -> [1..MaxI -> 1..MaxS]]
  /\ present \in [Lanes -> SUBSET (1..MaxI)]
  /\ folded \in [Lanes -> 0..MaxI]
  /\ retiredMaxSeq \in [Lanes -> 0..MaxS]

Init ==
  /\ seq = [lane \in Lanes |-> [i \in 1..MaxI |-> 1]]
  /\ present = [lane \in Lanes |-> {}]
  /\ folded = [lane \in Lanes |-> 0]
  /\ retiredMaxSeq = [lane \in Lanes |-> 0]

Install(lane, i, s) ==
  /\ i \in 1..MaxI
  /\ s \in 1..MaxS
  /\ i > folded[lane]
  /\ present' = [present EXCEPT ![lane] = @ \union {i}]
  /\ seq' = [seq EXCEPT ![lane][i] = s]
  /\ UNCHANGED <<folded, retiredMaxSeq>>

FoldOne(lane) ==
  /\ folded[lane] < MaxI
  /\ (folded[lane] + 1) \in present[lane]
  /\ LET n == folded[lane] + 1 IN
       /\ folded' = [folded EXCEPT ![lane] = n]
       /\ present' = [present EXCEPT ![lane] = @ \ {n}]
       /\ retiredMaxSeq' =
            [retiredMaxSeq EXCEPT ![lane] =
               IF seq[lane][n] > retiredMaxSeq[lane] THEN seq[lane][n] ELSE retiredMaxSeq[lane]]
       /\ UNCHANGED seq

Next ==
  \/ \E lane \in Lanes, i \in 1..MaxI, s \in 1..MaxS: Install(lane, i, s)
  \/ \E lane \in Lanes: FoldOne(lane)

Spec == Init /\ [][Next]_vars

\* Folded prefix is disjoint from present; retired max seq never exceeds MaxS
\* and is at least the seq of any still-folded boundary instance when defined.
Inv ==
  /\ TypeOK
  /\ \A lane \in Lanes:
       /\ \A i \in present[lane]: i > folded[lane]
       /\ retiredMaxSeq[lane] \in 0..MaxS
       /\ \A i \in 1..MaxI:
            (i <= folded[lane]) => (seq[lane][i] <= retiredMaxSeq[lane])

====
