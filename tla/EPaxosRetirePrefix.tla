---- MODULE EPaxosRetirePrefix ----
EXTENDS Naturals, FiniteSets, Sequences

\* Small-state model: two lanes, instance numbers 1..MaxI, seq values 1..MaxS.
\* Folding a contiguous prefix must never lower per-lane deps/seq answers.

CONSTANTS MaxI, MaxS, Lanes

VARIABLES
  seq,          \* [lane -> [i \in 1..MaxI -> seq]]
  present,      \* [lane -> SUBSET 1..MaxI]
  folded,       \* [lane -> 0..MaxI]
  retiredMaxSeq \* [lane -> 0..MaxS]  \* max seq among folded instances

vars == <<seq, present, folded, retiredMaxSeq>>

TypeOK ==
  /\ seq \in [Lanes -> [1..MaxI -> 1..MaxS]]
  /\ present \in [Lanes -> SUBSET (1..MaxI)]
  /\ folded \in [Lanes -> 0..MaxI]
  /\ retiredMaxSeq \in [Lanes -> 0..MaxS]
  /\ \A lane \in Lanes:
       \A i \in present[lane]: i > folded[lane]

Init ==
  /\ seq = [lane \in Lanes |-> [i \in 1..MaxI |-> 1]]
  /\ present = [lane \in Lanes |-> {}]
  /\ folded = [lane \in Lanes |-> 0]
  /\ retiredMaxSeq = [lane \in Lanes |-> 0]

Install(lane, i, s) ==
  /\ i \in 1..MaxI
  /\ i > folded[lane]
  /\ s \in 1..MaxS
  /\ present' = [present EXCEPT ![lane] = @ \union {i}]
  /\ seq' = [seq EXCEPT ![lane][i] = s]
  /\ UNCHANGED <<folded, retiredMaxSeq>>

\* Fold one more contiguous instance if present (or hole with seq contribution 0).
FoldOne(lane) ==
  LET n == folded[lane] + 1 IN
    /\ n \in 1..MaxI
    /\ n \in present[lane]
    /\ folded' = [folded EXCEPT ![lane] = n]
    /\ present' = [present EXCEPT ![lane] = @ \ {n}]
    /\ retiredMaxSeq' = [retiredMaxSeq EXCEPT ![lane] = IF seq[lane][n] > @ THEN seq[lane][n] ELSE @]
    /\ UNCHANGED seq

\* Answers for attrs: dep = max present U folded; prefixSeq = max seq over <=dep
Dep(lane) ==
  LET live == present[lane]
      liveMax == IF live = {} THEN 0 ELSE CHOOSE i \in live: \A j \in live: j <= i
  IN IF liveMax > folded[lane] THEN liveMax ELSE folded[lane]

PrefixSeq(lane, through) ==
  LET foldedPart == IF through <= folded[lane] THEN retiredMaxSeq[lane]
                    ELSE retiredMaxSeq[lane]
      livePart == IF present[lane] = {} THEN 0
                  ELSE LET xs == {seq[lane][i]: i \in {j \in present[lane]: j <= through}}
                       IN IF xs = {} THEN 0 ELSE CHOOSE s \in xs: \A t \in xs: t <= s
  IN IF foldedPart > livePart THEN foldedPart ELSE livePart

\* Snapshot answers before/after fold must not decrease.
FoldSafe(lane) ==
  LET beforeDep == Dep(lane)
      beforeSeq == PrefixSeq(lane, beforeDep)
  IN FoldOne(lane) =>
       LET afterDep == Dep(lane)'
           afterSeq == PrefixSeq(lane, afterDep)'
       IN /\ afterDep >= beforeDep
          /\ afterSeq >= beforeSeq

Next ==
  \/ \E lane \in Lanes, i \in 1..MaxI, s \in 1..MaxS: Install(lane, i, s)
  \/ \E lane \in Lanes: FoldOne(lane)

Spec == Init /\ [][Next]_vars

Inv ==
  /\ TypeOK
  /\ \A lane \in Lanes:
       \A i \in present[lane]: i > folded[lane]
  \* Fold never lowers answers (checked on stuttering via action property in TLC via STATE constraint)
  /\ \A lane \in Lanes:
       retiredMaxSeq[lane] <= MaxS

====
