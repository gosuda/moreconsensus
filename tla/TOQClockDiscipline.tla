---- MODULE TOQClockDiscipline ----
EXTENDS Naturals, Integers, FiniteSets

(***************************************************************************)
(* Finite operational model for the TOQ clock-discipline assumption used by *)
(* Config.TOQ. It does not implement clock synchronization or delay          *)
(* measurement; it checks the contract an embedding application must satisfy:*)
(* if every receiver clock differs from the origin clock by at most          *)
(* SkewBound and every one-way delay is at most MaxDelay, then choosing      *)
(* ProcessAt = send-local-time + MaxDelay + SkewBound guarantees every sync  *)
(* group member has received the PreAccept by the real time its local clock  *)
(* reaches ProcessAt.                                                       *)
(***************************************************************************)

CONSTANTS A, B, C, MaxDelay, SkewBound

Replicas == {A, B, C}
ASSUME MaxDelay \in Nat
ASSUME SkewBound \in Nat

VARIABLES origin, sendLocal, offset, actualDelay, processAt

Vars == <<origin, sendLocal, offset, actualDelay, processAt>>

ClockSkewOK ==
    \A r \in Replicas :
        \A s \in Replicas :
            offset[r] - offset[s] <= SkewBound /\ offset[s] - offset[r] <= SkewBound

TypeOK ==
    /\ origin \in Replicas
    /\ sendLocal \in SkewBound..(SkewBound + 2)
    /\ offset \in [Replicas -> 0..SkewBound]
    /\ actualDelay \in [Replicas -> 0..MaxDelay]
    /\ processAt \in Nat
    /\ ClockSkewOK

Init ==
    /\ origin \in Replicas
    /\ sendLocal \in SkewBound..(SkewBound + 2)
    /\ offset \in [Replicas -> 0..SkewBound]
    /\ actualDelay \in [Replicas -> 0..MaxDelay]
    /\ processAt = sendLocal + MaxDelay + SkewBound

Next ==
    UNCHANGED Vars

SendRealTime == sendLocal - offset[origin]
ArrivalRealTime(r) == SendRealTime + actualDelay[r]
LocalProcessRealTime(r) == processAt - offset[r]

EveryReceiverHasMessageAtProcessAt ==
    \A r \in Replicas : ArrivalRealTime(r) <= LocalProcessRealTime(r)

NoEarlyProcessingRequired ==
    \A r \in Replicas : LocalProcessRealTime(r) >= SendRealTime

Safety ==
    /\ TypeOK
    /\ EveryReceiverHasMessageAtProcessAt
    /\ NoEarlyProcessingRequired

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
