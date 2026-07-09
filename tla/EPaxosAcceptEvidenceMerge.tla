---- MODULE EPaxosAcceptEvidenceMerge ----
EXTENDS Naturals, FiniteSets, Sequences

(***************************************************************************)
(* Focused finite AcceptEvidence sender-merge and validation model. It       *)
(* checks one outbound merge slice and one inbound validation slice:          *)
(* 1. outbound recovery evidence skips sender 0 entries, coalesces repeated  *)
(*    sender evidence by maximum Seq and per-dependency maximum, and emits   *)
(*    at most one tuple for that sender;                                     *)
(* 2. inbound wire validation accepts identical duplicate sender evidence     *)
(*    but rejects duplicate same-sender tuples with conflicting attrs.        *)
(* This does not model arbitrary evidence histories, message delivery,        *)
(* recovery branch choices, or unbounded proof.                              *)
(***************************************************************************)

VARIABLES stage, outbound, inboundAccepted, validationError

Stages == {"start", "zero_sender_ignored", "same_sender_merged",
           "other_sender_appended", "inbound_duplicate_rejected"}
InboundResults == {"unknown", "accepted", "rejected"}
ValidationErrors == {"none", "duplicate_conflict"}
Senders == {1, 2}
DepIndexes == 1..3
EvidenceSeqs == 1..4
DepVals == 0..4
EvidenceTuple == [sender: Senders, seq: EvidenceSeqs, deps: [DepIndexes -> DepVals]]

ExistingSender1 == [sender |-> 1, seq |-> 2, deps |-> <<1, 0, 0>>]
IncomingSameSender == [sender |-> 1, seq |-> 3, deps |-> <<0, 4, 0>>]
OtherSender == [sender |-> 2, seq |-> 4, deps |-> <<0, 3, 0>>]
ZeroSenderEvidence == [sender |-> 0, seq |-> 4, deps |-> <<4, 4, 4>>]
CoalescedSender1 == [sender |-> 1, seq |-> 3, deps |-> <<1, 4, 0>>]
RawInboundDuplicate == <<ExistingSender1, IncomingSameSender>>
RawInboundIdenticalDuplicate == <<ExistingSender1, ExistingSender1>>

Vars == <<stage, outbound, inboundAccepted, validationError>>

MaxNat(a, b) == IF a >= b THEN a ELSE b
MaxDeps(a, b) == <<MaxNat(a[1], b[1]), MaxNat(a[2], b[2]), MaxNat(a[3], b[3])>>
MergeEvidence(a, b) ==
    [sender |-> a.sender,
     seq |-> MaxNat(a.seq, b.seq),
     deps |-> MaxDeps(a.deps, b.deps)]

SenderIndexes(evSeq, sender) == {i \in 1..Len(evSeq) : evSeq[i].sender = sender}
NoDuplicateSenders(evSeq) ==
    \A i, j \in 1..Len(evSeq) : i # j => evSeq[i].sender # evSeq[j].sender
NoConflictingDuplicates(evSeq) ==
    \A i, j \in 1..Len(evSeq) :
        i # j /\ evSeq[i].sender = evSeq[j].sender =>
            /\ evSeq[i].seq = evSeq[j].seq
            /\ evSeq[i].deps = evSeq[j].deps
ValidEvidence(ev) ==
    /\ ev.sender \in Senders
    /\ ev.seq \in EvidenceSeqs
    /\ ev.deps \in [DepIndexes -> DepVals]
InboundValid(evSeq) ==
    /\ \A i \in 1..Len(evSeq) : ValidEvidence(evSeq[i])
    /\ NoConflictingDuplicates(evSeq)
OutboundValid(evSeq) ==
    /\ InboundValid(evSeq)
    /\ NoDuplicateSenders(evSeq)

TypeOK ==
    /\ stage \in Stages
    /\ outbound \in Seq(EvidenceTuple)
    /\ inboundAccepted \in InboundResults
    /\ validationError \in ValidationErrors

Init ==
    /\ stage = "start"
    /\ outbound = <<ExistingSender1>>
    /\ inboundAccepted = "unknown"
    /\ validationError = "none"

IgnoreZeroSender ==
    /\ stage = "start"
    /\ ZeroSenderEvidence.sender = 0
    /\ outbound' = outbound
    /\ stage' = "zero_sender_ignored"
    /\ UNCHANGED <<inboundAccepted, validationError>>

MergeSameSender ==
    /\ stage = "zero_sender_ignored"
    /\ MergeEvidence(ExistingSender1, IncomingSameSender) = CoalescedSender1
    /\ outbound' = <<CoalescedSender1>>
    /\ stage' = "same_sender_merged"
    /\ UNCHANGED <<inboundAccepted, validationError>>

AppendDistinctSender ==
    /\ stage = "same_sender_merged"
    /\ OtherSender.sender # CoalescedSender1.sender
    /\ outbound' = Append(outbound, OtherSender)
    /\ stage' = "other_sender_appended"
    /\ UNCHANGED <<inboundAccepted, validationError>>

RejectInboundConflictingDuplicate ==
    /\ stage = "other_sender_appended"
    /\ ~InboundValid(RawInboundDuplicate)
    /\ InboundValid(RawInboundIdenticalDuplicate)
    /\ inboundAccepted' = "rejected"
    /\ validationError' = "duplicate_conflict"
    /\ stage' = "inbound_duplicate_rejected"
    /\ UNCHANGED outbound

Next == IgnoreZeroSender \/ MergeSameSender \/ AppendDistinctSender \/ RejectInboundConflictingDuplicate

OutboundNeverCarriesZeroSender ==
    \A i \in 1..Len(outbound) : outbound[i].sender # 0

ZeroSenderInputSkipped ==
    stage \in {"zero_sender_ignored", "same_sender_merged",
               "other_sender_appended", "inbound_duplicate_rejected"} =>
        /\ ZeroSenderEvidence.sender = 0
        /\ ZeroSenderEvidence \notin {outbound[i] : i \in 1..Len(outbound)}

OutboundEvidenceIsWireValid == OutboundValid(outbound)

SameSenderCoalescedOnce ==
    stage \in {"same_sender_merged", "other_sender_appended", "inbound_duplicate_rejected"} =>
        /\ outbound[1] = CoalescedSender1
        /\ Cardinality(SenderIndexes(outbound, 1)) = 1

DistinctSenderAppendedOnce ==
    stage \in {"other_sender_appended", "inbound_duplicate_rejected"} =>
        /\ Len(outbound) = 2
        /\ outbound[2] = OtherSender
        /\ Cardinality(SenderIndexes(outbound, 2)) = 1

InboundValidationRejectsConflictOnly ==
    /\ ~InboundValid(RawInboundDuplicate)
    /\ InboundValid(RawInboundIdenticalDuplicate)
    /\ (stage = "inbound_duplicate_rejected" =>
            /\ inboundAccepted = "rejected"
            /\ validationError = "duplicate_conflict")

Safety ==
    /\ OutboundNeverCarriesZeroSender
    /\ ZeroSenderInputSkipped
    /\ OutboundEvidenceIsWireValid
    /\ SameSenderCoalescedOnce
    /\ DistinctSenderAppendedOnce
    /\ InboundValidationRejectsConflictOnly

EventuallyCoversAcceptEvidenceMerge ==
    <> ( /\ stage = "inbound_duplicate_rejected"
         /\ outbound = <<CoalescedSender1, OtherSender>>
         /\ inboundAccepted = "rejected"
         /\ validationError = "duplicate_conflict" )

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
