---- MODULE EPaxosRollbackAllocation ----
EXTENDS Naturals, FiniteSets, Sequences

(***************************************************************************)
(* Focused finite rollback-allocation model. It checks the implementation    *)
(* rule needed when a replica restarts from an older local checkpoint, then   *)
(* learns a later instance for its own replica from quorum state before       *)
(* accepting new proposals. Any observed local ref must advance nextInstance. *)
(* Proposal allocation must also skip already-known local refs, even if it    *)
(* sees a defensive stale-next state. The model is finite and does not model  *)
(* full EPaxos recovery, message loss, storage checksums, or commands.        *)
(***************************************************************************)

VARIABLES stage, known, committed, applySeq, nextInstance, checkpointKnown,
          checkpointNext, checkpointApplySeq, allocatedAfterRollback

First == 1
LearnedAfterCheckpoint == 2
KnownFutureBeforeAllocation == 3
MaxInstance == 4
FreshAfterSkip == KnownFutureBeforeAllocation + 1
InstanceNums == First..MaxInstance
None == 0

Vars == <<stage, known, committed, applySeq, nextInstance, checkpointKnown,
          checkpointNext, checkpointApplySeq, allocatedAfterRollback>>

AdvancePast(ref, n) == IF ref >= n THEN ref + 1 ELSE n

AppliedSet(seq) == {seq[i] : i \in 1..Len(seq)}

NoDuplicateSeq(seq) ==
    \A i, j \in 1..Len(seq) : (seq[i] = seq[j]) => i = j

FirstFree(k, n) ==
    CHOOSE i \in InstanceNums :
        /\ i >= n
        /\ i \notin k
        /\ \A j \in InstanceNums : (j >= n /\ j < i) => j \in k

TypeOK ==
    /\ stage \in 0..6
    /\ known \subseteq InstanceNums
    /\ committed \subseteq InstanceNums
    /\ applySeq \in Seq(InstanceNums)
    /\ Len(applySeq) <= MaxInstance
    /\ NoDuplicateSeq(applySeq)
    /\ nextInstance \in 1..(MaxInstance + 1)
    /\ checkpointKnown \subseteq InstanceNums
    /\ checkpointNext \in 1..(MaxInstance + 1)
    /\ checkpointApplySeq \in Seq(InstanceNums)
    /\ Len(checkpointApplySeq) <= MaxInstance
    /\ NoDuplicateSeq(checkpointApplySeq)
    /\ allocatedAfterRollback \in InstanceNums \cup {None}
    /\ AppliedSet(applySeq) \subseteq known
    /\ committed \subseteq known
    /\ committed \subseteq AppliedSet(applySeq)
    /\ AppliedSet(checkpointApplySeq) \subseteq checkpointKnown

Init ==
    /\ stage = 0
    /\ known = {}
    /\ committed = {}
    /\ applySeq = <<>>
    /\ nextInstance = First
    /\ checkpointKnown = {}
    /\ checkpointNext = First
    /\ checkpointApplySeq = <<>>
    /\ allocatedAfterRollback = None

CommitBeforeCheckpoint ==
    /\ stage = 0
    /\ nextInstance = First
    /\ stage' = 1
    /\ known' = known \cup {First}
    /\ committed' = committed \cup {First}
    /\ applySeq' = Append(applySeq, First)
    /\ nextInstance' = First + 1
    /\ checkpointKnown' = known \cup {First}
    /\ checkpointNext' = First + 1
    /\ checkpointApplySeq' = Append(applySeq, First)
    /\ UNCHANGED allocatedAfterRollback

CommitAfterCheckpoint ==
    /\ stage = 1
    /\ nextInstance = LearnedAfterCheckpoint
    /\ stage' = 2
    /\ known' = known \cup {LearnedAfterCheckpoint}
    /\ committed' = committed \cup {LearnedAfterCheckpoint}
    /\ applySeq' = Append(applySeq, LearnedAfterCheckpoint)
    /\ nextInstance' = LearnedAfterCheckpoint + 1
    /\ UNCHANGED <<checkpointKnown, checkpointNext, checkpointApplySeq,
                  allocatedAfterRollback>>

RestoreCheckpoint ==
    /\ stage = 2
    /\ stage' = 3
    /\ known' = checkpointKnown
    /\ committed' = checkpointKnown
    /\ applySeq' = checkpointApplySeq
    /\ nextInstance' = checkpointNext
    /\ UNCHANGED <<checkpointKnown, checkpointNext, checkpointApplySeq,
                  allocatedAfterRollback>>

LearnOwnCommitFromQuorum ==
    /\ stage = 3
    /\ stage' = 4
    /\ known' = known \cup {LearnedAfterCheckpoint}
    /\ committed' = committed \cup {LearnedAfterCheckpoint}
    /\ applySeq' = Append(applySeq, LearnedAfterCheckpoint)
    /\ nextInstance' = AdvancePast(LearnedAfterCheckpoint, nextInstance)
    /\ UNCHANGED <<checkpointKnown, checkpointNext, checkpointApplySeq,
                  allocatedAfterRollback>>

DiscoverKnownFutureLocalRefWithoutAdvance ==
    /\ stage = 4
    /\ nextInstance = KnownFutureBeforeAllocation
    /\ stage' = 5
    /\ known' = known \cup {KnownFutureBeforeAllocation}
    /\ UNCHANGED <<committed, applySeq, nextInstance, checkpointKnown,
                  checkpointNext, checkpointApplySeq, allocatedAfterRollback>>

AllocateAfterRollbackCatchup ==
    /\ stage = 5
    /\ LET fresh == FirstFree(known, nextInstance) IN
        /\ fresh = FreshAfterSkip
        /\ stage' = 6
        /\ known' = known \cup {fresh}
        /\ committed' = committed \cup {fresh}
        /\ applySeq' = Append(applySeq, fresh)
        /\ nextInstance' = fresh + 1
        /\ allocatedAfterRollback' = fresh
    /\ UNCHANGED <<checkpointKnown, checkpointNext, checkpointApplySeq>>

Next ==
    \/ CommitBeforeCheckpoint
    \/ CommitAfterCheckpoint
    \/ RestoreCheckpoint
    \/ LearnOwnCommitFromQuorum
    \/ DiscoverKnownFutureLocalRefWithoutAdvance
    \/ AllocateAfterRollbackCatchup

CheckpointIsOlderThanQuorumCommit ==
    stage >= 3 =>
        /\ First \in checkpointKnown
        /\ LearnedAfterCheckpoint \notin checkpointKnown
        /\ checkpointNext = LearnedAfterCheckpoint

LearningLocalRefAdvancesNext ==
    stage >= 4 => nextInstance > LearnedAfterCheckpoint

DefensiveStateContainsKnownFutureRef ==
    stage >= 5 => KnownFutureBeforeAllocation \in known

AllocationSkipsKnownFutureRef ==
    stage = 6 =>
        /\ allocatedAfterRollback = FreshAfterSkip
        /\ allocatedAfterRollback > KnownFutureBeforeAllocation

ApplySequenceContainsLearnedBeforeFresh ==
    stage = 6 =>
        \E learnedPos, freshPos \in 1..Len(applySeq) :
            /\ applySeq[learnedPos] = LearnedAfterCheckpoint
            /\ applySeq[freshPos] = allocatedAfterRollback
            /\ learnedPos < freshPos

Safety ==
    /\ CheckpointIsOlderThanQuorumCommit
    /\ LearningLocalRefAdvancesNext
    /\ DefensiveStateContainsKnownFutureRef
    /\ AllocationSkipsKnownFutureRef
    /\ ApplySequenceContainsLearnedBeforeFresh

EventuallyAllocatesFreshAfterRollback ==
    <> (stage = 6 /\ allocatedAfterRollback = FreshAfterSkip)

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(CommitBeforeCheckpoint)
    /\ WF_Vars(CommitAfterCheckpoint)
    /\ WF_Vars(RestoreCheckpoint)
    /\ WF_Vars(LearnOwnCommitFromQuorum)
    /\ WF_Vars(DiscoverKnownFutureLocalRefWithoutAdvance)
    /\ WF_Vars(AllocateAfterRollbackCatchup)

====
