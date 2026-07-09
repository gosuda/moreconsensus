---- MODULE EPaxosRevisited ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused model for the EPaxos Revisited-inspired pieces implemented by    *)
(* this repository:                                                         *)
(*   1. explicit TOQ PreAccept envelopes: TOQ flag, zero sequence, and empty *)
(*      dependencies;                                                       *)
(*   2. delayed originator dependency assignment at ProcessAt, including the *)
(*      rule that a pending TOQ local assignment cannot decide;              *)
(*   3. deterministic receiver ProcessAt processing and fast-wait behavior;  *)
(*   4. execution chain pruning when a later known conflict already depends  *)
(*      on the base instance and has a strictly greater sequence number.     *)
(* The model abstracts physical clock synchronization and one-way-delay      *)
(* measurement into a finite ProcessAt scalar; it checks the core ordering   *)
(* obligations, not a real deployment clock discipline.                      *)
(***************************************************************************)

CONSTANTS A, B, C, D

ASSUME Cardinality({A, B, C, D}) = 4

VARIABLES chainCase, chainExecuted, tick, queued, processedAt, ownerPending, ownerAssignedAt,
          outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, phase, commitReason

Instances == {A, B, C, D}
Followers == {2, 3}
SlowQuorum == 2
\* The timing heuristic keeps stale-originator remote-superset commits at
\* all-remote unanimity for this 3-replica model; this is intentionally
\* stricter than the normal optimized 3-node fast quorum.
FastQuorum == 3
ProcessAt == 1
FastWaitDeadline == 2
TickLimit == 3
Unprocessed == TickLimit + 1

ChainCases == {
    "safe-chain",
    "uncommitted-safe",
    "lower-seq",
    "equal-seq",
    "missing-reverse",
    "unknown-dependency"
}
SafeChainCases == {"safe-chain", "uncommitted-safe"}
UnsafeChainCases == ChainCases \ SafeChainCases
TimingCases == {"remote-fast", "fast-wait-timeout"}
Statuses == {"none", "preaccepted", "accepted", "committed"}
Phases == {"preaccept", "fast-wait", "accepted", "committed"}
CommitReasons == {"none", "remote-fast"}
Vars == <<chainCase, chainExecuted, tick, queued, processedAt, ownerPending, ownerAssignedAt,
          outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, phase, commitReason>>

StatusRank(s) ==
    CASE s = "none" -> 0
      [] s = "preaccepted" -> 1
      [] s = "accepted" -> 2
      [] s = "committed" -> 3

Seq(i) ==
    CASE i = A -> IF chainCase \in {"lower-seq"} THEN 2 ELSE 1
      [] i = B -> IF chainCase = "lower-seq" THEN 1 ELSE IF chainCase = "equal-seq" THEN 1 ELSE 2
      [] i = C -> 3
      [] i = D -> 0

Deps(i) ==
    CASE chainCase = "safe-chain" ->
            CASE i = A -> {B}
              [] i = B -> {A, C}
              [] i = C -> {D}
              [] OTHER -> {}
      [] chainCase = "uncommitted-safe" ->
            CASE i = B -> {A}
              [] OTHER -> {}
      [] chainCase = "missing-reverse" ->
            {}
      [] chainCase = "unknown-dependency" ->
            CASE i = A -> {B}
              [] OTHER -> {}
      [] OTHER ->
            CASE i = B -> {A}
              [] OTHER -> {}

RecStatus(i) ==
    CASE i = A -> "committed"
      [] i = B -> IF chainCase \in {"safe-chain"} THEN "committed" ELSE IF chainCase = "unknown-dependency" THEN "none" ELSE "accepted"
      [] i = C -> IF chainCase = "safe-chain" THEN "committed" ELSE "none"
      [] OTHER -> "none"

KnownAfter(base, other, minStatus) ==
    /\ RecStatus(other) \in Statuses
    /\ StatusRank(RecStatus(other)) >= StatusRank(minStatus)
    /\ Seq(other) > Seq(base)
    /\ base \in Deps(other)

UnresolvedKnownConflict(base) ==
    \E other \in Instances \ {base} :
        /\ other \notin chainExecuted
        /\ RecStatus(other) \in {"preaccepted", "accepted"}
        /\ ~KnownAfter(base, other, "preaccepted")

Ready(base) ==
    /\ RecStatus(base) = "committed"
    /\ base \notin chainExecuted
    /\ ~UnresolvedKnownConflict(base)
    /\ \A dep \in Deps(base) :
        \/ dep \in chainExecuted
        \/ KnownAfter(base, dep, "committed")

AllowedReplies == IF timingCase = "remote-fast" THEN Followers ELSE {2}

TypeOK ==
    /\ chainCase \in ChainCases
    /\ chainExecuted \subseteq Instances
    /\ tick \in 0..TickLimit
    /\ queued \in BOOLEAN
    /\ processedAt \in 0..Unprocessed
    /\ ownerPending \in BOOLEAN
    /\ ownerAssignedAt \in 0..Unprocessed
    /\ outboundTOQ \in BOOLEAN
    /\ outboundSeqZero \in BOOLEAN
    /\ outboundDepsEmpty \in BOOLEAN
    /\ timingCase \in TimingCases
    /\ votes \subseteq Followers
    /\ votes \subseteq AllowedReplies
    /\ phase \in Phases
    /\ commitReason \in CommitReasons

Init ==
    /\ chainCase \in ChainCases
    /\ chainExecuted = {}
    /\ tick = 0
    /\ queued = TRUE
    /\ processedAt = Unprocessed
    /\ ownerPending = TRUE
    /\ ownerAssignedAt = Unprocessed
    /\ outboundTOQ = TRUE
    /\ outboundSeqZero = TRUE
    /\ outboundDepsEmpty = TRUE
    /\ timingCase \in TimingCases
    /\ votes = {}
    /\ phase = "preaccept"
    /\ commitReason = "none"

ExecuteA ==
    /\ Ready(A)
    /\ chainExecuted' = chainExecuted \cup {A}
    /\ UNCHANGED <<chainCase, tick, queued, processedAt, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, phase, commitReason>>

Tick ==
    /\ tick < TickLimit
    /\ tick' = tick + 1
    /\ UNCHANGED <<chainCase, chainExecuted, queued, processedAt, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, phase, commitReason>>

ProcessQueued ==
    /\ queued
    /\ tick >= ProcessAt
    /\ queued' = FALSE
    /\ processedAt' = tick
    /\ UNCHANGED <<chainCase, chainExecuted, tick, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, phase, commitReason>>

AssignOwnerTOQ ==
    /\ ownerPending
    /\ tick >= ProcessAt
    /\ ownerPending' = FALSE
    /\ ownerAssignedAt' = tick
    /\ UNCHANGED <<chainCase, chainExecuted, tick, queued, processedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, phase, commitReason>>

ReceiveDivergentVote(r) ==
    LET newVotes == votes \cup {r}
    IN
    /\ ~queued
    /\ phase \in {"preaccept", "fast-wait"}
    /\ r \in AllowedReplies \ votes
    /\ votes' = newVotes
    /\ IF Cardinality(newVotes) + 1 >= FastQuorum /\ ~ownerPending
       THEN /\ phase' = "committed"
            /\ commitReason' = "remote-fast"
       ELSE /\ phase' = phase
            /\ commitReason' = commitReason
    /\ UNCHANGED <<chainCase, chainExecuted, tick, queued, processedAt, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase>>

FinalizeBufferedFast ==
    /\ ~ownerPending
    /\ phase \in {"preaccept", "fast-wait"}
    /\ Cardinality(votes) + 1 >= FastQuorum
    /\ phase' = "committed"
    /\ commitReason' = "remote-fast"
    /\ UNCHANGED <<chainCase, chainExecuted, tick, queued, processedAt, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes>>

StartFastWait ==
    /\ ~ownerPending
    /\ phase = "preaccept"
    /\ Cardinality(votes) + 1 >= SlowQuorum
    /\ Cardinality(votes) + 1 < FastQuorum
    /\ tick < FastWaitDeadline
    /\ phase' = "fast-wait"
    /\ UNCHANGED <<chainCase, chainExecuted, tick, queued, processedAt, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, commitReason>>

FastCommitHigherAttrs ==
    /\ ~ownerPending
    /\ phase \in {"preaccept", "fast-wait"}
    /\ Cardinality(votes) + 1 >= FastQuorum
    /\ votes = Followers
    /\ processedAt # Unprocessed
    /\ phase' = "committed"
    /\ commitReason' = "remote-fast"
    /\ UNCHANGED <<chainCase, chainExecuted, tick, queued, processedAt, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes>>

SlowAcceptAfterFastWait ==
    /\ ~ownerPending
    /\ phase \in {"preaccept", "fast-wait"}
    /\ Cardinality(votes) + 1 >= SlowQuorum
    /\ Cardinality(votes) + 1 < FastQuorum
    /\ tick >= FastWaitDeadline
    /\ phase' = "accepted"
    /\ UNCHANGED <<chainCase, chainExecuted, tick, queued, processedAt, ownerPending, ownerAssignedAt,
                  outboundTOQ, outboundSeqZero, outboundDepsEmpty, timingCase, votes, commitReason>>

Next ==
    \/ ExecuteA
    \/ Tick
    \/ ProcessQueued
    \/ AssignOwnerTOQ
    \/ \E r \in Followers : ReceiveDivergentVote(r)
    \/ FinalizeBufferedFast
    \/ StartFastWait
    \/ FastCommitHigherAttrs
    \/ SlowAcceptAfterFastWait

SafeChainExecutesOnlySafeCases ==
    A \in chainExecuted => chainCase \in SafeChainCases

UnsafeChainsNeverExecuteBase ==
    chainCase \in UnsafeChainCases => A \notin chainExecuted

TOQOutboundPreAcceptIsExplicit ==
    outboundTOQ /\ outboundSeqZero /\ outboundDepsEmpty

OwnerAssignsOnlyAtOrAfterProcessAt ==
    ownerAssignedAt # Unprocessed => ownerAssignedAt >= ProcessAt

OwnerPendingHasNoAssignedAttrs ==
    ownerPending => ownerAssignedAt = Unprocessed

TOQPendingBlocksDecision ==
    ownerPending => phase = "preaccept"

QueuedPreAcceptProcessedOnlyAtOrAfterProcessAt ==
    processedAt # Unprocessed => processedAt >= ProcessAt

FastWaitRequiresSlowQuorum ==
    phase = "fast-wait" => Cardinality(votes) + 1 >= SlowQuorum /\ Cardinality(votes) + 1 < FastQuorum

SlowAcceptRequiresTimedDeadline ==
    phase = "accepted" => Cardinality(votes) + 1 >= SlowQuorum /\ Cardinality(votes) + 1 < FastQuorum /\ tick >= FastWaitDeadline

RemoteFastCommitRequiresUnanimousFastQuorum ==
    commitReason = "remote-fast" => phase = "committed" /\ votes = Followers /\ Cardinality(votes) + 1 >= FastQuorum /\ processedAt # Unprocessed /\ ownerAssignedAt # Unprocessed

Safety ==
    /\ SafeChainExecutesOnlySafeCases
    /\ UnsafeChainsNeverExecuteBase
    /\ TOQOutboundPreAcceptIsExplicit
    /\ OwnerAssignsOnlyAtOrAfterProcessAt
    /\ OwnerPendingHasNoAssignedAttrs
    /\ TOQPendingBlocksDecision
    /\ QueuedPreAcceptProcessedOnlyAtOrAfterProcessAt
    /\ FastWaitRequiresSlowQuorum
    /\ SlowAcceptRequiresTimedDeadline
    /\ RemoteFastCommitRequiresUnanimousFastQuorum

EventuallySafeChainBaseExecutes ==
    [](chainCase \in SafeChainCases => <> (A \in chainExecuted))

EventuallyTimedDecision ==
    <>(phase \in {"accepted", "committed"})

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(ExecuteA)
    /\ WF_Vars(Tick)
    /\ WF_Vars(ProcessQueued)
    /\ WF_Vars(AssignOwnerTOQ)
    /\ \A r \in Followers : WF_Vars(ReceiveDivergentVote(r))
    /\ WF_Vars(FinalizeBufferedFast)
    /\ WF_Vars(StartFastWait)
    /\ WF_Vars(FastCommitHigherAttrs)
    /\ WF_Vars(SlowAcceptAfterFastWait)

====
