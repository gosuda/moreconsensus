---- MODULE EPaxosResponses ----
EXTENDS Naturals, FiniteSets

CONSTANTS ReplicaCount

VARIABLES phase, preOK, preMatch, accOK, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch

Replicas == 1..ReplicaCount
Leader == 1

\* Repository response-quorum model: optimized EPaxos paper quorum for odd
\* N=2F+1; even sizes keep the conservative implementation quorum.
SlowQuorum == (ReplicaCount \div 2) + 1
FastQuorum ==
    IF ReplicaCount \in {1, 3, 5, 7}
    THEN IF ReplicaCount = 1 THEN 1 ELSE (ReplicaCount \div 2) + (((ReplicaCount \div 2) + 1) \div 2)
    ELSE ReplicaCount - ((ReplicaCount - 1) \div 4)
TryWitnessQuorum == FastQuorum + SlowQuorum - ReplicaCount
MaxBallot == ReplicaCount + 2
Ballots == 0..MaxBallot
NextAcceptBallot == acceptBallot + 1


Phases == {"preaccept", "accept", "prepare", "try_pre_accept", "committed"}
CommitReasons == {"none", "fast", "accept", "prepare"}
PrepareBranches == {"none", "committed", "accept_from_accepted", "try_pre_accept", "accept_from_preaccepted", "noop"}
Vars == <<phase, preOK, preMatch, accOK, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch>>

PrepareEvidenceCount == preparedCommittedCount + preparedAcceptedCount + preparedPreacceptedCount

TypeOK ==
    /\ ReplicaCount \in 1..7
    /\ phase \in Phases
    /\ preOK \subseteq Replicas
    /\ preMatch \subseteq preOK
    /\ accOK \subseteq Replicas
    /\ acceptBallot \in Ballots
    /\ prepareOK \subseteq Replicas
    /\ wrongBallotAcceptOK \in BOOLEAN
    /\ preparedCommittedCount \in 0..ReplicaCount
    /\ preparedAcceptedCount \in 0..ReplicaCount
    /\ preparedPreacceptedCount \in 0..ReplicaCount
    /\ tryPreAcceptWitnessCount \in 0..ReplicaCount
    /\ tryPreAcceptWitnessCount <= preparedPreacceptedCount
    /\ PrepareEvidenceCount <= Cardinality(prepareOK)
    /\ attrsMerged \in BOOLEAN
    /\ acceptedAfterPrepare \in BOOLEAN
    /\ committedBy \in CommitReasons
    /\ prepareBranch \in PrepareBranches

Init ==
    /\ phase = "preaccept"
    /\ preOK = {Leader}
    /\ preMatch = {Leader}
    /\ accOK = {}
    /\ acceptBallot = 0
    /\ wrongBallotAcceptOK = FALSE
    /\ prepareOK = {}
    /\ preparedCommittedCount = 0
    /\ preparedAcceptedCount = 0
    /\ preparedPreacceptedCount = 0
    /\ tryPreAcceptWitnessCount = 0
    /\ attrsMerged = FALSE
    /\ acceptedAfterPrepare = FALSE
    /\ committedBy = "none"
    /\ prepareBranch = "none"

PreAcceptMatch(r) ==
    LET newPreOK == preOK \cup {r}
        newPreMatch == preMatch \cup {r}
    IN
    /\ phase = "preaccept"
    /\ r \in Replicas \ preOK
    /\ preOK' = newPreOK
    /\ preMatch' = newPreMatch
    /\ IF Cardinality(newPreOK) >= FastQuorum /\ newPreOK = newPreMatch
       THEN /\ phase' = "committed"
            /\ committedBy' = "fast"
            /\ UNCHANGED <<accOK, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, prepareBranch>>
       ELSE /\ phase' = phase
            /\ UNCHANGED <<accOK, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch>>

PreAcceptDiverge(r) ==
    LET newPreOK == preOK \cup {r}
    IN
    /\ phase = "preaccept"
    /\ r \in Replicas \ preOK
    /\ preOK' = newPreOK
    /\ preMatch' = preMatch
    /\ IF Cardinality(newPreOK) >= SlowQuorum
       THEN /\ phase' = "accept"
            /\ attrsMerged' = TRUE
            /\ UNCHANGED <<accOK, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, acceptedAfterPrepare, committedBy, prepareBranch>>
       ELSE /\ phase' = phase
            /\ UNCHANGED <<accOK, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch>>

PreAcceptReject ==
    /\ phase = "preaccept"
    /\ acceptBallot < MaxBallot
    /\ phase' = "prepare"
    /\ accOK' = {}
    /\ acceptBallot' = NextAcceptBallot
    /\ UNCHANGED <<preOK, preMatch, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch>>

AcceptOK(r, ballot) ==
    \* A non-reject AcceptOK is the same message class for every ballot; only the current accept ballot can enter accOK.
    LET newAccOK == accOK \cup {r}
    IN
    /\ phase = "accept"
    /\ r \in Replicas
    /\ ballot \in Ballots
    /\ IF ballot = acceptBallot /\ r \notin accOK
       THEN /\ accOK' = newAccOK
            /\ IF Cardinality(newAccOK) >= SlowQuorum
               THEN /\ phase' = "committed"
                    /\ committedBy' = "accept"
                    /\ UNCHANGED <<preOK, preMatch, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, prepareBranch>>
               ELSE /\ phase' = phase
                    /\ UNCHANGED <<preOK, preMatch, acceptBallot, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch>>
       ELSE IF ballot # acceptBallot
            THEN /\ wrongBallotAcceptOK' = TRUE
                 /\ UNCHANGED <<phase, preOK, preMatch, accOK, acceptBallot, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch>>
            ELSE /\ UNCHANGED Vars

AcceptReject ==
    /\ phase = "accept"
    /\ acceptBallot < MaxBallot
    /\ phase' = "prepare"
    /\ accOK' = {}
    /\ acceptBallot' = NextAcceptBallot
    /\ UNCHANGED <<preOK, preMatch, wrongBallotAcceptOK, prepareOK, preparedCommittedCount, preparedAcceptedCount, preparedPreacceptedCount, tryPreAcceptWitnessCount, attrsMerged, acceptedAfterPrepare, committedBy, prepareBranch>>

PrepareOutcome(newPrepareOK, newPreparedCommittedCount, newPreparedAcceptedCount, newPreparedPreacceptedCount, newTryPreAcceptWitnessCount) ==
    IF Cardinality(newPrepareOK) >= SlowQuorum
    THEN IF newPreparedCommittedCount > 0
         THEN /\ phase' = "committed"
              /\ committedBy' = "prepare"
              /\ prepareBranch' = "committed"
              /\ attrsMerged' = attrsMerged
              /\ acceptedAfterPrepare' = acceptedAfterPrepare
         ELSE IF newPreparedAcceptedCount > 0
         THEN /\ phase' = "accept"
              /\ committedBy' = committedBy
              /\ prepareBranch' = "accept_from_accepted"
              /\ attrsMerged' = TRUE
              /\ acceptedAfterPrepare' = TRUE
         ELSE IF newPreparedPreacceptedCount > 0 /\ newTryPreAcceptWitnessCount >= TryWitnessQuorum
         THEN /\ phase' = "try_pre_accept"
              /\ committedBy' = committedBy
              /\ prepareBranch' = "try_pre_accept"
              /\ attrsMerged' = TRUE
              /\ acceptedAfterPrepare' = FALSE
         ELSE IF newPreparedPreacceptedCount > 0
         THEN /\ phase' = "accept"
              /\ committedBy' = committedBy
              /\ prepareBranch' = "accept_from_preaccepted"
              /\ attrsMerged' = TRUE
              /\ acceptedAfterPrepare' = TRUE
         ELSE /\ phase' = "accept"
              /\ committedBy' = committedBy
              /\ prepareBranch' = "noop"
              /\ attrsMerged' = attrsMerged
              /\ acceptedAfterPrepare' = TRUE
    ELSE /\ phase' = phase
         /\ committedBy' = committedBy
         /\ prepareBranch' = prepareBranch
         /\ attrsMerged' = attrsMerged
         /\ acceptedAfterPrepare' = acceptedAfterPrepare

PrepareNone(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommittedCount == preparedCommittedCount
        newPreparedAcceptedCount == preparedAcceptedCount
        newPreparedPreacceptedCount == preparedPreacceptedCount
        newTryPreAcceptWitnessCount == tryPreAcceptWitnessCount
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommittedCount' = newPreparedCommittedCount
    /\ preparedAcceptedCount' = newPreparedAcceptedCount
    /\ preparedPreacceptedCount' = newPreparedPreacceptedCount
    /\ tryPreAcceptWitnessCount' = newTryPreAcceptWitnessCount
    /\ PrepareOutcome(newPrepareOK, newPreparedCommittedCount, newPreparedAcceptedCount, newPreparedPreacceptedCount, newTryPreAcceptWitnessCount)
    /\ UNCHANGED <<preOK, preMatch, accOK, acceptBallot, wrongBallotAcceptOK>>

PrepareAccepted(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommittedCount == preparedCommittedCount
        newPreparedAcceptedCount == preparedAcceptedCount + 1
        newPreparedPreacceptedCount == preparedPreacceptedCount
        newTryPreAcceptWitnessCount == tryPreAcceptWitnessCount
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommittedCount' = newPreparedCommittedCount
    /\ preparedAcceptedCount' = newPreparedAcceptedCount
    /\ preparedPreacceptedCount' = newPreparedPreacceptedCount
    /\ tryPreAcceptWitnessCount' = newTryPreAcceptWitnessCount
    /\ PrepareOutcome(newPrepareOK, newPreparedCommittedCount, newPreparedAcceptedCount, newPreparedPreacceptedCount, newTryPreAcceptWitnessCount)
    /\ UNCHANGED <<preOK, preMatch, accOK, acceptBallot, wrongBallotAcceptOK>>

PreparePreacceptedFastEligible(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommittedCount == preparedCommittedCount
        newPreparedAcceptedCount == preparedAcceptedCount
        newPreparedPreacceptedCount == preparedPreacceptedCount + 1
        newTryPreAcceptWitnessCount == tryPreAcceptWitnessCount + 1
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommittedCount' = newPreparedCommittedCount
    /\ preparedAcceptedCount' = newPreparedAcceptedCount
    /\ preparedPreacceptedCount' = newPreparedPreacceptedCount
    /\ tryPreAcceptWitnessCount' = newTryPreAcceptWitnessCount
    /\ PrepareOutcome(newPrepareOK, newPreparedCommittedCount, newPreparedAcceptedCount, newPreparedPreacceptedCount, newTryPreAcceptWitnessCount)
    /\ UNCHANGED <<preOK, preMatch, accOK, acceptBallot, wrongBallotAcceptOK>>

PreparePreacceptedIncompatible(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommittedCount == preparedCommittedCount
        newPreparedAcceptedCount == preparedAcceptedCount
        newPreparedPreacceptedCount == preparedPreacceptedCount + 1
        newTryPreAcceptWitnessCount == tryPreAcceptWitnessCount
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommittedCount' = newPreparedCommittedCount
    /\ preparedAcceptedCount' = newPreparedAcceptedCount
    /\ preparedPreacceptedCount' = newPreparedPreacceptedCount
    /\ tryPreAcceptWitnessCount' = newTryPreAcceptWitnessCount
    /\ PrepareOutcome(newPrepareOK, newPreparedCommittedCount, newPreparedAcceptedCount, newPreparedPreacceptedCount, newTryPreAcceptWitnessCount)
    /\ UNCHANGED <<preOK, preMatch, accOK, acceptBallot, wrongBallotAcceptOK>>

PrepareCommitted(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommittedCount == preparedCommittedCount + 1
        newPreparedAcceptedCount == preparedAcceptedCount
        newPreparedPreacceptedCount == preparedPreacceptedCount
        newTryPreAcceptWitnessCount == tryPreAcceptWitnessCount
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommittedCount' = newPreparedCommittedCount
    /\ preparedAcceptedCount' = newPreparedAcceptedCount
    /\ preparedPreacceptedCount' = newPreparedPreacceptedCount
    /\ tryPreAcceptWitnessCount' = newTryPreAcceptWitnessCount
    /\ PrepareOutcome(newPrepareOK, newPreparedCommittedCount, newPreparedAcceptedCount, newPreparedPreacceptedCount, newTryPreAcceptWitnessCount)
    /\ UNCHANGED <<preOK, preMatch, accOK, acceptBallot, wrongBallotAcceptOK>>

Next ==
    \/ \E r \in Replicas : PreAcceptMatch(r)
    \/ \E r \in Replicas : PreAcceptDiverge(r)
    \/ PreAcceptReject
    \/ \E r \in Replicas, ballot \in Ballots : AcceptOK(r, ballot)
    \/ AcceptReject
    \/ \E r \in Replicas : PrepareNone(r)
    \/ \E r \in Replicas : PrepareAccepted(r)
    \/ \E r \in Replicas : PreparePreacceptedFastEligible(r)
    \/ \E r \in Replicas : PreparePreacceptedIncompatible(r)
    \/ \E r \in Replicas : PrepareCommitted(r)

FastCommitRequiresFastExactVotes ==
    committedBy = "fast" => Cardinality(preOK) >= FastQuorum /\ preOK = preMatch

AcceptCommitRequiresSlowAcceptVotes ==
    committedBy = "accept" => Cardinality(accOK) >= SlowQuorum

WrongBallotAcceptOKDoesNotCount ==
    wrongBallotAcceptOK /\ Cardinality(accOK) < SlowQuorum => committedBy # "accept"

PrepareCommitRequiresSlowPrepareQuorumAndCommittedVote ==
    committedBy = "prepare" => Cardinality(prepareOK) >= SlowQuorum /\ preparedCommittedCount > 0 /\ prepareBranch = "committed"

SlowAcceptRequiresQuorumEvidence ==
    phase = "accept" =>
        \/ Cardinality(preOK) >= SlowQuorum /\ attrsMerged
        \/ Cardinality(prepareOK) >= SlowQuorum
           /\ prepareBranch \in {"accept_from_accepted", "accept_from_preaccepted", "noop"}
           /\ acceptedAfterPrepare

NoCommitWithoutQuorumEvidence ==
    phase = "committed" =>
        \/ committedBy = "fast" /\ Cardinality(preOK) >= FastQuorum /\ preOK = preMatch
        \/ committedBy = "accept" /\ Cardinality(accOK) >= SlowQuorum
        \/ committedBy = "prepare" /\ Cardinality(prepareOK) >= SlowQuorum /\ preparedCommittedCount > 0 /\ prepareBranch = "committed"

NoPrepareBranchBeforeQuorum ==
    Cardinality(prepareOK) < SlowQuorum => prepareBranch = "none"

PrepareQuorumSelectsExactlyOneBranch ==
    Cardinality(prepareOK) >= SlowQuorum =>
        \/ prepareBranch = "committed"
           /\ preparedCommittedCount > 0
        \/ prepareBranch = "accept_from_accepted"
           /\ preparedCommittedCount = 0
           /\ preparedAcceptedCount > 0
        \/ prepareBranch = "try_pre_accept"
           /\ preparedCommittedCount = 0
           /\ preparedAcceptedCount = 0
           /\ preparedPreacceptedCount > 0
           /\ tryPreAcceptWitnessCount >= TryWitnessQuorum
        \/ prepareBranch = "accept_from_preaccepted"
           /\ preparedCommittedCount = 0
           /\ preparedAcceptedCount = 0
           /\ preparedPreacceptedCount > 0
           /\ tryPreAcceptWitnessCount < TryWitnessQuorum
        \/ prepareBranch = "noop"
           /\ preparedCommittedCount = 0
           /\ preparedAcceptedCount = 0
           /\ preparedPreacceptedCount = 0

CommittedPrepareEvidenceWins ==
    Cardinality(prepareOK) >= SlowQuorum /\ preparedCommittedCount > 0 => prepareBranch = "committed"

AcceptedPrepareEvidenceWinsWithoutCommitted ==
    Cardinality(prepareOK) >= SlowQuorum /\ preparedCommittedCount = 0 /\ preparedAcceptedCount > 0 =>
        prepareBranch = "accept_from_accepted"

TryPreAcceptRequiresFastEligibleWitnesses ==
    prepareBranch = "try_pre_accept" =>
        /\ preparedCommittedCount = 0
        /\ preparedAcceptedCount = 0
        /\ preparedPreacceptedCount > 0
        /\ tryPreAcceptWitnessCount >= TryWitnessQuorum

AcceptFromPreacceptedRequiresInsufficientTryWitnesses ==
    prepareBranch = "accept_from_preaccepted" =>
        /\ preparedCommittedCount = 0
        /\ preparedAcceptedCount = 0
        /\ preparedPreacceptedCount > 0
        /\ tryPreAcceptWitnessCount < TryWitnessQuorum

NoopRequiresAllNonePrepareQuorum ==
    prepareBranch = "noop" =>
        /\ Cardinality(prepareOK) >= SlowQuorum
        /\ preparedCommittedCount = 0
        /\ preparedAcceptedCount = 0
        /\ preparedPreacceptedCount = 0

PrepareEvidenceCountsFitQuorum ==
    /\ tryPreAcceptWitnessCount <= preparedPreacceptedCount
    /\ PrepareEvidenceCount <= Cardinality(prepareOK)

Safety ==
    /\ FastCommitRequiresFastExactVotes
    /\ AcceptCommitRequiresSlowAcceptVotes
    /\ PrepareCommitRequiresSlowPrepareQuorumAndCommittedVote
    /\ WrongBallotAcceptOKDoesNotCount
    /\ SlowAcceptRequiresQuorumEvidence
    /\ NoCommitWithoutQuorumEvidence
    /\ NoPrepareBranchBeforeQuorum
    /\ PrepareQuorumSelectsExactlyOneBranch
    /\ CommittedPrepareEvidenceWins
    /\ AcceptedPrepareEvidenceWinsWithoutCommitted
    /\ TryPreAcceptRequiresFastEligibleWitnesses
    /\ AcceptFromPreacceptedRequiresInsufficientTryWitnesses
    /\ NoopRequiresAllNonePrepareQuorum
    /\ PrepareEvidenceCountsFitQuorum

Spec == Init /\ [][Next]_Vars

====
