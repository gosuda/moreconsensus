---- MODULE EPaxosResponses ----
EXTENDS Naturals, FiniteSets

CONSTANTS ReplicaCount

VARIABLES phase, preOK, preMatch, accOK, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare, committedBy

Replicas == 1..ReplicaCount
Leader == 1

SlowQuorum == (ReplicaCount \div 2) + 1
FastQuorum == ReplicaCount - ((ReplicaCount - 1) \div 4)

Phases == {"preaccept", "accept", "prepare", "committed"}
CommitReasons == {"none", "fast", "accept", "prepare"}
Vars == <<phase, preOK, preMatch, accOK, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare, committedBy>>

TypeOK ==
    /\ ReplicaCount \in 1..7
    /\ phase \in Phases
    /\ preOK \subseteq Replicas
    /\ preMatch \subseteq preOK
    /\ accOK \subseteq Replicas
    /\ prepareOK \subseteq Replicas
    /\ preparedCommitted \subseteq prepareOK
    /\ preparedAccepted \subseteq prepareOK
    /\ attrsMerged \in BOOLEAN
    /\ acceptedAfterPrepare \in BOOLEAN
    /\ committedBy \in CommitReasons

Init ==
    /\ phase = "preaccept"
    /\ preOK = {Leader}
    /\ preMatch = {Leader}
    /\ accOK = {}
    /\ prepareOK = {}
    /\ preparedCommitted = {}
    /\ preparedAccepted = {}
    /\ attrsMerged = FALSE
    /\ acceptedAfterPrepare = FALSE
    /\ committedBy = "none"

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
            /\ UNCHANGED <<accOK, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare>>
       ELSE /\ phase' = phase
            /\ UNCHANGED <<accOK, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare, committedBy>>

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
            /\ UNCHANGED <<accOK, prepareOK, preparedCommitted, preparedAccepted, acceptedAfterPrepare, committedBy>>
       ELSE /\ phase' = phase
            /\ UNCHANGED <<accOK, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare, committedBy>>

PreAcceptReject ==
    /\ phase = "preaccept"
    /\ phase' = "prepare"
    /\ UNCHANGED <<preOK, preMatch, accOK, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare, committedBy>>

AcceptOK(r) ==
    LET newAccOK == accOK \cup {r}
    IN
    /\ phase = "accept"
    /\ r \in Replicas \ accOK
    /\ accOK' = newAccOK
    /\ IF Cardinality(newAccOK) >= SlowQuorum
       THEN /\ phase' = "committed"
            /\ committedBy' = "accept"
            /\ UNCHANGED <<preOK, preMatch, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare>>
       ELSE /\ phase' = phase
            /\ UNCHANGED <<preOK, preMatch, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare, committedBy>>

AcceptReject ==
    /\ phase = "accept"
    /\ phase' = "prepare"
    /\ UNCHANGED <<preOK, preMatch, accOK, prepareOK, preparedCommitted, preparedAccepted, attrsMerged, acceptedAfterPrepare, committedBy>>

PrepareOutcome(newPrepareOK, newPreparedCommitted, newPreparedAccepted) ==
    IF Cardinality(newPrepareOK) >= SlowQuorum
    THEN IF newPreparedCommitted # {}
         THEN /\ phase' = "committed"
              /\ committedBy' = "prepare"
              /\ attrsMerged' = attrsMerged
              /\ acceptedAfterPrepare' = acceptedAfterPrepare
         ELSE /\ phase' = "accept"
              /\ committedBy' = committedBy
              /\ attrsMerged' = (attrsMerged \/ newPreparedAccepted # {})
              /\ acceptedAfterPrepare' = TRUE
    ELSE /\ phase' = phase
         /\ committedBy' = committedBy
         /\ attrsMerged' = attrsMerged
         /\ acceptedAfterPrepare' = acceptedAfterPrepare

PrepareNone(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommitted == preparedCommitted
        newPreparedAccepted == preparedAccepted
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommitted' = newPreparedCommitted
    /\ preparedAccepted' = newPreparedAccepted
    /\ PrepareOutcome(newPrepareOK, newPreparedCommitted, newPreparedAccepted)
    /\ UNCHANGED <<preOK, preMatch, accOK>>

PrepareAccepted(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommitted == preparedCommitted
        newPreparedAccepted == preparedAccepted \cup {r}
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommitted' = newPreparedCommitted
    /\ preparedAccepted' = newPreparedAccepted
    /\ PrepareOutcome(newPrepareOK, newPreparedCommitted, newPreparedAccepted)
    /\ UNCHANGED <<preOK, preMatch, accOK>>

PrepareCommitted(r) ==
    LET newPrepareOK == prepareOK \cup {r}
        newPreparedCommitted == preparedCommitted \cup {r}
        newPreparedAccepted == preparedAccepted
    IN
    /\ phase = "prepare"
    /\ r \in Replicas \ prepareOK
    /\ prepareOK' = newPrepareOK
    /\ preparedCommitted' = newPreparedCommitted
    /\ preparedAccepted' = newPreparedAccepted
    /\ PrepareOutcome(newPrepareOK, newPreparedCommitted, newPreparedAccepted)
    /\ UNCHANGED <<preOK, preMatch, accOK>>

Next ==
    \/ \E r \in Replicas : PreAcceptMatch(r)
    \/ \E r \in Replicas : PreAcceptDiverge(r)
    \/ PreAcceptReject
    \/ \E r \in Replicas : AcceptOK(r)
    \/ AcceptReject
    \/ \E r \in Replicas : PrepareNone(r)
    \/ \E r \in Replicas : PrepareAccepted(r)
    \/ \E r \in Replicas : PrepareCommitted(r)

FastCommitRequiresFastExactVotes ==
    committedBy = "fast" => Cardinality(preOK) >= FastQuorum /\ preOK = preMatch

AcceptCommitRequiresSlowAcceptVotes ==
    committedBy = "accept" => Cardinality(accOK) >= SlowQuorum

PrepareCommitRequiresSlowPrepareQuorumAndCommittedVote ==
    committedBy = "prepare" => Cardinality(prepareOK) >= SlowQuorum /\ preparedCommitted # {}

SlowAcceptRequiresQuorumEvidence ==
    phase = "accept" =>
        \/ Cardinality(preOK) >= SlowQuorum /\ attrsMerged
        \/ Cardinality(prepareOK) >= SlowQuorum /\ preparedCommitted = {} /\ acceptedAfterPrepare

NoCommitWithoutQuorumEvidence ==
    phase = "committed" =>
        \/ committedBy = "fast" /\ Cardinality(preOK) >= FastQuorum /\ preOK = preMatch
        \/ committedBy = "accept" /\ Cardinality(accOK) >= SlowQuorum
        \/ committedBy = "prepare" /\ Cardinality(prepareOK) >= SlowQuorum /\ preparedCommitted # {}

Safety ==
    /\ FastCommitRequiresFastExactVotes
    /\ AcceptCommitRequiresSlowAcceptVotes
    /\ PrepareCommitRequiresSlowPrepareQuorumAndCommittedVote
    /\ SlowAcceptRequiresQuorumEvidence
    /\ NoCommitWithoutQuorumEvidence

Spec == Init /\ [][Next]_Vars

====
