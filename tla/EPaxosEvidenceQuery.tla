---- MODULE EPaxosEvidenceQuery ----
EXTENDS Naturals, FiniteSets

CONSTANTS ReplicaCount

(*
This focused finite model covers the committed-conflict TryPreAccept
read-only evidence query implemented in epaxos/node.go:
  - maybeStartTryEvidenceCheck guards before MsgEvidence broadcast,
  - handleEvidence read-only response construction,
  - handleEvidenceResp duplicate response drop,
  - tryEvidenceDecision / tryEvidenceRecordDecision sender-preserving
    evidence validation and fail-closed slow-accept fallback,
  - timerTryPreAccept failPendingTryEvidenceCheck fallback.
It intentionally remains finite evidence for configured odd cluster sizes, not an
unbounded recovery proof.
*)

VARIABLES phase,
          records,
          candidateCoversConflict,
          sameConfig,
          alreadyIgnored,
          evidenceRequested,
          authorized,
          failClosed,
          slowAcceptStarted,
          duplicateDropped,
          mismatchedDropped,
          staleRejectRestarted,
          readOnlyMutated

Replicas == 1..ReplicaCount
Coordinator == 1
Remotes == Replicas \ {Coordinator}
SlowQuorum == (ReplicaCount \div 2) + 1
FaultTolerance == ReplicaCount - SlowQuorum

Phases == {"try_pre_accept", "evidence", "ignore_broadcast", "slow_accept", "prepare"}
RecordKinds == {
    "unseen",
    "none",
    "same-valid-committed",
    "same-valid-value",
    "same-missing-evidence",
    "same-legacy-only",
    "same-bad-sender",
    "same-duplicate-sender",
    "same-no-target-dep",
    "different-value"
}
CommittedTupleRecords == {"same-valid-committed", "same-missing-evidence", "same-legacy-only", "same-bad-sender", "same-duplicate-sender", "same-no-target-dep"}
UsableEvidenceRecords == {"same-valid-committed", "same-valid-value"}
InvalidEvidenceRecords == {"same-missing-evidence", "same-legacy-only", "same-bad-sender", "same-duplicate-sender", "same-no-target-dep"}

Vars == <<phase,
          records,
          candidateCoversConflict,
          sameConfig,
          alreadyIgnored,
          evidenceRequested,
          authorized,
          failClosed,
          slowAcceptStarted,
          duplicateDropped,
          mismatchedDropped,
          staleRejectRestarted,
          readOnlyMutated>>

Responded(rs) == {r \in Remotes : rs[r] # "unseen"}
AllRemoteEvidenceResponses(rs) == Cardinality(Responded(rs)) >= Cardinality(Remotes)
TupleKnown(rs) == \E r \in Remotes : rs[r] \in CommittedTupleRecords
UsableEvidenceSenders(rs) == {r \in Remotes : rs[r] \in UsableEvidenceRecords}
InvalidEvidenceSeen(rs) == \E r \in Remotes : rs[r] \in InvalidEvidenceRecords
DifferentValueSeen(rs) == \E r \in Remotes : rs[r] = "different-value"

CanAuthorize(rs) ==
    /\ TupleKnown(rs)
    /\ Cardinality(UsableEvidenceSenders(rs)) >= FaultTolerance
    /\ ~InvalidEvidenceSeen(rs)
    /\ ~DifferentValueSeen(rs)

MustFailClosed(rs) ==
    IF TupleKnown(rs)
    THEN \/ InvalidEvidenceSeen(rs)
         \/ DifferentValueSeen(rs)
         \/ (AllRemoteEvidenceResponses(rs) /\ Cardinality(UsableEvidenceSenders(rs)) < FaultTolerance)
    ELSE AllRemoteEvidenceResponses(rs)

ResolvePhase(rs) ==
    IF MustFailClosed(rs)
    THEN "slow_accept"
    ELSE IF CanAuthorize(rs)
         THEN "ignore_broadcast"
         ELSE "evidence"

Init ==
    /\ ReplicaCount \in {3, 5, 7}
    /\ phase = "try_pre_accept"
    /\ records = [r \in Remotes |-> "unseen"]
    /\ candidateCoversConflict \in BOOLEAN
    /\ sameConfig \in BOOLEAN
    /\ alreadyIgnored \in BOOLEAN
    /\ evidenceRequested = FALSE
    /\ authorized = FALSE
    /\ failClosed = FALSE
    /\ slowAcceptStarted = FALSE
    /\ duplicateDropped = FALSE
    /\ mismatchedDropped = FALSE
    /\ staleRejectRestarted = FALSE
    /\ readOnlyMutated = FALSE

TypeOK ==
    /\ ReplicaCount \in {3, 5, 7}
    /\ phase \in Phases
    /\ records \in [Remotes -> RecordKinds]
    /\ candidateCoversConflict \in BOOLEAN
    /\ sameConfig \in BOOLEAN
    /\ alreadyIgnored \in BOOLEAN
    /\ evidenceRequested \in BOOLEAN
    /\ authorized \in BOOLEAN
    /\ failClosed \in BOOLEAN
    /\ slowAcceptStarted \in BOOLEAN
    /\ duplicateDropped \in BOOLEAN
    /\ mismatchedDropped \in BOOLEAN
    /\ staleRejectRestarted \in BOOLEAN
    /\ readOnlyMutated \in BOOLEAN

StartEvidenceCheck ==
    /\ phase = "try_pre_accept"
    /\ IF candidateCoversConflict /\ sameConfig /\ FaultTolerance > 0 /\ ~alreadyIgnored
       THEN /\ phase' = "evidence"
            /\ evidenceRequested' = TRUE
            /\ UNCHANGED <<records, authorized, failClosed, slowAcceptStarted, duplicateDropped, mismatchedDropped, staleRejectRestarted, readOnlyMutated>>
       ELSE /\ phase' = "slow_accept"
            /\ slowAcceptStarted' = TRUE
            /\ UNCHANGED <<records, evidenceRequested, authorized, failClosed, duplicateDropped, mismatchedDropped, staleRejectRestarted, readOnlyMutated>>
    /\ UNCHANGED <<candidateCoversConflict, sameConfig, alreadyIgnored>>

StaleBallotTryPreAcceptReject ==
    /\ phase = "try_pre_accept"
    /\ phase' = "prepare"
    /\ staleRejectRestarted' = TRUE
    /\ UNCHANGED <<records, candidateCoversConflict, sameConfig, alreadyIgnored, evidenceRequested, authorized, failClosed, slowAcceptStarted, duplicateDropped, mismatchedDropped, readOnlyMutated>>

RecordEvidenceResponse(r, kind) ==
    /\ phase = "evidence"
    /\ r \in Remotes
    /\ records[r] = "unseen"
    /\ kind \in RecordKinds \ {"unseen"}
    /\ LET nextRecords == [records EXCEPT ![r] = kind]
           nextPhase == ResolvePhase(nextRecords)
       IN /\ records' = nextRecords
          /\ phase' = nextPhase
          /\ authorized' = (nextPhase = "ignore_broadcast")
          /\ failClosed' = (nextPhase = "slow_accept")
          /\ slowAcceptStarted' = (nextPhase = "slow_accept")
    /\ readOnlyMutated' = FALSE
    /\ UNCHANGED <<candidateCoversConflict, sameConfig, alreadyIgnored, evidenceRequested, duplicateDropped, mismatchedDropped, staleRejectRestarted>>

DuplicateEvidenceResponse(r) ==
    /\ phase = "evidence"
    /\ r \in Remotes
    /\ records[r] # "unseen"
    /\ duplicateDropped' = TRUE
    /\ readOnlyMutated' = FALSE
    /\ UNCHANGED <<phase, records, candidateCoversConflict, sameConfig, alreadyIgnored, evidenceRequested, authorized, failClosed, slowAcceptStarted, mismatchedDropped, staleRejectRestarted>>

MismatchedEvidenceResponse ==
    /\ phase = "evidence"
    /\ mismatchedDropped' = TRUE
    /\ readOnlyMutated' = FALSE
    /\ UNCHANGED <<phase, records, candidateCoversConflict, sameConfig, alreadyIgnored, evidenceRequested, authorized, failClosed, slowAcceptStarted, duplicateDropped, staleRejectRestarted>>

TimerFailClosed ==
    /\ phase = "evidence"
    /\ phase' = "slow_accept"
    /\ failClosed' = TRUE
    /\ slowAcceptStarted' = TRUE
    /\ readOnlyMutated' = FALSE
    /\ UNCHANGED <<records, candidateCoversConflict, sameConfig, alreadyIgnored, evidenceRequested, authorized, duplicateDropped, mismatchedDropped, staleRejectRestarted>>

Next ==
    \/ StartEvidenceCheck
    \/ StaleBallotTryPreAcceptReject
    \/ \E r \in Remotes, kind \in RecordKinds \ {"unseen"} : RecordEvidenceResponse(r, kind)
    \/ \E r \in Remotes : DuplicateEvidenceResponse(r)
    \/ MismatchedEvidenceResponse
    \/ TimerFailClosed

Spec == Init /\ [][Next]_Vars

NoIgnoreWithoutStartGuards ==
    phase = "ignore_broadcast" => candidateCoversConflict /\ sameConfig /\ FaultTolerance > 0 /\ ~alreadyIgnored

NoIgnoreWithoutEvidenceCheck ==
    phase = "ignore_broadcast" => evidenceRequested /\ authorized /\ ~failClosed

AuthorizationRequiresSenderPreservingEvidence ==
    authorized => CanAuthorize(records)

LegacyAggregateNeverAuthorizes ==
    (\E r \in Remotes : records[r] = "same-legacy-only") => ~authorized

MissingOrMalformedSenderEvidenceNeverAuthorizes ==
    (\E r \in Remotes : records[r] \in {"same-missing-evidence", "same-bad-sender", "same-duplicate-sender", "same-no-target-dep"}) => ~authorized

DifferentTupleNeverAuthorizes ==
    DifferentValueSeen(records) => ~authorized

FailClosedStartsSlowAccept ==
    failClosed => phase = "slow_accept" /\ slowAcceptStarted /\ ~authorized

StaleBallotDoesNotStartEvidence ==
    staleRejectRestarted => phase = "prepare" /\ ~evidenceRequested /\ ~authorized

ReadOnlyEvidenceResponsesDoNotMutate ==
    ~readOnlyMutated

Safety ==
    /\ NoIgnoreWithoutStartGuards
    /\ NoIgnoreWithoutEvidenceCheck
    /\ AuthorizationRequiresSenderPreservingEvidence
    /\ LegacyAggregateNeverAuthorizes
    /\ MissingOrMalformedSenderEvidenceNeverAuthorizes
    /\ DifferentTupleNeverAuthorizes
    /\ FailClosedStartsSlowAccept
    /\ StaleBallotDoesNotStartEvidence
    /\ ReadOnlyEvidenceResponsesDoNotMutate

====
