---- MODULE EPaxosEvidenceStaleness ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Focused finite committed-conflict evidence staleness model. It checks    *)
(* two request-scoped cases implemented by handleEvidenceResp:              *)
(* 1. an older-ballot MsgEvidenceResp for the same target/conflict is       *)
(*    dropped before it can populate records[From];                         *)
(* 2. within the current ballot, one remote voter first returns empty        *)
(*    evidence, then returns a fresher duplicate committed tuple. Evidence  *)
(*    is keyed by sender, so the first same-ballot response wins, the later  *)
(*    duplicate cannot authorize a TryPreAccept ignore resend, and all       *)
(*    recorded empty remote responses fail closed to slow Accept.            *)
(* This does not model an arbitrary network, retries, message loss, or       *)
(* unbounded recovery.                                                       *)
(***************************************************************************)

VARIABLES phase, records, recordBallots, targetStatus, checkPresent,
          oldBallotDropped, duplicateFreshAttempted, duplicateDropped,
          ignoredBroadcast, slowAcceptStarted, acceptDepsConflict

Coordinator == 1
FirstVoter == 2
OtherVoter == 3
Voters == {Coordinator, FirstVoter, OtherVoter}
Remotes == Voters \ {Coordinator}
FaultTolerance == 1
OldBallot == "old"
CurrentBallot == "current"
NoBallot == "none"

Phases == {"evidence", "old-ballot-dropped", "first-stale-recorded",
           "fresh-duplicate-dropped", "all-empty-recorded", "slow-accept"}
RecordKinds == {"unseen", "empty", "fresh-committed"}
BallotKinds == {NoBallot, OldBallot, CurrentBallot}
TargetStatuses == {"try_pre_accept", "accepted"}

Vars == <<phase, records, recordBallots, targetStatus, checkPresent,
          oldBallotDropped, duplicateFreshAttempted, duplicateDropped,
          ignoredBroadcast, slowAcceptStarted, acceptDepsConflict>>

Responded(rs) == {r \in Remotes : rs[r] # "unseen"}
AllRemoteEvidenceResponses(rs) == Cardinality(Responded(rs)) = Cardinality(Remotes)
TupleKnown(rs) == \E r \in Remotes : rs[r] = "fresh-committed"
UsableEvidenceSenders(rs) == {r \in Remotes : rs[r] = "fresh-committed"}
CanAuthorize(rs) == TupleKnown(rs) /\ Cardinality(UsableEvidenceSenders(rs)) >= FaultTolerance

TypeOK ==
    /\ phase \in Phases
    /\ records \in [Remotes -> RecordKinds]
    /\ recordBallots \in [Remotes -> BallotKinds]
    /\ targetStatus \in TargetStatuses
    /\ checkPresent \in BOOLEAN
    /\ oldBallotDropped \in BOOLEAN
    /\ duplicateFreshAttempted \in BOOLEAN
    /\ duplicateDropped \in BOOLEAN
    /\ ignoredBroadcast \in BOOLEAN
    /\ slowAcceptStarted \in BOOLEAN
    /\ acceptDepsConflict \in BOOLEAN

Init ==
    /\ phase = "evidence"
    /\ records = [r \in Remotes |-> "unseen"]
    /\ recordBallots = [r \in Remotes |-> NoBallot]
    /\ targetStatus = "try_pre_accept"
    /\ checkPresent = TRUE
    /\ oldBallotDropped = FALSE
    /\ duplicateFreshAttempted = FALSE
    /\ duplicateDropped = FALSE
    /\ ignoredBroadcast = FALSE
    /\ slowAcceptStarted = FALSE
    /\ acceptDepsConflict = FALSE

OldBallotStaleResponseDropped ==
    /\ phase = "evidence"
    /\ checkPresent
    /\ OldBallot # CurrentBallot
    /\ oldBallotDropped' = TRUE
    /\ phase' = "old-ballot-dropped"
    /\ records' = records
    /\ recordBallots' = recordBallots
    /\ UNCHANGED <<targetStatus, checkPresent, duplicateFreshAttempted,
                  duplicateDropped, ignoredBroadcast, slowAcceptStarted,
                  acceptDepsConflict>>

FirstSameBallotStaleEmptyResponse ==
    /\ phase = "old-ballot-dropped"
    /\ checkPresent
    /\ records[FirstVoter] = "unseen"
    /\ records' = [records EXCEPT ![FirstVoter] = "empty"]
    /\ recordBallots' = [recordBallots EXCEPT ![FirstVoter] = CurrentBallot]
    /\ phase' = "first-stale-recorded"
    /\ UNCHANGED <<targetStatus, checkPresent, oldBallotDropped,
                  duplicateFreshAttempted, duplicateDropped, ignoredBroadcast,
                  slowAcceptStarted, acceptDepsConflict>>

FreshDuplicateFromSameVoter ==
    /\ phase = "first-stale-recorded"
    /\ checkPresent
    /\ records[FirstVoter] = "empty"
    /\ recordBallots[FirstVoter] = CurrentBallot
    /\ duplicateFreshAttempted' = TRUE
    /\ duplicateDropped' = TRUE
    /\ phase' = "fresh-duplicate-dropped"
    /\ records' = records
    /\ recordBallots' = recordBallots
    /\ UNCHANGED <<targetStatus, checkPresent, oldBallotDropped,
                  ignoredBroadcast, slowAcceptStarted, acceptDepsConflict>>

OtherSameBallotEmptyResponse ==
    /\ phase = "fresh-duplicate-dropped"
    /\ checkPresent
    /\ records[OtherVoter] = "unseen"
    /\ records' = [records EXCEPT ![OtherVoter] = "empty"]
    /\ recordBallots' = [recordBallots EXCEPT ![OtherVoter] = CurrentBallot]
    /\ phase' = "all-empty-recorded"
    /\ UNCHANGED <<targetStatus, checkPresent, oldBallotDropped,
                  duplicateFreshAttempted, duplicateDropped, ignoredBroadcast,
                  slowAcceptStarted, acceptDepsConflict>>

ResolveFailClosed ==
    /\ phase = "all-empty-recorded"
    /\ checkPresent
    /\ AllRemoteEvidenceResponses(records)
    /\ ~TupleKnown(records)
    /\ ~CanAuthorize(records)
    /\ phase' = "slow-accept"
    /\ targetStatus' = "accepted"
    /\ checkPresent' = FALSE
    /\ slowAcceptStarted' = TRUE
    /\ acceptDepsConflict' = TRUE
    /\ ignoredBroadcast' = FALSE
    /\ UNCHANGED <<records, recordBallots, oldBallotDropped,
                  duplicateFreshAttempted, duplicateDropped>>

Next ==
    \/ OldBallotStaleResponseDropped
    \/ FirstSameBallotStaleEmptyResponse
    \/ FreshDuplicateFromSameVoter
    \/ OtherSameBallotEmptyResponse
    \/ ResolveFailClosed

OldBallotResponseNeverRecords ==
    phase = "old-ballot-dropped" =>
        /\ records = [r \in Remotes |-> "unseen"]
        /\ recordBallots = [r \in Remotes |-> NoBallot]

RecordedEvidenceIsCurrentBallot ==
    \A r \in Remotes : records[r] # "unseen" => recordBallots[r] = CurrentBallot

FirstResponseWinsBySender ==
    phase \in {"first-stale-recorded", "fresh-duplicate-dropped",
               "all-empty-recorded", "slow-accept"} => records[FirstVoter] = "empty"

FreshDuplicateNeverAuthorizes ==
    duplicateFreshAttempted =>
        /\ duplicateDropped
        /\ records[FirstVoter] = "empty"
        /\ recordBallots[FirstVoter] = CurrentBallot
        /\ ~CanAuthorize(records)
        /\ ~ignoredBroadcast

FailClosedStartsSlowAccept ==
    phase = "slow-accept" =>
        /\ targetStatus = "accepted"
        /\ ~checkPresent
        /\ slowAcceptStarted
        /\ acceptDepsConflict
        /\ ~ignoredBroadcast

AllRemoteEmptyCannotAuthorize ==
    AllRemoteEvidenceResponses(records) /\ ~TupleKnown(records) => ~CanAuthorize(records)

Safety ==
    /\ OldBallotResponseNeverRecords
    /\ RecordedEvidenceIsCurrentBallot
    /\ FirstResponseWinsBySender
    /\ FreshDuplicateNeverAuthorizes
    /\ FailClosedStartsSlowAccept
    /\ AllRemoteEmptyCannotAuthorize

EventuallyCoversStaleDuplicateFailClosed ==
    <> ( /\ phase = "slow-accept"
         /\ oldBallotDropped
         /\ records[FirstVoter] = "empty"
         /\ records[OtherVoter] = "empty"
         /\ recordBallots[FirstVoter] = CurrentBallot
         /\ recordBallots[OtherVoter] = CurrentBallot
         /\ duplicateFreshAttempted
         /\ duplicateDropped
         /\ targetStatus = "accepted"
         /\ ~checkPresent
         /\ slowAcceptStarted
         /\ acceptDepsConflict
         /\ ~ignoredBroadcast )

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(OldBallotStaleResponseDropped)
    /\ WF_Vars(FirstSameBallotStaleEmptyResponse)
    /\ WF_Vars(FreshDuplicateFromSameVoter)
    /\ WF_Vars(OtherSameBallotEmptyResponse)
    /\ WF_Vars(ResolveFailClosed)

====
