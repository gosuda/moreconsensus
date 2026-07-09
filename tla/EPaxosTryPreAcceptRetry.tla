---- MODULE EPaxosTryPreAcceptRetry ----
EXTENDS Naturals

(***************************************************************************)
(* Focused finite TryPreAccept retry-timer model. It mirrors RawNode.onTimer *)
(* for timerTryPreAccept and failPendingTryEvidenceCheck: current-ballot     *)
(* pending evidence for the same target fails closed to slow Accept before a  *)
(* retry; stale same-target evidence is deleted and cannot fail the current   *)
(* recovery; unrelated-target evidence is retained; retry rebroadcasts carry  *)
(* either the normal TryPreAccept tuple or ignore markers, never both.        *)
(* This is a bounded logical-timer slice, not an arbitrary-network, message   *)
(* loss, or unbounded recovery model.                                        *)
(***************************************************************************)

VARIABLES scenario, stage, phase, currentEvidencePresent,
          staleEvidencePresent, otherTargetEvidencePresent,
          staleEvidenceCleaned, otherTargetEvidenceKept,
          normalRebroadcasts, ignoreRebroadcasts, acceptMessages,
          retryScheduled, durableRecordWritten, commandApplied

RemoteCount == 2

Scenarios == {
    "plain_retry",
    "ignore_retry",
    "current_evidence_timeout",
    "stale_evidence_retry",
    "other_target_evidence_retry"
}
Stages == {"start", "done"}
Phases == {"try_pre_accept", "accept"}
MessageCounts == 0..RemoteCount

Vars == <<scenario, stage, phase, currentEvidencePresent,
          staleEvidencePresent, otherTargetEvidencePresent,
          staleEvidenceCleaned, otherTargetEvidenceKept,
          normalRebroadcasts, ignoreRebroadcasts, acceptMessages,
          retryScheduled, durableRecordWritten, commandApplied>>

TypeOK ==
    /\ scenario \in Scenarios
    /\ stage \in Stages
    /\ phase \in Phases
    /\ currentEvidencePresent \in BOOLEAN
    /\ staleEvidencePresent \in BOOLEAN
    /\ otherTargetEvidencePresent \in BOOLEAN
    /\ staleEvidenceCleaned \in BOOLEAN
    /\ otherTargetEvidenceKept \in BOOLEAN
    /\ normalRebroadcasts \in MessageCounts
    /\ ignoreRebroadcasts \in MessageCounts
    /\ acceptMessages \in MessageCounts
    /\ retryScheduled \in BOOLEAN
    /\ durableRecordWritten \in BOOLEAN
    /\ commandApplied \in BOOLEAN

Init ==
    /\ scenario \in Scenarios
    /\ stage = "start"
    /\ phase = "try_pre_accept"
    /\ currentEvidencePresent = (scenario = "current_evidence_timeout")
    /\ staleEvidencePresent = (scenario = "stale_evidence_retry")
    /\ otherTargetEvidencePresent = (scenario = "other_target_evidence_retry")
    /\ staleEvidenceCleaned = FALSE
    /\ otherTargetEvidenceKept = FALSE
    /\ normalRebroadcasts = 0
    /\ ignoreRebroadcasts = 0
    /\ acceptMessages = 0
    /\ retryScheduled = FALSE
    /\ durableRecordWritten = FALSE
    /\ commandApplied = FALSE

PlainRetry ==
    /\ scenario = "plain_retry"
    /\ stage = "start"
    /\ ~currentEvidencePresent
    /\ ~staleEvidencePresent
    /\ ~otherTargetEvidencePresent
    /\ stage' = "done"
    /\ normalRebroadcasts' = RemoteCount
    /\ retryScheduled' = TRUE
    /\ UNCHANGED <<scenario, phase, currentEvidencePresent, staleEvidencePresent,
                  otherTargetEvidencePresent, staleEvidenceCleaned,
                  otherTargetEvidenceKept, ignoreRebroadcasts, acceptMessages,
                  durableRecordWritten, commandApplied>>

IgnoreRetry ==
    /\ scenario = "ignore_retry"
    /\ stage = "start"
    /\ ~currentEvidencePresent
    /\ ~staleEvidencePresent
    /\ ~otherTargetEvidencePresent
    /\ stage' = "done"
    /\ ignoreRebroadcasts' = RemoteCount
    /\ retryScheduled' = TRUE
    /\ UNCHANGED <<scenario, phase, currentEvidencePresent, staleEvidencePresent,
                  otherTargetEvidencePresent, staleEvidenceCleaned,
                  otherTargetEvidenceKept, normalRebroadcasts, acceptMessages,
                  durableRecordWritten, commandApplied>>

CurrentEvidenceTimeoutFailsClosed ==
    /\ scenario = "current_evidence_timeout"
    /\ stage = "start"
    /\ currentEvidencePresent
    /\ stage' = "done"
    /\ phase' = "accept"
    /\ currentEvidencePresent' = FALSE
    /\ acceptMessages' = RemoteCount
    /\ durableRecordWritten' = TRUE
    /\ retryScheduled' = FALSE
    /\ UNCHANGED <<scenario, staleEvidencePresent, otherTargetEvidencePresent,
                  staleEvidenceCleaned, otherTargetEvidenceKept,
                  normalRebroadcasts, ignoreRebroadcasts, commandApplied>>

StaleEvidenceRetry ==
    /\ scenario = "stale_evidence_retry"
    /\ stage = "start"
    /\ staleEvidencePresent
    /\ ~currentEvidencePresent
    /\ stage' = "done"
    /\ staleEvidencePresent' = FALSE
    /\ staleEvidenceCleaned' = TRUE
    /\ normalRebroadcasts' = RemoteCount
    /\ retryScheduled' = TRUE
    /\ UNCHANGED <<scenario, phase, currentEvidencePresent,
                  otherTargetEvidencePresent, otherTargetEvidenceKept,
                  ignoreRebroadcasts, acceptMessages, durableRecordWritten,
                  commandApplied>>

OtherTargetEvidenceRetry ==
    /\ scenario = "other_target_evidence_retry"
    /\ stage = "start"
    /\ otherTargetEvidencePresent
    /\ ~currentEvidencePresent
    /\ stage' = "done"
    /\ otherTargetEvidenceKept' = TRUE
    /\ normalRebroadcasts' = RemoteCount
    /\ retryScheduled' = TRUE
    /\ UNCHANGED <<scenario, phase, currentEvidencePresent, staleEvidencePresent,
                  staleEvidenceCleaned, otherTargetEvidencePresent,
                  ignoreRebroadcasts, acceptMessages, durableRecordWritten,
                  commandApplied>>

Next ==
    \/ PlainRetry
    \/ IgnoreRetry
    \/ CurrentEvidenceTimeoutFailsClosed
    \/ StaleEvidenceRetry
    \/ OtherTargetEvidenceRetry

CurrentEvidenceTimeoutPreemptsRetry ==
    scenario = "current_evidence_timeout" /\ stage = "done" =>
        /\ phase = "accept"
        /\ ~currentEvidencePresent
        /\ acceptMessages = RemoteCount
        /\ durableRecordWritten
        /\ ~retryScheduled
        /\ normalRebroadcasts = 0
        /\ ignoreRebroadcasts = 0
        /\ ~commandApplied

StaleEvidenceCannotFailCurrentRetry ==
    scenario = "stale_evidence_retry" /\ stage = "done" =>
        /\ phase = "try_pre_accept"
        /\ staleEvidenceCleaned
        /\ ~staleEvidencePresent
        /\ retryScheduled
        /\ normalRebroadcasts = RemoteCount
        /\ acceptMessages = 0
        /\ ~durableRecordWritten
        /\ ~commandApplied

OtherTargetEvidenceDoesNotBlockRetry ==
    scenario = "other_target_evidence_retry" /\ stage = "done" =>
        /\ phase = "try_pre_accept"
        /\ otherTargetEvidencePresent
        /\ otherTargetEvidenceKept
        /\ retryScheduled
        /\ normalRebroadcasts = RemoteCount
        /\ acceptMessages = 0
        /\ ~durableRecordWritten
        /\ ~commandApplied

IgnoredRetryCarriesOnlyIgnoreMarkers ==
    scenario = "ignore_retry" /\ stage = "done" =>
        /\ phase = "try_pre_accept"
        /\ retryScheduled
        /\ ignoreRebroadcasts = RemoteCount
        /\ normalRebroadcasts = 0
        /\ acceptMessages = 0
        /\ ~durableRecordWritten
        /\ ~commandApplied

PlainRetryCarriesOnlyNormalTuple ==
    scenario = "plain_retry" /\ stage = "done" =>
        /\ phase = "try_pre_accept"
        /\ retryScheduled
        /\ normalRebroadcasts = RemoteCount
        /\ ignoreRebroadcasts = 0
        /\ acceptMessages = 0
        /\ ~durableRecordWritten
        /\ ~commandApplied

RetryRebroadcastDoesNotMutateDurableOrApplicationState ==
    retryScheduled =>
        /\ phase = "try_pre_accept"
        /\ acceptMessages = 0
        /\ ~durableRecordWritten
        /\ ~commandApplied

TimerHasSingleOutcome ==
    stage = "done" =>
        \/ /\ retryScheduled
           /\ phase = "try_pre_accept"
           /\ acceptMessages = 0
        \/ /\ ~retryScheduled
           /\ phase = "accept"
           /\ acceptMessages = RemoteCount

Safety ==
    /\ CurrentEvidenceTimeoutPreemptsRetry
    /\ StaleEvidenceCannotFailCurrentRetry
    /\ OtherTargetEvidenceDoesNotBlockRetry
    /\ IgnoredRetryCarriesOnlyIgnoreMarkers
    /\ PlainRetryCarriesOnlyNormalTuple
    /\ RetryRebroadcastDoesNotMutateDurableOrApplicationState
    /\ TimerHasSingleOutcome

RetryScenarioCovered ==
    CASE scenario = "plain_retry" ->
            stage = "done" /\ retryScheduled /\ normalRebroadcasts = RemoteCount
      [] scenario = "ignore_retry" ->
            stage = "done" /\ retryScheduled /\ ignoreRebroadcasts = RemoteCount
      [] scenario = "current_evidence_timeout" ->
            stage = "done" /\ phase = "accept" /\ acceptMessages = RemoteCount /\ durableRecordWritten
      [] scenario = "stale_evidence_retry" ->
            stage = "done" /\ staleEvidenceCleaned /\ retryScheduled /\ normalRebroadcasts = RemoteCount
      [] scenario = "other_target_evidence_retry" ->
            stage = "done" /\ otherTargetEvidenceKept /\ retryScheduled /\ normalRebroadcasts = RemoteCount

EventuallyCoversTryPreAcceptRetry == <>RetryScenarioCovered

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
