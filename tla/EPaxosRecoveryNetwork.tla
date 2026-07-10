---------------------- MODULE EPaxosRecoveryNetwork ----------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* This is a finite transport slice for one EPaxos recovery target. The    *)
(* wire is a bounded multiset of exact envelopes. The checked executions   *)
(* contain exactly one pre-retry drop, one retry epoch, one duplicate, and  *)
(* one out-of-order delivery. They also exercise stale evidence, malformed  *)
(* input, and a configuration-mismatched response. The configuration case  *)
(* has both an add branch and a remove branch; the latter retransmits an    *)
(* N=4 old-configuration target after voter 2 has been removed.             *)
(*                                                                         *)
(* The temporal claims below depend on the named bounded-fair-loss,         *)
(* eventual-delivery, fair-scheduling, and live-quorum assumptions in      *)
(* FairSpec. This model makes no claim about arbitrary loss, arbitrary      *)
(* retries, arbitrary reconfiguration histories, or unbounded executions.  *)
(***************************************************************************)

CONSTANTS Scenario, HasCommittedConflict, MaxDrops, MaxDuplicates,
          MaxRetryEpoch, MaxCopies, LiveVoters

VARIABLES transition, stage, wire, retryEpoch,
          activeRef, activeBallot, activeTuple, activeConf, activeEvidence,
          initialProjection, lastRetryProjection,
          voteMap, evidenceMap,
          chosen, committed, commitTuple, durableWrites, applied, coverage

Conf1 == 1
Conf2 == 2
Conf3 == 3
Confs == {Conf1, Conf2, Conf3}

Coordinator == 1
AddedVoter == 4
RemovedVoter == 2
Replicas == {1, 2, 3, 4}

Voters1 == {1, 2, 3}
Voters2 == {1, 2, 3, 4}
Voters3 == {1, 3, 4}

VotersFor(c) ==
    CASE c = Conf1 -> Voters1
      [] c = Conf2 -> Voters2
      [] c = Conf3 -> Voters3

SlowQuorum(c) == (Cardinality(VotersFor(c)) \div 2) + 1

Scenarios == {"loss_retry", "evidence_retry", "config_retry"}
Transitions == IF Scenario = "config_retry" THEN {"add", "remove"}
               ELSE {"steady"}

TargetConf == CASE transition = "steady" -> Conf1
                   [] transition = "add" -> Conf1
                   [] transition = "remove" -> Conf2

CurrentConf == CASE transition = "steady" -> Conf1
                    [] transition = "add" -> Conf2
                    [] transition = "remove" -> Conf3

PinnedVoters == VotersFor(TargetConf)
PinnedRemotes == PinnedVoters \ {Coordinator}
PrimaryResponder == RemovedVoter
SecondaryResponder == 3

TargetRef == [replica |-> Coordinator, instance |-> 1]
ConflictRef == [replica |-> 3, instance |-> 7]
BadRef == [replica |-> 4, instance |-> 9]
RefUniverse == {TargetRef, ConflictRef, BadRef}

OldBallot == 0
CurrentBallot == 1
Ballots == {OldBallot, CurrentBallot}

NoTuple == [cmd |-> "none", seq |-> 0, deps |-> {}, conf |-> Conf1]
TargetTuple ==
    [cmd |-> "target-command",
     seq |-> 2,
     deps |-> IF HasCommittedConflict THEN {ConflictRef} ELSE {},
     conf |-> TargetConf]
ConflictTuple ==
    [cmd |-> "committed-conflict", seq |-> 1, deps |-> {}, conf |-> TargetConf]
AlternateTuple ==
    [cmd |-> "alternate-command", seq |-> 3, deps |-> {}, conf |-> TargetConf]
TupleUniverse == {NoTuple, TargetTuple, ConflictTuple, AlternateTuple}

UnseenEvidence == [kind |-> "unseen", tuple |-> NoTuple]
NoEvidence == [kind |-> "none", tuple |-> NoTuple]
CommittedEvidence == [kind |-> "committed", tuple |-> ConflictTuple]
ExpectedEvidence == IF HasCommittedConflict THEN CommittedEvidence ELSE NoEvidence
EvidenceUniverse == {UnseenEvidence, NoEvidence, CommittedEvidence}

BaseProjection ==
    [ref |-> TargetRef,
     ballot |-> CurrentBallot,
     tuple |-> TargetTuple,
     conf |-> TargetConf,
     evidence |-> ExpectedEvidence]

Projection(e) ==
    [ref |-> e.ref,
     ballot |-> e.ballot,
     tuple |-> e.tuple,
     conf |-> e.conf,
     evidence |-> e.evidence]

Envelope(kind, sender, receiver, ref, ballot, tuple, conf, evidence,
         epoch, ordinal, wellFormed) ==
    [kind |-> kind,
     sender |-> sender,
     to |-> receiver,
     ref |-> ref,
     ballot |-> ballot,
     tuple |-> tuple,
     conf |-> conf,
     evidence |-> evidence,
     epoch |-> epoch,
     ordinal |-> ordinal,
     wellFormed |-> wellFormed]

RecoverRequest(receiver, epoch) ==
    Envelope("RecoverReq", Coordinator, receiver, TargetRef, CurrentBallot,
             TargetTuple, TargetConf, ExpectedEvidence, epoch,
             IF epoch = 0 THEN 0 ELSE 4, TRUE)

RecoverResponse(sender, epoch) ==
    Envelope("RecoverResp", sender, Coordinator, TargetRef, CurrentBallot,
             TargetTuple, TargetConf, ExpectedEvidence, epoch,
             IF epoch = 0 THEN 1 ELSE IF sender = PrimaryResponder THEN 5 ELSE 6,
             TRUE)

StaleEvidenceEnvelope ==
    Envelope("EvidenceResp", SecondaryResponder, Coordinator, TargetRef,
             OldBallot, TargetTuple, TargetConf, CommittedEvidence,
             MaxRetryEpoch, 1, TRUE)

MalformedEnvelope ==
    Envelope("RecoverResp", AddedVoter, Coordinator, BadRef, CurrentBallot,
             AlternateTuple, TargetConf, CommittedEvidence,
             MaxRetryEpoch, 2, FALSE)

MismatchedConf == IF CurrentConf # TargetConf THEN CurrentConf ELSE Conf2
ConfigMismatchEnvelope ==
    Envelope("RecoverResp", AddedVoter, Coordinator, TargetRef, CurrentBallot,
             TargetTuple, MismatchedConf, ExpectedEvidence,
             MaxRetryEpoch, 3, TRUE)

RequestEnvelopes ==
    {RecoverRequest(r, ep) : r \in PinnedRemotes, ep \in 0..MaxRetryEpoch}
ResponseEnvelopes ==
    {RecoverResponse(r, ep) : r \in PinnedRemotes, ep \in 0..MaxRetryEpoch}
AdverseEnvelopes ==
    {StaleEvidenceEnvelope, MalformedEnvelope, ConfigMismatchEnvelope}
EnvelopeUniverse == RequestEnvelopes \cup ResponseEnvelopes \cup AdverseEnvelopes
EnvelopeFields ==
    {"kind", "sender", "to", "ref", "ballot", "tuple", "conf",
     "evidence", "epoch", "ordinal", "wellFormed"}

EmptyWire == [e \in EnvelopeUniverse |-> 0]
AddMessages(w, es) ==
    [e \in EnvelopeUniverse |-> w[e] + IF e \in es THEN 1 ELSE 0]
RemoveMessages(w, es) ==
    [e \in EnvelopeUniverse |-> w[e] - IF e \in es THEN 1 ELSE 0]
RemoveOne(w, e) == [w EXCEPT ![e] = @ - 1]
AddOne(w, e) == [w EXCEPT ![e] = @ + 1]
WireEmpty == \A e \in EnvelopeUniverse : wire[e] = 0

InitialRequest == RecoverRequest(PrimaryResponder, 0)
InitialResponse == RecoverResponse(PrimaryResponder, 0)
RetryRequests ==
    {RecoverRequest(r, MaxRetryEpoch) : r \in PinnedRemotes}
RetryPrimaryResponse == RecoverResponse(PrimaryResponder, MaxRetryEpoch)
RetrySecondaryResponse == RecoverResponse(SecondaryResponder, MaxRetryEpoch)
RetryResponses ==
    {RetryPrimaryResponse, RetrySecondaryResponse,
     StaleEvidenceEnvelope, MalformedEnvelope, ConfigMismatchEnvelope}

AcceptableResponse(e) ==
    /\ e.kind = "RecoverResp"
    /\ e.wellFormed
    /\ e.to = Coordinator
    /\ e.ref = activeRef
    /\ e.ballot = activeBallot
    /\ e.tuple = activeTuple
    /\ e.conf = activeConf
    /\ e.evidence = activeEvidence
    /\ e.epoch = retryEpoch
    /\ e.sender \in PinnedVoters

Stages ==
    {"idle", "initial-request", "initial-response", "dropped",
     "retry-request", "responses", "duplicated", "reordered",
     "duplicate-ignored", "stale-ignored", "malformed-ignored",
     "config-mismatch-ignored", "quorum", "committed", "applied"}

CoverageNames ==
    {"drop", "retry", "duplicate", "reorder", "duplicate_sender",
     "stale_evidence", "malformed", "config_mismatch", "old_config",
     "n4_old_config", "added_voter_reject", "removed_voter_accept",
     "commit"}

EmptyVoteMap ==
    [r \in RefUniverse |->
        [b \in Ballots |-> [n \in Replicas |-> FALSE]]]
EmptyEvidenceMap ==
    [r \in RefUniverse |->
        [b \in Ballots |-> [n \in Replicas |-> UnseenEvidence]]]

CountedSenders ==
    {n \in Replicas : voteMap[TargetRef][CurrentBallot][n]}
RecordedEvidenceSenders ==
    {n \in Replicas :
        evidenceMap[TargetRef][CurrentBallot][n] # UnseenEvidence}

PrePrimaryStages ==
    {"idle", "initial-request", "initial-response", "dropped",
     "retry-request", "responses", "duplicated"}
PrimaryOnlyStages ==
    {"reordered", "duplicate-ignored", "stale-ignored",
     "malformed-ignored", "config-mismatch-ignored"}
QuorumStages == {"quorum", "committed", "applied"}

Vars ==
    <<transition, stage, wire, retryEpoch,
      activeRef, activeBallot, activeTuple, activeConf, activeEvidence,
      initialProjection, lastRetryProjection,
      voteMap, evidenceMap,
      chosen, committed, commitTuple, durableWrites, applied, coverage>>

TypeOK ==
    /\ Scenario \in Scenarios
    /\ HasCommittedConflict \in BOOLEAN
    /\ MaxDrops = 1
    /\ MaxDuplicates = 1
    /\ MaxRetryEpoch = 1
    /\ MaxCopies = 2
    /\ LiveVoters \subseteq Replicas
    /\ transition \in Transitions
    /\ stage \in Stages
    /\ wire \in [EnvelopeUniverse -> 0..MaxCopies]
    /\ retryEpoch \in 0..MaxRetryEpoch
    /\ activeRef \in RefUniverse
    /\ activeBallot \in Ballots
    /\ activeTuple \in TupleUniverse
    /\ activeConf \in Confs
    /\ activeEvidence \in EvidenceUniverse
    /\ initialProjection = BaseProjection
    /\ lastRetryProjection = BaseProjection
    /\ voteMap \in [RefUniverse -> [Ballots -> [Replicas -> BOOLEAN]]]
    /\ evidenceMap \in
        [RefUniverse -> [Ballots -> [Replicas -> EvidenceUniverse]]]
    /\ chosen \subseteq TupleUniverse \ {NoTuple}
    /\ committed \in BOOLEAN
    /\ commitTuple \in TupleUniverse
    /\ durableWrites \in 0..1
    /\ applied \in BOOLEAN
    /\ coverage \in [CoverageNames -> 0..1]

Init ==
    /\ transition \in Transitions
    /\ stage = "idle"
    /\ wire = EmptyWire
    /\ retryEpoch = 0
    /\ activeRef = TargetRef
    /\ activeBallot = CurrentBallot
    /\ activeTuple = TargetTuple
    /\ activeConf = TargetConf
    /\ activeEvidence = ExpectedEvidence
    /\ initialProjection = BaseProjection
    /\ lastRetryProjection = BaseProjection
    /\ voteMap =
        [EmptyVoteMap EXCEPT
            ![TargetRef][CurrentBallot][Coordinator] = TRUE]
    /\ evidenceMap =
        [EmptyEvidenceMap EXCEPT
            ![TargetRef][CurrentBallot][Coordinator] = ExpectedEvidence]
    /\ chosen = {}
    /\ committed = FALSE
    /\ commitTuple = NoTuple
    /\ durableWrites = 0
    /\ applied = FALSE
    /\ coverage = [name \in CoverageNames |-> 0]

StartRecovery ==
    /\ stage = "idle"
    /\ wire[InitialRequest] = 0
    /\ stage' = "initial-request"
    /\ wire' = AddOne(wire, InitialRequest)
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied, coverage>>

DeliverInitialRequest ==
    /\ stage = "initial-request"
    /\ wire[InitialRequest] = 1
    /\ wire[InitialResponse] = 0
    /\ stage' = "initial-response"
    /\ wire' = AddOne(RemoveOne(wire, InitialRequest), InitialResponse)
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied, coverage>>

DropInitialResponse ==
    /\ stage = "initial-response"
    /\ wire[InitialResponse] = 1
    /\ coverage["drop"] < MaxDrops
    /\ stage' = "dropped"
    /\ wire' = RemoveOne(wire, InitialResponse)
    /\ coverage' = [coverage EXCEPT !["drop"] = @ + 1]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied>>

RetryRecovery ==
    /\ stage = "dropped"
    /\ retryEpoch < MaxRetryEpoch
    /\ coverage["retry"] = 0
    /\ WireEmpty
    /\ stage' = "retry-request"
    /\ wire' = AddMessages(wire, RetryRequests)
    /\ retryEpoch' = retryEpoch + 1
    /\ lastRetryProjection' = Projection(RecoverRequest(PrimaryResponder,
                                                         retryEpoch + 1))
    /\ coverage' =
        [coverage EXCEPT
            !["retry"] = @ + 1,
            !["old_config"] = IF TargetConf # CurrentConf THEN @ + 1 ELSE @,
            !["n4_old_config"] = IF transition = "remove" THEN @ + 1 ELSE @]
    /\ UNCHANGED <<transition, activeRef, activeBallot, activeTuple,
                  activeConf, activeEvidence, initialProjection, voteMap,
                  evidenceMap, chosen, committed, commitTuple,
                  durableWrites, applied>>

FollowersRespondToRetry ==
    /\ stage = "retry-request"
    /\ \A e \in RetryRequests : wire[e] = 1
    /\ \A e \in RetryResponses : wire[e] = 0
    /\ stage' = "responses"
    /\ wire' = AddMessages(RemoveMessages(wire, RetryRequests), RetryResponses)
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied, coverage>>

DuplicatePrimaryResponse ==
    /\ stage = "responses"
    /\ wire[RetryPrimaryResponse] = 1
    /\ coverage["duplicate"] < MaxDuplicates
    /\ stage' = "duplicated"
    /\ wire' = AddOne(wire, RetryPrimaryResponse)
    /\ coverage' = [coverage EXCEPT !["duplicate"] = @ + 1]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied>>

DeliverReorderedPrimary ==
    /\ stage = "duplicated"
    /\ wire[RetryPrimaryResponse] = MaxCopies
    /\ wire[StaleEvidenceEnvelope] = 1
    /\ StaleEvidenceEnvelope.ordinal < RetryPrimaryResponse.ordinal
    /\ AcceptableResponse(RetryPrimaryResponse)
    /\ ~voteMap[TargetRef][CurrentBallot][PrimaryResponder]
    /\ stage' = "reordered"
    /\ wire' = RemoveOne(wire, RetryPrimaryResponse)
    /\ voteMap' =
        [voteMap EXCEPT
            ![TargetRef][CurrentBallot][PrimaryResponder] = TRUE]
    /\ evidenceMap' =
        [evidenceMap EXCEPT
            ![TargetRef][CurrentBallot][PrimaryResponder] =
                RetryPrimaryResponse.evidence]
    /\ coverage' =
        [coverage EXCEPT
            !["reorder"] = @ + 1,
            !["removed_voter_accept"] =
                IF transition = "remove" THEN @ + 1 ELSE @]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, chosen, committed, commitTuple,
                  durableWrites, applied>>

IgnoreDuplicateSender ==
    /\ stage = "reordered"
    /\ wire[RetryPrimaryResponse] = 1
    /\ voteMap[TargetRef][CurrentBallot][PrimaryResponder]
    /\ stage' = "duplicate-ignored"
    /\ wire' = RemoveOne(wire, RetryPrimaryResponse)
    /\ coverage' = [coverage EXCEPT !["duplicate_sender"] = @ + 1]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied>>

IgnoreStaleEvidence ==
    /\ stage = "duplicate-ignored"
    /\ wire[StaleEvidenceEnvelope] = 1
    /\ StaleEvidenceEnvelope.ballot # activeBallot
    /\ stage' = "stale-ignored"
    /\ wire' = RemoveOne(wire, StaleEvidenceEnvelope)
    /\ coverage' = [coverage EXCEPT !["stale_evidence"] = @ + 1]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied>>

IgnoreMalformedEnvelope ==
    /\ stage = "stale-ignored"
    /\ wire[MalformedEnvelope] = 1
    /\ ~MalformedEnvelope.wellFormed
    /\ stage' = "malformed-ignored"
    /\ wire' = RemoveOne(wire, MalformedEnvelope)
    /\ coverage' = [coverage EXCEPT !["malformed"] = @ + 1]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied>>

IgnoreConfigMismatch ==
    /\ stage = "malformed-ignored"
    /\ wire[ConfigMismatchEnvelope] = 1
    /\ ConfigMismatchEnvelope.conf # activeConf
    /\ stage' = "config-mismatch-ignored"
    /\ wire' = RemoveOne(wire, ConfigMismatchEnvelope)
    /\ coverage' =
        [coverage EXCEPT
            !["config_mismatch"] = @ + 1,
            !["added_voter_reject"] =
                IF transition = "add" THEN @ + 1 ELSE @]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, applied>>

DeliverSecondaryForQuorum ==
    /\ stage = "config-mismatch-ignored"
    /\ wire[RetrySecondaryResponse] = 1
    /\ AcceptableResponse(RetrySecondaryResponse)
    /\ ~voteMap[TargetRef][CurrentBallot][SecondaryResponder]
    /\ stage' = "quorum"
    /\ wire' = RemoveOne(wire, RetrySecondaryResponse)
    /\ voteMap' =
        [voteMap EXCEPT
            ![TargetRef][CurrentBallot][SecondaryResponder] = TRUE]
    /\ evidenceMap' =
        [evidenceMap EXCEPT
            ![TargetRef][CurrentBallot][SecondaryResponder] =
                RetrySecondaryResponse.evidence]
    /\ UNCHANGED <<transition, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, chosen, committed, commitTuple,
                  durableWrites, applied, coverage>>

ChooseAtPinnedQuorum ==
    /\ stage = "quorum"
    /\ Cardinality(CountedSenders \cap PinnedVoters) >= SlowQuorum(TargetConf)
    /\ WireEmpty
    /\ stage' = "committed"
    /\ chosen' = {TargetTuple}
    /\ committed' = TRUE
    /\ commitTuple' = TargetTuple
    /\ durableWrites' = durableWrites + 1
    /\ coverage' = [coverage EXCEPT !["commit"] = @ + 1]
    /\ UNCHANGED <<transition, wire, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, applied>>

ApplyCommitted ==
    /\ stage = "committed"
    /\ committed
    /\ commitTuple = TargetTuple
    /\ stage' = "applied"
    /\ applied' = TRUE
    /\ UNCHANGED <<transition, wire, retryEpoch, activeRef, activeBallot,
                  activeTuple, activeConf, activeEvidence, initialProjection,
                  lastRetryProjection, voteMap, evidenceMap, chosen,
                  committed, commitTuple, durableWrites, coverage>>

Next ==
    \/ StartRecovery
    \/ DeliverInitialRequest
    \/ DropInitialResponse
    \/ RetryRecovery
    \/ FollowersRespondToRetry
    \/ DuplicatePrimaryResponse
    \/ DeliverReorderedPrimary
    \/ IgnoreDuplicateSender
    \/ IgnoreStaleEvidence
    \/ IgnoreMalformedEnvelope
    \/ IgnoreConfigMismatch
    \/ DeliverSecondaryForQuorum
    \/ ChooseAtPinnedQuorum
    \/ ApplyCommitted

ExactTransportEnvelopes ==
    /\ DOMAIN wire = EnvelopeUniverse
    /\ \A e \in EnvelopeUniverse :
        /\ DOMAIN e = EnvelopeFields
        /\ e.kind \in {"RecoverReq", "RecoverResp", "EvidenceResp"}
        /\ e.sender \in Replicas
        /\ e.to \in Replicas
        /\ e.ref \in RefUniverse
        /\ e.ballot \in Ballots
        /\ e.tuple \in TupleUniverse
        /\ e.conf \in Confs
        /\ e.evidence \in EvidenceUniverse
        /\ e.epoch \in 0..MaxRetryEpoch
        /\ e.ordinal \in 0..6
        /\ e.wellFormed \in BOOLEAN

RetryPreservesProtocolValue ==
    /\ activeRef = TargetRef
    /\ activeBallot = CurrentBallot
    /\ activeTuple = TargetTuple
    /\ activeConf = TargetConf
    /\ activeEvidence = ExpectedEvidence
    /\ initialProjection = BaseProjection
    /\ lastRetryProjection = initialProjection
    /\ \A e \in EnvelopeUniverse :
        (e.kind = "RecoverReq" /\ e.epoch > 0) =>
            Projection(e) = initialProjection

OnlyCurrentRoundMapsPopulated ==
    \A r \in RefUniverse :
        \A b \in Ballots :
            (r # TargetRef \/ b # CurrentBallot) =>
                /\ \A n \in Replicas : ~voteMap[r][b][n]
                /\ \A n \in Replicas :
                    evidenceMap[r][b][n] = UnseenEvidence

CurrentRoundSenderShape ==
    /\ (stage \in PrePrimaryStages =>
            /\ CountedSenders = {Coordinator}
            /\ RecordedEvidenceSenders = {Coordinator})
    /\ (stage \in PrimaryOnlyStages =>
            /\ CountedSenders = {Coordinator, PrimaryResponder}
            /\ RecordedEvidenceSenders = {Coordinator, PrimaryResponder})
    /\ (stage \in QuorumStages =>
            /\ CountedSenders =
                {Coordinator, PrimaryResponder, SecondaryResponder}
            /\ RecordedEvidenceSenders =
                {Coordinator, PrimaryResponder, SecondaryResponder})

CanonicalCurrentRoundSenders ==
    /\ OnlyCurrentRoundMapsPopulated
    /\ CurrentRoundSenderShape
    /\ CountedSenders \subseteq PinnedVoters
    /\ Cardinality(CountedSenders) =
        Cardinality({n \in PinnedVoters :
            voteMap[TargetRef][CurrentBallot][n]})

EvidenceSenderAuthentic ==
    /\ RecordedEvidenceSenders \subseteq PinnedVoters
    /\ \A n \in RecordedEvidenceSenders :
        evidenceMap[TargetRef][CurrentBallot][n] = ExpectedEvidence

EvidenceSound ==
    \A n \in RecordedEvidenceSenders :
        evidenceMap[TargetRef][CurrentBallot][n].kind = "committed" =>
            /\ HasCommittedConflict
            /\ evidenceMap[TargetRef][CurrentBallot][n].tuple = ConflictTuple
            /\ ConflictRef \in TargetTuple.deps

StaleAndDuplicateSenderDoNotCount ==
    /\ (stage = "duplicate-ignored" =>
            CountedSenders = {Coordinator, PrimaryResponder})
    /\ (stage = "stale-ignored" =>
            /\ CountedSenders = {Coordinator, PrimaryResponder}
            /\ SecondaryResponder \notin RecordedEvidenceSenders)

MalformedAndConfigMismatchStutter ==
    /\ (stage = "malformed-ignored" =>
            /\ CountedSenders = {Coordinator, PrimaryResponder}
            /\ RecordedEvidenceSenders = {Coordinator, PrimaryResponder}
            /\ chosen = {}
            /\ ~committed
            /\ durableWrites = 0
            /\ ~applied)
    /\ (stage = "config-mismatch-ignored" =>
            /\ CountedSenders = {Coordinator, PrimaryResponder}
            /\ RecordedEvidenceSenders = {Coordinator, PrimaryResponder}
            /\ AddedVoter \notin CountedSenders
            /\ chosen = {}
            /\ ~committed
            /\ durableWrites = 0
            /\ ~applied)

PureRetryNoDurableOrApply ==
    stage = "retry-request" =>
        /\ chosen = {}
        /\ ~committed
        /\ commitTuple = NoTuple
        /\ durableWrites = 0
        /\ ~applied
        /\ CountedSenders = {Coordinator}

PinnedConfigOnly ==
    /\ CountedSenders \subseteq PinnedVoters
    /\ (transition = "add" /\ stage \in
            {"config-mismatch-ignored", "quorum", "committed", "applied"} =>
            /\ TargetConf = Conf1
            /\ CurrentConf = Conf2
            /\ AddedVoter \in VotersFor(CurrentConf)
            /\ AddedVoter \notin PinnedVoters
            /\ AddedVoter \notin CountedSenders)
    /\ (transition = "remove" /\ stage \in
            {"reordered", "duplicate-ignored", "stale-ignored",
             "malformed-ignored", "config-mismatch-ignored", "quorum",
             "committed", "applied"} =>
            /\ TargetConf = Conf2
            /\ CurrentConf = Conf3
            /\ Cardinality(PinnedVoters) = 4
            /\ RemovedVoter \in PinnedVoters
            /\ RemovedVoter \notin VotersFor(CurrentConf)
            /\ RemovedVoter \in CountedSenders)

AtMostOneChosenTuple == Cardinality(chosen) <= 1

DecisionUsesPinnedQuorum ==
    committed =>
        /\ Cardinality(CountedSenders \cap PinnedVoters) >=
            SlowQuorum(TargetConf)
        /\ chosen = {TargetTuple}
        /\ commitTuple = TargetTuple
        /\ durableWrites = 1

CommitStateSafety ==
    /\ (applied => committed)
    /\ (~committed =>
            /\ chosen = {}
            /\ commitTuple = NoTuple
            /\ durableWrites = 0
            /\ ~applied)
    /\ (stage = "applied" =>
            /\ WireEmpty
            /\ committed
            /\ applied)

Safety ==
    /\ ExactTransportEnvelopes
    /\ RetryPreservesProtocolValue
    /\ CanonicalCurrentRoundSenders
    /\ EvidenceSenderAuthentic
    /\ EvidenceSound
    /\ StaleAndDuplicateSenderDoNotCount
    /\ MalformedAndConfigMismatchStutter
    /\ PureRetryNoDurableOrApply
    /\ PinnedConfigOnly
    /\ AtMostOneChosenTuple
    /\ DecisionUsesPinnedQuorum
    /\ CommitStateSafety

CommonCoverageComplete ==
    /\ coverage["drop"] = MaxDrops
    /\ coverage["retry"] = MaxRetryEpoch
    /\ coverage["duplicate"] = MaxDuplicates
    /\ coverage["reorder"] = 1
    /\ coverage["duplicate_sender"] = 1
    /\ coverage["stale_evidence"] = 1
    /\ coverage["malformed"] = 1
    /\ coverage["config_mismatch"] = 1
    /\ coverage["commit"] = 1

ScenarioCoverageComplete ==
    /\ CommonCoverageComplete
    /\ (Scenario = "evidence_retry" =>
            /\ HasCommittedConflict
            /\ \A n \in {Coordinator, PrimaryResponder, SecondaryResponder} :
                evidenceMap[TargetRef][CurrentBallot][n] = CommittedEvidence)
    /\ (Scenario = "loss_retry" => ~HasCommittedConflict)
    /\ (Scenario = "config_retry" =>
            /\ coverage["old_config"] = 1
            /\ IF transition = "add" THEN
                  /\ coverage["added_voter_reject"] = 1
                  /\ AddedVoter \notin CountedSenders
               ELSE
                  /\ coverage["n4_old_config"] = 1
                  /\ coverage["removed_voter_accept"] = 1
                  /\ Cardinality(PinnedVoters) = 4
                  /\ RemovedVoter \in CountedSenders)

BoundedFairLossAssumption ==
    /\ MaxDrops = 1
    /\ WF_Vars(DropInitialResponse)

EventualDeliveryAssumption ==
    /\ WF_Vars(DeliverInitialRequest)
    /\ WF_Vars(FollowersRespondToRetry)
    /\ WF_Vars(DeliverReorderedPrimary)
    /\ WF_Vars(IgnoreDuplicateSender)
    /\ WF_Vars(IgnoreStaleEvidence)
    /\ WF_Vars(IgnoreMalformedEnvelope)
    /\ WF_Vars(IgnoreConfigMismatch)
    /\ WF_Vars(DeliverSecondaryForQuorum)

LiveQuorumAssumption ==
    /\ {Coordinator, PrimaryResponder, SecondaryResponder} \subseteq LiveVoters
    /\ Cardinality(LiveVoters \cap PinnedVoters) >= SlowQuorum(TargetConf)
    /\ WF_Vars(DeliverReorderedPrimary)
    /\ WF_Vars(DeliverSecondaryForQuorum)

FairSchedulingAssumption ==
    /\ WF_Vars(StartRecovery)
    /\ WF_Vars(RetryRecovery)
    /\ WF_Vars(DuplicatePrimaryResponse)
    /\ WF_Vars(ChooseAtPinnedQuorum)
    /\ WF_Vars(ApplyCommitted)

FairSpec ==
    /\ Init
    /\ [][Next]_Vars
    /\ BoundedFairLossAssumption
    /\ EventualDeliveryAssumption
    /\ LiveQuorumAssumption
    /\ FairSchedulingAssumption

EventuallyCoversScenario ==
    <> (stage = "applied" /\ ScenarioCoverageComplete /\ WireEmpty)

EventuallyChosenByPinnedLiveQuorum ==
    <> (/\ committed
        /\ chosen = {TargetTuple}
        /\ Cardinality(CountedSenders \cap LiveVoters \cap PinnedVoters) >=
            SlowQuorum(TargetConf))

CommitStable ==
    [] (committed =>
        [] (/\ committed
            /\ commitTuple = TargetTuple
            /\ chosen = {TargetTuple}))

=============================================================================
