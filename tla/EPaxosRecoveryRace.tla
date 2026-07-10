---- MODULE EPaxosRecoveryRace ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* A finite slice of recovery for one instance owned by a crash-stopped    *)
(* replica.  Three acceptors, two non-owner coordinators, ballots 0..2,    *)
(* and the a/b/noop full tuples are deliberately fixed by the configs.     *)
(* Requests and replies remain in an unordered finite network until an     *)
(* action delivers them, so Prepare and Accept can overlap and replies can *)
(* be stale, duplicated, or reordered.  This is bounded TLC evidence, not  *)
(* an unbounded proof of EPaxos or Paxos.                                  *)
(***************************************************************************)

CONSTANTS Replicas, Owner, Coordinators, Target, A, B, Noop, Ballots

ASSUME
    /\ Replicas \subseteq Nat
    /\ Cardinality(Replicas) = 3
    /\ Owner \in Replicas
    /\ Coordinators = Replicas \ {Owner}
    /\ Cardinality(Coordinators) = 2
    /\ A # B
    /\ A # Noop
    /\ B # Noop
    /\ Ballots = 0..2

MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y

LowCoordinator == MinOf(Coordinators)
HighCoordinator == MaxOf(Coordinators)
InitialBallot == 0
LowBallot == 1
HighBallot == 2
QuorumSize == 2

AValue == [target |-> Target, cmd |-> A, seq |-> 1, deps |-> {}]
BValue == [target |-> Target, cmd |-> B, seq |-> 1, deps |-> {}]
NoopValue == [target |-> Target, cmd |-> Noop, seq |-> 0, deps |-> {}]
Values == {AValue, BValue, NoopValue}
UserValues == {AValue, BValue}

\* Same record shape as a value, but outside Values.
NoValue == [target |-> Target, cmd |-> Noop, seq |-> 2, deps |-> {Target}]
ValueOrNone == Values \cup {NoValue}

EmptyAccepted == [ballot |-> InitialBallot, value |-> NoValue]
AcceptedRecords ==
    {EmptyAccepted} \cup
    {[ballot |-> b, value |-> v] : b \in {LowBallot, HighBallot},
                                       v \in Values}

PromiseFact(r, b) == [replica |-> r, ballot |-> b]
AcceptFact(r, b, v) == [replica |-> r, ballot |-> b, value |-> v]

MessageKinds == {"PrepareReq", "PrepareResp", "AcceptReq", "AcceptResp"}
ResponseKinds == {"PrepareResp", "AcceptResp"}
RequestKinds == {"PrepareReq", "AcceptReq"}
Copies == 0..1

Msg(kind, from, to, ballot, value, acceptedBallot, acceptedValue, copy) ==
    [kind |-> kind, from |-> from, to |-> to, target |-> Target,
     ballot |-> ballot, value |-> value,
     acceptedBallot |-> acceptedBallot, acceptedValue |-> acceptedValue,
     copy |-> copy]

NoMessage ==
    Msg("none", Owner, Owner, InitialBallot, NoValue,
        InitialBallot, NoValue, 0)

PrepareReq(k, r, b) ==
    Msg("PrepareReq", k, r, b, NoValue, InitialBallot, NoValue, 0)
PrepareResp(r, k, b, acceptedRecord) ==
    Msg("PrepareResp", r, k, b, NoValue, acceptedRecord.ballot,
        acceptedRecord.value, 0)
AcceptReq(k, r, b, v) ==
    Msg("AcceptReq", k, r, b, v, InitialBallot, NoValue, 0)
AcceptResp(r, k, b, v) ==
    Msg("AcceptResp", r, k, b, v, b, v, 0)
WithCopy(m, copy) == [m EXCEPT !.copy = copy]

IsMessage(m) ==
    /\ m.kind \in MessageKinds
    /\ m.from \in Replicas
    /\ m.to \in Replicas
    /\ m.target = Target
    /\ m.ballot \in Ballots
    /\ m.value \in ValueOrNone
    /\ m.acceptedBallot \in Ballots
    /\ m.acceptedValue \in ValueOrNone
    /\ m.copy \in Copies
    /\ (m.kind \in RequestKinds => m.copy = 0)
    /\ (m.kind = "PrepareReq" =>
            /\ m.value = NoValue
            /\ m.acceptedBallot = InitialBallot
            /\ m.acceptedValue = NoValue)
    /\ (m.kind = "AcceptReq" =>
            /\ m.value \in Values
            /\ m.acceptedBallot = InitialBallot
            /\ m.acceptedValue = NoValue)
    /\ (m.kind = "PrepareResp" =>
            /\ m.value = NoValue
            /\ ((m.acceptedValue = NoValue /\
                 m.acceptedBallot = InitialBallot) \/
                (m.acceptedValue \in Values /\
                 m.acceptedBallot \in {LowBallot, HighBallot})))
    /\ (m.kind = "AcceptResp" =>
            /\ m.value \in Values
            /\ m.acceptedValue = m.value
            /\ m.acceptedBallot = m.ballot)

IsResponse(m) == IsMessage(m) /\ m.kind \in ResponseKinds

Phases == {"idle", "prepare", "prepared", "accept", "committed"}
CoverageTags ==
    {"overlap", "stale-reply", "duplicate-generated",
     "duplicate-ignored", "reordered-reply", "lower-request-rejected",
     "choice", "complete"}

VARIABLES
    live,
    promise,
    accepted,
    promiseHistory,
    acceptedHistory,
    higherSeen,
    lowAtHigher,
    network,
    phase,
    coordBallot,
    candidate,
    prepareReplies,
    acceptReplies,
    preempted,
    commitValue,
    commitHistory,
    coverage

Vars ==
    <<live, promise, accepted, promiseHistory, acceptedHistory, higherSeen,
      lowAtHigher, network, phase, coordBallot, candidate, prepareReplies,
      acceptReplies, preempted, commitValue, commitHistory, coverage>>

EmptyReplyMap ==
    [k \in Coordinators |->
        [b \in Ballots |-> [r \in Replicas |-> NoMessage]]]

ResponseUniverse ==
    {WithCopy(PrepareResp(r, k, b, a), c) :
        r \in Replicas, k \in Coordinators, b \in Ballots,
        a \in AcceptedRecords, c \in Copies}
    \cup
    {WithCopy(AcceptResp(r, k, b, v), c) :
        r \in Replicas, k \in Coordinators,
        b \in {LowBallot, HighBallot}, v \in Values, c \in Copies}

ReplyMapOK(replies) ==
    /\ replies \in
         [Coordinators -> [Ballots -> [Replicas ->
             {NoMessage} \cup ResponseUniverse]]]
    /\ \A k \in Coordinators, b \in Ballots, r \in Replicas :
          LET m == replies[k][b][r]
          IN m # NoMessage =>
              /\ m.from = r
              /\ m.to = k
              /\ m.ballot = b
              /\ m.target = Target

AcceptedAt(r, b, v) == AcceptFact(r, b, v) \in acceptedHistory
LowAcceptedValues(r) == {v \in Values : AcceptedAt(r, LowBallot, v)}
AcceptedValuesAt(r, b) == {v \in Values : AcceptedAt(r, b, v)}

ReplySenders(replies, k, b) ==
    {r \in Replicas : replies[k][b][r] # NoMessage}
MatchingAcceptSenders(k, b, v) ==
    {r \in Replicas :
        /\ acceptReplies[k][b][r] # NoMessage
        /\ acceptReplies[k][b][r].value = v}

CountedPrepareSenders(k) ==
    IF phase[k] = "prepare" /\ k \notin preempted
    THEN ReplySenders(prepareReplies, k, coordBallot[k])
    ELSE {}
CountedAcceptSenders(k) ==
    IF phase[k] = "accept" /\ k \notin preempted
    THEN MatchingAcceptSenders(k, coordBallot[k], candidate[k])
    ELSE {}

SlowChosen(v) ==
    \E b \in {LowBallot, HighBallot}, q \in SUBSET Replicas :
        /\ Cardinality(q) >= QuorumSize
        /\ \A r \in q : AcceptedAt(r, b, v)
ChosenValues == {v \in Values : SlowChosen(v)}

PrepareEvidence(k) ==
    {prepareReplies[k][coordBallot[k]][r] :
        r \in CountedPrepareSenders(k)}
AcceptedPrepareEvidence(k) ==
    {m \in PrepareEvidence(k) : m.acceptedValue # NoValue}
HighestEvidenceBallot(k) ==
    MaxOf({m.acceptedBallot : m \in AcceptedPrepareEvidence(k)})
HighestEvidenceValues(k) ==
    {m.acceptedValue :
        m \in {e \in AcceptedPrepareEvidence(k) :
                  e.acceptedBallot = HighestEvidenceBallot(k)}}
RecoveryValue(k) ==
    IF AcceptedPrepareEvidence(k) # {}
    THEN CHOOSE v \in HighestEvidenceValues(k) : TRUE
    ELSE IF candidate[k] \in Values THEN candidate[k] ELSE NoopValue

CurrentPrepareResponse(m) ==
    /\ m.kind = "PrepareResp"
    /\ m.to \in Coordinators
    /\ m.to \notin preempted
    /\ phase[m.to] = "prepare"
    /\ m.ballot = coordBallot[m.to]
CurrentAcceptResponse(m) ==
    /\ m.kind = "AcceptResp"
    /\ m.to \in Coordinators
    /\ m.to \notin preempted
    /\ phase[m.to] = "accept"
    /\ m.ballot = coordBallot[m.to]
    /\ m.value = candidate[m.to]
CurrentResponse(m) == CurrentPrepareResponse(m) \/ CurrentAcceptResponse(m)

TypeOK ==
    /\ live \subseteq Replicas
    /\ promise \in [Replicas -> Ballots]
    /\ accepted \in [Replicas -> AcceptedRecords]
    /\ promiseHistory \subseteq
          {PromiseFact(r, b) : r \in Replicas, b \in Ballots}
    /\ acceptedHistory \subseteq
          {AcceptFact(r, b, v) : r \in Replicas,
                                  b \in {LowBallot, HighBallot},
                                  v \in Values}
    /\ higherSeen \subseteq Replicas
    /\ lowAtHigher \in [Replicas -> SUBSET Values]
    /\ \A m \in network : IsMessage(m)
    /\ phase \in [Coordinators -> Phases]
    /\ coordBallot \in [Coordinators -> Ballots]
    /\ candidate \in [Coordinators -> ValueOrNone]
    /\ ReplyMapOK(prepareReplies)
    /\ ReplyMapOK(acceptReplies)
    /\ preempted \subseteq Coordinators
    /\ commitValue \in ValueOrNone
    /\ commitHistory \subseteq Values
    /\ coverage \in [CoverageTags -> 0..1]

Init ==
    /\ live = Replicas
    /\ promise = [r \in Replicas |-> InitialBallot]
    /\ accepted = [r \in Replicas |-> EmptyAccepted]
    /\ promiseHistory =
          {PromiseFact(r, InitialBallot) : r \in Replicas}
    /\ acceptedHistory = {}
    /\ higherSeen = {}
    /\ lowAtHigher = [r \in Replicas |-> {}]
    /\ network = {}
    /\ phase = [k \in Coordinators |-> "idle"]
    /\ coordBallot = [k \in Coordinators |-> InitialBallot]
    /\ candidate = [k \in Coordinators |-> NoValue]
    /\ prepareReplies = EmptyReplyMap
    /\ acceptReplies = EmptyReplyMap
    /\ preempted = {}
    /\ commitValue = NoValue
    /\ commitHistory = {}
    /\ coverage = [tag \in CoverageTags |-> 0]

CrashStopOwner ==
    /\ Owner \in live
    /\ live' = live \ {Owner}
    /\ UNCHANGED <<promise, accepted, promiseHistory, acceptedHistory,
                   higherSeen, lowAtHigher, network, phase, coordBallot,
                   candidate, prepareReplies, acceptReplies, preempted,
                   commitValue, commitHistory, coverage>>

StartLowerRecoveryWith(v) ==
    LET requests ==
        {PrepareReq(LowCoordinator, r, LowBallot) : r \in Replicas}
    IN
    /\ Owner \notin live
    /\ phase[LowCoordinator] = "idle"
    /\ v \in UserValues
    /\ phase' = [phase EXCEPT ![LowCoordinator] = "prepare"]
    /\ coordBallot' =
          [coordBallot EXCEPT ![LowCoordinator] = LowBallot]
    /\ candidate' = [candidate EXCEPT ![LowCoordinator] = v]
    /\ network' = network \cup requests
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, prepareReplies,
                   acceptReplies, preempted, commitValue, commitHistory,
                   coverage>>

StartLowerRecovery ==
    \E v \in UserValues : StartLowerRecoveryWith(v)

LowAcceptanceObserved ==
    \E r \in Replicas, v \in UserValues : AcceptedAt(r, LowBallot, v)

StartTakeover ==
    LET requests ==
        {PrepareReq(HighCoordinator, r, HighBallot) : r \in Replicas}
    IN
    /\ phase[LowCoordinator] = "accept"
    /\ phase[HighCoordinator] = "idle"
    /\ LowAcceptanceObserved
    /\ commitValue = NoValue
    /\ phase' = [phase EXCEPT ![HighCoordinator] = "prepare"]
    /\ coordBallot' =
          [coordBallot EXCEPT ![HighCoordinator] = HighBallot]
    /\ candidate' = [candidate EXCEPT ![HighCoordinator] = NoValue]
    /\ preempted' = preempted \cup {LowCoordinator}
    /\ network' = network \cup requests
    /\ coverage' = [coverage EXCEPT !["overlap"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, prepareReplies,
                   acceptReplies, commitValue, commitHistory>>

ProcessPrepareRequest(m) ==
    LET r == m.to
        response == PrepareResp(r, m.from, m.ballot, accepted[r])
    IN
    /\ m \in network
    /\ m.kind = "PrepareReq"
    /\ r \in live
    /\ m.ballot >= promise[r]
    /\ network' = (network \ {m}) \cup {response}
    /\ promise' = [promise EXCEPT ![r] = m.ballot]
    /\ promiseHistory' =
          promiseHistory \cup {PromiseFact(r, m.ballot)}
    /\ IF m.ballot = HighBallot /\ r \notin higherSeen
       THEN /\ higherSeen' = higherSeen \cup {r}
            /\ lowAtHigher' =
                  [lowAtHigher EXCEPT ![r] = LowAcceptedValues(r)]
       ELSE /\ higherSeen' = higherSeen
            /\ lowAtHigher' = lowAtHigher
    /\ UNCHANGED <<live, accepted, acceptedHistory, phase, coordBallot,
                   candidate, prepareReplies, acceptReplies, preempted,
                   commitValue, commitHistory, coverage>>

ProcessOnePrepareRequest ==
    \E m \in network : ProcessPrepareRequest(m)

CanAcceptBeforeOverlap(m) ==
    m.ballot # LowBallot \/ coverage["overlap"] = 1 \/
        ~\E r \in Replicas, v \in Values : AcceptedAt(r, LowBallot, v)

ProcessAcceptRequest(m) ==
    LET r == m.to
        fact == AcceptFact(r, m.ballot, m.value)
        response == AcceptResp(r, m.from, m.ballot, m.value)
    IN
    /\ m \in network
    /\ m.kind = "AcceptReq"
    /\ r \in live
    /\ m.ballot >= promise[r]
    /\ CanAcceptBeforeOverlap(m)
    /\ AcceptedValuesAt(r, m.ballot) \subseteq {m.value}
    /\ network' = (network \ {m}) \cup {response}
    /\ promise' = [promise EXCEPT ![r] = m.ballot]
    /\ accepted' =
          [accepted EXCEPT ![r] = [ballot |-> m.ballot,
                                    value |-> m.value]]
    /\ promiseHistory' =
          promiseHistory \cup {PromiseFact(r, m.ballot)}
    /\ acceptedHistory' = acceptedHistory \cup {fact}
    /\ IF m.ballot = HighBallot /\ r \notin higherSeen
       THEN /\ higherSeen' = higherSeen \cup {r}
            /\ lowAtHigher' =
                  [lowAtHigher EXCEPT ![r] = LowAcceptedValues(r)]
       ELSE /\ higherSeen' = higherSeen
            /\ lowAtHigher' = lowAtHigher
    /\ UNCHANGED <<live, phase, coordBallot, candidate, prepareReplies,
                   acceptReplies, preempted, commitValue, commitHistory,
                   coverage>>

ProcessOneAcceptRequest ==
    \E m \in network : ProcessAcceptRequest(m)

RejectStaleLowerRequest(m) ==
    /\ m \in network
    /\ m.kind \in RequestKinds
    /\ m.to \in live
    /\ m.ballot < promise[m.to]
    /\ m.ballot = LowBallot
    /\ network' = network \ {m}
    /\ coverage' =
          [coverage EXCEPT !["lower-request-rejected"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, prepareReplies, acceptReplies,
                   preempted, commitValue, commitHistory>>

RejectOneStaleLowerRequest ==
    \E m \in network : RejectStaleLowerRequest(m)

DropCrashedMessage(m) ==
    /\ m \in network
    /\ m.to \notin live
    /\ network' = network \ {m}
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, prepareReplies, acceptReplies,
                   preempted, commitValue, commitHistory, coverage>>

DropOneCrashedMessage == \E m \in network : DropCrashedMessage(m)

DuplicateHighPrepareResponse(m) ==
    /\ m \in network
    /\ m.kind = "PrepareResp"
    /\ m.to = HighCoordinator
    /\ m.ballot = HighBallot
    /\ m.copy = 0
    /\ coverage["duplicate-generated"] = 0
    /\ WithCopy(m, 1) \notin network
    /\ network' = network \cup {WithCopy(m, 1)}
    /\ coverage' =
          [coverage EXCEPT !["duplicate-generated"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, prepareReplies, acceptReplies,
                   preempted, commitValue, commitHistory>>

DuplicateOneHighPrepareResponse ==
    \E m \in network : DuplicateHighPrepareResponse(m)

RecordFreshPrepareResponse(m) ==
    /\ m \in network
    /\ CurrentPrepareResponse(m)
    /\ prepareReplies[m.to][m.ballot][m.from] = NoMessage
    /\ ~(m.copy = 1 /\ WithCopy(m, 0) \in network)
    /\ prepareReplies' =
          [prepareReplies EXCEPT ![m.to][m.ballot][m.from] = m]
    /\ network' = network \ {m}
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, acceptReplies, preempted,
                   commitValue, commitHistory, coverage>>

RecordFreshAcceptResponse(m) ==
    /\ m \in network
    /\ CurrentAcceptResponse(m)
    /\ acceptReplies[m.to][m.ballot][m.from] = NoMessage
    /\ acceptReplies' =
          [acceptReplies EXCEPT ![m.to][m.ballot][m.from] = m]
    /\ network' = network \ {m}
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, prepareReplies, preempted,
                   commitValue, commitHistory, coverage>>

RecordFreshResponse ==
    (\E m \in network : RecordFreshPrepareResponse(m)) \/
    (\E m \in network : RecordFreshAcceptResponse(m))

RecordReorderedResponse(m) ==
    /\ m \in network
    /\ CurrentPrepareResponse(m)
    /\ m.copy = 1
    /\ WithCopy(m, 0) \in network
    /\ prepareReplies[m.to][m.ballot][m.from] = NoMessage
    /\ prepareReplies' =
          [prepareReplies EXCEPT ![m.to][m.ballot][m.from] = m]
    /\ network' = network \ {m}
    /\ coverage' = [coverage EXCEPT !["reordered-reply"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, acceptReplies, preempted,
                   commitValue, commitHistory>>

RecordOneReorderedResponse ==
    \E m \in network : RecordReorderedResponse(m)

IgnoreDuplicateResponse(m) ==
    /\ m \in network
    /\ m.kind \in ResponseKinds
    /\ m.to \in Coordinators
    /\ \/ /\ m.kind = "PrepareResp"
          /\ prepareReplies[m.to][m.ballot][m.from] # NoMessage
       \/ /\ m.kind = "AcceptResp"
          /\ acceptReplies[m.to][m.ballot][m.from] # NoMessage
    /\ network' = network \ {m}
    /\ coverage' = [coverage EXCEPT !["duplicate-ignored"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, prepareReplies, acceptReplies,
                   preempted, commitValue, commitHistory>>

IgnoreOneDuplicateResponse ==
    \E m \in network : IgnoreDuplicateResponse(m)

IgnoreStaleResponse(m) ==
    /\ m \in network
    /\ m.kind \in ResponseKinds
    /\ m.to \in Coordinators
    /\ ~CurrentResponse(m)
    /\ IF m.kind = "PrepareResp"
       THEN prepareReplies[m.to][m.ballot][m.from] = NoMessage
       ELSE acceptReplies[m.to][m.ballot][m.from] = NoMessage
    /\ network' = network \ {m}
    /\ coverage' = [coverage EXCEPT !["stale-reply"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, phase,
                   coordBallot, candidate, prepareReplies, acceptReplies,
                   preempted, commitValue, commitHistory>>

IgnoreOneStaleResponse ==
    \E m \in network : IgnoreStaleResponse(m)

FinishPrepare(k) ==
    LET recovered == RecoveryValue(k)
    IN
    /\ k \in Coordinators
    /\ phase[k] = "prepare"
    /\ k \notin preempted
    /\ Cardinality(CountedPrepareSenders(k)) >= QuorumSize
    /\ recovered \in Values
    /\ phase' = [phase EXCEPT ![k] = "prepared"]
    /\ candidate' = [candidate EXCEPT ![k] = recovered]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, network,
                   coordBallot, prepareReplies, acceptReplies, preempted,
                   commitValue, commitHistory, coverage>>

FinishOnePrepare == \E k \in Coordinators : FinishPrepare(k)

SendAccept(k) ==
    LET requests ==
        {AcceptReq(k, r, coordBallot[k], candidate[k]) : r \in Replicas}
    IN
    /\ k \in Coordinators
    /\ phase[k] = "prepared"
    /\ k \notin preempted
    /\ candidate[k] \in Values
    /\ phase' = [phase EXCEPT ![k] = "accept"]
    /\ network' = network \cup requests
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, coordBallot,
                   candidate, prepareReplies, acceptReplies, preempted,
                   commitValue, commitHistory, coverage>>

SendOneAccept == \E k \in Coordinators : SendAccept(k)

FinishChoice(k) ==
    LET v == candidate[k]
    IN
    /\ k \in Coordinators
    /\ phase[k] = "accept"
    /\ k \notin preempted
    /\ coverage["overlap"] = 1
    /\ Cardinality(CountedAcceptSenders(k)) >= QuorumSize
    /\ SlowChosen(v)
    /\ commitValue \in {NoValue, v}
    /\ phase' = [phase EXCEPT ![k] = "committed"]
    /\ commitValue' = v
    /\ commitHistory' = commitHistory \cup {v}
    /\ coverage' = [coverage EXCEPT !["choice"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, network,
                   coordBallot, candidate, prepareReplies, acceptReplies,
                   preempted>>

FinishOneChoice == \E k \in Coordinators : FinishChoice(k)

CoveragePrerequisites ==
    \A tag \in CoverageTags \ {"complete"} : coverage[tag] = 1

CertifyCoverage ==
    /\ CoveragePrerequisites
    /\ commitValue # NoValue
    /\ coverage["complete"] = 0
    /\ coverage' = [coverage EXCEPT !["complete"] = 1]
    /\ UNCHANGED <<live, promise, accepted, promiseHistory,
                   acceptedHistory, higherSeen, lowAtHigher, network, phase,
                   coordBallot, candidate, prepareReplies, acceptReplies,
                   preempted, commitValue, commitHistory>>

Next ==
    \/ CrashStopOwner
    \/ StartLowerRecovery
    \/ StartTakeover
    \/ ProcessOnePrepareRequest
    \/ ProcessOneAcceptRequest
    \/ RejectOneStaleLowerRequest
    \/ DropOneCrashedMessage
    \/ DuplicateOneHighPrepareResponse
    \/ RecordFreshResponse
    \/ RecordOneReorderedResponse
    \/ IgnoreOneDuplicateResponse
    \/ IgnoreOneStaleResponse
    \/ FinishOnePrepare
    \/ SendOneAccept
    \/ FinishOneChoice
    \/ CertifyCoverage

(***************************************************************************)
(* Safety invariants.                                                       *)
(***************************************************************************)

AtMostOneChosenFullTuple == Cardinality(ChosenValues) <= 1

PromisesMonotonic ==
    \A h \in promiseHistory : h.ballot <= promise[h.replica]

OneAcceptedValuePerAcceptorBallot ==
    \A r \in Replicas, b \in {LowBallot, HighBallot} :
        Cardinality(AcceptedValuesAt(r, b)) <= 1

AcceptedTupleMatchesLatestState ==
    \A r \in Replicas :
        LET facts == {h \in acceptedHistory : h.replica = r}
        IN IF facts = {}
           THEN accepted[r] = EmptyAccepted
           ELSE /\ accepted[r].value \in Values
                /\ AcceptFact(r, accepted[r].ballot,
                              accepted[r].value) \in facts
                /\ \A h \in facts : h.ballot <= accepted[r].ballot

OnlyCurrentBallotUniqueSendersCount ==
    /\ \A k \in Coordinators :
          /\ CountedPrepareSenders(k) \subseteq Replicas
          /\ CountedAcceptSenders(k) \subseteq Replicas
    /\ \A k \in Coordinators, b \in Ballots, r \in Replicas :
          /\ LET m == prepareReplies[k][b][r]
             IN m # NoMessage =>
                 /\ m.kind = "PrepareResp"
                 /\ m.from = r
                 /\ m.to = k
                 /\ m.ballot = b
          /\ LET m == acceptReplies[k][b][r]
             IN m # NoMessage =>
                 /\ m.kind = "AcceptResp"
                 /\ m.from = r
                 /\ m.to = k
                 /\ m.ballot = b

LowerCoordinatorCannotMutateHigherRound ==
    \A r \in higherSeen : LowAcceptedValues(r) = lowAtHigher[r]

AcceptedBallotRespectsPromise ==
    \A r \in Replicas : accepted[r].ballot <= promise[r]

PrepareHighestEvidenceIsUnique ==
    \A k \in Coordinators :
        AcceptedPrepareEvidence(k) # {} =>
            Cardinality(HighestEvidenceValues(k)) = 1

ChoiceOccursAfterOverlap ==
    ChosenValues # {} => coverage["overlap"] = 1

CrashStopOwnerNeverCoordinates ==
    /\ Owner \notin Coordinators
    /\ Owner \notin live =>
          /\ phase \in [Coordinators -> Phases]
          /\ preempted \subseteq Coordinators

CommitStability ==
    /\ (commitValue = NoValue <=> commitHistory = {})
    /\ (commitValue # NoValue =>
          /\ commitHistory = {commitValue}
          /\ commitValue \in ChosenValues)
    /\ \A k \in Coordinators :
          phase[k] = "committed" => candidate[k] = commitValue

Safety ==
    /\ AtMostOneChosenFullTuple
    /\ PromisesMonotonic
    /\ OneAcceptedValuePerAcceptorBallot
    /\ AcceptedTupleMatchesLatestState
    /\ OnlyCurrentBallotUniqueSendersCount
    /\ LowerCoordinatorCannotMutateHigherRound
    /\ AcceptedBallotRespectsPromise
    /\ PrepareHighestEvidenceIsUnique
    /\ ChoiceOccursAfterOverlap
    /\ CrashStopOwnerNeverCoordinates
    /\ CommitStability

OneLiveQuorum ==
    /\ live = Coordinators
    /\ Cardinality(live) >= QuorumSize

EventuallyOneLiveQuorumChooses ==
    (<>[]OneLiveQuorum) => <>(commitValue # NoValue)

Spec == Init /\ [][Next]_Vars

(***************************************************************************)
(* Weak fairness is stated only for the finite progress configuration.      *)
(* Aggregate delivery actions consume a finite network; together with one  *)
(* live quorum they express eventual delivery without requiring the        *)
(* crash-stopped owner.  Duplicate injection and coverage certification    *)
(* are deliberately not fairness assumptions.                              *)
(***************************************************************************)
FairSpec ==
    /\ Init
    /\ [][Next]_Vars
    /\ WF_Vars(CrashStopOwner)
    /\ WF_Vars(StartLowerRecovery)
    /\ WF_Vars(StartTakeover)
    /\ WF_Vars(ProcessOnePrepareRequest)
    /\ WF_Vars(ProcessOneAcceptRequest)
    /\ WF_Vars(RejectOneStaleLowerRequest)
    /\ WF_Vars(DropOneCrashedMessage)
    /\ WF_Vars(RecordFreshResponse)
    /\ WF_Vars(RecordOneReorderedResponse)
    /\ WF_Vars(IgnoreOneDuplicateResponse)
    /\ WF_Vars(IgnoreOneStaleResponse)
    /\ WF_Vars(FinishOnePrepare)
    /\ WF_Vars(SendOneAccept)
    /\ WF_Vars(FinishOneChoice)

====
