--------------------- MODULE EPaxosInductiveProofs ---------------------
EXTENDS Naturals, FiniteSets, TLAPS

(***************************************************************************)
(* Parametric, arbitrary-length proof model.  Every carrier is an arbitrary *)
(* finite nonempty set; no carrier cardinality is fixed.  A behavior may    *)
(* contain arbitrarily many protocol and stuttering steps.  Values abstract *)
(* complete EPaxos tuples.  The abstraction is deliberately a conservative *)
(* proof-carrying recovery protocol: recovery evidence carries immutable    *)
(* sender acceptance histories, selects a highest accepted fact, and also   *)
(* requires all facts in the selected certificate to agree.  Acceptors may  *)
(* replace their latest value at higher ballots; safety is not obtained by   *)
(* freezing acceptedValue or by consulting chosenHistory as a value oracle. *)
(*                                                                         *)
(* The modeled concrete vocabulary below is not all Go execution.  It       *)
(* corresponds only to the semantic actions named by ConcreteNext.  Codec,  *)
(* allocation, Ready ownership, SCC execution, and TOQ timing are outside   *)
(* this theorem.  Separate abstract variables and actions make refinement a *)
(* relation between two state machines rather than a self-projection.       *)
(*                                                                         *)
(* Checked with the pinned native arm64 Darwin TLAPS prerelease commit       *)
(* 763bf3c.  That commit is intentionally recorded as a prerelease commit,  *)
(* not represented as a stable TLAPS release.                               *)
(***************************************************************************)

CONSTANTS Replicas, Instances, Values, Ballots, Configurations, Coordinators,
          NoValue, Noop, BottomBallot, RootConfig, PinnedConfig,
          Voters, Quorums, BallotOwner, InstanceOwner, ClientProposed,
          Generation, Predecessor, ConfigCommand, ConfigCommandInstance,
          ExecutionRank

FiniteCarrierAssumptions ==
    /\ IsFiniteSet(Replicas) /\ IsFiniteSet(Instances)
    /\ IsFiniteSet(Values) /\ IsFiniteSet(Ballots)
    /\ IsFiniteSet(Configurations) /\ IsFiniteSet(Coordinators)

CarrierAssumptions ==
    /\ Replicas # {} /\ Instances # {} /\ Values # {}
    /\ Ballots # {} /\ Ballots \subseteq Nat
    /\ Configurations # {} /\ Coordinators # {}
    /\ NoValue \notin Values /\ Noop \in Values
    /\ BottomBallot \in Ballots
    /\ \A b \in Ballots : BottomBallot <= b
    /\ RootConfig \in Configurations
    /\ PinnedConfig \in [Instances -> Configurations]

QuorumAssumptions ==
    /\ Voters \in [Configurations -> SUBSET Replicas]
    /\ Quorums \in [Configurations -> SUBSET (SUBSET Replicas)]
    /\ \A c \in Configurations :
          /\ Quorums[c] # {}
          /\ \A q \in Quorums[c] : q # {} /\ q \subseteq Voters[c]
          /\ \A q1, q2 \in Quorums[c] : q1 \cap q2 # {}

OwnershipAssumptions ==
    /\ BallotOwner \in [Instances -> [Ballots -> Coordinators]]
    /\ InstanceOwner \in [Instances -> Coordinators]
    /\ \A i \in Instances :
          BallotOwner[i][BottomBallot] = InstanceOwner[i]

ClientAssumptions ==
    /\ ClientProposed \in [Instances -> SUBSET (Values \ {Noop})]
    /\ \A i \in Instances : ClientProposed[i] # {}

ConfigurationAssumptions ==
    /\ Generation \in [Configurations -> Nat]
    /\ Predecessor \in [Configurations -> Configurations]
    /\ Generation[RootConfig] = 0
    /\ Predecessor[RootConfig] = RootConfig
    /\ \A c \in Configurations \ {RootConfig} :
          Generation[c] = Generation[Predecessor[c]] + 1
    /\ ConfigCommand \in [Configurations -> Values]
    /\ ConfigCommandInstance \in [Configurations -> Instances]
    /\ ExecutionRank \in [Configurations -> Nat]
    /\ \A c \in Configurations \ {RootConfig} :
          /\ ConfigCommand[c] # Noop
          /\ ConfigCommand[c] \in ClientProposed[ConfigCommandInstance[c]]
          /\ PinnedConfig[ConfigCommandInstance[c]] = Predecessor[c]
    /\ \A c1, c2 \in Configurations :
          ConfigCommand[c1] = ConfigCommand[c2] => c1 = c2
    /\ \A c1, c2 \in Configurations \ {RootConfig} :
          ConfigCommandInstance[c1] = ConfigCommandInstance[c2] => c1 = c2
    /\ \A c1, c2 \in Configurations \ {RootConfig} :
          /\ Predecessor[c1] = Predecessor[c2]
          /\ Generation[c1] = Generation[c2]
          /\ c1 # c2
          => ExecutionRank[c1] # ExecutionRank[c2]

DomainAssumptions ==
    /\ FiniteCarrierAssumptions /\ CarrierAssumptions /\ QuorumAssumptions
    /\ OwnershipAssumptions /\ ClientAssumptions
    /\ ConfigurationAssumptions

Acceptance(r, i, b, v, c) ==
    [replica |-> r, instance |-> i, ballot |-> b, value |-> v,
     config |-> c]

Evidence(k, r, i, b, c, h) ==
    [coordinator |-> k, sender |-> r, instance |-> i, ballot |-> b,
     config |-> c, history |-> h]

Proposal(i, b, v, c, k, kind, q, es) ==
    [instance |-> i, ballot |-> b, value |-> v, config |-> c,
     coordinator |-> k, kind |-> kind, evidenceQuorum |-> q,
     evidence |-> es]

Choice(i, b, v, c, q) ==
    [instance |-> i, ballot |-> b, value |-> v, config |-> c,
     quorum |-> q]

Round(k, i, b, c) ==
    [coordinator |-> k, instance |-> i, ballot |-> b, config |-> c]

SiblingConfigs(left, right) ==
    /\ left \in Configurations \ {RootConfig}
    /\ right \in Configurations \ {RootConfig}
    /\ Predecessor[left] = Predecessor[right]
    /\ Generation[left] = Generation[right]

ExecutionPrecedes(left, right) ==
    /\ SiblingConfigs(left, right)
    /\ ExecutionRank[left] < ExecutionRank[right]

ConfChangeResult(c, outcome, winner) ==
    [proposed |-> c, base |-> Predecessor[c],
     generation |-> Generation[c],
     commandInstance |-> ConfigCommandInstance[c],
     command |-> ConfigCommand[c],
     executionRank |-> ExecutionRank[c],
     outcome |-> outcome, winner |-> winner]

RootResult == ConfChangeResult(RootConfig, "applied", RootConfig)

AcceptanceRecords ==
    [replica : Replicas, instance : Instances, ballot : Ballots,
     value : Values, config : Configurations]
EvidenceRecords ==
    [coordinator : Coordinators, sender : Replicas, instance : Instances,
     ballot : Ballots, config : Configurations,
     history : SUBSET AcceptanceRecords]
ProposalRecords ==
    [instance : Instances, ballot : Ballots, value : Values,
     config : Configurations, coordinator : Coordinators,
     kind : {"normal", "recovery"}, evidenceQuorum : SUBSET Replicas,
     evidence : SUBSET EvidenceRecords]
ChoiceRecords ==
    [instance : Instances, ballot : Ballots, value : Values,
     config : Configurations, quorum : SUBSET Replicas]
RoundRecords ==
    [coordinator : Coordinators, instance : Instances, ballot : Ballots,
     config : Configurations]
ConfigRecords ==
    [proposed : Configurations, base : Configurations, generation : Nat,
     commandInstance : Instances, command : Values, executionRank : Nat,
     outcome : {"applied", "rejected-superseded"},
     winner : Configurations]

VARIABLES promise, acceptedBallot, acceptedValue,
          proposalHistory, acceptedHistory, chosenHistory,
          currentRound, roundHistory, evidenceHistory, countedEvidence,
          configHistory,
          retryPending, requestInFlight, replyAvailable, recoveryComplete,
          retryCount, wireEpoch,
          absProposalHistory, absAcceptedHistory, absChosenHistory,
          absConfigHistory

ConcreteVars ==
    <<promise, acceptedBallot, acceptedValue,
      proposalHistory, acceptedHistory, chosenHistory,
      currentRound, roundHistory, evidenceHistory, countedEvidence,
      configHistory, retryPending, requestInFlight, replyAvailable,
      recoveryComplete, retryCount, wireEpoch>>
AbstractVars ==
    <<absProposalHistory, absAcceptedHistory, absChosenHistory,
      absConfigHistory>>
Vars == <<ConcreteVars, AbstractVars>>
Update2(f, x, y, z) == [f EXCEPT ![x][y] = z]


SenderHistory(r, i) ==
    {a \in acceptedHistory : a.replica = r /\ a.instance = i}

ArchivedEvidenceFor(es, k, i, b, c, q) ==
    /\ \A e \in es :
          /\ e.coordinator = k /\ e.instance = i /\ e.ballot = b
          /\ e.config = c /\ e.sender \in q
    /\ \A r \in q : \E e \in es : e.sender = r
    /\ \A e1, e2 \in es : e1.sender = e2.sender => e1 = e2

EvidenceFor(es, k, i, b, c, q) ==
    /\ es \subseteq countedEvidence
    /\ \A e \in es :
          /\ e.coordinator = k /\ e.instance = i /\ e.ballot = b
          /\ e.config = c /\ e.sender \in q
    /\ \A r \in q : \E e \in es : e.sender = r
    /\ \A e1, e2 \in es : e1.sender = e2.sender => e1 = e2

CertificateHistory(es) == UNION {e.history : e \in es}

HighestSelected(es, v) ==
    LET hs == CertificateHistory(es)
    IN
    \/ /\ hs = {}
       /\ v = Noop
    \/ /\ hs # {}
       /\ \E top \in hs :
             /\ top.value = v
             /\ \A a \in hs : a.ballot <= top.ballot
             /\ \A a \in hs : a.ballot = top.ballot => a.value = v

RecoverySelection(es, k, i, b, c, q, v) ==
    /\ EvidenceFor(es, k, i, b, c, q)
    /\ HighestSelected(es, v)

ProposalForChoice(ch, p) ==
    /\ p \in proposalHistory
    /\ p.instance = ch.instance /\ p.ballot = ch.ballot
    /\ p.value = ch.value /\ p.config = ch.config

ProposalSafeForChoice(p, ch) ==
    /\ p.instance = ch.instance
    /\ ch.ballot < p.ballot
    /\ p.value = ch.value

ChosenConfigIn(history, c) ==
    \E ch \in history :
        /\ ch.instance = ConfigCommandInstance[c]
        /\ ch.value = ConfigCommand[c]

HasConfigResultIn(history, c) ==
    \E result \in history : result.proposed = c

PriorAppliedIn(history, c) ==
    {result \in history :
        /\ result.outcome = "applied"
        /\ ExecutionPrecedes(result.proposed, c)}

ConfigExecutionGuard(choices, results, c, outcome, winner) ==
    /\ c \in Configurations \ {RootConfig}
    /\ ChosenConfigIn(choices, c)
    /\ ~HasConfigResultIn(results, c)
    /\ \E parent \in results :
          /\ parent.proposed = Predecessor[c]
          /\ parent.outcome = "applied"
    /\ \A earlier \in Configurations \ {RootConfig} :
          ExecutionPrecedes(earlier, c) =>
              HasConfigResultIn(results, earlier)
    /\ \/ /\ PriorAppliedIn(results, c) = {}
          /\ outcome = "applied" /\ winner = c
       \/ /\ PriorAppliedIn(results, c) # {}
          /\ outcome = "rejected-superseded"
          /\ \E prior \in PriorAppliedIn(results, c) :
                winner = prior.proposed

EffectiveBacked(result) ==
    result.proposed = RootConfig \/
      ChosenConfigIn(chosenHistory, result.proposed)

AcceptorTypeOK ==
    /\ promise \in [Replicas -> [Instances -> Ballots]]
    /\ acceptedBallot \in [Replicas -> [Instances -> Ballots]]
    /\ acceptedValue \in [Replicas -> [Instances -> Values \cup {NoValue}]]

HistoryTypeOK ==
    /\ proposalHistory \subseteq ProposalRecords
    /\ acceptedHistory \subseteq AcceptanceRecords
    /\ chosenHistory \subseteq ChoiceRecords
    /\ roundHistory \subseteq RoundRecords
    /\ evidenceHistory \subseteq EvidenceRecords
    /\ countedEvidence \subseteq EvidenceRecords
    /\ configHistory \subseteq ConfigRecords

ControlTypeOK ==
    /\ currentRound \in [Coordinators -> [Instances -> Ballots]]
    /\ retryPending \in BOOLEAN /\ requestInFlight \in BOOLEAN
    /\ replyAvailable \in BOOLEAN /\ recoveryComplete \in BOOLEAN
    /\ retryCount \in Nat /\ wireEpoch \in Nat

AbstractTypeOK ==
    /\ absProposalHistory \subseteq ProposalRecords
    /\ absAcceptedHistory \subseteq AcceptanceRecords
    /\ absChosenHistory \subseteq ChoiceRecords
    /\ absConfigHistory \subseteq ConfigRecords

TypeOK ==
    /\ AcceptorTypeOK /\ HistoryTypeOK /\ ControlTypeOK /\ AbstractTypeOK

ProposalWellFormed(p) ==
    /\ p.config = PinnedConfig[p.instance]
    /\ p.coordinator = BallotOwner[p.instance][p.ballot]
    /\ (p.kind = "normal" =>
          /\ p.ballot = BottomBallot /\ p.evidence = {}
          /\ p.evidenceQuorum = {}
          /\ p.value \in ClientProposed[p.instance])
    /\ (p.kind = "recovery" =>
          /\ ArchivedEvidenceFor(p.evidence, p.coordinator, p.instance,
                                 p.ballot, p.config, p.evidenceQuorum)
          /\ HighestSelected(p.evidence, p.value))

ProposalShapeSound ==
    \A p \in proposalHistory : ProposalWellFormed(p)

ProposalEvidenceArchived ==
    \A p \in proposalHistory :
        p.kind = "recovery" => p.evidence \subseteq evidenceHistory

ProposalBallotUnique ==
    \A p1, p2 \in proposalHistory :
        p1.instance = p2.instance /\ p1.ballot = p2.ballot => p1 = p2

ProposalSafePair(p, ch) ==
    p.instance = ch.instance /\ ch.ballot < p.ballot =>
        p.value = ch.value

ProposalChosenSafe ==
    \A p \in proposalHistory, ch \in chosenHistory :
        ProposalSafePair(p, ch)

ProposalHistorySound ==
    /\ ProposalShapeSound /\ ProposalEvidenceArchived
    /\ ProposalBallotUnique /\ ProposalChosenSafe

AcceptedHistorySound ==
    /\ \A a \in acceptedHistory :
          /\ a.config = PinnedConfig[a.instance]
          /\ \E p \in proposalHistory :
                /\ p.instance = a.instance /\ p.ballot = a.ballot
                /\ p.value = a.value /\ p.config = a.config
    /\ \A a1, a2 \in acceptedHistory :
          /\ a1.replica = a2.replica /\ a1.instance = a2.instance
          /\ a1.ballot = a2.ballot
          => a1.value = a2.value
    /\ \A r \in Replicas, i \in Instances :
          /\ acceptedBallot[r][i] <= promise[r][i]
          /\ (acceptedValue[r][i] # NoValue =>
                /\ Acceptance(r, i, acceptedBallot[r][i],
                              acceptedValue[r][i], PinnedConfig[i])
                       \in acceptedHistory
                /\ \A a \in SenderHistory(r, i) :
                      a.ballot <= acceptedBallot[r][i])

EvidenceWellFormed(e) ==
    /\ Round(e.coordinator, e.instance, e.ballot, e.config) \in roundHistory
    /\ e.config = PinnedConfig[e.instance]
    /\ e.history \subseteq acceptedHistory
    /\ e.ballot <= promise[e.sender][e.instance]
    /\ \A a \in e.history :
          a.replica = e.sender /\ a.instance = e.instance
    /\ \A a \in SenderHistory(e.sender, e.instance) :
          a.ballot < e.ballot => a \in e.history

EvidenceArchiveSound ==
    \A e \in evidenceHistory : EvidenceWellFormed(e)

CountedEvidenceCurrent ==
    \A e \in countedEvidence :
        e.ballot = currentRound[e.coordinator][e.instance]

CountedEvidenceUnique ==
    \A e1, e2 \in countedEvidence :
        /\ e1.coordinator = e2.coordinator /\ e1.sender = e2.sender
        /\ e1.instance = e2.instance /\ e1.ballot = e2.ballot
        => e1 = e2

EvidenceHistorySound ==
    /\ countedEvidence \subseteq evidenceHistory
    /\ EvidenceArchiveSound /\ CountedEvidenceCurrent
    /\ CountedEvidenceUnique

CurrentRoundResponsesOnly ==
    /\ countedEvidence \subseteq evidenceHistory
    /\ CountedEvidenceCurrent
    /\ \A e \in countedEvidence :
          e.config = PinnedConfig[e.instance]
    /\ \A e1, e2 \in countedEvidence :
          /\ e1.coordinator = e2.coordinator /\ e1.sender = e2.sender
          /\ e1.instance = e2.instance
          => e1 = e2

ChosenCertificateSound ==
    \A ch \in chosenHistory :
        /\ ch.config = PinnedConfig[ch.instance]
        /\ ch.quorum \in Quorums[ch.config]
        /\ \E p \in proposalHistory : ProposalForChoice(ch, p)
        /\ \A r \in ch.quorum :
              Acceptance(r, ch.instance, ch.ballot, ch.value, ch.config)
                  \in acceptedHistory

ChosenAgreement ==
    \A ch1, ch2 \in chosenHistory :
        ch1.instance = ch2.instance => ch1.value = ch2.value

ConfigHistoryWellFormed ==
    /\ RootResult \in configHistory
    /\ \A result \in configHistory :
          result =
            ConfChangeResult(result.proposed, result.outcome, result.winner)
    /\ \A result \in configHistory : EffectiveBacked(result)
    /\ \A r1, r2 \in configHistory :
          r1.proposed = r2.proposed => r1 = r2
    /\ \A result \in configHistory :
          result.proposed # RootConfig =>
            /\ \E parent \in configHistory :
                  /\ parent.proposed = result.base
                  /\ parent.outcome = "applied"
            /\ \A earlier \in Configurations \ {RootConfig} :
                  ExecutionPrecedes(earlier, result.proposed) =>
                      HasConfigResultIn(configHistory, earlier)
            /\ (result.outcome = "applied" =>
                  /\ result.winner = result.proposed
                  /\ PriorAppliedIn(configHistory, result.proposed) = {})
            /\ (result.outcome = "rejected-superseded" =>
                  \E prior \in PriorAppliedIn(configHistory,
                                               result.proposed) :
                      result.winner = prior.proposed)

SingleAppliedOutcome ==
    \A r1, r2 \in configHistory :
        /\ r1.outcome = "applied" /\ r2.outcome = "applied"
        /\ r1.base = r2.base /\ r1.generation = r2.generation
        => r1.proposed = r2.proposed

ConfigHistorySafety == ConfigHistoryWellFormed /\ SingleAppliedOutcome

ValueProvenance ==
    /\ \A p \in proposalHistory :
          p.value = Noop \/ p.value \in ClientProposed[p.instance]
    /\ \A a \in acceptedHistory :
          a.value = Noop \/ a.value \in ClientProposed[a.instance]
    /\ \A ch \in chosenHistory :
          ch.value = Noop \/ ch.value \in ClientProposed[ch.instance]

Nontriviality == ValueProvenance

MappingInvariant ==
    /\ absProposalHistory = proposalHistory
    /\ absAcceptedHistory = acceptedHistory
    /\ absChosenHistory = chosenHistory
    /\ absConfigHistory = configHistory

SafetyInvariant ==
    /\ TypeOK
    /\ ProposalHistorySound
    /\ AcceptedHistorySound
    /\ EvidenceHistorySound
    /\ CurrentRoundResponsesOnly
    /\ ChosenCertificateSound
    /\ ChosenAgreement
    /\ ConfigHistorySafety
    /\ Nontriviality
    /\ MappingInvariant

Init ==
    /\ promise = [r \in Replicas |-> [i \in Instances |-> BottomBallot]]
    /\ acceptedBallot =
          [r \in Replicas |-> [i \in Instances |-> BottomBallot]]
    /\ acceptedValue = [r \in Replicas |-> [i \in Instances |-> NoValue]]
    /\ proposalHistory = {} /\ acceptedHistory = {} /\ chosenHistory = {}
    /\ currentRound =
          [k \in Coordinators |-> [i \in Instances |-> BottomBallot]]
    /\ roundHistory = {} /\ evidenceHistory = {} /\ countedEvidence = {}
    /\ configHistory = {RootResult}
    /\ retryPending = FALSE /\ requestInFlight = FALSE
    /\ replyAvailable = FALSE /\ recoveryComplete = FALSE
    /\ retryCount = 0 /\ wireEpoch = 0
    /\ absProposalHistory = {} /\ absAcceptedHistory = {}
    /\ absChosenHistory = {}
    /\ absConfigHistory = {RootResult}

NormalPropose(i, v, k) ==
    LET p == Proposal(i, BottomBallot, v, PinnedConfig[i], k,
                      "normal", {}, {})
    IN
    /\ i \in Instances /\ v \in ClientProposed[i]
    /\ k = InstanceOwner[i]
    /\ ~\E old \in proposalHistory :
          old.instance = i /\ old.ballot = BottomBallot
    /\ proposalHistory' = proposalHistory \cup {p}
    /\ absProposalHistory' = absProposalHistory \cup {p}
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   acceptedHistory, chosenHistory, currentRound, roundHistory,
                   evidenceHistory, countedEvidence, configHistory,
                   retryPending, requestInFlight, replyAvailable,
                   recoveryComplete, retryCount, wireEpoch,
                   absAcceptedHistory, absChosenHistory, absConfigHistory>>

BeginRecovery(k, i, b) ==
    /\ k \in Coordinators /\ i \in Instances /\ b \in Ballots
    /\ b > currentRound[k][i]
    /\ currentRound' = Update2(currentRound, k, i, b)
    /\ roundHistory' =
          roundHistory \cup {Round(k, i, b, PinnedConfig[i])}
    /\ countedEvidence' =
          {e \in countedEvidence :
              e.coordinator # k \/ e.instance # i}
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   evidenceHistory, configHistory,
                   retryPending, requestInFlight, replyAvailable,
                   recoveryComplete, retryCount, wireEpoch,
                   absProposalHistory, absAcceptedHistory, absChosenHistory,
                   absConfigHistory>>

CollectEvidence(k, r, i, b) ==
    LET e == Evidence(k, r, i, b, PinnedConfig[i], SenderHistory(r, i))
    IN
    /\ k \in Coordinators /\ r \in Replicas /\ i \in Instances
    /\ b \in Ballots /\ b = currentRound[k][i]
    /\ Round(k, i, b, PinnedConfig[i]) \in roundHistory
    /\ b >= promise[r][i]
    /\ ~\E old \in countedEvidence :
          old.coordinator = k /\ old.sender = r /\ old.instance = i
    /\ promise' = Update2(promise, r, i, b)
    /\ evidenceHistory' = evidenceHistory \cup {e}
    /\ countedEvidence' = countedEvidence \cup {e}
    /\ UNCHANGED <<acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, configHistory,
                   retryPending, requestInFlight, replyAvailable,
                   recoveryComplete, retryCount, wireEpoch,
                   absProposalHistory, absAcceptedHistory, absChosenHistory,
                   absConfigHistory>>

RecoveryPropose(k, i, b, v, q, es) ==
    LET p == Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es)
    IN
    /\ k \in Coordinators /\ i \in Instances /\ b \in Ballots
    /\ v \in Values /\ q \in Quorums[PinnedConfig[i]]
    /\ b = currentRound[k][i] /\ b > BottomBallot
    /\ k = BallotOwner[i][b]
    /\ RecoverySelection(es, k, i, b, PinnedConfig[i], q, v)
    /\ ~\E old \in proposalHistory :
          old.instance = i /\ old.ballot = b
    /\ proposalHistory' = proposalHistory \cup {p}
    /\ absProposalHistory' = absProposalHistory \cup {p}
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   acceptedHistory, chosenHistory, currentRound, roundHistory,
                   evidenceHistory, countedEvidence, configHistory,
                   retryPending, requestInFlight, replyAvailable,
                   recoveryComplete, retryCount, wireEpoch,
                   absAcceptedHistory, absChosenHistory, absConfigHistory>>

AcceptProposal(r, p) ==
    LET a == Acceptance(r, p.instance, p.ballot, p.value, p.config)
    IN
    /\ r \in Replicas /\ p \in proposalHistory
    /\ p.ballot >= promise[r][p.instance]
    /\ ~\E old \in acceptedHistory :
          /\ old.replica = r /\ old.instance = p.instance
          /\ old.ballot = p.ballot /\ old.value # p.value
    /\ promise' = Update2(promise, r, p.instance, p.ballot)
    /\ acceptedBallot' =
          Update2(acceptedBallot, r, p.instance, p.ballot)
    /\ acceptedValue' =
          Update2(acceptedValue, r, p.instance, p.value)
    /\ acceptedHistory' = acceptedHistory \cup {a}
    /\ absAcceptedHistory' = absAcceptedHistory \cup {a}
    /\ UNCHANGED <<proposalHistory, chosenHistory, currentRound, roundHistory,
                   evidenceHistory, countedEvidence, configHistory,
                   retryPending, requestInFlight, replyAvailable,
                   recoveryComplete, retryCount, wireEpoch,
                   absProposalHistory, absChosenHistory, absConfigHistory>>

CertifyChosen(p, q) ==
    LET ch == Choice(p.instance, p.ballot, p.value, p.config, q)
    IN
    /\ p \in proposalHistory /\ q \in Quorums[p.config]
    /\ \A r \in q :
          Acceptance(r, p.instance, p.ballot, p.value, p.config)
              \in acceptedHistory
    /\ chosenHistory' = chosenHistory \cup {ch}
    /\ absChosenHistory' = absChosenHistory \cup {ch}
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, currentRound, roundHistory,
                   evidenceHistory, countedEvidence, configHistory,
                   retryPending, requestInFlight, replyAvailable,
                   recoveryComplete, retryCount, wireEpoch,
                   absProposalHistory, absAcceptedHistory, absConfigHistory>>

RecordConfiguration(c, outcome, winner) ==
    LET result == ConfChangeResult(c, outcome, winner)
    IN
    /\ ConfigExecutionGuard(chosenHistory, configHistory,
                            c, outcome, winner)
    /\ configHistory' = configHistory \cup {result}
    /\ absConfigHistory' = absConfigHistory \cup {result}
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, evidenceHistory,
                   countedEvidence, retryPending, requestInFlight,
                   replyAvailable, recoveryComplete, retryCount, wireEpoch,
                   absProposalHistory, absAcceptedHistory, absChosenHistory>>

StartRetry ==
    /\ ~recoveryComplete /\ retryPending' = TRUE
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, evidenceHistory,
                   countedEvidence, configHistory, requestInFlight,
                   replyAvailable, recoveryComplete, retryCount, wireEpoch,
                   AbstractVars>>

IssueRetry ==
    /\ retryPending /\ ~recoveryComplete
    /\ requestInFlight' = TRUE
    /\ retryCount' = retryCount + 1 /\ wireEpoch' = wireEpoch + 1
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, evidenceHistory,
                   countedEvidence, configHistory, retryPending,
                   replyAvailable, recoveryComplete, AbstractVars>>

DropRequest ==
    /\ requestInFlight /\ ~replyAvailable /\ requestInFlight' = FALSE
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, evidenceHistory,
                   countedEvidence, configHistory, retryPending,
                   replyAvailable, recoveryComplete, retryCount, wireEpoch,
                   AbstractVars>>

DeliverRequest ==
    /\ requestInFlight /\ requestInFlight' = FALSE /\ replyAvailable' = TRUE
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, evidenceHistory,
                   countedEvidence, configHistory, retryPending,
                   recoveryComplete, retryCount, wireEpoch, AbstractVars>>

CompletionVars == <<recoveryComplete>>

FinishRetry ==
    /\ replyAvailable
    /\ recoveryComplete' = TRUE

CompleteRecovery ==
    /\ FinishRetry
    /\ retryPending' = FALSE /\ requestInFlight' = FALSE
    /\ replyAvailable' = FALSE
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, evidenceHistory,
                   countedEvidence, configHistory, retryCount, wireEpoch,
                   absProposalHistory, absAcceptedHistory, absChosenHistory,
                   absConfigHistory>>

InternalWireStep ==
    /\ wireEpoch' = wireEpoch + 1
    /\ UNCHANGED <<promise, acceptedBallot, acceptedValue,
                   proposalHistory, acceptedHistory, chosenHistory,
                   currentRound, roundHistory, evidenceHistory,
                   countedEvidence, configHistory, retryPending,
                   requestInFlight, replyAvailable, recoveryComplete,
                   retryCount, AbstractVars>>

ConcreteNext ==
    \/ \E i \in Instances, v \in Values, k \in Coordinators :
          NormalPropose(i, v, k)
    \/ \E k \in Coordinators, i \in Instances, b \in Ballots :
          BeginRecovery(k, i, b)
    \/ \E k \in Coordinators, r \in Replicas, i \in Instances,
          b \in Ballots : CollectEvidence(k, r, i, b)
    \/ \E k \in Coordinators, i \in Instances, b \in Ballots,
          v \in Values :
          \E q \in Quorums[PinnedConfig[i]] :
            \E es \in SUBSET EvidenceRecords :
              RecoveryPropose(k, i, b, v, q, es)
    \/ \E r \in Replicas, p \in proposalHistory : AcceptProposal(r, p)
    \/ \E p \in proposalHistory :
          \E q \in Quorums[p.config] : CertifyChosen(p, q)
    \/ \E c \in Configurations,
          outcome \in {"applied", "rejected-superseded"},
          winner \in Configurations :
          RecordConfiguration(c, outcome, winner)
    \/ StartRetry \/ IssueRetry \/ DropRequest \/ DeliverRequest
    \/ CompleteRecovery \/ InternalWireStep

Stutter ==
    /\ promise' = promise /\ acceptedBallot' = acceptedBallot
    /\ acceptedValue' = acceptedValue /\ proposalHistory' = proposalHistory
    /\ acceptedHistory' = acceptedHistory /\ chosenHistory' = chosenHistory
    /\ currentRound' = currentRound /\ roundHistory' = roundHistory
    /\ evidenceHistory' = evidenceHistory
    /\ countedEvidence' = countedEvidence /\ configHistory' = configHistory
    /\ retryPending' = retryPending /\ requestInFlight' = requestInFlight
    /\ replyAvailable' = replyAvailable
    /\ recoveryComplete' = recoveryComplete
    /\ retryCount' = retryCount /\ wireEpoch' = wireEpoch
    /\ absProposalHistory' = absProposalHistory
    /\ absAcceptedHistory' = absAcceptedHistory
    /\ absChosenHistory' = absChosenHistory
    /\ absConfigHistory' = absConfigHistory

NextOrStutter == ConcreteNext \/ Stutter
LEMMA Update2At ==
    ASSUME NEW A, NEW B, NEW C, NEW f \in [A -> [B -> C]],
           NEW x \in A, NEW y \in B, NEW z \in C
    PROVE Update2(f, x, y, z)[x][y] = z
BY SMT DEF Update2

LEMMA Update2Off ==
    ASSUME NEW A, NEW B, NEW C, NEW f \in [A -> [B -> C]],
           NEW x \in A, NEW y \in B, NEW z \in C,
           NEW u \in A, NEW w \in B, u # x \/ w # y
    PROVE Update2(f, x, y, z)[u][w] = f[u][w]
BY SMT DEF Update2
LEMMA Update2Type ==
    ASSUME NEW A, NEW B, NEW C, NEW f \in [A -> [B -> C]],
           NEW x \in A, NEW y \in B, NEW z \in C
    PROVE Update2(f, x, y, z) \in [A -> [B -> C]]
BY SMT DEF Update2

LEMMA BeginCurrentOff ==
    ASSUME NEW k, NEW i, NEW b, NEW x, NEW y,
           TypeOK, BeginRecovery(k, i, b),
           x \in Coordinators, y \in Instances, x # k \/ y # i
    PROVE currentRound'[x][y] = currentRound[x][y]
BY Update2Off, SMT DEF TypeOK, ControlTypeOK, BeginRecovery

LEMMA CollectPromiseAt ==
    ASSUME NEW k, NEW r, NEW i, NEW b,
           TypeOK, CollectEvidence(k, r, i, b)
    PROVE promise'[r][i] = b
BY Update2At, SMT DEF TypeOK, AcceptorTypeOK, CollectEvidence

LEMMA CollectPromiseOff ==
    ASSUME NEW k, NEW r, NEW i, NEW b, NEW rr, NEW ii,
           TypeOK, CollectEvidence(k, r, i, b),
           rr \in Replicas, ii \in Instances, rr # r \/ ii # i
    PROVE promise'[rr][ii] = promise[rr][ii]
BY Update2Off, SMT DEF TypeOK, AcceptorTypeOK, CollectEvidence

LEMMA AcceptUpdatesAt ==
    ASSUME NEW r, NEW p, TypeOK, p \in ProposalRecords,
           AcceptProposal(r, p)
    PROVE /\ promise'[r][p.instance] = p.ballot
          /\ acceptedBallot'[r][p.instance] = p.ballot
          /\ acceptedValue'[r][p.instance] = p.value
BY Update2At, SMT
   DEF TypeOK, AcceptorTypeOK, ProposalRecords, AcceptProposal

LEMMA AcceptUpdatesOff ==
    ASSUME NEW r, NEW p, NEW rr, NEW ii,
           TypeOK, p \in ProposalRecords, AcceptProposal(r, p),
           rr \in Replicas, ii \in Instances,
           rr # r \/ ii # p.instance
    PROVE /\ promise'[rr][ii] = promise[rr][ii]
          /\ acceptedBallot'[rr][ii] = acceptedBallot[rr][ii]
          /\ acceptedValue'[rr][ii] = acceptedValue[rr][ii]
BY Update2Off, SMT
   DEF TypeOK, AcceptorTypeOK, ProposalRecords, AcceptProposal


Spec == DomainAssumptions /\ Init /\ []NextOrStutter

(***************************************************************************)
(* Independent abstract machine over distinct abstract variables.           *)
(***************************************************************************)

AbsPropose(p) ==
    /\ p \in ProposalRecords /\ p.config = PinnedConfig[p.instance]
    /\ absProposalHistory' = absProposalHistory \cup {p}
    /\ UNCHANGED <<absAcceptedHistory, absChosenHistory, absConfigHistory>>
AbsAccept(a) ==
    /\ a \in AcceptanceRecords
    /\ \E p \in absProposalHistory :
          p.instance = a.instance /\ p.ballot = a.ballot /\ p.value = a.value
    /\ absAcceptedHistory' = absAcceptedHistory \cup {a}
    /\ UNCHANGED <<absProposalHistory, absChosenHistory, absConfigHistory>>
AbsChoose(ch) ==
    /\ ch \in ChoiceRecords /\ ch.quorum \in Quorums[ch.config]
    /\ \A r \in ch.quorum :
          Acceptance(r, ch.instance, ch.ballot, ch.value, ch.config)
              \in absAcceptedHistory
    /\ absChosenHistory' = absChosenHistory \cup {ch}
    /\ UNCHANGED <<absProposalHistory, absAcceptedHistory, absConfigHistory>>
AbsConfigure(result) ==
    /\ result \in ConfigRecords
    /\ ConfigExecutionGuard(absChosenHistory, absConfigHistory,
                            result.proposed, result.outcome, result.winner)
    /\ absConfigHistory' = absConfigHistory \cup {result}
    /\ UNCHANGED <<absProposalHistory, absAcceptedHistory, absChosenHistory>>
AbsStutter ==
    /\ absProposalHistory' = absProposalHistory
    /\ absAcceptedHistory' = absAcceptedHistory
    /\ absChosenHistory' = absChosenHistory
    /\ absConfigHistory' = absConfigHistory
AbstractNext ==
    \/ \E p \in ProposalRecords : AbsPropose(p)
    \/ \E a \in AcceptanceRecords : AbsAccept(a)
    \/ \E ch \in ChoiceRecords : AbsChoose(ch)
    \/ \E h \in ConfigRecords : AbsConfigure(h)
    \/ AbsStutter

(***************************************************************************)
(* Inductive preservation lemmas.  Each action proof is split from the      *)
(* temporal induction so TLAPS checks the base and every action class.       *)
(***************************************************************************)

LEMMA InitEstablishesInvariant ==
    ASSUME DomainAssumptions, Init
    PROVE SafetyInvariant
PROOF
<1>1. AcceptorTypeOK
    BY SMT DEF DomainAssumptions, CarrierAssumptions, Init, AcceptorTypeOK
<1>2. RootResult \in ConfigRecords
    BY SMT DEF DomainAssumptions, CarrierAssumptions,
               ConfigurationAssumptions, ConfigRecords, ConfChangeResult,
               RootResult
<1>3. HistoryTypeOK
    BY <1>2, SMT DEF Init, HistoryTypeOK
<1>4. ControlTypeOK
    BY SMT DEF DomainAssumptions, CarrierAssumptions, Init, ControlTypeOK
<1>5. AbstractTypeOK
    BY <1>2, SMT DEF Init, AbstractTypeOK
<1>6. TypeOK BY <1>1, <1>3, <1>4, <1>5 DEF TypeOK
<1>7. proposalHistory = {} /\ acceptedHistory = {} /\
       chosenHistory = {} /\ evidenceHistory = {} /\ countedEvidence = {}
    BY DEF Init
<1>8. ProposalHistorySound
    BY <1>7
       DEF ProposalHistorySound, ProposalShapeSound, ProposalEvidenceArchived,
           ProposalBallotUnique, ProposalChosenSafe
<1>9. AcceptedHistorySound
    BY <1>7, SMT DEF DomainAssumptions, CarrierAssumptions, Init,
                       AcceptedHistorySound, SenderHistory
<1>10. EvidenceHistorySound /\ CurrentRoundResponsesOnly
    BY <1>7
       DEF EvidenceHistorySound, EvidenceArchiveSound,
           CountedEvidenceCurrent, CountedEvidenceUnique,
           CurrentRoundResponsesOnly
<1>11. ChosenCertificateSound /\ ChosenAgreement
    BY <1>7 DEF ChosenCertificateSound, ChosenAgreement
<1>12. ConfigHistorySafety
    BY <1>2, SMT DEF DomainAssumptions, CarrierAssumptions,
               ConfigurationAssumptions, Init, ConfigHistorySafety,
               ConfigHistoryWellFormed, SingleAppliedOutcome,
               EffectiveBacked, ChosenConfigIn, ConfChangeResult, RootResult,
               PriorAppliedIn, HasConfigResultIn, ExecutionPrecedes,
               SiblingConfigs
<1>13. Nontriviality /\ MappingInvariant
    BY <1>7, SMT DEF Init, Nontriviality, ValueProvenance, MappingInvariant
<1> QED BY <1>6, <1>8, <1>9, <1>10, <1>11, <1>12, <1>13
    DEF SafetyInvariant

LEMMA ChosenBallotsAtLeastBottom ==
    ASSUME DomainAssumptions, TypeOK
    PROVE \A ch \in chosenHistory : BottomBallot <= ch.ballot
BY SMT DEF DomainAssumptions, CarrierAssumptions, TypeOK, HistoryTypeOK,
           ChoiceRecords

LEMMA DecisionConfigLeibniz ==
    ASSUME chosenHistory' = chosenHistory,
           configHistory' = configHistory,
           ChosenAgreement, ConfigHistorySafety
    PROVE ChosenAgreement' /\ ConfigHistorySafety'
BY SMT DEF ChosenAgreement, ConfigHistorySafety, ConfigHistoryWellFormed,
           SingleAppliedOutcome, EffectiveBacked, ChosenConfigIn,
           PriorAppliedIn, HasConfigResultIn

LEMMA NormalProposePreserves ==
    ASSUME NEW i, NEW v, NEW k,
           DomainAssumptions, SafetyInvariant, NormalPropose(i, v, k)
    PROVE SafetyInvariant'
PROOF
<1>1. AcceptorTypeOK' /\ ControlTypeOK'
    BY SMT DEF SafetyInvariant, TypeOK, AcceptorTypeOK, ControlTypeOK,
               NormalPropose
<1>2. HistoryTypeOK'
    BY SMT DEF DomainAssumptions, CarrierAssumptions, OwnershipAssumptions,
               ClientAssumptions, SafetyInvariant, TypeOK, HistoryTypeOK,
               NormalPropose, Proposal, ProposalRecords
<1>3. AbstractTypeOK'
    BY SMT DEF DomainAssumptions, CarrierAssumptions, OwnershipAssumptions,
               ClientAssumptions, SafetyInvariant, TypeOK, AbstractTypeOK,
               NormalPropose, Proposal, ProposalRecords
<1>4. TypeOK' BY <1>1, <1>2, <1>3 DEF TypeOK
<1>5. ProposalWellFormed(
          Proposal(i, BottomBallot, v, PinnedConfig[i], k, "normal", {}, {}))'
    BY SMT DEF DomainAssumptions, CarrierAssumptions, OwnershipAssumptions,
               ClientAssumptions, NormalPropose, ProposalWellFormed, Proposal
<1>6. \A p \in proposalHistory' : ProposalWellFormed(p)'
    PROOF
    <2>1. TAKE p \in proposalHistory'
    <2>2. p \in proposalHistory \/
           p = Proposal(i, BottomBallot, v, PinnedConfig[i], k,
                        "normal", {}, {})
        BY SMT DEF NormalPropose, Proposal
    <2>3. p \in proposalHistory => ProposalWellFormed(p)'
        BY SMT DEF SafetyInvariant, ProposalHistorySound,
                   ProposalShapeSound, NormalPropose
    <2>4. p = Proposal(i, BottomBallot, v, PinnedConfig[i], k,
                       "normal", {}, {}) => ProposalWellFormed(p)'
        BY <1>5, SMT
    <2> QED BY <2>2, <2>3, <2>4, SMT
<1>7. ProposalShapeSound'
    BY <1>6 DEF ProposalShapeSound
<1>8. ProposalBallotUnique'
    BY SMT DEF SafetyInvariant, ProposalHistorySound, ProposalBallotUnique,
               NormalPropose, Proposal
<1>9. \A ch \in chosenHistory :
          ProposalSafePair(
            Proposal(i, BottomBallot, v, PinnedConfig[i], k, "normal", {}, {}),
            ch)
    PROOF
    <2>1. TAKE ch \in chosenHistory
    <2>2. BottomBallot <= ch.ballot /\ BottomBallot \in Nat /\
           ch.ballot \in Nat
        BY ChosenBallotsAtLeastBottom, SMT
           DEF DomainAssumptions, CarrierAssumptions, SafetyInvariant,
               TypeOK, HistoryTypeOK, ChoiceRecords
    <2>3. ~(ch.ballot < BottomBallot) BY <2>2, SMT
    <2> QED BY <2>3 DEF ProposalSafePair, Proposal
<1>10. \A p \in proposalHistory', ch \in chosenHistory' :
          ProposalSafePair(p, ch)
    PROOF
    <2>1. TAKE p \in proposalHistory', ch \in chosenHistory'
    <2>2. ch \in chosenHistory
        BY SMT DEF NormalPropose
    <2>3. p \in proposalHistory \/
           p = Proposal(i, BottomBallot, v, PinnedConfig[i], k,
                        "normal", {}, {})
        BY SMT DEF NormalPropose, Proposal
    <2>4. p \in proposalHistory => ProposalSafePair(p, ch)
        BY <2>2, SMT DEF SafetyInvariant, ProposalHistorySound,
                            ProposalChosenSafe
    <2>5. p = Proposal(i, BottomBallot, v, PinnedConfig[i], k,
                       "normal", {}, {}) => ProposalSafePair(p, ch)
        BY <1>9, <2>2, SMT
    <2> QED BY <2>3, <2>4, <2>5, SMT
<1>11. ProposalChosenSafe' BY <1>10 DEF ProposalChosenSafe
<1>12. ProposalHistorySound'
    BY <1>7, <1>8, <1>11, SMT
       DEF SafetyInvariant, ProposalHistorySound, ProposalEvidenceArchived,
           NormalPropose, Proposal
<1>13. AcceptedHistorySound'
    BY SMT DEF SafetyInvariant, AcceptedHistorySound, NormalPropose,
               SenderHistory
<1>14. EvidenceHistorySound' /\ CurrentRoundResponsesOnly'
    BY SMT DEF SafetyInvariant, EvidenceHistorySound, EvidenceArchiveSound,
               EvidenceWellFormed, CountedEvidenceCurrent,
               CountedEvidenceUnique, CurrentRoundResponsesOnly,
               NormalPropose, SenderHistory
<1>15. ChosenCertificateSound'
    BY SMT DEF SafetyInvariant, ChosenCertificateSound, NormalPropose,
               ProposalForChoice
<1>16. chosenHistory' = chosenHistory /\ configHistory' = configHistory
    BY DEF NormalPropose
<1>17. ChosenAgreement' /\ ConfigHistorySafety'
    BY <1>16, DecisionConfigLeibniz, SMT DEF SafetyInvariant
<1>18. Nontriviality' /\ MappingInvariant'
    BY SMT DEF SafetyInvariant, Nontriviality, ValueProvenance,
               MappingInvariant, NormalPropose, Proposal
<1> QED BY <1>4, <1>12, <1>13, <1>14, <1>15, <1>17, <1>18
    DEF SafetyInvariant

LEMMA BeginRecoveryPreserves ==
    ASSUME NEW k, NEW i, NEW b,
           DomainAssumptions, SafetyInvariant, BeginRecovery(k, i, b)
    PROVE SafetyInvariant'
PROOF
<1>1. TypeOK'
    PROOF
    <2>1. AcceptorTypeOK'
        BY SMT DEF BeginRecovery, SafetyInvariant, TypeOK, AcceptorTypeOK
    <2>2. currentRound' \in [Coordinators -> [Instances -> Ballots]]
        BY Update2Type, SMT
           DEF SafetyInvariant, TypeOK, ControlTypeOK, BeginRecovery
    <2>3. ControlTypeOK'
        BY <2>2, SMT DEF SafetyInvariant, TypeOK, ControlTypeOK, BeginRecovery
    <2>4. HistoryTypeOK'
        BY SMT
           DEF DomainAssumptions, CarrierAssumptions, SafetyInvariant,
               TypeOK, HistoryTypeOK, BeginRecovery, Round, RoundRecords
    <2>5. AbstractTypeOK'
        BY SMT DEF BeginRecovery, SafetyInvariant, TypeOK, AbstractTypeOK
    <2> QED BY <2>1, <2>3, <2>4, <2>5 DEF TypeOK
<1>2. roundHistory \subseteq roundHistory' /\
       evidenceHistory' = evidenceHistory /\
       acceptedHistory' = acceptedHistory /\ promise' = promise
    BY SMT DEF BeginRecovery
<1>3. \A e \in evidenceHistory' : EvidenceWellFormed(e)'
    PROOF
    <2>1. TAKE e \in evidenceHistory'
    <2>2. e \in evidenceHistory BY <1>2, SMT
    <2>3. EvidenceWellFormed(e)
        BY <2>2 DEF SafetyInvariant, EvidenceHistorySound,
                      EvidenceArchiveSound
    <2>4. Round(e.coordinator, e.instance, e.ballot, e.config)
            \in roundHistory'
        BY <1>2, <2>3, SMT DEF EvidenceWellFormed
    <2>5. SenderHistory(e.sender, e.instance)' =
            SenderHistory(e.sender, e.instance)
        BY <1>2 DEF SenderHistory
    <2> QED BY <1>2, <2>3, <2>4, <2>5, SMT DEF EvidenceWellFormed
<1>4. EvidenceArchiveSound' BY <1>3 DEF EvidenceArchiveSound
<1>5. countedEvidence' \subseteq countedEvidence
    BY SMT DEF BeginRecovery
<1>6. countedEvidence' \subseteq evidenceHistory'
    BY <1>2, <1>5, SMT DEF SafetyInvariant, EvidenceHistorySound
<1>7. \A e \in countedEvidence' :
       e.ballot = currentRound'[e.coordinator][e.instance]
    PROOF
    <2>1. TAKE e \in countedEvidence'
    <2>2. e \in countedEvidence /\
           (e.coordinator # k \/ e.instance # i)
        BY SMT DEF BeginRecovery
    <2>3. e.coordinator \in Coordinators /\ e.instance \in Instances
        BY <2>2, SMT
           DEF SafetyInvariant, TypeOK, HistoryTypeOK, EvidenceRecords
    <2>4. e.coordinator \in DOMAIN currentRound /\
           e.instance \in DOMAIN currentRound[e.coordinator]
        BY <2>3, SMT DEF SafetyInvariant, TypeOK, ControlTypeOK
    <2>5. e.ballot = currentRound[e.coordinator][e.instance]
        BY <2>2 DEF SafetyInvariant, EvidenceHistorySound,
                      CountedEvidenceCurrent
    <2>6. e.coordinator # k =>
           currentRound'[e.coordinator][e.instance] =
             currentRound[e.coordinator][e.instance]
        BY BeginCurrentOff, <2>2, <2>3, SMT DEF SafetyInvariant
    <2>7. e.instance # i =>
           currentRound'[e.coordinator][e.instance] =
             currentRound[e.coordinator][e.instance]
        BY BeginCurrentOff, <2>2, <2>3, SMT DEF SafetyInvariant
    <2> QED BY <2>2, <2>5, <2>6, <2>7, SMT
<1>8. CountedEvidenceCurrent' BY <1>7 DEF CountedEvidenceCurrent
<1>9. CountedEvidenceUnique'
    BY <1>5, SMT
       DEF SafetyInvariant, EvidenceHistorySound, CountedEvidenceUnique
<1>10. EvidenceHistorySound'
    BY <1>4, <1>6, <1>8, <1>9 DEF EvidenceHistorySound
<1>11. CurrentRoundResponsesOnly'
    BY <1>5, <1>6, <1>8, <1>9, SMT
       DEF SafetyInvariant, CurrentRoundResponsesOnly
<1>12. proposalHistory' = proposalHistory /\
        acceptedHistory' = acceptedHistory /\ chosenHistory' = chosenHistory /\
        configHistory' = configHistory /\ promise' = promise /\
        acceptedBallot' = acceptedBallot /\
        acceptedValue' = acceptedValue /\
        absProposalHistory' = absProposalHistory /\
        absAcceptedHistory' = absAcceptedHistory /\
        absChosenHistory' = absChosenHistory /\
        absConfigHistory' = absConfigHistory
    BY DEF BeginRecovery
<1>13. ProposalHistorySound'
    BY <1>2, <1>12, SMT
       DEF SafetyInvariant, ProposalHistorySound, ProposalShapeSound,
           ProposalEvidenceArchived, ProposalBallotUnique,
           ProposalChosenSafe, ProposalWellFormed, ArchivedEvidenceFor
<1>14. AcceptedHistorySound'
    BY <1>12, SMT DEF SafetyInvariant, AcceptedHistorySound, SenderHistory
<1>15. ChosenCertificateSound'
    BY <1>12, SMT
       DEF SafetyInvariant, ChosenCertificateSound, ProposalForChoice
<1>16. ChosenAgreement' /\ ConfigHistorySafety'
    BY <1>12, DecisionConfigLeibniz, SMT DEF SafetyInvariant
<1>17. Nontriviality' /\ MappingInvariant'
    BY <1>12, SMT
       DEF SafetyInvariant, Nontriviality, ValueProvenance, MappingInvariant
<1> QED BY <1>1, <1>10, <1>11, <1>13, <1>14, <1>15, <1>16, <1>17
    DEF SafetyInvariant

LEMMA CollectEvidencePreserves ==
    ASSUME NEW k, NEW r, NEW i, NEW b,
           DomainAssumptions, SafetyInvariant, CollectEvidence(k, r, i, b)
    PROVE SafetyInvariant'
PROOF
<1>1. TypeOK'
    PROOF
    <2>1. promise' \in [Replicas -> [Instances -> Ballots]]
        BY Update2Type, SMT
           DEF SafetyInvariant, TypeOK, AcceptorTypeOK, CollectEvidence
    <2>2. AcceptorTypeOK'
        BY <2>1, SMT
           DEF SafetyInvariant, TypeOK, AcceptorTypeOK, CollectEvidence
    <2>3. HistoryTypeOK'
        BY SMT
           DEF DomainAssumptions, CarrierAssumptions, SafetyInvariant,
               TypeOK, HistoryTypeOK, CollectEvidence, Evidence,
               EvidenceRecords, SenderHistory
    <2>4. ControlTypeOK' /\ AbstractTypeOK'
        BY SMT
           DEF CollectEvidence, SafetyInvariant, TypeOK, ControlTypeOK,
               AbstractTypeOK
    <2> QED BY <2>2, <2>3, <2>4 DEF TypeOK
<1>2. SenderHistory(r, i)' = SenderHistory(r, i) /\
       promise'[r][i] = b /\
       Round(k, i, b, PinnedConfig[i]) \in roundHistory' /\
       SenderHistory(r, i) \subseteq acceptedHistory'
    PROOF
    <2>1. SenderHistory(r, i)' = SenderHistory(r, i)
        BY DEF CollectEvidence, SenderHistory
    <2>2. promise'[r][i] = b
        BY CollectPromiseAt DEF SafetyInvariant
    <2>3. Round(k, i, b, PinnedConfig[i]) \in roundHistory'
        BY DEF CollectEvidence
    <2>4. SenderHistory(r, i) \subseteq acceptedHistory'
        BY SMT DEF CollectEvidence, SenderHistory
    <2> QED BY <2>1, <2>2, <2>3, <2>4
<1>3. evidenceHistory' =
       evidenceHistory \cup
         {Evidence(k, r, i, b, PinnedConfig[i], SenderHistory(r, i))}
    BY DEF CollectEvidence
<1>4. \A e \in evidenceHistory : EvidenceWellFormed(e)'
    PROOF
    <2>1. TAKE e \in evidenceHistory
    <2>2. EvidenceWellFormed(e)
        BY <2>1 DEF SafetyInvariant, EvidenceHistorySound,
                      EvidenceArchiveSound
    <2>3. e.sender \in Replicas /\ e.instance \in Instances
        BY <2>1, SMT
           DEF SafetyInvariant, TypeOK, HistoryTypeOK, EvidenceRecords
    <2>4. b >= promise[r][i] /\ promise'[r][i] = b
        BY CollectPromiseAt, SMT DEF CollectEvidence, SafetyInvariant
    <2>5. e.sender = r /\ e.instance = i =>
           promise[e.sender][e.instance] <=
             promise'[e.sender][e.instance]
        BY <2>4, SMT
    <2>6. ~(e.sender = r /\ e.instance = i) =>
           promise[e.sender][e.instance] =
             promise'[e.sender][e.instance]
        BY CollectPromiseOff, <2>3, SMT DEF SafetyInvariant
    <2>7. promise[e.sender][e.instance] <=
           promise'[e.sender][e.instance]
        BY <2>2, <2>4, CollectPromiseAt, CollectPromiseOff, <2>3, SMT
           DEF EvidenceWellFormed
    <2>8. SenderHistory(e.sender, e.instance)' =
           SenderHistory(e.sender, e.instance)
        BY DEF CollectEvidence, SenderHistory
    <2> QED BY <2>2, <2>7, <2>8, SMT
       DEF EvidenceWellFormed, CollectEvidence
<1>5. \A e \in evidenceHistory' : EvidenceWellFormed(e)'
    PROOF
    <2>1. TAKE e \in evidenceHistory'
    <2>2. e \in evidenceHistory \/
           e = Evidence(k, r, i, b, PinnedConfig[i], SenderHistory(r, i))
        BY <1>3, SMT
    <2>3. e \in evidenceHistory => EvidenceWellFormed(e)'
        BY <1>4
    <2>4. e = Evidence(k, r, i, b, PinnedConfig[i], SenderHistory(r, i))
           => EvidenceWellFormed(e)'
        BY <1>2, SMT
           DEF EvidenceWellFormed, Evidence, CollectEvidence, SenderHistory
    <2> QED BY <2>2, <2>3, <2>4, SMT
<1>6. EvidenceArchiveSound' BY <1>5 DEF EvidenceArchiveSound
<1>7. countedEvidence' \subseteq evidenceHistory'
    BY <1>3, SMT DEF SafetyInvariant, EvidenceHistorySound, CollectEvidence,
                         Evidence
<1>8. CountedEvidenceCurrent'
    BY SMT
       DEF SafetyInvariant, EvidenceHistorySound, CountedEvidenceCurrent,
           CollectEvidence, Evidence
<1>9. CountedEvidenceUnique'
    BY SMT
       DEF SafetyInvariant, EvidenceHistorySound, CountedEvidenceUnique,
           CollectEvidence, Evidence
<1>10. EvidenceHistorySound'
    BY <1>6, <1>7, <1>8, <1>9 DEF EvidenceHistorySound
<1>11. CurrentRoundResponsesOnly'
    BY <1>7, <1>8, <1>9, SMT
       DEF SafetyInvariant, CurrentRoundResponsesOnly, CollectEvidence,
           Evidence
<1>12. proposalHistory' = proposalHistory /\
        acceptedHistory' = acceptedHistory /\ chosenHistory' = chosenHistory /\
        configHistory' = configHistory /\
        absProposalHistory' = absProposalHistory /\
        absAcceptedHistory' = absAcceptedHistory /\
        absChosenHistory' = absChosenHistory /\
        absConfigHistory' = absConfigHistory
    BY DEF CollectEvidence
<1>13. ProposalHistorySound'
    BY <1>12, SMT
       DEF SafetyInvariant, ProposalHistorySound, ProposalShapeSound,
           ProposalEvidenceArchived, ProposalBallotUnique,
           ProposalChosenSafe, ProposalWellFormed, ArchivedEvidenceFor,
           CollectEvidence
<1>14. AcceptedHistorySound'
    PROOF
    <2>1. \A a \in acceptedHistory' :
             /\ a.config = PinnedConfig[a.instance]
             /\ \E p \in proposalHistory' :
                   /\ p.instance = a.instance /\ p.ballot = a.ballot
                   /\ p.value = a.value /\ p.config = a.config
        BY <1>12, SMT DEF SafetyInvariant, AcceptedHistorySound
    <2>2. \A a1, a2 \in acceptedHistory :
             /\ a1.replica = a2.replica /\ a1.instance = a2.instance
             /\ a1.ballot = a2.ballot => a1.value = a2.value
        BY SMT DEF SafetyInvariant, AcceptedHistorySound
    <2>3. \A a1, a2 \in acceptedHistory' :
             /\ a1.replica = a2.replica /\ a1.instance = a2.instance
             /\ a1.ballot = a2.ballot => a1.value = a2.value
        BY <1>12, <2>2, SMT
    <2>4. \A rr \in Replicas, ii \in Instances :
             acceptedBallot'[rr][ii] <= promise'[rr][ii]
        PROOF
        <3>1. TAKE rr \in Replicas, ii \in Instances
        <3>2. acceptedBallot'[rr][ii] = acceptedBallot[rr][ii] /\
               acceptedBallot[rr][ii] <= promise[rr][ii]
            BY SMT
               DEF SafetyInvariant, AcceptedHistorySound, CollectEvidence
        <3>3. b >= promise[r][i] /\ promise'[r][i] = b
            BY CollectPromiseAt, SMT DEF CollectEvidence, SafetyInvariant
        <3>4. rr = r /\ ii = i =>
               acceptedBallot'[rr][ii] <= promise'[rr][ii]
            BY <3>2, <3>3, SMT
        <3>5. ~(rr = r /\ ii = i) =>
               acceptedBallot'[rr][ii] <= promise'[rr][ii]
            BY <3>2, CollectPromiseOff, SMT
        <3> QED BY <3>4, <3>5, SMT
    <2>5. \A rr \in Replicas, ii \in Instances :
             acceptedValue'[rr][ii] # NoValue =>
               /\ Acceptance(rr, ii, acceptedBallot'[rr][ii],
                             acceptedValue'[rr][ii], PinnedConfig[ii])
                    \in acceptedHistory'
               /\ \A a \in SenderHistory(rr, ii)' :
                     a.ballot <= acceptedBallot'[rr][ii]
        BY <1>12, SMT
           DEF SafetyInvariant, AcceptedHistorySound, CollectEvidence,
               SenderHistory
    <2> QED BY <2>1, <2>3, <2>4, <2>5 DEF AcceptedHistorySound
<1>15. ChosenCertificateSound'
    BY <1>12, SMT
       DEF SafetyInvariant, ChosenCertificateSound, ProposalForChoice
<1>16. Nontriviality' /\ MappingInvariant'
    BY <1>12, SMT
       DEF SafetyInvariant, Nontriviality, ValueProvenance, MappingInvariant
<1>17. ChosenAgreement' /\ ConfigHistorySafety'
    BY <1>12, DecisionConfigLeibniz, SMT DEF SafetyInvariant
<1> QED BY <1>1, <1>10, <1>11, <1>13, <1>14, <1>15, <1>16, <1>17
    DEF SafetyInvariant

LEMMA QuorumIntersection ==
    ASSUME NEW c, NEW q1, NEW q2, DomainAssumptions,
           c \in Configurations, q1 \in Quorums[c], q2 \in Quorums[c]
    PROVE q1 \cap q2 # {}
BY SMT DEF DomainAssumptions, QuorumAssumptions

LEMMA RecoverySelectionPreservesChosenValue ==
    ASSUME NEW es, NEW k, NEW i, NEW b, NEW c, NEW q, NEW v, NEW old,
           DomainAssumptions, TypeOK, ProposalHistorySound,
           AcceptedHistorySound, EvidenceHistorySound,
           ChosenCertificateSound, old \in chosenHistory,
           old.instance = i, old.ballot < b, c = PinnedConfig[i],
           q \in Quorums[c], RecoverySelection(es, k, i, b, c, q, v)
    PROVE v = old.value
PROOF
<1>1. old.config = c /\ old.quorum \in Quorums[c]
    BY SMT DEF ChosenCertificateSound
<1>2. c \in Configurations /\ q \cap old.quorum # {}
    BY <1>1, QuorumIntersection, SMT
       DEF TypeOK, HistoryTypeOK, ChoiceRecords
<1>3. \E r \in q \cap old.quorum : TRUE BY <1>2, SMT
<1>4. PICK r \in q \cap old.quorum : TRUE BY <1>3
<1>5. \E e \in es : e.sender = r
    BY SMT DEF RecoverySelection, EvidenceFor
<1>6. PICK e \in es : e.sender = r BY <1>5
<1>7. EvidenceFor(es, k, i, b, c, q) BY DEF RecoverySelection
<1>8. e \in countedEvidence /\ e.sender = r /\ e.instance = i /\
       e.ballot = b
    BY <1>6, <1>7, SMT DEF EvidenceFor
<1>9. e \in evidenceHistory
    BY <1>8, SMT DEF EvidenceHistorySound
<1>10. Acceptance(r, old.instance, old.ballot, old.value, old.config)
           \in acceptedHistory
    BY <1>1, SMT DEF ChosenCertificateSound
<1>11. Acceptance(r, old.instance, old.ballot, old.value, old.config)
           \in SenderHistory(r, i)
    BY <1>10, SMT DEF SenderHistory, Acceptance
<1>12. Acceptance(r, old.instance, old.ballot, old.value, old.config)
           \in e.history
    BY <1>8, <1>9, <1>11, SMT
       DEF EvidenceHistorySound, EvidenceArchiveSound, EvidenceWellFormed,
           Acceptance
<1>13. Acceptance(r, old.instance, old.ballot, old.value, old.config)
           \in CertificateHistory(es)
    BY <1>6, <1>12, SMT DEF CertificateHistory
<1>14. CertificateHistory(es) # {} BY <1>13, SMT
<1>15. \E top \in CertificateHistory(es) :
          /\ top.value = v
          /\ \A a \in CertificateHistory(es) : a.ballot <= top.ballot
          /\ \A a \in CertificateHistory(es) :
                a.ballot = top.ballot => a.value = v
    BY <1>14, SMT DEF RecoverySelection, HighestSelected
<1>16. PICK top \in CertificateHistory(es) :
          /\ top.value = v
          /\ \A a \in CertificateHistory(es) : a.ballot <= top.ballot
          /\ \A a \in CertificateHistory(es) :
                a.ballot = top.ballot => a.value = v
    BY <1>15
<1>17. top \in acceptedHistory /\ top.instance = i
    BY <1>6, SMT DEF RecoverySelection, EvidenceFor, CertificateHistory,
                       EvidenceHistorySound, EvidenceArchiveSound,
                       EvidenceWellFormed
<1>18. \E p \in proposalHistory :
          /\ p.instance = top.instance /\ p.ballot = top.ballot
          /\ p.value = top.value /\ p.config = top.config
    BY <1>17, SMT DEF AcceptedHistorySound
<1>19. PICK p \in proposalHistory :
          /\ p.instance = top.instance /\ p.ballot = top.ballot
          /\ p.value = top.value /\ p.config = top.config
    BY <1>18
<1>20. old.ballot \in Nat /\ top.ballot \in Nat
    BY <1>17, SMT DEF DomainAssumptions, CarrierAssumptions, TypeOK,
                        HistoryTypeOK, AcceptanceRecords, ChoiceRecords
<1>21. old.ballot = top.ballot \/ old.ballot < top.ballot
    BY <1>13, <1>16, <1>20, SMT DEF Acceptance
<1>22. old.ballot = top.ballot => v = old.value
    BY <1>13, <1>16, SMT DEF Acceptance
<1>23. old.ballot < top.ballot => v = old.value
    BY <1>16, <1>17, <1>19, SMT
       DEF ProposalHistorySound, ProposalChosenSafe, ProposalSafePair
<1> QED BY <1>21, <1>22, <1>23, SMT

LEMMA RecoveryProposeTypePreserves ==
    ASSUME NEW k, NEW i, NEW b, NEW v, NEW q, NEW es,
           DomainAssumptions, TypeOK,
           RecoveryPropose(k, i, b, v, q, es)
    PROVE TypeOK'
PROOF
<1>1. AcceptorTypeOK' /\ ControlTypeOK'
    BY SMT DEF TypeOK, RecoveryPropose, AcceptorTypeOK, ControlTypeOK
<1>2. PinnedConfig[i] \in Configurations /\
       q \in Quorums[PinnedConfig[i]]
    BY SMT DEF DomainAssumptions, CarrierAssumptions, RecoveryPropose
<1>3. q \subseteq Replicas
    BY <1>2, SMT DEF DomainAssumptions, QuorumAssumptions
<1>4. es \subseteq countedEvidence
    BY DEF RecoveryPropose, RecoverySelection, EvidenceFor
<1>5. countedEvidence \subseteq EvidenceRecords
    BY DEF TypeOK, HistoryTypeOK
<1>6. es \subseteq EvidenceRecords BY <1>4, <1>5, SMT
<1>7. Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es)
       \in ProposalRecords
    BY <1>3, <1>6, SMT
       DEF DomainAssumptions, CarrierAssumptions, TypeOK,
           RecoveryPropose, Proposal, ProposalRecords
<1>8. HistoryTypeOK'
    BY <1>7, SMT DEF TypeOK, HistoryTypeOK, RecoveryPropose
<1>9. AbstractTypeOK'
    BY <1>7, SMT DEF TypeOK, AbstractTypeOK, RecoveryPropose
<1> QED BY <1>1, <1>8, <1>9 DEF TypeOK

LEMMA RecoveryValueProvenance ==
    ASSUME NEW es, NEW k, NEW i, NEW b, NEW c, NEW q, NEW v,
           SafetyInvariant, RecoverySelection(es, k, i, b, c, q, v)
    PROVE v = Noop \/ v \in ClientProposed[i]
PROOF
<1>1. CertificateHistory(es) = {} => v = Noop
    BY SMT DEF RecoverySelection, HighestSelected
<1>2. CertificateHistory(es) # {} =>
       \E top \in CertificateHistory(es) : top.value = v
    BY SMT DEF RecoverySelection, HighestSelected
<1>3. \A top \in CertificateHistory(es) :
       \E e \in es : top \in e.history
    BY SMT DEF CertificateHistory
<1>4. \A e \in es : \A a \in e.history :
       a \in acceptedHistory /\ a.instance = i
    BY SMT
       DEF SafetyInvariant, RecoverySelection, EvidenceFor,
           EvidenceHistorySound, EvidenceArchiveSound, EvidenceWellFormed
<1>5. CertificateHistory(es) # {} =>
       \E top \in CertificateHistory(es) :
           /\ top.value = v /\ top \in acceptedHistory
           /\ top.instance = i
    BY <1>2, <1>3, <1>4, SMT
<1>6. \A a \in acceptedHistory :
       a.value = Noop \/ a.value \in ClientProposed[a.instance]
    BY DEF SafetyInvariant, Nontriviality, ValueProvenance
<1>7. CertificateHistory(es) # {} =>
       v = Noop \/ v \in ClientProposed[i]
    BY <1>5, <1>6, SMT
<1> QED BY <1>1, <1>7, SMT

LEMMA RecoveryProposePreserves ==
    ASSUME NEW k, NEW i, NEW b, NEW v, NEW q, NEW es,
           DomainAssumptions, SafetyInvariant,
           RecoveryPropose(k, i, b, v, q, es)
    PROVE SafetyInvariant'
PROOF
<1>1. TypeOK'
    BY RecoveryProposeTypePreserves DEF SafetyInvariant
<1>2. ProposalWellFormed(
          Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es))'
    BY SMT
       DEF SafetyInvariant, RecoveryPropose, RecoverySelection, EvidenceFor,
           ProposalWellFormed, ArchivedEvidenceFor, Proposal
<1>3. \A p \in proposalHistory' : ProposalWellFormed(p)'
    PROOF
    <2>1. TAKE p \in proposalHistory'
    <2>2. p \in proposalHistory \/
           p = Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es)
        BY SMT DEF RecoveryPropose, Proposal
    <2>3. p \in proposalHistory => ProposalWellFormed(p)'
        BY SMT
           DEF SafetyInvariant, ProposalHistorySound, ProposalShapeSound,
               ProposalWellFormed, ArchivedEvidenceFor, RecoveryPropose
    <2>4. p = Proposal(i, b, v, PinnedConfig[i], k,
                       "recovery", q, es) => ProposalWellFormed(p)'
        BY <1>2, SMT
    <2> QED BY <2>2, <2>3, <2>4, SMT
<1>4. ProposalShapeSound'
    BY <1>3 DEF ProposalShapeSound
<1>5. ProposalBallotUnique'
    BY SMT DEF SafetyInvariant, ProposalHistorySound, ProposalBallotUnique,
               RecoveryPropose, Proposal
<1>6. \A ch \in chosenHistory :
          ProposalSafePair(
            Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es), ch)
    PROOF
    <2>1. TAKE ch \in chosenHistory
    <2>2. i = ch.instance /\ ch.ballot < b => v = ch.value
        BY RecoverySelectionPreservesChosenValue, SMT
           DEF SafetyInvariant, RecoveryPropose
    <2> QED BY <2>2 DEF ProposalSafePair, Proposal
<1>7. \A p \in proposalHistory', ch \in chosenHistory' :
          ProposalSafePair(p, ch)
    PROOF
    <2>1. TAKE p \in proposalHistory', ch \in chosenHistory'
    <2>2. ch \in chosenHistory BY SMT DEF RecoveryPropose
    <2>3. p \in proposalHistory \/
           p = Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es)
        BY SMT DEF RecoveryPropose, Proposal
    <2>4. p \in proposalHistory => ProposalSafePair(p, ch)
        BY <2>2, SMT DEF SafetyInvariant, ProposalHistorySound,
                            ProposalChosenSafe
    <2>5. p = Proposal(i, b, v, PinnedConfig[i], k,
                       "recovery", q, es) => ProposalSafePair(p, ch)
        BY <1>6, <2>2, SMT
    <2> QED BY <2>3, <2>4, <2>5, SMT
<1>8. ProposalChosenSafe' BY <1>7 DEF ProposalChosenSafe
<1>9. \A p \in proposalHistory' :
       p.kind = "recovery" => p.evidence \subseteq evidenceHistory'
    PROOF
    <2>1. TAKE p \in proposalHistory'
    <2>2. p \in proposalHistory \/
           p = Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es)
        BY SMT DEF RecoveryPropose, Proposal
    <2>3. p \in proposalHistory =>
           (p.kind = "recovery" => p.evidence \subseteq evidenceHistory')
        BY SMT
           DEF SafetyInvariant, ProposalHistorySound,
               ProposalEvidenceArchived, RecoveryPropose
    <2>4. p = Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es) =>
           (p.kind = "recovery" => p.evidence \subseteq evidenceHistory')
        BY SMT
           DEF SafetyInvariant, EvidenceHistorySound, RecoveryPropose,
               RecoverySelection, EvidenceFor, Proposal
    <2> QED BY <2>2, <2>3, <2>4, SMT
<1>10. ProposalEvidenceArchived' BY <1>9 DEF ProposalEvidenceArchived
<1>11. ProposalHistorySound'
    BY <1>4, <1>5, <1>8, <1>10 DEF ProposalHistorySound
<1>12. AcceptedHistorySound'
    BY SMT DEF SafetyInvariant, AcceptedHistorySound, RecoveryPropose,
               SenderHistory
<1>13. EvidenceHistorySound' /\ CurrentRoundResponsesOnly'
    BY SMT
       DEF SafetyInvariant, EvidenceHistorySound, EvidenceArchiveSound,
           EvidenceWellFormed, CountedEvidenceCurrent,
           CountedEvidenceUnique, CurrentRoundResponsesOnly,
           RecoveryPropose, SenderHistory
<1>14. ChosenCertificateSound'
    BY SMT DEF SafetyInvariant, ChosenCertificateSound, RecoveryPropose,
               ProposalForChoice
<1>15. chosenHistory' = chosenHistory /\ configHistory' = configHistory
    BY DEF RecoveryPropose
<1>16. ChosenAgreement' /\ ConfigHistorySafety'
    BY <1>15, DecisionConfigLeibniz, SMT DEF SafetyInvariant
<1>17. Nontriviality'
    BY RecoveryValueProvenance, SMT
       DEF SafetyInvariant, Nontriviality, ValueProvenance,
           RecoveryPropose, Proposal
<1>18. MappingInvariant'
    BY SMT DEF SafetyInvariant, MappingInvariant, RecoveryPropose, Proposal
<1> QED BY <1>1, <1>11, <1>12, <1>13, <1>14, <1>16, <1>17, <1>18
    DEF SafetyInvariant

LEMMA AcceptProposalPreserves ==
    ASSUME NEW r, NEW p, DomainAssumptions, SafetyInvariant,
           AcceptProposal(r, p)
    PROVE SafetyInvariant'
PROOF
<1>1. r \in Replicas /\ p \in ProposalRecords /\
       p.instance \in Instances /\ p.ballot \in Ballots /\
       p.value \in Values
    BY SMT DEF SafetyInvariant, TypeOK, HistoryTypeOK, ProposalRecords,
               AcceptProposal
<1>2. promise' \in [Replicas -> [Instances -> Ballots]]
    BY <1>1, Update2Type, SMT
       DEF SafetyInvariant, TypeOK, AcceptorTypeOK, AcceptProposal
<1>3. acceptedBallot' \in [Replicas -> [Instances -> Ballots]]
    BY <1>1, Update2Type, SMT
       DEF SafetyInvariant, TypeOK, AcceptorTypeOK, AcceptProposal
<1>4. acceptedValue' \in
       [Replicas -> [Instances -> Values \cup {NoValue}]]
    BY <1>1, Update2Type, SMT
       DEF SafetyInvariant, TypeOK, AcceptorTypeOK, AcceptProposal
<1>5. AcceptorTypeOK' BY <1>2, <1>3, <1>4 DEF AcceptorTypeOK
<1>6. Acceptance(r, p.instance, p.ballot, p.value, p.config)
       \in AcceptanceRecords
    BY <1>1, SMT
       DEF DomainAssumptions, CarrierAssumptions, AcceptProposal, Acceptance,
           AcceptanceRecords, ProposalRecords
<1>7. HistoryTypeOK'
    BY <1>6, SMT DEF SafetyInvariant, TypeOK, HistoryTypeOK, AcceptProposal,
                         Acceptance
<1>8. ControlTypeOK'
    BY SMT DEF SafetyInvariant, TypeOK, ControlTypeOK, AcceptProposal
<1>9. AbstractTypeOK'
    BY <1>6, SMT
       DEF SafetyInvariant, TypeOK, AbstractTypeOK, AcceptProposal, Acceptance
<1>10. TypeOK' BY <1>5, <1>7, <1>8, <1>9 DEF TypeOK
<1>11. AcceptedHistorySound'
    PROOF
    <2>1. \A a \in acceptedHistory' :
             /\ a.config = PinnedConfig[a.instance]
             /\ \E prop \in proposalHistory' :
                   /\ prop.instance = a.instance /\ prop.ballot = a.ballot
                   /\ prop.value = a.value /\ prop.config = a.config
        PROOF
        <3>1. TAKE a \in acceptedHistory'
        <3>2. a \in acceptedHistory \/
               a = Acceptance(r, p.instance, p.ballot, p.value, p.config)
            BY SMT DEF AcceptProposal, Acceptance
        <3>3. a \in acceptedHistory =>
               /\ a.config = PinnedConfig[a.instance]
               /\ \E prop \in proposalHistory' :
                     /\ prop.instance = a.instance /\ prop.ballot = a.ballot
                     /\ prop.value = a.value /\ prop.config = a.config
            BY SMT
               DEF SafetyInvariant, AcceptedHistorySound, AcceptProposal
        <3>4. a = Acceptance(r, p.instance, p.ballot, p.value, p.config) =>
               /\ a.config = PinnedConfig[a.instance]
               /\ \E prop \in proposalHistory' :
                     /\ prop.instance = a.instance /\ prop.ballot = a.ballot
                     /\ prop.value = a.value /\ prop.config = a.config
            BY SMT
               DEF SafetyInvariant, ProposalHistorySound,
                   ProposalShapeSound, ProposalWellFormed, AcceptProposal,
                   Acceptance
        <3> QED BY <3>2, <3>3, <3>4, SMT
    <2>2. \A a1, a2 \in acceptedHistory' :
             /\ a1.replica = a2.replica /\ a1.instance = a2.instance
             /\ a1.ballot = a2.ballot => a1.value = a2.value
        PROOF
        <3>1. TAKE a1, a2 \in acceptedHistory'
        <3>2. a1 \in acceptedHistory \/
               a1 = Acceptance(r, p.instance, p.ballot, p.value, p.config)
            BY SMT DEF AcceptProposal, Acceptance
        <3>3. a2 \in acceptedHistory \/
               a2 = Acceptance(r, p.instance, p.ballot, p.value, p.config)
            BY SMT DEF AcceptProposal, Acceptance
        <3>4. a1 \in acceptedHistory /\ a2 \in acceptedHistory =>
               (/\ a1.replica = a2.replica /\ a1.instance = a2.instance
                   /\ a1.ballot = a2.ballot => a1.value = a2.value)
            BY SMT DEF SafetyInvariant, AcceptedHistorySound
        <3>5. a1 \in acceptedHistory /\
               a2 = Acceptance(r, p.instance, p.ballot, p.value, p.config) =>
               (/\ a1.replica = a2.replica /\ a1.instance = a2.instance
                   /\ a1.ballot = a2.ballot => a1.value = a2.value)
            BY SMT DEF AcceptProposal, Acceptance
        <3>6. a2 \in acceptedHistory /\
               a1 = Acceptance(r, p.instance, p.ballot, p.value, p.config) =>
               (/\ a1.replica = a2.replica /\ a1.instance = a2.instance
                   /\ a1.ballot = a2.ballot => a1.value = a2.value)
            BY SMT DEF AcceptProposal, Acceptance
        <3> QED BY <3>2, <3>3, <3>4, <3>5, <3>6, SMT
    <2>3. \A rr \in Replicas, ii \in Instances :
             acceptedBallot'[rr][ii] <= promise'[rr][ii]
        PROOF
        <3>1. TAKE rr \in Replicas, ii \in Instances
        <3>2. acceptedBallot[rr][ii] <= promise[rr][ii]
            BY DEF SafetyInvariant, AcceptedHistorySound
        <3>3. rr = r /\ ii = p.instance =>
               acceptedBallot'[rr][ii] <= promise'[rr][ii]
            BY AcceptUpdatesAt, <1>1, SMT
        <3>4. ~(rr = r /\ ii = p.instance) =>
               acceptedBallot'[rr][ii] <= promise'[rr][ii]
            BY AcceptUpdatesOff, <1>1, <3>2, SMT
        <3> QED BY <3>3, <3>4, SMT
    <2>4. \A rr \in Replicas, ii \in Instances :
             acceptedValue'[rr][ii] # NoValue =>
               /\ Acceptance(rr, ii, acceptedBallot'[rr][ii],
                             acceptedValue'[rr][ii], PinnedConfig[ii])
                    \in acceptedHistory'
               /\ \A a \in SenderHistory(rr, ii)' :
                     a.ballot <= acceptedBallot'[rr][ii]
        PROOF
        <3>1. TAKE rr \in Replicas, ii \in Instances
        <3>2. rr = r /\ ii = p.instance =>
               (acceptedValue'[rr][ii] # NoValue =>
                 /\ Acceptance(rr, ii, acceptedBallot'[rr][ii],
                               acceptedValue'[rr][ii], PinnedConfig[ii])
                      \in acceptedHistory'
                 /\ \A a \in SenderHistory(rr, ii)' :
                       a.ballot <= acceptedBallot'[rr][ii])
            BY AcceptUpdatesAt, <1>1, SMT
               DEF SafetyInvariant, AcceptedHistorySound, AcceptProposal,
                   Acceptance, SenderHistory, ProposalHistorySound,
                   ProposalShapeSound, ProposalWellFormed
        <3>3. ~(rr = r /\ ii = p.instance) =>
               (acceptedValue'[rr][ii] # NoValue =>
                 /\ Acceptance(rr, ii, acceptedBallot'[rr][ii],
                               acceptedValue'[rr][ii], PinnedConfig[ii])
                      \in acceptedHistory'
                 /\ \A a \in SenderHistory(rr, ii)' :
                       a.ballot <= acceptedBallot'[rr][ii])
            BY AcceptUpdatesOff, <1>1, SMT
               DEF SafetyInvariant, AcceptedHistorySound, AcceptProposal,
                   Acceptance, SenderHistory
        <3> QED BY <3>2, <3>3, SMT
    <2> QED BY <2>1, <2>2, <2>3, <2>4 DEF AcceptedHistorySound
<1>12. EvidenceArchiveSound'
    PROOF
    <2>1. \A e \in evidenceHistory' : EvidenceWellFormed(e)'
        PROOF
        <3>1. TAKE e \in evidenceHistory'
        <3>2. e \in evidenceHistory /\ EvidenceWellFormed(e)
            BY SMT
               DEF SafetyInvariant, EvidenceHistorySound,
                   EvidenceArchiveSound, AcceptProposal
        <3>3. e.sender \in Replicas /\ e.instance \in Instances
            BY <3>2, SMT
               DEF SafetyInvariant, TypeOK, HistoryTypeOK, EvidenceRecords
        <3>4. e.sender = r /\ e.instance = p.instance =>
                 e.ballot <= promise'[e.sender][e.instance]
            BY AcceptUpdatesAt, <1>1, <3>2, <3>3, SMT
               DEF EvidenceWellFormed, AcceptProposal
        <3>5. ~(e.sender = r /\ e.instance = p.instance) =>
                 e.ballot <= promise'[e.sender][e.instance]
            BY AcceptUpdatesOff, <1>1, <3>2, <3>3, SMT
               DEF EvidenceWellFormed
        <3>6. e.ballot <= promise'[e.sender][e.instance]
            BY <3>4, <3>5, SMT
        <3>7. \A a \in SenderHistory(e.sender, e.instance)' :
                 a.ballot < e.ballot => a \in e.history
            BY <3>2, <3>3, SMT
               DEF EvidenceWellFormed, AcceptProposal, Acceptance,
                   SenderHistory
        <3> QED BY <3>2, <3>6, <3>7, SMT DEF EvidenceWellFormed,
                                                     AcceptProposal
    <2> QED BY <2>1 DEF EvidenceArchiveSound
<1>13. countedEvidence' \subseteq evidenceHistory' /\
        CountedEvidenceCurrent' /\ CountedEvidenceUnique'
    BY SMT DEF SafetyInvariant, EvidenceHistorySound,
               CountedEvidenceCurrent, CountedEvidenceUnique, AcceptProposal
<1>14. EvidenceHistorySound'
    BY <1>12, <1>13 DEF EvidenceHistorySound
<1>15. CurrentRoundResponsesOnly'
    BY SMT DEF SafetyInvariant, CurrentRoundResponsesOnly,
               CountedEvidenceCurrent, AcceptProposal
<1>16. proposalHistory' = proposalHistory /\
        chosenHistory' = chosenHistory /\ configHistory' = configHistory
    BY DEF AcceptProposal
<1>17. ProposalHistorySound' /\ ChosenCertificateSound'
    BY <1>16, SMT DEF SafetyInvariant, ProposalHistorySound,
                        ProposalShapeSound, ProposalEvidenceArchived,
                        ProposalBallotUnique, ProposalChosenSafe,
                        ChosenCertificateSound, AcceptProposal,
                        ProposalForChoice
<1>18. ChosenAgreement' /\ ConfigHistorySafety'
    BY <1>16, DecisionConfigLeibniz, SMT DEF SafetyInvariant
<1>19. Nontriviality' /\ MappingInvariant'
    BY SMT DEF SafetyInvariant, Nontriviality, ValueProvenance,
               MappingInvariant, AcceptProposal, Acceptance
<1> QED BY <1>10, <1>11, <1>14, <1>15, <1>17, <1>18, <1>19
    DEF SafetyInvariant

LEMMA CertifyChosenPreserves ==
    ASSUME NEW p, NEW q
    PROVE DomainAssumptions /\ SafetyInvariant /\ CertifyChosen(p, q)
          => SafetyInvariant'
BY SMT DEF DomainAssumptions, SafetyInvariant, TypeOK, ProposalHistorySound,
           ProposalShapeSound, ProposalEvidenceArchived,
           ProposalBallotUnique, ProposalChosenSafe,
           AcceptedHistorySound, EvidenceHistorySound,
           CurrentRoundResponsesOnly, ChosenCertificateSound,
           ChosenAgreement, ConfigHistorySafety, Nontriviality,
           ValueProvenance, MappingInvariant, CertifyChosen, Choice,
           ChoiceRecords, ProposalForChoice

LEMMA RecordConfigurationPreserves ==
    ASSUME NEW c, NEW outcome, NEW winner
    PROVE DomainAssumptions /\ SafetyInvariant /\
          RecordConfiguration(c, outcome, winner) => SafetyInvariant'
BY SMT DEF DomainAssumptions, SafetyInvariant, TypeOK, ProposalHistorySound,
           AcceptedHistorySound, EvidenceHistorySound,
           CurrentRoundResponsesOnly, ChosenCertificateSound,
           ChosenAgreement, ConfigHistorySafety, ConfigHistoryWellFormed,
           SingleAppliedOutcome, EffectiveBacked, Nontriviality,
           ValueProvenance, MappingInvariant, RecordConfiguration,
           ConfigExecutionGuard, ConfChangeResult, ChosenConfigIn,
           HasConfigResultIn, PriorAppliedIn, ExecutionPrecedes,
           SiblingConfigs

LEMMA RetryStepsPreserve ==
    DomainAssumptions /\ SafetyInvariant /\
    (StartRetry \/ IssueRetry \/ DropRequest \/ DeliverRequest \/
     CompleteRecovery \/ InternalWireStep) => SafetyInvariant'
BY SMT DEF SafetyInvariant, TypeOK, ProposalHistorySound,
           AcceptedHistorySound, EvidenceHistorySound,
           CurrentRoundResponsesOnly, ChosenCertificateSound,
           ChosenAgreement, ConfigHistorySafety, ConfigHistoryWellFormed,
           SingleAppliedOutcome, EffectiveBacked, Nontriviality,
           ValueProvenance, MappingInvariant, StartRetry, IssueRetry,
           DropRequest, DeliverRequest, CompleteRecovery, InternalWireStep

LEMMA ConcreteStepPreserves ==
    DomainAssumptions /\ SafetyInvariant /\ ConcreteNext => SafetyInvariant'
BY NormalProposePreserves, BeginRecoveryPreserves, CollectEvidencePreserves,
   RecoveryProposePreserves, AcceptProposalPreserves,
   CertifyChosenPreserves, RecordConfigurationPreserves, RetryStepsPreserve,
   SMT DEF ConcreteNext

LEMMA StutterPreserves == SafetyInvariant /\ Stutter => SafetyInvariant'
BY SMT DEF Stutter

LEMMA NextPreservesInvariant ==
    DomainAssumptions /\ SafetyInvariant /\ NextOrStutter => SafetyInvariant'
BY ConcreteStepPreserves, StutterPreserves, SMT DEF NextOrStutter

FullInvariant == DomainAssumptions /\ SafetyInvariant

LEMMA FullInvariantInit == DomainAssumptions /\ Init => FullInvariant
BY InitEstablishesInvariant DEF FullInvariant

LEMMA FullInvariantStep ==
    FullInvariant /\ NextOrStutter => FullInvariant'
BY NextPreservesInvariant, SMT DEF FullInvariant, DomainAssumptions

LEMMA SafetyInvariantInductive == Spec => []FullInvariant
PROOF
<1>1. Spec => FullInvariant BY FullInvariantInit DEF Spec
<1>2. FullInvariant /\ NextOrStutter => FullInvariant'
    BY FullInvariantStep
<1> QED BY <1>1, <1>2, PTL DEF Spec

THEOREM ChosenAgreementInductive == Spec => []ChosenAgreement
PROOF
<1>1. Spec => []FullInvariant BY SafetyInvariantInductive
<1>2. FullInvariant => ChosenAgreement BY DEF FullInvariant, SafetyInvariant
<1> QED BY <1>1, <1>2, PTL

RecoverySafety ==
    /\ ChosenAgreement /\ ProposalHistorySound
    /\ EvidenceHistorySound /\ CurrentRoundResponsesOnly

THEOREM RecoverySafetyInductive == Spec => []RecoverySafety
PROOF
<1>1. Spec => []FullInvariant BY SafetyInvariantInductive
<1>2. FullInvariant => RecoverySafety
    BY DEF FullInvariant, SafetyInvariant, RecoverySafety
<1> QED BY <1>1, <1>2, PTL

THEOREM ConfigHistorySafetyInductive == Spec => []ConfigHistorySafety
PROOF
<1>1. Spec => []FullInvariant BY SafetyInvariantInductive
<1>2. FullInvariant => ConfigHistorySafety
    BY DEF FullInvariant, SafetyInvariant
<1> QED BY <1>1, <1>2, PTL

(***************************************************************************)
(* Fair-loss liveness.  These are exactly the three named hypotheses:       *)
(* eventual-delivery-under-fair-loss, infinite-retry, weak-fair-scheduling. *)
(* No partition liveness or bounded completion time is claimed.             *)
(***************************************************************************)

EventualDeliveryUnderFairLoss == requestInFlight ~> replyAvailable
InfiniteRetry == retryPending ~> requestInFlight
WeakFairScheduling == WF_CompletionVars(FinishRetry)

RetryPreservesProtocolValue ==
    /\ proposalHistory' = proposalHistory
    /\ acceptedHistory' = acceptedHistory
    /\ chosenHistory' = chosenHistory
    /\ configHistory' = configHistory

LEMMA RetrySafetyWithoutLivenessAssumptions ==
    IssueRetry => RetryPreservesProtocolValue
BY SMT DEF IssueRetry, RetryPreservesProtocolValue

WaitingForCompletion == replyAvailable /\ ~recoveryComplete

LEMMA CompletionEnabled ==
    WaitingForCompletion => ENABLED <<FinishRetry>>_CompletionVars
BY DEF WaitingForCompletion, FinishRetry, CompletionVars

LEMMA WaitingPersistsOrCompletes ==
    WaitingForCompletion /\ NextOrStutter
    => WaitingForCompletion' \/ recoveryComplete'
BY SMT DEF WaitingForCompletion, NextOrStutter, ConcreteNext, Stutter,
           NormalPropose, BeginRecovery, CollectEvidence, RecoveryPropose,
           AcceptProposal, CertifyChosen, RecordConfiguration,
           ConfigExecutionGuard, StartRetry, IssueRetry, DropRequest,
           DeliverRequest, CompleteRecovery, InternalWireStep

LEMMA CompletionActionCompletes ==
    WaitingForCompletion /\ NextOrStutter /\
    <<FinishRetry>>_CompletionVars => recoveryComplete'
BY SMT DEF WaitingForCompletion, FinishRetry, CompletionVars

LEMMA WeakFairCompletion ==
    Spec /\ WeakFairScheduling => (replyAvailable ~> recoveryComplete)
PROOF
<1>1. Spec => []NextOrStutter BY DEF Spec
<1>2. WaitingForCompletion /\ NextOrStutter
       => WaitingForCompletion' \/ recoveryComplete'
    BY WaitingPersistsOrCompletes
<1>3. WaitingForCompletion /\ NextOrStutter /\
       <<FinishRetry>>_CompletionVars => recoveryComplete'
    BY CompletionActionCompletes
<1>4. WaitingForCompletion => ENABLED <<FinishRetry>>_CompletionVars
    BY CompletionEnabled
<1>5. []NextOrStutter /\ WF_CompletionVars(FinishRetry)
       => (WaitingForCompletion ~> recoveryComplete)
    BY <1>2, <1>3, <1>4, PTL DEF NextOrStutter
<1>6. replyAvailable => WaitingForCompletion \/ recoveryComplete
    BY DEF WaitingForCompletion
<1> QED BY <1>1, <1>5, <1>6, PTL DEF WeakFairScheduling

THEOREM RetrySafetyUnderFairLoss ==
    /\ IssueRetry => RetryPreservesProtocolValue
    /\ Spec /\ EventualDeliveryUnderFairLoss /\ InfiniteRetry /\
       WeakFairScheduling => (retryPending ~> recoveryComplete)
PROOF
<1>1. IssueRetry => RetryPreservesProtocolValue
    BY RetrySafetyWithoutLivenessAssumptions
<1>2. Spec /\ WeakFairScheduling
       => (replyAvailable ~> recoveryComplete)
    BY WeakFairCompletion
<1>3. EventualDeliveryUnderFairLoss /\ InfiniteRetry
       => (retryPending ~> replyAvailable)
    BY PTL DEF EventualDeliveryUnderFairLoss, InfiniteRetry
<1> QED BY <1>1, <1>2, <1>3, PTL

(***************************************************************************)
(* Refinement and explicit negative mutations.                              *)
(***************************************************************************)

LEMMA NormalActionRefines ==
    ASSUME DomainAssumptions,
           \E i \in Instances, v \in Values, k \in Coordinators :
               NormalPropose(i, v, k)
    PROVE AbstractNext
PROOF
<1>1. PICK i \in Instances, v \in Values, k \in Coordinators :
          NormalPropose(i, v, k) BY SMT
<1>2. NormalPropose(i, v, k) BY <1>1
<1>3. Proposal(i, BottomBallot, v, PinnedConfig[i], k,
               "normal", {}, {}) \in ProposalRecords
    BY <1>2, SMT DEF DomainAssumptions, CarrierAssumptions,
                       OwnershipAssumptions, ClientAssumptions,
                       NormalPropose, Proposal, ProposalRecords
<1>4. AbsPropose(Proposal(i, BottomBallot, v, PinnedConfig[i], k,
                          "normal", {}, {}))
    BY <1>2, <1>3 DEF AbsPropose, NormalPropose, Proposal
<1>5. \E proposal \in ProposalRecords : AbsPropose(proposal)
    BY <1>3, <1>4, SMT
<1> QED BY <1>5 DEF AbstractNext

LEMMA RecoveryActionRefines ==
    ASSUME DomainAssumptions,
           \E k \in Coordinators, i \in Instances, b \in Ballots,
               v \in Values :
             \E q \in Quorums[PinnedConfig[i]] :
               \E es \in SUBSET EvidenceRecords :
                 RecoveryPropose(k, i, b, v, q, es)
    PROVE AbstractNext
PROOF
<1>1. PICK k \in Coordinators, i \in Instances, b \in Ballots,
             v \in Values :
          \E q \in Quorums[PinnedConfig[i]] :
            \E es \in SUBSET EvidenceRecords :
              RecoveryPropose(k, i, b, v, q, es) BY SMT
<1>2. PICK q \in Quorums[PinnedConfig[i]] :
          \E es \in SUBSET EvidenceRecords :
              RecoveryPropose(k, i, b, v, q, es) BY <1>1
<1>3. PICK es \in SUBSET EvidenceRecords :
          RecoveryPropose(k, i, b, v, q, es) BY <1>2
<1>4. RecoveryPropose(k, i, b, v, q, es) BY <1>3
<1>5. Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es)
          \in ProposalRecords
    BY <1>4, SMT DEF DomainAssumptions, CarrierAssumptions,
                       QuorumAssumptions, RecoveryPropose, Proposal,
                       ProposalRecords
<1>6. AbsPropose(
          Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es))
    BY <1>4, <1>5 DEF AbsPropose, RecoveryPropose, Proposal
<1>7. \E proposal \in ProposalRecords : AbsPropose(proposal)
    BY <1>5, <1>6, SMT
<1> QED BY <1>7 DEF AbstractNext

LEMMA ProposalActionRefines ==
    DomainAssumptions /\ MappingInvariant /\
    (\/ \E i \in Instances, v \in Values, k \in Coordinators :
          NormalPropose(i, v, k)
     \/ \E k \in Coordinators, i \in Instances, b \in Ballots,
          v \in Values :
          \E q \in Quorums[PinnedConfig[i]] :
            \E es \in SUBSET EvidenceRecords :
              RecoveryPropose(k, i, b, v, q, es))
    => AbstractNext
BY NormalActionRefines, RecoveryActionRefines, SMT
LEMMA AcceptanceWitnessRefines ==
    ASSUME DomainAssumptions, SafetyInvariant, MappingInvariant,
           NEW r \in Replicas, NEW p \in proposalHistory,
           AcceptProposal(r, p)
    PROVE AbstractNext
PROOF
<1>1. Acceptance(r, p.instance, p.ballot, p.value, p.config)
          \in AcceptanceRecords
    BY SMT DEF DomainAssumptions, CarrierAssumptions, SafetyInvariant,
               TypeOK, HistoryTypeOK, ProposalRecords, Acceptance,
               AcceptanceRecords
<1>2. p \in absProposalHistory
    BY SMT DEF MappingInvariant
<1>3. /\ absAcceptedHistory' =
              absAcceptedHistory \cup
                {Acceptance(r, p.instance, p.ballot, p.value, p.config)}
       /\ absProposalHistory' = absProposalHistory
       /\ absChosenHistory' = absChosenHistory
       /\ absConfigHistory' = absConfigHistory
    BY SMT DEF AcceptProposal, Acceptance
<1>4. AbsAccept(Acceptance(
          r, p.instance, p.ballot, p.value, p.config))
    BY <1>1, <1>2, <1>3 DEF AbsAccept, Acceptance
<1>5. \E a \in AcceptanceRecords : AbsAccept(a)
    BY <1>1, <1>4, SMT
<1> QED BY <1>5 DEF AbstractNext

LEMMA AcceptanceActionRefines ==
    DomainAssumptions /\ SafetyInvariant /\ MappingInvariant /\
    (\E r \in Replicas, p \in proposalHistory : AcceptProposal(r, p))
    => AbstractNext
BY AcceptanceWitnessRefines, SMT
LEMMA ChoiceWitnessRefines ==
    ASSUME DomainAssumptions, SafetyInvariant, MappingInvariant,
           NEW p \in proposalHistory, NEW q \in Quorums[p.config],
           CertifyChosen(p, q)
    PROVE AbstractNext
PROOF
<1>1. Choice(p.instance, p.ballot, p.value, p.config, q)
          \in ChoiceRecords
    BY SMT DEF DomainAssumptions, CarrierAssumptions, QuorumAssumptions,
               SafetyInvariant, TypeOK, HistoryTypeOK, ProposalRecords,
               Choice, ChoiceRecords
<1>2. \A r \in q :
          Acceptance(r, p.instance, p.ballot, p.value, p.config)
              \in absAcceptedHistory
    BY SMT DEF MappingInvariant, CertifyChosen
<1>3. /\ absChosenHistory' =
              absChosenHistory \cup
                {Choice(p.instance, p.ballot, p.value, p.config, q)}
       /\ absProposalHistory' = absProposalHistory
       /\ absAcceptedHistory' = absAcceptedHistory
       /\ absConfigHistory' = absConfigHistory
    BY SMT DEF CertifyChosen, Choice
<1>4. AbsChoose(Choice(p.instance, p.ballot, p.value, p.config, q))
    BY <1>1, <1>2, <1>3 DEF AbsChoose, Choice
<1>5. \E ch \in ChoiceRecords : AbsChoose(ch)
    BY <1>1, <1>4, SMT
<1> QED BY <1>5 DEF AbstractNext

LEMMA ChoiceActionRefines ==
    DomainAssumptions /\ SafetyInvariant /\ MappingInvariant /\
    (\E p \in proposalHistory :
       \E q \in Quorums[p.config] : CertifyChosen(p, q))
    => AbstractNext
BY ChoiceWitnessRefines, SMT

LEMMA ConfigActionRefines ==
    DomainAssumptions /\ MappingInvariant /\
    (\E c \in Configurations,
        outcome \in {"applied", "rejected-superseded"},
        winner \in Configurations :
        RecordConfiguration(c, outcome, winner))
    => AbstractNext
BY SMT DEF DomainAssumptions, CarrierAssumptions,
           ConfigurationAssumptions, MappingInvariant, AbstractNext,
           AbsConfigure, RecordConfiguration, ConfChangeResult, ConfigRecords

LEMMA BeginActionRefines ==
    ASSUME NEW k, NEW i, NEW b
    PROVE BeginRecovery(k, i, b) => AbstractNext
BY SMT DEF AbstractNext, AbsStutter, BeginRecovery

LEMMA CollectActionRefines ==
    ASSUME NEW k, NEW r, NEW i, NEW b
    PROVE CollectEvidence(k, r, i, b) => AbstractNext
BY SMT DEF AbstractNext, AbsStutter, CollectEvidence

LEMMA AbsStutterIsAbstractNext ==
    AbsStutter => AbstractNext
BY SMT DEF AbstractNext

LEMMA StartRetryStutters ==
    StartRetry => AbsStutter
BY SMT DEF AbsStutter, StartRetry, AbstractVars

LEMMA IssueRetryStutters ==
    IssueRetry => AbsStutter
BY SMT DEF AbsStutter, IssueRetry, AbstractVars

LEMMA DropRequestStutters ==
    DropRequest => AbsStutter
BY SMT DEF AbsStutter, DropRequest, AbstractVars

LEMMA DeliverRequestStutters ==
    DeliverRequest => AbsStutter
BY SMT DEF AbsStutter, DeliverRequest, AbstractVars

LEMMA CompleteRecoveryStutters ==
    CompleteRecovery => AbsStutter
BY SMT DEF AbsStutter, CompleteRecovery

LEMMA WireStepStutters ==
    InternalWireStep => AbsStutter
BY SMT DEF AbsStutter, InternalWireStep, AbstractVars

LEMMA RetryInternalActionsRefine ==
    (StartRetry \/ IssueRetry \/ DropRequest \/ DeliverRequest \/
     CompleteRecovery \/ InternalWireStep) => AbstractNext
BY AbsStutterIsAbstractNext, StartRetryStutters, IssueRetryStutters,
   DropRequestStutters, DeliverRequestStutters, CompleteRecoveryStutters,
   WireStepStutters, SMT

LEMMA InternalActionsRefine ==
    (\/ \E k \in Coordinators, i \in Instances, b \in Ballots :
          BeginRecovery(k, i, b)
     \/ \E k \in Coordinators, r \in Replicas, i \in Instances,
          b \in Ballots : CollectEvidence(k, r, i, b)
     \/ StartRetry \/ IssueRetry \/ DropRequest \/ DeliverRequest
     \/ CompleteRecovery \/ InternalWireStep)
    => AbstractNext
BY BeginActionRefines, CollectActionRefines, RetryInternalActionsRefine,
   SMT

LEMMA ConcreteActionRefines ==
    DomainAssumptions /\ SafetyInvariant /\ MappingInvariant /\
    ConcreteNext => AbstractNext
BY ProposalActionRefines, AcceptanceActionRefines, ChoiceActionRefines,
   ConfigActionRefines, InternalActionsRefine, SMT DEF ConcreteNext

LEMMA StutterRefines == Stutter => AbstractNext
BY SMT DEF Stutter, AbstractNext, AbsStutter

LEMMA RefinementStep ==
    DomainAssumptions /\ SafetyInvariant /\ MappingInvariant /\
    NextOrStutter => AbstractNext
BY ConcreteActionRefines, StutterRefines, SMT DEF NextOrStutter

\* This arbitrary-history abstraction theorem is intentionally not named
\* RawNodeRefinement.  It applies only to this module's restricted ConcreteNext.
\* No checked theorem currently connects EPaxosRawNodeRefinement or arbitrary
\* Go RawNode executions to this abstraction; that bridge remains an explicit
\* proof obligation.
THEOREM AbstractHistoryRefinement ==
    Spec => []MappingInvariant /\ []AbstractNext
PROOF
<1>1. Spec => []FullInvariant BY SafetyInvariantInductive
<1>2. FullInvariant => MappingInvariant BY DEF FullInvariant, SafetyInvariant
<1>3. Spec => []MappingInvariant BY <1>1, <1>2, PTL
<1>4. Spec => []NextOrStutter BY DEF Spec
<1>5. FullInvariant /\ NextOrStutter => AbstractNext
    BY RefinementStep, SMT DEF FullInvariant, SafetyInvariant
<1> QED BY <1>1, <1>3, <1>4, <1>5, PTL

\* Mutation: current-value-only certification omits same-ballot evidence.
FalseCertificate(p, q) ==
    /\ p \in proposalHistory /\ q \in Quorums[p.config]
    /\ \A r \in q : acceptedValue[r][p.instance] = p.value
    /\ ~\A r \in q :
          Acceptance(r, p.instance, p.ballot, p.value, p.config)
              \in acceptedHistory

LEMMA CertificateRequiresSameBallotHistory ==
    ASSUME NEW p, NEW q
    PROVE CertifyChosen(p, q) =>
      \A r \in q :
        Acceptance(r, p.instance, p.ballot, p.value, p.config)
            \in acceptedHistory
BY DEF CertifyChosen

\* Mutation: arbitrary recovery omits RecoverySelection and is not ConcreteNext.
ArbitraryRecoveryMutation(k, i, b, v, q, es) ==
    /\ k \in Coordinators /\ i \in Instances /\ b \in Ballots
    /\ v \in Values /\ q \in Quorums[PinnedConfig[i]]
    /\ proposalHistory' =
          proposalHistory \cup
            {Proposal(i, b, v, PinnedConfig[i], k, "recovery", q, es)}

LEMMA RecoveryProposalRequiresHighestEvidence ==
    ASSUME NEW k, NEW i, NEW b, NEW v, NEW q, NEW es
    PROVE RecoveryPropose(k, i, b, v, q, es) =>
      RecoverySelection(es, k, i, b, PinnedConfig[i], q, v)
BY DEF RecoveryPropose

\* Mutation: a bad concrete-only decision cannot stutter the mapping.
BadConcreteChoice(ch) ==
    /\ ch \in ChoiceRecords /\ ch \notin chosenHistory
    /\ chosenHistory' = chosenHistory \cup {ch}
    /\ absChosenHistory' = absChosenHistory
    /\ UNCHANGED <<proposalHistory, acceptedHistory, configHistory,
                   absProposalHistory, absAcceptedHistory, absConfigHistory>>

LEMMA BadConcreteChoiceBreaksMapping ==
    ASSUME NEW ch
    PROVE MappingInvariant /\ BadConcreteChoice(ch) => ~MappingInvariant'
BY SMT DEF MappingInvariant, BadConcreteChoice

=============================================================================
