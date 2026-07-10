---- MODULE EPaxosPaperSafety ----
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Finite, executable safety model for the EPaxos paper paths.  The four   *)
(* configurations select disjoint action families with Mode; no state      *)
(* constraint removes reachable protocol states.  Choice is derived only  *)
(* from monotonic preaccepted/accepted histories and response-carried      *)
(* evidence.  Passing a finite configuration is not an unbounded proof.    *)
(***************************************************************************)

CONSTANTS Replicas, Instances, UserCommands, Noop, Ballots, Configs,
          Voters, InstanceConfig, Coordinators, Mode

VARIABLES proposed,
          promise,
          durable,
          preacceptedHistory,
          acceptedHistory,
          commitHistory,
          network,
          coordPhase,
          coordBallot,
          coordCandidate,
          preReplies,
          prepareReplies,
          tryReplies,
          acceptReplies,
          raceSeen

Vars == <<proposed, promise, durable, preacceptedHistory, acceptedHistory,
          commitHistory, network, coordPhase, coordBallot, coordCandidate,
          preReplies, prepareReplies, tryReplies, acceptReplies, raceSeen>>

Modes == {"slow-race", "fast-recovery", "accept-evidence", "config-race"}
Statuses == {"none", "preaccepted", "accepted", "committed"}
Phases == {"idle", "preaccept", "prepare", "prepared", "try-preaccept",
           "retry-prepare", "accept", "fast-committed", "committed"}
MessageKinds == {"PreAcceptReq", "PreAcceptResp", "PrepareReq", "PrepareResp",
                 "TryPreAcceptReq", "TryPreAcceptResp", "AcceptReq",
                 "AcceptResp", "EvidenceReq", "EvidenceResp", "Commit"}
ResponseKinds == {"PreAcceptResp", "PrepareResp", "TryPreAcceptResp",
                  "AcceptResp", "EvidenceResp"}
RequestKinds == {"PreAcceptReq", "PrepareReq", "TryPreAcceptReq",
                 "AcceptReq", "EvidenceReq"}
RaceTags == {"two-coordinator-race", "recovery-low-first",
             "recovery-high-first", "stale-response-ignored",
             "duplicate-response-ignored", "duplicate-commit-ignored",
             "stale-request-ignored", "malformed-evidence-fail-closed",
             "try-authorized", "try-fail-closed", "accept-response-seen",
             "prepare-response-with-evidence-seen", "evidence-response-seen",
             "added-current-voter-ignored", "removed-pinned-voter-counted"}

SeqNums == 0..Cardinality(Instances)
Tuple == [cmd : UserCommands \cup {Noop}, seq : SeqNums,
          deps : SUBSET Instances, conf : Configs]
Tuples == Tuple
NoTuple == [cmd |-> Noop, seq |-> Cardinality(Instances) + 1,
            deps |-> {}, conf |-> CHOOSE c \in Configs : TRUE]
NoResponse == [type |-> "NoResponse"]

MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y
MinBallot == MinOf(Ballots)
RecoveryBallots == Ballots \ {MinBallot}
FirstRecoveryBallot == MinOf(RecoveryBallots)
LastRecoveryBallot == MaxOf(RecoveryBallots)
FirstCoordinator == MinOf(Coordinators)
FirstOrDefault(S) == IF S = {} THEN FirstCoordinator ELSE MinOf(S)
LastCoordinator == MaxOf(Coordinators)
Target == MinOf(Instances)
OtherInstances == Instances \ {Target}
OtherInstance ==
    IF OtherInstances = {} THEN Target
    ELSE CHOOSE i \in OtherInstances : TRUE

SlowQuorumSize(c) == (Cardinality(Voters[c]) \div 2) + 1
FastQuorumSize(c) ==
    LET n == Cardinality(Voters[c])
    IN IF n \in {1, 3, 5, 7}
       THEN IF n = 1 THEN 1 ELSE (n \div 2) + (((n \div 2) + 1) \div 2)
       ELSE n - ((n - 1) \div 4)
SlowQuorums(c) == {q \in SUBSET Voters[c] : Cardinality(q) >= SlowQuorumSize(c)}
FastQuorums(c) == {q \in SUBSET Voters[c] : Cardinality(q) >= FastQuorumSize(c)}

Owner(i) == MinOf(Coordinators \cap Voters[InstanceConfig[i]])
CurrentConfig ==
    IF Mode = "config-race"
    THEN CHOOSE c \in Configs : c # InstanceConfig[Target]
    ELSE InstanceConfig[Target]
AddedCurrentVoters == Voters[CurrentConfig] \ Voters[InstanceConfig[Target]]
RemovedPinnedVoters == Voters[InstanceConfig[Target]] \ Voters[CurrentConfig]
LowestMembers(S, n) ==
    {r \in S : Cardinality({s \in S : s <= r}) <= n}
HighestMembers(S, n) ==
    {r \in S : Cardinality({s \in S : s >= r}) <= n}
NormalSlowQuorum(c) == LowestMembers(Voters[c], SlowQuorumSize(c))
RecoverySlowQuorum(c) == HighestMembers(Voters[c], SlowQuorumSize(c))
NormalFastQuorum(c) == LowestMembers(Voters[c], FastQuorumSize(c))

BaseDeps(i) ==
    IF Mode \in {"fast-recovery", "accept-evidence"} /\ i = Target /\ OtherInstances # {}
    THEN {OtherInstance}
    ELSE {}
BaseSeq(i) == IF BaseDeps(i) = {} THEN 1 ELSE 2
UserTuple(i, c) ==
    [cmd |-> c, seq |-> BaseSeq(i), deps |-> BaseDeps(i),
     conf |-> InstanceConfig[i]]
NoopTuple(i) ==
    [cmd |-> Noop, seq |-> 0, deps |-> {}, conf |-> InstanceConfig[i]]
UserTuples(i) == {UserTuple(i, c) : c \in UserCommands}
AlternateTuple(t) ==
    [t EXCEPT !.seq = IF t.seq = 0 THEN 1 ELSE t.seq - 1]

IsTuple(t) == t \in Tuples
IsAcceptEvidence(ev) ==
    /\ ev.sender \in Replicas
    /\ ev.inst \in Instances
    /\ ev.conf \in Configs
    /\ ev.ballot \in Ballots
    /\ IsTuple(ev.tuple)
AcceptEvidence ==
    [sender : Replicas, inst : Instances, conf : Configs,
     ballot : Ballots, tuple : Tuples]

IsDurableRecord(d) ==
    /\ d.status \in Statuses
    /\ d.recordBallot \in Ballots
    /\ d.tuple \in Tuples \cup {NoTuple}
    /\ d.fastEligible \in BOOLEAN
    /\ d.fpDepsCommitted \subseteq Instances
    /\ \A ev \in d.acceptEvidence : IsAcceptEvidence(ev)
    /\ (d.status = "none" => d.tuple = NoTuple)
    /\ (d.status # "none" => d.tuple \in Tuples)
    /\ (d.status # "preaccepted" => ~d.fastEligible)
    /\ (d.tuple \in Tuples => d.tuple.conf \in Configs)

IsPreacceptedFact(h) ==
    /\ h.sender \in Replicas
    /\ h.inst \in Instances
    /\ h.ballot \in Ballots
    /\ IsTuple(h.tuple)
    /\ h.fpDepsCommitted \subseteq Instances
IsAcceptedFact(h) ==
    /\ h.sender \in Replicas
    /\ h.inst \in Instances
    /\ h.ballot \in Ballots
    /\ IsTuple(h.tuple)
IsCommitFact(h) ==
    /\ h.sender \in Replicas
    /\ h.inst \in Instances
    /\ IsTuple(h.tuple)

IsMessage(m) ==
    /\ m.type \in MessageKinds
    /\ m.from \in Replicas
    /\ m.to \in Replicas
    /\ m.inst \in Instances
    /\ m.conf \in Configs
    /\ m.ballot \in Ballots
    /\ m.tuple \in Tuples \cup {NoTuple}
    /\ m.recordStatus \in Statuses
    /\ m.recordBallot \in Ballots
    /\ m.fastEligible \in BOOLEAN
    /\ m.fpDepsCommitted \subseteq Instances
    /\ \A ev \in m.acceptEvidence : IsAcceptEvidence(ev)
IsResponse(m) == IsMessage(m) /\ m.type \in ResponseKinds

Msg(kind, from, to, i, conf, b, t, status, recordB, fast, fp, evs) ==
    [type |-> kind, from |-> from, to |-> to, inst |-> i,
     conf |-> conf, ballot |-> b, tuple |-> t,
     recordStatus |-> status, recordBallot |-> recordB,
     fastEligible |-> fast, fpDepsCommitted |-> fp,
     acceptEvidence |-> evs]

EmptyDurable ==
    [status |-> "none", recordBallot |-> MinBallot, tuple |-> NoTuple,
     fastEligible |-> FALSE, fpDepsCommitted |-> {}, acceptEvidence |-> {}]
EmptyReplyMap ==
    [k \in Coordinators |->
        [i \in Instances |->
            [b \in Ballots |-> [r \in Replicas |-> NoResponse]]]]

DirectEvidence(r, i, b, t) ==
    [sender |-> r, inst |-> i, conf |-> InstanceConfig[i],
     ballot |-> b, tuple |-> t]
PreFact(r, i, b, t, fp) ==
    [sender |-> r, inst |-> i, ballot |-> b, tuple |-> t,
     fpDepsCommitted |-> fp]
AcceptedFact(r, i, b, t) ==
    [sender |-> r, inst |-> i, ballot |-> b, tuple |-> t]
CommitFact(r, i, t) == [sender |-> r, inst |-> i, tuple |-> t]

LocallyCommittedDeps(r, t) ==
    {d \in t.deps : \E u \in Tuples : CommitFact(r, d, u) \in commitHistory}

PreacceptedAt(r, i, b, t) ==
    \E fp \in SUBSET Instances : PreFact(r, i, b, t, fp) \in preacceptedHistory
AcceptedAt(r, i, b, t) == AcceptedFact(r, i, b, t) \in acceptedHistory
CommittedAt(r, i, t) == CommitFact(r, i, t) \in commitHistory

MatchingFastHistory(q, i, t) ==
    \A r \in q : PreacceptedAt(r, i, MinBallot, t)
CarriedFPProofCovers(q, i, t) ==
    \A d \in t.deps :
        \E r \in q, fp \in SUBSET Instances :
            /\ PreFact(r, i, MinBallot, t, fp) \in preacceptedHistory
            /\ d \in fp
FastChosen(i, t) ==
    \E q \in FastQuorums(InstanceConfig[i]) :
        MatchingFastHistory(q, i, t) /\ CarriedFPProofCovers(q, i, t)
SlowChosen(i, t) ==
    \E b \in Ballots, q \in SlowQuorums(InstanceConfig[i]) :
        \A r \in q : AcceptedAt(r, i, b, t)
HistoricalTuples(i) ==
    {h.tuple : h \in {x \in preacceptedHistory : x.inst = i}} \cup
    {h.tuple : h \in {x \in acceptedHistory : x.inst = i}}
ChosenTuples(i) ==
    {t \in HistoricalTuples(i) : FastChosen(i, t) \/ SlowChosen(i, t)}

StoredReplies(replies, k, i, b) ==
    {replies[k][i][b][r] :
        r \in {s \in Replicas : replies[k][i][b][s] # NoResponse}}
ReplySenders(replies, k, i, b) ==
    {r \in Replicas : replies[k][i][b][r] # NoResponse}
MatchingReplySenders(replies, k, i, b, t) ==
    {r \in Voters[InstanceConfig[i]] :
        /\ replies[k][i][b][r] # NoResponse
        /\ replies[k][i][b][r].tuple = t}

EvidenceHistorySound(ev) ==
    /\ ev.conf = InstanceConfig[ev.inst]
    /\ AcceptedFact(ev.sender, ev.inst, ev.ballot, ev.tuple) \in acceptedHistory
PayloadEvidenceSound(m) ==
    /\ \A d \in m.fpDepsCommitted :
          \E t \in Tuples : CommittedAt(m.from, d, t)
    /\ \A ev \in m.acceptEvidence : EvidenceHistorySound(ev)
ResponseScoped(m, k, i, b) ==
    /\ IsResponse(m)
    /\ m.to = k
    /\ m.inst = i
    /\ m.conf = InstanceConfig[i]
    /\ m.ballot = b
    /\ m.from \in Voters[InstanceConfig[i]]

CommittedReplyCandidates(k, i, b) ==
    {m.tuple :
        m \in {rsp \in StoredReplies(prepareReplies, k, i, b) :
                rsp.recordStatus = "committed" /\ rsp.tuple \in Tuples}}
AllPrepareEvidence(k, i, b) ==
    UNION {m.acceptEvidence : m \in StoredReplies(prepareReplies, k, i, b)}
AcceptedReplyEvidence(k, i, b) ==
    {ev \in AllPrepareEvidence(k, i, b) : ev.inst = i}
HighestAcceptedBallot(k, i, b) ==
    MaxOf({ev.ballot : ev \in AcceptedReplyEvidence(k, i, b)})
HighestAcceptedCandidates(k, i, b) ==
    {ev.tuple : ev \in {e \in AcceptedReplyEvidence(k, i, b) :
                         e.ballot = HighestAcceptedBallot(k, i, b)}}
TryWitnessQuorumSize(c) ==
    FastQuorumSize(c) + SlowQuorumSize(c) - Cardinality(Voters[c])
PreacceptedCandidateSenders(k, i, b, t) ==
    {m.from :
        m \in {rsp \in StoredReplies(prepareReplies, k, i, b) :
                /\ rsp.recordStatus = "preaccepted"
                /\ rsp.fastEligible
                /\ rsp.tuple = t
                /\ \A d \in t.deps : d \in rsp.fpDepsCommitted}}
PrepareReplyTuples(k, i, b) ==
    {m.tuple : m \in {rsp \in StoredReplies(prepareReplies, k, i, b) :
                       rsp.tuple \in Tuples}}
PreacceptedReplyCandidates(k, i, b) ==
    {t \in PrepareReplyTuples(k, i, b) :
        Cardinality(PreacceptedCandidateSenders(k, i, b, t)) >=
            TryWitnessQuorumSize(InstanceConfig[i])}
PreparedCandidate(k, i, b) ==
    IF CommittedReplyCandidates(k, i, b) # {}
    THEN CHOOSE t \in CommittedReplyCandidates(k, i, b) : TRUE
    ELSE IF AcceptedReplyEvidence(k, i, b) # {}
         THEN CHOOSE t \in HighestAcceptedCandidates(k, i, b) : TRUE
         ELSE IF PreacceptedReplyCandidates(k, i, b) # {}
              THEN CHOOSE t \in PreacceptedReplyCandidates(k, i, b) : TRUE
              ELSE NoopTuple(i)

RecoveryBallotAllowed(k, b) ==
    /\ b \in RecoveryBallots
    /\ IF k = FirstCoordinator
       THEN b = FirstRecoveryBallot
       ELSE k = LastCoordinator /\ b = LastRecoveryBallot

ValidProposedTuple(i, t) ==
    /\ t \in UserTuples(i)
    /\ <<i, t.cmd>> \in proposed
ValidCandidate(i, t) == ValidProposedTuple(i, t) \/ t = NoopTuple(i)

RecoveryOrderChosen ==
    "recovery-low-first" \in raceSeen \/ "recovery-high-first" \in raceSeen
CoordinatorMayProgress(k, i) ==
    IF Mode = "accept-evidence"
    THEN k = FirstCoordinator
    ELSE IF "recovery-low-first" \in raceSeen
         THEN (k = FirstCoordinator \/
               (k = LastCoordinator /\
                coordPhase[FirstCoordinator][i] = "committed"))
         ELSE "recovery-high-first" \in raceSeen /\ k = LastCoordinator
PendingPreAcceptors(m) ==
    {r \in Voters[m.conf] :
        /\ ~\E h \in preacceptedHistory :
              h.sender = r /\ h.inst = m.inst /\ h.ballot = m.ballot
        /\ m.ballot >= promise[r][m.inst]
        /\ durable[r][m.inst].status \notin {"accepted", "committed"}}
PendingAcceptors(m) ==
    {r \in Voters[m.conf] :
        /\ ~AcceptedAt(r, m.inst, m.ballot, m.tuple)
        /\ m.ballot >= promise[r][m.inst]
        /\ (durable[r][m.inst].status # "committed" \/
            durable[r][m.inst].tuple = m.tuple)
        /\ ~\E u \in Tuples :
              u # m.tuple /\ AcceptedAt(r, m.inst, m.ballot, u)}
PendingPreparers(m) ==
    {r \in Voters[m.conf] : promise[r][m.inst] < m.ballot}
PendingCommitLearners(m) ==
    {r \in Voters[m.conf] :
        /\ ~CommittedAt(r, m.inst, m.tuple)
        /\ ~\E u \in Tuples : u # m.tuple /\ CommittedAt(r, m.inst, u)}

UnsentPreResponders(k, i, b, t) ==
    {r \in NormalFastQuorum(InstanceConfig[i]) :
        /\ PreacceptedAt(r, i, b, t)
        /\ ~\E m \in network :
              m.type = "PreAcceptResp" /\ m.from = r /\ m.to = k /\
              m.inst = i /\ m.ballot = b}
AcceptResponseQuorum(i, b) ==
    IF b = MinBallot
    THEN NormalSlowQuorum(InstanceConfig[i])
    ELSE RecoverySlowQuorum(InstanceConfig[i])
UnsentAcceptResponders(k, i, b, t) ==
    {r \in AcceptResponseQuorum(i, b) :
        /\ AcceptedAt(r, i, b, t)
        /\ ~\E m \in network :
              m.type = "AcceptResp" /\ m.from = r /\ m.to = k /\
              m.inst = i /\ m.ballot = b}
UnsentPrepareResponders(k, i, b) ==
    {r \in RecoverySlowQuorum(InstanceConfig[i]) :
        /\ promise[r][i] = b
        /\ ~\E m \in network :
              m.type = "PrepareResp" /\ m.from = r /\ m.to = k /\
              m.inst = i /\ m.ballot = b}
UnrecordedResponseSenders(kind, replies, k, i, b) ==
    {r \in Voters[InstanceConfig[i]] :
        /\ replies[k][i][b][r] = NoResponse
        /\ \E m \in network :
              m.type = kind /\ m.from = r /\ m.to = k /\
              m.inst = i /\ m.ballot = b}
UnsentRoundResponders(responseKind, requestKind, k, i, b) ==
    {r \in RecoverySlowQuorum(InstanceConfig[i]) :
        /\ promise[r][i] = b
        /\ \E req \in network :
              req.type = requestKind /\ req.from = k /\ req.to = r /\
              req.inst = i /\ req.ballot = b
        /\ ~\E rsp \in network :
              rsp.type = responseKind /\ rsp.from = r /\ rsp.to = k /\
              rsp.inst = i /\ rsp.ballot = b}

TypeOK ==
    /\ Replicas \subseteq Nat
    /\ Replicas # {}
    /\ Instances \subseteq Nat
    /\ Instances # {}
    /\ UserCommands # {}
    /\ Noop \notin UserCommands
    /\ Ballots \subseteq Nat
    /\ Cardinality(Ballots) >= 2
    /\ Configs # {}
    /\ Mode \in Modes
    /\ Coordinators \subseteq Replicas
    /\ Cardinality(Coordinators) = 2
    /\ Voters \in [Configs -> SUBSET Replicas]
    /\ \A c \in Configs : Voters[c] # {}
    /\ InstanceConfig \in [Instances -> Configs]
    /\ \A i \in Instances : Coordinators \cap Voters[InstanceConfig[i]] # {}
    /\ (Mode \in {"fast-recovery", "accept-evidence"} => Cardinality(Instances) = 2)
    /\ (Mode \in {"slow-race", "config-race"} => Cardinality(Instances) = 1)
    /\ proposed \subseteq Instances \X UserCommands
    /\ promise \in [Replicas -> [Instances -> Ballots]]
    /\ \A r \in Replicas, i \in Instances : IsDurableRecord(durable[r][i])
    /\ \A h \in preacceptedHistory : IsPreacceptedFact(h)
    /\ \A h \in acceptedHistory : IsAcceptedFact(h)
    /\ \A h \in commitHistory : IsCommitFact(h)
    /\ \A m \in network : IsMessage(m)
    /\ coordPhase \in [Coordinators -> [Instances -> Phases]]
    /\ coordBallot \in [Coordinators -> [Instances -> Ballots]]
    /\ \A k \in Coordinators, i \in Instances :
          coordCandidate[k][i] \in Tuples \cup {NoTuple}
    /\ \A replies \in {preReplies, prepareReplies, tryReplies, acceptReplies},
          k \in Coordinators, i \in Instances, b \in Ballots, r \in Replicas :
          replies[k][i][b][r] = NoResponse \/ IsResponse(replies[k][i][b][r])
    /\ raceSeen \subseteq RaceTags

Init ==
    /\ proposed = {}
    /\ promise = [r \in Replicas |-> [i \in Instances |-> MinBallot]]
    /\ durable = [r \in Replicas |-> [i \in Instances |-> EmptyDurable]]
    /\ preacceptedHistory = {}
    /\ acceptedHistory = {}
    /\ commitHistory = {}
    /\ network = {}
    /\ coordPhase = [k \in Coordinators |-> [i \in Instances |-> "idle"]]
    /\ coordBallot = [k \in Coordinators |-> [i \in Instances |-> MinBallot]]
    /\ coordCandidate = [k \in Coordinators |-> [i \in Instances |-> NoTuple]]
    /\ preReplies = EmptyReplyMap
    /\ prepareReplies = EmptyReplyMap
    /\ tryReplies = EmptyReplyMap
    /\ acceptReplies = EmptyReplyMap
    /\ raceSeen = {}

Propose(i, c) ==
    /\ i \in Instances
    /\ c \in UserCommands
    /\ (Mode # "fast-recovery" \/
         (i = Target /\ ChosenTuples(OtherInstance) # {}))
    /\ (Mode # "accept-evidence" \/ i = Target)
    /\ <<i, c>> \notin proposed
    /\ ~\E d \in UserCommands : <<i, d>> \in proposed
    /\ proposed' = proposed \cup {<<i, c>>}
    /\ UNCHANGED <<promise, durable, preacceptedHistory, acceptedHistory,
                   commitHistory, network, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

BootstrapFastDependency(c) ==
    LET i == OtherInstance
        k == Owner(i)
        b == MinBallot
        conf == InstanceConfig[i]
        t == UserTuple(i, c)
        voters == Voters[conf]
        accepted == {AcceptedFact(r, i, b, t) : r \in voters}
        committed == {CommitFact(r, i, t) : r \in voters}
        reqs == {Msg("AcceptReq", k, r, i, conf, b, t, "none", b,
                     FALSE, {}, {}) : r \in voters}
        responses ==
            {Msg("AcceptResp", r, k, i, conf, b, t, "accepted", b,
                 FALSE, {}, {DirectEvidence(r, i, b, t)}) : r \in voters}
        commits == {Msg("Commit", k, r, i, conf, b, t, "committed", b,
                        FALSE, {}, {}) : r \in voters}
    IN
    /\ Mode = "fast-recovery"
    /\ c \in UserCommands
    /\ ~\E d \in UserCommands : <<i, d>> \in proposed
    /\ coordPhase[k][i] = "idle"
    /\ proposed' = proposed \cup {<<i, c>>}
    /\ durable' =
          [r \in Replicas |->
            [j \in Instances |->
              IF j = i /\ r \in voters
              THEN [status |-> "committed", recordBallot |-> b, tuple |-> t,
                    fastEligible |-> FALSE, fpDepsCommitted |-> {},
                    acceptEvidence |-> {DirectEvidence(r, i, b, t)}]
              ELSE durable[r][j]]]
    /\ acceptedHistory' = acceptedHistory \cup accepted
    /\ commitHistory' = commitHistory \cup committed
    /\ network' = network \cup reqs \cup responses \cup commits
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "committed"]
    /\ coordCandidate' = [coordCandidate EXCEPT ![k][i] = t]
    /\ acceptReplies' =
          [kk \in Coordinators |->
            [j \in Instances |->
              [bb \in Ballots |->
                [r \in Replicas |->
                  IF kk = k /\ j = i /\ bb = b /\ r \in voters
                  THEN Msg("AcceptResp", r, k, i, conf, b, t, "accepted", b,
                           FALSE, {}, {DirectEvidence(r, i, b, t)})
                  ELSE acceptReplies[kk][j][bb][r]]]]]
    /\ raceSeen' = raceSeen \cup {"accept-response-seen"}
    /\ UNCHANGED <<promise, preacceptedHistory, coordBallot, preReplies,
                   prepareReplies, tryReplies>>

SendPreAccept(k, i, t) ==
    LET conf == InstanceConfig[i]
        reqs == {Msg("PreAcceptReq", k, r, i, conf, MinBallot, t,
                     "none", MinBallot, FALSE, {}, {}) : r \in Voters[conf]}
    IN
    /\ Mode = "fast-recovery"
    /\ i = Target
    /\ k = Owner(i)
    /\ coordPhase[k][i] = "idle"
    /\ ValidProposedTuple(i, t)
    /\ \A d \in t.deps : ChosenTuples(d) # {}
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "preaccept"]
    /\ coordBallot' = [coordBallot EXCEPT ![k][i] = MinBallot]
    /\ coordCandidate' = [coordCandidate EXCEPT ![k][i] = t]
    /\ network' = network \cup reqs
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, preReplies, prepareReplies,
                   tryReplies, acceptReplies, raceSeen>>

ReceivePreAccept(r, m) ==
    LET stored == IF Mode = "fast-recovery" /\
                         r = MaxOf(Voters[m.conf])
                  THEN AlternateTuple(m.tuple)
                  ELSE m.tuple
        fast == stored = m.tuple
        fp == IF fast THEN LocallyCommittedDeps(r, stored) ELSE {}
        fact == PreFact(r, m.inst, m.ballot, stored, fp)
        old == durable[r][m.inst]
        next == [status |-> "preaccepted", recordBallot |-> m.ballot,
                 tuple |-> stored, fastEligible |-> fast,
                 fpDepsCommitted |-> fp, acceptEvidence |-> old.acceptEvidence]
    IN
    /\ m \in network
    /\ m.type = "PreAcceptReq"
    /\ m.to = r
    /\ m.conf = InstanceConfig[m.inst]
    /\ r \in Voters[m.conf]
    /\ r = FirstOrDefault(PendingPreAcceptors(m))
    /\ m.ballot >= promise[r][m.inst]
    /\ m.tuple \in UserTuples(m.inst)
    /\ <<m.inst, m.tuple.cmd>> \in proposed
    /\ old.status \notin {"accepted", "committed"}
    /\ fact \notin preacceptedHistory
    /\ promise' = [promise EXCEPT ![r][m.inst] = m.ballot]
    /\ durable' = [durable EXCEPT ![r][m.inst] = next]
    /\ preacceptedHistory' = preacceptedHistory \cup {fact}
    /\ UNCHANGED <<proposed, acceptedHistory, commitHistory, network,
                   coordPhase, coordBallot, coordCandidate, preReplies,
                   prepareReplies, tryReplies, acceptReplies, raceSeen>>

SendPreAcceptResp(r, i, k) ==
    LET b == MinBallot
        t == durable[r][i].tuple
        m == Msg("PreAcceptResp", r, k, i, InstanceConfig[i], b, t,
                 durable[r][i].status, durable[r][i].recordBallot,
                 durable[r][i].fastEligible, durable[r][i].fpDepsCommitted,
                 {})
    IN
    /\ Msg("PreAcceptReq", k, r, i, InstanceConfig[i], b,
           coordCandidate[k][i], "none", b, FALSE, {}, {}) \in network
    /\ coordPhase[k][i] = "preaccept"
    /\ PendingPreAcceptors(
          Msg("PreAcceptReq", k, r, i, InstanceConfig[i], b,
              coordCandidate[k][i], "none", b, FALSE, {}, {})) = {}
    /\ durable[r][i].status = "preaccepted"
    /\ durable[r][i].recordBallot = b
    /\ PreFact(r, i, b, t, durable[r][i].fpDepsCommitted) \in preacceptedHistory
    /\ r = FirstOrDefault(UnsentPreResponders(k, i, b, t))
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

FastChoose(k, i) ==
    LET b == MinBallot
        t == coordCandidate[k][i]
        conf == InstanceConfig[i]
        commitMsgs == {Msg("Commit", k, r, i, conf, b, t, "committed", b,
                           FALSE, {}, {}) : r \in Voters[conf]}
        local == [status |-> "committed", recordBallot |-> b, tuple |-> t,
                  fastEligible |-> FALSE,
                  fpDepsCommitted |-> durable[k][i].fpDepsCommitted,
                  acceptEvidence |-> durable[k][i].acceptEvidence]
    IN
    /\ coordPhase[k][i] = "preaccept"
    /\ \E q \in FastQuorums(conf) :
          /\ q \subseteq MatchingReplySenders(preReplies, k, i, b, t)
          /\ \A d \in t.deps :
                \E r \in q : d \in preReplies[k][i][b][r].fpDepsCommitted
    /\ t \in ChosenTuples(i)
    /\ commitHistory' = commitHistory \cup {CommitFact(k, i, t)}
    /\ durable' = [durable EXCEPT ![k][i] = local]
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "fast-committed"]
    /\ network' = network \cup commitMsgs
    /\ UNCHANGED <<proposed, promise, preacceptedHistory, acceptedHistory,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies, raceSeen>>

NormalSlowInstance(i) ==
    \/ (Mode \in {"slow-race", "config-race"} /\ i = Target)
    \/ (Mode = "fast-recovery" /\ i = OtherInstance)
    \/ (Mode = "accept-evidence" /\ i = Target)

StartAccept(k, i, b, t) ==
    LET conf == InstanceConfig[i]
        reqs == {Msg("AcceptReq", k, r, i, conf, b, t, "none", b,
                     FALSE, {}, {}) : r \in Voters[conf]}
        normal == coordPhase[k][i] = "idle" /\ k = Owner(i) /\
                  b = MinBallot /\ NormalSlowInstance(i) /\
                  ValidProposedTuple(i, t)
        recovery == coordPhase[k][i] = "prepared" /\
                    b = coordBallot[k][i] /\ t = coordCandidate[k][i] /\
                    ValidCandidate(i, t) /\
                    (Mode # "accept-evidence" \/
                     "try-authorized" \in raceSeen \/
                     "try-fail-closed" \in raceSeen)
    IN
    /\ normal \/ recovery
    /\ network' = network \cup reqs
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "accept"]
    /\ coordBallot' = [coordBallot EXCEPT ![k][i] = b]
    /\ coordCandidate' = [coordCandidate EXCEPT ![k][i] = t]
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, preReplies, prepareReplies,
                   tryReplies, acceptReplies, raceSeen>>

ReceiveAccept(r, m) ==
    LET ev == DirectEvidence(r, m.inst, m.ballot, m.tuple)
        old == durable[r][m.inst]
        next == [status |-> "accepted", recordBallot |-> m.ballot,
                 tuple |-> m.tuple, fastEligible |-> FALSE,
                 fpDepsCommitted |-> old.fpDepsCommitted,
                 acceptEvidence |-> old.acceptEvidence \cup {ev}]
    IN
    /\ m \in network
    /\ m.type = "AcceptReq"
    /\ m.to = r
    /\ m.conf = InstanceConfig[m.inst]
    /\ r \in Voters[m.conf]
    /\ r = FirstOrDefault(PendingAcceptors(m))
    /\ m.ballot >= promise[r][m.inst]
    /\ ValidCandidate(m.inst, m.tuple)
    /\ ~\E u \in Tuples : u # m.tuple /\ AcceptedAt(r, m.inst, m.ballot, u)
    /\ AcceptedFact(r, m.inst, m.ballot, m.tuple) \notin acceptedHistory
    /\ old.status # "committed" \/ old.tuple = m.tuple
    /\ promise' = [promise EXCEPT ![r][m.inst] = m.ballot]
    /\ durable' = [durable EXCEPT ![r][m.inst] = next]
    /\ acceptedHistory' =
          acceptedHistory \cup {AcceptedFact(r, m.inst, m.ballot, m.tuple)}
    /\ UNCHANGED <<proposed, preacceptedHistory, commitHistory, network,
                   coordPhase, coordBallot, coordCandidate, preReplies,
                   prepareReplies, tryReplies, acceptReplies, raceSeen>>

SendAcceptResp(r, i, k, b) ==
    LET t == durable[r][i].tuple
        m == Msg("AcceptResp", r, k, i, InstanceConfig[i], b, t,
                 "accepted", b, FALSE, durable[r][i].fpDepsCommitted,
                 durable[r][i].acceptEvidence)
    IN
    /\ Msg("AcceptReq", k, r, i, InstanceConfig[i], b, t,
           "none", b, FALSE, {}, {}) \in network
    /\ coordPhase[k][i] = "accept"
    /\ coordCandidate[k][i] = t
    /\ PendingAcceptors(
          Msg("AcceptReq", k, r, i, InstanceConfig[i], b, t,
              "none", b, FALSE, {}, {})) = {}
    /\ AcceptedAt(r, i, b, t)
    /\ r = FirstOrDefault(UnsentAcceptResponders(k, i, b, t))
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

FinishAccept(k, i) ==
    LET b == coordBallot[k][i]
        t == coordCandidate[k][i]
        conf == InstanceConfig[i]
        commitMsgs == {Msg("Commit", k, r, i, conf, b, t, "committed", b,
                           FALSE, {}, {}) : r \in Voters[conf]}
        local == [status |-> "committed", recordBallot |-> b, tuple |-> t,
                  fastEligible |-> FALSE,
                  fpDepsCommitted |-> durable[k][i].fpDepsCommitted,
                  acceptEvidence |-> durable[k][i].acceptEvidence]
    IN
    /\ coordPhase[k][i] = "accept"
    /\ AcceptResponseQuorum(i, b) \subseteq
          MatchingReplySenders(acceptReplies, k, i, b, t)
    /\ SlowChosen(i, t)
    /\ commitHistory' = commitHistory \cup {CommitFact(k, i, t)}
    /\ durable' = [durable EXCEPT ![k][i] = local]
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "committed"]
    /\ network' = network \cup commitMsgs
    /\ UNCHANGED <<proposed, promise, preacceptedHistory, acceptedHistory,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies, raceSeen>>

CommitLearningEnabled(i) ==
    /\ i = Target
    /\ IF Mode = "accept-evidence"
       THEN coordPhase[FirstCoordinator][i] = "committed" /\
            coordBallot[FirstCoordinator][i] > MinBallot
       ELSE coordPhase[LastCoordinator][i] = "committed"

LearnCommit(r, m) ==
    LET old == durable[r][m.inst]
        next == [status |-> "committed", recordBallot |-> m.recordBallot,
                 tuple |-> m.tuple, fastEligible |-> FALSE,
                 fpDepsCommitted |-> old.fpDepsCommitted,
                 acceptEvidence |-> old.acceptEvidence]
    IN
    /\ m \in network
    /\ CommitLearningEnabled(m.inst)
    /\ "duplicate-response-ignored" \in raceSeen
    /\ m.type = "Commit"
    /\ m.to = r
    /\ m.conf = InstanceConfig[m.inst]
    /\ r = FirstOrDefault(PendingCommitLearners(m))
    /\ m.tuple \in ChosenTuples(m.inst)
    /\ ~\E u \in Tuples : u # m.tuple /\ CommittedAt(r, m.inst, u)
    /\ CommitFact(r, m.inst, m.tuple) \notin commitHistory
    /\ durable' = [durable EXCEPT ![r][m.inst] = next]
    /\ commitHistory' = commitHistory \cup {CommitFact(r, m.inst, m.tuple)}
    /\ UNCHANGED <<proposed, promise, preacceptedHistory, acceptedHistory,
                   network, coordPhase, coordBallot, coordCandidate,
                   preReplies, prepareReplies, tryReplies, acceptReplies,
                   raceSeen>>

StartPrepare(k, i, b) ==
    LET conf == InstanceConfig[i]
        reqs == {Msg("PrepareReq", k, r, i, conf, b, NoTuple, "none", b,
                     FALSE, {}, {}) : r \in Voters[conf]}
        otherActive == \E j \in Coordinators \ {k} :
            coordPhase[j][i] \in {"prepare", "prepared", "try-preaccept",
                                  "retry-prepare", "accept", "committed"}
    IN
    /\ i = Target
    /\ IF Mode = "accept-evidence"
       THEN k = FirstCoordinator
       ELSE k = FirstCoordinator \/
            (k = LastCoordinator /\
             coordPhase[FirstCoordinator][i] = "prepare")
    /\ RecoveryBallotAllowed(k, b)
    /\ coordPhase[k][i] \in {"idle", "fast-committed", "committed",
                              "retry-prepare"}
    /\ b > coordBallot[k][i]
    /\ ChosenTuples(i) # {}
    /\ ~(reqs \subseteq network)
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "prepare"]
    /\ coordBallot' = [coordBallot EXCEPT ![k][i] = b]
    /\ coordCandidate' = [coordCandidate EXCEPT ![k][i] = NoTuple]
    /\ network' = network \cup reqs
    /\ raceSeen' = IF otherActive
                    THEN raceSeen \cup {"two-coordinator-race"}
                    ELSE raceSeen
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, preReplies, prepareReplies,
                   tryReplies, acceptReplies>>

ChooseRecoveryOrder(i, order) ==
    /\ Mode # "accept-evidence"
    /\ i = Target
    /\ order \in {"recovery-low-first", "recovery-high-first"}
    /\ ~RecoveryOrderChosen
    /\ coordPhase[FirstCoordinator][i] = "prepare"
    /\ coordPhase[LastCoordinator][i] = "prepare"
    /\ raceSeen' = raceSeen \cup {order}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies>>

ReceivePrepare(r, m) ==
    /\ m \in network
    /\ m.type = "PrepareReq"
    /\ (Mode = "accept-evidence" \/ RecoveryOrderChosen)
    /\ CoordinatorMayProgress(m.from, m.inst)
    /\ m.to = r
    /\ m.conf = InstanceConfig[m.inst]
    /\ r \in Voters[m.conf]
    /\ r = FirstOrDefault(PendingPreparers(m))
    /\ m.ballot >= promise[r][m.inst]
    /\ m.ballot # promise[r][m.inst]
    /\ promise' = [promise EXCEPT ![r][m.inst] = m.ballot]
    /\ UNCHANGED <<proposed, durable, preacceptedHistory, acceptedHistory,
                   commitHistory, network, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

SendPrepareResp(r, i, k, b) ==
    LET d == durable[r][i]
        m == Msg("PrepareResp", r, k, i, InstanceConfig[i], b, d.tuple,
                 d.status, d.recordBallot, d.fastEligible,
                 d.fpDepsCommitted, d.acceptEvidence)
        req == Msg("PrepareReq", k, r, i, InstanceConfig[i], b, NoTuple,
                   "none", b, FALSE, {}, {})
    IN
    /\ req \in network
    /\ CoordinatorMayProgress(k, i)
    /\ PendingPreparers(req) = {}
    /\ promise[r][i] = b
    /\ r = FirstOrDefault(UnsentPrepareResponders(k, i, b))
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

FinishPrepare(k, i) ==
    LET b == coordBallot[k][i]
        responders == ReplySenders(prepareReplies, k, i, b)
        candidate == PreparedCandidate(k, i, b)
        tryBranch == CommittedReplyCandidates(k, i, b) = {} /\
                     AcceptedReplyEvidence(k, i, b) = {} /\
                     PreacceptedReplyCandidates(k, i, b) # {}
    IN
    /\ coordPhase[k][i] = "prepare"
    /\ CoordinatorMayProgress(k, i)
    /\ RecoverySlowQuorum(InstanceConfig[i]) \subseteq responders
    /\ ValidCandidate(i, candidate)
    /\ coordCandidate' = [coordCandidate EXCEPT ![k][i] = candidate]
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] =
                        IF tryBranch THEN "try-preaccept" ELSE "prepared"]
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordBallot,
                   preReplies, prepareReplies, tryReplies, acceptReplies,
                   raceSeen>>

StartTryPreAccept(k, i) ==
    LET b == coordBallot[k][i]
        t == coordCandidate[k][i]
        conf == InstanceConfig[i]
        tryReqs == {Msg("TryPreAcceptReq", k, r, i, conf, b, t, "none", b,
                        FALSE, {}, {}) : r \in Voters[conf]}
        evidenceReqs == {Msg("EvidenceReq", k, r, i, conf, b, t, "none", b,
                             FALSE, {}, {}) : r \in Voters[conf]}
    IN
    /\ CoordinatorMayProgress(k, i)
    /\ coordPhase[k][i] \in
          IF Mode = "accept-evidence" THEN {"prepared", "try-preaccept"}
          ELSE {"try-preaccept"}
    /\ t \in Tuples
    /\ ~(tryReqs \subseteq network)
    /\ network' = network \cup tryReqs \cup
          (IF Mode = "accept-evidence" THEN evidenceReqs ELSE {})
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "try-preaccept"]
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordBallot, coordCandidate,
                   preReplies, prepareReplies, tryReplies, acceptReplies,
                   raceSeen>>

ReceiveTryPreAccept(r, m) ==
    /\ m \in network
    /\ m.type = "TryPreAcceptReq"
    /\ m.to = r
    /\ m.conf = InstanceConfig[m.inst]
    /\ r \in Voters[m.conf]
    /\ m.ballot >= promise[r][m.inst]
    /\ promise' = [promise EXCEPT ![r][m.inst] = m.ballot]
    /\ UNCHANGED <<proposed, durable, preacceptedHistory, acceptedHistory,
                   commitHistory, network, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

TrySafe(r, i, t) ==
    durable[r][i].tuple = NoTuple \/ durable[r][i].tuple = t \/
    durable[r][i].status = "preaccepted"

SendTryPreAcceptResp(r, i, k, b) ==
    LET candidate == coordCandidate[k][i]
        answer == IF TrySafe(r, i, candidate) THEN candidate ELSE NoTuple
        d == durable[r][i]
        m == Msg("TryPreAcceptResp", r, k, i, InstanceConfig[i], b, answer,
                 d.status, d.recordBallot, d.fastEligible,
                 d.fpDepsCommitted, d.acceptEvidence)
        req == Msg("TryPreAcceptReq", k, r, i, InstanceConfig[i], b,
                   candidate, "none", b, FALSE, {}, {})
    IN
    /\ req \in network
    /\ CoordinatorMayProgress(k, i)
    /\ coordPhase[k][i] = "try-preaccept"
    /\ promise[r][i] = b
    /\ r = FirstOrDefault(UnsentRoundResponders(
          "TryPreAcceptResp", "TryPreAcceptReq", k, i, b))
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

SendEvidenceResp(r, i, k, b) ==
    LET d == durable[r][i]
        m == Msg("EvidenceResp", r, k, i, InstanceConfig[i], b, d.tuple,
                 d.status, d.recordBallot, d.fastEligible,
                 d.fpDepsCommitted, d.acceptEvidence)
        req == Msg("EvidenceReq", k, r, i, InstanceConfig[i], b,
                   coordCandidate[k][i], "none", b, FALSE, {}, {})
    IN
    /\ Mode = "accept-evidence"
    /\ CoordinatorMayProgress(k, i)
    /\ coordPhase[k][i] = "try-preaccept"
    /\ r = MinOf(RecoverySlowQuorum(InstanceConfig[i]))
    /\ req \in network
    /\ promise[r][i] = b
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

EvidenceReplySenders(k, i, b, t) ==
    {r \in Voters[InstanceConfig[i]] :
        LET m == preReplies[k][i][b][r]
        IN /\ m # NoResponse
           /\ m.type = "EvidenceResp"
           /\ m.tuple = t
           /\ PayloadEvidenceSound(m)
           /\ \E ev \in m.acceptEvidence :
                 ev.inst = i /\ ev.tuple = t}
EvidenceQuorumSize(c) == Cardinality(Voters[c]) - SlowQuorumSize(c)

FinishTryPreAccept(k, i) ==
    LET b == coordBallot[k][i]
        candidate == coordCandidate[k][i]
        responders == ReplySenders(tryReplies, k, i, b)
        matching == MatchingReplySenders(tryReplies, k, i, b, candidate)
        responseQuorum == RecoverySlowQuorum(InstanceConfig[i])
        tryMatches == responseQuorum \subseteq matching
        evidenceOK ==
            Mode # "accept-evidence" \/
            Cardinality(EvidenceReplySenders(k, i, b, candidate)) >=
                EvidenceQuorumSize(InstanceConfig[i])
        authorized == tryMatches /\ evidenceOK
        missingEvidence == tryMatches /\ ~evidenceOK
        rejected == responseQuorum \subseteq responders /\ ~tryMatches
    IN
    /\ coordPhase[k][i] = "try-preaccept"
    /\ authorized \/ missingEvidence \/ rejected
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] =
                        IF rejected THEN "retry-prepare" ELSE "prepared"]
    /\ raceSeen' = raceSeen \cup
          {IF authorized THEN "try-authorized" ELSE "try-fail-closed"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies>>

TimeoutTryPreAccept(k, i) ==
    LET b == coordBallot[k][i]
    IN
    /\ coordPhase[k][i] = "try-preaccept"
    /\ Mode = "accept-evidence"
    /\ EvidenceReplySenders(k, i, b, coordCandidate[k][i]) = {}
    /\ coordPhase' = [coordPhase EXCEPT ![k][i] = "prepared"]
    /\ raceSeen' = raceSeen \cup {"try-fail-closed"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies>>

RecordReply(replies, k, i, b, r, m) ==
    [replies EXCEPT ![k][i][b][r] = m]

RecordPreReply(m) ==
    /\ m.type = "PreAcceptResp"
    /\ coordPhase[m.to][m.inst] = "preaccept"
    /\ ResponseScoped(m, m.to, m.inst, m.ballot)
    /\ m.ballot = coordBallot[m.to][m.inst]
    /\ m.tuple = coordCandidate[m.to][m.inst]
    /\ PayloadEvidenceSound(m)
    /\ preReplies[m.to][m.inst][m.ballot][m.from] = NoResponse
    /\ UnsentPreResponders(m.to, m.inst, m.ballot, m.tuple) = {}
    /\ m.from = FirstOrDefault(UnrecordedResponseSenders(
          "PreAcceptResp", preReplies, m.to, m.inst, m.ballot))
    /\ preReplies' = RecordReply(preReplies, m.to, m.inst, m.ballot, m.from, m)
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

RecordPrepareReply(m) ==
    /\ m.type = "PrepareResp"
    /\ coordPhase[m.to][m.inst] = "prepare"
    /\ ResponseScoped(m, m.to, m.inst, m.ballot)
    /\ m.ballot = coordBallot[m.to][m.inst]
    /\ PayloadEvidenceSound(m)
    /\ prepareReplies[m.to][m.inst][m.ballot][m.from] = NoResponse
    /\ UnsentPrepareResponders(m.to, m.inst, m.ballot) = {}
    /\ m.from = FirstOrDefault(UnrecordedResponseSenders(
          "PrepareResp", prepareReplies, m.to, m.inst, m.ballot))
    /\ prepareReplies' =
          RecordReply(prepareReplies, m.to, m.inst, m.ballot, m.from, m)
    /\ raceSeen' =
          (IF m.acceptEvidence # {}
           THEN raceSeen \cup {"prepare-response-with-evidence-seen"}
           ELSE raceSeen)
          \cup (IF m.from \in RemovedPinnedVoters
                THEN {"removed-pinned-voter-counted"} ELSE {})
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, tryReplies,
                   acceptReplies>>

RecordTryReply(m) ==
    /\ m.type = "TryPreAcceptResp"
    /\ coordPhase[m.to][m.inst] = "try-preaccept"
    /\ ResponseScoped(m, m.to, m.inst, m.ballot)
    /\ m.ballot = coordBallot[m.to][m.inst]
    /\ m.tuple \in {NoTuple, coordCandidate[m.to][m.inst]}
    /\ PayloadEvidenceSound(m)
    /\ tryReplies[m.to][m.inst][m.ballot][m.from] = NoResponse
    /\ UnsentRoundResponders("TryPreAcceptResp", "TryPreAcceptReq",
                             m.to, m.inst, m.ballot) = {}
    /\ m.from = FirstOrDefault(UnrecordedResponseSenders(
          "TryPreAcceptResp", tryReplies, m.to, m.inst, m.ballot))
    /\ tryReplies' = RecordReply(tryReplies, m.to, m.inst, m.ballot, m.from, m)
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   acceptReplies, raceSeen>>

RecordAcceptReply(m) ==
    /\ m.type = "AcceptResp"
    /\ coordPhase[m.to][m.inst] = "accept"
    /\ ResponseScoped(m, m.to, m.inst, m.ballot)
    /\ m.ballot = coordBallot[m.to][m.inst]
    /\ m.tuple = coordCandidate[m.to][m.inst]
    /\ PayloadEvidenceSound(m)
    /\ \A ev \in m.acceptEvidence : ev.sender = m.from
    /\ acceptReplies[m.to][m.inst][m.ballot][m.from] = NoResponse
    /\ UnsentAcceptResponders(m.to, m.inst, m.ballot, m.tuple) = {}
    /\ m.from = FirstOrDefault(UnrecordedResponseSenders(
          "AcceptResp", acceptReplies, m.to, m.inst, m.ballot))
    /\ acceptReplies' =
          RecordReply(acceptReplies, m.to, m.inst, m.ballot, m.from, m)
    /\ raceSeen' = (raceSeen \cup {"accept-response-seen"})
          \cup (IF m.from \in RemovedPinnedVoters
                THEN {"removed-pinned-voter-counted"} ELSE {})
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies>>

RecordEvidenceReply(m) ==
    /\ m.type = "EvidenceResp"
    /\ Mode = "accept-evidence"
    /\ coordPhase[m.to][m.inst] = "try-preaccept"
    /\ ResponseScoped(m, m.to, m.inst, m.ballot)
    /\ m.ballot = coordBallot[m.to][m.inst]
    /\ m.tuple = coordCandidate[m.to][m.inst]
    /\ PayloadEvidenceSound(m)
    /\ \E ev \in m.acceptEvidence :
          ev.inst = m.inst /\ ev.tuple = m.tuple
    /\ preReplies[m.to][m.inst][m.ballot][m.from] = NoResponse
    /\ m.from = FirstOrDefault(UnrecordedResponseSenders(
          "EvidenceResp", preReplies, m.to, m.inst, m.ballot))
    /\ preReplies' =
          RecordReply(preReplies, m.to, m.inst, m.ballot, m.from, m)
    /\ raceSeen' = raceSeen \cup {"evidence-response-seen"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, prepareReplies, tryReplies,
                   acceptReplies>>

ExpectedMapValue(m) ==
    IF m.type = "PreAcceptResp" THEN preReplies[m.to][m.inst][m.ballot][m.from]
    ELSE IF m.type = "PrepareResp" THEN prepareReplies[m.to][m.inst][m.ballot][m.from]
    ELSE IF m.type = "TryPreAcceptResp" THEN tryReplies[m.to][m.inst][m.ballot][m.from]
    ELSE IF m.type = "AcceptResp" THEN acceptReplies[m.to][m.inst][m.ballot][m.from]
    ELSE NoResponse

IgnoreDuplicateResponse(m) ==
    /\ m.type \in {"AcceptResp", "PreAcceptResp"}
    /\ CommitLearningEnabled(m.inst)
    /\ m.to = Owner(m.inst)
    /\ m.from \in Replicas
    /\ m.ballot = MinBallot
    /\ "stale-response-ignored" \in raceSeen
    /\ (Mode # "accept-evidence" \/ "stale-request-ignored" \in raceSeen)
    /\ ExpectedMapValue(m) # NoResponse
    /\ "duplicate-response-ignored" \notin raceSeen
    /\ raceSeen' = raceSeen \cup {"duplicate-response-ignored"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies>>

IgnoreDuplicateCommit(m) ==
    /\ m.type = "Commit"
    /\ CommitLearningEnabled(m.inst)
    /\ CommittedAt(m.to, m.inst, m.tuple)
    /\ PendingCommitLearners(m) = {}
    /\ "duplicate-commit-ignored" \notin raceSeen
    /\ raceSeen' = raceSeen \cup {"duplicate-commit-ignored"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies>>

IgnoreStaleResponse(m) ==
    /\ m.type \in ResponseKinds
    /\ m.to \in Coordinators
    /\ m.ballot # coordBallot[m.to][m.inst]
    /\ m.type = "PrepareResp"
    /\ m.tuple = NoTuple
    /\ "stale-response-ignored" \notin raceSeen
    /\ raceSeen' = raceSeen \cup {"stale-response-ignored"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies>>

FailClosedMalformedEvidence(m) ==
    /\ Mode = "accept-evidence"
    /\ m.type = "EvidenceResp"
    /\ m.to \in Coordinators
    /\ coordPhase[m.to][m.inst] = "try-preaccept"
    /\ m.ballot = coordBallot[m.to][m.inst]
    /\ ~PayloadEvidenceSound(m)
    /\ coordPhase' = [coordPhase EXCEPT ![m.to][m.inst] = "prepared"]
    /\ raceSeen' = raceSeen \cup {"malformed-evidence-fail-closed",
                                  "try-fail-closed"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies>>

IgnoreAddedCurrentVoter(m) ==
    /\ Mode = "config-race"
    /\ m.type \in ResponseKinds
    /\ m.inst = Target
    /\ m.from \in AddedCurrentVoters
    /\ m.from \notin Voters[InstanceConfig[Target]]
    /\ "added-current-voter-ignored" \notin raceSeen
    /\ raceSeen' = raceSeen \cup {"added-current-voter-ignored"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies>>

DeliverResponse(m) ==
    /\ m \in network
    /\ m.type \in ResponseKinds
    /\ \/ RecordPreReply(m)
       \/ RecordPrepareReply(m)
       \/ RecordTryReply(m)
       \/ RecordAcceptReply(m)
       \/ RecordEvidenceReply(m)
       \/ IgnoreDuplicateResponse(m)
       \/ IgnoreStaleResponse(m)
       \/ FailClosedMalformedEvidence(m)
       \/ IgnoreAddedCurrentVoter(m)

IgnoreStaleRequest(m) ==
    /\ m \in network
    /\ Mode = "accept-evidence"
    /\ CommitLearningEnabled(m.inst)
    /\ "stale-response-ignored" \in raceSeen
    /\ m.type \in RequestKinds
    /\ m.to \in Replicas
    /\ m.ballot < promise[m.to][m.inst]
    /\ "stale-request-ignored" \notin raceSeen
    /\ raceSeen' = raceSeen \cup {"stale-request-ignored"}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, network, coordPhase,
                   coordBallot, coordCandidate, preReplies, prepareReplies,
                   tryReplies, acceptReplies>>

InjectStaleResponse(k, i, b, r) ==
    LET m == Msg("PrepareResp", r, k, i, InstanceConfig[i], b, NoTuple,
                 "none", MinBallot, FALSE, {}, {})
    IN
    /\ i = Target
    /\ k = FirstCoordinator
    /\ r = Owner(i)
    /\ b = MinBallot
    /\ coordBallot[k][i] > MinBallot
    /\ CommitLearningEnabled(i)
    /\ "stale-response-ignored" \notin raceSeen
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

InjectMalformedEvidence(k, i, r) ==
    LET b == coordBallot[k][i]
        t == coordCandidate[k][i]
        other == CHOOSE c \in UserCommands : c # t.cmd
        badTuple == UserTuple(i, other)
        badEv == DirectEvidence(r, i, b, badTuple)
        m == Msg("EvidenceResp", r, k, i, InstanceConfig[i], b, badTuple,
                 "accepted", b, FALSE, {}, {badEv})
    IN
    /\ Mode = "accept-evidence"
    /\ i = Target
    /\ k = FirstCoordinator
    /\ t \in Tuples
    /\ coordPhase[k][i] = "try-preaccept"
    /\ t.cmd \in UserCommands
    /\ Cardinality(UserCommands) >= 2
    /\ r \in Voters[InstanceConfig[i]]
    /\ r = Owner(i)
    /\ ~EvidenceHistorySound(badEv)
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

InjectAddedVoterResponse(k, i, r) ==
    LET b == coordBallot[k][i]
        m == Msg("PrepareResp", r, k, i, CurrentConfig, b, NoTuple,
                 "none", MinBallot, FALSE, {}, {})
    IN
    /\ Mode = "config-race"
    /\ i = Target
    /\ k = FirstCoordinator
    /\ CommitLearningEnabled(i)
    /\ "duplicate-commit-ignored" \in raceSeen
    /\ coordBallot[k][i] > MinBallot
    /\ r \in AddedCurrentVoters
    /\ m \notin network
    /\ network' = network \cup {m}
    /\ UNCHANGED <<proposed, promise, durable, preacceptedHistory,
                   acceptedHistory, commitHistory, coordPhase, coordBallot,
                   coordCandidate, preReplies, prepareReplies, tryReplies,
                   acceptReplies, raceSeen>>

Deliver(m) ==
    /\ m \in network
    /\ \/ ReceivePreAccept(m.to, m)
       \/ ReceiveAccept(m.to, m)
       \/ ReceivePrepare(m.to, m)
       \/ LearnCommit(m.to, m)
       \/ IgnoreDuplicateCommit(m)
       \/ DeliverResponse(m)
       \/ IgnoreStaleRequest(m)

Next ==
    \/ \E i \in Instances, c \in UserCommands : Propose(i, c)
    \/ \E c \in UserCommands : BootstrapFastDependency(c)
    \/ \E k \in Coordinators, i \in Instances :
          \E t \in UserTuples(i) : SendPreAccept(k, i, t)
    \/ \E r \in Replicas, i \in Instances, k \in Coordinators :
          SendPreAcceptResp(r, i, k)
    \/ \E k \in Coordinators, i \in Instances : FastChoose(k, i)
    \/ \E k \in Coordinators, i \in Instances, b \in Ballots :
          \E t \in UserTuples(i) \cup {NoopTuple(i)} :
              StartAccept(k, i, b, t)
    \/ \E r \in Replicas, i \in Instances, k \in Coordinators, b \in Ballots :
          SendAcceptResp(r, i, k, b)
    \/ \E k \in Coordinators, i \in Instances : FinishAccept(k, i)
    \/ \E k \in Coordinators, i \in Instances, b \in Ballots :
          StartPrepare(k, i, b)
    \/ \E i \in Instances,
          order \in {"recovery-low-first", "recovery-high-first"} :
          ChooseRecoveryOrder(i, order)
    \/ \E r \in Replicas, i \in Instances, k \in Coordinators, b \in Ballots :
          SendPrepareResp(r, i, k, b)
    \/ \E k \in Coordinators, i \in Instances : FinishPrepare(k, i)
    \/ \E k \in Coordinators, i \in Instances : StartTryPreAccept(k, i)
    \/ \E r \in Replicas, i \in Instances, k \in Coordinators, b \in Ballots :
          SendTryPreAcceptResp(r, i, k, b)
    \/ \E r \in Replicas, i \in Instances, k \in Coordinators, b \in Ballots :
          SendEvidenceResp(r, i, k, b)
    \/ \E k \in Coordinators, i \in Instances : FinishTryPreAccept(k, i)
    \/ \E k \in Coordinators, i \in Instances : TimeoutTryPreAccept(k, i)
    \/ \E m \in network : Deliver(m)
    \/ \E k \in Coordinators, i \in Instances, b \in Ballots,
          r \in Replicas : InjectStaleResponse(k, i, b, r)
    \/ \E k \in Coordinators, i \in Instances, r \in Replicas :
          InjectMalformedEvidence(k, i, r)
    \/ \E k \in Coordinators, i \in Instances, r \in Replicas :
          InjectAddedVoterResponse(k, i, r)

(***************************************************************************)
(* Safety invariants.                                                       *)
(***************************************************************************)

Nontriviality ==
    \A i \in Instances :
        \A t \in ChosenTuples(i) :
            t.cmd # Noop => <<i, t.cmd>> \in proposed

AcceptedValueWasProposed ==
    \A h \in acceptedHistory :
        h.tuple.cmd # Noop => <<h.inst, h.tuple.cmd>> \in proposed

LocalStability ==
    \A h, g \in commitHistory :
        h.sender = g.sender /\ h.inst = g.inst => h.tuple = g.tuple

ChosenStability ==
    \A i \in Instances : Cardinality(ChosenTuples(i)) <= 1

ChosenTupleAgreement ==
    \A h, g \in commitHistory :
        h.inst = g.inst => h.tuple = g.tuple

CommittedImpliesChosen ==
    \A h \in commitHistory : h.tuple \in ChosenTuples(h.inst)

PromiseMonotonic ==
    \A h \in acceptedHistory : h.ballot <= promise[h.sender][h.inst]

OneAcceptedTuplePerBallot ==
    \A h, g \in acceptedHistory :
        h.sender = g.sender /\ h.inst = g.inst /\ h.ballot = g.ballot =>
            h.tuple = g.tuple

AcceptedBallotRespectsPromise ==
    \A r \in Replicas, i \in Instances :
        durable[r][i].status = "accepted" =>
            durable[r][i].recordBallot <= promise[r][i]

AllReplyMaps == {preReplies, prepareReplies, tryReplies, acceptReplies}
CurrentRoundResponsesOnly ==
    \A replies \in AllReplyMaps, k \in Coordinators, i \in Instances,
       b \in Ballots, r \in Replicas :
        LET m == replies[k][i][b][r]
        IN m # NoResponse =>
            /\ ResponseScoped(m, k, i, b)
            /\ PayloadEvidenceSound(m)

UniqueResponseSender ==
    \A replies \in AllReplyMaps, k \in Coordinators, i \in Instances,
       b \in Ballots, r \in Replicas :
        replies[k][i][b][r] # NoResponse =>
            replies[k][i][b][r].from = r

PinnedConfigVotersOnly ==
    \A replies \in AllReplyMaps, k \in Coordinators, i \in Instances,
       b \in Ballots :
        ReplySenders(replies, k, i, b) \subseteq Voters[InstanceConfig[i]]

ResponseEvidenceSound ==
    \A replies \in AllReplyMaps, k \in Coordinators, i \in Instances,
       b \in Ballots :
        \A m \in StoredReplies(replies, k, i, b) :
            PayloadEvidenceSound(m)

EvidenceSenderAuthentic ==
    /\ \A k \in Coordinators, i \in Instances, b \in Ballots :
          \A m \in StoredReplies(acceptReplies, k, i, b) :
              \A ev \in m.acceptEvidence : ev.sender = m.from
    /\ \A replies \in {prepareReplies, tryReplies}, k \in Coordinators,
          i \in Instances, b \in Ballots :
          \A m \in StoredReplies(replies, k, i, b) :
              \A ev \in m.acceptEvidence :
                  ev.sender \in Voters[InstanceConfig[ev.inst]]

EvidenceBallotAndConfigScoped ==
    \A replies \in AllReplyMaps, k \in Coordinators, i \in Instances,
       b \in Ballots :
        \A m \in StoredReplies(replies, k, i, b) :
            \A ev \in m.acceptEvidence :
                /\ ev.ballot \in Ballots
                /\ ev.conf = InstanceConfig[ev.inst]
                /\ ev.sender \in Voters[ev.conf]

FastCommitUsesCarriedEvidence ==
    \A h \in commitHistory :
        FastChosen(h.inst, h.tuple) /\ ~SlowChosen(h.inst, h.tuple) =>
          \E k \in Coordinators, q \in FastQuorums(InstanceConfig[h.inst]) :
            /\ q \subseteq MatchingReplySenders(preReplies, k, h.inst,
                                                 MinBallot, h.tuple)
            /\ \A d \in h.tuple.deps :
                 \E r \in q :
                    d \in preReplies[k][h.inst][MinBallot][r].fpDepsCommitted

RecoveryEvidenceDoesNotChangeChosenTuple ==
    /\ \A k \in Coordinators, i \in Instances, b \in Ballots :
          \A m \in StoredReplies(tryReplies, k, i, b) :
              m.tuple \in {NoTuple, coordCandidate[k][i]}
    /\ \A i \in Instances :
          \A t \in ChosenTuples(i), h \in acceptedHistory :
              h.inst = i /\ h.ballot # MinBallot => h.tuple = t

RecoveryUsesPinnedConfig ==
    /\ \A h \in preacceptedHistory : h.tuple.conf = InstanceConfig[h.inst] /\
                                      h.sender \in Voters[InstanceConfig[h.inst]]
    /\ \A h \in acceptedHistory : h.tuple.conf = InstanceConfig[h.inst] /\
                                   h.sender \in Voters[InstanceConfig[h.inst]]
    /\ \A h \in commitHistory : h.tuple.conf = InstanceConfig[h.inst] /\
                                 h.sender \in Voters[InstanceConfig[h.inst]]
    /\ \A k \in Coordinators, i \in Instances :
          coordCandidate[k][i] \in Tuples =>
              coordCandidate[k][i].conf = InstanceConfig[i]

NoCurrentConfigQuorumForOldInstance ==
    Mode = "config-race" =>
      /\ CurrentConfig # InstanceConfig[Target]
      /\ AddedCurrentVoters # {}
      /\ RemovedPinnedVoters # {}
      /\ \A replies \in AllReplyMaps, k \in Coordinators, b \in Ballots :
            ReplySenders(replies, k, Target, b) \cap AddedCurrentVoters = {}
      /\ \A h \in acceptedHistory :
            h.inst = Target => h.sender \notin AddedCurrentVoters

EvidenceDoesNotChangeTuple ==
    /\ \A k \in Coordinators, i \in Instances, b \in Ballots :
          \A m \in StoredReplies(tryReplies, k, i, b) :
              m.tuple = NoTuple \/ m.tuple = coordCandidate[k][i]
    /\ \A k \in Coordinators, i \in Instances, b \in Ballots :
          \A m \in StoredReplies(preReplies, k, i, b) :
              m.type = "EvidenceResp" =>
                /\ m.tuple = coordCandidate[k][i]
                /\ \E ev \in m.acceptEvidence :
                      ev.inst = i /\ ev.tuple = m.tuple

PreacceptedCandidateUnambiguous ==
    \A k \in Coordinators, i \in Instances, b \in Ballots :
        Cardinality(PreacceptedReplyCandidates(k, i, b)) <= 1

PinnedConfigQuorumShapes ==
    \A i \in Instances :
        LET normal == NormalSlowQuorum(InstanceConfig[i])
            recovery == RecoverySlowQuorum(InstanceConfig[i])
        IN /\ normal \subseteq Voters[InstanceConfig[i]]
           /\ recovery \subseteq Voters[InstanceConfig[i]]
           /\ Cardinality(normal) = SlowQuorumSize(InstanceConfig[i])
           /\ Cardinality(recovery) = SlowQuorumSize(InstanceConfig[i])
           /\ normal \cap recovery # {}
           /\ normal # recovery

Safety ==
    /\ Nontriviality
    /\ AcceptedValueWasProposed
    /\ LocalStability
    /\ ChosenStability
    /\ ChosenTupleAgreement
    /\ CommittedImpliesChosen
    /\ PromiseMonotonic
    /\ OneAcceptedTuplePerBallot
    /\ AcceptedBallotRespectsPromise
    /\ CurrentRoundResponsesOnly
    /\ UniqueResponseSender
    /\ PinnedConfigVotersOnly
    /\ ResponseEvidenceSound
    /\ EvidenceSenderAuthentic
    /\ EvidenceBallotAndConfigScoped
    /\ FastCommitUsesCarriedEvidence
    /\ RecoveryEvidenceDoesNotChangeChosenTuple
    /\ RecoveryUsesPinnedConfig
    /\ NoCurrentConfigQuorumForOldInstance
    /\ EvidenceDoesNotChangeTuple
    /\ PreacceptedCandidateUnambiguous
    /\ PinnedConfigQuorumShapes



AllTargetReplicasLearned ==
    \A r \in Voters[InstanceConfig[Target]] :
        \E t \in ChosenTuples(Target) : CommittedAt(r, Target, t)

FinalRecoveryCommitted ==
    IF Mode = "accept-evidence"
    THEN coordPhase[FirstCoordinator][Target] = "committed" /\
         coordBallot[FirstCoordinator][Target] > MinBallot
    ELSE coordPhase[LastCoordinator][Target] = "committed"

EventuallyTwoCoordinatorRaceCompletes ==
    <> (/\ "two-coordinator-race" \in raceSeen
        /\ RecoveryOrderChosen
        /\ Cardinality(ChosenTuples(Target)) = 1
        /\ FinalRecoveryCommitted
        /\ AllTargetReplicasLearned
        /\ "duplicate-commit-ignored" \in raceSeen)

EventuallyStaleRoundIgnored ==
    <> ("stale-response-ignored" \in raceSeen)

EventuallyEvidencePropagationExercised ==
    <> (/\ "accept-response-seen" \in raceSeen
        /\ "prepare-response-with-evidence-seen" \in raceSeen
        /\ ("evidence-response-seen" \in raceSeen \/
            "try-fail-closed" \in raceSeen)
        /\ FinalRecoveryCommitted
        /\ AllTargetReplicasLearned
        /\ "duplicate-commit-ignored" \in raceSeen)

EventuallyConfigRaceCompletes ==
    <> (/\ "two-coordinator-race" \in raceSeen
        /\ "removed-pinned-voter-counted" \in raceSeen
        /\ "added-current-voter-ignored" \in raceSeen
        /\ Cardinality(ChosenTuples(Target)) = 1
        /\ FinalRecoveryCommitted
        /\ AllTargetReplicasLearned
        /\ "duplicate-commit-ignored" \in raceSeen)

\* Configuration-file substitutions keep all finite scopes explicit.
SingleConfigVoters == [c \in Configs |-> Replicas]
SingleConfigInstances == [i \in Instances |-> CHOOSE c \in Configs : TRUE]
ConfigRaceVoters ==
    [c \in Configs |-> IF c = "Old" THEN {1, 2, 3} ELSE {1, 3, 4}]
OldConfigInstances == [i \in Instances |-> "Old"]

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

====
