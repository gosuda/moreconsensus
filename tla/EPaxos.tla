---- MODULE EPaxos ----
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Replicas, InstanceNums, SeqNums, BallotNums, TickLimit, Commands, ConflictKeyACommands, ConflictKeyBCommands, None, PreAccepted, Accepted, Committed, Executed

VARIABLES status, seq, deps, command, messages, executed, execLog, preOK, preMatch, preSeq, preDeps, accOK, tick

Instances == Replicas \X InstanceNums
Statuses == {None, PreAccepted, Accepted, Committed, Executed}
MessageTypes == {"preaccept", "preaccept_resp", "accept", "accept_resp", "commit"}
Vars == <<status, seq, deps, command, messages, executed, execLog, preOK, preMatch, preSeq, preDeps, accOK, tick>>
Orders == UNION {[1..n -> Instances] : n \in 0..Cardinality(Instances)}

Conflicts ==
    (ConflictKeyACommands \X ConflictKeyACommands) \cup
    (ConflictKeyBCommands \X ConflictKeyBCommands)

\* Repository model quorum: optimized EPaxos paper quorum for odd N=2F+1
\* (plus the single-node degenerate case); even sizes keep conservative quorum.
SlowQuorum == (Cardinality(Replicas) \div 2) + 1
FastQuorum ==
    LET n == Cardinality(Replicas)
    IN IF n \in {1, 3, 5, 7}
       THEN IF n = 1 THEN 1 ELSE (n \div 2) + (((n \div 2) + 1) \div 2)
       ELSE n - ((n - 1) \div 4)

Owner(i) == i[1]
Max2(a, b) == IF a >= b THEN a ELSE b
DefaultSeq == CHOOSE s \in SeqNums : TRUE

Message(t, from, to, i, c, s, d, match) ==
    [type |-> t, from |-> from, to |-> to, inst |-> i, cmd |-> c, seq |-> s, deps |-> d, match |-> match]

Broadcast(t, from, i, c, s, d) ==
    {Message(t, from, r, i, c, s, d, TRUE) : r \in Replicas \ {from}}

LocalKnownConflicts(r, i, c) ==
    {j \in Instances :
        j # i /\
        command[r][j] # None /\
        status[r][j] \in {PreAccepted, Accepted, Committed, Executed} /\
        <<c, command[r][j]>> \in Conflicts}


NeededSeqs(r, base, local) ==
    {base} \cup {seq[r][j] + 1 : j \in local}

AttrSeqDefined(r, base, local) ==
    \E s \in SeqNums : \A x \in NeededSeqs(r, base, local) : x <= s

AttrSeq(r, base, local) ==
    LET needed == NeededSeqs(r, base, local)
    IN CHOOSE s \in SeqNums :
        /\ \A x \in needed : x <= s
        /\ \A y \in SeqNums : ((\A x \in needed : x <= y) => s <= y)

SafeLocalDeps(r, i, c, d) == LocalKnownConflicts(r, i, c) \subseteq d

DepsCommittedByQuorum(q, d) ==
    \A dep \in d : \E r \in q : status[r][dep] \in {Committed, Executed}

TypeOK ==
    /\ Replicas # {}
    /\ status \in [Replicas -> [Instances -> Statuses]]
    /\ seq \in [Replicas -> [Instances -> SeqNums \cup {0}]]
    /\ deps \in [Replicas -> [Instances -> SUBSET Instances]]
    /\ command \in [Replicas -> [Instances -> Commands \cup {None}]]
    /\ messages \in SUBSET [type : MessageTypes, from : Replicas, to : Replicas, inst : Instances, cmd : Commands \cup {None}, seq : SeqNums \cup {0}, deps : SUBSET Instances, match : BOOLEAN]
    /\ executed \in [Replicas -> SUBSET Instances]
    /\ execLog \in [Replicas -> Orders]
    /\ \A r \in Replicas : executed[r] = {execLog[r][k] : k \in 1..Len(execLog[r])}
    /\ preOK \in [Instances -> SUBSET Replicas]
    /\ preMatch \in [Instances -> SUBSET Replicas]
    /\ \A i \in Instances : preMatch[i] \subseteq preOK[i]
    /\ preSeq \in [Instances -> SeqNums \cup {0}]
    /\ preDeps \in [Instances -> SUBSET Instances]
    /\ accOK \in [Instances -> SUBSET Replicas]
    /\ tick \in 0..TickLimit

Init ==
    /\ status = [r \in Replicas |-> [i \in Instances |-> None]]
    /\ seq = [r \in Replicas |-> [i \in Instances |-> 0]]
    /\ deps = [r \in Replicas |-> [i \in Instances |-> {}]]
    /\ command = [r \in Replicas |-> [i \in Instances |-> None]]
    /\ messages = {}
    /\ executed = [r \in Replicas |-> {}]
    /\ execLog = [r \in Replicas |-> <<>>]
    /\ preOK = [i \in Instances |-> {}]
    /\ preMatch = [i \in Instances |-> {}]
    /\ preSeq = [i \in Instances |-> 0]
    /\ preDeps = [i \in Instances |-> {}]
    /\ accOK = [i \in Instances |-> {}]
    /\ tick = 0

Propose(i, c) ==
    LET local == LocalKnownConflicts(Owner(i), i, c)
        s == AttrSeq(Owner(i), DefaultSeq, local)
        d == local
    IN
    /\ i[1] \in Replicas
    /\ status[Owner(i)][i] = None
    /\ AttrSeqDefined(Owner(i), DefaultSeq, local)
    /\ SafeLocalDeps(Owner(i), i, c, d)
    /\ status' = [status EXCEPT ![Owner(i)][i] = PreAccepted]
    /\ seq' = [seq EXCEPT ![Owner(i)][i] = s]
    /\ deps' = [deps EXCEPT ![Owner(i)][i] = d]
    /\ command' = [command EXCEPT ![Owner(i)][i] = c]
    /\ preOK' = [preOK EXCEPT ![i] = {Owner(i)}]
    /\ preMatch' = [preMatch EXCEPT ![i] = {Owner(i)}]
    /\ preSeq' = [preSeq EXCEPT ![i] = s]
    /\ preDeps' = [preDeps EXCEPT ![i] = d]
    /\ accOK' = accOK
    /\ messages' = messages \cup Broadcast("preaccept", Owner(i), i, c, s, d)
    /\ UNCHANGED <<executed, execLog, tick>>

ReceivePreAccept(m) ==
    LET local == LocalKnownConflicts(m.to, m.inst, m.cmd)
        s == AttrSeq(m.to, m.seq, local)
        d == m.deps \cup local
    IN
    /\ m \in messages
    /\ m.type = "preaccept"
    /\ status[m.to][m.inst] \in {None, PreAccepted}
    /\ AttrSeqDefined(m.to, m.seq, local)
    /\ SafeLocalDeps(m.to, m.inst, m.cmd, d)
    /\ status' = [status EXCEPT ![m.to][m.inst] = PreAccepted]
    /\ seq' = [seq EXCEPT ![m.to][m.inst] = s]
    /\ deps' = [deps EXCEPT ![m.to][m.inst] = d]
    /\ command' = [command EXCEPT ![m.to][m.inst] = m.cmd]
    /\ messages' = (messages \ {m}) \cup {Message("preaccept_resp", m.to, m.from, m.inst, m.cmd, s, d, s = m.seq /\ d = m.deps)}
    /\ UNCHANGED <<executed, execLog, preOK, preMatch, preSeq, preDeps, accOK, tick>>

ReceivePreAcceptResp(m) ==
    LET i == m.inst
        owner == Owner(m.inst)
        newOK == preOK[i] \cup {m.from}
        newMatch == IF m.match THEN preMatch[i] \cup {m.from} ELSE preMatch[i]
        newSeq == Max2(preSeq[i], m.seq)
        newDeps == preDeps[i] \cup m.deps
        fast == Cardinality(newOK) >= FastQuorum /\ newOK = newMatch /\ DepsCommittedByQuorum(newOK, newDeps)
        slow == Cardinality(newOK) >= SlowQuorum
    IN
    /\ m \in messages
    /\ m.type = "preaccept_resp"
    /\ m.to = owner
    /\ status[owner][i] = PreAccepted
    /\ m.from \notin preOK[i]
    /\ preOK' = [preOK EXCEPT ![i] = newOK]
    /\ preMatch' = [preMatch EXCEPT ![i] = newMatch]
    /\ preSeq' = [preSeq EXCEPT ![i] = newSeq]
    /\ preDeps' = [preDeps EXCEPT ![i] = newDeps]
    /\ IF fast
       THEN /\ status' = [status EXCEPT ![owner][i] = Committed]
            /\ seq' = [seq EXCEPT ![owner][i] = newSeq]
            /\ deps' = [deps EXCEPT ![owner][i] = newDeps]
            /\ command' = [command EXCEPT ![owner][i] = m.cmd]
            /\ accOK' = accOK
            /\ messages' = (messages \ {m}) \cup Broadcast("commit", owner, i, m.cmd, newSeq, newDeps)
       ELSE IF slow
            THEN /\ status' = [status EXCEPT ![owner][i] = Accepted]
                 /\ seq' = [seq EXCEPT ![owner][i] = newSeq]
                 /\ deps' = [deps EXCEPT ![owner][i] = newDeps]
                 /\ command' = [command EXCEPT ![owner][i] = m.cmd]
                 /\ accOK' = [accOK EXCEPT ![i] = {owner}]
                 /\ messages' = (messages \ {m}) \cup Broadcast("accept", owner, i, m.cmd, newSeq, newDeps)
            ELSE /\ UNCHANGED <<status, seq, deps, command, accOK>>
                 /\ messages' = messages \ {m}
    /\ UNCHANGED <<executed, execLog, tick>>

ReceiveAccept(m) ==
    /\ m \in messages
    /\ m.type = "accept"
    /\ status[m.to][m.inst] \in {None, PreAccepted, Accepted}
    /\ status' = [status EXCEPT ![m.to][m.inst] = Accepted]
    /\ seq' = [seq EXCEPT ![m.to][m.inst] = m.seq]
    /\ deps' = [deps EXCEPT ![m.to][m.inst] = m.deps]
    /\ command' = [command EXCEPT ![m.to][m.inst] = m.cmd]
    /\ messages' = (messages \ {m}) \cup {Message("accept_resp", m.to, m.from, m.inst, m.cmd, m.seq, m.deps, TRUE)}
    /\ UNCHANGED <<executed, execLog, preOK, preMatch, preSeq, preDeps, accOK, tick>>

ReceiveAcceptResp(m) ==
    LET i == m.inst
        owner == Owner(m.inst)
        newAccOK == accOK[i] \cup {m.from}
    IN
    /\ m \in messages
    /\ m.type = "accept_resp"
    /\ m.to = owner
    /\ status[owner][i] = Accepted
    /\ m.from \notin accOK[i]
    /\ accOK' = [accOK EXCEPT ![i] = newAccOK]
    /\ IF Cardinality(newAccOK) >= SlowQuorum
       THEN /\ status' = [status EXCEPT ![owner][i] = Committed]
            /\ messages' = (messages \ {m}) \cup Broadcast("commit", owner, i, m.cmd, m.seq, m.deps)
       ELSE /\ status' = status
            /\ messages' = messages \ {m}
    /\ UNCHANGED <<seq, deps, command, executed, execLog, preOK, preMatch, preSeq, preDeps, tick>>

ReceiveCommit(m) ==
    /\ m \in messages
    /\ m.type = "commit"
    /\ status[m.to][m.inst] \in {None, PreAccepted, Accepted, Committed}
    /\ status' = [status EXCEPT ![m.to][m.inst] = Committed]
    /\ seq' = [seq EXCEPT ![m.to][m.inst] = m.seq]
    /\ deps' = [deps EXCEPT ![m.to][m.inst] = m.deps]
    /\ command' = [command EXCEPT ![m.to][m.inst] = m.cmd]
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<executed, execLog, preOK, preMatch, preSeq, preDeps, accOK, tick>>

ReadyComponent(r, comp) ==
    /\ comp # {}
    /\ comp \subseteq {i \in Instances : status[r][i] = Committed}
    /\ \A i \in comp : deps[r][i] \subseteq executed[r] \cup comp

MinimalReadyComponent(r, comp) ==
    /\ ReadyComponent(r, comp)
    /\ \A sub \in SUBSET comp : sub # {} /\ sub # comp => ~ReadyComponent(r, sub)

SeqSet(order) == {order[k] : k \in 1..Len(order)}

RefLess(a, b) == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] < b[2])

PermutationOf(order, comp) ==
    /\ SeqSet(order) = comp
    /\ Len(order) = Cardinality(comp)

OrderedBySeqRef(r, order) ==
    \A p, q \in 1..Len(order) :
        p < q => seq[r][order[p]] < seq[r][order[q]] \/ (seq[r][order[p]] = seq[r][order[q]] /\ RefLess(order[p], order[q]))

ExecuteComponent(r, comp, order) ==
    /\ MinimalReadyComponent(r, comp)
    /\ PermutationOf(order, comp)
    /\ OrderedBySeqRef(r, order)
    /\ status' = [status EXCEPT ![r] = [i \in Instances |-> IF i \in comp THEN Executed ELSE status[r][i]]]
    /\ executed' = [executed EXCEPT ![r] = executed[r] \cup comp]
    /\ execLog' = [execLog EXCEPT ![r] = execLog[r] \o order]
    /\ UNCHANGED <<seq, deps, command, messages, preOK, preMatch, preSeq, preDeps, accOK, tick>>

Tick ==
    /\ tick < TickLimit
    /\ tick' = tick + 1
    /\ UNCHANGED <<status, seq, deps, command, messages, executed, execLog, preOK, preMatch, preSeq, preDeps, accOK>>

Next ==
    \/ \E i \in Instances, c \in Commands : Propose(i, c)
    \/ \E m \in messages : ReceivePreAccept(m)
    \/ \E m \in messages : ReceivePreAcceptResp(m)
    \/ \E m \in messages : ReceiveAccept(m)
    \/ \E m \in messages : ReceiveAcceptResp(m)
    \/ \E m \in messages : ReceiveCommit(m)
    \/ \E r \in Replicas, comp \in SUBSET Instances, order \in Orders : ExecuteComponent(r, comp, order)
    \/ Tick

DependencyClosure ==
    \A r \in Replicas, i \in Instances : i \in executed[r] => deps[r][i] \subseteq executed[r]

ConflictOrder ==
    \A r \in Replicas, i, j \in Instances :
        (i # j /\ i \in executed[r] /\ j \in executed[r] /\
         command[r][i] # None /\ command[r][j] # None /\
         <<command[r][i], command[r][j]>> \in Conflicts)
        => (i \in deps[r][j] \/ j \in deps[r][i])

OwnerCommitHasQuorumEvidence ==
    \A i \in Instances :
        status[Owner(i)][i] \in {Committed, Executed} =>
            \/ Cardinality(preOK[i]) >= FastQuorum /\ preOK[i] = preMatch[i]
            \/ Cardinality(accOK[i]) >= SlowQuorum

SlowAcceptHasQuorumEvidence ==
    \A i \in Instances :
        status[Owner(i)][i] = Accepted => Cardinality(preOK[i]) >= SlowQuorum

Safety == DependencyClosure /\ ConflictOrder /\ OwnerCommitHasQuorumEvidence /\ SlowAcceptHasQuorumEvidence

Spec == Init /\ [][Next]_Vars

====
