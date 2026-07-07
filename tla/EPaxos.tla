---- MODULE EPaxos ----
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Replicas, InstanceNums, SeqNums, BallotNums, TickLimit, Commands, ConflictKeyACommands, ConflictKeyBCommands, None, PreAccepted, Accepted, Committed, Executed

VARIABLES status, ballot, seq, deps, command, messages, executed, tick

Instances == Replicas \X InstanceNums
Conflicts ==
    (ConflictKeyACommands \X ConflictKeyACommands) \cup
    (ConflictKeyBCommands \X ConflictKeyBCommands)



KnownConflicts(i, c) ==
    {j \in Instances : j # i /\ command[j] # None /\ <<c, command[j]>> \in Conflicts}

SafeDeps(i, c, d) == KnownConflicts(i, c) \subseteq d

TypeOK ==
    /\ status \in [Instances -> {None, PreAccepted, Accepted, Committed, Executed}]
    /\ ballot \in [Instances -> BallotNums \cup {0}]
    /\ seq \in [Instances -> SeqNums \cup {0}]
    /\ deps \in [Instances -> SUBSET Instances]
    /\ command \in [Instances -> Commands \cup {None}]
    /\ ConflictKeyACommands \subseteq Commands
    /\ ConflictKeyBCommands \subseteq Commands
    /\ messages \in SUBSET [type : {"preaccept", "preaccept_resp", "accept", "accept_resp", "commit", "prepare", "prepare_resp"}, from : Replicas, to : Replicas, inst : Instances]
    /\ executed \in SUBSET Instances
    /\ tick \in 0..TickLimit

Init ==
    /\ status = [i \in Instances |-> None]
    /\ ballot = [i \in Instances |-> 0]
    /\ seq = [i \in Instances |-> 0]
    /\ deps = [i \in Instances |-> {}]
    /\ command = [i \in Instances |-> None]
    /\ messages = {}
    /\ executed = {}
    /\ tick = 0

PreAccept(i, c, s, d) ==
    /\ SafeDeps(i, c, d)
    /\ status[i] \in {None, PreAccepted}
    /\ command' = [command EXCEPT ![i] = c]
    /\ status' = [status EXCEPT ![i] = PreAccepted]
    /\ seq' = [seq EXCEPT ![i] = s]
    /\ deps' = [deps EXCEPT ![i] = d]
    /\ ballot' = ballot
    /\ messages' = messages \cup {[type |-> "preaccept", from |-> i[1], to |-> r, inst |-> i] : r \in Replicas \ {i[1]}}
    /\ executed' = executed
    /\ tick' = tick

Accept(i, s, d) ==
    /\ deps[i] \subseteq d
    /\ status[i] \in {PreAccepted, Accepted}
    /\ status' = [status EXCEPT ![i] = Accepted]
    /\ seq' = [seq EXCEPT ![i] = s]
    /\ deps' = [deps EXCEPT ![i] = d]
    /\ UNCHANGED <<ballot, command, executed, tick>>
    /\ messages' = messages \cup {[type |-> "accept", from |-> i[1], to |-> r, inst |-> i] : r \in Replicas \ {i[1]}}

Commit(i) ==
    /\ status[i] \in {PreAccepted, Accepted, Committed}
    /\ status' = [status EXCEPT ![i] = Committed]
    /\ messages' = messages \cup {[type |-> "commit", from |-> i[1], to |-> r, inst |-> i] : r \in Replicas \ {i[1]}}
    /\ UNCHANGED <<ballot, seq, deps, command, executed, tick>>

Prepare(i) ==
    /\ status[i] \in {PreAccepted, Accepted}
    /\ ballot[i] + 1 \in BallotNums
    /\ ballot' = [ballot EXCEPT ![i] = ballot[i] + 1]
    /\ messages' = messages \cup {[type |-> "prepare", from |-> i[1], to |-> r, inst |-> i] : r \in Replicas \ {i[1]}}
    /\ UNCHANGED <<status, seq, deps, command, executed, tick>>

Execute(i) ==
    /\ status[i] = Committed
    /\ deps[i] \subseteq executed
    /\ executed' = executed \cup {i}
    /\ status' = [status EXCEPT ![i] = Executed]
    /\ UNCHANGED <<ballot, seq, deps, command, messages, tick>>

Tick ==
    /\ tick < TickLimit
    /\ tick' = tick + 1
    /\ UNCHANGED <<status, ballot, seq, deps, command, messages, executed>>

Next ==
    \/ \E i \in Instances, c \in Commands, s \in SeqNums, d \in SUBSET Instances : PreAccept(i, c, s, d)
    \/ \E i \in Instances, s \in SeqNums, d \in SUBSET Instances : Accept(i, s, d)
    \/ \E i \in Instances : Commit(i)
    \/ \E i \in Instances : Prepare(i)
    \/ \E i \in Instances : Execute(i)
    \/ Tick

DependencyClosure == \A i \in Instances : i \in executed => deps[i] \subseteq executed

ConflictOrder ==
    \A i, j \in Instances :
        (i # j /\ i \in executed /\ j \in executed /\
         command[i] # None /\ command[j] # None /\
         <<command[i], command[j]>> \in Conflicts)
        => (i \in deps[j] \/ j \in deps[i])

Safety == DependencyClosure /\ ConflictOrder

Spec == Init /\ [][Next]_<<status, ballot, seq, deps, command, messages, executed, tick>>

====
