---- MODULE EPaxosCertifiedCompaction ----
EXTENDS Naturals, FiniteSets

\* Finite crash model for the certified checkpoint/compaction contract.  A
\* checkpoint names an exact per-lane executed and applied prefix.  Snapshot
\* durability precedes voter attestations; a quorum certificate precedes the
\* atomic tombstone/delete step.  Restart restores the snapshot and replays
\* only retained delta.  Late compacted and stale-incarnation traffic cannot
\* recreate deleted rows.

CONSTANTS Voters, Lanes, MaxI, Quorum, GoodIncarnation

Zero == [lane \in Lanes |-> 0]
Frontiers == [Lanes -> 0..MaxI]

VARIABLES executed, applied, applicationBase,
          checkpoint, snapshotDurable, snapshotApp,
          votes, certified, compacted, records, summary,
          crashed, restored, rejected

vars == <<executed, applied, applicationBase,
          checkpoint, snapshotDurable, snapshotApp,
          votes, certified, compacted, records, summary,
          crashed, restored, rejected>>

TypeOK ==
  /\ executed \in Frontiers
  /\ applied \in Frontiers
  /\ applicationBase \in Frontiers
  /\ checkpoint \in Frontiers
  /\ snapshotDurable \in BOOLEAN
  /\ snapshotApp \in Frontiers
  /\ votes \subseteq Voters
  /\ certified \in BOOLEAN
  /\ compacted \in Frontiers
  /\ records \in [Lanes -> SUBSET (1..MaxI)]
  /\ summary \in Frontiers
  /\ crashed \in BOOLEAN
  /\ restored \in BOOLEAN
  /\ rejected \in BOOLEAN

Init ==
  /\ executed = Zero
  /\ applied = Zero
  /\ applicationBase = Zero
  /\ checkpoint = Zero
  /\ snapshotDurable = FALSE
  /\ snapshotApp = Zero
  /\ votes = {}
  /\ certified = FALSE
  /\ compacted = Zero
  /\ records = [lane \in Lanes |-> {}]
  /\ summary = Zero
  /\ crashed = FALSE
  /\ restored = FALSE
  /\ rejected = FALSE

Execute(lane) ==
  /\ ~crashed
  /\ executed[lane] < MaxI
  /\ LET next == executed[lane] + 1 IN
       /\ executed' = [executed EXCEPT ![lane] = next]
       /\ records' = [records EXCEPT ![lane] = @ \union {next}]
  /\ UNCHANGED <<applied, applicationBase, checkpoint, snapshotDurable,
                  snapshotApp, votes, certified, compacted, summary,
                  crashed, restored, rejected>>

Apply(lane) ==
  /\ ~crashed
  /\ applied[lane] < executed[lane]
  /\ applied' = [applied EXCEPT ![lane] = @ + 1]
  /\ applicationBase' = [applicationBase EXCEPT ![lane] = @ + 1]
  /\ UNCHANGED <<executed, checkpoint, snapshotDurable, snapshotApp,
                  votes, certified, compacted, records, summary,
                  crashed, restored, rejected>>

ChooseCheckpoint(cut) ==
  /\ checkpoint = Zero
  /\ cut \in Frontiers
  /\ \A lane \in Lanes:
       /\ cut[lane] > 0
       /\ cut[lane] = executed[lane]
       /\ cut[lane] = applied[lane]
  /\ checkpoint' = cut
  /\ UNCHANGED <<executed, applied, applicationBase, snapshotDurable,
                  snapshotApp, votes, certified, compacted, records,
                  summary, crashed, restored, rejected>>

PersistSnapshot ==
  /\ checkpoint # Zero
  /\ ~snapshotDurable
  /\ snapshotDurable' = TRUE
  /\ snapshotApp' = checkpoint
  /\ UNCHANGED <<executed, applied, applicationBase, checkpoint, votes,
                  certified, compacted, records, summary, crashed,
                  restored, rejected>>

Vote(voter) ==
  /\ snapshotDurable
  /\ voter \in Voters \ votes
  /\ votes' = votes \union {voter}
  /\ UNCHANGED <<executed, applied, applicationBase, checkpoint,
                  snapshotDurable, snapshotApp, certified, compacted,
                  records, summary, crashed, restored, rejected>>

Certify ==
  /\ snapshotDurable
  /\ ~certified
  /\ Cardinality(votes) >= Quorum
  /\ certified' = TRUE
  /\ UNCHANGED <<executed, applied, applicationBase, checkpoint,
                  snapshotDurable, snapshotApp, votes, compacted,
                  records, summary, crashed, restored, rejected>>

Compact ==
  /\ certified
  /\ compacted # checkpoint
  /\ compacted' = checkpoint
  /\ summary' = checkpoint
  /\ records' = [lane \in Lanes |-> {i \in records[lane] : i > checkpoint[lane]}]
  /\ UNCHANGED <<executed, applied, applicationBase, checkpoint,
                  snapshotDurable, snapshotApp, votes, certified,
                  crashed, restored, rejected>>

Crash ==
  /\ ~crashed
  /\ crashed' = TRUE
  /\ restored' = FALSE
  /\ applicationBase' = Zero
  /\ UNCHANGED <<executed, applied, checkpoint, snapshotDurable,
                  snapshotApp, votes, certified, compacted, records,
                  summary, rejected>>

Restart ==
  /\ crashed
  /\ snapshotDurable
  /\ crashed' = FALSE
  /\ restored' = TRUE
  /\ applicationBase' = snapshotApp
  /\ UNCHANGED <<executed, applied, checkpoint, snapshotDurable,
                  snapshotApp, votes, certified, compacted, records,
                  summary, rejected>>

ReplayDelta(lane) ==
  /\ ~crashed
  /\ restored
  /\ applicationBase[lane] < applied[lane]
  /\ applicationBase' = [applicationBase EXCEPT ![lane] = @ + 1]
  /\ UNCHANGED <<executed, applied, checkpoint, snapshotDurable,
                  snapshotApp, votes, certified, compacted, records,
                  summary, crashed, restored, rejected>>

Receive(lane, instance, incarnation) ==
  /\ lane \in Lanes
  /\ instance \in 1..MaxI
  /\ IF incarnation # GoodIncarnation \/ instance <= compacted[lane]
       THEN /\ rejected' = TRUE
            /\ UNCHANGED records
       ELSE /\ records' = [records EXCEPT ![lane] = @ \union {instance}]
            /\ UNCHANGED rejected
  /\ UNCHANGED <<executed, applied, applicationBase, checkpoint,
                  snapshotDurable, snapshotApp, votes, certified,
                  compacted, summary, crashed, restored>>

Next ==
  \/ \E lane \in Lanes: Execute(lane)
  \/ \E lane \in Lanes: Apply(lane)
  \/ \E cut \in Frontiers: ChooseCheckpoint(cut)
  \/ PersistSnapshot
  \/ \E voter \in Voters: Vote(voter)
  \/ Certify
  \/ Compact
  \/ Crash
  \/ Restart
  \/ \E lane \in Lanes: ReplayDelta(lane)
  \/ \E lane \in Lanes, instance \in 1..MaxI,
       incarnation \in {GoodIncarnation, GoodIncarnation + 1}:
       Receive(lane, instance, incarnation)

Spec == Init /\ [][Next]_vars

CheckpointSafety ==
  /\ TypeOK
  /\ \A lane \in Lanes:
       /\ applied[lane] <= executed[lane]
       /\ applicationBase[lane] <= applied[lane]
       /\ compacted[lane] <= checkpoint[lane]
       /\ summary[lane] = compacted[lane]
       /\ \A i \in records[lane]: i > compacted[lane]
  /\ snapshotDurable => snapshotApp = checkpoint
  /\ certified => (snapshotDurable /\ Cardinality(votes) >= Quorum)
  /\ compacted # Zero => certified
  /\ restored => snapshotDurable

====
