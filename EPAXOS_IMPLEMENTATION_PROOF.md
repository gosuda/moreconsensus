# EPaxos Implementation Proof and Claim Rationale

This document explains the EPaxos and EPaxos Revisited behavior implemented in this repository, why each stated property is expected to hold inside the documented fault envelope, and which evidence currently supports the claim. It is intentionally proof-oriented. It does not replace `RELEASE_SCOPE.md`: the current release decision remains **no-go** until the open release items there have current evidence.

## 1. Proof status and limits

The word "proof" in this repository means a combination of:

1. source invariants in the Go implementation;
2. finite TLC model checks for focused protocol slices;
3. deterministic simulation tests, including deterministic state-transition (DST) scenarios;
4. Go unit, fuzz, stress, and coverage gates; and
5. local Jepsen harness evidence plus optional external validation tooling where separately exercised.

This is not an unbounded mathematical proof of every possible EPaxos execution. The finite TLA+ models cover configured bounded state spaces. The current repository still names open non-claims: full technical-report optimized-recovery decision-tree parity, even-size optimized-quorum proof, operational synchronized-clock/one-way-delay implementation for TOQ deployments, arbitrary membership-change proof, in-place disk-corruption repair or synthesized reconstruction without a verified checkpoint, target-environment mixed-version compatibility beyond the local loopback drill, target-environment capacity proof, and incident-drill evidence.

## 2. Primary paper obligations checked

The implementation was checked against these primary references:

- Moraru, Andersen, and Kaminsky, **"There Is More Consensus in Egalitarian Parliaments"**, SOSP 2013. Relevant obligations:
  - Section 4.1: asynchronous messages, non-Byzantine failures, `N = 2F + 1`, per-replica instance spaces, and commit/execution separation.
  - Section 4.2: nontriviality, stability, consistency, execution consistency, execution linearizability, and liveness with fewer than half the replicas faulty.
  - Section 4.3: PreAccept, Accept, Commit, explicit Prepare recovery, dependency graph execution, SCC ordering, and sequence-number tie breaking.
  - Section 4.4: optimized fast quorum `F + floor((F + 1) / 2)` for odd `N = 2F + 1`, slow quorum `F + 1`, optimized recovery through TryPreAccept, and the additional dependency-evidence obligation for optimized recovery.
  - Section 4.5: compact dependency lists by highest instance per replica.
  - Section 4.6: failure recovery by preparing unfinished instances, including no-op finalization when no command is known.
  - Section 4.7: reconfiguration is a separate hard problem; this repository implements a bounded raft-like configuration-command subset and names its limits.
  - Section 4.9: dependency-chain/livelock concern.
- Moraru, Andersen, and Kaminsky, **"A Proof of Correctness for Egalitarian Paxos"**, CMU-PDL-13-111, August 2013. Relevant obligations:
  - Section 4: formal guarantee definitions.
  - Section 5: simplified EPaxos proof structure for safe tuples, execution consistency, execution linearizability, and liveness.
  - Section 6.1: preferred optimized fast-path quorums, `FP-deps-committed`, and `Accept-Deps` as an alternative evidence path for `F <= 3`.
  - Section 6.2 and Figure 2: optimized recovery branch structure, TryPreAccept conflict checks, and deferred-cycle recovery reasoning.
  - Section 7: linearizability for transitive interference and the stronger EPaxos-strict condition for non-transitive multi-object strict serializability.
- Tollman, Park, and Ousterhout, **"EPaxos Revisited"**, NSDI 2021. Relevant obligations:
  - Section 2.1: dependencies are the durable ordering object; a committed operation is durable before execution order is necessarily known.
  - Section 2.2: execution uses dependency graphs, transitive dependencies, SCCs, reverse topological order, and deterministic order within an SCC.
  - Section 3: unbounded dependency chains are reduced by pruning a dependency `B` from `A` when `B` already depends on `A` and `B` has a higher sequence number.
  - Section 4.1: TOQ uses synchronized clocks, `ProcessAt`, delayed receiver processing, delayed originator dependency assignment, and outbound PreAccepts with sequence number `0` and empty dependencies.
  - Section 4.2: sync groups trade conflict reduction against minimum latency.

## 3. Implementation object model

### 3.1 Replica and instance identity

The public core is `epaxos.RawNode`. An instance is named by `InstanceRef{Replica, Instance, Conf}`. The `Replica` owns the instance-number stream, `Instance` is monotonically allocated by the owner, and `Conf` pins the voter configuration used to interpret dependency-vector slots.

Why this matters:

- EPaxos safety is per instance: at most one tuple `(command, seq, deps)` may be committed for `Replica.Instance@Conf`.
- `Conf` pinning prevents a later membership change from changing the quorum or dependency-vector meaning for an old in-flight instance.
- Sorted `ConfState.Voters` gives every node the same dependency-vector slot mapping.

Source anchors: `epaxos/types.go` (`InstanceRef`, `ConfState`, `InstanceRecord`), `epaxos/node.go` (`NewRawNode`, `confFor`, `depsForConf`, `applyConfChange`).

### 3.2 Commands and conflict relation

A `Command` is opaque payload plus exact-byte `ConflictKeys`. Two user commands conflict if any conflict key is byte-identical. `CommandNoop` conflicts with nothing. `CommandConfChange` conflicts with every non-noop command.

Why this matters:

- EPaxos only needs to order interfering commands consistently. Non-conflicting commands may execute in different relative positions without changing the replicated state.
- Exact-byte keys make the conflict relation deterministic and testable.
- Applications that need range, prefix, predicate, or transaction-wide conflict semantics must encode sentinel keys themselves; the core does not infer application predicates.

Source anchors: `epaxos/types.go` (`Command`, `ConflictsWith`), `epaxos/node.go` (`computeAttrsAt`, `indexConflicts`).

### 3.3 Durable statuses

`InstanceRecord.Status` progresses through:

1. `StatusNone` for unknown or initial recovery state;
2. `StatusPreAccepted` after PreAccept attributes are durably recorded;
3. `StatusAccepted` after the slow path accepts final attributes;
4. `StatusCommitted` after the tuple is chosen;
5. `StatusExecuted` after the command has been emitted or internally applied.

The implementation treats persisted `InstanceRecord` values as the durable consensus state. Checksums cover status, ballot, command, sequence, dependencies, Accept-Deps recovery evidence, fast-path marker, TOQ metadata, and command bytes.

Why this matters:

- Stability depends on durable records not silently changing after commit.
- Recovery can choose safe values only from persisted evidence.
- Checksums make corrupt durable records fail closed rather than being replayed as protocol facts.

Source anchors: `epaxos/types.go` (`Status`, `InstanceRecord`), `epaxos/checksum.go`, `epaxos/storage.go`.

## 4. Quorum and failure-count claims

The implementation supports cluster sizes 1 through 7 exactly. The progress claim is built from the majority slow quorum, not from the fast quorum. A cluster can make progress only when a slow quorum of voters is live, reachable, and able to durably persist required `Ready.Records`.

| Voters `N` | Slow quorum | Current fast quorum | Progress tolerated crash/omission count `N - slowQuorum` | No-quorum fail-closed boundary |
| ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 1 | 0 | 1 unavailable voter means no progress. |
| 2 | 2 | 2 | 0 | 1 unavailable voter means no progress. |
| 3 | 2 | 2 | 1 | 2 unavailable voters mean no new quorum decision. |
| 4 | 3 | 4 | 1 | 2 unavailable voters mean no new quorum decision. |
| 5 | 3 | 3 | 2 | 3 unavailable voters mean no new quorum decision. |
| 6 | 4 | 5 | 2 | 3 unavailable voters mean no new quorum decision. |
| 7 | 4 | 5 | 3 | 4 unavailable voters mean no new quorum decision. |

Important boundaries:

- **Crash/omission/network progress claim:** if at most `N - slowQuorum` voters are unavailable and messages among the remaining slow quorum are eventually delivered before retry/recovery timeouts, committed user commands can make progress and recovered dependencies can close.
- **Fail-closed safety claim:** if no slow quorum is available, the core must not invent a committed application output. It may keep durable preaccepted or accepted state, retry, and later finish after heal.
- **Storage failure claim:** progress requires a slow quorum with working durable storage. A node whose storage rejects `Ready.Records` must not `Advance` that `Ready`, must not make the failed batch invisible, and must not apply commands from that failed batch. This is not a claim that arbitrary destructive data loss can be reconstructed without trusted quorum state or a verified backup.
- **Fast quorum claim:** odd sizes use the EPaxos optimized paper threshold (`3 -> 2`, `5 -> 3`, `7 -> 5`) plus dependency evidence; even sizes intentionally keep conservative fast quorums because the optimized proof assumes odd `N = 2F + 1`.

Source anchors: `epaxos/quorum.go` (`SlowQuorum`, `FastQuorum`, `TryWitnessQuorum`), `epaxos/sim_test.go` (`TestClusterSizesOneThroughSevenCommit`), `tla/Quorum.tla`.

## 5. Normal proposal path

### 5.1 Non-TOQ proposal

For ordinary EPaxos mode, `RawNode.Propose` clones or accepts ownership of the command, allocates the next local instance, computes local attributes, persists `StatusPreAccepted`, records a local preaccept vote, indexes conflicts, and broadcasts `MsgPreAccept`.

The attribute computation is:

- `Deps[i]` is the largest known conflicting instance number for voter slot `i` in the same pinned configuration.
- `Seq` is one greater than the maximum sequence number among dependencies.
- Configuration commands are included as barriers even when user conflict keys are disjoint.

Why this matches the paper:

- SOSP 2013 Section 4.3 says the command leader attaches `deps` for known interfering instances and `seq` greater than the sequence numbers of those dependencies.
- CMU-PDL-13-111 Section 5.1 defines the same `deps` and `seq` obligations.
- Section 4.5 permits compact dependency lists by highest instance per replica; this implementation uses per-replica prefix dependencies.

Source anchors: `epaxos/node.go` (`Propose`, `propose`, `computeAttrsAt`, `indexConflicts`).

### 5.2 Receiver PreAccept handling

A receiver validates the message envelope and checksum if present, computes its own local attributes, merges proposer attributes for non-TOQ PreAccepts, persists `StatusPreAccepted`, indexes the command, marks configuration barriers, and replies with `MsgPreAcceptResp` including:

- the receiver's sequence/dependency attributes;
- whether the tuple is fast-path eligible; and
- `DepsCommitted`, a bitset proving which dependency prefixes are durably committed or executed at the receiver.

Why this matters:

- A fast commit must be based on a quorum that records the same tuple or a safe tuple under the TOQ/deterministic timing rule.
- `DepsCommitted` implements the paper/TR `FP-deps-committed` evidence obligation for optimized fast quorum safety.

Source anchors: `epaxos/node.go` (`handlePreAccept`, `committedDepsMask`, `committedPrefix`), `epaxos/message.go` (`DepsCommitted`).

## 6. Fast path

The owner may commit after PreAccept when `canFastCommitPreAccept` finds a matching fast quorum and every dependency slot in the candidate attributes is covered by committed-prefix evidence.

The fast-path proof obligation is:

1. A matching quorum has recorded the same command and attributes, or in TOQ mode a safely greater quorum tuple after delayed owner assignment.
2. The fast quorum intersects every possible later recovery slow quorum enough for recovery to discover the candidate.
3. Every dependency prefix used by optimized fast commit has durable committed/executed evidence, or recovery-only Accept-Deps evidence is available for focused optimized recovery cases.
4. The tuple is persisted before the commit message is exposed through `Ready.Messages`.

Why this is safe inside the implemented envelope:

- If a later recovery starts, it must collect a slow quorum of `PrepareResp` values.
- For odd sizes, the implemented fast quorum matches the optimized EPaxos formula from SOSP 2013 Section 4.4 and CMU-PDL-13-111 Section 6.1.
- `FP-deps-committed` evidence prevents a stale dependency from later changing sequence in a way that would break the recovered tuple.
- Even sizes do not use the optimized odd-size threshold; they keep a conservative fast quorum and therefore do not claim the odd-size proof for even `N`.

Source anchors: `epaxos/quorum.go`, `epaxos/node.go` (`maybeFinalizePreAccept`, `fastCommitAttrsPreAccept`, `canFastCommitPreAccept`, `committedDepsMask`), `tla/EPaxos.tla`, `tla/EPaxosResponses.tla`, `tla/Quorum.tla`.

## 7. Slow accept path

If the fast path does not close after a slow quorum of PreAccept responses, the owner merges response attributes, persists `StatusAccepted`, broadcasts `MsgAccept`, and commits only after a slow quorum of current-ballot `MsgAcceptResp` votes. A non-reject `MsgAcceptResp` from a lower or higher ballot is ignored before it can enter `accOK` or merge recovery-only Accept-Deps evidence.

Why this is safe:

- The slow path is classic Paxos accept over the tuple `(command, seq, deps)` for a single instance and a single accept ballot.
- Slow quorums intersect; a future prepare quorum must see accepted or committed evidence for the chosen tuple.
- Accept receivers persist the tuple before replying.
- `MsgAccept` and current-ballot `MsgAcceptResp` carry separate `AcceptSeq`/`AcceptDeps` recovery evidence; this evidence helps recovery but does not mutate chosen execution attributes.
- Stale or future-ballot non-reject AcceptOKs are not evidence for the coordinator's current accept round and therefore cannot satisfy the current slow quorum.

Source anchors: `epaxos/node.go` (`startAccept`, `handleAccept`, `handleAcceptResp`, `mergeAcceptEvidence`, `acceptEvidenceFromMessage`), `epaxos/message.go` (`AcceptSeq`, `AcceptDeps`), `epaxos/branch_test.go` (`TestAcceptRespIgnoresStaleBallotForCurrentAcceptRound`), `tla/EPaxosResponses.tla`.

## 8. Recovery path

A node starts recovery when it owns an unfinished instance, coordinates a foreign dependency by deterministic recovery assignment, or sees missing dependency prefixes that block execution.

The prepare branch order is:

1. committed evidence wins immediately;
2. accepted evidence chooses the highest durable previous-record-ballot accepted tuple and continues through accept without unioning lower preaccepted attributes;
3. compatible fast-path-eligible preaccepted evidence may enter TryPreAccept if it meets `fast + slow - N` witness threshold;
4. other preaccepted evidence falls back to accept with merged attributes;
5. all-none evidence chooses a no-op through accept.

TryPreAccept rejects stale ballots, committed conflicts, mutually unordered uncommitted conflicts, and stale-dependency conflicts. `AcceptDeps` is recovery-only: sender-preserving evidence can waive only the committed stale-dependency rejection when the candidate already depends on the conflicting instance and `F` unique read-only evidence responses for the committed tuple contain no forced-fast-quorum sender whose Accept/AcceptReply deps omit the candidate; chosen `Seq`/`Deps` still provide the execution edge. Evidence checks are scoped by target ref, conflict ref, and the live TryPreAccept ballot, so older-ballot `MsgEvidenceResp` messages are discarded before they can populate sender records, while same-ballot duplicate sender responses keep the first record. Conflicting duplicate, stale-duplicate, legacy-only, missing tuple, timed-out, or multiple simultaneous ignore evidence fails closed to slow accept with refreshed local attributes and the committed conflict sequence when known. An uncommitted conflict can force slow accept when leader-in-fast-quorum or recorded deferral-cycle evidence says the candidate cannot safely continue TryPreAccept; otherwise recovery records a deferral and recovers the blocker.

Sender-preserving `AcceptEvidence` has separate outbound and inbound contracts. Outbound merge skips sender-zero entries, coalesces same-sender evidence by `mergeAttrs` (`max Seq`, per-slot max `Deps`), and preserves one tuple per sender. Inbound validation remains wire-facing: identical duplicate sender evidence is accepted as idempotent input, while conflicting same-sender evidence is rejected as malformed.

The TryPreAccept retry timer preserves that fail-closed boundary. On a logical `timerTryPreAccept`, same-target current-ballot pending evidence is resolved by slow `MsgAccept` before any retry send. Same-target stale-ballot evidence is deleted and unrelated-target evidence is left alone, so neither can fail the current recovery. A retry emits either normal TryPreAccept messages or ignore-marker TryPreAccept messages, reschedules the logical retry timer, and does not emit durable or application effects on the pure retry path.

Why this is safe inside the implemented envelope:

- A committed record is already stable evidence.
- An accepted record has classic Paxos safety because any later slow quorum intersects the accept quorum.
- TryPreAccept only tries to preserve a plausible fast-path tuple when enough fast witnesses exist.
- Conflicting local state can veto TryPreAccept unless chosen dependency evidence orders the conflict, or sender-preserving recovery-only `AcceptEvidence` proves a committed stale-dependency ignore is safe after that chosen dependency edge already exists.
- No-op recovery for all-none prepare quorums is safe because no quorum member knows a proposed command for that instance.

Rollback/restart catch-up also protects per-replica local instance allocation. `NewRawNode`, commit/pre-accept/accept/prepare, and dependency-recovery paths call `observeInstanceRef` for observed refs, so learning an own local ref from quorum advances `nextInstance`; `propose` additionally skips any already materialized local ref if `nextInstance` is stale. `tla/EPaxosRollbackAllocation.tla` checks a finite rollback sequence with a checkpoint before local instance 2, quorum-learned local instance 2, a defensive known future local instance 3 while `nextInstance` is stale, fresh allocation at 4, and an apply sequence where learned instance 2 precedes fresh instance 4. `TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit` checks the Go path and asserts applied order for all replicas after catch-up.

Open limitation:

- CMU-PDL-13-111 Section 6.2 contains a full optimized-recovery decision tree. This repository implements the Go committed stale-dependency evidence-search/resend-ignore path for supported `F <= 3` configurations, ballot-scoped evidence response handling, durable value-ballot recovery evidence, targeted leader-in-fast-quorum/deferred-cycle handling, and finite TLA coverage for the committed-conflict evidence-query plus evidence-staleness guard/fail-closed slices. Complete optimized-recovery decision-tree coverage in TLA and unbounded proof remain non-claims.
- `tla/EPaxosTryPreAcceptRetry.tla` adds a finite logical timer slice for the TryPreAccept retry path after pending-evidence checks, including current-evidence fail-closed timeout, stale-evidence cleanup, unrelated-evidence retention, normal-vs-ignore rebroadcast separation, and no durable/application effects on pure retry. It does not model arbitrary network delivery, message loss, complete optimized-recovery branch parity, or unbounded proof.
- `tla/EPaxosAcceptEvidenceMerge.tla` adds one finite sender-evidence merge/validation slice for zero-sender skip, same-sender merge, distinct-sender append, identical duplicate acceptance, and conflicting duplicate rejection. It does not model arbitrary evidence histories, message delivery, recovery branch choices, or unbounded proof.

Source anchors: `epaxos/node.go` (`startPrepare`, `handlePrepare`, `handlePrepareResp`, `handleEvidence`, `handleEvidenceResp`, `startTryPreAccept`, `handleTryPreAccept`, `handleTryPreAcceptResp`, `tryPreAcceptConflict`, `ensureDependencyRecovery`, `observeInstanceRef`, `propose`), `epaxos/protocol_coverage_test.go` (`TestEvidenceStaleDuplicateCommittedTupleFallsBackToSlowAccept`), `epaxos/recovery_test.go` (`TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit`), `tla/EPaxosResponses.tla`, `tla/EPaxosRecovery.tla`, `tla/EPaxosRollbackAllocation.tla`, `tla/EPaxosOptimizedRecovery.tla`, `tla/EPaxosEvidenceQuery.tla`, `tla/EPaxosEvidenceStaleness.tla`.

Additional source anchors for finite TryPreAccept retry coverage: `tla/EPaxosTryPreAcceptRetry.tla`, `tla/EPaxosTryPreAcceptRetry.cfg`, and `epaxos/protocol_coverage_test.go` (`TestTryPreAcceptTimerDropsStaleEvidenceChecksBeforeRetry`).

Additional source anchors for finite AcceptEvidence sender-merge coverage: `tla/EPaxosAcceptEvidenceMerge.tla`, `tla/EPaxosAcceptEvidenceMerge.cfg`, and `epaxos/protocol_coverage_test.go` (`TestSenderPreservingEvidenceValidationAndMergeContracts`, `TestCodecRejectsMalformedSenderEvidenceWireFrames`).

## 9. Execution path

Committed records are not emitted to the application immediately. `tryExecute` builds dependency components over committed but unexecuted instances, waits for missing dependencies to commit or recover, rejects execution while known conflicting preaccepted/accepted commands are unresolved, and emits ready components in deterministic `(Seq, ReplicaID, InstanceNum)` order.

Why this matches EPaxos:

- SOSP 2013 Section 4.3.2 and EPaxos Revisited Section 2.2 require dependency graph construction, SCC condensation, reverse topological order, and deterministic order inside an SCC.
- Dependency vectors represent per-replica prefixes, so `dependencyRefs` expands slot `k` into instances `1..k` for that replica.
- `componentReady` requires every outside dependency to be executed, committed, or recoverable before emission.

Why conflicting commands execute consistently:

1. For any two committed conflicting commands, at least one must depend on the other after PreAccept/Accept/recovery quorum processing.
2. If each depends on the other, every replica places them in the same SCC and sorts them deterministically by sequence and reference.
3. If only one depends on the other, topological execution puts the dependency first.
4. Stable committed attributes mean every replica sees the same dependency relation once it has the relevant committed records.

Source anchors: `epaxos/node.go` (`tryExecute`, `executionComponents`, `componentReady`, `dependencyRefs`, `hasUnresolvedKnownConflict`), `tla/EPaxos.tla`.

## 10. EPaxos Revisited chain pruning

EPaxos Revisited Section 3 observes that if instance `B` depends on `A` and `B` has a higher sequence number, then `B` must execute after `A`; therefore `A` need not wait for `B` and all of `B`'s future transitive dependencies. The implementation applies this as `dependencyKnownAfter` during SCC construction and readiness checks.

Why this is safe:

- The pruning condition uses committed or at-least-preaccepted evidence, not speculative unknown state.
- `B.Seq > A.Seq` plus `B.Deps[A.Replica] >= A.Instance` is exactly the ordering witness: if `A` and `B` are in the same component, sequence order puts `A` before `B`; if they are in different components, the dependency edge still orders `A` before `B`.
- Pruning removes a wait edge from `A` to `B`; it does not remove the protocol fact that `B` must not execute before its own dependencies are ready.

Source anchors: `epaxos/node.go` (`dependencyKnownAfter`, `executionComponents`, `componentReady`), `epaxos/revisited_test.go`, `tla/EPaxosRevisited.tla`.

## 11. EPaxos Revisited TOQ core

`Config.TOQ` is an explicit mode separate from the older deterministic `TimeOptimization` heuristic. In TOQ mode:

1. the embedder supplies `TOQClock`, `TOQOneWayDelay`, and optionally `TOQSyncGroup`;
2. each `TOQOneWayDelay[id]` is a conservative delivery bound that includes measured one-way network delay plus maximum clock-skew/synchronization uncertainty for that receiver and sync group;
3. the owner computes `ProcessAt = TOQClock() + max(conservative delay bound over sync group)`;
4. the owner persists a `StatusNone` record with `TOQPending=true`, command bytes, and `ProcessAt`;
5. outbound PreAccept messages are flagged `TOQ=true` and intentionally carry `Seq=0` and no dependencies;
6. the owner also queues a local TOQ PreAccept and delays its own dependency assignment until `ProcessAt`;
7. receivers queue future TOQ PreAccepts and compute local attributes at the message's `ProcessAt`;
8. finalization is blocked while `TOQPending` is true;
9. retries preserve the explicit TOQ envelope.

Why this preserves EPaxos safety:

- TOQ changes when dependency information is sampled; it does not remove quorum persistence or the slow path.
- The owner cannot commit until its delayed local assignment is durably cleared.
- A receiver computes attributes at `ProcessAt` and persists them before replying.
- If TOQ fails to align processing order, divergent attributes still fall back to slow accept.
- Message validation rejects TOQ PreAccepts that carry nonzero `Seq` or nonempty dependencies.

What TOQ does not prove:

- The core does not synchronize clocks.
- The core does not measure one-way delay.
- The core does not construct operational sync groups.
- `tla/TOQClockDiscipline.tla` checks only a finite contract: if skew and one-way delay are within configured bounds, the chosen `ProcessAt` is late enough for sync-group delivery before local processing.
- The Go core treats `TOQOneWayDelay` as already containing the skew margin modeled by `TOQClockDiscipline`; there is no separate `TOQSkewBound` field.

Source anchors: `epaxos/types.go` (`Config.TOQ`, `TOQClock`, `TOQOneWayDelay`, `TOQSyncGroup`, `TOQPending`, `ProcessAt`), `epaxos/node.go` (`configureTOQ`, `nextTOQProcessAt`, `localTOQPreAcceptMessage`, `propose`, `processDuePreAccepts`, `handleLocalTOQPreAccept`, `broadcastPreAccept`, `handlePreAccept`), `epaxos/message.go` (`TOQ` validation), `epaxos/toq_test.go`, `tla/EPaxosRevisited.tla`, `tla/TOQClockDiscipline.tla`.

## 12. Ready, durability, and crash idempotence

`Ready` is the only way protocol work leaves `RawNode`:

- `Ready.Records` are durable consensus records.
- `Ready.Messages` are transport messages.
- `Ready.Committed` are application commands that have passed dependency execution.
- `Ready.MustSync` is true when durable records are present.

The required order is records first, then messages and application effects, then `Advance`. `Advance` accepts only an exact prefix of the outstanding `Ready`. If storage or application work fails before `Advance`, the same outstanding batch remains visible for retry.

Why this prevents unsafe visibility:

- A message that advertises a protocol fact is not acknowledged before the backing record is durably accepted by the embedder.
- Application commands are not marked executed until the caller acknowledges them through `Advance`.
- A partial record-only `Advance` is allowed, but messages or committed commands require all earlier records from the same batch.
- Invalid `Advance` arguments leave the outstanding batch unchanged.

Example KV idempotence:

- The KV example writes an applied marker keyed by consensus instance in the same Pebble batch as the application mutation.
- Replaying a committed command after crash observes the applied marker and does not create a second logical application effect.

Source anchors: `epaxos/node.go` (`Ready`, `Advance`, `validateReadyAck`, `enqueueExecutedRecords`), `epaxos/storage.go` (`ApplyReady`), `examples/kv/epaxos_storage.go`, `tla/ReadyAdvance.tla`.

## 13. Storage failure and corruption behavior

The core storage contract is fail-closed:

- `MemoryStorage.ApplyReady` returns an error when writes are configured to fail.
- `RawNode` does not call `Advance` on behalf of the embedder.
- If the embedder cannot persist `Ready.Records`, it must not send the batch's messages or apply its committed commands.
- `LoadInstances` verifies record checksums and returns `ErrChecksumMismatch` on corruption.

The KV example extends this with Pebble-backed checksums, semantic checkpoint verification, offline checkpoint-backed replacement, and live-source checkpoint replacement for one stopped/corrupt member while a healthy quorum remains. It does not claim in-place Pebble/WAL surgery, checksum recomputation, corrupt-record deletion, synthesized reconstruction without a verified checkpoint, multi-replica/quorum-loss recovery, or repair after arbitrary destructive data loss.

Source anchors: `epaxos/storage.go`, `epaxos/checksum.go`, `examples/kv/backup.go`, `examples/kv/cluster.go`, `examples/kv/epaxos_storage.go`, `examples/kv/more_test.go`.

## 14. Configuration-change ordering

Configuration changes are encoded as commands and use the same dependency machinery:

- local proposals are rejected while an observed configuration command is pending;
- configuration commands conflict with every non-noop command;
- user commands include known unexecuted configuration commands as barriers;
- executing a configuration command installs the next `ConfState` for future instances;
- old in-flight instances remain pinned to the `ConfID` they were created under;
- replicas that have applied a configuration excluding themselves reject local `Propose` and `ProposeConfChange` before allocating an instance.

Why this is safe inside the bounded claim:

- User commands cannot bypass an observed unexecuted membership command.
- Quorum membership for an instance is stable because `Ref.Conf` selects the historical configuration.
- Later instances use the successor configuration only after the configuration command executes.

Finite replay coverage:

- `NewRawNode` loads durable instance records, remembers stored configuration states, replays executed configuration-change records in instance order, reconstructs intermediate `ConfID` domains needed by old in-flight records, and rejects a stored historical `ConfID` if its voters conflict with the voters deterministically produced by replayed commands. A replayed unexecuted configuration command remains a proposal barrier. TOQ configuration is validated after replay, so the default sync group uses the replayed current voters and explicit stale sync groups fail closed.
- Recovery for an old pinned instance uses `Ref.Conf` to choose prepare/accept voters and slow-quorum thresholds. `tla/EPaxosConfigRecovery.tla` checks a finite staged removal case where the current config is `{1,2,3}`, the old instance remains pinned to `{1,2,3,4}`, removed voter 4 counts only for the old prepare/accept quorum, the smaller current quorum is insufficient for that old instance, and removed-voter current-config proposals are rejected.
- `tla/EPaxosConfigRecoveryDedup.tla` checks a finite staged de-duplication case for that removal recovery path: one lost prepare/accept response and one duplicate prepare/accept response leave the old quorum below 3/4, and a distinct removed-voter response is still required. The matching Go regression keeps the recovered no-op out of application output.
- `tla/EPaxosConfigAddRecovery.tla` checks the complementary finite staged add case where the current config is `{1,2,3,4}`, the old instance remains pinned to `{1,2,3}`, added voter 4 attempts prepare/accept responses without entering the old quorum sets, the old 2/3 quorum is sufficient even though the current quorum would be 3/4, and the recovered no-op executes.
- `tla/EPaxosConfigChainRecovery.tla` checks a finite mid-chain recovery case after `{1,2,3}` adds voter 4 and then removes voter 2: an instance pinned to Conf2 `{1,2,3,4}` remains in prepare/accept when only current Conf3 quorum responses are present, advances when removed voter 2 supplies the modeled third old-quorum vote, and rejects removed-voter new-config proposals. The matching Go regression is `TestOldConfigRecoveryUsesPinnedMidChainVotersAfterAddThenRemove`.
- `tla/EPaxosConfigRecoveryRetry.tla` checks finite logical prepare/accept retry rebroadcasts for both removal and addition recovery slices: old peers and old dependency widths are retained, the removed old voter remains a retry target for old removal instances, the added current voter is excluded for old pre-addition instances, and pure retries have no durable/application effects.
- `tla/EPaxosConfigRecoveryLostResponseRetry.tla` checks one finite explicit old-config lost-response-before-retry recovery slice: removed-voter prepare and accept responses are not counted before deterministic retry rebroadcast, retries stay pinned to old peers and old dependency width, and replacement responses after retry complete recovery through the old 3/4 quorum. This is separate from `EPaxosConfigRecoveryRetry` and does not claim arbitrary message-loss retry behavior.
- `tla/EPaxosConfigTransitionRetry.tla` checks finite logical PreAccept/Accept retry rebroadcasts for normal local-owner old instances after both removal and addition transitions: old peers and old dependency widths are retained, the removed old voter remains a retry target for old removal instances, the added current voter is excluded for old pre-addition instances, and pure retries have no durable/application effects.
- `tla/EPaxosConfigTransitionDedup.tla` checks finite response de-duplication for normal local-owner old instances after removal and addition transitions: the local owner vote is counted separately from remote responses, duplicate old-voter PreAccept/Accept responses do not advance below old quorum, added current voters are excluded for pre-addition instances, and the modeled second distinct old remote response advances only at old quorum.

Open limitation:

- The current formal models cover a finite local barrier, one add-voter transition, one remove-voter transition, one add-then-remove chain, one mid-chain recovery slice after that chain, one durable restart-replay slice, one normal old-config transition response de-duplication slice, one normal old-config transition retry-timer slice, one staged old-instance recovery-after-removal slice, one staged lost+duplicate recovery response de-duplication slice, one staged old-instance recovery-after-addition slice, one finite old-config recovery retry-timer slice, and one explicit finite old-config lost-response-before-retry recovery slice. They do not cover arbitrary/multi-step reconfiguration chains, joint consensus, arbitrary recovery under configuration changes, arbitrary durable histories, arbitrary retry/timer histories, arbitrary message loss beyond the named finite pre-retry loss slice, or unbounded configuration histories.

Source anchors: `epaxos/node.go` (`NewRawNode`, `Propose`, `ProposeConfChange`, `configureTOQ`, `confChangeQuorumFrom`, `confFor`, `votersForConf`, `slowQuorumForConf`, `broadcast`, `startPrepare`, `handlePrepareResp`, `startAccept`, `handleAcceptResp`, `markPendingConf`, `refreshPendingConf`, `computeAttrsAt`, `rememberConf`, `replayExecutedConfig`, `replayConfChange`, `applyConfChange`, per-instance quorum/broadcast helpers), `epaxos/config_change_ordering_test.go`, `epaxos/toq_test.go`, `epaxos/sim_test.go`, `epaxos/recovery_test.go`, `tla/EPaxosConfigBarrier.tla`, `tla/EPaxosConfigTransition.tla`, `tla/EPaxosConfigRemoveTransition.tla`, `tla/EPaxosConfigChainTransition.tla`, `tla/EPaxosConfigReplay.tla`, `tla/EPaxosConfigRecovery.tla`, `tla/EPaxosConfigRecoveryDedup.tla`.

Additional source anchors for finite add/de-dup recovery coverage: `tla/EPaxosConfigAddRecovery.tla`, `tla/EPaxosConfigAddRecovery.cfg`, `tla/EPaxosConfigRecoveryDedup.cfg`.
Additional source anchors for finite config-recovery retry coverage: `tla/EPaxosConfigRecoveryRetry.tla`, `tla/EPaxosConfigRecoveryRetry.cfg`, and `epaxos/recovery_test.go` (`TestOldConfigRecoveryRetryUsesPinnedVotersAfterRemoval`, `TestOldConfigRecoveryRetryUsesPinnedVotersAfterAddition`).
Additional source anchors for finite config-recovery lost-response retry coverage: `tla/EPaxosConfigRecoveryLostResponseRetry.tla`, `tla/EPaxosConfigRecoveryLostResponseRetry.cfg`, and `epaxos/recovery_test.go` (`TestOldConfigRecoveryRetryCompletesAfterLostPreRetryPrepareResponse`, `TestOldConfigRecoveryRetryCompletesAfterLostPreRetryAcceptResponse`).
Additional source anchors for finite config-transition retry coverage: `tla/EPaxosConfigTransitionRetry.tla`, `tla/EPaxosConfigTransitionRetry.cfg`, and `epaxos/recovery_test.go` (`TestOldConfigTransitionRetryUsesPinnedVotersAfterRemoval`, `TestOldConfigTransitionRetryUsesPinnedVotersAfterAddition`).
Additional source anchors for finite config-transition response de-duplication coverage: `tla/EPaxosConfigTransitionDedup.tla`, `tla/EPaxosConfigTransitionDedup.cfg`, and `epaxos/recovery_test.go` (`TestOldConfigTransitionDedupUsesPinnedVotersAfterRemoval`, `TestOldConfigTransitionDedupUsesPinnedVotersAfterAddition`).
Additional source anchors for finite mid-chain config-recovery coverage: `tla/EPaxosConfigChainRecovery.tla`, `tla/EPaxosConfigChainRecovery.cfg`, and `epaxos/recovery_test.go` (`TestOldConfigRecoveryUsesPinnedMidChainVotersAfterAddThenRemove`).

## 15. Property-by-property rationale

### 15.1 Nontriviality

Claim: every committed user command was proposed by a client or embedder.

Reason:

- `Propose` is the only local user-command creation path.
- Receivers store commands only from validated PreAccept/Accept/Commit/Prepare-derived protocol messages.
- Recovery no-op creation uses `CommandNoop`, not `CommandUser`.

Evidence: `epaxos/node.go` (`Propose`, `propose`, `handlePrepareResp` all-none branch), `tla/EPaxos.tla` nontriviality invariant.

### 15.2 Stability

Claim: once a tuple is committed for an instance, the implementation does not replace it with a different committed tuple.

Reason:

- Committed records have `StatusCommitted` or `StatusExecuted` and incoming PreAccept/Accept/TryPreAccept handlers send the existing commit instead of overwriting.
- Ballots reject stale attempts.
- Checksums make durable mutation detectable.
- `Advance`-queued executed records preserve the committed command and attributes.

Evidence: `epaxos/node.go` (`handlePreAccept`, `handleAccept`, `handleTryPreAccept`, `commit`, `tryExecute`), `epaxos/checksum.go`, `tla/EPaxos.tla`, `tla/EPaxosResponses.tla`.

### 15.3 Per-instance consistency

Claim: two replicas cannot safely commit different commands for the same instance inside the modeled/non-Byzantine envelope.

Reason:

- Fast path requires a matching fast quorum plus dependency evidence.
- Slow path uses intersecting majority accept quorums.
- Prepare recovery gives priority to committed and accepted evidence before considering preaccepted candidates or no-op.
- TryPreAccept can be vetoed by conflicting local evidence.

Evidence: `epaxos/node.go` fast/slow/recovery paths, `tla/EPaxosResponses.tla`, `tla/EPaxosOptimizedRecovery.tla`, `tla/EPaxosEvidenceQuery.tla`.

### 15.4 Execution consistency

Claim: committed conflicting commands execute in the same relative order on every non-faulty replica that learns enough committed state to execute them.

Reason:

- At least one conflicting command records a dependency edge to the other through quorum attribute computation or recovery conflict checks.
- Dependency graph and SCC processing are deterministic.
- Sequence/ref ordering inside SCCs is deterministic.
- Missing dependencies trigger recovery or block execution.

Evidence: `epaxos/node.go` execution paths, `epaxos/dst_test.go` linearizability oracle tests, `tla/EPaxos.tla`, `tla/EPaxosRecovery.tla`.

### 15.5 Execution linearizability for interfering commands

Claim: if command `delta` is proposed only after conflicting command `gamma` has committed, every replica executes `gamma` before `delta`.

Reason:

- A committed `gamma` has durable quorum evidence.
- Any quorum that determines `delta`'s attributes intersects with the quorum that knows `gamma`.
- The intersecting replica adds `gamma` to `delta`'s dependencies and raises `delta.Seq` above `gamma.Seq`.
- Execution then either puts both commands in one SCC ordered by sequence/ref, or puts `gamma` in an earlier topological component.

Evidence: CMU-PDL-13-111 Theorem 5, `epaxos/node.go` (`computeAttrsAt`, `mergeAttrs`, `tryExecute`), DST read-after-write scenarios, TLA normal-case model.

Boundary:

- If the application acknowledges clients at commit time before dependency execution, the paper's per-object linearizability assumptions apply. This library exposes executed commands through `Ready.Committed`; applications that acknowledge only after applying `Ready.Committed` use the execution result rather than an early commit notification.

### 15.6 Liveness

Claim: with a healthy slow quorum, eventual message delivery, durable storage, and non-Byzantine behavior, proposed commands eventually commit and executable dependencies eventually close.

Reason:

- Retry timers rebroadcast PreAccept/Accept/TryPreAccept/Prepare messages using deterministic logical ticks.
- Recovery timers let a deterministic coordinator prepare missing or stalled dependencies.
- If an instance is genuinely unknown to a prepare quorum, recovery chooses a no-op, unblocking dependents.
- No leader election is required; any replica can propose its own instances, and deterministic recovery can finish foreign instances.

Evidence: `epaxos/node.go` (`Tick`, `onTimer`, `schedule`, `recoveryCoordinator`, `ensureDependencyRecovery`), `epaxos/sim_test.go`, `epaxos/dst_test.go`, `tla/EPaxosRecovery.tla`, local Jepsen restart/transport/storage profiles.

Boundary:

- FLP still applies: the implementation does not guarantee deterministic termination under unbounded asynchrony, Byzantine behavior, or permanent loss of a slow quorum.

### 15.7 Fail-closed no-quorum behavior

Claim: without a slow quorum, the system must not produce new committed application output for a trapped proposal.

Reason:

- Fast, slow, and recovery commit transitions all require quorum-derived evidence.
- `componentReady` will not emit commands with missing/uncommitted dependencies.
- Storage failure before `Advance` leaves Ready outstanding and prevents safe visibility.

Evidence: DST no-quorum tests, `tla/Quorum.tla`, `tla/ReadyAdvance.tla`, `epaxos/node.go` quorum checks.

### 15.8 Latest-read semantics in the KV example

Claim: the HTTP KV example's latest point reads and default latest scans use consensus barriers in the example's own write path.

Reason:

- Point reads wait for a key conflict barrier.
- Latest scans propose a scan-wide barrier conflict key shared by the HTTP write/transaction/delete path.
- Historical timestamp selectors intentionally avoid consensus progress and are documented as bounded/historical reads.

Boundary:

- This is an example-service contract, not a protocol-native range/prefix conflict predicate for arbitrary EPaxos applications.

Evidence: `examples/kv/cmd/kvnode/main.go`, `examples/kv/cmd/kvnode/main_test.go`, `jepsen/src/moreconsensus/epaxos_test.clj`, `tla/KVTimestampStaleness.tla`, `tla/KVOmissionRecovery.tla`.

## 16. Verification map

### 16.1 TLA+ models

| Model | What it checks | What it does not claim |
| --- | --- | --- |
| `tla/EPaxos.tla` | Normal PreAccept/Accept/Commit, dependency evidence, and deterministic execution. | Full recovery tree and unbounded state space. |
| `tla/EPaxosResponses.tla` | Response quorum evidence, prepare branch priority, `fast + slow - N` TryPreAccept witness threshold, and ballot-bound current-round AcceptOK evidence. | Unbounded state space. |
| `tla/EPaxosOptimizedRecovery.tla` | Focused 3/5/7 Accept-Deps stale-dependency optimized-recovery evidence slices. | The concrete `MsgEvidence` exchange, every technical-report branch, and unbounded proof. |
| `tla/EPaxosTryPreAcceptBranches.tla` | Focused 3/5/7 finite abstract TryPreAccept scenario/stage response-branch slice: stale restart, committed evidence ignore/fail-closed, direct/forced accept, one uncommitted deferral with duplicate suppression, and OK slow-quorum accept with `okVotes >= SlowQuorum` as the only quorum detail in this model. | TryPreAccept message paths, complete optimized-recovery branch parity, unbounded recovery trees, arbitrary message loss, and recovery under reconfiguration. |
| `tla/EPaxosTryPreAcceptMessagePath.tla` | Focused 3/5/7 finite TryPreAccept request/response message paths: follower commit-only, stale/conflict reject, duplicate matching re-ack without durable rewrite, fresh durable ack; coordinator stale restart, committed evidence/direct accept, uncommitted forced/deferred handling, older-ballot ignore, duplicate OK ignore, first OK below quorum, pre-seeded quorum immediate accept, and OK slow-quorum accept. | Full network histories, evidence-query internals, complete optimized-recovery branch parity, unbounded recovery trees, arbitrary message loss, and recovery under reconfiguration. |
| `tla/EPaxosTryConflictForce.tla` | Focused 3/5/7 finite quorum-arithmetic check for `tryConflictForcesSlowAccept`: required conflict leader forces slow Accept only without an existing dependency, required deferred-cycle leader forces slow Accept, optional leaders defer, and non-force cases start blocker recovery. | Full recovery histories, TryPreAccept message paths, evidence-query internals, complete optimized-recovery branch parity, and unbounded proof. |
| `tla/EPaxosEvidenceQuery.tla` | Focused 3/5/7 committed-conflict evidence-query slice: guard-gated `MsgEvidence`, read-only responses, duplicate/mismatched drops, sender-preserving evidence validation, stale rejection restart, and fail-closed fallback. | Every technical-report branch, arbitrary membership/reconfiguration recovery, and unbounded proof. |
| `tla/EPaxosTryPreAcceptRetry.tla` | Focused finite TryPreAccept retry timer: current-ballot same-target pending evidence fails closed before retry, stale same-target evidence is deleted, unrelated-target evidence is retained, and normal or ignore-marker retry rebroadcasts have no durable/application effects. | Arbitrary network delivery, message loss, complete optimized-recovery branch parity, recovery under reconfiguration, and unbounded proof. |
| `tla/EPaxosRecovery.tla` | Stopped-owner dependency recovery and no-op unblocking for finite configs. | Arbitrary recovery under reconfiguration. |
| `tla/EPaxosRollbackAllocation.tla` | Rollback allocation: a restored local checkpoint learns a later own committed instance from quorum, advances `nextInstance`, skips a known future local ref under a defensive stale-next state, allocates a fresh local ref, and preserves learned-before-fresh apply order. | Full EPaxos recovery, unbounded rollback histories, storage checksums, message loss, or arbitrary multi-replica rollback. |
| `tla/EPaxosConfigBarrier.tla` | Pending config barriers and user-command ordering against two fixed config refs. | Dynamic membership histories. |
| `tla/EPaxosConfigTransition.tla` | One finite add-voter transition and config pinning. | Multi-step, joint consensus, recovery during config change. |
| `tla/EPaxosConfigRemoveTransition.tla` | One finite remove-voter transition where an old in-flight instance remains pinned to old voters/quorum and a later instance excludes the removed voter. | Multi-step, joint consensus, recovery during config change. |
| `tla/EPaxosConfigChainTransition.tla` | One finite add-then-remove chain where old and mid-flight instances remain pinned across two config changes. | Arbitrary multi-step, joint consensus, recovery during config change. |
| `tla/EPaxosConfigChainRecovery.tla` | One finite mid-chain recovery slice where a Conf2 instance remains pinned to `{1,2,3,4}` after the current config becomes `{1,3,4}`; current-conf quorum responses do not advance prepare/accept, removed voter 2 supplies the modeled third old-quorum vote, and the no-op executes without application output. | Arbitrary recovery histories, arbitrary membership histories, retries, joint consensus, message loss, application output, and unbounded proof. |
| `tla/EPaxosConfigTransitionRetry.tla` | One finite normal old-config transition retry-timer slice where local-owner PreAccept/Accept retries after removal include removed voter 4 for old pinned instances, local-owner PreAccept/Accept retries after addition exclude added voter 4 for old pinned instances, dependency width stays old, and pure retries emit no durable/application effects. | Arbitrary retry histories beyond this timer slice, joint consensus, arbitrary message loss, arbitrary membership histories, application output, and unbounded proof. |
| `tla/EPaxosConfigTransitionDedup.tla` | One finite normal old-config transition response de-duplication slice where the local owner vote is counted separately from remote PreAccept/Accept responses, duplicate old-voter responses and added current voter responses do not satisfy old pinned quorums, and the modeled second distinct old remote advances at quorum. | Arbitrary message histories, joint consensus, arbitrary membership histories, application output, and unbounded proof. |
| `tla/EPaxosConfigRecovery.tla` | One finite staged recovery-under-removal slice where an old four-voter instance is recovered after the current config removes voter 4; prepare/accept quorums use the old slow quorum, count the removed voter only for that old instance, and reject removed-voter new-config proposals. | Arbitrary recovery under configuration changes, joint consensus, message loss, arbitrary membership histories, and unbounded proof. |
| `tla/EPaxosConfigRecoveryDedup.tla` | One finite staged lost+duplicate response de-duplication slice where an old four-voter instance recovery remains below old quorum after one lost response and a duplicate voter-3 response, then advances only after distinct removed voter 4 answers. | Retries, timer rebroadcast, arbitrary recovery under configuration changes, joint consensus, arbitrary message loss, arbitrary membership histories, application output, and unbounded proof. |
| `tla/EPaxosConfigRecoveryRetry.tla` | One finite old-config recovery retry-timer slice where prepare/accept retries after removal include removed voter 4 for old pinned instances, prepare/accept retries after addition exclude added voter 4 for old pinned instances, dependency width stays old, and pure retries emit no durable/application effects. | Arbitrary recovery histories, retry histories beyond this timer slice, joint consensus, arbitrary message loss, arbitrary membership histories, application output, and unbounded proof. |
| `tla/EPaxosConfigRecoveryLostResponseRetry.tla` | One finite old-config recovery lost-response retry slice where removed-voter prepare and accept responses are explicit pre-retry losses, deterministic retry rebroadcasts remain pinned to old peers with old dependency width, and replacement responses after retry complete no-op recovery through the old 3/4 quorum. | Arbitrary recovery histories, arbitrary retry histories, arbitrary message loss beyond this named finite pre-retry loss shape, joint consensus, arbitrary membership histories, application output, and unbounded proof. |
| `tla/EPaxosConfigAddRecovery.tla` | One finite staged recovery-after-addition slice where an old three-voter instance is recovered after the current config adds voter 4; added-voter prepare/accept attempts do not enter old quorum sets, old 2/3 quorum is sufficient, and the no-op executes. | Arbitrary recovery under configuration changes, joint consensus, message loss, arbitrary membership histories, and unbounded proof. |
| `tla/EPaxosRevisited.tla` | TOQ envelope, delayed assignment, pending-decision blocking, receiver processing, fast-wait behavior, and chain pruning. | Real clock synchronization and OWD measurement. |
| `tla/TOQClockDiscipline.tla` | Finite bounded-skew/bounded-delay `ProcessAt` contract; in Go this maps to `TOQOneWayDelay` values that already include skew margin. | Operational clock-sync implementation or delay measurement. |
| `tla/ReadyAdvance.tla` | Durable Ready/Advance prefix acknowledgement and retry. | Concrete storage engine behavior. |
| `tla/Quorum.tla` | Supported quorum table and intersections. | Byzantine or dynamic quorum systems. |
| `tla/KVTimestampStaleness.tla`, `tla/KVOmissionRecovery.tla` | KV timestamp and omission/recovery semantics. | General arbitrary application semantics. |

### 16.2 DST and deterministic simulation

DST scenarios must prove both positive progress and negative fail-closed behavior at the exact failure-count boundary. The required matrix is:

- cluster sizes 1 through 7 exactly;
- progress with at most `N - SlowQuorum(N)` omitted/crashed voters;
- no new committed application output when more than that number is unavailable and the proposer is trapped without quorum;
- linearizability/correctness under conflicting writes, reads, partition, and heal;
- storage write failure prevents application until durable writes recover;
- after heal/recovery, commands apply exactly once everywhere.

Current DST evidence now includes deterministic reordering, liveness-after-heal, majority/minority availability, storage failure, pause/skew, rollback, owner-independent recovery, cluster-size tests, and focused failure-boundary tests: `TestDSTFailureBoundarySlowQuorumSizesOneThroughSeven`, `TestDSTStorageFailureBoundarySlowQuorumSizesOneThroughSeven`, `TestDSTFailureBoundaryThreeNodeCrashBoundary`, `TestDSTFailureBoundaryFiveNodeMajorityAndMinorityOmission`, `TestDSTLinearizabilityConflictingWritesReadsAcrossPartitionHeal`, and `TestDSTStorageFailureRetriesOutstandingReadyExactlyOnceWithHealthyQuorum`. The focused DST boundary command `go test ./epaxos -run 'TestDST.*(FailureBoundary|Linearizability|Storage)' -count=1` passed while adding those tests.

### 16.3 Jepsen

Local Jepsen profiles exercise restart, transport partition, storage-unavailable, destructive-storage, scan semantics, transaction behavior, and checker logic. Optional external validation tooling exists for separately approved environments, but it is not counted as release evidence for the current simulation/local-loopback claim.

### 16.4 Go, race, coverage, fuzz, and static gates

The repository scripts wire focused verification:

- `tests/go_coverage.sh` for Go tests and coverage gates;
- `tests/tla_model_check.sh` for finite TLC models;
- `tests/fuzz_stress_campaign.sh` for bounded fuzz/stress campaigns;
- `tests/chaos_fault_campaign.sh` for focused fault campaign and local Jepsen profiles;
- `tests/operations_readiness_audit.sh` for operations artifacts;
- `tests/go_no_go_workflow.sh` for release decision;
- `tests/release_scope_audit.sh` for release traceability;
- `tests/audit_repo.sh` for static/text audit.

## 17. Current no-go blockers

The implementation and evidence currently support a bounded library/example claim, not a mission-critical production-ready claim. The release remains no-go while these classes remain open in `RELEASE_SCOPE.md`:

- full optimized-recovery parity beyond focused evidence slices;
- unbounded formal proof beyond finite TLC configs;
- operational TOQ clock synchronization and OWD measurement;
- in-place disk-corruption repair, checksum recomputation/deletion, synthesized reconstruction without a verified checkpoint, or multi-replica/quorum-loss recovery;
- exercised production deployment manifest;
- target-environment backup/restore and disaster-recovery drill;
- target-environment mixed-version compatibility beyond the local loopback old/new-binary drill;
- target-environment capacity envelope;
- incident readiness tabletop or live drill.

The correct completion rule is therefore: do not mark the active goal complete unless current evidence proves mission-critical production readiness under the stated fault counts and every release blocker is closed with direct evidence.
