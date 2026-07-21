# Implemented EPaxos Algorithm

This document describes the algorithm implemented by `gosuda.org/moreconsensus/epaxos`. It is intentionally more detailed than `README.md`; the README stays feature-level. For property-by-property proof rationale, failure-count boundaries, and evidence/non-claims, see `EPAXOS_IMPLEMENTATION_PROOF.md`.

## Public execution model

The embedding owns transport, durable storage, application state and responses, snapshot materialization, and any wall-clock sampling. A deterministic `RawNode` exposes `NewRawNode`, `Tick`, `Step`, `Propose`, `ProposeConfChange`, `HasReady`, `Ready`, `ReadyInto`, `Advance`, `ProvideRecordLoad`, `ProvideCheckpoint`, `Status`, and `RuntimeStats`; certified-bootstrap and explicit-TOQ calls are expert protocol extensions.

`Propose` accepts only opaque application `Command` values. Recovery no-ops, configuration changes, membership controls, and checkpoint barriers are distinct protocol entry kinds. Only application entries produce `Ready.Apply`.

The caller processes `Ready` in phase order: atomically persist hard state, protocol records, and snapshot metadata; send messages; install a received snapshot; apply `Ready.Apply` strictly in slice order; service `Ready.Checkpoint`; atomically perform `Ready.Compact`; then call `Advance` with the exact completed prefix. Work may repeat byte-for-byte before `Advance` or after crash. The core starts no goroutines, performs no I/O, invokes no application callback, and reads no clock. TOQ proposals consume an explicit caller-sampled `now` supplied to `ProcessTOQ`.

## Paper claim map

| Paper claim | Paper anchor | Repository implementation | Release status |
| --- | --- | --- | --- |
| Optimized EPaxos fast quorum | EPaxos SOSP 2013 §4.3/§4.4, ACM proceedings pp.362-365, and the EPaxos technical report §6 define the optimized fast quorum as `F + floor((F + 1) / 2)` including the command leader for odd `N = 2F + 1` clusters. That permits `N=3 -> 2`, `N=5 -> 3`, and `N=7 -> 5` pre-accept votes, with matching recovery evidence obligations. | Odd supported cluster sizes use the paper threshold (`N=3 -> 2`, `N=5 -> 3`, `N=7 -> 5`); even supported sizes keep the previous conservative quorum because the paper proof assumes `N=2F+1`. Fast commits also require FP-deps-committed prefix evidence tied to the matching fast quorum. | Implemented normal fast-path threshold/evidence for odd supported sizes plus Go committed stale-dependency evidence-search/resend-ignore; normal un-timed originator-disagrees fast commit, even-size optimized quorums, and unbounded proof remain non-claims. |
| EPaxos Revisited TOQ | EPaxos Revisited NSDI 2021 §4/§4.1, USENIX proceedings pp.616-617, requires synchronized physical clocks, one-way-delay estimates with clock-skew/sync margin and sync groups, outgoing `PreAccept` messages with sequence number `0` and empty dependencies, and delayed originator dependency assignment at `ProcessAt`. | `Config.TOQ` is separate from `Config.TimeOptimization`. The embedding supplies conservative `TOQOneWayDelay` bounds and an optional `TOQSyncGroup`, samples its clock externally, and passes `now` to `ProcessTOQ`; the core computes `ProcessAt`, persists `TOQPending`, and delays dependency assignment. | Implemented core TOQ behavior with explicit caller samples; synchronization, sampling, and delay measurement are embedding obligations. |
| EPaxos Revisited chain pruning | EPaxos Revisited NSDI 2021 §3, USENIX proceedings p.616, prunes dependency `B` from `A` when `B` already depends on `A` and has higher sequence number. | Dependency-graph construction applies this pruning before SCC execution. | Implemented claim. |
| EPaxos recovery | EPaxos SOSP 2013 §4.6, ACM proceedings p.366, requires prepare/accept recovery, try-pre-accept conflict checks, and dependency evidence sufficient for the quorum formula in use. CMU-PDL-13-111 §6/§6.2 also describes Accept-Deps evidence carried on Accept/AcceptReply for optimized recovery corner cases. | The implementation has owner-independent prepare, durable fast-path markers, FP-deps-committed prefix evidence on `MsgPreAcceptResp`, durable `InstanceRecord.RecordBallot` value-ballot evidence separate from promise `Ballot`, recovery-only aggregate `AcceptSeq`/`AcceptDeps` plus sender-preserving `AcceptEvidence`, TryPreAccept committed stale-dependency evidence search with read-only `MsgEvidence`/`MsgEvidenceResp`, authorized `IgnoreDependency` resends, fail-closed slow accept, and stopped-owner recovery for the implemented quorum table. | Implemented safety-focused recovery path for the finite evidence suite, Go committed stale-dependency evidence-search/resend-ignore for supported `F <= 3`, and finite optimized-recovery decision-tree branch parity for the implemented F<=3 Accept-Deps mode; arbitrary networks, message loss/retry schedules, recovery under reconfiguration, durable histories, and unbounded proof remain non-claims. |

Primary paper references: [EPaxos SOSP 2013](https://sigops.org/s/conferences/sosp/2013/papers/p358-moraru.pdf), [EPaxos technical report CMU-PDL-13-111](https://www.pdl.cmu.edu/PDL-FTP/associated/CMU-PDL-13-111.pdf), and [EPaxos Revisited NSDI 2021](https://www.usenix.org/system/files/nsdi21-tollman.pdf).

## TOQ operational-clock boundary

The core does not synchronize or sample clocks, measure one-way delay, select a production sync group, or quarantine nodes for drift. The caller passes an explicit sampled `now` to `ProcessTOQ`; each configured one-way-delay value must include network delay plus clock-skew uncertainty. `tla/TOQClockDiscipline.tla` is finite evidence for this supplied-bound contract, not an operational clock proof.

## Instance identity

Each command is stored in an `InstanceRecord` named by `(ReplicaID, InstanceNum, ConfID)`. `ReplicaID` identifies the owner replica. `InstanceNum` is monotonically allocated by that owner. `ConfID` pins the voter set used by the instance, which makes dependency vectors deterministic.

## Commands, footprints, and conflicts

`Command` contains an opaque `ID`, opaque `Payload`, canonical `Footprint`, and replicated `CycleKey`. The core does not decode payloads or deduplicate IDs. A footprint uses byte-lexicographic points, half-open spans `[Start,End)`, or explicit `All` scope. Point equality, point containment, and span overlap define conflict; `All` conflicts with every non-noop entry in the EPaxos group. Empty application footprints are invalid. Protocol-global entries conflict with every non-noop entry; recovery no-ops conflict with nothing.

Canonicalization sorts and deduplicates points, merges overlapping or adjacent spans, removes covered points, validates bounds, and deep-copies retained endpoints. The overlap index combines exact point postings, an augmented interval tree, and group-global postings without expanding ranges into physical keys.

The dependency vector stores the latest conflicting instance per voter in pinned configuration order. Sequence is one greater than the maximum dependency sequence. Footprints are a correctness contract: commands declared nonoverlapping must strongly commute in final state, every response, durable dedup state, and deterministic side effects.

## Quorums

The implementation supports cluster sizes 1, 2, 3, 4, 5, 6, and 7 exactly. Slow quorum is majority: `n/2 + 1`. Odd cluster sizes use the optimized EPaxos paper fast quorum for `N=2F+1`; even cluster sizes keep the previous conservative quorum because the paper proof assumes odd `N`.

| Voters | Slow quorum | Fast quorum | Max unavailable for progress | No-quorum boundary |
| --- | ---: | ---: | ---: | --- |
| 1 | 1 | 1 | 0 | 1 unavailable voter |
| 2 | 2 | 2 | 0 | 1 unavailable voter |
| 3 | 2 | 2 | 1 | 2 unavailable voters |
| 4 | 3 | 4 | 1 | 2 unavailable voters |
| 5 | 3 | 3 | 2 | 3 unavailable voters |
| 6 | 4 | 5 | 2 | 3 unavailable voters |
| 7 | 4 | 5 | 3 | 4 unavailable voters |

The fast-commit predicate is stricter than quorum size alone. A matching fast quorum must have exact/default fast-path markers, and every compact dependency prefix in the final attributes must be covered by `DepsCommitted` evidence from at least one participant in that matching quorum. For dependency vector slot `i` with value `k`, the evidence bit means the sender has durably recorded every implicit dependency for that replica through instance `k` as committed or executed.

## Pre-accept fast path

When a local application proposes a command outside TOQ mode, the owner replica:

1. Allocates the next local instance number.
2. Assigns a deterministic logical `ProcessAt` tick when the optional `TimeOptimization` mode is enabled.
3. Computes initial attributes from conflicts ordered before that `ProcessAt` value, or from the local conflict indexes when the optimization is disabled.
4. Persists a `StatusPreAccepted` record in `Ready.Records`.
5. Sends `MsgPreAccept` to other voters with the same `ProcessAt` value on the initial send and every retry.
6. Counts the local vote as one pre-accept reply.

`Config.TOQ` follows the EPaxos Revisited TOQ shape. The embedding samples time externally and passes `now` to `ProcessTOQ`; the core derives `ProcessAt` from that sample and configured one-way bounds, persists `TOQPending`, and delays local dependency assignment. Outbound TOQ `MsgPreAccept` messages use `TOQ=true`, `Seq=0`, and empty `Deps`; retries preserve that envelope.

For TOQ deployments, every `TOQOneWayDelay[id]` value is a conservative delivery bound supplied by the embedding application. It must include the measured one-way network delay plus the maximum clock-skew/synchronization uncertainty for that receiver and sync group. The core does not implement clock synchronization or delay measurement.

At local `ProcessAt`, the owner computes attributes from conflicts ordered before the TOQ timestamp, clears `TOQPending`, persists the ordinary `StatusPreAccepted` record, indexes the command as a conflict, seeds the local pre-accept vote, and only then evaluates fast/slow pre-accept completion. `MsgPreAcceptResp` votes that arrive while the local record is still pending are stored but cannot commit, fast-wait, or slow-accept the instance until the pending assignment is durably cleared.

A receiver validates the envelope, checksum, and voter incarnations. An unflagged `MsgPreAccept` carries a full-width dependency vector; a TOQ PreAccept carries the zero-sequence/empty-dependency envelope. Logical `Tick` drives `TimeOptimization`; explicit `ProcessTOQ(now)` releases due TOQ work. Due entries are processed by `ProcessAt`, instance reference, then sender. The receiver computes and durably records attributes before replying.

The owner fast-commits when it can identify a matching fast quorum for the implemented quorum table and the matching quorum covers every final dependency prefix with committed evidence. In TOQ mode, the delayed local assignment is the local fast-path witness, so optimized odd-size quorums such as `N=5 -> 3` can commit after local assignment plus enough covering remote replies without waiting for every remote voter. With the older deterministic `TimeOptimization` heuristic, the stale-originator greater-tuple rule remains stricter and requires unanimous remote replies. The commit changes the record to `StatusCommitted`, persists it, broadcasts `MsgCommit`, and attempts deterministic execution.

## Slow accept path

The slow path persists `StatusAccepted`, broadcasts `MsgAccept`, and waits for a slow quorum of current-ballot `MsgAcceptResp`. A non-reject `MsgAcceptResp` whose ballot differs from the coordinator's current accept ballot is ignored before it can count toward `accOK` or merge recovery-only Accept-Deps evidence. `MsgAccept` and `MsgAcceptResp` carry the chosen execution attributes in `Seq`/`Deps` plus separate recovery-only `AcceptSeq`/`AcceptDeps` aggregate evidence and sender-preserving `AcceptEvidence` entries. Each `AcceptEvidence` entry records the original Accept/AcceptReply sender, sequence, and dependencies for the containing value tuple; receivers persist it, commits propagate it, and `MsgPrepareResp`/`MsgEvidenceResp` return it for recovery. Legacy aggregate evidence is retained for compatibility but cannot authorize committed stale-dependency ignore decisions.

## Recovery path

The implementation includes owner-independent prepare/recovery message types and timer-driven recovery transitions. A replica may recover an instance it owns, or a foreign instance whose missing or uncommitted dependency blocks local execution, when it is the deterministic recovery coordinator for that reference.

Recovery raises a promise ballot, broadcasts `MsgPrepare`, and waits for a slow quorum of `MsgPrepareResp`. `MsgPrepareResp.Ballot` is the responder's promise ballot, not an accepted-value ballot. `MsgPrepareResp.RecordBallot` carries the responder's durable `InstanceRecord.RecordBallot`, the value ballot that is not overwritten by prepare promises and is covered by the record checksum/invariant. Value selection is therefore based on durable response status and value metadata:

1. Any committed response wins immediately.
2. Otherwise, accepted responses choose the highest durable previous-record-ballot accepted tuple and finish through `MsgAccept`; lower-ballot accepted/pre-accepted attributes are not unioned into that tuple.
3. Otherwise, matching pre-accepted responses that carry the durable `FastPathEligible` marker may enter `MsgTryPreAccept` recovery once they meet the fast/slow intersection threshold `fast + slow - n`.
4. Otherwise, any pre-accepted response falls back to merged `MsgAccept`.
5. If the prepare quorum reports `StatusNone`, recovery chooses a no-op and commits it through `MsgAccept`.

`MsgTryPreAccept` is the EPaxos optimized-recovery check: witnesses reject stale ballots, committed conflicts, mutually unordered uncommitted conflicts, and stale-dependency conflicts. A committed stale-dependency rejection for a conflict already present in the candidate's `Deps` starts a read-only `MsgEvidence`/`MsgEvidenceResp` check for that committed conflict, scoped by the candidate ref, conflict ref, and current TryPreAccept ballot. Older-ballot evidence responses are dropped before they can populate the live check, and same-ballot duplicate responses from the same sender keep the first recorded response. Once the coordinator has `F` unique checked replicas for the committed conflict tuple and no sender-preserving Accept/AcceptReply evidence from a sender forced into the candidate's possible fast quorum whose deps omit the candidate, it resends `MsgTryPreAccept` with `IgnoreDependency.Ref` for that one conflict. The witness consumes that marker only for the named committed dependency and only for the current request. Missing, malformed, contradictory, stale-duplicate, or insufficient evidence fails closed through slow `MsgAccept`; the slow path folds the same-configuration `ConflictRef` into `Deps` and bumps `Seq` using the committed tuple sequence when known. Uncommitted conflicts either force slow `MsgAccept` when leader-in-fast-quorum or recorded deferral-cycle evidence says the candidate fast quorum must contain the conflict leader, or they defer the candidate while recovering the blocker. Successful conceptual witnesses from prepare plus actual try-pre-accept witnesses advance to `MsgAccept`; recovery records and commit records do not retain `FastPathEligible`. Periodic logical-tick commit rebroadcasts help replicas that missed a commit during a transport outage.

`timerTryPreAccept` is also deterministic logical-tick work. If same-target evidence checks for the current TryPreAccept ballot are still pending when the timer fires, the coordinator fails closed to slow `MsgAccept` before retrying. Same-target evidence checks from older or newer ballots are deleted first and do not fail the current recovery. Evidence checks for another target remain in the pending map and do not block retry. A retry rebroadcast sends either the normal `MsgTryPreAccept` tuple or per-conflict ignore-marker messages, never both, and a pure retry emits no durable record or application command.

## Execution ordering

The node builds a dependency graph over committed, unexecuted instances, collapses strongly connected components, and executes only components whose true external dependencies are discharged. Known conflicting pre-accepted or accepted instances still block. Within an SCC, order is ascending `(Seq, CycleKey, InstanceRef)`; sequence always dominates user-supplied cycle bytes.

`Ready.Apply` contains only application entries and must be applied unchanged in slice order. Recovery no-ops have no application effect. Configuration, membership, and checkpoint controls execute inside the protocol core. A checkpoint barrier causes the complete ready SCC to execute before successor components pause, so all replicas take the same exact application cut.
## Configuration changes

`ProposeConfChange` validates that a membership operation changes the current voter set and keeps the size within the supported one-to-seven-replica range, then encodes it as a configuration command. A node treats any observed, unexecuted configuration command as a membership barrier: local user proposals and additional local configuration proposals are rejected until the barrier executes. Configuration commands conflict with every non-noop command, and user-command attribute computation also includes known configuration-command instances even when user conflict keys are disjoint. When a configuration command executes, future instances use the new sorted voter set and an incremented configuration id. Existing instances remain pinned to their original configuration id: their dependency-vector width, slow/fast quorum thresholds, retries, prepare/accept/commit broadcasts, and recovery quorums are selected from `Ref.Conf`, not from the node's current `ConfState`. A replica that has applied a configuration excluding itself rejects `Propose` and `ProposeConfChange` before allocating an instance; it can still step messages for older instances whose `Ref.Conf` includes it. A stale same-generation configuration command that executes after a later configuration is already installed is recorded as executed but does not overwrite the installed configuration history.

On restart, `NewRawNode` loads durable records, remembers any stored historical configurations, then replays executed configuration-change records in instance order to rebuild intermediate configuration ids that are needed by old in-flight instances. Replay rejects a stored configuration id whose voters conflict with the voters deterministically produced by executed configuration commands. A replayed unexecuted configuration command remains a pending barrier, so the restarted node still rejects new local proposals until that command executes.

When TOQ is enabled, default `TOQSyncGroup` selection and one-way-delay validation run after executed configuration replay, so an omitted sync group defaults to the replayed current voters and an explicit stale sync group containing a removed voter is rejected.
Recovery, retry, and broadcast paths select voter sets and slow-quorum thresholds by the instance `Ref.Conf`, so an old instance can still use a removed voter for its old pinned quorum, newly added voters cannot count for pre-addition instances, a mid-chain instance from `{1,2,3,4}` still needs the old 3/4 quorum after the current config becomes `{1,3,4}`, and current-configuration proposals from a removed local replica remain rejected.

The finite configuration evidence currently covers an executed add/remove durable replay slice, local-owner old-config PreAccept/Accept response de-duplication with the owner vote counted separately from remote responses, retry-timer rebroadcast after removal/addition, an explicit finite normal local-owner old-config lost-response-before-retry transition slice after removal/addition, add-then-remove old/mid transition retry-timer rebroadcast, add-then-remove old/mid transition lost-response-before-retry rebroadcast, mid-chain recovery after an add-then-remove history, staged old-instance recovery-after-removal, lost+duplicate recovery response de-duplication, recovery-after-addition, old-config prepare/accept recovery retry-timer slices, an explicit finite old-config lost-response-before-retry recovery slice, and an explicit finite mid-chain lost-response-before-retry recovery slice. Arbitrary durable histories, arbitrary recovery under reconfiguration, arbitrary retry histories, arbitrary message loss beyond those named finite pre-retry loss shapes, joint consensus, and unbounded membership changes remain outside the current claim.

## Ready, apply, and response contract

`Ready.Records` is durable protocol state; `Messages` is transport work; `Apply` is dependency-ordered application work; `RecordLoads`, `Checkpoint`, `Snapshot`, and `Compact` are explicit embedding handshakes. `MustSync` marks durability barriers. `Advance` accepts only an exact matching prefix and leaves unacknowledged work frozen for retry.

`Ready.Apply` may repeat before `Advance` and after crash. The core neither stores nor promises exactly-once responses. The application must atomically commit each command effect with `CommandID -> command digest + applied marker + full response or durable result handle`; reuse of an ID for different command bytes fails closed. Network timeouts remain ambiguous even when retained dedup state prevents duplicate effects.

The example KV follows this contract for writes and ordered point/range reads. Empty successes are reconstructable; ordered read bytes are durable result data. `NextCommandSequence` scans the result namespace rather than compactable protocol history.
## Checksums and serialization

Canonical BLAKE3 digests bind entry kind, the valid entry-specific union, canonical footprint, cycle key, sequence/dependencies, ballots, timing domain, and voter incarnations. Live transport is `MEP3`; old `MEP2` traffic is rejected. Decode rejects noncanonical footprints, malformed spans, over-limit metadata, invalid unions, stale incarnations, and checksum mismatch without retaining aliases.

The example writes Pebble record codec v9. It decodes v1-v8 only for startup migration, mapping legacy user keys to points and keyless legacy user commands to fail-closed `All`, then rewrites the current authenticated form. Corrupt, regressing, or partially migrated durable state fails startup.

## Allocation and ownership

By default proposals are cloned. With `ZeroCopyProposals`, the caller transfers already-canonical payload, footprint, and cycle buffers while observable through `Ready` or `Status`. Decode payload/footprint bytes alias the input buffer until `Step`; `Step` owns retained bytes. Pool reset clears every active alias.

## Ready reuse, bounded votes, and runtime telemetry

`ReadyInto` freezes the same batch as `Ready` and clones into caller capacity. New output remains disjoint while frozen; `Advance` clears acknowledged references before reuse. Quorum sender state remains pinned to each instance configuration. Reusable Ready, vote, decode, and conflict-index storage is cleared before reuse; oversized retained arenas are dropped. `RuntimeStats` reports resident, folded, deferred, recovery, and Ready state without cloning commands.

## Record load handshake

When a retained reference is not resident, `Ready.RecordLoads` lists sorted, deduplicated references and the embedding responds with `ProvideRecordLoad`. References covered by a durable compacted frontier never request deleted records: `Step` rejects them and emits the latest certified checkpoint offer.

## Certified checkpoints and durable compaction

When executed history reaches the retention threshold, replicas may originate group-global checkpoint barriers. The executor recovers every real dependency hole, executes the complete barrier SCC, and emits one stable `Ready.Checkpoint{ID, Through}` after all represented application work. `ProvideCheckpoint` returns a nonempty opaque bounded snapshot handle and nonzero application digest.

A descriptor binds cluster/config identities and incarnations, barrier tuple, exact multi-configuration execution frontier, protocol projection, allocator fences, and application digest. Voters attest only after prepared snapshot metadata is durable. A canonical slow-quorum certificate must itself become durable before `Ready.Compact` authorizes atomic tombstone advancement and record deletion. A late compacted reference never recreates protocol state.

Restart validates the checkpoint and certificate, installs the application snapshot first, seeds compacted dependency/execution summaries, and loads only records after `CompactedThrough`. Executed delta application commands may replay and rely on application result dedup. Prepared or certified-but-not-compacted checkpoints retain full history. Received snapshot handles are materialized and digest/size verified by the embedding before `Step`.

## KV and MVCC integration

Logical resources, not physical MVCC `key/timestamp` suffixes, belong in footprints. Point reads, writes, deletes, and intents use a logical point; scans, delete-ranges, and range tombstones use a half-open span. Cross-resource invariants use deterministic namespaces such as `row/`, `index/`, `unique/`, `txn/`, and `schema/` plus sentinel points. Group maintenance uses `All`, which covers one EPaxos group rather than an entire database.

MVCC timestamps, intents, transaction records, timestamp caches, retries, GC thresholds, and closed timestamps remain application state. A write applied at timestamp 10 may be visible to a later read at timestamp 10; only a read that already observed the old value at 10 forces an overlapping later write above 10 or a retry.

## Implementation blueprint

This section is an embedding blueprint, not a second protocol specification. The protocol decisions remain inside `RawNode`; the embedding supplies durable effects, transport, application execution, clock samples, and lifecycle control. `examples/kv` is the executable reference for these boundaries.

### 1. Separate the five ownership domains

Build one single-threaded event loop per replica around these components:

1. **Protocol core:** one `RawNode`. Only its event-loop goroutine calls `Tick`, `ProcessTOQ`, `Step`, `Propose`, `Ready`, `ProvideRecordLoad`, `ProvideCheckpoint`, and `Advance`.
2. **Protocol storage:** implements `Storage` and atomically stores hard state, configuration history, bootstrap state, voter identity, frontier updates, allocator floors, instance records, checkpoint metadata, and compaction tombstones.
3. **Application storage:** applies `Ready.Apply` in order and atomically stores the command result/dedup record with each application mutation.
4. **Transport:** carries authenticated serialized protocol messages. It may retry, duplicate, delay, and reorder messages, but must not mutate them.
5. **Clock and scheduler:** produces logical ticks and, only when TOQ is enabled, monotonic externally sampled time plus operational clock-health decisions.

Do not let transport or application workers call the core concurrently. Feed their completions back into the replica event loop. This gives each input a deterministic before/after relationship and makes crash injection reproducible.

### 2. Create durable schemas before constructing a node

Use separate key namespaces or column families for:

| Namespace | Key | Value and invariant |
| --- | --- | --- |
| Hard state | singleton | Complete `HardState`; never regress configuration id, tick, or voter history. |
| Configuration history | `ConfID` | Canonical sorted voter set and associated history metadata. Old instances retain their original configuration. |
| Instance record | `(ConfID, ReplicaID, InstanceNum)` | Current authenticated `InstanceRecord`. A replacement must obey ballot and status monotonicity. |
| Bootstrap/voter state | protocol-defined id | `BootstrapRecord`, `LocalVoterState`, and voter incarnations, committed in the same durability phase as related protocol state. |
| Frontier | `(ConfID, ReplicaID)` | Durable executed/committed frontier updates. A frontier only moves forward. |
| Allocator | singleton | Highest acknowledged local allocator floor. Never reuse an instance number after restart. |
| Checkpoint | checkpoint id | Descriptor, snapshot handle, application digest, quorum certificate, and compacted frontier. |
| Application result | `CommandID` | Command digest plus full response or durable result handle. Reusing an id with different command bytes fails closed. |

Implement `Storage.InitialState`, `LoadCheckpoint`, `LoadInstances`, and `LoadInstance` directly over these schemas. Startup must first validate the checkpoint and install its application snapshot, then construct `RawNode`, which loads authenticated protocol records strictly after the compacted frontier. Corruption, contradictory history, allocator regression, or missing data above the compacted frontier is a startup error, not an empty-state fallback.

### 3. Configure and start a replica

At minimum, configure a stable local `ReplicaID`, sorted voters, a stable cluster identity, voter incarnations, storage, retry ticks, and recovery ticks. Keep the same identity and durable store across an ordinary crash/restart. A process replacement with a new incarnation requires the repository's certified bootstrap flow; changing only the numeric id is unsafe.

```go
node, err := epaxos.NewRawNode(epaxos.Config{
    ID:            localID,
    Voters:        voters,
    Cluster:       clusterID,
    LocalIdentity: localIdentity,
    VoterIdentities: identities,
    Storage:       protocolStore,
    RetryTicks:    retryTicks,
    RecoveryTicks: recoveryTicks,
})
if err != nil {
    return err // fail startup; do not silently initialize an empty node
}
```

Drain the initial `Ready` before accepting client traffic. That first batch may contain complete hard state or bootstrap work that must be durable before messages are visible.

### 4. Implement the Ready loop as the only effect boundary

The following shape is intentionally serial. Production code may pipeline storage and transport internally, but it must preserve these phase barriers and acknowledge only completed prefixes.

```go
func drain(node *epaxos.RawNode) error {
    for node.HasReady() {
        rd := node.Ready() // frozen and byte-stable until Advance

        // Phase 1: atomically persist every protocol mutation in rd.
        if err := protocolStore.ApplyReady(rd); err != nil {
            return err // keep rd frozen; retry the same batch
        }

        // Phase 2: only durable protocol decisions may leave the process.
        if err := transport.Enqueue(rd.Messages, rd.BootstrapMessages); err != nil {
            return err // transport enqueue must be idempotent
        }

        // Phase 3: materialize and verify an offered received snapshot.
        if rd.Snapshot != nil {
            if err := application.InstallAndVerify(*rd.Snapshot); err != nil {
                return err
            }
        }

        // Phase 4: preserve this exact order. ApplyOnce stores effect and result.
        for _, command := range rd.Apply {
            if err := application.ApplyOnce(command); err != nil {
                return err
            }
        }

        // Phase 5: answer explicit storage/application handshakes.
        for _, ref := range rd.RecordLoads {
            record, found, err := protocolStore.LoadInstance(ref)
            if err != nil {
                return err
            }
            if err := node.ProvideRecordLoad(epaxos.RecordLoadResult{
                Ref: ref, Record: record, Found: found,
            }); err != nil {
                return err
            }
        }
        if rd.Checkpoint != nil {
            result, err := application.MaterializeCheckpoint(*rd.Checkpoint)
            if err != nil {
                return err
            }
            if err := node.ProvideCheckpoint(result); err != nil {
                return err
            }
        }

        // Phase 6: Compact has already been authorized by a durable certificate.
        // Its metadata/tombstones and record deletion must be one atomic write.
        if err := protocolStore.CompleteCompaction(rd.Compact); err != nil {
            return err
        }

        if err := node.Advance(rd); err != nil {
            return err // a mismatch is an embedding bug
        }
    }
    return nil
}
```

If one storage transaction already handles protocol persistence and authorized compaction, `CompleteCompaction` is part of phase 1 rather than a second write. The invariant is atomic tombstone advancement plus deletion, not the function split in the pseudocode. Never call `Advance` after a partial apply, failed snapshot install, unserviced required load, or failed checkpoint result.

### 5. Make client commands deterministic and complete

Encode a versioned payload containing every value needed to apply the command. Assign a nonzero `CommandID{Client, Sequence}` before retrying. Compute the canonical footprint from **logical resources**, not storage-engine keys. Compute `CycleKey` from stable business data when deterministic cycle order should be meaningful; otherwise use a stable command-derived value.

For an atomic financial transfer, one command should contain source, destination, amount, currency, and business id. Its footprint should conservatively include:

```text
acct/<source>
acct/<destination>
dedup/<client>/<sequence>
txn/<client>/<sequence>
```

The apply transaction validates the source balance, decides success or decline deterministically, mutates both balances or neither, writes the transaction/result row, and writes the dedup record atomically. Splitting debit and credit into separate EPaxos commands would expose an intermediate balance and would require an application transaction protocol outside EPaxos.

Every state or response difference must be represented by a conflict resource. Include unique indexes, idempotency rows, inventory counters, result rows, secondary indexes, range sentinels, and schema/version guards when they affect outcomes. Under-declaration is a safety bug. Over-declaration is safe but reduces concurrency.

### 6. Route clients for locality without changing voters

Client routing and consensus membership are different layers. A five-node financial service can assign each account to an adjacent home pair and choose the least-loaded healthy home as the command owner. That reduces the client-to-coordinator path and balances owner lanes. The resulting `PreAccept`, `Accept`, recovery, and commit traffic still targets the instance's full pinned voter configuration; the home pair is not a two-node consensus group.

On timeout, retry the same command id and bytes against either home. Never create a new id merely because the first coordinator reply was lost. The durable application-result table turns a repeated execution into the original response and rejects same-id/different-command reuse.

### 7. Understand the normal proposal state machine

The owner allocates `(local replica, next instance, current configuration)`, computes local attributes, persists `PRE-ACCEPTED`, and broadcasts `PreAccept`. A receiver:

1. Validates cluster/configuration identity, sender incarnation, ballot, checksum, command encoding, timing domain, and canonical footprint.
2. Finds locally known conflicting instances through the point/span/global conflict indexes.
3. Builds the latest dependency per voter lane and sets `Seq` above the maximum dependency sequence.
4. Persists the resulting pre-accepted tuple before sending `PreAcceptResp`.

The optimized fast decision is not "no conflicts exist anywhere." Treat it as this predicate:

```text
candidate covers the owner's durable local attributes
AND candidate is on the initial owner ballot
AND every counted remote reply has the exact candidate (Seq, Deps)
AND every counted vote carries the fast-path-eligible marker
AND counted votes form the configured optimized fast quorum
AND the union of their committed-prefix evidence covers every nonzero Deps slot
```

For odd `N = 2F + 1`, the optimized quorum is `F + floor((F + 1) / 2)`, including the owner. The quorum-intersection proof means a later recovery quorum cannot choose an incompatible history without intersecting the witnesses that persisted this candidate. Exact tuple agreement prevents the coordinator from mixing mutually incompatible reply fragments. Committed-prefix evidence proves that compact dependency entries do not hide an unresolved earlier instance in that lane.

The owner may adopt a reply candidate that monotonically covers its local tuple; the other counted remote replies must match that candidate exactly before commit. If the predicate cannot become true, merge the observed attributes, persist `ACCEPTED`, send `Accept`, and commit after a slow majority accepts that exact value.

### 8. Implement recovery as value selection, not reproposal

Any deterministic recovery coordinator may recover a stalled owner lane. Raise and persist a promise ballot, collect a slow quorum of `PrepareResp`, and choose in this order:

1. A committed value, if any response has one.
2. The accepted tuple with the highest durable `RecordBallot`.
3. A matching fast-path-eligible pre-accepted candidate with the required fast/slow intersection evidence, completed through `TryPreAccept`.
4. A conservative merge of pre-accepted evidence, completed through slow `Accept`.
5. A recovery no-op only when the quorum proves that no value exists.

Do not compare the prepare promise ballot as if it were a value ballot; `RecordBallot` identifies the accepted value. Preserve sender-attributed `AcceptEvidence`. `TryPreAccept` must reject stale ballots, unordered conflicts, and stale dependencies. Ambiguous, incomplete, timed-out, or contradictory evidence falls back to slow `Accept`; it never becomes an optimistic commit.

### 9. Execute the dependency graph deterministically

Execution is a graph problem over committed, not-yet-executed instances. Define an edge `A -> B` when `A` depends on `B`, so `A` must wait for `B`. Resolve configuration slots with the configuration pinned in `A.Ref.Conf`.

1. Start from committed, unexecuted resident records and follow dependency references.
2. Ignore a dependency already covered by a durable executed frontier.
3. Request folded records through `Ready.RecordLoads`; never fabricate a missing dependency.
4. Keep a component blocked if a true external dependency is not committed/executed or a known conflicting pre-accepted/accepted instance can still determine its order.
5. Apply chain pruning before SCC discovery: an edge from `A` to `B` can be pruned only under the implemented rule that `B` already depends on `A` and has the higher sequence number.
6. Run Tarjan's algorithm over the remaining graph:
   - assign `index[v] = lowlink[v] = nextIndex`;
   - push `v` on the active stack;
   - DFS each edge, propagating child lowlinks;
   - for an edge to an active vertex, lower `lowlink[v]` to that vertex's index;
   - when `lowlink[v] == index[v]`, pop through `v` to produce one SCC.
7. Build the SCC condensation DAG. It is acyclic. An SCC is ready only when every outgoing dependency reaches an executed instance or an SCC already selected earlier in this execution pass.
8. Within one ready SCC, sort by ascending `(Seq, CycleKey, InstanceRef)` and emit the whole component in that order.
9. Mark the emitted prefix durable through `Ready`/`Advance`, then repeat until no component is ready.

Tarjan discovery and condensation are `O(V + E)` before the deterministic per-SCC sorts. Never execute graph vertices in hash-map iteration order, sort by `CycleKey` before `Seq`, execute only part of a cycle, or parallelize one `Ready.Apply` slice.

### 10. Integrate TOQ only with an operational clock contract

Enable `Config.TOQ` only after the deployment provides:

- monotonic external time samples passed to `ProcessTOQ(now)`;
- clock synchronization monitoring and a policy that removes or quarantines unhealthy members;
- a conservative per-receiver `TOQOneWayDelay` equal to network delay plus clock-skew/synchronization uncertainty;
- a valid sync group for each live configuration;
- safe handling of rollback, overflow, and unavailable runtime configuration errors.

TOQ sends `PreAccept` with `Seq=0`, empty dependencies, `TOQ=true`, and a computed `ProcessAt`. The owner and receivers delay local dependency assignment until the relevant processing time. The owner's pending record is a hard barrier: early replies may be retained, but they cannot cause commit before local `ProcessAt` clears durably. Due work is ordered by `ProcessAt`, instance reference, then sender. The core consumes samples and bounds; it does not synchronize clocks or measure delay.

### 11. Specify transport behavior explicitly

Frame every message with protocol version, cluster identity, sender/receiver replica and incarnation, pinned configuration, message type, ballot, instance reference, authenticated value fields, and checksum. Enforce size limits before allocation. Use bounded queues and backpressure; dropping every retry indefinitely is a liveness failure.

Delivery is at-least-once. Duplicate and reordered messages are expected. A stale or duplicate message may return `ErrMessageRejected`; treat that as a protocol-level drop, not process corruption. Other validation errors should be observable and fail closed. Never send a message from `Ready` before the records in the same batch are durable.

### 12. Crash, restart, and compaction checklist

Before declaring restart support complete, verify all of the following:

- Restart reconstructs the same cluster identity, local voter incarnation, configuration history, allocator floor, checkpoint, and records.
- A crash before persistence emits no message; a crash after persistence but before send can resend safely.
- A crash after send but before `Advance` can replay the identical `Ready`.
- A crash after application mutation but before `Advance` returns the stored result without applying the effect twice.
- Folded records are loaded only through the `RecordLoads` handshake.
- A compacted reference is answered by certified checkpoint/frontier evidence and is never recreated.
- Snapshot bytes are materialized and digest-checked before the protocol consumes the snapshot.
- Protocol tombstones and physical record deletion commit atomically.

### 13. Verification matrix for a new embedding

Run deterministic scenarios before load testing:

| Area | Required scenario and assertion |
| --- | --- |
| Fast path | Independent commands commit after the configured matching fast quorum; all replicas apply the same bytes. |
| Conflict | Concurrent overlapping commands converge through fast or slow paths and produce one deterministic order. |
| Atomic command | Inject a crash at every persistence/apply boundary; debit and credit are both visible or both absent, and the response is stable. |
| Duplicate client retry | Retry identical id/bytes at another coordinator; one effect and one durable response result. Different bytes with the same id fail closed. |
| Recovery | Crash the owner after pre-accept, after accept, and after commit send; a non-owner recovers the same value. |
| Message faults | Duplicate, drop, delay, and reorder each message class; safety holds and a healthy quorum eventually progresses. |
| Quorum boundary | For five voters, zero, one, and two unavailable nodes retain quorum progress; three unavailable nodes do not commit. |
| Restart | Restart every replica from durable storage at every Ready boundary and compare final records, apply order, and application digest. |
| Graph | Cover chains, diamonds, cycles, blocked external dependencies, chain pruning, and deterministic SCC order. |
| Record folding | Force resident limits, service loads, restart, and compare behavior with an unbounded-resident run. |
| Membership | Add/remove voters with old in-flight instances; old refs use old pinned quorums and new refs use the successor configuration. |
| Checkpoint | Crash before preparation, after certification, and during atomic compaction; no represented command is lost or applied twice. |
| TOQ | Exercise early replies, equal timestamps, conservative delays, clock rollback, configuration refresh, and missing sync-group data. |

Then run race detection, randomized fault campaigns, formal/refinement checks, and long-running resource tests. Measure command latency by path, coordinator distribution, retries, recovery starts, resident/folded records, Ready queue depth, and application-result retention. Throughput degradation under faults should be explained by remaining coordinators, quorum availability, and link delay; a graph alone is not correctness evidence.
