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

When executed history reaches the retention threshold, replicas may originate group-global checkpoint barriers. The executor recovers every real dependency hole, executes the complete barrier SCC, and emits one stable `Ready.Checkpoint{ID, Through}` after all represented application work. `ProvideCheckpoint` returns an opaque bounded snapshot handle and nonzero application digest.

A descriptor binds cluster/config identities and incarnations, barrier tuple, exact multi-configuration execution frontier, protocol projection, allocator fences, and application digest. Voters attest only after prepared snapshot metadata is durable. A canonical slow-quorum certificate must itself become durable before `Ready.Compact` authorizes atomic tombstone advancement and record deletion. A late compacted reference never recreates protocol state.

Restart validates the checkpoint and certificate, installs the application snapshot first, seeds compacted dependency/execution summaries, and loads only records after `CompactedThrough`. Executed delta application commands may replay and rely on application result dedup. Prepared or certified-but-not-compacted checkpoints retain full history. Received snapshot handles are materialized and digest/size verified by the embedding before `Step`.

## KV and MVCC integration

Logical resources, not physical MVCC `key/timestamp` suffixes, belong in footprints. Point reads, writes, deletes, and intents use a logical point; scans, delete-ranges, and range tombstones use a half-open span. Cross-resource invariants use deterministic namespaces such as `row/`, `index/`, `unique/`, `txn/`, and `schema/` plus sentinel points. Group maintenance uses `All`, which covers one EPaxos group rather than an entire database.

MVCC timestamps, intents, transaction records, timestamp caches, retries, GC thresholds, and closed timestamps remain application state. A write applied at timestamp 10 may be visible to a later read at timestamp 10; only a read that already observed the old value at 10 forces an overlapping later write above 10 or a retry.
