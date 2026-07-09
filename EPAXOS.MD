# Implemented EPaxos Algorithm

This document describes the algorithm implemented by `gosuda.org/moreconsensus/epaxos`. It is intentionally more detailed than `README.md`; the README stays feature-level. For property-by-property proof rationale, failure-count boundaries, and evidence/non-claims, see `EPAXOS_IMPLEMENTATION_PROOF.md`.

## Public execution model

An application owns transport, persistence, and time. A replica is a deterministic `RawNode` with these calls:

1. `Tick` advances one logical tick and processes due timers.
2. `Propose` or `ProposeConfChange` creates local EPaxos instances.
3. `Step` applies one transport `Message` addressed to the local replica.
4. `HasReady` and `Ready` expose records to persist, messages to send, and commands to apply.
5. `Advance` confirms the caller completed the `Ready` obligations.

The core starts no goroutines, performs no I/O, and does not call the OS wall clock directly. Non-TOQ timing uses logical ticks; TOQ mode reads time only through the caller-supplied `TOQClock` function. Storage and transport are virtualized by the embedding application and by tests.

## Paper claim map

| Paper claim | Paper anchor | Repository implementation | Release status |
| --- | --- | --- | --- |
| Optimized EPaxos fast quorum | EPaxos SOSP 2013 §4.3/§4.4, ACM proceedings pp.362-365, and the EPaxos technical report §6 define the optimized fast quorum as `F + floor((F + 1) / 2)` including the command leader for odd `N = 2F + 1` clusters. That permits `N=3 -> 2`, `N=5 -> 3`, and `N=7 -> 5` pre-accept votes, with matching recovery evidence obligations. | Odd supported cluster sizes use the paper threshold (`N=3 -> 2`, `N=5 -> 3`, `N=7 -> 5`); even supported sizes keep the previous conservative quorum because the paper proof assumes `N=2F+1`. Fast commits also require FP-deps-committed prefix evidence tied to the matching fast quorum. | Implemented normal fast-path threshold/evidence for odd supported sizes plus Go committed stale-dependency evidence-search/resend-ignore; normal un-timed originator-disagrees fast commit, even-size optimized quorums, and unbounded proof remain non-claims. |
| EPaxos Revisited TOQ | EPaxos Revisited NSDI 2021 §4/§4.1, USENIX proceedings pp.616-617, requires synchronized physical clocks, one-way-delay estimates with clock-skew/sync margin and sync groups, outgoing `PreAccept` messages with sequence number `0` and empty dependencies, and delayed originator dependency assignment at `ProcessAt`. | `Config.TOQ` is an explicit mode separate from `Config.TimeOptimization`. The embedding application supplies `TOQClock`, `TOQOneWayDelay` that includes one-way network delay plus clock-skew/sync uncertainty margin, and an optional `TOQSyncGroup`; the core computes `ProcessAt`, persists a `StatusNone`/`TOQPending` record with the command, sends flagged `MsgPreAccept{TOQ:true, Seq:0, Deps:nil}`, delays local dependency assignment until `ProcessAt`, and makes receivers compute attributes at the flagged `ProcessAt`. The core does not synchronize clocks or measure one-way delay. | Implemented core TOQ behavior with caller-supplied synchronized-clock and OWD/skew-margin discipline; real clock synchronization and delay measurement are embedding obligations. |
| EPaxos Revisited chain pruning | EPaxos Revisited NSDI 2021 §3, USENIX proceedings p.616, prunes dependency `B` from `A` when `B` already depends on `A` and has higher sequence number. | Dependency-graph construction applies this pruning before SCC execution. | Implemented claim. |
| EPaxos recovery | EPaxos SOSP 2013 §4.6, ACM proceedings p.366, requires prepare/accept recovery, try-pre-accept conflict checks, and dependency evidence sufficient for the quorum formula in use. CMU-PDL-13-111 §6/§6.2 also describes Accept-Deps evidence carried on Accept/AcceptReply for optimized recovery corner cases. | The implementation has owner-independent prepare, durable fast-path markers, FP-deps-committed prefix evidence on `MsgPreAcceptResp`, durable `InstanceRecord.RecordBallot` value-ballot evidence separate from promise `Ballot`, recovery-only aggregate `AcceptSeq`/`AcceptDeps` plus sender-preserving `AcceptEvidence`, TryPreAccept committed stale-dependency evidence search with read-only `MsgEvidence`/`MsgEvidenceResp`, authorized `IgnoreDependency` resends, fail-closed slow accept, and stopped-owner recovery for the implemented quorum table. | Implemented safety-focused recovery path for the finite evidence suite and Go committed stale-dependency evidence-search/resend-ignore for supported `F <= 3`; complete TLA coverage of the new query path, full deferred-cycle decision-tree parity, and unbounded proof remain non-claims. |

Primary paper references: [EPaxos SOSP 2013](https://sigops.org/s/conferences/sosp/2013/papers/p358-moraru.pdf), [EPaxos technical report CMU-PDL-13-111](https://www.pdl.cmu.edu/PDL-FTP/associated/CMU-PDL-13-111.pdf), and [EPaxos Revisited NSDI 2021](https://www.usenix.org/system/files/nsdi21-tollman.pdf).

## Instance identity

Each command is stored in an `InstanceRecord` named by `(ReplicaID, InstanceNum, ConfID)`. `ReplicaID` identifies the owner replica. `InstanceNum` is monotonically allocated by that owner. `ConfID` pins the voter set used by the instance, which makes dependency vectors deterministic.

## Commands and conflicts

A command is opaque payload plus explicit conflict keys. Two user commands conflict when any conflict key is byte-identical. No-op recovery commands conflict with nothing. Configuration-change commands conflict with every non-noop command so membership updates are totally ordered with user commands.

The EPaxos dependency vector stores, for each voter in sorted configuration order, the latest known conflicting instance from that voter. The sequence number is one greater than the maximum sequence number of the dependencies observed while computing attributes.

The Go implementation stores dependency vectors as per-replica instance-number prefixes for the configuration pinned by the instance. During execution, a dependency value `k` for replica `r` means every locally known instance from replica `r` with instance number `<= k` in the same configuration is considered a dependency, except for the instance being executed. This matches EPaxos dependency-vector semantics while keeping the persisted representation compact.

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

`Config.TOQ` follows the EPaxos Revisited TOQ shape instead of the legacy attribute-carrying pre-accept. The owner samples `TOQClock` once, sets `ProcessAt` to the maximum of that sample plus the configured one-way delay for every member of `TOQSyncGroup` (defaulting to the voters), persists the command as `StatusNone` with `TOQPending=true`, and does not index the command as a local conflict yet. Outbound TOQ `MsgPreAccept` messages are explicit: `TOQ=true`, `Seq=0`, and empty `Deps`. Retries keep that same explicit TOQ envelope even after the owner has assigned its local attributes.

For TOQ deployments, every `TOQOneWayDelay[id]` value is a conservative delivery bound supplied by the embedding application. It must include the measured one-way network delay plus the maximum clock-skew/synchronization uncertainty for that receiver and sync group. The core does not implement clock synchronization or delay measurement.

At local `ProcessAt`, the owner computes attributes from conflicts ordered before the TOQ timestamp, clears `TOQPending`, persists the ordinary `StatusPreAccepted` record, indexes the command as a conflict, seeds the local pre-accept vote, and only then evaluates fast/slow pre-accept completion. `MsgPreAcceptResp` votes that arrive while the local record is still pending are stored but cannot commit, fast-wait, or slow-accept the instance until the pending assignment is durably cleared.

A receiving replica validates the message envelope and checksum when present. An unflagged `MsgPreAccept` must carry a full-width dependency vector; a flagged TOQ `MsgPreAccept` must carry the zero-sequence/empty-dependency envelope. Future pre-accepts are queued, with command bytes cloned, until the appropriate clock reaches `ProcessAt`: logical `Tick` for `TimeOptimization`, and `TOQClock` for TOQ. Due pre-accepts are processed by `ProcessAt`, then instance reference, then sender; this keeps equal-time processing deterministic across replicas. When an unflagged message is due, the receiver recomputes attributes, ignores timed pre-accepted conflicts ordered after the message, merges proposer attributes, persists the pre-accepted record, updates conflict indexes, and replies with `MsgPreAcceptResp` carrying the attributes it stored, its exact/default fast-path marker, and committed-prefix dependency evidence for those attributes. When a flagged TOQ message is due, the receiver computes attributes at `ProcessAt` and replies with those locally computed attributes; it does not merge legacy proposer attributes from the zero envelope.

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

`MsgTryPreAccept` is the EPaxos optimized-recovery check: witnesses reject stale ballots, committed conflicts, mutually unordered uncommitted conflicts, and stale-dependency conflicts. A committed stale-dependency rejection for a conflict already present in the candidate's `Deps` starts a read-only `MsgEvidence`/`MsgEvidenceResp` check for that committed conflict, keyed by the candidate ref. Once the coordinator has `F` unique checked replicas for the committed conflict tuple and no sender-preserving Accept/AcceptReply evidence from a sender forced into the candidate's possible fast quorum whose deps omit the candidate, it resends `MsgTryPreAccept` with `IgnoreDependency.Ref` for that one conflict. The witness consumes that marker only for the named committed stale dependency. Conflicting duplicate evidence, legacy-only evidence, missing tuple evidence, an evidence retry timeout, or a second simultaneous ignored conflict fails closed to slow `MsgAccept`; the slow path folds the same-configuration `ConflictRef` into `Deps` and bumps `Seq` using the committed tuple sequence when known. Uncommitted conflicts either force slow `MsgAccept` when leader-in-fast-quorum or recorded deferral-cycle evidence says the candidate fast quorum must contain the conflict leader, or they defer the candidate while recovering the blocker. Successful conceptual witnesses from prepare plus actual try-pre-accept witnesses advance to `MsgAccept`; recovery records and commit records do not retain `FastPathEligible`. Periodic logical-tick commit rebroadcasts help replicas that missed a commit during a transport outage.

## Execution ordering

Committed records are not emitted directly. The node builds a dependency graph over committed but unexecuted instances. Edges point from an instance to each locally known committed dependency represented by its dependency vector. Strongly connected components are collapsed. A component is executable when all dependencies outside the component are already executed or absent. Commands inside a component are emitted in deterministic `(Seq, ReplicaID, InstanceNum)` order.

The executor also waits when it knows a conflicting pre-accepted or accepted instance that has not committed yet. This prevents a replica from emitting a command while a known conflicting command is still unresolved.

EPaxos Revisited chain pruning is applied inside dependency-graph construction: when dependency `B` already depends on `A` and has a higher sequence number, `A` does not need to keep `B` in its blocking component because `B` must execute after `A`.

User commands are emitted to the embedding application through `Ready.Committed`. No-op recovery commands have no application effect. Configuration changes are applied inside the core when their component executes because they update future quorum membership.

## Configuration changes

`ProposeConfChange` validates that a membership operation changes the current voter set and keeps the size within the supported one-to-seven-replica range, then encodes it as a configuration command. A node treats any observed, unexecuted configuration command as a membership barrier: local user proposals and additional local configuration proposals are rejected until the barrier executes. Configuration commands conflict with every non-noop command, and user-command attribute computation also includes known configuration-command instances even when user conflict keys are disjoint. When a configuration command executes, future instances use the new sorted voter set and an incremented configuration id. Existing instances remain pinned to their original configuration id: their dependency-vector width, slow/fast quorum thresholds, retries, prepare/accept/commit broadcasts, and recovery quorums are selected from `Ref.Conf`, not from the node's current `ConfState`. A replica that has applied a configuration excluding itself rejects `Propose` and `ProposeConfChange` before allocating an instance; it can still step messages for older instances whose `Ref.Conf` includes it. A stale same-generation configuration command that executes after a later configuration is already installed is recorded as executed but does not overwrite the installed configuration history.

On restart, `NewRawNode` loads durable records, remembers any stored historical configurations, then replays executed configuration-change records in instance order to rebuild intermediate configuration ids that are needed by old in-flight instances. Replay rejects a stored configuration id whose voters conflict with the voters deterministically produced by executed configuration commands. A replayed unexecuted configuration command remains a pending barrier, so the restarted node still rejects new local proposals until that command executes.

When TOQ is enabled, default `TOQSyncGroup` selection and one-way-delay validation run after executed configuration replay, so an omitted sync group defaults to the replayed current voters and an explicit stale sync group containing a removed voter is rejected.
Recovery, retry, and broadcast paths select voter sets and slow-quorum thresholds by the instance `Ref.Conf`, so an old instance can still use a removed voter for its old pinned quorum, newly added voters cannot count for pre-addition instances, and current-configuration proposals from a removed local replica remain rejected.

The finite configuration evidence currently covers an executed add/remove durable replay slice plus staged old-instance recovery-after-removal, lost+duplicate recovery response de-duplication, and recovery-after-addition slices. Arbitrary durable histories, arbitrary recovery under reconfiguration, retries/timer rebroadcast, arbitrary message loss, joint consensus, and unbounded membership changes remain outside the current claim.

## Ready contract

`Ready.Records` are durable consensus state. `Ready.Messages` are transport work. `Ready.Committed` contains user commands that have passed dependency execution and must be applied by the embedding application before they are acknowledged. The embedding application must persist every record in a returned `Ready` before sending messages or acknowledging committed commands from that same batch. `MustSync` is true when durable records are present.

`Advance` returns an error and acknowledges only an exact prefix of the outstanding `Ready`. The acknowledged values must match the records, messages, committed commands, and `MustSync` bit returned by `Ready`; otherwise `Advance` returns `ErrInvalidReady` and leaves the outstanding batch unchanged for retry. A record-only prefix is valid, which lets storage persist part of a batch without marking application commands executed. `Messages` or `Committed` acknowledgements are accepted only after all earlier `Records` from that outstanding batch are acknowledged. When `MaxReadyMessages` caps `Ready.Messages`, records and committed commands still appear in full; after their successful acknowledgement the remaining message tail appears in later `Ready` batches with `MustSync` false.

`Ready` remains visible until `Advance` accepts it. If durable storage or application work fails before `Advance`, the caller can call `Ready` again and retry the same batch. The example KV applier writes an applied marker keyed by consensus instance in the same Pebble batch as the KV mutation, so a crash or `Advance` failure after the Pebble commit replays the consensus command without creating a second KV version.

For user commands, `Advance` queues the corresponding `StatusExecuted` records only after the application has observed and acknowledged `Ready.Committed`; those executed markers appear in a later `Ready.Records` batch and make replay after restart idempotent. Configuration changes and no-op commands do not appear in `Ready.Committed`; their executed records are queued internally when they execute.

## Checksums and serialization

`InstanceRecord` and `Message` checksums are canonical BLAKE3 digests. The digest includes fixed-endian primitive fields, length-prefixed slices, command payload, and conflict keys. The checksum field itself is excluded from its digest. The wire codec appends the checksum at the end of each encoded message. Decode returns typed errors on malformed input or checksum mismatch and never panics for malformed network bytes.

The example KV storage rejects persisted EPaxos record key/value reference mismatches during load. Its checkpoint repair path uses whole-directory replacement from a semantically verified checkpoint; the live-source helper additionally requires live quorum support and a target-owned next-instance floor before reopening a stopped corrupt member. It never repairs by deleting corrupt records or recomputing checksums.

## Allocation and ownership

The public API provides both safe-copy and zero-copy ownership paths. By default, proposals are copied into the node. When `Config.ZeroCopyProposals` is true, the caller transfers ownership of proposal payload and conflict-key slices and must not mutate or reuse them while they remain observable through `Ready` or `Status`.

`DecodeMessage` returns command payload and conflict-key byte slices backed by the input buffer while allocating decoded metadata slices such as dependencies. `DecodeMessageWithScratch` accepts `DecodeScratch` for transports that want to reuse dependency and conflict-key slice-header storage; decoded metadata aliases the scratch until the scratch is reused or reset, while payload and key bytes still alias the input buffer. Decode errors reset the destination message; when a scratch is supplied, the active scratch metadata is cleared while its capacity is retained.

`Step` copies inbound command bytes before storing them, so callers may reuse decode buffers after `Step` returns. `GetMessage`, `PutMessage`, `GetCommand`, and `PutCommand` expose sync.Pool-aware helpers for transport integrations.
