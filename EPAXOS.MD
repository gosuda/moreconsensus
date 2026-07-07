# Implemented EPaxos Algorithm

This document describes the algorithm implemented by `gosuda.org/moreconsensus/epaxos`. It is intentionally more detailed than `README.md`; the README stays feature-level.

## Public execution model

An application owns transport, persistence, and time. A replica is a deterministic `RawNode` with these calls:

1. `Tick` advances one logical tick and processes due timers.
2. `Propose` or `ProposeConfChange` creates local EPaxos instances.
3. `Step` applies one transport `Message` addressed to the local replica.
4. `HasReady` and `Ready` expose records to persist, messages to send, and commands to apply.
5. `Advance` confirms the caller completed the `Ready` obligations.

The core starts no goroutines, reads no wall-clock time, and performs no I/O. Storage and transport are virtualized by the embedding application and by tests.

## Instance identity

Each command is stored in an `InstanceRecord` named by `(ReplicaID, InstanceNum, ConfID)`. `ReplicaID` identifies the owner replica. `InstanceNum` is monotonically allocated by that owner. `ConfID` pins the voter set used by the instance, which makes dependency vectors deterministic.

## Commands and conflicts

A command is opaque payload plus explicit conflict keys. Two user commands conflict when any conflict key is byte-identical. No-op recovery commands conflict with nothing. Configuration-change commands conflict with every non-noop command so membership updates are totally ordered with user commands.

The EPaxos dependency vector stores, for each voter in sorted configuration order, the latest known conflicting instance from that voter. The sequence number is one greater than the maximum sequence number of the dependencies observed while computing attributes.

The Go implementation stores dependency vectors as per-replica instance-number prefixes for the configuration pinned by the instance. During execution, a dependency value `k` for replica `r` means every locally known instance from replica `r` with instance number `<= k` in the same configuration is considered a dependency, except for the instance being executed. This matches EPaxos dependency-vector semantics while keeping the persisted representation compact.

## Quorums

The implementation supports cluster sizes 1, 2, 3, 4, 5, 6, and 7 exactly. Slow quorum is majority: `n/2 + 1`. Fast quorum uses the conservative EPaxos formula `n - ((n - 1) / 4)`, yielding this table:

| Voters | Slow quorum | Fast quorum |
| --- | ---: | ---: |
| 1 | 1 | 1 |
| 2 | 2 | 2 |
| 3 | 2 | 3 |
| 4 | 3 | 4 |
| 5 | 3 | 4 |
| 6 | 4 | 5 |
| 7 | 4 | 6 |

## Pre-accept fast path

When a local application proposes a command, the owner replica:

1. Allocates the next local instance number.
2. Computes initial attributes from local conflict indexes.
3. Persists a `StatusPreAccepted` record in `Ready.Records`.
4. Sends `MsgPreAccept` to other voters.
5. Counts the local vote as one matching pre-accept reply.

A receiving replica validates the message envelope and checksum when present. It recomputes attributes using its local conflict index, merges them with the proposer attributes, persists the pre-accepted record, updates conflict indexes, and replies with `MsgPreAcceptResp` carrying the attributes it stored.

The owner fast-commits when it receives a fast quorum of pre-accept votes and every vote exactly matches the original attributes. The commit changes the record to `StatusCommitted`, persists it, broadcasts `MsgCommit`, and attempts deterministic execution.

## Slow accept path

If a slow quorum of pre-accept replies is available but attributes differ, the owner merges the reply attributes. Without the optional time optimization, it immediately enters the slow accept path. With the optional logical-time optimization, it may wait a bounded number of ticks for enough additional matching replies to still fast-commit. This optimization is purely tick-driven and has no wall-clock dependency.

The slow path persists `StatusAccepted`, broadcasts `MsgAccept`, and waits for a slow quorum of `MsgAcceptResp`. After that quorum, the owner commits the merged attributes and broadcasts `MsgCommit`.

## Recovery path

The implementation includes prepare/recovery message types and a timer-driven recovery transition. When a ballot rejection or stalled timer requires recovery, the owner raises its ballot, broadcasts `MsgPrepare`, collects a slow quorum of `MsgPrepareResp`, chooses a committed value if any response contains one, otherwise chooses the highest-ballot accepted or pre-accepted value, merges attributes, and finishes through `MsgAccept` before committing. Periodic logical-tick commit rebroadcasts help replicas that missed a commit during a transport outage.

## Execution ordering

Committed records are not emitted directly. The node builds a dependency graph over committed but unexecuted instances. Edges point from an instance to each locally known committed dependency represented by its dependency vector. Strongly connected components are collapsed. A component is executable when all dependencies outside the component are already executed or absent. Commands inside a component are emitted in deterministic `(Seq, ReplicaID, InstanceNum)` order.

The executor also waits when it knows a conflicting pre-accepted or accepted instance that has not committed yet. This prevents a replica from emitting a command while a known conflicting command is still unresolved.

User commands are emitted to the embedding application through `Ready.Committed`. No-op recovery commands have no application effect. Configuration changes are applied inside the core when their component executes because they update future quorum membership.

## Configuration changes

`ProposeConfChange` validates that a membership operation changes the current voter set and keeps the size within the supported one-to-seven-replica range, then encodes it as a configuration command. Only one configuration change may be pending. The command conflicts with every non-noop command. When the command executes, future instances use the new sorted voter set and an incremented configuration id. Existing instances remain pinned to their original configuration id.

## Ready contract

`Ready.Records` are durable consensus state. `Ready.Messages` are transport work. `Ready.Committed` contains user commands that have passed dependency execution and must be applied by the embedding application before they are acknowledged. The embedding application must persist every record in a returned `Ready` before sending messages or acknowledging committed commands from that same batch. `MustSync` is true when durable records are present.

`Advance` returns an error and acknowledges only an exact prefix of the outstanding `Ready`. The acknowledged values must match the records, messages, committed commands, and `MustSync` bit returned by `Ready`; otherwise `Advance` returns `ErrInvalidReady` and leaves the outstanding batch unchanged for retry. A record-only prefix is valid, which lets storage persist part of a batch without marking application commands executed. `Messages` or `Committed` acknowledgements are accepted only after all earlier `Records` from that outstanding batch are acknowledged. When `MaxReadyMessages` caps `Ready.Messages`, records and committed commands still appear in full; after their successful acknowledgement the remaining message tail appears in later `Ready` batches with `MustSync` false.

For user commands, `Advance` queues the corresponding `StatusExecuted` records only after the application has observed and acknowledged `Ready.Committed`; those executed markers appear in a later `Ready.Records` batch and make replay after restart idempotent. Configuration changes and no-op commands do not appear in `Ready.Committed`; their executed records are queued internally when they execute.

## Checksums and serialization

`InstanceRecord` and `Message` checksums are canonical BLAKE3 digests. The digest includes fixed-endian primitive fields, length-prefixed slices, command payload, and conflict keys. The checksum field itself is excluded from its digest. The wire codec appends the checksum at the end of each encoded message. Decode returns typed errors on malformed input or checksum mismatch and never panics for malformed network bytes.

## Allocation and ownership

The public API provides both safe-copy and zero-copy ownership paths. By default, proposals are copied into the node. When `Config.ZeroCopyProposals` is true, the caller transfers ownership of proposal payload and conflict-key slices and must not mutate or reuse them while they remain observable through `Ready` or `Status`.

`DecodeMessage` returns command payload and conflict-key byte slices backed by the input buffer while allocating decoded metadata slices such as dependencies. `DecodeMessageWithScratch` accepts `DecodeScratch` for transports that want to reuse dependency and conflict-key slice-header storage; decoded metadata aliases the scratch until the scratch is reused or reset, while payload and key bytes still alias the input buffer. Decode errors reset the destination message; when a scratch is supplied, the active scratch metadata is cleared while its capacity is retained.

`Step` copies inbound command bytes before storing them, so callers may reuse decode buffers after `Step` returns. `GetMessage`, `PutMessage`, `GetCommand`, and `PutCommand` expose sync.Pool-aware helpers for transport integrations.
