# moreconsensus

`moreconsensus` is a Go library for building replicated services with Egalitarian Paxos (EPaxos). The public API follows the shape of etcd raft: applications drive a deterministic `RawNode`, persist `Ready` records, send `Ready` messages, apply committed commands, and then call `Advance` with the acknowledged `Ready` prefix.

## Core features

- EPaxos fast path, slow accept path, commit broadcast, and recovery hooks.
- Deterministic logical ticks for all timing behavior.
- Caller-owned transport and storage virtualization.
- Pool-aware message and command helpers.
- Zero-copy command payload/key decode, reusable decode metadata buffers, and explicit proposal ownership options.
- BLAKE3 checksums through `github.com/zeebo/blake3` as the only intentional main-module runtime dependency.
- Deterministic simulation tests for cluster sizes 1 through 7.
- A Pebble-backed distributed key-value example in its own Go module, including atomic Pebble persistence for EPaxOS records and applied key-value writes.
- The key-value example exposes deterministic timestamp bounds, bounded staleness, and exact staleness reads over explicit record timestamps.
- The example HTTP service is built explicitly with `go build -tags kvnode ./cmd/kvnode` from `examples/kv`.
- The example HTTP service separates client, peer-replication, and administrative APIs onto `-listen`, `-peer-listen`, and `-admin-listen`, supports optional TLS via `-tls-cert`, `-tls-key`, and `-tls-ca`, and has configurable client-facing and peer deadline budgets via `-request-deadline-ms` and `-peer-deadline-ms`.
- Request body limits are configured per plane with `-max-client-body-bytes`, `-max-peer-body-bytes`, and `-max-admin-body-bytes`.
- Repository verification gates run from `tests/ci.sh`, including `tests/chaos_fault_campaign.sh` for focused core fault tests plus local Jepsen restart, transport-partition, storage-unavailable, and destructive-storage profiles. Current release validation is simulation/local-loopback scoped.

## Documentation

- [EPAXOS.MD](EPAXOS.MD) describes the implemented algorithm in detail.
- [EPAXOS_IMPLEMENTATION_PROOF.md](EPAXOS_IMPLEMENTATION_PROOF.md) explains the implemented algorithm, property claims, failure-count boundaries, proof rationale, and evidence/non-claims in detail.
- [MODEL_EQ_REPORT.MD](MODEL_EQ_REPORT.MD) describes the current TLA+ model correspondence and implementation verification scope.
- [RELEASE_SCOPE.md](RELEASE_SCOPE.md) is the self-contained release-scope lock for closed items, open items, and non-claims.
- [tla/EPaxos.tla](tla/EPaxos.tla), [tla/EPaxosResponses.tla](tla/EPaxosResponses.tla), [tla/EPaxosRecovery.tla](tla/EPaxosRecovery.tla), [tla/EPaxosOptimizedRecovery.tla](tla/EPaxosOptimizedRecovery.tla), [tla/EPaxosTryPreAcceptBranches.tla](tla/EPaxosTryPreAcceptBranches.tla), [tla/EPaxosTryPreAcceptMessagePath.tla](tla/EPaxosTryPreAcceptMessagePath.tla), [tla/EPaxosTryConflictForce.tla](tla/EPaxosTryConflictForce.tla), [tla/EPaxosEvidenceQuery.tla](tla/EPaxosEvidenceQuery.tla), [tla/EPaxosConfigBarrier.tla](tla/EPaxosConfigBarrier.tla), [tla/EPaxosConfigTransition.tla](tla/EPaxosConfigTransition.tla), [tla/EPaxosConfigRemoveTransition.tla](tla/EPaxosConfigRemoveTransition.tla), [tla/EPaxosConfigChainTransition.tla](tla/EPaxosConfigChainTransition.tla), [tla/EPaxosRevisited.tla](tla/EPaxosRevisited.tla), [tla/TOQClockDiscipline.tla](tla/TOQClockDiscipline.tla), [tla/ReadyAdvance.tla](tla/ReadyAdvance.tla), [tla/Quorum.tla](tla/Quorum.tla), [tla/KVTimestampStaleness.tla](tla/KVTimestampStaleness.tla), and [tla/KVOmissionRecovery.tla](tla/KVOmissionRecovery.tla) contain the finite executable formal models checked by CI.
- [tla/EPaxosEvidenceStaleness.tla](tla/EPaxosEvidenceStaleness.tla) contains the finite committed-conflict evidence staleness request-scoping model.
- [examples/kv](examples/kv) contains the Pebble/MyRocks-style key-value example.
- [jepsen](jepsen) contains the Jepsen workload harness for external validation.
- [tests](tests) contains the repository verification scripts used by CI.

## Support boundary and limits

- The production library surface is `gosuda.org/moreconsensus/epaxos`: `RawNode`, `Storage`, `Ready`, `Message`, `Command`, and the checksum/codec helpers. The Pebble-backed KV service under `examples/kv` is an executable validation harness and integration example, not an authenticated multi-tenant product.
- Voter sets are sorted, unique, nonzero replica IDs with cluster size 1 through 7. Configuration changes must keep the voter count in that range.
- The core starts no goroutines, performs no I/O, and does not call the OS wall clock. Non-TOQ protocol timing is deterministic logical ticks; explicit TOQ mode uses a caller-supplied synchronized-clock callback and caller-supplied conservative delay bounds that include one-way network delay plus clock-skew/synchronization margin.
- Fast quorums use the optimized EPaxos paper threshold for odd supported cluster sizes and conservative thresholds for even sizes, with FP-deps-committed prefix evidence required before fast commit. Accept/AcceptReply carry sender-preserving recovery-only Accept-Deps evidence that is persisted, propagated through commit/prepare/evidence responses, and used by TryPreAccept committed stale-dependency checks without changing chosen execution attributes. Explicit EPaxos Revisited TOQ core behavior is implemented behind `Config.TOQ`; the embedding application remains responsible for real clock synchronization and delay measurement. The unbounded optimized-recovery proof and complete TLA coverage of the read-only evidence-query path remain non-claims.
- Formal evidence is the finite TLC suite listed in `MODEL_EQ_REPORT.MD`; it is not an unbounded proof.
- The HTTP KV example exposes separate client, peer, and admin listener planes. The peer replication endpoint is POST-only. TLS is optional transport security only; it does not add authentication or authorization.
- HTTP body limits are `-max-client-body-bytes`, `-max-peer-body-bytes`, and `-max-admin-body-bytes`. Scan result cardinality is bounded by `-max-scan-limit`; scans must use `prefix` or `start`/`end`.
- Point `PUT` and `GET /kv/{key}` carry raw bytes. Transaction JSON accepts text `value` or binary-safe `value_b64`; scans return `value_b64` when the value is not valid UTF-8 JSON text.
- Latest point reads wait for the key's consensus barrier. Latest scans in the HTTP KV node wait on an internal scan-barrier conflict key shared by that node's HTTP write path; this is not a protocol-native range/prefix predicate for arbitrary applications.
- Bit-flipped persisted EPaxos records are detected by checksum and reject restart. The KV example has tested offline whole-directory restore, semantic checkpoint verification, explicit `RepairFromCheckpoint` replacement, and live-source `RecoverReplicaFromLiveCheckpoint` replacement for one stopped/corrupt member while a healthy quorum remains. In-place Pebble/WAL repair, checksum recomputation/deletion, synthesized reconstruction without a verified checkpoint, multi-replica/quorum-loss repair, and target-environment drills remain outside the current release claim.

## API contracts

- Embedders must persist every `Ready.Records` entry before sending `Ready.Messages` or acknowledging `Ready.Committed` from the same batch. `Ready.MustSync` means durable records are present.
- Embedders must apply `Ready.Committed` exactly once at the application layer before acknowledging those committed commands with `Advance`.
- `Advance` accepts only an exact prefix of the outstanding `Ready`; invalid or out-of-order acknowledgements leave the batch outstanding for retry.
- Storage implementations must return durable records with valid BLAKE3 checksums and must treat checksum mismatch as a hard load/apply error; recovery uses whole-directory replacement from a semantically verified checkpoint, not record deletion or checksum repair.
- Transport integrations may use `EncodeMessage`, `DecodeMessage`, and `DecodeMessageWithScratch`; `Step` clones inbound command bytes before retaining them.
- Conflict semantics are exact byte equality over `Command.ConflictKeys`. Applications that need range, prefix, or predicate conflicts must encode their own sentinel conflict keys on every relevant command.
- With `Config.ZeroCopyProposals`, callers transfer ownership of proposal payload and conflict-key slices to the node until those bytes are no longer observable through `Ready` or `Status`.

## Operational validation

- `bash tests/chaos_fault_campaign.sh` runs the CI-wired local fault campaign: focused core restart, delivery fault, storage fault, partition, pause, skew, storage/wire rollback simulation, and rollback tests; Jepsen harness unit tests; and local restart, transport, storage, and destructive-storage Jepsen profiles.
- Local Jepsen loopback runs use separate port ranges for client, peer, and administrative planes; override them with `JEPSEN_BASE_PORT`, `JEPSEN_PEER_BASE_PORT`, and `JEPSEN_ADMIN_BASE_PORT`.
- `JEPSEN_LOCAL_FAULTS=destructive-storage bash tests/jepsen_local.sh` runs the loopback KV cluster while each selected process is stopped, its Pebble directory is moved aside, and the original directory is restored before the process rejoins.
- External multi-host Jepsen validation is outside current release evidence. Use the local loopback commands above for the repository's simulation-scoped validation, and treat any separately run external validation as optional operator evidence.

## Module layout

The main module is `gosuda.org/moreconsensus`. Example dependencies are isolated in example modules and included by the committed Go workspace.
