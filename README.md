# moreconsensus

`moreconsensus` is a Go library for building replicated services with Egalitarian Paxos (EPaxos). Applications drive a deterministic `RawNode`; the embedding owns transport, durable and application state, response deduplication, snapshots, and any wall-clock sampling. Opaque commands carry only canonical logical point/span/all footprints and replicated cycle-order bytes for the core to interpret.

## Core features

- EPaxos fast path, slow accept path, commit broadcast, and owner-independent recovery.
- Deterministic logical ticks for protocol timing.
- Caller-owned storage and transport virtualization.
- Opaque application commands separated from protocol controls; only application entries produce ordered `Ready.Apply` work.
- Canonical byte-lexicographic point, half-open span, and explicit group-wide `All` conflict scopes backed by an overlap index.
- Deterministic SCC ordering by `Seq`, `CycleKey`, then instance reference.
- Certified exact-frontier checkpoints, content-addressed application snapshots, durable protocol compaction, and checkpoint-plus-delta restart.
- Safe-copy and explicit zero-copy ownership paths.
- Pool-aware message and command helpers.
- Canonical BLAKE3 checksums for records and messages.
- Deterministic simulation coverage for cluster sizes 1 through 7.
- A Pebble-backed distributed key-value example in its own Go module.
- Separate client, peer, and administrative service planes with TLS 1.3 mutual-authentication support.
- Repository gates for Go behavior, race testing, bounded finite model checking, fault simulation, and release-scope audits.

`Ready` is an exact-prefix retry contract. The embedding persists protocol state, sends messages, installs received snapshots, applies `Ready.Apply` in order, services checkpoint requests, performs compaction, and then calls `Advance`. Apply work may repeat before acknowledgement or after crash; command effects and the full `CommandID` response/digest record must therefore commit atomically. Footprints may omit only truly strongly commutative work: final state, responses, dedup state, and deterministic side effects must all be order-independent.

## Documentation

- [EPAXOS.MD](EPAXOS.MD) describes the implemented algorithm and public execution model.
- [EPAXOS_IMPLEMENTATION_PROOF.md](EPAXOS_IMPLEMENTATION_PROOF.md) explains property claims, failure-count boundaries, proof rationale, and evidence limits.
- [MODEL_EQ_REPORT.MD](MODEL_EQ_REPORT.MD) maps finite TLA+ models to implementation behavior and direct tests.
- [RELEASE_SCOPE.md](RELEASE_SCOPE.md) is the self-contained release-scope authority for closed items, open items, and non-claims.
- [release/EPAXOS_READINESS_EVIDENCE.md](release/EPAXOS_READINESS_EVIDENCE.md) records the current evidence snapshot and release-gate result.
- [tla](tla) contains the finite model suite; [tests](tests) contains verification and release-audit commands.
- [examples/kv](examples/kv) contains the key-value integration example.
- [jepsen](jepsen) contains the workload harness for separately approved external validation.
- The key-value example includes local lifecycle and deterministic fault procedures under `tests`; these reports remain bounded verification evidence.

## Support boundary

- The production library surface is `gosuda.org/moreconsensus/epaxos`; the key-value service is an integration example and validation harness, not a multi-tenant product.
- Voter sets support one through seven replicas. The core uses deterministic logical time; explicit TOQ samples and operational clock discipline remain embedder responsibilities.
- The example KV uses logical MVCC resources rather than physical version keys: point reads/writes use logical points; scans and range operations use half-open spans; cross-resource invariants use namespaced sentinels; `All` covers one EPaxos group, not an entire database.
- Formal and executable evidence is bounded finite model checking plus focused tests, including certified compaction and three-replica checkpoint/restart. It is not an exhaustive or unbounded Go/TLA refinement proof and does not establish real-network production readiness.
- Certified protocol-state compaction is implemented and boundedly exercised; the release decision remains **no-go** under [RELEASE_SCOPE.md](RELEASE_SCOPE.md) while unbounded Go/TLA action correspondence remains open.

## Module layout

The main module is `gosuda.org/moreconsensus`. Example dependencies are isolated in example modules and included by the committed Go workspace.
