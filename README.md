# moreconsensus

`moreconsensus` is a Go library for building replicated services with Egalitarian Paxos (EPaxos). Applications drive a deterministic `RawNode`, persist `Ready` records, send transport messages, apply committed commands, and acknowledge the exact `Ready` prefix with `Advance`.

## Core features

- EPaxos fast path, slow accept path, commit broadcast, and owner-independent recovery.
- Deterministic logical ticks for protocol timing.
- Caller-owned storage and transport virtualization.
- Safe-copy and explicit zero-copy ownership paths.
- Pool-aware message and command helpers.
- Canonical BLAKE3 checksums for records and messages.
- Deterministic simulation coverage for cluster sizes 1 through 7.
- A Pebble-backed distributed key-value example in its own Go module.
- Separate client, peer, and administrative service planes with TLS 1.3 mutual-authentication support.
- Repository gates for Go behavior, race testing, bounded finite model checking, fault simulation, and release-scope audits.

## Documentation

- [EPAXOS.MD](EPAXOS.MD) describes the implemented algorithm and public execution model.
- [EPAXOS_IMPLEMENTATION_PROOF.md](EPAXOS_IMPLEMENTATION_PROOF.md) explains property claims, failure-count boundaries, proof rationale, and evidence limits.
- [MODEL_EQ_REPORT.MD](MODEL_EQ_REPORT.MD) maps finite TLA+ models to implementation behavior and direct tests.
- [RELEASE_SCOPE.md](RELEASE_SCOPE.md) is the self-contained release-scope authority for closed items, open items, and non-claims.
- [release/EPAXOS_READINESS_EVIDENCE.md](release/EPAXOS_READINESS_EVIDENCE.md) records the current evidence snapshot and release-gate result.
- [tla](tla) contains the finite model suite; [tests](tests) contains verification and release-audit commands.
- [examples/kv](examples/kv) contains the key-value integration example.
- [jepsen](jepsen) contains the workload harness for separately approved external validation.
- The key-value example includes local lifecycle, capacity, and incident procedures under `tests`; these reports remain bounded and non-claim evidence.

## Support boundary

- The production library surface is `gosuda.org/moreconsensus/epaxos`; the key-value service is an integration example and validation harness, not a multi-tenant product.
- Voter sets support one through seven replicas. The core uses deterministic logical time; explicit TOQ inputs and operational clock discipline remain embedder responsibilities.
- Formal evidence is finite model checking plus focused executable tests. It is not an exhaustive or unbounded proof, and it does not establish real-network production readiness.
- Retention and checkpoint features provide bounded operational behavior; certified protocol-state compaction, target capacity, target data lifecycle, and signed incident evidence remain governed by [RELEASE_SCOPE.md](RELEASE_SCOPE.md).

## Module layout

The main module is `gosuda.org/moreconsensus`. Example dependencies are isolated in example modules and included by the committed Go workspace.
