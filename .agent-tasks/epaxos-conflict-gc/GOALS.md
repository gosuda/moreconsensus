# EPaxos conflict engine and executed-instance GC

## Goal

Replace the $O(\text{all instances})$ conflict and attrs machinery with a per-lane conflict engine, add two-tier executed-instance retirement with an asynchronous record-load handshake, expose `VisitConflicts`, and enforce strict linting.

## Success criteria

- R1: Attribute computation and TryPreAccept conflict checks avoid resident-instance scans.
- R2: The conflict index remains coherent for every record mutation and removal.
- R3: Embeddings receive a minimal, zero-allocation public conflict query API.
- R4: Executed instances retire automatically with configurable per-lane retention.
- R5: Payload-drop and fold reclamation bound resident memory, with proposal backpressure.
- R6: Wire format and durable storage remain unchanged; durable compaction remains separate.
- R7: Folded-record recovery uses a deterministic asynchronous Ready handshake.
- R8: Property tests, invariants, TLA evidence, and resident-state benchmarks prove behavior.
- R9: Strict golangci-lint, CI enforcement, and task-goal scaffolding are in place.
- R10: The work is delivered as atomic GitHub issues and PRs targeting `main`.

## Verifiable-goals scaffolding

- [Conflict engine tests](../../epaxos/conflict_engine_test.go)
- [Retirement tests](../../epaxos/retire_test.go)

Remove this task-goal file after the final implementation unit merges.
