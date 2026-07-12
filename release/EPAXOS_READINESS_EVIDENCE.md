# EPaxos Readiness Evidence Snapshot

Status: no-go evidence bundle for the active EPaxos production-readiness goal. This document records repository verification and its explicit boundaries; it does not claim mission-critical production readiness.

## Decision source

- Release authority: `RELEASE_SCOPE.md`.
- Current decision: `No-go.`.
- Authoritative workflow: `bash tests/go_no_go_workflow.sh`.
- The canonical open rows are Broader formal model coverage, Data lifecycle, Capacity envelope, and Incident readiness.

## Evidence identity

- This snapshot describes the checked repository tree and the commands listed below. A production release must bind evidence to an exact source revision, release binary digest, target identity, immutable evidence root, and independent reviewer signature.
- The coverage gate measures production packages separately: `epaxos` reached 89.5% against an 85.0% minimum and `examples/kv` reached 91.7% against a 90.0% minimum. Verification collectors are exercised by root behavior and race suites.
- No external target-environment closure bundle is present. Repository evidence therefore remains bounded, local, and non-release-approving.

## Gate status

| Gate | Result | Evidence boundary |
| --- | --- | --- |
| Root Go tests | Pass: `go test ./... -count=1` | Deterministic repository tests. |
| Lifecycle race tests | Pass: `go test -race ./tests/lifecyclecollector -count=1` | Collector behavior under the race detector. |
| Refinement-trace race tests | Pass: `go test -race ./tests/refinementtrace -count=1` | Executable bounded RawNode contracts under the race detector. |
| Required fast TLA suite | Pass: `bash tests/tla_model_check_fast.sh` with 14 finite jobs | Bounded TLC state spaces; not an unbounded proof. |
| Release scope structure | Pass when `bash tests/release_scope_audit.sh` is run | Canonical decision, four rows, links, model paths, and a tracked-text Hangul guard. |
| Repository text audit | Pass when `bash tests/audit_repo.sh` is run | Static forbidden-text and deterministic-core checks. |
| Operations artifact audit | Pass in the recorded local verification | Example/operator reports and local lifecycle evidence; not target-environment closure evidence. |
| Aggregate Go coverage | Pass: `epaxos` 89.5% >= 85.0%; `examples/kv` 91.7% >= 90.0% | Verification collectors remain covered by root behavior and race suites; optional platform/process branches are outside production-package coverage. |

## Bounded protocol and service evidence

- `RawNode` exposes deterministic `Tick`, `Step`, `Ready`, `ReadyInto`, `Advance`, proposals, configuration changes, storage, and transport message paths.
- `Ready.Records` are persisted before messages are sent or committed commands are acknowledged. `Advance` acknowledges only an exact outstanding prefix; retries retain the same frozen batch.
- The core supports voter counts 1 through 7, deterministic logical timing, caller-owned storage and transport, checksum-validated records and messages, malformed-input rejection without protocol panics, and explicit safe-copy or zero-copy ownership paths.
- The KV example exercises Pebble persistence, BLAKE3 record checksums, semantic checkpoint verification, whole-directory restore, offline repair, live-source restore, TLS-separated client/peer/admin planes, request and scan limits, binary-safe values, and lifecycle-owned logical ticks.
- Local deterministic simulation covers quorum-preserving crash, restart, omission, storage, partition, reordering, pause, skew, rollback, owner-independent recovery, and duplicate-free application for cluster sizes 1 through 7.
- Local Jepsen profiles cover restart, transport partition, storage-unavailable, destructive-storage, scan semantics, and checker behavior. These histories are loopback-only.

## Formal correspondence evidence

The TLA+ and executable evidence are deliberately layered:

- `tla/EPaxos.tla`, `tla/EPaxosResponses.tla`, `tla/EPaxosRecovery.tla`, and `tla/Quorum.tla` cover normal protocol, response evidence, finite recovery, and quorum arithmetic.
- `tla/EPaxosOptimizedRecovery.tla`, `tla/EPaxosOptimizedRecoveryDecisionTree.tla`, `tla/EPaxosEvidenceQuery.tla`, `tla/EPaxosEvidenceStaleness.tla`, `tla/EPaxosTryPreAcceptRetry.tla`, and the configured TryPreAccept and configuration-transition modules cover finite branch and message slices.
- `tla/EPaxosRevisited.tla`, `tla/TOQClockDiscipline.tla`, `tla/ReadyAdvance.tla`, `tla/EPaxosRawNodeRefinement.tla`, and `tla/EPaxosInductiveProofs.tla` cover finite TOQ/Ready/refinement workflows and a restricted abstract-history safety module.
- `tests/refinementtrace` executes real `RawNode` normal, recovery, TOQ, and configuration scenarios. Its semantic validator checks pre/post well-formedness, fail/drop stuttering, persistence of final Ready writes, exact-prefix Advance behavior, execution monotonicity, and observation stuttering. Its exported-method inventory prevents unreviewed `RawNode` transitions from silently entering the trace.
- `MODEL_EQ_REPORT.MD` is the direct mapping authority. It names implementation anchors, model actions, executable test anchors, and every correspondence boundary.

## Operational artifact evidence

The repository contains example/operator artifacts and audits:

- `tests/kvnode_capacity_envelope.sh`, `tests/kvnode_local_capacity_drill.sh`, `tests/kvnode_local_runner.go`, and the local lifecycle helpers produce bounded sample reports with explicit non-claim fields.
- `tests/kvnode_incident_tabletop_drill.sh` and its local report checks provide storage and transport fault branches, evidence capture, and escalation guidance.
- `examples/kv/cmd/kvcheckpoint` and its tests exercise checkpoint and rollback procedures.
- These artifacts do not prove a signed target capacity envelope, backup/restore/rollback drill, real-network fault history across multi-host independent failure domains, or signed operator evidence.

## Current open blockers preserving no-go

| Item | Required closure evidence |
| --- | --- |
| Broader formal model coverage | Unbounded Go/TLA refinement with a checked action correspondence, plus certified protocol-state compaction and late-message/incarnation fencing requirements. |
| Data lifecycle | Certified compaction operational drill and target backup, restore, rollback, and disaster-recovery evidence. |
| Capacity envelope | Signed target capacity envelope covering workload, latency, resources, retention, and operating limits. |
| Incident readiness | Real-network fault evidence across multi-host independent failure domains and signed operator-controlled incident, escalation, rollback, and recovery evidence. |

## Reproduction commands

The following commands are the repository evidence gates:

```text
go test ./... -count=1
go test -race ./tests/lifecyclecollector -count=1
go test -race ./tests/refinementtrace -count=1
bash tests/tla_model_check_fast.sh
bash tests/audit_repo.sh
bash tests/release_scope_audit.sh
bash tests/operations_readiness_audit.sh
bash tests/go_no_go_workflow.sh
```

`bash tests/go_coverage.sh` passes the scoped production-package thresholds and race stress suites. The fast TLA gate is finite; the larger `bash tests/tla_model_check.sh` profile is an additional manual check and does not close the formal blocker.

## Release boundary

- This evidence supports a bounded library and example-service claim on Darwin arm64, with deterministic simulation and three-node same-host loopback exercises. It does not support unbounded formal safety, certified protocol-state compaction, multi-host independent failure domains, real-network fault tolerance, operational TOQ clock synchronization, target capacity, target data lifecycle, or signed incident readiness. The release decision remains no-go until every open row has current target-bound evidence.
