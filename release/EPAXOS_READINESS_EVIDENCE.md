# EPaxos Readiness Evidence Snapshot

Status: no-go evidence bundle for the active EPaxos production-readiness goal. This document records repository verification and its explicit boundaries; it does not claim mission-critical production readiness.

## Decision source

- Release authority: `RELEASE_SCOPE.md`.
- Current decision: `No-go.`.
- Authoritative workflow: `bash tests/go_no_go_workflow.sh`.
- The canonical open row is Broader formal model coverage.

## Evidence identity

- This snapshot describes the checked repository tree and the commands listed below. A production release must bind evidence to an exact source revision, release binary digest, target identity, immutable evidence root, and independent reviewer signature.
- The coverage gate measures production packages separately: `epaxos` reached 87.3% against an 85.0% minimum and `examples/kv` reached 90.5% against a 90.0% minimum. Verification collectors are exercised by root behavior and race suites.
- No external target-environment closure bundle is present. Repository evidence therefore remains bounded, local, and non-release-approving.

## Gate status

| Gate | Result | Evidence boundary |
| --- | --- | --- |
| Root Go tests | Pass: `go test ./... -count=1` | Deterministic repository tests. |
| Lifecycle race tests | Pass: `go test -race ./tests/lifecyclecollector -count=1` | Collector behavior under the race detector. |
| Refinement-trace race tests | Pass: `go test -race ./tests/refinementtrace -count=1` | Executable bounded RawNode contracts under the race detector. |
| Required fast TLA suite | Pass: `bash tests/tla_model_check_fast.sh` with 31 finite jobs, including `EPaxosCertifiedCompactionFast.cfg` | Includes the ordered compaction/fencing model and six exact-marker negative mutants; bounded TLC state spaces are not an unbounded proof. |
| Release scope structure | Pass when `bash tests/release_scope_audit.sh` is run | Canonical decision, one open row, links, model paths, and tracked-text guards. |
| Repository text audit | Pass when `bash tests/audit_repo.sh` is run | Static forbidden-text and deterministic-core checks. |
| Operations artifact audit | Pass in the recorded local verification | Example/operator reports and local lifecycle evidence; not target-environment closure evidence. |
| Aggregate Go coverage | Pass: `epaxos` 87.3% >= 85.0%; `examples/kv` 90.5% >= 90.0% | Verification collectors remain covered by root behavior and race suites; optional platform/process branches are outside production-package coverage. |

## Bounded protocol and service evidence

- `RawNode` owns deterministic protocol state only. Opaque commands expose canonical point/span/all footprints and replicated cycle keys; protocol controls are separate entry kinds.
- `Ready` enforces ordered persistence, transport, snapshot install, application `Apply`, checkpoint, compaction, and exact-prefix `Advance` phases. Application effects and full `CommandID` results commit atomically in the KV example.
- The core supports voters 1 through 7, caller-owned storage/transport/time sampling, `MEP3` incarnation-authenticated messages, Pebble v9 migration, and safe-copy/zero-copy ownership.
- Certified checkpoint descriptors bind exact frontiers, voter incarnations, barrier/protocol state, allocator fences, and application digests. Durable quorum certification precedes atomic tombstone/deletion.
- The three-replica KV smoke covers empty-span ordered read, phantom conflict, certification, physical row deletion, snapshot-plus-delta restart, durable result replay, late compacted reference offer, and wrong-incarnation rejection.
- Local Jepsen profiles cover restart, transport partition, storage-unavailable, destructive-storage, scan semantics, and checker behavior. These histories are loopback-only.

## Formal correspondence evidence

The TLA+ and executable evidence are deliberately layered:

- `tla/EPaxosInductiveProofs.tla` is checked by TLAPS 1.6.0-pre at version `763bf3c`: 876 obligations, 867 proved, 9 failed, and 0 omitted. All anchor families prove. Four newly isolated helper obligations and five unchanged original preservation lemmas remain unchecked, so `tests/tlaps_check.sh` is fail-closed and exits 10.
- The 31-job TLC fast gate includes `tla/EPaxosCompactionFencing.tla` with `tla/EPaxosCompactionFencing.cfg`: an ordered 11-state positive model containing both lanes, both incarnations, and one fenced-configuration transition. It certifies six named requirements in that finite scope; six negative mutant configurations each produce their exact invariant violation, and `TestFencingLayersRejectFoldedLoadAndStaleBootstrapAuth` is the layered Go witness.
- `tla/EPaxosRevisited.tla`, `tla/TOQClockDiscipline.tla`, `tla/ReadyAdvance.tla`, `tla/EPaxosCertifiedCompaction.tla`, `tla/EPaxosRawNodeRefinement.tla`, and `tla/EPaxosInductiveProofs.tla` cover finite TOQ/Ready/checkpoint/refinement workflows and a restricted abstract-history safety module.
- `tests/refinementtrace` captures four real `RawNode` normal, recovery, TOQ, and configuration scenarios. `tests/trace_refinement_check.sh`, wired into `tests/ci.sh`, projects semantic snapshots onto 12 `tla/EPaxosRawNodeRefinement.tla` variables. Every consecutive pair is dispatched by its audited raw `(action, kind)` entry in a 96-pair permission table to the model action predicates or exact 12-variable equality; atomic commit-plus-execute uses a TLC-evaluated `PaperChoose`/`PaperExecute` relational composition without changing `PaperNext` or `RefinementProperty`, and all five negative controls reject.
- The trace replay proves sampled admission. The exhaustive AST inventory separately proves coverage of internal mutation/dispatch sites through exclusive `TraceActions`/`Stutter`/`Gap` classification. Their combined residuals are unexercised `PaperObserveRecovery`, audited-but-unexercised `NormalValidationDrop/message-step`, 13 bookkeeping variables frozen at `Init`, coordinator-and-designated-instance scope, and `wire` abstracted to a constant.
- `tests/formal_closure_collect.sh` stages unsigned-local `rawArtifact` records fail-closed. Verifier enums cover the compaction/fencing area, and `tests/formal_closure_evidence_selftest.py` covers 82 synthetic cases. These local artifacts do not supply producer/reviewer signatures or native-Darwin target attestation.
- `MODEL_EQ_REPORT.MD` is the direct mapping authority. It names implementation anchors, model actions, executable test anchors, and every correspondence boundary.

## Operational artifact evidence

The repository contains example/operator artifacts and audits:

- `tests/kvnode_local_runner.go`, the local lifecycle helpers, and `epaxos/dst_test.go` produce bounded checkpoint, restore, corruption, fault, and deterministic-work evidence.
- `examples/kv/cmd/kvcheckpoint` and its tests exercise checkpoint and rollback procedures.
- These artifacts and the certified-compaction implementation provide bounded local evidence; they do not prove arbitrary histories or unbounded Go/TLA refinement.

## Current open blockers preserving no-go

| Item | Required closure evidence |
| --- | --- |
| Broader formal model coverage | Local gaps: discharge the four named helper obligations and five unchanged original preservation lemmas still failing under TLAPS, and close the replay residual classes (`PaperObserveRecovery`, `NormalValidationDrop/message-step`, 13 frozen bookkeeping variables, coordinator-and-designated-instance scope, and constant-abstracted `wire`). External gaps: independent producer and reviewer signatures over the canonical bundle plus native-Darwin attestation for target `mc-kv-darwin24-arm64-launchd-3n-r1`. The finite compaction/fencing certification and sampled action replay are landed evidence, not row closure. |

### Closing broader-formal

Real closure requires a signed evidence bundle in addition to the local formal checks:

- Run `bash tests/formal_closure_collect.sh` to collect the producer-side local evidence. The collector does not supply the external approval inputs.
- Pin two externally controlled trust roots: the producer's public key at `tests/formal_closure_producer_public.pem` and the reviewer's public key at `tests/formal_closure_reviewer_public.pem`. The private keys stay under distinct external control; the producer and the reviewer are different parties, and neither key may be generated by the machine that assembles the bundle. The verifier checks that the PEM trust roots differ at the public-key level and that the reviewer identity is independent of the producer identity.
- Obtain a native Darwin execution attestation with `execution_mode` set to `native-darwin`, bound to target `mc-kv-darwin24-arm64-launchd-3n-r1`.
- The producer signs the canonical evidence payload with RSA-SHA256, and the independent reviewer signs it separately; this machine cannot self-issue either signature. The verifier checks both signatures against their pinned public keys and the bound canonical payload hash.

Invoke the release workflow against that signed bundle:

```text
GO_NO_GO_EVIDENCE_ROOT=/absolute/path/to/signed-bundle \
  bash tests/go_no_go_workflow.sh
```

## Reproduction commands

The following commands are the repository evidence gates:

```text
go test ./... -count=1
go test -race ./tests/lifecyclecollector -count=1
go test -race ./tests/refinementtrace -count=1
go test ./epaxos -run '^TestDST(DataLifecycle|TransientFault|DegradedPerformance)' -count=1
bash tests/tla_model_check_fast.sh
python3 tests/tla_model_check_runner.py --profile fast --jobs 2 --per-config-timeout 180 --overall-timeout 600
bash tests/audit_repo.sh
bash tests/release_scope_audit.sh
bash tests/operations_readiness_audit.sh
bash tests/go_no_go_workflow.sh
```

`bash tests/go_coverage.sh` passes the scoped production-package thresholds and race stress suites. The fast TLA gate is finite; the larger `bash tests/tla_model_check.sh` profile is an additional manual check and does not close the formal blocker.

## Release boundary

- This evidence supports a bounded library and example-service claim on Darwin arm64, including certified protocol compaction, checkpoint recovery, three-node same-host loopback exercises, sampled Go-to-TLC replay, and six finite compaction/fencing invariants. It does not support arbitrary-Go refinement, arbitrary compaction histories, arbitrary crash/network histories, operational TOQ clock synchronization, or target deployment guarantees. The release decision remains no-go for both the named local formal gaps and the missing independent signatures/native-Darwin target attestation.
