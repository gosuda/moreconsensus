# Release Scope Lock

This file is the self-contained authority for the active EPaxos production-readiness goal.

## Scope rule

A release claim is allowed only when the item is listed in **Closed release items** with current evidence. Any required item in **Open release items** forces a no-go decision. This file records the boundaries and evidence classes; it does not convert local evidence into production approval.

## Current release decision

No-go.

The repository provides bounded protocol, storage, service, deterministic-simulation, and degraded-performance evidence on Darwin arm64. It is not mission-critical production-ready. The broader-formal row remains open for local reasons—nine unchecked TLAPS obligations and the bounded trace-replay residual classes—and external reasons—independent producer/reviewer signatures and native-Darwin target attestation.

## Evidence identity

- `release/EPAXOS_READINESS_EVIDENCE.md` is the current repository evidence snapshot.
- A production release must bind every closed item to a source revision, release binary digest, target identity, immutable evidence root, and independent reviewer signature. No such target-bound closure bundle is present for this decision.
- Local and hosted test results are verification observations, not release signatures. The coverage gate measures production packages separately: `epaxos` must reach at least 85.0% and `examples/kv` at least 90.0%; verification collectors remain covered by their root Go and race suites.

## Verification prerequisites

| Gate | Status |
| --- | --- |
| Aggregate Go coverage | pass |

## Fault-tolerance target and evidence matrix

This matrix defines the supported simulation/local-loopback envelope. A claim is limited to its row, its stated fault count, and a surviving healthy quorum. Normal operation means safety, no duplicate committed application, and the row-specific progress or fail-closed behavior. The matrix does not claim real-network operation.

| Fault class | Boundary | Evidence and behavior | Explicit limit |
| --- | --- | --- | --- |
| Crash, restart, owner stop, and storage omission | Voter counts 1 through 7; tolerated unavailable counts are `0,0,1,1,2,2,3` for `N=1..7`; progress requires a slow quorum; no Byzantine behavior. | Deterministic simulation tests and `bash tests/chaos_fault_campaign.sh` cover progress, recovery, and duplicate-free application when a healthy quorum remains. | No quorum-loss, Byzantine, or target-host claim. |
| Message omission, duplication, reordering, and partition | Deterministic transport faults and local Jepsen loopback profiles; a healthy quorum side must remain. | `epaxos/dst_test.go`, `epaxos/stress_test.go`, and the local campaign cover heal-and-converge and minority fail-closed behavior. | No real-network history. |
| Logical timing faults | Deterministic `RawNode.Tick` skew and pause; explicit TOQ uses caller-provided clock and delay bounds. | `TestUnevenLogicalTickSkewAndBurstConvergesWithoutDuplicates`, `TestPausedClockDoesNotTickOrProcessReadyUntilResume`, and `tla/TOQClockDiscipline.cfg`. | The core does not synchronize clocks or measure one-way delay. |
| Rollback and restart catch-up | One isolated replica may roll back to an earlier durable state while quorum data remains trusted. | `TestRolledBackNodeCatchesUpFromQuorumWithoutDuplicateApply`, `TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit`, and `tla/EPaxosRollbackAllocation.cfg`. | No arbitrary rollback history or quorum-loss recovery. |
| Checksum-detected corruption | One stopped or corrupt KV member; a healthy quorum and a verified checkpoint are required. | KV checkpoint verification and whole-directory replacement tests fail closed on corrupt records and preserve duplicate-free application. | No in-place WAL repair, checksum rewriting, synthesized recovery, or target drill. |
| Configuration ordering and pinned quorums | Finite barrier, add, remove, add-then-remove, replay, retry, and de-duplication scenarios. | Go configuration tests and the finite models listed in `MODEL_EQ_REPORT.MD` cover old `Ref.Conf` voter domains and current-configuration barriers. | No arbitrary membership history, joint consensus, or unbounded proof. |
| Fast path and optimized recovery | Odd supported sizes use the paper fast quorum; even sizes retain conservative thresholds; FP-deps-committed evidence and sender-preserving recovery evidence are required. | `epaxos/quorum.go`, focused Go tests, and finite quorum/evidence/recovery models. | No arbitrary network, message-loss, durable-history, or unbounded optimized-recovery claim. |
| Service-plane containment | Separate client, peer, and admin listeners; TLS 1.3 mutual authentication; bounded request bodies and scans; deterministic lifecycle-owned ticks. | Tagged KV behavior/race tests, service fault campaign, and operations audits. | No multi-tenant RBAC claim. |
| Transient negligible faults and degraded performance | One replica pauses for one logical round while the quorum continues to execute commands; deterministic work increases without wall-clock timing. | `TestDSTTransientFaultCasesRemainAvailableAndRecover` and `TestDSTDegradedPerformanceTransientNegligibleFaultStaysAlive` prove continued service, exactly-once application, linearizable replay, and bounded logical progress. | No production performance claim. |

## Closed release items

The following items are closed only for the bounded evidence classes stated here.

| Item | Current evidence |
| --- | --- |
| Release scope lock | `RELEASE_SCOPE.md`, `tests/release_scope_audit.sh`, and the structural release audit. |
| Public API and durability contract | `EPAXOS.MD`, `README.md`, `epaxos/node.go`, and focused `Ready`/`Advance` tests. |
| EPaxos normal, slow, commit, and recovery paths | `EPAXOS.MD`, `EPAXOS_IMPLEMENTATION_PROOF.md`, `epaxos/node.go`, and `go test ./... -count=1`. |
| Supported quorum table | `epaxos/quorum.go`, `tla/Quorum.tla`, and cluster-size tests for 1 through 7. |
| Fast-path and Accept-Deps behavior | `epaxos/node.go`, `epaxos/message.go`, `examples/kv/epaxos_storage.go`, focused protocol tests, and finite optimized-recovery models. |
| Revisited chain pruning and explicit TOQ core | `epaxos/node.go`, `tla/EPaxosRevisited.tla`, `tla/TOQClockDiscipline.tla`, and focused TOQ tests. |
| Configuration barriers and historical voter pinning | `epaxos/config_change_ordering_test.go`, `epaxos/recovery_test.go`, `tla/EPaxosConfigBarrier.tla`, `tla/EPaxosConfigTransition.tla`, `tla/EPaxosConfigRemoveTransition.tla`, and `tla/EPaxosConfigChainTransition.tla`. |
| Deterministic storage and transport virtualization | `epaxos/storage.go`, `epaxos/sim_test.go`, `epaxos/dst_test.go`, and fault campaigns. |
| Checksums and malformed wire handling | `epaxos/checksum.go`, `epaxos/codec.go`, `epaxos/message.go`, checksum tests, and decoder fuzz seeds. |
| KV checkpoint verification and repair boundaries | `examples/kv/backup.go`, `examples/kv/cluster.go`, `examples/kv/epaxos_storage.go`, and KV checkpoint tests. |
| Service API, TLS, request, scan, and binary-value boundaries | `examples/kv/cmd/kvnode/main.go`, tagged KV tests, and `EPAXOS.MD`. |
| Local fault and Jepsen harnesses | `tests/chaos_fault_campaign.sh`, `tests/jepsen_local.sh`, and the Jepsen checker tests. |
| Finite formal model gate | `tests/tla_model_check_fast.sh` runs 31 finite jobs, including bootstrap base sizes 1–6 (successor sizes 2–7), crash-prefix, race, fairness, and the positive plus six negative compaction/fencing configurations; `tests/tla_model_check.sh` is the larger manual suite. Correspondence limits remain explicit in `MODEL_EQ_REPORT.MD`. |
| Operations artifact checks | `tests/operations_readiness_audit.sh` and the local lifecycle helpers validate bounded operator mechanics only. |
| DST data lifecycle and transient fault behavior | `TestDSTDataLifecycleCheckpointRestoreAndCorruptionRejection`, `TestDSTTransientFaultCasesRemainAvailableAndRecover`, and `TestDSTDegradedPerformanceTransientNegligibleFaultStaysAlive` cover durable checkpoint restore, corruption rejection, transient faults, exactly-once application, linearizable replay, and deterministic work degradation. |

## Open release items


| Item | Current state |
| --- | --- |
| Broader formal model coverage | TLAPS 1.6.0-pre at version `763bf3c` machine-checks `tla/EPaxosInductiveProofs.tla`: 876 obligations, 867 proved, 9 failed, and 0 omitted. The proved anchor families are `InitEstablishesInvariant`, `NormalProposePreserves`, `BeginRecoveryPreserves`, `CollectEvidencePreserves`, `RecoveryProposePreserves`, `QuorumIntersection`, and `RecoverySelectionPreservesChosenValue`. The four unchecked helper obligations are `CollectEvidenceArchivePreservationObligation`, `CollectAcceptedHistoryPreservationObligation`, `AcceptHistoryPreservationObligation`, and `AcceptEvidenceArchivePreservationObligation`; the five unchanged original preservation lemmas with undischarged proofs are `CertifyChosenPreserves`, `RecordConfigurationPreserves`, `RetryStepsPreserve`, `StutterPreserves`, and `WaitingPersistsOrCompletes`. `tests/tlaps_check.sh` is fail-closed and currently exits 10. The checked Go-trace-to-TLC replay in `tests/trace_refinement_check.sh`, wired into `tests/ci.sh`, projects four captured scenarios through a snapshot-only abstraction onto 12 `EPaxosRawNodeRefinement` variables; every consecutive pair is dispatched by its audited raw `(action, kind)` entry in the 96-pair permission table to the model action predicates or exact 12-variable stutter, the atomic commit-plus-execute pair uses a TLC-evaluated `PaperChoose`/`PaperExecute` relational composition without changing `PaperNext` or `RefinementProperty`, and all five negative controls reject. The replay proves sampled admission, while the exhaustive AST dispatch inventory proves coverage of internal mutation/dispatch sites through exclusive `TraceActions`/`Stutter`/`Gap` classification. Replay residuals are unexercised `PaperObserveRecovery`, audited-but-unexercised `NormalValidationDrop/message-step`, 13 bookkeeping variables frozen at `Init`, coordinator-and-designated-instance scope, and `wire` abstracted to a constant. `tla/EPaxosCompactionFencing.tla` and `tla/EPaxosCompactionFencing.cfg` certify six compaction/fencing requirements over an ordered 11-state positive model with both lanes, both incarnations, and one fenced-configuration transition; six named negative-mutant configurations each trigger their designated invariant violation, and the layered Go witness is `TestFencingLayersRejectFoldedLoadAndStaleBootstrapAuth`. `tests/formal_closure_collect.sh` stages unsigned-local `rawArtifact` records fail-closed, verifier enums cover compaction/fencing, and the synthetic self-test covers 82 cases. The row remains open because the nine TLAPS obligations and trace residual classes are local gaps and because no independent producer/reviewer signatures or native-Darwin target attestation bind a release bundle. |

Exactly one canonical item remains open. Closure requires discharge of the named local formal gaps and a valid externally signed, native-Darwin-attested evidence bundle; bounded simulation or rerunning a local sample is insufficient.

## Review baseline

Reviewers should start with the following authority and gates:

- `EPAXOS.MD` for implemented algorithm behavior and embedder obligations.
- `EPAXOS_IMPLEMENTATION_PROOF.md` for property rationale, failure-count boundaries, and non-claims.
- `MODEL_EQ_REPORT.MD` for direct model-to-implementation correspondence and formal limits.
- `release/EPAXOS_READINESS_EVIDENCE.md` for the current verification snapshot.
- `tests/ci.sh` and `.github/workflows/ci.yml` for the wired gate. Root Go tests, the scoped production coverage thresholds, and race suites are the executable prerequisites; verification collectors are exercised by behavior and race tests but are not counted as production-package coverage.
- `bash tests/go_no_go_workflow.sh` for the authoritative decision and canonical open-row IDs.

## Non-claims

- Finite TLC is bounded evidence, not an unbounded theorem over arbitrary Go executions.
- `tla/EPaxosInductiveProofs.tla` has 867 proved, 9 failed, and 0 omitted obligations for its restricted semantic `ConcreteNext`; it does not cover codec, allocation, `Ready` ownership, SCC execution, TOQ timing, or arbitrary `RawNode` executions.
- `tla/EPaxosRawNodeRefinement.tla` is an implementation-shaped bounded workflow model. The checked four-scenario action replay covers the 12-variable snapshot abstraction described above, not arbitrary Go execution refinement; `PaperObserveRecovery`, `NormalValidationDrop/message-step`, 13 frozen bookkeeping variables, coordinator-and-designated-instance scope, and constant-abstracted `wire` remain outside that checked correspondence.
- `tla/EPaxosVoterBootstrap.tla` is a finite bootstrap contract executed by the 31-job TLC fast gate for base sizes 1–6 (successor sizes 2–7); bootstrap state is still outside the sampled Go-to-TLC replay.
- The core does not synchronize clocks, measure one-way delay, prove operational TOQ discipline, or provide a production sync-group service.
- Deterministic simulation and local Jepsen loopback are bounded fault evidence; they do not establish target deployment behavior.
- The finite compaction/fencing model provides certified protocol-state compaction evidence for six named requirements in its ordered 11-state scope, with six rejecting mutants and mapped Go witnesses; it does not prove arbitrary compaction histories or unbounded uptime.
- KV checkpoint recovery requires semantic verification and whole-directory replacement; the finite compaction/fencing certification does not establish production storage-engine compaction for arbitrary histories.
- Local mixed-version rollback validates the harness for its exercised binary pair; it does not prove broad compatibility across substantive protocol or storage changes.
- Example/operator reports are bounded observations and are not release closure evidence.
