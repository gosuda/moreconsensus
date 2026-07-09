# EPaxos readiness evidence bundle

Status: no-go evidence bundle for the active EPaxos production-readiness goal. This file records current simulation/local-loopback repository evidence and remaining blockers; it does not make a production-ready claim.

## Decision source

- Release lock: `RELEASE_SCOPE.md`
- Current decision in release lock: `No-go.`
- Rule: any item remaining under `## Open release items` keeps the release decision no-go.

## Current implemented and exercised evidence

### Core protocol and service gates

- `bash tests/chaos_fault_campaign.sh` passed after adding `TestStorageWireRestartUpgradeRollbackSimulationConvergesWithoutDuplicateApply` and the local destructive-storage Jepsen profile to the campaign; full raw output is archived at `artifact://240`.
  - Covered focused EPaxos core fault tests, including storage/wire restart rollback simulation.
  - Covered KV persistence tests including checksum-detected corruption, checkpoint restore, and explicit checkpoint-backed repair.
  - Covered Jepsen harness unit tests.
  - Covered local Jepsen restart, transport, storage, and destructive-storage profiles.
- `go test ./examples/kv -run 'Test(OpenClusterRejectsBitFlippedPersistedEPaxosRecord|CheckpointRestoreRecoversBitFlippedPersistedEPaxosRecord)$' -count=1` passed in this session.
- `go test ./examples/kv -count=1` passed in this session.
- `cd jepsen && lein test moreconsensus.epaxos-test-test` passed in this session.
- `bash tests/tla_model_check.sh` passed after adding explicit TOQ envelope, delayed-assignment, and pending-decision checks to `tla/EPaxosRevisited.tla` plus the finite TOQ bounded-skew/bounded-delay contract in `tla/TOQClockDiscipline.tla`; `EPaxosRevisited.cfg` generated 2936 states and 1584 distinct states, and `TOQClockDiscipline.cfg` generated 3888 states and 1944 distinct states, with no TLC error.
- `go test ./epaxos -count=1` passed after adding explicit `Config.TOQ`, durable `TOQPending`/`ProcessAt`, flagged zero-attribute TOQ PreAccepts, delayed local assignment, restart/pending-conf coverage, and zero-`ProcessAt` coverage.
- `cd examples/kv && go test ./...` passed after EPaxos storage codec v3 added `ProcessAt`/`TOQPending`, canonical checksum migration for pre-TOQ/pre-fast-path records, and legacy compatibility tests.
- `cd examples/kv && go test -tags kvnode ./...` passed with the updated KV module.
- `bash tests/go_coverage.sh` passed after TOQ/core/storage coverage additions; both `epaxos` and `examples/kv` package coverage profiles reported 100.0%.
- `JAVA_BIN=/opt/homebrew/opt/openjdk/bin/java bash tests/tla_model_check.sh` passed after updating quorum formulas, normal-case dependency-commit evidence, and ballot-aware `tla/EPaxosResponses.tla` prepare branch-priority/try-witness/current-accept-round evidence; `EPaxosResponses.cfg` generated 17725 states and 1971 distinct states, and `EPaxosResponsesFive.cfg` generated 25341619 states and 1076385 distinct states, with no TLC error.
- `bash tests/tla_model_check.sh` passed after adding `tla/EPaxosConfigBarrier.tla` local configuration-barrier coverage for two fixed config refs plus one user command; `EPaxosConfigBarrier.cfg` generated 499 states and 193 distinct states with no TLC error.
- `bash tests/go_coverage.sh` passed after adding coverage-branch tests for restart recovery timers, accept-reject stale hints, stale-originator fast-commit prerequisites, local/remote `FastPathEligible` evidence, TOQ branch coverage, and EPaxos storage codec migration coverage.
- `bash tests/ci.sh` passed after the explicit TOQ, storage-migration, TOQ clock-discipline, prepare branch-priority, and config-barrier model updates; the go/no-go workflow preserved `release_decision=No-go.` with 11 open release items.
- `go test ./epaxos -run 'Test.*Accept.*Deps|Test.*TryPreAccept.*Accept' -count=1` passed after adding recovery-only Accept-Deps evidence and TryPreAccept conflict-use regressions.
- `go test ./epaxos ./examples/kv -count=1` passed after adding `AcceptSeq`/`AcceptDeps`, message/wire checksum support, and example-KV storage-codec persistence (added in v4 and migrated through current v6).
- `bash tests/tla_model_check.sh` passed after adding `tla/EPaxosOptimizedRecovery.tla` and wiring the 3-, 5-, and 7-replica optimized-recovery configs; `EPaxosOptimizedRecovery.cfg` generated 199 states and 106 distinct states, `EPaxosOptimizedRecoveryFive.cfg` generated 4339 states and 1634 distinct states, and `EPaxosOptimizedRecoverySeven.cfg` generated 145912 states and 41948 distinct states, with no TLC error.
- `bash tests/go_coverage.sh` passed after adding targeted EPaxos and example-KV Accept-Deps coverage tests; `epaxos` and `examples/kv` coverage profiles both reported 100.0%, including AcceptSeq/AcceptDeps validation, wire encode/decode bounds, legacy checksum migration, and storage-codec persistence across v4/v5/v6 round trips.
- `bash tests/ci.sh` passed after the Accept-Deps implementation, the 3-, 5-, and 7-replica `tla/EPaxosOptimizedRecovery.tla` configs, and the new EPaxos/KV Accept-Deps coverage tests; the go/no-go workflow still reported `release_decision=No-go.` with 11 open release items.
- `go test ./examples/kv -run 'Test.*Checkpoint.*Repair|Test.*VerifyCheckpoint' -count=1` passed after adding explicit offline checkpoint-backed repair, checkpoint read-only EPaxos record verification, corrupt-checkpoint rejection, and missing-checkpoint rejection coverage.
- `go test ./examples/kv -count=1` passed after adding the checkpoint-backed repair tests.
- `bash tests/go_coverage.sh` passed after the checkpoint-backed repair coverage; `epaxos` and `examples/kv` coverage profiles both reported 100.0%.
- `bash tests/ci.sh` passed after adding explicit checkpoint-backed repair, updating the KV persistence fault matrix, and updating the operations/release-scope audits; the go/no-go workflow still reported `release_decision=No-go.` with 11 open release items.
- `/opt/homebrew/opt/openjdk/bin/java -cp /tmp/tla2tools-v1.7.4.jar tlc2.TLC -config tla/EPaxosConfigTransition.cfg tla/EPaxosConfigTransition.tla` passed for the finite add-voter config-transition pinning model; TLC generated 62 states and 62 distinct states.
- `bash tests/tla_model_check.sh` passed after wiring `tla/EPaxosConfigTransition.cfg`; `EPaxosConfigTransition.cfg` generated 62 states and 62 distinct states with no TLC error.
- `bash tests/ci.sh` passed after adding the finite add-voter config-transition model, wiring it into the TLA gate, and updating release/model evidence; the go/no-go workflow still reported `release_decision=No-go.` with 11 open release items.
- `go test ./epaxos -run 'TestDST|TestDeterministic.*(Linearizability|Liveness|Availability)|TestClusterSizesOneThroughSevenCommit|TestUnevenLogicalTickSkewAndBurstConvergesWithoutDuplicates|TestPausedClockDoesNotTickOrProcessReadyUntilResume|TestRolledBackNodeCatchesUpFromQuorumWithoutDuplicateApply|Test.*Config.*' -count=1` passed after adding the fault-tolerance target/evidence matrix.
- `go test ./examples/kv -run 'Test.*Checkpoint.*Repair|Test.*VerifyCheckpoint|TestOpenClusterRejectsBitFlippedPersistedEPaxosRecord|TestApplyReadyDuplicateCommittedRefIsIdempotent' -count=1` passed after adding the fault-tolerance target/evidence matrix.
- `go test -tags kvnode ./examples/kv/cmd/kvnode -count=1` passed after adding the fault-tolerance target/evidence matrix.
- `bash tests/chaos_fault_campaign.sh` passed after adding the fault-tolerance target/evidence matrix and reran focused core fault, KV persistence fault, Jepsen harness, and local restart/transport/storage profiles.
- `bash tests/release_scope_audit.sh`, `bash tests/audit_repo.sh`, and `bash tests/operations_readiness_audit.sh` passed after adding the fault-tolerance target/evidence matrix and audit checks.
- `bash tests/go_no_go_workflow.sh` passed after the matrix/audit edits and preserved `release_decision=No-go.` with `open_release_items=11`.
- `go test ./epaxos -run 'TestDST.*(FailureBoundary|Linearizability|Storage)' -count=1` passed after adding exact slow-quorum failure-boundary DST coverage for cluster sizes 1 through 7, no-quorum fail-closed checks, partition/heal linearizability, and Ready storage-retry exactly-once behavior.
- `JAVA_BIN=/opt/homebrew/opt/openjdk/bin/java bash tests/tla_model_check.sh` passed after giving every TLC config a unique `-metadir`; full model output is archived at `artifact://186`.
- `go test ./epaxos -count=1`, `go test ./examples/kv -count=1`, and `go test -tags kvnode ./examples/kv/cmd/kvnode -count=1` passed after the stale AcceptResp guard, ballot-aware response model, exact storage-failure DST boundary, TOQ three-node optimized fast-quorum test, and KV API hardening fixes.
- `bash tests/toolchain_audit.sh` passed after pinning GitHub Actions to full 40-character commit SHAs resolved from the GitHub tag refs.
- `bash tests/release_scope_audit.sh` and `bash tests/audit_repo.sh` passed after refreshing release scope, model/proof docs, and audit requirements for the current evidence.
- `bash tests/go_coverage.sh` passed after the branch-test current-ballot fix; it reported 100.0% coverage for `./epaxos` and `./examples/kv`, and the `kvnode` package test passed under the script.
- `MORECONSENSUS_FUZZTIME=1s bash tests/fuzz_stress_campaign.sh` passed after the current AcceptResp, TOQ, storage-boundary, API-hardening, and documentation updates; `FuzzDecodeMessage` ran 9242 executions under the 1s override.
- `EPAXOS_IMPLEMENTATION_PROOF.md` was added as the paper-grounded implementation proof/rationale document and linked from `README.md`; committed release documents (`RELEASE_SCOPE.md` and this evidence bundle) state that the active production-readiness goal remains incomplete/no-go unless current evidence proves every release item.
- `go test ./epaxos -run Test.*Upgrade.*Rollback -count=1` and `go test ./epaxos -count=1` passed after adding deterministic storage/wire restart rollback simulation; raw outputs are archived at `artifact://237` and `artifact://238`.
- `bash tests/operations_readiness_audit.sh`, `bash tests/release_scope_audit.sh`, `bash tests/audit_repo.sh`, and `bash tests/go_no_go_workflow.sh` passed after the sender-preserving evidence-query/resend-ignore plus checkpoint-backed live corruption recovery scope updates; the workflow reported `release_decision=No-go.` with `open_release_items=6` (`artifact://539`).
- Focused durable RecordBallot and optimized-recovery regressions passed: `go test ./epaxos -run 'TestPreparePromiseDoesNotOverwritePersistedRecordBallotAcrossRestart|TestPrepareRecoveryChoosesHighestPersistedRecordBallot|TestPrepareResponseCarriesPromiseAndPreviousRecordBallot|TestPrepareRecoveryUsesOnlyHighestAcceptedRecordBallot' -count=1` (`artifact://308`) and `go test ./examples/kv -run 'TestEncodeDecodeEPaxosRecordPreservesRecordBallot|TestDecodeEPaxosRecordV4BackfillsRecordBallot|TestEncodeDecodeEPaxosRecordPreservesAcceptDepsEvidence|TestDecodeEPaxosRecordMigratesVersion3ChecksumWithoutAcceptEvidence' -count=1` (`artifact://310`).
- `go test ./epaxos -count=1`, `go test ./examples/kv -count=1`, and `bash tests/tla_model_check.sh` passed after the durable RecordBallot and targeted TryPreAccept recovery changes; package outputs are archived at `artifact://323` and `artifact://321`.
- `go test ./epaxos -run 'TestTryPreAcceptRejectsCommittedStaleDependencyWithoutAcceptDepsEvidence|TestTryPreAcceptDuplicateUncommittedConflictRejectionDoesNotRestartBlockerRecovery|TestCommitMessageAcceptDepsEvidenceAllowsLaterTryPreAccept|TestTryPreAcceptCommittedConflictRejectionAddsSlowAcceptDependency|TestTryPreAcceptResponseRecoveryBranches' -count=1` (`artifact://374`) and `go test ./epaxos -count=1` (`artifact://376`) passed after the committed-conflict slow-accept hardening.
- `go test ./epaxos -run 'TestTryPreAcceptCommittedStaleDependencyEvidenceResendsIgnoreMarker|TestTryPreAcceptIgnoreMarkerAllowsCommittedStaleDependency|TestTryPreAcceptCommittedStaleDependencyEvidenceOmittingCandidateFallsBackToAccept' -count=1` (`artifact://460`) passed after adding sender-preserving AcceptEvidence, read-only committed-conflict evidence queries, authorized TryPreAccept ignore resends, and fail-closed slow accept for omitting evidence.
- `go test ./epaxos -count=1` (`artifact://471`) and `go test ./examples/kv -count=1` (`artifact://473`) passed after the sender-preserving evidence-query/resend-ignore and v6 KV AcceptEvidence persistence changes.
- `go test ./examples/kv -run 'Test(RecoverReplicaFromLiveCheckpoint|VerifyCheckpoint.*Semantic|VerifyCheckpoint.*CrashWindow)' -count=1 -v` (`artifact://522`) passed after adding semantic checkpoint validation, live-source checkpoint replacement, quorum-support checks, and target-owned floor protection for single-node corruption recovery.
- `go test ./examples/kv -count=1` (`artifact://524`) passed after the checkpoint-backed live recovery implementation and tests.
- `bash tests/chaos_fault_campaign.sh` (`artifact://537`) passed after wiring semantic checkpoint verification and live-source recovery tests into the KV persistence fault matrix.
- `go test ./epaxos -count=1` (`artifact://620`) and `go test ./examples/kv -count=1` (`artifact://621`) passed after adding the finite evidence-query model, preserving the max-sequence recovery fail-safe, and adding KV recovery/iterator coverage seams.
- `go test -tags kvnode ./examples/kv/cmd/kvnode -run TestPeerBodyLimitAcceptsValidMessageAndRejectsOversizedWithoutSteppingNode -count=1 -v` (`artifact://627`) passed after making the synthetic peer commit carry the required `RecordBallot`.
- `bash tests/go_coverage.sh` passed after the EPaxos residual fail-safe coverage, KV iterator/recovery seam coverage, and peer-commit `RecordBallot` test fix; `epaxos` and `examples/kv` coverage profiles reported 100.0%, and the tagged `kvnode` package passed under the script.
- `bash tests/tla_model_check.sh` passed after adding `tla/EPaxosEvidenceQuery.tla` and wiring the 3-, 5-, and 7-replica evidence-query configs; `EPaxosEvidenceQuery.cfg` generated 352 states and 279 distinct states, `EPaxosEvidenceQueryFive.cfg` generated 14548 states and 9015 distinct states, and `EPaxosEvidenceQuerySeven.cfg` generated 393256 states and 224551 distinct states, with no TLC error.
- `bash tests/release_scope_audit.sh`, `bash tests/audit_repo.sh`, `bash tests/operations_readiness_audit.sh`, and `bash tests/go_no_go_workflow.sh` passed after the finite evidence-query model, deployment-manifest audit, documentation, and audit updates; the workflow reported `release_decision=No-go.` with `open_release_items=6` (`artifact://655`).
- `bash tests/kvnode_systemd_manifest_audit.sh` passed on this host after adding the cross-platform manifest exercise; it rendered the example `kvnode@.service`/EnvironmentFile `ExecStart` contract and skipped `systemd-analyze` because analyzer verification is opt-in via `KVNODE_SYSTEMD_ANALYZE=yes`.
- `bash tests/jepsen_remote_preflight_audit.sh` passed after adding remote Jepsen preflight-only safety coverage; it verifies destructive-storage and wall-clock-skew confirmations, broad remote-directory rejection, and safe confirmed preflight paths without touching remote hosts.
- `KVNODE_UPGRADE_OLD_REF=66428ba KVNODE_UPGRADE_NEW_REF=64cff8d KVNODE_UPGRADE_BASE_PORT=50080 KVNODE_UPGRADE_PEER_BASE_PORT=50180 KVNODE_UPGRADE_ADMIN_BASE_PORT=50280 KVNODE_UPGRADE_READY_ATTEMPTS=80 KVNODE_UPGRADE_CANARY_ATTEMPTS=10 KVNODE_UPGRADE_SETTLE_SECONDS=1 bash tests/kvnode_mixed_version_drill.sh` passed on local loopback after adding `tests/kvnode_mixed_version_drill.sh`; metadata reported old commit `66428baafa1ec75deb00ff1cedb2a55d934594c3`, new commit `64cff8d58fffcf882a7156f4459138055f4b0510`, differing source hashes, differing binary SHA-256s, split client/peer/admin support in both binaries, `build_source=git_archive_trimpath`, `smoke_only=no`, per-node upgraded-writer and rolled-back-writer 204 canaries, latest GET checks, and barrier scan checks. Boundary: this old/new delta is timeout-outcome wording only, so it proves local drill mechanics for two distinct binaries and not broad protocol/storage compatibility across substantive release deltas.
- `JAVA_BIN=/opt/homebrew/opt/openjdk/bin/java bash tests/tla_model_check.sh` passed after wiring `tla/EPaxosRollbackAllocation.cfg`; `EPaxosRollbackAllocation.cfg` generated 7 states and 7 distinct states with no TLC error.
- `go test ./epaxos -run TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit -count=1` passed after adding the explicit `recoveryRequireAppliedOrder(... learnedRef, afterCatchUpRef)` assertion on all replicas (`artifact://921`).
- `bash tests/release_scope_audit.sh`, `bash tests/audit_repo.sh`, `bash tests/operations_readiness_audit.sh`, and `bash tests/go_no_go_workflow.sh` passed after the rollback-allocation model, applied-order regression, evidence updates, and audit checks; the workflow preserved `release_decision=No-go.` with `open_release_items=5`.
- `JAVA_BIN=/opt/homebrew/opt/openjdk/bin/java bash tests/tla_model_check.sh` passed after wiring `tla/EPaxosConfigRemoveTransition.cfg`; `EPaxosConfigRemoveTransition.cfg` generated 70 states and 70 distinct states with no TLC error.
- `go test ./epaxos -run TestRemoveVoterConfChangeKeepsOldInFlightInstancePinned -count=1` passed after making per-instance quorum thresholds/broadcasts use `Ref.Conf` and rejecting local proposals from removed voters (`artifact://964`).

### Fault-tolerance envelope proof summary

The release lock's `## Fault-tolerance target and evidence matrix` is the source of truth for tolerated fault scope. Evidence below proves normal operation only inside that simulation/local-loopback envelope; it does not convert the current no-go decision into a production-ready claim.

| Fault-tolerance target | Fault-count boundary | Normal operation evidence in this bundle |
| --- | --- | --- |
| Quorum-preserving crash/restart/omission, storage-unavailable, and network delivery faults | Supported one-to-seven-voter configs; progress requires a Healthy quorum with tolerated unavailable counts `0,0,1,1,2,2,3` for `N=1..7`. | `bash tests/chaos_fault_campaign.sh`; `go test ./epaxos -run 'TestDST.*(FailureBoundary|StorageFailureBoundary|Linearizability|Storage)' -count=1`; `TestDSTFailureBoundarySlowQuorumSizesOneThroughSeven`; `TestDSTStorageFailureBoundarySlowQuorumSizesOneThroughSeven`; `go test ./epaxos -count=1`; deterministic DST/linearizability/liveness/availability tests; local Jepsen restart/transport/storage profiles. |
| Deterministic timing faults | Logical tick skew/pause and finite TOQ clock-discipline contract only. | `TestUnevenLogicalTickSkewAndBurstConvergesWithoutDuplicates`; `TestPausedClockDoesNotTickOrProcessReadyUntilResume`; `tla/TOQClockDiscipline.cfg`; OS wall-clock mutation is outside current release evidence. |
| VM rollback, storage/wire restart rollback, and durable restart catch-up | One isolated rolled-back replica with quorum data still trusted; one-node-at-a-time storage/wire restart plus rollback simulation; finite rollback-allocation model for older checkpoint plus learned own committed ref. | `TestRolledBackNodeCatchesUpFromQuorumWithoutDuplicateApply`; `TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit`; `tla/EPaxosRollbackAllocation.cfg`; `TestStorageWireRestartUpgradeRollbackSimulationConvergesWithoutDuplicateApply`; KV durable reopen tests; chaos fault campaign. |
| Storage unavailable/destructive local recovery | Local deterministic storage-write failures cover F/F+1 boundaries for `N=1..7`; local Jepsen destructive-storage removes/restores one 3-node member while quorum peers remain trusted. | `TestDSTStorageFailureBoundarySlowQuorumSizesOneThroughSeven`; `env JEPSEN_LOCAL_FAULTS=destructive-storage bash tests/jepsen_local.sh`; local destructive-storage profile inside `tests/chaos_fault_campaign.sh`; `go test ./examples/kv -count=1`. |
| Bit-level persisted-record corruption | One stopped/corrupt KV node with healthy quorum support; recovery uses semantic checkpoint verification plus either offline checkpoint repair or a live-source checkpoint replacement. | `TestOpenClusterRejectsBitFlippedPersistedEPaxosRecord`; `TestCheckpointRepairRecoversBitFlippedPersistedEPaxosRecord`; `TestVerifyCheckpointRejectsSemanticDataCorruption`; `TestVerifyCheckpointCrashWindowMarkerRules`; `TestRecoverReplicaFromLiveCheckpointRestoresStoppedReplica`; `TestRecoverReplicaFromLiveCheckpointRejectsUnsupportedSource`; `TestRecoverReplicaFromLiveCheckpointRejectsTargetOwnedFloorMismatch`; `bash tests/chaos_fault_campaign.sh`. |
| Configuration-change ordering and pinning | Finite local barrier plus one add-voter and one remove-voter transition. | `tla/EPaxosConfigBarrier.cfg`; `tla/EPaxosConfigTransition.cfg`; `tla/EPaxosConfigRemoveTransition.cfg`; `go test ./epaxos`; no arbitrary/multi-step/recovery-under-config-change proof. |
| Service-plane fault containment | HTTP request/route/storage-readiness faults in the example KV node. | `go test -tags kvnode ./examples/kv/cmd/kvnode -count=1`; `tests/operations_readiness_audit.sh`; request/body/scan/readiness/TLS tests named in `RELEASE_SCOPE.md`. |

Non-claims remain explicit: No target-environment remote claim, no in-place Pebble/WAL repair, no checksum recomputation or corrupt-record deletion, no synthesized reconstruction without a verified checkpoint, and no multi-replica/quorum-loss corruption recovery claim.

### Optional external validation tooling

- Current release evidence is simulation/local-loopback scoped. External multi-host Jepsen tooling is retained for operators who run and archive it separately, but it is not counted as current release evidence.
- Optional external validation must stay separate from the closed claims above; this bundle does not contain target-environment external histories.

### Operations artifact gates

- `bash tests/operations_readiness_audit.sh` passed in this session.
- `bash -n tests/kvnode_capacity_envelope.sh` passed in this session.
- `bash tests/kvnode_capacity_envelope.sh --help` passed in this session and showed the opt-in bounded harness usage.
- A local single-node loopback capacity-envelope sample passed with `KVNODE_CAPACITY_RUN=yes`, `KVNODE_CAPACITY_OPS_PER_PHASE=5`, `KVNODE_CAPACITY_VALUE_BYTES=64,1024`, and `KVNODE_CAPACITY_SCAN_LIMITS=1,8`; archived sample files are `local://kvnode-capacity-loopback-20260708T194748Z-summary.txt`, `local://kvnode-capacity-loopback-20260708T194748Z-latency.csv`, and `local://kvnode-capacity-loopback-20260708T194748Z-resources.csv`; generated summary reported 22 HTTP operations, 22.000 sampled operations/second, p50 0.007860s, p95 0.008602s, p99 0.008797s, and harness-only/non-production status.
- `bash tests/release_scope_audit.sh` passed in this session after release-scope updates.

### New or updated artifacts in this evidence bundle scope

- `jepsen/src/moreconsensus/epaxos_test.clj`
  - Contains local loopback restart, transport, storage, and destructive-storage profiles plus the advanced scan checker used by the simulation/local-loopback evidence.
- `jepsen/test/moreconsensus/epaxos_test_test.clj`
  - Adds scan-checker regressions and optional external-tooling unit guardrails; optional external tooling is not current release evidence.
- `tests/jepsen_remote_preflight_audit.sh`
  - Exercises `tests/jepsen_remote.sh` in preflight-only mode for missing destructive-storage confirmation, unsafe remote-directory rejection, confirmed safe destructive-storage preflight, missing wall-clock-skew confirmation, and confirmed wall-clock-skew preflight; `tests/ci.sh` now runs this audit before operations/release-scope checks.
- `examples/kv/cmd/kvnode/main.go`
  - Peer replication handler now rejects non-POST requests before reading or decoding the body.
- `examples/kv/cmd/kvnode/main_test.go`
  - Adds `TestHandleMessageRejectsNonPostBeforeReadingBody`.
- `examples/kv/backup.go`
  - Adds offline Pebble checkpoint, semantic read-only checkpoint verification for EPaxos records/applied markers/dense timestamped KV rows/dependency order, explicit checkpoint-backed repair, and whole-directory restore helpers for the example KV store.
- `examples/kv/more_test.go`
  - Adds checkpoint restore, explicit checkpoint-backed repair, semantic checkpoint rejection, live-source recovery, unsupported-source rejection, target-owned floor mismatch rejection, corrupt checkpoint rejection, missing checkpoint rejection, and post-repair writes.
- `tests/chaos_fault_campaign.sh`
  - Adds checkpoint restore, explicit checkpoint-backed repair, semantic checkpoint verification, and live-source recovery tests to the KV persistence fault matrix.
- `deploy/systemd/kvnode@.service`
  - Example/operator systemd template, not a verified production deployment.
- `deploy/systemd/kvnode.env.example`
  - Example/operator environment file for a three-node topology.
- `tests/kvnode_systemd_manifest_audit.sh`
  - Cross-platform static manifest exercise that validates required environment variables, renders the example `ExecStart`, checks peer/deadline/body-limit values, and keeps host-context `systemd-analyze verify` opt-in through `KVNODE_SYSTEMD_ANALYZE=yes`.
- `docs/operations/kvnode-data-lifecycle-incident-runbook.md`
  - Data lifecycle and incident runbook with backup, semantic checkpoint verification, explicit repair, restore, checksum, destructive-storage, live-source recovery boundaries, and incident procedures.
- `docs/operations/kvnode-upgrade-rollback.md`
  - Rolling upgrade and rollback plan.
- `tests/kvnode_capacity_envelope.sh`
  - Opt-in bounded capacity-envelope collection harness.
- `tests/kvnode_mixed_version_drill.sh`
  - Local loopback old/new binary rolling-upgrade and binary-rollback harness. It requires explicit `KVNODE_UPGRADE_OLD_REF`, builds old/new binaries from clean git archives with `-trimpath -buildvcs=false`, records source-tree hashes and binary SHA-256s, rejects identical refs/source/binaries unless `KVNODE_UPGRADE_SMOKE_ONLY=yes`, exercises one-node-at-a-time upgrade and rollback, and verifies each upgraded/rolled node with a 204 write plus latest GET and barrier scan from all nodes. Binary rollback uses the node's current data; checkpoint restore is documented as a separate data-lifecycle fallback, not this mixed-version drill. The currently archived old/new refs differ only by timeout-outcome wording, so this is drill-mechanics evidence for distinct binaries, not broad protocol/storage compatibility evidence.
- `tests/operations_readiness_audit.sh`
  - Audit for the operations artifacts above.
- `tests/toolchain.env`, `.github/workflows/ci.yml`, and `tests/toolchain_audit.sh`
  - Pin Go, Java, TLC, Leiningen, and GitHub Actions reproducibly; `actions/checkout`, `actions/setup-go`, `actions/setup-java`, and `actions/cache` are pinned to full 40-character commit SHAs resolved from their tag refs.
- `tla/EPaxosRecoveryFive.cfg`
  - Adds a 5-replica finite stopped-owner recovery configuration for the existing `EPaxosRecovery.tla` model.
- `tla/EPaxosOptimizedRecovery.tla`, `tla/EPaxosOptimizedRecovery.cfg`, `tla/EPaxosOptimizedRecoveryFive.cfg`, and `tla/EPaxosOptimizedRecoverySeven.cfg`
  - Add finite 3-, 5-, and 7-replica optimized-recovery coverage: prepare fast-witness threshold gates TryPreAccept, AcceptReply evidence is recorded without changing chosen attributes, and Accept-Deps covers only the stale-dependency case where chosen `Deps` already order the conflict.
- `tla/EPaxosEvidenceQuery.tla`, `tla/EPaxosEvidenceQuery.cfg`, `tla/EPaxosEvidenceQueryFive.cfg`, and `tla/EPaxosEvidenceQuerySeven.cfg`
  - Add finite 3-, 5-, and 7-replica committed-conflict evidence-query coverage: candidate-dependency/same-config guards before `MsgEvidence`, read-only response handling, duplicate/mismatched response drops, sender-preserving `AcceptEvidence` validation, stale TryPreAccept rejection restart, and fail-closed slow accept on missing, legacy-only, malformed, contradictory, or insufficient evidence.
- `tla/EPaxosConfigBarrier.tla` and `tla/EPaxosConfigBarrier.cfg`
  - Add finite local configuration-barrier coverage: two fixed config refs plus one user command check `pendingConf` blocking, dependency-vector inclusion, sequence elevation, barrier retention while another config remains unexecuted, and clearing after no known config remains unexecuted. This does not model dynamic membership transitions, quorum changes, recovery under config changes, durable storage, or unbounded instance spaces.
- `tla/EPaxosConfigTransition.tla` and `tla/EPaxosConfigTransition.cfg`
  - Add finite add-voter configuration-transition coverage: a config command chosen/executed under the old config, an old in-flight user instance remaining pinned to old voters/quorum after transition, and a later user instance using the new voters/quorum. This does not model multi-step reconfiguration chains, joint consensus, recovery under configuration changes, durable replay, or unbounded configuration histories.
- `tla/EPaxosConfigRemoveTransition.tla` and `tla/EPaxosConfigRemoveTransition.cfg`
  - Add finite remove-voter configuration-transition coverage: a config command chosen/executed under the old four-voter config, an old in-flight user instance remaining pinned to old voters/quorum and able to count the removed voter after transition, and a later user instance using the new three-voter quorum excluding the removed voter. This does not model joint consensus, concurrent config commands, recovery, message loss, durable replay, or unbounded configuration histories.
- `tla/EPaxosRollbackAllocation.tla` and `tla/EPaxosRollbackAllocation.cfg`
  - Add finite rollback-allocation coverage: an older local checkpoint, one later own committed instance learned from quorum, `nextInstance` advancement, defensive allocation skip over a known future local ref under a stale-next state, fresh allocation at instance 4, and learned-before-fresh apply-sequence order.
- `tla/EPaxosRevisited.tla`
  - Now checks finite explicit TOQ zero-sequence/empty-dependency envelopes, delayed owner assignment at `ProcessAt`, pending-decision blocking, receiver `ProcessAt` processing, fast-wait behavior, and chain pruning; physical clock synchronization and OWD measurement remain abstract configured inputs.
- `tla/TOQClockDiscipline.tla` and `tla/TOQClockDiscipline.cfg`
  - Add finite TOQ embedder clock-discipline contract evidence: bounded receiver clock skew plus bounded one-way delay makes the sender-chosen `ProcessAt` late enough for sync-group delivery before local processing. In Go/API terms, every configured `TOQOneWayDelay[id]` value must already include clock-skew/synchronization uncertainty; this remains a finite contract check, not an implementation of clock synchronization or delay measurement.
- `tests/tla_model_check.sh`
  - Wires `tla/EPaxosRecoveryFive.cfg`, `tla/EPaxosOptimizedRecovery.cfg`, `tla/EPaxosOptimizedRecoveryFive.cfg`, `tla/EPaxosOptimizedRecoverySeven.cfg`, `tla/EPaxosEvidenceQuery.cfg`, `tla/EPaxosEvidenceQueryFive.cfg`, `tla/EPaxosEvidenceQuerySeven.cfg`, `tla/EPaxosConfigBarrier.cfg`, `tla/EPaxosConfigTransition.cfg`, `tla/EPaxosConfigRemoveTransition.cfg`, `tla/EPaxosRollbackAllocation.cfg`, the updated `tla/EPaxosRevisited.cfg`, and `tla/TOQClockDiscipline.cfg` into the TLC gate, and gives each TLC config its own temporary `-metadir`.
- `MODEL_EQ_REPORT.MD`
  - Records the updated TOQ, ballot-aware response-recovery, Accept-Deps optimized-recovery, committed-conflict evidence-query, config-barrier, add-voter config-transition, remove-voter config-transition, and rollback-allocation model scope plus observed `EPaxosConfigRemoveTransition.cfg` `70/70`, `EPaxosRollbackAllocation.cfg` `7/7`, `EPaxosRevisited.cfg` `2936/1584`, `TOQClockDiscipline.cfg` `3888/1944`, `EPaxosResponses.cfg` `17725/1971`, `EPaxosResponsesFive.cfg` `25341619/1076385`, `EPaxosOptimizedRecovery.cfg` `199/106`, `EPaxosOptimizedRecoveryFive.cfg` `4339/1634`, `EPaxosOptimizedRecoverySeven.cfg` `145912/41948`, `EPaxosEvidenceQuery.cfg` `352/279`, `EPaxosEvidenceQueryFive.cfg` `14548/9015`, `EPaxosEvidenceQuerySeven.cfg` `393256/224551`, `EPaxosConfigBarrier.cfg` `499/193`, and `EPaxosConfigTransition.cfg` `62/62` generated/distinct states with no TLC error.
- `epaxos/types.go`, `epaxos/message.go`, `epaxos/codec.go`, `epaxos/checksum.go`, and `epaxos/node.go`
  - Add explicit TOQ API/configuration, message flagging/validation/codec/checksum support, durable `ProcessAt` and `TOQPending`, delayed local assignment, receiver-side TOQ attr computation, pending-finalization gating, zero-`ProcessAt` handling, recovery-only `AcceptSeq`/`AcceptDeps` aggregate evidence, sender-preserving `AcceptEvidence`, read-only `MsgEvidence`/`MsgEvidenceResp` committed-conflict checks, checksum-covered durable `InstanceRecord.RecordBallot`, `MsgPrepareResp.RecordBallot` separation of promise and value ballots, exact current-ballot `MsgAcceptResp` counting before Accept-Deps evidence merge, targeted leader-in-fast-quorum/deferred-cycle TryPreAccept handling, committed-conflict slow-accept hardening that folds a same-config `ConflictRef` into `Deps` and bumps `Seq` when the committed tuple is known, per-instance configuration pinning for dependency width/quorum/broadcast paths, removed-local-voter proposal rejection, and legacy checksum verifier support for example-KV migration.
- `epaxos/toq_test.go`, `epaxos/optimized_test.go`, `epaxos/protocol_coverage_test.go`, `epaxos/internal_test.go`, `epaxos/pool_ownership_test.go`, and `epaxos/branch_test.go`
  - Cover explicit TOQ proposal, restart, receiver, validation, zero-`ProcessAt`, pending configuration changes, optimized quorum fast commit including three-node TOQ local-plus-one-remote quorum, Accept-Deps carriage/persistence, sender-preserving evidence, read-only committed-conflict evidence queries, authorized TryPreAccept ignore markers, fail-closed omitting/legacy evidence, durable RecordBallot preservation across prepare/restart, highest accepted record-ballot recovery selection, initial-leader implicit TryPreAccept witness counting, leader-in-fast-quorum uncommitted-conflict slow accept, deferral/recovery of blockers, recorded deferral-cycle slow accept, committed-conflict slow-accept dependency plus sequence bump hardening, non-current-ballot AcceptResp rejection, AcceptSeq/AcceptDeps validation invariants, legacy checksum compatibility, Accept-Deps wire codec round-trip, decode/encode bounds, stale self-messages, single-voter commit, mode-mismatch rejection, checksum-version verifiers, TOQ wire codec round-trip, and TOQ higher-attrs eligibility.
- `examples/kv/epaxos_storage.go`, `examples/kv/cluster.go`, and `examples/kv/more_test.go`
  - Bump the EPaxos record storage codec to v6, reject EPaxos record key/value ref mismatches during load, persist TOQ metadata, sender-preserving Accept-Deps recovery evidence, and durable RecordBallot value-ballot evidence, migrate pre-RecordBallot/pre-Accept-Deps/pre-TOQ/pre-fast-path legacy checksums to canonical checksums, and cover oversized AcceptDeps plus malformed AcceptEvidence rejection across v1/v2/v3/v4/v5/v6 compatibility paths.
- `epaxos/quorum.go`, `epaxos/node.go`, `epaxos/message.go`, `epaxos/codec.go`, and `epaxos/checksum.go`
  - Add optimized EPaxos fast quorum thresholds for odd supported cluster sizes, retain conservative thresholds for even sizes, carry `DepsCommitted` prefix evidence on messages, and gate fast commit on matching fast-quorum dependency evidence.
- `epaxos/sim_test.go`, `epaxos/recovery_test.go`, `epaxos/remaining_test.go`, `epaxos/optimized_test.go`, and `epaxos/branch_test.go`
  - Cover the updated quorum table, five-node optimized fast path, divergent slow-path fallback, timing-boundary behavior, optimized TryPreAccept witness thresholds, remove-voter in-flight old-config pinning, removed-voter proposal rejection, rollback catch-up local `nextInstance` monotonicity, learned-before-fresh applied-order assertion, restart recovery timers, stale accept-reject hints, stale-originator fast-commit prerequisites, and local/remote fast-path eligibility evidence.
- `epaxos/dst_test.go`
  - Adds exact slow-quorum failure-boundary DST coverage for `N=1..7`, storage-write failure-boundary coverage for `N=1..7`, fail-closed no-quorum omission/crash/storage scenarios, five-node majority/minority partition behavior, conflict/read linearizability across partition heal, and transient Ready durable-write retry exactly-once behavior.
- `EPAXOS_IMPLEMENTATION_PROOF.md`
  - Adds the paper-grounded algorithm explanation, exact failure-count matrix, property-by-property proof rationale, TLA+/DST/Jepsen evidence map, and current non-claims/no-go blockers.
- `tests/chaos_fault_campaign.sh`
  - Includes the DST omission/storage boundary tests, TOQ three-node fast-quorum test, and stale AcceptResp regression in the core fault matrix, and labels the core node coverage as `nodes=1..7`.
- `tla/EPaxos.tla`, `tla/EPaxosResponses.tla`, and `tla/Quorum.tla`
  - Align finite model quorum formulas with optimized odd-size thresholds/even-size conservative thresholds, model normal-case dependency-commit evidence before fast commit, and add bounded response-model coverage for prepare branch priority, the `fast + slow - N` TryPreAccept witness threshold, plus current-ballot AcceptOK evidence so lower-/higher-ballot non-reject Accept replies cannot satisfy the current Accept quorum.
- `tests/go_no_go_workflow.sh`
  - Final no-go/go rule workflow for this release lock and evidence bundle.

## Current open blockers preserving no-go

The following blockers are still listed in `RELEASE_SCOPE.md` and prevent a go decision:

- Broader formal model coverage remains open beyond the finite configured TLC suite; `tla/EPaxosResponses.tla` adds bounded prepare branch-priority/try-witness checks, `tla/EPaxosOptimizedRecovery.tla` adds finite 3-, 5-, and 7-replica Accept-Deps optimized-recovery evidence checks, `tla/EPaxosEvidenceQuery.tla` adds finite 3-, 5-, and 7-replica committed-conflict evidence-query guard/fail-closed checks, `tla/EPaxosConfigBarrier.tla` adds finite local config-barrier checks, `tla/EPaxosConfigTransition.tla` adds one finite add-voter config-transition pinning check, `tla/EPaxosConfigRemoveTransition.tla` adds one finite remove-voter config-transition pinning check, `tla/EPaxosRollbackAllocation.tla` adds one finite rollback-allocation next-instance/skip/apply-order check, and `tla/TOQClockDiscipline.tla` adds a finite bounded-skew/bounded-delay `ProcessAt` contract check, but operational synchronized-clock/OWD-measurement implementation proof for TOQ deployments, unbounded proof, arbitrary membership-change proof, multi-step reconfiguration proof, recovery under configuration changes, complete optimized-recovery branch parity, full rollback-history proof, and arbitrary application/state-machine semantics remain open.
- Deployment manifest artifacts are example/operator material only; `tests/kvnode_systemd_manifest_audit.sh` renders and audits the example `ExecStart` contract, but reviewed execution under a target system manager, container, or orchestration environment remains open.
- Data lifecycle runbook exists, but a reviewed operator backup/restore/disaster-recovery drill remains open. The mixed-version drill's binary rollback keeps current data and does not exercise checkpoint restore.
- Target-environment capacity-envelope measurements remain open.
- Incident readiness has runbook/audit evidence only; operator review/tabletop or live drill evidence remains open.

## Final workflow command

Run:

```sh
bash tests/go_no_go_workflow.sh
```

Expected current result: no-go, with open release items listed from `RELEASE_SCOPE.md`.
