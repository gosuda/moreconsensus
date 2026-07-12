# Release Scope Lock

This file is the self-contained authority for the active EPaxos production-readiness goal.

## Scope rule

A release claim is allowed only when the item is listed in **Closed release items** with current evidence. Any required item in **Open release items** forces a no-go decision. This file records the boundaries and evidence classes; it does not convert local evidence into production approval.

## Current release decision

No-go.

The repository provides bounded protocol, storage, service, and deterministic-simulation evidence on Darwin arm64. It is not mission-critical production-ready. The missing release evidence includes unbounded Go/TLA refinement, certified protocol-state compaction, real-network fault evidence, and signed operator-controlled capacity/lifecycle/incident evidence.

## Evidence identity

- `release/EPAXOS_READINESS_EVIDENCE.md` is the current repository evidence snapshot.
- A production release must bind every closed item to a source revision, release binary digest, target identity, immutable evidence root, and independent reviewer signature. No such target-bound closure bundle is present for this decision.
- Local and hosted test results are verification observations, not release signatures. The coverage gate measures production packages separately: `epaxos` must reach at least 85.0% and `examples/kv` at least 90.0%; verification collectors remain covered by their root Go and race suites.

## Verification prerequisites

| Gate | Status |
| --- | --- |
| Aggregate Go coverage | pass |

## Fault-tolerance target and evidence matrix

This matrix defines the supported simulation/local-loopback envelope. A claim is limited to its row, its stated fault count, and a surviving healthy quorum. Normal operation means safety, no duplicate committed application, and the row-specific progress or fail-closed behavior. The matrix does not claim multi-host or real-network operation.

| Fault class | Boundary | Evidence and behavior | Explicit limit |
| --- | --- | --- | --- |
| Crash, restart, owner stop, and storage omission | Voter counts 1 through 7; tolerated unavailable counts are `0,0,1,1,2,2,3` for `N=1..7`; progress requires a slow quorum; no Byzantine behavior. | Deterministic simulation tests and `bash tests/chaos_fault_campaign.sh` cover progress, recovery, and duplicate-free application when a healthy quorum remains. | No quorum-loss, Byzantine, or target-host claim. |
| Message omission, duplication, reordering, and partition | Deterministic transport faults and local Jepsen loopback profiles; a healthy quorum side must remain. | `epaxos/dst_test.go`, `epaxos/stress_test.go`, and the local campaign cover heal-and-converge and minority fail-closed behavior. | No real-network or multi-host history. |
| Logical timing faults | Deterministic `RawNode.Tick` skew and pause; explicit TOQ uses caller-provided clock and delay bounds. | `TestUnevenLogicalTickSkewAndBurstConvergesWithoutDuplicates`, `TestPausedClockDoesNotTickOrProcessReadyUntilResume`, and `tla/TOQClockDiscipline.cfg`. | The core does not synchronize clocks or measure one-way delay. |
| Rollback and restart catch-up | One isolated replica may roll back to an earlier durable state while quorum data remains trusted. | `TestRolledBackNodeCatchesUpFromQuorumWithoutDuplicateApply`, `TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit`, and `tla/EPaxosRollbackAllocation.cfg`. | No arbitrary rollback history or quorum-loss recovery. |
| Checksum-detected corruption | One stopped or corrupt KV member; a healthy quorum and a verified checkpoint are required. | KV checkpoint verification and whole-directory replacement tests fail closed on corrupt records and preserve duplicate-free application. | No in-place WAL repair, checksum rewriting, synthesized recovery, or target drill. |
| Configuration ordering and pinned quorums | Finite barrier, add, remove, add-then-remove, replay, retry, and de-duplication scenarios. | Go configuration tests and the finite models listed in `MODEL_EQ_REPORT.MD` cover old `Ref.Conf` voter domains and current-configuration barriers. | No arbitrary membership history, joint consensus, or unbounded proof. |
| Fast path and optimized recovery | Odd supported sizes use the paper fast quorum; even sizes retain conservative thresholds; FP-deps-committed evidence and sender-preserving recovery evidence are required. | `epaxos/quorum.go`, focused Go tests, and finite quorum/evidence/recovery models. | No arbitrary network, message-loss, durable-history, or unbounded optimized-recovery claim. |
| Service-plane containment | Separate client, peer, and admin listeners; TLS 1.3 mutual authentication; bounded request bodies and scans; deterministic lifecycle-owned ticks. | Tagged KV behavior/race tests, service fault campaign, and operations audits. | No multi-tenant RBAC or independent failure-domain claim. |

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
| Finite formal model gate | `tests/tla_model_check_fast.sh` runs the required 14 finite jobs; `tests/tla_model_check.sh` is the larger manual suite. Correspondence limits remain explicit in `MODEL_EQ_REPORT.MD`. |
| Operations artifact checks | `tests/operations_readiness_audit.sh`, `tests/kvnode_capacity_envelope.sh`, `tests/kvnode_incident_tabletop_drill.sh`, and the local lifecycle helpers. These are example/operator artifacts, not target-environment proof. |

## Open release items


| Item | Current state |
| --- | --- |
| Broader formal model coverage | The finite TLC suite and executable `RawNode` trace cover bounded workflows. Exit requires an unbounded Go/TLA refinement argument, a checked action correspondence, and certified protocol-state compaction requirements including late-message and incarnation fencing. |
| Data lifecycle | Local checkpoint verification, restore, repair, and destructive-storage exercises exist. Exit requires a certified compaction operational drill plus target backup, restore, rollback, and disaster-recovery evidence. |
| Capacity envelope | A bounded local harness records throughput, latency, memory, disk, queue, value-size, scan, and peer-count samples. Exit requires a signed target capacity envelope with workload, resource, latency, and retention limits. |
| Incident readiness | A local tabletop harness exercises storage and transport fault branches and preserves non-claims. Exit requires real-network fault evidence across multi-host independent failure domains plus signed operator-controlled incident, escalation, rollback, and recovery evidence. |

Exactly these four canonical items remain open. Closing a row requires direct evidence that satisfies its exit condition; changing prose or rerunning a local sample is insufficient.

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
- `tla/EPaxosInductiveProofs.tla` proves only its restricted semantic `ConcreteNext`; it does not cover codec, allocation, `Ready` ownership, SCC execution, TOQ timing, or arbitrary `RawNode` executions.
- `tla/EPaxosRawNodeRefinement.tla` is an implementation-shaped bounded workflow model. `tests/refinementtrace` adds executable pre/post contracts and exported-method inventory checks, not a TLC/TLAPS action replay.
- `tla/EPaxosVoterBootstrap.tla` is a model-only bootstrap contract; bootstrap state is not part of the current `RawNode` semantic trace.
- The core does not synchronize clocks, measure one-way delay, prove operational TOQ discipline, or provide a production sync-group service.
- Deterministic simulation and local Jepsen loopback do not claim real-network or multi-host fault histories.
- Retention thresholds limit admission without deleting protocol history; certified compaction and unbounded uptime remain open.
- KV checkpoint recovery requires semantic verification and whole-directory replacement; in-place repair, synthesized reconstruction, and quorum-loss recovery remain open.
- Local mixed-version rollback validates the harness for its exercised binary pair; it does not prove broad compatibility across substantive protocol or storage changes.
- Example/operator reports are non-claims until target identity, signed provenance, immutable evidence, and independent review satisfy the corresponding open row.
