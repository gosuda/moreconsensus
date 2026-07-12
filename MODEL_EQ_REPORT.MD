# Model Correspondence and Scope Report

This report maps the finite TLA+ models to the Go implementation, compares the repository model suite with the attached original model, and states the direct evidence and limits for each correspondence layer. It is a current scope snapshot.

## Correspondence status

The repository uses several deliberately different evidence layers:

1. Paper-oriented TLA+ models describe protocol obligations and finite recovery slices.
2. `EPaxosRawNodeRefinement.tla` models implementation-shaped bounded workflows.
3. `tests/refinementtrace` executes real `RawNode` transitions and validates semantic pre/post contracts.
4. Go, race, simulation, service, and storage tests exercise behavior outside the TLA state space.

No checked theorem currently connects arbitrary Go executions to `EPaxosInductiveProofs.tla`, and no checked action replay connects TLC transitions to every `RawNode` transition. Those are release-blocking correspondence gaps.

## Model inventory

| Model | Direct scope | Go or executable evidence |
| --- | --- | --- |
| `tla/EPaxos.tla` | Normal PreAccept, Accept, Commit, local conflict attributes, quorum transitions, and minimal-component execution. | `epaxos/node.go`, `epaxos/sparse_progress.go`, `epaxos/revisited_test.go`. |
| `tla/EPaxosResponses.tla` | Fast/slow response evidence, prepare branch priority, TryPreAccept witness threshold `fast + slow - N`, and current-ballot AcceptOK gating. | `epaxos/node.go`, `epaxos/optimized_test.go`, `epaxos/protocol_coverage_test.go`. |
| `tla/Quorum.tla` | Supported quorum tables and intersections. | `epaxos/quorum.go`, `TestQuorumTables`, cluster-size tests for 1 through 7. |
| `tla/EPaxosRecovery.tla` | Stopped-owner dependency recovery and no-op unblocking under finite fairness. | `epaxos/recovery_test.go`, owner-independent recovery tests. |
| `tla/EPaxosOptimizedRecovery.tla` | Finite 3/5/7 Accept-Deps recovery evidence where chosen attributes remain separate from recovery-only evidence. | `epaxos/node.go`, `epaxos/message.go`, `epaxos/optimized_test.go`, `examples/kv/epaxos_storage.go`. |
| `tla/EPaxosOptimizedRecoveryDecisionTree.tla` | Every implemented F<=3 optimized-recovery decision branch, including no-op, committed, accepted, preaccepted, stale, evidence-authorized, fail-closed, force/defer, quorum, and duplicate/old response branches. | `epaxos/optimized_test.go`, `epaxos/protocol_coverage_test.go`. |
| `tla/EPaxosTryPreAcceptBranches.tla` | Finite abstract TryPreAccept response branches for stale restart, committed evidence, direct/forced accept, deferred conflict, and slow-quorum accept. | `epaxos/node.go`, focused TryPreAccept tests. |
| `tla/EPaxosTryPreAcceptMessagePath.tla` | Finite 3/5/7 request/response paths for follower persistence/duplicate handling and coordinator stale, evidence, conflict, and quorum branches. | Message-path regressions in `epaxos/protocol_coverage_test.go`. |
| `tla/EPaxosTryConflictForce.tla` | Finite leader-in-candidate-fast-quorum force/defer arithmetic. | `epaxos/node.go`, optimized recovery tests. |
| `tla/EPaxosEvidenceQuery.tla` | Guarded evidence request, read-only response, sender validation, stale restart, ignore resend, and fail-closed fallback. | `epaxos/node.go`, evidence-query tests, message validation tests. |
| `tla/EPaxosEvidenceStaleness.tla` | Older-ballot response drop, first-response-wins duplicate scoping, and fail-closed empty evidence. | `TestEvidenceStaleDuplicateCommittedTupleFallsBackToSlowAccept`. |
| `tla/EPaxosAcceptEvidenceMerge.tla` | Sender-zero skip, same-sender merge, distinct-sender append, identical duplicate acceptance, and conflicting duplicate rejection. | `TestSenderPreservingEvidenceValidationAndMergeContracts`, `TestCodecRejectsMalformedSenderEvidenceWireFrames`. |
| `tla/EPaxosTryPreAcceptRetry.tla` | Logical retry checks for current, stale, and unrelated pending evidence. | `TestTryPreAcceptTimerDropsStaleEvidenceChecksBeforeRetry`. |
| `tla/EPaxosRevisited.tla` | TOQ envelopes, delayed assignment, pending decision blocking, receiver timing, and dependency chain pruning. | `epaxos/toq_test.go`, `epaxos/revisited_test.go`, `epaxos/node.go`. |
| `tla/TOQClockDiscipline.tla` | Finite bounded-skew/bounded-delay contract for `ProcessAt`. | `Config.TOQClock`, `Config.TOQOneWayDelay`, `TestTOQThreeNodeFastCommitUsesOptimizedQuorumWithCoveringAttrs`. |
| `tla/ReadyAdvance.tla` | Durable Ready prefix acknowledgement and retry obligations. | `epaxos/ready_durability_test.go`, `tests/refinementtrace`. |
| `tla/EPaxosRollbackAllocation.tla` | Learned local reference advances allocation after rollback and preserves learned-before-fresh apply order. | `TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit`, rollback simulation tests. |
| `tla/EPaxosConfigBarrier.tla` | Pending configuration barriers, dependency inclusion, sequence elevation, and barrier clearing. | `epaxos/config_change_ordering_test.go`. |
| `tla/EPaxosConfigTransition.tla` | One add-voter transition with old and new pinned instance domains. | Configuration transition tests. |
| `tla/EPaxosConfigRemoveTransition.tla` | One remove-voter transition with old in-flight voter retention and new-instance exclusion. | `TestRemoveVoterConfChangeKeepsOldInFlightInstancePinned`. |
| `tla/EPaxosConfigChainTransition.tla` | One add-then-remove chain with old, intermediate, and final voter domains. | Configuration-chain tests. |
| `tla/EPaxosConfigReplay.tla` | Finite durable add/remove replay and pending configuration state. | `epaxos/config_change_ordering_test.go`, TOQ replay tests. |
| `tla/EPaxosConfigRecovery.tla` | Old pinned instance recovery after voter removal. | `TestOldConfigRecoveryUsesPinnedVotersAfterRemoval`. |
| `tla/EPaxosConfigRecoveryDedup.tla` | Lost and duplicate response de-duplication before a distinct old quorum vote. | Configuration recovery de-duplication tests. |
| `tla/EPaxosConfigAddRecovery.tla` | Old pinned recovery after voter addition; new voter responses do not enter the old quorum. | `TestOldConfigRecoveryUsesPinnedVotersAfterAddition`. |
| `tla/EPaxosConfigRecoveryRetry.tla` | Old-config prepare/accept retry targets and dependency widths after removal or addition. | Configuration recovery retry tests. |
| `tla/EPaxosConfigRecoveryLostResponseRetry.tla` | One explicit lost prepare and accept response before deterministic retry. | Lost-response recovery retry tests. |
| `tla/EPaxosConfigChainRecovery.tla` | Mid-chain recovery pinned to Conf2 after add then remove. | `TestOldConfigRecoveryUsesPinnedMidChainVotersAfterAddThenRemove`. |
| `tla/EPaxosConfigChainRecoveryLostResponseRetry.tla` | Mid-chain lost-response retry with Conf2 voter and dependency domains. | Mid-chain lost-response retry tests. |
| `tla/EPaxosConfigTransitionRetry.tla` | Normal local-owner old-config retry after removal or addition. | Configuration transition retry tests. |
| `tla/EPaxosConfigTransitionLostResponseRetry.tla` | Normal local-owner lost-response retry before old-quorum completion. | Configuration transition lost-response tests. |
| `tla/EPaxosConfigTransitionDedup.tla` | Owner vote separation and remote response de-duplication. | Configuration transition de-duplication tests. |
| `tla/EPaxosTimingDomain.tla` | Separation of untimed, logical, and TOQ timing domains at Ready/restart boundaries. | `epaxos/toq_test.go`, `tests/refinementtrace`. |
| `tla/EPaxosRawNodeRefinement.tla` | Bounded normal, recovery, TOQ, and configuration workflows with durability, Ready, evidence, execution, and branch invariants. | `tests/refinementtrace`, exported-method inventory. |
| `tla/EPaxosInductiveProofs.tla` | Parametric safety and `AbstractHistoryRefinement` for its restricted `ConcreteNext`. | Formal source only; no Go action bridge is checked. |
| `tla/EPaxosVoterBootstrap.tla` | Model-only bootstrap seal, fence, activation, abort, crash-prefix, and fairness obligations. | No corresponding `RawNode` semantic trace; explicitly excluded. |
| `tla/KVTimestampStaleness.tla` and `tla/KVOmissionRecovery.tla` | Example KV timestamp and omission/recovery slices. | `examples/kv`, Jepsen checker tests. |

The fast gate is executable through `bash tests/tla_model_check_fast.sh` and covers 14 named finite jobs. The larger `bash tests/tla_model_check.sh` profile is a manual finite suite. Neither is exhaustive.

## Paper-grounded claim map

| Claim area | Implementation correspondence | Status and boundary |
| --- | --- | --- |
| Fast quorum | `epaxos/quorum.go` uses the optimized EPaxos threshold for odd `N=2F+1` sizes and conservative thresholds for even sizes. Fast commit also requires matching response attributes and committed-prefix evidence. | Implemented for the stated odd-size path; even sizes are intentionally conservative; arbitrary recovery histories remain outside the claim. |
| Recovery | `epaxos/node.go` implements prepare priority, durable record ballots, TryPreAccept branches, Accept-Deps evidence, stale-ballot handling, and no-op recovery. | Implemented in the tested finite and deterministic envelope; unbounded recovery correspondence remains open. |
| Revisited TOQ | `Config.TOQ` persists `TOQPending` and `ProcessAt`, sends flagged attributes, delays originator assignment, and blocks decisions while pending. | Core behavior is implemented; clock synchronization, delay measurement, and sync-group operation remain embedder responsibilities. |
| Accept-Deps evidence | `Message`, codec, checksum, storage, and node paths preserve chosen `Seq`/`Deps` separately from recovery-only aggregate and sender evidence. | Finite 3/5/7 optimized-recovery and sender-merge slices pass; arbitrary evidence histories remain outside the claim. |
| Chain pruning | `sparse_progress.go` removes dependencies proven ordered by a higher-sequence dependency during SCC readiness. | Implemented and covered by Revisited tests and `tla/EPaxosRevisited.tla`. |

References: [EPaxos SOSP 2013](https://sigops.org/s/conferences/sosp/2013/papers/p358-moraru.pdf), [EPaxos technical report CMU-PDL-13-111](https://www.pdl.cmu.edu/PDL-FTP/associated/CMU-PDL-13-111.pdf), and [EPaxos Revisited NSDI 2021](https://www.usenix.org/system/files/nsdi21-tollman.pdf).

## Comparison with the attached original model

| Area | Attached original model | Current repository |
| --- | --- | --- |
| Fast quorum | Uses the optimized odd-size quorum formula. | Go and TLA use optimized thresholds for odd supported sizes and conservative thresholds for even sizes; `TestQuorumTables` checks the table. |
| Slow quorum | Majority quorum containing the leader. | Same majority rule, with historical voter configuration pinned by `InstanceRef.Conf`. |
| Messages | Set-valued sent-message state collapses duplicates structurally. | Real messages validate type, sender, recipient, reference, dependency width, and checksum; response votes de-duplicate by sender. |
| Initial dependencies | Leader log records are broadly included. | Go computes local conflict and configuration-barrier attributes; non-conflicting local records are not omnisciently added. |
| Receive dependencies | The original merges proposer dependencies with local log state. | `handlePreAccept` recomputes local attributes and merges only the defined legacy path; TOQ receivers compute at `ProcessAt`. |
| Fast path | Matching attributes in an optimized quorum plus dependency evidence. | `handlePreAcceptResp` requires the implemented quorum, current evidence, and committed-prefix coverage; divergent attributes use slow accept. |
| Recovery | Prepare chooses committed, accepted, preaccepted, try-preaccept, restart, or no-op branches. | Go implements the same branch family with durable ballots, current-ballot response gating, conflict metadata, evidence query, and deterministic retry timers. |
| Execution | The attached model names execution state but does not provide the full concrete execution action. | Go builds SCCs, waits for missing dependencies, blocks unresolved conflicts, and emits deterministic Ready commands. `tla/EPaxos.tla` covers minimal finite components. |
| Safety theorem | Type and per-instance consistency properties over the original state. | Repository models add dependency closure, quorum evidence, recovery, TOQ, chain pruning, and Ready durability invariants, all with explicit finite limits. |

## Implementation correspondence matrix

| Concern | Go anchor | Direct model/test anchor |
| --- | --- | --- |
| Proposal, conflict attributes, and TOQ assignment | `epaxos/node.go`: `propose`, `computeAttrsAt`, `nextTOQProcessAt`, `handleLocalTOQPreAccept`. | `tla/EPaxos.tla`, `tla/EPaxosRevisited.tla`, `tla/EPaxosTimingDomain.tla`, `epaxos/toq_test.go`. |
| PreAccept receive and durable response | `epaxos/node.go`: `handlePreAccept`, deferred TOQ processing, `FastPathEligible`, committed-prefix evidence. | `tla/EPaxos.tla`, `tla/EPaxosRevisited.tla`, `epaxos/toq_test.go`. |
| Fast/slow response split | `epaxos/node.go`: response de-duplication, fast quorum, FP-deps evidence, slow accept, current-ballot gating. | `tla/EPaxosResponses.tla`, `tla/Quorum.tla`, optimized and response tests. |
| AcceptEvidence validation | `epaxos/message.go`: sender-zero skip, same-sender merge, duplicate acceptance, conflicting duplicate rejection. | `tla/EPaxosAcceptEvidenceMerge.tla`, protocol coverage tests. |
| Prepare and recovery | `epaxos/node.go`: `startPrepare`, `handlePrepareResp`, `startAccept`, `handleAcceptResp`, deterministic recovery coordinator. | `tla/EPaxosRecovery.tla`, `tla/EPaxosResponses.tla`, recovery tests. |
| Stale committed conflict | `epaxos/node.go`: evidence query, authorized ignore resend, fail-closed slow accept, same-config conflict dependency. | `tla/EPaxosEvidenceQuery.tla`, `tla/EPaxosEvidenceStaleness.tla`, TryPreAccept tests. |
| Historical configurations | `epaxos/node.go`: `confFor`, `votersForConf`, `slowQuorumForConf`, `broadcast`, replay and pending barriers. | Configuration transition/recovery/retry models and tests listed above. |
| SCC execution and pruning | `epaxos/sparse_progress.go`: execution view, components, readiness, unresolved conflict checks, deterministic order. | `tla/EPaxos.tla`, `tla/EPaxosRevisited.tla`, `epaxos/revisited_test.go`. |
| Ready durability and ownership | `epaxos/node.go`: freeze, `ReadyInto`, exact-prefix `Advance`, disjoint next-batch storage. | `tla/ReadyAdvance.tla`, `tests/refinementtrace`, ready durability tests. |
| Rollback allocation | `epaxos/node.go`: `observeInstanceRef`, `nextLocalRef`, proposal stale-next skip. | `tla/EPaxosRollbackAllocation.tla`, rollback tests. |
| Wire and storage integrity | `epaxos/codec.go`, `epaxos/checksum.go`, storage interfaces, malformed-input tests. | Checksum/codec tests; no TLA claim for codec allocation or storage engine internals. |

## Executable trace boundary

`tests/refinementtrace` is an executable bounded contract over real `RawNode` values. It covers normal proposal/commit, recovery, TOQ, and configuration workflows. The validator checks:

- state snapshots are well formed before and after each transition;
- fail and drop transitions stutter without durable or application effects;
- every final Ready write is persisted before its acknowledgement;
- `Advance` accepts only the exact outstanding prefix and does not regress durability;
- execution and observations are monotonic and stutter when no protocol transition commits work; and
- every exported `RawNode` method is present in the explicit trace inventory.

The inventory and contracts prevent silent API drift. They do not replay TLC actions, prove all possible method sequences, or prove arbitrary Go refinement.

## Verification snapshot

- `go test ./... -count=1` passes.
- `go test -race ./tests/lifecyclecollector -count=1` passes.
- `go test -race ./tests/refinementtrace -count=1` passes.
- `bash tests/tla_model_check_fast.sh` passes all 14 configured jobs.
- `tests/go_coverage.sh` passes the scoped production-package thresholds: `epaxos` 89.5% against 85.0% minimum and `examples/kv` 91.7% against 90.0% minimum. Root behavior and race suites exercise verification collectors.
- `tlapm` is not installed in the local environment, and no TLAPS proof artifact is part of the checked gate.

## Formal and release boundaries

- `tla/EPaxosInductiveProofs.tla` is restricted to its module-level `ConcreteNext`; it excludes codec, allocation, Ready ownership, SCC execution, TOQ timing, the implementation-shaped RawNode model, and arbitrary Go executions.
- `tla/EPaxosRawNodeRefinement.tla` is finite and implementation-shaped; its TLC property is not an unbounded theorem.
- `tla/EPaxosVoterBootstrap.tla` is model-only and absent from the RawNode semantic trace.
- Deterministic simulation and loopback Jepsen histories do not represent real-network behavior.
- Certified protocol-state compaction and late-message/incarnation fencing remain open in `RELEASE_SCOPE.md`.

## Release decision

The correspondence evidence supports bounded implementation claims only. The authoritative decision is **No-go.** while the single formal release row remains open and the aggregate verification gates are green.
