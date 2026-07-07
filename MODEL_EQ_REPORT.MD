# Model Correspondence and Scope Report

This report maps the current finite TLA+ safety model to the current Go implementation and names implementation areas that are verified outside the model.

## Mapping

| Model anchor | Implementation anchor | Evidence |
| --- | --- | --- |
| `tla/EPaxos.tla:32` TLA Init | `epaxos/node.go:98` Go node construction | Initialization sets empty instance state before transport steps; Go also restores durable state and configuration history before accepting input. |
| `tla/EPaxos.tla:9` TLA Conflicts | `epaxos/types.go:147` Go command conflict predicate and `examples/kv/kv.go:319` KV transaction command construction | The model configures command membership in conflict-key sets, distinguishing independent `a` and `b` commands from transaction-like `ab`; Go conflicts commands by shared conflict-key bytes, and KV transactions add every operation key as an EPaxOS conflict key. |
| `tla/EPaxos.tla:18` TLA SafeDeps | `epaxos/node.go:706` Go attribute computation | Both model and implementation require known conflicting commands to appear in dependency attributes. |
| `tla/EPaxos.tla:42` TLA PreAccept | `epaxos/node.go:257` Go proposal path | Local proposal computes attributes, persists pre-accepted records, indexes conflicts, and emits pre-accept messages. |
| `tla/EPaxos.tla:42` TLA PreAccept receive | `epaxos/node.go:405` Go pre-accept handler | Remote pre-accept merges local conflicts with proposer attributes and stores the pre-accepted value. |
| `tla/EPaxos.tla:54` TLA Accept | `epaxos/node.go:461` Go accept handler | Accept stores slow-path attributes unless the instance is already committed or promised higher. |
| `tla/EPaxos.tla:63` TLA Commit | `epaxos/node.go:505` Go commit handler | Commit stores final value/attributes and invokes dependency-closed execution. |
| `tla/EPaxos.tla:69` TLA Prepare | `epaxos/node.go:518` Go prepare handler | Prepare persists higher promises and returns local accepted/committed state for recovery; TLA abstracts this as the ballot-raising recovery step. |
| Not represented in current TLA safety model | `epaxos/node.go:544` Go prepare response handler | Recovery response quorum handling is verified by Go tests, but the current TLA model abstracts away recovery quorum collection. |
| `tla/EPaxos.tla:76` TLA Execute | `epaxos/node.go:813` Go executor | Execution collapses committed dependency SCCs, persists executed status, applies configuration commands, and emits application commands. |
| `tla/EPaxos.tla:96` TLA DependencyClosure | `epaxos/node.go:911` Go component readiness | Component readiness requires outside dependencies to be executed or absent before application emission. |
| `tla/EPaxos.tla:98` TLA ConflictOrder | `epaxos/types.go:147` Go command conflict predicate and `epaxos/node.go:960` Go dependency references | Both require conflicting executed commands to be ordered by dependencies; Go interprets dependency vectors through configuration history. |
| `tla/EPaxos.tla:83` TLA Tick | `epaxos/node.go:237` Go logical tick | Tick advances logical time and dispatches logical timers; no wall-clock API is used in the core. |
| Not represented in current TLA safety model | `epaxos/checksum.go:65` Go record checksum | Durable record checksum coverage is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/checksum.go:84` Go message checksum | Transport checksum coverage is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/codec.go:40` Go decoder | Wire-decoder input tolerance is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/storage.go:9` Go storage interface | Storage virtualization is an implementation boundary used by deterministic tests rather than modeled state. |
| `tla/Quorum.tla:9` TLA quorum table | `epaxos/quorum.go:15` Go quorum calculation | Both define supported cluster sizes one through seven, majority slow quorum `n/2 + 1`, and conservative fast quorum `n - ((n - 1) / 4)`, with explicit expected tables checked by TLC and Go tests. |
| `tla/EPaxosKVConflict.cfg:11` selective command set | `examples/kv/kv.go:319` Go transaction conflict-key construction | The KV conflict TLC config models independent single-key commands and a transaction-like command that conflicts with both keys; atomic multi-key apply remains implementation-tested at the example storage layer. |
| Not represented in current TLA safety model | `examples/kv/kv.go:256` Go scan implementation | Advanced scan ordering, reverse ordering, timestamp collision behavior, and Pebble durability are example storage semantics verified by Go tests rather than by the current TLA state. |

## Current verification evidence

- Observed `go test ./...` from the repository root passing for the main EPaxos module.
- Observed `go test -coverprofile=coverage.out ./...` plus `go tool cover -func=coverage.out` from the repository root reporting 100.0% statement coverage for the main EPaxos module.
- Observed `go test ./...` from `examples/kv` passing.
- Observed `go test -coverprofile=coverage.out ./...` from `examples/kv` plus `go tool cover -func=coverage.out` reporting 100.0% statement coverage for the example module.
- Observed `go test -tags kvnode ./cmd/kvnode` from `examples/kv` passing for the tagged HTTP service package.
- Observed `go test ./...` coverage including deterministic ordered simulation scheduling, cluster destruction and restart via `TestRestartAllRawNodesRetainsExecutedAndAppliesOnlyNewCommand`, remove-voter progress via `TestRemoveVoterConfChangeAllowsLaterProgress`, storage write retry via `TestWriteErrorKeepsReadyForRetry`, partition heal convergence via `TestFiveNodePartitionHealConverges`, equal-sequence execution ordering via `TestExecutionEqualSeqTieBreaksByRef`, inactive dependency filtering via `TestExecutionComponentsSkipInactiveDependencyRefs`, `MaxReadyMessages` batching, and logical-time optimization fast-wait behavior.
- Observed `go test ./epaxos -run 'TestCodecChecksumZeroCopy|TestRemainingReadyAndProposalBranches|Test.*Proposal.*Ownership|Test.*Inbound.*ZeroCopy'` covering command payload/key zero-copy decode aliasing, default proposal safe-copy ownership, `Config.ZeroCopyProposals` caller-slice retention through `Ready` records, and inbound `Step` safe-copy behavior for decoded transport buffers.
- Observed `go test ./...` coverage in `examples/kv` including transaction atomicity, malformed transaction rejection, transaction batch error propagation, transaction commit error propagation, apply-order visible-version selection, timestamp collision overwrite behavior, ApplyCommitted close/open durability, delete-tombstone scan suppression, Pebble close/open durability, and reverse scans returning the newest version for repeated keys.
- Observed `tests/tla_model_check.sh` running TLC for `tla/EPaxos.cfg` with 11162 generated states and 2002 distinct states, for `tla/EPaxosKVConflict.cfg` with 3042650 generated states and 166034 distinct states, and for `tla/Quorum.cfg` with 2 generated states and 1 distinct state; all completed with no invariant violation.
- Observed `lein test` from `jepsen` passing 4 tests with 10 assertions for transaction write body encoding, delete body encoding, grouped scan reads, and per-group transaction atomic checking; observed `tests/ci.sh` running a local three-node Jepsen workload against deployed `kvnode` processes with `lein run test --no-ssh --nodes 127.0.0.1:19081,127.0.0.1:19082,127.0.0.1:19083 --time-limit 5 --concurrency 3` reporting register `:linearizable {:valid? true}` and transaction `:txn-atomic {:valid? true, :checked 24, :bad-count 0}`.
- Observed repository text audit with no disallowed external-project name, scaffolding marker, or wall-clock API matches.

## Open correspondence risks

- The TLA+ model is finite-bounded for TLC, so it demonstrates correspondence for the configured state space rather than unbounded proof.
- Checksum, wire-decoder tolerance, storage virtualization, recovery response collection, and example KV apply/scan storage semantics are verified by implementation tests but outside the current TLA safety state.
- The local Jepsen run uses in-process loopback deployment without SSH-managed OS faults; its transaction coverage is a grouped atomic write/delete/scan workload rather than a broad application workload.
