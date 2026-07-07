# Model Equivalence Report

This report maps the current TLA+ specification to the current Go implementation. Line numbers are regenerated from the working tree that contains this file.

## Mapping

| Model anchor | Implementation anchor | Evidence |
| --- | --- | --- |
| `tla/EPaxos.tla:27` TLA Init | `epaxos/node.go:98` Go node construction | Initialization sets empty instance state before transport steps; Go also restores durable state and configuration history before accepting input. |
| `tla/EPaxos.tla:15` TLA SafeDeps | `epaxos/node.go:702` Go attribute computation | Both model and implementation require known conflicting commands to appear in dependency attributes. |
| `tla/EPaxos.tla:37` TLA PreAccept | `epaxos/node.go:257` Go proposal path | Local proposal computes attributes, persists pre-accepted records, indexes conflicts, and emits pre-accept messages. |
| `tla/EPaxos.tla:37` TLA PreAccept receive | `epaxos/node.go:401` Go pre-accept handler | Remote pre-accept merges local conflicts with proposer attributes and stores the pre-accepted value. |
| `tla/EPaxos.tla:49` TLA Accept | `epaxos/node.go:457` Go accept handler | Accept stores slow-path attributes unless the instance is already committed or promised higher. |
| `tla/EPaxos.tla:58` TLA Commit | `epaxos/node.go:501` Go commit handler | Commit stores final value/attributes and invokes dependency-closed execution. |
| `tla/EPaxos.tla:64` TLA Prepare | `epaxos/node.go:514` Go prepare handler | Prepare persists higher promises and returns local accepted/committed state for recovery; TLA abstracts this as the ballot-raising recovery step. |
| Not represented in current TLA safety model | `epaxos/node.go:540` Go prepare response handler | Recovery response quorum handling is verified by Go tests, but the current TLA model abstracts away recovery quorum collection. |
| `tla/EPaxos.tla:71` TLA Execute | `epaxos/node.go:802` Go executor | Execution collapses committed dependency SCCs, persists executed status, applies configuration commands, and emits application commands. |
| `tla/EPaxos.tla:91` TLA DependencyClosure | `epaxos/node.go:900` Go component readiness | Component readiness requires outside dependencies to be executed or absent before application emission. |
| `tla/EPaxos.tla:93` TLA ConflictOrder | `epaxos/node.go:949` Go dependency references | Both require conflicting executed commands to be ordered by dependencies; Go interprets dependency vectors through configuration history. |
| `tla/EPaxos.tla:78` TLA Tick | `epaxos/node.go:237` Go logical tick | Tick advances logical time and dispatches logical timers; no wall-clock API is used in the core. |
| Not represented in current TLA safety model | `epaxos/checksum.go:65` Go record checksum | Durable record checksum coverage is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/checksum.go:84` Go message checksum | Transport checksum coverage is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/codec.go:40` Go decoder | Wire-decoder input tolerance is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/storage.go:9` Go storage interface | Storage virtualization is an implementation boundary used by deterministic tests rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/quorum.go:15` Go quorum calculation | Quorum arithmetic for cluster sizes one through seven is verified by Go tests; the current TLA constants enumerate voters directly. |
| Not represented in current TLA safety model | `examples/kv/kv.go:102` Go transaction batch apply | Example KV transactions are opaque EPaxos commands at the model boundary; atomic multi-key apply is verified by Go tests rather than by the current TLA state. |
| Not represented in current TLA safety model | `examples/kv/kv.go:169` Go scan implementation | Advanced scan ordering, reverse ordering, timestamp collision behavior, and Pebble durability are example storage semantics verified by Go tests rather than by the current TLA state. |

## Current verification evidence

- Observed `go test ./...` from the repository root passing for the main EPaxos module.
- Observed `go test -coverprofile=coverage.out ./...` plus `go tool cover -func=coverage.out` from the repository root reporting 100.0% statement coverage for the main EPaxos module.
- Observed `go test ./...` from `examples/kv` passing.
- Observed `go test -coverprofile=coverage.out ./...` from `examples/kv` plus `go tool cover -func=coverage.out` reporting 100.0% statement coverage for the example module.
- Observed `go test -tags kvnode ./cmd/kvnode` from `examples/kv` passing for the tagged HTTP service package.
- Observed `go test ./...` coverage including cluster destruction and restart via `TestRestartAllRawNodesRetainsExecutedAndAppliesOnlyNewCommand`, equal-sequence execution ordering via `TestExecutionEqualSeqTieBreaksByRef`, and inactive dependency filtering via `TestExecutionComponentsSkipInactiveDependencyRefs`.
- Observed `go test ./...` coverage in `examples/kv` including transaction atomicity, malformed transaction rejection, transaction batch error propagation, timestamp collision overwrite behavior, Pebble close/open durability, and reverse scans returning the newest version for repeated keys.
- Observed TLC passing with `/opt/homebrew/opt/openjdk/bin/java -cp /tmp/tla2tools.jar tlc2.TLC -config tla/EPaxos.cfg tla/EPaxos.tla`, generating 11162 states, 2002 distinct states, and no invariant violation.
- Observed `lein test` from `jepsen` passing namespace and dependency loading, and observed `tests/ci.sh` running a local three-node Jepsen workload against deployed `kvnode` processes with `lein run test --no-ssh --nodes 127.0.0.1:19081,127.0.0.1:19082,127.0.0.1:19083 --time-limit 5 --concurrency 3` reporting `:valid? true` after successful writes and reads.
- Observed repository text audit with no disallowed external-project name, scaffolding marker, or wall-clock API matches.

## Open equivalence risks

- The TLA+ model is finite-bounded for TLC, so it demonstrates correspondence for the configured state space rather than unbounded proof.
- Checksum, wire-decoder tolerance, storage virtualization, quorum arithmetic, recovery response collection, and example KV storage semantics are verified by implementation tests but outside the current TLA safety state.
- The local Jepsen run uses in-process loopback deployment without SSH-managed OS faults, and its workload currently covers register-style successful writes and reads rather than application-level transaction internals.
