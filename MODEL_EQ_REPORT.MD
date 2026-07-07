# Model Correspondence and Scope Report

This report maps the current finite TLA+ safety model to the current Go implementation and names implementation areas that are verified outside the model.

## Mapping

| Model anchor | Implementation anchor | Evidence |
| --- | --- | --- |
| `tla/EPaxos.tla:32` TLA Init | `epaxos/node.go:98` Go node construction | Initialization sets empty instance state before transport steps; Go also restores durable state and configuration history before accepting input. |
| `tla/EPaxos.tla:9` TLA Conflicts | `epaxos/types.go:150` Go command conflict predicate and `examples/kv/kv.go:368` KV transaction command construction | The model configures command membership in conflict-key sets; Go conflicts commands by shared conflict-key bytes, and KV transactions deduplicate EPAXOS conflict keys while preserving every payload operation in order. |
| `tla/EPaxos.tla:18` TLA SafeDeps | `epaxos/node.go:729` Go attribute computation | Both model and implementation require known conflicting commands to appear in dependency attributes. |
| `tla/EPaxos.tla:42` TLA PreAccept | `epaxos/node.go:257` Go proposal path | Local proposal computes attributes, persists pre-accepted records, indexes conflicts, and emits pre-accept messages. |
| `tla/EPaxos.tla:42` TLA PreAccept receive | `epaxos/node.go:422` Go pre-accept handler | Remote pre-accept merges local conflicts with proposer attributes and stores the pre-accepted value. |
| `tla/EPaxos.tla:54` TLA Accept | `epaxos/node.go:478` Go accept handler | Accept stores slow-path attributes unless the instance is already committed or promised higher. |
| `tla/EPaxos.tla:63` TLA Commit | `epaxos/node.go:522` Go commit handler | Commit stores final value/attributes and invokes dependency-closed execution. |
| `tla/EPaxos.tla:69` TLA Prepare | `epaxos/node.go:535` Go prepare handler | Prepare persists higher promises and returns local accepted/committed state for recovery; TLA abstracts this as the ballot-raising recovery step. |
| Not represented in current TLA safety model | `epaxos/node.go:561` Go prepare response handler | Recovery response quorum handling is verified by Go tests, but the current TLA model abstracts away recovery quorum collection. |
| `tla/EPaxos.tla:76` TLA Execute | `epaxos/node.go:836` Go executor and `epaxos/node.go:367` Ready acknowledgement | Execution collapses committed dependency SCCs; user commands are emitted through `Ready.Committed`, and durable `StatusExecuted` records are emitted only after `Advance` acknowledges application. |
| `tla/EPaxos.tla:96` TLA DependencyClosure | `epaxos/node.go:936` Go component readiness | Component readiness requires outside committed dependencies to be executed before application emission. |
| `tla/EPaxos.tla:98` TLA ConflictOrder | `epaxos/types.go:150` Go command conflict predicate and `epaxos/node.go:985` Go dependency references | Both require conflicting executed commands to be ordered by dependencies; Go interprets dependency vectors as known per-replica prefixes through configuration history. |
| `tla/EPaxos.tla:83` TLA Tick | `epaxos/node.go:237` Go logical tick | Tick advances logical time and dispatches logical timers; no wall-clock API is used in the core. |
| Not represented in current TLA safety model | `epaxos/checksum.go:65` Go record checksum | Durable record checksum coverage is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/checksum.go:84` Go message checksum | Transport checksum coverage is implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/codec.go:41` Go decoder | Wire-decoder input tolerance and error-path destination cleanup are implementation validation rather than modeled state. |
| Not represented in current TLA safety model | `epaxos/storage.go:9` Go storage interface | Storage virtualization is an implementation boundary used by deterministic tests rather than modeled state. |
| `tla/Quorum.tla:9` and `tla/Quorum.tla:55` TLA quorum formulas/intersections | `epaxos/quorum.go:15` Go quorum calculation | Both define supported cluster sizes one through seven, majority slow quorum `n/2 + 1`, conservative fast quorum `n - ((n - 1) / 4)`, explicit expected tables, and quorum-intersection invariants checked by TLC and Go tests. |
| `tla/EPaxosKVConflict.cfg:11` selective command set | `examples/kv/kv.go:368` Go transaction conflict-key construction | The KV conflict TLC config models independent single-key commands and a transaction-like command that conflicts with both keys; atomic multi-key apply and duplicate-key payload ordering remain implementation-tested at the example storage layer. |
| Not represented in current TLA safety model | `examples/kv/kv.go:305` Go scan implementation | Advanced scan ordering, reverse ordering, timestamp collision behavior, and Pebble durability are example storage semantics verified by Go tests rather than by the current TLA state. |
| Not represented in current TLA safety model | `examples/kv/cmd/kvnode/main.go:285` transport fault control, `examples/kv/cmd/kvnode/main.go:311` storage fault control, `jepsen/src/moreconsensus/epaxos_test.clj:196` transport nemesis, and `jepsen/src/moreconsensus/epaxos_test.clj:224` storage nemesis | Local transport partition and storage-unavailable controls are external validation mechanisms for the KV example and Jepsen workload rather than modeled state. |

## Current verification evidence

- Observed `tests/ci.sh` passing after the storage-fault hardening changes. The gate ran root Go tests, root coverage at 100.0%, example Go tests, example coverage at 100.0%, tagged kvnode tests, root/example/kvnode race checks, TLC model checks, local Jepsen restart, transport, and storage profiles, and the repository text audit.
- Observed `go test ./epaxos` passing with deterministic randomized simulation coverage for three- and five-node clusters, duplicate/drop/reorder delivery, logical ticks, restarts, dependency-vector prefix execution ordering, Ready/Advance durability, allocation/pool ownership assertions, decoder fuzz seeds, and decoder error cleanup.
- Observed `go test ./examples/kv` and `go test -tags kvnode ./examples/kv/cmd/kvnode` passing with KV duplicate-key transaction payload-order semantics, deduplicated EPAXOS conflict keys, post-Advance executed-record persistence, negative scan-limit rejection, transport fault-route coverage, and storage fault-route coverage that rejects work before RawNode progress.
- Observed `go test ./epaxos -coverprofile=coverage.out -count=1` reporting 100.0% statement coverage and `go tool cover -func=coverage.out` reporting `Advance` at 100.0% after the executed-record acknowledgement cap test.
- Observed `tests/tla_model_check.sh` running TLC for `tla/EPaxos.cfg` with 11162 generated states and 2002 distinct states, `tla/EPaxosKVConflict.cfg` with 3042650 generated states and 166034 distinct states, `tla/EPaxosThreeReplica.cfg` with 1104121 generated states and 123821 distinct states, and `tla/Quorum.cfg` with 2 generated states and 1 distinct state; all completed with no invariant violation.
- Observed targeted `lein test moreconsensus.epaxos-test-test` from `jepsen` passing 26 tests with 67 assertions for register delete normalization, indeterminate mutation response classification, transaction body encoding, grouped reads, scan shape checking, restart fault selection, transport fault selection, storage fault selection, client/nemesis routing, restart nemesis behavior, transport nemesis control requests, and storage nemesis control requests.
- Observed `tests/ci.sh` local transport profile reporting register `:linearizable {:valid? true}`, scan shape `:scan-shape {:valid? true, :checked 11, :bad-count 0}`, transaction `:txn-atomic {:valid? true, :checked 9, :bad-count 0}`, and overall `:valid? true`.
- Observed `tests/ci.sh` local restart profile reporting register `:linearizable {:valid? true}`, scan shape `:scan-shape {:valid? true, :checked 6, :bad-count 0}`, transaction `:txn-atomic {:valid? true, :checked 2, :bad-count 0}`, and overall `:valid? true`.
- Observed `tests/ci.sh` local storage profile reporting register `:linearizable {:valid? true}`, scan shape `:scan-shape {:valid? true, :checked 5, :bad-count 0}`, transaction `:txn-atomic {:valid? true, :checked 5, :bad-count 0}`, and overall `:valid? true`.
- Observed repository text audit with no disallowed external-project name, scaffolding marker, or wall-clock API matches.

## Open correspondence risks

- The TLA+ model is finite-bounded for TLC, so it demonstrates correspondence for the configured state space rather than unbounded proof.
- Checksum, wire-decoder tolerance, storage virtualization, recovery response collection, transport fault control, storage fault control, and example KV apply/scan storage semantics are verified by implementation and Jepsen tests but outside the current TLA safety state.
- Local Jepsen now covers loopback process restart, transport partition, and storage-unavailable profiles, but it still lacks SSH-managed multi-host deployment, destructive disk nemeses, and long-duration operational coverage.
