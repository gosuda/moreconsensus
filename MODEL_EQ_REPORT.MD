# Model Equivalence Report

This report maps the current TLA+ specification to the current Go implementation. Line numbers are regenerated from the working tree that contains this file.

## Mapping

| Model anchor | Implementation anchor | Evidence |
| --- | --- | --- |
| `tla/EPaxos.tla:25` TLA Init | `epaxos/node.go:98` Go node construction | Initialization builds quorum, configuration history, loaded instances, timers, and executor state before any transport step. |
| `tla/EPaxos.tla:13` TLA SafeDeps | `epaxos/node.go:698` Go attribute computation | Both model and implementation require known conflicting commands to appear in dependency attributes. |
| `tla/EPaxos.tla:35` TLA PreAccept | `epaxos/node.go:282` Go proposal path | Local proposal computes attributes, persists pre-accepted records, indexes conflicts, and emits pre-accept messages. |
| `tla/EPaxos.tla:35` TLA PreAccept receive | `epaxos/node.go:397` Go pre-accept handler | Remote pre-accept merges local conflicts with proposer attributes and stores the pre-accepted value. |
| `tla/EPaxos.tla:35` TLA Accept | `epaxos/node.go:453` Go accept handler | Accept stores slow-path attributes unless the instance is already committed or promised higher. |
| `tla/EPaxos.tla:56` TLA Commit | `epaxos/node.go:497` Go commit handler | Commit stores final value/attributes and invokes dependency-closed execution. |
| `tla/EPaxos.tla:62` TLA Prepare | `epaxos/node.go:510` Go prepare handler | Prepare persists higher promises and returns local accepted/committed state for recovery. |
| `tla/EPaxos.tla:62` TLA Prepare response | `epaxos/node.go:536` Go prepare response handler | Recovery chooses committed state if present, otherwise merges accepted/pre-accepted attributes before accept. |
| `tla/EPaxos.tla:68` TLA Execute | `epaxos/node.go:798` Go executor | Execution collapses committed dependency SCCs, persists executed status, applies configuration commands, and emits application commands. |
| `tla/EPaxos.tla:88` TLA DependencyClosure | `epaxos/node.go:896` Go component readiness | Component readiness requires outside dependencies to be executed or absent before application emission. |
| `tla/EPaxos.tla:90` TLA ConflictOrder | `epaxos/node.go:945` Go dependency references | Dependency vectors are interpreted through the configuration history for the instance id. |
| `tla/EPaxos.tla:75` TLA Tick | `epaxos/node.go:233` Go logical tick | Tick advances logical time and dispatches logical timers; no wall-clock API is used in the core. |
| `tla/EPaxos.tla:15` Checksum records | `epaxos/checksum.go:65` Go record checksum | Durable record fields are protected by canonical BLAKE3 checksums. |
| `tla/EPaxos.tla:21` Checksum messages | `epaxos/checksum.go:84` Go message checksum | Transport messages are protected by canonical BLAKE3 checksums. |
| `tla/EPaxos.tla:21` Wire tolerance | `epaxos/codec.go:40` Go decoder | Malformed wire input returns typed errors without panics. |
| `tla/EPaxos.tla:6` Storage virtualization | `epaxos/storage.go:9` Go storage interface | The model state is externalized through a deterministic storage interface in Go. |
| `tla/EPaxos.tla:4` Quorums | `epaxos/quorum.go:15` Go quorum calculation | Go computes majority slow quorums and conservative fast quorums for voter sets one through seven. |

## Current verification evidence

- Observed `go test ./...` from the repository root passing for the main EPaxos module.
- Observed `go test -coverprofile=coverage.out ./...` plus `go tool cover -func=coverage.out` reporting 98.2% after the latest protocol fixes; this is below the requested 100% completion gate and remains open.
- Observed `go test ./...` from `examples/kv` passing.
- Observed `go test -coverprofile=coverage.out ./...` from `examples/kv` plus `go tool cover -func=coverage.out` reporting 94.0%; this is below the requested 100% completion gate and remains open.
- Observed `go test -tags kvnode ./cmd/kvnode` from `examples/kv` building the tagged HTTP service package.
- Observed repository text audit with no forbidden project-name reference and no placeholder/skeleton/TODO/timing-timeout matches.
- TLC was not observed locally because no `tlc` executable is installed in this environment.
- Jepsen was not observed locally against a deployed cluster in this environment.
- Observed no local `tlc`, `lein`, or `clojure` executable; TLA and Jepsen syntax/runtime checks beyond parenthesis balance remain unmet in this environment.

## Open equivalence risks

- The TLA+ model is finite-bounded for TLC, but a local TLC run has not been observed.
- The Jepsen workload is wired to an HTTP KV endpoint, but a Jepsen run against a live multi-node deployment has not been observed.
- Main and example coverage are below the requested 100% full-codebase completion gate after the latest protocol fixes.
