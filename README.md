# moreconsensus

`moreconsensus` is a Go library for building replicated services with Egalitarian Paxos (EPaxos). The public API follows the shape of etcd raft: applications drive a deterministic `RawNode`, persist `Ready` records, send `Ready` messages, apply committed commands, and then call `Advance`.

## Core features

- EPaxos fast path, slow accept path, commit broadcast, and recovery hooks.
- Deterministic logical ticks for all timing behavior.
- Caller-owned transport and storage virtualization.
- Pool-aware message and command helpers.
- Zero-copy command payload/key decode and explicit proposal ownership options.
- BLAKE3 checksums through `github.com/zeebo/blake3` as the only intentional main-module runtime dependency.
- Deterministic simulation tests for cluster sizes 1 through 7.
- A Pebble-backed distributed key-value example in its own Go module, including atomic Pebble persistence for EPaxOS records and applied key-value writes.
- The example HTTP service is built explicitly with `go build -tags kvnode ./cmd/kvnode` from `examples/kv`.
- Repository verification gates run from `tests/ci.sh` and are wired into GitHub Actions CI.

## Documentation

- [EPAXOS.MD](EPAXOS.MD) describes the implemented algorithm in detail.
- [MODEL_EQ_REPORT.MD](MODEL_EQ_REPORT.MD) describes the current TLA+ model correspondence and implementation verification scope.
- [tla/EPaxos.tla](tla/EPaxos.tla) contains the executable-style formal model.
- [examples/kv](examples/kv) contains the Pebble/MyRocks-style key-value example.
- [jepsen](jepsen) contains the Jepsen workload harness for external validation.
- [tests](tests) contains the repository verification scripts used by CI.

## Module layout

The main module is `gosuda.org/moreconsensus`. Example dependencies are isolated in example modules and included by the committed Go workspace.
