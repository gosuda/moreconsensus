# moreconsensus

`moreconsensus` is a Go library for building replicated services with Egalitarian Paxos (EPaxos). The public API follows the shape of etcd raft: applications drive a deterministic `RawNode`, persist `Ready` records, send `Ready` messages, apply committed commands, and then call `Advance` with the acknowledged `Ready` prefix.

## Core features

- EPaxos fast path, slow accept path, commit broadcast, and recovery hooks.
- Deterministic logical ticks for all timing behavior.
- Caller-owned transport and storage virtualization.
- Pool-aware message and command helpers.
- Zero-copy command payload/key decode, reusable decode metadata buffers, and explicit proposal ownership options.
- BLAKE3 checksums through `github.com/zeebo/blake3` as the only intentional main-module runtime dependency.
- Deterministic simulation tests for cluster sizes 1 through 7.
- A Pebble-backed distributed key-value example in its own Go module, including atomic Pebble persistence for EPaxOS records and applied key-value writes.
- The example HTTP service is built explicitly with `go build -tags kvnode ./cmd/kvnode` from `examples/kv`.
- Repository verification gates run from `tests/ci.sh`, including local Jepsen restart, transport-partition, storage-unavailable, and destructive-storage profiles, and are wired into GitHub Actions CI. SSH-managed multi-host Jepsen profiles are available as opt-in operational gates.

## Documentation

- [EPAXOS.MD](EPAXOS.MD) describes the implemented algorithm in detail.
- [MODEL_EQ_REPORT.MD](MODEL_EQ_REPORT.MD) describes the current TLA+ model correspondence and implementation verification scope.
- [tla/EPaxos.tla](tla/EPaxos.tla), [tla/EPaxosResponses.tla](tla/EPaxosResponses.tla), [tla/ReadyAdvance.tla](tla/ReadyAdvance.tla), and [tla/Quorum.tla](tla/Quorum.tla) contain the finite executable formal models checked by CI.
- [examples/kv](examples/kv) contains the Pebble/MyRocks-style key-value example.
- [jepsen](jepsen) contains the Jepsen workload harness for external validation.
- [tests](tests) contains the repository verification scripts used by CI.

## Operational validation

- `JEPSEN_LOCAL_FAULTS=destructive-storage bash tests/jepsen_local.sh` runs the loopback KV cluster while each selected process is stopped, its Pebble directory is moved aside, and the original directory is restored before the process rejoins.
- `JEPSEN_NODES=n1,n2,n3 bash tests/jepsen_remote.sh` builds `kvnode`, uploads it to each SSH-managed host, starts one node per host, and runs the destructive-storage profile by default. `JEPSEN_REMOTE_FAULTS=restart|transport|storage|destructive-storage`, `JEPSEN_REMOTE_DURATION`, `JEPSEN_REMOTE_CONCURRENCY`, `MORECONSENSUS_KVNODE_HTTP_PORT`, and `MORECONSENSUS_KVNODE_REMOTE_DIR` tune that run.

## Module layout

The main module is `gosuda.org/moreconsensus`. Example dependencies are isolated in example modules and included by the committed Go workspace.
