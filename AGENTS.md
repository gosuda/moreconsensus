# Agent Instructions

Before completing relevant changes, run these non-overlapping gates; `tests/ci.sh`
covers race, integration, and formal checks, but not vet or lint:

```sh
go vet ./...
golangci-lint run ./...
(cd examples/kv && golangci-lint run ./...)
tests/ci.sh
```
