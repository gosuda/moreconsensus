# Agent Instructions

Run these commands before completing relevant changes:

```sh
go test -race -count=1 ./...
go vet ./...
golangci-lint run ./...
tests/ci.sh
```

Per-task goals live in `.agent-tasks/<task-id>/GOALS.md`.
Richer project conventions are init's job.
