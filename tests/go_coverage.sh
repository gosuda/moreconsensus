#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

require_full_cover() {
  local profile="$1"
  local total
  total="$(go tool cover -func="$profile" | awk '/^total:/ {print $3}')"
  if [[ "$total" != "100.0%" ]]; then
    echo "coverage total for $profile is $total, want 100.0%" >&2
    exit 1
  fi
}

run_race_stress() {
  go test -race -count=2 ./...
  (
    cd examples/kv
    go test -race -count=2 ./...
    go test -race -count=2 -tags kvnode ./cmd/kvnode
  )
}

go test ./...
go test -coverprofile=coverage.out ./...
require_full_cover coverage.out

(
  cd examples/kv
  go test ./...
  go test -coverprofile=coverage.out ./...
  require_full_cover coverage.out
  go test -tags kvnode ./cmd/kvnode
)

run_race_stress
