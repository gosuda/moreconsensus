#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

require_min_cover() {
  local profile="$1"
  local minimum="$2"
  local label="$3"
  local total
  total="$(go tool cover -func="$profile" | awk '/^total:/ {print $3}')"
  if ! awk -v got="$total" -v want="$minimum" \
    'BEGIN { sub(/%$/, "", got); sub(/%$/, "", want); exit !(got + 0 >= want + 0) }'; then
    echo "coverage total for $label is $total, want at least $minimum" >&2
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

# Verification collectors under tests/ are exercised by the root behavior and
# race suites, but their optional platform/process branches are not production
# library coverage. Coverage thresholds therefore apply to the production core
# and the example service separately.
go test ./...
go test -coverprofile=coverage.out ./epaxos
require_min_cover coverage.out 85.0% "epaxos"

(
  cd examples/kv
  go test ./...
  go test -coverprofile=coverage.out ./...
  require_min_cover coverage.out 90.0% "examples/kv"
  go test -tags kvnode ./cmd/kvnode
)

run_race_stress
