#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

expect_fail() {
  local name="$1"
  shift
  local output
  set +e
  output="$("$@" 2>&1)"
  local status=$?
  set -e
  if [[ $status -eq 0 ]]; then
    echo "remote preflight unexpectedly passed: $name" >&2
    echo "$output" >&2
    exit 1
  fi
  printf 'preflight_fail[%s]=%s\n' "$name" "$status"
}

expect_ok() {
  local name="$1"
  shift
  local output
  output="$("$@" 2>&1)"
  if [[ "$output" != *"remote_preflight=ok"* ]]; then
    echo "remote preflight did not report ok: $name" >&2
    echo "$output" >&2
    exit 1
  fi
  printf 'preflight_ok[%s]=1\n' "$name"
}

base_env=(
  JEPSEN_NODES=alpha,bravo,charlie
  JEPSEN_REMOTE_PREFLIGHT_ONLY=yes
  JEPSEN_REMOTE_DURATION=30
  JEPSEN_REMOTE_CONCURRENCY=3n
)

expect_fail missing-destructive-confirm \
  env "${base_env[@]}" JEPSEN_REMOTE_FAULTS=destructive-storage bash tests/jepsen_remote.sh

expect_fail unsafe-remote-dir \
  env "${base_env[@]}" JEPSEN_REMOTE_FAULTS=destructive-storage JEPSEN_REMOTE_CONFIRM_DESTRUCTIVE=yes MORECONSENSUS_KVNODE_REMOTE_DIR=/ bash tests/jepsen_remote.sh

expect_ok destructive-confirmed-safe-dir \
  env "${base_env[@]}" JEPSEN_REMOTE_FAULTS=destructive-storage JEPSEN_REMOTE_CONFIRM_DESTRUCTIVE=yes MORECONSENSUS_KVNODE_REMOTE_DIR=/tmp/moreconsensus-jepsen/audit bash tests/jepsen_remote.sh

expect_fail missing-wall-clock-confirm \
  env "${base_env[@]}" JEPSEN_REMOTE_FAULTS=wall-clock-skew bash tests/jepsen_remote.sh

expect_ok wall-clock-confirmed-safe-dir \
  env "${base_env[@]}" JEPSEN_REMOTE_FAULTS=wall-clock-skew JEPSEN_REMOTE_CONFIRM_WALL_CLOCK_SKEW=yes MORECONSENSUS_KVNODE_REMOTE_DIR=/tmp/moreconsensus-jepsen/audit bash tests/jepsen_remote.sh
