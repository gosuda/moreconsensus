#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASE_PORT="${JEPSEN_BASE_PORT:-19080}"
PEER_BASE_PORT="${JEPSEN_PEER_BASE_PORT:-19180}"
ADMIN_BASE_PORT="${JEPSEN_ADMIN_BASE_PORT:-19280}"
BIN="${TMPDIR:-/tmp}/moreconsensus-kvnode-$$"
DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/moreconsensus-jepsen.XXXXXX")"
PID_DIR="$DATA_DIR/pids"
PIDS=""

cleanup() {
  for pid_file in "$PID_DIR"/*.pid; do
    if [[ -f "$pid_file" ]]; then
      kill "$(cat "$pid_file")" >/dev/null 2>&1 || true
    fi
  done
  for pid in $PIDS; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in $PIDS; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  rm -rf "$DATA_DIR" "$BIN" jepsen/store jepsen/target jepsen/.lein-failures states
}
trap cleanup EXIT

(cd examples/kv && go build -tags kvnode -o "$BIN" ./cmd/kvnode)
mkdir -p "$PID_DIR"

peer_arg=""
node_arg=""
for id in 1 2 3; do
  client_port=$((BASE_PORT + id))
  peer_port=$((PEER_BASE_PORT + id))
  url="http://127.0.0.1:${peer_port}"
  if [[ -n "$peer_arg" ]]; then
    peer_arg="${peer_arg},"
  fi
  peer_arg="${peer_arg}${id}=${url}"
  if [[ -n "$node_arg" ]]; then
    node_arg="${node_arg},"
  fi
  node_arg="${node_arg}127.0.0.1:${client_port}"
done

start_node() {
  local id="$1"
  local client_port=$((BASE_PORT + id))
  local peer_port=$((PEER_BASE_PORT + id))
  local admin_port=$((ADMIN_BASE_PORT + id))
  "$BIN" -id "$id" -listen ":${client_port}" -peer-listen ":${peer_port}" -admin-listen ":${admin_port}" -data "$DATA_DIR/node-${id}" -peers "$peer_arg" >"$DATA_DIR/node-${id}.log" 2>&1 &
  local pid="$!"
  echo "$pid" >"$PID_DIR/node-${id}.pid"
  PIDS="$PIDS $pid"
}

for id in 1 2 3; do
  start_node "$id"
done

for id in 1 2 3; do
  admin_port=$((ADMIN_BASE_PORT + id))
  ready=0
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:${admin_port}/health" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.1
  done
  if [[ "$ready" != "1" ]]; then
    cat "$DATA_DIR"/node-*.log >&2 || true
    exit 1
  fi
done

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Djava.net.preferIPv4Stack=true"
export MORECONSENSUS_KVNODE_BIN="$BIN"
export MORECONSENSUS_KVNODE_DATA_DIR="$DATA_DIR"
export MORECONSENSUS_KVNODE_PEERS="$peer_arg"
export MORECONSENSUS_KVNODE_PID_DIR="$PID_DIR"
export MORECONSENSUS_KVNODE_BASE_PORT="$BASE_PORT"
export MORECONSENSUS_KVNODE_PEER_BASE_PORT="$PEER_BASE_PORT"
export MORECONSENSUS_KVNODE_ADMIN_BASE_PORT="$ADMIN_BASE_PORT"
export MORECONSENSUS_KVNODE_FAULTS="${JEPSEN_LOCAL_FAULTS:-restart}"
(
  cd jepsen
  lein run test --no-ssh --nodes "$node_arg" --time-limit 5 --concurrency 3
)
