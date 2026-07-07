#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASE_PORT="${JEPSEN_BASE_PORT:-19080}"
BIN="${TMPDIR:-/tmp}/moreconsensus-kvnode-$$"
DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/moreconsensus-jepsen.XXXXXX")"
PIDS=""

cleanup() {
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

peer_arg=""
node_arg=""
for id in 1 2 3; do
  port=$((BASE_PORT + id))
  url="http://127.0.0.1:${port}"
  if [[ -n "$peer_arg" ]]; then
    peer_arg="${peer_arg},"
  fi
  peer_arg="${peer_arg}${id}=${url}"
  if [[ -n "$node_arg" ]]; then
    node_arg="${node_arg},"
  fi
  node_arg="${node_arg}127.0.0.1:${port}"
done

for id in 1 2 3; do
  port=$((BASE_PORT + id))
  "$BIN" -id "$id" -listen ":${port}" -data "$DATA_DIR/node-${id}" -peers "$peer_arg" >"$DATA_DIR/node-${id}.log" 2>&1 &
  PIDS="$PIDS $!"
done

for id in 1 2 3; do
  port=$((BASE_PORT + id))
  ready=0
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
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
(
  cd jepsen
  lein run test --no-ssh --nodes "$node_arg" --time-limit 5 --concurrency 3
)
