#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

usage() {
  cat <<'USAGE'
kvnode local capacity loopback drill (opt-in, bounded)

Status: local loopback evidence only. This script starts a disposable three-node
kvnode cluster, then runs tests/kvnode_capacity_envelope.sh against all three
client/admin listeners with PIDs and data directories supplied for resource
sampling. It does not assert production capacity, target-environment coverage,
or operator acceptance.
The generated metadata and summary retain release_claim=none-target-environment-capacity-results-still-required.

Required opt-in:
  KVNODE_LOCAL_CAPACITY_RUN=yes

Optional inputs:
  KVNODE_LOCAL_CAPACITY_BASE_PORT        Client port base. Nodes use base+1..base+3. Default: 25080
  KVNODE_LOCAL_CAPACITY_PEER_BASE_PORT   Peer port base. Nodes use base+1..base+3. Default: 25180
  KVNODE_LOCAL_CAPACITY_ADMIN_BASE_PORT  Admin port base. Nodes use base+1..base+3. Default: 25280
  KVNODE_LOCAL_CAPACITY_READY_ATTEMPTS   Readiness polling attempts. Default: 120
  KVNODE_LOCAL_CAPACITY_CURL_TIMEOUT     curl timeout seconds. Default: 5
  KVNODE_LOCAL_CAPACITY_OUT_DIR          Run/evidence directory. Default: <tmp>/kvnode-local-capacity-<timestamp>
  KVNODE_CAPACITY_REPORT                 Optional capacity report path passed through.
                                          Default: <out-dir>/capacity/capacity-report.env

Capacity harness defaults for this wrapper:
  KVNODE_CAPACITY_OPS_PER_PHASE          Default: 5
  KVNODE_CAPACITY_VALUE_BYTES            Default: 64,1024
  KVNODE_CAPACITY_SCAN_LIMITS            Default: 1,8
  KVNODE_CAPACITY_ENVIRONMENT_LABEL      Default: local-loopback
  KVNODE_CAPACITY_WORKLOAD_LABEL         Default: local-capacity-drill
  KVNODE_CAPACITY_REPORT                 Default: capacity/capacity-report.env

Example:
  KVNODE_LOCAL_CAPACITY_RUN=yes tests/kvnode_local_capacity_drill.sh
USAGE
}

fail() {
  echo "kvnode-local-capacity status=fail reason=$*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "missing-required-command-$1"
  fi
}

positive_int() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    fail "$name-must-be-positive-integer"
  fi
  if (( 10#$value <= 0 )); then
    fail "$name-must-be-positive-integer"
  fi
}

port_base() {
  local name="$1"
  local value="$2"
  positive_int "$name" "$value"
  if (( 10#$value > 65532 )); then
    fail "$name-too-large"
  fi
}

label_value() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    fail "$name-must-not-be-empty"
  fi
  if [[ "$value" == *$'\n'* || "$value" == *$'\r'* || "$value" == *"="* ]]; then
    fail "$name-must-be-single-line-without-equals"
  fi
  if (( ${#value} > 128 )); then
    fail "$name-too-long"
  fi
  printf '%s' "$value"
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "${KVNODE_LOCAL_CAPACITY_RUN:-}" != "yes" ]]; then
  usage >&2
  echo >&2
  echo "Refusing to run without KVNODE_LOCAL_CAPACITY_RUN=yes." >&2
  exit 2
fi

require_command go
require_command curl
require_command mktemp
require_command mkdir
require_command cat

BASE_PORT="${KVNODE_LOCAL_CAPACITY_BASE_PORT:-25080}"
PEER_BASE_PORT="${KVNODE_LOCAL_CAPACITY_PEER_BASE_PORT:-25180}"
ADMIN_BASE_PORT="${KVNODE_LOCAL_CAPACITY_ADMIN_BASE_PORT:-25280}"
READY_ATTEMPTS="${KVNODE_LOCAL_CAPACITY_READY_ATTEMPTS:-120}"
CURL_TIMEOUT_SECONDS="${KVNODE_LOCAL_CAPACITY_CURL_TIMEOUT:-5}"
ENVIRONMENT_LABEL="$(label_value KVNODE_CAPACITY_ENVIRONMENT_LABEL "${KVNODE_CAPACITY_ENVIRONMENT_LABEL:-local-loopback}")"
WORKLOAD_LABEL="$(label_value KVNODE_CAPACITY_WORKLOAD_LABEL "${KVNODE_CAPACITY_WORKLOAD_LABEL:-local-capacity-drill}")"

port_base KVNODE_LOCAL_CAPACITY_BASE_PORT "$BASE_PORT"
port_base KVNODE_LOCAL_CAPACITY_PEER_BASE_PORT "$PEER_BASE_PORT"
port_base KVNODE_LOCAL_CAPACITY_ADMIN_BASE_PORT "$ADMIN_BASE_PORT"
positive_int KVNODE_LOCAL_CAPACITY_READY_ATTEMPTS "$READY_ATTEMPTS"
positive_int KVNODE_LOCAL_CAPACITY_CURL_TIMEOUT "$CURL_TIMEOUT_SECONDS"

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${KVNODE_LOCAL_CAPACITY_OUT_DIR:-${TMPDIR:-/tmp}/kvnode-local-capacity-${run_id}}"
BIN_DIR="$RUN_DIR/bin"
DATA_DIR="$RUN_DIR/data"
LOG_DIR="$RUN_DIR/logs"
CAPACITY_DIR="$RUN_DIR/capacity"
CAPACITY_REPORT="${KVNODE_CAPACITY_REPORT:-$CAPACITY_DIR/capacity-report.env}"
mkdir -p "$BIN_DIR" "$DATA_DIR" "$LOG_DIR" "$CAPACITY_DIR"

BIN="$BIN_DIR/kvnode"
NODE_PIDS=("" "" "" "")

print_logs() {
  local log=""
  for log in "$LOG_DIR"/*.log; do
    if [[ -f "$log" ]]; then
      echo "kvnode-local-capacity log_begin file=$log" >&2
      cat "$log" >&2 || true
      echo "kvnode-local-capacity log_end file=$log" >&2
    fi
  done
}

cleanup() {
  local status="$?"
  local pid=""
  for pid in "${NODE_PIDS[@]}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${NODE_PIDS[@]}"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if (( status != 0 )); then
    print_logs
    echo "kvnode-local-capacity run_dir=$RUN_DIR" >&2
  fi
}
trap cleanup EXIT

client_url() {
  local id="$1"
  printf 'http://127.0.0.1:%d' "$((BASE_PORT + id))"
}

peer_url() {
  local id="$1"
  printf 'http://127.0.0.1:%d' "$((PEER_BASE_PORT + id))"
}

admin_url() {
  local id="$1"
  printf 'http://127.0.0.1:%d' "$((ADMIN_BASE_PORT + id))"
}

peer_arg() {
  printf '1=%s,2=%s,3=%s' "$(peer_url 1)" "$(peer_url 2)" "$(peer_url 3)"
}

http_status() {
  local url="$1"
  curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || printf '000'
}

http_responds() {
  local status=""
  status="$(http_status "$1")"
  case "$status" in
    2*|3*|4*) return 0 ;;
    *) return 1 ;;
  esac
}

start_node() {
  local id="$1"
  local log_file="$LOG_DIR/node-$id.log"
  mkdir -p "$DATA_DIR/node-$id"
  "$BIN" \
    -id "$id" \
    -listen ":$((BASE_PORT + id))" \
    -peer-listen ":$((PEER_BASE_PORT + id))" \
    -admin-listen ":$((ADMIN_BASE_PORT + id))" \
    -data "$DATA_DIR/node-$id" \
    -peers "$(peer_arg)" \
    -request-deadline-ms 5000 \
    -peer-deadline-ms 2000 \
    >"$log_file" 2>&1 &
  NODE_PIDS[$id]="$!"
}

wait_node_ready() {
  local id="$1"
  local attempt=""
  local pid="${NODE_PIDS[$id]}"
  for ((attempt=1; attempt<=READY_ATTEMPTS; attempt++)); do
    if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      fail "node-$id-exited-before-ready"
    fi
    if http_responds "$(admin_url "$id")/health" && http_responds "$(admin_url "$id")/readyz"; then
      return
    fi
    sleep 0.1
  done
  fail "node-$id-ready-timeout"
}

client_urls() {
  printf '%s,%s,%s' "$(client_url 1)" "$(client_url 2)" "$(client_url 3)"
}

admin_urls() {
  printf '%s,%s,%s' "$(admin_url 1)" "$(admin_url 2)" "$(admin_url 3)"
}

data_dirs() {
  printf '%s,%s,%s' "$DATA_DIR/node-1" "$DATA_DIR/node-2" "$DATA_DIR/node-3"
}

pids_csv() {
  printf '%s,%s,%s' "${NODE_PIDS[1]}" "${NODE_PIDS[2]}" "${NODE_PIDS[3]}"
}

cat > "$RUN_DIR/metadata.env" <<EOF
status=local-loopback-only
run_id=$run_id
run_dir=$RUN_DIR
capacity_dir=$CAPACITY_DIR
non_claim=not_target_environment_capacity_evidence
release_claim=none-target-environment-capacity-results-still-required
environment_label=$ENVIRONMENT_LABEL
workload_label=$WORKLOAD_LABEL
EOF

echo "kvnode-local-capacity phase=build"
(
  cd examples/kv
  go build -tags kvnode -o "$BIN" ./cmd/kvnode
)

echo "kvnode-local-capacity phase=start-cluster"
for id in 1 2 3; do
  start_node "$id"
done
for id in 1 2 3; do
  wait_node_ready "$id"
done

echo "kvnode-local-capacity phase=capacity-envelope"
KVNODE_CAPACITY_RUN=yes \
KVNODE_CLIENT_URLS="$(client_urls)" \
KVNODE_ADMIN_URLS="$(admin_urls)" \
KVNODE_DATA_DIRS="$(data_dirs)" \
KVNODE_PIDS="$(pids_csv)" \
KVNODE_PEER_COUNT=3 \
KVNODE_CAPACITY_ENVIRONMENT_LABEL="$ENVIRONMENT_LABEL" \
KVNODE_CAPACITY_WORKLOAD_LABEL="$WORKLOAD_LABEL" \
KVNODE_CAPACITY_OPS_PER_PHASE="${KVNODE_CAPACITY_OPS_PER_PHASE:-5}" \
KVNODE_CAPACITY_VALUE_BYTES="${KVNODE_CAPACITY_VALUE_BYTES:-64,1024}" \
KVNODE_CAPACITY_SCAN_LIMITS="${KVNODE_CAPACITY_SCAN_LIMITS:-1,8}" \
KVNODE_CAPACITY_OUT_DIR="$CAPACITY_DIR" \
KVNODE_CAPACITY_REPORT="$CAPACITY_REPORT" \
KVNODE_CAPACITY_TIMEOUT_SECONDS="$CURL_TIMEOUT_SECONDS" \
bash tests/kvnode_capacity_envelope.sh

cat > "$RUN_DIR/summary.txt" <<EOF
status=local-loopback-only
capacity_dir=$CAPACITY_DIR
capacity_report=$CAPACITY_REPORT
peer_count=3
environment_label=$ENVIRONMENT_LABEL
workload_label=$WORKLOAD_LABEL
release_claim=none-target-environment-capacity-results-still-required
EOF

cat "$RUN_DIR/summary.txt"
echo "kvnode-local-capacity status=pass run_dir=$RUN_DIR"
