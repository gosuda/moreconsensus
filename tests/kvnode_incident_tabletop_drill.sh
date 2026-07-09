#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

usage() {
  cat <<'USAGE'
kvnode incident tabletop drill (local loopback harness only)

Status: local tabletop evidence only. This script starts a disposable three-node
kvnode cluster on loopback, exercises the storage-failure and transport-partition
runbook branches through admin test-fault endpoints, captures evidence files, and
clears every injected fault. It does not assert production readiness, target-
environment coverage, operator review, or disaster-recovery completion.

Required opt-in:
  KVNODE_INCIDENT_TABLETOP_RUN=yes

Optional inputs:
  KVNODE_INCIDENT_BASE_PORT        Client port base. Nodes use base+1..base+3.
                                   Default: 24080
  KVNODE_INCIDENT_PEER_BASE_PORT   Peer port base. Nodes use base+1..base+3.
                                   Default: 24180
  KVNODE_INCIDENT_ADMIN_BASE_PORT  Admin port base. Nodes use base+1..base+3.
                                   Default: 24280
  KVNODE_INCIDENT_READY_ATTEMPTS   Readiness polling attempts. Default: 120
  KVNODE_INCIDENT_CURL_TIMEOUT     curl per-request timeout seconds. Default: 5
  KVNODE_INCIDENT_OUT_DIR          Evidence/run directory. Default: <tmp>/kvnode-incident-tabletop-<timestamp>
  KVNODE_INCIDENT_TABLETOP_REPORT   Optional success report path. When set,
                                   writes a 0600 example/operator report.

Example:
  KVNODE_INCIDENT_TABLETOP_RUN=yes tests/kvnode_incident_tabletop_drill.sh
USAGE
}

fail() {
  echo "kvnode-incident-tabletop status=fail reason=$*" >&2
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

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "${KVNODE_INCIDENT_TABLETOP_RUN:-}" != "yes" ]]; then
  usage >&2
  echo >&2
  echo "Refusing to run without KVNODE_INCIDENT_TABLETOP_RUN=yes." >&2
  exit 2
fi

require_command go
require_command curl
require_command mktemp
require_command mkdir
require_command rm
require_command cat
require_command grep
require_command dirname
require_command chmod

BASE_PORT="${KVNODE_INCIDENT_BASE_PORT:-24080}"
PEER_BASE_PORT="${KVNODE_INCIDENT_PEER_BASE_PORT:-24180}"
ADMIN_BASE_PORT="${KVNODE_INCIDENT_ADMIN_BASE_PORT:-24280}"
READY_ATTEMPTS="${KVNODE_INCIDENT_READY_ATTEMPTS:-120}"
CURL_TIMEOUT_SECONDS="${KVNODE_INCIDENT_CURL_TIMEOUT:-5}"

port_base KVNODE_INCIDENT_BASE_PORT "$BASE_PORT"
port_base KVNODE_INCIDENT_PEER_BASE_PORT "$PEER_BASE_PORT"
port_base KVNODE_INCIDENT_ADMIN_BASE_PORT "$ADMIN_BASE_PORT"
positive_int KVNODE_INCIDENT_READY_ATTEMPTS "$READY_ATTEMPTS"
positive_int KVNODE_INCIDENT_CURL_TIMEOUT "$CURL_TIMEOUT_SECONDS"

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${KVNODE_INCIDENT_OUT_DIR:-${TMPDIR:-/tmp}/kvnode-incident-tabletop-${run_id}}"
BIN_DIR="$RUN_DIR/bin"
DATA_DIR="$RUN_DIR/data"
LOG_DIR="$RUN_DIR/logs"
EVIDENCE_DIR="$RUN_DIR/evidence"
mkdir -p "$BIN_DIR" "$DATA_DIR" "$LOG_DIR" "$EVIDENCE_DIR"

BIN="$BIN_DIR/kvnode"
NODE_PIDS=("" "" "" "")

print_logs() {
  local log=""
  for log in "$LOG_DIR"/*.log; do
    if [[ -f "$log" ]]; then
      echo "kvnode-incident-tabletop log_begin file=$log" >&2
      cat "$log" >&2 || true
      echo "kvnode-incident-tabletop log_end file=$log" >&2
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
    echo "kvnode-incident-tabletop evidence_dir=$EVIDENCE_DIR" >&2
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

http_get() {
  local url="$1"
  curl -fsS --max-time "$CURL_TIMEOUT_SECONDS" "$url"
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

capture_admin() {
  local label="$1"
  local id=""
  for id in 1 2 3; do
    curl -sS --max-time "$CURL_TIMEOUT_SECONDS" "$(admin_url "$id")/livez" -o "$EVIDENCE_DIR/$label-node-$id-livez.txt" || true
    curl -sS --max-time "$CURL_TIMEOUT_SECONDS" "$(admin_url "$id")/readyz" -o "$EVIDENCE_DIR/$label-node-$id-readyz.txt" || true
    curl -sS --max-time "$CURL_TIMEOUT_SECONDS" "$(admin_url "$id")/metrics" -o "$EVIDENCE_DIR/$label-node-$id-metrics.txt" || true
    curl -sS --max-time "$CURL_TIMEOUT_SECONDS" "$(admin_url "$id")/faults/storage" -o "$EVIDENCE_DIR/$label-node-$id-storage.json" || true
    curl -sS --max-time "$CURL_TIMEOUT_SECONDS" "$(admin_url "$id")/faults/transport" -o "$EVIDENCE_DIR/$label-node-$id-transport.json" || true
  done
}

expect_status() {
  local url="$1"
  local want="$2"
  local file="$3"
  local got=""
  got="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$file" -w '%{http_code}' "$url" 2>/dev/null || printf '000')"
  if [[ "$got" != "$want" ]]; then
    fail "unexpected-status-$got-want-$want-url-$url-body-$(cat "$file" 2>/dev/null || true)"
  fi
}

post_json_no_content() {
  local url="$1"
  local json="$2"
  local status=""
  local body_file="$RUN_DIR/last-post-response.txt"
  status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w '%{http_code}' -H 'Content-Type: application/json' -X POST --data-binary "$json" "$url")" || status="curl-error"
  if [[ "$status" != "204" ]]; then
    fail "post-failed-status-$status-url-$url-body-$(cat "$body_file" 2>/dev/null || true)"
  fi
}

delete_no_content() {
  local url="$1"
  local status=""
  local body_file="$RUN_DIR/last-delete-response.txt"
  status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w '%{http_code}' -X DELETE "$url")" || status="curl-error"
  if [[ "$status" != "204" ]]; then
    fail "delete-failed-status-$status-url-$url-body-$(cat "$body_file" 2>/dev/null || true)"
  fi
}

put_value() {
  local id="$1"
  local key="$2"
  local value="$3"
  local body_file="$RUN_DIR/last-put-response.txt"
  local status=""
  status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w '%{http_code}' -X PUT --data-binary "$value" "$(client_url "$id")/kv/$key")" || status="curl-error"
  if [[ "$status" != "204" ]]; then
    fail "put-failed-status-$status-key-$key-body-$(cat "$body_file" 2>/dev/null || true)"
  fi
}

assert_get_value() {
  local id="$1"
  local key="$2"
  local want="$3"
  local attempt=""
  local body_file="$RUN_DIR/get-$id-$key.txt"
  local status=""
  for ((attempt=1; attempt<=READY_ATTEMPTS; attempt++)); do
    status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w '%{http_code}' "$(client_url "$id")/kv/$key" 2>/dev/null || printf '000')"
    if [[ "$status" == "200" && "$(cat "$body_file")" == "$want" ]]; then
      return
    fi
    sleep 0.1
  done
  fail "get-failed-node-$id-key-$key-status-$status-body-$(cat "$body_file" 2>/dev/null || true)"
}

assert_metrics_contains() {
  local id="$1"
  local needle="$2"
  local metrics_file="$RUN_DIR/metrics-node-$id.txt"
  http_get "$(admin_url "$id")/metrics" > "$metrics_file" || fail "metrics-unavailable-node-$id"
  if ! grep -Fq -- "$needle" "$metrics_file"; then
    fail "metrics-node-$id-missing-$needle"
  fi
}

write_report() {
  local report_path="${KVNODE_INCIDENT_TABLETOP_REPORT:-}"
  [[ -n "$report_path" ]] || return 0
  [[ "$report_path" != "." && "$report_path" != "/" ]] || fail "KVNODE_INCIDENT_TABLETOP_REPORT-must-name-a-file"
  mkdir -p "$(dirname "$report_path")"
  {
    echo "status=example-operator-report"
    echo "artifact=incident-tabletop-drill"
    printf 'evidence_dir=%q\n' "$EVIDENCE_DIR"
    echo "storage_fault=exercised-and-cleared"
    echo "transport_fault=exercised-and-cleared"
    echo "canaries=baseline-and-after-clear-visible-on-all-nodes"
    echo "operator_review=not-performed"
    echo "release_claim=none-target-environment-operator-review-still-required"
  } > "$report_path"
  chmod 0600 "$report_path"
  printf 'report=%q\n' "$report_path"
}

cat > "$EVIDENCE_DIR/metadata.env" <<EOF
status=local-tabletop-only
run_id=$run_id
base_port=$BASE_PORT
peer_base_port=$PEER_BASE_PORT
admin_base_port=$ADMIN_BASE_PORT
evidence_dir=$EVIDENCE_DIR
non_claim=not_target_environment_not_operator_reviewed
release_claim=none-target-environment-operator-review-still-required
EOF

echo "kvnode-incident-tabletop phase=build"
(
  cd examples/kv
  go build -tags kvnode -o "$BIN" ./cmd/kvnode
)

echo "kvnode-incident-tabletop phase=start-cluster"
for id in 1 2 3; do
  start_node "$id"
done
for id in 1 2 3; do
  wait_node_ready "$id"
done
capture_admin baseline

put_value 1 tabletop-baseline baseline-value
for id in 1 2 3; do
  assert_get_value "$id" tabletop-baseline baseline-value
done

echo "kvnode-incident-tabletop phase=storage-fault"
post_json_no_content "$(admin_url 2)/faults/storage" '{"fail":true}'
expect_status "$(admin_url 2)/readyz" "503" "$EVIDENCE_DIR/storage-fault-node-2-readyz.txt"
assert_metrics_contains 2 "kvnode_storage_fault_active 1"
capture_admin storage-fault-active
delete_no_content "$(admin_url 2)/faults/storage"
expect_status "$(admin_url 2)/readyz" "200" "$EVIDENCE_DIR/storage-fault-cleared-node-2-readyz.txt"
assert_metrics_contains 2 "kvnode_storage_fault_active 0"

echo "kvnode-incident-tabletop phase=transport-fault"
post_json_no_content "$(admin_url 1)/faults/transport" '{"from":1,"to":2,"drop":true}'
post_json_no_content "$(admin_url 2)/faults/transport" '{"from":2,"to":1,"drop":true}'
post_json_no_content "$(admin_url 2)/faults/transport" '{"from":2,"to":3,"drop":true}'
post_json_no_content "$(admin_url 3)/faults/transport" '{"from":3,"to":2,"drop":true}'
assert_metrics_contains 2 "kvnode_transport_dropped_links 2"
capture_admin transport-fault-active
for id in 1 2 3; do
  delete_no_content "$(admin_url "$id")/faults/transport"
done
for id in 1 2 3; do
  assert_metrics_contains "$id" "kvnode_transport_dropped_links 0"
done
capture_admin faults-cleared

put_value 3 tabletop-after-clear after-clear-value
for id in 1 2 3; do
  assert_get_value "$id" tabletop-after-clear after-clear-value
done

cat > "$EVIDENCE_DIR/summary.txt" <<EOF
status=local-tabletop-only
storage_fault=exercised-and-cleared
transport_fault=exercised-and-cleared
canaries=baseline-and-after-clear-visible-on-all-nodes
release_claim=none-target-environment-operator-review-still-required
EOF

write_report
cat "$EVIDENCE_DIR/summary.txt"
echo "kvnode-incident-tabletop status=pass evidence_dir=$EVIDENCE_DIR"
