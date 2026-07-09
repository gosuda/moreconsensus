#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

usage() {
  cat <<'USAGE'
kvnode mixed-version upgrade/rollback drill (local loopback harness only)

Status: local evidence harness only. This script starts local loopback kvnode
processes and does not assert production readiness, target-environment coverage,
operator backup/restore readiness, or deployment safety.
Binary rollback in this drill restarts the old binary on the node's current data.
Pre-upgrade data checkpoints are captured as rollback inputs, but
checkpoint restore is a separate data-lifecycle fallback and is not exercised by
this mixed-version harness.

Required input:
  KVNODE_UPGRADE_OLD_REF       Git ref for the old kvnode build. There is no
                               default; choose the exact release/build ref under
                               test.

Optional inputs:
  KVNODE_UPGRADE_NEW_REF       Git ref for the new kvnode build. Default: HEAD.
                               The new binary is built from a clean git archive
                               of this ref, not from the active worktree.
  KVNODE_UPGRADE_BASE_PORT       Current-binary client port base. Nodes use
                                 base+1..base+3. Default: 21080
  KVNODE_UPGRADE_PEER_BASE_PORT  Stable peer port base. Legacy old binaries also
                                 use this as their client/admin base. Default: 21180
  KVNODE_UPGRADE_ADMIN_BASE_PORT Current-binary admin port base. Nodes use
                                 base+1..base+3. Default: 21280
  KVNODE_UPGRADE_CURL_TIMEOUT    curl per-request timeout seconds. Default: 5
  KVNODE_UPGRADE_READY_ATTEMPTS  readiness polling attempts. Default: 150
  KVNODE_UPGRADE_CANARY_ATTEMPTS
                                 canary PUT/GET/scan retry attempts. Default: 20
  KVNODE_UPGRADE_SETTLE_SECONDS
                                 seconds to wait after listener readiness before
                                 canaries. Default: 2
  KVNODE_UPGRADE_SMOKE_ONLY      Set to yes to allow identical old/new refs or
                                 binary SHA-256s for harness smoke checks only.

Example:
  KVNODE_UPGRADE_OLD_REF=v0.1.2 bash tests/kvnode_mixed_version_drill.sh
USAGE
}

fail() {
  echo "kvnode-mixed-version status=fail reason=$*" >&2
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

sha256_file() {
  local file="$1"
  local line=""
  if command -v sha256sum >/dev/null 2>&1; then
    line="$(sha256sum "$file")"
    printf '%s' "${line%% *}"
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    line="$(shasum -a 256 "$file")"
    printf '%s' "${line%% *}"
    return
  fi
  fail "missing-required-command-sha256sum-or-shasum"
}

source_tree_hash() {
  local commit="$1"
  local manifest="$RUN_DIR/source-$commit.txt"
  git ls-tree -r "$commit" -- go.mod go.work epaxos examples/kv >"$manifest"
  sha256_file "$manifest"
}

binary_supports_split_planes() {
  local bin="$1"
  local help=""
  help="$($bin -h 2>&1 || true)"
  case "$help" in
    *-peer-listen*) printf 'yes' ;;
    *) printf 'no' ;;
  esac
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

require_command git
require_command go
require_command curl
require_command tar
require_command mktemp
require_command cp
require_command rm
require_command mkdir

if [[ -z "${KVNODE_UPGRADE_OLD_REF:-}" ]]; then
  usage >&2
  echo >&2
  fail "KVNODE_UPGRADE_OLD_REF-required"
fi

OLD_REF="$KVNODE_UPGRADE_OLD_REF"
NEW_REF="${KVNODE_UPGRADE_NEW_REF:-HEAD}"
SMOKE_ONLY="${KVNODE_UPGRADE_SMOKE_ONLY:-no}"
if [[ "$SMOKE_ONLY" != "yes" && "$SMOKE_ONLY" != "no" ]]; then
  fail "KVNODE_UPGRADE_SMOKE_ONLY-must-be-yes-or-no"
fi

BASE_PORT="${KVNODE_UPGRADE_BASE_PORT:-21080}"
PEER_BASE_PORT="${KVNODE_UPGRADE_PEER_BASE_PORT:-21180}"
ADMIN_BASE_PORT="${KVNODE_UPGRADE_ADMIN_BASE_PORT:-21280}"
CURL_TIMEOUT_SECONDS="${KVNODE_UPGRADE_CURL_TIMEOUT:-5}"
READY_ATTEMPTS="${KVNODE_UPGRADE_READY_ATTEMPTS:-150}"
CANARY_ATTEMPTS="${KVNODE_UPGRADE_CANARY_ATTEMPTS:-20}"
SETTLE_SECONDS="${KVNODE_UPGRADE_SETTLE_SECONDS:-2}"

port_base KVNODE_UPGRADE_BASE_PORT "$BASE_PORT"
port_base KVNODE_UPGRADE_PEER_BASE_PORT "$PEER_BASE_PORT"
port_base KVNODE_UPGRADE_ADMIN_BASE_PORT "$ADMIN_BASE_PORT"
positive_int KVNODE_UPGRADE_CURL_TIMEOUT "$CURL_TIMEOUT_SECONDS"
positive_int KVNODE_UPGRADE_READY_ATTEMPTS "$READY_ATTEMPTS"
positive_int KVNODE_UPGRADE_CANARY_ATTEMPTS "$CANARY_ATTEMPTS"
positive_int KVNODE_UPGRADE_SETTLE_SECONDS "$SETTLE_SECONDS"

GIT_ROOT="$(cd "$(git rev-parse --show-toplevel)" && pwd -P)"
if [[ "$GIT_ROOT" != "$ROOT" ]]; then
  fail "script-root-does-not-match-git-root"
fi


OLD_COMMIT="$(git rev-parse --verify "${OLD_REF}^{commit}")"
NEW_COMMIT="$(git rev-parse --verify "${NEW_REF}^{commit}")"

TMP_PARENT="${TMPDIR:-/tmp}"
TMP_PARENT="${TMP_PARENT%/}"
RUN_DIR="$(mktemp -d "$TMP_PARENT/kvnode-mixed-version.XXXXXX")"
OLD_SRC="$RUN_DIR/old-src"
NEW_SRC="$RUN_DIR/new-src"
BIN_DIR="$RUN_DIR/bin"
DATA_DIR="$RUN_DIR/data"
LOG_DIR="$RUN_DIR/logs"
CHECKPOINT_DIR="$RUN_DIR/checkpoints"
mkdir -p "$OLD_SRC" "$NEW_SRC" "$BIN_DIR" "$DATA_DIR" "$LOG_DIR" "$CHECKPOINT_DIR"

OLD_BIN="$BIN_DIR/kvnode-old"
NEW_BIN="$BIN_DIR/kvnode-new"
NODE_PIDS=("" "" "" "")
NODE_LABELS=("" "" "" "")
NODE_SPLIT_PLANES=("" "" "" "")
OLD_SPLIT_PLANES=""
NEW_SPLIT_PLANES=""
LAST_PHASE="setup"

print_logs() {
  local log=""
  if [[ -d "${LOG_DIR:-}" ]]; then
    for log in "$LOG_DIR"/*.log; do
      if [[ -f "$log" ]]; then
        echo "kvnode-mixed-version log_begin file=$log" >&2
        cat "$log" >&2 || true
        echo "kvnode-mixed-version log_end file=$log" >&2
      fi
    done
  fi
}

safe_remove_run_dir() {
  if [[ -n "${RUN_DIR:-}" && -d "${RUN_DIR:-}" ]]; then
    case "$RUN_DIR" in
      "$TMP_PARENT"/kvnode-mixed-version.*)
        rm -rf "$RUN_DIR"
        ;;
      *)
        echo "kvnode-mixed-version cleanup_refused path=$RUN_DIR" >&2
        ;;
    esac
  fi
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
  if [[ "$status" != "0" ]]; then
    echo "kvnode-mixed-version status=fail phase=$LAST_PHASE" >&2
    print_logs
  fi
  safe_remove_run_dir
  exit "$status"
}
trap cleanup EXIT

safe_remove_data_path() {
  local target="$1"
  case "$target" in
    "$DATA_DIR"/node-1|"$DATA_DIR"/node-2|"$DATA_DIR"/node-3|"$CHECKPOINT_DIR"/node-1-pre-upgrade|"$CHECKPOINT_DIR"/node-2-pre-upgrade|"$CHECKPOINT_DIR"/node-3-pre-upgrade)
      rm -rf "$target"
      ;;
    *)
      fail "refusing-unsafe-remove-$target"
      ;;
  esac
}

build_new_binary() {
  LAST_PHASE="build-new"
  git archive --format=tar "$NEW_COMMIT" | (cd "$NEW_SRC" && tar -xf -)
  (cd "$NEW_SRC/examples/kv" && go build -trimpath -buildvcs=false -tags kvnode -o "$NEW_BIN" ./cmd/kvnode)
}

build_old_binary() {
  LAST_PHASE="build-old"
  git archive --format=tar "$OLD_COMMIT" | (cd "$OLD_SRC" && tar -xf -)
  (cd "$OLD_SRC/examples/kv" && go build -trimpath -buildvcs=false -tags kvnode -o "$OLD_BIN" ./cmd/kvnode)
}

peer_arg() {
  local out=""
  local id=""
  local peer_port=""
  for id in 1 2 3; do
    peer_port=$((PEER_BASE_PORT + id))
    if [[ -n "$out" ]]; then
      out="$out,"
    fi
    out="$out$id=http://127.0.0.1:$peer_port"
  done
  printf '%s' "$out"
}

client_url() {
  local id="$1"
  if [[ "${NODE_SPLIT_PLANES[$id]}" == "yes" ]]; then
    printf 'http://127.0.0.1:%s' "$((BASE_PORT + id))"
    return
  fi
  printf 'http://127.0.0.1:%s' "$((PEER_BASE_PORT + id))"
}

peer_url() {
  local id="$1"
  printf 'http://127.0.0.1:%s' "$((PEER_BASE_PORT + id))"
}

admin_url() {
  local id="$1"
  if [[ "${NODE_SPLIT_PLANES[$id]}" == "yes" ]]; then
    printf 'http://127.0.0.1:%s' "$((ADMIN_BASE_PORT + id))"
    return
  fi
  printf 'http://127.0.0.1:%s' "$((PEER_BASE_PORT + id))"
}

http_get() {
  local url="$1"
  curl -fsS --max-time "$CURL_TIMEOUT_SECONDS" "$url"
}

http_status() {
  local url="$1"
  local status=""
  status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || printf '000')"
  printf '%s' "$status"
}

http_responds() {
  local url="$1"
  local status=""
  status="$(http_status "$url")"
  case "$status" in
    2*|3*|4*) return 0 ;;
    *) return 1 ;;
  esac
}

node_listeners_ready() {
  local id="$1"
  http_responds "$(admin_url "$id")/health" &&
    http_responds "$(client_url "$id")/kv/mv-drill-probe?exact-time=0" &&
    http_responds "$(peer_url "$id")/epaxos/message"
}

http_put() {
  local url="$1"
  local value="$2"
  local body_file="$RUN_DIR/last-put-response.txt"
  local status=""
  status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w "%{http_code}" -X PUT --data-binary "$value" "$url")" || return 1
  if [[ "$status" != "204" ]]; then
    echo "kvnode-mixed-version put_failed status=$status url=$url body_begin" >&2
    cat "$body_file" >&2 || true
    echo "kvnode-mixed-version put_failed body_end" >&2
    return 1
  fi
}

diagnostic_put() {
  local writer_id="$1"
  local key="$2"
  local value="$3"
  local body_file="$RUN_DIR/diagnostic-put-response.txt"
  local status=""
  status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w "%{http_code}" -X PUT --data-binary "$value" "$(client_url "$writer_id")/kv/$key")" || status="curl-error"
  echo "kvnode-mixed-version diagnostic_put writer=$writer_id key=$key status=$status"
  if [[ -s "$body_file" ]]; then
    echo "kvnode-mixed-version diagnostic_put_body_begin writer=$writer_id key=$key" >&2
    cat "$body_file" >&2 || true
    echo "kvnode-mixed-version diagnostic_put_body_end writer=$writer_id key=$key" >&2
  fi
}

assert_health() {
  local id="$1"
  local body=""
  body="$(http_get "$(admin_url "$id")/health")" || fail "node-$id-health-failed"
  if [[ "$body" != "ok" ]]; then
    fail "node-$id-health-body-$body"
  fi
}

assert_readyz() {
  local id="$1"
  local body=""
  body="$(http_get "$(admin_url "$id")/readyz")" || fail "node-$id-readyz-failed"
  if [[ "$body" != "ready" ]]; then
    fail "node-$id-readyz-body-$body"
  fi
}

assert_metrics() {
  local id="$1"
  local body=""
  body="$(http_get "$(admin_url "$id")/metrics")" || fail "node-$id-metrics-failed"
  case "$body" in
    *kvnode_epaxos_instances*kvnode_epaxos_executed*kvnode_send_queue_depth*) ;;
    *) fail "node-$id-metrics-missing-kvnode-series" ;;
  esac
  case "$body" in
    *"kvnode_storage_fault_active 0"*) ;;
    *) fail "node-$id-storage-fault-active" ;;
  esac
  case "$body" in
    *"kvnode_transport_dropped_links 0"*) ;;
    *) fail "node-$id-transport-drops-active" ;;
  esac
}

assert_node_admin() {
  local id="$1"
  assert_health "$id"
  if [[ "${NODE_SPLIT_PLANES[$id]}" == "yes" ]]; then
    assert_readyz "$id"
    assert_metrics "$id"
  fi
}

wait_ready() {
  local id="$1"
  local attempt=""
  local pid="${NODE_PIDS[$id]}"
  for ((attempt=1; attempt<=READY_ATTEMPTS; attempt++)); do
    if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      fail "node-$id-exited-before-ready"
    fi
    if node_listeners_ready "$id"; then
      assert_node_admin "$id"
      return
    fi
    sleep 0.1
  done
  fail "node-$id-ready-timeout"
}

start_node() {
  local id="$1"
  local bin="$2"
  local label="$3"
  local split_planes="$4"
  local client_port=""
  local peer_port=""
  local admin_port=""
  local log_file=""
  if [[ -n "${NODE_PIDS[$id]}" ]]; then
    fail "node-$id-already-running"
  fi
  client_port=$((BASE_PORT + id))
  peer_port=$((PEER_BASE_PORT + id))
  admin_port=$((ADMIN_BASE_PORT + id))
  mkdir -p "$DATA_DIR/node-$id"
  log_file="$LOG_DIR/node-$id-$label.log"
  NODE_SPLIT_PLANES[$id]="$split_planes"
  if [[ "$split_planes" == "yes" ]]; then
    "$bin" \
      -id "$id" \
      -listen ":$client_port" \
      -peer-listen ":$peer_port" \
      -admin-listen ":$admin_port" \
      -data "$DATA_DIR/node-$id" \
      -peers "$(peer_arg)" \
      -request-deadline-ms 5000 \
      -peer-deadline-ms 2000 \
      >"$log_file" 2>&1 &
  else
    "$bin" \
      -id "$id" \
      -listen ":$peer_port" \
      -data "$DATA_DIR/node-$id" \
      -peers "$(peer_arg)" \
      >"$log_file" 2>&1 &
  fi
  NODE_PIDS[$id]="$!"
  NODE_LABELS[$id]="$label"
}

stop_node() {
  local id="$1"
  local pid="${NODE_PIDS[$id]}"
  if [[ -z "$pid" ]]; then
    return
  fi
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  NODE_PIDS[$id]=""
  NODE_LABELS[$id]=""
  NODE_SPLIT_PLANES[$id]=""
}

verify_cluster_health() {
  local id=""
  for id in 1 2 3; do
    assert_node_admin "$id"
  done
}

assert_get_value() {
  local id="$1"
  local key="$2"
  local want="$3"
  local body_file="$RUN_DIR/last-get-response.txt"
  local status=""
  local attempt=""
  local last_status=""
  local last_body=""
  for ((attempt=1; attempt<=CANARY_ATTEMPTS; attempt++)); do
    status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w "%{http_code}" "$(client_url "$id")/kv/$key" 2>/dev/null || printf 'curl-error')"
    last_body="$(cat "$body_file" 2>/dev/null || true)"
    if [[ "$status" == "200" && "$last_body" == "$want" ]]; then
      return
    fi
    last_status="$status"
    sleep 0.1
  done
  echo "kvnode-mixed-version get_failed node=$id key=$key status=$last_status body_begin" >&2
  printf '%s\n' "$last_body" >&2
  echo "kvnode-mixed-version get_failed body_end node=$id key=$key" >&2
  fail "node-$id-get-$key-failed"
}

assert_all_get_value() {
  local key="$1"
  local want="$2"
  local id=""
  for id in 1 2 3; do
    assert_get_value "$id" "$key" "$want"
  done
}

assert_scan_has() {
  local id="$1"
  local key="$2"
  local want="$3"
  local body_file="$RUN_DIR/last-scan-response.txt"
  local status=""
  local body=""
  local attempt=""
  for ((attempt=1; attempt<=CANARY_ATTEMPTS; attempt++)); do
    status="$(curl -sS --max-time "$CURL_TIMEOUT_SECONDS" -o "$body_file" -w "%{http_code}" "$(client_url "$id")/scan?prefix=mv-drill-&barrier=$key" 2>/dev/null || printf 'curl-error')"
    body="$(cat "$body_file" 2>/dev/null || true)"
    if [[ "$status" == "200" ]]; then
      case "$body" in
        *"{\"key\":\"$key\",\"value\":\"$want\",\"time\":"*) return ;;
      esac
    fi
    sleep 0.1
  done
  echo "kvnode-mixed-version scan_failed node=$id key=$key status=$status body_begin" >&2
  printf '%s\n' "$body" >&2
  echo "kvnode-mixed-version scan_failed body_end node=$id key=$key" >&2
  fail "node-$id-scan-missing-$key"
}

assert_all_scan_has() {
  local key="$1"
  local want="$2"
  local id=""
  for id in 1 2 3; do
    assert_scan_has "$id" "$key" "$want"
  done
}

put_and_verify_all() {
  local writer_id="$1"
  local key="$2"
  local value="$3"
  local attempt=""
  for ((attempt=1; attempt<=CANARY_ATTEMPTS; attempt++)); do
    if http_put "$(client_url "$writer_id")/kv/$key" "$value" 2>/dev/null; then
      assert_all_get_value "$key" "$value"
      assert_all_scan_has "$key" "$value"
      return
    fi
    sleep 0.1
  done
  http_put "$(client_url "$writer_id")/kv/$key" "$value" || fail "node-$writer_id-put-$key-failed"
  assert_all_get_value "$key" "$value"
  assert_all_scan_has "$key" "$value"
}

assert_all_canaries() {
  assert_all_get_value mv-drill-baseline baseline-old-node-1
  assert_all_get_value mv-drill-mixed-new-node-1 mixed-new-node-1-writer
  assert_all_get_value mv-drill-mixed-old-node-2 mixed-old-node-2-writer
  assert_all_get_value mv-drill-mixed-old-node-3 mixed-old-node-3-writer
  assert_all_get_value mv-drill-rollback-node-1 rollback-node-1-writer
  assert_all_get_value mv-drill-roll-node-2 roll-node-2-writer
  assert_all_get_value mv-drill-roll-node-3 roll-node-3-writer
  assert_all_get_value mv-drill-reverse-rollback-node-3 reverse-rollback-node-3-writer
  assert_all_get_value mv-drill-reverse-rollback-node-2 reverse-rollback-node-2-writer
  assert_all_get_value mv-drill-reverse-rollback-node-1 reverse-rollback-node-1-writer
  assert_all_scan_has mv-drill-baseline baseline-old-node-1
  assert_all_scan_has mv-drill-mixed-new-node-1 mixed-new-node-1-writer
  assert_all_scan_has mv-drill-mixed-old-node-2 mixed-old-node-2-writer
  assert_all_scan_has mv-drill-mixed-old-node-3 mixed-old-node-3-writer
  assert_all_scan_has mv-drill-rollback-node-1 rollback-node-1-writer
  assert_all_scan_has mv-drill-roll-node-2 roll-node-2-writer
  assert_all_scan_has mv-drill-roll-node-3 roll-node-3-writer
  assert_all_scan_has mv-drill-reverse-rollback-node-3 reverse-rollback-node-3-writer
  assert_all_scan_has mv-drill-reverse-rollback-node-2 reverse-rollback-node-2-writer
  assert_all_scan_has mv-drill-reverse-rollback-node-1 reverse-rollback-node-1-writer
}

settle_cluster() {
  sleep "$SETTLE_SECONDS"
}

checkpoint_node() {
  local id="$1"
  local checkpoint="$CHECKPOINT_DIR/node-$id-pre-upgrade"
  LAST_PHASE="checkpoint-node-$id"
  stop_node "$id"
  safe_remove_data_path "$checkpoint"
  cp -Rp "$DATA_DIR/node-$id" "$checkpoint"
}

emit_success() {
  local phase="$1"
  echo "kvnode-mixed-version status=success phase=$phase"
}

build_old_binary
build_new_binary
OLD_SHA256="$(sha256_file "$OLD_BIN")"
NEW_SHA256="$(sha256_file "$NEW_BIN")"
OLD_SOURCE_HASH="$(source_tree_hash "$OLD_COMMIT")"
NEW_SOURCE_HASH="$(source_tree_hash "$NEW_COMMIT")"
OLD_SPLIT_PLANES="$(binary_supports_split_planes "$OLD_BIN")"
NEW_SPLIT_PLANES="$(binary_supports_split_planes "$NEW_BIN")"
if [[ "$NEW_SPLIT_PLANES" != "yes" ]]; then
  fail "current-binary-missing-readyz-metrics-admin-plane"
fi
if [[ "$OLD_COMMIT" == "$NEW_COMMIT" && "$SMOKE_ONLY" != "yes" ]]; then
  fail "old-new-refs-identical-set-KVNODE_UPGRADE_SMOKE_ONLY-yes-for-smoke-only"
fi
if [[ "$OLD_SOURCE_HASH" == "$NEW_SOURCE_HASH" && "$SMOKE_ONLY" != "yes" ]]; then
  fail "old-new-source-tree-hash-identical-set-KVNODE_UPGRADE_SMOKE_ONLY-yes-for-smoke-only"
fi
if [[ "$OLD_SHA256" == "$NEW_SHA256" && "$SMOKE_ONLY" != "yes" ]]; then
  fail "old-new-binary-sha256-identical-set-KVNODE_UPGRADE_SMOKE_ONLY-yes-for-smoke-only"
fi

echo "kvnode-mixed-version status=success phase=binary-metadata old_ref=$OLD_REF old_commit=$OLD_COMMIT new_ref=$NEW_REF new_commit=$NEW_COMMIT old_source_hash=$OLD_SOURCE_HASH new_source_hash=$NEW_SOURCE_HASH old_sha256=$OLD_SHA256 new_sha256=$NEW_SHA256 old_split_planes=$OLD_SPLIT_PLANES new_split_planes=$NEW_SPLIT_PLANES build_source=git_archive_trimpath smoke_only=$SMOKE_ONLY"

LAST_PHASE="start-old-cluster"
start_node 1 "$OLD_BIN" old "$OLD_SPLIT_PLANES"
start_node 2 "$OLD_BIN" old "$OLD_SPLIT_PLANES"
start_node 3 "$OLD_BIN" old "$OLD_SPLIT_PLANES"
wait_ready 1
wait_ready 2
wait_ready 3
verify_cluster_health
settle_cluster
diagnostic_put 1 mv-drill-diagnostic-initial diagnostic-initial
put_and_verify_all 1 mv-drill-baseline baseline-old-node-1
emit_success baseline-old-cluster
checkpoint_node 1
start_node 1 "$NEW_BIN" new "$NEW_SPLIT_PLANES"
wait_ready 1
verify_cluster_health
settle_cluster
diagnostic_put 1 mv-drill-diagnostic-node-1-new diagnostic-node-1-new
LAST_PHASE="one-node-upgrade-mixed"
put_and_verify_all 1 mv-drill-mixed-new-node-1 mixed-new-node-1-writer
put_and_verify_all 2 mv-drill-mixed-old-node-2 mixed-old-node-2-writer
put_and_verify_all 3 mv-drill-mixed-old-node-3 mixed-old-node-3-writer
emit_success one-node-upgrade-mixed-read-write-scan
stop_node 1
start_node 1 "$OLD_BIN" old-rollback "$OLD_SPLIT_PLANES"
wait_ready 1
LAST_PHASE="rollback-old-binary-catchup"
verify_cluster_health
settle_cluster
assert_all_get_value mv-drill-baseline baseline-old-node-1
assert_all_get_value mv-drill-mixed-new-node-1 mixed-new-node-1-writer
assert_all_get_value mv-drill-mixed-old-node-2 mixed-old-node-2-writer
assert_all_get_value mv-drill-mixed-old-node-3 mixed-old-node-3-writer
assert_all_scan_has mv-drill-mixed-new-node-1 mixed-new-node-1-writer
assert_all_scan_has mv-drill-mixed-old-node-2 mixed-old-node-2-writer
assert_all_scan_has mv-drill-mixed-old-node-3 mixed-old-node-3-writer
diagnostic_put 1 mv-drill-diagnostic-node-1-rollback diagnostic-node-1-rollback
put_and_verify_all 1 mv-drill-rollback-node-1 rollback-node-1-writer
emit_success rollback-old-binary-catchup

LAST_PHASE="reupgrade-node-1"
stop_node 1
start_node 1 "$NEW_BIN" new-reupgrade "$NEW_SPLIT_PLANES"
wait_ready 1
verify_cluster_health
settle_cluster
diagnostic_put 1 mv-drill-diagnostic-node-1-reupgrade diagnostic-node-1-reupgrade
assert_all_get_value mv-drill-rollback-node-1 rollback-node-1-writer
emit_success node-1-reupgrade

checkpoint_node 2
start_node 2 "$NEW_BIN" new-roll "$NEW_SPLIT_PLANES"
wait_ready 2
verify_cluster_health
settle_cluster
diagnostic_put 2 mv-drill-diagnostic-node-2-new diagnostic-node-2-new
put_and_verify_all 2 mv-drill-roll-node-2 roll-node-2-writer
emit_success node-2-upgrade

checkpoint_node 3
start_node 3 "$NEW_BIN" new-roll "$NEW_SPLIT_PLANES"
wait_ready 3
verify_cluster_health
settle_cluster
diagnostic_put 3 mv-drill-diagnostic-node-3-new diagnostic-node-3-new
put_and_verify_all 3 mv-drill-roll-node-3 roll-node-3-writer
assert_all_get_value mv-drill-baseline baseline-old-node-1
assert_all_get_value mv-drill-mixed-new-node-1 mixed-new-node-1-writer
assert_all_get_value mv-drill-mixed-old-node-2 mixed-old-node-2-writer
assert_all_get_value mv-drill-mixed-old-node-3 mixed-old-node-3-writer
assert_all_get_value mv-drill-rollback-node-1 rollback-node-1-writer
assert_all_scan_has mv-drill-roll-node-2 roll-node-2-writer
assert_all_scan_has mv-drill-roll-node-3 roll-node-3-writer
emit_success node-3-upgrade

LAST_PHASE="reverse-rollback-node-3"
stop_node 3
start_node 3 "$OLD_BIN" old-reverse-rollback "$OLD_SPLIT_PLANES"
wait_ready 3
verify_cluster_health
settle_cluster
diagnostic_put 3 mv-drill-diagnostic-node-3-rollback diagnostic-node-3-rollback
assert_all_get_value mv-drill-baseline baseline-old-node-1
assert_all_get_value mv-drill-mixed-new-node-1 mixed-new-node-1-writer
assert_all_get_value mv-drill-mixed-old-node-2 mixed-old-node-2-writer
assert_all_get_value mv-drill-mixed-old-node-3 mixed-old-node-3-writer
assert_all_get_value mv-drill-rollback-node-1 rollback-node-1-writer
assert_all_get_value mv-drill-roll-node-2 roll-node-2-writer
assert_all_get_value mv-drill-roll-node-3 roll-node-3-writer
put_and_verify_all 3 mv-drill-reverse-rollback-node-3 reverse-rollback-node-3-writer
emit_success node-3-reverse-rollback

LAST_PHASE="reverse-rollback-node-2"
stop_node 2
start_node 2 "$OLD_BIN" old-reverse-rollback "$OLD_SPLIT_PLANES"
wait_ready 2
verify_cluster_health
settle_cluster
diagnostic_put 2 mv-drill-diagnostic-node-2-rollback diagnostic-node-2-rollback
assert_all_get_value mv-drill-reverse-rollback-node-3 reverse-rollback-node-3-writer
assert_all_get_value mv-drill-roll-node-3 roll-node-3-writer
put_and_verify_all 2 mv-drill-reverse-rollback-node-2 reverse-rollback-node-2-writer
emit_success node-2-reverse-rollback

LAST_PHASE="reverse-rollback-node-1"
stop_node 1
start_node 1 "$OLD_BIN" old-reverse-rollback "$OLD_SPLIT_PLANES"
wait_ready 1
verify_cluster_health
settle_cluster
diagnostic_put 1 mv-drill-diagnostic-node-1-final-rollback diagnostic-node-1-final-rollback
assert_all_get_value mv-drill-reverse-rollback-node-3 reverse-rollback-node-3-writer
assert_all_get_value mv-drill-reverse-rollback-node-2 reverse-rollback-node-2-writer
assert_all_get_value mv-drill-roll-node-2 roll-node-2-writer
assert_all_get_value mv-drill-roll-node-3 roll-node-3-writer
put_and_verify_all 1 mv-drill-reverse-rollback-node-1 reverse-rollback-node-1-writer
assert_all_canaries
emit_success node-1-reverse-rollback

emit_success complete
