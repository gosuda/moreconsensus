#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail_preflight() {
  echo "$1" >&2
  exit 2
}

env_yes() {
  [[ "${1:-}" == "1" || "${1:-}" == "true" || "${1:-}" == "yes" ]]
}

validate_nodes() {
  local raw="$1"
  if [[ -z "$raw" ]]; then
    fail_preflight "JEPSEN_NODES must list SSH hosts, comma-separated"
  fi
  IFS=',' read -r -a nodes <<<"$raw"
  for node in "${nodes[@]}"; do
    if [[ -z "$node" || "$node" == -* || "$node" == *[[:space:]]* ]]; then
      fail_preflight "JEPSEN_NODES contains an invalid host entry"
    fi
  done
}

validate_positive_int() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    fail_preflight "$name must be a positive integer"
  fi
}

validate_concurrency() {
  local value="$1"
  if [[ ! "$value" =~ ^[1-9][0-9]*n?$ ]]; then
    fail_preflight "JEPSEN_REMOTE_CONCURRENCY must be a positive integer or n-suffixed multiplier"
  fi
}

validate_faults() {
  case "$1" in
  restart | transport | storage | destructive-storage | wall-clock-skew) ;;
  *) fail_preflight "JEPSEN_REMOTE_FAULTS must be one of restart, transport, storage, destructive-storage, wall-clock-skew" ;;
  esac
}

validate_remote_dir() {
  local path="$1"
  if [[ -z "$path" ]]; then
    fail_preflight "MORECONSENSUS_KVNODE_REMOTE_DIR must be non-empty"
  fi
  if [[ "$path" != /* ]]; then
    fail_preflight "MORECONSENSUS_KVNODE_REMOTE_DIR must be absolute"
  fi
  case "$path" in
  / | /tmp | /var | /srv) fail_preflight "MORECONSENSUS_KVNODE_REMOTE_DIR is too broad" ;;
  */../* | */..) fail_preflight "MORECONSENSUS_KVNODE_REMOTE_DIR contains parent traversal" ;;
  *"*"* | *"?"* | *"["* | *"]"* | *"{"* | *"}"*) fail_preflight "MORECONSENSUS_KVNODE_REMOTE_DIR contains shell glob characters" ;;
  esac
  if ! env_yes "${MORECONSENSUS_KVNODE_REMOTE_DIR_ALLOW_UNSAFE:-}"; then
    case "$path" in
    /tmp/moreconsensus-jepsen | /tmp/moreconsensus-jepsen/* | /srv/kv | /srv/kv/*) ;;
    *) fail_preflight "MORECONSENSUS_KVNODE_REMOTE_DIR must be under /tmp/moreconsensus-jepsen or /srv/kv unless MORECONSENSUS_KVNODE_REMOTE_DIR_ALLOW_UNSAFE=yes" ;;
    esac
  fi
}

validate_nodes "${JEPSEN_NODES:-}"

remote_faults="${JEPSEN_REMOTE_FAULTS:-destructive-storage}"
remote_duration="${JEPSEN_REMOTE_DURATION:-30}"
remote_concurrency="${JEPSEN_REMOTE_CONCURRENCY:-3n}"
remote_dir="${MORECONSENSUS_KVNODE_REMOTE_DIR:-/tmp/moreconsensus-jepsen}"

validate_faults "$remote_faults"
validate_positive_int "JEPSEN_REMOTE_DURATION" "$remote_duration"
validate_concurrency "$remote_concurrency"
validate_remote_dir "$remote_dir"

if [[ "$remote_faults" == "destructive-storage" && "${JEPSEN_REMOTE_CONFIRM_DESTRUCTIVE:-}" != "yes" ]]; then
  fail_preflight "JEPSEN_REMOTE_CONFIRM_DESTRUCTIVE=yes is required for remote destructive-storage runs"
fi
if [[ "$remote_faults" == "wall-clock-skew" && "${JEPSEN_REMOTE_CONFIRM_WALL_CLOCK_SKEW:-}" != "yes" ]]; then
  fail_preflight "JEPSEN_REMOTE_CONFIRM_WALL_CLOCK_SKEW=yes is required for remote wall-clock-skew runs"
fi

if env_yes "${JEPSEN_REMOTE_PREFLIGHT_ONLY:-}"; then
  echo "remote_preflight=ok"
  echo "remote_preflight_nodes=${#nodes[@]}"
  echo "remote_preflight_faults=$remote_faults"
  echo "remote_preflight_dir=$remote_dir"
  echo "remote_preflight_duration=$remote_duration"
  echo "remote_preflight_concurrency=$remote_concurrency"
  exit 0
fi

BIN="${TMPDIR:-/tmp}/moreconsensus-kvnode-remote-$$"
cleanup() {
  rm -f "$BIN"
}
trap cleanup EXIT

(cd examples/kv && go build -tags kvnode -o "$BIN" ./cmd/kvnode)

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Djava.net.preferIPv4Stack=true"
export MORECONSENSUS_KVNODE_REMOTE=1
export MORECONSENSUS_KVNODE_BIN="$BIN"
export MORECONSENSUS_KVNODE_REMOTE_DIR="$remote_dir"
export MORECONSENSUS_KVNODE_HTTP_PORT="${MORECONSENSUS_KVNODE_HTTP_PORT:-8080}"
export MORECONSENSUS_KVNODE_PEER_PORT="${MORECONSENSUS_KVNODE_PEER_PORT:-8081}"
export MORECONSENSUS_KVNODE_ADMIN_PORT="${MORECONSENSUS_KVNODE_ADMIN_PORT:-8082}"
export MORECONSENSUS_KVNODE_FAULTS="$remote_faults"
export MORECONSENSUS_KVNODE_REMOTE_DESTRUCTIVE_CONFIRM="${JEPSEN_REMOTE_CONFIRM_DESTRUCTIVE:-}"
export MORECONSENSUS_KVNODE_REMOTE_WALL_CLOCK_SKEW_CONFIRM="${JEPSEN_REMOTE_CONFIRM_WALL_CLOCK_SKEW:-}"

(
  cd jepsen
  lein run test --nodes "$JEPSEN_NODES" --time-limit "$remote_duration" --concurrency "$remote_concurrency" "$@"
)
