#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

unit="deploy/systemd/kvnode@.service"
env_example="deploy/systemd/kvnode.env.example"
# Set KVNODE_SYSTEMD_MANIFEST_REPORT=/path/report.env to persist a
# machine-readable example/operator report after a successful audit.

fail() {
  echo "kvnode systemd manifest audit: $*" >&2
  exit 1
}

require_file() {
  [[ -f "$1" ]] || fail "missing file $1"
}

require_text() {
  local file="$1"
  local text="$2"
  LC_ALL=C grep -Fq -- "$text" "$file" || fail "missing text in $file: $text"
}

get_env_value() {
  local key="$1"
  local line
  line="$(LC_ALL=C grep -E "^${key}=" "$env_example" || true)"
  [[ -n "$line" ]] || fail "environment file did not define $key"
  if [[ "$line" == *$'\n'* ]]; then
    fail "environment file defined $key more than once"
  fi
  local value="${line#*=}"
  if [[ "$value" == '"'*'"' && ${#value} -ge 2 ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s' "$value"
}

write_manifest_report() {
  local report_path="$1"
  local rendered_exec="$2"
  local systemd_analyze_status="$3"
  [[ -n "$report_path" ]] || return 0
  [[ "$report_path" != "." && "$report_path" != "/" ]] || fail "KVNODE_SYSTEMD_MANIFEST_REPORT must name a file"
  mkdir -p "$(dirname "$report_path")"
  {
    echo "status=example-operator-report"
    echo "artifact=systemd-manifest-audit"
    printf 'unit=%q\n' "$unit"
    printf 'environment_file=%q\n' "$env_example"
    printf 'rendered_exec=%s\n' "$rendered_exec"
    echo "systemd_analyze=$systemd_analyze_status"
    echo "release_claim=none-target-environment-deployment-manifest-still-required"
  } > "$report_path"
  chmod 0600 "$report_path"
  printf 'report=%q\n' "$report_path"
}

require_file "$unit"
require_file "$env_example"

required_vars=(
  KVNODE_ID
  KVNODE_CLIENT_LISTEN
  KVNODE_PEER_LISTEN
  KVNODE_ADMIN_LISTEN
  KVNODE_DATA_DIR
  KVNODE_PEERS
  KVNODE_REQUEST_DEADLINE_MS
  KVNODE_PEER_DEADLINE_MS
  KVNODE_MAX_CLIENT_BODY_BYTES
  KVNODE_MAX_PEER_BODY_BYTES
  KVNODE_MAX_ADMIN_BODY_BYTES
  KVNODE_MAX_SCAN_LIMIT
  KVNODE_TLS_ARGS
)

for var in "${required_vars[@]}"; do
  require_text "$env_example" "${var}="
  printf -v "$var" '%s' "$(get_env_value "$var")"
  if [[ "$var" != "KVNODE_TLS_ARGS" ]]; then
    require_text "$unit" "\${${var}}"
  fi
done
require_text "$unit" '$KVNODE_TLS_ARGS'
require_text "$unit" 'ExecStart=/usr/local/bin/kvnode'
require_text "$unit" 'User=kvnode'
require_text "$unit" 'Group=kvnode'
require_text "$unit" 'StateDirectory=kvnode/%i'
require_text "$unit" 'ProtectSystem=strict'
require_text "$unit" 'NoNewPrivileges=true'

[[ "$KVNODE_ID" =~ ^[1-7]$ ]] || fail "KVNODE_ID must be one supported voter id 1..7"
for var in KVNODE_CLIENT_LISTEN KVNODE_PEER_LISTEN KVNODE_ADMIN_LISTEN; do
  value="${!var}"
  [[ "$value" == *:* ]] || fail "$var must include host:port"
done
[[ "$KVNODE_DATA_DIR" == /var/lib/kvnode/* ]] || fail "KVNODE_DATA_DIR must live under /var/lib/kvnode"

IFS=',' read -r -a peers <<< "$KVNODE_PEERS"
((${#peers[@]} >= 1 && ${#peers[@]} <= 7)) || fail "KVNODE_PEERS must define 1..7 voters"
found_self=0
for peer in "${peers[@]}"; do
  [[ "$peer" =~ ^[1-7]=https?://[^[:space:]]+:[0-9]+$ ]] || fail "bad peer entry: $peer"
  [[ "$peer" == "$KVNODE_ID="* ]] && found_self=1
done
((found_self == 1)) || fail "KVNODE_PEERS must contain this node id"

for var in KVNODE_REQUEST_DEADLINE_MS KVNODE_PEER_DEADLINE_MS KVNODE_MAX_CLIENT_BODY_BYTES KVNODE_MAX_PEER_BODY_BYTES KVNODE_MAX_ADMIN_BODY_BYTES KVNODE_MAX_SCAN_LIMIT; do
  value="${!var}"
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || fail "$var must be a positive integer"
done

rendered=(
  /usr/local/bin/kvnode
  -id "$KVNODE_ID"
  -listen "$KVNODE_CLIENT_LISTEN"
  -peer-listen "$KVNODE_PEER_LISTEN"
  -admin-listen "$KVNODE_ADMIN_LISTEN"
  -data "$KVNODE_DATA_DIR"
  -peers "$KVNODE_PEERS"
  -request-deadline-ms "$KVNODE_REQUEST_DEADLINE_MS"
  -peer-deadline-ms "$KVNODE_PEER_DEADLINE_MS"
  -max-client-body-bytes "$KVNODE_MAX_CLIENT_BODY_BYTES"
  -max-peer-body-bytes "$KVNODE_MAX_PEER_BODY_BYTES"
  -max-admin-body-bytes "$KVNODE_MAX_ADMIN_BODY_BYTES"
  -max-scan-limit "$KVNODE_MAX_SCAN_LIMIT"
)
if [[ -n "$KVNODE_TLS_ARGS" ]]; then
  read -r -a tls_args <<< "$KVNODE_TLS_ARGS"
  for arg in "${tls_args[@]}"; do
    [[ "$arg" == -tls-cert=* || "$arg" == -tls-key=* || "$arg" == -tls-ca=* ]] || fail "unexpected TLS arg: $arg"
    rendered+=("$arg")
  done
fi

rendered_exec="$(printf '%q ' "${rendered[@]}")"
printf 'rendered_exec=%s\n' "$rendered_exec"
echo "release_claim=none-target-environment-deployment-manifest-still-required"

systemd_analyze_status="skipped"
if [[ "${KVNODE_SYSTEMD_ANALYZE:-}" == "yes" ]]; then
  command -v systemd-analyze >/dev/null 2>&1 || fail "KVNODE_SYSTEMD_ANALYZE=yes requested but systemd-analyze is unavailable"
  systemd-analyze verify "$unit"
  systemd_analyze_status="verified"
  echo "systemd_analyze=verified"
else
  echo "systemd_analyze=skipped (set KVNODE_SYSTEMD_ANALYZE=yes on a systemd host to verify with systemd-analyze)"
fi

write_manifest_report "${KVNODE_SYSTEMD_MANIFEST_REPORT:-}" "$rendered_exec" "$systemd_analyze_status"
