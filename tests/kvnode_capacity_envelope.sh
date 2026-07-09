#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat <<'USAGE'
kvnode capacity-envelope harness (opt-in, bounded)

Status: harness only. This script gathers small, operator-chosen samples from an
already-running kvnode cluster. It does not start nodes, does not change release
scope, and does not produce production capacity evidence by itself.

Required opt-in:
  KVNODE_CAPACITY_RUN=yes

Common inputs:
  KVNODE_CLIENT_URLS              Comma-separated client base URLs.
                                  Default: http://127.0.0.1:8080
  KVNODE_ADMIN_URLS               Comma-separated admin base URLs for /metrics.
                                  Default: http://127.0.0.1:8082
  KVNODE_CAPACITY_OPS_PER_PHASE   PUT/GET operations per value-size phase.
                                  Default: 30, max: 1000
  KVNODE_CAPACITY_VALUE_BYTES     Comma-separated value sizes to test.
                                  Default: 64,1024,4096
  KVNODE_CAPACITY_SCAN_LIMITS     Comma-separated scan limits to request.
                                  Default: 1,16,128
  KVNODE_CAPACITY_OUT_DIR         Output directory.
                                  Default: <tmp>/kvnode-capacity-envelope-<timestamp>
  KVNODE_CAPACITY_TIMEOUT_SECONDS curl per-request timeout. Default: 5

Optional resource inputs:
  KVNODE_PIDS                     Comma-separated kvnode PIDs for RSS samples.
  KVNODE_PID_FILES                Comma-separated files containing kvnode PIDs.
  KVNODE_DATA_DIRS                Comma-separated kvnode data dirs for disk samples.
  KVNODE_PEER_COUNT               Expected peer count label. Defaults to client URL count.

Output:
  metadata.env                    Harness inputs and peer-count label.
  latency.csv                     operation,http_status,seconds rows.
  resources.csv                   before/after RSS, disk, queue-depth samples.
  summary.md                      Machine-generated sample summary with no readiness claim.

Example:
  KVNODE_CAPACITY_RUN=yes \
  KVNODE_CLIENT_URLS=http://127.0.0.1:8080,http://127.0.0.1:8083,http://127.0.0.1:8086 \
  KVNODE_ADMIN_URLS=http://127.0.0.1:8082,http://127.0.0.1:8085,http://127.0.0.1:8088 \
  KVNODE_DATA_DIRS=/var/lib/kvnode/1,/var/lib/kvnode/2,/var/lib/kvnode/3 \
  KVNODE_PIDS=1234,1235,1236 \
  tests/kvnode_capacity_envelope.sh
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "${KVNODE_CAPACITY_RUN:-}" != "yes" ]]; then
  usage >&2
  echo >&2
  echo "Refusing to run without KVNODE_CAPACITY_RUN=yes." >&2
  exit 2
fi

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_command curl
require_command awk
require_command sort
require_command wc
require_command du
require_command ps
require_command sed
require_command tr

positive_int() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "$name must be a positive integer" >&2
    exit 2
  fi
  if (( 10#$value <= 0 )); then
    echo "$name must be a positive integer" >&2
    exit 2
  fi
}

bounded_int() {
  local name="$1"
  local value="$2"
  local max="$3"
  positive_int "$name" "$value"
  if (( 10#$value > max )); then
    echo "$name must be <= $max" >&2
    exit 2
  fi
}

trim_trailing_slash() {
  local raw="$1"
  raw="${raw%/}"
  printf '%s' "$raw"
}

client_urls_raw="${KVNODE_CLIENT_URLS:-http://127.0.0.1:8080}"
admin_urls_raw="${KVNODE_ADMIN_URLS:-http://127.0.0.1:8082}"
value_sizes_raw="${KVNODE_CAPACITY_VALUE_BYTES:-64,1024,4096}"
scan_limits_raw="${KVNODE_CAPACITY_SCAN_LIMITS:-1,16,128}"
ops_per_phase="${KVNODE_CAPACITY_OPS_PER_PHASE:-30}"
timeout_seconds="${KVNODE_CAPACITY_TIMEOUT_SECONDS:-5}"
max_value_bytes="${KVNODE_CAPACITY_MAX_VALUE_BYTES:-65536}"
max_scan_limit="${KVNODE_CAPACITY_MAX_SCAN_LIMIT:-10000}"

bounded_int KVNODE_CAPACITY_OPS_PER_PHASE "$ops_per_phase" 1000
bounded_int KVNODE_CAPACITY_TIMEOUT_SECONDS "$timeout_seconds" 300
bounded_int KVNODE_CAPACITY_MAX_VALUE_BYTES "$max_value_bytes" 1048576
bounded_int KVNODE_CAPACITY_MAX_SCAN_LIMIT "$max_scan_limit" 100000

IFS=',' read -r -a CLIENT_URLS <<< "$client_urls_raw"
IFS=',' read -r -a ADMIN_URLS <<< "$admin_urls_raw"
IFS=',' read -r -a VALUE_SIZES <<< "$value_sizes_raw"
IFS=',' read -r -a SCAN_LIMITS <<< "$scan_limits_raw"
DATA_DIRS=("")
PIDS_FROM_ENV=("")
PID_FILES=("")
if [[ -n "${KVNODE_DATA_DIRS:-}" ]]; then
  IFS=',' read -r -a DATA_DIRS <<< "${KVNODE_DATA_DIRS}"
fi
if [[ -n "${KVNODE_PIDS:-}" ]]; then
  IFS=',' read -r -a PIDS_FROM_ENV <<< "${KVNODE_PIDS}"
fi
if [[ -n "${KVNODE_PID_FILES:-}" ]]; then
  IFS=',' read -r -a PID_FILES <<< "${KVNODE_PID_FILES}"
fi

if [[ "${#CLIENT_URLS[@]}" -eq 0 || -z "${CLIENT_URLS[0]}" ]]; then
  echo "KVNODE_CLIENT_URLS must contain at least one URL" >&2
  exit 2
fi
if [[ "${#ADMIN_URLS[@]}" -eq 0 || -z "${ADMIN_URLS[0]}" ]]; then
  echo "KVNODE_ADMIN_URLS must contain at least one URL" >&2
  exit 2
fi

for i in "${!CLIENT_URLS[@]}"; do
  CLIENT_URLS[$i]="$(trim_trailing_slash "${CLIENT_URLS[$i]}")"
done
for i in "${!ADMIN_URLS[@]}"; do
  ADMIN_URLS[$i]="$(trim_trailing_slash "${ADMIN_URLS[$i]}")"
done
for value_size in "${VALUE_SIZES[@]}"; do
  bounded_int KVNODE_CAPACITY_VALUE_BYTES "$value_size" "$max_value_bytes"
done
for scan_limit in "${SCAN_LIMITS[@]}"; do
  bounded_int KVNODE_CAPACITY_SCAN_LIMITS "$scan_limit" "$max_scan_limit"
done

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${KVNODE_CAPACITY_OUT_DIR:-${TMPDIR:-/tmp}/kvnode-capacity-envelope-${run_id}}"
mkdir -p "$out_dir"

LATENCY_CSV="$out_dir/latency.csv"
RESOURCES_CSV="$out_dir/resources.csv"
SUMMARY_MD="$out_dir/summary.md"
METADATA_ENV="$out_dir/metadata.env"
RUN_PREFIX="kvnode-capacity-${run_id}-$$"

printf 'operation,http_status,seconds\n' > "$LATENCY_CSV"
printf 'sample,resource,target,value,unit\n' > "$RESOURCES_CSV"

peer_count="${KVNODE_PEER_COUNT:-${#CLIENT_URLS[@]}}"
cat > "$METADATA_ENV" <<EOF
status=harness-only
run_id=$run_id
client_urls=$client_urls_raw
admin_urls=$admin_urls_raw
ops_per_phase=$ops_per_phase
value_bytes=$value_sizes_raw
scan_limits=$scan_limits_raw
peer_count=$peer_count
out_dir=$out_dir
EOF

all_pids=("")
for pid in "${PIDS_FROM_ENV[@]}"; do
  if [[ -n "$pid" ]]; then
    all_pids+=("$pid")
  fi
done
for pid_file in "${PID_FILES[@]}"; do
  if [[ -n "$pid_file" && -f "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if [[ -n "$pid" ]]; then
      all_pids+=("$pid")
    fi
  fi
done

make_value_file() {
  local size="$1"
  local file="$2"
  awk -v n="$size" 'BEGIN { for (i = 0; i < n; i++) printf "x" }' > "$file"
}

record_resources() {
  local sample="$1"
  local admin_url metrics_file queue_depth
  for pid in "${all_pids[@]}"; do
    if [[ -n "$pid" ]]; then
      rss_kib="$(ps -o rss= -p "$pid" 2>/dev/null | awk '{print $1}' || true)"
      if [[ -n "$rss_kib" ]]; then
        printf '%s,rss_kib,pid:%s,%s,KiB\n' "$sample" "$pid" "$rss_kib" >> "$RESOURCES_CSV"
      fi
    fi
  done
  for data_dir in "${DATA_DIRS[@]}"; do
    if [[ -n "$data_dir" && -d "$data_dir" ]]; then
      disk_kib="$(du -sk "$data_dir" | awk '{print $1}')"
      printf '%s,disk_kib,%s,%s,KiB\n' "$sample" "$data_dir" "$disk_kib" >> "$RESOURCES_CSV"
    fi
  done
  for admin_url in "${ADMIN_URLS[@]}"; do
    metrics_file="$out_dir/metrics-${sample}-$(printf '%s' "$admin_url" | tr '/:' '__').txt"
    if curl -fsS --max-time "$timeout_seconds" "$admin_url/metrics" > "$metrics_file"; then
      queue_depth="$(awk '/^kvnode_send_queue_depth / { print $2; found = 1 } END { if (!found) print "missing" }' "$metrics_file")"
      printf '%s,send_queue_depth,%s,%s,count\n' "$sample" "$admin_url" "$queue_depth" >> "$RESOURCES_CSV"
    else
      printf '%s,send_queue_depth,%s,metrics_unavailable,count\n' "$sample" "$admin_url" >> "$RESOURCES_CSV"
    fi
  done
}

record_curl() {
  local operation="$1"
  shift
  local result http_status seconds
  result="$(curl -sS -o /dev/null -w '%{http_code} %{time_total}' --max-time "$timeout_seconds" "$@")"
  http_status="${result%% *}"
  seconds="${result#* }"
  printf '%s,%s,%s\n' "$operation" "$http_status" "$seconds" >> "$LATENCY_CSV"
  if (( http_status < 200 || http_status >= 300 )); then
    echo "operation $operation returned HTTP $http_status" >&2
    exit 1
  fi
}

client_for_index() {
  local index="$1"
  printf '%s' "${CLIENT_URLS[$(( index % ${#CLIENT_URLS[@]} ))]}"
}

record_resources before
start_epoch="$(date +%s)"

op_index=0
for value_size in "${VALUE_SIZES[@]}"; do
  value_file="$out_dir/value-${value_size}.bin"
  make_value_file "$value_size" "$value_file"
  for ((i = 0; i < ops_per_phase; i++)); do
    client_url="$(client_for_index "$op_index")"
    key="${RUN_PREFIX}-v${value_size}-${i}"
    record_curl "put_value_bytes_${value_size}" -X PUT --data-binary "@$value_file" "$client_url/kv/$key"
    record_curl "get_value_bytes_${value_size}" "$client_url/kv/$key"
    op_index=$((op_index + 1))
  done
done

for scan_limit in "${SCAN_LIMITS[@]}"; do
  client_url="$(client_for_index "$op_index")"
  record_curl "scan_limit_${scan_limit}" "$client_url/scan?prefix=${RUN_PREFIX}-&limit=${scan_limit}"
  op_index=$((op_index + 1))
done

end_epoch="$(date +%s)"
record_resources after

latency_summary="$(awk -F, 'NR > 1 { print $3 }' "$LATENCY_CSV" | sort -n | awk '
  { values[NR] = $1; sum += $1 }
  END {
    if (NR == 0) {
      print "latency_samples=0"
      exit
    }
    p50 = int(NR * 0.50); if (p50 < 1) p50 = 1
    p95 = int(NR * 0.95); if (p95 < 1) p95 = 1
    p99 = int(NR * 0.99); if (p99 < 1) p99 = 1
    printf "latency_samples=%d\n", NR
    printf "latency_avg_seconds=%.6f\n", sum / NR
    printf "latency_p50_seconds=%.6f\n", values[p50]
    printf "latency_p95_seconds=%.6f\n", values[p95]
    printf "latency_p99_seconds=%.6f\n", values[p99]
  }
')"

operation_count="$(( $(wc -l < "$LATENCY_CSV") - 1 ))"
elapsed_seconds="$(( end_epoch - start_epoch ))"
if (( elapsed_seconds <= 0 )); then
  elapsed_seconds=1
fi
throughput="$(awk -v ops="$operation_count" -v seconds="$elapsed_seconds" 'BEGIN { printf "%.3f", ops / seconds }')"

cat > "$SUMMARY_MD" <<EOF
# kvnode capacity-envelope harness sample

Status: harness output only. These numbers are samples from the requested run and are not production capacity evidence unless the environment, workload, peer count, and operator procedure have been separately approved and recorded.

## Inputs

- Run ID: $run_id
- Client URLs: $client_urls_raw
- Admin URLs: $admin_urls_raw
- Peer-count label: $peer_count
- Operations per value-size phase: $ops_per_phase
- Value sizes requested: $value_sizes_raw bytes
- Scan limits requested: $scan_limits_raw
- Per-request timeout: ${timeout_seconds}s

## Measurements collected

- Throughput sample: $throughput operations/second over $operation_count HTTP operations and ${elapsed_seconds}s elapsed wall time.
$(printf '%s\n' "$latency_summary" | sed 's/^/- /')
- Memory RSS samples: see resources.csv rows with resource=rss_kib. Empty when KVNODE_PIDS/KVNODE_PID_FILES were not provided.
- Disk growth samples: see resources.csv rows with resource=disk_kib. Empty when KVNODE_DATA_DIRS was not provided.
- Queue-depth samples: see resources.csv rows with resource=send_queue_depth and metrics snapshots in this output directory.
- Value-size coverage: see latency.csv operation labels put_value_bytes_* and get_value_bytes_*.
- Scan-limit coverage: see latency.csv operation labels scan_limit_*.
- Peer-count coverage: recorded as the peer-count label above; run separate, isolated clusters for each peer-count envelope candidate.

## Raw files

- metadata.env
- latency.csv
- resources.csv
- metrics-before-*.txt and metrics-after-*.txt when admin metrics were reachable
EOF

printf 'kvnode capacity-envelope harness output: %s\n' "$out_dir"
