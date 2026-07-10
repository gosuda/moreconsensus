#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"
HARNESS="tests/kvnode_capacity_envelope.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/kvnode-capacity-schema-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "kvnode-capacity-self-test status=fail reason=$*" >&2
  exit 1
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

expect_failure() {
  local label="$1"
  shift
  if "$@" >"$TMP/$label.stdout" 2>"$TMP/$label.stderr"; then
    fail "$label-was-accepted"
  fi
}

bash -n "$HARNESS"

dry_output="$(bash "$HARNESS" --dry-run)"
[[ "$dry_output" == *"status=dry-run-schema-valid"* ]] || fail "local-dry-run-did-not-validate"
[[ "$dry_output" == *"release_claim=none-dry-run-only"* ]] || fail "local-dry-run-made-a-claim"
[[ "$dry_output" == *"network_requests=0"* ]] || fail "local-dry-run-did-not-remain-offline"
expect_failure malformed-budget env KVNODE_CAPACITY_WARMUP_OPS=0 bash "$HARNESS" --dry-run
expect_failure malformed-concurrency env KVNODE_CAPACITY_CONCURRENT_CONCURRENCY=65 bash "$HARNESS" --dry-run

binary_path="$ROOT/$HARNESS"
binary_hash="$(sha256_file "$binary_path")"

set_target_dry_run_environment() {
  export KVNODE_CAPACITY_MODE=target
  export KVNODE_CAPACITY_WARMUP_OPS=1
  export KVNODE_CAPACITY_STEADY_OPS=1
  export KVNODE_CAPACITY_CONCURRENT_OPS=1
  export KVNODE_CAPACITY_SATURATION_OPS=1
  export KVNODE_CAPACITY_RECOVERY_OPS=1
  export KVNODE_CAPACITY_WARMUP_CONCURRENCY=1
  export KVNODE_CAPACITY_STEADY_CONCURRENCY=1
  export KVNODE_CAPACITY_CONCURRENT_CONCURRENCY=1
  export KVNODE_CAPACITY_SATURATION_CONCURRENCY=1
  export KVNODE_CAPACITY_RECOVERY_CONCURRENCY=1
  export KVNODE_CAPACITY_RECOVERY_WAIT_SECONDS=1
  export KVNODE_CAPACITY_TARGET=fixture-target
  export KVNODE_CAPACITY_RELEASE_ID=fixture-release
  export KVNODE_CAPACITY_SOURCE_REVISION=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  export KVNODE_CAPACITY_BINARY_PATH="$binary_path"
  export KVNODE_CAPACITY_BINARY_SHA256="$binary_hash"
  export KVNODE_CAPACITY_ENVIRONMENT_NAME=fixture-environment
  export KVNODE_CAPACITY_ENVIRONMENT_LABEL=fixture-environment-label
  export KVNODE_CAPACITY_HARDWARE_DETAILS=fixture-hardware
  export KVNODE_CAPACITY_STORAGE_DETAILS=fixture-storage
  export KVNODE_CAPACITY_NETWORK_DETAILS=fixture-network
  export KVNODE_CAPACITY_TOPOLOGY_DETAILS=fixture-topology
  export KVNODE_CAPACITY_WORKLOAD_LABEL=fixture-workload
  export KVNODE_CAPACITY_PEER_COUNT=1
  export KVNODE_CAPACITY_OPERATOR=fixture-operator
  export KVNODE_CAPACITY_REVIEWER=fixture-reviewer
  export KVNODE_CAPACITY_RELEASE_CLAIM=target-capacity-accepted:fixture-only
  export KVNODE_CAPACITY_MIN_THROUGHPUT_OPS_PER_SECOND=1
  export KVNODE_CAPACITY_MAX_ERROR_RATE_PERCENT=10
  export KVNODE_CAPACITY_MAX_P99_LATENCY_SECONDS=1
  export KVNODE_CAPACITY_MAX_RSS_KIB=1000
  export KVNODE_CAPACITY_MAX_DISK_KIB=1000
  export KVNODE_CAPACITY_MAX_FDS=100
  export KVNODE_CAPACITY_MAX_QUEUE_DEPTH=100
  export KVNODE_CAPACITY_MIN_HEADROOM_PERCENT=10
  export KVNODE_PIDS="$$"
  export KVNODE_DATA_DIRS="$TMP"
}

target_dry_output="$({ set_target_dry_run_environment; bash "$HARNESS" --dry-run; })"
[[ "$target_dry_output" == *"status=dry-run-schema-valid"* ]] || fail "target-dry-run-did-not-validate"
[[ "$target_dry_output" == *"release_claim=none-dry-run-only"* ]] || fail "target-dry-run-made-a-claim"
missing_target_storage() {
  set_target_dry_run_environment
  unset KVNODE_CAPACITY_STORAGE_DETAILS
  bash "$HARNESS" --dry-run
}
expect_failure missing-target-storage missing_target_storage
missing_target_pids() {
  set_target_dry_run_environment
  unset KVNODE_PIDS
  bash "$HARNESS" --dry-run
}
expect_failure missing-target-pids missing_target_pids

missing_target_data_dirs() {
  set_target_dry_run_environment
  unset KVNODE_DATA_DIRS
  bash "$HARNESS" --dry-run
}
expect_failure missing-target-data-dirs missing_target_data_dirs

missing_target_threshold() {
  set_target_dry_run_environment
  unset KVNODE_CAPACITY_MAX_FDS
  bash "$HARNESS" --dry-run
}
expect_failure missing-target-threshold missing_target_threshold

missing_target_report() {
  set_target_dry_run_environment
  unset KVNODE_CAPACITY_REPORT
  bash "$HARNESS"
}
expect_failure missing-target-report missing_target_report

metadata="$TMP/metadata.env"
latency="$TMP/latency.csv"
resources="$TMP/resources.csv"
phase_summary="$TMP/phase_summary.csv"
checksums="$TMP/checksums.sha256"
report="$TMP/report.env"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
revision=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

cat > "$metadata" <<EOF
status=schema-fixture
target_name=fixture-target
source_revision=$revision
EOF

cat > "$latency" <<'EOF'
phase,worker,operation,http_status,seconds,outcome
warmup,1,put_value_bytes_64,200,0.100000,success
steady,1,put_value_bytes_64,200,0.100000,success
concurrent,1,put_value_bytes_64,200,0.100000,success
saturation,1,put_value_bytes_64,200,0.100000,success
recovery,1,put_value_bytes_64,200,0.100000,success
EOF

printf 'phase,sample,resource,target,value,unit,status\n' > "$resources"
for phase in warmup steady concurrent saturation recovery; do
  for sample in before after; do
    printf '%s,%s,rss_kib,pid:%s,100,KiB,ok\n' "$phase" "$sample" "$$" >> "$resources"
    printf '%s,%s,disk_kib,%s,100,KiB,ok\n' "$phase" "$sample" "$TMP" >> "$resources"
    printf '%s,%s,fd_count,pid:%s,10,count,ok\n' "$phase" "$sample" "$$" >> "$resources"
    printf '%s,%s,send_queue_depth,http://fixture.invalid,1,count,ok\n' "$phase" "$sample" >> "$resources"
  done
done

cat > "$phase_summary" <<'EOF'
phase,planned_operations,concurrency,attempted,successes,errors,error_rate_percent,elapsed_seconds,throughput_ops_per_second,latency_avg_seconds,latency_p50_seconds,latency_p95_seconds,latency_p99_seconds
warmup,1,1,1,1,0,0.000000,1,1.000000,0.100000,0.100000,0.100000,0.100000
steady,1,1,1,1,0,0.000000,1,1.000000,0.100000,0.100000,0.100000,0.100000
concurrent,1,1,1,1,0,0.000000,1,1.000000,0.100000,0.100000,0.100000,0.100000
saturation,1,1,1,1,0,0.000000,1,1.000000,0.100000,0.100000,0.100000,0.100000
recovery,1,1,1,1,0,0.000000,1,1.000000,0.100000,0.100000,0.100000,0.100000
EOF

metadata_hash="$(sha256_file "$metadata")"
latency_hash="$(sha256_file "$latency")"
resources_hash="$(sha256_file "$resources")"
phase_summary_hash="$(sha256_file "$phase_summary")"
cat > "$checksums" <<EOF
$metadata_hash  $metadata
$latency_hash  $latency
$resources_hash  $resources
$phase_summary_hash  $phase_summary
EOF
checksums_hash="$(sha256_file "$checksums")"

cat > "$report" <<EOF
status=schema-fixture
harness=tests/kvnode_capacity_envelope.sh
claim_scope=capacity-envelope-only-not-release-authorization
release_claim=none-schema-validation-only
acceptance_result=schema-valid-not-evidence
target_name=fixture-target
release_id=fixture-release
environment_name=fixture-environment
environment_label=fixture-environment-label
hardware_details=fixture-hardware
storage_details=fixture-storage
network_details=fixture-network
topology_details=fixture-topology
workload_label=fixture-workload
peer_count=1
pids=$$
data_dirs=$TMP
operator=fixture-operator
reviewer=fixture-reviewer
source_revision=$revision
binary_path=$binary_path
binary_sha256=$binary_hash
started_utc=$now
ended_utc=$now
phase_warmup_operations=1
phase_warmup_concurrency=1
phase_steady_operations=1
phase_steady_concurrency=1
phase_concurrent_operations=1
phase_concurrent_concurrency=1
phase_saturation_operations=1
phase_saturation_concurrency=1
phase_recovery_operations=1
phase_recovery_concurrency=1
operation_count=5
elapsed_seconds=1
success_count=5
error_count=0
error_rate_percent=0.000000
throughput_ops_per_second=5.000000
latency_p99_seconds=0.100000
max_rss_kib=100
max_disk_kib=100
max_fds=10
max_queue_depth=1
threshold_min_throughput_ops_per_second=1
threshold_max_error_rate_percent=10
threshold_max_p99_latency_seconds=1
threshold_max_rss_kib=1000
threshold_max_disk_kib=1000
threshold_max_fds=100
threshold_max_queue_depth=100
threshold_min_headroom_percent=10
threshold_results=throughput:pass,error_rate:pass,p99_latency:pass,rss:pass,disk:pass,fds:pass,queue_depth:pass,headroom:pass
resource_types=rss_kib,disk_kib,fd_count,send_queue_depth
checksums_path=$checksums
checksums_sha256=$checksums_hash
artifact_count=4
artifact_1_kind=metadata
artifact_1_path=$metadata
artifact_1_sha256=$metadata_hash
artifact_2_kind=latency
artifact_2_path=$latency
artifact_2_sha256=$latency_hash
artifact_3_kind=resources
artifact_3_path=$resources
artifact_3_sha256=$resources_hash
artifact_4_kind=phase-summary
artifact_4_path=$phase_summary
artifact_4_sha256=$phase_summary_hash
EOF

fixture_output="$(bash "$HARNESS" --validate-fixture "$report")"
[[ "$fixture_output" == *"report-validation=pass mode=fixture"* ]] || fail "valid-schema-fixture-was-not-validated"
expect_failure fixture-cannot-be-target bash "$HARNESS" --validate-report "$report"

cp "$report" "$TMP/duplicate.env"
printf 'status=schema-fixture\n' >> "$TMP/duplicate.env"
expect_failure duplicate-field bash "$HARNESS" --validate-fixture "$TMP/duplicate.env"

cp "$report" "$TMP/malformed.env"
printf 'malformed-without-equals\n' >> "$TMP/malformed.env"
expect_failure malformed-report bash "$HARNESS" --validate-fixture "$TMP/malformed.env"
awk '
  /^started_utc=/ { print "started_utc=2000-01-01T00:00:00Z"; next }
  /^ended_utc=/ { print "ended_utc=2000-01-01T00:00:01Z"; next }
  /^elapsed_seconds=/ { print "elapsed_seconds=1"; next }
  { print }
' "$report" > "$TMP/stale.env"
expect_failure stale-report bash "$HARNESS" --validate-fixture "$TMP/stale.env"

printf 'warmup,1,tampered,200,0.100000,success\n' >> "$latency"
expect_failure tampered-raw-artifact bash "$HARNESS" --validate-fixture "$report"

echo "kvnode-capacity-self-test status=pass claim=none-schema-validation-only"
