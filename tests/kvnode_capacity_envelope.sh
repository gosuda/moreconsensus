#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

usage() {
  cat <<'USAGE'
kvnode capacity-envelope harness (opt-in, bounded)

Status: harness only. Local mode gathers bounded samples from an already-running
cluster and is not production capacity evidence. Target mode produces a scoped
capacity-envelope claim only after provenance, resources, raw checksums, and all
predeclared thresholds validate. Neither mode authorizes a release by itself.

Commands:
  tests/kvnode_capacity_envelope.sh --dry-run
  tests/kvnode_capacity_envelope.sh --validate-report REPORT
  tests/kvnode_capacity_envelope.sh --validate-fixture REPORT
  tests/kvnode_capacity_envelope.sh --collect-darwin-v2 CONFIG OUT_DIR
  tests/kvnode_capacity_envelope.sh --assemble-darwin-v2 MEASUREMENT REVIEW REPORT
  tests/kvnode_capacity_envelope.sh --validate-darwin-v2 REPORT

Required opt-in for a live run:
  KVNODE_CAPACITY_RUN=yes

Common inputs:
  KVNODE_CAPACITY_MODE             local (default) or target.
  KVNODE_CLIENT_URLS              Comma-separated client base URLs.
                                  Default: http://127.0.0.1:8080
  KVNODE_ADMIN_URLS               Comma-separated admin base URLs for /metrics.
                                  Default: http://127.0.0.1:8082
  KVNODE_CAPACITY_OPS_PER_PHASE   Legacy bounded default for every phase.
                                  Default: 30, max: 1000
  KVNODE_CAPACITY_WARMUP_OPS      Warm-up operation budget.
  KVNODE_CAPACITY_STEADY_OPS      Sustained steady-state operation budget.
  KVNODE_CAPACITY_CONCURRENT_OPS  Concurrent-load operation budget.
  KVNODE_CAPACITY_SATURATION_OPS  Saturation/failure operation budget.
  KVNODE_CAPACITY_RECOVERY_OPS    Post-saturation recovery operation budget.
  KVNODE_CAPACITY_WARMUP_CONCURRENCY      Default: 1, max: 64
  KVNODE_CAPACITY_STEADY_CONCURRENCY      Default: 1, max: 64
  KVNODE_CAPACITY_CONCURRENT_CONCURRENCY  Default: 4, max: 64
  KVNODE_CAPACITY_SATURATION_CONCURRENCY  Default: 8, max: 64
  KVNODE_CAPACITY_RECOVERY_CONCURRENCY    Default: 1, max: 64
  KVNODE_CAPACITY_RECOVERY_WAIT_SECONDS   Bounded wait before recovery. Default: 1
  KVNODE_CAPACITY_VALUE_BYTES     Comma-separated value sizes. Default: 64,1024,4096
  KVNODE_CAPACITY_SCAN_LIMITS     Comma-separated scan limits. Default: 1,16,128
  KVNODE_CAPACITY_OUT_DIR         Output directory.
  KVNODE_CAPACITY_TIMEOUT_SECONDS curl per-request timeout. Default: 5
  KVNODE_CAPACITY_ENVIRONMENT_LABEL       Single-line environment label.
  KVNODE_CAPACITY_WORKLOAD_LABEL          Single-line workload label.
  KVNODE_CAPACITY_REPORT         Optional 0600 report path.

Resource inputs:
  KVNODE_PIDS                    Comma-separated kvnode PIDs.
  KVNODE_PID_FILES               Comma-separated files containing kvnode PIDs.
  KVNODE_DATA_DIRS               Comma-separated kvnode data directories.
  KVNODE_PEER_COUNT              Expected peer count label.

Target mode additionally requires explicit phase budgets and concurrency values,
PIDs, data directories, and all of the following provenance and thresholds:
  KVNODE_CAPACITY_TARGET
  KVNODE_CAPACITY_RELEASE_ID
  KVNODE_CAPACITY_SOURCE_REVISION          Full 40-64 digit hexadecimal revision.
  KVNODE_CAPACITY_BINARY_PATH
  KVNODE_CAPACITY_BINARY_SHA256
  KVNODE_CAPACITY_ENVIRONMENT_NAME
  KVNODE_CAPACITY_HARDWARE_DETAILS
  KVNODE_CAPACITY_STORAGE_DETAILS
  KVNODE_CAPACITY_NETWORK_DETAILS
  KVNODE_CAPACITY_TOPOLOGY_DETAILS
  KVNODE_CAPACITY_OPERATOR
  KVNODE_CAPACITY_REVIEWER                 Must differ from operator.
  KVNODE_CAPACITY_RELEASE_CLAIM            target-capacity-accepted:<scope>
  KVNODE_CAPACITY_MIN_THROUGHPUT_OPS_PER_SECOND
  KVNODE_CAPACITY_MAX_ERROR_RATE_PERCENT
  KVNODE_CAPACITY_MAX_P99_LATENCY_SECONDS
  KVNODE_CAPACITY_MAX_RSS_KIB
  KVNODE_CAPACITY_MAX_DISK_KIB
  KVNODE_CAPACITY_MAX_FDS
  KVNODE_CAPACITY_MAX_QUEUE_DEPTH
  KVNODE_CAPACITY_MIN_HEADROOM_PERCENT

Output:
  metadata.env                    Harness inputs and peer-count label.
  latency.csv                     operation,http_status,seconds rows. Phase and outcome columns are also recorded.
  resources.csv                   before/after RSS, disk, queue-depth samples. FD rows are also recorded.
  phase_summary.csv               Per-phase budgets, concurrency, rates, and latency tails.
  checksums.sha256                SHA-256 bindings for every raw artifact.
  summary.md                      Machine-generated sample summary with no readiness claim. Target summaries remain claim-scoped.
  report.env                     Optional local non-claim or validated target report.

Example local rehearsal:
  KVNODE_CAPACITY_RUN=yes \
  KVNODE_CLIENT_URLS=http://127.0.0.1:8080 \
  KVNODE_ADMIN_URLS=http://127.0.0.1:8082 \
  tests/kvnode_capacity_envelope.sh
USAGE
}

fail() {
  echo "kvnode-capacity status=fail reason=$*" >&2
  exit 2
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "missing-required-command-$1"
  fi
}

positive_int() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( 10#$value <= 0 )); then
    fail "$name-must-be-positive-integer"
  fi
}

bounded_int() {
  local name="$1"
  local value="$2"
  local max="$3"
  positive_int "$name" "$value"
  if (( 10#$value > max )); then
    fail "$name-must-be-at-most-$max"
  fi
}

nonnegative_decimal() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^([0-9]+)([.][0-9]+)?$ ]]; then
    fail "$name-must-be-a-nonnegative-decimal"
  fi
}

positive_decimal() {
  local name="$1"
  local value="$2"
  nonnegative_decimal "$name" "$value"
  if ! awk -v value="$value" 'BEGIN { exit !(value > 0) }'; then
    fail "$name-must-be-greater-than-zero"
  fi
}

percent_decimal() {
  local name="$1"
  local value="$2"
  local allow_zero="${3:-yes}"
  nonnegative_decimal "$name" "$value"
  if ! awk -v value="$value" -v allow_zero="$allow_zero" 'BEGIN {
    if (value > 100) exit 1
    if (allow_zero == "no" && value <= 0) exit 1
  }'; then
    fail "$name-must-be-in-range"
  fi
}

label_value() {
  local name="$1"
  local value="$2"
  local max="${3:-128}"
  if [[ -z "$value" ]]; then
    fail "$name-must-not-be-empty"
  fi
  if [[ "$value" == *$'\n'* || "$value" == *$'\r'* || "$value" == *"="* ]]; then
    fail "$name-must-be-a-single-line-without-equals"
  fi
  if (( ${#value} > max )); then
    fail "$name-must-be-at-most-$max-characters"
  fi
  printf '%s' "$value"
}
normalize_hash() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}


validate_report_path() {
  local name="$1"
  local value="$2"
  [[ -n "$value" ]] || return 0
  if [[ "$value" == "." || "$value" == "/" || "$value" == */ ]]; then
    echo "$name must name a file" >&2
    exit 2
  fi
}

trim_trailing_slash() {
  local raw="$1"
  raw="${raw%/}"
  printf '%s' "$raw"
}

sha256_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    fail "missing-required-command-sha256sum-or-shasum"
  fi
}

timestamp_epoch() {
  local value="$1"
  local epoch=""
  if epoch="$(date -u -d "$value" +%s 2>/dev/null)"; then
    printf '%s' "$epoch"
    return 0
  fi
  if epoch="$(date -j -u -f '%Y-%m-%dT%H:%M:%SZ' "$value" +%s 2>/dev/null)"; then
    printf '%s' "$epoch"
    return 0
  fi
  return 1
}

REPORT_KEYS=("")
REPORT_VALUES=("")

load_report() {
  local report="$1"
  local line key value existing count
  [[ -f "$report" ]] || fail "report-not-found"
  REPORT_KEYS=("")
  REPORT_VALUES=("")
  count=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    count=$((count + 1))
    if (( count > 1024 )); then
      fail "report-has-too-many-fields"
    fi
    if [[ ! "$line" =~ ^([A-Za-z][A-Za-z0-9_]*)=(.+)$ ]]; then
      fail "report-line-$count-is-malformed"
    fi
    key="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"
    if [[ "$value" == *$'\r'* ]]; then
      fail "report-field-$key-contains-carriage-return"
    fi
    for existing in "${REPORT_KEYS[@]}"; do
      if [[ "$existing" == "$key" ]]; then
        fail "report-field-$key-is-duplicate"
      fi
    done
    REPORT_KEYS+=("$key")
    REPORT_VALUES+=("$value")
  done < "$report"
  if (( count == 0 )); then
    fail "report-is-empty"
  fi
}

report_value() {
  local wanted="$1"
  local index
  for index in "${!REPORT_KEYS[@]}"; do
    if [[ "${REPORT_KEYS[$index]}" == "$wanted" ]]; then
      printf '%s' "${REPORT_VALUES[$index]}"
      return 0
    fi
  done
  return 1
}

required_report_value() {
  local key="$1"
  local value
  if ! value="$(report_value "$key")" || [[ -z "$value" ]]; then
    fail "report-field-$key-is-required"
  fi
  printf '%s' "$value"
}

assert_report_equals() {
  local key="$1"
  local expected="$2"
  local actual
  actual="$(required_report_value "$key")"
  if [[ "$actual" != "$expected" ]]; then
    fail "report-field-$key-must-equal-$expected"
  fi
}

manifest_contains() {
  local manifest="$1"
  local checksum="$2"
  local path="$3"
  awk -v wanted="$checksum  $path" '$0 == wanted { found = 1 } END { exit !found }' "$manifest"
}

ceiling_with_headroom_passes() {
  local actual="$1"
  local maximum="$2"
  local headroom="$3"
  awk -v actual="$actual" -v maximum="$maximum" -v headroom="$headroom" 'BEGIN {
    limit = maximum * (1 - headroom / 100)
    exit !(actual <= limit + 0.0000005)
  }'
}

floor_with_headroom_passes() {
  local actual="$1"
  local minimum="$2"
  local headroom="$3"
  awk -v actual="$actual" -v minimum="$minimum" -v headroom="$headroom" 'BEGIN {
    limit = minimum * (1 + headroom / 100)
    exit !(actual + 0.0000005 >= limit)
  }'
}

validate_report() {
  local report="$1"
  local expected_mode="$2"
  local status claim acceptance start_utc end_utc start_epoch end_epoch now_epoch max_age
  local source_revision binary_path binary_hash computed_hash operator reviewer
  local operations successes errors reported_error recomputed_error
  local throughput error_rate p99 max_rss max_disk max_fds max_queue
  local min_throughput max_error max_p99 threshold_rss threshold_disk threshold_fds threshold_queue headroom
  local artifact_count artifact_index artifact_kind artifact_path artifact_hash prior_index prior_path
  local checksums_path checksums_hash expected_thresholds
  local seen_metadata=0 seen_latency=0 seen_resources=0 seen_phase_summary=0

  load_report "$report"
  assert_report_equals harness tests/kvnode_capacity_envelope.sh
  assert_report_equals claim_scope capacity-envelope-only-not-release-authorization

  status="$(required_report_value status)"
  claim="$(required_report_value release_claim)"
  acceptance="$(required_report_value acceptance_result)"
  if [[ "$expected_mode" == "target" ]]; then
    [[ "$status" == "target-capacity-evidence" ]] || fail "report-status-is-not-target-capacity-evidence"
    [[ "$claim" == target-capacity-accepted:* && "$claim" != *none* ]] || fail "report-release-claim-is-not-a-scoped-target-claim"
    [[ "$acceptance" == "pass" ]] || fail "report-acceptance-result-is-not-pass"
  else
    [[ "$status" == "schema-fixture" ]] || fail "fixture-status-is-invalid"
    [[ "$claim" == "none-schema-validation-only" ]] || fail "fixture-release-claim-is-invalid"
    [[ "$acceptance" == "schema-valid-not-evidence" ]] || fail "fixture-acceptance-result-is-invalid"
  fi

  for artifact_kind in target_name release_id environment_name environment_label hardware_details storage_details network_details topology_details workload_label peer_count pids data_dirs operator reviewer; do
    label_value "report-field-$artifact_kind" "$(required_report_value "$artifact_kind")" 512 >/dev/null
  done

  operator="$(required_report_value operator)"
  reviewer="$(required_report_value reviewer)"
  [[ "$operator" != "$reviewer" ]] || fail "report-operator-and-reviewer-must-differ"

  source_revision="$(required_report_value source_revision)"
  [[ "$source_revision" =~ ^[0-9a-fA-F]{40,64}$ ]] || fail "report-source-revision-is-not-immutable"
  binary_path="$(required_report_value binary_path)"
  binary_hash="$(required_report_value binary_sha256)"
  [[ "$binary_hash" =~ ^[0-9a-fA-F]{64}$ ]] || fail "report-binary-sha256-is-malformed"
  [[ -f "$binary_path" ]] || fail "report-binary-path-is-not-a-file"
  computed_hash="$(sha256_file "$binary_path")"
  [[ "$(normalize_hash "$computed_hash")" == "$(normalize_hash "$binary_hash")" ]] || fail "report-binary-sha256-does-not-match"

  start_utc="$(required_report_value started_utc)"
  end_utc="$(required_report_value ended_utc)"
  [[ "$start_utc" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || fail "report-started-utc-is-malformed"
  [[ "$end_utc" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || fail "report-ended-utc-is-malformed"
  start_epoch="$(timestamp_epoch "$start_utc")" || fail "report-started-utc-is-invalid"
  end_epoch="$(timestamp_epoch "$end_utc")" || fail "report-ended-utc-is-invalid"
  (( end_epoch >= start_epoch )) || fail "report-time-range-is-reversed"
  now_epoch="$(date -u +%s)"
  max_age="${KVNODE_CAPACITY_MAX_EVIDENCE_AGE_SECONDS:-86400}"
  bounded_int KVNODE_CAPACITY_MAX_EVIDENCE_AGE_SECONDS "$max_age" 604800
  (( end_epoch <= now_epoch + 300 )) || fail "report-ended-utc-is-in-the-future"
  (( now_epoch - end_epoch <= max_age )) || fail "report-is-stale"

  for artifact_kind in warmup steady concurrent saturation recovery; do
    bounded_int "report-phase-${artifact_kind}-operations" "$(required_report_value "phase_${artifact_kind}_operations")" 1000
    bounded_int "report-phase-${artifact_kind}-concurrency" "$(required_report_value "phase_${artifact_kind}_concurrency")" 64
  done

  operations="$(required_report_value operation_count)"
  successes="$(required_report_value success_count)"
  errors="$(required_report_value error_count)"
  positive_int report-operation-count "$operations"
  [[ "$successes" =~ ^[0-9]+$ ]] || fail "report-success-count-is-malformed"
  [[ "$errors" =~ ^[0-9]+$ ]] || fail "report-error-count-is-malformed"
  (( 10#$successes + 10#$errors == 10#$operations )) || fail "report-operation-accounting-does-not-balance"

  reported_error="$(required_report_value error_rate_percent)"
  nonnegative_decimal report-error-rate-percent "$reported_error"
  recomputed_error="$(awk -v errors="$errors" -v operations="$operations" 'BEGIN { printf "%.6f", 100 * errors / operations }')"
  awk -v a="$reported_error" -v b="$recomputed_error" 'BEGIN { d = a-b; if (d < 0) d = -d; exit !(d <= 0.000001) }' || fail "report-error-rate-does-not-match-counts"

  throughput="$(required_report_value throughput_ops_per_second)"
  error_rate="$reported_error"
  p99="$(required_report_value latency_p99_seconds)"
  max_rss="$(required_report_value max_rss_kib)"
  max_disk="$(required_report_value max_disk_kib)"
  max_fds="$(required_report_value max_fds)"
  max_queue="$(required_report_value max_queue_depth)"
  min_throughput="$(required_report_value threshold_min_throughput_ops_per_second)"
  max_error="$(required_report_value threshold_max_error_rate_percent)"
  max_p99="$(required_report_value threshold_max_p99_latency_seconds)"
  threshold_rss="$(required_report_value threshold_max_rss_kib)"
  threshold_disk="$(required_report_value threshold_max_disk_kib)"
  threshold_fds="$(required_report_value threshold_max_fds)"
  threshold_queue="$(required_report_value threshold_max_queue_depth)"
  headroom="$(required_report_value threshold_min_headroom_percent)"

  positive_decimal report-throughput "$throughput"
  percent_decimal report-error-rate "$error_rate" yes
  positive_decimal report-p99 "$p99"
  positive_decimal report-max-rss "$max_rss"
  positive_decimal report-max-disk "$max_disk"
  positive_decimal report-max-fds "$max_fds"
  nonnegative_decimal report-max-queue "$max_queue"
  positive_decimal report-threshold-min-throughput "$min_throughput"
  percent_decimal report-threshold-max-error "$max_error" no
  positive_decimal report-threshold-max-p99 "$max_p99"
  positive_decimal report-threshold-max-rss "$threshold_rss"
  positive_decimal report-threshold-max-disk "$threshold_disk"
  positive_decimal report-threshold-max-fds "$threshold_fds"
  positive_decimal report-threshold-max-queue "$threshold_queue"
  percent_decimal report-threshold-min-headroom "$headroom" no

  floor_with_headroom_passes "$throughput" "$min_throughput" "$headroom" || fail "report-throughput-threshold-failed"
  ceiling_with_headroom_passes "$error_rate" "$max_error" "$headroom" || fail "report-error-rate-threshold-failed"
  ceiling_with_headroom_passes "$p99" "$max_p99" "$headroom" || fail "report-p99-threshold-failed"
  ceiling_with_headroom_passes "$max_rss" "$threshold_rss" "$headroom" || fail "report-rss-threshold-failed"
  ceiling_with_headroom_passes "$max_disk" "$threshold_disk" "$headroom" || fail "report-disk-threshold-failed"
  ceiling_with_headroom_passes "$max_fds" "$threshold_fds" "$headroom" || fail "report-fds-threshold-failed"
  ceiling_with_headroom_passes "$max_queue" "$threshold_queue" "$headroom" || fail "report-queue-threshold-failed"
  expected_thresholds="throughput:pass,error_rate:pass,p99_latency:pass,rss:pass,disk:pass,fds:pass,queue_depth:pass,headroom:pass"
  assert_report_equals threshold_results "$expected_thresholds"

  checksums_path="$(required_report_value checksums_path)"
  checksums_hash="$(required_report_value checksums_sha256)"
  [[ "$checksums_hash" =~ ^[0-9a-fA-F]{64}$ ]] || fail "report-checksums-sha256-is-malformed"
  [[ -f "$checksums_path" ]] || fail "report-checksums-path-is-not-a-file"
  computed_hash="$(sha256_file "$checksums_path")"
  [[ "$(normalize_hash "$computed_hash")" == "$(normalize_hash "$checksums_hash")" ]] || fail "report-checksums-sha256-does-not-match"

  artifact_count="$(required_report_value artifact_count)"
  bounded_int report-artifact-count "$artifact_count" 256
  metadata_artifact_path=""
  latency_artifact_path=""
  resources_artifact_path=""
  phase_summary_artifact_path=""
  for ((artifact_index = 1; artifact_index <= artifact_count; artifact_index++)); do
    artifact_kind="$(required_report_value "artifact_${artifact_index}_kind")"
    artifact_path="$(required_report_value "artifact_${artifact_index}_path")"
    artifact_hash="$(required_report_value "artifact_${artifact_index}_sha256")"
    [[ "$artifact_kind" =~ ^[a-z][a-z0-9_-]*$ ]] || fail "report-artifact-$artifact_index-kind-is-malformed"
    [[ "$artifact_hash" =~ ^[0-9a-fA-F]{64}$ ]] || fail "report-artifact-$artifact_index-sha256-is-malformed"
    [[ -f "$artifact_path" ]] || fail "report-artifact-$artifact_index-is-not-a-file"
    for ((prior_index = 1; prior_index < artifact_index; prior_index++)); do
      prior_path="$(required_report_value "artifact_${prior_index}_path")"
      [[ "$prior_path" != "$artifact_path" ]] || fail "report-artifact-path-is-duplicate"
    done
    computed_hash="$(sha256_file "$artifact_path")"
    [[ "$(normalize_hash "$computed_hash")" == "$(normalize_hash "$artifact_hash")" ]] || fail "report-artifact-$artifact_index-sha256-does-not-match"
    manifest_contains "$checksums_path" "$artifact_hash" "$artifact_path" || fail "report-artifact-$artifact_index-is-missing-from-checksums"
    case "$artifact_kind" in
      metadata)
        seen_metadata=$((seen_metadata + 1))
        metadata_artifact_path="$artifact_path"
        ;;
      latency)
        seen_latency=$((seen_latency + 1))
        latency_artifact_path="$artifact_path"
        ;;
      resources)
        seen_resources=$((seen_resources + 1))
        resources_artifact_path="$artifact_path"
        ;;
      phase-summary)
        seen_phase_summary=$((seen_phase_summary + 1))
        phase_summary_artifact_path="$artifact_path"
        ;;
    esac
  done
  (( seen_metadata == 1 && seen_latency == 1 && seen_resources == 1 && seen_phase_summary == 1 )) || fail "report-required-raw-artifacts-must-occur-exactly-once"

  if [[ "$expected_mode" == "target" ]]; then
    awk -F= -v revision="$source_revision" -v target="$(required_report_value target_name)" -v claim="$claim" '
      $1 == "status" && $2 == "target-capacity-evidence" { status_found = 1 }
      $1 == "source_revision" && $2 == revision { revision_found = 1 }
      $1 == "target_name" && $2 == target { target_found = 1 }
      $1 == "release_claim" && $2 == claim { claim_found = 1 }
      $1 == "acceptance_result" && $2 == "pass" { acceptance_found = 1 }
      END { exit !(status_found && revision_found && target_found && claim_found && acceptance_found) }
    ' "$metadata_artifact_path" || fail "report-metadata-artifact-does-not-bind-accepted-target-claim"
  fi

  [[ "$(awk 'NR == 1 { print; exit }' "$latency_artifact_path")" == "phase,worker,operation,http_status,seconds,outcome" ]] || fail "report-latency-artifact-header-is-malformed"
  raw_accounting="$(awk -F, '
    NR == 1 { next }
    NF != 6 || $1 == "" || $2 !~ /^[0-9]+$/ || $3 == "" || $4 !~ /^[0-9][0-9][0-9]$/ || $5 !~ /^[0-9]+([.][0-9]+)?$/ { malformed = 1; next }
    $6 != "success" && $6 != "http_or_transport_error" && $6 != "transport_error" { malformed = 1; next }
    ($4 + 0 >= 200 && $4 + 0 < 300 && $6 != "success") || (($4 + 0 < 200 || $4 + 0 >= 300) && $6 == "success") { malformed = 1; next }
    { attempted++ }
    $6 == "success" { successes++ }
    END {
      if (malformed || attempted == 0) exit 1
      printf "%d,%d,%d", attempted, successes, attempted - successes
    }
  ' "$latency_artifact_path")" || fail "report-latency-artifact-contains-malformed-rows"
  [[ "$raw_accounting" == "$operations,$successes,$errors" ]] || fail "report-latency-artifact-accounting-does-not-match-report"
  raw_latency_values="${TMPDIR:-/tmp}/kvnode-capacity-validate-latency.$$.$RANDOM"
  awk -F, 'NR > 1 { print $5 }' "$latency_artifact_path" | sort -n > "$raw_latency_values"
  raw_p99="$(awk '{ values[NR] = $1 } END { rank = int((NR * 99 + 99) / 100); if (rank < 1) rank = 1; printf "%.6f", values[rank] }' "$raw_latency_values")"
  rm -f "$raw_latency_values"
  awk -v reported="$p99" -v raw="$raw_p99" 'BEGIN { d = reported - raw; if (d < 0) d = -d; exit !(d <= 0.000001) }' || fail "report-p99-does-not-match-latency-artifact"

  [[ "$(awk 'NR == 1 { print; exit }' "$resources_artifact_path")" == "phase,sample,resource,target,value,unit,status" ]] || fail "report-resources-artifact-header-is-malformed"
  awk -F, '
    NR == 1 { next }
    NF != 7 || $1 == "" || ($2 != "before" && $2 != "after") ||
      ($3 != "rss_kib" && $3 != "disk_kib" && $3 != "fd_count" && $3 != "send_queue_depth") ||
      $4 == "" || $5 !~ /^[0-9]+([.][0-9]+)?$/ || $6 == "" || $7 != "ok" { malformed = 1 }
    END { exit malformed }
  ' "$resources_artifact_path" || fail "report-resources-artifact-contains-missing-or-malformed-rows"
  for raw_resource in rss_kib disk_kib fd_count send_queue_depth; do
    for raw_phase in warmup steady concurrent saturation recovery; do
      for raw_sample in before after; do
        raw_count="$(awk -F, -v resource="$raw_resource" -v phase="$raw_phase" -v sample="$raw_sample" 'NR > 1 && $1 == phase && $2 == sample && $3 == resource { count++ } END { print count + 0 }' "$resources_artifact_path")"
        (( raw_count > 0 )) || fail "report-resources-artifact-is-missing-$raw_resource-$raw_phase-$raw_sample"
      done
    done
  done
  raw_max_rss="$(awk -F, 'NR > 1 && $3 == "rss_kib" { if (!found || $5 > max) max=$5; found=1 } END { if (found) print max }' "$resources_artifact_path")"
  raw_max_disk="$(awk -F, 'NR > 1 && $3 == "disk_kib" { if (!found || $5 > max) max=$5; found=1 } END { if (found) print max }' "$resources_artifact_path")"
  raw_max_fds="$(awk -F, 'NR > 1 && $3 == "fd_count" { if (!found || $5 > max) max=$5; found=1 } END { if (found) print max }' "$resources_artifact_path")"
  raw_max_queue="$(awk -F, 'NR > 1 && $3 == "send_queue_depth" { if (!found || $5 > max) max=$5; found=1 } END { if (found) print max }' "$resources_artifact_path")"
  [[ "$raw_max_rss" == "$max_rss" && "$raw_max_disk" == "$max_disk" && "$raw_max_fds" == "$max_fds" && "$raw_max_queue" == "$max_queue" ]] || fail "report-resource-maxima-do-not-match-resources-artifact"

  [[ "$(awk 'NR == 1 { print; exit }' "$phase_summary_artifact_path")" == "phase,planned_operations,concurrency,attempted,successes,errors,error_rate_percent,elapsed_seconds,throughput_ops_per_second,latency_avg_seconds,latency_p50_seconds,latency_p95_seconds,latency_p99_seconds" ]] || fail "report-phase-summary-header-is-malformed"
  [[ "$(wc -l < "$phase_summary_artifact_path" | awk '{print $1}')" == "6" ]] || fail "report-phase-summary-must-have-five-rows"
  phase_attempted_total=0
  for raw_phase in warmup steady concurrent saturation recovery; do
    raw_plan="$(required_report_value "phase_${raw_phase}_operations")"
    raw_concurrency="$(required_report_value "phase_${raw_phase}_concurrency")"
    raw_phase_row="$(awk -F, -v phase="$raw_phase" '$1 == phase { count++; row=$0 } END { if (count == 1) print row; else exit 1 }' "$phase_summary_artifact_path")" || fail "report-phase-summary-phase-$raw_phase-must-occur-once"
    raw_phase_fields="$(printf '%s\n' "$raw_phase_row" | awk -F, -v plan="$raw_plan" -v concurrency="$raw_concurrency" '
      NF != 13 || $2 != plan || $3 != concurrency || $4 !~ /^[0-9]+$/ || $5 !~ /^[0-9]+$/ || $6 !~ /^[0-9]+$/ ||
        $7 !~ /^[0-9]+([.][0-9]+)?$/ || $8 !~ /^[0-9]+$/ || $9 !~ /^[0-9]+([.][0-9]+)?$/ ||
        $10 !~ /^[0-9]+([.][0-9]+)?$/ || $11 !~ /^[0-9]+([.][0-9]+)?$/ ||
        $12 !~ /^[0-9]+([.][0-9]+)?$/ || $13 !~ /^[0-9]+([.][0-9]+)?$/ { exit 1 }
      { print $4 }
    ')" || fail "report-phase-summary-phase-$raw_phase-is-malformed"
    [[ "$raw_phase_fields" == "$raw_plan" ]] || fail "report-phase-summary-phase-$raw_phase-did-not-complete-budget"
    phase_attempted_total=$((phase_attempted_total + raw_phase_fields))
  done
  (( phase_attempted_total == operations )) || fail "report-phase-summary-operation-count-does-not-match"

  elapsed_report="$(required_report_value elapsed_seconds)"
  positive_int report-elapsed-seconds "$elapsed_report"
  expected_elapsed=$((end_epoch - start_epoch))
  (( expected_elapsed > 0 )) || expected_elapsed=1
  (( 10#$elapsed_report == expected_elapsed )) || fail "report-elapsed-seconds-does-not-match-time-range"
  raw_throughput="$(awk -v operations="$operations" -v elapsed="$elapsed_report" 'BEGIN { printf "%.6f", operations / elapsed }')"
  awk -v reported="$throughput" -v raw="$raw_throughput" 'BEGIN { d = reported - raw; if (d < 0) d = -d; exit !(d <= 0.000001) }' || fail "report-throughput-does-not-match-operation-count-and-time"

  assert_report_equals resource_types rss_kib,disk_kib,fd_count,send_queue_depth
  echo "kvnode-capacity report-validation=pass mode=$expected_mode report=$report"
}

command_mode="run"
command_report=""
case "${1:-}" in
  --help|-h)
    usage
    exit 0
    ;;
  --dry-run)
    command_mode="dry-run"
    shift
    ;;
  --validate-report)
    [[ $# -eq 2 ]] || fail "--validate-report-requires-one-path"
    command_mode="validate-target"
    command_report="$2"
    shift 2
    ;;
  --collect-darwin-v2)
    [[ $# -eq 3 ]] || fail "--collect-darwin-v2-requires-config-and-output-directory"
    exec python3 tests/kvnode_capacity_envelope_v2.py collect "$2" "$3"
    ;;
  --assemble-darwin-v2)
    [[ $# -eq 4 ]] || fail "--assemble-darwin-v2-requires-measurement-review-and-report"
    exec python3 tests/kvnode_capacity_envelope_v2.py assemble "$2" "$3" "$4"
    ;;
  --validate-darwin-v2)
    [[ $# -eq 2 ]] || fail "--validate-darwin-v2-requires-one-path"
    exec python3 tests/kvnode_capacity_envelope_v2.py verify "$2"
    ;;
  --validate-fixture)
    [[ $# -eq 2 ]] || fail "--validate-fixture-requires-one-path"
    command_mode="validate-fixture"
    command_report="$2"
    shift 2
    ;;
  "")
    ;;
  *)
    fail "unknown-argument-$1"
    ;;
esac
[[ $# -eq 0 ]] || fail "unexpected-arguments"

require_command awk
require_command date
if [[ "$command_mode" == validate-* ]]; then
  require_command sort
  require_command wc
  require_command tr
  require_command rm
fi
if [[ "$command_mode" == "validate-target" ]]; then
  if awk -F= '$1 == "schema_version" && $2 == "kvnode-capacity-evidence-v2" { found = 1 } END { exit !found }' "$command_report"; then
    require_command python3
    exec python3 tests/kvnode_capacity_envelope_v2.py verify "$command_report"
  fi
  validate_report "$command_report" target
  exit 0
fi
if [[ "$command_mode" == "validate-fixture" ]]; then
  validate_report "$command_report" fixture
  exit 0
fi

mode="${KVNODE_CAPACITY_MODE:-local}"
[[ "$mode" == "local" || "$mode" == "target" ]] || fail "KVNODE_CAPACITY_MODE-must-be-local-or-target"

client_urls_raw="${KVNODE_CLIENT_URLS:-http://127.0.0.1:8080}"
admin_urls_raw="${KVNODE_ADMIN_URLS:-http://127.0.0.1:8082}"
value_sizes_raw="${KVNODE_CAPACITY_VALUE_BYTES:-64,1024,4096}"
scan_limits_raw="${KVNODE_CAPACITY_SCAN_LIMITS:-1,16,128}"
ops_per_phase="${KVNODE_CAPACITY_OPS_PER_PHASE:-30}"
timeout_seconds="${KVNODE_CAPACITY_TIMEOUT_SECONDS:-5}"
max_value_bytes="${KVNODE_CAPACITY_MAX_VALUE_BYTES:-65536}"
max_scan_limit="${KVNODE_CAPACITY_MAX_SCAN_LIMIT:-10000}"
recovery_wait_seconds="${KVNODE_CAPACITY_RECOVERY_WAIT_SECONDS:-1}"
environment_label="$(label_value KVNODE_CAPACITY_ENVIRONMENT_LABEL "${KVNODE_CAPACITY_ENVIRONMENT_LABEL:-unspecified}")"
workload_label="$(label_value KVNODE_CAPACITY_WORKLOAD_LABEL "${KVNODE_CAPACITY_WORKLOAD_LABEL:-unspecified}")"
capacity_report="${KVNODE_CAPACITY_REPORT:-}"
validate_report_path KVNODE_CAPACITY_REPORT "$capacity_report"

warmup_ops="${KVNODE_CAPACITY_WARMUP_OPS:-$ops_per_phase}"
steady_ops="${KVNODE_CAPACITY_STEADY_OPS:-$ops_per_phase}"
concurrent_ops="${KVNODE_CAPACITY_CONCURRENT_OPS:-$ops_per_phase}"
saturation_ops="${KVNODE_CAPACITY_SATURATION_OPS:-$ops_per_phase}"
recovery_ops="${KVNODE_CAPACITY_RECOVERY_OPS:-$ops_per_phase}"
warmup_concurrency="${KVNODE_CAPACITY_WARMUP_CONCURRENCY:-1}"
steady_concurrency="${KVNODE_CAPACITY_STEADY_CONCURRENCY:-1}"
concurrent_concurrency="${KVNODE_CAPACITY_CONCURRENT_CONCURRENCY:-4}"
saturation_concurrency="${KVNODE_CAPACITY_SATURATION_CONCURRENCY:-8}"
recovery_concurrency="${KVNODE_CAPACITY_RECOVERY_CONCURRENCY:-1}"

bounded_int KVNODE_CAPACITY_OPS_PER_PHASE "$ops_per_phase" 1000
bounded_int KVNODE_CAPACITY_TIMEOUT_SECONDS "$timeout_seconds" 300
bounded_int KVNODE_CAPACITY_MAX_VALUE_BYTES "$max_value_bytes" 1048576
bounded_int KVNODE_CAPACITY_MAX_SCAN_LIMIT "$max_scan_limit" 100000
bounded_int KVNODE_CAPACITY_RECOVERY_WAIT_SECONDS "$recovery_wait_seconds" 300
for phase_budget in "$warmup_ops" "$steady_ops" "$concurrent_ops" "$saturation_ops" "$recovery_ops"; do
  bounded_int phase-operation-budget "$phase_budget" 1000
done
for phase_concurrency in "$warmup_concurrency" "$steady_concurrency" "$concurrent_concurrency" "$saturation_concurrency" "$recovery_concurrency"; do
  bounded_int phase-concurrency "$phase_concurrency" 64
done

IFS=',' read -r -a CLIENT_URLS <<< "$client_urls_raw"
IFS=',' read -r -a ADMIN_URLS <<< "$admin_urls_raw"
IFS=',' read -r -a VALUE_SIZES <<< "$value_sizes_raw"
IFS=',' read -r -a SCAN_LIMITS <<< "$scan_limits_raw"
DATA_DIRS=("")
PIDS_FROM_ENV=("")
PID_FILES=("")
[[ -z "${KVNODE_DATA_DIRS:-}" ]] || IFS=',' read -r -a DATA_DIRS <<< "${KVNODE_DATA_DIRS}"
[[ -z "${KVNODE_PIDS:-}" ]] || IFS=',' read -r -a PIDS_FROM_ENV <<< "${KVNODE_PIDS}"
[[ -z "${KVNODE_PID_FILES:-}" ]] || IFS=',' read -r -a PID_FILES <<< "${KVNODE_PID_FILES}"

validate_nonempty_list() {
  local name="$1"
  local max="$2"
  shift 2
  local item
  (( $# > 0 )) || fail "$name-must-not-be-empty"
  (( $# <= max )) || fail "$name-has-too-many-items"
  for item in "$@"; do
    [[ -n "$item" ]] || fail "$name-contains-an-empty-item"
    [[ "$item" != *$'\n'* && "$item" != *$'\r'* && "$item" != *$'\t'* ]] || fail "$name-contains-control-characters"
  done
}

validate_nonempty_list KVNODE_CLIENT_URLS 16 "${CLIENT_URLS[@]}"
validate_nonempty_list KVNODE_ADMIN_URLS 16 "${ADMIN_URLS[@]}"
validate_nonempty_list KVNODE_CAPACITY_VALUE_BYTES 16 "${VALUE_SIZES[@]}"
validate_nonempty_list KVNODE_CAPACITY_SCAN_LIMITS 16 "${SCAN_LIMITS[@]}"
for i in "${!CLIENT_URLS[@]}"; do
  CLIENT_URLS[$i]="$(trim_trailing_slash "${CLIENT_URLS[$i]}")"
  [[ "${CLIENT_URLS[$i]}" == http://* || "${CLIENT_URLS[$i]}" == https://* ]] || fail "KVNODE_CLIENT_URLS-contains-an-invalid-url"
done
for i in "${!ADMIN_URLS[@]}"; do
  ADMIN_URLS[$i]="$(trim_trailing_slash "${ADMIN_URLS[$i]}")"
  [[ "${ADMIN_URLS[$i]}" == http://* || "${ADMIN_URLS[$i]}" == https://* ]] || fail "KVNODE_ADMIN_URLS-contains-an-invalid-url"
done
for value_size in "${VALUE_SIZES[@]}"; do
  bounded_int KVNODE_CAPACITY_VALUE_BYTES "$value_size" "$max_value_bytes"
done
for scan_limit in "${SCAN_LIMITS[@]}"; do
  bounded_int KVNODE_CAPACITY_SCAN_LIMITS "$scan_limit" "$max_scan_limit"
done

all_pids=("")
add_pid() {
  local value="$1"
  if [[ -z "${all_pids[0]}" ]]; then
    all_pids[0]="$value"
  else
    all_pids+=("$value")
  fi
}
for pid in "${PIDS_FROM_ENV[@]}"; do
  [[ -z "$pid" ]] || add_pid "$pid"
done
for pid_file in "${PID_FILES[@]}"; do
  [[ -z "$pid_file" ]] && continue
  [[ -f "$pid_file" ]] || fail "KVNODE_PID_FILES-entry-is-not-a-file"
  pid="$(awk 'NR == 1 { print; exit }' "$pid_file")"
  [[ -n "$pid" ]] || fail "KVNODE_PID_FILES-entry-is-empty"
  add_pid "$pid"
done
for pid in "${all_pids[@]}"; do
  [[ -z "$pid" ]] || positive_int KVNODE_PIDS "$pid"
done
for data_dir in "${DATA_DIRS[@]}"; do
  [[ -z "$data_dir" ]] || [[ "$data_dir" != *$'\n'* && "$data_dir" != *$'\r'* && "$data_dir" != *$'\t'* ]] || fail "KVNODE_DATA_DIRS-contains-control-characters"
done

peer_count="${KVNODE_PEER_COUNT:-${#CLIENT_URLS[@]}}"
positive_int KVNODE_PEER_COUNT "$peer_count"

target_name="${KVNODE_CAPACITY_TARGET:-}"
release_id="${KVNODE_CAPACITY_RELEASE_ID:-}"
source_revision="${KVNODE_CAPACITY_SOURCE_REVISION:-}"
binary_path="${KVNODE_CAPACITY_BINARY_PATH:-}"
binary_sha256="${KVNODE_CAPACITY_BINARY_SHA256:-}"
environment_name="${KVNODE_CAPACITY_ENVIRONMENT_NAME:-}"
hardware_details="${KVNODE_CAPACITY_HARDWARE_DETAILS:-}"
storage_details="${KVNODE_CAPACITY_STORAGE_DETAILS:-}"
network_details="${KVNODE_CAPACITY_NETWORK_DETAILS:-}"
topology_details="${KVNODE_CAPACITY_TOPOLOGY_DETAILS:-}"
operator_name="${KVNODE_CAPACITY_OPERATOR:-}"
reviewer_name="${KVNODE_CAPACITY_REVIEWER:-}"
release_claim="${KVNODE_CAPACITY_RELEASE_CLAIM:-}"
threshold_min_throughput="${KVNODE_CAPACITY_MIN_THROUGHPUT_OPS_PER_SECOND:-}"
threshold_max_error="${KVNODE_CAPACITY_MAX_ERROR_RATE_PERCENT:-}"
threshold_max_p99="${KVNODE_CAPACITY_MAX_P99_LATENCY_SECONDS:-}"
threshold_max_rss="${KVNODE_CAPACITY_MAX_RSS_KIB:-}"
threshold_max_disk="${KVNODE_CAPACITY_MAX_DISK_KIB:-}"
threshold_max_fds="${KVNODE_CAPACITY_MAX_FDS:-}"
threshold_max_queue="${KVNODE_CAPACITY_MAX_QUEUE_DEPTH:-}"
threshold_min_headroom="${KVNODE_CAPACITY_MIN_HEADROOM_PERCENT:-}"

if [[ "$mode" == "target" ]]; then
  required_target_vars=(
    KVNODE_CAPACITY_WARMUP_OPS KVNODE_CAPACITY_STEADY_OPS KVNODE_CAPACITY_CONCURRENT_OPS
    KVNODE_CAPACITY_SATURATION_OPS KVNODE_CAPACITY_RECOVERY_OPS
    KVNODE_CAPACITY_WARMUP_CONCURRENCY KVNODE_CAPACITY_STEADY_CONCURRENCY
    KVNODE_CAPACITY_CONCURRENT_CONCURRENCY KVNODE_CAPACITY_SATURATION_CONCURRENCY
    KVNODE_CAPACITY_RECOVERY_CONCURRENCY KVNODE_CAPACITY_RECOVERY_WAIT_SECONDS
    KVNODE_CAPACITY_TARGET KVNODE_CAPACITY_RELEASE_ID KVNODE_CAPACITY_SOURCE_REVISION
    KVNODE_CAPACITY_BINARY_PATH KVNODE_CAPACITY_BINARY_SHA256 KVNODE_CAPACITY_ENVIRONMENT_NAME
    KVNODE_CAPACITY_ENVIRONMENT_LABEL KVNODE_CAPACITY_HARDWARE_DETAILS
    KVNODE_CAPACITY_STORAGE_DETAILS KVNODE_CAPACITY_NETWORK_DETAILS KVNODE_CAPACITY_TOPOLOGY_DETAILS
    KVNODE_CAPACITY_WORKLOAD_LABEL KVNODE_CAPACITY_PEER_COUNT KVNODE_CAPACITY_OPERATOR
    KVNODE_CAPACITY_REVIEWER KVNODE_CAPACITY_RELEASE_CLAIM
    KVNODE_CAPACITY_MIN_THROUGHPUT_OPS_PER_SECOND KVNODE_CAPACITY_MAX_ERROR_RATE_PERCENT
    KVNODE_CAPACITY_MAX_P99_LATENCY_SECONDS KVNODE_CAPACITY_MAX_RSS_KIB
    KVNODE_CAPACITY_MAX_DISK_KIB KVNODE_CAPACITY_MAX_FDS KVNODE_CAPACITY_MAX_QUEUE_DEPTH
    KVNODE_CAPACITY_MIN_HEADROOM_PERCENT
  )
  for required_name in "${required_target_vars[@]}"; do
    [[ -n "${!required_name:-}" ]] || fail "$required_name-is-required-in-target-mode"
  done
  pid_count=0
  for pid in "${all_pids[@]}"; do
    [[ -z "$pid" ]] && continue
    pid_count=$((pid_count + 1))
    kill -0 "$pid" >/dev/null 2>&1 || fail "target-mode-PID-$pid-is-not-running"
  done
  (( pid_count > 0 )) || fail "target-mode-requires-PIDs"
  data_dir_count=0
  for data_dir in "${DATA_DIRS[@]}"; do
    [[ -z "$data_dir" ]] && continue
    data_dir_count=$((data_dir_count + 1))
    [[ -d "$data_dir" ]] || fail "target-mode-data-directory-does-not-exist"
  done
  (( data_dir_count > 0 )) || fail "target-mode-requires-data-directories"
  if [[ "$command_mode" == "run" && -z "$capacity_report" ]]; then
    fail "KVNODE_CAPACITY_REPORT-is-required-in-target-mode"
  fi
  for provenance_name in target_name release_id environment_name hardware_details storage_details network_details topology_details operator_name reviewer_name; do
    label_value "$provenance_name" "${!provenance_name}" 512 >/dev/null
  done
  [[ "$environment_label" != "unspecified" && "$workload_label" != "unspecified" ]] || fail "target-mode-labels-must-be-specific"
  [[ "$operator_name" != "$reviewer_name" ]] || fail "target-mode-operator-and-reviewer-must-differ"
  [[ "$source_revision" =~ ^[0-9a-fA-F]{40,64}$ ]] || fail "KVNODE_CAPACITY_SOURCE_REVISION-must-be-a-full-immutable-revision"
  [[ "$binary_sha256" =~ ^[0-9a-fA-F]{64}$ ]] || fail "KVNODE_CAPACITY_BINARY_SHA256-must-be-SHA-256"
  [[ -f "$binary_path" ]] || fail "KVNODE_CAPACITY_BINARY_PATH-must-be-a-file"
  [[ "$binary_path" != *$'\n'* && "$binary_path" != *$'\r'* ]] || fail "KVNODE_CAPACITY_BINARY_PATH-must-be-a-single-line"
  actual_binary_sha256="$(sha256_file "$binary_path")"
  [[ "$(normalize_hash "$actual_binary_sha256")" == "$(normalize_hash "$binary_sha256")" ]] || fail "KVNODE_CAPACITY_BINARY_SHA256-does-not-match-binary"
  label_value KVNODE_CAPACITY_RELEASE_CLAIM "$release_claim" 512 >/dev/null
  [[ "$release_claim" == target-capacity-accepted:* && "$release_claim" != *none* ]] || fail "KVNODE_CAPACITY_RELEASE_CLAIM-must-be-a-scoped-non-none-target-claim"
  positive_decimal KVNODE_CAPACITY_MIN_THROUGHPUT_OPS_PER_SECOND "$threshold_min_throughput"
  percent_decimal KVNODE_CAPACITY_MAX_ERROR_RATE_PERCENT "$threshold_max_error" no
  positive_decimal KVNODE_CAPACITY_MAX_P99_LATENCY_SECONDS "$threshold_max_p99"
  positive_decimal KVNODE_CAPACITY_MAX_RSS_KIB "$threshold_max_rss"
  positive_decimal KVNODE_CAPACITY_MAX_DISK_KIB "$threshold_max_disk"
  positive_decimal KVNODE_CAPACITY_MAX_FDS "$threshold_max_fds"
  positive_decimal KVNODE_CAPACITY_MAX_QUEUE_DEPTH "$threshold_max_queue"
  percent_decimal KVNODE_CAPACITY_MIN_HEADROOM_PERCENT "$threshold_min_headroom" no
fi

if [[ "$command_mode" == "dry-run" ]]; then
  echo "status=dry-run-schema-valid"
  echo "mode=$mode"
  echo "phase_order=warmup,steady,concurrent,saturation,recovery"
  echo "release_claim=none-dry-run-only"
  echo "network_requests=0"
  exit 0
fi

if [[ "${KVNODE_CAPACITY_RUN:-}" != "yes" ]]; then
  usage >&2
  echo >&2
  echo "Refusing to run without KVNODE_CAPACITY_RUN=yes." >&2
  exit 2
fi

require_command curl
require_command sort
require_command wc
require_command du
require_command ps
require_command sed
require_command tr
require_command dirname
require_command chmod
require_command mkdir
require_command sleep

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${KVNODE_CAPACITY_OUT_DIR:-${TMPDIR:-/tmp}/kvnode-capacity-envelope-${run_id}}"
[[ "$out_dir" != *$'\n'* && "$out_dir" != *$'\r'* && "$out_dir" != *$'\t'* ]] || fail "KVNODE_CAPACITY_OUT_DIR-contains-control-characters"
mkdir -p "$out_dir"
out_dir="$(cd "$out_dir" && pwd -P)"

LATENCY_CSV="$out_dir/latency.csv"
RESOURCES_CSV="$out_dir/resources.csv"
PHASE_SUMMARY_CSV="$out_dir/phase_summary.csv"
SUMMARY_MD="$out_dir/summary.md"
METADATA_ENV="$out_dir/metadata.env"
CHECKSUMS_FILE="$out_dir/checksums.sha256"
RUN_PREFIX="kvnode-capacity-${run_id}-$$"
METRIC_FILES=()
ARTIFACT_KINDS=()
ARTIFACT_PATHS=()

printf 'phase,worker,operation,http_status,seconds,outcome\n' > "$LATENCY_CSV"
printf 'phase,sample,resource,target,value,unit,status\n' > "$RESOURCES_CSV"
printf 'phase,planned_operations,concurrency,attempted,successes,errors,error_rate_percent,elapsed_seconds,throughput_ops_per_second,latency_avg_seconds,latency_p50_seconds,latency_p95_seconds,latency_p99_seconds\n' > "$PHASE_SUMMARY_CSV"

started_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
start_epoch="$(date -u +%s)"
if [[ "$mode" == "local" ]]; then
  cat > "$METADATA_ENV" <<EOF
status=harness-only
release_claim=none-target-environment-capacity-results-still-required
environment_label=$environment_label
workload_label=$workload_label
run_id=$run_id
client_urls=$client_urls_raw
admin_urls=$admin_urls_raw
ops_per_phase=$ops_per_phase
value_bytes=$value_sizes_raw
scan_limits=$scan_limits_raw
peer_count=$peer_count
out_dir=$out_dir
phase_order=warmup,steady,concurrent,saturation,recovery
EOF
else
  cat > "$METADATA_ENV" <<EOF
status=target-capacity-run-in-progress
claim_scope=capacity-envelope-only-not-release-authorization
target_name=$target_name
release_id=$release_id
source_revision=$source_revision
binary_path=$binary_path
binary_sha256=$binary_sha256
environment_name=$environment_name
environment_label=$environment_label
hardware_details=$hardware_details
storage_details=$storage_details
network_details=$network_details
topology_details=$topology_details
workload_label=$workload_label
peer_count=$peer_count
operator=$operator_name
reviewer=$reviewer_name
started_utc=$started_utc
phase_order=warmup,steady,concurrent,saturation,recovery
phase_warmup_operations=$warmup_ops
phase_warmup_concurrency=$warmup_concurrency
phase_steady_operations=$steady_ops
phase_steady_concurrency=$steady_concurrency
phase_concurrent_operations=$concurrent_ops
phase_concurrent_concurrency=$concurrent_concurrency
phase_saturation_operations=$saturation_ops
phase_saturation_concurrency=$saturation_concurrency
phase_recovery_operations=$recovery_ops
phase_recovery_concurrency=$recovery_concurrency
EOF
fi

fd_count_for_pid() {
  local pid="$1"
  local proc_dir="/proc/$pid/fd"
  local entries
  if [[ -d "$proc_dir" ]]; then
    shopt -s nullglob
    entries=("$proc_dir"/*)
    shopt -u nullglob
    printf '%s' "${#entries[@]}"
    return 0
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -n -P -p "$pid" 2>/dev/null | awk 'NR > 1 { count++ } END { print count + 0 }'
    return 0
  fi
  return 1
}

record_resources() {
  local phase="$1"
  local sample="$2"
  local pid rss_kib fd_count data_dir disk_kib admin_url metrics_file queue_depth status
  for pid in "${all_pids[@]}"; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" >/dev/null 2>&1; then
      rss_kib="$(ps -o rss= -p "$pid" 2>/dev/null | awk '{print $1}' || true)"
      if [[ "$rss_kib" =~ ^[0-9]+$ ]]; then
        printf '%s,%s,rss_kib,pid:%s,%s,KiB,ok\n' "$phase" "$sample" "$pid" "$rss_kib" >> "$RESOURCES_CSV"
      else
        printf '%s,%s,rss_kib,pid:%s,missing,KiB,unavailable\n' "$phase" "$sample" "$pid" >> "$RESOURCES_CSV"
      fi
      if fd_count="$(fd_count_for_pid "$pid")" && [[ "$fd_count" =~ ^[0-9]+$ ]]; then
        printf '%s,%s,fd_count,pid:%s,%s,count,ok\n' "$phase" "$sample" "$pid" "$fd_count" >> "$RESOURCES_CSV"
      else
        printf '%s,%s,fd_count,pid:%s,missing,count,unavailable\n' "$phase" "$sample" "$pid" >> "$RESOURCES_CSV"
      fi
    else
      printf '%s,%s,rss_kib,pid:%s,missing,KiB,unavailable\n' "$phase" "$sample" "$pid" >> "$RESOURCES_CSV"
      printf '%s,%s,fd_count,pid:%s,missing,count,unavailable\n' "$phase" "$sample" "$pid" >> "$RESOURCES_CSV"
    fi
  done
  for data_dir in "${DATA_DIRS[@]}"; do
    [[ -z "$data_dir" ]] && continue
    if [[ -d "$data_dir" ]]; then
      disk_kib="$(du -sk "$data_dir" 2>/dev/null | awk '{print $1}' || true)"
      if [[ "$disk_kib" =~ ^[0-9]+$ ]]; then
        printf '%s,%s,disk_kib,%s,%s,KiB,ok\n' "$phase" "$sample" "$data_dir" "$disk_kib" >> "$RESOURCES_CSV"
      else
        printf '%s,%s,disk_kib,%s,missing,KiB,unavailable\n' "$phase" "$sample" "$data_dir" >> "$RESOURCES_CSV"
      fi
    else
      printf '%s,%s,disk_kib,%s,missing,KiB,unavailable\n' "$phase" "$sample" "$data_dir" >> "$RESOURCES_CSV"
    fi
  done
  for admin_url in "${ADMIN_URLS[@]}"; do
    metrics_file="$out_dir/metrics-${phase}-${sample}-$(printf '%s' "$admin_url" | tr '/:' '__').txt"
    status="unavailable"
    queue_depth="missing"
    if curl -fsS --max-time "$timeout_seconds" "$admin_url/metrics" > "$metrics_file" 2>/dev/null; then
      queue_depth="$(awk '$1 == "kvnode_send_queue_depth" && $2 ~ /^[0-9]+([.][0-9]+)?$/ { if (!found || $2 > maximum) maximum = $2; found = 1 } END { if (found) print maximum; else print "missing" }' "$metrics_file")"
      if [[ "$queue_depth" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        status="ok"
      fi
    fi
    METRIC_FILES+=("$metrics_file")
    printf '%s,%s,send_queue_depth,%s,%s,count,%s\n' "$phase" "$sample" "$admin_url" "$queue_depth" "$status" >> "$RESOURCES_CSV"
  done
}

make_value_file() {
  local size="$1"
  local file="$2"
  awk -v n="$size" 'BEGIN { for (i = 0; i < n; i++) printf "x" }' > "$file"
}

VALUE_FILES=()
for value_size in "${VALUE_SIZES[@]}"; do
  value_file="$out_dir/value-${value_size}.bin"
  make_value_file "$value_size" "$value_file"
  VALUE_FILES+=("$value_file")
done

record_request_to_file() {
  local phase="$1"
  local worker="$2"
  local operation="$3"
  local output_file="$4"
  shift 4
  local result http_status seconds outcome
  if result="$(curl -sS -o /dev/null -w '%{http_code} %{time_total}' --max-time "$timeout_seconds" "$@" 2>/dev/null)"; then
    outcome="success"
  else
    outcome="transport_error"
  fi
  http_status="${result%% *}"
  seconds="${result#* }"
  [[ "$http_status" =~ ^[0-9]{3}$ ]] || http_status="000"
  [[ "$seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || seconds="0.000000"
  if (( 10#$http_status < 200 || 10#$http_status >= 300 )); then
    outcome="http_or_transport_error"
  fi
  printf '%s,%s,%s,%s,%s,%s\n' "$phase" "$worker" "$operation" "$http_status" "$seconds" "$outcome" >> "$output_file"
}

run_worker() {
  local phase="$1"
  local worker="$2"
  local concurrency="$3"
  local budget="$4"
  local output_file="$5"
  local operation_index local_index client_url key value_index value_size scan_index scan_limit operation
  : > "$output_file"
  local_index=0
  for ((operation_index = worker - 1; operation_index < budget; operation_index += concurrency)); do
    client_url="${CLIENT_URLS[$((operation_index % ${#CLIENT_URLS[@]}))]}"
    key="${RUN_PREFIX}-${phase}-worker-${worker}"
    value_index=$(((operation_index + worker) % ${#VALUE_FILES[@]}))
    value_size="${VALUE_SIZES[$value_index]}"
    case $((local_index % 3)) in
      0)
        operation="put_value_bytes_${value_size}"
        record_request_to_file "$phase" "$worker" "$operation" "$output_file" -X PUT --data-binary "@${VALUE_FILES[$value_index]}" "$client_url/kv/$key"
        ;;
      1)
        operation="get_value_bytes_${value_size}"
        record_request_to_file "$phase" "$worker" "$operation" "$output_file" "$client_url/kv/$key"
        ;;
      2)
        scan_index=$((operation_index % ${#SCAN_LIMITS[@]}))
        scan_limit="${SCAN_LIMITS[$scan_index]}"
        operation="scan_limit_${scan_limit}"
        record_request_to_file "$phase" "$worker" "$operation" "$output_file" "$client_url/scan?prefix=${RUN_PREFIX}-${phase}-&limit=${scan_limit}"
        ;;
    esac
    local_index=$((local_index + 1))
  done
}

warmup_elapsed=0
steady_elapsed=0
concurrent_elapsed=0
saturation_elapsed=0
recovery_elapsed=0

run_phase() {
  local phase="$1"
  local budget="$2"
  local concurrency="$3"
  local phase_start phase_end elapsed worker worker_count shard
  local shards=()
  record_resources "$phase" before
  phase_start="$(date -u +%s)"
  worker_count="$concurrency"
  (( worker_count <= budget )) || worker_count="$budget"
  for ((worker = 1; worker <= worker_count; worker++)); do
    shard="$out_dir/latency-${phase}-worker-${worker}.csv"
    shards+=("$shard")
    run_worker "$phase" "$worker" "$worker_count" "$budget" "$shard" &
  done
  wait
  for shard in "${shards[@]}"; do
    cat "$shard" >> "$LATENCY_CSV"
    rm -f "$shard"
  done
  phase_end="$(date -u +%s)"
  elapsed=$((phase_end - phase_start))
  (( elapsed > 0 )) || elapsed=1
  case "$phase" in
    warmup) warmup_elapsed="$elapsed" ;;
    steady) steady_elapsed="$elapsed" ;;
    concurrent) concurrent_elapsed="$elapsed" ;;
    saturation) saturation_elapsed="$elapsed" ;;
    recovery) recovery_elapsed="$elapsed" ;;
  esac
  record_resources "$phase" after
}

run_phase warmup "$warmup_ops" "$warmup_concurrency"
run_phase steady "$steady_ops" "$steady_concurrency"
run_phase concurrent "$concurrent_ops" "$concurrent_concurrency"
run_phase saturation "$saturation_ops" "$saturation_concurrency"
sleep "$recovery_wait_seconds"
run_phase recovery "$recovery_ops" "$recovery_concurrency"

ended_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
end_epoch="$(date -u +%s)"
elapsed_seconds=$((end_epoch - start_epoch))
(( elapsed_seconds > 0 )) || elapsed_seconds=1

append_phase_summary() {
  local phase="$1"
  local planned="$2"
  local concurrency="$3"
  local elapsed="$4"
  local samples_file="$out_dir/.latency-${phase}.tmp"
  local attempted successes errors error_rate throughput latency_values
  awk -F, -v phase="$phase" 'NR > 1 && $1 == phase { print $5 }' "$LATENCY_CSV" | sort -n > "$samples_file"
  attempted="$(wc -l < "$samples_file" | awk '{print $1}')"
  successes="$(awk -F, -v phase="$phase" 'NR > 1 && $1 == phase && $6 == "success" { count++ } END { print count + 0 }' "$LATENCY_CSV")"
  errors=$((attempted - successes))
  error_rate="$(awk -v errors="$errors" -v attempted="$attempted" 'BEGIN { printf "%.6f", 100 * errors / attempted }')"
  throughput="$(awk -v attempted="$attempted" -v elapsed="$elapsed" 'BEGIN { printf "%.6f", attempted / elapsed }')"
  latency_values="$(awk '
    { values[NR] = $1; sum += $1 }
    END {
      p50 = int((NR * 50 + 99) / 100); if (p50 < 1) p50 = 1
      p95 = int((NR * 95 + 99) / 100); if (p95 < 1) p95 = 1
      p99 = int((NR * 99 + 99) / 100); if (p99 < 1) p99 = 1
      printf "%.6f,%.6f,%.6f,%.6f", sum / NR, values[p50], values[p95], values[p99]
    }' "$samples_file")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$phase" "$planned" "$concurrency" "$attempted" "$successes" "$errors" "$error_rate" "$elapsed" "$throughput" "$latency_values" >> "$PHASE_SUMMARY_CSV"
  rm -f "$samples_file"
}

append_phase_summary warmup "$warmup_ops" "$warmup_concurrency" "$warmup_elapsed"
append_phase_summary steady "$steady_ops" "$steady_concurrency" "$steady_elapsed"
append_phase_summary concurrent "$concurrent_ops" "$concurrent_concurrency" "$concurrent_elapsed"
append_phase_summary saturation "$saturation_ops" "$saturation_concurrency" "$saturation_elapsed"
append_phase_summary recovery "$recovery_ops" "$recovery_concurrency" "$recovery_elapsed"

all_samples="$out_dir/.latency-all.tmp"
awk -F, 'NR > 1 { print $5 }' "$LATENCY_CSV" | sort -n > "$all_samples"
operation_count="$(wc -l < "$all_samples" | awk '{print $1}')"
success_count="$(awk -F, 'NR > 1 && $6 == "success" { count++ } END { print count + 0 }' "$LATENCY_CSV")"
error_count=$((operation_count - success_count))
error_rate_percent="$(awk -v errors="$error_count" -v operations="$operation_count" 'BEGIN { printf "%.6f", 100 * errors / operations }')"
throughput="$(awk -v operations="$operation_count" -v seconds="$elapsed_seconds" 'BEGIN { printf "%.6f", operations / seconds }')"
latency_summary="$(awk '
  { values[NR] = $1; sum += $1 }
  END {
    p50 = int((NR * 50 + 99) / 100); if (p50 < 1) p50 = 1
    p95 = int((NR * 95 + 99) / 100); if (p95 < 1) p95 = 1
    p99 = int((NR * 99 + 99) / 100); if (p99 < 1) p99 = 1
    printf "latency_samples=%d\n", NR
    printf "latency_avg_seconds=%.6f\n", sum / NR
    printf "latency_p50_seconds=%.6f\n", values[p50]
    printf "latency_p95_seconds=%.6f\n", values[p95]
    printf "latency_p99_seconds=%.6f\n", values[p99]
  }' "$all_samples")"
rm -f "$all_samples"
latency_p99="$(printf '%s\n' "$latency_summary" | awk -F= '$1 == "latency_p99_seconds" { print $2 }')"

resource_max() {
  local resource="$1"
  awk -F, -v resource="$resource" 'NR > 1 && $3 == resource && $7 == "ok" && $5 ~ /^[0-9]+([.][0-9]+)?$/ {
    if (!found || $5 > maximum) maximum = $5
    found = 1
  } END { if (found) print maximum; else print "missing" }' "$RESOURCES_CSV"
}

max_rss="$(resource_max rss_kib)"
max_disk="$(resource_max disk_kib)"
max_fds="$(resource_max fd_count)"
max_queue="$(resource_max send_queue_depth)"

validate_target_resource_rows() {
  local samples=10
  local expected actual resource target_count
  target_count="$pid_count"
  for resource in rss_kib fd_count; do
    expected=$((samples * target_count))
    actual="$(awk -F, -v resource="$resource" 'NR > 1 && $3 == resource && $7 == "ok" { count++ } END { print count + 0 }' "$RESOURCES_CSV")"
    (( actual == expected )) || fail "target-mode-resource-$resource-rows-are-missing"
  done
  target_count="$data_dir_count"
  expected=$((samples * target_count))
  actual="$(awk -F, 'NR > 1 && $3 == "disk_kib" && $7 == "ok" { count++ } END { print count + 0 }' "$RESOURCES_CSV")"
  (( actual == expected )) || fail "target-mode-resource-disk_kib-rows-are-missing"
  target_count="${#ADMIN_URLS[@]}"
  expected=$((samples * target_count))
  actual="$(awk -F, 'NR > 1 && $3 == "send_queue_depth" && $7 == "ok" { count++ } END { print count + 0 }' "$RESOURCES_CSV")"
  (( actual == expected )) || fail "target-mode-resource-send_queue_depth-rows-are-missing"
}

threshold_results="not-evaluated-local-non-claim"
acceptance_result="not-applicable-local-non-claim"
if [[ "$mode" == "target" ]]; then
  validate_target_resource_rows
  [[ "$max_rss" != missing && "$max_disk" != missing && "$max_fds" != missing && "$max_queue" != missing ]] || fail "target-mode-resource-maxima-are-missing"
  floor_with_headroom_passes "$throughput" "$threshold_min_throughput" "$threshold_min_headroom" || fail "target-throughput-threshold-failed"
  ceiling_with_headroom_passes "$error_rate_percent" "$threshold_max_error" "$threshold_min_headroom" || fail "target-error-rate-threshold-failed"
  ceiling_with_headroom_passes "$latency_p99" "$threshold_max_p99" "$threshold_min_headroom" || fail "target-p99-threshold-failed"
  ceiling_with_headroom_passes "$max_rss" "$threshold_max_rss" "$threshold_min_headroom" || fail "target-rss-threshold-failed"
  ceiling_with_headroom_passes "$max_disk" "$threshold_max_disk" "$threshold_min_headroom" || fail "target-disk-threshold-failed"
  ceiling_with_headroom_passes "$max_fds" "$threshold_max_fds" "$threshold_min_headroom" || fail "target-fds-threshold-failed"
  ceiling_with_headroom_passes "$max_queue" "$threshold_max_queue" "$threshold_min_headroom" || fail "target-queue-threshold-failed"
  threshold_results="throughput:pass,error_rate:pass,p99_latency:pass,rss:pass,disk:pass,fds:pass,queue_depth:pass,headroom:pass"
  acceptance_result="pass"
fi
if [[ "$mode" == "target" ]]; then
  cat > "$METADATA_ENV" <<EOF
status=target-capacity-evidence
claim_scope=capacity-envelope-only-not-release-authorization
release_claim=$release_claim
acceptance_result=$acceptance_result
run_id=$run_id
target_name=$target_name
release_id=$release_id
source_revision=$source_revision
binary_path=$binary_path
binary_sha256=$binary_sha256
started_utc=$started_utc
ended_utc=$ended_utc
environment_name=$environment_name
environment_label=$environment_label
hardware_details=$hardware_details
storage_details=$storage_details
network_details=$network_details
topology_details=$topology_details
workload_label=$workload_label
peer_count=$peer_count
pids=$(IFS=,; printf '%s' "${all_pids[*]}")
data_dirs=$(IFS=,; printf '%s' "${DATA_DIRS[*]}")
operator=$operator_name
reviewer=$reviewer_name
phase_order=warmup,steady,concurrent,saturation,recovery
phase_warmup_operations=$warmup_ops
phase_warmup_concurrency=$warmup_concurrency
phase_steady_operations=$steady_ops
phase_steady_concurrency=$steady_concurrency
phase_concurrent_operations=$concurrent_ops
phase_concurrent_concurrency=$concurrent_concurrency
phase_saturation_operations=$saturation_ops
phase_saturation_concurrency=$saturation_concurrency
phase_recovery_operations=$recovery_ops
phase_recovery_concurrency=$recovery_concurrency
recovery_wait_seconds=$recovery_wait_seconds
operation_count=$operation_count
success_count=$success_count
error_count=$error_count
error_rate_percent=$error_rate_percent
elapsed_seconds=$elapsed_seconds
throughput_ops_per_second=$throughput
latency_p99_seconds=$latency_p99
max_rss_kib=$max_rss
max_disk_kib=$max_disk
max_fds=$max_fds
max_queue_depth=$max_queue
threshold_min_throughput_ops_per_second=$threshold_min_throughput
threshold_max_error_rate_percent=$threshold_max_error
threshold_max_p99_latency_seconds=$threshold_max_p99
threshold_max_rss_kib=$threshold_max_rss
threshold_max_disk_kib=$threshold_max_disk
threshold_max_fds=$threshold_max_fds
threshold_max_queue_depth=$threshold_max_queue
threshold_min_headroom_percent=$threshold_min_headroom
threshold_results=$threshold_results
EOF
fi


cat > "$SUMMARY_MD" <<EOF
# kvnode capacity-envelope harness sample

Status: $(if [[ "$mode" == local ]]; then printf '%s' 'harness output only; local rehearsal is not target capacity evidence'; else printf '%s' 'target capacity-envelope thresholds passed; claim is scoped and is not release authorization'; fi).
Release claim: $(if [[ "$mode" == local ]]; then printf '%s' 'release_claim=none-target-environment-capacity-results-still-required'; else printf 'release_claim=%s' "$release_claim"; fi).

## Inputs

- Run ID: $run_id
- Environment label: $environment_label
- Workload label: $workload_label
- Peer-count label: $peer_count
- Operations per value-size phase: $ops_per_phase
- Value sizes requested: $value_sizes_raw bytes
- Scan limits requested: $scan_limits_raw
- Phase order: warmup, steady, concurrent, saturation, recovery

## Measurements collected

- Throughput sample: $throughput operations/second over $operation_count HTTP operations and ${elapsed_seconds}s elapsed wall time.
$(printf '%s\n' "$latency_summary" | sed 's/^/- /')
- Error accounting: successes=$success_count errors=$error_count error_rate_percent=$error_rate_percent.
- Memory RSS samples: maximum=$max_rss KiB; see resources.csv rows with resource=rss_kib.
- Disk growth samples: absolute maximum=$max_disk KiB; see resources.csv rows with resource=disk_kib.
- FD samples: maximum=$max_fds; see resources.csv rows with resource=fd_count.
- Queue-depth samples: maximum=$max_queue; see resources.csv rows with resource=send_queue_depth.
- Peer-count coverage: recorded as the peer-count label above.
- Acceptance: $acceptance_result ($threshold_results).
EOF

ARTIFACT_KINDS=(metadata latency resources phase-summary)
ARTIFACT_PATHS=("$METADATA_ENV" "$LATENCY_CSV" "$RESOURCES_CSV" "$PHASE_SUMMARY_CSV")
for metrics_file in "${METRIC_FILES[@]}"; do
  ARTIFACT_KINDS+=(metrics)
  ARTIFACT_PATHS+=("$metrics_file")
done
: > "$CHECKSUMS_FILE"
ARTIFACT_HASHES=()
for artifact_index in "${!ARTIFACT_PATHS[@]}"; do
  artifact_hash="$(sha256_file "${ARTIFACT_PATHS[$artifact_index]}")"
  ARTIFACT_HASHES+=("$artifact_hash")
  printf '%s  %s\n' "$artifact_hash" "${ARTIFACT_PATHS[$artifact_index]}" >> "$CHECKSUMS_FILE"
done
checksums_hash="$(sha256_file "$CHECKSUMS_FILE")"

write_report() {
  local report_path="$capacity_report"
  [[ -n "$report_path" ]] || return 0
  mkdir -p "$(dirname "$report_path")"
  {
    echo "status=example-operator-report"
    echo "artifact=capacity-envelope-sample"
    echo "run_id=$run_id"
    echo "harness=tests/kvnode_capacity_envelope.sh"
    echo "environment_label=$environment_label"
    echo "workload_label=$workload_label"
    echo "peer_count=$peer_count"
    echo "operation_count=$operation_count"
    echo "throughput_ops_per_second=$throughput"
    printf '%s\n' "$latency_summary"
    echo "elapsed_seconds=$elapsed_seconds"
    echo "ops_per_phase=$ops_per_phase"
    echo "value_bytes=$value_sizes_raw"
    echo "scan_limits=$scan_limits_raw"
    printf 'out_dir=%q\n' "$out_dir"
    echo "latency_file=latency.csv"
    echo "resources_file=resources.csv"
    echo "evidence_files=metadata.env,summary.md,latency.csv,resources.csv"
    echo "target_environment=not-measured"
    echo "release_claim=none-target-environment-capacity-results-still-required"
  } > "$report_path"
  chmod 0600 "$report_path"
  printf 'report=%q\n' "$report_path"
}

write_target_report() {
  local report_path="$capacity_report"
  local artifact_number index
  [[ -n "$report_path" ]] || fail "KVNODE_CAPACITY_REPORT-is-required-in-target-mode"
  mkdir -p "$(dirname "$report_path")"
  {
    echo "status=target-capacity-evidence"
    echo "harness=tests/kvnode_capacity_envelope.sh"
    echo "claim_scope=capacity-envelope-only-not-release-authorization"
    echo "release_claim=$release_claim"
    echo "acceptance_result=$acceptance_result"
    echo "run_id=$run_id"
    echo "target_name=$target_name"
    echo "release_id=$release_id"
    echo "source_revision=$source_revision"
    echo "binary_path=$binary_path"
    echo "binary_sha256=$binary_sha256"
    echo "started_utc=$started_utc"
    echo "ended_utc=$ended_utc"
    echo "environment_name=$environment_name"
    echo "environment_label=$environment_label"
    echo "hardware_details=$hardware_details"
    echo "storage_details=$storage_details"
    echo "network_details=$network_details"
    echo "topology_details=$topology_details"
    echo "workload_label=$workload_label"
    echo "peer_count=$peer_count"
    echo "pids=$(IFS=,; printf '%s' "${all_pids[*]}")"
    echo "data_dirs=$(IFS=,; printf '%s' "${DATA_DIRS[*]}")"
    echo "operator=$operator_name"
    echo "reviewer=$reviewer_name"
    echo "phase_warmup_operations=$warmup_ops"
    echo "phase_warmup_concurrency=$warmup_concurrency"
    echo "phase_steady_operations=$steady_ops"
    echo "phase_steady_concurrency=$steady_concurrency"
    echo "phase_concurrent_operations=$concurrent_ops"
    echo "phase_concurrent_concurrency=$concurrent_concurrency"
    echo "phase_saturation_operations=$saturation_ops"
    echo "phase_saturation_concurrency=$saturation_concurrency"
    echo "phase_recovery_operations=$recovery_ops"
    echo "phase_recovery_concurrency=$recovery_concurrency"
    echo "recovery_wait_seconds=$recovery_wait_seconds"
    echo "operation_count=$operation_count"
    echo "elapsed_seconds=$elapsed_seconds"
    echo "success_count=$success_count"
    echo "error_count=$error_count"
    echo "error_rate_percent=$error_rate_percent"
    echo "throughput_ops_per_second=$throughput"
    printf '%s\n' "$latency_summary"
    echo "max_rss_kib=$max_rss"
    echo "max_disk_kib=$max_disk"
    echo "max_fds=$max_fds"
    echo "max_queue_depth=$max_queue"
    echo "resource_types=rss_kib,disk_kib,fd_count,send_queue_depth"
    echo "resource_sample_points=10"
    echo "threshold_min_throughput_ops_per_second=$threshold_min_throughput"
    echo "threshold_max_error_rate_percent=$threshold_max_error"
    echo "threshold_max_p99_latency_seconds=$threshold_max_p99"
    echo "threshold_max_rss_kib=$threshold_max_rss"
    echo "threshold_max_disk_kib=$threshold_max_disk"
    echo "threshold_max_fds=$threshold_max_fds"
    echo "threshold_max_queue_depth=$threshold_max_queue"
    echo "threshold_min_headroom_percent=$threshold_min_headroom"
    echo "threshold_results=$threshold_results"
    echo "checksums_path=$CHECKSUMS_FILE"
    echo "checksums_sha256=$checksums_hash"
    echo "artifact_count=${#ARTIFACT_PATHS[@]}"
    artifact_number=0
    for index in "${!ARTIFACT_PATHS[@]}"; do
      artifact_number=$((artifact_number + 1))
      echo "artifact_${artifact_number}_kind=${ARTIFACT_KINDS[$index]}"
      echo "artifact_${artifact_number}_path=${ARTIFACT_PATHS[$index]}"
      echo "artifact_${artifact_number}_sha256=${ARTIFACT_HASHES[$index]}"
    done
  } > "$report_path"
  chmod 0600 "$report_path"
  validate_report "$report_path" target >/dev/null
  printf 'report=%q\n' "$report_path"
}

if [[ "$mode" == "target" ]]; then
  write_target_report
else
  write_report
fi

printf 'kvnode capacity-envelope harness output: %s\n' "$out_dir"
if [[ "$mode" == "local" && "$error_count" -gt 0 ]]; then
  fail "local-run-recorded-$error_count-request-errors"
fi
