#!/usr/bin/env bash
set -euo pipefail

PROGRAM="kvnode-target-deployment-evidence"
SELF_PATH="tests/kvnode_target_deployment_evidence.sh"

usage() {
  cat <<'USAGE'
kvnode target deployment evidence collector/verifier

This tool packages and verifies evidence captured by an approved target procedure.
It does not deploy, connect to, start, stop, restart, or otherwise operate a target.
Collection is read-only with respect to the target: it copies already-captured raw
artifacts into a new, explicitly named output directory and hashes those copies.
No live-check mode is implemented; unknown options fail closed.

Commands:
  collect --input INPUT.env --output-dir NEW_DIRECTORY
  verify REPORT.env

collect requires a new --output-dir and never overwrites an existing path. INPUT.env
must use either the Linux/systemd kvnode-target-deployment-input-v1 contract or
the native Darwin/launchd kvnode-target-deployment-input-v2 contract. See
--print-input-schema and --print-input-schema-v2. Artifact paths in INPUT.env
must name non-empty, regular, non-symlink files. A successful collection writes:

  NEW_DIRECTORY/evidence.env
  NEW_DIRECTORY/raw/<evidence-category>.artifact

verify is read-only. It rejects missing, duplicate, unexpected, malformed, stale,
unhashed, placeholder, source-only, local/example, skipped, or non-claim evidence.
The default maximum evidence age is 604800 seconds. Override it only with a
positive KVNODE_DEPLOYMENT_MAX_AGE_SECONDS value.

Test-only synthetic reports are rejected unless
KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes is set explicitly. That switch never
turns a synthetic report into target or release evidence.
USAGE
}

fail() {
  echo "$PROGRAM status=fail reason=$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing-required-command-$1"
}

categories_v1=(
  binary
  rendered_argv
  deployment_manifest
  systemd_verification
  service_identity_permissions
  persistent_volume
  network
  firewall
  tls
  peer_connectivity
  resource_limits
  restart
  boot_persistence
  health
  readiness
  graceful_stop
  logs
  metrics
  canary
  rollback
  operator_attestation
  reviewer_attestation
)

candidate_scalar_keys_v1=(
  evidence_schema
  evidence_mode
  release_claim
  release_id
  collection_scope
  collection_method
  target_execution
  target_name
  target_environment
  target_platform
  linux_distribution
  linux_version
  orchestrator
  orchestrator_version
  application_version
  image_reference
  source_revision
  service_user
  service_group
  service_uid
  service_gid
  service_permissions_profile
  persistent_volume_mounts
  persistent_volume_owner
  collection_started_at_utc
  collection_completed_at_utc
  operator_identity
  operator_signoff
  operator_signed_at_utc
  reviewer_identity
  reviewer_signoff
  reviewer_signed_at_utc
)

report_scalar_keys_v1=(
  evidence_schema
  collector
  evidence_mode
  release_claim
  release_id
  collection_scope
  collection_method
  target_execution
  target_name
  target_environment
  target_platform
  linux_distribution
  linux_version
  orchestrator
  orchestrator_version
  application_version
  image_reference
  source_revision
  service_user
  service_group
  service_uid
  service_gid
  service_permissions_profile
  persistent_volume_mounts
  persistent_volume_owner
  collection_started_at_utc
  collection_completed_at_utc
  operator_identity
  operator_signoff
  operator_signed_at_utc
  reviewer_identity
  reviewer_signoff
  reviewer_signed_at_utc
  report_created_at_utc
  artifact_count
)

categories_v2=(
  binary
  source_provenance
  rendered_argv
  deployment_manifest
  supervisor_verification
  service_identity_permissions
  durable_storage
  process_binary_binding
  network
  security_posture
  tls
  peer_connectivity
  resource_limits
  restart
  boot_persistence
  health
  readiness
  graceful_stop
  logs
  metrics
  canary
  rollback
  operator_attestation
  reviewer_attestation
)

candidate_scalar_keys_v2=(
  evidence_schema
  evidence_mode
  release_claim
  release_id
  collection_scope
  collection_method
  target_execution
  target_id
  target_environment
  deployment_profile
  target_platform
  architecture
  binary_format
  execution_mode
  orchestrator
  launchd_domain
  darwin_version
  macos_version
  os_build
  kernel_version
  filesystem_type
  staging_root_path
  staging_root_uri
  staging_root_filesystem
  staging_root_writable
  final_evidence_root_path
  final_evidence_root_uri
  final_evidence_root_filesystem
  final_evidence_root_read_only_required
  final_evidence_root_external
  final_evidence_image_format
  apfs_data_root
  apfs_checkpoint_root
  apfs_quarantine_root
  apfs_log_root
  binary_uri
  binary_path
  binary_expected_sha256
  binary_source_revision
  binary_immutable
  source_revision
  service_user
  service_group
  service_uid
  service_gid
  service_permissions_profile
  host_topology
  network_scope
  tls_scope
  tls_ca_path
  mutual_tls
  client_authorization
  nonclaims
  boot_observation
  boot_observation_synthetic
  boot_uuid_before
  boot_uuid_after
  graceful_signal
  graceful_accepts_stopped
  graceful_inflight_drained
  graceful_exit_seconds
  graceful_durable_canary
  rollback_bundle_uri
  rollback_bundle_sha256
  rollback_bundle_immutable
  rollback_binary_format
  rollback_architecture
  node_1_label
  node_1_pid
  node_1_client_listener
  node_1_peer_listener
  node_1_admin_listener
  node_1_data_directory
  node_1_plist_path
  node_1_tls_cert_path
  node_1_tls_key_path
  node_1_plist_sha256
  node_1_plutil_lint_result
  node_1_launchd_bootstrap_result
  node_1_launchd_print_result
  node_1_program_arguments_sha256
  node_1_launchctl_program_arguments_sha256
  node_1_process_arguments_sha256
  node_1_executable_path
  node_1_executable_sha256
  node_2_label
  node_2_pid
  node_2_client_listener
  node_2_peer_listener
  node_2_admin_listener
  node_2_data_directory
  node_2_plist_path
  node_2_tls_cert_path
  node_2_tls_key_path
  node_2_plist_sha256
  node_2_plutil_lint_result
  node_2_launchd_bootstrap_result
  node_2_launchd_print_result
  node_2_program_arguments_sha256
  node_2_launchctl_program_arguments_sha256
  node_2_process_arguments_sha256
  node_2_executable_path
  node_2_executable_sha256
  node_3_label
  node_3_pid
  node_3_client_listener
  node_3_peer_listener
  node_3_admin_listener
  node_3_data_directory
  node_3_plist_path
  node_3_tls_cert_path
  node_3_tls_key_path
  node_3_plist_sha256
  node_3_plutil_lint_result
  node_3_launchd_bootstrap_result
  node_3_launchd_print_result
  node_3_program_arguments_sha256
  node_3_launchctl_program_arguments_sha256
  node_3_process_arguments_sha256
  node_3_executable_path
  node_3_executable_sha256
  collection_started_at_utc
  collection_completed_at_utc
  operator_identity
  operator_signoff
  operator_signed_at_utc
  reviewer_identity
  reviewer_signoff
  reviewer_signed_at_utc
)

report_scalar_keys_v2=(
  evidence_schema
  collector
  verifier_version
  evidence_mode
  release_claim
  release_id
  collection_scope
  collection_method
  target_execution
  target_id
  target_environment
  deployment_profile
  target_platform
  architecture
  binary_format
  execution_mode
  orchestrator
  launchd_domain
  darwin_version
  macos_version
  os_build
  kernel_version
  filesystem_type
  staging_root_path
  staging_root_uri
  staging_root_filesystem
  staging_root_writable
  final_evidence_root_path
  final_evidence_root_uri
  final_evidence_root_filesystem
  final_evidence_root_read_only_required
  final_evidence_root_external
  final_evidence_image_format
  apfs_data_root
  apfs_checkpoint_root
  apfs_quarantine_root
  apfs_log_root
  binary_uri
  binary_path
  binary_expected_sha256
  binary_source_revision
  binary_immutable
  source_revision
  service_user
  service_group
  service_uid
  service_gid
  service_permissions_profile
  host_topology
  network_scope
  tls_scope
  tls_ca_path
  mutual_tls
  client_authorization
  nonclaims
  boot_observation
  boot_observation_synthetic
  boot_uuid_before
  boot_uuid_after
  graceful_signal
  graceful_accepts_stopped
  graceful_inflight_drained
  graceful_exit_seconds
  graceful_durable_canary
  rollback_bundle_uri
  rollback_bundle_sha256
  rollback_bundle_immutable
  rollback_binary_format
  rollback_architecture
  node_1_label
  node_1_pid
  node_1_client_listener
  node_1_peer_listener
  node_1_admin_listener
  node_1_data_directory
  node_1_plist_path
  node_1_tls_cert_path
  node_1_tls_key_path
  node_1_plist_sha256
  node_1_plutil_lint_result
  node_1_launchd_bootstrap_result
  node_1_launchd_print_result
  node_1_program_arguments_sha256
  node_1_launchctl_program_arguments_sha256
  node_1_process_arguments_sha256
  node_1_executable_path
  node_1_executable_sha256
  node_2_label
  node_2_pid
  node_2_client_listener
  node_2_peer_listener
  node_2_admin_listener
  node_2_data_directory
  node_2_plist_path
  node_2_tls_cert_path
  node_2_tls_key_path
  node_2_plist_sha256
  node_2_plutil_lint_result
  node_2_launchd_bootstrap_result
  node_2_launchd_print_result
  node_2_program_arguments_sha256
  node_2_launchctl_program_arguments_sha256
  node_2_process_arguments_sha256
  node_2_executable_path
  node_2_executable_sha256
  node_3_label
  node_3_pid
  node_3_client_listener
  node_3_peer_listener
  node_3_admin_listener
  node_3_data_directory
  node_3_plist_path
  node_3_tls_cert_path
  node_3_tls_key_path
  node_3_plist_sha256
  node_3_plutil_lint_result
  node_3_launchd_bootstrap_result
  node_3_launchd_print_result
  node_3_program_arguments_sha256
  node_3_launchctl_program_arguments_sha256
  node_3_process_arguments_sha256
  node_3_executable_path
  node_3_executable_sha256
  collection_started_at_utc
  collection_completed_at_utc
  operator_identity
  operator_signoff
  operator_signed_at_utc
  reviewer_identity
  reviewer_signoff
  reviewer_signed_at_utc
  report_created_at_utc
  artifact_count
)

categories=()
candidate_scalar_keys=()
report_scalar_keys=()
contract_version=""
input_schema=""
evidence_schema=""

activate_contract() {
  local schema="$1"
  case "$schema" in
    kvnode-target-deployment-input-v1|kvnode-target-deployment-evidence-v1)
      contract_version="v1"
      input_schema="kvnode-target-deployment-input-v1"
      evidence_schema="kvnode-target-deployment-evidence-v1"
      categories=("${categories_v1[@]}")
      candidate_scalar_keys=("${candidate_scalar_keys_v1[@]}")
      report_scalar_keys=("${report_scalar_keys_v1[@]}")
      ;;
    kvnode-target-deployment-input-v2|kvnode-target-deployment-evidence-v2)
      contract_version="v2"
      input_schema="kvnode-target-deployment-input-v2"
      evidence_schema="kvnode-target-deployment-evidence-v2"
      categories=("${categories_v2[@]}")
      candidate_scalar_keys=("${candidate_scalar_keys_v2[@]}")
      report_scalar_keys=("${report_scalar_keys_v2[@]}")
      ;;
    *) fail "unsupported-evidence-schema-$schema" ;;
  esac
}

is_candidate_key() {
  local wanted="$1"
  local key=""
  local category=""
  for key in "${candidate_scalar_keys[@]}"; do
    [[ "$wanted" == "$key" ]] && return 0
  done
  for category in "${categories[@]}"; do
    [[ "$wanted" == "${category}_result" || "$wanted" == "${category}_artifact" ]] && return 0
  done
  return 1
}

is_report_key() {
  local wanted="$1"
  local key=""
  local category=""
  for key in "${report_scalar_keys[@]}"; do
    [[ "$wanted" == "$key" ]] && return 0
  done
  for category in "${categories[@]}"; do
    [[ "$wanted" == "${category}_criterion" || "$wanted" == "${category}_result" || "$wanted" == "${category}_artifact" || "$wanted" == "${category}_sha256" ]] && return 0
  done
  return 1
}

validate_record_shape() {
  local file="$1"
  local kind="$2"
  local seen="|"
  local line=""
  local key=""
  local schema=""
  local line_number=0

  [[ -f "$file" && ! -L "$file" ]] || fail "$kind-must-be-regular-non-symlink-file"
  [[ -s "$file" ]] || fail "$kind-must-not-be-empty"

  while IFS= read -r line || [[ -n "$line" ]]; do
    line_number=$((line_number + 1))
    if [[ ! "$line" =~ ^([a-z][a-z0-9_]*)=([^[:cntrl:]]*)$ ]]; then
      fail "$kind-malformed-line-$line_number"
    fi
    key="${line%%=*}"
    case "$seen" in
      *"|${key}|"*) fail "$kind-duplicate-field-$key" ;;
    esac
    seen="${seen}${key}|"
    if [[ "$key" == "evidence_schema" ]]; then
      schema="${line#*=}"
    fi
  done < "$file"

  [[ -n "$schema" ]] || fail "missing-required-field-evidence_schema"
  case "$kind:$schema" in
    input:kvnode-target-deployment-input-v1|input:kvnode-target-deployment-input-v2|report:kvnode-target-deployment-evidence-v1|report:kvnode-target-deployment-evidence-v2)
      activate_contract "$schema"
      ;;
    *) fail "unsupported-$kind-schema-$schema" ;;
  esac

  while IFS= read -r line || [[ -n "$line" ]]; do
    key="${line%%=*}"
    if [[ "$kind" == "input" ]]; then
      is_candidate_key "$key" || fail "$kind-unexpected-field-$key"
    else
      is_report_key "$key" || fail "$kind-unexpected-field-$key"
    fi
  done < "$file"
}

VALUE=""
get_required() {
  local file="$1"
  local wanted="$2"
  local line=""
  local key=""
  local found=0
  VALUE=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    key="${line%%=*}"
    if [[ "$key" == "$wanted" ]]; then
      VALUE="${line#*=}"
      found=1
      break
    fi
  done < "$file"
  (( found == 1 )) || fail "missing-required-field-$wanted"
  [[ -n "$VALUE" ]] || fail "empty-required-field-$wanted"
}

require_exact() {
  local file="$1"
  local key="$2"
  local expected="$3"
  get_required "$file" "$key"
  [[ "$VALUE" == "$expected" ]] || fail "$key-must-equal-$expected"
}

lowercase() {
  printf '%s' "$1" | LC_ALL=C tr '[:upper:]' '[:lower:]'
}

reject_placeholder() {
  local key="$1"
  local value="$2"
  local lower=""
  lower="$(lowercase "$value")"
  case "$lower" in
    none|unknown|unspecified|tbd|todo|placeholder|n/a|na|not-performed|not_performed|not-applicable|not_applicable|skipped|latest|null)
      fail "$key-placeholder-value"
      ;;
  esac
  case "$lower" in
    *'<placeholder>'*|*'replace-me'*|*'replace_me'*|*'changeme'*|*'change-me'*)
      fail "$key-placeholder-value"
      ;;
  esac
}

reject_local_or_example_target() {
  local key="$1"
  local value="$2"
  local lower=""
  lower="$(lowercase "$value")"
  case "-${lower}-" in
    *[-._]local[-._]*|*[-._]localhost[-._]*|*[-._]loopback[-._]*|*[-._]example[-._]*)
      fail "$key-must-not-be-local-or-example"
      ;;
  esac
}

require_named_value() {
  local file="$1"
  local key="$2"
  get_required "$file" "$key"
  reject_placeholder "$key" "$VALUE"
  [[ "$VALUE" =~ ^[A-Za-z0-9][A-Za-z0-9._:@/+,-]{0,255}$ ]] || fail "$key-malformed"
}

require_nonzero_sha256_value() {
  local key="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9a-f]{64}$ ]] || fail "$key-must-be-lowercase-sha256"
  [[ "$value" != "0000000000000000000000000000000000000000000000000000000000000000" ]] || fail "$key-must-not-be-zero-sha256"
}

parse_utc_epoch() {
  local timestamp="$1"
  local parsed=""
  [[ "$timestamp" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || return 1
  if parsed="$(date -u -d "$timestamp" +%s 2>/dev/null)"; then
    :
  elif parsed="$(date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$timestamp" +%s 2>/dev/null)"; then
    :
  else
    return 1
  fi
  [[ "$parsed" =~ ^[0-9]+$ ]] || return 1
  printf '%s' "$parsed"
}

validate_timestamps() {
  local file="$1"
  local include_report_created="$2"
  local max_age="${KVNODE_DEPLOYMENT_MAX_AGE_SECONDS:-604800}"
  local now=""
  local started=""
  local completed=""
  local operator_signed=""
  local reviewer_signed=""
  local report_created=""
  local timestamp=""
  local epoch=""

  [[ "$max_age" =~ ^[1-9][0-9]*$ ]] || fail "KVNODE_DEPLOYMENT_MAX_AGE_SECONDS-must-be-positive-integer"
  (( 10#$max_age <= 31536000 )) || fail "KVNODE_DEPLOYMENT_MAX_AGE_SECONDS-too-large"
  now="$(date -u +%s)"

  get_required "$file" collection_started_at_utc
  timestamp="$VALUE"
  started="$(parse_utc_epoch "$timestamp")" || fail "collection_started_at_utc-must-be-valid-UTC"
  get_required "$file" collection_completed_at_utc
  timestamp="$VALUE"
  completed="$(parse_utc_epoch "$timestamp")" || fail "collection_completed_at_utc-must-be-valid-UTC"
  get_required "$file" operator_signed_at_utc
  timestamp="$VALUE"
  operator_signed="$(parse_utc_epoch "$timestamp")" || fail "operator_signed_at_utc-must-be-valid-UTC"
  get_required "$file" reviewer_signed_at_utc
  timestamp="$VALUE"
  reviewer_signed="$(parse_utc_epoch "$timestamp")" || fail "reviewer_signed_at_utc-must-be-valid-UTC"

  (( started <= completed )) || fail "collection-time-order-invalid"
  (( completed <= operator_signed )) || fail "operator-signoff-predates-collection"
  (( operator_signed <= reviewer_signed )) || fail "reviewer-signoff-predates-operator-signoff"
  (( reviewer_signed <= now + 300 )) || fail "reviewer-signoff-is-in-the-future"
  (( now - completed <= 10#$max_age )) || fail "deployment-evidence-stale"

  if [[ "$include_report_created" == "yes" ]]; then
    get_required "$file" report_created_at_utc
    timestamp="$VALUE"
    report_created="$(parse_utc_epoch "$timestamp")" || fail "report_created_at_utc-must-be-valid-UTC"
    (( reviewer_signed <= report_created )) || fail "report-predates-reviewer-signoff"
    (( report_created <= now + 300 )) || fail "report-created-in-the-future"
    (( now - report_created <= 10#$max_age )) || fail "deployment-report-stale"
  fi
}

criterion_for() {
  local category="$1"
  if [[ "$contract_version" == "v1" ]]; then
    case "$category" in
      binary) CRITERION="deployed binary bytes match the exact recorded SHA-256" ;;
      rendered_argv) CRITERION="target runtime argv records the exact executable and all effective arguments" ;;
      deployment_manifest) CRITERION="deployed manifest bytes match the reviewed manifest SHA-256" ;;
      systemd_verification) CRITERION="target systemd-analyze verification succeeds against the deployed unit and dependencies" ;;
      service_identity_permissions) CRITERION="service runs as the recorded dedicated non-root user and group with reviewed least-privilege permissions" ;;
      persistent_volume) CRITERION="persistent mounts are present at the recorded paths and owned by the recorded service identity" ;;
      network) CRITERION="target listener addresses routes and network policy match the approved topology" ;;
      firewall) CRITERION="target firewall admits only approved client peer and administration flows" ;;
      tls) CRITERION="target TLS certificates trust chain names validity and private-key permissions pass review" ;;
      peer_connectivity) CRITERION="every configured peer completes authenticated bidirectional connectivity checks" ;;
      resource_limits) CRITERION="effective CPU memory file descriptor process and storage limits match approved values" ;;
      restart) CRITERION="target service recovers after an observed process failure under the configured restart policy" ;;
      boot_persistence) CRITERION="target service is enabled and returns healthy after an observed host boot" ;;
      health) CRITERION="target health checks pass for every deployed node" ;;
      readiness) CRITERION="target readiness checks pass for every deployed node after peer convergence" ;;
      graceful_stop) CRITERION="graceful stop drains requests terminates within deadline and preserves durable recovery" ;;
      logs) CRITERION="target logs are retained queryable timestamped and correlated across every node" ;;
      metrics) CRITERION="target metrics are scraped labeled current and cover agreed deployment alerts" ;;
      canary) CRITERION="target canary completes approved write read and cleanup checks without errors" ;;
      rollback) CRITERION="rollback record binds the prior immutable image trigger procedure outcome and restored health" ;;
      operator_attestation) CRITERION="named operator signs the complete target deployment evidence after collection" ;;
      reviewer_attestation) CRITERION="different named reviewer independently approves the complete hashed evidence bundle" ;;
      *) fail "unknown-evidence-category-$category" ;;
    esac
    return
  fi

  case "$category" in
    binary) CRITERION="native Mach-O arm64 binary bytes match the immutable URI exact SHA-256 and source revision" ;;
    source_provenance) CRITERION="native build provenance binds source revision binary SHA-256 Darwin arm64 and unmodified build metadata" ;;
    rendered_argv) CRITERION="each plist launchctl record and live process exposes identical exact ProgramArguments" ;;
    deployment_manifest) CRITERION="three system LaunchDaemon plist bytes match their distinct reviewed SHA-256 values" ;;
    supervisor_verification) CRITERION="plutil lint launchd bootstrap and launchctl print pass for all three system-domain labels and live PIDs" ;;
    service_identity_permissions) CRITERION="all three native services use the recorded dedicated non-root identity and reviewed permissions" ;;
    durable_storage) CRITERION="all three node directories and checkpoint quarantine and log roots are observed on APFS with durable canary recovery" ;;
    process_binary_binding) CRITERION="every distinct live PID executable vnode and argv bind to the one immutable native binary path and SHA-256" ;;
    network) CRITERION="exactly nine recorded listeners are distinct expected loopback client peer and admin sockets" ;;
    security_posture) CRITERION="system LaunchDaemons least privilege loopback-only exposure APFS scope and explicit non-claims are reviewed" ;;
    tls) CRITERION="loopback TLS server certificates chain validate with IP SANs and protected keys without mTLS or client-authorization claims" ;;
    peer_connectivity) CRITERION="all six directed peer checks demonstrate reciprocal TLS server authentication and convergence only" ;;
    resource_limits) CRITERION="observed native Darwin limits and shared-host resource posture are recorded without isolation or capacity claims" ;;
    restart) CRITERION="launchd replaces an observed killed process with a distinct healthy PID and preserves durable data" ;;
    boot_persistence) CRITERION="system LaunchDaemons return after a real host reboot proven by distinct observed boot UUIDs" ;;
    health) CRITERION="CA-verified live and health checks pass for every native node" ;;
    readiness) CRITERION="CA-verified readiness cross-node writes reads barrier scans and drained queues pass" ;;
    graceful_stop) CRITERION="observed SIGTERM stops accepts drains inflight requests exits within deadline and preserves a durable canary" ;;
    logs) CRITERION="per-node launchd logs are timestamped retained and correlated to labels and PIDs without centralized-logging claims" ;;
    metrics) CRITERION="timestamped metrics from all three nodes are current labeled and exercise the reviewed predicates" ;;
    canary) CRITERION="unique writes exact reads barrier scans and cleanup pass on all three native nodes" ;;
    rollback) CRITERION="observed rollback binds a prior immutable Mach-O arm64 bundle and restores health before returning to final bytes" ;;
    operator_attestation) CRITERION="named operator signs the complete target release source binary and Darwin deployment evidence after collection" ;;
    reviewer_attestation) CRITERION="different named reviewer independently approves every hashed artifact binding and explicit non-claim" ;;
    *) fail "unknown-evidence-category-$category" ;;
  esac
}

validate_mode_and_signoffs() {
  local file="$1"
  local mode=""
  local claim=""
  local operator_identity=""
  local reviewer_identity=""

  get_required "$file" evidence_mode
  mode="$VALUE"
  get_required "$file" release_claim
  claim="$VALUE"
  case "$mode" in
    target)
      [[ "$claim" == "target-deployment-accepted" ]] || fail "release_claim-must-be-target-deployment-accepted"
      ;;
    test-only-synthetic)
      [[ "${KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE:-}" == "yes" ]] || fail "test-fixture-requires-explicit-opt-in"
      [[ "$claim" == "test-only-synthetic-deployment-accepted" ]] || fail "synthetic-release-claim-invalid"
      ;;
    *) fail "evidence_mode-invalid" ;;
  esac

  require_named_value "$file" operator_identity
  operator_identity="$VALUE"
  require_named_value "$file" reviewer_identity
  reviewer_identity="$VALUE"
  [[ "$(lowercase "$operator_identity")" != "$(lowercase "$reviewer_identity")" ]] || fail "reviewer-must-be-independent-from-operator"
  require_exact "$file" operator_signoff approved
  require_exact "$file" reviewer_signoff approved
}

validate_source_revision() {
  local file="$1"
  local source_revision=""
  get_required "$file" source_revision
  source_revision="$VALUE"
  [[ "$source_revision" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || fail "source_revision-must-be-exact-lowercase-40-or-64-hex-revision"
  [[ "$source_revision" == *[1-9a-f]* ]] || fail "source_revision-must-not-be-zero"
}

validate_category_records() {
  local file="$1"
  local kind="$2"
  local category=""
  local artifact=""

  for category in "${categories[@]}"; do
    require_exact "$file" "${category}_result" pass
    get_required "$file" "${category}_artifact"
    artifact="$VALUE"
    reject_placeholder "${category}_artifact" "$artifact"
    if [[ "$kind" == "report" ]]; then
      [[ "$artifact" == "raw/${category}.artifact" ]] || fail "${category}_artifact-path-invalid"
      get_required "$file" "${category}_sha256"
      require_nonzero_sha256_value "${category}_sha256" "$VALUE"
      get_required "$file" "${category}_criterion"
      criterion_for "$category"
      [[ "$VALUE" == "$CRITERION" ]] || fail "${category}_criterion-invalid"
    else
      [[ -f "$artifact" && ! -L "$artifact" ]] || fail "${category}_artifact-must-be-regular-non-symlink-file"
      [[ -s "$artifact" ]] || fail "${category}_artifact-must-not-be-empty"
    fi
  done
}

validate_service_identity() {
  local file="$1"
  local service_user=""
  local service_uid=""
  require_named_value "$file" service_user
  service_user="$VALUE"
  require_named_value "$file" service_group
  get_required "$file" service_uid
  service_uid="$VALUE"
  [[ "$service_uid" =~ ^[1-9][0-9]*$ ]] || fail "service_uid-must-be-positive-integer"
  get_required "$file" service_gid
  [[ "$VALUE" =~ ^[1-9][0-9]*$ ]] || fail "service_gid-must-be-positive-integer"
  require_named_value "$file" service_permissions_profile
  [[ "$VALUE" == "dedicated-non-root-least-privilege" ]] || fail "service_permissions_profile-must-be-dedicated-non-root-least-privilege"
}

require_absolute_path() {
  local key="$1"
  local value="$2"
  [[ "$value" == /* ]] || fail "$key-must-be-absolute-path"
  [[ "$value" != *".."* ]] || fail "$key-must-not-contain-parent-traversal"
  [[ "$value" != *","* && "$value" != *"="* ]] || fail "$key-malformed-path"
}

require_uuid() {
  local key="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$ ]] || fail "$key-must-be-UUID"
}

require_macho_arm64_binary() {
  local path="$1"
  local mode="$2"
  local source_revision="$3"
  local header=""
  local file_type=""
  local file_size=""
  local load_command_count=""
  local load_command_bytes=""
  local file_description=""
  local tool_output=""

  require_command od
  require_command wc
  require_command file
  header="$(od -An -tx1 -N8 "$path" | tr -d '[:space:]')" || fail "binary-header-read-failed"
  [[ "$header" == "cffaedfe0c000001" ]] || fail "binary-artifact-must-be-native-mach-o-64-arm64"
  file_type="$(od -An -tx1 -j12 -N4 "$path" | tr -d '[:space:]')" || fail "binary-file-type-read-failed"
  [[ "$file_type" == "02000000" ]] || fail "binary-artifact-must-be-Mach-O-executable"
  file_size="$(wc -c < "$path" | tr -d '[:space:]')" || fail "binary-size-read-failed"
  [[ "$file_size" =~ ^[0-9]+$ ]] || fail "binary-size-malformed"
  (( 10#$file_size >= 4096 )) || fail "binary-artifact-too-small-to-be-native-kvnode"
  load_command_count="$(od -An -tu4 -j16 -N4 "$path" | tr -d '[:space:]')" || fail "binary-load-command-count-read-failed"
  load_command_bytes="$(od -An -tu4 -j20 -N4 "$path" | tr -d '[:space:]')" || fail "binary-load-command-size-read-failed"
  [[ "$load_command_count" =~ ^[1-9][0-9]*$ ]] || fail "binary-artifact-must-have-Mach-O-load-commands"
  [[ "$load_command_bytes" =~ ^[1-9][0-9]*$ ]] || fail "binary-artifact-must-have-Mach-O-load-command-bytes"
  (( 32 + 10#$load_command_bytes <= 10#$file_size )) || fail "binary-artifact-Mach-O-load-commands-exceed-file-size"
  file_description="$(file -b "$path")" || fail "file-inspection-failed"
  [[ "$(lowercase "$file_description")" == *"mach-o 64-bit"* && "$(lowercase "$file_description")" == *"arm64"* ]] ||
    fail "file-inspection-must-confirm-Mach-O-64-arm64"

  if [[ "$mode" == "target" ]]; then
    (( 10#$file_size >= 1048576 )) || fail "target-kvnode-binary-must-be-at-least-one-MiB"
    [[ "$(uname -s)" == "Darwin" && "$(uname -m)" == "arm64" ]] || fail "target-Darwin-evidence-must-be-verified-natively-on-Darwin-arm64"
    require_command lipo
    require_command otool
    require_command go
    lipo -verify_arch arm64 "$path" >/dev/null 2>&1 || fail "lipo-must-confirm-arm64-binary"
    tool_output="$(otool -hv "$path" 2>&1)" || fail "otool-must-parse-native-binary"
    [[ "$tool_output" == *"ARM64"* ]] || fail "otool-must-confirm-ARM64-header"
    tool_output="$(go version -m "$path" 2>&1)" || fail "go-version-m-must-parse-target-binary"
    [[ "$tool_output" == *"vcs.revision=$source_revision"* ]] || fail "embedded-binary-source-revision-mismatch"
    [[ "$tool_output" == *"vcs.modified=false"* ]] || fail "target-binary-must-have-unmodified-VCS-provenance"
  fi
}

reject_synthetic_target_scalars() {
  local file="$1"
  local line=""
  local key=""
  local value=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    key="${line%%=*}"
    value="$(lowercase "${line#*=}")"
    case "$key" in
      evidence_mode|release_claim|collector|verifier_version|report_created_at_utc|artifact_count|*_criterion|*_result|*_artifact|*_sha256) continue ;;
    esac
    case "$value" in
      *synthetic*|*test-only*|*test_only*|*fixture*) fail "target-evidence-must-not-contain-synthetic-marker-in-$key" ;;
    esac
  done < "$file"
}
canonical_program_arguments_sha256() {
  local node="$1"
  local binary_path="$2"
  local client_listener="$3"
  local peer_listener="$4"
  local admin_listener="$5"
  local data_directory="$6"
  local tls_cert_path="$7"
  local tls_key_path="$8"
  local tls_ca_path="$9"
  local output=""
  if command -v sha256sum >/dev/null 2>&1; then
    output="$(
      printf '%s\0' \
        "$binary_path" \
        -id "$node" \
        -listen "$client_listener" \
        -peer-listen "$peer_listener" \
        -admin-listen "$admin_listener" \
        -data "$data_directory" \
        -peers "1=https://127.0.0.1:19091,2=https://127.0.0.1:19191,3=https://127.0.0.1:19291" \
        -request-deadline-ms 5000 \
        -peer-deadline-ms 2000 \
        -max-client-body-bytes 1048576 \
        -max-peer-body-bytes 1048576 \
        -max-admin-body-bytes 65536 \
        -max-scan-limit 1000 \
        -tls-cert "$tls_cert_path" \
        -tls-key "$tls_key_path" \
        -tls-ca "$tls_ca_path" |
        sha256sum
    )" || fail "ProgramArguments-sha256-failed"
  elif command -v shasum >/dev/null 2>&1; then
    output="$(
      printf '%s\0' \
        "$binary_path" \
        -id "$node" \
        -listen "$client_listener" \
        -peer-listen "$peer_listener" \
        -admin-listen "$admin_listener" \
        -data "$data_directory" \
        -peers "1=https://127.0.0.1:19091,2=https://127.0.0.1:19191,3=https://127.0.0.1:19291" \
        -request-deadline-ms 5000 \
        -peer-deadline-ms 2000 \
        -max-client-body-bytes 1048576 \
        -max-peer-body-bytes 1048576 \
        -max-admin-body-bytes 65536 \
        -max-scan-limit 1000 \
        -tls-cert "$tls_cert_path" \
        -tls-key "$tls_key_path" \
        -tls-ca "$tls_ca_path" |
        shasum -a 256
    )" || fail "ProgramArguments-sha256-failed"
  else
    fail "missing-required-command-sha256sum-or-shasum"
  fi
  output="${output%%[[:space:]]*}"
  require_nonzero_sha256_value ProgramArguments_sha256 "$output"
  printf '%s' "$output"
}


validate_v1_semantics() {
  local file="$1"
  local kind="$2"
  local target_name=""
  local target_environment=""
  local image_reference=""
  local service_user=""
  local service_group=""
  local volume_mounts=""

  require_exact "$file" evidence_schema "$([[ "$kind" == "input" ]] && printf '%s' "$input_schema" || printf '%s' "$evidence_schema")"
  if [[ "$kind" == "report" ]]; then
    require_exact "$file" collector "$SELF_PATH"
    require_exact "$file" artifact_count "${#categories[@]}"
  fi
  validate_mode_and_signoffs "$file"
  require_exact "$file" collection_scope target-runtime
  require_exact "$file" collection_method pre-captured-read-only
  require_exact "$file" target_execution performed
  require_exact "$file" target_platform linux
  require_exact "$file" orchestrator systemd

  require_named_value "$file" release_id
  require_named_value "$file" target_name
  target_name="$VALUE"
  require_named_value "$file" target_environment
  target_environment="$VALUE"
  reject_local_or_example_target target_name "$target_name"
  reject_local_or_example_target target_environment "$target_environment"
  require_named_value "$file" linux_distribution
  require_named_value "$file" linux_version
  require_named_value "$file" orchestrator_version
  require_named_value "$file" application_version

  get_required "$file" image_reference
  image_reference="$VALUE"
  reject_placeholder image_reference "$image_reference"
  [[ "$image_reference" =~ ^[A-Za-z0-9._:/-]+@sha256:([0-9a-f]{64})$ ]] || fail "image_reference-must-use-immutable-sha256-digest"
  require_nonzero_sha256_value image_digest "${BASH_REMATCH[1]}"
  validate_source_revision "$file"
  validate_service_identity "$file"
  get_required "$file" service_user
  service_user="$VALUE"
  get_required "$file" service_group
  service_group="$VALUE"
  get_required "$file" persistent_volume_mounts
  volume_mounts="$VALUE"
  [[ "$volume_mounts" =~ ^/[^[:space:],=]+(,/[^[:space:],=]+)*$ ]] || fail "persistent_volume_mounts-must-be-absolute-comma-separated-paths"
  [[ "$volume_mounts" != *".."* ]] || fail "persistent_volume_mounts-must-not-contain-parent-traversal"
  require_exact "$file" persistent_volume_owner "${service_user}:${service_group}"
  validate_category_records "$file" "$kind"
  validate_timestamps "$file" "$([[ "$kind" == "report" ]] && printf yes || printf no)"
}

artifact_path_for() {
  local file="$1"
  local kind="$2"
  local base_dir="$3"
  local category="$4"
  get_required "$file" "${category}_artifact"
  if [[ "$kind" == "report" ]]; then
    ARTIFACT_PATH="$base_dir/$VALUE"
  else
    ARTIFACT_PATH="$VALUE"
  fi
}

require_artifact_exact_line() {
  local file="$1"
  local expected="$2"
  local reason="$3"
  local line=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" == "$expected" ]] && return 0
  done < "$file"
  fail "$reason"
}

validate_v2_target_artifact_contracts() {
  local file="$1"
  local kind="$2"
  local base_dir="$3"
  local node=""
  local label=""
  local pid=""
  local plist_path=""
  local plist_hash=""
  local argv_hash=""
  local executable_path=""
  local executable_hash=""
  local client_listener=""
  local peer_listener=""
  local admin_listener=""
  local root=""

  artifact_path_for "$file" "$kind" "$base_dir" source_provenance
  require_artifact_exact_line "$ARTIFACT_PATH" "target_id=$(value_for_output "$file" target_id)" source-provenance-missing-target-id-binding
  require_artifact_exact_line "$ARTIFACT_PATH" "release_id=$(value_for_output "$file" release_id)" source-provenance-missing-release-id-binding
  require_artifact_exact_line "$ARTIFACT_PATH" "source_revision=$(value_for_output "$file" source_revision)" source-provenance-missing-source-revision-binding
  require_artifact_exact_line "$ARTIFACT_PATH" "binary_sha256=$(value_for_output "$file" binary_expected_sha256)" source-provenance-missing-binary-sha256-binding
  require_artifact_exact_line "$ARTIFACT_PATH" "go_vcs_modified=false" source-provenance-must-record-unmodified-build

  for node in 1 2 3; do
    get_required "$file" "node_${node}_label"
    label="$VALUE"
    get_required "$file" "node_${node}_pid"
    pid="$VALUE"
    get_required "$file" "node_${node}_plist_path"
    plist_path="$VALUE"
    get_required "$file" "node_${node}_plist_sha256"
    plist_hash="$VALUE"
    get_required "$file" "node_${node}_program_arguments_sha256"
    argv_hash="$VALUE"
    get_required "$file" "node_${node}_executable_path"
    executable_path="$VALUE"
    get_required "$file" "node_${node}_executable_sha256"
    executable_hash="$VALUE"
    get_required "$file" "node_${node}_client_listener"
    client_listener="$VALUE"
    get_required "$file" "node_${node}_peer_listener"
    peer_listener="$VALUE"
    get_required "$file" "node_${node}_admin_listener"
    admin_listener="$VALUE"

    artifact_path_for "$file" "$kind" "$base_dir" supervisor_verification
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_plist_path=$plist_path" "supervisor-artifact-node-${node}-plist-path-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_plist_sha256=$plist_hash" "supervisor-artifact-node-${node}-plist-hash-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_plutil_lint_command=/usr/bin/plutil -lint $plist_path" "supervisor-artifact-node-${node}-missing-plutil-command"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_plutil_lint_exit=0" "supervisor-artifact-node-${node}-plutil-did-not-pass"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_launchd_bootstrap_command=/bin/launchctl bootstrap system $plist_path" "supervisor-artifact-node-${node}-missing-system-bootstrap-command"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_launchd_bootstrap_exit=0" "supervisor-artifact-node-${node}-bootstrap-did-not-pass"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_launchd_print_command=/bin/launchctl print system/$label" "supervisor-artifact-node-${node}-missing-system-print-command"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_launchd_print_exit=0" "supervisor-artifact-node-${node}-launchd-print-did-not-pass"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_launchd_print_pid=$pid" "supervisor-artifact-node-${node}-live-pid-mismatch"

    artifact_path_for "$file" "$kind" "$base_dir" rendered_argv
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_plist_program_arguments_sha256=$argv_hash" "rendered-argv-node-${node}-plist-arguments-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_launchctl_program_arguments_sha256=$argv_hash" "rendered-argv-node-${node}-launchctl-arguments-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_process_arguments_sha256=$argv_hash" "rendered-argv-node-${node}-process-arguments-mismatch"

    artifact_path_for "$file" "$kind" "$base_dir" process_binary_binding
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_pid=$pid" "process-binding-node-${node}-pid-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_executable_path=$executable_path" "process-binding-node-${node}-executable-path-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_executable_sha256=$executable_hash" "process-binding-node-${node}-executable-hash-mismatch"

    artifact_path_for "$file" "$kind" "$base_dir" network
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_client_listener=$client_listener" "network-artifact-node-${node}-client-listener-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_peer_listener=$peer_listener" "network-artifact-node-${node}-peer-listener-mismatch"
    require_artifact_exact_line "$ARTIFACT_PATH" "node_${node}_admin_listener=$admin_listener" "network-artifact-node-${node}-admin-listener-mismatch"
  done

  artifact_path_for "$file" "$kind" "$base_dir" durable_storage
  for root in apfs_data_root apfs_checkpoint_root apfs_quarantine_root apfs_log_root; do
    require_artifact_exact_line "$ARTIFACT_PATH" "$root=$(value_for_output "$file" "$root")" "durable-storage-artifact-missing-$root"
  done
  require_artifact_exact_line "$ARTIFACT_PATH" "filesystem_type=apfs" durable-storage-artifact-must-record-APFS

  artifact_path_for "$file" "$kind" "$base_dir" boot_persistence
  require_artifact_exact_line "$ARTIFACT_PATH" "boot_observation=real-host-reboot" boot-artifact-must-record-real-host-reboot
  require_artifact_exact_line "$ARTIFACT_PATH" "boot_uuid_before=$(value_for_output "$file" boot_uuid_before)" boot-artifact-pre-boot-UUID-mismatch
  require_artifact_exact_line "$ARTIFACT_PATH" "boot_uuid_after=$(value_for_output "$file" boot_uuid_after)" boot-artifact-post-boot-UUID-mismatch

  artifact_path_for "$file" "$kind" "$base_dir" graceful_stop
  require_artifact_exact_line "$ARTIFACT_PATH" "graceful_signal=SIGTERM" graceful-artifact-must-record-SIGTERM
  require_artifact_exact_line "$ARTIFACT_PATH" "graceful_accepts_stopped=true" graceful-artifact-must-record-accept-stop
  require_artifact_exact_line "$ARTIFACT_PATH" "graceful_inflight_drained=true" graceful-artifact-must-record-inflight-drain
  require_artifact_exact_line "$ARTIFACT_PATH" "graceful_exit_seconds=$(value_for_output "$file" graceful_exit_seconds)" graceful-artifact-exit-time-mismatch
  require_artifact_exact_line "$ARTIFACT_PATH" "graceful_durable_canary=pass" graceful-artifact-must-record-durable-canary

  artifact_path_for "$file" "$kind" "$base_dir" tls
  require_artifact_exact_line "$ARTIFACT_PATH" "tls_scope=server-authentication-only" tls-artifact-must-record-server-auth-only
  require_artifact_exact_line "$ARTIFACT_PATH" "mutual_tls=false" tls-artifact-must-record-no-mTLS
  require_artifact_exact_line "$ARTIFACT_PATH" "client_authorization=false" tls-artifact-must-record-no-client-authorization

  artifact_path_for "$file" "$kind" "$base_dir" security_posture
  require_artifact_exact_line "$ARTIFACT_PATH" "launchd_domain=system" security-artifact-must-record-system-domain
  require_artifact_exact_line "$ARTIFACT_PATH" "host_topology=single-darwin-host" security-artifact-must-record-single-host
  require_artifact_exact_line "$ARTIFACT_PATH" "network_scope=loopback-only" security-artifact-must-record-loopback-only
  require_artifact_exact_line "$ARTIFACT_PATH" "nonclaims=$(value_for_output "$file" nonclaims)" security-artifact-nonclaims-mismatch
  require_artifact_exact_line "$ARTIFACT_PATH" "staging_root_path=$(value_for_output "$file" staging_root_path)" security-artifact-staging-root-path-mismatch
  require_artifact_exact_line "$ARTIFACT_PATH" "staging_root_writable=true" security-artifact-must-record-writable-staging-root
  require_artifact_exact_line "$ARTIFACT_PATH" "final_evidence_root_path=$(value_for_output "$file" final_evidence_root_path)" security-artifact-final-evidence-root-path-mismatch
  require_artifact_exact_line "$ARTIFACT_PATH" "final_evidence_root_read_only_required=true" security-artifact-must-record-final-read-only-requirement
  require_artifact_exact_line "$ARTIFACT_PATH" "final_evidence_root_external=true" security-artifact-must-record-final-external-evidence-root
  require_artifact_exact_line "$ARTIFACT_PATH" "final_evidence_image_format=udro" security-artifact-must-record-final-UDRO-image

  artifact_path_for "$file" "$kind" "$base_dir" rollback
  require_artifact_exact_line "$ARTIFACT_PATH" "rollback_bundle_uri=$(value_for_output "$file" rollback_bundle_uri)" rollback-artifact-bundle-URI-mismatch
  require_artifact_exact_line "$ARTIFACT_PATH" "rollback_bundle_sha256=$(value_for_output "$file" rollback_bundle_sha256)" rollback-artifact-bundle-hash-mismatch
  require_artifact_exact_line "$ARTIFACT_PATH" "rollback_bundle_immutable=true" rollback-artifact-must-record-immutable-bundle
  require_artifact_exact_line "$ARTIFACT_PATH" "rollback_binary_format=mach-o-64" rollback-artifact-must-record-Mach-O-64
  require_artifact_exact_line "$ARTIFACT_PATH" "rollback_architecture=arm64" rollback-artifact-must-record-arm64
}

validate_v2_semantics() {
  local file="$1"
  local kind="$2"
  local mode=""
  local staging_root_path=""
  local final_evidence_root_path=""
  local source_revision=""
  local release_id=""
  local binary_path=""
  local binary_hash=""
  local binary_artifact=""
  local data_root=""
  local root=""
  local node=""
  local label=""
  local expected_label=""
  local pid=""
  local pids="|"
  local expected_client=""
  local expected_peer=""
  local expected_admin=""
  local plist_hash=""
  local plist_hashes="|"
  local argv_hash=""
  local canonical_argv_hash=""
  local tls_ca_path=""
  local tls_cert_path=""
  local tls_key_path=""
  local rollback_hash=""
  local argv_hashes="|"
  local before_uuid=""
  local after_uuid=""
  local exit_seconds=""

  require_exact "$file" evidence_schema "$([[ "$kind" == "input" ]] && printf '%s' "$input_schema" || printf '%s' "$evidence_schema")"
  if [[ "$kind" == "report" ]]; then
    require_exact "$file" collector "$SELF_PATH"
    require_exact "$file" verifier_version darwin-v2
    require_exact "$file" artifact_count "${#categories[@]}"
  fi
  validate_mode_and_signoffs "$file"
  get_required "$file" evidence_mode
  mode="$VALUE"
  if [[ "$mode" == "target" ]]; then
    reject_synthetic_target_scalars "$file"
  fi
  require_exact "$file" collection_scope target-runtime
  require_exact "$file" collection_method pre-captured-staging-to-read-only-final
  require_exact "$file" target_execution performed
  require_exact "$file" target_id mc-kv-darwin24-arm64-launchd-3n-r1
  require_exact "$file" target_environment native-darwin24-arm64-launchd-system-domain-v1
  require_exact "$file" deployment_profile native-darwin24-arm64-launchd-system-domain-v1
  require_exact "$file" target_platform darwin
  require_exact "$file" architecture arm64
  require_exact "$file" binary_format mach-o-64
  require_exact "$file" execution_mode native
  require_exact "$file" orchestrator launchd
  require_exact "$file" launchd_domain system

  require_named_value "$file" release_id
  release_id="$VALUE"
  validate_source_revision "$file"
  get_required "$file" source_revision
  source_revision="$VALUE"
  require_exact "$file" binary_source_revision "$source_revision"
  get_required "$file" binary_expected_sha256
  binary_hash="$VALUE"
  require_nonzero_sha256_value binary_expected_sha256 "$binary_hash"
  require_exact "$file" binary_immutable true
  get_required "$file" binary_path
  binary_path="$VALUE"
  require_absolute_path binary_path "$binary_path"
  [[ "$binary_path" == "/var/db/moreconsensus/releases/${release_id}-${binary_hash}/bin/kvnode" ]] || fail "binary_path-must-be-release-and-content-addressed-immutable-path"
  require_exact "$file" binary_uri "file:$binary_path"

  get_required "$file" darwin_version
  [[ "$VALUE" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || fail "darwin_version-malformed"
  get_required "$file" macos_version
  [[ "$VALUE" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || fail "macos_version-malformed"
  get_required "$file" os_build
  [[ "$VALUE" =~ ^[0-9]{2}[A-Z][A-Za-z0-9]+$ ]] || fail "os_build-malformed"
  get_required "$file" kernel_version
  reject_placeholder kernel_version "$VALUE"
  [[ "$VALUE" == "Darwin Kernel Version "* ]] || fail "kernel_version-must-describe-Darwin-kernel"
  require_exact "$file" filesystem_type apfs
  get_required "$file" staging_root_path
  staging_root_path="$VALUE"
  require_absolute_path staging_root_path "$staging_root_path"
  require_exact "$file" staging_root_uri "file:$staging_root_path"
  require_exact "$file" staging_root_filesystem apfs
  require_exact "$file" staging_root_writable true
  get_required "$file" final_evidence_root_path
  final_evidence_root_path="$VALUE"
  require_absolute_path final_evidence_root_path "$final_evidence_root_path"
  require_exact "$file" final_evidence_root_uri "file:$final_evidence_root_path"
  require_exact "$file" final_evidence_root_filesystem apfs
  require_exact "$file" final_evidence_root_read_only_required true
  require_exact "$file" final_evidence_root_external true
  require_exact "$file" final_evidence_image_format udro
  [[ "$staging_root_path" != "$final_evidence_root_path" ]] || fail "staging-root-must-differ-from-final-read-only-root"
  if [[ "$mode" == "target" ]]; then
    [[ "$final_evidence_root_path" == "/Volumes/mc-kv-evidence-${release_id}" ]] || fail "target-final-evidence-root-must-use-release-bound-/Volumes-mount"
  fi

  get_required "$file" apfs_data_root
  data_root="$VALUE"
  for root in apfs_data_root apfs_checkpoint_root apfs_quarantine_root apfs_log_root; do
    get_required "$file" "$root"
    require_absolute_path "$root" "$VALUE"
    [[ "$VALUE" == /var/db/moreconsensus/* ]] || fail "$root-must-be-under-/var/db/moreconsensus"
  done
  get_required "$file" apfs_checkpoint_root
  [[ "$VALUE" != "$data_root" ]] || fail "apfs-roots-must-be-distinct"
  get_required "$file" apfs_quarantine_root
  [[ "$VALUE" != "$data_root" ]] || fail "apfs-roots-must-be-distinct"
  get_required "$file" apfs_log_root
  [[ "$VALUE" != "$data_root" ]] || fail "apfs-roots-must-be-distinct"

  validate_service_identity "$file"
  get_required "$file" service_user
  [[ "$(lowercase "$VALUE")" != "root" ]] || fail "service_user-must-not-be-root"
  require_exact "$file" host_topology single-darwin-host
  require_exact "$file" network_scope loopback-only
  require_exact "$file" tls_scope server-authentication-only
  get_required "$file" tls_ca_path
  tls_ca_path="$VALUE"
  require_absolute_path tls_ca_path "$tls_ca_path"
  [[ "$tls_ca_path" == /var/db/moreconsensus/* ]] || fail "tls_ca_path-must-be-under-/var/db/moreconsensus"
  require_exact "$file" mutual_tls false
  require_exact "$file" client_authorization false
  require_exact "$file" nonclaims same-host,loopback-only,no-independent-failure-domain,server-auth-tls-only,no-client-authorization,no-production-capacity,no-off-host-backup

  require_exact "$file" boot_observation real-host-reboot
  require_exact "$file" boot_observation_synthetic false
  get_required "$file" boot_uuid_before
  before_uuid="$VALUE"
  require_uuid boot_uuid_before "$before_uuid"
  get_required "$file" boot_uuid_after
  after_uuid="$VALUE"
  require_uuid boot_uuid_after "$after_uuid"
  [[ "$(lowercase "$before_uuid")" != "$(lowercase "$after_uuid")" ]] || fail "boot-UUID-must-transition-across-real-host-reboot"

  require_exact "$file" graceful_signal SIGTERM
  require_exact "$file" graceful_accepts_stopped true
  require_exact "$file" graceful_inflight_drained true
  get_required "$file" graceful_exit_seconds
  exit_seconds="$VALUE"
  [[ "$exit_seconds" =~ ^[1-9][0-9]*$ ]] || fail "graceful_exit_seconds-must-be-positive-integer"
  (( 10#$exit_seconds <= 30 )) || fail "graceful_exit_seconds-exceeds-30-second-deadline"
  require_exact "$file" graceful_durable_canary pass

  get_required "$file" rollback_bundle_sha256
  rollback_hash="$VALUE"
  require_nonzero_sha256_value rollback_bundle_sha256 "$rollback_hash"
  get_required "$file" rollback_bundle_uri
  [[ "$VALUE" == "file:/var/db/moreconsensus/releases/prior-${rollback_hash}/bundles/"* ]] || fail "rollback_bundle_uri-must-be-content-addressed-native-immutable-file"
  require_exact "$file" rollback_bundle_immutable true
  require_exact "$file" rollback_binary_format mach-o-64
  require_exact "$file" rollback_architecture arm64

  for node in 1 2 3; do
    case "$node" in
      1)
        expected_label="org.gosuda.moreconsensus.kvnode.1"
        expected_client="127.0.0.1:19090"
        expected_peer="127.0.0.1:19091"
        expected_admin="127.0.0.1:19092"
        ;;
      2)
        expected_label="org.gosuda.moreconsensus.kvnode.2"
        expected_client="127.0.0.1:19190"
        expected_peer="127.0.0.1:19191"
        expected_admin="127.0.0.1:19192"
        ;;
      3)
        expected_label="org.gosuda.moreconsensus.kvnode.3"
        expected_client="127.0.0.1:19290"
        expected_peer="127.0.0.1:19291"
        expected_admin="127.0.0.1:19292"
        ;;
    esac
    require_exact "$file" "node_${node}_label" "$expected_label"
    get_required "$file" "node_${node}_pid"
    pid="$VALUE"
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || fail "node_${node}_pid-must-be-positive-integer"
    case "$pids" in *"|${pid}|"*) fail "node-PIDs-must-be-distinct" ;; esac
    pids="${pids}${pid}|"
    require_exact "$file" "node_${node}_client_listener" "$expected_client"
    require_exact "$file" "node_${node}_peer_listener" "$expected_peer"
    require_exact "$file" "node_${node}_admin_listener" "$expected_admin"
    require_exact "$file" "node_${node}_data_directory" "$data_root/node${node}"
    require_exact "$file" "node_${node}_plist_path" "/Library/LaunchDaemons/${expected_label}.plist"
    get_required "$file" "node_${node}_tls_cert_path"
    tls_cert_path="$VALUE"
    require_absolute_path "node_${node}_tls_cert_path" "$tls_cert_path"
    [[ "$tls_cert_path" == /var/db/moreconsensus/* ]] || fail "node_${node}_tls_cert_path-must-be-under-/var/db/moreconsensus"
    get_required "$file" "node_${node}_tls_key_path"
    tls_key_path="$VALUE"
    require_absolute_path "node_${node}_tls_key_path" "$tls_key_path"
    [[ "$tls_key_path" == /var/db/moreconsensus/* ]] || fail "node_${node}_tls_key_path-must-be-under-/var/db/moreconsensus"
    get_required "$file" "node_${node}_plist_sha256"
    plist_hash="$VALUE"
    require_nonzero_sha256_value "node_${node}_plist_sha256" "$plist_hash"
    case "$plist_hashes" in *"|${plist_hash}|"*) fail "node-plist-hashes-must-be-distinct" ;; esac
    plist_hashes="${plist_hashes}${plist_hash}|"
    require_exact "$file" "node_${node}_plutil_lint_result" pass
    require_exact "$file" "node_${node}_launchd_bootstrap_result" pass
    require_exact "$file" "node_${node}_launchd_print_result" pass
    get_required "$file" "node_${node}_program_arguments_sha256"
    argv_hash="$VALUE"
    require_nonzero_sha256_value "node_${node}_program_arguments_sha256" "$argv_hash"
    canonical_argv_hash="$(
      canonical_program_arguments_sha256 \
        "$node" \
        "$binary_path" \
        "$expected_client" \
        "$expected_peer" \
        "$expected_admin" \
        "$data_root/node${node}" \
        "$tls_cert_path" \
        "$tls_key_path" \
        "$tls_ca_path"
    )"
    [[ "$argv_hash" == "$canonical_argv_hash" ]] || fail "node_${node}_program_arguments_sha256-does-not-match-exact-ProgramArguments"
    case "$argv_hashes" in *"|${argv_hash}|"*) fail "node-ProgramArguments-hashes-must-be-distinct" ;; esac
    argv_hashes="${argv_hashes}${argv_hash}|"
    require_exact "$file" "node_${node}_launchctl_program_arguments_sha256" "$argv_hash"
    require_exact "$file" "node_${node}_process_arguments_sha256" "$argv_hash"
    require_exact "$file" "node_${node}_executable_path" "$binary_path"
    require_exact "$file" "node_${node}_executable_sha256" "$binary_hash"
  done

  validate_category_records "$file" "$kind"
  if [[ "$kind" == "input" && "$mode" == "target" ]]; then
    validate_v2_target_artifact_contracts "$file" input ""
  fi
  if [[ "$kind" == "input" ]]; then
    get_required "$file" binary_artifact
    binary_artifact="$VALUE"
    require_macho_arm64_binary "$binary_artifact" "$mode" "$source_revision"
    [[ "$(sha256_file "$binary_artifact")" == "$binary_hash" ]] || fail "binary_expected_sha256-does-not-match-native-binary-artifact"
  else
    get_required "$file" binary_sha256
    [[ "$VALUE" == "$binary_hash" ]] || fail "binary_expected_sha256-does-not-match-collected-binary-sha256"
  fi
  validate_timestamps "$file" "$([[ "$kind" == "report" ]] && printf yes || printf no)"
}

validate_common_semantics() {
  local file="$1"
  local kind="$2"
  case "$contract_version" in
    v1) validate_v1_semantics "$file" "$kind" ;;
    v2) validate_v2_semantics "$file" "$kind" ;;
    *) fail "internal-contract-version-not-active" ;;
  esac
}

sha256_file() {
  local path="$1"
  local output=""
  if command -v sha256sum >/dev/null 2>&1; then
    output="$(sha256sum "$path")" || fail "sha256-failed-$path"
  elif command -v shasum >/dev/null 2>&1; then
    output="$(shasum -a 256 "$path")" || fail "sha256-failed-$path"
  else
    fail "missing-required-command-sha256sum-or-shasum"
  fi
  output="${output%%[[:space:]]*}"
  require_nonzero_sha256_value artifact_sha256 "$output"
  printf '%s' "$output"
}
verify_target_read_only_evidence_root() {
  local report="$1"
  local report_dir="$2"
  local root=""
  local root_physical=""
  local disk_info=""
  local compact_info=""
  local volume_uuid=""

  get_required "$report" final_evidence_root_path
  root="$VALUE"
  [[ -d "$root" && ! -L "$root" ]] || fail "final-evidence-root-must-be-real-mounted-directory"
  root_physical="$(cd "$root" && pwd -P)" || fail "final-evidence-root-cannot-be-resolved"
  case "$report_dir" in
    "$root_physical"|"$root_physical"/*) ;;
    *) fail "deployment-report-must-reside-under-final-evidence-root" ;;
  esac
  [[ ! -w "$root_physical" ]] || fail "final-evidence-root-must-be-observed-read-only"
  require_command diskutil
  disk_info="$(diskutil info "$root_physical" 2>&1)" || fail "diskutil-must-inspect-final-evidence-root"
  compact_info="$(printf '%s' "$disk_info" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
  [[ "$compact_info" == *"filesystempersonality:apfs"* ]] || fail "final-evidence-root-filesystem-must-be-observed-APFS"
  if [[ "$compact_info" =~ volumeuuid:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) ]]; then
    volume_uuid="${BASH_REMATCH[1]}"
  else
    fail "final-evidence-root-volume-UUID-missing"
  fi
  [[ "$compact_info" == *"volumeread-only:yes"* ]] || fail "final-evidence-root-volume-must-be-observed-read-only"
  [[ "$compact_info" == *"protocol:diskimage"* || "$compact_info" == *"virtual:yes"* ]] ||
    fail "final-evidence-root-must-be-observed-external-disk-image"
  OBSERVED_EVIDENCE_ROOT="$root_physical"
  OBSERVED_VOLUME_UUID="$volume_uuid"
}
verify_writable_staging_root() {
  local input="$1"
  local output_dir="$2"
  local root=""
  local disk_info=""
  local compact_info=""

  get_required "$input" staging_root_path
  root="$VALUE"
  [[ -d "$root" && ! -L "$root" ]] || fail "staging-root-must-be-real-mounted-directory"
  [[ -w "$root" ]] || fail "staging-root-must-be-observed-writable"
  require_absolute_path output_dir "$output_dir"
  case "$output_dir" in
    "$root"/*) ;;
    *) fail "v2-output-dir-must-reside-under-staging-root" ;;
  esac
  require_command diskutil
  disk_info="$(diskutil info "$root" 2>&1)" || fail "diskutil-must-inspect-staging-root"
  compact_info="$(printf '%s' "$disk_info" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
  [[ "$compact_info" == *"filesystempersonality:apfs"* ]] || fail "staging-root-filesystem-must-be-observed-APFS"
  [[ "$compact_info" == *"volumeread-only:no"* ]] || fail "staging-root-must-not-masquerade-as-read-only-final-root"
}



collect_evidence() {
  local input=""
  local output_dir=""
  local created_output="no"
  local category=""
  local source=""
  local destination=""
  local key=""
  local now=""
  local tmp_report=""
  local report=""

  while (( $# > 0 )); do
    case "$1" in
      --input)
        (( $# >= 2 )) || fail "--input-requires-a-path"
        input="$2"
        shift 2
        ;;
      --output-dir)
        (( $# >= 2 )) || fail "--output-dir-requires-a-path"
        output_dir="$2"
        shift 2
        ;;
      --help|-h)
        usage
        return 0
        ;;
      *) fail "unknown-collect-option-$1" ;;
    esac
  done

  [[ -n "$input" ]] || fail "collect-requires---input"
  [[ -n "$output_dir" ]] || fail "collect-requires-explicit---output-dir"
  [[ "$output_dir" != "." && "$output_dir" != "/" ]] || fail "output-dir-must-be-a-named-new-directory"
  [[ ! -e "$output_dir" && ! -L "$output_dir" ]] || fail "output-dir-already-exists"

  validate_record_shape "$input" input
  validate_common_semantics "$input" input
  if [[ "$contract_version" == "v2" ]]; then
    verify_writable_staging_root "$input" "$output_dir"
  fi

  umask 077
  mkdir -p "$(dirname "$output_dir")"
  mkdir "$output_dir"
  created_output="yes"
  mkdir "$output_dir/raw"
  report="$output_dir/evidence.env"
  tmp_report="$output_dir/.evidence.env.tmp"

  cleanup_partial_collection() {
    local status="$?"
    if (( status != 0 )) && [[ "$created_output" == "yes" && -n "$output_dir" && "$output_dir" != "." && "$output_dir" != "/" ]]; then
      rm -rf "$output_dir"
    fi
    return "$status"
  }
  trap cleanup_partial_collection EXIT

  for category in "${categories[@]}"; do
    get_required "$input" "${category}_artifact"
    source="$VALUE"
    destination="$output_dir/raw/${category}.artifact"
    cp "$source" "$destination"
    chmod 0400 "$destination"
  done

  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  {
    echo "evidence_schema=$evidence_schema"
    echo "collector=$SELF_PATH"
    if [[ "$contract_version" == "v2" ]]; then
      echo "verifier_version=darwin-v2"
    fi
    for key in "${candidate_scalar_keys[@]}"; do
      case "$key" in
        evidence_schema) ;;
        *)
          get_required "$input" "$key"
          printf '%s=%s\n' "$key" "$VALUE"
          ;;
      esac
    done
    echo "report_created_at_utc=$now"
    echo "artifact_count=${#categories[@]}"
    for category in "${categories[@]}"; do
      criterion_for "$category"
      printf '%s_criterion=%s\n' "$category" "$CRITERION"
      echo "${category}_result=pass"
      echo "${category}_artifact=raw/${category}.artifact"
      destination="$output_dir/raw/${category}.artifact"
      printf '%s_sha256=%s\n' "$category" "$(sha256_file "$destination")"
    done
  } > "$tmp_report"
  chmod 0600 "$tmp_report"
  mv "$tmp_report" "$report"
  created_output="no"
  trap - EXIT

  if [[ "$contract_version" == "v2" ]]; then
    verify_evidence "$report" staging
  else
    verify_evidence "$report" final
  fi
  printf '%s status=collected report=%s release_claim=%s\n' "$PROGRAM" "$report" "$(value_for_output "$report" release_claim)"
}

value_for_output() {
  local file="$1"
  local key="$2"
  get_required "$file" "$key"
  printf '%s' "$VALUE"
}

verify_evidence() {
  local report="$1"
  local phase="${2:-final}"
  local report_dir=""
  local category=""
  local artifact=""
  local recorded_hash=""
  local actual_hash=""
  local binary_relative=""
  local mode=""
  local source_revision=""

  [[ -n "$report" ]] || fail "verify-requires-report-path"
  validate_record_shape "$report" report
  validate_common_semantics "$report" report

  report_dir="$(cd "$(dirname "$report")" && pwd -P)"
  if [[ "$contract_version" == "v2" && "$phase" == "final" ]]; then
    get_required "$report" evidence_mode
    mode="$VALUE"
    if [[ "${KVNODE_DEPLOYMENT_TEST_SKIP_FINAL_ROOT_OBSERVATION:-}" == "yes" ]]; then
      [[ "$mode" == "test-only-synthetic" && "${KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE:-}" == "yes" ]] ||
        fail "final-root-observation-bypass-is-test-fixture-only"
      OBSERVED_EVIDENCE_ROOT="test-only-final-root-observation-bypassed"
      OBSERVED_VOLUME_UUID="test-only-final-root-observation-bypassed"
    else
      verify_target_read_only_evidence_root "$report" "$report_dir"
    fi
  fi
  [[ ! -L "$report_dir/raw" && -d "$report_dir/raw" ]] || fail "raw-artifact-directory-must-be-real-directory"
  for category in "${categories[@]}"; do
    get_required "$report" "${category}_artifact"
    artifact="$report_dir/$VALUE"
    [[ -f "$artifact" && ! -L "$artifact" ]] || fail "${category}_raw-artifact-must-be-regular-non-symlink-file"
    [[ -s "$artifact" ]] || fail "${category}_raw-artifact-must-not-be-empty"
    get_required "$report" "${category}_sha256"
    recorded_hash="$VALUE"
    actual_hash="$(sha256_file "$artifact")"
    [[ "$actual_hash" == "$recorded_hash" ]] || fail "${category}_raw-artifact-sha256-mismatch"
  done
  if [[ "$contract_version" == "v2" ]]; then
    get_required "$report" evidence_mode
    mode="$VALUE"
    if [[ "$mode" == "target" ]]; then
      validate_v2_target_artifact_contracts "$report" report "$report_dir"
    fi
  fi

  get_required "$report" binary_sha256
  recorded_hash="$VALUE"
  get_required "$report" binary_artifact
  binary_relative="$VALUE"
  actual_hash="$(sha256_file "$report_dir/$binary_relative")"
  [[ "$actual_hash" == "$recorded_hash" ]] || fail "binary-checksum-does-not-bind-deployed-binary"
  if [[ "$contract_version" == "v2" ]]; then
    get_required "$report" evidence_mode
    mode="$VALUE"
    get_required "$report" source_revision
    source_revision="$VALUE"
    require_macho_arm64_binary "$report_dir/$binary_relative" "$mode" "$source_revision"
  fi

  get_required "$report" deployment_manifest_sha256
  recorded_hash="$VALUE"
  get_required "$report" deployment_manifest_artifact
  actual_hash="$(sha256_file "$report_dir/$VALUE")"
  [[ "$actual_hash" == "$recorded_hash" ]] || fail "manifest-checksum-does-not-bind-deployed-manifest"

  get_required "$report" rendered_argv_sha256
  recorded_hash="$VALUE"
  get_required "$report" rendered_argv_artifact
  actual_hash="$(sha256_file "$report_dir/$VALUE")"
  [[ "$actual_hash" == "$recorded_hash" ]] || fail "rendered-argv-checksum-does-not-bind-raw-artifact"

  if [[ "$contract_version" == "v1" ]]; then
    printf '%s status=verified evidence_mode=%s release_id=%s target=%s source_revision=%s release_claim=%s\n' \
      "$PROGRAM" \
      "$(value_for_output "$report" evidence_mode)" \
      "$(value_for_output "$report" release_id)" \
      "$(value_for_output "$report" target_name)" \
      "$(value_for_output "$report" source_revision)" \
      "$(value_for_output "$report" release_claim)"
  elif [[ "$phase" == "staging" ]]; then
    printf '%s status=staged verifier_version=%s evidence_mode=%s release_id=%s target_id=%s source_revision=%s binary_sha256=%s staging_root_writable=observed-true staging_root_path=%s final_evidence_root_read_only=not-observed limitations=%s release_claim=%s\n' \
      "$PROGRAM" \
      "$(value_for_output "$report" verifier_version)" \
      "$(value_for_output "$report" evidence_mode)" \
      "$(value_for_output "$report" release_id)" \
      "$(value_for_output "$report" target_id)" \
      "$(value_for_output "$report" source_revision)" \
      "$(value_for_output "$report" binary_sha256)" \
      "$(value_for_output "$report" staging_root_path)" \
      "$(value_for_output "$report" nonclaims)" \
      "$(value_for_output "$report" release_claim)"
  else
    printf '%s status=verified verifier_version=%s evidence_mode=%s release_id=%s target_id=%s source_revision=%s binary_sha256=%s final_evidence_root_read_only=observed-true final_evidence_root_path=%s evidence_volume_uuid=%s limitations=%s release_claim=%s\n' \
      "$PROGRAM" \
      "$(value_for_output "$report" verifier_version)" \
      "$(value_for_output "$report" evidence_mode)" \
      "$(value_for_output "$report" release_id)" \
      "$(value_for_output "$report" target_id)" \
      "$(value_for_output "$report" source_revision)" \
      "$(value_for_output "$report" binary_sha256)" \
      "$OBSERVED_EVIDENCE_ROOT" \
      "$OBSERVED_VOLUME_UUID" \
      "$(value_for_output "$report" nonclaims)" \
      "$(value_for_output "$report" release_claim)"
  fi
}

print_input_schema() {
  local version="$1"
  local key=""
  local category=""
  case "$version" in
    v1) activate_contract kvnode-target-deployment-input-v1 ;;
    v2) activate_contract kvnode-target-deployment-input-v2 ;;
    *) fail "internal-print-schema-version-invalid" ;;
  esac
  echo "evidence_schema=$input_schema"
  for key in "${candidate_scalar_keys[@]}"; do
    [[ "$key" == "evidence_schema" ]] || printf '%s=<required>\n' "$key"
  done
  for category in "${categories[@]}"; do
    printf '%s_result=pass\n' "$category"
    printf '%s_artifact=<non-empty-regular-file>\n' "$category"
  done
}

require_command date
require_command dirname
require_command mkdir
require_command cp
require_command chmod
require_command mv
require_command rm
require_command tr

case "${1:-}" in
  collect)
    shift
    collect_evidence "$@"
    ;;
  verify)
    shift
    (( $# == 1 )) || fail "verify-requires-exactly-one-report-path"
    verify_evidence "$1"
    ;;
  --print-input-schema)
    (( $# == 1 )) || fail "--print-input-schema-takes-no-arguments"
    print_input_schema v1
    ;;
  --print-input-schema-v2)
    (( $# == 1 )) || fail "--print-input-schema-v2-takes-no-arguments"
    print_input_schema v2
    ;;
  --help|-h|"")
    usage
    ;;
  *)
    fail "unknown-command-${1:-empty}"
    ;;
esac
