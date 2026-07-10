#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
SCRIPT="$ROOT/tests/kvnode_target_deployment_evidence.sh"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/kvnode-target-deployment-evidence-test.XXXXXX")"
DARWIN_STAGING_ATTACHED=no
DARWIN_FINAL_ATTACHED=no
DARWIN_STAGING_ROOT=""
DARWIN_FINAL_ROOT=""
cleanup() {
  set +e
  if [[ "$DARWIN_FINAL_ATTACHED" == "yes" && -n "$DARWIN_FINAL_ROOT" ]]; then
    /usr/bin/hdiutil detach "$DARWIN_FINAL_ROOT" >/dev/null 2>&1
  fi
  if [[ "$DARWIN_STAGING_ATTACHED" == "yes" && -n "$DARWIN_STAGING_ROOT" ]]; then
    /usr/bin/hdiutil detach "$DARWIN_STAGING_ROOT" >/dev/null 2>&1
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

fail() {
  echo "kvnode-target-deployment-evidence-test status=fail reason=$*" >&2
  exit 1
}
file_sha256() {
  local path="$1"
  local output=""
  if command -v sha256sum >/dev/null 2>&1; then
    output="$(sha256sum "$path")"
  else
    output="$(shasum -a 256 "$path")"
  fi
  printf '%s' "${output%%[[:space:]]*}"
}
fixture_program_arguments_sha256() {
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
    )"
  else
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
    )"
  fi
  printf '%s' "${output%%[[:space:]]*}"
}


categories=(
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

SOURCE_DIR="$TMP_ROOT/source-artifacts"
INPUT="$TMP_ROOT/synthetic-input.env"
BUNDLE="$TMP_ROOT/synthetic-bundle"
mkdir "$SOURCE_DIR"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

for category in "${categories[@]}"; do
  {
    echo "fixture=test-only-synthetic"
    echo "category=$category"
    echo "result=pass"
    echo "captured_at_utc=$now"
  } > "$SOURCE_DIR/${category}.artifact"
done
printf '\177ELF synthetic test-only binary bytes\n' > "$SOURCE_DIR/binary.artifact"
printf '/opt/kvnode/kvnode -id 1 -listen 10.44.0.11:8080 -peer-listen 10.44.0.11:8081 -admin-listen 10.44.0.11:8082 -data /var/lib/kvnode/1\n' > "$SOURCE_DIR/rendered_argv.artifact"
printf '[Service]\nExecStart=/opt/kvnode/kvnode\nUser=kvnode\nGroup=kvnode\n' > "$SOURCE_DIR/deployment_manifest.artifact"
printf 'fixture=test-only-synthetic systemd-analyze-verify-exit=0 result=pass\n' > "$SOURCE_DIR/systemd_verification.artifact"

cat > "$INPUT" <<EOF
evidence_schema=kvnode-target-deployment-input-v1
evidence_mode=test-only-synthetic
release_claim=test-only-synthetic-deployment-accepted
release_id=synthetic-release-20260710-a
collection_scope=target-runtime
collection_method=pre-captured-read-only
target_execution=performed
target_name=synthetic-zone-a-node-1
target_environment=synthetic-staging-a
target_platform=linux
linux_distribution=synthetic-linux
linux_version=9.4
orchestrator=systemd
orchestrator_version=255.7
application_version=1.2.3
image_reference=registry.invalid/kvnode@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
source_revision=1234567890abcdef1234567890abcdef12345678
service_user=kvnode
service_group=kvnode
service_uid=41001
service_gid=41001
service_permissions_profile=dedicated-non-root-least-privilege
persistent_volume_mounts=/var/lib/kvnode/1,/var/log/kvnode
persistent_volume_owner=kvnode:kvnode
collection_started_at_utc=$now
collection_completed_at_utc=$now
operator_identity=synthetic-operator-a
operator_signoff=approved
operator_signed_at_utc=$now
reviewer_identity=synthetic-reviewer-b
reviewer_signoff=approved
reviewer_signed_at_utc=$now
EOF
for category in "${categories[@]}"; do
  {
    echo "${category}_result=pass"
    echo "${category}_artifact=$SOURCE_DIR/${category}.artifact"
  } >> "$INPUT"
done

collect_fixture() {
  KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" collect --input "$INPUT" --output-dir "$BUNDLE"
}

verify_fixture() {
  KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" verify "$1"
}

expect_verify_reject() {
  local label="$1"
  local expected="$2"
  local report="$3"
  local output=""
  local status=0
  set +e
  output="$(KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes KVNODE_DEPLOYMENT_TEST_SKIP_FINAL_ROOT_OBSERVATION=yes "$SCRIPT" verify "$report" 2>&1)"
  status=$?
  set -e
  (( status != 0 )) || fail "$label-was-accepted"
  [[ "$output" == *"$expected"* ]] || fail "$label-rejected-for-unexpected-reason-output=$output"
}

expect_command_reject() {
  local label="$1"
  local expected="$2"
  shift 2
  local output=""
  local status=0
  set +e
  output="$("$@" 2>&1)"
  status=$?
  set -e
  (( status != 0 )) || fail "$label-was-accepted"
  [[ "$output" == *"$expected"* ]] || fail "$label-rejected-for-unexpected-reason-output=$output"
}

CASE_DIR=""
CASE_REPORT=""
fresh_case() {
  local label="$1"
  CASE_DIR="$TMP_ROOT/case-$label"
  mkdir "$CASE_DIR"
  cp -R "$BUNDLE/." "$CASE_DIR"
  CASE_REPORT="$CASE_DIR/evidence.env"
  chmod 0600 "$CASE_REPORT"
}

replace_field() {
  local file="$1"
  local wanted="$2"
  local replacement="$3"
  local tmp="$file.tmp"
  local line=""
  local key=""
  local found=0
  : > "$tmp"
  while IFS= read -r line || [[ -n "$line" ]]; do
    key="${line%%=*}"
    if [[ "$key" == "$wanted" ]]; then
      printf '%s=%s\n' "$wanted" "$replacement" >> "$tmp"
      found=1
    else
      printf '%s\n' "$line" >> "$tmp"
    fi
  done < "$file"
  (( found == 1 )) || fail "replace-field-missing-$wanted"
  mv "$tmp" "$file"
}

remove_field() {
  local file="$1"
  local wanted="$2"
  local tmp="$file.tmp"
  local line=""
  local key=""
  local found=0
  : > "$tmp"
  while IFS= read -r line || [[ -n "$line" ]]; do
    key="${line%%=*}"
    if [[ "$key" == "$wanted" ]]; then
      found=1
    else
      printf '%s\n' "$line" >> "$tmp"
    fi
  done < "$file"
  (( found == 1 )) || fail "remove-field-missing-$wanted"
  mv "$tmp" "$file"
}

collect_output="$(collect_fixture 2>&1)" || fail "synthetic-collection-failed-output=$collect_output"
[[ -f "$BUNDLE/evidence.env" ]] || fail "collector-did-not-write-report"
[[ -d "$BUNDLE/raw" ]] || fail "collector-did-not-write-raw-artifacts"
positive_output="$(verify_fixture "$BUNDLE/evidence.env" 2>&1)" || fail "synthetic-positive-fixture-failed-output=$positive_output"
[[ "$positive_output" == *"status=verified"* ]] || fail "positive-fixture-did-not-verify"
[[ "$positive_output" == *"evidence_mode=test-only-synthetic"* ]] || fail "positive-fixture-lost-test-only-marker"
[[ "$positive_output" == *"release_id=synthetic-release-20260710-a"* ]] || fail "positive-fixture-lost-release-id"

expect_command_reject fixture-without-opt-in test-fixture-requires-explicit-opt-in "$SCRIPT" verify "$BUNDLE/evidence.env"
expect_command_reject collection-without-output-dir collect-requires-explicit---output-dir env KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" collect --input "$INPUT"
expect_command_reject unsupported-live-option unknown-collect-option---live env KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" collect --input "$INPUT" --output-dir "$TMP_ROOT/live-output" --live

fresh_case missing-field
remove_field "$CASE_REPORT" metrics_sha256
expect_verify_reject missing-field missing-required-field-metrics_sha256 "$CASE_REPORT"

fresh_case missing-release-id
remove_field "$CASE_REPORT" release_id
expect_verify_reject missing-release-id missing-required-field-release_id "$CASE_REPORT"

fresh_case source-revision-64
replace_field "$CASE_REPORT" source_revision 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
revision_64_output="$(verify_fixture "$CASE_REPORT" 2>&1)" || fail "64-hex-source-revision-failed-output=$revision_64_output"
[[ "$revision_64_output" == *"source_revision=1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"* ]] || fail "64-hex-source-revision-not-emitted"

fresh_case malformed-source-revision
replace_field "$CASE_REPORT" source_revision 1234567890abcdef1234567890abcdef123456789
expect_verify_reject malformed-source-revision source_revision-must-be-exact-lowercase-40-or-64-hex-revision "$CASE_REPORT"

fresh_case zero-source-revision
replace_field "$CASE_REPORT" source_revision 0000000000000000000000000000000000000000000000000000000000000000
expect_verify_reject zero-source-revision source_revision-must-not-be-zero "$CASE_REPORT"

fresh_case duplicate-field
printf 'target_name=second-target\n' >> "$CASE_REPORT"
expect_verify_reject duplicate-field report-duplicate-field-target_name "$CASE_REPORT"

fresh_case malformed-field
printf 'this is not a field\n' >> "$CASE_REPORT"
expect_verify_reject malformed-field report-malformed-line "$CASE_REPORT"

fresh_case unexpected-field
printf 'unreviewed_extra=true\n' >> "$CASE_REPORT"
expect_verify_reject unexpected-field report-unexpected-field-unreviewed_extra "$CASE_REPORT"

fresh_case stale
replace_field "$CASE_REPORT" collection_started_at_utc 2000-01-01T00:00:00Z
replace_field "$CASE_REPORT" collection_completed_at_utc 2000-01-01T00:00:01Z
replace_field "$CASE_REPORT" operator_signed_at_utc 2000-01-01T00:00:02Z
replace_field "$CASE_REPORT" reviewer_signed_at_utc 2000-01-01T00:00:03Z
replace_field "$CASE_REPORT" report_created_at_utc 2000-01-01T00:00:04Z
expect_verify_reject stale deployment-evidence-stale "$CASE_REPORT"

fresh_case source-only
replace_field "$CASE_REPORT" collection_scope source-only-render
expect_verify_reject source-only collection_scope-must-equal-target-runtime "$CASE_REPORT"

fresh_case target-not-executed
replace_field "$CASE_REPORT" target_execution not-performed
expect_verify_reject target-not-executed target_execution-must-equal-performed "$CASE_REPORT"

fresh_case skipped-systemd
replace_field "$CASE_REPORT" systemd_verification_result skipped
expect_verify_reject skipped-systemd systemd_verification_result-must-equal-pass "$CASE_REPORT"

fresh_case local-target
replace_field "$CASE_REPORT" target_name localhost
expect_verify_reject local-target target_name-must-not-be-local-or-example "$CASE_REPORT"

fresh_case example-target
replace_field "$CASE_REPORT" target_environment example
expect_verify_reject example-target target_environment-must-not-be-local-or-example "$CASE_REPORT"

fresh_case placeholder
replace_field "$CASE_REPORT" service_permissions_profile TBD
expect_verify_reject placeholder service_permissions_profile-placeholder-value "$CASE_REPORT"

fresh_case mutable-image
replace_field "$CASE_REPORT" image_reference registry.invalid/kvnode:latest
expect_verify_reject mutable-image image_reference-must-use-immutable-sha256-digest "$CASE_REPORT"

fresh_case missing-binary-hash
remove_field "$CASE_REPORT" binary_sha256
expect_verify_reject missing-binary-hash missing-required-field-binary_sha256 "$CASE_REPORT"

fresh_case missing-restart-persistence
remove_field "$CASE_REPORT" boot_persistence_artifact
expect_verify_reject missing-restart-persistence missing-required-field-boot_persistence_artifact "$CASE_REPORT"

fresh_case non-claim
replace_field "$CASE_REPORT" release_claim none-target-environment-deployment-still-required
expect_verify_reject non-claim synthetic-release-claim-invalid "$CASE_REPORT"

fresh_case same-reviewer
replace_field "$CASE_REPORT" reviewer_identity synthetic-operator-a
expect_verify_reject same-reviewer reviewer-must-be-independent-from-operator "$CASE_REPORT"

fresh_case changed-criterion
replace_field "$CASE_REPORT" canary_criterion canary-was-assumed
expect_verify_reject changed-criterion canary_criterion-invalid "$CASE_REPORT"

fresh_case malformed-hash
replace_field "$CASE_REPORT" firewall_sha256 abc123
expect_verify_reject malformed-hash firewall_sha256-must-be-lowercase-sha256 "$CASE_REPORT"

fresh_case tampered-raw-artifact
chmod 0600 "$CASE_DIR/raw/peer_connectivity.artifact"
printf 'tampered=true\n' >> "$CASE_DIR/raw/peer_connectivity.artifact"
expect_verify_reject tampered-raw-artifact peer_connectivity_raw-artifact-sha256-mismatch "$CASE_REPORT"

fresh_case symlinked-raw-artifact
rm "$CASE_DIR/raw/logs.artifact"
ln -s "$SOURCE_DIR/logs.artifact" "$CASE_DIR/raw/logs.artifact"
expect_verify_reject symlinked-raw-artifact logs_raw-artifact-must-be-regular-non-symlink-file "$CASE_REPORT"

fresh_case missing-manifest
rm "$CASE_DIR/raw/deployment_manifest.artifact"
expect_verify_reject missing-manifest deployment_manifest_raw-artifact-must-be-regular-non-symlink-file "$CASE_REPORT"

# Native Darwin v2 is additive: this fixture deliberately uses paths with spaces
# and multiline transcripts so every copy and parse remains quoted and lossless.
darwin_categories=(
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

DARWIN_RELEASE_ID="synthetic-darwin-release-20260710-a-$$"
DARWIN_SOURCE_REVISION="1234567890abcdef1234567890abcdef12345678"
DARWIN_SOURCE_DIR="$TMP_ROOT/darwin source artifacts"
DARWIN_INPUT="$TMP_ROOT/darwin synthetic input.env"
DARWIN_STAGING_IMAGE="$TMP_ROOT/darwin-evidence-staging.sparsebundle"
DARWIN_STAGING_ROOT="$TMP_ROOT/darwin evidence staging"
DARWIN_FINAL_IMAGE="$TMP_ROOT/darwin-evidence-final.dmg"
DARWIN_FINAL_ROOT="/Volumes/mc-kv-evidence-$DARWIN_RELEASE_ID"
mkdir -p "$DARWIN_SOURCE_DIR" "$DARWIN_STAGING_ROOT"
/usr/bin/hdiutil create -size 128m -fs APFS -type SPARSEBUNDLE -volname "mc-kv-evidence-$DARWIN_RELEASE_ID" "$DARWIN_STAGING_IMAGE" >/dev/null
/usr/bin/hdiutil attach -nobrowse -mountpoint "$DARWIN_STAGING_ROOT" "$DARWIN_STAGING_IMAGE" >/dev/null
DARWIN_STAGING_ATTACHED=yes
DARWIN_BUNDLE="$DARWIN_STAGING_ROOT/artifacts/deployment-manifest"

for category in "${darwin_categories[@]}"; do
  {
    printf 'fixture=test-only-synthetic\n'
    printf 'category=%s\n' "$category"
    printf 'observed_command=synthetic command with spaces for %s\n' "$category"
    printf 'observed_stdout=line one\nline two with spaces\n'
    printf 'observed_result=pass\n'
    printf 'captured_at_utc=%s\n' "$now"
  } > "$DARWIN_SOURCE_DIR/${category}.artifact"
done

DARWIN_FIXTURE_SOURCE="$TMP_ROOT/darwin fixture main.go"
printf 'package main\nfunc main() {}\n' > "$DARWIN_FIXTURE_SOURCE"
rm "$DARWIN_SOURCE_DIR/binary.artifact"
env GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -buildvcs=false -trimpath -o "$DARWIN_SOURCE_DIR/binary.artifact" "$DARWIN_FIXTURE_SOURCE"
rm "$DARWIN_FIXTURE_SOURCE"
DARWIN_BINARY_SHA256="$(file_sha256 "$DARWIN_SOURCE_DIR/binary.artifact")"
DARWIN_BINARY_PATH="/var/db/moreconsensus/releases/${DARWIN_RELEASE_ID}-${DARWIN_BINARY_SHA256}/bin/kvnode"

cat > "$DARWIN_INPUT" <<EOF
evidence_schema=kvnode-target-deployment-input-v2
evidence_mode=test-only-synthetic
release_claim=test-only-synthetic-deployment-accepted
release_id=$DARWIN_RELEASE_ID
collection_scope=target-runtime
collection_method=pre-captured-staging-to-read-only-final
target_execution=performed
target_id=mc-kv-darwin24-arm64-launchd-3n-r1
target_environment=native-darwin24-arm64-launchd-system-domain-v1
deployment_profile=native-darwin24-arm64-launchd-system-domain-v1
target_platform=darwin
architecture=arm64
binary_format=mach-o-64
execution_mode=native
orchestrator=launchd
launchd_domain=system
darwin_version=24.6.0
macos_version=15.6
os_build=24G84
kernel_version=Darwin Kernel Version 24.6.0: synthetic test-only evidence
filesystem_type=apfs
staging_root_path=$DARWIN_STAGING_ROOT
staging_root_uri=file:$DARWIN_STAGING_ROOT
staging_root_filesystem=apfs
staging_root_writable=true
final_evidence_root_path=$DARWIN_FINAL_ROOT
final_evidence_root_uri=file:$DARWIN_FINAL_ROOT
final_evidence_root_filesystem=apfs
final_evidence_root_read_only_required=true
final_evidence_root_external=true
final_evidence_image_format=udro
apfs_data_root=/var/db/moreconsensus/synthetic-campaign/data
apfs_checkpoint_root=/var/db/moreconsensus/synthetic-campaign/checkpoint
apfs_quarantine_root=/var/db/moreconsensus/synthetic-campaign/quarantine
apfs_log_root=/var/db/moreconsensus/synthetic-campaign/log
binary_uri=file:$DARWIN_BINARY_PATH
binary_path=$DARWIN_BINARY_PATH
binary_expected_sha256=$DARWIN_BINARY_SHA256
binary_source_revision=$DARWIN_SOURCE_REVISION
binary_immutable=true
source_revision=$DARWIN_SOURCE_REVISION
service_user=kvnode
service_group=kvnode
service_uid=41001
service_gid=41001
service_permissions_profile=dedicated-non-root-least-privilege
host_topology=single-darwin-host
network_scope=loopback-only
tls_scope=server-authentication-only
tls_ca_path=/var/db/moreconsensus/synthetic-campaign/tls/ca.pem
mutual_tls=false
client_authorization=false
nonclaims=same-host,loopback-only,no-independent-failure-domain,server-auth-tls-only,no-client-authorization,no-production-capacity,no-off-host-backup
boot_observation=real-host-reboot
boot_observation_synthetic=false
boot_uuid_before=11111111-1111-4111-8111-111111111111
boot_uuid_after=22222222-2222-4222-8222-222222222222
graceful_signal=SIGTERM
graceful_accepts_stopped=true
graceful_inflight_drained=true
graceful_exit_seconds=7
graceful_durable_canary=pass
rollback_bundle_uri=file:/var/db/moreconsensus/releases/prior-7777777777777777777777777777777777777777777777777777777777777777/bundles/kvnode-darwin-arm64.tar
rollback_bundle_sha256=7777777777777777777777777777777777777777777777777777777777777777
rollback_bundle_immutable=true
rollback_binary_format=mach-o-64
rollback_architecture=arm64
collection_started_at_utc=$now
collection_completed_at_utc=$now
operator_identity=synthetic-darwin-operator-a
operator_signoff=approved
operator_signed_at_utc=$now
reviewer_identity=synthetic-darwin-reviewer-b
reviewer_signoff=approved
reviewer_signed_at_utc=$now
EOF

for node in 1 2 3; do
  case "$node" in
    1)
      label=org.gosuda.moreconsensus.kvnode.1
      pid=5101
      client=127.0.0.1:19090
      peer=127.0.0.1:19091
      admin=127.0.0.1:19092
      plist_hash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      ;;
    2)
      label=org.gosuda.moreconsensus.kvnode.2
      pid=5102
      client=127.0.0.1:19190
      peer=127.0.0.1:19191
      admin=127.0.0.1:19192
      plist_hash=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      ;;
    3)
      label=org.gosuda.moreconsensus.kvnode.3
      pid=5103
      client=127.0.0.1:19290
      peer=127.0.0.1:19291
      admin=127.0.0.1:19292
      plist_hash=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
      ;;
  esac
  tls_cert_path="/var/db/moreconsensus/synthetic-campaign/tls/node${node}.crt"
  tls_key_path="/var/db/moreconsensus/synthetic-campaign/tls/node${node}.key"
  argv_hash="$(
    fixture_program_arguments_sha256 \
      "$node" \
      "$DARWIN_BINARY_PATH" \
      "$client" \
      "$peer" \
      "$admin" \
      "/var/db/moreconsensus/synthetic-campaign/data/node${node}" \
      "$tls_cert_path" \
      "$tls_key_path" \
      /var/db/moreconsensus/synthetic-campaign/tls/ca.pem
  )"
  cat >> "$DARWIN_INPUT" <<EOF
node_${node}_label=$label
node_${node}_pid=$pid
node_${node}_client_listener=$client
node_${node}_peer_listener=$peer
node_${node}_admin_listener=$admin
node_${node}_data_directory=/var/db/moreconsensus/synthetic-campaign/data/node${node}
node_${node}_plist_path=/Library/LaunchDaemons/$label.plist
node_${node}_tls_cert_path=$tls_cert_path
node_${node}_tls_key_path=$tls_key_path
node_${node}_plist_sha256=$plist_hash
node_${node}_plutil_lint_result=pass
node_${node}_launchd_bootstrap_result=pass
node_${node}_launchd_print_result=pass
node_${node}_program_arguments_sha256=$argv_hash
node_${node}_launchctl_program_arguments_sha256=$argv_hash
node_${node}_process_arguments_sha256=$argv_hash
node_${node}_executable_path=$DARWIN_BINARY_PATH
node_${node}_executable_sha256=$DARWIN_BINARY_SHA256
EOF
done

for category in "${darwin_categories[@]}"; do
  {
    printf '%s_result=pass\n' "$category"
    printf '%s_artifact=%s/%s.artifact\n' "$category" "$DARWIN_SOURCE_DIR" "$category"
  } >> "$DARWIN_INPUT"
done
MASQUERADE_INPUT="$TMP_ROOT/darwin writable-root masquerade.env"
cp "$DARWIN_INPUT" "$MASQUERADE_INPUT"
printf 'evidence_root_read_only=true\n' >> "$MASQUERADE_INPUT"
expect_command_reject darwin-writable-root-masquerades-as-final input-unexpected-field-evidence_root_read_only \
  env KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" collect --input "$MASQUERADE_INPUT" --output-dir "$DARWIN_STAGING_ROOT/artifacts/masquerade"


darwin_collect_output="$(KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" collect --input "$DARWIN_INPUT" --output-dir "$DARWIN_BUNDLE" 2>&1)" ||
  fail "darwin-v2-collection-failed-output=$darwin_collect_output"
[[ "$darwin_collect_output" == *"status=staged"*"staging_root_writable=observed-true"*"final_evidence_root_read_only=not-observed"* ]] ||
  fail "darwin-v2-collection-made-false-final-read-only-claim-output=$darwin_collect_output"
[[ -f "$DARWIN_BUNDLE/evidence.env" ]] || fail "darwin-v2-collector-did-not-write-report"
[[ -f "$DARWIN_BUNDLE/raw/source_provenance.artifact" ]] || fail "darwin-v2-collector-lost-source-provenance"
expect_command_reject darwin-writable-staging-cannot-final-verify final-evidence-root-must-be-real-mounted-directory \
  env KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" verify "$DARWIN_BUNDLE/evidence.env"

/usr/bin/hdiutil detach "$DARWIN_STAGING_ROOT" >/dev/null
DARWIN_STAGING_ATTACHED=no
/usr/bin/hdiutil convert "$DARWIN_STAGING_IMAGE" -format UDRO -o "$DARWIN_FINAL_IMAGE" >/dev/null
/usr/bin/hdiutil attach -readonly -nobrowse "$DARWIN_FINAL_IMAGE" >/dev/null
DARWIN_FINAL_ATTACHED=yes
[[ -d "$DARWIN_FINAL_ROOT" ]] || fail "darwin-v2-final-image-did-not-mount-at-release-bound-root"
DARWIN_BUNDLE="$DARWIN_FINAL_ROOT/artifacts/deployment-manifest"
DARWIN_VOLUME_UUID=""
while IFS= read -r disk_line || [[ -n "$disk_line" ]]; do
  case "$disk_line" in
    *"Volume UUID:"*)
      DARWIN_VOLUME_UUID="${disk_line#*:}"
      DARWIN_VOLUME_UUID="$(printf '%s' "$DARWIN_VOLUME_UUID" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
      ;;
  esac
done < <(/usr/sbin/diskutil info "$DARWIN_FINAL_ROOT")
[[ "$DARWIN_VOLUME_UUID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] ||
  fail "darwin-v2-final-volume-UUID-missing"

darwin_expected_stdout="kvnode-target-deployment-evidence status=verified verifier_version=darwin-v2 evidence_mode=test-only-synthetic release_id=$DARWIN_RELEASE_ID target_id=mc-kv-darwin24-arm64-launchd-3n-r1 source_revision=$DARWIN_SOURCE_REVISION binary_sha256=$DARWIN_BINARY_SHA256 final_evidence_root_read_only=observed-true final_evidence_root_path=$DARWIN_FINAL_ROOT evidence_volume_uuid=$DARWIN_VOLUME_UUID limitations=same-host,loopback-only,no-independent-failure-domain,server-auth-tls-only,no-client-authorization,no-production-capacity,no-off-host-backup release_claim=test-only-synthetic-deployment-accepted"
darwin_positive_output="$(KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" verify "$DARWIN_BUNDLE/evidence.env" 2>&1)" ||
  fail "darwin-v2-positive-fixture-failed-output=$darwin_positive_output"
[[ "$darwin_positive_output" == "$darwin_expected_stdout" ]] ||
  fail "darwin-v2-stdout-contract-mismatch-output=$darwin_positive_output"
HEADER_ONLY_INPUT="$TMP_ROOT/darwin header-only input.env"
HEADER_ONLY_BINARY="$TMP_ROOT/darwin header-only binary"
HEADER_ONLY_OUTPUT="$TMP_ROOT/darwin header-only output"
cp "$DARWIN_INPUT" "$HEADER_ONLY_INPUT"
printf '\317\372\355\376\014\000\000\001\000\000\000\000\002\000\000\000\001\000\000\000\010\000\000\000\000\000\000\000\000\000\000\000' > "$HEADER_ONLY_BINARY"
HEADER_ONLY_SHA256="$(file_sha256 "$HEADER_ONLY_BINARY")"
HEADER_ONLY_PATH="/var/db/moreconsensus/releases/${DARWIN_RELEASE_ID}-${HEADER_ONLY_SHA256}/bin/kvnode"
replace_field "$HEADER_ONLY_INPUT" binary_artifact "$HEADER_ONLY_BINARY"
replace_field "$HEADER_ONLY_INPUT" binary_expected_sha256 "$HEADER_ONLY_SHA256"
replace_field "$HEADER_ONLY_INPUT" binary_path "$HEADER_ONLY_PATH"
replace_field "$HEADER_ONLY_INPUT" binary_uri "file:$HEADER_ONLY_PATH"
for node in 1 2 3; do
  case "$node" in
    1) client=127.0.0.1:19090; peer=127.0.0.1:19091; admin=127.0.0.1:19092 ;;
    2) client=127.0.0.1:19190; peer=127.0.0.1:19191; admin=127.0.0.1:19192 ;;
    3) client=127.0.0.1:19290; peer=127.0.0.1:19291; admin=127.0.0.1:19292 ;;
  esac
  tls_cert_path="/var/db/moreconsensus/synthetic-campaign/tls/node${node}.crt"
  tls_key_path="/var/db/moreconsensus/synthetic-campaign/tls/node${node}.key"
  argv_hash="$(
    fixture_program_arguments_sha256 \
      "$node" \
      "$HEADER_ONLY_PATH" \
      "$client" \
      "$peer" \
      "$admin" \
      "/var/db/moreconsensus/synthetic-campaign/data/node${node}" \
      "$tls_cert_path" \
      "$tls_key_path" \
      /var/db/moreconsensus/synthetic-campaign/tls/ca.pem
  )"
  replace_field "$HEADER_ONLY_INPUT" "node_${node}_program_arguments_sha256" "$argv_hash"
  replace_field "$HEADER_ONLY_INPUT" "node_${node}_launchctl_program_arguments_sha256" "$argv_hash"
  replace_field "$HEADER_ONLY_INPUT" "node_${node}_process_arguments_sha256" "$argv_hash"
  replace_field "$HEADER_ONLY_INPUT" "node_${node}_executable_path" "$HEADER_ONLY_PATH"
  replace_field "$HEADER_ONLY_INPUT" "node_${node}_executable_sha256" "$HEADER_ONLY_SHA256"
done
expect_command_reject darwin-header-only-binary binary-artifact-too-small-to-be-native-kvnode \
  env KVNODE_DEPLOYMENT_ALLOW_TEST_FIXTURE=yes "$SCRIPT" collect --input "$HEADER_ONLY_INPUT" --output-dir "$HEADER_ONLY_OUTPUT"


DARWIN_CASE_DIR=""
DARWIN_CASE_REPORT=""
fresh_darwin_case() {
  local label="$1"
  DARWIN_CASE_DIR="$TMP_ROOT/darwin-case-$label"
  mkdir "$DARWIN_CASE_DIR"
  cp -R "$DARWIN_BUNDLE/." "$DARWIN_CASE_DIR"
  DARWIN_CASE_REPORT="$DARWIN_CASE_DIR/evidence.env"
  chmod 0600 "$DARWIN_CASE_REPORT"
}

fresh_darwin_case synthetic-relabel
replace_field "$DARWIN_CASE_REPORT" evidence_mode target
replace_field "$DARWIN_CASE_REPORT" release_claim target-deployment-accepted
expect_verify_reject darwin-synthetic-relabel target-evidence-must-not-contain-synthetic-marker-in-release_id "$DARWIN_CASE_REPORT"

fresh_darwin_case staging-root-not-writable
replace_field "$DARWIN_CASE_REPORT" staging_root_writable false
expect_verify_reject darwin-staging-root-not-writable staging_root_writable-must-equal-true "$DARWIN_CASE_REPORT"

fresh_darwin_case final-root-read-only-not-required
replace_field "$DARWIN_CASE_REPORT" final_evidence_root_read_only_required false
expect_verify_reject darwin-final-root-read-only-not-required final_evidence_root_read_only_required-must-equal-true "$DARWIN_CASE_REPORT"

fresh_darwin_case non-apfs-final-evidence-root
replace_field "$DARWIN_CASE_REPORT" final_evidence_root_filesystem ext4
expect_verify_reject darwin-non-apfs-final-evidence-root final_evidence_root_filesystem-must-equal-apfs "$DARWIN_CASE_REPORT"

fresh_darwin_case internal-final-evidence-root
replace_field "$DARWIN_CASE_REPORT" final_evidence_root_external false
expect_verify_reject darwin-internal-final-evidence-root final_evidence_root_external-must-equal-true "$DARWIN_CASE_REPORT"

fresh_darwin_case staging-reused-as-final
replace_field "$DARWIN_CASE_REPORT" final_evidence_root_path "$DARWIN_STAGING_ROOT"
replace_field "$DARWIN_CASE_REPORT" final_evidence_root_uri "file:$DARWIN_STAGING_ROOT"
expect_verify_reject darwin-staging-reused-as-final staging-root-must-differ-from-final-read-only-root "$DARWIN_CASE_REPORT"

fresh_darwin_case unknown-version
replace_field "$DARWIN_CASE_REPORT" evidence_schema kvnode-target-deployment-evidence-v3
expect_verify_reject darwin-unknown-version unsupported-report-schema-kvnode-target-deployment-evidence-v3 "$DARWIN_CASE_REPORT"

fresh_darwin_case missing-verifier-version
remove_field "$DARWIN_CASE_REPORT" verifier_version
expect_verify_reject darwin-missing-verifier-version missing-required-field-verifier_version "$DARWIN_CASE_REPORT"

fresh_darwin_case linux-masquerade
printf 'linux_distribution=not-darwin\n' >> "$DARWIN_CASE_REPORT"
expect_verify_reject darwin-linux-masquerade report-unexpected-field-linux_distribution "$DARWIN_CASE_REPORT"

fresh_darwin_case image-masquerade
printf 'image_reference=registry.invalid/kvnode@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' >> "$DARWIN_CASE_REPORT"
expect_verify_reject darwin-image-masquerade report-unexpected-field-image_reference "$DARWIN_CASE_REPORT"

fresh_darwin_case launch-agent
replace_field "$DARWIN_CASE_REPORT" launchd_domain user
expect_verify_reject darwin-launch-agent launchd_domain-must-equal-system "$DARWIN_CASE_REPORT"

fresh_darwin_case multi-host
replace_field "$DARWIN_CASE_REPORT" host_topology multi-host
expect_verify_reject darwin-multi-host host_topology-must-equal-single-darwin-host "$DARWIN_CASE_REPORT"

fresh_darwin_case localhost-as-multi-host
replace_field "$DARWIN_CASE_REPORT" network_scope localhost-multi-host
expect_verify_reject darwin-localhost-as-multi-host network_scope-must-equal-loopback-only "$DARWIN_CASE_REPORT"

fresh_darwin_case mtls-claim
replace_field "$DARWIN_CASE_REPORT" mutual_tls true
expect_verify_reject darwin-mtls-claim mutual_tls-must-equal-false "$DARWIN_CASE_REPORT"

fresh_darwin_case client-authorization-claim
replace_field "$DARWIN_CASE_REPORT" client_authorization true
expect_verify_reject darwin-client-authorization-claim client_authorization-must-equal-false "$DARWIN_CASE_REPORT"

fresh_darwin_case missing-nonclaims
remove_field "$DARWIN_CASE_REPORT" nonclaims
expect_verify_reject darwin-missing-nonclaims missing-required-field-nonclaims "$DARWIN_CASE_REPORT"

fresh_darwin_case synthetic-reboot
replace_field "$DARWIN_CASE_REPORT" boot_observation_synthetic true
expect_verify_reject darwin-synthetic-reboot boot_observation_synthetic-must-equal-false "$DARWIN_CASE_REPORT"

fresh_darwin_case unchanged-boot-uuid
replace_field "$DARWIN_CASE_REPORT" boot_uuid_after 11111111-1111-4111-8111-111111111111
expect_verify_reject darwin-unchanged-boot-uuid boot-UUID-must-transition-across-real-host-reboot "$DARWIN_CASE_REPORT"

fresh_darwin_case mutable-binary
replace_field "$DARWIN_CASE_REPORT" binary_immutable false
expect_verify_reject darwin-mutable-binary binary_immutable-must-equal-true "$DARWIN_CASE_REPORT"

fresh_darwin_case mutable-binary-path
replace_field "$DARWIN_CASE_REPORT" binary_path /usr/local/bin/kvnode
expect_verify_reject darwin-mutable-binary-path binary_path-must-be-release-and-content-addressed-immutable-path "$DARWIN_CASE_REPORT"

fresh_darwin_case source-binary-mismatch
replace_field "$DARWIN_CASE_REPORT" binary_source_revision abcdefabcdefabcdefabcdefabcdefabcdefabcdef
expect_verify_reject darwin-source-binary-mismatch binary_source_revision-must-equal "$DARWIN_CASE_REPORT"

fresh_darwin_case malformed-revision
replace_field "$DARWIN_CASE_REPORT" source_revision ABCDEF1234567890abcdef1234567890abcdef12
expect_verify_reject darwin-malformed-revision source_revision-must-be-exact-lowercase-40-or-64-hex-revision "$DARWIN_CASE_REPORT"

fresh_darwin_case zero-revision
replace_field "$DARWIN_CASE_REPORT" source_revision 0000000000000000000000000000000000000000
expect_verify_reject darwin-zero-revision source_revision-must-not-be-zero "$DARWIN_CASE_REPORT"

fresh_darwin_case zero-binary-digest
replace_field "$DARWIN_CASE_REPORT" binary_expected_sha256 0000000000000000000000000000000000000000000000000000000000000000
expect_verify_reject darwin-zero-binary-digest binary_expected_sha256-must-not-be-zero-sha256 "$DARWIN_CASE_REPORT"

fresh_darwin_case malformed-plist-digest
replace_field "$DARWIN_CASE_REPORT" node_1_plist_sha256 abc123
expect_verify_reject darwin-malformed-plist-digest node_1_plist_sha256-must-be-lowercase-sha256 "$DARWIN_CASE_REPORT"

fresh_darwin_case duplicate-node
replace_field "$DARWIN_CASE_REPORT" node_2_pid 5101
expect_verify_reject darwin-duplicate-node node-PIDs-must-be-distinct "$DARWIN_CASE_REPORT"

fresh_darwin_case mismatched-node-label
replace_field "$DARWIN_CASE_REPORT" node_3_label org.gosuda.moreconsensus.kvnode.2
expect_verify_reject darwin-mismatched-node-label node_3_label-must-equal-org.gosuda.moreconsensus.kvnode.3 "$DARWIN_CASE_REPORT"

fresh_darwin_case launch-agent-plist-path
replace_field "$DARWIN_CASE_REPORT" node_1_plist_path /Users/operator/Library/LaunchAgents/org.gosuda.moreconsensus.kvnode.1.plist
expect_verify_reject darwin-launch-agent-plist-path node_1_plist_path-must-equal-/Library/LaunchDaemons/org.gosuda.moreconsensus.kvnode.1.plist "$DARWIN_CASE_REPORT"

fresh_darwin_case mismatched-listener
replace_field "$DARWIN_CASE_REPORT" node_2_peer_listener 127.0.0.1:19091
expect_verify_reject darwin-mismatched-listener node_2_peer_listener-must-equal-127.0.0.1:19191 "$DARWIN_CASE_REPORT"

fresh_darwin_case duplicate-plist
replace_field "$DARWIN_CASE_REPORT" node_2_plist_sha256 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
expect_verify_reject darwin-duplicate-plist node-plist-hashes-must-be-distinct "$DARWIN_CASE_REPORT"

fresh_darwin_case plutil-skipped
replace_field "$DARWIN_CASE_REPORT" node_1_plutil_lint_result skipped
expect_verify_reject darwin-plutil-skipped node_1_plutil_lint_result-must-equal-pass "$DARWIN_CASE_REPORT"

fresh_darwin_case bootstrap-skipped
replace_field "$DARWIN_CASE_REPORT" node_2_launchd_bootstrap_result skipped
expect_verify_reject darwin-bootstrap-skipped node_2_launchd_bootstrap_result-must-equal-pass "$DARWIN_CASE_REPORT"

fresh_darwin_case launchctl-print-skipped
replace_field "$DARWIN_CASE_REPORT" node_3_launchd_print_result skipped
expect_verify_reject darwin-launchctl-print-skipped node_3_launchd_print_result-must-equal-pass "$DARWIN_CASE_REPORT"

fresh_darwin_case incomplete-ProgramArguments
replace_field "$DARWIN_CASE_REPORT" node_1_program_arguments_sha256 4444444444444444444444444444444444444444444444444444444444444444
expect_verify_reject darwin-incomplete-ProgramArguments node_1_program_arguments_sha256-does-not-match-exact-ProgramArguments "$DARWIN_CASE_REPORT"

fresh_darwin_case argv-mismatch
replace_field "$DARWIN_CASE_REPORT" node_1_process_arguments_sha256 4444444444444444444444444444444444444444444444444444444444444444
expect_verify_reject darwin-argv-mismatch node_1_process_arguments_sha256-must-equal "$DARWIN_CASE_REPORT"

fresh_darwin_case executable-path-mismatch
replace_field "$DARWIN_CASE_REPORT" node_2_executable_path /var/db/moreconsensus/releases/other/bin/kvnode
expect_verify_reject darwin-executable-path-mismatch node_2_executable_path-must-equal "$DARWIN_CASE_REPORT"

fresh_darwin_case executable-hash-mismatch
replace_field "$DARWIN_CASE_REPORT" node_3_executable_sha256 4444444444444444444444444444444444444444444444444444444444444444
expect_verify_reject darwin-executable-hash-mismatch node_3_executable_sha256-must-equal "$DARWIN_CASE_REPORT"

fresh_darwin_case graceful-signal
replace_field "$DARWIN_CASE_REPORT" graceful_signal SIGKILL
expect_verify_reject darwin-graceful-signal graceful_signal-must-equal-SIGTERM "$DARWIN_CASE_REPORT"

fresh_darwin_case graceful-drain
replace_field "$DARWIN_CASE_REPORT" graceful_inflight_drained false
expect_verify_reject darwin-graceful-drain graceful_inflight_drained-must-equal-true "$DARWIN_CASE_REPORT"

fresh_darwin_case graceful-deadline
replace_field "$DARWIN_CASE_REPORT" graceful_exit_seconds 31
expect_verify_reject darwin-graceful-deadline graceful_exit_seconds-exceeds-30-second-deadline "$DARWIN_CASE_REPORT"

fresh_darwin_case mutable-rollback
replace_field "$DARWIN_CASE_REPORT" rollback_bundle_immutable false
expect_verify_reject darwin-mutable-rollback rollback_bundle_immutable-must-equal-true "$DARWIN_CASE_REPORT"

fresh_darwin_case mutable-rollback-uri
replace_field "$DARWIN_CASE_REPORT" rollback_bundle_uri file:/var/db/moreconsensus/releases/current/bundles/kvnode-darwin-arm64.tar
expect_verify_reject darwin-mutable-rollback-uri rollback_bundle_uri-must-be-content-addressed-native-immutable-file "$DARWIN_CASE_REPORT"

fresh_darwin_case tampered-binary
chmod 0600 "$DARWIN_CASE_DIR/raw/binary.artifact"
printf 'tampered\n' >> "$DARWIN_CASE_DIR/raw/binary.artifact"
expect_verify_reject darwin-tampered-binary binary_raw-artifact-sha256-mismatch "$DARWIN_CASE_REPORT"

echo "kvnode-target-deployment-evidence-test status=pass fixture=test-only-synthetic release_claim=none-target-deployment-not-performed"
