#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

require_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "missing operations-readiness artifact: $file" >&2
    exit 1
  fi
}

require_text() {
  local file="$1"
  local text="$2"
  if ! LC_ALL=C grep -Fq -- "$text" "$file"; then
    echo "missing operations-readiness text in $file: $text" >&2
    exit 1
  fi
}

require_occurrences() {
  local file="$1"
  local text="$2"
  local expected="$3"
  local count
  count="$(LC_ALL=C grep -F -c -- "$text" "$file" || true)"
  count="$(printf '%s' "$count" | tr -d '[:space:]')"
  if [[ "$count" != "$expected" ]]; then
    echo "expected $expected occurrences in $file, found $count: $text" >&2
    exit 1
  fi
}

line_number() {
  local file="$1"
  local text="$2"
  local line
  line="$(LC_ALL=C grep -Fnm1 -- "$text" "$file" | cut -d: -f1 || true)"
  if [[ -z "$line" ]]; then
    echo "missing operations-readiness text in $file: $text" >&2
    exit 1
  fi
  printf '%s\n' "$line"
}

require_text_before() {
  local file="$1"
  local first="$2"
  local second="$3"
  local first_line
  local second_line
  first_line="$(line_number "$file" "$first")"
  second_line="$(line_number "$file" "$second")"
  if (( first_line >= second_line )); then
    echo "expected text in $file before '$second': $first" >&2
    exit 1
  fi
}

require_text_between() {
  local file="$1"
  local start="$2"
  local text="$3"
  local end="$4"
  local start_line
  local end_line
  local candidate_line
  start_line="$(line_number "$file" "$start")"
  end_line="$(line_number "$file" "$end")"
  while IFS=: read -r candidate_line _; do
    if (( candidate_line > start_line && candidate_line < end_line )); then
      return
    fi
  done < <(LC_ALL=C grep -Fn -- "$text" "$file" || true)
  echo "expected text in $file between '$start' and '$end': $text" >&2
  exit 1
}

last_line_number() {
  local file="$1"
  local text="$2"
  local line
  line="$(LC_ALL=C grep -Fn -- "$text" "$file" | tail -n 1 | cut -d: -f1 || true)"
  if [[ -z "$line" ]]; then
    echo "missing operations-readiness text in $file: $text" >&2
    exit 1
  fi
  printf '%s\n' "$line"
}

require_last_text_after() {
  local file="$1"
  local first="$2"
  local second="$3"
  local first_line
  local second_line
  first_line="$(line_number "$file" "$first")"
  second_line="$(last_line_number "$file" "$second")"
  if (( first_line >= second_line )); then
    echo "expected last text in $file after '$first': $second" >&2
    exit 1
  fi
}

require_local_runner_lifecycle_reports() {
  local expected_report_list="reports=checkpoint-report.env,verify-report.env,restore-report.env,repair-report.env"
  local observed_reports=()
  local observed_report_list
  local label

  require_text "$local_runner" "data-lifecycle/*-report.env"
  require_text "$local_runner" 'reportPath := filepath.Join(dir, label+"-report.env")'
  require_text "$local_runner" '"KVNODE_CHECKPOINT_REPORT="+reportPath'
  require_text "$local_runner" '"command=KVNODE_CHECKPOINT_REPORT=" + reportPath'
  require_text "$local_runner" 'if err := requireDataLifecycleReport(reportPath, label); err != nil {'
  require_text "$local_runner" 'func requireDataLifecycleReport(reportPath, operation string) error {'
  require_text "$local_runner" '"status=example-operator-report\n"'
  require_text "$local_runner" '"operation=" + operation + "\n"'
  require_text "$local_runner" '"result=success\n"'
  require_text "$local_runner" 'dataLifecycleNonClaim + "\n"'
  require_text "$local_runner" '"reports=" + strings.Join(reports, ",")'
  require_text "$local_runner" "data_lifecycle=offline-checkpoint-verify-restore-repair"
  require_text "$local_runner" "release_claim=none-target-environment-data-lifecycle-drill-still-required"
  require_occurrences "$local_runner" "reports = append(reports, filepath.Base(report))" 3
  require_occurrences "$local_runner" "reports := []string{filepath.Base(report)}" 1

  for label in checkpoint verify restore repair; do
    require_text "$local_runner" "runDataLifecycleCommand(lifecycleDir, \"$label\""
    observed_reports+=("$label-report.env")
  done

  observed_report_list="reports=$(IFS=,; printf '%s' "${observed_reports[*]}")"
  if [[ "$observed_report_list" != "$expected_report_list" ]]; then
    echo "local runner data-lifecycle report list drifted: $observed_report_list" >&2
    exit 1
  fi
  require_text_before "$local_runner" 'runDataLifecycleCommand(lifecycleDir, "repair"' 'lines := dataLifecycleEvidenceLines(statusLocalGoRunnerOnly, reports)'
  require_text_before "$local_runner" 'if err := requireDataLifecycleReport(reportPath, label); err != nil {' 'return reportPath, nil'
}

require_local_runner_consolidated_data_lifecycle_report() {
  local runner_help_output="$1"
  local bad_report_path
  local bad_runner_report_output
  local text

  require_text "$local_runner" "KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT"
  require_text "$local_runner" "Optional 0600 data-lifecycle report path"
  require_text "$local_runner" "optional data lifecycle report when KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT is set"
  require_text "$local_runner" 'validateOptionalReportPath("KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT", dataLifecycleReport)'
  require_text "$local_runner" 'return fmt.Errorf("%s must name a file", name)'
  require_text "$local_runner" "writeDataLifecycleReport"
  require_text "$local_runner" "status=example-operator-report"
  require_text "$local_runner" "artifact=data-lifecycle-drill"
  require_text "$local_runner" "data_lifecycle=offline-checkpoint-verify-restore-repair"
  require_text "$local_runner" "checkpoint=verified"
  require_text "$local_runner" '"reports=" + strings.Join(reports, ",")'
  require_text "$local_runner" 'dataLifecycleEvidenceLines("status=example-operator-report", reports)'
  require_text "$local_runner" "restore=stopped-node-restored-and-restarted"
  require_text "$local_runner" "repair=stopped-node-repaired-from-verified-checkpoint-and-restarted"
  require_text "$local_runner" "canaries=pre-checkpoint-and-post-restore-visible-on-all-nodes-after-repair"
  require_text "$local_runner" "release_claim=none-target-environment-data-lifecycle-drill-still-required"
  require_text "$local_runner" 'os.WriteFile(reportPath, []byte(content), 0o600)'
  require_text "$local_runner" 'os.Chmod(reportPath, 0o600)'

  for text in \
    "KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT" \
    "Optional 0600 data-lifecycle report path" \
    "optional data lifecycle report when KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT is set"; do
    if [[ "$runner_help_output" != *"$text"* ]]; then
      echo "missing kvnode local Go runner help output: $text" >&2
      exit 1
    fi
  done

  for bad_report_path in . /; do
    if bad_runner_report_output="$(KVNODE_GO_RUNNER_RUN= KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT="$bad_report_path" go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode data 2>&1 >/dev/null)"; then
      echo "kvnode local Go runner must reject KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT=$bad_report_path" >&2
      exit 1
    fi
    if [[ "$bad_runner_report_output" != *"KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT must name a file"* ]]; then
      echo "missing operations-readiness output from $local_runner: KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT must name a file" >&2
      exit 1
    fi
    if [[ "$bad_runner_report_output" == *"Refusing to run without KVNODE_GO_RUNNER_RUN=yes."* ]]; then
      echo "kvnode local Go runner must validate KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT before opt-in refusal" >&2
      exit 1
    fi
  done
}


require_local_runner_deployment_manifest() {
  local runner_help_output="$1"
  local expected_launch_defaults="launch_defaults=request_deadline_ms=5000,peer_deadline_ms=2000,max_client_body_bytes=1048576,max_peer_body_bytes=1048576,max_admin_body_bytes=65536,max_scan_limit=1000"
  local text

  require_text "$local_runner" "deployment"
  require_text "$local_runner" "[--mode all|deployment|incident|capacity|data]"
  require_text "$local_runner" "deployment  Run the static systemd manifest audit"
  require_text "$local_runner" "deployment-manifest-summary.txt"
  require_text "$local_runner" "systemd-manifest-report.env"
  require_text "$local_runner" "systemd-manifest-audit.log"
  require_text "$local_runner" "deployment-manifest-local-launch.env"
  require_text "$local_runner" 'cmd := exec.Command("bash", "tests/kvnode_systemd_manifest_audit.sh")'
  require_text "$local_runner" '"KVNODE_SYSTEMD_MANIFEST_REPORT="+reportPath'
  require_text "$local_runner" "systemd_manifest_audit=passed"
  require_text "$local_runner" "manifest_report=systemd-manifest-report.env"
  require_text "$local_runner" "local_launch_report=deployment-manifest-local-launch.env"
  require_text "$local_runner" "_exec_argv_json="
  require_text "$local_runner" "launch_path=manifest-derived-local-substitution"
  require_text "$local_runner" "$expected_launch_defaults"
  require_text "$local_runner" "local_substitution=temp-binary-temp-data-loopback-listeners"
  require_text "$local_runner" "systemd_exec_contract=rendered-then-substituted"
  require_text "$local_runner" "artifact=systemd-manifest-local-launch"
  require_text "$local_runner" "canary=deployment-manifest-value-visible-on-all-nodes"
  require_text "$local_runner" "non_claim=local-static-render-plus-manifest-derived-loopback-process-check-only"
  require_text "$local_runner" "deployment_manifest_ran="
  require_text "$local_runner" "release_claim=none-target-environment-deployment-manifest-still-required"

  if LC_ALL=C grep -Eiq 'target[-_ ]environment[-_ ]deployment[-_ ](proof|proved|verified|passed|complete|ready)|production[-_ ]deployment[-_ ](proof|proved|verified|passed|complete|ready)|release_claim=target[-_ ]environment[-_ ]deployment|deployment[_-]proof' "$local_runner"; then
    echo "kvnode local Go runner must not imply target-environment deployment proof" >&2
    exit 1
  fi

  for text in \
    "deployment" \
    "deployment-manifest-summary.txt" \
    "systemd-manifest-report.env" \
    "systemd-manifest-audit.log" \
    "release_claim=none-target-environment-deployment-manifest-still-required"; do
    if [[ "$runner_help_output" != *"$text"* ]]; then
      echo "missing kvnode local Go runner help output: $text" >&2
      exit 1
    fi
  done
}

require_local_runner_capacity_labels() {
  require_text "$local_runner" "KVNODE_GO_RUNNER_ENVIRONMENT_LABEL   Single-line environment label. Default: local-loopback."
  require_text "$local_runner" "KVNODE_GO_RUNNER_WORKLOAD_LABEL      Single-line workload label. Default: local-go-runner."
  require_text "$local_runner" 'defaultEnvironmentLabel = "local-loopback"'
  require_text "$local_runner" 'defaultWorkloadLabel    = "local-go-runner"'
  require_text "$local_runner" 'environmentLabel, err := envLabel("KVNODE_GO_RUNNER_ENVIRONMENT_LABEL", defaultEnvironmentLabel)'
  require_text "$local_runner" 'workloadLabel, err := envLabel("KVNODE_GO_RUNNER_WORKLOAD_LABEL", defaultWorkloadLabel)'
  require_text "$local_runner" 'func envLabel(name, def string) (string, error) {'
  require_text "$local_runner" 'return "", fmt.Errorf("%s must not be empty", name)'
  require_text "$local_runner" 'strings.ContainsAny(value, "\r\n") || strings.Contains(value, "=")'
  require_text "$local_runner" 'return "", fmt.Errorf("%s must be a single line without =", name)'
  require_text "$local_runner" 'if len(value) > 128 {'
  require_text "$local_runner" 'return "", fmt.Errorf("%s must be <= 128 characters", name)'
  require_text "$local_runner" 'return os.WriteFile(filepath.Join(runDir, "metadata.env"), []byte(content), 0o644)'
  require_text "$local_runner" 'return os.WriteFile(filepath.Join(runDir, "capacity-summary.txt"), []byte(summary), 0o644)'
  require_text "$local_runner" 'return os.WriteFile(filepath.Join(runDir, "summary.txt"), []byte(strings.Join(lines, "\n")), 0o644)'
  require_occurrences "$local_runner" '"environment_label=" + cfg.environmentLabel' 2
  require_occurrences "$local_runner" '"workload_label=" + cfg.workloadLabel' 2
  require_text "$local_runner" 'lines = append(lines, "environment_label="+cfg.environmentLabel, "workload_label="+cfg.workloadLabel)'
  require_text_before "$local_runner" 'if capacityRan {' 'lines = append(lines, "environment_label="+cfg.environmentLabel, "workload_label="+cfg.workloadLabel)'
}

unit="deploy/systemd/kvnode@.service"
env_example="deploy/systemd/kvnode.env.example"
runbook="docs/operations/kvnode-data-lifecycle-incident-runbook.md"
upgrade="docs/operations/kvnode-upgrade-rollback.md"
capacity="tests/kvnode_capacity_envelope.sh"
local_capacity="tests/kvnode_local_capacity_drill.sh"
manifest="tests/kvnode_systemd_manifest_audit.sh"
mixed_drill="tests/kvnode_mixed_version_drill.sh"
checkpoint_helper="examples/kv/cmd/kvcheckpoint/main.go"
checkpoint_helper_test="examples/kv/cmd/kvcheckpoint/main_test.go"
incident_drill="tests/kvnode_incident_tabletop_drill.sh"
local_runner="tests/kvnode_local_runner.go"

for file in "$unit" "$env_example" "$checkpoint_helper" "$checkpoint_helper_test" "$incident_drill" "$local_capacity" "$local_runner" "$runbook" "$upgrade" "$capacity" "$manifest" "$mixed_drill"; do
  require_file "$file"
done

# systemd deployment template: example-only status, expected sections, kvnode
# launch contract, and sandboxing/hardening markers.
require_text "$unit" "Example/operator material for the EPaxos KV node."
require_text "$unit" "not a verified production deployment manifest"
require_text "$unit" "[Unit]"
require_text "$unit" "[Service]"
require_text "$unit" "[Install]"
require_text "$unit" "ExecStart=/usr/local/bin/kvnode"
require_text "$unit" '-id ${KVNODE_ID}'
require_text "$unit" '-listen ${KVNODE_CLIENT_LISTEN}'
require_text "$unit" '-peer-listen ${KVNODE_PEER_LISTEN}'
require_text "$unit" '-admin-listen ${KVNODE_ADMIN_LISTEN}'
require_text "$unit" '-data ${KVNODE_DATA_DIR}'
require_text "$unit" '-peers ${KVNODE_PEERS}'
require_text "$unit" '-request-deadline-ms ${KVNODE_REQUEST_DEADLINE_MS}'
require_text "$unit" '-peer-deadline-ms ${KVNODE_PEER_DEADLINE_MS}'
require_text "$unit" '-max-client-body-bytes ${KVNODE_MAX_CLIENT_BODY_BYTES}'
require_text "$unit" '-max-peer-body-bytes ${KVNODE_MAX_PEER_BODY_BYTES}'
require_text "$unit" '-max-admin-body-bytes ${KVNODE_MAX_ADMIN_BODY_BYTES}'
require_text "$unit" '-max-scan-limit ${KVNODE_MAX_SCAN_LIMIT}'
require_text "$unit" '$KVNODE_TLS_ARGS'
require_text "$unit" "NoNewPrivileges=true"
require_text "$unit" "PrivateTmp=true"
require_text "$unit" "ProtectSystem=strict"
require_text "$unit" "ProtectHome=true"
require_text "$unit" "ReadWritePaths=/var/lib/kvnode/%i"
require_text "$unit" "ReadOnlyPaths=/etc/kvnode"
require_text "$unit" "CapabilityBoundingSet="
require_text "$unit" "AmbientCapabilities="
require_text "$unit" "PrivateDevices=true"
require_text "$unit" "ProtectClock=true"
require_text "$unit" "ProtectControlGroups=true"
require_text "$unit" "ProtectKernelLogs=true"
require_text "$unit" "ProtectKernelModules=true"
require_text "$unit" "ProtectKernelTunables=true"
require_text "$unit" "RestrictRealtime=true"
require_text "$unit" "RestrictSUIDSGID=true"
require_text "$unit" "LockPersonality=true"
require_text "$unit" "MemoryDenyWriteExecute=true"
require_text "$unit" "SystemCallArchitectures=native"

# Environment example: peer topology, deadlines, request limits, TLS knobs, and
# example-only status must stay visible to operators copying the file.
require_text "$env_example" "Example/operator material for kvnode@.service."
require_text "$env_example" "not a verified production deployment environment"
require_text "$env_example" "KVNODE_PEER_LISTEN="
require_text "$env_example" "KVNODE_PEERS="
require_text "$env_example" "KVNODE_REQUEST_DEADLINE_MS="
require_text "$env_example" "KVNODE_PEER_DEADLINE_MS="
require_text "$env_example" "KVNODE_MAX_CLIENT_BODY_BYTES="
require_text "$env_example" "KVNODE_MAX_PEER_BODY_BYTES="
require_text "$env_example" "KVNODE_MAX_ADMIN_BODY_BYTES="
require_text "$env_example" "KVNODE_MAX_SCAN_LIMIT="
require_text "$env_example" "KVNODE_TLS_ARGS="
require_text "$env_example" "-tls-cert=/etc/kvnode/tls/node1.crt"
require_text "$env_example" "-tls-key=/etc/kvnode/tls/node1.key"
require_text "$env_example" "-tls-ca=/etc/kvnode/tls/ca.crt"
require_text "$env_example" "transport configuration only; it does not add application authz/authn"

# Cross-platform manifest exercise: renders the example EnvironmentFile into the
# ExecStart contract, writes an explicit non-claim report when requested, and
# runs systemd-analyze verify when the host provides it.
require_text "$manifest" "KVNODE_SYSTEMD_MANIFEST_REPORT=/path/report.env"
require_text "$manifest" "write_manifest_report()"
require_text "$manifest" "KVNODE_SYSTEMD_MANIFEST_REPORT must name a file"
require_text "$manifest" "status=example-operator-report"
require_text "$manifest" "artifact=systemd-manifest-audit"
require_text "$manifest" "rendered_exec="
require_text "$manifest" "systemd_analyze=skipped"
require_text "$manifest" "systemd_analyze="
require_text "$manifest" "KVNODE_SYSTEMD_ANALYZE=yes"
require_text "$manifest" "systemd-analyze verify"
require_text "$manifest" "release_claim=none-target-environment-deployment-manifest-still-required"
require_text "$manifest" "chmod 0600"
for bad_report_path in . /; do
  if bad_manifest_report_output="$(KVNODE_SYSTEMD_MANIFEST_REPORT="$bad_report_path" bash "$manifest" 2>&1 >/dev/null)"; then
    echo "systemd manifest audit must reject KVNODE_SYSTEMD_MANIFEST_REPORT=$bad_report_path" >&2
    exit 1
  fi
  if [[ "$bad_manifest_report_output" != *"KVNODE_SYSTEMD_MANIFEST_REPORT must name a file"* ]]; then
    echo "missing operations-readiness output from $manifest: KVNODE_SYSTEMD_MANIFEST_REPORT must name a file" >&2
    exit 1
  fi
done
manifest_report_dir="$(mktemp -d "${TMPDIR:-/tmp}/kvnode-systemd-manifest-audit.XXXXXX")"
trap 'rm -rf "$manifest_report_dir"' EXIT
manifest_report="$manifest_report_dir/report.env"
KVNODE_SYSTEMD_MANIFEST_REPORT="$manifest_report" bash "$manifest" >/dev/null
require_text "$manifest_report" "status=example-operator-report"
require_text "$manifest_report" "artifact=systemd-manifest-audit"
require_text "$manifest_report" "unit="
require_text "$manifest_report" "environment_file="
require_text "$manifest_report" "rendered_exec="
if ! LC_ALL=C grep -Eq '^rendered_exec=/usr/local/bin/kvnode -id 1 -listen' "$manifest_report"; then
  echo "systemd manifest report must include unescaped rendered_exec command prefix" >&2
  exit 1
fi
if LC_ALL=C grep -Fq -- 'rendered_exec=/usr/local/bin/kvnode\ -id' "$manifest_report"; then
  echo "systemd manifest report must not double-escape rendered_exec command prefix" >&2
  exit 1
fi
require_text "$manifest_report" "systemd_analyze=skipped"
require_text "$manifest_report" "release_claim=none-target-environment-deployment-manifest-still-required"
if manifest_report_mode="$(stat -c '%a' "$manifest_report" 2>/dev/null)"; then
  :
else
  manifest_report_mode="$(stat -f '%Lp' "$manifest_report")"
fi
if [[ "$manifest_report_mode" != "600" ]]; then
  echo "systemd manifest report mode must be 0600, got $manifest_report_mode" >&2
  exit 1
fi

# Offline checkpoint helper: example/operator command, verified restore boundary,
# and no live-service backup endpoint claim.
require_text "$checkpoint_helper" "Command kvcheckpoint performs offline checkpoint, verification, restore, and"
require_text "$checkpoint_helper" "Status: offline example/operator helper only"
require_text "$checkpoint_helper" "restoreVerified"
require_text "$checkpoint_helper" "kv.VerifyCheckpoint"
require_text "$checkpoint_helper" "kv.RestoreCheckpoint"
require_text "$checkpoint_helper" "KVNODE_CHECKPOINT_REPORT=/path/report.env writes a success report after a completed operation"
require_text "$checkpoint_helper" "status=example-operator-report"
require_text "$checkpoint_helper" "release_claim=none-target-environment-data-lifecycle-drill-still-required"
require_text "$checkpoint_helper_test" "TestRestoreRejectsCorruptCheckpointWithoutReplacingLiveData"
require_text "$checkpoint_helper_test" "TestRepairRejectsCorruptCheckpointWithoutReplacingLiveData"

require_text "$checkpoint_helper_test" "TestRunWritesOperationReportsForSuccessfulCommands"
require_text "$checkpoint_helper_test" "TestRunReportsBadReportPathOnlyAfterSuccessfulOperation"
# Local incident tabletop harness: opt-in, loopback-only, report-capable after
# fault clearing, and explicitly not a target-environment/operator-review claim.
require_text "$incident_drill" "kvnode incident tabletop drill (local loopback harness only)"
require_text "$incident_drill" "KVNODE_INCIDENT_TABLETOP_RUN=yes"
require_text "$incident_drill" "KVNODE_INCIDENT_TABLETOP_REPORT"
require_text "$incident_drill" "writes a 0600 example/operator report"
require_text "$incident_drill" "Refusing to run without KVNODE_INCIDENT_TABLETOP_RUN=yes."
require_text "$incident_drill" "require_command chmod"
require_text "$incident_drill" "status=local-tabletop-only"
require_text "$incident_drill" "storage_fault=exercised-and-cleared"
require_text "$incident_drill" "transport_fault=exercised-and-cleared"
require_text "$incident_drill" "canaries=baseline-and-after-clear-visible-on-all-nodes"
require_text "$incident_drill" "non_claim=not_target_environment_not_operator_reviewed"
require_text "$incident_drill" "release_claim=none-target-environment-operator-review-still-required"
require_text "$incident_drill" "write_report()"
require_text "$incident_drill" '[[ "$report_path" != "." && "$report_path" != "/" ]] || fail "KVNODE_INCIDENT_TABLETOP_REPORT-must-name-a-file"'
require_text "$incident_drill" "status=example-operator-report"
require_text "$incident_drill" "artifact=incident-tabletop-drill"
require_text "$incident_drill" "operator_review=not-performed"
require_text "$incident_drill" "chmod 0600"
require_last_text_after "$incident_drill" "put_value 3 tabletop-after-clear after-clear-value" "write_report"
bash -n "$incident_drill"

incident_report_probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/kvnode-incident-report-audit.XXXXXX")"
trap 'rm -rf "$manifest_report_dir" "$incident_report_probe_dir"' EXIT
incident_report_probe="$incident_report_probe_dir/write-report-probe.sh"
{
  cat <<'EOF'
set -euo pipefail
fail() {
  echo "kvnode-incident-tabletop status=fail reason=$*" >&2
  exit 1
}
: "${EVIDENCE_DIR:?}"
EOF
  LC_ALL=C sed -n '/^write_report() {/,/^}/p' "$incident_drill"
  printf '%s\n' 'write_report'
} > "$incident_report_probe"

for bad_report_path in . /; do
  if bad_incident_report_output="$(EVIDENCE_DIR="$incident_report_probe_dir/evidence" KVNODE_INCIDENT_TABLETOP_REPORT="$bad_report_path" bash "$incident_report_probe" 2>&1 >/dev/null)"; then
    echo "incident tabletop report writer must reject KVNODE_INCIDENT_TABLETOP_REPORT=$bad_report_path" >&2
    exit 1
  fi
  if [[ "$bad_incident_report_output" != *"KVNODE_INCIDENT_TABLETOP_REPORT-must-name-a-file"* ]]; then
    echo "missing operations-readiness output from $incident_drill: KVNODE_INCIDENT_TABLETOP_REPORT-must-name-a-file" >&2
    exit 1
  fi
done

incident_report="$incident_report_probe_dir/report.env"
EVIDENCE_DIR="$incident_report_probe_dir/evidence" KVNODE_INCIDENT_TABLETOP_REPORT="$incident_report" bash "$incident_report_probe" >/dev/null
require_text "$incident_report" "status=example-operator-report"
require_text "$incident_report" "artifact=incident-tabletop-drill"
require_text "$incident_report" "evidence_dir="
require_text "$incident_report" "storage_fault=exercised-and-cleared"
require_text "$incident_report" "transport_fault=exercised-and-cleared"
require_text "$incident_report" "canaries=baseline-and-after-clear-visible-on-all-nodes"
require_text "$incident_report" "operator_review=not-performed"
require_text "$incident_report" "release_claim=none-target-environment-operator-review-still-required"
if incident_report_mode="$(stat -c '%a' "$incident_report" 2>/dev/null)"; then
  :
else
  incident_report_mode="$(stat -f '%Lp' "$incident_report")"
fi
if [[ "$incident_report_mode" != "600" ]]; then
  echo "incident tabletop report mode must be 0600, got $incident_report_mode" >&2
  exit 1
fi

# Local Go runner: build-tagged, opt-in, loopback-only evidence that exercises
# deployment manifest evidence plus admin fault/readiness/metrics endpoints.
require_text "$local_runner" "kvnode local Go runner (opt-in, local loopback only)"
require_text "$local_runner" "KVNODE_GO_RUNNER_RUN=yes"
if ! runner_help_output="$(go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --help 2>&1)"; then
  echo "$runner_help_output" >&2
  exit 1
fi
if runner_refusal_output="$(KVNODE_GO_RUNNER_RUN= go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode incident 2>&1 >/dev/null)"; then
  echo "kvnode local Go runner must refuse without KVNODE_GO_RUNNER_RUN=yes" >&2
  exit 1
fi
if [[ "$runner_refusal_output" != *"Refusing to run without KVNODE_GO_RUNNER_RUN=yes."* ]]; then
  echo "missing operations-readiness output from $local_runner: Refusing to run without KVNODE_GO_RUNNER_RUN=yes." >&2
  exit 1
fi
if bad_environment_label_output="$(KVNODE_GO_RUNNER_RUN=yes KVNODE_GO_RUNNER_ENVIRONMENT_LABEL=bad=label go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode capacity 2>&1)"; then
  echo "kvnode local Go runner must reject KVNODE_GO_RUNNER_ENVIRONMENT_LABEL containing =" >&2
  exit 1
fi
if [[ "$bad_environment_label_output" != *"KVNODE_GO_RUNNER_ENVIRONMENT_LABEL must be a single line without ="* ]]; then
  echo "missing operations-readiness output from $local_runner: KVNODE_GO_RUNNER_ENVIRONMENT_LABEL must be a single line without =" >&2
  exit 1
fi
if bad_workload_label_output="$(KVNODE_GO_RUNNER_RUN=yes KVNODE_GO_RUNNER_WORKLOAD_LABEL=bad=label go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode capacity 2>&1)"; then
  echo "kvnode local Go runner must reject KVNODE_GO_RUNNER_WORKLOAD_LABEL containing =" >&2
  exit 1
fi
if [[ "$bad_workload_label_output" != *"KVNODE_GO_RUNNER_WORKLOAD_LABEL must be a single line without ="* ]]; then
  echo "missing operations-readiness output from $local_runner: KVNODE_GO_RUNNER_WORKLOAD_LABEL must be a single line without =" >&2
  exit 1
fi
require_text "$local_runner" "status=local-go-runner-only"
require_local_runner_deployment_manifest "$runner_help_output"
require_text "$local_runner" "none-target-environment-capacity-results-still-required"
require_local_runner_capacity_labels
require_text "$local_runner" "none-target-environment-operator-review-still-required"
require_text "$local_runner" "[--mode all|deployment|incident|capacity|data]"
require_text "$local_runner" 'data        Stop one local node, checkpoint/verify/restore/repair its data offline, emit helper reports, restart it, and verify catch-up.'
require_text "$local_runner" "data-lifecycle-summary.txt"
require_text "$local_runner" "buildKVCheckpoint"
require_text "$local_runner" "./cmd/kvcheckpoint"
require_text "$local_runner" 'runDataLifecycleCommand(lifecycleDir, "checkpoint"'
require_text "$local_runner" 'runDataLifecycleCommand(lifecycleDir, "verify"'
require_text "$local_runner" 'runDataLifecycleCommand(lifecycleDir, "restore"'
require_text "$local_runner" 'runDataLifecycleCommand(lifecycleDir, "repair"'
require_local_runner_lifecycle_reports
require_local_runner_consolidated_data_lifecycle_report "$runner_help_output"
require_text "$local_runner" "data_lifecycle=offline-checkpoint-verify-restore-repair"
require_text "$local_runner" "restore=stopped-node-restored-and-restarted"
require_text "$local_runner" "repair=stopped-node-repaired-from-verified-checkpoint-and-restarted"
require_text "$local_runner" "canaries=pre-checkpoint-and-post-restore-visible-on-all-nodes-after-repair"
require_text "$local_runner" "release_claim=none-target-environment-data-lifecycle-drill-still-required"
require_text "$local_runner" "go build"
require_text "$local_runner" "/faults/storage"
require_text "$local_runner" "/faults/transport"
require_text "$local_runner" "/readyz"
require_text "$local_runner" "/metrics"

# Data lifecycle/incident runbook: backup/verify/repair/restore boundaries,
# confirmations, evidence capture, and named incident response procedures.
require_text "$runbook" "Status: example/operator runbook material"
require_text "$runbook" "does not make a release, production-ready, or go-live claim"
require_text "$runbook" "## Non-claims and hard boundaries"
require_text "$runbook" "Automatic in-place Pebble/WAL repair is not claimed"
require_text "$runbook" "Automatic EPaxos checksum repair is not claimed"
require_text "$runbook" "kv.VerifyCheckpoint"
require_text "$runbook" "kv.RepairFromCheckpoint"
require_text "$runbook" "kv.RestoreCheckpoint"
require_text "$runbook" "semantic checkpoint verification"
require_text "$runbook" "kv.Cluster.RecoverReplicaFromLiveCheckpoint"
require_text "$runbook" "target-owned next-instance floor"
require_text "$runbook" "## Evidence capture baseline"
require_text "$runbook" "## One-time offline checkpoint/restore helper"
require_text "$runbook" "examples/kv/cmd/kvcheckpoint"
require_text "$runbook" "KVNODE_CHECKPOINT_REPORT=/path/report.env"
require_text "$runbook" "status=example-operator-report"
require_text "$runbook" "quoted data/checkpoint paths"
require_text "$runbook" "each successful helper operation write a small report"
require_text "$runbook" "verified restore"
require_text "$runbook" "does not expose an unverified raw byte-copy restore path"
require_text "$runbook" "choose exactly one recovery path"
require_text "$runbook" "set -euo pipefail"
require_text "$runbook" "checkpoint-epaxos-verify.txt"
require_text "$runbook" "repair-helper.txt"
require_text "$runbook" "## Pebble checkpoint backup"
require_text "$runbook" "## Offline whole-directory repair or restore from a checkpoint"
require_text "$runbook" "## Checksum-mismatch response"
require_text "$runbook" "External host destructive-storage and wall-clock-skew runs are outside the current simulation-scoped release evidence"
require_text "$runbook" "External host destructive-storage runs are outside current release evidence"
require_text "$runbook" "Local drill command"
require_text "$runbook" "JEPSEN_LOCAL_FAULTS=destructive-storage bash tests/jepsen_local.sh"
require_text "$runbook" "Environment variables used for the local loopback run"
require_text "$runbook" "Do not point destructive-storage drills at a shared application directory"
require_text "$runbook" "A destructive-storage pass proves remove/restore of the original directory under the test harness"
require_text "$runbook" "## Incident response: storage failure"
require_text "$runbook" "## Incident response: network partition"
require_text "$runbook" "## Incident response: peer compromise"
require_text "$runbook" "## Incident response: replay or checksum suspicion"
require_text "$runbook" "## Incident response: recovery stalls"
require_text "$runbook" "## Local incident tabletop drill"
require_text "$runbook" "tests/kvnode_incident_tabletop_drill.sh"
require_text "$runbook" "status=local-tabletop-only"
require_text "$runbook" "go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode data"
require_text "$runbook" "data-lifecycle-summary.txt"
require_text "$runbook" 'stops node 2 before opening its Pebble directory, runs `kvcheckpoint checkpoint`, `kvcheckpoint verify`, `kvcheckpoint restore`, restarts node 2'
require_text "$runbook" "release_claim=none-target-environment-data-lifecycle-drill-still-required"
require_text "$runbook" "does not replace a reviewed target-environment backup/restore/disaster-recovery drill"
require_text "$runbook" "operator review"
require_text "$runbook" "No one performed automatic in-place repair, checksum recomputation, corrupt-record deletion, or synthesized reconstruction without a verified checkpoint under this runbook."

# Rolling upgrade/rollback plan: one-node-at-a-time upgrade, checkpoint before
# binary replacement, rollback criteria/procedure, and post-checks.
require_text "$upgrade" "Status: operator plan only."
require_text "$upgrade" "does not assert production readiness"
require_text "$upgrade" "one-node-at-a-time replacement"
require_text "$upgrade" "Upgrade exactly one node at a time."
require_text "$upgrade" "## Per-node rolling replacement"
require_text "$upgrade" "**Checkpoint before upgrade.**"
require_text "$upgrade" "Treat a failed checkpoint as a hard stop; do not upgrade that node."
require_text "$upgrade" "**Post-check the selected node.**"
require_text "$upgrade" "**Post-check the cluster.**"
require_text "$upgrade" "## Rollback criteria"
require_text "$upgrade" "Rollback is mandatory when any of these conditions occur"
require_text "$upgrade" "## Rollback procedure"
require_text "$upgrade" "Rollback one node at a time, using the latest node that was changed first."
require_text "$upgrade" "Start the old binary on the node's current data directory"
require_text "$upgrade" "checkpoint restore can discard committed entries"
require_text "$upgrade" "Run the same node and cluster post-checks used after upgrade"
require_text "$upgrade" "this plan by itself is not production evidence"

# Mixed-version drill harness: maintained local-loopback artifact only. Syntax is
# audited here; execution requires explicit old/new refs and remains opt-in.
require_text "$mixed_drill" "kvnode mixed-version upgrade/rollback drill"
require_text "$mixed_drill" "KVNODE_UPGRADE_OLD_REF"
require_text "$mixed_drill" "build_source=git_archive_trimpath"
require_text "$mixed_drill" "Binary rollback in this drill restarts the old binary on the node's current data"
require_text "$mixed_drill" "checkpoint restore is a separate data-lifecycle fallback"
bash -n "$mixed_drill"

# Capacity-envelope harness: opt-in execution, bounded inputs, output evidence
# files, and no standalone production-evidence claim.
require_text "$capacity" "kvnode capacity-envelope harness (opt-in, bounded)"
require_text "$capacity" "KVNODE_CAPACITY_RUN=yes"
require_text "$capacity" "Refusing to run without KVNODE_CAPACITY_RUN=yes."
require_text "$capacity" "Default: 30, max: 1000"
require_text "$capacity" "Default: 64,1024,4096"
require_text "$capacity" "Default: 1,16,128"
require_text "$capacity" "KVNODE_CAPACITY_ENVIRONMENT_LABEL"
require_text "$capacity" "Single-line environment label. Default: unspecified"
require_text "$capacity" "KVNODE_CAPACITY_WORKLOAD_LABEL"
require_text "$capacity" "Single-line workload label. Default: unspecified"
require_text "$capacity" "KVNODE_CAPACITY_REPORT"
require_text "$capacity" "Optional success report path"
require_text "$capacity" "report.env                     Optional 0600 report with throughput and latency summary fields."
require_text "$capacity" "writes a 0600 example/operator report with throughput and latency summary fields."
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_OPS_PER_PHASE "$ops_per_phase" 1000'
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_TIMEOUT_SECONDS "$timeout_seconds" 300'
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_MAX_VALUE_BYTES "$max_value_bytes" 1048576'
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_MAX_SCAN_LIMIT "$max_scan_limit" 100000'
require_text "$capacity" "label_value()"
require_text "$capacity" 'environment_label="$(label_value KVNODE_CAPACITY_ENVIRONMENT_LABEL "${KVNODE_CAPACITY_ENVIRONMENT_LABEL:-unspecified}")"'
require_text "$capacity" 'workload_label="$(label_value KVNODE_CAPACITY_WORKLOAD_LABEL "${KVNODE_CAPACITY_WORKLOAD_LABEL:-unspecified}")"'
require_text "$capacity" '$name must not be empty'
require_text "$capacity" '$name must be a single line without ='
require_text "$capacity" '$name must be <= 128 characters'
require_text "$capacity" "validate_report_path()"
require_text "$capacity" 'capacity_report="${KVNODE_CAPACITY_REPORT:-}"'
require_text "$capacity" 'validate_report_path KVNODE_CAPACITY_REPORT "$capacity_report"'
require_text "$capacity" '[[ "$value" == "." || "$value" == "/" ]]'
require_text "$capacity" '$name must name a file'
require_text_before "$capacity" 'validate_report_path KVNODE_CAPACITY_REPORT "$capacity_report"' "record_resources before"
require_text "$capacity" "metadata.env                    Harness inputs and peer-count label."
require_text "$capacity" "latency.csv                     operation,http_status,seconds rows."
require_text "$capacity" "resources.csv                   before/after RSS, disk, queue-depth samples."
require_text "$capacity" "summary.md                      Machine-generated sample summary with no readiness claim."
require_text "$capacity" "not production capacity evidence"
require_text "$capacity" "status=harness-only"
require_text "$capacity" "release_claim=none-target-environment-capacity-results-still-required"
require_text "$capacity" 'environment_label=$environment_label'
require_text "$capacity" 'workload_label=$workload_label'
require_text "$capacity" "latency_summary="
require_text "$capacity" "Throughput sample:"
require_text "$capacity" '- Environment label: $environment_label'
require_text "$capacity" '- Workload label: $workload_label'
require_text "$capacity" "Memory RSS samples:"
require_text "$capacity" "Disk growth samples:"
require_text "$capacity" "Queue-depth samples:"
require_text "$capacity" "Peer-count coverage:"
require_text "$capacity" "write_report()"
require_text "$capacity" "status=example-operator-report"
require_text "$capacity" "artifact=capacity-envelope-sample"
require_text "$capacity" "throughput_ops_per_second="
require_text "$capacity" "operation_count="
require_text "$capacity" "latency_samples="
require_text "$capacity" "latency_avg_seconds="
require_text "$capacity" "latency_p50_seconds="
require_text "$capacity" "latency_p95_seconds="
require_text "$capacity" "latency_p99_seconds="
require_text_between "$capacity" "write_report()" 'printf '\''%s\n'\'' "$latency_summary"' '} > "$report_path"'
require_text "$capacity" "latency_file=latency.csv"
require_text "$capacity" "resources_file=resources.csv"
require_text "$capacity" "chmod 0600"
bash -n "$capacity"
for bad_report_path in . /; do
  if bad_capacity_report_output="$(KVNODE_CAPACITY_RUN=yes KVNODE_CAPACITY_REPORT="$bad_report_path" bash "$capacity" 2>&1 >/dev/null)"; then
    echo "capacity envelope audit must reject KVNODE_CAPACITY_REPORT=$bad_report_path" >&2
    exit 1
  fi
  if [[ "$bad_capacity_report_output" != *"KVNODE_CAPACITY_REPORT must name a file"* ]]; then
    echo "missing operations-readiness output from $capacity: KVNODE_CAPACITY_REPORT must name a file" >&2
    exit 1
  fi
done

# Local capacity wrapper: starts a disposable loopback cluster and delegates to
# the bounded capacity harness without making a target-environment claim.
require_text "$local_capacity" "kvnode local capacity loopback drill (opt-in, bounded)"
require_text "$local_capacity" "KVNODE_LOCAL_CAPACITY_RUN=yes"
require_text "$local_capacity" "Refusing to run without KVNODE_LOCAL_CAPACITY_RUN=yes."
require_text "$local_capacity" "KVNODE_CAPACITY_RUN=yes"
require_text "$local_capacity" "KVNODE_CAPACITY_ENVIRONMENT_LABEL      Default: local-loopback"
require_text "$local_capacity" "KVNODE_CAPACITY_WORKLOAD_LABEL         Default: local-capacity-drill"
require_text "$local_capacity" "Default: <out-dir>/capacity/capacity-report.env"
require_text "$local_capacity" 'ENVIRONMENT_LABEL="$(label_value KVNODE_CAPACITY_ENVIRONMENT_LABEL "${KVNODE_CAPACITY_ENVIRONMENT_LABEL:-local-loopback}")"'
require_text "$local_capacity" 'WORKLOAD_LABEL="$(label_value KVNODE_CAPACITY_WORKLOAD_LABEL "${KVNODE_CAPACITY_WORKLOAD_LABEL:-local-capacity-drill}")"'
require_text "$local_capacity" 'CAPACITY_REPORT="${KVNODE_CAPACITY_REPORT:-$CAPACITY_DIR/capacity-report.env}"'
require_text "$local_capacity" 'CAPACITY_DIR="$RUN_DIR/capacity"'
require_text_before "$local_capacity" 'CAPACITY_DIR="$RUN_DIR/capacity"' 'CAPACITY_REPORT="${KVNODE_CAPACITY_REPORT:-$CAPACITY_DIR/capacity-report.env}"'
require_text "$local_capacity" 'KVNODE_CAPACITY_ENVIRONMENT_LABEL="$ENVIRONMENT_LABEL" \'
require_text "$local_capacity" 'KVNODE_CAPACITY_WORKLOAD_LABEL="$WORKLOAD_LABEL" \'
require_text "$local_capacity" 'KVNODE_CAPACITY_REPORT="$CAPACITY_REPORT" \'
require_text_before "$local_capacity" 'CAPACITY_REPORT="${KVNODE_CAPACITY_REPORT:-$CAPACITY_DIR/capacity-report.env}"' 'KVNODE_CAPACITY_REPORT="$CAPACITY_REPORT" \'
require_text "$local_capacity" 'environment_label=$ENVIRONMENT_LABEL'
require_text "$local_capacity" 'workload_label=$WORKLOAD_LABEL'
require_text "$local_capacity" "KVNODE_PEER_COUNT=3"
require_text "$local_capacity" 'capacity_report=$CAPACITY_REPORT'
require_text_before "$local_capacity" 'KVNODE_CAPACITY_REPORT="$CAPACITY_REPORT" \' 'capacity_report=$CAPACITY_REPORT'
require_text "$local_capacity" "status=local-loopback-only"
require_text "$local_capacity" "not_target_environment_capacity_evidence"
require_text "$local_capacity" "release_claim=none-target-environment-capacity-results-still-required"
bash -n "$local_capacity"
