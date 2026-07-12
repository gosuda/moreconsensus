#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

fail() {
  echo "operations-readiness-audit status=fail reason=$*" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing-file-$path"
}

require_text() {
  local path="$1"
  local text="$2"
  LC_ALL=C grep -Fq -- "$text" "$path" || fail "missing-text-${path}-${text}"
}


bash_syntax() {
  local path="$1"
  bash -n "$path" || fail "invalid-shell-syntax-$path"
}


capacity="tests/kvnode_capacity_envelope.sh"
local_capacity="tests/kvnode_local_capacity_drill.sh"
incident="tests/kvnode_incident_tabletop_drill.sh"
mixed="tests/kvnode_mixed_version_drill.sh"
runner="tests/kvnode_local_runner.go"
checkpoint="examples/kv/cmd/kvcheckpoint/main.go"
checkpoint_test="examples/kv/cmd/kvcheckpoint/main_test.go"

for shell_file in "$capacity" "$local_capacity" "$incident" "$mixed"; do
  require_file "$shell_file"
  bash_syntax "$shell_file"
done
for go_file in "$runner" "$checkpoint" "$checkpoint_test"; do
  require_file "$go_file"
done

# Capacity evidence remains bounded and explicitly non-claim.
require_text "$capacity" "kvnode capacity-envelope harness (opt-in, bounded)"
require_text "$capacity" "KVNODE_CAPACITY_RUN=yes"
require_text "$capacity" "KVNODE_CAPACITY_REPORT"
require_text "$capacity" "target_environment=not-measured"
require_text "$capacity" "release_claim=none-target-environment-capacity-results-still-required"
require_text "$capacity" "chmod 0600"
require_text "$local_capacity" "kvnode local capacity loopback drill (opt-in, bounded)"
require_text "$local_capacity" "KVNODE_LOCAL_CAPACITY_RUN=yes"
require_text "$local_capacity" "KVNODE_CAPACITY_REPORT"
require_text "$local_capacity" "release_claim=none-target-environment-capacity-results-still-required"

# Incident and rollback artifacts must preserve their local-only boundaries.
require_text "$incident" "kvnode incident tabletop drill (local loopback harness only)"
require_text "$incident" "KVNODE_INCIDENT_TABLETOP_RUN=yes"
require_text "$incident" "/faults/storage"
require_text "$incident" "/faults/transport"
require_text "$incident" "release_claim=none-target-environment-operator-review-still-required"
require_text "$mixed" "kvnode mixed-version upgrade/rollback drill (local loopback harness only)"
require_text "$mixed" "KVNODE_UPGRADE_OLD_REF"
require_text "$mixed" "checkpoint restore is a separate data-lifecycle fallback"
require_text "$mixed" "does not assert production readiness"

# The local Go runner now launches directly from explicit arguments and exposes
# only the remaining local drills.
require_text "$runner" "[--mode all|incident|capacity|data]"
require_text "$runner" "local loopback evidence only"
require_text "$runner" "-enable-fault-injection=true"

# The checkpoint helper is the remaining lifecycle entry point.
require_text "$checkpoint" "checkpoint DATA_DIR CHECKPOINT_DIR"
require_text "$checkpoint" "verify CHECKPOINT_DIR"
require_text "$checkpoint" "restore DATA_DIR CHECKPOINT_DIR"
require_text "$checkpoint" "repair DATA_DIR CHECKPOINT_DIR"
require_text "$checkpoint" "offline example/operator helper only"

runner_help="$(go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --help 2>&1)" ||
  fail "local-runner-help-failed"
[[ "$runner_help" == *"[--mode all|incident|capacity|data]"* ]] ||
  fail "local-runner-help-retains-obsolete-mode"
if invalid_mode_output="$(go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode invalid 2>&1)"; then
  fail "local-runner-accepted-invalid-mode"
fi
[[ "$invalid_mode_output" == *"bad --mode"* ]] || fail "local-runner-invalid-mode-error-missing"

if runner_opt_in_output="$(go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode incident 2>&1)"; then
  fail "local-runner-ran-without-explicit-opt-in"
fi
[[ "$runner_opt_in_output" == *"Refusing to run without KVNODE_GO_RUNNER_RUN=yes."* ]] ||
  fail "local-runner-opt-in-refusal-missing"

if invalid_environment_output="$(KVNODE_GO_RUNNER_ENVIRONMENT_LABEL='bad=value' go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode incident 2>&1)"; then
  fail "local-runner-accepted-invalid-environment-label"
fi
[[ "$invalid_environment_output" == *"KVNODE_GO_RUNNER_ENVIRONMENT_LABEL must be a single line without ="* ]] ||
  fail "local-runner-environment-label-validation-missing"

if invalid_workload_output="$(KVNODE_GO_RUNNER_WORKLOAD_LABEL='bad=value' go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode incident 2>&1)"; then
  fail "local-runner-accepted-invalid-workload-label"
fi
[[ "$invalid_workload_output" == *"KVNODE_GO_RUNNER_WORKLOAD_LABEL must be a single line without ="* ]] ||
  fail "local-runner-workload-label-validation-missing"

report_probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/kvnode-runner-report-audit.XXXXXX")"
trap 'rm -rf "$report_probe_dir"' EXIT
if invalid_report_output="$(KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT="$report_probe_dir" go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode data 2>&1)"; then
  fail "local-runner-accepted-directory-report-path"
fi
[[ "$invalid_report_output" == *"KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT must name a file"* ]] ||
  fail "local-runner-report-path-validation-missing"

for command in \
  "go run ./examples/kv/cmd/kvcheckpoint --help" \
  "bash tests/kvnode_capacity_envelope.sh --dry-run" \
  "bash tests/kvnode_local_capacity_drill.sh --help" \
  "bash tests/kvnode_incident_tabletop_drill.sh --help" \
  "bash tests/kvnode_mixed_version_drill.sh --help"; do
  bash -c "$command" >/dev/null || fail "artifact-smoke-failed-$command"
done

echo "operations-readiness-audit status=pass artifacts=capacity,incident,lifecycle,rollback"
