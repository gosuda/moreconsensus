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


mixed="tests/kvnode_mixed_version_drill.sh"
runner="tests/kvnode_local_runner.go"
checkpoint="examples/kv/cmd/kvcheckpoint/main.go"
checkpoint_test="examples/kv/cmd/kvcheckpoint/main_test.go"

require_file "$mixed"
bash_syntax "$mixed"
for go_file in "$runner" "$checkpoint" "$checkpoint_test"; do
  require_file "$go_file"
done

require_text "$mixed" "kvnode mixed-version upgrade/rollback drill (local loopback harness only)"
require_text "$mixed" "KVNODE_UPGRADE_OLD_REF"
require_text "$mixed" "checkpoint restore is a separate data-lifecycle fallback"
require_text "$mixed" "does not assert production readiness"

# The local Go runner now launches directly from explicit arguments and exposes
# only the remaining local drills.
require_text "$runner" "[--mode all|data]"
require_text "$runner" "local loopback evidence only"
require_text "$runner" "KVNODE_GO_RUNNER_RUN=yes"

# The checkpoint helper is the remaining lifecycle entry point.
require_text "$checkpoint" "checkpoint DATA_DIR CHECKPOINT_DIR"
require_text "$checkpoint" "verify CHECKPOINT_DIR"
require_text "$checkpoint" "restore DATA_DIR CHECKPOINT_DIR"
require_text "$checkpoint" "repair DATA_DIR CHECKPOINT_DIR"
require_text "$checkpoint" "offline example/operator helper only"

runner_help="$(go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --help 2>&1)" ||
  fail "local-runner-help-failed"
[[ "$runner_help" == *"[--mode all|data]"* ]] ||
  fail "local-runner-help-missing-data-mode"
if invalid_mode_output="$(go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode invalid 2>&1)"; then
  fail "local-runner-accepted-invalid-mode"
fi
[[ "$invalid_mode_output" == *"bad --mode"* ]] || fail "local-runner-invalid-mode-error-missing"

if runner_opt_in_output="$(go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode data 2>&1)"; then
  fail "local-runner-ran-without-explicit-opt-in"
fi
[[ "$runner_opt_in_output" == *"Refusing to run without KVNODE_GO_RUNNER_RUN=yes."* ]] ||
  fail "local-runner-opt-in-refusal-missing"

report_probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/kvnode-runner-report-audit.XXXXXX")"
trap 'rm -rf "$report_probe_dir"' EXIT
if invalid_report_output="$(KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT="$report_probe_dir" go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode data 2>&1)"; then
  fail "local-runner-accepted-directory-report-path"
fi
[[ "$invalid_report_output" == *"KVNODE_GO_RUNNER_DATA_LIFECYCLE_REPORT must name a file"* ]] ||
  fail "local-runner-report-path-validation-missing"

for command in \
  "go run ./examples/kv/cmd/kvcheckpoint --help" \
  "bash tests/kvnode_mixed_version_drill.sh --help"; do
  bash -c "$command" >/dev/null || fail "artifact-smoke-failed-$command"
done

echo "operations-readiness-audit status=pass artifacts=lifecycle,rollback"
