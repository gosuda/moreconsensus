#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

bash tests/toolchain_audit.sh
bash tests/visualizer_build.sh
bash tests/go_coverage.sh
bash tests/tla_model_check_fast.sh
bash tests/trace_refinement_check.sh
bash tests/fuzz_stress_campaign.sh
bash tests/chaos_fault_campaign.sh
bash tests/jepsen_remote_preflight_audit.sh
bash tests/operations_readiness_audit.sh
bash tests/go_no_go_workflow.sh
bash tests/release_scope_audit.sh
bash tests/audit_repo.sh
