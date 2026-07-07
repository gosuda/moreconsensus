#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

bash tests/go_coverage.sh
bash tests/tla_model_check.sh
bash tests/jepsen_local.sh
bash tests/audit_repo.sh
