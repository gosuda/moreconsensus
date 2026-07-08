#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

bash tests/go_coverage.sh
bash tests/tla_model_check.sh
(cd jepsen && lein test moreconsensus.epaxos-test-test)
bash tests/jepsen_local.sh
env JEPSEN_LOCAL_FAULTS=transport bash tests/jepsen_local.sh
env JEPSEN_LOCAL_FAULTS=storage bash tests/jepsen_local.sh
env JEPSEN_LOCAL_FAULTS=destructive-storage bash tests/jepsen_local.sh
bash tests/audit_repo.sh
