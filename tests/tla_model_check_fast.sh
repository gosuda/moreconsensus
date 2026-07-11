#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 tests/tla_model_check_runner.py \
  --profile fast \
  --jobs 2 \
  --per-config-timeout 60 \
  --overall-timeout 300
