#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

repository_patterns=(
  "Tiger""beetle"
  "tiger""beetle"
)

for pattern in "${repository_patterns[@]}"; do
  if LC_ALL=C grep -R -n -I \
    --exclude-dir=.git \
    --exclude-dir=jepsen/target \
    --exclude-dir=jepsen/store \
    --exclude-dir=states \
    --exclude='*.out' \
    --exclude='.lein-failures' \
    -- "$pattern" .; then
    echo "disallowed repository text matched" >&2
    exit 1
  fi
done

production_patterns=(
  "place""holder"
  "skele""ton"
  "st""ub"
  "TO""DO"
)

for pattern in "${production_patterns[@]}"; do
  if LC_ALL=C grep -R -n -I \
    --include='*.go' \
    --exclude='*_test.go' \
    -- "$pattern" epaxos examples; then
    echo "disallowed production implementation text matched" >&2
    exit 1
  fi
done

determinism_patterns=(
  "time"".New"
  "time"".Sleep"
  "time"".After"
)

for pattern in "${determinism_patterns[@]}"; do
  if LC_ALL=C grep -R -n -I \
    --include='*.go' \
    --exclude='*_test.go' \
    -- "$pattern" epaxos; then
    echo "wall-clock API matched in deterministic protocol core" >&2
    exit 1
  fi
done
