#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

patterns=(
  "Tiger""beetle"
  "tiger""beetle"
  "place""holder"
  "skele""ton"
  "st""ub"
  "TO""DO"
  "time""out"
  "Time""out"
  "time"".New"
  "time"".Sleep"
  "time"".After"
)

for pattern in "${patterns[@]}"; do
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
