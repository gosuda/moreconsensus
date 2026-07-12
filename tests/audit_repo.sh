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

python3 - <<'PY'
from __future__ import annotations

import subprocess
import unicodedata
from pathlib import Path

root = Path.cwd()
paths = subprocess.run(
    ["git", "ls-files", "-z"],
    cwd=root,
    check=True,
    stdout=subprocess.PIPE,
).stdout.split(b"\0")
for raw in paths:
    if not raw:
        continue
    path = root / raw.decode("utf-8")
    data = path.read_bytes()
    if b"\0" in data:
        continue
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        continue
    if any("HANGUL" in unicodedata.name(char, "") for char in text):
        raise SystemExit(f"Hangul text is present in tracked file: {path}")
PY
