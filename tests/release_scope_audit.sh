#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

scope="$ROOT/RELEASE_SCOPE.md"
evidence="$ROOT/release/EPAXOS_READINESS_EVIDENCE.md"

require_path() {
  local path="$1"
  if [[ ! -e "$ROOT/$path" ]]; then
    echo "required release artifact is missing: $path" >&2
    exit 1
  fi
}

require_text() {
  local file="$1"
  local text="$2"
  if ! LC_ALL=C grep -Fq -- "$text" "$file"; then
    echo "required text is missing from ${file#$ROOT/}: $text" >&2
    exit 1
  fi
}

for path in \
  README.md \
  EPAXOS.MD \
  EPAXOS_IMPLEMENTATION_PROOF.md \
  MODEL_EQ_REPORT.MD \
  RELEASE_SCOPE.md \
  release/EPAXOS_READINESS_EVIDENCE.md \
  tests/audit_repo.sh \
  tests/ci.sh \
  tests/go_no_go_workflow.sh \
  tests/release_evidence_verifier.py \
  tests/tla_model_check.sh \
  tests/tla_model_check_fast.sh \
  tests/tla_model_check_runner.py \
  tests/operations_readiness_audit.sh \
  deploy/systemd/kvnode@.service \
  deploy/systemd/kvnode.env.example \
  docs/operations/kvnode-data-lifecycle-incident-runbook.md \
  docs/operations/kvnode-upgrade-rollback.md; do
  require_path "$path"
done

require_text "$scope" "## Current release decision"
require_text "$scope" "No-go."
require_text "$scope" "## Closed release items"
require_text "$scope" "## Verification prerequisites"
require_text "$scope" "| Gate | Status |"
require_text "$scope" "## Open release items"
require_text "$scope" "## Non-claims"
require_text "$scope" "unbounded proof"
require_text "$scope" "certified protocol-state compaction"
require_text "$scope" "multi-host independent failure domains"
require_text "$scope" "real-network fault evidence"
require_text "$scope" "signed operator-controlled deployment/capacity/lifecycle/incident evidence"

require_text "$evidence" "Status: no-go evidence bundle"
require_text "$evidence" "Current open blockers preserving no-go"
require_text "$scope" "| Aggregate Go coverage | fail |"
require_text "$evidence" "| Aggregate Go coverage | Fail:"
require_text "$evidence" "bash tests/go_no_go_workflow.sh"
require_text "$evidence" "bash tests/tla_model_check_fast.sh"
require_text "$evidence" "go test ./... -count=1"

python3 - "$ROOT" "$scope" "$evidence" <<'PY'
from __future__ import annotations

import subprocess
import sys
import unicodedata
from pathlib import Path

root = Path(sys.argv[1])
scope = Path(sys.argv[2])
evidence = Path(sys.argv[3])

labels = {
    "Broader formal model coverage",
    "Deployment manifest",
    "Data lifecycle",
    "Capacity envelope",
    "Incident readiness",
}


def section(path: Path, heading: str) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    starts = [i for i, line in enumerate(lines) if line == heading]
    if len(starts) != 1:
        raise SystemExit(f"{path.name} must contain exactly one {heading!r} heading")
    start = starts[0] + 1
    end = len(lines)
    for i in range(start, len(lines)):
        if lines[i].startswith("## "):
            end = i
            break
    return lines[start:end]


def open_rows(path: Path) -> set[str]:
    lines = section(path, "## Open release items")
    try:
        header = lines.index("| Item | Current state |")
    except ValueError as exc:
        raise SystemExit("open release items must use the canonical table header") from exc
    if header + 1 >= len(lines) or lines[header + 1] != "| --- | --- |":
        raise SystemExit("open release items table separator is malformed")
    rows: list[str] = []
    for line in lines[header + 2 :]:
        if not line.strip():
            continue
        if not line.startswith("| "):
            if rows:
                break
            raise SystemExit(f"unexpected open release item content: {line}")
        fields = line.split("|")
        if len(fields) != 4 or not fields[1].strip() or not fields[2].strip():
            raise SystemExit(f"malformed open release item row: {line}")
        rows.append(fields[1].strip())
    actual = set(rows)
    if actual != labels or len(rows) != len(labels):
        raise SystemExit(f"canonical open release rows mismatch: {rows!r}")
    return actual


open_rows(scope)

decision_lines = [line.strip() for line in section(scope, "## Current release decision") if line.strip()]

prerequisite_lines = section(scope, "## Verification prerequisites")
try:
    prerequisite_header = prerequisite_lines.index("| Gate | Status |")
except ValueError as exc:
    raise SystemExit("verification prerequisites must use the canonical table header") from exc
if prerequisite_header + 1 >= len(prerequisite_lines) or prerequisite_lines[prerequisite_header + 1] != "| --- | --- |":
    raise SystemExit("verification prerequisites table separator is malformed")
coverage_rows = [
    line for line in prerequisite_lines[prerequisite_header + 2 :]
    if line.startswith("| Aggregate Go coverage |")
]
if len(coverage_rows) != 1 or coverage_rows[0] not in (
    "| Aggregate Go coverage | pass |",
    "| Aggregate Go coverage | fail |",
):
    raise SystemExit("verification prerequisites must declare Aggregate Go coverage as pass or fail")
if not decision_lines or decision_lines[0] != "No-go.":
    raise SystemExit("current release decision must be exactly No-go.")

for path in (scope, evidence):
    text = path.read_text(encoding="utf-8")
    for marker in (
        "after adding",
        "after updating",
        "in this pass",
        "in this session",
        "New or updated artifacts",
        "Current verification evidence",
        "artifact://",
        "open_release_items=11",
    ):
        if marker.casefold() in text.casefold():
            raise SystemExit(f"stale chronology marker in {path.relative_to(root)}: {marker}")

# Model/config references in authority documents must resolve to repository files.
# The executable model scripts remain the source of coverage inventory; this only
# prevents prose from pointing at an artifact that does not exist.
for path in (scope, evidence, root / "MODEL_EQ_REPORT.MD", root / "EPAXOS_IMPLEMENTATION_PROOF.md"):
    text = path.read_text(encoding="utf-8")
    for token in text.replace("`", " ").split():
        token = token.rstrip(",.;:)\"]")
        if token.startswith("tla/") and token.endswith((".tla", ".cfg")):
            if not (root / token).is_file():
                raise SystemExit(f"document cites missing model artifact: {token}")

tracked = subprocess.run(
    ["git", "ls-files", "-z"],
    cwd=root,
    check=True,
    stdout=subprocess.PIPE,
).stdout.split(b"\0")
for raw in tracked:
    if not raw:
        continue
    path = root / raw.decode("utf-8")
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise SystemExit(f"cannot read tracked path {path}: {exc}") from exc
    if b"\0" in data:
        continue
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        continue
    if any("HANGUL" in unicodedata.name(char, "") for char in text):
        raise SystemExit(f"Hangul text is present in tracked file: {path.relative_to(root)}")

PY

echo "release scope audit passed"
