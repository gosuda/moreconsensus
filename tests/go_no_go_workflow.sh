#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

scope="RELEASE_SCOPE.md"
evidence="release/EPAXOS_READINESS_EVIDENCE.md"

if [[ ! -f "$scope" ]]; then
  echo "missing release scope lock: $scope" >&2
  exit 1
fi
if [[ ! -f "$evidence" ]]; then
  echo "missing evidence bundle: $evidence" >&2
  exit 1
fi

decision="$(awk '
  /^## Current release decision$/ { in_section = 1; next }
  in_section && NF { print; exit }
' "$scope")"

open_count="$(awk '
  /^## Open release items$/ { in_open = 1; next }
  in_open && /^## / { in_open = 0 }
  in_open && /^\| / && $0 !~ /^\| Item \|/ && $0 !~ /^\| --- \|/ { count++ }
  END { print count + 0 }
' "$scope")"

if (( open_count > 0 )); then
  if [[ "$decision" != "No-go." ]]; then
    echo "release decision must be No-go. while open release items remain; got: $decision" >&2
    exit 1
  fi
else
  if [[ "$decision" != "Go." ]]; then
    echo "release decision must be Go. only after open release items are empty; got: $decision" >&2
    exit 1
  fi
fi

for required in \
  "Status: no-go evidence bundle" \
  "Current open blockers preserving no-go" \
  "bash tests/go_no_go_workflow.sh"; do
  if ! LC_ALL=C grep -Fq -- "$required" "$evidence"; then
    echo "evidence bundle missing required text: $required" >&2
    exit 1
  fi
done

printf 'release_decision=%s\n' "$decision"
printf 'open_release_items=%s\n' "$open_count"
if (( open_count > 0 )); then
  echo "open_release_item_rows:"
  awk '
    /^## Open release items$/ { in_open = 1; next }
    in_open && /^## / { in_open = 0 }
    in_open && /^\| / && $0 !~ /^\| Item \|/ && $0 !~ /^\| --- \|/ { print $0 }
  ' "$scope"
fi
