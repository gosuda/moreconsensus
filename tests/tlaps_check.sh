#!/usr/bin/env bash
set -euo pipefail

# TLAPM release: 1.6.0-pre; install prefix: /dev/shm/tlaps
# Asset: tlapm-1.6.0-pre-x86_64-linux-gnu.tar.gz
# SHA-256: 28db02bafd7c899befb696a66812e19a6d2704688f78668cc127cbe4951de8d2

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -n "${TLAPM_BIN:-}" ]]; then
  tlapm="$TLAPM_BIN"
elif tlapm="$(command -v tlapm)"; then
  :
else
  echo "error: tlapm not found; set TLAPM_BIN or add tlapm to PATH" >&2
  exit 127
fi

if [[ ! -x "$tlapm" ]]; then
  echo "error: tlapm is not executable: $tlapm" >&2
  exit 126
fi

module="tla/EPaxosInductiveProofs.tla"
cache_dir="${TLAPM_CACHE_DIR:-${TMPDIR:-/tmp}/moreconsensus-tlapm}"
mkdir -p "$cache_dir"
log="$(mktemp "${TMPDIR:-/tmp}/tlaps-check.XXXXXX")"
summary="$(mktemp "${TMPDIR:-/tmp}/tlaps-summary.XXXXXX")"
trap 'rm -f "$log" "$summary"' EXIT

set +e
"$tlapm" --cleanfp --strict --stretch 3 --threads 8 \
  --cache-dir "$cache_dir" "$module" 2>&1 | tee "$log"
tlapm_status=${PIPESTATUS[0]}
set -e

anchors=(
  InitEstablishesInvariant
  NormalProposePreserves
  BeginRecoveryPreserves
  CollectEvidencePreserves
  RecoveryProposePreserves
  QuorumIntersection
  RecoverySelectionPreservesChosenValue
)

completed=false
if grep -Eq '(All [1-9][0-9]* obligations? proved|[1-9][0-9]*/[1-9][0-9]* obligations failed)' "$log"; then
  completed=true
fi
anchor_failure=false
for anchor in "${anchors[@]}"; do
  start="$(grep -En "^(LEMMA|THEOREM)[[:space:]]+${anchor}[[:space:]]*==" "$module" | cut -d: -f1)"
  if [[ -z "$start" ]]; then
    status=missing
  else
    end="$(awk -v start="$start" 'NR > start && /^(LEMMA|THEOREM)[[:space:]]/ { print NR - 1; exit }' "$module")"
    [[ -n "$end" ]] || end="$(wc -l < "$module")"
    status=proved
    while IFS= read -r failed_line; do
      if (( failed_line >= start && failed_line <= end )); then
        status=failed
        break
      fi
    done < <(grep -Eo 'File "[^\"]+", line [0-9]+' "$log" | awk '{print $NF}')
    if [[ "$completed" != true && "$status" == proved ]]; then
      status=not-confirmed
    fi
  fi
  printf 'anchor_lemma=%s status=%s\n' "$anchor" "$status" | tee -a "$summary"
  [[ "$status" == proved ]] || anchor_failure=true
done

for anchor in "${anchors[@]}"; do
  if ! grep -Eq "^anchor_lemma=${anchor} status=proved$" "$summary"; then
    echo "error: tlapm summary does not prove anchor lemma: $anchor" >&2
    anchor_failure=true
  fi
done

if (( tlapm_status != 0 )); then
  exit "$tlapm_status"
fi
if [[ "$anchor_failure" == true ]]; then
  exit 1
fi
if ! grep -Eq 'All [1-9][0-9]* obligations? proved' "$log"; then
  echo "error: tlapm exited successfully without a non-empty proved-obligation summary" >&2
  exit 1
fi
if grep -Eqi '([1-9][0-9]*/[1-9][0-9]* obligations failed|proof incomplete|omitted proof)' "$log"; then
  echo "error: tlapm output reports failed or omitted obligations" >&2
  exit 1
fi

