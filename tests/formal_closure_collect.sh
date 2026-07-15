#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

staging_rel="${FORMAL_CLOSURE_STAGING:-.formal-closure-evidence}"
case "$staging_rel" in
  ""|.|..|/*|../*|*/../*|*/..)
    printf 'ERROR: FORMAL_CLOSURE_STAGING must be a non-root-relative directory below the repository root: %s\n' "$staging_rel" >&2
    exit 2
    ;;
esac

staging="$ROOT/$staging_rel"
rm -rf -- "${staging:?staging directory must not be empty}"
mkdir -p "$staging/logs"

source_revision="$(git rev-parse HEAD)"
records="$staging/component-records.jsonl"
runs="$staging/collection-runs.jsonl"
: > "$records"
: > "$runs"

sha256_file() {
  sha256sum "$1" | cut -d ' ' -f 1
}

record_component() {
  local component="$1"
  local artifact_class="$2"
  local log_rel="$3"
  local exit_code="$4"
  shift 4

  python3 - "$records" "$runs" "$component" "$artifact_class" "$log_rel" "$exit_code" "$source_revision" "$@" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

records_file, runs_file, component, artifact_class, log_rel, exit_code, source_revision, *command = sys.argv[1:]

staging_dir = os.path.dirname(records_file)
full_path = os.path.join(staging_dir, log_rel)

size_bytes = os.path.getsize(full_path)
if size_bytes < 1:
    sys.stderr.write(f"ERROR: artifact log {log_rel} is empty (0 bytes), violating schema minimum size of 1 byte\n")
    sys.exit(1)

sha256_val = __import__("hashlib").sha256(open(full_path, "rb").read()).hexdigest()
created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

# Generate artifact_id using the identifier logic
def identifier(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

artifact_id = identifier(component)

artifact_entry = {
    "artifact_id": artifact_id,
    "kind": artifact_class,
    "path": log_rel,
    "sha256": sha256_val,
    "size_bytes": size_bytes,
    "source_revision": source_revision,
    "created_at": created_at,
}

run_entry = {
    "component": component,
    "kind": artifact_class,
    "path": log_rel,
    "exit_code": int(exit_code),
    "command": command,
    "status": "executed"
}

with open(records_file, "a", encoding="utf-8") as out_rec:
    out_rec.write(json.dumps(artifact_entry, sort_keys=True, separators=(",", ":")) + "\n")

with open(runs_file, "a", encoding="utf-8") as out_runs:
    out_runs.write(json.dumps(run_entry, sort_keys=True, separators=(",", ":")) + "\n")
PY
  write_manifest
}

record_skip() {
  local component="$1"
  local reason="$2"
  python3 - "$runs" "$component" "$reason" <<'PY'
import json
import sys

runs_file, component, reason = sys.argv[1:]
run_entry = {
    "component": component,
    "status": "skipped",
    "reason": reason
}
with open(runs_file, "a", encoding="utf-8") as output:
    output.write(json.dumps(run_entry, sort_keys=True, separators=(",", ":")) + "\n")
PY
  write_manifest
}

write_manifest() {
  python3 - "$records" "$runs" "$staging/collection-manifest.json" "$ROOT" "$source_revision" <<'PY'
import hashlib
import json
import pathlib
import re
import sys

records_path, runs_path, manifest_path, root_text, source_revision = sys.argv[1:]
root = pathlib.Path(root_text)

artifacts = []
records_file_path = pathlib.Path(records_path)
if records_file_path.exists():
    content = records_file_path.read_text(encoding="utf-8").strip()
    if content:
        artifacts = [json.loads(line) for line in content.splitlines()]

collection_runs = []
runs_file_path = pathlib.Path(runs_path)
if runs_file_path.exists():
    content = runs_file_path.read_text(encoding="utf-8").strip()
    if content:
        collection_runs = [json.loads(line) for line in content.splitlines()]

def identifier(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

models = []
model_ids = {}
for path in sorted((root / "tla").glob("*.tla")):
    model_id = identifier(path.stem)
    model_ids[path.stem] = model_id
    models.append({
        "model_id": model_id,
        "path": path.relative_to(root).as_posix(),
        "sha256": digest(path),
        "role": "claim-root-shaped-spec" if path.stem == "EPaxosRawNodeRefinement" else "supporting",
        "source_revision": source_revision,
    })

configs = []
for path in sorted((root / "tla").glob("*.cfg")):
    candidates = [stem for stem in model_ids if path.stem.startswith(stem)]
    if not candidates:
        raise SystemExit(f"ERROR: cannot associate config with a model: {path.relative_to(root)}")
    model_stem = max(candidates, key=len)
    configs.append({
        "config_id": identifier(path.stem),
        "path": path.relative_to(root).as_posix(),
        "sha256": digest(path),
        "model_id": model_ids[model_stem],
        "source_revision": source_revision,
    })

models.sort(key=lambda item: item["model_id"])
configs.sort(key=lambda item: item["config_id"])
specification_manifest = {"models": models, "configs": configs}
specification_manifest["manifest_sha256"] = hashlib.sha256(
    json.dumps(specification_manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
).hexdigest()

manifest = {
    "record_kind": "formal-closure-collection-manifest",
    "record_mode": "local-producer-pre-signature",
    "source_revision": source_revision,
    "specification_manifest": specification_manifest,
    "artifacts": artifacts,
    "collection_runs": collection_runs,
    "release_evidence": False,
    "remaining_requirements": [
        "producer signature from the pinned producer trust root",
        "independent reviewer signature from the pinned reviewer trust root",
        "native-darwin execution attestation bound to the release target",
    ],
}
pathlib.Path(manifest_path).write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
}

last_component_exit_code=0

run_component() {
  local component="$1"
  local artifact_class="$2"
  local script="$3"
  local log_rel="$4"
  local log="$staging/$log_rel"

  if [[ ! -f "$script" ]]; then
    printf 'ERROR: expected component script missing: %s\n' "$script" | tee "$log" >&2
    record_component "$component" "$artifact_class" "$log_rel" 127 bash "$script"
    last_component_exit_code=127
    return 0
  fi

  local rc=0
  if bash "$script" >"$log" 2>&1; then
    rc=0
  else
    rc=$?
  fi

  cat "$log"
  record_component "$component" "$artifact_class" "$log_rel" "$rc" bash "$script"
  last_component_exit_code="$rc"
  return 0
}

status=0
tlapm_bin=""
if [[ -n "${TLAPM_BIN:-}" ]]; then
  if [[ -x "$TLAPM_BIN" ]]; then
    tlapm_bin="$TLAPM_BIN"
  elif command -v "$TLAPM_BIN" >/dev/null 2>&1; then
    tlapm_bin="$(command -v "$TLAPM_BIN")"
  fi
fi
if [[ -z "$tlapm_bin" ]] && command -v tlapm >/dev/null 2>&1; then
  tlapm_bin="$(command -v tlapm)"
fi

if [[ -n "$tlapm_bin" ]]; then
  TLAPM_BIN="$tlapm_bin" run_component "tlaps" "tlaps-raw-log" "tests/tlaps_check.sh" "logs/tlaps.log"
  if [[ "$last_component_exit_code" -ne 0 ]]; then
    status=1
  fi
else
  printf 'SKIPPED: TLAPS is unavailable; no tlaps-raw-log was collected.\n'
  record_skip "tlaps" "TLAPM_BIN is unset or not executable and tlapm is absent from PATH"
fi

run_component "tlc-fast-gate" "tlc-raw-log" "tests/tla_model_check_fast.sh" "logs/tla-model-check-fast.log"
if [[ "$last_component_exit_code" -ne 0 ]]; then
  status=1
fi

run_component "trace-refinement" "trace-replay" "tests/trace_refinement_check.sh" "logs/trace-refinement.log"
if [[ "$last_component_exit_code" -ne 0 ]]; then
  status=1
fi

write_manifest
printf 'Collection manifest: %s\n' "$staging/collection-manifest.json"
printf 'This local producer collection is not release evidence. A signed release bundle still needs producer and independent reviewer signatures plus native-darwin target attestation.\n'

if [[ "$status" -ne 0 ]]; then
  printf 'ERROR: formal closure collection failed closed; see %s\n' "$staging/collection-manifest.json" >&2
  exit 1
fi
