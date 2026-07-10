#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
VERIFY="$ROOT/tests/target_data_lifecycle_evidence_verify.py"
SCHEMA="$ROOT/tests/target_data_lifecycle_evidence.schema.json"
PYTHON="${PYTHON:-python3}"

fail() {
  echo "target-data-lifecycle-evidence-selftest status=fail reason=$*" >&2
  exit 1
}

for command in "$PYTHON" cp dirname grep rm mkdir mktemp; do
  command -v "$command" >/dev/null 2>&1 || fail "missing-required-command-$command"
done
[[ -x "$VERIFY" ]] || fail "verifier-not-executable"
[[ -f "$SCHEMA" ]] || fail "schema-missing"

TMP_PARENT="${TMPDIR:-/tmp}"
TMP_PARENT="${TMP_PARENT%/}"
WORK="$(mktemp -d "$TMP_PARENT/target-data-lifecycle-evidence-selftest.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

make_fixture() {
  local directory="$1"
  local age_days="${2:-0}"
  mkdir -p "$directory/raw"
  "$PYTHON" - "$directory" "$age_days" <<'PY'
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

out = Path(sys.argv[1])
age_days = int(sys.argv[2])
base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=2, days=age_days)

def stamp(seconds):
    return (base + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

artifact_ids = [
    "release-provenance",
    "pre-health",
    "pre-quorum",
    "pre-readiness",
    "pre-metrics",
    "checkpoint-semantic",
    "retention-receipt",
    "disaster-scenario",
    "recovery-transcript",
    "rollback-decision",
    "restart",
    "rejoin",
    "catch-up",
    "canaries",
    "integrity",
    "objectives",
    "post-logs",
    "post-readiness",
    "post-metrics",
    "operator-signoff",
    "reviewer-signoff",
]
raw_artifacts = []
for artifact_id in artifact_ids:
    relative = f"raw/{artifact_id}.txt"
    payload = (
        "Synthetic self-test raw artifact only.\n"
        "release_claim=none\n"
        f"artifact_id={artifact_id}\n"
    ).encode("utf-8")
    (out / relative).write_bytes(payload)
    raw_artifacts.append({
        "id": artifact_id,
        "path": relative,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "captured_at": stamp(460),
    })

report = {
    "schema_version": "1.0.0",
    "evidence_class": "synthetic-test-only",
    "release_claim": "none",
    "synthetic_test_fixture": True,
    "generated_at": stamp(460),
    "valid_until": stamp(460 + 7 * 24 * 60 * 60),
    "target": {
        "name": "dr-west-a",
        "environment": "production-west",
        "cluster_id": "cluster-west-17",
        "release_id": "v2.4.0",
        "source_revision": "1" * 40,
        "binary_sha256": "2" * 64,
        "provenance_artifact_id": "release-provenance",
    },
    "drill": {
        "drill_id": "drill-20260710-a",
        "started_at": stamp(0),
        "completed_at": stamp(430),
    },
    "pre_drill": {
        "checked_at": stamp(60),
        "health_result": "pass",
        "quorum_result": "pass",
        "expected_voters": 3,
        "healthy_voters": 3,
        "readiness_result": "pass",
        "metrics_result": "pass",
        "health_artifact_id": "pre-health",
        "quorum_artifact_id": "pre-quorum",
        "readiness_artifact_id": "pre-readiness",
        "metrics_artifact_id": "pre-metrics",
    },
    "checkpoint": {
        "checkpoint_id": "checkpoint-west-17-0042",
        "created_at": stamp(180),
        "source_node": "node-west-2",
        "manifest_sha256": "3" * 64,
        "state_digest_sha256": "4" * 64,
        "record_count": 1200,
        "semantic_verification_result": "pass",
        "semantic_artifact_id": "checkpoint-semantic",
        "retention": {
            "location": "s3://dr-west-retention/releases/v2.4.0/checkpoint-0042",
            "immutable_object_version": "version-9f2c",
            "retention_until": stamp(460 + 90 * 24 * 60 * 60),
            "result": "pass",
            "artifact_id": "retention-receipt",
        },
    },
    "disaster": {
        "scenario": "single-node durable-volume loss",
        "scope": "one voting member in west failure domain",
        "occurred_at": stamp(240),
        "affected_node": "node-west-2",
        "result": "pass",
        "artifact_id": "disaster-scenario",
    },
    "recovery": {
        "node_stopped_at": stamp(250),
        "recovery_started_at": stamp(260),
        "operation": "restore",
        "operation_result": "pass",
        "stopped_node": "node-west-2",
        "stopped_node_confirmed": True,
        "transcript_artifact_id": "recovery-transcript",
        "rollback_or_quarantine": {
            "action": "quarantine",
            "at": stamp(270),
            "result": "pass",
            "artifact_id": "rollback-decision",
        },
        "restart": {
            "at": stamp(300),
            "result": "pass",
            "artifact_id": "restart",
        },
        "service_restored_at": stamp(360),
        "rejoin": {
            "at": stamp(370),
            "result": "pass",
            "artifact_id": "rejoin",
        },
        "catch_up": {
            "at": stamp(380),
            "result": "pass",
            "lag_entries": 0,
            "artifact_id": "catch-up",
        },
    },
    "canaries": {
        "pre_checked_at": stamp(120),
        "post_checked_at": stamp(400),
        "result": "pass",
        "values": [
            {
                "key": "orders/canary-17",
                "pre_value": "accepted-000017",
                "post_value": "accepted-000017",
                "result": "pass",
            },
            {
                "key": "accounts/canary-04",
                "pre_value": "balance-004200",
                "post_value": "balance-004200",
                "result": "pass",
            },
        ],
        "artifact_id": "canaries",
    },
    "integrity": {
        "checked_at": stamp(410),
        "result": "pass",
        "expected_record_count": 1200,
        "observed_record_count": 1200,
        "expected_application_count": 2400,
        "observed_application_count": 2400,
        "duplicate_applications": 0,
        "data_loss_records": 0,
        "artifact_id": "integrity",
    },
    "objectives": {
        "rpo": {
            "recovery_point_at": stamp(180),
            "measured_seconds": 60,
            "threshold_seconds": 120,
            "result": "met",
        },
        "rto": {
            "started_at": stamp(240),
            "restored_at": stamp(360),
            "measured_seconds": 120,
            "threshold_seconds": 180,
            "result": "met",
        },
        "artifact_id": "objectives",
    },
    "observability": {
        "checked_at": stamp(420),
        "logs_result": "pass",
        "logs_artifact_id": "post-logs",
        "readiness_result": "pass",
        "readiness_artifact_id": "post-readiness",
        "metrics_result": "pass",
        "metrics_artifact_id": "post-metrics",
    },
    "sign_off": {
        "operator": {
            "name": "Casey Morgan",
            "role": "Recovery Operator",
            "signed_at": stamp(440),
            "result": "approved",
            "artifact_id": "operator-signoff",
        },
        "independent_reviewer": {
            "name": "Riley Chen",
            "role": "Independent Reliability Reviewer",
            "signed_at": stamp(450),
            "result": "approved",
            "artifact_id": "reviewer-signoff",
        },
    },
    "raw_artifacts": raw_artifacts,
}
(out / "evidence.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
PY
}

cat > "$WORK/mutate.py" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

path = Path(sys.argv[1])
command = sys.argv[2]

def descend(document, dotted):
    parts = dotted.split(".")
    current = document
    for part in parts[:-1]:
        current = current[int(part)] if isinstance(current, list) else current[part]
    return current, parts[-1]

if command == "malformed":
    path.write_text("{\n", encoding="utf-8")
    raise SystemExit(0)
if command == "duplicate-top-key":
    text = path.read_text(encoding="utf-8")
    needle = '  "schema_version": "1.0.0",\n'
    if text.count(needle) != 1:
        raise SystemExit("schema_version line not unique")
    path.write_text(text.replace(needle, needle + needle, 1), encoding="utf-8")
    raise SystemExit(0)

document = json.loads(path.read_text(encoding="utf-8"))
if command == "refresh-generation":
    now = datetime.now(timezone.utc).replace(microsecond=0)
    document["generated_at"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    document["valid_until"] = (now + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(0)
parent, leaf = descend(document, sys.argv[3])
if command == "set":
    value = json.loads(sys.argv[4])
    if isinstance(parent, list):
        parent[int(leaf)] = value
    else:
        parent[leaf] = value
elif command == "delete":
    if isinstance(parent, list):
        del parent[int(leaf)]
    else:
        del parent[leaf]
else:
    raise SystemExit(f"unknown mutation: {command}")
path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
PY

PRISTINE="$WORK/pristine"
make_fixture "$PRISTINE"

# The schema and complete fixture are both JSON, and the fixture only passes in
# the explicit synthetic non-claim mode.
"$PYTHON" - "$SCHEMA" "$PRISTINE/evidence.json" <<'PY'
import json
import sys
for name in sys.argv[1:]:
    with open(name, "r", encoding="utf-8") as handle:
        json.load(handle)
PY
"$PYTHON" "$VERIFY" --self-test-fixture "$PRISTINE/evidence.json" >"$WORK/complete.out" 2>&1 || {
  cat "$WORK/complete.out" >&2
  fail "complete-synthetic-fixture-rejected"
}
if ! grep -Fq 'mode=synthetic-test-fixture release_claim=none' "$WORK/complete.out"; then
  fail "synthetic-success-output-is-not-an-explicit-non-claim"
fi

pass_count=1
expect_fixture_failure() {
  local name="$1"
  local case_dir="$WORK/case-$name"
  shift
  cp -R "$PRISTINE" "$case_dir"
  "$@" "$case_dir/evidence.json"
  if "$PYTHON" "$VERIFY" --self-test-fixture "$case_dir/evidence.json" >"$case_dir/result.out" 2>&1; then
    cat "$case_dir/result.out" >&2
    fail "$name-was-accepted"
  fi
  pass_count=$((pass_count + 1))
}

expect_default_failure() {
  local name="$1"
  local case_dir="$WORK/case-$name"
  shift
  cp -R "$PRISTINE" "$case_dir"
  "$@" "$case_dir/evidence.json"
  if "$PYTHON" "$VERIFY" "$case_dir/evidence.json" >"$case_dir/result.out" 2>&1; then
    cat "$case_dir/result.out" >&2
    fail "$name-was-accepted"
  fi
  pass_count=$((pass_count + 1))
}

no_change() { :; }
mutate_set() {
  local dotted="$1"
  local json_value="$2"
  local report="$3"
  "$PYTHON" "$WORK/mutate.py" "$report" set "$dotted" "$json_value"
}
mutate_delete() {
  local dotted="$1"
  local report="$2"
  "$PYTHON" "$WORK/mutate.py" "$report" delete "$dotted"
}
mutate_malformed() {
  "$PYTHON" "$WORK/mutate.py" "$1" malformed
}
mutate_duplicate_key() {
  "$PYTHON" "$WORK/mutate.py" "$1" duplicate-top-key
}
remove_artifact() {
  local relative="$1"
  local report="$2"
  rm "$(dirname "$report")/$relative"
}
empty_artifact() {
  local relative="$1"
  local report="$2"
  : > "$(dirname "$report")/$relative"
}
make_target_nonclaim() {
  local report="$1"
  "$PYTHON" "$WORK/mutate.py" "$report" set synthetic_test_fixture false
  "$PYTHON" "$WORK/mutate.py" "$report" set evidence_class '"target-data-lifecycle"'
  # release_claim deliberately remains none.
}

# Production mode must never accept the complete non-claim fixture.
expect_default_failure synthetic-fixture-in-production-mode no_change
expect_default_failure claim-none make_target_nonclaim

# Provenance, target identity, and fail-closed JSON shape.
expect_fixture_failure missing-binary-provenance mutate_delete target.binary_sha256
expect_fixture_failure missing-provenance-artifact-reference mutate_delete target.provenance_artifact_id
expect_fixture_failure mutable-source-revision mutate_set target.source_revision '"main"'
expect_fixture_failure placeholder-release mutate_set target.release_id '"TBD"'
expect_fixture_failure local-report mutate_set target.environment '"local"'
expect_fixture_failure example-report mutate_set target.environment '"example"'
expect_fixture_failure local-cluster-identity mutate_set target.cluster_id '"local-cluster"'
expect_fixture_failure unknown-field mutate_set target.unreviewed_field '"value"'
expect_fixture_failure malformed-json mutate_malformed
expect_fixture_failure duplicate-json-key mutate_duplicate_key

# Pre-drill health/quorum/readiness/metrics must all prove a passing target.
expect_fixture_failure pre-health-failed mutate_set pre_drill.health_result '"fail"'
expect_fixture_failure pre-quorum-failed mutate_set pre_drill.quorum_result '"fail"'
expect_fixture_failure pre-quorum-incomplete mutate_set pre_drill.healthy_voters '2'
expect_fixture_failure pre-readiness-failed mutate_set pre_drill.readiness_result '"fail"'
expect_fixture_failure pre-metrics-failed mutate_set pre_drill.metrics_result '"fail"'

# Checkpoint identity, semantic verification, and off-host immutable retention.
expect_fixture_failure placeholder-checkpoint mutate_set checkpoint.checkpoint_id '"placeholder"'
expect_fixture_failure checkpoint-semantic-failed mutate_set checkpoint.semantic_verification_result '"fail"'
expect_fixture_failure checkpoint-empty-records mutate_set checkpoint.record_count '0'
expect_fixture_failure on-host-retention mutate_set checkpoint.retention.location '"file:///var/backups/checkpoint"'
expect_fixture_failure retention-unverified mutate_set checkpoint.retention.result '"fail"'

# The recovery must be on a stopped affected node and include every transition.
expect_fixture_failure disaster-scenario-missing mutate_delete disaster.scenario
expect_fixture_failure disaster-result-failed mutate_set disaster.result '"fail"'
expect_fixture_failure stopped-node-unconfirmed mutate_set recovery.stopped_node_confirmed 'false'
expect_fixture_failure stopped-node-mismatch mutate_set recovery.stopped_node '"node-west-3"'
expect_fixture_failure recovery-operation-failed mutate_set recovery.operation_result '"fail"'
expect_fixture_failure missing-recovery-transcript mutate_delete recovery.transcript_artifact_id
expect_fixture_failure rollback-quarantine-failed mutate_set recovery.rollback_or_quarantine.result '"fail"'
expect_fixture_failure restart-failed mutate_set recovery.restart.result '"fail"'
expect_fixture_failure rejoin-failed mutate_set recovery.rejoin.result '"fail"'
expect_fixture_failure catch-up-failed mutate_set recovery.catch_up.result '"fail"'
expect_fixture_failure catch-up-lagged mutate_set recovery.catch_up.lag_entries '1'

# Canary, duplicate-application, and data-loss contracts are semantic checks.
expect_fixture_failure changed-canary mutate_set canaries.values.0.post_value '"different"'
expect_fixture_failure placeholder-canary mutate_set canaries.values.0.post_value '"TBD"'
expect_fixture_failure duplicate-canary-key mutate_set canaries.values.1.key '"orders/canary-17"'
expect_fixture_failure integrity-result-failed mutate_set integrity.result '"fail"'
expect_fixture_failure duplicate-application mutate_set integrity.duplicate_applications '1'
expect_fixture_failure data-loss mutate_set integrity.data_loss_records '1'
expect_fixture_failure record-count-mismatch mutate_set integrity.observed_record_count '1199'
expect_fixture_failure application-count-mismatch mutate_set integrity.observed_application_count '2399'

# RPO/RTO measurements must bind to the timeline and meet declared thresholds.
expect_fixture_failure rpo-threshold-missed mutate_set objectives.rpo.threshold_seconds '30'
expect_fixture_failure rpo-measurement-inconsistent mutate_set objectives.rpo.measured_seconds '59'
expect_fixture_failure rto-threshold-missed mutate_set objectives.rto.threshold_seconds '60'
expect_fixture_failure rto-measurement-inconsistent mutate_set objectives.rto.measured_seconds '119'
expect_fixture_failure objective-result-not-met mutate_set objectives.rto.result '"missed"'

# Post-recovery observability and both independent approvals are mandatory.
expect_fixture_failure logs-failed mutate_set observability.logs_result '"fail"'
expect_fixture_failure readiness-failed mutate_set observability.readiness_result '"fail"'
expect_fixture_failure metrics-failed mutate_set observability.metrics_result '"fail"'
expect_fixture_failure operator-placeholder mutate_set sign_off.operator.name '"unknown"'
expect_fixture_failure operator-not-approved mutate_set sign_off.operator.result '"rejected"'
expect_fixture_failure reviewer-not-independent mutate_set sign_off.independent_reviewer.name '"Casey Morgan"'
expect_fixture_failure reviewer-not-approved mutate_set sign_off.independent_reviewer.result '"rejected"'

# Raw evidence is closed under references and must exist, be nonempty, unique,
# normalized, and match its declared SHA-256.
expect_fixture_failure missing-raw-artifact remove_artifact raw/recovery-transcript.txt
expect_fixture_failure empty-raw-artifact empty_artifact raw/recovery-transcript.txt
expect_fixture_failure unhashed-raw-artifact mutate_set raw_artifacts.0.sha256 '""'
expect_fixture_failure mismatched-raw-hash mutate_set raw_artifacts.0.sha256 '"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'
expect_fixture_failure duplicate-artifact-id mutate_set raw_artifacts.2.id '"pre-health"'
expect_fixture_failure duplicate-artifact-path mutate_set raw_artifacts.2.path '"raw/pre-health.txt"'
expect_fixture_failure artifact-path-traversal mutate_set raw_artifacts.0.path '"../pre-health.txt"'
expect_fixture_failure artifact-captured-before-drill mutate_set raw_artifacts.0.captured_at '"2000-01-01T00:00:00Z"'

# Timeline ordering and evidence freshness are fail-closed.
expect_fixture_failure inconsistent-time-order mutate_set recovery.rejoin.at '"2000-01-01T00:00:00Z"'
STALE="$WORK/stale"
make_fixture "$STALE" 60
if "$PYTHON" "$VERIFY" --self-test-fixture "$STALE/evidence.json" >"$STALE/result.out" 2>&1; then
  cat "$STALE/result.out" >&2
  fail "stale-evidence-was-accepted"
fi
pass_count=$((pass_count + 1))
"$PYTHON" "$WORK/mutate.py" "$STALE/evidence.json" refresh-generation
if "$PYTHON" "$VERIFY" --self-test-fixture "$STALE/evidence.json" >"$STALE/repackaged-result.out" 2>&1; then
  cat "$STALE/repackaged-result.out" >&2
  fail "repackaged-stale-evidence-was-accepted"
fi
pass_count=$((pass_count + 1))

printf 'target-data-lifecycle-evidence-selftest status=pass cases=%d release_claim=none destructive_operations=none\n' "$pass_count"
