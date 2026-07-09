#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

require_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "missing operations-readiness artifact: $file" >&2
    exit 1
  fi
}

require_text() {
  local file="$1"
  local text="$2"
  if ! LC_ALL=C grep -Fq -- "$text" "$file"; then
    echo "missing operations-readiness text in $file: $text" >&2
    exit 1
  fi
}

unit="deploy/systemd/kvnode@.service"
env_example="deploy/systemd/kvnode.env.example"
runbook="docs/operations/kvnode-data-lifecycle-incident-runbook.md"
upgrade="docs/operations/kvnode-upgrade-rollback.md"
capacity="tests/kvnode_capacity_envelope.sh"
manifest="tests/kvnode_systemd_manifest_audit.sh"
mixed_drill="tests/kvnode_mixed_version_drill.sh"

for file in "$unit" "$env_example" "$runbook" "$upgrade" "$capacity" "$manifest" "$mixed_drill"; do
  require_file "$file"
done

# systemd deployment template: example-only status, expected sections, kvnode
# launch contract, and sandboxing/hardening markers.
require_text "$unit" "Example/operator material for the EPaxos KV node."
require_text "$unit" "not a verified production deployment manifest"
require_text "$unit" "[Unit]"
require_text "$unit" "[Service]"
require_text "$unit" "[Install]"
require_text "$unit" "ExecStart=/usr/local/bin/kvnode"
require_text "$unit" '-id ${KVNODE_ID}'
require_text "$unit" '-listen ${KVNODE_CLIENT_LISTEN}'
require_text "$unit" '-peer-listen ${KVNODE_PEER_LISTEN}'
require_text "$unit" '-admin-listen ${KVNODE_ADMIN_LISTEN}'
require_text "$unit" '-data ${KVNODE_DATA_DIR}'
require_text "$unit" '-peers ${KVNODE_PEERS}'
require_text "$unit" '-request-deadline-ms ${KVNODE_REQUEST_DEADLINE_MS}'
require_text "$unit" '-peer-deadline-ms ${KVNODE_PEER_DEADLINE_MS}'
require_text "$unit" '-max-client-body-bytes ${KVNODE_MAX_CLIENT_BODY_BYTES}'
require_text "$unit" '-max-peer-body-bytes ${KVNODE_MAX_PEER_BODY_BYTES}'
require_text "$unit" '-max-admin-body-bytes ${KVNODE_MAX_ADMIN_BODY_BYTES}'
require_text "$unit" '-max-scan-limit ${KVNODE_MAX_SCAN_LIMIT}'
require_text "$unit" '$KVNODE_TLS_ARGS'
require_text "$unit" "NoNewPrivileges=true"
require_text "$unit" "PrivateTmp=true"
require_text "$unit" "ProtectSystem=strict"
require_text "$unit" "ProtectHome=true"
require_text "$unit" "ReadWritePaths=/var/lib/kvnode/%i"
require_text "$unit" "ReadOnlyPaths=/etc/kvnode"
require_text "$unit" "CapabilityBoundingSet="
require_text "$unit" "AmbientCapabilities="
require_text "$unit" "PrivateDevices=true"
require_text "$unit" "ProtectClock=true"
require_text "$unit" "ProtectControlGroups=true"
require_text "$unit" "ProtectKernelLogs=true"
require_text "$unit" "ProtectKernelModules=true"
require_text "$unit" "ProtectKernelTunables=true"
require_text "$unit" "RestrictRealtime=true"
require_text "$unit" "RestrictSUIDSGID=true"
require_text "$unit" "LockPersonality=true"
require_text "$unit" "MemoryDenyWriteExecute=true"
require_text "$unit" "SystemCallArchitectures=native"

# Environment example: peer topology, deadlines, request limits, TLS knobs, and
# example-only status must stay visible to operators copying the file.
require_text "$env_example" "Example/operator material for kvnode@.service."
require_text "$env_example" "not a verified production deployment environment"
require_text "$env_example" "KVNODE_PEER_LISTEN="
require_text "$env_example" "KVNODE_PEERS="
require_text "$env_example" "KVNODE_REQUEST_DEADLINE_MS="
require_text "$env_example" "KVNODE_PEER_DEADLINE_MS="
require_text "$env_example" "KVNODE_MAX_CLIENT_BODY_BYTES="
require_text "$env_example" "KVNODE_MAX_PEER_BODY_BYTES="
require_text "$env_example" "KVNODE_MAX_ADMIN_BODY_BYTES="
require_text "$env_example" "KVNODE_MAX_SCAN_LIMIT="
require_text "$env_example" "KVNODE_TLS_ARGS="
require_text "$env_example" "-tls-cert=/etc/kvnode/tls/node1.crt"
require_text "$env_example" "-tls-key=/etc/kvnode/tls/node1.key"
require_text "$env_example" "-tls-ca=/etc/kvnode/tls/ca.crt"
require_text "$env_example" "transport configuration only; it does not add application authz/authn"

# Cross-platform manifest exercise: renders the example EnvironmentFile into the
# ExecStart contract and runs systemd-analyze verify when the host provides it.
require_text "$manifest" "rendered_exec="
require_text "$manifest" "systemd_analyze=skipped"
require_text "$manifest" "KVNODE_SYSTEMD_ANALYZE=yes"
require_text "$manifest" "systemd-analyze verify"
bash "$manifest" >/dev/null

# Data lifecycle/incident runbook: backup/verify/repair/restore boundaries,
# confirmations, evidence capture, and named incident response procedures.
require_text "$runbook" "Status: example/operator runbook material"
require_text "$runbook" "does not make a release, production-ready, or go-live claim"
require_text "$runbook" "## Non-claims and hard boundaries"
require_text "$runbook" "Automatic in-place Pebble/WAL repair is not claimed"
require_text "$runbook" "Automatic EPaxos checksum repair is not claimed"
require_text "$runbook" "kv.VerifyCheckpoint"
require_text "$runbook" "kv.RepairFromCheckpoint"
require_text "$runbook" "kv.RestoreCheckpoint"
require_text "$runbook" "semantic checkpoint verification"
require_text "$runbook" "kv.Cluster.RecoverReplicaFromLiveCheckpoint"
require_text "$runbook" "target-owned next-instance floor"
require_text "$runbook" "## Evidence capture baseline"
require_text "$runbook" "## One-time offline checkpoint/restore helper"
require_text "$runbook" "## Pebble checkpoint backup"
require_text "$runbook" "## Offline whole-directory repair or restore from a checkpoint"
require_text "$runbook" "## Checksum-mismatch response"
require_text "$runbook" "External host destructive-storage and wall-clock-skew runs are outside the current simulation-scoped release evidence"
require_text "$runbook" "External host destructive-storage runs are outside current release evidence"
require_text "$runbook" "Local drill command"
require_text "$runbook" "JEPSEN_LOCAL_FAULTS=destructive-storage bash tests/jepsen_local.sh"
require_text "$runbook" "Environment variables used for the local loopback run"
require_text "$runbook" "Do not point destructive-storage drills at a shared application directory"
require_text "$runbook" "A destructive-storage pass proves remove/restore of the original directory under the test harness"
require_text "$runbook" "## Incident response: storage failure"
require_text "$runbook" "## Incident response: network partition"
require_text "$runbook" "## Incident response: peer compromise"
require_text "$runbook" "## Incident response: replay or checksum suspicion"
require_text "$runbook" "## Incident response: recovery stalls"
require_text "$runbook" "No one performed automatic in-place repair, checksum recomputation, corrupt-record deletion, or synthesized reconstruction without a verified checkpoint under this runbook."

# Rolling upgrade/rollback plan: one-node-at-a-time upgrade, checkpoint before
# binary replacement, rollback criteria/procedure, and post-checks.
require_text "$upgrade" "Status: operator plan only."
require_text "$upgrade" "does not assert production readiness"
require_text "$upgrade" "one-node-at-a-time replacement"
require_text "$upgrade" "Upgrade exactly one node at a time."
require_text "$upgrade" "## Per-node rolling replacement"
require_text "$upgrade" "**Checkpoint before upgrade.**"
require_text "$upgrade" "Treat a failed checkpoint as a hard stop; do not upgrade that node."
require_text "$upgrade" "**Post-check the selected node.**"
require_text "$upgrade" "**Post-check the cluster.**"
require_text "$upgrade" "## Rollback criteria"
require_text "$upgrade" "Rollback is mandatory when any of these conditions occur"
require_text "$upgrade" "## Rollback procedure"
require_text "$upgrade" "Rollback one node at a time, using the latest node that was changed first."
require_text "$upgrade" "Start the old binary on the node's current data directory"
require_text "$upgrade" "checkpoint restore can discard committed entries"
require_text "$upgrade" "Run the same node and cluster post-checks used after upgrade"
require_text "$upgrade" "this plan by itself is not production evidence"

# Mixed-version drill harness: maintained local-loopback artifact only. Syntax is
# audited here; execution requires explicit old/new refs and remains opt-in.
require_text "$mixed_drill" "kvnode mixed-version upgrade/rollback drill"
require_text "$mixed_drill" "KVNODE_UPGRADE_OLD_REF"
require_text "$mixed_drill" "build_source=git_archive_trimpath"
require_text "$mixed_drill" "Binary rollback in this drill restarts the old binary on the node's current data"
require_text "$mixed_drill" "checkpoint restore is a separate data-lifecycle fallback"
bash -n "$mixed_drill"

# Capacity-envelope harness: opt-in execution, bounded inputs, output evidence
# files, and no standalone production-evidence claim.
require_text "$capacity" "kvnode capacity-envelope harness (opt-in, bounded)"
require_text "$capacity" "KVNODE_CAPACITY_RUN=yes"
require_text "$capacity" "Refusing to run without KVNODE_CAPACITY_RUN=yes."
require_text "$capacity" "Default: 30, max: 1000"
require_text "$capacity" "Default: 64,1024,4096"
require_text "$capacity" "Default: 1,16,128"
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_OPS_PER_PHASE "$ops_per_phase" 1000'
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_TIMEOUT_SECONDS "$timeout_seconds" 300'
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_MAX_VALUE_BYTES "$max_value_bytes" 1048576'
require_text "$capacity" 'bounded_int KVNODE_CAPACITY_MAX_SCAN_LIMIT "$max_scan_limit" 100000'
require_text "$capacity" "metadata.env                    Harness inputs and peer-count label."
require_text "$capacity" "latency.csv                     operation,http_status,seconds rows."
require_text "$capacity" "resources.csv                   before/after RSS, disk, queue-depth samples."
require_text "$capacity" "summary.md                      Machine-generated sample summary with no readiness claim."
require_text "$capacity" "not production capacity evidence"
require_text "$capacity" "status=harness-only"
require_text "$capacity" "latency_summary="
require_text "$capacity" "Throughput sample:"
require_text "$capacity" "Memory RSS samples:"
require_text "$capacity" "Disk growth samples:"
require_text "$capacity" "Queue-depth samples:"
require_text "$capacity" "Peer-count coverage:"
