# KV node data lifecycle and incident runbook

Status: example/operator runbook material until exercised in the target environment. This document does not make a release, production-ready, or go-live claim.

Scope: the example KV node under `examples/kv/cmd/kvnode`, with a Pebble data directory supplied by `-data` and distinct client, peer, and admin listeners supplied by `-listen`, `-peer-listen`, and `-admin-listen`. The binary is built from `examples/kv` with:

```sh
cd /path/to/moreconsensus/examples/kv
go build -tags kvnode -o /opt/moreconsensus/bin/kvnode ./cmd/kvnode
```

The runbook covers backups, semantic checkpoint verification, explicit offline checkpoint-backed repair, tested live-source checkpoint replacement boundaries, whole-directory restore, corruption response, destructive-storage drill boundaries, and first-response procedures for storage, network, peer-compromise, replay/checksum, and recovery-stall incidents.

## Non-claims and hard boundaries

- Automatic in-place Pebble/WAL repair is not claimed. Do not run tools that rewrite live Pebble files as a repair step under this runbook.
- Automatic EPaxos checksum repair is not claimed. Do not recompute checksums, delete corrupt records, or edit persisted EPaxos records to force startup.
- Synthesized quorum-backed state reconstruction after bit-level corruption, majority data loss, or compromised-peer data poisoning is not claimed. The tested live-source path copies a verified checkpoint from a live source and fails closed unless live quorum support and the target-owned next-instance floor are present.
- `kv.VerifyCheckpoint` opens a checkpoint read-only and semantically validates EPaxos records, key/ref consistency, applied markers, dense timestamped KV rows, and dependency order. `kv.RepairFromCheckpoint` is an explicit offline whole-directory replacement from that verified checkpoint. `kv.RestoreCheckpoint` remains a byte-copy primitive; none of these functions repairs a corrupt live directory in place, recomputes checksums, or deletes individual records.
- `kv.Cluster.RecoverReplicaFromLiveCheckpoint` is a tested in-process/local-harness recovery mode for one stopped corrupt member while a healthy quorum remains: it checkpoints a live source, runs semantic checkpoint verification, checks live quorum support and target-owned owner-ref floor, then performs whole-directory replacement. The current `kvnode` service still has no HTTP/admin backup or restore endpoint.
- Destructive-storage Jepsen tests move/remove a stopped node data directory and restore the original directory before restart. They are not evidence that arbitrary disk corruption can be repaired in place.
- External host destructive-storage and wall-clock-skew runs are outside the current simulation-scoped release evidence. Keep any such optional validation on disposable or explicitly approved hosts and archive it separately.
- The current `kvnode` service exposes admin health, readiness, metrics, and test fault controls; it does not expose an HTTP backup or restore endpoint. The helper below opens the KV store offline, so the node using that data directory must be stopped first.

## Operator variables

Set these before running the examples. Replace addresses, service names, and paths with the environment-specific values.

```sh
export NODE_ID=1
export CLIENT_ADDR="127.0.0.1:8080"
export PEER_ADDR="127.0.0.1:8081"
export ADMIN_ADDR="127.0.0.1:8082"
export DATA_DIR="/srv/kv/node-${NODE_ID}"
export CHECKPOINT_ROOT="/srv/kv-checkpoints/node-${NODE_ID}"
export INCIDENT_ROOT="/srv/kv-incidents"
export SERVICE="kvnode@${NODE_ID}.service"
export STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
export EVIDENCE_DIR="${INCIDENT_ROOT}/${STAMP}-node-${NODE_ID}"
mkdir -p "${CHECKPOINT_ROOT}" "${EVIDENCE_DIR}"
```

Useful admin endpoints on the admin listener:

```sh
curl -fsS "http://${ADMIN_ADDR}/livez"       # liveness-compatible endpoint, body: ok
curl -fsS "http://${ADMIN_ADDR}/readyz"      # readiness; storage fault returns 503
curl -fsS "http://${ADMIN_ADDR}/health"      # liveness-compatible health endpoint
curl -fsS "http://${ADMIN_ADDR}/metrics"     # text metrics including storage fault and send queue
curl -fsS "http://${ADMIN_ADDR}/faults/storage"    # test storage fault state
curl -fsS "http://${ADMIN_ADDR}/faults/transport"  # test transport drops
```

If TLS is enabled with `-tls-cert`, `-tls-key`, and `-tls-ca`, use `https://` and supply the appropriate client trust settings, for example:

```sh
curl --cacert /etc/kvnode/ca.pem -fsS "https://${ADMIN_ADDR}/readyz"
```

## Evidence capture baseline

Before any backup, restore, or incident action, capture enough context to explain what happened and to roll back if needed.

```sh
mkdir -p "${EVIDENCE_DIR}"
date -u +%Y-%m-%dT%H:%M:%SZ | tee "${EVIDENCE_DIR}/operator-started-at.txt"
printf 'node_id=%s\nclient=%s\npeer=%s\nadmin=%s\ndata_dir=%s\n' \
  "${NODE_ID}" "${CLIENT_ADDR}" "${PEER_ADDR}" "${ADMIN_ADDR}" "${DATA_DIR}" \
  | tee "${EVIDENCE_DIR}/node-context.env"

curl -sS "http://${ADMIN_ADDR}/livez"          -o "${EVIDENCE_DIR}/livez.txt"          || true
curl -sS "http://${ADMIN_ADDR}/readyz"         -o "${EVIDENCE_DIR}/readyz.txt"         || true
curl -sS "http://${ADMIN_ADDR}/health"         -o "${EVIDENCE_DIR}/health.txt"         || true
curl -sS "http://${ADMIN_ADDR}/metrics"        -o "${EVIDENCE_DIR}/metrics.txt"        || true
curl -sS "http://${ADMIN_ADDR}/faults/storage" -o "${EVIDENCE_DIR}/faults-storage.json" || true
curl -sS "http://${ADMIN_ADDR}/faults/transport" -o "${EVIDENCE_DIR}/faults-transport.json" || true

systemctl status "${SERVICE}" --no-pager > "${EVIDENCE_DIR}/systemctl-status.txt" 2>&1 || true
journalctl -u "${SERVICE}" --since "30 minutes ago" --no-pager > "${EVIDENCE_DIR}/journal-last-30m.txt" 2>&1 || true
```

For data-directory evidence, prefer a stopped-node copy. If the node is still running and you only need metadata, capture a file list without copying live Pebble files:

```sh
find "${DATA_DIR}" -maxdepth 2 -print > "${EVIDENCE_DIR}/data-dir-listing.txt" 2>&1 || true
du -sh "${DATA_DIR}" > "${EVIDENCE_DIR}/data-dir-size.txt" 2>&1 || true
```

## One-time offline checkpoint/restore helper

Use the maintained offline helper in `examples/kv/cmd/kvcheckpoint`. It imports `gosuda.org/moreconsensus/examples/kv` and exposes the same checkpoint, semantic verification, verified restore, and verified repair operations documented below; semantic live-source replacement remains covered by the in-process `Cluster` test harness, not this standalone `kvnode` helper.

Build it from the checkout that matches the deployed binary version you are drilling:

```sh
cd /path/to/moreconsensus/examples/kv
go build -o /opt/moreconsensus/bin/kvcheckpoint ./cmd/kvcheckpoint
```

Preconditions:

- The `kvnode` process using `DATA_DIR` is stopped before `checkpoint`, `restore`, or `repair`. `verify` opens only `CHECKPOINT_DIR` read-only and may run before stopping the node.
- `CHECKPOINT_DIR` is on storage with enough capacity for a complete Pebble checkpoint plus manifest files.
- The checkpoint directory must be new or empty for backup. Do not overwrite an existing checkpoint.
- Use the helper from the same repository version as the deployed `kvnode` binary and data format.

Commands:

```sh
export CHECKPOINT_DIR="${CHECKPOINT_ROOT}/${STAMP}"
test ! -e "${CHECKPOINT_DIR}"

/opt/moreconsensus/bin/kvcheckpoint checkpoint "${DATA_DIR}" "${CHECKPOINT_DIR}"
/opt/moreconsensus/bin/kvcheckpoint verify "${CHECKPOINT_DIR}"
```

After verification, choose exactly one recovery path. For checksum/corruption response, use the preferred repair path:

```sh
/opt/moreconsensus/bin/kvcheckpoint repair "${DATA_DIR}" "${CHECKPOINT_DIR}"
```

For full-directory rollback instead, use the restore path:

```sh
/opt/moreconsensus/bin/kvcheckpoint restore "${DATA_DIR}" "${CHECKPOINT_DIR}"
```

`repair` is the preferred operator command for checksum/corruption response because it calls semantic checkpoint verification before whole-directory replacement. `restore` is retained for full-directory rollback and also verifies the checkpoint before copying; the helper intentionally does not expose an unverified raw byte-copy restore path. Do not run both recovery commands for the same incident unless a reviewed rollback plan explicitly calls for a second replacement.

Rollback note: deleting the built helper binary is sufficient to remove the helper. It does not modify data until you run `checkpoint`, `restore`, or `repair`; `verify` opens the checkpoint read-only.

## Pebble checkpoint backup

This procedure produces a Pebble checkpoint for one node data directory. Because the current service has no admin checkpoint endpoint, the safe operator procedure is an offline checkpoint of a stopped node. For a live backup, implement `DB.Checkpoint` inside the process that already owns the Pebble handle and document that separately after it is tested.

Preconditions:

- The cluster has quorum without this node, or the maintenance window accepts this node being stopped.
- `curl http://${ADMIN_ADDR}/readyz` succeeded before stopping, unless the backup is being taken for incident forensics.
- `CHECKPOINT_ROOT` is outside `DATA_DIR`. Never write checkpoints under the live data directory.
- There is enough free space for a full checkpoint and an off-host copy.
- A previous checkpoint path will not be reused.

Commands:

```sh
export CHECKPOINT_DIR="${CHECKPOINT_ROOT}/${STAMP}"
test ! -e "${CHECKPOINT_DIR}"

# Capture pre-stop state.
mkdir -p "${EVIDENCE_DIR}"
curl -sS "http://${ADMIN_ADDR}/readyz"  -o "${EVIDENCE_DIR}/backup-pre-readyz.txt"  || true
curl -sS "http://${ADMIN_ADDR}/metrics" -o "${EVIDENCE_DIR}/backup-pre-metrics.txt" || true
journalctl -u "${SERVICE}" --since "15 minutes ago" --no-pager > "${EVIDENCE_DIR}/backup-pre-journal.txt" 2>&1 || true

# Stop only the selected node; adapt this line if the environment does not use systemd.
sudo systemctl stop "${SERVICE}"

# Run the offline checkpoint.
/opt/moreconsensus/bin/kvcheckpoint checkpoint "${DATA_DIR}" "${CHECKPOINT_DIR}"

# Record a relocatable checkpoint manifest and protect the checkpoint from accidental edits.
(
  cd "${CHECKPOINT_DIR}"
  find . -type f -print0 | sort -z | xargs -0 shasum -a 256
) > "${CHECKPOINT_DIR}.sha256"
(
  cd "${CHECKPOINT_DIR}"
  find . -type f -print
) > "${EVIDENCE_DIR}/checkpoint-files.txt"
du -sh "${CHECKPOINT_DIR}" > "${EVIDENCE_DIR}/checkpoint-size.txt"
chmod -R a-w "${CHECKPOINT_DIR}"

# Copy off-host or to immutable backup storage. Replace the destination with the approved backup target.
rsync -a --numeric-ids --delete "${CHECKPOINT_DIR}/" "backup-host:/backups/kvnode/${NODE_ID}/${STAMP}/"
rsync -a --numeric-ids "${CHECKPOINT_DIR}.sha256" "backup-host:/backups/kvnode/${NODE_ID}/${STAMP}.sha256"

# Restart and verify the node rejoins.
sudo systemctl start "${SERVICE}"
curl -fsS "http://${ADMIN_ADDR}/readyz"
curl -fsS "http://${ADMIN_ADDR}/metrics" | tee "${EVIDENCE_DIR}/backup-post-metrics.txt"
```

Rollback notes:

- If checkpoint creation fails before restart, leave `CHECKPOINT_DIR` in place for inspection, keep the node stopped only as long as needed to collect logs, then restart from the unchanged `DATA_DIR`.
- If restart fails after a backup, do not alter `DATA_DIR`; collect `journalctl -u "${SERVICE}"` and the helper output, then restart from the same directory after resolving the operational fault.
- A checkpoint is a backup candidate only after its manifest and off-host copy are recorded. A local-only checkpoint does not satisfy disaster-recovery evidence.

Evidence to retain:

- Operator command transcript and UTC timestamps.
- Pre/post `/readyz` and `/metrics` output.
- `journalctl` around stop, checkpoint, and restart.
- `CHECKPOINT_DIR`, `CHECKPOINT_DIR.sha256`, file listing, size, and off-host copy location.

## Offline whole-directory repair or restore from a checkpoint

`kv.RestoreCheckpoint(dataDir, checkpointDir)` is a byte-copy primitive that replaces `DATA_DIR` with a copied checkpoint directory. `kv.RepairFromCheckpoint(dataDir, checkpointDir)` first opens the checkpoint read-only, validates EPaxos records, key/ref consistency, applied markers, dense timestamped KV rows, and dependency order, and then calls the same whole-directory replacement. Both move the old data directory aside as `basename.pre-restore-*` before the final rename; if that final rename fails, they attempt to roll the old directory back.

Preconditions:

- The node using `DATA_DIR` is stopped and no process has the Pebble directory open.
- The checkpoint was previously verified with its checksum manifest and is from the same deployment line you intend to recover to.
- The restore target is not `.` or `/`; `RestoreCheckpoint` rejects those broad targets, but operators must still verify paths before running commands.
- The operator accepts data loss for writes after the checkpoint timestamp on this replica.
- At least a quorum of non-restored peers remains trusted before rejoining one restored node. If multiple replicas are corrupt or lost, stop and escalate; this runbook does not claim synthesized quorum reconstruction without a verified checkpoint.

Commands:

```sh
set -euo pipefail
export CHECKPOINT_DIR="/srv/kv-checkpoints/node-${NODE_ID}/YYYYMMDDTHHMMSSZ"
export RESTORE_EVIDENCE_DIR="${INCIDENT_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)-restore-node-${NODE_ID}"
mkdir -p "${RESTORE_EVIDENCE_DIR}"

# Verify the checkpoint manifest before using it. The manifest uses paths relative to CHECKPOINT_DIR.
(
  cd "${CHECKPOINT_DIR}"
  shasum -a 256 -c "${CHECKPOINT_DIR}.sha256"
) | tee "${RESTORE_EVIDENCE_DIR}/checkpoint-verify.txt"

# Verify the checkpoint can be opened read-only and semantically matches its applied EPaxos commands.
/opt/moreconsensus/bin/kvcheckpoint verify "${CHECKPOINT_DIR}" > "${RESTORE_EVIDENCE_DIR}/checkpoint-epaxos-verify.txt"
cat "${RESTORE_EVIDENCE_DIR}/checkpoint-epaxos-verify.txt"

# Stop and preserve a separate quarantine copy if retention is required.
sudo systemctl stop "${SERVICE}"
rsync -a --numeric-ids "${DATA_DIR}/" "${RESTORE_EVIDENCE_DIR}/live-data-before-restore/" || true
find "${DATA_DIR}" -type f -print0 | sort -z | xargs -0 shasum -a 256 > "${RESTORE_EVIDENCE_DIR}/live-data-before-restore.sha256" 2>/dev/null || true

# Repair from the whole checkpoint; this verifies again before byte-copy replacement.
/opt/moreconsensus/bin/kvcheckpoint repair "${DATA_DIR}" "${CHECKPOINT_DIR}" > "${RESTORE_EVIDENCE_DIR}/repair-helper.txt"
cat "${RESTORE_EVIDENCE_DIR}/repair-helper.txt"

# Restart one restored node and check only its admin plane first.
sudo systemctl start "${SERVICE}"
curl -fsS "http://${ADMIN_ADDR}/livez"  | tee "${RESTORE_EVIDENCE_DIR}/post-restore-livez.txt"
curl -fsS "http://${ADMIN_ADDR}/readyz" | tee "${RESTORE_EVIDENCE_DIR}/post-restore-readyz.txt"
curl -fsS "http://${ADMIN_ADDR}/metrics" | tee "${RESTORE_EVIDENCE_DIR}/post-restore-metrics.txt"
```

Rollback notes:

- If `RepairFromCheckpoint` fails before the final rename, it should leave the original directory in place or attempt to rename the pre-restore directory back. Confirm with `find "${DATA_DIR}" -maxdepth 1 -print` before starting the service.
- If the service fails after a successful restore, stop it and either rerun restore from a different verified checkpoint or restore from the quarantine copy captured in `live-data-before-restore/`. Do not try to patch individual Pebble or EPaxos files.
- Because a successful restore removes the internal `*.pre-restore-*` temporary directory, create an explicit quarantine copy before the restore whenever incident retention matters.

Evidence to retain:

- Checkpoint source path, checksum verification output, and checkpoint timestamp.
- Stop/start transcript, helper verify/repair output, and post-restore admin endpoint output.
- Quarantine copy path and checksums if a corrupt live directory existed.
- A statement of expected data-loss window: checkpoint timestamp through restore completion.

## Checksum-mismatch response

Symptoms include `epaxos.ErrChecksumMismatch` in tests or logs, startup failure after opening a KV cluster, or repeated process exit before admin endpoints bind. Treat this as data corruption or replay suspicion until proven otherwise.

Immediate actions:

```sh
export INCIDENT="${INCIDENT_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)-checksum-node-${NODE_ID}"
mkdir -p "${INCIDENT}"

sudo systemctl stop "${SERVICE}" || true
journalctl -u "${SERVICE}" --since "2 hours ago" --no-pager > "${INCIDENT}/journal-last-2h.txt" 2>&1 || true
find "${DATA_DIR}" -maxdepth 2 -print > "${INCIDENT}/data-dir-listing.txt" 2>&1 || true
find "${DATA_DIR}" -type f -print0 | sort -z | xargs -0 shasum -a 256 > "${INCIDENT}/data-dir.sha256" 2>/dev/null || true
rsync -a --numeric-ids "${DATA_DIR}/" "${INCIDENT}/quarantine-data/" || true
chmod -R a-w "${INCIDENT}/quarantine-data" || true
```

Decision path:

1. If a checkpoint exists, first verify its manifest and then use `kvcheckpoint verify` plus `kvcheckpoint repair` from the offline helper. This authenticates persisted EPaxos records before whole-directory replacement.
2. If no verified checkpoint exists and quorum remains healthy without this node, keep this node stopped and preserve evidence. Do not restart it against the cluster with suspected corrupt state.
3. If a quorum is unavailable and multiple nodes have suspected corruption, stop and escalate to incident command. This runbook does not claim majority reconstruction or corrupt-record deletion.
4. If the checksum mismatch follows a test fault injection rather than real disk evidence, still preserve logs and confirm the test harness restored original storage before restarting.

Rollback notes:

- A failed repair attempt does not authorize in-place repair. Return to a verified checkpoint or the quarantined pre-restore copy only if incident command explicitly chooses that risk.
- Never make the quarantined corrupt directory writable except for a controlled forensic copy.

Evidence to retain:

- Exact error text, process exit status, and first log line showing checksum failure.
- Checksums of the suspected directory and the checkpoint used for restore.
- Whether the affected node was serving client, peer, or admin traffic when the error appeared.
- Confirmation that automatic checksum recomputation, record deletion, or Pebble rewrite was not performed.

## Destructive-storage drill and recovery boundaries

Local destructive-storage Jepsen behavior: each selected process is stopped, its Pebble directory is moved aside to a sibling `*.removed` directory, and the original directory is restored before the process rejoins.

Local drill command:

```sh
JEPSEN_LOCAL_FAULTS=destructive-storage bash tests/jepsen_local.sh \
  2>&1 | tee "${EVIDENCE_DIR}/jepsen-local-destructive-storage.txt"
```

External host destructive-storage runs are outside current release evidence. If an operator keeps separate external validation tooling, run it only on disposable or explicitly approved hosts and archive its output separately; this runbook's closed evidence is the local drill command above plus deterministic storage-failure tests.

Boundaries:

- Do not point destructive-storage drills at a shared application directory, host root, home directory, or parent of non-test data.
- A destructive-storage pass proves remove/restore of the original directory under the test harness. It does not prove restore from an arbitrary old checkpoint, in-place corruption repair, or recovery after majority data loss.

Evidence to retain:

- Environment variables used for the local loopback run, including `JEPSEN_LOCAL_FAULTS=destructive-storage` and any base-port overrides.
- Full local Jepsen run output and generated store artifacts.
- Per-process `kvnode` logs showing stop, storage remove, restore, restart, and health check.


## Local data lifecycle drill

The build-tagged Go runner includes a local loopback data-lifecycle drill for the offline helper path. It starts a disposable three-node `kvnode` cluster, writes a pre-checkpoint canary, stops node 2 before opening its Pebble directory, runs `kvcheckpoint checkpoint`, `kvcheckpoint verify`, `kvcheckpoint restore`, restarts node 2, writes a post-restore canary, stops node 2 again, runs `kvcheckpoint repair`, restarts node 2, and verifies both canaries from all three client listeners.

Local data lifecycle command:

```sh
KVNODE_GO_RUNNER_RUN=yes KVNODE_GO_RUNNER_MODE=data \
  go run -tags kvnode_local_runner ./tests/kvnode_local_runner.go --mode data \
  2>&1 | tee "${EVIDENCE_DIR}/go-runner-data-lifecycle-local.txt"
```

This is local loopback evidence only. The generated `data-lifecycle-summary.txt`, `checkpoint.log`, `verify.log`, `restore.log`, `repair.log`, and final `summary.txt` should be retained with the transcript. The summary includes `status=local-go-runner-only`, `data_lifecycle=offline-checkpoint-verify-restore-repair`, and `release_claim=none-target-environment-data-lifecycle-drill-still-required`; it does not replace a reviewed target-environment backup/restore/disaster-recovery drill.

Evidence to retain:

- `metadata.env`, `data-lifecycle-summary.txt`, `summary.txt`, and the four helper logs from the script evidence directory.
- The full runner transcript, including the preserved `run_dir`.
- Confirmation that the drill used the disposable runner data directory only and stopped the selected node before each offline `kvcheckpoint` operation.

## Local incident tabletop drill

The repository includes a local loopback tabletop harness for the test-fault branches of the storage-failure and network-partition procedures. It starts a disposable three-node `kvnode` cluster, captures admin evidence files, injects and clears one storage fault plus bidirectional transport drops around one node, and verifies post-clear client canaries on all nodes.

Local tabletop command:

```sh
KVNODE_INCIDENT_TABLETOP_RUN=yes bash tests/kvnode_incident_tabletop_drill.sh \
  2>&1 | tee "${EVIDENCE_DIR}/incident-tabletop-local.txt"
```

This is local tabletop evidence only. It does not replace operator review, target-environment execution, real host/network fault handling, credential rotation, or disaster-recovery sign-off.

Evidence to retain:

- `metadata.env`, `summary.txt`, and per-node `/livez`, `/readyz`, `/metrics`, `/faults/storage`, and `/faults/transport` captures from the script evidence directory.
- The full tabletop transcript, including `storage_fault=exercised-and-cleared`, `transport_fault=exercised-and-cleared`, and `status=local-tabletop-only`.
- Confirmation that no storage directories were wiped or restored during the network-partition branch.

## Incident response: storage failure

Signals:

- `/readyz` returns HTTP 503 with `storage fault active`.
- `/metrics` includes `kvnode_storage_fault_active 1`.
- Client write or transaction requests return unavailable while `/livez` still returns `ok`.
- Process logs show Pebble open, write, fsync, or checksum errors.

Actions:

```sh
export INCIDENT="${INCIDENT_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)-storage-node-${NODE_ID}"
mkdir -p "${INCIDENT}"
curl -sS "http://${ADMIN_ADDR}/livez" -o "${INCIDENT}/livez.txt" || true
curl -sS "http://${ADMIN_ADDR}/readyz" -o "${INCIDENT}/readyz.txt" || true
curl -sS "http://${ADMIN_ADDR}/metrics" -o "${INCIDENT}/metrics.txt" || true
curl -sS "http://${ADMIN_ADDR}/faults/storage" -o "${INCIDENT}/faults-storage.json" || true
journalctl -u "${SERVICE}" --since "1 hour ago" --no-pager > "${INCIDENT}/journal-last-hour.txt" 2>&1 || true
```

If this is a known test fault injection, clear it and verify readiness:

```sh
curl -fsS -X DELETE "http://${ADMIN_ADDR}/faults/storage"
curl -fsS "http://${ADMIN_ADDR}/readyz"
```

If this is not a test fault:

1. Stop the affected node to prevent further writes to suspect local storage.
2. Preserve a read-only quarantine copy of `DATA_DIR` and checksums.
3. Restore from a verified checkpoint only if the incident commander accepts the checkpoint data-loss window.
4. Restart one node at a time and verify `/readyz`, `/metrics`, and client reads after each restart.

Rollback notes:

- If clearing a test storage fault does not restore readiness, treat it as a real storage incident.
- If the restored node fails readiness, stop it again; do not repeatedly start against a suspect directory.

## Incident response: network partition

Signals:

- Client operations time out or return unavailable while local `/livez` succeeds.
- `/metrics` shows rising `kvnode_send_queue_depth` or nonzero `kvnode_transport_dropped_links`.
- `/faults/transport` shows dropped links during a test run.
- Host-level network checks show loss between peer listeners.

Actions:

```sh
export INCIDENT="${INCIDENT_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)-network-node-${NODE_ID}"
mkdir -p "${INCIDENT}"
curl -sS "http://${ADMIN_ADDR}/metrics" -o "${INCIDENT}/metrics.txt" || true
curl -sS "http://${ADMIN_ADDR}/faults/transport" -o "${INCIDENT}/faults-transport.json" || true
for peer in 127.0.0.1:8081 127.0.0.1:9081 127.0.0.1:10081; do
  nc -vz "${peer%:*}" "${peer##*:}" >> "${INCIDENT}/peer-connectivity.txt" 2>&1 || true
done
```

If this is a known test transport fault, clear all dropped links:

```sh
curl -fsS -X DELETE "http://${ADMIN_ADDR}/faults/transport"
curl -fsS "http://${ADMIN_ADDR}/metrics"
```

If this is a real partition:

1. Do not wipe or restore storage as a network fix.
2. Keep clients routed to the side with quorum when the deployment layer can do so safely.
3. Repair routing, firewall, DNS, or load balancer state before restarting processes.
4. Restart at most one node at a time if process restart is needed after network repair.
5. After heal, confirm send queue depth stabilizes and committed/executed metrics continue to advance under a small test write.

Rollback notes:

- Reapply the previous network policy only if the new policy made quorum connectivity worse.
- If a test transport fault was cleared but metrics still show dropped links, capture `/faults/transport` and logs before further changes.

Evidence to retain:

- Peer address map from the `-peers` flag.
- Connectivity checks between every peer listener.
- `/metrics` before and after heal.
- Any firewall, route, DNS, or load-balancer changes.

## Incident response: peer compromise

Signals:

- A node certificate, key, host, or data directory may be controlled by an unauthorized actor.
- Peer traffic originates from an unexpected host or identity.
- The compromised node may have served or accepted client/admin requests.

Actions:

```sh
export INCIDENT="${INCIDENT_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)-compromise-node-${NODE_ID}"
mkdir -p "${INCIDENT}"
journalctl -u "${SERVICE}" --since "24 hours ago" --no-pager > "${INCIDENT}/journal-last-24h.txt" 2>&1 || true
curl -sS "http://${ADMIN_ADDR}/metrics" -o "${INCIDENT}/metrics.txt" || true
sudo systemctl stop "${SERVICE}" || true
rsync -a --numeric-ids "${DATA_DIR}/" "${INCIDENT}/quarantine-data/" || true
chmod -R a-w "${INCIDENT}/quarantine-data" || true
```

Containment:

1. Remove the suspect node from client and admin access paths immediately.
2. Block peer traffic from the suspect host at the network layer, or stop the service if you control the host.
3. Rotate TLS material for remaining trusted nodes when `-tls-cert`, `-tls-key`, and `-tls-ca` are used. Do not reuse the suspect node's key or checkpoint.
4. Continue serving only if the remaining trusted voters still form quorum. If quorum depends on the suspect peer, stop writes and escalate.
5. Restore the compromised node only from a checkpoint taken before compromise and only after host rebuild and credential rotation.

Rollback notes:

- Do not reintroduce the peer merely to regain quorum unless incident command accepts the integrity risk.
- If a credential rotation breaks peer communication, roll back to the last known-good trusted certificate bundle only if it does not include compromised material.

Evidence to retain:

- Suspect credential fingerprints, host identifiers, and access logs.
- Peer and admin network flows around the compromise window.
- Checkpoint chosen for rebuild and proof it predates compromise.
- Explicit decision record for any degraded-quorum or write-stop period.

## Incident response: replay or checksum suspicion

Signals:

- Checksum mismatch on restart or load.
- Unexpected duplicate application, stale value, or replay-like symptom from clients.
- Divergence between checkpoint manifests, data-directory checksums, or expected record histories.

Actions:

1. Freeze writes at the client routing layer if client-visible correctness is in doubt.
2. Stop the suspect node and preserve the data directory as read-only evidence.
3. Capture client operation IDs, timestamps, and response bodies for the suspected replay window.
4. Compare checkpoint manifests and data-directory checksums; do not modify the live data to make checksums pass.
5. Restore from a verified checkpoint only after choosing the acceptable data-loss window.

Commands:

```sh
export INCIDENT="${INCIDENT_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)-replay-suspicion-node-${NODE_ID}"
mkdir -p "${INCIDENT}"
sudo systemctl stop "${SERVICE}" || true
journalctl -u "${SERVICE}" --since "6 hours ago" --no-pager > "${INCIDENT}/journal-last-6h.txt" 2>&1 || true
find "${DATA_DIR}" -type f -print0 | sort -z | xargs -0 shasum -a 256 > "${INCIDENT}/suspect-data.sha256" 2>/dev/null || true
rsync -a --numeric-ids "${DATA_DIR}/" "${INCIDENT}/suspect-data/" || true
chmod -R a-w "${INCIDENT}/suspect-data" || true
```

Rollback notes:

- If client routing was frozen and the suspicion is disproved, unfreeze clients before restarting additional nodes.
- If suspicion is confirmed, keep the suspect node out of quorum and follow the offline restore path, or the tested in-process live-source checkpoint replacement path if the environment has an approved wrapper for it.

Evidence to retain:

- Client request/response samples with timestamps.
- Node logs and checksums from the suspect window.
- The checkpoint or quarantine copy selected for recovery.
- The reason in-place repair, checksum recomputation, corrupt-record deletion, and synthesized reconstruction without a verified checkpoint were not attempted.

## Incident response: recovery stalls

Signals:

- Node is live but never becomes ready after restore or restart.
- `kvnode_send_queue_depth` remains high.
- `kvnode_epaxos_instances` grows while `kvnode_epaxos_executed` does not advance under load.
- Peers show asymmetric connectivity or repeated unavailable responses.

Actions:

```sh
export INCIDENT="${INCIDENT_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)-recovery-stall-node-${NODE_ID}"
mkdir -p "${INCIDENT}"
for i in 1 2 3 4 5; do
  date -u +%Y-%m-%dT%H:%M:%SZ >> "${INCIDENT}/metrics-samples.txt"
  curl -sS "http://${ADMIN_ADDR}/metrics" >> "${INCIDENT}/metrics-samples.txt" || true
  sleep 5
done
curl -sS "http://${ADMIN_ADDR}/readyz" -o "${INCIDENT}/readyz.txt" || true
curl -sS "http://${ADMIN_ADDR}/faults/transport" -o "${INCIDENT}/faults-transport.json" || true
journalctl -u "${SERVICE}" --since "1 hour ago" --no-pager > "${INCIDENT}/journal-last-hour.txt" 2>&1 || true
```

Triage:

1. Confirm peer listener connectivity and that every node uses the same `-peers` map.
2. Confirm admin readiness is not blocked by a storage fault.
3. Confirm only one node is being restored or restarted at a time.
4. If the stall follows checkpoint restore, stop the restored node and retry from a newer verified checkpoint if available.
5. If the stall affects a majority, stop write traffic and escalate; this runbook does not claim reconstruction from an unavailable quorum.

Rollback notes:

- Roll back the last operational change first: network policy, credential bundle, restored checkpoint, then process version.
- Do not roll back by mixing unverified data directories across nodes.

Evidence to retain:

- Five or more timestamped metrics samples.
- Peer connectivity matrix.
- The `-peers` string and listener addresses from each node.
- Restore checkpoint path or restart reason if applicable.

## Post-incident closeout checklist

Complete these before declaring the runbook exercise or incident closed:

- [ ] Evidence bundle path is recorded and copied off-host.
- [ ] Affected node IDs, data directories, checkpoint IDs, and operator commands are documented.
- [ ] `/livez`, `/readyz`, and `/metrics` are captured after recovery.
- [ ] Client-facing read/write smoke checks are captured after recovery, if writes were allowed.
- [ ] Any data-loss window from checkpoint restore is written down in UTC.
- [ ] Quarantined corrupt or compromised data remains read-only.
- [ ] No one performed automatic in-place repair, checksum recomputation, corrupt-record deletion, or synthesized reconstruction without a verified checkpoint under this runbook.
- [ ] If optional external destructive-storage validation was run, its approval, target list, command line, and output are attached separately from this simulation-scoped release evidence.
- [ ] Open gaps are carried forward: no in-place live-file repair claim, no synthesized reconstruction without a verified checkpoint claim, and no production-readiness claim until real operational drills are exercised and reviewed.
