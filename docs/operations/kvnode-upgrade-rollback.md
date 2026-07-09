# kvnode rolling upgrade and rollback plan

Status: operator plan only. This artifact is not measured production evidence, does not assert production readiness, and does not change the release decision. Use it only after the exact binary, flags, data paths, TLS material, peer map, and checkpoint/restore procedure have been exercised in a non-production environment.

## Scope

This plan covers one-node-at-a-time replacement of the `kvnode` example service built from `examples/kv` with:

```sh
go build -tags kvnode ./cmd/kvnode
```

The runtime contract to preserve across the upgrade is the full flag set used by the existing node, including `-id`, `-listen`, `-peer-listen`, `-admin-listen`, `-data`, `-peers`, `-request-deadline-ms`, `-peer-deadline-ms`, `-tls-cert`, `-tls-key`, `-tls-ca`, `-max-client-body-bytes`, `-max-peer-body-bytes`, `-max-admin-body-bytes`, and `-max-scan-limit`. Do not change voters, peer URLs, data directories, TLS trust roots, body limits, or scan limits during the binary replacement unless a separately reviewed migration plan explicitly covers that change.

## Preconditions

- Pick a maintenance window and freeze the deployment inputs: old binary checksum, new binary checksum, launch flags/environment, node IDs, client/peer/admin listen addresses, data directories, and peer map.
- Confirm every node is healthy before touching the first node:
  - `GET /livez` and `GET /readyz` on every admin listener return success.
  - `GET /metrics` on every admin listener shows `kvnode_storage_fault_active 0`, expected `kvnode_transport_dropped_links`, and a send queue depth that is stable or draining.
  - A small canary write/read/scan succeeds through the client listener selected for the rollout.
- Confirm a checkpoint/restore path has already been tested for this exact data layout. The example KV package contains Pebble checkpoint and restore support; `kvnode` itself does not expose an admin checkpoint endpoint, so the operator must use a tested maintenance helper or take a closed-data-dir backup after stopping the node. Do not take a raw live filesystem copy of an open Pebble directory as the checkpoint.
- Keep the previous binary and the pre-upgrade checkpoint for every node until the rollback window expires.

## Per-node rolling replacement

Upgrade exactly one node at a time. Do not start the next node until the current node has completed the post-checks and the observation window.

1. **Select and drain client traffic.** Remove only the selected node's client listener from external client routing. Leave peer addresses unchanged until the node is intentionally stopped.
2. **Record node state.** Capture the node ID, binary checksum, full launch command or unit environment, `-data` path, peer map, admin URL, current `/readyz`, current `/metrics`, disk usage for the data path, and process RSS if available.
3. **Checkpoint before upgrade.** Create the node checkpoint before installing the new binary:
   - Preferred path: run the tested maintenance checkpoint mechanism that calls the KV checkpoint logic and writes to a timestamped checkpoint directory outside `-data`.
   - If no live checkpoint mechanism is available, stop the selected `kvnode`, wait for the process to exit, then copy/archive the closed data directory to a timestamped checkpoint path before changing the binary.
   - Record the checkpoint path, byte size, ownership/mode metadata, and checksum manifest. Treat a failed checkpoint as a hard stop; do not upgrade that node.
4. **Stop only the selected node.** If it was not already stopped for the checkpoint, stop it cleanly now. Do not kill other nodes and do not alter their `-peers` flag.
5. **Install the replacement binary atomically.** Stage the new binary next to the old one, verify its checksum, preserve the old binary path or rollback symlink, then switch the service to the new binary without changing the data directory or flags.
6. **Restart the selected node.** Start it with the same `-id`, `-listen`, `-peer-listen`, `-admin-listen`, `-data`, `-peers`, deadlines, TLS files, body limits, and scan limit used before the stop.
7. **Post-check the selected node.** Require all of the following before restoring client routing:
   - `GET /livez` succeeds.
   - `GET /readyz` succeeds.
   - `GET /metrics` shows `kvnode_storage_fault_active 0` and the send queue depth is stable or draining.
   - A canary `PUT`, `GET`, and bounded `GET /scan?...&limit=<small>` succeeds against the upgraded node's client listener.
8. **Post-check the cluster.** On all nodes, confirm admin health, queue depth, and transport-fault metrics are still within the predeclared rollout bounds. Confirm an existing key written before the upgrade is still readable.
9. **Observe before continuing.** Hold for the predeclared observation window. If the node remains healthy, return its client listener to routing and repeat the same procedure for the next node.

## Rollback criteria

Rollback is mandatory when any of these conditions occur and cannot be explained by an unrelated, already-understood dependency outage:

- The upgraded node fails to start, fails `/readyz`, or reports `kvnode_storage_fault_active 1`.
- Canary write/read/scan fails, returns inconsistent data, or exceeds the predeclared deadline budget.
- Peer communication degrades: send queue depth grows without draining, transport drops appear unexpectedly, or other nodes start failing readiness after the selected node restarts.
- Client-facing error rate, timeout rate, or latency exceeds the predeclared rollout threshold.
- Disk growth, memory RSS, or file-descriptor use exceeds the capacity headroom reserved for the rollout.
- Logs show persistent decode, storage, TLS, or peer-message errors after restart.
- The checkpoint cannot be verified or the old binary is not immediately available.

## Rollback procedure

Rollback one node at a time, using the latest node that was changed first.

1. Remove the affected node's client listener from routing.
2. Stop only the affected `kvnode` and wait for process exit.
3. Preserve the failed-upgrade data directory for later investigation by moving or archiving it; never overwrite it in place.
4. Restore the pre-upgrade binary or rollback symlink and verify its checksum.
5. Restore the pre-upgrade data checkpoint only while the node is stopped. Use the tested restore path; do not splice individual Pebble files into a live data directory.
6. Start the node with the exact pre-upgrade flags and TLS files.
7. Run the same node and cluster post-checks used after upgrade: `/livez`, `/readyz`, `/metrics`, canary write/read/scan, old-key read, disk/RSS capture, and log review.
8. Keep the node out of client routing until the observation window passes. If rollback does not restore health, stop the rollout and escalate with the checkpoint path, failed-upgrade data archive, logs, metrics snapshots, and binary checksums.

## Completion record

For each node, archive the following as rollout evidence only after the run is complete: old/new binary checksums, full launch flags, checkpoint manifest, health and metrics snapshots before/after, canary request log, disk/RSS observations, rollback decision, and operator sign-off. These records are operational evidence for that run; this plan by itself is not production evidence.
