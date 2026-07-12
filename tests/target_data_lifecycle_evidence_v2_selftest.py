#!/usr/bin/env python3
"""Positive and adversarial self-tests for lifecycle evidence v2."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
VERIFY = ROOT / "tests/target_data_lifecycle_evidence_v2_verify.py"
SCHEMA = ROOT / "release/evidence/schema/target-data-lifecycle-evidence-v2.schema.json"
TARGET_ID = "mc-kv-darwin24-arm64-launchd-3n-r1"
PROFILE = "native-darwin24-arm64-launchd-system-domain-v1"
REVISION = "0123456789abcdef0123456789abcdef01234567"
BINARY_SHA = "13579bdf2468ace013579bdf2468ace013579bdf2468ace013579bdf2468ace0"
CLUSTER_ID = "mc-kv-darwin24-3n-r1"
RELEASE_ID = "mc-kv-0123456789ab-r1"
SOURCE_NODE = "node2"
SOURCE_IDENTITY = f"{TARGET_ID}/{CLUSTER_ID}/{SOURCE_NODE}"
CLIENT_TLS_CA_SHA = "21" * 32
CLIENT_TLS_CERT_SHA = "32" * 32
ADMIN_TLS_CA_SHA = "43" * 32
ADMIN_TLS_CERT_SHA = "54" * 32
PEER_TLS_CA_SHA = "65" * 32
PEER_TLS_CERT_SHAS = ["76" * 32, "87" * 32, "98" * 32]
PEER_TLS_IDENTITIES = [
    {
        "replica_id": index,
        "cert_sha256": PEER_TLS_CERT_SHAS[index - 1],
        "uri_san": f"spiffe://gosuda.org/moreconsensus/replica/{index}",
    }
    for index in range(1, 4)
]
TLS_IDENTITY_CANONICAL = (
    f"client-ca={CLIENT_TLS_CA_SHA}\n"
    f"client-cert={CLIENT_TLS_CERT_SHA}\n"
    f"admin-ca={ADMIN_TLS_CA_SHA}\n"
    f"admin-cert={ADMIN_TLS_CERT_SHA}\n"
    f"peer-ca={PEER_TLS_CA_SHA}\n"
)
for peer in PEER_TLS_IDENTITIES:
    TLS_IDENTITY_CANONICAL += (
        f"peer-{peer['replica_id']}-cert={peer['cert_sha256']}\n"
        f"peer-{peer['replica_id']}-uri={peer['uri_san']}\n"
    )
TLS_IDENTITY_SHA = hashlib.sha256(TLS_IDENTITY_CANONICAL.encode("ascii")).hexdigest()


class SelfTestFailure(Exception):
    pass


def stamp(base: datetime, seconds: int) -> str:
    return (base + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_fixture(directory: Path) -> Path:
    raw_dir = directory / "raw"
    raw_dir.mkdir(parents=True)
    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=2)
    artifact_ids = [
        "evidence-mount",
        "release-provenance",
        "client-mtls-required",
        "admin-mtls-required",
        "peer-1-authorization-required",
        "peer-2-authorization-required",
        "peer-3-authorization-required",
        "pre-drill",
        "checkpoint-manifest",
        "configuration-history",
        "checkpoint-snapshot",
        "checkpoint-metadata-report",
        "backup-copy",
        "disaster",
        "quarantine-live-data",
        "staged-verification",
        "restore-report",
        "reject-corrupt-transcript",
        "reject-corrupt-quarantine",
        "reject-truncated-transcript",
        "reject-truncated-quarantine",
        "reject-metadata-transcript",
        "reject-metadata-quarantine",
        "reject-cross-cluster-transcript",
        "reject-cross-cluster-quarantine",
        "legacy-verification",
        "probe-node1",
        "probe-node2",
        "probe-node3",
        "convergence-observation",
        "post-rejoin-checkpoint",
        "integrity",
        "objectives",
        "command-checkpoint",
        "command-verify-current",
        "command-copy-backup",
        "command-verify-backup",
        "command-stage-restore",
        "command-verify-stage",
        "command-atomic-publish",
        "command-restart",
        "command-post-restore-probes",
        "operator-signoff",
        "reviewer-signoff",
    ]
    raw_artifacts: list[dict[str, Any]] = []
    artifact_digests: dict[str, str] = {}
    for artifact_id in artifact_ids:
        relative = f"raw/{artifact_id}.txt"
        if artifact_id in {"client-mtls-required", "admin-mtls-required"}:
            plane = artifact_id.split("-", 1)[0]
            ca_sha = CLIENT_TLS_CA_SHA if plane == "client" else ADMIN_TLS_CA_SHA
            payload = (
                json.dumps(
                    {
                        "schema": "moreconsensus.lifecycle-mtls-negative-probe.v1",
                        "plane": plane,
                        "target_origin": f"https://127.0.0.1:{19090 if plane == 'client' else 19092}",
                        "tls_version": "1.3",
                        "trusted_ca_sha256": ca_sha,
                        "client_certificate_present": False,
                        "handshake_rejected": True,
                        "authenticated_control_before_and_after": True,
                    },
                    indent=2,
                )
                + "\n"
            ).encode()
        elif artifact_id.startswith("peer-") and artifact_id.endswith("-authorization-required"):
            destination_id = int(artifact_id.split("-")[1])
            sender_id = destination_id % 3 + 1
            payload = (
                json.dumps(
                    {
                        "schema": "moreconsensus.lifecycle-peer-authorization-probe.v1",
                        "destination_replica_id": destination_id,
                        "authenticated_sender_replica_id": sender_id,
                        "authenticated_sender_uri_san": f"spiffe://gosuda.org/moreconsensus/replica/{sender_id}",
                        "trusted_ca_sha256": PEER_TLS_CA_SHA,
                        "tls_version": "1.3",
                        "authenticated_handshake_accepted": True,
                        "no_certificate_handshake_rejected": True,
                        "certificate_message_sender_mismatch_rejected": True,
                    },
                    indent=2,
                )
                + "\n"
            ).encode()
        else:
            payload = (
                "Synthetic lifecycle v2 self-test evidence only.\n"
                f"artifact_id={artifact_id}\n"
                "observation=native-darwin-apfs-checkpoint-contract\n"
                "release_claim=none\n"
            ).encode()
        (directory / relative).write_bytes(payload)
        payload_digest = hashlib.sha256(payload).hexdigest()
        artifact_digests[artifact_id] = payload_digest
        raw_artifacts.append(
            {
                "id": artifact_id,
                "path": relative,
                "sha256": payload_digest,
                "captured_at": stamp(base, 165),
            }
        )

    manifest_identity = "01" * 32
    semantic_digest = "23" * 32
    hard_state_digest = "45" * 32
    destination_state_digest = "67" * 32
    commands = [
        ("checkpoint", "/opt/moreconsensus/bin/kvcheckpoint checkpoint /var/db/moreconsensus/campaign/data/node2 /var/db/moreconsensus/campaign/checkpoints/node2/cp-42"),
        ("verify-current", "/opt/moreconsensus/bin/kvcheckpoint verify /var/db/moreconsensus/campaign/checkpoints/node2/cp-42"),
        ("copy-backup", "/usr/bin/ditto /var/db/moreconsensus/campaign/checkpoints/node2/cp-42 /var/db/moreconsensus/campaign/backups/node2/cp-42"),
        ("verify-backup", "/opt/moreconsensus/bin/kvcheckpoint verify /var/db/moreconsensus/campaign/backups/node2/cp-42"),
        ("stage-restore", "/opt/moreconsensus/bin/kvcheckpoint restore /var/db/moreconsensus/campaign/data/node2 /var/db/moreconsensus/campaign/backups/node2/cp-42"),
        ("verify-stage", "/opt/moreconsensus/bin/kvcheckpoint verify /var/db/moreconsensus/campaign/data/.restore-node2-42"),
        ("atomic-publish", "/usr/bin/rename /var/db/moreconsensus/campaign/data/.restore-node2-42 /var/db/moreconsensus/campaign/data/node2"),
        ("restart", "/bin/launchctl bootstrap system /Library/LaunchDaemons/org.gosuda.moreconsensus.kvnode.2.plist"),
        ("post-restore-probes", "/usr/bin/curl --cacert /var/db/moreconsensus/campaign/tls/ca.pem https://127.0.0.1:19192/readyz"),
    ]
    observed_commands = []
    for index, (step, command) in enumerate(commands):
        observed_commands.append(
            {
                "step": step,
                "command": command,
                "started_at": stamp(base, 15 + index * 10),
                "completed_at": stamp(base, 16 + index * 10),
                "exit_code": 0,
                "result": "pass",
                "artifact_id": f"command-{step}",
            }
        )

    rejection_common = {
        "attempted_target_id": TARGET_ID,
        "attempted_cluster_id": CLUSTER_ID,
        "attempted_source_identity": SOURCE_IDENTITY,
        "attempted_release_checksum": f"sha256:{BINARY_SHA}",
        "command": "/opt/moreconsensus/bin/kvcheckpoint restore /var/db/moreconsensus/campaign/data/rejected /var/db/moreconsensus/campaign/quarantine/suspect",
        "exit_code": 1,
        "result": "rejected-before-publish",
        "destination_before_sha256": destination_state_digest,
        "destination_after_sha256": destination_state_digest,
        "destination_mutated": False,
        "publish_attempted": False,
        "quarantined": True,
    }
    rejections = []
    for short, case in (
        ("corrupt", "corrupt-manifest"),
        ("truncated", "truncated-manifest"),
        ("metadata", "mismatched-metadata"),
        ("cross-cluster", "cross-cluster"),
    ):
        item = dict(rejection_common)
        item.update(
            {
                "case": case,
                "transcript_artifact_id": f"reject-{short}-transcript",
                "quarantine_artifact_id": f"reject-{short}-quarantine",
            }
        )
        if case == "mismatched-metadata":
            item["attempted_release_checksum"] = "sha256:" + "89" * 32
        if case == "cross-cluster":
            item["attempted_target_id"] = "mc-kv-darwin24-arm64-launchd-3n-r2"
            item["attempted_cluster_id"] = "mc-kv-darwin24-3n-r2"
            item["attempted_source_identity"] = "mc-kv-darwin24-arm64-launchd-3n-r2/mc-kv-darwin24-3n-r2/node2"
        rejections.append(item)

    report: dict[str, Any] = {
        "schema_version": "2.0.0",
        "verifier_version": "2.0.0",
        "evidence_class": "synthetic-test-only",
        "release_claim": "none",
        "synthetic_test_fixture": True,
        "generated_at": stamp(base, 170),
        "valid_until": stamp(base, 170 + 7 * 24 * 60 * 60),
        "evidence_root": {
            "filesystem": "apfs",
            "external": True,
            "mount_read_only": True,
            "mount_artifact_id": "evidence-mount",
        },
        "scope": {
            "same_host": True,
            "loopback_only": True,
            "tls_mode": "mutual-auth-separated-planes",
            "mtls": True,
            "client_authorization": True,
            "peer_authorization": True,
            "multi_tenant_rbac": False,
            "client_tls_ca_sha256": CLIENT_TLS_CA_SHA,
            "client_tls_cert_sha256": CLIENT_TLS_CERT_SHA,
            "admin_tls_ca_sha256": ADMIN_TLS_CA_SHA,
            "admin_tls_cert_sha256": ADMIN_TLS_CERT_SHA,
            "peer_tls_ca_sha256": PEER_TLS_CA_SHA,
            "peer_tls_identities": PEER_TLS_IDENTITIES,
            "tls_identity_sha256": TLS_IDENTITY_SHA,
            "client_mtls_rejection_artifact_id": "client-mtls-required",
            "admin_mtls_rejection_artifact_id": "admin-mtls-required",
            "peer_authorization_artifact_ids": [
                "peer-1-authorization-required",
                "peer-2-authorization-required",
                "peer-3-authorization-required",
            ],
        },
        "target": {
            "target_id": TARGET_ID,
            "environment_profile": PROFILE,
            "platform": "darwin",
            "architecture": "arm64",
            "execution_mode": "native",
            "supervisor": "launchd",
            "supervisor_domain": "system",
            "filesystem": "apfs",
            "cluster_id": CLUSTER_ID,
            "release_id": RELEASE_ID,
            "source_revision": REVISION,
            "binary_sha256": BINARY_SHA,
            "provenance_artifact_id": "release-provenance",
        },
        "drill": {
            "drill_id": "lifecycle-darwin-r1-0042",
            "executor_identity": "Alex Recovery",
            "started_at": stamp(base, 0),
            "completed_at": stamp(base, 140),
        },
        "pre_drill": {
            "checked_at": stamp(base, 10),
            "nodes_observed": 3,
            "healthy_voters": 3,
            "result": "pass",
            "artifact_id": "pre-drill",
        },
        "checkpoint": {
            "checkpoint_id": "checkpoint-node2-0042",
            "created_at": stamp(base, 20),
            "source_node": SOURCE_NODE,
            "source_store_identity": SOURCE_IDENTITY,
            "checkpoint_format": "manifest-v1",
            "manifest_version": 1,
            "manifest_file_sha256": artifact_digests["checkpoint-manifest"],
            "manifest_artifact_id": "checkpoint-manifest",
            "manifest_identity_blake3": manifest_identity,
            "semantic_state_digest_blake3": semantic_digest,
            "hard_state": {
                "codec_version": 1,
                "digest_blake3": hard_state_digest,
                "tick": 240,
                "current_configuration_generation": 1,
                "current_voters": [1, 2, 3],
            },
            "configuration_history": [{"generation": 1, "voters": [1, 2, 3]}],
            "configuration_history_sha256": artifact_digests["configuration-history"],
            "configuration_history_artifact_id": "configuration-history",
            "record_count": 120,
            "applied_count": 100,
            "snapshot_sha256": artifact_digests["checkpoint-snapshot"],
            "snapshot_artifact_id": "checkpoint-snapshot",
            "metadata_report_sha256": artifact_digests["checkpoint-metadata-report"],
            "metadata_report_artifact_id": "checkpoint-metadata-report",
            "release_checksum": f"sha256:{BINARY_SHA}",
            "current_target_claim": True,
        },
        "backup": {
            "created_at": stamp(base, 30),
            "source_path": "/var/db/moreconsensus/campaign/checkpoints/node2/cp-42",
            "backup_path": "/var/db/moreconsensus/campaign/backups/node2/cp-42",
            "source_filesystem": "apfs",
            "backup_filesystem": "apfs",
            "off_directory_copy": True,
            "source_directory_id": "apfs-file-id-4201",
            "backup_directory_id": "apfs-file-id-6209",
            "same_file": False,
            "independent_file_copies_verified": True,
            "source_snapshot_sha256": artifact_digests["checkpoint-snapshot"],
            "backup_snapshot_sha256": artifact_digests["checkpoint-snapshot"],
            "copy_result": "pass",
            "artifact_id": "backup-copy",
        },
        "disaster": {
            "scenario": "stopped-node-data-directory-loss",
            "occurred_at": stamp(base, 40),
            "affected_node": SOURCE_NODE,
            "result": "pass",
            "artifact_id": "disaster",
        },
        "recovery": {
            "node_stopped_at": stamp(base, 50),
            "recovery_started_at": stamp(base, 60),
            "operation": "restore",
            "stopped_node": SOURCE_NODE,
            "stopped_node_confirmed": True,
            "restore_target_id": TARGET_ID,
            "restore_cluster_id": CLUSTER_ID,
            "restore_node": SOURCE_NODE,
            "restore_store_identity": SOURCE_IDENTITY,
            "destination_path": "/var/db/moreconsensus/campaign/data/node2",
            "destination_initially_absent": True,
            "stage_path": "/var/db/moreconsensus/campaign/data/.restore-node2-42",
            "stage_initially_empty": True,
            "quarantine": {
                "path": "/var/db/moreconsensus/campaign/data/node2.quarantine-42",
                "created_at": stamp(base, 70),
                "read_only": True,
                "result": "pass",
                "artifact_id": "quarantine-live-data",
            },
            "staged_verification": {
                "verified_at": stamp(base, 80),
                "manifest_identity_blake3": manifest_identity,
                "semantic_state_digest_blake3": semantic_digest,
                "hard_state_digest_blake3": hard_state_digest,
                "hard_state_tick": 240,
                "configuration_generation": 1,
                "configuration_history_sha256": artifact_digests["configuration-history"],
                "record_count": 120,
                "applied_count": 100,
                "snapshot_sha256": artifact_digests["checkpoint-snapshot"],
                "checksum_result": "pass",
                "configuration_result": "pass",
                "ownership_result": "pass",
                "artifact_id": "staged-verification",
            },
            "atomic_publish_result": "pass",
            "published_at": stamp(base, 90),
            "restore_report_sha256": artifact_digests["restore-report"],
            "restore_report_artifact_id": "restore-report",
        },
        "pre_publish_rejections": rejections,
        "legacy_verification": {
            "command": "/opt/moreconsensus/bin/kvcheckpoint verify --legacy /var/db/moreconsensus/campaign/quarantine/legacy-cp",
            "exit_code": 0,
            "checkpoint_format": "legacy",
            "current_target_claim": False,
            "release_claim": "none",
            "publish_attempted": False,
            "result": "verified-legacy-non-current",
            "artifact_id": "legacy-verification",
        },
        "post_restore": {
            "service_restored_at": stamp(base, 100),
            "node_probes": [
                {
                    "node": node,
                    "health": "pass",
                    "readiness": "pass",
                    "pre_checkpoint_canary": "pass",
                    "post_checkpoint_canary": "pass",
                    "bounded_scan": "pass",
                    "result": "pass",
                    "artifact_id": f"probe-{node}",
                }
                for node in ("node1", "node2", "node3")
            ],
            "convergence": {
                "measurement_kind": "bounded-observable-convergence",
                "observed_at": stamp(base, 120),
                "observation_window_seconds": 30,
                "restored_node_served_post_checkpoint_value": True,
                "all_nodes_served_pre_checkpoint_value": True,
                "all_nodes_served_post_checkpoint_value": True,
                "send_queues_stable": True,
                "max_observed_send_queue_depth": 0,
                "post_rejoin_checkpoint_verified": True,
                "exact_zero_lag_claimed": False,
                "result": "pass",
                "artifact_id": "convergence-observation",
            },
            "post_rejoin_checkpoint_artifact_id": "post-rejoin-checkpoint",
        },
        "integrity": {
            "checked_at": stamp(base, 130),
            "restored_manifest_identity_blake3": manifest_identity,
            "restored_hard_state_digest_blake3": hard_state_digest,
            "restored_configuration_generation": 1,
            "restored_configuration_history_sha256": artifact_digests["configuration-history"],
            "restored_record_count": 120,
            "restored_applied_count": 100,
            "post_rejoin_record_count": 128,
            "post_rejoin_applied_count": 108,
            "duplicate_applications": 0,
            "data_loss_records": 0,
            "result": "pass",
            "artifact_id": "integrity",
        },
        "objectives": {
            "rpo": {
                "checkpoint_at": stamp(base, 20),
                "disaster_at": stamp(base, 40),
                "measured_seconds": 20,
                "threshold_seconds": 300,
                "result": "met",
            },
            "rto": {
                "disaster_at": stamp(base, 40),
                "service_restored_at": stamp(base, 100),
                "measured_seconds": 60,
                "threshold_seconds": 300,
                "result": "met",
            },
            "artifact_id": "objectives",
        },
        "observed_commands": observed_commands,
        "sign_off": {
            "operator": {
                "identity": "Morgan Operator",
                "role": "Recovery Operator",
                "authenticated_by": "https://id.example.invalid/people/morgan",
                "signed_at": stamp(base, 150),
                "result": "approved",
                "artifact_id": "operator-signoff",
            },
            "independent_reviewer": {
                "identity": "Riley Reviewer",
                "role": "Independent Recovery Reviewer",
                "authenticated_by": "https://id.example.invalid/people/riley",
                "signed_at": stamp(base, 160),
                "result": "approved",
                "artifact_id": "reviewer-signoff",
            },
        },
        "raw_artifacts": raw_artifacts,
    }
    def replace_artifact(artifact_id: str, payload: bytes) -> str:
        artifact = next(item for item in raw_artifacts if item["id"] == artifact_id)
        (directory / artifact["path"]).write_bytes(payload)
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        artifact["sha256"] = payload_sha256
        return payload_sha256

    def helper_report(operation: str, data_dir: str, checkpoint_dir: str) -> bytes:
        fields = {
            "status": "example-operator-report",
            "operation": operation,
            "result": "success",
            "data_dir": data_dir,
            "checkpoint_dir": checkpoint_dir,
            "checkpoint_format": "manifest-v1",
            "manifest_version": "1",
            "manifest_identity": manifest_identity,
            "semantic_state_digest": semantic_digest,
            "record_count": "120",
            "applied_count": "100",
            "hard_state_digest": hard_state_digest,
            "source_identity": SOURCE_IDENTITY,
            "release_checksum": f"sha256:{BINARY_SHA}",
            "current_target_claim": "true",
            "evidence_class": "bounded-data-lifecycle",
        }
        return "".join(f"{key}={json.dumps(value)}\n" for key, value in fields.items()).encode()

    report["checkpoint"]["metadata_report_sha256"] = replace_artifact(
        "checkpoint-metadata-report",
        helper_report(
            "verify",
            "",
            report["backup"]["source_path"],
        ),
    )
    report["recovery"]["restore_report_sha256"] = replace_artifact(
        "restore-report",
        helper_report(
            "restore",
            report["recovery"]["stage_path"],
            report["backup"]["backup_path"],
        ),
    )
    configuration_payload = (
        json.dumps(
            {
                "schema": "moreconsensus.epaxos-configuration-history.v1",
                "hard_state": report["checkpoint"]["hard_state"],
                "configuration_history": report["checkpoint"]["configuration_history"],
                "verification_result": "pass",
            },
            sort_keys=True,
        )
        + "\n"
    ).encode()
    configuration_sha256 = replace_artifact("configuration-history", configuration_payload)
    report["checkpoint"]["configuration_history_sha256"] = configuration_sha256
    report["recovery"]["staged_verification"]["configuration_history_sha256"] = configuration_sha256
    report["integrity"]["restored_configuration_history_sha256"] = configuration_sha256

    for observation in observed_commands:
        command_payload = (
            json.dumps(
                {
                    "schema": "moreconsensus.lifecycle-command-observation.v1",
                    "verifier_version": "2.0.0",
                    "target_id": TARGET_ID,
                    "release_id": RELEASE_ID,
                    "source_revision": REVISION,
                    "binary_sha256": BINARY_SHA,
                    "environment_profile": PROFILE,
                    "step": observation["step"],
                    "command": observation["command"],
                    "started_at": observation["started_at"],
                    "completed_at": observation["completed_at"],
                    "exit_code": observation["exit_code"],
                    "result": observation["result"],
                },
                sort_keys=True,
            )
            + "\n"
        ).encode()
        replace_artifact(observation["artifact_id"], command_payload)

    report_path = directory / "evidence.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    return report_path


def run_verifier(report: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VERIFY), *args, str(report)],
        text=True,
        capture_output=True,
        check=False,
    )


def load_report(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def save_report(path: Path, report: dict[str, Any]) -> None:
    path.write_text(json.dumps(report, indent=2) + "\n")


def descend(document: Any, dotted: str) -> tuple[Any, str]:
    parts = dotted.split(".")
    current = document
    for part in parts[:-1]:
        current = current[int(part)] if isinstance(current, list) else current[part]
    return current, parts[-1]


def set_value(dotted: str, value: Any) -> Callable[[Path], None]:
    def mutate(path: Path) -> None:
        report = load_report(path)
        parent, leaf = descend(report, dotted)
        if isinstance(parent, list):
            parent[int(leaf)] = value
        else:
            parent[leaf] = value
        save_report(path, report)
    return mutate


def delete_value(dotted: str) -> Callable[[Path], None]:
    def mutate(path: Path) -> None:
        report = load_report(path)
        parent, leaf = descend(report, dotted)
        if isinstance(parent, list):
            del parent[int(leaf)]
        else:
            del parent[leaf]
        save_report(path, report)
    return mutate


def rename_digest_label(path: Path) -> None:
    report = load_report(path)
    value = report["checkpoint"].pop("manifest_identity_blake3")
    report["checkpoint"]["manifest_identity_sha256"] = value
    save_report(path, report)


def add_unobservable_lag(path: Path) -> None:
    report = load_report(path)
    report["post_restore"]["convergence"]["lag_entries"] = 0
    save_report(path, report)


def omit_configuration_generation(path: Path) -> None:
    report = load_report(path)
    del report["checkpoint"]["configuration_history"][0]["generation"]
    save_report(path, report)


def invalid_configuration_transition(path: Path) -> None:
    report = load_report(path)
    report["checkpoint"]["configuration_history"].append(
        {
            "generation": 2,
            "base_generation": 1,
            "voters": [1, 2, 3],
            "source_record": "c1-r1-i4",
            "change": "add-voter",
            "voter": 4,
            "outcome": "applied",
        }
    )
    report["checkpoint"]["hard_state"]["current_configuration_generation"] = 2
    save_report(path, report)


def duplicate_command_step(path: Path) -> None:
    report = load_report(path)
    report["observed_commands"][1]["step"] = report["observed_commands"][0]["step"]
    save_report(path, report)


def missing_rejection_case(path: Path) -> None:
    report = load_report(path)
    report["pre_publish_rejections"][3]["case"] = "corrupt-manifest"
    save_report(path, report)


def metadata_not_mismatched(path: Path) -> None:
    report = load_report(path)
    item = report["pre_publish_rejections"][2]
    item["attempted_release_checksum"] = report["checkpoint"]["release_checksum"]
    item["attempted_source_identity"] = report["checkpoint"]["source_store_identity"]
    save_report(path, report)


def cross_cluster_not_cross(path: Path) -> None:
    report = load_report(path)
    item = report["pre_publish_rejections"][3]
    item["attempted_target_id"] = report["target"]["target_id"]
    item["attempted_cluster_id"] = report["target"]["cluster_id"]
    item["attempted_source_identity"] = report["checkpoint"]["source_store_identity"]
    save_report(path, report)


def marker_only_command_artifact(path: Path) -> None:
    report = load_report(path)
    artifact = next(item for item in report["raw_artifacts"] if item["id"] == "command-checkpoint")
    payload = b"status=pass\nresult=success\n"
    artifact_path = path.parent / artifact["path"]
    artifact_path.write_bytes(payload)
    artifact["sha256"] = hashlib.sha256(payload).hexdigest()
    save_report(path, report)


def rewrite_bound_artifact(path: Path, artifact_id: str, payload: bytes, digest_paths: tuple[str, ...] = ()) -> None:
    report = load_report(path)
    artifact = next(item for item in report["raw_artifacts"] if item["id"] == artifact_id)
    (path.parent / artifact["path"]).write_bytes(payload)
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    artifact["sha256"] = payload_sha256
    for dotted in digest_paths:
        parent, leaf = descend(report, dotted)
        parent[leaf] = payload_sha256
    save_report(path, report)


def opaque_command_artifact(path: Path) -> None:
    rewrite_bound_artifact(path, "command-checkpoint", b"checkpoint operation passed with observed output\n")


def corrupt_metadata_report(path: Path) -> None:
    artifact_path = path.parent / "raw/checkpoint-metadata-report.txt"
    payload = artifact_path.read_text().replace('"23' + "23" * 31 + '"', '"ab' + "ab" * 31 + '"').encode()
    rewrite_bound_artifact(
        path,
        "checkpoint-metadata-report",
        payload,
        ("checkpoint.metadata_report_sha256",),
    )


def truncated_metadata_report(path: Path) -> None:
    artifact_path = path.parent / "raw/checkpoint-metadata-report.txt"
    lines = [line for line in artifact_path.read_text().splitlines() if not line.startswith("hard_state_digest=")]
    rewrite_bound_artifact(
        path,
        "checkpoint-metadata-report",
        ("\n".join(lines) + "\n").encode(),
        ("checkpoint.metadata_report_sha256",),
    )


def mismatched_restore_report(path: Path) -> None:
    artifact_path = path.parent / "raw/restore-report.txt"
    payload = artifact_path.read_text().replace(
        json.dumps(SOURCE_IDENTITY),
        json.dumps(f"{TARGET_ID}/mc-kv-darwin24-3n-r2/node2"),
    ).encode()
    rewrite_bound_artifact(
        path,
        "restore-report",
        payload,
        ("recovery.restore_report_sha256",),
    )


def mismatched_configuration_artifact(path: Path) -> None:
    artifact_path = path.parent / "raw/configuration-history.txt"
    document = json.loads(artifact_path.read_text())
    document["hard_state"]["current_voters"] = [1, 2]
    payload = (json.dumps(document, sort_keys=True) + "\n").encode()
    rewrite_bound_artifact(
        path,
        "configuration-history",
        payload,
        (
            "checkpoint.configuration_history_sha256",
            "recovery.staged_verification.configuration_history_sha256",
            "integrity.restored_configuration_history_sha256",
        ),
    )


def mismatched_metadata_artifact_digest(path: Path) -> None:
    report = load_report(path)
    report["checkpoint"]["metadata_report_sha256"] = "ab" * 32
    save_report(path, report)


def malformed_json(path: Path) -> None:
    path.write_text("{\n")


def duplicate_json_key(path: Path) -> None:
    contents = path.read_text()
    needle = '  "schema_version": "2.0.0",\n'
    path.write_text(contents.replace(needle, needle + needle, 1))


def remove_artifact_file(path: Path) -> None:
    (path.parent / "raw/restore-report.txt").unlink()


def symlink_artifact(path: Path) -> None:
    artifact_path = path.parent / "raw/restore-report.txt"
    artifact_path.unlink()
    artifact_path.symlink_to("checkpoint-metadata-report.txt")


def mismatch_artifact_hash(path: Path) -> None:
    report = load_report(path)
    report["raw_artifacts"][0]["sha256"] = "ef" * 32
    save_report(path, report)


def expect_failure(pristine: Path, name: str, mutate: Callable[[Path], None], *args: str) -> None:
    case_dir = pristine.parent / f"case-{name}"
    shutil.copytree(pristine, case_dir)
    report = case_dir / "evidence.json"
    mutate(report)
    result = run_verifier(report, "--self-test-fixture", *args)
    if result.returncode == 0:
        raise SelfTestFailure(f"{name} was accepted: {result.stdout.strip()}")


def main() -> int:
    try:
        schema = json.loads(SCHEMA.read_text())
        if schema["properties"]["schema_version"]["const"] != "2.0.0":
            raise SelfTestFailure("schema version mismatch")
        with tempfile.TemporaryDirectory(prefix="target-data-lifecycle-v2-selftest.") as temporary:
            work = Path(temporary)
            pristine = work / "pristine"
            pristine.mkdir()
            report = make_fixture(pristine)
            positive = run_verifier(
                report,
                "--self-test-fixture",
                "--expected-target-id", TARGET_ID,
                "--expected-release-id", RELEASE_ID,
                "--expected-source-revision", REVISION,
                "--expected-binary-sha256", BINARY_SHA,
                "--expected-environment-profile", PROFILE,
            )
            if positive.returncode != 0:
                raise SelfTestFailure(f"positive fixture rejected: {positive.stderr.strip()}")
            required_output = (
                "status=pass verifier_version=2.0.0 mode=synthetic-test-fixture "
                "release_claim=none"
            )
            if required_output not in positive.stdout:
                raise SelfTestFailure("positive output lacks explicit versioned synthetic non-claim")
            production = run_verifier(report)
            if production.returncode == 0:
                raise SelfTestFailure("synthetic fixture accepted as production evidence")
            mutable_target_dir = work / "target-mutable-root"
            shutil.copytree(pristine, mutable_target_dir)
            mutable_target_report = mutable_target_dir / "evidence.json"
            mutable_document = load_report(mutable_target_report)
            mutable_document["synthetic_test_fixture"] = False
            mutable_document["evidence_class"] = "target-data-lifecycle-darwin-v2"
            mutable_document["release_claim"] = "target-data-lifecycle-criteria-met"
            save_report(mutable_target_report, mutable_document)
            mutable_result = run_verifier(mutable_target_report)
            if mutable_result.returncode == 0 or "mounted read-only" not in mutable_result.stderr:
                raise SelfTestFailure("mutable evidence root did not fail the physical read-only mount check")
            report_link = work / "evidence-link.json"
            report_link.symlink_to(report)
            if run_verifier(report_link, "--self-test-fixture").returncode == 0:
                raise SelfTestFailure("symbolic-link report input was accepted")


            cases: list[tuple[str, Callable[[Path], None], tuple[str, ...]]] = [
                ("wrong-schema-version", set_value("schema_version", "1.0.0"), ()),
                ("wrong-verifier-version", set_value("verifier_version", "2.0.1"), ()),
                ("malformed-json", malformed_json, ()),
                ("duplicate-json-key", duplicate_json_key, ()),
                ("unknown-top-field", set_value("unreviewed", True), ()),
                ("wrong-target", set_value("target.target_id", "mc-kv-darwin24-arm64-launchd-3n-r2"), ()),
                ("wrong-profile", set_value("target.environment_profile", "native-darwin-session"), ()),
                ("wrong-platform", set_value("target.platform", "linux"), ()),
                ("wrong-architecture", set_value("target.architecture", "amd64"), ()),
                ("container-mode", set_value("target.execution_mode", "container"), ()),
                ("wrong-supervisor", set_value("target.supervisor", "systemd"), ()),
                ("non-apfs-target", set_value("target.filesystem", "ext4"), ()),
                ("release-not-source-derived", set_value("target.release_id", "mc-kv-fedcba987654-r1"), ()),
                ("mtls-underclaim", set_value("scope.mtls", False), ()),
                ("tls-identity-mismatch", set_value("scope.client_tls_ca_sha256", "65" * 32), ()),
                ("mutable-root-declaration", set_value("evidence_root.mount_read_only", False), ()),
                ("manifest-format", set_value("checkpoint.checkpoint_format", "legacy"), ()),
                ("manifest-version", set_value("checkpoint.manifest_version", 2), ()),
                ("algorithm-label-mismatch", rename_digest_label, ()),
                ("malformed-blake3", set_value("checkpoint.semantic_state_digest_blake3", "ab12"), ()),
                ("release-checksum-label", set_value("checkpoint.release_checksum", f"blake3:{BINARY_SHA}"), ()),
                ("release-checksum-mismatch", set_value("checkpoint.release_checksum", "sha256:" + "ab" * 32), ()),
                ("non-current-checkpoint", set_value("checkpoint.current_target_claim", False), ()),
                ("source-store-mismatch", set_value("checkpoint.source_store_identity", TARGET_ID + "/other/node2"), ()),
                ("applied-exceeds-records", set_value("checkpoint.applied_count", 121), ()),
                ("omitted-config-generation", omit_configuration_generation, ()),
                ("hardstate-config-generation", set_value("checkpoint.hard_state.current_configuration_generation", 2), ()),
                ("hardstate-voters", set_value("checkpoint.hard_state.current_voters", [1, 2]), ()),
                ("invalid-config-transition", invalid_configuration_transition, ()),
                ("same-backup-path", set_value("backup.backup_path", "/var/db/moreconsensus/campaign/checkpoints/node2/cp-42"), ()),
                ("nested-backup-path", set_value("backup.backup_path", "/var/db/moreconsensus/campaign/checkpoints/node2/cp-42/copy"), ()),
                ("same-directory-identity", set_value("backup.backup_directory_id", "apfs-file-id-4201"), ()),
                ("same-file-backup", set_value("backup.same_file", True), ()),
                ("fake-independent-copy", set_value("backup.independent_file_copies_verified", False), ()),
                ("backup-snapshot-mismatch", set_value("backup.backup_snapshot_sha256", "cd" * 32), ()),
                ("backup-not-apfs", set_value("backup.backup_filesystem", "hfs"), ()),
                ("affected-node-mismatch", set_value("disaster.affected_node", "node3"), ()),
                ("restore-cross-target", set_value("recovery.restore_target_id", "mc-kv-darwin24-arm64-launchd-3n-r2"), ()),
                ("restore-cross-cluster", set_value("recovery.restore_cluster_id", "mc-kv-darwin24-3n-r2"), ()),
                ("restore-cross-store", set_value("recovery.restore_store_identity", TARGET_ID + "/mc-kv-darwin24-3n-r2/node2"), ()),
                ("destination-not-absent", set_value("recovery.destination_initially_absent", False), ()),
                ("stage-not-empty", set_value("recovery.stage_initially_empty", False), ()),
                ("stage-not-sibling", set_value("recovery.stage_path", "/tmp/.restore-node2-42"), ()),
                ("quarantine-writable", set_value("recovery.quarantine.read_only", False), ()),
                ("stale-hardstate", set_value("recovery.staged_verification.hard_state_tick", 239), ()),
                ("hardstate-digest-mismatch", set_value("recovery.staged_verification.hard_state_digest_blake3", "de" * 32), ()),
                ("stage-config-generation", set_value("recovery.staged_verification.configuration_generation", 2), ()),
                ("stage-record-count", set_value("recovery.staged_verification.record_count", 119), ()),
                ("stage-applied-count", set_value("recovery.staged_verification.applied_count", 99), ()),
                ("stage-checksum-failed", set_value("recovery.staged_verification.checksum_result", "fail"), ()),
                ("stage-config-failed", set_value("recovery.staged_verification.configuration_result", "fail"), ()),
                ("stage-ownership-failed", set_value("recovery.staged_verification.ownership_result", "fail"), ()),
                ("publish-failed", set_value("recovery.atomic_publish_result", "fail"), ()),
                ("missing-rejection-case", missing_rejection_case, ()),
                ("rejection-exit-zero", set_value("pre_publish_rejections.0.exit_code", 0), ()),
                ("rejection-mutated-destination", set_value("pre_publish_rejections.0.destination_after_sha256", "aa" * 32), ()),
                ("rejection-mutation-flag", set_value("pre_publish_rejections.1.destination_mutated", True), ()),
                ("rejection-publish-attempt", set_value("pre_publish_rejections.1.publish_attempted", True), ()),
                ("rejection-not-quarantined", set_value("pre_publish_rejections.1.quarantined", False), ()),
                ("metadata-not-mismatched", metadata_not_mismatched, ()),
                ("cross-cluster-not-cross", cross_cluster_not_cross, ()),
                ("legacy-without-explicit-path", set_value("legacy_verification.command", "/opt/moreconsensus/bin/kvcheckpoint verify /tmp/legacy"), ()),
                ("legacy-restore", set_value("legacy_verification.command", "/opt/moreconsensus/bin/kvcheckpoint restore --legacy /tmp/legacy"), ()),
                ("legacy-current-claim", set_value("legacy_verification.current_target_claim", True), ()),
                ("legacy-publish", set_value("legacy_verification.publish_attempted", True), ()),
                ("missing-node-probe", delete_value("post_restore.node_probes.2"), ()),
                ("duplicate-node-probe", set_value("post_restore.node_probes.2.node", "node2"), ()),
                ("application-probe-failed", set_value("post_restore.node_probes.1.post_checkpoint_canary", "fail"), ()),
                ("unobservable-lag", add_unobservable_lag, ()),
                ("invented-zero-lag", set_value("post_restore.convergence.exact_zero_lag_claimed", True), ()),
                ("post-rejoin-not-verified", set_value("post_restore.convergence.post_rejoin_checkpoint_verified", False), ()),
                ("restored-identity-mismatch", set_value("integrity.restored_manifest_identity_blake3", "fe" * 32), ()),
                ("restored-config-generation", set_value("integrity.restored_configuration_generation", 2), ()),
                ("restored-record-count", set_value("integrity.restored_record_count", 119), ()),
                ("restored-applied-count", set_value("integrity.restored_applied_count", 99), ()),
                ("post-rejoin-record-regression", set_value("integrity.post_rejoin_record_count", 119), ()),
                ("post-rejoin-applied-regression", set_value("integrity.post_rejoin_applied_count", 99), ()),
                ("duplicate-application", set_value("integrity.duplicate_applications", 1), ()),
                ("data-loss", set_value("integrity.data_loss_records", 1), ()),
                ("invented-rpo", set_value("objectives.rpo.measured_seconds", 19), ()),
                ("rpo-over-threshold", set_value("objectives.rpo.threshold_seconds", 10), ()),
                ("invented-rto", set_value("objectives.rto.measured_seconds", 59), ()),
                ("rto-over-threshold", set_value("objectives.rto.threshold_seconds", 30), ()),
                ("missing-observed-command", delete_value("observed_commands.8"), ()),
                ("duplicate-command-step", duplicate_command_step, ()),
                ("command-exit-nonzero", set_value("observed_commands.0.exit_code", 1), ()),
                ("legacy-current-verify-command", set_value("observed_commands.1.command", "/opt/moreconsensus/bin/kvcheckpoint verify --legacy /tmp/cp"), ()),
                ("fake-copy-command", set_value("observed_commands.2.command", "/bin/echo copied"), ()),
                ("marker-only-success", marker_only_command_artifact, ()),
                ("opaque-success-artifact", opaque_command_artifact, ()),
                ("corrupt-metadata-report", corrupt_metadata_report, ()),
                ("truncated-metadata-report", truncated_metadata_report, ()),
                ("mismatched-restore-report", mismatched_restore_report, ()),
                ("mismatched-configuration-artifact", mismatched_configuration_artifact, ()),
                ("executor-only-signoff", set_value("sign_off.independent_reviewer.identity", "Alex Recovery"), ()),
                ("reviewer-not-independent", set_value("sign_off.independent_reviewer.identity", "Morgan Operator"), ()),
                ("metadata-artifact-digest-mismatch", mismatched_metadata_artifact_digest, ()),
                ("missing-artifact-file", remove_artifact_file, ()),
                ("artifact-hash-mismatch", mismatch_artifact_hash, ()),
                ("expected-release-mismatch", lambda _path: None, ("--expected-release-id", "wrong-release")),
                ("artifact-path-traversal", set_value("raw_artifacts.0.path", "../evidence-mount.txt"), ()),
                ("artifact-symlink", symlink_artifact, ()),
            ]
            for name, mutate, args in cases:
                expect_failure(pristine, name, mutate, *args)
            case_count = 4 + len(cases)
            print(
                "target-data-lifecycle-evidence-v2-selftest status=pass "
                f"cases={case_count} verifier_version=2.0.0 release_claim=none "
                "destructive_operations=none"
            )
            return 0
    except (OSError, json.JSONDecodeError, SelfTestFailure) as exc:
        print(f"target-data-lifecycle-evidence-v2-selftest status=fail reason={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
