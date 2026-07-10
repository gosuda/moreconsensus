#!/usr/bin/env python3
"""Focused self-test for the target incident evidence verifier."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

TESTS_DIR = Path(__file__).resolve().parent
VERIFIER_PATH = TESTS_DIR / "verify_target_incident_evidence.py"
SCHEMA_PATH = TESTS_DIR / "target_incident_evidence.schema.json"
DARWIN_SCHEMA_PATH = (
    TESTS_DIR.parent / "release" / "evidence" / "schema" / "target-incident-evidence-v2.schema.json"
)
sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("target_incident_verifier", VERIFIER_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("cannot load target incident verifier")
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

SCENARIO_CLASSES = [
    "storage_failure",
    "network_partition",
    "peer_compromise",
    "replay_checksum_suspicion",
    "recovery_stall",
]
DARWIN_INCIDENT_CLASSES = [
    "process_crash_restart",
    "one_node_unavailability",
    "bad_config_rollback",
    "certificate_secret_rotation",
    "storage_pressure_failure",
    "corrupted_checkpoint",
]


def utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_hashed(path: Path, content: str) -> str:
    path.write_text(content, encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_fixture(root: Path, now: datetime) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    binary = root / "synthetic-fixture-kvnode"
    binary_hash = write_hashed(binary, "synthetic fixture binary bytes\n")
    base = now - timedelta(hours=4)
    participants = [
        {
            "participant_id": "operator-1",
            "name": "Synthetic Operator",
            "role": "operator",
            "organization": "Synthetic Operations",
        },
        {
            "participant_id": "reviewer-1",
            "name": "Synthetic Independent Reviewer",
            "role": "independent-reviewer",
            "organization": "Synthetic Assurance",
        },
        {
            "participant_id": "commander-1",
            "name": "Synthetic Incident Commander",
            "role": "incident-commander",
            "organization": "Synthetic Response",
        },
    ]
    artifacts: list[dict[str, Any]] = []
    scenarios: list[dict[str, Any]] = []
    follow_ups: list[dict[str, Any]] = []

    for index, incident_class in enumerate(SCENARIO_CLASSES):
        scenario_id = f"INC-{index + 1:03d}"
        started = base + timedelta(minutes=index * 10)
        detected = started + timedelta(minutes=1)
        triaged = started + timedelta(minutes=2)
        approved = started + timedelta(minutes=3)
        mitigated_start = started + timedelta(minutes=4)
        mitigated_end = started + timedelta(minutes=5)
        safety_checked = started + timedelta(minutes=6)
        recovered = started + timedelta(minutes=7)
        canary_checked = started + timedelta(minutes=8)
        ended = started + timedelta(minutes=9)

        artifact_ids: dict[str, str] = {}
        for kind, suffix, captured in (
            ("raw-log", "log", detected),
            ("raw-metric", "metric", safety_checked),
            ("raw-communication", "communication", approved),
        ):
            artifact_id = f"ART-{index + 1:03d}-{suffix.upper()}"
            artifact_path = root / f"synthetic-fixture-{scenario_id.lower()}-{suffix}.txt"
            artifact_hash = write_hashed(
                artifact_path,
                f"synthetic fixture {incident_class} {kind} evidence\n",
            )
            artifact_ids[kind] = artifact_id
            artifacts.append(
                {
                    "artifact_id": artifact_id,
                    "scenario_id": scenario_id,
                    "kind": kind,
                    "path": str(artifact_path),
                    "sha256": artifact_hash,
                    "captured_at": utc(captured),
                }
            )

        scenarios.append(
            {
                "scenario_id": scenario_id,
                "incident_class": incident_class,
                "participants": [
                    {
                        "participant_id": "operator-1",
                        "incident_role": "mitigation executor",
                    },
                    {
                        "participant_id": "reviewer-1",
                        "incident_role": "live-action approver",
                    },
                    {
                        "participant_id": "commander-1",
                        "incident_role": "triage decision maker",
                    },
                ],
                "started_at": utc(started),
                "ended_at": utc(ended),
                "detection": {
                    "signal": f"Synthetic target alert for {incident_class}",
                    "detected_at": utc(detected),
                    "evidence_artifact_ids": [
                        artifact_ids["raw-log"],
                        artifact_ids["raw-metric"],
                    ],
                },
                "triage": {
                    "decision": "Isolate affected scope and recover service",
                    "rationale": "Synthetic signal and quorum evidence identified a bounded response",
                    "decided_at": utc(triaged),
                    "decision_maker_participant_id": "commander-1",
                },
                "mitigation": {
                    "actions": [
                        {
                            "sequence": 1,
                            "action_type": "traffic-control",
                            "exact_action": f"Apply recorded synthetic isolation plan {index + 1}",
                            "target": f"synthetic-target-peer-{index + 1}",
                            "result": "succeeded",
                            "evidence_artifact_ids": [artifact_ids["raw-log"]],
                        }
                    ],
                    "executor_participant_id": "operator-1",
                    "scope": f"Synthetic bounded scope for {incident_class}",
                    "live_action": True,
                    "started_at": utc(mitigated_start),
                    "completed_at": utc(mitigated_end),
                    "change_reference": f"SYN-CHANGE-{index + 1:03d}",
                    "authorization": {
                        "approval_reference": f"SYN-APPROVAL-{index + 1:03d}",
                        "approver_participant_ids": ["reviewer-1"],
                        "approved_at": utc(approved),
                        "decision": "approved",
                        "abort_criteria": [
                            "Abort if quorum drops below the recorded safe threshold"
                        ],
                    },
                },
                "quorum_data_safety": {
                    "checked_at": utc(safety_checked),
                    "quorum_result": "quorum-maintained",
                    "data_safety_result": "no-data-loss",
                    "method": "Compare committed canary reads and peer progress metrics",
                    "evidence_artifact_ids": [artifact_ids["raw-metric"]],
                },
                "acceptance_results": [
                    {
                        "criterion": "Quorum remains available and committed canary data is unchanged",
                        "result": "pass",
                        "observed_at": utc(canary_checked),
                        "evidence_artifact_ids": [artifact_ids["raw-metric"]],
                    }
                ],
                "communication_escalation": {
                    "notified_at": utc(triaged),
                    "audience": "Synthetic service owner and response channel",
                    "escalation_path": "Synthetic primary on-call to incident commander",
                    "message_summary": f"Synthetic {incident_class} triage and bounded mitigation",
                    "evidence_artifact_ids": [artifact_ids["raw-communication"]],
                },
                "recovery_evidence": {
                    "recovered_at": utc(recovered),
                    "recovery_state": "service-restored",
                    "validation": "Committed canary reads succeeded across the recorded target quorum",
                    "evidence_artifact_ids": [
                        artifact_ids["raw-log"],
                        artifact_ids["raw-metric"],
                    ],
                },
                "post_incident_canaries": {
                    "checked_at": utc(canary_checked),
                    "canaries": [
                        {
                            "name": "Committed read and write continuity",
                            "result": "pass",
                            "evidence_artifact_ids": [artifact_ids["raw-metric"]],
                        }
                    ],
                },
            }
        )
        follow_ups.append(
            {
                "action_id": f"FOLLOW-{index + 1:03d}",
                "scenario_id": scenario_id,
                "description": f"Review synthetic {incident_class} response timings",
                "owner_participant_id": "commander-1",
                "tracking_reference": f"SYN-FOLLOW-{index + 1:03d}",
                "due_at": utc(now + timedelta(days=7)),
                "status": "open",
            }
        )

    latest_end = base + timedelta(minutes=49)
    signed_at = latest_end + timedelta(minutes=11)
    closed_at = latest_end + timedelta(minutes=21)
    recorded_at = closed_at + timedelta(minutes=5)
    return {
        "schema_version": "1.0",
        "record_kind": "target-incident-evidence",
        "record_mode": "synthetic-test",
        "claim": "synthetic-test-incident-response-evidence-complete",
        "target": {
            "name": "synthetic-target-alpha",
            "environment": "synthetic",
            "service": "synthetic-kvnode",
            "cluster_id": "synthetic-cluster-1",
        },
        "participants": participants,
        "release_provenance": {
            "release_id": "synthetic-release-1",
            "source_repository": "https://invalid.example/synthetic/repository",
            "source_revision": "a" * 40,
            "source_tree": "clean",
            "binary_path": str(binary),
            "binary_sha256": binary_hash,
            "built_at": utc(base - timedelta(hours=1)),
        },
        "opened_at": utc(base),
        "closed_at": utc(closed_at),
        "recorded_at": utc(recorded_at),
        "valid_until": utc(now + timedelta(days=7)),
        "scenarios": scenarios,
        "raw_artifacts": artifacts,
        "follow_up_actions": follow_ups,
        "sign_off": {
            "operator": {
                "participant_id": "operator-1",
                "role": "operator",
                "signed_at": utc(signed_at),
                "decision": "approved",
                "statement": "Synthetic operator confirms the fixture record is internally complete",
            },
            "independent_reviewer": {
                "participant_id": "reviewer-1",
                "role": "independent-reviewer",
                "signed_at": utc(signed_at),
                "decision": "approved",
                "statement": "Synthetic reviewer independently confirms the fixture record",
            },
        },
    }


def build_darwin_fixture(root: Path, now: datetime) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    binary_path = root / "binary" / "kvnode"
    binary_path.parent.mkdir(parents=True, exist_ok=True)
    binary_path.write_bytes(
        b"\xcf\xfa\xed\xfe"
        + (0x0100000C).to_bytes(4, "little")
        + b"\x00\x00\x00\x00"
        + b"\x02\x00\x00\x00"
        + b"\x00" * 116
    )
    binary_hash = hashlib.sha256(binary_path.read_bytes()).hexdigest()
    source_revision = "b" * 40
    release_id = "mc-kv-bbbbbbbbbbbb-r1"
    target_id = "mc-kv-darwin24-arm64-launchd-3n-r1"
    environment = "native-darwin24-arm64-launchd-system-domain-v1"
    verifier_version = "target-incident-evidence-verifier/2.0"
    opened_at = now - timedelta(hours=6)
    manifest_relative = Path("manifest") / "release-manifest.json"
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    release_manifest = {
        "manifest_version": "incident-release-manifest-v2",
        "verifier_version": verifier_version,
        "origin": "synthetic-self-test",
        "record_mode": "synthetic-test",
        "target_id": target_id,
        "release_id": release_id,
        "source_revision": source_revision,
        "binary_uri": "file:binary/kvnode",
        "binary_sha256": binary_hash,
        "environment": environment,
        "platform": "darwin",
        "architecture": "arm64",
        "binary_format": "mach-o-64",
        "build_command": (
            "env GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath "
            "-buildvcs=true -tags kvnode -o file:binary/kvnode ./cmd/kvnode"
        ),
        "go_version": "go1.26.5",
        "vcs_modified": False,
        "codesign_requirement": "valid-adhoc-or-identified",
        "created_at": utc(opened_at - timedelta(minutes=30)),
    }
    manifest_path.write_text(
        json.dumps(release_manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    artifacts: list[dict[str, Any]] = []

    def add_artifact(
        artifact_id: str,
        drill_id: str,
        kind: str,
        captured_at: datetime,
        command: str,
        output: str,
        *,
        result: str = "observed-success",
        exit_code: int = 0,
    ) -> str:
        relative = Path("raw") / drill_id.lower() / f"{artifact_id.lower()}.json"
        artifact_path = root / relative
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        envelope = {
            "artifact_version": "incident-raw-v2",
            "verifier_version": verifier_version,
            "target_id": target_id,
            "release_id": release_id,
            "source_revision": source_revision,
            "binary_sha256": binary_hash,
            "environment": environment,
            "record_mode": "synthetic-test",
            "drill_id": drill_id,
            "observed_at": utc(captured_at),
            "command": command,
            "exit_code": exit_code,
            "result": result,
            "output": output,
        }
        artifact_path.write_text(json.dumps(envelope, indent=2) + "\n", encoding="utf-8")
        artifacts.append(
            {
                "artifact_id": artifact_id,
                "drill_id": drill_id,
                "kind": kind,
                "uri": f"file:{relative.as_posix()}",
                "sha256": hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
                "captured_at": utc(captured_at),
            }
        )
        return artifact_id

    topology_artifacts = [
        add_artifact(
            f"CAMPAIGN-TOPOLOGY-NODE{index}",
            "campaign",
            "raw-command-output",
            opened_at + timedelta(minutes=2),
            f"launchctl print system/org.gosuda.moreconsensus.kvnode.{index}",
            (
                f"Observed node{index} pid={42000 + index}, label="
                f"org.gosuda.moreconsensus.kvnode.{index}, native arm64 binary hash="
                f"{binary_hash}, loopback client port={18990 + index * 100}, peer port="
                f"{18991 + index * 100}, admin port={18992 + index * 100}, and APFS data "
                f"path=/var/db/moreconsensus/campaign-incident-v2/data/node{index}."
            ),
        )
        for index in range(1, 4)
    ]
    alert_artifact = add_artifact(
        "CAMPAIGN-ALERT",
        "campaign",
        "raw-alert",
        opened_at + timedelta(minutes=3),
        "collect alert transition history from the approved incident channel",
        "Observed alert transitions include trigger time, clear time, affected node, "
        "metric predicate, and incident correlation for the Darwin campaign.",
    )
    runbook_artifact = add_artifact(
        "CAMPAIGN-RUNBOOK",
        "campaign",
        "raw-runbook",
        opened_at + timedelta(minutes=4),
        "capture the approved runbook revision and action authorization",
        "Observed runbook export binds the approved rollback boundary, abort criteria, "
        "quorum guard, and escalation path used during every drill.",
    )

    class_nonclaims = {
        "process_crash_restart": ["not-host-reboot", "not-independent-failure-domain"],
        "one_node_unavailability": ["not-multi-host", "not-independent-failure-domain"],
        "bad_config_rollback": ["not-client-or-peer-authorization-evidence"],
        "certificate_secret_rotation": [
            "not-mtls",
            "not-client-authorization",
            "not-peer-authorization",
        ],
        "storage_pressure_failure": [
            "not-physical-apfs-failure",
            "not-enospc",
            "not-media-failure",
        ],
        "corrupted_checkpoint": [
            "not-live-corruption",
            "not-forged-manifest-resistance",
        ],
    }
    observations = {
        "process_crash_restart": {
            "node": "node2",
            "launchd_label": "org.gosuda.moreconsensus.kvnode.2",
            "crash_signal": "SIGKILL",
            "old_pid": 42002,
            "new_pid": 42102,
            "supervisor_restart_observed": True,
            "durable_canary_observed": True,
        },
        "one_node_unavailability": {
            "unavailable_node": "node2",
            "healthy_nodes": ["node1", "node3"],
            "expected_voters": 3,
            "available_voters": 2,
            "quorum_write_observed": True,
            "cross_node_read_observed": True,
        },
        "bad_config_rollback": {
            "node": "node2",
            "launchd_label": "org.gosuda.moreconsensus.kvnode.2",
            "invalid_config_sha256": hashlib.sha256(b"invalid-plist").hexdigest(),
            "last_known_good_sha256": hashlib.sha256(b"approved-plist").hexdigest(),
            "validation_rejected": True,
            "rollback_completed": True,
            "service_restored": True,
        },
        "certificate_secret_rotation": {
            "nodes_rotated": ["node1", "node2", "node3"],
            "rotation_scope": "server-certificate-and-private-key",
            "reload_method": "rolling-launchd-restart",
            "old_certificate_sha256": hashlib.sha256(b"old-certificate").hexdigest(),
            "new_certificate_sha256": hashlib.sha256(b"new-certificate").hexdigest(),
            "old_private_key_sha256": hashlib.sha256(b"old-private-key").hexdigest(),
            "new_private_key_sha256": hashlib.sha256(b"new-private-key").hexdigest(),
            "private_key_material_collected": False,
            "tls_server_auth_verified": True,
            "mtls_observed": False,
            "client_authorization_observed": False,
        },
        "storage_pressure_failure": {
            "node": "node2",
            "failure_mode": "logical-storage-unavailable-gate-with-apfs-free-space-observation",
            "apfs_free_bytes_before": 8_000_000_000,
            "apfs_free_bytes_after": 7_999_000_000,
            "storage_fault_metric_observed": True,
            "readiness_failed_observed": True,
            "quorum_service_observed": True,
            "physical_apfs_failure_observed": False,
            "fault_gate_cleared": True,
        },
        "corrupted_checkpoint": {
            "node": "node2",
            "checkpoint_mode": "offline-altered-copy",
            "pristine_manifest_sha256": hashlib.sha256(b"pristine-manifest").hexdigest(),
            "altered_manifest_sha256": hashlib.sha256(b"altered-manifest").hexdigest(),
            "node_stopped_before_copy": True,
            "altered_copy_rejected": True,
            "quarantine_path": (
                "/var/db/moreconsensus/campaign-incident-v2/quarantine/"
                "node2-altered-checkpoint"
            ),
            "pristine_reverified": True,
            "suspect_copy_restored": False,
            "service_restored_from_pristine": True,
        },
    }
    affected = {
        "process_crash_restart": ["node2"],
        "one_node_unavailability": ["node2"],
        "bad_config_rollback": ["node2"],
        "certificate_secret_rotation": ["node1", "node2", "node3"],
        "storage_pressure_failure": ["node2"],
        "corrupted_checkpoint": ["node2"],
    }
    drills: list[dict[str, Any]] = []
    for index, incident_class in enumerate(DARWIN_INCIDENT_CLASSES):
        drill_id = f"DARWIN-DRILL-{index + 1:02d}"
        started_at = opened_at + timedelta(minutes=10 + index * 15)
        completed_at = started_at + timedelta(minutes=10)
        artifact_ids: list[str] = []
        for artifact_index, (kind, suffix) in enumerate(
            (
                ("raw-command-output", "COMMAND"),
                ("raw-log", "LOG"),
                ("raw-metric", "METRIC"),
                ("raw-communication", "COMMUNICATION"),
            ),
            start=1,
        ):
            expected_rejection = (
                incident_class == "corrupted_checkpoint" and kind == "raw-command-output"
            )
            artifact_ids.append(
                add_artifact(
                    f"DRILL-{index + 1:02d}-{suffix}",
                    drill_id,
                    kind,
                    started_at + timedelta(minutes=artifact_index),
                    (
                        "kvcheckpoint verify "
                        "/var/db/moreconsensus/campaign-incident-v2/quarantine/"
                        "node2-altered-checkpoint"
                        if expected_rejection
                        else f"observe Darwin launchd incident class {incident_class}"
                    ),
                    (
                        "Observed checkpoint verifier rejection for the altered offline copy; "
                        "the pristine checkpoint remained unchanged and reverified successfully."
                        if expected_rejection
                        else f"Observed Darwin launchd evidence for {incident_class}: "
                        "the bounded action, loopback service result, timestamps, and node identity "
                        "were captured from the native three-process target."
                    ),
                    result="expected-rejection" if expected_rejection else "observed-success",
                    exit_code=2 if expected_rejection else 0,
                )
            )
        drills.append(
            {
                "drill_id": drill_id,
                "incident_class": incident_class,
                "affected_nodes": affected[incident_class],
                "condition_source": (
                    f"Observed preapproved Darwin condition source for {incident_class} "
                    "with a healthy three-node baseline"
                ),
                "injection_method": (
                    f"Executed the bounded native launchd procedure for {incident_class} "
                    "against only the declared nodes"
                ),
                "impact_boundary": (
                    "Impact remained bounded to the declared same-host loopback processes "
                    "and APFS campaign directories"
                ),
                "expected_outcome": (
                    "The unaffected quorum must continue verified service and the affected "
                    "scope must recover within the approved boundary"
                ),
                "observed_outcome": (
                    "Raw command, log, metric, and communication evidence observed the "
                    "expected bounded recovery result"
                ),
                "rollback_plan": (
                    "Clear the injected condition, restore the approved bytes, stop writes "
                    "on quorum degradation, and preserve suspect state"
                ),
                "nonclaims": class_nonclaims[incident_class],
                "started_at": utc(started_at),
                "completed_at": utc(completed_at),
                "executor_participant_id": "operator-1",
                "approver_participant_id": "commander-1",
                "approved_at": utc(started_at - timedelta(minutes=1)),
                "result": "observed-pass",
                "evidence_artifact_ids": artifact_ids,
                "observations": observations[incident_class],
            }
        )

    latest_drill_end = opened_at + timedelta(minutes=95)
    operator_signed_at = latest_drill_end + timedelta(minutes=5)
    reviewer_signed_at = operator_signed_at + timedelta(minutes=1)
    operator_signoff_artifact = add_artifact(
        "CAMPAIGN-OPERATOR-SIGNOFF",
        "campaign",
        "raw-signoff",
        operator_signed_at,
        "capture operator approval assertion for the completed Darwin incident campaign",
        "Observed operator approval binds every drill identifier, artifact digest, target "
        "identity, release identity, and explicit nonclaim in the completed record.",
        result="observed-approval",
    )
    reviewer_signoff_artifact = add_artifact(
        "CAMPAIGN-REVIEWER-SIGNOFF",
        "campaign",
        "raw-signoff",
        reviewer_signed_at,
        "capture independent reviewer approval assertion after operator signoff",
        "Observed independent review checked raw bytes, identity bindings, chronology, "
        "bounded claims, and all six incident drill outcomes before approval.",
        result="observed-approval",
    )
    closed_at = reviewer_signed_at + timedelta(minutes=4)
    recorded_at = closed_at + timedelta(minutes=1)
    return {
        "schema_version": "2.0",
        "verifier_version": verifier_version,
        "record_kind": "target-incident-evidence",
        "record_mode": "synthetic-test",
        "claim": "synthetic-test-darwin-incident-readiness-observed",
        "target": {
            "name": target_id,
            "environment": environment,
            "service": "kvnode",
            "cluster_id": "mc-kv-darwin24-3n-r1",
        },
        "profile": {
            "platform": "darwin",
            "os_version": "24.6.0",
            "os_build": "24G84",
            "architecture": "arm64",
            "binary_format": "mach-o-64",
            "execution_mode": "native",
            "supervisor": "launchd",
            "launchd_domain": "system",
            "storage_filesystem": "apfs",
            "network_scope": "same-host-loopback",
            "tls_mode": "server-auth-only",
        },
        "topology": {
            "node_count": 3,
            "nodes": [
                {
                    "name": f"node{index}",
                    "node_id": index,
                    "launchd_label": f"org.gosuda.moreconsensus.kvnode.{index}",
                    "client_endpoint": f"https://127.0.0.1:{18990 + index * 100}",
                    "peer_endpoint": f"https://127.0.0.1:{18991 + index * 100}",
                    "admin_endpoint": f"https://127.0.0.1:{18992 + index * 100}",
                    "data_path": (
                        f"/var/db/moreconsensus/campaign-incident-v2/data/node{index}"
                    ),
                    "pid": 42000 + index,
                    "binary_sha256": binary_hash,
                    "observed_at": utc(opened_at + timedelta(minutes=1)),
                    "evidence_artifact_id": topology_artifacts[index - 1],
                }
                for index in range(1, 4)
            ],
        },
        "participants": [
            {
                "participant_id": "operator-1",
                "name": "Darwin Campaign Operator",
                "role": "operator",
                "organization": "Operations",
            },
            {
                "participant_id": "reviewer-1",
                "name": "Darwin Independent Reviewer",
                "role": "independent-reviewer",
                "organization": "Assurance",
            },
            {
                "participant_id": "commander-1",
                "name": "Darwin Incident Commander",
                "role": "incident-commander",
                "organization": "Response",
            },
        ],
        "release_provenance": {
            "release_id": release_id,
            "source_repository": "https://invalid.example/moreconsensus",
            "source_revision": source_revision,
            "source_tree": "clean",
            "binary_uri": "file:binary/kvnode",
            "binary_sha256": binary_hash,
            "release_manifest_uri": f"file:{manifest_relative.as_posix()}",
            "release_manifest_sha256": manifest_hash,
            "built_at": utc(opened_at - timedelta(hours=1)),
        },
        "opened_at": utc(opened_at),
        "closed_at": utc(closed_at),
        "recorded_at": utc(recorded_at),
        "valid_until": utc(now + timedelta(days=7)),
        "drills": drills,
        "raw_artifacts": artifacts,
        "operational_artifacts": {
            "topology_artifact_ids": topology_artifacts,
            "alert_artifact_ids": [alert_artifact],
            "runbook_artifact_ids": [runbook_artifact],
            "signoff_artifact_ids": [
                operator_signoff_artifact,
                reviewer_signoff_artifact,
            ],
        },
        "sign_off": {
            "operator": {
                "participant_id": "operator-1",
                "role": "operator",
                "signed_at": utc(operator_signed_at),
                "decision": "approved",
                "statement": (
                    "Operator confirms all bounded Darwin incident actions and raw results"
                ),
                "artifact_id": operator_signoff_artifact,
            },
            "independent_reviewer": {
                "participant_id": "reviewer-1",
                "role": "independent-reviewer",
                "signed_at": utc(reviewer_signed_at),
                "decision": "approved",
                "statement": (
                    "Independent reviewer confirms identity, chronology, raw bytes, and nonclaims"
                ),
                "artifact_id": reviewer_signoff_artifact,
            },
        },
        "nonclaims": {
            "multi_host": False,
            "independent_failure_domains": False,
            "mtls": False,
            "client_authorization": False,
            "peer_authorization": False,
            "production_capacity": False,
            "physical_storage_failure": False,
        },
    }


def rewrite_darwin_artifact(
    root: Path,
    document: dict[str, Any],
    artifact_id: str,
    updates: dict[str, Any],
) -> None:
    artifact = next(
        item for item in document["raw_artifacts"] if item["artifact_id"] == artifact_id
    )
    artifact_path = root / artifact["uri"][len("file:") :]
    envelope = json.loads(artifact_path.read_text(encoding="utf-8"))
    envelope.update(updates)
    artifact_path.write_text(json.dumps(envelope, indent=2) + "\n", encoding="utf-8")
    artifact["sha256"] = hashlib.sha256(artifact_path.read_bytes()).hexdigest()


def field_paths(value: Any, prefix: tuple[Any, ...] = ()) -> Iterable[tuple[Any, ...]]:
    if isinstance(value, dict):
        for key, child in value.items():
            path = prefix + (key,)
            yield path
            yield from field_paths(child, path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from field_paths(child, prefix + (index,))


def parent_and_key(value: Any, path: tuple[Any, ...]) -> tuple[Any, Any]:
    parent = value
    for component in path[:-1]:
        parent = parent[component]
    return parent, path[-1]


def malformed_value(value: Any) -> Any:
    if isinstance(value, dict):
        return []
    if isinstance(value, list):
        return []
    if isinstance(value, bool):
        return "true"
    if isinstance(value, int):
        return 0
    if isinstance(value, str):
        return ""
    return "malformed"


def assert_valid(
    document: dict[str, Any],
    now: datetime,
    name: str,
    *,
    test_mode: bool = True,
    evidence_root: Path | None = None,
) -> None:
    errors = verifier.validate_document(
        document,
        test_mode=test_mode,
        expected_target=document.get("target", {}).get("name") if not test_mode else None,
        expected_release_id=(
            document.get("release_provenance", {}).get("release_id") if not test_mode else None
        ),
        expected_source_revision=(
            document.get("release_provenance", {}).get("source_revision") if not test_mode else None
        ),
        expected_binary_sha256=(
            document.get("release_provenance", {}).get("binary_sha256") if not test_mode else None
        ),
        expected_environment=(
            document.get("target", {}).get("environment") if not test_mode else None
        ),
        evidence_root=evidence_root,
        now=now,
        verify_files=True,
    )
    if errors:
        raise AssertionError(f"{name} unexpectedly failed: {errors[:5]}")


def assert_invalid(
    document: dict[str, Any],
    now: datetime,
    name: str,
    *,
    test_mode: bool = True,
    verify_files: bool = False,
    expected_error: str | None = None,
    evidence_root: Path | None = None,
) -> None:
    errors = verifier.validate_document(
        document,
        test_mode=test_mode,
        expected_target=document.get("target", {}).get("name") if not test_mode else None,
        expected_release_id=(
            document.get("release_provenance", {}).get("release_id") if not test_mode else None
        ),
        expected_source_revision=(
            document.get("release_provenance", {}).get("source_revision") if not test_mode else None
        ),
        expected_binary_sha256=(
            document.get("release_provenance", {}).get("binary_sha256") if not test_mode else None
        ),
        expected_environment=(
            document.get("target", {}).get("environment") if not test_mode else None
        ),
        evidence_root=evidence_root,
        now=now,
        verify_files=verify_files,
    )
    if not errors:
        raise AssertionError(f"{name} unexpectedly passed")
    if expected_error is not None and not any(expected_error in error for error in errors):
        raise AssertionError(f"{name} did not report {expected_error!r}: {errors[:5]}")


def run_cli(args: list[str], expect_success: bool) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [sys.executable, str(VERIFIER_PATH), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if expect_success and result.returncode != 0:
        raise AssertionError(f"CLI unexpectedly failed: {result.stderr}")
    if not expect_success and result.returncode == 0:
        raise AssertionError(f"CLI unexpectedly passed: {result.stdout}")
    return result


def main() -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    with SCHEMA_PATH.open("r", encoding="utf-8") as handle:
        schema = json.load(handle, object_pairs_hook=verifier._object_pairs_without_duplicates)
    schema_classes = set(
        schema["$defs"]["scenario"]["properties"]["incident_class"]["enum"]
    )
    if schema_classes != set(SCENARIO_CLASSES):
        raise AssertionError("schema incident classes do not match the verifier contract")
    with DARWIN_SCHEMA_PATH.open("r", encoding="utf-8") as handle:
        darwin_schema = json.load(
            handle,
            object_pairs_hook=verifier._object_pairs_without_duplicates,
        )
    if darwin_schema.get("$id", "").endswith("target-incident-evidence-2.0.json") is False:
        raise AssertionError("Darwin schema is not explicitly versioned as 2.0")
    darwin_drill_refs = {
        item["$ref"]
        for item in darwin_schema["properties"]["drills"]["items"]["oneOf"]
    }
    expected_darwin_refs = {
        "#/$defs/processCrashRestartDrill",
        "#/$defs/oneNodeUnavailabilityDrill",
        "#/$defs/badConfigRollbackDrill",
        "#/$defs/certificateSecretRotationDrill",
        "#/$defs/storagePressureFailureDrill",
        "#/$defs/corruptedCheckpointDrill",
    }
    if darwin_drill_refs != expected_darwin_refs:
        raise AssertionError("Darwin schema drill classes do not match the verifier contract")
    if schema["properties"]["schema_version"] != {"const": "1.0"}:
        raise AssertionError("the v1 schema contract was not preserved")

    with tempfile.TemporaryDirectory(prefix="target-incident-selftest-") as temp_dir:
        root = Path(temp_dir) / "synthetic-fixture-evidence"
        fixture = build_fixture(root, now)
        fixture_path = root / "synthetic-fixture.json"
        fixture_path.write_text(json.dumps(fixture, indent=2) + "\n", encoding="utf-8")

        assert_valid(fixture, now, "complete synthetic fixture")
        cli_ok = run_cli(["--test-mode", str(fixture_path)], expect_success=True)
        if "test_only=true release_claim=none" not in cli_ok.stdout:
            raise AssertionError("synthetic CLI success was not explicitly marked as a non-claim")
        run_cli(
            [
                "--expected-target",
                fixture["target"]["name"],
                "--expected-source-revision",
                fixture["release_provenance"]["source_revision"],
                "--expected-binary-sha256",
                fixture["release_provenance"]["binary_sha256"],
                str(fixture_path),
            ],
            expect_success=False,
        )

        target_marked_fixture = copy.deepcopy(fixture)
        target_marked_fixture["record_mode"] = "target"
        target_marked_fixture["claim"] = "target-incident-response-evidence-complete"
        assert_invalid(
            target_marked_fixture,
            now,
            "production rejects synthetic and local evidence",
            test_mode=False,
            expected_error="local, synthetic, placeholder, or non-claim marker",
        )
        assert_invalid(
            target_marked_fixture,
            now,
            "production scans raw evidence for local fixture markers",
            test_mode=False,
            verify_files=True,
            expected_error="raw artifact contains prohibited non-target marker",
        )
        local_fault = copy.deepcopy(target_marked_fixture)
        local_fault["scenarios"][0]["detection"]["signal"] = (
            "Observed test-fault endpoint response from 127.0.0.1"
        )
        assert_invalid(
            local_fault,
            now,
            "production rejects local test-fault evidence",
            test_mode=False,
            expected_error=(
                "$.scenarios[0].detection.signal: contains a local, synthetic, "
                "placeholder, or non-claim marker"
            ),
        )

        placeholder = copy.deepcopy(target_marked_fixture)
        placeholder["scenarios"][0]["triage"]["decision"] = "TBD placeholder"
        assert_invalid(
            placeholder,
            now,
            "production rejects placeholder evidence",
            test_mode=False,
            expected_error=(
                "$.scenarios[0].triage.decision: contains a local, synthetic, "
                "placeholder, or non-claim marker"
            ),
        )

        production_non_claim = copy.deepcopy(target_marked_fixture)
        production_non_claim["claim"] = "none"
        assert_invalid(
            production_non_claim,
            now,
            "production rejects a none claim",
            test_mode=False,
            expected_error="$.claim: must be an explicit non-none target claim",
        )

        non_claim = copy.deepcopy(fixture)
        non_claim["claim"] = "none"
        assert_invalid(non_claim, now, "non-claim record")

        missing_scenario = copy.deepcopy(fixture)
        missing_scenario["scenarios"].pop()
        assert_invalid(missing_scenario, now, "missing incident class")

        duplicate_class = copy.deepcopy(fixture)
        duplicate_class["scenarios"][1]["incident_class"] = duplicate_class["scenarios"][0][
            "incident_class"
        ]
        assert_invalid(duplicate_class, now, "duplicate incident class")

        duplicate_scenario = copy.deepcopy(fixture)
        duplicate_scenario["scenarios"][1]["scenario_id"] = duplicate_scenario["scenarios"][0][
            "scenario_id"
        ]
        assert_invalid(duplicate_scenario, now, "duplicate scenario identifier")

        offset_timestamp = copy.deepcopy(fixture)
        offset_timestamp["scenarios"][0]["detection"]["detected_at"] = "2026-01-01T00:00:00+00:00"
        assert_invalid(offset_timestamp, now, "non-Z UTC timestamp")

        reversed_timestamp = copy.deepcopy(fixture)
        reversed_timestamp["scenarios"][0]["triage"]["decided_at"] = utc(now - timedelta(days=2))
        assert_invalid(reversed_timestamp, now, "non-UTC ordering")

        missing_artifact_hash = copy.deepcopy(fixture)
        del missing_artifact_hash["raw_artifacts"][0]["sha256"]
        assert_invalid(missing_artifact_hash, now, "missing raw artifact hash")

        malformed_artifact_hash = copy.deepcopy(fixture)
        malformed_artifact_hash["raw_artifacts"][0]["sha256"] = "0" * 64
        assert_invalid(
            malformed_artifact_hash,
            now,
            "raw artifact checksum mismatch",
            verify_files=True,
            expected_error="does not match the file content",
        )

        missing_binary_hash = copy.deepcopy(fixture)
        del missing_binary_hash["release_provenance"]["binary_sha256"]
        assert_invalid(missing_binary_hash, now, "missing release binary hash")

        duplicate_artifact = copy.deepcopy(fixture)
        duplicate_artifact["raw_artifacts"][1]["artifact_id"] = duplicate_artifact[
            "raw_artifacts"
        ][0]["artifact_id"]
        assert_invalid(duplicate_artifact, now, "duplicate raw artifact")

        duplicate_reference = copy.deepcopy(fixture)
        duplicate_reference["scenarios"][0]["detection"]["evidence_artifact_ids"].append(
            duplicate_reference["scenarios"][0]["detection"]["evidence_artifact_ids"][0]
        )
        assert_invalid(duplicate_reference, now, "duplicate raw artifact reference")

        stale = copy.deepcopy(target_marked_fixture)
        stale["valid_until"] = utc(now - timedelta(seconds=1))
        assert_invalid(
            stale,
            now,
            "stale target record",
            test_mode=False,
            expected_error="record is stale",
        )

        scenario_field_checks = 0
        for scenario_index, scenario in enumerate(fixture["scenarios"]):
            for relative_path in field_paths(scenario):
                omitted = copy.deepcopy(fixture)
                omitted_parent, omitted_key = parent_and_key(
                    omitted["scenarios"][scenario_index], relative_path
                )
                del omitted_parent[omitted_key]
                assert_invalid(
                    omitted,
                    now,
                    f"omitted scenario field {scenario_index}:{relative_path}",
                )

                malformed = copy.deepcopy(fixture)
                malformed_parent, malformed_key = parent_and_key(
                    malformed["scenarios"][scenario_index], relative_path
                )
                malformed_parent[malformed_key] = malformed_value(malformed_parent[malformed_key])
                assert_invalid(
                    malformed,
                    now,
                    f"malformed scenario field {scenario_index}:{relative_path}",
                )
                scenario_field_checks += 2

        darwin_root = Path(temp_dir) / "darwin-v2-evidence"
        darwin_fixture = build_darwin_fixture(darwin_root, now)
        darwin_fixture_path = darwin_root / "incident-v2.json"
        darwin_fixture_path.write_text(
            json.dumps(darwin_fixture, indent=2) + "\n",
            encoding="utf-8",
        )
        assert_valid(
            darwin_fixture,
            now,
            "truthful Darwin v2 synthetic fixture",
            evidence_root=darwin_root,
        )
        darwin_cli = run_cli(
            [
                "--test-mode",
                "--evidence-root",
                str(darwin_root),
                str(darwin_fixture_path),
            ],
            expect_success=True,
        )
        if "test_only=true release_claim=none" not in darwin_cli.stdout:
            raise AssertionError("Darwin v2 synthetic success was not marked as a non-claim")

        darwin_target = copy.deepcopy(darwin_fixture)
        darwin_target["record_mode"] = "target"
        darwin_target["claim"] = "target-darwin-incident-readiness-observed"
        assert_invalid(
            darwin_target,
            now,
            "synthetic Darwin evidence relabeled as production",
            test_mode=False,
            verify_files=True,
            evidence_root=darwin_root,
            expected_error="production kvnode Mach-O is truncated or implausibly small",
        )
        darwin_target_path = darwin_root / "incident-v2-target.json"
        darwin_target_path.write_text(
            json.dumps(darwin_target, indent=2) + "\n",
            encoding="utf-8",
        )
        relabeled_cli = run_cli(
            [
                "--expected-target",
                darwin_target["target"]["name"],
                "--expected-release-id",
                darwin_target["release_provenance"]["release_id"],
                "--expected-source-revision",
                darwin_target["release_provenance"]["source_revision"],
                "--expected-binary-sha256",
                darwin_target["release_provenance"]["binary_sha256"],
                "--expected-environment",
                darwin_target["target"]["environment"],
                "--evidence-root",
                str(darwin_root),
                str(darwin_target_path),
            ],
            expect_success=False,
        )
        for expected_failure in (
            "production kvnode Mach-O is truncated or implausibly small",
            "$.release_provenance.manifest.origin",
            ".envelope.record_mode: does not match record record_mode",
        ):
            if expected_failure not in relabeled_cli.stderr:
                raise AssertionError(
                    f"relabeled synthetic Darwin record missed {expected_failure!r}: "
                    f"{relabeled_cli.stderr}"
                )

        missing_v2_expected_identity = run_cli(
            [
                "--expected-target",
                darwin_target["target"]["name"],
                "--expected-source-revision",
                darwin_target["release_provenance"]["source_revision"],
                "--expected-binary-sha256",
                darwin_target["release_provenance"]["binary_sha256"],
                "--evidence-root",
                str(darwin_root),
                str(darwin_target_path),
            ],
            expect_success=False,
        )
        if "--expected-release-id" not in missing_v2_expected_identity.stderr:
            raise AssertionError("Darwin v2 CLI did not fail closed on a missing release identity")

        remote_topology = copy.deepcopy(darwin_target)
        remote_topology["topology"]["nodes"][1]["peer_endpoint"] = "https://10.0.0.22:19191"
        assert_invalid(
            remote_topology,
            now,
            "fabricated remote topology",
            test_mode=False,
            evidence_root=darwin_root,
            expected_error="must equal https://127.0.0.1:19191",
        )

        loopback_relabel = copy.deepcopy(darwin_target)
        loopback_relabel["profile"]["network_scope"] = "multi-host"
        assert_invalid(
            loopback_relabel,
            now,
            "loopback relabeled as multi-host",
            test_mode=False,
            evidence_root=darwin_root,
            expected_error="outside the same-host Darwin profile",
        )

        false_nonclaim = copy.deepcopy(darwin_target)
        false_nonclaim["nonclaims"]["mtls"] = True
        assert_invalid(
            false_nonclaim,
            now,
            "false mutual TLS claim",
            test_mode=False,
            evidence_root=darwin_root,
            expected_error="must equal false",
        )

        false_authorization = copy.deepcopy(darwin_target)
        rotation = next(
            drill
            for drill in false_authorization["drills"]
            if drill["incident_class"] == "certificate_secret_rotation"
        )
        rotation["observations"]["client_authorization_observed"] = True
        assert_invalid(
            false_authorization,
            now,
            "false client authorization claim",
            test_mode=False,
            evidence_root=darwin_root,
            expected_error="client_authorization_observed: must equal false",
        )

        linux_profile = copy.deepcopy(darwin_target)
        linux_profile["profile"]["supervisor"] = "systemd"
        assert_invalid(
            linux_profile,
            now,
            "Linux supervisor artifact in Darwin v2",
            test_mode=False,
            evidence_root=darwin_root,
            expected_error="prohibited by Darwin v2",
        )

        duplicate_drill_id = copy.deepcopy(darwin_fixture)
        duplicate_drill_id["drills"][1]["drill_id"] = duplicate_drill_id["drills"][0][
            "drill_id"
        ]
        assert_invalid(
            duplicate_drill_id,
            now,
            "duplicated Darwin drill identifier",
            evidence_root=darwin_root,
            expected_error="duplicates an earlier value",
        )

        duplicate_drill_class = copy.deepcopy(darwin_fixture)
        duplicate_drill_class["drills"][1]["incident_class"] = duplicate_drill_class[
            "drills"
        ][0]["incident_class"]
        assert_invalid(
            duplicate_drill_class,
            now,
            "duplicated Darwin drill class",
            evidence_root=darwin_root,
            expected_error="incident classes mismatch",
        )

        missing_raw_log = copy.deepcopy(darwin_fixture)
        next(
            artifact
            for artifact in missing_raw_log["raw_artifacts"]
            if artifact["artifact_id"] == "DRILL-01-LOG"
        )["kind"] = "raw-metric"
        assert_invalid(
            missing_raw_log,
            now,
            "missing drill raw log",
            evidence_root=darwin_root,
            expected_error="must reference raw-log evidence",
        )

        executor_only_signoff = copy.deepcopy(darwin_fixture)
        executor_only_signoff["sign_off"]["independent_reviewer"][
            "participant_id"
        ] = "operator-1"
        assert_invalid(
            executor_only_signoff,
            now,
            "executor-only operational signoff",
            evidence_root=darwin_root,
            expected_error="participant must have role independent-reviewer",
        )

        reviewer_is_executor = copy.deepcopy(darwin_fixture)
        reviewer_is_executor["drills"][0]["executor_participant_id"] = "reviewer-1"
        assert_invalid(
            reviewer_is_executor,
            now,
            "independent reviewer also executed a drill",
            evidence_root=darwin_root,
            expected_error="independent reviewer must not be a drill executor",
        )

        simultaneous_signoff = copy.deepcopy(darwin_fixture)
        simultaneous_signoff["sign_off"]["independent_reviewer"]["signed_at"] = (
            simultaneous_signoff["sign_off"]["operator"]["signed_at"]
        )
        assert_invalid(
            simultaneous_signoff,
            now,
            "simultaneous non-independent signoff",
            evidence_root=darwin_root,
            expected_error="must follow operator signoff",
        )

        stale_darwin = copy.deepcopy(darwin_target)
        stale_darwin["valid_until"] = utc(now - timedelta(seconds=1))
        assert_invalid(
            stale_darwin,
            now,
            "stale Darwin target record",
            test_mode=False,
            evidence_root=darwin_root,
            expected_error="record is stale",
        )

        mismatched_topology_hash = copy.deepcopy(darwin_fixture)
        mismatched_topology_hash["topology"]["nodes"][0]["binary_sha256"] = "0" * 64
        assert_invalid(
            mismatched_topology_hash,
            now,
            "topology binary identity mismatch",
            evidence_root=darwin_root,
            expected_error="does not match release binary",
        )

        darwin_linux_root = Path(temp_dir) / "darwin-linux-artifact"
        darwin_linux_artifact = build_darwin_fixture(darwin_linux_root, now)
        rewrite_darwin_artifact(
            darwin_linux_root,
            darwin_linux_artifact,
            "DRILL-01-COMMAND",
            {
                "output": (
                    "Observed systemctl status output for a Linux service, which is not "
                    "eligible evidence for the native Darwin campaign."
                )
            },
        )
        assert_invalid(
            darwin_linux_artifact,
            now,
            "systemd raw artifact in Darwin record",
            verify_files=True,
            evidence_root=darwin_linux_root,
            expected_error="prohibited by Darwin v2",
        )

        darwin_remote_root = Path(temp_dir) / "darwin-remote-artifact"
        darwin_remote_artifact = build_darwin_fixture(darwin_remote_root, now)
        rewrite_darwin_artifact(
            darwin_remote_root,
            darwin_remote_artifact,
            "DRILL-02-COMMAND",
            {
                "output": (
                    "Observed a purported peer at 192.0.2.44:19191 and recorded it as "
                    "part of the incident topology without loopback proof."
                )
            },
        )
        assert_invalid(
            darwin_remote_artifact,
            now,
            "fabricated remote address in raw evidence",
            verify_files=True,
            evidence_root=darwin_remote_root,
            expected_error="contains non-loopback peer address 192.0.2.44",
        )

        darwin_identity_root = Path(temp_dir) / "darwin-identity-artifact"
        darwin_identity_artifact = build_darwin_fixture(darwin_identity_root, now)
        rewrite_darwin_artifact(
            darwin_identity_root,
            darwin_identity_artifact,
            "DRILL-03-LOG",
            {"release_id": "stale-release-id"},
        )
        assert_invalid(
            darwin_identity_artifact,
            now,
            "raw evidence release identity mismatch",
            verify_files=True,
            evidence_root=darwin_identity_root,
            expected_error="does not match record release_id",
        )

        darwin_timestamp_root = Path(temp_dir) / "darwin-timestamp-artifact"
        darwin_timestamp_artifact = build_darwin_fixture(darwin_timestamp_root, now)
        rewrite_darwin_artifact(
            darwin_timestamp_root,
            darwin_timestamp_artifact,
            "DRILL-04-METRIC",
            {"observed_at": utc(now - timedelta(days=2))},
        )
        assert_invalid(
            darwin_timestamp_artifact,
            now,
            "raw evidence timestamp mismatch",
            verify_files=True,
            evidence_root=darwin_timestamp_root,
            expected_error="must exactly match captured_at",
        )

        darwin_marker_root = Path(temp_dir) / "darwin-marker-artifact"
        darwin_marker_artifact = build_darwin_fixture(darwin_marker_root, now)
        rewrite_darwin_artifact(
            darwin_marker_root,
            darwin_marker_artifact,
            "DRILL-05-METRIC",
            {"output": "pass"},
        )
        assert_invalid(
            darwin_marker_artifact,
            now,
            "marker-only pass record",
            verify_files=True,
            evidence_root=darwin_marker_root,
            expected_error="not a marker-only pass record",
        )

        darwin_rejection_root = Path(temp_dir) / "darwin-rejection-artifact"
        darwin_rejection_artifact = build_darwin_fixture(darwin_rejection_root, now)
        rewrite_darwin_artifact(
            darwin_rejection_root,
            darwin_rejection_artifact,
            "DRILL-06-COMMAND",
            {"exit_code": 0},
        )
        assert_invalid(
            darwin_rejection_artifact,
            now,
            "fabricated corrupted-checkpoint rejection",
            verify_files=True,
            evidence_root=darwin_rejection_root,
            expected_error="must be nonzero for expected-rejection",
        )

        darwin_missing_file_root = Path(temp_dir) / "darwin-missing-file"
        darwin_missing_file = build_darwin_fixture(darwin_missing_file_root, now)
        missing_artifact = next(
            artifact
            for artifact in darwin_missing_file["raw_artifacts"]
            if artifact["artifact_id"] == "DRILL-01-LOG"
        )
        (darwin_missing_file_root / missing_artifact["uri"][len("file:") :]).unlink()
        assert_invalid(
            darwin_missing_file,
            now,
            "missing raw evidence file",
            verify_files=True,
            evidence_root=darwin_missing_file_root,
            expected_error="must resolve inside --evidence-root",
        )

        darwin_manifest_hash_root = Path(temp_dir) / "darwin-manifest-hash"
        darwin_manifest_hash = build_darwin_fixture(darwin_manifest_hash_root, now)
        manifest_uri = darwin_manifest_hash["release_provenance"]["release_manifest_uri"]
        manifest_file = darwin_manifest_hash_root / manifest_uri[len("file:") :]
        manifest_file.write_text(
            manifest_file.read_text(encoding="utf-8") + " ",
            encoding="utf-8",
        )
        assert_invalid(
            darwin_manifest_hash,
            now,
            "release manifest hash mismatch",
            verify_files=True,
            evidence_root=darwin_manifest_hash_root,
            expected_error="does not match the release manifest bytes",
        )

        darwin_manifest_identity_root = Path(temp_dir) / "darwin-manifest-identity"
        darwin_manifest_identity = build_darwin_fixture(darwin_manifest_identity_root, now)
        manifest_uri = darwin_manifest_identity["release_provenance"]["release_manifest_uri"]
        manifest_file = darwin_manifest_identity_root / manifest_uri[len("file:") :]
        manifest_document = json.loads(manifest_file.read_text(encoding="utf-8"))
        manifest_document["release_id"] = "stale-release"
        manifest_file.write_text(
            json.dumps(manifest_document, indent=2) + "\n",
            encoding="utf-8",
        )
        darwin_manifest_identity["release_provenance"]["release_manifest_sha256"] = (
            hashlib.sha256(manifest_file.read_bytes()).hexdigest()
        )
        assert_invalid(
            darwin_manifest_identity,
            now,
            "release manifest identity mismatch",
            verify_files=True,
            evidence_root=darwin_manifest_identity_root,
            expected_error="does not match record release_id",
        )

        manifest_escape = copy.deepcopy(darwin_fixture)
        manifest_escape["release_provenance"]["release_manifest_uri"] = (
            "file:manifest/../outside.json"
        )
        assert_invalid(
            manifest_escape,
            now,
            "release manifest root escape",
            evidence_root=darwin_root,
            expected_error="must be a normalized root-relative file URI",
        )

        darwin_container_root = Path(temp_dir) / "darwin-container-artifact"
        darwin_container_artifact = build_darwin_fixture(darwin_container_root, now)
        rewrite_darwin_artifact(
            darwin_container_root,
            darwin_container_artifact,
            "DRILL-02-LOG",
            {
                "output": (
                    "Observed docker inspect output naming a container image and runtime "
                    "instead of the required native Darwin launchd process."
                )
            },
        )
        assert_invalid(
            darwin_container_artifact,
            now,
            "container raw artifact in Darwin record",
            verify_files=True,
            evidence_root=darwin_container_root,
            expected_error="prohibited by Darwin v2",
        )

        missing_alert = copy.deepcopy(darwin_fixture)
        missing_alert["operational_artifacts"]["alert_artifact_ids"] = []
        assert_invalid(
            missing_alert,
            now,
            "missing observed alert artifact",
            evidence_root=darwin_root,
            expected_error="must contain at least 1 item",
        )

        duplicate_topology_pid = copy.deepcopy(darwin_fixture)
        duplicate_topology_pid["topology"]["nodes"][1]["pid"] = duplicate_topology_pid[
            "topology"
        ]["nodes"][0]["pid"]
        assert_invalid(
            duplicate_topology_pid,
            now,
            "duplicated topology process identifier",
            evidence_root=darwin_root,
            expected_error="duplicates an earlier value",
        )

        mismatched_topology_evidence = copy.deepcopy(darwin_fixture)
        mismatched_topology_evidence["topology"]["nodes"][1]["evidence_artifact_id"] = (
            mismatched_topology_evidence["topology"]["nodes"][0]["evidence_artifact_id"]
        )
        assert_invalid(
            mismatched_topology_evidence,
            now,
            "duplicated topology evidence reference",
            evidence_root=darwin_root,
            expected_error="ordered per-node topology evidence references",
        )

        darwin_field_checks = 0
        for drill_index, drill in enumerate(darwin_fixture["drills"]):
            for relative_path in field_paths(drill):
                omitted = copy.deepcopy(darwin_fixture)
                omitted_parent, omitted_key = parent_and_key(
                    omitted["drills"][drill_index],
                    relative_path,
                )
                del omitted_parent[omitted_key]
                assert_invalid(
                    omitted,
                    now,
                    f"omitted Darwin drill field {drill_index}:{relative_path}",
                    evidence_root=darwin_root,
                )

                malformed = copy.deepcopy(darwin_fixture)
                malformed_parent, malformed_key = parent_and_key(
                    malformed["drills"][drill_index],
                    relative_path,
                )
                malformed_parent[malformed_key] = malformed_value(
                    malformed_parent[malformed_key]
                )
                assert_invalid(
                    malformed,
                    now,
                    f"malformed Darwin drill field {drill_index}:{relative_path}",
                    evidence_root=darwin_root,
                )
                darwin_field_checks += 2

        duplicate_json_path = root / "duplicate-key.json"
        duplicate_json_path.write_text(
            '{"schema_version":"1.0","schema_version":"1.0"}\n',
            encoding="utf-8",
        )
        run_cli(["--test-mode", str(duplicate_json_path)], expect_success=False)

        print(
            "target_incident_evidence_selftest=passed "
            f"scenario_field_rejections={scenario_field_checks} "
            f"darwin_drill_field_rejections={darwin_field_checks} "
            "fault_injection=not-performed release_claim=none"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
