#!/usr/bin/env python3
"""Fail-closed verifier for target incident-response evidence records."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

SCENARIO_CLASSES = {
    "storage_failure",
    "network_partition",
    "peer_compromise",
    "replay_checksum_suspicion",
    "recovery_stall",
}
ARTIFACT_KINDS = {
    "raw-log",
    "raw-metric",
    "raw-command-output",
    "raw-communication",
    "raw-snapshot",
}
ACTION_TYPES = {
    "command",
    "config-change",
    "traffic-control",
    "process-control",
    "storage-operation",
    "access-control",
    "observe-only",
}
UTC_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
REVISION_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
TARGET_CLAIM_PATTERN = re.compile(r"^target-[a-z0-9]+(?:-[a-z0-9]+)*$")
NON_TARGET_FILE_MARKERS = (
    b"synthetic",
    b"fixture",
    b"placeholder",
    b"tabletop",
    b"loopback",
    b"localhost",
    b"127.0.0.1",
    b"::1",
    b"test-fault",
    b"test_fault",
    b"local-only",
    b"local_only",
    b"/tmp/",
    b"example-operator-report",
    b"release_claim=none",
    b"none-target",
)
NON_TARGET_MARKER = re.compile(
    r"(?:^|[^a-z0-9])(?:placeholder|tbd|todo|unknown|unspecified|synthetic|fixture|"
    r"example|tabletop|loopback|local[-_ ]only|test[-_ ]fault|mock|fake|"
    r"none[-_ ]target|non[-_ ]claim|no[-_ ]claim)(?:$|[^a-z0-9])",
    re.IGNORECASE,
)

TOP_LEVEL_KEYS = {
    "schema_version",
    "record_kind",
    "record_mode",
    "claim",
    "target",
    "participants",
    "release_provenance",
    "opened_at",
    "closed_at",
    "recorded_at",
    "valid_until",
    "scenarios",
    "raw_artifacts",
    "follow_up_actions",
    "sign_off",
}
SCENARIO_KEYS = {
    "scenario_id",
    "incident_class",
    "participants",
    "started_at",
    "ended_at",
    "detection",
    "triage",
    "mitigation",
    "quorum_data_safety",
    "acceptance_results",
    "communication_escalation",
    "recovery_evidence",
    "post_incident_canaries",
}

DARWIN_SCHEMA_VERSION = "2.0"
DARWIN_VERIFIER_VERSION = "target-incident-evidence-verifier/2.0"
DARWIN_TARGET_ID = "mc-kv-darwin24-arm64-launchd-3n-r1"
DARWIN_ENVIRONMENT = "native-darwin24-arm64-launchd-system-domain-v1"
DARWIN_CLUSTER_ID = "mc-kv-darwin24-3n-r1"
DARWIN_INCIDENT_CLASSES = {
    "process_crash_restart",
    "one_node_unavailability",
    "bad_config_rollback",
    "certificate_secret_rotation",
    "storage_pressure_failure",
    "corrupted_checkpoint",
}
DARWIN_NONCLAIMS = {
    "multi_host",
    "independent_failure_domains",
    "mtls",
    "client_authorization",
    "peer_authorization",
    "production_capacity",
    "physical_storage_failure",
}
DARWIN_NODE_CONTRACT = {
    "node1": {
        "node_id": 1,
        "launchd_label": "org.gosuda.moreconsensus.kvnode.1",
        "client_endpoint": "https://127.0.0.1:19090",
        "peer_endpoint": "https://127.0.0.1:19091",
        "admin_endpoint": "https://127.0.0.1:19092",
    },
    "node2": {
        "node_id": 2,
        "launchd_label": "org.gosuda.moreconsensus.kvnode.2",
        "client_endpoint": "https://127.0.0.1:19190",
        "peer_endpoint": "https://127.0.0.1:19191",
        "admin_endpoint": "https://127.0.0.1:19192",
    },
    "node3": {
        "node_id": 3,
        "launchd_label": "org.gosuda.moreconsensus.kvnode.3",
        "client_endpoint": "https://127.0.0.1:19290",
        "peer_endpoint": "https://127.0.0.1:19291",
        "admin_endpoint": "https://127.0.0.1:19292",
    },
}
DARWIN_ARTIFACT_KINDS = {
    "raw-log",
    "raw-metric",
    "raw-command-output",
    "raw-communication",
    "raw-alert",
    "raw-runbook",
    "raw-signoff",
}
DARWIN_TOP_LEVEL_KEYS = {
    "schema_version",
    "verifier_version",
    "record_kind",
    "record_mode",
    "claim",
    "target",
    "profile",
    "topology",
    "participants",
    "release_provenance",
    "opened_at",
    "closed_at",
    "recorded_at",
    "valid_until",
    "drills",
    "raw_artifacts",
    "operational_artifacts",
    "sign_off",
    "nonclaims",
}
DARWIN_DRILL_KEYS = {
    "drill_id",
    "incident_class",
    "affected_nodes",
    "condition_source",
    "injection_method",
    "impact_boundary",
    "expected_outcome",
    "observed_outcome",
    "rollback_plan",
    "nonclaims",
    "started_at",
    "completed_at",
    "executor_participant_id",
    "approver_participant_id",
    "approved_at",
    "result",
    "evidence_artifact_ids",
    "observations",
}
DARWIN_RAW_ENVELOPE_KEYS = {
    "artifact_version",
    "verifier_version",
    "target_id",
    "release_id",
    "source_revision",
    "binary_sha256",
    "environment",
    "record_mode",
    "drill_id",
    "observed_at",
    "command",
    "exit_code",
    "result",
    "output",
}
DARWIN_RELEASE_MANIFEST_KEYS = {
    "manifest_version",
    "verifier_version",
    "origin",
    "record_mode",
    "target_id",
    "release_id",
    "source_revision",
    "binary_uri",
    "binary_sha256",
    "environment",
    "platform",
    "architecture",
    "binary_format",
    "build_command",
    "go_version",
    "vcs_modified",
    "codesign_requirement",
    "created_at",
}
DARWIN_LINUX_OR_CONTAINER_MARKER = re.compile(
    r"(?:^|[^a-z0-9])(?:linux|systemd|systemctl|journalctl|docker|containers?|"
    r"containerd|podman|kubernetes|kubectl|oci|lima|qemu|vagrant|vm|"
    r"virtual[- ]machine|cgroup|/proc)(?:$|[^a-z0-9])",
    re.IGNORECASE,
)
DARWIN_FALSE_CLAIM_MARKER = re.compile(
    r"(?:multi[- ]host|remote[- ]peer|independent[- ]failure[- ]domain|"
    r"\bmtls\b|mutual[- ]tls|client[- ]authorization|peer[- ]authorization|"
    r"production[- ]capacity)",
    re.IGNORECASE,
)
IPV4_PATTERN = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")


class Checker:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def fail(self, path: str, message: str) -> None:
        self.errors.append(f"{path}: {message}")

    def obj(self, value: Any, path: str, keys: set[str]) -> dict[str, Any]:
        if not isinstance(value, dict):
            self.fail(path, "must be an object")
            return {}
        missing = sorted(keys - set(value))
        unexpected = sorted(set(value) - keys)
        for key in missing:
            self.fail(f"{path}.{key}", "is required")
        for key in unexpected:
            self.fail(f"{path}.{key}", "is not permitted")
        return value

    def array(self, value: Any, path: str, minimum: int = 0) -> list[Any]:
        if not isinstance(value, list):
            self.fail(path, "must be an array")
            return []
        if len(value) < minimum:
            self.fail(path, f"must contain at least {minimum} item(s)")
        return value

    def text(
        self,
        value: Any,
        path: str,
        *,
        allowed: set[str] | None = None,
        pattern: re.Pattern[str] | None = None,
    ) -> str | None:
        if not isinstance(value, str) or not value.strip():
            self.fail(path, "must be a non-empty string")
            return None
        if value != value.strip():
            self.fail(path, "must not have leading or trailing whitespace")
        if allowed is not None and value not in allowed:
            self.fail(path, f"must be one of {sorted(allowed)}")
        if pattern is not None and pattern.fullmatch(value) is None:
            self.fail(path, "has invalid format")
        return value

    def identifier(self, value: Any, path: str) -> str | None:
        return self.text(value, path, pattern=IDENTIFIER_PATTERN)

    def timestamp(self, value: Any, path: str) -> datetime | None:
        if not isinstance(value, str) or UTC_PATTERN.fullmatch(value) is None:
            self.fail(path, "must be UTC in YYYY-MM-DDTHH:MM:SSZ form")
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            self.fail(path, "is not a real UTC timestamp")
            return None

    def boolean(self, value: Any, path: str) -> bool | None:
        if not isinstance(value, bool):
            self.fail(path, "must be a boolean")
            return None
        return value

    def positive_integer(self, value: Any, path: str) -> int | None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            self.fail(path, "must be an integer greater than zero")
            return None
        return value

    def unique(self, values: Iterable[Any], path: str) -> None:
        seen: set[Any] = set()
        for index, value in enumerate(values):
            try:
                duplicate = value in seen
                seen.add(value)
            except TypeError:
                self.fail(f"{path}[{index}]", "must be a scalar unique value")
                continue
            if duplicate:
                self.fail(f"{path}[{index}]", "duplicates an earlier value")

    def ordered(
        self,
        earlier: datetime | None,
        later: datetime | None,
        earlier_path: str,
        later_path: str,
    ) -> None:
        if earlier is not None and later is not None and earlier > later:
            self.fail(later_path, f"must not precede {earlier_path}")


def _object_pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_document(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(
            handle,
            object_pairs_hook=_object_pairs_without_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"invalid JSON numeric constant: {token}")
            ),
        )
    if not isinstance(value, dict):
        raise ValueError("evidence document root must be an object")
    return value


def _walk_strings(value: Any, path: str = "$") -> Iterable[tuple[str, str]]:
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, dict):
        for key, child in value.items():
            yield from _walk_strings(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _walk_strings(child, f"{path}[{index}]")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _validate_hashed_file(
    checker: Checker,
    raw_path: Any,
    expected_hash: Any,
    path_path: str,
    hash_path: str,
    *,
    verify_files: bool,
) -> None:
    path_text = checker.text(raw_path, path_path)
    hash_text = checker.text(expected_hash, hash_path, pattern=SHA256_PATTERN)
    if path_text is None:
        return
    file_path = Path(path_text)
    if not file_path.is_absolute():
        checker.fail(path_path, "must be an absolute raw evidence path")
        return
    if not verify_files:
        return
    if file_path.is_symlink():
        checker.fail(path_path, "must not be a symbolic link")
        return
    if not file_path.is_file():
        checker.fail(path_path, "must name an existing regular file")
        return
    if hash_text is not None:
        try:
            observed_hash = _sha256_file(file_path)
        except OSError as exc:
            checker.fail(path_path, f"cannot be read: {exc}")
            return
        if observed_hash != hash_text:
            checker.fail(hash_path, "does not match the file content")


def _non_target_marker_in_file(path: Path) -> str | None:
    try:
        with path.open("rb") as handle:
            carry = b""
            while True:
                block = handle.read(1024 * 1024)
                if not block:
                    return None
                candidate = carry + block.lower()
                for marker in NON_TARGET_FILE_MARKERS:
                    if marker in candidate:
                        return marker.decode("ascii")
                carry = candidate[-64:]
    except OSError:
        return None

def _validate_references(
    checker: Checker,
    value: Any,
    path: str,
    scenario_id: str | None,
    artifacts: dict[str, dict[str, Any]],
) -> set[str]:
    refs = checker.array(value, path, 1)
    parsed: list[str] = []
    for index, raw_ref in enumerate(refs):
        ref = checker.identifier(raw_ref, f"{path}[{index}]")
        if ref is None:
            continue
        parsed.append(ref)
        artifact = artifacts.get(ref)
        if artifact is None:
            checker.fail(f"{path}[{index}]", "does not identify a raw artifact")
        elif scenario_id is not None and artifact.get("scenario_id") != scenario_id:
            checker.fail(f"{path}[{index}]", "belongs to a different scenario")
    checker.unique(parsed, path)
    return set(parsed)


def _validate_v1_document(
    document: dict[str, Any],
    *,
    test_mode: bool,
    expected_target: str | None = None,
    expected_source_revision: str | None = None,
    expected_binary_sha256: str | None = None,
    now: datetime | None = None,
    verify_files: bool = True,
) -> list[str]:
    checker = Checker()
    root = checker.obj(document, "$", TOP_LEVEL_KEYS)

    schema_version = checker.text(root.get("schema_version"), "$.schema_version")
    if schema_version is not None and schema_version != "1.0":
        checker.fail("$.schema_version", "must equal 1.0")
    record_kind = checker.text(root.get("record_kind"), "$.record_kind")
    if record_kind is not None and record_kind != "target-incident-evidence":
        checker.fail("$.record_kind", "must equal target-incident-evidence")
    record_mode = checker.text(
        root.get("record_mode"), "$.record_mode", allowed={"target", "synthetic-test"}
    )
    claim = checker.text(root.get("claim"), "$.claim")
    if test_mode:
        if record_mode is not None and record_mode != "synthetic-test":
            checker.fail("$.record_mode", "test mode accepts only synthetic-test records")
        if claim is not None and claim != "synthetic-test-incident-response-evidence-complete":
            checker.fail("$.claim", "must be the synthetic test claim in test mode")
    else:
        if record_mode is not None and record_mode != "target":
            checker.fail("$.record_mode", "production verification accepts only target records")
        if claim is not None and TARGET_CLAIM_PATTERN.fullmatch(claim) is None:
            checker.fail("$.claim", "must be an explicit non-none target claim")
        if expected_target is None:
            checker.fail("$", "production verification requires --expected-target")
        if expected_source_revision is None:
            checker.fail("$", "production verification requires --expected-source-revision")
        if expected_binary_sha256 is None:
            checker.fail("$", "production verification requires --expected-binary-sha256")
        for string_path, value in _walk_strings(root):
            folded = value.casefold()
            if (
                NON_TARGET_MARKER.search(value) is not None
                or "localhost" in folded
                or "127.0.0.1" in folded
                or "::1" in folded
                or folded.startswith("/tmp/")
            ):
                checker.fail(string_path, "contains a local, synthetic, placeholder, or non-claim marker")

    target = checker.obj(
        root.get("target"),
        "$.target",
        {"name", "environment", "service", "cluster_id"},
    )
    target_name = checker.text(target.get("name"), "$.target.name")
    environment = checker.text(target.get("environment"), "$.target.environment")
    checker.text(target.get("service"), "$.target.service")
    checker.identifier(target.get("cluster_id"), "$.target.cluster_id")
    if not test_mode and environment is not None and environment.casefold() in {
        "local",
        "test",
        "development",
        "example",
        "synthetic",
    }:
        checker.fail("$.target.environment", "must name a target environment")
    if not test_mode and expected_target is not None and target_name != expected_target:
        checker.fail("$.target.name", "does not match --expected-target")

    participants_raw = checker.array(root.get("participants"), "$.participants", 3)
    participants: dict[str, dict[str, Any]] = {}
    participant_names: list[str] = []
    participant_roles: dict[str, str] = {}
    for index, raw_participant in enumerate(participants_raw):
        path = f"$.participants[{index}]"
        participant = checker.obj(
            raw_participant,
            path,
            {"participant_id", "name", "role", "organization"},
        )
        participant_id = checker.identifier(participant.get("participant_id"), f"{path}.participant_id")
        name = checker.text(participant.get("name"), f"{path}.name")
        role = checker.text(participant.get("role"), f"{path}.role")
        checker.text(participant.get("organization"), f"{path}.organization")
        if participant_id is not None:
            if participant_id in participants:
                checker.fail(f"{path}.participant_id", "duplicates an earlier participant")
            else:
                participants[participant_id] = participant
                if role is not None:
                    participant_roles[participant_id] = role
        if name is not None:
            participant_names.append(name)
    checker.unique(participant_names, "$.participants[*].name")
    present_roles = set(participant_roles.values())
    for required_role in {"operator", "independent-reviewer", "incident-commander"}:
        if required_role not in present_roles:
            checker.fail("$.participants", f"must include role {required_role}")

    provenance = checker.obj(
        root.get("release_provenance"),
        "$.release_provenance",
        {
            "release_id",
            "source_repository",
            "source_revision",
            "source_tree",
            "binary_path",
            "binary_sha256",
            "built_at",
        },
    )
    checker.identifier(provenance.get("release_id"), "$.release_provenance.release_id")
    checker.text(provenance.get("source_repository"), "$.release_provenance.source_repository")
    revision = checker.text(
        provenance.get("source_revision"),
        "$.release_provenance.source_revision",
        pattern=REVISION_PATTERN,
    )
    source_tree = checker.text(provenance.get("source_tree"), "$.release_provenance.source_tree")
    if source_tree is not None and source_tree != "clean":
        checker.fail("$.release_provenance.source_tree", "must equal clean")
    _validate_hashed_file(
        checker,
        provenance.get("binary_path"),
        provenance.get("binary_sha256"),
        "$.release_provenance.binary_path",
        "$.release_provenance.binary_sha256",
        verify_files=verify_files,
    )
    binary_hash = provenance.get("binary_sha256")
    built_at = checker.timestamp(provenance.get("built_at"), "$.release_provenance.built_at")
    if not test_mode and expected_source_revision is not None and revision != expected_source_revision:
        checker.fail("$.release_provenance.source_revision", "does not match --expected-source-revision")
    if not test_mode and expected_binary_sha256 is not None and binary_hash != expected_binary_sha256:
        checker.fail("$.release_provenance.binary_sha256", "does not match --expected-binary-sha256")

    opened_at = checker.timestamp(root.get("opened_at"), "$.opened_at")
    closed_at = checker.timestamp(root.get("closed_at"), "$.closed_at")
    recorded_at = checker.timestamp(root.get("recorded_at"), "$.recorded_at")
    valid_until = checker.timestamp(root.get("valid_until"), "$.valid_until")
    checker.ordered(built_at, opened_at, "$.release_provenance.built_at", "$.opened_at")
    checker.ordered(opened_at, closed_at, "$.opened_at", "$.closed_at")
    checker.ordered(closed_at, recorded_at, "$.closed_at", "$.recorded_at")
    checker.ordered(recorded_at, valid_until, "$.recorded_at", "$.valid_until")
    if recorded_at is not None and valid_until is not None:
        if valid_until - recorded_at > timedelta(days=30):
            checker.fail("$.valid_until", "must be no more than 30 days after recorded_at")
    check_now = now or datetime.now(timezone.utc)
    if not test_mode:
        if valid_until is not None and valid_until < check_now:
            checker.fail("$.valid_until", "record is stale")
        if recorded_at is not None and recorded_at > check_now + timedelta(minutes=5):
            checker.fail("$.recorded_at", "must not be more than five minutes in the future")

    artifacts_raw = checker.array(root.get("raw_artifacts"), "$.raw_artifacts", 15)
    artifacts: dict[str, dict[str, Any]] = {}
    artifact_paths: list[str] = []
    artifact_times: dict[str, datetime] = {}
    for index, raw_artifact in enumerate(artifacts_raw):
        path = f"$.raw_artifacts[{index}]"
        artifact = checker.obj(
            raw_artifact,
            path,
            {"artifact_id", "scenario_id", "kind", "path", "sha256", "captured_at"},
        )
        artifact_id = checker.identifier(artifact.get("artifact_id"), f"{path}.artifact_id")
        checker.identifier(artifact.get("scenario_id"), f"{path}.scenario_id")
        artifact_kind = checker.text(artifact.get("kind"), f"{path}.kind", allowed=ARTIFACT_KINDS)
        _validate_hashed_file(
            checker,
            artifact.get("path"),
            artifact.get("sha256"),
            f"{path}.path",
            f"{path}.sha256",
            verify_files=verify_files,
        )
        if (
            not test_mode
            and verify_files
            and artifact_kind in {
                "raw-log",
                "raw-metric",
                "raw-command-output",
                "raw-communication",
            }
            and isinstance(artifact.get("path"), str)
        ):
            marker = _non_target_marker_in_file(Path(artifact["path"]))
            if marker is not None:
                checker.fail(
                    f"{path}.path",
                    f"raw artifact contains prohibited non-target marker {marker!r}",
                )
        artifact_path = artifact.get("path")
        if isinstance(artifact_path, str):
            artifact_paths.append(artifact_path)
        captured_at = checker.timestamp(artifact.get("captured_at"), f"{path}.captured_at")
        if artifact_id is not None:
            if artifact_id in artifacts:
                checker.fail(f"{path}.artifact_id", "duplicates an earlier raw artifact")
            else:
                artifacts[artifact_id] = artifact
                if captured_at is not None:
                    artifact_times[artifact_id] = captured_at
    checker.unique(artifact_paths, "$.raw_artifacts[*].path")

    scenarios_raw = checker.array(root.get("scenarios"), "$.scenarios", 5)
    if len(scenarios_raw) != 5:
        checker.fail("$.scenarios", "must contain exactly five incident classes")
    scenario_ids: list[str] = []
    scenario_classes: list[str] = []
    scenario_bounds: dict[str, tuple[datetime | None, datetime | None]] = {}
    referenced_artifacts: set[str] = set()
    latest_scenario_end: datetime | None = None

    for index, raw_scenario in enumerate(scenarios_raw):
        path = f"$.scenarios[{index}]"
        scenario = checker.obj(raw_scenario, path, SCENARIO_KEYS)
        scenario_id = checker.identifier(scenario.get("scenario_id"), f"{path}.scenario_id")
        incident_class = checker.text(
            scenario.get("incident_class"),
            f"{path}.incident_class",
            allowed=SCENARIO_CLASSES,
        )
        if scenario_id is not None:
            scenario_ids.append(scenario_id)
        if incident_class is not None:
            scenario_classes.append(incident_class)

        scenario_participants_raw = checker.array(scenario.get("participants"), f"{path}.participants", 2)
        scenario_participant_ids: list[str] = []
        for participant_index, raw_scenario_participant in enumerate(scenario_participants_raw):
            participant_path = f"{path}.participants[{participant_index}]"
            scenario_participant = checker.obj(
                raw_scenario_participant,
                participant_path,
                {"participant_id", "incident_role"},
            )
            participant_id = checker.identifier(
                scenario_participant.get("participant_id"),
                f"{participant_path}.participant_id",
            )
            checker.text(
                scenario_participant.get("incident_role"),
                f"{participant_path}.incident_role",
            )
            if participant_id is not None:
                scenario_participant_ids.append(participant_id)
                if participant_id not in participants:
                    checker.fail(f"{participant_path}.participant_id", "does not identify a participant")
        checker.unique(scenario_participant_ids, f"{path}.participants[*].participant_id")
        scenario_participant_set = set(scenario_participant_ids)

        started_at = checker.timestamp(scenario.get("started_at"), f"{path}.started_at")
        ended_at = checker.timestamp(scenario.get("ended_at"), f"{path}.ended_at")
        checker.ordered(opened_at, started_at, "$.opened_at", f"{path}.started_at")
        checker.ordered(started_at, ended_at, f"{path}.started_at", f"{path}.ended_at")
        checker.ordered(ended_at, closed_at, f"{path}.ended_at", "$.closed_at")
        if ended_at is not None and (latest_scenario_end is None or ended_at > latest_scenario_end):
            latest_scenario_end = ended_at
        if scenario_id is not None:
            scenario_bounds[scenario_id] = (started_at, ended_at)

        detection = checker.obj(
            scenario.get("detection"),
            f"{path}.detection",
            {"signal", "detected_at", "evidence_artifact_ids"},
        )
        checker.text(detection.get("signal"), f"{path}.detection.signal")
        detected_at = checker.timestamp(detection.get("detected_at"), f"{path}.detection.detected_at")
        detection_refs = _validate_references(
            checker,
            detection.get("evidence_artifact_ids"),
            f"{path}.detection.evidence_artifact_ids",
            scenario_id,
            artifacts,
        )
        referenced_artifacts.update(detection_refs)

        triage = checker.obj(
            scenario.get("triage"),
            f"{path}.triage",
            {"decision", "rationale", "decided_at", "decision_maker_participant_id"},
        )
        checker.text(triage.get("decision"), f"{path}.triage.decision")
        checker.text(triage.get("rationale"), f"{path}.triage.rationale")
        decided_at = checker.timestamp(triage.get("decided_at"), f"{path}.triage.decided_at")
        decision_maker = checker.identifier(
            triage.get("decision_maker_participant_id"),
            f"{path}.triage.decision_maker_participant_id",
        )
        if decision_maker is not None and decision_maker not in scenario_participant_set:
            checker.fail(
                f"{path}.triage.decision_maker_participant_id",
                "must identify a scenario participant",
            )

        mitigation = checker.obj(
            scenario.get("mitigation"),
            f"{path}.mitigation",
            {
                "actions",
                "executor_participant_id",
                "scope",
                "live_action",
                "started_at",
                "completed_at",
                "change_reference",
                "authorization",
            },
        )
        executor = checker.identifier(
            mitigation.get("executor_participant_id"),
            f"{path}.mitigation.executor_participant_id",
        )
        if executor is not None and executor not in scenario_participant_set:
            checker.fail(
                f"{path}.mitigation.executor_participant_id",
                "must identify a scenario participant",
            )
        checker.text(mitigation.get("scope"), f"{path}.mitigation.scope")
        live_action = checker.boolean(mitigation.get("live_action"), f"{path}.mitigation.live_action")
        mitigation_started = checker.timestamp(
            mitigation.get("started_at"), f"{path}.mitigation.started_at"
        )
        mitigation_completed = checker.timestamp(
            mitigation.get("completed_at"), f"{path}.mitigation.completed_at"
        )
        checker.text(mitigation.get("change_reference"), f"{path}.mitigation.change_reference")

        actions_raw = checker.array(mitigation.get("actions"), f"{path}.mitigation.actions", 1)
        action_sequences: list[int] = []
        for action_index, raw_action in enumerate(actions_raw):
            action_path = f"{path}.mitigation.actions[{action_index}]"
            action = checker.obj(
                raw_action,
                action_path,
                {"sequence", "action_type", "exact_action", "target", "result", "evidence_artifact_ids"},
            )
            sequence = checker.positive_integer(action.get("sequence"), f"{action_path}.sequence")
            if sequence is not None:
                action_sequences.append(sequence)
            checker.text(action.get("action_type"), f"{action_path}.action_type", allowed=ACTION_TYPES)
            checker.text(action.get("exact_action"), f"{action_path}.exact_action")
            checker.text(action.get("target"), f"{action_path}.target")
            result = checker.text(
                action.get("result"),
                f"{action_path}.result",
                allowed={"succeeded", "failed", "aborted"},
            )
            if result is not None and result != "succeeded":
                checker.fail(f"{action_path}.result", "must be succeeded for a complete claim")
            action_refs = _validate_references(
                checker,
                action.get("evidence_artifact_ids"),
                f"{action_path}.evidence_artifact_ids",
                scenario_id,
                artifacts,
            )
            referenced_artifacts.update(action_refs)
        checker.unique(action_sequences, f"{path}.mitigation.actions[*].sequence")
        if action_sequences and action_sequences != list(range(1, len(action_sequences) + 1)):
            checker.fail(f"{path}.mitigation.actions", "sequence values must be contiguous and ordered from 1")

        authorization = checker.obj(
            mitigation.get("authorization"),
            f"{path}.mitigation.authorization",
            {
                "approval_reference",
                "approver_participant_ids",
                "approved_at",
                "decision",
                "abort_criteria",
            },
        )
        approvers_raw = checker.array(
            authorization.get("approver_participant_ids"),
            f"{path}.mitigation.authorization.approver_participant_ids",
            1 if live_action is True else 0,
        )
        approvers: list[str] = []
        for approver_index, raw_approver in enumerate(approvers_raw):
            approver = checker.identifier(
                raw_approver,
                f"{path}.mitigation.authorization.approver_participant_ids[{approver_index}]",
            )
            if approver is not None:
                approvers.append(approver)
                if approver not in scenario_participant_set:
                    checker.fail(
                        f"{path}.mitigation.authorization.approver_participant_ids[{approver_index}]",
                        "must identify a scenario participant",
                    )
        checker.unique(approvers, f"{path}.mitigation.authorization.approver_participant_ids")
        abort_criteria_raw = checker.array(
            authorization.get("abort_criteria"),
            f"{path}.mitigation.authorization.abort_criteria",
            1 if live_action is True else 0,
        )
        abort_criteria: list[str] = []
        for criterion_index, raw_criterion in enumerate(abort_criteria_raw):
            criterion = checker.text(
                raw_criterion,
                f"{path}.mitigation.authorization.abort_criteria[{criterion_index}]",
            )
            if criterion is not None:
                abort_criteria.append(criterion)
        checker.unique(abort_criteria, f"{path}.mitigation.authorization.abort_criteria")
        approval_reference = authorization.get("approval_reference")
        approval_decision = authorization.get("decision")
        approved_at: datetime | None = None
        if live_action is True:
            checker.text(
                approval_reference,
                f"{path}.mitigation.authorization.approval_reference",
            )
            approved_at = checker.timestamp(
                authorization.get("approved_at"),
                f"{path}.mitigation.authorization.approved_at",
            )
            decision = checker.text(
                approval_decision,
                f"{path}.mitigation.authorization.decision",
                allowed={"approved", "denied", "not-required"},
            )
            if decision is not None and decision != "approved":
                checker.fail(
                    f"{path}.mitigation.authorization.decision",
                    "must be approved for a completed live action",
                )
            if executor is not None and approvers and all(item == executor for item in approvers):
                checker.fail(
                    f"{path}.mitigation.authorization.approver_participant_ids",
                    "must include an approver independent of the executor",
                )
        elif live_action is False:
            if approval_reference is not None:
                checker.fail(
                    f"{path}.mitigation.authorization.approval_reference",
                    "must be null when live_action is false",
                )
            if approvers:
                checker.fail(
                    f"{path}.mitigation.authorization.approver_participant_ids",
                    "must be empty when live_action is false",
                )
            if authorization.get("approved_at") is not None:
                checker.fail(
                    f"{path}.mitigation.authorization.approved_at",
                    "must be null when live_action is false",
                )
            if approval_decision != "not-required":
                checker.fail(
                    f"{path}.mitigation.authorization.decision",
                    "must be not-required when live_action is false",
                )
            if abort_criteria:
                checker.fail(
                    f"{path}.mitigation.authorization.abort_criteria",
                    "must be empty when live_action is false",
                )

        safety = checker.obj(
            scenario.get("quorum_data_safety"),
            f"{path}.quorum_data_safety",
            {"checked_at", "quorum_result", "data_safety_result", "method", "evidence_artifact_ids"},
        )
        safety_at = checker.timestamp(safety.get("checked_at"), f"{path}.quorum_data_safety.checked_at")
        checker.text(
            safety.get("quorum_result"),
            f"{path}.quorum_data_safety.quorum_result",
            allowed={"quorum-maintained", "quorum-restored"},
        )
        checker.text(
            safety.get("data_safety_result"),
            f"{path}.quorum_data_safety.data_safety_result",
            allowed={"no-data-loss", "restored-from-verified-backup"},
        )
        checker.text(safety.get("method"), f"{path}.quorum_data_safety.method")
        safety_refs = _validate_references(
            checker,
            safety.get("evidence_artifact_ids"),
            f"{path}.quorum_data_safety.evidence_artifact_ids",
            scenario_id,
            artifacts,
        )
        referenced_artifacts.update(safety_refs)

        acceptance_raw = checker.array(
            scenario.get("acceptance_results"), f"{path}.acceptance_results", 1
        )
        acceptance_criteria: list[str] = []
        acceptance_times: list[tuple[str, datetime | None]] = []
        for acceptance_index, raw_acceptance in enumerate(acceptance_raw):
            acceptance_path = f"{path}.acceptance_results[{acceptance_index}]"
            acceptance = checker.obj(
                raw_acceptance,
                acceptance_path,
                {"criterion", "result", "observed_at", "evidence_artifact_ids"},
            )
            criterion = checker.text(acceptance.get("criterion"), f"{acceptance_path}.criterion")
            if criterion is not None:
                acceptance_criteria.append(criterion)
            result = checker.text(
                acceptance.get("result"),
                f"{acceptance_path}.result",
                allowed={"pass", "fail"},
            )
            if result is not None and result != "pass":
                checker.fail(f"{acceptance_path}.result", "must be pass for a complete claim")
            observed_at = checker.timestamp(
                acceptance.get("observed_at"), f"{acceptance_path}.observed_at"
            )
            acceptance_times.append((f"{acceptance_path}.observed_at", observed_at))
            acceptance_refs = _validate_references(
                checker,
                acceptance.get("evidence_artifact_ids"),
                f"{acceptance_path}.evidence_artifact_ids",
                scenario_id,
                artifacts,
            )
            referenced_artifacts.update(acceptance_refs)
        checker.unique(acceptance_criteria, f"{path}.acceptance_results[*].criterion")

        communication = checker.obj(
            scenario.get("communication_escalation"),
            f"{path}.communication_escalation",
            {"notified_at", "audience", "escalation_path", "message_summary", "evidence_artifact_ids"},
        )
        notified_at = checker.timestamp(
            communication.get("notified_at"), f"{path}.communication_escalation.notified_at"
        )
        checker.text(communication.get("audience"), f"{path}.communication_escalation.audience")
        checker.text(
            communication.get("escalation_path"),
            f"{path}.communication_escalation.escalation_path",
        )
        checker.text(
            communication.get("message_summary"),
            f"{path}.communication_escalation.message_summary",
        )
        communication_refs = _validate_references(
            checker,
            communication.get("evidence_artifact_ids"),
            f"{path}.communication_escalation.evidence_artifact_ids",
            scenario_id,
            artifacts,
        )
        referenced_artifacts.update(communication_refs)
        if communication_refs and not any(
            artifacts.get(ref, {}).get("kind") == "raw-communication" for ref in communication_refs
        ):
            checker.fail(
                f"{path}.communication_escalation.evidence_artifact_ids",
                "must reference a raw-communication artifact",
            )

        recovery = checker.obj(
            scenario.get("recovery_evidence"),
            f"{path}.recovery_evidence",
            {"recovered_at", "recovery_state", "validation", "evidence_artifact_ids"},
        )
        recovered_at = checker.timestamp(
            recovery.get("recovered_at"), f"{path}.recovery_evidence.recovered_at"
        )
        checker.text(
            recovery.get("recovery_state"),
            f"{path}.recovery_evidence.recovery_state",
            allowed={"service-restored", "threat-contained-service-restored"},
        )
        checker.text(recovery.get("validation"), f"{path}.recovery_evidence.validation")
        recovery_refs = _validate_references(
            checker,
            recovery.get("evidence_artifact_ids"),
            f"{path}.recovery_evidence.evidence_artifact_ids",
            scenario_id,
            artifacts,
        )
        referenced_artifacts.update(recovery_refs)

        canary_group = checker.obj(
            scenario.get("post_incident_canaries"),
            f"{path}.post_incident_canaries",
            {"checked_at", "canaries"},
        )
        canaries_at = checker.timestamp(
            canary_group.get("checked_at"), f"{path}.post_incident_canaries.checked_at"
        )
        canaries_raw = checker.array(
            canary_group.get("canaries"), f"{path}.post_incident_canaries.canaries", 1
        )
        canary_names: list[str] = []
        for canary_index, raw_canary in enumerate(canaries_raw):
            canary_path = f"{path}.post_incident_canaries.canaries[{canary_index}]"
            canary = checker.obj(
                raw_canary,
                canary_path,
                {"name", "result", "evidence_artifact_ids"},
            )
            name = checker.text(canary.get("name"), f"{canary_path}.name")
            if name is not None:
                canary_names.append(name)
            result = checker.text(
                canary.get("result"),
                f"{canary_path}.result",
                allowed={"pass", "fail"},
            )
            if result is not None and result != "pass":
                checker.fail(f"{canary_path}.result", "must be pass for a complete claim")
            canary_refs = _validate_references(
                checker,
                canary.get("evidence_artifact_ids"),
                f"{canary_path}.evidence_artifact_ids",
                scenario_id,
                artifacts,
            )
            referenced_artifacts.update(canary_refs)
        checker.unique(canary_names, f"{path}.post_incident_canaries.canaries[*].name")

        checker.ordered(started_at, detected_at, f"{path}.started_at", f"{path}.detection.detected_at")
        checker.ordered(detected_at, decided_at, f"{path}.detection.detected_at", f"{path}.triage.decided_at")
        checker.ordered(decided_at, mitigation_started, f"{path}.triage.decided_at", f"{path}.mitigation.started_at")
        checker.ordered(mitigation_started, mitigation_completed, f"{path}.mitigation.started_at", f"{path}.mitigation.completed_at")
        checker.ordered(mitigation_completed, safety_at, f"{path}.mitigation.completed_at", f"{path}.quorum_data_safety.checked_at")
        checker.ordered(safety_at, recovered_at, f"{path}.quorum_data_safety.checked_at", f"{path}.recovery_evidence.recovered_at")
        checker.ordered(recovered_at, canaries_at, f"{path}.recovery_evidence.recovered_at", f"{path}.post_incident_canaries.checked_at")
        checker.ordered(canaries_at, ended_at, f"{path}.post_incident_canaries.checked_at", f"{path}.ended_at")
        checker.ordered(detected_at, notified_at, f"{path}.detection.detected_at", f"{path}.communication_escalation.notified_at")
        checker.ordered(notified_at, ended_at, f"{path}.communication_escalation.notified_at", f"{path}.ended_at")
        checker.ordered(detected_at, approved_at, f"{path}.detection.detected_at", f"{path}.mitigation.authorization.approved_at")
        checker.ordered(approved_at, mitigation_started, f"{path}.mitigation.authorization.approved_at", f"{path}.mitigation.started_at")
        for acceptance_path, acceptance_time in acceptance_times:
            checker.ordered(detected_at, acceptance_time, f"{path}.detection.detected_at", acceptance_path)
            checker.ordered(acceptance_time, ended_at, acceptance_path, f"{path}.ended_at")

    checker.unique(scenario_ids, "$.scenarios[*].scenario_id")
    checker.unique(scenario_classes, "$.scenarios[*].incident_class")
    if set(scenario_classes) != SCENARIO_CLASSES:
        missing = sorted(SCENARIO_CLASSES - set(scenario_classes))
        extra = sorted(set(scenario_classes) - SCENARIO_CLASSES)
        checker.fail("$.scenarios", f"incident classes mismatch; missing={missing}, extra={extra}")

    scenario_id_set = set(scenario_ids)
    for artifact_id, artifact in artifacts.items():
        scenario_id = artifact.get("scenario_id")
        if scenario_id not in scenario_id_set:
            checker.fail(
                f"$.raw_artifacts[{artifact_id}].scenario_id",
                "does not identify one of the five scenarios",
            )
            continue
        started_at, ended_at = scenario_bounds.get(scenario_id, (None, None))
        captured_at = artifact_times.get(artifact_id)
        checker.ordered(
            started_at,
            captured_at,
            f"scenario {scenario_id} started_at",
            f"artifact {artifact_id} captured_at",
        )
        checker.ordered(
            captured_at,
            ended_at,
            f"artifact {artifact_id} captured_at",
            f"scenario {scenario_id} ended_at",
        )
    orphan_artifacts = sorted(set(artifacts) - referenced_artifacts)
    if orphan_artifacts:
        checker.fail("$.raw_artifacts", f"contains unreferenced artifacts: {orphan_artifacts}")
    for scenario_id in scenario_id_set:
        scenario_artifacts = [artifact for artifact in artifacts.values() if artifact.get("scenario_id") == scenario_id]
        kinds = {artifact.get("kind") for artifact in scenario_artifacts}
        for required_kind in {"raw-log", "raw-metric", "raw-communication"}:
            if required_kind not in kinds:
                checker.fail(
                    "$.raw_artifacts",
                    f"scenario {scenario_id} is missing {required_kind} evidence",
                )

    follow_ups_raw = checker.array(root.get("follow_up_actions"), "$.follow_up_actions", 5)
    follow_up_ids: list[str] = []
    follow_up_scenarios: list[str] = []
    for index, raw_follow_up in enumerate(follow_ups_raw):
        path = f"$.follow_up_actions[{index}]"
        follow_up = checker.obj(
            raw_follow_up,
            path,
            {
                "action_id",
                "scenario_id",
                "description",
                "owner_participant_id",
                "tracking_reference",
                "due_at",
                "status",
            },
        )
        action_id = checker.identifier(follow_up.get("action_id"), f"{path}.action_id")
        scenario_id = checker.identifier(follow_up.get("scenario_id"), f"{path}.scenario_id")
        checker.text(follow_up.get("description"), f"{path}.description")
        owner = checker.identifier(
            follow_up.get("owner_participant_id"), f"{path}.owner_participant_id"
        )
        checker.text(follow_up.get("tracking_reference"), f"{path}.tracking_reference")
        due_at = checker.timestamp(follow_up.get("due_at"), f"{path}.due_at")
        checker.text(follow_up.get("status"), f"{path}.status", allowed={"open", "closed"})
        if action_id is not None:
            follow_up_ids.append(action_id)
        if scenario_id is not None:
            follow_up_scenarios.append(scenario_id)
            if scenario_id not in scenario_id_set:
                checker.fail(f"{path}.scenario_id", "does not identify a scenario")
            else:
                checker.ordered(
                    scenario_bounds[scenario_id][1],
                    due_at,
                    f"scenario {scenario_id} ended_at",
                    f"{path}.due_at",
                )
        if owner is not None and owner not in participants:
            checker.fail(f"{path}.owner_participant_id", "does not identify a participant")
    checker.unique(follow_up_ids, "$.follow_up_actions[*].action_id")
    for scenario_id in scenario_id_set:
        if scenario_id not in follow_up_scenarios:
            checker.fail("$.follow_up_actions", f"scenario {scenario_id} has no follow-up action")

    sign_off = checker.obj(
        root.get("sign_off"), "$.sign_off", {"operator", "independent_reviewer"}
    )
    signatures: dict[str, tuple[str | None, datetime | None]] = {}
    for key, expected_role in (
        ("operator", "operator"),
        ("independent_reviewer", "independent-reviewer"),
    ):
        path = f"$.sign_off.{key}"
        signature = checker.obj(
            sign_off.get(key),
            path,
            {"participant_id", "role", "signed_at", "decision", "statement"},
        )
        participant_id = checker.identifier(signature.get("participant_id"), f"{path}.participant_id")
        role = checker.text(
            signature.get("role"),
            f"{path}.role",
            allowed={"operator", "independent-reviewer"},
        )
        signed_at = checker.timestamp(signature.get("signed_at"), f"{path}.signed_at")
        decision = checker.text(
            signature.get("decision"),
            f"{path}.decision",
            allowed={"approved", "rejected"},
        )
        checker.text(signature.get("statement"), f"{path}.statement")
        if role is not None and role != expected_role:
            checker.fail(f"{path}.role", f"must equal {expected_role}")
        if decision is not None and decision != "approved":
            checker.fail(f"{path}.decision", "must be approved for a complete claim")
        if participant_id is not None:
            if participant_id not in participants:
                checker.fail(f"{path}.participant_id", "does not identify a participant")
            elif participant_roles.get(participant_id) != expected_role:
                checker.fail(
                    f"{path}.participant_id",
                    f"participant must have role {expected_role}",
                )
        checker.ordered(latest_scenario_end, signed_at, "latest scenario ended_at", f"{path}.signed_at")
        checker.ordered(signed_at, closed_at, f"{path}.signed_at", "$.closed_at")
        signatures[key] = (participant_id, signed_at)
    operator_id = signatures.get("operator", (None, None))[0]
    reviewer_id = signatures.get("independent_reviewer", (None, None))[0]
    if operator_id is not None and operator_id == reviewer_id:
        checker.fail("$.sign_off", "operator and independent reviewer must be different participants")

    return checker.errors


def _darwin_resolve_uri(
    checker: Checker,
    raw_uri: Any,
    path: str,
    evidence_root: Path | None,
    *,
    required_prefix: str,
    verify_files: bool,
) -> Path | None:
    uri = checker.text(raw_uri, path)
    if uri is None:
        return None
    if not uri.startswith(required_prefix):
        checker.fail(path, f"must be a root-relative URI below {required_prefix}")
        return None
    relative_text = uri[len("file:") :]
    relative = PurePosixPath(relative_text)
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
        or "\\" in relative_text
        or "//" in relative_text
    ):
        checker.fail(path, "must be a normalized root-relative file URI")
        return None
    if evidence_root is None:
        checker.fail("$", "Darwin v2 verification requires --evidence-root")
        return None
    if not evidence_root.is_absolute():
        checker.fail("$", "--evidence-root must be absolute")
        return None
    if verify_files and (evidence_root.is_symlink() or not evidence_root.is_dir()):
        checker.fail("$", "--evidence-root must be an existing non-symlink directory")
        return None
    candidate = evidence_root.joinpath(*relative.parts)
    try:
        resolved_root = evidence_root.resolve(strict=verify_files)
        resolved_candidate = candidate.resolve(strict=verify_files)
        resolved_candidate.relative_to(resolved_root)
    except (OSError, RuntimeError, ValueError) as exc:
        checker.fail(path, f"must resolve inside --evidence-root: {exc}")
        return None
    if verify_files:
        current = evidence_root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                checker.fail(path, "must not traverse a symbolic link")
                return None
    return candidate


def _darwin_validate_macho(
    checker: Checker,
    binary_path: Path | None,
    expected_hash: str | None,
    *,
    verify_files: bool,
    production: bool,
) -> None:
    if binary_path is None or not verify_files:
        return
    if not binary_path.is_file():
        checker.fail("$.release_provenance.binary_uri", "must name an existing regular file")
        return
    try:
        file_size = binary_path.stat().st_size
        if _sha256_file(binary_path) != expected_hash:
            checker.fail("$.release_provenance.binary_sha256", "does not match the binary bytes")
        with binary_path.open("rb") as handle:
            header = handle.read(32)
            if production and len(header) == 32:
                command_bytes = handle.read(int.from_bytes(header[20:24], "little"))
            else:
                command_bytes = b""
    except OSError as exc:
        checker.fail("$.release_provenance.binary_uri", f"cannot be read: {exc}")
        return
    if len(header) < 12 or header[:4] != b"\xcf\xfa\xed\xfe":
        checker.fail("$.release_provenance.binary_uri", "must be a native little-endian Mach-O 64 binary")
        return
    cpu_type = int.from_bytes(header[4:8], "little")
    if cpu_type != 0x0100000C:
        checker.fail("$.release_provenance.binary_uri", "Mach-O CPU type must be arm64")
    if not production:
        return
    if file_size < 1024 * 1024:
        checker.fail(
            "$.release_provenance.binary_uri",
            "production kvnode Mach-O is truncated or implausibly small",
        )
    if len(header) != 32:
        checker.fail("$.release_provenance.binary_uri", "Mach-O 64 header is truncated")
        return
    file_type = int.from_bytes(header[12:16], "little")
    command_count = int.from_bytes(header[16:20], "little")
    command_size = int.from_bytes(header[20:24], "little")
    if file_type != 2:
        checker.fail("$.release_provenance.binary_uri", "Mach-O file type must be MH_EXECUTE")
    if command_count < 1 or command_size < 8 or 32 + command_size > file_size:
        checker.fail("$.release_provenance.binary_uri", "Mach-O load-command table is invalid")
        return
    if len(command_bytes) != command_size:
        checker.fail("$.release_provenance.binary_uri", "Mach-O load-command table is truncated")
        return
    offset = 0
    text_segment = False
    entrypoint = False
    for command_index in range(command_count):
        if offset + 8 > len(command_bytes):
            checker.fail(
                "$.release_provenance.binary_uri",
                f"Mach-O load command {command_index} is truncated",
            )
            return
        command = int.from_bytes(command_bytes[offset : offset + 4], "little")
        size = int.from_bytes(command_bytes[offset + 4 : offset + 8], "little")
        if size < 8 or size % 4 != 0 or offset + size > len(command_bytes):
            checker.fail(
                "$.release_provenance.binary_uri",
                f"Mach-O load command {command_index} has an invalid size",
            )
            return
        if command == 0x19 and size >= 72:
            segment_name = command_bytes[offset + 8 : offset + 24].split(b"\x00", 1)[0]
            segment_file_size = int.from_bytes(
                command_bytes[offset + 48 : offset + 56],
                "little",
            )
            initial_protection = int.from_bytes(
                command_bytes[offset + 60 : offset + 64],
                "little",
            )
            if (
                segment_name == b"__TEXT"
                and segment_file_size > 0
                and segment_file_size <= file_size
                and initial_protection & 0x4
            ):
                text_segment = True
        if command in {0x5, 0x80000028}:
            entrypoint = True
        offset += size
    if offset != command_size:
        checker.fail("$.release_provenance.binary_uri", "Mach-O load-command sizes do not close")
    if not text_segment:
        checker.fail("$.release_provenance.binary_uri", "Mach-O has no executable __TEXT segment")
    if not entrypoint:
        checker.fail("$.release_provenance.binary_uri", "Mach-O has no executable entry point")
    try:
        build_info_found = False
        with binary_path.open("rb") as handle:
            carry = b""
            while True:
                block = handle.read(1024 * 1024)
                if not block:
                    break
                candidate = carry + block
                if b"\xff Go buildinf:" in candidate:
                    build_info_found = True
                    break
                carry = candidate[-32:]
        if not build_info_found:
            checker.fail(
                "$.release_provenance.binary_uri",
                "production kvnode must contain Go build provenance",
            )
    except OSError as exc:
        checker.fail("$.release_provenance.binary_uri", f"cannot inspect Go build provenance: {exc}")


def _darwin_validate_local_binary(
    checker: Checker,
    binary_path: Path | None,
    source_revision: str | None,
    *,
    production: bool,
    verify_files: bool,
) -> None:
    if not production or not verify_files or binary_path is None:
        return
    if sys.platform != "darwin":
        checker.fail("$", "Darwin v2 production binary checks require a native Darwin verifier")
        return
    try:
        if binary_path.stat().st_mode & 0o111 == 0:
            checker.fail("$.release_provenance.binary_uri", "production kvnode must be executable")
            return
    except OSError as exc:
        checker.fail("$.release_provenance.binary_uri", f"cannot inspect executable mode: {exc}")
        return
    commands = (
        (
            "file",
            ["/usr/bin/file", "-b", str(binary_path)],
            lambda output: "Mach-O 64-bit executable arm64" in output,
            "must be identified by file as a Mach-O 64-bit arm64 executable",
        ),
        (
            "lipo",
            ["/usr/bin/lipo", "-verify_arch", "arm64", str(binary_path)],
            lambda _output: True,
            "must pass lipo arm64 architecture verification",
        ),
        (
            "otool",
            ["/usr/bin/otool", "-hv", str(binary_path)],
            lambda output: "ARM64" in output.upper(),
            "must be loadable by otool as arm64 Mach-O",
        ),
        (
            "codesign",
            ["/usr/bin/codesign", "--verify", "--strict", "--verbose=2", str(binary_path)],
            lambda _output: True,
            "must pass local Darwin code-signature verification",
        ),
        (
            "go-version-m",
            ["/usr/bin/env", "go", "version", "-m", str(binary_path)],
            lambda output: (
                "build\tGOOS=darwin" in output
                and "build\tGOARCH=arm64" in output
                and "build\tvcs.modified=false" in output
                and source_revision is not None
                and f"build\tvcs.revision={source_revision}" in output
            ),
            "must contain matching clean Darwin arm64 Go VCS provenance",
        ),
    )
    for command_name, argv, predicate, failure in commands:
        try:
            completed = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            checker.fail(
                "$.release_provenance.binary_uri",
                f"{command_name} validation could not run: {exc}",
            )
            continue
        output = completed.stdout + completed.stderr
        if completed.returncode != 0 or not predicate(output):
            checker.fail("$.release_provenance.binary_uri", failure)


def _validate_darwin_release_manifest(
    checker: Checker,
    provenance: dict[str, Any],
    identity: dict[str, str | None],
    evidence_root: Path | None,
    *,
    test_mode: bool,
    verify_files: bool,
) -> datetime | None:
    manifest_path = _darwin_resolve_uri(
        checker,
        provenance.get("release_manifest_uri"),
        "$.release_provenance.release_manifest_uri",
        evidence_root,
        required_prefix="file:manifest/",
        verify_files=verify_files,
    )
    expected_hash = _darwin_sha(
        checker,
        provenance.get("release_manifest_sha256"),
        "$.release_provenance.release_manifest_sha256",
    )
    if manifest_path is None or not verify_files:
        return None
    if not manifest_path.is_file():
        checker.fail(
            "$.release_provenance.release_manifest_uri",
            "must name an existing regular release manifest",
        )
        return None
    try:
        if _sha256_file(manifest_path) != expected_hash:
            checker.fail(
                "$.release_provenance.release_manifest_sha256",
                "does not match the release manifest bytes",
            )
        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest = json.load(
                handle,
                object_pairs_hook=_object_pairs_without_duplicates,
                parse_constant=lambda token: (_ for _ in ()).throw(
                    ValueError(f"invalid JSON numeric constant: {token}")
                ),
            )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        checker.fail(
            "$.release_provenance.release_manifest_uri",
            f"must contain a readable release manifest: {exc}",
        )
        return None
    manifest = checker.obj(manifest, "$.release_provenance.manifest", DARWIN_RELEASE_MANIFEST_KEYS)
    checker.text(
        manifest.get("manifest_version"),
        "$.release_provenance.manifest.manifest_version",
        allowed={"incident-release-manifest-v2"},
    )
    checker.text(
        manifest.get("verifier_version"),
        "$.release_provenance.manifest.verifier_version",
        allowed={DARWIN_VERIFIER_VERSION},
    )
    for manifest_key, identity_key in (
        ("record_mode", "record_mode"),
        ("target_id", "target_id"),
        ("release_id", "release_id"),
        ("source_revision", "source_revision"),
        ("binary_sha256", "binary_sha256"),
        ("environment", "environment"),
    ):
        value = checker.text(
            manifest.get(manifest_key),
            f"$.release_provenance.manifest.{manifest_key}",
        )
        if value is not None and value != identity.get(identity_key):
            checker.fail(
                f"$.release_provenance.manifest.{manifest_key}",
                f"does not match record {identity_key}",
            )
    binary_uri = checker.text(
        manifest.get("binary_uri"),
        "$.release_provenance.manifest.binary_uri",
    )
    if binary_uri is not None and binary_uri != provenance.get("binary_uri"):
        checker.fail(
            "$.release_provenance.manifest.binary_uri",
            "does not match release_provenance.binary_uri",
        )
    required_origin = "synthetic-self-test" if test_mode else "native-darwin-build"
    checker.text(
        manifest.get("origin"),
        "$.release_provenance.manifest.origin",
        allowed={required_origin},
    )
    for key, required in (
        ("platform", "darwin"),
        ("architecture", "arm64"),
        ("binary_format", "mach-o-64"),
        ("codesign_requirement", "valid-adhoc-or-identified"),
    ):
        checker.text(
            manifest.get(key),
            f"$.release_provenance.manifest.{key}",
            allowed={required},
        )
    build_command = checker.text(
        manifest.get("build_command"),
        "$.release_provenance.manifest.build_command",
    )
    if build_command is not None:
        for required_token in (
            "GOOS=darwin",
            "GOARCH=arm64",
            "CGO_ENABLED=0",
            "go build",
            "-trimpath",
            "-buildvcs=true",
            "-tags kvnode",
        ):
            if required_token not in build_command:
                checker.fail(
                    "$.release_provenance.manifest.build_command",
                    f"must include {required_token}",
                )
    go_version = checker.text(
        manifest.get("go_version"),
        "$.release_provenance.manifest.go_version",
    )
    if go_version is not None and re.fullmatch(r"go1\.[0-9]+(?:\.[0-9]+)?", go_version) is None:
        checker.fail("$.release_provenance.manifest.go_version", "must be a concrete Go version")
    _darwin_boolean(
        checker,
        manifest.get("vcs_modified"),
        "$.release_provenance.manifest.vcs_modified",
        False,
    )
    return checker.timestamp(
        manifest.get("created_at"),
        "$.release_provenance.manifest.created_at",
    )


def _darwin_narrative(checker: Checker, value: Any, path: str) -> str | None:
    text = checker.text(value, path)
    if text is not None and len(text) < 20:
        checker.fail(path, "must describe a concrete observed condition, command, or result")
    return text


def _darwin_boolean(checker: Checker, value: Any, path: str, expected: bool) -> None:
    observed = checker.boolean(value, path)
    if observed is not None and observed is not expected:
        checker.fail(path, f"must equal {str(expected).lower()}")


def _darwin_integer(
    checker: Checker,
    value: Any,
    path: str,
    *,
    minimum: int = 0,
) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        checker.fail(path, f"must be an integer greater than or equal to {minimum}")
        return None
    return value


def _darwin_sha(checker: Checker, value: Any, path: str) -> str | None:
    return checker.text(value, path, pattern=SHA256_PATTERN)


def _validate_darwin_observations(
    checker: Checker,
    incident_class: str | None,
    raw_observations: Any,
    path: str,
) -> None:
    if incident_class == "process_crash_restart":
        keys = {
            "node",
            "launchd_label",
            "crash_signal",
            "old_pid",
            "new_pid",
            "supervisor_restart_observed",
            "durable_canary_observed",
        }
        observations = checker.obj(raw_observations, path, keys)
        checker.text(observations.get("node"), f"{path}.node", allowed={"node2"})
        checker.text(
            observations.get("launchd_label"),
            f"{path}.launchd_label",
            allowed={DARWIN_NODE_CONTRACT["node2"]["launchd_label"]},
        )
        checker.text(observations.get("crash_signal"), f"{path}.crash_signal", allowed={"SIGKILL"})
        old_pid = _darwin_integer(checker, observations.get("old_pid"), f"{path}.old_pid", minimum=1)
        new_pid = _darwin_integer(checker, observations.get("new_pid"), f"{path}.new_pid", minimum=1)
        if old_pid is not None and old_pid == new_pid:
            checker.fail(f"{path}.new_pid", "must differ from old_pid")
        _darwin_boolean(
            checker,
            observations.get("supervisor_restart_observed"),
            f"{path}.supervisor_restart_observed",
            True,
        )
        _darwin_boolean(
            checker,
            observations.get("durable_canary_observed"),
            f"{path}.durable_canary_observed",
            True,
        )
        return
    if incident_class == "one_node_unavailability":
        keys = {
            "unavailable_node",
            "healthy_nodes",
            "expected_voters",
            "available_voters",
            "quorum_write_observed",
            "cross_node_read_observed",
        }
        observations = checker.obj(raw_observations, path, keys)
        checker.text(
            observations.get("unavailable_node"),
            f"{path}.unavailable_node",
            allowed={"node2"},
        )
        healthy = checker.array(observations.get("healthy_nodes"), f"{path}.healthy_nodes", 2)
        if healthy != ["node1", "node3"]:
            checker.fail(f"{path}.healthy_nodes", "must equal ['node1', 'node3']")
        expected = _darwin_integer(
            checker, observations.get("expected_voters"), f"{path}.expected_voters", minimum=1
        )
        available = _darwin_integer(
            checker, observations.get("available_voters"), f"{path}.available_voters", minimum=1
        )
        if expected is not None and expected != 3:
            checker.fail(f"{path}.expected_voters", "must equal 3")
        if available is not None and available != 2:
            checker.fail(f"{path}.available_voters", "must equal 2")
        _darwin_boolean(
            checker,
            observations.get("quorum_write_observed"),
            f"{path}.quorum_write_observed",
            True,
        )
        _darwin_boolean(
            checker,
            observations.get("cross_node_read_observed"),
            f"{path}.cross_node_read_observed",
            True,
        )
        return
    if incident_class == "bad_config_rollback":
        keys = {
            "node",
            "launchd_label",
            "invalid_config_sha256",
            "last_known_good_sha256",
            "validation_rejected",
            "rollback_completed",
            "service_restored",
        }
        observations = checker.obj(raw_observations, path, keys)
        checker.text(observations.get("node"), f"{path}.node", allowed={"node2"})
        checker.text(
            observations.get("launchd_label"),
            f"{path}.launchd_label",
            allowed={DARWIN_NODE_CONTRACT["node2"]["launchd_label"]},
        )
        invalid_hash = _darwin_sha(
            checker, observations.get("invalid_config_sha256"), f"{path}.invalid_config_sha256"
        )
        good_hash = _darwin_sha(
            checker,
            observations.get("last_known_good_sha256"),
            f"{path}.last_known_good_sha256",
        )
        if invalid_hash is not None and invalid_hash == good_hash:
            checker.fail(f"{path}.invalid_config_sha256", "must differ from last_known_good_sha256")
        for key in ("validation_rejected", "rollback_completed", "service_restored"):
            _darwin_boolean(checker, observations.get(key), f"{path}.{key}", True)
        return
    if incident_class == "certificate_secret_rotation":
        keys = {
            "nodes_rotated",
            "rotation_scope",
            "reload_method",
            "old_certificate_sha256",
            "new_certificate_sha256",
            "old_private_key_sha256",
            "new_private_key_sha256",
            "private_key_material_collected",
            "tls_server_auth_verified",
            "mtls_observed",
            "client_authorization_observed",
        }
        observations = checker.obj(raw_observations, path, keys)
        nodes = checker.array(observations.get("nodes_rotated"), f"{path}.nodes_rotated", 3)
        if nodes != ["node1", "node2", "node3"]:
            checker.fail(f"{path}.nodes_rotated", "must equal ['node1', 'node2', 'node3']")
        checker.text(
            observations.get("rotation_scope"),
            f"{path}.rotation_scope",
            allowed={"server-certificate-and-private-key"},
        )
        checker.text(
            observations.get("reload_method"),
            f"{path}.reload_method",
            allowed={"rolling-launchd-restart"},
        )
        old_certificate = _darwin_sha(
            checker,
            observations.get("old_certificate_sha256"),
            f"{path}.old_certificate_sha256",
        )
        new_certificate = _darwin_sha(
            checker,
            observations.get("new_certificate_sha256"),
            f"{path}.new_certificate_sha256",
        )
        old_key = _darwin_sha(
            checker,
            observations.get("old_private_key_sha256"),
            f"{path}.old_private_key_sha256",
        )
        new_key = _darwin_sha(
            checker,
            observations.get("new_private_key_sha256"),
            f"{path}.new_private_key_sha256",
        )
        if old_certificate is not None and old_certificate == new_certificate:
            checker.fail(f"{path}.new_certificate_sha256", "must differ from old certificate")
        if old_key is not None and old_key == new_key:
            checker.fail(f"{path}.new_private_key_sha256", "must differ from old private key")
        _darwin_boolean(
            checker,
            observations.get("private_key_material_collected"),
            f"{path}.private_key_material_collected",
            False,
        )
        _darwin_boolean(
            checker,
            observations.get("tls_server_auth_verified"),
            f"{path}.tls_server_auth_verified",
            True,
        )
        _darwin_boolean(
            checker, observations.get("mtls_observed"), f"{path}.mtls_observed", False
        )
        _darwin_boolean(
            checker,
            observations.get("client_authorization_observed"),
            f"{path}.client_authorization_observed",
            False,
        )
        return
    if incident_class == "storage_pressure_failure":
        keys = {
            "node",
            "failure_mode",
            "apfs_free_bytes_before",
            "apfs_free_bytes_after",
            "storage_fault_metric_observed",
            "readiness_failed_observed",
            "quorum_service_observed",
            "physical_apfs_failure_observed",
            "fault_gate_cleared",
        }
        observations = checker.obj(raw_observations, path, keys)
        checker.text(observations.get("node"), f"{path}.node", allowed={"node2"})
        checker.text(
            observations.get("failure_mode"),
            f"{path}.failure_mode",
            allowed={"logical-storage-unavailable-gate-with-apfs-free-space-observation"},
        )
        _darwin_integer(
            checker,
            observations.get("apfs_free_bytes_before"),
            f"{path}.apfs_free_bytes_before",
            minimum=1,
        )
        _darwin_integer(
            checker,
            observations.get("apfs_free_bytes_after"),
            f"{path}.apfs_free_bytes_after",
            minimum=1,
        )
        for key in (
            "storage_fault_metric_observed",
            "readiness_failed_observed",
            "quorum_service_observed",
            "fault_gate_cleared",
        ):
            _darwin_boolean(checker, observations.get(key), f"{path}.{key}", True)
        _darwin_boolean(
            checker,
            observations.get("physical_apfs_failure_observed"),
            f"{path}.physical_apfs_failure_observed",
            False,
        )
        return
    if incident_class == "corrupted_checkpoint":
        keys = {
            "node",
            "checkpoint_mode",
            "pristine_manifest_sha256",
            "altered_manifest_sha256",
            "node_stopped_before_copy",
            "altered_copy_rejected",
            "quarantine_path",
            "pristine_reverified",
            "suspect_copy_restored",
            "service_restored_from_pristine",
        }
        observations = checker.obj(raw_observations, path, keys)
        checker.text(observations.get("node"), f"{path}.node", allowed={"node2"})
        checker.text(
            observations.get("checkpoint_mode"),
            f"{path}.checkpoint_mode",
            allowed={"offline-altered-copy"},
        )
        pristine_hash = _darwin_sha(
            checker,
            observations.get("pristine_manifest_sha256"),
            f"{path}.pristine_manifest_sha256",
        )
        altered_hash = _darwin_sha(
            checker,
            observations.get("altered_manifest_sha256"),
            f"{path}.altered_manifest_sha256",
        )
        if pristine_hash is not None and pristine_hash == altered_hash:
            checker.fail(f"{path}.altered_manifest_sha256", "must differ from pristine manifest")
        quarantine = checker.text(
            observations.get("quarantine_path"), f"{path}.quarantine_path"
        )
        if quarantine is not None and (
            not quarantine.startswith("/var/db/moreconsensus/")
            or "/quarantine/" not in quarantine
            or ".." in PurePosixPath(quarantine).parts
        ):
            checker.fail(
                f"{path}.quarantine_path",
                "must be an absolute APFS campaign quarantine path below /var/db/moreconsensus",
            )
        for key in (
            "node_stopped_before_copy",
            "altered_copy_rejected",
            "pristine_reverified",
            "service_restored_from_pristine",
        ):
            _darwin_boolean(checker, observations.get(key), f"{path}.{key}", True)
        _darwin_boolean(
            checker,
            observations.get("suspect_copy_restored"),
            f"{path}.suspect_copy_restored",
            False,
        )
        return
    checker.obj(raw_observations, path, set())


def _validate_darwin_raw_envelope(
    checker: Checker,
    artifact_path: Path | None,
    artifact: dict[str, Any],
    artifact_index: int,
    identity: dict[str, str | None],
    *,
    verify_files: bool,
) -> None:
    path = f"$.raw_artifacts[{artifact_index}]"
    if artifact_path is None or not verify_files:
        return
    if not artifact_path.is_file():
        checker.fail(f"{path}.uri", "must name an existing regular file")
        return
    try:
        if artifact_path.stat().st_size == 0:
            checker.fail(f"{path}.uri", "must not be empty")
            return
        if _sha256_file(artifact_path) != artifact.get("sha256"):
            checker.fail(f"{path}.sha256", "does not match the raw artifact bytes")
        with artifact_path.open("r", encoding="utf-8") as handle:
            envelope = json.load(
                handle,
                object_pairs_hook=_object_pairs_without_duplicates,
                parse_constant=lambda token: (_ for _ in ()).throw(
                    ValueError(f"invalid JSON numeric constant: {token}")
                ),
            )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        checker.fail(f"{path}.uri", f"must contain a readable raw v2 JSON envelope: {exc}")
        return
    envelope = checker.obj(envelope, f"{path}.envelope", DARWIN_RAW_ENVELOPE_KEYS)
    checker.text(
        envelope.get("artifact_version"),
        f"{path}.envelope.artifact_version",
        allowed={"incident-raw-v2"},
    )
    checker.text(
        envelope.get("verifier_version"),
        f"{path}.envelope.verifier_version",
        allowed={DARWIN_VERIFIER_VERSION},
    )
    for envelope_key, identity_key in (
        ("target_id", "target_id"),
        ("release_id", "release_id"),
        ("source_revision", "source_revision"),
        ("binary_sha256", "binary_sha256"),
        ("environment", "environment"),
        ("record_mode", "record_mode"),
    ):
        value = checker.text(envelope.get(envelope_key), f"{path}.envelope.{envelope_key}")
        if value is not None and value != identity.get(identity_key):
            checker.fail(
                f"{path}.envelope.{envelope_key}",
                f"does not match record {identity_key}",
            )
    drill_id = checker.text(envelope.get("drill_id"), f"{path}.envelope.drill_id")
    if drill_id is not None and drill_id != artifact.get("drill_id"):
        checker.fail(f"{path}.envelope.drill_id", "does not match raw artifact drill_id")
    observed_at = checker.timestamp(envelope.get("observed_at"), f"{path}.envelope.observed_at")
    captured_at = checker.timestamp(artifact.get("captured_at"), f"{path}.captured_at")
    if observed_at is not None and captured_at is not None and observed_at != captured_at:
        checker.fail(f"{path}.envelope.observed_at", "must exactly match captured_at")
    command = checker.text(envelope.get("command"), f"{path}.envelope.command")
    output = checker.text(envelope.get("output"), f"{path}.envelope.output")
    exit_code = _darwin_integer(
        checker, envelope.get("exit_code"), f"{path}.envelope.exit_code", minimum=0
    )
    result = checker.text(
        envelope.get("result"),
        f"{path}.envelope.result",
        allowed={"observed-success", "expected-rejection", "observed-approval"},
    )
    if result == "expected-rejection":
        if exit_code == 0:
            checker.fail(f"{path}.envelope.exit_code", "must be nonzero for expected-rejection")
    elif result is not None and exit_code is not None and exit_code != 0:
        checker.fail(f"{path}.envelope.exit_code", "must be zero for successful observations")
    if command is not None and len(command) < 8:
        checker.fail(f"{path}.envelope.command", "must record the observed command")
    if output is not None:
        normalized = re.sub(r"[^a-z0-9]+", "", output.casefold())
        if len(output) < 40 or normalized in {"pass", "passed", "ok", "success", "verified"}:
            checker.fail(
                f"{path}.envelope.output",
                "must contain raw observed output, not a marker-only pass record",
            )
    for field_name, value in (("command", command), ("output", output)):
        if value is None:
            continue
        if DARWIN_LINUX_OR_CONTAINER_MARKER.search(value) is not None:
            checker.fail(
                f"{path}.envelope.{field_name}",
                "contains a Linux, systemd, container, or VM artifact prohibited by Darwin v2",
            )
        if DARWIN_FALSE_CLAIM_MARKER.search(value) is not None:
            checker.fail(
                f"{path}.envelope.{field_name}",
                "contains a production claim outside the same-host Darwin profile",
            )
        for address in IPV4_PATTERN.findall(value):
            if address != "127.0.0.1":
                checker.fail(
                    f"{path}.envelope.{field_name}",
                    f"contains non-loopback peer address {address}",
                )


def _validate_darwin_v2_document(
    document: dict[str, Any],
    *,
    test_mode: bool,
    expected_target: str | None,
    expected_release_id: str | None,
    expected_source_revision: str | None,
    expected_binary_sha256: str | None,
    expected_environment: str | None,
    evidence_root: Path | None,
    now: datetime | None,
    verify_files: bool,
) -> list[str]:
    checker = Checker()
    root = checker.obj(document, "$", DARWIN_TOP_LEVEL_KEYS)
    checker.text(root.get("schema_version"), "$.schema_version", allowed={DARWIN_SCHEMA_VERSION})
    checker.text(
        root.get("verifier_version"),
        "$.verifier_version",
        allowed={DARWIN_VERIFIER_VERSION},
    )
    checker.text(
        root.get("record_kind"),
        "$.record_kind",
        allowed={"target-incident-evidence"},
    )
    record_mode = checker.text(
        root.get("record_mode"),
        "$.record_mode",
        allowed={"target", "synthetic-test"},
    )
    claim = checker.text(root.get("claim"), "$.claim")
    if test_mode:
        if record_mode is not None and record_mode != "synthetic-test":
            checker.fail("$.record_mode", "test mode accepts only synthetic-test records")
        if claim != "synthetic-test-darwin-incident-readiness-observed":
            checker.fail("$.claim", "must be the Darwin v2 synthetic test claim")
    else:
        if record_mode is not None and record_mode != "target":
            checker.fail("$.record_mode", "production verification accepts only target records")
        if claim != "target-darwin-incident-readiness-observed":
            checker.fail("$.claim", "must equal target-darwin-incident-readiness-observed")
        for option, label in (
            (expected_target, "--expected-target"),
            (expected_release_id, "--expected-release-id"),
            (expected_source_revision, "--expected-source-revision"),
            (expected_binary_sha256, "--expected-binary-sha256"),
            (expected_environment, "--expected-environment"),
        ):
            if option is None:
                checker.fail("$", f"Darwin v2 production verification requires {label}")
        if evidence_root is None:
            checker.fail("$", "Darwin v2 production verification requires --evidence-root")
        for string_path, value in _walk_strings(root):
            if re.search(
                r"(?:^|[^a-z0-9])(?:placeholder|tbd|todo|synthetic|fixture|"
                r"tabletop|mock|fake)(?:$|[^a-z0-9])",
                value,
                re.IGNORECASE,
            ):
                checker.fail(string_path, "contains a synthetic, tabletop, or placeholder marker")
            if DARWIN_LINUX_OR_CONTAINER_MARKER.search(value) is not None:
                checker.fail(
                    string_path,
                    "contains a Linux, systemd, container, or VM artifact prohibited by Darwin v2",
                )
            if ".nonclaims" not in string_path and DARWIN_FALSE_CLAIM_MARKER.search(value):
                checker.fail(
                    string_path,
                    "contains a production claim outside the same-host Darwin profile",
                )

    target_keys = {"name", "environment", "service", "cluster_id"}
    target = checker.obj(root.get("target"), "$.target", target_keys)
    target_name = checker.text(target.get("name"), "$.target.name")
    environment = checker.text(target.get("environment"), "$.target.environment")
    checker.text(target.get("service"), "$.target.service", allowed={"kvnode"})
    checker.text(target.get("cluster_id"), "$.target.cluster_id", allowed={DARWIN_CLUSTER_ID})
    if target_name is not None and target_name != DARWIN_TARGET_ID:
        checker.fail("$.target.name", f"must equal {DARWIN_TARGET_ID}")
    if environment is not None and environment != DARWIN_ENVIRONMENT:
        checker.fail("$.target.environment", f"must equal {DARWIN_ENVIRONMENT}")
    if not test_mode and expected_target is not None and target_name != expected_target:
        checker.fail("$.target.name", "does not match --expected-target")
    if not test_mode and expected_environment is not None and environment != expected_environment:
        checker.fail("$.target.environment", "does not match --expected-environment")

    profile_keys = {
        "platform",
        "os_version",
        "os_build",
        "architecture",
        "binary_format",
        "execution_mode",
        "supervisor",
        "launchd_domain",
        "storage_filesystem",
        "network_scope",
        "tls_mode",
    }
    profile = checker.obj(root.get("profile"), "$.profile", profile_keys)
    for key, required in (
        ("platform", "darwin"),
        ("architecture", "arm64"),
        ("binary_format", "mach-o-64"),
        ("execution_mode", "native"),
        ("supervisor", "launchd"),
        ("launchd_domain", "system"),
        ("storage_filesystem", "apfs"),
        ("network_scope", "same-host-loopback"),
        ("tls_mode", "server-auth-only"),
    ):
        checker.text(profile.get(key), f"$.profile.{key}", allowed={required})
    os_version = checker.text(profile.get("os_version"), "$.profile.os_version")
    if os_version is not None and re.fullmatch(r"24\.[0-9]+\.[0-9]+", os_version) is None:
        checker.fail("$.profile.os_version", "must identify Darwin 24")
    checker.text(profile.get("os_build"), "$.profile.os_build")

    nonclaims = checker.obj(root.get("nonclaims"), "$.nonclaims", DARWIN_NONCLAIMS)
    for key in DARWIN_NONCLAIMS:
        _darwin_boolean(checker, nonclaims.get(key), f"$.nonclaims.{key}", False)

    topology = checker.obj(root.get("topology"), "$.topology", {"node_count", "nodes"})
    node_count = _darwin_integer(checker, topology.get("node_count"), "$.topology.node_count", minimum=1)
    if node_count is not None and node_count != 3:
        checker.fail("$.topology.node_count", "must equal 3")
    nodes_raw = checker.array(topology.get("nodes"), "$.topology.nodes", 3)
    if len(nodes_raw) != 3:
        checker.fail("$.topology.nodes", "must contain exactly three nodes")
    observed_nodes: list[str] = []
    observed_pids: list[int] = []
    topology_times: list[datetime | None] = []
    topology_artifact_refs: list[str] = []

    provenance_keys = {
        "release_id",
        "source_repository",
        "source_revision",
        "source_tree",
        "binary_uri",
        "binary_sha256",
        "release_manifest_uri",
        "release_manifest_sha256",
        "built_at",
    }
    provenance = checker.obj(root.get("release_provenance"), "$.release_provenance", provenance_keys)
    release_id = checker.identifier(provenance.get("release_id"), "$.release_provenance.release_id")
    checker.text(provenance.get("source_repository"), "$.release_provenance.source_repository")
    source_revision = checker.text(
        provenance.get("source_revision"),
        "$.release_provenance.source_revision",
        pattern=REVISION_PATTERN,
    )
    checker.text(
        provenance.get("source_tree"),
        "$.release_provenance.source_tree",
        allowed={"clean"},
    )
    binary_hash = _darwin_sha(
        checker,
        provenance.get("binary_sha256"),
        "$.release_provenance.binary_sha256",
    )
    binary_path = _darwin_resolve_uri(
        checker,
        provenance.get("binary_uri"),
        "$.release_provenance.binary_uri",
        evidence_root,
        required_prefix="file:binary/",
        verify_files=verify_files,
    )
    _darwin_validate_macho(
        checker,
        binary_path,
        binary_hash,
        verify_files=verify_files,
        production=not test_mode,
    )
    _darwin_validate_local_binary(
        checker,
        binary_path,
        source_revision,
        production=not test_mode,
        verify_files=verify_files,
    )
    built_at = checker.timestamp(provenance.get("built_at"), "$.release_provenance.built_at")
    release_identity = {
        "record_mode": record_mode,
        "target_id": target_name,
        "release_id": release_id,
        "source_revision": source_revision,
        "binary_sha256": binary_hash,
        "environment": environment,
    }
    manifest_created_at = _validate_darwin_release_manifest(
        checker,
        provenance,
        release_identity,
        evidence_root,
        test_mode=test_mode,
        verify_files=verify_files,
    )
    if not test_mode:
        if expected_release_id is not None and release_id != expected_release_id:
            checker.fail("$.release_provenance.release_id", "does not match --expected-release-id")
        if expected_source_revision is not None and source_revision != expected_source_revision:
            checker.fail(
                "$.release_provenance.source_revision",
                "does not match --expected-source-revision",
            )
        if expected_binary_sha256 is not None and binary_hash != expected_binary_sha256:
            checker.fail(
                "$.release_provenance.binary_sha256",
                "does not match --expected-binary-sha256",
            )

    for index, raw_node in enumerate(nodes_raw):
        path = f"$.topology.nodes[{index}]"
        node = checker.obj(
            raw_node,
            path,
            {
                "name",
                "node_id",
                "launchd_label",
                "client_endpoint",
                "peer_endpoint",
                "admin_endpoint",
                "data_path",
                "pid",
                "binary_sha256",
                "observed_at",
                "evidence_artifact_id",
            },
        )
        name = checker.text(node.get("name"), f"{path}.name")
        if name is not None:
            observed_nodes.append(name)
        expected_node = DARWIN_NODE_CONTRACT.get(name or "")
        if expected_node is None:
            checker.fail(f"{path}.name", "must identify node1, node2, or node3")
        else:
            node_id = _darwin_integer(checker, node.get("node_id"), f"{path}.node_id", minimum=1)
            if node_id is not None and node_id != expected_node["node_id"]:
                checker.fail(f"{path}.node_id", f"must equal {expected_node['node_id']}")
            for key in (
                "launchd_label",
                "client_endpoint",
                "peer_endpoint",
                "admin_endpoint",
            ):
                value = checker.text(node.get(key), f"{path}.{key}")
                if value is not None and value != expected_node[key]:
                    checker.fail(f"{path}.{key}", f"must equal {expected_node[key]}")
        data_path = checker.text(node.get("data_path"), f"{path}.data_path")
        if data_path is not None and (
            not data_path.startswith("/var/db/moreconsensus/")
            or f"/data/{name}" not in data_path
            or ".." in PurePosixPath(data_path).parts
        ):
            checker.fail(
                f"{path}.data_path",
                "must be the node APFS data path below /var/db/moreconsensus",
            )
        pid = _darwin_integer(checker, node.get("pid"), f"{path}.pid", minimum=1)
        if pid is not None:
            observed_pids.append(pid)
        node_hash = _darwin_sha(checker, node.get("binary_sha256"), f"{path}.binary_sha256")
        if node_hash is not None and binary_hash is not None and node_hash != binary_hash:
            checker.fail(f"{path}.binary_sha256", "does not match release binary")
        topology_times.append(checker.timestamp(node.get("observed_at"), f"{path}.observed_at"))
        topology_artifact_ref = checker.identifier(
            node.get("evidence_artifact_id"),
            f"{path}.evidence_artifact_id",
        )
        if topology_artifact_ref is not None:
            topology_artifact_refs.append(topology_artifact_ref)
    if observed_nodes != ["node1", "node2", "node3"]:
        checker.fail("$.topology.nodes[*].name", "must be ordered exactly node1, node2, node3")
    checker.unique(observed_pids, "$.topology.nodes[*].pid")
    checker.unique(topology_artifact_refs, "$.topology.nodes[*].evidence_artifact_id")

    participants_raw = checker.array(root.get("participants"), "$.participants", 3)
    participants: dict[str, dict[str, Any]] = {}
    participant_roles: dict[str, str] = {}
    participant_names: list[str] = []
    for index, raw_participant in enumerate(participants_raw):
        path = f"$.participants[{index}]"
        participant = checker.obj(
            raw_participant,
            path,
            {"participant_id", "name", "role", "organization"},
        )
        participant_id = checker.identifier(participant.get("participant_id"), f"{path}.participant_id")
        name = checker.text(participant.get("name"), f"{path}.name")
        role = checker.text(
            participant.get("role"),
            f"{path}.role",
            allowed={"operator", "independent-reviewer", "incident-commander"},
        )
        checker.text(participant.get("organization"), f"{path}.organization")
        if participant_id is not None:
            if participant_id in participants:
                checker.fail(f"{path}.participant_id", "duplicates an earlier participant")
            participants[participant_id] = participant
            if role is not None:
                participant_roles[participant_id] = role
        if name is not None:
            participant_names.append(name.casefold())
    checker.unique(participant_names, "$.participants[*].name")
    for role in {"operator", "independent-reviewer", "incident-commander"}:
        if role not in participant_roles.values():
            checker.fail("$.participants", f"must include role {role}")

    opened_at = checker.timestamp(root.get("opened_at"), "$.opened_at")
    closed_at = checker.timestamp(root.get("closed_at"), "$.closed_at")
    recorded_at = checker.timestamp(root.get("recorded_at"), "$.recorded_at")
    valid_until = checker.timestamp(root.get("valid_until"), "$.valid_until")
    checker.ordered(built_at, opened_at, "$.release_provenance.built_at", "$.opened_at")
    checker.ordered(
        built_at,
        manifest_created_at,
        "$.release_provenance.built_at",
        "$.release_provenance.manifest.created_at",
    )
    checker.ordered(
        manifest_created_at,
        opened_at,
        "$.release_provenance.manifest.created_at",
        "$.opened_at",
    )
    checker.ordered(opened_at, closed_at, "$.opened_at", "$.closed_at")
    checker.ordered(closed_at, recorded_at, "$.closed_at", "$.recorded_at")
    checker.ordered(recorded_at, valid_until, "$.recorded_at", "$.valid_until")
    for index, topology_time in enumerate(topology_times):
        checker.ordered(opened_at, topology_time, "$.opened_at", f"$.topology.nodes[{index}].observed_at")
        checker.ordered(
            topology_time,
            closed_at,
            f"$.topology.nodes[{index}].observed_at",
            "$.closed_at",
        )
    if recorded_at is not None and valid_until is not None:
        if valid_until - recorded_at > timedelta(days=30):
            checker.fail("$.valid_until", "must be no more than 30 days after recorded_at")
    check_now = now or datetime.now(timezone.utc)
    if not test_mode:
        if valid_until is not None and valid_until < check_now:
            checker.fail("$.valid_until", "record is stale")
        if recorded_at is not None and recorded_at > check_now + timedelta(minutes=5):
            checker.fail("$.recorded_at", "must not be more than five minutes in the future")

    identity = {
        "target_id": target_name,
        "release_id": release_id,
        "source_revision": source_revision,
        "binary_sha256": binary_hash,
        "environment": environment,
        "record_mode": record_mode,
    }
    artifacts_raw = checker.array(root.get("raw_artifacts"), "$.raw_artifacts", 31)
    artifacts: dict[str, dict[str, Any]] = {}
    artifact_paths: list[str] = []
    artifact_times: dict[str, datetime | None] = {}
    for index, raw_artifact in enumerate(artifacts_raw):
        path = f"$.raw_artifacts[{index}]"
        artifact = checker.obj(
            raw_artifact,
            path,
            {"artifact_id", "drill_id", "kind", "uri", "sha256", "captured_at"},
        )
        artifact_id = checker.identifier(artifact.get("artifact_id"), f"{path}.artifact_id")
        checker.identifier(artifact.get("drill_id"), f"{path}.drill_id")
        checker.text(artifact.get("kind"), f"{path}.kind", allowed=DARWIN_ARTIFACT_KINDS)
        artifact_path = _darwin_resolve_uri(
            checker,
            artifact.get("uri"),
            f"{path}.uri",
            evidence_root,
            required_prefix="file:raw/",
            verify_files=verify_files,
        )
        _darwin_sha(checker, artifact.get("sha256"), f"{path}.sha256")
        captured_at = checker.timestamp(artifact.get("captured_at"), f"{path}.captured_at")
        if artifact_id is not None:
            if artifact_id in artifacts:
                checker.fail(f"{path}.artifact_id", "duplicates an earlier raw artifact")
            else:
                artifacts[artifact_id] = artifact
                artifact_times[artifact_id] = captured_at
        uri = artifact.get("uri")
        if isinstance(uri, str):
            artifact_paths.append(uri)
        _validate_darwin_raw_envelope(
            checker,
            artifact_path,
            artifact,
            index,
            identity,
            verify_files=verify_files,
        )
    checker.unique(artifact_paths, "$.raw_artifacts[*].uri")

    drills_raw = checker.array(root.get("drills"), "$.drills", 6)
    if len(drills_raw) != 6:
        checker.fail("$.drills", "must contain exactly six Darwin v2 incident drills")
    drill_ids: list[str] = []
    drill_classes: list[str] = []
    drill_bounds: dict[str, tuple[datetime | None, datetime | None]] = {}
    executors: set[str] = set()
    referenced_artifacts: set[str] = set()
    latest_drill_end: datetime | None = None
    class_affected_nodes = {
        "process_crash_restart": ["node2"],
        "one_node_unavailability": ["node2"],
        "bad_config_rollback": ["node2"],
        "certificate_secret_rotation": ["node1", "node2", "node3"],
        "storage_pressure_failure": ["node2"],
        "corrupted_checkpoint": ["node2"],
    }
    class_nonclaims = {
        "process_crash_restart": {"not-host-reboot", "not-independent-failure-domain"},
        "one_node_unavailability": {"not-multi-host", "not-independent-failure-domain"},
        "bad_config_rollback": {"not-client-or-peer-authorization-evidence"},
        "certificate_secret_rotation": {
            "not-mtls",
            "not-client-authorization",
            "not-peer-authorization",
        },
        "storage_pressure_failure": {
            "not-physical-apfs-failure",
            "not-enospc",
            "not-media-failure",
        },
        "corrupted_checkpoint": {
            "not-live-corruption",
            "not-forged-manifest-resistance",
        },
    }
    for index, raw_drill in enumerate(drills_raw):
        path = f"$.drills[{index}]"
        drill = checker.obj(raw_drill, path, DARWIN_DRILL_KEYS)
        drill_id = checker.identifier(drill.get("drill_id"), f"{path}.drill_id")
        incident_class = checker.text(
            drill.get("incident_class"),
            f"{path}.incident_class",
            allowed=DARWIN_INCIDENT_CLASSES,
        )
        if drill_id is not None:
            drill_ids.append(drill_id)
        if incident_class is not None:
            drill_classes.append(incident_class)
        affected_nodes = checker.array(drill.get("affected_nodes"), f"{path}.affected_nodes", 1)
        if incident_class is not None and affected_nodes != class_affected_nodes[incident_class]:
            checker.fail(
                f"{path}.affected_nodes",
                f"must equal {class_affected_nodes[incident_class]}",
            )
        for key in (
            "condition_source",
            "injection_method",
            "impact_boundary",
            "expected_outcome",
            "observed_outcome",
            "rollback_plan",
        ):
            _darwin_narrative(checker, drill.get(key), f"{path}.{key}")
        drill_nonclaims_raw = checker.array(drill.get("nonclaims"), f"{path}.nonclaims", 1)
        parsed_nonclaims: list[str] = []
        for nonclaim_index, raw_nonclaim in enumerate(drill_nonclaims_raw):
            nonclaim = checker.text(raw_nonclaim, f"{path}.nonclaims[{nonclaim_index}]")
            if nonclaim is not None:
                parsed_nonclaims.append(nonclaim)
        checker.unique(parsed_nonclaims, f"{path}.nonclaims")
        if incident_class is not None and set(parsed_nonclaims) != class_nonclaims[incident_class]:
            checker.fail(
                f"{path}.nonclaims",
                f"must explicitly equal {sorted(class_nonclaims[incident_class])}",
            )
        started_at = checker.timestamp(drill.get("started_at"), f"{path}.started_at")
        completed_at = checker.timestamp(drill.get("completed_at"), f"{path}.completed_at")
        approved_at = checker.timestamp(drill.get("approved_at"), f"{path}.approved_at")
        checker.ordered(opened_at, approved_at, "$.opened_at", f"{path}.approved_at")
        checker.ordered(approved_at, started_at, f"{path}.approved_at", f"{path}.started_at")
        checker.ordered(started_at, completed_at, f"{path}.started_at", f"{path}.completed_at")
        checker.ordered(completed_at, closed_at, f"{path}.completed_at", "$.closed_at")
        if drill_id is not None:
            drill_bounds[drill_id] = (started_at, completed_at)
        if completed_at is not None and (
            latest_drill_end is None or completed_at > latest_drill_end
        ):
            latest_drill_end = completed_at
        executor = checker.identifier(
            drill.get("executor_participant_id"),
            f"{path}.executor_participant_id",
        )
        approver = checker.identifier(
            drill.get("approver_participant_id"),
            f"{path}.approver_participant_id",
        )
        if executor is not None:
            executors.add(executor)
            if executor not in participants:
                checker.fail(f"{path}.executor_participant_id", "does not identify a participant")
        if approver is not None and approver not in participants:
            checker.fail(f"{path}.approver_participant_id", "does not identify a participant")
        if executor is not None and executor == approver:
            checker.fail(f"{path}.approver_participant_id", "must be independent of executor")
        if approver is not None and participant_roles.get(approver) not in {
            "incident-commander",
            "independent-reviewer",
        }:
            checker.fail(
                f"{path}.approver_participant_id",
                "must identify an incident commander or independent reviewer",
            )
        checker.text(drill.get("result"), f"{path}.result", allowed={"observed-pass"})
        refs_raw = checker.array(
            drill.get("evidence_artifact_ids"),
            f"{path}.evidence_artifact_ids",
            4,
        )
        drill_refs: list[str] = []
        for ref_index, raw_ref in enumerate(refs_raw):
            ref_path = f"{path}.evidence_artifact_ids[{ref_index}]"
            ref = checker.identifier(raw_ref, ref_path)
            if ref is None:
                continue
            drill_refs.append(ref)
            artifact = artifacts.get(ref)
            if artifact is None:
                checker.fail(ref_path, "does not identify a raw artifact")
            elif artifact.get("drill_id") != drill_id:
                checker.fail(ref_path, "raw artifact belongs to a different drill")
            else:
                captured_at = artifact_times.get(ref)
                checker.ordered(started_at, captured_at, f"{path}.started_at", f"artifact {ref}")
                checker.ordered(captured_at, completed_at, f"artifact {ref}", f"{path}.completed_at")
        checker.unique(drill_refs, f"{path}.evidence_artifact_ids")
        kinds = {artifacts.get(ref, {}).get("kind") for ref in drill_refs}
        for required_kind in {
            "raw-command-output",
            "raw-log",
            "raw-metric",
            "raw-communication",
        }:
            if required_kind not in kinds:
                checker.fail(
                    f"{path}.evidence_artifact_ids",
                    f"must reference {required_kind} evidence",
                )
        referenced_artifacts.update(drill_refs)
        _validate_darwin_observations(
            checker,
            incident_class,
            drill.get("observations"),
            f"{path}.observations",
        )
    checker.unique(drill_ids, "$.drills[*].drill_id")
    checker.unique(drill_classes, "$.drills[*].incident_class")
    if set(drill_classes) != DARWIN_INCIDENT_CLASSES:
        checker.fail(
            "$.drills",
            "incident classes mismatch; "
            f"missing={sorted(DARWIN_INCIDENT_CLASSES - set(drill_classes))}, "
            f"extra={sorted(set(drill_classes) - DARWIN_INCIDENT_CLASSES)}",
        )

    operational = checker.obj(
        root.get("operational_artifacts"),
        "$.operational_artifacts",
        {
            "topology_artifact_ids",
            "alert_artifact_ids",
            "runbook_artifact_ids",
            "signoff_artifact_ids",
        },
    )
    operational_contract = (
        ("topology_artifact_ids", {"raw-command-output"}, 3),
        ("alert_artifact_ids", {"raw-alert"}, 1),
        ("runbook_artifact_ids", {"raw-runbook"}, 1),
        ("signoff_artifact_ids", {"raw-signoff"}, 2),
    )
    operational_refs: dict[str, list[str]] = {}
    for key, allowed_kinds, minimum in operational_contract:
        refs_raw = checker.array(operational.get(key), f"$.operational_artifacts.{key}", minimum)
        refs: list[str] = []
        for ref_index, raw_ref in enumerate(refs_raw):
            ref_path = f"$.operational_artifacts.{key}[{ref_index}]"
            ref = checker.identifier(raw_ref, ref_path)
            if ref is None:
                continue
            refs.append(ref)
            artifact = artifacts.get(ref)
            if artifact is None:
                checker.fail(ref_path, "does not identify a raw artifact")
            else:
                if artifact.get("drill_id") != "campaign":
                    checker.fail(ref_path, "operational artifact drill_id must be campaign")
                if artifact.get("kind") not in allowed_kinds:
                    checker.fail(ref_path, f"must reference one of {sorted(allowed_kinds)}")
                captured_at = artifact_times.get(ref)
                checker.ordered(opened_at, captured_at, "$.opened_at", f"artifact {ref}")
                checker.ordered(captured_at, closed_at, f"artifact {ref}", "$.closed_at")
        checker.unique(refs, f"$.operational_artifacts.{key}")
        operational_refs[key] = refs
        referenced_artifacts.update(refs)
    if operational_refs.get("topology_artifact_ids") != topology_artifact_refs:
        checker.fail(
            "$.operational_artifacts.topology_artifact_ids",
            "must exactly match the ordered per-node topology evidence references",
        )

    sign_off = checker.obj(
        root.get("sign_off"),
        "$.sign_off",
        {"operator", "independent_reviewer"},
    )
    signature_values: dict[str, tuple[str | None, datetime | None, str | None]] = {}
    for key, required_role in (
        ("operator", "operator"),
        ("independent_reviewer", "independent-reviewer"),
    ):
        path = f"$.sign_off.{key}"
        signature = checker.obj(
            sign_off.get(key),
            path,
            {"participant_id", "role", "signed_at", "decision", "statement", "artifact_id"},
        )
        participant_id = checker.identifier(signature.get("participant_id"), f"{path}.participant_id")
        checker.text(signature.get("role"), f"{path}.role", allowed={required_role})
        signed_at = checker.timestamp(signature.get("signed_at"), f"{path}.signed_at")
        checker.text(signature.get("decision"), f"{path}.decision", allowed={"approved"})
        _darwin_narrative(checker, signature.get("statement"), f"{path}.statement")
        artifact_id = checker.identifier(signature.get("artifact_id"), f"{path}.artifact_id")
        if participant_id is not None:
            if participant_id not in participants:
                checker.fail(f"{path}.participant_id", "does not identify a participant")
            elif participant_roles.get(participant_id) != required_role:
                checker.fail(f"{path}.participant_id", f"participant must have role {required_role}")
        if key == "independent_reviewer" and participant_id in executors:
            checker.fail(
                f"{path}.participant_id",
                "independent reviewer must not be a drill executor",
            )
        if artifact_id is not None:
            artifact = artifacts.get(artifact_id)
            if artifact is None or artifact.get("kind") != "raw-signoff":
                checker.fail(f"{path}.artifact_id", "must identify a raw-signoff artifact")
            if artifact_id not in operational_refs.get("signoff_artifact_ids", []):
                checker.fail(
                    f"{path}.artifact_id",
                    "must be listed in operational_artifacts.signoff_artifact_ids",
                )
            artifact_time = artifact_times.get(artifact_id)
            if signed_at is not None and artifact_time is not None and signed_at != artifact_time:
                checker.fail(f"{path}.signed_at", "must exactly match signoff artifact captured_at")
        checker.ordered(latest_drill_end, signed_at, "latest drill completed_at", f"{path}.signed_at")
        checker.ordered(signed_at, closed_at, f"{path}.signed_at", "$.closed_at")
        signature_values[key] = (participant_id, signed_at, artifact_id)
    operator_id, operator_time, operator_artifact = signature_values.get(
        "operator", (None, None, None)
    )
    reviewer_id, reviewer_time, reviewer_artifact = signature_values.get(
        "independent_reviewer", (None, None, None)
    )
    if operator_id is not None and operator_id == reviewer_id:
        checker.fail("$.sign_off", "operator and independent reviewer must be different participants")
    if operator_artifact is not None and operator_artifact == reviewer_artifact:
        checker.fail("$.sign_off", "operator and reviewer must use different raw signoff artifacts")
    checker.ordered(
        operator_time,
        reviewer_time,
        "$.sign_off.operator.signed_at",
        "$.sign_off.independent_reviewer.signed_at",
    )
    if operator_time is not None and reviewer_time is not None and operator_time == reviewer_time:
        checker.fail(
            "$.sign_off.independent_reviewer.signed_at",
            "must follow operator signoff for independent review",
        )

    valid_drill_ids = set(drill_ids) | {"campaign"}
    for artifact_id, artifact in artifacts.items():
        artifact_drill_id = artifact.get("drill_id")
        if artifact_drill_id not in valid_drill_ids:
            checker.fail(
                f"$.raw_artifacts[{artifact_id}].drill_id",
                "does not identify a drill or campaign artifact",
            )
    orphan_artifacts = sorted(set(artifacts) - referenced_artifacts)
    if orphan_artifacts:
        checker.fail("$.raw_artifacts", f"contains unreferenced artifacts: {orphan_artifacts}")
    return checker.errors


def validate_document(
    document: dict[str, Any],
    *,
    test_mode: bool,
    expected_target: str | None = None,
    expected_release_id: str | None = None,
    expected_source_revision: str | None = None,
    expected_binary_sha256: str | None = None,
    expected_environment: str | None = None,
    evidence_root: Path | None = None,
    now: datetime | None = None,
    verify_files: bool = True,
) -> list[str]:
    if document.get("schema_version") == DARWIN_SCHEMA_VERSION:
        return _validate_darwin_v2_document(
            document,
            test_mode=test_mode,
            expected_target=expected_target,
            expected_release_id=expected_release_id,
            expected_source_revision=expected_source_revision,
            expected_binary_sha256=expected_binary_sha256,
            expected_environment=expected_environment,
            evidence_root=evidence_root,
            now=now,
            verify_files=verify_files,
        )
    return _validate_v1_document(
        document,
        test_mode=test_mode,
        expected_target=expected_target,
        expected_source_revision=expected_source_revision,
        expected_binary_sha256=expected_binary_sha256,
        now=now,
        verify_files=verify_files,
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a complete target incident evidence record and all referenced file hashes."
    )
    parser.add_argument("evidence", type=Path, help="incident evidence JSON document")
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="accept only synthetic-test fixtures; never produces a target claim",
    )
    parser.add_argument("--expected-target", help="exact target name required in production mode")
    parser.add_argument(
        "--expected-release-id",
        help="exact release identifier required by Darwin v2 production records",
    )
    parser.add_argument(
        "--expected-environment",
        help="exact environment/profile required by Darwin v2 production records",
    )
    parser.add_argument(
        "--evidence-root",
        type=Path,
        help="absolute external evidence root for Darwin v2 root-relative file URIs",
    )
    parser.add_argument(
        "--expected-source-revision",
        help="exact immutable source revision required in production mode",
    )
    parser.add_argument(
        "--expected-binary-sha256",
        help="exact release binary SHA-256 required in production mode",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        document = load_document(args.evidence)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"target_incident_evidence=invalid reason={exc}", file=sys.stderr)
        return 1
    errors = validate_document(
        document,
        test_mode=args.test_mode,
        expected_target=args.expected_target,
        expected_release_id=args.expected_release_id,
        expected_source_revision=args.expected_source_revision,
        expected_binary_sha256=args.expected_binary_sha256,
        expected_environment=args.expected_environment,
        evidence_root=args.evidence_root,
    )
    if errors:
        print("target_incident_evidence=invalid", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    if args.test_mode:
        print(
            "target_incident_evidence=verified mode=synthetic-test "
            "test_only=true release_claim=none"
        )
    else:
        print(
            "target_incident_evidence=verified mode=target "
            f"target={document['target']['name']} claim={document['claim']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
