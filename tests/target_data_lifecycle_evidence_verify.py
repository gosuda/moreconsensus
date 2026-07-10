#!/usr/bin/env python3
"""Fail-closed verifier for target data-lifecycle release evidence.

The verifier only reads a report and the raw artifacts named by that report. It
never runs a drill, contacts a target, or changes target state. Synthetic
fixtures are accepted only with the explicit --self-test-fixture switch and
always produce a non-claim result.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

SCHEMA_VERSION = "1.0.0"
TARGET_EVIDENCE_CLASS = "target-data-lifecycle"
TARGET_CLAIM = "target-data-lifecycle-criteria-met"
SYNTHETIC_EVIDENCE_CLASS = "synthetic-test-only"
SYNTHETIC_CLAIM = "none"
MAX_VALIDITY = timedelta(days=30)
MAX_GENERATION_LAG = timedelta(hours=24)
FUTURE_SKEW = timedelta(minutes=5)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REVISION_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
PLACEHOLDER_RE = re.compile(
    r"(?:^|[^a-z0-9])(?:tbd|todo|placeholder|replace[-_ ]?me|unknown|"
    r"unspecified|not[-_ ]?set|n/?a)(?:$|[^a-z0-9])",
    re.IGNORECASE,
)
NON_TARGET_RE = re.compile(
    r"(?:^|[^a-z0-9])(?:local|localhost|loopback|example|fixture|synthetic|"
    r"test[-_ ]?only)(?:$|[^a-z0-9])",
    re.IGNORECASE,
)


class EvidenceError(Exception):
    pass


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def fail(message: str) -> None:
    raise EvidenceError(message)


def expect_object(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{path} must be an object")
    return value


def expect_array(value: Any, path: str, minimum: int = 0) -> list[Any]:
    if not isinstance(value, list):
        fail(f"{path} must be an array")
    if len(value) < minimum:
        fail(f"{path} must contain at least {minimum} item(s)")
    return value


def expect_keys(value: Any, path: str, keys: Iterable[str]) -> dict[str, Any]:
    obj = expect_object(value, path)
    expected = set(keys)
    actual = set(obj)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        fail(f"{path} missing required field(s): {', '.join(missing)}")
    if extra:
        fail(f"{path} contains unknown field(s): {', '.join(extra)}")
    return obj


def expect_string(value: Any, path: str, *, allow_none: bool = False) -> str:
    if not isinstance(value, str) or not value.strip():
        fail(f"{path} must be a non-empty string")
    if len(value) > 512:
        fail(f"{path} must be at most 512 characters")
    if value != value.strip() or "\n" in value or "\r" in value:
        fail(f"{path} must be a trimmed single-line string")
    if not allow_none and value.casefold() == "none":
        fail(f"{path} must not be none")
    return value


def expect_identity(value: Any, path: str) -> str:
    text = expect_string(value, path)
    if PLACEHOLDER_RE.search(text):
        fail(f"{path} contains a placeholder")
    if NON_TARGET_RE.search(text):
        fail(f"{path} identifies local, example, or synthetic evidence")
    return text


def reject_placeholders(value: Any, path: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            reject_placeholders(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_placeholders(child, f"{path}[{index}]")
    elif isinstance(value, str) and PLACEHOLDER_RE.search(value):
        fail(f"{path} contains a placeholder")


def expect_id(value: Any, path: str) -> str:
    text = expect_string(value, path)
    if not SAFE_ID_RE.fullmatch(text):
        fail(f"{path} must be a stable identifier")
    if PLACEHOLDER_RE.search(text):
        fail(f"{path} contains a placeholder")
    return text


def expect_integer(value: Any, path: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        fail(f"{path} must be an integer")
    if value < minimum:
        fail(f"{path} must be >= {minimum}")
    return value


def expect_bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        fail(f"{path} must be a boolean")
    return value


def expect_literal(value: Any, path: str, expected: str) -> str:
    text = expect_string(value, path, allow_none=(expected == "none"))
    if text != expected:
        fail(f"{path} must be {expected!r}")
    return text


def expect_one_of(value: Any, path: str, choices: set[str]) -> str:
    text = expect_string(value, path)
    if text not in choices:
        fail(f"{path} must be one of: {', '.join(sorted(choices))}")
    return text


def expect_sha256(value: Any, path: str) -> str:
    text = expect_string(value, path)
    if not SHA256_RE.fullmatch(text):
        fail(f"{path} must be a lowercase SHA-256 digest")
    return text


def expect_time(value: Any, path: str) -> datetime:
    text = expect_string(value, path)
    if not UTC_RE.fullmatch(text):
        fail(f"{path} must be a whole-second UTC timestamp ending in Z")
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise EvidenceError(f"{path} is not a valid UTC timestamp") from exc


def require_order(first: datetime, first_path: str, second: datetime, second_path: str) -> None:
    if first > second:
        fail(f"inconsistent time ordering: {first_path} must not be after {second_path}")


def artifact_reference(value: Any, path: str, references: list[str]) -> str:
    artifact_id = expect_id(value, path)
    references.append(artifact_id)
    return artifact_id


def validate_schema_contract(report: dict[str, Any]) -> None:
    schema_path = Path(__file__).with_name("target_data_lifecycle_evidence.schema.json")
    try:
        with schema_path.open("r", encoding="utf-8") as handle:
            schema = json.load(handle, object_pairs_hook=reject_duplicate_keys)
    except (OSError, json.JSONDecodeError, EvidenceError) as exc:
        fail(f"cannot load verifier schema: {exc}")
    try:
        schema_version = schema["properties"]["schema_version"]["const"]
    except (KeyError, TypeError) as exc:
        raise EvidenceError("verifier schema does not declare schema_version const") from exc
    if schema_version != SCHEMA_VERSION:
        fail("verifier and schema versions disagree")
    if report.get("schema_version") != schema_version:
        fail(f"schema_version must be {schema_version!r}")


def validate_artifacts(
    raw_artifacts: Any,
    report_dir: Path,
    references: list[str],
    started_at: datetime,
    generated_at: datetime,
) -> None:
    entries = expect_array(raw_artifacts, "$.raw_artifacts", minimum=1)
    ids: set[str] = set()
    paths: set[str] = set()
    by_id: dict[str, dict[str, Any]] = {}

    for index, raw in enumerate(entries):
        path = f"$.raw_artifacts[{index}]"
        item = expect_keys(raw, path, ("id", "path", "sha256", "captured_at"))
        artifact_id = expect_id(item["id"], f"{path}.id")
        relative_text = expect_string(item["path"], f"{path}.path")
        digest = expect_sha256(item["sha256"], f"{path}.sha256")
        captured_at = expect_time(item["captured_at"], f"{path}.captured_at")
        require_order(started_at, "$.drill.started_at", captured_at, f"{path}.captured_at")
        require_order(captured_at, f"{path}.captured_at", generated_at, "$.generated_at")

        if artifact_id in ids:
            fail(f"duplicate raw artifact id: {artifact_id}")
        ids.add(artifact_id)
        if relative_text in paths:
            fail(f"duplicate raw artifact path: {relative_text}")
        paths.add(relative_text)

        if "\\" in relative_text:
            fail(f"{path}.path must use POSIX relative path syntax")
        relative = PurePosixPath(relative_text)
        if relative.is_absolute() or not relative.parts or any(part in ("", ".", "..") for part in relative.parts):
            fail(f"{path}.path must be a normalized relative path")

        candidate = report_dir.joinpath(*relative.parts)
        current = report_dir
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                fail(f"{path}.path must not traverse a symbolic link")
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise EvidenceError(f"missing raw artifact {relative_text}: {exc}") from exc
        try:
            resolved.relative_to(report_dir)
        except ValueError as exc:
            raise EvidenceError(f"{path}.path escapes the report directory") from exc
        if not resolved.is_file():
            fail(f"raw artifact is not a regular file: {relative_text}")
        if resolved.stat().st_size == 0:
            fail(f"raw artifact is empty: {relative_text}")

        hasher = hashlib.sha256()
        try:
            with resolved.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    hasher.update(chunk)
        except OSError as exc:
            raise EvidenceError(f"cannot read raw artifact {relative_text}: {exc}") from exc
        if hasher.hexdigest() != digest:
            fail(f"raw artifact SHA-256 mismatch: {relative_text}")
        by_id[artifact_id] = item

    reference_set = set(references)
    duplicate_references = sorted({item for item in references if references.count(item) > 1})
    if duplicate_references:
        fail(f"raw artifact referenced more than once: {', '.join(duplicate_references)}")
    missing = sorted(reference_set - ids)
    unreferenced = sorted(ids - reference_set)
    if missing:
        fail(f"missing raw artifact record(s): {', '.join(missing)}")
    if unreferenced:
        fail(f"unreferenced raw artifact record(s): {', '.join(unreferenced)}")


def validate_report(report: Any, report_path: Path, self_test_fixture: bool, now: datetime) -> tuple[str, str]:
    top = expect_keys(
        report,
        "$",
        (
            "schema_version",
            "evidence_class",
            "release_claim",
            "synthetic_test_fixture",
            "generated_at",
            "valid_until",
            "target",
            "drill",
            "pre_drill",
            "checkpoint",
            "disaster",
            "recovery",
            "canaries",
            "integrity",
            "objectives",
            "observability",
            "sign_off",
            "raw_artifacts",
        ),
    )
    validate_schema_contract(top)

    is_synthetic = expect_bool(top["synthetic_test_fixture"], "$.synthetic_test_fixture")
    if self_test_fixture:
        if not is_synthetic:
            fail("--self-test-fixture requires synthetic_test_fixture=true")
        expect_literal(top["evidence_class"], "$.evidence_class", SYNTHETIC_EVIDENCE_CLASS)
        expect_literal(top["release_claim"], "$.release_claim", SYNTHETIC_CLAIM)
    else:
        if is_synthetic:
            fail("synthetic test fixtures are not target evidence")
        expect_literal(top["evidence_class"], "$.evidence_class", TARGET_EVIDENCE_CLASS)
        expect_literal(top["release_claim"], "$.release_claim", TARGET_CLAIM)
    reject_placeholders(top, "$")

    generated_at = expect_time(top["generated_at"], "$.generated_at")
    valid_until = expect_time(top["valid_until"], "$.valid_until")
    require_order(generated_at, "$.generated_at", valid_until, "$.valid_until")
    if valid_until > generated_at + MAX_VALIDITY:
        fail("$.valid_until exceeds the 30-day maximum evidence validity")
    if generated_at > now + FUTURE_SKEW:
        fail("evidence was generated in the future")
    if now > valid_until:
        fail("evidence is stale")

    target = expect_keys(
        top["target"],
        "$.target",
        ("name", "environment", "cluster_id", "release_id", "source_revision", "binary_sha256", "provenance_artifact_id"),
    )
    target_name = expect_identity(target["name"], "$.target.name")
    expect_identity(target["environment"], "$.target.environment")
    cluster_id = expect_id(target["cluster_id"], "$.target.cluster_id")
    if NON_TARGET_RE.search(cluster_id):
        fail("$.target.cluster_id identifies local, example, or synthetic evidence")
    expect_identity(target["release_id"], "$.target.release_id")
    revision = expect_string(target["source_revision"], "$.target.source_revision")
    if not REVISION_RE.fullmatch(revision):
        fail("$.target.source_revision must be an immutable lowercase 40- or 64-hex revision")
    expect_sha256(target["binary_sha256"], "$.target.binary_sha256")
    provenance_artifact_id = expect_id(target["provenance_artifact_id"], "$.target.provenance_artifact_id")

    drill = expect_keys(top["drill"], "$.drill", ("drill_id", "started_at", "completed_at"))
    expect_id(drill["drill_id"], "$.drill.drill_id")
    started_at = expect_time(drill["started_at"], "$.drill.started_at")
    completed_at = expect_time(drill["completed_at"], "$.drill.completed_at")
    require_order(started_at, "$.drill.started_at", completed_at, "$.drill.completed_at")

    references: list[str] = [provenance_artifact_id]
    pre = expect_keys(
        top["pre_drill"],
        "$.pre_drill",
        (
            "checked_at",
            "health_result",
            "quorum_result",
            "expected_voters",
            "healthy_voters",
            "readiness_result",
            "metrics_result",
            "health_artifact_id",
            "quorum_artifact_id",
            "readiness_artifact_id",
            "metrics_artifact_id",
        ),
    )
    pre_checked = expect_time(pre["checked_at"], "$.pre_drill.checked_at")
    expect_literal(pre["health_result"], "$.pre_drill.health_result", "pass")
    expect_literal(pre["quorum_result"], "$.pre_drill.quorum_result", "pass")
    expected_voters = expect_integer(pre["expected_voters"], "$.pre_drill.expected_voters", 1)
    healthy_voters = expect_integer(pre["healthy_voters"], "$.pre_drill.healthy_voters", 1)
    if healthy_voters != expected_voters:
        fail("pre-drill healthy_voters must equal expected_voters")
    expect_literal(pre["readiness_result"], "$.pre_drill.readiness_result", "pass")
    expect_literal(pre["metrics_result"], "$.pre_drill.metrics_result", "pass")
    for field in ("health_artifact_id", "quorum_artifact_id", "readiness_artifact_id", "metrics_artifact_id"):
        artifact_reference(pre[field], f"$.pre_drill.{field}", references)

    checkpoint = expect_keys(
        top["checkpoint"],
        "$.checkpoint",
        (
            "checkpoint_id",
            "created_at",
            "source_node",
            "manifest_sha256",
            "state_digest_sha256",
            "record_count",
            "semantic_verification_result",
            "semantic_artifact_id",
            "retention",
        ),
    )
    expect_id(checkpoint["checkpoint_id"], "$.checkpoint.checkpoint_id")
    checkpoint_at = expect_time(checkpoint["created_at"], "$.checkpoint.created_at")
    expect_id(checkpoint["source_node"], "$.checkpoint.source_node")
    expect_sha256(checkpoint["manifest_sha256"], "$.checkpoint.manifest_sha256")
    expect_sha256(checkpoint["state_digest_sha256"], "$.checkpoint.state_digest_sha256")
    checkpoint_records = expect_integer(checkpoint["record_count"], "$.checkpoint.record_count", 1)
    expect_literal(checkpoint["semantic_verification_result"], "$.checkpoint.semantic_verification_result", "pass")
    artifact_reference(checkpoint["semantic_artifact_id"], "$.checkpoint.semantic_artifact_id", references)
    retention = expect_keys(
        checkpoint["retention"],
        "$.checkpoint.retention",
        ("location", "immutable_object_version", "retention_until", "result", "artifact_id"),
    )
    location = expect_string(retention["location"], "$.checkpoint.retention.location")
    if not re.match(r"^(?:s3|gs|azure)://[^/\s]+/.+", location):
        fail("$.checkpoint.retention.location must be an off-host object-store location")
    if NON_TARGET_RE.search(location):
        fail("$.checkpoint.retention.location must not identify local or example storage")
    expect_identity(retention["immutable_object_version"], "$.checkpoint.retention.immutable_object_version")
    retention_until = expect_time(retention["retention_until"], "$.checkpoint.retention.retention_until")
    expect_literal(retention["result"], "$.checkpoint.retention.result", "pass")
    artifact_reference(retention["artifact_id"], "$.checkpoint.retention.artifact_id", references)

    disaster = expect_keys(
        top["disaster"],
        "$.disaster",
        ("scenario", "scope", "occurred_at", "affected_node", "result", "artifact_id"),
    )
    expect_identity(disaster["scenario"], "$.disaster.scenario")
    expect_identity(disaster["scope"], "$.disaster.scope")
    disaster_at = expect_time(disaster["occurred_at"], "$.disaster.occurred_at")
    affected_node = expect_id(disaster["affected_node"], "$.disaster.affected_node")
    expect_literal(disaster["result"], "$.disaster.result", "pass")
    artifact_reference(disaster["artifact_id"], "$.disaster.artifact_id", references)

    recovery = expect_keys(
        top["recovery"],
        "$.recovery",
        (
            "node_stopped_at",
            "recovery_started_at",
            "operation",
            "operation_result",
            "stopped_node",
            "stopped_node_confirmed",
            "transcript_artifact_id",
            "rollback_or_quarantine",
            "restart",
            "service_restored_at",
            "rejoin",
            "catch_up",
        ),
    )
    node_stopped_at = expect_time(recovery["node_stopped_at"], "$.recovery.node_stopped_at")
    recovery_started_at = expect_time(recovery["recovery_started_at"], "$.recovery.recovery_started_at")
    expect_one_of(recovery["operation"], "$.recovery.operation", {"repair", "restore"})
    expect_literal(recovery["operation_result"], "$.recovery.operation_result", "pass")
    stopped_node = expect_id(recovery["stopped_node"], "$.recovery.stopped_node")
    if stopped_node != affected_node:
        fail("recovery stopped_node must match disaster affected_node")
    if expect_bool(recovery["stopped_node_confirmed"], "$.recovery.stopped_node_confirmed") is not True:
        fail("recovery must confirm that the recovered node was stopped")
    artifact_reference(recovery["transcript_artifact_id"], "$.recovery.transcript_artifact_id", references)

    rollback = expect_keys(
        recovery["rollback_or_quarantine"],
        "$.recovery.rollback_or_quarantine",
        ("action", "at", "result", "artifact_id"),
    )
    expect_one_of(rollback["action"], "$.recovery.rollback_or_quarantine.action", {"quarantine", "rollback"})
    rollback_at = expect_time(rollback["at"], "$.recovery.rollback_or_quarantine.at")
    expect_literal(rollback["result"], "$.recovery.rollback_or_quarantine.result", "pass")
    artifact_reference(rollback["artifact_id"], "$.recovery.rollback_or_quarantine.artifact_id", references)

    restart = expect_keys(recovery["restart"], "$.recovery.restart", ("at", "result", "artifact_id"))
    restart_at = expect_time(restart["at"], "$.recovery.restart.at")
    expect_literal(restart["result"], "$.recovery.restart.result", "pass")
    artifact_reference(restart["artifact_id"], "$.recovery.restart.artifact_id", references)
    service_restored_at = expect_time(recovery["service_restored_at"], "$.recovery.service_restored_at")

    rejoin = expect_keys(recovery["rejoin"], "$.recovery.rejoin", ("at", "result", "artifact_id"))
    rejoin_at = expect_time(rejoin["at"], "$.recovery.rejoin.at")
    expect_literal(rejoin["result"], "$.recovery.rejoin.result", "pass")
    artifact_reference(rejoin["artifact_id"], "$.recovery.rejoin.artifact_id", references)

    catch_up = expect_keys(recovery["catch_up"], "$.recovery.catch_up", ("at", "result", "lag_entries", "artifact_id"))
    catch_up_at = expect_time(catch_up["at"], "$.recovery.catch_up.at")
    expect_literal(catch_up["result"], "$.recovery.catch_up.result", "pass")
    if expect_integer(catch_up["lag_entries"], "$.recovery.catch_up.lag_entries", 0) != 0:
        fail("$.recovery.catch_up.lag_entries must be zero")
    artifact_reference(catch_up["artifact_id"], "$.recovery.catch_up.artifact_id", references)

    canaries = expect_keys(top["canaries"], "$.canaries", ("pre_checked_at", "post_checked_at", "result", "values", "artifact_id"))
    pre_canary_at = expect_time(canaries["pre_checked_at"], "$.canaries.pre_checked_at")
    post_canary_at = expect_time(canaries["post_checked_at"], "$.canaries.post_checked_at")
    expect_literal(canaries["result"], "$.canaries.result", "pass")
    values = expect_array(canaries["values"], "$.canaries.values", minimum=1)
    canary_keys: set[str] = set()
    for index, raw in enumerate(values):
        path = f"$.canaries.values[{index}]"
        item = expect_keys(raw, path, ("key", "pre_value", "post_value", "result"))
        key = expect_string(item["key"], f"{path}.key")
        if key in canary_keys:
            fail(f"duplicate canary key: {key}")
        canary_keys.add(key)
        pre_value = expect_string(item["pre_value"], f"{path}.pre_value", allow_none=True)
        post_value = expect_string(item["post_value"], f"{path}.post_value", allow_none=True)
        if pre_value != post_value:
            fail(f"{path} pre/post canary values differ")
        expect_literal(item["result"], f"{path}.result", "pass")
    artifact_reference(canaries["artifact_id"], "$.canaries.artifact_id", references)

    integrity = expect_keys(
        top["integrity"],
        "$.integrity",
        (
            "checked_at",
            "result",
            "expected_record_count",
            "observed_record_count",
            "expected_application_count",
            "observed_application_count",
            "duplicate_applications",
            "data_loss_records",
            "artifact_id",
        ),
    )
    integrity_at = expect_time(integrity["checked_at"], "$.integrity.checked_at")
    expect_literal(integrity["result"], "$.integrity.result", "pass")
    expected_records = expect_integer(integrity["expected_record_count"], "$.integrity.expected_record_count", 1)
    observed_records = expect_integer(integrity["observed_record_count"], "$.integrity.observed_record_count", 0)
    if expected_records != checkpoint_records or observed_records != expected_records:
        fail("integrity record counts do not prove checkpoint preservation")
    expected_applications = expect_integer(integrity["expected_application_count"], "$.integrity.expected_application_count", 1)
    observed_applications = expect_integer(integrity["observed_application_count"], "$.integrity.observed_application_count", 0)
    if observed_applications != expected_applications:
        fail("integrity application counts differ")
    if expect_integer(integrity["duplicate_applications"], "$.integrity.duplicate_applications", 0) != 0:
        fail("duplicate applications were observed")
    if expect_integer(integrity["data_loss_records"], "$.integrity.data_loss_records", 0) != 0:
        fail("data loss was observed")
    artifact_reference(integrity["artifact_id"], "$.integrity.artifact_id", references)

    objectives = expect_keys(top["objectives"], "$.objectives", ("rpo", "rto", "artifact_id"))
    rpo = expect_keys(objectives["rpo"], "$.objectives.rpo", ("recovery_point_at", "measured_seconds", "threshold_seconds", "result"))
    recovery_point_at = expect_time(rpo["recovery_point_at"], "$.objectives.rpo.recovery_point_at")
    rpo_measured = expect_integer(rpo["measured_seconds"], "$.objectives.rpo.measured_seconds", 0)
    rpo_threshold = expect_integer(rpo["threshold_seconds"], "$.objectives.rpo.threshold_seconds", 0)
    expect_literal(rpo["result"], "$.objectives.rpo.result", "met")
    if recovery_point_at != checkpoint_at:
        fail("RPO recovery_point_at must equal checkpoint created_at")
    actual_rpo = int((disaster_at - recovery_point_at).total_seconds())
    if actual_rpo < 0 or rpo_measured != actual_rpo:
        fail("RPO measurement does not match evidence timestamps")
    if rpo_measured > rpo_threshold:
        fail("measured RPO exceeds declared threshold")

    rto = expect_keys(objectives["rto"], "$.objectives.rto", ("started_at", "restored_at", "measured_seconds", "threshold_seconds", "result"))
    rto_started_at = expect_time(rto["started_at"], "$.objectives.rto.started_at")
    rto_restored_at = expect_time(rto["restored_at"], "$.objectives.rto.restored_at")
    rto_measured = expect_integer(rto["measured_seconds"], "$.objectives.rto.measured_seconds", 0)
    rto_threshold = expect_integer(rto["threshold_seconds"], "$.objectives.rto.threshold_seconds", 0)
    expect_literal(rto["result"], "$.objectives.rto.result", "met")
    if rto_started_at != disaster_at or rto_restored_at != service_restored_at:
        fail("RTO endpoints must match disaster and service-restored timestamps")
    actual_rto = int((rto_restored_at - rto_started_at).total_seconds())
    if actual_rto < 0 or rto_measured != actual_rto:
        fail("RTO measurement does not match evidence timestamps")
    if rto_measured > rto_threshold:
        fail("measured RTO exceeds declared threshold")
    artifact_reference(objectives["artifact_id"], "$.objectives.artifact_id", references)

    observability = expect_keys(
        top["observability"],
        "$.observability",
        ("checked_at", "logs_result", "logs_artifact_id", "readiness_result", "readiness_artifact_id", "metrics_result", "metrics_artifact_id"),
    )
    observability_at = expect_time(observability["checked_at"], "$.observability.checked_at")
    for result_field, artifact_field in (
        ("logs_result", "logs_artifact_id"),
        ("readiness_result", "readiness_artifact_id"),
        ("metrics_result", "metrics_artifact_id"),
    ):
        expect_literal(observability[result_field], f"$.observability.{result_field}", "pass")
        artifact_reference(observability[artifact_field], f"$.observability.{artifact_field}", references)

    sign_off = expect_keys(top["sign_off"], "$.sign_off", ("operator", "independent_reviewer"))
    signer_times: dict[str, datetime] = {}
    signer_names: dict[str, str] = {}
    for signer_key in ("operator", "independent_reviewer"):
        path = f"$.sign_off.{signer_key}"
        signer = expect_keys(sign_off[signer_key], path, ("name", "role", "signed_at", "result", "artifact_id"))
        signer_names[signer_key] = expect_identity(signer["name"], f"{path}.name")
        expect_identity(signer["role"], f"{path}.role")
        signer_times[signer_key] = expect_time(signer["signed_at"], f"{path}.signed_at")
        expect_literal(signer["result"], f"{path}.result", "approved")
        artifact_reference(signer["artifact_id"], f"{path}.artifact_id", references)
    if signer_names["operator"].casefold() == signer_names["independent_reviewer"].casefold():
        fail("operator and independent reviewer must be different people")

    for first, first_path, second, second_path in (
        (started_at, "$.drill.started_at", pre_checked, "$.pre_drill.checked_at"),
        (pre_checked, "$.pre_drill.checked_at", pre_canary_at, "$.canaries.pre_checked_at"),
        (pre_canary_at, "$.canaries.pre_checked_at", checkpoint_at, "$.checkpoint.created_at"),
        (checkpoint_at, "$.checkpoint.created_at", disaster_at, "$.disaster.occurred_at"),
        (disaster_at, "$.disaster.occurred_at", node_stopped_at, "$.recovery.node_stopped_at"),
        (node_stopped_at, "$.recovery.node_stopped_at", recovery_started_at, "$.recovery.recovery_started_at"),
        (recovery_started_at, "$.recovery.recovery_started_at", rollback_at, "$.recovery.rollback_or_quarantine.at"),
        (rollback_at, "$.recovery.rollback_or_quarantine.at", restart_at, "$.recovery.restart.at"),
        (restart_at, "$.recovery.restart.at", service_restored_at, "$.recovery.service_restored_at"),
        (service_restored_at, "$.recovery.service_restored_at", rejoin_at, "$.recovery.rejoin.at"),
        (rejoin_at, "$.recovery.rejoin.at", catch_up_at, "$.recovery.catch_up.at"),
        (catch_up_at, "$.recovery.catch_up.at", post_canary_at, "$.canaries.post_checked_at"),
        (post_canary_at, "$.canaries.post_checked_at", integrity_at, "$.integrity.checked_at"),
        (post_canary_at, "$.canaries.post_checked_at", observability_at, "$.observability.checked_at"),
        (integrity_at, "$.integrity.checked_at", completed_at, "$.drill.completed_at"),
        (observability_at, "$.observability.checked_at", completed_at, "$.drill.completed_at"),
        (completed_at, "$.drill.completed_at", signer_times["operator"], "$.sign_off.operator.signed_at"),
        (signer_times["operator"], "$.sign_off.operator.signed_at", signer_times["independent_reviewer"], "$.sign_off.independent_reviewer.signed_at"),
        (signer_times["independent_reviewer"], "$.sign_off.independent_reviewer.signed_at", generated_at, "$.generated_at"),
        (generated_at, "$.generated_at", retention_until, "$.checkpoint.retention.retention_until"),
    ):
        require_order(first, first_path, second, second_path)
    if generated_at - completed_at > MAX_GENERATION_LAG:
        fail("evidence generated more than 24 hours after drill completion is stale")

    validate_artifacts(top["raw_artifacts"], report_path.parent.resolve(), references, started_at, generated_at)
    claim = SYNTHETIC_CLAIM if self_test_fixture else TARGET_CLAIM
    return target_name, claim


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify target data-lifecycle evidence without running or altering a target."
    )
    parser.add_argument(
        "--self-test-fixture",
        action="store_true",
        help="accept only an explicitly marked synthetic non-claim fixture",
    )
    parser.add_argument("report", type=Path, help="JSON report; artifact paths are relative to its directory")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report_path = args.report.resolve(strict=True)
        if not report_path.is_file():
            fail("report must be a regular file")
        with report_path.open("r", encoding="utf-8") as handle:
            report = json.load(handle, object_pairs_hook=reject_duplicate_keys)
        now = datetime.now(timezone.utc)
        target, claim = validate_report(report, report_path, args.self_test_fixture, now)
    except (EvidenceError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"target-data-lifecycle-evidence status=fail reason={exc}", file=sys.stderr)
        return 1

    if args.self_test_fixture:
        print(
            "target-data-lifecycle-evidence status=pass mode=synthetic-test-fixture "
            f"release_claim=none target_label={target}"
        )
    else:
        print(
            "target-data-lifecycle-evidence status=pass mode=target-evidence "
            f"release_claim={claim} target={target}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
