#!/usr/bin/env python3
"""Verify native Darwin checkpoint/restore lifecycle evidence v2.

Version 2 is additive. The v1 schema, verifier, and self-test remain unchanged.
This verifier reads evidence only; it never executes a checkpoint or mutates a
restore destination. Synthetic fixtures require an explicit non-claim switch.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

SCHEMA_VERSION = "2.0.0"
VERIFIER_VERSION = "2.0.0"
TARGET_ID = "mc-kv-darwin24-arm64-launchd-3n-r1"
CLUSTER_ID = "mc-kv-darwin24-3n-r1"
ENVIRONMENT_PROFILE = "native-darwin24-arm64-launchd-system-domain-v1"
TARGET_EVIDENCE_CLASS = "target-data-lifecycle-darwin-v2"
TARGET_CLAIM = "target-data-lifecycle-criteria-met"
SYNTHETIC_EVIDENCE_CLASS = "synthetic-test-only"
SYNTHETIC_CLAIM = "none"
MAX_VALIDITY = timedelta(days=30)
MAX_GENERATION_LAG = timedelta(hours=24)
FUTURE_SKEW = timedelta(minutes=5)
HEX_256_RE = re.compile(r"^[0-9a-f]{64}$")
REVISION_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
PLACEHOLDER_RE = re.compile(
    r"(?:^|[^a-z0-9])(?:tbd|todo|placeholder|replace[-_ ]?me|unknown|"
    r"unspecified|not[-_ ]?set|n/?a)(?:$|[^a-z0-9])",
    re.IGNORECASE,
)
MARKER_LINE_RE = re.compile(
    r"^(?:status|result|success|passed|release_claim)\s*[:=]\s*(?:pass|passed|success|ok|none)$",
    re.IGNORECASE,
)
REQUIRED_REJECTION_CASES = {
    "corrupt-manifest",
    "truncated-manifest",
    "mismatched-metadata",
    "cross-cluster",
}
REQUIRED_COMMAND_STEPS = {
    "checkpoint",
    "verify-current",
    "copy-backup",
    "verify-backup",
    "stage-restore",
    "verify-stage",
    "atomic-publish",
    "restart",
    "post-restore-probes",
}


class EvidenceError(Exception):
    """Evidence is malformed, incomplete, stale, or semantically inconsistent."""


def fail(message: str) -> None:
    raise EvidenceError(message)


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def exact_object(value: Any, keys: Iterable[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{path} must be an object")
    expected = set(keys)
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        fail(f"{path} missing required field(s): {', '.join(missing)}")
    if extra:
        fail(f"{path} contains unknown field(s): {', '.join(extra)}")
    return value


def array(value: Any, path: str, minimum: int = 0, maximum: int | None = None) -> list[Any]:
    if not isinstance(value, list):
        fail(f"{path} must be an array")
    if len(value) < minimum:
        fail(f"{path} must contain at least {minimum} item(s)")
    if maximum is not None and len(value) > maximum:
        fail(f"{path} must contain at most {maximum} item(s)")
    return value


def text(value: Any, path: str, *, maximum: int = 1024, allow_none: bool = False) -> str:
    if not isinstance(value, str) or not value.strip():
        fail(f"{path} must be a non-empty string")
    if len(value) > maximum:
        fail(f"{path} must be at most {maximum} characters")
    if value != value.strip() or "\n" in value or "\r" in value:
        fail(f"{path} must be a trimmed single-line string")
    if not allow_none and value.casefold() == "none":
        fail(f"{path} must not be none")
    if PLACEHOLDER_RE.search(value):
        fail(f"{path} contains a placeholder")
    return value


def literal(value: Any, path: str, expected: Any) -> Any:
    if type(value) is not type(expected) or value != expected:
        fail(f"{path} must be {expected!r}")
    return value


def boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        fail(f"{path} must be a boolean")
    return value


def integer(value: Any, path: str, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        fail(f"{path} must be an integer")
    if value < minimum:
        fail(f"{path} must be >= {minimum}")
    if maximum is not None and value > maximum:
        fail(f"{path} must be <= {maximum}")
    return value


def stable_id(value: Any, path: str) -> str:
    result = text(value, path, maximum=128)
    if not SAFE_ID_RE.fullmatch(result):
        fail(f"{path} must be a stable identifier")
    return result


def digest(value: Any, path: str, algorithm: str) -> str:
    result = text(value, path, maximum=64)
    if not HEX_256_RE.fullmatch(result):
        fail(f"{path} must be a lowercase {algorithm} digest")
    return result


def sha256(value: Any, path: str) -> str:
    return digest(value, path, "SHA-256")


def blake3(value: Any, path: str) -> str:
    return digest(value, path, "BLAKE3")


def timestamp(value: Any, path: str) -> datetime:
    result = text(value, path, maximum=20)
    if not UTC_RE.fullmatch(result):
        fail(f"{path} must be a whole-second UTC timestamp ending in Z")
    try:
        return datetime.strptime(result, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise EvidenceError(f"{path} is not a valid UTC timestamp") from exc


def require_order(first: datetime, first_path: str, second: datetime, second_path: str) -> None:
    if first > second:
        fail(f"inconsistent time ordering: {first_path} must not be after {second_path}")


def absolute_path(value: Any, path: str) -> PurePosixPath:
    result = text(value, path)
    if "\\" in result:
        fail(f"{path} must use POSIX path syntax")
    parsed = PurePosixPath(result)
    if not parsed.is_absolute() or any(part in ("", ".", "..") for part in parsed.parts):
        fail(f"{path} must be a normalized absolute path")
    if str(parsed) != result:
        fail(f"{path} must be normalized")
    return parsed


def artifact_reference(value: Any, path: str, references: list[str]) -> str:
    artifact_id = stable_id(value, path)
    references.append(artifact_id)
    return artifact_id


def validate_schema_contract() -> None:
    schema_path = Path(__file__).resolve().parent.parent / "release/evidence/schema/target-data-lifecycle-evidence-v2.schema.json"
    try:
        with schema_path.open("r", encoding="utf-8") as handle:
            schema = json.load(handle, object_pairs_hook=reject_duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError, EvidenceError) as exc:
        fail(f"cannot load lifecycle v2 schema: {exc}")
    root = exact_object(
        schema,
        {
            "$schema",
            "$id",
            "title",
            "description",
            "type",
            "additionalProperties",
            "required",
            "properties",
            "allOf",
            "$defs",
        },
        "$schema",
    )
    literal(root["$schema"], "$schema.$schema", "https://json-schema.org/draft/2020-12/schema")
    literal(root["$id"], "$schema.$id", "https://gosuda.org/moreconsensus/schemas/target-data-lifecycle-evidence-2.0.0.json")
    properties = root.get("properties")
    if not isinstance(properties, dict):
        fail("$schema.properties must be an object")
    try:
        schema_version = properties["schema_version"]["const"]
        verifier_version = properties["verifier_version"]["const"]
    except (KeyError, TypeError) as exc:
        raise EvidenceError("lifecycle v2 schema lacks version constants") from exc
    literal(schema_version, "$schema.properties.schema_version.const", SCHEMA_VERSION)
    literal(verifier_version, "$schema.properties.verifier_version.const", VERIFIER_VERSION)


def looks_marker_only(payload: bytes) -> bool:
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError:
        return False
    lines = [line.strip() for line in decoded.splitlines() if line.strip()]
    return bool(lines) and len(lines) <= 4 and all(MARKER_LINE_RE.fullmatch(line) for line in lines)


def validate_artifacts(
    raw: Any,
    report_dir: Path,
    references: list[str],
    started_at: datetime,
    generated_at: datetime,
) -> dict[str, dict[str, Any]]:
    entries = array(raw, "$.raw_artifacts", minimum=1)
    by_id: dict[str, dict[str, Any]] = {}
    paths: set[str] = set()
    for index, value in enumerate(entries):
        path = f"$.raw_artifacts[{index}]"
        item = exact_object(value, {"id", "path", "sha256", "captured_at"}, path)
        artifact_id = stable_id(item["id"], f"{path}.id")
        relative_text = text(item["path"], f"{path}.path")
        declared_digest = sha256(item["sha256"], f"{path}.sha256")
        captured_at = timestamp(item["captured_at"], f"{path}.captured_at")
        require_order(started_at, "$.drill.started_at", captured_at, f"{path}.captured_at")
        require_order(captured_at, f"{path}.captured_at", generated_at, "$.generated_at")
        if artifact_id in by_id:
            fail(f"duplicate raw artifact id: {artifact_id}")
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
            raise EvidenceError(f"{path}.path escapes the evidence directory") from exc
        if not resolved.is_file() or resolved.is_symlink():
            fail(f"raw artifact is not a regular non-symlink file: {relative_text}")
        try:
            payload = resolved.read_bytes()
        except OSError as exc:
            raise EvidenceError(f"cannot read raw artifact {relative_text}: {exc}") from exc
        if not payload:
            fail(f"raw artifact is empty: {relative_text}")
        if looks_marker_only(payload):
            fail(f"raw artifact is marker-only rather than observable evidence: {relative_text}")
        actual_digest = hashlib.sha256(payload).hexdigest()
        if actual_digest != declared_digest:
            fail(f"raw artifact SHA-256 mismatch: {relative_text}")
        by_id[artifact_id] = {"record": item, "payload": payload}

    reference_set = set(references)
    duplicate_references = sorted({item for item in references if references.count(item) > 1})
    if duplicate_references:
        fail(f"raw artifact referenced more than once: {', '.join(duplicate_references)}")
    missing = sorted(reference_set - set(by_id))
    unreferenced = sorted(set(by_id) - reference_set)
    if missing:
        fail(f"missing raw artifact record(s): {', '.join(missing)}")
    if unreferenced:
        fail(f"unreferenced raw artifact record(s): {', '.join(unreferenced)}")
    return by_id


def require_artifact_digest(
    artifact_id: str,
    declared_digest: str,
    by_id: dict[str, dict[str, Any]],
    path: str,
) -> None:
    actual = by_id[artifact_id]["record"]["sha256"]
    if actual != declared_digest:
        fail(f"{path} does not match the referenced raw artifact SHA-256")


def artifact_payload(by_id: dict[str, dict[str, Any]], artifact_id: str, path: str) -> bytes:
    payload = by_id[artifact_id].get("payload")
    if not isinstance(payload, bytes):
        fail(f"{path} has no readable artifact payload")
    return payload


def artifact_json(by_id: dict[str, dict[str, Any]], artifact_id: str, path: str) -> dict[str, Any]:
    try:
        decoded = artifact_payload(by_id, artifact_id, path).decode("utf-8")
        value = json.loads(decoded, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeError, json.JSONDecodeError, EvidenceError) as exc:
        raise EvidenceError(f"{path} must contain one strict JSON record: {exc}") from exc
    if not isinstance(value, dict):
        fail(f"{path} must contain a JSON object")
    return value


def parse_helper_report(by_id: dict[str, dict[str, Any]], artifact_id: str, path: str) -> dict[str, str]:
    try:
        decoded = artifact_payload(by_id, artifact_id, path).decode("utf-8")
    except UnicodeError as exc:
        raise EvidenceError(f"{path} must be a UTF-8 kvcheckpoint report") from exc
    report: dict[str, str] = {}
    for line_number, line in enumerate(decoded.splitlines(), start=1):
        if not line or "=" not in line:
            fail(f"{path} line {line_number} is not a key=value report field")
        key, encoded = line.split("=", 1)
        if not re.fullmatch(r"[a-z][a-z0-9_]*", key) or key in report:
            fail(f"{path} line {line_number} has an invalid or duplicate field")
        try:
            values = shlex.split(encoded, posix=True)
        except ValueError as exc:
            raise EvidenceError(f"{path} line {line_number} has invalid quoting: {exc}") from exc
        if len(values) != 1:
            fail(f"{path} line {line_number} must encode exactly one value")
        report[key] = values[0]
    required = {
        "status",
        "operation",
        "result",
        "data_dir",
        "checkpoint_dir",
        "checkpoint_format",
        "manifest_version",
        "manifest_identity",
        "semantic_state_digest",
        "record_count",
        "applied_count",
        "hard_state_digest",
        "source_identity",
        "release_checksum",
        "current_target_claim",
        "evidence_class",
    }
    missing = sorted(required - set(report))
    extra = sorted(set(report) - required)
    if missing:
        fail(f"{path} missing kvcheckpoint report field(s): {', '.join(missing)}")
    if extra:
        fail(f"{path} contains unknown kvcheckpoint report field(s): {', '.join(extra)}")
    return report


def validate_helper_report(
    report: dict[str, str],
    path: str,
    *,
    operation: str,
    data_dir: str,
    checkpoint_dir: str,
    manifest_identity: str,
    semantic_digest: str,
    hard_state_digest: str,
    record_count: int,
    applied_count: int,
    source_identity: str,
    release_checksum: str,
) -> None:
    expected = {
        "status": "example-operator-report",
        "operation": operation,
        "result": "success",
        "data_dir": data_dir,
        "checkpoint_dir": checkpoint_dir,
        "checkpoint_format": "manifest-v1",
        "manifest_version": "1",
        "manifest_identity": manifest_identity,
        "semantic_state_digest": semantic_digest,
        "record_count": str(record_count),
        "applied_count": str(applied_count),
        "hard_state_digest": hard_state_digest,
        "source_identity": source_identity,
        "release_checksum": release_checksum,
        "current_target_claim": "true",
        "evidence_class": "bounded-data-lifecycle",
    }
    for field, expected_value in expected.items():
        if report[field] != expected_value:
            fail(f"{path}.{field} does not match authenticated lifecycle metadata")


def validate_voters(value: Any, path: str) -> list[int]:
    voters = array(value, path, minimum=1, maximum=7)
    parsed = [integer(voter, f"{path}[{index}]", minimum=1) for index, voter in enumerate(voters)]
    if parsed != sorted(set(parsed)):
        fail(f"{path} must be sorted and unique")
    return parsed


def validate_configuration_history(value: Any, hard_state: dict[str, Any]) -> tuple[int, list[int]]:
    history = array(value, "$.checkpoint.configuration_history", minimum=1)
    previous_voters: list[int] | None = None
    for index, raw in enumerate(history):
        path = f"$.checkpoint.configuration_history[{index}]"
        generation = index + 1
        if index == 0:
            item = exact_object(raw, {"generation", "voters"}, path)
            literal(item["generation"], f"{path}.generation", 1)
        else:
            item = exact_object(
                raw,
                {"generation", "base_generation", "voters", "source_record", "change", "voter", "outcome"},
                path,
            )
            literal(item["generation"], f"{path}.generation", generation)
            literal(item["base_generation"], f"{path}.base_generation", generation - 1)
            stable_id(item["source_record"], f"{path}.source_record")
            change = text(item["change"], f"{path}.change")
            if change not in {"add-voter", "remove-voter"}:
                fail(f"{path}.change must be add-voter or remove-voter")
            changed_voter = integer(item["voter"], f"{path}.voter", minimum=1)
            literal(item["outcome"], f"{path}.outcome", "applied")
        voters = validate_voters(item["voters"], f"{path}.voters")
        if previous_voters is not None:
            previous_set, current_set = set(previous_voters), set(voters)
            if item["change"] == "add-voter":
                if current_set != previous_set | {item["voter"]} or item["voter"] in previous_set:
                    fail(f"{path} does not encode its declared add-voter transition")
            elif current_set != previous_set - {item["voter"]} or item["voter"] not in previous_set:
                fail(f"{path} does not encode its declared remove-voter transition")
        previous_voters = voters
    assert previous_voters is not None
    current_generation = integer(
        hard_state["current_configuration_generation"],
        "$.checkpoint.hard_state.current_configuration_generation",
        minimum=1,
    )
    current_voters = validate_voters(hard_state["current_voters"], "$.checkpoint.hard_state.current_voters")
    if current_generation != len(history):
        fail("HardState configuration generation does not close the full configuration history")
    if current_voters != previous_voters:
        fail("HardState voters do not match the final configuration history entry")
    return current_generation, current_voters


def validate_signer(value: Any, path: str, references: list[str]) -> tuple[str, datetime]:
    signer = exact_object(value, {"identity", "role", "authenticated_by", "signed_at", "result", "artifact_id"}, path)
    identity = text(signer["identity"], f"{path}.identity")
    text(signer["role"], f"{path}.role")
    provider = text(signer["authenticated_by"], f"{path}.authenticated_by")
    if not re.fullmatch(r"https://[^/?#\s]+(?:/[^?#\s]*)?", provider):
        fail(f"{path}.authenticated_by must be a credential-free HTTPS identity-provider URI")
    signed_at = timestamp(signer["signed_at"], f"{path}.signed_at")
    literal(signer["result"], f"{path}.result", "approved")
    artifact_reference(signer["artifact_id"], f"{path}.artifact_id", references)
    return identity, signed_at


def validate_report(
    document: Any,
    report_path: Path,
    self_test_fixture: bool,
    now: datetime,
    expected: argparse.Namespace,
) -> dict[str, str]:
    top = exact_object(
        document,
        {
            "schema_version",
            "verifier_version",
            "evidence_class",
            "release_claim",
            "synthetic_test_fixture",
            "generated_at",
            "valid_until",
            "evidence_root",
            "scope",
            "target",
            "drill",
            "pre_drill",
            "checkpoint",
            "backup",
            "disaster",
            "recovery",
            "pre_publish_rejections",
            "legacy_verification",
            "post_restore",
            "integrity",
            "objectives",
            "observed_commands",
            "sign_off",
            "raw_artifacts",
        },
        "$",
    )
    literal(top["schema_version"], "$.schema_version", SCHEMA_VERSION)
    literal(top["verifier_version"], "$.verifier_version", VERIFIER_VERSION)
    is_synthetic = boolean(top["synthetic_test_fixture"], "$.synthetic_test_fixture")
    if self_test_fixture:
        literal(is_synthetic, "$.synthetic_test_fixture", True)
        literal(top["evidence_class"], "$.evidence_class", SYNTHETIC_EVIDENCE_CLASS)
        literal(top["release_claim"], "$.release_claim", SYNTHETIC_CLAIM)
    else:
        literal(is_synthetic, "$.synthetic_test_fixture", False)
        literal(top["evidence_class"], "$.evidence_class", TARGET_EVIDENCE_CLASS)
        literal(top["release_claim"], "$.release_claim", TARGET_CLAIM)

    generated_at = timestamp(top["generated_at"], "$.generated_at")
    valid_until = timestamp(top["valid_until"], "$.valid_until")
    require_order(generated_at, "$.generated_at", valid_until, "$.valid_until")
    if valid_until > generated_at + MAX_VALIDITY:
        fail("$.valid_until exceeds the 30-day maximum evidence validity")
    if generated_at > now + FUTURE_SKEW:
        fail("evidence was generated in the future")
    if now > valid_until:
        fail("evidence is stale")

    references: list[str] = []
    evidence_root = exact_object(top["evidence_root"], {"filesystem", "external", "mount_read_only", "mount_artifact_id"}, "$.evidence_root")
    literal(evidence_root["filesystem"], "$.evidence_root.filesystem", "apfs")
    literal(evidence_root["external"], "$.evidence_root.external", True)
    literal(evidence_root["mount_read_only"], "$.evidence_root.mount_read_only", True)
    artifact_reference(evidence_root["mount_artifact_id"], "$.evidence_root.mount_artifact_id", references)
    if not self_test_fixture:
        try:
            flags = os.statvfs(report_path.parent).f_flag
        except OSError as exc:
            raise EvidenceError(f"cannot inspect evidence filesystem: {exc}") from exc
        if flags & getattr(os, "ST_RDONLY", 1) == 0:
            fail("target lifecycle v2 evidence root must be mounted read-only")

    scope = exact_object(
        top["scope"],
        {
            "same_host",
            "loopback_only",
            "tls_mode",
            "mtls",
            "client_authorization",
            "peer_authorization",
            "multi_tenant_rbac",
            "client_tls_ca_sha256",
            "client_tls_cert_sha256",
            "admin_tls_ca_sha256",
            "admin_tls_cert_sha256",
            "peer_tls_ca_sha256",
            "peer_tls_identities",
            "tls_identity_sha256",
            "client_mtls_rejection_artifact_id",
            "admin_mtls_rejection_artifact_id",
            "peer_authorization_artifact_ids",
        },
        "$.scope",
    )
    for key, expected_value in {
        "same_host": True,
        "loopback_only": True,
        "tls_mode": "mutual-auth-separated-planes",
        "mtls": True,
        "client_authorization": True,
        "peer_authorization": True,
        "multi_tenant_rbac": False,
    }.items():
        literal(scope[key], f"$.scope.{key}", expected_value)
    for key in (
        "client_tls_ca_sha256",
        "client_tls_cert_sha256",
        "admin_tls_ca_sha256",
        "admin_tls_cert_sha256",
        "peer_tls_ca_sha256",
        "tls_identity_sha256",
    ):
        sha256(scope[key], f"$.scope.{key}")
    raw_peer_identities = scope["peer_tls_identities"]
    if not isinstance(raw_peer_identities, list) or len(raw_peer_identities) != 3:
        fail("$.scope.peer_tls_identities must contain exactly three replica identities")
    peer_identities: list[dict[str, Any]] = []
    for index, raw_peer in enumerate(raw_peer_identities, start=1):
        path = f"$.scope.peer_tls_identities[{index - 1}]"
        peer = exact_object(raw_peer, {"replica_id", "cert_sha256", "uri_san"}, path)
        integer(peer["replica_id"], f"{path}.replica_id", minimum=index, maximum=index)
        sha256(peer["cert_sha256"], f"{path}.cert_sha256")
        literal(peer["uri_san"], f"{path}.uri_san", f"spiffe://gosuda.org/moreconsensus/replica/{index}")
        peer_identities.append(peer)
    artifact_reference(scope["client_mtls_rejection_artifact_id"], "$.scope.client_mtls_rejection_artifact_id", references)
    artifact_reference(scope["admin_mtls_rejection_artifact_id"], "$.scope.admin_mtls_rejection_artifact_id", references)
    raw_peer_authorization_ids = scope["peer_authorization_artifact_ids"]
    expected_peer_authorization_ids = [f"peer-{index}-authorization-required" for index in range(1, 4)]
    if raw_peer_authorization_ids != expected_peer_authorization_ids:
        fail("$.scope.peer_authorization_artifact_ids must contain the ordered per-replica authorization receipts")
    for index, artifact_id in enumerate(raw_peer_authorization_ids):
        artifact_reference(artifact_id, f"$.scope.peer_authorization_artifact_ids[{index}]", references)
    canonical_tls_identity = (
        f"client-ca={scope['client_tls_ca_sha256']}\n"
        f"client-cert={scope['client_tls_cert_sha256']}\n"
        f"admin-ca={scope['admin_tls_ca_sha256']}\n"
        f"admin-cert={scope['admin_tls_cert_sha256']}\n"
        f"peer-ca={scope['peer_tls_ca_sha256']}\n"
    )
    for peer in peer_identities:
        replica_id = peer["replica_id"]
        canonical_tls_identity += (
            f"peer-{replica_id}-cert={peer['cert_sha256']}\n"
            f"peer-{replica_id}-uri={peer['uri_san']}\n"
        )
    tls_identity = hashlib.sha256(canonical_tls_identity.encode("ascii")).hexdigest()
    literal(scope["tls_identity_sha256"], "$.scope.tls_identity_sha256", tls_identity)

    target = exact_object(
        top["target"],
        {
            "target_id",
            "environment_profile",
            "platform",
            "architecture",
            "execution_mode",
            "supervisor",
            "supervisor_domain",
            "filesystem",
            "cluster_id",
            "release_id",
            "source_revision",
            "binary_sha256",
            "provenance_artifact_id",
        },
        "$.target",
    )
    target_id = literal(target["target_id"], "$.target.target_id", TARGET_ID)
    environment_profile = literal(target["environment_profile"], "$.target.environment_profile", ENVIRONMENT_PROFILE)
    for key, expected_value in {
        "platform": "darwin",
        "architecture": "arm64",
        "execution_mode": "native",
        "supervisor": "launchd",
        "supervisor_domain": "system",
        "filesystem": "apfs",
    }.items():
        literal(target[key], f"$.target.{key}", expected_value)
    cluster_id = literal(target["cluster_id"], "$.target.cluster_id", CLUSTER_ID)
    release_id = text(target["release_id"], "$.target.release_id")
    source_revision = text(target["source_revision"], "$.target.source_revision", maximum=64)
    if not REVISION_RE.fullmatch(source_revision):
        fail("$.target.source_revision must be an immutable lowercase 40- or 64-hex revision")
    if release_id != f"mc-kv-{source_revision[:12]}-r1":
        fail("$.target.release_id must be derived from the bound source revision")
    binary_sha256 = sha256(target["binary_sha256"], "$.target.binary_sha256")
    artifact_reference(target["provenance_artifact_id"], "$.target.provenance_artifact_id", references)
    for option, actual, path in (
        (expected.expected_target_id, target_id, "target_id"),
        (expected.expected_release_id, release_id, "release_id"),
        (expected.expected_source_revision, source_revision, "source_revision"),
        (expected.expected_binary_sha256, binary_sha256, "binary_sha256"),
        (expected.expected_environment_profile, environment_profile, "environment_profile"),
    ):
        if option is not None and option != actual:
            fail(f"expected {path} {option!r}, got {actual!r}")

    drill = exact_object(top["drill"], {"drill_id", "executor_identity", "started_at", "completed_at"}, "$.drill")
    stable_id(drill["drill_id"], "$.drill.drill_id")
    executor = text(drill["executor_identity"], "$.drill.executor_identity")
    started_at = timestamp(drill["started_at"], "$.drill.started_at")
    completed_at = timestamp(drill["completed_at"], "$.drill.completed_at")
    require_order(started_at, "$.drill.started_at", completed_at, "$.drill.completed_at")

    pre = exact_object(top["pre_drill"], {"checked_at", "nodes_observed", "healthy_voters", "result", "artifact_id"}, "$.pre_drill")
    pre_at = timestamp(pre["checked_at"], "$.pre_drill.checked_at")
    literal(pre["nodes_observed"], "$.pre_drill.nodes_observed", 3)
    literal(pre["healthy_voters"], "$.pre_drill.healthy_voters", 3)
    literal(pre["result"], "$.pre_drill.result", "pass")
    artifact_reference(pre["artifact_id"], "$.pre_drill.artifact_id", references)

    checkpoint = exact_object(
        top["checkpoint"],
        {
            "checkpoint_id",
            "created_at",
            "source_node",
            "source_store_identity",
            "checkpoint_format",
            "manifest_version",
            "manifest_file_sha256",
            "manifest_artifact_id",
            "manifest_identity_blake3",
            "semantic_state_digest_blake3",
            "hard_state",
            "configuration_history",
            "configuration_history_sha256",
            "configuration_history_artifact_id",
            "record_count",
            "applied_count",
            "snapshot_sha256",
            "snapshot_artifact_id",
            "metadata_report_sha256",
            "metadata_report_artifact_id",
            "release_checksum",
            "current_target_claim",
        },
        "$.checkpoint",
    )
    stable_id(checkpoint["checkpoint_id"], "$.checkpoint.checkpoint_id")
    checkpoint_at = timestamp(checkpoint["created_at"], "$.checkpoint.created_at")
    source_node = text(checkpoint["source_node"], "$.checkpoint.source_node")
    if source_node not in {"node1", "node2", "node3"}:
        fail("$.checkpoint.source_node must identify node1, node2, or node3")
    source_store_identity = text(checkpoint["source_store_identity"], "$.checkpoint.source_store_identity")
    expected_store_identity = f"{target_id}/{cluster_id}/{source_node}"
    if source_store_identity != expected_store_identity:
        fail("checkpoint source_store_identity is not bound to target, cluster, and source node")
    literal(checkpoint["checkpoint_format"], "$.checkpoint.checkpoint_format", "manifest-v1")
    literal(checkpoint["manifest_version"], "$.checkpoint.manifest_version", 1)
    manifest_file_sha256 = sha256(checkpoint["manifest_file_sha256"], "$.checkpoint.manifest_file_sha256")
    manifest_artifact_id = artifact_reference(checkpoint["manifest_artifact_id"], "$.checkpoint.manifest_artifact_id", references)
    manifest_identity = blake3(checkpoint["manifest_identity_blake3"], "$.checkpoint.manifest_identity_blake3")
    semantic_digest = blake3(checkpoint["semantic_state_digest_blake3"], "$.checkpoint.semantic_state_digest_blake3")
    hard_state = exact_object(
        checkpoint["hard_state"],
        {"codec_version", "digest_blake3", "tick", "current_configuration_generation", "current_voters"},
        "$.checkpoint.hard_state",
    )
    literal(hard_state["codec_version"], "$.checkpoint.hard_state.codec_version", 1)
    hard_state_digest = blake3(hard_state["digest_blake3"], "$.checkpoint.hard_state.digest_blake3")
    hard_state_tick = integer(hard_state["tick"], "$.checkpoint.hard_state.tick", minimum=1)
    configuration_generation, _ = validate_configuration_history(checkpoint["configuration_history"], hard_state)
    configuration_history_sha256 = sha256(checkpoint["configuration_history_sha256"], "$.checkpoint.configuration_history_sha256")
    configuration_artifact_id = artifact_reference(
        checkpoint["configuration_history_artifact_id"],
        "$.checkpoint.configuration_history_artifact_id",
        references,
    )
    checkpoint_records = integer(checkpoint["record_count"], "$.checkpoint.record_count", minimum=1)
    checkpoint_applied = integer(checkpoint["applied_count"], "$.checkpoint.applied_count", minimum=1)
    if checkpoint_applied > checkpoint_records:
        fail("$.checkpoint.applied_count must not exceed record_count")
    snapshot_sha256 = sha256(checkpoint["snapshot_sha256"], "$.checkpoint.snapshot_sha256")
    snapshot_artifact_id = artifact_reference(checkpoint["snapshot_artifact_id"], "$.checkpoint.snapshot_artifact_id", references)
    metadata_report_sha256 = sha256(checkpoint["metadata_report_sha256"], "$.checkpoint.metadata_report_sha256")
    metadata_artifact_id = artifact_reference(checkpoint["metadata_report_artifact_id"], "$.checkpoint.metadata_report_artifact_id", references)
    release_checksum = text(checkpoint["release_checksum"], "$.checkpoint.release_checksum")
    if release_checksum != f"sha256:{binary_sha256}":
        fail("checkpoint release_checksum is not bound to the target binary SHA-256")
    literal(checkpoint["current_target_claim"], "$.checkpoint.current_target_claim", True)

    backup = exact_object(
        top["backup"],
        {
            "created_at",
            "source_path",
            "backup_path",
            "source_filesystem",
            "backup_filesystem",
            "off_directory_copy",
            "source_directory_id",
            "backup_directory_id",
            "same_file",
            "independent_file_copies_verified",
            "source_snapshot_sha256",
            "backup_snapshot_sha256",
            "copy_result",
            "artifact_id",
        },
        "$.backup",
    )
    backup_at = timestamp(backup["created_at"], "$.backup.created_at")
    source_path = absolute_path(backup["source_path"], "$.backup.source_path")
    backup_path = absolute_path(backup["backup_path"], "$.backup.backup_path")
    if source_path == backup_path or source_path in backup_path.parents or backup_path in source_path.parents:
        fail("backup must be an off-directory copy, not the source or a nested alias")
    literal(backup["source_filesystem"], "$.backup.source_filesystem", "apfs")
    literal(backup["backup_filesystem"], "$.backup.backup_filesystem", "apfs")
    literal(backup["off_directory_copy"], "$.backup.off_directory_copy", True)
    source_directory_id = text(backup["source_directory_id"], "$.backup.source_directory_id")
    backup_directory_id = text(backup["backup_directory_id"], "$.backup.backup_directory_id")
    if source_directory_id == backup_directory_id:
        fail("backup source and copy must have different APFS directory identities")
    literal(backup["same_file"], "$.backup.same_file", False)
    literal(backup["independent_file_copies_verified"], "$.backup.independent_file_copies_verified", True)
    backup_source_snapshot = sha256(backup["source_snapshot_sha256"], "$.backup.source_snapshot_sha256")
    backup_copy_snapshot = sha256(backup["backup_snapshot_sha256"], "$.backup.backup_snapshot_sha256")
    if backup_source_snapshot != snapshot_sha256 or backup_copy_snapshot != snapshot_sha256:
        fail("APFS backup snapshot digest does not match the authenticated checkpoint snapshot")
    literal(backup["copy_result"], "$.backup.copy_result", "pass")
    artifact_reference(backup["artifact_id"], "$.backup.artifact_id", references)

    disaster = exact_object(top["disaster"], {"scenario", "occurred_at", "affected_node", "result", "artifact_id"}, "$.disaster")
    literal(disaster["scenario"], "$.disaster.scenario", "stopped-node-data-directory-loss")
    disaster_at = timestamp(disaster["occurred_at"], "$.disaster.occurred_at")
    affected_node = text(disaster["affected_node"], "$.disaster.affected_node")
    if affected_node != source_node:
        fail("disaster affected_node must match checkpoint source_node")
    literal(disaster["result"], "$.disaster.result", "pass")
    artifact_reference(disaster["artifact_id"], "$.disaster.artifact_id", references)

    recovery = exact_object(
        top["recovery"],
        {
            "node_stopped_at",
            "recovery_started_at",
            "operation",
            "stopped_node",
            "stopped_node_confirmed",
            "restore_target_id",
            "restore_cluster_id",
            "restore_node",
            "restore_store_identity",
            "destination_path",
            "destination_initially_absent",
            "stage_path",
            "stage_initially_empty",
            "quarantine",
            "staged_verification",
            "atomic_publish_result",
            "published_at",
            "restore_report_sha256",
            "restore_report_artifact_id",
        },
        "$.recovery",
    )
    node_stopped_at = timestamp(recovery["node_stopped_at"], "$.recovery.node_stopped_at")
    recovery_started_at = timestamp(recovery["recovery_started_at"], "$.recovery.recovery_started_at")
    literal(recovery["operation"], "$.recovery.operation", "restore")
    stopped_node = text(recovery["stopped_node"], "$.recovery.stopped_node")
    if stopped_node != affected_node:
        fail("recovery stopped_node must match disaster affected_node")
    literal(recovery["stopped_node_confirmed"], "$.recovery.stopped_node_confirmed", True)
    literal(recovery["restore_target_id"], "$.recovery.restore_target_id", target_id)
    literal(recovery["restore_cluster_id"], "$.recovery.restore_cluster_id", cluster_id)
    literal(recovery["restore_node"], "$.recovery.restore_node", source_node)
    literal(recovery["restore_store_identity"], "$.recovery.restore_store_identity", source_store_identity)
    destination_path = absolute_path(recovery["destination_path"], "$.recovery.destination_path")
    stage_path = absolute_path(recovery["stage_path"], "$.recovery.stage_path")
    if destination_path == stage_path or destination_path.parent != stage_path.parent:
        fail("restore stage must be a distinct sibling of the final destination")
    literal(recovery["destination_initially_absent"], "$.recovery.destination_initially_absent", True)
    literal(recovery["stage_initially_empty"], "$.recovery.stage_initially_empty", True)
    quarantine = exact_object(recovery["quarantine"], {"path", "created_at", "read_only", "result", "artifact_id"}, "$.recovery.quarantine")
    quarantine_path = absolute_path(quarantine["path"], "$.recovery.quarantine.path")
    if quarantine_path in {destination_path, stage_path} or destination_path in quarantine_path.parents:
        fail("quarantine path must be distinct from restore destination and stage")
    quarantine_at = timestamp(quarantine["created_at"], "$.recovery.quarantine.created_at")
    literal(quarantine["read_only"], "$.recovery.quarantine.read_only", True)
    literal(quarantine["result"], "$.recovery.quarantine.result", "pass")
    artifact_reference(quarantine["artifact_id"], "$.recovery.quarantine.artifact_id", references)
    staged = exact_object(
        recovery["staged_verification"],
        {
            "verified_at",
            "manifest_identity_blake3",
            "semantic_state_digest_blake3",
            "hard_state_digest_blake3",
            "hard_state_tick",
            "configuration_generation",
            "configuration_history_sha256",
            "record_count",
            "applied_count",
            "snapshot_sha256",
            "checksum_result",
            "configuration_result",
            "ownership_result",
            "artifact_id",
        },
        "$.recovery.staged_verification",
    )
    staged_at = timestamp(staged["verified_at"], "$.recovery.staged_verification.verified_at")
    for field, expected_value, validator in (
        ("manifest_identity_blake3", manifest_identity, blake3),
        ("semantic_state_digest_blake3", semantic_digest, blake3),
        ("hard_state_digest_blake3", hard_state_digest, blake3),
        ("configuration_history_sha256", configuration_history_sha256, sha256),
        ("snapshot_sha256", snapshot_sha256, sha256),
    ):
        actual = validator(staged[field], f"$.recovery.staged_verification.{field}")
        if actual != expected_value:
            fail(f"staged restore {field} does not match the source checkpoint")
    if integer(staged["hard_state_tick"], "$.recovery.staged_verification.hard_state_tick", minimum=1) != hard_state_tick:
        fail("staged restore contains stale or mismatched HardState tick")
    if integer(staged["configuration_generation"], "$.recovery.staged_verification.configuration_generation", minimum=1) != configuration_generation:
        fail("staged restore omits or changes the current configuration generation")
    if integer(staged["record_count"], "$.recovery.staged_verification.record_count", minimum=1) != checkpoint_records:
        fail("staged restore record count differs from the manifest")
    if integer(staged["applied_count"], "$.recovery.staged_verification.applied_count", minimum=1) != checkpoint_applied:
        fail("staged restore applied count differs from the manifest")
    for field in ("checksum_result", "configuration_result", "ownership_result"):
        literal(staged[field], f"$.recovery.staged_verification.{field}", "pass")
    artifact_reference(staged["artifact_id"], "$.recovery.staged_verification.artifact_id", references)
    literal(recovery["atomic_publish_result"], "$.recovery.atomic_publish_result", "pass")
    published_at = timestamp(recovery["published_at"], "$.recovery.published_at")
    restore_report_sha256 = sha256(recovery["restore_report_sha256"], "$.recovery.restore_report_sha256")
    restore_report_artifact_id = artifact_reference(recovery["restore_report_artifact_id"], "$.recovery.restore_report_artifact_id", references)

    rejection_cases: set[str] = set()
    for index, raw_rejection in enumerate(array(top["pre_publish_rejections"], "$.pre_publish_rejections", 4, 4)):
        path = f"$.pre_publish_rejections[{index}]"
        rejection = exact_object(
            raw_rejection,
            {
                "case",
                "attempted_target_id",
                "attempted_cluster_id",
                "attempted_source_identity",
                "attempted_release_checksum",
                "command",
                "exit_code",
                "result",
                "destination_before_sha256",
                "destination_after_sha256",
                "destination_mutated",
                "publish_attempted",
                "quarantined",
                "transcript_artifact_id",
                "quarantine_artifact_id",
            },
            path,
        )
        case = text(rejection["case"], f"{path}.case")
        if case not in REQUIRED_REJECTION_CASES or case in rejection_cases:
            fail(f"{path}.case is missing, duplicate, or unsupported")
        rejection_cases.add(case)
        attempted_target = text(rejection["attempted_target_id"], f"{path}.attempted_target_id")
        attempted_cluster = stable_id(rejection["attempted_cluster_id"], f"{path}.attempted_cluster_id")
        attempted_source = text(rejection["attempted_source_identity"], f"{path}.attempted_source_identity")
        attempted_release = text(rejection["attempted_release_checksum"], f"{path}.attempted_release_checksum")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", attempted_release):
            fail(f"{path}.attempted_release_checksum must carry an exact SHA-256 algorithm label")
        command = text(rejection["command"], f"{path}.command")
        try:
            tokens = shlex.split(command)
        except ValueError as exc:
            raise EvidenceError(f"{path}.command is not parseable: {exc}") from exc
        if "restore" not in tokens and "verify" not in tokens:
            fail(f"{path}.command must observe a verify or restore rejection")
        integer(rejection["exit_code"], f"{path}.exit_code", minimum=1, maximum=255)
        literal(rejection["result"], f"{path}.result", "rejected-before-publish")
        before = sha256(rejection["destination_before_sha256"], f"{path}.destination_before_sha256")
        after = sha256(rejection["destination_after_sha256"], f"{path}.destination_after_sha256")
        if before != after:
            fail(f"{path} mutated the destination while rejecting invalid input")
        literal(rejection["destination_mutated"], f"{path}.destination_mutated", False)
        literal(rejection["publish_attempted"], f"{path}.publish_attempted", False)
        literal(rejection["quarantined"], f"{path}.quarantined", True)
        artifact_reference(rejection["transcript_artifact_id"], f"{path}.transcript_artifact_id", references)
        artifact_reference(rejection["quarantine_artifact_id"], f"{path}.quarantine_artifact_id", references)
        if case in {"corrupt-manifest", "truncated-manifest"}:
            if (attempted_target, attempted_cluster, attempted_source, attempted_release) != (
                target_id,
                cluster_id,
                source_store_identity,
                release_checksum,
            ):
                fail(f"{path} must isolate byte corruption from identity mismatches")
        elif case == "mismatched-metadata":
            if attempted_target != target_id or attempted_cluster != cluster_id:
                fail(f"{path} must isolate metadata mismatch within the current target")
            if attempted_source == source_store_identity and attempted_release == release_checksum:
                fail(f"{path} does not contain mismatched authenticated metadata")
        elif attempted_target == target_id and attempted_cluster == cluster_id:
            fail(f"{path} does not exercise a cross-cluster restore rejection")
    if rejection_cases != REQUIRED_REJECTION_CASES:
        fail("pre_publish_rejections does not cover every required adversarial case")

    legacy = exact_object(
        top["legacy_verification"],
        {"command", "exit_code", "checkpoint_format", "current_target_claim", "release_claim", "publish_attempted", "result", "artifact_id"},
        "$.legacy_verification",
    )
    legacy_command = text(legacy["command"], "$.legacy_verification.command")
    try:
        legacy_tokens = shlex.split(legacy_command)
    except ValueError as exc:
        raise EvidenceError(f"$.legacy_verification.command is not parseable: {exc}") from exc
    if "verify" not in legacy_tokens or "--legacy" not in legacy_tokens or "restore" in legacy_tokens:
        fail("legacy verification must use the explicit verify --legacy path and must not restore")
    literal(legacy["exit_code"], "$.legacy_verification.exit_code", 0)
    literal(legacy["checkpoint_format"], "$.legacy_verification.checkpoint_format", "legacy")
    literal(legacy["current_target_claim"], "$.legacy_verification.current_target_claim", False)
    literal(legacy["release_claim"], "$.legacy_verification.release_claim", "none")
    literal(legacy["publish_attempted"], "$.legacy_verification.publish_attempted", False)
    literal(legacy["result"], "$.legacy_verification.result", "verified-legacy-non-current")
    artifact_reference(legacy["artifact_id"], "$.legacy_verification.artifact_id", references)

    post_restore = exact_object(top["post_restore"], {"service_restored_at", "node_probes", "convergence", "post_rejoin_checkpoint_artifact_id"}, "$.post_restore")
    service_restored_at = timestamp(post_restore["service_restored_at"], "$.post_restore.service_restored_at")
    observed_nodes: set[str] = set()
    for index, raw_probe in enumerate(array(post_restore["node_probes"], "$.post_restore.node_probes", 3, 3)):
        path = f"$.post_restore.node_probes[{index}]"
        probe = exact_object(
            raw_probe,
            {"node", "health", "readiness", "pre_checkpoint_canary", "post_checkpoint_canary", "bounded_scan", "result", "artifact_id"},
            path,
        )
        node = text(probe["node"], f"{path}.node")
        if node not in {"node1", "node2", "node3"} or node in observed_nodes:
            fail(f"{path}.node must be one unique cluster node")
        observed_nodes.add(node)
        for field in ("health", "readiness", "pre_checkpoint_canary", "post_checkpoint_canary", "bounded_scan", "result"):
            literal(probe[field], f"{path}.{field}", "pass")
        artifact_reference(probe["artifact_id"], f"{path}.artifact_id", references)
    if observed_nodes != {"node1", "node2", "node3"}:
        fail("post-restore application probes must cover all three nodes")
    convergence = exact_object(
        post_restore["convergence"],
        {
            "measurement_kind",
            "observed_at",
            "observation_window_seconds",
            "restored_node_served_post_checkpoint_value",
            "all_nodes_served_pre_checkpoint_value",
            "all_nodes_served_post_checkpoint_value",
            "send_queues_stable",
            "max_observed_send_queue_depth",
            "post_rejoin_checkpoint_verified",
            "exact_zero_lag_claimed",
            "result",
            "artifact_id",
        },
        "$.post_restore.convergence",
    )
    literal(convergence["measurement_kind"], "$.post_restore.convergence.measurement_kind", "bounded-observable-convergence")
    convergence_at = timestamp(convergence["observed_at"], "$.post_restore.convergence.observed_at")
    integer(convergence["observation_window_seconds"], "$.post_restore.convergence.observation_window_seconds", minimum=1)
    for field in (
        "restored_node_served_post_checkpoint_value",
        "all_nodes_served_pre_checkpoint_value",
        "all_nodes_served_post_checkpoint_value",
        "send_queues_stable",
        "post_rejoin_checkpoint_verified",
    ):
        literal(convergence[field], f"$.post_restore.convergence.{field}", True)
    integer(convergence["max_observed_send_queue_depth"], "$.post_restore.convergence.max_observed_send_queue_depth", minimum=0)
    literal(convergence["exact_zero_lag_claimed"], "$.post_restore.convergence.exact_zero_lag_claimed", False)
    literal(convergence["result"], "$.post_restore.convergence.result", "pass")
    artifact_reference(convergence["artifact_id"], "$.post_restore.convergence.artifact_id", references)
    artifact_reference(
        post_restore["post_rejoin_checkpoint_artifact_id"],
        "$.post_restore.post_rejoin_checkpoint_artifact_id",
        references,
    )

    integrity = exact_object(
        top["integrity"],
        {
            "checked_at",
            "restored_manifest_identity_blake3",
            "restored_hard_state_digest_blake3",
            "restored_configuration_generation",
            "restored_configuration_history_sha256",
            "restored_record_count",
            "restored_applied_count",
            "post_rejoin_record_count",
            "post_rejoin_applied_count",
            "duplicate_applications",
            "data_loss_records",
            "result",
            "artifact_id",
        },
        "$.integrity",
    )
    integrity_at = timestamp(integrity["checked_at"], "$.integrity.checked_at")
    if blake3(integrity["restored_manifest_identity_blake3"], "$.integrity.restored_manifest_identity_blake3") != manifest_identity:
        fail("restored manifest identity differs from the source checkpoint")
    if blake3(integrity["restored_hard_state_digest_blake3"], "$.integrity.restored_hard_state_digest_blake3") != hard_state_digest:
        fail("restored HardState digest differs from the source checkpoint")
    if integer(integrity["restored_configuration_generation"], "$.integrity.restored_configuration_generation", minimum=1) != configuration_generation:
        fail("restored configuration generation differs from the full source history")
    if sha256(integrity["restored_configuration_history_sha256"], "$.integrity.restored_configuration_history_sha256") != configuration_history_sha256:
        fail("restored configuration history differs from the source checkpoint")
    restored_records = integer(integrity["restored_record_count"], "$.integrity.restored_record_count", minimum=1)
    restored_applied = integer(integrity["restored_applied_count"], "$.integrity.restored_applied_count", minimum=1)
    if restored_records != checkpoint_records or restored_applied != checkpoint_applied:
        fail("immediate restored record/applied counts differ from the checkpoint manifest")
    post_records = integer(integrity["post_rejoin_record_count"], "$.integrity.post_rejoin_record_count", minimum=1)
    post_applied = integer(integrity["post_rejoin_applied_count"], "$.integrity.post_rejoin_applied_count", minimum=1)
    if post_records < restored_records or post_applied < restored_applied:
        fail("post-rejoin record/applied counts regressed")
    literal(integrity["duplicate_applications"], "$.integrity.duplicate_applications", 0)
    literal(integrity["data_loss_records"], "$.integrity.data_loss_records", 0)
    literal(integrity["result"], "$.integrity.result", "pass")
    artifact_reference(integrity["artifact_id"], "$.integrity.artifact_id", references)

    objectives = exact_object(top["objectives"], {"rpo", "rto", "artifact_id"}, "$.objectives")
    rpo = exact_object(objectives["rpo"], {"checkpoint_at", "disaster_at", "measured_seconds", "threshold_seconds", "result"}, "$.objectives.rpo")
    rpo_checkpoint_at = timestamp(rpo["checkpoint_at"], "$.objectives.rpo.checkpoint_at")
    rpo_disaster_at = timestamp(rpo["disaster_at"], "$.objectives.rpo.disaster_at")
    if rpo_checkpoint_at != checkpoint_at or rpo_disaster_at != disaster_at:
        fail("RPO endpoints must equal observed checkpoint and disaster timestamps")
    rpo_measured = integer(rpo["measured_seconds"], "$.objectives.rpo.measured_seconds", minimum=0)
    rpo_threshold = integer(rpo["threshold_seconds"], "$.objectives.rpo.threshold_seconds", minimum=1)
    if rpo_measured != int((disaster_at - checkpoint_at).total_seconds()) or rpo_measured > rpo_threshold:
        fail("RPO measurement is invented, inconsistent, or over threshold")
    literal(rpo["result"], "$.objectives.rpo.result", "met")
    rto = exact_object(objectives["rto"], {"disaster_at", "service_restored_at", "measured_seconds", "threshold_seconds", "result"}, "$.objectives.rto")
    rto_disaster_at = timestamp(rto["disaster_at"], "$.objectives.rto.disaster_at")
    rto_restored_at = timestamp(rto["service_restored_at"], "$.objectives.rto.service_restored_at")
    if rto_disaster_at != disaster_at or rto_restored_at != service_restored_at:
        fail("RTO endpoints must equal observed disaster and service-restored timestamps")
    rto_measured = integer(rto["measured_seconds"], "$.objectives.rto.measured_seconds", minimum=0)
    rto_threshold = integer(rto["threshold_seconds"], "$.objectives.rto.threshold_seconds", minimum=1)
    if rto_measured != int((service_restored_at - disaster_at).total_seconds()) or rto_measured > rto_threshold:
        fail("RTO measurement is invented, inconsistent, or over threshold")
    literal(rto["result"], "$.objectives.rto.result", "met")
    artifact_reference(objectives["artifact_id"], "$.objectives.artifact_id", references)

    observed_steps: set[str] = set()
    command_times: list[tuple[datetime, datetime, str]] = []
    command_artifacts: dict[str, tuple[str, dict[str, Any]]] = {}
    for index, raw_observation in enumerate(array(top["observed_commands"], "$.observed_commands", minimum=9, maximum=9)):
        path = f"$.observed_commands[{index}]"
        observation = exact_object(raw_observation, {"step", "command", "started_at", "completed_at", "exit_code", "result", "artifact_id"}, path)
        step = text(observation["step"], f"{path}.step")
        if step not in REQUIRED_COMMAND_STEPS or step in observed_steps:
            fail(f"{path}.step is missing, duplicate, or unsupported")
        observed_steps.add(step)
        command = text(observation["command"], f"{path}.command")
        try:
            tokens = shlex.split(command)
        except ValueError as exc:
            raise EvidenceError(f"{path}.command is not parseable: {exc}") from exc
        if not tokens:
            fail(f"{path}.command must contain an observed command")
        if step == "checkpoint" and "checkpoint" not in tokens:
            fail(f"{path}.command does not execute checkpoint")
        if step in {"verify-current", "verify-backup", "verify-stage"}:
            if "verify" not in tokens or "--legacy" in tokens:
                fail(f"{path}.command must execute current manifest verification")
        if step == "copy-backup" and not any(token.rsplit("/", 1)[-1] in {"cp", "ditto", "rsync"} for token in tokens):
            fail(f"{path}.command must observe a native off-directory copy")
        command_started = timestamp(observation["started_at"], f"{path}.started_at")
        command_completed = timestamp(observation["completed_at"], f"{path}.completed_at")
        require_order(command_started, f"{path}.started_at", command_completed, f"{path}.completed_at")
        command_times.append((command_started, command_completed, path))
        literal(observation["exit_code"], f"{path}.exit_code", 0)
        literal(observation["result"], f"{path}.result", "pass")
        observation_artifact_id = artifact_reference(observation["artifact_id"], f"{path}.artifact_id", references)
        command_artifacts[step] = (observation_artifact_id, observation)
    if observed_steps != REQUIRED_COMMAND_STEPS:
        fail("observed_commands does not cover every required lifecycle operation")

    sign_off = exact_object(top["sign_off"], {"operator", "independent_reviewer"}, "$.sign_off")
    operator, operator_at = validate_signer(sign_off["operator"], "$.sign_off.operator", references)
    reviewer, reviewer_at = validate_signer(sign_off["independent_reviewer"], "$.sign_off.independent_reviewer", references)
    if operator.casefold() == reviewer.casefold():
        fail("operator and independent reviewer must be different people")
    if reviewer.casefold() == executor.casefold():
        fail("executor-only sign-off is forbidden; an independent reviewer is required")

    for first, first_path, second, second_path in (
        (started_at, "$.drill.started_at", pre_at, "$.pre_drill.checked_at"),
        (pre_at, "$.pre_drill.checked_at", checkpoint_at, "$.checkpoint.created_at"),
        (checkpoint_at, "$.checkpoint.created_at", backup_at, "$.backup.created_at"),
        (backup_at, "$.backup.created_at", disaster_at, "$.disaster.occurred_at"),
        (disaster_at, "$.disaster.occurred_at", node_stopped_at, "$.recovery.node_stopped_at"),
        (node_stopped_at, "$.recovery.node_stopped_at", recovery_started_at, "$.recovery.recovery_started_at"),
        (recovery_started_at, "$.recovery.recovery_started_at", quarantine_at, "$.recovery.quarantine.created_at"),
        (quarantine_at, "$.recovery.quarantine.created_at", staged_at, "$.recovery.staged_verification.verified_at"),
        (staged_at, "$.recovery.staged_verification.verified_at", published_at, "$.recovery.published_at"),
        (published_at, "$.recovery.published_at", service_restored_at, "$.post_restore.service_restored_at"),
        (service_restored_at, "$.post_restore.service_restored_at", convergence_at, "$.post_restore.convergence.observed_at"),
        (convergence_at, "$.post_restore.convergence.observed_at", integrity_at, "$.integrity.checked_at"),
        (integrity_at, "$.integrity.checked_at", completed_at, "$.drill.completed_at"),
        (completed_at, "$.drill.completed_at", operator_at, "$.sign_off.operator.signed_at"),
        (operator_at, "$.sign_off.operator.signed_at", reviewer_at, "$.sign_off.independent_reviewer.signed_at"),
        (reviewer_at, "$.sign_off.independent_reviewer.signed_at", generated_at, "$.generated_at"),
    ):
        require_order(first, first_path, second, second_path)
    for command_started, command_completed, path in command_times:
        require_order(started_at, "$.drill.started_at", command_started, f"{path}.started_at")
        require_order(command_completed, f"{path}.completed_at", completed_at, "$.drill.completed_at")
    if generated_at - completed_at > MAX_GENERATION_LAG:
        fail("evidence generated more than 24 hours after drill completion is stale")

    by_id = validate_artifacts(top["raw_artifacts"], report_path.parent.resolve(), references, started_at, generated_at)
    for plane, artifact_key, ca_key in (
        ("client", "client_mtls_rejection_artifact_id", "client_tls_ca_sha256"),
        ("admin", "admin_mtls_rejection_artifact_id", "admin_tls_ca_sha256"),
    ):
        artifact_id = scope[artifact_key]
        path = f"$.scope.{artifact_key}"
        probe = exact_object(
            artifact_json(by_id, artifact_id, path),
            {
                "schema",
                "plane",
                "target_origin",
                "tls_version",
                "trusted_ca_sha256",
                "client_certificate_present",
                "handshake_rejected",
                "authenticated_control_before_and_after",
            },
            path,
        )
        literal(probe["schema"], f"{path}.schema", "moreconsensus.lifecycle-mtls-negative-probe.v1")
        literal(probe["plane"], f"{path}.plane", plane)
        origin = text(probe["target_origin"], f"{path}.target_origin")
        if re.fullmatch(r"https://127\.0\.0\.1:[0-9]+", origin) is None:
            fail(f"{path}.target_origin must be an exact loopback HTTPS origin")
        literal(probe["tls_version"], f"{path}.tls_version", "1.3")
        literal(probe["trusted_ca_sha256"], f"{path}.trusted_ca_sha256", scope[ca_key])
        literal(probe["client_certificate_present"], f"{path}.client_certificate_present", False)
        literal(probe["handshake_rejected"], f"{path}.handshake_rejected", True)
        literal(probe["authenticated_control_before_and_after"], f"{path}.authenticated_control_before_and_after", True)
    for destination_index, artifact_id in enumerate(raw_peer_authorization_ids, start=1):
        path = f"$.scope.peer_authorization_artifact_ids[{destination_index - 1}]"
        sender_id = destination_index % 3 + 1
        probe = exact_object(
            artifact_json(by_id, artifact_id, path),
            {
                "schema",
                "destination_replica_id",
                "authenticated_sender_replica_id",
                "authenticated_sender_uri_san",
                "trusted_ca_sha256",
                "tls_version",
                "authenticated_handshake_accepted",
                "no_certificate_handshake_rejected",
                "certificate_message_sender_mismatch_rejected",
            },
            path,
        )
        literal(probe["schema"], f"{path}.schema", "moreconsensus.lifecycle-peer-authorization-probe.v1")
        literal(probe["destination_replica_id"], f"{path}.destination_replica_id", destination_index)
        literal(probe["authenticated_sender_replica_id"], f"{path}.authenticated_sender_replica_id", sender_id)
        literal(
            probe["authenticated_sender_uri_san"],
            f"{path}.authenticated_sender_uri_san",
            f"spiffe://gosuda.org/moreconsensus/replica/{sender_id}",
        )
        literal(probe["trusted_ca_sha256"], f"{path}.trusted_ca_sha256", scope["peer_tls_ca_sha256"])
        literal(probe["tls_version"], f"{path}.tls_version", "1.3")
        literal(probe["authenticated_handshake_accepted"], f"{path}.authenticated_handshake_accepted", True)
        literal(probe["no_certificate_handshake_rejected"], f"{path}.no_certificate_handshake_rejected", True)
        literal(
            probe["certificate_message_sender_mismatch_rejected"],
            f"{path}.certificate_message_sender_mismatch_rejected",
            True,
        )
    require_artifact_digest(manifest_artifact_id, manifest_file_sha256, by_id, "$.checkpoint.manifest_file_sha256")
    require_artifact_digest(configuration_artifact_id, configuration_history_sha256, by_id, "$.checkpoint.configuration_history_sha256")
    require_artifact_digest(snapshot_artifact_id, snapshot_sha256, by_id, "$.checkpoint.snapshot_sha256")
    require_artifact_digest(metadata_artifact_id, metadata_report_sha256, by_id, "$.checkpoint.metadata_report_sha256")
    require_artifact_digest(restore_report_artifact_id, restore_report_sha256, by_id, "$.recovery.restore_report_sha256")

    metadata_report = parse_helper_report(by_id, metadata_artifact_id, "$.checkpoint.metadata_report_artifact_id")
    validate_helper_report(
        metadata_report,
        "$.checkpoint.metadata_report_artifact_id",
        operation="verify",
        data_dir="",
        checkpoint_dir=str(source_path),
        manifest_identity=manifest_identity,
        semantic_digest=semantic_digest,
        hard_state_digest=hard_state_digest,
        record_count=checkpoint_records,
        applied_count=checkpoint_applied,
        source_identity=source_store_identity,
        release_checksum=release_checksum,
    )
    restore_report = parse_helper_report(by_id, restore_report_artifact_id, "$.recovery.restore_report_artifact_id")
    validate_helper_report(
        restore_report,
        "$.recovery.restore_report_artifact_id",
        operation="restore",
        data_dir=str(stage_path),
        checkpoint_dir=str(backup_path),
        manifest_identity=manifest_identity,
        semantic_digest=semantic_digest,
        hard_state_digest=hard_state_digest,
        record_count=checkpoint_records,
        applied_count=checkpoint_applied,
        source_identity=source_store_identity,
        release_checksum=release_checksum,
    )

    configuration_record = exact_object(
        artifact_json(by_id, configuration_artifact_id, "$.checkpoint.configuration_history_artifact_id"),
        {"schema", "hard_state", "configuration_history", "verification_result"},
        "$.checkpoint.configuration_history_artifact_id",
    )
    literal(
        configuration_record["schema"],
        "$.checkpoint.configuration_history_artifact_id.schema",
        "moreconsensus.epaxos-configuration-history.v1",
    )
    if configuration_record["hard_state"] != checkpoint["hard_state"]:
        fail("configuration-history artifact HardState does not match checkpoint evidence")
    if configuration_record["configuration_history"] != checkpoint["configuration_history"]:
        fail("configuration-history artifact does not contain the declared full configuration chain")
    literal(
        configuration_record["verification_result"],
        "$.checkpoint.configuration_history_artifact_id.verification_result",
        "pass",
    )

    for step, (artifact_id, observation) in command_artifacts.items():
        artifact_path = f"$.observed_commands[{step}].artifact_id"
        command_record = exact_object(
            artifact_json(by_id, artifact_id, artifact_path),
            {
                "schema",
                "verifier_version",
                "target_id",
                "release_id",
                "source_revision",
                "binary_sha256",
                "environment_profile",
                "step",
                "command",
                "started_at",
                "completed_at",
                "exit_code",
                "result",
            },
            artifact_path,
        )
        expected_command_record = {
            "schema": "moreconsensus.lifecycle-command-observation.v1",
            "verifier_version": VERIFIER_VERSION,
            "target_id": target_id,
            "release_id": release_id,
            "source_revision": source_revision,
            "binary_sha256": binary_sha256,
            "environment_profile": environment_profile,
            "step": observation["step"],
            "command": observation["command"],
            "started_at": observation["started_at"],
            "completed_at": observation["completed_at"],
            "exit_code": observation["exit_code"],
            "result": observation["result"],
        }
        if command_record != expected_command_record:
            fail(f"{artifact_path} is not bound to its command, result, target, release, source, binary, profile, and verifier")

    return {
        "target_id": target_id,
        "release_id": release_id,
        "source_revision": source_revision,
        "binary_sha256": binary_sha256,
        "environment_profile": environment_profile,
        "claim": SYNTHETIC_CLAIM if self_test_fixture else TARGET_CLAIM,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify native Darwin checkpoint/restore lifecycle evidence v2 without altering a target."
    )
    parser.add_argument(
        "--self-test-fixture",
        action="store_true",
        help="accept only an explicitly synthetic non-claim fixture; bypasses the physical read-only mount check",
    )
    parser.add_argument("--expected-target-id")
    parser.add_argument("--expected-release-id")
    parser.add_argument("--expected-source-revision")
    parser.add_argument("--expected-binary-sha256")
    parser.add_argument("--expected-environment-profile")
    parser.add_argument("report", type=Path)
    return parser.parse_args()


def resolved_regular_input(path: Path) -> Path:
    if path.is_symlink():
        fail("report path must not be a symbolic link")
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        fail("report must be a regular non-symlink file")
    return resolved


def main() -> int:
    args = parse_args()
    try:
        validate_schema_contract()
        report_path = resolved_regular_input(args.report)
        with report_path.open("r", encoding="utf-8") as handle:
            document = json.load(handle, object_pairs_hook=reject_duplicate_keys)
        binding = validate_report(document, report_path, args.self_test_fixture, datetime.now(timezone.utc), args)
    except (EvidenceError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"target-data-lifecycle-evidence-v2 status=fail verifier_version={VERIFIER_VERSION} reason={exc}", file=sys.stderr)
        return 1

    mode = "synthetic-test-fixture" if args.self_test_fixture else "target-evidence"
    print(
        "target-data-lifecycle-evidence-v2 status=pass "
        f"verifier_version={VERIFIER_VERSION} mode={mode} "
        f"release_claim={binding['claim']} target_id={binding['target_id']} "
        f"release_id={binding['release_id']} source_revision={binding['source_revision']} "
        f"binary_sha256={binding['binary_sha256']} environment_profile={binding['environment_profile']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
