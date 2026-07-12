#!/usr/bin/env python3
"""Fail-closed verifier for authoritative release-item closure evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import os
import plistlib
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

INDEX_SCHEMA = "moreconsensus.release-closure-index.v2"
RECORD_SCHEMA = "moreconsensus.release-evidence-record.v2"
RELEASE_INDEX_MODE = "darwin-production-v2"
FIXTURE_INDEX_MODE = "synthetic-test-fixture"
TARGET_ID = "mc-kv-darwin24-arm64-launchd-3n-r1"
ENVIRONMENT_PROFILE = "native-darwin24-arm64-launchd-system-domain-v1"
RELEASE_EVIDENCE_MODE = {
    "broader-formal": "formal-closure-v2",
    "deployment-manifest": "darwin-deployment-v2",
    "data-lifecycle": "darwin-data-lifecycle-v2",
    "capacity-envelope": "darwin-capacity-v2",
    "incident-readiness": "darwin-incident-v2",
}
RELEASE_CLAIM = "item-closed-for-release"
FIXTURE_CLAIM = "synthetic-item-closed-for-test-only"
MAX_EXECUTION_AGE = timedelta(days=30)
MAX_REVIEW_AGE = timedelta(days=30)
MAX_INDEX_AGE = timedelta(days=7)
MAX_JSON_BYTES = 1024 * 1024

ITEM_LABELS = {
    "Broader formal model coverage": "broader-formal",
    "Deployment manifest": "deployment-manifest",
    "Data lifecycle": "data-lifecycle",
    "Capacity envelope": "capacity-envelope",
    "Incident readiness": "incident-readiness",
}
ITEM_IDS = tuple(ITEM_LABELS.values())
ITEM_ID_SET = frozenset(ITEM_IDS)
REQUIRED_RESULT_ROLE = {item_id: "result" for item_id in ITEM_IDS}

HEX_REVISION_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
UTC_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
SAFE_COMPONENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
FORBIDDEN_TEXT_RE = re.compile(
    r"(?:^|[\s/_.-])(?:none|n/?a|unknown|unspecified|placeholder|example|todo|tbd|"
    r"not[-_ ]performed|not[-_ ]measured|local|localhost|loopback|127\.0\.0\.1)(?:$|[\s/_.-])",
    re.IGNORECASE,
)
LOCAL_ENDPOINT_RE = re.compile(
    r"(?:localhost(?=$|[:/])|127\.0\.0\.1(?=$|[:/])|\[?::1\]?(?=$|[:/])|"
    r"(?:^|[\s/_.-])(?:local|loopback)(?:$|[\s:/_.-]))",
    re.IGNORECASE,
)


class VerificationError(Exception):
    pass


def fail(message: str) -> None:
    raise VerificationError(message)


def require_exact_keys(value: Any, expected: set[str], context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{context} must be a JSON object")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        fail(f"{context} fields mismatch (missing={missing}, extra={extra})")
    return value


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def load_json(path: Path, context: str) -> Any:
    try:
        size = path.stat().st_size
    except OSError as exc:
        fail(f"cannot stat {context} {path}: {exc}")
    if size > MAX_JSON_BYTES:
        fail(f"{context} exceeds {MAX_JSON_BYTES} bytes: {path}")
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read {context} {path}: {exc}")
    try:
        return json.loads(raw, object_pairs_hook=reject_duplicate_keys)
    except VerificationError:
        raise
    except (json.JSONDecodeError, RecursionError) as exc:
        fail(f"malformed {context} {path}: {exc}")


def parse_utc(value: Any, context: str) -> datetime:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        fail(f"{context} must use UTC YYYY-MM-DDTHH:MM:SSZ format")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        fail(f"invalid {context}: {exc}")


def validate_text(value: Any, context: str, *, reject_placeholders: bool = True) -> str:
    if not isinstance(value, str) or value != value.strip() or not (3 <= len(value) <= 256):
        fail(f"{context} must be a trimmed 3-256 character string")
    if "\n" in value or "\r" in value or "\x00" in value:
        fail(f"{context} must be single-line text")
    if reject_placeholders and (FORBIDDEN_TEXT_RE.search(value) or LOCAL_ENDPOINT_RE.search(value)):
        fail(f"{context} contains a placeholder, non-performance, or local-only value")
    return value

def validate_identifier(value: Any, context: str) -> str:
    value = validate_text(value, context)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}", value):
        fail(f"{context} must be a stable identifier without whitespace")
    return value


def validate_sha256(value: Any, context: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        fail(f"{context} must be a lowercase SHA-256")
    if len(set(value)) == 1:
        fail(f"{context} is a placeholder SHA-256")
    return value


def validate_revision(value: Any, context: str) -> str:
    if not isinstance(value, str) or not HEX_REVISION_RE.fullmatch(value):
        fail(f"{context} must be a lowercase immutable 40- or 64-hex revision")
    if len(set(value)) == 1:
        fail(f"{context} is a placeholder revision")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        fail(f"cannot hash artifact {path}: {exc}")
    return digest.hexdigest()


def parse_file_uri(uri: Any, expected: PurePosixPath, context: str) -> PurePosixPath:
    if not isinstance(uri, str) or not uri.startswith("file:"):
        fail(f"{context} must be a relative file: URI")
    raw_path = uri.removeprefix("file:")
    if not raw_path or raw_path.startswith("/") or "\\" in raw_path or "//" in raw_path:
        fail(f"{context} must be a canonical relative file: URI")
    path = PurePosixPath(raw_path)
    if any(part in ("", ".", "..") or not SAFE_COMPONENT_RE.fullmatch(part) for part in path.parts):
        fail(f"{context} contains an unsafe path")
    if path != expected:
        fail(f"{context} must be file:{expected.as_posix()}")
    return path


def resolve_regular_file(base: Path, relative: PurePosixPath, context: str) -> Path:
    candidate = base.joinpath(*relative.parts)
    base_resolved = base.resolve()
    try:
        candidate_resolved = candidate.resolve(strict=True)
    except OSError as exc:
        fail(f"missing {context} {candidate}: {exc}")
    if not candidate_resolved.is_relative_to(base_resolved):
        fail(f"{context} escapes closure-index directory: {candidate}")
    cursor = candidate
    while cursor != base:
        if cursor.is_symlink():
            fail(f"{context} must not use symlinks: {candidate}")
        cursor = cursor.parent
    if not candidate.is_file():
        fail(f"{context} is not a regular file: {candidate}")
    return candidate


def parse_scope(scope_path: Path) -> tuple[str, list[str], list[str]]:
    try:
        lines = scope_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read release scope {scope_path}: {exc}")

    decision_heading = [index for index, line in enumerate(lines) if line == "## Current release decision"]
    if len(decision_heading) != 1:
        fail("release scope must contain exactly one Current release decision heading")
    decision = ""
    for line in lines[decision_heading[0] + 1 :]:
        if line.startswith("## "):
            break
        if line.strip():
            decision = line.strip()
            break
    if decision not in ("Go.", "No-go."):
        fail(f"release decision must be exactly Go. or No-go.; got: {decision!r}")

    open_heading = [index for index, line in enumerate(lines) if line == "## Open release items"]
    if len(open_heading) != 1:
        fail("release scope must contain exactly one Open release items heading")
    section = lines[open_heading[0] + 1 :]
    for index, line in enumerate(section):
        if line.startswith("## "):
            section = section[:index]
            break

    headers = [index for index, line in enumerate(section) if line == "| Item | Current state |"]
    if len(headers) != 1:
        fail("open release table must contain exactly one '| Item | Current state |' header")
    header = headers[0]
    if any(line.strip() for line in section[:header]):
        fail("open release section must not contain content before the authoritative table header")
    if header + 1 >= len(section) or section[header + 1] != "| --- | --- |":
        fail("open release table must use the exact two-column separator")

    open_ids: list[str] = []
    open_rows: list[str] = []
    table_started = False
    for line in section[header + 2 :]:
        if not line.strip():
            if table_started:
                break
            continue
        if not line.startswith("|"):
            if table_started:
                break
            fail("unexpected content between open release table header and rows")
        table_started = True
        match = re.fullmatch(r"\| ([^|]+?) \| (.+) \|", line)
        if match is None:
            fail(f"malformed open release table row: {line}")
        label = match.group(1).strip()
        state = match.group(2).strip()
        if not label or not state:
            fail(f"malformed open release table row: {line}")
        item_id = ITEM_LABELS.get(label)
        if item_id is None:
            fail(f"unknown authoritative open release item: {label}")
        if item_id in open_ids:
            fail(f"duplicate authoritative open release item: {item_id}")
        open_ids.append(item_id)
        open_rows.append(line)

    expected_decision = "No-go." if open_ids else "Go."
    if decision != expected_decision:
        fail(
            f"release decision must be {expected_decision} for {len(open_ids)} open authoritative items; "
            f"got: {decision}"
        )
    return decision, open_ids, open_rows


def validate_identity(value: Any, context: str, expected_role: str) -> tuple[str, str]:
    identity = require_exact_keys(value, {"identity", "authenticated_by", "role"}, context)
    name = validate_text(identity["identity"], f"{context}.identity")
    provider = validate_text(identity["authenticated_by"], f"{context}.authenticated_by")
    if identity["role"] != expected_role:
        fail(f"{context}.role must be {expected_role}")
    parsed_provider = urlsplit(provider)
    if (
        parsed_provider.scheme != "https"
        or not parsed_provider.hostname
        or parsed_provider.username is not None
        or parsed_provider.password is not None
        or parsed_provider.query
        or parsed_provider.fragment
    ):
        fail(f"{context}.authenticated_by must be an HTTPS identity-provider URI without credentials")
    return name, provider


def validate_record(
    record: Any,
    item_id: str,
    index_target: str,
    index_environment: str,
    index_release_id: str,
    index_source_revision: str,
    index_binary_sha256: str,
    index_generated_at: datetime,
    evidence_root: Path,
    test_mode: bool,
    now: datetime,
) -> dict[str, Any]:
    record = require_exact_keys(
        record,
        {
            "schema",
            "evidence_mode",
            "item_id",
            "release_id",
            "target",
            "environment",
            "scope",
            "source_revision",
            "release_binary_sha256",
            "build_artifact",
            "started_at",
            "completed_at",
            "raw_artifacts",
            "acceptance",
            "operator",
            "independent_reviewer",
            "reviewed_at",
            "release_claim",
        },
        f"record {item_id}",
    )
    if record["schema"] != RECORD_SCHEMA:
        fail(f"record {item_id} has unsupported schema")
    if record["item_id"] != item_id:
        fail(f"record path/index item {item_id} does not match record item_id {record['item_id']!r}")

    expected_mode = FIXTURE_INDEX_MODE if test_mode else RELEASE_EVIDENCE_MODE[item_id]
    if record["evidence_mode"] != expected_mode:
        fail(f"record {item_id} evidence_mode must be {expected_mode}")
    expected_claim = FIXTURE_CLAIM if test_mode else RELEASE_CLAIM
    if record["release_claim"] != expected_claim:
        fail(f"record {item_id} release_claim must be {expected_claim}")

    release_id = validate_identifier(record["release_id"], f"record {item_id}.release_id")
    if release_id != index_release_id:
        fail(f"record {item_id} release_id does not match closure index")
    target = validate_identifier(record["target"], f"record {item_id}.target")
    if target != index_target:
        fail(f"record {item_id} target does not match closure index")
    environment = validate_identifier(record["environment"], f"record {item_id}.environment")
    if environment != index_environment:
        fail(f"record {item_id} environment does not match closure index")
    validate_text(record["scope"], f"record {item_id}.scope")
    source_revision = validate_revision(record["source_revision"], f"record {item_id}.source_revision")
    if source_revision != index_source_revision:
        fail(f"record {item_id} source revision does not match closure index")
    release_binary_sha256 = validate_sha256(
        record["release_binary_sha256"], f"record {item_id}.release_binary_sha256"
    )
    if release_binary_sha256 != index_binary_sha256:
        fail(f"record {item_id} release binary SHA-256 does not match closure index")

    build_artifact = require_exact_keys(
        record["build_artifact"], {"kind", "sha256"}, f"record {item_id}.build_artifact"
    )
    expected_kind = "formal-spec" if item_id == "broader-formal" else "binary"
    if build_artifact["kind"] != expected_kind:
        fail(f"record {item_id} build_artifact.kind must be {expected_kind}")
    primary_sha256 = validate_sha256(
        build_artifact["sha256"], f"record {item_id}.build_artifact.sha256"
    )
    if item_id != "broader-formal" and primary_sha256 != release_binary_sha256:
        fail(f"record {item_id} primary artifact must be the shared release binary")

    started_at = parse_utc(record["started_at"], f"record {item_id}.started_at")
    completed_at = parse_utc(record["completed_at"], f"record {item_id}.completed_at")
    reviewed_at = parse_utc(record["reviewed_at"], f"record {item_id}.reviewed_at")
    if not started_at <= completed_at <= reviewed_at <= index_generated_at <= now:
        fail(f"record {item_id} timestamps must satisfy started <= completed <= reviewed <= indexed <= now")
    if now - completed_at > MAX_EXECUTION_AGE:
        fail(f"record {item_id} target execution or formal verification is stale")
    if now - reviewed_at > MAX_REVIEW_AGE:
        fail(f"record {item_id} independent review is stale")

    operator, _ = validate_identity(
        record["operator"], f"record {item_id}.operator", "operator"
    )
    reviewer, _ = validate_identity(
        record["independent_reviewer"],
        f"record {item_id}.independent_reviewer",
        "independent-reviewer",
    )
    if operator.casefold() == reviewer.casefold():
        fail(f"record {item_id} reviewer must be independent from operator")

    artifacts = record["raw_artifacts"]
    minimum_artifacts = 4 if item_id == "broader-formal" else 3
    if not isinstance(artifacts, list) or len(artifacts) < minimum_artifacts:
        fail(
            f"record {item_id}.raw_artifacts must contain release binary, primary/result, and review artifacts"
        )
    seen_uris: set[str] = set()
    role_paths: dict[str, list[Path]] = {}
    allowed_roles = {"binary", "formal-spec", "result", "review"}
    for artifact_index, artifact in enumerate(artifacts):
        context = f"record {item_id}.raw_artifacts[{artifact_index}]"
        artifact = require_exact_keys(artifact, {"role", "uri", "sha256"}, context)
        role = artifact["role"]
        if role not in allowed_roles:
            fail(f"{context}.role is unsupported")
        if artifact["uri"] in seen_uris:
            fail(f"record {item_id} contains duplicate raw artifact URI")
        seen_uris.add(artifact["uri"])
        if role == "binary":
            relative = parse_file_uri(
                artifact["uri"], PurePosixPath("kvnode"), f"{context}.uri"
            )
        else:
            expected_prefix = PurePosixPath("artifacts") / item_id
            if not isinstance(artifact["uri"], str) or not artifact["uri"].startswith("file:"):
                fail(f"{context}.uri must be a relative file: URI")
            relative_parts = PurePosixPath(artifact["uri"].removeprefix("file:"))
            if relative_parts.parent != expected_prefix:
                fail(f"{context}.uri must name one file under file:{expected_prefix.as_posix()}/")
            relative = parse_file_uri(artifact["uri"], relative_parts, f"{context}.uri")
        artifact_path = resolve_regular_file(evidence_root, relative, context)
        declared_sha256 = validate_sha256(artifact["sha256"], f"{context}.sha256")
        if declared_sha256 != sha256_file(artifact_path):
            fail(f"{context} SHA-256 mismatch")
        role_paths.setdefault(role, []).append(artifact_path)
        if role == expected_kind and declared_sha256 != primary_sha256:
            fail(f"record {item_id} primary artifact does not match build_artifact SHA-256")
        if role == "binary" and declared_sha256 != release_binary_sha256:
            fail(f"record {item_id} binary artifact does not match shared release binary SHA-256")

    if len(role_paths.get(expected_kind, [])) != 1:
        fail(f"record {item_id} must contain exactly one {expected_kind} primary artifact")
    if len(role_paths.get("binary", [])) != 1:
        fail(f"record {item_id} must contain exactly one shared release binary artifact")
    if len(role_paths.get("result", [])) != 1:
        fail(f"record {item_id} must contain exactly one authoritative result artifact")
    if len(role_paths.get("review", [])) != 1:
        fail(f"record {item_id} must contain exactly one independent-review artifact")

    acceptance = record["acceptance"]
    if not isinstance(acceptance, list) or not acceptance:
        fail(f"record {item_id}.acceptance must contain explicit criteria and results")
    seen_criteria: set[str] = set()
    for acceptance_index, result in enumerate(acceptance):
        context = f"record {item_id}.acceptance[{acceptance_index}]"
        result = require_exact_keys(result, {"criterion", "result", "details"}, context)
        criterion = validate_text(result["criterion"], f"{context}.criterion")
        validate_text(result["details"], f"{context}.details")
        if result["result"] != "pass":
            fail(f"{context}.result must be pass")
        if criterion in seen_criteria:
            fail(f"record {item_id} contains duplicate acceptance criterion")
        seen_criteria.add(criterion)

    return {
        "item_id": item_id,
        "release_id": release_id,
        "target": target,
        "environment": environment,
        "source_revision": source_revision,
        "primary_sha256": primary_sha256,
        "binary_sha256": release_binary_sha256,
        "binary_path": role_paths["binary"][0],
        "result_path": role_paths["result"][0],
    }


def validate_repository_provenance(
    repository_root: Path, source_revision: str, decision_revision: str
) -> None:
    try:
        head = subprocess.run(
            ["git", "-C", str(repository_root), "rev-parse", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        status = subprocess.run(
            ["git", "-C", str(repository_root), "status", "--porcelain=v1", "--untracked-files=all"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        subprocess.run(
            ["git", "-C", str(repository_root), "cat-file", "-e", f"{source_revision}^{{commit}}"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        ancestor = subprocess.run(
            [
                "git",
                "-C",
                str(repository_root),
                "merge-base",
                "--is-ancestor",
                source_revision,
                decision_revision,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        changed = subprocess.run(
            [
                "git",
                "-C",
                str(repository_root),
                "diff",
                "--name-only",
                "--no-renames",
                source_revision,
                decision_revision,
                "--",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        fail(f"cannot verify source/decision repository provenance: {exc}")
    observed_head = validate_revision(head.stdout.strip(), "repository HEAD")
    if observed_head != decision_revision:
        fail("closure index decision_revision does not match repository HEAD")
    if status.stdout:
        fail("release closure requires a clean decision worktree")
    if ancestor.returncode != 0:
        fail("binary source_revision must be an ancestor of decision_revision")
    allowed_decision_paths = {
        "RELEASE_SCOPE.md",
        "release/EPAXOS_READINESS_EVIDENCE.md",
    }
    changed_paths = {line for line in changed.stdout.splitlines() if line}
    disallowed = sorted(changed_paths - allowed_decision_paths)
    if disallowed:
        fail(f"decision revision changes executable/source paths after the binary build: {disallowed}")

def validate_external_evidence_root(
    evidence_root: Path, repository_root: Path, release_id: str
) -> None:
    if not evidence_root.is_absolute():
        fail("production evidence root must be an absolute path")
    if evidence_root.is_symlink() or not evidence_root.is_dir():
        fail("production evidence root must be a real directory")
    resolved_root = evidence_root.resolve()
    if resolved_root == repository_root or resolved_root.is_relative_to(repository_root):
        fail("production evidence root must be external to the source repository")
    try:
        filesystem_flags = os.statvfs(resolved_root).f_flag
    except OSError as exc:
        fail(f"cannot inspect production evidence filesystem: {exc}")
    if not filesystem_flags & os.ST_RDONLY:
        fail("production evidence root must be physically mounted read-only")
    expected_root = Path("/Volumes") / f"mc-kv-evidence-{release_id}"
    if evidence_root != expected_root:
        fail(f"production evidence root must be exactly {expected_root}")
    try:
        disk_info_result = subprocess.run(
            ["/usr/sbin/diskutil", "info", "-plist", str(resolved_root)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
        )
        disk_info = plistlib.loads(disk_info_result.stdout)
    except (OSError, subprocess.SubprocessError, plistlib.InvalidFileException) as exc:
        fail(f"cannot inspect production evidence APFS mount: {exc}")
    if (
        not isinstance(disk_info, dict)
        or str(disk_info.get("FilesystemType", "")).casefold() != "apfs"
        or disk_info.get("Writable") is not False
        or disk_info.get("MountPoint") != str(resolved_root)
    ):
        fail("production evidence root must be the exact read-only APFS mount point")
    for path in (resolved_root, *resolved_root.rglob("*")):
        if path.is_symlink():
            fail(f"immutable evidence root must not contain symlinks: {path}")
        if not (path.is_dir() or path.is_file()):
            fail(f"immutable evidence root contains a non-regular entry: {path}")

def validate_release_binary(path: Path, expected_sha256: str, test_mode: bool) -> None:
    if path.is_symlink() or not path.is_file():
        fail("release binary must be a regular non-symlink file")
    if sha256_file(path) != expected_sha256:
        fail("release binary bytes do not match the shared SHA-256")
    if path.stat().st_size < 4096:
        fail("release binary is header-only or implausibly small")
    with path.open("rb") as binary:
        header = binary.read(8)
    if header != b"\xcf\xfa\xed\xfe\x0c\x00\x00\x01":
        fail("release binary must be a thin Mach-O 64-bit arm64 executable")
    if path.stat().st_mode & 0o111 == 0:
        fail("release binary must be executable")
    if not test_mode:
        try:
            observed = subprocess.run(
                ["/usr/bin/file", "-b", str(path)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            fail(f"cannot inspect release binary format: {exc}")
        description = observed.stdout.casefold()
        if "mach-o" not in description or "arm64" not in description:
            fail("release binary file inspection did not prove Mach-O arm64")


def parse_env_result(path: Path, context: str) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read {context}: {exc}")
    values: dict[str, str] = {}
    for line_number, line in enumerate(lines, 1):
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            fail(f"{context} line {line_number} is not key=value")
        key, value = line.split("=", 1)
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", key) or key in values:
            fail(f"{context} contains an invalid or duplicate key: {key!r}")
        values[key] = value
    return values


def require_result_path_within_root(path_text: str, base: Path, evidence_root: Path, context: str) -> None:
    candidate = Path(path_text)
    if not candidate.is_absolute():
        candidate = base / candidate
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        fail(f"{context} does not resolve: {exc}")
    if not resolved.is_relative_to(evidence_root):
        fail(f"{context} must remain inside the immutable evidence root")




def result_binding(
    item_id: str, result_path: Path, evidence_root: Path, test_mode: bool
) -> dict[str, str]:
    if item_id == "deployment-manifest":
        values = parse_env_result(result_path, "deployment-manifest result")
        expected_mode = "test-only-synthetic" if test_mode else "target"
        required_values = {
            "evidence_schema": "kvnode-target-deployment-evidence-v2",
            "verifier_version": "darwin-v2",
            "evidence_mode": expected_mode,
            "target_id": TARGET_ID,
            "target_environment": ENVIRONMENT_PROFILE,
        }
        for key, expected in required_values.items():
            if values.get(key) != expected:
                fail(f"deployment-manifest result {key} must equal {expected}")
        target = values.get("target_id")
        environment = values.get("target_environment")
        release_id = values.get("release_id")
        source_revision = values.get("source_revision")
        binary_sha256 = values.get("binary_sha256")
        for key, value in values.items():
            if key.endswith("_artifact"):
                require_result_path_within_root(
                    value, result_path.parent, evidence_root, f"deployment-manifest.{key}"
                )
        evidence_root_value = values.get("final_evidence_root_path")
        if not test_mode and evidence_root_value != str(evidence_root):
            fail("deployment-manifest final_evidence_root_path does not match closure evidence root")
    elif item_id == "capacity-envelope":
        values = parse_env_result(result_path, "capacity-envelope result")
        if test_mode:
            required_values = {
                "status": "synthetic-darwin-capacity-evidence-v2",
                "schema_version": "kvnode-capacity-evidence-v2",
                "verifier_version": "2.0.0",
                "evidence_class": "synthetic-test-only",
                "evidence_mode": "test-only-synthetic",
                "production_capacity_certification": "not-claimed",
                "acceptance_result": "pass-synthetic-test-fixture",
                "claim_scope": "none-synthetic",
                "release_claim": "none-synthetic-fixture-not-release-evidence",
                "target_name": TARGET_ID,
                "environment_profile": ENVIRONMENT_PROFILE,
            }
        else:
            required_values = {
                "status": "darwin-production-capacity-certification-v2",
                "schema_version": "kvnode-capacity-evidence-v2",
                "verifier_version": "2.0.0",
                "evidence_class": "production-capacity-certification",
                "evidence_mode": "target",
                "production_capacity_certification": "approved",
                "acceptance_result": "pass-production-capacity-certification",
                "claim_scope": "declared-native-darwin-same-host-support-envelope",
                "release_claim": "target-bound-production-capacity-envelope-certified",
                "target_name": TARGET_ID,
                "environment_profile": ENVIRONMENT_PROFILE,
            }
        for key, expected in required_values.items():
            if values.get(key) != expected:
                fail(f"capacity-envelope result {key} must equal {expected}")
        target = values.get("target_name")
        environment = values.get("environment_profile")
        release_id = values.get("release_id")
        source_revision = values.get("source_revision")
        binary_sha256 = values.get("binary_sha256")
        for key, value in values.items():
            if key == "binary_path" or key.endswith("_path"):
                require_result_path_within_root(
                    value, result_path.parent, evidence_root, f"capacity-envelope.{key}"
                )
    elif item_id == "data-lifecycle":
        document = load_json(result_path, "data-lifecycle result")
        if not isinstance(document, dict) or not isinstance(document.get("target"), dict):
            fail("data-lifecycle result lacks v2 target provenance")
        if document.get("schema_version") != "2.0.0" or document.get("verifier_version") != "2.0.0":
            fail("data-lifecycle result must use schema/verifier v2")
        expected_class = "synthetic-test-only" if test_mode else "target-data-lifecycle-darwin-v2"
        if document.get("evidence_class") != expected_class:
            fail("data-lifecycle result evidence_class is not the required row mode")
        target_object = document["target"]
        target = target_object.get("target_id")
        environment = target_object.get("environment_profile")
        release_id = target_object.get("release_id")
        source_revision = target_object.get("source_revision")
        binary_sha256 = target_object.get("binary_sha256")
        raw_artifacts = document.get("raw_artifacts")
        if isinstance(raw_artifacts, list):
            for index, raw_artifact in enumerate(raw_artifacts):
                if isinstance(raw_artifact, dict) and isinstance(raw_artifact.get("path"), str):
                    require_result_path_within_root(
                        raw_artifact["path"],
                        result_path.parent,
                        evidence_root,
                        f"data-lifecycle.raw_artifacts[{index}].path",
                    )
    elif item_id == "incident-readiness":
        document = load_json(result_path, "incident-readiness result")
        if (
            not isinstance(document, dict)
            or not isinstance(document.get("target"), dict)
            or not isinstance(document.get("release_provenance"), dict)
        ):
            fail("incident-readiness result lacks v2 release provenance")
        if document.get("schema_version") != "2.0":
            fail("incident-readiness result must use Darwin schema v2")
        expected_mode = "synthetic-test" if test_mode else "target"
        if document.get("record_mode") != expected_mode:
            fail("incident-readiness result record_mode is not the required row mode")
        target_object = document["target"]
        provenance = document["release_provenance"]
        target = target_object.get("name")
        environment = target_object.get("environment")
        release_id = provenance.get("release_id")
        source_revision = provenance.get("source_revision")
        binary_sha256 = provenance.get("binary_sha256")
    else:
        document = load_json(result_path, "broader-formal result")
        if (
            not isinstance(document, dict)
            or not isinstance(document.get("release"), dict)
            or document.get("schema_version") != "1.0.0"
        ):
            fail("broader-formal result is not the signed formal closure contract")
        expected_mode = "synthetic-test" if test_mode else "target"
        if document.get("record_mode") != expected_mode:
            fail("broader-formal result record_mode is not the required row mode")
        release = document["release"]
        return {
            "release_id": validate_identifier(
                release.get("release_id"), "broader-formal result release_id"
            ),
            "source_revision": validate_revision(
                release.get("source_revision"), "broader-formal result source_revision"
            ),
            "primary_sha256": validate_sha256(
                release.get("formal_spec_sha256"), "broader-formal result formal_spec_sha256"
            ),
        }
    return {
        "target": validate_identifier(target, f"{item_id} result target"),
        "environment": validate_identifier(environment, f"{item_id} result environment"),
        "release_id": validate_identifier(release_id, f"{item_id} result release_id"),
        "source_revision": validate_revision(source_revision, f"{item_id} result source_revision"),
        "binary_sha256": validate_sha256(binary_sha256, f"{item_id} result binary_sha256"),
    }


def run_verifier_command(command: list[str], item_id: str) -> str:
    try:
        result = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        fail(f"{item_id} authoritative evidence verifier failed: {exc}")
    output = result.stdout.strip()
    if not output or "\n" in output or "\r" in output:
        fail(f"{item_id} authoritative evidence verifier returned malformed output")
    return output


def verify_item_result(
    metadata: dict[str, Any],
    repository_root: Path,
    evidence_root: Path,
    test_mode: bool,
    test_hook_root: Path | None,
) -> None:
    item_id = metadata["item_id"]
    result_path = metadata["result_path"]
    binding = result_binding(item_id, result_path, evidence_root, test_mode)
    expected_binding = {
        "target": metadata["target"],
        "environment": metadata["environment"],
        "release_id": metadata["release_id"],
        "source_revision": metadata["source_revision"],
        "binary_sha256": metadata["binary_sha256"],
        "primary_sha256": metadata["primary_sha256"],
    }
    for key, value in binding.items():
        if value != expected_binding[key]:
            fail(f"{item_id} result artifact {key} does not match closure record")

    if test_mode:
        if test_hook_root is None:
            fail("synthetic test mode requires --test-hook-root")
        hook = test_hook_root / f"{item_id}-verifier"
        if not hook.is_file() or hook.is_symlink():
            fail(f"missing synthetic item verifier hook: {hook}")
        output = run_verifier_command(
            [
                str(hook),
                str(result_path),
                metadata["target"],
                metadata["environment"],
                metadata["release_id"],
                metadata["source_revision"],
                metadata["binary_sha256"],
                metadata["primary_sha256"],
            ],
            item_id,
        )
        expected = (
            f"release-evidence-hook item_id={item_id} status=verified "
            "mode=synthetic-test-fixture release_claim=none"
        )
        if output != expected:
            fail(f"{item_id} synthetic verifier hook returned an invalid non-claim result")
        return

    tests_dir = repository_root / "tests"
    if item_id == "broader-formal":
        command = [
            "python3",
            str(tests_dir / "verify_formal_closure_evidence.py"),
            "--expected-release-id",
            metadata["release_id"],
            "--expected-source-revision",
            metadata["source_revision"],
            "--expected-formal-spec-sha256",
            metadata["primary_sha256"],
            str(result_path),
        ]
        expected_output = (
            "formal_closure_evidence=verified mode=target "
            f"release_id={metadata['release_id']} source_revision={metadata['source_revision']} "
            f"formal_spec_sha256={metadata['primary_sha256']} claim=formal-closure-criteria-met"
        )
    elif item_id == "deployment-manifest":
        command = [
            "bash",
            str(tests_dir / "kvnode_target_deployment_evidence.sh"),
            "verify",
            str(result_path),
        ]
        expected_output = None
    elif item_id == "data-lifecycle":
        command = [
            "python3",
            str(tests_dir / "target_data_lifecycle_evidence_v2_verify.py"),
            "--expected-target-id",
            metadata["target"],
            "--expected-release-id",
            metadata["release_id"],
            "--expected-source-revision",
            metadata["source_revision"],
            "--expected-binary-sha256",
            metadata["binary_sha256"],
            "--expected-environment-profile",
            metadata["environment"],
            str(result_path),
        ]
        expected_output = (
            "target-data-lifecycle-evidence-v2 status=pass verifier_version=2.0.0 "
            "mode=target-evidence release_claim=target-data-lifecycle-criteria-met "
            f"target_id={metadata['target']} release_id={metadata['release_id']} "
            f"source_revision={metadata['source_revision']} binary_sha256={metadata['binary_sha256']} "
            f"environment_profile={metadata['environment']}"
        )
    elif item_id == "capacity-envelope":
        command = [
            "python3",
            str(tests_dir / "kvnode_capacity_envelope_v2.py"),
            "verify",
            str(result_path),
        ]
        expected_output = (
            "kvnode-capacity-v2 verification=pass "
            "evidence_class=production-capacity-certification evidence_mode=target "
            "production_capacity_certification=approved "
            "release_claim=target-bound-production-capacity-envelope-certified "
            f"target_id={metadata['target']} environment_profile={metadata['environment']} "
            f"release_id={metadata['release_id']} source_revision={metadata['source_revision']} "
            f"binary_sha256={metadata['binary_sha256']} report={result_path}"
        )
    else:
        command = [
            "python3",
            str(tests_dir / "verify_target_incident_evidence.py"),
            "--expected-target",
            metadata["target"],
            "--expected-release-id",
            metadata["release_id"],
            "--expected-environment",
            metadata["environment"],
            "--expected-source-revision",
            metadata["source_revision"],
            "--expected-binary-sha256",
            metadata["binary_sha256"],
            "--evidence-root",
            str(evidence_root),
            str(result_path),
        ]
        expected_output = (
            "target_incident_evidence=verified mode=target "
            f"target={metadata['target']} claim=target-darwin-incident-readiness-observed"
        )
    output = run_verifier_command(command, item_id)
    if item_id == "deployment-manifest":
        deployment_pattern = (
            r"kvnode-target-deployment-evidence status=verified verifier_version=darwin-v2 "
            r"evidence_mode=target release_id="
            + re.escape(metadata["release_id"])
            + r" target_id="
            + re.escape(metadata["target"])
            + r" source_revision="
            + re.escape(metadata["source_revision"])
            + r" binary_sha256="
            + re.escape(metadata["binary_sha256"])
            + r" final_evidence_root_read_only=observed-true final_evidence_root_path="
            + re.escape(str(evidence_root))
            + r" evidence_volume_uuid=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} "
            r"limitations=same-host,loopback-only,no-independent-failure-domain,"
            r"no-production-capacity,no-off-host-backup "
            r"release_claim=target-deployment-accepted"
        )
        if re.fullmatch(deployment_pattern, output) is None:
            fail("deployment-manifest verifier output does not match the authoritative v2 target result")
    elif output != expected_output:
        fail(f"{item_id} verifier output does not match the authoritative v2 target result")


def verify(
    repository_root: Path,
    scope_path: Path,
    evidence_root: Path | None,
    test_mode: bool,
    test_hook_root: Path | None,
    now: datetime,
) -> tuple[str, list[str], list[str], list[str]]:
    decision, open_ids, open_rows = parse_scope(scope_path)
    open_set = set(open_ids)
    closed_ids = [item_id for item_id in ITEM_IDS if item_id not in open_set]

    if evidence_root is None:
        if closed_ids:
            fail(
                "an explicit external immutable evidence root is required for closed items: "
                f"{','.join(closed_ids)}"
            )
        return decision, open_ids, closed_ids, open_rows
    if not evidence_root.exists():
        if closed_ids:
            fail(f"external evidence root does not exist: {evidence_root}")
        return decision, open_ids, closed_ids, open_rows

    index_path = evidence_root / "release-closure-index.json"
    if not index_path.exists():
        if closed_ids:
            fail(f"closure index is required for closed items: {','.join(closed_ids)}")
        return decision, open_ids, closed_ids, open_rows
    if not index_path.is_file() or index_path.is_symlink():
        fail("closure index must be a regular non-symlink file")

    index = require_exact_keys(
        load_json(index_path, "closure index"),
        {
            "schema",
            "mode",
            "target",
            "environment",
            "release_id",
            "source_revision",
            "binary_sha256",
            "decision_revision",
            "generated_at",
            "records",
        },
        "closure index",
    )
    if index["schema"] != INDEX_SCHEMA:
        fail("closure index has unsupported schema")
    expected_index_mode = FIXTURE_INDEX_MODE if test_mode else RELEASE_INDEX_MODE
    if index["mode"] != expected_index_mode:
        fail(f"closure index mode must be {expected_index_mode}")
    target = validate_identifier(index["target"], "closure index.target")
    if target != TARGET_ID:
        fail(f"closure index.target must be {TARGET_ID}")
    environment = validate_identifier(index["environment"], "closure index.environment")
    if environment != ENVIRONMENT_PROFILE:
        fail(f"closure index.environment must be {ENVIRONMENT_PROFILE}")
    release_id = validate_identifier(index["release_id"], "closure index.release_id")
    source_revision = validate_revision(index["source_revision"], "closure index.source_revision")
    expected_release_id = f"mc-kv-{source_revision[:12]}-r1"
    if release_id != expected_release_id:
        fail(f"closure index.release_id must be {expected_release_id}")
    binary_sha256 = validate_sha256(index["binary_sha256"], "closure index.binary_sha256")
    decision_revision = validate_revision(
        index["decision_revision"], "closure index.decision_revision"
    )
    generated_at = parse_utc(index["generated_at"], "closure index.generated_at")
    if generated_at > now:
        fail("closure index generated_at is in the future")
    if now - generated_at > MAX_INDEX_AGE:
        fail("closure index is stale")
    if not test_mode:
        validate_external_evidence_root(evidence_root, repository_root, release_id)
        validate_repository_provenance(repository_root, source_revision, decision_revision)

    entries = index["records"]
    if not isinstance(entries, list):
        fail("closure index.records must be a JSON array")
    index_ids: list[str] = []
    seen_uris: set[str] = set()
    metadata_records: list[dict[str, Any]] = []
    for entry_index, entry in enumerate(entries):
        context = f"closure index.records[{entry_index}]"
        entry = require_exact_keys(entry, {"item_id", "record_uri", "record_sha256"}, context)
        item_id = entry["item_id"]
        if item_id not in ITEM_ID_SET:
            fail(f"{context}.item_id is not authoritative: {item_id!r}")
        if item_id in index_ids:
            fail(f"duplicate closure index item_id: {item_id}")
        index_ids.append(item_id)
        if entry["record_uri"] in seen_uris:
            fail("duplicate closure record URI")
        seen_uris.add(entry["record_uri"])
        expected_record = PurePosixPath("closure-records") / f"{item_id}.json"
        relative = parse_file_uri(entry["record_uri"], expected_record, f"{context}.record_uri")
        record_path = resolve_regular_file(evidence_root, relative, f"closure record {item_id}")
        expected_hash = validate_sha256(entry["record_sha256"], f"{context}.record_sha256")
        if sha256_file(record_path) != expected_hash:
            fail(f"closure record {item_id} SHA-256 mismatch")
        record = load_json(record_path, f"closure record {item_id}")
        metadata_records.append(
            validate_record(
                record,
                item_id,
                target,
                environment,
                release_id,
                source_revision,
                binary_sha256,
                generated_at,
                evidence_root,
                test_mode,
                now,
            )
        )

    index_set = set(index_ids)
    closed_set = set(closed_ids)
    overlap = open_set & index_set
    missing = closed_set - index_set
    extra = index_set - closed_set
    if overlap or missing or extra:
        fail(
            "authoritative item closure mismatch "
            f"(open_and_closed={sorted(overlap)}, missing_records={sorted(missing)}, extra_records={sorted(extra)})"
        )
    if open_set | index_set != ITEM_ID_SET:
        fail("each authoritative item must be exactly open or validly closed")

    common_binding = (target, environment, release_id, source_revision, binary_sha256)
    for metadata in metadata_records:
        observed_binding = (
            metadata["target"],
            metadata["environment"],
            metadata["release_id"],
            metadata["source_revision"],
            metadata["binary_sha256"],
        )
        if observed_binding != common_binding:
            fail(
                "closure records must share exact Darwin target, environment, release_id, "
                "source_revision, and binary SHA-256"
            )
        validate_release_binary(
            metadata["binary_path"], metadata["binary_sha256"], test_mode
        )

    for metadata in metadata_records:
        verify_item_result(
            metadata, repository_root, evidence_root, test_mode, test_hook_root
        )

    return decision, open_ids, closed_ids, open_rows


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--scope", type=Path, required=True)
    parser.add_argument(
        "--evidence-root",
        type=Path,
        help="external immutable evidence directory containing release-closure-index.json",
    )
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--test-hook-root", type=Path, help="synthetic verifier hooks; test-only")
    parser.add_argument("--now", help="fixed UTC verification time; accepted only with --test-mode")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    repository_root = args.repository_root.resolve()
    scope_path = args.scope.resolve()
    canonical_scope = repository_root / "RELEASE_SCOPE.md"
    if scope_path != canonical_scope:
        fail("scope must use the canonical path under repository root")
    if args.now and not args.test_mode:
        fail("--now is test-only")
    if args.test_hook_root and not args.test_mode:
        fail("--test-hook-root is test-only")
    if args.evidence_root and not args.test_mode and not args.evidence_root.is_absolute():
        fail("production --evidence-root must be absolute")
    if args.evidence_root and not args.test_mode and args.evidence_root.is_symlink():
        fail("production --evidence-root must not be a symlink")
    evidence_root = args.evidence_root.resolve() if args.evidence_root else None
    test_hook_root = args.test_hook_root.resolve() if args.test_hook_root else None
    now = parse_utc(args.now, "--now") if args.now else datetime.now(timezone.utc).replace(microsecond=0)
    decision, open_ids, closed_ids, open_rows = verify(
        repository_root,
        scope_path,
        evidence_root,
        args.test_mode,
        test_hook_root,
        now,
    )
    print(f"release_decision={decision}")
    if args.test_mode:
        print("verification_mode=synthetic-test-fixture")
        print("release_claim=none-synthetic-fixture-not-release-evidence")
    else:
        print("verification_mode=release")
    print(f"open_release_items={len(open_ids)}")
    print(f"open_release_item_ids={','.join(open_ids) if open_ids else 'none'}")
    print(f"closed_release_item_ids={','.join(closed_ids) if closed_ids else 'none'}")
    if open_rows:
        print("open_release_item_rows:")
        for row in open_rows:
            print(row)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except VerificationError as exc:
        print(f"release evidence verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
