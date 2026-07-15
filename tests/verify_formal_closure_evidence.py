#!/usr/bin/env python3
"""Fail-closed verifier for broader-formal closure evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import stat
import subprocess
import sys
import tempfile
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import urlsplit
from urllib.request import urlopen

SCHEMA_VERSION = "3.0.0"
RECORD_KIND = "formal-closure-evidence"
TARGET_MODE = "target"
SYNTHETIC_MODE = "synthetic-test"
TARGET_CLAIM = "formal-closure-criteria-met"
NON_CLAIM = "none"
MAX_EVIDENCE_AGE = timedelta(days=30)
MAX_VALIDITY = timedelta(days=30)
FUTURE_SKEW = timedelta(minutes=5)
MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_MARKER_BYTES = 64 * 1024
PRODUCER_TRUST_ROOT_PATH = "tests/formal_closure_producer_public.pem"
REVIEWER_TRUST_ROOT_PATH = "tests/formal_closure_reviewer_public.pem"
DECISION_ONLY_PATHS = frozenset({"RELEASE_SCOPE.md", "release/EPAXOS_READINESS_EVIDENCE.md"})

SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
REVISION_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
UTC_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
FORBIDDEN_TEXT_RE = re.compile(
    r"(?:\bplaceholder\b|\bdummy\b|\btodo\b|\btbd\b|\bexample\b|\bfake\b|"
    r"\blocalhost\b|\blocal-only\b|\bfinite-configured-tlc-only\b|\bnot-proved\b)",
    re.IGNORECASE,
)

TOOL_IDS = ("go", "java", "tlaps", "tlc", "trace-replay")
EMPTY_ENVIRONMENT_SHA256 = hashlib.sha256(b"{}").hexdigest()
FINITE_AREA_IDS = (
    "checked-timing",
    "compaction-fencing",
    "competing-coordinators",
    "config-crash",
    "config-history",
    "config-race",
    "execution-consistency",
    "execution-pruning",
    "execution-scc",
    "loss",
    "paper-safety-normal",
    "paper-safety-recovery",
    "paper-safety-slow",
    "rawnode-refinement",
    "ready-crash",
    "rebroadcast",
    "retry",
    "toq",
)
INVARIANT_IDS = (
    "AcceptedValueWasProposed",
    "AdvanceAcknowledgesOnlyCompletedPrefixes",
    "AppliedAtMostOnce",
    "ApplyRequiresAllReadyRecordsDurable",
    "ChosenStability",
    "ChosenTupleAgreement",
    "ClosedConfigFenced",
    "CommittedImpliesChosen",
    "ConflictingExecutionConsistency",
    "CurrentRoundResponsesOnly",
    "DeterministicWithinSCC",
    "EvidenceBallotAndConfigScoped",
    "EvidenceSenderAuthentic",
    "ExecuteOnlyReadySinkSCC",
    "FastCommitUsesCarriedEvidence",
    "FoldRequiresDurableExecuted",
    "FoldedAbsentResident",
    "ImplStepRefinesOrStutters",
    "LateMessageRematerializes",
    "LocalStability",
    "Nontriviality",
    "OneAcceptedTuplePerBallot",
    "PayloadDropPreservesAuthority",
    "PinnedConfigVotersOnly",
    "PromiseMonotonic",
    "PruneOnlyKnownAfter",
    "PruningPreservesOrder",
    "PureRetryNoDurableOrApply",
    "RecoveryEvidenceDoesNotChangeChosenTuple",
    "ResponseEvidenceSound",
    "RestartReexposesDurableUnappliedCommands",
    "RetryPreservesProtocolValue",
    "SCCPartition",
    "SendRequiresAllReadyRecordsDurable",
    "StaleIncarnationFenced",
    "TOQFastCommitRequiresCoveringCarriedEvidence",
    "TOQReplyAfterDurableRecord",
    "TOQReplyCarriesProcessedTuple",
    "UniqueResponseSender",
)
TEMPORAL_IDS = (
    "EventuallyConfigRaceCompletes",
    "EventuallyEvidencePropagationExercised",
    "EventuallyRecoveredBatchDrains",
    "EventuallyStaleRoundIgnored",
    "EventuallyTwoCoordinatorRaceCompletes",
)
THEOREM_BY_CLASS = {
    "arbitrary-history-safety": "ChosenAgreementInductive",
    "configuration-induction": "ConfigHistorySafetyInductive",
    "fair-loss-liveness": "RetrySafetyUnderFairLoss",
    "recovery-induction": "RecoverySafetyInductive",
    "refinement": "RawNodeRefinement",
}
FAIR_LOSS_ASSUMPTIONS = ("eventual-delivery-under-fair-loss", "infinite-retry", "weak-fair-scheduling")
ARTIFACT_KINDS = {
    "collection-manifest",
    "execution-result-manifest",
    "independent-review",
    "java-binary",
    "go-binary",
    "mutation-baseline",
    "mutation-detector",
    "mutation-manifest",
    "mutation-mutant",
    "native-execution-record",
    "native-execution-signature",
    "nonvacuity-witness",
    "producer-signature",
    "reviewer-signature",
    "scope-enforcement",
    "tlaps-binary",
    "tlaps-raw-log",
    "tlc-binary",
    "tlc-raw-log",
    "toq-clock-discipline",
    "toq-clock-observation",
    "trace-export",
    "trace-replay-binary",
    "trace-replay",
}

TOP_KEYS = {
    "claim",
    "evidence_basis",
    "finite_model_checks",
    "generated_at",
    "inductive_proofs",
    "joint_consensus",
    "mutation_evidence",
    "native_execution",
    "production_attestation",
    "properties",
    "raw_artifacts",
    "record_kind",
    "record_mode",
    "release",
    "reviewed_input_set",
    "schema_version",
    "sign_off",
    "specification_manifest",
    "target",
    "toolchain",
    "toq_clock_discipline",
    "trace_refinement",
    "valid_until",
}


class EvidenceError(Exception):
    """A closed evidence record is invalid."""


def fail(message: str) -> None:
    raise EvidenceError(message)


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_json(path: Path, context: str) -> Any:
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            fail(f"{context} exceeds {MAX_JSON_BYTES} bytes")
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle, object_pairs_hook=reject_duplicate_keys)
    except EvidenceError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"cannot load {context} {path}: {exc}")


def exact_object(value: Any, keys: Iterable[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{path} must be an object")
    expected = set(keys)
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        fail(f"{path} keys mismatch; missing={missing}, extra={extra}")
    return value


def array(value: Any, path: str, minimum: int = 0) -> list[Any]:
    if not isinstance(value, list) or len(value) < minimum:
        fail(f"{path} must be an array with at least {minimum} item(s)")
    return value


def text(value: Any, path: str, *, minimum: int = 1, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not (minimum <= len(value) <= maximum)
        or "\n" in value
        or "\r" in value
    ):
        fail(f"{path} must be a trimmed single-line string of length {minimum}..{maximum}")
    return value


def authenticated_identity(value: Any, path: str) -> str:
    result = text(value, path)
    normalized = unicodedata.normalize("NFC", result)
    if result != normalized or ":" not in result or any(character.isspace() for character in result):
        fail(f"{path} must be a canonical authenticated identity")
    return result


def public_key_der(path: Path, context: str) -> bytes:
    try:
        completed = subprocess.run(
            ["openssl", "pkey", "-pubin", "-in", str(path), "-outform", "DER"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot normalize {context}: {exc}")
    if completed.returncode != 0 or not completed.stdout:
        fail(f"{context} is not a valid public key")
    return completed.stdout


def identifier(value: Any, path: str) -> str:
    result = text(value, path, maximum=128)
    if not IDENTIFIER_RE.fullmatch(result):
        fail(f"{path} must be a safe identifier")
    return result


def sha256(value: Any, path: str) -> str:
    result = text(value, path, minimum=64, maximum=64)
    if not SHA256_RE.fullmatch(result):
        fail(f"{path} must be a lowercase SHA-256")
    return result


def revision(value: Any, path: str) -> str:
    result = text(value, path, minimum=40, maximum=64)
    if not REVISION_RE.fullmatch(result):
        fail(f"{path} must be a full lowercase source revision")
    return result


def integer(
    value: Any,
    path: str,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        suffix = f" and <= {maximum}" if maximum is not None else ""
        fail(f"{path} must be an integer >= {minimum}{suffix}")
    return value


def boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        fail(f"{path} must be a boolean")
    return value


def literal(value: Any, path: str, expected: Any) -> Any:
    if value != expected or type(value) is not type(expected):
        fail(f"{path} must be {expected!r}")
    return value


def utc(value: Any, path: str) -> datetime:
    result = text(value, path, minimum=20, maximum=20)
    if not UTC_RE.fullmatch(result):
        fail(f"{path} must be UTC in YYYY-MM-DDTHH:MM:SSZ form")
    try:
        return datetime.strptime(result, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        fail(f"{path} is not a valid UTC time: {exc}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def relative_path(value: Any, path: str, required_prefix: str | None = None) -> PurePosixPath:
    raw = text(value, path)
    if "\\" in raw or ":" in raw:
        fail(f"{path} must be a portable relative POSIX path")
    parsed = PurePosixPath(raw)
    if parsed.is_absolute() or any(part in {"", ".", ".."} for part in parsed.parts):
        fail(f"{path} must be a normalized relative POSIX path")
    if required_prefix is not None and (not parsed.parts or parsed.parts[0] != required_prefix):
        fail(f"{path} must be beneath {required_prefix}/")
    return parsed


def regular_file(root: Path, relative: PurePosixPath, path: str) -> Path:
    root_resolved = root.resolve()
    candidate = root.joinpath(*relative.parts)
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root_resolved)
    except (OSError, ValueError) as exc:
        fail(f"{path} does not resolve to a contained file: {exc}")
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            fail(f"{path} must not traverse a symbolic link")
    if not resolved.is_file():
        fail(f"{path} must reference a regular file")
    return resolved


def verified_file(
    root: Path,
    raw_path: Any,
    expected_hash: Any,
    path: str,
    *,
    required_prefix: str | None = None,
    maximum_bytes: int | None = None,
) -> tuple[Path, str]:
    relative = relative_path(raw_path, f"{path}.path", required_prefix)
    expected = sha256(expected_hash, f"{path}.sha256")
    resolved = regular_file(root, relative, f"{path}.path")
    if maximum_bytes is not None and resolved.stat().st_size > maximum_bytes:
        fail(f"{path} exceeds the {maximum_bytes}-byte artifact limit")
    actual = sha256_file(resolved)
    if actual != expected:
        fail(f"{path}.sha256 does not match file content")
    return resolved, expected


def snapshot_verified_file(
    root: Path,
    raw_path: Any,
    raw_sha256: Any,
    path: str,
    *,
    required_prefix: str | None = None,
    maximum_bytes: int = MAX_ARTIFACT_BYTES,
) -> tuple[Any, Path, Path, str, int, tuple[int, int, int, int]]:
    relative = relative_path(raw_path, f"{path}.path", required_prefix)
    root_resolved = root.resolve()
    candidate = root_resolved.joinpath(*relative.parts)
    try:
        candidate.relative_to(root_resolved)
        initial = candidate.lstat()
    except (OSError, ValueError) as exc:
        fail(f"cannot inspect {path}.path: {exc}")
    if stat.S_ISLNK(initial.st_mode) or not stat.S_ISREG(initial.st_mode):
        fail(f"{path}.path must be a regular non-symlink file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
        before = os.fstat(descriptor)
    except OSError as exc:
        fail(f"cannot securely open {path}.path: {exc}")
    snapshot = tempfile.NamedTemporaryFile(prefix="formal-evidence-", delete=True)
    digest = hashlib.sha256()
    copied = 0
    try:
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            copied += len(chunk)
            if copied > maximum_bytes:
                fail(f"{path}.path exceeds the {maximum_bytes}-byte artifact limit")
            digest.update(chunk)
            snapshot.write(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    fact = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    if fact != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) or copied != before.st_size:
        fail(f"{path}.path changed while it was being snapshotted")
    actual = digest.hexdigest()
    expected = sha256(raw_sha256, f"{path}.sha256")
    if actual != expected:
        fail(f"{path}.sha256 does not match file content")
    snapshot.flush()
    return snapshot, Path(snapshot.name), candidate, actual, copied, fact


def recheck_snapshot_source(
    source: Path,
    expected_fact: tuple[int, int, int, int],
    expected_sha256: str,
    context: str,
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(source, flags)
        before = os.fstat(descriptor)
    except OSError as exc:
        fail(f"cannot recheck {context}: {exc}")
    digest = hashlib.sha256()
    size = 0
    try:
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    fact = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    if (
        fact != expected_fact
        or fact != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or size != before.st_size
        or digest.hexdigest() != expected_sha256
    ):
        fail(f"{context} changed after its immutable snapshot")


def walk_strings(value: Any, path: str = "$") -> Iterable[tuple[str, str]]:
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, dict):
        for key, child in value.items():
            yield from walk_strings(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_strings(child, f"{path}[{index}]")


def reject_placeholders(document: dict[str, Any]) -> None:
    allowed = {
        ("$.claim", NON_CLAIM),
        ("$.record_mode", SYNTHETIC_MODE),
        ("$.toq_clock_discipline.target_environment", SYNTHETIC_MODE),
    }
    for path, value in walk_strings(document):
        if (path, value) in allowed:
            continue
        if FORBIDDEN_TEXT_RE.search(value):
            fail(f"{path} contains a placeholder or non-claim marker")


def remote_uri(value: Any, path: str, *, content_hash: str | None = None) -> str:
    result = text(value, path)
    parsed = urlsplit(result)
    if parsed.scheme not in {"https", "s3", "gs", "azure"}:
        fail(f"{path} must use an immutable remote evidence URI scheme")
    host = (parsed.hostname or "").lower()
    if not host or host in {"localhost", "127.0.0.1", "::1"} or host.endswith(".local"):
        fail(f"{path} must not be local")
    if not parsed.path or parsed.path == "/":
        fail(f"{path} must identify a specific evidence object")
    if content_hash is not None:
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) < 2 or parts[-2:] != ["sha256", content_hash]:
            fail(f"{path} must end in /sha256/<declared SHA-256>")
    return result


def unique_strings(value: Any, path: str, minimum: int = 0) -> list[str]:
    values = array(value, path, minimum)
    result = [identifier(item, f"{path}[{index}]") for index, item in enumerate(values)]
    if len(result) != len(set(result)):
        fail(f"{path} contains duplicate values")
    return result


def command(value: Any, path: str) -> list[str]:
    values = array(value, path, 1)
    result = [text(item, f"{path}[{index}]") for index, item in enumerate(values)]
    if result[0] in {"sh", "bash", "zsh"}:
        fail(f"{path} must be an exact argv, not an opaque shell command")
    return result


def canonical_manifest_sha256(models: list[dict[str, Any]], configs: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        {"configs": configs, "models": models},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")


def canonical_command_manifest_sha256(
    finite_checks: list[dict[str, Any]],
    inductive_proofs: list[dict[str, Any]],
    trace_refinement: dict[str, Any],
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "finite_model_checks": [
                    {"config_id": item["config_id"], "command": item["command"]}
                    for item in finite_checks
                ],
                "inductive_proofs": [
                    {"theorem_class": item["theorem_class"], "command": item["command"]}
                    for item in inductive_proofs
                ],
                "trace_export_command": trace_refinement["export_command"],
                "trace_replay_command": trace_refinement["replay_command"],
            }
        )
    ).hexdigest()


def canonical_toolchain_manifest_sha256(toolchain: list[dict[str, Any]]) -> str:
    return hashlib.sha256(canonical_json_bytes(toolchain)).hexdigest()


def canonical_reviewed_inputs_sha256(inputs: list[dict[str, Any]]) -> str:
    return hashlib.sha256(canonical_json_bytes(inputs)).hexdigest()


def evidence_attestation_payload(document: dict[str, Any]) -> bytes:
    """Canonical complete semantics signed independently by producer and reviewer.

    Signature digest fields and signature byte artifacts are necessarily excluded
    to avoid self-reference. Their ids, algorithms, identities, decisions, trust
    roots, chronology, and every other evidence field remain covered.
    """
    payload = dict(document)
    payload["production_attestation"] = {
        key: value
        for key, value in document["production_attestation"].items()
        if key != "signed_payload_sha256"
    }
    payload["sign_off"] = {
        key: value
        for key, value in document["sign_off"].items()
        if key != "signed_payload_sha256"
    }
    payload["raw_artifacts"] = [
        (
            {key: value for key, value in artifact.items() if key != "sha256"}
            if artifact.get("kind") in {"producer-signature", "reviewer-signature"}
            else artifact
        )
        for artifact in document["raw_artifacts"]
    ]
    return canonical_json_bytes(payload)


def verify_rsa_sha256_signature(payload: bytes, public_key: Path, signature: Path, context: str) -> None:
    try:
        completed = subprocess.run(
            ["openssl", "dgst", "-sha256", "-verify", str(public_key), "-signature", str(signature)],
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot verify {context} with local openssl: {exc}")
    if completed.returncode != 0 or b"Verified OK" not in completed.stdout:
        fail(f"{context} RSA-SHA256 signature verification failed")


def verify_repository_revision(repository_root: Path, expected_revision: str) -> None:
    try:
        head = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository_root), "rev-parse", "--verify", "HEAD"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        status = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository_root), "status", "--porcelain", "--untracked-files=all"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        ancestor = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository_root), "merge-base", "--is-ancestor", expected_revision, "HEAD"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        changed = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository_root), "diff", "--name-only", f"{expected_revision}..HEAD", "--"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot verify target repository revision: {exc}")
    if head.returncode != 0:
        fail("cannot resolve target repository HEAD")
    revision(head.stdout.strip(), "repository HEAD")
    if status.returncode != 0 or status.stdout:
        fail("target repository source tree is not clean")
    if ancestor.returncode != 0:
        fail("--expected-source-revision is not an ancestor of target repository HEAD")
    if changed.returncode != 0:
        fail("cannot compare --expected-source-revision with target repository HEAD")
    changed_paths = {line for line in changed.stdout.splitlines() if line}
    unexpected = sorted(changed_paths - DECISION_ONLY_PATHS)
    if unexpected:
        fail(f"target repository changed non-decision path(s) after source revision: {unexpected}")

def repository_tree_id(repository_root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository_root), "rev-parse", "HEAD^{tree}"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot resolve target repository tree: {exc}")
    if completed.returncode != 0:
        fail("cannot resolve target repository tree")
    return revision(completed.stdout.strip(), "target repository tree")


def require_git_tracked(repository_root: Path, relative: PurePosixPath, context: str) -> None:
    try:
        completed = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository_root), "ls-files", "--error-unmatch", "--", str(relative)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot verify {context} is tracked at the target revision: {exc}")
    if completed.returncode != 0:
        fail(f"{context} is not tracked at the target revision")


def require_theorem_declaration(model_path: Path, theorem_id: str, context: str) -> None:
    if model_path.stat().st_size > MAX_JSON_BYTES:
        fail(f"{context} exceeds the model inspection limit")
    try:
        model_text = model_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        fail(f"cannot inspect {context}: {exc}")
    pattern = rf"(?m)^\s*THEOREM\s+{re.escape(theorem_id)}(?:\s|==)"
    if re.search(pattern, model_text) is None:
        fail(f"{context} does not declare THEOREM {theorem_id}")


def require_log_text(path: Path, required: Iterable[str], context: str) -> None:
    remaining = set(required)
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                remaining = {needle for needle in remaining if needle not in line}
                if not remaining:
                    return
    except (OSError, UnicodeError) as exc:
        fail(f"cannot inspect {context}: {exc}")
    fail(f"{context} lacks required native tool output: {sorted(remaining)}")


def strip_tla_comments(source: str, context: str) -> str:
    effective: list[str] = []
    index = 0
    block_depth = 0
    while index < len(source):
        if source.startswith("(*", index):
            block_depth += 1
            index += 2
        elif source.startswith("*)", index):
            if block_depth == 0:
                fail(f"{context} contains an unmatched block-comment terminator")
            block_depth -= 1
            index += 2
        elif block_depth:
            if source[index] == "\n":
                effective.append("\n")
            index += 1
        elif source.startswith("\\*", index):
            newline = source.find("\n", index)
            if newline < 0:
                break
            effective.append("\n")
            index = newline + 1
        else:
            effective.append(source[index])
            index += 1
    if block_depth:
        fail(f"{context} contains an unterminated block comment")
    return "".join(effective)


def effective_config_directives(source: str, context: str) -> dict[str, list[str]]:
    directives: dict[str, list[str]] = {}
    for raw_line in strip_tla_comments(source, context).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = re.fullmatch(r"([A-Z_]+)\s+([A-Za-z0-9_!]+)", line)
        if match is None:
            fail(f"{context} contains an unsupported effective directive: {line!r}")
        directives.setdefault(match.group(1), []).append(match.group(2))
    return directives


def require_config_declarations(
    config_path: Path,
    invariant_operators: Iterable[str],
    temporal_operators: Iterable[str],
    context: str,
) -> None:
    if config_path.stat().st_size > MAX_JSON_BYTES:
        fail(f"{context} exceeds the config inspection limit")
    try:
        config_text = config_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        fail(f"cannot inspect {context}: {exc}")
    directives = effective_config_directives(config_text, context)
    if len(directives.get("SPECIFICATION", [])) != 1:
        fail(f"{context} must declare exactly one active SPECIFICATION")
    if directives.get("CHECK_DEADLOCK") != ["TRUE"]:
        fail(f"{context} must declare exactly one active CHECK_DEADLOCK TRUE")
    allowed_keywords = {"SPECIFICATION", "INVARIANT", "PROPERTY", "CHECK_DEADLOCK"}
    unexpected = sorted(set(directives) - allowed_keywords)
    if unexpected:
        fail(f"{context} contains unsupported directives {unexpected}")
    for keyword, operators in (("INVARIANT", invariant_operators), ("PROPERTY", temporal_operators)):
        declared = directives.get(keyword, [])
        for operator in operators:
            if declared.count(operator) != 1:
                fail(f"{context} must declare exactly one active {keyword} {operator}")


def validate_semantic_mutation(
    baseline_file: Path,
    mutant_file: Path,
    *,
    mutation_operator: str,
    subject_name: str,
    context: str,
) -> None:
    try:
        baseline = baseline_file.read_text(encoding="utf-8")
        mutant = mutant_file.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        fail(f"cannot inspect {context}: {exc}")
    if mutation_operator == "remove-theorem-declaration":
        pattern = re.compile(
            rf"(?m)^[ \t]*THEOREM[ \t]+{re.escape(subject_name)}(?:[ \t]+|[ \t]*==).*(?:\n|\Z)"
        )
        matches = list(pattern.finditer(strip_tla_comments(baseline, context)))
        if len(matches) != 1:
            fail(f"{context} baseline does not contain exactly one active theorem declaration")
        expected, count = re.subn(pattern, "", baseline)
    elif mutation_operator == "remove-property-directive":
        keyword = "INVARIANT" if subject_name in INVARIANT_IDS else "PROPERTY"
        pattern = re.compile(rf"(?m)^[ \t]*{keyword}[ \t]+{re.escape(subject_name)}[ \t]*(?:\n|\Z)")
        expected, count = re.subn(pattern, "", baseline)
    elif mutation_operator == "disable-deadlock-check":
        pattern = re.compile(r"(?m)^[ \t]*CHECK_DEADLOCK[ \t]+TRUE[ \t]*$")
        expected, count = re.subn(pattern, "CHECK_DEADLOCK FALSE", baseline)
    else:
        fail(f"{context} has unsupported mutation operator {mutation_operator!r}")
    if count != 1 or mutant != expected:
        fail(f"{context} mutant is not the exact subject-specific semantic mutation")


def parse_marker(path: Path, prefix: str, context: str) -> dict[str, Any]:
    matches: list[str] = []
    try:
        with path.open("rb") as handle:
            while True:
                raw_line = handle.readline(MAX_MARKER_BYTES + 1)
                if not raw_line:
                    break
                if len(raw_line) > MAX_MARKER_BYTES and not raw_line.endswith(b"\n"):
                    fail(f"{context} contains an oversized line")
                line = raw_line.decode("utf-8").rstrip("\r\n")
                if line.startswith(prefix):
                    matches.append(line[len(prefix) :])
                    if len(matches) > 1:
                        break
    except EvidenceError:
        raise
    except (OSError, UnicodeError) as exc:
        fail(f"cannot inspect {context}: {exc}")
    if len(matches) != 1:
        fail(f"{context} must contain exactly one {prefix.strip()} machine result")
    try:
        value = json.loads(matches[0], object_pairs_hook=reject_duplicate_keys)
    except (EvidenceError, json.JSONDecodeError) as exc:
        fail(f"invalid machine result in {context}: {exc}")
    if not isinstance(value, dict):
        fail(f"machine result in {context} must be an object")
    return value


def validate_trace_export(
    path: Path,
    *,
    release_id: str,
    source_revision: str,
    source_tree_id: str,
    target_id: str,
    formal_spec_sha256: str,
    exported_at: str,
    export_command_sha256: str,
    go_binary_sha256: str,
    trace_count: int,
) -> None:
    export = load_json(path, "Go-to-shaped-spec trace export")
    export = exact_object(
        export,
        {
            "deterministic", "environment_sha256", "exit_code", "export_command_sha256", "exported_at",
            "formal_spec_sha256", "format", "go_binary_sha256", "release_id", "source_revision",
            "source_tree_id", "target_id", "traces", "working_directory",
        },
        "trace export",
    )
    literal(export["deterministic"], "trace export.deterministic", True)
    literal(export["release_id"], "trace export.release_id", release_id)
    literal(export["source_revision"], "trace export.source_revision", source_revision)
    literal(export["source_tree_id"], "trace export.source_tree_id", source_tree_id)
    literal(export["target_id"], "trace export.target_id", target_id)
    literal(export["formal_spec_sha256"], "trace export.formal_spec_sha256", formal_spec_sha256)
    literal(export["format"], "trace export.format", "moreconsensus-shaped-spec-trace-v1")
    literal(export["exported_at"], "trace export.exported_at", exported_at)
    literal(export["export_command_sha256"], "trace export.export_command_sha256", export_command_sha256)
    literal(export["go_binary_sha256"], "trace export.go_binary_sha256", go_binary_sha256)
    literal(export["working_directory"], "trace export.working_directory", ".")
    literal(export["environment_sha256"], "trace export.environment_sha256", EMPTY_ENVIRONMENT_SHA256)
    literal(export["exit_code"], "trace export.exit_code", 0)
    traces = array(export["traces"], "trace export.traces", 1)
    if len(traces) != trace_count:
        fail("trace export.traces count does not match trace_refinement.trace_count")
    trace_ids: set[str] = set()
    for trace_index, raw_trace in enumerate(traces):
        trace_path = f"trace export.traces[{trace_index}]"
        trace = exact_object(raw_trace, {"events", "trace_id"}, trace_path)
        trace_id = identifier(trace["trace_id"], f"{trace_path}.trace_id")
        if trace_id in trace_ids:
            fail("trace export contains duplicate trace_id values")
        events = array(trace["events"], f"{trace_path}.events", 2)
        mapped_actions: list[str] = []
        allowed_actions = {
            "Accept",
            "Commit",
            "ConfigChange",
            "Execute",
            "PreAccept",
            "Prepare",
            "Propose",
            "TryPreAccept",
        }
        for event_index, raw_event in enumerate(events):
            event_path = f"{trace_path}.events[{event_index}]"
            event = exact_object(raw_event, {"go_event", "spec_action", "step"}, event_path)
            literal(event["step"], f"{event_path}.step", event_index + 1)
            go_event = identifier(event["go_event"], f"{event_path}.go_event")
            spec_action = identifier(event["spec_action"], f"{event_path}.spec_action")
            if go_event != spec_action or go_event not in allowed_actions:
                fail(f"{event_path} does not bind an allowed Go event to the same shaped-spec action")
            mapped_actions.append(spec_action)
        if mapped_actions[0] != "Propose" or "Commit" not in mapped_actions:
            fail(f"{trace_path} must begin with Propose and reach Commit")
        if mapped_actions.index("Commit") < mapped_actions.index("Propose"):
            fail(f"{trace_path} commits before proposal")


def validate_schema_contract() -> None:
    schema_path = Path(__file__).with_name("formal_closure_evidence.schema.json")
    schema = load_json(schema_path, "formal closure schema")
    root = exact_object(schema, {"$schema", "$id", "title", "description", "type", "additionalProperties", "required", "properties", "allOf", "$defs"}, "$schema")
    literal(root["$schema"], "$schema.$schema", "https://json-schema.org/draft/2020-12/schema")
    literal(root["$id"], "$schema.$id", "https://gosuda.org/moreconsensus/schemas/formal-closure-evidence-3.0.0.json")
    literal(root["type"], "$schema.type", "object")
    literal(root["additionalProperties"], "$schema.additionalProperties", False)
    required = array(root["required"], "$schema.required")
    properties = exact_object(root["properties"], TOP_KEYS, "$schema.properties")
    if set(required) != TOP_KEYS or len(required) != len(TOP_KEYS):
        fail("formal closure schema required fields do not match verifier contract")
    literal(properties["schema_version"].get("const"), "$schema.properties.schema_version.const", SCHEMA_VERSION)
    literal(properties["record_kind"].get("const"), "$schema.properties.record_kind.const", RECORD_KIND)
    semantic_refs = {
        "mutation_evidence": "#/$defs/mutationEvidence",
        "native_execution": "#/$defs/nativeExecution",
        "reviewed_input_set": "#/$defs/reviewedInputSet",
        "target": "#/$defs/target",
    }
    for name, expected_ref in semantic_refs.items():
        literal(
            properties[name].get("$ref"),
            f"$schema.properties.{name}.$ref",
            expected_ref,
        )
    definitions = root["$defs"]
    if not isinstance(definitions, dict):
        fail("$schema.$defs must be an object")
    for name in ("mutationEvidence", "mutationSubject", "nativeExecution", "reviewedInput", "reviewedInputSet", "target"):
        definition = definitions.get(name)
        if not isinstance(definition, dict) or definition.get("additionalProperties") is not False:
            fail(f"$schema.$defs.{name} must be a closed object")
    raw_kind_enum = (
        definitions.get("rawArtifact", {})
        .get("properties", {})
        .get("kind", {})
        .get("enum")
    )
    if not isinstance(raw_kind_enum, list) or set(raw_kind_enum) != ARTIFACT_KINDS:
        fail("$schema raw artifact roles do not match the verifier contract")


def validate_document(
    document: Any,
    *,
    evidence_path: Path,
    repository_root: Path,
    expected_release_id: str,
    expected_source_revision: str,
    expected_formal_spec_sha256: str,
    expected_target_id: str,
    expected_environment_profile: str,
    allow_synthetic_test_fixture: bool,
    now: datetime,
) -> dict[str, str]:
    top = exact_object(document, TOP_KEYS, "$")
    literal(top["schema_version"], "$.schema_version", SCHEMA_VERSION)
    literal(top["record_kind"], "$.record_kind", RECORD_KIND)
    reject_placeholders(top)

    mode = text(top["record_mode"], "$.record_mode")
    claim = text(top["claim"], "$.claim", maximum=128)
    if mode == TARGET_MODE:
        literal(claim, "$.claim", TARGET_CLAIM)
    elif mode == SYNTHETIC_MODE:
        if not allow_synthetic_test_fixture:
            fail("synthetic test evidence requires --allow-synthetic-test-fixture")
        literal(claim, "$.claim", NON_CLAIM)
    else:
        fail("$.record_mode must be 'target' or 'synthetic-test'")

    release = exact_object(
        top["release"],
        {"release_id", "source_repository", "source_revision", "source_tree", "formal_spec_sha256"},
        "$.release",
    )
    release_id = identifier(release["release_id"], "$.release.release_id")
    expected_release = identifier(expected_release_id, "expected release id")
    if release_id != expected_release:
        fail("$.release.release_id does not match --expected-release-id")
    source_revision = revision(release["source_revision"], "$.release.source_revision")
    expected_revision = revision(expected_source_revision, "expected source revision")
    if source_revision != expected_revision:
        fail("$.release.source_revision does not match --expected-source-revision")
    if mode == TARGET_MODE:
        verify_repository_revision(repository_root, expected_revision)
    formal_spec_sha = sha256(release["formal_spec_sha256"], "$.release.formal_spec_sha256")
    expected_spec_sha = sha256(expected_formal_spec_sha256, "expected formal spec SHA-256")
    if formal_spec_sha != expected_spec_sha:
        fail("$.release.formal_spec_sha256 does not match --expected-formal-spec-sha256")
    literal(release["source_tree"], "$.release.source_tree", "clean")
    remote_uri(release["source_repository"], "$.release.source_repository")

    target = exact_object(
        top["target"],
        {
            "architecture",
            "environment",
            "execution_mode",
            "operating_system",
            "source_tree_id",
            "target_id",
        },
        "$.target",
    )
    target_id = identifier(target["target_id"], "$.target.target_id")
    expected_target = identifier(expected_target_id, "expected target id")
    if target_id != expected_target:
        fail("$.target.target_id does not match --expected-target-id")
    expected_environment = identifier(
        expected_environment_profile,
        "expected environment profile",
    )
    if target["environment"] != expected_environment:
        fail("$.target.environment does not match --expected-environment-profile")
    source_tree_id = revision(target["source_tree_id"], "$.target.source_tree_id")
    literal(target["operating_system"], "$.target.operating_system", "Darwin")
    architecture = identifier(target["architecture"], "$.target.architecture")
    if mode == TARGET_MODE:
        literal(target["environment"], "$.target.environment", "production")
        literal(target["execution_mode"], "$.target.execution_mode", "native-darwin")
        if platform.system() != "Darwin" or architecture != platform.machine():
            fail("$.target does not match the native verifier environment")
        if repository_tree_id(repository_root) != source_tree_id:
            fail("$.target.source_tree_id does not match the exact target repository tree")
    else:
        literal(target["environment"], "$.target.environment", SYNTHETIC_MODE)
        literal(target["execution_mode"], "$.target.execution_mode", SYNTHETIC_MODE)

    generated_at = utc(top["generated_at"], "$.generated_at")
    valid_until = utc(top["valid_until"], "$.valid_until")
    if generated_at > now + FUTURE_SKEW:
        fail("$.generated_at is too far in the future")
    if now - generated_at > MAX_EVIDENCE_AGE:
        fail("formal closure evidence is stale")
    if valid_until <= now:
        fail("formal closure evidence has expired")
    if valid_until <= generated_at or valid_until - generated_at > MAX_VALIDITY:
        fail("$.valid_until must be after generation and no more than 30 days later")
    execution_times: list[datetime] = []

    artifacts_raw = array(top["raw_artifacts"], "$.raw_artifacts", 1)
    artifacts: dict[str, dict[str, Any]] = {}
    artifact_files: dict[str, Path] = {}
    artifact_snapshots: list[Any] = []
    artifact_sources: dict[str, tuple[Path, tuple[int, int, int, int], str]] = {}
    artifact_created_times: dict[str, datetime] = {}
    artifact_paths: set[Path] = set()
    artifact_hashes: set[str] = set()
    for index, raw in enumerate(artifacts_raw):
        path = f"$.raw_artifacts[{index}]"
        artifact = exact_object(
            raw,
            {"artifact_id", "kind", "path", "sha256", "size_bytes", "source_revision", "created_at"},
            path,
        )
        artifact_id = identifier(artifact["artifact_id"], f"{path}.artifact_id")
        if artifact_id in artifacts:
            fail(f"duplicate raw artifact id {artifact_id!r}")
        kind = text(artifact["kind"], f"{path}.kind")
        if kind not in ARTIFACT_KINDS:
            fail(f"{path}.kind is unsupported")
        artifact_revision = revision(artifact["source_revision"], f"{path}.source_revision")
        if artifact_revision != source_revision:
            fail(f"{path}.source_revision does not match the release")
        created_at = utc(artifact["created_at"], f"{path}.created_at")
        if created_at > generated_at or generated_at - created_at > MAX_EVIDENCE_AGE:
            fail(f"{path}.created_at is outside the evidence freshness window")
        snapshot, resolved, source_path, artifact_hash, snapshot_size, source_fact = snapshot_verified_file(
            evidence_path.parent,
            artifact["path"],
            artifact["sha256"],
            path,
            required_prefix="artifacts",
            maximum_bytes=MAX_ARTIFACT_BYTES,
        )
        if source_path in artifact_paths:
            fail(f"{path}.path duplicates another raw artifact")
        if artifact_hash in artifact_hashes and kind != "mutation-baseline":
            fail(f"{path}.sha256 duplicates another non-baseline raw artifact's content")
        artifact_paths.add(source_path)
        if kind != "mutation-baseline":
            artifact_hashes.add(artifact_hash)
        size = integer(
            artifact["size_bytes"],
            f"{path}.size_bytes",
            1,
            MAX_ARTIFACT_BYTES,
        )
        if snapshot_size != size:
            fail(f"{path}.size_bytes does not match file content")
        artifact["sha256"] = artifact_hash
        artifacts[artifact_id] = artifact
        artifact_files[artifact_id] = resolved
        artifact_snapshots.append(snapshot)
        artifact_sources[artifact_id] = (source_path, source_fact, artifact_hash)
        artifact_created_times[artifact_id] = created_at

    references: set[str] = set()

    def reference_artifact(
        artifact_id_value: Any,
        path: str,
        kind: str,
        expected_hash: str | None = None,
    ) -> tuple[dict[str, Any], Path]:
        artifact_id = identifier(artifact_id_value, path)
        if artifact_id in references:
            fail(f"{path} reuses raw artifact {artifact_id!r}; each evidence role must be distinct")
        artifact = artifacts.get(artifact_id)
        if artifact is None:
            fail(f"{path} references unknown raw artifact {artifact_id!r}")
        if artifact["kind"] != kind:
            fail(f"{path} must reference a {kind} artifact")
        if expected_hash is not None and artifact["sha256"] != expected_hash:
            fail(f"{path} artifact hash does not match its bound hash")
        references.add(artifact_id)
        return artifact, artifact_files[artifact_id]
    attestation = exact_object(
        top["production_attestation"],
        {
            "decision", "decided_at", "signature_algorithm", "signature_artifact_id", "signed_at",
            "signed_payload_sha256", "signer_identity", "trust_root_path", "trust_root_sha256",
        },
        "$.production_attestation",
    )
    producer_identity = authenticated_identity(
        attestation["signer_identity"],
        "$.production_attestation.signer_identity",
    )
    literal(attestation["signature_algorithm"], "$.production_attestation.signature_algorithm", "rsa-sha256")
    producer_trust_path = relative_path(
        attestation["trust_root_path"],
        "$.production_attestation.trust_root_path",
    )
    literal(
        str(producer_trust_path),
        "$.production_attestation.trust_root_path",
        PRODUCER_TRUST_ROOT_PATH,
    )
    if producer_trust_path.suffix != ".pem":
        fail("$.production_attestation.trust_root_path must reference a repository-pinned PEM public key")
    producer_trust_hash = sha256(
        attestation["trust_root_sha256"],
        "$.production_attestation.trust_root_sha256",
    )
    producer_trust_file = regular_file(
        repository_root,
        producer_trust_path,
        "$.production_attestation.trust_root_path",
    )
    if mode == TARGET_MODE:
        require_git_tracked(
            repository_root,
            producer_trust_path,
            "$.production_attestation.trust_root_path",
        )
    if sha256_file(producer_trust_file) != producer_trust_hash:
        fail("$.production_attestation.trust_root_sha256 does not match the repository trust root")
    literal(attestation["decision"], "$.production_attestation.decision", "approved")
    producer_decided_at = utc(attestation["decided_at"], "$.production_attestation.decided_at")
    signed_at = utc(attestation["signed_at"], "$.production_attestation.signed_at")
    if (
        producer_decided_at > signed_at
        or signed_at > generated_at
        or generated_at - producer_decided_at > MAX_EVIDENCE_AGE
    ):
        fail("$.production_attestation decision/signature chronology is inconsistent or stale")
    producer_signature_id = identifier(
        attestation["signature_artifact_id"],
        "$.production_attestation.signature_artifact_id",
    )
    producer_payload_hash = sha256(
        attestation["signed_payload_sha256"],
        "$.production_attestation.signed_payload_sha256",
    )
    _, producer_signature_file = reference_artifact(
        producer_signature_id,
        "$.production_attestation.signature_artifact_id",
        "producer-signature",
    )


    tool_entries = array(top["toolchain"], "$.toolchain", len(TOOL_IDS))
    if len(tool_entries) != len(TOOL_IDS):
        fail("$.toolchain must contain exactly Go, Java, TLC, TLAPS, and the trace replay engine")
    tools: dict[str, dict[str, Any]] = {}
    tool_kind = {
        "go": "go-binary",
        "java": "java-binary",
        "tlc": "tlc-binary",
        "tlaps": "tlaps-binary",
        "trace-replay": "trace-replay-binary",
    }
    for index, raw in enumerate(tool_entries):
        path = f"$.toolchain[{index}]"
        tool = exact_object(
            raw,
            {"tool_id", "version", "repository_pin_path", "repository_pin_sha256", "binary_sha256", "binary_artifact_id", "source_revision"},
            path,
        )
        tool_id = identifier(tool["tool_id"], f"{path}.tool_id")
        if tool_id not in TOOL_IDS or tool_id in tools:
            fail(f"{path}.tool_id must uniquely identify a required pinned formal-evidence tool")
        version_text = text(tool["version"], f"{path}.version", maximum=128)
        if revision(tool["source_revision"], f"{path}.source_revision") != source_revision:
            fail(f"{path}.source_revision does not match the release")
        pin_path = relative_path(tool["repository_pin_path"], f"{path}.repository_pin_path")
        pin_hash = sha256(tool["repository_pin_sha256"], f"{path}.repository_pin_sha256")
        pin_file = regular_file(repository_root, pin_path, f"{path}.repository_pin_path")
        if mode == TARGET_MODE:
            require_git_tracked(repository_root, pin_path, f"{path}.repository_pin_path")
        if sha256_file(pin_file) != pin_hash:
            fail(f"{path}.repository_pin_sha256 does not match the repository pin")
        pin = load_json(pin_file, f"{path} repository tool pin")
        pin = exact_object(pin, {"binary_sha256", "tool_id", "version"}, f"{path} repository tool pin")
        literal(pin["tool_id"], f"{path} repository tool pin.tool_id", tool_id)
        literal(pin["version"], f"{path} repository tool pin.version", version_text)
        binary_hash = sha256(tool["binary_sha256"], f"{path}.binary_sha256")
        literal(pin["binary_sha256"], f"{path} repository tool pin.binary_sha256", binary_hash)
        reference_artifact(tool["binary_artifact_id"], f"{path}.binary_artifact_id", tool_kind[tool_id], binary_hash)
        tools[tool_id] = tool
    if tuple(sorted(tools)) != TOOL_IDS:
        fail("$.toolchain must contain exactly Go, Java, TLC, TLAPS, and the trace replay engine")

    manifest = exact_object(top["specification_manifest"], {"manifest_sha256", "models", "configs"}, "$.specification_manifest")
    models_raw = array(manifest["models"], "$.specification_manifest.models", 1)
    configs_raw = array(manifest["configs"], "$.specification_manifest.configs", 1)
    models: dict[str, dict[str, Any]] = {}
    configs: dict[str, dict[str, Any]] = {}
    config_files: dict[str, Path] = {}
    model_files: dict[str, Path] = {}
    root_model_ids: list[str] = []
    model_paths: set[str] = set()
    config_paths: set[str] = set()
    for index, raw in enumerate(models_raw):
        path = f"$.specification_manifest.models[{index}]"
        model = exact_object(raw, {"model_id", "path", "sha256", "role", "source_revision"}, path)
        model_id = identifier(model["model_id"], f"{path}.model_id")
        if model_id in models:
            fail(f"duplicate model id {model_id!r}")
        model_path = relative_path(model["path"], f"{path}.path")
        if model_path.suffix != ".tla" or str(model_path) in model_paths:
            fail(f"{path}.path must uniquely identify a .tla file")
        model_hash = sha256(model["sha256"], f"{path}.sha256")
        model_file = regular_file(repository_root, model_path, f"{path}.path")
        if mode == TARGET_MODE:
            require_git_tracked(repository_root, model_path, f"{path}.path")
        if sha256_file(model_file) != model_hash:
            fail(f"{path}.sha256 does not match repository model content")
        if revision(model["source_revision"], f"{path}.source_revision") != source_revision:
            fail(f"{path}.source_revision does not match the release")
        role = text(model["role"], f"{path}.role")
        if role not in {"claim-root-shaped-spec", "supporting"}:
            fail(f"{path}.role is invalid")
        if role == "claim-root-shaped-spec":
            root_model_ids.append(model_id)
            if model_hash != formal_spec_sha:
                fail(f"{path}.sha256 must equal the release formal spec SHA-256")
        models[model_id] = model
        model_files[model_id] = model_file
        model_paths.add(str(model_path))
    if len(root_model_ids) != 1:
        fail("specification manifest must have exactly one claim-root-shaped-spec model")
    for index, raw in enumerate(configs_raw):
        path = f"$.specification_manifest.configs[{index}]"
        config = exact_object(raw, {"config_id", "path", "sha256", "model_id", "source_revision"}, path)
        config_id = identifier(config["config_id"], f"{path}.config_id")
        if config_id in configs:
            fail(f"duplicate config id {config_id!r}")
        config_path = relative_path(config["path"], f"{path}.path")
        if config_path.suffix != ".cfg" or str(config_path) in config_paths:
            fail(f"{path}.path must uniquely identify a .cfg file")
        config_hash = sha256(config["sha256"], f"{path}.sha256")
        config_file = regular_file(repository_root, config_path, f"{path}.path")
        if mode == TARGET_MODE:
            require_git_tracked(repository_root, config_path, f"{path}.path")
        if sha256_file(config_file) != config_hash:
            fail(f"{path}.sha256 does not match repository config content")
        model_id = identifier(config["model_id"], f"{path}.model_id")
        if model_id not in models:
            fail(f"{path}.model_id references an unknown model")
        if revision(config["source_revision"], f"{path}.source_revision") != source_revision:
            fail(f"{path}.source_revision does not match the release")
        configs[config_id] = config
        config_files[config_id] = config_file
        config_paths.add(str(config_path))
    model_order = [item["model_id"] for item in models_raw]
    config_order = [item["config_id"] for item in configs_raw]
    if model_order != sorted(model_order) or config_order != sorted(config_order):
        fail("specification manifest models and configs must be sorted by id")
    manifest_hash = sha256(manifest["manifest_sha256"], "$.specification_manifest.manifest_sha256")
    if canonical_manifest_sha256(models_raw, configs_raw) != manifest_hash:
        fail("$.specification_manifest.manifest_sha256 does not match the exact manifest")

    property_manifest = exact_object(top["properties"], {"invariants", "temporal_properties"}, "$.properties")

    def validate_property_entries(raw_entries: Any, path: str, expected_ids: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        entries = array(raw_entries, path, len(expected_ids))
        if len(entries) != len(expected_ids):
            fail(f"{path} must contain the exact required property set")
        result: dict[str, dict[str, Any]] = {}
        for index, raw in enumerate(entries):
            entry_path = f"{path}[{index}]"
            entry = exact_object(raw, {"property_id", "operator", "module_id", "required"}, entry_path)
            property_id = identifier(entry["property_id"], f"{entry_path}.property_id")
            if property_id in result:
                fail(f"{path} contains duplicate property {property_id!r}")
            literal(entry["operator"], f"{entry_path}.operator", property_id)
            if identifier(entry["module_id"], f"{entry_path}.module_id") not in models:
                fail(f"{entry_path}.module_id references an unknown model")
            literal(entry["required"], f"{entry_path}.required", True)
            result[property_id] = entry
        if tuple(sorted(result)) != expected_ids:
            missing = sorted(set(expected_ids) - set(result))
            extra = sorted(set(result) - set(expected_ids))
            fail(f"{path} property coverage mismatch; missing={missing}, extra={extra}")
        if [entry["property_id"] for entry in entries] != sorted(result):
            fail(f"{path} must be sorted by property_id")
        return result

    invariants = validate_property_entries(property_manifest["invariants"], "$.properties.invariants", INVARIANT_IDS)
    temporal = validate_property_entries(property_manifest["temporal_properties"], "$.properties.temporal_properties", TEMPORAL_IDS)

    checks_raw = array(top["finite_model_checks"], "$.finite_model_checks", 1)
    checks: dict[str, dict[str, Any]] = {}
    covered_areas: list[str] = []
    checked_invariants: set[str] = set()
    checked_temporal: set[str] = set()
    used_models: set[str] = set()
    for index, raw in enumerate(checks_raw):
        path = f"$.finite_model_checks[{index}]"
        check = exact_object(
            raw,
            {
                "area_ids", "checked_invariant_ids", "checked_temporal_ids", "command", "command_sha256",
                "completed_at", "config_id", "deadlock_result", "distinct_states", "environment_sha256",
                "errors", "exit_code", "generated_states", "java_binary_sha256", "queue_states", "result",
                "search_depth", "started_at", "tlc_log_artifact_id", "tool_binary_sha256", "tool_id",
                "tool_version", "working_directory",
            },
            path,
        )
        config_id = identifier(check["config_id"], f"{path}.config_id")
        if config_id not in configs or config_id in checks:
            fail(f"{path}.config_id must uniquely cover a manifest config")
        check_areas = unique_strings(check["area_ids"], f"{path}.area_ids", 1)
        unknown_areas = sorted(set(check_areas) - set(FINITE_AREA_IDS))
        if unknown_areas:
            fail(f"{path}.area_ids contains unknown areas {unknown_areas}")
        covered_areas.extend(check_areas)
        invariant_coverage = unique_strings(check["checked_invariant_ids"], f"{path}.checked_invariant_ids", 1)
        temporal_coverage = unique_strings(check["checked_temporal_ids"], f"{path}.checked_temporal_ids", 1)
        if set(invariant_coverage) - set(invariants):
            fail(f"{path}.checked_invariant_ids contains unmanifested properties")
        if set(temporal_coverage) - set(temporal):
            fail(f"{path}.checked_temporal_ids contains unmanifested properties")
        checked_model_id = configs[config_id]["model_id"]
        mismatched_properties = sorted(
            property_id
            for property_id in (*invariant_coverage, *temporal_coverage)
            if (invariants.get(property_id) or temporal.get(property_id))["module_id"] != checked_model_id
        )
        if mismatched_properties:
            fail(f"{path} property module does not match the model loaded by the checked config: {mismatched_properties}")
        checked_invariants.update(invariant_coverage)
        checked_temporal.update(temporal_coverage)
        argv = command(check["command"], f"{path}.command")
        expected_argv = [
            "java",
            "-jar",
            "tla2tools.jar",
            "-config",
            configs[config_id]["path"],
            models[configs[config_id]["model_id"]]["path"],
        ]
        if argv != expected_argv:
            fail(f"{path}.command must be the exact pinned TLC invocation for its model and config")
        literal(check["tool_id"], f"{path}.tool_id", "tlc")
        literal(check["tool_version"], f"{path}.tool_version", tools["tlc"]["version"])
        literal(check["tool_binary_sha256"], f"{path}.tool_binary_sha256", tools["tlc"]["binary_sha256"])
        command_hash = hashlib.sha256(canonical_json_bytes(argv)).hexdigest()
        literal(check["command_sha256"], f"{path}.command_sha256", command_hash)
        literal(check["java_binary_sha256"], f"{path}.java_binary_sha256", tools["java"]["binary_sha256"])
        literal(check["working_directory"], f"{path}.working_directory", ".")
        literal(check["environment_sha256"], f"{path}.environment_sha256", EMPTY_ENVIRONMENT_SHA256)
        literal(check["exit_code"], f"{path}.exit_code", 0)
        generated_states = integer(check["generated_states"], f"{path}.generated_states", 1)
        distinct_states = integer(check["distinct_states"], f"{path}.distinct_states", 1)
        if distinct_states > generated_states:
            fail(f"{path}.distinct_states must not exceed generated_states")
        search_depth = integer(check["search_depth"], f"{path}.search_depth", 1)
        literal(check["queue_states"], f"{path}.queue_states", 0)
        literal(check["errors"], f"{path}.errors", 0)
        literal(check["deadlock_result"], f"{path}.deadlock_result", "pass")
        literal(check["result"], f"{path}.result", "pass")
        started_at = utc(check["started_at"], f"{path}.started_at")
        completed_at = utc(check["completed_at"], f"{path}.completed_at")
        if started_at > completed_at or completed_at > generated_at or generated_at - completed_at > MAX_EVIDENCE_AGE:
            fail(f"{path} execution time is inconsistent or stale")
        execution_times.append(completed_at)
        require_config_declarations(
            config_files[config_id],
            (invariants[property_id]["operator"] for property_id in invariant_coverage),
            (temporal[property_id]["operator"] for property_id in temporal_coverage),
            f"{path} manifest config",
        )
        _, log_file = reference_artifact(check["tlc_log_artifact_id"], f"{path}.tlc_log_artifact_id", "tlc-raw-log")
        marker = parse_marker(log_file, "FORMAL_CLOSURE_TLC_RESULT ", f"{path} TLC raw log")
        marker_keys = {
            "area_ids", "checked_invariant_ids", "checked_temporal_ids", "completed_at", "config_id",
            "config_sha256", "distinct_states", "errors", "generated_states", "model_sha256", "queue_states",
            "result", "search_depth", "source_revision", "started_at", "tool_binary_sha256", "tool_version",
        }
        exact_object(marker, marker_keys, f"{path} TLC machine result")
        expected_marker = {
            "area_ids": check_areas,
            "checked_invariant_ids": invariant_coverage,
            "checked_temporal_ids": temporal_coverage,
            "completed_at": check["completed_at"],
            "config_id": config_id,
            "config_sha256": configs[config_id]["sha256"],
            "distinct_states": distinct_states,
            "errors": 0,
            "generated_states": generated_states,
            "queue_states": 0,
            "result": "pass",
            "model_sha256": models[configs[config_id]["model_id"]]["sha256"],
            "search_depth": search_depth,
            "source_revision": source_revision,
            "started_at": check["started_at"],
            "tool_binary_sha256": tools["tlc"]["binary_sha256"],
            "tool_version": tools["tlc"]["version"],
        }
        if marker != expected_marker:
            fail(f"{path} fields do not match the TLC raw log machine result")
        command_marker = parse_marker(
            log_file,
            "FORMAL_CLOSURE_COMMAND_BINDING ",
            f"{path} TLC command binding",
        )
        expected_command_marker = {
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "target_id": target_id,
            "config_id": config_id,
            "config_sha256": configs[config_id]["sha256"],
            "model_sha256": models[configs[config_id]["model_id"]]["sha256"],
            "command_sha256": command_hash,
            "java_binary_sha256": tools["java"]["binary_sha256"],
            "tlc_binary_sha256": tools["tlc"]["binary_sha256"],
            "working_directory": ".",
            "environment_sha256": EMPTY_ENVIRONMENT_SHA256,
            "exit_code": 0,
            "completed_at": check["completed_at"],
            "result": "pass",
        }
        exact_object(command_marker, set(expected_command_marker), f"{path} TLC command binding")
        if command_marker != expected_command_marker:
            fail(f"{path} command or tool binding does not match the exact TLC execution")
        if mode == TARGET_MODE:
            require_log_text(
                log_file,
                (
                    "Model checking completed. No error has been found.",
                    f"{generated_states} states generated, {distinct_states} distinct states found, 0 states left on queue.",
                    f"The depth of the complete state graph search is {search_depth}.",
                ),
                f"{path} TLC raw log",
            )
        used_models.add(configs[config_id]["model_id"])
        checks[config_id] = check
    if set(checks) != set(configs):
        fail("finite model checks must cover every manifest config exactly once")
    if len(covered_areas) != len(set(covered_areas)):
        fail("finite area coverage contains duplicate areas")
    if tuple(sorted(covered_areas)) != FINITE_AREA_IDS:
        missing = sorted(set(FINITE_AREA_IDS) - set(covered_areas))
        extra = sorted(set(covered_areas) - set(FINITE_AREA_IDS))
        fail(f"finite area coverage mismatch; missing={missing}, extra={extra}")
    if checked_invariants != set(invariants):
        fail(f"finite checks do not cover every invariant; missing={sorted(set(invariants) - checked_invariants)}")
    if checked_temporal != set(temporal):
        fail(f"finite checks do not cover every temporal property; missing={sorted(set(temporal) - checked_temporal)}")

    proofs_raw = array(top["inductive_proofs"], "$.inductive_proofs", 5)
    if len(proofs_raw) != 5:
        fail("$.inductive_proofs must contain exactly five required checked theorem classes")
    proofs: dict[str, dict[str, Any]] = {}
    theorem_ids: set[str] = set()
    for index, raw in enumerate(proofs_raw):
        path = f"$.inductive_proofs[{index}]"
        proof = exact_object(
            raw,
            {
                "admitted_obligations", "assumptions", "checked_at", "command", "command_sha256",
                "environment_sha256", "exit_code", "history_scope", "inductive", "module_id",
                "obligations_proved", "obligations_total", "omitted_obligations", "result", "status",
                "theorem_class", "theorem_id", "tlaps_log_artifact_id", "tool_binary_sha256", "tool_id",
                "tool_version", "working_directory",
            },
            path,
        )
        theorem_class = text(proof["theorem_class"], f"{path}.theorem_class")
        if theorem_class not in THEOREM_BY_CLASS or theorem_class in proofs:
            fail(f"{path}.theorem_class must uniquely cover a required inductive theorem class")
        theorem_id = identifier(proof["theorem_id"], f"{path}.theorem_id")
        literal(theorem_id, f"{path}.theorem_id", THEOREM_BY_CLASS[theorem_class])
        if theorem_id in theorem_ids:
            fail(f"duplicate theorem id {theorem_id!r}")
        theorem_ids.add(theorem_id)
        module_id = identifier(proof["module_id"], f"{path}.module_id")
        if module_id not in models:
            fail(f"{path}.module_id references an unknown model")
        used_models.add(module_id)
        require_theorem_declaration(
            model_files[module_id],
            theorem_id,
            f"{path} proof module",
        )
        literal(proof["status"], f"{path}.status", "checked")
        literal(proof["result"], f"{path}.result", "pass")
        literal(proof["inductive"], f"{path}.inductive", True)
        literal(proof["history_scope"], f"{path}.history_scope", "arbitrary-history")
        assumptions = unique_strings(proof["assumptions"], f"{path}.assumptions")
        expected_assumptions = FAIR_LOSS_ASSUMPTIONS if theorem_class == "fair-loss-liveness" else ()
        if tuple(sorted(assumptions)) != expected_assumptions:
            fail(f"{path}.assumptions does not match the theorem class contract")
        obligations_total = integer(proof["obligations_total"], f"{path}.obligations_total", 1)
        obligations_proved = integer(proof["obligations_proved"], f"{path}.obligations_proved", 1)
        if obligations_total != obligations_proved:
            fail(f"{path} has unchecked proof obligations")
        literal(proof["admitted_obligations"], f"{path}.admitted_obligations", 0)
        literal(proof["omitted_obligations"], f"{path}.omitted_obligations", 0)
        literal(proof["tool_id"], f"{path}.tool_id", "tlaps")
        literal(proof["tool_version"], f"{path}.tool_version", tools["tlaps"]["version"])
        literal(proof["tool_binary_sha256"], f"{path}.tool_binary_sha256", tools["tlaps"]["binary_sha256"])
        proof_argv = command(proof["command"], f"{path}.command")
        expected_proof_argv = ["tlapm", models[module_id]["path"], "--theorem", theorem_id]
        if proof_argv != expected_proof_argv:
            fail(f"{path}.command must be the exact pinned TLAPS invocation for the theorem and module")
        proof_command_hash = hashlib.sha256(canonical_json_bytes(proof_argv)).hexdigest()
        literal(proof["command_sha256"], f"{path}.command_sha256", proof_command_hash)
        literal(proof["working_directory"], f"{path}.working_directory", ".")
        literal(proof["environment_sha256"], f"{path}.environment_sha256", EMPTY_ENVIRONMENT_SHA256)
        literal(proof["exit_code"], f"{path}.exit_code", 0)
        checked_at = utc(proof["checked_at"], f"{path}.checked_at")
        if checked_at > generated_at or generated_at - checked_at > MAX_EVIDENCE_AGE:
            fail(f"{path}.checked_at is inconsistent or stale")
        execution_times.append(checked_at)
        _, log_file = reference_artifact(proof["tlaps_log_artifact_id"], f"{path}.tlaps_log_artifact_id", "tlaps-raw-log")
        marker = parse_marker(log_file, "FORMAL_CLOSURE_TLAPS_RESULT ", f"{path} TLAPS raw log")
        marker_keys = {
            "admitted_obligations", "assumptions", "checked_at", "history_scope", "module_sha256",
            "obligations_proved", "obligations_total", "omitted_obligations", "result", "source_revision",
            "status", "theorem_class", "theorem_id", "tool_binary_sha256", "tool_version",
        }
        exact_object(marker, marker_keys, f"{path} TLAPS machine result")
        expected_marker = {
            "admitted_obligations": 0,
            "assumptions": assumptions,
            "checked_at": proof["checked_at"],
            "history_scope": "arbitrary-history",
            "module_sha256": models[module_id]["sha256"],
            "obligations_proved": obligations_proved,
            "obligations_total": obligations_total,
            "omitted_obligations": 0,
            "result": "pass",
            "source_revision": source_revision,
            "status": "checked",
            "theorem_class": theorem_class,
            "theorem_id": theorem_id,
            "tool_binary_sha256": tools["tlaps"]["binary_sha256"],
            "tool_version": tools["tlaps"]["version"],
        }
        if marker != expected_marker:
            fail(f"{path} fields do not match the TLAPS raw log machine result")
        command_marker = parse_marker(
            log_file,
            "FORMAL_CLOSURE_COMMAND_BINDING ",
            f"{path} TLAPS command binding",
        )
        expected_command_marker = {
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "target_id": target_id,
            "theorem_class": theorem_class,
            "theorem_id": theorem_id,
            "module_sha256": models[module_id]["sha256"],
            "command_sha256": proof_command_hash,
            "tlaps_binary_sha256": tools["tlaps"]["binary_sha256"],
            "working_directory": ".",
            "environment_sha256": EMPTY_ENVIRONMENT_SHA256,
            "exit_code": 0,
            "completed_at": proof["checked_at"],
            "result": "pass",
        }
        exact_object(command_marker, set(expected_command_marker), f"{path} TLAPS command binding")
        if command_marker != expected_command_marker:
            fail(f"{path} command or tool binding does not match the exact TLAPS execution")
        if mode == TARGET_MODE:
            require_log_text(
                log_file,
                (theorem_id, "All obligations proved."),
                f"{path} TLAPS raw log",
            )
        proofs[theorem_class] = proof
    if set(proofs) != set(THEOREM_BY_CLASS):
        fail("inductive proofs do not cover the exact required theorem classes")
    if [proof["theorem_class"] for proof in proofs_raw] != sorted(THEOREM_BY_CLASS):
        fail("$.inductive_proofs must be sorted by theorem_class")

    basis = exact_object(
        top["evidence_basis"],
        {"finite_status", "inductive_status", "closure_basis", "finite_only", "unchecked_proof_count"},
        "$.evidence_basis",
    )
    literal(basis["finite_status"], "$.evidence_basis.finite_status", "passed-bounded-checks")
    literal(basis["inductive_status"], "$.evidence_basis.inductive_status", "checked-passed-unbounded")
    literal(basis["closure_basis"], "$.evidence_basis.closure_basis", "finite-plus-inductive")
    literal(basis["finite_only"], "$.evidence_basis.finite_only", False)
    literal(basis["unchecked_proof_count"], "$.evidence_basis.unchecked_proof_count", 0)

    mutation_evidence = exact_object(
        top["mutation_evidence"],
        {"executed_at", "record_artifact_id", "record_sha256", "result", "subjects"},
        "$.mutation_evidence",
    )
    mutation_executed_at = utc(mutation_evidence["executed_at"], "$.mutation_evidence.executed_at")
    if mutation_executed_at > generated_at or generated_at - mutation_executed_at > MAX_EVIDENCE_AGE:
        fail("$.mutation_evidence.executed_at is inconsistent or stale")
    execution_times.append(mutation_executed_at)
    literal(mutation_evidence["result"], "$.mutation_evidence.result", "pass")
    mutation_record_hash = sha256(mutation_evidence["record_sha256"], "$.mutation_evidence.record_sha256")
    _, mutation_record_file = reference_artifact(
        mutation_evidence["record_artifact_id"],
        "$.mutation_evidence.record_artifact_id",
        "mutation-manifest",
        mutation_record_hash,
    )
    mutation_record = exact_object(
        load_json(mutation_record_file, "mutation/nonvacuity manifest"),
        {
            "evidence_mode", "executed_at", "formal_spec_sha256", "mutations", "nonvacuity",
            "record_kind", "release_id", "result", "schema_version", "source_revision",
            "source_tree_id", "target_id",
        },
        "mutation/nonvacuity manifest",
    )
    literal(mutation_record["schema_version"], "mutation/nonvacuity manifest.schema_version", SCHEMA_VERSION)
    literal(
        mutation_record["record_kind"],
        "mutation/nonvacuity manifest.record_kind",
        "formal-closure-mutation-nonvacuity",
    )
    literal(mutation_record["release_id"], "mutation/nonvacuity manifest.release_id", release_id)
    literal(mutation_record["source_revision"], "mutation/nonvacuity manifest.source_revision", source_revision)
    literal(mutation_record["source_tree_id"], "mutation/nonvacuity manifest.source_tree_id", source_tree_id)
    literal(mutation_record["formal_spec_sha256"], "mutation/nonvacuity manifest.formal_spec_sha256", formal_spec_sha)
    literal(mutation_record["target_id"], "mutation/nonvacuity manifest.target_id", target_id)
    literal(mutation_record["evidence_mode"], "mutation/nonvacuity manifest.evidence_mode", mode)
    literal(mutation_record["executed_at"], "mutation/nonvacuity manifest.executed_at", mutation_evidence["executed_at"])
    literal(mutation_record["result"], "mutation/nonvacuity manifest.result", "pass")

    expected_subjects = {
        *(f"theorem:{name}" for name in THEOREM_BY_CLASS),
        *(f"config:{name}" for name in configs),
        *(f"property:{name}" for name in INVARIANT_IDS),
        *(f"property:{name}" for name in TEMPORAL_IDS),
    }
    raw_mutations = array(mutation_record["mutations"], "mutation/nonvacuity manifest.mutations", len(expected_subjects))
    raw_nonvacuity = array(
        mutation_record["nonvacuity"],
        "mutation/nonvacuity manifest.nonvacuity",
        len(expected_subjects),
    )
    raw_mutation_by_subject: dict[str, dict[str, Any]] = {}
    raw_witness_by_subject: dict[str, dict[str, Any]] = {}
    raw_mutation_keys = {
        "authoritative_id", "authoritative_path", "authoritative_sha256", "baseline_command",
        "baseline_command_sha256", "baseline_path", "baseline_sha256", "detected_by",
        "detector_output_path", "detector_output_sha256", "detector_tool_binary_sha256",
        "detector_tool_id", "environment_sha256", "java_binary_sha256", "mutant_command",
        "mutant_command_sha256", "mutated_path", "mutated_sha256", "mutation_id",
        "mutation_operator", "baseline_exit_code", "mutant_exit_code", "baseline_completed_at",
        "detector_completed_at", "result", "subject_id", "subject_kind", "working_directory",
    }
    raw_witness_keys = {
        "authoritative_id", "authoritative_path", "authoritative_sha256", "baseline_command",
        "baseline_command_sha256", "baseline_sha256", "detected_by", "detector_output_sha256",
        "detector_tool_binary_sha256", "detector_tool_id", "environment_sha256",
        "java_binary_sha256", "mutated_sha256", "mutation_operator", "baseline_exit_code",
        "baseline_completed_at", "result", "subject_id", "subject_kind", "witness_count",
        "witness_path", "witness_sha256", "working_directory",
    }
    for index, raw in enumerate(raw_mutations):
        path = f"mutation/nonvacuity manifest.mutations[{index}]"
        entry = exact_object(raw, raw_mutation_keys, path)
        subject_id = identifier(entry["subject_id"], f"{path}.subject_id")
        literal(entry["mutation_id"], f"{path}.mutation_id", subject_id)
        if subject_id in raw_mutation_by_subject:
            fail(f"{path} duplicates subject mutation {subject_id!r}")
        raw_mutation_by_subject[subject_id] = entry
    for index, raw in enumerate(raw_nonvacuity):
        path = f"mutation/nonvacuity manifest.nonvacuity[{index}]"
        entry = exact_object(raw, raw_witness_keys, path)
        subject_id = identifier(entry["subject_id"], f"{path}.subject_id")
        if subject_id in raw_witness_by_subject:
            fail(f"{path} duplicates nonvacuity subject {subject_id!r}")
        raw_witness_by_subject[subject_id] = entry
    if set(raw_mutation_by_subject) != expected_subjects or set(raw_witness_by_subject) != expected_subjects:
        fail("mutation/nonvacuity manifest must cover every theorem, config, and property exactly once")
    if [entry["subject_id"] for entry in raw_mutations] != sorted(expected_subjects):
        fail("mutation/nonvacuity manifest.mutations must be sorted by subject_id")
    if [entry["subject_id"] for entry in raw_nonvacuity] != sorted(expected_subjects):
        fail("mutation/nonvacuity manifest.nonvacuity must be sorted by subject_id")

    subjects_raw = array(mutation_evidence["subjects"], "$.mutation_evidence.subjects", len(expected_subjects))
    if len(subjects_raw) != len(expected_subjects):
        fail("$.mutation_evidence.subjects must cover every theorem, config, and property exactly once")
    subject_ids: list[str] = []
    for index, raw in enumerate(subjects_raw):
        path = f"$.mutation_evidence.subjects[{index}]"
        subject = exact_object(
            raw,
            {
                "authoritative_id", "authoritative_path", "authoritative_sha256", "baseline_artifact_id",
                "baseline_command", "baseline_command_sha256", "baseline_completed_at", "baseline_exit_code",
                "baseline_result", "baseline_sha256", "detected_by", "detector_artifact_id",
                "detector_completed_at", "detector_sha256", "detector_tool_binary_sha256",
                "detector_tool_id", "environment_sha256", "java_binary_sha256", "mutant_artifact_id",
                "mutant_command", "mutant_command_sha256", "mutant_exit_code", "mutant_sha256",
                "mutation_operator", "mutation_result", "nonvacuity_result", "subject_id",
                "subject_kind", "witness_artifact_id", "witness_count", "witness_observed_at",
                "witness_sha256", "working_directory",
            },
            path,
        )
        subject_id = identifier(subject["subject_id"], f"{path}.subject_id")
        expected_kind, _, subject_name = subject_id.partition(":")
        if subject_id not in expected_subjects or subject_id in subject_ids:
            fail(f"{path}.subject_id must uniquely identify a required theorem, config, or property")
        literal(subject["subject_kind"], f"{path}.subject_kind", expected_kind)

        if expected_kind == "theorem":
            proof = proofs[subject_name]
            authoritative_id = proof["module_id"]
            authoritative_path = models[authoritative_id]["path"]
            authoritative_hash = models[authoritative_id]["sha256"]
            authoritative_file = model_files[authoritative_id]
            mutation_operator = "remove-theorem-declaration"
            detector_tool_id = "tlaps"
            theorem_id = proof["theorem_id"]
            config_model_path = None
        elif expected_kind == "config":
            authoritative_id = subject_name
            authoritative_path = configs[authoritative_id]["path"]
            authoritative_hash = configs[authoritative_id]["sha256"]
            authoritative_file = config_files[authoritative_id]
            mutation_operator = "disable-deadlock-check"
            detector_tool_id = "tlc"
            theorem_id = None
            config_model_path = models[configs[authoritative_id]["model_id"]]["path"]
        else:
            candidate_configs = [
                check["config_id"]
                for check in checks_raw
                if subject_name in check["checked_invariant_ids"]
                or subject_name in check["checked_temporal_ids"]
            ]
            if len(candidate_configs) != 1:
                fail(f"{path} property must map to exactly one authoritative checked config")
            authoritative_id = candidate_configs[0]
            authoritative_path = configs[authoritative_id]["path"]
            authoritative_hash = configs[authoritative_id]["sha256"]
            authoritative_file = config_files[authoritative_id]
            mutation_operator = "remove-property-directive"
            detector_tool_id = "tlc"
            theorem_id = None
            config_model_path = models[configs[authoritative_id]["model_id"]]["path"]

        literal(subject["authoritative_id"], f"{path}.authoritative_id", authoritative_id)
        literal(subject["authoritative_path"], f"{path}.authoritative_path", authoritative_path)
        literal(subject["authoritative_sha256"], f"{path}.authoritative_sha256", authoritative_hash)
        literal(subject["mutation_operator"], f"{path}.mutation_operator", mutation_operator)

        baseline_hash = sha256(subject["baseline_sha256"], f"{path}.baseline_sha256")
        mutant_hash = sha256(subject["mutant_sha256"], f"{path}.mutant_sha256")
        if baseline_hash != authoritative_hash:
            fail(f"{path}.baseline_sha256 does not match the authoritative repository input")
        if baseline_hash == mutant_hash:
            fail(f"{path} does not bind an actual mutation")
        baseline_artifact, baseline_file = reference_artifact(
            subject["baseline_artifact_id"],
            f"{path}.baseline_artifact_id",
            "mutation-baseline",
            baseline_hash,
        )
        mutant_artifact, mutant_file = reference_artifact(
            subject["mutant_artifact_id"],
            f"{path}.mutant_artifact_id",
            "mutation-mutant",
            mutant_hash,
        )
        if baseline_file.read_bytes() != authoritative_file.read_bytes():
            fail(f"{path} baseline bytes do not equal the authoritative repository input")
        validate_semantic_mutation(
            baseline_file,
            mutant_file,
            mutation_operator=mutation_operator,
            subject_name=THEOREM_BY_CLASS[subject_name] if expected_kind == "theorem" else subject_name,
            context=path,
        )

        baseline_input_path = baseline_artifact["path"]
        mutant_input_path = mutant_artifact["path"]
        if detector_tool_id == "tlaps":
            expected_baseline_command = ["tlapm", baseline_input_path, "--theorem", theorem_id]
            expected_mutant_command = ["tlapm", mutant_input_path, "--theorem", theorem_id]
            expected_java_hash = None
        else:
            expected_baseline_command = [
                "java", "-jar", "tla2tools.jar", "-config", baseline_input_path, config_model_path
            ]
            expected_mutant_command = [
                "java", "-jar", "tla2tools.jar", "-config", mutant_input_path, config_model_path
            ]
            expected_java_hash = tools["java"]["binary_sha256"]
        baseline_command = command(subject["baseline_command"], f"{path}.baseline_command")
        mutant_command = command(subject["mutant_command"], f"{path}.mutant_command")
        if baseline_command != expected_baseline_command or mutant_command != expected_mutant_command:
            fail(f"{path} mutation commands do not bind the exact baseline and mutant inputs")
        baseline_command_hash = hashlib.sha256(canonical_json_bytes(baseline_command)).hexdigest()
        mutant_command_hash = hashlib.sha256(canonical_json_bytes(mutant_command)).hexdigest()
        literal(subject["baseline_command_sha256"], f"{path}.baseline_command_sha256", baseline_command_hash)
        literal(subject["mutant_command_sha256"], f"{path}.mutant_command_sha256", mutant_command_hash)
        literal(subject["detector_tool_id"], f"{path}.detector_tool_id", detector_tool_id)
        literal(
            subject["detector_tool_binary_sha256"],
            f"{path}.detector_tool_binary_sha256",
            tools[detector_tool_id]["binary_sha256"],
        )
        literal(subject["java_binary_sha256"], f"{path}.java_binary_sha256", expected_java_hash)
        literal(subject["working_directory"], f"{path}.working_directory", ".")
        literal(subject["environment_sha256"], f"{path}.environment_sha256", EMPTY_ENVIRONMENT_SHA256)
        literal(subject["baseline_exit_code"], f"{path}.baseline_exit_code", 0)
        literal(subject["mutant_exit_code"], f"{path}.mutant_exit_code", 1)
        literal(subject["baseline_result"], f"{path}.baseline_result", "accepted")
        literal(subject["mutation_result"], f"{path}.mutation_result", "rejected")
        literal(subject["nonvacuity_result"], f"{path}.nonvacuity_result", "pass")
        detected_by = identifier(subject["detected_by"], f"{path}.detected_by")
        literal(
            detected_by,
            f"{path}.detected_by",
            f"native-{detector_tool_id}-semantic-mutation-detector-v1",
        )
        baseline_completed_at = utc(subject["baseline_completed_at"], f"{path}.baseline_completed_at")
        detector_completed_at = utc(subject["detector_completed_at"], f"{path}.detector_completed_at")
        if (
            baseline_completed_at > detector_completed_at
            or detector_completed_at > mutation_executed_at
        ):
            fail(f"{path} baseline/mutant execution chronology is inconsistent")
        literal(
            subject["witness_observed_at"],
            f"{path}.witness_observed_at",
            subject["baseline_completed_at"],
        )
        execution_times.extend((baseline_completed_at, detector_completed_at))

        detector_hash = sha256(subject["detector_sha256"], f"{path}.detector_sha256")
        _, detector_file = reference_artifact(
            subject["detector_artifact_id"],
            f"{path}.detector_artifact_id",
            "mutation-detector",
            detector_hash,
        )
        detector_keys = {
            "authoritative_id", "authoritative_path", "authoritative_sha256", "baseline_sha256",
            "command", "command_sha256", "completed_at", "detected_by",
            "detector_tool_binary_sha256", "detector_tool_id", "environment_sha256",
            "exit_code", "formal_spec_sha256", "java_binary_sha256", "mutated_sha256",
            "mutation_operator", "record_kind", "release_id", "result", "source_revision",
            "source_tree_id", "subject_id", "subject_kind", "target_id", "working_directory",
        }
        detector = exact_object(
            load_json(detector_file, f"{path} mutant rejection output"),
            detector_keys,
            f"{path} mutant rejection output",
        )
        expected_detector = {
            "record_kind": "formal-closure-mutant-rejection",
            "subject_id": subject_id,
            "subject_kind": expected_kind,
            "authoritative_id": authoritative_id,
            "authoritative_path": authoritative_path,
            "authoritative_sha256": authoritative_hash,
            "mutation_operator": mutation_operator,
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha,
            "target_id": target_id,
            "baseline_sha256": baseline_hash,
            "mutated_sha256": mutant_hash,
            "detected_by": detected_by,
            "command": mutant_command,
            "command_sha256": mutant_command_hash,
            "working_directory": ".",
            "environment_sha256": EMPTY_ENVIRONMENT_SHA256,
            "detector_tool_id": detector_tool_id,
            "detector_tool_binary_sha256": tools[detector_tool_id]["binary_sha256"],
            "java_binary_sha256": expected_java_hash,
            "exit_code": 1,
            "completed_at": subject["detector_completed_at"],
            "result": "rejected",
        }
        if detector != expected_detector:
            fail(f"{path} mutant rejection is not the exact runner-bound semantic result")

        witness_count = integer(subject["witness_count"], f"{path}.witness_count", 1)
        witness_hash = sha256(subject["witness_sha256"], f"{path}.witness_sha256")
        _, witness_file = reference_artifact(
            subject["witness_artifact_id"],
            f"{path}.witness_artifact_id",
            "nonvacuity-witness",
            witness_hash,
        )
        witness_keys = {
            "authoritative_id", "authoritative_path", "authoritative_sha256", "baseline_sha256",
            "command", "command_sha256", "completed_at", "detected_by",
            "detector_tool_binary_sha256", "detector_tool_id", "environment_sha256",
            "exit_code", "formal_spec_sha256", "java_binary_sha256", "mutated_sha256",
            "mutation_operator", "mutant_output_sha256", "record_kind", "release_id",
            "result", "source_revision", "source_tree_id", "subject_id", "subject_kind",
            "target_id", "witness_count", "working_directory",
        }
        witness = exact_object(
            load_json(witness_file, f"{path} baseline acceptance output"),
            witness_keys,
            f"{path} baseline acceptance output",
        )
        expected_witness = {
            "record_kind": "formal-closure-baseline-acceptance",
            "subject_id": subject_id,
            "subject_kind": expected_kind,
            "authoritative_id": authoritative_id,
            "authoritative_path": authoritative_path,
            "authoritative_sha256": authoritative_hash,
            "mutation_operator": mutation_operator,
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha,
            "target_id": target_id,
            "baseline_sha256": baseline_hash,
            "mutated_sha256": mutant_hash,
            "detected_by": detected_by,
            "command": baseline_command,
            "command_sha256": baseline_command_hash,
            "working_directory": ".",
            "environment_sha256": EMPTY_ENVIRONMENT_SHA256,
            "detector_tool_id": detector_tool_id,
            "detector_tool_binary_sha256": tools[detector_tool_id]["binary_sha256"],
            "java_binary_sha256": expected_java_hash,
            "exit_code": 0,
            "completed_at": subject["baseline_completed_at"],
            "result": "accepted",
            "mutant_output_sha256": detector_hash,
            "witness_count": witness_count,
        }
        if witness != expected_witness:
            fail(f"{path} baseline acceptance is not the exact runner-bound nonvacuity result")
        raw_mutation = raw_mutation_by_subject[subject_id]
        raw_witness = raw_witness_by_subject[subject_id]
        expected_raw_mutation = {
            "mutation_id": subject_id,
            "subject_id": subject_id,
            "subject_kind": expected_kind,
            "authoritative_id": authoritative_id,
            "authoritative_path": authoritative_path,
            "authoritative_sha256": authoritative_hash,
            "mutation_operator": mutation_operator,
            "baseline_path": f"inputs/{subject['baseline_artifact_id']}",
            "baseline_sha256": baseline_hash,
            "mutated_path": f"inputs/{subject['mutant_artifact_id']}",
            "mutated_sha256": mutant_hash,
            "detector_output_path": f"inputs/{subject['detector_artifact_id']}",
            "detector_output_sha256": detector_hash,
            "detected_by": detected_by,
            "detector_tool_id": detector_tool_id,
            "detector_tool_binary_sha256": tools[detector_tool_id]["binary_sha256"],
            "java_binary_sha256": expected_java_hash,
            "baseline_command": baseline_command,
            "baseline_command_sha256": baseline_command_hash,
            "mutant_command": mutant_command,
            "mutant_command_sha256": mutant_command_hash,
            "working_directory": ".",
            "environment_sha256": EMPTY_ENVIRONMENT_SHA256,
            "baseline_exit_code": 0,
            "mutant_exit_code": 1,
            "baseline_completed_at": subject["baseline_completed_at"],
            "detector_completed_at": subject["detector_completed_at"],
            "result": "rejected",
        }
        expected_raw_witness = {
            "subject_id": subject_id,
            "subject_kind": expected_kind,
            "authoritative_id": authoritative_id,
            "authoritative_path": authoritative_path,
            "authoritative_sha256": authoritative_hash,
            "mutation_operator": mutation_operator,
            "baseline_sha256": baseline_hash,
            "mutated_sha256": mutant_hash,
            "detector_output_sha256": detector_hash,
            "detected_by": detected_by,
            "detector_tool_id": detector_tool_id,
            "detector_tool_binary_sha256": tools[detector_tool_id]["binary_sha256"],
            "java_binary_sha256": expected_java_hash,
            "baseline_command": baseline_command,
            "baseline_command_sha256": baseline_command_hash,
            "working_directory": ".",
            "environment_sha256": EMPTY_ENVIRONMENT_SHA256,
            "baseline_exit_code": 0,
            "baseline_completed_at": subject["baseline_completed_at"],
            "witness_count": witness_count,
            "result": "accepted",
            "witness_path": f"inputs/{subject['witness_artifact_id']}",
            "witness_sha256": witness_hash,
        }
        if raw_mutation != expected_raw_mutation or raw_witness != expected_raw_witness:
            fail(f"{path} does not match the committed mutation/nonvacuity manifest")
        subject_ids.append(subject_id)
    if subject_ids != sorted(expected_subjects):
        fail("$.mutation_evidence.subjects must be sorted by subject_id")

    trace = exact_object(
        top["trace_refinement"],
        {
            "deterministic", "environment_sha256", "export_command", "export_command_sha256",
            "export_exit_code", "export_result", "exported_at", "exporter", "format", "go_binary_sha256",
            "java_binary_sha256", "replay_artifact_id", "replay_command", "replay_command_sha256",
            "replay_exit_code", "replay_result", "replay_sha256", "replay_tool_binary_sha256",
            "replayed_at", "shaped_spec_model_id", "source_revision", "trace_artifact_id",
            "trace_count", "trace_sha256", "working_directory",
        },
        "$.trace_refinement",
    )
    if revision(trace["source_revision"], "$.trace_refinement.source_revision") != source_revision:
        fail("$.trace_refinement.source_revision does not match the release")
    literal(trace["exporter"], "$.trace_refinement.exporter", "go-to-shaped-spec-v1")
    literal(trace["format"], "$.trace_refinement.format", "moreconsensus-shaped-spec-trace-v1")
    shaped_model_id = identifier(trace["shaped_spec_model_id"], "$.trace_refinement.shaped_spec_model_id")
    literal(shaped_model_id, "$.trace_refinement.shaped_spec_model_id", root_model_ids[0])
    used_models.add(shaped_model_id)
    literal(trace["deterministic"], "$.trace_refinement.deterministic", True)
    trace_count = integer(trace["trace_count"], "$.trace_refinement.trace_count", 1)
    export_argv = command(trace["export_command"], "$.trace_refinement.export_command")
    replay_argv = command(trace["replay_command"], "$.trace_refinement.replay_command")
    expected_export_argv = ["go", "test", "./traceexport", "-run", "TestExport"]
    expected_replay_argv = ["java", "-jar", "trace-replay.jar", "go-shaped.trace.json"]
    if export_argv != expected_export_argv or replay_argv != expected_replay_argv:
        fail("$.trace_refinement commands must be the exact pinned export and replay invocations")
    export_command_hash = hashlib.sha256(canonical_json_bytes(export_argv)).hexdigest()
    replay_command_hash = hashlib.sha256(canonical_json_bytes(replay_argv)).hexdigest()
    literal(trace["export_command_sha256"], "$.trace_refinement.export_command_sha256", export_command_hash)
    literal(trace["replay_command_sha256"], "$.trace_refinement.replay_command_sha256", replay_command_hash)
    literal(trace["java_binary_sha256"], "$.trace_refinement.java_binary_sha256", tools["java"]["binary_sha256"])
    literal(trace["go_binary_sha256"], "$.trace_refinement.go_binary_sha256", tools["go"]["binary_sha256"])
    literal(
        trace["replay_tool_binary_sha256"],
        "$.trace_refinement.replay_tool_binary_sha256",
        tools["trace-replay"]["binary_sha256"],
    )
    literal(trace["working_directory"], "$.trace_refinement.working_directory", ".")
    literal(trace["environment_sha256"], "$.trace_refinement.environment_sha256", EMPTY_ENVIRONMENT_SHA256)
    literal(trace["export_exit_code"], "$.trace_refinement.export_exit_code", 0)
    literal(trace["replay_exit_code"], "$.trace_refinement.replay_exit_code", 0)
    literal(trace["export_result"], "$.trace_refinement.export_result", "pass")
    literal(trace["replay_result"], "$.trace_refinement.replay_result", "pass")
    exported_at = utc(trace["exported_at"], "$.trace_refinement.exported_at")
    replayed_at = utc(trace["replayed_at"], "$.trace_refinement.replayed_at")
    if (
        exported_at > replayed_at
        or replayed_at > generated_at
        or generated_at - replayed_at > MAX_EVIDENCE_AGE
        or generated_at - exported_at > MAX_EVIDENCE_AGE
    ):
        fail("$.trace_refinement export/replay chronology is inconsistent or stale")
    execution_times.extend((exported_at, replayed_at))
    trace_hash = sha256(trace["trace_sha256"], "$.trace_refinement.trace_sha256")
    _, trace_file = reference_artifact(
        trace["trace_artifact_id"],
        "$.trace_refinement.trace_artifact_id",
        "trace-export",
        trace_hash,
    )
    validate_trace_export(
        trace_file,
        release_id=release_id,
        source_revision=source_revision,
        source_tree_id=source_tree_id,
        target_id=target_id,
        formal_spec_sha256=formal_spec_sha,
        exported_at=trace["exported_at"],
        export_command_sha256=export_command_hash,
        go_binary_sha256=tools["go"]["binary_sha256"],
        trace_count=trace_count,
    )
    replay_hash = sha256(trace["replay_sha256"], "$.trace_refinement.replay_sha256")
    _, replay_file = reference_artifact(
        trace["replay_artifact_id"],
        "$.trace_refinement.replay_artifact_id",
        "trace-replay",
        replay_hash,
    )
    replay_marker = parse_marker(replay_file, "FORMAL_CLOSURE_TRACE_REPLAY ", "trace replay raw artifact")
    expected_replay_marker = {
        "deterministic": True,
        "release_id": release_id,
        "source_revision": source_revision,
        "source_tree_id": source_tree_id,
        "target_id": target_id,
        "formal_spec_sha256": formal_spec_sha,
        "trace_count": trace_count,
        "trace_sha256": sha256_file(trace_file),
        "exported_at": trace["exported_at"],
        "replayed_at": trace["replayed_at"],
        "export_command_sha256": export_command_hash,
        "replay_command_sha256": replay_command_hash,
        "java_binary_sha256": tools["java"]["binary_sha256"],
        "go_binary_sha256": tools["go"]["binary_sha256"],
        "trace_replay_binary_sha256": tools["trace-replay"]["binary_sha256"],
        "working_directory": ".",
        "environment_sha256": EMPTY_ENVIRONMENT_SHA256,
        "export_exit_code": 0,
        "replay_exit_code": 0,
        "result": "pass",
    }
    exact_object(replay_marker, set(expected_replay_marker), "trace replay machine result")
    if replay_marker != expected_replay_marker:
        fail("trace replay fields do not match the exact target, commands, tools, and trace")

    joint = exact_object(
        top["joint_consensus"],
        {"collected_at", "status", "declared_scope", "release_scope_enforced", "proof_theorem_id", "scope_artifact_id"},
        "$.joint_consensus",
    )
    joint_status = text(joint["status"], "$.joint_consensus.status")
    declared_scope = text(joint["declared_scope"], "$.joint_consensus.declared_scope")
    lowered_scope = declared_scope.lower()
    literal(joint["release_scope_enforced"], "$.joint_consensus.release_scope_enforced", True)
    if joint_status == "implemented-and-proved":
        theorem_id = identifier(joint["proof_theorem_id"], "$.joint_consensus.proof_theorem_id")
        if theorem_id != THEOREM_BY_CLASS["configuration-induction"]:
            fail("implemented joint consensus must bind the checked configuration induction theorem")
        if "joint consensus" not in lowered_scope or "implemented" not in lowered_scope:
            fail("implemented joint consensus must be explicit in declared_scope")
    elif joint_status == "excluded-from-declared-scope":
        literal(joint["proof_theorem_id"], "$.joint_consensus.proof_theorem_id", None)
        if "joint consensus" not in lowered_scope or "excluded" not in lowered_scope:
            fail("excluded joint consensus must be explicit in declared_scope")
    else:
        fail("$.joint_consensus.status must be implemented-and-proved or excluded-from-declared-scope")
    scope_collected_at = utc(joint["collected_at"], "$.joint_consensus.collected_at")
    if scope_collected_at > generated_at or generated_at - scope_collected_at > MAX_EVIDENCE_AGE:
        fail("$.joint_consensus.collected_at is inconsistent or stale")
    execution_times.append(scope_collected_at)
    _, scope_file = reference_artifact(joint["scope_artifact_id"], "$.joint_consensus.scope_artifact_id", "scope-enforcement")
    scope_marker = parse_marker(scope_file, "FORMAL_CLOSURE_SCOPE_RESULT ", "joint-consensus scope artifact")
    expected_scope_marker = {
        "formal_spec_sha256": formal_spec_sha,
        "collected_at": joint["collected_at"],
        "release_id": release_id,
        "result": "pass",
        "source_revision": source_revision,
        "source_tree_id": source_tree_id,
        "status": joint_status,
        "target_id": target_id,
    }
    exact_object(scope_marker, set(expected_scope_marker), "joint-consensus scope machine result")
    if scope_marker != expected_scope_marker:
        fail("joint-consensus scope fields do not match the exact target")

    toq = exact_object(
        top["toq_clock_discipline"],
        {
            "artifact_id", "artifact_sha256", "clock_source", "collected_at", "evidence_uri", "in_claim",
            "maximum_clock_error_ms", "remote_artifact_id", "remote_artifact_sha256",
            "remote_evidence_sha256", "result", "source_revision", "target_environment",
        },
        "$.toq_clock_discipline",
    )
    literal(toq["in_claim"], "$.toq_clock_discipline.in_claim", True)
    target_environment = text(toq["target_environment"], "$.toq_clock_discipline.target_environment")
    literal(target_environment, "$.toq_clock_discipline.target_environment", "production" if mode == TARGET_MODE else SYNTHETIC_MODE)
    literal(toq["result"], "$.toq_clock_discipline.result", "pass")
    literal(
        toq["clock_source"],
        "$.toq_clock_discipline.clock_source",
        "authenticated monotonic source",
    )
    maximum_clock_error = integer(
        toq["maximum_clock_error_ms"],
        "$.toq_clock_discipline.maximum_clock_error_ms",
        1,
        1000,
    )
    collected_at = utc(toq["collected_at"], "$.toq_clock_discipline.collected_at")
    if collected_at > generated_at or generated_at - collected_at > MAX_EVIDENCE_AGE:
        fail("$.toq_clock_discipline.collected_at is inconsistent or stale")
    execution_times.append(collected_at)
    remote_evidence_hash = sha256(toq["remote_evidence_sha256"], "$.toq_clock_discipline.remote_evidence_sha256")
    evidence_uri = remote_uri(
        toq["evidence_uri"],
        "$.toq_clock_discipline.evidence_uri",
        content_hash=remote_evidence_hash,
    )
    remote_artifact_hash = sha256(
        toq["remote_artifact_sha256"],
        "$.toq_clock_discipline.remote_artifact_sha256",
    )
    literal(
        remote_artifact_hash,
        "$.toq_clock_discipline.remote_artifact_sha256",
        remote_evidence_hash,
    )
    _, remote_observation_file = reference_artifact(
        toq["remote_artifact_id"],
        "$.toq_clock_discipline.remote_artifact_id",
        "toq-clock-observation",
        remote_evidence_hash,
    )
    remote_observation = exact_object(
        load_json(remote_observation_file, "TOQ clock observation"),
        {
            "clock_source", "formal_spec_sha256", "maximum_clock_error_ms", "observed_at",
            "record_kind", "release_id", "result", "source_revision", "source_tree_id",
            "target_environment", "target_id",
        },
        "TOQ clock observation",
    )
    expected_remote_observation = {
        "record_kind": "formal-closure-toq-clock-observation",
        "release_id": release_id,
        "source_revision": source_revision,
        "source_tree_id": source_tree_id,
        "formal_spec_sha256": formal_spec_sha,
        "target_id": target_id,
        "result": "pass",
        "clock_source": "authenticated monotonic source",
        "observed_at": toq["collected_at"],
        "maximum_clock_error_ms": maximum_clock_error,
        "target_environment": target_environment,
    }
    if remote_observation != expected_remote_observation:
        fail("TOQ clock observation bytes do not bind the exact target, source, time, and error bound")
    toq_hash = sha256(toq["artifact_sha256"], "$.toq_clock_discipline.artifact_sha256")
    if revision(toq["source_revision"], "$.toq_clock_discipline.source_revision") != source_revision:
        fail("$.toq_clock_discipline.source_revision does not match the release")
    _, toq_file = reference_artifact(toq["artifact_id"], "$.toq_clock_discipline.artifact_id", "toq-clock-discipline", toq_hash)
    toq_marker = parse_marker(toq_file, "FORMAL_CLOSURE_TOQ_CLOCK ", "TOQ clock-discipline artifact")
    exact_object(
        toq_marker,
        {
            "clock_source", "collected_at", "evidence_uri", "maximum_clock_error_ms",
            "formal_spec_sha256",
            "release_id", "remote_evidence_sha256", "result", "source_revision",
            "source_tree_id", "target_environment", "target_id",
        },
        "TOQ clock machine result",
    )
    if toq_marker != {
        "clock_source": toq["clock_source"],
        "formal_spec_sha256": formal_spec_sha,
        "evidence_uri": evidence_uri,
        "collected_at": toq["collected_at"],
        "maximum_clock_error_ms": maximum_clock_error,
        "remote_evidence_sha256": remote_evidence_hash,
        "release_id": release_id,
        "result": "pass",
        "source_revision": source_revision,
        "source_tree_id": source_tree_id,
        "target_id": target_id,
        "target_environment": target_environment,
    }:
        fail("TOQ clock fields do not match the target evidence artifact")

    command_manifest_hash = canonical_command_manifest_sha256(checks_raw, proofs_raw, trace)
    toolchain_manifest_hash = canonical_toolchain_manifest_sha256(tool_entries)
    execution_input_manifest_hash = hashlib.sha256(
        canonical_json_bytes(
            {
                "properties": property_manifest,
                "specification_manifest": manifest,
                "target": target,
            }
        )
    ).hexdigest()
    native = exact_object(
        top["native_execution"],
        {
            "attested", "command_manifest_sha256", "completed_at", "execution_mode",
            "input_manifest_sha256", "record_artifact_id", "record_sha256", "result",
            "signature_artifact_id", "signature_sha256", "toolchain_manifest_sha256",
        },
        "$.native_execution",
    )
    literal(native["command_manifest_sha256"], "$.native_execution.command_manifest_sha256", command_manifest_hash)
    literal(native["toolchain_manifest_sha256"], "$.native_execution.toolchain_manifest_sha256", toolchain_manifest_hash)
    literal(native["input_manifest_sha256"], "$.native_execution.input_manifest_sha256", execution_input_manifest_hash)
    if mode == SYNTHETIC_MODE:
        literal(native["attested"], "$.native_execution.attested", False)
        literal(native["execution_mode"], "$.native_execution.execution_mode", SYNTHETIC_MODE)
        literal(native["completed_at"], "$.native_execution.completed_at", None)
        literal(native["result"], "$.native_execution.result", "not-applicable")
        literal(native["record_artifact_id"], "$.native_execution.record_artifact_id", None)
        literal(native["record_sha256"], "$.native_execution.record_sha256", None)
        literal(native["signature_artifact_id"], "$.native_execution.signature_artifact_id", None)
        literal(native["signature_sha256"], "$.native_execution.signature_sha256", None)
    else:
        literal(native["attested"], "$.native_execution.attested", True)
        literal(native["execution_mode"], "$.native_execution.execution_mode", "native-darwin")
        literal(native["result"], "$.native_execution.result", "pass")
        native_completed_at = utc(native["completed_at"], "$.native_execution.completed_at")
        if native_completed_at > generated_at or native_completed_at < max(execution_times):
            fail("$.native_execution.completed_at must follow every attested execution result")
        execution_times.append(native_completed_at)
        native_record_hash = sha256(native["record_sha256"], "$.native_execution.record_sha256")
        _, native_record_file = reference_artifact(
            native["record_artifact_id"],
            "$.native_execution.record_artifact_id",
            "native-execution-record",
            native_record_hash,
        )
        native_signature_hash = sha256(native["signature_sha256"], "$.native_execution.signature_sha256")
        _, native_signature_file = reference_artifact(
            native["signature_artifact_id"],
            "$.native_execution.signature_artifact_id",
            "native-execution-signature",
            native_signature_hash,
        )
        native_record = exact_object(
            load_json(native_record_file, "native execution attestation"),
            {
                "architecture", "command_manifest_sha256", "completed_at", "execution_mode",
                "formal_spec_sha256", "input_manifest_sha256", "operating_system", "record_kind",
                "release_id", "result", "source_revision", "source_tree_id", "target_id",
                "toolchain_manifest_sha256",
            },
            "native execution attestation",
        )
        expected_native_record = {
            "record_kind": "formal-closure-native-execution-attestation",
            "execution_mode": "native-darwin",
            "operating_system": "Darwin",
            "architecture": architecture,
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha,
            "target_id": target_id,
            "input_manifest_sha256": execution_input_manifest_hash,
            "command_manifest_sha256": command_manifest_hash,
            "toolchain_manifest_sha256": toolchain_manifest_hash,
            "completed_at": native["completed_at"],
            "result": "pass",
        }
        if native_record != expected_native_record:
            fail("$.native_execution record does not bind the exact target, inputs, commands, and tools")
        verify_rsa_sha256_signature(
            canonical_json_bytes(native_record),
            producer_trust_file,
            native_signature_file,
            "native execution attestation",
        )


    reviewed = exact_object(
        top["reviewed_input_set"],
        {
            "digest_algorithm", "inputs", "manifest_artifact_id", "manifest_sha256",
            "reviewed_input_bindings_sha256",
        },
        "$.reviewed_input_set",
    )
    literal(
        reviewed["digest_algorithm"],
        "$.reviewed_input_set.digest_algorithm",
        "sha256-canonical-json-v1",
    )
    collection_manifest_hash = sha256(
        reviewed["manifest_sha256"],
        "$.reviewed_input_set.manifest_sha256",
    )
    collection_manifest_artifact, collection_manifest_file = reference_artifact(
        reviewed["manifest_artifact_id"],
        "$.reviewed_input_set.manifest_artifact_id",
        "collection-manifest",
        collection_manifest_hash,
    )
    reviewed_inputs = array(reviewed["inputs"], "$.reviewed_input_set.inputs", 1)
    reviewed_by_role: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(reviewed_inputs):
        path = f"$.reviewed_input_set.inputs[{index}]"
        entry = exact_object(raw, {"role", "sha256", "size_bytes"}, path)
        role = identifier(entry["role"], f"{path}.role")
        if role in reviewed_by_role:
            fail(f"$.reviewed_input_set.inputs contains duplicate role {role!r}")
        reviewed_by_role[role] = {
            "role": role,
            "sha256": sha256(entry["sha256"], f"{path}.sha256"),
            "size_bytes": integer(entry["size_bytes"], f"{path}.size_bytes", 1, MAX_ARTIFACT_BYTES),
        }
    if [entry["role"] for entry in reviewed_inputs] != sorted(reviewed_by_role):
        fail("$.reviewed_input_set.inputs must be sorted by role")
    reviewed_digest = sha256(
        reviewed["reviewed_input_bindings_sha256"],
        "$.reviewed_input_set.reviewed_input_bindings_sha256",
    )
    if canonical_reviewed_inputs_sha256(reviewed_inputs) != reviewed_digest:
        fail("$.reviewed_input_set.reviewed_input_bindings_sha256 does not match the canonical input set")

    expected_reviewed: dict[str, dict[str, Any]] = {}

    def expect_reviewed(role: str, digest_value: str, size_value: int) -> None:
        if role in expected_reviewed:
            fail(f"duplicate authoritative reviewed input role {role!r}")
        expected_reviewed[role] = {
            "role": role,
            "sha256": digest_value,
            "size_bytes": size_value,
        }

    def expect_reviewed_artifact(role: str, artifact_id_value: str) -> None:
        artifact = artifacts[artifact_id_value]
        expect_reviewed(role, artifact["sha256"], artifact["size_bytes"])

    def expect_reviewed_repository(role: str, file: Path) -> None:
        expect_reviewed(role, sha256_file(file), file.stat().st_size)

    expect_reviewed_artifact("collection-manifest", collection_manifest_artifact["artifact_id"])
    expect_reviewed_repository("producer-pinned-public-key", producer_trust_file)
    expect_reviewed_repository(
        "reviewer-pinned-public-key",
        regular_file(repository_root, PurePosixPath(REVIEWER_TRUST_ROOT_PATH), "reviewer trust root"),
    )
    for model_id, file in model_files.items():
        expect_reviewed_repository(f"model:{model_id}", file)
    for config_id, file in config_files.items():
        expect_reviewed_repository(f"config:{config_id}", file)
    for tool_id, tool in tools.items():
        expect_reviewed_repository(
            f"tool-pin:{tool_id}",
            regular_file(
                repository_root,
                relative_path(tool["repository_pin_path"], f"toolchain {tool_id} pin path"),
                f"toolchain {tool_id} pin path",
            ),
        )
        expect_reviewed_artifact(f"tool-binary:{tool_id}", tool["binary_artifact_id"])
    expect_reviewed_artifact("mutation-nonvacuity", mutation_evidence["record_artifact_id"])
    for subject in subjects_raw:
        subject_id = subject["subject_id"]
        expect_reviewed_artifact(f"mutation:{subject_id}:baseline", subject["baseline_artifact_id"])
        expect_reviewed_artifact(f"mutation:{subject_id}:mutant", subject["mutant_artifact_id"])
        expect_reviewed_artifact(f"mutation:{subject_id}:detector-output", subject["detector_artifact_id"])
        expect_reviewed_artifact(f"nonvacuity:{subject_id}:witness", subject["witness_artifact_id"])
    for check in checks_raw:
        expect_reviewed_artifact(f"tlc-log:{check['config_id']}", check["tlc_log_artifact_id"])
    for proof in proofs_raw:
        expect_reviewed_artifact(f"tlaps-log:{proof['theorem_class']}", proof["tlaps_log_artifact_id"])
    expect_reviewed_artifact("trace-export", trace["trace_artifact_id"])
    expect_reviewed_artifact("trace-replay", trace["replay_artifact_id"])
    expect_reviewed_artifact("scope-enforcement-source", joint["scope_artifact_id"])
    expect_reviewed_artifact("toq-clock-discipline", toq["artifact_id"])
    expect_reviewed_artifact("toq-clock-observation", toq["remote_artifact_id"])
    if mode == TARGET_MODE:
        expect_reviewed_artifact("native-execution-record", native["record_artifact_id"])
        expect_reviewed_artifact("native-execution-signature", native["signature_artifact_id"])
    if reviewed_by_role != expected_reviewed:
        missing = sorted(set(expected_reviewed) - set(reviewed_by_role))
        extra = sorted(set(reviewed_by_role) - set(expected_reviewed))
        mismatched = sorted(
            role
            for role in set(reviewed_by_role) & set(expected_reviewed)
            if reviewed_by_role[role] != expected_reviewed[role]
        )
        fail(
            "$.reviewed_input_set does not match every authoritative input exactly; "
            f"missing={missing}, extra={extra}, mismatched={mismatched}"
        )
    signoff = exact_object(
        top["sign_off"],
        {
            "artifact_id", "artifact_sha256", "decision", "independent", "mutation_evidence_sha256",
            "reviewed_at", "reviewed_by", "reviewed_input_bindings_sha256",
            "reviewer_trust_root_path", "reviewer_trust_root_sha256", "signature_algorithm",
            "signature_artifact_id", "signed_payload_sha256", "submitted_by",
        },
        "$.sign_off",
    )

    def validate_identity(value: Any, path: str) -> tuple[str, str, str]:
        identity_obj = exact_object(value, {"name", "organization", "authenticated_identity"}, path)
        return (
            text(identity_obj["name"], f"{path}.name"),
            text(identity_obj["organization"], f"{path}.organization"),
            authenticated_identity(
                identity_obj["authenticated_identity"],
                f"{path}.authenticated_identity",
            ),
        )

    submitter = validate_identity(signoff["submitted_by"], "$.sign_off.submitted_by")
    reviewer = validate_identity(signoff["reviewed_by"], "$.sign_off.reviewed_by")
    if submitter[2] != producer_identity:
        fail("$.sign_off.submitted_by authenticated identity must equal the producer signer identity")
    if reviewer[2].casefold() in {submitter[2].casefold(), producer_identity.casefold()}:
        fail("$.sign_off.reviewed_by authenticated identity must be independent from submitter and producer")
    collection_record = exact_object(
        load_json(collection_manifest_file, "formal closure collection manifest"),
        {
            "schema_version", "record_mode", "release", "generated_at", "valid_until", "toolchain",
            "models", "configs", "properties", "finite_model_checks", "inductive_proofs",
            "trace_refinement", "joint_consensus", "toq_clock_discipline",
            "mutation_nonvacuity_path", "sign_off", "producer",
        },
        "formal closure collection manifest",
    )
    literal(
        collection_record["schema_version"],
        "collection manifest.schema_version",
        SCHEMA_VERSION,
    )
    literal(collection_record["record_mode"], "collection manifest.record_mode", mode)
    literal(collection_record["generated_at"], "collection manifest.generated_at", top["generated_at"])
    literal(collection_record["valid_until"], "collection manifest.valid_until", top["valid_until"])
    collection_release = exact_object(
        collection_record["release"],
        {
            "release_id", "source_repository", "source_revision", "source_tree_id",
            "formal_spec_sha256", "formal_spec_model_id", "target_id",
        },
        "collection manifest.release",
    )
    if collection_release != {
        "release_id": release_id,
        "source_repository": release["source_repository"],
        "source_revision": source_revision,
        "source_tree_id": source_tree_id,
        "formal_spec_sha256": formal_spec_sha,
        "formal_spec_model_id": root_model_ids[0],
        "target_id": target_id,
    }:
        fail("collection manifest.release does not bind the exact evidence release and target")

    collection_locators: set[str] = set()

    def collection_locator(value: Any, path: str) -> str:
        locator = str(relative_path(value, path))
        if locator in collection_locators:
            fail(f"{path} duplicates another collection input locator")
        collection_locators.add(locator)
        return locator

    collection_tools = array(
        collection_record["toolchain"],
        "collection manifest.toolchain",
        len(TOOL_IDS),
    )
    tool_projection: list[dict[str, str]] = []
    for index, raw_tool in enumerate(collection_tools):
        path = f"collection manifest.toolchain[{index}]"
        collection_tool = exact_object(
            raw_tool,
            {"tool_id", "version", "repository_pin_path", "binary_path"},
            path,
        )
        tool_id = identifier(collection_tool["tool_id"], f"{path}.tool_id")
        collection_locator(collection_tool["binary_path"], f"{path}.binary_path")
        tool_projection.append(
            {
                "tool_id": tool_id,
                "version": text(collection_tool["version"], f"{path}.version", maximum=128),
                "repository_pin_path": str(
                    relative_path(collection_tool["repository_pin_path"], f"{path}.repository_pin_path")
                ),
            }
        )
    expected_tool_projection = [
        {
            "tool_id": entry["tool_id"],
            "version": entry["version"],
            "repository_pin_path": entry["repository_pin_path"],
        }
        for entry in tool_entries
    ]
    if tool_projection != expected_tool_projection:
        fail("collection manifest.toolchain does not match the exact pinned toolchain")

    collection_models = array(collection_record["models"], "collection manifest.models", 1)
    model_projection = [
        exact_object(entry, {"model_id", "path", "role"}, f"collection manifest.models[{index}]")
        for index, entry in enumerate(collection_models)
    ]
    expected_model_projection = [
        {"model_id": entry["model_id"], "path": entry["path"], "role": entry["role"]}
        for entry in models_raw
    ]
    if model_projection != expected_model_projection:
        fail("collection manifest.models does not match the exact specification manifest")

    collection_configs = array(collection_record["configs"], "collection manifest.configs", 1)
    config_projection = [
        exact_object(entry, {"config_id", "path", "model_id"}, f"collection manifest.configs[{index}]")
        for index, entry in enumerate(collection_configs)
    ]
    expected_config_projection = [
        {"config_id": entry["config_id"], "path": entry["path"], "model_id": entry["model_id"]}
        for entry in configs_raw
    ]
    if config_projection != expected_config_projection:
        fail("collection manifest.configs does not match the exact specification manifest")

    collection_properties = exact_object(
        collection_record["properties"],
        {"invariants", "temporal_properties"},
        "collection manifest.properties",
    )
    for property_group in ("invariants", "temporal_properties"):
        collection_entries = array(
            collection_properties[property_group],
            f"collection manifest.properties.{property_group}",
        )
        collection_projection = [
            exact_object(
                entry,
                {"property_id", "operator", "module_id"},
                f"collection manifest.properties.{property_group}[{index}]",
            )
            for index, entry in enumerate(collection_entries)
        ]
        expected_projection = [
            {
                "property_id": entry["property_id"],
                "operator": entry["operator"],
                "module_id": entry["module_id"],
            }
            for entry in property_manifest[property_group]
        ]
        if collection_projection != expected_projection:
            fail(f"collection manifest.properties.{property_group} does not match the evidence")

    collection_checks = array(
        collection_record["finite_model_checks"],
        "collection manifest.finite_model_checks",
        1,
    )
    check_projection: list[dict[str, Any]] = []
    for index, raw_check in enumerate(collection_checks):
        path = f"collection manifest.finite_model_checks[{index}]"
        collection_check = exact_object(raw_check, {"config_id", "command", "log_path"}, path)
        collection_locator(collection_check["log_path"], f"{path}.log_path")
        check_projection.append(
            {
                "config_id": identifier(collection_check["config_id"], f"{path}.config_id"),
                "command": command(collection_check["command"], f"{path}.command"),
            }
        )
    if check_projection != [
        {"config_id": entry["config_id"], "command": entry["command"]}
        for entry in checks_raw
    ]:
        fail("collection manifest finite checks do not match the exact executed commands")

    collection_proofs = array(
        collection_record["inductive_proofs"],
        "collection manifest.inductive_proofs",
        len(THEOREM_BY_CLASS),
    )
    proof_projection: list[dict[str, Any]] = []
    for index, raw_proof in enumerate(collection_proofs):
        path = f"collection manifest.inductive_proofs[{index}]"
        collection_proof = exact_object(
            raw_proof,
            {"theorem_class", "module_id", "command", "log_path"},
            path,
        )
        collection_locator(collection_proof["log_path"], f"{path}.log_path")
        proof_projection.append(
            {
                "theorem_class": text(collection_proof["theorem_class"], f"{path}.theorem_class"),
                "module_id": identifier(collection_proof["module_id"], f"{path}.module_id"),
                "command": command(collection_proof["command"], f"{path}.command"),
            }
        )
    if proof_projection != [
        {
            "theorem_class": entry["theorem_class"],
            "module_id": entry["module_id"],
            "command": entry["command"],
        }
        for entry in proofs_raw
    ]:
        fail("collection manifest inductive proofs do not match the exact executed commands")

    collection_trace = exact_object(
        collection_record["trace_refinement"],
        {
            "exporter", "format", "shaped_spec_model_id", "export_command", "replay_command",
            "trace_path", "replay_log_path",
        },
        "collection manifest.trace_refinement",
    )
    collection_locator(collection_trace["trace_path"], "collection manifest.trace_refinement.trace_path")
    collection_locator(
        collection_trace["replay_log_path"],
        "collection manifest.trace_refinement.replay_log_path",
    )
    if {
        "exporter": collection_trace["exporter"],
        "format": collection_trace["format"],
        "shaped_spec_model_id": collection_trace["shaped_spec_model_id"],
        "export_command": command(
            collection_trace["export_command"],
            "collection manifest.trace_refinement.export_command",
        ),
        "replay_command": command(
            collection_trace["replay_command"],
            "collection manifest.trace_refinement.replay_command",
        ),
    } != {
        "exporter": trace["exporter"],
        "format": trace["format"],
        "shaped_spec_model_id": trace["shaped_spec_model_id"],
        "export_command": trace["export_command"],
        "replay_command": trace["replay_command"],
    }:
        fail("collection manifest trace refinement does not match the exact executed commands")

    collection_joint = exact_object(
        collection_record["joint_consensus"],
        {"declared_scope", "proof_theorem_id", "scope_log_path"},
        "collection manifest.joint_consensus",
    )
    collection_locator(
        collection_joint["scope_log_path"],
        "collection manifest.joint_consensus.scope_log_path",
    )
    if (
        collection_joint["declared_scope"] != joint["declared_scope"]
        or collection_joint["proof_theorem_id"] != joint["proof_theorem_id"]
    ):
        fail("collection manifest joint-consensus declaration does not match the evidence")

    collection_toq = exact_object(
        collection_record["toq_clock_discipline"],
        {"clock_source", "clock_log_path", "remote_observation_path"},
        "collection manifest.toq_clock_discipline",
    )
    collection_locator(collection_toq["clock_log_path"], "collection manifest.toq_clock_discipline.clock_log_path")
    collection_locator(
        collection_toq["remote_observation_path"],
        "collection manifest.toq_clock_discipline.remote_observation_path",
    )
    literal(
        collection_toq["clock_source"],
        "collection manifest.toq_clock_discipline.clock_source",
        toq["clock_source"],
    )
    collection_locator(
        collection_record["mutation_nonvacuity_path"],
        "collection manifest.mutation_nonvacuity_path",
    )

    collection_signoff = exact_object(
        collection_record["sign_off"],
        {"submitted_by", "reviewed_by", "review_log_path"},
        "collection manifest.sign_off",
    )
    collection_locator(collection_signoff["review_log_path"], "collection manifest.sign_off.review_log_path")
    if validate_identity(
        collection_signoff["submitted_by"],
        "collection manifest.sign_off.submitted_by",
    ) != submitter or validate_identity(
        collection_signoff["reviewed_by"],
        "collection manifest.sign_off.reviewed_by",
    ) != reviewer:
        fail("collection manifest sign-off identities do not match the signed evidence")

    collection_producer = exact_object(
        collection_record["producer"],
        {
            "signer_identity", "decided_at", "signed_at", "native_execution_record_path",
            "native_execution_signature_path",
        },
        "collection manifest.producer",
    )
    literal(
        authenticated_identity(
            collection_producer["signer_identity"],
            "collection manifest.producer.signer_identity",
        ),
        "collection manifest.producer.signer_identity",
        producer_identity,
    )
    literal(
        collection_producer["decided_at"],
        "collection manifest.producer.decided_at",
        attestation["decided_at"],
    )
    literal(
        collection_producer["signed_at"],
        "collection manifest.producer.signed_at",
        attestation["signed_at"],
    )
    native_collection_paths = (
        collection_producer["native_execution_record_path"],
        collection_producer["native_execution_signature_path"],
    )
    if mode == TARGET_MODE:
        for index, value in enumerate(native_collection_paths):
            collection_locator(value, f"collection manifest.producer native locator {index}")
    elif native_collection_paths != (None, None):
        fail("synthetic collection manifest must not declare native execution inputs")
    literal(signoff["independent"], "$.sign_off.independent", True)
    literal(signoff["decision"], "$.sign_off.decision", "approved")
    literal(
        signoff["reviewed_input_bindings_sha256"],
        "$.sign_off.reviewed_input_bindings_sha256",
        reviewed_digest,
    )
    literal(
        signoff["mutation_evidence_sha256"],
        "$.sign_off.mutation_evidence_sha256",
        mutation_record_hash,
    )
    reviewed_at = utc(signoff["reviewed_at"], "$.sign_off.reviewed_at")
    if (
        reviewed_at > signed_at
        or producer_decided_at > reviewed_at
        or producer_decided_at < max(execution_times)
        or generated_at - reviewed_at > MAX_EVIDENCE_AGE
    ):
        fail("$.sign_off review/producer/execution chronology is inconsistent or stale")
    review_hash = sha256(signoff["artifact_sha256"], "$.sign_off.artifact_sha256")
    _, review_file = reference_artifact(signoff["artifact_id"], "$.sign_off.artifact_id", "independent-review", review_hash)
    review_marker = parse_marker(review_file, "FORMAL_CLOSURE_REVIEW_RESULT ", "independent review artifact")
    exact_object(
        review_marker,
        {"authenticated_identity", "decision", "independent", "reviewed_at", "source_revision"},
        "independent review machine result",
    )
    if review_marker != {
        "authenticated_identity": reviewer[2],
        "decision": "approved",
        "independent": True,
        "reviewed_at": signoff["reviewed_at"],
        "source_revision": source_revision,
    }:
        fail("sign-off fields do not match the independent review decision")
    review_binding = parse_marker(
        review_file,
        "FORMAL_CLOSURE_REVIEW_BINDING ",
        "independent review semantic binding",
    )
    expected_review_binding = {
        "decision": "approved",
        "formal_spec_sha256": formal_spec_sha,
        "mutation_nonvacuity_sha256": mutation_record_hash,
        "release_id": release_id,
        "reviewed_at": signoff["reviewed_at"],
        "reviewed_input_bindings_sha256": reviewed_digest,
        "source_revision": source_revision,
        "source_tree_id": source_tree_id,
        "target_id": target_id,
    }
    exact_object(review_binding, set(expected_review_binding), "independent review semantic binding")
    if review_binding != expected_review_binding:
        fail("independent review does not bind the exact target and reviewed input set")
    literal(signoff["signature_algorithm"], "$.sign_off.signature_algorithm", "rsa-sha256")
    reviewer_trust_path = relative_path(
        signoff["reviewer_trust_root_path"],
        "$.sign_off.reviewer_trust_root_path",
    )
    literal(
        str(reviewer_trust_path),
        "$.sign_off.reviewer_trust_root_path",
        REVIEWER_TRUST_ROOT_PATH,
    )
    if reviewer_trust_path.suffix != ".pem":
        fail("$.sign_off.reviewer_trust_root_path must reference a repository-pinned PEM public key")
    reviewer_trust_hash = sha256(
        signoff["reviewer_trust_root_sha256"],
        "$.sign_off.reviewer_trust_root_sha256",
    )
    if reviewer_trust_hash == producer_trust_hash:
        fail("$.sign_off reviewer trust root must be independent from the producer trust root")
    reviewer_trust_file = regular_file(
        repository_root,
        reviewer_trust_path,
        "$.sign_off.reviewer_trust_root_path",
    )
    if mode == TARGET_MODE:
        require_git_tracked(
            repository_root,
            reviewer_trust_path,
            "$.sign_off.reviewer_trust_root_path",
        )
    if sha256_file(reviewer_trust_file) != reviewer_trust_hash:
        fail("$.sign_off.reviewer_trust_root_sha256 does not match the repository trust root")
    if public_key_der(reviewer_trust_file, "reviewer trust root") == public_key_der(
        producer_trust_file,
        "producer trust root",
    ):
        fail("$.sign_off reviewer trust root must contain a key independent from the producer trust root")
    reviewer_signature_payload_hash = sha256(
        signoff["signed_payload_sha256"],
        "$.sign_off.signed_payload_sha256",
    )
    _, reviewer_signature_file = reference_artifact(
        signoff["signature_artifact_id"],
        "$.sign_off.signature_artifact_id",
        "reviewer-signature",
    )
    if set(models) != used_models:
        fail(f"unexercised manifest model(s): {sorted(set(models) - used_models)}")
    if references != set(artifacts):
        fail(f"unreferenced raw artifact(s): {sorted(set(artifacts) - references)}")
    for artifact_id, (source, fact, digest_value) in artifact_sources.items():
        recheck_snapshot_source(
            source,
            fact,
            digest_value,
            f"raw artifact {artifact_id!r}",
        )
        created_at = artifact_created_times[artifact_id]
        if artifacts[artifact_id]["kind"] in {"producer-signature", "reviewer-signature"}:
            if created_at > signed_at:
                fail(f"signature artifact {artifact_id!r} postdates the signed evidence")
        elif created_at > producer_decided_at or created_at > reviewed_at:
            fail(f"reviewed artifact {artifact_id!r} was created after an approval decision")
    attestation_payload = evidence_attestation_payload(top)
    attestation_payload_hash = hashlib.sha256(attestation_payload).hexdigest()
    if reviewer_signature_payload_hash != attestation_payload_hash:
        fail("$.sign_off.signed_payload_sha256 does not match the canonical complete evidence payload")
    if producer_payload_hash != attestation_payload_hash:
        fail("$.production_attestation.signed_payload_sha256 does not match the canonical complete evidence payload")
    verify_rsa_sha256_signature(
        attestation_payload,
        reviewer_trust_file,
        reviewer_signature_file,
        "independent review attestation",
    )
    verify_rsa_sha256_signature(
        attestation_payload,
        producer_trust_file,
        producer_signature_file,
        "production evidence attestation",
    )


    return {
        "claim": claim,
        "formal_spec_sha256": formal_spec_sha,
        "mode": mode,
        "release_id": release_id,
        "source_revision": source_revision,
        "target_id": target_id,
        "environment_profile": target["environment"],
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-release-id", required=True)
    parser.add_argument("--expected-source-revision", required=True)
    parser.add_argument("--expected-formal-spec-sha256", required=True)
    parser.add_argument("--expected-target-id", required=True)
    parser.add_argument("--expected-environment-profile", required=True)
    parser.add_argument(
        "--allow-synthetic-test-fixture",
        action="store_true",
        help="accept a synthetic-test/non-claim record; never use for a production gate",
    )
    parser.add_argument(
        "--repository-root",
        type=Path,
        help="test-only repository root override; requires --allow-synthetic-test-fixture",
    )
    parser.add_argument("evidence", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    try:
        args = parse_args(argv)
        validate_schema_contract()
        document = load_json(args.evidence, "formal closure evidence")
        if args.repository_root is not None and not args.allow_synthetic_test_fixture:
            fail("--repository-root is test-only and requires --allow-synthetic-test-fixture")
        if (
            (args.allow_synthetic_test_fixture or args.repository_root is not None)
            and isinstance(document, dict)
            and document.get("record_mode") != SYNTHETIC_MODE
        ):
            fail("test-only verifier options may be used only with a synthetic-test/non-claim record")
        repository_root = args.repository_root or Path(__file__).resolve().parent.parent
        result = validate_document(
            document,
            evidence_path=args.evidence.resolve(),
            repository_root=repository_root.resolve(),
            expected_release_id=args.expected_release_id,
            expected_source_revision=args.expected_source_revision,
            expected_formal_spec_sha256=args.expected_formal_spec_sha256,
            expected_target_id=args.expected_target_id,
            expected_environment_profile=args.expected_environment_profile,
            allow_synthetic_test_fixture=args.allow_synthetic_test_fixture,
            now=datetime.now(timezone.utc),
        )
        print(
            "formal_closure_evidence=verified "
            f"mode={result['mode']} release_id={result['release_id']} "
            f"source_revision={result['source_revision']} "
            f"formal_spec_sha256={result['formal_spec_sha256']} "
            f"target_id={result['target_id']} "
            f"environment_profile={result['environment_profile']} claim={result['claim']}"
        )
        return 0
    except EvidenceError as exc:
        print(f"formal_closure_evidence=invalid: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
