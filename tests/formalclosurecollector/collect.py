#!/usr/bin/env python3
"""Assemble and externally sign fail-closed formal-closure evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

TESTS_DIR = Path(__file__).resolve().parents[1]
VERIFIER_PATH = TESTS_DIR / "verify_formal_closure_evidence.py"
sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location("formal_closure_collector_verifier", VERIFIER_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError("cannot load the formal closure verifier")
verifier = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verifier)

COLLECTION_SCHEMA_VERSION = "3.0.0"
MUTATION_RECORD_KIND = "formal-closure-mutation-nonvacuity"
NATIVE_EXECUTION_RECORD_KIND = "formal-closure-native-execution-attestation"
REVIEW_BINDING_MARKER = "FORMAL_CLOSURE_REVIEW_BINDING "
COMMAND_BINDING_MARKER = "FORMAL_CLOSURE_COMMAND_BINDING "
EXCLUDED_JOINT_SCOPE = "Joint consensus is excluded from the declared release scope."
IMPLEMENTED_JOINT_SCOPE = (
    "Joint consensus is implemented and covered by the checked configuration-induction theorem."
)
MAX_MANIFEST_BYTES = 2 * 1024 * 1024


class CollectionError(Exception):
    """The supplied raw evidence cannot support a closure record."""


def fail(message: str) -> None:
    raise CollectionError(message)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")


def reviewed_input_entries(values: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"role": role, "sha256": value["sha256"], "size_bytes": value["size_bytes"]}
        for role, value in sorted(values.items())
    ]


def reviewed_input_bindings_sha256(values: dict[str, dict[str, Any]]) -> str:
    return hashlib.sha256(canonical_bytes(reviewed_input_entries(values))).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strict_json(path: Path, context: str, maximum: int = MAX_MANIFEST_BYTES) -> Any:
    try:
        if path.stat().st_size > maximum:
            fail(f"{context} exceeds {maximum} bytes")
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=verifier.reject_duplicate_keys)
    except CollectionError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, verifier.EvidenceError) as exc:
        fail(f"cannot load {context}: {exc}")


def strict_json_bytes(content: bytes, context: str) -> Any:
    if len(content) > MAX_MANIFEST_BYTES:
        fail(f"{context} exceeds {MAX_MANIFEST_BYTES} bytes")
    try:
        return json.loads(content.decode("utf-8"), object_pairs_hook=verifier.reject_duplicate_keys)
    except (UnicodeError, json.JSONDecodeError, verifier.EvidenceError) as exc:
        fail(f"cannot load {context}: {exc}")


def exact(value: Any, keys: Iterable[str], path: str) -> dict[str, Any]:
    expected = set(keys)
    if not isinstance(value, dict):
        fail(f"{path} must be an object")
    if set(value) != expected:
        fail(f"{path} keys mismatch; missing={sorted(expected - set(value))}, extra={sorted(set(value) - expected)}")
    return value


def items(value: Any, path: str, minimum: int = 0) -> list[Any]:
    if not isinstance(value, list) or len(value) < minimum:
        fail(f"{path} must be an array with at least {minimum} item(s)")
    return value


def string(value: Any, path: str, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or value != value.strip() or "\n" in value or "\r" in value:
        fail(f"{path} must be a non-empty single-line string")
    return value


def identifier(value: Any, path: str) -> str:
    result = string(value, path, 128)
    if verifier.IDENTIFIER_RE.fullmatch(result) is None:
        fail(f"{path} is not a valid identifier")
    return result


def sha256_text(value: Any, path: str) -> str:
    result = string(value, path, 64)
    if verifier.SHA256_RE.fullmatch(result) is None:
        fail(f"{path} is not a SHA-256 digest")
    return result


def revision(value: Any, path: str) -> str:
    result = string(value, path, 64)
    if verifier.REVISION_RE.fullmatch(result) is None:
        fail(f"{path} must be a 40- or 64-character lowercase hexadecimal revision")
    return result


def utc(value: Any, path: str) -> datetime:
    result = string(value, path, 20)
    if verifier.UTC_RE.fullmatch(result) is None:
        fail(f"{path} must use YYYY-MM-DDTHH:MM:SSZ")
    try:
        return datetime.strptime(result, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        fail(f"{path} is not a valid UTC timestamp: {exc}")


def integer(value: Any, path: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        fail(f"{path} must be an integer >= {minimum}")
    return value


def relative_repository_file(repository: Path, raw: Any, path: str, suffix: str | None = None) -> tuple[str, Path]:
    value = string(raw, path)
    parsed = PurePosixPath(value)
    if parsed.is_absolute() or not parsed.parts or any(part in {"", ".", ".."} for part in parsed.parts):
        fail(f"{path} must be a normalized repository-relative path")
    if suffix is not None and parsed.suffix != suffix:
        fail(f"{path} must end in {suffix}")
    resolved = (repository / Path(*parsed.parts)).resolve()
    try:
        resolved.relative_to(repository.resolve())
    except ValueError:
        fail(f"{path} escapes the repository root")
    require_regular(resolved, path)
    return str(parsed), resolved


def resolve_input(base: Path, raw: Any, path: str) -> Path:
    value = string(raw, path)
    candidate = Path(value)
    resolved = (candidate if candidate.is_absolute() else base / candidate).resolve()
    require_regular(resolved, path)
    return resolved


def require_regular(path: Path, context: str) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        fail(f"cannot inspect {context}: {exc}")
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        fail(f"{context} must be a regular non-symlink file")


def openssl(args: list[str], context: str, payload: bytes | None = None) -> bytes:
    try:
        completed = subprocess.run(
            ["openssl", *args],
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot run openssl for {context}: {exc}")
    if completed.returncode != 0:
        fail(f"openssl failed for {context}: {completed.stderr.decode('utf-8', errors='replace').strip()}")
    return completed.stdout


def validate_key_pair(private_key: Path, public_key: Path, context: str) -> bytes:
    require_regular(private_key, f"{context} private key")
    require_regular(public_key, f"{context} public key")
    openssl(["rsa", "-in", str(private_key), "-check", "-noout"], f"{context} RSA private key")
    openssl(["rsa", "-pubin", "-in", str(public_key), "-noout"], f"{context} RSA public key")
    derived = openssl(["pkey", "-in", str(private_key), "-pubout", "-outform", "DER"], f"{context} private/public derivation")
    supplied = openssl(["pkey", "-pubin", "-in", str(public_key), "-outform", "DER"], f"{context} public key normalization")
    if derived != supplied:
        fail(f"{context} private key does not correspond to the supplied pinned public key")
    return supplied


def sign(private_key: Path, payload: bytes, context: str) -> bytes:
    return openssl(["dgst", "-sha256", "-sign", str(private_key)], context, payload)


def parse_marker(path: Path, prefix: str, context: str) -> dict[str, Any]:
    try:
        return verifier.parse_marker(path, prefix, context)
    except verifier.EvidenceError as exc:
        fail(str(exc))


def marker_exact(path: Path, prefix: str, keys: Iterable[str], context: str) -> dict[str, Any]:
    result = parse_marker(path, prefix, context)
    return exact(result, keys, f"{context} machine result")


def parse_marker_bytes(content: bytes, prefix: str, context: str) -> dict[str, Any]:
    matches: list[str] = []
    try:
        for raw_line in content.splitlines():
            if len(raw_line) > verifier.MAX_MARKER_BYTES:
                fail(f"{context} contains an oversized line")
            line = raw_line.decode("utf-8")
            if line.startswith(prefix):
                matches.append(line[len(prefix) :])
    except UnicodeError as exc:
        fail(f"cannot inspect {context}: {exc}")
    if len(matches) != 1:
        fail(f"{context} must contain exactly one {prefix.strip()} machine result")
    try:
        value = json.loads(matches[0], object_pairs_hook=verifier.reject_duplicate_keys)
    except (json.JSONDecodeError, verifier.EvidenceError) as exc:
        fail(f"invalid machine result in {context}: {exc}")
    if not isinstance(value, dict):
        fail(f"machine result in {context} must be an object")
    return value


def marker_bytes_exact(
    content: bytes,
    prefix: str,
    keys: Iterable[str],
    context: str,
) -> dict[str, Any]:
    return exact(
        parse_marker_bytes(content, prefix, context),
        keys,
        f"{context} machine result",
    )


def ensure_fresh(event: datetime, generated_at: datetime, path: str) -> None:
    if event > generated_at or generated_at - event > verifier.MAX_EVIDENCE_AGE:
        fail(f"{path} is inconsistent or stale")


def exact_strings(value: Any, path: str, minimum: int = 0) -> list[str]:
    result = [
        identifier(item, f"{path}[{index}]")
        for index, item in enumerate(items(value, path, minimum))
    ]
    if len(result) != len(set(result)):
        fail(f"{path} contains duplicate values")
    return result


class InputBindings:
    def __init__(self) -> None:
        self._paths: dict[Path, str] = {}
        self._files: dict[tuple[int, int], str] = {}
        self.values: dict[str, dict[str, Any]] = {}

    def _reserve(self, role: str, path: Path) -> None:
        require_regular(path, role)
        resolved = path.resolve()
        previous = self._paths.get(resolved)
        if previous is not None:
            fail(f"duplicate input artifact path used by {previous!r} and {role!r}")
        if role in self.values:
            fail(f"duplicate input role {role!r}")
        self._paths[resolved] = role

    def _open(self, role: str, path: Path) -> tuple[int, os.stat_result]:
        self._reserve(role, path)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
            before = os.fstat(descriptor)
        except OSError as exc:
            fail(f"cannot snapshot input {role!r}: {exc}")
        if not stat.S_ISREG(before.st_mode):
            os.close(descriptor)
            fail(f"input {role!r} must be a regular file")
        file_id = (before.st_dev, before.st_ino)
        previous = self._files.get(file_id)
        if previous is not None:
            os.close(descriptor)
            fail(f"duplicate input artifact file used by {previous!r} and {role!r}")
        self._files[file_id] = role
        return descriptor, before

    def _finish(
        self,
        role: str,
        descriptor: int,
        before: os.stat_result,
        digest: str,
        size: int,
    ) -> tuple[str, int]:
        after = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or size != before.st_size
        ):
            fail(f"input {role!r} changed while it was being snapshotted")
        if size < 1 or size > verifier.MAX_ARTIFACT_BYTES:
            fail(f"input {role!r} has invalid size {size}")
        self.values[role] = {"sha256": digest, "size_bytes": size}
        return digest, size

    def snapshot(self, role: str, path: Path) -> tuple[bytes, str]:
        descriptor, before = self._open(role, path)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        size = 0
        try:
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > verifier.MAX_ARTIFACT_BYTES:
                    fail(f"input {role!r} exceeds the artifact limit")
                chunks.append(chunk)
                digest.update(chunk)
            result = self._finish(role, descriptor, before, digest.hexdigest(), size)
        finally:
            os.close(descriptor)
        return b"".join(chunks), result[0]

    def copy_snapshot(self, role: str, path: Path, destination: Path) -> tuple[str, int]:
        descriptor, before = self._open(role, path)
        digest = hashlib.sha256()
        size = 0
        try:
            with destination.open("xb") as output:
                while True:
                    chunk = os.read(descriptor, 1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > verifier.MAX_ARTIFACT_BYTES:
                        fail(f"input {role!r} exceeds the artifact limit")
                    output.write(chunk)
                    digest.update(chunk)
            return self._finish(role, descriptor, before, digest.hexdigest(), size)
        finally:
            os.close(descriptor)

    def add(self, role: str, path: Path) -> tuple[str, int]:
        _, digest = self.snapshot(role, path)
        return digest, self.values[role]["size_bytes"]


class ArtifactWriter:
    def __init__(self, stage: Path, source_revision: str, created_at: str, bindings: InputBindings) -> None:
        self.stage = stage
        self.artifacts_dir = stage / "artifacts"
        self.artifacts_dir.mkdir()
        self.source_revision = source_revision
        self.created_at = created_at
        self.bindings = bindings
        self.records: list[dict[str, Any]] = []
        self._ids: set[str] = set()
        self._hash_roles: dict[str, str] = {}

    def _record(self, artifact_id: str, kind: str, suffix: str, path: Path, digest: str, size: int) -> Path:
        identifier(artifact_id, "artifact id")
        if artifact_id in self._ids:
            fail(f"duplicate output artifact id {artifact_id!r}")
        previous = self._hash_roles.get(digest)
        if previous is not None and kind != "mutation-baseline":
            fail(f"duplicate output artifact content for {previous!r} and {artifact_id!r}")
        self._ids.add(artifact_id)
        if previous is None:
            self._hash_roles[digest] = artifact_id
        relative = f"artifacts/{artifact_id}{suffix}"
        self.records.append(
            {
                "artifact_id": artifact_id,
                "kind": kind,
                "path": relative,
                "sha256": digest,
                "size_bytes": size,
                "source_revision": self.source_revision,
                "created_at": self.created_at,
            }
        )
        return path

    def add_input(self, artifact_id: str, kind: str, suffix: str, role: str, source: Path) -> tuple[Path, str]:
        destination = self.artifacts_dir / f"{artifact_id}{suffix}"
        digest, size = self.bindings.copy_snapshot(role, source, destination)
        os.chmod(destination, 0o444)
        return self._record(artifact_id, kind, suffix, destination, digest, size), digest

    def add_generated(self, artifact_id: str, kind: str, suffix: str, content: bytes) -> tuple[Path, str]:
        if not content or len(content) > verifier.MAX_ARTIFACT_BYTES:
            fail(f"generated artifact {artifact_id!r} has invalid size")
        destination = self.artifacts_dir / f"{artifact_id}{suffix}"
        destination.write_bytes(content)
        os.chmod(destination, 0o444)
        digest = hashlib.sha256(content).hexdigest()
        return self._record(artifact_id, kind, suffix, destination, digest, len(content)), digest

    def reserve_generated(
        self,
        artifact_id: str,
        kind: str,
        suffix: str,
        size: int,
    ) -> tuple[Path, dict[str, Any]]:
        identifier(artifact_id, "artifact id")
        if artifact_id in self._ids or size < 1 or size > verifier.MAX_ARTIFACT_BYTES:
            fail(f"cannot reserve generated artifact {artifact_id!r}")
        self._ids.add(artifact_id)
        destination = self.artifacts_dir / f"{artifact_id}{suffix}"
        record = {
            "artifact_id": artifact_id,
            "kind": kind,
            "path": f"artifacts/{artifact_id}{suffix}",
            "sha256": "0" * 64,
            "size_bytes": size,
            "source_revision": self.source_revision,
            "created_at": self.created_at,
        }
        self.records.append(record)
        return destination, record

    def finalize_reserved(
        self,
        destination: Path,
        record: dict[str, Any],
        content: bytes,
    ) -> Path:
        if len(content) != record["size_bytes"]:
            fail(f"generated artifact {record['artifact_id']!r} changed size after payload binding")
        digest = hashlib.sha256(content).hexdigest()
        previous = self._hash_roles.get(digest)
        if previous is not None:
            fail(f"duplicate output artifact content for {previous!r} and {record['artifact_id']!r}")
        destination.write_bytes(content)
        os.chmod(destination, 0o444)
        record["sha256"] = digest
        self._hash_roles[digest] = record["artifact_id"]
        return destination


def validate_identity(value: Any, path: str) -> dict[str, str]:
    identity = exact(value, {"name", "organization", "authenticated_identity"}, path)
    return {
        "name": string(identity["name"], f"{path}.name"),
        "organization": string(identity["organization"], f"{path}.organization"),
        "authenticated_identity": verifier.authenticated_identity(
            identity["authenticated_identity"],
            f"{path}.authenticated_identity",
        ),
    }


def validate_properties(raw: Any, model_ids: set[str]) -> dict[str, list[dict[str, Any]]]:
    manifest = exact(raw, {"invariants", "temporal_properties"}, "$.properties")

    def validate_entries(value: Any, path: str, expected_ids: tuple[str, ...]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        seen: set[str] = set()
        for index, raw_entry in enumerate(items(value, path, len(expected_ids))):
            entry_path = f"{path}[{index}]"
            entry = exact(raw_entry, {"property_id", "operator", "module_id"}, entry_path)
            property_id = identifier(entry["property_id"], f"{entry_path}.property_id")
            if property_id in seen:
                fail(f"{path} contains duplicate property {property_id!r}")
            operator = identifier(entry["operator"], f"{entry_path}.operator")
            if operator != property_id:
                fail(f"{entry_path}.operator must equal property_id")
            module_id = identifier(entry["module_id"], f"{entry_path}.module_id")
            if module_id not in model_ids:
                fail(f"{entry_path}.module_id references an unknown model")
            entries.append(
                {
                    "property_id": property_id,
                    "operator": operator,
                    "module_id": module_id,
                    "required": True,
                }
            )
            seen.add(property_id)
        if tuple(sorted(seen)) != expected_ids or [entry["property_id"] for entry in entries] != sorted(seen):
            fail(f"{path} must be sorted and cover the exact required property set")
        return entries

    return {
        "invariants": validate_entries(manifest["invariants"], "$.properties.invariants", verifier.INVARIANT_IDS),
        "temporal_properties": validate_entries(
            manifest["temporal_properties"], "$.properties.temporal_properties", verifier.TEMPORAL_IDS
        ),
    }


def active_tla_text(source: str, context: str) -> str:
    visible: list[str] = []
    block_depth = 0
    index = 0
    while index < len(source):
        if source.startswith("(*", index):
            block_depth += 1
            visible.extend("  ")
            index += 2
        elif source.startswith("*)", index):
            if block_depth == 0:
                fail(f"{context} contains an unmatched block-comment terminator")
            block_depth -= 1
            visible.extend("  ")
            index += 2
        elif block_depth:
            visible.append("\n" if source[index] == "\n" else " ")
            index += 1
        elif source.startswith("\\*", index):
            newline = source.find("\n", index)
            if newline < 0:
                visible.extend(" " * (len(source) - index))
                index = len(source)
            else:
                visible.extend(" " * (newline - index))
                visible.append("\n")
                index = newline + 1
        else:
            visible.append(source[index])
            index += 1
    if block_depth:
        fail(f"{context} contains an unterminated block comment")
    return "".join(visible)


def exact_semantic_mutant(
    authoritative: bytes,
    *,
    pattern: str,
    replacement: str,
    context: str,
) -> bytes:
    try:
        source = authoritative.decode("utf-8")
    except UnicodeError as exc:
        fail(f"cannot decode {context}: {exc}")
    active = active_tla_text(source, context)
    matches = list(re.finditer(pattern, active, re.MULTILINE))
    if len(matches) != 1:
        fail(f"{context} must contain exactly one active mutation subject")
    match = matches[0]
    line_start = source.rfind("\n", 0, match.start()) + 1
    line_end = source.find("\n", match.end())
    if line_end < 0:
        line_end = len(source)
        terminator = ""
    else:
        terminator = "\n"
    indent = re.match(r"\s*", source[line_start:line_end]).group(0)
    mutant = source[:line_start] + indent + replacement + terminator + source[line_end + len(terminator):]
    return mutant.encode("utf-8")


def validate_mutation_record(
    record: Any,
    *,
    record_base: Path,
    repository_root: Path,
    writer: ArtifactWriter,
    record_mode: str,
    release_id: str,
    source_revision: str,
    source_tree_id: str,
    formal_spec_sha256: str,
    target_id: str,
    models: list[dict[str, Any]],
    configs: list[dict[str, Any]],
    properties: dict[str, list[dict[str, Any]]],
    tool_hashes: dict[str, str],
    generated_at: datetime,
) -> dict[str, Any]:
    top = exact(
        record,
        {
            "schema_version", "record_kind", "release_id", "source_revision", "source_tree_id",
            "formal_spec_sha256", "target_id", "evidence_mode", "executed_at", "result",
            "mutations", "nonvacuity",
        },
        "mutation evidence",
    )
    expected_header = {
        "schema_version": COLLECTION_SCHEMA_VERSION,
        "record_kind": MUTATION_RECORD_KIND,
        "release_id": release_id,
        "source_revision": source_revision,
        "source_tree_id": source_tree_id,
        "formal_spec_sha256": formal_spec_sha256,
        "target_id": target_id,
        "evidence_mode": record_mode,
        "executed_at": top["executed_at"],
        "result": "pass",
    }
    for key, expected in expected_header.items():
        if top[key] != expected:
            fail(f"mutation evidence {key} does not match the exact target contract")
    campaign_completed_at = utc(top["executed_at"], "mutation evidence.executed_at")
    ensure_fresh(campaign_completed_at, generated_at, "mutation evidence.executed_at")

    model_by_id = {entry["model_id"]: entry for entry in models}
    config_by_id = {entry["config_id"]: entry for entry in configs}
    property_by_id = {
        entry["property_id"]: (kind, entry)
        for kind, entries in (
            ("INVARIANT", properties["invariants"]),
            ("PROPERTY", properties["temporal_properties"]),
        )
        for entry in entries
    }
    expected_subjects = {
        *(f"theorem:{name}" for name in verifier.THEOREM_BY_CLASS),
        *(f"config:{name}" for name in config_by_id),
        *(f"property:{name}" for name in property_by_id),
    }

    def authoritative_mutation(subject_id: str) -> dict[str, Any]:
        subject_kind, _, subject_name = subject_id.partition(":")
        if subject_kind == "theorem":
            theorem_id = verifier.THEOREM_BY_CLASS[subject_name]
            candidates: list[tuple[dict[str, Any], bytes, bytes]] = []
            theorem_pattern = rf"^\s*THEOREM\s+{re.escape(theorem_id)}(?:\s|==).*$"
            for model in models:
                file = repository_root / model["path"]
                content = file.read_bytes()
                try:
                    mutant = exact_semantic_mutant(
                        content,
                        pattern=theorem_pattern,
                        replacement=f"\\* MUTATION removed theorem {theorem_id}",
                        context=f"theorem {theorem_id}",
                    )
                except CollectionError:
                    continue
                candidates.append((model, content, mutant))
            if len(candidates) != 1:
                fail(f"mutation subject {subject_id!r} must identify exactly one authoritative theorem")
            authority, baseline, mutant = candidates[0]
            return {
                "authoritative_id": authority["model_id"],
                "authoritative_path": authority["path"],
                "authoritative_sha256": authority["sha256"],
                "baseline": baseline,
                "mutant": mutant,
                "suffix": ".tla",
                "mutation_operator": "remove-theorem-declaration",
                "detector_tool_id": "tlaps",
                "detector_tool_binary_sha256": tool_hashes["tlaps"],
                "java_binary_sha256": None,
                "theorem_id": theorem_id,
                "model_path": authority["path"],
                "detected_by": "tlaps-exit-status",
            }
        if subject_kind == "config":
            authority = config_by_id[subject_name]
            baseline = (repository_root / authority["path"]).read_bytes()
            mutant = exact_semantic_mutant(
                baseline,
                pattern=r"^\s*CHECK_DEADLOCK\s+TRUE\s*$",
                replacement="CHECK_DEADLOCK FALSE",
                context=f"config {subject_name}",
            )
            return {
                "authoritative_id": authority["config_id"],
                "authoritative_path": authority["path"],
                "authoritative_sha256": authority["sha256"],
                "baseline": baseline,
                "mutant": mutant,
                "suffix": ".cfg",
                "mutation_operator": "disable-deadlock-check",
                "detector_tool_id": "tlc",
                "detector_tool_binary_sha256": tool_hashes["tlc"],
                "java_binary_sha256": tool_hashes["java"],
                "model_path": model_by_id[authority["model_id"]]["path"],
                "detected_by": "tlc-exit-status",
            }
        keyword, property_entry = property_by_id[subject_name]
        candidates: list[tuple[dict[str, Any], bytes, bytes]] = []
        pattern = rf"^\s*{keyword}\s+{re.escape(subject_name)}\s*$"
        for config in configs:
            if config["model_id"] != property_entry["module_id"]:
                continue
            content = (repository_root / config["path"]).read_bytes()
            try:
                mutant = exact_semantic_mutant(
                    content,
                    pattern=pattern,
                    replacement=f"\\* MUTATION removed {keyword} {subject_name}",
                    context=f"property {subject_name}",
                )
            except CollectionError:
                continue
            candidates.append((config, content, mutant))
        if len(candidates) != 1:
            fail(f"mutation subject {subject_id!r} must identify exactly one authoritative property config")
        authority, baseline, mutant = candidates[0]
        return {
            "authoritative_id": authority["config_id"],
            "authoritative_path": authority["path"],
            "authoritative_sha256": authority["sha256"],
            "baseline": baseline,
            "mutant": mutant,
            "suffix": ".cfg",
            "mutation_operator": "remove-property-directive",
            "detector_tool_id": "tlc",
            "detector_tool_binary_sha256": tool_hashes["tlc"],
            "java_binary_sha256": tool_hashes["java"],
            "model_path": model_by_id[authority["model_id"]]["path"],
            "detected_by": "tlc-exit-status",
        }

    mutations_by_subject: dict[str, dict[str, Any]] = {}
    mutations_raw = items(top["mutations"], "mutation evidence.mutations", len(expected_subjects))
    for index, raw_mutation in enumerate(mutations_raw):
        path = f"mutation evidence.mutations[{index}]"
        mutation = exact(
            raw_mutation,
            {
                "mutation_id", "subject_id", "subject_kind", "baseline_path", "baseline_sha256",
                "mutated_path", "mutated_sha256", "detector_output_path",
                "detector_output_sha256", "result", "detected_by",
            },
            path,
        )
        subject_id = identifier(mutation["subject_id"], f"{path}.subject_id")
        expected_kind = subject_id.partition(":")[0]
        if (
            subject_id not in expected_subjects
            or subject_id in mutations_by_subject
            or mutation["mutation_id"] != subject_id
            or mutation["subject_kind"] != expected_kind
        ):
            fail(f"{path} must uniquely identify a required theorem, config, or property")
        authority = authoritative_mutation(subject_id)
        if mutation["detected_by"] != authority["detected_by"]:
            fail(f"{path}.detected_by does not identify the pinned semantic detector")
        token = hashlib.sha256(subject_id.encode("utf-8")).hexdigest()[:20]
        suffix = authority["suffix"]
        baseline_id = f"mutation-{token}-baseline"
        mutant_id = f"mutation-{token}-mutant"
        baseline_source = resolve_input(record_base, mutation["baseline_path"], f"{path}.baseline_path")
        baseline_file, baseline_hash = writer.add_input(
            baseline_id,
            "mutation-baseline",
            suffix,
            f"mutation:{subject_id}:baseline",
            baseline_source,
        )
        if (
            sha256_text(mutation["baseline_sha256"], f"{path}.baseline_sha256") != baseline_hash
            or baseline_file.read_bytes() != authority["baseline"]
            or baseline_hash != authority["authoritative_sha256"]
        ):
            fail(f"{path} baseline does not byte-equal its authoritative repository input")
        mutant_source = resolve_input(record_base, mutation["mutated_path"], f"{path}.mutated_path")
        mutant_file, mutant_hash = writer.add_input(
            mutant_id,
            "mutation-mutant",
            suffix,
            f"mutation:{subject_id}:mutant",
            mutant_source,
        )
        if (
            sha256_text(mutation["mutated_sha256"], f"{path}.mutated_sha256") != mutant_hash
            or mutant_file.read_bytes() != authority["mutant"]
        ):
            fail(f"{path} mutant is not the exact subject-specific semantic mutation")

        baseline_relative = f"artifacts/{baseline_id}{suffix}"
        mutant_relative = f"artifacts/{mutant_id}{suffix}"
        if expected_kind == "theorem":
            baseline_command = ["tlapm", baseline_relative, "--theorem", authority["theorem_id"]]
            mutant_command = ["tlapm", mutant_relative, "--theorem", authority["theorem_id"]]
        else:
            baseline_command = [
                "java", "-jar", "tla2tools.jar", "-config", baseline_relative, authority["model_path"],
            ]
            mutant_command = [
                "java", "-jar", "tla2tools.jar", "-config", mutant_relative, authority["model_path"],
            ]
        baseline_command_hash = hashlib.sha256(canonical_bytes(baseline_command)).hexdigest()
        mutant_command_hash = hashlib.sha256(canonical_bytes(mutant_command)).hexdigest()

        detector_source = resolve_input(
            record_base,
            mutation["detector_output_path"],
            f"{path}.detector_output_path",
        )
        detector_file, detector_hash = writer.add_input(
            f"mutation-{token}-detector",
            "mutation-detector",
            ".json",
            f"mutation:{subject_id}:detector-output",
            detector_source,
        )
        if sha256_text(mutation["detector_output_sha256"], f"{path}.detector_output_sha256") != detector_hash:
            fail(f"{path}.detector_output_sha256 does not match the detector output")
        detector = exact(
            strict_json(detector_file, f"{path} detector output"),
            {
                "record_kind", "subject_id", "subject_kind", "authoritative_id",
                "authoritative_path", "authoritative_sha256", "mutation_operator", "release_id",
                "source_revision", "source_tree_id", "formal_spec_sha256", "target_id",
                "baseline_sha256", "mutated_sha256", "detected_by", "command",
                "command_sha256", "working_directory", "environment_sha256", "detector_tool_id",
                "detector_tool_binary_sha256", "java_binary_sha256", "exit_code",
                "completed_at", "result",
            },
            f"{path} detector output",
        )
        expected_detector = {
            "record_kind": "formal-closure-mutant-rejection",
            "subject_id": subject_id,
            "subject_kind": expected_kind,
            "authoritative_id": authority["authoritative_id"],
            "authoritative_path": authority["authoritative_path"],
            "authoritative_sha256": authority["authoritative_sha256"],
            "mutation_operator": authority["mutation_operator"],
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha256,
            "target_id": target_id,
            "baseline_sha256": baseline_hash,
            "mutated_sha256": mutant_hash,
            "detected_by": authority["detected_by"],
            "command": mutant_command,
            "command_sha256": mutant_command_hash,
            "working_directory": ".",
            "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
            "detector_tool_id": authority["detector_tool_id"],
            "detector_tool_binary_sha256": authority["detector_tool_binary_sha256"],
            "java_binary_sha256": authority["java_binary_sha256"],
            "exit_code": 1,
            "completed_at": detector["completed_at"],
            "result": "rejected",
        }
        detector_completed_at = utc(detector["completed_at"], f"{path} detector output.completed_at")
        ensure_fresh(detector_completed_at, generated_at, f"{path} detector output.completed_at")
        if (
            detector_completed_at > campaign_completed_at
            or mutation["result"] != "rejected"
            or detector != expected_detector
        ):
            fail(f"{path} lacks a runner-bound target-specific mutant rejection")
        mutations_by_subject[subject_id] = {
            "subject_id": subject_id,
            "subject_kind": expected_kind,
            "authoritative_id": authority["authoritative_id"],
            "authoritative_path": authority["authoritative_path"],
            "authoritative_sha256": authority["authoritative_sha256"],
            "mutation_operator": authority["mutation_operator"],
            "baseline_artifact_id": baseline_id,
            "baseline_sha256": baseline_hash,
            "mutant_artifact_id": mutant_id,
            "mutant_sha256": mutant_hash,
            "detector_artifact_id": detector_file.stem,
            "detector_sha256": detector_hash,
            "detected_by": authority["detected_by"],
            "detector_tool_id": authority["detector_tool_id"],
            "detector_tool_binary_sha256": authority["detector_tool_binary_sha256"],
            "java_binary_sha256": authority["java_binary_sha256"],
            "baseline_command": baseline_command,
            "baseline_command_sha256": baseline_command_hash,
            "mutant_command": mutant_command,
            "mutant_command_sha256": mutant_command_hash,
            "working_directory": ".",
            "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
            "baseline_exit_code": 0,
            "mutant_exit_code": 1,
            "detector_completed_at": detector["completed_at"],
            "mutation_result": "rejected",
        }
    if set(mutations_by_subject) != expected_subjects or [
        entry["subject_id"] for entry in mutations_raw
    ] != sorted(expected_subjects):
        fail("mutation evidence must be sorted and cover every theorem, config, and property")

    witnesses_by_subject: dict[str, dict[str, Any]] = {}
    nonvacuity_raw = items(top["nonvacuity"], "mutation evidence.nonvacuity", len(expected_subjects))
    for index, raw_entry in enumerate(nonvacuity_raw):
        path = f"mutation evidence.nonvacuity[{index}]"
        entry = exact(
            raw_entry,
            {
                "subject_id", "subject_kind", "baseline_sha256", "mutated_sha256",
                "detector_output_sha256", "detected_by", "witness_count", "result",
                "witness_path", "witness_sha256",
            },
            path,
        )
        subject_id = identifier(entry["subject_id"], f"{path}.subject_id")
        if subject_id not in mutations_by_subject or subject_id in witnesses_by_subject:
            fail(f"{path}.subject_id must uniquely match a required subject mutation")
        mutation = mutations_by_subject[subject_id]
        if (
            entry["subject_kind"] != mutation["subject_kind"]
            or entry["baseline_sha256"] != mutation["baseline_sha256"]
            or entry["mutated_sha256"] != mutation["mutant_sha256"]
            or entry["detector_output_sha256"] != mutation["detector_sha256"]
            or entry["detected_by"] != mutation["detected_by"]
        ):
            fail(f"{path} does not bind its exact baseline, mutant, and semantic detector")
        witness_count = integer(entry["witness_count"], f"{path}.witness_count", 1)
        witness_source = resolve_input(record_base, entry["witness_path"], f"{path}.witness_path")
        token = hashlib.sha256(subject_id.encode("utf-8")).hexdigest()[:20]
        witness_file, witness_hash = writer.add_input(
            f"nonvacuity-{token}-witness",
            "nonvacuity-witness",
            ".json",
            f"nonvacuity:{subject_id}:witness",
            witness_source,
        )
        if sha256_text(entry["witness_sha256"], f"{path}.witness_sha256") != witness_hash:
            fail(f"{path}.witness_sha256 does not match the witness artifact")
        witness = exact(
            strict_json(witness_file, f"{path} witness"),
            {
                "record_kind", "subject_id", "subject_kind", "authoritative_id",
                "authoritative_path", "authoritative_sha256", "mutation_operator", "release_id",
                "source_revision", "source_tree_id", "formal_spec_sha256", "target_id",
                "baseline_sha256", "mutated_sha256", "detected_by", "command",
                "command_sha256", "working_directory", "environment_sha256", "detector_tool_id",
                "detector_tool_binary_sha256", "java_binary_sha256", "exit_code",
                "completed_at", "result", "mutant_output_sha256", "witness_count",
            },
            f"{path} witness",
        )
        expected_witness = {
            "record_kind": "formal-closure-baseline-acceptance",
            "subject_id": subject_id,
            "subject_kind": mutation["subject_kind"],
            "authoritative_id": mutation["authoritative_id"],
            "authoritative_path": mutation["authoritative_path"],
            "authoritative_sha256": mutation["authoritative_sha256"],
            "mutation_operator": mutation["mutation_operator"],
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha256,
            "target_id": target_id,
            "baseline_sha256": mutation["baseline_sha256"],
            "mutated_sha256": mutation["mutant_sha256"],
            "detected_by": mutation["detected_by"],
            "command": mutation["baseline_command"],
            "command_sha256": mutation["baseline_command_sha256"],
            "working_directory": ".",
            "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
            "detector_tool_id": mutation["detector_tool_id"],
            "detector_tool_binary_sha256": mutation["detector_tool_binary_sha256"],
            "java_binary_sha256": mutation["java_binary_sha256"],
            "exit_code": 0,
            "completed_at": witness["completed_at"],
            "result": "accepted",
            "mutant_output_sha256": mutation["detector_sha256"],
            "witness_count": witness_count,
        }
        witness_completed_at = utc(witness["completed_at"], f"{path} witness.completed_at")
        ensure_fresh(witness_completed_at, generated_at, f"{path} witness.completed_at")
        detector_completed_at = utc(mutation["detector_completed_at"], f"{path} detector.completed_at")
        if (
            witness_completed_at > detector_completed_at
            or detector_completed_at > campaign_completed_at
            or entry["result"] != "pass"
            or witness != expected_witness
        ):
            fail(f"{path} lacks runner-bound baseline acceptance before mutant rejection")
        witnesses_by_subject[subject_id] = {
            "witness_artifact_id": witness_file.stem,
            "witness_sha256": witness_hash,
            "witness_count": witness_count,
            "witness_observed_at": witness["completed_at"],
            "baseline_completed_at": witness["completed_at"],
            "baseline_result": "accepted",
            "nonvacuity_result": "pass",
        }
    if set(witnesses_by_subject) != expected_subjects or [
        entry["subject_id"] for entry in nonvacuity_raw
    ] != sorted(expected_subjects):
        fail("nonvacuity evidence must be sorted and cover every theorem, config, and property")
    return {
        "executed_at": top["executed_at"],
        "result": "pass",
        "subjects": [
            {**mutations_by_subject[subject_id], **witnesses_by_subject[subject_id]}
            for subject_id in sorted(expected_subjects)
        ],
    }


@contextmanager
def git_replacements_disabled() -> Iterable[None]:
    previous = os.environ.get("GIT_NO_REPLACE_OBJECTS")
    os.environ["GIT_NO_REPLACE_OBJECTS"] = "1"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("GIT_NO_REPLACE_OBJECTS", None)
        else:
            os.environ["GIT_NO_REPLACE_OBJECTS"] = previous


def git_tree_id(repository: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository), "rev-parse", "HEAD^{tree}"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot resolve target source tree: {exc}")
    if completed.returncode != 0:
        fail(f"cannot resolve target source tree: {completed.stderr.strip()}")
    return revision(completed.stdout.strip(), "target repository tree")


def require_head_blob_match(
    repository: Path,
    relative: str,
    worktree_path: Path,
    snapshot_sha256: str,
    snapshot_size: int,
    context: str,
) -> None:
    try:
        blob = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository), "cat-file", "blob", f"HEAD:{relative}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        tree = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repository), "ls-tree", "-z", "HEAD", "--", relative],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        fail(f"cannot compare {context} with the bound Git tree: {exc}")
    if blob.returncode != 0 or tree.returncode != 0:
        fail(f"{context} is absent from the bound Git tree")
    if (
        len(blob.stdout) != snapshot_size
        or hashlib.sha256(blob.stdout).hexdigest() != snapshot_sha256
    ):
        fail(f"{context} bytes do not match the bound Git blob")
    try:
        metadata, listed_path = tree.stdout.rstrip(b"\0").split(b"\t", 1)
        mode, object_type, _ = metadata.decode("ascii").split(" ", 2)
        listed = listed_path.decode("utf-8")
    except (ValueError, UnicodeError) as exc:
        fail(f"cannot parse {context} Git tree entry: {exc}")
    if listed != relative or object_type != "blob" or mode not in {"100644", "100755"}:
        fail(f"{context} has an unsupported Git tree entry")
    executable = bool(worktree_path.stat().st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))
    if executable != (mode == "100755"):
        fail(f"{context} executable mode does not match the bound Git blob")


def collect(
    *,
    manifest_path: Path,
    repository_root: Path,
    output_dir: Path,
    producer_private_key: Path,
    producer_public_key: Path,
    reviewer_private_key: Path,
    reviewer_public_key: Path,
    expected_target_id: str,
    expected_environment_profile: str,
    allow_synthetic_test_fixture: bool,
    now: datetime | None = None,
) -> Path:
    if platform.system() != "Darwin":
        fail("formal closure collection is supported only on native Darwin")
    repository = repository_root.resolve()
    if not repository.is_dir():
        fail("repository root must be a directory")
    manifest_resolved = manifest_path.resolve()
    bindings = InputBindings()
    manifest_bytes, _ = bindings.snapshot("collection-manifest", manifest_resolved)
    manifest_base = manifest_resolved.parent
    manifest = exact(
        strict_json_bytes(manifest_bytes, "collection manifest"),
        {
            "schema_version",
            "record_mode",
            "release",
            "generated_at",
            "valid_until",
            "toolchain",
            "models",
            "configs",
            "properties",
            "finite_model_checks",
            "inductive_proofs",
            "trace_refinement",
            "joint_consensus",
            "toq_clock_discipline",
            "mutation_nonvacuity_path",
            "sign_off",
            "producer",
        },
        "$",
    )
    if manifest["schema_version"] != COLLECTION_SCHEMA_VERSION:
        fail("$.schema_version is unsupported")
    mode = string(manifest["record_mode"], "$.record_mode")
    if mode == verifier.SYNTHETIC_MODE:
        if not allow_synthetic_test_fixture:
            fail("synthetic test collection requires --allow-synthetic-test-fixture")
    elif mode == verifier.TARGET_MODE:
        if allow_synthetic_test_fixture:
            fail("test-only collection options may not be used for a target record")
    else:
        fail("$.record_mode must be 'target' or 'synthetic-test'")

    release_raw = exact(
        manifest["release"],
        {
            "release_id",
            "source_repository",
            "source_revision",
            "source_tree_id",
            "formal_spec_sha256",
            "formal_spec_model_id",
            "target_id",
        },
        "$.release",
    )
    release_id = identifier(release_raw["release_id"], "$.release.release_id")
    source_repository = string(release_raw["source_repository"], "$.release.source_repository")
    try:
        verifier.remote_uri(source_repository, "$.release.source_repository")
    except verifier.EvidenceError as exc:
        fail(str(exc))
    source_revision = revision(release_raw["source_revision"], "$.release.source_revision")
    source_tree_id = revision(release_raw["source_tree_id"], "$.release.source_tree_id")
    formal_spec_sha = sha256_text(release_raw["formal_spec_sha256"], "$.release.formal_spec_sha256")
    root_model_id = identifier(release_raw["formal_spec_model_id"], "$.release.formal_spec_model_id")
    target_id = identifier(release_raw["target_id"], "$.release.target_id")
    trusted_target_id = identifier(expected_target_id, "expected target id")
    if target_id != trusted_target_id:
        fail("$.release.target_id does not match --expected-target-id")
    trusted_environment = identifier(
        expected_environment_profile,
        "expected environment profile",
    )
    required_environment = (
        "production" if mode == verifier.TARGET_MODE else verifier.SYNTHETIC_MODE
    )
    if trusted_environment != required_environment:
        fail("--expected-environment-profile does not match the requested record mode")

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    generated_at = utc(manifest["generated_at"], "$.generated_at")
    valid_until = utc(manifest["valid_until"], "$.valid_until")
    if generated_at > current + verifier.FUTURE_SKEW or current - generated_at > verifier.MAX_EVIDENCE_AGE:
        fail("$.generated_at is future-dated or stale")
    if valid_until <= current or valid_until <= generated_at or valid_until - generated_at > verifier.MAX_VALIDITY:
        fail("$.valid_until is expired or outside the 30-day validity window")
    generated_text = manifest["generated_at"]

    if mode == verifier.TARGET_MODE:
        try:
            with git_replacements_disabled():
                verifier.verify_repository_revision(repository, source_revision)
        except verifier.EvidenceError as exc:
            fail(str(exc))
        if git_tree_id(repository) != source_tree_id:
            fail("$.release.source_tree_id does not match the exact target repository tree")

    output = output_dir.resolve()
    if output.exists():
        fail("output directory already exists; refusing to mix or overwrite evidence")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent))
    try:

        expected_producer_public = (repository / verifier.PRODUCER_TRUST_ROOT_PATH).resolve()
        expected_reviewer_public = (repository / verifier.REVIEWER_TRUST_ROOT_PATH).resolve()
        if producer_public_key.resolve() != expected_producer_public:
            fail("producer public key path is not the verifier-pinned producer trust-root path")
        if reviewer_public_key.resolve() != expected_reviewer_public:
            fail("reviewer public key path is not the verifier-pinned reviewer trust-root path")
        if producer_private_key.resolve() == reviewer_private_key.resolve():
            fail("producer and reviewer private-key paths must be distinct")
        producer_public_bytes, producer_public_hash = bindings.snapshot(
            "producer-pinned-public-key",
            expected_producer_public,
        )
        reviewer_public_bytes, reviewer_public_hash = bindings.snapshot(
            "reviewer-pinned-public-key",
            expected_reviewer_public,
        )
        producer_public_snapshot = stage / ".producer-public.snapshot.pem"
        reviewer_public_snapshot = stage / ".reviewer-public.snapshot.pem"
        producer_public_snapshot.write_bytes(producer_public_bytes)
        reviewer_public_snapshot.write_bytes(reviewer_public_bytes)
        os.chmod(producer_public_snapshot, 0o444)
        os.chmod(reviewer_public_snapshot, 0o444)
        if mode == verifier.TARGET_MODE:
            require_head_blob_match(
                repository,
                verifier.PRODUCER_TRUST_ROOT_PATH,
                expected_producer_public,
                producer_public_hash,
                len(producer_public_bytes),
                "producer pinned public key",
            )
            require_head_blob_match(
                repository,
                verifier.REVIEWER_TRUST_ROOT_PATH,
                expected_reviewer_public,
                reviewer_public_hash,
                len(reviewer_public_bytes),
                "reviewer pinned public key",
            )
        producer_der = validate_key_pair(
            producer_private_key.resolve(),
            producer_public_snapshot,
            "producer",
        )
        reviewer_der = validate_key_pair(
            reviewer_private_key.resolve(),
            reviewer_public_snapshot,
            "reviewer",
        )
        if producer_der == reviewer_der:
            fail("producer and reviewer signing identities must be distinct")
        if producer_public_hash == reviewer_public_hash:
            fail("producer and reviewer trust roots must be distinct")

        producer_raw = exact(
            manifest["producer"],
            {
                "signer_identity",
                "decided_at",
                "signed_at",
                "native_execution_record_path",
                "native_execution_signature_path",
            },
            "$.producer",
        )
        producer_identity = verifier.authenticated_identity(
            producer_raw["signer_identity"],
            "$.producer.signer_identity",
        )
        producer_decided_at = utc(producer_raw["decided_at"], "$.producer.decided_at")
        signed_at = utc(producer_raw["signed_at"], "$.producer.signed_at")
        if signed_at != generated_at or producer_decided_at > signed_at:
            fail("$.producer decision/signature chronology is inconsistent")

        writer = ArtifactWriter(stage, source_revision, generated_text, bindings)
        _, collection_manifest_hash = writer.add_generated(
            "collection-manifest",
            "collection-manifest",
            ".json",
            manifest_bytes,
        )

        models: list[dict[str, Any]] = []
        model_ids: set[str] = set()
        for index, raw_model in enumerate(items(manifest["models"], "$.models", 1)):
            path = f"$.models[{index}]"
            model = exact(raw_model, {"model_id", "path", "role"}, path)
            model_id = identifier(model["model_id"], f"{path}.model_id")
            if model_id in model_ids:
                fail(f"duplicate model id {model_id!r}")
            relative, source = relative_repository_file(repository, model["path"], f"{path}.path", ".tla")
            digest, model_size = bindings.add(f"model:{model_id}", source)
            if mode == verifier.TARGET_MODE:
                require_head_blob_match(
                    repository,
                    relative,
                    source,
                    digest,
                    model_size,
                    f"model {model_id!r}",
                )
            role = string(model["role"], f"{path}.role")
            if role not in {"claim-root-shaped-spec", "supporting"}:
                fail(f"{path}.role is unsupported")
            models.append(
                {"model_id": model_id, "path": relative, "sha256": digest, "role": role, "source_revision": source_revision}
            )
            model_ids.add(model_id)
        if [entry["model_id"] for entry in models] != sorted(model_ids):
            fail("$.models must be sorted by model_id")
        roots = [entry for entry in models if entry["role"] == "claim-root-shaped-spec"]
        if len(roots) != 1 or roots[0]["model_id"] != root_model_id or roots[0]["sha256"] != formal_spec_sha:
            fail("models do not bind exactly one declared claim-root formal specification")

        configs: list[dict[str, Any]] = []
        config_ids: set[str] = set()
        for index, raw_config in enumerate(items(manifest["configs"], "$.configs", 1)):
            path = f"$.configs[{index}]"
            config = exact(raw_config, {"config_id", "path", "model_id"}, path)
            config_id = identifier(config["config_id"], f"{path}.config_id")
            if config_id in config_ids:
                fail(f"duplicate config id {config_id!r}")
            model_id = identifier(config["model_id"], f"{path}.model_id")
            if model_id not in model_ids:
                fail(f"{path}.model_id references an unknown model")
            relative, source = relative_repository_file(repository, config["path"], f"{path}.path", ".cfg")
            digest, config_size = bindings.add(f"config:{config_id}", source)
            if mode == verifier.TARGET_MODE:
                require_head_blob_match(
                    repository,
                    relative,
                    source,
                    digest,
                    config_size,
                    f"config {config_id!r}",
                )
            configs.append(
                {
                    "config_id": config_id,
                    "path": relative,
                    "sha256": digest,
                    "model_id": model_id,
                    "source_revision": source_revision,
                }
            )
            config_ids.add(config_id)
        if [entry["config_id"] for entry in configs] != sorted(config_ids):
            fail("$.configs must be sorted by config_id")
        config_by_id = {entry["config_id"]: entry for entry in configs}
        model_by_id = {entry["model_id"]: entry for entry in models}

        properties = validate_properties(manifest["properties"], model_ids)
        invariant_ids = {entry["property_id"] for entry in properties["invariants"]}
        temporal_ids = {entry["property_id"] for entry in properties["temporal_properties"]}

        toolchain: list[dict[str, Any]] = []
        tool_ids: set[str] = set()
        tool_hashes: dict[str, str] = {}
        tool_versions: dict[str, str] = {}
        tool_kind = {
            "go": "go-binary",
            "java": "java-binary",
            "tlc": "tlc-binary",
            "tlaps": "tlaps-binary",
            "trace-replay": "trace-replay-binary",
        }
        for index, raw_tool in enumerate(
            items(manifest["toolchain"], "$.toolchain", len(verifier.TOOL_IDS))
        ):
            path = f"$.toolchain[{index}]"
            tool = exact(raw_tool, {"tool_id", "version", "repository_pin_path", "binary_path"}, path)
            tool_id = identifier(tool["tool_id"], f"{path}.tool_id")
            if tool_id not in verifier.TOOL_IDS or tool_id in tool_ids:
                fail(f"{path}.tool_id must uniquely identify a required pinned formal-evidence tool")
            version = string(tool["version"], f"{path}.version", 128)
            pin_relative, pin_path = relative_repository_file(
                repository,
                tool["repository_pin_path"],
                f"{path}.repository_pin_path",
                ".json",
            )
            pin_bytes, pin_hash = bindings.snapshot(f"tool-pin:{tool_id}", pin_path)
            if mode == verifier.TARGET_MODE:
                require_head_blob_match(
                    repository,
                    pin_relative,
                    pin_path,
                    pin_hash,
                    len(pin_bytes),
                    f"tool pin {tool_id!r}",
                )
            binary_source = resolve_input(manifest_base, tool["binary_path"], f"{path}.binary_path")
            _, binary_hash = writer.add_input(
                f"{tool_id}-binary",
                tool_kind[tool_id],
                ".bin",
                f"tool-binary:{tool_id}",
                binary_source,
            )
            pin = exact(
                strict_json_bytes(pin_bytes, f"{tool_id} tool pin"),
                {"tool_id", "version", "binary_sha256"},
                f"{tool_id} tool pin",
            )
            if pin != {"tool_id": tool_id, "version": version, "binary_sha256": binary_hash}:
                fail(f"{tool_id} tool pin does not match the supplied identity and binary")
            toolchain.append(
                {
                    "tool_id": tool_id,
                    "version": version,
                    "repository_pin_path": pin_relative,
                    "repository_pin_sha256": pin_hash,
                    "binary_sha256": binary_hash,
                    "binary_artifact_id": f"{tool_id}-binary",
                    "source_revision": source_revision,
                }
            )
            tool_ids.add(tool_id)
            tool_hashes[tool_id] = binary_hash
            tool_versions[tool_id] = version
        if len(toolchain) != len(verifier.TOOL_IDS) or tuple(sorted(tool_ids)) != verifier.TOOL_IDS:
            fail("$.toolchain does not contain the exact required pinned tool set")
        toolchain.sort(key=lambda entry: entry["tool_id"])

        mutation_path = resolve_input(
            manifest_base,
            manifest["mutation_nonvacuity_path"],
            "$.mutation_nonvacuity_path",
        )
        mutation_file, mutation_hash = writer.add_input(
            "mutation-manifest",
            "mutation-manifest",
            ".json",
            "mutation-nonvacuity",
            mutation_path,
        )
        mutation_evidence = validate_mutation_record(
            strict_json(mutation_file, "mutation/nonvacuity evidence"),
            record_base=mutation_path.parent,
            repository_root=repository,
            writer=writer,
            record_mode=mode,
            release_id=release_id,
            source_revision=source_revision,
            source_tree_id=source_tree_id,
            formal_spec_sha256=formal_spec_sha,
            target_id=target_id,
            models=models,
            configs=configs,
            properties=properties,
            tool_hashes=tool_hashes,
            generated_at=generated_at,
        )
        mutation_evidence.update(
            {
                "record_artifact_id": "mutation-manifest",
                "record_sha256": mutation_hash,
            }
        )
        latest_observation_at = utc(
            mutation_evidence["executed_at"],
            "mutation evidence.executed_at",
        )

        finite_checks: list[dict[str, Any]] = []
        seen_configs: set[str] = set()
        covered_areas: list[str] = []
        covered_invariants: set[str] = set()
        covered_temporal: set[str] = set()
        finite_raw = items(manifest["finite_model_checks"], "$.finite_model_checks", 1)
        for index, raw_check in enumerate(finite_raw):
            path = f"$.finite_model_checks[{index}]"
            check = exact(raw_check, {"config_id", "command", "log_path"}, path)
            config_id = identifier(check["config_id"], f"{path}.config_id")
            if config_id not in config_by_id or config_id in seen_configs:
                fail(f"{path}.config_id must uniquely cover a manifest config")
            command = [string(arg, f"{path}.command[{arg_index}]") for arg_index, arg in enumerate(items(check["command"], f"{path}.command", 1))]
            expected_command = [
                "java",
                "-jar",
                "tla2tools.jar",
                "-config",
                config_by_id[config_id]["path"],
                model_by_id[config_by_id[config_id]["model_id"]]["path"],
            ]
            if command != expected_command:
                fail(f"{path}.command must be the exact pinned TLC invocation for its model and config")
            source = resolve_input(manifest_base, check["log_path"], f"{path}.log_path")
            artifact_id = f"tlc-{config_id}"
            copied, _ = writer.add_input(artifact_id, "tlc-raw-log", ".log", f"tlc-log:{config_id}", source)
            marker = marker_exact(
                copied,
                "FORMAL_CLOSURE_TLC_RESULT ",
                {
                    "area_ids",
                    "checked_invariant_ids",
                    "checked_temporal_ids",
                    "completed_at",
                    "config_id",
                    "config_sha256",
                    "distinct_states",
                    "errors",
                    "generated_states",
                    "model_sha256",
                    "queue_states",
                    "result",
                    "search_depth",
                    "source_revision",
                    "started_at",
                    "tool_binary_sha256",
                    "tool_version",
                },
                f"{path} TLC log",
            )
            areas = exact_strings(marker["area_ids"], f"{path} TLC result.area_ids", 1)
            checked_invariant_ids = exact_strings(
                marker["checked_invariant_ids"],
                f"{path} TLC result.checked_invariant_ids",
                1,
            )
            checked_temporal_ids = exact_strings(
                marker["checked_temporal_ids"],
                f"{path} TLC result.checked_temporal_ids",
                1,
            )
            if set(areas) - set(verifier.FINITE_AREA_IDS):
                fail(f"{path} TLC result contains unknown finite areas")
            if set(checked_invariant_ids) - invariant_ids or set(checked_temporal_ids) - temporal_ids:
                fail(f"{path} TLC result claims unmanifested properties")
            checked_model_id = config_by_id[config_id]["model_id"]
            property_by_id = {
                entry["property_id"]: entry
                for entry in (*properties["invariants"], *properties["temporal_properties"])
            }
            mismatched_properties = sorted(
                property_id
                for property_id in (*checked_invariant_ids, *checked_temporal_ids)
                if property_by_id[property_id]["module_id"] != checked_model_id
            )
            if mismatched_properties:
                fail(f"{path} property module does not match the model loaded by the checked config")
            generated_states = integer(marker["generated_states"], f"{path} TLC result.generated_states", 1)
            distinct_states = integer(marker["distinct_states"], f"{path} TLC result.distinct_states", 1)
            search_depth = integer(marker["search_depth"], f"{path} TLC result.search_depth", 1)
            expected_marker = {
                "area_ids": areas,
                "checked_invariant_ids": checked_invariant_ids,
                "checked_temporal_ids": checked_temporal_ids,
                "completed_at": marker["completed_at"],
                "config_id": config_id,
                "config_sha256": config_by_id[config_id]["sha256"],
                "distinct_states": distinct_states,
                "errors": 0,
                "generated_states": generated_states,
                "model_sha256": model_by_id[config_by_id[config_id]["model_id"]]["sha256"],
                "queue_states": 0,
                "result": "pass",
                "search_depth": search_depth,
                "source_revision": source_revision,
                "started_at": marker["started_at"],
                "tool_binary_sha256": tool_hashes["tlc"],
                "tool_version": tool_versions["tlc"],
            }
            if marker != expected_marker or distinct_states > generated_states:
                fail(f"{path} TLC semantic result does not match the pinned run")
            started_at = utc(marker["started_at"], f"{path} TLC result.started_at")
            completed_at = utc(marker["completed_at"], f"{path} TLC result.completed_at")
            if started_at > completed_at:
                fail(f"{path} TLC result completion precedes its start")
            ensure_fresh(completed_at, generated_at, f"{path} TLC result.completed_at")
            latest_observation_at = max(latest_observation_at, completed_at)
            command_binding = marker_exact(
                copied,
                COMMAND_BINDING_MARKER,
                {
                    "release_id",
                    "source_revision",
                    "source_tree_id",
                    "target_id",
                    "config_id",
                    "config_sha256",
                    "model_sha256",
                    "command_sha256",
                    "java_binary_sha256",
                    "tlc_binary_sha256",
                    "working_directory",
                    "environment_sha256",
                    "exit_code",
                    "completed_at",
                    "result",
                },
                f"{path} TLC command binding",
            )
            if command_binding != {
                "release_id": release_id,
                "source_revision": source_revision,
                "source_tree_id": source_tree_id,
                "target_id": target_id,
                "config_id": config_id,
                "config_sha256": config_by_id[config_id]["sha256"],
                "model_sha256": model_by_id[config_by_id[config_id]["model_id"]]["sha256"],
                "command_sha256": hashlib.sha256(canonical_bytes(command)).hexdigest(),
                "java_binary_sha256": tool_hashes["java"],
                "tlc_binary_sha256": tool_hashes["tlc"],
                "working_directory": ".",
                "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
                "exit_code": 0,
                "completed_at": marker["completed_at"],
                "result": "pass",
            }:
                fail(f"{path} TLC result does not bind the exact command and pinned executables")
            try:
                verifier.require_log_text(
                    copied,
                    (
                        "Model checking completed. No error has been found.",
                        f"{generated_states} states generated, {distinct_states} distinct states found, 0 states left on queue.",
                        f"The depth of the complete state graph search is {search_depth}.",
                    ),
                    f"{path} TLC native log",
                )
            except verifier.EvidenceError as exc:
                fail(str(exc))
            finite_checks.append(
                {
                    "config_id": config_id,
                    "area_ids": areas,
                    "checked_invariant_ids": checked_invariant_ids,
                    "checked_temporal_ids": checked_temporal_ids,
                    "command": command,
                    "command_sha256": hashlib.sha256(canonical_bytes(command)).hexdigest(),
                    "java_binary_sha256": tool_hashes["java"],
                    "working_directory": ".",
                    "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
                    "exit_code": 0,
                    "tool_id": "tlc",
                    "tool_version": tool_versions["tlc"],
                    "tool_binary_sha256": tool_hashes["tlc"],
                    "generated_states": generated_states,
                    "distinct_states": distinct_states,
                    "search_depth": search_depth,
                    "queue_states": 0,
                    "errors": 0,
                    "deadlock_result": "pass",
                    "result": "pass",
                    "started_at": marker["started_at"],
                    "completed_at": marker["completed_at"],
                    "tlc_log_artifact_id": artifact_id,
                }
            )
            seen_configs.add(config_id)
            covered_areas.extend(areas)
            covered_invariants.update(checked_invariant_ids)
            covered_temporal.update(checked_temporal_ids)
        if seen_configs != config_ids:
            fail("finite checks do not cover every config exactly once")
        if tuple(sorted(covered_areas)) != verifier.FINITE_AREA_IDS or len(covered_areas) != len(set(covered_areas)):
            fail("finite checks do not cover every required area exactly once")
        if covered_invariants != invariant_ids or covered_temporal != temporal_ids:
            fail("finite checks do not cover every required property")
        finite_checks.sort(key=lambda entry: entry["config_id"])

        proofs: list[dict[str, Any]] = []
        proof_classes: set[str] = set()
        for index, raw_proof in enumerate(items(manifest["inductive_proofs"], "$.inductive_proofs", 5)):
            path = f"$.inductive_proofs[{index}]"
            proof = exact(raw_proof, {"theorem_class", "module_id", "command", "log_path"}, path)
            theorem_class = identifier(proof["theorem_class"], f"{path}.theorem_class")
            if theorem_class not in verifier.THEOREM_BY_CLASS or theorem_class in proof_classes:
                fail(f"{path}.theorem_class must uniquely cover a required theorem class")
            module_id = identifier(proof["module_id"], f"{path}.module_id")
            if module_id not in model_by_id:
                fail(f"{path}.module_id references an unknown model")
            proof_command = [
                string(arg, f"{path}.command[{arg_index}]")
                for arg_index, arg in enumerate(items(proof["command"], f"{path}.command", 1))
            ]
            theorem_id = verifier.THEOREM_BY_CLASS[theorem_class]
            expected_proof_command = ["tlapm", model_by_id[module_id]["path"], "--theorem", theorem_id]
            if proof_command != expected_proof_command:
                fail(f"{path}.command must be the exact pinned TLAPS invocation for the theorem and module")
            source = resolve_input(manifest_base, proof["log_path"], f"{path}.log_path")
            artifact_id = f"tlaps-{theorem_class}"
            copied, _ = writer.add_input(
                artifact_id, "tlaps-raw-log", ".log", f"tlaps-log:{theorem_class}", source
            )
            marker = marker_exact(
                copied,
                "FORMAL_CLOSURE_TLAPS_RESULT ",
                {
                    "admitted_obligations",
                    "assumptions",
                    "checked_at",
                    "history_scope",
                    "module_sha256",
                    "obligations_proved",
                    "obligations_total",
                    "omitted_obligations",
                    "result",
                    "source_revision",
                    "status",
                    "theorem_class",
                    "theorem_id",
                    "tool_binary_sha256",
                    "tool_version",
                },
                f"{path} TLAPS log",
            )
            assumptions = exact_strings(marker["assumptions"], f"{path} TLAPS result.assumptions")
            expected_assumptions = list(verifier.FAIR_LOSS_ASSUMPTIONS) if theorem_class == "fair-loss-liveness" else []
            total = integer(marker["obligations_total"], f"{path} TLAPS result.obligations_total", 1)
            proved = integer(marker["obligations_proved"], f"{path} TLAPS result.obligations_proved", 1)
            expected_marker = {
                "admitted_obligations": 0,
                "assumptions": expected_assumptions,
                "checked_at": marker["checked_at"],
                "history_scope": "arbitrary-history",
                "module_sha256": model_by_id[module_id]["sha256"],
                "obligations_proved": total,
                "obligations_total": total,
                "omitted_obligations": 0,
                "result": "pass",
                "source_revision": source_revision,
                "status": "checked",
                "theorem_class": theorem_class,
                "theorem_id": theorem_id,
                "tool_binary_sha256": tool_hashes["tlaps"],
                "tool_version": tool_versions["tlaps"],
            }
            if marker != expected_marker or proved != total or assumptions != expected_assumptions:
                fail(f"{path} TLAPS semantic result does not prove the required theorem")
            checked_at = utc(marker["checked_at"], f"{path} TLAPS result.checked_at")
            ensure_fresh(checked_at, generated_at, f"{path} TLAPS result.checked_at")
            latest_observation_at = max(latest_observation_at, checked_at)
            proof_command_binding = marker_exact(
                copied,
                COMMAND_BINDING_MARKER,
                {
                    "release_id",
                    "source_revision",
                    "source_tree_id",
                    "target_id",
                    "theorem_class",
                    "theorem_id",
                    "module_sha256",
                    "command_sha256",
                    "tlaps_binary_sha256",
                    "working_directory",
                    "environment_sha256",
                    "exit_code",
                    "completed_at",
                    "result",
                },
                f"{path} TLAPS command binding",
            )
            if proof_command_binding != {
                "release_id": release_id,
                "source_revision": source_revision,
                "source_tree_id": source_tree_id,
                "target_id": target_id,
                "theorem_class": theorem_class,
                "theorem_id": theorem_id,
                "module_sha256": model_by_id[module_id]["sha256"],
                "command_sha256": hashlib.sha256(canonical_bytes(proof_command)).hexdigest(),
                "tlaps_binary_sha256": tool_hashes["tlaps"],
                "working_directory": ".",
                "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
                "exit_code": 0,
                "completed_at": marker["checked_at"],
                "result": "pass",
            }:
                fail(f"{path} TLAPS result does not bind the exact command and pinned executable")
            try:
                native_lines = copied.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError) as exc:
                fail(f"cannot inspect {path} TLAPS native obligations: {exc}")
            obligation_lines = {
                line
                for line in native_lines
                if line.startswith("Obligation ") and line.endswith(" proved.")
            }
            if len(obligation_lines) != total:
                fail(f"{path} TLAPS native log does not contain every distinct proved obligation")
            try:
                verifier.require_log_text(copied, (theorem_id, "All obligations proved."), f"{path} TLAPS native log")
            except verifier.EvidenceError as exc:
                fail(str(exc))
            proofs.append(
                {
                    "theorem_id": theorem_id,
                    "theorem_class": theorem_class,
                    "module_id": module_id,
                    "command": proof_command,
                    "command_sha256": hashlib.sha256(canonical_bytes(proof_command)).hexdigest(),
                    "working_directory": ".",
                    "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
                    "exit_code": 0,
                    "status": "checked",
                    "result": "pass",
                    "inductive": True,
                    "history_scope": "arbitrary-history",
                    "assumptions": assumptions,
                    "obligations_total": total,
                    "obligations_proved": proved,
                    "admitted_obligations": 0,
                    "omitted_obligations": 0,
                    "tool_id": "tlaps",
                    "tool_version": tool_versions["tlaps"],
                    "tool_binary_sha256": tool_hashes["tlaps"],
                    "checked_at": marker["checked_at"],
                    "tlaps_log_artifact_id": artifact_id,
                }
            )
            proof_classes.add(theorem_class)
        if proof_classes != set(verifier.THEOREM_BY_CLASS) or len(proofs) != len(verifier.THEOREM_BY_CLASS):
            fail("inductive proofs do not cover exactly the five required theorem classes")
        proofs.sort(key=lambda entry: entry["theorem_class"])

        trace_raw = exact(
            manifest["trace_refinement"],
            {
                "exporter",
                "format",
                "shaped_spec_model_id",
                "export_command",
                "replay_command",
                "trace_path",
                "replay_log_path",
            },
            "$.trace_refinement",
        )
        if trace_raw["exporter"] != "go-to-shaped-spec-v1" or trace_raw["format"] != "moreconsensus-shaped-spec-trace-v1":
            fail("$.trace_refinement format identity is unsupported")
        shaped_model_id = identifier(trace_raw["shaped_spec_model_id"], "$.trace_refinement.shaped_spec_model_id")
        if shaped_model_id != root_model_id:
            fail("$.trace_refinement.shaped_spec_model_id must be the claim-root model")
        export_command = [string(arg, "$.trace_refinement.export_command item") for arg in items(trace_raw["export_command"], "$.trace_refinement.export_command", 1)]
        replay_command = [string(arg, "$.trace_refinement.replay_command item") for arg in items(trace_raw["replay_command"], "$.trace_refinement.replay_command", 1)]
        if export_command != ["go", "test", "./traceexport", "-run", "TestExport"]:
            fail("$.trace_refinement.export_command must be the exact pinned Go exporter invocation")
        if replay_command != ["java", "-jar", "trace-replay.jar", "go-shaped.trace.json"]:
            fail("$.trace_refinement.replay_command must be the exact pinned replay invocation")
        trace_source = resolve_input(manifest_base, trace_raw["trace_path"], "$.trace_refinement.trace_path")
        trace_file, trace_hash = writer.add_input("trace-export", "trace-export", ".json", "trace-export", trace_source)
        trace_document = strict_json(trace_file, "trace export")
        if not isinstance(trace_document, dict) or not isinstance(trace_document.get("traces"), list):
            fail("trace export lacks structured traces")
        trace_count = len(trace_document["traces"])
        export_command_hash = hashlib.sha256(canonical_bytes(export_command)).hexdigest()
        replay_command_hash = hashlib.sha256(canonical_bytes(replay_command)).hexdigest()
        exported_at = string(trace_document.get("exported_at"), "trace export.exported_at")
        try:
            verifier.validate_trace_export(
                trace_file,
                release_id=release_id,
                source_revision=source_revision,
                source_tree_id=source_tree_id,
                target_id=target_id,
                formal_spec_sha256=formal_spec_sha,
                exported_at=exported_at,
                export_command_sha256=export_command_hash,
                go_binary_sha256=tool_hashes["go"],
                trace_count=trace_count,
            )
        except verifier.EvidenceError as exc:
            fail(str(exc))
        export_time = utc(exported_at, "trace export.exported_at")
        ensure_fresh(export_time, generated_at, "trace export.exported_at")
        replay_source = resolve_input(
            manifest_base,
            trace_raw["replay_log_path"],
            "$.trace_refinement.replay_log_path",
        )
        replay_file, replay_hash = writer.add_input(
            "trace-replay",
            "trace-replay",
            ".log",
            "trace-replay",
            replay_source,
        )
        replay_marker = marker_exact(
            replay_file,
            "FORMAL_CLOSURE_TRACE_REPLAY ",
            {
                "deterministic",
                "release_id",
                "source_revision",
                "source_tree_id",
                "target_id",
                "formal_spec_sha256",
                "trace_count",
                "trace_sha256",
                "exported_at",
                "replayed_at",
                "export_command_sha256",
                "replay_command_sha256",
                "java_binary_sha256",
                "go_binary_sha256",
                "trace_replay_binary_sha256",
                "working_directory",
                "environment_sha256",
                "export_exit_code",
                "replay_exit_code",
                "result",
            },
            "trace replay log",
        )
        expected_replay = {
            "deterministic": True,
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "target_id": target_id,
            "formal_spec_sha256": formal_spec_sha,
            "trace_count": trace_count,
            "trace_sha256": trace_hash,
            "exported_at": exported_at,
            "replayed_at": replay_marker["replayed_at"],
            "export_command_sha256": export_command_hash,
            "replay_command_sha256": replay_command_hash,
            "java_binary_sha256": tool_hashes["java"],
            "go_binary_sha256": tool_hashes["go"],
            "trace_replay_binary_sha256": tool_hashes["trace-replay"],
            "working_directory": ".",
            "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
            "export_exit_code": 0,
            "replay_exit_code": 0,
            "result": "pass",
        }
        replay_completed_at = utc(replay_marker["replayed_at"], "trace replay result.replayed_at")
        ensure_fresh(replay_completed_at, generated_at, "trace replay result.replayed_at")
        if export_time > replay_completed_at or replay_marker != expected_replay:
            fail("trace replay does not bind the exact target, commands, tools, trace, and chronology")
        latest_observation_at = max(latest_observation_at, replay_completed_at)
        common_target = {
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "target_id": target_id,
            "formal_spec_sha256": formal_spec_sha,
            "result": "pass",
        }
        joint_raw = exact(
            manifest["joint_consensus"],
            {"declared_scope", "proof_theorem_id", "scope_log_path"},
            "$.joint_consensus",
        )
        scope_source = resolve_input(
            manifest_base,
            joint_raw["scope_log_path"],
            "$.joint_consensus.scope_log_path",
        )
        scope_file, scope_hash = writer.add_input(
            "scope-enforcement",
            "scope-enforcement",
            ".log",
            "scope-enforcement-source",
            scope_source,
        )
        scope_marker = marker_exact(
            scope_file,
            "FORMAL_CLOSURE_SCOPE_RESULT ",
            {*common_target, "collected_at", "status"},
            "joint-consensus scope log",
        )
        scope_collected_at = utc(scope_marker["collected_at"], "joint-consensus scope result.collected_at")
        ensure_fresh(scope_collected_at, generated_at, "joint-consensus scope result.collected_at")
        latest_observation_at = max(latest_observation_at, scope_collected_at)
        if scope_marker != {
            **common_target,
            "collected_at": scope_marker["collected_at"],
            "status": scope_marker["status"],
        }:
            fail("joint-consensus scope semantic result does not match the exact target")
        joint_status = string(scope_marker["status"], "joint-consensus scope result.status")
        proof_theorem_id = joint_raw["proof_theorem_id"]
        declared_scope = string(joint_raw["declared_scope"], "$.joint_consensus.declared_scope")
        if joint_status == "implemented-and-proved":
            if (
                proof_theorem_id != verifier.THEOREM_BY_CLASS["configuration-induction"]
                or declared_scope != IMPLEMENTED_JOINT_SCOPE
            ):
                fail("implemented joint consensus lacks its exact proof and canonical affirmative scope")
        elif joint_status == "excluded-from-declared-scope":
            if proof_theorem_id is not None or declared_scope != EXCLUDED_JOINT_SCOPE:
                fail("excluded joint consensus lacks its canonical affirmative scope")
        else:
            fail("joint-consensus scope status is unsupported")

        toq_raw = exact(
            manifest["toq_clock_discipline"],
            {"clock_source", "clock_log_path", "remote_observation_path"},
            "$.toq_clock_discipline",
        )
        clock_source = string(toq_raw["clock_source"], "$.toq_clock_discipline.clock_source")
        if clock_source != "authenticated monotonic source":
            fail("$.toq_clock_discipline.clock_source is not an admissible authenticated monotonic source")
        remote_observation_input = resolve_input(
            manifest_base,
            toq_raw["remote_observation_path"],
            "$.toq_clock_discipline.remote_observation_path",
        )
        remote_observation_file, remote_observation_hash = writer.add_input(
            "toq-clock-observation",
            "toq-clock-observation",
            ".json",
            "toq-clock-observation",
            remote_observation_input,
        )
        remote_observation = exact(
            strict_json(remote_observation_file, "TOQ clock observation"),
            {
                *common_target,
                "clock_source",
                "maximum_clock_error_ms",
                "observed_at",
                "record_kind",
                "target_environment",
            },
            "TOQ clock observation",
        )
        clock_input = resolve_input(
            manifest_base,
            toq_raw["clock_log_path"],
            "$.toq_clock_discipline.clock_log_path",
        )
        clock_file, clock_hash = writer.add_input(
            "toq-clock-discipline",
            "toq-clock-discipline",
            ".log",
            "toq-clock-discipline",
            clock_input,
        )
        clock_marker = marker_exact(
            clock_file,
            "FORMAL_CLOSURE_TOQ_CLOCK ",
            {
                *common_target,
                "clock_source",
                "collected_at",
                "evidence_uri",
                "maximum_clock_error_ms",
                "remote_evidence_sha256",
                "target_environment",
            },
            "TOQ clock log",
        )
        target_environment = "production" if mode == verifier.TARGET_MODE else verifier.SYNTHETIC_MODE
        remote_hash = sha256_text(
            clock_marker["remote_evidence_sha256"],
            "TOQ clock result.remote_evidence_sha256",
        )
        if remote_hash != remote_observation_hash:
            fail("TOQ clock marker does not hash the supplied observation bytes")
        try:
            evidence_uri = verifier.remote_uri(
                clock_marker["evidence_uri"],
                "TOQ clock result.evidence_uri",
                content_hash=remote_hash,
            )
        except verifier.EvidenceError as exc:
            fail(str(exc))
        collected_at = utc(clock_marker["collected_at"], "TOQ clock result.collected_at")
        ensure_fresh(collected_at, generated_at, "TOQ clock result.collected_at")
        latest_observation_at = max(latest_observation_at, collected_at)
        maximum_clock_error = integer(
            clock_marker["maximum_clock_error_ms"],
            "TOQ clock result.maximum_clock_error_ms",
            1,
        )
        if maximum_clock_error > 1000:
            fail("TOQ clock maximum error exceeds the admissible bound")
        expected_clock = {
            **common_target,
            "clock_source": clock_source,
            "collected_at": clock_marker["collected_at"],
            "evidence_uri": evidence_uri,
            "maximum_clock_error_ms": maximum_clock_error,
            "remote_evidence_sha256": remote_hash,
            "target_environment": target_environment,
        }
        if clock_marker != expected_clock:
            fail("TOQ clock semantic result does not match the release environment")
        if remote_observation != {
            **common_target,
            "record_kind": "formal-closure-toq-clock-observation",
            "clock_source": clock_source,
            "observed_at": clock_marker["collected_at"],
            "maximum_clock_error_ms": maximum_clock_error,
            "target_environment": target_environment,
        }:
            fail("TOQ clock observation does not bind the exact target, source, time, and error bound")
        target = {
            "target_id": target_id,
            "operating_system": "Darwin",
            "architecture": platform.machine(),
            "execution_mode": "native-darwin" if mode == verifier.TARGET_MODE else verifier.SYNTHETIC_MODE,
            "environment": "production" if mode == verifier.TARGET_MODE else verifier.SYNTHETIC_MODE,
            "source_tree_id": source_tree_id,
        }
        specification_manifest = {
            "manifest_sha256": verifier.canonical_manifest_sha256(models, configs),
            "models": models,
            "configs": configs,
        }
        trace_refinement = {
            "source_revision": source_revision,
            "exporter": "go-to-shaped-spec-v1",
            "format": "moreconsensus-shaped-spec-trace-v1",
            "shaped_spec_model_id": shaped_model_id,
            "deterministic": True,
            "trace_count": trace_count,
            "export_command": export_command,
            "export_command_sha256": export_command_hash,
            "replay_command": replay_command,
            "replay_command_sha256": replay_command_hash,
            "java_binary_sha256": tool_hashes["java"],
            "go_binary_sha256": tool_hashes["go"],
            "replay_tool_binary_sha256": tool_hashes["trace-replay"],
            "working_directory": ".",
            "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
            "export_exit_code": 0,
            "replay_exit_code": 0,
            "export_result": "pass",
            "replay_result": "pass",
            "trace_artifact_id": "trace-export",
            "trace_sha256": trace_hash,
            "replay_artifact_id": "trace-replay",
            "replay_sha256": replay_hash,
            "exported_at": exported_at,
            "replayed_at": replay_marker["replayed_at"],
        }
        command_manifest_hash = verifier.canonical_command_manifest_sha256(
            finite_checks,
            proofs,
            trace_refinement,
        )
        toolchain_manifest_hash = verifier.canonical_toolchain_manifest_sha256(toolchain)
        execution_input_manifest_hash = hashlib.sha256(
            canonical_bytes(
                {
                    "properties": properties,
                    "specification_manifest": specification_manifest,
                    "target": target,
                }
            )
        ).hexdigest()
        if mode == verifier.TARGET_MODE:
            native_record_path = resolve_input(
                manifest_base,
                producer_raw["native_execution_record_path"],
                "$.producer.native_execution_record_path",
            )
            native_record_file, native_record_hash = writer.add_input(
                "native-execution-record",
                "native-execution-record",
                ".json",
                "native-execution-record",
                native_record_path,
            )
            native_record = exact(
                strict_json(native_record_file, "native execution attestation"),
                {
                    "record_kind",
                    "execution_mode",
                    "operating_system",
                    "architecture",
                    "release_id",
                    "source_revision",
                    "source_tree_id",
                    "formal_spec_sha256",
                    "target_id",
                    "input_manifest_sha256",
                    "command_manifest_sha256",
                    "toolchain_manifest_sha256",
                    "completed_at",
                    "result",
                },
                "native execution attestation",
            )
            expected_native_record = {
                "record_kind": NATIVE_EXECUTION_RECORD_KIND,
                "execution_mode": "native-darwin",
                "operating_system": "Darwin",
                "architecture": platform.machine(),
                "release_id": release_id,
                "source_revision": source_revision,
                "source_tree_id": source_tree_id,
                "formal_spec_sha256": formal_spec_sha,
                "target_id": target_id,
                "input_manifest_sha256": execution_input_manifest_hash,
                "command_manifest_sha256": command_manifest_hash,
                "toolchain_manifest_sha256": toolchain_manifest_hash,
                "completed_at": native_record["completed_at"],
                "result": "pass",
            }
            execution_completed_at = utc(
                native_record["completed_at"],
                "native execution attestation.completed_at",
            )
            ensure_fresh(
                execution_completed_at,
                generated_at,
                "native execution attestation.completed_at",
            )
            if native_record != expected_native_record or execution_completed_at < latest_observation_at:
                fail(
                    "native execution attestation does not bind all target inputs, commands, and tools "
                    "after their completion"
                )
            native_signature_path = resolve_input(
                manifest_base,
                producer_raw["native_execution_signature_path"],
                "$.producer.native_execution_signature_path",
            )
            native_signature_file, native_signature_hash = writer.add_input(
                "native-execution-signature",
                "native-execution-signature",
                ".bin",
                "native-execution-signature",
                native_signature_path,
            )
            try:
                verifier.verify_rsa_sha256_signature(
                    canonical_bytes(native_record),
                    producer_public_snapshot,
                    native_signature_file,
                    "native execution attestation",
                )
            except verifier.EvidenceError as exc:
                fail(str(exc))
            native_execution: dict[str, Any] = {
                "attested": True,
                "execution_mode": "native-darwin",
                "input_manifest_sha256": execution_input_manifest_hash,
                "command_manifest_sha256": command_manifest_hash,
                "toolchain_manifest_sha256": toolchain_manifest_hash,
                "completed_at": native_record["completed_at"],
                "result": "pass",
                "record_artifact_id": "native-execution-record",
                "record_sha256": native_record_hash,
                "signature_artifact_id": "native-execution-signature",
                "signature_sha256": native_signature_hash,
            }
            latest_observation_at = execution_completed_at
        else:
            if (
                producer_raw["native_execution_record_path"] is not None
                or producer_raw["native_execution_signature_path"] is not None
            ):
                fail("synthetic collection must not carry a native target execution attestation")
            native_execution = {
                "attested": False,
                "execution_mode": verifier.SYNTHETIC_MODE,
                "input_manifest_sha256": execution_input_manifest_hash,
                "command_manifest_sha256": command_manifest_hash,
                "toolchain_manifest_sha256": toolchain_manifest_hash,
                "completed_at": None,
                "result": "not-applicable",
                "record_artifact_id": None,
                "record_sha256": None,
                "signature_artifact_id": None,
                "signature_sha256": None,
            }

        signoff_raw = exact(
            manifest["sign_off"], {"submitted_by", "reviewed_by", "review_log_path"}, "$.sign_off"
        )
        submitted_by = validate_identity(signoff_raw["submitted_by"], "$.sign_off.submitted_by")
        reviewed_by = validate_identity(signoff_raw["reviewed_by"], "$.sign_off.reviewed_by")
        identities = {
            producer_identity.casefold(),
            submitted_by["authenticated_identity"].casefold(),
            reviewed_by["authenticated_identity"].casefold(),
        }
        if len(identities) != 2 or submitted_by["authenticated_identity"].casefold() != producer_identity.casefold():
            fail("submitter must be the producer and the reviewer must be an independent identity")
        reviewed_input_bindings_hash = reviewed_input_bindings_sha256(bindings.values)
        reviewed_inputs = reviewed_input_entries(bindings.values)
        review_input = resolve_input(manifest_base, signoff_raw["review_log_path"], "$.sign_off.review_log_path")
        review_file, review_hash = writer.add_input(
            "independent-review", "independent-review", ".log", "independent-review", review_input
        )
        review_marker = marker_exact(
            review_file,
            "FORMAL_CLOSURE_REVIEW_RESULT ",
            {"authenticated_identity", "decision", "independent", "reviewed_at", "source_revision"},
            "independent review log",
        )
        expected_review = {
            "authenticated_identity": reviewed_by["authenticated_identity"],
            "decision": "approved",
            "independent": True,
            "reviewed_at": review_marker["reviewed_at"],
            "source_revision": source_revision,
        }
        reviewed_at = utc(review_marker["reviewed_at"], "independent review result.reviewed_at")
        ensure_fresh(reviewed_at, generated_at, "independent review result.reviewed_at")
        if producer_decided_at < latest_observation_at:
            fail("producer decision predates evidence it claims to approve")
        if reviewed_at < producer_decided_at:
            fail("independent review predates the producer decision or executed evidence")
        if review_marker != expected_review:
            fail("independent review decision does not match the reviewer and release")
        review_binding = marker_exact(
            review_file,
            REVIEW_BINDING_MARKER,
            {
                "release_id",
                "source_revision",
                "source_tree_id",
                "target_id",
                "formal_spec_sha256",
                "mutation_nonvacuity_sha256",
                "reviewed_input_bindings_sha256",
                "reviewed_at",
                "decision",
            },
            "independent review binding",
        )
        if review_binding != {
            "release_id": release_id,
            "source_revision": source_revision,
            "source_tree_id": source_tree_id,
            "target_id": target_id,
            "formal_spec_sha256": formal_spec_sha,
            "mutation_nonvacuity_sha256": mutation_hash,
            "reviewed_input_bindings_sha256": reviewed_input_bindings_hash,
            "reviewed_at": review_marker["reviewed_at"],
            "decision": "approved",
        }:
            fail("independent review does not bind the target and mutation/nonvacuity evidence")


        signoff: dict[str, Any] = {
            "submitted_by": submitted_by,
            "reviewed_by": reviewed_by,
            "independent": True,
            "decision": "approved",
            "reviewed_at": review_marker["reviewed_at"],
            "artifact_id": "independent-review",
            "artifact_sha256": review_hash,
            "reviewer_trust_root_path": verifier.REVIEWER_TRUST_ROOT_PATH,
            "reviewer_trust_root_sha256": reviewer_public_hash,
            "reviewed_input_bindings_sha256": reviewed_input_bindings_hash,
            "mutation_evidence_sha256": mutation_hash,
            "signed_payload_sha256": "0" * 64,
            "signature_artifact_id": "reviewer-signature",
            "signature_algorithm": "rsa-sha256",
        }

        reviewer_signature_size = len(
            sign(reviewer_private_key.resolve(), b"", "reviewer signature size")
        )
        producer_signature_size = len(
            sign(producer_private_key.resolve(), b"", "producer signature size")
        )
        reviewer_signature_file, reviewer_signature_record = writer.reserve_generated(
            "reviewer-signature",
            "reviewer-signature",
            ".bin",
            reviewer_signature_size,
        )
        producer_signature_file, producer_signature_record = writer.reserve_generated(
            "producer-signature",
            "producer-signature",
            ".bin",
            producer_signature_size,
        )

        document: dict[str, Any] = {
            "schema_version": verifier.SCHEMA_VERSION,
            "record_kind": verifier.RECORD_KIND,
            "record_mode": mode,
            "claim": verifier.TARGET_CLAIM if mode == verifier.TARGET_MODE else verifier.NON_CLAIM,
            "release": {
                "release_id": release_id,
                "source_repository": source_repository,
                "source_revision": source_revision,
                "source_tree": "clean",
                "formal_spec_sha256": formal_spec_sha,
            },
            "target": target,
            "generated_at": generated_text,
            "valid_until": manifest["valid_until"],
            "toolchain": toolchain,
            "specification_manifest": specification_manifest,
            "properties": properties,
            "finite_model_checks": finite_checks,
            "inductive_proofs": proofs,
            "evidence_basis": {
                "finite_status": "passed-bounded-checks",
                "inductive_status": "checked-passed-unbounded",
                "closure_basis": "finite-plus-inductive",
                "finite_only": False,
                "unchecked_proof_count": 0,
            },
            "mutation_evidence": mutation_evidence,
            "trace_refinement": trace_refinement,
            "joint_consensus": {
                "status": joint_status,
                "declared_scope": declared_scope,
                "release_scope_enforced": True,
                "proof_theorem_id": proof_theorem_id,
                "scope_artifact_id": "scope-enforcement",
                "collected_at": scope_marker["collected_at"],
            },
            "toq_clock_discipline": {
                "in_claim": True,
                "target_environment": target_environment,
                "result": "pass",
                "clock_source": clock_source,
                "maximum_clock_error_ms": expected_clock["maximum_clock_error_ms"],
                "collected_at": clock_marker["collected_at"],
                "evidence_uri": evidence_uri,
                "remote_evidence_sha256": remote_hash,
                "artifact_id": "toq-clock-discipline",
                "artifact_sha256": clock_hash,
                "remote_artifact_id": "toq-clock-observation",
                "remote_artifact_sha256": remote_observation_hash,
                "source_revision": source_revision,
            },
            "native_execution": native_execution,
            "reviewed_input_set": {
                "digest_algorithm": "sha256-canonical-json-v1",
                "manifest_artifact_id": "collection-manifest",
                "manifest_sha256": collection_manifest_hash,
                "reviewed_input_bindings_sha256": reviewed_input_bindings_hash,
                "inputs": reviewed_inputs,
            },
            "production_attestation": {
                "signer_identity": producer_identity,
                "decision": "approved",
                "decided_at": producer_raw["decided_at"],
                "trust_root_path": verifier.PRODUCER_TRUST_ROOT_PATH,
                "trust_root_sha256": producer_public_hash,
                "signature_algorithm": "rsa-sha256",
                "signed_payload_sha256": "0" * 64,
                "signature_artifact_id": "producer-signature",
                "signed_at": producer_raw["signed_at"],
            },
            "sign_off": signoff,
            "raw_artifacts": writer.records,
        }
        attestation_payload = verifier.evidence_attestation_payload(document)
        attestation_hash = hashlib.sha256(attestation_payload).hexdigest()
        document["production_attestation"]["signed_payload_sha256"] = attestation_hash
        document["sign_off"]["signed_payload_sha256"] = attestation_hash
        reviewer_signature = sign(
            reviewer_private_key.resolve(),
            attestation_payload,
            "independent review signature",
        )
        reviewer_signature_file = writer.finalize_reserved(
            reviewer_signature_file,
            reviewer_signature_record,
            reviewer_signature,
        )
        producer_signature = sign(
            producer_private_key.resolve(),
            attestation_payload,
            "production evidence signature",
        )
        producer_signature_file = writer.finalize_reserved(
            producer_signature_file,
            producer_signature_record,
            producer_signature,
        )
        if verifier.evidence_attestation_payload(document) != attestation_payload:
            fail("signature hashes must be the only attestation-payload exclusions")
        try:
            verifier.verify_rsa_sha256_signature(
                attestation_payload,
                reviewer_public_snapshot,
                reviewer_signature_file,
                "new independent review signature",
            )
            verifier.verify_rsa_sha256_signature(
                attestation_payload,
                producer_public_snapshot,
                producer_signature_file,
                "new production evidence signature",
            )
        except verifier.EvidenceError as exc:
            fail(str(exc))

        evidence_path = stage / "formal-closure-evidence.json"
        evidence_path.write_bytes(json.dumps(document, indent=2, sort_keys=True).encode("utf-8") + b"\n")
        os.chmod(evidence_path, 0o444)
        try:
            with git_replacements_disabled():
                verifier.validate_schema_contract()
                verifier.validate_document(
                    document,
                    evidence_path=evidence_path,
                    repository_root=repository,
                    expected_release_id=release_id,
                    expected_source_revision=source_revision,
                    expected_formal_spec_sha256=formal_spec_sha,
                    expected_target_id=trusted_target_id,
                    expected_environment_profile=trusted_environment,
                    allow_synthetic_test_fixture=allow_synthetic_test_fixture,
                    now=current,
                )
        except verifier.EvidenceError as exc:
            fail(f"assembled evidence does not satisfy the verifier contract: {exc}")

        producer_public_snapshot.unlink()
        reviewer_public_snapshot.unlink()
        os.replace(stage, output)
        return output / evidence_path.name
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--producer-private-key", type=Path, required=True)
    parser.add_argument("--producer-public-key", type=Path, required=True)
    parser.add_argument("--reviewer-private-key", type=Path, required=True)
    parser.add_argument("--reviewer-public-key", type=Path, required=True)
    parser.add_argument("--expected-target-id", required=True)
    parser.add_argument("--expected-environment-profile", required=True)
    parser.add_argument(
        "--allow-synthetic-test-fixture",
        action="store_true",
        help="permit an explicit synthetic-test/claim=none fixture; never enables a target claim",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    try:
        args = parse_args(argv)
        evidence = collect(
            manifest_path=args.manifest,
            repository_root=args.repository_root,
            output_dir=args.output_dir,
            producer_private_key=args.producer_private_key,
            producer_public_key=args.producer_public_key,
            reviewer_private_key=args.reviewer_private_key,
            reviewer_public_key=args.reviewer_public_key,
            expected_target_id=args.expected_target_id,
            expected_environment_profile=args.expected_environment_profile,
            allow_synthetic_test_fixture=args.allow_synthetic_test_fixture,
        )
        document = strict_json(evidence, "assembled evidence")
        print(
            "formal_closure_collection=complete "
            f"mode={document['record_mode']} claim={document['claim']} evidence={evidence}"
        )
        return 0
    except (CollectionError, verifier.EvidenceError, OSError) as exc:
        print(f"formal_closure_collection=invalid: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
