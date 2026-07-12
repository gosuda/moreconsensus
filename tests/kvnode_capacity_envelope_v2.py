#!/usr/bin/env python3
"""Collect, assemble, and verify native Darwin arm64 capacity evidence v2."""

from __future__ import annotations

import argparse
import concurrent.futures
import ctypes
import hashlib
import json
import math
import os
import platform
import re
import shutil
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-evidence-v2.json"
PRODUCTION_SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-production-capacity-certification-v2.json"
PRODUCTION_TEST_SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-production-capacity-test-fixture-v2.json"
PRODUCTION_INTERMEDIATE_SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-production-capacity-unsigned-intermediate-v2.json"
MEASUREMENT_SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-measurement-v2.json"
REVIEW_SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-independent-review-v2.json"
PRODUCTION_CAMPAIGN_SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-production-capacity-campaign-v2.json"
PRODUCTION_APPROVAL_SCHEMA_URI = "https://gosuda.org/moreconsensus/schemas/kvnode-production-capacity-approval-v2.json"
VERIFIER_VERSION = "2.0.0"
TARGET_ID = "mc-kv-darwin24-arm64-launchd-3n-r1"
ENVIRONMENT_PROFILE = "native-darwin24-arm64-launchd-system-domain-v1"
PHASES = ("warmup", "steady", "concurrent", "saturation", "recovery-observation")
MEASUREMENT_PHASES = PHASES[1:]
REQUIRED_NONCLAIMS = {
    "not-multi-host",
    "not-independent-failure-domain",
    "tls-server-auth-only-not-mtls-or-client-authorization",
    "no-hard-cpu-isolation",
    "no-hard-memory-isolation",
    "workstation-characterization-not-production-capacity-proof",
    "apfs-fsync-probe-not-kvnode-io-service-time",
}
PRODUCTION_RUN_ARTIFACT_KINDS = {
    "isolation-evidence",
    "request-observations",
    "system-observations",
}
PRODUCTION_MIN_REPETITIONS = 3
PRODUCTION_CLAIM_SCOPE = "declared-native-darwin-same-host-support-envelope"
PRODUCTION_RELEASE_CLAIM = "target-bound-production-capacity-envelope-certified"
PRODUCTION_INTERMEDIATE_CLASS = "production-capacity-certification-unsigned-intermediate"
PRODUCTION_FINAL_CLASS = "production-capacity-certification"
PRODUCTION_TEST_CLASS = "synthetic-production-capacity-certification-test-only"
REQUIRED_ARTIFACT_KINDS = {
    "environment-observations",
    "pre-run-plan",
    "phase-windows",
    "request-samples",
    "process-samples",
    "disk-latency-raw",
    "disk-latency-samples",
    "measurement",
}
SAMPLE_PROVENANCE = {
    "process_rusage": "darwin-libproc-proc_pid_rusage-v2",
    "fd_count": "darwin-lsof-numeric-f-field-only",
    "disk_latency": "darwin-apfs-fsync-probe-per-data-dir",
    "apfs_allocated": "darwin-du-sk-times-1024",
    "apfs_logical": "darwin-du-sk-A-times-1024",
    "readiness": "ca-verified-https-readyz",
    "metrics": "ca-verified-https-metrics",
}
FORBIDDEN_OBSERVER_TERMS = ("/proc", "docker", "containerd", "podman", "lima", "qemu", "systemd")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$")
REVISION_RE = re.compile(r"^[0-9a-f]{40,64}$")
UTC_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z$")
NUMERIC_FD_RE = re.compile(r"^f([0-9]+)[A-Za-z]*$")


class EvidenceError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise EvidenceError(message)


def load_json(path: Path, label: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                fail(f"{label} contains duplicate key {key}")
            result[key] = value
        return result

    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle, object_pairs_hook=reject_duplicates)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"cannot read {label}: {exc}")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
def write_new_file_no_follow(path: Path, content: bytes, mode: int, label: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if no_follow == 0:
        fail(f"{label} requires native no-follow file creation")
    descriptor = -1
    created = False
    completed = False
    try:
        descriptor = os.open(path, flags | no_follow, mode)
        created = True
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                fail(f"{label} could not write complete bytes")
            view = view[written:]
        os.fsync(descriptor)
        completed = True
    except OSError as exc:
        fail(f"{label} must be a new non-symlink file: {exc}")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if created and not completed:
            path.unlink(missing_ok=True)


def write_new_production_json(path: Path, value: Any, label: str) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    write_new_file_no_follow(path, payload, 0o600, label)




def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        fail(f"cannot hash {path}: {exc}")
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        fail(f"{label} must be a canonical UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        fail(f"{label} is invalid: {exc}")
    return parsed


def exact_object(value: Any, required: set[str], label: str, optional: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    optional = optional or set()
    keys = set(value)
    missing = required - keys
    extra = keys - required - optional
    if missing:
        fail(f"{label} is missing fields: {','.join(sorted(missing))}")
    if extra:
        fail(f"{label} has unexpected fields: {','.join(sorted(extra))}")
    return value


def require_string(value: Any, label: str, *, maximum: int = 1024) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or any(c in value for c in "\r\n\x00"):
        fail(f"{label} must be a nonempty bounded single-line string")
    return value


def require_int(value: Any, label: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or (maximum is not None and value > maximum):
        fail(f"{label} must be an integer in range")
    return value


def require_number(value: Any, label: str, *, minimum: float = 0.0, maximum: float | None = None, strict_minimum: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        fail(f"{label} must be a finite number")
    number = float(value)
    if (strict_minimum and number <= minimum) or (not strict_minimum and number < minimum) or (maximum is not None and number > maximum):
        fail(f"{label} is outside its allowed range")
    return number


def require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        fail(f"{label} must be a lowercase SHA-256")
    return value


def binding_from(value: Any, label: str) -> dict[str, Any]:
    binding = exact_object(
        value,
        {"target_id", "release_id", "source_revision", "binary_sha256", "environment_profile"},
        label,
    )
    if binding["target_id"] != TARGET_ID:
        fail(f"{label}.target_id is not the Darwin target")
    if not isinstance(binding["release_id"], str) or not RELEASE_ID_RE.fullmatch(binding["release_id"]):
        fail(f"{label}.release_id is malformed")
    if not isinstance(binding["source_revision"], str) or not REVISION_RE.fullmatch(binding["source_revision"]):
        fail(f"{label}.source_revision must be a lowercase immutable revision")
    require_sha256(binding["binary_sha256"], f"{label}.binary_sha256")
    if binding["environment_profile"] != ENVIRONMENT_PROFILE:
        fail(f"{label}.environment_profile is invalid")
    return binding


def sample_binding(value: Any, expected: dict[str, Any], label: str) -> None:
    row = exact_object(value, {"target", "verifier_version"}, label)
    if binding_from(row["target"], f"{label}.target") != expected:
        fail(f"{label} does not match the record target binding")
    if row["verifier_version"] != VERIFIER_VERSION:
        fail(f"{label}.verifier_version is invalid")


def parse_env_receipt(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read receipt: {exc}")
    if not lines or len(lines) > 128:
        fail("receipt must contain 1-128 fields")
    result: dict[str, str] = {}
    for index, line in enumerate(lines, 1):
        match = re.fullmatch(r"([A-Za-z][A-Za-z0-9_]*)=(.+)", line)
        if match is None:
            fail(f"receipt line {index} is malformed")
        key, value = match.groups()
        if key in result:
            fail(f"receipt field {key} is duplicated")
        result[key] = value
    return result


def safe_artifact(root: Path, relative: Any, label: str) -> Path:
    rel = require_string(relative, label, maximum=512)
    candidate_rel = Path(rel)
    if candidate_rel.is_absolute() or ".." in candidate_rel.parts or "." in candidate_rel.parts:
        fail(f"{label} must be a normalized relative path")
    candidate = root.joinpath(candidate_rel)
    try:
        if candidate.is_symlink() or not candidate.is_file():
            fail(f"{label} must identify a regular non-symlink file")
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        fail(f"{label} escapes the evidence root")
    if resolved.stat().st_size <= 0:
        fail(f"{label} must not be empty")
    return resolved


def safe_absolute(root: Path, raw: str, label: str) -> Path:
    path = Path(require_string(raw, label, maximum=2048))
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        fail(f"{label} must be an absolute regular non-symlink file")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        fail(f"{label} escapes evidence_root_path")
    return resolved


def run_observation(argv: list[str], *, timeout: float = 30.0) -> dict[str, Any]:
    try:
        completed = subprocess.run(argv, check=False, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        fail(f"observation command failed to execute: {' '.join(argv)}: {exc}")
    return {
        "argv": argv,
        "returncode": completed.returncode,
        "stdout": completed.stdout.rstrip("\n"),
        "stderr": completed.stderr.rstrip("\n"),
    }


def validate_schema_contract() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "release/evidence/schema/kvnode-capacity-evidence-v2.schema.json"
    schema = load_json(schema_path, "capacity v2 schema")
    root = exact_object(
        schema,
        {"$schema", "$id", "title", "description", "type", "additionalProperties", "required", "properties", "$defs"},
        "capacity v2 schema",
    )
    if root["$schema"] != "https://json-schema.org/draft/2020-12/schema" or root["$id"] != SCHEMA_URI:
        fail("capacity v2 schema identity is invalid")
    if root["type"] != "object" or root["additionalProperties"] is not False:
        fail("capacity v2 schema root must be a closed object")
    properties = root["properties"]
    if properties.get("evidence_class", {}).get("const") != "workstation-capacity-characterization":
        fail("capacity v2 schema must fix the characterization class")
    if properties.get("production_capacity_certification", {}).get("const") is not False:
        fail("capacity v2 schema must forbid production capacity certification")
    production_schema_path = (
        Path(__file__).resolve().parents[1]
        / "release/evidence/schema/kvnode-production-capacity-certification-v2.schema.json"
    )
    production = load_json(production_schema_path, "production capacity v2 schema")
    production_root = exact_object(
        production,
        {"$schema", "$id", "title", "description", "type", "additionalProperties", "required", "properties", "$defs"},
        "production capacity v2 schema",
    )
    if (
        production_root["$schema"] != "https://json-schema.org/draft/2020-12/schema"
        or production_root["$id"] != PRODUCTION_SCHEMA_URI
        or production_root["type"] != "object"
        or production_root["additionalProperties"] is not False
    ):
        fail("production capacity v2 schema identity or closure is invalid")
    production_properties = production_root["properties"]
    if production_properties.get("evidence_class", {}).get("const") != PRODUCTION_FINAL_CLASS:
        fail("production capacity v2 schema must fix the production certification class")
    if production_properties.get("production_capacity_certification", {}).get("const") is not True:
        fail("production capacity v2 schema must require production certification")
    for field in ("campaign", "binary", "intermediate", "approval"):
        if field not in production_root["required"] or field not in production_properties:
            fail(f"production capacity v2 schema omits emitted field {field}")
    native_schema = production_root["$defs"]["nativeEnvironment"]["properties"]
    workload_schema = production_root["$defs"]["supportEnvelope"]["properties"]["workload"]
    if native_schema["tls_scope"].get("const") != "mutual-auth-separated-planes":
        fail("production capacity schema must require separated-plane mutual TLS")
    if "value_sizes_bytes" not in workload_schema["required"] or workload_schema["properties"]["value_sizes_bytes"].get("const") != [64, 1024, 65536, 1048576]:
        fail("production capacity schema must require the exact four value-size cells")
    test_schema_path = (
        Path(__file__).resolve().parents[1]
        / "release/evidence/schema/kvnode-production-capacity-test-fixture-v2.schema.json"
    )
    test_schema = load_json(test_schema_path, "synthetic production capacity v2 schema")
    test_root = exact_object(
        test_schema,
        {"$schema", "$id", "title", "description", "type", "additionalProperties", "required", "properties"},
        "synthetic production capacity v2 schema",
    )
    if (
        test_root["$schema"] != "https://json-schema.org/draft/2020-12/schema"
        or test_root["$id"] != PRODUCTION_TEST_SCHEMA_URI
        or test_root["type"] != "object"
        or test_root["additionalProperties"] is not False
        or test_root["properties"].get("evidence_class", {}).get("const") != PRODUCTION_TEST_CLASS
        or test_root["properties"].get("production_capacity_certification", {}).get("const") is not False
    ):
        fail("synthetic production capacity v2 schema identity or nonproduction contract is invalid")


def parse_numeric_lsof_fds(output: str) -> set[int]:
    result: set[int] = set()
    for line in output.splitlines():
        match = NUMERIC_FD_RE.fullmatch(line.strip())
        if match is not None:
            result.add(int(match.group(1)))
    return result


def darwin_numeric_fd_count(pid: int) -> int:
    require_int(pid, "pid", minimum=1)
    observed = run_observation(["lsof", "-nP", "-a", "-p", str(pid), "-F", "f"])
    if observed["returncode"] != 0:
        fail(f"lsof could not observe PID {pid}")
    fds = parse_numeric_lsof_fds(observed["stdout"])
    if not fds:
        fail(f"lsof returned no numeric descriptors for PID {pid}")
    return len(fds)


class RusageInfoV2(ctypes.Structure):
    _fields_ = [
        ("ri_uuid", ctypes.c_ubyte * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
        ("ri_child_user_time", ctypes.c_uint64),
        ("ri_child_system_time", ctypes.c_uint64),
        ("ri_child_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_child_interrupt_wkups", ctypes.c_uint64),
        ("ri_child_pageins", ctypes.c_uint64),
        ("ri_child_elapsed_abstime", ctypes.c_uint64),
        ("ri_diskio_bytesread", ctypes.c_uint64),
        ("ri_diskio_byteswritten", ctypes.c_uint64),
    ]


def darwin_process_observation(pid: int) -> dict[str, Any]:
    if platform.system() != "Darwin":
        fail("Darwin process observation cannot run on a non-Darwin host")
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    path_buffer = ctypes.create_string_buffer(4096)
    path_length = libproc.proc_pidpath(pid, path_buffer, len(path_buffer))
    if path_length <= 0:
        fail(f"proc_pidpath failed for PID {pid}")
    usage = RusageInfoV2()
    if libproc.proc_pid_rusage(pid, 2, ctypes.byref(usage)) != 0:
        fail(f"proc_pid_rusage v2 failed for PID {pid}")
    return {
        "process_start_token": int(usage.ri_proc_start_abstime),
        "executable_path": os.path.realpath(path_buffer.value.decode("utf-8")),
        "cpu_user_time_ns": int(usage.ri_user_time),
        "cpu_system_time_ns": int(usage.ri_system_time),
        "rss_bytes": int(usage.ri_resident_size),
        "physical_footprint_bytes": int(usage.ri_phys_footprint),
        "disk_read_bytes": int(usage.ri_diskio_bytesread),
        "disk_written_bytes": int(usage.ri_diskio_byteswritten),
        "open_fd_count": darwin_numeric_fd_count(pid),
    }


def du_bytes(path: Path, apparent: bool) -> int:
    argv = ["du", "-sk"]
    if apparent:
        argv.append("-A")
    argv.append(str(path))
    observed = run_observation(argv)
    if observed["returncode"] != 0:
        fail(f"du could not observe {path}")
    token = observed["stdout"].split(maxsplit=1)[0]
    if not token.isdigit():
        fail(f"du returned malformed size for {path}")
    return int(token) * 1024


def https_get(url: str, context: ssl.SSLContext, timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, context=context, timeout=timeout) as response:
            return response.status, response.read(4 * 1024 * 1024)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(4 * 1024 * 1024)


def parse_metrics(body: bytes) -> dict[str, int]:
    values: dict[str, int] = {}
    wanted = {"kvnode_send_queue_depth", "kvnode_epaxos_instances", "kvnode_epaxos_executed"}
    try:
        text = body.decode("utf-8")
    except UnicodeError:
        fail("metrics response is not UTF-8")
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in wanted and re.fullmatch(r"[0-9]+", parts[1]):
            values[parts[0]] = int(parts[1])
    if set(values) != wanted:
        fail("metrics response is missing required integer gauges")
    return values


def nearest_rank(values: Iterable[int], percentile: int) -> int:
    ordered = sorted(values)
    if not ordered:
        fail("cannot compute a percentile from zero values")
    rank = max(1, math.ceil(len(ordered) * percentile / 100))
    return ordered[rank - 1]


def artifact_entry(root: Path, path: Path, kind: str) -> dict[str, Any]:
    root = root.resolve(strict=True)
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError:
        fail(f"artifact {path} is outside the evidence root")
    if path.is_symlink() or not path.is_file() or path.stat().st_size <= 0:
        fail(f"artifact {path} must be a nonempty regular non-symlink file")
    return {
        "kind": kind,
        "path": relative.as_posix(),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def load_jsonl(path: Path, label: str, *, maximum: int = 1_000_000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if line_number > maximum:
                    fail(f"{label} has too many rows")
                if not line.endswith("\n") or not line.strip():
                    fail(f"{label} row {line_number} is malformed")
                value = json.loads(line)
                if not isinstance(value, dict):
                    fail(f"{label} row {line_number} must be an object")
                rows.append(value)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"cannot read {label}: {exc}")
    if not rows:
        fail(f"{label} has no rows")
    return rows


def apfs_fsync_probe(data_dir: Path, node_id: int, probe_token: str) -> tuple[dict[str, Any], dict[str, Any]]:
    probe_path = data_dir / f".kvnode-capacity-fsync-{probe_token}-{node_id}-{time.monotonic_ns()}"
    payload = b"kvnode-capacity-apfs-fsync-probe\n" * 128
    descriptor = -1
    started_ns = 0
    completed_ns = 0
    try:
        descriptor = os.open(probe_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        written = os.write(descriptor, payload)
        if written != len(payload):
            fail(f"short APFS fsync probe write for node {node_id}")
        started_ns = time.monotonic_ns()
        os.fsync(descriptor)
        completed_ns = time.monotonic_ns()
    except OSError as exc:
        fail(f"APFS fsync probe failed for node {node_id}: {exc}")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            probe_path.unlink(missing_ok=True)
        except OSError as exc:
            fail(f"cannot remove APFS fsync probe for node {node_id}: {exc}")
    if completed_ns <= started_ns:
        fail(f"APFS fsync probe clock did not advance for node {node_id}")
    raw = {
        "node_id": node_id,
        "data_dir": str(data_dir),
        "probe_bytes": len(payload),
        "fsync_started_monotonic_ns": started_ns,
        "fsync_completed_monotonic_ns": completed_ns,
        "status": "ok",
        "provenance": "darwin-apfs-fsync-probe-per-data-dir",
    }
    normalized = {
        "node_id": node_id,
        "data_dir": str(data_dir),
        "monotonic_ns": completed_ns,
        "latency_ns": completed_ns - started_ns,
        "provenance": "darwin-apfs-fsync-probe-per-data-dir",
    }
    return raw, normalized


@dataclass
class CollectorContext:
    config: dict[str, Any]
    root: Path
    binding: dict[str, Any]
    nodes: list[dict[str, Any]]
    ssl_context: ssl.SSLContext
    timeout_seconds: float
    phase_lock: threading.Lock
    phase: str
    sample_rows: list[dict[str, Any]]
    disk_rows: list[dict[str, Any]]
    disk_raw_rows: list[dict[str, Any]]
    probe_token: str
    observed_peaks: dict[int, int]
    previous_cpu: dict[int, tuple[int, int]]
    sample_lock: threading.Lock

    def current_phase(self) -> str:
        with self.phase_lock:
            return self.phase

    def set_phase(self, phase: str) -> None:
        with self.phase_lock:
            self.phase = phase

    def binding_row(self) -> dict[str, Any]:
        return {"target": self.binding, "verifier_version": VERIFIER_VERSION}

    def sample_once(self, phase: str | None = None) -> None:
        with self.sample_lock:
            self._sample_once_unlocked(phase)

    def _sample_once_unlocked(self, phase: str | None = None) -> None:
        phase = phase or self.current_phase()
        if phase not in PHASES:
            return
        timestamp = utc_now()
        monotonic_ns = time.monotonic_ns()
        node_rows: list[dict[str, Any]] = []
        data_rows: list[dict[str, Any]] = []
        for node in self.nodes:
            pid = node["pid"]
            observed = darwin_process_observation(pid)
            cpu_observed_monotonic_ns = time.monotonic_ns()
            if observed["process_start_token"] != node["process_start_token"]:
                fail(f"PID {pid} was reused or restarted")
            if observed["executable_path"] != node["executable_path"]:
                fail(f"PID {pid} executable path changed")
            observed_hash = sha256_file(Path(observed["executable_path"]))
            if observed_hash != self.binding["binary_sha256"]:
                fail(f"PID {pid} executable hash changed")
            cpu_total_ns = observed["cpu_user_time_ns"] + observed["cpu_system_time_ns"]
            previous_total_ns, previous_monotonic_ns = self.previous_cpu[pid]
            if cpu_total_ns < previous_total_ns or cpu_observed_monotonic_ns <= previous_monotonic_ns:
                fail(f"PID {pid} CPU counters or observation clock regressed")
            cpu_utilization_percent = (cpu_total_ns - previous_total_ns) * 100.0 / (cpu_observed_monotonic_ns - previous_monotonic_ns)
            self.previous_cpu[pid] = (cpu_total_ns, cpu_observed_monotonic_ns)
            self.observed_peaks[pid] = max(self.observed_peaks.get(pid, 0), observed["rss_bytes"])
            ready_status, _ = https_get(node["admin_url"] + "/readyz", self.ssl_context, self.timeout_seconds)
            metrics_status, metrics_body = https_get(node["admin_url"] + "/metrics", self.ssl_context, self.timeout_seconds)
            if metrics_status != 200:
                fail(f"metrics request failed for PID {pid}")
            metrics = parse_metrics(metrics_body)
            node_rows.append(
                {
                    "node_id": node["node_id"],
                    "pid": pid,
                    "process_start_token": observed["process_start_token"],
                    "executable_path": observed["executable_path"],
                    "observed_binary_sha256": observed_hash,
                    "cpu_user_time_ns": observed["cpu_user_time_ns"],
                    "cpu_system_time_ns": observed["cpu_system_time_ns"],
                    "cpu_utilization_percent": cpu_utilization_percent,
                    "rss_bytes": observed["rss_bytes"],
                    "physical_footprint_bytes": observed["physical_footprint_bytes"],
                    "observed_peak_rss_bytes": self.observed_peaks[pid],
                    "open_fd_count": observed["open_fd_count"],
                    "disk_read_bytes": observed["disk_read_bytes"],
                    "disk_written_bytes": observed["disk_written_bytes"],
                    "ready_http_status": ready_status,
                    "send_queue_depth": metrics["kvnode_send_queue_depth"],
                    "epaxos_instances": metrics["kvnode_epaxos_instances"],
                    "epaxos_executed": metrics["kvnode_epaxos_executed"],
                    "process_restart_count": 0,
                }
            )
            data_dir = Path(node["data_dir"])
            data_rows.append(
                {
                    "node_id": node["node_id"],
                    "data_dir": node["data_dir"],
                    "apfs_allocated_bytes": du_bytes(data_dir, False),
                    "apfs_logical_bytes": du_bytes(data_dir, True),
                }
            )
            raw_probe, disk_probe = apfs_fsync_probe(data_dir, node["node_id"], self.probe_token)
            raw_probe.update({"binding": self.binding_row(), "timestamp_utc": utc_now(), "phase": phase})
            disk_probe.update({"binding": self.binding_row(), "timestamp_utc": utc_now(), "phase": phase})
            self.disk_raw_rows.append(raw_probe)
            self.disk_rows.append(disk_probe)
        row = {
            "binding": self.binding_row(),
            "sample_index": 0,
            "timestamp_utc": timestamp,
            "monotonic_ns": monotonic_ns,
            "phase": phase,
            "provenance": SAMPLE_PROVENANCE,
            "nodes": node_rows,
            "data_dirs": data_rows,
        }
        row["sample_index"] = len(self.sample_rows) + 1
        self.sample_rows.append(row)


def sample_loop(context: CollectorContext, stop: threading.Event, interval_seconds: float, errors: list[BaseException]) -> None:
    deadline = time.monotonic()
    while not stop.is_set():
        try:
            context.sample_once()
        except BaseException as exc:  # Propagate sampler failures to the collector.
            errors.append(exc)
            stop.set()
            return
        deadline += interval_seconds
        stop.wait(max(0.0, deadline - time.monotonic()))




def request_once(
    context: CollectorContext,
    phase: str,
    worker: int,
    index: int,
    operation_slot: int,
    key: str,
    value: bytes,
) -> dict[str, Any]:
    node = context.nodes[index % len(context.nodes)]
    if operation_slot == 0:
        operation = "put"
        request = urllib.request.Request(node["client_url"] + "/kv/" + key, method="PUT", data=value)
    elif operation_slot == 1:
        operation = "get"
        request = urllib.request.Request(node["client_url"] + "/kv/" + key, method="GET")
    else:
        operation = "scan"
        request = urllib.request.Request(node["client_url"] + "/scan?prefix=" + key + "&limit=16", method="GET")
    started_utc = utc_now()
    started_ns = time.monotonic_ns()
    status = 0
    outcome = "transport-error"
    try:
        with urllib.request.urlopen(request, context=context.ssl_context, timeout=context.timeout_seconds) as response:
            status = response.status
            response.read(4 * 1024 * 1024)
        outcome = "success" if 200 <= status < 300 else "http-error"
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.read(4 * 1024 * 1024)
        outcome = "http-error"
    except (OSError, TimeoutError, urllib.error.URLError):
        outcome = "transport-error"
    ended_ns = time.monotonic_ns()
    return {
        "binding": context.binding_row(),
        "phase": phase,
        "worker": worker,
        "operation_index": index,
        "operation": operation,
        "node_id": node["node_id"],
        "started_utc": started_utc,
        "started_monotonic_ns": started_ns,
        "ended_monotonic_ns": ended_ns,
        "latency_ns": max(1, ended_ns - started_ns),
        "http_status": status,
        "outcome": outcome,
    }


def run_phase_workload(context: CollectorContext, phase: str, operations: int, concurrency: int, run_id: str) -> list[dict[str, Any]]:
    def worker(worker_id: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        key = f"kvnode-capacity-{run_id}-{phase}-worker-{worker_id}"
        value = (f"value-{run_id}-{phase}-{worker_id}-".encode("ascii") * 128)[:4096]
        for local_index, operation_index in enumerate(range(worker_id - 1, operations, concurrency)):
            rows.append(request_once(context, phase, worker_id, operation_index, local_index % 3, key, value))
        return rows

    worker_count = min(operations, concurrency)
    rows: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(worker, worker_id) for worker_id in range(1, worker_count + 1)]
        for future in futures:
            rows.extend(future.result())
    return sorted(rows, key=lambda row: row["started_monotonic_ns"])


def validate_collector_config(config: Any) -> dict[str, Any]:
    required = {
        "target",
        "operator_identity",
        "binary_path",
        "tls_ca_path",
        "nodes",
        "resource_envelope",
        "pre_run_plan_path",
        "phases",
        "sample_interval_seconds",
        "request_timeout_seconds",
        "recovery_wait_seconds",
        "nonclaims",
    }
    config = exact_object(config, required, "collector config")
    binding = binding_from(config["target"], "collector config.target")
    require_string(config["operator_identity"], "collector config.operator_identity", maximum=128)
    binary = Path(require_string(config["binary_path"], "collector config.binary_path", maximum=2048))
    ca = Path(require_string(config["tls_ca_path"], "collector config.tls_ca_path", maximum=2048))
    plan = Path(require_string(config["pre_run_plan_path"], "collector config.pre_run_plan_path", maximum=2048))
    for path, label in ((binary, "binary"), (ca, "TLS CA"), (plan, "pre-run plan")):
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            fail(f"collector {label} path must be an absolute regular non-symlink file")
    if sha256_file(binary) != binding["binary_sha256"]:
        fail("collector binary does not match target.binary_sha256")
    nodes = config["nodes"]
    if not isinstance(nodes, list) or len(nodes) != 3:
        fail("collector config must name exactly three nodes")
    seen_pids: set[int] = set()
    seen_ids: set[int] = set()
    seen_urls: set[str] = set()
    seen_data_dirs: set[Path] = set()
    for index, raw_node in enumerate(nodes):
        node = exact_object(raw_node, {"node_id", "launchd_label", "pid", "data_dir", "client_url", "admin_url"}, f"collector node {index + 1}")
        node_id = require_int(node["node_id"], f"collector node {index + 1}.node_id", minimum=1, maximum=3)
        pid = require_int(node["pid"], f"collector node {index + 1}.pid", minimum=1)
        expected_label = f"org.gosuda.moreconsensus.kvnode.{node_id}"
        if node["launchd_label"] != expected_label:
            fail(f"collector node {node_id} launchd label must equal {expected_label}")
        if node_id in seen_ids or pid in seen_pids:
            fail("collector nodes must have distinct node IDs and PIDs")
        seen_ids.add(node_id)
        seen_pids.add(pid)
        for key in ("client_url", "admin_url"):
            url = require_string(node[key], f"collector node {node_id}.{key}")
            if re.fullmatch(r"https://127\.0\.0\.1:[0-9]{1,5}", url) is None:
                fail("collector URLs must use loopback HTTPS")
            if url in seen_urls:
                fail("collector client and admin URLs must be unique across exact nodes")
            seen_urls.add(url)
        data_dir = Path(require_string(node["data_dir"], f"collector node {node_id}.data_dir", maximum=2048))
        if not data_dir.is_absolute() or data_dir.is_symlink() or not data_dir.is_dir():
            fail("collector data directories must be absolute existing non-symlink directories")
        resolved_data_dir = data_dir.resolve(strict=True)
        if resolved_data_dir in seen_data_dirs:
            fail("collector data directories must be unique across exact nodes")
        seen_data_dirs.add(resolved_data_dir)
    if seen_ids != {1, 2, 3}:
        fail("collector node IDs must be exactly 1,2,3")
    envelope = config["resource_envelope"]
    validate_resource_envelope(envelope, binding, None)
    plan_value = load_json(plan, "pre-run plan")
    if sha256_file(plan) != envelope["plan_sha256"]:
        fail("pre-run plan hash does not match the resource envelope")
    validate_plan(plan_value, binding, envelope, config["operator_identity"], None)
    phases = config["phases"]
    exact_object(phases, set(PHASES), "collector config.phases")
    for phase in PHASES:
        phase_config = exact_object(phases[phase], {"operations", "concurrency"}, f"collector phase {phase}")
        require_int(phase_config["operations"], f"collector phase {phase}.operations", minimum=1, maximum=1000)
        require_int(phase_config["concurrency"], f"collector phase {phase}.concurrency", minimum=1, maximum=64)
    require_number(config["sample_interval_seconds"], "sample interval", minimum=0.25, maximum=10)
    require_number(config["request_timeout_seconds"], "request timeout", minimum=0.1, maximum=300)
    require_number(config["recovery_wait_seconds"], "recovery wait", minimum=0, maximum=300)
    if not isinstance(config["nonclaims"], list) or not REQUIRED_NONCLAIMS.issubset(set(config["nonclaims"])):
        fail("collector config is missing mandatory nonclaims")
    return config


def validate_native_host(binary_path: Path, nodes: list[dict[str, Any]], binding: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], dict[int, tuple[int, int]]]:
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        fail("Darwin v2 collection requires a native Darwin arm64 host")
    if os.geteuid() == 0:
        fail("Darwin v2 collection forbids a root collector; grant the unprivileged collector only the required process and APFS access")
    observations = {
        "uname": run_observation(["uname", "-srm"]),
        "sw_vers": run_observation(["sw_vers"]),
        "file": run_observation(["file", str(binary_path)]),
        "go-build": run_observation(["go", "version", "-m", str(binary_path)]),
        "virtualization": run_observation(["sysctl", "-n", "kern.hv_vmm_present"]),
        "hardware": run_observation(["system_profiler", "SPHardwareDataType"]),
    }
    for raw_node in nodes:
        node_id = raw_node["node_id"]
        data_dir = raw_node["data_dir"]
        observations[f"apfs-stat-node-{node_id}"] = run_observation(["stat", "-f", "%T", data_dir])
        observations[f"apfs-diskutil-node-{node_id}"] = run_observation(["diskutil", "info", data_dir])
        observations[f"launchd-node-{node_id}"] = run_observation(["launchctl", "print", f"system/{raw_node['launchd_label']}"])
    for name, observation in observations.items():
        if observation["returncode"] != 0:
            fail(f"native host observation {name} failed")
    file_text = observations["file"]["stdout"].lower()
    if "mach-o" not in file_text or "arm64" not in file_text:
        fail("collector binary is not a Mach-O arm64 executable")
    go_build = observations["go-build"]["stdout"]
    if binding["source_revision"] not in go_build or "vcs.modified=false" not in go_build:
        fail("collector binary build provenance does not match source_revision or is modified")
    if observations["virtualization"]["stdout"].strip() != "0":
        fail("collector host reports a virtual machine monitor")
    for raw_node in nodes:
        node_id = raw_node["node_id"]
        if observations[f"apfs-stat-node-{node_id}"]["stdout"].strip().lower() != "apfs":
            fail(f"node {node_id} data directory is not on APFS")
        if "apfs" not in observations[f"apfs-diskutil-node-{node_id}"]["stdout"].lower():
            fail(f"node {node_id} diskutil observation does not identify APFS")
        launchd_output = observations[f"launchd-node-{node_id}"]["stdout"]
        if re.search(rf"(^|\s)pid = {raw_node['pid']}(\s|$)", launchd_output) is None:
            fail(f"node {node_id} launchd observation does not bind PID {raw_node['pid']}")
    observed_nodes: list[dict[str, Any]] = []
    cpu_baselines: dict[int, tuple[int, int]] = {}
    for raw_node in nodes:
        observed = darwin_process_observation(raw_node["pid"])
        observed_at_ns = time.monotonic_ns()
        executable_path = os.path.realpath(str(binary_path))
        if observed["executable_path"] != executable_path:
            fail(f"PID {raw_node['pid']} does not execute the expected binary")
        raw_node = dict(raw_node)
        raw_node["process_start_token"] = observed["process_start_token"]
        raw_node["executable_path"] = executable_path
        cpu_baselines[raw_node["pid"]] = (observed["cpu_user_time_ns"] + observed["cpu_system_time_ns"], observed_at_ns)
        observed_nodes.append(raw_node)
    return observations, observed_nodes, cpu_baselines


def collect(config_path: Path, out_dir: Path) -> Path:
    validate_schema_contract()
    config = validate_collector_config(load_json(config_path, "collector config"))
    out_dir.mkdir(parents=True, exist_ok=True)
    root = out_dir.resolve(strict=True)
    if out_dir.is_symlink():
        fail("collector output directory must not be a symlink")
    for existing in root.iterdir():
        fail(f"collector output directory must be empty: found {existing.name}")
    binding = config["target"]
    binary_path = Path(config["binary_path"]).resolve(strict=True)
    observations, nodes, cpu_baselines = validate_native_host(binary_path, config["nodes"], binding)
    evidence_binary = root / "kvnode"
    shutil.copyfile(binary_path, evidence_binary)
    evidence_binary.chmod(0o500)
    if sha256_file(evidence_binary) != binding["binary_sha256"]:
        fail("archived evidence binary does not match target.binary_sha256")
    plan_source = Path(config["pre_run_plan_path"]).resolve(strict=True)
    plan_path = root / "pre-run-plan.json"
    shutil.copyfile(plan_source, plan_path)
    environment_path = root / "environment-observations.json"
    phases_path = root / "phase-windows.json"
    requests_path = root / "request-samples.jsonl"
    samples_path = root / "process-samples.jsonl"
    disk_raw_path = root / "disk-latency-apfs-fsync-raw.jsonl"
    disk_samples_path = root / "disk-latency-samples.jsonl"
    measurement_path = root / "measurement.json"
    environment = {
        "schema": "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-environment-v2.json",
        "binding": {"target": binding, "verifier_version": VERIFIER_VERSION},
        "execution_mode": "native",
        "platform": "darwin",
        "architecture": "arm64",
        "binary_format": "mach-o-64-arm64",
        "filesystem": "apfs",
        "network_scope": "same-host-loopback",
        "tls_scope": "server-auth-only",
        "tls_ca_sha256": sha256_file(Path(config["tls_ca_path"])),
        "observed_commands": list(observations.values()),
    }
    write_json(environment_path, environment)
    ssl_context = ssl.create_default_context(cafile=config["tls_ca_path"])
    context = CollectorContext(
        config=config,
        root=root,
        binding=binding,
        nodes=nodes,
        ssl_context=ssl_context,
        timeout_seconds=float(config["request_timeout_seconds"]),
        phase_lock=threading.Lock(),
        phase="preflight",
        sample_rows=[],
        disk_rows=[],
        disk_raw_rows=[],
        probe_token=f"{int(time.time())}-{os.getpid()}",
        observed_peaks={},
        previous_cpu=cpu_baselines,
        sample_lock=threading.Lock(),
    )
    stop = threading.Event()
    sampler_errors: list[BaseException] = []
    sample_thread = threading.Thread(
        target=sample_loop,
        args=(context, stop, float(config["sample_interval_seconds"]), sampler_errors),
        daemon=True,
    )
    sample_thread.start()
    phase_windows: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    started_utc = utc_now()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    try:
        for phase in PHASES:
            if phase == "recovery-observation" and config["recovery_wait_seconds"]:
                context.set_phase("between-phases")
                time.sleep(float(config["recovery_wait_seconds"]))
            with context.sample_lock:
                context.set_phase(phase)
                phase_started_utc = utc_now()
                phase_started_ns = time.monotonic_ns()
                context._sample_once_unlocked(phase)
            phase_config = config["phases"][phase]
            rows = run_phase_workload(context, phase, phase_config["operations"], phase_config["concurrency"], run_id)
            with context.sample_lock:
                context._sample_once_unlocked(phase)
                phase_completed_ns = time.monotonic_ns()
                phase_completed_utc = utc_now()
                context.set_phase("between-phases")
            request_rows.extend(rows)
            phase_windows.append(
                {
                    "binding": context.binding_row(),
                    "phase": phase,
                    "planned_operations": phase_config["operations"],
                    "concurrency": phase_config["concurrency"],
                    "started_utc": phase_started_utc,
                    "completed_utc": phase_completed_utc,
                    "started_monotonic_ns": phase_started_ns,
                    "completed_monotonic_ns": phase_completed_ns,
                }
            )
            if sampler_errors:
                raise sampler_errors[0]
    finally:
        context.set_phase("complete")
        stop.set()
        sample_thread.join(timeout=20)
        if sample_thread.is_alive():
            fail("bounded process sampler did not stop")
    if sampler_errors:
        raise sampler_errors[0]
    if not context.disk_rows or not context.disk_raw_rows:
        fail("APFS fsync probes yielded no disk latency samples")
    completed_utc = utc_now()
    write_json(phases_path, phase_windows)
    write_jsonl(requests_path, request_rows)
    write_jsonl(samples_path, context.sample_rows)
    write_jsonl(disk_raw_path, context.disk_raw_rows)
    write_jsonl(disk_samples_path, context.disk_rows)
    artifacts = [
        artifact_entry(root, environment_path, "environment-observations"),
        artifact_entry(root, plan_path, "pre-run-plan"),
        artifact_entry(root, phases_path, "phase-windows"),
        artifact_entry(root, requests_path, "request-samples"),
        artifact_entry(root, samples_path, "process-samples"),
        artifact_entry(root, disk_raw_path, "disk-latency-raw"),
        artifact_entry(root, disk_samples_path, "disk-latency-samples"),
    ]
    native_environment = {
        "platform": "darwin",
        "architecture": "arm64",
        "binary_format": "mach-o-64-arm64",
        "execution_mode": "native",
        "supervisor": "launchd",
        "launchd_domain": "system",
        "filesystem": "apfs",
        "network_scope": "same-host-loopback",
        "tls_scope": "server-auth-only",
        "host_id": platform.node(),
        "os_version": platform.mac_ver()[0],
        "kernel_release": platform.release(),
        "tls_ca_sha256": sha256_file(Path(config["tls_ca_path"])),
    }
    summary = recompute_summary(
        binding,
        nodes,
        config["resource_envelope"],
        float(config["sample_interval_seconds"]),
        phase_windows,
        request_rows,
        context.sample_rows,
        context.disk_rows,
    )
    measurement = {
        "schema": MEASUREMENT_SCHEMA_URI,
        "verifier_version": VERIFIER_VERSION,
        "evidence_class": "workstation-capacity-characterization",
        "production_capacity_certification": False,
        "claim_scope": "native-darwin-same-host-bounded-characterization",
        "operator_identity": config["operator_identity"],
        "target": binding,
        "native_environment": native_environment,
        "nodes": nodes,
        "resource_envelope": config["resource_envelope"],
        "sampling": {
            "interval_seconds": config["sample_interval_seconds"],
            "started_utc": started_utc,
            "completed_utc": completed_utc,
            "phase_order": list(PHASES),
            "measurement_phase_order": list(MEASUREMENT_PHASES),
            "sample_count": len(context.sample_rows),
            "sample_provenance": SAMPLE_PROVENANCE,
        },
        "summary": summary,
        "artifacts": artifacts,
        "nonclaims": config["nonclaims"],
    }
    write_json(measurement_path, measurement)
    validate_measurement(measurement, root, measurement_path)
    return measurement_path


def validate_native_environment(value: Any, expected_tls_scope: str = "server-auth-only") -> dict[str, Any]:
    required = {
        "platform",
        "architecture",
        "binary_format",
        "execution_mode",
        "supervisor",
        "launchd_domain",
        "filesystem",
        "network_scope",
        "tls_scope",
        "host_id",
        "os_version",
        "kernel_release",
        "tls_ca_sha256",
    }
    environment = exact_object(value, required, "native_environment")
    expected = {
        "platform": "darwin",
        "architecture": "arm64",
        "binary_format": "mach-o-64-arm64",
        "execution_mode": "native",
        "supervisor": "launchd",
        "launchd_domain": "system",
        "filesystem": "apfs",
        "network_scope": "same-host-loopback",
        "tls_scope": expected_tls_scope,
    }
    for key, wanted in expected.items():
        if environment[key] != wanted:
            fail(f"native_environment.{key} must equal {wanted}")
    for key in ("host_id", "os_version", "kernel_release"):
        require_string(environment[key], f"native_environment.{key}", maximum=128)
    if re.fullmatch(r"24\.[0-9]+\.[0-9]+", environment["kernel_release"]) is None:
        fail("native_environment.kernel_release must identify Darwin 24")
    require_sha256(environment["tls_ca_sha256"], "native_environment.tls_ca_sha256")
    return environment


def validate_nodes(value: Any, binding: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) != 3:
        fail("nodes must contain exactly three entries")
    seen_ids: set[int] = set()
    seen_pids: set[int] = set()
    seen_urls: set[str] = set()
    seen_data_dirs: set[str] = set()
    nodes: list[dict[str, Any]] = []
    for index, raw in enumerate(value, 1):
        node = exact_object(
            raw,
            {"node_id", "launchd_label", "pid", "process_start_token", "executable_path", "data_dir", "client_url", "admin_url"},
            f"nodes[{index}]",
        )
        node_id = require_int(node["node_id"], f"nodes[{index}].node_id", minimum=1, maximum=3)
        expected_label = f"org.gosuda.moreconsensus.kvnode.{node_id}"
        if node["launchd_label"] != expected_label:
            fail(f"nodes[{index}].launchd_label must equal {expected_label}")
        pid = require_int(node["pid"], f"nodes[{index}].pid", minimum=1)
        require_int(node["process_start_token"], f"nodes[{index}].process_start_token", minimum=1)
        if node_id in seen_ids or pid in seen_pids:
            fail("nodes contain duplicate node IDs or PIDs")
        seen_ids.add(node_id)
        seen_pids.add(pid)
        for key in ("executable_path", "data_dir"):
            path = require_string(node[key], f"nodes[{index}].{key}", maximum=1024)
            if not path.startswith("/"):
                fail(f"nodes[{index}].{key} must be absolute")
            if key == "data_dir":
                if path in seen_data_dirs:
                    fail("nodes contain duplicate APFS data directories")
                seen_data_dirs.add(path)
        for key in ("client_url", "admin_url"):
            url = require_string(node[key], f"nodes[{index}].{key}")
            if re.fullmatch(r"https://127\.0\.0\.1:[0-9]{1,5}", url) is None:
                fail(f"nodes[{index}].{key} must be loopback HTTPS")
            if url in seen_urls:
                fail("nodes contain duplicate client or admin URLs")
            seen_urls.add(url)
        nodes.append(node)
    if seen_ids != {1, 2, 3}:
        fail("node IDs must be exactly 1,2,3")
    return sorted(nodes, key=lambda node: node["node_id"])


def validate_limit(value: Any, label: str, unit: str, *, allow_zero: bool = False, observational_isolation: bool = False) -> float:
    optional = {"isolation_status", "enforcement_artifact"} if observational_isolation else set()
    limit = exact_object(value, {"unit", "limit"}, label, optional)
    if limit["unit"] != unit:
        fail(f"{label}.unit must equal {unit}")
    return require_number(limit["limit"], f"{label}.limit", minimum=0, strict_minimum=not allow_zero)


def validate_resource_envelope(value: Any, binding: dict[str, Any], started: datetime | None) -> dict[str, Any]:
    required = {
        "observed",
        "approved_before_run_utc",
        "plan_sha256",
        "cpu",
        "memory",
        "open_fds",
        "apfs_allocated_growth",
        "disk_latency_p99",
        "request_throughput_minimum",
        "request_latency_p99",
        "request_error_rate_maximum_percent",
        "send_queue_depth",
    }
    envelope = exact_object(value, required, "resource_envelope")
    if envelope["observed"] is not True:
        fail("resource_envelope.observed must be true")
    approved = parse_utc(envelope["approved_before_run_utc"], "resource_envelope.approved_before_run_utc")
    if started is not None and approved >= started:
        fail("resource envelope was not approved before the run")
    require_sha256(envelope["plan_sha256"], "resource_envelope.plan_sha256")
    validate_limit(envelope["cpu"], "resource_envelope.cpu", "percent", observational_isolation=True)
    validate_limit(envelope["memory"], "resource_envelope.memory", "bytes", observational_isolation=True)
    for name in ("cpu", "memory"):
        isolation = envelope[name]
        if isolation.get("isolation_status") != "none-observation-only" or isolation.get("enforcement_artifact", "missing") is not None:
            fail(f"Darwin v2 {name} isolation must be an explicit observation-only non-claim")
    validate_limit(envelope["open_fds"], "resource_envelope.open_fds", "count")
    validate_limit(envelope["apfs_allocated_growth"], "resource_envelope.apfs_allocated_growth", "bytes")
    validate_limit(envelope["disk_latency_p99"], "resource_envelope.disk_latency_p99", "nanoseconds")
    validate_limit(envelope["request_throughput_minimum"], "resource_envelope.request_throughput_minimum", "operations-per-second")
    validate_limit(envelope["request_latency_p99"], "resource_envelope.request_latency_p99", "nanoseconds")
    error_limit = validate_limit(
        envelope["request_error_rate_maximum_percent"],
        "resource_envelope.request_error_rate_maximum_percent",
        "percent",
        allow_zero=True,
    )
    if error_limit > 100:
        fail("request error rate envelope exceeds 100 percent")
    validate_limit(envelope["send_queue_depth"], "resource_envelope.send_queue_depth", "count")
    return envelope


def validate_plan(value: Any, binding: dict[str, Any], envelope: dict[str, Any], operator: str, started: datetime | None) -> dict[str, Any]:
    plan = exact_object(
        value,
        {"schema", "verifier_version", "binding", "operator_identity", "approved_at_utc", "phase_order", "phases", "resource_envelope", "host_policy"},
        "pre-run plan",
    )
    if plan["schema"] != "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-pre-run-plan-v2.json":
        fail("pre-run plan schema is invalid")
    sample_binding(plan["binding"], binding, "pre-run plan.binding")
    if plan["verifier_version"] != VERIFIER_VERSION:
        fail("pre-run plan verifier_version is invalid")
    if plan["operator_identity"] != operator:
        fail("pre-run plan operator does not match measurement operator")
    approved = parse_utc(plan["approved_at_utc"], "pre-run plan.approved_at_utc")
    if approved != parse_utc(envelope["approved_before_run_utc"], "resource envelope approval"):
        fail("pre-run plan approval time does not match resource envelope")
    if started is not None and approved >= started:
        fail("pre-run plan was not approved before measurement")
    if plan["phase_order"] != list(PHASES):
        fail("pre-run plan phase order is invalid")
    phases = exact_object(plan["phases"], set(PHASES), "pre-run plan.phases")
    for phase in PHASES:
        phase_value = exact_object(phases[phase], {"operations", "concurrency"}, f"pre-run plan phase {phase}")
        require_int(phase_value["operations"], f"pre-run plan phase {phase}.operations", minimum=1, maximum=1000)
        require_int(phase_value["concurrency"], f"pre-run plan phase {phase}.concurrency", minimum=1, maximum=64)
    planned_envelope = {key: item for key, item in envelope.items() if key != "plan_sha256"}
    if plan["resource_envelope"] != planned_envelope:
        fail("pre-run plan resource envelope does not match the record")
    host_policy = exact_object(plan["host_policy"], {"power", "sleep", "competing_workload"}, "pre-run plan.host_policy")
    if host_policy != {"power": "ac-required", "sleep": "forbidden-during-run", "competing_workload": "forbidden-undeclared"}:
        fail("pre-run plan host policy is not the bounded Darwin policy")
    return plan


def validate_environment_artifact(value: Any, binding: dict[str, Any], nodes: list[dict[str, Any]]) -> dict[str, Any]:
    environment = exact_object(
        value,
        {"schema", "binding", "execution_mode", "platform", "architecture", "binary_format", "filesystem", "network_scope", "tls_scope", "tls_ca_sha256", "observed_commands"},
        "environment observations",
    )
    if environment["schema"] != "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-environment-v2.json":
        fail("environment observations schema is invalid")
    sample_binding(environment["binding"], binding, "environment observations.binding")
    expected = {
        "execution_mode": "native",
        "platform": "darwin",
        "architecture": "arm64",
        "binary_format": "mach-o-64-arm64",
        "filesystem": "apfs",
        "network_scope": "same-host-loopback",
        "tls_scope": "server-auth-only",
    }
    for key, wanted in expected.items():
        if environment[key] != wanted:
            fail(f"environment observations {key} is invalid")
    require_sha256(environment["tls_ca_sha256"], "environment observations.tls_ca_sha256")
    commands = environment["observed_commands"]
    if not isinstance(commands, list) or len(commands) < 15:
        fail("environment observations must retain native host, binary, three-node APFS, and launchd commands/results")
    observed_binaries: set[str] = set()
    observed_launchd_labels: set[str] = set()
    expected_launchd_pids = {node["launchd_label"]: node["pid"] for node in nodes}
    expected_data_dirs = {node["data_dir"] for node in nodes}
    observed_stat_dirs: set[str] = set()
    observed_diskutil_dirs: set[str] = set()
    file_paths: set[str] = set()
    go_paths: set[str] = set()
    go_build_output = ""
    for index, raw_command in enumerate(commands, 1):
        command = exact_object(raw_command, {"argv", "returncode", "stdout", "stderr"}, f"observed command {index}")
        argv = command["argv"]
        if not isinstance(argv, list) or not argv or any(not isinstance(part, str) or not part for part in argv):
            fail(f"observed command {index}.argv is invalid")
        command_text = " ".join(argv).lower()
        if any(term in command_text for term in FORBIDDEN_OBSERVER_TERMS):
            fail(f"observed command {index} uses a prohibited Linux/container observer")
        require_int(command["returncode"], f"observed command {index}.returncode", minimum=0, maximum=255)
        if command["returncode"] != 0:
            fail(f"observed command {index} did not succeed")
        stdout = require_string(command["stdout"], f"observed command {index}.stdout", maximum=100_000)
        if not isinstance(command["stderr"], str) or len(command["stderr"]) > 100_000:
            fail(f"observed command {index}.stderr is invalid")
        binary = Path(argv[0]).name
        observed_binaries.add(binary)
        if binary == "file":
            if len(argv) != 2 or "mach-o" not in stdout.lower() or "arm64" not in stdout.lower():
                fail("environment file observation does not prove the observed Mach-O arm64 binary")
            file_paths.add(argv[1])
        if binary == "stat":
            if len(argv) != 4 or argv[1:3] != ["-f", "%T"] or argv[3] not in expected_data_dirs or stdout.strip().lower() != "apfs":
                fail("environment stat observation does not bind an exact APFS data directory")
            observed_stat_dirs.add(argv[3])
        if binary == "diskutil":
            if len(argv) != 3 or argv[1] != "info" or argv[2] not in expected_data_dirs or "apfs" not in stdout.lower():
                fail("environment diskutil observation does not bind an exact APFS data directory")
            observed_diskutil_dirs.add(argv[2])
        if binary == "go":
            go_build_output += stdout
            if len(argv) != 4 or argv[1:3] != ["version", "-m"]:
                fail("environment Go build observation command is invalid")
            go_paths.add(argv[3])
        if binary == "sysctl":
            if argv[1:] != ["-n", "kern.hv_vmm_present"] or stdout.strip() != "0":
                fail("environment virtualization observation is not the exact native zero observation")
        if binary == "launchctl" and len(argv) == 3 and argv[1] == "print" and argv[2].startswith("system/"):
            label = argv[2].removeprefix("system/")
            expected_pid = expected_launchd_pids.get(label)
            if expected_pid is not None and re.search(rf"(^|\s)pid = {expected_pid}(\s|$)", stdout):
                observed_launchd_labels.add(label)
    if not {"uname", "sw_vers", "file", "go", "sysctl", "system_profiler", "stat", "diskutil", "launchctl"}.issubset(observed_binaries):
        fail("environment observations omit required native commands")
    if observed_stat_dirs != expected_data_dirs or observed_diskutil_dirs != expected_data_dirs:
        fail("environment observations do not prove every exact APFS data directory")
    if len(file_paths) != 1 or file_paths != go_paths:
        fail("environment file and Go build observations do not bind the same exact binary path")
    if observed_launchd_labels != set(expected_launchd_pids):
        fail("environment observations do not prove the three exact system-domain launchd labels and PIDs")
    if binding["source_revision"] not in go_build_output or "vcs.modified=false" not in go_build_output:
        fail("environment observations do not bind binary build provenance to source_revision")
    return environment


def validate_phase_windows(rows: Any, binding: dict[str, Any], plan: dict[str, Any], started: datetime, completed: datetime) -> tuple[list[dict[str, Any]], dict[str, tuple[int, int]]]:
    if not isinstance(rows, list) or len(rows) != len(PHASES):
        fail("phase-windows must contain exactly five phases")
    windows: dict[str, tuple[int, int]] = {}
    previous_end = -1
    for index, phase in enumerate(PHASES):
        row = exact_object(
            rows[index],
            {"binding", "phase", "planned_operations", "concurrency", "started_utc", "completed_utc", "started_monotonic_ns", "completed_monotonic_ns"},
            f"phase-windows[{index + 1}]",
        )
        sample_binding(row["binding"], binding, f"phase-windows[{index + 1}].binding")
        if row["phase"] != phase:
            fail("phase-windows are missing, reordered, or duplicated")
        if row["planned_operations"] != plan["phases"][phase]["operations"] or row["concurrency"] != plan["phases"][phase]["concurrency"]:
            fail(f"phase-windows {phase} does not match the pre-run plan")
        row_start_utc = parse_utc(row["started_utc"], f"phase-windows {phase}.started_utc")
        row_end_utc = parse_utc(row["completed_utc"], f"phase-windows {phase}.completed_utc")
        if not (started <= row_start_utc < row_end_utc <= completed):
            fail(f"phase-windows {phase} UTC range is invalid")
        row_start = require_int(row["started_monotonic_ns"], f"phase-windows {phase}.started_monotonic_ns", minimum=1)
        row_end = require_int(row["completed_monotonic_ns"], f"phase-windows {phase}.completed_monotonic_ns", minimum=1)
        if row_start >= row_end or row_start < previous_end:
            fail("phase windows overlap or are reversed")
        previous_end = row_end
        windows[phase] = (row_start, row_end)
    return rows, windows


def validate_request_rows(rows: list[dict[str, Any]], binding: dict[str, Any], windows: dict[str, tuple[int, int]], plan: dict[str, Any]) -> None:
    counts = {phase: 0 for phase in PHASES}
    indexes: dict[str, set[int]] = {phase: set() for phase in PHASES}
    for index, row in enumerate(rows, 1):
        request = exact_object(
            row,
            {"binding", "phase", "worker", "operation_index", "operation", "node_id", "started_utc", "started_monotonic_ns", "ended_monotonic_ns", "latency_ns", "http_status", "outcome"},
            f"request sample {index}",
        )
        sample_binding(request["binding"], binding, f"request sample {index}.binding")
        phase = request["phase"]
        if phase not in PHASES:
            fail(f"request sample {index} has an invalid phase")
        counts[phase] += 1
        worker = require_int(request["worker"], f"request sample {index}.worker", minimum=1, maximum=64)
        operation_index = require_int(request["operation_index"], f"request sample {index}.operation_index", minimum=0)
        if operation_index in indexes[phase]:
            fail(f"request sample {index} duplicates a phase operation index")
        indexes[phase].add(operation_index)
        concurrency = plan["phases"][phase]["concurrency"]
        if worker != operation_index % concurrency + 1:
            fail(f"request sample {index} worker does not match deterministic partitioning")
        expected_operation = ("put", "get", "scan")[(operation_index // concurrency) % 3]
        if request["operation"] != expected_operation:
            fail(f"request sample {index} operation does not match deterministic worker sequence")
        require_int(request["node_id"], f"request sample {index}.node_id", minimum=1, maximum=3)
        parse_utc(request["started_utc"], f"request sample {index}.started_utc")
        request_start = require_int(request["started_monotonic_ns"], f"request sample {index}.started_monotonic_ns", minimum=1)
        request_end = require_int(request["ended_monotonic_ns"], f"request sample {index}.ended_monotonic_ns", minimum=1)
        latency = require_int(request["latency_ns"], f"request sample {index}.latency_ns", minimum=1)
        if request_end <= request_start or request_end - request_start != latency:
            fail(f"request sample {index} has inconsistent nanosecond latency")
        window_start, window_end = windows[phase]
        if not (window_start <= request_start < request_end <= window_end):
            fail(f"request sample {index} lies outside its phase window")
        status = require_int(request["http_status"], f"request sample {index}.http_status", minimum=0, maximum=599)
        if request["outcome"] not in {"success", "http-error", "transport-error"}:
            fail(f"request sample {index}.outcome is invalid")
        if (200 <= status < 300) != (request["outcome"] == "success"):
            fail(f"request sample {index} status and outcome disagree")
    for phase in PHASES:
        planned = plan["phases"][phase]["operations"]
        if indexes[phase] != set(range(planned)):
            fail(f"request samples for {phase} do not contain the exact planned operation index set")
        if counts[phase] != planned or planned <= 0:
            fail(f"request samples do not complete nonzero plan for {phase}")


def validate_process_rows(rows: list[dict[str, Any]], binding: dict[str, Any], nodes: list[dict[str, Any]], windows: dict[str, tuple[int, int]], interval: float) -> None:
    expected_by_pid = {node["pid"]: node for node in nodes}
    previous_counters: dict[int, tuple[int, int, int, int, int, int]] = {}
    phase_times: dict[str, list[int]] = {phase: [] for phase in PHASES}
    prior_timestamp = -1
    for index, row in enumerate(rows, 1):
        sample = exact_object(row, {"binding", "sample_index", "timestamp_utc", "monotonic_ns", "phase", "provenance", "nodes", "data_dirs"}, f"process sample {index}")
        sample_binding(sample["binding"], binding, f"process sample {index}.binding")
        if sample["sample_index"] != index:
            fail("process sample indexes are not contiguous")
        parse_utc(sample["timestamp_utc"], f"process sample {index}.timestamp_utc")
        monotonic = require_int(sample["monotonic_ns"], f"process sample {index}.monotonic_ns", minimum=1)
        if monotonic <= prior_timestamp:
            fail("process sample monotonic timestamps are not strictly increasing")
        prior_timestamp = monotonic
        phase = sample["phase"]
        if phase not in PHASES:
            fail(f"process sample {index}.phase is invalid")
        window_start, window_end = windows[phase]
        if not (window_start <= monotonic <= window_end):
            fail(f"process sample {index} lies outside its phase")
        phase_times[phase].append(monotonic)
        if sample["provenance"] != SAMPLE_PROVENANCE:
            fail(f"process sample {index} provenance is invalid")
        raw_nodes = sample["nodes"]
        if not isinstance(raw_nodes, list) or len(raw_nodes) != 3:
            fail(f"process sample {index} must bind exactly three PIDs")
        seen_pids: set[int] = set()
        for raw_node in raw_nodes:
            node_sample = exact_object(
                raw_node,
                {"node_id", "pid", "process_start_token", "executable_path", "observed_binary_sha256", "cpu_user_time_ns", "cpu_system_time_ns", "cpu_utilization_percent", "rss_bytes", "physical_footprint_bytes", "observed_peak_rss_bytes", "open_fd_count", "disk_read_bytes", "disk_written_bytes", "ready_http_status", "send_queue_depth", "epaxos_instances", "epaxos_executed", "process_restart_count"},
                f"process sample {index} node",
            )
            pid = require_int(node_sample["pid"], f"process sample {index}.pid", minimum=1)
            if pid in seen_pids or pid not in expected_by_pid:
                fail(f"process sample {index} has duplicate or unexpected PID")
            seen_pids.add(pid)
            expected = expected_by_pid[pid]
            if node_sample["node_id"] != expected["node_id"] or node_sample["process_start_token"] != expected["process_start_token"] or node_sample["executable_path"] != expected["executable_path"]:
                fail(f"process sample {index} proves PID reuse or executable mismatch")
            if node_sample["observed_binary_sha256"] != binding["binary_sha256"]:
                fail(f"process sample {index} binary hash mismatch")
            user = require_int(node_sample["cpu_user_time_ns"], f"process sample {index}.cpu_user_time_ns", minimum=0)
            system = require_int(node_sample["cpu_system_time_ns"], f"process sample {index}.cpu_system_time_ns", minimum=0)
            require_number(node_sample["cpu_utilization_percent"], f"process sample {index}.cpu_utilization_percent", minimum=0)
            rss = require_int(node_sample["rss_bytes"], f"process sample {index}.rss_bytes", minimum=1)
            require_int(node_sample["physical_footprint_bytes"], f"process sample {index}.physical_footprint_bytes", minimum=1)
            peak = require_int(node_sample["observed_peak_rss_bytes"], f"process sample {index}.observed_peak_rss_bytes", minimum=1)
            fds = require_int(node_sample["open_fd_count"], f"process sample {index}.open_fd_count", minimum=1)
            disk_read = require_int(node_sample["disk_read_bytes"], f"process sample {index}.disk_read_bytes", minimum=0)
            disk_write = require_int(node_sample["disk_written_bytes"], f"process sample {index}.disk_written_bytes", minimum=0)
            if peak < rss:
                fail(f"process sample {index} observed peak RSS is below current RSS")
            if node_sample["ready_http_status"] != 200:
                fail(f"process sample {index} readiness failed")
            queue = require_int(node_sample["send_queue_depth"], f"process sample {index}.send_queue_depth", minimum=0)
            instances = require_int(node_sample["epaxos_instances"], f"process sample {index}.epaxos_instances", minimum=0)
            executed = require_int(node_sample["epaxos_executed"], f"process sample {index}.epaxos_executed", minimum=0)
            if executed > instances:
                fail(f"process sample {index} executed count exceeds instance count")
            if node_sample["process_restart_count"] != 0:
                fail(f"process sample {index} reports a process restart")
            current = (user, system, peak, disk_read, disk_write, instances, executed)
            previous = previous_counters.get(pid)
            if previous is not None and any(now < before for now, before in zip(current, previous)):
                fail(f"process sample {index} contains an impossible decreasing counter")
            previous_counters[pid] = current
        if seen_pids != set(expected_by_pid):
            fail(f"process sample {index} does not bind all exact PIDs")
        data_dirs = sample["data_dirs"]
        if not isinstance(data_dirs, list) or len(data_dirs) != 3:
            fail(f"process sample {index} must contain three APFS directory observations")
        seen_data_ids: set[int] = set()
        for data in data_dirs:
            data_row = exact_object(data, {"node_id", "data_dir", "apfs_allocated_bytes", "apfs_logical_bytes"}, f"process sample {index} data dir")
            node_id = require_int(data_row["node_id"], f"process sample {index} data node_id", minimum=1, maximum=3)
            expected = next(node for node in nodes if node["node_id"] == node_id)
            if node_id in seen_data_ids or data_row["data_dir"] != expected["data_dir"]:
                fail(f"process sample {index} data directory binding is invalid")
            seen_data_ids.add(node_id)
            allocated = require_int(data_row["apfs_allocated_bytes"], f"process sample {index}.apfs_allocated_bytes", minimum=0)
            logical = require_int(data_row["apfs_logical_bytes"], f"process sample {index}.apfs_logical_bytes", minimum=0)
            if logical < 0 or allocated < 0:
                fail("APFS byte observations cannot be negative")
    max_gap_ns = math.ceil(interval * 2.5 * 1_000_000_000)
    for phase, times in phase_times.items():
        if len(times) < 2:
            fail(f"process samples are missing bounded observations for {phase}")
        for before, after in zip(times, times[1:]):
            if after - before > max_gap_ns:
                fail(f"process sampling interval was exceeded in {phase}")


def validate_disk_rows(raw_rows: list[dict[str, Any]], rows: list[dict[str, Any]], binding: dict[str, Any], nodes: list[dict[str, Any]], windows: dict[str, tuple[int, int]]) -> None:
    if len(raw_rows) != len(rows):
        fail("raw and normalized APFS fsync probe counts differ")
    expected_by_id = {node["node_id"]: node for node in nodes}
    observed_phase_nodes: set[tuple[str, int]] = set()
    for index, (raw_row, row) in enumerate(zip(raw_rows, rows), 1):
        sample = exact_object(row, {"binding", "timestamp_utc", "monotonic_ns", "phase", "node_id", "data_dir", "latency_ns", "provenance"}, f"disk latency sample {index}")
        raw = exact_object(raw_row, {"binding", "timestamp_utc", "phase", "node_id", "data_dir", "probe_bytes", "fsync_started_monotonic_ns", "fsync_completed_monotonic_ns", "status", "provenance"}, f"raw disk latency sample {index}")
        sample_binding(sample["binding"], binding, f"disk latency sample {index}.binding")
        sample_binding(raw["binding"], binding, f"raw disk latency sample {index}.binding")
        parse_utc(sample["timestamp_utc"], f"disk latency sample {index}.timestamp_utc")
        parse_utc(raw["timestamp_utc"], f"raw disk latency sample {index}.timestamp_utc")
        phase = sample["phase"]
        if phase not in PHASES or raw["phase"] != phase:
            fail(f"disk latency sample {index}.phase is invalid or disagrees with raw probe")
        monotonic = require_int(sample["monotonic_ns"], f"disk latency sample {index}.monotonic_ns", minimum=1)
        start, end = windows[phase]
        if not (start <= monotonic <= end):
            fail(f"disk latency sample {index} lies outside its phase")
        node_id = require_int(sample["node_id"], f"disk latency sample {index}.node_id", minimum=1, maximum=3)
        expected = expected_by_id.get(node_id)
        if expected is None or sample["data_dir"] != expected["data_dir"] or raw["node_id"] != node_id or raw["data_dir"] != expected["data_dir"]:
            fail(f"disk latency sample {index} has an unexpected node or APFS data directory")
        latency = require_int(sample["latency_ns"], f"disk latency sample {index}.latency_ns", minimum=1)
        raw_start = require_int(raw["fsync_started_monotonic_ns"], f"raw disk latency sample {index}.fsync_started_monotonic_ns", minimum=1)
        raw_end = require_int(raw["fsync_completed_monotonic_ns"], f"raw disk latency sample {index}.fsync_completed_monotonic_ns", minimum=1)
        require_int(raw["probe_bytes"], f"raw disk latency sample {index}.probe_bytes", minimum=1)
        if raw["status"] != "ok" or raw_end != monotonic or raw_end - raw_start != latency:
            fail(f"disk latency sample {index} does not match its successful raw fsync probe")
        if sample["provenance"] != "darwin-apfs-fsync-probe-per-data-dir" or raw["provenance"] != sample["provenance"]:
            fail(f"disk latency sample {index} provenance is invalid")
        observed_phase_nodes.add((phase, node_id))
    expected_phase_nodes = {(phase, node_id) for phase in PHASES for node_id in (1, 2, 3)}
    if not expected_phase_nodes.issubset(observed_phase_nodes):
        fail("APFS fsync latency samples do not cover every node data directory in every phase")


def recompute_summary(binding: dict[str, Any], nodes: list[dict[str, Any]], envelope: dict[str, Any], interval: float, phase_rows: list[dict[str, Any]], request_rows: list[dict[str, Any]], process_rows: list[dict[str, Any]], disk_rows: list[dict[str, Any]]) -> dict[str, Any]:
    measurement_requests = [row for row in request_rows if row["phase"] in MEASUREMENT_PHASES]
    warmup_requests = [row for row in request_rows if row["phase"] == "warmup"]
    if not measurement_requests or not warmup_requests:
        fail("zero-work capacity summaries are forbidden")
    successful = [row for row in measurement_requests if row["outcome"] == "success"]
    errors = len(measurement_requests) - len(successful)
    if not successful:
        fail("successful-request percentiles require successful measured requests")
    measured_duration_ns = sum(
        row["completed_monotonic_ns"] - row["started_monotonic_ns"]
        for row in phase_rows
        if row["phase"] in MEASUREMENT_PHASES
    )
    if measured_duration_ns <= 0:
        fail("measured phase duration must be positive")
    first_nodes: dict[int, dict[str, Any]] = {}
    last_nodes: dict[int, dict[str, Any]] = {}
    max_cpu = 0.0
    max_rss = 0
    max_peak = 0
    max_fds = 0
    max_queue = 0
    first_allocated: dict[int, int] = {}
    max_allocated: dict[int, int] = {}
    for row in process_rows:
        for node in row["nodes"]:
            pid = node["pid"]
            first_nodes.setdefault(pid, node)
            last_nodes[pid] = node
            max_cpu = max(max_cpu, float(node["cpu_utilization_percent"]))
            max_rss = max(max_rss, node["rss_bytes"])
            max_peak = max(max_peak, node["observed_peak_rss_bytes"])
            max_fds = max(max_fds, node["open_fd_count"])
            max_queue = max(max_queue, node["send_queue_depth"])
        for data in row["data_dirs"]:
            node_id = data["node_id"]
            first_allocated.setdefault(node_id, data["apfs_allocated_bytes"])
            max_allocated[node_id] = max(max_allocated.get(node_id, 0), data["apfs_allocated_bytes"])
    disk_read = sum(last_nodes[pid]["disk_read_bytes"] - first_nodes[pid]["disk_read_bytes"] for pid in first_nodes)
    disk_write = sum(last_nodes[pid]["disk_written_bytes"] - first_nodes[pid]["disk_written_bytes"] for pid in first_nodes)
    max_growth = max(max_allocated[node_id] - first_allocated[node_id] for node_id in first_allocated)
    latencies = [row["latency_ns"] for row in successful]
    disk_latencies = [row["latency_ns"] for row in disk_rows if row["phase"] in MEASUREMENT_PHASES]
    summary = {
        "binding": {"target": binding, "verifier_version": VERIFIER_VERSION},
        "warmup_attempts": len(warmup_requests),
        "measurement_attempts": len(measurement_requests),
        "measurement_successes": len(successful),
        "measurement_errors": errors,
        "measurement_error_rate_percent": errors * 100.0 / len(measurement_requests),
        "measurement_throughput_operations_per_second": len(measurement_requests) * 1_000_000_000 / measured_duration_ns,
        "successful_request_latency_p50_ns": nearest_rank(latencies, 50),
        "successful_request_latency_p95_ns": nearest_rank(latencies, 95),
        "successful_request_latency_p99_ns": nearest_rank(latencies, 99),
        "max_cpu_utilization_percent": max_cpu,
        "max_rss_bytes": max_rss,
        "observed_peak_rss_bytes": max_peak,
        "max_open_fd_count": max_fds,
        "disk_read_bytes": disk_read,
        "disk_written_bytes": disk_write,
        "disk_latency_p99_ns": nearest_rank(disk_latencies, 99),
        "max_apfs_allocated_growth_bytes": max_growth,
        "max_send_queue_depth": max_queue,
        "ready_failure_count": sum(1 for row in process_rows for node in row["nodes"] if node["ready_http_status"] != 200),
        "process_restart_count": sum(node["process_restart_count"] for row in process_rows for node in row["nodes"]),
        "thresholds_passed": True,
    }
    threshold_pairs = (
        (summary["max_cpu_utilization_percent"], envelope["cpu"]["limit"], "maximum", "CPU"),
        (summary["observed_peak_rss_bytes"], envelope["memory"]["limit"], "maximum", "memory"),
        (summary["max_open_fd_count"], envelope["open_fds"]["limit"], "maximum", "open FDs"),
        (summary["max_apfs_allocated_growth_bytes"], envelope["apfs_allocated_growth"]["limit"], "maximum", "APFS allocated growth"),
        (summary["disk_latency_p99_ns"], envelope["disk_latency_p99"]["limit"], "maximum", "disk latency"),
        (summary["measurement_throughput_operations_per_second"], envelope["request_throughput_minimum"]["limit"], "minimum", "throughput"),
        (summary["successful_request_latency_p99_ns"], envelope["request_latency_p99"]["limit"], "maximum", "request latency"),
        (summary["measurement_error_rate_percent"], envelope["request_error_rate_maximum_percent"]["limit"], "maximum", "request error rate"),
        (summary["max_send_queue_depth"], envelope["send_queue_depth"]["limit"], "maximum", "send queue"),
    )
    for actual, limit, direction, label in threshold_pairs:
        if (direction == "maximum" and actual > limit) or (direction == "minimum" and actual < limit):
            fail(f"observed {label} is outside the approved resource envelope")
    return summary


def artifact_map(value: Any, root: Path, include_measurement: bool) -> tuple[dict[str, Path], list[dict[str, Any]]]:
    if not isinstance(value, list) or not value:
        fail("artifacts must be a nonempty array")
    result: dict[str, Path] = {}
    entries: list[dict[str, Any]] = []
    for index, raw in enumerate(value, 1):
        artifact = exact_object(raw, {"kind", "path", "sha256", "size_bytes"}, f"artifact {index}")
        kind = require_string(artifact["kind"], f"artifact {index}.kind", maximum=64)
        if kind in result:
            fail(f"artifact kind {kind} is duplicated")
        path = safe_artifact(root, artifact["path"], f"artifact {index}.path")
        expected_hash = require_sha256(artifact["sha256"], f"artifact {index}.sha256")
        expected_size = require_int(artifact["size_bytes"], f"artifact {index}.size_bytes", minimum=1)
        if sha256_file(path) != expected_hash or path.stat().st_size != expected_size:
            fail(f"artifact {index} bytes do not match their binding")
        result[kind] = path
        entries.append(artifact)
    required = REQUIRED_ARTIFACT_KINDS if include_measurement else REQUIRED_ARTIFACT_KINDS - {"measurement"}
    if set(result) != required:
        fail("artifact closure is missing required raw files or contains unexpected kinds")
    return result, entries


def validate_measurement(measurement: Any, root: Path, measurement_path: Path) -> dict[str, Any]:
    required = {
        "schema",
        "verifier_version",
        "evidence_class",
        "production_capacity_certification",
        "claim_scope",
        "operator_identity",
        "target",
        "native_environment",
        "nodes",
        "resource_envelope",
        "sampling",
        "summary",
        "artifacts",
        "nonclaims",
    }
    measurement = exact_object(measurement, required, "measurement")
    if measurement["schema"] != MEASUREMENT_SCHEMA_URI or measurement["verifier_version"] != VERIFIER_VERSION:
        fail("measurement schema or verifier version is invalid")
    if measurement["evidence_class"] != "workstation-capacity-characterization" or measurement["production_capacity_certification"] is not False:
        fail("Darwin evidence cannot claim production capacity certification")
    if measurement["claim_scope"] != "native-darwin-same-host-bounded-characterization":
        fail("measurement claim scope is invalid")
    operator = require_string(measurement["operator_identity"], "measurement.operator_identity", maximum=128)
    binding = binding_from(measurement["target"], "measurement.target")
    native_environment = validate_native_environment(measurement["native_environment"])
    nodes = validate_nodes(measurement["nodes"], binding)
    sampling = exact_object(
        measurement["sampling"],
        {"interval_seconds", "started_utc", "completed_utc", "phase_order", "measurement_phase_order", "sample_count", "sample_provenance"},
        "sampling",
    )
    interval = require_number(sampling["interval_seconds"], "sampling.interval_seconds", minimum=0.25, maximum=10)
    started = parse_utc(sampling["started_utc"], "sampling.started_utc")
    completed = parse_utc(sampling["completed_utc"], "sampling.completed_utc")
    if started >= completed:
        fail("sampling time range is reversed or empty")
    if sampling["phase_order"] != list(PHASES) or sampling["measurement_phase_order"] != list(MEASUREMENT_PHASES):
        fail("sampling phase order or warmup exclusion is invalid")
    if sampling["sample_provenance"] != SAMPLE_PROVENANCE:
        fail("sampling provenance is invalid")
    envelope = validate_resource_envelope(measurement["resource_envelope"], binding, started)
    artifacts, _ = artifact_map(measurement["artifacts"], root, include_measurement=False)
    plan_path = artifacts["pre-run-plan"]
    if sha256_file(plan_path) != envelope["plan_sha256"]:
        fail("resource envelope plan hash does not match the raw pre-run plan")
    plan = validate_plan(load_json(plan_path, "pre-run plan"), binding, envelope, operator, started)
    environment_observations = validate_environment_artifact(load_json(artifacts["environment-observations"], "environment observations"), binding, nodes)
    if environment_observations["tls_ca_sha256"] != native_environment["tls_ca_sha256"]:
        fail("native environment TLS CA fingerprint does not match raw environment observations")
    phase_rows = load_json(artifacts["phase-windows"], "phase windows")
    phase_rows, windows = validate_phase_windows(phase_rows, binding, plan, started, completed)
    request_rows = load_jsonl(artifacts["request-samples"], "request samples")
    validate_request_rows(request_rows, binding, windows, plan)
    process_rows = load_jsonl(artifacts["process-samples"], "process samples")
    if sampling["sample_count"] != len(process_rows):
        fail("sampling.sample_count does not match raw process samples")
    validate_process_rows(process_rows, binding, nodes, windows, interval)
    disk_raw_rows = load_jsonl(artifacts["disk-latency-raw"], "raw disk latency probes")
    disk_rows = load_jsonl(artifacts["disk-latency-samples"], "disk latency samples")
    validate_disk_rows(disk_raw_rows, disk_rows, binding, nodes, windows)
    recomputed = recompute_summary(binding, nodes, envelope, interval, phase_rows, request_rows, process_rows, disk_rows)
    if measurement["summary"] != recomputed:
        fail("measurement summary does not exactly match raw samples and units")
    nonclaims = measurement["nonclaims"]
    if not isinstance(nonclaims, list) or any(not isinstance(value, str) for value in nonclaims) or not REQUIRED_NONCLAIMS.issubset(set(nonclaims)):
        fail("measurement is missing mandatory capacity nonclaims")
    if len(nonclaims) != len(set(nonclaims)):
        fail("measurement nonclaims are duplicated")
    return measurement


def validate_review(review: Any, binding: dict[str, Any], operator: str, measurement_hash: str, completed: datetime, root: Path, review_path: Path) -> dict[str, Any]:
    review = exact_object(
        review,
        {"schema", "verifier_version", "binding", "operator_identity", "reviewer_identity", "decision", "reviewed_at_utc", "measurement_sha256", "observations"},
        "independent review artifact",
    )
    if review["schema"] != REVIEW_SCHEMA_URI or review["verifier_version"] != VERIFIER_VERSION:
        fail("independent review schema or verifier version is invalid")
    sample_binding(review["binding"], binding, "independent review.binding")
    if review["operator_identity"] != operator:
        fail("independent review operator does not match measurement operator")
    reviewer = require_string(review["reviewer_identity"], "independent review.reviewer_identity", maximum=128)
    if reviewer.casefold() == operator.casefold():
        fail("independent reviewer must differ from operator")
    if review["decision"] != "approved-workstation-characterization":
        fail("independent review did not approve the bounded characterization")
    if require_sha256(review["measurement_sha256"], "independent review.measurement_sha256") != measurement_hash:
        fail("independent review does not bind the measurement bytes")
    reviewed = parse_utc(review["reviewed_at_utc"], "independent review.reviewed_at_utc")
    if reviewed < completed:
        fail("independent review predates measurement completion")
    observations = review["observations"]
    required_observations = {
        "raw_hashes_verified",
        "resource_envelope_verified",
        "pid_binary_bindings_verified",
        "warmup_exclusion_verified",
        "nonclaims_verified",
        "production_capacity_not_certified",
    }
    observations = exact_object(observations, required_observations, "independent review.observations")
    if any(observations[key] is not True for key in required_observations):
        fail("independent review observations must all be true")
    if review_path.is_symlink() or not review_path.is_file():
        fail("independent review artifact must be a regular non-symlink file")
    return review


def assemble(measurement_path: Path, review_path: Path, report_path: Path) -> Path:
    validate_schema_contract()
    if measurement_path.is_symlink() or review_path.is_symlink():
        fail("measurement and review inputs must not be symlinks")
    root = measurement_path.resolve(strict=True).parent
    try:
        review_resolved = review_path.resolve(strict=True)
        review_resolved.relative_to(root)
    except (OSError, ValueError):
        fail("independent review artifact must be inside the evidence root")
    measurement = validate_measurement(load_json(measurement_path, "measurement"), root, measurement_path)
    measurement_hash = sha256_file(measurement_path)
    completed = parse_utc(measurement["sampling"]["completed_utc"], "measurement completion")
    review = validate_review(
        load_json(review_resolved, "independent review"),
        measurement["target"],
        measurement["operator_identity"],
        measurement_hash,
        completed,
        root,
        review_resolved,
    )
    record = {
        key: measurement[key]
        for key in (
            "verifier_version",
            "evidence_class",
            "production_capacity_certification",
            "claim_scope",
            "target",
            "native_environment",
            "nodes",
            "resource_envelope",
            "sampling",
            "summary",
            "nonclaims",
        )
    }
    record["schema"] = SCHEMA_URI
    record["artifacts"] = list(measurement["artifacts"]) + [artifact_entry(root, measurement_path, "measurement")]
    record["independent_review"] = {
        "reviewer_identity": review["reviewer_identity"],
        "operator_identity": review["operator_identity"],
        "decision": review["decision"],
        "reviewed_at_utc": review["reviewed_at_utc"],
        "artifact_path": review_resolved.relative_to(root).as_posix(),
        "artifact_sha256": sha256_file(review_resolved),
        "measurement_sha256": measurement_hash,
    }
    record_path = root / "capacity-evidence-v2.json"
    write_json(record_path, record)
    binary_path = root / "kvnode"
    if not binary_path.is_file() or binary_path.is_symlink():
        fail("evidence root must contain the exact binary as kvnode")
    if sha256_file(binary_path) != measurement["target"]["binary_sha256"]:
        fail("evidence binary does not match the measurement target")
    report_resolved = report_path.absolute()
    try:
        report_resolved.parent.mkdir(parents=True, exist_ok=True)
        report_resolved.parent.resolve(strict=True).relative_to(root)
    except (OSError, ValueError):
        fail("capacity receipt must be written inside the evidence root")
    artifacts = {entry["kind"]: safe_artifact(root, entry["path"], f"record artifact {entry['kind']}") for entry in record["artifacts"]}
    receipt = {
        "status": "darwin-capacity-characterization-evidence-v2",
        "schema_version": "kvnode-capacity-evidence-v2",
        "harness": "tests/kvnode_capacity_envelope.sh",
        "verifier_version": VERIFIER_VERSION,
        "evidence_class": "workstation-capacity-characterization",
        "evidence_mode": "characterization",
        "claim_scope": "native-darwin-same-host-bounded-characterization",
        "acceptance_result": "pass-workstation-characterization",
        "production_capacity_certification": "not-claimed",
        "release_claim": "none-characterization-nonclaim",
        "target_name": measurement["target"]["target_id"],
        "release_id": measurement["target"]["release_id"],
        "source_revision": measurement["target"]["source_revision"],
        "binary_sha256": measurement["target"]["binary_sha256"],
        "environment_profile": measurement["target"]["environment_profile"],
        "evidence_root_path": str(root),
        "binary_path": str(binary_path.resolve(strict=True)),
        "record_path": str(record_path.resolve(strict=True)),
        "record_sha256": sha256_file(record_path),
        "measurement_path": str(artifacts["measurement"]),
        "measurement_sha256": measurement_hash,
        "raw_samples_path": str(artifacts["process-samples"]),
        "requests_path": str(artifacts["request-samples"]),
        "phase_windows_path": str(artifacts["phase-windows"]),
        "review_path": str(review_resolved),
        "review_sha256": sha256_file(review_resolved),
    }
    report_resolved.write_text("".join(f"{key}={value}\n" for key, value in receipt.items()), encoding="utf-8")
    os.chmod(report_resolved, 0o600)
    verify_receipt(report_resolved)
    return report_resolved


def validate_record(record: Any, root: Path, record_path: Path) -> dict[str, Any]:
    required = {
        "schema",
        "verifier_version",
        "evidence_class",
        "production_capacity_certification",
        "claim_scope",
        "target",
        "native_environment",
        "nodes",
        "resource_envelope",
        "sampling",
        "summary",
        "artifacts",
        "independent_review",
        "nonclaims",
    }
    record = exact_object(record, required, "capacity record")
    if record["schema"] != SCHEMA_URI or record["verifier_version"] != VERIFIER_VERSION:
        fail("capacity record schema or verifier version is invalid")
    if record["evidence_class"] != "workstation-capacity-characterization" or record["production_capacity_certification"] is not False:
        fail("capacity record attempts a forbidden production-capacity claim")
    if record["claim_scope"] != "native-darwin-same-host-bounded-characterization":
        fail("capacity record claim scope is invalid")
    binding = binding_from(record["target"], "capacity record.target")
    validate_native_environment(record["native_environment"])
    nodes = validate_nodes(record["nodes"], binding)
    sampling = record["sampling"]
    started = parse_utc(sampling.get("started_utc"), "capacity record sampling.started_utc") if isinstance(sampling, dict) else fail("sampling must be an object")
    envelope = validate_resource_envelope(record["resource_envelope"], binding, started)
    artifacts, entries = artifact_map(record["artifacts"], root, include_measurement=True)
    measurement = validate_measurement(load_json(artifacts["measurement"], "measurement artifact"), root, artifacts["measurement"])
    for key in (
        "verifier_version",
        "evidence_class",
        "production_capacity_certification",
        "claim_scope",
        "target",
        "native_environment",
        "nodes",
        "resource_envelope",
        "sampling",
        "summary",
        "nonclaims",
    ):
        if record[key] != measurement[key]:
            fail(f"capacity record {key} does not match reviewed measurement")
    review_summary = exact_object(
        record["independent_review"],
        {"reviewer_identity", "operator_identity", "decision", "reviewed_at_utc", "artifact_path", "artifact_sha256", "measurement_sha256"},
        "capacity record.independent_review",
    )
    review_path = safe_artifact(root, review_summary["artifact_path"], "capacity record independent review artifact_path")
    if sha256_file(review_path) != require_sha256(review_summary["artifact_sha256"], "capacity record independent review artifact_sha256"):
        fail("independent review artifact hash mismatch")
    measurement_hash = sha256_file(artifacts["measurement"])
    if review_summary["measurement_sha256"] != measurement_hash:
        fail("capacity record review does not bind measurement")
    review = validate_review(
        load_json(review_path, "independent review artifact"),
        binding,
        measurement["operator_identity"],
        measurement_hash,
        parse_utc(measurement["sampling"]["completed_utc"], "measurement completion"),
        root,
        review_path,
    )
    if review_summary["reviewer_identity"] != review["reviewer_identity"] or review_summary["operator_identity"] != review["operator_identity"] or review_summary["decision"] != review["decision"] or review_summary["reviewed_at_utc"] != review["reviewed_at_utc"]:
        fail("capacity record independent review summary does not match raw review")
    return record


def verify_characterization_receipt(report_path: Path) -> dict[str, Any]:
    validate_schema_contract()
    receipt = parse_env_receipt(report_path)
    required = {
        "status",
        "schema_version",
        "harness",
        "verifier_version",
        "evidence_class",
        "evidence_mode",
        "claim_scope",
        "acceptance_result",
        "production_capacity_certification",
        "release_claim",
        "target_name",
        "release_id",
        "source_revision",
        "binary_sha256",
        "environment_profile",
        "evidence_root_path",
        "binary_path",
        "record_path",
        "record_sha256",
        "measurement_path",
        "measurement_sha256",
        "raw_samples_path",
        "requests_path",
        "phase_windows_path",
        "review_path",
        "review_sha256",
    }
    if set(receipt) != required:
        fail("Darwin v2 receipt fields are not exact")
    expected = {
        "status": "darwin-capacity-characterization-evidence-v2",
        "schema_version": "kvnode-capacity-evidence-v2",
        "harness": "tests/kvnode_capacity_envelope.sh",
        "verifier_version": VERIFIER_VERSION,
        "evidence_class": "workstation-capacity-characterization",
        "evidence_mode": "characterization",
        "claim_scope": "native-darwin-same-host-bounded-characterization",
        "acceptance_result": "pass-workstation-characterization",
        "production_capacity_certification": "not-claimed",
        "release_claim": "none-characterization-nonclaim",
        "target_name": TARGET_ID,
        "environment_profile": ENVIRONMENT_PROFILE,
    }
    for key, wanted in expected.items():
        if receipt[key] != wanted:
            fail(f"receipt {key} must equal {wanted}")
    if not REVISION_RE.fullmatch(receipt["source_revision"]):
        fail("receipt source_revision is malformed")
    require_sha256(receipt["binary_sha256"], "receipt binary_sha256")
    require_sha256(receipt["record_sha256"], "receipt record_sha256")
    require_sha256(receipt["measurement_sha256"], "receipt measurement_sha256")
    require_sha256(receipt["review_sha256"], "receipt review_sha256")
    root_raw = Path(receipt["evidence_root_path"])
    if not root_raw.is_absolute() or root_raw.is_symlink() or not root_raw.is_dir():
        fail("receipt evidence_root_path must be an absolute non-symlink directory")
    root = root_raw.resolve(strict=True)
    try:
        report_resolved = report_path.resolve(strict=True)
        report_resolved.relative_to(root)
    except (OSError, ValueError):
        fail("receipt is outside its evidence root")
    binary_path = safe_absolute(root, receipt["binary_path"], "receipt binary_path")
    record_path = safe_absolute(root, receipt["record_path"], "receipt record_path")
    measurement_path = safe_absolute(root, receipt["measurement_path"], "receipt measurement_path")
    raw_samples_path = safe_absolute(root, receipt["raw_samples_path"], "receipt raw_samples_path")
    requests_path = safe_absolute(root, receipt["requests_path"], "receipt requests_path")
    phase_windows_path = safe_absolute(root, receipt["phase_windows_path"], "receipt phase_windows_path")
    review_path = safe_absolute(root, receipt["review_path"], "receipt review_path")
    if sha256_file(binary_path) != receipt["binary_sha256"]:
        fail("receipt binary bytes do not match binary_sha256")
    file_observation = run_observation(["file", str(binary_path)])
    file_text = file_observation["stdout"].lower()
    if file_observation["returncode"] != 0 or "mach-o" not in file_text or "arm64" not in file_text:
        fail("receipt binary is not a native Mach-O arm64 artifact")
    if sha256_file(record_path) != receipt["record_sha256"] or sha256_file(measurement_path) != receipt["measurement_sha256"] or sha256_file(review_path) != receipt["review_sha256"]:
        fail("receipt artifact hashes do not match")
    record = validate_record(load_json(record_path, "capacity record"), root, record_path)
    binding = record["target"]
    if receipt["release_id"] != binding["release_id"] or receipt["source_revision"] != binding["source_revision"] or receipt["binary_sha256"] != binding["binary_sha256"]:
        fail("receipt target/release/source/binary binding does not match capacity record")
    artifact_paths = {entry["kind"]: safe_artifact(root, entry["path"], f"record artifact {entry['kind']}") for entry in record["artifacts"]}
    expected_paths = {
        "measurement": measurement_path,
        "process-samples": raw_samples_path,
        "request-samples": requests_path,
        "phase-windows": phase_windows_path,
    }
    for kind, path in expected_paths.items():
        if artifact_paths[kind] != path:
            fail(f"receipt {kind} path does not match record artifact closure")
    review_record_path = safe_artifact(root, record["independent_review"]["artifact_path"], "record review path")
    if review_record_path != review_path:
        fail("receipt review path does not match capacity record")
    now = datetime.now(timezone.utc)
    completed = parse_utc(record["sampling"]["completed_utc"], "record completed_utc")
    max_age = int(os.environ.get("KVNODE_CAPACITY_MAX_EVIDENCE_AGE_SECONDS", "86400"))
    if max_age <= 0 or max_age > 604800:
        fail("KVNODE_CAPACITY_MAX_EVIDENCE_AGE_SECONDS is outside 1-604800")
    if completed > now or (now - completed).total_seconds() > max_age:
        fail("Darwin v2 capacity evidence is future-dated or stale")
    return record


PRODUCTION_SYSTEM_PROVENANCE = {
    "process": "darwin-libproc-proc_pid_rusage-v2",
    "system_cpu": "darwin-host_statistics64-cpu-load-info",
    "memory_pressure": "darwin-host_statistics64-vm-info",
    "filesystem": "darwin-statfs-apfs",
    "fsync": "darwin-apfs-fsync-probe-per-data-dir",
}


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def require_production_evidence_mode(value: Any, label: str) -> str:
    if value not in {"target", "test-only-synthetic"}:
        fail(f"{label} must be target or test-only-synthetic")
    if value == "test-only-synthetic" and os.environ.get("KVNODE_CAPACITY_ALLOW_TEST_FIXTURE") != "yes":
        fail("test-only synthetic capacity evidence requires KVNODE_CAPACITY_ALLOW_TEST_FIXTURE=yes")
    return value


def validate_production_artifact(
    root: Path,
    value: Any,
    label: str,
    expected_kind: str | None = None,
) -> tuple[dict[str, Any], Path]:
    artifact = exact_object(value, {"kind", "path", "sha256", "size_bytes"}, label)
    kind = require_string(artifact["kind"], f"{label}.kind", maximum=64)
    if expected_kind is not None and kind != expected_kind:
        fail(f"{label}.kind must equal {expected_kind}")
    path = safe_artifact(root, artifact["path"], f"{label}.path")
    expected_hash = require_sha256(artifact["sha256"], f"{label}.sha256")
    expected_size = require_int(artifact["size_bytes"], f"{label}.size_bytes", minimum=1)
    if sha256_file(path) != expected_hash or path.stat().st_size != expected_size:
        fail(f"{label} bytes do not match their declared hash and size")
    return artifact, path


def validate_support_envelope(value: Any) -> dict[str, Any]:
    envelope = exact_object(value, {"topology", "workload", "isolation"}, "support_envelope")
    topology = exact_object(
        envelope["topology"],
        {"node_count", "host_scope", "network_scope", "supervisor", "filesystem"},
        "support_envelope.topology",
    )
    expected_topology = {
        "node_count": 3,
        "host_scope": "same-host",
        "network_scope": "loopback",
        "supervisor": "launchd-system",
        "filesystem": "apfs",
    }
    if topology != expected_topology:
        fail("support_envelope.topology is not the exact supported Darwin topology")
    workload = exact_object(
        envelope["workload"],
        {
            "name",
            "operation_sequence",
            "key_seed",
            "key_count",
            "value_sizes_bytes",
            "warmup_operations",
            "measurement_operations",
            "concurrency",
        },
        "support_envelope.workload",
    )
    require_string(workload["name"], "support_envelope.workload.name", maximum=128)
    if workload["operation_sequence"] != ["put", "get", "scan"]:
        fail("support_envelope.workload.operation_sequence must be deterministic put/get/scan")
    require_string(workload["key_seed"], "support_envelope.workload.key_seed", maximum=128)
    if re.fullmatch(r"[A-Za-z0-9._-]+", workload["key_seed"]) is None:
        fail("support_envelope.workload.key_seed must be URL-safe deterministic text")
    require_int(workload["key_count"], "support_envelope.workload.key_count", minimum=1, maximum=10_000_000)
    value_sizes = workload["value_sizes_bytes"]
    if value_sizes != [64, 1024, 65536, 1048576]:
        fail("support_envelope.workload.value_sizes_bytes must equal [64,1024,65536,1048576]")
    warmup_operations = require_int(workload["warmup_operations"], "support_envelope.workload.warmup_operations", minimum=1)
    measurement_operations = require_int(workload["measurement_operations"], "support_envelope.workload.measurement_operations", minimum=1)
    concurrency = require_int(workload["concurrency"], "support_envelope.workload.concurrency", minimum=1, maximum=64)
    if concurrency > min(warmup_operations, measurement_operations):
        fail("support_envelope.workload.concurrency exceeds a declared phase operation count")
    isolation = exact_object(
        envelope["isolation"],
        {
            "dedicated_host_required",
            "virtualization_forbidden",
            "ac_power_required",
            "sleep_forbidden",
            "competing_workloads_forbidden",
        },
        "support_envelope.isolation",
    )
    if any(isolation[key] is not True for key in isolation):
        fail("support_envelope.isolation must require every declared Darwin isolation condition")
    return envelope


def validate_production_thresholds(value: Any) -> dict[str, Any]:
    expected = {
        "throughput": ("operations-per-second", "minimum", False),
        "request_latency_p99": ("nanoseconds", "maximum", False),
        "rss": ("bytes", "maximum", False),
        "open_fds": ("count", "maximum", False),
        "disk_allocated_growth": ("bytes", "maximum", True),
        "disk_written": ("bytes", "maximum", True),
        "fsync_latency_p99": ("nanoseconds", "maximum", False),
        "process_cpu": ("percent", "maximum", False),
        "system_cpu": ("percent", "maximum", False),
        "memory_pressure": ("percent", "maximum", False),
        "filesystem_free": ("bytes", "minimum", False),
        "request_error_rate": ("percent", "maximum", True),
    }
    thresholds = exact_object(value, set(expected), "thresholds")
    for name, (unit, direction, allow_zero) in expected.items():
        threshold = exact_object(thresholds[name], {"unit", "direction", "limit"}, f"thresholds.{name}")
        if threshold["unit"] != unit or threshold["direction"] != direction:
            fail(f"thresholds.{name} has an invalid unit or direction")
        limit = require_number(
            threshold["limit"],
            f"thresholds.{name}.limit",
            minimum=0,
            strict_minimum=not allow_zero,
        )
        if name in {"system_cpu", "memory_pressure", "request_error_rate"} and limit > 100:
            fail(f"thresholds.{name}.limit exceeds 100 normalized percent")
    return thresholds


def validate_production_binary_provenance(
    value: Any,
    root: Path,
    binding: dict[str, Any],
    evidence_mode: str,
    binary_path: Path,
) -> dict[str, Any]:
    provenance = exact_object(
        value,
        {
            "schema",
            "verifier_version",
            "evidence_class",
            "evidence_mode",
            "target",
            "binary_path",
            "observed_binary_sha256",
            "source_revision",
            "vcs_modified",
            "binary_format",
            "observed_commands",
        },
        "binary provenance",
    )
    if (
        provenance["schema"] != "https://gosuda.org/moreconsensus/schemas/kvnode-production-binary-provenance-v2.json"
        or provenance["verifier_version"] != VERIFIER_VERSION
        or provenance["evidence_class"] != "production-capacity-binary-provenance"
        or provenance["evidence_mode"] != evidence_mode
    ):
        fail("binary provenance identity is invalid")
    if binding_from(provenance["target"], "binary provenance.target") != binding:
        fail("binary provenance target binding does not match the campaign")
    observed_path = safe_absolute(root, require_string(provenance["binary_path"], "binary provenance.binary_path", maximum=2048), "binary provenance.binary_path")
    if observed_path != binary_path:
        fail("binary provenance does not identify the archived campaign binary")
    if (
        provenance["observed_binary_sha256"] != binding["binary_sha256"]
        or provenance["source_revision"] != binding["source_revision"]
        or provenance["vcs_modified"] is not False
        or provenance["binary_format"] != "mach-o-64-arm64"
    ):
        fail("binary provenance does not bind the exact source revision and binary")
    commands = provenance["observed_commands"]
    if not isinstance(commands, list) or len(commands) < 2:
        fail("binary provenance must retain file and Go build observations")
    observed: set[str] = set()
    file_stdout = ""
    go_stdout = ""
    for index, raw in enumerate(commands, 1):
        command = exact_object(raw, {"argv", "returncode", "stdout", "stderr"}, f"binary provenance command {index}")
        argv = command["argv"]
        if not isinstance(argv, list) or not argv or any(not isinstance(part, str) or not part for part in argv):
            fail(f"binary provenance command {index}.argv is invalid")
        if command["returncode"] != 0 or not isinstance(command["stdout"], str) or not isinstance(command["stderr"], str):
            fail(f"binary provenance command {index} did not retain a successful raw observation")
        if any(term in " ".join(argv).lower() for term in FORBIDDEN_OBSERVER_TERMS):
            fail(f"binary provenance command {index} uses a prohibited observer")
        observed_binary = Path(argv[0]).name
        observed.add(observed_binary)
        if observed_binary == "file":
            if argv != ["file", str(binary_path)]:
                fail("binary provenance file observation does not name the exact archived binary")
            file_stdout += command["stdout"]
        if observed_binary == "go":
            if argv != ["go", "version", "-m", str(binary_path)]:
                fail("binary provenance Go build observation does not name the exact archived binary")
            go_stdout += command["stdout"]
    if not {"file", "go"}.issubset(observed):
        fail("binary provenance omits file or Go build observations")
    if "mach-o" not in file_stdout.lower() or "arm64" not in file_stdout.lower():
        fail("binary provenance raw file output does not prove a Mach-O arm64 binary")
    revision_settings = re.findall(r"(?m)^\s*build\s+vcs\.revision=([0-9a-f]{40,64})\s*$", go_stdout)
    modified_settings = re.findall(r"(?m)^\s*build\s+vcs\.modified=(true|false)\s*$", go_stdout)
    if revision_settings != [binding["source_revision"]] or modified_settings != ["false"]:
        fail("binary provenance raw Go output does not contain unique exact VCS settings")
    return provenance


def validate_production_isolation(
    value: Any,
    binding: dict[str, Any],
    native_environment: dict[str, Any],
    repetition: dict[str, Any],
    repetition_id: str,
    evidence_mode: str,
) -> dict[str, Any]:
    isolation = exact_object(
        value,
        {
            "schema",
            "verifier_version",
            "evidence_class",
            "evidence_mode",
            "target",
            "repetition_id",
            "host_id",
            "captured_at_utc",
            "captured_monotonic_ns",
            "dedicated_host",
            "same_host_nodes",
            "workload_generator_same_host",
            "virtualization_present",
            "ac_power",
            "sleep_disabled",
            "competing_workload_count",
            "launchd_domain",
            "network_scope",
            "filesystem",
            "nodes",
            "workload_generator",
            "observed_commands",
        },
        f"{repetition_id} isolation evidence",
    )
    if (
        isolation["schema"] != "https://gosuda.org/moreconsensus/schemas/kvnode-production-isolation-observation-v2.json"
        or isolation["verifier_version"] != VERIFIER_VERSION
        or isolation["evidence_class"] != "production-capacity-isolation-observation"
        or isolation["evidence_mode"] != evidence_mode
    ):
        fail(f"{repetition_id} isolation evidence identity is invalid")
    if binding_from(isolation["target"], f"{repetition_id} isolation target") != binding:
        fail(f"{repetition_id} isolation target does not match the campaign")
    if isolation["repetition_id"] != repetition_id or isolation["host_id"] != native_environment["host_id"]:
        fail(f"{repetition_id} isolation host or repetition binding is invalid")
    repetition_started = parse_utc(repetition["started_utc"], f"{repetition_id}.started_utc")
    repetition_completed = parse_utc(repetition["completed_utc"], f"{repetition_id}.completed_utc")
    captured_at = parse_utc(isolation["captured_at_utc"], f"{repetition_id} isolation captured_at_utc")
    captured_monotonic = require_int(
        isolation["captured_monotonic_ns"],
        f"{repetition_id} isolation captured_monotonic_ns",
        minimum=1,
    )
    if (
        not repetition_started <= captured_at <= repetition_completed
        or not repetition["warmup_started_monotonic_ns"] <= captured_monotonic <= repetition["warmup_completed_monotonic_ns"]
    ):
        fail(f"{repetition_id} isolation capture is outside the repetition preflight")
    required_truth = ("dedicated_host", "same_host_nodes", "workload_generator_same_host", "ac_power", "sleep_disabled")
    if any(isolation[key] is not True for key in required_truth):
        fail(f"{repetition_id} did not observe every required same-host Darwin isolation condition")
    if isolation["virtualization_present"] is not False or isolation["competing_workload_count"] != 0:
        fail(f"{repetition_id} observed virtualization or a competing workload")
    if (
        isolation["launchd_domain"] != "system"
        or isolation["network_scope"] != "same-host-loopback"
        or isolation["filesystem"] != "apfs"
    ):
        fail(f"{repetition_id} isolation does not match the declared support envelope")
    raw_nodes = isolation["nodes"]
    if not isinstance(raw_nodes, list) or len(raw_nodes) != 3:
        fail(f"{repetition_id} isolation must bind exactly three target processes")
    expected_launchd_pids: dict[str, int] = {}
    node_pids: set[int] = set()
    expected_data_dirs: set[str] = set()
    expected_processes: dict[int, dict[str, Any]] = {}
    expected_peer_list = "1=https://127.0.0.1:19091,2=https://127.0.0.1:19191,3=https://127.0.0.1:19291"
    for index, raw_node in enumerate(raw_nodes, 1):
        node = exact_object(
            raw_node,
            {
                "node_id",
                "launchd_label",
                "pid",
                "process_start_token",
                "executable_path",
                "data_dir",
                "client_url",
                "peer_url",
                "admin_url",
                "peer_cert_path",
                "peer_key_path",
                "peer_ca_path",
                "client_cert_path",
                "client_key_path",
                "client_ca_path",
                "admin_cert_path",
                "admin_key_path",
                "admin_ca_path",
                "program_arguments",
                "listener_owner_pid",
                "listener_observation_provenance",
                "observed_binary_sha256",
                "observation_provenance",
                "observed_at_utc",
            },
            f"{repetition_id} isolation node {index}",
        )
        node_id = require_int(node["node_id"], f"{repetition_id} isolation node {index}.node_id", minimum=1, maximum=3)
        label = f"org.gosuda.moreconsensus.kvnode.{node_id}"
        pid = require_int(node["pid"], f"{repetition_id} isolation node {index}.pid", minimum=1)
        require_int(node["process_start_token"], f"{repetition_id} isolation node {index}.process_start_token", minimum=1)
        executable_path = require_string(node["executable_path"], f"{repetition_id} isolation node {index}.executable_path", maximum=1024)
        data_dir = require_string(node["data_dir"], f"{repetition_id} isolation node {index}.data_dir", maximum=1024)
        observed_at = parse_utc(node["observed_at_utc"], f"{repetition_id} isolation node {index}.observed_at_utc")
        for url_name in ("client_url", "peer_url", "admin_url"):
            url = require_string(node[url_name], f"{repetition_id} isolation node {index}.{url_name}", maximum=256)
            if re.fullmatch(r"https://127\.0\.0\.1:[0-9]{1,5}", url) is None:
                fail(f"{repetition_id} isolation node {index}.{url_name} must be loopback HTTPS")
        for path_name in (
            "peer_cert_path", "peer_key_path", "peer_ca_path",
            "client_cert_path", "client_key_path", "client_ca_path",
            "admin_cert_path", "admin_key_path", "admin_ca_path",
        ):
            path = require_string(node[path_name], f"{repetition_id} isolation node {index}.{path_name}", maximum=1024)
            if not path.startswith("/"):
                fail(f"{repetition_id} isolation node {index}.{path_name} must be absolute")
        expected_arguments = [
            executable_path,
            "-id", str(node_id),
            "-listen", node["client_url"].removeprefix("https://"),
            "-peer-listen", node["peer_url"].removeprefix("https://"),
            "-admin-listen", node["admin_url"].removeprefix("https://"),
            "-data", data_dir,
            "-peers", expected_peer_list,
            "-request-deadline-ms", "5000",
            "-peer-deadline-ms", "2000",
            "-max-client-body-bytes", "1048576",
            "-max-peer-body-bytes", "2097152",
            "-max-admin-body-bytes", "65536",
            "-max-scan-limit", "1000",
            "-production=true",
            "-peer-tls-cert", node["peer_cert_path"],
            "-peer-tls-key", node["peer_key_path"],
            "-peer-tls-ca", node["peer_ca_path"],
            "-client-tls-cert", node["client_cert_path"],
            "-client-tls-key", node["client_key_path"],
            "-client-client-ca", node["client_ca_path"],
            "-admin-tls-cert", node["admin_cert_path"],
            "-admin-tls-key", node["admin_key_path"],
            "-admin-client-ca", node["admin_ca_path"],
            "-pebble-cache-bytes", "8388608",
            "-pebble-memtable-bytes", "4194304",
            "-pebble-memtable-stop-writes", "2",
            "-pebble-max-open-files", "1000",
            "-pebble-max-concurrent-compactions", "1",
            "-pebble-bytes-per-sync", "524288",
            "-pebble-wal-bytes-per-sync", "0",
            "-retention-max-resident-instances", "100000",
            "-retention-max-durable-records", "100000",
            "-retention-max-data-bytes", "10737418240",
        ]
        if (
            node["launchd_label"] != label
            or pid in node_pids
            or not executable_path.startswith("/")
            or not data_dir.startswith("/")
            or data_dir in expected_data_dirs
            or node["program_arguments"] != expected_arguments
            or any(not isinstance(argument, str) or not argument or any(character.isspace() for character in argument) for argument in node["program_arguments"])
            or node["listener_owner_pid"] != pid
            or node["listener_observation_provenance"] != "darwin-lsof-nP-listener-owner-v2"
            or node["observed_binary_sha256"] != binding["binary_sha256"]
            or node["observation_provenance"] != "darwin-libproc-proc_pidpath-rusage-and-sha256-v2"
            or not repetition_started <= observed_at <= repetition_completed
        ):
            fail(f"{repetition_id} isolation node {index} does not bind the exact observed target process and canonical argv")
        node_pids.add(pid)
        expected_data_dirs.add(data_dir)
        expected_launchd_pids[label] = pid
        expected_processes[pid] = node
    if {node["node_id"] for node in raw_nodes} != {1, 2, 3}:
        fail(f"{repetition_id} isolation node IDs must be exactly 1,2,3")
    workload = exact_object(
        isolation["workload_generator"],
        {
            "pid",
            "process_start_token",
            "executable_path",
            "program_arguments",
            "observed_at_utc",
            "observation_provenance",
            "workload_sha256",
            "client_cert_path",
            "client_key_path",
            "client_ca_path",
            "client_cert_sha256",
        },
        f"{repetition_id} workload generator",
    )
    workload_pid = require_int(workload["pid"], f"{repetition_id} workload generator.pid", minimum=1)
    require_int(workload["process_start_token"], f"{repetition_id} workload generator.process_start_token", minimum=1)
    workload_executable = require_string(workload["executable_path"], f"{repetition_id} workload generator.executable_path", maximum=1024)
    workload_observed_at = parse_utc(workload["observed_at_utc"], f"{repetition_id} workload generator.observed_at_utc")
    workload_arguments = workload["program_arguments"]
    client_cert_path = require_string(workload["client_cert_path"], f"{repetition_id} workload generator.client_cert_path", maximum=1024)
    client_key_path = require_string(workload["client_key_path"], f"{repetition_id} workload generator.client_key_path", maximum=1024)
    client_ca_path = require_string(workload["client_ca_path"], f"{repetition_id} workload generator.client_ca_path", maximum=1024)
    client_cert_sha256 = require_sha256(workload["client_cert_sha256"], f"{repetition_id} workload generator.client_cert_sha256")
    if (
        workload_pid in node_pids
        or not workload_executable.startswith("/")
        or not isinstance(workload_arguments, list)
        or not workload_arguments
        or workload_arguments[0] != workload_executable
        or any(not isinstance(argument, str) or not argument or any(character.isspace() for character in argument) for argument in workload_arguments)
        or not all(path.startswith("/") for path in (client_cert_path, client_key_path, client_ca_path))
        or workload_arguments[-6:] != ["--client-cert", client_cert_path, "--client-key", client_key_path, "--ca", client_ca_path]
        or client_cert_sha256 == "0" * 64
        or workload["observation_provenance"] != "darwin-libproc-proc_pidpath-rusage-v2"
        or workload["workload_sha256"] != canonical_json_sha256(repetition["support_envelope"]["workload"])
        or not repetition_started <= workload_observed_at <= repetition_completed
    ):
        fail(f"{repetition_id} workload generator is not bound to the declared same-host workload")
    commands = isolation["observed_commands"]
    if not isinstance(commands, list) or len(commands) != 14:
        fail(f"{repetition_id} isolation evidence must contain the exact fourteen raw Darwin observations")
    observed_sysctl = False
    observed_pmset_custom = False
    observed_pmset_batt = False
    observed_ps = False
    observed_client_cert_hash = False
    observed_launchd_labels: set[str] = set()
    observed_stat_dirs: set[str] = set()
    observed_diskutil_dirs: set[str] = set()
    ps_output = ""
    for index, raw in enumerate(commands, 1):
        command = exact_object(raw, {"argv", "returncode", "stdout", "stderr"}, f"{repetition_id} isolation command {index}")
        argv = command["argv"]
        if not isinstance(argv, list) or not argv or any(not isinstance(part, str) or not part for part in argv):
            fail(f"{repetition_id} isolation command {index}.argv is invalid")
        if command["returncode"] != 0 or not isinstance(command["stdout"], str) or not isinstance(command["stderr"], str):
            fail(f"{repetition_id} isolation command {index} is not a successful raw observation")
        if any(term in " ".join(argv).lower() for term in FORBIDDEN_OBSERVER_TERMS):
            fail(f"{repetition_id} isolation command {index} uses a prohibited observer")
        stdout = command["stdout"]
        if argv[0] == "/usr/sbin/sysctl":
            if observed_sysctl or argv != ["/usr/sbin/sysctl", "-n", "kern.hv_vmm_present"] or stdout.strip() != "0":
                fail(f"{repetition_id} virtualization command is not the exact native zero observation")
            observed_sysctl = True
        elif argv[0] == "/usr/bin/pmset":
            if argv == ["/usr/bin/pmset", "-g", "custom"]:
                ac_profile = re.search(r"(?ms)^AC Power:\s*\n(.*?)(?=^[A-Za-z ]+ Power:\s*$|\Z)", stdout)
                if observed_pmset_custom or ac_profile is None or re.search(r"(?m)^\s*sleep\s+0\s*$", ac_profile.group(1)) is None:
                    fail(f"{repetition_id} AC power profile does not disable sleep")
                observed_pmset_custom = True
            elif argv == ["/usr/bin/pmset", "-g", "batt"]:
                if observed_pmset_batt or "Now drawing from 'AC Power'" not in stdout:
                    fail(f"{repetition_id} active power source is not AC")
                observed_pmset_batt = True
            else:
                fail(f"{repetition_id} pmset command is not an exact required observer")
        elif argv[0] == "/bin/ps":
            if observed_ps or argv != ["/bin/ps", "-axo", "pid=,ppid=,%cpu=,rss=,command="]:
                fail(f"{repetition_id} process-list command is not the exact same-host observation")
            observed_ps = True
            ps_output = stdout
        elif argv[0] == "/usr/bin/shasum":
            expected_output = f"{client_cert_sha256}  {client_cert_path}"
            if observed_client_cert_hash or argv != ["/usr/bin/shasum", "-a", "256", client_cert_path] or stdout.strip() != expected_output:
                fail(f"{repetition_id} workload client certificate hash observation is invalid")
            observed_client_cert_hash = True
        elif argv[0] == "/bin/launchctl":
            if len(argv) != 3 or argv[1] != "print" or not argv[2].startswith("system/"):
                fail(f"{repetition_id} launchctl command is invalid")
            label = argv[2].removeprefix("system/")
            expected_pid = expected_launchd_pids.get(label)
            if expected_pid is None or label in observed_launchd_labels or re.search(rf"(^|\s)pid = {expected_pid}(\s|$)", stdout) is None:
                fail(f"{repetition_id} launchctl output does not bind an exact node PID")
            observed_launchd_labels.add(label)
        elif argv[0] == "/usr/bin/stat":
            if len(argv) != 4 or argv[1:3] != ["-f", "%T"] or argv[3] not in expected_data_dirs or argv[3] in observed_stat_dirs or stdout.strip().lower() != "apfs":
                fail(f"{repetition_id} stat command does not bind an exact APFS node data directory")
            observed_stat_dirs.add(argv[3])
        elif argv[0] == "/usr/sbin/diskutil":
            if len(argv) != 3 or argv[1] != "info" or argv[2] not in expected_data_dirs or argv[2] in observed_diskutil_dirs or "apfs" not in stdout.lower():
                fail(f"{repetition_id} diskutil command does not bind an exact APFS node data directory")
            observed_diskutil_dirs.add(argv[2])
        else:
            fail(f"{repetition_id} isolation evidence contains an unexpected command")
    if not observed_sysctl or not observed_pmset_custom or not observed_pmset_batt or not observed_ps or not observed_client_cert_hash:
        fail(f"{repetition_id} isolation evidence omits a required Darwin or client-certificate observation")
    if observed_launchd_labels != set(expected_launchd_pids):
        fail(f"{repetition_id} launchd observations do not bind all exact node PIDs")
    if observed_stat_dirs != expected_data_dirs or observed_diskutil_dirs != expected_data_dirs:
        fail(f"{repetition_id} filesystem observations do not bind all exact APFS data directories")
    watched_commands = {
        pid: " ".join(node["program_arguments"])
        for pid, node in expected_processes.items()
    }
    watched_commands[workload_pid] = " ".join(workload_arguments)
    watched_executables = {node["executable_path"] for node in expected_processes.values()} | {workload_executable}
    observed_watched_pids: set[int] = set()
    for line in ps_output.splitlines():
        pid_match = re.match(r"^\s*([0-9]+)\s+", line)
        if pid_match is None:
            continue
        pid = int(pid_match.group(1))
        expected_command = watched_commands.get(pid)
        contains_watched_executable = any(executable in line for executable in watched_executables)
        if expected_command is not None:
            if re.search(rf"\s{re.escape(expected_command)}\s*$", line) is None:
                fail(f"{repetition_id} process-list output does not bind PID {pid} to exact canonical argv")
            observed_watched_pids.add(pid)
        elif contains_watched_executable:
            fail(f"{repetition_id} process-list output contains an undeclared competing target or workload process")
    if observed_watched_pids != set(watched_commands):
        fail(f"{repetition_id} process-list output omits a declared node or workload generator")
    return isolation


def validate_production_requests(
    rows: list[dict[str, Any]],
    binding: dict[str, Any],
    repetition: dict[str, Any],
    workload: dict[str, Any],
    expected_client_cert_sha256: str,
    isolation_nodes: list[dict[str, Any]],
    native_environment: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, tuple[int, int]]]:
    repetition_id = repetition["repetition_id"]
    windows = {
        "warmup": (
            repetition["warmup_started_monotonic_ns"],
            repetition["warmup_completed_monotonic_ns"],
        ),
        "measurement": (
            repetition["measurement_started_monotonic_ns"],
            repetition["measurement_completed_monotonic_ns"],
        ),
    }
    value_sizes = workload["value_sizes_bytes"]
    expected_counts = {
        "warmup": workload["warmup_operations"] * len(value_sizes),
        "measurement": workload["measurement_operations"] * len(value_sizes),
    }
    expected_nodes = {node["node_id"]: node for node in isolation_nodes}
    phase_rows: dict[str, list[dict[str, Any]]] = {"warmup": [], "measurement": []}
    repetition_started = parse_utc(repetition["started_utc"], f"{repetition_id}.started_utc")
    repetition_completed = parse_utc(repetition["completed_utc"], f"{repetition_id}.completed_utc")
    for index, raw in enumerate(rows, 1):
        row = exact_object(
            raw,
            {
                "target",
                "repetition_id",
                "sequence",
                "phase",
                "operation_index",
                "operation",
                "key_index",
                "value_bytes",
                "node_id",
                "request_url",
                "listener_owner_pid",
                "tls_ca_sha256",
                "client_cert_sha256",
                "started_utc",
                "started_monotonic_ns",
                "ended_monotonic_ns",
                "latency_ns",
                "http_status",
                "outcome",
            },
            f"{repetition_id} request observation {index}",
        )
        if binding_from(row["target"], f"{repetition_id} request observation {index}.target") != binding:
            fail(f"{repetition_id} request observation {index} target mismatch")
        if row["repetition_id"] != repetition_id or row["sequence"] != repetition["sequence"]:
            fail(f"{repetition_id} request observation {index} repetition binding mismatch")
        phase = row["phase"]
        if phase not in phase_rows:
            fail(f"{repetition_id} request observation {index} phase is invalid")
        operation_index = require_int(row["operation_index"], f"{repetition_id} request observation {index}.operation_index", minimum=0)
        operations_per_size = workload[f"{phase}_operations"]
        size_index = operation_index // operations_per_size
        local_index = operation_index % operations_per_size
        if size_index >= len(value_sizes):
            fail(f"{repetition_id} request observation {index} exceeds declared size cells")
        expected_operation = workload["operation_sequence"][local_index % len(workload["operation_sequence"])]
        if row["operation"] != expected_operation:
            fail(f"{repetition_id} request observation {index} violates the deterministic operation sequence")
        if row["key_index"] != local_index % workload["key_count"] or row["value_bytes"] != value_sizes[size_index]:
            fail(f"{repetition_id} request observation {index} violates the deterministic key/value size cell")
        node_id = local_index % 3 + 1
        expected_node = expected_nodes.get(node_id)
        key = f"{workload['key_seed']}-{local_index % workload['key_count']:08d}"
        expected_url = (
            expected_node["client_url"] + f"/scan?prefix={key}&limit=16"
            if expected_operation == "scan"
            else expected_node["client_url"] + f"/kv/{key}"
        ) if expected_node is not None else ""
        if (
            row["node_id"] != node_id
            or expected_node is None
            or row["request_url"] != expected_url
            or row["listener_owner_pid"] != expected_node["pid"]
            or row["client_cert_sha256"] != expected_client_cert_sha256
            or row["tls_ca_sha256"] != native_environment["tls_ca_sha256"]
        ):
            fail(f"{repetition_id} request observation {index} is not bound to the exact node listener and TLS CA")
        started_utc = parse_utc(row["started_utc"], f"{repetition_id} request observation {index}.started_utc")
        if not repetition_started <= started_utc <= repetition_completed:
            fail(f"{repetition_id} request observation {index} UTC timestamp is outside the repetition")
        started = require_int(row["started_monotonic_ns"], f"{repetition_id} request observation {index}.started_monotonic_ns", minimum=1)
        ended = require_int(row["ended_monotonic_ns"], f"{repetition_id} request observation {index}.ended_monotonic_ns", minimum=1)
        latency = require_int(row["latency_ns"], f"{repetition_id} request observation {index}.latency_ns", minimum=1)
        if ended - started != latency or not (windows[phase][0] <= started < ended <= windows[phase][1]):
            fail(f"{repetition_id} request observation {index} timing is invalid")
        status = require_int(row["http_status"], f"{repetition_id} request observation {index}.http_status", minimum=0, maximum=599)
        if row["outcome"] not in {"success", "http-error", "transport-error"}:
            fail(f"{repetition_id} request observation {index}.outcome is invalid")
        if (200 <= status < 300) != (row["outcome"] == "success"):
            fail(f"{repetition_id} request observation {index} status and outcome disagree")
        phase_rows[phase].append(row)
    for phase, expected_count in expected_counts.items():
        observations = phase_rows[phase]
        if len(observations) != expected_count:
            fail(f"{repetition_id} {phase} observations do not match the declared workload")
        if [row["operation_index"] for row in observations] != list(range(expected_count)):
            fail(f"{repetition_id} {phase} operation indexes are not deterministic and contiguous")
        events = [
            event
            for row in observations
            for event in (
                (row["started_monotonic_ns"], 1),
                (row["ended_monotonic_ns"], -1),
            )
        ]
        active = 0
        peak = 0
        for _, delta in sorted(events, key=lambda event: (event[0], event[1])):
            active += delta
            if active < 0:
                fail(f"{repetition_id} {phase} request intervals are inconsistent")
            peak = max(peak, active)
        if active != 0 or peak != workload["concurrency"]:
            fail(f"{repetition_id} {phase} requests do not evidence declared workload concurrency")
    measurement = phase_rows["measurement"]
    successes = [row for row in measurement if row["outcome"] == "success"]
    if not successes:
        fail(f"{repetition_id} has no successful measured requests")
    duration = windows["measurement"][1] - windows["measurement"][0]
    errors = len(measurement) - len(successes)
    summary = {
        "warmup_attempts": len(phase_rows["warmup"]),
        "measurement_attempts": len(measurement),
        "measurement_successes": len(successes),
        "measurement_errors": errors,
        "measurement_error_rate_percent": errors * 100.0 / len(measurement),
        "measurement_throughput_operations_per_second": len(measurement) * 1_000_000_000 / duration,
        "request_latency_p99_ns": nearest_rank([row["latency_ns"] for row in successes], 99),
    }
    request_bounds = {
        phase: (
            min(row["started_monotonic_ns"] for row in observations),
            max(row["ended_monotonic_ns"] for row in observations),
        )
        for phase, observations in phase_rows.items()
    }
    return summary, request_bounds


def validate_production_system_observations(
    rows: list[dict[str, Any]],
    binding: dict[str, Any],
    repetition: dict[str, Any],
    isolation_nodes: list[dict[str, Any]],
    request_bounds: dict[str, tuple[int, int]],
) -> dict[str, Any]:
    repetition_id = repetition["repetition_id"]
    windows = {
        "warmup": (
            repetition["warmup_started_monotonic_ns"],
            repetition["warmup_completed_monotonic_ns"],
        ),
        "measurement": (
            repetition["measurement_started_monotonic_ns"],
            repetition["measurement_completed_monotonic_ns"],
        ),
    }
    expected_bindings = [
        {
            key: node[key]
            for key in (
                "node_id",
                "pid",
                "process_start_token",
                "executable_path",
                "observed_binary_sha256",
            )
        }
        for node in sorted(isolation_nodes, key=lambda item: item["node_id"])
    ]
    phase_times: dict[str, list[int]] = {"warmup": [], "measurement": []}
    prior_monotonic = -1
    measurement_rows: list[dict[str, Any]] = []
    repetition_started = parse_utc(repetition["started_utc"], f"{repetition_id}.started_utc")
    repetition_completed = parse_utc(repetition["completed_utc"], f"{repetition_id}.completed_utc")
    for index, raw in enumerate(rows, 1):
        row = exact_object(
            raw,
            {
                "target",
                "repetition_id",
                "sequence",
                "phase",
                "sample_index",
                "timestamp_utc",
                "monotonic_ns",
                "provenance",
                "aggregation_scope",
                "process_bindings",
                "active_power_source",
                "sleep_disabled",
                "virtualization_present",
                "competing_workload_count",
                "isolation_provenance",
                "process_cpu_percent",
                "system_cpu_percent",
                "rss_bytes",
                "open_fd_count",
                "apfs_allocated_bytes",
                "disk_written_bytes",
                "fsync_latency_ns",
                "filesystem_free_bytes",
                "memory_pressure_percent",
            },
            f"{repetition_id} system observation {index}",
        )
        if binding_from(row["target"], f"{repetition_id} system observation {index}.target") != binding:
            fail(f"{repetition_id} system observation {index} target mismatch")
        if (
            row["repetition_id"] != repetition_id
            or row["sequence"] != repetition["sequence"]
            or row["sample_index"] != index
        ):
            fail(f"{repetition_id} system observation {index} sequence binding is invalid")
        phase = row["phase"]
        if phase not in phase_times:
            fail(f"{repetition_id} system observation {index}.phase is invalid")
        timestamp_utc = parse_utc(row["timestamp_utc"], f"{repetition_id} system observation {index}.timestamp_utc")
        if not repetition_started <= timestamp_utc <= repetition_completed:
            fail(f"{repetition_id} system observation {index} UTC timestamp is outside the repetition")
        monotonic = require_int(row["monotonic_ns"], f"{repetition_id} system observation {index}.monotonic_ns", minimum=1)
        if monotonic <= prior_monotonic or not (windows[phase][0] <= monotonic <= windows[phase][1]):
            fail(f"{repetition_id} system observation {index} timing is invalid")
        prior_monotonic = monotonic
        if row["aggregation_scope"] != "three-node-cluster-total":
            fail(f"{repetition_id} system observation {index} aggregation scope is invalid")
        if row["provenance"] != PRODUCTION_SYSTEM_PROVENANCE:
            fail(f"{repetition_id} system observation {index} provenance is invalid")
        if (
            row["active_power_source"] != "AC Power"
            or row["sleep_disabled"] is not True
            or row["virtualization_present"] is not False
            or row["competing_workload_count"] != 0
            or row["isolation_provenance"] != "darwin-pmset-sysctl-ps-continuous-sample-v2"
        ):
            fail(f"{repetition_id} system observation {index} does not retain required isolation state")
        process_bindings = row["process_bindings"]
        if not isinstance(process_bindings, list) or process_bindings != expected_bindings:
            fail(f"{repetition_id} system observation {index} is not bound to the exact measured processes")
        require_number(row["process_cpu_percent"], f"{repetition_id} system observation {index}.process_cpu_percent", minimum=0)
        require_number(row["system_cpu_percent"], f"{repetition_id} system observation {index}.system_cpu_percent", minimum=0, maximum=100)
        require_int(row["rss_bytes"], f"{repetition_id} system observation {index}.rss_bytes", minimum=1)
        require_int(row["open_fd_count"], f"{repetition_id} system observation {index}.open_fd_count", minimum=1)
        require_int(row["apfs_allocated_bytes"], f"{repetition_id} system observation {index}.apfs_allocated_bytes", minimum=0)
        require_int(row["disk_written_bytes"], f"{repetition_id} system observation {index}.disk_written_bytes", minimum=0)
        require_int(row["fsync_latency_ns"], f"{repetition_id} system observation {index}.fsync_latency_ns", minimum=1)
        require_int(row["filesystem_free_bytes"], f"{repetition_id} system observation {index}.filesystem_free_bytes", minimum=1)
        require_number(row["memory_pressure_percent"], f"{repetition_id} system observation {index}.memory_pressure_percent", minimum=0, maximum=100)
        phase_times[phase].append(monotonic)
        if phase == "measurement":
            measurement_rows.append(row)
    for phase, times in phase_times.items():
        if len(times) < 2:
            fail(f"{repetition_id} system metrics do not cover {phase} with multiple samples")
        request_start, request_end = request_bounds[phase]
        if times[0] > request_start or times[-1] < request_end:
            fail(f"{repetition_id} {phase} system metrics do not span the measured requests")
        if phase == "measurement":
            max_gap = math.ceil((windows[phase][1] - windows[phase][0]) / 2)
            if any(after - before > max_gap for before, after in zip(times, times[1:])):
                fail(f"{repetition_id} measurement system metrics have an unobserved interval")
    disk_counters = [row["disk_written_bytes"] for row in measurement_rows]
    if any(after < before for before, after in zip(disk_counters, disk_counters[1:])):
        fail(f"{repetition_id} disk write counters regress")
    first_allocated = measurement_rows[0]["apfs_allocated_bytes"]
    return {
        "max_rss_bytes": max(row["rss_bytes"] for row in measurement_rows),
        "max_open_fd_count": max(row["open_fd_count"] for row in measurement_rows),
        "max_disk_allocated_growth_bytes": max(0, max(row["apfs_allocated_bytes"] for row in measurement_rows) - first_allocated),
        "disk_written_bytes": disk_counters[-1] - disk_counters[0],
        "fsync_latency_p99_ns": nearest_rank([row["fsync_latency_ns"] for row in measurement_rows], 99),
        "max_process_cpu_percent": max(float(row["process_cpu_percent"]) for row in measurement_rows),
        "max_system_cpu_percent": max(float(row["system_cpu_percent"]) for row in measurement_rows),
        "max_memory_pressure_percent": max(float(row["memory_pressure_percent"]) for row in measurement_rows),
        "min_filesystem_free_bytes": min(row["filesystem_free_bytes"] for row in measurement_rows),
    }


def enforce_production_thresholds(summary: dict[str, Any], thresholds: dict[str, Any], repetition_id: str) -> None:
    observations = {
        "throughput": summary["measurement_throughput_operations_per_second"],
        "request_latency_p99": summary["request_latency_p99_ns"],
        "rss": summary["max_rss_bytes"],
        "open_fds": summary["max_open_fd_count"],
        "disk_allocated_growth": summary["max_disk_allocated_growth_bytes"],
        "disk_written": summary["disk_written_bytes"],
        "fsync_latency_p99": summary["fsync_latency_p99_ns"],
        "process_cpu": summary["max_process_cpu_percent"],
        "system_cpu": summary["max_system_cpu_percent"],
        "memory_pressure": summary["max_memory_pressure_percent"],
        "filesystem_free": summary["min_filesystem_free_bytes"],
        "request_error_rate": summary["measurement_error_rate_percent"],
    }
    for name, actual in observations.items():
        threshold = thresholds[name]
        limit = float(threshold["limit"])
        if (
            threshold["direction"] == "maximum"
            and float(actual) > limit
            or threshold["direction"] == "minimum"
            and float(actual) < limit
        ):
            fail(f"{repetition_id} {name} observation is outside the externally approvable threshold")


def validate_production_repetition(
    value: Any,
    root: Path,
    binding: dict[str, Any],
    native_environment: dict[str, Any],
    support_envelope: dict[str, Any],
    thresholds: dict[str, Any],
    evidence_mode: str,
    expected_sequence: int,
    repetition_count: int,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    repetition = exact_object(
        value,
        {
            "evidence_class",
            "evidence_mode",
            "production_capacity_certification",
            "claim_scope",
            "target",
            "native_environment",
            "support_envelope",
            "repetition_id",
            "sequence",
            "repetition_count",
            "started_utc",
            "completed_utc",
            "warmup_started_monotonic_ns",
            "warmup_completed_monotonic_ns",
            "measurement_started_monotonic_ns",
            "measurement_completed_monotonic_ns",
            "artifacts",
            "summary",
        },
        f"production repetition {expected_sequence}",
    )
    repetition_id = f"repetition-{expected_sequence:03d}"
    if (
        repetition["evidence_class"] != "production-capacity-repetition"
        or repetition["evidence_mode"] != evidence_mode
        or repetition["production_capacity_certification"] is not False
        or repetition["claim_scope"] != "target-bound-production-capacity-repetition-intermediate"
    ):
        fail(f"{repetition_id} is not an explicit production-capacity repetition intermediate")
    if (
        repetition["repetition_id"] != repetition_id
        or repetition["sequence"] != expected_sequence
        or repetition["repetition_count"] != repetition_count
    ):
        fail(f"{repetition_id} sequence or campaign count is invalid")
    if binding_from(repetition["target"], f"{repetition_id}.target") != binding:
        fail(f"{repetition_id} target binding does not match the campaign")
    if validate_native_environment(repetition["native_environment"], "mutual-auth-separated-planes") != native_environment:
        fail(f"{repetition_id} native environment does not match the campaign")
    if validate_support_envelope(repetition["support_envelope"]) != support_envelope:
        fail(f"{repetition_id} support envelope does not match the campaign")
    started = parse_utc(repetition["started_utc"], f"{repetition_id}.started_utc")
    completed = parse_utc(repetition["completed_utc"], f"{repetition_id}.completed_utc")
    if started >= completed:
        fail(f"{repetition_id} UTC range is reversed or empty")
    warmup_start = require_int(repetition["warmup_started_monotonic_ns"], f"{repetition_id}.warmup_started_monotonic_ns", minimum=1)
    warmup_end = require_int(repetition["warmup_completed_monotonic_ns"], f"{repetition_id}.warmup_completed_monotonic_ns", minimum=1)
    measurement_start = require_int(repetition["measurement_started_monotonic_ns"], f"{repetition_id}.measurement_started_monotonic_ns", minimum=1)
    measurement_end = require_int(repetition["measurement_completed_monotonic_ns"], f"{repetition_id}.measurement_completed_monotonic_ns", minimum=1)
    if not warmup_start < warmup_end <= measurement_start < measurement_end:
        fail(f"{repetition_id} warmup and measurement windows overlap or are reversed")
    raw_artifacts = repetition["artifacts"]
    if not isinstance(raw_artifacts, list) or len(raw_artifacts) != len(PRODUCTION_RUN_ARTIFACT_KINDS):
        fail(f"{repetition_id} must bind exactly isolation, request, and system raw artifacts")
    artifact_paths: dict[str, Path] = {}
    artifact_entries: list[dict[str, Any]] = []
    for index, raw_artifact in enumerate(raw_artifacts, 1):
        entry, path = validate_production_artifact(root, raw_artifact, f"{repetition_id} artifact {index}")
        if entry["kind"] in artifact_paths:
            fail(f"{repetition_id} duplicates raw artifact kind {entry['kind']}")
        artifact_paths[entry["kind"]] = path
        artifact_entries.append(entry)
    if set(artifact_paths) != PRODUCTION_RUN_ARTIFACT_KINDS:
        fail(f"{repetition_id} raw artifact closure is incomplete or mixed")
    isolation = validate_production_isolation(
        load_json(artifact_paths["isolation-evidence"], f"{repetition_id} isolation evidence"),
        binding,
        native_environment,
        repetition,
        repetition_id,
        evidence_mode,
    )
    request_summary, request_bounds = validate_production_requests(
        load_jsonl(artifact_paths["request-observations"], f"{repetition_id} request observations"),
        binding,
        repetition,
        support_envelope["workload"],
        isolation["workload_generator"]["client_cert_sha256"],
        isolation["nodes"],
        native_environment,
    )
    if isolation["captured_monotonic_ns"] > request_bounds["warmup"][0]:
        fail(f"{repetition_id} isolation preflight does not precede the measured workload")
    system_summary = validate_production_system_observations(
        load_jsonl(artifact_paths["system-observations"], f"{repetition_id} system observations"),
        binding,
        repetition,
        isolation["nodes"],
        request_bounds,
    )
    summary = {
        "repetition_id": repetition_id,
        "sequence": expected_sequence,
        **request_summary,
        **system_summary,
        "thresholds_passed": True,
    }
    enforce_production_thresholds(summary, thresholds, repetition_id)
    if repetition["summary"] != summary:
        fail(f"{repetition_id} summary does not exactly match raw observations")
    return repetition, summary, artifact_entries


def validate_production_campaign(
    value: Any,
    root: Path,
    campaign_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], Path]:
    campaign = exact_object(
        value,
        {
            "schema",
            "verifier_version",
            "evidence_class",
            "evidence_mode",
            "production_capacity_certification",
            "claim_scope",
            "operator_identity",
            "target",
            "native_environment",
            "support_envelope",
            "thresholds",
            "binary_provenance",
            "repetition_count",
            "repetitions",
        },
        "production capacity campaign",
    )
    evidence_mode = require_production_evidence_mode(campaign["evidence_mode"], "production capacity campaign.evidence_mode")
    expected_campaign_class = (
        "production-capacity-campaign"
        if evidence_mode == "target"
        else "synthetic-production-capacity-campaign-test-only"
    )
    if (
        campaign["schema"] != PRODUCTION_CAMPAIGN_SCHEMA_URI
        or campaign["verifier_version"] != VERIFIER_VERSION
        or campaign["evidence_class"] != expected_campaign_class
        or campaign["production_capacity_certification"] is not False
        or campaign["claim_scope"] != "target-bound-production-capacity-campaign-intermediate"
    ):
        fail("production capacity campaign identity or non-certification state is invalid")
    operator = require_string(campaign["operator_identity"], "production capacity campaign.operator_identity", maximum=128)
    binding = binding_from(campaign["target"], "production capacity campaign.target")
    native_environment = validate_native_environment(campaign["native_environment"], "mutual-auth-separated-planes")
    support_envelope = validate_support_envelope(campaign["support_envelope"])
    thresholds = validate_production_thresholds(campaign["thresholds"])
    binary_path = root / "kvnode"
    if binary_path.is_symlink() or not binary_path.is_file() or sha256_file(binary_path) != binding["binary_sha256"]:
        fail("production capacity campaign does not contain the exact target binary as kvnode")
    binary_entry, binary_provenance_path = validate_production_artifact(
        root,
        campaign["binary_provenance"],
        "production capacity campaign.binary_provenance",
        "binary-provenance",
    )
    validate_production_binary_provenance(
        load_json(binary_provenance_path, "production binary provenance"),
        root,
        binding,
        evidence_mode,
        binary_path.resolve(strict=True),
    )
    repetition_count = require_int(
        campaign["repetition_count"],
        "production capacity campaign.repetition_count",
        minimum=PRODUCTION_MIN_REPETITIONS,
        maximum=20,
    )
    repetitions = campaign["repetitions"]
    if not isinstance(repetitions, list) or len(repetitions) != repetition_count:
        fail("production capacity campaign repetitions do not match repetition_count")
    summaries: list[dict[str, Any]] = []
    raw_artifacts = [binary_entry]
    prior_completed: datetime | None = None
    seen_paths: set[str] = {binary_entry["path"]}
    for sequence, raw_repetition in enumerate(repetitions, 1):
        repetition, summary, entries = validate_production_repetition(
            raw_repetition,
            root,
            binding,
            native_environment,
            support_envelope,
            thresholds,
            evidence_mode,
            sequence,
            repetition_count,
        )
        started = parse_utc(repetition["started_utc"], f"repetition-{sequence:03d}.started_utc")
        completed = parse_utc(repetition["completed_utc"], f"repetition-{sequence:03d}.completed_utc")
        if prior_completed is not None and started <= prior_completed:
            fail("production capacity repetitions overlap or are not in deterministic order")
        prior_completed = completed
        for entry in entries:
            if entry["path"] in seen_paths:
                fail("production capacity repetitions mix or reuse raw artifacts")
            seen_paths.add(entry["path"])
        raw_artifacts.extend(entries)
        summaries.append(summary)
    if campaign_path.is_symlink() or not campaign_path.is_file():
        fail("production capacity campaign must be a regular non-symlink file")
    require_string(operator, "production capacity campaign.operator_identity", maximum=128)
    return campaign, summaries, raw_artifacts, binary_path


def production_intermediate_record(
    campaign: dict[str, Any],
    campaign_path: Path,
    root: Path,
    summaries: list[dict[str, Any]],
    raw_artifacts: list[dict[str, Any]],
    binary_path: Path,
) -> dict[str, Any]:
    return {
        "schema": PRODUCTION_INTERMEDIATE_SCHEMA_URI,
        "verifier_version": VERIFIER_VERSION,
        "evidence_class": PRODUCTION_INTERMEDIATE_CLASS,
        "evidence_mode": campaign["evidence_mode"],
        "production_capacity_certification": False,
        "claim_scope": PRODUCTION_CLAIM_SCOPE,
        "release_claim": "none-unsigned-intermediate",
        "operator_identity": campaign["operator_identity"],
        "target": campaign["target"],
        "native_environment": campaign["native_environment"],
        "support_envelope": campaign["support_envelope"],
        "thresholds": campaign["thresholds"],
        "repetition_count": campaign["repetition_count"],
        "repetition_summaries": summaries,
        "raw_artifacts": raw_artifacts,
        "campaign": artifact_entry(root, campaign_path, "campaign"),
        "binary": artifact_entry(root, binary_path, "binary"),
    }


def validate_production_intermediate(value: Any, root: Path, intermediate_path: Path) -> dict[str, Any]:
    record = exact_object(
        value,
        {
            "schema",
            "verifier_version",
            "evidence_class",
            "evidence_mode",
            "production_capacity_certification",
            "claim_scope",
            "release_claim",
            "operator_identity",
            "target",
            "native_environment",
            "support_envelope",
            "thresholds",
            "repetition_count",
            "repetition_summaries",
            "raw_artifacts",
            "campaign",
            "binary",
        },
        "production capacity unsigned intermediate",
    )
    mode = require_production_evidence_mode(record["evidence_mode"], "production capacity unsigned intermediate.evidence_mode")
    if (
        record["schema"] != PRODUCTION_INTERMEDIATE_SCHEMA_URI
        or record["verifier_version"] != VERIFIER_VERSION
        or record["evidence_class"] != PRODUCTION_INTERMEDIATE_CLASS
        or record["production_capacity_certification"] is not False
        or record["claim_scope"] != PRODUCTION_CLAIM_SCOPE
        or record["release_claim"] != "none-unsigned-intermediate"
    ):
        fail("production capacity unsigned intermediate identity or fail-closed state is invalid")
    campaign_entry, campaign_path = validate_production_artifact(root, record["campaign"], "unsigned intermediate.campaign", "campaign")
    binary_entry, binary_path = validate_production_artifact(root, record["binary"], "unsigned intermediate.binary", "binary")
    campaign, summaries, raw_artifacts, expected_binary = validate_production_campaign(
        load_json(campaign_path, "production capacity campaign"),
        root,
        campaign_path,
    )
    if campaign["evidence_mode"] != mode or binary_path != expected_binary.resolve(strict=True):
        fail("unsigned intermediate campaign mode or binary path does not match")
    expected = production_intermediate_record(
        campaign,
        campaign_path,
        root,
        summaries,
        raw_artifacts,
        expected_binary,
    )
    if record != expected or record["campaign"] != campaign_entry or record["binary"] != binary_entry:
        fail("production capacity unsigned intermediate does not exactly match raw campaign evidence")
    if intermediate_path.is_symlink() or not intermediate_path.is_file():
        fail("production capacity unsigned intermediate must be a regular non-symlink file")
    return record


def production_receipt_values(
    record: dict[str, Any],
    root: Path,
    record_path: Path,
    *,
    final: bool,
) -> dict[str, str]:
    mode = record["evidence_mode"]
    if final:
        status = (
            "darwin-production-capacity-certification-v2"
            if mode == "target"
            else "synthetic-darwin-production-capacity-certification-v2"
        )
        acceptance = "pass-production-capacity-certification" if mode == "target" else "pass-test-only-synthetic"
        production = "approved" if mode == "target" else "test-only-not-production"
        release_claim = PRODUCTION_RELEASE_CLAIM if mode == "target" else "none-test-only-synthetic"
    else:
        status = "darwin-capacity-certification-unsigned-intermediate-v2"
        acceptance = "fail-closed-approval-required"
        production = "not-approved"
        release_claim = "none-unsigned-intermediate"
    values = {
        "status": status,
        "schema_version": "kvnode-capacity-evidence-v2",
        "harness": "tests/kvnode_capacity_envelope_v2.py",
        "verifier_version": VERIFIER_VERSION,
        "evidence_class": record["evidence_class"],
        "evidence_mode": mode,
        "claim_scope": PRODUCTION_CLAIM_SCOPE,
        "acceptance_result": acceptance,
        "production_capacity_certification": production,
        "release_claim": release_claim,
        "target_name": record["target"]["target_id"],
        "release_id": record["target"]["release_id"],
        "source_revision": record["target"]["source_revision"],
        "binary_sha256": record["target"]["binary_sha256"],
        "environment_profile": record["target"]["environment_profile"],
        "evidence_root_path": str(root),
        "binary_path": str(safe_artifact(root, record["binary"]["path"], "production binary path")),
        "record_path": str(record_path.resolve(strict=True)),
        "record_sha256": sha256_file(record_path),
        "campaign_path": str(safe_artifact(root, record["campaign"]["path"], "production campaign path")),
        "campaign_sha256": record["campaign"]["sha256"],
        "support_envelope_sha256": canonical_json_sha256(record["support_envelope"]),
        "thresholds_sha256": canonical_json_sha256(record["thresholds"]),
        "repetition_count": str(record["repetition_count"]),
    }
    if final:
        approval = record["approval"]
        values.update(
            {
                "approval_path": str(safe_artifact(root, approval["approval_artifact"]["path"], "approval path")),
                "approval_sha256": approval["approval_artifact"]["sha256"],
                "approver_identity": approval["approver_identity"],
                "trust_root_path": str(safe_artifact(root, approval["trust_root_artifact"]["path"], "trust root path")),
                "trust_root_sha256": approval["trust_root_artifact"]["sha256"],
                "signature_path": str(safe_artifact(root, approval["signature_artifact"]["path"], "signature path")),
                "signature_sha256": approval["signature_artifact"]["sha256"],
            }
        )
    return values


def write_production_receipt(
    report_path: Path,
    record: dict[str, Any],
    root: Path,
    record_path: Path,
    *,
    final: bool,
) -> Path:
    report_resolved = report_path.absolute()
    try:
        report_resolved.parent.mkdir(parents=True, exist_ok=True)
        report_resolved.parent.resolve(strict=True).relative_to(root)
    except (OSError, ValueError):
        fail("production capacity receipt must be written inside its evidence root")
    values = production_receipt_values(record, root, record_path, final=final)
    payload = "".join(f"{key}={value}\n" for key, value in values.items()).encode("utf-8")
    write_new_file_no_follow(report_resolved, payload, 0o600, "production capacity receipt")
    return report_resolved


def assemble_production(campaign_path: Path, report_path: Path) -> Path:
    validate_schema_contract()
    root = campaign_path.resolve(strict=True).parent
    campaign, summaries, raw_artifacts, binary_path = validate_production_campaign(
        load_json(campaign_path, "production capacity campaign"),
        root,
        campaign_path,
    )
    intermediate = production_intermediate_record(
        campaign,
        campaign_path,
        root,
        summaries,
        raw_artifacts,
        binary_path,
    )
    intermediate_path = root / "production-capacity-unsigned-intermediate-v2.json"
    write_new_production_json(intermediate_path, intermediate, "production capacity unsigned intermediate")
    validate_production_intermediate(intermediate, root, intermediate_path)
    report = write_production_receipt(report_path, intermediate, root, intermediate_path, final=False)
    verify_receipt(report)
    return report


def verify_rsa_sha256_signature(payload: bytes, public_key: Path, signature: Path, label: str) -> None:
    try:
        key_info = subprocess.run(
            ["openssl", "pkey", "-pubin", "-in", str(public_key), "-text_pub", "-noout"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        fail(f"{label} trust-root inspector could not execute: {exc}")
    key_text = key_info.stdout.decode("utf-8", errors="replace")
    bit_match = re.search(r"Public-Key:\s*\(([0-9]+) bit\)", key_text)
    if (
        key_info.returncode != 0
        or bit_match is None
        or int(bit_match.group(1)) < 2048
        or "modulus:" not in key_text.lower()
    ):
        fail(f"{label} trust root must be an RSA public key of at least 2048 bits")
    try:
        completed = subprocess.run(
            ["openssl", "dgst", "-sha256", "-verify", str(public_key), "-signature", str(signature)],
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        fail(f"{label} signature verifier could not execute: {exc}")
    if completed.returncode != 0 or b"Verified OK" not in completed.stdout:
        fail(f"{label} RSA-SHA256 signature verification failed")


def validate_production_approval(
    approval: Any,
    approval_path: Path,
    trust_root_path: Path,
    signature_path: Path,
    intermediate: dict[str, Any],
    intermediate_path: Path,
    root: Path,
) -> dict[str, Any]:
    approval = exact_object(
        approval,
        {
            "schema",
            "verifier_version",
            "evidence_class",
            "evidence_mode",
            "decision",
            "approver_identity",
            "approver_organization",
            "operator_identity",
            "target",
            "support_envelope_sha256",
            "thresholds_sha256",
            "intermediate_sha256",
            "signed_at_utc",
            "trust_root_sha256",
            "signature_algorithm",
        },
        "production capacity external approval",
    )
    mode = intermediate["evidence_mode"]
    expected_class = (
        "production-capacity-threshold-approval"
        if mode == "target"
        else "synthetic-production-capacity-threshold-approval-test-only"
    )
    if (
        approval["schema"] != PRODUCTION_APPROVAL_SCHEMA_URI
        or approval["verifier_version"] != VERIFIER_VERSION
        or approval["evidence_class"] != expected_class
        or approval["evidence_mode"] != mode
        or approval["decision"] != "approved-production-capacity-certification"
        or approval["signature_algorithm"] != "rsa-sha256"
    ):
        fail("production capacity approval identity, decision, or algorithm is invalid")
    approver = require_string(approval["approver_identity"], "production capacity approval.approver_identity", maximum=128)
    require_string(approval["approver_organization"], "production capacity approval.approver_organization", maximum=128)
    if approval["operator_identity"] != intermediate["operator_identity"] or approver.casefold() == intermediate["operator_identity"].casefold():
        fail("production capacity approval is missing an independent external approver")
    if binding_from(approval["target"], "production capacity approval.target") != intermediate["target"]:
        fail("production capacity approval target does not match the intermediate")
    if (
        approval["support_envelope_sha256"] != canonical_json_sha256(intermediate["support_envelope"])
        or approval["thresholds_sha256"] != canonical_json_sha256(intermediate["thresholds"])
        or approval["intermediate_sha256"] != sha256_file(intermediate_path)
    ):
        fail("production capacity approval does not bind the support envelope, thresholds, and intermediate")
    if (
        require_sha256(approval["trust_root_sha256"], "production capacity approval.trust_root_sha256")
        != sha256_file(trust_root_path)
    ):
        fail("production capacity approval trust root hash mismatch")
    if mode == "target":
        expected_trust_root = os.environ.get("KVNODE_CAPACITY_APPROVER_TRUST_ROOT_SHA256", "")
        expected_approver = os.environ.get("KVNODE_CAPACITY_APPROVER_IDENTITY", "")
        if not SHA256_RE.fullmatch(expected_trust_root):
            fail("target production capacity verification requires an externally pinned approver trust root hash")
        if not expected_approver or any(character in expected_approver for character in "\r\n\x00"):
            fail("target production capacity verification requires an externally pinned approver identity")
        if approval["trust_root_sha256"] != expected_trust_root or approver != expected_approver:
            fail("production capacity approval does not match the externally pinned approver trust contract")
    signed_at = parse_utc(approval["signed_at_utc"], "production capacity approval.signed_at_utc")
    campaign_completed = max(
        parse_utc(repetition["completed_utc"], "production repetition completed_utc")
        for repetition in load_json(
            safe_artifact(root, intermediate["campaign"]["path"], "approval campaign path"),
            "approval campaign",
        )["repetitions"]
    )
    if signed_at < campaign_completed or signed_at > datetime.now(timezone.utc):
        fail("production capacity approval is premature or future-dated")
    for path, label in (
        (approval_path, "approval"),
        (trust_root_path, "trust root"),
        (signature_path, "signature"),
    ):
        if path.is_symlink() or not path.is_file():
            fail(f"production capacity {label} must be a regular non-symlink file")
        try:
            path.resolve(strict=True).relative_to(root)
        except (OSError, ValueError):
            fail(f"production capacity {label} must be inside the evidence root")
    verify_rsa_sha256_signature(approval_path.read_bytes(), trust_root_path, signature_path, "production capacity approval")
    return approval


def resolve_external_approval_input(path: Path, root: Path, label: str) -> Path:
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        fail(f"production capacity {label} must be an existing file inside the evidence root")
    if resolved.is_symlink() or not resolved.is_file():
        fail(f"production capacity {label} must be a regular non-symlink file")
    return resolved


def certify_production(
    intermediate_path: Path,
    approval_path: Path,
    trust_root_path: Path,
    signature_path: Path,
    report_path: Path,
) -> Path:
    validate_schema_contract()
    root = intermediate_path.resolve(strict=True).parent
    intermediate = validate_production_intermediate(
        load_json(intermediate_path, "production capacity unsigned intermediate"),
        root,
        intermediate_path,
    )
    approval_resolved = resolve_external_approval_input(approval_path, root, "approval")
    trust_root_resolved = resolve_external_approval_input(trust_root_path, root, "trust root")
    signature_resolved = resolve_external_approval_input(signature_path, root, "signature")
    approval = validate_production_approval(
        load_json(approval_resolved, "production capacity external approval"),
        approval_resolved,
        trust_root_resolved,
        signature_resolved,
        intermediate,
        intermediate_path,
        root,
    )
    mode = intermediate["evidence_mode"]
    record = {
        key: intermediate[key]
        for key in (
            "verifier_version",
            "evidence_mode",
            "claim_scope",
            "operator_identity",
            "target",
            "native_environment",
            "support_envelope",
            "thresholds",
            "repetition_count",
            "repetition_summaries",
            "raw_artifacts",
            "campaign",
            "binary",
        )
    }
    record.update(
        {
            "schema": PRODUCTION_SCHEMA_URI if mode == "target" else PRODUCTION_TEST_SCHEMA_URI,
            "evidence_class": PRODUCTION_FINAL_CLASS if mode == "target" else PRODUCTION_TEST_CLASS,
            "production_capacity_certification": mode == "target",
            "release_claim": PRODUCTION_RELEASE_CLAIM if mode == "target" else "none-test-only-synthetic",
            "intermediate": artifact_entry(root, intermediate_path, "unsigned-intermediate"),
            "approval": {
                "approver_identity": approval["approver_identity"],
                "approver_organization": approval["approver_organization"],
                "decision": approval["decision"],
                "signed_at_utc": approval["signed_at_utc"],
                "signature_algorithm": approval["signature_algorithm"],
                "approval_artifact": artifact_entry(root, approval_path.resolve(strict=True), "external-approval"),
                "trust_root_artifact": artifact_entry(root, trust_root_path.resolve(strict=True), "approver-trust-root"),
                "signature_artifact": artifact_entry(root, signature_path.resolve(strict=True), "approver-signature"),
            },
        }
    )
    record_path = root / (
        "production-capacity-certification-v2.json"
        if mode == "target"
        else "production-capacity-test-fixture-v2.json"
    )
    write_new_production_json(record_path, record, "production capacity certificate")
    validate_production_certificate(record, root, record_path)
    report = write_production_receipt(report_path, record, root, record_path, final=True)
    verify_receipt(report)
    return report


def validate_production_certificate(value: Any, root: Path, record_path: Path) -> dict[str, Any]:
    record = exact_object(
        value,
        {
            "schema",
            "verifier_version",
            "evidence_class",
            "evidence_mode",
            "production_capacity_certification",
            "claim_scope",
            "release_claim",
            "operator_identity",
            "target",
            "native_environment",
            "support_envelope",
            "thresholds",
            "repetition_count",
            "repetition_summaries",
            "raw_artifacts",
            "campaign",
            "binary",
            "intermediate",
            "approval",
        },
        "production capacity certificate",
    )
    mode = require_production_evidence_mode(record["evidence_mode"], "production capacity certificate.evidence_mode")
    if mode == "target":
        expected_class = PRODUCTION_FINAL_CLASS
        expected_certification = True
        expected_claim = PRODUCTION_RELEASE_CLAIM
        expected_schema = PRODUCTION_SCHEMA_URI
    else:
        expected_class = PRODUCTION_TEST_CLASS
        expected_certification = False
        expected_claim = "none-test-only-synthetic"
        expected_schema = PRODUCTION_TEST_SCHEMA_URI
    if (
        record["schema"] != expected_schema
        or record["verifier_version"] != VERIFIER_VERSION
        or record["evidence_class"] != expected_class
        or record["production_capacity_certification"] is not expected_certification
        or record["claim_scope"] != PRODUCTION_CLAIM_SCOPE
        or record["release_claim"] != expected_claim
    ):
        fail("production capacity certificate class, mode, claim, or certification state is invalid")
    intermediate_entry, intermediate_path = validate_production_artifact(
        root,
        record["intermediate"],
        "production capacity certificate.intermediate",
        "unsigned-intermediate",
    )
    intermediate = validate_production_intermediate(
        load_json(intermediate_path, "production capacity unsigned intermediate"),
        root,
        intermediate_path,
    )
    for key in (
        "verifier_version",
        "evidence_mode",
        "claim_scope",
        "operator_identity",
        "target",
        "native_environment",
        "support_envelope",
        "thresholds",
        "repetition_count",
        "repetition_summaries",
        "raw_artifacts",
        "campaign",
        "binary",
    ):
        if record[key] != intermediate[key]:
            fail(f"production capacity certificate {key} does not match the signed intermediate")
    approval_summary = exact_object(
        record["approval"],
        {
            "approver_identity",
            "approver_organization",
            "decision",
            "signed_at_utc",
            "signature_algorithm",
            "approval_artifact",
            "trust_root_artifact",
            "signature_artifact",
        },
        "production capacity certificate.approval",
    )
    approval_entry, approval_path = validate_production_artifact(root, approval_summary["approval_artifact"], "certificate approval artifact", "external-approval")
    trust_entry, trust_path = validate_production_artifact(root, approval_summary["trust_root_artifact"], "certificate trust root artifact", "approver-trust-root")
    signature_entry, signature_path = validate_production_artifact(root, approval_summary["signature_artifact"], "certificate signature artifact", "approver-signature")
    approval = validate_production_approval(
        load_json(approval_path, "production capacity external approval"),
        approval_path,
        trust_path,
        signature_path,
        intermediate,
        intermediate_path,
        root,
    )
    expected_approval = {
        "approver_identity": approval["approver_identity"],
        "approver_organization": approval["approver_organization"],
        "decision": approval["decision"],
        "signed_at_utc": approval["signed_at_utc"],
        "signature_algorithm": approval["signature_algorithm"],
        "approval_artifact": approval_entry,
        "trust_root_artifact": trust_entry,
        "signature_artifact": signature_entry,
    }
    if approval_summary != expected_approval or record["intermediate"] != intermediate_entry:
        fail("production capacity certificate approval summary does not match signed artifacts")
    if record_path.is_symlink() or not record_path.is_file():
        fail("production capacity certificate must be a regular non-symlink file")
    return record


def verify_production_receipt(report_path: Path, receipt: dict[str, str]) -> dict[str, Any]:
    intermediate_status = "darwin-capacity-certification-unsigned-intermediate-v2"
    final_statuses = {
        "darwin-production-capacity-certification-v2",
        "synthetic-darwin-production-capacity-certification-v2",
    }
    status = receipt.get("status")
    final = status in final_statuses
    expected_fields = {
        "status",
        "schema_version",
        "harness",
        "verifier_version",
        "evidence_class",
        "evidence_mode",
        "claim_scope",
        "acceptance_result",
        "production_capacity_certification",
        "release_claim",
        "target_name",
        "release_id",
        "source_revision",
        "binary_sha256",
        "environment_profile",
        "evidence_root_path",
        "binary_path",
        "record_path",
        "record_sha256",
        "campaign_path",
        "campaign_sha256",
        "support_envelope_sha256",
        "thresholds_sha256",
        "repetition_count",
    }
    if final:
        expected_fields.update(
            {
                "approval_path",
                "approval_sha256",
                "approver_identity",
                "trust_root_path",
                "trust_root_sha256",
                "signature_path",
                "signature_sha256",
            }
        )
    if status != intermediate_status and not final:
        fail("production capacity receipt status is invalid")
    if set(receipt) != expected_fields:
        fail("production capacity receipt fields are not exact")
    if (
        receipt["schema_version"] != "kvnode-capacity-evidence-v2"
        or receipt["harness"] != "tests/kvnode_capacity_envelope_v2.py"
        or receipt["verifier_version"] != VERIFIER_VERSION
        or receipt["claim_scope"] != PRODUCTION_CLAIM_SCOPE
        or receipt["target_name"] != TARGET_ID
        or receipt["environment_profile"] != ENVIRONMENT_PROFILE
    ):
        fail("production capacity receipt fixed contract fields are invalid")
    root_raw = Path(receipt["evidence_root_path"])
    if not root_raw.is_absolute() or root_raw.is_symlink() or not root_raw.is_dir():
        fail("production capacity receipt evidence root must be an absolute non-symlink directory")
    root = root_raw.resolve(strict=True)
    try:
        report_path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError):
        fail("production capacity receipt is outside its evidence root")
    record_path = safe_absolute(root, receipt["record_path"], "production capacity receipt.record_path")
    if sha256_file(record_path) != require_sha256(receipt["record_sha256"], "production capacity receipt.record_sha256"):
        fail("production capacity receipt record hash mismatch")
    if final:
        record = validate_production_certificate(load_json(record_path, "production capacity certificate"), root, record_path)
    else:
        record = validate_production_intermediate(load_json(record_path, "production capacity unsigned intermediate"), root, record_path)
    expected = production_receipt_values(record, root, record_path, final=final)
    if receipt != expected:
        fail("production capacity receipt does not exactly match the validated record")
    completed = max(
        parse_utc(repetition["completed_utc"], "production repetition completed_utc")
        for repetition in load_json(
            safe_artifact(root, record["campaign"]["path"], "production capacity receipt campaign"),
            "production capacity receipt campaign",
        )["repetitions"]
    )
    max_age = int(os.environ.get("KVNODE_CAPACITY_MAX_EVIDENCE_AGE_SECONDS", "86400"))
    now = datetime.now(timezone.utc)
    if max_age <= 0 or max_age > 604800 or completed > now or (now - completed).total_seconds() > max_age:
        fail("production capacity evidence is future-dated, stale, or uses an invalid age policy")
    return record


def verify_receipt(report_path: Path) -> dict[str, Any]:
    validate_schema_contract()
    receipt = parse_env_receipt(report_path)
    status = receipt.get("status", "")
    if status in {
        "darwin-capacity-certification-unsigned-intermediate-v2",
        "darwin-production-capacity-certification-v2",
        "synthetic-darwin-production-capacity-certification-v2",
    }:
        return verify_production_receipt(report_path, receipt)
    return verify_characterization_receipt(report_path)


def verification_stdout(record: dict[str, Any], report_path: Path) -> str:
    if record["evidence_class"] == "workstation-capacity-characterization":
        evidence_mode = "characterization"
        certification = "not-claimed"
        release_claim = "none-characterization-nonclaim"
    else:
        evidence_mode = record["evidence_mode"]
        certification = (
            "approved"
            if record["evidence_class"] == PRODUCTION_FINAL_CLASS
            else "test-only-not-production"
            if record["evidence_class"] == PRODUCTION_TEST_CLASS
            else "not-approved"
        )
        release_claim = record["release_claim"]
    return (
        "kvnode-capacity-v2 verification=pass"
        f" evidence_class={record['evidence_class']}"
        f" evidence_mode={evidence_mode}"
        f" production_capacity_certification={certification}"
        f" release_claim={release_claim}"
        f" target_id={record['target']['target_id']}"
        f" environment_profile={record['target']['environment_profile']}"
        f" release_id={record['target']['release_id']}"
        f" source_revision={record['target']['source_revision']}"
        f" binary_sha256={record['target']['binary_sha256']}"
        f" report={report_path}"
    )


def production_assembly_stdout(record: dict[str, Any], report_path: Path) -> str:
    return (
        "kvnode-capacity-v2 production-assembly=complete"
        f" evidence_class={record['evidence_class']}"
        f" evidence_mode={record['evidence_mode']}"
        " production_capacity_certification=not-approved"
        " release_claim=none-unsigned-intermediate"
        f" report={report_path}"
    )


def production_certification_stdout(record: dict[str, Any], report_path: Path) -> str:
    certification = "approved" if record["evidence_class"] == PRODUCTION_FINAL_CLASS else "test-only-not-production"
    return (
        "kvnode-capacity-v2 production-certification=complete"
        f" evidence_class={record['evidence_class']}"
        f" evidence_mode={record['evidence_mode']}"
        f" production_capacity_certification={certification}"
        f" release_claim={record['release_claim']}"
        f" report={report_path}"
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("config", type=Path)
    collect_parser.add_argument("out_dir", type=Path)
    assemble_parser = subparsers.add_parser("assemble")
    assemble_parser.add_argument("measurement", type=Path)
    assemble_parser.add_argument("review", type=Path)
    assemble_parser.add_argument("report", type=Path)
    production_assemble_parser = subparsers.add_parser("assemble-production")
    production_assemble_parser.add_argument("campaign", type=Path)
    production_assemble_parser.add_argument("report", type=Path)
    production_certify_parser = subparsers.add_parser("certify-production")
    production_certify_parser.add_argument("intermediate", type=Path)
    production_certify_parser.add_argument("approval", type=Path)
    production_certify_parser.add_argument("trust_root", type=Path)
    production_certify_parser.add_argument("signature", type=Path)
    production_certify_parser.add_argument("report", type=Path)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("report", type=Path)
    fd_parser = subparsers.add_parser("darwin-fd-count")
    fd_parser.add_argument("pid", type=int)
    args = parser.parse_args(argv)
    if args.command == "collect":
        path = collect(args.config, args.out_dir)
        print(f"kvnode-capacity-v2 collection=complete measurement={path}")
    elif args.command == "assemble":
        path = assemble(args.measurement, args.review, args.report)
        print(f"kvnode-capacity-v2 assembly=complete report={path}")
    elif args.command == "assemble-production":
        path = assemble_production(args.campaign, args.report)
        record = verify_receipt(path)
        print(production_assembly_stdout(record, path))
    elif args.command == "certify-production":
        path = certify_production(
            args.intermediate,
            args.approval,
            args.trust_root,
            args.signature,
            args.report,
        )
        record = verify_receipt(path)
        print(production_certification_stdout(record, path))
    elif args.command == "verify":
        record = verify_receipt(args.report)
        print(verification_stdout(record, args.report))
    elif args.command == "darwin-fd-count":
        print(darwin_numeric_fd_count(args.pid))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except EvidenceError as exc:
        print(f"kvnode-capacity-v2 status=fail reason={exc}", file=sys.stderr)
        raise SystemExit(2)
