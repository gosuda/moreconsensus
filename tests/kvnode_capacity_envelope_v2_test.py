#!/usr/bin/env python3
from __future__ import annotations

import copy
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tests/kvnode_capacity_envelope_v2.py"
SPEC = importlib.util.spec_from_file_location("kvnode_capacity_envelope_v2", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
CAPACITY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CAPACITY
SPEC.loader.exec_module(CAPACITY)


def utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")


class CapacityV2Fixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.binary = root / "kvnode"
        shutil.copyfile("/bin/echo", self.binary)
        self.binary.chmod(0o500)
        self.binary_sha256 = CAPACITY.sha256_file(self.binary)
        self.binding = {
            "target_id": CAPACITY.TARGET_ID,
            "release_id": "mc-kv-aaaaaaaaaaaa-r1",
            "source_revision": "a" * 40,
            "binary_sha256": self.binary_sha256,
            "environment_profile": CAPACITY.ENVIRONMENT_PROFILE,
        }
        now = datetime.now(timezone.utc)
        self.approved = now - timedelta(minutes=20)
        self.started = now - timedelta(minutes=10)
        self.completed = self.started + timedelta(seconds=12)
        self.reviewed = now - timedelta(minutes=1)
        self.phase_rows: list[dict[str, Any]] = []
        self.request_rows: list[dict[str, Any]] = []
        self.process_rows: list[dict[str, Any]] = []
        self.disk_rows: list[dict[str, Any]] = []
        self.disk_raw_rows: list[dict[str, Any]] = []
        self.nodes = [
            {
                "node_id": node_id,
                "launchd_label": f"org.gosuda.moreconsensus.kvnode.{node_id}",
                "pid": 1000 + node_id,
                "process_start_token": 5000 + node_id,
                "executable_path": f"/var/db/moreconsensus/bin/kvnode-{node_id}",
                "data_dir": f"/var/db/moreconsensus/campaign/data/node{node_id}",
                "client_url": f"https://127.0.0.1:{18990 + node_id * 100}",
                "admin_url": f"https://127.0.0.1:{18992 + node_id * 100}",
            }
            for node_id in (1, 2, 3)
        ]
        base_ns = 10_000_000_000
        sample_index = 0
        for phase_index, phase in enumerate(CAPACITY.PHASES):
            phase_start = base_ns + phase_index * 2_000_000_000
            phase_end = phase_start + 1_000_000_000
            phase_start_utc = self.started + timedelta(seconds=phase_index * 2)
            phase_end_utc = phase_start_utc + timedelta(seconds=1)
            self.phase_rows.append(
                {
                    "binding": self.sample_binding(),
                    "phase": phase,
                    "planned_operations": 2,
                    "concurrency": 1,
                    "started_utc": utc(phase_start_utc),
                    "completed_utc": utc(phase_end_utc),
                    "started_monotonic_ns": phase_start,
                    "completed_monotonic_ns": phase_end,
                }
            )
            for operation_index, offset in enumerate((200_000_000, 500_000_000)):
                request_start = phase_start + offset
                request_latency = 100_000_000 + operation_index * 10_000_000
                self.request_rows.append(
                    {
                        "binding": self.sample_binding(),
                        "phase": phase,
                        "worker": 1,
                        "operation_index": operation_index,
                        "operation": "put" if operation_index == 0 else "get",
                        "node_id": operation_index + 1,
                        "started_utc": utc(phase_start_utc + timedelta(seconds=offset / 1_000_000_000)),
                        "started_monotonic_ns": request_start,
                        "ended_monotonic_ns": request_start + request_latency,
                        "latency_ns": request_latency,
                        "http_status": 200,
                        "outcome": "success",
                    }
                )
            for offset in (100_000_000, 900_000_000):
                sample_index += 1
                counter = sample_index * 1000
                self.process_rows.append(
                    {
                        "binding": self.sample_binding(),
                        "sample_index": sample_index,
                        "timestamp_utc": utc(phase_start_utc + timedelta(seconds=offset / 1_000_000_000)),
                        "monotonic_ns": phase_start + offset,
                        "phase": phase,
                        "provenance": copy.deepcopy(CAPACITY.SAMPLE_PROVENANCE),
                        "nodes": [
                            {
                                "node_id": node["node_id"],
                                "pid": node["pid"],
                                "process_start_token": node["process_start_token"],
                                "executable_path": node["executable_path"],
                                "observed_binary_sha256": self.binary_sha256,
                                "cpu_user_time_ns": counter + node["node_id"],
                                "cpu_system_time_ns": counter // 2 + node["node_id"],
                                "cpu_utilization_percent": 10.0 + sample_index,
                                "rss_bytes": 100_000 + counter + node["node_id"],
                                "physical_footprint_bytes": 120_000 + counter + node["node_id"],
                                "observed_peak_rss_bytes": 100_000 + counter + node["node_id"],
                                "open_fd_count": 20 + node["node_id"],
                                "disk_read_bytes": counter * 10 + node["node_id"],
                                "disk_written_bytes": counter * 20 + node["node_id"],
                                "ready_http_status": 200,
                                "send_queue_depth": 0,
                                "epaxos_instances": counter + node["node_id"],
                                "epaxos_executed": counter + node["node_id"],
                                "process_restart_count": 0,
                            }
                            for node in self.nodes
                        ],
                        "data_dirs": [
                            {
                                "node_id": node["node_id"],
                                "data_dir": node["data_dir"],
                                "apfs_allocated_bytes": 1_000_000 + counter + node["node_id"],
                                "apfs_logical_bytes": 2_000_000 + counter + node["node_id"],
                            }
                            for node in self.nodes
                        ],
                    }
                )
            for node in self.nodes:
                completed_ns = phase_start + 650_000_000 + node["node_id"] * 10_000_000
                latency_ns = 1_000_000 + phase_index * 10 + node["node_id"]
                self.disk_raw_rows.append(
                    {
                        "binding": self.sample_binding(),
                        "timestamp_utc": utc(phase_start_utc + timedelta(seconds=0.7)),
                        "phase": phase,
                        "node_id": node["node_id"],
                        "data_dir": node["data_dir"],
                        "probe_bytes": 4096,
                        "fsync_started_monotonic_ns": completed_ns - latency_ns,
                        "fsync_completed_monotonic_ns": completed_ns,
                        "status": "ok",
                        "provenance": "darwin-apfs-fsync-probe-per-data-dir",
                    }
                )
                self.disk_rows.append(
                    {
                        "binding": self.sample_binding(),
                        "timestamp_utc": utc(phase_start_utc + timedelta(seconds=0.7)),
                        "monotonic_ns": completed_ns,
                        "phase": phase,
                        "node_id": node["node_id"],
                        "data_dir": node["data_dir"],
                        "latency_ns": latency_ns,
                        "provenance": "darwin-apfs-fsync-probe-per-data-dir",
                    }
                )
        envelope_without_hash = {
            "observed": True,
            "approved_before_run_utc": utc(self.approved),
            "cpu": {"unit": "percent", "limit": 100.0, "isolation_status": "none-observation-only", "enforcement_artifact": None},
            "memory": {"unit": "bytes", "limit": 10_000_000},
            "open_fds": {"unit": "count", "limit": 100},
            "apfs_allocated_growth": {"unit": "bytes", "limit": 1_000_000},
            "disk_latency_p99": {"unit": "nanoseconds", "limit": 10_000_000},
            "request_throughput_minimum": {"unit": "operations-per-second", "limit": 1.0},
            "request_latency_p99": {"unit": "nanoseconds", "limit": 1_000_000_000},
            "request_error_rate_maximum_percent": {"unit": "percent", "limit": 0.01},
            "send_queue_depth": {"unit": "count", "limit": 64},
        }
        envelope_without_hash["memory"]["isolation_status"] = "none-observation-only"
        envelope_without_hash["memory"]["enforcement_artifact"] = None
        self.plan = {
            "schema": "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-pre-run-plan-v2.json",
            "verifier_version": CAPACITY.VERIFIER_VERSION,
            "binding": self.sample_binding(),
            "operator_identity": "operator@example.invalid",
            "approved_at_utc": utc(self.approved),
            "phase_order": list(CAPACITY.PHASES),
            "phases": {phase: {"operations": 2, "concurrency": 1} for phase in CAPACITY.PHASES},
            "resource_envelope": envelope_without_hash,
            "host_policy": {
                "power": "ac-required",
                "sleep": "forbidden-during-run",
                "competing_workload": "forbidden-undeclared",
            },
        }
        self.plan_path = root / "pre-run-plan.json"
        write_json(self.plan_path, self.plan)
        self.envelope = copy.deepcopy(envelope_without_hash)
        self.envelope["plan_sha256"] = CAPACITY.sha256_file(self.plan_path)
        tls_ca_sha256 = "c" * 64
        self.environment = {
            "schema": "https://gosuda.org/moreconsensus/schemas/kvnode-capacity-environment-v2.json",
            "binding": self.sample_binding(),
            "execution_mode": "native",
            "platform": "darwin",
            "architecture": "arm64",
            "binary_format": "mach-o-64-arm64",
            "filesystem": "apfs",
            "network_scope": "same-host-loopback",
            "tls_scope": "server-auth-only",
            "tls_ca_sha256": tls_ca_sha256,
            "observed_commands": [
                {"argv": ["uname", "-srm"], "returncode": 0, "stdout": "Darwin fixture 24.6.0 arm64", "stderr": ""},
                {"argv": ["sw_vers"], "returncode": 0, "stdout": "ProductVersion: 15.6", "stderr": ""},
                {"argv": ["file", str(self.binary)], "returncode": 0, "stdout": "Mach-O 64-bit executable arm64", "stderr": ""},
                {"argv": ["go", "version", "-m", str(self.binary)], "returncode": 0, "stdout": f"build vcs.revision={self.binding['source_revision']} vcs.modified=false", "stderr": ""},
                {"argv": ["sysctl", "-n", "kern.hv_vmm_present"], "returncode": 0, "stdout": "0", "stderr": ""},
                {"argv": ["system_profiler", "SPHardwareDataType"], "returncode": 0, "stdout": "Chip: Apple fixture", "stderr": ""},
                *[
                    {"argv": ["stat", "-f", "%T", node["data_dir"]], "returncode": 0, "stdout": "apfs", "stderr": ""}
                    for node in self.nodes
                ],
                *[
                    {"argv": ["diskutil", "info", node["data_dir"]], "returncode": 0, "stdout": "Type (Bundle): apfs", "stderr": ""}
                    for node in self.nodes
                ],
                *[
                    {"argv": ["launchctl", "print", f"system/{node['launchd_label']}"], "returncode": 0, "stdout": f"pid = {node['pid']}", "stderr": ""}
                    for node in self.nodes
                ],
            ],
        }
        self.native_environment = {
            "platform": "darwin",
            "architecture": "arm64",
            "binary_format": "mach-o-64-arm64",
            "execution_mode": "native",
            "supervisor": "launchd",
            "launchd_domain": "system",
            "filesystem": "apfs",
            "network_scope": "same-host-loopback",
            "tls_scope": "server-auth-only",
            "host_id": "fixture-darwin-host",
            "os_version": "15.6",
            "kernel_release": "24.6.0",
            "tls_ca_sha256": tls_ca_sha256,
        }
        self.nonclaims = sorted(CAPACITY.REQUIRED_NONCLAIMS)
        self.paths = {
            "environment-observations": root / "environment-observations.json",
            "pre-run-plan": self.plan_path,
            "phase-windows": root / "phase-windows.json",
            "request-samples": root / "request-samples.jsonl",
            "process-samples": root / "process-samples.jsonl",
            "disk-latency-raw": root / "disk-latency-apfs-fsync-raw.jsonl",
            "disk-latency-samples": root / "disk-latency-samples.jsonl",
        }
        write_json(self.paths["environment-observations"], self.environment)
        write_json(self.paths["phase-windows"], self.phase_rows)
        write_jsonl(self.paths["request-samples"], self.request_rows)
        write_jsonl(self.paths["process-samples"], self.process_rows)
        write_jsonl(self.paths["disk-latency-raw"], self.disk_raw_rows)
        write_jsonl(self.paths["disk-latency-samples"], self.disk_rows)
        self.refresh_measurement()
        self.write_review()

    def sample_binding(self) -> dict[str, Any]:
        return {"target": copy.deepcopy(self.binding), "verifier_version": CAPACITY.VERIFIER_VERSION}

    def refresh_measurement(self, *, recompute_summary: bool = True) -> None:
        artifacts = [CAPACITY.artifact_entry(self.root, path, kind) for kind, path in self.paths.items()]
        if not hasattr(self, "summary") or recompute_summary:
            self.summary = CAPACITY.recompute_summary(
                self.binding,
                self.nodes,
                self.envelope,
                1.0,
                self.phase_rows,
                self.request_rows,
                self.process_rows,
                self.disk_rows,
            )
        self.measurement = {
            "schema": CAPACITY.MEASUREMENT_SCHEMA_URI,
            "verifier_version": CAPACITY.VERIFIER_VERSION,
            "evidence_class": "workstation-capacity-characterization",
            "production_capacity_certification": False,
            "claim_scope": "native-darwin-same-host-bounded-characterization",
            "operator_identity": "operator@example.invalid",
            "target": copy.deepcopy(self.binding),
            "native_environment": copy.deepcopy(self.native_environment),
            "nodes": copy.deepcopy(self.nodes),
            "resource_envelope": copy.deepcopy(self.envelope),
            "sampling": {
                "interval_seconds": 1.0,
                "started_utc": utc(self.started - timedelta(seconds=1)),
                "completed_utc": utc(self.completed),
                "phase_order": list(CAPACITY.PHASES),
                "measurement_phase_order": list(CAPACITY.MEASUREMENT_PHASES),
                "sample_count": len(self.process_rows),
                "sample_provenance": copy.deepcopy(CAPACITY.SAMPLE_PROVENANCE),
            },
            "summary": copy.deepcopy(self.summary),
            "artifacts": artifacts,
            "nonclaims": list(self.nonclaims),
        }
        self.measurement_path = self.root / "measurement.json"
        write_json(self.measurement_path, self.measurement)

    def write_review(self, *, reviewer: str = "reviewer@example.invalid") -> None:
        self.review = {
            "schema": CAPACITY.REVIEW_SCHEMA_URI,
            "verifier_version": CAPACITY.VERIFIER_VERSION,
            "binding": self.sample_binding(),
            "operator_identity": "operator@example.invalid",
            "reviewer_identity": reviewer,
            "decision": "approved-workstation-characterization",
            "reviewed_at_utc": utc(self.reviewed),
            "measurement_sha256": CAPACITY.sha256_file(self.measurement_path),
            "observations": {
                "raw_hashes_verified": True,
                "resource_envelope_verified": True,
                "pid_binary_bindings_verified": True,
                "warmup_exclusion_verified": True,
                "nonclaims_verified": True,
                "production_capacity_not_certified": True,
            },
        }
        self.review_path = self.root / "independent-review.json"
        write_json(self.review_path, self.review)

    def rewrite_raw(self, kind: str) -> None:
        if kind == "environment-observations":
            write_json(self.paths[kind], self.environment)
        elif kind == "phase-windows":
            write_json(self.paths[kind], self.phase_rows)
        elif kind == "request-samples":
            write_jsonl(self.paths[kind], self.request_rows)
        elif kind == "process-samples":
            write_jsonl(self.paths[kind], self.process_rows)
        elif kind == "disk-latency-raw":
            write_jsonl(self.paths[kind], self.disk_raw_rows)
        elif kind == "disk-latency-samples":
            write_jsonl(self.paths[kind], self.disk_rows)
        else:
            raise AssertionError(kind)
        self.refresh_measurement(recompute_summary=False)
        self.write_review()

    def assemble(self) -> Path:
        report = self.root / "report.env"
        CAPACITY.assemble(self.measurement_path, self.review_path, report)
        return report


class CapacityEnvelopeDarwinV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="capacity-v2-test.")
        self.root = Path(self.temp.name)
        self.fixture = CapacityV2Fixture(self.root)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def assert_assemble_fails(self) -> None:
        with self.assertRaises(CAPACITY.EvidenceError):
            self.fixture.assemble()

    def test_positive_fixture_dispatches_through_preserved_shell_verifier(self) -> None:
        report = self.fixture.assemble()
        completed = subprocess.run(
            ["bash", "tests/kvnode_capacity_envelope.sh", "--validate-report", str(report)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            completed.stdout.strip(),
            (
                "kvnode-capacity-v2 verification=pass"
                " evidence_class=workstation-capacity-characterization"
                " evidence_mode=characterization"
                " production_capacity_certification=not-claimed"
                " release_claim=none-characterization-nonclaim"
                f" target_id={CAPACITY.TARGET_ID}"
                f" environment_profile={CAPACITY.ENVIRONMENT_PROFILE}"
                f" release_id={self.fixture.binding['release_id']}"
                f" source_revision={self.fixture.binding['source_revision']}"
                f" binary_sha256={self.fixture.binding['binary_sha256']}"
                f" report={report}"
            ),
        )
        receipt = CAPACITY.parse_env_receipt(report)
        self.assertEqual(receipt["production_capacity_certification"], "not-claimed")

    def test_numeric_fd_parser_ignores_non_descriptor_lsof_rows(self) -> None:
        observed = "p123\nfcwd\nftxt\nf0\nf1u\nf17r\nmem\n"
        self.assertEqual(CAPACITY.parse_numeric_lsof_fds(observed), {0, 1, 17})
        self.assertGreater(CAPACITY.darwin_numeric_fd_count(os.getpid()), 0)

    def test_rejects_container_or_linux_observer(self) -> None:
        self.fixture.environment["observed_commands"][0]["argv"] = ["cat", "/proc/1001/status"]
        self.fixture.rewrite_raw("environment-observations")
        self.assert_assemble_fails()

    def test_rejects_duplicate_exact_pid_set(self) -> None:
        self.fixture.measurement["nodes"][1]["pid"] = self.fixture.measurement["nodes"][0]["pid"]
        write_json(self.fixture.measurement_path, self.fixture.measurement)
        self.fixture.write_review()
        self.assert_assemble_fails()

    def test_rejects_pid_reuse(self) -> None:
        self.fixture.process_rows[3]["nodes"][0]["process_start_token"] += 1
        self.fixture.rewrite_raw("process-samples")
        self.assert_assemble_fails()

    def test_rejects_observed_binary_mismatch(self) -> None:
        self.fixture.process_rows[2]["nodes"][1]["observed_binary_sha256"] = "b" * 64
        self.fixture.rewrite_raw("process-samples")
        self.assert_assemble_fails()

    def test_rejects_missing_raw_samples(self) -> None:
        self.fixture.paths["process-samples"].unlink()
        self.assert_assemble_fails()

    def test_rejects_unit_confusion(self) -> None:
        self.fixture.process_rows[0]["nodes"][0]["rss_bytes"] = "100 KiB"
        self.fixture.rewrite_raw("process-samples")
        self.assert_assemble_fails()

    def test_rejects_impossible_monotonic_counter(self) -> None:
        self.fixture.process_rows[4]["nodes"][0]["disk_written_bytes"] = 1
        self.fixture.rewrite_raw("process-samples")
        self.assert_assemble_fails()

    def test_rejects_phase_overlap(self) -> None:
        self.fixture.phase_rows[1]["started_monotonic_ns"] = self.fixture.phase_rows[0]["completed_monotonic_ns"] - 1
        self.fixture.rewrite_raw("phase-windows")
        self.assert_assemble_fails()

    def test_rejects_warmup_contamination(self) -> None:
        self.fixture.summary["measurement_attempts"] += self.fixture.summary["warmup_attempts"]
        self.fixture.refresh_measurement(recompute_summary=False)
        self.fixture.write_review()
        self.assert_assemble_fails()

    def test_rejects_percentile_order_or_wrong_nearest_rank(self) -> None:
        self.fixture.summary["successful_request_latency_p50_ns"] = self.fixture.summary["successful_request_latency_p99_ns"] + 1
        self.fixture.refresh_measurement(recompute_summary=False)
        self.fixture.write_review()
        self.assert_assemble_fails()

    def test_rejects_zero_work_pass(self) -> None:
        self.fixture.request_rows = [row for row in self.fixture.request_rows if row["phase"] != "steady"]
        self.fixture.rewrite_raw("request-samples")
        self.assert_assemble_fails()

    def test_rejects_claimed_cpu_isolation_without_enforcement(self) -> None:
        self.fixture.measurement["resource_envelope"]["cpu"]["isolation_status"] = "enforced"
        self.fixture.measurement["production_capacity_certification"] = True
        self.fixture.measurement["evidence_class"] = "production-capacity-proof"
        write_json(self.fixture.measurement_path, self.fixture.measurement)
        self.fixture.write_review()
        self.assert_assemble_fails()

    def test_rejects_semantically_wrong_fd_provenance(self) -> None:
        self.fixture.process_rows[0]["provenance"]["fd_count"] = "linux-proc-fd-count"
        self.fixture.rewrite_raw("process-samples")
        self.assert_assemble_fails()

    def test_rejects_missing_independent_review(self) -> None:
        report = self.fixture.assemble()
        self.fixture.review_path.unlink()
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.verify_receipt(report)

    def test_rejects_self_review(self) -> None:
        self.fixture.write_review(reviewer="operator@example.invalid")
        self.assert_assemble_fails()

    def test_rejects_request_outside_phase_window(self) -> None:
        self.fixture.request_rows[2]["started_monotonic_ns"] = self.fixture.phase_rows[0]["started_monotonic_ns"]
        self.fixture.request_rows[2]["ended_monotonic_ns"] = self.fixture.request_rows[2]["started_monotonic_ns"] + self.fixture.request_rows[2]["latency_ns"]
        self.fixture.rewrite_raw("request-samples")
        self.assert_assemble_fails()

    def test_rejects_process_restart_signal(self) -> None:
        self.fixture.process_rows[-1]["nodes"][2]["process_restart_count"] = 1
        self.fixture.rewrite_raw("process-samples")
        self.assert_assemble_fails()

    def test_rejects_duplicate_phase_operation_index(self) -> None:
        self.fixture.request_rows[1]["operation_index"] = 0
        self.fixture.request_rows[1]["operation"] = "put"
        self.fixture.rewrite_raw("request-samples")
        self.assert_assemble_fails()

    def test_rejects_duplicate_node_url(self) -> None:
        self.fixture.measurement["nodes"][1]["client_url"] = self.fixture.measurement["nodes"][0]["client_url"]
        write_json(self.fixture.measurement_path, self.fixture.measurement)
        self.fixture.write_review()
        self.assert_assemble_fails()

    def test_rejects_apfs_observation_for_unbound_directory(self) -> None:
        stat_observation = next(
            command
            for command in self.fixture.environment["observed_commands"]
            if command["argv"][0] == "stat"
        )
        stat_observation["argv"][-1] = "/tmp/not-a-node-data-directory"
        self.fixture.rewrite_raw("environment-observations")
        self.assert_assemble_fails()

    def test_concurrent_workload_seeds_each_worker_before_get_or_scan(self) -> None:
        calls: list[tuple[int, int, int]] = []

        def observe(
            context: Any,
            phase: str,
            worker: int,
            operation_index: int,
            operation_slot: int,
            key: str,
            value: bytes,
        ) -> dict[str, int]:
            calls.append((worker, operation_index, operation_slot))
            return {"started_monotonic_ns": operation_index}

        with mock.patch.object(CAPACITY, "request_once", side_effect=observe):
            CAPACITY.run_phase_workload(mock.Mock(), "steady", 7, 3, "synthetic-test")
        self.assertEqual(
            sorted(calls),
            [
                (1, 0, 0),
                (1, 3, 1),
                (1, 6, 2),
                (2, 1, 0),
                (2, 4, 1),
                (3, 2, 0),
                (3, 5, 1),
            ],
        )

    def test_rejects_root_collector(self) -> None:
        with (
            mock.patch.object(CAPACITY.platform, "system", return_value="Darwin"),
            mock.patch.object(CAPACITY.platform, "machine", return_value="arm64"),
            mock.patch.object(CAPACITY.os, "geteuid", return_value=0),
            self.assertRaises(CAPACITY.EvidenceError),
        ):
            CAPACITY.validate_native_host(self.fixture.binary, self.fixture.nodes, self.fixture.binding)

    def test_fails_closed_when_numeric_fd_observation_is_unavailable(self) -> None:
        unavailable = {"argv": ["lsof"], "returncode": 1, "stdout": "", "stderr": "permission denied"}
        with mock.patch.object(CAPACITY, "run_observation", return_value=unavailable), self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.darwin_numeric_fd_count(self.fixture.nodes[0]["pid"])

    def test_rejects_tampered_raw_apfs_fsync_probe(self) -> None:
        self.fixture.disk_raw_rows[0]["fsync_completed_monotonic_ns"] += 1
        self.fixture.rewrite_raw("disk-latency-raw")
        self.assert_assemble_fails()

    def test_rejects_launchd_pid_mismatch(self) -> None:
        launchctl = next(
            command
            for command in self.fixture.environment["observed_commands"]
            if command["argv"][0] == "launchctl"
        )
        launchctl["stdout"] = "pid = 999999"
        self.fixture.rewrite_raw("environment-observations")
        self.assert_assemble_fails()

    def test_rejects_tls_ca_binding_mismatch(self) -> None:
        self.fixture.measurement["native_environment"]["tls_ca_sha256"] = "d" * 64
        write_json(self.fixture.measurement_path, self.fixture.measurement)
        self.fixture.write_review()
        self.assert_assemble_fails()

    def test_rejects_characterization_receipt_relabelled_as_production(self) -> None:
        report = self.fixture.assemble()
        receipt = CAPACITY.parse_env_receipt(report)
        receipt["status"] = "darwin-production-capacity-certification-v2"
        receipt["evidence_class"] = CAPACITY.PRODUCTION_FINAL_CLASS
        receipt["evidence_mode"] = "target"
        receipt["production_capacity_certification"] = "approved"
        receipt["release_claim"] = CAPACITY.PRODUCTION_RELEASE_CLAIM
        report.write_text(
            "".join(f"{key}={value}\n" for key, value in receipt.items()),
            encoding="utf-8",
        )
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.verify_receipt(report)

    def test_schema_fixes_characterization_and_nonproduction_claim(self) -> None:
        CAPACITY.validate_schema_contract()
        schema = json.loads((ROOT / "release/evidence/schema/kvnode-capacity-evidence-v2.schema.json").read_text(encoding="utf-8"))
        self.assertEqual(schema["properties"]["evidence_class"]["const"], "workstation-capacity-characterization")
        self.assertIs(schema["properties"]["production_capacity_certification"]["const"], False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
