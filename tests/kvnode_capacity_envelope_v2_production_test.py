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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tests/kvnode_capacity_envelope_v2.py"
SPEC = importlib.util.spec_from_file_location("kvnode_capacity_envelope_v2_production", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
CAPACITY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CAPACITY
SPEC.loader.exec_module(CAPACITY)


def utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


class ProductionCapacityFixture:
    def __init__(self, base: Path) -> None:
        self.base = base
        self.root = base / "evidence"
        self.keys = base / "external-keys"
        self.root.mkdir()
        self.keys.mkdir()
        self.binary = self.root / "kvnode"
        shutil.copyfile("/bin/echo", self.binary)
        self.binary.chmod(0o500)
        self.binding = {
            "target_id": CAPACITY.TARGET_ID,
            "release_id": "synthetic-mc-kv-capacity-r1",
            "source_revision": "a" * 40,
            "binary_sha256": CAPACITY.sha256_file(self.binary),
            "environment_profile": CAPACITY.ENVIRONMENT_PROFILE,
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
            "tls_scope": "mutual-auth-separated-planes",
            "host_id": "synthetic-darwin-host.invalid",
            "os_version": "15.6-synthetic",
            "kernel_release": "24.6.0",
            "tls_ca_sha256": "c" * 64,
        }
        self.support_envelope = {
            "topology": {
                "node_count": 3,
                "host_scope": "same-host",
                "network_scope": "loopback",
                "supervisor": "launchd-system",
                "filesystem": "apfs",
            },
            "workload": {
                "name": "synthetic-deterministic-kv-mix-v1",
                "operation_sequence": ["put", "get", "scan"],
                "key_seed": "synthetic-seed-not-production",
                "key_count": 32,
                "value_sizes_bytes": [64, 1024, 65536, 1048576],
                "warmup_operations": 3,
                "measurement_operations": 6,
                "concurrency": 2,
            },
            "isolation": {
                "dedicated_host_required": True,
                "virtualization_forbidden": True,
                "ac_power_required": True,
                "sleep_forbidden": True,
                "competing_workloads_forbidden": True,
            },
        }
        self.thresholds = {
            "throughput": {"unit": "operations-per-second", "direction": "minimum", "limit": 1.0},
            "request_latency_p99": {"unit": "nanoseconds", "direction": "maximum", "limit": 1_000_000_000},
            "rss": {"unit": "bytes", "direction": "maximum", "limit": 10_000_000},
            "open_fds": {"unit": "count", "direction": "maximum", "limit": 100},
            "disk_allocated_growth": {"unit": "bytes", "direction": "maximum", "limit": 1_000_000},
            "disk_written": {"unit": "bytes", "direction": "maximum", "limit": 1_000_000},
            "fsync_latency_p99": {"unit": "nanoseconds", "direction": "maximum", "limit": 10_000_000},
            "process_cpu": {"unit": "percent", "direction": "maximum", "limit": 90.0},
            "system_cpu": {"unit": "percent", "direction": "maximum", "limit": 95.0},
            "memory_pressure": {"unit": "percent", "direction": "maximum", "limit": 90.0},
            "filesystem_free": {"unit": "bytes", "direction": "minimum", "limit": 1_000_000},
            "request_error_rate": {"unit": "percent", "direction": "maximum", "limit": 0.0},
        }
        self.binary_provenance_path = self.root / "binary-provenance.json"
        write_json(
            self.binary_provenance_path,
            {
                "schema": "https://gosuda.org/moreconsensus/schemas/kvnode-production-binary-provenance-v2.json",
                "verifier_version": CAPACITY.VERIFIER_VERSION,
                "evidence_class": "production-capacity-binary-provenance",
                "evidence_mode": "test-only-synthetic",
                "target": copy.deepcopy(self.binding),
                "binary_path": str(self.binary.resolve()),
                "observed_binary_sha256": self.binding["binary_sha256"],
                "source_revision": self.binding["source_revision"],
                "vcs_modified": False,
                "binary_format": "mach-o-64-arm64",
                "observed_commands": [
                    {"argv": ["file", str(self.binary.resolve())], "returncode": 0, "stdout": "Mach-O 64-bit executable arm64 synthetic", "stderr": ""},
                    {"argv": ["go", "version", "-m", str(self.binary.resolve())], "returncode": 0, "stdout": f"build\tvcs.revision={self.binding['source_revision']}\nbuild\tvcs.modified=false", "stderr": ""},
                ],
            },
        )
        now = datetime.now(timezone.utc)
        self.repetitions: list[dict[str, Any]] = []
        self.isolation_paths: list[Path] = []
        self.request_paths: list[Path] = []
        self.system_paths: list[Path] = []
        for sequence in range(1, 4):
            self.repetitions.append(self._make_repetition(sequence, now - timedelta(minutes=20 - sequence * 3)))
        self.campaign = {
            "schema": CAPACITY.PRODUCTION_CAMPAIGN_SCHEMA_URI,
            "verifier_version": CAPACITY.VERIFIER_VERSION,
            "evidence_class": "synthetic-production-capacity-campaign-test-only",
            "evidence_mode": "test-only-synthetic",
            "production_capacity_certification": False,
            "claim_scope": "target-bound-production-capacity-campaign-intermediate",
            "operator_identity": "synthetic-operator@example.invalid",
            "target": copy.deepcopy(self.binding),
            "native_environment": copy.deepcopy(self.native_environment),
            "support_envelope": copy.deepcopy(self.support_envelope),
            "thresholds": copy.deepcopy(self.thresholds),
            "binary_provenance": CAPACITY.artifact_entry(self.root, self.binary_provenance_path, "binary-provenance"),
            "repetition_count": 3,
            "repetitions": self.repetitions,
        }
        self.campaign_path = self.root / "production-campaign.json"
        self.write_campaign()

    def _make_repetition(self, sequence: int, started: datetime) -> dict[str, Any]:
        repetition_id = f"repetition-{sequence:03d}"
        directory = self.root / repetition_id
        directory.mkdir()
        base_ns = sequence * 100_000_000_000
        warmup_start = base_ns
        warmup_end = base_ns + 1_000_000_000
        measurement_start = base_ns + 1_100_000_000
        measurement_end = base_ns + 3_100_000_000
        repetition: dict[str, Any] = {
            "evidence_class": "production-capacity-repetition",
            "evidence_mode": "test-only-synthetic",
            "production_capacity_certification": False,
            "claim_scope": "target-bound-production-capacity-repetition-intermediate",
            "target": copy.deepcopy(self.binding),
            "native_environment": copy.deepcopy(self.native_environment),
            "support_envelope": copy.deepcopy(self.support_envelope),
            "repetition_id": repetition_id,
            "sequence": sequence,
            "repetition_count": 3,
            "started_utc": utc(started),
            "completed_utc": utc(started + timedelta(minutes=2)),
            "warmup_started_monotonic_ns": warmup_start,
            "warmup_completed_monotonic_ns": warmup_end,
            "measurement_started_monotonic_ns": measurement_start,
            "measurement_completed_monotonic_ns": measurement_end,
            "artifacts": [],
            "summary": {},
        }
        isolation_path = directory / "isolation.json"
        nodes: list[dict[str, Any]] = []
        expected_peer_list = "1=https://127.0.0.1:19091,2=https://127.0.0.1:19191,3=https://127.0.0.1:19291"
        for node_id in (1, 2, 3):
            executable_path = "/var/db/moreconsensus/bin/kvnode"
            data_dir = f"/var/db/moreconsensus/capacity/node{node_id}"
            client_url = f"https://127.0.0.1:{18990 + node_id * 100}"
            peer_url = f"https://127.0.0.1:{18991 + node_id * 100}"
            admin_url = f"https://127.0.0.1:{18992 + node_id * 100}"
            peer_cert_path = f"/var/db/moreconsensus/tls/node{node_id}-peer.pem"
            peer_key_path = f"/var/db/moreconsensus/tls/node{node_id}-peer-key.pem"
            peer_ca_path = "/var/db/moreconsensus/tls/peer-ca.pem"
            client_cert_path = f"/var/db/moreconsensus/tls/node{node_id}-client.pem"
            client_key_path = f"/var/db/moreconsensus/tls/node{node_id}-client-key.pem"
            client_ca_path = "/var/db/moreconsensus/tls/client-ca.pem"
            admin_cert_path = f"/var/db/moreconsensus/tls/node{node_id}-admin.pem"
            admin_key_path = f"/var/db/moreconsensus/tls/node{node_id}-admin-key.pem"
            admin_ca_path = "/var/db/moreconsensus/tls/admin-ca.pem"
            arguments = [
                executable_path,
                "-id", str(node_id),
                "-listen", client_url.removeprefix("https://"),
                "-peer-listen", peer_url.removeprefix("https://"),
                "-admin-listen", admin_url.removeprefix("https://"),
                "-data", data_dir,
                "-peers", expected_peer_list,
                "-request-deadline-ms", "5000",
                "-peer-deadline-ms", "2000",
                "-max-client-body-bytes", "1048576",
                "-max-peer-body-bytes", "2097152",
                "-max-admin-body-bytes", "65536",
                "-max-scan-limit", "1000",
                "-production=true",
                "-peer-tls-cert", peer_cert_path,
                "-peer-tls-key", peer_key_path,
                "-peer-tls-ca", peer_ca_path,
                "-client-tls-cert", client_cert_path,
                "-client-tls-key", client_key_path,
                "-client-client-ca", client_ca_path,
                "-admin-tls-cert", admin_cert_path,
                "-admin-tls-key", admin_key_path,
                "-admin-client-ca", admin_ca_path,
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
            pid = 2000 + sequence * 10 + node_id
            nodes.append(
                {
                    "node_id": node_id,
                    "launchd_label": f"org.gosuda.moreconsensus.kvnode.{node_id}",
                    "pid": pid,
                    "process_start_token": 5000 + sequence * 10 + node_id,
                    "executable_path": executable_path,
                    "data_dir": data_dir,
                    "client_url": client_url,
                    "peer_url": peer_url,
                    "admin_url": admin_url,
                    "peer_cert_path": peer_cert_path,
                    "peer_key_path": peer_key_path,
                    "peer_ca_path": peer_ca_path,
                    "client_cert_path": client_cert_path,
                    "client_key_path": client_key_path,
                    "client_ca_path": client_ca_path,
                    "admin_cert_path": admin_cert_path,
                    "admin_key_path": admin_key_path,
                    "admin_ca_path": admin_ca_path,
                    "program_arguments": arguments,
                    "listener_owner_pid": pid,
                    "listener_observation_provenance": "darwin-lsof-nP-listener-owner-v2",
                    "observed_binary_sha256": self.binding["binary_sha256"],
                    "observation_provenance": "darwin-libproc-proc_pidpath-rusage-and-sha256-v2",
                    "observed_at_utc": utc(started + timedelta(seconds=1)),
                }
            )
        workload_client_cert = "/var/db/moreconsensus/tls/capacity-client.pem"
        workload_client_key = "/var/db/moreconsensus/tls/capacity-client-key.pem"
        workload_client_ca = "/var/db/moreconsensus/tls/client-ca.pem"
        workload_client_cert_sha256 = "d" * 64
        workload_generator = {
            "pid": 3000 + sequence,
            "process_start_token": 6000 + sequence,
            "executable_path": "/usr/local/bin/kvnode-capacity-loadgen",
            "program_arguments": [
                "/usr/local/bin/kvnode-capacity-loadgen",
                "--campaign",
                repetition_id,
                "--client-cert",
                workload_client_cert,
                "--client-key",
                workload_client_key,
                "--ca",
                workload_client_ca,
            ],
            "observed_at_utc": utc(started + timedelta(seconds=1)),
            "observation_provenance": "darwin-libproc-proc_pidpath-rusage-v2",
            "workload_sha256": CAPACITY.canonical_json_sha256(self.support_envelope["workload"]),
            "client_cert_path": workload_client_cert,
            "client_key_path": workload_client_key,
            "client_ca_path": workload_client_ca,
            "client_cert_sha256": workload_client_cert_sha256,
        }
        process_lines = "".join(
            f"{node['pid']} 1 10.0 1000 {' '.join(node['program_arguments'])}\n"
            for node in nodes
        ) + f"{workload_generator['pid']} 1 5.0 1000 {' '.join(workload_generator['program_arguments'])}\n"
        commands = [
            {"argv": ["/usr/sbin/sysctl", "-n", "kern.hv_vmm_present"], "returncode": 0, "stdout": "0", "stderr": ""},
            {"argv": ["/usr/bin/pmset", "-g", "custom"], "returncode": 0, "stdout": "Battery Power:\n sleep 5\nAC Power:\n sleep 0", "stderr": ""},
            {"argv": ["/usr/bin/pmset", "-g", "batt"], "returncode": 0, "stdout": "Now drawing from 'AC Power'", "stderr": ""},
            {"argv": ["/bin/ps", "-axo", "pid=,ppid=,%cpu=,rss=,command="], "returncode": 0, "stdout": process_lines, "stderr": ""},
            {"argv": ["/usr/bin/shasum", "-a", "256", workload_client_cert], "returncode": 0, "stdout": f"{workload_client_cert_sha256}  {workload_client_cert}", "stderr": ""},
            *[
                {
                    "argv": ["/bin/launchctl", "print", f"system/{node['launchd_label']}"],
                    "returncode": 0,
                    "stdout": f"pid = {node['pid']}",
                    "stderr": "",
                }
                for node in nodes
            ],
            *[
                {"argv": ["/usr/bin/stat", "-f", "%T", node["data_dir"]], "returncode": 0, "stdout": "apfs", "stderr": ""}
                for node in nodes
            ],
            *[
                {"argv": ["/usr/sbin/diskutil", "info", node["data_dir"]], "returncode": 0, "stdout": "Type (Bundle): apfs", "stderr": ""}
                for node in nodes
            ],
        ]
        write_json(
            isolation_path,
            {
                "schema": "https://gosuda.org/moreconsensus/schemas/kvnode-production-isolation-observation-v2.json",
                "verifier_version": CAPACITY.VERIFIER_VERSION,
                "evidence_class": "production-capacity-isolation-observation",
                "evidence_mode": "test-only-synthetic",
                "target": copy.deepcopy(self.binding),
                "repetition_id": repetition_id,
                "host_id": self.native_environment["host_id"],
                "captured_at_utc": utc(started + timedelta(seconds=1)),
                "captured_monotonic_ns": warmup_start + 50_000_000,
                "dedicated_host": True,
                "same_host_nodes": True,
                "workload_generator_same_host": True,
                "virtualization_present": False,
                "ac_power": True,
                "sleep_disabled": True,
                "competing_workload_count": 0,
                "launchd_domain": "system",
                "network_scope": "same-host-loopback",
                "filesystem": "apfs",
                "nodes": nodes,
                "workload_generator": workload_generator,
                "observed_commands": commands,
            },
        )
        self.isolation_paths.append(isolation_path)
        requests: list[dict[str, Any]] = []
        for phase, count, window_start in (
            ("warmup", 3, warmup_start),
            ("measurement", 6, measurement_start),
        ):
            value_sizes = self.support_envelope["workload"]["value_sizes_bytes"]
            total_operations = count * len(value_sizes)
            for operation_index in range(total_operations):
                size_index = operation_index // count
                local_index = operation_index % count
                request_start = window_start + 100_000_000 + (operation_index // 2) * 100_000_000
                latency = 50_000_000
                operation = self.support_envelope["workload"]["operation_sequence"][local_index % 3]
                key_index = local_index % self.support_envelope["workload"]["key_count"]
                node_index = local_index % 3
                requests.append(
                    {
                        "target": copy.deepcopy(self.binding),
                        "repetition_id": repetition_id,
                        "sequence": sequence,
                        "phase": phase,
                        "operation_index": operation_index,
                        "operation": operation,
                        "key_index": key_index,
                        "value_bytes": value_sizes[size_index],
                        "node_id": node_index + 1,
                        "request_url": (
                            nodes[node_index]["client_url"]
                            + (
                                f"/scan?prefix={self.support_envelope['workload']['key_seed']}-{key_index:08d}&limit=16"
                                if operation == "scan"
                                else f"/kv/{self.support_envelope['workload']['key_seed']}-{key_index:08d}"
                            )
                        ),
                        "listener_owner_pid": nodes[node_index]["pid"],
                        "tls_ca_sha256": self.native_environment["tls_ca_sha256"],
                        "client_cert_sha256": workload_client_cert_sha256,
                        "started_utc": utc(started + timedelta(seconds=operation_index + 2)),
                        "started_monotonic_ns": request_start,
                        "ended_monotonic_ns": request_start + latency,
                        "latency_ns": latency,
                        "http_status": 200,
                        "outcome": "success",
                    }
                )
        request_path = directory / "requests.jsonl"
        write_jsonl(request_path, requests)
        self.request_paths.append(request_path)
        system_rows: list[dict[str, Any]] = []
        monotonic_points = [
            ("warmup", warmup_start + 100_000_000),
            ("warmup", warmup_start + 900_000_000),
            ("measurement", measurement_start + 100_000_000),
            ("measurement", measurement_start + 1_000_000_000),
            ("measurement", measurement_start + 1_900_000_000),
        ]
        for sample_index, (phase, monotonic_ns) in enumerate(monotonic_points, 1):
            system_rows.append(
                {
                    "target": copy.deepcopy(self.binding),
                    "repetition_id": repetition_id,
                    "sequence": sequence,
                    "phase": phase,
                    "sample_index": sample_index,
                    "timestamp_utc": utc(started + timedelta(seconds=sample_index)),
                    "monotonic_ns": monotonic_ns,
                    "provenance": copy.deepcopy(CAPACITY.PRODUCTION_SYSTEM_PROVENANCE),
                    "aggregation_scope": "three-node-cluster-total",
                    "process_bindings": [
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
                        for node in nodes
                    ],
                    "active_power_source": "AC Power",
                    "sleep_disabled": True,
                    "virtualization_present": False,
                    "competing_workload_count": 0,
                    "isolation_provenance": "darwin-pmset-sysctl-ps-continuous-sample-v2",
                    "process_cpu_percent": 20.0 + sample_index,
                    "system_cpu_percent": 30.0 + sample_index,
                    "rss_bytes": 200_000 + sample_index * 1_000,
                    "open_fd_count": 20 + sample_index,
                    "apfs_allocated_bytes": 1_000_000 + sample_index * 100,
                    "disk_written_bytes": sample_index * 1_000,
                    "fsync_latency_ns": 1_000_000 + sample_index,
                    "filesystem_free_bytes": 10_000_000 - sample_index * 1_000,
                    "memory_pressure_percent": 10.0 + sample_index,
                }
            )
        system_path = directory / "system.jsonl"
        write_jsonl(system_path, system_rows)
        self.system_paths.append(system_path)
        repetition["artifacts"] = [
            CAPACITY.artifact_entry(self.root, isolation_path, "isolation-evidence"),
            CAPACITY.artifact_entry(self.root, request_path, "request-observations"),
            CAPACITY.artifact_entry(self.root, system_path, "system-observations"),
        ]
        request_summary, request_bounds = CAPACITY.validate_production_requests(
            requests,
            self.binding,
            repetition,
            self.support_envelope["workload"],
            workload_client_cert_sha256,
            nodes,
            self.native_environment,
        )
        system_summary = CAPACITY.validate_production_system_observations(
            system_rows,
            self.binding,
            repetition,
            nodes,
            request_bounds,
        )
        repetition["summary"] = {
            "repetition_id": repetition_id,
            "sequence": sequence,
            **request_summary,
            **system_summary,
            "thresholds_passed": True,
        }
        return repetition

    def write_campaign(self) -> None:
        write_json(self.campaign_path, self.campaign)
    def refresh_repetition_artifact(self, repetition_index: int, kind: str, path: Path) -> None:
        artifacts = self.campaign["repetitions"][repetition_index]["artifacts"]
        for index, artifact in enumerate(artifacts):
            if artifact["kind"] == kind:
                artifacts[index] = CAPACITY.artifact_entry(self.root, path, kind)
                self.write_campaign()
                return
        raise AssertionError(kind)


    def assemble(self) -> Path:
        report = self.root / "unsigned-result.env"
        CAPACITY.assemble_production(self.campaign_path, report)
        return report

    def create_external_approval(self, intermediate_path: Path) -> tuple[Path, Path, Path]:
        private_key = self.keys / "approver-private.pem"
        public_key = self.root / "approver-public.pem"
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(private_key)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            ["openssl", "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        approval_path = self.root / "external-approval.json"
        approval = {
            "schema": CAPACITY.PRODUCTION_APPROVAL_SCHEMA_URI,
            "verifier_version": CAPACITY.VERIFIER_VERSION,
            "evidence_class": "synthetic-production-capacity-threshold-approval-test-only",
            "evidence_mode": "test-only-synthetic",
            "decision": "approved-production-capacity-certification",
            "approver_identity": "synthetic-approver@example.invalid",
            "approver_organization": "synthetic-test-only.invalid",
            "operator_identity": self.campaign["operator_identity"],
            "target": copy.deepcopy(self.binding),
            "support_envelope_sha256": CAPACITY.canonical_json_sha256(self.support_envelope),
            "thresholds_sha256": CAPACITY.canonical_json_sha256(self.thresholds),
            "intermediate_sha256": CAPACITY.sha256_file(intermediate_path),
            "signed_at_utc": utc(datetime.now(timezone.utc) - timedelta(seconds=1)),
            "trust_root_sha256": CAPACITY.sha256_file(public_key),
            "signature_algorithm": "rsa-sha256",
        }
        write_json(approval_path, approval)
        signature_path = self.root / "external-approval.sig"
        subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", str(private_key), "-out", str(signature_path), str(approval_path)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return approval_path, public_key, signature_path

    def certify(self) -> Path:
        unsigned_report = self.assemble()
        intermediate_path = Path(CAPACITY.parse_env_receipt(unsigned_report)["record_path"])
        approval, public_key, signature = self.create_external_approval(intermediate_path)
        report = self.root / "certification-result.env"
        CAPACITY.certify_production(intermediate_path, approval, public_key, signature, report)
        return report

    @staticmethod
    def rewrite_receipt_hash(report: Path, key: str, value: str) -> None:
        rows = CAPACITY.parse_env_receipt(report)
        rows[key] = value
        report.write_text("".join(f"{name}={item}\n" for name, item in rows.items()), encoding="utf-8")


class ProductionCapacityCertificationV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="capacity-production-v2-test.")
        self.environment = mock.patch.dict(os.environ, {"KVNODE_CAPACITY_ALLOW_TEST_FIXTURE": "yes"})
        self.environment.start()
        self.fixture = ProductionCapacityFixture(Path(self.temp.name))

    def tearDown(self) -> None:
        self.environment.stop()
        self.temp.cleanup()

    def assert_assemble_fails(self) -> None:
        with self.assertRaises(CAPACITY.EvidenceError):
            self.fixture.assemble()

    def test_signed_synthetic_fixture_remains_explicitly_nonproduction(self) -> None:
        report = self.fixture.certify()
        receipt = CAPACITY.parse_env_receipt(report)
        self.assertEqual(receipt["status"], "synthetic-darwin-production-capacity-certification-v2")
        self.assertEqual(receipt["evidence_class"], CAPACITY.PRODUCTION_TEST_CLASS)
        self.assertEqual(receipt["evidence_mode"], "test-only-synthetic")
        self.assertEqual(receipt["production_capacity_certification"], "test-only-not-production")
        self.assertEqual(receipt["release_claim"], "none-test-only-synthetic")
        completed = subprocess.run(
            ["python3", str(MODULE_PATH), "verify", str(report)],
            cwd=ROOT,
            env={**os.environ, "KVNODE_CAPACITY_ALLOW_TEST_FIXTURE": "yes"},
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("evidence_class=synthetic-production-capacity-certification-test-only", completed.stdout)
        self.assertIn("production_capacity_certification=test-only-not-production", completed.stdout)
        self.assertNotIn("production_capacity_certification=approved", completed.stdout)

    def test_production_cli_stdout_preserves_unsigned_and_synthetic_classes(self) -> None:
        environment = {**os.environ, "KVNODE_CAPACITY_ALLOW_TEST_FIXTURE": "yes"}
        unsigned_report = self.fixture.root / "cli-unsigned-result.env"
        assembled = subprocess.run(
            [
                "python3",
                str(MODULE_PATH),
                "assemble-production",
                str(self.fixture.campaign_path),
                str(unsigned_report),
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(assembled.returncode, 0, assembled.stderr)
        self.assertIn(f"evidence_class={CAPACITY.PRODUCTION_INTERMEDIATE_CLASS}", assembled.stdout)
        self.assertIn("production_capacity_certification=not-approved", assembled.stdout)
        self.assertIn("release_claim=none-unsigned-intermediate", assembled.stdout)
        intermediate_path = Path(CAPACITY.parse_env_receipt(unsigned_report)["record_path"])
        approval, public_key, signature = self.fixture.create_external_approval(intermediate_path)
        final_report = self.fixture.root / "cli-synthetic-result.env"
        certified = subprocess.run(
            [
                "python3",
                str(MODULE_PATH),
                "certify-production",
                str(intermediate_path),
                str(approval),
                str(public_key),
                str(signature),
                str(final_report),
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(certified.returncode, 0, certified.stderr)
        self.assertIn(f"evidence_class={CAPACITY.PRODUCTION_TEST_CLASS}", certified.stdout)
        self.assertIn("production_capacity_certification=test-only-not-production", certified.stdout)
        self.assertIn("release_claim=none-test-only-synthetic", certified.stdout)
        self.assertNotIn("production_capacity_certification=approved", certified.stdout)

    def test_unsigned_intermediate_fails_closed_without_approval(self) -> None:
        report = self.fixture.assemble()
        receipt = CAPACITY.parse_env_receipt(report)
        self.assertEqual(receipt["evidence_class"], CAPACITY.PRODUCTION_INTERMEDIATE_CLASS)
        self.assertEqual(receipt["acceptance_result"], "fail-closed-approval-required")
        self.assertEqual(receipt["production_capacity_certification"], "not-approved")
        self.assertEqual(receipt["release_claim"], "none-unsigned-intermediate")
        intermediate = Path(receipt["record_path"])
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.certify_production(
                intermediate,
                self.fixture.root / "absent-approval.json",
                self.fixture.root / "absent-public.pem",
                self.fixture.root / "absent-signature.bin",
                self.fixture.root / "must-not-exist.env",
            )

    def test_target_approval_requires_externally_pinned_trust_and_identity(self) -> None:
        report = self.fixture.assemble()
        intermediate_path = Path(CAPACITY.parse_env_receipt(report)["record_path"])
        intermediate = json.loads(intermediate_path.read_text(encoding="utf-8"))
        approval_path, trust_root_path, signature_path = self.fixture.create_external_approval(intermediate_path)
        approval = json.loads(approval_path.read_text(encoding="utf-8"))
        intermediate["evidence_mode"] = "target"
        approval["evidence_mode"] = "target"
        approval["evidence_class"] = "production-capacity-threshold-approval"
        write_json(approval_path, approval)
        with (
            mock.patch.dict(
                os.environ,
                {
                    "KVNODE_CAPACITY_APPROVER_TRUST_ROOT_SHA256": "",
                    "KVNODE_CAPACITY_APPROVER_IDENTITY": "",
                },
            ),
            self.assertRaisesRegex(CAPACITY.EvidenceError, "externally pinned approver trust root"),
        ):
            CAPACITY.validate_production_approval(
                approval,
                approval_path,
                trust_root_path,
                signature_path,
                intermediate,
                intermediate_path,
                self.fixture.root,
            )

    def test_rejects_single_repetition_campaign(self) -> None:
        self.fixture.campaign["repetition_count"] = 1
        self.fixture.campaign["repetitions"] = self.fixture.campaign["repetitions"][:1]
        self.fixture.campaign["repetitions"][0]["repetition_count"] = 1
        self.fixture.write_campaign()
        self.assert_assemble_fails()

    def test_rejects_mixed_run_modes(self) -> None:
        self.fixture.campaign["repetitions"][1]["evidence_mode"] = "target"
        self.fixture.write_campaign()
        self.assert_assemble_fails()

    def test_rejects_relabelled_characterization_repetition(self) -> None:
        self.fixture.campaign["repetitions"][0]["evidence_class"] = "workstation-capacity-characterization"
        self.fixture.write_campaign()
        self.assert_assemble_fails()

    def test_rejects_characterization_document_as_production_campaign(self) -> None:
        write_json(
            self.fixture.campaign_path,
            {
                "schema": CAPACITY.MEASUREMENT_SCHEMA_URI,
                "verifier_version": CAPACITY.VERIFIER_VERSION,
                "evidence_class": "workstation-capacity-characterization",
                "production_capacity_certification": False,
            },
        )
        self.assert_assemble_fails()

    def test_rejects_target_binary_and_source_mismatches(self) -> None:
        mutations: list[tuple[str, Callable[[dict[str, Any]], None]]] = [
            ("target", lambda repetition: repetition["target"].__setitem__("target_id", "other-target")),
            ("binary", lambda repetition: repetition["target"].__setitem__("binary_sha256", "b" * 64)),
            ("source", lambda repetition: repetition["target"].__setitem__("source_revision", "b" * 40)),
        ]
        original = copy.deepcopy(self.fixture.campaign)
        for label, mutate in mutations:
            with self.subTest(label=label):
                self.fixture.campaign = copy.deepcopy(original)
                mutate(self.fixture.campaign["repetitions"][0])
                self.fixture.write_campaign()
                self.assert_assemble_fails()

    def test_rejects_threshold_tamper_after_external_approval(self) -> None:
        report = self.fixture.certify()
        receipt = CAPACITY.parse_env_receipt(report)
        record_path = Path(receipt["record_path"])
        record = json.loads(record_path.read_text(encoding="utf-8"))
        record["thresholds"]["throughput"]["limit"] = 0.001
        write_json(record_path, record)
        self.fixture.rewrite_receipt_hash(report, "record_sha256", CAPACITY.sha256_file(record_path))
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.verify_receipt(report)

    def test_rejects_tampered_raw_system_metrics(self) -> None:
        report = self.fixture.certify()
        rows = CAPACITY.load_jsonl(self.fixture.system_paths[0], "synthetic system rows")
        rows[-1]["rss_bytes"] += 1
        write_jsonl(self.fixture.system_paths[0], rows)
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.verify_receipt(report)

    def test_rejects_tampered_external_signature(self) -> None:
        report = self.fixture.certify()
        receipt = CAPACITY.parse_env_receipt(report)
        signature_path = Path(receipt["signature_path"])
        signature = bytearray(signature_path.read_bytes())
        signature[0] ^= 0x01
        signature_path.write_bytes(signature)
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.verify_receipt(report)

    def test_rejects_wrong_observed_process_executable(self) -> None:
        isolation_path = self.fixture.isolation_paths[0]
        isolation = json.loads(isolation_path.read_text(encoding="utf-8"))
        isolation["nodes"][0]["executable_path"] = "/var/db/moreconsensus/bin/not-kvnode"
        write_json(isolation_path, isolation)
        self.fixture.refresh_repetition_artifact(0, "isolation-evidence", isolation_path)
        self.assert_assemble_fails()

    def test_rejects_system_metrics_bound_to_reused_process(self) -> None:
        system_path = self.fixture.system_paths[0]
        rows = CAPACITY.load_jsonl(system_path, "synthetic system rows")
        rows[2]["process_bindings"][0]["process_start_token"] += 1
        write_jsonl(system_path, rows)
        self.fixture.refresh_repetition_artifact(0, "system-observations", system_path)
        self.assert_assemble_fails()

    def test_rejects_system_samples_clustered_before_measured_work(self) -> None:
        system_path = self.fixture.system_paths[0]
        rows = CAPACITY.load_jsonl(system_path, "synthetic system rows")
        measurement_start = self.fixture.campaign["repetitions"][0]["measurement_started_monotonic_ns"]
        for offset, row in zip((100_000_000, 200_000_000, 300_000_000), rows[2:]):
            row["monotonic_ns"] = measurement_start + offset
        write_jsonl(system_path, rows)
        self.fixture.refresh_repetition_artifact(0, "system-observations", system_path)
        self.assert_assemble_fails()

    def test_rejects_serial_requests_for_declared_concurrency(self) -> None:
        request_path = self.fixture.request_paths[0]
        rows = CAPACITY.load_jsonl(request_path, "synthetic request rows")
        repetition = self.fixture.campaign["repetitions"][0]
        for phase, window_key in (
            ("warmup", "warmup_started_monotonic_ns"),
            ("measurement", "measurement_started_monotonic_ns"),
        ):
            window_start = repetition[window_key]
            phase_rows = [row for row in rows if row["phase"] == phase]
            for row in phase_rows:
                started_ns = window_start + 100_000_000 + row["operation_index"] * 200_000_000
                row["started_monotonic_ns"] = started_ns
                row["ended_monotonic_ns"] = started_ns + row["latency_ns"]
        write_jsonl(request_path, rows)
        self.fixture.refresh_repetition_artifact(0, "request-observations", request_path)
        self.assert_assemble_fails()

    def test_rejects_wrong_isolation_command_and_stale_capture(self) -> None:
        original = json.loads(self.fixture.isolation_paths[0].read_text(encoding="utf-8"))
        mutations: list[tuple[str, Callable[[dict[str, Any]], None]]] = [
            (
                "wrong-sysctl",
                lambda isolation: isolation["observed_commands"][0].__setitem__(
                    "argv", ["sysctl", "-n", "hw.ncpu"]
                ),
            ),
            (
                "stale-capture",
                lambda isolation: isolation.__setitem__(
                    "captured_at_utc",
                    utc(datetime.now(timezone.utc) - timedelta(days=3)),
                ),
            ),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                isolation = copy.deepcopy(original)
                mutate(isolation)
                write_json(self.fixture.isolation_paths[0], isolation)
                self.fixture.refresh_repetition_artifact(
                    0,
                    "isolation-evidence",
                    self.fixture.isolation_paths[0],
                )
                self.assert_assemble_fails()

    def test_rejects_ec_key_mislabeled_as_rsa_approval(self) -> None:
        report = self.fixture.assemble()
        intermediate_path = Path(CAPACITY.parse_env_receipt(report)["record_path"])
        intermediate = json.loads(intermediate_path.read_text(encoding="utf-8"))
        approval_path, _, signature_path = self.fixture.create_external_approval(intermediate_path)
        ec_private = self.fixture.keys / "ec-private.pem"
        ec_public = self.fixture.root / "ec-public.pem"
        subprocess.run(
            [
                "openssl",
                "genpkey",
                "-algorithm",
                "EC",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-out",
                str(ec_private),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            ["openssl", "pkey", "-in", str(ec_private), "-pubout", "-out", str(ec_public)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        approval = json.loads(approval_path.read_text(encoding="utf-8"))
        approval["trust_root_sha256"] = CAPACITY.sha256_file(ec_public)
        write_json(approval_path, approval)
        subprocess.run(
            [
                "openssl",
                "dgst",
                "-sha256",
                "-sign",
                str(ec_private),
                "-out",
                str(signature_path),
                str(approval_path),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with self.assertRaisesRegex(CAPACITY.EvidenceError, "must be an RSA public key"):
            CAPACITY.validate_production_approval(
                approval,
                approval_path,
                ec_public,
                signature_path,
                intermediate,
                intermediate_path,
                self.fixture.root.resolve(),
            )

    def test_rejects_symlinked_unsigned_intermediate_without_touching_target(self) -> None:
        outside = self.fixture.base / "outside-intermediate-target"
        outside.write_text("unchanged", encoding="utf-8")
        (self.fixture.root / "production-capacity-unsigned-intermediate-v2.json").symlink_to(outside)
        self.assert_assemble_fails()
        self.assertEqual(outside.read_text(encoding="utf-8"), "unchanged")

    def test_rejects_symlinked_receipt_without_touching_target(self) -> None:
        outside = self.fixture.base / "outside-receipt-target"
        outside.write_text("unchanged", encoding="utf-8")
        report = self.fixture.root / "linked-result.env"
        report.symlink_to(outside)
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.assemble_production(self.fixture.campaign_path, report)
        self.assertEqual(outside.read_text(encoding="utf-8"), "unchanged")

    def test_rejects_symlinked_certificate_without_touching_target(self) -> None:
        unsigned = self.fixture.assemble()
        intermediate_path = Path(CAPACITY.parse_env_receipt(unsigned)["record_path"])
        approval, public_key, signature = self.fixture.create_external_approval(intermediate_path)
        outside = self.fixture.base / "outside-certificate-target"
        outside.write_text("unchanged", encoding="utf-8")
        (self.fixture.root / "production-capacity-test-fixture-v2.json").symlink_to(outside)
        with self.assertRaises(CAPACITY.EvidenceError):
            CAPACITY.certify_production(
                intermediate_path,
                approval,
                public_key,
                signature,
                self.fixture.root / "must-not-exist.env",
            )
        self.assertEqual(outside.read_text(encoding="utf-8"), "unchanged")

    def test_production_schema_fixes_certification_class_and_claim(self) -> None:
        CAPACITY.validate_schema_contract()
        schema_path = ROOT / "release/evidence/schema/kvnode-production-capacity-certification-v2.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertEqual(schema["properties"]["evidence_class"]["const"], CAPACITY.PRODUCTION_FINAL_CLASS)
        self.assertIs(schema["properties"]["production_capacity_certification"]["const"], True)
        self.assertEqual(schema["properties"]["release_claim"]["const"], CAPACITY.PRODUCTION_RELEASE_CLAIM)
        self.assertEqual(set(schema["required"]), set(schema["properties"]))
        self.assertEqual(schema["properties"]["raw_artifacts"]["minItems"], 10)
        test_schema_path = ROOT / "release/evidence/schema/kvnode-production-capacity-test-fixture-v2.schema.json"
        test_schema = json.loads(test_schema_path.read_text(encoding="utf-8"))
        report = self.fixture.certify()
        record = json.loads(Path(CAPACITY.parse_env_receipt(report)["record_path"]).read_text(encoding="utf-8"))
        self.assertEqual(record["schema"], CAPACITY.PRODUCTION_TEST_SCHEMA_URI)
        self.assertEqual(set(record), set(test_schema["required"]))
        self.assertEqual(set(test_schema["required"]), set(test_schema["properties"]))
        self.assertEqual(len(record["raw_artifacts"]), 10)


if __name__ == "__main__":
    unittest.main(verbosity=2)
