#!/usr/bin/env python3
"""Self-contained positive and negative tests for release closure evidence."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import subprocess
import runpy
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable

ITEMS = {
    "broader-formal": "Broader formal model coverage",
    "deployment-manifest": "Deployment manifest",
    "data-lifecycle": "Data lifecycle",
    "capacity-envelope": "Capacity envelope",
    "incident-readiness": "Incident readiness",
}
NOW = "2030-01-10T12:00:00Z"
SOURCE_REVISION = hashlib.sha256(b"immutable synthetic source revision").hexdigest()
RELEASE_ID = f"mc-kv-{SOURCE_REVISION[:12]}-r1"
TARGET_ID = "mc-kv-darwin24-arm64-launchd-3n-r1"
ENVIRONMENT_PROFILE = "native-darwin24-arm64-launchd-system-domain-v1"
MACHO_BINARY = b"\xcf\xfa\xed\xfe\x0c\x00\x00\x01" + (b"\xa5" * 5000)


class ReleaseEvidenceVerifierTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = Path(__file__).resolve().parent.parent
        cls.verifier = cls.repo_root / "tests" / "release_evidence_verifier.py"
        cls.workflow = cls.repo_root / "tests" / "go_no_go_workflow.sh"

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="release-evidence-fixture-")
        self.base = Path(self.temp.name)
        self.root = self.base / "source"
        self.evidence_root = self.base / "external-evidence"
        self.hook_root = self.base / "synthetic-hooks"
        (self.root / "release").mkdir(parents=True)
        self.evidence_root.mkdir()
        self.hook_root.mkdir()
        self.write_evidence(no_go=False)
        self.write_scope([], "Go.")
        self.records = self.make_valid_records()
        self.write_index()
        self.write_test_hooks()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_test_hooks(self) -> None:
        for item_id in ITEMS:
            hook = self.hook_root / f"{item_id}-verifier"
            hook.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                "[ \"$#\" -eq 7 ]\n"
                "[ -f \"$1\" ]\n"
                f": > \"$(dirname \"$0\")/{item_id}.ran\"\n"
                f"printf '%s\\n' 'release-evidence-hook item_id={item_id} status=verified "
                "mode=synthetic-test-fixture release_claim=none'\n",
                encoding="utf-8",
            )
            hook.chmod(0o755)


    def write_scope(self, open_ids: list[str], decision: str) -> None:
        rows = "\n".join(f"| {ITEMS[item_id]} | Synthetic table state. |" for item_id in open_ids)
        content = (
            "# Synthetic release scope\n\n"
            "## Current release decision\n\n"
            f"{decision}\n\n"
            "## Open release items\n\n"
            "| Item | Current state |\n"
            "| --- | --- |\n"
            f"{rows}\n\n"
            "## Review baseline\n\nSynthetic fixture only.\n"
        )
        (self.root / "RELEASE_SCOPE.md").write_text(content, encoding="utf-8")

    def write_evidence(self, *, no_go: bool) -> None:
        lines = ["bash tests/go_no_go_workflow.sh"]
        if no_go:
            lines.extend(["Status: no-go evidence bundle", "Current open blockers preserving no-go"])
        else:
            lines.append("Status: go evidence bundle")
        (self.root / "release" / "EPAXOS_READINESS_EVIDENCE.md").write_text(
            "\n".join(lines) + "\n", encoding="utf-8"
        )

    @staticmethod
    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def make_valid_records(self) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        binary = self.evidence_root / "kvnode"
        binary.write_bytes(MACHO_BINARY)
        binary.chmod(0o755)
        binary_sha256 = self.digest(binary)
        for item_id in ITEMS:
            artifact_dir = self.evidence_root / "artifacts" / item_id
            artifact_dir.mkdir(parents=True)
            primary_kind = "formal-spec" if item_id == "broader-formal" else "binary"
            primary = artifact_dir / "model.tla" if item_id == "broader-formal" else binary
            if item_id == "broader-formal":
                primary.write_bytes(b"formal specification bytes\n")
            primary_sha256 = self.digest(primary)

            result = artifact_dir / "acceptance-report.txt"
            if item_id == "deployment-manifest":
                result.write_text(
                    "evidence_schema=kvnode-target-deployment-evidence-v2\n"
                    "verifier_version=darwin-v2\n"
                    "evidence_mode=test-only-synthetic\n"
                    f"target_id={TARGET_ID}\n"
                    f"target_environment={ENVIRONMENT_PROFILE}\n"
                    f"release_id={RELEASE_ID}\n"
                    f"source_revision={SOURCE_REVISION}\n"
                    f"binary_sha256={binary_sha256}\n",
                    encoding="utf-8",
                )
            elif item_id == "capacity-envelope":
                result.write_text(
                    "status=synthetic-darwin-capacity-evidence-v2\n"
                    "schema_version=kvnode-capacity-evidence-v2\n"
                    "verifier_version=2.0.0\n"
                    "evidence_class=synthetic-test-only\n"
                    "evidence_mode=test-only-synthetic\n"
                    "production_capacity_certification=not-claimed\n"
                    "acceptance_result=pass-synthetic-test-fixture\n"
                    "claim_scope=none-synthetic\n"
                    "release_claim=none-synthetic-fixture-not-release-evidence\n"
                    f"target_name={TARGET_ID}\n"
                    f"environment_profile={ENVIRONMENT_PROFILE}\n"
                    f"release_id={RELEASE_ID}\n"
                    f"source_revision={SOURCE_REVISION}\n"
                    f"binary_sha256={binary_sha256}\n",
                    encoding="utf-8",
                )
            elif item_id == "data-lifecycle":
                result.write_text(
                    json.dumps(
                        {
                            "schema_version": "2.0.0",
                            "verifier_version": "2.0.0",
                            "evidence_class": "synthetic-test-only",
                            "target": {
                                "target_id": TARGET_ID,
                                "environment_profile": ENVIRONMENT_PROFILE,
                                "release_id": RELEASE_ID,
                                "source_revision": SOURCE_REVISION,
                                "binary_sha256": binary_sha256,
                            },
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
            elif item_id == "incident-readiness":
                result.write_text(
                    json.dumps(
                        {
                            "schema_version": "2.0",
                            "record_mode": "synthetic-test",
                            "target": {"name": TARGET_ID, "environment": ENVIRONMENT_PROFILE},
                            "release_provenance": {
                                "release_id": RELEASE_ID,
                                "source_revision": SOURCE_REVISION,
                                "binary_sha256": binary_sha256,
                            },
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
            else:
                result.write_text(
                    json.dumps(
                        {
                            "schema_version": "1.0.0",
                            "record_mode": "synthetic-test",
                            "release": {
                                "release_id": RELEASE_ID,
                                "source_revision": SOURCE_REVISION,
                                "formal_spec_sha256": primary_sha256,
                            },
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )

            review = artifact_dir / "independent-review.txt"
            review.write_bytes(f"independent review attestation for {item_id}\n".encode())
            raw_artifacts = []
            if item_id == "broader-formal":
                raw_artifacts.append(
                    {
                        "role": "formal-spec",
                        "uri": f"file:artifacts/{item_id}/model.tla",
                        "sha256": primary_sha256,
                    }
                )
            raw_artifacts.extend(
                [
                    {
                        "role": "binary",
                        "uri": "file:kvnode",
                        "sha256": binary_sha256,
                    },
                    {
                        "role": "result",
                        "uri": f"file:artifacts/{item_id}/acceptance-report.txt",
                        "sha256": self.digest(result),
                    },
                    {
                        "role": "review",
                        "uri": f"file:artifacts/{item_id}/independent-review.txt",
                        "sha256": self.digest(review),
                    },
                ]
            )
            record = {
                "schema": "moreconsensus.release-evidence-record.v2",
                "evidence_mode": "synthetic-test-fixture",
                "item_id": item_id,
                "release_id": RELEASE_ID,
                "target": TARGET_ID,
                "environment": ENVIRONMENT_PROFILE,
                "scope": f"authoritative closure scope for {item_id}",
                "source_revision": SOURCE_REVISION,
                "release_binary_sha256": binary_sha256,
                "build_artifact": {"kind": primary_kind, "sha256": primary_sha256},
                "started_at": "2030-01-08T10:00:00Z",
                "completed_at": "2030-01-08T11:00:00Z",
                "raw_artifacts": raw_artifacts,
                "acceptance": [
                    {
                        "criterion": f"named acceptance criterion for {item_id}",
                        "result": "pass",
                        "details": "observed result satisfies the synthetic verifier contract",
                    }
                ],
                "operator": {
                    "identity": "operator-01@corp.invalid",
                    "authenticated_by": "https://identity.corp.invalid",
                    "role": "operator",
                },
                "independent_reviewer": {
                    "identity": "reviewer-02@corp.invalid",
                    "authenticated_by": "https://identity.corp.invalid",
                    "role": "independent-reviewer",
                },
                "reviewed_at": "2030-01-09T12:00:00Z",
                "release_claim": "synthetic-item-closed-for-test-only",
            }
            records[item_id] = record
        return records

    def record_path(self, item_id: str) -> Path:
        return self.evidence_root / "closure-records" / f"{item_id}.json"

    def write_record(self, item_id: str) -> None:
        path = self.record_path(item_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.records[item_id], indent=2) + "\n", encoding="utf-8")

    def write_index(self) -> None:
        for item_id in ITEMS:
            self.write_record(item_id)
        entries = [
            {
                "item_id": item_id,
                "record_uri": f"file:closure-records/{item_id}.json",
                "record_sha256": self.digest(self.record_path(item_id)),
            }
            for item_id in ITEMS
        ]
        index = {
            "schema": "moreconsensus.release-closure-index.v2",
            "mode": "synthetic-test-fixture",
            "target": TARGET_ID,
            "environment": ENVIRONMENT_PROFILE,
            "release_id": RELEASE_ID,
            "source_revision": SOURCE_REVISION,
            "binary_sha256": self.records["deployment-manifest"]["release_binary_sha256"],
            "decision_revision": SOURCE_REVISION,
            "generated_at": "2030-01-09T13:00:00Z",
            "records": entries,
        }
        (self.evidence_root / "release-closure-index.json").write_text(
            json.dumps(index, indent=2) + "\n", encoding="utf-8"
        )

    def refresh_record_hash(self, item_id: str) -> None:
        index_path = self.evidence_root / "release-closure-index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        for entry in index["records"]:
            if entry["item_id"] == item_id:
                entry["record_sha256"] = self.digest(self.record_path(item_id))
        index_path.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")

    def mutate_record(self, item_id: str, mutation: Callable[[dict[str, Any]], None]) -> None:
        mutation(self.records[item_id])
        self.write_record(item_id)
        self.refresh_record_hash(item_id)

    def update_result_hash(self, item_id: str) -> None:
        result_path = self.evidence_root / "artifacts" / item_id / "acceptance-report.txt"
        record = self.records[item_id]
        result_artifact = next(
            artifact for artifact in record["raw_artifacts"] if artifact["role"] == "result"
        )
        result_artifact["sha256"] = self.digest(result_path)
        self.write_record(item_id)
        self.refresh_record_hash(item_id)

    def mutate_index(self, mutation: Callable[[dict[str, Any]], None]) -> None:
        index_path = self.evidence_root / "release-closure-index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        mutation(index)
        index_path.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")

    def replace_release_binary(self, content: bytes) -> None:
        index_path = self.evidence_root / "release-closure-index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        binary_path = self.evidence_root / "kvnode"
        binary_path.write_bytes(content)
        binary_path.chmod(0o755)
        binary_sha256 = self.digest(binary_path)
        for item_id, record in self.records.items():
            record["release_binary_sha256"] = binary_sha256
            if item_id != "broader-formal":
                record["build_artifact"]["sha256"] = binary_sha256
            binary_artifact = next(
                artifact for artifact in record["raw_artifacts"] if artifact["role"] == "binary"
            )
            binary_artifact["sha256"] = binary_sha256
            if item_id != "broader-formal":
                result_path = (
                    self.evidence_root / "artifacts" / item_id / "acceptance-report.txt"
                )
                if item_id in {"deployment-manifest", "capacity-envelope"}:
                    result_content = result_path.read_text(encoding="utf-8")
                    result_content = re.sub(
                        r"(?m)^binary_sha256=.*$",
                        f"binary_sha256={binary_sha256}",
                        result_content,
                    )
                    result_path.write_text(result_content, encoding="utf-8")
                else:
                    result_document = json.loads(result_path.read_text(encoding="utf-8"))
                    if item_id == "data-lifecycle":
                        result_document["target"]["binary_sha256"] = binary_sha256
                    else:
                        result_document["release_provenance"]["binary_sha256"] = binary_sha256
                    result_path.write_text(
                        json.dumps(result_document) + "\n", encoding="utf-8"
                    )
                result_artifact = next(
                    artifact
                    for artifact in record["raw_artifacts"]
                    if artifact["role"] == "result"
                )
                result_artifact["sha256"] = self.digest(result_path)
            self.write_record(item_id)
        index["binary_sha256"] = binary_sha256
        for entry in index["records"]:
            entry["record_sha256"] = self.digest(self.record_path(entry["item_id"]))
        index_path.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")

    def rewrite_capacity_result_target(self, target: str, *, update_record: bool) -> None:
        result_path = self.evidence_root / "artifacts" / "capacity-envelope" / "acceptance-report.txt"
        content = result_path.read_text(encoding="utf-8")
        content = re.sub(r"(?m)^target_name=.*$", f"target_name={target}", content)
        result_path.write_text(content, encoding="utf-8")
        self.update_result_hash("capacity-envelope")
        if update_record:
            self.records["capacity-envelope"]["target"] = target
            self.write_record("capacity-envelope")
            self.refresh_record_hash("capacity-envelope")

    def verifier_command(self, *, test_mode: bool = True) -> list[str]:
        command = [
            "python3",
            str(self.verifier),
            "--repository-root",
            str(self.root),
            "--scope",
            str(self.root / "RELEASE_SCOPE.md"),
            "--evidence-root",
            str(self.evidence_root),
        ]
        if test_mode:
            command.extend(["--test-mode", "--now", NOW])
            command.extend(["--test-hook-root", str(self.hook_root)])
        return command

    def run_verifier(self, *, test_mode: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self.verifier_command(test_mode=test_mode),
            cwd=self.repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def run_workflow(self) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update(
            {
                "GO_NO_GO_TEST_MODE": "yes",
                "GO_NO_GO_TEST_ROOT": str(self.root),
                "GO_NO_GO_TEST_EVIDENCE_ROOT": str(self.evidence_root),
                "GO_NO_GO_TEST_HOOK_ROOT": str(self.hook_root),
                "GO_NO_GO_NOW": NOW,
            }
        )
        return subprocess.run(
            ["bash", str(self.workflow)],
            cwd=self.repo_root,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def assert_verifier_fails(self) -> None:
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("release evidence verification failed:", result.stderr)

    def test_complete_synthetic_closures_pass_only_in_explicit_test_mode(self) -> None:
        result = self.run_workflow()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("release_decision=Go.", result.stdout)
        self.assertIn("open_release_items=0", result.stdout)
        self.assertIn("closed_release_item_ids=" + ",".join(ITEMS), result.stdout)
        self.assertIn("verification_mode=synthetic-test-fixture", result.stdout)
        self.assertIn("release_claim=none-synthetic-fixture-not-release-evidence", result.stdout)
        for item_id in ITEMS:
            self.assertTrue((self.hook_root / f"{item_id}.ran").is_file())


        production_attempt = self.run_verifier(test_mode=False)
        self.assertNotEqual(production_attempt.returncode, 0, production_attempt.stdout)
        self.assertIn("closure index mode must be darwin-production-v2", production_attempt.stderr)

    def test_all_five_open_without_index_preserves_no_go(self) -> None:
        (self.evidence_root / "release-closure-index.json").unlink()
        self.write_scope(list(ITEMS), "No-go.")
        self.write_evidence(no_go=True)
        result = self.run_workflow()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("release_decision=No-go.", result.stdout)
        self.assertIn("open_release_items=5", result.stdout)

    def test_empty_open_table_without_records_fails(self) -> None:
        (self.evidence_root / "release-closure-index.json").unlink()
        result = self.run_workflow()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("closure index is required for closed items", result.stderr)

    def test_closed_items_require_explicit_external_evidence_root(self) -> None:
        command = self.verifier_command()
        evidence_option = command.index("--evidence-root")
        del command[evidence_option : evidence_option + 2]
        result = subprocess.run(
            command,
            cwd=self.repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("explicit external immutable evidence root is required", result.stderr)

    def test_go_decision_rejects_stale_no_go_bundle(self) -> None:
        self.write_evidence(no_go=True)
        result = self.run_workflow()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("go evidence bundle missing required status", result.stderr)


    def test_placeholder_record_fails(self) -> None:
        self.mutate_record("deployment-manifest", lambda record: record.__setitem__("target", "TBD"))
        self.assert_verifier_fails()
    def test_placeholder_acceptance_result_fails(self) -> None:
        self.mutate_record(
            "broader-formal",
            lambda record: record["acceptance"][0].update(
                {"criterion": "TBD", "details": "not performed"}
            ),
        )
        self.assert_verifier_fails()


    def test_local_only_record_fails(self) -> None:
        self.mutate_record(
            "capacity-envelope", lambda record: record.__setitem__("target", "local-loopback")
        )
        self.assert_verifier_fails()
    def test_port_qualified_loopback_record_fails(self) -> None:
        self.mutate_record(
            "deployment-manifest",
            lambda record: record.__setitem__("target", "https://localhost:8443/cluster"),
        )
        self.assert_verifier_fails()


    def test_non_claim_record_fails(self) -> None:
        self.mutate_record(
            "incident-readiness",
            lambda record: record.__setitem__(
                "release_claim", "none-target-environment-operator-review-still-required"
            ),
        )
        self.assert_verifier_fails()

    def test_unhashed_artifact_fails(self) -> None:
        self.mutate_record(
            "data-lifecycle", lambda record: record["raw_artifacts"][1].pop("sha256")
        )
        self.assert_verifier_fails()

    def test_unreviewed_record_fails(self) -> None:
        self.mutate_record("incident-readiness", lambda record: record.pop("reviewed_at"))
        self.assert_verifier_fails()
    def test_unverifiable_identity_provider_fails(self) -> None:
        self.mutate_record(
            "incident-readiness",
            lambda record: record["independent_reviewer"].__setitem__(
                "authenticated_by", "identity-provider-a"
            ),
        )
        self.assert_verifier_fails()

    def test_reviewer_role_must_be_independent(self) -> None:
        self.mutate_record(
            "incident-readiness",
            lambda record: record["independent_reviewer"].__setitem__(
                "role", "operator"
            ),
        )
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("role must be independent-reviewer", result.stderr)


    def test_stale_review_fails(self) -> None:
        def make_stale(record: dict[str, Any]) -> None:
            record["started_at"] = "2029-09-29T10:00:00Z"
            record["completed_at"] = "2029-09-30T11:00:00Z"
            record["reviewed_at"] = "2029-10-01T12:00:00Z"

        self.mutate_record("broader-formal", make_stale)
        self.assert_verifier_fails()
    def test_stale_execution_with_fresh_review_fails(self) -> None:
        def make_execution_stale(record: dict[str, Any]) -> None:
            record["started_at"] = "2029-01-01T10:00:00Z"
            record["completed_at"] = "2029-01-01T11:00:00Z"
            record["reviewed_at"] = "2030-01-09T12:00:00Z"

        self.mutate_record("capacity-envelope", make_execution_stale)
        self.assert_verifier_fails()


    def test_mismatched_item_record_fails(self) -> None:
        self.mutate_record(
            "deployment-manifest", lambda record: record.__setitem__("item_id", "data-lifecycle")
        )
        self.assert_verifier_fails()

    def test_result_artifact_binding_mismatch_fails(self) -> None:
        self.rewrite_capacity_result_target("prod-cluster-beta", update_record=False)
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("target_name must equal", result.stderr)

    def test_record_target_mismatch_fails(self) -> None:
        self.mutate_record(
            "capacity-envelope",
            lambda record: record.__setitem__("target", "prod-cluster-beta"),
        )
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("target does not match closure index", result.stderr)

    def test_production_capacity_binding_rejects_characterization_and_unsigned_results(
        self,
    ) -> None:
        result_path = (
            self.evidence_root / "artifacts" / "capacity-envelope" / "acceptance-report.txt"
        )
        binary_sha256 = self.digest(self.evidence_root / "kvnode")
        production_result = (
            "status=darwin-production-capacity-certification-v2\n"
            "schema_version=kvnode-capacity-evidence-v2\n"
            "verifier_version=2.0.0\n"
            "evidence_class=production-capacity-certification\n"
            "evidence_mode=target\n"
            "production_capacity_certification=approved\n"
            "acceptance_result=pass-production-capacity-certification\n"
            "claim_scope=declared-native-darwin-same-host-support-envelope\n"
            "release_claim=target-bound-production-capacity-envelope-certified\n"
            f"target_name={TARGET_ID}\n"
            f"environment_profile={ENVIRONMENT_PROFILE}\n"
            f"release_id={RELEASE_ID}\n"
            f"source_revision={SOURCE_REVISION}\n"
            f"binary_sha256={binary_sha256}\n"
        )
        verifier_module = runpy.run_path(str(self.verifier))
        result_binding = verifier_module["result_binding"]
        verification_error = verifier_module["VerificationError"]

        result_path.write_text(production_result, encoding="utf-8")
        binding = result_binding(
            "capacity-envelope", result_path, self.evidence_root, False
        )
        self.assertEqual(binding["target"], TARGET_ID)

        result_path.write_text(
            production_result.replace(
                "evidence_class=production-capacity-certification",
                "evidence_class=workstation-capacity-characterization",
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            verification_error,
            "evidence_class must equal production-capacity-certification",
        ):
            result_binding("capacity-envelope", result_path, self.evidence_root, False)

        result_path.write_text(
            production_result.replace(
                "status=darwin-production-capacity-certification-v2",
                "status=darwin-capacity-characterization-evidence-v2",
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            verification_error,
            "status must equal darwin-production-capacity-certification-v2",
        ):
            result_binding("capacity-envelope", result_path, self.evidence_root, False)

        result_path.write_text(
            production_result.replace(
                "status=darwin-production-capacity-certification-v2",
                "status=darwin-capacity-certification-unsigned-intermediate-v2",
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            verification_error,
            "status must equal darwin-production-capacity-certification-v2",
        ):
            result_binding("capacity-envelope", result_path, self.evidence_root, False)

    def test_missing_item_verifier_hook_fails(self) -> None:
        (self.hook_root / "capacity-envelope-verifier").unlink()
        self.assert_verifier_fails()

    def test_mixed_v1_v2_result_is_rejected(self) -> None:
        result_path = (
            self.evidence_root
            / "artifacts"
            / "deployment-manifest"
            / "acceptance-report.txt"
        )
        content = result_path.read_text(encoding="utf-8").replace(
            "kvnode-target-deployment-evidence-v2",
            "kvnode-target-deployment-evidence-v1",
            1,
        )
        result_path.write_text(content, encoding="utf-8")
        self.update_result_hash("deployment-manifest")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("evidence_schema must equal kvnode-target-deployment-evidence-v2", result.stderr)

    def test_cross_release_result_substitution_is_rejected(self) -> None:
        result_path = (
            self.evidence_root / "artifacts" / "capacity-envelope" / "acceptance-report.txt"
        )
        content = re.sub(
            r"(?m)^release_id=.*$",
            "release_id=mc-kv-deadbeefdead-r1",
            result_path.read_text(encoding="utf-8"),
        )
        result_path.write_text(content, encoding="utf-8")
        self.update_result_hash("capacity-envelope")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("result artifact release_id does not match closure record", result.stderr)

    def test_user_domain_environment_substitution_is_rejected(self) -> None:
        result_path = (
            self.evidence_root / "artifacts" / "capacity-envelope" / "acceptance-report.txt"
        )
        content = result_path.read_text(encoding="utf-8").replace(
            ENVIRONMENT_PROFILE,
            "native-darwin24-arm64-launchd-user-domain-v1",
            1,
        )
        result_path.write_text(content, encoding="utf-8")
        self.update_result_hash("capacity-envelope")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("environment_profile must equal", result.stderr)

    def test_row_result_relabel_is_rejected(self) -> None:
        deployment_result = (
            self.evidence_root
            / "artifacts"
            / "deployment-manifest"
            / "acceptance-report.txt"
        )
        capacity_result = (
            self.evidence_root / "artifacts" / "capacity-envelope" / "acceptance-report.txt"
        )
        capacity_result.write_bytes(deployment_result.read_bytes())
        self.update_result_hash("capacity-envelope")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(
            "status must equal synthetic-darwin-capacity-evidence-v2", result.stderr
        )

    def test_header_only_binary_is_rejected(self) -> None:
        self.replace_release_binary(
            b"\xcf\xfa\xed\xfe\x0c\x00\x00\x01" + (b"\x00" * 24)
        )
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("header-only or implausibly small", result.stderr)

    def test_writable_external_root_is_rejected(self) -> None:
        def make_production_index(index: dict[str, Any]) -> None:
            index["mode"] = "darwin-production-v2"
            index["generated_at"] = "2026-07-10T00:00:00Z"

        self.mutate_index(make_production_index)
        result = self.run_verifier(test_mode=False)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("physically mounted read-only", result.stderr)

    def test_open_and_closed_same_item_fails(self) -> None:
        self.write_scope(["incident-readiness"], "No-go.")
        result = self.run_verifier()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("open_and_closed=['incident-readiness']", result.stderr)
    def test_content_before_authoritative_open_table_fails(self) -> None:
        scope = self.root / "RELEASE_SCOPE.md"
        content = scope.read_text(encoding="utf-8")
        content = content.replace(
            "## Open release items\n\n| Item | Current state |",
            "## Open release items\n\n| Incident readiness | Still open. |\n| Item | Current state |",
            1,
        )
        scope.write_text(content, encoding="utf-8")
        self.assert_verifier_fails()


    def test_duplicate_index_item_id_fails(self) -> None:
        index_path = self.evidence_root / "release-closure-index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        index["records"].append(copy.deepcopy(index["records"][0]))
        index_path.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
        self.assert_verifier_fails()

    def test_missing_index_release_id_fails_closed(self) -> None:
        index_path = self.evidence_root / "release-closure-index.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        index.pop("release_id")
        index_path.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
        self.assert_verifier_fails()

    def test_duplicate_json_field_fails(self) -> None:
        item_id = "broader-formal"
        path = self.record_path(item_id)
        raw = path.read_text(encoding="utf-8")
        raw = raw.replace(
            '  "item_id": "broader-formal",',
            '  "item_id": "broader-formal",\n  "item_id": "broader-formal",',
            1,
        )
        path.write_text(raw, encoding="utf-8")
        self.refresh_record_hash(item_id)
        self.assert_verifier_fails()

    def test_raw_artifact_hash_mismatch_fails(self) -> None:
        wrong_hash = hashlib.sha256(b"different bytes").hexdigest()

        def replace_hash(record: dict[str, Any]) -> None:
            record["build_artifact"]["sha256"] = wrong_hash
            record["raw_artifacts"][0]["sha256"] = wrong_hash

        self.mutate_record("capacity-envelope", replace_hash)
        self.assert_verifier_fails()
    def test_wrong_item_result_artifact_role_fails(self) -> None:
        def replace_role(record: dict[str, Any]) -> None:
            record["raw_artifacts"][1]["role"] = "log"

        self.mutate_record("capacity-envelope", replace_role)
        self.assert_verifier_fails()


    def test_malformed_record_fails(self) -> None:
        item_id = "data-lifecycle"
        self.record_path(item_id).write_text("{not-json\n", encoding="utf-8")
        self.refresh_record_hash(item_id)
        self.assert_verifier_fails()


if __name__ == "__main__":
    unittest.main(verbosity=2)
