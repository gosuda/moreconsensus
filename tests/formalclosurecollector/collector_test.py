#!/usr/bin/env python3
"""Focused positive and adversarial tests for the formal-closure collector."""

from __future__ import annotations

import copy
import hashlib
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

HERE = Path(__file__).resolve().parent
TESTS_DIR = HERE.parent
COLLECTOR = HERE / "collect.py"
VERIFIER = TESTS_DIR / "verify_formal_closure_evidence.py"
SELFTEST = TESTS_DIR / "formal_closure_evidence_selftest.py"
sys.dont_write_bytecode = True


def load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collector = load_module("formal_closure_collector_under_test", COLLECTOR)
verifier = load_module("formal_closure_verifier_for_collector_test", VERIFIER)
fixture_builder = load_module("formal_closure_fixture_for_collector_test", SELFTEST)


def compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_file(path: Path) -> str:
    return digest_bytes(path.read_bytes())


def relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def append_marker(path: Path, prefix: str, value: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(prefix + compact(value) + "\n")


def rewrite_marker(path: Path, prefix: str, mutate: Callable[[dict[str, Any]], None]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    matches = 0
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            value = json.loads(line[len(prefix) :])
            mutate(value)
            lines[index] = prefix + compact(value)
            matches += 1
    if matches != 1:
        raise AssertionError(f"expected one {prefix!r} marker in {path}, got {matches}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def raw_path(root: Path, evidence_path: Path, document: dict[str, Any], artifact_id: str) -> Path:
    artifact = next(item for item in document["raw_artifacts"] if item["artifact_id"] == artifact_id)
    return evidence_path.parent / artifact["path"]


def make_template(root: Path, now: datetime) -> None:
    base, evidence_path, repository = fixture_builder.build_fixture(root, now)
    source_tree_id = base["target"]["source_tree_id"]
    target_id = base["target"]["target_id"]

    finite = base["finite_model_checks"][0]
    tlc_log = raw_path(root, evidence_path, base, finite["tlc_log_artifact_id"])
    with tlc_log.open("a", encoding="utf-8") as handle:
        handle.write("Model checking completed. No error has been found.\n")
        handle.write(
            f"{finite['generated_states']} states generated, {finite['distinct_states']} distinct states found, "
            f"{finite['queue_states']} states left on queue.\n"
        )
        handle.write(f"The depth of the complete state graph search is {finite['search_depth']}.\n")
    for proof in base["inductive_proofs"]:
        proof_log = raw_path(root, evidence_path, base, proof["tlaps_log_artifact_id"])
        with proof_log.open("a", encoding="utf-8") as handle:
            handle.write(f"Theorem {proof['theorem_id']}\n")
            for obligation in range(1, proof["obligations_total"] + 1):
                handle.write(f"Obligation {obligation} proved.\n")
            handle.write("All obligations proved.\n")

    mutation_source = json.loads(
        raw_path(
            root,
            evidence_path,
            base,
            base["mutation_evidence"]["record_artifact_id"],
        ).read_text(encoding="utf-8")
    )
    subject_by_id = {
        entry["subject_id"]: entry for entry in base["mutation_evidence"]["subjects"]
    }
    mutation_role_paths: dict[str, Path] = {}
    witness_role_paths: dict[str, Path] = {}
    for mutation in mutation_source["mutations"]:
        subject_id = mutation["subject_id"]
        subject = subject_by_id[subject_id]
        baseline = raw_path(root, evidence_path, base, subject["baseline_artifact_id"])
        mutant = raw_path(root, evidence_path, base, subject["mutant_artifact_id"])
        detector = raw_path(root, evidence_path, base, subject["detector_artifact_id"])
        mutation["baseline_path"] = relative(root, baseline)
        mutation["mutated_path"] = relative(root, mutant)
        mutation["detector_output_path"] = relative(root, detector)
        mutation_role_paths[f"mutation:{subject_id}:baseline"] = baseline
        mutation_role_paths[f"mutation:{subject_id}:mutant"] = mutant
        mutation_role_paths[f"mutation:{subject_id}:detector-output"] = detector
    for witness in mutation_source["nonvacuity"]:
        subject_id = witness["subject_id"]
        witness_file = raw_path(
            root,
            evidence_path,
            base,
            subject_by_id[subject_id]["witness_artifact_id"],
        )
        witness["witness_path"] = relative(root, witness_file)
        witness_role_paths[f"nonvacuity:{subject_id}:witness"] = witness_file
    mutation_path = root / "mutation-nonvacuity.json"
    mutation_path.write_text(
        json.dumps(mutation_source, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    replay_log = raw_path(
        root,
        evidence_path,
        base,
        base["trace_refinement"]["replay_artifact_id"],
    )
    scope_log = raw_path(
        root,
        evidence_path,
        base,
        base["joint_consensus"]["scope_artifact_id"],
    )
    clock_log = raw_path(
        root,
        evidence_path,
        base,
        base["toq_clock_discipline"]["artifact_id"],
    )
    review_log = raw_path(root, evidence_path, base, base["sign_off"]["artifact_id"])
    rewrite_marker(
        review_log,
        "FORMAL_CLOSURE_REVIEW_RESULT ",
        lambda value: value.update({"reviewed_at": base["generated_at"]}),
    )
    manifest = {
        "schema_version": collector.COLLECTION_SCHEMA_VERSION,
        "record_mode": verifier.SYNTHETIC_MODE,
        "release": {
            "release_id": base["release"]["release_id"],
            "source_repository": base["release"]["source_repository"],
            "source_revision": base["release"]["source_revision"],
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": base["release"]["formal_spec_sha256"],
            "formal_spec_model_id": "formal-closure",
            "target_id": target_id,
        },
        "generated_at": base["generated_at"],
        "valid_until": base["valid_until"],
        "toolchain": [
            {
                "tool_id": entry["tool_id"],
                "version": entry["version"],
                "repository_pin_path": entry["repository_pin_path"],
                "binary_path": relative(
                    root,
                    raw_path(root, evidence_path, base, entry["binary_artifact_id"]),
                ),
            }
            for entry in base["toolchain"]
        ],
        "models": [
            {"model_id": entry["model_id"], "path": entry["path"], "role": entry["role"]}
            for entry in base["specification_manifest"]["models"]
        ],
        "configs": [
            {"config_id": entry["config_id"], "path": entry["path"], "model_id": entry["model_id"]}
            for entry in base["specification_manifest"]["configs"]
        ],
        "properties": {
            "invariants": [
                {key: entry[key] for key in ("property_id", "operator", "module_id")}
                for entry in base["properties"]["invariants"]
            ],
            "temporal_properties": [
                {key: entry[key] for key in ("property_id", "operator", "module_id")}
                for entry in base["properties"]["temporal_properties"]
            ],
        },
        "finite_model_checks": [
            {
                "config_id": entry["config_id"],
                "command": entry["command"],
                "log_path": relative(
                    root,
                    raw_path(root, evidence_path, base, entry["tlc_log_artifact_id"]),
                ),
            }
            for entry in base["finite_model_checks"]
        ],
        "inductive_proofs": [
            {
                "theorem_class": entry["theorem_class"],
                "module_id": entry["module_id"],
                "command": entry["command"],
                "log_path": relative(
                    root,
                    raw_path(root, evidence_path, base, entry["tlaps_log_artifact_id"]),
                ),
            }
            for entry in base["inductive_proofs"]
        ],
        "trace_refinement": {
            "exporter": base["trace_refinement"]["exporter"],
            "format": base["trace_refinement"]["format"],
            "shaped_spec_model_id": base["trace_refinement"]["shaped_spec_model_id"],
            "export_command": base["trace_refinement"]["export_command"],
            "replay_command": base["trace_refinement"]["replay_command"],
            "trace_path": relative(
                root,
                raw_path(root, evidence_path, base, base["trace_refinement"]["trace_artifact_id"]),
            ),
            "replay_log_path": relative(root, replay_log),
        },
        "joint_consensus": {
            "declared_scope": collector.EXCLUDED_JOINT_SCOPE,
            "proof_theorem_id": base["joint_consensus"]["proof_theorem_id"],
            "scope_log_path": relative(root, scope_log),
        },
        "toq_clock_discipline": {
            "clock_source": base["toq_clock_discipline"]["clock_source"],
            "clock_log_path": relative(root, clock_log),
            "remote_observation_path": relative(
                root,
                raw_path(
                    root,
                    evidence_path,
                    base,
                    base["toq_clock_discipline"]["remote_artifact_id"],
                ),
            ),
        },
        "mutation_nonvacuity_path": relative(root, mutation_path),
        "sign_off": {
            "submitted_by": base["sign_off"]["submitted_by"],
            "reviewed_by": base["sign_off"]["reviewed_by"],
            "review_log_path": relative(root, review_log),
        },
        "producer": {
            "signer_identity": base["production_attestation"]["signer_identity"],
            "decided_at": base["generated_at"],
            "signed_at": base["generated_at"],
            "native_execution_record_path": None,
            "native_execution_signature_path": None,
        },
    }
    manifest["sign_off"]["submitted_by"]["authenticated_identity"] = manifest["producer"]["signer_identity"]
    manifest_path = root / "collection.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    reviewable_paths: dict[str, Path] = {
        "collection-manifest": manifest_path,
        "model:formal-closure": repository / "specs/FormalClosure.tla",
        "config:complete-config": repository / "specs/FormalClosure.cfg",
        "producer-pinned-public-key": repository / verifier.PRODUCER_TRUST_ROOT_PATH,
        "reviewer-pinned-public-key": repository / verifier.REVIEWER_TRUST_ROOT_PATH,
        "mutation-nonvacuity": mutation_path,
        "tlc-log:complete-config": tlc_log,
        "trace-export": raw_path(
            root,
            evidence_path,
            base,
            base["trace_refinement"]["trace_artifact_id"],
        ),
        "trace-replay": replay_log,
        "scope-enforcement-source": scope_log,
        "toq-clock-discipline": clock_log,
        "toq-clock-observation": raw_path(
            root,
            evidence_path,
            base,
            base["toq_clock_discipline"]["remote_artifact_id"],
        ),
        **mutation_role_paths,
        **witness_role_paths,
    }
    for entry in base["toolchain"]:
        tool_id = entry["tool_id"]
        reviewable_paths[f"tool-pin:{tool_id}"] = repository / entry["repository_pin_path"]
        reviewable_paths[f"tool-binary:{tool_id}"] = raw_path(
            root,
            evidence_path,
            base,
            entry["binary_artifact_id"],
        )
    for proof in base["inductive_proofs"]:
        reviewable_paths[f"tlaps-log:{proof['theorem_class']}"] = raw_path(
            root,
            evidence_path,
            base,
            proof["tlaps_log_artifact_id"],
        )
    reviewable_values = {
        role: {"sha256": digest_file(path), "size_bytes": path.stat().st_size}
        for role, path in reviewable_paths.items()
    }
    reviewed_input_bindings_hash = collector.reviewed_input_bindings_sha256(reviewable_values)
    rewrite_marker(
        review_log,
        collector.REVIEW_BINDING_MARKER,
        lambda value: value.update(
            {
                "release_id": base["release"]["release_id"],
                "source_revision": base["release"]["source_revision"],
                "source_tree_id": source_tree_id,
                "target_id": target_id,
                "formal_spec_sha256": base["release"]["formal_spec_sha256"],
                "mutation_nonvacuity_sha256": digest_file(mutation_path),
                "reviewed_input_bindings_sha256": reviewed_input_bindings_hash,
                "reviewed_at": base["generated_at"],
                "decision": "approved",
            }
        ),
    )


class FormalClosureCollectorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory(prefix="formal-closure-collector-tests-")
        cls.template = Path(cls._temporary.name) / "template"
        cls.template.mkdir()
        cls.now = datetime.now(timezone.utc).replace(microsecond=0)
        make_template(cls.template, cls.now)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def setUp(self) -> None:
        self.work = Path(self._temporary.name) / self.id().rsplit(".", 1)[-1]
        shutil.copytree(self.template, self.work)
        self.manifest_path = self.work / "collection.json"
        self.repository = self.work / "repository"
        self.output = self.work / "output"

    def load_manifest(self) -> dict[str, Any]:
        return json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def save_manifest(self, value: dict[str, Any]) -> None:
        self.manifest_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def input_path(self, value: str) -> Path:
        return self.work / value

    def collector_command(
        self,
        *,
        reviewer_private: Path | None = None,
        reviewer_public: Path | None = None,
    ) -> list[str]:
        manifest = self.load_manifest()
        return [
            sys.executable,
            str(COLLECTOR),
            "--manifest",
            str(self.manifest_path),
            "--repository-root",
            str(self.repository),
            "--output-dir",
            str(self.output),
            "--producer-private-key",
            str(self.work / "producer-private.pem"),
            "--producer-public-key",
            str(self.repository / verifier.PRODUCER_TRUST_ROOT_PATH),
            "--reviewer-private-key",
            str(reviewer_private or self.work / "reviewer-private.pem"),
            "--reviewer-public-key",
            str(reviewer_public or self.repository / verifier.REVIEWER_TRUST_ROOT_PATH),
            "--expected-target-id",
            manifest["release"]["target_id"],
            "--expected-environment-profile",
            (
                "production"
                if manifest["record_mode"] == verifier.TARGET_MODE
                else verifier.SYNTHETIC_MODE
            ),
            "--allow-synthetic-test-fixture",
        ]

    def run_collector(
        self,
        *,
        expected_success: bool,
        expected_error: str | None = None,
        reviewer_private: Path | None = None,
        reviewer_public: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run(
            self.collector_command(reviewer_private=reviewer_private, reviewer_public=reviewer_public),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )
        if expected_success:
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("mode=synthetic-test claim=none", result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertFalse(self.output.exists(), "failed collection published a partial output")
            if expected_error is not None:
                self.assertIn(expected_error, result.stderr)
        return result

    def run_verifier(self, evidence: Path) -> subprocess.CompletedProcess[str]:
        manifest = self.load_manifest()
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        return subprocess.run(
            [
                sys.executable,
                str(VERIFIER),
                "--expected-release-id",
                manifest["release"]["release_id"],
                "--expected-source-revision",
                manifest["release"]["source_revision"],
                "--expected-formal-spec-sha256",
                manifest["release"]["formal_spec_sha256"],
                "--expected-target-id",
                manifest["release"]["target_id"],
                "--expected-environment-profile",
                (
                    "production"
                    if manifest["record_mode"] == verifier.TARGET_MODE
                    else verifier.SYNTHETIC_MODE
                ),
                "--allow-synthetic-test-fixture",
                "--repository-root",
                str(self.repository),
                str(evidence),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )

    def test_collects_signed_nonclaim_and_existing_verifier_accepts_it(self) -> None:
        self.run_collector(expected_success=True)
        evidence = self.output / "formal-closure-evidence.json"
        document = json.loads(evidence.read_text(encoding="utf-8"))
        self.assertEqual(document["record_mode"], "synthetic-test")
        self.assertEqual(document["claim"], "none")
        self.assertNotEqual(
            document["production_attestation"]["trust_root_sha256"],
            document["sign_off"]["reviewer_trust_root_sha256"],
        )
        self.assertEqual(len(document["mutation_evidence"]["subjects"]), 44)
        self.assertEqual(len(document["reviewed_input_set"]["inputs"]), 203)
        self.assertEqual(len(document["raw_artifacts"]), 197)
        self.assertEqual(
            document["production_attestation"]["signed_payload_sha256"],
            document["sign_off"]["signed_payload_sha256"],
        )
        verified = self.run_verifier(evidence)
        self.assertEqual(verified.returncode, 0, verified.stderr)
        self.assertIn("formal_closure_evidence=verified mode=synthetic-test", verified.stdout)
        self.assertIn("claim=none", verified.stdout)

    def test_rejects_tlc_header_only_forgery(self) -> None:
        manifest = self.load_manifest()
        path = self.input_path(manifest["finite_model_checks"][0]["log_path"])
        machine_lines = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.startswith("FORMAL_CLOSURE_")
        ]
        path.write_text(
            "\n".join(
                [
                    "TLC success header",
                    *machine_lines,
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        self.run_collector(expected_success=False, expected_error="lacks required native tool output")

    def test_rejects_tlaps_header_only_forgery(self) -> None:
        manifest = self.load_manifest()
        path = self.input_path(manifest["inductive_proofs"][0]["log_path"])
        machine_lines = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.startswith("FORMAL_CLOSURE_")
        ]
        theorem_id = verifier.THEOREM_BY_CLASS[
            manifest["inductive_proofs"][0]["theorem_class"]
        ]
        path.write_text(
            "\n".join(
                [
                    "TLAPS success header",
                    f"Theorem {theorem_id}",
                    "All obligations proved.",
                    *machine_lines,
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        self.run_collector(
            expected_success=False,
            expected_error="does not contain every distinct proved obligation",
        )

    def test_rejects_mixed_source_hashes(self) -> None:
        manifest = self.load_manifest()
        proof_log = self.input_path(manifest["inductive_proofs"][0]["log_path"])
        rewrite_marker(
            proof_log,
            "FORMAL_CLOSURE_TLAPS_RESULT ",
            lambda value: value.update({"source_revision": "0" * 64}),
        )
        self.run_collector(expected_success=False, expected_error="does not prove the required theorem")

    def test_rejects_stale_replayed_refinement(self) -> None:
        manifest = self.load_manifest()
        replay_log = self.input_path(manifest["trace_refinement"]["replay_log_path"])
        rewrite_marker(
            replay_log,
            "FORMAL_CLOSURE_TRACE_REPLAY ",
            lambda value: value.update({"replayed_at": "2020-01-01T00:00:00Z"}),
        )
        self.run_collector(expected_success=False, expected_error="inconsistent or stale")

    def test_rejects_same_signer_for_producer_and_reviewer(self) -> None:
        producer_public = self.repository / verifier.PRODUCER_TRUST_ROOT_PATH
        reviewer_public = self.repository / verifier.REVIEWER_TRUST_ROOT_PATH
        copied_private = self.work / "copied-producer-private.pem"
        copied_private.write_bytes((self.work / "producer-private.pem").read_bytes())
        reviewer_public.write_bytes(producer_public.read_bytes())
        self.run_collector(
            expected_success=False,
            expected_error="signing identities must be distinct",
            reviewer_private=copied_private,
            reviewer_public=reviewer_public,
        )

    def test_rejects_missing_theorem(self) -> None:
        manifest = self.load_manifest()
        manifest["inductive_proofs"].pop()
        self.save_manifest(manifest)
        self.run_collector(expected_success=False, expected_error="at least 5 item")

    def test_rejects_missing_model(self) -> None:
        manifest = self.load_manifest()
        manifest["models"].clear()
        self.save_manifest(manifest)
        self.run_collector(expected_success=False, expected_error="at least 1 item")

    def test_rejects_missing_mutation(self) -> None:
        manifest = self.load_manifest()
        mutation_path = self.input_path(manifest["mutation_nonvacuity_path"])
        mutation = json.loads(mutation_path.read_text(encoding="utf-8"))
        mutation["mutations"].pop()
        mutation_path.write_text(json.dumps(mutation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.run_collector(expected_success=False, expected_error="at least 44 item")

    def test_rejects_duplicate_raw_artifact_path(self) -> None:
        manifest = self.load_manifest()
        manifest["inductive_proofs"][1]["log_path"] = manifest["inductive_proofs"][0]["log_path"]
        self.save_manifest(manifest)
        self.run_collector(expected_success=False, expected_error="duplicate input artifact path")

    def test_signature_tamper_reaches_cryptographic_failure(self) -> None:
        self.run_collector(expected_success=True)
        evidence = self.output / "formal-closure-evidence.json"
        document = json.loads(evidence.read_text(encoding="utf-8"))
        signature_record = next(
            item for item in document["raw_artifacts"] if item["artifact_id"] == "producer-signature"
        )
        signature_path = self.output / signature_record["path"]
        signature_path.chmod(0o644)
        signature = bytearray(signature_path.read_bytes())
        signature[len(signature) // 2] ^= 0x01
        signature_path.write_bytes(signature)
        signature_record["sha256"] = digest_file(signature_path)
        evidence.chmod(0o644)
        evidence.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        verified = self.run_verifier(evidence)
        self.assertNotEqual(verified.returncode, 0, verified.stdout)
        self.assertIn("production evidence attestation RSA-SHA256 signature verification failed", verified.stderr)

    def test_rejects_clock_target_binding_mismatch(self) -> None:
        manifest = self.load_manifest()
        clock_log = self.input_path(manifest["toq_clock_discipline"]["clock_log_path"])
        rewrite_marker(
            clock_log,
            "FORMAL_CLOSURE_TOQ_CLOCK ",
            lambda value: value.update({"target_id": "different-darwin-target"}),
        )
        self.run_collector(expected_success=False, expected_error="does not match the release environment")

    def test_rejects_clock_source_not_bound_by_observation(self) -> None:
        manifest = self.load_manifest()
        manifest["toq_clock_discipline"]["clock_source"] = "different authenticated clock source"
        self.save_manifest(manifest)
        self.run_collector(
            expected_success=False,
            expected_error="not an admissible authenticated monotonic source",
        )

    def test_rejects_reused_review_for_changed_formal_input(self) -> None:
        manifest = self.load_manifest()
        tlc_log = self.input_path(manifest["finite_model_checks"][0]["log_path"])
        with tlc_log.open("a", encoding="utf-8") as handle:
            handle.write("Additional post-review formal output line.\n")
        self.run_collector(
            expected_success=False,
            expected_error="independent review does not bind",
        )

    def test_rejects_negated_joint_scope_statement(self) -> None:
        manifest = self.load_manifest()
        manifest["joint_consensus"]["declared_scope"] = (
            "Joint consensus is not excluded from the declared release scope."
        )
        self.save_manifest(manifest)
        self.run_collector(
            expected_success=False,
            expected_error="canonical affirmative scope",
        )

    def test_rejects_unbound_tlc_command(self) -> None:
        manifest = self.load_manifest()
        config_path = manifest["configs"][0]["path"]
        manifest["finite_model_checks"][0]["command"] = [
            "false",
            "-config",
            config_path,
        ]
        self.save_manifest(manifest)
        self.run_collector(
            expected_success=False,
            expected_error="must be the exact pinned TLC invocation",
        )

    def test_rejects_property_operator_not_equal_to_property_id(self) -> None:
        manifest = self.load_manifest()
        manifest["properties"]["invariants"][0]["operator"] = "DifferentOperator"
        self.save_manifest(manifest)
        self.run_collector(
            expected_success=False,
            expected_error="operator must equal property_id",
        )

    def test_rejects_empty_per_check_property_coverage(self) -> None:
        manifest = self.load_manifest()
        tlc_log = self.input_path(manifest["finite_model_checks"][0]["log_path"])
        rewrite_marker(
            tlc_log,
            "FORMAL_CLOSURE_TLC_RESULT ",
            lambda value: value.update({"checked_invariant_ids": []}),
        )
        self.run_collector(
            expected_success=False,
            expected_error="array with at least 1 item",
        )

    def test_rejects_self_asserted_mutation_detector_result(self) -> None:
        manifest = self.load_manifest()
        mutation_path = self.input_path(manifest["mutation_nonvacuity_path"])
        mutation = json.loads(mutation_path.read_text(encoding="utf-8"))
        first = mutation["mutations"][0]
        detector_path = self.input_path(first["detector_output_path"])
        detector = json.loads(detector_path.read_text(encoding="utf-8"))
        detector["result"] = "pass"
        detector_path.write_text(
            json.dumps(detector, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        first["detector_output_sha256"] = digest_file(detector_path)
        mutation_path.write_text(
            json.dumps(mutation, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.run_collector(
            expected_success=False,
            expected_error="semantic rejection result",
        )

    def test_rejects_opaque_only_mutation_text(self) -> None:
        manifest = self.load_manifest()
        mutation_path = self.input_path(manifest["mutation_nonvacuity_path"])
        mutation_path.write_text(
            "All theorem, config, and property mutations were rejected.\n",
            encoding="utf-8",
        )
        self.run_collector(
            expected_success=False,
            expected_error="cannot load mutation/nonvacuity evidence",
        )

    def test_rejects_tlc_command_tool_hash_mismatch(self) -> None:
        manifest = self.load_manifest()
        tlc_log = self.input_path(manifest["finite_model_checks"][0]["log_path"])
        rewrite_marker(
            tlc_log,
            collector.COMMAND_BINDING_MARKER,
            lambda value: value.update({"tlc_binary_sha256": "0" * 64}),
        )
        self.run_collector(
            expected_success=False,
            expected_error="does not bind the exact command and pinned executables",
        )

    def test_rejects_same_target_label_with_different_tree(self) -> None:
        manifest = self.load_manifest()
        mutation_path = self.input_path(manifest["mutation_nonvacuity_path"])
        mutation = json.loads(mutation_path.read_text(encoding="utf-8"))
        mutation["source_tree_id"] = "0" * 64
        mutation_path.write_text(
            json.dumps(mutation, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.run_collector(
            expected_success=False,
            expected_error="source_tree_id does not match the exact target contract",
        )

    def test_rejects_review_digest_with_omitted_input(self) -> None:
        manifest = self.load_manifest()
        review_log = self.input_path(manifest["sign_off"]["review_log_path"])
        rewrite_marker(
            review_log,
            collector.REVIEW_BINDING_MARKER,
            lambda value: value.update(
                {
                    "reviewed_input_bindings_sha256": hashlib.sha256(
                        collector.canonical_bytes([])
                    ).hexdigest()
                }
            ),
        )
        self.run_collector(
            expected_success=False,
            expected_error="independent review does not bind",
        )

    def test_rejects_absent_trace_replay_time(self) -> None:
        manifest = self.load_manifest()
        replay_log = self.input_path(manifest["trace_refinement"]["replay_log_path"])
        rewrite_marker(
            replay_log,
            "FORMAL_CLOSURE_TRACE_REPLAY ",
            lambda value: value.pop("replayed_at"),
        )
        self.run_collector(
            expected_success=False,
            expected_error="keys mismatch",
        )

    def test_rejects_review_before_execution(self) -> None:
        manifest = self.load_manifest()
        review_log = self.input_path(manifest["sign_off"]["review_log_path"])
        stale = "2020-01-01T00:00:00Z"
        rewrite_marker(
            review_log,
            "FORMAL_CLOSURE_REVIEW_RESULT ",
            lambda value: value.update({"reviewed_at": stale}),
        )
        rewrite_marker(
            review_log,
            collector.REVIEW_BINDING_MARKER,
            lambda value: value.update({"reviewed_at": stale}),
        )
        self.run_collector(
            expected_success=False,
            expected_error="inconsistent or stale",
        )

    def test_rejects_header_only_mutation_detector(self) -> None:
        manifest = self.load_manifest()
        mutation_path = self.input_path(manifest["mutation_nonvacuity_path"])
        mutation = json.loads(mutation_path.read_text(encoding="utf-8"))
        first = mutation["mutations"][0]
        detector_path = self.input_path(first["detector_output_path"])
        detector_path.write_text(
            "FORMAL_CLOSURE_MUTATION_RESULT rejected\n",
            encoding="utf-8",
        )
        first["detector_output_sha256"] = digest_file(detector_path)
        matching_witness = next(
            item
            for item in mutation["nonvacuity"]
            if item["subject_id"] == first["subject_id"]
        )
        matching_witness["detector_output_sha256"] = first["detector_output_sha256"]
        mutation_path.write_text(
            json.dumps(mutation, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.run_collector(
            expected_success=False,
            expected_error="cannot load",
        )

    def test_synthetic_fixture_cannot_be_promoted_to_target_claim(self) -> None:
        def git(*args: str) -> str:
            completed = subprocess.run(
                ["git", "-C", str(self.repository), *args],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            return completed.stdout.strip()

        git("init", "-q")
        git("config", "user.name", "Formal Collector Test")
        git("config", "user.email", "formal-collector@invalid.test")
        git("add", ".")
        git("commit", "-q", "-m", "synthetic repository fixture")
        target_revision = git("rev-parse", "HEAD")
        target_tree = git("rev-parse", "HEAD^{tree}")

        manifest = self.load_manifest()
        old_revision = manifest["release"]["source_revision"]
        for path in self.work.rglob("*"):
            if (
                not path.is_file()
                or ".git" in path.parts
                or path.suffix not in {".json", ".log"}
            ):
                continue
            text = path.read_text(encoding="utf-8")
            if old_revision in text:
                path.write_text(
                    text.replace(old_revision, target_revision),
                    encoding="utf-8",
                )
        manifest = self.load_manifest()
        manifest["record_mode"] = verifier.TARGET_MODE
        manifest["release"]["source_revision"] = target_revision
        manifest["release"]["source_tree_id"] = target_tree
        self.save_manifest(manifest)
        command = self.collector_command()
        command.remove("--allow-synthetic-test-fixture")
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "does not match the exact target contract",
            result.stderr,
        )
        self.assertFalse(self.output.exists())

    def test_rejects_skip_worktree_substitution_against_bound_git_blob(self) -> None:
        repository = self.work / "blob-bound-repository"
        repository.mkdir()
        pinned = repository / "pinned.pem"
        pinned.write_text("committed pinned key bytes\n", encoding="utf-8")

        def git(*args: str) -> None:
            completed = subprocess.run(
                ["git", "-C", str(repository), *args],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

        git("init", "-q")
        git("config", "user.name", "Formal Collector Test")
        git("config", "user.email", "formal-collector@invalid.test")
        git("add", "pinned.pem")
        git("commit", "-q", "-m", "pin trust root")
        git("update-index", "--skip-worktree", "pinned.pem")
        pinned.write_text("substituted attacker key bytes\n", encoding="utf-8")
        with self.assertRaisesRegex(
            collector.CollectionError,
            "bytes do not match the bound Git blob",
        ):
            collector.require_head_blob_match(
                repository,
                "pinned.pem",
                pinned,
                digest_file(pinned),
                pinned.stat().st_size,
                "pinned trust root",
            )

    def test_git_replace_cannot_redirect_bound_tree_or_blob(self) -> None:
        repository = self.work / "replace-bound-repository"
        repository.mkdir()
        pinned = repository / "pinned.pem"

        def git(*args: str, no_replace: bool = False) -> str:
            command = ["git"]
            if no_replace:
                command.append("--no-replace-objects")
            completed = subprocess.run(
                [*command, "-C", str(repository), *args],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            return completed.stdout.strip()

        git("init", "-q")
        git("config", "user.name", "Formal Collector Test")
        git("config", "user.email", "formal-collector@invalid.test")
        pinned.write_text("committed pinned key bytes\n", encoding="utf-8")
        git("add", "pinned.pem")
        git("commit", "-q", "-m", "original trust root")
        original_commit = git("rev-parse", "HEAD", no_replace=True)
        original_tree = git("rev-parse", "HEAD^{tree}", no_replace=True)
        pinned.write_text("replacement attacker key bytes\n", encoding="utf-8")
        git("add", "pinned.pem")
        git("commit", "-q", "-m", "replacement trust root")
        replacement_commit = git("rev-parse", "HEAD", no_replace=True)
        git("reset", "--hard", original_commit, no_replace=True)
        git("replace", original_commit, replacement_commit)
        self.assertEqual(collector.git_tree_id(repository), original_tree)
        pinned.write_text("replacement attacker key bytes\n", encoding="utf-8")
        with self.assertRaisesRegex(
            collector.CollectionError,
            "bytes do not match the bound Git blob",
        ):
            collector.require_head_blob_match(
                repository,
                "pinned.pem",
                pinned,
                digest_file(pinned),
                pinned.stat().st_size,
                "pinned trust root",
            )

    def test_rejects_collection_without_explicit_synthetic_permission(self) -> None:
        command = self.collector_command()
        command.remove("--allow-synthetic-test-fixture")
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires --allow-synthetic-test-fixture", result.stderr)
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
