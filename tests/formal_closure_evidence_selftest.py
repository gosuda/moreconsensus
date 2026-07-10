#!/usr/bin/env python3
"""Focused positive and negative self-test for formal closure evidence."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import platform
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

TESTS_DIR = Path(__file__).resolve().parent
VERIFIER_PATH = TESTS_DIR / "verify_formal_closure_evidence.py"
sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("formal_closure_verifier", VERIFIER_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("cannot load formal closure verifier")
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

RELEASE_ID = "release-2030-01"
SOURCE_REVISION = hashlib.sha256(b"synthetic non-claim source revision").hexdigest()


def timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compact(value: dict[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)

def openssl(args: list[str], *, payload: bytes | None = None) -> bytes:
    completed = subprocess.run(
        ["openssl", *args],
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(f"local openssl failed: {completed.stderr.decode('utf-8', errors='replace')}")
    return completed.stdout


def build_fixture(root: Path, now: datetime) -> tuple[dict[str, Any], Path, Path]:
    repository = root / "repository"
    bundle = root / "bundle"
    artifacts_dir = bundle / "artifacts"
    pins_dir = repository / "pins"
    specs_dir = repository / "specs"
    trust_dir = repository / "tests"
    artifacts_dir.mkdir(parents=True)
    pins_dir.mkdir(parents=True)
    specs_dir.mkdir(parents=True)
    trust_dir.mkdir(parents=True)

    producer_private = root / "producer-private.pem"
    reviewer_private = root / "reviewer-private.pem"
    producer_public = trust_dir / "formal_closure_producer_public.pem"
    reviewer_public = trust_dir / "formal_closure_reviewer_public.pem"
    for private_key, public_key in (
        (producer_private, producer_public),
        (reviewer_private, reviewer_public),
    ):
        openssl(
            [
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
                str(private_key),
            ]
        )
        openssl(["pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)])

    versions = {
        "go": "1.24.5",
        "java": "21.0.4",
        "tlc": "2.20",
        "tlaps": "1.5.0",
        "trace-replay": "1.0.0",
    }
    model_path = specs_dir / "FormalClosure.tla"
    config_path = specs_dir / "FormalClosure.cfg"
    model_path.write_text(
        "\n".join(
            [
                "---- MODULE FormalClosure ----",
                "Spec == TRUE",
                *(f"THEOREM {theorem_id} == TRUE" for theorem_id in verifier.THEOREM_BY_CLASS.values()),
                "====",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_path.write_text(
        "\n".join(
            [
                "SPECIFICATION Spec",
                *(f"INVARIANT {property_id}" for property_id in verifier.INVARIANT_IDS),
                *(f"PROPERTY {property_id}" for property_id in verifier.TEMPORAL_IDS),
                "CHECK_DEADLOCK TRUE",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    formal_spec_sha = digest(model_path)
    config_sha = digest(config_path)
    source_tree_id = hashlib.sha256(b"synthetic non-claim source tree").hexdigest()
    target_id = "synthetic-darwin-formal-target"
    generated_at = now.replace(microsecond=0) - timedelta(minutes=2)
    event_at = generated_at - timedelta(minutes=3)
    event_text = timestamp(event_at)
    generated_text = timestamp(generated_at)
    target = {
        "target_id": target_id,
        "operating_system": "Darwin",
        "architecture": platform.machine(),
        "execution_mode": verifier.SYNTHETIC_MODE,
        "environment": verifier.SYNTHETIC_MODE,
        "source_tree_id": source_tree_id,
    }
    common_target = {
        "release_id": RELEASE_ID,
        "source_revision": SOURCE_REVISION,
        "source_tree_id": source_tree_id,
        "target_id": target_id,
        "formal_spec_sha256": formal_spec_sha,
        "result": "pass",
    }
    raw_artifacts: list[dict[str, Any]] = []
    artifact_files: dict[str, Path] = {}

    def add_artifact(artifact_id: str, kind: str, content: bytes) -> tuple[str, str]:
        artifact_path = artifacts_dir / artifact_id
        artifact_path.write_bytes(content)
        artifact_hash = digest(artifact_path)
        raw_artifacts.append(
            {
                "artifact_id": artifact_id,
                "kind": kind,
                "path": f"artifacts/{artifact_id}",
                "sha256": artifact_hash,
                "size_bytes": len(content),
                "source_revision": SOURCE_REVISION,
                "created_at": event_text,
            }
        )
        artifact_files[artifact_id] = artifact_path
        return artifact_id, artifact_hash

    binary_hashes: dict[str, str] = {}
    binary_ids: dict[str, str] = {}
    for tool_id in verifier.TOOL_IDS:
        artifact_id, artifact_hash = add_artifact(
            f"{tool_id}-binary.bin",
            f"{tool_id}-binary",
            f"pinned {tool_id} executable bytes\n".encode(),
        )
        binary_ids[tool_id] = artifact_id
        binary_hashes[tool_id] = artifact_hash
    pin_paths: dict[str, Path] = {}
    for tool_id, version in versions.items():
        pin_path = pins_dir / f"{tool_id}.json"
        pin_path.write_text(
            json.dumps(
                {"binary_sha256": binary_hashes[tool_id], "tool_id": tool_id, "version": version},
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        pin_paths[tool_id] = pin_path
    toolchain = [
        {
            "tool_id": tool_id,
            "version": versions[tool_id],
            "repository_pin_path": f"pins/{tool_id}.json",
            "repository_pin_sha256": digest(pin_paths[tool_id]),
            "binary_sha256": binary_hashes[tool_id],
            "binary_artifact_id": binary_ids[tool_id],
            "source_revision": SOURCE_REVISION,
        }
        for tool_id in verifier.TOOL_IDS
    ]
    models = [
        {
            "model_id": "formal-closure",
            "path": "specs/FormalClosure.tla",
            "sha256": formal_spec_sha,
            "role": "claim-root-shaped-spec",
            "source_revision": SOURCE_REVISION,
        }
    ]
    configs = [
        {
            "config_id": "complete-config",
            "path": "specs/FormalClosure.cfg",
            "sha256": config_sha,
            "model_id": "formal-closure",
            "source_revision": SOURCE_REVISION,
        }
    ]
    specification_manifest = {
        "manifest_sha256": verifier.canonical_manifest_sha256(models, configs),
        "models": models,
        "configs": configs,
    }
    properties = {
        "invariants": [
            {
                "property_id": property_id,
                "operator": property_id,
                "module_id": "formal-closure",
                "required": True,
            }
            for property_id in verifier.INVARIANT_IDS
        ],
        "temporal_properties": [
            {
                "property_id": property_id,
                "operator": property_id,
                "module_id": "formal-closure",
                "required": True,
            }
            for property_id in verifier.TEMPORAL_IDS
        ],
    }

    tlc_command = [
        "java",
        "-jar",
        "tla2tools.jar",
        "-config",
        "specs/FormalClosure.cfg",
        "specs/FormalClosure.tla",
    ]
    tlc_command_hash = hashlib.sha256(verifier.canonical_json_bytes(tlc_command)).hexdigest()
    tlc_result = {
        "area_ids": list(verifier.FINITE_AREA_IDS),
        "checked_invariant_ids": list(verifier.INVARIANT_IDS),
        "checked_temporal_ids": list(verifier.TEMPORAL_IDS),
        "completed_at": event_text,
        "config_id": "complete-config",
        "config_sha256": config_sha,
        "distinct_states": 80,
        "errors": 0,
        "generated_states": 100,
        "model_sha256": formal_spec_sha,
        "queue_states": 0,
        "result": "pass",
        "search_depth": 12,
        "source_revision": SOURCE_REVISION,
        "started_at": event_text,
        "tool_binary_sha256": binary_hashes["tlc"],
        "tool_version": versions["tlc"],
    }
    tlc_binding = {
        "release_id": RELEASE_ID,
        "source_revision": SOURCE_REVISION,
        "source_tree_id": source_tree_id,
        "target_id": target_id,
        "config_id": "complete-config",
        "config_sha256": config_sha,
        "model_sha256": formal_spec_sha,
        "command_sha256": tlc_command_hash,
        "java_binary_sha256": binary_hashes["java"],
        "tlc_binary_sha256": binary_hashes["tlc"],
        "working_directory": ".",
        "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
        "exit_code": 0,
        "completed_at": event_text,
        "result": "pass",
    }
    tlc_log_id, _ = add_artifact(
        "complete-config.tlc.log",
        "tlc-raw-log",
        (
            "TLC raw output\nFORMAL_CLOSURE_TLC_RESULT "
            + compact(tlc_result)
            + "\nFORMAL_CLOSURE_COMMAND_BINDING "
            + compact(tlc_binding)
            + "\n"
        ).encode(),
    )
    finite_checks = [
        {
            "config_id": "complete-config",
            "area_ids": list(verifier.FINITE_AREA_IDS),
            "checked_invariant_ids": list(verifier.INVARIANT_IDS),
            "checked_temporal_ids": list(verifier.TEMPORAL_IDS),
            "command": tlc_command,
            "command_sha256": tlc_command_hash,
            "java_binary_sha256": binary_hashes["java"],
            "working_directory": ".",
            "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
            "exit_code": 0,
            "tool_id": "tlc",
            "tool_version": versions["tlc"],
            "tool_binary_sha256": binary_hashes["tlc"],
            "generated_states": 100,
            "distinct_states": 80,
            "search_depth": 12,
            "queue_states": 0,
            "errors": 0,
            "deadlock_result": "pass",
            "result": "pass",
            "started_at": event_text,
            "completed_at": event_text,
            "tlc_log_artifact_id": tlc_log_id,
        }
    ]

    proofs: list[dict[str, Any]] = []
    for theorem_class in sorted(verifier.THEOREM_BY_CLASS):
        theorem_id = verifier.THEOREM_BY_CLASS[theorem_class]
        assumptions = list(verifier.FAIR_LOSS_ASSUMPTIONS) if theorem_class == "fair-loss-liveness" else []
        proof_command = ["tlapm", "specs/FormalClosure.tla", "--theorem", theorem_id]
        proof_command_hash = hashlib.sha256(verifier.canonical_json_bytes(proof_command)).hexdigest()
        proof_result = {
            "admitted_obligations": 0,
            "assumptions": assumptions,
            "checked_at": event_text,
            "history_scope": "arbitrary-history",
            "module_sha256": formal_spec_sha,
            "obligations_proved": 7,
            "obligations_total": 7,
            "omitted_obligations": 0,
            "result": "pass",
            "source_revision": SOURCE_REVISION,
            "status": "checked",
            "theorem_class": theorem_class,
            "theorem_id": theorem_id,
            "tool_binary_sha256": binary_hashes["tlaps"],
            "tool_version": versions["tlaps"],
        }
        proof_binding = {
            "release_id": RELEASE_ID,
            "source_revision": SOURCE_REVISION,
            "source_tree_id": source_tree_id,
            "target_id": target_id,
            "theorem_class": theorem_class,
            "theorem_id": theorem_id,
            "module_sha256": formal_spec_sha,
            "command_sha256": proof_command_hash,
            "tlaps_binary_sha256": binary_hashes["tlaps"],
            "working_directory": ".",
            "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
            "exit_code": 0,
            "completed_at": event_text,
            "result": "pass",
        }
        log_id, _ = add_artifact(
            f"{theorem_class}.tlaps.log",
            "tlaps-raw-log",
            (
                "TLAPS raw output\nFORMAL_CLOSURE_TLAPS_RESULT "
                + compact(proof_result)
                + "\nFORMAL_CLOSURE_COMMAND_BINDING "
                + compact(proof_binding)
                + "\n"
            ).encode(),
        )
        proofs.append(
            {
                "theorem_id": theorem_id,
                "theorem_class": theorem_class,
                "module_id": "formal-closure",
                "command": proof_command,
                "command_sha256": proof_command_hash,
                "working_directory": ".",
                "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
                "exit_code": 0,
                "status": "checked",
                "result": "pass",
                "inductive": True,
                "history_scope": "arbitrary-history",
                "assumptions": assumptions,
                "obligations_total": 7,
                "obligations_proved": 7,
                "admitted_obligations": 0,
                "omitted_obligations": 0,
                "tool_id": "tlaps",
                "tool_version": versions["tlaps"],
                "tool_binary_sha256": binary_hashes["tlaps"],
                "checked_at": event_text,
                "tlaps_log_artifact_id": log_id,
            }
        )

    export_command = ["go", "test", "./traceexport", "-run", "TestExport"]
    replay_command = ["java", "-jar", "trace-replay.jar", "go-shaped.trace.json"]
    export_command_hash = hashlib.sha256(verifier.canonical_json_bytes(export_command)).hexdigest()
    replay_command_hash = hashlib.sha256(verifier.canonical_json_bytes(replay_command)).hexdigest()
    trace_id, trace_hash = add_artifact(
        "go-shaped.trace.json",
        "trace-export",
        (
            compact(
                {
                    "deterministic": True,
                    "release_id": RELEASE_ID,
                    "source_revision": SOURCE_REVISION,
                    "source_tree_id": source_tree_id,
                    "target_id": target_id,
                    "formal_spec_sha256": formal_spec_sha,
                    "format": "moreconsensus-shaped-spec-trace-v1",
                    "exported_at": event_text,
                    "export_command_sha256": export_command_hash,
                    "go_binary_sha256": binary_hashes["go"],
                    "working_directory": ".",
                    "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
                    "exit_code": 0,
                    "traces": [
                        {
                            "trace_id": "trace-1",
                            "events": [
                                {"step": 1, "go_event": "Propose", "spec_action": "Propose"},
                                {"step": 2, "go_event": "Commit", "spec_action": "Commit"},
                            ],
                        }
                    ],
                }
            )
            + "\n"
        ).encode(),
    )
    replay_result = {
        "deterministic": True,
        "release_id": RELEASE_ID,
        "source_revision": SOURCE_REVISION,
        "source_tree_id": source_tree_id,
        "target_id": target_id,
        "formal_spec_sha256": formal_spec_sha,
        "trace_count": 1,
        "trace_sha256": trace_hash,
        "exported_at": event_text,
        "replayed_at": event_text,
        "export_command_sha256": export_command_hash,
        "replay_command_sha256": replay_command_hash,
        "java_binary_sha256": binary_hashes["java"],
        "go_binary_sha256": binary_hashes["go"],
        "trace_replay_binary_sha256": binary_hashes["trace-replay"],
        "working_directory": ".",
        "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
        "export_exit_code": 0,
        "replay_exit_code": 0,
        "result": "pass",
    }
    replay_id, replay_hash = add_artifact(
        "go-shaped.replay.log",
        "trace-replay",
        ("Replay raw output\nFORMAL_CLOSURE_TRACE_REPLAY " + compact(replay_result) + "\n").encode(),
    )
    trace_refinement = {
        "source_revision": SOURCE_REVISION,
        "exporter": "go-to-shaped-spec-v1",
        "format": "moreconsensus-shaped-spec-trace-v1",
        "shaped_spec_model_id": "formal-closure",
        "deterministic": True,
        "trace_count": 1,
        "export_command": export_command,
        "export_command_sha256": export_command_hash,
        "replay_command": replay_command,
        "replay_command_sha256": replay_command_hash,
        "java_binary_sha256": binary_hashes["java"],
        "go_binary_sha256": binary_hashes["go"],
        "replay_tool_binary_sha256": binary_hashes["trace-replay"],
        "working_directory": ".",
        "environment_sha256": verifier.EMPTY_ENVIRONMENT_SHA256,
        "export_exit_code": 0,
        "replay_exit_code": 0,
        "export_result": "pass",
        "replay_result": "pass",
        "trace_artifact_id": trace_id,
        "trace_sha256": trace_hash,
        "replay_artifact_id": replay_id,
        "replay_sha256": replay_hash,
        "exported_at": event_text,
        "replayed_at": event_text,
    }
    joint_status = "excluded-from-declared-scope"
    scope_result = {
        **common_target,
        "collected_at": event_text,
        "status": joint_status,
    }
    scope_id, _ = add_artifact(
        "release-scope.log",
        "scope-enforcement",
        ("Scope check raw output\nFORMAL_CLOSURE_SCOPE_RESULT " + compact(scope_result) + "\n").encode(),
    )
    remote_toq_observation = {
        **common_target,
        "record_kind": "formal-closure-toq-clock-observation",
        "clock_source": "authenticated monotonic source",
        "observed_at": event_text,
        "maximum_clock_error_ms": 25,
        "target_environment": verifier.SYNTHETIC_MODE,
    }
    remote_toq_bytes = (compact(remote_toq_observation) + "\n").encode()
    remote_toq_id, remote_toq_hash = add_artifact(
        "toq-clock-observation.json",
        "toq-clock-observation",
        remote_toq_bytes,
    )
    toq_uri = f"https://evidence.gosuda.org/formal/clock/sha256/{remote_toq_hash}"

    toq_result = {
        **common_target,
        "clock_source": "authenticated monotonic source",
        "collected_at": event_text,
        "evidence_uri": toq_uri,
        "maximum_clock_error_ms": 25,
        "remote_evidence_sha256": remote_toq_hash,
        "target_environment": verifier.SYNTHETIC_MODE,
    }
    toq_id, toq_hash = add_artifact(
        "toq-clock.log",
        "toq-clock-discipline",
        ("Clock discipline raw output\nFORMAL_CLOSURE_TOQ_CLOCK " + compact(toq_result) + "\n").encode(),
    )

    expected_subjects = sorted(
        {
            *(f"theorem:{name}" for name in verifier.THEOREM_BY_CLASS),
            "config:complete-config",
            *(f"property:{name}" for name in verifier.INVARIANT_IDS),
            *(f"property:{name}" for name in verifier.TEMPORAL_IDS),
        }
    )
    mutation_entries: list[dict[str, Any]] = []
    nonvacuity_entries: list[dict[str, Any]] = []
    mutation_subjects: list[dict[str, Any]] = []
    for subject_id in expected_subjects:
        subject_kind = subject_id.partition(":")[0]
        token = hashlib.sha256(subject_id.encode()).hexdigest()[:20]
        baseline_id, baseline_hash = add_artifact(
            f"mutation-{token}-baseline.input",
            "mutation-baseline",
            f"baseline semantic input for {subject_id}\n".encode(),
        )
        mutant_id, mutant_hash = add_artifact(
            f"mutation-{token}-mutant.input",
            "mutation-mutant",
            f"mutated semantic input for {subject_id}\n".encode(),
        )
        detected_by = f"semantic-{subject_kind}-mutation-check"
        detector = {
            "record_kind": "formal-closure-subject-mutation-result",
            "subject_id": subject_id,
            "subject_kind": subject_kind,
            "release_id": RELEASE_ID,
            "source_revision": SOURCE_REVISION,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha,
            "target_id": target_id,
            "completed_at": event_text,
            "result": "rejected",
            "baseline_sha256": baseline_hash,
            "mutated_sha256": mutant_hash,
            "detected_by": detected_by,
        }
        detector_id, detector_hash = add_artifact(
            f"mutation-{token}-detector.json",
            "mutation-detector",
            (json.dumps(detector, indent=2, sort_keys=True) + "\n").encode(),
        )
        witness = {
            "record_kind": "formal-closure-nonvacuity-witness",
            "subject_id": subject_id,
            "subject_kind": subject_kind,
            "release_id": RELEASE_ID,
            "source_revision": SOURCE_REVISION,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha,
            "target_id": target_id,
            "observed_at": event_text,
            "witness_count": 1,
            "result": "pass",
            "baseline_sha256": baseline_hash,
            "mutated_sha256": mutant_hash,
            "detector_output_sha256": detector_hash,
            "detected_by": detected_by,
        }
        witness_id, witness_hash = add_artifact(
            f"nonvacuity-{token}-witness.json",
            "nonvacuity-witness",
            (json.dumps(witness, indent=2, sort_keys=True) + "\n").encode(),
        )
        mutation_entries.append(
            {
                "mutation_id": subject_id,
                "subject_id": subject_id,
                "subject_kind": subject_kind,
                "baseline_path": f"inputs/{baseline_id}",
                "baseline_sha256": baseline_hash,
                "mutated_path": f"inputs/{mutant_id}",
                "mutated_sha256": mutant_hash,
                "detector_output_path": f"inputs/{detector_id}",
                "detector_output_sha256": detector_hash,
                "result": "rejected",
                "detected_by": detected_by,
            }
        )
        nonvacuity_entries.append(
            {
                "subject_id": subject_id,
                "subject_kind": subject_kind,
                "baseline_sha256": baseline_hash,
                "mutated_sha256": mutant_hash,
                "detector_output_sha256": detector_hash,
                "detected_by": detected_by,
                "witness_count": 1,
                "result": "pass",
                "witness_path": f"inputs/{witness_id}",
                "witness_sha256": witness_hash,
            }
        )
        mutation_subjects.append(
            {
                "subject_id": subject_id,
                "subject_kind": subject_kind,
                "baseline_artifact_id": baseline_id,
                "baseline_sha256": baseline_hash,
                "mutant_artifact_id": mutant_id,
                "mutant_sha256": mutant_hash,
                "detector_artifact_id": detector_id,
                "detector_sha256": detector_hash,
                "detected_by": detected_by,
                "detector_completed_at": event_text,
                "mutation_result": "rejected",
                "witness_artifact_id": witness_id,
                "witness_sha256": witness_hash,
                "witness_count": 1,
                "witness_observed_at": event_text,
                "nonvacuity_result": "pass",
            }
        )
    mutation_record = {
        "schema_version": verifier.SCHEMA_VERSION,
        "record_kind": "formal-closure-mutation-nonvacuity",
        "release_id": RELEASE_ID,
        "source_revision": SOURCE_REVISION,
        "source_tree_id": source_tree_id,
        "formal_spec_sha256": formal_spec_sha,
        "target_id": target_id,
        "evidence_mode": verifier.SYNTHETIC_MODE,
        "executed_at": event_text,
        "result": "pass",
        "mutations": mutation_entries,
        "nonvacuity": nonvacuity_entries,
    }
    mutation_record_id, mutation_record_hash = add_artifact(
        "mutation-manifest.json",
        "mutation-manifest",
        (json.dumps(mutation_record, indent=2, sort_keys=True) + "\n").encode(),
    )
    mutation_evidence = {
        "record_artifact_id": mutation_record_id,
        "record_sha256": mutation_record_hash,
        "executed_at": event_text,
        "result": "pass",
        "subjects": mutation_subjects,
    }

    reviewer_identity = "oidc:reviewer-a"
    collection_manifest = {
        "schema_version": verifier.SCHEMA_VERSION,
        "record_mode": verifier.SYNTHETIC_MODE,
        "release": {
            "release_id": RELEASE_ID,
            "source_repository": "https://gosuda.org/moreconsensus/source",
            "source_revision": SOURCE_REVISION,
            "source_tree_id": source_tree_id,
            "formal_spec_sha256": formal_spec_sha,
            "formal_spec_model_id": "formal-closure",
            "target_id": target_id,
        },
        "generated_at": generated_text,
        "valid_until": timestamp(generated_at + timedelta(days=1)),
        "toolchain": [
            {
                "tool_id": entry["tool_id"],
                "version": entry["version"],
                "repository_pin_path": entry["repository_pin_path"],
                "binary_path": f"inputs/{entry['tool_id']}-binary.bin",
            }
            for entry in toolchain
        ],
        "models": [
            {"model_id": entry["model_id"], "path": entry["path"], "role": entry["role"]}
            for entry in models
        ],
        "configs": [
            {"config_id": entry["config_id"], "path": entry["path"], "model_id": entry["model_id"]}
            for entry in configs
        ],
        "properties": {
            key: [
                {
                    "property_id": entry["property_id"],
                    "operator": entry["operator"],
                    "module_id": entry["module_id"],
                }
                for entry in entries
            ]
            for key, entries in properties.items()
        },
        "finite_model_checks": [
            {
                "config_id": entry["config_id"],
                "command": entry["command"],
                "log_path": f"inputs/{entry['tlc_log_artifact_id']}",
            }
            for entry in finite_checks
        ],
        "inductive_proofs": [
            {
                "theorem_class": entry["theorem_class"],
                "module_id": entry["module_id"],
                "command": entry["command"],
                "log_path": f"inputs/{entry['tlaps_log_artifact_id']}",
            }
            for entry in proofs
        ],
        "trace_refinement": {
            "exporter": trace_refinement["exporter"],
            "format": trace_refinement["format"],
            "shaped_spec_model_id": trace_refinement["shaped_spec_model_id"],
            "export_command": trace_refinement["export_command"],
            "replay_command": trace_refinement["replay_command"],
            "trace_path": f"inputs/{trace_id}",
            "replay_log_path": f"inputs/{replay_id}",
        },
        "joint_consensus": {
            "declared_scope": "Joint consensus is excluded from the declared release scope.",
            "proof_theorem_id": None,
            "scope_log_path": f"inputs/{scope_id}",
        },
        "toq_clock_discipline": {
            "clock_source": "authenticated monotonic source",
            "clock_log_path": f"inputs/{toq_id}",
            "remote_observation_path": f"inputs/{remote_toq_id}",
        },
        "mutation_nonvacuity_path": f"inputs/{mutation_record_id}",
        "sign_off": {
            "submitted_by": {
                "name": "Submitter A",
                "organization": "Engineering A",
                "authenticated_identity": "oidc:evidence-producer",
            },
            "reviewed_by": {
                "name": "Reviewer A",
                "organization": "Assurance B",
                "authenticated_identity": reviewer_identity,
            },
            "review_log_path": "inputs/independent-review.log",
        },
        "producer": {
            "signer_identity": "oidc:evidence-producer",
            "decided_at": event_text,
            "signed_at": event_text,
            "native_execution_record_path": None,
            "native_execution_signature_path": None,
        },
    }
    collection_manifest_id, collection_manifest_hash = add_artifact(
        "collection-manifest.json",
        "collection-manifest",
        (json.dumps(collection_manifest, indent=2, sort_keys=True) + "\n").encode(),
    )

    binding_roles: dict[str, tuple[str, int]] = {
        "collection-manifest": (
            collection_manifest_hash,
            artifact_files[collection_manifest_id].stat().st_size,
        ),
        "producer-pinned-public-key": (digest(producer_public), producer_public.stat().st_size),
        "reviewer-pinned-public-key": (digest(reviewer_public), reviewer_public.stat().st_size),
        "model:formal-closure": (formal_spec_sha, model_path.stat().st_size),
        "config:complete-config": (config_sha, config_path.stat().st_size),
        "mutation-nonvacuity": (
            mutation_record_hash,
            artifact_files[mutation_record_id].stat().st_size,
        ),
        "tlc-log:complete-config": (
            digest(artifact_files[tlc_log_id]),
            artifact_files[tlc_log_id].stat().st_size,
        ),
        "trace-export": (trace_hash, artifact_files[trace_id].stat().st_size),
        "trace-replay": (replay_hash, artifact_files[replay_id].stat().st_size),
        "scope-enforcement-source": (
            digest(artifact_files[scope_id]),
            artifact_files[scope_id].stat().st_size,
        ),
        "toq-clock-discipline": (toq_hash, artifact_files[toq_id].stat().st_size),
        "toq-clock-observation": (
            remote_toq_hash,
            artifact_files[remote_toq_id].stat().st_size,
        ),
    }
    for tool_id in verifier.TOOL_IDS:
        binding_roles[f"tool-pin:{tool_id}"] = (
            digest(pin_paths[tool_id]),
            pin_paths[tool_id].stat().st_size,
        )
        binding_roles[f"tool-binary:{tool_id}"] = (
            binary_hashes[tool_id],
            artifact_files[binary_ids[tool_id]].stat().st_size,
        )
    for proof in proofs:
        artifact_id = proof["tlaps_log_artifact_id"]
        binding_roles[f"tlaps-log:{proof['theorem_class']}"] = (
            digest(artifact_files[artifact_id]),
            artifact_files[artifact_id].stat().st_size,
        )
    for subject in mutation_subjects:
        subject_id = subject["subject_id"]
        for suffix, field in (
            ("baseline", "baseline_artifact_id"),
            ("mutant", "mutant_artifact_id"),
            ("detector-output", "detector_artifact_id"),
        ):
            artifact_id = subject[field]
            binding_roles[f"mutation:{subject_id}:{suffix}"] = (
                digest(artifact_files[artifact_id]),
                artifact_files[artifact_id].stat().st_size,
            )
        witness_id = subject["witness_artifact_id"]
        binding_roles[f"nonvacuity:{subject_id}:witness"] = (
            digest(artifact_files[witness_id]),
            artifact_files[witness_id].stat().st_size,
        )
    reviewed_inputs = [
        {"role": role, "sha256": value[0], "size_bytes": value[1]}
        for role, value in sorted(binding_roles.items())
    ]
    reviewed_digest = verifier.canonical_reviewed_inputs_sha256(reviewed_inputs)

    review_result = {
        "authenticated_identity": reviewer_identity,
        "decision": "approved",
        "independent": True,
        "reviewed_at": event_text,
        "source_revision": SOURCE_REVISION,
    }
    review_binding = {
        "release_id": RELEASE_ID,
        "source_revision": SOURCE_REVISION,
        "source_tree_id": source_tree_id,
        "target_id": target_id,
        "formal_spec_sha256": formal_spec_sha,
        "mutation_nonvacuity_sha256": mutation_record_hash,
        "reviewed_input_bindings_sha256": reviewed_digest,
        "reviewed_at": event_text,
        "decision": "approved",
    }
    review_id, review_hash = add_artifact(
        "independent-review.log",
        "independent-review",
        (
            "Review raw output\nFORMAL_CLOSURE_REVIEW_RESULT "
            + compact(review_result)
            + "\nFORMAL_CLOSURE_REVIEW_BINDING "
            + compact(review_binding)
            + "\n"
        ).encode(),
    )
    command_manifest_hash = verifier.canonical_command_manifest_sha256(
        finite_checks,
        proofs,
        trace_refinement,
    )
    toolchain_manifest_hash = verifier.canonical_toolchain_manifest_sha256(toolchain)
    execution_input_manifest_hash = hashlib.sha256(
        verifier.canonical_json_bytes(
            {
                "properties": properties,
                "specification_manifest": specification_manifest,
                "target": target,
            }
        )
    ).hexdigest()
    signoff: dict[str, Any] = {
        "submitted_by": {
            "name": "Submitter A",
            "organization": "Engineering A",
            "authenticated_identity": "oidc:evidence-producer",
        },
        "reviewed_by": {
            "name": "Reviewer A",
            "organization": "Assurance B",
            "authenticated_identity": reviewer_identity,
        },
        "independent": True,
        "decision": "approved",
        "reviewed_at": event_text,
        "artifact_id": review_id,
        "artifact_sha256": review_hash,
        "reviewed_input_bindings_sha256": reviewed_digest,
        "mutation_evidence_sha256": mutation_record_hash,
        "reviewer_trust_root_path": verifier.REVIEWER_TRUST_ROOT_PATH,
        "reviewer_trust_root_sha256": digest(reviewer_public),
        "signed_payload_sha256": "0" * 64,
        "signature_artifact_id": "reviewer-signature.bin",
        "signature_algorithm": "rsa-sha256",
    }
    document: dict[str, Any] = {
        "schema_version": verifier.SCHEMA_VERSION,
        "record_kind": verifier.RECORD_KIND,
        "record_mode": verifier.SYNTHETIC_MODE,
        "claim": verifier.NON_CLAIM,
        "release": {
            "release_id": RELEASE_ID,
            "source_repository": "https://gosuda.org/moreconsensus/source",
            "source_revision": SOURCE_REVISION,
            "source_tree": "clean",
            "formal_spec_sha256": formal_spec_sha,
        },
        "target": target,
        "generated_at": generated_text,
        "valid_until": timestamp(generated_at + timedelta(days=1)),
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
            "declared_scope": "Joint consensus is excluded from the declared release scope.",
            "release_scope_enforced": True,
            "proof_theorem_id": None,
            "scope_artifact_id": scope_id,
            "collected_at": event_text,
        },
        "toq_clock_discipline": {
            "in_claim": True,
            "target_environment": verifier.SYNTHETIC_MODE,
            "result": "pass",
            "clock_source": "authenticated monotonic source",
            "maximum_clock_error_ms": 25,
            "collected_at": event_text,
            "evidence_uri": toq_uri,
            "remote_evidence_sha256": remote_toq_hash,
            "artifact_id": toq_id,
            "artifact_sha256": toq_hash,
            "remote_artifact_id": remote_toq_id,
            "remote_artifact_sha256": remote_toq_hash,
            "source_revision": SOURCE_REVISION,
        },
        "native_execution": {
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
        },
        "reviewed_input_set": {
            "digest_algorithm": "sha256-canonical-json-v1",
            "manifest_artifact_id": collection_manifest_id,
            "manifest_sha256": collection_manifest_hash,
            "reviewed_input_bindings_sha256": reviewed_digest,
            "inputs": reviewed_inputs,
        },
        "production_attestation": {
            "signer_identity": "oidc:evidence-producer",
            "decision": "approved",
            "decided_at": event_text,
            "trust_root_path": verifier.PRODUCER_TRUST_ROOT_PATH,
            "trust_root_sha256": digest(producer_public),
            "signature_algorithm": "rsa-sha256",
            "signed_payload_sha256": "0" * 64,
            "signature_artifact_id": "producer-signature.bin",
            "signed_at": event_text,
        },
        "sign_off": signoff,
        "raw_artifacts": raw_artifacts,
    }
    signature_records: dict[str, dict[str, Any]] = {}
    for artifact_id, kind, private_key in (
        ("reviewer-signature.bin", "reviewer-signature", reviewer_private),
        ("producer-signature.bin", "producer-signature", producer_private),
    ):
        signature_size = len(openssl(["dgst", "-sha256", "-sign", str(private_key)], payload=b""))
        record = {
            "artifact_id": artifact_id,
            "kind": kind,
            "path": f"artifacts/{artifact_id}",
            "sha256": "0" * 64,
            "size_bytes": signature_size,
            "source_revision": SOURCE_REVISION,
            "created_at": event_text,
        }
        raw_artifacts.append(record)
        signature_records[artifact_id] = record
        artifact_files[artifact_id] = artifacts_dir / artifact_id

    attestation_payload = verifier.evidence_attestation_payload(document)
    attestation_hash = hashlib.sha256(attestation_payload).hexdigest()
    document["production_attestation"]["signed_payload_sha256"] = attestation_hash
    document["sign_off"]["signed_payload_sha256"] = attestation_hash
    reviewer_signature = openssl(
        ["dgst", "-sha256", "-sign", str(reviewer_private)],
        payload=attestation_payload,
    )
    producer_signature = openssl(
        ["dgst", "-sha256", "-sign", str(producer_private)],
        payload=attestation_payload,
    )
    for artifact_id, content in (
        ("reviewer-signature.bin", reviewer_signature),
        ("producer-signature.bin", producer_signature),
    ):
        record = signature_records[artifact_id]
        if len(content) != record["size_bytes"]:
            raise AssertionError("RSA signature size changed after payload binding")
        artifact_files[artifact_id].write_bytes(content)
        record["sha256"] = hashlib.sha256(content).hexdigest()
    if verifier.evidence_attestation_payload(document) != attestation_payload:
        raise AssertionError("signature artifact hashes must be the only attestation-payload exclusions")
    evidence_path = bundle / "formal-closure-evidence.json"
    evidence_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return document, evidence_path, repository


def validation_kwargs(document: dict[str, Any], evidence_path: Path, repository: Path, now: datetime) -> dict[str, Any]:
    return {
        "evidence_path": evidence_path,
        "repository_root": repository,
        "expected_release_id": RELEASE_ID,
        "expected_source_revision": SOURCE_REVISION,
        "expected_formal_spec_sha256": document["release"]["formal_spec_sha256"],
        "expected_target_id": document["target"]["target_id"],
        "expected_environment_profile": document["target"]["environment"],
        "allow_synthetic_test_fixture": True,
        "now": now,
    }


def expect_invalid(
    base: dict[str, Any],
    evidence_path: Path,
    repository: Path,
    now: datetime,
    mutate: Callable[[dict[str, Any]], None],
    expected: str,
    *,
    overrides: dict[str, Any] | None = None,
) -> None:
    document = copy.deepcopy(base)
    mutate(document)
    kwargs = validation_kwargs(document, evidence_path, repository, now)
    if overrides:
        kwargs.update(overrides)
    try:
        verifier.validate_document(document, **kwargs)
    except verifier.EvidenceError as exc:
        if expected not in str(exc):
            raise AssertionError(f"expected {expected!r}, got {str(exc)!r}") from exc
    else:
        raise AssertionError(f"mutation unexpectedly passed; expected {expected!r}")


def main() -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    cases = 0
    with tempfile.TemporaryDirectory(prefix="formal-closure-selftest-") as temporary:
        base, evidence_path, repository = build_fixture(Path(temporary), now)
        verifier.validate_schema_contract()
        cases += 1

        result = verifier.validate_document(base, **validation_kwargs(base, evidence_path, repository, now))
        if result["mode"] != verifier.SYNTHETIC_MODE or result["claim"] != verifier.NON_CLAIM:
            raise AssertionError("accepted fixture did not remain an explicit synthetic non-claim")
        cases += 1

        cli = [
            sys.executable,
            str(VERIFIER_PATH),
            "--expected-release-id",
            RELEASE_ID,
            "--expected-source-revision",
            SOURCE_REVISION,
            "--expected-formal-spec-sha256",
            base["release"]["formal_spec_sha256"],
            "--expected-target-id",
            base["target"]["target_id"],
            "--expected-environment-profile",
            base["target"]["environment"],
            "--allow-synthetic-test-fixture",
            "--repository-root",
            str(repository),
            str(evidence_path),
        ]
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        completed = subprocess.run(cli, text=True, capture_output=True, env=environment, check=False)
        if completed.returncode != 0 or not completed.stdout.startswith(
            "formal_closure_evidence=verified mode=synthetic-test"
        ) or "claim=none" not in completed.stdout:
            raise AssertionError(f"synthetic CLI acceptance failed: stdout={completed.stdout!r} stderr={completed.stderr!r}")
        cases += 1

        no_flag_cli = [part for part in cli if part not in {"--allow-synthetic-test-fixture", "--repository-root", str(repository)}]
        completed = subprocess.run(no_flag_cli, text=True, capture_output=True, env=environment, check=False)
        if completed.returncode == 0 or "requires --allow-synthetic-test-fixture" not in completed.stderr:
            raise AssertionError("synthetic fixture passed the production CLI")
        cases += 1
        target_with_test_options = copy.deepcopy(base)
        target_with_test_options["record_mode"] = "target"
        target_override_path = evidence_path.parent / "target-with-test-options.json"
        target_override_path.write_text(
            json.dumps(target_with_test_options, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        completed = subprocess.run(
            cli[:-1] + [str(target_override_path)],
            text=True,
            capture_output=True,
            env=environment,
            check=False,
        )
        if completed.returncode == 0 or "test-only verifier options" not in completed.stderr:
            raise AssertionError("target record accepted test-only verifier options")
        cases += 1

        extra_unreviewed_path = evidence_path.parent / "artifacts" / "extra-unreviewed.log"
        extra_unreviewed_path.write_text("extra unreviewed artifact\n", encoding="utf-8")

        def add_extra_unreviewed_artifact(document: dict[str, Any]) -> None:
            document["raw_artifacts"].append(
                {
                    "artifact_id": "extra-unreviewed-artifact",
                    "kind": "scope-enforcement",
                    "path": "artifacts/extra-unreviewed.log",
                    "sha256": digest(extra_unreviewed_path),
                    "size_bytes": extra_unreviewed_path.stat().st_size,
                    "source_revision": SOURCE_REVISION,
                    "created_at": document["production_attestation"]["decided_at"],
                }
            )

        negative_cases: list[tuple[str, Callable[[dict[str, Any]], None], str, dict[str, Any] | None]] = [
            ("production-nonclaim", lambda d: d.update({"record_mode": "target"}), "$.claim must be", None),
            ("finite-tlc-only", lambda d: (d.update({"inductive_proofs": []}), d["evidence_basis"].update({"inductive_status": "absent", "closure_basis": "finite-only", "finite_only": True, "unchecked_proof_count": 5})), "at least 5", None),
            ("unchecked-proof", lambda d: d["inductive_proofs"][0].update({"status": "unchecked"}), ".status must be 'checked'", None),
            ("unproved-obligation", lambda d: d["inductive_proofs"][0].update({"obligations_proved": 6}), "unchecked proof obligations", None),
            ("admitted-proof", lambda d: d["inductive_proofs"][0].update({"admitted_obligations": 1}), "admitted_obligations must be 0", None),
            ("missing-invariant", lambda d: d["properties"]["invariants"].pop(), "at least 33", None),
            ("duplicate-invariant", lambda d: d["properties"]["invariants"].__setitem__(-1, copy.deepcopy(d["properties"]["invariants"][0])), "duplicate property", None),
            ("missing-temporal", lambda d: d["properties"]["temporal_properties"].pop(), "at least 5", None),
            ("missing-finite-area", lambda d: d["finite_model_checks"][0]["area_ids"].pop(), "do not match the TLC raw log", None),
            ("duplicate-finite-area", lambda d: d["finite_model_checks"][0]["area_ids"].append(d["finite_model_checks"][0]["area_ids"][0]), "duplicate values", None),
            ("missing-config-check", lambda d: d.update({"finite_model_checks": []}), "at least 1", None),
            ("generated-states", lambda d: d["finite_model_checks"][0].update({"generated_states": 0}), "generated_states must be an integer >= 1", None),
            ("distinct-states", lambda d: d["finite_model_checks"][0].update({"distinct_states": 101}), "must not exceed", None),
            ("search-depth", lambda d: d["finite_model_checks"][0].update({"search_depth": 0}), "search_depth must be an integer >= 1", None),
            ("nonzero-queue", lambda d: d["finite_model_checks"][0].update({"queue_states": 1}), "queue_states must be 0", None),
            ("tlc-errors", lambda d: d["finite_model_checks"][0].update({"errors": 1}), "errors must be 0", None),
            ("tlc-result", lambda d: d["finite_model_checks"][0].update({"result": "fail"}), "result must be 'pass'", None),
            ("source-argument-mismatch", lambda d: None, "does not match --expected-source-revision", {"expected_source_revision": "0" * 64}),
            ("embedded-source-mismatch", lambda d: d["raw_artifacts"][0].update({"source_revision": "0" * 64}), "does not match the release", None),
            ("formal-spec-argument-mismatch", lambda d: None, "does not match --expected-formal-spec-sha256", {"expected_formal_spec_sha256": "0" * 64}),
            ("model-hash-mismatch", lambda d: d["specification_manifest"]["models"][0].update({"sha256": "0" * 64}), "does not match repository model content", None),
            ("config-hash-mismatch", lambda d: d["specification_manifest"]["configs"][0].update({"sha256": "0" * 64}), "does not match repository config content", None),
            ("manifest-hash-mismatch", lambda d: d["specification_manifest"].update({"manifest_sha256": "0" * 64}), "does not match the exact manifest", None),
            ("tool-pin-version", lambda d: d["toolchain"][0].update({"version": "99"}), "repository tool pin.version", None),
            ("tool-pin-hash", lambda d: d["toolchain"][0].update({"repository_pin_sha256": "0" * 64}), "does not match the repository pin", None),
            ("tool-binary-hash", lambda d: d["toolchain"][0].update({"binary_sha256": "0" * 64}), "repository tool pin.binary_sha256", None),
            ("tool-artifact-content-hash", lambda d: d["raw_artifacts"][0].update({"sha256": "0" * 64}), "does not match file content", None),
            ("missing-tlc-log", lambda d: d["finite_model_checks"][0].update({"tlc_log_artifact_id": "absent-tlc-log"}), "unknown raw artifact", None),
            ("missing-tlaps-log", lambda d: d["inductive_proofs"][0].update({"tlaps_log_artifact_id": "absent-tlaps-log"}), "unknown raw artifact", None),
            ("tlc-log-binding", lambda d: d["finite_model_checks"][0].update({"generated_states": 101}), "do not match the TLC raw log", None),
            ("tlaps-log-binding", lambda d: d["inductive_proofs"][0].update({"obligations_total": 8, "obligations_proved": 8}), "do not match the TLAPS raw log", None),
            ("nondeterministic-trace", lambda d: d["trace_refinement"].update({"deterministic": False}), "deterministic must be True", None),
            ("trace-export-fail", lambda d: d["trace_refinement"].update({"export_result": "fail"}), "export_result must be 'pass'", None),
            ("trace-replay-fail", lambda d: d["trace_refinement"].update({"replay_result": "fail"}), "replay_result must be 'pass'", None),
            ("trace-hash-mismatch", lambda d: d["trace_refinement"].update({"trace_sha256": "0" * 64}), "artifact hash does not match", None),
            ("joint-status", lambda d: d["joint_consensus"].update({"status": "unknown"}), "status must be", None),
            ("joint-scope-unenforced", lambda d: d["joint_consensus"].update({"release_scope_enforced": False}), "release_scope_enforced must be True", None),
            ("joint-exclusion-opaque", lambda d: d["joint_consensus"].update({"declared_scope": "restricted protocol scope"}), "must be explicit", None),
            ("toq-not-in-claim", lambda d: d["toq_clock_discipline"].update({"in_claim": False}), "in_claim must be True", None),
            ("toq-local-reference", lambda d: d["toq_clock_discipline"].update({"evidence_uri": "https://localhost/clock"}), "placeholder or non-claim marker", None),
            ("toq-mutable-reference", lambda d: d["toq_clock_discipline"].update({"evidence_uri": "https://evidence.gosuda.org/formal/clock/current"}), "must end in /sha256/", None),
            ("toq-hash-mismatch", lambda d: d["toq_clock_discipline"].update({"artifact_sha256": "0" * 64}), "artifact hash does not match", None),
            ("dependent-reviewer", lambda d: d["sign_off"].update({"reviewed_by": copy.deepcopy(d["sign_off"]["submitted_by"])}), "must be independent", None),
            ("same-authenticated-reviewer", lambda d: d["sign_off"]["reviewed_by"].update({"authenticated_identity": d["sign_off"]["submitted_by"]["authenticated_identity"]}), "authenticated identity must be independent", None),
            ("review-not-independent", lambda d: d["sign_off"].update({"independent": False}), "independent must be True", None),
            ("review-not-approved", lambda d: d["sign_off"].update({"decision": "rejected"}), "decision must be 'approved'", None),
            ("review-hash-mismatch", lambda d: d["sign_off"].update({"artifact_sha256": "0" * 64}), "artifact hash does not match", None),
            ("review-payload-binding", lambda d: d["sign_off"]["reviewed_by"].update({"name": "Reviewer B"}), "collection manifest sign-off identities do not match", None),
            ("review-trust-independence", lambda d: d["sign_off"].update({"reviewer_trust_root_sha256": d["production_attestation"]["trust_root_sha256"]}), "reviewer trust root must be independent", None),
            ("producer-trust-hash", lambda d: d["production_attestation"].update({"trust_root_sha256": "0" * 64}), "does not match the repository trust root", None),
            ("producer-payload-binding", lambda d: d["production_attestation"].update({"signed_payload_sha256": "0" * 64}), "signed_payload_sha256 does not match", None),
            ("refreshed-artifact-metadata", lambda d: d["raw_artifacts"][0].update({"created_at": d["generated_at"]}), "was created after an approval decision", None),
            ("stale-evidence", lambda d: d.update({"generated_at": timestamp(now - timedelta(days=31))}), "evidence is stale", None),
            ("expired-evidence", lambda d: d.update({"valid_until": timestamp(now - timedelta(seconds=1))}), "has expired", None),
            ("future-evidence", lambda d: d.update({"generated_at": timestamp(now + timedelta(minutes=6)), "valid_until": timestamp(now + timedelta(days=1))}), "too far in the future", None),
            ("placeholder", lambda d: d["sign_off"]["reviewed_by"].update({"name": "Placeholder Reviewer"}), "placeholder or non-claim marker", None),
            ("unknown-field", lambda d: d.update({"opaque_evidence": "value"}), "keys mismatch", None),
            ("command-tool-mismatch", lambda d: d["finite_model_checks"][0].update({"command_sha256": "0" * 64}), "command_sha256 must be", None),
            ("same-label-different-tree", lambda d: d["target"].update({"source_tree_id": "0" * 64}), "command or tool binding does not match", None),
            ("omitted-reviewed-input", lambda d: d["reviewed_input_set"]["inputs"].pop(), "canonical input set", None),
            ("absent-trace-time", lambda d: d["trace_refinement"].pop("replayed_at"), "keys mismatch", None),
            ("stale-trace-time", lambda d: d["trace_refinement"].update({"replayed_at": "2020-01-01T00:00:00Z"}), "chronology is inconsistent or stale", None),
            ("clock-source-mismatch", lambda d: d["toq_clock_discipline"].update({"clock_source": "different authenticated clock"}), "clock_source must be 'authenticated monotonic source'", None),
            ("review-before-execution", lambda d: d["sign_off"].update({"reviewed_at": "2020-01-01T00:00:00Z"}), "chronology is inconsistent or stale", None),
            ("opaque-mutation-summary", lambda d: d["mutation_evidence"].update({"subjects": [], "opaque_summary": "all mutations rejected"}), "keys mismatch", None),
            ("old-target-version", lambda d: d.update({"schema_version": "1.0.0", "record_mode": "target", "claim": verifier.TARGET_CLAIM}), "$.schema_version must be '3.0.0'", None),
            ("extra-unreviewed-artifact", add_extra_unreviewed_artifact, "unreferenced raw artifact", None),
        ]

        for name, mutate, expected, overrides in negative_cases:
            try:
                expect_invalid(base, evidence_path, repository, now, mutate, expected, overrides=overrides)
            except Exception as exc:
                raise AssertionError(f"negative case {name!r} failed: {exc}") from exc
            cases += 1
        oversized_path = evidence_path.parent / "artifacts" / "oversized.log"
        with oversized_path.open("wb") as handle:
            handle.truncate(verifier.MAX_ARTIFACT_BYTES + 1)

        def add_oversized_artifact(document: dict[str, Any]) -> None:
            document["raw_artifacts"].append(
                {
                    **copy.deepcopy(document["raw_artifacts"][0]),
                    "artifact_id": "oversized-log",
                    "kind": "tlc-raw-log",
                    "path": "artifacts/oversized.log",
                    "sha256": "0" * 64,
                    "size_bytes": verifier.MAX_ARTIFACT_BYTES + 1,
                }
            )

        expect_invalid(
            base,
            evidence_path,
            repository,
            now,
            add_oversized_artifact,
            "artifact limit",
        )
        cases += 1

        producer_payload = verifier.evidence_attestation_payload(base)
        try:
            verifier.verify_rsa_sha256_signature(
                producer_payload,
                repository / verifier.REVIEWER_TRUST_ROOT_PATH,
                evidence_path.parent / "artifacts" / "producer-signature.bin",
                "wrong-key self-test",
            )
        except verifier.EvidenceError as exc:
            if "signature verification failed" not in str(exc):
                raise
        else:
            raise AssertionError("producer signature passed with the independent review key")
        cases += 1

        try:
            verifier.require_log_text(
                evidence_path.parent / "artifacts" / "complete-config.tlc.log",
                ("Model checking completed. No error has been found.",),
                "native TLC self-test",
            )
        except verifier.EvidenceError as exc:
            if "lacks required native tool output" not in str(exc):
                raise
        else:
            raise AssertionError("marker-only synthetic TLC log passed native target log validation")
        cases += 1

        incomplete_config = repository / "specs" / "Incomplete.cfg"
        incomplete_config.write_text("SPECIFICATION Spec\nCHECK_DEADLOCK TRUE\n", encoding="utf-8")
        try:
            verifier.require_config_declarations(
                incomplete_config,
                (verifier.INVARIANT_IDS[0],),
                (),
                "incomplete config self-test",
            )
        except verifier.EvidenceError as exc:
            if "active INVARIANT" not in str(exc):
                raise
        else:
            raise AssertionError("config without a required invariant declaration passed")
        cases += 1
        incomplete_model = repository / "specs" / "Incomplete.tla"
        incomplete_model.write_text("---- MODULE Incomplete ----\n====\n", encoding="utf-8")
        try:
            verifier.require_theorem_declaration(
                incomplete_model,
                verifier.THEOREM_BY_CLASS["arbitrary-history-safety"],
                "incomplete theorem self-test",
            )
        except verifier.EvidenceError as exc:
            if "does not declare THEOREM" not in str(exc):
                raise
        else:
            raise AssertionError("model without a required theorem declaration passed")
        cases += 1

        opaque_trace = evidence_path.parent / "opaque-trace.json"
        opaque_trace.write_text('{"events":[]}\n', encoding="utf-8")
        try:
            verifier.validate_trace_export(
                opaque_trace,
                release_id=RELEASE_ID,
                source_revision=SOURCE_REVISION,
                source_tree_id=base["target"]["source_tree_id"],
                target_id=base["target"]["target_id"],
                formal_spec_sha256=base["release"]["formal_spec_sha256"],
                exported_at=base["trace_refinement"]["exported_at"],
                export_command_sha256=base["trace_refinement"]["export_command_sha256"],
                go_binary_sha256=next(
                    tool["binary_sha256"] for tool in base["toolchain"] if tool["tool_id"] == "go"
                ),
                trace_count=1,
            )
        except verifier.EvidenceError as exc:
            if "keys mismatch" not in str(exc):
                raise
        else:
            raise AssertionError("opaque shaped-spec trace export passed")
        cases += 1

        try:
            verifier.verify_repository_revision(repository, SOURCE_REVISION)
        except verifier.EvidenceError as exc:
            if "target repository HEAD" not in str(exc):
                raise
        else:
            raise AssertionError("non-git synthetic repository passed target revision verification")
        cases += 1
        provenance_repository = Path(temporary) / "provenance-repository"
        provenance_repository.mkdir()

        def local_git(*args: str) -> str:
            git_result = subprocess.run(
                ["git", "-C", str(provenance_repository), *args],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            if git_result.returncode != 0:
                raise AssertionError(f"local git self-test failed: {git_result.stderr}")
            return git_result.stdout.strip()

        local_git("init", "-q")
        local_git("config", "user.name", "Formal Self Test")
        local_git("config", "user.email", "formal-self-test@invalid.test")
        (provenance_repository / "source.txt").write_text("source\n", encoding="utf-8")
        local_git("add", "source.txt")
        local_git("commit", "-q", "-m", "source")
        provenance_source_revision = local_git("rev-parse", "HEAD")
        (provenance_repository / "RELEASE_SCOPE.md").write_text("decision\n", encoding="utf-8")
        local_git("add", "RELEASE_SCOPE.md")
        local_git("commit", "-q", "-m", "decision")
        verifier.verify_repository_revision(provenance_repository, provenance_source_revision)
        cases += 1

        (provenance_repository / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")
        local_git("add", "unexpected.txt")
        local_git("commit", "-q", "-m", "unexpected")
        try:
            verifier.verify_repository_revision(provenance_repository, provenance_source_revision)
        except verifier.EvidenceError as exc:
            if "changed non-decision path" not in str(exc):
                raise
        else:
            raise AssertionError("non-decision source change passed ancestor provenance validation")
        cases += 1

        duplicate_path = evidence_path.parent / "duplicate-key.json"
        raw = evidence_path.read_text(encoding="utf-8")
        duplicate_path.write_text(raw.replace('  "claim": "none",', '  "claim": "none",\n  "claim": "none",', 1), encoding="utf-8")
        duplicate_cli = cli[:-1] + [str(duplicate_path)]
        completed = subprocess.run(duplicate_cli, text=True, capture_output=True, env=environment, check=False)
        if completed.returncode == 0 or "duplicate JSON key" not in completed.stderr:
            raise AssertionError("duplicate JSON key was not rejected")
        cases += 1

    print(f"formal closure evidence self-test: {cases} cases passed (1 synthetic non-claim fixture; 0 production claims)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
