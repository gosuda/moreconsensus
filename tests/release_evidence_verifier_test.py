#!/usr/bin/env python3
"""Tests for the single authoritative formal release row."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


class ReleaseEvidenceVerifierTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = Path(__file__).resolve().parent.parent
        cls.verifier = cls.repo_root / "tests" / "release_evidence_verifier.py"

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="release-evidence-scope-")
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_scope(self, rows: list[tuple[str, str]], decision: str) -> Path:
        lines = [
            "# Synthetic release scope",
            "",
            "## Current release decision",
            "",
            decision,
            "",
            "## Open release items",
            "",
            "| Item | Current state |",
            "| --- | --- |",
        ]
        lines.extend(f"| {label} | {state} |" for label, state in rows)
        path = self.root / "RELEASE_SCOPE.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def run_verifier(self, scope: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "python3",
                str(self.verifier),
                "--repository-root",
                str(self.root),
                "--scope",
                str(scope),
            ],
            cwd=self.repo_root,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_single_open_formal_row_is_no_go_without_external_evidence(self) -> None:
        scope = self.write_scope(
            [("Broader formal model coverage", "Finite evidence only.")], "No-go."
        )
        result = self.run_verifier(scope)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("release_decision=No-go.", result.stdout)
        self.assertIn("open_release_items=1", result.stdout)
        self.assertIn("open_release_item_ids=broader-formal", result.stdout)
        self.assertIn("closed_release_item_ids=none", result.stdout)

    def test_removed_release_labels_are_rejected(self) -> None:
        scope = self.write_scope([("Obsolete release row", "Removed.")], "No-go.")
        result = self.run_verifier(scope)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unknown authoritative open release item", result.stderr)


    def test_go_without_formal_closure_evidence_fails_closed(self) -> None:
        scope = self.write_scope([], "Go.")
        result = self.run_verifier(scope)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("explicit external immutable evidence root is required", result.stderr)

    def test_no_go_is_required_when_formal_row_is_open(self) -> None:
        scope = self.write_scope(
            [("Broader formal model coverage", "Still open.")], "Go."
        )
        result = self.run_verifier(scope)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("release decision must be No-go", result.stderr)

    def test_duplicate_formal_rows_fail(self) -> None:
        scope = self.write_scope(
            [
                ("Broader formal model coverage", "Still open."),
                ("Broader formal model coverage", "Still open twice."),
            ],
            "No-go.",
        )
        result = self.run_verifier(scope)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate authoritative open release item", result.stderr)


if __name__ == "__main__":
    unittest.main()
