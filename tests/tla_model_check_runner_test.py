import asyncio
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

from tests.tla_model_check_runner import FAST_PROFILE, Job, run_job, run_profile, select_profile


FAKE_JAVA = '''#!/usr/bin/env python3
import json
import os
import pathlib
import sys
import time

capture = pathlib.Path(os.environ["FAKE_JAVA_CAPTURE"])
capture.write_text(json.dumps(sys.argv[1:]), encoding="utf-8")
args = sys.argv[1:]
metadir = pathlib.Path(args[args.index("-metadir") + 1])
metadir.joinpath("fake-state").write_text("owned", encoding="utf-8")
mode = os.environ.get("FAKE_JAVA_MODE", "success")
if mode == "sleep":
    time.sleep(10)
elif mode == "nonzero":
    print("synthetic Java failure", flush=True)
    raise SystemExit(7)
elif mode == "deadlock":
    print("Error: Deadlock reached.", flush=True)
elif mode == "invariant":
    print("Error: Invariant InvariantSafety is violated.", flush=True)
else:
    print("Model checking completed. No error has been found.", flush=True)
'''


class TLAModelCheckRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.java = self.root / "fake-java"
        self.java.write_text(FAKE_JAVA, encoding="utf-8")
        self.java.chmod(0o755)
        self.jar = self.root / "tools.jar"
        self.jar.write_bytes(b"fake")
        self.capture = self.root / "arguments.json"
        self.environment = mock.patch.dict(os.environ, {"FAKE_JAVA_CAPTURE": str(self.capture)}, clear=False)
        self.environment.start()

    def tearDown(self):
        self.environment.stop()
        self.temp.cleanup()

    def run_one(self, mode="success", timeout=2):
        with mock.patch.dict(os.environ, {"FAKE_JAVA_MODE": mode}, clear=False):
            return asyncio.run(run_job(Job("tla/Module.tla", "tla/Config.cfg"), self.root, str(self.java), self.jar, timeout, asyncio.Semaphore(1)))

    def test_fast_profile_is_exact_and_full_is_nonempty(self):
        fast = select_profile("fast")
        self.assertEqual(len(fast), 23)
        self.assertEqual(tuple((job.module, job.config) for job in fast), FAST_PROFILE)
        self.assertGreater(len(select_profile("full")), len(fast))
        with self.assertRaises(ValueError):
            select_profile("empty")

    def test_argument_construction_success_and_metadir_cleanup(self):
        self.assertIsNone(self.run_one())
        arguments = json.loads(self.capture.read_text(encoding="utf-8"))
        self.assertEqual(arguments[:3], ["-cp", str(self.jar), "tlc2.TLC"])
        self.assertEqual(arguments[-3:], ["-config", "tla/Config.cfg", "tla/Module.tla"])
        metadir = pathlib.Path(arguments[arguments.index("-metadir") + 1])
        self.assertFalse(metadir.exists())

    def test_timeout_and_nonzero_propagate_and_cleanup(self):
        timeout_error = self.run_one("sleep", timeout=1.0)
        self.assertIn("timed out", timeout_error)
        timeout_arguments = json.loads(self.capture.read_text(encoding="utf-8"))
        self.assertFalse(pathlib.Path(timeout_arguments[timeout_arguments.index("-metadir") + 1]).exists())
        nonzero_error = self.run_one("nonzero")
        self.assertIn("exited 7", nonzero_error)

    def test_deadlock_and_invariant_outputs_fail_closed(self):
        for mode in ("deadlock", "invariant"):
            with self.subTest(mode=mode):
                self.assertIn("deadlock or invariant failure", self.run_one(mode))

    def test_overall_timeout_cancels_jobs_and_cleans_metadirs(self):
        with mock.patch.dict(os.environ, {"FAKE_JAVA_MODE": "sleep"}, clear=False):
            errors = asyncio.run(run_profile((Job("a.tla", "a.cfg"),), self.root, str(self.java), self.jar, 1, 0, 1.0))
        self.assertIn("overall TLC deadline exceeded", errors[0])
        arguments = json.loads(self.capture.read_text(encoding="utf-8"))
        self.assertFalse(pathlib.Path(arguments[arguments.index("-metadir") + 1]).exists())


if __name__ == "__main__":
    unittest.main()
