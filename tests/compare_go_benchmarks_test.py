import pathlib
import tempfile
import unittest

from tests.compare_go_benchmarks import BenchmarkError, compare, parse


def output(rows, samples=10):
    lines = ["goos: darwin", "goarch: arm64"]
    for name, ns, bytes_per_op, allocs, resident in rows:
        for _ in range(samples):
            metrics = f"{ns} ns/op {bytes_per_op} B/op {allocs} allocs/op"
            if resident is not None:
                metrics += f" {resident} resident-B/instance"
            lines.append(f"{name} 100 {metrics}")
    return "\n".join(lines) + "\n"


class CompareGoBenchmarksTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.before = self.root / "before.bench"
        self.after = self.root / "after.bench"
        self.rows = [("BenchmarkReadyRetry/voters=3-8", 100, 64, 1, None)]

    def tearDown(self):
        self.temp.cleanup()

    def write(self, before=None, after=None):
        self.before.write_text(output(before or self.rows), encoding="utf-8")
        self.after.write_text(output(after or self.rows), encoding="utf-8")

    def test_equal_and_five_percent_boundary_pass(self):
        self.write(after=[("BenchmarkReadyRetry/voters=3-8", 105, 64, 1, None)])
        self.assertEqual(compare(self.before, self.after), [])

    def test_time_over_five_percent_fails(self):
        self.write(after=[("BenchmarkReadyRetry/voters=3-8", 105.01, 64, 1, None)])
        self.assertIn("median ns/op increased", compare(self.before, self.after)[0])

    def test_allocation_byte_and_resident_regressions_fail(self):
        rows = [("BenchmarkLiveInstanceRetention/voters=3-8", 100, 64, 1, 700)]
        self.write(rows, [("BenchmarkLiveInstanceRetention/voters=3-8", 100, 65, 2, 701)])
        errors = compare(self.before, self.after)
        self.assertEqual(len(errors), 3)

    def test_missing_and_extra_names_fail(self):
        self.write(after=[("BenchmarkDifferent-8", 100, 64, 1, None)])
        errors = compare(self.before, self.after)
        self.assertTrue(any("missing benchmark" in error for error in errors))
        self.assertTrue(any("extra benchmark" in error for error in errors))

    def test_changed_unit_fails(self):
        self.write()
        text = self.after.read_text(encoding="utf-8").replace("B/op", "bytes/op")
        self.after.write_text(text, encoding="utf-8")
        self.assertIn("changed units", compare(self.before, self.after)[0])

    def test_malformed_and_short_samples_fail_closed(self):
        self.before.write_text("BenchmarkBad-8 100 not-a-number ns/op\n", encoding="utf-8")
        with self.assertRaises(BenchmarkError):
            parse(self.before)
        self.before.write_text(output(self.rows, samples=9), encoding="utf-8")
        with self.assertRaises(BenchmarkError):
            parse(self.before)


if __name__ == "__main__":
    unittest.main()
