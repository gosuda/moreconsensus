#!/usr/bin/env python3
"""Compare repeated Go benchmark output without third-party dependencies."""

from __future__ import annotations

import argparse
import math
import pathlib
import re
import statistics
import sys
from collections import defaultdict

BENCHMARK_RE = re.compile(r"^(Benchmark\S+)\s+([1-9][0-9]*)\s+(.+)$")
VALUE_RE = re.compile(r"^(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$")
REQUIRED_SAMPLES = 10
NONREGRESSION_UNITS = {"allocs/op", "B/op", "resident-B/instance"}


class BenchmarkError(ValueError):
    pass


def parse(path: pathlib.Path, required_samples: int = REQUIRED_SAMPLES):
    samples: dict[str, list[dict[str, float]]] = defaultdict(list)
    units_by_name: dict[str, tuple[str, ...]] = {}
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.startswith("Benchmark"):
            continue
        match = BENCHMARK_RE.match(raw.strip())
        if not match:
            raise BenchmarkError(f"{path}:{line_number}: malformed benchmark sample")
        name, _, fields = match.groups()
        tokens = fields.split()
        if len(tokens) % 2:
            raise BenchmarkError(f"{path}:{line_number}: malformed metric/value pairs")
        metrics: dict[str, float] = {}
        for index in range(0, len(tokens), 2):
            value_text, unit = tokens[index:index + 2]
            if not VALUE_RE.match(value_text):
                raise BenchmarkError(f"{path}:{line_number}: malformed value {value_text!r}")
            value = float(value_text)
            if not math.isfinite(value) or unit in metrics:
                raise BenchmarkError(f"{path}:{line_number}: invalid or duplicate unit {unit!r}")
            metrics[unit] = value
        if "ns/op" not in metrics:
            raise BenchmarkError(f"{path}:{line_number}: missing ns/op")
        units = tuple(metrics)
        if name in units_by_name and units_by_name[name] != units:
            raise BenchmarkError(f"{path}:{line_number}: changed units within {name}")
        units_by_name[name] = units
        samples[name].append(metrics)
    if not samples:
        raise BenchmarkError(f"{path}: no benchmark samples")
    for name, rows in samples.items():
        if len(rows) != required_samples:
            raise BenchmarkError(f"{path}: {name} has {len(rows)} samples, want {required_samples}")
    return dict(samples), units_by_name


def compare(before_path: pathlib.Path, after_path: pathlib.Path, required_samples: int = REQUIRED_SAMPLES) -> list[str]:
    before, before_units = parse(before_path, required_samples)
    after, after_units = parse(after_path, required_samples)
    errors: list[str] = []
    before_names, after_names = set(before), set(after)
    for name in sorted(before_names - after_names):
        errors.append(f"missing benchmark: {name}")
    for name in sorted(after_names - before_names):
        errors.append(f"extra benchmark: {name}")
    for name in sorted(before_names & after_names):
        if before_units[name] != after_units[name]:
            errors.append(f"{name}: changed units {before_units[name]} -> {after_units[name]}")
            continue
        for unit in before_units[name]:
            baseline = statistics.median(row[unit] for row in before[name])
            candidate = statistics.median(row[unit] for row in after[name])
            if unit == "ns/op" and candidate > baseline * 1.05:
                errors.append(f"{name}: median ns/op increased {baseline:g} -> {candidate:g} (>5%)")
            elif unit in NONREGRESSION_UNITS and candidate > baseline:
                errors.append(f"{name}: median {unit} increased {baseline:g} -> {candidate:g}")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("before", type=pathlib.Path)
    parser.add_argument("after", type=pathlib.Path)
    parser.add_argument("--samples", type=int, default=REQUIRED_SAMPLES)
    args = parser.parse_args(argv)
    try:
        errors = compare(args.before, args.after, args.samples)
    except (OSError, UnicodeError, BenchmarkError) as error:
        print(error, file=sys.stderr)
        return 2
    if errors:
        print("benchmark comparison failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(f"benchmark comparison passed: {args.after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
