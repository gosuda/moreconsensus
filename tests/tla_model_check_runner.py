#!/usr/bin/env python3
"""Run pinned TLC finite-model profiles with bounded parallelism."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import pathlib
import shutil
import sys
import subprocess
import tempfile
import urllib.request
from dataclasses import dataclass

BOOTSTRAP_PROFILE = (
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapSize1.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapSize2.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapSize3.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapSize4.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapSize5.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapSize6.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapCrashPrefix.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapRace.cfg"),
    ("tla/EPaxosVoterBootstrap.tla", "tla/EPaxosVoterBootstrapFair.cfg"),
)

FAST_PROFILE = (
    ("tla/ReadyAdvance.tla", "tla/ReadyAdvance.cfg"),
    ("tla/ReadyAdvance.tla", "tla/ReadyAdvanceCapped.cfg"),
    ("tla/Quorum.tla", "tla/Quorum.cfg"),
    ("tla/EPaxosPaperSafety.tla", "tla/EPaxosPaperSafetyFastRecovery.cfg"),
    ("tla/EPaxosRevisited.tla", "tla/EPaxosRevisitedOptimizedFast.cfg"),
    ("tla/EPaxosRevisited.tla", "tla/EPaxosRevisitedDelayedOwnerIncomparable.cfg"),
    ("tla/EPaxosRevisited.tla", "tla/EPaxosRevisitedDelayedOwnerFallback.cfg"),
    ("tla/EPaxosExecutionConsistency.tla", "tla/EPaxosExecutionPruning.cfg"),
    ("tla/EPaxosOptimizedRecoveryDecisionTree.tla", "tla/EPaxosOptimizedRecoveryDecisionTree.cfg"),
    ("tla/EPaxosTimingDomain.tla", "tla/EPaxosTimingDomainRecovery.cfg"),
    ("tla/EPaxosRawNodeRefinement.tla", "tla/EPaxosRawNodeRefinementNormal.cfg"),
    ("tla/EPaxosRawNodeRefinement.tla", "tla/EPaxosRawNodeRefinementTOQ.cfg"),
    ("tla/EPaxosRawNodeRefinement.tla", "tla/EPaxosRawNodeRefinementRecovery.cfg"),
    ("tla/EPaxosSparsePrefix.tla", "tla/EPaxosSparsePrefixSafety.cfg"),
    *BOOTSTRAP_PROFILE,
)

FULL_PROFILE = (
    *(("tla/EPaxos.tla", cfg) for cfg in ("tla/EPaxos.cfg", "tla/EPaxosKVConflict.cfg", "tla/EPaxosThreeReplica.cfg")),
    *(("tla/ReadyAdvance.tla", cfg) for cfg in ("tla/ReadyAdvance.cfg", "tla/ReadyAdvanceCapped.cfg")),
    *(("tla/EPaxosResponses.tla", cfg) for cfg in ("tla/EPaxosResponses.cfg", "tla/EPaxosResponsesFive.cfg")),
    *(("tla/EPaxosRecovery.tla", cfg) for cfg in ("tla/EPaxosRecovery.cfg", "tla/EPaxosRecoveryFive.cfg")),
    *(("tla/EPaxosOptimizedRecovery.tla", cfg) for cfg in ("tla/EPaxosOptimizedRecovery.cfg", "tla/EPaxosOptimizedRecoveryFive.cfg", "tla/EPaxosOptimizedRecoverySeven.cfg")),
    *(("tla/EPaxosTryPreAcceptBranches.tla", cfg) for cfg in ("tla/EPaxosTryPreAcceptBranches.cfg", "tla/EPaxosTryPreAcceptBranchesFive.cfg", "tla/EPaxosTryPreAcceptBranchesSeven.cfg")),
    *(("tla/EPaxosTryPreAcceptMessagePath.tla", cfg) for cfg in ("tla/EPaxosTryPreAcceptMessagePath.cfg", "tla/EPaxosTryPreAcceptMessagePathFive.cfg", "tla/EPaxosTryPreAcceptMessagePathSeven.cfg")),
    *(("tla/EPaxosTryConflictForce.tla", cfg) for cfg in ("tla/EPaxosTryConflictForce.cfg", "tla/EPaxosTryConflictForceFive.cfg", "tla/EPaxosTryConflictForceSeven.cfg")),
    *(("tla/EPaxosEvidenceQuery.tla", cfg) for cfg in ("tla/EPaxosEvidenceQuery.cfg", "tla/EPaxosEvidenceQueryFive.cfg", "tla/EPaxosEvidenceQuerySeven.cfg")),
    *((f"tla/{name}.tla", f"tla/{name}.cfg") for name in (
        "EPaxosEvidenceStaleness", "EPaxosAcceptEvidenceMerge", "EPaxosTryPreAcceptRetry",
        "EPaxosOptimizedRecoveryDecisionTree", "EPaxosConfigBarrier", "EPaxosConfigTransition",
        "EPaxosConfigRemoveTransition", "EPaxosConfigChainTransition", "EPaxosConfigChainTransitionRetry",
        "EPaxosConfigChainTransitionLostResponseRetry", "EPaxosConfigChainRecovery",
        "EPaxosConfigChainRecoveryLostResponseRetry", "EPaxosConfigTransitionDedup",
        "EPaxosConfigTransitionRetry", "EPaxosConfigTransitionLostResponseRetry", "EPaxosConfigReplay",
        "EPaxosConfigRecovery", "EPaxosConfigRecoveryDedup", "EPaxosConfigRecoveryRetry",
        "EPaxosConfigRecoveryLostResponseRetry", "EPaxosConfigAddRecovery", "EPaxosRollbackAllocation",
        "EPaxosRevisited", "TOQClockDiscipline", "KVTimestampStaleness", "KVOmissionRecovery",
    )),
    *BOOTSTRAP_PROFILE,
    ("tla/Quorum.tla", "tla/Quorum.cfg"),
)

SUCCESS_TEXT = "Model checking completed. No error has been found."
FAILURE_TEXT = (
    "deadlock reached",
    "invariant invariant",
    "is violated",
    "model checking completed. no error has been found"  # handled as success before failure scan
)


@dataclass(frozen=True)
class Job:
    module: str
    config: str

    @property
    def label(self) -> str:
        return pathlib.Path(self.config).name


def read_toolchain(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"{path}:{number}: malformed toolchain entry")
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    for key in ("TLA_TOOLS_VERSION", "TLA_TOOLS_URL", "TLA_TOOLS_SHA256"):
        if not values.get(key):
            raise ValueError(f"{path}: missing {key}")
    return values


def select_profile(name: str) -> tuple[Job, ...]:
    raw = FAST_PROFILE if name == "fast" else FULL_PROFILE if name == "full" else ()
    jobs = tuple(Job(*entry) for entry in raw)
    if not jobs:
        raise ValueError(f"empty or unknown TLC profile {name!r}")
    return jobs


def prepare_jar(root: pathlib.Path, requested: pathlib.Path | None) -> pathlib.Path:
    toolchain = read_toolchain(root / "tests" / "toolchain.env")
    jar = requested or pathlib.Path(os.environ.get("TLA_JAR", f"/tmp/tla2tools-{toolchain['TLA_TOOLS_VERSION']}.jar"))
    if not jar.is_file():
        jar.parent.mkdir(parents=True, exist_ok=True)
        temporary = jar.with_name(jar.name + ".download")
        try:
            urllib.request.urlretrieve(toolchain["TLA_TOOLS_URL"], temporary)
            temporary.replace(jar)
        finally:
            temporary.unlink(missing_ok=True)
    actual = hashlib.sha256(jar.read_bytes()).hexdigest()
    expected = toolchain["TLA_TOOLS_SHA256"].lower()
    if actual.lower() != expected:
        raise ValueError(f"checksum mismatch for {jar}: got {actual}, want {expected}")
    return jar


def resolve_java(requested: str | None) -> str:
    candidates = [requested or os.environ.get("JAVA_BIN", "java")]
    if not requested:
        candidates.append("/opt/homebrew/opt/openjdk/bin/java")
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if not resolved:
            continue
        try:
            result = subprocess.run(
                [resolved, "-version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            return resolved
    raise ValueError(f"Java executable not found or unusable: {candidates[0]}")


async def run_job(job: Job, root: pathlib.Path, java: str, jar: pathlib.Path, timeout: float, semaphore: asyncio.Semaphore) -> str | None:
    async with semaphore:
        metadir = pathlib.Path(tempfile.mkdtemp(prefix="moreconsensus-tlc-"))
        process = None
        output: list[str] = []
        try:
            arguments = (java, "-cp", str(jar), "tlc2.TLC", "-workers", "4", "-metadir", str(metadir), "-config", job.config, job.module)
            process = await asyncio.create_subprocess_exec(*arguments, cwd=root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)

            async def stream() -> None:
                assert process is not None and process.stdout is not None
                while line := await process.stdout.readline():
                    text = line.decode("utf-8", errors="replace").rstrip()
                    output.append(text)
                    print(f"[{job.label}] {text}", flush=True)

            reader = asyncio.create_task(stream())
            try:
                if timeout > 0:
                    await asyncio.wait_for(process.wait(), timeout)
                else:
                    await process.wait()
            except TimeoutError:
                process.kill()
                await process.wait()
                await reader
                return f"{job.label}: timed out after {timeout:g}s"
            except asyncio.CancelledError:
                process.kill()
                await process.wait()
                await reader
                raise
            await reader
            text = "\n".join(output)
            lowered = text.lower()
            if process.returncode != 0:
                return f"{job.label}: Java exited {process.returncode}"
            if "deadlock reached" in lowered or "is violated" in lowered or "invariant invariant" in lowered:
                return f"{job.label}: TLC reported deadlock or invariant failure"
            if SUCCESS_TEXT not in text:
                return f"{job.label}: missing explicit TLC success result"
            print(f"[{job.label}] SUCCESS", flush=True)
            return None
        finally:
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            shutil.rmtree(metadir, ignore_errors=True)


async def run_profile(jobs: tuple[Job, ...], root: pathlib.Path, java: str, jar: pathlib.Path, workers: int, per_config_timeout: float, overall_timeout: float) -> list[str]:
    if not jobs:
        return ["empty TLC profile"]
    semaphore = asyncio.Semaphore(workers)
    tasks = [asyncio.create_task(run_job(job, root, java, jar, per_config_timeout, semaphore)) for job in jobs]
    try:
        gather = asyncio.gather(*tasks)
        results = await asyncio.wait_for(gather, overall_timeout) if overall_timeout > 0 else await gather
    except TimeoutError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return [f"overall TLC deadline exceeded after {overall_timeout:g}s"]
    return [result for result in results if result is not None]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("fast", "full"), required=True)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--per-config-timeout", type=float)
    parser.add_argument("--overall-timeout", type=float)
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1])
    parser.add_argument("--java-bin")
    parser.add_argument("--tla-jar", type=pathlib.Path)
    args = parser.parse_args(argv)
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    per_config = args.per_config_timeout if args.per_config_timeout is not None else (180.0 if args.profile == "fast" else 0.0)
    overall = args.overall_timeout if args.overall_timeout is not None else (600.0 if args.profile == "fast" else 0.0)
    if per_config < 0 or overall < 0:
        parser.error("timeouts must be nonnegative")
    try:
        root = args.root.resolve()
        jobs = select_profile(args.profile)
        jar = prepare_jar(root, args.tla_jar)
        java = resolve_java(args.java_bin)
        missing = [path for job in jobs for path in (job.module, job.config) if not (root / path).is_file()]
        if missing:
            raise ValueError(f"profile contains missing files: {', '.join(sorted(set(missing)))}")
        errors = asyncio.run(run_profile(jobs, root, java, jar, args.jobs, per_config, overall))
    except (OSError, UnicodeError, ValueError) as error:
        print(error, file=sys.stderr)
        return 2
    if errors:
        print("TLC profile failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(f"TLC {args.profile} profile passed ({len(jobs)} jobs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
