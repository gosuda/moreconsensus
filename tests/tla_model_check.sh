#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TOOLCHAIN_ENV="$ROOT/tests/toolchain.env"
source "$TOOLCHAIN_ENV"

verify_sha256() {
  local file="$1"
  local expected="$2"
  local actual

  if command -v sha256sum >/dev/null 2>&1; then
    echo "$expected  $file" | sha256sum -c -
    return
  fi

  actual="$(shasum -a 256 "$file" | cut -d ' ' -f 1)"
  if [[ "$actual" != "$expected" ]]; then
    echo "sha256 mismatch for $file: got $actual, want $expected" >&2
    exit 1
  fi
}

TLA_JAR="${TLA_JAR:-/tmp/tla2tools-${TLA_TOOLS_VERSION}.jar}"
if [[ ! -f "$TLA_JAR" ]]; then
  curl -fsSL -o "$TLA_JAR" "$TLA_TOOLS_URL"
fi
verify_sha256 "$TLA_JAR" "$TLA_TOOLS_SHA256"

JAVA_BIN="${JAVA_BIN:-java}"
if ! "$JAVA_BIN" -version >/dev/null 2>&1; then
  if [[ -x /opt/homebrew/opt/openjdk/bin/java ]]; then
    JAVA_BIN=/opt/homebrew/opt/openjdk/bin/java
  fi
fi

run_tlc() {
  local module="$1"
  local cfg="$2"
  local metadir
  local status

  metadir="$(mktemp -d "${TMPDIR:-/tmp}/moreconsensus-tlc.XXXXXX")"
  "$JAVA_BIN" -cp "$TLA_JAR" tlc2.TLC -metadir "$metadir" -config "$cfg" "$module" || status=$?
  rm -rf "$metadir"
  return "${status:-0}"
}

for cfg in tla/EPaxos.cfg tla/EPaxosKVConflict.cfg tla/EPaxosThreeReplica.cfg; do
  run_tlc tla/EPaxos.tla "$cfg"
done

for cfg in tla/ReadyAdvance.cfg tla/ReadyAdvanceCapped.cfg; do
  run_tlc tla/ReadyAdvance.tla "$cfg"
done

for cfg in tla/EPaxosResponses.cfg tla/EPaxosResponsesFive.cfg; do
  run_tlc tla/EPaxosResponses.tla "$cfg"
done

for cfg in tla/EPaxosRecovery.cfg tla/EPaxosRecoveryFive.cfg; do
  run_tlc tla/EPaxosRecovery.tla "$cfg"
done

for cfg in tla/EPaxosOptimizedRecovery.cfg tla/EPaxosOptimizedRecoveryFive.cfg tla/EPaxosOptimizedRecoverySeven.cfg; do
  run_tlc tla/EPaxosOptimizedRecovery.tla "$cfg"
done

for cfg in tla/EPaxosTryPreAcceptBranches.cfg tla/EPaxosTryPreAcceptBranchesFive.cfg tla/EPaxosTryPreAcceptBranchesSeven.cfg; do
  run_tlc tla/EPaxosTryPreAcceptBranches.tla "$cfg"
done

for cfg in tla/EPaxosEvidenceQuery.cfg tla/EPaxosEvidenceQueryFive.cfg tla/EPaxosEvidenceQuerySeven.cfg; do
  run_tlc tla/EPaxosEvidenceQuery.tla "$cfg"
done

for cfg in tla/EPaxosConfigBarrier.cfg; do
  run_tlc tla/EPaxosConfigBarrier.tla "$cfg"
done

for cfg in tla/EPaxosConfigTransition.cfg; do
  run_tlc tla/EPaxosConfigTransition.tla "$cfg"
done

for cfg in tla/EPaxosConfigRemoveTransition.cfg; do
  run_tlc tla/EPaxosConfigRemoveTransition.tla "$cfg"
done

for cfg in tla/EPaxosConfigChainTransition.cfg; do
  run_tlc tla/EPaxosConfigChainTransition.tla "$cfg"
done

for cfg in tla/EPaxosRollbackAllocation.cfg; do
  run_tlc tla/EPaxosRollbackAllocation.tla "$cfg"
done

for cfg in tla/EPaxosRevisited.cfg; do
  run_tlc tla/EPaxosRevisited.tla "$cfg"
done

for cfg in tla/TOQClockDiscipline.cfg; do
  run_tlc tla/TOQClockDiscipline.tla "$cfg"
done

for cfg in tla/KVTimestampStaleness.cfg; do
  run_tlc tla/KVTimestampStaleness.tla "$cfg"
done

for cfg in tla/KVOmissionRecovery.cfg; do
  run_tlc tla/KVOmissionRecovery.tla "$cfg"
done

run_tlc tla/Quorum.tla tla/Quorum.cfg
