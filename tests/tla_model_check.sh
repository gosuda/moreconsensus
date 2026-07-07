#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TLA_JAR="${TLA_JAR:-/tmp/tla2tools.jar}"
if [[ ! -f "$TLA_JAR" ]]; then
  curl -fsSL -o "$TLA_JAR" https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
fi

JAVA_BIN="${JAVA_BIN:-java}"
if ! "$JAVA_BIN" -version >/dev/null 2>&1; then
  if [[ -x /opt/homebrew/opt/openjdk/bin/java ]]; then
    JAVA_BIN=/opt/homebrew/opt/openjdk/bin/java
  fi
fi

for cfg in tla/EPaxos.cfg tla/EPaxosKVConflict.cfg; do
  "$JAVA_BIN" -cp "$TLA_JAR" tlc2.TLC -config "$cfg" tla/EPaxos.tla
  rm -rf states
done

"$JAVA_BIN" -cp "$TLA_JAR" tlc2.TLC -config tla/Quorum.cfg tla/Quorum.tla
rm -rf states
