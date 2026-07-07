#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${JEPSEN_NODES:-}" ]]; then
  echo "JEPSEN_NODES must list SSH hosts, comma-separated" >&2
  exit 2
fi

BIN="${TMPDIR:-/tmp}/moreconsensus-kvnode-remote-$$"
cleanup() {
  rm -f "$BIN"
}
trap cleanup EXIT

(cd examples/kv && go build -tags kvnode -o "$BIN" ./cmd/kvnode)

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Djava.net.preferIPv4Stack=true"
export MORECONSENSUS_KVNODE_REMOTE=1
export MORECONSENSUS_KVNODE_BIN="$BIN"
export MORECONSENSUS_KVNODE_REMOTE_DIR="${MORECONSENSUS_KVNODE_REMOTE_DIR:-/tmp/moreconsensus-jepsen}"
export MORECONSENSUS_KVNODE_HTTP_PORT="${MORECONSENSUS_KVNODE_HTTP_PORT:-8080}"
export MORECONSENSUS_KVNODE_FAULTS="${JEPSEN_REMOTE_FAULTS:-destructive-storage}"

(
  cd jepsen
  lein run test --nodes "$JEPSEN_NODES" --time-limit "${JEPSEN_REMOTE_DURATION:-30}" --concurrency "${JEPSEN_REMOTE_CONCURRENCY:-3n}" "$@"
)
