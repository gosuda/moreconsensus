#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${1:-8080}"

"$ROOT/visualizer/build.sh"
exec python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$ROOT/visualizer/dist"
