#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT="$(mktemp -d)"
trap 'rm -rf "$OUTPUT"' EXIT

cd "$ROOT"
./visualizer/build.sh "$OUTPUT"

ASSETS=(index.html styles.css app.js wasm.js view.js favicon.svg wasm_exec.js epaxos.wasm)
for asset in "${ASSETS[@]}"; do
  if [[ ! -s "$OUTPUT/$asset" ]]; then
    echo "visualizer build did not produce nonempty $asset" >&2
    exit 1
  fi
done

GOOS=js GOARCH=wasm go vet -tags=purego ./visualizer/cmd/wasm
