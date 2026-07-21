#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT="${1:-$ROOT/visualizer/dist}"
SHIM="$(go env GOROOT)/lib/wasm/wasm_exec.js"
ASSETS=(index.html styles.css app.js wasm.js view.js favicon.svg)

if [[ ! -f "$SHIM" ]]; then
  echo "Go's WebAssembly runtime shim was not found." >&2
  exit 1
fi

mkdir -p "$OUTPUT"
for asset in "${ASSETS[@]}"; do
  rm -f "$OUTPUT/$asset"
  cp "$ROOT/visualizer/web/$asset" "$OUTPUT/$asset"
done
rm -f "$OUTPUT/wasm_exec.js" "$OUTPUT/epaxos.wasm"
cp "$SHIM" "$OUTPUT/wasm_exec.js"
chmod 0644 "$OUTPUT/wasm_exec.js"
GOOS=js GOARCH=wasm go build -tags=purego -trimpath -ldflags='-s -w' -o "$OUTPUT/epaxos.wasm" ./visualizer/cmd/wasm
