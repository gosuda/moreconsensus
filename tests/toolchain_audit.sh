#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

source tests/toolchain.env

require_text() {
  local file="$1"
  local text="$2"
  if ! grep -Fq -- "$text" "$file"; then
    echo "missing toolchain text in $file: $text" >&2
    exit 1
  fi
}

reject_text() {
  local file="$1"
  local text="$2"
  if grep -Fq -- "$text" "$file"; then
    echo "unpinned toolchain text in $file: $text" >&2
    exit 1
  fi
}

require_sha40() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9a-f]{40}$ ]]; then
    echo "unpinned action SHA for $name: $value" >&2
    exit 1
  fi
}

require_text tests/toolchain.env "GO_VERSION=$GO_VERSION"
require_text tests/toolchain.env "JAVA_DISTRIBUTION=$JAVA_DISTRIBUTION"
require_text tests/toolchain.env "JAVA_VERSION=$JAVA_VERSION"
require_text tests/toolchain.env "TLA_TOOLS_VERSION=$TLA_TOOLS_VERSION"
require_text tests/toolchain.env "TLA_TOOLS_URL=$TLA_TOOLS_URL"
require_text tests/toolchain.env "TLA_TOOLS_SHA256=$TLA_TOOLS_SHA256"
require_text tests/toolchain.env "LEIN_VERSION=$LEIN_VERSION"
require_text tests/toolchain.env "LEIN_STANDALONE_URL=$LEIN_STANDALONE_URL"
require_text tests/toolchain.env "LEIN_STANDALONE_SHA256=$LEIN_STANDALONE_SHA256"
require_text tests/toolchain.env "ACTIONS_CHECKOUT_SHA=$ACTIONS_CHECKOUT_SHA"
require_text tests/toolchain.env "ACTIONS_SETUP_GO_SHA=$ACTIONS_SETUP_GO_SHA"
require_text tests/toolchain.env "ACTIONS_SETUP_JAVA_SHA=$ACTIONS_SETUP_JAVA_SHA"
require_text tests/toolchain.env "ACTIONS_CACHE_SHA=$ACTIONS_CACHE_SHA"
require_sha40 ACTIONS_CHECKOUT_SHA "$ACTIONS_CHECKOUT_SHA"
require_sha40 ACTIONS_SETUP_GO_SHA "$ACTIONS_SETUP_GO_SHA"
require_sha40 ACTIONS_SETUP_JAVA_SHA "$ACTIONS_SETUP_JAVA_SHA"
require_sha40 ACTIONS_CACHE_SHA "$ACTIONS_CACHE_SHA"

require_text go.mod "toolchain go$GO_VERSION"
require_text go.work "toolchain go$GO_VERSION"
require_text examples/kv/go.mod "toolchain go$GO_VERSION"
require_text .github/workflows/ci.yml "GO_VERSION: '$GO_VERSION'"
require_text .github/workflows/ci.yml "JAVA_DISTRIBUTION: '$JAVA_DISTRIBUTION'"
require_text .github/workflows/ci.yml "distribution: \${{ env.JAVA_DISTRIBUTION }}"
require_text .github/workflows/ci.yml "JAVA_VERSION: '$JAVA_VERSION'"
require_text .github/workflows/ci.yml "LEIN_VERSION: '$LEIN_VERSION'"
require_text .github/workflows/ci.yml "$LEIN_STANDALONE_URL"
require_text .github/workflows/ci.yml "$LEIN_STANDALONE_SHA256"
require_text .github/workflows/ci.yml "actions/checkout@$ACTIONS_CHECKOUT_SHA"
require_text .github/workflows/ci.yml "actions/setup-go@$ACTIONS_SETUP_GO_SHA"
require_text .github/workflows/ci.yml "actions/setup-java@$ACTIONS_SETUP_JAVA_SHA"
require_text .github/workflows/ci.yml "actions/cache@$ACTIONS_CACHE_SHA"
require_text tests/tla_model_check.sh 'source "$TOOLCHAIN_ENV"'
require_text tests/tla_model_check.sh '$TLA_TOOLS_URL'
require_text tests/tla_model_check.sh '$TLA_TOOLS_SHA256'

reject_text .github/workflows/ci.yml "1.26.x"
reject_text .github/workflows/ci.yml "/stable/bin/lein"
reject_text .github/workflows/ci.yml "actions/checkout@v"
reject_text .github/workflows/ci.yml "actions/setup-go@v"
reject_text .github/workflows/ci.yml "actions/setup-java@v"
reject_text .github/workflows/ci.yml "actions/cache@v"
reject_text tests/tla_model_check.sh "releases/latest"
