#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

workflow_root="$ROOT"
verifier="$ROOT/tests/release_evidence_verifier.py"

case "${GO_NO_GO_TEST_MODE:-}" in
  yes)
    if [[
      -z "${GO_NO_GO_TEST_ROOT:-}" ||
      -z "${GO_NO_GO_TEST_EVIDENCE_ROOT:-}" ||
      -z "${GO_NO_GO_TEST_HOOK_ROOT:-}" ||
      -z "${GO_NO_GO_NOW:-}"
    ]]; then
      echo "GO_NO_GO_TEST_ROOT, GO_NO_GO_TEST_EVIDENCE_ROOT, GO_NO_GO_TEST_HOOK_ROOT, and GO_NO_GO_NOW are required in explicit test mode" >&2
      exit 1
    fi
    if [[ -n "${GO_NO_GO_EVIDENCE_ROOT:-}" ]]; then
      echo "GO_NO_GO_EVIDENCE_ROOT must not be used for synthetic fixtures" >&2
      exit 1
    fi
    workflow_root="$GO_NO_GO_TEST_ROOT"
    ;;
  "")
    if [[
      -n "${GO_NO_GO_TEST_ROOT:-}" ||
      -n "${GO_NO_GO_TEST_EVIDENCE_ROOT:-}" ||
      -n "${GO_NO_GO_TEST_HOOK_ROOT:-}" ||
      -n "${GO_NO_GO_NOW:-}"
    ]]; then
      echo "GO_NO_GO_TEST_* and GO_NO_GO_NOW are test-only overrides" >&2
      exit 1
    fi
    ;;
  *)
    echo "GO_NO_GO_TEST_MODE must be exactly yes when enabled" >&2
    exit 1
    ;;
esac

scope="$workflow_root/RELEASE_SCOPE.md"
evidence="$workflow_root/release/EPAXOS_READINESS_EVIDENCE.md"

if [[ ! -f "$scope" ]]; then
  echo "missing release scope lock: $scope" >&2
  exit 1
fi
if [[ ! -f "$evidence" ]]; then
  echo "missing evidence bundle: $evidence" >&2
  exit 1
fi
if [[ ! -f "$verifier" ]]; then
  echo "missing release evidence verifier: $verifier" >&2
  exit 1
fi

if [[ "${GO_NO_GO_TEST_MODE:-}" == "yes" ]]; then
  verification_output="$(
    python3 "$verifier" \
      --repository-root "$workflow_root" \
      --scope "$scope" \
      --evidence-root "$GO_NO_GO_TEST_EVIDENCE_ROOT" \
      --test-mode \
      --test-hook-root "$GO_NO_GO_TEST_HOOK_ROOT" \
      --now "$GO_NO_GO_NOW"
  )"
elif [[ -n "${GO_NO_GO_EVIDENCE_ROOT:-}" ]]; then
  verification_output="$(
    python3 "$verifier" \
      --repository-root "$workflow_root" \
      --scope "$scope" \
      --evidence-root "$GO_NO_GO_EVIDENCE_ROOT"
  )"
else
  verification_output="$(
    python3 "$verifier" \
      --repository-root "$workflow_root" \
      --scope "$scope"
  )"
fi

decision_line="${verification_output%%$'\n'*}"
if [[ "$decision_line" != "release_decision=Go." && "$decision_line" != "release_decision=No-go." ]]; then
  echo "release evidence verifier returned an invalid decision line: $decision_line" >&2
  exit 1
fi

if ! LC_ALL=C grep -Fq -- "bash tests/go_no_go_workflow.sh" "$evidence"; then
  echo "evidence bundle missing required workflow command" >&2
  exit 1
fi
if [[ "$decision_line" == "release_decision=No-go." ]]; then
  for required in \
    "Status: no-go evidence bundle" \
    "Current open blockers preserving no-go"; do
    if ! LC_ALL=C grep -Fq -- "$required" "$evidence"; then
      echo "no-go evidence bundle missing required text: $required" >&2
      exit 1
    fi
  done
else
  if ! LC_ALL=C grep -Fq -- "Status: go evidence bundle" "$evidence"; then
    echo "go evidence bundle missing required status" >&2
    exit 1
  fi
  for forbidden in \
    "Status: no-go evidence bundle" \
    "Current open blockers preserving no-go"; do
    if LC_ALL=C grep -Fq -- "$forbidden" "$evidence"; then
      echo "go evidence bundle contains stale no-go text: $forbidden" >&2
      exit 1
    fi
  done
fi

printf '%s\n' "$verification_output"
