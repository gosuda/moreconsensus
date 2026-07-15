#!/usr/bin/env bash
# Checked Go-trace-to-TLC action correspondence gate.
#
# Generates the projected RawNode scenario traces with
# tests/refinementtrace/cmd/tracetla, replays every scenario trace through TLC
# against tla/EPaxosTraceCheck.tla (which extends the refinement model and
# re-verifies each consecutive projected pair with faithful complete
# twelve-variable actions and invariants), asserts that TLC consumed every
# projected state, and then requires TLC to REJECT five generated negative
# controls (action-label swap, paper-state corruption, non-paper mapped-state
# corruption, recovery-evidence rewrite, and an executed-without-choice
# ChooseExecute midpoint). Finally it re-runs the four pre-existing refinement
# cfgs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT:?}"

TRACES_DIR="tla/traces"
TOOLCHAIN="tests/toolchain.env"

tla_version="$(sed -n 's/^TLA_TOOLS_VERSION=//p' "${TOOLCHAIN}")"
tla_sha256="$(sed -n 's/^TLA_TOOLS_SHA256=//p' "${TOOLCHAIN}")"
JAR="${TLA_JAR:-/tmp/tla2tools-${tla_version:?missing TLA_TOOLS_VERSION}.jar}"
if [[ ! -f "${JAR}" ]]; then
  echo "trace-refinement: TLC jar ${JAR} is missing (run tests/tla_model_check_fast.sh first)" >&2
  exit 1
fi
echo "${tla_sha256:?missing TLA_TOOLS_SHA256}  ${JAR}" | sha256sum --check --quiet -

JAVA_BIN="${JAVA_BIN:-java}"
REVISION="$(git -C "${ROOT}" rev-parse HEAD)"

rm -f -- "${TRACES_DIR}"/Trace_*.tla "${TRACES_DIR}"/Trace_*.cfg "${TRACES_DIR}"/manifest.tsv \
  "${TRACES_DIR}"/EPaxosTraceCheck.tla "${TRACES_DIR}"/EPaxosRawNodeRefinement.tla
go run ./tests/refinementtrace/cmd/tracetla -revision "${REVISION}" -out "${TRACES_DIR}" >/dev/null
# TLC resolves imported modules relative to the main module directory.
cp tla/EPaxosTraceCheck.tla tla/EPaxosRawNodeRefinement.tla "${TRACES_DIR}/"

# The config-outcome-history trace is the sole sampled ChooseExecute pair.
# Preserve its executed post-state but restore its decision to NoTuple: the
# explicit PaperChooseRel -> PaperExecuteRel intermediate must reject it.
choose_execute_mutant="Trace_config_outcome_history__mutant_choose_execute_midpoint"
cp "${TRACES_DIR}/Trace_config_outcome_history.tla" "${TRACES_DIR}/${choose_execute_mutant}.tla"
cp "${TRACES_DIR}/Trace_config_outcome_history.cfg" "${TRACES_DIR}/${choose_execute_mutant}.cfg"
perl -0pi -e 's/^---- MODULE Trace_config_outcome_history ----$/---- MODULE Trace_config_outcome_history__mutant_choose_execute_midpoint ----/m' "${TRACES_DIR}/${choose_execute_mutant}.tla"
perl -0pi -e '
  $changed = s~(paper \|-> "ChooseExecute".*?paperDecision \|-> \[instA \|-> )\[present \|-> TRUE, cmd \|-> "CmdA", seq \|-> 1, deps \|-> \{\}, conf \|-> "old"\]~${1}[present |-> FALSE, cmd |-> "CmdA", seq |-> 0, deps |-> {}, conf |-> "old"]~s;
  END { exit !$changed }
' "${TRACES_DIR}/${choose_execute_mutant}.tla"
printf 'reject\t%s\t115\tchoose-execute-midpoint\n' "${choose_execute_mutant}" >> "${TRACES_DIR}/manifest.tsv"

run_tlc() {
  local config="$1"
  local module="${2:-$1}"
  local metadir
  metadir="$(mktemp -d "${TMPDIR:-/tmp}/moreconsensus-tracetlc-XXXXXX")"
  set +e
  (cd "${TRACES_DIR}" && "${JAVA_BIN}" -XX:+UseParallelGC -cp "${JAR}" tlc2.TLC \
    -workers 4 -metadir "${metadir}" -config "${config}.cfg" "${module}")
  TLC_STATUS=$?
  set -e
  rm -rf -- "${metadir:?}"
}

failures=0
while IFS=$'\t' read -r expectation module steps detail; do
  [[ -n "${module}" ]] || continue
  echo "== trace-refinement: ${module} (${expectation}, ${steps} states, ${detail}) =="
  output_file="$(mktemp "${TMPDIR:-/tmp}/moreconsensus-tracetlc-out-XXXXXX")"
  run_tlc "${module}" >"${output_file}" 2>&1
  case "${expectation}" in
    accept)
      if [[ "${TLC_STATUS}" -ne 0 ]] || ! grep -q "Model checking completed. No error has been found." "${output_file}"; then
        echo "trace-refinement: ${module} was NOT accepted (exit ${TLC_STATUS})" >&2
        tail -n 40 -- "${output_file}" >&2
        failures=$((failures + 1))
      elif ! grep -q "The depth of the complete state graph search is ${steps}." "${output_file}"; then
        echo "trace-refinement: ${module} did not consume all ${steps} projected states" >&2
        grep "depth of the complete state graph" -- "${output_file}" >&2 || true
        failures=$((failures + 1))
      else
        grep -E "states generated|depth of the complete" -- "${output_file}"
      fi
      ;;
    reject)
      if [[ "${TLC_STATUS}" -eq 0 ]] || ! grep -Eq "Error: Invariant .* is violated" "${output_file}"; then
        echo "trace-refinement: negative control ${module} was NOT rejected (exit ${TLC_STATUS})" >&2
        tail -n 40 -- "${output_file}" >&2
        failures=$((failures + 1))
      else
        grep -E "Error: Invariant .* is violated" -- "${output_file}"
      fi
      ;;
    *)
      echo "trace-refinement: unknown manifest expectation ${expectation}" >&2
      failures=$((failures + 1))
      ;;
  esac
  rm -f -- "${output_file}"
done < "${TRACES_DIR}/manifest.tsv"

accepts="$(grep -c $'^accept\t' "${TRACES_DIR}/manifest.tsv")"
rejects="$(grep -c $'^reject\t' "${TRACES_DIR}/manifest.tsv")"
if [[ "${accepts}" -ne 4 || "${rejects}" -ne 5 ]]; then
  echo "trace-refinement: manifest lists ${accepts} scenario traces and ${rejects} negative controls, want 4 and 5" >&2
  failures=$((failures + 1))
fi

for config in EPaxosRawNodeRefinementConfig EPaxosRawNodeRefinementNormal EPaxosRawNodeRefinementRecovery EPaxosRawNodeRefinementTOQ; do
  cp "tla/${config}.cfg" "${TRACES_DIR}/"
  echo "== trace-refinement: ${config} (pre-existing cfg) =="
  output_file="$(mktemp "${TMPDIR:-/tmp}/moreconsensus-tracetlc-out-XXXXXX")"
  run_tlc "${config}" EPaxosRawNodeRefinement >"${output_file}" 2>&1
  if [[ "${TLC_STATUS}" -ne 0 ]] || ! grep -q "Model checking completed. No error has been found." "${output_file}"; then
    echo "trace-refinement: pre-existing config ${config} failed (exit ${TLC_STATUS})" >&2
    tail -n 40 -- "${output_file}" >&2
    failures=$((failures + 1))
  else
    grep -E "states generated|depth of the complete" -- "${output_file}"
  fi
  rm -f -- "${output_file}"
done

# The mutants are build artifacts of this check only; drop them after use.
rm -f -- "${TRACES_DIR}"/Trace_*__mutant_*.tla "${TRACES_DIR}"/Trace_*__mutant_*.cfg

if [[ "${failures}" -ne 0 ]]; then
  echo "trace-refinement: ${failures} failure(s)" >&2
  exit 1
fi
echo "trace-refinement: all scenario traces and four pre-existing cfgs accepted; all five negative controls rejected"
