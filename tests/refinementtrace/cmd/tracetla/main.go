// Command tracetla captures the finite RawNode scenarios, projects them onto
// the EPaxosRawNodeRefinement variable domain, and writes one TLC-checkable
// trace module and config per scenario into tla/traces/, together with four
// deterministic negative controls: a label swap, a paper-state corruption, a
// non-paper stutter mutation, and a recovery-evidence rewrite. The check
// script adds a fifth, shell-generated choose-execute midpoint mutant.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	refinementtrace "gosuda.org/moreconsensus/tests/refinementtrace"
)

func main() {
	revision := flag.String("revision", "", "clean 40- or 64-character lowercase hex source revision")
	outDir := flag.String("out", "tla/traces", "output directory for generated trace modules")
	flag.Parse()
	if err := run(*revision, *outDir); err != nil {
		fmt.Fprintln(os.Stderr, "tracetla:", err)
		os.Exit(1)
	}
}

// minimum non-stutter paper actions per scenario; the generator fails closed
// when a capture stops exercising them (vacuity guard).
var requiredPaper = map[string]map[string]int{
	"normal-fast-slow":          {"Choose": 1, "Execute": 1},
	"recovery-response-restart": {"BeginRecovery": 1, "Choose": 1, "Execute": 1},
	"toq-logical-processat":     {"Choose": 1, "Execute": 1},
	"config-outcome-history":    {"Reconfigure": 1, "ChooseExecute": 1, "Choose": 1, "Execute": 1},
}

func run(revision, outDir string) error {
	if _, err := os.Stat(filepath.Join("tla", "EPaxosTraceCheck.tla")); err != nil {
		return fmt.Errorf("run from the repository root: %w", err)
	}
	projections, err := refinementtrace.CaptureTLA(revision)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return err
	}
	var manifest strings.Builder
	for _, projection := range projections {
		counts := paperCounts(projection)
		if err := checkRequired(projection.Scenario, counts); err != nil {
			return err
		}
		module := "Trace_" + strings.ReplaceAll(projection.Scenario, "-", "_")
		if err := writeTrace(outDir, module, projection); err != nil {
			return err
		}
		fmt.Fprintf(&manifest, "accept\t%s\t%d\t%s\n", module, len(projection.Steps), countsString(counts))
		if projection.Scenario == "normal-fast-slow" {
			labelMutant, err := mutateLabel(projection)
			if err != nil {
				return err
			}
			if err := writeTrace(outDir, module+"__mutant_label", labelMutant); err != nil {
				return err
			}
			fmt.Fprintf(&manifest, "reject\t%s\t%d\tlabel-swap\n", module+"__mutant_label", len(labelMutant.Steps))
			stateMutant, err := mutateState(projection)
			if err != nil {
				return err
			}
			if err := writeTrace(outDir, module+"__mutant_state", stateMutant); err != nil {
				return err
			}
			fmt.Fprintf(&manifest, "reject\t%s\t%d\tstate-corruption\n", module+"__mutant_state", len(stateMutant.Steps))
			nonPaperMutant, err := mutateNonPaperState(projection)
			if err != nil {
				return err
			}
			if err := writeTrace(outDir, module+"__mutant_nonpaper", nonPaperMutant); err != nil {
				return err
			}
			fmt.Fprintf(&manifest, "reject\t%s\t%d\tnon-paper-state-corruption\n", module+"__mutant_nonpaper", len(nonPaperMutant.Steps))
		}
		if projection.Scenario == "recovery-response-restart" {
			recoveryEvidenceMutant, err := mutateRecoveryEvidence(projection)
			if err != nil {
				return err
			}
			if err := writeTrace(outDir, module+"__mutant_recovery_evidence", recoveryEvidenceMutant); err != nil {
				return err
			}
			fmt.Fprintf(&manifest, "reject\t%s\t%d\trecovery-evidence-rewrite\n", module+"__mutant_recovery_evidence", len(recoveryEvidenceMutant.Steps))
		}
	}
	manifestPath := filepath.Join(outDir, "manifest.tsv")
	if err := os.WriteFile(manifestPath, []byte(manifest.String()), 0o600); err != nil {
		return err
	}
	fmt.Print(manifest.String())
	return nil
}

func paperCounts(projection refinementtrace.TraceProjection) map[string]int {
	counts := map[string]int{}
	for _, step := range projection.Steps {
		if step.Paper != "Stutter" && step.Paper != "Init" {
			counts[step.Paper]++
		}
	}
	return counts
}

func checkRequired(scenario string, counts map[string]int) error {
	required, ok := requiredPaper[scenario]
	if !ok {
		return fmt.Errorf("scenario %s has no required paper-action floor", scenario)
	}
	for action, minimum := range required {
		if counts[action] < minimum {
			return fmt.Errorf("scenario %s exercises %d %s paper actions, want at least %d", scenario, counts[action], action, minimum)
		}
	}
	return nil
}

func countsString(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, ";")
}

// mutateLabel swaps the action label of the first Choose step to a different
// allowlisted label whose audited (action, kind) pair grants stutter only, so
// TLC must reject the pair through exact twelve-variable stutter (the paper
// decision genuinely changes).
func mutateLabel(projection refinementtrace.TraceProjection) (refinementtrace.TraceProjection, error) {
	mutant := cloneProjection(projection)
	for i := range mutant.Steps {
		if mutant.Steps[i].Paper == "Choose" {
			mutant.Steps[i].Action = "RecoveryPersistAccepted"
			return mutant, nil
		}
	}
	return refinementtrace.TraceProjection{}, fmt.Errorf("scenario %s has no Choose step to mutate", projection.Scenario)
}

// mutateNonPaperState is the direct regression guard for full mapped-state
// stuttering. It corrupts records.instA.ballot on an otherwise exact-stutter
// NormalBuildReady step. The value remains in the model ballot domain and does
// not alter a paper variable, so only the complete action correspondence can
// reject it.
func mutateNonPaperState(projection refinementtrace.TraceProjection) (refinementtrace.TraceProjection, error) {
	mutant := cloneProjection(projection)
	for i := range mutant.Steps {
		step := mutant.Steps[i]
		if step.Action != "NormalBuildReady" || step.Kind != "ready-observed" || step.Paper != "Stutter" {
			continue
		}
		state := make(map[string]any, len(step.State))
		for key, value := range step.State {
			state[key] = value
		}
		recordsSource := step.State["records"].(map[string]any)
		records := make(map[string]any, len(recordsSource))
		for key, value := range recordsSource {
			records[key] = value
		}
		instASource := recordsSource["instA"].(map[string]any)
		instA := make(map[string]any, len(instASource))
		for key, value := range instASource {
			instA[key] = value
		}
		ballot := instA["ballot"].(uint64)
		if ballot >= 2 {
			return refinementtrace.TraceProjection{}, fmt.Errorf("scenario %s stutter ballot %d cannot be corrupted within the model domain", projection.Scenario, ballot)
		}
		instA["ballot"] = ballot + 1
		records["instA"] = instA
		state["records"] = records
		mutant.Steps[i].State = state
		return mutant, nil
	}
	return refinementtrace.TraceProjection{}, fmt.Errorf("scenario %s has no NormalBuildReady exact-stutter step to corrupt", projection.Scenario)
}

// mutateRecoveryEvidence rewrites the persisted recovery evidence from the
// BeginRecovery pair through the remaining trace. The rewrite stays internally
// consistent, so only the action's pin to the projected record can reject it.
func mutateRecoveryEvidence(projection refinementtrace.TraceProjection) (refinementtrace.TraceProjection, error) {
	mutant := cloneProjection(projection)
	started := false
	for i := range mutant.Steps {
		step := &mutant.Steps[i]

		if step.Paper == "BeginRecovery" {
			started = true
		}
		if !started {
			continue
		}
		state := make(map[string]any, len(step.State))
		for key, value := range step.State {
			state[key] = value
		}
		for _, key := range []string{"recoveryEvidence", "paperEvidence"} {
			evidenceSource := step.State[key].(map[string]any)
			evidence := make(map[string]any, len(evidenceSource))
			for name, value := range evidenceSource {
				evidence[name] = value
			}
			instA := evidenceSource["instA"].(map[string]any)
			record := make(map[string]any, len(instA))
			for field, value := range instA {
				record[field] = value
			}
			record["recordBallot"] = uint64(0)
			evidence["instA"] = record
			state[key] = evidence
		}
		step.State = state
	}
	if !started {
		return refinementtrace.TraceProjection{}, fmt.Errorf("scenario %s has no BeginRecovery step to mutate", projection.Scenario)
	}
	return mutant, nil
}
// mutateState corrupts one projected state value: the first Execute step's
// paperExecuted set loses its instance, so the model's PaperExecute predicate
// (and the mapping invariants) must reject the pair.
func mutateState(projection refinementtrace.TraceProjection) (refinementtrace.TraceProjection, error) {
	mutant := cloneProjection(projection)
	for i := range mutant.Steps {
		if mutant.Steps[i].Paper == "Execute" {
			state := make(map[string]any, len(mutant.Steps[i].State))
			for key, value := range mutant.Steps[i].State {
				state[key] = value
			}
			state["paperExecuted"] = []string{}
			mutant.Steps[i].State = state
			return mutant, nil
		}
	}
	return refinementtrace.TraceProjection{}, fmt.Errorf("scenario %s has no Execute step to mutate", projection.Scenario)
}

func cloneProjection(projection refinementtrace.TraceProjection) refinementtrace.TraceProjection {
	out := projection
	out.Steps = append([]refinementtrace.TraceStep(nil), projection.Steps...)
	return out
}

func writeTrace(outDir, module string, projection refinementtrace.TraceProjection) error {
	var b strings.Builder
	fmt.Fprintf(&b, "---- MODULE %s ----\n", module)
	b.WriteString("(* Generated by tests/refinementtrace/cmd/tracetla. Do not edit. *)\n")
	b.WriteString("EXTENDS EPaxosTraceCheck\n\n")
	b.WriteString("TraceDataDef == <<\n")
	for i, step := range projection.Steps {
		b.WriteString("    ")
		b.WriteString(renderStep(step))
		if i != len(projection.Steps)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString(">>\n")
	b.WriteString("====\n")
	if err := os.WriteFile(filepath.Join(outDir, module+".tla"), []byte(b.String()), 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, module+".cfg"), []byte(renderConfig(projection.Mode)), 0o600)
}

func renderConfig(mode string) string {
	nodes := "{1, 2, 3}"
	if mode == "config" {
		nodes = "{1, 2, 3, 4}"
	}
	var b strings.Builder
	b.WriteString("SPECIFICATION TraceSpec\n\n")
	for _, invariant := range []string{
		"TraceStepAccepted", "TypeOK", "RefinementInvariant", "TupleConfigMapping",
		"EvidenceMapping", "DurabilityBeforeEffects", "ReadyFrozen", "ExactConfigPinning",
		"TOQPendingBlocksApplication", "RecoveryBallotAndUniqueSender", "ExecutionOrdering",
		"CommittedMapsChosen", "AcknowledgementIsDurable",
	} {
		fmt.Fprintf(&b, "INVARIANT %s\n", invariant)
	}
	b.WriteString("\nCHECK_DEADLOCK FALSE\n")
	b.WriteString("\nCONSTANTS\n")
	fmt.Fprintf(&b, "    Nodes = %s\n", nodes)
	b.WriteString("    Commands = {\"alpha\", \"beta\"}\n")
	b.WriteString("    OldConfig = 1\n")
	b.WriteString("    NewConfig = 2\n")
	fmt.Fprintf(&b, "    Mode = \"%s\"\n", mode)
	b.WriteString("    MaxTick = 2\n")
	b.WriteString("    TraceData <- TraceDataDef\n")
	return b.String()
}

func renderStep(step refinementtrace.TraceStep) string {
	state := step.State
	fields := []string{
		fmt.Sprintf("lbl |-> %q", step.Action),
		fmt.Sprintf("kind |-> %q", step.Kind),
		fmt.Sprintf("paper |-> %q", step.Paper),
		fmt.Sprintf("inst |-> %q", step.Inst),
		"records |-> " + renderInstMap(state["records"], renderRecord),
		"durableRecords |-> " + renderInstMap(state["durableRecords"], renderRecord),
		"recoveryEvidence |-> " + renderInstMap(state["recoveryEvidence"], renderEvidence),
		"applied |-> " + renderStringSet(state["applied"]),
		"applyLog |-> " + renderStringSeq(state["applyLog"]),
		fmt.Sprintf("currentTick |-> %d", state["currentTick"].(uint64)),
		"toqPending |-> " + renderBool(state["toqPending"].(bool)),
		fmt.Sprintf("activeConfig |-> %q", state["activeConfig"].(string)),
		"paperDecision |-> " + renderInstMap(state["paperDecision"], renderTuple),
		"paperExecuted |-> " + renderStringSet(state["paperExecuted"]),
		"paperEvidence |-> " + renderInstMap(state["paperEvidence"], renderEvidence),
		fmt.Sprintf("paperConfig |-> %q", state["paperConfig"].(string)),
	}
	return "[" + strings.Join(fields, ", ") + "]"
}

func renderBool(v bool) string {
	if v {
		return "TRUE"
	}
	return "FALSE"
}

func renderStringSet(v any) string {
	values := v.([]string)
	if len(values) == 0 {
		return "{}"
	}
	quoted := make([]string, len(values))
	for i, s := range values {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return "{" + strings.Join(quoted, ", ") + "}"
}

func renderStringSeq(v any) string {
	values := v.([]string)
	if len(values) == 0 {
		return "<<>>"
	}
	quoted := make([]string, len(values))
	for i, s := range values {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return "<<" + strings.Join(quoted, ", ") + ">>"
}

func renderInstMap(v any, render func(any) string) string {
	m := v.(map[string]any)
	return "[instA |-> " + render(m["instA"]) + ", instB |-> " + render(m["instB"]) + "]"
}

func renderTuple(v any) string {
	m := v.(map[string]any)
	return fmt.Sprintf("[present |-> %s, cmd |-> %q, seq |-> %d, deps |-> %s, conf |-> %q]",
		renderBool(m["present"].(bool)), m["cmd"].(string), m["seq"].(uint64),
		renderStringSet(m["deps"]), m["conf"].(string))
}

func renderRecord(v any) string {
	m := v.(map[string]any)
	return fmt.Sprintf("[status |-> %q, ballot |-> %d, recordBallot |-> %d, tuple |-> %s]",
		m["status"].(string), m["ballot"].(uint64), m["recordBallot"].(uint64), renderTuple(m["tuple"]))
}

func renderEvidence(v any) string {
	m := v.(map[string]any)
	return fmt.Sprintf("[ballot |-> %d, recordBallot |-> %d, senders |-> %s, tuple |-> %s]",
		m["ballot"].(uint64), m["recordBallot"].(uint64), renderStringSet(m["senders"]), renderTuple(m["tuple"]))
}
