package epaxos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
)

const faultTraceVersion = 1

type faultTrace struct {
	Version        int                    `json:"version"`
	SourceRevision string                 `json:"source_revision"`
	Config         faultSimConfig         `json:"config"`
	Seed           uint64                 `json:"seed"`
	Actions        []faultSimAction       `json:"actions"`
	Operations     []faultHistoryOperation `json:"operations"`
	Receipts       []faultActionReceipt   `json:"receipts"`
	Counters       map[string]uint64      `json:"counters"`
	TerminalHash   string                 `json:"terminal_hash"`
	Oracle         faultOracleResult      `json:"oracle"`
}

func faultSourceRevision() string {
	if revision := os.Getenv("EPAXOS_SOURCE_REVISION"); revision != "" {
		return revision
	}
	if revision := os.Getenv("SOURCE_REVISION"); revision != "" {
		return revision
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		revision := ""
		modified := false
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
		if revision != "" {
			if modified {
				return revision + "+modified"
			}
			return revision
		}
	}
	if revision := faultRepositoryRevision(); revision != "" {
		return revision + "+working-tree"
	}
	return "unknown-working-tree"
}

func faultRepositoryRevision() string {
	directory, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		gitDirectory := filepath.Join(directory, ".git")
		head, headErr := os.ReadFile(filepath.Join(gitDirectory, "HEAD"))
		if headErr != nil {
			pointer, pointerErr := os.ReadFile(gitDirectory)
			if pointerErr == nil {
				value := strings.TrimSpace(string(pointer))
				if strings.HasPrefix(value, "gitdir: ") {
					gitDirectory = strings.TrimSpace(strings.TrimPrefix(value, "gitdir: "))
					if !filepath.IsAbs(gitDirectory) {
						gitDirectory = filepath.Join(directory, gitDirectory)
					}
					head, headErr = os.ReadFile(filepath.Join(gitDirectory, "HEAD"))
				}
			}
		}
		if headErr == nil {
			value := strings.TrimSpace(string(head))
			if !strings.HasPrefix(value, "ref: ") {
				return value
			}
			reference := strings.TrimSpace(strings.TrimPrefix(value, "ref: "))
			if revision, err := os.ReadFile(filepath.Join(gitDirectory, filepath.FromSlash(reference))); err == nil {
				return strings.TrimSpace(string(revision))
			}
			if packed, err := os.ReadFile(filepath.Join(gitDirectory, "packed-refs")); err == nil {
				for _, line := range strings.Split(string(packed), "\n") {
					fields := strings.Fields(line)
					if len(fields) == 2 && fields[1] == reference {
						return fields[0]
					}
				}
			}
			return ""
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return ""
		}
		directory = parent
	}
}

func faultSeed(defaultSeed uint64) (uint64, error) {
	value := os.Getenv("EPAXOS_FAULT_SEED")
	if value == "" {
		return defaultSeed, nil
	}
	seed, err := strconv.ParseUint(value, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("parse EPAXOS_FAULT_SEED=%q: %w", value, err)
	}
	return seed, nil
}

func (h *faultSimHarness) trace(oracle faultOracleResult) faultTrace {
	trace := faultTrace{
		Version:        faultTraceVersion,
		SourceRevision: faultSourceRevision(),
		Config:         h.cfg,
		Seed:           h.cfg.Seed,
		TerminalHash:   h.terminalHash(),
		Oracle:         oracle,
		Counters:       make(map[string]uint64, len(h.counters)),
	}
	trace.Actions = make([]faultSimAction, len(h.actions))
	for i, action := range h.actions {
		trace.Actions[i] = action.clone()
	}
	trace.Operations = append([]faultHistoryOperation(nil), h.history...)
	trace.Receipts = append([]faultActionReceipt(nil), h.receipts...)
	for name, count := range h.counters {
		trace.Counters[name] = count
	}
	return trace
}

func faultMarshalTrace(trace faultTrace) ([]byte, error) {
	if trace.Version != faultTraceVersion {
		return nil, fmt.Errorf("fault trace version %d, want %d", trace.Version, faultTraceVersion)
	}
	if trace.SourceRevision == "" {
		return nil, fmt.Errorf("fault trace source revision is empty")
	}
	if trace.Config.Size < 1 || trace.Config.Size > 7 || trace.Seed != trace.Config.Seed {
		return nil, fmt.Errorf("fault trace configuration is inconsistent: %#v", trace.Config)
	}
	if trace.TerminalHash == "" {
		return nil, fmt.Errorf("fault trace terminal hash is empty")
	}
	return json.MarshalIndent(trace, "", "  ")
}

func faultUnmarshalTrace(data []byte) (faultTrace, error) {
	var trace faultTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&trace); err != nil {
		return trace, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return trace, fmt.Errorf("fault trace contains trailing JSON values")
		}
		return trace, fmt.Errorf("fault trace trailing data: %w", err)
	}
	if trace.Version != faultTraceVersion {
		return trace, fmt.Errorf("fault trace version %d is unsupported", trace.Version)
	}
	if trace.SourceRevision == "" || trace.TerminalHash == "" {
		return trace, fmt.Errorf("fault trace is missing required evidence fields")
	}
	if trace.Config.Size < 1 || trace.Config.Size > 7 || trace.Seed != trace.Config.Seed {
		return trace, fmt.Errorf("fault trace configuration is inconsistent")
	}
	return trace, nil
}

func faultReplayTrace(trace faultTrace) (*faultSimHarness, error) {
	if trace.Version != faultTraceVersion {
		return nil, fmt.Errorf("cannot replay fault trace version %d", trace.Version)
	}
	h, err := newFaultSimHarness(trace.Config)
	if err != nil {
		return nil, err
	}
	for index, action := range trace.Actions {
		if _, err := h.action(action); err != nil {
			return nil, fmt.Errorf("replay action %d (%s): %w", index, action.Kind, err)
		}
	}
	if len(trace.Receipts) != 0 && !reflect.DeepEqual(h.receipts, trace.Receipts) {
		return nil, fmt.Errorf("replay receipts differ\n got: %#v\nwant: %#v", h.receipts, trace.Receipts)
	}
	if trace.Counters != nil && !reflect.DeepEqual(h.counters, trace.Counters) {
		return nil, fmt.Errorf("replay counters differ\n got: %#v\nwant: %#v", h.counters, trace.Counters)
	}
	if len(trace.Operations) != 0 && !reflect.DeepEqual(h.history, trace.Operations) {
		return nil, fmt.Errorf("replay client histories differ\n got: %#v\nwant: %#v", h.history, trace.Operations)
	}
	if got := h.terminalHash(); got != trace.TerminalHash {
		return nil, fmt.Errorf("replay terminal hash %s, want %s", got, trace.TerminalHash)
	}
	return h, nil
}

func faultWriteTrace(path string, trace faultTrace) error {
	data, err := faultMarshalTrace(trace)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func faultReadTrace(path string) (faultTrace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return faultTrace{}, err
	}
	return faultUnmarshalTrace(data)
}

func faultActionIsClientOrFault(action faultSimAction) bool {
	switch action.Kind {
	case faultActionPropose, faultActionConfChange,
		faultActionDrop, faultActionDuplicate, faultActionHold, faultActionRelease,
		faultActionCorrupt, faultActionPartition, faultActionHeal, faultActionCrash,
		faultActionStorageFailure, faultActionPause, faultActionResume,
		faultActionSetClock, faultActionInjectMalformed:
		return true
	default:
		return false
	}
}

func faultMinimizationCandidate(trace faultTrace, actions []faultSimAction) faultTrace {
	candidate := trace
	candidate.Actions = make([]faultSimAction, len(actions))
	for i, action := range actions {
		candidate.Actions[i] = action.clone()
	}
	candidate.Operations = nil
	candidate.Receipts = nil
	candidate.Counters = nil
	candidate.TerminalHash = "synthetic-pending"
	candidate.Oracle = faultOracleResult{}
	return candidate
}

func faultMinimizationValid(actions []faultSimAction) bool {
	witnesses := 0
	for _, action := range actions {
		if action.Kind == faultActionPropose {
			witnesses++
		}
	}
	return witnesses > 0
}

func faultMinimizeTrace(trace faultTrace, failure func(faultTrace) bool) faultTrace {
	current := make([]faultSimAction, len(trace.Actions))
	for i, action := range trace.Actions {
		current[i] = action.clone()
	}
	for chunk := len(current) / 2; chunk >= 1; {
		removed := false
		for start := 0; start < len(current); start += chunk {
			end := start + chunk
			if end > len(current) {
				end = len(current)
			}
			containsRequired := false
			for _, action := range current[start:end] {
				if action.Required {
					containsRequired = true
					break
				}
			}
			if containsRequired {
				continue
			}
			candidateActions := make([]faultSimAction, 0, len(current)-(end-start))
			candidateActions = append(candidateActions, current[:start]...)
			candidateActions = append(candidateActions, current[end:]...)
			if !faultMinimizationValid(candidateActions) {
				continue
			}
			candidate := faultMinimizationCandidate(trace, candidateActions)
			if failure(candidate) {
				current = candidateActions
				removed = true
				break
			}
		}
		if !removed {
			chunk /= 2
		}
	}
	for index := 0; index < len(current); {
		action := current[index]
		if action.Required || !faultActionIsClientOrFault(action) {
			index++
			continue
		}
		candidateActions := make([]faultSimAction, 0, len(current)-1)
		candidateActions = append(candidateActions, current[:index]...)
		candidateActions = append(candidateActions, current[index+1:]...)
		if faultMinimizationValid(candidateActions) && failure(faultMinimizationCandidate(trace, candidateActions)) {
			current = candidateActions
			continue
		}
		index++
	}
	return faultMinimizationCandidate(trace, current)
}

func faultMustAction(t *testing.T, h *faultSimHarness, action faultSimAction) faultActionReceipt {
	t.Helper()
	receipt, err := h.action(action)
	if err != nil {
		t.Fatalf("fault action %s: %v", action.Kind, err)
	}
	return receipt
}

func faultRoundTripScenario(t *testing.T, seed uint64) (*faultSimHarness, faultOracleResult) {
	t.Helper()
	h, err := newFaultSimHarness(faultSimConfig{Size: 3, Seed: seed})
	if err != nil {
		t.Fatal(err)
	}
	faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})
	op := faultClientOperation{Client: 70, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "trace", Value: "stable"}}}
	faultMustAction(t, h, faultSimAction{Kind: faultActionPropose, Node: 1, Operation: &op, Required: true})
	faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
	pending := h.pendingEnvelopeIDs(false)
	if len(pending) < 2 {
		t.Fatalf("roundtrip scenario queued envelopes %v, want at least two", pending)
	}
	faultMustAction(t, h, faultSimAction{Kind: faultActionHold, Envelope: pending[0]})
	faultMustAction(t, h, faultSimAction{Kind: faultActionDuplicate, Envelope: pending[1], Count: 1})
	duplicateID := h.nextEnvelope
	faultMustAction(t, h, faultSimAction{Kind: faultActionDeliver, Envelope: duplicateID})
	faultMustAction(t, h, faultSimAction{Kind: faultActionDeliver, Envelope: pending[1]})
	faultMustAction(t, h, faultSimAction{Kind: faultActionRelease, Envelope: pending[0]})
	ref, ok := h.refFor(op.commandID())
	if !ok {
		t.Fatal("roundtrip proposal ref is absent")
	}
	if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, 80); err != nil {
		t.Fatal(err)
	}
	oracle := faultCheckOracle(h, h.ids)
	if !oracle.OK {
		t.Fatalf("roundtrip oracle failed: %#v", oracle)
	}
	return h, oracle
}

func TestFaultTraceMarshalReplay(t *testing.T) {
	h, oracle := faultRoundTripScenario(t, 0x5eed)
	trace := h.trace(oracle)
	data, err := faultMarshalTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := faultUnmarshalTrace(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(trace, decoded) {
		t.Fatalf("trace JSON roundtrip differs\n got: %#v\nwant: %#v", decoded, trace)
	}
	replayed, err := faultReplayTrace(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := replayed.terminalHash(); got != trace.TerminalHash {
		t.Fatalf("replay terminal hash %s, want %s", got, trace.TerminalHash)
	}
	if replayed.counters["duplicate"] == 0 || replayed.counters["hold"] == 0 || replayed.counters["release"] == 0 || replayed.counters["reorder"] == 0 {
		t.Fatalf("roundtrip did not retain exact fault counters: %#v", replayed.counters)
	}
}

func TestFaultTraceMinimization(t *testing.T) {
	witness := faultClientOperation{Client: 99, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "witness", Value: "failure"}}}
	trace := faultTrace{
		Version: faultTraceVersion, SourceRevision: "synthetic", Config: faultSimConfig{Size: 3, Seed: 17}.normalized(), Seed: 17,
		TerminalHash: "synthetic",
		Actions: []faultSimAction{
			{Kind: faultActionPump, Required: true},
			{Kind: faultActionTick, Node: 1},
			{Kind: faultActionPropose, Node: 1, Operation: &witness, Required: true},
			{Kind: faultActionDuplicate, Envelope: 8},
			{Kind: faultActionCorrupt, Envelope: 9, Mutation: "truncate"},
			{Kind: faultActionDrop, Envelope: 10},
			{Kind: faultActionTick, Node: 2},
		},
	}
	syntheticFailure := func(candidate faultTrace) bool {
		hasWitness := false
		hasCorruption := false
		for _, action := range candidate.Actions {
			if action.Kind == faultActionPropose && action.Operation != nil && action.Operation.Client == 99 {
				hasWitness = true
			}
			if action.Kind == faultActionCorrupt {
				hasCorruption = true
			}
		}
		return hasWitness && hasCorruption
	}
	if !syntheticFailure(trace) {
		t.Fatal("synthetic trace does not fail its intentionally failing oracle")
	}
	minimized := faultMinimizeTrace(trace, syntheticFailure)
	if len(minimized.Actions) >= len(trace.Actions) {
		t.Fatalf("minimizer kept %d actions from %d, want a strict reduction", len(minimized.Actions), len(trace.Actions))
	}
	if !syntheticFailure(minimized) {
		t.Fatalf("minimized actions no longer reproduce synthetic failure: %#v", minimized.Actions)
	}
	if !faultMinimizationValid(minimized.Actions) {
		t.Fatal("minimizer removed every witness client operation")
	}
	artifactDir := t.TempDir()
	originalPath := filepath.Join(artifactDir, "failure.json")
	minimizedPath := filepath.Join(artifactDir, "failure.min.json")
	if err := faultWriteTrace(originalPath, trace); err != nil {
		t.Fatal(err)
	}
	if err := faultWriteTrace(minimizedPath, minimized); err != nil {
		t.Fatal(err)
	}
	reloaded, err := faultReadTrace(minimizedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reloaded.Actions, minimized.Actions) {
		t.Fatalf("retained minimized artifact actions differ: got %#v want %#v", reloaded.Actions, minimized.Actions)
	}
}

func TestFaultCampaignReplay(t *testing.T) {
	path := os.Getenv("EPAXOS_FAULT_TRACE")
	if path == "" {
		h, oracle := faultRoundTripScenario(t, 0x51a7e)
		trace := h.trace(oracle)
		if _, err := faultReplayTrace(trace); err != nil {
			t.Fatal(err)
		}
		return
	}
	trace, err := faultReadTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := faultReplayTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	result := faultCheckOracle(replayed, faultOracleExpectedNodes(replayed))
	if !result.OK {
		t.Fatalf("replayed trace safety oracle failed: %#v", result)
	}
}
