package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func validTestTrace(size int) FaultTrace {
	state := map[string]string{"key": "value"}
	trace := FaultTrace{
		Version: traceVersion, SourceRevision: strings.Repeat("a", 40), Size: size, Seed: 0x5eed,
		Profiles: []string{"duplicate"},
		Operations: []TraceOperation{{ID: 1, Kind: string(OpPut), Node: 1, Key: "key", Value: "value"}, {ID: 2, Kind: string(OpGet), Node: 1, Key: "key"}},
		Actions: []TraceAction{{ID: 1, Kind: "duplicate", Node: 1, From: 1, To: 2, Applicable: true}},
		Receipts: []TraceReceipt{{ID: 1, ActionID: 1, Kind: "duplicate", Count: 1, Applicable: true}},
		TerminalHashes: map[string]string{"node-1": "hash"}, TerminalDigest: terminalStateDigest(state), OracleResult: "pass",
	}
	if size == 1 {
		trace.Actions[0].Applicable = false
		trace.Actions[0].Reason = "no remote link"
		trace.Receipts[0].Applicable = false
		trace.Receipts[0].Reason = "no remote link"
	}
	return trace
}

func TestTraceRoundTripAndReplay(t *testing.T) {
	trace := validTestTrace(3)
	path := filepath.Join(t.TempDir(), "trace.json")
	if err := writeTraceDurable(path, trace); err != nil {
		t.Fatal(err)
	}
	loaded, err := readTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, trace) {
		t.Fatalf("round trip changed trace:\nloaded=%#v\nwant=%#v", loaded, trace)
	}
	digest, err := replayTraceModel(loaded)
	if err != nil || digest != trace.TerminalDigest {
		t.Fatalf("replay digest=%s err=%v", digest, err)
	}
	loaded.TerminalDigest = strings.Repeat("0", 64)
	if digest, err := replayTraceModel(loaded); err == nil || digest == loaded.TerminalDigest {
		t.Fatalf("replay failed to report mismatch: digest=%s err=%v", digest, err)
	}
}

func TestTraceReplayAppliesAtomicTransaction(t *testing.T) {
	members := []TxnOperation{{Key: "a", Value: "one"}, {Key: "b", Value: "two"}, {Delete: true, Key: "a"}}
	trace := validTestTrace(3)
	trace.Operations = []TraceOperation{{ID: 1, Kind: string(OpTxn), Node: 1, Value: encodeTxnForTrace(members)}}
	trace.TerminalDigest = terminalStateDigest(map[string]string{"b": "two"})
	if digest, err := replayTraceModel(trace); err != nil || digest != trace.TerminalDigest {
		t.Fatalf("transaction replay digest=%s err=%v", digest, err)
	}
}

func TestTraceRequiresExplicitSingleNodePeerNonApplicability(t *testing.T) {
	trace := validTestTrace(1)
	if err := validateTrace(trace); err != nil {
		t.Fatalf("explicit single-node non-applicability rejected: %v", err)
	}
	trace.Actions[0].Applicable = true
	trace.Actions[0].Reason = ""
	trace.Receipts[0].Applicable = true
	trace.Receipts[0].Reason = ""
	if err := validateTrace(trace); err == nil {
		t.Fatal("single-node peer fault was accepted as applicable")
	}
}

func TestTraceValidatesActionReceiptIntegrity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*FaultTrace)
	}{
		{name: "version", mutate: func(trace *FaultTrace) { trace.Version = "v0" }},
		{name: "size", mutate: func(trace *FaultTrace) { trace.Size = 8 }},
		{name: "operation order", mutate: func(trace *FaultTrace) { trace.Operations[1].ID = 1 }},
		{name: "action zero", mutate: func(trace *FaultTrace) { trace.Actions[0].ID = 0 }},
		{name: "receipt count", mutate: func(trace *FaultTrace) { trace.Receipts[0].Count = 0 }},
		{name: "receipt reference", mutate: func(trace *FaultTrace) { trace.Receipts[0].ActionID = 99 }},
		{name: "receipt kind", mutate: func(trace *FaultTrace) { trace.Receipts[0].Kind = "loss" }},
		{name: "receipt applicability", mutate: func(trace *FaultTrace) { trace.Receipts[0].Applicable = false }},
		{name: "empty operations", mutate: func(trace *FaultTrace) { trace.Operations = nil }},
		{name: "empty digest", mutate: func(trace *FaultTrace) { trace.TerminalDigest = "" }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			trace := validTestTrace(3)
			testCase.mutate(&trace)
			if err := validateTrace(trace); err == nil {
				t.Fatalf("invalid trace accepted: %#v", trace)
			}
		})
	}
}

func TestTraceMinimizationKeepsSyntheticWitness(t *testing.T) {
	trace := validTestTrace(3)
	trace.Operations = []TraceOperation{
		{ID: 1, Kind: string(OpPut), Node: 1, Key: "noise-a", Value: "a"},
		{ID: 2, Kind: string(OpPut), Node: 1, Key: "witness", Value: "bad"},
		{ID: 3, Kind: string(OpPut), Node: 1, Key: "noise-b", Value: "b"},
	}
	trace.Actions = []TraceAction{
		{ID: 1, Kind: "noise-a", Applicable: true},
		{ID: 2, Kind: "witness", Applicable: true},
		{ID: 3, Kind: "noise-b", Applicable: true},
	}
	trace.Receipts = []TraceReceipt{
		{ID: 1, ActionID: 1, Kind: "noise-a", Count: 1, Applicable: true},
		{ID: 2, ActionID: 2, Kind: "witness", Count: 1, Applicable: true},
		{ID: 3, ActionID: 3, Kind: "noise-b", Count: 1, Applicable: true},
	}
	fails := func(candidate FaultTrace) bool {
		hasOperation := false
		for _, operation := range candidate.Operations {
			hasOperation = hasOperation || operation.Key == "witness"
		}
		hasAction := false
		for _, action := range candidate.Actions {
			hasAction = hasAction || action.Kind == "witness"
		}
		return hasOperation && hasAction
	}
	minimized := minimizeTrace(trace, fails)
	if !fails(minimized) || validateTrace(minimized) != nil {
		t.Fatalf("minimized trace lost witness or validity: %#v", minimized)
	}
	if len(minimized.Operations) >= len(trace.Operations) || len(minimized.Actions) >= len(trace.Actions) {
		t.Fatalf("minimizer did not shrink trace: original=%d/%d minimized=%d/%d", len(trace.Operations), len(trace.Actions), len(minimized.Operations), len(minimized.Actions))
	}
	if len(minimized.Receipts) != len(minimized.Actions) {
		t.Fatalf("minimized action receipts are not paired: %#v", minimized)
	}
}

func TestReadTraceRejectsTrailingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.json")
	if err := writeTraceDurable(path, validTestTrace(3)); err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // G304: path is controlled test path
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("{}\n"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := readTrace(path); err == nil {
		t.Fatal("readTrace accepted trailing JSON")
	}
}
