package main

import (
	"bytes"
	"errors"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const traceVersion = "faultcampaign/v1"

type TraceOperation struct {
	ID    uint64 `json:"id"`
	Kind  string `json:"kind"`
	Node  int    `json:"node"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type TraceAction struct {
	ID         uint64 `json:"id"`
	Kind       string `json:"kind"`
	Node       int    `json:"node,omitempty"`
	From       uint64 `json:"from,omitempty"`
	To         uint64 `json:"to,omitempty"`
	Applicable bool   `json:"applicable"`
	Reason     string `json:"reason,omitempty"`
}

type TraceReceipt struct {
	ID         uint64 `json:"id"`
	ActionID   uint64 `json:"action_id"`
	Kind       string `json:"kind"`
	Count      uint64 `json:"count"`
	Applicable bool   `json:"applicable"`
	Reason     string `json:"reason,omitempty"`
}

type FaultTrace struct {
	Version         string            `json:"version"`
	SourceRevision  string            `json:"source_revision"`
	Size            int               `json:"size"`
	Seed            uint64            `json:"seed"`
	Profiles        []string          `json:"profiles"`
	Operations      []TraceOperation  `json:"operations"`
	Actions         []TraceAction     `json:"actions"`
	Receipts        []TraceReceipt    `json:"receipts"`
	TerminalHashes  map[string]string `json:"terminal_hashes"`
	TerminalDigest  string            `json:"terminal_digest"`
	OracleResult    string            `json:"oracle_result"`
	Nondeterminism  string            `json:"nondeterminism,omitempty"`
}

func validateTrace(trace FaultTrace) error {
	if trace.Version != traceVersion {
		return fmt.Errorf("trace version %q, want %q", trace.Version, traceVersion)
	}
	if strings.TrimSpace(trace.SourceRevision) == "" {
		return fmt.Errorf("trace source revision is empty")
	}
	if trace.Size < 1 || trace.Size > 7 {
		return fmt.Errorf("trace size %d is outside 1..7", trace.Size)
	}
	if len(trace.Profiles) == 0 {
		return fmt.Errorf("trace profiles are empty")
	}
	for i, profile := range trace.Profiles {
		if strings.TrimSpace(profile) == "" {
			return fmt.Errorf("trace profile %d is empty", i)
		}
		if i > 0 && trace.Profiles[i-1] >= profile {
			return fmt.Errorf("trace profiles must be sorted and unique")
		}
	}
	if len(trace.Operations) == 0 {
		if trace.OracleResult != "fail" {
			return fmt.Errorf("passing trace operations are empty")
		}
	} else if err := validateTraceOperationIDs(trace.Operations); err != nil {
		return err
	}
	if len(trace.Actions) == 0 {
		return fmt.Errorf("trace actions are empty")
	}
	if err := validateTraceActionIDs(trace.Actions); err != nil {
		return err
	}
	if len(trace.Receipts) == 0 {
		return fmt.Errorf("trace receipts are empty")
	}
	if err := validateTraceReceiptIDs(trace.Receipts); err != nil {
		return err
	}
	byAction := make(map[uint64]TraceReceipt, len(trace.Receipts))
	for _, receipt := range trace.Receipts {
		if receipt.Count == 0 {
			return fmt.Errorf("receipt %d has zero count", receipt.ID)
		}
		if _, exists := byAction[receipt.ActionID]; exists {
			return fmt.Errorf("action %d has multiple receipts", receipt.ActionID)
		}
		byAction[receipt.ActionID] = receipt
	}
	for _, action := range trace.Actions {
		receipt, ok := byAction[action.ID]
		if !ok {
			return fmt.Errorf("action %d has no receipt", action.ID)
		}
		if receipt.Kind != action.Kind {
			return fmt.Errorf("action %d kind %q does not match receipt kind %q", action.ID, action.Kind, receipt.Kind)
		}
		if receipt.Applicable != action.Applicable {
			return fmt.Errorf("action %d applicability does not match receipt", action.ID)
		}
		if !action.Applicable {
			if strings.TrimSpace(action.Reason) == "" || strings.TrimSpace(receipt.Reason) == "" {
				return fmt.Errorf("non-applicable action %d requires action and receipt reasons", action.ID)
			}
		}
		if trace.Size == 1 && isPeerFault(action.Kind) {
			if action.Applicable || strings.TrimSpace(action.Reason) == "" {
				return fmt.Errorf("single-node peer fault %q must be explicitly non-applicable", action.Kind)
			}
		}
		delete(byAction, action.ID)
	}
	if len(byAction) != 0 {
		return fmt.Errorf("trace contains a receipt for an unknown action")
	}
	if strings.TrimSpace(trace.TerminalDigest) == "" {
		return fmt.Errorf("trace terminal digest is empty")
	}
	if strings.TrimSpace(trace.OracleResult) == "" {
		return fmt.Errorf("trace oracle result is empty")
	}
	if trace.TerminalHashes == nil {
		return fmt.Errorf("trace terminal hashes are absent")
	}
	return nil
}

func validateTraceOperationIDs(operations []TraceOperation) error {
	var prior uint64
	for i, operation := range operations {
		if operation.ID == 0 || (i > 0 && operation.ID <= prior) {
			return fmt.Errorf("trace operation IDs must be nonzero, sorted, and unique")
		}
		prior = operation.ID
		if operation.Node < 1 || operation.Node > 7 {
			return fmt.Errorf("operation %d has invalid node %d", operation.ID, operation.Node)
		}
		switch operation.Kind {
		case string(OpPut), string(OpGet), string(OpDelete):
			if operation.Key == "" {
				return fmt.Errorf("operation %d requires a key", operation.ID)
			}
		case string(OpTxn):
			if operation.Value == "" {
				return fmt.Errorf("transaction operation %d has no encoded members", operation.ID)
			}
		default:
			return fmt.Errorf("operation %d has unsupported kind %q", operation.ID, operation.Kind)
		}
	}
	return nil
}

func validateTraceActionIDs(actions []TraceAction) error {
	var prior uint64
	for i, action := range actions {
		if action.ID == 0 || (i > 0 && action.ID <= prior) {
			return fmt.Errorf("trace action IDs must be nonzero, sorted, and unique")
		}
		prior = action.ID
		if strings.TrimSpace(action.Kind) == "" {
			return fmt.Errorf("action %d kind is empty", action.ID)
		}
	}
	return nil
}

func validateTraceReceiptIDs(receipts []TraceReceipt) error {
	var prior uint64
	for i, receipt := range receipts {
		if receipt.ID == 0 || (i > 0 && receipt.ID <= prior) {
			return fmt.Errorf("trace receipt IDs must be nonzero, sorted, and unique")
		}
		if receipt.ActionID == 0 {
			return fmt.Errorf("receipt %d has zero action ID", receipt.ID)
		}
		prior = receipt.ID
	}
	return nil
}

func isPeerFault(kind string) bool {
	switch kind {
	case "loss", "duplicate", "reorder", "asymmetric-partition":
		return true
	default:
		return false
	}
}

func readTrace(path string) (FaultTrace, error) {
	//nolint:gosec // G304: path is controlled trace path
	file, err := os.Open(path)
	if err != nil {
		return FaultTrace{}, err
	}
	defer func() {
		_ = file.Close()
	}()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var trace FaultTrace
	if err := decoder.Decode(&trace); err != nil {
		return FaultTrace{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return FaultTrace{}, fmt.Errorf("trace contains trailing JSON values")
		}
		return FaultTrace{}, fmt.Errorf("trace has malformed trailing content: %w", err)
	}
	if err := validateTrace(trace); err != nil {
		return FaultTrace{}, err
	}
	return trace, nil
}

func writeTraceDurable(path string, trace FaultTrace) error {
	if err := validateTrace(trace); err != nil {
		return err
	}
	return writeJSONDurable(path, trace)
}

func replayTraceModel(trace FaultTrace) (string, error) {
	if err := validateTrace(trace); err != nil {
		return "", err
	}
	state := make(map[string]string)
	for _, operation := range trace.Operations {
		switch OperationKind(operation.Kind) {
		case OpPut:
			state[operation.Key] = operation.Value
		case OpDelete:
			delete(state, operation.Key)
		case OpGet:
			// Reads do not alter the deterministic terminal model.
		case OpTxn:
			var members []TxnOperation
			if err := json.Unmarshal([]byte(operation.Value), &members); err != nil {
				return "", fmt.Errorf("decode transaction operation %d: %w", operation.ID, err)
			}
			if len(members) == 0 {
				return "", fmt.Errorf("transaction operation %d is empty", operation.ID)
			}
			for _, member := range members {
				if member.Key == "" {
					return "", fmt.Errorf("transaction operation %d has an empty key", operation.ID)
				}
				if member.Delete {
					delete(state, member.Key)
				} else {
					state[member.Key] = member.Value
				}
			}
		}
	}
	digest := terminalStateDigest(state)
	if digest != trace.TerminalDigest {
		return digest, fmt.Errorf("replay terminal digest %s does not match trace %s", digest, trace.TerminalDigest)
	}
	return digest, nil
}

func minimizeTrace(input FaultTrace, fails func(FaultTrace) bool) FaultTrace {
	if fails == nil || validateTrace(input) != nil || !fails(input) {
		return input
	}
	current := cloneTrace(input)
	units := traceUnits(current)
	for chunk := len(units) / 2; chunk >= 1; chunk /= 2 {
		changed := true
		for changed {
			changed = false
			units = traceUnits(current)
			for start := 0; start+chunk <= len(units); start++ {
				candidate := removeTraceUnits(current, units[start:start+chunk])
				if validateTrace(candidate) == nil && fails(candidate) {
					current = candidate
					changed = true
					break
				}
			}
		}
		if chunk == 1 {
			break
		}
	}
	return current
}

type traceUnit struct {
	kind string
	id   uint64
}

func traceUnits(trace FaultTrace) []traceUnit {
	units := make([]traceUnit, 0, len(trace.Operations)+len(trace.Actions))
	for _, operation := range trace.Operations {
		units = append(units, traceUnit{kind: "operation", id: operation.ID})
	}
	for _, action := range trace.Actions {
		units = append(units, traceUnit{kind: "action", id: action.ID})
	}
	return units
}

func removeTraceUnits(trace FaultTrace, removed []traceUnit) FaultTrace {
	out := cloneTrace(trace)
	operations := make(map[uint64]struct{})
	actions := make(map[uint64]struct{})
	for _, unit := range removed {
		if unit.kind == "operation" {
			operations[unit.id] = struct{}{}
		} else {
			actions[unit.id] = struct{}{}
		}
	}
	out.Operations = out.Operations[:0]
	for _, operation := range trace.Operations {
		if _, drop := operations[operation.ID]; !drop {
			out.Operations = append(out.Operations, operation)
		}
	}
	out.Actions = out.Actions[:0]
	for _, action := range trace.Actions {
		if _, drop := actions[action.ID]; !drop {
			out.Actions = append(out.Actions, action)
		}
	}
	out.Receipts = out.Receipts[:0]
	for _, receipt := range trace.Receipts {
		if _, drop := actions[receipt.ActionID]; !drop {
			out.Receipts = append(out.Receipts, receipt)
		}
	}
	return out
}

func cloneTrace(trace FaultTrace) FaultTrace {
	out := trace
	out.Profiles = append([]string(nil), trace.Profiles...)
	out.Operations = append([]TraceOperation(nil), trace.Operations...)
	out.Actions = append([]TraceAction(nil), trace.Actions...)
	out.Receipts = append([]TraceReceipt(nil), trace.Receipts...)
	out.TerminalHashes = make(map[string]string, len(trace.TerminalHashes))
	for key, value := range trace.TerminalHashes {
		out.TerminalHashes[key] = value
	}
	return out
}

func sortTrace(trace *FaultTrace) {
	sort.Strings(trace.Profiles)
	sort.Slice(trace.Operations, func(i, j int) bool { return trace.Operations[i].ID < trace.Operations[j].ID })
	sort.Slice(trace.Actions, func(i, j int) bool { return trace.Actions[i].ID < trace.Actions[j].ID })
	sort.Slice(trace.Receipts, func(i, j int) bool { return trace.Receipts[i].ID < trace.Receipts[j].ID })
}

func encodeTxnForTrace(members []TxnOperation) string {
	payload, _ := json.Marshal(members)
	return string(bytes.TrimSpace(payload))
}
