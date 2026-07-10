package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// OperationKind is a client-observable operation supported by kvnode.
type OperationKind string

const (
	OpPut    OperationKind = "put"
	OpGet    OperationKind = "get"
	OpDelete OperationKind = "delete"
	OpTxn    OperationKind = "txn"
)

// ResultKind distinguishes acknowledged, rejected, and indeterminate requests.
type ResultKind string

const (
	ResultOK      ResultKind = "ok"
	ResultFail    ResultKind = "fail"
	ResultUnknown ResultKind = "unknown"
)

// TxnOperation is one atomic transaction member.
type TxnOperation struct {
	Delete bool   `json:"delete,omitempty"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
}

// HistoryEvent records invocation and completion in a campaign-local logical clock.
type HistoryEvent struct {
	ID         uint64         `json:"id"`
	Kind       OperationKind  `json:"kind"`
	Node       int            `json:"node"`
	Start      uint64         `json:"start"`
	End        uint64         `json:"end"`
	Result     ResultKind     `json:"result"`
	HTTPStatus int            `json:"http_status,omitempty"`
	Key        string         `json:"key,omitempty"`
	Value      string         `json:"value,omitempty"`
	Found      bool           `json:"found,omitempty"`
	Txn        []TxnOperation `json:"txn,omitempty"`
}

// CheckerResult is the durable, client-observable safety result.
type CheckerResult struct {
	Valid          bool              `json:"valid"`
	Reads          int               `json:"reads"`
	Mutations      int               `json:"mutations"`
	Acknowledged   int               `json:"acknowledged"`
	Terminal       map[string]string `json:"terminal"`
	Linearization  []uint64          `json:"linearization,omitempty"`
	Error          string            `json:"error,omitempty"`
	TerminalDigest string            `json:"terminal_digest,omitempty"`
}

const maxHistoryEvents = 64

func checkHistory(events []HistoryEvent, minReads, minMutations int) CheckerResult {
	result := CheckerResult{Terminal: make(map[string]string)}
	if minReads < 0 || minMutations < 0 {
		result.Error = "minimum operation counts must be nonnegative"
		return result
	}
	if len(events) > maxHistoryEvents {
		result.Error = fmt.Sprintf("history has %d events, limit is %d", len(events), maxHistoryEvents)
		return result
	}
	copied := cloneHistory(events)
	seen := make(map[uint64]struct{}, len(copied))
	for i := range copied {
		e := &copied[i]
		if e.ID == 0 {
			result.Error = "history event ID must be nonzero"
			return result
		}
		if _, exists := seen[e.ID]; exists {
			result.Error = fmt.Sprintf("duplicate history event ID %d", e.ID)
			return result
		}
		seen[e.ID] = struct{}{}
		if e.Start >= e.End {
			result.Error = fmt.Sprintf("event %d has invalid interval [%d,%d]", e.ID, e.Start, e.End)
			return result
		}
		if e.Node < 1 || e.Node > 7 {
			result.Error = fmt.Sprintf("event %d has invalid node %d", e.ID, e.Node)
			return result
		}
		if err := validateHistoryEvent(*e); err != nil {
			result.Error = fmt.Sprintf("event %d: %v", e.ID, err)
			return result
		}
		if e.Result == ResultOK {
			result.Acknowledged++
			if e.Kind == OpGet {
				result.Reads++
			} else {
				result.Mutations++
			}
		}
	}
	if result.Reads < minReads {
		result.Error = fmt.Sprintf("successful reads=%d, require at least %d", result.Reads, minReads)
		return result
	}
	if result.Mutations < minMutations {
		result.Error = fmt.Sprintf("successful mutations=%d, require at least %d", result.Mutations, minMutations)
		return result
	}

	order := make([]int, len(copied))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return copied[order[i]].ID < copied[order[j]].ID })
	var predecessors [maxHistoryEvents]uint64
	for i := range copied {
		for j := range copied {
			if i != j && copied[j].End < copied[i].Start {
				predecessors[i] |= uint64(1) << uint(j)
			}
		}
	}
	state := make(map[string]string)
	linear := make([]uint64, 0, len(copied))
	memo := make(map[string]struct{})
	if !linearizeHistory(copied, order, predecessors[:len(copied)], 0, state, &linear, memo) {
		result.Error = "history has no legal sequential execution"
		return result
	}
	result.Valid = true
	result.Terminal = cloneState(state)
	result.Linearization = append([]uint64(nil), linear...)
	result.TerminalDigest = terminalStateDigest(state)
	return result
}

func validateHistoryEvent(e HistoryEvent) error {
	switch e.Result {
	case ResultOK, ResultFail, ResultUnknown:
	default:
		return fmt.Errorf("unsupported result %q", e.Result)
	}
	switch e.Kind {
	case OpPut, OpGet, OpDelete:
		if e.Key == "" {
			return fmt.Errorf("%s requires a key", e.Kind)
		}
		if len(e.Txn) != 0 {
			return fmt.Errorf("%s must not carry transaction members", e.Kind)
		}
	case OpTxn:
		if len(e.Txn) == 0 {
			return fmt.Errorf("transaction must have at least one member")
		}
		if e.Key != "" {
			return fmt.Errorf("transaction must not carry a top-level key")
		}
		for i, op := range e.Txn {
			if op.Key == "" {
				return fmt.Errorf("transaction member %d has an empty key", i)
			}
		}
	default:
		return fmt.Errorf("unsupported operation %q", e.Kind)
	}
	return nil
}

func linearizeHistory(events []HistoryEvent, order []int, predecessors []uint64, done uint64, state map[string]string, linear *[]uint64, memo map[string]struct{}) bool {
	if len(*linear) == len(events) {
		return true
	}
	key := fmt.Sprintf("%016x:%s", done, terminalStateDigest(state))
	if _, failed := memo[key]; failed {
		return false
	}
	for _, index := range order {
		bit := uint64(1) << uint(index)
		if done&bit != 0 || predecessors[index]&^done != 0 {
			continue
		}
		e := events[index]
		branches := eventBranches(e, state)
		for _, next := range branches {
			*linear = append(*linear, e.ID)
			if linearizeHistory(events, order, predecessors, done|bit, next, linear, memo) {
				for stateKey := range state {
					delete(state, stateKey)
				}
				for stateKey, value := range next {
					state[stateKey] = value
				}
				return true
			}
			*linear = (*linear)[:len(*linear)-1]
		}
	}
	memo[key] = struct{}{}
	return false
}

func eventBranches(e HistoryEvent, state map[string]string) []map[string]string {
	if e.Result == ResultFail || (e.Result == ResultUnknown && e.Kind == OpGet) {
		return []map[string]string{cloneState(state)}
	}
	if e.Kind == OpGet {
		value, found := state[e.Key]
		if e.Result == ResultOK && (found != e.Found || (found && value != e.Value)) {
			return nil
		}
		return []map[string]string{cloneState(state)}
	}
	applied := cloneState(state)
	applyMutation(e, applied)
	if e.Result == ResultUnknown {
		return []map[string]string{cloneState(state), applied}
	}
	return []map[string]string{applied}
}

func applyMutation(e HistoryEvent, state map[string]string) {
	switch e.Kind {
	case OpPut:
		state[e.Key] = e.Value
	case OpDelete:
		delete(state, e.Key)
	case OpTxn:
		for _, op := range e.Txn {
			if op.Delete {
				delete(state, op.Key)
			} else {
				state[op.Key] = op.Value
			}
		}
	}
}

func cloneHistory(events []HistoryEvent) []HistoryEvent {
	out := append([]HistoryEvent(nil), events...)
	for i := range out {
		out[i].Txn = append([]TxnOperation(nil), events[i].Txn...)
	}
	return out
}

func cloneState(state map[string]string) map[string]string {
	out := make(map[string]string, len(state))
	for key, value := range state {
		out[key] = value
	}
	return out
}

func terminalStateDigest(state map[string]string) string {
	type pair struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]pair, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, pair{Key: key, Value: state[key]})
	}
	payload, _ := json.Marshal(pairs)
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}
