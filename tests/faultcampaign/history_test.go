package main

import (
	"reflect"
	"testing"
)

func TestHistoryCheckerPutGetDeleteAndAtomicTransaction(t *testing.T) {
	events := []HistoryEvent{
		{ID: 1, Kind: OpPut, Node: 1, Start: 1, End: 2, Result: ResultOK, Key: "alpha", Value: "one"},
		{ID: 2, Kind: OpGet, Node: 2, Start: 3, End: 4, Result: ResultOK, Key: "alpha", Found: true, Value: "one"},
		{ID: 3, Kind: OpTxn, Node: 1, Start: 5, End: 6, Result: ResultOK, Txn: []TxnOperation{{Key: "beta", Value: "two"}, {Delete: true, Key: "alpha"}}},
		{ID: 4, Kind: OpGet, Node: 3, Start: 7, End: 8, Result: ResultOK, Key: "beta", Found: true, Value: "two"},
		{ID: 5, Kind: OpDelete, Node: 2, Start: 9, End: 10, Result: ResultOK, Key: "beta"},
		{ID: 6, Kind: OpGet, Node: 1, Start: 11, End: 12, Result: ResultOK, Key: "beta", Found: false},
	}
	result := checkHistory(events, 3, 3)
	if !result.Valid {
		t.Fatalf("checker rejected legal history: %s", result.Error)
	}
	if result.Reads != 3 || result.Mutations != 3 || result.Acknowledged != 6 {
		t.Fatalf("unexpected counts: %#v", result)
	}
	if len(result.Terminal) != 0 {
		t.Fatalf("terminal state=%v, want empty", result.Terminal)
	}
	if !reflect.DeepEqual(result.Linearization, []uint64{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("linearization=%v", result.Linearization)
	}
}

func TestHistoryCheckerRejectsImpossibleStaleRead(t *testing.T) {
	result := checkHistory([]HistoryEvent{
		{ID: 1, Kind: OpPut, Node: 1, Start: 1, End: 2, Result: ResultOK, Key: "key", Value: "new"},
		{ID: 2, Kind: OpGet, Node: 2, Start: 3, End: 4, Result: ResultOK, Key: "key", Found: false},
	}, 1, 1)
	if result.Valid || result.Error == "" {
		t.Fatalf("checker accepted impossible history: %#v", result)
	}
}

func TestHistoryCheckerAllowsOverlappingReadBeforePut(t *testing.T) {
	result := checkHistory([]HistoryEvent{
		{ID: 1, Kind: OpPut, Node: 1, Start: 1, End: 4, Result: ResultOK, Key: "key", Value: "value"},
		{ID: 2, Kind: OpGet, Node: 2, Start: 2, End: 3, Result: ResultOK, Key: "key", Found: false},
	}, 1, 1)
	if !result.Valid {
		t.Fatalf("checker rejected overlapping legal history: %s", result.Error)
	}
	if !reflect.DeepEqual(result.Linearization, []uint64{2, 1}) {
		t.Fatalf("linearization=%v, want read before put", result.Linearization)
	}
}

func TestHistoryCheckerUnknownMutationIsOptional(t *testing.T) {
	applied := checkHistory([]HistoryEvent{
		{ID: 1, Kind: OpPut, Node: 1, Start: 1, End: 4, Result: ResultUnknown, Key: "key", Value: "value"},
		{ID: 2, Kind: OpGet, Node: 2, Start: 2, End: 3, Result: ResultOK, Key: "key", Found: true, Value: "value"},
	}, 1, 0)
	if !applied.Valid || applied.Terminal["key"] != "value" {
		t.Fatalf("unknown mutation could not be applied: %#v", applied)
	}
	omitted := checkHistory([]HistoryEvent{
		{ID: 1, Kind: OpPut, Node: 1, Start: 1, End: 2, Result: ResultUnknown, Key: "key", Value: "value"},
		{ID: 2, Kind: OpGet, Node: 2, Start: 3, End: 4, Result: ResultOK, Key: "key", Found: false},
	}, 1, 0)
	if !omitted.Valid {
		t.Fatalf("unknown mutation could not be omitted: %#v", omitted)
	}
}

func TestHistoryCheckerValidatesCountsAndShape(t *testing.T) {
	valid := HistoryEvent{ID: 1, Kind: OpGet, Node: 1, Start: 1, End: 2, Result: ResultOK, Key: "key", Found: false}
	tests := []struct {
		name   string
		events []HistoryEvent
		reads  int
		writes int
	}{
		{name: "zero id", events: []HistoryEvent{{Kind: OpGet, Node: 1, Start: 1, End: 2, Result: ResultOK, Key: "key"}}},
		{name: "duplicate id", events: []HistoryEvent{valid, valid}},
		{name: "bad interval", events: []HistoryEvent{{ID: 1, Kind: OpGet, Node: 1, Start: 2, End: 2, Result: ResultOK, Key: "key"}}},
		{name: "bad node", events: []HistoryEvent{{ID: 1, Kind: OpGet, Node: 8, Start: 1, End: 2, Result: ResultOK, Key: "key"}}},
		{name: "bad kind", events: []HistoryEvent{{ID: 1, Kind: "cas", Node: 1, Start: 1, End: 2, Result: ResultOK, Key: "key"}}},
		{name: "bad result", events: []HistoryEvent{{ID: 1, Kind: OpGet, Node: 1, Start: 1, End: 2, Result: "maybe", Key: "key"}}},
		{name: "empty txn", events: []HistoryEvent{{ID: 1, Kind: OpTxn, Node: 1, Start: 1, End: 2, Result: ResultOK}}},
		{name: "minimum read", events: []HistoryEvent{valid}, reads: 2},
		{name: "minimum mutation", events: []HistoryEvent{valid}, reads: 1, writes: 1},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			result := checkHistory(testCase.events, testCase.reads, testCase.writes)
			if result.Valid || result.Error == "" {
				t.Fatalf("invalid history accepted: %#v", result)
			}
		})
	}
}

func TestHistoryCheckerRejectsUnboundedSearch(t *testing.T) {
	events := make([]HistoryEvent, maxHistoryEvents+1)
	for index := range events {
		events[index] = HistoryEvent{
			ID: uint64(index + 1), Kind: OpGet, Node: 1,
			Start: uint64(index*2 + 1), End: uint64(index*2 + 2),
			Result: ResultOK, Key: "key", Found: false,
		}
	}
	result := checkHistory(events, 0, 0)
	if result.Valid || result.Error == "" {
		t.Fatalf("unbounded history accepted: %#v", result)
	}
}

func TestHistoryCheckerDeterministicAndDoesNotTakeCallerOwnership(t *testing.T) {
	events := []HistoryEvent{
		{ID: 2, Kind: OpGet, Node: 2, Start: 3, End: 4, Result: ResultOK, Key: "key", Found: true, Value: "value"},
		{ID: 1, Kind: OpTxn, Node: 1, Start: 1, End: 2, Result: ResultOK, Txn: []TxnOperation{{Key: "key", Value: "value"}}},
	}
	before := cloneHistory(events)
	first := checkHistory(events, 1, 1)
	second := checkHistory(events, 1, 1)
	if !first.Valid || first.TerminalDigest != second.TerminalDigest || !reflect.DeepEqual(first.Linearization, second.Linearization) {
		t.Fatalf("checker is not deterministic: first=%#v second=%#v", first, second)
	}
	if !reflect.DeepEqual(events, before) {
		t.Fatalf("checker mutated caller history: before=%#v after=%#v", before, events)
	}
	first.Terminal["key"] = "changed"
	if second.Terminal["key"] != "value" {
		t.Fatal("checker results share terminal map ownership")
	}
}
