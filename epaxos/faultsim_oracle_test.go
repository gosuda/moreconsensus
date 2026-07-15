package epaxos

import (
	"bytes"
	"fmt"
	"testing"
)

type faultOracleResult struct {
	OK                 bool   `json:"ok"`
	Linearizable       bool   `json:"linearizable"`
	ChosenAgreement    bool   `json:"chosen_agreement"`
	ExactlyOnce        bool   `json:"exactly_once_application"`
	ConflictOrder      bool   `json:"converged_conflict_order"`
	ConvergedState     bool   `json:"converged_state"`
	MinorityFailClosed bool   `json:"minority_fail_closed"`
	DurableSlowQuorum  bool   `json:"durable_slow_quorum"`
	HealedLiveness     bool   `json:"healed_liveness"`
	LogicalStepBound   int    `json:"logical_step_bound,omitempty"`
	Error              string `json:"error,omitempty"`
}

func faultOracleExpectedNodes(h *faultSimHarness) []ReplicaID {
	var newest ConfState
	for _, id := range h.ids {
		status := h.replicas[id].node.Status()
		if status.Conf.ID > newest.ID {
			newest = status.Conf.Clone()
		}
	}
	if newest.ID == 0 {
		return append([]ReplicaID(nil), h.ids...)
	}
	out := make([]ReplicaID, 0, len(newest.Voters))
	for _, id := range newest.Voters {
		if h.replicas[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

func faultCheckOracle(h *faultSimHarness, expectedNodes []ReplicaID) faultOracleResult {
	result := faultOracleResult{LogicalStepBound: h.lastProgressBound}
	var failures []string
	if ok, reason := faultHistoryLinearizable(h.history, 12); ok {
		result.Linearizable = true
	} else {
		failures = append(failures, "history: "+reason)
	}
	if ok, reason := faultChosenAgreement(h); ok {
		result.ChosenAgreement = true
	} else {
		failures = append(failures, "chosen agreement: "+reason)
	}
	if ok, reason := faultExactlyOnce(h, expectedNodes); ok {
		result.ExactlyOnce = true
	} else {
		failures = append(failures, "exactly once: "+reason)
	}
	if ok, reason := faultConflictOrder(h, expectedNodes); ok {
		result.ConflictOrder = true
	} else {
		failures = append(failures, "conflict order: "+reason)
	}
	if ok, reason := faultConvergedState(h, expectedNodes); ok {
		result.ConvergedState = true
	} else {
		failures = append(failures, "state convergence: "+reason)
	}
	result.MinorityFailClosed = true
	for _, check := range h.failClosed {
		if !check.OK {
			result.MinorityFailClosed = false
			failures = append(failures, "fail-closed check: "+check.Name)
		}
	}
	if ok, reason := faultDurableQuorums(h); ok {
		result.DurableSlowQuorum = true
	} else {
		failures = append(failures, "durable quorum: "+reason)
	}
	if ok, reason := faultCompletedEverywhere(h, expectedNodes); ok {
		result.HealedLiveness = true
	} else {
		failures = append(failures, "healed liveness: "+reason)
	}
	result.OK = result.Linearizable && result.ChosenAgreement && result.ExactlyOnce &&
		result.ConflictOrder && result.ConvergedState && result.MinorityFailClosed &&
		result.DurableSlowQuorum && result.HealedLiveness
	if len(failures) != 0 {
		result.Error = fmt.Sprint(failures)
	}
	return result
}

func faultHistoryLinearizable(history []faultHistoryOperation, bound int) (bool, string) {
	if len(history) > bound {
		return false, fmt.Sprintf("history has %d operations, bound is %d", len(history), bound)
	}
	mandatory := make([]faultHistoryOperation, 0, len(history))
	optional := make([]faultHistoryOperation, 0, len(history))
	for _, operation := range history {
		switch operation.Result {
		case "ok":
			mandatory = append(mandatory, operation)
		case "unknown", "":
			optional = append(optional, operation)
		case "fail":
		default:
			return false, fmt.Sprintf("operation %#v has unknown result %q", operation.Operation.commandID(), operation.Result)
		}
	}
	if len(optional) >= 63 {
		return false, "too many optional operations"
	}
	for mask := uint64(0); mask < uint64(1)<<len(optional); mask++ {
		selected := append([]faultHistoryOperation(nil), mandatory...)
		for i, operation := range optional {
			if mask&(uint64(1)<<i) != 0 {
				selected = append(selected, operation)
			}
		}
		placed := make([]bool, len(selected))
		if faultLinearizationDFS(selected, placed, make(map[string]string), 0) {
			return true, ""
		}
	}
	return false, "no legal sequential history respects responses and real-time precedence"
}

func faultLinearizationDFS(operations []faultHistoryOperation, placed []bool, state map[string]string, depth int) bool {
	if depth == len(operations) {
		return true
	}
	for candidate := range operations {
		if placed[candidate] || faultHasUnplacedPredecessor(operations, placed, candidate) {
			continue
		}
		next, ok := faultApplyHistoryOperation(state, operations[candidate])
		if !ok {
			continue
		}
		placed[candidate] = true
		if faultLinearizationDFS(operations, placed, next, depth+1) {
			return true
		}
		placed[candidate] = false
	}
	return false
}

func faultHasUnplacedPredecessor(operations []faultHistoryOperation, placed []bool, candidate int) bool {
	for index, predecessor := range operations {
		if index == candidate || placed[index] || predecessor.Complete == 0 {
			continue
		}
		if predecessor.Complete < operations[candidate].Invoke {
			return true
		}
	}
	return false
}

func faultApplyHistoryOperation(state map[string]string, history faultHistoryOperation) (map[string]string, bool) {
	next := make(map[string]string, len(state)+len(history.Operation.Writes))
	for key, value := range state {
		next[key] = value
	}
	response := faultOperationResponse{}
	for _, key := range history.Operation.Reads {
		response.Reads = append(response.Reads, faultKV{Key: key, Value: next[key]})
	}
	if history.Result == "ok" && !response.equal(history.Response) {
		return nil, false
	}
	for _, write := range history.Operation.Writes {
		next[write.Key] = write.Value
	}
	for _, key := range history.Operation.Deletes {
		delete(next, key)
	}
	return next, true
}

func faultChosenAgreement(h *faultSimHarness) (bool, string) {
	chosen := make(map[InstanceRef]InstanceRecord)
	for _, id := range h.ids {
		for ref, record := range h.replicas[id].store.Records {
			if record.Status < StatusCommitted {
				continue
			}
			if existing, ok := chosen[ref]; ok {
				if !faultSameChosen(existing, record) {
					return false, fmt.Sprintf("node %d chose a different tuple for %s", id, ref)
				}
				continue
			}
			chosen[ref] = record.Clone()
		}
	}
	return true, ""
}

func faultSameChosen(a, b InstanceRecord) bool {
	if a.Ref != b.Ref || a.Seq != b.Seq || a.ProcessAt != b.ProcessAt || a.TOQPending != b.TOQPending ||
		a.FastPathEligible != b.FastPathEligible || !faultSameCommand(a.Command, b.Command) || len(a.Deps) != len(b.Deps) ||
		a.ConfChangeResult.Outcome != b.ConfChangeResult.Outcome ||
		a.ConfChangeResult.Conf.ID != b.ConfChangeResult.Conf.ID ||
		!sameReplicaIDs(a.ConfChangeResult.Conf.Voters, b.ConfChangeResult.Conf.Voters) {
		return false
	}
	for i := range a.Deps {
		if a.Deps[i] != b.Deps[i] {
			return false
		}
	}
	return true
}

func faultExactlyOnce(h *faultSimHarness, nodes []ReplicaID) (bool, string) {
	for _, id := range nodes {
		r := h.replicas[id]
		seen := make(map[InstanceRef]struct{}, len(r.app.Log))
		for _, event := range r.app.Log {
			if _, duplicate := seen[event.Ref]; duplicate {
				return false, fmt.Sprintf("node %d applied %s more than once", id, event.Ref)
			}
			seen[event.Ref] = struct{}{}
		}
		for _, history := range h.history {
			if history.Result != "ok" {
				continue
			}
			event, ok := r.app.Applied[history.Ref]
			if !ok {
				return false, fmt.Sprintf("node %d did not apply completed %s", id, history.Ref)
			}
			command, err := history.Operation.command()
			if err != nil || !faultSameCommand(event.Command, command) {
				return false, fmt.Sprintf("node %d applied wrong command for %s", id, history.Ref)
			}
		}
	}
	return true, ""
}

func faultConflictOrder(h *faultSimHarness, nodes []ReplicaID) (bool, string) {
	if len(nodes) == 0 {
		return false, "expected node set is empty"
	}
	commands := make(map[InstanceRef]Command)
	for _, history := range h.history {
		if history.Result != "ok" {
			continue
		}
		command, err := history.Operation.command()
		if err != nil {
			return false, err.Error()
		}
		commands[history.Ref] = command
	}
	positions := func(id ReplicaID) map[InstanceRef]int {
		out := make(map[InstanceRef]int)
		for index, event := range h.replicas[id].app.Log {
			if _, tracked := commands[event.Ref]; tracked {
				out[event.Ref] = index
			}
		}
		return out
	}
	base := positions(nodes[0])
	refs := make([]InstanceRef, 0, len(commands))
	for ref := range commands {
		refs = append(refs, ref)
	}
	sortRefs(refs)
	for _, id := range nodes[1:] {
		other := positions(id)
		for left := range len(refs) {
			for right := left + 1; right < len(refs); right++ {
				a, b := refs[left], refs[right]
				if !commands[a].ConflictsWith(commands[b]) {
					continue
				}
				ap, aok := base[a]
				bp, bok := base[b]
				op, ook := other[a]
				qp, qok := other[b]
				if !aok || !bok || !ook || !qok || (ap < bp) != (op < qp) {
					return false, fmt.Sprintf("node %d conflict order differs for %s and %s", id, a, b)
				}
			}
		}
	}
	return true, ""
}

func faultConvergedState(h *faultSimHarness, nodes []ReplicaID) (bool, string) {
	if len(nodes) == 0 {
		return false, "expected node set is empty"
	}
	base := h.replicas[nodes[0]].app.State
	for _, id := range nodes[1:] {
		other := h.replicas[id].app.State
		if len(base) != len(other) {
			return false, fmt.Sprintf("node %d state size %d, want %d", id, len(other), len(base))
		}
		for key, value := range base {
			if other[key] != value {
				return false, fmt.Sprintf("node %d key %q=%q, want %q", id, key, other[key], value)
			}
		}
	}
	return true, ""
}

func faultDurableQuorums(h *faultSimHarness) (bool, string) {
	for _, history := range h.history {
		if history.Result != "ok" {
			continue
		}
		count := 0
		voters := 0
		for _, id := range h.ids {
			if record, ok := h.replicas[id].store.Records[history.Ref]; ok {
				if voters == 0 {
					voters = len(record.Deps)
				}
				if record.Status >= StatusCommitted {
					count++
				}
			}
		}
		if voters == 0 {
			return false, fmt.Sprintf("completed %s has no durable record", history.Ref)
		}
		slow, err := SlowQuorum(voters)
		if err != nil || count < slow {
			return false, fmt.Sprintf("completed %s durable copies=%d, slow quorum=%d", history.Ref, count, slow)
		}
	}
	return true, ""
}

func faultCompletedEverywhere(h *faultSimHarness, nodes []ReplicaID) (bool, string) {
	completed := 0
	for _, history := range h.history {
		if history.Result != "ok" {
			continue
		}
		completed++
		for _, id := range nodes {
			if !h.appliedOn(id, []InstanceRef{history.Ref}) {
				return false, fmt.Sprintf("completed %s is absent on healed node %d", history.Ref, id)
			}
		}
	}
	if completed == 0 {
		return false, "history has no completed client operation"
	}
	return true, ""
}

func TestFaultOracleLinearizableAtomicTransactions(t *testing.T) {
	h, err := newFaultSimHarness(faultSimConfig{Size: 1, Seed: 41})
	if err != nil {
		t.Fatal(err)
	}
	faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})
	operations := []faultClientOperation{
		{Client: 1, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "x", Value: "one"}}},
		{Client: 1, Sequence: 2, Kind: "txn", Reads: []string{"x"}, Writes: []faultKV{{Key: "x", Value: "two"}, {Key: "y", Value: "atomic"}}},
		{Client: 1, Sequence: 3, Kind: "get", Reads: []string{"y"}},
	}
	for index := range operations {
		op := operations[index]
		faultMustAction(t, h, faultSimAction{Kind: faultActionPropose, Node: 1, Operation: &op})
		ref, ok := h.refFor(op.commandID())
		if !ok {
			t.Fatalf("operation %#v has no ref", op.commandID())
		}
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, []ReplicaID{1}, 20); err != nil {
			t.Fatal(err)
		}
	}
	result := faultCheckOracle(h, []ReplicaID{1})
	if !result.OK {
		t.Fatalf("atomic history oracle failed: %#v", result)
	}
	last := &h.history[len(h.history)-1]
	if len(last.Response.Reads) != 1 || last.Response.Reads[0] != (faultKV{Key: "y", Value: "atomic"}) {
		t.Fatalf("get response = %#v, want atomic transaction output", last.Response)
	}
	original := last.Response
	last.Response = faultOperationResponse{Reads: []faultKV{{Key: "y", Value: "torn"}}}
	if ok, _ := faultHistoryLinearizable(h.history, 12); ok {
		t.Fatal("linearizability oracle accepted an impossible torn transaction result")
	}
	last.Response = original
}

func TestFaultSchedulerExplicitEncodedEnvelopeActions(t *testing.T) {
	h, err := newFaultSimHarness(faultSimConfig{Size: 3, Seed: 77})
	if err != nil {
		t.Fatal(err)
	}
	faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})
	op := faultClientOperation{Client: 77, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "immutable", Value: "yes"}}}
	faultMustAction(t, h, faultSimAction{Kind: faultActionPropose, Node: 1, Operation: &op})
	faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
	ids := h.pendingEnvelopeIDs(false)
	if len(ids) != 2 {
		t.Fatalf("initial envelope IDs = %v, want exactly two", ids)
	}
	original := h.envelopes[ids[0]].bytes()
	faultMustAction(t, h, faultSimAction{Kind: faultActionHold, Envelope: ids[0]})
	faultMustAction(t, h, faultSimAction{Kind: faultActionRelease, Envelope: ids[0]})
	faultMustAction(t, h, faultSimAction{Kind: faultActionDuplicate, Envelope: ids[1], Count: 1})
	duplicate := h.nextEnvelope
	faultMustAction(t, h, faultSimAction{Kind: faultActionCorrupt, Envelope: duplicate, Mutation: "truncate"})
	corrupted := h.nextEnvelope
	receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionDeliver, Envelope: corrupted})
	if !receipt.Rejected || receipt.Affected != 1 {
		t.Fatalf("corrupt delivery receipt = %#v, want accounted rejection", receipt)
	}
	faultMustAction(t, h, faultSimAction{Kind: faultActionDrop, Envelope: ids[1]})
	if !bytes.Equal(original, h.envelopes[ids[0]].bytes()) {
		t.Fatal("hold/duplicate/corrupt mutated the immutable source envelope")
	}
	ref, _ := h.refFor(op.commandID())
	if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, 80); err != nil {
		t.Fatalf("%v; counters=%v records=%v", err, h.counters, faultHarnessRecordStates(h))
	}
	result := faultCheckOracle(h, h.ids)
	if !result.OK {
		t.Fatalf("scheduler safety oracle failed: %#v", result)
	}
	for _, counter := range []string{"hold", "release", "duplicate", "corrupt", "malformed", "drop"} {
		if h.counters[counter] == 0 {
			t.Fatalf("explicit action counter %q is zero: %#v", counter, h.counters)
		}
	}
}

func TestFaultSchedulerCapacityBackpressureHasNoUnaccountedDiscard(t *testing.T) {
	t.Run("outbound-envelope-capacity", func(t *testing.T) {
		h, err := newFaultSimHarness(faultSimConfig{Size: 3, Seed: 81, MaxEnvelopes: 1})
		if err != nil {
			t.Fatal(err)
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})
		op := faultClientOperation{Client: 81, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "queue", Value: "retained"}}}
		ref := faultCampaignPropose(t, h, 1, op)
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		if !receipt.Rejected || h.counters["backpressure"] == 0 {
			t.Fatalf("envelope saturation receipt = %#v counters=%#v", receipt, h.counters)
		}
		if _, durable := h.replicas[1].store.Records[ref]; durable {
			t.Fatalf("backpressured Ready partially persisted %s", ref)
		}
		if !h.replicas[1].node.HasReady() || h.inflightCount() != 0 {
			t.Fatalf("backpressured Ready was discarded: hasReady=%v inflight=%d", h.replicas[1].node.HasReady(), h.inflightCount())
		}
		h.cfg.MaxEnvelopes = 16
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, h.ids, 80); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("atomic-application-batch-capacity", func(t *testing.T) {
		h, err := newFaultSimHarness(faultSimConfig{Size: 1, Seed: 82, MaxApplicationBatch: 1})
		if err != nil {
			t.Fatal(err)
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})
		first := faultClientOperation{Client: 82, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "a", Value: "one"}}}
		second := faultClientOperation{Client: 82, Sequence: 2, Kind: "put", Writes: []faultKV{{Key: "b", Value: "two"}}}
		firstRef := faultCampaignPropose(t, h, 1, first)
		secondRef := faultCampaignPropose(t, h, 1, second)
		receipt := faultMustAction(t, h, faultSimAction{Kind: faultActionPump})
		if !receipt.Rejected || h.counters["backpressure"] == 0 {
			t.Fatalf("application saturation receipt = %#v counters=%#v", receipt, h.counters)
		}
		if len(h.replicas[1].store.Records) != 0 || len(h.replicas[1].app.Log) != 0 {
			t.Fatalf("application backpressure partially mutated durable images: records=%#v log=%#v", h.replicas[1].store.Records, h.replicas[1].app.Log)
		}
		h.cfg.MaxApplicationBatch = 2
		if _, err := h.driveUntilApplied([]InstanceRef{firstRef, secondRef}, []ReplicaID{1}, 40); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFaultSchedulerLogicalTimePauseBurstAndTOQRollback(t *testing.T) {
	classic, err := newFaultSimHarness(faultSimConfig{Size: 1, Seed: 91})
	if err != nil {
		t.Fatal(err)
	}
	faultMustAction(t, classic, faultSimAction{Kind: faultActionPump, Required: true})
	faultMustAction(t, classic, faultSimAction{Kind: faultActionPause, Node: 1})
	pausedTick := faultMustAction(t, classic, faultSimAction{Kind: faultActionTick, Node: 1})
	if !pausedTick.Rejected || classic.replicas[1].node.Status().Tick != 0 {
		t.Fatalf("paused tick receipt/status = %#v tick=%d", pausedTick, classic.replicas[1].node.Status().Tick)
	}
	faultMustAction(t, classic, faultSimAction{Kind: faultActionResume, Node: 1})
	burst := faultMustAction(t, classic, faultSimAction{Kind: faultActionBurst, Node: 1, Count: 3})
	if burst.Affected != 3 || classic.replicas[1].node.Status().Tick != 3 {
		t.Fatalf("burst receipt/status = %#v tick=%d", burst, classic.replicas[1].node.Status().Tick)
	}

	toq, err := newFaultSimHarness(faultSimConfig{Size: 1, Seed: 92, TOQ: true})
	if err != nil {
		t.Fatal(err)
	}
	faultMustAction(t, toq, faultSimAction{Kind: faultActionPump, Required: true})
	faultMustAction(t, toq, faultSimAction{Kind: faultActionSetClock, Node: 1, Clock: 10})
	faultMustAction(t, toq, faultSimAction{Kind: faultActionProcessTOQ, Node: 1})
	faultMustAction(t, toq, faultSimAction{Kind: faultActionSetClock, Node: 1, Clock: 9})
	rollback := faultMustAction(t, toq, faultSimAction{Kind: faultActionProcessTOQ, Node: 1})
	if !rollback.Rejected || toq.counters["clock-rollback"] == 0 || toq.counters["clock-rollback-rejected"] == 0 {
		t.Fatalf("TOQ rollback receipt/counters = %#v %#v", rollback, toq.counters)
	}
}

func TestFaultCrashCutsReplayExactly(t *testing.T) {
	cuts := []faultCrashCut{
		faultCrashBeforeReady,
		faultCrashAfterFrozenReady,
		faultCrashAfterPersistence,
		faultCrashAfterApplication,
		faultCrashAfterAdvance,
		faultCrashAfterExecutedPersistence,
	}
	for cutIndex, cut := range cuts {
		t.Run(string(cut), func(t *testing.T) {
			h, err := newFaultSimHarness(faultSimConfig{Size: 1, Seed: uint64(100 + cutIndex)}) //nolint:gosec // G115: test harness converts bounded int index/count
			if err != nil {
				t.Fatal(err)
			}
			faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})
			op := faultClientOperation{Client: 500 + uint64(cutIndex), Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "cut", Value: string(cut)}}} //nolint:gosec // G115: test harness converts bounded int index/count
			faultMustAction(t, h, faultSimAction{Kind: faultActionPropose, Node: 1, Operation: &op})
			faultMustAction(t, h, faultSimAction{Kind: faultActionCrash, Node: 1, Cut: cut})
			refs := []InstanceRef{}
			if cut != faultCrashBeforeReady && cut != faultCrashAfterFrozenReady {
				ref, _ := h.refFor(op.commandID())
				refs = append(refs, ref)
			}
			canary := faultClientOperation{Client: 900 + uint64(cutIndex), Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "canary", Value: string(cut)}}} //nolint:gosec // G115: test harness converts bounded int index/count
			faultMustAction(t, h, faultSimAction{Kind: faultActionPropose, Node: 1, Operation: &canary})
			canaryRef, _ := h.refFor(canary.commandID())
			refs = append(refs, canaryRef)
			if _, err := h.driveUntilApplied(refs, []ReplicaID{1}, 40); err != nil {
				t.Fatal(err)
			}
			result := faultCheckOracle(h, []ReplicaID{1})
			if !result.OK {
				t.Fatalf("crash cut oracle failed: %#v", result)
			}
			trace := h.trace(result)
			if _, err := faultReplayTrace(trace); err != nil {
				t.Fatalf("crash cut replay differs: %v", err)
			}
		})
	}
	t.Run("selected-durable-image", func(t *testing.T) {
		h, err := newFaultSimHarness(faultSimConfig{Size: 1, Seed: 199})
		if err != nil {
			t.Fatal(err)
		}
		faultMustAction(t, h, faultSimAction{Kind: faultActionPump, Required: true})
		faultMustAction(t, h, faultSimAction{Kind: faultActionSnapshot, Node: 1, Name: "selected"})
		discarded := faultClientOperation{Client: 199, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "discarded", Value: "not-durable"}}}
		faultCampaignPropose(t, h, 1, discarded)
		faultMustAction(t, h, faultSimAction{Kind: faultActionCrash, Node: 1, Cut: faultCrashBeforeReady, Image: "selected"})
		if len(h.replicas[1].store.Records) != 0 || len(h.replicas[1].app.Log) != 0 {
			t.Fatalf("selected image reconstruction retained non-durable state: records=%#v log=%#v", h.replicas[1].store.Records, h.replicas[1].app.Log)
		}
		canary := faultClientOperation{Client: 200, Sequence: 1, Kind: "put", Writes: []faultKV{{Key: "selected", Value: "reconstructed"}}}
		ref := faultCampaignPropose(t, h, 1, canary)
		if _, err := h.driveUntilApplied([]InstanceRef{ref}, []ReplicaID{1}, 40); err != nil {
			t.Fatal(err)
		}
		result := faultCheckOracle(h, []ReplicaID{1})
		if !result.OK {
			t.Fatalf("selected-image oracle failed: %#v", result)
		}
		trace := h.trace(result)
		if _, err := faultReplayTrace(trace); err != nil {
			t.Fatalf("selected-image replay differs: %v", err)
		}
	})
}
