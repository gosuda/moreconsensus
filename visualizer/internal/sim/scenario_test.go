package sim

import (
	"reflect"
	"testing"
)

func TestScenarioCatalogContract(t *testing.T) {
	t.Parallel()
	catalog := Catalog()
	got := make([]string, len(catalog))
	for i := range catalog {
		got[i] = catalog[i].ID
	}
	want := []string{"parallel", "fast-path", "conflict-cycle", "recovery", "optimization"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog order = %#v, want %#v", got, want)
	}
	catalog[0].ID = "mutated"
	if Catalog()[0].ID != "parallel" {
		t.Fatal("Catalog exposed mutable package storage")
	}
	if _, err := BuildScenario("missing"); errorCode(err) != CodeNotFound {
		t.Fatalf("missing scenario error = %v, code %q", err, errorCode(err))
	}
}

func TestParallelScenarioContract(t *testing.T) {
	t.Parallel()
	trace := mustScenario(t, "parallel")
	final := trace.Frames[len(trace.Frames)-1]
	firstRef := final.Snapshot.Commands[0].Ref
	secondRef := final.Snapshot.Commands[1].Ref
	if traceHasMessageType(trace.Frames, "accept") || traceHasMessageType(trace.Frames, "accept-resp") {
		t.Fatal("parallel trace contains Accept")
	}
	for _, replica := range final.Snapshot.Replicas {
		if countApplied(replica, firstRef) != 1 || countApplied(replica, secondRef) != 1 {
			t.Fatalf("R%d apply counts = %#v", replica.ID, replica.Applied)
		}
		first, firstOK := instanceByRef(replica, firstRef)
		second, secondOK := instanceByRef(replica, secondRef)
		if !firstOK || !secondOK || contains(first.Edges, secondRef) || contains(second.Edges, firstRef) {
			t.Fatalf("R%d cross-dependencies = %#v / %#v", replica.ID, first, second)
		}
	}
	assertMilestones(t, trace, []string{"No permanent leader", "Different keys, no dependencies", "Both take the fast path"})
}

func TestFastPathScenarioContract(t *testing.T) {
	t.Parallel()
	trace := mustScenario(t, "fast-path")
	for _, required := range []string{"pre-accept", "pre-accept-resp", "commit"} {
		if !traceHasMessageType(trace.Frames, required) {
			t.Fatalf("fast-path trace has no %s", required)
		}
	}
	if traceHasMessageType(trace.Frames, "accept") || traceHasMessageType(trace.Frames, "accept-resp") {
		t.Fatal("fast-path trace contains Accept")
	}
	final := trace.Frames[len(trace.Frames)-1]
	ref := final.Snapshot.Commands[0].Ref
	for _, replica := range final.Snapshot.Replicas {
		if countApplied(replica, ref) != 1 {
			t.Fatalf("R%d apply count = %d", replica.ID, countApplied(replica, ref))
		}
		if value, ok := stateValue(replica, "profile"); !ok || value != "ready" {
			t.Fatalf("R%d profile = %q, %v", replica.ID, value, ok)
		}
	}
	assertMilestones(t, trace, []string{"R1 owns this instance", "A fast quorum matches", "Accept is skipped"})
}

func TestConflictCycleScenarioContract(t *testing.T) {
	t.Parallel()
	trace := mustScenario(t, "conflict-cycle")
	if !traceHasMessageType(trace.Frames, "accept") || !traceHasMessageType(trace.Frames, "accept-resp") {
		t.Fatal("conflict trace lacks Accept round")
	}
	final := trace.Frames[len(trace.Frames)-1]
	firstRef := final.Snapshot.Commands[0].Ref
	secondRef := final.Snapshot.Commands[1].Ref
	wantOrder := appliedRefs(final.Snapshot.Replicas[0])
	for _, replica := range final.Snapshot.Replicas {
		first, firstOK := instanceByRef(replica, firstRef)
		second, secondOK := instanceByRef(replica, secondRef)
		if !firstOK || !secondOK || !contains(first.Edges, secondRef) || !contains(second.Edges, firstRef) {
			t.Fatalf("R%d reciprocal dependencies = %#v / %#v", replica.ID, first, second)
		}
		if !equalStrings(wantOrder, appliedRefs(replica)) {
			t.Fatalf("R%d apply order = %#v, want %#v", replica.ID, appliedRefs(replica), wantOrder)
		}
		if value, ok := stateValue(replica, "cart"); !ok || value != "paid" {
			t.Fatalf("R%d cart = %q, %v", replica.ID, value, ok)
		}
	}
	assertMilestones(t, trace, []string{"Same key, different arrival order", "Attributes diverge", "Accept chooses merged attributes", "The cycle becomes an order"})
}

func TestRecoveryScenarioContract(t *testing.T) {
	t.Parallel()
	trace := mustScenario(t, "recovery")
	if !traceHasMessageType(trace.Frames, "prepare") || !traceHasMessageType(trace.Frames, "prepare-resp") {
		t.Fatal("recovery trace lacks Prepare exchange")
	}
	final := trace.Frames[len(trace.Frames)-1]
	firstRef := final.Snapshot.Commands[0].Ref
	secondRef := final.Snapshot.Commands[1].Ref
	for _, replica := range final.Snapshot.Replicas {
		if countApplied(replica, firstRef) != 1 || countApplied(replica, secondRef) != 1 {
			t.Fatalf("R%d apply counts = %#v", replica.ID, replica.Applied)
		}
		if value, ok := stateValue(replica, "order"); !ok || value != "confirmed" {
			t.Fatalf("R%d order = %q, %v", replica.ID, value, ok)
		}
	}
	assertMilestones(t, trace, []string{"The owner disappears", "A dependency blocks execution", "R2 raises a recovery ballot", "The value survives its owner"})
}

func TestOptimizationScenarioContract(t *testing.T) {
	t.Parallel()
	trace := mustScenario(t, "optimization")
	initial := trace.Frames[0]
	if initial.Snapshot.Cluster.Size != 5 || !initial.Snapshot.Cluster.Financial {
		t.Fatalf("optimization cluster = %#v", initial.Snapshot.Cluster)
	}
	if len(initial.Snapshot.Accounts) != 10 || len(initial.Snapshot.Links) != 10 {
		t.Fatalf("optimization topology has %d accounts and %d links", len(initial.Snapshot.Accounts), len(initial.Snapshot.Links))
	}
	for _, replica := range initial.Snapshot.Replicas {
		if replica.Booted {
			t.Fatalf("R%d booted before the bootstrap trace", replica.ID)
		}
	}

	sawRTT := false
	for index, frame := range trace.Frames {
		if frame.Learning == nil || frame.Learning.Phase == "" || frame.Learning.Title == "" ||
			frame.Learning.Summary == "" || frame.Learning.Why == "" ||
			frame.Learning.Invariant == "" || len(frame.Learning.Algorithm) == 0 {
			t.Fatalf("frame %d learning content = %#v", index, frame.Learning)
		}
		for _, message := range frame.Snapshot.Messages {
			if message.RTTMS > 0 {
				sawRTT = true
			}
		}
	}
	if !sawRTT {
		t.Fatal("optimization trace contains no RTT-scheduled message")
	}

	final := trace.Frames[len(trace.Frames)-1]
	if len(final.Snapshot.Commands) != 1 {
		t.Fatalf("optimization commands = %#v", final.Snapshot.Commands)
	}
	command := final.Snapshot.Commands[0]
	if command.Operation != "TRANSFER" || len(command.Resources) != 4 ||
		!contains(command.Resources, "acct/northwind") ||
		!contains(command.Resources, "acct/contoso") ||
		!contains(command.Resources, "dedup/1/1") ||
		!contains(command.Resources, "txn/1/1") {
		t.Fatalf("optimization transfer = %#v", command)
	}
	for _, replica := range final.Snapshot.Replicas {
		if !replica.Booted || replica.Crashed || countApplied(replica, command.Ref) != 1 {
			t.Fatalf("R%d final protocol state = %#v", replica.ID, replica)
		}
		if got := stateInt(t, replica, accountStateKey("northwind")); got != 24_750_000 {
			t.Fatalf("R%d northwind balance = %d", replica.ID, got)
		}
		if got := stateInt(t, replica, accountStateKey("contoso")); got != 12_250_000 {
			t.Fatalf("R%d contoso balance = %d", replica.ID, got)
		}
	}
	assertMilestones(t, trace, []string{
		"Bootstrap durable state",
		"Five locality replicas are online",
		"Locality routes the debit",
		"RTT shapes the fast quorum",
		"TOQ moves work to ProcessAt",
	})
}

func mustScenario(t *testing.T, id string) ScenarioTrace {
	t.Helper()
	trace, err := BuildScenario(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Frames) < 2 || trace.Frames[0].Index != 0 || len(trace.Frames[0].Events) != 0 {
		t.Fatalf("%s frame zero = %#v", id, trace.Frames[0])
	}
	return trace
}

func assertMilestones(t *testing.T, trace ScenarioTrace, want []string) {
	t.Helper()
	var got []string
	for _, frame := range trace.Frames {
		if frame.Headline != "" && (len(got) == 0 || got[len(got)-1] != frame.Headline) {
			got = append(got, frame.Headline)
		}
		if frame.Headline != "" && (frame.Explanation == "" || frame.Focus == nil) {
			t.Fatalf("headline %q lacks explanation or focus", frame.Headline)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s milestones = %#v, want %#v", trace.Scenario.ID, got, want)
	}
}
