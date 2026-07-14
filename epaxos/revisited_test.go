package epaxos

import "testing"

func TestRevisitedChainPruningExecutesBaseBeforeLaterDependencyChain(t *testing.T) {
	rn := revisitedRawNode(t)
	shared := []byte("revisited-chain")
	a := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	b := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	c := InstanceRef{Replica: 3, Instance: 1, Conf: 1}

	revisitedInstall(rn, InstanceRecord{
		Ref:     a,
		Status:  StatusCommitted,
		Seq:     1,
		Deps:    []InstanceNum{0, 1, 0},
		Command: revisitedCommand(1, "A", shared),
	})
	revisitedInstall(rn, InstanceRecord{
		Ref:     b,
		Status:  StatusCommitted,
		Seq:     2,
		Deps:    []InstanceNum{1, 0, 1},
		Command: revisitedCommand(2, "B", shared),
	})
	revisitedInstall(rn, InstanceRecord{
		Ref:     c,
		Status:  StatusCommitted,
		Seq:     3,
		Deps:    []InstanceNum{0, 0, 2},
		Command: revisitedCommand(3, "C", shared),
	})

	view := rn.newExecutionView()
	comp := revisitedComponentContaining(rn.executionComponents(&view), a)
	if len(comp) != 1 || comp[0] != a {
		t.Fatalf("component containing %s = %v, want singleton after pruning %s -> %s", a, comp, a, b)
	}
	var candidates []recoveryCandidate
	if !rn.componentReady(&view, comp, &candidates) {
		t.Fatalf("component containing %s was not ready after pruning %s, despite %s proving Seq(%s) > Seq(%s)", a, b, b, b, a)
	}

	rn.tryExecute()
	rd := rn.Ready()
	revisitedRequireCommittedRefs(t, rd.Committed, []InstanceRef{a})
	if rn.instances[a].rec.Status != StatusExecuted {
		t.Fatalf("base %s status = %s, want executed", a, rn.instances[a].rec.Status)
	}
	for _, ref := range []InstanceRef{b, c} {
		if rn.instances[ref].rec.Status == StatusExecuted {
			t.Fatalf("later dependency %s executed before its own prerequisites were ready; ready=%#v", ref, rd)
		}
	}
}

func TestRevisitedChainPruningIgnoresKnownUncommittedConflictAfterBase(t *testing.T) {
	for _, status := range []Status{StatusPreAccepted, StatusAccepted} {
		t.Run(status.String(), func(t *testing.T) {
			rn := revisitedRawNode(t)
			shared := []byte("revisited-known-conflict")
			a := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
			b := InstanceRef{Replica: 2, Instance: 1, Conf: 1}

			revisitedInstall(rn, InstanceRecord{
				Ref:     a,
				Status:  StatusCommitted,
				Seq:     1,
				Deps:    rn.q.deps(),
				Command: revisitedCommand(10, "A", shared),
			})
			revisitedInstall(rn, InstanceRecord{
				Ref:     b,
				Status:  status,
				Seq:     2,
				Deps:    []InstanceNum{1, 0, 0},
				Command: revisitedCommand(11, "B", shared),
			})

			view := rn.newExecutionView()
			var candidates []recoveryCandidate
			if !rn.componentReady(&view, []InstanceRef{a}, &candidates) {
				t.Fatalf("%s should not block %s: %s records %s in deps and has higher sequence", b, a, b, a)
			}
			rn.tryExecute()
			rd := rn.Ready()
			revisitedRequireCommittedRefs(t, rd.Committed, []InstanceRef{a})
			if rn.instances[b].rec.Status != status {
				t.Fatalf("known uncommitted conflict %s status = %s, want %s", b, rn.instances[b].rec.Status, status)
			}
		})
	}
}

func TestRevisitedChainPruningDoesNotIgnoreUnsafeKnownConflicts(t *testing.T) {
	shared := []byte("revisited-unsafe-conflict")
	base := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 1, Conf: 1}

	tests := []struct {
		name         string
		baseRef      InstanceRef
		baseSeq      uint64
		conflictRef  InstanceRef
		conflictSeq  uint64
		conflictDeps []InstanceNum
	}{
		{
			name:         "lower sequence",
			baseRef:      base,
			baseSeq:      2,
			conflictRef:  conflict,
			conflictSeq:  1,
			conflictDeps: []InstanceNum{1, 0, 0},
		},
		{
			name:         "equal sequence",
			baseRef:      base,
			baseSeq:      1,
			conflictRef:  conflict,
			conflictSeq:  1,
			conflictDeps: []InstanceNum{1, 0, 0},
		},
		{
			name:         "missing reverse dependency",
			baseRef:      base,
			baseSeq:      1,
			conflictRef:  conflict,
			conflictSeq:  2,
			conflictDeps: []InstanceNum{0, 0, 0},
		},
		{
			name:         "different configuration",
			baseRef:      base,
			baseSeq:      1,
			conflictRef:  InstanceRef{Replica: 2, Instance: 1, Conf: 2},
			conflictSeq:  2,
			conflictDeps: []InstanceNum{1, 0, 0},
		},
		{
			name:         "dependency vector too short",
			baseRef:      InstanceRef{Replica: 3, Instance: 1, Conf: 1},
			baseSeq:      1,
			conflictRef:  conflict,
			conflictSeq:  2,
			conflictDeps: []InstanceNum{0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn := revisitedRawNode(t)
			rn.confHistory[2] = ConfState{ID: 2, Voters: makeIDs(3)}
			revisitedInstall(rn, InstanceRecord{
				Ref:     tt.baseRef,
				Status:  StatusCommitted,
				Seq:     tt.baseSeq,
				Deps:    rn.depsForConf(tt.baseRef.Conf),
				Command: revisitedCommand(20, "base", shared),
			})
			revisitedInstall(rn, InstanceRecord{
				Ref:     tt.conflictRef,
				Status:  StatusAccepted,
				Seq:     tt.conflictSeq,
				Deps:    tt.conflictDeps,
				Command: revisitedCommand(21, "conflict", shared),
			})

			rn.tryExecute()
			rd := rn.Ready()
			if got := revisitedCommittedRefCount(rd.Committed, tt.baseRef); got != 0 {
				t.Fatalf("unsafe conflict emitted %s %d times, want blocked; ready=%#v", tt.baseRef, got, rd)
			}
			if rn.instances[tt.baseRef].rec.Status == StatusExecuted {
				t.Fatalf("unsafe conflict executed %s despite %s", tt.baseRef, tt.name)
			}
		})
	}
}

func TestRevisitedChainPruningDoesNotBypassUnknownDependency(t *testing.T) {
	for _, tt := range []struct {
		name string
		dep  *InstanceRecord
	}{
		{name: "missing"},
		{name: "status none", dep: &InstanceRecord{
			Ref:     InstanceRef{Replica: 2, Instance: 1, Conf: 1},
			Status:  StatusNone,
			Seq:     2,
			Deps:    []InstanceNum{1, 0, 0},
			Command: revisitedCommand(31, "unknown", []byte("revisited-unknown")),
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rn := revisitedRawNode(t)
			a := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
			revisitedInstall(rn, InstanceRecord{
				Ref:     a,
				Status:  StatusCommitted,
				Seq:     1,
				Deps:    []InstanceNum{0, 1, 0},
				Command: revisitedCommand(30, "base", []byte("revisited-unknown")),
			})
			if tt.dep != nil {
				revisitedInstall(rn, *tt.dep)
			}

			rn.tryExecute()
			rd := rn.Ready()
			if got := revisitedCommittedRefCount(rd.Committed, a); got != 0 {
				t.Fatalf("unknown dependency emitted %s %d times, want blocked; ready=%#v", a, got, rd)
			}
			if rn.instances[a].rec.Status == StatusExecuted {
				t.Fatalf("unknown dependency executed %s in %s case", a, tt.name)
			}

		})
	}
}

func TestRevisitedSparseMaxPrefixPrunesOnlyExactWitness(t *testing.T) {
	rn := revisitedRawNode(t)
	maxInstance := ^InstanceNum(0)
	base := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
	witness := InstanceRef{Conf: 1, Replica: 2, Instance: maxInstance}
	revisitedInstall(rn, InstanceRecord{Ref: base, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, maxInstance, 0}, Command: Command{Kind: CommandNoop}})
	revisitedInstall(rn, InstanceRecord{Ref: witness, Status: StatusCommitted, Seq: 2, Deps: []InstanceNum{1, 0, 0}, Command: Command{Kind: CommandNoop}})
	view := rn.newExecutionView()
	component := revisitedComponentContaining(rn.executionComponents(&view), base)
	if len(component) != 1 || component[0] != base {
		t.Fatalf("exact high witness did not prune only its materialized SCC edge: %v", component)
	}
	var candidates []recoveryCandidate
	if rn.componentReady(&view, component, &candidates) {
		t.Fatal("exact high witness incorrectly waived absent lower prefix")
	}
	want := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	if len(candidates) != 1 || candidates[0].ref != want {
		t.Fatalf("exact high witness recovery candidates = %v, want %s", candidates, want)
	}
	if rn.executed.prefix(laneFor(witness)) != 0 {
		t.Fatal("exact pruning advanced executed coverage")
	}
}

func revisitedRawNode(t *testing.T) *RawNode {
	t.Helper()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

func revisitedInstall(rn *RawNode, rec InstanceRecord) {
	rec = checkedRecord(rec)
	rn.installInstance(&instance{rec: rec, phase: phaseFromStatus(rec.Status)})
}

func revisitedCommand(client uint64, payload string, key []byte) Command {
	return Command{
		ID:           CommandID{Client: client, Sequence: 1},
		Payload:      []byte(payload),
		ConflictKeys: [][]byte{key},
	}
}

func revisitedComponentContaining(comps [][]InstanceRef, target InstanceRef) []InstanceRef {
	for _, comp := range comps {
		for _, ref := range comp {
			if ref == target {
				return append([]InstanceRef(nil), comp...)
			}
		}
	}
	return nil
}

func revisitedRequireCommittedRefs(t *testing.T, got []CommittedCommand, want []InstanceRef) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("committed refs = %v, want %v; committed=%#v", refs(got), want, got)
	}
	for i := range want {
		if got[i].Ref != want[i] {
			t.Fatalf("committed refs = %v, want %v; committed=%#v", refs(got), want, got)
		}
	}
}

func revisitedCommittedRefCount(committed []CommittedCommand, ref InstanceRef) int {
	var count int
	for _, cmd := range committed {
		if cmd.Ref == ref {
			count++
		}
	}
	return count
}
