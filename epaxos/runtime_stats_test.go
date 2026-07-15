package epaxos

import "testing"

func TestIsExecutedAndRuntimeStatsAreAllocationFree(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 1000, Sequence: 1}, Payload: []byte("runtime-stats"), Footprint: Footprint{Points: [][]byte{[]byte("runtime-stats-key")}}})
	if err != nil {
		t.Fatal(err)
	}
	if !rn.IsExecuted(ref) {
		t.Fatal("single-voter proposal did not enter the executed tracker")
	}
	rd := rn.Ready()
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(rd); err != nil {
		t.Fatal(err)
	}
	if !rn.IsExecuted(ref) {
		t.Fatal("durably acknowledged committed command was not reported executed")
	}
	if got := rn.RuntimeStats(); got.ResidentInstances != 1 || got.ExecutedRefs != 1 {
		t.Fatalf("runtime stats=%#v, want one resident/executed ref", got)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if !rn.IsExecuted(ref) {
			t.Fatal("execution query changed")
		}
		_ = rn.RuntimeStats()
	}); allocs != 0 {
		t.Fatalf("runtime queries allocated %v times, want 0", allocs)
	}
}

func TestRuntimeStatsTracksReadyQueuesDeferralsRecoveriesAndLogicalAge(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 1001, Sequence: 1}, Payload: []byte("queue-stats"), Footprint: Footprint{Points: [][]byte{[]byte("queue-stats-key")}}})
	if err != nil {
		t.Fatal(err)
	}
	pending := rn.RuntimeStats()
	if pending.PendingReadyRecords == 0 || pending.PendingReadyMessages == 0 || pending.FrozenReadyRecords != 0 {
		t.Fatalf("pending RuntimeStats=%#v", pending)
	}
	var frozen Ready
	if err := rn.ReadyInto(&frozen); err != nil {
		t.Fatal(err)
	}
	frozenStats := rn.RuntimeStats()
	if frozenStats.FrozenReadyRecords == 0 || frozenStats.FrozenReadyMessages == 0 || frozenStats.PendingReadyRecords != 0 {
		t.Fatalf("frozen RuntimeStats=%#v", frozenStats)
	}
	inst := rn.instances[ref]
	if err := rn.Step(canonicalTestMessage(Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true})); err != nil { t.Fatal(err)
 }
	if got := rn.RuntimeStats(); got.NextReadyRecords == 0 || got.NextReadyMessages == 0 {
		t.Fatalf("concurrent Ready output not isolated in next queue: %#v", got)
	}

	timed, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 5, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	deferredRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	deferred := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: deferredRef, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: make([]InstanceNum, 3), Command: Command{Payload: []byte("deferred"), Footprint: Footprint{Points: [][]byte{[]byte("deferred-key")}}}, ProcessAt: 5}
	if err := timed.Step(canonicalTestMessage(deferred)); err != nil { t.Fatal(err)
 }
	if got := timed.RuntimeStats(); got.DeferredPreAccepts != 1 {
		t.Fatalf("deferred RuntimeStats=%#v", got)
	}
	for range 3 {
		if err := timed.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if got := timed.RuntimeStats(); got.OldestUnexecutedAgeTicks != 0 {
		t.Fatalf("deferred-only logical age=%d, want no materialized instance", got.OldestUnexecutedAgeTicks)
	}

	recoveryRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	recovery := &instance{rec: InstanceRecord{Ref: recoveryRef, Deps: timed.q.deps()}, createdTick: timed.tick}
	timed.instances[recoveryRef] = recovery
	if err := timed.startPrepare(recovery); err != nil {
		t.Fatal(err)
	}
	if got := timed.RuntimeStats(); got.ActiveRecoveries != 1 {
		t.Fatalf("recovery RuntimeStats=%#v", got)
	}
	if err := timed.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := timed.RuntimeStats(); got.OldestUnexecutedAgeTicks != 1 {
		t.Fatalf("logical age=%d, want 1 tick", got.OldestUnexecutedAgeTicks)
	}
}
