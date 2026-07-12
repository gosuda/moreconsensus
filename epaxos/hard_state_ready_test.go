package epaxos

import (
	"errors"
	"reflect"
	"testing"
)

func TestInitialReadyCarriesCompleteHardState(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{3, 1, 2}, Storage: store})
	if err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	want := HardState{Conf: ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}}}
	if !rd.HardState.Equal(want) {
		t.Fatalf("initial hard state = %#v, want %#v", rd.HardState, want)
	}
	if !rd.MustSync || len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("initial Ready = %#v, want hard-state-only sync", rd)
	}
	repeated := rn.Ready()
	if !reflect.DeepEqual(repeated, rd) {
		t.Fatalf("repeated initial Ready = %#v, want frozen %#v", repeated, rd)
	}

	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(rd); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatalf("initial hard-state acknowledgement left Ready: %#v", rn.Ready())
	}
	state, err := store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if !state.HardState.Equal(want) {
		t.Fatalf("stored initial hard state = %#v, want %#v", state.HardState, want)
	}
}

func TestTickReadyPersistsAndRestartsLogicalTime(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(initial); err != nil {
		t.Fatal(err)
	}

	rn.Tick()
	rd := rn.Ready()
	if rd.HardState.Tick != 1 || rd.HardState.Conf.ID != 1 || !rd.MustSync {
		t.Fatalf("tick-only Ready = %#v, want complete hard state at tick 1", rd)
	}
	if len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("tick-only Ready carried payload work: %#v", rd)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.Status().Tick; got != 1 {
		t.Fatalf("restarted tick = %d, want 1", got)
	}
	if restarted.HasReady() {
		t.Fatalf("restart re-emitted acknowledged hard state: %#v", restarted.Ready())
	}

	if err := rn.Advance(rd); err != nil {
		t.Fatal(err)
	}
	rn.Tick()
	next := rn.Ready()
	if next.HardState.Tick != 2 {
		t.Fatalf("next hard-state tick = %d, want 2", next.HardState.Tick)
	}
}

func TestOutstandingReadyFrozenWhileTickAndStepAccumulate(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	frozen := rn.Ready()
	if frozen.HardState.Tick != 0 || frozen.HardState.Conf.ID != 1 {
		t.Fatalf("initial frozen Ready = %#v", frozen)
	}

	rn.Tick()
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	msg := Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{ID: CommandID{Client: 700, Sequence: 1}, Payload: []byte("frozen-step"), ConflictKeys: [][]byte{[]byte("frozen-step-key")}},
	}
	if err := rn.Step(msg); err != nil {
		t.Fatal(err)
	}
	if got := rn.Ready(); !reflect.DeepEqual(got, frozen) {
		t.Fatalf("outstanding Ready changed after Tick/Step: got %#v want %#v", got, frozen)
	}
	if len(rn.nextReady.Records) == 0 || len(rn.nextReady.Messages) == 0 {
		t.Fatalf("Tick/Step work did not remain in next batch: %#v", rn.nextReady)
	}

	if err := store.ApplyReady(frozen); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(frozen); err != nil {
		t.Fatal(err)
	}
	next := rn.Ready()
	if next.HardState.Tick != 1 || next.HardState.Conf.ID != 1 {
		t.Fatalf("next Ready hard state = %#v, want tick 1", next.HardState)
	}
	if len(next.Records) == 0 || len(next.Messages) == 0 || next.Records[0].Ref != ref {
		t.Fatalf("next Ready lost Step effects: %#v", next)
	}
}

func TestAdvanceHardStateAndPayloadPrefixesPreserveBarriers(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rn.Propose(Command{ID: CommandID{Client: 701, Sequence: 1}, Payload: []byte("prefix"), ConflictKeys: [][]byte{[]byte("prefix-key")}}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if rd.HardState.Empty() || len(rd.Records) == 0 || len(rd.Messages) == 0 {
		t.Fatalf("Ready lacks hard-state/payload barrier inputs: %#v", rd)
	}

	withoutHardState := Ready{Records: rd.Records[:1], MustSync: true}
	if err := rn.Advance(withoutHardState); !errors.Is(err, ErrInvalidReady) {
		t.Fatalf("record acknowledgement without hard state err = %v, want ErrInvalidReady", err)
	}
	before := rn.Ready()
	if !reflect.DeepEqual(before, rd) {
		t.Fatalf("invalid acknowledgement changed frozen Ready: got %#v want %#v", before, rd)
	}

	hardOnly := Ready{HardState: rd.HardState.Clone(), MustSync: true}
	if err := rn.Advance(hardOnly); err != nil {
		t.Fatalf("hard-state-only Advance: %v", err)
	}
	remaining := rn.Ready()
	if !remaining.HardState.Empty() || len(remaining.Records) != len(rd.Records) {
		t.Fatalf("remaining Ready after hard-state prefix = %#v", remaining)
	}
	if err := rn.Advance(Ready{Records: remaining.Records, MustSync: true}); err != nil {
		t.Fatalf("record-only prefix after hard state: %v", err)
	}
	payload := rn.Ready()
	if len(payload.Records) != 0 || len(payload.Messages) == 0 || payload.MustSync {
		t.Fatalf("payload Ready after durable prefixes = %#v", payload)
	}
	if err := rn.Advance(payload); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatalf("full prefix sequence left Ready: %#v", rn.Ready())
	}
}

func TestInvalidHardStateAcknowledgementsStutter(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	rn.Tick()
	canonical := rn.Ready()
	if canonical.HardState.Tick != 1 {
		t.Fatalf("canonical hard-state tick = %d, want 1", canonical.HardState.Tick)
	}

	mutations := []struct {
		name string
		edit func(*HardState)
	}{
		{name: "regressive tick", edit: func(h *HardState) { h.Tick = 0 }},
		{name: "future tick", edit: func(h *HardState) { h.Tick = 2 }},
		{name: "wrong configuration", edit: func(h *HardState) { h.Conf.Voters[2] = 4 }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			bad := canonical.Clone()
			mutation.edit(&bad.HardState)
			if err := rn.Advance(bad); !errors.Is(err, ErrInvalidReady) {
				t.Fatalf("Advance mutated hard state err = %v, want ErrInvalidReady", err)
			}
			if got := rn.Ready(); !reflect.DeepEqual(got, canonical) {
				t.Fatalf("invalid acknowledgement changed Ready: got %#v want %#v", got, canonical)
			}
		})
	}
	if err := rn.Advance(canonical); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStorageHardStateValidationIsAtomic(t *testing.T) {
	conf1 := ConfState{ID: 1, Voters: makeIDs(3)}
	baseRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	base := hardStateStorageRecord(baseRef, "base")
	store := NewMemoryStorage()
	if err := store.ApplyReady(Ready{HardState: HardState{Conf: conf1, Tick: 5}, Records: []InstanceRecord{base}, MustSync: true}); err != nil {
		t.Fatal(err)
	}

	newRef := InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	newRecord := hardStateStorageRecord(newRef, "new")
	cases := []struct {
		name string
		rd   Ready
	}{
		{
			name: "tick regression",
			rd:   Ready{HardState: HardState{Conf: conf1, Tick: 4}, Records: []InstanceRecord{newRecord}, MustSync: true},
		},
		{
			name: "same ID voter conflict",
			rd: Ready{HardState: HardState{
				Conf: ConfState{ID: 1, Voters: []ReplicaID{1, 2, 4}}, Tick: 6,
			}, Records: []InstanceRecord{newRecord}, MustSync: true},
		},
		{
			name: "incomplete hard state",
			rd:   Ready{HardState: HardState{Tick: 6}, Records: []InstanceRecord{newRecord}, MustSync: true},
		},
		{
			name: "record checksum",
			rd: func() Ready {
				bad := newRecord.Clone()
				bad.Command.Payload[0] ^= 0xff
				return Ready{HardState: HardState{Conf: conf1, Tick: 6}, Records: []InstanceRecord{bad}, MustSync: true}
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beforeHard := store.Hard.Clone()
			beforeBase, _ := store.Instance(baseRef)
			if err := store.ApplyReady(tc.rd); err == nil {
				t.Fatal("ApplyReady unexpectedly accepted invalid batch")
			}
			if !store.Hard.Equal(beforeHard) {
				t.Fatalf("rejected batch changed hard state: got %#v want %#v", store.Hard, beforeHard)
			}
			if _, ok := store.Instance(newRef); ok {
				t.Fatalf("rejected batch installed record %s", newRef)
			}
			afterBase, _ := store.Instance(baseRef)
			if !instanceRecordEqual(afterBase, beforeBase) {
				t.Fatalf("rejected batch changed existing record: got %#v want %#v", afterBase, beforeBase)
			}
		})
	}

	replay := Ready{HardState: store.Hard.Clone(), Records: []InstanceRecord{base.Clone()}, MustSync: true}
	if err := store.ApplyReady(replay); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if err := store.ApplyReady(replay); err != nil {
		t.Fatalf("second exact replay: %v", err)
	}
}

func TestMemoryStorageRejectsCurrentConfigurationRegressionAtomically(t *testing.T) {
	store := NewMemoryStorage()
	current := HardState{Conf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}, Tick: 8}
	if err := store.ApplyReady(Ready{HardState: current, MustSync: true}); err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 1, Instance: 3, Conf: 1}
	rec := hardStateStorageRecord(ref, "regression")
	regressive := Ready{
		HardState: HardState{Conf: ConfState{ID: 1, Voters: makeIDs(3)}, Tick: 9},
		Records:   []InstanceRecord{rec},
		MustSync:  true,
	}
	if err := store.ApplyReady(regressive); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("configuration regression err = %v, want ErrInvalidConfig", err)
	}
	if !store.Hard.Equal(current) {
		t.Fatalf("configuration regression changed hard state: got %#v want %#v", store.Hard, current)
	}
	if _, ok := store.Instance(ref); ok {
		t.Fatalf("configuration regression installed record %s", ref)
	}
}

func TestReadyHardStateCloneReleaseAndAckValidationAllocations(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	rn.Tick()
	rd := rn.Ready()
	clone := rd.Clone()
	clone.HardState.Conf.Voters[0] = 9
	if rd.HardState.Conf.Voters[0] != 1 {
		t.Fatalf("Ready.Clone shared hard-state voters: %#v", rd.HardState)
	}

	canonical := rn.Ready()
	var validationErr error
	allocs := testing.AllocsPerRun(1000, func() {
		validationErr = rn.validateReadyAck(canonical)
	})
	if validationErr != nil {
		t.Fatalf("validateReadyAck: %v", validationErr)
	}
	if allocs != 0 {
		t.Fatalf("warmed hard-state acknowledgement validation allocations = %v, want 0", allocs)
	}

	canonical.Release()
	if !canonical.Empty() || !canonical.HardState.Empty() {
		t.Fatalf("released Ready retained hard state: %#v", canonical)
	}
}

func TestConfigurationExecutionCarriesCompleteHardState(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	fence := fenceBootstrapPlan(t, f, plan)
	snapshot := certifyBootstrapSnapshot(t, f, plan, fence)
	ready := readyBootstrapTarget(t, f, plan, snapshot)
	ref, err := f.node.ActivateVoter(plan, snapshot, ready)
	if err != nil {
		t.Fatal(err)
	}
	rd := f.node.Ready()
	want := HardState{Conf: plan.Successor.Clone()}
	if !rd.HardState.Equal(want) {
		t.Fatalf("certified activation Ready hard state = %#v, want %#v", rd.HardState, want)
	}
	if !readyHasStatus(rd, ref, StatusExecuted) || !rd.MustSync ||
		rd.LocalVoterState == nil || len(rd.ConfigHistory) == 0 || len(rd.BootstrapRecords) == 0 {
		t.Fatalf("certified activation Ready lacks atomic durable state: %#v", rd)
	}
	if err := f.store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := f.node.Advance(rd); err != nil {
		t.Fatal(err)
	}

	identities := append(cloneVoterIdentities(f.identities), f.target.Clone())
	restarted, err := NewRawNode(Config{
		ID: 1, Voters: plan.Request.Base.Voters, Cluster: f.cluster,
		LocalIdentity: f.identities[0], VoterIdentities: identities, Storage: f.store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.currentHardState.Equal(want) {
		t.Fatalf("restarted current hard state = %#v, want %#v", restarted.currentHardState, want)
	}
	if restarted.HasReady() {
		t.Fatalf("restart re-emitted stored certified activation hard state: %#v", restarted.Ready())
	}
}

func TestTickGeneratedRetryIsFencedByHardState(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(initial); err != nil {
		t.Fatal(err)
	}
	if _, err := rn.Propose(Command{ID: CommandID{Client: 702, Sequence: 1}, Payload: []byte("retry-fence"), ConflictKeys: [][]byte{[]byte("retry-fence-key")}}); err != nil {
		t.Fatal(err)
	}
	proposal := rn.Ready()
	if err := store.ApplyReady(proposal); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(proposal); err != nil {
		t.Fatal(err)
	}

	rn.Tick()
	retry := rn.Ready()
	if retry.HardState.Tick != 1 || retry.HardState.Conf.ID != 1 || !retry.MustSync {
		t.Fatalf("retry Ready is not fenced by tick 1 hard state: %#v", retry)
	}
	if len(retry.Messages) == 0 {
		t.Fatalf("tick 1 produced no retry messages: %#v", retry)
	}
}

func TestCappedFrozenReadyDoesNotExposeLaterMessages(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, MaxReadyMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(initial); err != nil {
		t.Fatal(err)
	}

	first, err := rn.Propose(Command{ID: CommandID{Client: 703, Sequence: 1}, Payload: []byte("first-cap"), ConflictKeys: [][]byte{[]byte("first-cap-key")}})
	if err != nil {
		t.Fatal(err)
	}
	frozen := rn.Ready()
	if len(frozen.Messages) != 1 || frozen.Messages[0].Ref != first {
		t.Fatalf("frozen capped Ready = %#v, want one message for %s", frozen, first)
	}
	second, err := rn.Propose(Command{ID: CommandID{Client: 703, Sequence: 2}, Payload: []byte("second-cap"), ConflictKeys: [][]byte{[]byte("second-cap-key")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := rn.Ready(); !reflect.DeepEqual(got, frozen) {
		t.Fatalf("later proposal changed capped frozen Ready: got %#v want %#v", got, frozen)
	}
	if len(rn.nextReady.Records) == 0 || rn.nextReady.Records[0].Ref != second {
		t.Fatalf("later proposal was not isolated in next batch: %#v", rn.nextReady)
	}
	if err := store.ApplyReady(frozen); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(frozen); err != nil {
		t.Fatal(err)
	}
	next := rn.Ready()
	if len(next.Records) == 0 || next.Records[0].Ref != second {
		t.Fatalf("next Ready did not expose later proposal: %#v", next)
	}
	if len(next.Messages) != 1 || next.Messages[0].Ref != first {
		t.Fatalf("next Ready did not preserve older capped message prefix: %#v", next.Messages)
	}
}

func hardStateStorageRecord(ref InstanceRef, payload string) InstanceRecord {
	rec := InstanceRecord{
		Ref:              ref,
		Ballot:           Ballot{Replica: ref.Replica},
		RecordBallot:     Ballot{Replica: ref.Replica},
		Status:           StatusPreAccepted,
		Seq:              1,
		Deps:             make([]InstanceNum, 3),
		Command:          Command{ID: CommandID{Client: uint64(ref.Instance), Sequence: 1}, Payload: []byte(payload), ConflictKeys: [][]byte{[]byte(payload)}},
		FastPathEligible: true,
	}
	rec.Checksum = ChecksumRecord(rec)
	return rec
}
