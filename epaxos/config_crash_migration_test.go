package epaxos

import (
	"errors"
	"reflect"
	"testing"
)

func TestMultiTransitionHardStateRequiresCausalConfigRecordPrefix(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, initial)

	conf1To2 := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{
		Type: MsgCommit, From: 1, To: 1, Ref: conf1To2,
		Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1},
		Seq: 1, Deps: []InstanceNum{0, 0, 0},
		Command: confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}),
	}); err != nil {
		t.Fatal(err)
	}
	conf2 := ConfState{ID: 2, Voters: []ReplicaID{1, 2}}
	assertConfState(t, rn.Status().Conf, conf2)

	userRef := InstanceRef{Replica: 2, Instance: 1, Conf: 2}
	userCommand := configOrderingUserCommand(80, "conf2-before-conf3")
	if err := rn.Step(Message{
		Type:         MsgCommit,
		From:         userRef.Replica,
		To:           1,
		Ref:          userRef,
		Ballot:       Ballot{Replica: userRef.Replica},
		RecordBallot: Ballot{Replica: userRef.Replica},
		Seq:          1,
		Deps:         []InstanceNum{0, 0},
		Command:      userCommand,
	}); err != nil {
		t.Fatal(err)
	}
	if got := rn.instances[userRef].rec.Status; got != StatusExecuted {
		t.Fatalf("Conf2 user commit %s status=%s, want %s", userRef, got, StatusExecuted)
	}

	conf2To3 := InstanceRef{Replica: 2, Instance: 2, Conf: 2}
	if err := rn.Step(Message{
		Type:         MsgCommit,
		From:         conf2To3.Replica,
		To:           1,
		Ref:          conf2To3,
		Ballot:       Ballot{Replica: conf2To3.Replica},
		RecordBallot: Ballot{Replica: conf2To3.Replica},
		Seq:          2,
		Deps:         []InstanceNum{0, userRef.Instance},
		Command:      confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 2}),
	}); err != nil {
		t.Fatal(err)
	}
	conf3 := ConfState{ID: 3, Voters: []ReplicaID{1}}
	assertConfState(t, rn.Status().Conf, conf3)

	frozen := rn.Ready()
	if !frozen.HardState.Equal(HardState{Conf: conf3}) || !frozen.MustSync {
		t.Fatalf("multi-transition Ready hard state=%#v MustSync=%t, want Conf3 sync", frozen.HardState, frozen.MustSync)
	}
	applied := make([]int, 0, 2)
	for i, rec := range frozen.Records {
		if rec.Status == StatusExecuted && rec.Command.Kind == CommandConfChange &&
			rec.ConfChangeResult.Outcome == ConfChangeApplied {
			applied = append(applied, i)
		}
	}
	if len(applied) != 2 || frozen.Records[applied[0]].Ref != conf1To2 ||
		frozen.Records[applied[0]].ConfChangeResult.Conf.ID != conf2.ID ||
		frozen.Records[applied[1]].Ref != conf2To3 ||
		frozen.Records[applied[1]].ConfChangeResult.Conf.ID != conf3.ID {
		t.Fatalf("multi-transition Ready applied results=%#v records=%#v", applied, frozen.Records)
	}
	lastCausal := applied[len(applied)-1]

	assertRejectedStutter := func(name string, ack Ready) {
		t.Helper()
		if err := rn.Advance(ack); !errors.Is(err, ErrInvalidReady) {
			t.Fatalf("%s Advance err=%v, want %v", name, err, ErrInvalidReady)
		}
		if got := rn.Ready(); !reflect.DeepEqual(got, frozen) {
			t.Fatalf("%s Advance changed frozen Ready: got %#v want %#v", name, got, frozen)
		}
	}
	assertRejectedStutter("hard-state-only", Ready{HardState: frozen.HardState.Clone(), MustSync: true})
	short := frozen.Clone()
	short.Records = short.Records[:lastCausal]
	short.Messages = nil
	short.Committed = nil
	short.MustSync = true
	assertRejectedStutter("short-record-prefix", short)

	causal := frozen.Clone()
	causal.Records = causal.Records[:lastCausal+1]
	causal.Messages = nil
	causal.Committed = nil
	causal.MustSync = true
	if err := store.ApplyReady(causal); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(causal); err != nil {
		t.Fatalf("causal config prefix Advance: %v", err)
	}
	assertConfState(t, store.Hard.Conf, conf3)
	if len(store.Configs) != 2 {
		t.Fatalf("durable configuration history=%#v, want exact Conf2 and Conf3", store.Configs)
	}
	assertConfState(t, store.Configs[0], conf2)
	assertConfState(t, store.Configs[1], conf3)
	if got := store.Records[userRef].Status; got != StatusCommitted {
		t.Fatalf("durable old-Conf2 user record status=%s, want %s before application replay", got, StatusCommitted)
	}

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, conf3)
	if got := restarted.instances[userRef].rec.Status; got != StatusExecuted {
		t.Fatalf("restarted old-Conf2 user commit %s status=%s, want %s", userRef, got, StatusExecuted)
	}
	replay := restarted.Ready()
	if !replay.HardState.Empty() || len(replay.Records) != 0 || len(replay.Messages) != 0 ||
		len(replay.Committed) != 1 || replay.Committed[0].Ref != userRef ||
		!commandEqual(replay.Committed[0].Command, userCommand) || replay.MustSync {
		t.Fatalf("restarted old-Conf2 application replay=%#v, want one committed command", replay)
	}
	advanceOK(t, restarted, replay)
	executed := restarted.Ready()
	if len(executed.Records) != 1 || executed.Records[0].Ref != userRef ||
		executed.Records[0].Status != StatusExecuted || !executed.MustSync {
		t.Fatalf("old-Conf2 executed persistence Ready=%#v", executed)
	}
	if err := store.ApplyReady(executed); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, restarted, executed)

	again, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, again.Status().Conf, conf3)
	if again.HasReady() {
		t.Fatalf("fully persisted multi-transition restart produced Ready: %#v", again.Ready())
	}
}

func TestLegacyMigrationExplicitAppliedRemainsAuthoritative(t *testing.T) {
	conf2 := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}
	explicitRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	laterRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	earlierRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	explicit := checkedRecord(InstanceRecord{
		Ref:          explicitRef,
		Ballot:       Ballot{Replica: explicitRef.Replica},
		RecordBallot: Ballot{Replica: explicitRef.Replica},
		Status:       StatusExecuted,
		Seq:          3,
		Deps:         []InstanceNum{laterRef.Instance, earlierRef.Instance, 0},
		Command:      confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
		ConfChangeResult: ConfChangeResult{
			Outcome: ConfChangeApplied,
			Conf:    conf2,
		},
	})
	later := checkedRecord(InstanceRecord{
		Ref:          laterRef,
		Ballot:       Ballot{Replica: laterRef.Replica},
		RecordBallot: Ballot{Replica: laterRef.Replica},
		Status:       StatusExecuted,
		Seq:          2,
		Deps:         []InstanceNum{0, earlierRef.Instance, 0},
		Command:      confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 5}),
	})
	earlier := checkedRecord(InstanceRecord{
		Ref:          earlierRef,
		Ballot:       Ballot{Replica: earlierRef.Replica},
		RecordBallot: Ballot{Replica: earlierRef.Replica},
		Status:       StatusExecuted,
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		Command:      confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 6}),
	})
	records := []InstanceRecord{later, earlier, explicit}

	for _, reverse := range []bool{false, true} {
		name := "forward storage iteration"
		if reverse {
			name = "reversed storage iteration"
		}
		t.Run(name, func(t *testing.T) {
			storage := &configOutcomeReplayStorage{
				hard:    HardState{Conf: conf2},
				configs: []ConfState{conf2},
				records: records,
				reverse: reverse,
			}
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: storage})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, conf2)
			explicitResult := restarted.instances[explicitRef].rec.ConfChangeResult
			if explicitResult.Outcome != ConfChangeApplied {
				t.Fatalf("explicit result=%#v, want Applied", explicitResult)
			}
			assertConfState(t, explicitResult.Conf, conf2)
			for _, ref := range []InstanceRef{earlierRef, laterRef} {
				result := restarted.instances[ref].rec.ConfChangeResult
				if result.Outcome != ConfChangeRejectedSuperseded || !confStateIsZero(result.Conf) {
					t.Fatalf("legacy candidate %s result=%#v, want RejectedSuperseded", ref, result)
				}
			}

			rd := restarted.Ready()
			if !rd.HardState.Empty() || len(rd.Records) != 2 || rd.Records[0].Ref != earlierRef ||
				rd.Records[1].Ref != laterRef || !rd.MustSync {
				t.Fatalf("explicit-authoritative migration Ready=%#v, want causal legacy order", rd)
			}
			for _, rec := range rd.Records {
				if rec.ConfChangeResult.Outcome != ConfChangeRejectedSuperseded || !VerifyRecordChecksum(rec) {
					t.Fatalf("migrated legacy record %s result/checksum=%#v/%t", rec.Ref, rec.ConfChangeResult, VerifyRecordChecksum(rec))
				}
			}

			durable := NewMemoryStorage()
			durable.Hard = HardState{Conf: conf2.Clone()}
			durable.Configs = []ConfState{conf2.Clone()}
			durable.Records = map[InstanceRef]InstanceRecord{
				explicitRef: explicit.Clone(),
				laterRef:    later.Clone(),
				earlierRef:  earlier.Clone(),
			}
			if err := durable.ApplyReady(rd); err != nil {
				t.Fatal(err)
			}
			advanceOK(t, restarted, rd)
			again, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: durable})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, again.Status().Conf, conf2)
			if again.HasReady() {
				t.Fatalf("persisted explicit-authoritative migration produced Ready: %#v", again.Ready())
			}
		})
	}
}
