package epaxos

import (
	"errors"
	"testing"
)

func configOrderingAdvanceInitial(t *testing.T, rn *RawNode, store *MemoryStorage) {
	t.Helper()
	rd := rn.Ready()
	want := HardState{Conf: rn.Status().Conf, Tick: rn.tick}
	if !rd.HardState.Equal(want) || !rd.MustSync ||
		len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Apply) != 0 {
		t.Fatalf("initial Ready = %#v, want hard-state-only %#v", rd, want)
	}
	if store != nil {
		if err := store.ApplyReady(rd); err != nil {
			t.Fatal(err)
		}
	}
	advanceOK(t, rn, rd)
}

func TestInboundPendingConfChangeRejectsLocalUserProposeUntilExecuted(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}

	confRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	confCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	preAccept := Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     confRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: confCmd,
	}
	if err := rn.Step(canonicalTestMessage(preAccept)); err != nil { t.Fatalf("Step(%s config change) err=%v", preAccept.Type, err)
 }
	stored := rn.instances[confRef]
	if stored == nil || stored.rec.Status != StatusPreAccepted || stored.rec.Kind != EntryConfChange {
		t.Fatalf("stored config-change record for %s = %#v, want pre-accepted config change", confRef, stored)
	}
	if !rn.pendingConf {
		t.Fatalf("pre-accepted config change %s did not install pending configuration barrier", confRef)
	}

	if _, err := rn.Propose(configOrderingUserCommand(10, "blocked")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose while config change %s unexecuted err=%v, want %v", confRef, err, ErrMessageRejected)
	}

	commit := Message{
		Type:         MsgCommit,
		From:         1,
		To:           2,
		Ref:          confRef,
		Ballot:       stored.rec.Ballot,
		RecordBallot: stored.rec.RecordBallot,
		Seq:          stored.rec.Seq,
		Deps:         append([]InstanceNum(nil), stored.rec.Deps...),
		Kind:         stored.rec.Kind,
		ConfChange:   stored.rec.ConfChange,
		Command:      stored.rec.Command,
	}
	if err := rn.Step(canonicalTestMessage(commit)); err != nil { t.Fatalf("Step(%s config change) err=%v", commit.Type, err)
 }
	if rn.pendingConf {
		t.Fatalf("executed config change %s left pending configuration barrier set", confRef)
	}
	if got := rn.instances[confRef].rec.Status; got != StatusExecuted {
		t.Fatalf("config change %s status=%s, want %s", confRef, got, StatusExecuted)
	}

	acceptedRef, err := rn.Propose(configOrderingUserCommand(11, "after-conf"))
	if err != nil {
		t.Fatalf("Propose after executed config change %s err=%v", confRef, err)
	}
	if acceptedRef.Conf != confRef.Conf+1 {
		t.Fatalf("accepted proposal ref=%s, want configuration %d after %s", acceptedRef, confRef.Conf+1, confRef)
	}
}

func TestPendingConfChangeDependencyIncludedInUserPreAcceptResp(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	configOrderingAdvanceInitial(t, rn, nil)

	confRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	confCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	if err := rn.Step(canonicalTestMessage(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     confRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: confCmd,
	})); err != nil { t.Fatalf("Step(%s config change) err=%v", MsgPreAccept, err)
 }

	userRef := InstanceRef{Replica: 3, Instance: 1, Conf: confRef.Conf}
	userPreAccept := Message{
		Type:    MsgPreAccept,
		From:    3,
		To:      2,
		Ref:     userRef,
		Ballot:  Ballot{Replica: 3},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: configOrderingUserCommand(20, "after-known-conf"),
	}
	if err := rn.Step(canonicalTestMessage(userPreAccept)); err != nil { t.Fatalf("Step(%s user command) err=%v", userPreAccept.Type, err)
 }

	resp := requireConfigOrderingPreAcceptResp(t, rn.Ready().Messages, userRef)
	if resp.Reject {
		t.Fatalf("user pre-accept response for %s rejected: %#v", userRef, resp)
	}
	if resp.Ref.Conf != confRef.Conf {
		t.Fatalf("user pre-accept response ref=%s, want same configuration as %s", resp.Ref, confRef)
	}
	idx, ok := rn.confFor(confRef.Conf).Index(confRef.Replica)
	if !ok {
		t.Fatalf("configuration %d has no dependency slot for replica %d", confRef.Conf, confRef.Replica)
	}
	if len(resp.Deps) != len(rn.confFor(confRef.Conf).Voters) {
		t.Fatalf("user pre-accept response deps=%v, want width %d for configuration %d", resp.Deps, len(rn.confFor(confRef.Conf).Voters), confRef.Conf)
	}
	if got := resp.Deps[idx]; got != confRef.Instance {
		t.Fatalf("user pre-accept response deps=%v, want dependency on config change %s at slot %d", resp.Deps, confRef, idx)
	}
	if resp.Seq != 2 {
		t.Fatalf("user pre-accept response seq=%d, want 2 after config change dependency %s", resp.Seq, confRef)
	}
}

func TestRefreshPendingConfRetainsBarrierForSecondUnexecutedConfChange(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}

	executedRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	executedCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	if err := rn.Step(canonicalTestMessage(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     executedRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: executedCmd,
	})); err != nil { t.Fatalf("Step(%s first config change) err=%v", MsgPreAccept, err)
 }

	pendingRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	pendingCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 5})
	if err := rn.Step(canonicalTestMessage(Message{
		Type:    MsgPreAccept,
		From:    3,
		To:      2,
		Ref:     pendingRef,
		Ballot:  Ballot{Replica: 3},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: pendingCmd,
	})); err != nil { t.Fatalf("Step(%s second config change) err=%v", MsgPreAccept, err)
 }

	first := rn.instances[executedRef]
	if first == nil || first.rec.Kind != EntryConfChange || first.rec.Status != StatusPreAccepted {
		t.Fatalf("first config-change record for %s = %#v, want pre-accepted config change", executedRef, first)
	}
	second := rn.instances[pendingRef]
	if second == nil || second.rec.Kind != EntryConfChange || second.rec.Status != StatusPreAccepted {
		t.Fatalf("second config-change record for %s = %#v, want pre-accepted config change", pendingRef, second)
	}

	commit := Message{
		Type:         MsgCommit,
		From:         1,
		To:           2,
		Ref:          executedRef,
		Ballot:       first.rec.Ballot,
		RecordBallot: first.rec.RecordBallot,
		Seq:          first.rec.Seq,
		Deps:         append([]InstanceNum(nil), first.rec.Deps...),
		Kind:         first.rec.Kind,
		ConfChange:   first.rec.ConfChange,
		Command:      first.rec.Command,
	}
	if err := rn.Step(canonicalTestMessage(commit)); err != nil { t.Fatalf("Step(%s first config change) err=%v", commit.Type, err)
 }
	if got := rn.instances[executedRef].rec.Status; got != StatusExecuted {
		t.Fatalf("first config change %s status=%s, want %s", executedRef, got, StatusExecuted)
	}
	if got := rn.instances[pendingRef].rec.Status; got != StatusPreAccepted {
		t.Fatalf("second config change %s status=%s, want it to remain unexecuted", pendingRef, got)
	}
	if !rn.pendingConf {
		t.Fatalf("executing %s cleared pendingConf while unexecuted config change %s remained", executedRef, pendingRef)
	}
	if _, err := rn.Propose(configOrderingUserCommand(25, "still-blocked")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose with unexecuted config change %s remaining err=%v, want %v", pendingRef, err, ErrMessageRejected)
	}
	confAfterFirst := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}
	assertConfState(t, rn.Status().Conf, confAfterFirst)

	second = rn.instances[pendingRef]
	commitSecond := Message{
		Type:         MsgCommit,
		From:         3,
		To:           2,
		Ref:          pendingRef,
		Ballot:       second.rec.Ballot,
		RecordBallot: second.rec.RecordBallot,
		Seq:          second.rec.Seq,
		Deps:         append([]InstanceNum(nil), second.rec.Deps...),
		Kind:         second.rec.Kind,
		ConfChange:   second.rec.ConfChange,
		Command:      second.rec.Command,
	}
	if err := rn.Step(canonicalTestMessage(commitSecond)); err != nil { t.Fatalf("Step(%s second config change) err=%v", commitSecond.Type, err)
 }
	if got := rn.instances[pendingRef].rec.Status; got != StatusExecuted {
		t.Fatalf("second config change %s status=%s, want %s", pendingRef, got, StatusExecuted)
	}
	if rn.pendingConf {
		t.Fatalf("executed stale same-generation config change %s left pending configuration barrier set", pendingRef)
	}
	assertConfState(t, rn.Status().Conf, confAfterFirst)

}

func TestSingleVoterConfChangeClearsPendingBarrierAfterImmediateExecution(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	fence := fenceBootstrapPlan(t, f, plan)
	snapshot := certifyBootstrapSnapshot(t, f, plan, fence)
	ready := readyBootstrapTarget(t, f, plan, snapshot)

	confRef, err := f.node.ActivateVoter(plan, snapshot, ready)
	if err != nil {
		t.Fatal(err)
	}
	if f.node.pendingConf {
		t.Fatalf("single-voter certified activation %s executed immediately but left pending configuration barrier set", confRef)
	}
	if got := f.node.instances[confRef].rec.Status; got != StatusExecuted {
		t.Fatalf("single-voter certified activation %s status=%s, want %s", confRef, got, StatusExecuted)
	}
	persistBootstrapReady(t, f)

	userRef, err := f.node.Propose(configOrderingUserCommand(30, "after-single-conf"))
	if err != nil {
		t.Fatalf("Propose after immediately executed certified activation %s err=%v", confRef, err)
	}
	if userRef.Conf != plan.Successor.ID {
		t.Fatalf("proposal after single-voter certified activation used ref=%s, want config %d", userRef, plan.Successor.ID)
	}
}

func TestConfigReplayReconstructsHistoryFromExecutedRecordsOnRestart(t *testing.T) {
	store := NewMemoryStorage()
	addRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	removeRef := InstanceRef{Replica: 2, Instance: 1, Conf: 2}
	oldRef := InstanceRef{Replica: 3, Instance: 2, Conf: 1}
	midRef := InstanceRef{Replica: 4, Instance: 1, Conf: 2}

	store.Records[addRef] = checkedRecord(InstanceRecord{
		Ref:              addRef,
		Ballot:           Ballot{Replica: 1},
		Status:           StatusExecuted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 4},
		FastPathEligible: true,
	})
	store.Records[removeRef] = checkedRecord(InstanceRecord{
		Ref:              removeRef,
		Ballot:           Ballot{Replica: 2},
		Status:           StatusExecuted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeRemoveVoter, Replica: 3},
		FastPathEligible: true,
	})
	store.Records[oldRef] = checkedRecord(InstanceRecord{
		Ref:              oldRef,
		Ballot:           Ballot{Replica: 3},
		Status:           StatusPreAccepted,
		Seq:              2,
		Deps:             []InstanceNum{0, 0, 1},
		Command:          configOrderingUserCommand(40, "old-conf"),
		FastPathEligible: true,
	})
	store.Records[midRef] = checkedRecord(InstanceRecord{
		Ref:              midRef,
		Ballot:           Ballot{Replica: 4},
		Status:           StatusPreAccepted,
		Seq:              2,
		Deps:             []InstanceNum{0, 0, 1, 0},
		Command:          configOrderingUserCommand(41, "mid-conf"),
		FastPathEligible: true,
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.confFor(1), ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}})
	assertConfState(t, restarted.confFor(2), ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	assertConfState(t, restarted.confFor(3), ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}})
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}})

	assertConfigOrderingDepsWidth(t, restarted, oldRef, 3)
	assertConfigOrderingDepsWidth(t, restarted, midRef, 4)
	assertConfigOrderingDependencyLane(t, restarted, oldRef, InstanceRef{Replica: 3, Instance: 1, Conf: 1})
	assertConfigOrderingDependencyLane(t, restarted, midRef, InstanceRef{Replica: 3, Instance: 1, Conf: 2})

	if err := restarted.Step(canonicalTestMessage(Message{
		Type:    MsgPreAccept,
		From:    3,
		To:      1,
		Ref:     InstanceRef{Replica: 3, Instance: 3, Conf: 1},
		Ballot:  Ballot{Replica: 3},
		Seq:     3,
		Deps:    []InstanceNum{0, 0, 2},
		Command: configOrderingUserCommand(42, "old-conf-after-restart"),
	})); err != nil { t.Fatalf("Step accepted-domain %s after restart err=%v", MsgPreAccept, err)
 }
	if err := restarted.Step(canonicalTestMessage(Message{
		Type:    MsgPreAccept,
		From:    4,
		To:      1,
		Ref:     InstanceRef{Replica: 4, Instance: 2, Conf: 2},
		Ballot:  Ballot{Replica: 4},
		Seq:     3,
		Deps:    []InstanceNum{0, 0, 1, 1},
		Command: configOrderingUserCommand(43, "mid-conf-after-restart"),
	})); err != nil { t.Fatalf("Step accepted-domain %s after restart err=%v", MsgPreAccept, err)
 }

	ref, err := restarted.Propose(configOrderingUserCommand(44, "current-conf-after-restart"))
	if err != nil {
		t.Fatalf("Propose on current configuration after restart err=%v", err)
	}
	if ref.Conf != 3 {
		t.Fatalf("proposal after restart used ref=%s, want current configuration 3", ref)
	}
	assertConfigOrderingDepsWidth(t, restarted, ref, 3)

	removed, err := NewRawNode(Config{ID: 3, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, removed.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}})
	if _, err := removed.Propose(configOrderingUserCommand(45, "removed-user")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("removed voter Propose err=%v, want %v", err, ErrMessageRejected)
	}
	if _, err := removed.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 5}); !errors.Is(err, ErrVoterCertificateRequired) {
		t.Fatalf("removed voter legacy AddVoter err=%v, want %v", err, ErrVoterCertificateRequired)
	}
}

func TestConfigReplayRestoresPendingConfChangeBarrierOnRestart(t *testing.T) {
	store := NewMemoryStorage()
	addRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	removeRef := InstanceRef{Replica: 2, Instance: 1, Conf: 2}
	pendingRef := InstanceRef{Replica: 4, Instance: 1, Conf: 3}

	store.Records[addRef] = checkedRecord(InstanceRecord{
		Ref:              addRef,
		Ballot:           Ballot{Replica: 1},
		Status:           StatusExecuted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 4},
		FastPathEligible: true,
	})
	store.Records[removeRef] = checkedRecord(InstanceRecord{
		Ref:              removeRef,
		Ballot:           Ballot{Replica: 2},
		Status:           StatusExecuted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeRemoveVoter, Replica: 3},
		FastPathEligible: true,
	})
	store.Records[pendingRef] = checkedRecord(InstanceRecord{
		Ref:              pendingRef,
		Ballot:           Ballot{Replica: 4},
		Status:           StatusPreAccepted,
		Seq:              2,
		Deps:             []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 5},
		FastPathEligible: true,
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}})
	pending := restarted.instances[pendingRef]
	if pending == nil || pending.rec.Status != StatusPreAccepted || pending.rec.Kind != EntryConfChange {
		t.Fatalf("restarted pending config-change record for %s = %#v, want unexecuted config change", pendingRef, pending)
	}
	if !restarted.pendingConf {
		t.Fatalf("restarted node did not restore pending configuration barrier for unexecuted config change %s", pendingRef)
	}
	if _, err := restarted.Propose(configOrderingUserCommand(46, "blocked-by-replayed-pending-conf")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose with replayed pending config change %s err=%v, want %v", pendingRef, err, ErrMessageRejected)
	}
	if _, err := restarted.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 4}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("ProposeConfChange with replayed pending config change %s err=%v, want %v", pendingRef, err, ErrMessageRejected)
	}
}

func TestConfigReplayRejectsConflictingStoredConfigsOnRestart(t *testing.T) {
	newStore := func() *MemoryStorage {
		store := NewMemoryStorage()
		addRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		removeRef := InstanceRef{Replica: 2, Instance: 1, Conf: 2}

		store.Records[addRef] = checkedRecord(InstanceRecord{
			Ref:              addRef,
			Ballot:           Ballot{Replica: 1},
			Status:           StatusExecuted,
			Seq:              1,
			Deps:             []InstanceNum{0, 0, 0},
			Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 4},
			FastPathEligible: true,
		})
		store.Records[removeRef] = checkedRecord(InstanceRecord{
			Ref:              removeRef,
			Ballot:           Ballot{Replica: 2},
			Status:           StatusExecuted,
			Seq:              1,
			Deps:             []InstanceNum{0, 0, 0, 0},
			Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeRemoveVoter, Replica: 3},
			FastPathEligible: true,
		})
		return store
	}

	for _, tc := range []struct {
		name    string
		configs []ConfState
	}{
		{
			name: "conf2 conflicts with executed add voter replay",
			configs: []ConfState{
				{ID: 2, Voters: []ReplicaID{1, 2, 4}},
			},
		},
		{
			name: "conf3 conflicts with executed remove voter replay",
			configs: []ConfState{
				{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
				{ID: 3, Voters: []ReplicaID{1, 2, 3, 4}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore()
			store.Configs = append([]ConfState(nil), tc.configs...)

			_, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewRawNode replaying executed config records with stored configs %#v err=%v, want %v", tc.configs, err, ErrInvalidConfig)
			}
		})
	}
}

func TestConflictingHardStateConfigRejectedOnRestart(t *testing.T) {
	store := NewMemoryStorage()
	store.Hard = HardState{Conf: ConfState{ID: 1, Voters: []ReplicaID{1, 2, 4}}}

	_, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewRawNode with hard-state voters conflicting with initial config err=%v, want %v", err, ErrInvalidConfig)
	}
}

func TestConflictingDuplicateStoredConfigsRejectedOnRestart(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{
		{ID: 2, Voters: []ReplicaID{1, 2, 3}},
		{ID: 2, Voters: []ReplicaID{1, 2, 4}},
	}

	_, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewRawNode with duplicate config ID and conflicting voters err=%v, want %v", err, ErrInvalidConfig)
	}
}

func TestRestartSelectsHighestConfigurationIndependentOfHistoryOrder(t *testing.T) {
	conf2 := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}
	conf3 := ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}}
	for _, tc := range []struct {
		name    string
		hard    HardState
		configs []ConfState
	}{
		{name: "reversed history", configs: []ConfState{conf3, conf2}},
		{name: "hard state newer than history", hard: HardState{Conf: conf3}, configs: []ConfState{conf2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Hard = tc.hard
			store.Configs = append([]ConfState(nil), tc.configs...)
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, conf3)
		})
	}
}

func TestConfigReplayMigratesMalformedConfChangeToRejectedInvalid(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusExecuted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter},
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 1, Voters: makeIDs(3)})
	got := restarted.instances[ref].rec.ConfChangeResult
	if got.Outcome != ConfChangeRejectedInvalid || !confStateIsZero(got.Conf) {
		t.Fatalf("malformed durable config result=%#v, want RejectedInvalid with zero config", got)
	}
	rd := restarted.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref ||
		rd.Records[0].ConfChangeResult.Outcome != ConfChangeRejectedInvalid ||
		!VerifyRecordChecksum(rd.Records[0]) {
		t.Fatalf("malformed durable config migration Ready=%#v", rd)
	}
}

func TestInvalidConfChangeConfigReplayBecomesRejectedInvalid(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusExecuted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 2},
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 1, Voters: makeIDs(3)})
	got := restarted.instances[ref].rec.ConfChangeResult
	if got.Outcome != ConfChangeRejectedInvalid || !confStateIsZero(got.Conf) {
		t.Fatalf("invalid durable config result=%#v, want RejectedInvalid with zero config", got)
	}
}

type configOutcomeReplayStorage struct {
	hard    HardState
	configs []ConfState
	records []InstanceRecord
	reverse bool
}

func (s *configOutcomeReplayStorage) InitialState() (StorageState, error) {
	history := make([]ConfigHistoryEntry, len(s.configs))
	for i := range s.configs {
		history[i] = ConfigHistoryEntry{Conf: s.configs[i].Clone()}
	}
	return StorageState{HardState: HardState{Conf: s.hard.Conf.Clone(), Tick: s.hard.Tick}, ConfigHistory: history}, nil
}

func (s *configOutcomeReplayStorage) LoadCheckpoint() (Checkpoint, error) { return Checkpoint{}, nil }

func (s *configOutcomeReplayStorage) LoadInstances(_ ExecutionFrontier, fn func(InstanceRecord) error) error {
	for i := range s.records {
		index := i
		if s.reverse {
			index = len(s.records) - 1 - i
		}
		if err := fn(s.records[index].Clone()); err != nil {
			return err
		}
	}
	return nil
}

func (s *configOutcomeReplayStorage) LoadInstance(ref InstanceRef) (InstanceRecord, bool, error) {
	for _, record := range s.records {
		if record.Ref == ref {
			return record.Clone(), true, nil
		}
	}
	return InstanceRecord{}, false, nil
}

func TestConcurrentSameGenerationConfigOutcomesReplayExecutionWinner(t *testing.T) {
	for _, tc := range []struct {
		name        string
		withConfigs bool
		reverse     bool
	}{
		{name: "records only live iteration"},
		{name: "records only reversed iteration", reverse: true},
		{name: "stored configs live iteration", withConfigs: true},
		{name: "stored configs reversed iteration", withConfigs: true, reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
			if err != nil {
				t.Fatal(err)
			}
			configOrderingAdvanceInitial(t, rn, nil)
			add4Ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
			add5Ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
			add5 := checkedRecord(InstanceRecord{
				Ref:          add5Ref,
				Ballot:       Ballot{Replica: 2},
				RecordBallot: Ballot{Replica: 2},
				Status:       StatusCommitted,
				Seq:          1,
				Deps:         []InstanceNum{0, 0, 0},
				Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 5},
			})
			add4 := checkedRecord(InstanceRecord{
				Ref:          add4Ref,
				Ballot:       Ballot{Replica: 1},
				RecordBallot: Ballot{Replica: 1},
				Status:       StatusCommitted,
				Seq:          2,
				Deps:         []InstanceNum{0, 1, 0},
				Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 4},
			})
			// Insert in reference order even though the dependency requires Add5
			// to execute first.
			rn.instances[add4Ref] = &instance{rec: add4, phase: phaseCommitted}
			rn.instances[add5Ref] = &instance{rec: add5, phase: phaseCommitted}
			rn.markPendingConf(add4)
			rn.markPendingConf(add5)
			rn.tryExecute()

			wantConf2 := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 5}}
			assertConfState(t, rn.Status().Conf, wantConf2)
			winner := rn.instances[add5Ref].rec
			loser := rn.instances[add4Ref].rec
			if winner.ConfChangeResult.Outcome != ConfChangeApplied {
				t.Fatalf("Add5 outcome=%v, want Applied", winner.ConfChangeResult.Outcome)
			}
			assertConfState(t, winner.ConfChangeResult.Conf, wantConf2)
			if loser.ConfChangeResult.Outcome != ConfChangeRejectedSuperseded ||
				!confStateIsZero(loser.ConfChangeResult.Conf) {
				t.Fatalf("Add4 result=%#v, want RejectedSuperseded with zero config", loser.ConfChangeResult)
			}
			if rn.pendingConf {
				t.Fatal("terminal concurrent configuration outcomes left pending barrier set")
			}

			rd := rn.Ready()
			if !rd.HardState.Equal(HardState{Conf: wantConf2}) || !rd.MustSync {
				t.Fatalf("executed configuration Ready hard state=%#v, want current configuration %#v", rd.HardState, wantConf2)
			}
			if len(rd.Records) != 2 || rd.Records[0].Ref != add5Ref || rd.Records[1].Ref != add4Ref {
				t.Fatalf("executed configuration Ready order=%#v, want Add5 winner then Add4 loser", rd.Records)
			}
			for _, rec := range rd.Records {
				if !VerifyRecordChecksum(rec) {
					t.Fatalf("executed result for %s has invalid checksum", rec.Ref)
				}
			}
			durable := NewMemoryStorage()
			if err := durable.ApplyReady(rd); err != nil {
				t.Fatal(err)
			}
			assertConfState(t, durable.Hard.Conf, wantConf2)
			if len(durable.Configs) != 1 {
				t.Fatalf("durable configuration checkpoints=%#v, want exactly Conf2", durable.Configs)
			}
			assertConfState(t, durable.Configs[0], wantConf2)
			if err := durable.ApplyReady(rd); err != nil {
				t.Fatalf("idempotent Ready reapply: %v", err)
			}
			if len(durable.Configs) != 1 {
				t.Fatalf("idempotent Ready reapply duplicated configurations: %#v", durable.Configs)
			}
			if err := rn.Advance(rd); err != nil {
				t.Fatal(err)
			}

			replay := &configOutcomeReplayStorage{
				records: []InstanceRecord{
					durable.Records[add5Ref].Clone(),
					durable.Records[add4Ref].Clone(),
				},
				reverse: tc.reverse,
			}
			if tc.withConfigs {
				replay.hard = durable.Hard
				replay.configs = append([]ConfState(nil), durable.Configs...)
			}
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: replay})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, wantConf2)
			replayedWinner := restarted.instances[add5Ref].rec
			replayedLoser := restarted.instances[add4Ref].rec
			if replayedWinner.ConfChangeResult.Outcome != ConfChangeApplied ||
				replayedLoser.ConfChangeResult.Outcome != ConfChangeRejectedSuperseded {
				t.Fatalf("replayed outcomes winner=%#v loser=%#v", replayedWinner.ConfChangeResult, replayedLoser.ConfChangeResult)
			}
			assertConfState(t, replayedWinner.ConfChangeResult.Conf, wantConf2)
			if tc.withConfigs {
				if restarted.HasReady() {
					t.Fatalf("matching persisted hard state was re-emitted: %#v", restarted.Ready())
				}
			} else {
				initial := restarted.Ready()
				wantHard := HardState{Conf: wantConf2}
				if !initial.HardState.Equal(wantHard) || !initial.MustSync ||
					len(initial.Records) != 0 || len(initial.Messages) != 0 || len(initial.Apply) != 0 {
					t.Fatalf("legacy restart initial Ready = %#v, want hard-state-only %#v", initial, wantHard)
				}
				restartDurable := NewMemoryStorage()
				if err := restartDurable.ApplyReady(initial); err != nil {
					t.Fatal(err)
				}
				advanceOK(t, restarted, initial)
				if restarted.HasReady() {
					t.Fatalf("initial hard-state acknowledgement left Ready: %#v", restarted.Ready())
				}
			}
		})
	}
}

func TestLegacyDuplicateConfigOutcomesMigrateInExecutionOrder(t *testing.T) {
	add5 := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 5})
	tests := []struct {
		name      string
		records   []InstanceRecord
		winnerRef InstanceRef
		loserRef  InstanceRef
	}{
		{
			name: "dependency order overrides reference order",
			records: []InstanceRecord{
				checkedRecord(InstanceRecord{
					Ref:          InstanceRef{Replica: 1, Instance: 1, Conf: 1},
					Ballot:       Ballot{Replica: 1},
					RecordBallot: Ballot{Replica: 1},
					Status:       StatusExecuted,
					Seq:          2,
					Deps:         []InstanceNum{0, 1, 0},
					Kind: EntryConfChange, ConfChange: add5,
				}),
				checkedRecord(InstanceRecord{
					Ref:          InstanceRef{Replica: 2, Instance: 1, Conf: 1},
					Ballot:       Ballot{Replica: 2},
					RecordBallot: Ballot{Replica: 2},
					Status:       StatusExecuted,
					Seq:          1,
					Deps:         []InstanceNum{0, 0, 0},
					Kind: EntryConfChange, ConfChange: add5,
				}),
			},
			winnerRef: InstanceRef{Replica: 2, Instance: 1, Conf: 1},
			loserRef:  InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		},
		{
			name: "equal sequence SCC uses reference tie",
			records: []InstanceRecord{
				checkedRecord(InstanceRecord{
					Ref:          InstanceRef{Replica: 1, Instance: 1, Conf: 1},
					Ballot:       Ballot{Replica: 1},
					RecordBallot: Ballot{Replica: 1},
					Status:       StatusExecuted,
					Seq:          2,
					Deps:         []InstanceNum{0, 1, 0},
					Kind: EntryConfChange, ConfChange: add5,
				}),
				checkedRecord(InstanceRecord{
					Ref:          InstanceRef{Replica: 2, Instance: 1, Conf: 1},
					Ballot:       Ballot{Replica: 2},
					RecordBallot: Ballot{Replica: 2},
					Status:       StatusExecuted,
					Seq:          2,
					Deps:         []InstanceNum{1, 0, 0},
					Kind: EntryConfChange, ConfChange: add5,
				}),
			},
			winnerRef: InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			loserRef:  InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		},
	}
	for _, tc := range tests {
		for _, reverse := range []bool{false, true} {
			name := "forward storage iteration"
			if reverse {
				name = "reversed storage iteration"
			}
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				store := &configOutcomeReplayStorage{records: tc.records, reverse: reverse}
				restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
				if err != nil {
					t.Fatal(err)
				}
				want := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 5}}
				assertConfState(t, restarted.Status().Conf, want)
				winner := restarted.instances[tc.winnerRef].rec
				loser := restarted.instances[tc.loserRef].rec
				if winner.ConfChangeResult.Outcome != ConfChangeApplied {
					t.Fatalf("legacy winner %s result=%#v, want Applied", tc.winnerRef, winner.ConfChangeResult)
				}
				assertConfState(t, winner.ConfChangeResult.Conf, want)
				if loser.ConfChangeResult.Outcome != ConfChangeRejectedSuperseded ||
					!confStateIsZero(loser.ConfChangeResult.Conf) {
					t.Fatalf("legacy loser %s result=%#v, want RejectedSuperseded", tc.loserRef, loser.ConfChangeResult)
				}

				rd := restarted.Ready()
				if !rd.HardState.Equal(HardState{Conf: want}) || len(rd.Records) != 2 ||
					rd.Records[0].Ref != tc.winnerRef || rd.Records[1].Ref != tc.loserRef {
					t.Fatalf("legacy migration Ready=%#v, want winner then loser and Conf2 hard state", rd)
				}
				for _, rec := range rd.Records {
					if !VerifyRecordChecksum(rec) {
						t.Fatalf("migrated legacy result for %s has invalid checksum", rec.Ref)
					}
				}
				durable := NewMemoryStorage()
				if err := durable.ApplyReady(rd); err != nil {
					t.Fatal(err)
				}
				advanceOK(t, restarted, rd)
				again, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: durable})
				if err != nil {
					t.Fatal(err)
				}
				assertConfState(t, again.Status().Conf, want)
				if again.HasReady() {
					t.Fatalf("persisted migrated outcomes produced restart Ready: %#v", again.Ready())
				}
			})
		}
	}
}

func TestLegacyInvalidBeforeValidConfigMigrationDoesNotConsumeGeneration(t *testing.T) {
	invalidRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	validRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	records := []InstanceRecord{
		checkedRecord(InstanceRecord{
			Ref:          invalidRef,
			Ballot:       Ballot{Replica: 1},
			RecordBallot: Ballot{Replica: 1},
			Status:       StatusExecuted,
			Seq:          1,
			Deps:         []InstanceNum{0, 0, 0},
			Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 2},
		}),
		checkedRecord(InstanceRecord{
			Ref:          validRef,
			Ballot:       Ballot{Replica: 2},
			RecordBallot: Ballot{Replica: 2},
			Status:       StatusExecuted,
			Seq:          2,
			Deps:         []InstanceNum{1, 0, 0},
			Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 4},
		}),
	}
	for _, reverse := range []bool{false, true} {
		store := &configOutcomeReplayStorage{records: records, reverse: reverse}
		restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
		if err != nil {
			t.Fatal(err)
		}
		invalidResult := restarted.instances[invalidRef].rec.ConfChangeResult
		if invalidResult.Outcome != ConfChangeRejectedInvalid || !confStateIsZero(invalidResult.Conf) {
			t.Fatalf("legacy invalid-first result=%#v, want RejectedInvalid", invalidResult)
		}
		want := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}
		validResult := restarted.instances[validRef].rec.ConfChangeResult
		if validResult.Outcome != ConfChangeApplied {
			t.Fatalf("legacy later-valid result=%#v, want Applied", validResult)
		}
		assertConfState(t, validResult.Conf, want)
		rd := restarted.Ready()
		if len(rd.Records) != 2 || rd.Records[0].Ref != invalidRef || rd.Records[1].Ref != validRef {
			t.Fatalf("legacy invalid-first migration Ready=%#v", rd.Records)
		}
		for _, rec := range rd.Records {
			if !VerifyRecordChecksum(rec) {
				t.Fatalf("legacy invalid-first migrated record %s has invalid checksum", rec.Ref)
			}
		}
		durable := NewMemoryStorage()
		if err := durable.ApplyReady(rd); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, restarted, rd)
		again, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: durable})
		if err != nil {
			t.Fatal(err)
		}
		assertConfState(t, again.Status().Conf, want)
		if again.HasReady() {
			t.Fatalf("persisted invalid-first migration produced restart Ready: %#v", again.Ready())
		}
	}
}

func TestConfigReplayRejectsInvalidExplicitOutcomeChains(t *testing.T) {
	baseRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	record := func(ref InstanceRef, change ConfChange, result ConfChangeResult) InstanceRecord {
		return checkedRecord(InstanceRecord{
			Ref:              ref,
			Ballot:           Ballot{Replica: ref.Replica},
			RecordBallot:     Ballot{Replica: ref.Replica},
			Status:           StatusExecuted,
			Seq:              1,
			Deps:             []InstanceNum{0, 0, 0},
			Kind: EntryConfChange, ConfChange: change,
			ConfChangeResult: result,
		})
	}
	applied4 := record(baseRef,
		ConfChange{Type: ConfChangeAddVoter, Replica: 4},
		ConfChangeResult{Outcome: ConfChangeApplied, Conf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}})
	for _, tc := range []struct {
		name    string
		records []InstanceRecord
	}{
		{
			name: "two applied results",
			records: []InstanceRecord{
				applied4,
				record(InstanceRef{Replica: 2, Instance: 1, Conf: 1},
					ConfChange{Type: ConfChangeAddVoter, Replica: 5},
					ConfChangeResult{Outcome: ConfChangeApplied, Conf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 5}}}),
			},
		},
		{
			name: "applied voters disagree with command",
			records: []InstanceRecord{
				record(baseRef,
					ConfChange{Type: ConfChangeAddVoter, Replica: 4},
					ConfChangeResult{Outcome: ConfChangeApplied, Conf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 6}}}),
			},
		},
		{
			name: "valid command labeled rejected invalid",
			records: []InstanceRecord{
				record(baseRef,
					ConfChange{Type: ConfChangeAddVoter, Replica: 4},
					ConfChangeResult{Outcome: ConfChangeRejectedInvalid}),
			},
		},
		{
			name: "invalid command labeled superseded",
			records: []InstanceRecord{
				record(baseRef,
					ConfChange{Type: ConfChangeAddVoter, Replica: 2},
					ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}),
			},
		},
		{
			name: "superseded without winner or checkpoint",
			records: []InstanceRecord{
				record(baseRef,
					ConfChange{Type: ConfChangeAddVoter, Replica: 4},
					ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}),
			},
		},
		{
			name: "rejection carries configuration",
			records: []InstanceRecord{
				record(baseRef,
					ConfChange{Type: ConfChangeAddVoter, Replica: 4},
					ConfChangeResult{Outcome: ConfChangeRejectedSuperseded, Conf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &configOutcomeReplayStorage{records: tc.records}
			if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store}); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewRawNode invalid outcome chain err=%v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestConfChangeResultReadyFencesSuccessorMessages(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	configOrderingAdvanceInitial(t, rn, store)
	confRef, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3})
	if err != nil {
		t.Fatal(err)
	}
	inst := rn.instances[confRef]
	if err := rn.Step(canonicalTestMessage(Message{
		Type: MsgPreAcceptResp, From: 2, To: 1, Ref: confRef,
		Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: append([]InstanceNum(nil), inst.rec.Deps...),
		FastPathEligible: true,
	})); err != nil { t.Fatal(err)
 }
	successorRef, err := rn.Propose(configOrderingUserCommand(70, "successor-before-advance"))
	if err != nil {
		t.Fatal(err)
	}
	if successorRef.Conf != 2 {
		t.Fatalf("successor proposal ref=%s, want configuration 2", successorRef)
	}
	rd := rn.Ready()
	configResultIndex := -1
	successorRecordIndex := -1
	for i, rec := range rd.Records {
		if rec.Ref == confRef && rec.Status == StatusExecuted {
			if rec.ConfChangeResult.Outcome != ConfChangeApplied {
				t.Fatalf("configuration execution result=%#v, want Applied", rec.ConfChangeResult)
			}
			configResultIndex = i
		}
		if rec.Ref == successorRef {
			successorRecordIndex = i
		}
	}
	if configResultIndex < 0 || successorRecordIndex <= configResultIndex {
		t.Fatalf("Ready records do not fence successor after applied result: config=%d successor=%d records=%#v", configResultIndex, successorRecordIndex, rd.Records)
	}
	hasSuccessorMessage := false
	for _, message := range rd.Messages {
		if message.Ref == successorRef {
			hasSuccessorMessage = true
		}
	}
	if !hasSuccessorMessage {
		t.Fatalf("Ready has no successor message for %s: %#v", successorRef, rd.Messages)
	}
	if err := rn.Advance(Ready{HardState: rd.HardState, Messages: rd.Messages, MustSync: rd.MustSync}); !errors.Is(err, ErrInvalidReady) {
		t.Fatalf("Advance successor messages without applied result record err=%v, want ErrInvalidReady", err)
	}
	retry := rn.Ready()
	if len(retry.Records) != len(rd.Records) || retry.Records[configResultIndex].ConfChangeResult.Outcome != ConfChangeApplied {
		t.Fatalf("failed successor-only Advance changed durable result prefix: %#v", retry.Records)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(rd); err != nil {
		t.Fatal(err)
	}
	assertConfState(t, store.Hard.Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2}})
}

func TestInvalidExecutedConfChangeDoesNotConsumeGeneration(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	configOrderingAdvanceInitial(t, rn, nil)
	invalidRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	validRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	invalid := checkedRecord(InstanceRecord{
		Ref:          invalidRef,
		Ballot:       Ballot{Replica: 1},
		RecordBallot: Ballot{Replica: 1},
		Status:       StatusCommitted,
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 2},
	})
	valid := checkedRecord(InstanceRecord{
		Ref:          validRef,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Status:       StatusCommitted,
		Seq:          2,
		Deps:         []InstanceNum{1, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 4},
	})
	rn.instances[invalidRef] = &instance{rec: invalid, phase: phaseCommitted}
	rn.instances[validRef] = &instance{rec: valid, phase: phaseCommitted}
	rn.markPendingConf(invalid)
	rn.markPendingConf(valid)
	rn.tryExecute()

	invalidResult := rn.instances[invalidRef].rec.ConfChangeResult
	if invalidResult.Outcome != ConfChangeRejectedInvalid || !confStateIsZero(invalidResult.Conf) {
		t.Fatalf("invalid-first result=%#v, want RejectedInvalid", invalidResult)
	}
	want := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}
	validResult := rn.instances[validRef].rec.ConfChangeResult
	if validResult.Outcome != ConfChangeApplied {
		t.Fatalf("later valid result=%#v, want Applied", validResult)
	}
	assertConfState(t, validResult.Conf, want)
	assertConfState(t, rn.Status().Conf, want)
	if rn.pendingConf {
		t.Fatal("terminal invalid and applied configuration commands left pending barrier set")
	}
	rd := rn.Ready()
	if len(rd.Records) != 2 || rd.Records[0].Ref != invalidRef || rd.Records[1].Ref != validRef {
		t.Fatalf("invalid-first Ready order=%#v", rd.Records)
	}
}

func TestConfigReplayAcceptsSupersededWithStoredWinnerCheckpoint(t *testing.T) {
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	loser := checkedRecord(InstanceRecord{
		Ref:          ref,
		Ballot:       Ballot{Replica: 1},
		RecordBallot: Ballot{Replica: 1},
		Status:       StatusExecuted,
		Seq:          2,
		Deps:         []InstanceNum{0, 0, 0},
		Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: 4},
		ConfChangeResult: ConfChangeResult{
			Outcome: ConfChangeRejectedSuperseded,
		},
	})
	winnerCheckpoint := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 5}}
	store := &configOutcomeReplayStorage{
		hard:    HardState{Conf: winnerCheckpoint},
		configs: []ConfState{winnerCheckpoint},
		records: []InstanceRecord{loser},
	}
	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, winnerCheckpoint)
	if got := restarted.instances[ref].rec.ConfChangeResult.Outcome; got != ConfChangeRejectedSuperseded {
		t.Fatalf("checkpointed loser outcome=%v, want RejectedSuperseded", got)
	}
}

func TestMemoryStorageRejectsConflictingAppliedSuccessorAtomically(t *testing.T) {
	appliedRecord := func(ref InstanceRef, replica ReplicaID) InstanceRecord {
		voters := []ReplicaID{1, 2, 3, replica}
		return checkedRecord(InstanceRecord{
			Ref:          ref,
			Ballot:       Ballot{Replica: ref.Replica},
			RecordBallot: Ballot{Replica: ref.Replica},
			Status:       StatusExecuted,
			Seq:          1,
			Deps:         []InstanceNum{0, 0, 0},
			Kind: EntryConfChange, ConfChange: ConfChange{Type: ConfChangeAddVoter, Replica: replica},
			ConfChangeResult: ConfChangeResult{
				Outcome: ConfChangeApplied,
				Conf:    ConfState{ID: 2, Voters: voters},
			},
		})
	}
	store := NewMemoryStorage()
	firstRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	first := appliedRecord(firstRef, 4)
	if err := store.ApplyReady(Ready{HardState: HardState{Conf: first.ConfChangeResult.Conf}, Records: []InstanceRecord{first}, MustSync: true}); err != nil {
		t.Fatal(err)
	}
	want := first.ConfChangeResult.Conf
	assertConfState(t, store.Hard.Conf, want)

	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	conflict := appliedRecord(conflictRef, 5)
	if err := store.ApplyReady(Ready{HardState: HardState{Conf: conflict.ConfChangeResult.Conf}, Records: []InstanceRecord{conflict}, MustSync: true}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ApplyReady conflicting successor err=%v, want ErrInvalidConfig", err)
	}
	assertConfState(t, store.Hard.Conf, want)
	if _, exists := store.Records[conflictRef]; exists {
		t.Fatalf("conflicting ApplyReady partially persisted record %s", conflictRef)
	}
	if len(store.Configs) != 1 {
		t.Fatalf("conflicting ApplyReady changed checkpoints: %#v", store.Configs)
	}
	assertConfState(t, store.Configs[0], want)
}

func assertConfigOrderingDepsWidth(t *testing.T, rn *RawNode, ref InstanceRef, want int) {
	t.Helper()
	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing instance %s after restart", ref)
	}
	if got := len(inst.rec.Deps); got != want {
		t.Fatalf("instance %s deps=%v, want width %d", ref, inst.rec.Deps, want)
	}
}

func assertConfigOrderingDependencyLane(t *testing.T, rn *RawNode, ref, want InstanceRef) {
	t.Helper()
	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing instance %s", ref)
	}
	conf := rn.confFor(ref.Conf)
	slot, ok := conf.Index(want.Replica)
	if !ok || slot >= len(inst.rec.Deps) || inst.rec.Deps[slot] != want.Instance || want.Conf != ref.Conf {
		t.Fatalf("compact dependency lane for %s = conf %#v deps %v, want through %s", ref, conf, inst.rec.Deps, want)
	}
	view := rn.newExecutionView()
	iter := view.dependencyRefs(rn, ref)
	if dependency, ok := iter.next(); ok {
		t.Fatalf("absent compact dependency was materialized as graph ref %s", dependency)
	}
}

func configOrderingUserCommand(sequence uint64, payload string) Command {
	return Command{
		ID:        CommandID{Client: 91, Sequence: sequence},
		Payload:   []byte(payload),
		Footprint: Footprint{Points: [][]byte{[]byte("user-key")}},
	}
}

func requireConfigOrderingPreAcceptResp(t *testing.T, messages []Message, ref InstanceRef) Message {
	t.Helper()
	for _, m := range messages {
		if m.Type == MsgPreAcceptResp && m.Ref == ref {
			return m
		}
	}
	t.Fatalf("Ready messages did not include %s for %s: %#v", MsgPreAcceptResp, ref, messages)
	return Message{}
}
