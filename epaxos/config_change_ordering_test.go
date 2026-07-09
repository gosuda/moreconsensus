package epaxos

import (
	"errors"
	"testing"
)

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
		Command: confCmd,
	}
	if err := rn.Step(preAccept); err != nil {
		t.Fatalf("Step(%s config change) err=%v", preAccept.Type, err)
	}
	stored := rn.instances[confRef]
	if stored == nil || stored.rec.Status != StatusPreAccepted || stored.rec.Command.Kind != CommandConfChange {
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
		Command:      stored.rec.Command,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatalf("Step(%s config change) err=%v", commit.Type, err)
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

	confRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	confCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	if err := rn.Step(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     confRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: confCmd,
	}); err != nil {
		t.Fatalf("Step(%s config change) err=%v", MsgPreAccept, err)
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
	if err := rn.Step(userPreAccept); err != nil {
		t.Fatalf("Step(%s user command) err=%v", userPreAccept.Type, err)
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
	if err := rn.Step(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     executedRef,
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: executedCmd,
	}); err != nil {
		t.Fatalf("Step(%s first config change) err=%v", MsgPreAccept, err)
	}

	pendingRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	pendingCmd := confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 5})
	if err := rn.Step(Message{
		Type:    MsgPreAccept,
		From:    3,
		To:      2,
		Ref:     pendingRef,
		Ballot:  Ballot{Replica: 3},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: pendingCmd,
	}); err != nil {
		t.Fatalf("Step(%s second config change) err=%v", MsgPreAccept, err)
	}

	first := rn.instances[executedRef]
	if first == nil || first.rec.Command.Kind != CommandConfChange || first.rec.Status != StatusPreAccepted {
		t.Fatalf("first config-change record for %s = %#v, want pre-accepted config change", executedRef, first)
	}
	second := rn.instances[pendingRef]
	if second == nil || second.rec.Command.Kind != CommandConfChange || second.rec.Status != StatusPreAccepted {
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
		Command:      first.rec.Command,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatalf("Step(%s first config change) err=%v", commit.Type, err)
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
		Command:      second.rec.Command,
	}
	if err := rn.Step(commitSecond); err != nil {
		t.Fatalf("Step(%s second config change) err=%v", commitSecond.Type, err)
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
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
	if err != nil {
		t.Fatal(err)
	}

	confRef, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 2})
	if err != nil {
		t.Fatal(err)
	}
	if rn.pendingConf {
		t.Fatalf("single-voter config change %s executed immediately but left pending configuration barrier set", confRef)
	}
	if got := rn.instances[confRef].rec.Status; got != StatusExecuted {
		t.Fatalf("single-voter config change %s status=%s, want %s", confRef, got, StatusExecuted)
	}

	userRef, err := rn.Propose(configOrderingUserCommand(30, "after-single-conf"))
	if err != nil {
		t.Fatalf("Propose after immediately executed config change %s err=%v", confRef, err)
	}
	if userRef.Conf != confRef.Conf+1 {
		t.Fatalf("user proposal ref=%s, want configuration %d after %s", userRef, confRef.Conf+1, confRef)
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
		Command:          confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
		FastPathEligible: true,
	})
	store.Records[removeRef] = checkedRecord(InstanceRecord{
		Ref:              removeRef,
		Ballot:           Ballot{Replica: 2},
		Status:           StatusExecuted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0, 0},
		Command:          confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}),
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
	assertConfigOrderingDependencyRefs(t, restarted, oldRef, []InstanceRef{{Replica: 3, Instance: 1, Conf: 1}})
	assertConfigOrderingDependencyRefs(t, restarted, midRef, []InstanceRef{{Replica: 3, Instance: 1, Conf: 2}})

	if err := restarted.Step(Message{
		Type:    MsgPreAccept,
		From:    3,
		To:      1,
		Ref:     InstanceRef{Replica: 3, Instance: 3, Conf: 1},
		Ballot:  Ballot{Replica: 3},
		Seq:     3,
		Deps:    []InstanceNum{0, 0, 2},
		Command: configOrderingUserCommand(42, "old-conf-after-restart"),
	}); err != nil {
		t.Fatalf("Step accepted-domain %s after restart err=%v", MsgPreAccept, err)
	}
	if err := restarted.Step(Message{
		Type:    MsgPreAccept,
		From:    4,
		To:      1,
		Ref:     InstanceRef{Replica: 4, Instance: 2, Conf: 2},
		Ballot:  Ballot{Replica: 4},
		Seq:     3,
		Deps:    []InstanceNum{0, 0, 1, 1},
		Command: configOrderingUserCommand(43, "mid-conf-after-restart"),
	}); err != nil {
		t.Fatalf("Step accepted-domain %s after restart err=%v", MsgPreAccept, err)
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
	if _, err := removed.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 5}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("removed voter ProposeConfChange err=%v, want %v", err, ErrMessageRejected)
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
		Command:          confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
		FastPathEligible: true,
	})
	store.Records[removeRef] = checkedRecord(InstanceRecord{
		Ref:              removeRef,
		Ballot:           Ballot{Replica: 2},
		Status:           StatusExecuted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0, 0},
		Command:          confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}),
		FastPathEligible: true,
	})
	store.Records[pendingRef] = checkedRecord(InstanceRecord{
		Ref:              pendingRef,
		Ballot:           Ballot{Replica: 4},
		Status:           StatusPreAccepted,
		Seq:              2,
		Deps:             []InstanceNum{0, 0, 0},
		Command:          confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 5}),
		FastPathEligible: true,
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}})
	pending := restarted.instances[pendingRef]
	if pending == nil || pending.rec.Status != StatusPreAccepted || pending.rec.Command.Kind != CommandConfChange {
		t.Fatalf("restarted pending config-change record for %s = %#v, want unexecuted config change", pendingRef, pending)
	}
	if !restarted.pendingConf {
		t.Fatalf("restarted node did not restore pending configuration barrier for unexecuted config change %s", pendingRef)
	}
	if _, err := restarted.Propose(configOrderingUserCommand(46, "blocked-by-replayed-pending-conf")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose with replayed pending config change %s err=%v, want %v", pendingRef, err, ErrMessageRejected)
	}
	if _, err := restarted.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 6}); !errors.Is(err, ErrMessageRejected) {
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
			Command:          confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
			FastPathEligible: true,
		})
		store.Records[removeRef] = checkedRecord(InstanceRecord{
			Ref:              removeRef,
			Ballot:           Ballot{Replica: 2},
			Status:           StatusExecuted,
			Seq:              1,
			Deps:             []InstanceNum{0, 0, 0, 0},
			Command:          confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}),
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

func TestConfigReplayIgnoresMalformedConfChangeOnRestart(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusExecuted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeAddVoter), 4}},
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatalf("NewRawNode replaying malformed config change err=%v", err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}})
}

func TestInvalidConfChangeConfigReplayIgnoredOnRestart(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusExecuted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 2}),
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatalf("NewRawNode replaying invalid config change err=%v", err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}})
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

func assertConfigOrderingDependencyRefs(t *testing.T, rn *RawNode, ref InstanceRef, want []InstanceRef) {
	t.Helper()
	got := rn.dependencyRefs(ref)
	if len(got) != len(want) {
		t.Fatalf("dependencyRefs(%s)=%v, want %v", ref, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dependencyRefs(%s)=%v, want %v", ref, got, want)
		}
	}
}

func configOrderingUserCommand(sequence uint64, payload string) Command {
	return Command{
		ID:           CommandID{Client: 91, Sequence: sequence},
		Payload:      []byte(payload),
		ConflictKeys: [][]byte{[]byte("user-key")},
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
