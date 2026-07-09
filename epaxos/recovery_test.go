package epaxos

import (
	"bytes"
	"errors"
	"testing"
)

func TestDependencyClosureStartsPrefixRecoveryBeforeExecution(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}

	dependentRef := InstanceRef{Replica: 1, Instance: 3, Conf: 1}
	foreignPrefixRefs := []InstanceRef{
		{Replica: 2, Instance: 1, Conf: 1},
		{Replica: 2, Instance: 2, Conf: 1},
	}
	dependent := Command{ID: CommandID{Client: 10, Sequence: 1}, Payload: []byte("dependent"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{
		Type:         MsgCommit,
		From:         2,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Seq:          3,
		Deps:         []InstanceNum{0, 2, 0},
		Command:      dependent,
		RecordStatus: StatusCommitted,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatal(err)
	}
	recoveryRequireDependencyRefs(t, rn.dependencyRefs(dependentRef), foreignPrefixRefs)

	rd := rn.Ready()
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before dependency closure: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryRequirePrepareRefs(t, rd.Messages, foreignPrefixRefs[:1])
	recoveryRequireNoPrepareRefs(t, rd.Messages, []InstanceRef{
		{Replica: 1, Instance: 1, Conf: 1},
		{Replica: 1, Instance: 2, Conf: 1},
	})
}

func TestForeignMissingDependencyRecoveryNoopsAndReleasesDependent(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}

	missingRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	dependentRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	dependent := Command{ID: CommandID{Client: 20, Sequence: 1}, Payload: []byte("after-noop"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{
		Type:         MsgCommit,
		From:         3,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 3},
		RecordBallot: Ballot{Replica: 3},
		Seq:          2,
		Deps:         []InstanceNum{0, 1, 0},
		Command:      dependent,
		RecordStatus: StatusCommitted,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before missing dependency recovery: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryRequirePrepareRefs(t, rd.Messages, []InstanceRef{missingRef})
	recoveryApplyReady(t, store, rn, rd)

	ballot := Ballot{Number: 1, Replica: 1}
	resp := Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: missingRef, Ballot: ballot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0}}
	if err := rn.Step(resp); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	recoveryRequireRecord(t, rd.Records, missingRef, StatusAccepted, CommandNoop)
	recoveryRequireMessages(t, rd.Messages, MsgAccept, missingRef, CommandNoop)
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before noop accept quorum: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryApplyReady(t, store, rn, rd)

	acceptResp := Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: missingRef, Ballot: ballot, Seq: 1, Deps: []InstanceNum{0, 0, 0}, RecordStatus: StatusAccepted, Command: Command{Kind: CommandNoop}}
	if err := rn.Step(acceptResp); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	recoveryRequireRecord(t, rd.Records, missingRef, StatusExecuted, CommandNoop)
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 1 {
		t.Fatalf("dependent command release count=%d, want 1; committed=%#v", got, rd.Committed)
	}
	for _, c := range rd.Committed {
		if c.Ref == missingRef || c.Command.Kind == CommandNoop {
			t.Fatalf("noop recovery emitted application command: %#v", rd.Committed)
		}
	}
	recoveryApplyReady(t, store, rn, rd)

	if rn.HasReady() {
		rd = rn.Ready()
		if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
			t.Fatalf("dependent command emitted again after recovery: count=%d ready=%#v", got, rd)
		}
		recoveryApplyReady(t, store, rn, rd)
	}
}

func TestRestartResumesForeignRecoveryBallot(t *testing.T) {
	for _, tt := range []struct {
		name string
		st   Status
		cmd  Command
	}{
		{name: "none", st: StatusNone, cmd: Command{Kind: CommandNoop}},
		{name: "accepted", st: StatusAccepted, cmd: Command{Kind: CommandNoop}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			ref := InstanceRef{Replica: 2, Instance: 7, Conf: 1}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:     ref,
				Ballot:  Ballot{Number: 1, Replica: 1},
				Status:  tt.st,
				Seq:     1,
				Deps:    []InstanceNum{0, 0, 0},
				Command: tt.cmd,
			})
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
			if err != nil {
				t.Fatal(err)
			}
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before recovery deadline: %#v", restarted.Ready())
			}
			for tick := 1; tick < 5; tick++ {
				restarted.Tick()
				if restarted.HasReady() {
					t.Fatalf("recovery fired after %d ticks, before deadline", tick)
				}
			}
			restarted.Tick()
			rd := restarted.Ready()
			recoveryRequirePrepareRefs(t, rd.Messages, []InstanceRef{ref})
			for _, m := range rd.Messages {
				if m.Type != MsgPrepare || m.Ref != ref {
					continue
				}
				if m.From != 1 || (m.To != 2 && m.To != 3) {
					t.Fatalf("foreign recovery prepare = %#v, want from local node to peers", m)
				}
			}
		})
	}
}

func TestOldConfigRecoveryUsesPinnedVotersAfterRemoval(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}, Storage: store, RetryTicks: 2, RecoveryTicks: 4})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery fired after %d ticks, before deadline", tick)
		}
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargets(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4})
	recoveryApplyReady(t, store, restarted, rd)

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery reached slow quorum without removed voter 4: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted duplicate voter 3 prepare response as removed voter 4: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargets(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3, 4})
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	recoveryApplyReady(t, store, restarted, rd)

	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 4)
	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery committed without removed voter 4 accept response: %#v", restarted.Ready())
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted duplicate voter 3 accept response as removed voter 4: %#v", restarted.Ready())
	}
	if err := restarted.Step(acceptResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargets(t, rd.Messages, MsgCommit, ref, []ReplicaID{1, 3, 4})
	recoveryRequireRecord(t, rd.Records, ref, StatusExecuted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestOldConfigRecoveryUsesPinnedMidChainVotersAfterAddThenRemove(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{
		{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
		{ID: 3, Voters: []ReplicaID{1, 3, 4}},
	}
	ref := InstanceRef{Replica: 2, Instance: 7, Conf: 2}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 2},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: 2, RecoveryTicks: 4})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 3, 4}})
	assertConfState(t, restarted.confFor(2), ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before mid-chain old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("mid-chain old-config recovery fired after %d ticks, before deadline: %#v", tick, restarted.Ready())
		}
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{2, 3, 4}, 4)
	recoveryApplyReady(t, store, restarted, rd)

	requireNoopRecordWithDepsWidth := func(records []InstanceRecord, status Status) {
		t.Helper()
		for _, rec := range records {
			if rec.Ref != ref || rec.Status != status {
				continue
			}
			if rec.Command.Kind != CommandNoop {
				t.Fatalf("record %s/%s command kind = %v, want %v: %#v", ref, status, rec.Command.Kind, CommandNoop, rec)
			}
			if len(rec.Deps) != 4 {
				t.Fatalf("record %s/%s deps width = %d, want pinned Conf2 width 4: %#v", ref, status, len(rec.Deps), rec)
			}
			return
		}
		t.Fatalf("missing %s record for %s with noop command and pinned Conf2 deps: %#v", status, ref, records)
	}

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 1, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(4)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("mid-chain old-config recovery counted local 1 plus current voter 4 as Conf2 prepare quorum: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(2)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{2, 3, 4}, 4)
	requireNoopRecordWithDepsWidth(rd.Records, StatusAccepted)
	if len(rd.Committed) != 0 {
		t.Fatalf("mid-chain old-config recovery accept emitted application commands before accept quorum: %#v", rd.Committed)
	}
	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 4)
	recoveryApplyReady(t, store, restarted, rd)

	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(4)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("mid-chain old-config recovery counted local 1 plus current voter 4 as Conf2 accept quorum: %#v", restarted.Ready())
	}
	if err := restarted.Step(acceptResp(2)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgCommit, ref, []ReplicaID{2, 3, 4}, 4)
	requireNoopRecordWithDepsWidth(rd.Records, StatusExecuted)
	if len(rd.Committed) != 0 {
		t.Fatalf("mid-chain old-config noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestOldConfigChainRecoveryRetryCompletesAfterLostPreRetryResponses(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{
		{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
		{ID: 3, Voters: []ReplicaID{1, 3, 4}},
	}
	ref := InstanceRef{Replica: 2, Instance: 7, Conf: 2}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 2},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	const retryTicks = 2
	const recoveryTicks = 4
	restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: recoveryTicks})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 3, 4}})
	assertConfState(t, restarted.confFor(2), ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	recoveryRequireNoReady(t, restarted, "restart before mid-chain old-config recovery deadline")
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		recoveryRequireNoReady(t, restarted, "mid-chain old-config recovery before deadline")
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{2, 3, 4}, 4)
	recoveryApplyReady(t, store, restarted, rd)

	requireNoopRecordWithDepsWidth := func(records []InstanceRecord, status Status) {
		t.Helper()
		for _, rec := range records {
			if rec.Ref != ref || rec.Status != status {
				continue
			}
			if rec.Command.Kind != CommandNoop {
				t.Fatalf("record %s/%s command kind = %v, want %v: %#v", ref, status, rec.Command.Kind, CommandNoop, rec)
			}
			if len(rec.Deps) != 4 {
				t.Fatalf("record %s/%s deps width = %d, want pinned Conf2 width 4: %#v", ref, status, len(rec.Deps), rec)
			}
			return
		}
		t.Fatalf("missing %s record for %s with noop command and pinned Conf2 deps: %#v", status, ref, records)
	}

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 1, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(4)); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "mid-chain recovery counted current quorum prepare response as old Conf2 quorum")
	lostPreRetryPrepareResponse := prepareResp(2)
	if lostPreRetryPrepareResponse.Type != MsgPrepareResp || lostPreRetryPrepareResponse.From != 2 || lostPreRetryPrepareResponse.To != 1 {
		t.Fatalf("lost pre-retry prepare response fixture = %#v", lostPreRetryPrepareResponse)
	}

	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		recoveryRequireNoReady(t, restarted, "mid-chain recovery prepare retry before deadline")
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{2, 3, 4}, 4)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("mid-chain recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("mid-chain recovery prepare retry emitted durable/application effects: %#v", retry)
	}
	recoveryApplyReady(t, store, restarted, retry)

	if err := restarted.Step(prepareResp(2)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{2, 3, 4}, 4)
	requireNoopRecordWithDepsWidth(rd.Records, StatusAccepted)
	if len(rd.Committed) != 0 {
		t.Fatalf("mid-chain recovery accept emitted application commands before accept quorum: %#v", rd.Committed)
	}
	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 4)
	recoveryApplyReady(t, store, restarted, rd)

	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(4)); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "mid-chain recovery counted current quorum accept response as old Conf2 quorum")
	lostPreRetryAcceptResponse := acceptResp(2)
	if lostPreRetryAcceptResponse.Type != MsgAcceptResp || lostPreRetryAcceptResponse.From != 2 || lostPreRetryAcceptResponse.To != 1 {
		t.Fatalf("lost pre-retry accept response fixture = %#v", lostPreRetryAcceptResponse)
	}

	for tick := uint64(1); tick < retryTicks; tick++ {
		restarted.Tick()
		recoveryRequireNoReady(t, restarted, "mid-chain recovery accept retry before deadline")
	}
	restarted.Tick()
	retry = restarted.Ready()
	retryAcceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgAccept, ref, []ReplicaID{2, 3, 4}, 4)
	if retryAcceptBallot != acceptBallot {
		t.Fatalf("mid-chain recovery accept retry ballot = %#v, want original rebroadcast ballot %#v", retryAcceptBallot, acceptBallot)
	}
	acceptRetryMsg := optimizedRequireMessage(t, retry.Messages, MsgAccept, 4)
	if acceptRetryMsg.Seq != acceptMsg.Seq || !instanceNumsEqual(acceptRetryMsg.Deps, acceptMsg.Deps) {
		t.Fatalf("mid-chain recovery accept retry attrs = seq %d deps %v, want original seq %d deps %v", acceptRetryMsg.Seq, acceptRetryMsg.Deps, acceptMsg.Seq, acceptMsg.Deps)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("mid-chain recovery accept retry emitted durable/application effects: %#v", retry)
	}
	recoveryApplyReady(t, store, restarted, retry)

	if err := restarted.Step(acceptResp(2)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgCommit, ref, []ReplicaID{2, 3, 4}, 4)
	requireNoopRecordWithDepsWidth(rd.Records, StatusExecuted)
	if len(rd.Committed) != 0 {
		t.Fatalf("mid-chain noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestOldConfigRecoveryRetryUsesPinnedVotersAfterRemoval(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	const retryTicks = 2
	const recoveryTicks = 4
	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: recoveryTicks})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery fired after %d ticks, before deadline: %#v", tick, restarted.Ready())
		}
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryApplyReady(t, store, restarted, rd)
	if restarted.HasReady() {
		t.Fatalf("old-config recovery prepare left immediate work after Advance: %#v", restarted.Ready())
	}

	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery prepare retry fired after %d ticks, before deadline: %#v", tick, restarted.Ready())
		}
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("old-config recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("old-config recovery prepare retry emitted durable/application effects: %#v", retry)
	}
	recoveryApplyReady(t, store, restarted, retry)

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery reached accept before old prepare quorum: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config recovery accept emitted application commands before accept quorum: %#v", rd.Committed)
	}
	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 4)
	recoveryApplyReady(t, store, restarted, rd)
	if restarted.HasReady() {
		t.Fatalf("old-config recovery accept left immediate work after Advance: %#v", restarted.Ready())
	}

	for tick := uint64(1); tick < retryTicks; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery accept retry fired after %d ticks, before deadline: %#v", tick, restarted.Ready())
		}
	}
	restarted.Tick()
	retry = restarted.Ready()
	retryAcceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgAccept, ref, []ReplicaID{1, 3, 4}, 4)
	if retryAcceptBallot != acceptBallot {
		t.Fatalf("old-config recovery accept retry ballot = %#v, want original rebroadcast ballot %#v", retryAcceptBallot, acceptBallot)
	}
	acceptRetryMsg := optimizedRequireMessage(t, retry.Messages, MsgAccept, 4)
	if acceptRetryMsg.Seq != acceptMsg.Seq || !instanceNumsEqual(acceptRetryMsg.Deps, acceptMsg.Deps) {
		t.Fatalf("old-config recovery accept retry attrs = seq %d deps %v, want original seq %d deps %v", acceptRetryMsg.Seq, acceptRetryMsg.Deps, acceptMsg.Seq, acceptMsg.Deps)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("old-config recovery accept retry emitted durable/application effects: %#v", retry)
	}
}

func TestOldConfigRecoveryRetryCompletesAfterLostPreRetryAcceptResponse(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	const retryTicks = 2
	const recoveryTicks = 4
	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: recoveryTicks})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	recoveryRequireNoReady(t, restarted, "restart before old-config recovery deadline")
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		recoveryRequireNoReady(t, restarted, "old-config recovery before deadline")
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryApplyReady(t, store, restarted, rd)

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "old-config recovery counted one prepare response as quorum")
	if err := restarted.Step(prepareResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config recovery accept emitted application commands before accept quorum: %#v", rd.Committed)
	}
	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 4)
	recoveryApplyReady(t, store, restarted, rd)

	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "old-config recovery counted one accept response as quorum")
	lostPreRetryResponse := acceptResp(4)
	if lostPreRetryResponse.Type != MsgAcceptResp || lostPreRetryResponse.From != 4 || lostPreRetryResponse.To != 2 {
		t.Fatalf("lost pre-retry response fixture = %#v", lostPreRetryResponse)
	}

	for tick := uint64(1); tick < retryTicks; tick++ {
		restarted.Tick()
		recoveryRequireNoReady(t, restarted, "old-config recovery accept retry before deadline")
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryAcceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgAccept, ref, []ReplicaID{1, 3, 4}, 4)
	if retryAcceptBallot != acceptBallot {
		t.Fatalf("old-config recovery accept retry ballot = %#v, want original rebroadcast ballot %#v", retryAcceptBallot, acceptBallot)
	}
	acceptRetryMsg := optimizedRequireMessage(t, retry.Messages, MsgAccept, 4)
	if acceptRetryMsg.Seq != acceptMsg.Seq || !instanceNumsEqual(acceptRetryMsg.Deps, acceptMsg.Deps) {
		t.Fatalf("old-config recovery accept retry attrs = seq %d deps %v, want original seq %d deps %v", acceptRetryMsg.Seq, acceptRetryMsg.Deps, acceptMsg.Seq, acceptMsg.Deps)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("old-config recovery accept retry emitted durable/application effects: %#v", retry)
	}
	recoveryApplyReady(t, store, restarted, retry)

	if err := restarted.Step(acceptResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgCommit, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryRequireRecord(t, rd.Records, ref, StatusExecuted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestOldConfigRecoveryRetryCompletesAfterLostPreRetryPrepareResponse(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	const retryTicks = 2
	const recoveryTicks = 4
	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: recoveryTicks})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	recoveryRequireNoReady(t, restarted, "restart before old-config recovery deadline")
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		recoveryRequireNoReady(t, restarted, "old-config recovery before deadline")
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryApplyReady(t, store, restarted, rd)

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "old-config recovery counted one prepare response as quorum")
	lostPreRetryResponse := prepareResp(4)
	if lostPreRetryResponse.Type != MsgPrepareResp || lostPreRetryResponse.From != 4 || lostPreRetryResponse.To != 2 {
		t.Fatalf("lost pre-retry response fixture = %#v", lostPreRetryResponse)
	}

	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		recoveryRequireNoReady(t, restarted, "old-config recovery prepare retry before deadline")
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("old-config recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("old-config recovery prepare retry emitted durable/application effects: %#v", retry)
	}
	recoveryApplyReady(t, store, restarted, retry)

	if err := restarted.Step(prepareResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config recovery accept emitted application commands before accept quorum: %#v", rd.Committed)
	}
	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 4)
	recoveryApplyReady(t, store, restarted, rd)

	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "old-config recovery counted one accept response as quorum")
	if err := restarted.Step(acceptResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgCommit, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryRequireRecord(t, rd.Records, ref, StatusExecuted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestOldConfigRecoveryRetryUsesPinnedVotersAfterAddition(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	const retryTicks = 2
	const recoveryTicks = 4
	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: recoveryTicks})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery fired after %d ticks, before deadline: %#v", tick, restarted.Ready())
		}
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3}, 3)
	recoveryApplyReady(t, store, restarted, rd)
	if restarted.HasReady() {
		t.Fatalf("old-config recovery prepare left immediate work after Advance: %#v", restarted.Ready())
	}

	for tick := uint64(1); tick < recoveryTicks; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery prepare retry fired after %d ticks, before deadline: %#v", tick, restarted.Ready())
		}
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{1, 3}, 3)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("old-config recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("old-config recovery prepare retry emitted durable/application effects: %#v", retry)
	}
	recoveryApplyReady(t, store, restarted, retry)
	if restarted.HasReady() {
		t.Fatalf("old-config recovery prepare retry left immediate work after Advance: %#v", restarted.Ready())
	}

	prepareResp := Message{Type: MsgPrepareResp, From: 3, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0}}
	if err := restarted.Step(prepareResp); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3}, 3)
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config recovery accept emitted application commands before accept quorum: %#v", rd.Committed)
	}
	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	recoveryApplyReady(t, store, restarted, rd)
	if restarted.HasReady() {
		t.Fatalf("old-config recovery accept left immediate work after Advance: %#v", restarted.Ready())
	}

	for tick := uint64(1); tick < retryTicks; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery accept retry fired after %d ticks, before deadline: %#v", tick, restarted.Ready())
		}
	}
	restarted.Tick()
	retry = restarted.Ready()
	retryAcceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgAccept, ref, []ReplicaID{1, 3}, 3)
	if retryAcceptBallot != acceptBallot {
		t.Fatalf("old-config recovery accept retry ballot = %#v, want original rebroadcast ballot %#v", retryAcceptBallot, acceptBallot)
	}
	acceptRetryMsg := optimizedRequireMessage(t, retry.Messages, MsgAccept, 3)
	if acceptRetryMsg.Seq != acceptMsg.Seq || !instanceNumsEqual(acceptRetryMsg.Deps, acceptMsg.Deps) {
		t.Fatalf("old-config recovery accept retry attrs = seq %d deps %v, want original seq %d deps %v", acceptRetryMsg.Seq, acceptRetryMsg.Deps, acceptMsg.Seq, acceptMsg.Deps)
	}
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("old-config recovery accept retry emitted durable/application effects: %#v", retry)
	}
}

func TestOldConfigRecoveryUsesPinnedVotersAfterAddition(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: 2, RecoveryTicks: 4})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery fired after %d ticks, before deadline", tick)
		}
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3}, 3)
	recoveryApplyReady(t, store, restarted, rd)

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted newly added voter 4 prepare response: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3}, 3)
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	recoveryApplyReady(t, store, restarted, rd)

	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted newly added voter 4 accept response: %#v", restarted.Ready())
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgCommit, ref, []ReplicaID{1, 3}, 3)
	recoveryRequireRecord(t, rd.Records, ref, StatusExecuted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestOldConfigTransitionRetryUsesPinnedVotersAfterRemoval(t *testing.T) {
	for _, tt := range []struct {
		name       string
		status     Status
		ballot     Ballot
		recordBall Ballot
		msgType    MessageType
		seq        uint64
		deps       []InstanceNum
		acceptSeq  uint64
		acceptDeps []InstanceNum
	}{
		{
			name:       "preaccepted",
			status:     StatusPreAccepted,
			ballot:     Ballot{Replica: 1},
			recordBall: Ballot{Replica: 1},
			msgType:    MsgPreAccept,
			seq:        5,
			deps:       []InstanceNum{0, 2, 3, 4},
		},
		{
			name:       "accepted",
			status:     StatusAccepted,
			ballot:     Ballot{Replica: 1},
			recordBall: Ballot{Replica: 1},
			msgType:    MsgAccept,
			seq:        6,
			deps:       []InstanceNum{0, 4, 3, 2},
			acceptSeq:  7,
			acceptDeps: []InstanceNum{0, 5, 6, 7},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}}
			ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
			cmd := Command{ID: CommandID{Client: 91, Sequence: 1}, Payload: []byte("old-config-transition-removal-" + tt.name), ConflictKeys: [][]byte{[]byte("old-config-transition-removal-key")}}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:              ref,
				Ballot:           tt.ballot,
				RecordBallot:     tt.recordBall,
				Status:           tt.status,
				Seq:              tt.seq,
				Deps:             append([]InstanceNum(nil), tt.deps...),
				AcceptSeq:        tt.acceptSeq,
				AcceptDeps:       append([]InstanceNum(nil), tt.acceptDeps...),
				Command:          cmd,
				FastPathEligible: tt.status == StatusPreAccepted,
			})

			const retryTicks = 2
			restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3, 4}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: 10})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before old-config transition retry deadline: %#v", restarted.Ready())
			}
			for tick := uint64(1); tick < retryTicks; tick++ {
				restarted.Tick()
				if restarted.HasReady() {
					t.Fatalf("old-config transition %s retry fired after %d ticks, before deadline: %#v", tt.name, tick, restarted.Ready())
				}
			}

			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.msgType, ref, []ReplicaID{2, 3, 4}, cmd, tt.deps)
			if retry.Ballot != tt.ballot || retry.RecordStatus != tt.status {
				t.Fatalf("old-config transition %s retry tuple = ballot %v status %s, want ballot %v status %s: %#v", tt.name, retry.Ballot, retry.RecordStatus, tt.ballot, tt.status, retry)
			}
			if tt.status == StatusAccepted && retry.RecordBallot != tt.recordBall {
				t.Fatalf("old-config transition accepted retry record ballot = %v, want %v: %#v", retry.RecordBallot, tt.recordBall, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition %s retry seq = %d, want %d: %#v", tt.name, retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition accepted retry accept attrs = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			if len(rd.Records) != 0 || len(rd.Committed) != 0 || rd.MustSync {
				t.Fatalf("old-config transition %s retry emitted durable/application effects: %#v", tt.name, rd)
			}
		})
	}
}

func TestOldConfigTransitionRetryUsesPinnedVotersAfterAddition(t *testing.T) {
	for _, tt := range []struct {
		name       string
		status     Status
		ballot     Ballot
		recordBall Ballot
		msgType    MessageType
		seq        uint64
		deps       []InstanceNum
		acceptSeq  uint64
		acceptDeps []InstanceNum
	}{
		{
			name:       "preaccepted",
			status:     StatusPreAccepted,
			ballot:     Ballot{Replica: 1},
			recordBall: Ballot{Replica: 1},
			msgType:    MsgPreAccept,
			seq:        5,
			deps:       []InstanceNum{0, 2, 3},
		},
		{
			name:       "accepted",
			status:     StatusAccepted,
			ballot:     Ballot{Replica: 1},
			recordBall: Ballot{Replica: 1},
			msgType:    MsgAccept,
			seq:        6,
			deps:       []InstanceNum{0, 4, 3},
			acceptSeq:  7,
			acceptDeps: []InstanceNum{0, 5, 6},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}}
			ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
			cmd := Command{ID: CommandID{Client: 92, Sequence: 1}, Payload: []byte("old-config-transition-addition-" + tt.name), ConflictKeys: [][]byte{[]byte("old-config-transition-addition-key")}}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:              ref,
				Ballot:           tt.ballot,
				RecordBallot:     tt.recordBall,
				Status:           tt.status,
				Seq:              tt.seq,
				Deps:             append([]InstanceNum(nil), tt.deps...),
				AcceptSeq:        tt.acceptSeq,
				AcceptDeps:       append([]InstanceNum(nil), tt.acceptDeps...),
				Command:          cmd,
				FastPathEligible: tt.status == StatusPreAccepted,
			})

			const retryTicks = 2
			restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: 10})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before old-config transition retry deadline: %#v", restarted.Ready())
			}
			for tick := uint64(1); tick < retryTicks; tick++ {
				restarted.Tick()
				if restarted.HasReady() {
					t.Fatalf("old-config transition %s retry fired after %d ticks, before deadline: %#v", tt.name, tick, restarted.Ready())
				}
			}

			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.msgType, ref, []ReplicaID{2, 3}, cmd, tt.deps)
			if retry.Ballot != tt.ballot || retry.RecordStatus != tt.status {
				t.Fatalf("old-config transition %s retry tuple = ballot %v status %s, want ballot %v status %s: %#v", tt.name, retry.Ballot, retry.RecordStatus, tt.ballot, tt.status, retry)
			}
			if tt.status == StatusAccepted && retry.RecordBallot != tt.recordBall {
				t.Fatalf("old-config transition accepted retry record ballot = %v, want %v: %#v", retry.RecordBallot, tt.recordBall, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition %s retry seq = %d, want %d: %#v", tt.name, retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition accepted retry accept attrs = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			if len(rd.Records) != 0 || len(rd.Committed) != 0 || rd.MustSync {
				t.Fatalf("old-config transition %s retry emitted durable/application effects: %#v", tt.name, rd)
			}
		})
	}
}

func TestOldConfigTransitionRetryCompletesAfterLostPreRetryResponses(t *testing.T) {
	for _, tt := range []struct {
		name             string
		status           Status
		retryType        MessageType
		responseType     MessageType
		storeConfigs     []ConfState
		currentVoters    []ReplicaID
		wantCurrentConf  ConfState
		ref              InstanceRef
		seq              uint64
		deps             []InstanceNum
		acceptSeq        uint64
		acceptDeps       []InstanceNum
		preRetryFrom     ReplicaID
		preRetryRejected bool
		lostFrom         ReplicaID
		retryTargets     []ReplicaID
	}{
		{
			name:            "removal/preaccepted",
			status:          StatusPreAccepted,
			retryType:       MsgPreAccept,
			responseType:    MsgPreAcceptResp,
			storeConfigs:    []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}},
			currentVoters:   []ReplicaID{1, 2, 3, 4},
			wantCurrentConf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}},
			ref:             InstanceRef{Replica: 1, Instance: 9, Conf: 1},
			seq:             5,
			deps:            []InstanceNum{0, 2, 3, 4},
			preRetryFrom:    2,
			lostFrom:        4,
			retryTargets:    []ReplicaID{2, 3, 4},
		},
		{
			name:            "removal/accepted",
			status:          StatusAccepted,
			retryType:       MsgAccept,
			responseType:    MsgAcceptResp,
			storeConfigs:    []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}},
			currentVoters:   []ReplicaID{1, 2, 3, 4},
			wantCurrentConf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}},
			ref:             InstanceRef{Replica: 1, Instance: 9, Conf: 1},
			seq:             6,
			deps:            []InstanceNum{0, 0, 0, 0},
			acceptSeq:       7,
			acceptDeps:      []InstanceNum{0, 5, 6, 7},
			preRetryFrom:    2,
			lostFrom:        4,
			retryTargets:    []ReplicaID{2, 3, 4},
		},
		{
			name:             "addition/preaccepted",
			status:           StatusPreAccepted,
			retryType:        MsgPreAccept,
			responseType:     MsgPreAcceptResp,
			storeConfigs:     []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}},
			currentVoters:    []ReplicaID{1, 2, 3},
			wantCurrentConf:  ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
			ref:              InstanceRef{Replica: 1, Instance: 10, Conf: 1},
			seq:              5,
			deps:             []InstanceNum{0, 2, 3},
			preRetryFrom:     4,
			preRetryRejected: true,
			lostFrom:         2,
			retryTargets:     []ReplicaID{2, 3},
		},
		{
			name:             "addition/accepted",
			status:           StatusAccepted,
			retryType:        MsgAccept,
			responseType:     MsgAcceptResp,
			storeConfigs:     []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}},
			currentVoters:    []ReplicaID{1, 2, 3},
			wantCurrentConf:  ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
			ref:              InstanceRef{Replica: 1, Instance: 10, Conf: 1},
			seq:              6,
			deps:             []InstanceNum{0, 0, 0},
			acceptSeq:        7,
			acceptDeps:       []InstanceNum{0, 5, 6},
			preRetryFrom:     4,
			preRetryRejected: true,
			lostFrom:         2,
			retryTargets:     []ReplicaID{2, 3},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Configs = append([]ConfState(nil), tt.storeConfigs...)
			ballot := Ballot{Replica: 1}
			cmd := Command{ID: CommandID{Client: 97, Sequence: 1}, Payload: []byte("old-config-transition-lost-pre-retry-" + tt.name), ConflictKeys: [][]byte{[]byte("old-config-transition-lost-pre-retry-key")}}
			store.Records[tt.ref] = checkedRecord(InstanceRecord{
				Ref:              tt.ref,
				Ballot:           ballot,
				RecordBallot:     ballot,
				Status:           tt.status,
				Seq:              tt.seq,
				Deps:             append([]InstanceNum(nil), tt.deps...),
				AcceptSeq:        tt.acceptSeq,
				AcceptDeps:       append([]InstanceNum(nil), tt.acceptDeps...),
				Command:          cmd,
				FastPathEligible: tt.status == StatusPreAccepted,
			})

			response := func(from ReplicaID, ballot Ballot) Message {
				switch tt.status {
				case StatusPreAccepted:
					return Message{Type: tt.responseType, From: from, To: 1, Ref: tt.ref, Ballot: ballot, Seq: tt.seq, Deps: append([]InstanceNum(nil), tt.deps...), RecordStatus: StatusPreAccepted, FastPathEligible: true}
				case StatusAccepted:
					return Message{Type: tt.responseType, From: from, To: 1, Ref: tt.ref, Ballot: ballot, RecordBallot: ballot, Seq: tt.seq, Deps: append([]InstanceNum(nil), tt.deps...), AcceptSeq: tt.acceptSeq, AcceptDeps: append([]InstanceNum(nil), tt.acceptDeps...), RecordStatus: StatusAccepted}
				default:
					t.Fatalf("unhandled status %s", tt.status)
					return Message{}
				}
			}

			const retryTicks = 2
			restarted, err := NewRawNode(Config{ID: 1, Voters: tt.currentVoters, Storage: store, RetryTicks: retryTicks, RecoveryTicks: 10})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, tt.wantCurrentConf)
			recoveryRequireNoReady(t, restarted, "restart before old-config transition lost-response retry deadline")

			if err := restarted.Step(response(tt.preRetryFrom, ballot)); err != nil && (!tt.preRetryRejected || !errors.Is(err, ErrMessageRejected)) {
				t.Fatal(err)
			}
			recoveryRequireNoReady(t, restarted, "old-config transition counted pre-retry response under current config")
			lostPreRetryResponse := response(tt.lostFrom, ballot)
			if lostPreRetryResponse.Type != tt.responseType || lostPreRetryResponse.From != tt.lostFrom || lostPreRetryResponse.To != 1 {
				t.Fatalf("lost pre-retry response fixture = %#v", lostPreRetryResponse)
			}

			for tick := uint64(1); tick < retryTicks; tick++ {
				restarted.Tick()
				recoveryRequireNoReady(t, restarted, "old-config transition retry before deadline after lost response")
			}
			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.retryType, tt.ref, tt.retryTargets, cmd, tt.deps)
			if retry.Ballot != ballot || retry.RecordStatus != tt.status {
				t.Fatalf("old-config transition retry tuple = ballot %v status %s, want ballot %v status %s: %#v", retry.Ballot, retry.RecordStatus, ballot, tt.status, retry)
			}
			if tt.status == StatusAccepted && (retry.RecordBallot != ballot || retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition accepted retry attrs = record ballot %v accept seq %d deps %v, want record ballot %v accept seq %d deps %v: %#v", retry.RecordBallot, retry.AcceptSeq, retry.AcceptDeps, ballot, tt.acceptSeq, tt.acceptDeps, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition retry seq = %d, want %d: %#v", retry.Seq, tt.seq, retry)
			}
			if len(rd.Records) != 0 || len(rd.Committed) != 0 || rd.MustSync {
				t.Fatalf("old-config transition retry emitted durable/application effects: %#v", rd)
			}
			recoveryApplyReady(t, store, restarted, rd)

			if err := restarted.Step(response(tt.lostFrom, retry.Ballot)); err != nil {
				t.Fatal(err)
			}
			rd = restarted.Ready()
			switch tt.status {
			case StatusPreAccepted:
				accept := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgAccept, tt.ref, tt.retryTargets, cmd, tt.deps)
				if accept.Seq != tt.seq {
					t.Fatalf("old-config transition preaccepted accept seq = %d, want %d: %#v", accept.Seq, tt.seq, accept)
				}
				recoveryRequireRecordWithCommandAndDeps(t, rd.Records, tt.ref, StatusAccepted, cmd, tt.seq, tt.deps)
				if len(rd.Committed) != 0 {
					t.Fatalf("old-config transition preaccepted emitted application commands before accept quorum: %#v", rd.Committed)
				}
			case StatusAccepted:
				commit := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgCommit, tt.ref, tt.retryTargets, cmd, tt.deps)
				if commit.Seq != tt.seq || commit.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(commit.AcceptDeps, tt.acceptDeps) {
					t.Fatalf("old-config transition accepted commit attrs = seq %d accept seq %d deps %v/%v, want seq %d accept seq %d deps %v/%v: %#v", commit.Seq, commit.AcceptSeq, commit.Deps, commit.AcceptDeps, tt.seq, tt.acceptSeq, tt.deps, tt.acceptDeps, commit)
				}
				recoveryRequireRecordWithCommandAndDeps(t, rd.Records, tt.ref, StatusCommitted, cmd, tt.seq, tt.deps)
				recoveryRequireCommittedCommand(t, rd.Committed, tt.ref, cmd, tt.seq, tt.deps)
			default:
				t.Fatalf("unhandled status %s", tt.status)
			}
			recoveryApplyReady(t, store, restarted, rd)
		})
	}
}

func TestOldConfigTransitionDedupUsesPinnedVotersAfterRemoval(t *testing.T) {
	for _, tt := range []struct {
		name       string
		status     Status
		retryType  MessageType
		seq        uint64
		deps       []InstanceNum
		acceptSeq  uint64
		acceptDeps []InstanceNum
	}{
		{
			name:      "preaccepted",
			status:    StatusPreAccepted,
			retryType: MsgPreAccept,
			seq:       5,
			deps:      []InstanceNum{0, 2, 3, 4},
		},
		{
			name:       "accepted",
			status:     StatusAccepted,
			retryType:  MsgAccept,
			seq:        6,
			deps:       []InstanceNum{0, 0, 0, 0},
			acceptSeq:  7,
			acceptDeps: []InstanceNum{0, 0, 0, 0},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}}
			ref := InstanceRef{Replica: 1, Instance: 8, Conf: 1}
			cmd := Command{ID: CommandID{Client: 93, Sequence: 1}, Payload: []byte("old-config-transition-dedup-removal-" + tt.name), ConflictKeys: [][]byte{[]byte("old-config-transition-dedup-removal-key")}}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:              ref,
				Ballot:           Ballot{Replica: 1},
				RecordBallot:     Ballot{Replica: 1},
				Status:           tt.status,
				Seq:              tt.seq,
				Deps:             append([]InstanceNum(nil), tt.deps...),
				AcceptSeq:        tt.acceptSeq,
				AcceptDeps:       append([]InstanceNum(nil), tt.acceptDeps...),
				Command:          cmd,
				FastPathEligible: tt.status == StatusPreAccepted,
			})

			const retryTicks = 2
			restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3, 4}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: 10})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
			recoveryRequireNoReady(t, restarted, "restart before old-config transition removal retry deadline")
			for tick := uint64(1); tick < retryTicks; tick++ {
				restarted.Tick()
				recoveryRequireNoReady(t, restarted, "old-config transition removal "+tt.name+" retry before deadline")
			}

			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.retryType, ref, []ReplicaID{2, 3, 4}, cmd, tt.deps)
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition removal %s retry seq = %d, want %d: %#v", tt.name, retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition removal accepted retry accept attrs = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			if len(rd.Records) != 0 || len(rd.Committed) != 0 || rd.MustSync {
				t.Fatalf("old-config transition removal %s retry emitted durable/application effects: %#v", tt.name, rd)
			}
			recoveryApplyReady(t, store, restarted, rd)

			switch tt.status {
			case StatusPreAccepted:
				preAcceptResp := func(from ReplicaID) Message {
					return Message{Type: MsgPreAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), RecordStatus: StatusPreAccepted, FastPathEligible: true}
				}
				if err := restarted.Step(preAcceptResp(2)); err != nil {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition removal preaccepted counted one remote old voter as quorum")
				if err := restarted.Step(preAcceptResp(2)); err != nil {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition removal preaccepted counted duplicate voter 2 as voter 3")
				if err := restarted.Step(preAcceptResp(3)); err != nil {
					t.Fatal(err)
				}
				rd = restarted.Ready()
				accept := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgAccept, ref, []ReplicaID{2, 3, 4}, cmd, tt.deps)
				if accept.Seq != tt.seq {
					t.Fatalf("old-config transition removal preaccepted accept seq = %d, want %d: %#v", accept.Seq, tt.seq, accept)
				}
				recoveryRequireRecordWithCommandAndDeps(t, rd.Records, ref, StatusAccepted, cmd, tt.seq, tt.deps)
				if len(rd.Committed) != 0 {
					t.Fatalf("old-config transition removal preaccepted emitted application commands before accept quorum: %#v", rd.Committed)
				}
			case StatusAccepted:
				acceptResp := func(from ReplicaID) Message {
					return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, RecordBallot: retry.RecordBallot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), AcceptSeq: retry.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), retry.AcceptDeps...), RecordStatus: StatusAccepted}
				}
				if err := restarted.Step(acceptResp(2)); err != nil {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition removal accepted counted one remote old voter as quorum")
				if err := restarted.Step(acceptResp(2)); err != nil {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition removal accepted counted duplicate voter 2 as voter 3")
				if err := restarted.Step(acceptResp(3)); err != nil {
					t.Fatal(err)
				}
				rd = restarted.Ready()
				commit := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgCommit, ref, []ReplicaID{2, 3, 4}, cmd, tt.deps)
				if commit.Seq != tt.seq || commit.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(commit.AcceptDeps, tt.acceptDeps) {
					t.Fatalf("old-config transition removal accepted commit attrs = seq %d accept seq %d deps %v/%v, want seq %d accept seq %d deps %v/%v: %#v", commit.Seq, commit.AcceptSeq, commit.Deps, commit.AcceptDeps, tt.seq, tt.acceptSeq, tt.deps, tt.acceptDeps, commit)
				}
				recoveryRequireRecordWithCommandAndDeps(t, rd.Records, ref, StatusCommitted, cmd, tt.seq, tt.deps)
				recoveryRequireCommittedCommand(t, rd.Committed, ref, cmd, tt.seq, tt.deps)
			default:
				t.Fatalf("unhandled status %s", tt.status)
			}
		})
	}
}

func TestOldConfigTransitionDedupUsesPinnedVotersAfterAddition(t *testing.T) {
	for _, tt := range []struct {
		name       string
		status     Status
		retryType  MessageType
		seq        uint64
		deps       []InstanceNum
		acceptSeq  uint64
		acceptDeps []InstanceNum
	}{
		{
			name:      "preaccepted",
			status:    StatusPreAccepted,
			retryType: MsgPreAccept,
			seq:       5,
			deps:      []InstanceNum{0, 2, 3},
		},
		{
			name:       "accepted",
			status:     StatusAccepted,
			retryType:  MsgAccept,
			seq:        6,
			deps:       []InstanceNum{0, 0, 0},
			acceptSeq:  7,
			acceptDeps: []InstanceNum{0, 0, 0},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}}
			ref := InstanceRef{Replica: 1, Instance: 8, Conf: 1}
			cmd := Command{ID: CommandID{Client: 94, Sequence: 1}, Payload: []byte("old-config-transition-dedup-addition-" + tt.name), ConflictKeys: [][]byte{[]byte("old-config-transition-dedup-addition-key")}}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:              ref,
				Ballot:           Ballot{Replica: 1},
				RecordBallot:     Ballot{Replica: 1},
				Status:           tt.status,
				Seq:              tt.seq,
				Deps:             append([]InstanceNum(nil), tt.deps...),
				AcceptSeq:        tt.acceptSeq,
				AcceptDeps:       append([]InstanceNum(nil), tt.acceptDeps...),
				Command:          cmd,
				FastPathEligible: tt.status == StatusPreAccepted,
			})

			const retryTicks = 2
			restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: 10})
			if err != nil {
				t.Fatal(err)
			}
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
			recoveryRequireNoReady(t, restarted, "restart before old-config transition addition retry deadline")
			for tick := uint64(1); tick < retryTicks; tick++ {
				restarted.Tick()
				recoveryRequireNoReady(t, restarted, "old-config transition addition "+tt.name+" retry before deadline")
			}

			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.retryType, ref, []ReplicaID{2, 3}, cmd, tt.deps)
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition addition %s retry seq = %d, want %d: %#v", tt.name, retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition addition accepted retry accept attrs = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			if len(rd.Records) != 0 || len(rd.Committed) != 0 || rd.MustSync {
				t.Fatalf("old-config transition addition %s retry emitted durable/application effects: %#v", tt.name, rd)
			}
			recoveryApplyReady(t, store, restarted, rd)

			switch tt.status {
			case StatusPreAccepted:
				preAcceptResp := func(from ReplicaID) Message {
					return Message{Type: MsgPreAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), RecordStatus: StatusPreAccepted, FastPathEligible: true}
				}
				if err := restarted.Step(preAcceptResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition addition preaccepted counted added current voter 4")
				if err := restarted.Step(preAcceptResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition addition preaccepted counted duplicate added current voter 4")
				if err := restarted.Step(preAcceptResp(2)); err != nil {
					t.Fatal(err)
				}
				rd = restarted.Ready()
				accept := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgAccept, ref, []ReplicaID{2, 3}, cmd, tt.deps)
				if accept.Seq != tt.seq {
					t.Fatalf("old-config transition addition preaccepted accept seq = %d, want %d: %#v", accept.Seq, tt.seq, accept)
				}
				recoveryRequireRecordWithCommandAndDeps(t, rd.Records, ref, StatusAccepted, cmd, tt.seq, tt.deps)
				if len(rd.Committed) != 0 {
					t.Fatalf("old-config transition addition preaccepted emitted application commands before accept quorum: %#v", rd.Committed)
				}
			case StatusAccepted:
				acceptResp := func(from ReplicaID) Message {
					return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, RecordBallot: retry.RecordBallot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), AcceptSeq: retry.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), retry.AcceptDeps...), RecordStatus: StatusAccepted}
				}
				if err := restarted.Step(acceptResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition addition accepted counted added current voter 4")
				if err := restarted.Step(acceptResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
					t.Fatal(err)
				}
				recoveryRequireNoReady(t, restarted, "old-config transition addition accepted counted duplicate added current voter 4")
				if err := restarted.Step(acceptResp(2)); err != nil {
					t.Fatal(err)
				}
				rd = restarted.Ready()
				commit := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgCommit, ref, []ReplicaID{2, 3}, cmd, tt.deps)
				if commit.Seq != tt.seq || commit.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(commit.AcceptDeps, tt.acceptDeps) {
					t.Fatalf("old-config transition addition accepted commit attrs = seq %d accept seq %d deps %v/%v, want seq %d accept seq %d deps %v/%v: %#v", commit.Seq, commit.AcceptSeq, commit.Deps, commit.AcceptDeps, tt.seq, tt.acceptSeq, tt.deps, tt.acceptDeps, commit)
				}
				recoveryRequireRecordWithCommandAndDeps(t, rd.Records, ref, StatusCommitted, cmd, tt.seq, tt.deps)
				recoveryRequireCommittedCommand(t, rd.Committed, ref, cmd, tt.seq, tt.deps)
			default:
				t.Fatalf("unhandled status %s", tt.status)
			}
		})
	}
}

func TestRestartLocalVoteSeedGuardSkipsNonOwnerPreAcceptedBallot(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: 9, Conf: 1}
	ballot := Ballot{Number: 2, Replica: 2}
	seq := uint64(5)
	deps := []InstanceNum{0, 0, 0}
	cmd := Command{ID: CommandID{Client: 95, Sequence: 1}, Payload: []byte("restart-local-vote-preaccepted"), ConflictKeys: [][]byte{[]byte("restart-local-vote-key")}}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:              ref,
		Ballot:           ballot,
		RecordBallot:     ballot,
		Status:           StatusPreAccepted,
		Seq:              seq,
		Deps:             append([]InstanceNum(nil), deps...),
		Command:          cmd,
		FastPathEligible: true,
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: 2, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "restart seeded local preaccept vote for non-owner recovery ballot")

	err = restarted.Step(Message{
		Type:             MsgPreAcceptResp,
		From:             2,
		To:               1,
		Ref:              ref,
		Ballot:           ballot,
		Seq:              seq,
		Deps:             append([]InstanceNum(nil), deps...),
		RecordStatus:     StatusPreAccepted,
		FastPathEligible: true,
	})
	if err != nil {
		if !errors.Is(err, ErrMessageRejected) {
			t.Fatal(err)
		}
		recoveryRequireNoReady(t, restarted, "rejected non-owner preaccept response advanced local record")
		return
	}
	recoveryRequireNoReady(t, restarted, "non-owner preaccept response counted seeded owner vote as quorum")
}

func TestRestartLocalVoteSeedGuardSkipsNonOwnerAcceptedBallot(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: 10, Conf: 1}
	ballot := Ballot{Number: 2, Replica: 2}
	seq := uint64(6)
	deps := []InstanceNum{0, 0, 0}
	acceptSeq := uint64(7)
	acceptDeps := []InstanceNum{0, 0, 0}
	cmd := Command{ID: CommandID{Client: 96, Sequence: 1}, Payload: []byte("restart-local-vote-accepted"), ConflictKeys: [][]byte{[]byte("restart-local-vote-key")}}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:          ref,
		Ballot:       ballot,
		RecordBallot: ballot,
		Status:       StatusAccepted,
		Seq:          seq,
		Deps:         append([]InstanceNum(nil), deps...),
		AcceptSeq:    acceptSeq,
		AcceptDeps:   append([]InstanceNum(nil), acceptDeps...),
		Command:      cmd,
	})

	restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: 2, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "restart seeded local accept vote for non-owner recovery ballot")

	err = restarted.Step(Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       ballot,
		RecordBallot: ballot,
		Seq:          seq,
		Deps:         append([]InstanceNum(nil), deps...),
		AcceptSeq:    acceptSeq,
		AcceptDeps:   append([]InstanceNum(nil), acceptDeps...),
		RecordStatus: StatusAccepted,
	})
	if err != nil {
		if !errors.Is(err, ErrMessageRejected) {
			t.Fatal(err)
		}
		recoveryRequireNoReady(t, restarted, "rejected non-owner accept response committed local record")
		return
	}
	recoveryRequireNoReady(t, restarted, "non-owner accept response counted seeded owner vote as quorum")
}

func TestSimulatorNonOwnerRecoversPreAcceptedDependency(t *testing.T) {
	s := newSimCluster(t, 3, false)
	first := Command{ID: CommandID{Client: 30, Sequence: 1}, Payload: []byte("owner-command"), ConflictKeys: [][]byte{[]byte("shared")}}
	firstRef, err := s.nodes[1].Propose(first)
	if err != nil {
		t.Fatal(err)
	}
	ownerReady := s.nodes[1].Ready()
	if err := s.stores[1].ApplyReady(ownerReady); err != nil {
		t.Fatal(err)
	}
	for _, m := range ownerReady.Messages {
		if m.Type == MsgPreAccept && !s.deliver(m) {
			t.Fatalf("preaccept to follower was unexpectedly delayed: %#v", m)
		}
	}
	advanceOK(t, s.nodes[1], ownerReady)

	s.pause(1)
	s.drain()

	second := Command{ID: CommandID{Client: 31, Sequence: 1}, Payload: []byte("follower-command"), ConflictKeys: [][]byte{[]byte("shared")}}
	secondRef, err := s.nodes[2].Propose(second)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickOnly(2, 6)

	for _, id := range []ReplicaID{2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, firstRef, first.Payload, 1)
		requireCommittedPayloadCount(t, s.apps[id], id, secondRef, second.Payload, 1)
	}
}

func TestSimulatorIsolatedLocalTimeoutRecoversAfterConflictingQuorumCommit(t *testing.T) {
	s := newSimCluster(t, 3, true)
	key := []byte("current-canary-recovery")

	s.omit(1)
	isolated := Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("isolated-local"), ConflictKeys: [][]byte{key}}
	isolatedRef, err := s.nodes[1].Propose(isolated)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickOnly(1, 6)
	if committedPayload(s.apps[1], isolatedRef, isolated.Payload) {
		t.Fatalf("isolated replica applied %s without quorum before heal: %#v", isolatedRef, s.apps[1])
	}

	quorum := Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("quorum-commit"), ConflictKeys: [][]byte{key}}
	quorumRef, err := s.nodes[2].Propose(quorum)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(6)
	for _, id := range []ReplicaID{2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, quorumRef, quorum.Payload, 1)
	}
	if committedPayload(s.apps[1], quorumRef, quorum.Payload) {
		t.Fatalf("isolated replica applied quorum commit %s before transport healed: %#v", quorumRef, s.apps[1])
	}

	s.heal(1)
	s.tickAll(20)
	requireCommittedPayloadCount(t, s.apps[1], 1, quorumRef, quorum.Payload, 1)

	barrier := Command{ID: CommandID{Client: 1, Sequence: 2}, ConflictKeys: [][]byte{key}}
	barrierRef, err := s.nodes[1].Propose(barrier)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(20)
	requireCommittedPayloadCount(t, s.apps[1], 1, barrierRef, barrier.Payload, 1)
	requireCommittedPayloadCount(t, s.apps[1], 1, isolatedRef, isolated.Payload, 1)
	recoveryRequireAppliedOrder(t, s.apps[1], 1, quorumRef, isolatedRef, barrierRef)
	requireNoDuplicateCommittedRefs(t, s.apps[1], 1)
}

func TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit(t *testing.T) {
	s := newSimCluster(t, 3, false)
	key := []byte("rollback-local-key")

	beforeCheckpoint := Command{ID: CommandID{Client: 40, Sequence: 1}, Payload: []byte("before-checkpoint"), ConflictKeys: [][]byte{key}}
	beforeCheckpointRef, err := s.nodes[3].Propose(beforeCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	requireCommittedPayloadCount(t, s.apps[3], 3, beforeCheckpointRef, beforeCheckpoint.Payload, 1)

	checkpointStore := cloneMemoryStorage(s.stores[3])
	checkpointApp := cloneCommittedCommands(s.apps[3])

	learned := Command{ID: CommandID{Client: 40, Sequence: 2}, Payload: []byte("learned-after-checkpoint"), ConflictKeys: [][]byte{key}}
	learnedRef, err := s.nodes[3].Propose(learned)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	if learnedRef.Replica != 3 || learnedRef.Instance <= beforeCheckpointRef.Instance {
		t.Fatalf("post-checkpoint proposal ref=%s, want local instance after %s", learnedRef, beforeCheckpointRef)
	}
	for _, id := range []ReplicaID{1, 2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, learnedRef, learned.Payload, 1)
	}

	quorumRecords := make(map[ReplicaID]InstanceRecord, 2)
	for _, id := range []ReplicaID{1, 2} {
		rec, ok := s.stores[id].Records[learnedRef]
		if !ok || rec.Status < StatusCommitted {
			t.Fatalf("node %d storage record for %s = %#v, ok=%v; want committed quorum record", id, learnedRef, rec, ok)
		}
		quorumRecords[id] = rec
	}

	s.restart(3, checkpointStore)
	s.apps[3] = checkpointApp
	requireCommittedPayloadCount(t, s.apps[3], 3, learnedRef, learned.Payload, 0)

	for _, id := range []ReplicaID{1, 2} {
		if !s.deliver(recoveryCommitMessageFromRecord(id, 3, quorumRecords[id])) {
			t.Fatalf("commit message for %s from node %d to restored node 3 was unexpectedly delayed", learnedRef, id)
		}
	}
	s.drain()
	requireCommittedPayloadCount(t, s.apps[3], 3, learnedRef, learned.Payload, 1)
	requireNoDuplicateCommittedRefs(t, s.apps[3], 3)

	afterCatchUp := Command{ID: CommandID{Client: 40, Sequence: 3}, Payload: []byte("after-catch-up"), ConflictKeys: [][]byte{key}}
	afterCatchUpRef, err := s.nodes[3].Propose(afterCatchUp)
	if err != nil {
		t.Fatal(err)
	}
	if afterCatchUpRef.Replica != 3 || afterCatchUpRef.Instance <= learnedRef.Instance {
		t.Fatalf("proposal after learning local commit ref=%s, want local instance greater than learned ref %s", afterCatchUpRef, learnedRef)
	}
	s.drain()
	for _, id := range []ReplicaID{1, 2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, afterCatchUpRef, afterCatchUp.Payload, 1)
		recoveryRequireAppliedOrder(t, s.apps[id], id, learnedRef, afterCatchUpRef)
		requireNoDuplicateCommittedRefs(t, s.apps[id], id)
	}
}

func recoveryCommitMessageFromRecord(from, to ReplicaID, rec InstanceRecord) Message {
	return Message{
		Type:           MsgCommit,
		From:           from,
		To:             to,
		Ref:            rec.Ref,
		Ballot:         rec.Ballot,
		RecordBallot:   recordTupleBallot(rec),
		Seq:            rec.Seq,
		Deps:           append([]InstanceNum(nil), rec.Deps...),
		AcceptSeq:      rec.AcceptSeq,
		AcceptDeps:     append([]InstanceNum(nil), rec.AcceptDeps...),
		AcceptEvidence: cloneAcceptEvidence(rec.AcceptEvidence),
		Command:        rec.Command.Clone(),
		RecordStatus:   StatusCommitted,
	}
}

func recoveryRequireAppliedOrder(t *testing.T, app []CommittedCommand, id ReplicaID, want ...InstanceRef) {
	t.Helper()
	positions := make(map[InstanceRef]int, len(app))
	for i, c := range app {
		if _, ok := positions[c.Ref]; !ok {
			positions[c.Ref] = i
		}
	}
	previous := -1
	for _, ref := range want {
		pos, ok := positions[ref]
		if !ok {
			t.Fatalf("node %d applied refs %v, missing %s", id, refs(app), ref)
		}
		if pos <= previous {
			t.Fatalf("node %d applied refs %v, want ordered subsequence %v", id, refs(app), want)
		}
		previous = pos
	}
}

func recoveryApplyReady(t *testing.T, store *MemoryStorage, rn *RawNode, rd Ready) {
	t.Helper()
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
}

func recoveryRequirePrepareRefs(t *testing.T, messages []Message, want []InstanceRef) {
	t.Helper()
	for _, ref := range want {
		count := 0
		for _, m := range messages {
			if m.Type == MsgPrepare && m.Ref == ref {
				count++
				if m.From == m.To {
					t.Fatalf("prepare for %s sent to self: %#v", ref, m)
				}
				if m.Ballot.Number == 0 || m.Ballot.Replica == 0 {
					t.Fatalf("prepare for %s has no recovery ballot: %#v", ref, m)
				}
			}
		}
		if count != 2 {
			t.Fatalf("prepare messages for %s = %d, want 2; messages=%#v", ref, count, messages)
		}
	}
}

func recoveryRequireMessageTargets(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, want []ReplicaID) Ballot {
	t.Helper()
	return recoveryRequireMessageTargetsWithDepsWidth(t, messages, typ, ref, want, 4)
}

func recoveryRequireMessageTargetsWithDepsWidth(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, want []ReplicaID, depsWidth int) Ballot {
	t.Helper()
	seen := make(map[ReplicaID]Message, len(want))
	for _, m := range messages {
		if m.Type != typ || m.Ref != ref {
			continue
		}
		if m.From == m.To {
			t.Fatalf("%s for %s sent to self: %#v", typ, ref, m)
		}
		seen[m.To] = m
		if m.Command.Kind != CommandNoop {
			t.Fatalf("%s for %s command kind = %v, want %v: %#v", typ, ref, m.Command.Kind, CommandNoop, m)
		}
		if len(m.Deps) != depsWidth {
			t.Fatalf("%s for %s deps width = %d, want old config width %d: %#v", typ, ref, len(m.Deps), depsWidth, m)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("%s messages for %s targets = %v, want %v; messages=%#v", typ, ref, messageTargets(seen), want, messages)
	}
	var ballot Ballot
	for _, to := range want {
		m, ok := seen[to]
		if !ok {
			t.Fatalf("%s messages for %s targets = %v, want %v; messages=%#v", typ, ref, messageTargets(seen), want, messages)
		}
		if m.Ballot.Number == 0 || m.Ballot.Replica == 0 {
			t.Fatalf("%s for %s has no recovery ballot: %#v", typ, ref, m)
		}
		if ballot == (Ballot{}) {
			ballot = m.Ballot
		} else if m.Ballot != ballot {
			t.Fatalf("%s messages for %s used different ballots: %#v", typ, ref, messages)
		}
	}
	return ballot
}

func recoveryRequireMessageTargetsWithCommandAndDeps(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, wantTargets []ReplicaID, wantCommand Command, wantDeps []InstanceNum) Message {
	t.Helper()
	seen := make(map[ReplicaID]Message, len(wantTargets))
	for _, m := range messages {
		if m.Type != typ || m.Ref != ref {
			t.Fatalf("unexpected message while checking %s retry for %s: %#v; all messages=%#v", typ, ref, m, messages)
		}
		if m.From == m.To {
			t.Fatalf("%s retry for %s sent to self: %#v", typ, ref, m)
		}
		if !commandEqual(m.Command, wantCommand) {
			t.Fatalf("%s retry for %s command = %#v, want %#v", typ, ref, m.Command, wantCommand)
		}
		if len(m.Deps) != len(wantDeps) || !instanceNumsEqual(m.Deps, wantDeps) {
			t.Fatalf("%s retry for %s deps = %v, want old config deps %v: %#v", typ, ref, m.Deps, wantDeps, m)
		}
		seen[m.To] = m
	}
	if len(messages) != len(wantTargets) {
		t.Fatalf("%s retry message count for %s = %d, want %d targets %v; messages=%#v", typ, ref, len(messages), len(wantTargets), wantTargets, messages)
	}
	if len(seen) != len(wantTargets) {
		t.Fatalf("%s retry messages for %s targets = %v, want %v; messages=%#v", typ, ref, messageTargets(seen), wantTargets, messages)
	}
	var exemplar Message
	for _, to := range wantTargets {
		m, ok := seen[to]
		if !ok {
			t.Fatalf("%s retry messages for %s targets = %v, want %v; messages=%#v", typ, ref, messageTargets(seen), wantTargets, messages)
		}
		if exemplar.Type == 0 {
			exemplar = m
		} else if m.Ballot != exemplar.Ballot || m.RecordBallot != exemplar.RecordBallot || m.RecordStatus != exemplar.RecordStatus || m.Seq != exemplar.Seq || !instanceNumsEqual(m.AcceptDeps, exemplar.AcceptDeps) || m.AcceptSeq != exemplar.AcceptSeq {
			t.Fatalf("%s retry messages for %s disagree on tuple metadata: %#v", typ, ref, messages)
		}
	}
	return exemplar
}

func recoveryRequireNoReady(t *testing.T, rn *RawNode, context string) {
	t.Helper()
	if rn.HasReady() {
		t.Fatalf("%s produced Ready: %#v", context, rn.Ready())
	}
}

func recoveryRequireRecordWithCommandAndDeps(t *testing.T, records []InstanceRecord, ref InstanceRef, status Status, wantCommand Command, wantSeq uint64, wantDeps []InstanceNum) InstanceRecord {
	t.Helper()
	for _, rec := range records {
		if rec.Ref != ref || rec.Status != status {
			continue
		}
		if !commandEqual(rec.Command, wantCommand) {
			t.Fatalf("record %s/%s command = %#v, want %#v", ref, status, rec.Command, wantCommand)
		}
		if rec.Command.Kind == CommandNoop {
			t.Fatalf("record %s/%s used noop command, want user command %#v", ref, status, wantCommand)
		}
		if rec.Seq != wantSeq || !instanceNumsEqual(rec.Deps, wantDeps) {
			t.Fatalf("record %s/%s attrs = seq %d deps %v, want seq %d deps %v: %#v", ref, status, rec.Seq, rec.Deps, wantSeq, wantDeps, rec)
		}
		return rec
	}
	t.Fatalf("missing %s record for %s with command %#v attrs seq %d deps %v: %#v", status, ref, wantCommand, wantSeq, wantDeps, records)
	return InstanceRecord{}
}

func recoveryRequireCommittedCommand(t *testing.T, committed []CommittedCommand, ref InstanceRef, wantCommand Command, wantSeq uint64, wantDeps []InstanceNum) {
	t.Helper()
	count := 0
	for _, c := range committed {
		if c.Ref != ref {
			continue
		}
		count++
		if !commandEqual(c.Command, wantCommand) {
			t.Fatalf("committed %s command = %#v, want %#v", ref, c.Command, wantCommand)
		}
		if c.Command.Kind == CommandNoop {
			t.Fatalf("committed %s used noop command, want user command %#v", ref, wantCommand)
		}
		if c.Seq != wantSeq || !instanceNumsEqual(c.Deps, wantDeps) {
			t.Fatalf("committed %s attrs = seq %d deps %v, want seq %d deps %v: %#v", ref, c.Seq, c.Deps, wantSeq, wantDeps, c)
		}
	}
	if count != 1 {
		t.Fatalf("committed commands for %s = %d, want 1 with command %#v; committed=%#v", ref, count, wantCommand, committed)
	}
}

func messageTargets(messages map[ReplicaID]Message) []ReplicaID {
	targets := make([]ReplicaID, 0, len(messages))
	for to := range messages {
		targets = append(targets, to)
	}
	for i := 1; i < len(targets); i++ {
		for j := i; j > 0 && targets[j] < targets[j-1]; j-- {
			targets[j], targets[j-1] = targets[j-1], targets[j]
		}
	}
	return targets
}

func recoveryRequireDependencyRefs(t *testing.T, got []InstanceRef, want []InstanceRef) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("dependency refs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dependency refs = %v, want %v", got, want)
		}
	}
}

func recoveryRequireNoPrepareRefs(t *testing.T, messages []Message, refs []InstanceRef) {
	t.Helper()
	for _, ref := range refs {
		for _, m := range messages {
			if m.Type == MsgPrepare && m.Ref == ref {
				t.Fatalf("unexpected prepare for %s in %#v", ref, messages)
			}
		}
	}
}

func recoveryRequireMessages(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, kind CommandKind) {
	t.Helper()
	count := 0
	for _, m := range messages {
		if m.Type != typ || m.Ref != ref {
			continue
		}
		count++
		if m.Command.Kind != kind {
			t.Fatalf("%s message for %s command kind = %v, want %v: %#v", typ, ref, m.Command.Kind, kind, m)
		}
	}
	if count != 2 {
		t.Fatalf("%s messages for %s = %d, want 2; messages=%#v", typ, ref, count, messages)
	}
}

func recoveryRequireRecord(t *testing.T, records []InstanceRecord, ref InstanceRef, status Status, kind CommandKind) {
	t.Helper()
	for _, rec := range records {
		if rec.Ref != ref || rec.Status != status {
			continue
		}
		if rec.Command.Kind != kind {
			t.Fatalf("record %s/%s command kind = %v, want %v: %#v", ref, status, rec.Command.Kind, kind, rec)
		}
		return
	}
	t.Fatalf("missing %s record for %s with command kind %v: %#v", status, ref, kind, records)
}

func recoveryCommittedPayloadCount(committed []CommittedCommand, ref InstanceRef, payload []byte) int {
	count := 0
	for _, c := range committed {
		if c.Ref == ref && bytes.Equal(c.Command.Payload, payload) {
			count++
		}
	}
	return count
}
