package epaxos

import (
	"bytes"
	"errors"
	"testing"
)

func recoveryFinishPreAcceptedValidation(t *testing.T, store *MemoryStorage, rn *RawNode, rd Ready, ref InstanceRef, targets []ReplicaID, cmd Command, seq uint64, deps []InstanceNum) Ready {
	t.Helper()
	inst := rn.instances[ref]
	if inst.phase == phasePreAccept {
		if !rd.Empty() {
			t.Fatalf("PreAccept slow fallback for %s emitted early work: %#v", ref, rd)
		}
		for tick := uint64(1); tick < rn.retryTicks; tick++ {
			recoveryPersistTickOnlyHardState(t, store, rn, "PreAccept slow fallback before retry deadline")
		}
		rn.Tick()
		rd = rn.Ready()
	}
	if inst.phase == phaseAccept {
		return rd
	}
	if inst.phase != phaseTryPreAccept {
		t.Fatalf("PreAccepted validation phase for %s = %d, want PreAccept, TryPreAccept, or Accept", ref, inst.phase)
	}
	recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgTryPreAccept, ref, targets, cmd, deps)
	recoveryRequireRecordWithCommandAndDeps(t, rd.Records, ref, StatusPreAccepted, cmd, seq, deps)
	recoveryApplyReady(t, store, rn, rd)

	for _, from := range targets {
		if inst.phase == phaseAccept {
			break
		}
		if err := rn.Step(Message{
			Type:         MsgTryPreAcceptResp,
			From:         from,
			To:           rn.id,
			Ref:          ref,
			Ballot:       inst.rec.Ballot,
			Seq:          inst.rec.Seq,
			Deps:         append([]InstanceNum(nil), inst.rec.Deps...),
			RecordStatus: StatusPreAccepted,
		}); err != nil {
			t.Fatalf("step validated PreAccept response from %d: %v", from, err)
		}
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("validated PreAccept phase/record = %d/%#v, want Accept/Accepted", inst.phase, inst.rec)
	}
	return rn.Ready()
}

func TestDependencyClosureStartsPrefixRecoveryBeforeExecution(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}
	recoveryPersistInitialHardState(t, store, rn)

	dependentRef := InstanceRef{Replica: 1, Instance: 3, Conf: 1}
	foreignPrefixRefs := []InstanceRef{
		{Replica: 2, Instance: 1, Conf: 1},
		{Replica: 2, Instance: 2, Conf: 1},
	}
	dependent := Command{ID: CommandID{Client: 10, Sequence: 1}, Payload: []byte("dependent"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{Type: MsgCommit, From: 2,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Seq:          3,
		Deps:         []InstanceNum{0, 2, 0},
		Command:      dependent}
	if err := rn.Step(commit); err != nil {
		t.Fatal(err)
	}
	if got := rn.instances[dependentRef].rec.Deps[1]; got != 2 {
		t.Fatalf("dependent replica-2 compact prefix = %d, want 2", got)
	}
	if rn.instances[foreignPrefixRefs[0]] == nil {
		t.Fatalf("first exact missing dependency %s was not materialized for recovery", foreignPrefixRefs[0])
	}
	if rn.instances[foreignPrefixRefs[1]] != nil {
		t.Fatalf("later dependency %s was materialized before first exact blocker closed", foreignPrefixRefs[1])
	}

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
	recoveryPersistInitialHardState(t, store, rn)

	missingRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	dependentRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	dependent := Command{ID: CommandID{Client: 20, Sequence: 1}, Payload: []byte("after-noop"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{Type: MsgCommit, From: 3,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 3},
		RecordBallot: Ballot{Replica: 3},
		Seq:          2,
		Deps:         []InstanceNum{0, 1, 0},
		Command:      dependent}
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

	acceptResp := Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: missingRef, Ballot: ballot, RecordBallot: ballot, Seq: 1, Deps: []InstanceNum{0, 0, 0}, RecordStatus: StatusAccepted}
	wrongAcceptResp := acceptResp
	wrongAcceptResp.Seq++
	if err := rn.Step(wrongAcceptResp); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, rn, "foreign missing-dependency recovery counted mismatched accept tuple")
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
		seq  uint64
		cmd  Command
	}{
		{name: "none", st: StatusNone, cmd: Command{}},
		{name: "accepted", st: StatusAccepted, cmd: Command{Kind: CommandNoop}, seq: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			ref := InstanceRef{Replica: 2, Instance: 7, Conf: 1}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:     ref,
				Ballot:  Ballot{Number: 1, Replica: 1},
				Status:  tt.st,
				Seq:     tt.seq,
				Deps:    []InstanceNum{0, 0, 0},
				Command: tt.cmd,
			})
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
			if err != nil {
				t.Fatal(err)
			}
			recoveryPersistInitialHardState(t, store, restarted)
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before recovery deadline: %#v", restarted.Ready())
			}
			for tick := 1; tick < 5; tick++ {
				recoveryPersistTickOnlyHardState(t, store, restarted, "foreign recovery before deadline")
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery before deadline")
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
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, RecordBallot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	wrongAcceptResp := acceptResp(3)
	wrongAcceptResp.Seq++
	if err := restarted.Step(wrongAcceptResp); err != nil {
		t.Fatal(err)
	}
	recoveryRequireNoReady(t, restarted, "old-config recovery counted mismatched accept tuple")
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 3, 4}})
	assertConfState(t, restarted.confFor(2), ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before mid-chain old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "mid-chain old-config recovery before deadline")
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
		return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: acceptBallot, RecordBallot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 3, 4}})
	assertConfState(t, restarted.confFor(2), ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	recoveryRequireNoReady(t, restarted, "restart before mid-chain old-config recovery deadline")
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "mid-chain old-config recovery before deadline")
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
		recoveryPersistTickOnlyHardState(t, store, restarted, "mid-chain recovery prepare retry before deadline")
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{2, 3, 4}, 4)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("mid-chain recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "mid-chain recovery prepare retry")
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
		return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: acceptBallot, RecordBallot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
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
		recoveryPersistTickOnlyHardState(t, store, restarted, "mid-chain recovery accept retry before deadline")
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
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "mid-chain recovery accept retry")
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery before deadline")
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	recoveryApplyReady(t, store, restarted, rd)
	if restarted.HasReady() {
		t.Fatalf("old-config recovery prepare left immediate work after Advance: %#v", restarted.Ready())
	}

	for tick := uint64(1); tick < recoveryTicks; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery prepare retry before deadline")
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("old-config recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "old-config recovery prepare retry")
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
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery accept retry before deadline")
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
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "old-config recovery accept retry")
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	recoveryRequireNoReady(t, restarted, "restart before old-config recovery deadline")
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery before deadline")
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
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, RecordBallot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
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
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery accept retry before deadline")
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
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "old-config recovery accept retry")
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	recoveryRequireNoReady(t, restarted, "restart before old-config recovery deadline")
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery before deadline")
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
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery prepare retry before deadline")
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4}, 4)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("old-config recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "old-config recovery prepare retry")
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
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, RecordBallot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := uint64(1); tick < recoveryTicks; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery before deadline")
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3}, 3)
	recoveryApplyReady(t, store, restarted, rd)
	if restarted.HasReady() {
		t.Fatalf("old-config recovery prepare left immediate work after Advance: %#v", restarted.Ready())
	}

	for tick := uint64(1); tick < recoveryTicks; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery prepare retry before deadline")
	}
	restarted.Tick()
	retry := restarted.Ready()
	retryPrepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, retry.Messages, MsgPrepare, ref, []ReplicaID{1, 3}, 3)
	if retryPrepareBallot != prepareBallot {
		t.Fatalf("old-config recovery prepare retry ballot = %#v, want original rebroadcast ballot %#v", retryPrepareBallot, prepareBallot)
	}
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "old-config recovery prepare retry")
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
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery accept retry before deadline")
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
	recoveryRequireNoRecordOrApplicationEffects(t, restarted, retry, "old-config recovery accept retry")
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
	recoveryPersistInitialHardState(t, store, restarted)
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		recoveryPersistTickOnlyHardState(t, store, restarted, "old-config recovery before deadline")
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
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, RecordBallot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
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
			recoveryPersistInitialHardState(t, store, restarted)
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before old-config transition retry deadline: %#v", restarted.Ready())
			}
			for tick := uint64(1); tick < retryTicks; tick++ {
				recoveryPersistTickOnlyHardState(t, store, restarted, "old-config transition retry before deadline")
			}

			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.msgType, ref, []ReplicaID{2, 3, 4}, cmd, tt.deps)
			if retry.Ballot != tt.ballot {
				t.Fatalf("old-config transition %s retry ballot = %v, want %v: %#v", tt.name, retry.Ballot, tt.ballot, retry)
			}
			if retry.RecordBallot != (Ballot{}) || retry.RecordStatus != StatusNone {
				t.Fatalf("old-config transition %s retry exposed record metadata = ballot %v status %s, want canonical zero values: %#v", tt.name, retry.RecordBallot, retry.RecordStatus, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition %s retry seq = %d, want %d: %#v", tt.name, retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition accepted retry accept attrs = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			recoveryRequireNoRecordOrApplicationEffects(t, restarted, rd, "old-config transition "+tt.name+" retry")
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
			recoveryPersistInitialHardState(t, store, restarted)
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before old-config transition retry deadline: %#v", restarted.Ready())
			}
			for tick := uint64(1); tick < retryTicks; tick++ {
				recoveryPersistTickOnlyHardState(t, store, restarted, "old-config transition retry before deadline")
			}

			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.msgType, ref, []ReplicaID{2, 3}, cmd, tt.deps)
			if retry.Ballot != tt.ballot {
				t.Fatalf("old-config transition %s retry ballot = %v, want %v: %#v", tt.name, retry.Ballot, tt.ballot, retry)
			}
			if retry.RecordBallot != (Ballot{}) || retry.RecordStatus != StatusNone {
				t.Fatalf("old-config transition %s retry exposed record metadata = ballot %v status %s, want canonical zero values: %#v", tt.name, retry.RecordBallot, retry.RecordStatus, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition %s retry seq = %d, want %d: %#v", tt.name, retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition accepted retry accept attrs = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			recoveryRequireNoRecordOrApplicationEffects(t, restarted, rd, "old-config transition "+tt.name+" retry")
		})
	}
}

func TestOldConfigChainTransitionRetryUsesPinnedVotersAfterAddThenRemove(t *testing.T) {
	for _, tt := range []struct {
		name           string
		ref            InstanceRef
		status         Status
		msgType        MessageType
		targets        []ReplicaID
		wantRecordConf ConfState
		seq            uint64
		deps           []InstanceNum
		acceptSeq      uint64
		acceptDeps     []InstanceNum
	}{
		{
			name:           "conf1/preaccepted",
			ref:            InstanceRef{Replica: 1, Instance: 11, Conf: 1},
			status:         StatusPreAccepted,
			msgType:        MsgPreAccept,
			targets:        []ReplicaID{2, 3},
			wantRecordConf: ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}},
			seq:            5,
			deps:           []InstanceNum{0, 2, 3},
		},
		{
			name:           "conf1/accepted",
			ref:            InstanceRef{Replica: 1, Instance: 12, Conf: 1},
			status:         StatusAccepted,
			msgType:        MsgAccept,
			targets:        []ReplicaID{2, 3},
			wantRecordConf: ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}},
			seq:            6,
			deps:           []InstanceNum{0, 4, 3},
			acceptSeq:      7,
			acceptDeps:     []InstanceNum{0, 5, 6},
		},
		{
			name:           "conf2/preaccepted",
			ref:            InstanceRef{Replica: 1, Instance: 13, Conf: 2},
			status:         StatusPreAccepted,
			msgType:        MsgPreAccept,
			targets:        []ReplicaID{2, 3, 4},
			wantRecordConf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
			seq:            8,
			deps:           []InstanceNum{0, 2, 3, 4},
		},
		{
			name:           "conf2/accepted",
			ref:            InstanceRef{Replica: 1, Instance: 14, Conf: 2},
			status:         StatusAccepted,
			msgType:        MsgAccept,
			targets:        []ReplicaID{2, 3, 4},
			wantRecordConf: ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
			seq:            9,
			deps:           []InstanceNum{0, 4, 3, 2},
			acceptSeq:      10,
			acceptDeps:     []InstanceNum{0, 7, 6, 5},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Configs = []ConfState{
				{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
				{ID: 3, Voters: []ReplicaID{1, 3, 4}},
			}
			ballot := Ballot{Replica: 1}
			cmd := Command{ID: CommandID{Client: 98, Sequence: 1}, Payload: []byte("old-config-chain-transition-retry-" + tt.name), ConflictKeys: [][]byte{[]byte("old-config-chain-transition-retry-key")}}
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

			const retryTicks = 2
			restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: 10})
			if err != nil {
				t.Fatal(err)
			}
			recoveryPersistInitialHardState(t, store, restarted)
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 3, 4}})
			assertConfState(t, restarted.confFor(tt.ref.Conf), tt.wantRecordConf)
			recoveryRequireNoReady(t, restarted, "restart before old-config chain transition retry deadline")
			for tick := uint64(1); tick < retryTicks; tick++ {
				recoveryPersistTickOnlyHardState(t, store, restarted, "old-config chain transition retry before deadline")
			}

			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.msgType, tt.ref, tt.targets, cmd, tt.deps)
			if retry.Ballot != ballot {
				t.Fatalf("old-config chain transition %s retry ballot = %v, want %v: %#v", tt.name, retry.Ballot, ballot, retry)
			}
			if retry.RecordBallot != (Ballot{}) || retry.RecordStatus != StatusNone {
				t.Fatalf("old-config chain transition %s retry exposed record metadata = ballot %v status %s, want canonical zero values: %#v", tt.name, retry.RecordBallot, retry.RecordStatus, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config chain transition %s retry seq = %d, want %d: %#v", tt.name, retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config chain transition accepted retry evidence = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			recoveryRequireNoRecordOrApplicationEffects(t, restarted, rd, "old-config chain transition "+tt.name+" retry")
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
					return Message{Type: tt.responseType, From: from, To: 1, Ref: tt.ref, Ballot: ballot, Seq: tt.seq, Deps: append([]InstanceNum(nil), tt.deps...), FastPathEligible: true}
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
			recoveryPersistInitialHardState(t, store, restarted)
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
				recoveryPersistTickOnlyHardState(t, store, restarted, "old-config transition retry before deadline after lost response")
			}
			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.retryType, tt.ref, tt.retryTargets, cmd, tt.deps)
			if retry.Ballot != ballot {
				t.Fatalf("old-config transition retry ballot = %v, want %v: %#v", retry.Ballot, ballot, retry)
			}
			if retry.RecordBallot != (Ballot{}) || retry.RecordStatus != StatusNone {
				t.Fatalf("old-config transition retry exposed record metadata = ballot %v status %s, want canonical zero values: %#v", retry.RecordBallot, retry.RecordStatus, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config transition accepted retry evidence = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config transition retry seq = %d, want %d: %#v", retry.Seq, tt.seq, retry)
			}
			recoveryRequireNoRecordOrApplicationEffects(t, restarted, rd, "old-config transition retry")
			recoveryApplyReady(t, store, restarted, rd)

			if err := restarted.Step(response(tt.lostFrom, retry.Ballot)); err != nil {
				t.Fatal(err)
			}
			rd = restarted.Ready()
			switch tt.status {
			case StatusPreAccepted:
				rd = recoveryFinishPreAcceptedValidation(t, store, restarted, rd, tt.ref, tt.retryTargets, cmd, tt.seq, tt.deps)
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

func TestOldConfigChainTransitionRetryCompletesAfterLostPreRetryResponses(t *testing.T) {
	for _, tt := range []struct {
		name         string
		status       Status
		retryType    MessageType
		responseType MessageType
		ref          InstanceRef
		seq          uint64
		deps         []InstanceNum
		acceptSeq    uint64
		acceptDeps   []InstanceNum
		preRetryFrom ReplicaID
		preRetryErr  error
		lostFrom     ReplicaID
		retryTargets []ReplicaID
	}{
		{
			name:         "conf1/preaccepted",
			status:       StatusPreAccepted,
			retryType:    MsgPreAccept,
			responseType: MsgPreAcceptResp,
			ref:          InstanceRef{Replica: 1, Instance: 15, Conf: 1},
			seq:          11,
			deps:         []InstanceNum{0, 2, 3},
			preRetryFrom: 4,
			preRetryErr:  ErrMessageRejected,
			lostFrom:     2,
			retryTargets: []ReplicaID{2, 3},
		},
		{
			name:         "conf1/accepted",
			status:       StatusAccepted,
			retryType:    MsgAccept,
			responseType: MsgAcceptResp,
			ref:          InstanceRef{Replica: 1, Instance: 16, Conf: 1},
			seq:          12,
			deps:         []InstanceNum{0, 0, 0},
			acceptSeq:    13,
			acceptDeps:   []InstanceNum{0, 5, 6},
			preRetryFrom: 4,
			preRetryErr:  ErrMessageRejected,
			lostFrom:     2,
			retryTargets: []ReplicaID{2, 3},
		},
		{
			name:         "conf2/preaccepted",
			status:       StatusPreAccepted,
			retryType:    MsgPreAccept,
			responseType: MsgPreAcceptResp,
			ref:          InstanceRef{Replica: 1, Instance: 17, Conf: 2},
			seq:          14,
			deps:         []InstanceNum{0, 2, 3, 4},
			preRetryFrom: 4,
			lostFrom:     2,
			retryTargets: []ReplicaID{2, 3, 4},
		},
		{
			name:         "conf2/accepted",
			status:       StatusAccepted,
			retryType:    MsgAccept,
			responseType: MsgAcceptResp,
			ref:          InstanceRef{Replica: 1, Instance: 18, Conf: 2},
			seq:          15,
			deps:         []InstanceNum{0, 0, 0, 0},
			acceptSeq:    16,
			acceptDeps:   []InstanceNum{0, 7, 6, 5},
			preRetryFrom: 4,
			lostFrom:     2,
			retryTargets: []ReplicaID{2, 3, 4},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			store.Configs = []ConfState{
				{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
				{ID: 3, Voters: []ReplicaID{1, 3, 4}},
			}
			ballot := Ballot{Replica: 1}
			cmd := Command{ID: CommandID{Client: 99, Sequence: 1}, Payload: []byte("old-config-chain-transition-lost-pre-retry-" + tt.name), ConflictKeys: [][]byte{[]byte("old-config-chain-transition-lost-pre-retry-key")}}
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
					return Message{Type: tt.responseType, From: from, To: 1, Ref: tt.ref, Ballot: ballot, Seq: tt.seq, Deps: append([]InstanceNum(nil), tt.deps...), FastPathEligible: true}
				case StatusAccepted:
					return Message{Type: tt.responseType, From: from, To: 1, Ref: tt.ref, Ballot: ballot, RecordBallot: ballot, Seq: tt.seq, Deps: append([]InstanceNum(nil), tt.deps...), AcceptSeq: tt.acceptSeq, AcceptDeps: append([]InstanceNum(nil), tt.acceptDeps...), RecordStatus: StatusAccepted}
				default:
					t.Fatalf("unhandled status %s", tt.status)
					return Message{}
				}
			}

			const retryTicks = 2
			restarted, err := NewRawNode(Config{ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: retryTicks, RecoveryTicks: 10})
			if err != nil {
				t.Fatal(err)
			}
			recoveryPersistInitialHardState(t, store, restarted)
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 3, 4}})
			assertConfState(t, restarted.confFor(1), ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}})
			assertConfState(t, restarted.confFor(2), ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
			recoveryRequireNoReady(t, restarted, "restart before old-config chain transition lost-response retry deadline")

			preRetryResponse := response(tt.preRetryFrom, ballot)
			if preRetryResponse.Type != tt.responseType || preRetryResponse.From != tt.preRetryFrom || preRetryResponse.To != 1 {
				t.Fatalf("pre-retry response fixture = %#v", preRetryResponse)
			}
			err = restarted.Step(preRetryResponse)
			if tt.preRetryErr == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, tt.preRetryErr) {
				t.Fatalf("pre-retry response err=%v, want %v", err, tt.preRetryErr)
			}
			recoveryRequireNoReady(t, restarted, "old-config chain transition counted pre-retry response before lost old voter")
			lostPreRetryResponse := response(tt.lostFrom, ballot)
			if lostPreRetryResponse.Type != tt.responseType || lostPreRetryResponse.From != tt.lostFrom || lostPreRetryResponse.To != 1 {
				t.Fatalf("lost pre-retry response fixture = %#v", lostPreRetryResponse)
			}

			for tick := uint64(1); tick < retryTicks; tick++ {
				recoveryPersistTickOnlyHardState(t, store, restarted, "old-config chain transition retry before deadline after lost response")
			}
			restarted.Tick()
			rd := restarted.Ready()
			retry := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, tt.retryType, tt.ref, tt.retryTargets, cmd, tt.deps)
			if retry.Ballot != ballot {
				t.Fatalf("old-config chain transition retry ballot = %v, want %v: %#v", retry.Ballot, ballot, retry)
			}
			if retry.RecordBallot != (Ballot{}) || retry.RecordStatus != StatusNone {
				t.Fatalf("old-config chain transition retry exposed record metadata = ballot %v status %s, want canonical zero values: %#v", retry.RecordBallot, retry.RecordStatus, retry)
			}
			if retry.Seq != tt.seq {
				t.Fatalf("old-config chain transition retry seq = %d, want %d: %#v", retry.Seq, tt.seq, retry)
			}
			if tt.status == StatusAccepted && (retry.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(retry.AcceptDeps, tt.acceptDeps)) {
				t.Fatalf("old-config chain transition accepted retry evidence = seq %d deps %v, want seq %d deps %v: %#v", retry.AcceptSeq, retry.AcceptDeps, tt.acceptSeq, tt.acceptDeps, retry)
			}
			recoveryRequireNoRecordOrApplicationEffects(t, restarted, rd, "old-config chain transition retry")
			recoveryApplyReady(t, store, restarted, rd)
			recoveryRequireNoReady(t, restarted, "old-config chain transition retry left immediate work before replacement response")

			if err := restarted.Step(response(tt.lostFrom, retry.Ballot)); err != nil {
				t.Fatal(err)
			}
			rd = restarted.Ready()
			switch tt.status {
			case StatusPreAccepted:
				rd = recoveryFinishPreAcceptedValidation(t, store, restarted, rd, tt.ref, tt.retryTargets, cmd, tt.seq, tt.deps)
				accept := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgAccept, tt.ref, tt.retryTargets, cmd, tt.deps)
				if accept.Seq != tt.seq {
					t.Fatalf("old-config chain transition preaccepted accept seq = %d, want %d: %#v", accept.Seq, tt.seq, accept)
				}
				recoveryRequireRecordWithCommandAndDeps(t, rd.Records, tt.ref, StatusAccepted, cmd, tt.seq, tt.deps)
				if len(rd.Committed) != 0 {
					t.Fatalf("old-config chain transition preaccepted emitted application commands before accept quorum: %#v", rd.Committed)
				}
			case StatusAccepted:
				commit := recoveryRequireMessageTargetsWithCommandAndDeps(t, rd.Messages, MsgCommit, tt.ref, tt.retryTargets, cmd, tt.deps)
				if commit.Seq != tt.seq || commit.AcceptSeq != tt.acceptSeq || !instanceNumsEqual(commit.AcceptDeps, tt.acceptDeps) {
					t.Fatalf("old-config chain transition accepted commit attrs = seq %d accept seq %d deps %v/%v, want seq %d accept seq %d deps %v/%v: %#v", commit.Seq, commit.AcceptSeq, commit.Deps, commit.AcceptDeps, tt.seq, tt.acceptSeq, tt.deps, tt.acceptDeps, commit)
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
			recoveryPersistInitialHardState(t, store, restarted)
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
			recoveryRequireNoReady(t, restarted, "restart before old-config transition removal retry deadline")
			for tick := uint64(1); tick < retryTicks; tick++ {
				recoveryPersistTickOnlyHardState(t, store, restarted, "old-config transition removal retry before deadline")
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
			recoveryRequireNoRecordOrApplicationEffects(t, restarted, rd, "old-config transition removal "+tt.name+" retry")
			recoveryApplyReady(t, store, restarted, rd)

			switch tt.status {
			case StatusPreAccepted:
				preAcceptResp := func(from ReplicaID) Message {
					return Message{Type: MsgPreAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), FastPathEligible: true}
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
				rd = recoveryFinishPreAcceptedValidation(t, store, restarted, rd, ref, []ReplicaID{2, 3, 4}, cmd, tt.seq, tt.deps)
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
					return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, RecordBallot: retry.Ballot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), AcceptSeq: retry.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), retry.AcceptDeps...), RecordStatus: StatusAccepted}
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
			recoveryPersistInitialHardState(t, store, restarted)
			assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
			recoveryRequireNoReady(t, restarted, "restart before old-config transition addition retry deadline")
			for tick := uint64(1); tick < retryTicks; tick++ {
				recoveryPersistTickOnlyHardState(t, store, restarted, "old-config transition addition retry before deadline")
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
			recoveryRequireNoRecordOrApplicationEffects(t, restarted, rd, "old-config transition addition "+tt.name+" retry")
			recoveryApplyReady(t, store, restarted, rd)

			switch tt.status {
			case StatusPreAccepted:
				preAcceptResp := func(from ReplicaID) Message {
					return Message{Type: MsgPreAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), FastPathEligible: true}
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
				rd = recoveryFinishPreAcceptedValidation(t, store, restarted, rd, ref, []ReplicaID{2, 3}, cmd, tt.seq, tt.deps)
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
					return Message{Type: MsgAcceptResp, From: from, To: 1, Ref: ref, Ballot: retry.Ballot, RecordBallot: retry.Ballot, Seq: retry.Seq, Deps: append([]InstanceNum(nil), retry.Deps...), AcceptSeq: retry.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), retry.AcceptDeps...), RecordStatus: StatusAccepted}
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
		RecordBallot:     Ballot{Replica: ref.Replica},
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
	recoveryPersistInitialHardState(t, store, restarted)
	recoveryRequireNoReady(t, restarted, "restart seeded local preaccept vote for non-owner recovery ballot")

	err = restarted.Step(Message{Type: MsgPreAcceptResp, From: 2,
		To:     1,
		Ref:    ref,
		Ballot: ballot,
		Seq:    seq,
		Deps:   append([]InstanceNum(nil), deps...), FastPathEligible: true})
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("non-owner preaccept response err = %v, want %v", err, ErrInvalidMessage)
	}
	recoveryRequireNoReady(t, restarted, "invalid non-owner preaccept response advanced local record")
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
	recoveryPersistInitialHardState(t, store, restarted)
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
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("non-owner accept response err = %v, want %v", err, ErrInvalidMessage)
	}
	recoveryRequireNoReady(t, restarted, "invalid non-owner accept response committed local record")
}

func TestSimulatorNonOwnerRecoversPreAcceptedDependency(t *testing.T) {
	s := newSimCluster(t, 3, false)
	s.drain()
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
	s.drain()
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
	s.drain()
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
	s.drain()
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
	return Message{Type: MsgCommit, From: from,
		To:             to,
		Ref:            rec.Ref,
		Ballot:         rec.Ballot,
		RecordBallot:   recordTupleBallot(rec),
		Seq:            rec.Seq,
		Deps:           append([]InstanceNum(nil), rec.Deps...),
		AcceptSeq:      rec.AcceptSeq,
		AcceptDeps:     append([]InstanceNum(nil), rec.AcceptDeps...),
		AcceptEvidence: cloneAcceptEvidence(rec.AcceptEvidence),
		Command:        rec.Command.Clone()}
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

func recoveryRequireCurrentTickHardState(t *testing.T, rn *RawNode, rd Ready, context string) {
	t.Helper()
	status := rn.Status()
	want := HardState{Conf: status.Conf, Tick: status.Tick}
	if !rd.HardState.Equal(want) || !rd.MustSync {
		t.Fatalf("%s Ready hard state = %#v (MustSync=%v), want current %#v with MustSync", context, rd.HardState, rd.MustSync, want)
	}
}

func recoveryPersistInitialHardState(t *testing.T, store *MemoryStorage, rn *RawNode) {
	t.Helper()
	rd := rn.Ready()
	recoveryRequireCurrentTickHardState(t, rn, rd, "initial")
	if len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("initial Ready carried protocol payload: %#v", rd)
	}
	recoveryApplyReady(t, store, rn, rd)
}

func recoveryPersistTickOnlyHardState(t *testing.T, store *MemoryStorage, rn *RawNode, context string) {
	t.Helper()
	rn.Tick()
	rd := rn.Ready()
	recoveryRequireCurrentTickHardState(t, rn, rd, context)
	if len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("%s produced protocol payload before deadline: %#v", context, rd)
	}
	recoveryApplyReady(t, store, rn, rd)
}

func recoveryRequireNoRecordOrApplicationEffects(t *testing.T, rn *RawNode, rd Ready, context string) {
	t.Helper()
	recoveryRequireCurrentTickHardState(t, rn, rd, context)
	if len(rd.Records) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("%s emitted record/application effects: %#v", context, rd)
	}
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
				if !commandEqual(m.Command, Command{}) ||
					len(m.Deps) != 0 ||
					m.RecordBallot != (Ballot{}) ||
					m.RecordStatus != StatusNone ||
					m.Seq != 0 ||
					m.AcceptSeq != 0 ||
					len(m.AcceptDeps) != 0 ||
					len(m.AcceptEvidence) != 0 {
					t.Fatalf("prepare for %s has non-minimal metadata: %#v", ref, m)
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
		switch typ {
		case MsgPrepare:
			if !commandEqual(m.Command, Command{}) ||
				len(m.Deps) != 0 ||
				m.RecordBallot != (Ballot{}) ||
				m.RecordStatus != StatusNone ||
				m.Seq != 0 ||
				m.AcceptSeq != 0 ||
				len(m.AcceptDeps) != 0 ||
				len(m.AcceptEvidence) != 0 {
				t.Fatalf("prepare for %s has non-minimal metadata: %#v", ref, m)
			}
		case MsgAccept:
			if !commandEqual(m.Command, Command{Kind: CommandNoop}) {
				t.Fatalf("accept for %s command = %#v, want noop", ref, m.Command)
			}
			if len(m.Deps) != depsWidth {
				t.Fatalf("accept for %s deps width = %d, want old config width %d: %#v", ref, len(m.Deps), depsWidth, m)
			}
			if m.RecordBallot != (Ballot{}) || m.RecordStatus != StatusNone {
				t.Fatalf("accept for %s exposed record metadata: %#v", ref, m)
			}
		case MsgCommit:
			if !commandEqual(m.Command, Command{Kind: CommandNoop}) {
				t.Fatalf("commit for %s command = %#v, want noop", ref, m.Command)
			}
			if len(m.Deps) != depsWidth {
				t.Fatalf("commit for %s deps width = %d, want old config width %d: %#v", ref, len(m.Deps), depsWidth, m)
			}
			if m.RecordBallot == (Ballot{}) || m.RecordStatus != StatusNone {
				t.Fatalf("commit for %s record metadata = ballot %v status %s, want nonzero ballot and zero status: %#v", ref, m.RecordBallot, m.RecordStatus, m)
			}
		default:
			t.Fatalf("unsupported recovery message type %s for %s: %#v", typ, ref, m)
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
		switch typ {
		case MsgPreAccept, MsgAccept, MsgTryPreAccept:
			if m.RecordBallot != (Ballot{}) || m.RecordStatus != StatusNone {
				t.Fatalf("%s retry for %s exposed record metadata: %#v", typ, ref, m)
			}
		case MsgCommit:
			if m.RecordBallot == (Ballot{}) || m.RecordStatus != StatusNone {
				t.Fatalf("commit for %s record metadata = ballot %v status %s, want nonzero ballot and zero status: %#v", ref, m.RecordBallot, m.RecordStatus, m)
			}
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

func TestMaxUint64DependencyStartsBoundedExactRecovery(t *testing.T) {
	const driveLimit = 2
	rn, err := NewRawNode(Config{
		ID:                              1,
		Voters:                          makeIDs(7),
		RecoveryTicks:                   5,
		MaxDependencyRecoveriesPerDrive: driveLimit,
		MaxConcurrentRecoveries:         driveLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	max := ^InstanceNum(0)
	dependentRef := InstanceRef{Conf: 1, Replica: 7, Instance: max}
	deps := make([]InstanceNum, 7)
	for i := range deps {
		deps[i] = max
	}
	commit := Message{
		Type:         MsgCommit,
		From:         7,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 7},
		RecordBallot: Ballot{Replica: 7},
		Seq:          9,
		Deps:         deps,
		Command:      Command{ID: CommandID{Client: 700, Sequence: 1}, Payload: []byte("max-dependent")},
	}
	if err := rn.Step(commit); err != nil {
		t.Fatalf("MaxUint64 dependency commit failed: %v", err)
	}
	wantStarted := []InstanceRef{
		{Conf: 1, Replica: 2, Instance: 1},
		{Conf: 1, Replica: 3, Instance: 1},
	}
	for _, ref := range wantStarted {
		inst := rn.instances[ref]
		if inst == nil || inst.phase != phasePrepare || inst.rec.Ballot.Replica != rn.id {
			t.Fatalf("bounded recovery %s = %#v, want local Prepare", ref, inst)
		}
	}
	for replica := ReplicaID(4); replica <= 7; replica++ {
		ref := InstanceRef{Conf: 1, Replica: replica, Instance: 1}
		if rn.instances[ref] != nil {
			t.Fatalf("budget overflow materialized %s", ref)
		}
	}
	if got := len(rn.instances); got != 1+driveLimit {
		t.Fatalf("MaxUint64 dependency materialized %d instances, want %d", got, 1+driveLimit)
	}
	rd := rn.Ready()
	prepareCount := 0
	for _, message := range rd.Messages {
		if message.Type == MsgPrepare {
			prepareCount++
			if message.Ref != wantStarted[0] && message.Ref != wantStarted[1] {
				t.Fatalf("unexpected bounded recovery message for %s", message.Ref)
			}
		}
	}
	if want := driveLimit * (len(deps) - 1); prepareCount != want {
		t.Fatalf("Prepare messages = %d, want %d", prepareCount, want)
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("MaxUint64 dependent executed across absent prefix: %#v", rd.Committed)
	}
	again := rn.Ready()
	if len(again.Records) != len(rd.Records) || len(again.Messages) != len(rd.Messages) || len(again.Committed) != 0 {
		t.Fatalf("repeated Ready changed bounded output: first=%#v second=%#v", rd, again)
	}
}

func TestSparsePrefixRecoveryRoundRobinAndSharedDriveBudget(t *testing.T) {
	rn, err := NewRawNode(Config{
		ID:                              1,
		Voters:                          makeIDs(3),
		MaxDependencyRecoveriesPerDrive: 1,
		MaxConcurrentRecoveries:         8,
	})
	if err != nil {
		t.Fatal(err)
	}
	baseA := InstanceRef{Conf: 1, Replica: 1, Instance: 10}
	baseB := InstanceRef{Conf: 1, Replica: 1, Instance: 11}
	rn.installInstance(&instance{rec: InstanceRecord{Ref: baseA, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, 3, 0}, Command: Command{Kind: CommandNoop}}})
	rn.installInstance(&instance{rec: InstanceRecord{Ref: baseB, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, 0, 3}, Command: Command{Kind: CommandNoop}}})

	first := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	second := InstanceRef{Conf: 1, Replica: 3, Instance: 1}
	third := InstanceRef{Conf: 1, Replica: 2, Instance: 2}
	complete := func(ref InstanceRef) {
		t.Helper()
		inst := rn.instances[ref]
		if inst == nil || inst.phase != phasePrepare {
			t.Fatalf("recovery %s = %#v, want Prepare", ref, inst)
		}
		inst.phase = phaseCommitted
		inst.rec.Status = StatusExecuted
		rn.noteCommitted(ref)
		rn.executed.add(ref)
	}

	rn.beginDrive()
	rn.tryExecute()
	if rn.instances[first] == nil || rn.instances[second] != nil {
		t.Fatalf("first source selection: replica2=%#v replica3=%#v", rn.instances[first], rn.instances[second])
	}
	complete(first)
	rn.tryExecute()
	if rn.instances[second] != nil || rn.instances[third] != nil {
		t.Fatalf("nested tryExecute reset shared budget: second=%#v third=%#v", rn.instances[second], rn.instances[third])
	}
	rn.endDrive()

	rn.tryExecute()
	if rn.instances[second] == nil || rn.instances[third] != nil {
		t.Fatalf("round robin did not advance to second source: second=%#v third=%#v", rn.instances[second], rn.instances[third])
	}
	complete(second)
	rn.tryExecute()
	if rn.instances[third] == nil {
		t.Fatal("round robin did not wrap to the long first source")
	}
}

func TestDependencyRecoveryBudgetZeroDoesNotMaterialize(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), MaxDependencyRecoveriesPerDrive: 1})
	if err != nil {
		t.Fatal(err)
	}
	base := InstanceRef{Conf: 1, Replica: 1, Instance: 2}
	missing := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	rn.installInstance(&instance{rec: InstanceRecord{Ref: base, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, 1, 0}, Command: Command{Kind: CommandNoop}}})
	rn.beginDrive()
	rn.dependencyRecoveryStartsLeft = 0
	rn.tryExecute()
	rn.endDrive()
	if rn.instances[missing] != nil {
		t.Fatalf("exhausted drive budget materialized %s", missing)
	}
}

func TestRestartReconstructsSparsePrefixAndInFlightRecovery(t *testing.T) {
	store := NewMemoryStorage()
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	record := func(ref InstanceRef, status Status, deps []InstanceNum, command Command) InstanceRecord {
		rec := InstanceRecord{
			Ref:          ref,
			Ballot:       Ballot{Replica: ref.Replica},
			RecordBallot: Ballot{Replica: ref.Replica},
			Status:       status,
			Seq:          1,
			Deps:         deps,
			Command:      command,
		}
		rec.Checksum = ChecksumRecord(rec)
		return rec
	}
	one := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	two := InstanceRef{Conf: 1, Replica: 2, Instance: 2}
	three := InstanceRef{Conf: 1, Replica: 2, Instance: 3}
	four := InstanceRef{Conf: 1, Replica: 2, Instance: 4}
	dependent := InstanceRef{Conf: 1, Replica: 1, Instance: 9}
	records := []InstanceRecord{
		record(one, StatusExecuted, make([]InstanceNum, 3), Command{Kind: CommandNoop}),
		record(three, StatusExecuted, make([]InstanceNum, 3), Command{Kind: CommandNoop}),
		record(dependent, StatusCommitted, []InstanceNum{0, 4, 0}, Command{ID: CommandID{Client: 900, Sequence: 1}, Payload: []byte("sparse-restart")}),
	}
	if err := store.ApplyReady(Ready{HardState: HardState{Conf: conf}, Records: records}); err != nil {
		t.Fatal(err)
	}
	config := Config{
		ID:                              1,
		Voters:                          makeIDs(3),
		Storage:                         store,
		MaxDependencyRecoveriesPerDrive: 1,
		MaxConcurrentRecoveries:         2,
	}
	rn, err := NewRawNode(config)
	if err != nil {
		t.Fatal(err)
	}
	if rn.executed.prefix(laneFor(one)) != 1 || !rn.executed.contains(three) {
		t.Fatalf("restart tracker = through %d exact3 %v", rn.executed.prefix(laneFor(one)), rn.executed.contains(three))
	}
	if rn.instances[two] == nil || rn.instances[four] != nil {
		t.Fatalf("startup recovery selected wrong exact hole: two=%#v four=%#v", rn.instances[two], rn.instances[four])
	}
	started := rn.Ready()
	if err := store.ApplyReady(started); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(started); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewRawNode(config)
	if err != nil {
		t.Fatal(err)
	}
	if inst := restarted.instances[two]; inst == nil || inst.phase != phasePrepare {
		t.Fatalf("in-flight recovery did not resume: %#v", inst)
	}
	if restarted.instances[four] != nil {
		t.Fatalf("restart duplicated in-flight recovery by materializing %s", four)
	}

	recovered := record(two, StatusExecuted, make([]InstanceNum, 3), Command{Kind: CommandNoop})
	recovered.Ballot = Ballot{Number: 1, Replica: 1}
	recovered.RecordBallot = recovered.Ballot
	recovered.Checksum = ChecksumRecord(recovered)
	if err := store.ApplyReady(Ready{Records: []InstanceRecord{recovered}}); err != nil {
		t.Fatal(err)
	}
	afterCompletion, err := NewRawNode(config)
	if err != nil {
		t.Fatal(err)
	}
	if got := afterCompletion.executed.prefix(laneFor(one)); got != 3 {
		t.Fatalf("filled durable hole rebuilt frontier %d, want 3", got)
	}
	if afterCompletion.instances[four] == nil {
		t.Fatalf("completed exact recovery did not advance next blocker to %s", four)
	}
}

func TestLegacyTimeOptimizationRestartFloorsTickFromProcessAt(t *testing.T) {
	store := NewMemoryStorage()
	priorRef := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	prior := InstanceRecord{
		Ref:          priorRef,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Status:       StatusAccepted,
		Seq:          1,
		Deps:         make([]InstanceNum, 3),
		Command:      Command{ID: CommandID{Client: 1000, Sequence: 1}, Payload: []byte("legacy"), ConflictKeys: [][]byte{[]byte("legacy-time-key")}},
		ProcessAt:    100,
	}
	prior.Checksum = ChecksumRecordWithoutTimingDomain(prior)
	if !VerifyRecordChecksumWithoutTimingDomain(prior) {
		t.Fatal("test legacy record did not match the previous checksum layout")
	}
	store.Records[priorRef] = prior
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, TimeOptimization: true, TimeOptimizationTicks: 1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ambiguous legacy ProcessAt restart err=%v, want ErrInvalidConfig", err)
	}
	legacyLogical := TimingDomainLogical
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, TimeOptimization: true, TimeOptimizationTicks: 1, LegacyProcessAtDomain: &legacyLogical})
	if err != nil {
		t.Fatal(err)
	}
	if rn.tick != 100 {
		t.Fatalf("legacy logical restart tick = %d, want ProcessAt floor 100", rn.tick)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 1001, Sequence: 1}, Payload: []byte("after-restart"), ConflictKeys: [][]byte{[]byte("legacy-time-key")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := rn.instances[ref].rec.ProcessAt; got != 101 {
		t.Fatalf("post-migration proposal ProcessAt = %d, want 101", got)
	}
	if got := rn.instances[ref].rec.Deps[1]; got != priorRef.Instance {
		t.Fatalf("post-migration proposal lost conflicting durable dependency: deps=%v", rn.instances[ref].rec.Deps)
	}
}

func TestRecoveryAdoptionPreservesProcessAtAcrossRestart(t *testing.T) {
	tests := []struct {
		name             string
		status           Status
		fastPathEligible bool
		wantPhase        phase
	}{
		{name: "accepted", status: StatusAccepted, wantPhase: phaseAccept},
		{name: "fast-preaccepted", status: StatusPreAccepted, fastPathEligible: true, wantPhase: phaseTryPreAccept},
		{name: "preaccepted-fallback", status: StatusPreAccepted, wantPhase: phaseAccept},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryStorage()
			clock := uint64(200)
			runtime := &TOQRuntimeConfig{
				Conf:        ConfState{ID: 1, Voters: makeIDs(3)},
				OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 0, 3: 0},
			}
			config := Config{ID: 1, Voters: makeIDs(3), Storage: store, TOQ: true, TOQClock: func() uint64 { return clock }, TOQRuntime: runtime}
			rn, err := NewRawNode(config)
			if err != nil {
				t.Fatal(err)
			}
			recoveryPersistInitialHardState(t, store, rn)
			target := InstanceRef{Conf: 1, Replica: 2, Instance: 8}
			rec := InstanceRecord{Ref: target, Status: StatusNone, Deps: make([]InstanceNum, 3)}
			rec.Checksum = ChecksumRecord(rec)
			inst := &instance{rec: rec, phase: phaseIdle}
			rn.installInstance(inst)
			if err := rn.startPrepare(inst); err != nil {
				t.Fatal(err)
			}
			initial := rn.Ready()
			if err := store.ApplyReady(initial); err != nil {
				t.Fatal(err)
			}
			if err := rn.Advance(initial); err != nil {
				t.Fatal(err)
			}
			chosenBallot := Ballot{Replica: target.Replica}
			response := Message{
				Type:             MsgPrepareResp,
				From:             2,
				To:               1,
				Ref:              target,
				Ballot:           inst.rec.Ballot,
				RecordBallot:     chosenBallot,
				RecordStatus:     test.status,
				Seq:              3,
				Deps:             make([]InstanceNum, 3),
				Command:          Command{ID: CommandID{Client: 1100, Sequence: 1}, Payload: []byte("timed-recovered")},
				FastPathEligible: test.fastPathEligible,
				ProcessAt:        100,
				TOQ:              true,
			}
			if err := rn.Step(response); err != nil {
				t.Fatal(err)
			}
			if inst.phase != test.wantPhase || inst.rec.ProcessAt != 100 || inst.processAt != 100 {
				t.Fatalf("adopted phase/ProcessAt = %d/%d/%d, want %d/100/100", inst.phase, inst.rec.ProcessAt, inst.processAt, test.wantPhase)
			}
			adopted := rn.Ready()
			foundTimedRecord := false
			for _, record := range adopted.Records {
				if record.Ref == target && record.ProcessAt == 100 {
					foundTimedRecord = true
				}
			}
			if !foundTimedRecord {
				t.Fatalf("adopted Ready lost ProcessAt: %#v", adopted.Records)
			}
			for _, message := range adopted.Messages {
				if (message.Type == MsgAccept || message.Type == MsgTryPreAccept) && message.Ref == target && message.ProcessAt != 100 {
					t.Fatalf("adopted recovery message lost ProcessAt: %#v", message)
				}
			}
			if err := store.ApplyReady(adopted); err != nil {
				t.Fatal(err)
			}
			if err := rn.Advance(adopted); err != nil {
				t.Fatal(err)
			}
			restarted, err := NewRawNode(config)
			if err != nil {
				t.Fatal(err)
			}
			if got := restarted.instances[target].rec.ProcessAt; got != 100 {
				t.Fatalf("restarted recovered ProcessAt = %d, want 100", got)
			}
			if !restarted.toqClosed || restarted.toqClosedThrough != 100 {
				t.Fatalf("restarted TOQ closed bucket = %v/%d record=%#v, want true/100", restarted.toqClosed, restarted.toqClosedThrough, restarted.instances[target].rec)
			}
		})
	}
}

func TestLegacyZeroProcessAtDomainSelection(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	legacy := InstanceRecord{
		Ref:          ref,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Status:       StatusAccepted,
		Seq:          1,
		Deps:         make([]InstanceNum, 3),
		Command:      Command{ID: CommandID{Client: 1200, Sequence: 1}, Payload: []byte("legacy-zero")},
		ProcessAt:    0,
	}
	legacy.Checksum = ChecksumRecordWithoutTimingDomain(legacy)
	store.Records[ref] = legacy
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil legacy zero domain err=%v, want ErrInvalidConfig", err)
	}

	untimed := TimingDomainUntimed
	untimedNode, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, LegacyProcessAtDomain: &untimed})
	if err != nil {
		t.Fatal(err)
	}
	if got := untimedNode.instances[ref].rec.TimingDomain; got != TimingDomainUntimed {
		t.Fatalf("explicit Untimed legacy zero domain = %d", got)
	}

	logical := TimingDomainLogical
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, LegacyProcessAtDomain: &logical}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("explicit Logical legacy zero err=%v, want ErrInvalidConfig", err)
	}

	toq := TimingDomainTOQ
	clock := uint64(0)
	toqNode, err := NewRawNode(Config{
		ID:                    1,
		Voters:                makeIDs(3),
		Storage:               store,
		TOQ:                   true,
		TOQClock:              func() uint64 { return clock },
		TOQRuntime:            &TOQRuntimeConfig{Conf: ConfState{ID: 1, Voters: makeIDs(3)}, OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 0, 3: 0}},
		LegacyProcessAtDomain: &toq,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := toqNode.instances[ref].rec.TimingDomain; got != TimingDomainTOQ {
		t.Fatalf("explicit TOQ legacy zero domain = %d", got)
	}
	if !toqNode.toqClosed || toqNode.toqClosedThrough != 0 {
		t.Fatalf("explicit TOQ legacy zero closed floor = %v/%d", toqNode.toqClosed, toqNode.toqClosedThrough)
	}
}

func TestCanonicalLogicalTimingDomainHonorsAuthoritativeRestartTick(t *testing.T) {
	makeRecord := func(processAt uint64) InstanceRecord {
		ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
		rec := InstanceRecord{
			Ref:          ref,
			Ballot:       Ballot{Replica: 2},
			RecordBallot: Ballot{Replica: 2},
			Status:       StatusAccepted,
			Seq:          1,
			Deps:         make([]InstanceNum, 3),
			Command:      Command{ID: CommandID{Client: 1201, Sequence: 1}, Payload: []byte("canonical-logical")},
			ProcessAt:    processAt,
			TimingDomain: TimingDomainLogical,
		}
		rec.Checksum = ChecksumRecord(rec)
		return rec
	}
	for _, authoritativeTick := range []uint64{0, 10} {
		store := NewMemoryStorage()
		rec := makeRecord(100)
		if err := store.ApplyReady(Ready{HardState: HardState{Conf: ConfState{ID: 1, Voters: makeIDs(3)}, Tick: authoritativeTick}, Records: []InstanceRecord{rec}}); err != nil {
			t.Fatal(err)
		}
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, TimeOptimization: true, TimeOptimizationTicks: 1})
		if err != nil {
			t.Fatal(err)
		}
		if rn.tick != authoritativeTick {
			t.Fatalf("authoritative restart Tick=%d became %d with future ProcessAt=100", authoritativeTick, rn.tick)
		}
	}

	legacy := NewMemoryStorage()
	maxRecord := makeRecord(^uint64(0))
	legacy.Records[maxRecord.Ref] = maxRecord
	maxNode, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: legacy, TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	if maxNode.tick != ^uint64(0) {
		t.Fatalf("legacy absent-HardState logical floor = %d, want MaxUint64", maxNode.tick)
	}
	if _, err := maxNode.Propose(Command{ID: CommandID{Client: 1202, Sequence: 1}, Payload: []byte("after-max")}); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("proposal after Max logical floor err=%v, want ErrLogicalTimeExhausted", err)
	}
	if err := maxNode.Tick(); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("Tick after Max logical floor err=%v, want ErrLogicalTimeExhausted", err)
	}
}

func TestTerminalMessageReleasesDeferredPreAcceptCapacity(t *testing.T) {
	tests := []struct {
		name string
		toq  bool
	}{
		{name: "logical"},
		{name: "toq", toq: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := uint64(0)
			config := Config{ID: 1, Voters: makeIDs(3), MaxDeferredPreAccepts: 1, TimeOptimization: !test.toq}
			if test.toq {
				config.TOQ = true
				config.TOQClock = func() uint64 { return clock }
				config.TOQRuntime = &TOQRuntimeConfig{Conf: ConfState{ID: 1, Voters: makeIDs(3)}, OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 0, 3: 0}}
			}
			rn, err := NewRawNode(config)
			if err != nil {
				t.Fatal(err)
			}
			future := func(instanceNumber InstanceNum, processAt uint64) Message {
				message := Message{
					Type:      MsgPreAccept,
					From:      2,
					To:        1,
					Ref:       InstanceRef{Conf: 1, Replica: 2, Instance: instanceNumber},
					Ballot:    Ballot{Replica: 2},
					ProcessAt: processAt,
					Command:   Command{ID: CommandID{Client: 1300, Sequence: uint64(instanceNumber)}, Payload: []byte("future-owned")},
				}
				if test.toq {
					message.TOQ = true
				} else {
					message.Seq = 1
					message.Deps = make([]InstanceNum, 3)
				}
				return message
			}
			first := future(1, 10)
			if err := rn.Step(first); err != nil {
				t.Fatal(err)
			}
			if err := rn.Step(first.Clone()); err != nil {
				t.Fatalf("duplicate future PreAccept: %v", err)
			}
			if len(rn.deferredIndex) != 1 {
				t.Fatalf("duplicate future entry count = %d", len(rn.deferredIndex))
			}
			var retained *deferredPreAccept
			for _, entry := range rn.deferredIndex {
				retained = entry
			}
			commit := Message{
				Type:         MsgCommit,
				From:         3,
				To:           1,
				Ref:          first.Ref,
				Ballot:       first.Ballot,
				RecordBallot: first.Ballot,
				Seq:          1,
				Deps:         make([]InstanceNum, 3),
				Command:      first.Command.Clone(),
				ProcessAt:    first.ProcessAt,
				TOQ:          test.toq,
			}
			if err := rn.Step(commit); err != nil {
				t.Fatal(err)
			}
			if len(rn.deferredIndex) != 0 || rn.logicalPreAccepts.Len() != 0 || rn.toqPreAccepts.Len() != 0 {
				t.Fatalf("terminal message retained deferred state: index=%d logical=%d toq=%d", len(rn.deferredIndex), rn.logicalPreAccepts.Len(), rn.toqPreAccepts.Len())
			}
			if retained == nil || retained.index != -1 || retained.message.Type != 0 || retained.message.Command.Payload != nil || retained.message.Deps != nil {
				t.Fatalf("removed deferred entry retained owned data: %#v", retained)
			}
			if err := rn.Step(future(2, 11)); err != nil {
				t.Fatalf("second future PreAccept was not admitted after terminal cleanup: %v", err)
			}
			if len(rn.deferredIndex) != 1 {
				t.Fatalf("second future entry count = %d, want 1", len(rn.deferredIndex))
			}
		})
	}
}

func TestEpochCarryRecoveryCountsAgainstConcurrentCap(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), MaxDependencyRecoveriesPerDrive: 2, MaxConcurrentRecoveries: 1})
	if err != nil {
		t.Fatal(err)
	}
	activeRef := InstanceRef{Conf: 1, Replica: 2, Instance: 99}
	active := &instance{rec: InstanceRecord{Ref: activeRef, Ballot: Ballot{Epoch: 1, Number: 0, Replica: 1}, Status: StatusNone, Deps: make([]InstanceNum, 3)}, phase: phasePrepare}
	rn.installInstance(active)
	base := InstanceRef{Conf: 1, Replica: 1, Instance: 2}
	rn.installInstance(&instance{rec: InstanceRecord{Ref: base, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, 1, 0}, Command: Command{Kind: CommandNoop}}})
	rn.tryExecute()
	missing := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	if rn.instances[missing] != nil {
		t.Fatalf("epoch-carry recovery bypassed concurrent cap and materialized %s", missing)
	}
	if got := rn.activeRecoveryCount(); got != 1 {
		t.Fatalf("epoch-carry active recovery count = %d, want 1", got)
	}
}

func TestReplacementGenerationPreventsStaleRecoveryTimerAlias(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(2), RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	command := Command{ID: CommandID{Client: 1600, Sequence: 1}, Payload: []byte("generation-alias")}
	preAccept := Message{Type: MsgPreAccept, From: 2, To: 1, Ref: ref, Ballot: Ballot{Replica: 2}, Seq: 1, Deps: make([]InstanceNum, 2), Command: command}
	if err := rn.Step(preAccept); err != nil {
		t.Fatal(err)
	}
	if rn.instances[ref].generation != 0 {
		t.Fatalf("initial follower generation = %d, want 0", rn.instances[ref].generation)
	}
	advanceOK(t, rn, rn.Ready())
	for range 5 {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rn.Ready())
	}
	accept := Message{Type: MsgAccept, From: 1, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: make([]InstanceNum, 2), Command: command}
	if err := rn.Step(accept); err != nil {
		t.Fatal(err)
	}
	if rn.instances[ref].generation != 1 {
		t.Fatalf("replacement generation = %d, want 1", rn.instances[ref].generation)
	}
	advanceOK(t, rn, rn.Ready())
	for rn.tick < 10 {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		for _, message := range rd.Messages {
			if message.Type == MsgPrepare {
				t.Fatalf("stale generation timer started Prepare at tick %d: %#v", rn.tick, message)
			}
		}
		advanceOK(t, rn, rd)
	}
	for rn.tick < 15 {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		if rn.tick < 15 {
			for _, message := range rd.Messages {
				if message.Type == MsgPrepare {
					t.Fatalf("replacement-relative Prepare fired early at tick %d", rn.tick)
				}
			}
		} else {
			found := false
			for _, message := range rd.Messages {
				found = found || message.Type == MsgPrepare
			}
			if !found {
				t.Fatalf("replacement-relative timer emitted no Prepare at tick %d: %#v", rn.tick, rd)
			}
		}
		advanceOK(t, rn, rd)
	}
}

func TestReplacementGenerationOverflowRejectsWithoutMutation(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(2), RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	rec := checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 2},
		Status:  StatusPreAccepted,
		Seq:     1,
		Deps:    make([]InstanceNum, 2),
		Command: Command{ID: CommandID{Client: 1601, Sequence: 1}, Payload: []byte("generation-overflow")},
	})
	inst := &instance{rec: rec, phase: phaseIdle, generation: ^uint64(0)}
	rn.installInstance(inst)
	before := inst.rec.Clone()
	readyBefore := cloneReady(rn.pendingReady)
	message := Message{Type: MsgAccept, From: 1, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: make([]InstanceNum, 2), Command: rec.Command.Clone()}
	if err := rn.Step(message); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("generation overflow replacement err=%v, want ErrLogicalTimeExhausted", err)
	}
	if rn.instances[ref] != inst || inst.generation != ^uint64(0) || !instanceRecordEqual(inst.rec, before) {
		t.Fatalf("generation overflow mutated instance: %#v before=%#v", inst, before)
	}
	if len(rn.pendingReady.Records) != len(readyBefore.Records) || len(rn.pendingReady.Messages) != len(readyBefore.Messages) ||
		len(rn.pendingReady.Committed) != len(readyBefore.Committed) || !rn.pendingReady.HardState.Equal(readyBefore.HardState) {
		t.Fatalf("generation overflow mutated Ready: before=%#v after=%#v", readyBefore, rn.pendingReady)
	}
}

func TestMaxGenerationDuplicateRequestsAndCommitRemainTolerant(t *testing.T) {
	newNode := func(t *testing.T) *RawNode {
		t.Helper()
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(2), RetryTicks: 10, RecoveryTicks: 10})
		if err != nil {
			t.Fatal(err)
		}
		return rn
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	command := Command{ID: CommandID{Client: 1602, Sequence: 1}, Payload: []byte("generation-duplicate"), ConflictKeys: [][]byte{[]byte("generation-duplicate-key")}}

	t.Run("PreAccept duplicate re-acks", func(t *testing.T) {
		rn := newNode(t)
		message := Message{Type: MsgPreAccept, From: 2, To: 1, Ref: ref, Ballot: Ballot{Replica: 2}, Seq: 1, Deps: make([]InstanceNum, 2), Command: command}
		if err := rn.Step(message); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rn.Ready())
		inst := rn.instances[ref]
		inst.generation = ^uint64(0)
		if err := rn.Step(message); err != nil {
			t.Fatalf("exact duplicate PreAccept at max generation: %v", err)
		}
		rd := rn.Ready()
		if len(rd.Records) != 0 || len(rd.Messages) != 1 || rd.Messages[0].Type != MsgPreAcceptResp {
			t.Fatalf("duplicate PreAccept Ready = %#v, want only one re-ack", rd)
		}
		if rn.instances[ref] != inst || inst.generation != ^uint64(0) {
			t.Fatalf("duplicate PreAccept replaced max-generation instance: %#v", rn.instances[ref])
		}
	})

	t.Run("Accept duplicate re-acks", func(t *testing.T) {
		rn := newNode(t)
		ballot := Ballot{Number: 1, Replica: 2}
		message := Message{Type: MsgAccept, From: 2, To: 1, Ref: ref, Ballot: ballot, Seq: 1, Deps: make([]InstanceNum, 2), Command: command}
		if err := rn.Step(message); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rn.Ready())
		inst := rn.instances[ref]
		inst.generation = ^uint64(0)
		if err := rn.Step(message); err != nil {
			t.Fatalf("exact duplicate Accept at max generation: %v", err)
		}
		rd := rn.Ready()
		if len(rd.Records) != 0 || len(rd.Messages) != 1 || rd.Messages[0].Type != MsgAcceptResp {
			t.Fatalf("duplicate Accept Ready = %#v, want only one re-ack", rd)
		}
		if rn.instances[ref] != inst || inst.generation != ^uint64(0) {
			t.Fatalf("duplicate Accept replaced max-generation instance: %#v", rn.instances[ref])
		}
	})

	t.Run("Commit installs without incrementing old generation", func(t *testing.T) {
		rn := newNode(t)
		ballot := Ballot{Number: 1, Replica: 2}
		accept := Message{Type: MsgAccept, From: 2, To: 1, Ref: ref, Ballot: ballot, Seq: 1, Deps: make([]InstanceNum, 2), Command: command}
		if err := rn.Step(accept); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rn.Ready())
		old := rn.instances[ref]
		old.generation = ^uint64(0)
		commit := Message{Type: MsgCommit, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordBallot: ballot, Seq: 1, Deps: make([]InstanceNum, 2), Command: command}
		if err := rn.Step(commit); err != nil {
			t.Fatalf("Commit over max-generation Accepted instance: %v", err)
		}
		if rn.instances[ref] == old || rn.instances[ref].rec.Status < StatusCommitted {
			t.Fatalf("Commit did not install committed tuple: %#v", rn.instances[ref])
		}
	})
}

func TestLegacyTimedNoopMigrationIsAuthenticatedAndCanonical(t *testing.T) {
	makeStore := func(rec InstanceRecord) *MemoryStorage {
		store := NewMemoryStorage()
		store.Hard = HardState{Conf: ConfState{ID: 1, Voters: makeIDs(3)}}
		store.Records[rec.Ref] = rec.Clone()
		return store
	}
	t.Run("logical nonpending", func(t *testing.T) {
		ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
		ballot := Ballot{Replica: 2}
		rec := InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: Command{Kind: CommandNoop}, ProcessAt: 12}
		rec.Checksum = ChecksumRecordWithoutTimingDomain(rec)
		domain := TimingDomainLogical
		store := makeStore(rec)
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, TimeOptimization: true, LegacyProcessAtDomain: &domain})
		if err != nil {
			t.Fatal(err)
		}
		got := rn.instances[ref].rec
		if got.Status != rec.Status || got.Ballot != rec.Ballot || got.RecordBallot != rec.RecordBallot || got.Seq != rec.Seq || !instanceNumsEqual(got.Deps, rec.Deps) ||
			got.ProcessAt != 0 || got.TimingDomain != TimingDomainUntimed || got.TOQPending || got.Command.Kind != CommandNoop || !VerifyRecordChecksum(got) {
			t.Fatalf("logical legacy no-op migration = %#v, want preserved tuple with canonical untimed metadata", got)
		}
		rd := rn.Ready()
		found := false
		for _, upgraded := range rd.Records {
			found = found || (upgraded.Ref == ref && upgraded.Checksum == got.Checksum)
		}
		if !found {
			t.Fatalf("logical legacy no-op migration did not expose canonical upgrade: %#v", rd.Records)
		}
		if err := store.ApplyReady(rd); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rd)
		if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, TimeOptimization: true}); err != nil {
			t.Fatalf("canonical migrated no-op restart: %v", err)
		}
	})

	t.Run("TOQ pending", func(t *testing.T) {
		ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
		rec := InstanceRecord{Ref: ref, Ballot: Ballot{Replica: 1}, Status: StatusNone, Deps: make([]InstanceNum, 3), Command: Command{Kind: CommandNoop}, ProcessAt: 20, TOQPending: true}
		rec.Checksum = ChecksumRecordWithoutTimingDomain(rec)
		domain := TimingDomainTOQ
		store := makeStore(rec)
		conf := ConfState{ID: 1, Voters: makeIDs(3)}
		cfg := Config{ID: 1, Voters: conf.Voters, Storage: store, TOQ: true, TOQClock: func() uint64 { return 20 }, TOQRuntime: &TOQRuntimeConfig{Conf: conf, OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 0, 3: 0}}, LegacyProcessAtDomain: &domain}
		rn, err := NewRawNode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		got := rn.instances[ref]
		if got == nil || got.rec.Status != StatusPreAccepted || got.rec.Ballot != rec.Ballot || got.rec.RecordBallot != rec.Ballot || got.phase != phasePreAccept ||
			got.rec.Seq != 1 || len(got.rec.Deps) != 3 || got.rec.Deps[0] != 0 || got.rec.Deps[1] != 0 || got.rec.Deps[2] != 0 ||
			got.rec.ProcessAt != 0 || got.rec.TimingDomain != TimingDomainUntimed || got.rec.TOQPending || got.rec.Command.Kind != CommandNoop || !got.rec.FastPathEligible || !VerifyRecordChecksum(got.rec) {
			t.Fatalf("TOQ-pending legacy no-op migration = %#v, want complete canonical local PreAccepted vote", got)
		}
		if _, selfVote := got.preOK[rn.id]; !selfVote {
			t.Fatalf("migrated local no-op did not restore self vote: %#v", got.preOK)
		}
		if len(rn.toqPreAccepts) != 0 || len(rn.deferredIndex) != 0 {
			t.Fatalf("migrated TOQ-pending no-op remained in timed queues: toq=%#v deferred=%#v", rn.toqPreAccepts, rn.deferredIndex)
		}
		rd := rn.Ready()
		if len(rd.Records) != 1 || rd.Records[0].Status != StatusPreAccepted || rd.Records[0].RecordBallot != rec.Ballot ||
			rd.Records[0].ProcessAt != 0 || rd.Records[0].TimingDomain != TimingDomainUntimed || rd.Records[0].TOQPending || !VerifyRecordChecksum(rd.Records[0]) {
			t.Fatalf("migration Ready is not one complete canonical local acceptance: %#v", rd.Records)
		}
		if len(rd.Messages) != 0 {
			t.Fatalf("migration emitted protocol output before canonical record persistence: %#v", rd.Messages)
		}
		if err := store.ApplyReady(rd); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rd)
		for tick := uint64(1); tick <= rn.retryTicks; tick++ {
			if err := rn.Tick(); err != nil {
				t.Fatal(err)
			}
			rd = rn.Ready()
			if tick == rn.retryTicks {
				recoveryRequireMessages(t, rd.Messages, MsgPreAccept, ref, CommandNoop)
				for _, message := range rd.Messages {
					if message.Ref == ref {
						if message.ProcessAt != 0 || message.TOQ || message.Command.Kind != CommandNoop {
							t.Fatalf("migrated no-op retry is not canonical untimed: %#v", message)
						}
						if err := message.Validate(conf); err != nil {
							t.Fatalf("migrated no-op retry failed validation: %v", err)
						}
					}
				}
			}
			if err := store.ApplyReady(rd); err != nil {
				t.Fatal(err)
			}
			advanceOK(t, rn, rd)
		}
		for _, from := range []ReplicaID{2, 3} {
			if err := rn.Step(Message{Type: MsgPreAcceptResp, From: from, To: 1, Ref: ref, Ballot: rec.Ballot, Seq: 1, Deps: make([]InstanceNum, 3), FastPathEligible: true}); err != nil {
				t.Fatal(err)
			}
		}
		rd = rn.Ready()
		recoveryRequireRecord(t, rd.Records, ref, StatusExecuted, CommandNoop)
		if err := store.ApplyReady(rd); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rd)
		canonicalCfg := cfg
		canonicalCfg.LegacyProcessAtDomain = nil
		restarted, err := NewRawNode(canonicalCfg)
		if err != nil {
			t.Fatalf("restart after terminal migrated no-op: %v", err)
		}
		if restarted.HasReady() || restarted.instances[ref] == nil || restarted.instances[ref].rec.Status != StatusExecuted {
			t.Fatalf("terminal migrated no-op restart repeated migration or lost terminal state: ready=%v instance=%#v", restarted.HasReady(), restarted.instances[ref])
		}

		wrong := TimingDomainLogical
		if _, err := NewRawNode(Config{ID: 1, Voters: conf.Voters, Storage: makeStore(rec), TOQ: true, TOQClock: func() uint64 { return 20 }, TOQRuntime: cfg.TOQRuntime, LegacyProcessAtDomain: &wrong}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("TOQ-pending legacy no-op wrong policy err=%v, want ErrInvalidConfig", err)
		}
		tampered := rec.Clone()
		tampered.ProcessAt++
		if _, err := NewRawNode(Config{ID: 1, Voters: conf.Voters, Storage: makeStore(tampered), TOQ: true, TOQClock: func() uint64 { return 20 }, TOQRuntime: cfg.TOQRuntime, LegacyProcessAtDomain: &domain}); !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("tampered legacy no-op err=%v, want ErrChecksumMismatch", err)
		}
	})
}

func TestHigherBallotAcceptReplacesRecoveryTimerInPlace(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 2, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	protocolCoverageAdvanceHardStateOnly(t, rn, 0)
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	command := optimizedTestCommand("bounded-timer", "bounded-timer-key")
	for number := uint64(1); number <= 128; number++ {
		message := Message{Type: MsgAccept, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: number, Replica: 2}, Seq: 1, Deps: make([]InstanceNum, 3), Command: command}
		if err := rn.Step(message); err != nil {
			t.Fatal(err)
		}
		if len(rn.timers) != 1 {
			t.Fatalf("higher-ballot replacement %d left %d timers, want one: %#v", number, len(rn.timers), rn.timers)
		}
		if rn.timers[0].kind != timerPrepare || rn.timers[0].gen != rn.instances[ref].generation {
			t.Fatalf("replacement timer %d = %#v, current generation=%d", number, rn.timers[0], rn.instances[ref].generation)
		}
	}
	advanceOK(t, rn, rn.Ready())
	for tick := uint64(1); tick <= 10; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		prepareCount := 0
		for _, message := range rd.Messages {
			if message.Type == MsgPrepare && message.Ref == ref {
				prepareCount++
			}
		}
		if tick < 10 && prepareCount != 0 {
			t.Fatalf("latest recovery timer fired early at tick %d: %#v", tick, rd.Messages)
		}
		if tick == 10 && prepareCount != 2 {
			t.Fatalf("latest recovery timer emitted %d Prepare messages at tick %d, want two targets: %#v", prepareCount, tick, rd.Messages)
		}
		advanceOK(t, rn, rd)
	}
}

func TestPrepareResponseGenerationDeltaPreflightIsAtomic(t *testing.T) {
	t.Run("TryPreAccept then Accept", func(t *testing.T) {
		run := func(t *testing.T, generation uint64, wantErr bool) {
			ref := InstanceRef{Conf: 1, Replica: 2, Instance: 32}
			rn, inst, ballot := optimizedStartRecovery(t, 3, ref)
			inst.generation = generation
			command := optimizedTestCommand("delta-try-accept", "delta-try-accept-key")
			message := Message{Type: MsgPrepareResp, From: 3, To: 1, Ref: ref, Ballot: ballot, RecordBallot: Ballot{Replica: ref.Replica}, RecordStatus: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: command, FastPathEligible: true}
			before := inst.rec.Clone()
			readyBefore := cloneReady(rn.pendingReady)
			err := rn.Step(message)
			if wantErr {
				if !errors.Is(err, ErrLogicalTimeExhausted) {
					t.Fatalf("Max-1 Try->Accept error = %v, want ErrLogicalTimeExhausted", err)
				}
				if inst.generation != generation || inst.phase != phasePrepare || !instanceRecordEqual(inst.rec, before) ||
					len(rn.pendingReady.Records) != len(readyBefore.Records) || len(rn.pendingReady.Messages) != len(readyBefore.Messages) {
					t.Fatalf("Max-1 Try->Accept mutated state: generation=%d phase=%d record=%#v Ready=%#v", inst.generation, inst.phase, inst.rec, rn.pendingReady)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if inst.generation != ^uint64(0) || inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
				t.Fatalf("exact-fit Try->Accept = generation %d phase %d status %s", inst.generation, inst.phase, inst.rec.Status)
			}
		}
		run(t, ^uint64(0)-1, true)
		run(t, ^uint64(0)-2, false)
	})

	t.Run("single voter Accept then Commit", func(t *testing.T) {
		run := func(t *testing.T, generation uint64, wantErr bool) {
			rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
			if err != nil {
				t.Fatal(err)
			}
			protocolCoverageAdvanceHardStateOnly(t, rn, 0)
			ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
			ballot := Ballot{Number: 1, Replica: 1}
			rec := InstanceRecord{Ref: ref, Ballot: ballot, Status: StatusNone, Deps: make([]InstanceNum, 1)}
			rec.Checksum = ChecksumRecord(rec)
			inst := &instance{rec: rec, phase: phasePrepare, generation: generation}
			rn.installInstance(inst)
			message := Message{Type: MsgPrepareResp, From: 1, To: 1, Ref: ref, Ballot: ballot, RecordStatus: StatusNone, Deps: make([]InstanceNum, 1)}
			before := inst.rec.Clone()
			err = rn.Step(message)
			if wantErr {
				if !errors.Is(err, ErrLogicalTimeExhausted) {
					t.Fatalf("single-voter Max-1 Accept->Commit error = %v, want ErrLogicalTimeExhausted", err)
				}
				if inst.generation != generation || inst.phase != phasePrepare || !instanceRecordEqual(inst.rec, before) || rn.HasReady() {
					t.Fatalf("single-voter Max-1 Accept->Commit mutated state: generation=%d phase=%d record=%#v Ready=%#v", inst.generation, inst.phase, inst.rec, rn.Ready())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if inst.generation != ^uint64(0) || inst.phase != phaseCommitted || inst.rec.Status < StatusCommitted {
				t.Fatalf("single-voter exact-fit Accept->Commit = generation %d phase %d status %s", inst.generation, inst.phase, inst.rec.Status)
			}
		}
		run(t, ^uint64(0)-1, true)
		run(t, ^uint64(0)-2, false)
	})
}

func TestDueTimerGenerationDeltaPreflightIsAtomic(t *testing.T) {
	run := func(t *testing.T, generation uint64, wantErr bool) {
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), RetryTicks: 1, RecoveryTicks: 3})
		if err != nil {
			t.Fatal(err)
		}
		protocolCoverageAdvanceHardStateOnly(t, rn, 0)
		ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
		ballot := Ballot{Replica: 1}
		command := optimizedTestCommand("timer-delta", "timer-delta-key")
		rec := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 1), Command: command, FastPathEligible: true})
		inst := &instance{rec: rec, phase: phasePreAccept, generation: generation, preOK: map[ReplicaID]attrVote{1: {seq: 1, deps: make([]InstanceNum, 1), fastPathEligible: true}}}
		rn.installInstance(inst)
		if err := rn.schedule(inst, timerPreAccept, 1); err != nil {
			t.Fatal(err)
		}
		before := inst.rec.Clone()
		err = rn.Tick()
		if wantErr {
			if !errors.Is(err, ErrLogicalTimeExhausted) {
				t.Fatalf("Max-1 due timer error = %v, want ErrLogicalTimeExhausted", err)
			}
			if rn.tick != 0 || inst.generation != generation || inst.phase != phasePreAccept || !instanceRecordEqual(inst.rec, before) || len(rn.timers) != 1 || rn.HasReady() {
				t.Fatalf("Max-1 due timer mutated state: tick=%d instance=%#v timers=%#v Ready=%#v", rn.tick, inst, rn.timers, rn.Ready())
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if rn.tick != 1 || inst.generation != ^uint64(0) || inst.rec.Status != StatusExecuted || len(rn.timers) != 0 {
			t.Fatalf("exact-fit due timer = tick %d generation %d status %s timers %#v", rn.tick, inst.generation, inst.rec.Status, rn.timers)
		}
	}
	run(t, ^uint64(0)-1, true)
	run(t, ^uint64(0)-2, false)
}
