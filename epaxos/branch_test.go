package epaxos

import (
	"errors"
	"testing"
)

type failingStorage struct{ err error }

func (f failingStorage) InitialState() (StorageState, error) {
	return StorageState{}, f.err
}
func (f failingStorage) LoadInstances(func(InstanceRecord) error) error { return nil }

type loadFailStorage struct{ rec InstanceRecord }

func (l loadFailStorage) InitialState() (StorageState, error)              { return StorageState{}, nil }
func (l loadFailStorage) LoadInstances(fn func(InstanceRecord) error) error { return fn(l.rec) }

func TestConstructionAndReadyBranches(t *testing.T) {
	sentinel := errors.New("boom")
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: failingStorage{err: sentinel}}); !errors.Is(err, sentinel) {
		t.Fatalf("initial state err=%v", err)
	}
	badHS := &MemoryStorage{Hard: HardState{Conf: ConfState{ID: 9, Voters: []ReplicaID{1, 1}}}, Records: map[InstanceRef]InstanceRecord{}}
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: badHS}); err == nil {
		t.Fatal("expected bad hard-state config")
	}
	badCfg := &MemoryStorage{Configs: []ConfState{{ID: 2, Voters: []ReplicaID{0}}}, Records: map[InstanceRef]InstanceRecord{}}
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: badCfg}); err == nil {
		t.Fatal("expected bad config history")
	}
	badRec := InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Checksum: [32]byte{1}}
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: loadFailStorage{rec: badRec}}); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("load checksum err=%v", err)
	}
	executed := InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1}, Status: StatusExecuted, Seq: 1, Deps: []InstanceNum{0}, Command: Command{Kind: CommandNoop}}
	executed.Checksum = ChecksumRecord(executed)
	loaded, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: loadFailStorage{rec: executed}})
	if err != nil || len(loaded.Status().Executed) != 1 {
		t.Fatalf("executed load err=%v", err)
	}
	for _, st := range []Status{StatusNone, StatusPreAccepted, StatusAccepted, StatusCommitted, StatusExecuted, Status(99)} {
		_ = phaseFromStatus(st)
	}
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), ZeroCopyProposals: true})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	wantInitial := HardState{Conf: ConfState{ID: 1, Voters: []ReplicaID{1}}}
	if !initial.HardState.Equal(wantInitial) || !initial.MustSync ||
		len(initial.Records) != 0 || len(initial.Messages) != 0 || len(initial.Committed) != 0 {
		t.Fatalf("initial Ready = %#v, want hard-state-only %#v", initial, wantInitial)
	}
	advanceOK(t, rn, initial)
	if _, err := rn.Propose(Command{Kind: CommandConfChange}); err == nil {
		t.Fatal("expected conf command rejection")
	}
	if _, err := rn.Propose(Command{Kind: CommandNoop}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if rd.Empty() {
		t.Fatal("expected ready")
	}
	if !rn.HasReady() {
		t.Fatal("outstanding ready should remain visible until advance")
	}
	requireSameReady(t, rn.Ready(), rd)
	if err := rn.Advance(Ready{Records: rd.Records[:1], MustSync: rd.MustSync}); err != nil {
		t.Fatal(err)
	}
	tail := rn.Ready()
	if !tail.Empty() {
		if err := rn.Advance(tail); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDirectProtocolBranches(t *testing.T) {
	s := newSimCluster(t, 3, false)
	ref, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	rec := s.nodes[2].instances[ref].rec
	dup := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, Command: rec.Command}
	dup.Checksum = ChecksumMessage(dup)
	if err := s.nodes[2].Step(dup); err != nil {
		t.Fatal(err)
	}
	pre := s.nodes[3]
	pre.instances[ref] = &instance{rec: InstanceRecord{Ref: ref, Ballot: Ballot{Number: 7, Replica: 3}, Status: StatusPreAccepted, Seq: 1, Deps: []InstanceNum{0, 0, 0}, Command: rec.Command}}
	low := dup
	low.To = 3
	low.Ballot = Ballot{Replica: 1}
	low.Checksum = ChecksumMessage(low)
	if err := pre.Step(low); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref2, err := rn.Propose(Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("y"), ConflictKeys: [][]byte{[]byte("y")}})
	if err != nil {
		t.Fatal(err)
	}
	inst := rn.instances[ref2]
	rn.handlePreAcceptResp(Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref2, Ballot: Ballot{Number: 5, Replica: 2}, Reject: true, RejectHint: Ballot{Number: 5, Replica: 2}, Deps: rn.q.deps()})
	if inst.phase != phasePrepare {
		t.Fatal("reject did not start prepare")
	}
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref2, RecordStatus: StatusAccepted, Ballot: Ballot{Number: 6, Replica: 2}, RecordBallot: Ballot{Number: 6, Replica: 2}, Seq: 3, Deps: rn.q.deps(), Command: inst.rec.Command})
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref2, RecordStatus: StatusAccepted, Ballot: Ballot{Number: 6, Replica: 2}, RecordBallot: Ballot{Number: 6, Replica: 2}, Seq: 3, Deps: rn.q.deps(), Command: inst.rec.Command})
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 3, To: 1, Ref: ref2, RecordStatus: StatusCommitted, Ballot: Ballot{Number: 7, Replica: 3}, RecordBallot: Ballot{Number: 7, Replica: 3}, Seq: 4, Deps: rn.q.deps(), Command: inst.rec.Command})
	rn.startAccept(inst, inst.rec.Attributes())
	rn.commit(inst, inst.rec.Attributes())
}

func TestAcceptResponseAndConfigBranches(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	inst := rn.instances[ref]
	rn.startAccept(inst, inst.rec.Attributes())
	rn.handleAcceptResp(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 9, Replica: 2}, Reject: true, RejectHint: Ballot{Number: 9, Replica: 2}, Deps: rn.q.deps()})
	inst.phase = phaseAccept
	inst.rec.Status = StatusAccepted
	inst.rec.RecordBallot = inst.rec.Ballot
	inst.accOK = nil
	rn.handleAcceptResp(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, RecordBallot: inst.rec.Ballot, RecordStatus: StatusAccepted, Seq: inst.rec.Seq, Deps: rn.q.deps()})
	rn.handleAcceptResp(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, RecordBallot: inst.rec.Ballot, RecordStatus: StatusAccepted, Seq: inst.rec.Seq, Deps: rn.q.deps()})
	rn.enqueueRecord(InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 99, Conf: 1}, Deps: rn.q.deps(), Command: Command{Kind: CommandNoop}})
	rn.enqueueMessage(Message{Type: MsgPrepareResp, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}, Deps: rn.q.deps()})
	if _, err := FastQuorum(0); err == nil {
		t.Fatal("expected fast quorum error")
	}
	PutMessage(nil)
	PutCommand(nil)
	rn.applyConfChange(InstanceRef{Replica: 1, Instance: 100, Conf: 1}, Command{Kind: CommandConfChange, Payload: []byte{99, 0}})
	rn.applyConfChange(InstanceRef{Replica: 1, Instance: 101, Conf: 1}, Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeAddVoter), 0, 0, 0, 0, 0, 0, 0, 0}})
	rn.applyConfChange(InstanceRef{Replica: 1, Instance: 102, Conf: 1}, Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeRemoveVoter), 2, 0, 0, 0, 0, 0, 0, 0}})
}

func TestRestartAcceptedForeignInstanceSchedulesPrepareRecovery(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 2, Instance: 7, Conf: 1}
	accepted := InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 2},
		Status:  StatusAccepted,
		Seq:     3,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{ID: CommandID{Client: 40, Sequence: 1}, Payload: []byte("foreign-accepted"), ConflictKeys: [][]byte{[]byte("foreign-accepted-key")}},
	}
	store.Records[ref] = checkedRecord(accepted)

	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 4})
	if err != nil {
		t.Fatal(err)
	}
	inst := rn.instances[ref]
	if inst == nil || inst.phase != phaseAccept {
		t.Fatalf("restart phase for %s = %#v, want accepted instance waiting for prepare recovery timer", ref, inst)
	}
	initial := rn.Ready()
	wantInitial := HardState{Conf: ConfState{ID: 1, Voters: makeIDs(3)}}
	if !initial.HardState.Equal(wantInitial) || !initial.MustSync ||
		len(initial.Records) != 0 || len(initial.Messages) != 0 || len(initial.Committed) != 0 {
		t.Fatalf("restart initial Ready = %#v, want hard-state-only %#v", initial, wantInitial)
	}
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, initial)
	for tick := uint64(1); tick < 4; tick++ {
		rn.Tick()
		tickReady := rn.Ready()
		wantTick := HardState{Conf: wantInitial.Conf, Tick: tick}
		if !tickReady.HardState.Equal(wantTick) || !tickReady.MustSync ||
			len(tickReady.Records) != 0 || len(tickReady.Messages) != 0 || len(tickReady.Committed) != 0 {
			t.Fatalf("foreign accepted recovery Ready after %d ticks = %#v, want hard-state-only %#v", tick, tickReady, wantTick)
		}
		if err := store.ApplyReady(tickReady); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, tickReady)
	}

	rn.Tick()
	if inst.phase != phasePrepare {
		t.Fatalf("foreign accepted recovery phase = %d, want prepare after recovery deadline", inst.phase)
	}
	rd := rn.Ready()
	if !rd.HardState.Equal(HardState{Conf: wantInitial.Conf, Tick: 4}) || !rd.MustSync {
		t.Fatalf("recovery Ready hard state = %#v, want tick 4", rd.HardState)
	}
	recoveryRequirePrepareRefs(t, rd.Messages, []InstanceRef{ref})
	for _, msg := range rd.Messages {
		if msg.Type == MsgAccept && msg.Ref == ref {
			t.Fatalf("foreign accepted restart retried accept instead of prepare recovery: %#v", rd.Messages)
		}
	}
	recovered := optimizedRequireRecord(t, rd, ref)
	if recovered.Status != StatusAccepted || recovered.Ballot.Replica != 1 || !accepted.Ballot.Less(recovered.Ballot) {
		t.Fatalf("prepare recovery record = %#v, want accepted record advanced from %v by local coordinator", recovered, accepted.Ballot)
	}
}

func TestAcceptResponseIgnoresNonHigherRejectHint(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 41, Sequence: 1}, Payload: []byte("accept-reject"), ConflictKeys: [][]byte{[]byte("accept-reject-key")}})
	if err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())

	inst := rn.instances[ref]
	rn.startAccept(inst, inst.rec.Attributes())
	current := inst.rec.Ballot
	advanceOK(t, rn, rn.Ready())
	if err := rn.Step(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: current, Reject: true, RejectHint: current, Deps: rn.q.deps()}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Ballot != current {
		t.Fatalf("stale accept reject moved phase/ballot to %d/%v, want accept/%v", inst.phase, inst.rec.Ballot, current)
	}
	if rn.HasReady() {
		t.Fatalf("stale accept reject produced prepare work: %#v", rn.Ready())
	}
}

func TestAcceptRespIgnoresStaleBallotForCurrentAcceptRound(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 42, Sequence: 1}, Payload: []byte("accept-stale-ballot"), ConflictKeys: [][]byte{[]byte("accept-stale-ballot-key")}})
	if err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())

	inst := rn.instances[ref]
	rn.startAccept(inst, inst.rec.Attributes())
	previousBallot := inst.rec.Ballot
	advanceOK(t, rn, rn.Ready())

	inst.rec.Ballot, err = previousBallot.Next(rn.id)
	if err != nil {
		t.Fatal(err)
	}
	rn.startAccept(inst, inst.rec.Attributes())
	currentBallot := inst.rec.Ballot
	currentAcceptSeq := inst.rec.AcceptSeq
	currentAcceptDeps := append([]InstanceNum(nil), inst.rec.AcceptDeps...)
	advanceOK(t, rn, rn.Ready())

	staleAcceptDeps := []InstanceNum{0, 7, 0}
	if err := rn.Step(Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       previousBallot,
		RecordBallot: previousBallot,
		RecordStatus: StatusAccepted,
		Seq:          inst.rec.Seq,
		Deps:         append([]InstanceNum(nil), inst.rec.Deps...),
		AcceptSeq:    currentAcceptSeq + 7,
		AcceptDeps:   staleAcceptDeps,
	}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("stale accept response committed instance at phase/status %d/%s, want accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	if inst.rec.AcceptSeq != currentAcceptSeq || !instanceNumsEqual(inst.rec.AcceptDeps, currentAcceptDeps) {
		t.Fatalf("stale accept response merged Accept-Deps evidence to seq=%d deps=%v, want seq=%d deps=%v", inst.rec.AcceptSeq, inst.rec.AcceptDeps, currentAcceptSeq, currentAcceptDeps)
	}
	if rn.HasReady() {
		t.Fatalf("stale accept response produced ready work: %#v", rn.Ready())
	}

	if err := rn.Step(Message{Type: MsgAcceptResp, From: 3, To: 1, Ref: ref, Ballot: currentBallot, RecordBallot: currentBallot, RecordStatus: StatusAccepted, Seq: inst.rec.Seq, Deps: append([]InstanceNum(nil), inst.rec.Deps...)}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseCommitted || inst.rec.Status < StatusCommitted {
		t.Fatalf("current-ballot accept response left phase/status %d/%s, want committed or executed", inst.phase, inst.rec.Status)
	}
	requireCommittedForRef(t, rn.Ready(), ref)
}

func TestAcceptRespInitializesNilAccOKAndWaitsForSlowQuorum(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 43, Sequence: 1}, Payload: []byte("accept-init"), ConflictKeys: [][]byte{[]byte("accept-init-key")}})
	if err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())

	inst := rn.instances[ref]
	chosen := inst.rec.Attributes()
	rn.startAccept(inst, chosen)
	currentBallot := inst.rec.Ballot
	advanceOK(t, rn, rn.Ready())

	inst.accOK = nil
	if err := rn.Step(Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       currentBallot,
		RecordBallot: currentBallot,
		RecordStatus: StatusAccepted,
		Seq:          chosen.Seq + 1,
		Deps:         append([]InstanceNum(nil), chosen.Deps...),
	}); err != nil {
		t.Fatal(err)
	}
	if inst.accOK != nil || inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("wrong-tuple accept response changed accept round: phase/status=%d/%s accOK=%#v", inst.phase, inst.rec.Status, inst.accOK)
	}
	if rn.HasReady() {
		t.Fatalf("wrong-tuple accept response emitted ready work: %#v", rn.Ready())
	}

	if err := rn.Step(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: currentBallot, RecordBallot: currentBallot, Seq: chosen.Seq, Deps: append([]InstanceNum(nil), chosen.Deps...), RecordStatus: StatusAccepted}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("first current-ballot accept response committed at phase/status %d/%s, want accept/%s before slow quorum", inst.phase, inst.rec.Status, StatusAccepted)
	}
	if rn.HasReady() {
		t.Fatalf("single accept response emitted ready before slow quorum: %#v", rn.Ready())
	}

	if err := rn.Step(Message{Type: MsgAcceptResp, From: 3, To: 1, Ref: ref, Ballot: currentBallot, RecordBallot: currentBallot, Seq: chosen.Seq, Deps: append([]InstanceNum(nil), chosen.Deps...), RecordStatus: StatusAccepted}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseCommitted || inst.rec.Status < StatusCommitted {
		t.Fatalf("second current-ballot accept response left phase/status %d/%s, want committed after slow quorum", inst.phase, inst.rec.Status)
	}
	requireCommittedForRef(t, rn.Ready(), ref)
}

func TestAcceptRespDuplicateCurrentBallotResponseIgnored(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(5), RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 44, Sequence: 1}, Payload: []byte("accept-duplicate"), ConflictKeys: [][]byte{[]byte("accept-duplicate-key")}})
	if err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())

	inst := rn.instances[ref]
	chosen := inst.rec.Attributes()
	rn.startAccept(inst, chosen)
	currentBallot := inst.rec.Ballot
	advanceOK(t, rn, rn.Ready())

	firstEvidence := Attributes{Seq: chosen.Seq + 2, Deps: []InstanceNum{0, 2, 0, 0, 0}}
	if err := rn.Step(Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       currentBallot,
		RecordBallot: currentBallot,
		Seq:          chosen.Seq,
		Deps:         append([]InstanceNum(nil), chosen.Deps...),
		AcceptSeq:    firstEvidence.Seq,
		AcceptDeps:   append([]InstanceNum(nil), firstEvidence.Deps...),
		RecordStatus: StatusAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Committed) != 0 {
		t.Fatalf("first accept response committed before five-node slow quorum: %#v", rd.Committed)
	}
	rec := optimizedRequireRecord(t, rd, ref)
	recEvidence, ok := rec.AcceptAttributes()
	if !ok || recEvidence.Seq != firstEvidence.Seq || !instanceNumsEqual(recEvidence.Deps, firstEvidence.Deps) {
		t.Fatalf("first accept response evidence = seq %d deps %v ok=%v, want seq %d deps %v", recEvidence.Seq, recEvidence.Deps, ok, firstEvidence.Seq, firstEvidence.Deps)
	}
	advanceOK(t, rn, rd)

	secondEvidence := Attributes{Seq: firstEvidence.Seq + 5, Deps: []InstanceNum{0, 4, 0, 0, 0}}
	if err := rn.Step(Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       currentBallot,
		RecordBallot: currentBallot,
		Seq:          chosen.Seq,
		Deps:         append([]InstanceNum(nil), chosen.Deps...),
		AcceptSeq:    secondEvidence.Seq,
		AcceptDeps:   append([]InstanceNum(nil), secondEvidence.Deps...),
		RecordStatus: StatusAccepted,
	}); err != nil {
		t.Fatal(err)
	}
	gotEvidence, ok := inst.rec.AcceptAttributes()
	if !ok || gotEvidence.Seq != firstEvidence.Seq || !instanceNumsEqual(gotEvidence.Deps, firstEvidence.Deps) {
		t.Fatalf("duplicate accept response changed evidence to seq %d deps %v ok=%v, want first evidence seq %d deps %v", gotEvidence.Seq, gotEvidence.Deps, ok, firstEvidence.Seq, firstEvidence.Deps)
	}
	if rn.HasReady() {
		t.Fatalf("duplicate accept response emitted ready work: %#v", rn.Ready())
	}
}

func TestAcceptRespZeroAcceptEvidenceIgnored(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(5), RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 45, Sequence: 1}, Payload: []byte("accept-zero-evidence"), ConflictKeys: [][]byte{[]byte("accept-zero-evidence-key")}})
	if err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())

	inst := rn.instances[ref]
	rn.startAccept(inst, inst.rec.Attributes())
	currentBallot := inst.rec.Ballot
	initialAcceptSeq := inst.rec.AcceptSeq
	initialAcceptDeps := append([]InstanceNum(nil), inst.rec.AcceptDeps...)
	advanceOK(t, rn, rn.Ready())

	if err := rn.Step(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: currentBallot, RecordBallot: currentBallot, RecordStatus: StatusAccepted, Seq: inst.rec.Seq, Deps: append([]InstanceNum(nil), inst.rec.Deps...)}); err != nil {
		t.Fatal(err)
	}
	if inst.rec.AcceptSeq != initialAcceptSeq || !instanceNumsEqual(inst.rec.AcceptDeps, initialAcceptDeps) {
		t.Fatalf("zero accept evidence changed record evidence to seq=%d deps=%v, want seq=%d deps=%v", inst.rec.AcceptSeq, inst.rec.AcceptDeps, initialAcceptSeq, initialAcceptDeps)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("zero-evidence accept response committed at phase/status %d/%s, want accept/%s before slow quorum", inst.phase, inst.rec.Status, StatusAccepted)
	}
	if rn.HasReady() {
		t.Fatalf("zero accept evidence emitted ready work: %#v", rn.Ready())
	}
}

func TestCanFastCommitPreAcceptAdoptsCoveringTupleAtDefaultBallot(t *testing.T) {
	newCandidate := func(timeOptimization bool, processAt uint64, ballot Ballot) (*RawNode, *instance, Attributes) {
		t.Helper()
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), TimeOptimization: timeOptimization, TimeOptimizationTicks: 5})
		if err != nil {
			t.Fatal(err)
		}
		ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		localDeps := rn.q.deps()
		remoteAttrs := Attributes{Seq: 2, Deps: rn.q.deps()}
		inst := &instance{
			rec: InstanceRecord{
				Ref:              ref,
				Ballot:           ballot,
				Status:           StatusPreAccepted,
				Seq:              1,
				Deps:             localDeps,
				Command:          Command{ID: CommandID{Client: 42, Sequence: 1}, Payload: []byte("stale-originator"), ConflictKeys: [][]byte{[]byte("stale-originator-key")}},
				FastPathEligible: true,
			},
			phase:     phasePreAccept,
			processAt: processAt,
			preOK: map[ReplicaID]attrVote{
				2: {seq: remoteAttrs.Seq, deps: append([]InstanceNum(nil), remoteAttrs.Deps...), fastPathEligible: true},
				3: {seq: remoteAttrs.Seq, deps: append([]InstanceNum(nil), remoteAttrs.Deps...), fastPathEligible: true},
			},
		}
		return rn, inst, remoteAttrs
	}

	for _, tc := range []struct {
		name             string
		timeOptimization bool
		processAt        uint64
		ballot           Ballot
		want             bool
	}{
		{name: "ordinary EPaxos", ballot: Ballot{Replica: 1}, want: true},
		{name: "timed EPaxos", timeOptimization: true, processAt: 5, ballot: Ballot{Replica: 1}, want: true},
		{name: "timed without process at", timeOptimization: true, ballot: Ballot{Replica: 1}, want: true},
		{name: "non-default ballot", timeOptimization: true, processAt: 5, ballot: Ballot{Number: 1, Replica: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rn, inst, remoteAttrs := newCandidate(tc.timeOptimization, tc.processAt, tc.ballot)
			if got := rn.canFastCommitPreAccept(inst, remoteAttrs); got != tc.want {
				t.Fatalf("canFastCommitPreAccept()=%v, want %v; candidate=%#v attrs=%#v", got, tc.want, inst, remoteAttrs)
			}
		})
	}
}

func TestCanFastCommitPreAcceptRejectsIneligibleLocalRecordDespiteMatchingVotes(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	inst := &instance{
		rec: InstanceRecord{
			Ref:              InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			Ballot:           Ballot{Replica: 1},
			Status:           StatusPreAccepted,
			Seq:              attrs.Seq,
			Deps:             append([]InstanceNum(nil), attrs.Deps...),
			Command:          Command{ID: CommandID{Client: 43, Sequence: 1}, Payload: []byte("local-ineligible"), ConflictKeys: [][]byte{[]byte("local-ineligible-key")}},
			FastPathEligible: false,
		},
		phase: phasePreAccept,
		preOK: map[ReplicaID]attrVote{
			2: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
			3: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
		},
	}
	if rn.canFastCommitPreAccept(inst, attrs) {
		t.Fatalf("local attrs fast-committed even though local record is not FastPathEligible: candidate=%#v attrs=%#v", inst, attrs)
	}
	inst.rec.FastPathEligible = true
	if !rn.canFastCommitPreAccept(inst, attrs) {
		t.Fatalf("matching fast-path-eligible local attrs were rejected; control candidate=%#v attrs=%#v", inst, attrs)
	}
}

func TestTOQHigherAttrsFastCommitRejectsIneligibleLocalRecord(t *testing.T) {
	clock := uint64(10)
	rn, err := NewRawNode(Config{
		ID:       1,
		Voters:   makeIDs(3),
		TOQ:      true,
		TOQClock: func() uint64 { return clock },
		TOQRuntime: &TOQRuntimeConfig{
			Conf:        ConfState{ID: 1, Voters: makeIDs(3)},
			OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 2, 3: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	localAttrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	higherAttrs := Attributes{Seq: 2, Deps: append([]InstanceNum(nil), localAttrs.Deps...)}
	inst := &instance{
		rec: InstanceRecord{
			Ref:              InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			Ballot:           Ballot{Replica: 1},
			Status:           StatusPreAccepted,
			Seq:              localAttrs.Seq,
			Deps:             append([]InstanceNum(nil), localAttrs.Deps...),
			Command:          Command{ID: CommandID{Client: 45, Sequence: 1}, Payload: []byte("toq-higher-attrs"), ConflictKeys: [][]byte{[]byte("toq-higher-attrs-key")}},
			FastPathEligible: false,
			ProcessAt:        15,
		},
		phase:     phasePreAccept,
		processAt: 15,
		preOK: map[ReplicaID]attrVote{
			2: {seq: higherAttrs.Seq, deps: append([]InstanceNum(nil), higherAttrs.Deps...), fastPathEligible: true},
			3: {seq: higherAttrs.Seq, deps: append([]InstanceNum(nil), higherAttrs.Deps...), fastPathEligible: true},
		},
	}
	if rn.canFastCommitPreAccept(inst, higherAttrs) {
		t.Fatalf("TOQ higher attrs fast-committed while local record was not FastPathEligible: candidate=%#v attrs=%#v", inst, higherAttrs)
	}
	inst.rec.FastPathEligible = true
	if !rn.canFastCommitPreAccept(inst, higherAttrs) {
		t.Fatalf("TOQ higher attrs with eligible local record were rejected; control candidate=%#v attrs=%#v", inst, higherAttrs)
	}
}

func TestCanFastCommitPreAcceptRejectsIneligibleRemoteVoteDespiteMatchingAttrs(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(5)})
	if err != nil {
		t.Fatal(err)
	}
	attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	inst := &instance{
		rec: InstanceRecord{
			Ref:              InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			Ballot:           Ballot{Replica: 1},
			Status:           StatusPreAccepted,
			Seq:              attrs.Seq,
			Deps:             append([]InstanceNum(nil), attrs.Deps...),
			Command:          Command{ID: CommandID{Client: 44, Sequence: 1}, Payload: []byte("remote-ineligible"), ConflictKeys: [][]byte{[]byte("remote-ineligible-key")}},
			FastPathEligible: true,
		},
		phase: phasePreAccept,
		preOK: map[ReplicaID]attrVote{
			2: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
			3: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: false},
		},
	}
	if rn.canFastCommitPreAccept(inst, attrs) {
		t.Fatalf("local attrs fast-committed even though a matching remote vote is not FastPathEligible: candidate=%#v attrs=%#v", inst, attrs)
	}
	vote := inst.preOK[3]
	vote.fastPathEligible = true
	inst.preOK[3] = vote
	if !rn.canFastCommitPreAccept(inst, attrs) {
		t.Fatalf("matching fast-path-eligible remote votes were rejected; candidate=%#v attrs=%#v", inst, attrs)
	}
}

func TestComparatorAndParserBranches(t *testing.T) {
	h := timerHeap{{deadline: 1, ref: InstanceRef{Replica: 2, Instance: 1}}, {deadline: 1, ref: InstanceRef{Replica: 3, Instance: 0}}}
	if !h.Less(0, 1) {
		t.Fatal("replica comparator branch")
	}
	h = timerHeap{{deadline: 1, ref: InstanceRef{Replica: 2, Instance: 1}, kind: timerAccept}, {deadline: 1, ref: InstanceRef{Replica: 2, Instance: 2}, kind: timerPreAccept}}
	if !h.Less(0, 1) {
		t.Fatal("instance comparator branch")
	}
	h = timerHeap{{deadline: 1, ref: InstanceRef{Replica: 2, Instance: 1}, kind: timerPreAccept}, {deadline: 1, ref: InstanceRef{Replica: 2, Instance: 1}, kind: timerAccept}}
	if !h.Less(0, 1) {
		t.Fatal("kind comparator branch")
	}
	if !lessRef(InstanceRef{Conf: 1}, InstanceRef{Conf: 2}) || !lessRef(InstanceRef{Conf: 1, Replica: 1}, InstanceRef{Conf: 1, Replica: 2}) {
		t.Fatal("lessRef branches")
	}
	p := parser{b: []byte{2, 'x'}}
	if p.bytes() != nil || !p.err {
		t.Fatal("parser bytes should detect short buffer")
	}
	if (Attributes{Seq: 1, Deps: []InstanceNum{1}}).Equal(Attributes{Seq: 1, Deps: []InstanceNum{2}}) {
		t.Fatal("attribute dep equality branch")
	}
	if mergeAttrs(Attributes{}, Attributes{}).Seq != 1 {
		t.Fatal("merge default seq branch")
	}
}
