package epaxos

import (
	"encoding/binary"
	"errors"
	"testing"
)

func checkedRecord(rec InstanceRecord) InstanceRecord {
	rec.Checksum = ChecksumRecord(rec)
	return rec
}

func confChangeCommand(cc ConfChange) Command {
	var payload [9]byte
	payload[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(payload[1:], uint64(cc.Replica))
	return Command{Kind: CommandConfChange, Payload: payload[:], ConflictKeys: [][]byte{[]byte("\xffconf")}}
}

func TestRemainingValidationAndEncodingBranches(t *testing.T) {
	reject := Message{Type: MsgAcceptResp, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []InstanceNum{0, 0}, Reject: true}
	if _, err := EncodeMessage(nil, reject); err != nil {
		t.Fatal(err)
	}
	conf := ConfState{ID: 1, Voters: []ReplicaID{1, 2}}
	if err := (Message{Type: MsgCommit, To: 1, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []InstanceNum{0, 0}}).Validate(conf); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("missing sender err=%v", err)
	}
	st := NewMemoryStorage()
	st.Hard = HardState{Conf: ConfState{ID: 3, Voters: makeIDs(3)}}
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: st})
	if err != nil || rn.Status().Conf.ID != 3 {
		t.Fatalf("hard-state conf load err=%v", err)
	}
	st = NewMemoryStorage()
	st.Configs = []ConfState{{ID: 0, Voters: makeIDs(1)}}
	rn, err = NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: st})
	if err != nil || rn.Status().Conf.ID != 1 {
		t.Fatalf("zero config id normalization err=%v", err)
	}
}

func TestRemainingReadyAndProposalBranches(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	rn.pendingReady = Ready{Records: []InstanceRecord{{}, {}}, Messages: []Message{{}, {}}, Committed: []CommittedCommand{{}, {}}, MustSync: true}
	rd := rn.Ready()
	rn.Advance(Ready{Records: rd.Records[:1], Messages: rd.Messages[:1], Committed: rd.Committed[:1]})
	if len(rn.pendingReady.Records) != 1 || len(rn.pendingReady.Messages) != 1 || len(rn.pendingReady.Committed) != 1 {
		t.Fatal("partial advance did not retain tails")
	}
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 5}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("pending conf err=%v", err)
	}
	zr, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), ZeroCopyProposals: true})
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{Payload: []byte("owned"), ConflictKeys: [][]byte{[]byte("k")}}
	if got := zr.ownedCommand(cmd); len(got.Payload) == 0 || &got.Payload[0] != &cmd.Payload[0] {
		t.Fatal("zero-copy ownership branch copied payload")
	}
	inst := &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 9, Conf: 1}, Status: StatusCommitted, Deps: zr.q.deps(), Command: Command{Kind: CommandNoop}}, phase: phaseCommitted}
	zr.startAccept(inst, inst.rec.Attributes())
	zr.commit(inst, inst.rec.Attributes())
	zr.schedule(inst, timerAccept, 0)
}

func TestRemainingResponseBranches(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	inst := rn.instances[ref]
	inst.preOK = nil
	resp := Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Seq: inst.rec.Seq, Deps: inst.rec.Deps}
	rn.handlePreAcceptResp(resp)
	before := len(inst.preOK)
	rn.handlePreAcceptResp(resp)
	if len(inst.preOK) != before {
		t.Fatal("duplicate preaccept response changed votes")
	}
	prepRef := InstanceRef{Replica: 1, Instance: 22, Conf: 1}
	prep := &instance{rec: InstanceRecord{Ref: prepRef, Ballot: Ballot{Replica: 1}, Status: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: Command{Payload: []byte("p")}}, phase: phasePrepare}
	rn.instances[prepRef] = prep
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: prepRef, RecordStatus: StatusAccepted, Ballot: Ballot{Number: 2, Replica: 2}, Seq: 2, Deps: rn.q.deps(), Command: prep.rec.Command})
	if prep.phase != phasePrepare {
		t.Fatal("prepare should wait for quorum")
	}
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: prepRef, RecordStatus: StatusAccepted, Ballot: Ballot{Number: 2, Replica: 2}, Seq: 2, Deps: rn.q.deps(), Command: prep.rec.Command})
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 3, To: 1, Ref: prepRef, RecordStatus: StatusAccepted, Ballot: Ballot{Number: 3, Replica: 3}, Seq: 3, Deps: rn.q.deps(), Command: prep.rec.Command})
	if prep.phase != phaseAccept || prep.rec.Seq != 3 {
		t.Fatalf("highest accepted not chosen: phase=%d seq=%d", prep.phase, prep.rec.Seq)
	}
	committedRef := InstanceRef{Replica: 1, Instance: 23, Conf: 1}
	committed := &instance{rec: InstanceRecord{Ref: committedRef, Ballot: Ballot{Replica: 1}, Status: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: Command{Payload: []byte("c")}}, phase: phasePrepare, prepareOK: map[ReplicaID]InstanceRecord{1: {Ref: committedRef}}}
	rn.instances[committedRef] = committed
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: committedRef, RecordStatus: StatusCommitted, Ballot: Ballot{Number: 3, Replica: 2}, Seq: 3, Deps: rn.q.deps(), Command: committed.rec.Command})
	if committed.rec.Status < StatusCommitted {
		t.Fatal("committed prepare response was not chosen")
	}
}

func TestRemainingDependencyAndStorageBranches(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{Kind: CommandConfChange}
	rn.instances[InstanceRef{Replica: 1, Instance: 1, Conf: 1}] = &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Status: StatusNone, Command: Command{Payload: []byte("skip")}}}
	rn.instances[InstanceRef{Replica: 2, Instance: 1, Conf: 1}] = &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Status: StatusCommitted, Seq: 4, Command: Command{Kind: CommandNoop}}}
	rn.instances[InstanceRef{Replica: 3, Instance: 1, Conf: 1}] = &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 3, Instance: 1, Conf: 1}, Status: StatusCommitted, Seq: 5, Command: Command{Payload: []byte("dep")}}}
	attrs := rn.computeAttrs(cmd, InstanceRef{Replica: 1, Instance: 99, Conf: 1})
	if attrs.Seq != 6 || attrs.Deps[2] != 1 {
		t.Fatalf("conf attrs=%#v", attrs)
	}
	none := rn.computeAttrs(Command{Kind: CommandNoop}, InstanceRef{})
	if none.Seq != 1 {
		t.Fatal("noop attrs failed")
	}
	if rn.hasUnresolvedKnownConflict(InstanceRef{}, nil) {
		t.Fatal("missing instance conflict")
	}
	if deps := rn.dependencyRefs(InstanceRef{}); deps != nil {
		t.Fatal("missing dependency refs")
	}
	st := NewMemoryStorage()
	bad := InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Checksum: [32]byte{1}}
	if err := st.ApplyReady(Ready{Records: []InstanceRecord{bad}}); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("apply checksum err=%v", err)
	}
	refs := []InstanceRef{{Conf: 2, Replica: 1, Instance: 1}, {Conf: 1, Replica: 9, Instance: 9}, {Conf: 1, Replica: 9, Instance: 1}}
	sortRefs(refs)
	if refs[0].Conf != 1 || refs[1].Instance != 9 || refs[2].Conf != 2 {
		t.Fatalf("sort refs %#v", refs)
	}
}

func TestFinalExecutionBranches(t *testing.T) {
	single, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 1, Instance: 10, Conf: 1}
	inst := &instance{rec: InstanceRecord{Ref: ref, Status: StatusPreAccepted, Seq: 1, Deps: single.q.deps(), Command: Command{Payload: []byte("one")}}, phase: phasePreAccept}
	single.instances[ref] = inst
	single.startAccept(inst, inst.rec.Attributes())
	if inst.rec.Status != StatusExecuted {
		t.Fatalf("single-node accept did not execute: %s", inst.rec.Status)
	}

	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	a := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	b := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	c := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	rn.instances[a] = &instance{rec: InstanceRecord{Ref: a, Status: StatusCommitted, Seq: 3, Deps: []InstanceNum{0, 1, 0}, Command: Command{Payload: []byte("a")}}}
	rn.instances[b] = &instance{rec: InstanceRecord{Ref: b, Status: StatusCommitted, Seq: 2, Deps: []InstanceNum{0, 0, 1}, Command: Command{Payload: []byte("b")}}}
	rn.instances[c] = &instance{rec: InstanceRecord{Ref: c, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{1, 0, 0}, Command: Command{Payload: []byte("c")}}}
	comps := rn.executionComponents()
	if len(comps) != 1 || len(comps[0]) != 3 {
		t.Fatalf("expected one cycle component, got %#v", comps)
	}
	rn.tryExecute()
	rd := rn.Ready()
	if len(rd.Committed) != 3 || rd.Committed[0].Seq != 1 || rd.Committed[2].Seq != 3 {
		t.Fatalf("SCC execution order = %#v", rd.Committed)
	}

	rn2, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	x := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	y := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	rn2.instances[x] = &instance{rec: InstanceRecord{Ref: x, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, 1, 0}, Command: Command{Payload: []byte("x")}}}
	rn2.instances[y] = &instance{rec: InstanceRecord{Ref: y, Status: StatusCommitted, Seq: 1, Deps: rn2.q.deps(), Command: Command{Payload: []byte("y")}}}
	if rn2.componentReady([]InstanceRef{x}) {
		t.Fatal("component should wait for committed external dependency")
	}
	rn2.executed[y] = struct{}{}
	if !rn2.componentReady([]InstanceRef{x}) {
		t.Fatal("component should accept executed dependency")
	}
	if comps := rn2.executionComponents(); len(comps) != 1 {
		t.Fatalf("executed dependency should be skipped by component builder: %#v", comps)
	}
	rn2.instances[y].rec.Status = StatusNone
	if rn2.hasUnresolvedKnownConflict(x, map[InstanceRef]struct{}{}) {
		t.Fatal("none-status conflict should be ignored")
	}
	rn2.instances[y].rec.Status = StatusCommitted
	if rn2.hasUnresolvedKnownConflict(x, map[InstanceRef]struct{}{}) {
		t.Fatal("committed conflict should be ignored by unresolved check")
	}

	conf, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
	if err != nil {
		t.Fatal(err)
	}
	conf.applyConfChange(Command{Kind: CommandConfChange, Payload: []byte{99, 0, 0, 0, 0, 0, 0, 0, 0}})
	conf.applyConfChange(Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeRemoveVoter), 1, 0, 0, 0, 0, 0, 0, 0}})
	if conf.Status().Conf.Contains(0) {
		t.Fatal("invalid configuration applied")
	}
}

func TestReadyPersistsExecutedStateForRestart(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 9, Sequence: 1}, Payload: []byte("value"), ConflictKeys: [][]byte{[]byte("k")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Committed) != 1 || rd.Committed[0].Ref != ref || string(rd.Committed[0].Command.Payload) != "value" {
		t.Fatalf("executed command not emitted in ready: %#v", rd.Committed)
	}
	var sawExecuted bool
	for _, rec := range rd.Records {
		if rec.Ref == ref && rec.Status == StatusExecuted {
			sawExecuted = true
		}
	}
	if !sawExecuted {
		t.Fatalf("ready did not persist executed status for %s: %#v", ref, rd.Records)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	rn.Advance(rd)
	persisted, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("stored record %s not found", ref)
	}
	if persisted.Status != StatusExecuted {
		t.Fatalf("stored status=%s, want executed", persisted.Status)
	}
	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	status := restarted.Status()
	if len(status.Executed) != 1 || status.Executed[0] != ref {
		t.Fatalf("restart executed set = %#v, want %s", status.Executed, ref)
	}
	if restarted.HasReady() {
		t.Fatal("restart re-emitted an already executed command")
	}
}

func TestRestartReplaysExecutedConfigurationChange(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusExecuted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
	})
	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	status := restarted.Status()
	if status.Conf.ID != 2 || !status.Conf.Contains(4) || len(status.Conf.Voters) != 4 {
		t.Fatalf("executed config was not replayed: %#v", status.Conf)
	}
	if len(status.Executed) != 1 || status.Executed[0] != ref {
		t.Fatalf("executed config ref was not retained: %#v", status.Executed)
	}
	next, err := restarted.Propose(Command{ID: CommandID{Client: 7, Sequence: 1}, Payload: []byte("after"), ConflictKeys: [][]byte{[]byte("after")}})
	if err != nil {
		t.Fatal(err)
	}
	if next.Conf != 2 {
		t.Fatalf("proposal used config %d after replay, want 2", next.Conf)
	}
}

func TestPreparePromiseForUnknownInstancePersistsAndRejectsStaleBallots(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 2, Instance: 10, Conf: 99}
	promise := Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 2, Replica: 2}}
	if err := rn.Step(promise); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusNone || rd.Records[0].Ballot != promise.Ballot {
		t.Fatalf("promise record = %#v", rd.Records)
	}
	if len(rd.Records[0].Deps) != 3 {
		t.Fatalf("unknown config promise deps = %v, want current voter width", rd.Records[0].Deps)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgPrepareResp || rd.Messages[0].RecordStatus != StatusNone || rd.Messages[0].Ballot != promise.Ballot {
		t.Fatalf("promise response = %#v", rd.Messages)
	}
	rn.Advance(rd)

	stale := promise
	stale.Ballot = Ballot{Number: 1, Replica: 2}
	if err := rn.Step(stale); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgPrepareResp || !rd.Messages[0].Reject || rd.Messages[0].RejectHint != promise.Ballot {
		t.Fatalf("stale prepare response = %#v", rd.Messages)
	}
}

func TestAcceptForCommittedInstanceReturnsChosenCommand(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	chosen := Message{
		Type:    MsgCommit,
		From:    2,
		To:      1,
		Ref:     ref,
		Ballot:  Ballot{Replica: 2},
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("chosen"), ConflictKeys: [][]byte{[]byte("k")}},
	}
	if err := rn.Step(chosen); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Committed) != 1 || string(rd.Committed[0].Command.Payload) != "chosen" {
		t.Fatalf("commit was not applied before accept retry: %#v", rd.Committed)
	}
	rn.Advance(rd)

	retry := Message{
		Type:    MsgAccept,
		From:    2,
		To:      1,
		Ref:     ref,
		Ballot:  Ballot{Number: 9, Replica: 2},
		Seq:     9,
		Deps:    rn.q.deps(),
		Command: Command{ID: CommandID{Client: 1, Sequence: 2}, Payload: []byte("wrong"), ConflictKeys: [][]byte{[]byte("other")}},
	}
	if err := rn.Step(retry); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	if len(rd.Records) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("accept retry changed committed state: records=%#v committed=%#v", rd.Records, rd.Committed)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgCommit || string(rd.Messages[0].Command.Payload) != "chosen" || rd.Messages[0].RecordStatus < StatusCommitted {
		t.Fatalf("accept retry response = %#v", rd.Messages)
	}
}

func TestRestartRetransmitsLocalUncommittedInstances(t *testing.T) {
	tests := []struct {
		name   string
		st     Status
		ballot Ballot
		ticks  uint64
		msg    MessageType
	}{
		{name: "preaccepted", st: StatusPreAccepted, ballot: Ballot{Replica: 1}, ticks: 2, msg: MsgPreAccept},
		{name: "accepted", st: StatusAccepted, ballot: Ballot{Replica: 1}, ticks: 2, msg: MsgAccept},
		{name: "recovering", st: StatusPreAccepted, ballot: Ballot{Number: 1, Replica: 1}, ticks: 5, msg: MsgPrepare},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:     ref,
				Ballot:  tt.ballot,
				Status:  tt.st,
				Seq:     1,
				Deps:    []InstanceNum{0, 0, 0},
				Command: Command{ID: CommandID{Client: 1, Sequence: uint64(tt.st)}, Payload: []byte(tt.name), ConflictKeys: [][]byte{[]byte(tt.name)}},
			})
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
			if err != nil {
				t.Fatal(err)
			}
			if restarted.HasReady() {
				t.Fatal("restart emitted work before the retry deadline")
			}
			for tick := uint64(1); tick < tt.ticks; tick++ {
				restarted.Tick()
				if restarted.HasReady() {
					t.Fatalf("retry fired after %d ticks, before deadline %d", tick, tt.ticks)
				}
			}
			restarted.Tick()
			rd := restarted.Ready()
			if len(rd.Messages) != 2 {
				t.Fatalf("retry messages = %#v", rd.Messages)
			}
			for _, m := range rd.Messages {
				if m.Type != tt.msg || m.Ref != ref || m.From != 1 || (m.To != 2 && m.To != 3) {
					t.Fatalf("retry message = %#v, want %s for %s", m, tt.msg, ref)
				}
			}
		})
	}
}
