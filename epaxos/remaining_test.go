package epaxos

import (
	"errors"
	"testing"
)

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
