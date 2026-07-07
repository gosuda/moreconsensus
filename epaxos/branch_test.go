package epaxos

import (
	"errors"
	"testing"
)

type failingStorage struct{ err error }

func (f failingStorage) InitialState() (HardState, []ConfState, error) {
	return HardState{}, nil, f.err
}
func (f failingStorage) LoadInstances(func(InstanceRecord) error) error { return nil }

type loadFailStorage struct{ rec InstanceRecord }

func (l loadFailStorage) InitialState() (HardState, []ConfState, error)     { return HardState{}, nil, nil }
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
	executed := InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Status: StatusExecuted, Seq: 1, Deps: []InstanceNum{0}, Command: Command{Kind: CommandNoop}}
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
	if !rn.Ready().Empty() {
		t.Fatal("empty ready should be empty")
	}
	rn.Advance(Ready{})
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
	if !rn.Ready().Empty() {
		t.Fatal("ready while awaiting advance should be empty")
	}
	rn.Advance(Ready{Records: rd.Records[:1]})
	rn.Advance(rd)
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
	rn.handlePreAcceptResp(Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref2, Reject: true, RejectHint: Ballot{Number: 5, Replica: 2}, Deps: rn.q.deps()})
	if inst.phase != phasePrepare {
		t.Fatal("reject did not start prepare")
	}
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref2, RecordStatus: StatusAccepted, Ballot: Ballot{Number: 6, Replica: 2}, Seq: 3, Deps: rn.q.deps(), Command: inst.rec.Command})
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref2, RecordStatus: StatusAccepted, Ballot: Ballot{Number: 6, Replica: 2}, Seq: 3, Deps: rn.q.deps(), Command: inst.rec.Command})
	rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 3, To: 1, Ref: ref2, RecordStatus: StatusCommitted, Ballot: Ballot{Number: 7, Replica: 3}, Seq: 4, Deps: rn.q.deps(), Command: inst.rec.Command})
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
	rn.handleAcceptResp(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Reject: true, RejectHint: Ballot{Number: 9, Replica: 2}, Deps: rn.q.deps()})
	inst.phase = phaseAccept
	inst.accOK = nil
	rn.handleAcceptResp(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Deps: rn.q.deps()})
	rn.handleAcceptResp(Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Deps: rn.q.deps()})
	rn.enqueueRecord(InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 99, Conf: 1}, Deps: rn.q.deps(), Command: Command{Kind: CommandNoop}})
	rn.enqueueMessage(Message{Type: MsgPrepareResp, From: 1, To: 2, Ref: ref})
	if _, err := FastQuorum(0); err == nil {
		t.Fatal("expected fast quorum error")
	}
	PutMessage(nil)
	PutCommand(nil)
	rn.applyConfChange(Command{Kind: CommandConfChange, Payload: []byte{99, 0}})
	rn.applyConfChange(Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeAddVoter), 0, 0, 0, 0, 0, 0, 0, 0}})
	rn.applyConfChange(Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeRemoveVoter), 2, 0, 0, 0, 0, 0, 0, 0}})
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
