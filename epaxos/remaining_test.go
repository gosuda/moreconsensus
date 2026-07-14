package epaxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"slices"
	"testing"
)

func checkedRecord(rec InstanceRecord) InstanceRecord {
	if rec.Status >= StatusPreAccepted && rec.RecordBallot == (Ballot{}) {
		rec.RecordBallot = rec.Ballot
	}
	if rec.Status != StatusPreAccepted {
		rec.FastPathEligible = false
	}
	rec.Checksum = ChecksumRecord(rec)
	return rec
}

type remainingNodeProtocolSnapshot struct {
	conf                  ConfState
	currentHardState      HardState
	acknowledgedHardState HardState
	nextInstance          InstanceNum
	tick                  uint64
	pendingConf           bool
	awaitAdvance          bool
	toqActive             bool
	toqActiveConf         ConfState
	toqActiveGroup        []ReplicaID
	toqActiveDelays       map[ReplicaID]uint64
	toqRuntimeCount       int
	toqSeen               bool
	toqLastNow            uint64
	toqClosed             bool
	toqClosedThrough      uint64
	instances             int
	conflicts             int
	executed              int
	appliedConfByBase     int
	confHistory           int
	timers                int
	deferredPreAccepts    int
	tryEvidenceChecks     int
	pendingReady          Ready
	nextReady             Ready
	frozenReady           Ready
}

func snapshotRemainingNodeProtocol(rn *RawNode) remainingNodeProtocolSnapshot {
	snapshot := remainingNodeProtocolSnapshot{
		conf:                  rn.q.conf.Clone(),
		currentHardState:      rn.currentHardState.Clone(),
		acknowledgedHardState: rn.acknowledgedHardState.Clone(),
		nextInstance:          rn.nextInstance,
		tick:                  rn.tick,
		pendingConf:           rn.pendingConf,
		awaitAdvance:          rn.awaitAdvance,
		toqActive:             rn.toqActive != nil,
		toqRuntimeCount:       len(rn.toqRuntimes),
		toqSeen:               rn.toqSeen,
		toqLastNow:            rn.toqLastNow,
		toqClosed:             rn.toqClosed,
		toqClosedThrough:      rn.toqClosedThrough,
		instances:             len(rn.instances),
		conflicts:             rn.engine.residentCount(),
		executed:              len(rn.executed.exact),
		appliedConfByBase:     len(rn.appliedConfByBase),
		confHistory:           len(rn.confHistory),
		timers:                len(rn.timers),
		deferredPreAccepts:    len(rn.deferredIndex),
		tryEvidenceChecks:     len(rn.tryEvidenceChecks),
		pendingReady:          cloneReady(rn.pendingReady),
		nextReady:             cloneReady(rn.nextReady),
		frozenReady:           cloneReady(rn.frozenReady),
	}
	if rn.toqActive != nil {
		snapshot.toqActiveConf = rn.toqActive.conf.Clone()
		snapshot.toqActiveGroup = append([]ReplicaID(nil), rn.toqActive.group...)
		snapshot.toqActiveDelays = make(map[ReplicaID]uint64, len(rn.toqActive.delays))
		for id, delay := range rn.toqActive.delays {
			snapshot.toqActiveDelays[id] = delay
		}
	}
	return snapshot
}

func requireRemainingNodeProtocolUnchanged(t *testing.T, rn *RawNode, before remainingNodeProtocolSnapshot) {
	t.Helper()
	if got := snapshotRemainingNodeProtocol(rn); !reflect.DeepEqual(got, before) {
		t.Fatalf("rejected input mutated node protocol state:\n got  %#v\n want %#v", got, before)
	}
}

func remainingApplyHardStateOnly(t *testing.T, rn *RawNode, wantTick uint64) {
	t.Helper()
	rd := rn.Ready()
	if rd.HardState.Empty() || !rd.HardState.Equal(rn.currentHardState) || rd.HardState.Tick != wantTick || !rd.MustSync {
		t.Fatalf("hard-state-only Ready = %#v, want exact current hard state at tick %d", rd, wantTick)
	}
	if len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("hard-state-only Ready at tick %d carried protocol payload: %#v", wantTick, rd)
	}
	store, ok := rn.storage.(interface{ ApplyReady(Ready) error })
	if !ok {
		t.Fatalf("storage %T cannot persist Ready", rn.storage)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
}

type duplicateRecordStorage struct {
	rec InstanceRecord
}

func (s duplicateRecordStorage) InitialState() (StorageState, error) {
	return StorageState{}, nil
}

func (s duplicateRecordStorage) LoadInstances(fn func(InstanceRecord) error) error {
	if err := fn(s.rec.Clone()); err != nil {
		return err
	}
	return fn(s.rec.Clone())
}

func confChangeCommand(cc ConfChange) Command {
	var payload [9]byte
	payload[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(payload[1:], uint64(cc.Replica))
	return Command{Kind: CommandConfChange, Payload: payload[:], ConflictKeys: [][]byte{[]byte("\xffconf")}}
}

func assertConfState(t *testing.T, got, want ConfState) {
	t.Helper()
	if got.ID != want.ID || !slices.Equal(got.Voters, want.Voters) {
		t.Fatalf("config=%#v, want %#v", got, want)
	}
}

func TestRemainingValidationAndEncodingBranches(t *testing.T) {
	rejectBallot := Ballot{Replica: 1}
	reject := Message{Type: MsgAcceptResp, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Ballot: rejectBallot, Deps: []InstanceNum{0, 0}, Reject: true, RejectReason: RejectNone, RejectHint: rejectBallot}
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
	advanceInvalid(t, rn, Ready{HardState: rd.HardState, Records: rd.Records[:1], Messages: rd.Messages[:1], Committed: rd.Committed[:1], MustSync: rd.MustSync})
	if len(rn.frozenReady.Records) != 2 || len(rn.frozenReady.Messages) != 2 || len(rn.frozenReady.Committed) != 2 {
		t.Fatal("barrier rejection changed frozen ready")
	}
	advanceOK(t, rn, Ready{HardState: rd.HardState, Records: rd.Records[:1], MustSync: rd.MustSync})
	if len(rn.frozenReady.Records) != 1 || len(rn.frozenReady.Messages) != 2 || len(rn.frozenReady.Committed) != 2 {
		t.Fatal("partial record advance did not retain frozen work")
	}
	advanceOK(t, rn, rn.Ready())
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 2}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("pending conf err=%v", err)
	}
	zr, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
	if err != nil {
		t.Fatal(err)
	}
	inst := &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 9, Conf: 1}, Status: StatusCommitted, Deps: zr.q.deps(), Command: Command{Kind: CommandNoop}}, phase: phaseCommitted}
	zr.startAccept(inst, inst.rec.Attributes())
	zr.commit(inst, inst.rec.Attributes())
	if err := zr.schedule(inst, timerAccept, 0); err != nil {
		panic(err)
	}
}

func TestProposeRejectsNonEmptyNoopWithoutDurableMutation(t *testing.T) {
	tests := []struct {
		name string
		cmd  Command
	}{
		{name: "id", cmd: Command{Kind: CommandNoop, ID: CommandID{Client: 1, Sequence: 1}}},
		{name: "payload", cmd: Command{Kind: CommandNoop, Payload: []byte("not-empty")}},
		{name: "conflict key", cmd: Command{Kind: CommandNoop, ConflictKeys: [][]byte{[]byte("not-empty")}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStorage()
			rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotRemainingNodeProtocol(rn)
			if _, err := rn.Propose(tc.cmd); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Propose(%#v) err=%v, want ErrInvalidConfig", tc.cmd, err)
			}
			requireRemainingNodeProtocolUnchanged(t, rn, before)
			if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store}); err != nil {
				t.Fatalf("restart after rejected no-op: %v", err)
			}
		})
	}
}

func TestProposeConfChangeValidatesMembershipChanges(t *testing.T) {
	tests := []struct {
		name   string
		voters []ReplicaID
		change ConfChange
	}{
		{
			name:   "add zero",
			voters: makeIDs(3),
			change: ConfChange{Type: ConfChangeAddVoter, Replica: 0},
		},
		{
			name:   "add existing voter",
			voters: makeIDs(3),
			change: ConfChange{Type: ConfChangeAddVoter, Replica: 2},
		},
		{
			name:   "add beyond seven voters",
			voters: makeIDs(7),
			change: ConfChange{Type: ConfChangeAddVoter, Replica: 8},
		},
		{
			name:   "remove absent voter",
			voters: makeIDs(3),
			change: ConfChange{Type: ConfChangeRemoveVoter, Replica: 4},
		},
		{
			name:   "remove only voter",
			voters: makeIDs(1),
			change: ConfChange{Type: ConfChangeRemoveVoter, Replica: 1},
		},
		{
			name:   "unknown type",
			voters: makeIDs(3),
			change: ConfChange{Type: ConfChangeType(99), Replica: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, err := NewRawNode(Config{ID: 1, Voters: tt.voters})
			if err != nil {
				t.Fatal(err)
			}
			before := rn.Status().Conf
			beforeProtocol := snapshotRemainingNodeProtocol(rn)

			want := ErrInvalidConfig
			if tt.change.Type == ConfChangeAddVoter {
				want = ErrVoterCertificateRequired
			}
			if _, err := rn.ProposeConfChange(tt.change); !errors.Is(err, want) {
				t.Fatalf("ProposeConfChange(%#v) err=%v, want %v", tt.change, err, want)
			}
			assertConfState(t, rn.Status().Conf, before)
			requireRemainingNodeProtocolUnchanged(t, rn, beforeProtocol)
		})
	}
}

func TestValidProposeConfChangeSetsPending(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	before := rn.Status().Conf

	ref, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !rn.pendingConf {
		t.Fatal("valid ProposeConfChange did not set pendingConf")
	}
	assertConfState(t, rn.Status().Conf, before)
	if !rn.HasReady() {
		t.Fatal("valid ProposeConfChange produced no ready work")
	}
	rd := rn.Ready()
	if len(rd.Records) == 0 || rd.Records[0].Ref != ref || rd.Records[0].Command.Kind != CommandConfChange {
		t.Fatalf("valid ProposeConfChange ready records = %#v, want config change record for %s", rd.Records, ref)
	}
}

func TestProposeSkipsOccupiedLocalNextInstance(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	occupied := InstanceRef{Replica: rn.id, Instance: rn.nextInstance, Conf: rn.q.conf.ID}
	rn.instances[occupied] = &instance{rec: InstanceRecord{Ref: occupied}}

	first, err := rn.Propose(Command{ID: CommandID{Client: 51, Sequence: 1}, Payload: []byte("after-occupied"), ConflictKeys: [][]byte{[]byte("skip-local-instance")}})
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := InstanceRef{Replica: occupied.Replica, Instance: occupied.Instance + 1, Conf: occupied.Conf}
	if first != wantFirst {
		t.Fatalf("Propose with occupied local nextInstance returned %s, want %s", first, wantFirst)
	}

	second, err := rn.Propose(Command{ID: CommandID{Client: 51, Sequence: 2}, Payload: []byte("after-skip"), ConflictKeys: [][]byte{[]byte("skip-local-instance-2")}})
	if err != nil {
		t.Fatal(err)
	}
	wantSecond := InstanceRef{Replica: occupied.Replica, Instance: occupied.Instance + 2, Conf: occupied.Conf}
	if second != wantSecond {
		t.Fatalf("Propose after skipped occupied local instance returned %s, want %s", second, wantSecond)
	}
}

func TestProposalOwnershipCopiesCallerSlicesByDefault(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	payload := []byte("caller-payload")
	key := []byte("caller-key")
	if _, err := rn.Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: payload, ConflictKeys: [][]byte{key}}); err != nil {
		t.Fatal(err)
	}

	payload[0] = 'C'
	key[0] = 'C'

	rd := rn.Ready()
	if len(rd.Records) == 0 {
		t.Fatal("expected proposed record in Ready")
	}
	got := rd.Records[0].Command
	if !bytes.Equal(got.Payload, []byte("caller-payload")) {
		t.Fatalf("default proposal retained caller payload slice: got %q", got.Payload)
	}
	if len(got.ConflictKeys) != 1 || !bytes.Equal(got.ConflictKeys[0], []byte("caller-key")) {
		t.Fatalf("default proposal retained caller conflict key slice: got %q", got.ConflictKeys)
	}
}

func TestProposalOwnershipZeroCopyRetainsCallerSlices(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), ZeroCopyProposals: true})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	payload := []byte("caller-payload")
	key := []byte("caller-key")
	if _, err := rn.Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: payload, ConflictKeys: [][]byte{key}}); err != nil {
		t.Fatal(err)
	}

	payload[0] = 'C'
	key[0] = 'C'

	rd := rn.Ready()
	if len(rd.Records) == 0 {
		t.Fatal("expected proposed record in Ready")
	}
	got := rd.Records[0].Command
	if !bytes.Equal(got.Payload, []byte("Caller-payload")) {
		t.Fatalf("zero-copy proposal copied caller payload slice: got %q", got.Payload)
	}
	if len(got.ConflictKeys) != 1 || !bytes.Equal(got.ConflictKeys[0], []byte("Caller-key")) {
		t.Fatalf("zero-copy proposal copied caller conflict key slice: got %q", got.ConflictKeys)
	}
}

func TestInboundStepCopiesDecodedCommandDespiteZeroCopyProposals(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), ZeroCopyProposals: true})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	wantPayload := []byte("inbound-payload-unique")
	wantKey := []byte("inbound-conflict-key-unique")
	msg := Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{ID: CommandID{Client: 7, Sequence: 11}, Payload: wantPayload, ConflictKeys: [][]byte{wantKey}},
	}
	buf, err := EncodeMessage(make([]byte, 0, 128), msg)
	if err != nil {
		t.Fatal(err)
	}
	var inbound Message
	if err := DecodeMessage(buf, &inbound); err != nil {
		t.Fatal(err)
	}
	if err := rn.Step(inbound); err != nil {
		t.Fatal(err)
	}

	payloadOffset := bytes.Index(buf, wantPayload)
	if payloadOffset < 0 {
		t.Fatal("encoded payload bytes not found")
	}
	for i := range wantPayload {
		buf[payloadOffset+i] = 'x'
	}
	keyOffset := bytes.Index(buf, wantKey)
	if keyOffset < 0 {
		t.Fatal("encoded conflict key bytes not found")
	}
	for i := range wantKey {
		buf[keyOffset+i] = 'y'
	}

	rd := rn.Ready()
	if len(rd.Records) != 1 {
		t.Fatalf("ready records = %#v, want one inbound pre-accept record", rd.Records)
	}
	got := rd.Records[0].Command
	if !bytes.Equal(got.Payload, wantPayload) {
		t.Fatalf("inbound step retained decoded payload buffer: got %q want %q", got.Payload, wantPayload)
	}
	if len(got.ConflictKeys) != 1 || !bytes.Equal(got.ConflictKeys[0], wantKey) {
		t.Fatalf("inbound step retained decoded conflict key buffer: got %q want %q", got.ConflictKeys, wantKey)
	}
}

func TestMaxReadyMessagesCapsOnlyMessages(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), MaxReadyMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	second := InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	rn.pendingReady = Ready{
		Records: []InstanceRecord{
			{Ref: first, Command: Command{Payload: []byte("record-1")}},
			{Ref: second, Command: Command{Payload: []byte("record-2")}},
		},
		Messages: []Message{
			{Type: MsgPreAccept, From: 1, To: 2, Ref: first, Deps: []InstanceNum{0, 0, 0}},
			{Type: MsgPreAccept, From: 1, To: 3, Ref: second, Deps: []InstanceNum{0, 0, 0}},
		},
		Committed: []CommittedCommand{
			{Ref: first, Command: Command{Payload: []byte("commit-1")}},
			{Ref: second, Command: Command{Payload: []byte("commit-2")}},
		},
		MustSync: true,
	}

	rd := rn.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].To != 2 {
		t.Fatalf("first ready messages = %#v, want only the first capped message", rd.Messages)
	}
	if len(rd.Records) != 2 || rd.Records[0].Ref != first || rd.Records[1].Ref != second {
		t.Fatalf("records were capped with messages: %#v", rd.Records)
	}
	if len(rd.Committed) != 2 || rd.Committed[0].Ref != first || rd.Committed[1].Ref != second {
		t.Fatalf("committed commands were capped with messages: %#v", rd.Committed)
	}

	advanceOK(t, rn, rd)
	tail := rn.Ready()
	if len(tail.Records) != 0 || len(tail.Committed) != 0 {
		t.Fatalf("advanced records/committed reappeared with message tail: records=%#v committed=%#v", tail.Records, tail.Committed)
	}
	if len(tail.Messages) != 1 || tail.Messages[0].To != 3 || tail.Messages[0].Ref != second {
		t.Fatalf("message tail ready = %#v, want the unsent second message", tail.Messages)
	}
}

func TestTimeOptimizationDelaysSlowAcceptUntilFastWaitTick(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(6), Storage: store, RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref, err := rn.Propose(Command{ID: CommandID{Client: 7, Sequence: 1}, Payload: []byte("value"), ConflictKeys: [][]byte{[]byte("key")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Messages) != 5 {
		t.Fatalf("initial preaccept messages = %#v", rd.Messages)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local instance %s", ref)
	}
	for _, from := range []ReplicaID{2, 3, 4} {
		resp := Message{Type: MsgPreAcceptResp, From: from,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    inst.rec.Seq,
			Deps:   append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true}
		if err := rn.Step(resp); err != nil {
			t.Fatalf("step preaccept response from %d: %v", from, err)
		}
	}
	if got, slow, fast := inst.preOK.len(), rn.q.slowQuorum(), rn.q.fastQuorum(); got != slow || got >= fast {
		t.Fatalf("preaccept votes = %d, want slow quorum %d below fast quorum %d", got, slow, fast)
	}
	if inst.phase != phasePreAccept {
		t.Fatalf("phase after slow quorum preaccept responses = %d, want preaccept", inst.phase)
	}
	if rn.HasReady() {
		t.Fatalf("slow quorum response produced ready work before fast-wait deadline: %#v", rn.Ready())
	}

	for tick := uint64(1); tick < 3; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phasePreAccept {
			t.Fatalf("phase after tick %d = %d, want preaccept before fast-wait deadline", tick, inst.phase)
		}
		remainingApplyHardStateOnly(t, rn, tick)
	}

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept {
		t.Fatalf("phase at fast-wait deadline = %d, want accept", inst.phase)
	}
	rd = rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusAccepted {
		t.Fatalf("fast-wait ready records = %#v, want accepted record for %s", rd.Records, ref)
	}
	if len(rd.Messages) != 5 {
		t.Fatalf("fast-wait accept messages = %#v", rd.Messages)
	}
	seen := make(map[ReplicaID]bool, 5)
	for _, msg := range rd.Messages {
		if msg.Type != MsgAccept ||
			msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot != (Ballot{}) ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != inst.rec.Seq ||
			!instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
			!commandEqual(msg.Command, inst.rec.Command) {
			t.Fatalf("fast-wait message = %#v, want canonical accept for %s from replica 1", msg, ref)
		}
		seen[msg.To] = true
	}
	for _, to := range []ReplicaID{2, 3, 4, 5, 6} {
		if !seen[to] {
			t.Fatalf("missing accept message to replica %d in %#v", to, rd.Messages)
		}
	}
}

func TestTimeOptimizationRetryCannotBypassFastWaitDeadline(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(6), Storage: store, RetryTicks: 1, TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref, err := rn.Propose(Command{ID: CommandID{Client: 70, Sequence: 1}, Payload: []byte("value"), ConflictKeys: [][]byte{[]byte("retry-fast-wait-key")}})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, initial)

	inst := rn.instances[ref]
	for _, from := range []ReplicaID{2, 3, 4} {
		if err := rn.Step(Message{Type: MsgPreAcceptResp, From: from,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    inst.rec.Seq,
			Deps:   append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true}); err != nil {
			t.Fatalf("step preaccept response from %d: %v", from, err)
		}
	}

	for tick := uint64(1); tick < 3; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phasePreAccept {
			t.Fatalf("phase after retry tick %d = %d, want preaccept", tick, inst.phase)
		}
		retry := rn.Ready()
		optimizedRequireNoMessageType(t, retry.Messages, MsgAccept)
		if len(retry.Messages) != 5 {
			t.Fatalf("retry tick %d messages = %#v, want five PreAccept retries", tick, retry.Messages)
		}
		if err := store.ApplyReady(retry); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, retry)
	}

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept {
		t.Fatalf("phase at fast-wait deadline = %d, want accept", inst.phase)
	}
	atDeadline := rn.Ready()
	msg := optimizedRequireMessage(t, atDeadline.Messages, MsgAccept, 2)
	if msg.From != 1 ||
		msg.Ref != ref ||
		msg.Ballot != inst.rec.Ballot ||
		msg.RecordBallot != (Ballot{}) ||
		msg.RecordStatus != StatusNone ||
		msg.Seq != inst.rec.Seq ||
		!instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
		!commandEqual(msg.Command, inst.rec.Command) {
		t.Fatalf("fast-wait deadline message = %#v, want canonical accept for %s", msg, ref)
	}
}

func TestTimeOptimizationLateFastQuorumCommitsWithoutAccept(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(6), Storage: store, RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	cmd := Command{ID: CommandID{Client: 8, Sequence: 1}, Payload: []byte("late-fast-value"), ConflictKeys: [][]byte{[]byte("late-fast-key")}}
	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Messages) != 5 {
		t.Fatalf("initial preaccept messages = %#v", rd.Messages)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local instance %s", ref)
	}
	stepMatchingPreAcceptResp := func(from ReplicaID) {
		t.Helper()
		resp := Message{Type: MsgPreAcceptResp, From: from,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    inst.rec.Seq,
			Deps:   append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true}
		if err := rn.Step(resp); err != nil {
			t.Fatalf("step matching preaccept response from %d: %v", from, err)
		}
	}

	for _, from := range []ReplicaID{2, 3, 4} {
		stepMatchingPreAcceptResp(from)
	}
	if got, slow, fast := inst.preOK.len(), rn.q.slowQuorum(), rn.q.fastQuorum(); got != slow || got >= fast {
		t.Fatalf("preaccept votes = %d, want slow quorum %d below fast quorum %d", got, slow, fast)
	}
	if inst.phase != phasePreAccept {
		t.Fatalf("phase after slow quorum preaccept responses = %d, want preaccept", inst.phase)
	}
	if rn.HasReady() {
		t.Fatalf("slow quorum response produced ready work before fast-wait deadline: %#v", rn.Ready())
	}

	for tick := uint64(1); tick < 3; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phasePreAccept {
			t.Fatalf("phase after tick %d = %d, want preaccept before fast-wait deadline", tick, inst.phase)
		}
		remainingApplyHardStateOnly(t, rn, tick)
	}

	stepMatchingPreAcceptResp(5)
	if inst.preOK != nil {
		t.Fatalf("committed instance retained PreAccept votes: %#v", inst.preOK)
	}
	if inst.phase != phaseCommitted {
		t.Fatalf("phase after late fast-quorum response = %d, want committed", inst.phase)
	}
	if inst.rec.Status != StatusExecuted {
		t.Fatalf("instance status after dependency-ready commit = %s, want executed", inst.rec.Status)
	}

	rd = rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusCommitted {
		t.Fatalf("late fast-quorum ready records = %#v, want committed record for %s", rd.Records, ref)
	}
	if len(rd.Messages) != 5 {
		t.Fatalf("late fast-quorum commit messages = %#v", rd.Messages)
	}
	seen := make(map[ReplicaID]bool, 5)
	for _, msg := range rd.Messages {
		if msg.Type == MsgAccept {
			t.Fatalf("late fast-quorum response emitted accept message: %#v", msg)
		}
		if msg.Type != MsgCommit ||
			msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot == (Ballot{}) ||
			msg.RecordBallot != inst.rec.RecordBallot ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != inst.rec.Seq ||
			!instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
			!commandEqual(msg.Command, inst.rec.Command) {
			t.Fatalf("late fast-quorum message = %#v, want canonical commit for %s from replica 1", msg, ref)
		}
		seen[msg.To] = true
	}
	for _, to := range []ReplicaID{2, 3, 4, 5, 6} {
		if !seen[to] {
			t.Fatalf("missing commit message to replica %d in %#v", to, rd.Messages)
		}
	}
	if len(rd.Committed) != 1 {
		t.Fatalf("late fast-quorum committed commands = %#v, want one command", rd.Committed)
	}
	committed := requireCommittedForRef(t, rd, ref)
	if committed.Command.Kind != CommandUser || committed.Command.ID != cmd.ID || !bytes.Equal(committed.Command.Payload, cmd.Payload) {
		t.Fatalf("committed command = %#v, want user command %#v", committed.Command, cmd)
	}
	if len(committed.Command.ConflictKeys) != 1 || !bytes.Equal(committed.Command.ConflictKeys[0], cmd.ConflictKeys[0]) {
		t.Fatalf("committed conflict keys = %#v, want %#v", committed.Command.ConflictKeys, cmd.ConflictKeys)
	}
}

func TestFiveNodeOptimizedFastPathCommitsAtThreePreAcceptVotes(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(5), Storage: store, RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	if slow, fast := rn.q.slowQuorum(), rn.q.fastQuorum(); slow != 3 || fast != 3 {
		t.Fatalf("five-node optimized quorum slow=%d fast=%d, want both 3", slow, fast)
	}
	cmd := Command{ID: CommandID{Client: 80, Sequence: 1}, Payload: []byte("optimized-five-fast"), ConflictKeys: [][]byte{[]byte("optimized-five-fast-key")}}
	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Messages) != 4 {
		t.Fatalf("initial five-node preaccept messages = %#v", rd.Messages)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local instance %s", ref)
	}
	stepMatchingPreAcceptResp := func(from ReplicaID) {
		t.Helper()
		if err := rn.Step(Message{Type: MsgPreAcceptResp, From: from,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    inst.rec.Seq,
			Deps:   append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true}); err != nil {
			t.Fatalf("step matching five-node preaccept response from %d: %v", from, err)
		}
	}

	stepMatchingPreAcceptResp(2)
	if got := inst.preOK.len(); got != 2 {
		t.Fatalf("preaccept votes one below optimized quorum = %d, want 2", got)
	}
	if inst.phase != phasePreAccept {
		t.Fatalf("phase one below optimized fast quorum = %d, want preaccept", inst.phase)
	}
	if rn.HasReady() {
		t.Fatalf("one below optimized fast quorum emitted ready work: %#v", rn.Ready())
	}

	stepMatchingPreAcceptResp(3)
	if inst.preOK != nil {
		t.Fatalf("optimized fast commit retained PreAccept votes: %#v", inst.preOK)
	}
	if inst.phase != phaseCommitted {
		t.Fatalf("phase at optimized five-node fast quorum = %d, want committed", inst.phase)
	}
	if inst.rec.Status != StatusExecuted {
		t.Fatalf("instance status after optimized five-node fast commit = %s, want executed", inst.rec.Status)
	}
	rd = rn.Ready()
	if rec := optimizedRequireRecord(t, rd, ref); rec.Status != StatusCommitted {
		t.Fatalf("optimized five-node fast record = %#v, want committed", rec)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	for _, to := range []ReplicaID{2, 3, 4, 5} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		if msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot == (Ballot{}) ||
			msg.RecordBallot != inst.rec.RecordBallot ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != inst.rec.Seq ||
			!instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
			!commandEqual(msg.Command, cmd) {
			t.Fatalf("optimized five-node fast commit to %d = %#v, want canonical committed tuple", to, msg)
		}
	}
	committed := requireCommittedForRef(t, rd, ref)
	if committed.Command.Kind != CommandUser || committed.Command.ID != cmd.ID || !bytes.Equal(committed.Command.Payload, cmd.Payload) {
		t.Fatalf("optimized five-node committed command = %#v, want %#v", committed.Command, cmd)
	}
}

func TestFiveNodeOptimizedDivergentPreAcceptFallsBackToAcceptAtFastThreshold(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(5), Storage: store, RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	if slow, fast := rn.q.slowQuorum(), rn.q.fastQuorum(); slow != 3 || fast != 3 {
		t.Fatalf("five-node optimized quorum slow=%d fast=%d, want both 3", slow, fast)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 81, Sequence: 1}, Payload: []byte("optimized-five-divergent"), ConflictKeys: [][]byte{[]byte("optimized-five-divergent-key")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local instance %s", ref)
	}
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 2,
		To:     1,
		Ref:    ref,
		Ballot: inst.rec.Ballot,
		Seq:    inst.rec.Seq,
		Deps:   append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true}); err != nil {
		t.Fatalf("step matching five-node preaccept response: %v", err)
	}
	divergentDeps := append([]InstanceNum(nil), inst.rec.Deps...)
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 3,
		To:     1,
		Ref:    ref,
		Ballot: inst.rec.Ballot,
		Seq:    inst.rec.Seq + 1,
		Deps:   divergentDeps}); err != nil {
		t.Fatalf("step divergent five-node preaccept response: %v", err)
	}

	if inst.preOK != nil {
		t.Fatalf("divergent optimized Accept retained PreAccept votes: %#v", inst.preOK)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("phase/status after divergent optimized quorum = %d/%s, want accept/accepted", inst.phase, inst.rec.Status)
	}
	rd = rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusAccepted || rec.FastPathEligible || rec.Seq != 2 {
		t.Fatalf("divergent optimized quorum record = %#v, want non-fast accepted seq 2", rec)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgCommit)
	for _, to := range []ReplicaID{2, 3, 4, 5} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
		if msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != rec.Ballot ||
			msg.RecordBallot != (Ballot{}) ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != 2 ||
			!instanceNumsEqual(msg.Deps, rec.Deps) ||
			!commandEqual(msg.Command, rec.Command) {
			t.Fatalf("divergent optimized quorum accept to %d = %#v, want canonical accepted tuple", to, msg)
		}
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("divergent optimized quorum emitted committed commands: %#v", rd.Committed)
	}
}

func TestFiveNodeFastPathRequiresDepsCommittedEvidenceForDependency(t *testing.T) {
	rn, inst, ref := proposeFiveNodeCommandWithDependency(t, InstanceRef{Replica: 2, Instance: 1, Conf: 1}, StatusPreAccepted, nil)
	attrs := inst.rec.Attributes()

	stepFiveNodeMatchingPreAcceptResp(t, rn, inst, ref, 2, 0)
	if got := inst.preOK.len(); got != 2 {
		t.Fatalf("preaccept votes one below evidence-gated fast quorum = %d, want 2", got)
	}
	if rn.HasReady() {
		t.Fatalf("one matching response without dependency evidence emitted ready work: %#v", rn.Ready())
	}

	stepFiveNodeMatchingPreAcceptResp(t, rn, inst, ref, 3, 0)
	if inst.preOK != nil {
		t.Fatalf("evidence-gated slow Accept retained PreAccept votes: %#v", inst.preOK)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("phase/status without dependency evidence = %d/%s, want slow-path accept/accepted", inst.phase, inst.rec.Status)
	}
	requireFiveNodeSlowAccept(t, rn.Ready(), ref, attrs)
}

func TestFiveNodeFastPathCommitsWithMatchingDepsCommittedEvidence(t *testing.T) {
	rn, inst, ref := proposeFiveNodeCommandWithDependency(t, InstanceRef{Replica: 2, Instance: 1, Conf: 1}, StatusPreAccepted, nil)
	dependencyBit := uint64(1) << 1

	stepFiveNodeMatchingPreAcceptResp(t, rn, inst, ref, 2, dependencyBit)
	stepFiveNodeMatchingPreAcceptResp(t, rn, inst, ref, 3, 0)

	if inst.preOK != nil {
		t.Fatalf("evidence-gated fast commit retained PreAccept votes: %#v", inst.preOK)
	}
	if inst.phase != phaseCommitted || inst.rec.Status != StatusCommitted {
		t.Fatalf("phase/status with matching dependency evidence = %d/%s, want committed without execution", inst.phase, inst.rec.Status)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusCommitted || !instanceNumsEqual(rec.Deps, []InstanceNum{0, 1, 0, 0, 0}) {
		t.Fatalf("evidence-gated fast commit record = %#v, want committed with dependency on replica 2 instance 1", rec)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	for _, to := range []ReplicaID{2, 3, 4, 5} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		if msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != rec.Ballot ||
			msg.RecordBallot == (Ballot{}) ||
			msg.RecordBallot != rec.RecordBallot ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != rec.Seq ||
			!instanceNumsEqual(msg.Deps, rec.Deps) ||
			!commandEqual(msg.Command, rec.Command) {
			t.Fatalf("evidence-gated commit message to %d = %#v, want canonical committed tuple deps %v", to, msg, rec.Deps)
		}
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("fast commit with locally unresolved dependency emitted application commands: %#v", rd.Committed)
	}
}

func TestDepsCommittedPrefixEvidenceRequiresContiguousCommittedPrefix(t *testing.T) {
	tests := []struct {
		name      string
		seedHole  func(*testing.T, *RawNode, []byte)
		wantCause string
	}{
		{
			name:      "missing first instance",
			wantCause: "missing 2.1",
		},
		{
			name: "uncommitted first instance",
			seedHole: func(t *testing.T, rn *RawNode, key []byte) {
				t.Helper()
				seedDependencyRecord(t, rn, InstanceRef{Replica: 2, Instance: 1, Conf: 1}, StatusPreAccepted, key)
			},
			wantCause: "uncommitted 2.1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte("deps-committed-prefix-hole-" + tc.name)
			follower := newDepsCommittedFiveNode(t, 3)
			if tc.seedHole != nil {
				tc.seedHole(t, follower, key)
			}
			seedDependencyRecord(t, follower, InstanceRef{Replica: 2, Instance: 2, Conf: 1}, StatusCommitted, key)

			ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
			if err := follower.Step(Message{
				Type:    MsgPreAccept,
				From:    1,
				To:      3,
				Ref:     ref,
				Ballot:  Ballot{Replica: 1},
				Seq:     1,
				Deps:    follower.q.deps(),
				Command: dependencyEvidenceCommand(100, "prefix-target", key),
			}); err != nil {
				t.Fatalf("Step(preaccept with %s prefix hole) err=%v", tc.wantCause, err)
			}
			resp := optimizedRequireMessage(t, follower.Ready().Messages, MsgPreAcceptResp, 1)
			if !instanceNumsEqual(resp.Deps, []InstanceNum{0, 2, 0, 0, 0}) {
				t.Fatalf("prefix-hole response deps = %v, want compact dependency through replica 2 instance 2", resp.Deps)
			}
			if resp.DepsCommitted&(uint64(1)<<1) != 0 {
				t.Fatalf("prefix-hole response DepsCommitted=%05b, want replica 2 bit clear because %s is not committed", resp.DepsCommitted, tc.wantCause)
			}

			leader, inst, proposed := proposeFiveNodeCommandWithDependency(t, InstanceRef{Replica: 2, Instance: 2, Conf: 1}, StatusCommitted, tc.seedHole)
			if vote, ok := inst.preOK.get(leader.q.conf, 1); !ok || vote.depsCommitted&(uint64(1)<<1) != 0 {
				got := uint64(0)
				if ok {
					got = vote.depsCommitted
				}
				t.Fatalf("originator DepsCommitted=%05b, want replica 2 bit clear because %s is not committed", got, tc.wantCause)
			}
			attrs := inst.rec.Attributes()
			stepFiveNodeMatchingPreAcceptResp(t, leader, inst, proposed, 2, resp.DepsCommitted)
			stepFiveNodeMatchingPreAcceptResp(t, leader, inst, proposed, 3, 0)
			if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
				t.Fatalf("phase/status with prefix-hole evidence = %d/%s, want slow-path accept/accepted", inst.phase, inst.rec.Status)
			}
			requireFiveNodeSlowAccept(t, leader.Ready(), proposed, attrs)
		})
	}
}

func proposeFiveNodeCommandWithDependency(t *testing.T, dep InstanceRef, depStatus Status, seedPrefixHole func(*testing.T, *RawNode, []byte)) (*RawNode, *instance, InstanceRef) {
	t.Helper()
	key := []byte("deps-committed-evidence-key")
	rn := newDepsCommittedFiveNode(t, 1)
	if seedPrefixHole != nil {
		seedPrefixHole(t, rn, key)
	}
	seedDependencyRecord(t, rn, dep, depStatus, key)
	ref, err := rn.Propose(dependencyEvidenceCommand(200, "dependent", key))
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Messages) != 4 {
		t.Fatalf("initial five-node dependency preaccept messages = %#v", rd.Messages)
	}
	advanceOK(t, rn, rd)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing proposed instance %s", ref)
	}
	wantDeps := []InstanceNum{0, dep.Instance, 0, 0, 0}
	if !instanceNumsEqual(inst.rec.Deps, wantDeps) {
		t.Fatalf("proposed deps = %v, want dependency vector %v", inst.rec.Deps, wantDeps)
	}
	return rn, inst, ref
}

func newDepsCommittedFiveNode(t *testing.T, id ReplicaID) *RawNode {
	t.Helper()
	rn, err := NewRawNode(Config{ID: id, Voters: makeIDs(5), RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	if slow, fast := rn.q.slowQuorum(), rn.q.fastQuorum(); slow != 3 || fast != 3 {
		t.Fatalf("five-node optimized quorum slow=%d fast=%d, want both 3", slow, fast)
	}
	return rn
}

func seedDependencyRecord(t *testing.T, rn *RawNode, ref InstanceRef, status Status, key []byte) {
	t.Helper()
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: ref.Replica},
		Status:  status,
		Seq:     uint64(ref.Instance),
		Deps:    rn.q.deps(),
		Command: dependencyEvidenceCommand(uint64(ref.Replica)*10+uint64(ref.Instance), "dependency", key),
	})
}

func dependencyEvidenceCommand(sequence uint64, payload string, key []byte) Command {
	return Command{
		ID:           CommandID{Client: 90, Sequence: sequence},
		Payload:      []byte(payload),
		ConflictKeys: [][]byte{append([]byte(nil), key...)},
	}
}

func stepFiveNodeMatchingPreAcceptResp(t *testing.T, rn *RawNode, inst *instance, ref InstanceRef, from ReplicaID, depsCommitted uint64) {
	t.Helper()
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: from,
		To:     1,
		Ref:    ref,
		Ballot: inst.rec.Ballot,
		Seq:    inst.rec.Seq,
		Deps:   append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true,
		DepsCommitted: depsCommitted}); err != nil {
		t.Fatalf("step matching dependency-evidence preaccept response from %d: %v", from, err)
	}
}

func requireFiveNodeSlowAccept(t *testing.T, rd Ready, ref InstanceRef, attrs Attributes) {
	t.Helper()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusAccepted || rec.FastPathEligible || rec.Seq != attrs.Seq || !instanceNumsEqual(rec.Deps, attrs.Deps) {
		t.Fatalf("dependency-evidence slow accept record = %#v, want non-fast accepted attrs seq %d deps %v", rec, attrs.Seq, attrs.Deps)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgCommit)
	for _, to := range []ReplicaID{2, 3, 4, 5} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
		if msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != rec.Ballot ||
			msg.RecordBallot != (Ballot{}) ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != attrs.Seq ||
			!instanceNumsEqual(msg.Deps, attrs.Deps) ||
			!commandEqual(msg.Command, rec.Command) {
			t.Fatalf("dependency-evidence accept message to %d = %#v, want canonical accepted attrs seq %d deps %v", to, msg, attrs.Seq, attrs.Deps)
		}
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("dependency-evidence slow accept emitted committed commands: %#v", rd.Committed)
	}
}

func TestOutboundPreAcceptProcessAtCarriesCreatedTickOnRetries(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, TimeOptimization: true, TimeOptimizationTicks: 5})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref, err := rn.Propose(Command{ID: CommandID{Client: 21, Sequence: 1}, Payload: []byte("process-at-local"), ConflictKeys: [][]byte{[]byte("process-at-local-key")}})
	if err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusPreAccepted {
		t.Fatalf("initial ready records = %#v, want pre-accepted local record for %s", rd.Records, ref)
	}
	requirePreAcceptProcessAt(t, rd.Messages, ref, rn.instances[ref].rec, 5, []ReplicaID{2, 3})
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 1)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	retry := rn.Ready()
	if len(retry.Records) != 0 || len(retry.Committed) != 0 {
		t.Fatalf("preaccept retry changed durable/application work: records=%#v committed=%#v", retry.Records, retry.Committed)
	}
	requirePreAcceptProcessAt(t, retry.Messages, ref, rn.instances[ref].rec, 5, []ReplicaID{2, 3})
}

func TestInboundFuturePreAcceptTimingWaitsForProcessAt(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	msg := processAtPreAccept(ref, 1, 2, 3, 3, Command{ID: CommandID{Client: 22, Sequence: 1}, Payload: []byte("delayed"), ConflictKeys: [][]byte{[]byte("delayed-key")}})

	if err := rn.Step(msg); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatalf("future preaccept produced ready work before ProcessAt: %#v", rn.Ready())
	}
	for tick := uint64(1); tick < msg.ProcessAt; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		remainingApplyHardStateOnly(t, rn, tick)
	}

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusPreAccepted {
		t.Fatalf("due preaccept records = %#v, want one pre-accepted record for %s", rd.Records, ref)
	}
	if !bytes.Equal(rd.Records[0].Command.Payload, msg.Command.Payload) {
		t.Fatalf("due preaccept payload = %q, want %q", rd.Records[0].Command.Payload, msg.Command.Payload)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgPreAcceptResp || rd.Messages[0].From != 2 || rd.Messages[0].To != 1 || rd.Messages[0].Ref != ref {
		t.Fatalf("due preaccept response = %#v, want one response to replica 1 for %s", rd.Messages, ref)
	}
}

func TestInboundFutureProcessAtPreAcceptWithNonDefaultBallotProcessesImmediately(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	msg := processAtPreAccept(ref, 1, 2, 9, 3, Command{ID: CommandID{Client: 23, Sequence: 1}, Payload: []byte("non-default-ballot"), ConflictKeys: [][]byte{[]byte("non-default-ballot-key")}})
	msg.Ballot = Ballot{Number: 1, Replica: 1}

	if err := rn.Step(msg); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusPreAccepted {
		t.Fatalf("non-default ballot future preaccept records = %#v, want immediate pre-accepted record for %s", rd.Records, ref)
	}
	if rd.Records[0].Ballot != msg.Ballot || rd.Records[0].FastPathEligible {
		t.Fatalf("non-default ballot record = %#v, want ballot %v and ineligible fast path", rd.Records[0], msg.Ballot)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgPreAcceptResp || rd.Messages[0].Ref != ref || rd.Messages[0].To != 1 {
		t.Fatalf("non-default ballot future preaccept response = %#v, want immediate response to originator", rd.Messages)
	}
	if !bytes.Equal(rd.Records[0].Command.Payload, msg.Command.Payload) {
		t.Fatalf("immediate preaccept payload = %q, want %q", rd.Records[0].Command.Payload, msg.Command.Payload)
	}
}

func TestPreAcceptRejectWithNonGreaterHintIsIgnored(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref, err := rn.Propose(Command{ID: CommandID{Client: 24, Sequence: 1}, Payload: []byte("stale-reject"), ConflictKeys: [][]byte{[]byte("stale-reject-key")}})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	advanceOK(t, rn, initial)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local instance %s", ref)
	}
	ballot := inst.rec.Ballot
	if err := rn.Step(Message{
		Type:         MsgPreAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       ballot,
		Deps:         rn.q.deps(),
		Reject:       true,
		RejectReason: RejectNone,
		RejectHint:   ballot,
	}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phasePreAccept || inst.rec.Ballot != ballot {
		t.Fatalf("non-greater preaccept reject changed phase/ballot to %d/%v, want preaccept/%v", inst.phase, inst.rec.Ballot, ballot)
	}
	if rn.HasReady() {
		t.Fatalf("ignored preaccept reject produced ready work: %#v", rn.Ready())
	}
}

func TestDuePreAcceptTimingOrdersByProcessAtAndRef(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 4, Voters: makeIDs(4), TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	sharedKey := []byte("deterministic-delay-key")
	first := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	second := InstanceRef{Replica: 2, Instance: 2, Conf: 1}
	later := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	messages := []Message{
		processAtPreAccept(second, 2, 4, 2, 4, Command{ID: CommandID{Client: 31, Sequence: 2}, Payload: []byte("arrived-first-but-ref-second"), ConflictKeys: [][]byte{sharedKey}}),
		processAtPreAccept(later, 3, 4, 3, 4, Command{ID: CommandID{Client: 31, Sequence: 3}, Payload: []byte("later-process-at"), ConflictKeys: [][]byte{sharedKey}}),
		processAtPreAccept(first, 1, 4, 2, 4, Command{ID: CommandID{Client: 31, Sequence: 1}, Payload: []byte("arrived-last-but-ref-first"), ConflictKeys: [][]byte{sharedKey}}),
	}
	for _, msg := range messages {
		if err := rn.Step(msg); err != nil {
			t.Fatalf("Step(%s %s) err=%v", msg.Type, msg.Ref, err)
		}
	}
	if rn.HasReady() {
		t.Fatalf("queued future preaccepts produced ready before due tick: %#v", rn.Ready())
	}
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 1)

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if got, want := recordRefs(rd.Records), []InstanceRef{first, second}; !slices.Equal(got, want) {
		t.Fatalf("records due at same ProcessAt arrived in order %v, want deterministic ref order %v; records=%#v", got, want, rd.Records)
	}
	if !instanceNumsEqual(rd.Records[0].Deps, []InstanceNum{0, 0, 0, 0}) || rd.Records[0].Seq != 1 {
		t.Fatalf("first same-tick record attrs = seq %d deps %v, want no conflicting predecessor", rd.Records[0].Seq, rd.Records[0].Deps)
	}
	if !instanceNumsEqual(rd.Records[1].Deps, []InstanceNum{1, 0, 0, 0}) || rd.Records[1].Seq != 2 {
		t.Fatalf("second same-tick record attrs = seq %d deps %v, want dependency on %s", rd.Records[1].Seq, rd.Records[1].Deps, first)
	}
	advanceOK(t, rn, rd)

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	laterReady := rn.Ready()
	if got, want := recordRefs(laterReady.Records), []InstanceRef{later}; !slices.Equal(got, want) {
		t.Fatalf("later ProcessAt records = %v, want only %s; records=%#v", got, later, laterReady.Records)
	}
	if !instanceNumsEqual(laterReady.Records[0].Deps, []InstanceNum{1, 2, 0, 0}) || laterReady.Records[0].Seq != 3 {
		t.Fatalf("later record attrs = seq %d deps %v, want dependencies on %s and %s", laterReady.Records[0].Seq, laterReady.Records[0].Deps, first, second)
	}
}

func TestDeferredPreAcceptTimingClonesCommandBuffers(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 2})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	payload := []byte("queued-payload")
	key := []byte("queued-key")
	msg := processAtPreAccept(InstanceRef{Replica: 1, Instance: 2, Conf: 1}, 1, 2, 2, 3, Command{
		ID:           CommandID{Client: 41, Sequence: 1},
		Payload:      payload,
		ConflictKeys: [][]byte{key},
	})
	if err := rn.Step(msg); err != nil {
		t.Fatal(err)
	}

	payload[0] = 'X'
	key[0] = 'Y'
	msg.Deps[0] = 99
	msg.Command.Payload[1] = 'Z'
	msg.Command.ConflictKeys[0][1] = 'W'

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 1)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 {
		t.Fatalf("deferred preaccept records = %#v, want one record", rd.Records)
	}
	got := rd.Records[0]
	if !bytes.Equal(got.Command.Payload, []byte("queued-payload")) {
		t.Fatalf("deferred preaccept retained caller payload buffer: got %q", got.Command.Payload)
	}
	if len(got.Command.ConflictKeys) != 1 || !bytes.Equal(got.Command.ConflictKeys[0], []byte("queued-key")) {
		t.Fatalf("deferred preaccept retained caller conflict-key buffer: got %q", got.Command.ConflictKeys)
	}
	if !instanceNumsEqual(got.Deps, []InstanceNum{0, 0, 0}) {
		t.Fatalf("deferred preaccept retained caller deps buffer: got %v", got.Deps)
	}
}

func TestPreAcceptTimingFastCommitsUnanimousRepliesWithStaleOriginator(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 5})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref, err := rn.Propose(Command{ID: CommandID{Client: 45, Sequence: 1}, Payload: []byte("stale-originator"), ConflictKeys: [][]byte{[]byte("stale-originator-key")}})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	requirePreAcceptProcessAt(t, initial.Messages, ref, rn.instances[ref].rec, 5, []ReplicaID{2, 3})
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, initial)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local instance %s", ref)
	}
	remoteAttrs := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 0, 0}}
	for _, from := range []ReplicaID{2, 3} {
		resp := Message{Type: MsgPreAcceptResp, From: from,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    remoteAttrs.Seq,
			Deps:   append([]InstanceNum(nil), remoteAttrs.Deps...), FastPathEligible: true}
		if err := rn.Step(resp); err != nil {
			t.Fatalf("Step(unanimous preaccept response from %d) err=%v", from, err)
		}
	}

	if inst.phase != phaseCommitted {
		t.Fatalf("phase after unanimous timed replies = %d, want committed without slow accept", inst.phase)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusCommitted {
		t.Fatalf("unanimous timed reply records = %#v, want committed record for %s", rd.Records, ref)
	}
	if rd.Records[0].Seq != remoteAttrs.Seq || !instanceNumsEqual(rd.Records[0].Deps, remoteAttrs.Deps) {
		t.Fatalf("committed attrs = seq %d deps %v, want unanimous reply attrs seq %d deps %v", rd.Records[0].Seq, rd.Records[0].Deps, remoteAttrs.Seq, remoteAttrs.Deps)
	}
	if len(rd.Messages) != 2 {
		t.Fatalf("unanimous timed reply messages = %#v, want commit broadcasts", rd.Messages)
	}
	commitRec := rd.Records[0]
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		if msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != commitRec.Ballot ||
			msg.RecordBallot == (Ballot{}) ||
			msg.RecordBallot != commitRec.RecordBallot ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != remoteAttrs.Seq ||
			!instanceNumsEqual(msg.Deps, remoteAttrs.Deps) ||
			!commandEqual(msg.Command, commitRec.Command) {
			t.Fatalf("unanimous timed reply message to %d = %#v, want canonical commit for %s", to, msg, ref)
		}
	}
}

func proposeTimedCommandWithPriorConflict(t *testing.T) (*RawNode, *instance, InstanceRef) {
	t.Helper()
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 5})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	key := []byte("timed-originator-dependency-key")
	conflictingRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:     conflictingRef,
		Ballot:  Ballot{Replica: 2},
		Status:  StatusCommitted,
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: Command{ID: CommandID{Client: 60, Sequence: 1}, Payload: []byte("prior-conflict"), ConflictKeys: [][]byte{key}},
	})
	rn.executed.add(conflictingRef)

	ref, err := rn.Propose(Command{ID: CommandID{Client: 62, Sequence: 1}, Payload: []byte("timed-originator"), ConflictKeys: [][]byte{key}})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	requirePreAcceptProcessAt(t, initial.Messages, ref, rn.instances[ref].rec, 5, []ReplicaID{2, 3})
	if err := store.ApplyReady(initial); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, initial)
	remoteOnlyRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:     remoteOnlyRef,
		Ballot:  Ballot{Replica: 3},
		Status:  StatusCommitted,
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: Command{ID: CommandID{Client: 61, Sequence: 1}, Payload: []byte("remote-only-dependency"), ConflictKeys: [][]byte{key}},
	})
	rn.executed.add(remoteOnlyRef)

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local instance %s", ref)
	}
	if !instanceNumsEqual(inst.rec.Deps, []InstanceNum{0, 1, 0}) {
		t.Fatalf("originator deps = %v, want dependency on replica 2 only", inst.rec.Deps)
	}
	return rn, inst, ref
}

func stepTimedPreAcceptResp(t *testing.T, rn *RawNode, inst *instance, ref InstanceRef, from ReplicaID, attrs Attributes) {
	t.Helper()
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: from,
		To:     1,
		Ref:    ref,
		Ballot: inst.rec.Ballot,
		Seq:    attrs.Seq,
		Deps:   append([]InstanceNum(nil), attrs.Deps...), FastPathEligible: true}); err != nil {
		t.Fatalf("Step(timed preaccept response from %d) err=%v", from, err)
	}
}

func requireTimedSlowAccept(t *testing.T, rd Ready, ref InstanceRef, attrs Attributes) {
	t.Helper()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusAccepted || rec.Seq != attrs.Seq || !instanceNumsEqual(rec.Deps, attrs.Deps) {
		t.Fatalf("timed slow-accept record = %#v, want accepted attrs seq %d deps %v", rec, attrs.Seq, attrs.Deps)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgCommit)
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
		if msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != rec.Ballot ||
			msg.RecordBallot != (Ballot{}) ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != attrs.Seq ||
			!instanceNumsEqual(msg.Deps, attrs.Deps) ||
			!commandEqual(msg.Command, rec.Command) {
			t.Fatalf("timed slow-accept message to %d = %#v, want canonical accepted attrs seq %d deps %v", to, msg, attrs.Seq, attrs.Deps)
		}
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("timed slow-accept emitted committed commands: %#v", rd.Committed)
	}
}

func TestPreAcceptTimingRejectsUnanimousRepliesThatDropOriginatorDependency(t *testing.T) {
	rn, inst, ref := proposeTimedCommandWithPriorConflict(t)
	droppingOriginatorDep := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 0, 0}}
	for _, from := range []ReplicaID{2, 3} {
		stepTimedPreAcceptResp(t, rn, inst, ref, from, droppingOriginatorDep)
	}

	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("phase/status after replies that drop originator dep = %d/%s, want slow-path accept/accepted", inst.phase, inst.rec.Status)
	}
	wantAttrs := Attributes{Seq: droppingOriginatorDep.Seq, Deps: []InstanceNum{0, 1, 0}}
	requireTimedSlowAccept(t, rn.Ready(), ref, wantAttrs)
}

func TestPreAcceptTimingFastCommitsStrictRemoteSuperset(t *testing.T) {
	rn, inst, ref := proposeTimedCommandWithPriorConflict(t)
	remoteSuperset := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 1, 1}}
	for _, from := range []ReplicaID{2, 3} {
		stepTimedPreAcceptResp(t, rn, inst, ref, from, remoteSuperset)
	}

	if inst.phase != phaseCommitted {
		t.Fatalf("phase after unanimous strict-superset timed replies = %d, want committed", inst.phase)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusCommitted || rec.Seq != remoteSuperset.Seq || !instanceNumsEqual(rec.Deps, remoteSuperset.Deps) {
		t.Fatalf("strict-superset commit record = %#v, want committed attrs seq %d deps %v", rec, remoteSuperset.Seq, remoteSuperset.Deps)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		if msg.From != 1 ||
			msg.Ref != ref ||
			msg.Ballot != rec.Ballot ||
			msg.RecordBallot == (Ballot{}) ||
			msg.RecordBallot != rec.RecordBallot ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != remoteSuperset.Seq ||
			!instanceNumsEqual(msg.Deps, remoteSuperset.Deps) ||
			!commandEqual(msg.Command, rec.Command) {
			t.Fatalf("strict-superset commit message to %d = %#v, want canonical committed attrs seq %d deps %v", to, msg, remoteSuperset.Seq, remoteSuperset.Deps)
		}
	}
}

func TestPreAcceptTimingUsesAnyPaperFastQuorumWithCoveringTuple(t *testing.T) {
	rn, inst, ref := proposeTimedCommandWithPriorConflict(t)
	remoteSuperset := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 1, 1}}
	remoteOriginatorOnly := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 1, 0}}
	stepTimedPreAcceptResp(t, rn, inst, ref, 2, remoteSuperset)
	stepTimedPreAcceptResp(t, rn, inst, ref, 3, remoteOriginatorOnly)

	if inst.phase != phaseCommitted || inst.rec.Status < StatusCommitted {
		t.Fatalf("phase/status after one covering paper fast quorum = %d/%s, want committed", inst.phase, inst.rec.Status)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusCommitted || !rec.Attributes().Equal(remoteSuperset) {
		t.Fatalf("paper-quorum fast commit record = %#v, want committed attrs %#v", rec, remoteSuperset)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
}

func TestPreAcceptFastCommitRejectsNonDefaultBallot(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		mut  func(*instance)
	}{
		{
			name: "non-default ballot",
			cfg:  Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 5},
			mut: func(inst *instance) {
				inst.rec.Ballot = Ballot{Number: 1, Replica: 1}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rn, err := NewRawNode(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			remainingApplyHardStateOnly(t, rn, 0)
			ref, err := rn.Propose(Command{ID: CommandID{Client: 46, Sequence: 1}, Payload: []byte("guarded-stale-originator"), ConflictKeys: [][]byte{[]byte("guarded-stale-originator-key")}})
			if err != nil {
				t.Fatal(err)
			}
			initial := rn.Ready()
			advanceOK(t, rn, initial)

			inst := rn.instances[ref]
			if inst == nil {
				t.Fatalf("missing local instance %s", ref)
			}
			if tc.mut != nil {
				tc.mut(inst)
			}
			remoteAttrs := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 0, 0}}
			for _, from := range []ReplicaID{2, 3} {
				resp := Message{Type: MsgPreAcceptResp, From: from,
					To:     1,
					Ref:    ref,
					Ballot: inst.rec.Ballot,
					Seq:    remoteAttrs.Seq,
					Deps:   append([]InstanceNum(nil), remoteAttrs.Deps...), FastPathEligible: true}
				if err := rn.Step(resp); err != nil {
					t.Fatalf("Step(stale-originator guarded response from %d) err=%v", from, err)
				}
			}

			if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
				t.Fatalf("guarded stale-originator replies phase/status = %d/%s, want slow-path accept/accepted", inst.phase, inst.rec.Status)
			}
			rd := rn.Ready()
			if len(rd.Committed) != 0 {
				t.Fatalf("guarded stale-originator replies committed fast-path commands: %#v", rd.Committed)
			}
			if rec := optimizedRequireRecord(t, rd, ref); rec.Status != StatusAccepted || rec.Seq != remoteAttrs.Seq || !instanceNumsEqual(rec.Deps, remoteAttrs.Deps) {
				t.Fatalf("guarded stale-originator record = %#v, want accepted with reply attrs seq %d deps %v", rec, remoteAttrs.Seq, remoteAttrs.Deps)
			}
			optimizedRequireNoMessageType(t, rd.Messages, MsgCommit)
			for _, to := range []ReplicaID{2, 3} {
				msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
				if msg.From != 1 ||
					msg.Ref != ref ||
					msg.Ballot != inst.rec.Ballot ||
					msg.RecordBallot != (Ballot{}) ||
					msg.RecordStatus != StatusNone ||
					msg.Seq != remoteAttrs.Seq ||
					!instanceNumsEqual(msg.Deps, remoteAttrs.Deps) ||
					!commandEqual(msg.Command, inst.rec.Command) {
					t.Fatalf("guarded stale-originator accept to %d = %#v, want canonical accepted attrs seq %d deps %v", to, msg, remoteAttrs.Seq, remoteAttrs.Deps)
				}
			}
		})
	}
}

func TestMessageCodecProcessAtRoundTripAndChecksum(t *testing.T) {
	msg := Message{
		Type:      MsgPreAccept,
		From:      1,
		To:        2,
		Ref:       InstanceRef{Replica: 1, Instance: 3, Conf: 1},
		ProcessAt: 17,
		Ballot:    Ballot{Replica: 1},
		Seq:       4,
		Deps:      []InstanceNum{0, 2, 0},
		Command:   Command{ID: CommandID{Client: 51, Sequence: 1}, Payload: []byte("codec-process-at"), ConflictKeys: [][]byte{[]byte("codec-key")}},
	}
	encoded, err := EncodeMessage(nil, msg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Message
	if err := DecodeMessage(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ProcessAt != msg.ProcessAt || decoded.Ref != msg.Ref || decoded.Seq != msg.Seq || !instanceNumsEqual(decoded.Deps, msg.Deps) || !commandEqual(decoded.Command, msg.Command) {
		t.Fatalf("decoded message = %#v, want ProcessAt/ref/attrs/command from %#v", decoded, msg)
	}
	if !VerifyMessageChecksum(decoded) {
		t.Fatalf("decoded message checksum does not authenticate ProcessAt-bearing message: %#v", decoded)
	}

	corrupt := append([]byte(nil), encoded...)
	processAtOffset := encodedProcessAtOffset(t, corrupt)
	corrupt[processAtOffset] ^= 0x01
	if err := DecodeMessage(corrupt, &decoded); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("DecodeMessage with modified ProcessAt err=%v, want %v", err, ErrChecksumMismatch)
	}
}

func TestPreAcceptMessageLessOrdersProcessAtRefAndSender(t *testing.T) {
	base := Message{ProcessAt: 5, Ref: InstanceRef{Replica: 2, Instance: 3, Conf: 4}, From: 2}
	for _, tt := range []struct {
		name string
		a    Message
		b    Message
	}{
		{name: "process at", a: Message{ProcessAt: 4, Ref: base.Ref, From: base.From}, b: base},
		{name: "conf", a: Message{ProcessAt: 5, Ref: InstanceRef{Replica: 2, Instance: 3, Conf: 3}, From: base.From}, b: base},
		{name: "replica", a: Message{ProcessAt: 5, Ref: InstanceRef{Replica: 1, Instance: 3, Conf: 4}, From: base.From}, b: base},
		{name: "instance", a: Message{ProcessAt: 5, Ref: InstanceRef{Replica: 2, Instance: 2, Conf: 4}, From: base.From}, b: base},
		{name: "sender", a: Message{ProcessAt: 5, Ref: base.Ref, From: 1}, b: base},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if !preAcceptMessageLess(tt.a, tt.b) {
				t.Fatalf("preAcceptMessageLess(%#v, %#v) = false, want true", tt.a, tt.b)
			}
			if preAcceptMessageLess(tt.b, tt.a) {
				t.Fatalf("preAcceptMessageLess(%#v, %#v) = true, want false", tt.b, tt.a)
			}
		})
	}
	if preAcceptMessageLess(base, base) {
		t.Fatalf("preAcceptMessageLess(%#v, same) = true, want false", base)
	}
}

func processAtPreAccept(ref InstanceRef, from, to ReplicaID, processAt uint64, depsWidth int, cmd Command) Message {
	return Message{
		Type:      MsgPreAccept,
		From:      from,
		To:        to,
		Ref:       ref,
		ProcessAt: processAt,
		Ballot:    Ballot{Replica: ref.Replica},
		Seq:       1,
		Deps:      make([]InstanceNum, depsWidth),
		Command:   cmd,
	}
}

func requirePreAcceptProcessAt(t *testing.T, messages []Message, ref InstanceRef, want InstanceRecord, processAt uint64, wantTo []ReplicaID) {
	t.Helper()
	if len(messages) != len(wantTo) {
		t.Fatalf("preaccept messages = %#v, want %d messages", messages, len(wantTo))
	}
	gotTo := make([]ReplicaID, len(messages))
	for i, msg := range messages {
		if msg.Type != MsgPreAccept || msg.From != ref.Replica || msg.Ref != ref || msg.ProcessAt != processAt {
			t.Fatalf("preaccept message %d = %#v, want ProcessAt %d for %s from %d", i, msg, processAt, ref, ref.Replica)
		}
		if msg.Ballot != want.Ballot ||
			msg.RecordBallot != (Ballot{}) ||
			msg.RecordStatus != StatusNone ||
			msg.Seq != want.Seq ||
			!instanceNumsEqual(msg.Deps, want.Deps) ||
			!commandEqual(msg.Command, want.Command) ||
			msg.FastPathEligible ||
			msg.DepsCommitted != 0 {
			t.Fatalf("preaccept message %d = %#v, want canonical tuple %#v", i, msg, want)
		}
		gotTo[i] = msg.To
	}
	if !slices.Equal(gotTo, wantTo) {
		t.Fatalf("preaccept recipients = %v, want %v", gotTo, wantTo)
	}
}

func recordRefs(records []InstanceRecord) []InstanceRef {
	out := make([]InstanceRef, len(records))
	for i := range records {
		out[i] = records[i].Ref
	}
	return out
}

func encodedProcessAtOffset(t *testing.T, encoded []byte) int {
	t.Helper()
	offset := len(wireMagic)
	for field := range 6 {
		_, n := binary.Uvarint(encoded[offset:])
		if n <= 0 {
			t.Fatalf("encoded message field %d before ProcessAt is not a valid uvarint", field)
		}
		offset += n
	}
	if _, n := binary.Uvarint(encoded[offset:]); n <= 0 {
		t.Fatalf("encoded ProcessAt at offset %d is not a valid uvarint", offset)
	}
	return offset
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
	putAttrVoteSet(inst.preOK)
	inst.preOK = nil
	resp := Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Seq: inst.rec.Seq, Deps: inst.rec.Deps}
	if err := rn.handlePreAcceptResp(resp); err != nil {
		panic(err)
	}
	before := inst.preOK.len()
	if err := rn.handlePreAcceptResp(resp); err != nil {
		panic(err)
	}
	if inst.preOK.len() != before {
		t.Fatal("duplicate preaccept response changed votes")
	}
	startPrepared := func(t *testing.T, voters int, rec InstanceRecord) (*RawNode, *instance, Ballot) {
		t.Helper()
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(voters)})
		if err != nil {
			t.Fatal(err)
		}
		if rec.Deps == nil {
			rec.Deps = rn.q.deps()
		}
		inst := &instance{rec: rec}
		rn.instances[rec.Ref] = inst
		if err := rn.startPrepare(inst); err != nil {
			panic(err)
		}
		return rn, inst, inst.rec.Ballot
	}

	t.Run("duplicate prepare responses do not form quorum", func(t *testing.T) {
		prepRef := InstanceRef{Replica: 1, Instance: 22, Conf: 1}
		rn, prep, ballot := startPrepared(t, 5, InstanceRecord{
			Ref:     prepRef,
			Status:  StatusPreAccepted,
			Seq:     1,
			Command: Command{Payload: []byte("local")},
		})
		resp := Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: prepRef, RecordStatus: StatusAccepted, Ballot: ballot, RecordBallot: Ballot{Replica: 2}, Seq: 2, Deps: []InstanceNum{0, 2, 0, 0, 0}, Command: Command{Payload: []byte("accepted-2")}}
		if err := rn.handlePrepareResp(resp); err != nil {
			t.Fatal(err)
		}
		if prep.phase != phasePrepare {
			t.Fatalf("single prepare response formed quorum: phase=%d", prep.phase)
		}
		votes := prep.prepareOK.len()
		if err := rn.handlePrepareResp(resp); err != nil {
			t.Fatal(err)
		}
		if prep.phase != phasePrepare || prep.prepareOK.len() != votes {
			t.Fatalf("duplicate prepare response changed recovery state: phase=%d votes=%d want phase=%d votes=%d", prep.phase, prep.prepareOK.len(), phasePrepare, votes)
		}
		resp.From = 3
		resp.Seq = 3
		resp.Deps = []InstanceNum{0, 0, 3, 0, 0}
		resp.Command = Command{Payload: []byte("accepted-3")}
		if err := rn.handlePrepareResp(resp); err != nil {
			t.Fatal(err)
		}
		if prep.phase != phaseAccept {
			t.Fatalf("distinct prepare quorum did not start accept: phase=%d", prep.phase)
		}
	})

	t.Run("committed prepare response is chosen", func(t *testing.T) {
		committedRef := InstanceRef{Replica: 1, Instance: 23, Conf: 1}
		committedCommand := Command{Payload: []byte("committed")}
		rn, prep, ballot := startPrepared(t, 3, InstanceRecord{
			Ref:     committedRef,
			Status:  StatusAccepted,
			Seq:     7,
			Deps:    []InstanceNum{7, 0, 0},
			Command: Command{Payload: []byte("accepted-local")},
		})
		if err := rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: committedRef, RecordStatus: StatusCommitted, Ballot: ballot, RecordBallot: Ballot{Replica: 2}, Seq: 4, Deps: []InstanceNum{0, 0, 4}, Command: committedCommand}); err != nil {
			t.Fatal(err)
		}
		if prep.phase != phaseCommitted || prep.rec.Status != StatusCommitted || prep.rec.Seq != 4 || !bytes.Equal(prep.rec.Command.Payload, committedCommand.Payload) {
			t.Fatalf("committed prepare response not chosen: phase=%d record=%#v", prep.phase, prep.rec)
		}
	})

	t.Run("accepted response beats preaccepted without merging attributes", func(t *testing.T) {
		prepRef := InstanceRef{Replica: 1, Instance: 24, Conf: 1}
		acceptedCommand := Command{Payload: []byte("accepted")}
		rn, prep, ballot := startPrepared(t, 3, InstanceRecord{
			Ref:     prepRef,
			Status:  StatusPreAccepted,
			Seq:     6,
			Deps:    []InstanceNum{5, 0, 0},
			Command: Command{Payload: []byte("preaccepted")},
		})
		if err := rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: prepRef, RecordStatus: StatusAccepted, Ballot: ballot, RecordBallot: Ballot{Replica: 2}, Seq: 4, Deps: []InstanceNum{0, 7, 0}, Command: acceptedCommand}); err != nil {
			t.Fatal(err)
		}
		if prep.phase != phaseAccept || prep.rec.Status != StatusAccepted || !bytes.Equal(prep.rec.Command.Payload, acceptedCommand.Payload) {
			t.Fatalf("accepted prepare response not selected over preaccepted: phase=%d record=%#v", prep.phase, prep.rec)
		}
		if prep.rec.Seq != 4 || len(prep.rec.Deps) != 3 || prep.rec.Deps[0] != 0 || prep.rec.Deps[1] != 7 {
			t.Fatalf("prepare attributes = seq=%d deps=%#v, want accepted tuple only", prep.rec.Seq, prep.rec.Deps)
		}
	})

	t.Run("non-owner none quorum starts noop accept", func(t *testing.T) {
		nonOwnerRef := InstanceRef{Replica: 2, Instance: 25, Conf: 1}
		rn, prep, ballot := startPrepared(t, 3, InstanceRecord{Ref: nonOwnerRef, Status: StatusNone})
		if err := rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: nonOwnerRef, RecordStatus: StatusNone, Ballot: ballot, Deps: rn.q.deps()}); err != nil {
			t.Fatal(err)
		}
		if prep.phase != phaseAccept || prep.rec.Status != StatusAccepted {
			t.Fatalf("non-owner StatusNone quorum did not start accept: phase=%d record=%#v", prep.phase, prep.rec)
		}
		if prep.rec.Command.Kind != CommandNoop {
			t.Fatalf("StatusNone quorum command kind=%d, want noop", prep.rec.Command.Kind)
		}
	})
}

func TestConflictIndexIncludesKnownInstancesFromEveryReplica(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("shared-key")
	priors := []InstanceRecord{
		{
			Ref:     InstanceRef{Replica: 2, Instance: 5, Conf: 1},
			Status:  StatusCommitted,
			Seq:     7,
			Deps:    rn.q.deps(),
			Command: Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("prior-r2"), ConflictKeys: [][]byte{key}},
		},
		{
			Ref:     InstanceRef{Replica: 3, Instance: 3, Conf: 1},
			Status:  StatusCommitted,
			Seq:     4,
			Deps:    rn.q.deps(),
			Command: Command{ID: CommandID{Client: 3, Sequence: 1}, Payload: []byte("prior-r3"), ConflictKeys: [][]byte{key}},
		},
	}
	for _, rec := range priors {
		rec = checkedRecord(rec)
		rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
	}

	ref, err := rn.Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("next"), ConflictKeys: [][]byte{key}})
	if err != nil {
		t.Fatal(err)
	}
	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing proposed instance %s", ref)
	}
	if inst.rec.Seq != 8 || len(inst.rec.Deps) != 3 || inst.rec.Deps[0] != 0 || inst.rec.Deps[1] != 5 || inst.rec.Deps[2] != 3 {
		t.Fatalf("proposed attrs seq=%d deps=%v, want seq=8 deps=[0 5 3]", inst.rec.Seq, inst.rec.Deps)
	}
}

func TestRemainingDependencyAndStorageBranches(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{Kind: CommandConfChange}
	for _, rec := range []InstanceRecord{
		checkedRecord(InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Status: StatusNone, Command: Command{Payload: []byte("skip")}}),
		checkedRecord(InstanceRecord{Ref: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Status: StatusCommitted, Seq: 4, Command: Command{Kind: CommandNoop}}),
		checkedRecord(InstanceRecord{Ref: InstanceRef{Replica: 3, Instance: 1, Conf: 1}, Status: StatusCommitted, Seq: 5, Command: Command{Payload: []byte("dep")}}),
	} {
		rn.installInstance(&instance{rec: rec, phase: phaseFromStatus(rec.Status)})
	}
	attrs := rn.computeAttrs(cmd, InstanceRef{Replica: 1, Instance: 99, Conf: 1})
	if attrs.Seq != 6 || attrs.Deps[2] != 1 {
		t.Fatalf("conf attrs=%#v", attrs)
	}
	none := rn.computeAttrs(Command{Kind: CommandNoop}, InstanceRef{})
	if none.Seq != 1 {
		t.Fatal("noop attrs failed")
	}
	view := rn.newExecutionView()
	var candidates []recoveryCandidate
	if rn.hasUnresolvedKnownConflict(&view, InstanceRef{}, nil, &candidates) {
		t.Fatal("missing instance conflict")
	}
	iter := view.dependencyRefs(rn, InstanceRef{})
	if _, ok := iter.next(); ok {
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
	view := rn.newExecutionView()
	comps := rn.executionComponents(&view)
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
	view2 := rn2.newExecutionView()
	var candidates []recoveryCandidate
	if rn2.componentReady(&view2, []InstanceRef{x}, &candidates) {
		t.Fatal("component should wait for committed external dependency")
	}
	rn2.executed.add(y)
	candidates = candidates[:0]
	if !rn2.componentReady(&view2, []InstanceRef{x}, &candidates) {
		t.Fatal("component should accept executed dependency")
	}
	if comps := rn2.executionComponents(&view2); len(comps) != 1 {
		t.Fatalf("executed dependency should be skipped by component builder: %#v", comps)
	}
	rn2.instances[y].rec.Status = StatusNone
	candidates = candidates[:0]
	if rn2.hasUnresolvedKnownConflict(&view2, x, map[InstanceRef]struct{}{}, &candidates) {
		t.Fatal("none-status conflict should be ignored")
	}
	rn2.instances[y].rec.Status = StatusCommitted
	candidates = candidates[:0]
	if rn2.hasUnresolvedKnownConflict(&view2, x, map[InstanceRef]struct{}{}, &candidates) {
		t.Fatal("committed conflict should be ignored by unresolved check")
	}

	conf, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
	if err != nil {
		t.Fatal(err)
	}
	conf.applyConfChange(InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command{Kind: CommandConfChange, Payload: []byte{99, 0, 0, 0, 0, 0, 0, 0, 0}})
	conf.applyConfChange(InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeRemoveVoter), 1, 0, 0, 0, 0, 0, 0, 0}})
	if conf.Status().Conf.Contains(0) {
		t.Fatal("invalid configuration applied")
	}
}

func TestApplyConfChangeRejectsInvalidCommittedPayloads(t *testing.T) {
	tests := []struct {
		name   string
		voters []ReplicaID
		cmd    Command
	}{
		{
			name:   "malformed payload",
			voters: makeIDs(3),
			cmd:    Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeAddVoter), 4}},
		},
		{
			name:   "add zero",
			voters: makeIDs(3),
			cmd:    confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 0}),
		},
		{
			name:   "add existing voter",
			voters: makeIDs(3),
			cmd:    confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 2}),
		},
		{
			name:   "add beyond seven voters",
			voters: makeIDs(7),
			cmd:    confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 8}),
		},
		{
			name:   "remove absent voter",
			voters: makeIDs(3),
			cmd:    confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 4}),
		},
		{
			name:   "remove only voter",
			voters: makeIDs(1),
			cmd:    confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 1}),
		},
		{
			name:   "unknown type",
			voters: makeIDs(3),
			cmd:    confChangeCommand(ConfChange{Type: ConfChangeType(99), Replica: 2}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, err := NewRawNode(Config{ID: 1, Voters: tt.voters})
			if err != nil {
				t.Fatal(err)
			}
			before := rn.Status().Conf
			rn.pendingConf = true

			rn.applyConfChange(InstanceRef{Replica: 1, Instance: 1, Conf: 1}, tt.cmd)

			if rn.pendingConf {
				t.Fatalf("applyConfChange(%#v) left pendingConf set", tt.cmd)
			}
			assertConfState(t, rn.Status().Conf, before)
		})
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
	if readyHasStatus(rd, ref, StatusExecuted) {
		t.Fatalf("first ready persisted executed status before advance for %s: %#v", ref, rd.Records)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
	executedReady := rn.Ready()
	if len(executedReady.Committed) != 0 || !readyHasStatus(executedReady, ref, StatusExecuted) {
		t.Fatalf("executed ready for %s = %#v", ref, executedReady)
	}
	if err := store.ApplyReady(executedReady); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, executedReady)
	if rn.HasReady() {
		t.Fatal("node still has ready work after executed record")
	}
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

func TestNewRawNodeRejectsSemanticallyInvalidDurableRecords(t *testing.T) {
	base := InstanceRecord{
		Ref:          InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Status:       StatusPreAccepted,
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		Command:      Command{ID: CommandID{Client: 1, Sequence: 1}, ConflictKeys: [][]byte{[]byte("key")}},
	}
	tests := []struct {
		name   string
		mutate func(*InstanceRecord)
	}{
		{name: "partial reference", mutate: func(rec *InstanceRecord) { rec.Ref.Replica = 0 }},
		{name: "unknown configuration", mutate: func(rec *InstanceRecord) { rec.Ref.Conf = 99 }},
		{name: "non-voter owner", mutate: func(rec *InstanceRecord) { rec.Ref.Replica = 4 }},
		{name: "invalid status", mutate: func(rec *InstanceRecord) { rec.Status = Status(255) }},
		{name: "invalid command kind", mutate: func(rec *InstanceRecord) { rec.Command.Kind = CommandKind(255) }},
		{name: "short configuration command", mutate: func(rec *InstanceRecord) {
			rec.Command = Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeAddVoter)}}
		}},
		{name: "unknown configuration command type", mutate: func(rec *InstanceRecord) {
			rec.Command = Command{Kind: CommandConfChange, Payload: make([]byte, 9)}
			rec.Command.Payload[0] = 255
		}},
		{name: "zero configuration command replica", mutate: func(rec *InstanceRecord) {
			rec.Command = Command{Kind: CommandConfChange, Payload: make([]byte, 9)}
			rec.Command.Payload[0] = byte(ConfChangeAddVoter)
		}},
		{name: "zero chosen sequence", mutate: func(rec *InstanceRecord) { rec.Seq = 0 }},
		{name: "short dependencies", mutate: func(rec *InstanceRecord) { rec.Deps = rec.Deps[:2] }},
		{name: "non-voter promise ballot", mutate: func(rec *InstanceRecord) { rec.Ballot.Replica = 4 }},
		{name: "record ballot above promise", mutate: func(rec *InstanceRecord) {
			rec.RecordBallot = Ballot{Number: 2, Replica: 2}
		}},
		{name: "invalid accept evidence sender", mutate: func(rec *InstanceRecord) {
			rec.Status = StatusAccepted
			rec.AcceptSeq = 1
			rec.AcceptDeps = []InstanceNum{0, 0, 0}
			rec.AcceptEvidence = []AcceptEvidence{{Sender: 4, Seq: 1, Deps: []InstanceNum{0, 0, 0}}}
		}},
		{name: "non-pending none carries command", mutate: func(rec *InstanceRecord) {
			rec.Status = StatusNone
			rec.RecordBallot = Ballot{}
			rec.Seq = 0
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := base.Clone()
			tc.mutate(&rec)
			rec.Checksum = ChecksumRecord(rec)
			store := NewMemoryStorage()
			store.Records[rec.Ref] = rec
			if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store}); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewRawNode with %s record err=%v, want ErrInvalidConfig", tc.name, err)
			}
		})
	}
}

func TestNewRawNodeRejectsDuplicateDurableRecords(t *testing.T) {
	rec := checkedRecord(InstanceRecord{
		Ref:          InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Status:       StatusPreAccepted,
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		Command:      Command{ConflictKeys: [][]byte{[]byte("duplicate-record-key")}},
	})
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: duplicateRecordStorage{rec: rec}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewRawNode with duplicate durable records err=%v, want ErrInvalidConfig", err)
	}
}

func TestPreparePromiseForKnownInstancePersistsAndRejectsStaleBallots(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 2, Instance: 10, Conf: 1}
	promise := Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 2, Replica: 2}}
	if err := rn.Step(promise); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusNone || rd.Records[0].Ballot != promise.Ballot {
		t.Fatalf("promise record = %#v", rd.Records)
	}
	if len(rd.Records[0].Deps) != 3 {
		t.Fatalf("known config promise deps = %v, want pinned voter width", rd.Records[0].Deps)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgPrepareResp || rd.Messages[0].RecordStatus != StatusNone || rd.Messages[0].Ballot != promise.Ballot {
		t.Fatalf("promise response = %#v", rd.Messages)
	}
	advanceOK(t, rn, rd)

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

func TestValueMessageWithoutBallotCannotPoisonRestart(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotRemainingNodeProtocol(rn)
	err = rn.Step(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{ConflictKeys: [][]byte{[]byte("zero-ballot-poison")}},
	})
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("zero-ballot PreAccept err=%v, want ErrInvalidMessage", err)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)
	if _, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store}); err != nil {
		t.Fatalf("restart after zero-ballot rejection: %v", err)
	}
}

func TestInvalidInboundConfigurationCommandsCannotPoisonRestart(t *testing.T) {
	tests := []struct {
		name    string
		command Command
	}{
		{name: "short payload", command: Command{Kind: CommandConfChange, Payload: []byte{byte(ConfChangeAddVoter)}}},
		{name: "invalid transition", command: confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 2})},
		{name: "unknown command kind", command: Command{Kind: CommandKind(255)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStorage()
			rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store})
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotRemainingNodeProtocol(rn)
			err = rn.Step(Message{
				Type:    MsgPreAccept,
				From:    1,
				To:      2,
				Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: 1},
				Ballot:  Ballot{Replica: 1},
				Seq:     1,
				Deps:    []InstanceNum{0, 0, 0},
				Command: tc.command,
			})
			if !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("Step malformed configuration command err=%v, want ErrInvalidMessage", err)
			}
			requireRemainingNodeProtocolUnchanged(t, rn, before)
			if _, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store}); err != nil {
				t.Fatalf("restart after rejected configuration command: %v", err)
			}
		})
	}

	store := NewMemoryStorage()
	store.Hard.Conf = ConfState{ID: ^ConfID(0), Voters: makeIDs(3)}
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotRemainingNodeProtocol(rn)
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4}); !errors.Is(err, ErrVoterCertificateRequired) {
		t.Fatalf("legacy AddVoter at max configuration err=%v, want ErrVoterCertificateRequired", err)
	}
	err = rn.Step(Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      2,
		Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: ^ConfID(0)},
		Ballot:  Ballot{Replica: 1},
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
	})
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("max-configuration inbound command err=%v, want ErrInvalidMessage", err)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)
	maxRef := InstanceRef{Replica: 1, Instance: 1, Conf: ^ConfID(0)}
	store.Records[maxRef] = checkedRecord(InstanceRecord{
		Ref:              maxRef,
		Ballot:           Ballot{Replica: 1},
		RecordBallot:     Ballot{Replica: 1},
		Status:           StatusExecuted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0},
		Command:          confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
		ConfChangeResult: ConfChangeResult{Outcome: ConfChangeRejectedInvalid},
	})
	restarted, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatalf("restart with explicit max-configuration rejection: %v", err)
	}
	if got := restarted.instances[maxRef].rec.ConfChangeResult; got.Outcome != ConfChangeRejectedInvalid || !confStateIsZero(got.Conf) {
		t.Fatalf("max-configuration durable result=%#v, want RejectedInvalid", got)
	}
	if rd := restarted.Ready(); len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 ||
		(!rd.HardState.Empty() && (!rd.HardState.Equal(restarted.currentHardState) || !rd.MustSync)) {
		t.Fatalf("explicit max-configuration rejection produced migration payload Ready: %#v", rd)
	}
}

func TestPrepareForUnknownConfigurationIsRejectedWithoutMutation(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotRemainingNodeProtocol(rn)
	ref := InstanceRef{Replica: 2, Instance: 1, Conf: 99}
	err = rn.Step(Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}})
	if !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("unknown configuration Prepare error = %v, want ErrMessageRejected", err)
	}
	if rn.instances[ref] != nil {
		t.Fatalf("unknown configuration Prepare created instance %#v", rn.instances[ref])
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)
}

func TestAcceptForCommittedInstanceReturnsChosenCommand(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	chosen := Message{
		Type:         MsgCommit,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Seq:          1,
		Deps:         rn.q.deps(),
		Command:      Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("chosen"), ConflictKeys: [][]byte{[]byte("k")}},
	}
	if err := rn.Step(chosen); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Committed) != 1 || string(rd.Committed[0].Command.Payload) != "chosen" {
		t.Fatalf("commit was not applied before accept retry: %#v", rd.Committed)
	}
	advanceOK(t, rn, rd)

	executedReady := rn.Ready()
	if len(executedReady.Committed) != 0 || !readyHasStatus(executedReady, ref, StatusExecuted) {
		t.Fatalf("executed ready for %s = %#v", ref, executedReady)
	}
	var fullRecord InstanceRecord
	for _, record := range executedReady.Records {
		if record.Ref == ref && record.Status == StatusExecuted {
			fullRecord = record.Clone()
			break
		}
	}
	if fullRecord.Ref.IsZero() {
		t.Fatalf("executed ready lacks full record for %s: %#v", ref, executedReady.Records)
	}
	advanceOK(t, rn, executedReady)
	if inst := rn.instances[ref]; inst == nil || !inst.payloadAbsent {
		t.Fatalf("executed resident=%#v, want payload stub", inst)
	}

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
	loadReady := rn.Ready()
	if len(loadReady.RecordLoads) != 1 || loadReady.RecordLoads[0] != ref || len(loadReady.Messages) != 0 {
		t.Fatalf("accept retry ready=%#v, want one record load and no message before restore", loadReady)
	}
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: fullRecord, Found: true}); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, loadReady)
	rd = rn.Ready()
	if len(rd.Records) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("accept retry changed committed state: records=%#v committed=%#v", rd.Records, rd.Committed)
	}
	if len(rd.Messages) != 1 {
		t.Fatalf("accept retry response = %#v, want one chosen commit", rd.Messages)
	}
	msg := rd.Messages[0]
	if msg.Type != MsgCommit ||
		msg.From != 1 ||
		msg.To != 2 ||
		msg.Ref != ref ||
		msg.Ballot != chosen.Ballot ||
		msg.RecordBallot == (Ballot{}) ||
		msg.RecordBallot != chosen.RecordBallot ||
		msg.RecordStatus != StatusNone ||
		msg.Seq != chosen.Seq ||
		!instanceNumsEqual(msg.Deps, chosen.Deps) ||
		!commandEqual(msg.Command, chosen.Command) {
		t.Fatalf("accept retry response = %#v, want canonical chosen commit", rd.Messages)
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
			remainingApplyHardStateOnly(t, restarted, 0)
			for tick := uint64(1); tick < tt.ticks; tick++ {
				if err := restarted.Tick(); err != nil {
					t.Fatal(err)
				}
				remainingApplyHardStateOnly(t, restarted, tick)
			}
			if err := restarted.Tick(); err != nil {
				t.Fatal(err)
			}
			rd := restarted.Ready()
			if len(rd.Messages) != 2 {
				t.Fatalf("retry messages = %#v", rd.Messages)
			}
			for _, m := range rd.Messages {
				if m.Type != tt.msg || m.Ref != ref || m.From != 1 || (m.To != 2 && m.To != 3) {
					t.Fatalf("retry message = %#v, want %s for %s", m, tt.msg, ref)
				}
				if tt.msg == MsgAccept {
					rec := store.Records[ref]
					if m.Ballot != rec.Ballot ||
						m.RecordBallot != (Ballot{}) ||
						m.RecordStatus != StatusNone ||
						m.Seq != rec.Seq ||
						!instanceNumsEqual(m.Deps, rec.Deps) ||
						!commandEqual(m.Command, rec.Command) {
						t.Fatalf("accept retry message = %#v, want canonical accepted tuple %#v", m, rec)
					}
				}
			}
		})
	}
}

func TestLateLogicalPreAcceptIsConservative(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{
		ID:                    2,
		Voters:                makeIDs(3),
		Storage:               store,
		RetryTicks:            5,
		RecoveryTicks:         5,
		TimeOptimization:      true,
		TimeOptimizationTicks: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	for tick := uint64(1); tick <= 2; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		remainingApplyHardStateOnly(t, rn, tick)
	}

	lateRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	late := processAtPreAccept(lateRef, 1, 2, 2, 3, Command{ID: CommandID{Client: 411, Sequence: 1}, Payload: []byte("late-logical"), ConflictKeys: [][]byte{[]byte("late-logical-key")}})
	if err := rn.Step(late); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != lateRef || rd.Records[0].FastPathEligible ||
		len(rd.Messages) != 1 || rd.Messages[0].FastPathEligible {
		t.Fatalf("late logical Ready=%#v, want conservative non-fast record and response", rd)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)

	timelyRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	timely := processAtPreAccept(timelyRef, 3, 2, 3, 3, Command{ID: CommandID{Client: 412, Sequence: 1}, Payload: []byte("timely-logical"), ConflictKeys: [][]byte{[]byte("timely-logical-key")}})
	if err := rn.Step(timely); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatalf("future logical message produced Ready before Tick: %#v", rn.Ready())
	}
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != timelyRef || !rd.Records[0].FastPathEligible ||
		len(rd.Messages) != 1 || !rd.Messages[0].FastPathEligible {
		t.Fatalf("timely logical Ready=%#v, want timestamp-fast-eligible record and response", rd)
	}
}

func TestExecutedTrackerKeepsSparseExactHolesAndCheckedMax(t *testing.T) {
	var tracker executedTracker
	lane := instanceLane{conf: 7, replica: 3}
	one := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: 1}
	two := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: 2}
	three := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: 3}
	maxRef := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: ^InstanceNum(0)}

	tracker.add(one)
	tracker.add(three)
	tracker.add(maxRef)
	if got := tracker.prefix(lane); got != 1 {
		t.Fatalf("sparse executed frontier = %d, want 1", got)
	}
	if !tracker.contains(three) || !tracker.contains(maxRef) || tracker.contains(two) {
		t.Fatalf("sparse exact membership lost: one=%v two=%v three=%v maxRef=%v", tracker.contains(one), tracker.contains(two), tracker.contains(three), tracker.contains(maxRef))
	}
	tracker.add(two)
	if got := tracker.prefix(lane); got != 3 {
		t.Fatalf("filled executed frontier = %d, want 3", got)
	}
	if next, ok := instanceSuccessor(^InstanceNum(0)); ok || next != 0 {
		t.Fatalf("MaxUint64 successor = %d,%v, want 0,false", next, ok)
	}
}

func TestCommittedThroughDoesNotBridgeSparseHoleOrMaxEndpoint(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	lane := instanceLane{conf: 1, replica: 2}
	install := func(instanceNumber InstanceNum, status Status) {
		ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instanceNumber}
		rn.installInstance(&instance{rec: InstanceRecord{Ref: ref, Status: status}})
	}
	install(1, StatusCommitted)
	install(3, StatusExecuted)
	install(^InstanceNum(0), StatusCommitted)
	if rn.committedPrefix(lane.replica, 3, lane.conf) {
		t.Fatal("committed frontier bridged missing exact slot 2")
	}
	if got := rn.committedThrough[lane]; got != 1 {
		t.Fatalf("sparse committed frontier = %d, want 1", got)
	}
	install(2, StatusCommitted)
	if !rn.committedPrefix(lane.replica, 3, lane.conf) {
		t.Fatal("committed frontier did not merge exact slots 1..3")
	}
	if rn.committedPrefix(lane.replica, ^InstanceNum(0), lane.conf) {
		t.Fatal("distant MaxUint64 record bridged the missing interval")
	}
}

func TestLogicalDeferredPoisonDoesNotBlockLaterDuePreAccept(t *testing.T) {
	for _, tc := range []struct {
		name    string
		install func(*RawNode, Message) *instance
		wantErr error
	}{
		{
			name: "same-ballot conflict",
			install: func(rn *RawNode, message Message) *instance {
				rec := checkedRecord(InstanceRecord{Ref: message.Ref, Ballot: message.Ballot, RecordBallot: message.Ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("poison-conflict", "poison-conflict-key"), ProcessAt: message.ProcessAt, TimingDomain: TimingDomainLogical})
				inst := &instance{rec: rec, phase: phaseIdle}
				rn.installInstance(inst)
				return inst
			},
			wantErr: ErrInvalidMessage,
		},
		{
			name: "stale",
			install: func(rn *RawNode, message Message) *instance {
				rec := checkedRecord(InstanceRecord{Ref: message.Ref, Ballot: Ballot{Number: 1, Replica: 3}, RecordBallot: message.Ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: message.Command.Clone(), ProcessAt: message.ProcessAt, TimingDomain: TimingDomainLogical})
				inst := &instance{rec: rec, phase: phaseIdle}
				rn.installInstance(inst)
				return inst
			},
		},
		{
			name: "generation exhausted",
			install: func(rn *RawNode, message Message) *instance {
				rec := checkedRecord(InstanceRecord{Ref: message.Ref, Ballot: Ballot{}, RecordBallot: message.Ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: message.Command.Clone(), ProcessAt: message.ProcessAt - 1, TimingDomain: TimingDomainLogical})
				inst := &instance{rec: rec, phase: phaseIdle, generation: ^uint64(0)}
				rn.installInstance(inst)
				return inst
			},
			wantErr: ErrLogicalTimeExhausted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), RetryTicks: 2, RecoveryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 2})
			if err != nil {
				t.Fatal(err)
			}
			remainingApplyHardStateOnly(t, rn, 0)
			first := processAtPreAccept(InstanceRef{Conf: 1, Replica: 1, Instance: 1}, 1, 2, 2, 3, optimizedTestCommand("poison-a", "poison-a-key"))
			second := processAtPreAccept(InstanceRef{Conf: 1, Replica: 3, Instance: 1}, 3, 2, 2, 3, optimizedTestCommand("valid-b", "valid-b-key"))
			if err := rn.Step(first); err != nil {
				t.Fatal(err)
			}
			if err := rn.Step(second); err != nil {
				t.Fatal(err)
			}
			firstEntry := rn.deferredIndex[deferredPreAcceptKey{domain: preAcceptLogical, ref: first.Ref, ballot: first.Ballot, from: first.From}]
			if firstEntry == nil {
				t.Fatal("first deferred entry missing after admission")
			}
			poison := tc.install(rn, first)
			if err := rn.Tick(); err != nil {
				t.Fatal(err)
			}
			remainingApplyHardStateOnly(t, rn, 1)
			err = rn.Tick()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("due processing error = %v, want nil", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("due processing error = %v, want %v", err, tc.wantErr)
			}
			if len(rn.deferredIndex) != 0 || firstEntry.index != -1 || !reflect.DeepEqual(firstEntry.message, Message{}) {
				t.Fatalf("poison entry was not cleared exactly once: index=%d message=%#v deferred=%d", firstEntry.index, firstEntry.message, len(rn.deferredIndex))
			}
			if rn.instances[first.Ref] != poison {
				t.Fatalf("poison entry replaced current state: %#v", rn.instances[first.Ref])
			}
			if valid := rn.instances[second.Ref]; valid == nil || valid.rec.Status != StatusPreAccepted {
				t.Fatalf("later valid due entry did not process: %#v", valid)
			}
			if err := rn.Tick(); err != nil {
				t.Fatalf("later Tick repeated poison error: %v", err)
			}
		})
	}
}

func TestProcessedLogicalPreAcceptRetryReacksPinnedTupleAfterLaterConflict(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 2, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
	command := optimizedTestCommand("logical-retry", "logical-retry-key")
	message := processAtPreAccept(ref, 1, 2, 2, 3, command)
	if err := rn.Step(message); err != nil {
		t.Fatal(err)
	}
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 1)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	processed := rn.Ready()
	if err := rn.Advance(processed); err != nil {
		t.Fatal(err)
	}
	pinned := rn.instances[ref].rec.Clone()
	conflictRef := InstanceRef{Conf: 1, Replica: 3, Instance: 1}
	conflict := checkedRecord(InstanceRecord{Ref: conflictRef, Ballot: Ballot{Replica: 3}, RecordBallot: Ballot{Replica: 3}, Status: StatusCommitted, Seq: pinned.Seq + 10, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("later-conflict", "logical-retry-key"), ProcessAt: 3, TimingDomain: TimingDomainLogical})
	rn.installInstance(&instance{rec: conflict, phase: phaseCommitted})
	if err := rn.Step(message); err != nil {
		t.Fatal(err)
	}
	if !instanceRecordEqual(rn.instances[ref].rec, pinned) {
		t.Fatalf("exact logical retry changed pinned tuple: got=%#v want=%#v", rn.instances[ref].rec, pinned)
	}
	rd := rn.Ready()
	if len(rd.Records) != 0 || len(rd.Messages) != 1 || rd.Messages[0].Type != MsgPreAcceptResp ||
		rd.Messages[0].Seq != pinned.Seq || !instanceNumsEqual(rd.Messages[0].Deps, pinned.Deps) {
		t.Fatalf("exact logical retry Ready = %#v, want only original tuple re-ack", rd)
	}
}

func TestTerminalFuturePreAcceptRetriesNeverEnterDeferredQueue(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 5, MaxDeferredPreAccepts: 1})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	ballot := Ballot{Replica: 2}
	command := optimizedTestCommand("terminal-retry", "terminal-retry-key")
	rec := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: StatusCommitted, Seq: 1, Deps: make([]InstanceNum, 3), Command: command, ProcessAt: 10, TimingDomain: TimingDomainLogical})
	inst := &instance{rec: rec, phase: phaseCommitted}
	rn.installInstance(inst)
	message := processAtPreAccept(ref, 2, 1, 10, 3, command)
	for retry := 0; retry < 32; retry++ {
		if err := rn.Step(message); err != nil {
			t.Fatalf("terminal future retry %d: %v", retry, err)
		}
		if len(rn.deferredIndex) != 0 || len(rn.logicalPreAccepts) != 0 {
			t.Fatalf("terminal future retry %d grew deferred queue: index=%d heap=%d", retry, len(rn.deferredIndex), len(rn.logicalPreAccepts))
		}
	}
	if rn.instances[ref] != inst || !instanceRecordEqual(inst.rec, rec) {
		t.Fatalf("terminal future retries mutated committed record: %#v", rn.instances[ref])
	}
	rd := rn.Ready()
	if len(rd.Records) != 0 || len(rd.Messages) != 32 {
		t.Fatalf("terminal future retry Ready = %#v, want 32 immediate Commit responses", rd)
	}
	for _, response := range rd.Messages {
		if response.Type != MsgCommit || response.ProcessAt != rec.ProcessAt || response.TOQ {
			t.Fatalf("terminal future retry response is not canonical Commit: %#v", response)
		}
	}
}

func TestLogicalDeferredPoisonAtMaxStillDrainsIndependentDueTimer(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 1, RetryTicks: 1, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	rn.tick = ^uint64(0) - 1
	rn.currentHardState.Tick = rn.tick
	rn.acknowledgedHardState = rn.currentHardState.Clone()
	timerRef := InstanceRef{Conf: 1, Replica: 3, Instance: 1}
	timerBallot := Ballot{Number: 1, Replica: 3}
	timerRec := checkedRecord(InstanceRecord{Ref: timerRef, Ballot: timerBallot, RecordBallot: timerBallot, Status: StatusAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("timer-progress", "timer-progress-key"), ProcessAt: rn.tick, TimingDomain: TimingDomainLogical})
	timerInst := &instance{rec: timerRec, phase: phaseAccept}
	rn.installInstance(timerInst)
	if err := rn.schedule(timerInst, timerAccept, 1); err != nil {
		t.Fatal(err)
	}
	poisonRef := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
	poisonCommand := optimizedTestCommand("max-poison", "max-poison-key")
	message := processAtPreAccept(poisonRef, 1, 2, ^uint64(0), 3, poisonCommand)
	if err := rn.Step(message); err != nil {
		t.Fatal(err)
	}
	entry := rn.deferredIndex[deferredPreAcceptKey{domain: preAcceptLogical, ref: poisonRef, ballot: message.Ballot, from: message.From}]
	poisonRec := checkedRecord(InstanceRecord{Ref: poisonRef, Ballot: Ballot{}, RecordBallot: message.Ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: poisonCommand, ProcessAt: rn.tick, TimingDomain: TimingDomainLogical})
	poison := &instance{rec: poisonRec, phase: phaseIdle, generation: ^uint64(0)}
	rn.installInstance(poison)
	if err := rn.Tick(); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("max Tick poison error = %v, want ErrLogicalTimeExhausted", err)
	}
	if rn.tick != ^uint64(0) || entry == nil || entry.index != -1 || len(rn.deferredIndex) != 0 {
		t.Fatalf("max Tick did not advance/drop poison exactly once: tick=%d entry=%#v deferred=%d", rn.tick, entry, len(rn.deferredIndex))
	}
	rd := rn.Ready()
	timerMessages := 0
	for _, outbound := range rd.Messages {
		if outbound.Type == MsgAccept && outbound.Ref == timerRef {
			timerMessages++
		}
	}
	if timerMessages != 2 {
		t.Fatalf("independent max-due timer emitted %d Accepts, want two targets: %#v", timerMessages, rd.Messages)
	}
	for _, pending := range rn.timers {
		if pending.ref == timerRef && pending.kind == timerAccept {
			t.Fatalf("max-due timer remained/repeated after bounded drain: %#v", rn.timers)
		}
	}
}
