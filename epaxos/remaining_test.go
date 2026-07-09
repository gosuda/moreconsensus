package epaxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

func checkedRecord(rec InstanceRecord) InstanceRecord {
	if rec.Status >= StatusPreAccepted && rec.RecordBallot == (Ballot{}) {
		rec.RecordBallot = rec.Ballot
	}
	rec.Checksum = ChecksumRecord(rec)
	return rec
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
	advanceInvalid(t, rn, Ready{Records: rd.Records[:1], Messages: rd.Messages[:1], Committed: rd.Committed[:1], MustSync: rd.MustSync})
	if len(rn.pendingReady.Records) != 2 || len(rn.pendingReady.Messages) != 2 || len(rn.pendingReady.Committed) != 2 {
		t.Fatal("barrier rejection changed pending ready")
	}
	advanceOK(t, rn, Ready{Records: rd.Records[:1], MustSync: rd.MustSync})
	if len(rn.pendingReady.Records) != 1 || len(rn.pendingReady.Messages) != 2 || len(rn.pendingReady.Committed) != 2 {
		t.Fatal("partial record advance did not retain later work")
	}
	advanceOK(t, rn, rn.Ready())
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 5}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("pending conf err=%v", err)
	}
	zr, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
	if err != nil {
		t.Fatal(err)
	}
	inst := &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 9, Conf: 1}, Status: StatusCommitted, Deps: zr.q.deps(), Command: Command{Kind: CommandNoop}}, phase: phaseCommitted}
	zr.startAccept(inst, inst.rec.Attributes())
	zr.commit(inst, inst.rec.Attributes())
	zr.schedule(inst, timerAccept, 0)
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

			if _, err := rn.ProposeConfChange(tt.change); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("ProposeConfChange(%#v) err=%v, want ErrInvalidConfig", tt.change, err)
			}
			if rn.pendingConf {
				t.Fatalf("ProposeConfChange(%#v) left pendingConf set", tt.change)
			}
			assertConfState(t, rn.Status().Conf, before)
			if rn.HasReady() {
				t.Fatalf("ProposeConfChange(%#v) produced ready work despite rejection: %#v", tt.change, rn.Ready())
			}
		})
	}
}

func TestValidProposeConfChangeSetsPending(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	before := rn.Status().Conf

	ref, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
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

func TestProposalOwnershipCopiesCallerSlicesByDefault(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
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
		resp := Message{
			Type:             MsgPreAcceptResp,
			From:             from,
			To:               1,
			Ref:              ref,
			Ballot:           inst.rec.Ballot,
			Seq:              inst.rec.Seq,
			Deps:             append([]InstanceNum(nil), inst.rec.Deps...),
			RecordStatus:     StatusPreAccepted,
			FastPathEligible: true,
		}
		if err := rn.Step(resp); err != nil {
			t.Fatalf("step preaccept response from %d: %v", from, err)
		}
	}
	if got, slow, fast := len(inst.preOK), rn.q.slowQuorum(), rn.q.fastQuorum(); got != slow || got >= fast {
		t.Fatalf("preaccept votes = %d, want slow quorum %d below fast quorum %d", got, slow, fast)
	}
	if inst.phase != phasePreAccept {
		t.Fatalf("phase after slow quorum preaccept responses = %d, want preaccept", inst.phase)
	}
	if rn.HasReady() {
		t.Fatalf("slow quorum response produced ready work before fast-wait deadline: %#v", rn.Ready())
	}

	for tick := uint64(1); tick < 3; tick++ {
		rn.Tick()
		if inst.phase != phasePreAccept {
			t.Fatalf("phase after tick %d = %d, want preaccept before fast-wait deadline", tick, inst.phase)
		}
		if rn.HasReady() {
			t.Fatalf("ready work appeared after tick %d before fast-wait deadline: %#v", tick, rn.Ready())
		}
	}

	rn.Tick()
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
		if msg.Type != MsgAccept || msg.From != 1 || msg.Ref != ref {
			t.Fatalf("fast-wait message = %#v, want accept for %s from replica 1", msg, ref)
		}
		seen[msg.To] = true
	}
	for _, to := range []ReplicaID{2, 3, 4, 5, 6} {
		if !seen[to] {
			t.Fatalf("missing accept message to replica %d in %#v", to, rd.Messages)
		}
	}
}

func TestTimeOptimizationLateFastQuorumCommitsWithoutAccept(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(6), Storage: store, RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
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
		resp := Message{
			Type:             MsgPreAcceptResp,
			From:             from,
			To:               1,
			Ref:              ref,
			Ballot:           inst.rec.Ballot,
			Seq:              inst.rec.Seq,
			Deps:             append([]InstanceNum(nil), inst.rec.Deps...),
			RecordStatus:     StatusPreAccepted,
			FastPathEligible: true,
		}
		if err := rn.Step(resp); err != nil {
			t.Fatalf("step matching preaccept response from %d: %v", from, err)
		}
	}

	for _, from := range []ReplicaID{2, 3, 4} {
		stepMatchingPreAcceptResp(from)
	}
	if got, slow, fast := len(inst.preOK), rn.q.slowQuorum(), rn.q.fastQuorum(); got != slow || got >= fast {
		t.Fatalf("preaccept votes = %d, want slow quorum %d below fast quorum %d", got, slow, fast)
	}
	if inst.phase != phasePreAccept {
		t.Fatalf("phase after slow quorum preaccept responses = %d, want preaccept", inst.phase)
	}
	if rn.HasReady() {
		t.Fatalf("slow quorum response produced ready work before fast-wait deadline: %#v", rn.Ready())
	}

	for tick := uint64(1); tick < 3; tick++ {
		rn.Tick()
		if inst.phase != phasePreAccept {
			t.Fatalf("phase after tick %d = %d, want preaccept before fast-wait deadline", tick, inst.phase)
		}
		if rn.HasReady() {
			t.Fatalf("ready work appeared after tick %d before fast-wait deadline: %#v", tick, rn.Ready())
		}
	}

	stepMatchingPreAcceptResp(5)
	if got, fast := len(inst.preOK), rn.q.fastQuorum(); got != fast {
		t.Fatalf("preaccept votes after late response = %d, want fast quorum %d", got, fast)
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
		if msg.Type != MsgCommit || msg.From != 1 || msg.Ref != ref || msg.RecordStatus != StatusCommitted {
			t.Fatalf("late fast-quorum message = %#v, want commit for %s from replica 1", msg, ref)
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
		if err := rn.Step(Message{
			Type:             MsgPreAcceptResp,
			From:             from,
			To:               1,
			Ref:              ref,
			Ballot:           inst.rec.Ballot,
			Seq:              inst.rec.Seq,
			Deps:             append([]InstanceNum(nil), inst.rec.Deps...),
			RecordStatus:     StatusPreAccepted,
			FastPathEligible: true,
		}); err != nil {
			t.Fatalf("step matching five-node preaccept response from %d: %v", from, err)
		}
	}

	stepMatchingPreAcceptResp(2)
	if got := len(inst.preOK); got != 2 {
		t.Fatalf("preaccept votes one below optimized quorum = %d, want 2", got)
	}
	if inst.phase != phasePreAccept {
		t.Fatalf("phase one below optimized fast quorum = %d, want preaccept", inst.phase)
	}
	if rn.HasReady() {
		t.Fatalf("one below optimized fast quorum emitted ready work: %#v", rn.Ready())
	}

	stepMatchingPreAcceptResp(3)
	if got, fast := len(inst.preOK), rn.q.fastQuorum(); got != fast {
		t.Fatalf("preaccept votes at optimized fast quorum = %d, want %d", got, fast)
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
		if msg.Ref != ref || msg.RecordStatus != StatusCommitted {
			t.Fatalf("optimized five-node fast commit to %d = %#v", to, msg)
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
	if err := rn.Step(Message{
		Type:             MsgPreAcceptResp,
		From:             2,
		To:               1,
		Ref:              ref,
		Ballot:           inst.rec.Ballot,
		Seq:              inst.rec.Seq,
		Deps:             append([]InstanceNum(nil), inst.rec.Deps...),
		RecordStatus:     StatusPreAccepted,
		FastPathEligible: true,
	}); err != nil {
		t.Fatalf("step matching five-node preaccept response: %v", err)
	}
	divergentDeps := append([]InstanceNum(nil), inst.rec.Deps...)
	if err := rn.Step(Message{
		Type:         MsgPreAcceptResp,
		From:         3,
		To:           1,
		Ref:          ref,
		Ballot:       inst.rec.Ballot,
		Seq:          inst.rec.Seq + 1,
		Deps:         divergentDeps,
		RecordStatus: StatusPreAccepted,
	}); err != nil {
		t.Fatalf("step divergent five-node preaccept response: %v", err)
	}

	if got, fast := len(inst.preOK), rn.q.fastQuorum(); got != fast {
		t.Fatalf("preaccept votes at divergent optimized fast threshold = %d, want %d", got, fast)
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
		if msg.Ref != ref || msg.RecordStatus != StatusAccepted || msg.Seq != 2 {
			t.Fatalf("divergent optimized quorum accept to %d = %#v", to, msg)
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
	if got := len(inst.preOK); got != 2 {
		t.Fatalf("preaccept votes one below evidence-gated fast quorum = %d, want 2", got)
	}
	if rn.HasReady() {
		t.Fatalf("one matching response without dependency evidence emitted ready work: %#v", rn.Ready())
	}

	stepFiveNodeMatchingPreAcceptResp(t, rn, inst, ref, 3, 0)
	if got, fast := len(inst.preOK), rn.q.fastQuorum(); got != fast {
		t.Fatalf("preaccept votes at evidence-gated fast quorum = %d, want %d", got, fast)
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

	if got, fast := len(inst.preOK), rn.q.fastQuorum(); got != fast {
		t.Fatalf("preaccept votes with dependency evidence = %d, want optimized fast quorum %d", got, fast)
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
		if msg.Ref != ref || msg.RecordStatus != StatusCommitted || !instanceNumsEqual(msg.Deps, rec.Deps) {
			t.Fatalf("evidence-gated commit message to %d = %#v, want committed attrs deps %v", to, msg, rec.Deps)
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
			if got := inst.preOK[1].depsCommitted; got&(uint64(1)<<1) != 0 {
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
	if err := rn.Step(Message{
		Type:             MsgPreAcceptResp,
		From:             from,
		To:               1,
		Ref:              ref,
		Ballot:           inst.rec.Ballot,
		Seq:              inst.rec.Seq,
		Deps:             append([]InstanceNum(nil), inst.rec.Deps...),
		RecordStatus:     StatusPreAccepted,
		FastPathEligible: true,
		DepsCommitted:    depsCommitted,
	}); err != nil {
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
		if msg.Ref != ref || msg.RecordStatus != StatusAccepted || msg.Seq != attrs.Seq || !instanceNumsEqual(msg.Deps, attrs.Deps) {
			t.Fatalf("dependency-evidence accept message to %d = %#v, want accepted attrs seq %d deps %v", to, msg, attrs.Seq, attrs.Deps)
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
	ref, err := rn.Propose(Command{ID: CommandID{Client: 21, Sequence: 1}, Payload: []byte("process-at-local"), ConflictKeys: [][]byte{[]byte("process-at-local-key")}})
	if err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != ref || rd.Records[0].Status != StatusPreAccepted {
		t.Fatalf("initial ready records = %#v, want pre-accepted local record for %s", rd.Records, ref)
	}
	requirePreAcceptProcessAt(t, rd.Messages, ref, 5, []ReplicaID{2, 3})
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)

	rn.Tick()
	if rn.HasReady() {
		t.Fatalf("retry fired before retry deadline: %#v", rn.Ready())
	}
	rn.Tick()
	retry := rn.Ready()
	if len(retry.Records) != 0 || len(retry.Committed) != 0 {
		t.Fatalf("preaccept retry changed durable/application work: records=%#v committed=%#v", retry.Records, retry.Committed)
	}
	requirePreAcceptProcessAt(t, retry.Messages, ref, 5, []ReplicaID{2, 3})
}

func TestInboundFuturePreAcceptTimingWaitsForProcessAt(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	msg := processAtPreAccept(ref, 1, 2, 3, 3, Command{ID: CommandID{Client: 22, Sequence: 1}, Payload: []byte("delayed"), ConflictKeys: [][]byte{[]byte("delayed-key")}})

	if err := rn.Step(msg); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatalf("future preaccept produced ready work before ProcessAt: %#v", rn.Ready())
	}
	for tick := uint64(1); tick < msg.ProcessAt; tick++ {
		rn.Tick()
		if rn.HasReady() {
			t.Fatalf("future preaccept produced ready work at tick %d before ProcessAt %d: %#v", tick, msg.ProcessAt, rn.Ready())
		}
	}

	rn.Tick()
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
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Reject: true, RejectHint: ballot}); err != nil {
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
	rn.Tick()
	if rn.HasReady() {
		t.Fatalf("preaccepts due at tick 2 produced ready at tick 1: %#v", rn.Ready())
	}

	rn.Tick()
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

	rn.Tick()
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

	rn.Tick()
	if rn.HasReady() {
		t.Fatalf("deferred preaccept produced ready before ProcessAt: %#v", rn.Ready())
	}
	rn.Tick()
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
	ref, err := rn.Propose(Command{ID: CommandID{Client: 45, Sequence: 1}, Payload: []byte("stale-originator"), ConflictKeys: [][]byte{[]byte("stale-originator-key")}})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	requirePreAcceptProcessAt(t, initial.Messages, ref, 5, []ReplicaID{2, 3})
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
		resp := Message{
			Type:             MsgPreAcceptResp,
			From:             from,
			To:               1,
			Ref:              ref,
			Ballot:           inst.rec.Ballot,
			Seq:              remoteAttrs.Seq,
			Deps:             append([]InstanceNum(nil), remoteAttrs.Deps...),
			RecordStatus:     StatusPreAccepted,
			FastPathEligible: true,
		}
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
	for _, msg := range rd.Messages {
		if msg.Type != MsgCommit || msg.Ref != ref || msg.RecordStatus != StatusCommitted {
			t.Fatalf("unanimous timed reply message = %#v, want commit for %s", msg, ref)
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
	rn.executed[conflictingRef] = struct{}{}

	ref, err := rn.Propose(Command{ID: CommandID{Client: 62, Sequence: 1}, Payload: []byte("timed-originator"), ConflictKeys: [][]byte{key}})
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	requirePreAcceptProcessAt(t, initial.Messages, ref, 5, []ReplicaID{2, 3})
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
	rn.executed[remoteOnlyRef] = struct{}{}

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
	if err := rn.Step(Message{
		Type:             MsgPreAcceptResp,
		From:             from,
		To:               1,
		Ref:              ref,
		Ballot:           inst.rec.Ballot,
		Seq:              attrs.Seq,
		Deps:             append([]InstanceNum(nil), attrs.Deps...),
		RecordStatus:     StatusPreAccepted,
		FastPathEligible: true,
	}); err != nil {
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
		if msg.Ref != ref || msg.RecordStatus != StatusAccepted || msg.Seq != attrs.Seq || !instanceNumsEqual(msg.Deps, attrs.Deps) {
			t.Fatalf("timed slow-accept message to %d = %#v, want accepted attrs seq %d deps %v", to, msg, attrs.Seq, attrs.Deps)
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
		if msg.Ref != ref || msg.RecordStatus != StatusCommitted || msg.Seq != remoteSuperset.Seq || !instanceNumsEqual(msg.Deps, remoteSuperset.Deps) {
			t.Fatalf("strict-superset commit message to %d = %#v, want committed attrs seq %d deps %v", to, msg, remoteSuperset.Seq, remoteSuperset.Deps)
		}
	}
}

func TestPreAcceptTimingRequiresUnanimousRemoteSuperset(t *testing.T) {
	rn, inst, ref := proposeTimedCommandWithPriorConflict(t)
	remoteSuperset := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 1, 1}}
	remoteOriginatorOnly := Attributes{Seq: inst.rec.Seq + 1, Deps: []InstanceNum{0, 1, 0}}
	stepTimedPreAcceptResp(t, rn, inst, ref, 2, remoteSuperset)
	stepTimedPreAcceptResp(t, rn, inst, ref, 3, remoteOriginatorOnly)

	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("phase/status after non-unanimous timed replies = %d/%s, want slow-path accept/accepted", inst.phase, inst.rec.Status)
	}
	requireTimedSlowAccept(t, rn.Ready(), ref, remoteSuperset)
}

func TestPreAcceptFastCommitRejectsStaleOriginatorAttrsWithoutTimingOrDefaultBallot(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		mut  func(*instance)
	}{
		{
			name: "missing process-at",
			cfg:  Config{ID: 1, Voters: makeIDs(3), RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 5},
			mut: func(inst *instance) {
				inst.processAt = 0
			},
		},
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
				resp := Message{
					Type:             MsgPreAcceptResp,
					From:             from,
					To:               1,
					Ref:              ref,
					Ballot:           inst.rec.Ballot,
					Seq:              remoteAttrs.Seq,
					Deps:             append([]InstanceNum(nil), remoteAttrs.Deps...),
					RecordStatus:     StatusPreAccepted,
					FastPathEligible: true,
				}
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
				if msg.Ref != ref || msg.RecordStatus != StatusAccepted || msg.Seq != remoteAttrs.Seq || !instanceNumsEqual(msg.Deps, remoteAttrs.Deps) {
					t.Fatalf("guarded stale-originator accept to %d = %#v, want accepted attrs seq %d deps %v", to, msg, remoteAttrs.Seq, remoteAttrs.Deps)
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

func requirePreAcceptProcessAt(t *testing.T, messages []Message, ref InstanceRef, processAt uint64, wantTo []ReplicaID) {
	t.Helper()
	if len(messages) != len(wantTo) {
		t.Fatalf("preaccept messages = %#v, want %d messages", messages, len(wantTo))
	}
	gotTo := make([]ReplicaID, len(messages))
	for i, msg := range messages {
		if msg.Type != MsgPreAccept || msg.From != ref.Replica || msg.Ref != ref || msg.ProcessAt != processAt {
			t.Fatalf("preaccept message %d = %#v, want ProcessAt %d for %s from %d", i, msg, processAt, ref, ref.Replica)
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
	inst.preOK = nil
	resp := Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Seq: inst.rec.Seq, Deps: inst.rec.Deps}
	rn.handlePreAcceptResp(resp)
	before := len(inst.preOK)
	rn.handlePreAcceptResp(resp)
	if len(inst.preOK) != before {
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
		rn.startPrepare(inst)
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
		rn.handlePrepareResp(resp)
		if prep.phase != phasePrepare {
			t.Fatalf("single prepare response formed quorum: phase=%d", prep.phase)
		}
		votes := len(prep.prepareOK)
		rn.handlePrepareResp(resp)
		if prep.phase != phasePrepare || len(prep.prepareOK) != votes {
			t.Fatalf("duplicate prepare response changed recovery state: phase=%d votes=%d want phase=%d votes=%d", prep.phase, len(prep.prepareOK), phasePrepare, votes)
		}
		resp.From = 3
		resp.Seq = 3
		resp.Deps = []InstanceNum{0, 0, 3, 0, 0}
		resp.Command = Command{Payload: []byte("accepted-3")}
		rn.handlePrepareResp(resp)
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
		rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: committedRef, RecordStatus: StatusCommitted, Ballot: ballot, RecordBallot: Ballot{Replica: 2}, Seq: 4, Deps: []InstanceNum{0, 0, 4}, Command: committedCommand})
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
		rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: prepRef, RecordStatus: StatusAccepted, Ballot: ballot, RecordBallot: Ballot{Replica: 2}, Seq: 4, Deps: []InstanceNum{0, 7, 0}, Command: acceptedCommand})
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
		rn.handlePrepareResp(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: nonOwnerRef, RecordStatus: StatusNone, Ballot: ballot, Deps: rn.q.deps()})
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
		rn.instances[rec.Ref] = &instance{rec: rec, phase: phaseCommitted}
		rn.indexConflicts(rec)
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
	advanceOK(t, rn, executedReady)

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
