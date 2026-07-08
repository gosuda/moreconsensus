package epaxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestValueHelpersAndStrings(t *testing.T) {
	for typ := MsgPreAccept; typ <= MsgPrepareResp; typ++ {
		if typ.String() == "unknown" {
			t.Fatalf("message type %d unknown", typ)
		}
	}
	if MessageType(99).String() != "unknown" {
		t.Fatal("unknown message type string")
	}
	for st := StatusNone; st <= StatusExecuted; st++ {
		if st.String() == "unknown" {
			t.Fatalf("status %d unknown", st)
		}
	}
	if Status(99).String() != "unknown" {
		t.Fatal("unknown status string")
	}
	if !((InstanceRef{}).IsZero()) || (InstanceRef{Replica: 1}).IsZero() {
		t.Fatal("zero ref detection failed")
	}
	if (InstanceRef{Replica: 1, Instance: 2, Conf: 3}).String() != "1.2@3" {
		t.Fatal("ref string failed")
	}
	if !(Ballot{Epoch: 1}).Less(Ballot{Epoch: 2}) || !(Ballot{Epoch: 2, Number: 1}).Less(Ballot{Epoch: 2, Number: 2}) || !(Ballot{Epoch: 2, Number: 2, Replica: 1}).Less(Ballot{Epoch: 2, Number: 2, Replica: 2}) {
		t.Fatal("ballot less failed")
	}
	if (Ballot{Number: 7}).Next(3) != (Ballot{Number: 8, Replica: 3}) {
		t.Fatal("ballot next failed")
	}
	userA := Command{Payload: []byte("a"), ConflictKeys: [][]byte{[]byte("x")}}
	userB := Command{Payload: []byte("b"), ConflictKeys: [][]byte{[]byte("x")}}
	userC := Command{Payload: []byte("c"), ConflictKeys: [][]byte{[]byte("y")}}
	if !userA.ConflictsWith(userB) || userA.ConflictsWith(userC) {
		t.Fatal("user conflict failed")
	}
	if userA.ConflictsWith(Command{Kind: CommandNoop}) {
		t.Fatal("noop conflict failed")
	}
	if !userA.ConflictsWith(Command{Kind: CommandConfChange}) {
		t.Fatal("conf change conflict failed")
	}
	borrowed := userA.Borrow()
	if !bytes.Equal(borrowed.Payload, userA.Payload) {
		t.Fatal("borrow changed command")
	}
	rd := Ready{Records: []InstanceRecord{{}}, Messages: []Message{{Deps: []InstanceNum{1}}}, Committed: []CommittedCommand{{}}, MustSync: true}
	rd.Release()
	if !rd.Empty() || rd.MustSync {
		t.Fatal("ready release failed")
	}
	if (Attributes{Seq: 1, Deps: []InstanceNum{1}}).Equal(Attributes{Seq: 2, Deps: []InstanceNum{1}}) {
		t.Fatal("attributes equality sequence failed")
	}
	if (Attributes{Seq: 1, Deps: []InstanceNum{1}}).Equal(Attributes{Seq: 1, Deps: []InstanceNum{1, 2}}) {
		t.Fatal("attributes equality length failed")
	}
}

func TestConfigValidationAndMessageValidation(t *testing.T) {
	invalids := []Config{
		{ID: 1, Voters: nil},
		{ID: 1, Voters: []ReplicaID{1, 1}},
		{ID: 1, Voters: []ReplicaID{0}},
		{ID: 9, Voters: []ReplicaID{1, 2, 3}},
		{ID: 1, Voters: []ReplicaID{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	for _, cfg := range invalids {
		if _, err := NewRawNode(cfg); err == nil {
			t.Fatalf("expected invalid config %#v", cfg)
		}
	}
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	base := Message{Type: MsgCommit, From: 1, To: 1, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Deps: []InstanceNum{0, 0, 0}}
	bad := base
	bad.Type = 99
	if err := rn.Step(bad); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("bad type err=%v", err)
	}
	bad = base
	bad.From = 9
	if err := rn.Step(bad); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("unknown sender err=%v", err)
	}
	bad = base
	bad.Deps = nil
	if err := rn.Step(bad); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("bad deps err=%v", err)
	}
	bad = base
	bad.Checksum = [32]byte{1}
	if err := rn.Step(bad); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("bad checksum err=%v", err)
	}
	bad = base
	bad.To = 2
	if err := rn.Step(bad); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("wrong target err=%v", err)
	}
}

func TestMessageValidateRequiresDependencyVectorWidthForAttributes(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}

	attrMessages := []MessageType{
		MsgPreAccept,
		MsgPreAcceptResp,
		MsgAccept,
		MsgCommit,
		MsgPrepareResp,
	}
	for _, typ := range attrMessages {
		for _, tc := range []struct {
			name string
			deps []InstanceNum
		}{
			{name: "short", deps: []InstanceNum{0, 0}},
			{name: "overwide", deps: []InstanceNum{0, 0, 0, 0}},
		} {
			t.Run(typ.String()+"/"+tc.name, func(t *testing.T) {
				msg := Message{Type: typ, From: 1, To: 2, Ref: ref, Deps: tc.deps}
				if err := msg.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("Validate err=%v, want %v", err, ErrInvalidMessage)
				}
			})
		}
	}
}

func TestMessageValidateDependencyWidthExemptions(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}

	for _, tc := range []struct {
		name string
		msg  Message
	}{
		{
			name: "prepare short deps",
			msg:  Message{Type: MsgPrepare, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0}},
		},
		{
			name: "prepare overwide deps",
			msg:  Message{Type: MsgPrepare, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0, 0, 0}},
		},
		{
			name: "accept response short deps",
			msg:  Message{Type: MsgAcceptResp, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0}},
		},
		{
			name: "accept response overwide deps",
			msg:  Message{Type: MsgAcceptResp, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0, 0, 0}},
		},
		{
			name: "preaccept reject short deps",
			msg:  Message{Type: MsgPreAcceptResp, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0}, Reject: true},
		},
		{
			name: "preaccept reject overwide deps",
			msg:  Message{Type: MsgPreAcceptResp, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0, 0, 0}, Reject: true},
		},
		{
			name: "prepare reject short deps",
			msg:  Message{Type: MsgPrepareResp, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0}, Reject: true},
		},
		{
			name: "prepare reject overwide deps",
			msg:  Message{Type: MsgPrepareResp, From: 1, To: 2, Ref: ref, Deps: []InstanceNum{0, 0, 0, 0}, Reject: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.msg.Validate(conf); err != nil {
				t.Fatalf("Validate err=%v, want nil", err)
			}
		})
	}
}

func TestStorageEdgeCases(t *testing.T) {
	st := NewMemoryStorage()
	st.Configs = []ConfState{{ID: 2, Voters: makeIDs(3)}}
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: st})
	if err != nil {
		t.Fatal(err)
	}
	if rn.Status().Conf.ID != 2 {
		t.Fatal("loaded config history was ignored")
	}
	rec := InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0}, Command: Command{Payload: []byte("x")}}
	rec.Checksum = ChecksumRecord(rec)
	st.Records[rec.Ref] = rec
	got, ok := st.Instance(rec.Ref)
	if !ok || !VerifyRecordChecksum(got) {
		t.Fatal("instance lookup failed")
	}
	got.Command.Payload[0] = 'y'
	again, _ := st.Instance(rec.Ref)
	if string(again.Command.Payload) != "x" {
		t.Fatal("instance lookup did not clone")
	}
	st.Records[rec.Ref] = InstanceRecord{Ref: rec.Ref, Checksum: [32]byte{1}}
	if err := st.LoadInstances(func(InstanceRecord) error { return nil }); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("checksum mismatch err=%v", err)
	}
	st.Records[rec.Ref] = rec
	sentinel := errors.New("sentinel")
	if err := st.LoadInstances(func(InstanceRecord) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("callback err=%v", err)
	}
	nilMap := &MemoryStorage{}
	if err := nilMap.ApplyReady(Ready{Records: []InstanceRecord{rec}}); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareRecoveryPath(t *testing.T) {
	s := newSimCluster(t, 3, false)
	ref, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	inst := s.nodes[1].instances[ref]
	s.nodes[1].startPrepare(inst)
	s.drain()
	if len(s.apps[1]) != 1 {
		t.Fatalf("prepare path did not commit locally: %d", len(s.apps[1]))
	}
}

func TestRejectPathsAndTimers(t *testing.T) {
	s := newSimCluster(t, 3, false)
	ref, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := s.nodes[1].Ready()
	if len(rd.Messages) == 0 {
		t.Fatal("expected preaccept messages")
	}
	msg := rd.Messages[0]
	if err := s.nodes[msg.To].Step(msg); err != nil {
		t.Fatal(err)
	}
	remote := s.nodes[msg.To].instances[ref]
	remote.rec.Ballot = Ballot{Number: 9, Replica: msg.To}
	lowAccept := Message{Type: MsgAccept, From: 1, To: msg.To, Ref: ref, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: []InstanceNum{0, 0, 0}, Command: Command{Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}}}
	lowAccept.Checksum = ChecksumMessage(lowAccept)
	if err := s.nodes[msg.To].Step(lowAccept); err != nil {
		t.Fatal(err)
	}
	if err := s.nodes[1].Advance(rd); err != nil {
		t.Fatal(err)
	}
	inst := s.nodes[1].instances[ref]
	s.nodes[1].onTimer(inst, timerPreAccept)
	s.nodes[1].startAccept(inst, inst.rec.Attributes())
	s.nodes[1].onTimer(inst, timerAccept)
	s.nodes[1].startPrepare(inst)
	s.nodes[1].onTimer(inst, timerPrepare)
	s.nodes[1].onTimer(inst, timerFastWait)
}

func TestEncodeMessageRejectsWireLimitOverflowWithoutAppending(t *testing.T) {
	base := Message{
		Type: MsgCommit,
		From: 1,
		To:   2,
		Ref:  InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Deps: []InstanceNum{0},
	}
	tests := []struct {
		name string
		msg  Message
	}{
		{
			name: "too many deps",
			msg: Message{
				Type: MsgCommit,
				From: 1,
				To:   2,
				Ref:  InstanceRef{Replica: 1, Instance: 1, Conf: 1},
				Deps: make([]InstanceNum, maxWireDeps+1),
			},
		},
		{
			name: "too many conflict keys",
			msg: func() Message {
				msg := base
				msg.Command.ConflictKeys = make([][]byte, maxWireConflictKeys+1)
				return msg
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backing := []byte("caller-prefix|capacity-canary")
			dst := backing[:len("caller-prefix")]
			wantBacking := append([]byte(nil), backing...)

			got, err := EncodeMessage(dst, tc.msg)
			if !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("EncodeMessage err=%v, want %v", err, ErrInvalidMessage)
			}
			if !bytes.Equal(got, dst) {
				t.Fatalf("EncodeMessage returned dst %q, want unchanged %q", got, dst)
			}
			if !bytes.Equal(backing, wantBacking) {
				t.Fatalf("EncodeMessage modified caller backing: got %q want %q", backing, wantBacking)
			}
		})
	}
}

func malformedCodecFrame(deps, conflictKeys uint64) []byte {
	buf := append([]byte(nil), wireMagic[:]...)
	for _, v := range []uint64{
		uint64(MsgCommit),
		1,
		1,
		1,
		1,
		1,
		0,
		0,
		0,
		0,
		deps,
	} {
		buf = binary.AppendUvarint(buf, v)
	}
	if deps <= maxWireDeps {
		for range deps {
			buf = binary.AppendUvarint(buf, 0)
		}
		for _, v := range []uint64{
			0,
			0,
			uint64(CommandNoop),
			0,
			conflictKeys,
		} {
			buf = binary.AppendUvarint(buf, v)
		}
		if conflictKeys <= maxWireConflictKeys {
			for range conflictKeys {
				buf = binary.AppendUvarint(buf, 0)
			}
		}
		buf = append(buf, 0)
		for range 4 {
			buf = binary.AppendUvarint(buf, 0)
		}
	}
	return append(buf, make([]byte, 32)...)
}

func TestCodecMalformedBranches(t *testing.T) {
	var out Message
	if err := DecodeMessage([]byte{'M', 'E', 'P', '1', 0xff}, &out); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("bad varint err=%v", err)
	}
	buf := malformedCodecFrame(maxWireDeps+1, 0)
	if err := DecodeMessage(buf, &out); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("too many deps err=%v", err)
	}
	buf = malformedCodecFrame(0, maxWireConflictKeys+1)
	if err := DecodeMessage(buf, &out); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("too many keys err=%v", err)
	}
	truncated := append([]byte(nil), wireMagic[:]...)
	truncated = append(truncated, make([]byte, 32)...)
	if err := DecodeMessage(truncated, &out); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("truncated err=%v", err)
	}
}
