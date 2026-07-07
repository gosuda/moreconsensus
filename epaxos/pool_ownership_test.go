package epaxos

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestPoolClearsOwnedReferencesAndReusesWarmObjects(t *testing.T) {
	payload := []byte("payload-bytes")
	deps := []InstanceNum{1, 2, 3}
	keys := [][]byte{[]byte("alpha"), []byte("beta")}

	m := GetMessage()
	*m = Message{
		Type: MsgCommit,
		From: 1,
		To:   2,
		Ref:  InstanceRef{Replica: 1, Instance: 9, Conf: 1},
		Deps: deps,
		Command: Command{
			ID:           CommandID{Client: 7, Sequence: 8},
			Kind:         CommandConfChange,
			Payload:      payload,
			ConflictKeys: keys,
		},
	}
	PutMessage(m)
	gotMessage := GetMessage()
	assertMessageCleared(t, gotMessage)
	PutMessage(gotMessage)

	c := GetCommand()
	*c = Command{ID: CommandID{Client: 11, Sequence: 12}, Kind: CommandConfChange, Payload: payload, ConflictKeys: keys}
	PutCommand(c)
	gotCommand := GetCommand()
	assertCommandCleared(t, *gotCommand)
	PutCommand(gotCommand)

	messageAllocs := testing.AllocsPerRun(1000, func() {
		msg := GetMessage()
		PutMessage(msg)
	})
	if messageAllocs != 0 {
		t.Fatalf("warm GetMessage/PutMessage allocations = %v, want 0", messageAllocs)
	}

	commandAllocs := testing.AllocsPerRun(1000, func() {
		cmd := GetCommand()
		PutCommand(cmd)
	})
	if commandAllocs != 0 {
		t.Fatalf("warm GetCommand/PutCommand allocations = %v, want 0", commandAllocs)
	}
}

func TestReadyReleaseClearsHeadersAndBackingArraysWithoutAllocation(t *testing.T) {
	payload := []byte("ready-payload")
	deps := []InstanceNum{4, 5, 6}
	keys := [][]byte{[]byte("ready-key")}
	records := []InstanceRecord{{
		Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Status:  StatusCommitted,
		Seq:     10,
		Deps:    deps,
		Command: Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: payload, ConflictKeys: keys},
	}}
	messages := []Message{{
		Type:    MsgCommit,
		From:    1,
		To:      2,
		Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Seq:     10,
		Deps:    deps,
		Command: Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: payload, ConflictKeys: keys},
	}}
	committed := []CommittedCommand{{
		Ref:     InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Seq:     10,
		Deps:    deps,
		Command: Command{ID: CommandID{Client: 3, Sequence: 1}, Payload: payload, ConflictKeys: keys},
	}}
	rd := Ready{Records: records, Messages: messages, Committed: committed, MustSync: true}
	rd.Release()
	if rd.Records != nil || rd.Messages != nil || rd.Committed != nil || rd.MustSync {
		t.Fatalf("released Ready kept headers or sync bit: %#v", rd)
	}
	assertCommandCleared(t, records[0].Command)
	if records[0].Deps != nil {
		t.Fatalf("released record retained deps: %#v", records[0].Deps)
	}
	assertMessageCleared(t, &messages[0])
	assertCommandCleared(t, committed[0].Command)
	if committed[0].Deps != nil {
		t.Fatalf("released committed command retained deps: %#v", committed[0].Deps)
	}

	allocRecords := make([]InstanceRecord, 1)
	allocMessages := make([]Message, 1)
	allocCommitted := make([]CommittedCommand, 1)
	allocs := testing.AllocsPerRun(1000, func() {
		allocRecords[0] = InstanceRecord{Deps: deps, Command: Command{Payload: payload, ConflictKeys: keys}}
		allocMessages[0] = Message{Deps: deps, Command: Command{Payload: payload, ConflictKeys: keys}}
		allocCommitted[0] = CommittedCommand{Deps: deps, Command: Command{Payload: payload, ConflictKeys: keys}}
		ready := Ready{Records: allocRecords[:], Messages: allocMessages[:], Committed: allocCommitted[:], MustSync: true}
		ready.Release()
	})
	if allocs != 0 {
		t.Fatalf("Ready.Release allocations = %v, want 0", allocs)
	}
}

func FuzzDecodeMessage(f *testing.F) {
	for _, seed := range decodeMessageSeedCorpus() {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var out Message
		err := DecodeMessage(data, &out)
		if err != nil {
			if !errors.Is(err, ErrInvalidMessage) && !errors.Is(err, ErrChecksumMismatch) {
				t.Fatalf("DecodeMessage error = %v, want typed codec error", err)
			}
			return
		}

		encoded, err := EncodeMessage(nil, out)
		if err != nil {
			t.Fatalf("re-encode decoded message: %v", err)
		}
		var round Message
		if err := DecodeMessage(encoded, &round); err != nil {
			t.Fatalf("round-trip decode: %v", err)
		}
		if !reflect.DeepEqual(out, round) {
			t.Fatalf("round-trip message mismatch\noriginal: %#v\nround:    %#v", out, round)
		}
	})
}

func TestDecodeMessageClearsDestinationOnPartialDecodeErrors(t *testing.T) {
	base := Message{
		Type:   MsgCommit,
		From:   1,
		To:     2,
		Ref:    InstanceRef{Replica: 1, Instance: 7, Conf: 1},
		Ballot: Ballot{Epoch: 2, Number: 3, Replica: 1},
		Seq:    11,
		Deps:   []InstanceNum{4, 5},
		Command: Command{
			ID:           CommandID{Client: 9, Sequence: 10},
			Payload:      []byte("partial-decode-payload"),
			ConflictKeys: [][]byte{[]byte("partial-decode-key")},
		},
		RejectHint:   Ballot{Epoch: 3, Number: 4, Replica: 2},
		RecordStatus: StatusCommitted,
	}
	valid := mustEncodeMessageSeed(base)
	corruptChecksum := append([]byte(nil), valid...)
	corruptChecksum[len(corruptChecksum)-1] ^= 0x80

	invalidAfterCommand := append([]byte(nil), wireMagic[:]...)
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(base.Type))
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(base.From))
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(base.To))
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(base.Ref.Replica))
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(base.Ref.Instance))
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(base.Ref.Conf))
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, base.Ballot.Epoch)
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, base.Ballot.Number)
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(base.Ballot.Replica))
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, base.Seq)
	invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(len(base.Deps)))
	for _, dep := range base.Deps {
		invalidAfterCommand = binary.AppendUvarint(invalidAfterCommand, uint64(dep))
	}
	invalidAfterCommand = appendCommand(invalidAfterCommand, base.Command)
	invalidAfterCommand = append(invalidAfterCommand, make([]byte, 32)...)

	for _, tc := range []struct {
		name string
		data []byte
		want error
	}{
		{name: "checksum corrupted after complete decode", data: corruptChecksum, want: ErrChecksumMismatch},
		{name: "invalid after parsed payload and keys", data: invalidAfterCommand, want: ErrInvalidMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := Message{
				Type:    MsgAccept,
				From:    9,
				To:      8,
				Ref:     InstanceRef{Replica: 7, Instance: 6, Conf: 5},
				Ballot:  Ballot{Epoch: 4, Number: 3, Replica: 2},
				Seq:     1,
				Deps:    []InstanceNum{99},
				Command: Command{
					ID:           CommandID{Client: 7, Sequence: 8},
					Payload:      []byte("previous-payload"),
					ConflictKeys: [][]byte{[]byte("previous-key")},
				},
			}
			if err := DecodeMessage(tc.data, &out); !errors.Is(err, tc.want) {
				t.Fatalf("DecodeMessage error = %v, want %v", err, tc.want)
			}
			assertMessageCleared(t, &out)
		})
	}
}

func decodeMessageSeedCorpus() [][]byte {
	base := Message{
		Type:   MsgCommit,
		From:   1,
		To:     2,
		Ref:    InstanceRef{Replica: 1, Instance: 7, Conf: 1},
		Ballot: Ballot{Epoch: 1, Number: 2, Replica: 1},
		Seq:    3,
		Deps:   []InstanceNum{0, 4, 5},
		Command: Command{
			ID:           CommandID{Client: 9, Sequence: 10},
			Payload:      []byte("payload"),
			ConflictKeys: [][]byte{[]byte("alpha"), []byte("beta")},
		},
		RecordStatus: StatusCommitted,
	}
	valid := mustEncodeMessageSeed(base)
	mutatedChecksum := append([]byte(nil), valid...)
	mutatedChecksum[len(mutatedChecksum)-1] ^= 0x80
	mutatedBody := append([]byte(nil), valid...)
	mutatedBody[len(wireMagic)+1] ^= 0x40

	maxDeps := base
	maxDeps.Deps = make([]InstanceNum, 128)
	for i := range maxDeps.Deps {
		maxDeps.Deps[i] = InstanceNum(i)
	}

	maxKeys := base
	maxKeys.Command.ConflictKeys = make([][]byte, 128)
	for i := range maxKeys.Command.ConflictKeys {
		maxKeys.Command.ConflictKeys[i] = []byte{byte(i), byte(127 - i)}
	}

	tooManyDeps := base
	tooManyDeps.Deps = make([]InstanceNum, 129)
	tooManyKeys := base
	tooManyKeys.Command.ConflictKeys = make([][]byte, 129)

	seeds := [][]byte{
		valid,
		mustEncodeMessageSeed(maxDeps),
		mustEncodeMessageSeed(maxKeys),
		mustEncodeMessageSeed(tooManyDeps),
		mustEncodeMessageSeed(tooManyKeys),
		valid[:len(valid)-1],
		valid[:len(wireMagic)],
		valid[:len(wireMagic)+32],
		mutatedChecksum,
		mutatedBody,
		nil,
		{},
		{0},
		{'M'},
		{'M', 'E', 'P'},
		{'M', 'E', 'P', '1'},
		{0xff, 0xfe, 0xfd, 0xfc},
		{3, 1, 4, 1, 5, 9},
		{'M', 'E', 'P', '1', 0xff},
	}
	return append(seeds, deterministicShortByteSeeds()...)
}

func deterministicShortByteSeeds() [][]byte {
	state := uint32(0x9e3779b9)
	seeds := make([][]byte, 0, 16)
	for n := 1; n <= 16; n++ {
		seed := make([]byte, n)
		for i := range seed {
			state = state*1664525 + 1013904223
			seed[i] = byte(state >> 24)
		}
		seeds = append(seeds, seed)
	}
	return seeds
}

func mustEncodeMessageSeed(m Message) []byte {
	encoded, err := EncodeMessage(nil, m)
	if err != nil {
		panic(err)
	}
	return encoded
}

func assertMessageCleared(t *testing.T, m *Message) {
	t.Helper()
	if m.Type != 0 || m.From != 0 || m.To != 0 || !m.Ref.IsZero() || m.Ballot != (Ballot{}) || m.Seq != 0 || len(m.Deps) != 0 || m.Reject || m.RejectHint != (Ballot{}) || m.RecordStatus != 0 {
		t.Fatalf("message retained metadata: %#v", m)
	}
	assertCommandCleared(t, m.Command)
	if m.Deps != nil {
		t.Fatalf("message retained deps backing storage: %#v", m.Deps)
	}
	if m.Checksum != ([32]byte{}) {
		t.Fatalf("message retained checksum: %#v", m.Checksum)
	}
}

func assertCommandCleared(t *testing.T, c Command) {
	t.Helper()
	if c.ID != (CommandID{}) || c.Kind != CommandUser || c.Payload != nil || c.ConflictKeys != nil {
		t.Fatalf("command retained caller-owned data: %#v", c)
	}
}
