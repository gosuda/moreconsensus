package epaxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

var (
	allocationChecksumSink [32]byte
	allocationBytesSink    []byte
	allocationRefSink      InstanceRef
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

func TestEncodeMessageWithPresizedDestinationHasNoAllocation(t *testing.T) {
	msg := allocationTestMessage()
	encoded, err := EncodeMessage(nil, msg)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, len(encoded))
	backing := dst[:cap(dst)]

	allocs := testing.AllocsPerRun(1000, func() {
		out, err := EncodeMessage(dst[:0], msg)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != len(encoded) {
			t.Fatalf("encoded length = %d, want %d", len(out), len(encoded))
		}
		if len(out) == 0 || &out[0] != &backing[0] {
			t.Fatal("EncodeMessage did not append into caller-owned destination")
		}
		allocationBytesSink = out
	})
	if allocs != 0 {
		t.Fatalf("EncodeMessage allocations with pre-sized destination = %v, want 0", allocs)
	}
}

func TestChecksumRecordAndMessageReuseWarmHasherWithoutAllocation(t *testing.T) {
	msg := allocationTestMessage()
	rec := allocationTestRecord(msg)

	tests := []struct {
		name string
		run  func()
	}{
		{
			name: "record checksum reuses warm hasher",
			run: func() {
				allocationChecksumSink = ChecksumRecord(rec)
			},
		},
		{
			name: "message checksum reuses warm hasher",
			run: func() {
				allocationChecksumSink = ChecksumMessage(msg)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run()
			allocs := testing.AllocsPerRun(1000, tt.run)
			if allocs != 0 {
				t.Fatalf("%s allocations = %v, want 0", tt.name, allocs)
			}
		})
	}
}

func TestProposeZeroCopyPayloadKeyCommandUsesLowerAllocationBudgetThanSafeCopyCommand(t *testing.T) {
	safeCopyAllocs := proposePayloadKeyCommandAllocs(t, false)
	zeroCopyAllocs := proposePayloadKeyCommandAllocs(t, true)
	if zeroCopyAllocs >= safeCopyAllocs {
		t.Fatalf("zero-copy Propose allocations = %v, want less than safe-copy allocations %v", zeroCopyAllocs, safeCopyAllocs)
	}
	if safeCopyAllocs > 54 {
		t.Fatalf("safe-copy Propose allocations = %v, want <= 54", safeCopyAllocs)
	}
	if zeroCopyAllocs > 48 {
		t.Fatalf("zero-copy Propose allocations = %v, want <= 48", zeroCopyAllocs)
	}
}

func proposePayloadKeyCommandAllocs(t *testing.T, zeroCopy bool) float64 {
	t.Helper()
	payload := []byte("proposal-allocation-payload")
	key := []byte("proposal-allocation-key")
	cmd := Command{ID: CommandID{Client: 31, Sequence: 41}, Payload: payload, ConflictKeys: [][]byte{key}}
	voters := []ReplicaID{1, 2, 3}

	return testing.AllocsPerRun(1000, func() {
		rn, err := NewRawNode(Config{ID: 1, Voters: voters, ZeroCopyProposals: zeroCopy})
		if err != nil {
			t.Fatal(err)
		}
		ref, err := rn.Propose(cmd)
		if err != nil {
			t.Fatal(err)
		}
		allocationRefSink = ref
	})
}

func allocationTestMessage() Message {
	return Message{
		Type:   MsgCommit,
		From:   1,
		To:     2,
		Ref:    InstanceRef{Replica: 1, Instance: 7, Conf: 1},
		Ballot: Ballot{Epoch: 2, Number: 3, Replica: 1},
		Seq:    11,
		Deps:   []InstanceNum{4, 5, 6},
		Command: Command{
			ID:           CommandID{Client: 9, Sequence: 10},
			Payload:      []byte("allocation-payload"),
			ConflictKeys: [][]byte{[]byte("allocation-key-a"), []byte("allocation-key-b")},
		},
		RejectHint:   Ballot{Epoch: 3, Number: 4, Replica: 2},
		RecordStatus: StatusCommitted,
	}
}

func allocationTestRecord(m Message) InstanceRecord {
	return InstanceRecord{
		Ref:     m.Ref,
		Ballot:  m.Ballot,
		Status:  StatusCommitted,
		Seq:     m.Seq,
		Deps:    m.Deps,
		Command: m.Command,
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

func TestDecodeMessageWithNilScratchMatchesDecodeMessage(t *testing.T) {
	valid := mustEncodeMessageSeed(decodeScratchTestMessage())
	corruptChecksum := append([]byte(nil), valid...)
	corruptChecksum[len(corruptChecksum)-1] ^= 0x80
	truncated := append([]byte(nil), valid[:len(valid)-1]...)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "valid message", data: valid},
		{name: "checksum mismatch", data: corruptChecksum},
		{name: "truncated message", data: truncated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want Message
			wantErr := DecodeMessage(tc.data, &want)
			var got Message
			gotErr := DecodeMessageWithScratch(tc.data, &got, nil)
			if (wantErr == nil) != (gotErr == nil) || wantErr != nil && !errors.Is(gotErr, wantErr) {
				t.Fatalf("DecodeMessageWithScratch error = %v, want %v", gotErr, wantErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("nil scratch decode mismatch\nDecodeMessage:            %#v\nDecodeMessageWithScratch: %#v", want, got)
			}
		})
	}
}

func TestDecodeMessageWithScratchUsesPresizedMetadataWithoutAllocation(t *testing.T) {
	input := decodeScratchTestMessage()
	encoded := mustEncodeMessageSeed(input)
	scratch := DecodeScratch{
		Deps:         make([]InstanceNum, 0, len(input.Deps)),
		ConflictKeys: make([][]byte, 0, len(input.Command.ConflictKeys)),
	}
	var out Message

	if err := DecodeMessageWithScratch(encoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}
	assertDecodedScratchMessage(t, out, input)
	if len(out.Deps) == 0 || len(scratch.Deps) == 0 || &out.Deps[0] != &scratch.Deps[0] {
		t.Fatalf("decoded deps did not use scratch storage: deps=%#v scratch=%#v", out.Deps, scratch.Deps)
	}
	if len(out.Command.ConflictKeys) == 0 || len(scratch.ConflictKeys) == 0 || &out.Command.ConflictKeys[0] != &scratch.ConflictKeys[0] {
		t.Fatalf("decoded conflict keys did not use scratch storage: keys=%#v scratch=%#v", out.Command.ConflictKeys, scratch.ConflictKeys)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := DecodeMessageWithScratch(encoded, &out, &scratch); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("DecodeMessageWithScratch allocations with pre-sized scratch = %v, want 0", allocs)
	}
}

func TestDecodeMessageWithScratchGrowsUndersizedMetadataStorage(t *testing.T) {
	input := decodeScratchTestMessage()
	encoded := mustEncodeMessageSeed(input)
	oldDeps := []InstanceNum{99}
	oldKey := []byte("stale-key-reference")
	scratch := DecodeScratch{
		Deps:         oldDeps[:0],
		ConflictKeys: [][]byte{oldKey},
	}
	oldDepSlots := scratch.Deps[:cap(scratch.Deps)]
	oldKeySlots := scratch.ConflictKeys[:cap(scratch.ConflictKeys)]
	var out Message

	if err := DecodeMessageWithScratch(encoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}

	assertDecodedScratchMessage(t, out, input)
	if cap(scratch.Deps) < len(input.Deps) {
		t.Fatalf("scratch deps capacity = %d, want at least decoded deps %d", cap(scratch.Deps), len(input.Deps))
	}
	if cap(scratch.ConflictKeys) < len(input.Command.ConflictKeys) {
		t.Fatalf("scratch conflict key capacity = %d, want at least decoded keys %d", cap(scratch.ConflictKeys), len(input.Command.ConflictKeys))
	}
	if len(out.Deps) == 0 || len(scratch.Deps) == 0 || &out.Deps[0] != &scratch.Deps[0] {
		t.Fatalf("grown decoded deps did not use scratch storage: deps=%#v scratch=%#v", out.Deps, scratch.Deps)
	}
	if len(out.Command.ConflictKeys) == 0 || len(scratch.ConflictKeys) == 0 || &out.Command.ConflictKeys[0] != &scratch.ConflictKeys[0] {
		t.Fatalf("grown decoded conflict keys did not use scratch storage: keys=%#v scratch=%#v", out.Command.ConflictKeys, scratch.ConflictKeys)
	}
	if &scratch.Deps[0] == &oldDepSlots[0] {
		t.Fatalf("undersized deps scratch reused old storage with capacity %d for %d decoded deps", cap(oldDepSlots), len(input.Deps))
	}
	if &scratch.ConflictKeys[0] == &oldKeySlots[0] {
		t.Fatalf("undersized conflict-key scratch reused old storage with capacity %d for %d decoded keys", cap(oldKeySlots), len(input.Command.ConflictKeys))
	}
	if oldKeySlots[0] != nil {
		t.Fatalf("scratch growth retained stale conflict key bytes: %q", oldKeySlots[0])
	}
}

func TestDecodeScratchResetClearsConflictKeyRefsAndKeepsCapacity(t *testing.T) {
	input := decodeScratchTestMessage()
	encoded := mustEncodeMessageSeed(input)
	scratch := DecodeScratch{}
	var out Message

	if err := DecodeMessageWithScratch(encoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}
	depsCap := cap(scratch.Deps)
	keysCap := cap(scratch.ConflictKeys)
	retainedKeySlots := scratch.ConflictKeys[:cap(scratch.ConflictKeys)]
	if len(retainedKeySlots) != len(input.Command.ConflictKeys) {
		t.Fatalf("scratch conflict-key slots = %d, want %d before reset", len(retainedKeySlots), len(input.Command.ConflictKeys))
	}

	scratch.Reset()

	if len(scratch.Deps) != 0 || cap(scratch.Deps) != depsCap {
		t.Fatalf("reset deps len/cap = %d/%d, want 0/%d", len(scratch.Deps), cap(scratch.Deps), depsCap)
	}
	if len(scratch.ConflictKeys) != 0 || cap(scratch.ConflictKeys) != keysCap {
		t.Fatalf("reset conflict keys len/cap = %d/%d, want 0/%d", len(scratch.ConflictKeys), cap(scratch.ConflictKeys), keysCap)
	}
	for i, key := range retainedKeySlots {
		if key != nil {
			t.Fatalf("reset retained conflict key slot %d: %q", i, key)
		}
	}
}

func TestDecodeMessageWithScratchAliasesInputPayloadAndKeyBytes(t *testing.T) {
	input := decodeScratchTestMessage()
	encoded := mustEncodeMessageSeed(input)
	scratch := DecodeScratch{
		Deps:         make([]InstanceNum, 0, len(input.Deps)),
		ConflictKeys: make([][]byte, 0, len(input.Command.ConflictKeys)),
	}
	var out Message
	if err := DecodeMessageWithScratch(encoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}

	payloadOffset := bytes.Index(encoded, input.Command.Payload)
	if payloadOffset < 0 {
		t.Fatal("encoded payload bytes not found")
	}
	keyOffset := bytes.Index(encoded, input.Command.ConflictKeys[0])
	if keyOffset < 0 {
		t.Fatal("encoded conflict key bytes not found")
	}

	encoded[payloadOffset] = 'S'
	encoded[keyOffset] = 'S'
	if !bytes.Equal(out.Command.Payload, []byte("Scratch-payload")) {
		t.Fatalf("decoded payload does not alias input buffer: %q", out.Command.Payload)
	}
	if len(out.Command.ConflictKeys) != len(input.Command.ConflictKeys) || !bytes.Equal(out.Command.ConflictKeys[0], []byte("Scratch-key-a")) {
		t.Fatalf("decoded conflict key does not alias input buffer: %q", out.Command.ConflictKeys)
	}
}

func TestDecodeMessageWithScratchReuseClearsInactiveConflictKeys(t *testing.T) {
	first := decodeScratchTestMessage([]byte("scratch-key-a"), []byte("scratch-key-b"))
	second := decodeScratchTestMessage([]byte("scratch-key-c"))
	firstEncoded := mustEncodeMessageSeed(first)
	secondEncoded := mustEncodeMessageSeed(second)
	scratch := DecodeScratch{
		Deps:         make([]InstanceNum, 0, len(first.Deps)),
		ConflictKeys: make([][]byte, 0, len(first.Command.ConflictKeys)),
	}
	var out Message
	if err := DecodeMessageWithScratch(firstEncoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}
	if len(scratch.ConflictKeys) != 2 {
		t.Fatalf("first decode conflict keys = %d, want 2", len(scratch.ConflictKeys))
	}
	if err := DecodeMessageWithScratch(secondEncoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}
	assertDecodedScratchMessage(t, out, second)
	if len(out.Command.ConflictKeys) != 1 {
		t.Fatalf("second decode conflict keys = %d, want 1", len(out.Command.ConflictKeys))
	}
	retained := scratch.ConflictKeys[:cap(scratch.ConflictKeys)]
	for i := len(out.Command.ConflictKeys); i < len(retained); i++ {
		if retained[i] != nil {
			t.Fatalf("scratch retained inactive conflict key %d: %q", i, retained[i])
		}
	}
}

func TestDecodeMessageWithScratchClearsDestinationOnMalformedInput(t *testing.T) {
	base := decodeScratchTestMessage()
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
			scratch := DecodeScratch{
				Deps:         make([]InstanceNum, 0, len(base.Deps)),
				ConflictKeys: make([][]byte, 0, len(base.Command.ConflictKeys)),
			}
			out := Message{
				Type:   MsgAccept,
				From:   9,
				To:     8,
				Ref:    InstanceRef{Replica: 7, Instance: 6, Conf: 5},
				Ballot: Ballot{Epoch: 4, Number: 3, Replica: 2},
				Seq:    1,
				Deps:   []InstanceNum{99},
				Command: Command{
					ID:           CommandID{Client: 7, Sequence: 8},
					Payload:      []byte("previous-payload"),
					ConflictKeys: [][]byte{[]byte("previous-key")},
				},
			}
			if err := DecodeMessageWithScratch(tc.data, &out, &scratch); !errors.Is(err, tc.want) {
				t.Fatalf("DecodeMessageWithScratch error = %v, want %v", err, tc.want)
			}
			assertMessageCleared(t, &out)
			if len(scratch.Deps) != 0 || len(scratch.ConflictKeys) != 0 {
				t.Fatalf("scratch exposed decoded metadata after error: %#v", scratch)
			}
		})
	}
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
				Type:   MsgAccept,
				From:   9,
				To:     8,
				Ref:    InstanceRef{Replica: 7, Instance: 6, Conf: 5},
				Ballot: Ballot{Epoch: 4, Number: 3, Replica: 2},
				Seq:    1,
				Deps:   []InstanceNum{99},
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

func decodeScratchTestMessage(keys ...[]byte) Message {
	if len(keys) == 0 {
		keys = [][]byte{[]byte("scratch-key-a"), []byte("scratch-key-b")}
	}
	return Message{
		Type:   MsgCommit,
		From:   1,
		To:     2,
		Ref:    InstanceRef{Replica: 1, Instance: 7, Conf: 1},
		Ballot: Ballot{Epoch: 2, Number: 3, Replica: 1},
		Seq:    11,
		Deps:   []InstanceNum{4, 5, 6},
		Command: Command{
			ID:           CommandID{Client: 9, Sequence: 10},
			Payload:      []byte("scratch-payload"),
			ConflictKeys: keys,
		},
		RejectHint:   Ballot{Epoch: 3, Number: 4, Replica: 2},
		RecordStatus: StatusCommitted,
	}
}

func assertDecodedScratchMessage(t *testing.T, got Message, want Message) {
	t.Helper()
	want.Checksum = ChecksumMessage(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded message mismatch\ngot:  %#v\nwant: %#v", got, want)
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

	seeds := [][]byte{
		valid,
		mustEncodeMessageSeed(maxDeps),
		mustEncodeMessageSeed(maxKeys),
		malformedCodecFrame(maxWireDeps+1, 0),
		malformedCodecFrame(0, maxWireConflictKeys+1),
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
