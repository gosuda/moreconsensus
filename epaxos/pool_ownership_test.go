package epaxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

var (
	allocationChecksumSink      [32]byte
	allocationBytesSink         []byte
	dependencyIteratorCountSink int
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

func TestRecordChecksumVersionsDistinguishDurableMetadata(t *testing.T) {
	record := InstanceRecord{
		Ref:              InstanceRef{Replica: 1, Instance: 9, Conf: 1},
		Ballot:           Ballot{Epoch: 2, Number: 3, Replica: 1},
		RecordBallot:     Ballot{Epoch: 2, Number: 3, Replica: 1},
		Status:           StatusPreAccepted,
		Seq:              7,
		Deps:             []InstanceNum{0, 4, 5},
		Command:          Command{ID: CommandID{Client: 91, Sequence: 1}, Payload: []byte("checksum-version"), ConflictKeys: [][]byte{[]byte("checksum-version-key")}},
		FastPathEligible: true,
		ProcessAt:        42,
		TimingDomain:     TimingDomainTOQ,
		TOQPending:       true,
	}

	canonical := record
	canonical.Checksum = ChecksumRecord(canonical)
	if !VerifyRecordChecksum(canonical) {
		t.Fatalf("canonical checksum rejected for record %#v", canonical)
	}
	if VerifyRecordChecksumWithoutTOQ(canonical) || VerifyRecordChecksumWithoutFastPathOrTOQ(canonical) {
		t.Fatalf("canonical checksum was accepted as an older layout")
	}

	withAcceptEvidence := record
	withAcceptEvidence.AcceptSeq = 11
	withAcceptEvidence.AcceptDeps = []InstanceNum{2, 4, 6}
	withAcceptEvidence.AcceptEvidence = []AcceptEvidence{{Sender: 3, Seq: 11, Deps: []InstanceNum{2, 4, 6}}}

	canonicalWithAcceptEvidence := withAcceptEvidence
	canonicalWithAcceptEvidence.Checksum = ChecksumRecord(canonicalWithAcceptEvidence)
	if !VerifyRecordChecksum(canonicalWithAcceptEvidence) {
		t.Fatalf("canonical checksum with Accept-Deps evidence rejected for record %#v", canonicalWithAcceptEvidence)
	}
	if VerifyRecordChecksumWithoutAcceptEvidence(canonicalWithAcceptEvidence) {
		t.Fatalf("canonical checksum with Accept-Deps evidence was accepted as the legacy no-Accept-evidence layout")
	}
	if VerifyRecordChecksumWithoutSenderAcceptEvidence(canonicalWithAcceptEvidence) {
		t.Fatalf("canonical checksum with sender-preserving AcceptEvidence was accepted as the legacy no-sender layout")
	}

	senderChanged := canonicalWithAcceptEvidence.Clone()
	senderChanged.AcceptEvidence[0].Sender = 2
	if VerifyRecordChecksum(senderChanged) {
		t.Fatalf("record with changed AcceptEvidence sender was accepted before recomputing checksum")
	}
	senderChangedChecksum := ChecksumRecord(senderChanged)
	if senderChangedChecksum == canonicalWithAcceptEvidence.Checksum {
		t.Fatalf("AcceptEvidence sender change did not change canonical checksum")
	}
	senderChanged.Checksum = senderChangedChecksum
	if !VerifyRecordChecksum(senderChanged) {
		t.Fatalf("record with changed AcceptEvidence sender was rejected after recomputing checksum")
	}

	depsChanged := canonicalWithAcceptEvidence.Clone()
	depsChanged.AcceptEvidence[0].Deps[1] = 8
	if VerifyRecordChecksum(depsChanged) {
		t.Fatalf("record with changed AcceptEvidence deps was accepted before recomputing checksum")
	}
	depsChangedChecksum := ChecksumRecord(depsChanged)
	if depsChangedChecksum == canonicalWithAcceptEvidence.Checksum {
		t.Fatalf("AcceptEvidence deps change did not change canonical checksum")
	}
	depsChanged.Checksum = depsChangedChecksum
	if !VerifyRecordChecksum(depsChanged) {
		t.Fatalf("record with changed AcceptEvidence deps was rejected after recomputing checksum")
	}

	withoutSenderAcceptEvidence := withAcceptEvidence.Clone()
	withoutSenderAcceptEvidence.TimingDomain = TimingDomainUntimed
	withoutSenderAcceptEvidence.AcceptEvidence = nil
	withoutSenderAcceptEvidence.Checksum = checksumRecord(withoutSenderAcceptEvidence, true, true, true, true, false, false)
	if VerifyRecordChecksum(withoutSenderAcceptEvidence) {
		t.Fatalf("legacy checksum without sender-preserving AcceptEvidence was accepted as canonical")
	}
	if !VerifyRecordChecksumWithoutSenderAcceptEvidence(withoutSenderAcceptEvidence) {
		t.Fatalf("legacy checksum without sender-preserving AcceptEvidence was rejected by legacy verifier")
	}

	withoutAcceptEvidence := withAcceptEvidence.Clone()
	withoutAcceptEvidence.TimingDomain = TimingDomainUntimed
	withoutAcceptEvidence.RecordBallot = Ballot{}
	withoutAcceptEvidence.AcceptSeq = 0
	withoutAcceptEvidence.AcceptDeps = nil
	withoutAcceptEvidence.AcceptEvidence = nil
	withoutAcceptEvidence.Checksum = checksumRecord(withoutAcceptEvidence, true, true, false, false, false, false)
	if VerifyRecordChecksum(withoutAcceptEvidence) {
		t.Fatalf("legacy checksum without Accept-Deps evidence was accepted as canonical")
	}
	if !VerifyRecordChecksumWithoutAcceptEvidence(withoutAcceptEvidence) {
		t.Fatalf("legacy checksum without Accept-Deps evidence was rejected by legacy verifier")
	}

	withoutTOQ := record
	withoutTOQ.TimingDomain = TimingDomainUntimed
	withoutTOQ.RecordBallot = Ballot{}
	withoutTOQ.AcceptSeq = 0
	withoutTOQ.AcceptDeps = nil
	withoutTOQ.AcceptEvidence = nil
	withoutTOQ.ProcessAt = 0
	withoutTOQ.TOQPending = false
	withoutTOQ.Checksum = checksumRecord(withoutTOQ, true, false, false, false, false, false)
	if VerifyRecordChecksum(withoutTOQ) {
		t.Fatalf("checksum without TOQ metadata was accepted as canonical")
	}
	if !VerifyRecordChecksumWithoutTOQ(withoutTOQ) {
		t.Fatalf("checksum without TOQ metadata was rejected by legacy verifier")
	}
	if VerifyRecordChecksumWithoutFastPathOrTOQ(withoutTOQ) {
		t.Fatalf("checksum without TOQ metadata was accepted as the older no-fast-path layout")
	}

	withoutFastPathOrTOQ := record
	withoutFastPathOrTOQ.TimingDomain = TimingDomainUntimed
	withoutFastPathOrTOQ.RecordBallot = Ballot{}
	withoutFastPathOrTOQ.AcceptSeq = 0
	withoutFastPathOrTOQ.AcceptDeps = nil
	withoutFastPathOrTOQ.AcceptEvidence = nil
	withoutFastPathOrTOQ.FastPathEligible = false
	withoutFastPathOrTOQ.ProcessAt = 0
	withoutFastPathOrTOQ.TOQPending = false
	withoutFastPathOrTOQ.Checksum = checksumRecord(withoutFastPathOrTOQ, false, false, false, false, false, false)
	if VerifyRecordChecksum(withoutFastPathOrTOQ) || VerifyRecordChecksumWithoutTOQ(withoutFastPathOrTOQ) {
		t.Fatalf("checksum without fast-path and TOQ metadata was accepted as a newer layout")
	}
	if !VerifyRecordChecksumWithoutFastPathOrTOQ(withoutFastPathOrTOQ) {
		t.Fatalf("checksum without fast-path and TOQ metadata was rejected by oldest-layout verifier")
	}
}

func allocationTestMessage() Message {
	return Message{Type: MsgCommit, From: 1,
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
		RejectHint: Ballot{Epoch: 3, Number: 4, Replica: 2}}
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

func TestEncodeDecodeMessagePreservesExplicitTOQPreAccept(t *testing.T) {
	want := Message{
		Type:      MsgPreAccept,
		From:      1,
		To:        2,
		Ref:       InstanceRef{Replica: 1, Instance: 17, Conf: 1},
		ProcessAt: 123,
		TOQ:       true,
		Ballot:    Ballot{Replica: 1},
		Command: Command{
			ID:           CommandID{Client: 77, Sequence: 3},
			Payload:      []byte("toq-wire-payload"),
			ConflictKeys: [][]byte{[]byte("toq-wire-key")},
		},
		RecordStatus: StatusNone,
	}
	encoded, err := EncodeMessage(nil, want)
	if err != nil {
		t.Fatalf("EncodeMessage explicit TOQ PreAccept: %v", err)
	}

	var got Message
	if err := DecodeMessage(encoded, &got); err != nil {
		t.Fatalf("DecodeMessage explicit TOQ PreAccept: %v", err)
	}
	want.Deps = []InstanceNum{}
	want.AcceptDeps = []InstanceNum{}
	want.Checksum = ChecksumMessage(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TOQ PreAccept wire round-trip mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
	if err := got.Validate(ConfState{ID: 1, Voters: makeIDs(3)}); err != nil {
		t.Fatalf("decoded explicit TOQ PreAccept failed validation: %v", err)
	}
}

func TestEncodeDecodeMessagePreservesAcceptEvidence(t *testing.T) {
	want := Message{Type: MsgAccept, From: 1,
		To:         2,
		Ref:        InstanceRef{Replica: 1, Instance: 18, Conf: 1},
		Ballot:     Ballot{Epoch: 2, Number: 1, Replica: 1},
		Seq:        7,
		Deps:       []InstanceNum{0, 4, 5},
		AcceptSeq:  9,
		AcceptDeps: []InstanceNum{3, 4, 5},
		AcceptEvidence: []AcceptEvidence{
			{Sender: 3, Seq: 10, Deps: []InstanceNum{1, 4, 6}},
			{Sender: 1, Seq: 9, Deps: []InstanceNum{3, 4, 5}},
		},
		Command: Command{
			ID:           CommandID{Client: 78, Sequence: 4},
			Payload:      []byte("accept-evidence-wire-payload"),
			ConflictKeys: [][]byte{[]byte("accept-evidence-wire-key")},
		}}
	encoded, err := EncodeMessage(nil, want)
	if err != nil {
		t.Fatalf("EncodeMessage AcceptEvidence: %v", err)
	}

	var got Message
	if err := DecodeMessage(encoded, &got); err != nil {
		t.Fatalf("DecodeMessage AcceptEvidence: %v", err)
	}
	want.Checksum = ChecksumMessage(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AcceptEvidence wire round-trip mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
	if !reflect.DeepEqual(got.AcceptEvidence, want.AcceptEvidence) {
		t.Fatalf("decoded AcceptEvidence = %#v, want %#v", got.AcceptEvidence, want.AcceptEvidence)
	}
	if got.Seq != 7 || !reflect.DeepEqual(got.Deps, []InstanceNum{0, 4, 5}) {
		t.Fatalf("AcceptEvidence mutated chosen attributes: seq=%d deps=%v", got.Seq, got.Deps)
	}
	attrs, ok := got.AcceptAttributes()
	if !ok || attrs.Seq != 9 || !reflect.DeepEqual(attrs.Deps, []InstanceNum{3, 4, 5}) {
		t.Fatalf("decoded aggregate Accept-Deps attributes = %#v, %v", attrs, ok)
	}
	if err := got.Validate(ConfState{ID: 1, Voters: makeIDs(3)}); err != nil {
		t.Fatalf("decoded AcceptEvidence message failed validation: %v", err)
	}
}

func TestDecodeMessageRejectsOverwideWireCountsBeforeAllocation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{name: "dependency count", frame: malformedWireCountFrame(maxWireDeps+1, 0)},
		{name: "conflict key count", frame: malformedWireCountFrame(0, maxWireConflictKeys+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := Message{
				Type: MsgCommit,
				Deps: []InstanceNum{9},
				Command: Command{
					Payload:      []byte("stale-payload"),
					ConflictKeys: [][]byte{[]byte("stale-key")},
				},
			}
			scratch := DecodeScratch{
				Deps:         []InstanceNum{1, 2, 3},
				ConflictKeys: [][]byte{[]byte("old-key")},
			}
			if err := DecodeMessageWithScratch(tc.frame, &out, &scratch); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("DecodeMessageWithScratch(%s) err=%v, want %v", tc.name, err, ErrInvalidMessage)
			}
			assertMessageCleared(t, &out)
			if len(scratch.Deps) != 0 || len(scratch.ConflictKeys) != 0 {
				t.Fatalf("decode failure left scratch populated: deps=%v keys=%v", scratch.Deps, scratch.ConflictKeys)
			}
		})
	}
}

func malformedWireCountFrame(deps, conflictKeys uint64) []byte {
	buf := append([]byte(nil), wireMagic[:]...)
	for _, v := range []uint64{
		uint64(MsgCommit),
		1,
		1,
		1,
		1,
		1,
		0,
	} {
		buf = binary.AppendUvarint(buf, v)
	}
	buf = append(buf, 0)
	for _, v := range []uint64{
		0,
		0,
		1,
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
			buf = append(buf, 0)
			for range 7 {
				buf = binary.AppendUvarint(buf, 0)
			}
			buf = append(buf, 0)
			buf = binary.AppendUvarint(buf, 0)
			buf = binary.AppendUvarint(buf, uint64(StatusCommitted))
		}
	}
	return append(buf, make([]byte, 32)...)
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
	base := Message{Type: MsgCommit, From: 1,
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
		RejectHint: Ballot{Epoch: 3, Number: 4, Replica: 2}}
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
	return Message{Type: MsgCommit, From: 1,
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
		RejectHint: Ballot{Epoch: 3, Number: 4, Replica: 2}}
}

func assertDecodedScratchMessage(t *testing.T, got Message, want Message) {
	t.Helper()
	want.Checksum = ChecksumMessage(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded message mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
}

func decodeMessageSeedCorpus() [][]byte {
	base := Message{Type: MsgCommit, From: 1,
		To:     2,
		Ref:    InstanceRef{Replica: 1, Instance: 7, Conf: 1},
		Ballot: Ballot{Epoch: 1, Number: 2, Replica: 1},
		Seq:    3,
		Deps:   []InstanceNum{0, 4, 5},
		Command: Command{
			ID:           CommandID{Client: 9, Sequence: 10},
			Payload:      []byte("payload"),
			ConflictKeys: [][]byte{[]byte("alpha"), []byte("beta")},
		}}
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
		malformedCodecFrame(maxWireDeps+1, 0, 0),
		malformedCodecFrame(0, maxWireDeps+1, 0),
		malformedCodecFrame(0, 0, maxWireConflictKeys+1),
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
	if m.Type != 0 || m.From != 0 || m.To != 0 || !m.Ref.IsZero() || m.Ballot != (Ballot{}) || m.RecordBallot != (Ballot{}) || m.Seq != 0 || len(m.Deps) != 0 || m.AcceptSeq != 0 || len(m.AcceptDeps) != 0 || len(m.AcceptEvidence) != 0 || m.Reject || m.RejectHint != (Ballot{}) || m.RecordStatus != 0 {
		t.Fatalf("message retained metadata: %#v", m)
	}
	for i, dep := range m.Deps[:cap(m.Deps)] {
		if dep != 0 {
			t.Fatalf("message retained dependency %d at slot %d", dep, i)
		}
	}
	for i, dep := range m.AcceptDeps[:cap(m.AcceptDeps)] {
		if dep != 0 {
			t.Fatalf("message retained accept dependency %d at slot %d", dep, i)
		}
	}
	for i, evidence := range m.AcceptEvidence[:cap(m.AcceptEvidence)] {
		if evidence.Sender != 0 || evidence.Seq != 0 || len(evidence.Deps) != 0 {
			t.Fatalf("message retained AcceptEvidence metadata at slot %d: %#v", i, evidence)
		}
		for j, dep := range evidence.Deps[:cap(evidence.Deps)] {
			if dep != 0 {
				t.Fatalf("message retained AcceptEvidence dependency %d at slot %d/%d", dep, i, j)
			}
		}
	}
	assertCommandCleared(t, m.Command)
	if m.Checksum != ([32]byte{}) {
		t.Fatalf("message retained checksum: %#v", m.Checksum)
	}
}

func assertCommandCleared(t *testing.T, c Command) {
	t.Helper()
	if c.ID != (CommandID{}) || c.Kind != CommandUser || len(c.Payload) != 0 || len(c.ConflictKeys) != 0 {
		t.Fatalf("command retained caller-owned data: %#v", c)
	}
	for i, value := range c.Payload[:cap(c.Payload)] {
		if value != 0 {
			t.Fatalf("command retained payload byte %d at slot %d", value, i)
		}
	}
	for i, key := range c.ConflictKeys[:cap(c.ConflictKeys)] {
		if len(key) != 0 {
			t.Fatalf("command retained conflict-key length at slot %d: %#v", i, key)
		}
		for j, value := range key[:cap(key)] {
			if value != 0 {
				t.Fatalf("command retained conflict-key byte %d at slot %d/%d", value, i, j)
			}
		}
	}
}

func dependencyIteratorAllocationNode(t testing.TB, through InstanceNum) (*RawNode, InstanceRef) {
	t.Helper()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	base := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
	rn.installInstance(&instance{rec: InstanceRecord{Ref: base, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, through, 0}, Command: Command{Kind: CommandNoop}}})
	for _, number := range []InstanceNum{1, 8} {
		ref := InstanceRef{Conf: 1, Replica: 2, Instance: number}
		rn.installInstance(&instance{rec: InstanceRecord{Ref: ref, Status: StatusCommitted, Seq: 1, Deps: make([]InstanceNum, 3), Command: Command{Kind: CommandNoop}}})
	}
	return rn, base
}

func countMaterializedDependencies(rn *RawNode, base InstanceRef) int {
	view := rn.newExecutionView()
	iter := view.dependencyRefs(rn, base)
	count := 0
	for {
		_, ok := iter.next()
		if !ok {
			return count
		}
		count++
	}
}

func countMaterializedDependenciesInView(rn *RawNode, view *executionView, base InstanceRef) int {
	iter := view.dependencyRefs(rn, base)
	count := 0
	for {
		_, ok := iter.next()
		if !ok {
			return count
		}
		count++
	}
}

func TestDependencyIteratorAllocationIndependentOfEndpoint(t *testing.T) {
	small, smallBase := dependencyIteratorAllocationNode(t, 8)
	max, maxBase := dependencyIteratorAllocationNode(t, ^InstanceNum(0))
	if got, want := countMaterializedDependencies(small, smallBase), countMaterializedDependencies(max, maxBase); got != want || got != 2 {
		t.Fatalf("iterator cardinality small/max = %d/%d, want 2/2", got, want)
	}
	countMaterializedDependencies(small, smallBase)
	countMaterializedDependencies(max, maxBase)
	smallAllocs := testing.AllocsPerRun(100, func() {
		dependencyIteratorCountSink = countMaterializedDependencies(small, smallBase)
	})
	maxAllocs := testing.AllocsPerRun(100, func() {
		dependencyIteratorCountSink = countMaterializedDependencies(max, maxBase)
	})
	if smallAllocs != maxAllocs {
		t.Fatalf("iterator allocations depend on endpoint: small=%v max=%v", smallAllocs, maxAllocs)
	}
	smallView := small.newExecutionView()
	maxView := max.newExecutionView()
	smallPrimitiveAllocs := testing.AllocsPerRun(100, func() {
		dependencyIteratorCountSink = countMaterializedDependenciesInView(small, &smallView, smallBase)
	})
	maxPrimitiveAllocs := testing.AllocsPerRun(100, func() {
		dependencyIteratorCountSink = countMaterializedDependenciesInView(max, &maxView, maxBase)
	})
	if smallPrimitiveAllocs != 0 || maxPrimitiveAllocs != 0 {
		t.Fatalf("warmed dependency iterator allocated: small=%v max=%v", smallPrimitiveAllocs, maxPrimitiveAllocs)
	}
}

func BenchmarkDependencyIteratorEndpoint(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		through InstanceNum
	}{
		{name: "Small", through: 8},
		{name: "Max", through: ^InstanceNum(0)},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			rn, base := dependencyIteratorAllocationNode(b, benchmark.through)
			countMaterializedDependencies(rn, base)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				dependencyIteratorCountSink = countMaterializedDependencies(rn, base)
			}
		})
	}
}

func BenchmarkDependencyIteratorPrimitive(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		through InstanceNum
	}{
		{name: "Small", through: 8},
		{name: "Max", through: ^InstanceNum(0)},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			rn, base := dependencyIteratorAllocationNode(b, benchmark.through)
			view := rn.newExecutionView()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				dependencyIteratorCountSink = countMaterializedDependenciesInView(rn, &view, base)
			}
		})
	}
}
