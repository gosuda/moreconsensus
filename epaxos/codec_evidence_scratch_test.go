package epaxos

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestDecodeMessageRejectsOversizedMessageType(t *testing.T) {
	valid, _ := evidenceScratchTestMessages()
	valid.Type = MessageType(1)
	encoded := mustEncodeMessageSeed(valid)
	frame := append([]byte(nil), encoded[:len(wireMagic)]...)
	frame = binary.AppendUvarint(frame, 257)
	frame = append(frame, encoded[len(wireMagic)+1:]...)

	var got Message
	if err := DecodeMessage(frame, &got); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("oversized message type error = %v, want ErrInvalidMessage", err)
	}
}

func TestDecodeMessageWithScratchReusesAcceptEvidenceWithoutAllocation(t *testing.T) {
	large, small := evidenceScratchTestMessages()
	largeEncoded := mustEncodeMessageSeed(large)
	smallEncoded := mustEncodeMessageSeed(small)
	scratch := newEvidenceDecodeScratch(large)
	var out Message

	if err := DecodeMessageWithScratch(largeEncoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}
	assertDecodedScratchMessage(t, out, large)
	assertAcceptEvidenceUsesScratch(t, out.AcceptEvidence, &scratch)
	outerBase := &scratch.AcceptEvidence[0]
	depsBase := &scratch.AcceptEvidenceDeps[0]

	if err := DecodeMessageWithScratch(smallEncoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}
	assertDecodedScratchMessage(t, out, small)
	assertAcceptEvidenceUsesScratch(t, out.AcceptEvidence, &scratch)
	if &scratch.AcceptEvidence[0] != outerBase || &scratch.AcceptEvidenceDeps[0] != depsBase {
		t.Fatal("alternating evidence shapes replaced pre-sized scratch storage")
	}
	assertInactiveAcceptEvidenceCleared(t, &scratch)

	var decodeErr error
	allocs := testing.AllocsPerRun(1000, func() {
		decodeErr = DecodeMessageWithScratch(largeEncoded, &out, &scratch)
		if decodeErr != nil {
			return
		}
		decodeErr = DecodeMessageWithScratch(smallEncoded, &out, &scratch)
	})
	if decodeErr != nil {
		t.Fatalf("alternating evidence decode: %v", decodeErr)
	}
	if allocs != 0 {
		t.Fatalf("alternating large/small AcceptEvidence decode allocations = %v, want 0", allocs)
	}
	assertDecodedScratchMessage(t, out, small)
	assertAcceptEvidenceUsesScratch(t, out.AcceptEvidence, &scratch)
	assertInactiveAcceptEvidenceCleared(t, &scratch)
}

func TestDecodeScratchResetClearsAcceptEvidenceReferencesAndKeepsCapacity(t *testing.T) {
	large, _ := evidenceScratchTestMessages()
	encoded := mustEncodeMessageSeed(large)
	scratch := newEvidenceDecodeScratch(large)
	var out Message
	if err := DecodeMessageWithScratch(encoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}

	depsCap := cap(scratch.Deps)
	acceptDepsCap := cap(scratch.AcceptDeps)
	evidenceCap := cap(scratch.AcceptEvidence)
	evidenceDepsCap := cap(scratch.AcceptEvidenceDeps)
	keysCap := cap(scratch.ConflictKeys)
	retainedEvidence := scratch.AcceptEvidence[:cap(scratch.AcceptEvidence)]
	retainedKeys := scratch.ConflictKeys[:cap(scratch.ConflictKeys)]

	// Reset must clear pointer-bearing slots outside the current length too.
	scratch.AcceptEvidence = scratch.AcceptEvidence[:1]
	retainedEvidence[len(retainedEvidence)-1] = AcceptEvidence{
		Sender: 99,
		Seq:    99,
		Deps:   []InstanceNum{99},
	}
	scratch.ConflictKeys = scratch.ConflictKeys[:1]
	retainedKeys[len(retainedKeys)-1] = []byte("inactive-key-reference")
	scratch.Reset()

	if len(scratch.Deps) != 0 || cap(scratch.Deps) != depsCap ||
		len(scratch.AcceptDeps) != 0 || cap(scratch.AcceptDeps) != acceptDepsCap ||
		len(scratch.AcceptEvidence) != 0 || cap(scratch.AcceptEvidence) != evidenceCap ||
		len(scratch.AcceptEvidenceDeps) != 0 || cap(scratch.AcceptEvidenceDeps) != evidenceDepsCap ||
		len(scratch.ConflictKeys) != 0 || cap(scratch.ConflictKeys) != keysCap {
		t.Fatalf("Reset changed scratch capacity or retained active lengths: %#v", scratch)
	}
	for i, evidence := range retainedEvidence {
		if evidence.Sender != 0 || evidence.Seq != 0 || evidence.Deps != nil {
			t.Fatalf("Reset retained AcceptEvidence slot %d: %#v", i, evidence)
		}
	}
	for i, key := range retainedKeys {
		if key != nil {
			t.Fatalf("Reset retained conflict-key slot %d: %q", i, key)
		}
	}
}

func TestDecodeMessageWithScratchGrowthClearsAbandonedAcceptEvidenceReferences(t *testing.T) {
	large, _ := evidenceScratchTestMessages()
	encoded := mustEncodeMessageSeed(large)
	staleDeps := []InstanceNum{91, 92}
	oldEvidence := []AcceptEvidence{{Sender: 9, Seq: 9, Deps: staleDeps}}
	scratch := newEvidenceDecodeScratch(large)
	scratch.AcceptEvidence = oldEvidence[:0]
	var out Message

	if err := DecodeMessageWithScratch(encoded, &out, &scratch); err != nil {
		t.Fatal(err)
	}
	assertDecodedScratchMessage(t, out, large)
	assertAcceptEvidenceUsesScratch(t, out.AcceptEvidence, &scratch)
	if oldEvidence[0].Sender != 0 || oldEvidence[0].Seq != 0 || oldEvidence[0].Deps != nil {
		t.Fatalf("evidence scratch growth retained abandoned reference: %#v", oldEvidence[0])
	}
}

func TestDecodeMessageWithScratchMalformedAcceptEvidenceReuseHasNoAllocation(t *testing.T) {
	large, _ := evidenceScratchTestMessages()
	valid := mustEncodeMessageSeed(large)
	corruptChecksum := append([]byte(nil), valid...)
	corruptChecksum[len(corruptChecksum)-1] ^= 0x80

	lateTruncation := []uint64{2, 9, maxWireDeps}
	for i := range maxWireDeps {
		lateTruncation = append(lateTruncation, uint64(i))
	}
	lateTruncation = append(lateTruncation, 3)

	tests := []struct {
		name  string
		frame []byte
		want  error
	}{
		{
			name:  "overwide evidence count",
			frame: malformedAcceptEvidenceFrame(maxWireAcceptEvidence + 1),
			want:  ErrInvalidMessage,
		},
		{
			name:  "overwide dependency count",
			frame: malformedAcceptEvidenceFrame(1, 2, 9, maxWireDeps+1),
			want:  ErrInvalidMessage,
		},
		{
			name:  "truncated dependency vector",
			frame: malformedAcceptEvidenceFrame(1, 2, 9, 3, 10, 11),
			want:  ErrInvalidMessage,
		},
		{
			name:  "late truncation does not grow arena",
			frame: malformedAcceptEvidenceFrame(2, lateTruncation...),
			want:  ErrInvalidMessage,
		},
		{
			name:  "truncated after complete evidence",
			frame: malformedAcceptEvidenceFrame(1, 2, 9, 3, 10, 11, 12),
			want:  ErrInvalidMessage,
		},
		{
			name:  "checksum mismatch after complete decode",
			frame: corruptChecksum,
			want:  ErrChecksumMismatch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scratch := newEvidenceDecodeScratch(large)
			initialEvidenceCap := cap(scratch.AcceptEvidence)
			initialEvidenceDepsCap := cap(scratch.AcceptEvidenceDeps)
			initialKeyCap := cap(scratch.ConflictKeys)
			var out Message
			if err := DecodeMessageWithScratch(valid, &out, &scratch); err != nil {
				t.Fatal(err)
			}
			assertDecodedScratchMessage(t, out, large)

			var decodeErr error
			allocs := testing.AllocsPerRun(1000, func() {
				decodeErr = DecodeMessageWithScratch(valid, &out, &scratch)
				if decodeErr != nil {
					return
				}
				decodeErr = DecodeMessageWithScratch(tc.frame, &out, &scratch)
			})
			if !errors.Is(decodeErr, tc.want) {
				t.Fatalf("malformed evidence decode error = %v, want %v", decodeErr, tc.want)
			}
			if allocs != 0 {
				t.Fatalf("valid/malformed evidence reuse allocations = %v, want 0", allocs)
			}
			if !reflect.DeepEqual(out, Message{}) {
				t.Fatalf("malformed evidence decode retained destination data: %#v", out)
			}
			if len(scratch.Deps) != 0 || len(scratch.AcceptDeps) != 0 ||
				len(scratch.AcceptEvidence) != 0 || len(scratch.AcceptEvidenceDeps) != 0 ||
				len(scratch.ConflictKeys) != 0 {
				t.Fatalf("malformed evidence decode retained scratch lengths: %#v", scratch)
			}
			if cap(scratch.AcceptEvidence) != initialEvidenceCap ||
				cap(scratch.AcceptEvidenceDeps) != initialEvidenceDepsCap ||
				cap(scratch.ConflictKeys) != initialKeyCap {
				t.Fatalf("malformed evidence decode amplified scratch capacity: evidence=%d/%d deps=%d/%d keys=%d/%d",
					cap(scratch.AcceptEvidence), initialEvidenceCap,
					cap(scratch.AcceptEvidenceDeps), initialEvidenceDepsCap,
					cap(scratch.ConflictKeys), initialKeyCap)
			}
			for i, evidence := range scratch.AcceptEvidence[:cap(scratch.AcceptEvidence)] {
				if evidence.Sender != 0 || evidence.Seq != 0 || evidence.Deps != nil {
					t.Fatalf("malformed evidence decode retained outer slot %d: %#v", i, evidence)
				}
			}
			for i, key := range scratch.ConflictKeys[:cap(scratch.ConflictKeys)] {
				if key != nil {
					t.Fatalf("malformed evidence decode retained key slot %d: %q", i, key)
				}
			}
		})
	}
}

func evidenceScratchTestMessages() (Message, Message) {
	large := decodeScratchTestMessage([]byte("large-key-a"), []byte("large-key-b"))
	large.AcceptSeq = 14
	large.AcceptDeps = []InstanceNum{3, 5, 8, 13}
	large.AcceptEvidence = []AcceptEvidence{
		{Sender: 2, Seq: 14, Deps: []InstanceNum{1, 2, 3, 4}},
		{Sender: 3, Seq: 15, Deps: []InstanceNum{5, 6}},
		{Sender: 4, Seq: 16, Deps: []InstanceNum{}},
		{Sender: 5, Seq: 17, Deps: []InstanceNum{7, 8, 9}},
	}

	small := decodeScratchTestMessage([]byte("small-key"))
	small.AcceptSeq = 6
	small.AcceptDeps = []InstanceNum{2}
	small.AcceptEvidence = []AcceptEvidence{
		{Sender: 2, Seq: 6, Deps: []InstanceNum{10, 11}},
	}
	return large, small
}

func newEvidenceDecodeScratch(m Message) DecodeScratch {
	totalEvidenceDeps := 0
	for _, evidence := range m.AcceptEvidence {
		totalEvidenceDeps += len(evidence.Deps)
	}
	return DecodeScratch{
		Deps:               make([]InstanceNum, 0, len(m.Deps)),
		AcceptDeps:         make([]InstanceNum, 0, len(m.AcceptDeps)),
		AcceptEvidence:     make([]AcceptEvidence, 0, len(m.AcceptEvidence)),
		AcceptEvidenceDeps: make([]InstanceNum, 0, totalEvidenceDeps),
		ConflictKeys:       make([][]byte, 0, len(m.Command.ConflictKeys)),
	}
}

func assertAcceptEvidenceUsesScratch(t *testing.T, got []AcceptEvidence, scratch *DecodeScratch) {
	t.Helper()
	if len(got) != len(scratch.AcceptEvidence) {
		t.Fatalf("decoded/scratch evidence lengths = %d/%d", len(got), len(scratch.AcceptEvidence))
	}
	if len(got) > 0 && &got[0] != &scratch.AcceptEvidence[0] {
		t.Fatal("decoded AcceptEvidence outer slice does not use scratch storage")
	}
	offset := 0
	for i, evidence := range got {
		if cap(evidence.Deps) != len(evidence.Deps) {
			t.Fatalf("AcceptEvidence %d dependency capacity = %d, want isolated length %d", i, cap(evidence.Deps), len(evidence.Deps))
		}
		if len(evidence.Deps) > 0 && &evidence.Deps[0] != &scratch.AcceptEvidenceDeps[offset] {
			t.Fatalf("AcceptEvidence %d dependencies do not partition the flat scratch arena", i)
		}
		offset += len(evidence.Deps)
	}
	if offset != len(scratch.AcceptEvidenceDeps) {
		t.Fatalf("evidence dependency partitions cover %d arena entries, want %d", offset, len(scratch.AcceptEvidenceDeps))
	}
}

func assertInactiveAcceptEvidenceCleared(t *testing.T, scratch *DecodeScratch) {
	t.Helper()
	retained := scratch.AcceptEvidence[:cap(scratch.AcceptEvidence)]
	for i := len(scratch.AcceptEvidence); i < len(retained); i++ {
		evidence := retained[i]
		if evidence.Sender != 0 || evidence.Seq != 0 || evidence.Deps != nil {
			t.Fatalf("scratch retained inactive AcceptEvidence slot %d: %#v", i, evidence)
		}
	}
}

func malformedAcceptEvidenceFrame(evidenceCount uint64, evidenceBody ...uint64) []byte {
	frame := append([]byte(nil), wireMagic[:]...)
	for _, value := range []uint64{
		uint64(MsgCommit),
		1,
		2,
		1,
		7,
		1,
		0,
	} {
		frame = binary.AppendUvarint(frame, value)
	}
	frame = append(frame, 0)
	for _, value := range []uint64{
		2,
		3,
		1,
		0,
		0,
		0,
		11,
		0,
		0,
		0,
		evidenceCount,
	} {
		frame = binary.AppendUvarint(frame, value)
	}
	for _, value := range evidenceBody {
		frame = binary.AppendUvarint(frame, value)
	}
	return append(frame, make([]byte, 32)...)
}
