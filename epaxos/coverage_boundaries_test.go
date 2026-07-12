package epaxos

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestBootstrapChunkCodecRoundTripAndMalformedFrames(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 3, 1)
	payload := []byte("bootstrap chunk payload")
	chunk := BootstrapChunk{
		Cluster:         fixture.cluster,
		Plan:            fixture.planID,
		Manifest:        StateDigest{4},
		From:            fixture.identities[0].Replica,
		FromIncarnation: fixture.identities[0].Incarnation,
		To:              fixture.identities[1].Replica,
		Index:           0,
		Offset:          0,
		Total:           uint64(len(payload)),
		Payload:         payload,
	}
	signed, err := SignBootstrapChunk(chunk, fixture.identities[0], fixture.private[0])
	if err != nil {
		t.Fatalf("SignBootstrapChunk: %v", err)
	}
	if err := VerifyBootstrapChunk(signed, fixture.identities[0]); err != nil {
		t.Fatalf("VerifyBootstrapChunk: %v", err)
	}

	encoded, err := EncodeBootstrapChunk([]byte{0xa5}, signed)
	if err != nil {
		t.Fatalf("EncodeBootstrapChunk: %v", err)
	}
	if encoded[0] != 0xa5 {
		t.Fatalf("EncodeBootstrapChunk overwrote caller prefix: %x", encoded[0])
	}
	frame := encoded[1:]
	var decoded BootstrapChunk
	if err := DecodeBootstrapChunk(frame, &decoded); err != nil {
		t.Fatalf("DecodeBootstrapChunk: %v", err)
	}
	if !reflect.DeepEqual(decoded, signed) {
		t.Fatalf("decoded chunk differs from signed chunk:\n got %#v\nwant %#v", decoded, signed)
	}
	if err := VerifyBootstrapChunk(decoded, fixture.identities[0]); err != nil {
		t.Fatalf("VerifyBootstrapChunk(decoded): %v", err)
	}

	invalidDigest := signed
	invalidDigest.PayloadDigest[0]++
	unchanged, err := EncodeBootstrapChunk([]byte{0x7f}, invalidDigest)
	if !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("invalid payload digest error = %v, want ErrInvalidBootstrapMessage", err)
	}
	if _, conflictErr := bootstrapChunkSigningBytes(invalidDigest); !errors.Is(conflictErr, ErrBootstrapChunkConflict) {
		t.Fatalf("invalid payload digest canonical error = %v, want ErrBootstrapChunkConflict", conflictErr)
	}
	if !bytes.Equal(unchanged, []byte{0x7f}) {
		t.Fatalf("invalid encode changed destination: %x", unchanged)
	}

	digestOffset := bytes.Index(frame, signed.PayloadDigest[:])
	if digestOffset < 0 {
		t.Fatal("signed payload digest not found in canonical frame")
	}
	for length := 0; length < len(frame); length++ {
		mutated := frame[:length]
		got := BootstrapChunk{From: 99}
		err := DecodeBootstrapChunk(mutated, &got)
		if !errors.Is(err, ErrInvalidBootstrapMessage) {
			t.Fatalf("truncated frame length %d error = %v, want ErrInvalidBootstrapMessage", length, err)
		}
		if !reflect.DeepEqual(got, BootstrapChunk{}) {
			t.Fatalf("truncated frame length %d retained output: %#v", length, got)
		}
	}
	nonCanonical := append([]byte(nil), frame[:len(bootstrapChunkMagic)]...)
	nonCanonical = append(nonCanonical, 0x81, 0x00)
	nonCanonical = append(nonCanonical, frame[len(bootstrapChunkMagic)+1:]...)
	var nonCanonicalChunk BootstrapChunk
	if err := DecodeBootstrapChunk(nonCanonical, &nonCanonicalChunk); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("non-canonical version error = %v, want ErrInvalidBootstrapMessage", err)
	}
	if !reflect.DeepEqual(nonCanonicalChunk, BootstrapChunk{}) {
		t.Fatalf("non-canonical frame retained output: %#v", nonCanonicalChunk)
	}
	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "bad-magic", mutate: func(b []byte) []byte { b[0] ^= 0xff; return b }},
		{name: "bad-version", mutate: func(b []byte) []byte {
			b[len(bootstrapChunkMagic)] = 2
			return b
		}},
		{name: "trailing-byte", mutate: func(b []byte) []byte { return append(b, 0x01) }},
		{name: "payload-digest", mutate: func(b []byte) []byte {
			b[digestOffset] ^= 0xff
			return b
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := tc.mutate(append([]byte(nil), frame...))
			got := BootstrapChunk{From: 99}
			if err := DecodeBootstrapChunk(mutated, &got); err == nil {
				t.Fatal("DecodeBootstrapChunk accepted malformed frame")
			}
			if !reflect.DeepEqual(got, BootstrapChunk{}) {
				t.Fatalf("DecodeBootstrapChunk retained malformed output: %#v", got)
			}
		})
	}
}

func TestStorageStateCloneDoesNotShareOwnedSlices(t *testing.T) {
	conf := ConfState{ID: 7, Voters: []ReplicaID{1, 2, 3}}
	state := StorageState{
		HardState: HardState{Conf: conf.Clone(), Tick: 9},
		ConfigHistory: []ConfigHistoryEntry{{
			Conf:       conf.Clone(),
			AppliedRef: InstanceRef{Replica: 1, Instance: 4, Conf: conf.ID},
		}},
		LocalVoterState: LocalVoterState{
			Identity: VoterIdentity{Replica: 1, Incarnation: 2, VerifyKey: []byte{1, 2, 3}},
			Conf:     conf.Clone(),
		},
		Frontiers: []FrontierUpdate{{
			Frontier: BootstrapFrontier{
				Conf:  conf.ID,
				Lanes: []BootstrapLaneFrontier{{Replica: 1, ObservedThrough: 4, Sparse: []InstanceNum{2, 4}}},
			},
		}},
	}

	clone := state.Clone()
	clone.HardState.Conf.Voters[0] = 9
	clone.ConfigHistory[0].Conf.Voters[1] = 8
	clone.LocalVoterState.Conf.Voters[2] = 7
	clone.LocalVoterState.Identity.VerifyKey[0] = 9
	clone.Frontiers[0].Frontier.Lanes[0].Sparse[0] = 6

	if state.HardState.Conf.Voters[0] != 1 || state.ConfigHistory[0].Conf.Voters[1] != 2 ||
		state.LocalVoterState.Conf.Voters[2] != 3 || state.LocalVoterState.Identity.VerifyKey[0] != 1 ||
		state.Frontiers[0].Frontier.Lanes[0].Sparse[0] != 2 {
		t.Fatalf("StorageState.Clone shared mutable storage: original=%#v", state)
	}
}

func TestDeferredPreAcceptEntryRemovalCleansBothDomains(t *testing.T) {
	node := &RawNode{
		deferredIndex:         make(map[deferredPreAcceptKey]*deferredPreAccept),
		maxDeferredPreAccepts: 4,
	}
	for i, domain := range []preAcceptDomain{preAcceptLogical, preAcceptTOQ} {
		message := Message{
			Type:   MsgPreAccept,
			From:   1,
			To:     2,
			Ref:    InstanceRef{Replica: 1, Instance: InstanceNum(i + 1), Conf: 1},
			Ballot: Ballot{Number: 0, Replica: 1},
			Deps:   []InstanceNum{0, 0, 0},
		}
		admitted, err := node.admitDeferredPreAccept(domain, message)
		if err != nil || !admitted {
			t.Fatalf("admitDeferredPreAccept(%d): admitted=%v err=%v", domain, admitted, err)
		}
		key := deferredPreAcceptKey{domain: domain, ref: message.Ref, ballot: message.Ballot, from: message.From}
		entry := node.deferredIndex[key]
		if entry == nil {
			t.Fatalf("missing deferred entry for domain %d", domain)
		}
		node.removeDeferredPreAcceptEntry(entry)
		if _, ok := node.deferredIndex[key]; ok {
			t.Fatalf("deferred index retained removed entry for domain %d", domain)
		}
		if len(node.logicalPreAccepts) != 0 || len(node.toqPreAccepts) != 0 {
			t.Fatalf("deferred queue retained removed entry for domain %d", domain)
		}
		if entry.message.Type != 0 || entry.message.Deps != nil {
			t.Fatalf("removed deferred message was not reset: %#v", entry.message)
		}
		node.removeDeferredPreAcceptEntry(entry)
	}
	node.removeDeferredPreAcceptEntry(nil)
}

func TestMatchingInitialLeaderTryRecordRequiresExactAttributes(t *testing.T) {
	node, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	conf := node.Status().Conf
	command := Command{ID: CommandID{Client: 44, Sequence: 1}, Payload: []byte("try")}
	ref := InstanceRef{Replica: 2, Instance: 1, Conf: conf.ID}
	record := InstanceRecord{
		Ref:              ref,
		Status:           StatusPreAccepted,
		FastPathEligible: true,
		Seq:              2,
		Deps:             []InstanceNum{0, 1, 0},
		Command:          command,
	}
	matching := func(candidate InstanceRecord, attrs Attributes) bool {
		inst := &instance{rec: InstanceRecord{Ref: ref, Command: command}, prepareOK: &recordVoteSet{}}
		if !inst.prepareOK.add(conf, 2, candidate) {
			t.Fatal("recordVoteSet.add rejected test record")
		}
		return node.hasMatchingInitialLeaderTryRecord(inst, attrs)
	}
	if !matching(record, record.Attributes()) {
		t.Fatal("matching initial leader TryPreAccept record was rejected")
	}
	if matching(record, Attributes{Seq: record.Seq + 1, Deps: record.Deps}) {
		t.Fatal("attribute mismatch was accepted")
	}
	wrongStatus := record
	wrongStatus.Status = StatusAccepted
	if matching(wrongStatus, record.Attributes()) {
		t.Fatal("non-preaccepted record was accepted")
	}
	wrongFastPath := record
	wrongFastPath.FastPathEligible = false
	if matching(wrongFastPath, record.Attributes()) {
		t.Fatal("non-fast-path record was accepted")
	}
	if node.hasMatchingInitialLeaderTryRecord(nil, record.Attributes()) {
		t.Fatal("nil instance was accepted")
	}
	if node.hasMatchingInitialLeaderTryRecord(&instance{rec: InstanceRecord{}}, record.Attributes()) {
		t.Fatal("zero-reference instance was accepted")
	}
}

func TestPoolNestedByteSliceResetHonorsCapacityBounds(t *testing.T) {
	items := [][]byte{[]byte("one"), []byte("two")}
	retained := resetNestedByteSlicesForPool(items, 2, 8)
	smallBytes := []byte(items[0][:cap(items[0])])
	secondBytes := []byte(items[1][:cap(items[1])])
	if len(retained) != 0 || cap(retained) != cap(items) || smallBytes[0] != 0 || secondBytes[0] != 0 {
		t.Fatalf("small nested reset did not clear and retain storage: len=%d cap=%d items=%q", len(retained), cap(retained), items)
	}

	large := make([][]byte, 1, 3)
	large[0] = []byte("discard")
	if retained := resetNestedByteSlicesForPool(large, 2, 8); retained != nil || large[0] != nil {
		t.Fatalf("large nested reset retained storage: result=%#v large=%#v", retained, large)
	}
}

func TestCommandResetReleasesOwnedReferences(t *testing.T) {
	command := Command{
		ID:           CommandID{Client: 7, Sequence: 8},
		Kind:         CommandConfChange,
		Payload:      []byte("payload"),
		ConflictKeys: [][]byte{[]byte("key")},
	}
	command.Reset()
	if command.ID != (CommandID{}) || command.Kind != CommandUser || command.Payload != nil || command.ConflictKeys != nil {
		t.Fatalf("Command.Reset left owned state: %#v", command)
	}
}

func TestBootstrapErrorWrapsBaseError(t *testing.T) {
	err := bootstrapError(ErrBootstrapBounds, "field %s", "payload")
	if !errors.Is(err, ErrBootstrapBounds) || err.Error() != "epaxos: bootstrap input exceeds bound: field payload" {
		t.Fatalf("bootstrapError = %v", err)
	}
}
