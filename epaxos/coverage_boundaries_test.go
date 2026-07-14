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
	built, err := BuildBootstrapChunk(chunk)
	if err != nil {
		t.Fatalf("BuildBootstrapChunk: %v", err)
	}
	if err := ValidateBootstrapChunk(built, fixture.identities[0].Replica, fixture.identities[0].Incarnation); err != nil {
		t.Fatalf("ValidateBootstrapChunk: %v", err)
	}

	encoded, err := EncodeBootstrapChunk([]byte{0xa5}, built)
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
	if !reflect.DeepEqual(decoded, built) {
		t.Fatalf("decoded chunk differs from built chunk:\n got %#v\nwant %#v", decoded, built)
	}
	if err := ValidateBootstrapChunk(decoded, fixture.identities[0].Replica, fixture.identities[0].Incarnation); err != nil {
		t.Fatalf("ValidateBootstrapChunk(decoded): %v", err)
	}

	invalidDigest := built
	invalidDigest.PayloadDigest[0]++
	unchanged, err := EncodeBootstrapChunk([]byte{0x7f}, invalidDigest)
	if !errors.Is(err, ErrBootstrapChunkConflict) {
		t.Fatalf("invalid payload digest error = %v, want ErrBootstrapChunkConflict", err)
	}
	if _, conflictErr := bootstrapChunkCanonicalBytes(invalidDigest); !errors.Is(conflictErr, ErrBootstrapChunkConflict) {
		t.Fatalf("invalid payload digest canonical error = %v, want ErrBootstrapChunkConflict", conflictErr)
	}
	if !bytes.Equal(unchanged, []byte{0x7f}) {
		t.Fatalf("invalid encode changed destination: %x", unchanged)
	}

	digestOffset := bytes.Index(frame, built.PayloadDigest[:])
	if digestOffset < 0 {
		t.Fatal("built payload digest not found in canonical frame")
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
			b[len(bootstrapChunkMagic)] = 3
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
			Identity: VoterIdentity{Replica: 1, Incarnation: 2},
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
	clone.LocalVoterState.Identity.Incarnation = 3
	clone.Frontiers[0].Frontier.Lanes[0].Sparse[0] = 6

	if state.HardState.Conf.Voters[0] != 1 || state.ConfigHistory[0].Conf.Voters[1] != 2 ||
		state.LocalVoterState.Conf.Voters[2] != 3 || state.LocalVoterState.Identity.Incarnation != 2 ||
		state.Frontiers[0].Frontier.Lanes[0].Sparse[0] != 2 {
		t.Fatalf("StorageState.Clone shared mutable storage: original=%#v", state)
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
