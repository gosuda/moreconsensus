package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"
	"gosuda.org/moreconsensus/epaxos"
)

func TestPebbleHardStateOnlyPersistenceRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		applyDB bool
	}{
		{name: "PebbleStorage"},
		{name: "DB", applyDB: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			hardState := epaxos.HardState{
				Conf: epaxos.ConfState{ID: 4, Voters: []epaxos.ReplicaID{1, 3, 7}},
				Tick: 42,
			}
			rd := epaxos.Ready{HardState: hardState, MustSync: true}
			if tc.applyDB {
				err = db.ApplyReady(rd)
			} else {
				err = db.EPaxosStorage().ApplyReady(rd)
			}
			if err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			requirePebbleHardState(t, db.EPaxosStorage(), hardState)
			loaded := 0
			if err := db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error {
				loaded++
				return nil
			}); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if loaded != 0 {
				_ = db.Close()
				t.Fatalf("hard-state-only batch loaded %d records", loaded)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close() }()
			requirePebbleHardState(t, reopened.EPaxosStorage(), hardState)
		})
	}
}

func TestPebbleHardStateCanonicalEncodingUsesDomainSeparatedChecksum(t *testing.T) {
	hardState := epaxos.HardState{
		Conf: epaxos.ConfState{ID: 0x0102030405060708, Voters: []epaxos.ReplicaID{1, 3}},
		Tick: 0x1112131415161718,
	}
	first := encodeEPaxosHardState(hardState)
	second := encodeEPaxosHardState(hardState)
	if !bytes.Equal(first, second) {
		t.Fatalf("canonical hard-state encoding is nondeterministic: first=%x second=%x", first, second)
	}
	wantPayload := []byte{
		1,
		1, 2, 3, 4, 5, 6, 7, 8,
		2,
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 3,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	}
	payload := first[:len(first)-epaxosHardStateChecksumSize]
	if !bytes.Equal(payload, wantPayload) {
		t.Fatalf("canonical hard-state payload=%x, want %x", payload, wantPayload)
	}
	plainChecksum := blake3.Sum256(payload)
	var storedChecksum [epaxosHardStateChecksumSize]byte
	copy(storedChecksum[:], first[len(payload):])
	if storedChecksum == plainChecksum {
		t.Fatal("hard-state checksum was not domain separated")
	}
	decoded, err := decodeEPaxosHardState(first)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.Equal(hardState) {
		t.Fatalf("decoded hard state=%#v, want %#v", decoded, hardState)
	}
}

func TestDBApplyReadyHardStateRecordAndMarkerAreAtomicAndReplayable(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hardState := epaxos.HardState{
		Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}},
		Tick: 9,
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	command := CommandForPut(71, 1, []byte("hard-state-atomic"), []byte("value"))
	record := hardStateTestRecord(ref, command)
	rd := epaxos.Ready{
		HardState: hardState,
		Records:   []epaxos.InstanceRecord{record},
		Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: record.Seq, Deps: record.Deps, Command: command}},
		MustSync:  true,
	}

	commitErr := errors.New("injected hard-state batch commit failure")
	db.newBatch = func() txnBatch {
		return &hardStateCommitErrorBatch{inner: db.pebble.NewBatch(), err: commitErr}
	}
	if err := db.ApplyReady(rd); !errors.Is(err, commitErr) {
		_ = db.Close()
		t.Fatalf("ApplyReady commit error=%v, want %v", err, commitErr)
	}
	requirePebbleHardState(t, db.EPaxosStorage(), epaxos.HardState{})
	requireNoHardStateTestRecord(t, db, ref)
	if done, err := db.appliedReadyCommand(ref); err != nil || done {
		_ = db.Close()
		t.Fatalf("marker after failed batch: done=%v err=%v", done, err)
	}
	if value, ok, err := db.Get([]byte("hard-state-atomic")); err != nil || ok {
		_ = db.Close()
		t.Fatalf("KV after failed batch: value=%q ok=%v err=%v", value, ok, err)
	}
	if got := db.LatestRecordTime(); got != 0 {
		_ = db.Close()
		t.Fatalf("latest record time after failed batch=%d, want 0", got)
	}

	db.newBatch = func() txnBatch { return db.pebble.NewBatch() }
	if err := db.ApplyReady(rd); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	requireAppliedHardStateTestBatch(t, db, hardState, ref, "value")
	if got := db.LatestRecordTime(); got != 1 {
		_ = db.Close()
		t.Fatalf("latest record time after apply=%d, want 1", got)
	}
	if err := db.ApplyReady(rd); err != nil {
		_ = db.Close()
		t.Fatalf("exact replay: %v", err)
	}
	if err := db.ApplyReady(rd); err != nil {
		_ = db.Close()
		t.Fatalf("second exact replay: %v", err)
	}
	if got := db.LatestRecordTime(); got != 1 {
		_ = db.Close()
		t.Fatalf("exact replay advanced latest record time to %d", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	requireAppliedHardStateTestBatch(t, reopened, hardState, ref, "value")
	if got := reopened.LatestRecordTime(); got != 1 {
		t.Fatalf("reopened latest record time=%d, want 1", got)
	}
}

func TestDBApplyReadyRejectsHardStateRegressionAndConflictWithoutMutation(t *testing.T) {
	base := epaxos.HardState{
		Conf: epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 2, 3}},
		Tick: 10,
	}
	tests := []struct {
		name      string
		hardState epaxos.HardState
	}{
		{
			name:      "tick regression",
			hardState: epaxos.HardState{Conf: base.Conf.Clone(), Tick: 9},
		},
		{
			name:      "configuration regression",
			hardState: epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1, 2, 3}}, Tick: 11},
		},
		{
			name:      "same configuration voters conflict",
			hardState: epaxos.HardState{Conf: epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 2, 4}}, Tick: 11},
		},
		{
			name:      "zero voter",
			hardState: epaxos.HardState{Conf: epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{0, 2, 3}}, Tick: 11},
		},
		{
			name:      "duplicate voter",
			hardState: epaxos.HardState{Conf: epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 2, 2}}, Tick: 11},
		},
		{
			name:      "unsorted voters",
			hardState: epaxos.HardState{Conf: epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 3, 2}}, Tick: 11},
		},
		{
			name:      "empty voter set",
			hardState: epaxos.HardState{Conf: epaxos.ConfState{ID: 2}, Tick: 11},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if err := db.ApplyReady(epaxos.Ready{HardState: base, MustSync: true}); err != nil {
				t.Fatal(err)
			}
			ref := epaxos.InstanceRef{Replica: 1, Instance: 7, Conf: 2}
			key := []byte("rejected-hard-state")
			command := CommandForPut(72, 1, key, []byte("must-not-write"))
			record := hardStateTestRecord(ref, command)
			rd := epaxos.Ready{
				HardState: tc.hardState,
				Records:   []epaxos.InstanceRecord{record},
				Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: record.Seq, Deps: record.Deps, Command: command}},
				MustSync:  true,
			}
			if err := db.ApplyReady(rd); !errors.Is(err, epaxos.ErrInvalidConfig) {
				t.Fatalf("ApplyReady error=%v, want %v", err, epaxos.ErrInvalidConfig)
			}
			requirePebbleHardState(t, db.EPaxosStorage(), base)
			requireNoHardStateTestRecord(t, db, ref)
			if done, err := db.appliedReadyCommand(ref); err != nil || done {
				t.Fatalf("marker after rejected batch: done=%v err=%v", done, err)
			}
			if value, ok, err := db.Get(key); err != nil || ok {
				t.Fatalf("KV after rejected batch: value=%q ok=%v err=%v", value, ok, err)
			}
			if got := db.LatestRecordTime(); got != 0 {
				t.Fatalf("rejected batch advanced latest record time to %d", got)
			}
		})
	}
}

func TestPebbleHardStateStrictDecodeRejectsCorruptionAndMalformedVoters(t *testing.T) {
	valid := encodeEPaxosHardState(epaxos.HardState{
		Conf: epaxos.ConfState{ID: 3, Voters: []epaxos.ReplicaID{1, 2, 4}},
		Tick: 17,
	})
	tests := []struct {
		name       string
		mutate     func([]byte) []byte
		want       error
		wantString string
	}{
		{
			name: "bit flip",
			mutate: func(src []byte) []byte {
				src[len(src)-epaxosHardStateChecksumSize-1] ^= 0x01
				return src
			},
			want: epaxos.ErrChecksumMismatch,
		},
		{
			name: "unknown version",
			mutate: func(src []byte) []byte {
				src[0]++
				return src
			},
			wantString: "version",
		},
		{
			name: "trailing byte",
			mutate: func(src []byte) []byte {
				return append(src, 0)
			},
			wantString: "trailing bytes",
		},
		{
			name: "zero voter count",
			mutate: func(src []byte) []byte {
				src[9] = 0
				return src
			},
			want: epaxos.ErrInvalidConfig,
		},
		{
			name: "too many voters",
			mutate: func(src []byte) []byte {
				src[9] = 8
				return src
			},
			want: epaxos.ErrInvalidConfig,
		},
		{
			name: "zero voter",
			mutate: func(src []byte) []byte {
				binary.BigEndian.PutUint64(src[10:18], 0)
				return resignHardStateTestBytes(src)
			},
			want: epaxos.ErrInvalidConfig,
		},
		{
			name: "duplicate voter",
			mutate: func(src []byte) []byte {
				binary.BigEndian.PutUint64(src[18:26], 1)
				return resignHardStateTestBytes(src)
			},
			want: epaxos.ErrInvalidConfig,
		},
		{
			name: "out of order voters",
			mutate: func(src []byte) []byte {
				binary.BigEndian.PutUint64(src[18:26], 5)
				binary.BigEndian.PutUint64(src[26:34], 4)
				return resignHardStateTestBytes(src)
			},
			want: epaxos.ErrInvalidConfig,
		},
		{
			name: "zero configuration id",
			mutate: func(src []byte) []byte {
				binary.BigEndian.PutUint64(src[1:9], 0)
				return resignHardStateTestBytes(src)
			},
			want: epaxos.ErrInvalidConfig,
		},
		{
			name: "truncated checksum",
			mutate: func(src []byte) []byte {
				return src[:len(src)-1]
			},
			wantString: "malformed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			encoded := tc.mutate(append([]byte(nil), valid...))
			if err := db.pebble.Set(epaxosHardStateKey, encoded, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			got, configs, err := db.EPaxosStorage().InitialState()
			if err == nil {
				t.Fatalf("InitialState accepted malformed value %x", encoded)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("InitialState error=%v, want %v", err, tc.want)
			}
			if tc.wantString != "" && !strings.Contains(err.Error(), tc.wantString) {
				t.Fatalf("InitialState error=%v, want text %q", err, tc.wantString)
			}
			if !got.Empty() || len(configs) != 0 {
				t.Fatalf("InitialState returned state on error: hard=%#v configs=%#v", got, configs)
			}

			ref := epaxos.InstanceRef{Replica: 1, Instance: 8, Conf: 3}
			record := hardStateTestRecord(ref, CommandForPut(73, 1, []byte("corrupt-state"), []byte("no-write")))
			next := epaxos.HardState{Conf: epaxos.ConfState{ID: 3, Voters: []epaxos.ReplicaID{1, 2, 4}}, Tick: 18}
			if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{HardState: next, Records: []epaxos.InstanceRecord{record}, MustSync: true}); err == nil {
				t.Fatal("ApplyReady accepted a transition over malformed durable hard state")
			}
			requireNoHardStateTestRecord(t, db, ref)
		})
	}
}

func TestPebbleHardStateLegacyAbsenceIsReadable(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	storage := db.EPaxosStorage()
	requirePebbleHardState(t, storage, epaxos.HardState{})
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	record := hardStateTestRecord(ref, CommandForPut(74, 1, []byte("legacy-record"), []byte("value")))
	if err := storage.ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{record}, MustSync: true}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	requirePebbleHardState(t, reopened.EPaxosStorage(), epaxos.HardState{})
	loaded := 0
	if err := reopened.EPaxosStorage().LoadInstances(func(got epaxos.InstanceRecord) error {
		loaded++
		if got.Ref != ref {
			t.Fatalf("legacy record ref=%s, want %s", got.Ref, ref)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if loaded != 1 {
		t.Fatalf("loaded legacy records=%d, want 1", loaded)
	}
}

func hardStateTestRecord(ref epaxos.InstanceRef, command epaxos.Command) epaxos.InstanceRecord {
	ballot := epaxos.Ballot{Number: 1, Replica: ref.Replica}
	record := epaxos.InstanceRecord{
		Ref:          ref,
		Ballot:       ballot,
		RecordBallot: ballot,
		Status:       epaxos.StatusExecuted,
		Seq:          1,
		Deps:         []epaxos.InstanceNum{0},
		Command:      command,
	}
	record.Checksum = epaxos.ChecksumRecord(record)
	return record
}

func requirePebbleHardState(t *testing.T, storage *PebbleStorage, want epaxos.HardState) {
	t.Helper()
	got, configs, err := storage.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("hard state=%#v, want %#v", got, want)
	}
	if len(configs) != 0 {
		t.Fatalf("configuration history=%#v, want empty", configs)
	}
}

func requireNoHardStateTestRecord(t *testing.T, db *DB, ref epaxos.InstanceRef) {
	t.Helper()
	found := false
	if err := db.EPaxosStorage().LoadInstances(func(record epaxos.InstanceRecord) error {
		if record.Ref == ref {
			found = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("rejected batch persisted record %s", ref)
	}
}

func requireAppliedHardStateTestBatch(t *testing.T, db *DB, hardState epaxos.HardState, ref epaxos.InstanceRef, wantValue string) {
	t.Helper()
	requirePebbleHardState(t, db.EPaxosStorage(), hardState)
	found := false
	if err := db.EPaxosStorage().LoadInstances(func(record epaxos.InstanceRecord) error {
		if record.Ref == ref {
			found = true
			if !epaxos.VerifyRecordChecksum(record) {
				t.Fatalf("persisted record %s has invalid checksum", ref)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("record %s was not persisted", ref)
	}
	if done, err := db.appliedReadyCommand(ref); err != nil || !done {
		t.Fatalf("applied marker for %s: done=%v err=%v", ref, done, err)
	}
	value, ok, err := db.Get([]byte("hard-state-atomic"))
	if err != nil || !ok || string(value) != wantValue {
		t.Fatalf("persisted KV: value=%q ok=%v err=%v, want %q", value, ok, err, wantValue)
	}
}

func resignHardStateTestBytes(encoded []byte) []byte {
	payloadSize := len(encoded) - epaxosHardStateChecksumSize
	checksum := checksumEPaxosHardState(encoded[:payloadSize])
	copy(encoded[payloadSize:], checksum[:])
	return encoded
}

type hardStateCommitErrorBatch struct {
	inner txnBatch
	err   error
}

func (batch *hardStateCommitErrorBatch) Set(key, value []byte, opts *pebble.WriteOptions) error {
	return batch.inner.Set(key, value, opts)
}

func (batch *hardStateCommitErrorBatch) Commit(*pebble.WriteOptions) error {
	return batch.err
}

func (batch *hardStateCommitErrorBatch) Close() error {
	return batch.inner.Close()
}
