package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"
	"gosuda.org/moreconsensus/epaxos"
)

func TestEPaxosRecordCodecV8TimingDomainsRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		domain    epaxos.TimingDomain
		processAt uint64
		pending   bool
	}{
		{name: "untimed", domain: epaxos.TimingDomainUntimed},
		{name: "logical", domain: epaxos.TimingDomainLogical, processAt: 41},
		{name: "toq immediate", domain: epaxos.TimingDomainTOQ},
		{name: "toq pending", domain: epaxos.TimingDomainTOQ, processAt: 73, pending: true},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := timingCodecRecordKV(epaxos.InstanceNum(i+1), tc.domain, tc.processAt, tc.pending)
			encoded := encodeEPaxosRecord(record)
			if encoded[0] != 8 {
				t.Fatalf("codec version=%d, want 8", encoded[0])
			}
			domainAt := timingDomainOffsetKV(record, encoded)
			if encoded[domainAt] != byte(tc.domain) {
				t.Fatalf("encoded timing domain=%d, want %d", encoded[domainAt], tc.domain)
			}
			if encoded[domainAt+1] != boolByteKV(tc.pending) || encoded[domainAt+2] != boolByteKV(record.FastPathEligible) {
				t.Fatalf("encoded timing suffix=%x, want domain/pending/fast=%d/%d/%d", encoded[domainAt:domainAt+3], tc.domain, boolByteKV(tc.pending), boolByteKV(record.FastPathEligible))
			}

			legacy := record
			legacy.TimingDomain = epaxos.TimingDomainUntimed
			legacy.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(legacy)
			legacyEncoded := encodeHistoricalEPaxosRecordKV(legacy, 7)
			if len(encoded) != len(legacyEncoded)+1 {
				t.Fatalf("v8 length=%d, v7 length=%d, want one added domain byte", len(encoded), len(legacyEncoded))
			}

			decoded, err := decodeEPaxosRecord(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.TimingDomain != tc.domain || decoded.ProcessAt != tc.processAt || decoded.TOQPending != tc.pending {
				t.Fatalf("decoded timing=(%d,%d,%t), want (%d,%d,%t)", decoded.TimingDomain, decoded.ProcessAt, decoded.TOQPending, tc.domain, tc.processAt, tc.pending)
			}
			if decoded.Checksum != record.Checksum || !epaxos.VerifyRecordChecksum(decoded) {
				t.Fatalf("decoded v8 record has noncanonical checksum %x", decoded.Checksum)
			}
		})
	}
}

func TestEPaxosRecordCodecV8RejectsInvalidTimingAndShape(t *testing.T) {
	invalidTiming := []struct {
		name      string
		domain    epaxos.TimingDomain
		processAt uint64
		pending   bool
		want      string
	}{
		{name: "untimed nonzero", domain: epaxos.TimingDomainUntimed, processAt: 1, want: "timing metadata"},
		{name: "logical zero", domain: epaxos.TimingDomainLogical, want: "timing metadata"},
		{name: "logical pending", domain: epaxos.TimingDomainLogical, processAt: 1, pending: true, want: "timing metadata"},
		{name: "untimed pending", domain: epaxos.TimingDomainUntimed, pending: true, want: "timing metadata"},
		{name: "unknown domain", domain: epaxos.TimingDomainTOQ + 1, processAt: 1, want: "timing domain"},
	}
	for i, tc := range invalidTiming {
		t.Run(tc.name, func(t *testing.T) {
			record := timingCodecRecordKV(epaxos.InstanceNum(i+20), tc.domain, tc.processAt, tc.pending)
			record.Checksum = epaxos.ChecksumRecord(record)
			if got, err := decodeEPaxosRecord(encodeEPaxosRecord(record)); err == nil || !strings.Contains(err.Error(), tc.want) || !reflect.DeepEqual(got, epaxos.InstanceRecord{}) {
				t.Fatalf("decode invalid timing got=%#v err=%v, want zero record and %q", got, err, tc.want)
			}
		})
	}

	valid := timingCodecRecordKV(40, epaxos.TimingDomainLogical, 91, false)
	encoded := encodeEPaxosRecord(valid)
	domainAt := timingDomainOffsetKV(valid, encoded)
	for _, tc := range []struct {
		name   string
		offset int
		value  byte
		want   string
	}{
		{name: "unknown encoded domain", offset: domainAt, value: 0xff, want: "timing domain"},
		{name: "nonboolean pending", offset: domainAt + 1, value: 2, want: "record flags"},
		{name: "nonboolean fast path", offset: domainAt + 2, value: 2, want: "record flags"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			corrupt[tc.offset] = tc.value
			if _, err := decodeEPaxosRecord(corrupt); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode error=%v, want containing %q", err, tc.want)
			}
		})
	}

	t.Run("unknown status", func(t *testing.T) {
		record := valid
		record.Status = epaxos.StatusExecuted + 1
		record.Checksum = epaxos.ChecksumRecord(record)
		if _, err := decodeEPaxosRecord(encodeEPaxosRecord(record)); err == nil || !strings.Contains(err.Error(), "record status") {
			t.Fatalf("decode unknown status error=%v", err)
		}
	})

	t.Run("unknown command kind", func(t *testing.T) {
		record := valid
		record.Command.Kind = epaxos.CommandConfChange + 1
		record.Checksum = epaxos.ChecksumRecord(record)
		if _, err := decodeEPaxosRecord(encodeEPaxosRecord(record)); err == nil || !strings.Contains(err.Error(), "command kind") {
			t.Fatalf("decode unknown command error=%v", err)
		}
	})

	t.Run("noncanonical varint", func(t *testing.T) {
		noncanonical := append([]byte(nil), encoded[:1]...)
		noncanonical = append(noncanonical, 0x81, 0x00)
		noncanonical = append(noncanonical, encoded[2:]...)
		if _, err := decodeEPaxosRecord(noncanonical); err == nil || !strings.Contains(err.Error(), "malformed epaxos record") {
			t.Fatalf("decode noncanonical varint error=%v", err)
		}
	})

	t.Run("authenticated domain bit flip", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		corrupt[domainAt] = byte(epaxos.TimingDomainTOQ)
		if _, err := decodeEPaxosRecord(corrupt); !errors.Is(err, epaxos.ErrChecksumMismatch) {
			t.Fatalf("decode domain bit flip error=%v, want %v", err, epaxos.ErrChecksumMismatch)
		}
	})

	t.Run("checksum mutation", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		corrupt[len(corrupt)-1] ^= 1
		if _, err := decodeEPaxosRecord(corrupt); !errors.Is(err, epaxos.ErrChecksumMismatch) {
			t.Fatalf("decode checksum mutation error=%v, want %v", err, epaxos.ErrChecksumMismatch)
		}
	})

	t.Run("every truncation rejects without mutation", func(t *testing.T) {
		for cut := range len(encoded) {
			truncated := append([]byte(nil), encoded[:cut]...)
			before := append([]byte(nil), truncated...)
			got, err := decodeEPaxosRecord(truncated)
			if err == nil {
				t.Fatalf("cut %d/%d decoded successfully", cut, len(encoded))
			}
			if !reflect.DeepEqual(got, epaxos.InstanceRecord{}) {
				t.Fatalf("cut %d returned partial record %#v", cut, got)
			}
			if !bytes.Equal(truncated, before) {
				t.Fatalf("cut %d mutated input from %x to %x", cut, before, truncated)
			}
		}
	})
}

func TestEPaxosRecordCodecV7PreservesAmbiguousTimingForCore(t *testing.T) {
	tests := []struct {
		name         string
		processAt    uint64
		pending      bool
		legacyDomain epaxos.TimingDomain
	}{
		{name: "logical-looking", processAt: 51, legacyDomain: epaxos.TimingDomainLogical},
		{name: "toq-looking pending", processAt: 87, pending: true, legacyDomain: epaxos.TimingDomainTOQ},
		{name: "toq-looking processed", processAt: 88, legacyDomain: epaxos.TimingDomainTOQ},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := timingCodecRecordKV(epaxos.InstanceNum(i+60), epaxos.TimingDomainUntimed, tc.processAt, tc.pending)
			record.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(record)
			encoded := encodeHistoricalEPaxosRecordKV(record, 7)
			originalChecksum := record.Checksum

			decoded, err := decodeEPaxosRecord(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.TimingDomain != epaxos.TimingDomainUntimed || decoded.ProcessAt != tc.processAt || decoded.TOQPending != tc.pending {
				t.Fatalf("decoded timing=(%d,%d,%t), want unassigned domain with (%d,%t)", decoded.TimingDomain, decoded.ProcessAt, decoded.TOQPending, tc.processAt, tc.pending)
			}
			if decoded.Checksum != originalChecksum || !epaxos.VerifyRecordChecksumWithoutTimingDomain(decoded) || epaxos.VerifyRecordChecksum(decoded) {
				t.Fatalf("v7 decode silently changed or canonicalized checksum: got=%x old=%x", decoded.Checksum, originalChecksum)
			}

			path := t.TempDir()
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if err := db.pebble.Set(epaxosRecordKey(record.Ref), encoded, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			baseConfig := epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: db.EPaxosStorage()}
			if tc.pending {
				inferred := baseConfig
				inferred.TOQ = true
				inferred.TOQClock = func() uint64 { return tc.processAt }
				inferred.MaxDeferredPreAccepts = 1
				inferred.TOQRuntime = &epaxos.TOQRuntimeConfig{
					Conf:        epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}},
					OneWayDelay: map[epaxos.ReplicaID]uint64{1: 0},
				}
				if _, err := epaxos.NewRawNode(inferred); err != nil {
					t.Fatalf("startup did not recognize authenticated v7 TOQ-pending evidence: %v", err)
				}
				return
			}
			if _, err := epaxos.NewRawNode(baseConfig); !errors.Is(err, epaxos.ErrInvalidConfig) || !strings.Contains(err.Error(), "ambiguous legacy timing domain") {
				t.Fatalf("startup without legacy domain error=%v, want explicit ambiguity rejection", err)
			}
			explicit := baseConfig
			legacyDomain := tc.legacyDomain
			explicit.LegacyProcessAtDomain = &legacyDomain
			if tc.legacyDomain == epaxos.TimingDomainLogical {
				explicit.TimeOptimization = true
			} else {
				explicit.TOQ = true
				explicit.TOQClock = func() uint64 { return tc.processAt }
			}
			if _, err := epaxos.NewRawNode(explicit); err != nil {
				t.Fatalf("startup with explicit legacy domain %d: %v", tc.legacyDomain, err)
			}
		})
	}
	t.Run("zero ProcessAt requires operator interpretation", func(t *testing.T) {
		record := timingCodecRecordKV(70, epaxos.TimingDomainUntimed, 0, false)
		record.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(record)
		encoded := encodeHistoricalEPaxosRecordKV(record, 7)
		decoded, err := decodeEPaxosRecord(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.TimingDomain != epaxos.TimingDomainUntimed || decoded.ProcessAt != 0 || decoded.TOQPending || decoded.Checksum != record.Checksum || !epaxos.VerifyRecordChecksumWithoutTimingDomain(decoded) {
			t.Fatalf("decoded zero-ProcessAt v7 record=%#v, want unchanged no-domain record", decoded)
		}

		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		if err := db.pebble.Set(epaxosRecordKey(record.Ref), encoded, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		baseConfig := epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: db.EPaxosStorage()}
		if _, err := epaxos.NewRawNode(baseConfig); !errors.Is(err, epaxos.ErrInvalidConfig) || !strings.Contains(err.Error(), "ambiguous legacy timing domain") {
			t.Fatalf("zero ProcessAt startup without operator choice error=%v", err)
		}

		untimed := epaxos.TimingDomainUntimed
		untimedConfig := baseConfig
		untimedConfig.LegacyProcessAtDomain = &untimed
		if _, err := epaxos.NewRawNode(untimedConfig); err != nil {
			t.Fatalf("zero ProcessAt explicit Untimed migration: %v", err)
		}

		toq := epaxos.TimingDomainTOQ
		toqConfig := baseConfig
		toqConfig.LegacyProcessAtDomain = &toq
		toqConfig.TOQ = true
		toqConfig.TOQClock = func() uint64 { return 0 }
		if _, err := epaxos.NewRawNode(toqConfig); err != nil {
			t.Fatalf("zero ProcessAt explicit TOQ migration: %v", err)
		}
	})
}

func TestEPaxosRecordCodecV1ThroughV7MigrateToNoDomainCanonical(t *testing.T) {
	for version := byte(1); version <= 7; version++ {
		t.Run("v"+string(rune('0'+version)), func(t *testing.T) {
			record := historicalTimingRecordKV(version)
			record.Checksum = checksumHistoricalEPaxosRecordKV(record, version)
			encoded := encodeHistoricalEPaxosRecordKV(record, version)

			decoded, err := decodeEPaxosRecord(encoded)
			if err != nil {
				t.Fatalf("decode v%d: %v", version, err)
			}
			if decoded.TimingDomain != epaxos.TimingDomainUntimed {
				t.Fatalf("decoded v%d assigned domain %d", version, decoded.TimingDomain)
			}
			wantChecksum := epaxos.ChecksumRecordWithoutTimingDomain(decoded)
			if decoded.Checksum != wantChecksum || !epaxos.VerifyRecordChecksumWithoutTimingDomain(decoded) {
				t.Fatalf("decoded v%d checksum=%x, want no-domain canonical %x", version, decoded.Checksum, wantChecksum)
			}
			if version == 7 && decoded.Checksum != record.Checksum {
				t.Fatalf("decoded v7 changed immediately previous checksum from %x to %x", record.Checksum, decoded.Checksum)
			}
			if version < 7 && decoded.Checksum == record.Checksum {
				t.Fatalf("decoded v%d retained pre-v7 checksum %x", version, decoded.Checksum)
			}
			if epaxos.VerifyRecordChecksum(decoded) {
				t.Fatalf("decoded v%d was silently promoted to a v8 checksum", version)
			}

			corrupt := append([]byte(nil), encoded...)
			corrupt[len(corrupt)-1] ^= 1
			if _, err := decodeEPaxosRecord(corrupt); !errors.Is(err, epaxos.ErrChecksumMismatch) {
				t.Fatalf("decode corrupt v%d checksum error=%v, want %v", version, err, epaxos.ErrChecksumMismatch)
			}
		})
	}
}

func TestTimingDomainReadyPersistenceReplayAndReopen(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hardState := epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}}, Tick: 99}
	records := []epaxos.InstanceRecord{
		timingCodecRecordKV(1, epaxos.TimingDomainUntimed, 0, false),
		timingCodecRecordKV(2, epaxos.TimingDomainLogical, 41, false),
		timingCodecRecordKV(3, epaxos.TimingDomainTOQ, 73, true),
	}
	ready := epaxos.Ready{HardState: hardState, Records: records, MustSync: true}
	if err := db.EPaxosStorage().ApplyReady(ready); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.EPaxosStorage().ApplyReady(ready); err != nil {
		_ = db.Close()
		t.Fatalf("exact replay: %v", err)
	}
	for _, record := range records {
		value, closer, err := db.pebble.Get(epaxosRecordKey(record.Ref))
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if len(value) == 0 || value[0] != epaxosRecordCodec {
			_ = closer.Close()
			_ = db.Close()
			t.Fatalf("persisted record %s codec=%x, want v8", record.Ref, value)
		}
		_ = closer.Close()
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	gotHardState, _, err := reopened.EPaxosStorage().InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if !gotHardState.Equal(hardState) {
		t.Fatalf("reopened hard state=%#v, want %#v", gotHardState, hardState)
	}
	loaded := make(map[epaxos.InstanceRef]epaxos.InstanceRecord)
	if err := reopened.EPaxosStorage().LoadInstances(func(record epaxos.InstanceRecord) error {
		loaded[record.Ref] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, want := range records {
		got, ok := loaded[want.Ref]
		if !ok || got.TimingDomain != want.TimingDomain || got.ProcessAt != want.ProcessAt || got.TOQPending != want.TOQPending || !epaxos.VerifyRecordChecksum(got) {
			t.Fatalf("reopened record %s=%#v, want timing (%d,%d,%t)", want.Ref, got, want.TimingDomain, want.ProcessAt, want.TOQPending)
		}
	}
}

func TestTimingDomainMalformedReadyDoesNotPartiallyWrite(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	base := timingCodecRecordKV(1, epaxos.TimingDomainUntimed, 0, false)
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{base}, MustSync: true}); err != nil {
		t.Fatal(err)
	}

	staged := timingCodecRecordKV(2, epaxos.TimingDomainLogical, 12, false)
	malformed := timingCodecRecordKV(3, epaxos.TimingDomainUntimed, 13, false)
	malformed.Checksum = epaxos.ChecksumRecord(malformed)
	hardState := epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}}, Tick: 13}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{HardState: hardState, Records: []epaxos.InstanceRecord{staged, malformed}, MustSync: true}); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("malformed replay error=%v, want %v", err, epaxos.ErrChecksumMismatch)
	}
	gotHardState, _, err := db.EPaxosStorage().InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if !gotHardState.Empty() {
		t.Fatalf("malformed replay partially persisted hard state %#v", gotHardState)
	}
	loaded := make(map[epaxos.InstanceRef]struct{})
	if err := db.EPaxosStorage().LoadInstances(func(record epaxos.InstanceRecord) error {
		loaded[record.Ref] = struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := loaded[base.Ref]; !ok || len(loaded) != 1 {
		t.Fatalf("records after malformed replay=%v, want only base %s", loaded, base.Ref)
	}
}

func TestTimingDomainCheckpointDigestRestoreAndRepair(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "source")
	db, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	hardState := epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}}, Tick: 100}
	records := []epaxos.InstanceRecord{
		timingCodecRecordKV(1, epaxos.TimingDomainLogical, 44, false),
		timingCodecRecordKV(2, epaxos.TimingDomainTOQ, 81, false),
	}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{HardState: hardState, Records: records, MustSync: true}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	if _, err := db.CheckpointWithMetadata(checkpointDir, CheckpointMetadata{SourceIdentity: "timing-codec", ReleaseChecksum: "sha256:timing-codec"}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyCheckpointWithInfo(checkpointDir); err != nil {
		t.Fatalf("verify timing checkpoint: %v", err)
	}

	for _, operation := range []struct {
		name string
		call func(string, string) error
	}{
		{name: "restore", call: RestoreCheckpoint},
		{name: "repair", call: RepairFromCheckpoint},
	} {
		t.Run(operation.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "node")
			if err := operation.call(target, checkpointDir); err != nil {
				t.Fatal(err)
			}
			restored, err := Open(target)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = restored.Close() }()
			loaded := make(map[epaxos.InstanceRef]epaxos.InstanceRecord)
			if err := restored.EPaxosStorage().LoadInstances(func(record epaxos.InstanceRecord) error {
				loaded[record.Ref] = record
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for _, want := range records {
				got, ok := loaded[want.Ref]
				if !ok || got.TimingDomain != want.TimingDomain || got.ProcessAt != want.ProcessAt || !epaxos.VerifyRecordChecksum(got) {
					t.Fatalf("%s record %s=%#v, want authenticated timing (%d,%d)", operation.name, want.Ref, got, want.TimingDomain, want.ProcessAt)
				}
			}
		})
	}
}

func timingCodecRecordKV(instance epaxos.InstanceNum, domain epaxos.TimingDomain, processAt uint64, pending bool) epaxos.InstanceRecord {
	ballot := epaxos.Ballot{Replica: 1}
	record := epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 1, Instance: instance, Conf: 1},
		Ballot:           ballot,
		RecordBallot:     ballot,
		Status:           epaxos.StatusPreAccepted,
		Seq:              uint64(instance),
		Deps:             []epaxos.InstanceNum{0},
		Command:          CommandForPut(900, uint64(instance), []byte("timing-codec"), []byte{byte(instance)}),
		FastPathEligible: true,
		ProcessAt:        processAt,
		TimingDomain:     domain,
		TOQPending:       pending,
	}
	if pending {
		record.RecordBallot = epaxos.Ballot{}
		record.Status = epaxos.StatusNone
		record.Seq = 0
		record.FastPathEligible = false
	}
	record.Checksum = epaxos.ChecksumRecord(record)
	return record
}

func timingDomainOffsetKV(record epaxos.InstanceRecord, encoded []byte) int {
	return len(encoded) - len(record.Checksum) - len(encodeConfChangeResultKV(record.ConfChangeResult)) - 3
}

func boolByteKV(value bool) byte {
	if value {
		return 1
	}
	return 0
}

func historicalTimingRecordKV(version byte) epaxos.InstanceRecord {
	ballot := epaxos.Ballot{Replica: 1}
	record := epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 1, Instance: epaxos.InstanceNum(100 + version), Conf: 1},
		Ballot:           ballot,
		RecordBallot:     ballot,
		Status:           epaxos.StatusPreAccepted,
		Seq:              uint64(100 + version),
		Deps:             []epaxos.InstanceNum{0},
		Command:          CommandForPut(901, uint64(version), []byte("historical-timing"), []byte{version}),
		FastPathEligible: version >= 2,
	}
	if version >= 3 {
		record.ProcessAt = uint64(200 + version)
		record.TOQPending = version%2 == 0
	}
	if version >= 4 {
		record.AcceptSeq = record.Seq + 1
		record.AcceptDeps = []epaxos.InstanceNum{0}
	}
	if version >= 6 {
		record.AcceptEvidence = []epaxos.AcceptEvidence{{Sender: 1, Seq: record.AcceptSeq, Deps: []epaxos.InstanceNum{0}}}
	}
	return record
}

func encodeHistoricalEPaxosRecordKV(record epaxos.InstanceRecord, version byte) []byte {
	out := []byte{version}
	out = binary.AppendUvarint(out, uint64(record.Ref.Replica))
	out = binary.AppendUvarint(out, uint64(record.Ref.Instance))
	out = binary.AppendUvarint(out, uint64(record.Ref.Conf))
	out = binary.AppendUvarint(out, record.Ballot.Epoch)
	out = binary.AppendUvarint(out, record.Ballot.Number)
	out = binary.AppendUvarint(out, uint64(record.Ballot.Replica))
	if version >= 5 {
		out = binary.AppendUvarint(out, record.RecordBallot.Epoch)
		out = binary.AppendUvarint(out, record.RecordBallot.Number)
		out = binary.AppendUvarint(out, uint64(record.RecordBallot.Replica))
	}
	out = binary.AppendUvarint(out, uint64(record.Status))
	out = binary.AppendUvarint(out, record.Seq)
	out = binary.AppendUvarint(out, uint64(len(record.Deps)))
	for _, dependency := range record.Deps {
		out = binary.AppendUvarint(out, uint64(dependency))
	}
	if version >= 4 {
		out = binary.AppendUvarint(out, record.AcceptSeq)
		out = binary.AppendUvarint(out, uint64(len(record.AcceptDeps)))
		for _, dependency := range record.AcceptDeps {
			out = binary.AppendUvarint(out, uint64(dependency))
		}
	}
	if version >= 6 {
		out = binary.AppendUvarint(out, uint64(len(record.AcceptEvidence)))
		for _, evidence := range record.AcceptEvidence {
			out = binary.AppendUvarint(out, uint64(evidence.Sender))
			out = binary.AppendUvarint(out, evidence.Seq)
			out = binary.AppendUvarint(out, uint64(len(evidence.Deps)))
			for _, dependency := range evidence.Deps {
				out = binary.AppendUvarint(out, uint64(dependency))
			}
		}
	}
	out = binary.AppendUvarint(out, record.Command.ID.Client)
	out = binary.AppendUvarint(out, record.Command.ID.Sequence)
	out = binary.AppendUvarint(out, uint64(record.Command.Kind))
	out = binary.AppendUvarint(out, uint64(len(record.Command.Payload)))
	out = append(out, record.Command.Payload...)
	out = binary.AppendUvarint(out, uint64(len(record.Command.ConflictKeys)))
	for _, key := range record.Command.ConflictKeys {
		out = binary.AppendUvarint(out, uint64(len(key)))
		out = append(out, key...)
	}
	if version >= 3 {
		out = binary.AppendUvarint(out, record.ProcessAt)
		out = append(out, boolByteKV(record.TOQPending))
	}
	if version >= 2 {
		out = append(out, boolByteKV(record.FastPathEligible))
	}
	if version >= 7 {
		out = append(out, encodeConfChangeResultKV(record.ConfChangeResult)...)
	}
	return append(out, record.Checksum[:]...)
}

func checksumHistoricalEPaxosRecordKV(record epaxos.InstanceRecord, version byte) [32]byte {
	hasher := blake3.New()
	writeUint64 := func(value uint64) {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], value)
		_, _ = hasher.Write(encoded[:])
	}
	writeByte := func(value byte) {
		_, _ = hasher.Write([]byte{value})
	}
	writeBytes := func(value []byte) {
		writeUint64(uint64(len(value)))
		_, _ = hasher.Write(value)
	}
	writeBallot := func(ballot epaxos.Ballot) {
		writeUint64(ballot.Epoch)
		writeUint64(ballot.Number)
		writeUint64(uint64(ballot.Replica))
	}

	writeUint64(uint64(record.Ref.Replica))
	writeUint64(uint64(record.Ref.Instance))
	writeUint64(uint64(record.Ref.Conf))
	writeBallot(record.Ballot)
	if version >= 5 {
		writeBallot(record.RecordBallot)
	}
	writeByte(byte(record.Status))
	writeUint64(record.Seq)
	writeUint64(uint64(len(record.Deps)))
	for _, dependency := range record.Deps {
		writeUint64(uint64(dependency))
	}
	if version >= 4 {
		writeUint64(record.AcceptSeq)
		writeUint64(uint64(len(record.AcceptDeps)))
		for _, dependency := range record.AcceptDeps {
			writeUint64(uint64(dependency))
		}
	}
	if version >= 6 {
		writeUint64(uint64(len(record.AcceptEvidence)))
		for _, evidence := range record.AcceptEvidence {
			writeUint64(uint64(evidence.Sender))
			writeUint64(evidence.Seq)
			writeUint64(uint64(len(evidence.Deps)))
			for _, dependency := range evidence.Deps {
				writeUint64(uint64(dependency))
			}
		}
	}
	writeUint64(record.Command.ID.Client)
	writeUint64(record.Command.ID.Sequence)
	writeByte(byte(record.Command.Kind))
	writeBytes(record.Command.Payload)
	writeUint64(uint64(len(record.Command.ConflictKeys)))
	for _, key := range record.Command.ConflictKeys {
		writeBytes(key)
	}
	if version >= 2 {
		writeByte(boolByteKV(record.FastPathEligible))
	}
	if version >= 3 {
		writeUint64(record.ProcessAt)
		writeByte(boolByteKV(record.TOQPending))
	}
	if version >= 7 {
		writeByte(byte(record.ConfChangeResult.Outcome))
		writeUint64(uint64(record.ConfChangeResult.Conf.ID))
		writeUint64(uint64(len(record.ConfChangeResult.Conf.Voters)))
		for _, voter := range record.ConfChangeResult.Conf.Voters {
			writeUint64(uint64(voter))
		}
	}
	var checksum [32]byte
	sum := hasher.Sum(checksum[:0])
	copy(checksum[:], sum)
	return checksum
}
