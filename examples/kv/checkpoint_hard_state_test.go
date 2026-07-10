package kv

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

func TestCheckpointHardStateManifestRoundTrip(t *testing.T) {
	_, checkpointDir, hardState, ref := newCheckpointHardStateFixture(t)
	info, err := VerifyCheckpointWithInfo(checkpointDir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Format != "manifest-v1" || info.ManifestVersion != checkpointManifestVersion || info.ManifestIdentity == "" ||
		info.SemanticStateDigest == "" || info.HardStateDigest == "" || info.RecordCount != 1 || info.AppliedCount != 1 ||
		info.SourceIdentity != "replica-1" || info.ReleaseChecksum != "sha256:test-release" || !info.CurrentTarget {
		t.Fatalf("checkpoint info=%#v, want complete authenticated current summary", info)
	}
	repeated, err := VerifyCheckpointWithInfo(checkpointDir)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.ManifestIdentity != info.ManifestIdentity || repeated.SemanticStateDigest != info.SemanticStateDigest {
		t.Fatalf("repeated verification changed deterministic identity: first=%#v second=%#v", info, repeated)
	}
	if err := VerifyLegacyCheckpoint(checkpointDir); err == nil || !strings.Contains(err.Error(), "refuses a current checkpoint manifest") {
		t.Fatalf("legacy verification accepted current manifest: %v", err)
	}
	checkpointDB, err := Open(checkpointDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = checkpointDB.Close() }()
	requirePebbleHardState(t, checkpointDB.EPaxosStorage(), hardState)
	found := false
	if err := checkpointDB.EPaxosStorage().LoadInstances(func(record epaxos.InstanceRecord) error {
		if record.Ref == ref {
			found = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("checkpoint omitted record %s", ref)
	}
}

func TestVerifyCheckpointRejectsHardStateCorruptionAndCausalMismatch(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(t *testing.T, checkpointDir string)
		want       error
		wantString string
	}{
		{
			name: "hard-state bit flip",
			mutate: func(t *testing.T, checkpointDir string) {
				mutateCheckpointHardState(t, checkpointDir, func(encoded []byte) []byte {
					encoded[10] ^= 1
					return encoded
				})
			},
			want: epaxos.ErrChecksumMismatch,
		},
		{
			name: "hard-state omission",
			mutate: func(t *testing.T, checkpointDir string) {
				mutateCheckpointHardState(t, checkpointDir, func([]byte) []byte { return nil })
			},
			wantString: "omitted epaxos hard state",
		},
		{
			name: "configuration ownership mismatch",
			mutate: func(t *testing.T, checkpointDir string) {
				mutateCheckpointHardState(t, checkpointDir, func([]byte) []byte {
					return encodeEPaxosHardState(epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{2}}, Tick: 42})
				})
			},
			wantString: "not owned by its pinned configuration",
		},
		{
			name: "tick regression",
			mutate: func(t *testing.T, checkpointDir string) {
				mutateCheckpointHardState(t, checkpointDir, func([]byte) []byte {
					return encodeEPaxosHardState(epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}}, Tick: 41})
				})
			},
			wantString: "hard-state digest mismatch",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, checkpointDir, _, _ := newCheckpointHardStateFixture(t)
			tc.mutate(t, checkpointDir)
			err := VerifyCheckpoint(checkpointDir)
			if err == nil {
				t.Fatal("VerifyCheckpoint accepted corrupted hard state")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("VerifyCheckpoint error=%v, want %v", err, tc.want)
			}
			if tc.wantString != "" && !strings.Contains(err.Error(), tc.wantString) {
				t.Fatalf("VerifyCheckpoint error=%v, want containing %q", err, tc.wantString)
			}
		})
	}
}

func TestVerifyCheckpointRejectsOmittedCausalConfigurationOutcome(t *testing.T) {
	dataDir := t.TempDir()
	db, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	payload := make([]byte, 9)
	payload[0] = byte(epaxos.ConfChangeAddVoter)
	binary.LittleEndian.PutUint64(payload[1:], 2)
	record := epaxos.InstanceRecord{
		Ref:          ref,
		Ballot:       epaxos.Ballot{Replica: 1},
		RecordBallot: epaxos.Ballot{Replica: 1},
		Status:       epaxos.StatusExecuted,
		Seq:          1,
		Deps:         []epaxos.InstanceNum{0},
		Command:      epaxos.Command{Kind: epaxos.CommandConfChange, Payload: payload},
		ConfChangeResult: epaxos.ConfChangeResult{
			Outcome: epaxos.ConfChangeApplied,
			Conf:    epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 2}},
		},
	}
	record.Checksum = epaxos.ChecksumRecord(record)
	hardState := epaxos.HardState{Conf: epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 2}}, Tick: 9}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{HardState: hardState, Records: []epaxos.InstanceRecord{record}, MustSync: true}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	if err := db.Checkpoint(checkpointDir); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	checkpointDB, err := pebble.Open(checkpointDir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	record.ConfChangeResult = epaxos.ConfChangeResult{}
	record.Checksum = epaxos.ChecksumRecord(record)
	if err := checkpointDB.Set(epaxosRecordKey(ref), encodeEPaxosRecord(record), pebble.Sync); err != nil {
		_ = checkpointDB.Close()
		t.Fatal(err)
	}
	if err := checkpointDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckpoint(checkpointDir); err == nil || !strings.Contains(err.Error(), "omitted its causal outcome") {
		t.Fatalf("VerifyCheckpoint omitted outcome error=%v", err)
	}
}

func TestVerifyCheckpointRejectsManifestAndFileAttacks(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(t *testing.T, checkpointDir string)
		wantString string
	}{
		{
			name: "manifest checksum tamper",
			mutate: func(t *testing.T, checkpointDir string) {
				encoded := readCheckpointManifestBytes(t, checkpointDir)
				encoded[len(encoded)-1] ^= 1
				writeCheckpointManifestBytes(t, checkpointDir, encoded)
			},
			wantString: "manifest checksum mismatch",
		},
		{
			name: "applied marker count tamper",
			mutate: func(t *testing.T, checkpointDir string) {
				manifest := decodedCheckpointManifestForMutation(t, checkpointDir)
				manifest.appliedCount++
				writeCheckpointManifestBytes(t, checkpointDir, encodeCheckpointManifest(manifest))
			},
			wantString: "applied-marker count mismatch",
		},
		{
			name: "extra file",
			mutate: func(t *testing.T, checkpointDir string) {
				if err := os.WriteFile(filepath.Join(checkpointDir, "EXTRA"), []byte("extra"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			wantString: "file count mismatch",
		},
		{
			name: "removed file",
			mutate: func(t *testing.T, checkpointDir string) {
				manifest, _, err := readCheckpointManifest(checkpointDir)
				if err != nil {
					t.Fatal(err)
				}
				for _, file := range manifest.files {
					if strings.HasPrefix(file.path, "OPTIONS-") {
						if err := os.Remove(filepath.Join(checkpointDir, filepath.FromSlash(file.path))); err != nil {
							t.Fatal(err)
						}
						return
					}
				}
				t.Fatal("checkpoint manifest had no removable OPTIONS file")
			},
			wantString: "file count mismatch",
		},
		{
			name: "tampered regular file",
			mutate: func(t *testing.T, checkpointDir string) {
				manifest, _, err := readCheckpointManifest(checkpointDir)
				if err != nil {
					t.Fatal(err)
				}
				for _, file := range manifest.files {
					if strings.HasPrefix(file.path, "OPTIONS-") {
						path := filepath.Join(checkpointDir, filepath.FromSlash(file.path))
						out, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := out.Write([]byte("tamper")); err != nil {
							_ = out.Close()
							t.Fatal(err)
						}
						if err := out.Close(); err != nil {
							t.Fatal(err)
						}
						return
					}
				}
				t.Fatal("checkpoint manifest had no tamperable OPTIONS file")
			},
			wantString: "",
		},
		{
			name: "symlink",
			mutate: func(t *testing.T, checkpointDir string) {
				if err := os.Symlink("CURRENT", filepath.Join(checkpointDir, "link")); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
			},
			wantString: "symlink",
		},
		{
			name: "path traversal",
			mutate: func(t *testing.T, checkpointDir string) {
				manifest := decodedCheckpointManifestForMutation(t, checkpointDir)
				manifest.files[0].path = "../escape"
				writeCheckpointManifestBytes(t, checkpointDir, encodeCheckpointManifest(manifest))
			},
			wantString: "invalid checkpoint manifest path",
		},
		{
			name: "duplicate entry",
			mutate: func(t *testing.T, checkpointDir string) {
				manifest := decodedCheckpointManifestForMutation(t, checkpointDir)
				manifest.files = append(manifest.files, manifest.files[0])
				sort.Slice(manifest.files, func(i, j int) bool { return manifest.files[i].path < manifest.files[j].path })
				writeCheckpointManifestBytes(t, checkpointDir, encodeCheckpointManifest(manifest))
			},
			wantString: "duplicate checkpoint manifest entry",
		},
		{
			name: "unknown version",
			mutate: func(t *testing.T, checkpointDir string) {
				encoded := readCheckpointManifestBytes(t, checkpointDir)
				encoded[len(checkpointManifestMagic)]++
				writeCheckpointManifestBytes(t, checkpointDir, encoded)
			},
			wantString: "unknown checkpoint manifest version",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, checkpointDir, _, _ := newCheckpointHardStateFixture(t)
			tc.mutate(t, checkpointDir)
			err := VerifyCheckpoint(checkpointDir)
			if err == nil || (tc.wantString != "" && !strings.Contains(err.Error(), tc.wantString)) {
				t.Fatalf("VerifyCheckpoint error=%v, want rejection containing %q", err, tc.wantString)
			}
		})
	}
}

func TestVerifyCheckpointLegacyModeIsExplicitAndNonClaiming(t *testing.T) {
	legacyDir := t.TempDir()
	db, err := Open(legacyDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckpoint(legacyDir); err == nil || !strings.Contains(err.Error(), "explicit legacy verification") {
		t.Fatalf("VerifyCheckpoint legacy error=%v", err)
	}
	info, err := VerifyLegacyCheckpointWithInfo(legacyDir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Format != "legacy" || info.ManifestIdentity != "" || info.CurrentTarget {
		t.Fatalf("legacy checkpoint info=%#v, want explicit non-claiming result", info)
	}
}

func TestRestoreAndRepairRejectInvalidCheckpointWithoutMutation(t *testing.T) {
	for _, operation := range []struct {
		name string
		call func(string, string) error
	}{
		{name: "restore", call: RestoreCheckpoint},
		{name: "repair", call: RepairFromCheckpoint},
	} {
		t.Run(operation.name, func(t *testing.T) {
			_, checkpointDir, _, _ := newCheckpointHardStateFixture(t)
			if err := os.WriteFile(filepath.Join(checkpointDir, "EXTRA"), []byte("tamper"), 0o600); err != nil {
				t.Fatal(err)
			}
			dataDir := t.TempDir()
			sentinel := filepath.Join(dataDir, "live-sentinel")
			if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := operation.call(dataDir, checkpointDir); err == nil || !strings.Contains(err.Error(), "file count mismatch") {
				t.Fatalf("%s error=%v, want manifest rejection", operation.name, err)
			}
			value, err := os.ReadFile(sentinel)
			if err != nil || string(value) != "unchanged" {
				t.Fatalf("%s mutated live data: value=%q err=%v", operation.name, value, err)
			}
		})
	}
}

func TestCheckpointRestoreAndRepairPreserveHardStateAndRecords(t *testing.T) {
	_, checkpointDir, hardState, ref := newCheckpointHardStateFixture(t)
	for _, operation := range []struct {
		name string
		call func(string, string) error
	}{
		{name: "restore", call: RestoreCheckpoint},
		{name: "repair", call: RepairFromCheckpoint},
	} {
		t.Run(operation.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "node")
			if err := operation.call(dataDir, checkpointDir); err != nil {
				t.Fatal(err)
			}
			db, err := Open(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			requirePebbleHardState(t, db.EPaxosStorage(), hardState)
			found := false
			if err := db.EPaxosStorage().LoadInstances(func(record epaxos.InstanceRecord) error {
				found = found || record.Ref == ref
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatalf("%s omitted record %s", operation.name, ref)
			}
			value, ok, err := db.Get([]byte("checkpoint-hard-state"))
			if err != nil || !ok || string(value) != "value" {
				t.Fatalf("%s restored KV value=%q ok=%v err=%v", operation.name, value, ok, err)
			}
		})
	}
}

func newCheckpointHardStateFixture(t *testing.T) (string, string, epaxos.HardState, epaxos.InstanceRef) {
	t.Helper()
	dataDir := t.TempDir()
	db, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	hardState := epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}}, Tick: 42}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	command := CommandForPut(801, 1, []byte("checkpoint-hard-state"), []byte("value"))
	record := hardStateTestRecord(ref, command)
	if err := db.ApplyReady(epaxos.Ready{
		HardState: hardState,
		Records:   []epaxos.InstanceRecord{record},
		Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: record.Seq, Deps: record.Deps, Command: command}},
		MustSync:  true,
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	if _, err := db.CheckpointWithMetadata(checkpointDir, CheckpointMetadata{SourceIdentity: "replica-1", ReleaseChecksum: "sha256:test-release"}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return dataDir, checkpointDir, hardState, ref
}

func mutateCheckpointHardState(t *testing.T, checkpointDir string, mutate func([]byte) []byte) {
	t.Helper()
	db, err := pebble.Open(checkpointDir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value, closer, err := db.Get(epaxosHardStateKey)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	encoded := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	encoded = mutate(encoded)
	if encoded == nil {
		err = db.Delete(epaxosHardStateKey, pebble.Sync)
	} else {
		err = db.Set(epaxosHardStateKey, encoded, pebble.Sync)
	}
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func decodedCheckpointManifestForMutation(t *testing.T, checkpointDir string) checkpointManifest {
	t.Helper()
	manifest, _, err := readCheckpointManifest(checkpointDir)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func readCheckpointManifestBytes(t *testing.T, checkpointDir string) []byte {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join(checkpointDir, checkpointManifestName))
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func writeCheckpointManifestBytes(t *testing.T, checkpointDir string, encoded []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(checkpointDir, checkpointManifestName), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}
