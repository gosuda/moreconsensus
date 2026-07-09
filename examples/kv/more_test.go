package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"
	"gosuda.org/moreconsensus/epaxos"
)

func TestKVErrorAndPrefixBranches(t *testing.T) {
	if _, err := Open("/dev/null/not-a-directory"); err == nil {
		t.Fatal("expected open error")
	}
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: epaxos.Command{Kind: epaxos.CommandNoop}}); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: epaxos.Command{Payload: []byte{opPut, 9}}}); err == nil {
		t.Fatal("expected malformed command")
	}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: epaxos.Command{Payload: []byte{99, 0, 0}}}); err == nil {
		t.Fatal("expected unknown operation")
	}
	if _, ok, err := db.Get([]byte("missing")); err != nil || ok {
		t.Fatalf("missing get ok=%v err=%v", ok, err)
	}
	if err := db.PutVersion([]byte("pre-a"), []byte("1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("pre-b"), []byte("2"), 2); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("other"), []byte("3"), 3); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("pre-")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 2 {
		t.Fatalf("prefix scan len=%d", len(scan))
	}
	all, err := db.Scan(ScanOptions{})
	if err != nil || len(all) < 3 {
		t.Fatalf("full scan len=%d err=%v", len(all), err)
	}
	if _, _, ok := DecodeDataKey([]byte("bad"), 1); ok {
		t.Fatal("short key decoded")
	}
	badSep := append(EncodeUserPrefix(nil, 1, []byte("no-separator")), make([]byte, 8)...)
	if _, _, ok := DecodeDataKey(badSep, 1); ok {
		t.Fatal("bad separator decoded")
	}
	if limit := prefixLimit([]byte{0xff, 0xff}); limit != nil {
		t.Fatalf("prefix limit=%x", limit)
	}
	p := parser{err: true}
	if p.bytes() != nil {
		t.Fatal("errored parser returned bytes")
	}
	p = parser{b: []byte{0xff}}
	if p.bytes() != nil || !p.err {
		t.Fatal("bad varint did not fail")
	}
	if !IsNotFound(pebble.ErrNotFound) || IsNotFound(errors.New("x")) {
		t.Fatal("not-found helper failed")
	}
}

func TestParserUvarintAlreadyErroredReturnsZero(t *testing.T) {
	p := parser{b: []byte{1, 'x'}, err: true}
	if got := p.uvarint(); got != 0 {
		t.Fatalf("uvarint=%d", got)
	}
	if !p.err {
		t.Fatal("parser error state cleared")
	}
	if len(p.b) != 2 || p.b[0] != 1 || p.b[1] != 'x' {
		t.Fatalf("parser consumed input %x", p.b)
	}
}

func TestClusterErrorBranches(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cluster.Get(99, []byte("x")); err == nil {
		t.Fatal("expected unknown replica")
	}
	sentinel := errors.New("ready apply failed")
	cluster.readyAppliers[1] = func(epaxos.Ready) error { return sentinel }
	if err := cluster.Put([]byte("x"), []byte("y")); !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
	cluster, err = OpenCluster([]string{t.TempDir(), t.TempDir(), t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	if err := cluster.Nodes[1].Step(epaxos.Message{Type: epaxos.MsgCommit, From: 2, To: 1, Ref: epaxos.InstanceRef{Replica: 2, Instance: 90, Conf: 1}, Ballot: epaxos.Ballot{Replica: 2}, RecordBallot: epaxos.Ballot{Replica: 2}, Seq: 1, Deps: []epaxos.InstanceNum{0, 0, 0}, Command: epaxos.Command{Payload: []byte{opPut, 9}}}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drain(); err == nil {
		t.Fatal("expected malformed apply failure")
	}
}

func TestClusterPutRetriesOutstandingReadyAfterApplyFailure(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()

	realApply := cluster.readyAppliers[1]
	sentinel := errors.New("ready apply failed before durable write")
	attempts := 0
	cluster.readyAppliers[1] = func(epaxos.Ready) error {
		attempts++
		return sentinel
	}

	key := []byte("ready-retry-key")
	value := []byte("ready-retry-value")
	if err := cluster.Put(key, value); !errors.Is(err, sentinel) {
		t.Fatalf("Put err=%v, want %v", err, sentinel)
	}
	if attempts != 1 {
		t.Fatalf("failing applier attempts=%d, want 1", attempts)
	}
	if got, ok, err := cluster.Get(1, key); err != nil || ok {
		t.Fatalf("value before retry = %q ok=%v err=%v, want absent", got, ok, err)
	}

	nextBeforeRetry := cluster.next
	cluster.readyAppliers[1] = realApply
	if err := cluster.Drain(); err != nil {
		t.Fatal(err)
	}
	if cluster.next != nextBeforeRetry {
		t.Fatalf("Drain advanced proposal sequence from %d to %d; retry should not propose", nextBeforeRetry, cluster.next)
	}
	got, ok, err := cluster.Get(1, key)
	if err != nil || !ok || string(got) != string(value) {
		t.Fatalf("value after retry = %q ok=%v err=%v, want %q", got, ok, err, value)
	}
}

func TestOpenClusterPartialFailureClosesOpenedDBs(t *testing.T) {
	first := t.TempDir()
	_, err := OpenCluster([]string{first, "/dev/null/not-a-directory"})
	if err == nil {
		t.Fatal("expected partial open failure")
	}
	db, err := Open(first)
	if err != nil {
		t.Fatalf("first database remained locked after partial failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenClusterReopensDurableReadyPathWithoutCommandReplay(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("durable-key"), []byte("durable-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	executedBefore := make(map[epaxos.ReplicaID][]epaxos.InstanceRef, len(paths))
	for i := range paths {
		id := epaxos.ReplicaID(i + 1)
		executed := cluster.Nodes[id].Status().Executed
		if len(executed) == 0 {
			t.Fatalf("replica %d executed refs empty before close", id)
		}
		executedBefore[id] = append([]epaxos.InstanceRef(nil), executed...)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}

	cluster, err = OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	for id, wantRefs := range executedBefore {
		gotRefs := cluster.Nodes[id].Status().Executed
		for _, want := range wantRefs {
			if !hasExecutedRef(gotRefs, want) {
				t.Fatalf("replica %d executed refs after reopen = %#v, want %s", id, gotRefs, want)
			}
		}
	}
	restartedCommitted := 0
	for id, apply := range cluster.readyAppliers {
		id, apply := id, apply
		cluster.readyAppliers[id] = func(rd epaxos.Ready) error {
			restartedCommitted += len(rd.Committed)
			return apply(rd)
		}
	}
	if err := cluster.Drain(); err != nil {
		t.Fatal(err)
	}
	if restartedCommitted != 0 {
		t.Fatalf("restarted cluster emitted %d committed commands", restartedCommitted)
	}
	for i := range paths {
		id := epaxos.ReplicaID(i + 1)
		value, ok, err := cluster.Get(id, []byte("durable-key"))
		if err != nil || !ok || string(value) != "durable-value" {
			t.Fatalf("replica %d value=%q ok=%v err=%v", id, value, ok, err)
		}
	}
}

func TestOpenClusterRejectsBitFlippedPersistedEPaxosRecord(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	rec := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     ref,
		Ballot:  epaxos.Ballot{Replica: 1},
		Status:  epaxos.StatusCommitted,
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0},
		Command: CommandForPut(7, 1, []byte("corruption-key"), []byte("corruption-value")),
	})
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{rec}}); err != nil {
		t.Fatal(err)
	}
	loaded := 0
	if err := db.EPaxosStorage().LoadInstances(func(got epaxos.InstanceRecord) error {
		loaded++
		if got.Ref != ref || !epaxos.VerifyRecordChecksum(got) {
			return epaxos.ErrChecksumMismatch
		}
		return nil
	}); err != nil {
		t.Fatalf("valid persisted record failed before corruption: %v", err)
	}
	if loaded != 1 {
		t.Fatalf("loaded records before corruption=%d, want 1", loaded)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	pebbleDB, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value, closer, err := pebbleDB.Get(epaxosRecordKey(ref))
	if err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	payloadAt := bytes.Index(corrupted, []byte("corruption-value"))
	if payloadAt < 0 {
		_ = pebbleDB.Close()
		t.Fatalf("persisted EPaxos record did not contain payload bytes: %x", corrupted)
	}
	corrupted[payloadAt] ^= 0x01
	if err := pebbleDB.Set(epaxosRecordKey(ref), corrupted, pebble.Sync); err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	if err := pebbleDB.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = OpenCluster([]string{path})
	if !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("OpenCluster err=%v, want %v", err, epaxos.ErrChecksumMismatch)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("corrupt database remained locked after failed cluster open: %v", err)
	}
	defer func() { _ = db.Close() }()
	err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error { return nil })
	if !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("LoadInstances err=%v, want %v", err, epaxos.ErrChecksumMismatch)
	}
}

func TestCheckpointRestoreRecoversBitFlippedPersistedEPaxosRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node")
	cluster, err := OpenCluster([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("restore-key"), []byte("restore-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := cluster.DBs[1].Checkpoint(checkpoint); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}

	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	corruptPersistedEPaxosRecordValue(t, path, ref, []byte("restore-value"))
	_, err = OpenCluster([]string{path})
	if !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("OpenCluster on corrupt data err=%v, want %v", err, epaxos.ErrChecksumMismatch)
	}

	if err := RestoreCheckpoint(path, checkpoint); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenCluster([]string{path})
	if err != nil {
		t.Fatalf("OpenCluster after checkpoint restore failed: %v", err)
	}
	defer func() { _ = recovered.Close() }()
	value, ok, err := recovered.Get(1, []byte("restore-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "restore-value" {
		t.Fatalf("restored value ok=%v value=%q, want restore-value", ok, value)
	}
	if err := recovered.Put([]byte("restore-key"), []byte("after-restore")); err != nil {
		t.Fatalf("put after checkpoint restore failed: %v", err)
	}
	value, ok, err = recovered.Get(1, []byte("restore-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "after-restore" {
		t.Fatalf("post-restore value ok=%v value=%q, want after-restore", ok, value)
	}
}

func TestCheckpointRepairRecoversBitFlippedPersistedEPaxosRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node")
	cluster, err := OpenCluster([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("repair-key"), []byte("checkpoint-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := cluster.DBs[1].Checkpoint(checkpoint); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}

	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	corruptPersistedEPaxosRecordValue(t, path, ref, []byte("checkpoint-value"))
	_, err = OpenCluster([]string{path})
	if !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("OpenCluster on corrupt live data err=%v, want %v", err, epaxos.ErrChecksumMismatch)
	}

	if err := RepairFromCheckpoint(path, checkpoint); err != nil {
		t.Fatalf("RepairFromCheckpoint failed: %v", err)
	}
	recovered, err := OpenCluster([]string{path})
	if err != nil {
		t.Fatalf("OpenCluster after checkpoint repair failed: %v", err)
	}
	defer func() { _ = recovered.Close() }()
	value, ok, err := recovered.Get(1, []byte("repair-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "checkpoint-value" {
		t.Fatalf("repaired value ok=%v value=%q, want checkpoint-value", ok, value)
	}
	if err := recovered.Put([]byte("repair-key"), []byte("after-repair")); err != nil {
		t.Fatalf("put after checkpoint repair failed: %v", err)
	}
	value, ok, err = recovered.Get(1, []byte("repair-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "after-repair" {
		t.Fatalf("post-repair value ok=%v value=%q, want after-repair", ok, value)
	}
}

func TestCheckpointRepairRejectsCorruptCheckpointAndLeavesLiveData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node")
	cluster, err := OpenCluster([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("checkpoint-key"), []byte("checkpoint-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := cluster.DBs[1].Checkpoint(checkpoint); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("live-only-key"), []byte("live-only-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}

	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	corruptPersistedEPaxosRecordValue(t, checkpoint, ref, []byte("checkpoint-value"))
	if err := RepairFromCheckpoint(path, checkpoint); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("RepairFromCheckpoint with corrupt checkpoint err=%v, want %v", err, epaxos.ErrChecksumMismatch)
	}

	live, err := OpenCluster([]string{path})
	if err != nil {
		t.Fatalf("OpenCluster after rejected repair failed: %v", err)
	}
	defer func() { _ = live.Close() }()
	value, ok, err := live.Get(1, []byte("checkpoint-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "checkpoint-value" {
		t.Fatalf("original live checkpoint-key ok=%v value=%q, want checkpoint-value", ok, value)
	}
	value, ok, err = live.Get(1, []byte("live-only-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "live-only-value" {
		t.Fatalf("live-only key after rejected repair ok=%v value=%q, want live-only-value", ok, value)
	}
}

func TestVerifyCheckpointAndRepairFromCheckpointRejectMissingCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*testing.T, string) error
	}{
		{
			name: "verify",
			call: func(t *testing.T, checkpoint string) error {
				return VerifyCheckpoint(checkpoint)
			},
		},
		{
			name: "repair",
			call: func(t *testing.T, checkpoint string) error {
				return RepairFromCheckpoint(filepath.Join(t.TempDir(), "data"), checkpoint)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkpoint := filepath.Join(t.TempDir(), "missing-checkpoint")
			if err := tc.call(t, checkpoint); err == nil {
				t.Fatalf("%s accepted missing checkpoint directory", tc.name)
			}
			if _, err := os.Stat(checkpoint); !os.IsNotExist(err) {
				t.Fatalf("%s left checkpoint path stat err=%v, want not exist", tc.name, err)
			}
		})
	}
}

func TestVerifyCheckpointRejectsEmptyPathAndCorruptRecords(t *testing.T) {
	if err := VerifyCheckpoint(""); err == nil || !strings.Contains(err.Error(), "checkpoint path") {
		t.Fatalf("VerifyCheckpoint empty path err=%v, want checkpoint path error", err)
	}

	path := filepath.Join(t.TempDir(), "node")
	cluster, err := OpenCluster([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Put([]byte("verify-key"), []byte("verify-value")); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := cluster.DBs[1].Checkpoint(checkpoint); err != nil {
		_ = cluster.Close()
		t.Fatal(err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}

	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	corruptPersistedEPaxosRecordValue(t, checkpoint, ref, []byte("verify-value"))
	if err := VerifyCheckpoint(checkpoint); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("VerifyCheckpoint corrupt checkpoint err=%v, want %v", err, epaxos.ErrChecksumMismatch)
	}
}

func TestRecoverReplicaFromLiveCheckpointRestoresStoppedReplica(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()

	beforeKey := []byte("live-recovery-before")
	if err := cluster.Put(beforeKey, []byte("before-value")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StopReplica(2); err != nil {
		t.Fatal(err)
	}
	corruptPersistedEPaxosRecordValue(t, paths[1], epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, []byte("before-value"))

	duringKey := []byte("live-recovery-during")
	if err := cluster.Put(duringKey, []byte("during-value")); err != nil {
		t.Fatal(err)
	}

	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], checkpoint); err != nil {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint failed: %v", err)
	}
	assertReplicaValue(t, cluster, 2, beforeKey, "before-value")
	assertReplicaValue(t, cluster, 2, duringKey, "during-value")

	futureKey := []byte("live-recovery-after")
	if err := cluster.Put(futureKey, []byte("after-value")); err != nil {
		t.Fatalf("post-recovery Put failed: %v", err)
	}
	for _, id := range []epaxos.ReplicaID{1, 2, 3} {
		assertReplicaValue(t, cluster, id, futureKey, "after-value")
	}
}

func TestRecoverReplicaFromLiveCheckpointRejectsUnsupportedSource(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()

	if err := cluster.Put([]byte("live-supported"), []byte("supported-value")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StopReplica(2); err != nil {
		t.Fatal(err)
	}
	corruptPersistedEPaxosRecordValue(t, paths[1], epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, []byte("supported-value"))

	unsupportedRef := epaxos.InstanceRef{Replica: 1, Instance: 99, Conf: 1}
	unsupportedRecord := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     unsupportedRef,
		Ballot:  epaxos.Ballot{Replica: 1},
		Status:  epaxos.StatusCommitted,
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0, 0, 0},
		Command: CommandForPut(91, 1, []byte("source-only"), []byte("source-only-value")),
	})
	if err := cluster.DBs[1].ApplyReady(epaxos.Ready{
		Records: []epaxos.InstanceRecord{unsupportedRecord},
		Committed: []epaxos.CommittedCommand{{
			Ref:     unsupportedRef,
			Seq:     unsupportedRecord.Seq,
			Deps:    append([]epaxos.InstanceNum(nil), unsupportedRecord.Deps...),
			Command: unsupportedRecord.Command,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	err = cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], checkpoint)
	if err == nil || !strings.Contains(err.Error(), "live support") {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint err=%v, want live support rejection", err)
	}
	if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
		t.Fatalf("rejected recovery reopened target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
	}
	assertEPaxosChecksumMismatch(t, paths[1])
}

func TestRecoverReplicaFromLiveCheckpointRejectsTargetOwnedFloorMismatch(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()

	if err := cluster.Put([]byte("target-owned-floor-seed"), []byte("seed-value")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StopReplica(2); err != nil {
		t.Fatal(err)
	}
	corruptPersistedEPaxosRecordValue(t, paths[1], epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, []byte("seed-value"))

	targetOwnedRef := epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	command := CommandForPut(95, 1, []byte("target-owned-floor-key"), []byte("target-owned-floor-value"))
	checkpointRecord := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     targetOwnedRef,
		Ballot:  epaxos.Ballot{Replica: 2},
		Status:  epaxos.StatusPreAccepted,
		Seq:     7,
		Deps:    []epaxos.InstanceNum{0, 0, 0},
		Command: command,
	})
	livePeerRecord := checkpointRecord.Clone()
	livePeerRecord.Ballot = epaxos.Ballot{Number: 1, Replica: 3}
	livePeerRecord.RecordBallot = livePeerRecord.Ballot
	livePeerRecord.Status = epaxos.StatusAccepted
	livePeerRecord.Checksum = epaxos.ChecksumRecord(livePeerRecord)

	if err := cluster.DBs[1].EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{checkpointRecord}}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.DBs[3].EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{livePeerRecord}}); err != nil {
		t.Fatal(err)
	}

	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	err = cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], checkpoint)
	if err == nil || !strings.Contains(err.Error(), "target-owned record") {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint err=%v, want target-owned tuple mismatch rejection", err)
	}
	if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
		t.Fatalf("rejected recovery reopened target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
	}
	assertEPaxosChecksumMismatch(t, paths[1])
}

func TestVerifyCheckpointRejectsSemanticDataCorruption(t *testing.T) {
	t.Run("data value changed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "node")
		cluster, err := OpenCluster([]string{path})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.Put([]byte("semantic-key"), []byte("semantic-value")); err != nil {
			_ = cluster.Close()
			t.Fatal(err)
		}
		checkpoint := filepath.Join(t.TempDir(), "checkpoint")
		if err := cluster.DBs[1].Checkpoint(checkpoint); err != nil {
			_ = cluster.Close()
			t.Fatal(err)
		}
		if err := cluster.Close(); err != nil {
			t.Fatal(err)
		}

		overwriteCheckpointDataValue(t, checkpoint, []byte("semantic-key"), 1, []byte("mutated-value"))
		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "data rows do not match applied epaxos commands") {
			t.Fatalf("VerifyCheckpoint mutated data err=%v, want semantic data mismatch", err)
		}
	})

	t.Run("committed marker deleted", func(t *testing.T) {
		checkpoint := filepath.Join(t.TempDir(), "checkpoint")
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForPut(92, 1, []byte("marker-key"), []byte("marker-value")),
		})
		writeAppliedCheckpointRecord(t, checkpoint, record)
		deleteCheckpointAppliedMarker(t, checkpoint, ref)

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "data rows do not match applied epaxos commands") {
			t.Fatalf("VerifyCheckpoint deleted marker err=%v, want semantic data mismatch", err)
		}
	})
}

func TestVerifyCheckpointCrashWindowMarkerRules(t *testing.T) {
	t.Run("committed user with applied marker", func(t *testing.T) {
		checkpoint := filepath.Join(t.TempDir(), "checkpoint")
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForPut(93, 1, []byte("committed-window"), []byte("committed-value")),
		})
		writeAppliedCheckpointRecord(t, checkpoint, record)

		if err := VerifyCheckpoint(checkpoint); err != nil {
			t.Fatalf("VerifyCheckpoint rejected committed applied user record: %v", err)
		}
	})

	t.Run("executed user missing applied marker", func(t *testing.T) {
		checkpoint := filepath.Join(t.TempDir(), "checkpoint")
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForPut(94, 1, []byte("executed-user"), []byte("executed-value")),
		})
		writeCheckpointRecordOnly(t, checkpoint, record)

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "missing applied marker") {
			t.Fatalf("VerifyCheckpoint executed user without marker err=%v, want missing applied marker", err)
		}
	})

	t.Run("executed noop missing applied marker", func(t *testing.T) {
		checkpoint := filepath.Join(t.TempDir(), "checkpoint")
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: epaxos.Command{Kind: epaxos.CommandNoop},
		})
		writeCheckpointRecordOnly(t, checkpoint, record)

		if err := VerifyCheckpoint(checkpoint); err != nil {
			t.Fatalf("VerifyCheckpoint rejected executed noop without marker: %v", err)
		}
	})
}

func TestVerifyCheckpointRejectsMalformedConsensusAndMarkerState(t *testing.T) {
	t.Run("malformed epaxos record key", func(t *testing.T) {
		checkpoint := t.TempDir()
		setRawPebbleKV(t, checkpoint, []byte{epaxosStorePrefix, epaxosRecordEntry, 0}, []byte{epaxosRecordCodec})

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "malformed epaxos record key") {
			t.Fatalf("VerifyCheckpoint err=%v, want malformed record key", err)
		}
	})

	t.Run("record key ref must match authenticated value ref", func(t *testing.T) {
		checkpoint := t.TempDir()
		keyRef := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		valueRef := epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     valueRef,
			Ballot:  epaxos.Ballot{Replica: 2},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0, 0},
			Command: CommandForPut(100, 1, []byte("ref-mismatch"), []byte("value")),
		})
		setRawPebbleKV(t, checkpoint, epaxosRecordKey(keyRef), encodeEPaxosRecord(record))

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "key/value ref mismatch") {
			t.Fatalf("VerifyCheckpoint err=%v, want record key/value mismatch", err)
		}
	})

	t.Run("malformed applied marker key", func(t *testing.T) {
		checkpoint := t.TempDir()
		setRawPebbleKV(t, checkpoint, []byte{epaxosStorePrefix, epaxosAppliedEntry, 0}, []byte{1})

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "malformed epaxos applied marker key") {
			t.Fatalf("VerifyCheckpoint err=%v, want malformed marker key", err)
		}
	})

	t.Run("applied marker value must be canonical", func(t *testing.T) {
		checkpoint := t.TempDir()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record := checkpointPutRecord(ref, "bad-marker", "value")
		writeCheckpointRecordOnly(t, checkpoint, record)
		setRawPebbleKV(t, checkpoint, epaxosAppliedKey(ref), []byte{2})

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "malformed epaxos applied marker") {
			t.Fatalf("VerifyCheckpoint err=%v, want malformed marker value", err)
		}
	})

	t.Run("applied marker must reference an existing record", func(t *testing.T) {
		checkpoint := t.TempDir()
		setRawPebbleKV(t, checkpoint, epaxosAppliedKey(epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}), []byte{1})

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "has no epaxos record") {
			t.Fatalf("VerifyCheckpoint err=%v, want marker without record rejection", err)
		}
	})

	t.Run("applied marker must not point at uncommitted record", func(t *testing.T) {
		checkpoint := t.TempDir()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusAccepted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForPut(101, 1, []byte("uncommitted-marker"), []byte("value")),
		})
		writeCheckpointRecordOnly(t, checkpoint, record)
		setRawPebbleKV(t, checkpoint, epaxosAppliedKey(ref), []byte{1})

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "references uncommitted epaxos record") {
			t.Fatalf("VerifyCheckpoint err=%v, want uncommitted marker rejection", err)
		}
	})

	t.Run("applied record command must be parseable", func(t *testing.T) {
		checkpoint := t.TempDir()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: epaxos.Command{ID: epaxos.CommandID{Client: 102, Sequence: 1}, Payload: []byte{opPut, 9}},
		})
		writeCheckpointRecordOnly(t, checkpoint, record)
		setRawPebbleKV(t, checkpoint, epaxosAppliedKey(ref), []byte{1})

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "applied command") || !strings.Contains(err.Error(), "malformed command") {
			t.Fatalf("VerifyCheckpoint err=%v, want malformed applied command rejection", err)
		}
	})
}

func TestVerifyCheckpointRejectsMalformedDataRows(t *testing.T) {
	tests := []struct {
		name    string
		write   func(t *testing.T, checkpoint string)
		wantErr string
	}{
		{
			name: "malformed data key",
			write: func(t *testing.T, checkpoint string) {
				setRawPebbleKV(t, checkpoint, []byte{recordPrefix, 0}, []byte{valueRecord, 'x'})
			},
			wantErr: "malformed data key",
		},
		{
			name: "invalid user key",
			write: func(t *testing.T, checkpoint string) {
				setRawPebbleKV(t, checkpoint, EncodeDataKey(nil, 1, []byte("bad\x00key"), 1), []byte{valueRecord, 'x'})
			},
			wantErr: "key must be non-empty",
		},
		{
			name: "empty value",
			write: func(t *testing.T, checkpoint string) {
				setRawPebbleKV(t, checkpoint, EncodeDataKey(nil, 1, []byte("empty-value"), 1), nil)
			},
			wantErr: "empty data value",
		},
		{
			name: "delete value must have no payload",
			write: func(t *testing.T, checkpoint string) {
				setRawPebbleKV(t, checkpoint, EncodeDataKey(nil, 1, []byte("delete-payload"), 1), []byte{deleteRecord, 'x'})
			},
			wantErr: "malformed delete value",
		},
		{
			name: "unknown value kind",
			write: func(t *testing.T, checkpoint string) {
				setRawPebbleKV(t, checkpoint, EncodeDataKey(nil, 1, []byte("unknown-kind"), 1), []byte{99})
			},
			wantErr: "unknown data value kind",
		},
		{
			name: "timestamp positions must be dense",
			write: func(t *testing.T, checkpoint string) {
				setRawPebbleKV(t, checkpoint, EncodeDataKey(nil, 1, []byte("gap"), 2), []byte{valueRecord, 'x'})
			},
			wantErr: "not dense position",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkpoint := t.TempDir()
			tt.write(t, checkpoint)

			if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("VerifyCheckpoint err=%v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestCheckpointCommandGroupClassifiesAndRejectsCommands(t *testing.T) {
	tests := []struct {
		name       string
		cmd        epaxos.Command
		wantWrites bool
		wantGroup  string
		wantErr    string
	}{
		{name: "noop has no writes", cmd: epaxos.Command{Kind: epaxos.CommandNoop}},
		{name: "empty payload has no writes", cmd: epaxos.Command{}},
		{name: "put", cmd: CommandForPut(110, 1, []byte("put-key"), []byte("put-value")), wantWrites: true, wantGroup: checkpointGroupKey([]string{checkpointAtomKey(valueRecord, []byte("put-key"), []byte("put-value"))})},
		{name: "put malformed", cmd: epaxos.Command{Payload: []byte{opPut, 9}}, wantErr: "malformed command"},
		{name: "put invalid key", cmd: epaxos.Command{Payload: appendKVCommand(opPut, []byte("bad\x00put"), []byte("value"))}, wantErr: "key must be non-empty"},
		{name: "delete", cmd: CommandForDelete(110, 2, []byte("delete-key")), wantWrites: true, wantGroup: checkpointGroupKey([]string{checkpointAtomKey(deleteRecord, []byte("delete-key"), nil)})},
		{name: "delete malformed", cmd: epaxos.Command{Payload: []byte{opDelete, 1, 'k', 1}}, wantErr: "malformed command"},
		{name: "delete invalid key", cmd: epaxos.Command{Payload: appendKVCommand(opDelete, []byte("bad\x00delete"), nil)}, wantErr: "key must be non-empty"},
		{name: "txn malformed count", cmd: epaxos.Command{Payload: []byte{opTxn, 0xff}}, wantErr: "malformed command"},
		{name: "txn missing operation", cmd: epaxos.Command{Payload: []byte{opTxn, 1}}, wantErr: "malformed command"},
		{name: "txn invalid key", cmd: CommandForTxn(110, 3, []TxnOp{{Key: []byte("bad\x00txn"), Value: []byte("value")}}), wantErr: "key must be non-empty"},
		{name: "txn unknown operation", cmd: epaxos.Command{Payload: appendKVFields([]byte{opTxn, 1, 99}, []byte("unknown-op-key"), []byte("value"))}, wantErr: "unknown transaction op 99"},
		{name: "txn trailing bytes", cmd: epaxos.Command{Payload: []byte{opTxn, 0, 1}}, wantErr: "malformed command"},
		{name: "empty txn has no writes", cmd: CommandForTxn(110, 4, nil)},
		{
			name: "txn groups final effect per key including delete",
			cmd: CommandForTxn(110, 5, []TxnOp{
				{Key: []byte("dup"), Value: []byte("first")},
				{Key: []byte("other"), Value: []byte("kept")},
				{Key: []byte("dup"), Value: []byte("last")},
				{Delete: true, Key: []byte("other")},
			}),
			wantWrites: true,
			wantGroup: checkpointGroupKey([]string{
				checkpointAtomKey(valueRecord, []byte("dup"), []byte("last")),
				checkpointAtomKey(deleteRecord, []byte("other"), nil),
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group, writes, err := checkpointCommandGroup(tt.cmd)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("checkpointCommandGroup err=%v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("checkpointCommandGroup err=%v", err)
			}
			if writes != tt.wantWrites || group != tt.wantGroup {
				t.Fatalf("checkpointCommandGroup writes=%v group=%q, want writes=%v group=%q", writes, group, tt.wantWrites, tt.wantGroup)
			}
		})
	}
}

func TestCheckpointTimestampAssignmentHonorsDependenciesAndRefOrdering(t *testing.T) {
	ref1 := epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	ref2 := epaxos.InstanceRef{Replica: 2, Instance: 2, Conf: 1}
	group := checkpointGroupKey([]string{checkpointAtomKey(valueRecord, []byte("same-key"), []byte("same-value"))})
	state := checkpointState{
		records: map[epaxos.InstanceRef]epaxos.InstanceRecord{
			ref1: {Ref: ref1, Deps: []epaxos.InstanceNum{0, 0, 0}},
			ref2: {Ref: ref2, Deps: []epaxos.InstanceNum{0, 1, 0}},
		},
		writeGroups: map[epaxos.InstanceRef]string{
			ref1: group,
			ref2: group,
		},
	}
	groups := []checkpointDataGroup{{timestamp: 1, key: group}, {timestamp: 2, key: group}}
	candidates := map[string][]epaxos.InstanceRef{group: {ref1, ref2}}
	if err := assignCheckpointTimestamps(groups, candidates, state); err != nil {
		t.Fatalf("assignCheckpointTimestamps rejected dependency-ordered writes: %v", err)
	}

	reversed := map[string][]epaxos.InstanceRef{group: {ref2, ref1}}
	if err := assignCheckpointTimestamps(groups, reversed, state); err != nil {
		t.Fatalf("assignCheckpointTimestamps could not skip a used candidate and select dependency-ready write: %v", err)
	}

	missingDepState := checkpointState{
		records: map[epaxos.InstanceRef]epaxos.InstanceRecord{
			ref2: {Ref: ref2, Deps: []epaxos.InstanceNum{0, 1, 0}},
		},
		writeGroups: map[epaxos.InstanceRef]string{
			ref1: group,
			ref2: group,
		},
	}
	if err := assignCheckpointTimestamps([]checkpointDataGroup{{timestamp: 1, key: group}}, map[string][]epaxos.InstanceRef{group: {ref2}}, missingDepState); err == nil || !strings.Contains(err.Error(), "no dependency-satisfied") {
		t.Fatalf("assignCheckpointTimestamps missing dependency err=%v", err)
	}

	if err := assignCheckpointTimestamps([]checkpointDataGroup{{timestamp: 1, key: group}}, map[string][]epaxos.InstanceRef{group: {ref1}}, state); err == nil || !strings.Contains(err.Error(), "applied command count") {
		t.Fatalf("assignCheckpointTimestamps missing data row err=%v", err)
	}

	depRef := epaxos.InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	depRecord := epaxos.InstanceRecord{Ref: ref2, Deps: []epaxos.InstanceNum{0, 2, 1}}
	assigned := map[epaxos.InstanceRef]uint64{ref1: 1}
	writeGroups := map[epaxos.InstanceRef]string{
		ref1:   group,
		depRef: group,
	}
	if checkpointDependenciesAssigned(depRecord, assigned, writeGroups) {
		t.Fatal("checkpointDependenciesAssigned accepted an unassigned prefix dependency")
	}
	assigned[depRef] = 2
	if !checkpointDependenciesAssigned(depRecord, assigned, writeGroups) {
		t.Fatal("checkpointDependenciesAssigned rejected assigned prefix dependencies")
	}

	ordering := []struct {
		name        string
		left, right epaxos.InstanceRef
		want        bool
	}{
		{name: "lower conf", left: epaxos.InstanceRef{Replica: 9, Instance: 9, Conf: 1}, right: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 2}, want: true},
		{name: "higher conf", left: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 2}, right: epaxos.InstanceRef{Replica: 9, Instance: 9, Conf: 1}},
		{name: "lower replica", left: epaxos.InstanceRef{Replica: 1, Instance: 9, Conf: 1}, right: epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}, want: true},
		{name: "higher replica", left: epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}, right: epaxos.InstanceRef{Replica: 1, Instance: 9, Conf: 1}},
		{name: "lower instance", left: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, right: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}, want: true},
	}
	for _, tt := range ordering {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkpointRefLess(tt.left, tt.right); got != tt.want {
				t.Fatalf("checkpointRefLess(%s, %s)=%v, want %v", tt.left, tt.right, got, tt.want)
			}
		})
	}
}

func TestClusterLiveCheckpointRecoveryRejectsInvalidOperatorsAndUnhealthySources(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()

	if err := cluster.StopReplica(99); err == nil || !strings.Contains(err.Error(), "unknown replica") {
		t.Fatalf("StopReplica unknown err=%v, want unknown replica", err)
	}
	if err := cluster.StopReplica(3); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StopReplica(3); err != nil {
		t.Fatalf("StopReplica already stopped err=%v, want nil", err)
	}
	if err := cluster.Put([]byte("after-stop-one"), []byte("live-value")); err != nil {
		t.Fatalf("Put through remaining live node failed: %v", err)
	}
	assertReplicaValue(t, cluster, 1, []byte("after-stop-one"), "live-value")
	assertReplicaValue(t, cluster, 2, []byte("after-stop-one"), "live-value")

	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := cluster.RecoverReplicaFromLiveCheckpoint(99, 1, paths[0], checkpoint); err == nil || !strings.Contains(err.Error(), "unknown replica") {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint unknown target err=%v", err)
	}
	if err := cluster.RecoverReplicaFromLiveCheckpoint(2, 99, paths[1], checkpoint); err == nil || !strings.Contains(err.Error(), "unknown checkpoint source") {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint unknown source err=%v", err)
	}
	if err := cluster.RecoverReplicaFromLiveCheckpoint(1, 1, paths[0], checkpoint); err == nil || !strings.Contains(err.Error(), "source must differ") {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint same source err=%v", err)
	}
	if err := cluster.RecoverReplicaFromLiveCheckpoint(2, 3, paths[1], checkpoint); err == nil || !strings.Contains(err.Error(), "not live") {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint stopped source err=%v", err)
	}
	if err := cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], checkpoint); err == nil || !strings.Contains(err.Error(), "healthy quorum") {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint unhealthy quorum err=%v", err)
	}
}

func TestClusterLiveCheckpointRecoveryRejectsVerificationAndPeerScanFailures(t *testing.T) {
	t.Run("checkpoint verification failure leaves stopped target closed", func(t *testing.T) {
		paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
		cluster, err := OpenCluster(paths)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cluster.Close() }()
		if err := cluster.Put([]byte("verify-failure-seed"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := cluster.StopReplica(2); err != nil {
			t.Fatal(err)
		}
		setRawOpenPebbleKV(t, cluster.DBs[1].pebble, []byte{recordPrefix, 0}, []byte{valueRecord, 'x'})

		err = cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], filepath.Join(t.TempDir(), "checkpoint"))
		if err == nil || !strings.Contains(err.Error(), "live checkpoint verification failed") {
			t.Fatalf("RecoverReplicaFromLiveCheckpoint err=%v, want checkpoint verification failure", err)
		}
		if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
			t.Fatalf("rejected recovery reopened target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
		}
	})

	t.Run("live peer scan failure leaves stopped target closed", func(t *testing.T) {
		paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
		cluster, err := OpenCluster(paths)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cluster.Close() }()
		if err := cluster.Put([]byte("scan-failure-seed"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := cluster.StopReplica(2); err != nil {
			t.Fatal(err)
		}
		corruptOpenDBEPaxosRecordValue(t, cluster.DBs[3], epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, []byte("value"))

		err = cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], filepath.Join(t.TempDir(), "checkpoint"))
		if err == nil || !strings.Contains(err.Error(), "support scan failed") {
			t.Fatalf("RecoverReplicaFromLiveCheckpoint err=%v, want live support scan failure", err)
		}
		if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
			t.Fatalf("rejected recovery reopened target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
		}
	})
}

func TestVerifyCheckpointTargetOwnerFloorRejectsMissingAndCommittedMismatch(t *testing.T) {
	target := epaxos.ReplicaID(2)
	ref1 := epaxos.InstanceRef{Replica: target, Instance: 1, Conf: 1}
	ref2 := epaxos.InstanceRef{Replica: target, Instance: 2, Conf: 1}
	committed := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     ref1,
		Ballot:  epaxos.Ballot{Replica: target},
		Status:  epaxos.StatusCommitted,
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0, 0, 0},
		Command: CommandForPut(120, 1, []byte("target-owned"), []byte("checkpoint")),
	})
	liveCommitted := committed.Clone()
	liveCommitted.Command = CommandForPut(120, 1, []byte("target-owned"), []byte("live"))
	liveCommitted.Checksum = epaxos.ChecksumRecord(liveCommitted)

	liveRecords := map[epaxos.ReplicaID]map[epaxos.InstanceRef]epaxos.InstanceRecord{
		1: {
			ref1: committed,
			ref2: checkedKVRecord(epaxos.InstanceRecord{
				Ref:     ref2,
				Ballot:  epaxos.Ballot{Replica: target},
				Status:  epaxos.StatusPreAccepted,
				Seq:     2,
				Deps:    []epaxos.InstanceNum{0, 0, 0},
				Command: CommandForPut(120, 2, []byte("target-owned-two"), []byte("value")),
			}),
		},
	}
	if err := verifyCheckpointTargetOwnerFloor(map[epaxos.InstanceRef]epaxos.InstanceRecord{ref1: committed}, liveRecords, target); err == nil || !strings.Contains(err.Error(), "missing target-owned prefix") {
		t.Fatalf("verifyCheckpointTargetOwnerFloor missing prefix err=%v", err)
	}
	if err := verifyCheckpointTargetOwnerFloor(map[epaxos.InstanceRef]epaxos.InstanceRecord{ref1: committed, ref2: liveRecords[1][ref2]}, map[epaxos.ReplicaID]map[epaxos.InstanceRef]epaxos.InstanceRecord{1: {ref1: liveCommitted}}, target); err == nil || !strings.Contains(err.Error(), "committed target-owned record") {
		t.Fatalf("verifyCheckpointTargetOwnerFloor committed mismatch err=%v", err)
	}
}

func TestConsensusRecordEqualityHelpersRejectMismatches(t *testing.T) {
	if acceptEvidenceEqualKV([]epaxos.AcceptEvidence{{Sender: 1}}, nil) {
		t.Fatal("acceptEvidenceEqualKV accepted evidence length mismatch")
	}
	if acceptEvidenceEqualKV([]epaxos.AcceptEvidence{{Sender: 1, Seq: 1, Deps: []epaxos.InstanceNum{1}}}, []epaxos.AcceptEvidence{{Sender: 2, Seq: 1, Deps: []epaxos.InstanceNum{1}}}) {
		t.Fatal("acceptEvidenceEqualKV accepted evidence sender mismatch")
	}
	if acceptEvidenceEqualKV([]epaxos.AcceptEvidence{{Sender: 1, Seq: 1, Deps: []epaxos.InstanceNum{1}}}, []epaxos.AcceptEvidence{{Sender: 1, Seq: 1, Deps: []epaxos.InstanceNum{2}}}) {
		t.Fatal("acceptEvidenceEqualKV accepted evidence dependency mismatch")
	}
	if instanceNumsEqualKV([]epaxos.InstanceNum{1}, []epaxos.InstanceNum{1, 2}) {
		t.Fatal("instanceNumsEqualKV accepted length mismatch")
	}
	if instanceNumsEqualKV([]epaxos.InstanceNum{1, 2}, []epaxos.InstanceNum{1, 3}) {
		t.Fatal("instanceNumsEqualKV accepted value mismatch")
	}
	if sameEPaxosCommand(CommandForPut(130, 1, []byte("cmd"), []byte("left")), CommandForPut(130, 1, []byte("cmd"), []byte("right"))) {
		t.Fatal("sameEPaxosCommand accepted payload mismatch")
	}
	left := CommandForTxn(130, 2, []TxnOp{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: []byte("2")}})
	right := CommandForTxn(130, 2, []TxnOp{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("c"), Value: []byte("2")}})
	right.Payload = append([]byte(nil), left.Payload...)
	if sameEPaxosCommand(left, right) {
		t.Fatal("sameEPaxosCommand accepted conflict key mismatch")
	}
}

func TestPebbleStorageLoadInstancesRejectsMalformedKeysAndRefMismatch(t *testing.T) {
	t.Run("malformed durable key", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		setRawOpenPebbleKV(t, db.pebble, []byte{epaxosStorePrefix, epaxosRecordEntry, 0}, []byte{epaxosRecordCodec})

		err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error {
			t.Fatal("callback must not run for malformed durable key")
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "malformed epaxos record key") {
			t.Fatalf("LoadInstances err=%v, want malformed key", err)
		}
	})

	t.Run("durable key ref must match value ref", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		record := checkpointPutRecord(epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}, "load-mismatch", "value")
		setRawOpenPebbleKV(t, db.pebble, epaxosRecordKey(epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}), encodeEPaxosRecord(record))

		err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error {
			t.Fatal("callback must not run for mismatched durable record")
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "key/value ref mismatch") {
			t.Fatalf("LoadInstances err=%v, want key/value mismatch", err)
		}
	})
}

func TestVerifyCheckpointCoversOrderingAndIteratorErrorBranches(t *testing.T) {
	t.Run("same-effect candidates are sorted before timestamp assignment", func(t *testing.T) {
		checkpoint := t.TempDir()
		ref2 := epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1}
		ref1 := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record2 := checkpointPutRecord(ref2, "same-effect", "value")
		record1 := checkpointPutRecord(ref1, "same-effect", "value")
		db, err := Open(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		err = db.ApplyReady(epaxos.Ready{
			Records: []epaxos.InstanceRecord{record2, record1},
			Committed: []epaxos.CommittedCommand{
				{Ref: ref2, Seq: record2.Seq, Deps: record2.Deps, Command: record2.Command},
				{Ref: ref1, Seq: record1.Seq, Deps: record1.Deps, Command: record1.Command},
			},
		})
		if closeErr := db.Close(); err != nil {
			t.Fatal(err)
		} else if closeErr != nil {
			t.Fatal(closeErr)
		}

		if err := VerifyCheckpoint(checkpoint); err != nil {
			t.Fatalf("VerifyCheckpoint rejected sorted equivalent writes: %v", err)
		}
	})

	t.Run("dependency order is checked through public verification", func(t *testing.T) {
		checkpoint := t.TempDir()
		depRef := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		dependentRef := epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}
		dep := checkpointPutRecord(depRef, "dependency-earlier", "value")
		dependent := checkpointPutRecord(dependentRef, "dependency-later", "value")
		dependent.Deps = []epaxos.InstanceNum{1}
		dependent.Checksum = epaxos.ChecksumRecord(dependent)
		db, err := Open(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		err = db.ApplyReady(epaxos.Ready{
			Records: []epaxos.InstanceRecord{dep, dependent},
			Committed: []epaxos.CommittedCommand{
				{Ref: dependentRef, Seq: dependent.Seq, Deps: dependent.Deps, Command: dependent.Command},
				{Ref: depRef, Seq: dep.Seq, Deps: dep.Deps, Command: dep.Command},
			},
		})
		if closeErr := db.Close(); err != nil {
			t.Fatal(err)
		} else if closeErr != nil {
			t.Fatal(closeErr)
		}

		if err := VerifyCheckpoint(checkpoint); err == nil || !strings.Contains(err.Error(), "no dependency-satisfied") {
			t.Fatalf("VerifyCheckpoint dependency-order err=%v, want dependency rejection", err)
		}
	})

}

func TestCheckpointDirectHelpersCoverDuplicateAndLegacyBranches(t *testing.T) {
	t.Run("record loader rejects duplicate decoded refs", func(t *testing.T) {
		checkpoint := t.TempDir()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record := checkpointPutRecord(ref, "duplicate-record", "value")
		writeCheckpointRecordOnly(t, checkpoint, record)
		db, err := Open(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		err = loadCheckpointRecords(db.pebble, map[epaxos.InstanceRef]epaxos.InstanceRecord{ref: record})
		if err == nil || !strings.Contains(err.Error(), "duplicate epaxos record") {
			t.Fatalf("loadCheckpointRecords err=%v, want duplicate record rejection", err)
		}
	})

	t.Run("applied marker loader rejects duplicate decoded refs", func(t *testing.T) {
		checkpoint := t.TempDir()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record := checkpointPutRecord(ref, "duplicate-marker", "value")
		writeAppliedCheckpointRecord(t, checkpoint, record)
		db, err := Open(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		state := checkpointState{
			records: map[epaxos.InstanceRef]epaxos.InstanceRecord{ref: record},
			applied: map[epaxos.InstanceRef]struct{}{ref: {}},
		}
		err = loadCheckpointAppliedMarkers(db.pebble, state, map[string]int{}, map[string][]epaxos.InstanceRef{})
		if err == nil || !strings.Contains(err.Error(), "duplicate epaxos applied marker") {
			t.Fatalf("loadCheckpointAppliedMarkers err=%v, want duplicate marker rejection", err)
		}
	})

	if atom, err := checkpointValueAtom([]byte("delete-key"), []byte{deleteRecord}); err != nil || atom != checkpointAtomKey(deleteRecord, []byte("delete-key"), nil) {
		t.Fatalf("checkpointValueAtom delete atom=%q err=%v", atom, err)
	}
	if _, _, err := checkpointCommandGroup(epaxos.Command{Payload: []byte{99}}); err == nil || !strings.Contains(err.Error(), "unknown op 99") {
		t.Fatalf("checkpointCommandGroup unknown op err=%v", err)
	}
	if !checkpointDependenciesAssigned(epaxos.InstanceRecord{Ref: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Deps: []epaxos.InstanceNum{1}}, nil, nil) {
		t.Fatal("checkpointDependenciesAssigned should ignore non-writing dependencies")
	}
}

func TestCheckpointLoadersSurfaceIteratorConstructionErrors(t *testing.T) {
	sentinel := errors.New("checkpoint iterator construction failed")
	original := checkpointNewIter
	checkpointNewIter = func(*pebble.DB, *pebble.IterOptions) (checkpointIterator, error) {
		return nil, sentinel
	}
	t.Cleanup(func() { checkpointNewIter = original })

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "records",
			call: func() error {
				return loadCheckpointRecords(nil, map[epaxos.InstanceRef]epaxos.InstanceRecord{})
			},
		},
		{
			name: "applied markers",
			call: func() error {
				state := checkpointState{
					records: map[epaxos.InstanceRef]epaxos.InstanceRecord{},
					applied: map[epaxos.InstanceRef]struct{}{},
				}
				return loadCheckpointAppliedMarkers(nil, state, map[string]int{}, map[string][]epaxos.InstanceRef{})
			},
		},
		{
			name: "data groups",
			call: func() error {
				_, _, err := loadCheckpointDataGroups(nil)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, sentinel) {
				t.Fatalf("%s loader err=%v, want %v", tt.name, err, sentinel)
			}
		})
	}
}

func TestCheckpointDataGroupsSurfaceTerminalIteratorError(t *testing.T) {
	sentinel := errors.New("checkpoint data scan failed")
	iter := &terminalErrorIterator{err: sentinel}
	original := checkpointNewIter
	checkpointNewIter = func(*pebble.DB, *pebble.IterOptions) (checkpointIterator, error) {
		return iter, nil
	}
	t.Cleanup(func() { checkpointNewIter = original })

	_, _, err := loadCheckpointDataGroups(nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("loadCheckpointDataGroups err=%v, want %v", err, sentinel)
	}
	if !iter.closed {
		t.Fatal("checkpoint iterator was not closed after terminal error")
	}
}

func TestClusterRecoveryAndProposalResidualBranches(t *testing.T) {
	t.Run("checkpoint creation error is surfaced before mutating target", func(t *testing.T) {
		paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
		cluster, err := OpenCluster(paths)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cluster.Close() }()
		if err := cluster.StopReplica(2); err != nil {
			t.Fatal(err)
		}
		checkpointFile := filepath.Join(t.TempDir(), "checkpoint-file")
		if err := os.WriteFile(checkpointFile, []byte("not a directory"), 0o600); err != nil {
			t.Fatal(err)
		}

		err = cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], checkpointFile)
		if err == nil {
			t.Fatal("RecoverReplicaFromLiveCheckpoint accepted checkpoint path that is a file")
		}
		if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
			t.Fatalf("failed checkpoint creation reopened target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
		}
	})

	t.Run("repair failure after live quorum verification leaves target stopped", func(t *testing.T) {
		paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
		cluster, err := OpenCluster(paths)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cluster.Close() }()
		if err := cluster.Put([]byte("repair-failure-seed"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := cluster.StopReplica(2); err != nil {
			t.Fatal(err)
		}

		err = cluster.RecoverReplicaFromLiveCheckpoint(2, 1, ".", filepath.Join(t.TempDir(), "checkpoint"))
		if err == nil || !strings.Contains(err.Error(), "broad data directory") {
			t.Fatalf("RecoverReplicaFromLiveCheckpoint repair err=%v, want broad data dir rejection", err)
		}
		if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
			t.Fatalf("failed repair reopened target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
		}
	})

	t.Run("invalid recovered voter set closes reopened database", func(t *testing.T) {
		paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
		cluster, err := OpenCluster(paths)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cluster.Close() }()
		if err := cluster.StopReplica(2); err != nil {
			t.Fatal(err)
		}
		cluster.ids = []epaxos.ReplicaID{1, 2, 3, 3, 3}

		err = cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], filepath.Join(t.TempDir(), "checkpoint"))
		if !errors.Is(err, epaxos.ErrInvalidConfig) {
			t.Fatalf("RecoverReplicaFromLiveCheckpoint err=%v, want invalid config", err)
		}
		if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
			t.Fatalf("failed raw node creation registered target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
		}
		db, openErr := Open(paths[1])
		if openErr != nil {
			t.Fatalf("failed raw node creation left recovered database locked: %v", openErr)
		}
		if closeErr := db.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	})

	t.Run("proposal fails when every replica is stopped", func(t *testing.T) {
		cluster, err := OpenCluster([]string{t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cluster.Close() }()
		if err := cluster.StopReplica(1); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Put([]byte("no-live"), []byte("value")); err == nil || !strings.Contains(err.Error(), "no live replica") {
			t.Fatalf("Put with no live replicas err=%v, want no live replica", err)
		}
	})
}

func TestRecoverReplicaFromLiveCheckpointSurfacesStopErrorWithoutReopeningTarget(t *testing.T) {
	cluster, paths := newRecoverableLiveCheckpointScenario(t, "stop-seam-key", "stop-seam-value")
	sentinel := errors.New("stop replica failed")
	original := clusterStopReplica
	clusterStopReplica = func(*Cluster, epaxos.ReplicaID) error {
		return sentinel
	}
	t.Cleanup(func() { clusterStopReplica = original })

	err := cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], filepath.Join(t.TempDir(), "checkpoint"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint err=%v, want %v", err, sentinel)
	}
	if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
		t.Fatalf("stop failure reopened target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
	}
	assertEPaxosChecksumMismatch(t, paths[1])
}

func TestRecoverReplicaFromLiveCheckpointSurfacesOpenErrorAndLeavesTargetStopped(t *testing.T) {
	cluster, paths := newRecoverableLiveCheckpointScenario(t, "open-seam-key", "open-seam-value")
	sentinel := errors.New("open repaired database failed")
	original := clusterOpenDB
	clusterOpenDB = func(string) (*DB, error) {
		return nil, sentinel
	}
	t.Cleanup(func() { clusterOpenDB = original })

	err := cluster.RecoverReplicaFromLiveCheckpoint(2, 1, paths[1], filepath.Join(t.TempDir(), "checkpoint"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("RecoverReplicaFromLiveCheckpoint err=%v, want %v", err, sentinel)
	}
	if cluster.DBs[2] != nil || cluster.Nodes[2] != nil {
		t.Fatalf("open failure registered target: db=%v node=%v", cluster.DBs[2], cluster.Nodes[2])
	}
	db, openErr := Open(paths[1])
	if openErr != nil {
		t.Fatalf("open failure left repaired database unusable: %v", openErr)
	}
	value, ok, getErr := db.Get([]byte("open-seam-key"))
	closeErr := db.Close()
	if getErr != nil || !ok || string(value) != "open-seam-value" {
		t.Fatalf("repaired value=%q ok=%v err=%v, want open-seam-value", value, ok, getErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

func TestVerifyCheckpointAgainstLiveQuorumSurfacesCheckpointCloseErrorAfterSemanticVerification(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	if err := cluster.Put([]byte("close-seam-key"), []byte("close-seam-value")); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := cluster.DBs[1].Checkpoint(checkpoint); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("checkpoint close failed")
	original := clusterCloseCheckpointDB
	clusterCloseCheckpointDB = func(db *DB) error {
		if err := db.Close(); err != nil {
			return err
		}
		return sentinel
	}
	t.Cleanup(func() { clusterCloseCheckpointDB = original })

	err = cluster.verifyCheckpointAgainstLiveQuorum(checkpoint, 2)
	if !errors.Is(err, sentinel) {
		t.Fatalf("verifyCheckpointAgainstLiveQuorum err=%v, want %v", err, sentinel)
	}
}

func newRecoverableLiveCheckpointScenario(t *testing.T, key, value string) (*Cluster, []string) {
	t.Helper()
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cluster.Close() })
	if err := cluster.Put([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StopReplica(2); err != nil {
		t.Fatal(err)
	}
	corruptPersistedEPaxosRecordValue(t, paths[1], epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, []byte(value))
	return cluster, paths
}

func TestVerifyCheckpointAgainstLiveQuorumResidualBranches(t *testing.T) {
	t.Run("missing checkpoint cannot be scanned", func(t *testing.T) {
		cluster := &Cluster{ids: []epaxos.ReplicaID{1}, DBs: map[epaxos.ReplicaID]*DB{}}
		err := cluster.verifyCheckpointAgainstLiveQuorum(filepath.Join(t.TempDir(), "missing"), 1)
		if err == nil {
			t.Fatal("verifyCheckpointAgainstLiveQuorum accepted missing checkpoint")
		}
	})

	t.Run("semantic checkpoint failure is returned before live scan", func(t *testing.T) {
		checkpoint := t.TempDir()
		setRawPebbleKV(t, checkpoint, EncodeDataKey(nil, 1, []byte("bad-checkpoint"), 2), []byte{valueRecord, 'x'})
		cluster := &Cluster{ids: []epaxos.ReplicaID{1}, DBs: map[epaxos.ReplicaID]*DB{}}
		if err := cluster.verifyCheckpointAgainstLiveQuorum(checkpoint, 1); err == nil || !strings.Contains(err.Error(), "not dense position") {
			t.Fatalf("verifyCheckpointAgainstLiveQuorum err=%v, want checkpoint verification failure", err)
		}
	})

	t.Run("nil live peer is skipped and uncommitted checkpoint records need no quorum", func(t *testing.T) {
		checkpoint := t.TempDir()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusAccepted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForPut(140, 1, []byte("uncommitted-quorum"), []byte("value")),
		})
		writeCheckpointRecordOnly(t, checkpoint, record)
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		cluster := &Cluster{
			ids: []epaxos.ReplicaID{1, 2, 3},
			DBs: map[epaxos.ReplicaID]*DB{1: db, 3: nil},
		}
		if err := cluster.verifyCheckpointAgainstLiveQuorum(checkpoint, 2); err != nil {
			t.Fatalf("verifyCheckpointAgainstLiveQuorum rejected uncommitted checkpoint record: %v", err)
		}
	})
}

func TestDecodeEPaxosRecordRejectsZeroEvidenceSenderAndUpgradesLegacyChecksum(t *testing.T) {
	badSender := checkedKVRecord(epaxos.InstanceRecord{
		Ref:            epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:         epaxos.Ballot{Replica: 1},
		Status:         epaxos.StatusAccepted,
		Seq:            1,
		Deps:           []epaxos.InstanceNum{0},
		AcceptSeq:      2,
		AcceptDeps:     []epaxos.InstanceNum{0},
		AcceptEvidence: []epaxos.AcceptEvidence{{Sender: 0, Seq: 2, Deps: []epaxos.InstanceNum{0}}},
		Command:        CommandForPut(150, 1, []byte("bad-sender"), []byte("value")),
	})
	if _, err := decodeEPaxosRecord(encodeEPaxosRecord(badSender)); err == nil || !strings.Contains(err.Error(), "bad epaxos accept evidence sender") {
		t.Fatalf("decodeEPaxosRecord bad sender err=%v", err)
	}

	legacy := epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1},
		Ballot:           epaxos.Ballot{Replica: 1},
		RecordBallot:     epaxos.Ballot{Replica: 1},
		Status:           epaxos.StatusAccepted,
		Seq:              2,
		Deps:             []epaxos.InstanceNum{0},
		AcceptSeq:        3,
		AcceptDeps:       []epaxos.InstanceNum{0},
		Command:          CommandForPut(150, 2, []byte("legacy-sender"), []byte("value")),
		FastPathEligible: true,
	}
	legacy.Checksum = checksumRecordWithoutSenderEvidenceKV(legacy)
	decoded, err := decodeEPaxosRecord(encodeEPaxosRecordV5KV(legacy))
	if err != nil {
		t.Fatalf("decodeEPaxosRecord legacy sender checksum err=%v", err)
	}
	if decoded.Ref != legacy.Ref || !epaxos.VerifyRecordChecksum(decoded) {
		t.Fatalf("decoded legacy record=%#v, want canonical checksum for %s", decoded, legacy.Ref)
	}
}

func encodeEPaxosRecordV5KV(rec epaxos.InstanceRecord) []byte {
	out := []byte{5}
	out = binary.AppendUvarint(out, uint64(rec.Ref.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Instance))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Conf))
	out = binary.AppendUvarint(out, rec.Ballot.Epoch)
	out = binary.AppendUvarint(out, rec.Ballot.Number)
	out = binary.AppendUvarint(out, uint64(rec.Ballot.Replica))
	out = binary.AppendUvarint(out, rec.RecordBallot.Epoch)
	out = binary.AppendUvarint(out, rec.RecordBallot.Number)
	out = binary.AppendUvarint(out, uint64(rec.RecordBallot.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Status))
	out = binary.AppendUvarint(out, rec.Seq)
	out = binary.AppendUvarint(out, uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, rec.AcceptSeq)
	out = binary.AppendUvarint(out, uint64(len(rec.AcceptDeps)))
	for _, dep := range rec.AcceptDeps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, rec.Command.ID.Client)
	out = binary.AppendUvarint(out, rec.Command.ID.Sequence)
	out = binary.AppendUvarint(out, uint64(rec.Command.Kind))
	out = binary.AppendUvarint(out, uint64(len(rec.Command.Payload)))
	out = append(out, rec.Command.Payload...)
	out = binary.AppendUvarint(out, uint64(len(rec.Command.ConflictKeys)))
	for _, key := range rec.Command.ConflictKeys {
		out = binary.AppendUvarint(out, uint64(len(key)))
		out = append(out, key...)
	}
	out = binary.AppendUvarint(out, rec.ProcessAt)
	if rec.TOQPending {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	if rec.FastPathEligible {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	return append(out, rec.Checksum[:]...)
}

func checksumRecordWithoutSenderEvidenceKV(rec epaxos.InstanceRecord) [32]byte {
	h := blake3.New()
	writeUint64KV := func(v uint64) {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		_, _ = h.Write(b[:])
	}
	writeByteKV := func(v byte) {
		_, _ = h.Write([]byte{v})
	}
	writeBytesKV := func(value []byte) {
		writeUint64KV(uint64(len(value)))
		_, _ = h.Write(value)
	}
	writeUint64KV(uint64(rec.Ref.Replica))
	writeUint64KV(uint64(rec.Ref.Instance))
	writeUint64KV(uint64(rec.Ref.Conf))
	writeUint64KV(rec.Ballot.Epoch)
	writeUint64KV(rec.Ballot.Number)
	writeUint64KV(uint64(rec.Ballot.Replica))
	writeUint64KV(rec.RecordBallot.Epoch)
	writeUint64KV(rec.RecordBallot.Number)
	writeUint64KV(uint64(rec.RecordBallot.Replica))
	writeByteKV(byte(rec.Status))
	writeUint64KV(rec.Seq)
	writeUint64KV(uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		writeUint64KV(uint64(dep))
	}
	writeUint64KV(rec.AcceptSeq)
	writeUint64KV(uint64(len(rec.AcceptDeps)))
	for _, dep := range rec.AcceptDeps {
		writeUint64KV(uint64(dep))
	}
	writeUint64KV(rec.Command.ID.Client)
	writeUint64KV(rec.Command.ID.Sequence)
	writeByteKV(byte(rec.Command.Kind))
	writeBytesKV(rec.Command.Payload)
	writeUint64KV(uint64(len(rec.Command.ConflictKeys)))
	for _, key := range rec.Command.ConflictKeys {
		writeBytesKV(key)
	}
	if rec.FastPathEligible {
		writeByteKV(1)
	} else {
		writeByteKV(0)
	}
	writeUint64KV(rec.ProcessAt)
	if rec.TOQPending {
		writeByteKV(1)
	} else {
		writeByteKV(0)
	}
	var out [32]byte
	sum := h.Sum(out[:0])
	copy(out[:], sum)
	return out
}

func checkpointPutRecord(ref epaxos.InstanceRef, key, value string) epaxos.InstanceRecord {
	return checkedKVRecord(epaxos.InstanceRecord{
		Ref:     ref,
		Ballot:  epaxos.Ballot{Replica: ref.Replica},
		Status:  epaxos.StatusCommitted,
		Seq:     uint64(ref.Instance),
		Deps:    make([]epaxos.InstanceNum, max(int(ref.Replica), 1)),
		Command: CommandForPut(99, uint64(ref.Instance), []byte(key), []byte(value)),
	})
}

func setRawPebbleKV(t *testing.T, path string, key, value []byte) {
	t.Helper()
	pebbleDB, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	setRawOpenPebbleKV(t, pebbleDB, key, value)
	if err := pebbleDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func setRawOpenPebbleKV(t *testing.T, pebbleDB *pebble.DB, key, value []byte) {
	t.Helper()
	if err := pebbleDB.Set(key, value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func corruptOpenDBEPaxosRecordValue(t *testing.T, db *DB, ref epaxos.InstanceRef, needle []byte) {
	t.Helper()
	value, closer, err := db.pebble.Get(epaxosRecordKey(ref))
	if err != nil {
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	payloadAt := bytes.Index(corrupted, needle)
	if payloadAt < 0 {
		t.Fatalf("persisted EPaxos record did not contain payload bytes %q: %x", needle, corrupted)
	}
	corrupted[payloadAt] ^= 0x01
	setRawOpenPebbleKV(t, db.pebble, epaxosRecordKey(ref), corrupted)
}

func assertReplicaValue(t *testing.T, cluster *Cluster, id epaxos.ReplicaID, key []byte, want string) {
	t.Helper()
	value, ok, err := cluster.Get(id, key)
	if err != nil || !ok || string(value) != want {
		t.Fatalf("replica %d key %q value=%q ok=%v err=%v, want %q", id, key, value, ok, err, want)
	}
}

func assertEPaxosChecksumMismatch(t *testing.T, path string) {
	t.Helper()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error { return nil })
	if !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("LoadInstances on %s err=%v, want %v", path, err, epaxos.ErrChecksumMismatch)
	}
}

func writeCheckpointRecordOnly(t *testing.T, path string, record epaxos.InstanceRecord) {
	t.Helper()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{record}}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeAppliedCheckpointRecord(t *testing.T, path string, record epaxos.InstanceRecord) {
	t.Helper()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyReady(epaxos.Ready{
		Records: []epaxos.InstanceRecord{record},
		Committed: []epaxos.CommittedCommand{{
			Ref:     record.Ref,
			Seq:     record.Seq,
			Deps:    append([]epaxos.InstanceNum(nil), record.Deps...),
			Command: record.Command,
		}},
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func overwriteCheckpointDataValue(t *testing.T, path string, key []byte, ts uint64, value []byte) {
	t.Helper()
	pebbleDB, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	encoded := append([]byte{valueRecord}, value...)
	if err := pebbleDB.Set(EncodeDataKey(nil, 1, key, ts), encoded, pebble.Sync); err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	if err := pebbleDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func deleteCheckpointAppliedMarker(t *testing.T, path string, ref epaxos.InstanceRef) {
	t.Helper()
	pebbleDB, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pebbleDB.Delete(epaxosAppliedKey(ref), pebble.Sync); err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	if err := pebbleDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func corruptPersistedEPaxosRecordValue(t *testing.T, path string, ref epaxos.InstanceRef, needle []byte) {
	t.Helper()
	pebbleDB, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value, closer, err := pebbleDB.Get(epaxosRecordKey(ref))
	if err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	payloadAt := bytes.Index(corrupted, needle)
	if payloadAt < 0 {
		_ = pebbleDB.Close()
		t.Fatalf("persisted EPaxos record did not contain payload bytes %q: %x", needle, corrupted)
	}
	corrupted[payloadAt] ^= 0x01
	if err := pebbleDB.Set(epaxosRecordKey(ref), corrupted, pebble.Sync); err != nil {
		_ = pebbleDB.Close()
		t.Fatal(err)
	}
	if err := pebbleDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClusterDeletePropagatesDurableStorageFailure(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	sentinel := errors.New("ready apply failed")
	cluster.readyAppliers[1] = func(epaxos.Ready) error { return sentinel }
	if err := cluster.Delete([]byte("x")); !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
}

func TestClusterPutReturnsAdvanceValidationErrorAfterDurableApply(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()

	apply := cluster.readyAppliers[1]
	cluster.readyAppliers[1] = func(rd epaxos.Ready) error {
		if err := apply(rd); err != nil {
			return err
		}
		if len(rd.Records) == 0 {
			t.Fatal("ready had no records to corrupt")
		}
		rd.Records[0].Seq++
		return nil
	}

	err = cluster.Put([]byte("advance-key"), []byte("advance-value"))
	if !errors.Is(err, epaxos.ErrInvalidReady) {
		t.Fatalf("put err=%v, want %v", err, epaxos.ErrInvalidReady)
	}
	value, ok, err := cluster.Get(1, []byte("advance-key"))
	if err != nil || !ok || string(value) != "advance-value" {
		t.Fatalf("value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestOpenLoadIteratorFactoryErrorClosesPebbleDB(t *testing.T) {
	path := t.TempDir()
	sentinel := errors.New("load iterator")

	_, err := open(path, func(*pebble.DB) func(*pebble.IterOptions) (kvIterator, error) {
		return func(*pebble.IterOptions) (kvIterator, error) {
			return nil, sentinel
		}
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("database remained locked after load iterator failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenLoadIteratorTerminalErrorClosesPebbleDB(t *testing.T) {
	path := t.TempDir()
	sentinel := errors.New("load scan")
	iter := &terminalErrorIterator{err: sentinel}

	_, err := open(path, func(*pebble.DB) func(*pebble.IterOptions) (kvIterator, error) {
		return func(*pebble.IterOptions) (kvIterator, error) {
			return iter, nil
		}
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if !iter.closed {
		t.Fatal("load iterator was not closed")
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("database remained locked after load scan failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNextRecordTimeStartsZeroValuedDBAtOne(t *testing.T) {
	db := &DB{}
	if got := db.nextRecordTime(); got != 1 {
		t.Fatalf("first timestamp=%d, want 1", got)
	}
	if got := db.nextRecordTime(); got != 1 {
		t.Fatalf("pending timestamp advanced to %d before a successful write", got)
	}
}

func TestVersionWriteErrorsDoNotAdvanceNextAutomaticTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		write func(*DB) error
	}{
		{
			name: "put",
			write: func(db *DB) error {
				return db.PutVersion([]byte("closed-put"), []byte("value"), 99)
			},
		},
		{
			name: "delete",
			write: func(db *DB) error {
				return db.DeleteVersion([]byte("closed-delete"), 99)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			pebbleDB, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pebbleDB.Close() }()
			db = &DB{pebble: pebbleDB, cf: 1, nextTime: 7}

			if err := tt.write(db); !errors.Is(err, pebble.ErrReadOnly) {
				t.Fatalf("err=%v, want read-only write failure", err)
			}
			if got := db.nextRecordTime(); got != 7 {
				t.Fatalf("next automatic timestamp advanced to %d after failed explicit write", got)
			}
		})
	}
}

func TestIteratorFactoryErrorsPropagate(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	sentinel := errors.New("iterator")
	db.newIter = func(*pebble.IterOptions) (kvIterator, error) {
		return nil, sentinel
	}
	if _, _, err := db.Get([]byte("x")); !errors.Is(err, sentinel) {
		t.Fatalf("get err=%v", err)
	}
	if _, err := db.Scan(ScanOptions{}); !errors.Is(err, sentinel) {
		t.Fatalf("scan err=%v", err)
	}
}

func TestOpenClusterRawNodeFailureClosesOpenedDBs(t *testing.T) {
	first := t.TempDir()
	sentinel := errors.New("raw node")
	_, err := openCluster([]string{first}, func(epaxos.Config) (*epaxos.RawNode, error) {
		return nil, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	db, err := Open(first)
	if err != nil {
		t.Fatalf("first database remained locked after raw node failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClusterProposalAndTransportErrorBranches(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	if err := cluster.proposeAndDrain(epaxos.Command{Kind: epaxos.CommandConfChange}); !errors.Is(err, epaxos.ErrInvalidConfig) {
		t.Fatalf("proposal err=%v", err)
	}
	if err := cluster.deliver(epaxos.Message{To: 99}); err != nil {
		t.Fatalf("unknown target err=%v", err)
	}
	if err := cluster.deliver(epaxos.Message{Type: epaxos.MsgPreAccept, From: 99, To: 1, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}}); err != nil {
		t.Fatalf("rejected sender err=%v", err)
	}
	if err := cluster.deliver(epaxos.Message{From: 1, To: 1, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}}); !errors.Is(err, epaxos.ErrInvalidMessage) {
		t.Fatalf("invalid transport err=%v", err)
	}
	if err := cluster.drainWithLimit(0); err == nil {
		t.Fatal("expected non-quiescence error")
	}
}

func TestDrainReturnsDeliveryFailure(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir(), t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	sentinel := errors.New("delivery")
	cluster.deliverMessage = func(epaxos.Message) error {
		return sentinel
	}
	if _, err := cluster.Nodes[1].Propose(CommandForPut(1, 1, []byte("x"), []byte("y"))); err != nil {
		t.Fatal(err)
	}
	if err := cluster.drainWithLimit(1); !errors.Is(err, sentinel) {
		t.Fatalf("drain err=%v", err)
	}
}

func TestCommandForTxnAppliesPutsThenDeleteAndPut(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	first := epaxos.CommittedCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 10, Conf: 1},
		Command: CommandForTxn(7, 1, []TxnOp{{Key: []byte("alpha"), Value: []byte("one")}, {Key: []byte("beta"), Value: []byte("two")}}),
	}
	if err := db.ApplyCommitted(first); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Start: []byte("a"), End: []byte("z")})
	if err != nil {
		t.Fatal(err)
	}
	wantTime := uint64(1)
	if len(scan) != 2 ||
		string(scan[0].Key) != "alpha" || string(scan[0].Value) != "one" || scan[0].Time != wantTime ||
		string(scan[1].Key) != "beta" || string(scan[1].Value) != "two" || scan[1].Time != wantTime {
		t.Fatalf("first transaction scan %#v", scan)
	}

	second := epaxos.CommittedCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 11, Conf: 1},
		Command: CommandForTxn(7, 2, []TxnOp{{Delete: true, Key: []byte("alpha")}, {Key: []byte("gamma"), Value: []byte("three")}}),
	}
	if err := db.ApplyCommitted(second); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.Get([]byte("alpha")); err != nil || ok {
		t.Fatalf("deleted key ok=%v err=%v", ok, err)
	}
	v, ok, err := db.Get([]byte("beta"))
	if err != nil || !ok || string(v) != "two" {
		t.Fatalf("beta value=%q ok=%v err=%v", v, ok, err)
	}
	v, ok, err = db.Get([]byte("gamma"))
	if err != nil || !ok || string(v) != "three" {
		t.Fatalf("gamma value=%q ok=%v err=%v", v, ok, err)
	}
}

func TestTxnPayloadRejectsMalformedInputsAndKeepsBatchAtomic(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{name: "bad count varint", payload: []byte{opTxn, 0xff}, wantErr: "kv: malformed command"},
		{name: "missing transaction operation", payload: []byte{opTxn, 1}, wantErr: "kv: malformed command"},
		{name: "unknown transaction operation", payload: []byte{opTxn, 1, 99, 0, 0}, wantErr: "kv: unknown transaction op 99"},
		{name: "extra byte after operations", payload: []byte{opTxn, 0, 0}, wantErr: "kv: malformed command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			err = db.ApplyCommitted(epaxos.CommittedCommand{
				Ref:     epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
				Command: epaxos.Command{Payload: tt.payload},
			})
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("err=%v want %q", err, tt.wantErr)
			}
		})
	}

	t.Run("unknown operation leaves earlier write unapplied", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		payload := []byte{opTxn, 2, opPut}
		payload = appendKVFields(payload, []byte("alpha"), []byte("one"))
		payload = append(payload, 99)
		payload = appendKVFields(payload, []byte("beta"), nil)
		err = db.ApplyCommitted(epaxos.CommittedCommand{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1},
			Command: epaxos.Command{Payload: payload},
		})
		if err == nil || !strings.Contains(err.Error(), "unknown transaction op") {
			t.Fatalf("err=%v", err)
		}
		if _, ok, err := db.Get([]byte("alpha")); err != nil || ok {
			t.Fatalf("partial write ok=%v err=%v", ok, err)
		}
	})
}

func TestTxnBatchSetReturnsIndexErrorForPutAndDelete(t *testing.T) {
	tests := []struct {
		name string
		op   TxnOp
	}{
		{name: "put", op: TxnOp{Key: []byte("alpha"), Value: []byte("one")}},
		{name: "delete", op: TxnOp{Delete: true, Key: []byte("alpha")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()

			setErr := errors.New("batch set failed")
			db.newBatch = func() txnBatch {
				return txnBatchWithSetErr{err: setErr}
			}

			err = db.ApplyCommitted(epaxos.CommittedCommand{
				Ref:     epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 1},
				Command: CommandForTxn(7, 3, []TxnOp{tt.op}),
			})
			if !errors.Is(err, setErr) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestTxnBatchCommitErrorIsReturned(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	commitErr := errors.New("batch commit failed")
	db.newBatch = func() txnBatch {
		return txnBatchWithCommitErr{err: commitErr}
	}

	err = db.ApplyCommitted(epaxos.CommittedCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 4, Conf: 1},
		Command: CommandForTxn(7, 4, []TxnOp{{Key: []byte("alpha"), Value: []byte("one")}}),
	})
	if !errors.Is(err, commitErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestApplyReadyPersistsExecutedRecordAndKVAcrossReopen(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: db.EPaxosStorage()})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := node.Propose(CommandForPut(9, 1, []byte("ready-key"), []byte("ready-value")))
	if err != nil {
		t.Fatal(err)
	}
	rd := node.Ready()
	if len(rd.Committed) != 1 || rd.Committed[0].Ref != ref {
		t.Fatalf("ready committed = %#v, want one command for %s", rd.Committed, ref)
	}
	if err := db.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := node.Advance(rd); err != nil {
		t.Fatal(err)
	}
	executedReady := node.Ready()
	if len(executedReady.Committed) != 0 {
		t.Fatalf("post-advance ready committed = %#v, want no replayed command", executedReady.Committed)
	}
	if len(executedReady.Records) != 1 || executedReady.Records[0].Ref != ref || executedReady.Records[0].Status != epaxos.StatusExecuted {
		t.Fatalf("post-advance ready records = %#v, want one executed record for %s", executedReady.Records, ref)
	}
	if err := db.ApplyReady(executedReady); err != nil {
		t.Fatal(err)
	}
	if err := node.Advance(executedReady); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	value, ok, err := db.Get([]byte("ready-key"))
	if err != nil || !ok || string(value) != "ready-value" {
		t.Fatalf("reopened value=%q ok=%v err=%v", value, ok, err)
	}
	restarted, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: db.EPaxosStorage()})
	if err != nil {
		t.Fatal(err)
	}
	if !hasExecutedRef(restarted.Status().Executed, ref) {
		t.Fatalf("restarted executed refs = %#v, want %s", restarted.Status().Executed, ref)
	}
	if replay := restarted.Ready(); !replay.Empty() {
		t.Fatalf("restart produced Ready for an already executed command: %#v", replay)
	}
}

func TestApplyReadyDuplicateCommittedRefIsIdempotent(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ref := epaxos.InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	key := []byte("duplicate-ready-key")
	value := []byte("duplicate-ready-value")
	cmd := CommandForPut(77, 1, key, value)
	rec := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     ref,
		Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
		Status:  epaxos.StatusCommitted,
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0},
		Command: cmd,
	})
	rd := epaxos.Ready{
		Records: []epaxos.InstanceRecord{rec},
		Committed: []epaxos.CommittedCommand{{
			Ref:     ref,
			Seq:     rec.Seq,
			Deps:    rec.Deps,
			Command: cmd,
		}},
	}

	firstTime := db.nextRecordTime()
	if err := db.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	afterFirst := db.nextRecordTime()
	if afterFirst != firstTime+1 {
		t.Fatalf("next record time after first ApplyReady = %d, want %d", afterFirst, firstTime+1)
	}
	if err := db.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if afterDuplicate := db.nextRecordTime(); afterDuplicate != afterFirst {
		t.Fatalf("duplicate ApplyReady advanced next record time to %d, want %d", afterDuplicate, afterFirst)
	}

	got, ok, err := db.GetExact(key, firstTime)
	if err != nil || !ok || string(got) != string(value) {
		t.Fatalf("value at first timestamp = %q ok=%v err=%v, want %q", got, ok, err, value)
	}
	got, ok, err = db.GetExact(key, afterFirst)
	if err != nil || ok {
		t.Fatalf("value at duplicate timestamp = %q ok=%v err=%v, want absent", got, ok, err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: key})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Key) != string(key) || string(scan[0].Value) != string(value) || scan[0].Time != firstTime {
		t.Fatalf("scan after duplicate ApplyReady = %#v, want one version at time %d", scan, firstTime)
	}
	duplicateScan, err := db.Scan(ScanOptions{Prefix: key, Bounds: ExactTimestamp(afterFirst)})
	if err != nil {
		t.Fatal(err)
	}
	if len(duplicateScan) != 0 {
		t.Fatalf("duplicate timestamp scan = %#v, want no second version", duplicateScan)
	}
}

func TestAppliedReadyCommandReturnsStorageErrors(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 8, Conf: 1}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("applied marker lookup panic=%v, want returned storage error", r)
		}
	}()

	done, err := db.appliedReadyCommand(ref)
	if err == nil {
		t.Fatal("expected applied marker lookup to return the closed database error")
	}
	if done {
		t.Fatal("applied marker lookup reported done despite storage error")
	}
}

func TestPebbleStorageLoadInstancesDecodesDurableRecordOrder(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records := []epaxos.InstanceRecord{
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 3, Instance: 2, Conf: 2},
			Ballot:  epaxos.Ballot{Epoch: 4, Number: 5, Replica: 3},
			Status:  epaxos.StatusAccepted,
			Seq:     8,
			Deps:    []epaxos.InstanceNum{1, 0, 2},
			Command: CommandForPut(20, 1, []byte("late"), []byte("value-late")),
		}),
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 2, Instance: 9, Conf: 1},
			Ballot:  epaxos.Ballot{Epoch: 1, Number: 7, Replica: 2},
			Status:  epaxos.StatusCommitted,
			Seq:     6,
			Deps:    []epaxos.InstanceNum{0, 9, 1},
			Command: CommandForDelete(20, 2, []byte("middle")),
		}),
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 4, Conf: 1},
			Ballot:  epaxos.Ballot{Epoch: 2, Number: 1, Replica: 1},
			Status:  epaxos.StatusPreAccepted,
			Seq:     5,
			Deps:    []epaxos.InstanceNum{4, 0, 0},
			Command: CommandForTxn(20, 3, []TxnOp{{Key: []byte("first"), Value: []byte("value-first")}, {Delete: true, Key: []byte("gone")}}),
		}),
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 2},
			Ballot:  epaxos.Ballot{Epoch: 3, Number: 2, Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     7,
			Deps:    []epaxos.InstanceNum{0, 1, 3},
			Command: CommandForPut(20, 4, []byte("third"), []byte("value-third")),
		}),
	}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: records}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantRefs := []epaxos.InstanceRef{
		{Replica: 1, Instance: 4, Conf: 1},
		{Replica: 2, Instance: 9, Conf: 1},
		{Replica: 1, Instance: 3, Conf: 2},
		{Replica: 3, Instance: 2, Conf: 2},
	}
	if len(loaded) != len(wantRefs) {
		t.Fatalf("loaded %d records, want %d: %#v", len(loaded), len(wantRefs), loaded)
	}
	for i, want := range wantRefs {
		if loaded[i].Ref != want {
			t.Fatalf("loaded[%d] ref = %s, want %s; all records %#v", i, loaded[i].Ref, want, loaded)
		}
		if !epaxos.VerifyRecordChecksum(loaded[i]) {
			t.Fatalf("loaded[%d] checksum was not preserved: %#v", i, loaded[i])
		}
	}
	if loaded[0].Status != epaxos.StatusPreAccepted || loaded[0].Seq != 5 || len(loaded[0].Deps) != 3 || loaded[0].Deps[0] != 4 {
		t.Fatalf("first loaded record lost attributes: %#v", loaded[0])
	}
	if string(loaded[0].Command.ConflictKeys[0]) != "first" || string(loaded[0].Command.ConflictKeys[1]) != "gone" {
		t.Fatalf("transaction conflict keys not decoded: %#v", loaded[0].Command.ConflictKeys)
	}
	if loaded[3].Ballot.Number != 5 || string(loaded[3].Command.Payload) != string(CommandForPut(20, 1, []byte("late"), []byte("value-late")).Payload) {
		t.Fatalf("last loaded record lost ballot or payload: %#v", loaded[3])
	}
}

func TestPebbleStorageRoundTripsFastPathEligibleRecord(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record := checkedKVRecord(epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 1, Instance: 17, Conf: 1},
		Ballot:           epaxos.Ballot{Replica: 1},
		Status:           epaxos.StatusPreAccepted,
		Seq:              9,
		Deps:             []epaxos.InstanceNum{0, 2, 0},
		Command:          CommandForPut(45, 1, []byte("fast-path-key"), []byte("fast-path-value")),
		FastPathEligible: true,
	})
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{record}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded %d records, want one: %#v", len(loaded), loaded)
	}
	got := loaded[0]
	if got.Ref != record.Ref || got.Ballot != record.Ballot || got.Status != record.Status || got.Seq != record.Seq {
		t.Fatalf("loaded record metadata = %#v, want %#v", got, record)
	}
	if !got.FastPathEligible {
		t.Fatalf("loaded record lost FastPathEligible=true: %#v", got)
	}
	if got.Checksum != record.Checksum || !epaxos.VerifyRecordChecksum(got) {
		t.Fatalf("loaded record checksum = %x, want %x and valid for %#v", got.Checksum, record.Checksum, got)
	}
	if len(got.Deps) != len(record.Deps) || got.Deps[1] != record.Deps[1] {
		t.Fatalf("loaded deps = %#v, want %#v", got.Deps, record.Deps)
	}
	if got.Command.ID != record.Command.ID || got.Command.Kind != record.Command.Kind || string(got.Command.Payload) != string(record.Command.Payload) {
		t.Fatalf("loaded command = %#v, want %#v", got.Command, record.Command)
	}
	if len(got.Command.ConflictKeys) != 1 || string(got.Command.ConflictKeys[0]) != "fast-path-key" {
		t.Fatalf("loaded conflict keys = %#v", got.Command.ConflictKeys)
	}
}

func TestEncodeEPaxosRecordPersistsTOQPendingMetadata(t *testing.T) {
	record := checkedKVRecord(epaxos.InstanceRecord{
		Ref:        epaxos.InstanceRef{Replica: 4, Instance: 21, Conf: 2},
		Ballot:     epaxos.Ballot{Epoch: 2, Number: 3, Replica: 4},
		Status:     epaxos.StatusCommitted,
		Seq:        13,
		Deps:       []epaxos.InstanceNum{2, 1, 0, 3},
		Command:    CommandForPut(48, 1, []byte("toq-pending"), []byte("value")),
		ProcessAt:  101,
		TOQPending: true,
	})

	got, err := decodeEPaxosRecord(encodeEPaxosRecord(record))
	if err != nil {
		t.Fatal(err)
	}
	if got.ProcessAt != record.ProcessAt || !got.TOQPending {
		t.Fatalf("decoded TOQ metadata ProcessAt=%d TOQPending=%v, want %d true for %#v", got.ProcessAt, got.TOQPending, record.ProcessAt, got)
	}
	if got.FastPathEligible {
		t.Fatalf("decoded record invented FastPathEligible=true for TOQ-only record: %#v", got)
	}
	if got.Checksum != record.Checksum || !epaxos.VerifyRecordChecksum(got) {
		t.Fatalf("decoded checksum = %x, want original %x and canonical-valid for %#v", got.Checksum, record.Checksum, got)
	}
}

func TestEncodeDecodeEPaxosRecordPreservesAcceptDepsEvidence(t *testing.T) {
	record := checkedKVRecord(epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 2, Instance: 25, Conf: 1},
		Ballot:           epaxos.Ballot{Epoch: 3, Number: 5, Replica: 2},
		Status:           epaxos.StatusAccepted,
		Seq:              18,
		Deps:             []epaxos.InstanceNum{0, 7, 8},
		AcceptSeq:        29,
		AcceptDeps:       []epaxos.InstanceNum{9, 10, 11},
		Command:          CommandForPut(50, 1, []byte("accept-deps"), []byte("evidence")),
		ProcessAt:        113,
		TOQPending:       true,
		FastPathEligible: true,
	})

	got, err := decodeEPaxosRecord(encodeEPaxosRecord(record))
	if err != nil {
		t.Fatal(err)
	}
	if got.AcceptSeq != record.AcceptSeq {
		t.Fatalf("AcceptSeq=%d, want %d for decoded record %#v", got.AcceptSeq, record.AcceptSeq, got)
	}
	if len(got.AcceptDeps) != len(record.AcceptDeps) {
		t.Fatalf("AcceptDeps=%#v, want %#v", got.AcceptDeps, record.AcceptDeps)
	}
	for i := range got.AcceptDeps {
		if got.AcceptDeps[i] != record.AcceptDeps[i] {
			t.Fatalf("AcceptDeps=%#v, want %#v", got.AcceptDeps, record.AcceptDeps)
		}
	}
	if got.Seq != record.Seq {
		t.Fatalf("chosen Seq=%d, want %d for decoded record %#v", got.Seq, record.Seq, got)
	}
	if len(got.Deps) != len(record.Deps) {
		t.Fatalf("chosen Deps=%#v, want %#v", got.Deps, record.Deps)
	}
	for i := range got.Deps {
		if got.Deps[i] != record.Deps[i] {
			t.Fatalf("chosen Deps=%#v, want %#v", got.Deps, record.Deps)
		}
	}
	wantChecksum := epaxos.ChecksumRecord(got)
	if got.Checksum != record.Checksum || got.Checksum != wantChecksum || !epaxos.VerifyRecordChecksum(got) {
		t.Fatalf("decoded checksum = %x, original %x canonical %x valid=%v for %#v", got.Checksum, record.Checksum, wantChecksum, epaxos.VerifyRecordChecksum(got), got)
	}
}

func TestEncodeDecodeEPaxosRecordPreservesSenderAcceptEvidence(t *testing.T) {
	record := checkedKVRecord(epaxos.InstanceRecord{
		Ref:        epaxos.InstanceRef{Replica: 2, Instance: 29, Conf: 1},
		Ballot:     epaxos.Ballot{Epoch: 7, Number: 9, Replica: 2},
		Status:     epaxos.StatusAccepted,
		Seq:        31,
		Deps:       []epaxos.InstanceNum{5, 6, 7},
		AcceptSeq:  41,
		AcceptDeps: []epaxos.InstanceNum{8, 9, 10},
		AcceptEvidence: []epaxos.AcceptEvidence{
			{Sender: 3, Seq: 41, Deps: []epaxos.InstanceNum{8, 9, 10}},
			{Sender: 4, Seq: 43, Deps: []epaxos.InstanceNum{11, 12, 13}},
		},
		Command:          CommandForPut(52, 1, []byte("sender-evidence"), []byte("value")),
		ProcessAt:        377,
		TOQPending:       true,
		FastPathEligible: true,
	})

	got, err := decodeEPaxosRecord(encodeEPaxosRecord(record))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.AcceptEvidence) != len(record.AcceptEvidence) {
		t.Fatalf("AcceptEvidence=%#v, want %#v", got.AcceptEvidence, record.AcceptEvidence)
	}
	for i := range got.AcceptEvidence {
		gotEv, wantEv := got.AcceptEvidence[i], record.AcceptEvidence[i]
		if gotEv.Sender != wantEv.Sender || gotEv.Seq != wantEv.Seq {
			t.Fatalf("AcceptEvidence[%d] sender/seq=%d/%d, want %d/%d in %#v", i, gotEv.Sender, gotEv.Seq, wantEv.Sender, wantEv.Seq, got.AcceptEvidence)
		}
		if len(gotEv.Deps) != len(wantEv.Deps) {
			t.Fatalf("AcceptEvidence[%d].Deps=%#v, want %#v", i, gotEv.Deps, wantEv.Deps)
		}
		for j := range gotEv.Deps {
			if gotEv.Deps[j] != wantEv.Deps[j] {
				t.Fatalf("AcceptEvidence[%d].Deps=%#v, want %#v", i, gotEv.Deps, wantEv.Deps)
			}
		}
	}
	wantChecksum := epaxos.ChecksumRecord(got)
	if got.Checksum != record.Checksum || got.Checksum != wantChecksum || !epaxos.VerifyRecordChecksum(got) {
		t.Fatalf("decoded checksum = %x, original %x canonical %x valid=%v for %#v", got.Checksum, record.Checksum, wantChecksum, epaxos.VerifyRecordChecksum(got), got)
	}
	if epaxos.VerifyRecordChecksumWithoutSenderAcceptEvidence(got) {
		t.Fatalf("decoded v6 checksum %x also verified against the legacy no-sender AcceptEvidence layout for %#v", got.Checksum, got)
	}
}

func TestEncodeDecodeEPaxosRecordPreservesRecordBallot(t *testing.T) {
	record := checkedKVRecord(epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 2, Instance: 27, Conf: 1},
		Ballot:           epaxos.Ballot{Epoch: 5, Number: 13, Replica: 1},
		RecordBallot:     epaxos.Ballot{Epoch: 3, Number: 8, Replica: 2},
		Status:           epaxos.StatusAccepted,
		Seq:              21,
		Deps:             []epaxos.InstanceNum{0, 17, 4},
		AcceptSeq:        34,
		AcceptDeps:       []epaxos.InstanceNum{18, 19, 20},
		Command:          CommandForPut(51, 1, []byte("record-ballot"), []byte("value-ballot")),
		ProcessAt:        233,
		TOQPending:       true,
		FastPathEligible: true,
	})

	got, err := decodeEPaxosRecord(encodeEPaxosRecord(record))
	if err != nil {
		t.Fatal(err)
	}
	if got.Ballot != record.Ballot {
		t.Fatalf("promise Ballot=%#v, want %#v for decoded record %#v", got.Ballot, record.Ballot, got)
	}
	if got.RecordBallot != record.RecordBallot {
		t.Fatalf("RecordBallot=%#v, want persisted value ballot %#v for decoded record %#v", got.RecordBallot, record.RecordBallot, got)
	}
	if got.RecordBallot == got.Ballot {
		t.Fatalf("decoded record collapsed distinct promise and record ballots into %#v", got.Ballot)
	}
	if got.Checksum != record.Checksum || !epaxos.VerifyRecordChecksum(got) {
		t.Fatalf("decoded checksum = %x, want original %x and canonical-valid for %#v", got.Checksum, record.Checksum, got)
	}
}

func TestDecodeEPaxosRecordV4BackfillsRecordBallot(t *testing.T) {
	tests := []struct {
		name       string
		status     epaxos.Status
		ballot     epaxos.Ballot
		wantRecord epaxos.Ballot
	}{
		{
			name:       "accepted legacy record uses durable ballot as record ballot",
			status:     epaxos.StatusAccepted,
			ballot:     epaxos.Ballot{Epoch: 6, Number: 9, Replica: 2},
			wantRecord: epaxos.Ballot{Epoch: 6, Number: 9, Replica: 2},
		},
		{
			name:       "status none promise has no value ballot",
			status:     epaxos.StatusNone,
			ballot:     epaxos.Ballot{Epoch: 6, Number: 10, Replica: 3},
			wantRecord: epaxos.Ballot{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacy := epaxos.InstanceRecord{
				Ref:              epaxos.InstanceRef{Replica: 3, Instance: epaxos.InstanceNum(28 + tt.ballot.Number), Conf: 1},
				Ballot:           tt.ballot,
				Status:           tt.status,
				Seq:              22,
				Deps:             []epaxos.InstanceNum{3, 0, 5},
				AcceptSeq:        35,
				AcceptDeps:       []epaxos.InstanceNum{21, 22, 23},
				Command:          CommandForPut(51, 2, []byte("legacy-record-ballot"), []byte("legacy-value")),
				ProcessAt:        377,
				TOQPending:       true,
				FastPathEligible: true,
			}
			if tt.status == epaxos.StatusNone {
				legacy.Seq = 0
				legacy.AcceptSeq = 0
				legacy.AcceptDeps = nil
				legacy.Command = epaxos.Command{}
				legacy.TOQPending = false
				legacy.FastPathEligible = false
			}
			legacy.Checksum = legacyEPaxosChecksumV4WithoutRecordBallot(legacy)

			got, err := decodeEPaxosRecord(legacyEPaxosRecordV4WithoutRecordBallot(legacy))
			if err != nil {
				t.Fatal(err)
			}
			if got.Ballot != legacy.Ballot {
				t.Fatalf("decoded promise Ballot=%#v, want legacy ballot %#v for %#v", got.Ballot, legacy.Ballot, got)
			}
			if got.RecordBallot != tt.wantRecord {
				t.Fatalf("decoded RecordBallot=%#v, want %#v for legacy status %s and record %#v", got.RecordBallot, tt.wantRecord, tt.status, got)
			}
			if got.Checksum == legacy.Checksum {
				t.Fatalf("decoded checksum stayed on legacy v4 no-record-ballot value %x for %#v", got.Checksum, got)
			}
			wantChecksum := epaxos.ChecksumRecord(got)
			if got.Checksum != wantChecksum || !epaxos.VerifyRecordChecksum(got) {
				t.Fatalf("decoded checksum = %x, want canonical %x and valid for %#v", got.Checksum, wantChecksum, got)
			}
		})
	}
}

func TestDecodeEPaxosRecordMigratesVersion3ChecksumWithoutAcceptEvidence(t *testing.T) {
	legacy := epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 3, Instance: 26, Conf: 2},
		Ballot:           epaxos.Ballot{Epoch: 4, Number: 6, Replica: 3},
		Status:           epaxos.StatusAccepted,
		Seq:              19,
		Deps:             []epaxos.InstanceNum{12, 0, 13},
		Command:          CommandForPut(50, 2, []byte("legacy-acceptless"), []byte("value")),
		ProcessAt:        211,
		TOQPending:       true,
		FastPathEligible: true,
	}
	legacy.Checksum = legacyEPaxosChecksumWithoutAcceptEvidence(legacy)
	if !epaxos.VerifyRecordChecksumWithoutAcceptEvidence(legacy) {
		t.Fatalf("test fixture checksum %x is not valid for the legacy no-Accept-evidence layout", legacy.Checksum)
	}
	if epaxos.VerifyRecordChecksum(legacy) {
		t.Fatalf("test fixture checksum %x unexpectedly matched the canonical Accept-evidence layout", legacy.Checksum)
	}

	got, err := decodeEPaxosRecord(legacyEPaxosRecordV3(legacy))
	if err != nil {
		t.Fatal(err)
	}
	if got.Checksum == legacy.Checksum {
		t.Fatalf("decoded checksum stayed on legacy no-Accept-evidence value %x for %#v", got.Checksum, got)
	}
	wantChecksum := epaxos.ChecksumRecord(got)
	if got.Checksum != wantChecksum || !epaxos.VerifyRecordChecksum(got) {
		t.Fatalf("decoded checksum = %x, want canonical %x and valid for %#v", got.Checksum, wantChecksum, got)
	}
	if got.AcceptSeq != 0 || len(got.AcceptDeps) != 0 {
		t.Fatalf("decoded legacy v3 record invented Accept evidence AcceptSeq=%d AcceptDeps=%#v", got.AcceptSeq, got.AcceptDeps)
	}
	if got.Seq != legacy.Seq {
		t.Fatalf("chosen Seq=%d, want %d for decoded record %#v", got.Seq, legacy.Seq, got)
	}
	if len(got.Deps) != len(legacy.Deps) {
		t.Fatalf("chosen Deps=%#v, want %#v", got.Deps, legacy.Deps)
	}
	for i := range got.Deps {
		if got.Deps[i] != legacy.Deps[i] {
			t.Fatalf("chosen Deps=%#v, want %#v", got.Deps, legacy.Deps)
		}
	}
	if got.ProcessAt != legacy.ProcessAt || got.TOQPending != legacy.TOQPending || got.FastPathEligible != legacy.FastPathEligible {
		t.Fatalf("decoded v3 metadata ProcessAt=%d TOQPending=%v FastPathEligible=%v, want %d %v %v for %#v", got.ProcessAt, got.TOQPending, got.FastPathEligible, legacy.ProcessAt, legacy.TOQPending, legacy.FastPathEligible, got)
	}
}

func TestDecodeEPaxosRecordMigratesPreTOQChecksums(t *testing.T) {
	version2FastFalse := epaxos.InstanceRecord{
		Ref:     epaxos.InstanceRef{Replica: 2, Instance: 22, Conf: 1},
		Ballot:  epaxos.Ballot{Epoch: 1, Number: 4, Replica: 2},
		Status:  epaxos.StatusAccepted,
		Seq:     14,
		Deps:    []epaxos.InstanceNum{0, 5, 0},
		Command: CommandForPut(49, 1, []byte("v2-fast-false"), []byte("value-false")),
	}
	version2FastFalse.Checksum = legacyEPaxosChecksumWithoutTOQ(version2FastFalse)

	version2FastTrue := epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 3, Instance: 23, Conf: 1},
		Ballot:           epaxos.Ballot{Epoch: 1, Number: 5, Replica: 3},
		Status:           epaxos.StatusPreAccepted,
		Seq:              15,
		Deps:             []epaxos.InstanceNum{6, 0, 0},
		Command:          CommandForPut(49, 2, []byte("v2-fast-true"), []byte("value-true")),
		FastPathEligible: true,
	}
	version2FastTrue.Checksum = legacyEPaxosChecksumWithoutTOQ(version2FastTrue)

	version1FastTrue := epaxos.InstanceRecord{
		Ref:              epaxos.InstanceRef{Replica: 4, Instance: 24, Conf: 1},
		Ballot:           epaxos.Ballot{Epoch: 1, Number: 6, Replica: 4},
		Status:           epaxos.StatusPreAccepted,
		Seq:              16,
		Deps:             []epaxos.InstanceNum{0, 0, 7},
		Command:          CommandForPut(49, 3, []byte("v1-fast-true"), []byte("value-true")),
		FastPathEligible: true,
	}
	version1FastTrue.Checksum = legacyEPaxosChecksumWithoutTOQ(version1FastTrue)

	tests := []struct {
		name         string
		input        []byte
		record       epaxos.InstanceRecord
		wantFastPath bool
	}{
		{
			name:   "version 2 explicit false fast path uses pre-TOQ checksum migration",
			input:  legacyEPaxosRecordV2(version2FastFalse),
			record: version2FastFalse,
		},
		{
			name:         "version 2 explicit true fast path byte survives pre-TOQ checksum migration",
			input:        legacyEPaxosRecordV2(version2FastTrue),
			record:       version2FastTrue,
			wantFastPath: true,
		},
		{
			name:         "version 1 omitted fast path byte is recovered before pre-TOQ checksum migration",
			input:        legacyEPaxosRecordV1(version1FastTrue),
			record:       version1FastTrue,
			wantFastPath: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeEPaxosRecord(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got.FastPathEligible != tt.wantFastPath {
				t.Fatalf("FastPathEligible=%v, want %v for decoded record %#v", got.FastPathEligible, tt.wantFastPath, got)
			}
			if got.Checksum == tt.record.Checksum {
				t.Fatalf("decoded checksum stayed on pre-TOQ value %x for %#v", got.Checksum, got)
			}
			wantChecksum := epaxos.ChecksumRecord(got)
			if got.Checksum != wantChecksum || !epaxos.VerifyRecordChecksum(got) {
				t.Fatalf("decoded checksum = %x, want canonical %x and valid for %#v", got.Checksum, wantChecksum, got)
			}
			if got.ProcessAt != 0 || got.TOQPending {
				t.Fatalf("decoded legacy record invented TOQ metadata ProcessAt=%d TOQPending=%v for %#v", got.ProcessAt, got.TOQPending, got)
			}
			if got.Ref != tt.record.Ref || got.Ballot != tt.record.Ballot || got.Status != tt.record.Status || got.Seq != tt.record.Seq {
				t.Fatalf("decoded metadata = %#v, want %#v", got, tt.record)
			}
			if len(got.Deps) != len(tt.record.Deps) {
				t.Fatalf("decoded deps = %#v, want %#v", got.Deps, tt.record.Deps)
			}
			for i := range got.Deps {
				if got.Deps[i] != tt.record.Deps[i] {
					t.Fatalf("decoded deps = %#v, want %#v", got.Deps, tt.record.Deps)
				}
			}
			if got.Command.ID != tt.record.Command.ID || got.Command.Kind != tt.record.Command.Kind || string(got.Command.Payload) != string(tt.record.Command.Payload) {
				t.Fatalf("decoded command = %#v, want %#v", got.Command, tt.record.Command)
			}
			if len(got.Command.ConflictKeys) != len(tt.record.Command.ConflictKeys) {
				t.Fatalf("decoded conflict keys = %#v, want %#v", got.Command.ConflictKeys, tt.record.Command.ConflictKeys)
			}
			for i := range got.Command.ConflictKeys {
				if string(got.Command.ConflictKeys[i]) != string(tt.record.Command.ConflictKeys[i]) {
					t.Fatalf("decoded conflict keys = %#v, want %#v", got.Command.ConflictKeys, tt.record.Command.ConflictKeys)
				}
			}
		})
	}
}

func TestDecodeEPaxosRecordAcceptsVersion1FastPathCompatibility(t *testing.T) {
	legacyNoFastPath := epaxos.InstanceRecord{
		Ref:        epaxos.InstanceRef{Replica: 3, Instance: 20, Conf: 1},
		Ballot:     epaxos.Ballot{Number: 2, Replica: 3},
		Status:     epaxos.StatusPreAccepted,
		Seq:        12,
		Deps:       []epaxos.InstanceNum{0, 4, 0},
		Command:    epaxos.Command{ID: epaxos.CommandID{Client: 47, Sequence: 3}, Payload: []byte("legacy-multi-key-payload"), ConflictKeys: [][]byte{[]byte("legacy-key-a"), []byte("legacy-key-b")}},
		ProcessAt:  99,
		TOQPending: true,
		// This flag was not present in the legacy checksum layout; decoding must
		// leave it false instead of guessing true from bytes that cannot prove it.
		FastPathEligible: true,
	}
	legacyNoFastPath.Checksum = legacyEPaxosChecksumWithoutFastPathOrTOQ(legacyNoFastPath)

	tests := []struct {
		name         string
		record       epaxos.InstanceRecord
		wantFastPath bool
		wantMigrated bool
	}{
		{
			name: "legacy checksum for false flag",
			record: checkedKVRecord(epaxos.InstanceRecord{
				Ref:     epaxos.InstanceRef{Replica: 1, Instance: 18, Conf: 1},
				Ballot:  epaxos.Ballot{Replica: 1},
				Status:  epaxos.StatusAccepted,
				Seq:     10,
				Deps:    []epaxos.InstanceNum{0, 3, 0},
				Command: CommandForPut(46, 1, []byte("legacy-false"), []byte("value-false")),
			}),
		},
		{
			name: "checksum restores omitted true flag",
			record: checkedKVRecord(epaxos.InstanceRecord{
				Ref:              epaxos.InstanceRef{Replica: 2, Instance: 19, Conf: 1},
				Ballot:           epaxos.Ballot{Replica: 2},
				Status:           epaxos.StatusPreAccepted,
				Seq:              11,
				Deps:             []epaxos.InstanceNum{0, 0, 4},
				Command:          CommandForPut(46, 2, []byte("legacy-true"), []byte("value-true")),
				FastPathEligible: true,
			}),
			wantFastPath: true,
		},
		{
			name:         "checksum without fast path or TOQ migrates to canonical false flag",
			record:       legacyNoFastPath,
			wantMigrated: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeEPaxosRecord(legacyEPaxosRecordV1(tt.record))
			if err != nil {
				t.Fatal(err)
			}
			if got.FastPathEligible != tt.wantFastPath {
				t.Fatalf("FastPathEligible=%v, want %v for decoded record %#v", got.FastPathEligible, tt.wantFastPath, got)
			}
			if tt.wantMigrated {
				if got.Checksum == tt.record.Checksum {
					t.Fatalf("decoded checksum stayed on legacy pre-fast-path value %x for %#v", got.Checksum, got)
				}
			} else if got.Checksum != tt.record.Checksum {
				t.Fatalf("decoded checksum = %x, want original %x for %#v", got.Checksum, tt.record.Checksum, got)
			}
			wantChecksum := epaxos.ChecksumRecord(got)
			if got.Checksum != wantChecksum || !epaxos.VerifyRecordChecksum(got) {
				t.Fatalf("decoded checksum = %x, want canonical %x and valid for %#v", got.Checksum, wantChecksum, got)
			}
			if got.Ref != tt.record.Ref || got.Ballot != tt.record.Ballot || got.Status != tt.record.Status || got.Seq != tt.record.Seq {
				t.Fatalf("decoded metadata = %#v, want %#v", got, tt.record)
			}
			if len(got.Deps) != len(tt.record.Deps) {
				t.Fatalf("decoded deps = %#v, want %#v", got.Deps, tt.record.Deps)
			}
			for i := range got.Deps {
				if got.Deps[i] != tt.record.Deps[i] {
					t.Fatalf("decoded deps = %#v, want %#v", got.Deps, tt.record.Deps)
				}
			}
			if got.Command.ID != tt.record.Command.ID || got.Command.Kind != tt.record.Command.Kind || string(got.Command.Payload) != string(tt.record.Command.Payload) {
				t.Fatalf("decoded command = %#v, want %#v", got.Command, tt.record.Command)
			}
			if len(got.Command.ConflictKeys) != len(tt.record.Command.ConflictKeys) {
				t.Fatalf("decoded conflict keys = %#v, want %#v", got.Command.ConflictKeys, tt.record.Command.ConflictKeys)
			}
			for i := range got.Command.ConflictKeys {
				if string(got.Command.ConflictKeys[i]) != string(tt.record.Command.ConflictKeys[i]) {
					t.Fatalf("decoded conflict keys = %#v, want %#v", got.Command.ConflictKeys, tt.record.Command.ConflictKeys)
				}
			}
		})
	}

	t.Run("LoadInstances consumes migrated pre-fast-path checksum", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		if err := db.pebble.Set(epaxosRecordKey(legacyNoFastPath.Ref), legacyEPaxosRecordV1(legacyNoFastPath), pebble.Sync); err != nil {
			t.Fatal(err)
		}

		var loaded []epaxos.InstanceRecord
		if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
			loaded = append(loaded, rec)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 1 {
			t.Fatalf("loaded %d records, want one: %#v", len(loaded), loaded)
		}
		got := loaded[0]
		if got.FastPathEligible {
			t.Fatalf("loaded legacy pre-fast-path record restored unverifiable FastPathEligible=true: %#v", got)
		}
		if got.Checksum == legacyNoFastPath.Checksum {
			t.Fatalf("loaded checksum stayed on legacy pre-fast-path value %x for %#v", got.Checksum, got)
		}
		wantChecksum := epaxos.ChecksumRecord(got)
		if got.Checksum != wantChecksum || !epaxos.VerifyRecordChecksum(got) {
			t.Fatalf("loaded checksum = %x, want canonical %x and valid for %#v", got.Checksum, wantChecksum, got)
		}
		if got.Ref != legacyNoFastPath.Ref || got.Ballot != legacyNoFastPath.Ballot || got.Status != legacyNoFastPath.Status || got.Seq != legacyNoFastPath.Seq {
			t.Fatalf("loaded metadata = %#v, want %#v", got, legacyNoFastPath)
		}
		if len(got.Command.ConflictKeys) != len(legacyNoFastPath.Command.ConflictKeys) {
			t.Fatalf("loaded conflict keys = %#v, want %#v", got.Command.ConflictKeys, legacyNoFastPath.Command.ConflictKeys)
		}
		for i := range got.Command.ConflictKeys {
			if string(got.Command.ConflictKeys[i]) != string(legacyNoFastPath.Command.ConflictKeys[i]) {
				t.Fatalf("loaded conflict keys = %#v, want %#v", got.Command.ConflictKeys, legacyNoFastPath.Command.ConflictKeys)
			}
		}
	})
}

func TestApplyReadyDoesNotPersistExecutedRecordWhenKVStagingFails(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	cmd := CommandForPut(31, 1, []byte("atomic-key"), []byte("atomic-value"))
	rec := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     ref,
		Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
		Status:  epaxos.StatusExecuted,
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0},
		Command: cmd,
	})
	setErr := errors.New("kv stage failed")
	db.newBatch = func() txnBatch {
		return &failOnSetBatch{inner: db.pebble.NewBatch(), failAt: 2, err: setErr}
	}
	err = db.ApplyReady(epaxos.Ready{
		Records:   []epaxos.InstanceRecord{rec},
		Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: rec.Seq, Deps: rec.Deps, Command: cmd}},
	})
	if !errors.Is(err, setErr) {
		t.Fatalf("err=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("failed ApplyReady persisted consensus records: %#v", loaded)
	}
	if value, ok, err := db.Get([]byte("atomic-key")); err != nil || ok {
		t.Fatalf("failed ApplyReady value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestPebbleStorageLoadInstancesReturnsIteratorDecodeAndCallbackErrors(t *testing.T) {
	t.Run("iterator creation", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		sentinel := errors.New("storage iterator")
		oldNewIter := newPebbleStorageIter
		var sawRecordBounds bool
		newPebbleStorageIter = func(_ *pebble.DB, opts *pebble.IterOptions) (kvIterator, error) {
			sawRecordBounds = len(opts.LowerBound) == 2 && opts.LowerBound[0] == epaxosStorePrefix && opts.LowerBound[1] == epaxosRecordEntry
			return nil, sentinel
		}
		t.Cleanup(func() { newPebbleStorageIter = oldNewIter })
		err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error {
			t.Fatal("callback must not run when iterator creation fails")
			return nil
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err=%v", err)
		}
		if !sawRecordBounds {
			t.Fatal("iterator was not scoped to durable record keys")
		}
	})

	t.Run("decode failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		if err := db.pebble.Set(epaxosRecordKey(ref), []byte{epaxosRecordCodec + 1}, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error {
			t.Fatal("callback must not run for a malformed durable record")
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "bad epaxos record version") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("callback failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		rec := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 2, Instance: 3, Conf: 1},
			Ballot:  epaxos.Ballot{Number: 1, Replica: 2},
			Status:  epaxos.StatusCommitted,
			Seq:     4,
			Deps:    []epaxos.InstanceNum{0, 3},
			Command: CommandForPut(41, 1, []byte("callback-key"), []byte("callback-value")),
		})
		if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{rec}}); err != nil {
			t.Fatal(err)
		}
		sentinel := errors.New("load callback")
		var seen []epaxos.InstanceRef
		err = db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
			seen = append(seen, rec.Ref)
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err=%v", err)
		}
		if len(seen) != 1 || seen[0] != rec.Ref {
			t.Fatalf("callback refs=%#v, want only %s", seen, rec.Ref)
		}
	})
}

func TestPebbleStorageApplyReadyRejectsBadRecordsAndLeavesStorageUnchanged(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{}); err != nil {
		t.Fatalf("empty ready err=%v", err)
	}
	bad := epaxos.InstanceRecord{Ref: epaxos.InstanceRef{Replica: 1, Instance: 9, Conf: 1}, Checksum: [32]byte{1}}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{bad}}); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("err=%v", err)
	}
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("invalid record advanced durable storage: %#v", loaded)
	}
}

func TestDBApplyReadyErrorPathsDoNotAdvanceDurableOrKVState(t *testing.T) {
	t.Run("empty ready bypasses batch", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 12
		db.newBatch = func() txnBatch {
			t.Fatal("empty ready must not allocate a write batch")
			return txnBatchWithSetErr{err: errors.New("unreachable")}
		}
		if err := db.ApplyReady(epaxos.Ready{}); err != nil {
			t.Fatalf("empty ready err=%v", err)
		}
		if got := db.nextRecordTime(); got != 12 {
			t.Fatalf("next automatic timestamp advanced to %d for empty ready", got)
		}
	})

	t.Run("invalid consensus record", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 5
		bad := epaxos.InstanceRecord{Ref: epaxos.InstanceRef{Replica: 1, Instance: 10, Conf: 1}, Checksum: [32]byte{1}}
		cmd := CommandForPut(42, 1, []byte("bad-ready"), []byte("value"))
		err = db.ApplyReady(epaxos.Ready{
			Records:   []epaxos.InstanceRecord{bad},
			Committed: []epaxos.CommittedCommand{{Ref: bad.Ref, Command: cmd}},
		})
		if !errors.Is(err, epaxos.ErrChecksumMismatch) {
			t.Fatalf("err=%v", err)
		}
		assertNoEPaxosRecords(t, db)
		if value, ok, err := db.Get([]byte("bad-ready")); err != nil || ok {
			t.Fatalf("invalid record applied value=%q ok=%v err=%v", value, ok, err)
		}
		if got := db.nextRecordTime(); got != 5 {
			t.Fatalf("next automatic timestamp advanced to %d after invalid record", got)
		}
	})

	t.Run("malformed committed command", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 6
		ref := epaxos.InstanceRef{Replica: 1, Instance: 12, Conf: 1}
		payload := []byte{opTxn, 2}
		payload = append(payload, opPut)
		payload = appendKVFields(payload, []byte("txn-ready"), []byte("value"))
		payload = append(payload, opPut, 9)
		cmd := epaxos.Command{Payload: payload, ConflictKeys: [][]byte{[]byte("txn-ready")}}
		rec := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: cmd,
		})
		err = db.ApplyReady(epaxos.Ready{
			Records:   []epaxos.InstanceRecord{rec},
			Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: rec.Seq, Deps: rec.Deps, Command: cmd}},
		})
		if err == nil || !strings.Contains(err.Error(), "kv: malformed command") {
			t.Fatalf("err=%v", err)
		}
		assertNoEPaxosRecords(t, db)
		if value, ok, err := db.Get([]byte("txn-ready")); err != nil || ok {
			t.Fatalf("malformed command value=%q ok=%v err=%v", value, ok, err)
		}
		if got := db.nextRecordTime(); got != 6 {
			t.Fatalf("next automatic timestamp advanced to %d after malformed command", got)
		}
	})

	t.Run("applied marker read failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 8
		lookupErr := errors.New("applied marker read failed")
		key := []byte("marker-read-ready")
		ref := epaxos.InstanceRef{Replica: 1, Instance: 13, Conf: 1}
		var gotRef epaxos.InstanceRef
		db.lookupAppliedMarker = func(ref epaxos.InstanceRef) (io.Closer, error) {
			gotRef = ref
			return nil, lookupErr
		}
		cmd := CommandForPut(42, 4, key, []byte("value"))
		err = db.ApplyReady(epaxos.Ready{
			Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: 1, Deps: []epaxos.InstanceNum{0}, Command: cmd}},
		})
		if !errors.Is(err, lookupErr) {
			t.Fatalf("err=%v", err)
		}
		if gotRef != ref {
			t.Fatalf("applied marker lookup ref=%s, want %s", gotRef, ref)
		}
		if got := db.nextRecordTime(); got != 8 {
			t.Fatalf("next automatic timestamp advanced to %d after applied marker read failure", got)
		}
		assertNoEPaxosRecords(t, db)
		if value, ok, err := db.Get(key); err != nil || ok {
			t.Fatalf("marker read failure value=%q ok=%v err=%v", value, ok, err)
		}
	})

	t.Run("applied marker write failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 9
		markerErr := errors.New("applied marker set failed")
		db.newBatch = func() txnBatch {
			return &failOnSetBatch{inner: db.pebble.NewBatch(), failAt: 3, err: markerErr}
		}
		key := []byte("marker-write-ready")
		ref := epaxos.InstanceRef{Replica: 1, Instance: 14, Conf: 1}
		cmd := CommandForPut(42, 5, key, []byte("value"))
		rec := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: cmd,
		})
		err = db.ApplyReady(epaxos.Ready{
			Records:   []epaxos.InstanceRecord{rec},
			Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: rec.Seq, Deps: rec.Deps, Command: cmd}},
		})
		if !errors.Is(err, markerErr) {
			t.Fatalf("err=%v", err)
		}
		assertNoEPaxosRecords(t, db)
		if value, ok, err := db.Get(key); err != nil || ok {
			t.Fatalf("marker write failure value=%q ok=%v err=%v", value, ok, err)
		}
		if done, err := db.appliedReadyCommand(ref); err != nil || done {
			t.Fatalf("marker after failed write done=%v err=%v", done, err)
		}
		if got := db.nextRecordTime(); got != 9 {
			t.Fatalf("next automatic timestamp advanced to %d after applied marker write failure", got)
		}
	})

	t.Run("batch commit failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 7
		commitErr := errors.New("ready commit failed")
		db.newBatch = func() txnBatch {
			return &commitErrBatch{inner: db.pebble.NewBatch(), err: commitErr}
		}
		ref := epaxos.InstanceRef{Replica: 1, Instance: 11, Conf: 1}
		cmd := CommandForPut(42, 2, []byte("commit-ready"), []byte("value"))
		rec := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: cmd,
		})
		err = db.ApplyReady(epaxos.Ready{
			Records:   []epaxos.InstanceRecord{rec},
			Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: rec.Seq, Deps: rec.Deps, Command: cmd}},
		})
		if !errors.Is(err, commitErr) {
			t.Fatalf("err=%v", err)
		}
		assertNoEPaxosRecords(t, db)
		if value, ok, err := db.Get([]byte("commit-ready")); err != nil || ok {
			t.Fatalf("failed commit value=%q ok=%v err=%v", value, ok, err)
		}
		if got := db.nextRecordTime(); got != 7 {
			t.Fatalf("next automatic timestamp advanced to %d after failed ready commit", got)
		}
	})
}

func TestStageEPaxosRecordsRejectsInvalidChecksumBeforeWriting(t *testing.T) {
	setErr := errors.New("record set failed")
	bad := epaxos.InstanceRecord{Ref: epaxos.InstanceRef{Replica: 1, Instance: 12, Conf: 1}, Checksum: [32]byte{1}}
	if err := stageEPaxosRecords(txnBatchWithSetErr{err: setErr}, []epaxos.InstanceRecord{bad}); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("err=%v", err)
	}
	good := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 13, Conf: 1},
		Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
		Status:  epaxos.StatusAccepted,
		Seq:     2,
		Deps:    []epaxos.InstanceNum{0},
		Command: CommandForDelete(43, 1, []byte("stage")),
	})
	if err := stageEPaxosRecords(txnBatchWithSetErr{err: setErr}, []epaxos.InstanceRecord{good}); !errors.Is(err, setErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestDecodeEPaxosRecordRejectsMalformedRecords(t *testing.T) {
	valid := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 14, Conf: 1},
		Ballot:  epaxos.Ballot{Epoch: 1, Number: 2, Replica: 1},
		Status:  epaxos.StatusCommitted,
		Seq:     3,
		Deps:    []epaxos.InstanceNum{0},
		Command: CommandForPut(44, 1, []byte("decode"), []byte("value")),
	})
	corruptChecksum := encodeEPaxosRecord(valid)
	corruptChecksum[len(corruptChecksum)-1] ^= 0x80
	tests := []struct {
		name    string
		record  []byte
		wantErr string
		wantIs  error
	}{
		{name: "bad version", record: []byte{epaxosRecordCodec + 1}, wantErr: "kv: bad epaxos record version"},
		{name: "excessive dependency count", record: malformedEPaxosRecord(129, 0, true), wantErr: "kv: bad epaxos dependency count"},
		{name: "excessive conflict key count", record: malformedEPaxosRecord(0, 129, true), wantErr: "kv: bad epaxos conflict-key count"},
		{name: "short checksum", record: malformedEPaxosRecord(0, 0, false), wantErr: "kv: malformed epaxos record"},
		{name: "extra bytes", record: append(encodeEPaxosRecord(valid), 1), wantErr: "kv: malformed epaxos record"},
		{name: "checksum mismatch", record: corruptChecksum, wantIs: epaxos.ErrChecksumMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeEPaxosRecord(tt.record)
			switch {
			case tt.wantIs != nil:
				if !errors.Is(err, tt.wantIs) {
					t.Fatalf("err=%v", err)
				}
			case err == nil || err.Error() != tt.wantErr:
				t.Fatalf("err=%v want %q", err, tt.wantErr)
			}
		})
	}
}

func TestDecodeEPaxosRecordRejectsOversizedAcceptDeps(t *testing.T) {
	_, err := decodeEPaxosRecord(malformedEPaxosRecordWithAcceptDeps(129))
	if err == nil || err.Error() != "kv: bad epaxos accept dependency count" {
		t.Fatalf("err=%v want %q", err, "kv: bad epaxos accept dependency count")
	}
}

func TestDecodeEPaxosRecordRejectsMalformedAcceptEvidence(t *testing.T) {
	tests := []struct {
		name    string
		record  []byte
		wantErr string
	}{
		{
			name:    "duplicate sender",
			record:  malformedEPaxosRecordWithAcceptEvidence(2, 3, 9, 0, 3),
			wantErr: "kv: duplicate epaxos accept evidence sender",
		},
		{
			name:    "excessive evidence count",
			record:  malformedEPaxosRecordWithAcceptEvidence(129),
			wantErr: "kv: bad epaxos accept evidence count",
		},
		{
			name:    "excessive evidence dependency count",
			record:  malformedEPaxosRecordWithAcceptEvidence(1, 3, 9, 129),
			wantErr: "kv: bad epaxos accept evidence dependency count",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeEPaxosRecord(tt.record)
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("err=%v want %q", err, tt.wantErr)
			}
		})
	}
}

func TestDirectVersionAndDeleteErrorBranchesPreserveAutomaticTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		write func(*DB) error
	}{
		{
			name: "put set",
			write: func(db *DB) error {
				return db.PutVersion([]byte("direct-put"), []byte("value"), 20)
			},
		},
		{
			name: "delete set",
			write: func(db *DB) error {
				return db.DeleteVersion([]byte("direct-delete"), 20)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			db.nextTime = 9
			setErr := errors.New("direct set failed")
			db.newBatch = func() txnBatch {
				return txnBatchWithSetErr{err: setErr}
			}
			if err := tt.write(db); !errors.Is(err, setErr) {
				t.Fatalf("err=%v", err)
			}
			if got := db.nextRecordTime(); got != 9 {
				t.Fatalf("next automatic timestamp advanced to %d after failed %s", got, tt.name)
			}
		})
	}
}

func TestApplyCommittedEmptyTxnAndDeleteSetErrorDoNotAdvanceTimestamp(t *testing.T) {
	t.Run("empty transaction", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 6
		commitErr := errors.New("empty transaction commit")
		db.newBatch = func() txnBatch {
			return txnBatchWithCommitErr{err: commitErr}
		}
		if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: CommandForTxn(45, 1, nil)}); err != nil {
			t.Fatalf("empty transaction err=%v", err)
		}
		if got := db.nextRecordTime(); got != 6 {
			t.Fatalf("next automatic timestamp advanced to %d for empty transaction", got)
		}
	})

	t.Run("delete set error", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 8
		setErr := errors.New("delete set failed")
		db.newBatch = func() txnBatch {
			return txnBatchWithSetErr{err: setErr}
		}
		err = db.ApplyCommitted(epaxos.CommittedCommand{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 15, Conf: 1},
			Command: CommandForDelete(45, 2, []byte("delete-set")),
		})
		if !errors.Is(err, setErr) {
			t.Fatalf("err=%v", err)
		}
		if value, ok, err := db.Get([]byte("delete-set")); err != nil || ok {
			t.Fatalf("failed delete write value=%q ok=%v err=%v", value, ok, err)
		}
		if got := db.nextRecordTime(); got != 8 {
			t.Fatalf("next automatic timestamp advanced to %d after delete set failure", got)
		}
	})
}

func assertNoEPaxosRecords(t *testing.T, db *DB) {
	t.Helper()
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("durable records advanced: %#v", loaded)
	}
}

func malformedEPaxosRecord(deps, keys uint64, withChecksum bool) []byte {
	out := []byte{epaxosRecordCodec}
	for range 11 {
		out = binary.AppendUvarint(out, 1)
	}
	out = binary.AppendUvarint(out, deps)
	for i := uint64(0); i < deps && i < 128; i++ {
		out = binary.AppendUvarint(out, 1)
	}
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, 0)
	for range 3 {
		out = binary.AppendUvarint(out, 1)
	}
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, keys)
	for i := uint64(0); i < keys && i < 128; i++ {
		out = binary.AppendUvarint(out, 0)
	}
	if withChecksum {
		out = append(out, make([]byte, 32)...)
	}
	return out
}

func malformedEPaxosRecordWithAcceptDeps(acceptDeps uint64) []byte {
	out := []byte{epaxosRecordCodec}
	for range 11 {
		out = binary.AppendUvarint(out, 1)
	}
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, 1)
	out = binary.AppendUvarint(out, acceptDeps)
	return out
}

func malformedEPaxosRecordWithAcceptEvidence(evidence uint64, fields ...uint64) []byte {
	out := []byte{epaxosRecordCodec}
	for range 11 {
		out = binary.AppendUvarint(out, 1)
	}
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, 1)
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, evidence)
	for _, field := range fields {
		out = binary.AppendUvarint(out, field)
	}
	return out
}

func legacyEPaxosRecordV1(rec epaxos.InstanceRecord) []byte {
	out := []byte{1}
	out = binary.AppendUvarint(out, uint64(rec.Ref.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Instance))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Conf))
	out = binary.AppendUvarint(out, rec.Ballot.Epoch)
	out = binary.AppendUvarint(out, rec.Ballot.Number)
	out = binary.AppendUvarint(out, uint64(rec.Ballot.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Status))
	out = binary.AppendUvarint(out, rec.Seq)
	out = binary.AppendUvarint(out, uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, rec.Command.ID.Client)
	out = binary.AppendUvarint(out, rec.Command.ID.Sequence)
	out = binary.AppendUvarint(out, uint64(rec.Command.Kind))
	out = binary.AppendUvarint(out, uint64(len(rec.Command.Payload)))
	out = append(out, rec.Command.Payload...)
	out = binary.AppendUvarint(out, uint64(len(rec.Command.ConflictKeys)))
	for _, key := range rec.Command.ConflictKeys {
		out = binary.AppendUvarint(out, uint64(len(key)))
		out = append(out, key...)
	}
	out = append(out, rec.Checksum[:]...)
	return out
}

func legacyEPaxosRecordV2(rec epaxos.InstanceRecord) []byte {
	out := legacyEPaxosRecordV1(rec)
	checksumOffset := len(out) - len(rec.Checksum)
	out = out[:checksumOffset]
	out[0] = 2
	if rec.FastPathEligible {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = append(out, rec.Checksum[:]...)
	return out
}

func legacyEPaxosRecordV3(rec epaxos.InstanceRecord) []byte {
	out := legacyEPaxosRecordV1(rec)
	checksumOffset := len(out) - len(rec.Checksum)
	out = out[:checksumOffset]
	out[0] = 3
	out = binary.AppendUvarint(out, rec.ProcessAt)
	if rec.TOQPending {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	if rec.FastPathEligible {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = append(out, rec.Checksum[:]...)
	return out
}

func legacyEPaxosRecordV4WithoutRecordBallot(rec epaxos.InstanceRecord) []byte {
	out := []byte{4}
	out = binary.AppendUvarint(out, uint64(rec.Ref.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Instance))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Conf))
	out = binary.AppendUvarint(out, rec.Ballot.Epoch)
	out = binary.AppendUvarint(out, rec.Ballot.Number)
	out = binary.AppendUvarint(out, uint64(rec.Ballot.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Status))
	out = binary.AppendUvarint(out, rec.Seq)
	out = binary.AppendUvarint(out, uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, rec.AcceptSeq)
	out = binary.AppendUvarint(out, uint64(len(rec.AcceptDeps)))
	for _, dep := range rec.AcceptDeps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, rec.Command.ID.Client)
	out = binary.AppendUvarint(out, rec.Command.ID.Sequence)
	out = binary.AppendUvarint(out, uint64(rec.Command.Kind))
	out = binary.AppendUvarint(out, uint64(len(rec.Command.Payload)))
	out = append(out, rec.Command.Payload...)
	out = binary.AppendUvarint(out, uint64(len(rec.Command.ConflictKeys)))
	for _, key := range rec.Command.ConflictKeys {
		out = binary.AppendUvarint(out, uint64(len(key)))
		out = append(out, key...)
	}
	out = binary.AppendUvarint(out, rec.ProcessAt)
	if rec.TOQPending {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	if rec.FastPathEligible {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = append(out, rec.Checksum[:]...)
	return out
}

func legacyEPaxosChecksumV4WithoutRecordBallot(rec epaxos.InstanceRecord) [32]byte {
	h := blake3.New()
	var b [8]byte
	writeUint64 := func(v uint64) {
		binary.LittleEndian.PutUint64(b[:], v)
		_, _ = h.Write(b[:])
	}
	writeByte := func(v byte) {
		_, _ = h.Write([]byte{v})
	}
	writeBytes := func(v []byte) {
		writeUint64(uint64(len(v)))
		_, _ = h.Write(v)
	}

	writeUint64(uint64(rec.Ref.Replica))
	writeUint64(uint64(rec.Ref.Instance))
	writeUint64(uint64(rec.Ref.Conf))
	writeUint64(rec.Ballot.Epoch)
	writeUint64(rec.Ballot.Number)
	writeUint64(uint64(rec.Ballot.Replica))
	writeByte(byte(rec.Status))
	writeUint64(rec.Seq)
	writeUint64(uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		writeUint64(uint64(dep))
	}
	writeUint64(rec.AcceptSeq)
	writeUint64(uint64(len(rec.AcceptDeps)))
	for _, dep := range rec.AcceptDeps {
		writeUint64(uint64(dep))
	}
	writeUint64(rec.Command.ID.Client)
	writeUint64(rec.Command.ID.Sequence)
	writeByte(byte(rec.Command.Kind))
	writeBytes(rec.Command.Payload)
	writeUint64(uint64(len(rec.Command.ConflictKeys)))
	for _, key := range rec.Command.ConflictKeys {
		writeBytes(key)
	}
	if rec.FastPathEligible {
		writeByte(1)
	} else {
		writeByte(0)
	}
	writeUint64(rec.ProcessAt)
	if rec.TOQPending {
		writeByte(1)
	} else {
		writeByte(0)
	}

	var out [32]byte
	sum := h.Sum(out[:0])
	copy(out[:], sum)
	return out
}

func legacyEPaxosChecksumWithoutAcceptEvidence(rec epaxos.InstanceRecord) [32]byte {
	return legacyEPaxosChecksum(rec, true, true)
}

func legacyEPaxosChecksumWithoutTOQ(rec epaxos.InstanceRecord) [32]byte {
	return legacyEPaxosChecksum(rec, true, false)
}

func legacyEPaxosChecksumWithoutFastPathOrTOQ(rec epaxos.InstanceRecord) [32]byte {
	return legacyEPaxosChecksum(rec, false, false)
}

func legacyEPaxosChecksum(rec epaxos.InstanceRecord, includeFastPath, includeTOQ bool) [32]byte {
	h := blake3.New()
	var b [8]byte
	writeUint64 := func(v uint64) {
		binary.LittleEndian.PutUint64(b[:], v)
		_, _ = h.Write(b[:])
	}
	writeByte := func(v byte) {
		_, _ = h.Write([]byte{v})
	}
	writeBytes := func(v []byte) {
		writeUint64(uint64(len(v)))
		_, _ = h.Write(v)
	}

	writeUint64(uint64(rec.Ref.Replica))
	writeUint64(uint64(rec.Ref.Instance))
	writeUint64(uint64(rec.Ref.Conf))
	writeUint64(rec.Ballot.Epoch)
	writeUint64(rec.Ballot.Number)
	writeUint64(uint64(rec.Ballot.Replica))
	writeByte(byte(rec.Status))
	writeUint64(rec.Seq)
	writeUint64(uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		writeUint64(uint64(dep))
	}
	writeUint64(rec.Command.ID.Client)
	writeUint64(rec.Command.ID.Sequence)
	writeByte(byte(rec.Command.Kind))
	writeBytes(rec.Command.Payload)
	writeUint64(uint64(len(rec.Command.ConflictKeys)))
	for _, key := range rec.Command.ConflictKeys {
		writeBytes(key)
	}
	if includeFastPath {
		if rec.FastPathEligible {
			writeByte(1)
		} else {
			writeByte(0)
		}
	}
	if includeTOQ {
		writeUint64(rec.ProcessAt)
		if rec.TOQPending {
			writeByte(1)
		} else {
			writeByte(0)
		}
	}

	var out [32]byte
	sum := h.Sum(out[:0])
	copy(out[:], sum)
	return out
}

type commitErrBatch struct {
	inner txnBatch
	err   error
}

func (b *commitErrBatch) Set(key, value []byte, opts *pebble.WriteOptions) error {
	return b.inner.Set(key, value, opts)
}

func (b *commitErrBatch) Commit(*pebble.WriteOptions) error {
	return b.err
}

func (b *commitErrBatch) Close() error {
	return b.inner.Close()
}

type terminalErrorIterator struct {
	err    error
	closed bool
}

func (i *terminalErrorIterator) First() bool {
	return false
}

func (i *terminalErrorIterator) SeekGE([]byte) bool {
	return false
}

func (i *terminalErrorIterator) Next() bool {
	return false
}

func (i *terminalErrorIterator) Key() []byte {
	return nil
}

func (i *terminalErrorIterator) Value() []byte {
	return nil
}

func (i *terminalErrorIterator) Error() error {
	return i.err
}

func (i *terminalErrorIterator) Close() error {
	i.closed = true
	return nil
}

type scriptedIteratorEntry struct {
	key   []byte
	value []byte
}

type scriptedIterator struct {
	entries []scriptedIteratorEntry
	index   int
	err     error
}

func (i *scriptedIterator) First() bool {
	i.index = 0
	return len(i.entries) > 0
}

func (i *scriptedIterator) SeekGE([]byte) bool {
	i.index = 0
	return len(i.entries) > 0
}

func (i *scriptedIterator) Next() bool {
	i.index++
	return i.index < len(i.entries)
}

func (i *scriptedIterator) Key() []byte {
	return i.entries[i.index].key
}

func (i *scriptedIterator) Value() []byte {
	return i.entries[i.index].value
}

func (i *scriptedIterator) Error() error {
	return i.err
}

func (i *scriptedIterator) Close() error {
	return nil
}

type txnBatchWithSetErr struct {
	err error
}

func (b txnBatchWithSetErr) Set(_, _ []byte, _ *pebble.WriteOptions) error {
	return b.err
}

func (txnBatchWithSetErr) Commit(*pebble.WriteOptions) error {
	return nil
}

func (txnBatchWithSetErr) Close() error {
	return nil
}

type txnBatchWithCommitErr struct {
	err error
}

func (txnBatchWithCommitErr) Set(_, _ []byte, _ *pebble.WriteOptions) error {
	return nil
}

func (b txnBatchWithCommitErr) Commit(*pebble.WriteOptions) error {
	return b.err
}

func (txnBatchWithCommitErr) Close() error {
	return nil
}

type failOnSetBatch struct {
	inner  txnBatch
	failAt int
	seen   int
	err    error
}

func (b *failOnSetBatch) Set(key, value []byte, opts *pebble.WriteOptions) error {
	b.seen++
	if b.seen == b.failAt {
		return b.err
	}
	return b.inner.Set(key, value, opts)
}

func (b *failOnSetBatch) Commit(opts *pebble.WriteOptions) error {
	return b.inner.Commit(opts)
}

func (b *failOnSetBatch) Close() error {
	return b.inner.Close()
}

func checkedKVRecord(rec epaxos.InstanceRecord) epaxos.InstanceRecord {
	if rec.Status >= epaxos.StatusPreAccepted && rec.RecordBallot == (epaxos.Ballot{}) {
		rec.RecordBallot = rec.Ballot
	}
	rec.Checksum = epaxos.ChecksumRecord(rec)
	return rec
}

func hasExecutedRef(refs []epaxos.InstanceRef, want epaxos.InstanceRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func TestPutVersionSameTimestampKeepsLaterValue(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("collision"), []byte("first"), 44); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("collision"), []byte("second"), 44); err != nil {
		t.Fatal(err)
	}
	v, ok, err := db.Get([]byte("collision"))
	if err != nil || !ok || string(v) != "second" {
		t.Fatalf("value=%q ok=%v err=%v", v, ok, err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("collision")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Value) != "second" {
		t.Fatalf("scan %#v", scan)
	}
}

func TestPebbleDBPersistsVersionWriteAcrossReopen(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("durable"), []byte("kept"), 88); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	v, ok, err := db.Get([]byte("durable"))
	if err != nil || !ok || string(v) != "kept" {
		t.Fatalf("value=%q ok=%v err=%v", v, ok, err)
	}
}

func TestScanOmitsOlderValueHiddenByNewerDelete(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("scan-alive"), []byte("kept"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("scan-gone"), []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteVersion([]byte("scan-gone"), 2); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("scan-")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Key) != "scan-alive" || string(scan[0].Value) != "kept" || scan[0].Time != 1 {
		t.Fatalf("scan %#v", scan)
	}
}

func TestApplyCommittedChangesPersistAcrossReopen(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	commands := []epaxos.CommittedCommand{
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command: CommandForPut(7, 1, []byte("applied-put"), []byte("one"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Command: CommandForPut(7, 2, []byte("applied-deleted"), []byte("before"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 1}, Command: CommandForDelete(7, 3, []byte("applied-deleted"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 4, Conf: 1}, Command: CommandForPut(7, 4, []byte("txn-deleted"), []byte("before"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 5, Conf: 1}, Command: CommandForTxn(7, 5, []TxnOp{
			{Key: []byte("txn-put"), Value: []byte("two")},
			{Delete: true, Key: []byte("txn-deleted")},
		})},
	}
	for _, cmd := range commands {
		if err := db.ApplyCommitted(cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	value, ok, err := db.Get([]byte("applied-put"))
	if err != nil || !ok || string(value) != "one" {
		t.Fatalf("applied-put value=%q ok=%v err=%v", value, ok, err)
	}
	if _, ok, err := db.Get([]byte("applied-deleted")); err != nil || ok {
		t.Fatalf("applied-deleted ok=%v err=%v", ok, err)
	}
	value, ok, err = db.Get([]byte("txn-put"))
	if err != nil || !ok || string(value) != "two" {
		t.Fatalf("txn-put value=%q ok=%v err=%v", value, ok, err)
	}
	if _, ok, err := db.Get([]byte("txn-deleted")); err != nil || ok {
		t.Fatalf("txn-deleted ok=%v err=%v", ok, err)
	}
}

func TestApplyCommittedSameRefSameKeyKeepsLaterValue(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ref := epaxos.InstanceRef{Replica: 1, Instance: 44, Conf: 1}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Ref: ref, Command: CommandForPut(7, 1, []byte("same-ref"), []byte("first"))}); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Ref: ref, Command: CommandForPut(7, 2, []byte("same-ref"), []byte("second"))}); err != nil {
		t.Fatal(err)
	}
	value, ok, err := db.Get([]byte("same-ref"))
	if err != nil || !ok || string(value) != "second" {
		t.Fatalf("value=%q ok=%v err=%v", value, ok, err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("same-ref")})
	if err != nil {
		t.Fatal(err)
	}
	wantTime := uint64(2)
	if len(scan) != 1 || string(scan[0].Value) != "second" || scan[0].Time != wantTime {
		t.Fatalf("scan %#v", scan)
	}
}

func TestReverseScanUsesNewestVersionForRepeatedKey(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("scan-b"), []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("scan-b"), []byte("new"), 2); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("scan-c"), []byte("later"), 3); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range scan {
		if string(kv.Key) == "scan-b" {
			if string(kv.Value) != "new" {
				t.Fatalf("reverse scan returned repeated key value %q in %#v", kv.Value, scan)
			}
			return
		}
	}
	t.Fatalf("reverse scan missed repeated key in %#v", scan)
}

func TestTimestampPointReadsApplyBoundsTombstonesAndExactMatching(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PutVersion([]byte("point"), []byte("old"), 5); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("point"), []byte("collision-loser"), 10); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("point"), []byte("collision-winner"), 10); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("point"), []byte("new"), 15); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteVersion([]byte("point"), 20); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("point"), []byte("after-delete"), 25); err != nil {
		t.Fatal(err)
	}

	value, ok, err := db.GetExact([]byte("point"), 10)
	if err != nil || !ok || string(value) != "collision-winner" {
		t.Fatalf("exact collision value=%q ok=%v err=%v", value, ok, err)
	}
	value[0] = 'X'
	value, ok, err = db.GetExact([]byte("point"), 10)
	if err != nil || !ok || string(value) != "collision-winner" {
		t.Fatalf("exact value was not copied: value=%q ok=%v err=%v", value, ok, err)
	}

	value, ok, err = db.GetAtOrBefore([]byte("point"), 17)
	if err != nil || !ok || string(value) != "new" {
		t.Fatalf("at-or-before value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetWithinBounds([]byte("point"), 6, 12)
	if err != nil || !ok || string(value) != "collision-winner" {
		t.Fatalf("bounded value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetExact([]byte("point"), 5)
	if err != nil || !ok || string(value) != "old" {
		t.Fatalf("exact old value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetExact([]byte("point"), 11)
	if err != nil || ok || value != nil {
		t.Fatalf("inexact timestamp value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetExact([]byte("point"), 20)
	if err != nil || ok || value != nil {
		t.Fatalf("exact tombstone value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetAtOrBefore([]byte("point"), 22)
	if err != nil || ok || value != nil {
		t.Fatalf("tombstone at upper bound value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetWithinBounds([]byte("point"), 6, 22)
	if err != nil || ok || value != nil {
		t.Fatalf("bounded tombstone value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestTimestampCollisionWithTombstoneMakesExactReadAbsent(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PutVersion([]byte("same-ts-delete"), []byte("live"), 44); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteVersion([]byte("same-ts-delete"), 44); err != nil {
		t.Fatal(err)
	}

	value, ok, err := db.GetExact([]byte("same-ts-delete"), 44)
	if err != nil || ok || value != nil {
		t.Fatalf("exact same-timestamp tombstone value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetAtOrBefore([]byte("same-ts-delete"), 44)
	if err != nil || ok || value != nil {
		t.Fatalf("at-or-before same-timestamp tombstone value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestTimestampScanBoundsChooseEligibleVersionPerKey(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, write := range []struct {
		key   string
		value string
		ts    uint64
	}{
		{key: "ts-scan-a", value: "old-a", ts: 10},
		{key: "ts-scan-a", value: "too-new-a", ts: 30},
		{key: "ts-scan-b", value: "old-b", ts: 12},
		{key: "ts-scan-b", value: "too-new-b", ts: 25},
		{key: "ts-scan-c", value: "inside-c", ts: 16},
		{key: "ts-scan-d", value: "old-d", ts: 12},
		{key: "ts-scan-d", value: "newest-inside-d", ts: 17},
	} {
		if err := db.PutVersion([]byte(write.key), []byte(write.value), write.ts); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteVersion([]byte("ts-scan-b"), 18); err != nil {
		t.Fatal(err)
	}

	scan, err := db.Scan(ScanOptions{Prefix: []byte("ts-scan-"), Bounds: TimestampAtOrBefore(20)})
	if err != nil {
		t.Fatal(err)
	}
	assertKVRows(t, scan, []KV{
		{Key: []byte("ts-scan-a"), Value: []byte("old-a"), Time: 10},
		{Key: []byte("ts-scan-c"), Value: []byte("inside-c"), Time: 16},
		{Key: []byte("ts-scan-d"), Value: []byte("newest-inside-d"), Time: 17},
	})
	scan[0].Key[0] = 'X'
	scan[0].Value[0] = 'X'
	scan, err = db.Scan(ScanOptions{Prefix: []byte("ts-scan-"), Bounds: TimestampAtOrBefore(20)})
	if err != nil {
		t.Fatal(err)
	}
	assertKVRows(t, scan, []KV{
		{Key: []byte("ts-scan-a"), Value: []byte("old-a"), Time: 10},
		{Key: []byte("ts-scan-c"), Value: []byte("inside-c"), Time: 16},
		{Key: []byte("ts-scan-d"), Value: []byte("newest-inside-d"), Time: 17},
	})

	scan, err = db.Scan(ScanOptions{Prefix: []byte("ts-scan-"), Bounds: TimestampWithinBounds(12, 18)})
	if err != nil {
		t.Fatal(err)
	}
	assertKVRows(t, scan, []KV{
		{Key: []byte("ts-scan-c"), Value: []byte("inside-c"), Time: 16},
		{Key: []byte("ts-scan-d"), Value: []byte("newest-inside-d"), Time: 17},
	})

	scan, err = db.Scan(ScanOptions{Prefix: []byte("ts-scan-"), Bounds: ExactTimestamp(12)})
	if err != nil {
		t.Fatal(err)
	}
	assertKVRows(t, scan, []KV{
		{Key: []byte("ts-scan-b"), Value: []byte("old-b"), Time: 12},
		{Key: []byte("ts-scan-d"), Value: []byte("old-d"), Time: 12},
	})
}

func TestRelativeStalenessPointReadsUseExplicitReferenceTimestamp(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, write := range []struct {
		key   string
		value string
		ts    uint64
	}{
		{key: "stale-key", value: "lower-inclusive", ts: 10},
		{key: "stale-key", value: "reference-newer", ts: 18},
		{key: "stale-key", value: "per-key-too-new", ts: 22},
		{key: "clock-key", value: "global-latest", ts: 30},
		{key: "zero-key", value: "zero", ts: 0},
	} {
		if err := db.PutVersion([]byte(write.key), []byte(write.value), write.ts); err != nil {
			t.Fatal(err)
		}
	}

	value, ok, err := db.GetBoundedStaleness([]byte("stale-key"), 12, 3)
	if err != nil || !ok || string(value) != "lower-inclusive" {
		t.Fatalf("bounded staleness explicit reference value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetBoundedStaleness([]byte("stale-key"), 18, 0)
	if err != nil || !ok || string(value) != "reference-newer" {
		t.Fatalf("bounded staleness upper-inclusive value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetBoundedStaleness([]byte("zero-key"), 2, 5)
	if err != nil || !ok || string(value) != "zero" {
		t.Fatalf("bounded staleness saturated lower bound value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetExactStaleness([]byte("stale-key"), 12, 2)
	if err != nil || !ok || string(value) != "lower-inclusive" {
		t.Fatalf("exact staleness explicit reference value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.GetExactStaleness([]byte("stale-key"), 1, 2)
	if err != nil || ok || value != nil {
		t.Fatalf("exact staleness underflow value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestRelativeStalenessReadsDoNotAdvanceLatestRecordTime(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PutVersion([]byte("latest-key"), []byte("latest"), 30); err != nil {
		t.Fatal(err)
	}
	latest := db.LatestRecordTime()
	if latest != 30 {
		t.Fatalf("latest record time=%d, want 30", latest)
	}

	if _, _, err := db.GetBoundedStaleness([]byte("latest-key"), 30, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.GetExactStaleness([]byte("latest-key"), 30, 0); err != nil {
		t.Fatal(err)
	}
	if got := db.LatestRecordTime(); got != latest {
		t.Fatalf("staleness reads advanced latest record time from %d to %d", latest, got)
	}
}

func TestRelativeStalenessScansUseExplicitReferenceTimestamp(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, write := range []struct {
		key   string
		value string
		ts    uint64
	}{
		{key: "rel-scan-a", value: "inside-a", ts: 10},
		{key: "rel-scan-a", value: "too-new-a", ts: 22},
		{key: "rel-scan-b", value: "lower-inclusive-b", ts: 9},
		{key: "rel-scan-b", value: "too-new-b", ts: 14},
		{key: "rel-scan-c", value: "too-old-c", ts: 8},
		{key: "rel-scan-c", value: "global-latest-c", ts: 30},
		{key: "rel-scan-d", value: "deleted-inside-d", ts: 11},
	} {
		if err := db.PutVersion([]byte(write.key), []byte(write.value), write.ts); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteVersion([]byte("rel-scan-d"), 12); err != nil {
		t.Fatal(err)
	}

	scan, err := db.Scan(ScanOptions{Prefix: []byte("rel-scan-"), Bounds: BoundedStaleness(12, 3)})
	if err != nil {
		t.Fatal(err)
	}
	assertKVRows(t, scan, []KV{
		{Key: []byte("rel-scan-a"), Value: []byte("inside-a"), Time: 10},
		{Key: []byte("rel-scan-b"), Value: []byte("lower-inclusive-b"), Time: 9},
	})

	scan, err = db.Scan(ScanOptions{Prefix: []byte("rel-scan-"), Bounds: ExactStaleness(12, 2)})
	if err != nil {
		t.Fatal(err)
	}
	assertKVRows(t, scan, []KV{
		{Key: []byte("rel-scan-a"), Value: []byte("inside-a"), Time: 10},
	})
}

func TestExactStalenessUnderflowDoesNotWrapAcrossCivilClockBoundary(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PutVersion([]byte("civil-underflow"), []byte("wrapped"), ^uint64(0)); err != nil {
		t.Fatal(err)
	}

	value, ok, err := db.GetExactStaleness([]byte("civil-underflow"), 0, 1)
	if err != nil || ok || value != nil {
		t.Fatalf("exact staleness underflow value=%q ok=%v err=%v", value, ok, err)
	}

	scan, err := db.Scan(ScanOptions{Prefix: []byte("civil-underflow"), Bounds: ExactStaleness(0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 0 {
		t.Fatalf("exact staleness underflow scan=%#v", scan)
	}
}

func TestBoundedStalenessUsesNumericRecordTimesAcrossRepeatedCivilBoundary(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	const boundary = uint64(20161231235960)
	for _, write := range []struct {
		value string
		ts    uint64
	}{
		{value: "lower-adjacent", ts: boundary - 1},
		{value: "first-at-boundary", ts: boundary},
		{value: "winner-at-boundary", ts: boundary},
		{value: "newer-adjacent", ts: boundary + 1},
	} {
		if err := db.PutVersion([]byte("civil-repeat"), []byte(write.value), write.ts); err != nil {
			t.Fatal(err)
		}
	}

	value, ok, err := db.GetBoundedStaleness([]byte("civil-repeat"), boundary, 1)
	if err != nil || !ok || string(value) != "winner-at-boundary" {
		t.Fatalf("bounded staleness boundary value=%q ok=%v err=%v", value, ok, err)
	}

	value, ok, err = db.GetExactStaleness([]byte("civil-repeat"), boundary, 1)
	if err != nil || !ok || string(value) != "lower-adjacent" {
		t.Fatalf("exact staleness adjacent value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestStalenessScanKeepsPerKeyHistoryAcrossCivilBoundary(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	const boundary = uint64(20161231235960)
	for _, write := range []struct {
		key   string
		value string
		ts    uint64
	}{
		{key: "civil-scan-a", value: "too-old-a", ts: boundary - 2},
		{key: "civil-scan-a", value: "chosen-a", ts: boundary - 1},
		{key: "civil-scan-a", value: "too-new-a", ts: boundary + 1},
		{key: "civil-scan-b", value: "lower-b", ts: boundary - 1},
		{key: "civil-scan-b", value: "chosen-b", ts: boundary},
		{key: "civil-scan-b", value: "too-new-b", ts: boundary + 1},
		{key: "civil-scan-c", value: "too-old-c", ts: boundary - 2},
		{key: "civil-scan-c", value: "too-new-c", ts: boundary + 1},
		{key: "civil-scan-d", value: "deleted-d", ts: boundary - 1},
	} {
		if err := db.PutVersion([]byte(write.key), []byte(write.value), write.ts); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteVersion([]byte("civil-scan-d"), boundary); err != nil {
		t.Fatal(err)
	}

	scan, err := db.Scan(ScanOptions{Prefix: []byte("civil-scan-"), Bounds: BoundedStaleness(boundary, 1)})
	if err != nil {
		t.Fatal(err)
	}
	assertKVRows(t, scan, []KV{
		{Key: []byte("civil-scan-a"), Value: []byte("chosen-a"), Time: boundary - 1},
		{Key: []byte("civil-scan-b"), Value: []byte("chosen-b"), Time: boundary},
	})
}

func TestTimestampBoundsValidateRejectsInvalidInterval(t *testing.T) {
	err := TimestampWithinBounds(11, 10).Validate()
	if !errors.Is(err, ErrInvalidTimestampBounds) {
		t.Fatalf("err=%v, want invalid timestamp bounds", err)
	}
}

func TestZeroValueDBHasNoLatestRecordTime(t *testing.T) {
	var db DB
	if got := db.LatestRecordTime(); got != 0 {
		t.Fatalf("latest record time=%d, want 0", got)
	}
}

func TestExactStalenessUnderflowClassifiesAllRecordsTooOld(t *testing.T) {
	bounds := ExactStaleness(2, 3)
	for _, ts := range []uint64{0, 2, ^uint64(0)} {
		if got := bounds.classify(ts); got != timestampTooOld {
			t.Fatalf("classify(%d)=%v, want too old", ts, got)
		}
	}
}

func TestGetWithBoundsSkipsMalformedAndTooNewVersions(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	iter := &scriptedIterator{
		index: -1,
		entries: []scriptedIteratorEntry{
			{key: []byte("not a data key"), value: []byte{valueRecord, 'b', 'a', 'd'}},
			{key: EncodeDataKey(nil, db.cf, []byte("record"), 9), value: []byte{valueRecord, 'f', 'u', 't', 'u', 'r', 'e'}},
			{key: EncodeDataKey(nil, db.cf, []byte("record"), 5), value: []byte{valueRecord, 'e', 'l', 'i', 'g', 'i', 'b', 'l', 'e'}},
		},
	}
	db.newIter = func(*pebble.IterOptions) (kvIterator, error) {
		return iter, nil
	}

	value, ok, err := db.GetWithBounds([]byte("record"), TimestampWithinBounds(1, 5))
	if err != nil || !ok || string(value) != "eligible" {
		t.Fatalf("value=%q ok=%v err=%v, want eligible row", value, ok, err)
	}
}

func TestScanRejectsInvalidRangeKeys(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, tc := range []struct {
		name string
		opt  ScanOptions
	}{
		{name: "start", opt: ScanOptions{Start: []byte("bad\x00start")}},
		{name: "end", opt: ScanOptions{End: []byte("bad\x00end")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.Scan(tc.opt); !errors.Is(err, errInvalidKey) {
				t.Fatalf("err=%v, want invalid key", err)
			}
		})
	}
}

func TestTimestampReadsRejectInvalidBoundsAndKeys(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PutVersion([]byte("valid"), []byte("value"), 7); err != nil {
		t.Fatal(err)
	}
	value, ok, err := db.GetWithinBounds([]byte("valid"), 9, 8)
	if !errors.Is(err, ErrInvalidTimestampBounds) {
		t.Fatalf("point read err=%v, want invalid timestamp bounds", err)
	}
	if ok || value != nil {
		t.Fatalf("invalid point-read bounds returned value=%q ok=%v", value, ok)
	}

	if _, err := db.Scan(ScanOptions{Prefix: []byte("valid"), Bounds: TimestampWithinBounds(9, 8)}); !errors.Is(err, ErrInvalidTimestampBounds) {
		t.Fatalf("scan err=%v, want invalid timestamp bounds", err)
	}

	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{name: "empty", key: nil},
		{name: "embedded separator", key: []byte("bad\x00key")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, read := range []struct {
				name string
				call func([]byte) ([]byte, bool, error)
			}{
				{
					name: "at or before",
					call: func(key []byte) ([]byte, bool, error) {
						return db.GetAtOrBefore(key, 7)
					},
				},
				{
					name: "within bounds",
					call: func(key []byte) ([]byte, bool, error) {
						return db.GetWithinBounds(key, 1, 7)
					},
				},
				{
					name: "exact",
					call: func(key []byte) ([]byte, bool, error) {
						return db.GetExact(key, 7)
					},
				},
				{
					name: "bounded staleness",
					call: func(key []byte) ([]byte, bool, error) {
						return db.GetBoundedStaleness(key, 7, 1)
					},
				},
				{
					name: "exact staleness",
					call: func(key []byte) ([]byte, bool, error) {
						return db.GetExactStaleness(key, 0, 1)
					},
				},
			} {
				t.Run(read.name, func(t *testing.T) {
					value, ok, err := read.call(tc.key)
					if !errors.Is(err, errInvalidKey) {
						t.Fatalf("err=%v, want invalid key", err)
					}
					if ok || value != nil {
						t.Fatalf("invalid key returned value=%q ok=%v", value, ok)
					}
				})
			}
		})
	}

	if _, err := db.Scan(ScanOptions{Prefix: []byte("bad\x00key"), Bounds: TimestampAtOrBefore(7)}); !errors.Is(err, errInvalidKey) {
		t.Fatalf("scan invalid prefix err=%v, want invalid key", err)
	}
}

func assertKVRows(t *testing.T, got []KV, want []KV) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows=%#v, want %#v", got, want)
	}
	for i := range want {
		if string(got[i].Key) != string(want[i].Key) ||
			string(got[i].Value) != string(want[i].Value) ||
			got[i].Time != want[i].Time {
			t.Fatalf("row %d=%#v, want %#v in rows %#v", i, got[i], want[i], got)
		}
	}
}
