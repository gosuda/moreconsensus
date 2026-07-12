package kv

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

func TestOpenWithOptionsAndStorageStats(t *testing.T) {
	cache := pebble.NewCache(1 << 20)
	defer cache.Unref()
	options := &pebble.Options{
		Cache:                       cache,
		BytesPerSync:                512 << 10,
		WALBytesPerSync:             0,
		MemTableSize:                1 << 20,
		MemTableStopWritesThreshold: 2,
		MaxOpenFiles:                128,
		MaxConcurrentCompactions:    func() int { return 1 },
	}
	path := t.TempDir()
	db, err := OpenWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	record := hardStateTestRecord(
		epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 1}},
	)
	if err := db.ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{record}}); err != nil {
		t.Fatal(err)
	}
	first := db.StorageStats()
	if first.DurableInstanceRecords != 1 {
		t.Fatalf("durable records=%d, want 1", first.DurableInstanceRecords)
	}
	if first.WALBytes == 0 || first.DiskUsageBytes == 0 {
		t.Fatalf("storage byte stats not populated: %+v", first)
	}
	if err := db.ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{record}}); err != nil {
		t.Fatal(err)
	}
	if got := db.StorageStats().DurableInstanceRecords; got != 1 {
		t.Fatalf("record overwrite changed durable count to %d", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if got := reopened.StorageStats().DurableInstanceRecords; got != 1 {
		t.Fatalf("startup durable count=%d, want 1", got)
	}
	if next, err := reopened.NextCommandSequence(1); err != nil || next != 2 {
		t.Fatalf("next durable command sequence=%d err=%v, want 2", next, err)
	}
}

func TestNextCommandSequenceSurvivesReopenAndIncludesUncommittedRecords(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record := hardStateTestRecord(
		epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		epaxos.Command{ID: epaxos.CommandID{Client: 9, Sequence: 7}},
	)
	record.Status = epaxos.StatusAccepted
	record.Checksum = epaxos.ChecksumRecord(record)
	if err := db.ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{record}}); err != nil {
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
	if next, err := reopened.NextCommandSequence(9); err != nil || next != 8 {
		t.Fatalf("next durable command sequence=%d err=%v, want 8", next, err)
	}
	if next, err := reopened.NextCommandSequence(10); err != nil || next != 1 {
		t.Fatalf("new client sequence=%d err=%v, want 1", next, err)
	}
}

func TestOpenWithOptionsShallowClonesCallerOptions(t *testing.T) {
	options := &pebble.Options{MemTableSize: 1 << 20, MemTableStopWritesThreshold: 2}
	db, err := OpenWithOptions(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	options.ReadOnly = true
	if err := db.PutVersion([]byte("key"), []byte("value"), 1); err != nil {
		t.Fatalf("caller mutation changed open database options: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
