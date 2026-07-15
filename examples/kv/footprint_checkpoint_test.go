package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

func TestFootprintOrderedRangeUsesLogicalSpan(t *testing.T) {
	command, err := CommandForScan(7, 1, ScanOptions{Start: []byte("a"), End: []byte("z"), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(command.Footprint.Points) != 0 || len(command.Footprint.Spans) != 1 {
		t.Fatalf("scan footprint=%#v, want one logical span", command.Footprint)
	}
	span := command.Footprint.Spans[0]
	if !bytes.Equal(span.Start, []byte("a")) || !bytes.Equal(span.End, []byte("z")) {
		t.Fatalf("scan span=[%q,%q), want [a,z)", span.Start, span.End)
	}
	for _, test := range []struct {
		key      string
		conflict bool
	}{{"a", true}, {"m", true}, {"z", false}} {
		point := CommandForPut(7, 2, []byte(test.key), []byte("value"))
		if got := command.ConflictsWith(point); got != test.conflict {
			t.Fatalf("span conflict with %q=%t, want %t", test.key, got, test.conflict)
		}
	}

	prefix, err := CommandForScan(7, 3, ScanOptions{Prefix: []byte("row/"), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(prefix.Footprint.Spans) != 1 || !bytes.Equal(prefix.Footprint.Spans[0].Start, []byte("row/")) ||
		!bytes.Equal(prefix.Footprint.Spans[0].End, []byte("row0")) {
		t.Fatalf("prefix footprint=%#v, want [row/,row0)", prefix.Footprint)
	}
}

func TestCommandResultDedupAndOrderedRead(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	put := CommandForPut(11, 1, []byte("key"), []byte("value"))
	get := CommandForGet(11, 2, []byte("key"))
	ready := epaxos.Ready{Apply: []epaxos.ApplyCommand{
		{Ref: epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 1}, Command: put},
		{Ref: epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 2}, Command: get},
	}}
	if err := db.ApplyReady(ready); err != nil {
		t.Fatal(err)
	}
	response, found, err := db.CommandResult(get.ID)
	if err != nil || !found {
		t.Fatalf("ordered get result found=%t err=%v", found, err)
	}
	result, err := DecodeOrderedGetResult(response)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || !bytes.Equal(result.Value, []byte("value")) {
		t.Fatalf("ordered get result=%#v, want value", result)
	}

	duplicate := epaxos.Ready{Apply: []epaxos.ApplyCommand{{
		Ref: epaxos.InstanceRef{Conf: 1, Replica: 2, Instance: 9}, Command: put,
	}}}
	before := db.LatestRecordTime()
	if err := db.ApplyReady(duplicate); err != nil {
		t.Fatal(err)
	}
	if after := db.LatestRecordTime(); after != before {
		t.Fatalf("duplicate advanced record time from %d to %d", before, after)
	}

	reused := CommandForPut(11, 1, []byte("key"), []byte("different"))
	if err := db.ApplyReady(epaxos.Ready{Apply: []epaxos.ApplyCommand{{Command: reused}}}); err == nil {
		t.Fatal("command ID reuse with a different digest succeeded")
	}
	if next, err := db.NextCommandSequence(11); err != nil || next != 3 {
		t.Fatalf("next command sequence=%d err=%v, want 3", next, err)
	}
}

func TestSnapshotInstallRestoresApplicationAndDedupState(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	initial := CommandForPut(21, 1, []byte("snapshot-key"), []byte("before"))
	if err := db.ApplyReady(epaxos.Ready{Apply: []epaxos.ApplyCommand{{Command: initial}}}); err != nil {
		t.Fatal(err)
	}
	var checkpointID epaxos.CheckpointID
	checkpointID[0] = 1
	result, err := db.CreateApplicationCheckpoint(epaxos.CheckpointRequest{ID: checkpointID})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := epaxos.Checkpoint{
		Descriptor:          epaxos.CheckpointDescriptor{ApplicationDigest: result.ApplicationDigest},
		ApplicationSnapshot: result.ApplicationSnapshot,
	}

	mutated := CommandForPut(21, 2, []byte("snapshot-key"), []byte("after"))
	if err := db.ApplyReady(epaxos.Ready{Apply: []epaxos.ApplyCommand{{Command: mutated}}}); err != nil {
		t.Fatal(err)
	}
	if err := db.installApplicationSnapshot(checkpoint); err != nil {
		t.Fatal(err)
	}
	value, ok, err := db.Get([]byte("snapshot-key"))
	if err != nil || !ok || !bytes.Equal(value, []byte("before")) {
		t.Fatalf("restored value=%q ok=%t err=%v, want before", value, ok, err)
	}
	if _, found, err := db.CommandResult(initial.ID); err != nil || !found {
		t.Fatalf("checkpoint lost initial dedup result found=%t err=%v", found, err)
	}
	if _, found, err := db.CommandResult(mutated.ID); err != nil || found {
		t.Fatalf("checkpoint retained post-cut dedup result found=%t err=%v", found, err)
	}
	if err := db.MaterializeApplicationSnapshot(result.ApplicationSnapshot, []byte("corrupt")); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("corrupt materialization err=%v, want checksum mismatch", err)
	}
}

func TestCheckpointCompactionThreeReplicaRestartSmoke(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	newNode := func(config epaxos.Config) (*epaxos.RawNode, error) {
		config.RetainExecutedPerLane = 5
		return epaxos.NewRawNode(config)
	}
	cluster, err := openCluster(paths, newNode)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cluster != nil {
			_ = cluster.Close()
		}
	}()

	scan, err := CommandForScan(77, 1, ScanOptions{Start: []byte("a"), End: []byte("z"), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	scanRef, err := cluster.Nodes[1].Propose(scan)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drain(); err != nil {
		t.Fatal(err)
	}
	point := CommandForPut(77, 2, []byte("m"), []byte("phantom"))
	pointRef, err := cluster.Nodes[1].Propose(point)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drain(); err != nil {
		t.Fatal(err)
	}
	pointRecord, found, err := cluster.DBs[1].LoadInstance(pointRef)
	if err != nil || !found {
		t.Fatalf("load point record found=%t err=%v", found, err)
	}
	if len(pointRecord.Deps) < 1 || pointRecord.Deps[0] < scanRef.Instance {
		t.Fatalf("point %s deps=%v do not retain preceding overlapping scan %s", pointRef, pointRecord.Deps, scanRef)
	}

	for index := range 10 {
		key := []byte{byte('n' + index)}
		command := CommandForPut(88, uint64(index+1), key, []byte("tail")) //nolint:gosec // bounded test index
		if _, err := cluster.Nodes[1].Propose(command); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Drain(); err != nil {
			t.Fatal(err)
		}
	}
	certified := false
	for range 500 {
		if err := cluster.TickAll(); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Drain(); err != nil {
			t.Fatal(err)
		}
		certified = true
		for _, id := range cluster.ids {
			checkpoint, err := cluster.DBs[id].EPaxosStorage().LoadCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			if checkpoint.Certificate.Empty() || len(checkpoint.CompactedThrough.Configs) == 0 {
				certified = false
				break
			}
		}
		if certified {
			break
		}
	}
	if !certified {
		t.Fatal("three-replica checkpoint did not certify and compact")
	}

	for _, id := range cluster.ids {
		db := cluster.DBs[id]
		checkpoint, err := db.EPaxosStorage().LoadCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		prefix := []byte{epaxosStorePrefix, epaxosRecordEntry}
		iter, err := db.pebble.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixLimit(prefix)})
		if err != nil {
			t.Fatal(err)
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			ref, ok := decodeEPaxosRecordKey(iter.Key())
			if !ok {
				_ = iter.Close()
				t.Fatalf("replica %d retained malformed protocol key", id)
			}
			if executionFrontierCovers(checkpoint.CompactedThrough, ref) {
				_ = iter.Close()
				t.Fatalf("replica %d retained compacted protocol row %s", id, ref)
			}
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			t.Fatal(err)
		}
		if err := iter.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
	cluster = nil
	restarted, err := openCluster(paths, newNode)
	if err != nil {
		t.Fatal(err)
	}
	cluster = restarted
	if err := cluster.Drain(); err != nil {
		t.Fatal(err)
	}
	for _, id := range cluster.ids {
		value, ok, err := cluster.Get(id, []byte("m"))
		if err != nil || !ok || !bytes.Equal(value, []byte("phantom")) {
			t.Fatalf("replica %d restored m=%q ok=%t err=%v", id, value, ok, err)
		}
		response, found, err := cluster.DBs[id].CommandResult(scan.ID)
		if err != nil || !found {
			t.Fatalf("replica %d restored scan result found=%t err=%v", id, found, err)
		}
		result, err := DecodeOrderedScanResult(response)
		if err != nil || len(result.Rows) != 0 {
			t.Fatalf("replica %d restored pre-insert scan=%#v err=%v", id, result, err)
		}
	}

	late := epaxos.Message{
		Type: epaxos.MsgPrepare, From: 2, To: 1, FromIncarnation: 1, ToIncarnation: 1,
		Ref:    epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot: epaxos.Ballot{Number: 1, Replica: 2},
	}
	late.Checksum = epaxos.ChecksumMessage(late)
	if err := cluster.Nodes[1].Step(late); !errors.Is(err, epaxos.ErrMessageRejected) {
		t.Fatalf("late compacted message err=%v, want rejected with checkpoint offer", err)
	}
	ready := cluster.Nodes[1].Ready()
	if len(ready.RecordLoads) != 0 {
		t.Fatalf("late compacted message requested deleted records: %v", ready.RecordLoads)
	}
	sawOffer := false
	for _, message := range ready.Messages {
		sawOffer = sawOffer || message.Type == epaxos.MsgCheckpointOffer
	}
	if !sawOffer {
		t.Fatal("late compacted message did not produce a certified checkpoint offer")
	}
	if err := cluster.Drain(); err != nil {
		t.Fatal(err)
	}
	wrongIncarnation := late
	wrongIncarnation.FromIncarnation++
	wrongIncarnation.Checksum = epaxos.ChecksumMessage(wrongIncarnation)
	if err := cluster.Nodes[1].Step(wrongIncarnation); !errors.Is(err, epaxos.ErrMessageRejected) {
		t.Fatalf("wrong-incarnation message err=%v, want message rejection", err)
	}
}

func TestApplicationSnapshotTransferRejectsMalformedContent(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := ApplicationSnapshotBundleSize([]byte("short")); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
		t.Fatalf("short handle size err=%v, want invalid checkpoint", err)
	}
	validBundle := encodeApplicationSnapshot([][2][]byte{{{recordPrefix, 'k'}, []byte("value")}})
	validDigest := digestApplicationSnapshot(validBundle)
	validHandle := encodeApplicationSnapshotHandle(validDigest, uint64(len(validBundle)))
	if size, err := ApplicationSnapshotBundleSize(validHandle); err != nil || size != uint64(len(validBundle)) {
		t.Fatalf("bundle size=%d err=%v, want %d", size, err, len(validBundle))
	}
	if _, err := db.ApplicationSnapshotBundle([]byte("short")); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
		t.Fatalf("short bundle handle err=%v, want invalid checkpoint", err)
	}
	if _, err := db.ApplicationSnapshotBundle(validHandle); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("missing bundle err=%v, want not found", err)
	}
	if err := db.MaterializeApplicationSnapshot([]byte("short"), validBundle); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
		t.Fatalf("short materialization handle err=%v, want invalid checkpoint", err)
	}
	if err := db.MaterializeApplicationSnapshot(validHandle, append(bytes.Clone(validBundle), 0)); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("wrong-size materialization err=%v, want checksum mismatch", err)
	}

	malformedBundles := [][]byte{
		nil,
		{epaxosApplicationSnapshotCodec + 1, 0},
		append([]byte{epaxosApplicationSnapshotCodec}, binary.AppendUvarint(nil, 100)...),
		encodeApplicationSnapshot([][2][]byte{{{0xff}, nil}}),
		encodeApplicationSnapshot([][2][]byte{{{recordPrefix, 'b'}, nil}, {{recordPrefix, 'b'}, nil}}),
		append(encodeApplicationSnapshot(nil), 0),
		{epaxosApplicationSnapshotCodec, 1, 1},
	}
	for index, bundle := range malformedBundles {
		if _, err := decodeApplicationSnapshot(bundle); err == nil {
			t.Fatalf("malformed bundle %d decoded", index)
		}
	}
	malformed := malformedBundles[3]
	malformedDigest := digestApplicationSnapshot(malformed)
	malformedHandle := encodeApplicationSnapshotHandle(malformedDigest, uint64(len(malformed)))
	if err := db.MaterializeApplicationSnapshot(malformedHandle, malformed); err == nil {
		t.Fatal("materialized authenticated but malformed application snapshot")
	}
	if err := db.pebble.Set(applicationSnapshotKey(malformedHandle), malformed, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ApplicationSnapshotBundle(malformedHandle); err == nil {
		t.Fatal("served authenticated but malformed application snapshot")
	}

	if err := db.MaterializeApplicationSnapshot(validHandle, validBundle); err != nil {
		t.Fatal(err)
	}
	loaded, err := db.ApplicationSnapshotBundle(validHandle)
	if err != nil || !bytes.Equal(loaded, validBundle) {
		t.Fatalf("loaded bundle=%x err=%v, want %x", loaded, err, validBundle)
	}
	corrupt := bytes.Clone(validBundle)
	corrupt[len(corrupt)-1] ^= 1
	if err := db.pebble.Set(applicationSnapshotKey(validHandle), corrupt, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ApplicationSnapshotBundle(validHandle); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("corrupt stored bundle err=%v, want checksum mismatch", err)
	}
}

func TestApplicationSnapshotReadyInstallValidatesProtocolAndApplicationState(t *testing.T) {
	source, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	command := CommandForPut(91, 1, []byte("snapshot-ready"), []byte("before"))
	if err := source.ApplyReady(epaxos.Ready{Apply: []epaxos.ApplyCommand{{Command: command}}}); err != nil {
		t.Fatal(err)
	}
	var checkpointID epaxos.CheckpointID
	checkpointID[0] = 9
	result, err := source.CreateApplicationCheckpoint(epaxos.CheckpointRequest{ID: checkpointID})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := source.ApplicationSnapshotBundle(result.ApplicationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := certifiedKVCheckpoint(t, result, 1)

	if _, err := source.validateApplicationSnapshot(epaxos.Checkpoint{}); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
		t.Fatalf("empty checkpoint validation err=%v, want invalid checkpoint", err)
	}
	mismatched := checkpoint.Clone()
	mismatched.Descriptor.ApplicationDigest[0] ^= 1
	if _, err := source.validateApplicationSnapshot(mismatched); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
		t.Fatalf("mismatched checkpoint validation err=%v, want invalid checkpoint", err)
	}

	target, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	if err := target.ApplyReady(epaxos.Ready{Snapshot: &epaxos.Snapshot{Mode: 99, Checkpoint: checkpoint}}); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
		t.Fatalf("unknown snapshot mode err=%v, want invalid checkpoint", err)
	}
	if err := target.ApplyReady(epaxos.Ready{Snapshot: &epaxos.Snapshot{Mode: epaxos.SnapshotInstall, Checkpoint: checkpoint}}); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("unmaterialized install err=%v, want not found", err)
	}
	if err := target.MaterializeApplicationSnapshot(result.ApplicationSnapshot, bundle); err != nil {
		t.Fatal(err)
	}
	if err := target.ApplyReady(epaxos.Ready{Snapshot: &epaxos.Snapshot{Mode: epaxos.SnapshotInstall, Checkpoint: checkpoint}}); err != nil {
		t.Fatal(err)
	}
	value, found, err := target.Get([]byte("snapshot-ready"))
	if err != nil || !found || !bytes.Equal(value, []byte("before")) {
		t.Fatalf("installed value=%q found=%t err=%v, want before", value, found, err)
	}
}

func TestCheckpointStorageRejectsInvalidCertificatesAndCompaction(t *testing.T) {
	newDB := func(t *testing.T) *DB {
		t.Helper()
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	result := epaxos.CheckpointResult{ID: epaxos.CheckpointID{7}, ApplicationDigest: epaxos.StateDigest{8}}
	checkpoint := certifiedKVCheckpoint(t, result, 1)
	snapshot := epaxos.Snapshot{Mode: epaxos.SnapshotPersistLocal, Checkpoint: checkpoint}
	compaction := []epaxos.CompactionRange{{
		Checkpoint: checkpoint.Descriptor.ID,
		Conf:       1,
		Replica:    1,
		Through:    1,
	}}

	t.Run("invalid snapshot checksum", func(t *testing.T) {
		db := newDB(t)
		invalid := snapshot.Clone()
		invalid.Checkpoint.Checksum[0] ^= 1
		if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Snapshot: &invalid}); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
			t.Fatalf("invalid snapshot err=%v, want invalid checkpoint", err)
		}
	})
	t.Run("compaction requires checkpoint", func(t *testing.T) {
		db := newDB(t)
		if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Compact: compaction}); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
			t.Fatalf("uncertified compaction err=%v, want invalid checkpoint", err)
		}
	})
	t.Run("exact certified compaction", func(t *testing.T) {
		db := newDB(t)
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 1},
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForPut(92, 1, []byte("compacted"), []byte("value")),
		})
		if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{
			Records:  []epaxos.InstanceRecord{record},
			Snapshot: &snapshot,
			Compact:  compaction,
		}); err != nil {
			t.Fatal(err)
		}
		stored, err := db.EPaxosStorage().LoadCheckpoint()
		if err != nil || stored.CompactedThrough.Configs[0].Lanes[0].Through != 1 {
			t.Fatalf("stored checkpoint=%#v err=%v", stored, err)
		}
		if _, found, err := db.LoadInstance(record.Ref); err != nil || found {
			t.Fatalf("compacted record found=%t err=%v", found, err)
		}
		if got := executionFrontierThrough(stored.CompactedThrough, 2, 1); got != 0 {
			t.Fatalf("wrong-configuration frontier=%d, want zero", got)
		}
		if got := executionFrontierThrough(stored.CompactedThrough, 1, 2); got != 0 {
			t.Fatalf("wrong-replica frontier=%d, want zero", got)
		}
	})
	t.Run("range mismatch", func(t *testing.T) {
		db := newDB(t)
		if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Snapshot: &snapshot}); err != nil {
			t.Fatal(err)
		}
		batch := db.pebble.NewBatch()
		defer func() { _ = batch.Close() }()
		if _, err := stageEPaxosCheckpoint(batch, db.pebble, epaxos.Ready{
			Compact: append(compaction, compaction[0]),
		}); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
			t.Fatalf("wrong compaction count err=%v, want invalid checkpoint", err)
		}
		wrong := append([]epaxos.CompactionRange(nil), compaction...)
		wrong[0].Through++
		if _, err := stageEPaxosCheckpoint(batch, db.pebble, epaxos.Ready{Compact: wrong}); !errors.Is(err, epaxos.ErrInvalidCheckpoint) {
			t.Fatalf("wrong compaction range err=%v, want invalid checkpoint", err)
		}
		if _, err := stageEPaxosCheckpoint(txnBatchWithSetErr{err: errors.New("unused")}, db.pebble, epaxos.Ready{Compact: compaction}); err == nil {
			t.Fatal("compaction accepted a batch without Delete")
		}
		deleteErr := errors.New("delete checkpoint record")
		if _, err := stageEPaxosCheckpoint(checkpointDeleteErrorBatch{err: deleteErr}, db.pebble, epaxos.Ready{Compact: compaction}); !errors.Is(err, deleteErr) {
			t.Fatalf("delete failure err=%v, want injected error", err)
		}
	})
	t.Run("checkpoint codec", func(t *testing.T) {
		encoded, err := encodeEPaxosCheckpoint(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		for index, malformed := range [][]byte{
			nil,
			{epaxosCheckpointCodec, '{'},
			append([]byte{epaxosCheckpointCodec}, []byte("{}")...),
			append(bytes.Clone(encoded), ' '),
		} {
			if _, err := decodeEPaxosCheckpoint(malformed); err == nil {
				t.Fatalf("malformed checkpoint %d decoded", index)
			}
		}
		decoded, err := decodeEPaxosCheckpoint(encoded)
		if err != nil || decoded.Checksum != checkpoint.Checksum {
			t.Fatalf("decoded checkpoint=%#v err=%v", decoded, err)
		}
		db := newDB(t)
		if err := db.pebble.Set(epaxosCheckpointKey, []byte{epaxosCheckpointCodec, '{'}, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := db.EPaxosStorage().LoadCheckpoint(); err == nil {
			t.Fatal("loaded malformed durable checkpoint")
		}
	})
	t.Run("frontier overflow", func(t *testing.T) {
		db := newDB(t)
		record := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 1},
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForDelete(93, 1, []byte("overflow")),
		})
		if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{record}}); err != nil {
			t.Fatal(err)
		}
		frontier := epaxos.ExecutionFrontier{Configs: []epaxos.ExecutionConfigFrontier{{
			Conf: 1,
			Lanes: []epaxos.ExecutionLaneFrontier{{
				Replica: 1,
				Through: ^epaxos.InstanceNum(0),
			}},
		}}}
		if err := db.EPaxosStorage().LoadInstances(frontier, func(epaxos.InstanceRecord) error { return nil }); err == nil {
			t.Fatal("maximum compacted frontier did not reject successor overflow")
		}
	})
}

func TestCommandResultCodecRejectsNoncanonicalLengths(t *testing.T) {
	var digest epaxos.StateDigest
	valid := encodeEPaxosCommandResult(digest, []byte("x"))
	overflow := append(make([]byte, 33), bytes.Repeat([]byte{0x80}, binary.MaxVarintLen64)...)
	overflow[0] = epaxosCommandResultCodec
	noncanonical := append(make([]byte, 33), 0x81, 0x00, 'x')
	noncanonical[0] = epaxosCommandResultCodec
	for index, encoded := range [][]byte{
		nil,
		overflow,
		valid[:len(valid)-1],
		noncanonical,
	} {
		if _, _, err := decodeEPaxosCommandResult(encoded); err == nil {
			t.Fatalf("malformed command result %d decoded", index)
		}
	}
	gotDigest, response, err := decodeEPaxosCommandResult(valid)
	if err != nil || gotDigest != digest || !bytes.Equal(response, []byte("x")) {
		t.Fatalf("decoded digest=%x response=%q err=%v", gotDigest, response, err)
	}
}

func certifiedKVCheckpoint(t *testing.T, result epaxos.CheckpointResult, through epaxos.InstanceNum) epaxos.Checkpoint {
	t.Helper()
	identity := epaxos.VoterIdentity{Replica: 1, Incarnation: 1}
	descriptor := epaxos.CheckpointDescriptor{
		ID:                result.ID,
		Conf:              epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}},
		Voters:            []epaxos.VoterIdentity{identity},
		Through:           epaxos.ExecutionFrontier{Configs: []epaxos.ExecutionConfigFrontier{{Conf: 1, Lanes: []epaxos.ExecutionLaneFrontier{{Replica: 1, Through: through}}}}},
		ApplicationDigest: result.ApplicationDigest,
	}
	digest := epaxos.DigestCheckpointDescriptor(descriptor)
	certificate, err := epaxos.BuildCheckpointCertificate(descriptor, []epaxos.VoterAcknowledgement{{
		Voter: identity, AttestedDigest: digest,
	}})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := epaxos.Checkpoint{
		Descriptor:          descriptor,
		Certificate:         certificate,
		ApplicationSnapshot: bytes.Clone(result.ApplicationSnapshot),
	}
	checkpoint.Checksum = epaxos.DigestCheckpoint(checkpoint)
	return checkpoint
}

type checkpointDeleteErrorBatch struct {
	err error
}

func (b checkpointDeleteErrorBatch) Set(_, _ []byte, _ *pebble.WriteOptions) error {
	return nil
}

func (b checkpointDeleteErrorBatch) Delete(_ []byte, _ *pebble.WriteOptions) error {
	return b.err
}

func (checkpointDeleteErrorBatch) Commit(*pebble.WriteOptions) error {
	return nil
}

func (checkpointDeleteErrorBatch) Close() error {
	return nil
}

func TestCheckpointStorageDetectsCorruptKeysValuesAndLegacyRecords(t *testing.T) {
	newDB := func(t *testing.T) *DB {
		t.Helper()
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	t.Run("missing and malformed instance", func(t *testing.T) {
		db := newDB(t)
		ref := epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 1}
		if _, found, err := db.LoadInstance(ref); err != nil || found {
			t.Fatalf("missing instance found=%t err=%v", found, err)
		}
		if err := db.pebble.Set(epaxosRecordKey(ref), []byte{0xff}, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.LoadInstance(ref); err == nil {
			t.Fatal("malformed instance value loaded")
		}
	})
	t.Run("instance key and value reference mismatch", func(t *testing.T) {
		db := newDB(t)
		keyRef := epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 1}
		valueRecord := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 2},
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForDelete(94, 1, []byte("mismatch")),
		})
		if err := db.pebble.Set(epaxosRecordKey(keyRef), encodeEPaxosRecord(valueRecord), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.LoadInstance(keyRef); err == nil {
			t.Fatal("instance key/value mismatch loaded")
		}
	})
	t.Run("malformed record key", func(t *testing.T) {
		db := newDB(t)
		key := []byte{epaxosStorePrefix, epaxosRecordEntry, 1}
		if err := db.pebble.Set(key, []byte{1}, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err := db.EPaxosStorage().LoadInstances(epaxos.ExecutionFrontier{}, func(epaxos.InstanceRecord) error { return nil }); err == nil {
			t.Fatal("malformed durable record key loaded")
		}
	})
	t.Run("record key and value reference mismatch", func(t *testing.T) {
		db := newDB(t)
		keyRef := epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 1}
		valueRecord := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Conf: 1, Replica: 1, Instance: 2},
			Ballot:  epaxos.Ballot{Replica: 1},
			Status:  epaxos.StatusCommitted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: CommandForDelete(95, 1, []byte("load-mismatch")),
		})
		if err := db.pebble.Set(epaxosRecordKey(keyRef), encodeEPaxosRecord(valueRecord), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err := db.EPaxosStorage().LoadInstances(epaxos.ExecutionFrontier{}, func(epaxos.InstanceRecord) error { return nil }); err == nil {
			t.Fatal("durable record key/value mismatch loaded")
		}
	})
	t.Run("legacy record migration", func(t *testing.T) {
		db := newDB(t)
		record := timingCodecRecordKV(8, epaxos.TimingDomainLogical, 9, false)
		record.Checksum = epaxos.ChecksumRecordWithoutMembershipResult(record)
		if err := db.pebble.Set(epaxosRecordKey(record.Ref), encodeHistoricalEPaxosRecordKV(record, 8), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		var loaded []epaxos.InstanceRecord
		if err := db.EPaxosStorage().LoadInstances(epaxos.ExecutionFrontier{}, func(record epaxos.InstanceRecord) error {
			loaded = append(loaded, record)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 1 {
			t.Fatalf("loaded legacy records=%d, want one", len(loaded))
		}
		value, closer, err := db.pebble.Get(epaxosRecordKey(record.Ref))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = closer.Close() }()
		if len(value) == 0 || value[0] != epaxosRecordCodec {
			t.Fatalf("migrated record codec=%x, want %x", value, epaxosRecordCodec)
		}
	})
	t.Run("command result key and value validation", func(t *testing.T) {
		if _, ok := decodeEPaxosCommandResultKey([]byte{epaxosStorePrefix, epaxosCommandResultEntry}); ok {
			t.Fatal("short command result key decoded")
		}

		db := newDB(t)
		malformedKey := append(epaxosCommandResultClientPrefix(1), 1)
		if err := db.pebble.Set(malformedKey, []byte{1}, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := db.NextCommandSequence(1); err == nil {
			t.Fatal("malformed command result key scanned")
		}

		db = newDB(t)
		id := epaxos.CommandID{Client: 2, Sequence: 1}
		if err := db.pebble.Set(epaxosCommandResultKey(id), []byte{1}, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := db.NextCommandSequence(2); err == nil {
			t.Fatal("malformed command result value scanned")
		}

		db = newDB(t)
		maxID := epaxos.CommandID{Client: 3, Sequence: ^uint64(0)}
		if err := db.pebble.Set(epaxosCommandResultKey(maxID), encodeEPaxosCommandResult(epaxos.StateDigest{}, nil), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := db.NextCommandSequence(3); err == nil {
			t.Fatal("exhausted command sequence advanced")
		}
	})
	if err := validateEPaxosHardState(epaxos.HardState{}); err != nil {
		t.Fatalf("empty hard state err=%v", err)
	}
	if sameEPaxosVoters([]epaxos.ReplicaID{1}, []epaxos.ReplicaID{1, 2}) {
		t.Fatal("different voter lengths compare equal")
	}
	for index, encoded := range [][]byte{nil, {epaxosHardStateCodec}} {
		if _, err := decodeEPaxosHardState(encoded); err == nil {
			t.Fatalf("short hard state %d decoded", index)
		}
	}
	if _, err := decodeEPaxosRecord([]byte{epaxosRecordCodec, '{'}); err == nil {
		t.Fatal("malformed current record decoded")
	}
}
