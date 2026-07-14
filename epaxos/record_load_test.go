package epaxos

import (
	"errors"
	"testing"
)

func foldTestRef(engine *conflictEngine, rec InstanceRecord) {
	// Contiguous fold watermark requires every instance in (folded, through].
	lane := laneFor(rec.Ref)
	for instance := InstanceNum(1); instance <= rec.Ref.Instance; instance++ {
		r := rec
		r.Ref.Instance = instance
		r.Seq = uint64(instance)
		r.Checksum = ChecksumRecord(r)
		if instance == rec.Ref.Instance {
			r = rec
		}
		engine.apply(nil, r)
		engine.foldRecord(r)
	}
	engine.advanceFold(lane, rec.Ref.Instance)
}

func TestRecordLoadFoundReplaysDeferred(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 5}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Status: StatusCommitted, Seq: 3, Ballot: Ballot{Replica: 2},
		Deps: rn.q.deps(), Command: Command{Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("k")}},
	})
	// Mark folded without resident.
	foldTestRef(&rn.engine, rec)

	prep := Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}}
	if err := rn.Step(prep); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.RecordLoads) != 1 || rd.RecordLoads[0] != ref {
		t.Fatalf("RecordLoads=%v, want [%s]", rd.RecordLoads, ref)
	}
	// frozen stable
	rd2 := rn.Ready()
	if len(rd2.RecordLoads) != 1 || rd2.RecordLoads[0] != ref {
		t.Fatalf("frozen RecordLoads changed: %v", rd2.RecordLoads)
	}
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: rec, Found: true}); err != nil {
		t.Fatal(err)
	}
	if rn.instances[ref] == nil {
		t.Fatal("expected installed instance after ProvideRecordLoad")
	}
	if len(rn.pendingRecordLoads) != 0 {
		t.Fatalf("pending not cleared: %#v", rn.pendingRecordLoads)
	}
}

func TestRecordLoadNotFoundIncrementsMisses(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 7}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Status: StatusCommitted, Seq: 1, Ballot: Ballot{Replica: 2},
		Deps: rn.q.deps(), Command: Command{Payload: []byte("y")},
	})
	foldTestRef(&rn.engine, rec)
	if err := rn.Step(Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Found: false}); err != nil {
		t.Fatal(err)
	}
	if rn.RuntimeStats().RecordLoadMisses != 1 {
		t.Fatalf("misses=%d", rn.RuntimeStats().RecordLoadMisses)
	}
	if rn.instances[ref] != nil {
		t.Fatal("unexpected install on miss")
	}
}

func TestRecordLoadCorruptLeavesPending(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 9}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Status: StatusCommitted, Seq: 1, Ballot: Ballot{Replica: 2},
		Deps: rn.q.deps(), Command: Command{Payload: []byte("z")},
	})
	foldTestRef(&rn.engine, rec)
	if err := rn.Step(Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}}); err != nil {
		t.Fatal(err)
	}
	bad := rec
	bad.Checksum[0] ^= 0xff
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: bad, Found: true}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("err=%v, want ErrInvalidRecord", err)
	}
	if len(rn.pendingRecordLoads) != 1 {
		t.Fatalf("pending cleared on corrupt: %#v", rn.pendingRecordLoads)
	}
	// correct provide succeeds
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: rec, Found: true}); err != nil {
		t.Fatal(err)
	}
}

func TestRecordLoadUnrequested(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Found: false}); !errors.Is(err, ErrUnrequestedRecordLoad) {
		t.Fatalf("err=%v", err)
	}
}

func TestRecordLoadCapacityRejectsStep(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), MaxDeferredRecordLoads: 1})
	if err != nil {
		t.Fatal(err)
	}
	r1 := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	r2 := InstanceRef{Conf: 1, Replica: 2, Instance: 2}
	for _, ref := range []InstanceRef{r1, r2} {
		rec := checkedRecord(InstanceRecord{
			Ref: ref, Status: StatusCommitted, Seq: 1, Ballot: Ballot{Replica: 2},
			Deps: rn.q.deps(), Command: Command{Payload: []byte("c")},
		})
		foldTestRef(&rn.engine, rec)
	}
	if err := rn.Step(Message{Type: MsgPrepare, From: 2, To: 1, Ref: r1, Ballot: Ballot{Number: 1, Replica: 2}}); err != nil {
		t.Fatal(err)
	}
	err = rn.Step(Message{Type: MsgPrepare, From: 2, To: 1, Ref: r2, Ballot: Ballot{Number: 1, Replica: 2}})
	if !errors.Is(err, ErrDeferredRecordLoadFull) || !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("err=%v, want capacity rejection", err)
	}
}

func TestRecordLoadDedupSingleRequest(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 3}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Status: StatusCommitted, Seq: 1, Ballot: Ballot{Replica: 2},
		Deps: rn.q.deps(), Command: Command{Payload: []byte("d")},
	})
	foldTestRef(&rn.engine, rec)
	for i := 0; i < 3; i++ {
		if err := rn.Step(Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: uint64(i + 1), Replica: 2}}); err != nil {
			t.Fatal(err)
		}
	}
	rd := rn.Ready()
	if len(rd.RecordLoads) != 1 {
		t.Fatalf("RecordLoads=%v", rd.RecordLoads)
	}
	if got := len(rn.pendingRecordLoads[ref].messages); got != 3 {
		t.Fatalf("deferred messages=%d, want 3", got)
	}
}
