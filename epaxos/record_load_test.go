package epaxos

import (
	"errors"
	"fmt"
	"testing"
)

func provideRecordLoadsFromStore(node *RawNode, store *MemoryStorage, rd Ready) error {
	for _, ref := range rd.RecordLoads {
		rec, found := store.Instance(ref)
		if err := node.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: rec, Found: found}); err != nil {
			residentStatus := StatusNone
			residentChecksum := [32]byte{}
			if inst := node.instances[ref]; inst != nil {
				residentStatus = inst.rec.Status
				residentChecksum = inst.rec.Checksum
			}
			return fmt.Errorf("provide record load %s (found=%v stored=%s/%x resident=%s/%x): %w", ref, found, rec.Status, rec.Checksum, residentStatus, residentChecksum, err)
		}
	}
	return nil
}

func payloadStubTestNode(t *testing.T) (*RawNode, InstanceRef, InstanceRecord) {
	t.Helper()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetainExecutedPerLane: 8})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Ballot: Ballot{Replica: 2}, RecordBallot: Ballot{Replica: 2},
		Status: StatusExecuted, Seq: 3, Deps: rn.q.deps(),
		Command: Command{ID: CommandID{Client: 7, Sequence: 1}, Payload: []byte("chosen"), Footprint: Footprint{Points: [][]byte{[]byte("k")}}},
	})
	rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
	rn.executed.add(ref)
	rn.durableExecuted.add(ref)
	rn.retireExecuted()
	if inst := rn.instances[ref]; inst == nil || !inst.payloadAbsent {
		t.Fatalf("resident instance = %#v, want payload stub", inst)
	}
	return rn, ref, rec
}

func payloadStubAccept(rec InstanceRecord) Message {
	return Message{
		Type: MsgAccept, From: rec.Ref.Replica, To: 1, Ref: rec.Ref,
		Ballot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps,
		AcceptSeq: rec.AcceptSeq, AcceptDeps: rec.AcceptDeps,
		AcceptEvidence: rec.AcceptEvidence, Command: rec.Command,
	}
}

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
		Deps: rn.q.deps(), Command: Command{Payload: []byte("x"), Footprint: Footprint{Points: [][]byte{[]byte("k")}}},
	})
	// Mark folded without resident.
	foldTestRef(&rn.engine, rec)

	prep := Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}}
	if err := rn.Step(canonicalTestMessage(prep)); err != nil { t.Fatal(err)
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
	if err := rn.Step(canonicalTestMessage(Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}})); err != nil { t.Fatal(err)
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
	if err := rn.Step(canonicalTestMessage(Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}})); err != nil { t.Fatal(err)
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
	if err := rn.Step(canonicalTestMessage(Message{Type: MsgPrepare, From: 2, To: 1, Ref: r1, Ballot: Ballot{Number: 1, Replica: 2}})); err != nil { t.Fatal(err)
 }
	err = rn.Step(canonicalTestMessage(Message{Type: MsgPrepare, From: 2, To: 1, Ref: r2, Ballot: Ballot{Number: 1, Replica: 2}}))
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
		if err := rn.Step(canonicalTestMessage(Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: uint64(i + 1), Replica: 2}})); err != nil { t.Fatal(err)
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

func TestPayloadStubRequiredLoadRejectsMismatchThenReplays(t *testing.T) {
	rn, ref, rec := payloadStubTestNode(t)
	if err := rn.Step(canonicalTestMessage(payloadStubAccept(rec))); err != nil { t.Fatal(err)
 }
	rd := rn.Ready()
	if len(rd.RecordLoads) != 1 || rd.RecordLoads[0] != ref {
		t.Fatalf("RecordLoads=%v, want [%s]", rd.RecordLoads, ref)
	}

	wrong := rec.Clone()
	wrong.Command.Payload = []byte("stale")
	wrong.Checksum = ChecksumRecord(wrong)
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: wrong, Found: true}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("mismatched restore error=%v, want ErrInvalidRecord", err)
	}
	if wait := rn.pendingRecordLoads[ref]; wait == nil || len(wait.messages) != 1 {
		t.Fatalf("mismatched restore consumed wait: %#v", wait)
	}

	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: rec, Found: true}); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
	out := rn.Ready()
	if len(out.Messages) != 1 || out.Messages[0].Type != MsgCommit || !commandEqual(out.Messages[0].Command, rec.Command) {
		t.Fatalf("replayed Accept output=%#v, want full chosen Commit", out.Messages)
	}
	if inst := rn.instances[ref]; inst == nil || !inst.payloadAbsent || inst.rec.Command.Payload != nil {
		t.Fatalf("restored resident not re-stubbed: %#v", inst)
	}
	if got := rn.RuntimeStats().PayloadStubInstances; got != 1 {
		t.Fatalf("payload stub gauge=%d, want 1", got)
	}
}

func TestPayloadStubRequiredMissKeepsWait(t *testing.T) {
	rn, ref, rec := payloadStubTestNode(t)
	if err := rn.Step(canonicalTestMessage(payloadStubAccept(rec))); err != nil { t.Fatal(err)
 }
	rd := rn.Ready()
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Found: false}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("required miss error=%v, want ErrInvalidRecord", err)
	}
	if wait := rn.pendingRecordLoads[ref]; wait == nil || !wait.required || len(wait.messages) != 1 {
		t.Fatalf("required miss consumed wait: %#v", wait)
	}
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: rec, Found: true}); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
	if out := rn.Ready(); len(out.Messages) != 1 || out.Messages[0].Type != MsgCommit {
		t.Fatalf("corrected load output=%#v, want one Commit", out.Messages)
	}
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: rec, Found: true}); !errors.Is(err, ErrUnrequestedRecordLoad) {
		t.Fatalf("duplicate provide error=%v, want ErrUnrequestedRecordLoad", err)
	}
}

func TestPayloadStubRestoreExecutesPendingExports(t *testing.T) {
	rn, ref, rec := payloadStubTestNode(t)
	rn.broadcast(MsgCommit, rn.instances[ref].rec)
	rn.enqueueRecord(rn.instances[ref].rec)
	rd := rn.Ready()
	if len(rd.RecordLoads) != 1 || rd.RecordLoads[0] != ref {
		t.Fatalf("RecordLoads=%v, want one deduplicated request", rd.RecordLoads)
	}
	if err := rn.ProvideRecordLoad(RecordLoadResult{Ref: ref, Record: rec, Found: true}); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
	out := rn.Ready()
	if len(out.Records) != 1 || !commandEqual(out.Records[0].Command, rec.Command) {
		t.Fatalf("restored Ready.Records=%#v, want full durable record", out.Records)
	}
	if len(out.Messages) != 2 {
		t.Fatalf("restored broadcast messages=%#v, want two remote voters", out.Messages)
	}
	for _, message := range out.Messages {
		if message.Type != MsgCommit || !commandEqual(message.Command, rec.Command) {
			t.Fatalf("restored broadcast message=%#v, want full Commit", message)
		}
	}
	if inst := rn.instances[ref]; inst == nil || !inst.payloadAbsent {
		t.Fatalf("export restore did not re-stub resident: %#v", inst)
	}
}
