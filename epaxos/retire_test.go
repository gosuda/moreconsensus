package epaxos

import (
	"errors"
	"testing"
)

func TestExecutedTrackerContainsFoldedPrefix(t *testing.T) {
	var tr executedTracker
	lane := instanceLane{conf: 1, replica: 1}
	for i := InstanceNum(1); i <= 3; i++ {
		tr.add(InstanceRef{Conf: 1, Replica: 1, Instance: i})
	}
	if tr.prefix(lane) != 3 {
		t.Fatalf("prefix=%d", tr.prefix(lane))
	}
	tr.forgetExactThrough(lane, 2)
	if !tr.contains(InstanceRef{Conf: 1, Replica: 1, Instance: 2}) {
		t.Fatal("expected contains via through after exact cleanup")
	}
	if !tr.contains(InstanceRef{Conf: 1, Replica: 1, Instance: 3}) {
		t.Fatal("expected contains for exact remaining")
	}
}

func TestRetireFoldsBeyondRetention(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetainExecutedPerLane: 1})
	if err != nil {
		t.Fatal(err)
	}
	lane := instanceLane{conf: 1, replica: 1}
	for i := InstanceNum(1); i <= 5; i++ {
		ref := InstanceRef{Conf: 1, Replica: 1, Instance: i}
		rec := checkedRecord(InstanceRecord{
			Ref: ref, Status: StatusExecuted, Seq: uint64(i), Ballot: Ballot{Replica: 1},
			Deps: rn.q.deps(), Command: Command{Payload: []byte("p"), Footprint: Footprint{Points: [][]byte{[]byte("k")}}},
		})
		rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
		rn.executed.add(ref)
		rn.durableExecuted.add(ref)
	}
	rn.retireExecuted()
	// retain 1 => fold through 4
	if got := rn.engine.foldedThrough(lane); got != 4 {
		t.Fatalf("foldedThrough=%d, want 4", got)
	}
	if rn.instances[InstanceRef{Conf: 1, Replica: 1, Instance: 5}] == nil {
		t.Fatal("expected retention tail resident")
	}
	if rn.instances[InstanceRef{Conf: 1, Replica: 1, Instance: 4}] != nil {
		t.Fatal("expected instance 4 folded")
	}
	if !rn.executed.contains(InstanceRef{Conf: 1, Replica: 1, Instance: 3}) {
		t.Fatal("executed contains must remain true for folded prefix")
	}
}

func TestProposeBackpressureMaxResident(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), MaxResidentInstances: 1})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Status: StatusCommitted, Seq: 1, Ballot: Ballot{Replica: 1},
		Deps: rn.q.deps(), Command: Command{Payload: []byte("x"), Footprint: Footprint{Points: [][]byte{[]byte("k")}}},
	})
	rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
	if _, err := rn.Propose(Command{Payload: []byte("y"), Footprint: Footprint{Points: [][]byte{[]byte("k")}}}); !errors.Is(err, ErrResidentInstancesExceeded) {
		t.Fatalf("err=%v", err)
	}
}

func TestRetirePayloadDropPreservesChecksum(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetainExecutedPerLane: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Status: StatusExecuted, Seq: 1, Ballot: Ballot{Replica: 1},
		Deps: rn.q.deps(), Command: Command{Payload: []byte("my-payload"), Footprint: Footprint{Points: [][]byte{[]byte("k")}}},
	})
	rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
	rn.executed.add(ref)
	rn.durableExecuted.add(ref)

	origChecksum := rec.Checksum
	if origChecksum == ([32]byte{}) {
		t.Fatal("expected non-zero original checksum")
	}

	rn.retireExecuted()

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatal("expected instance to remain resident")
	}
	if !inst.payloadAbsent {
		t.Fatal("expected payload to be dropped")
	}
	if inst.rec.Command.Payload != nil {
		t.Fatal("expected Command.Payload to be nil")
	}
	if !VerifyRecordChecksum(inst.rec) {
		t.Fatalf("resident payload stub has invalid checksum: %#v", inst.rec)
	}
	if inst.payloadChecksum != origChecksum {
		t.Fatalf("full checksum authority mutated: got %x, want %x", inst.payloadChecksum, origChecksum)
	}
}

func TestRetirePayloadStubGaugeAndEmptyCapacity(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetainExecutedPerLane: 8})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 0, 1024)
	for i, commandPayload := range [][]byte{[]byte("full"), payload} {
		ref := InstanceRef{Conf: 1, Replica: 1, Instance: InstanceNum(i + 1)}
		rec := checkedRecord(InstanceRecord{
			Ref: ref, Status: StatusExecuted, Seq: uint64(i + 1), Ballot: Ballot{Replica: 1},
			Deps: rn.q.deps(), Command: Command{Payload: commandPayload, Footprint: Footprint{Points: [][]byte{[]byte("k")}}},
		})
		rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
		rn.executed.add(ref)
		rn.durableExecuted.add(ref)
	}

	rn.retireExecuted()
	full := rn.instances[InstanceRef{Conf: 1, Replica: 1, Instance: 1}]
	empty := rn.instances[InstanceRef{Conf: 1, Replica: 1, Instance: 2}]
	if full == nil || !full.payloadAbsent {
		t.Fatalf("full payload resident=%#v, want stub", full)
	}
	if empty == nil || empty.payloadAbsent || empty.rec.Command.Payload != nil {
		t.Fatalf("empty payload resident=%#v, want nil non-stub payload", empty)
	}
	if got := rn.RuntimeStats().PayloadStubInstances; got != 1 {
		t.Fatalf("payload stub gauge=%d, want 1", got)
	}

	rn.retainExecutedPerLane = 0
	rn.retireExecuted()
	if got := rn.RuntimeStats().PayloadStubInstances; got != 0 {
		t.Fatalf("payload stub gauge after fold=%d, want 0", got)
	}
	if len(rn.instances) != 0 {
		t.Fatalf("resident instances after fold=%#v, want none", rn.instances)
	}
}
