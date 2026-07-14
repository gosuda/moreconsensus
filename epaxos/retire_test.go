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
			Deps: rn.q.deps(), Command: Command{Payload: []byte("p"), ConflictKeys: [][]byte{[]byte("k")}},
		})
		rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
		rn.executed.add(ref)
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
		Deps: rn.q.deps(), Command: Command{Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("k")}},
	})
	rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
	if _, err := rn.Propose(Command{Payload: []byte("y"), ConflictKeys: [][]byte{[]byte("k")}}); !errors.Is(err, ErrResidentInstancesExceeded) {
		t.Fatalf("err=%v", err)
	}
}

