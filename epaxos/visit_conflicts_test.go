package epaxos

import "testing"

func TestVisitConflictsYieldsAndStops(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("vk")
	for _, rec := range []InstanceRecord{
		checkedRecord(InstanceRecord{Ref: InstanceRef{Conf: 1, Replica: 2, Instance: 1}, Status: StatusCommitted, Seq: 2, Ballot: Ballot{Replica: 2}, Deps: rn.q.deps(), Command: Command{Payload: []byte("a"), ConflictKeys: [][]byte{key}}}),
		checkedRecord(InstanceRecord{Ref: InstanceRef{Conf: 1, Replica: 3, Instance: 1}, Status: StatusPreAccepted, Seq: 3, Ballot: Ballot{Replica: 3}, Deps: rn.q.deps(), Command: Command{Payload: []byte("b"), ConflictKeys: [][]byte{key}}}),
	} {
		rn.installInstance(&instance{rec: rec, phase: phaseFromStatus(rec.Status)})
	}
	var got []InstanceRef
	rn.VisitConflicts(Command{Payload: []byte("c"), ConflictKeys: [][]byte{key}}, func(ref InstanceRef, _ Status) bool {
		got = append(got, ref)
		return len(got) < 1 // stop early
	})
	if len(got) != 1 {
		t.Fatalf("got=%v, want early stop after 1", got)
	}
}

func TestVisitConflictsAllocs(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("ak")
	rec := checkedRecord(InstanceRecord{Ref: InstanceRef{Conf: 1, Replica: 2, Instance: 1}, Status: StatusCommitted, Seq: 1, Ballot: Ballot{Replica: 2}, Deps: rn.q.deps(), Command: Command{Payload: []byte("a"), ConflictKeys: [][]byte{key}}})
	rn.installInstance(&instance{rec: rec, phase: phaseCommitted})
	cmd := Command{Payload: []byte("c"), ConflictKeys: [][]byte{key}}
	allocs := testing.AllocsPerRun(1000, func() {
		rn.VisitConflicts(cmd, func(InstanceRef, Status) bool { return true })
	})
	if allocs != 0 {
		t.Fatalf("allocs=%v, want 0", allocs)
	}
}
