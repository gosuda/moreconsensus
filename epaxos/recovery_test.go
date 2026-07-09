package epaxos

import (
	"bytes"
	"testing"
)

func TestDependencyClosureStartsPrefixRecoveryBeforeExecution(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}

	dependentRef := InstanceRef{Replica: 1, Instance: 3, Conf: 1}
	foreignPrefixRefs := []InstanceRef{
		{Replica: 2, Instance: 1, Conf: 1},
		{Replica: 2, Instance: 2, Conf: 1},
	}
	dependent := Command{ID: CommandID{Client: 10, Sequence: 1}, Payload: []byte("dependent"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{
		Type:         MsgCommit,
		From:         2,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Seq:          3,
		Deps:         []InstanceNum{0, 2, 0},
		Command:      dependent,
		RecordStatus: StatusCommitted,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatal(err)
	}
	recoveryRequireDependencyRefs(t, rn.dependencyRefs(dependentRef), foreignPrefixRefs)

	rd := rn.Ready()
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before dependency closure: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryRequirePrepareRefs(t, rd.Messages, foreignPrefixRefs[:1])
	recoveryRequireNoPrepareRefs(t, rd.Messages, []InstanceRef{
		{Replica: 1, Instance: 1, Conf: 1},
		{Replica: 1, Instance: 2, Conf: 1},
	})
}

func TestForeignMissingDependencyRecoveryNoopsAndReleasesDependent(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}

	missingRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	dependentRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	dependent := Command{ID: CommandID{Client: 20, Sequence: 1}, Payload: []byte("after-noop"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{
		Type:         MsgCommit,
		From:         3,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 3},
		RecordBallot: Ballot{Replica: 3},
		Seq:          2,
		Deps:         []InstanceNum{0, 1, 0},
		Command:      dependent,
		RecordStatus: StatusCommitted,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before missing dependency recovery: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryRequirePrepareRefs(t, rd.Messages, []InstanceRef{missingRef})
	recoveryApplyReady(t, store, rn, rd)

	ballot := Ballot{Number: 1, Replica: 1}
	resp := Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: missingRef, Ballot: ballot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0}}
	if err := rn.Step(resp); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	recoveryRequireRecord(t, rd.Records, missingRef, StatusAccepted, CommandNoop)
	recoveryRequireMessages(t, rd.Messages, MsgAccept, missingRef, CommandNoop)
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before noop accept quorum: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryApplyReady(t, store, rn, rd)

	acceptResp := Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: missingRef, Ballot: ballot, Seq: 1, Deps: []InstanceNum{0, 0, 0}, RecordStatus: StatusAccepted, Command: Command{Kind: CommandNoop}}
	if err := rn.Step(acceptResp); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	recoveryRequireRecord(t, rd.Records, missingRef, StatusExecuted, CommandNoop)
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 1 {
		t.Fatalf("dependent command release count=%d, want 1; committed=%#v", got, rd.Committed)
	}
	for _, c := range rd.Committed {
		if c.Ref == missingRef || c.Command.Kind == CommandNoop {
			t.Fatalf("noop recovery emitted application command: %#v", rd.Committed)
		}
	}
	recoveryApplyReady(t, store, rn, rd)

	if rn.HasReady() {
		rd = rn.Ready()
		if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
			t.Fatalf("dependent command emitted again after recovery: count=%d ready=%#v", got, rd)
		}
		recoveryApplyReady(t, store, rn, rd)
	}
}

func TestRestartResumesForeignRecoveryBallot(t *testing.T) {
	for _, tt := range []struct {
		name string
		st   Status
		cmd  Command
	}{
		{name: "none", st: StatusNone, cmd: Command{Kind: CommandNoop}},
		{name: "accepted", st: StatusAccepted, cmd: Command{Kind: CommandNoop}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			ref := InstanceRef{Replica: 2, Instance: 7, Conf: 1}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:     ref,
				Ballot:  Ballot{Number: 1, Replica: 1},
				Status:  tt.st,
				Seq:     1,
				Deps:    []InstanceNum{0, 0, 0},
				Command: tt.cmd,
			})
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
			if err != nil {
				t.Fatal(err)
			}
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before recovery deadline: %#v", restarted.Ready())
			}
			for tick := 1; tick < 5; tick++ {
				restarted.Tick()
				if restarted.HasReady() {
					t.Fatalf("recovery fired after %d ticks, before deadline", tick)
				}
			}
			restarted.Tick()
			rd := restarted.Ready()
			recoveryRequirePrepareRefs(t, rd.Messages, []InstanceRef{ref})
			for _, m := range rd.Messages {
				if m.Type != MsgPrepare || m.Ref != ref {
					continue
				}
				if m.From != 1 || (m.To != 2 && m.To != 3) {
					t.Fatalf("foreign recovery prepare = %#v, want from local node to peers", m)
				}
			}
		})
	}
}

func TestSimulatorNonOwnerRecoversPreAcceptedDependency(t *testing.T) {
	s := newSimCluster(t, 3, false)
	first := Command{ID: CommandID{Client: 30, Sequence: 1}, Payload: []byte("owner-command"), ConflictKeys: [][]byte{[]byte("shared")}}
	firstRef, err := s.nodes[1].Propose(first)
	if err != nil {
		t.Fatal(err)
	}
	ownerReady := s.nodes[1].Ready()
	if err := s.stores[1].ApplyReady(ownerReady); err != nil {
		t.Fatal(err)
	}
	for _, m := range ownerReady.Messages {
		if m.Type == MsgPreAccept && !s.deliver(m) {
			t.Fatalf("preaccept to follower was unexpectedly delayed: %#v", m)
		}
	}
	advanceOK(t, s.nodes[1], ownerReady)

	s.pause(1)
	s.drain()

	second := Command{ID: CommandID{Client: 31, Sequence: 1}, Payload: []byte("follower-command"), ConflictKeys: [][]byte{[]byte("shared")}}
	secondRef, err := s.nodes[2].Propose(second)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickOnly(2, 6)

	for _, id := range []ReplicaID{2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, firstRef, first.Payload, 1)
		requireCommittedPayloadCount(t, s.apps[id], id, secondRef, second.Payload, 1)
	}
}

func TestSimulatorIsolatedLocalTimeoutRecoversAfterConflictingQuorumCommit(t *testing.T) {
	s := newSimCluster(t, 3, true)
	key := []byte("current-canary-recovery")

	s.omit(1)
	isolated := Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("isolated-local"), ConflictKeys: [][]byte{key}}
	isolatedRef, err := s.nodes[1].Propose(isolated)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickOnly(1, 6)
	if committedPayload(s.apps[1], isolatedRef, isolated.Payload) {
		t.Fatalf("isolated replica applied %s without quorum before heal: %#v", isolatedRef, s.apps[1])
	}

	quorum := Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("quorum-commit"), ConflictKeys: [][]byte{key}}
	quorumRef, err := s.nodes[2].Propose(quorum)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(6)
	for _, id := range []ReplicaID{2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, quorumRef, quorum.Payload, 1)
	}
	if committedPayload(s.apps[1], quorumRef, quorum.Payload) {
		t.Fatalf("isolated replica applied quorum commit %s before transport healed: %#v", quorumRef, s.apps[1])
	}

	s.heal(1)
	s.tickAll(20)
	requireCommittedPayloadCount(t, s.apps[1], 1, quorumRef, quorum.Payload, 1)

	barrier := Command{ID: CommandID{Client: 1, Sequence: 2}, ConflictKeys: [][]byte{key}}
	barrierRef, err := s.nodes[1].Propose(barrier)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(20)
	requireCommittedPayloadCount(t, s.apps[1], 1, barrierRef, barrier.Payload, 1)
	requireCommittedPayloadCount(t, s.apps[1], 1, isolatedRef, isolated.Payload, 1)
	recoveryRequireAppliedOrder(t, s.apps[1], 1, quorumRef, isolatedRef, barrierRef)
	requireNoDuplicateCommittedRefs(t, s.apps[1], 1)
}

func recoveryRequireAppliedOrder(t *testing.T, app []CommittedCommand, id ReplicaID, want ...InstanceRef) {
	t.Helper()
	positions := make(map[InstanceRef]int, len(app))
	for i, c := range app {
		if _, ok := positions[c.Ref]; !ok {
			positions[c.Ref] = i
		}
	}
	previous := -1
	for _, ref := range want {
		pos, ok := positions[ref]
		if !ok {
			t.Fatalf("node %d applied refs %v, missing %s", id, refs(app), ref)
		}
		if pos <= previous {
			t.Fatalf("node %d applied refs %v, want ordered subsequence %v", id, refs(app), want)
		}
		previous = pos
	}
}

func recoveryApplyReady(t *testing.T, store *MemoryStorage, rn *RawNode, rd Ready) {
	t.Helper()
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
}

func recoveryRequirePrepareRefs(t *testing.T, messages []Message, want []InstanceRef) {
	t.Helper()
	for _, ref := range want {
		count := 0
		for _, m := range messages {
			if m.Type == MsgPrepare && m.Ref == ref {
				count++
				if m.From == m.To {
					t.Fatalf("prepare for %s sent to self: %#v", ref, m)
				}
				if m.Ballot.Number == 0 || m.Ballot.Replica == 0 {
					t.Fatalf("prepare for %s has no recovery ballot: %#v", ref, m)
				}
			}
		}
		if count != 2 {
			t.Fatalf("prepare messages for %s = %d, want 2; messages=%#v", ref, count, messages)
		}
	}
}

func recoveryRequireDependencyRefs(t *testing.T, got []InstanceRef, want []InstanceRef) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("dependency refs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dependency refs = %v, want %v", got, want)
		}
	}
}

func recoveryRequireNoPrepareRefs(t *testing.T, messages []Message, refs []InstanceRef) {
	t.Helper()
	for _, ref := range refs {
		for _, m := range messages {
			if m.Type == MsgPrepare && m.Ref == ref {
				t.Fatalf("unexpected prepare for %s in %#v", ref, messages)
			}
		}
	}
}

func recoveryRequireMessages(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, kind CommandKind) {
	t.Helper()
	count := 0
	for _, m := range messages {
		if m.Type != typ || m.Ref != ref {
			continue
		}
		count++
		if m.Command.Kind != kind {
			t.Fatalf("%s message for %s command kind = %v, want %v: %#v", typ, ref, m.Command.Kind, kind, m)
		}
	}
	if count != 2 {
		t.Fatalf("%s messages for %s = %d, want 2; messages=%#v", typ, ref, count, messages)
	}
}

func recoveryRequireRecord(t *testing.T, records []InstanceRecord, ref InstanceRef, status Status, kind CommandKind) {
	t.Helper()
	for _, rec := range records {
		if rec.Ref != ref || rec.Status != status {
			continue
		}
		if rec.Command.Kind != kind {
			t.Fatalf("record %s/%s command kind = %v, want %v: %#v", ref, status, rec.Command.Kind, kind, rec)
		}
		return
	}
	t.Fatalf("missing %s record for %s with command kind %v: %#v", status, ref, kind, records)
}

func recoveryCommittedPayloadCount(committed []CommittedCommand, ref InstanceRef, payload []byte) int {
	count := 0
	for _, c := range committed {
		if c.Ref == ref && bytes.Equal(c.Command.Payload, payload) {
			count++
		}
	}
	return count
}
