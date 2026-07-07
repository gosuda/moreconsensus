package epaxos

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"
)

type simCluster struct {
	t      *testing.T
	nodes  map[ReplicaID]*RawNode
	stores map[ReplicaID]*MemoryStorage
	apps   map[ReplicaID][]CommittedCommand
	drop   map[[2]ReplicaID]bool
}

func newSimCluster(t *testing.T, n int, opt bool) *simCluster {
	t.Helper()
	ids := makeIDs(n)
	s := &simCluster{t: t, nodes: make(map[ReplicaID]*RawNode), stores: make(map[ReplicaID]*MemoryStorage), apps: make(map[ReplicaID][]CommittedCommand), drop: make(map[[2]ReplicaID]bool)}
	for _, id := range ids {
		st := NewMemoryStorage()
		rn, err := NewRawNode(Config{ID: id, Voters: ids, Storage: st, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: opt, TimeOptimizationTicks: 1})
		if err != nil {
			t.Fatalf("new node %d: %v", id, err)
		}
		s.nodes[id] = rn
		s.stores[id] = st
	}
	return s
}

func (s *simCluster) ids() []ReplicaID {
	ids := make([]ReplicaID, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (s *simCluster) drain() {
	for round := 0; round < 1000; round++ {
		progress := false
		for _, id := range s.ids() {
			rn := s.nodes[id]
			if !rn.HasReady() {
				continue
			}
			progress = true
			rd := rn.Ready()
			if err := s.stores[id].ApplyReady(rd); err != nil {
				s.t.Fatalf("apply ready %d: %v", id, err)
			}
			for _, c := range rd.Committed {
				s.apps[id] = append(s.apps[id], c)
			}
			for _, m := range rd.Messages {
				if s.drop[[2]ReplicaID{m.From, m.To}] {
					continue
				}
				if to := s.nodes[m.To]; to != nil {
					if err := to.Step(m); err != nil && !errors.Is(err, ErrMessageRejected) {
						s.t.Fatalf("step %s %d->%d: %v", m.Type, m.From, m.To, err)
					}
				}
			}
			rn.Advance(rd)
		}
		if !progress {
			return
		}
	}
	s.t.Fatalf("simulation did not quiesce")
}

func (s *simCluster) tickAll(n int) {
	for i := 0; i < n; i++ {
		for _, id := range s.ids() {
			s.nodes[id].Tick()
		}
		s.drain()
	}
}

func TestClusterSizesOneThroughSevenCommit(t *testing.T) {
	for size := 1; size <= 7; size++ {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			s := newSimCluster(t, size, false)
			_, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: uint64(size)}, Payload: []byte("set"), ConflictKeys: [][]byte{[]byte("k")}})
			if err != nil {
				t.Fatal(err)
			}
			s.drain()
			for _, id := range s.ids() {
				if got := len(s.apps[id]); got != 1 {
					t.Fatalf("node %d applied %d commands", id, got)
				}
				if !bytes.Equal(s.apps[id][0].Command.Payload, []byte("set")) {
					t.Fatalf("node %d payload mismatch", id)
				}
			}
		})
	}
}

func TestConflictingConcurrentCommandsConverge(t *testing.T) {
	s := newSimCluster(t, 5, true)
	if _, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("a"), ConflictKeys: [][]byte{[]byte("same")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.nodes[2].Propose(Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("b"), ConflictKeys: [][]byte{[]byte("same")}}); err != nil {
		t.Fatal(err)
	}
	s.drain()
	want := refs(s.apps[1])
	for _, id := range s.ids() {
		if got := refs(s.apps[id]); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d order %v want %v", id, got, want)
		}
	}
}

func TestDuplicateMessagesAndMalformedInput(t *testing.T) {
	s := newSimCluster(t, 3, false)
	if _, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}}); err != nil {
		t.Fatal(err)
	}
	rd := s.nodes[1].Ready()
	if err := s.stores[1].ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if len(rd.Messages) == 0 {
		t.Fatal("expected messages")
	}
	m := rd.Messages[0]
	if err := s.nodes[m.To].Step(m); err != nil {
		t.Fatal(err)
	}
	if err := s.nodes[m.To].Step(m); err != nil {
		t.Fatal(err)
	}
	bad := m
	bad.To = 99
	if err := s.nodes[m.To].Step(bad); !errors.Is(err, ErrMessageRejected) && !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("bad target err=%v", err)
	}
	s.nodes[1].Advance(rd)
	var decoded Message
	if err := DecodeMessage([]byte{1, 2, 3}, &decoded); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("malformed decode err=%v", err)
	}
}

func TestCodecChecksumZeroCopy(t *testing.T) {
	m := Message{Type: MsgCommit, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: []InstanceNum{0, 0, 0}, Command: Command{ID: CommandID{Client: 7, Sequence: 9}, Payload: []byte("payload"), ConflictKeys: [][]byte{[]byte("k")}}, RecordStatus: StatusCommitted}
	buf, err := EncodeMessage(make([]byte, 0, 128), m)
	if err != nil {
		t.Fatal(err)
	}
	var out Message
	if err := DecodeMessage(buf, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Command.Payload, []byte("payload")) {
		t.Fatal("payload mismatch")
	}
	buf[len(buf)-1] ^= 0xff
	if err := DecodeMessage(buf, &out); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("corrupt decode err=%v", err)
	}
}

func TestRestartFromMemoryStorage(t *testing.T) {
	s := newSimCluster(t, 3, false)
	if _, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}}); err != nil {
		t.Fatal(err)
	}
	s.drain()
	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: s.stores[1]})
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.Status().Instances) == 0 {
		t.Fatal("restart lost instances")
	}
}

func TestRestartAllRawNodesRetainsExecutedAndAppliesOnlyNewCommand(t *testing.T) {
	ids := makeIDs(3)
	s := newSimCluster(t, len(ids), false)
	first := Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("first"), ConflictKeys: [][]byte{[]byte("shared")}}
	second := Command{ID: CommandID{Client: 1, Sequence: 2}, Payload: []byte("second"), ConflictKeys: [][]byte{[]byte("shared")}}
	if _, err := s.nodes[1].Propose(first); err != nil {
		t.Fatal(err)
	}
	s.drain()
	if got := len(s.apps[1]); got != 1 {
		t.Fatalf("node 1 applied %d commands before restart", got)
	}
	firstRef := s.apps[1][0].Ref
	for _, id := range s.ids() {
		if got := len(s.apps[id]); got != 1 {
			t.Fatalf("node %d applied %d commands before restart", id, got)
		}
		if s.apps[id][0].Ref != firstRef {
			t.Fatalf("node %d first ref = %s, want %s", id, s.apps[id][0].Ref, firstRef)
		}
	}

	s.nodes = make(map[ReplicaID]*RawNode, len(ids))
	for _, id := range ids {
		rn, err := NewRawNode(Config{ID: id, Voters: ids, Storage: s.stores[id], RetryTicks: 2, RecoveryTicks: 5})
		if err != nil {
			t.Fatalf("restart node %d: %v", id, err)
		}
		s.nodes[id] = rn
	}
	s.apps = make(map[ReplicaID][]CommittedCommand, len(ids))

	if _, err := s.nodes[2].Propose(second); err != nil {
		t.Fatal(err)
	}
	s.drain()
	for _, id := range s.ids() {
		rn := s.nodes[id]
		applied := s.apps[id]
		if len(applied) != 1 {
			t.Fatalf("node %d applied %d commands after restart: %#v", id, len(applied), applied)
		}
		gotCmd := applied[0].Command
		if gotCmd.ID != second.ID ||
			!bytes.Equal(gotCmd.Payload, second.Payload) ||
			len(gotCmd.ConflictKeys) != 1 ||
			!bytes.Equal(gotCmd.ConflictKeys[0], second.ConflictKeys[0]) {
			t.Fatalf("node %d applied command = %#v, want %#v", id, gotCmd, second)
		}
		if applied[0].Ref == firstRef {
			t.Fatalf("node %d re-applied first ref %s", id, firstRef)
		}
		var hasFirst bool
		for _, ref := range rn.Status().Executed {
			if ref == firstRef {
				hasFirst = true
				break
			}
		}
		if !hasFirst {
			t.Fatalf("node %d executed refs lost first ref %s: %#v", id, firstRef, rn.Status().Executed)
		}
	}
}

func TestLogicalTicksRecoveryAndStorageFailure(t *testing.T) {
	s := newSimCluster(t, 3, true)
	s.drop[[2]ReplicaID{1, 2}] = true
	s.drop[[2]ReplicaID{2, 1}] = true
	if _, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("x"), ConflictKeys: [][]byte{[]byte("x")}}); err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(3)
	s.drop = map[[2]ReplicaID]bool{}
	s.tickAll(8)
	if len(s.apps[1]) != 1 || len(s.apps[2]) != 1 || len(s.apps[3]) != 1 {
		t.Fatalf("recovery did not apply everywhere: %#v", map[ReplicaID]int{1: len(s.apps[1]), 2: len(s.apps[2]), 3: len(s.apps[3])})
	}
	s.stores[1].FailWrites = true
	if _, err := s.nodes[1].Propose(Command{ID: CommandID{Client: 1, Sequence: 2}, Payload: []byte("y"), ConflictKeys: [][]byte{[]byte("y")}}); err != nil {
		t.Fatal(err)
	}
	rd := s.nodes[1].Ready()
	if err := s.stores[1].ApplyReady(rd); err == nil {
		t.Fatal("expected storage failure")
	}
}

func TestConfChangeAndPools(t *testing.T) {
	s := newSimCluster(t, 3, false)
	if _, err := s.nodes[1].ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4}); err != nil {
		t.Fatal(err)
	}
	s.drain()
	if !s.nodes[1].Status().Conf.Contains(4) {
		t.Fatal("configuration was not applied")
	}
	m := GetMessage()
	m.Type = MsgCommit
	PutMessage(m)
	c := GetCommand()
	c.Payload = []byte("owned")
	PutCommand(c)
}

func TestQuorumTables(t *testing.T) {
	wantSlow := []int{1, 2, 2, 3, 3, 4, 4}
	wantFast := []int{1, 2, 3, 4, 4, 5, 6}
	for n := 1; n <= 7; n++ {
		slow, err := SlowQuorum(n)
		if err != nil {
			t.Fatal(err)
		}
		fast, err := FastQuorum(n)
		if err != nil {
			t.Fatal(err)
		}
		if slow != wantSlow[n-1] || fast != wantFast[n-1] {
			t.Fatalf("n=%d slow=%d fast=%d", n, slow, fast)
		}
	}
	if _, err := SlowQuorum(8); err == nil {
		t.Fatal("expected invalid size")
	}
}

func TestExecutionEqualSeqTieBreaksByRef(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	a := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	b := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	c := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	rn.instances[a] = &instance{rec: InstanceRecord{Ref: a, Status: StatusCommitted, Seq: 7, Deps: []InstanceNum{1, 0, 0}, Command: Command{Payload: []byte("a")}}}
	rn.instances[b] = &instance{rec: InstanceRecord{Ref: b, Status: StatusCommitted, Seq: 7, Deps: []InstanceNum{0, 1, 0}, Command: Command{Payload: []byte("b")}}}
	rn.instances[c] = &instance{rec: InstanceRecord{Ref: c, Status: StatusCommitted, Seq: 7, Deps: []InstanceNum{0, 0, 1}, Command: Command{Payload: []byte("c")}}}

	comps := rn.executionComponents()
	if len(comps) != 1 || len(comps[0]) != 3 {
		t.Fatalf("equal-seq cycle components = %#v", comps)
	}
	if refs := append([]InstanceRef(nil), comps[0]...); len(refs) == 3 && refs[0] == b && refs[1] == c && refs[2] == a {
		t.Fatalf("test setup no longer exercises execution sort: %#v", refs)
	}

	rn.tryExecute()
	rd := rn.Ready()
	want := []InstanceRef{b, c, a}
	if got := refs(rd.Committed); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("equal-seq execution order = %v, want %v", got, want)
	}
	for i, cmd := range rd.Committed {
		if cmd.Seq != 7 {
			t.Fatalf("committed[%d] seq = %d, want 7", i, cmd.Seq)
		}
	}
}

func TestExecutionComponentsSkipInactiveDependencyRefs(t *testing.T) {
	tests := []struct {
		name string
		dep  *instance
	}{
		{name: "missing"},
		{name: "not-yet-chosen", dep: &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Status: StatusAccepted, Seq: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
			if err != nil {
				t.Fatal(err)
			}
			x := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
			y := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
			rn.instances[x] = &instance{rec: InstanceRecord{Ref: x, Status: StatusCommitted, Seq: 2, Deps: []InstanceNum{0, 1, 0}, Command: Command{Payload: []byte("x")}}}
			if tt.dep != nil {
				rn.instances[y] = tt.dep
			}

			comps := rn.executionComponents()
			if len(comps) != 1 || len(comps[0]) != 1 || comps[0][0] != x {
				t.Fatalf("component with inactive dependency = %#v, want only %s", comps, x)
			}
		})
	}
}

func TestRemoveVoterConfChangeAllowsLaterProgress(t *testing.T) {
	s := newSimCluster(t, 3, false)
	if _, err := s.nodes[1].ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}); err != nil {
		t.Fatal(err)
	}
	s.drain()
	for _, id := range []ReplicaID{1, 2, 3} {
		conf := s.nodes[id].Status().Conf
		if conf.ID != 2 || conf.Contains(3) || !conf.Contains(1) || !conf.Contains(2) {
			t.Fatalf("node %d config after voter removal = %#v", id, conf)
		}
	}

	cmd := Command{ID: CommandID{Client: 7, Sequence: 1}, Payload: []byte("after-removal"), ConflictKeys: [][]byte{[]byte("after-removal")}}
	ref, err := s.nodes[1].Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Conf != 2 {
		t.Fatalf("proposal used config %d, want 2", ref.Conf)
	}
	s.drain()
	for _, id := range []ReplicaID{1, 2} {
		if !committedPayload(s.apps[id], ref, cmd.Payload) {
			t.Fatalf("node %d did not apply command %s after voter removal: %#v", id, ref, s.apps[id])
		}
	}
	if committedPayload(s.apps[3], ref, cmd.Payload) {
		t.Fatalf("removed voter applied command %s: %#v", ref, s.apps[3])
	}
}

func TestWriteErrorKeepsReadyForRetry(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 8, Sequence: 1}, Payload: []byte("durable"), ConflictKeys: [][]byte{[]byte("durable")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) == 0 || len(rd.Committed) != 1 || rd.Committed[0].Ref != ref {
		t.Fatalf("ready for %s = %#v", ref, rd)
	}

	store.FailWrites = true
	if err := store.ApplyReady(rd); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("write error = %v", err)
	}
	if _, ok := store.Instance(ref); ok {
		t.Fatalf("record %s was stored after rejected write", ref)
	}
	if rn.HasReady() {
		t.Fatal("node exposed new work before the current ready was advanced")
	}

	store.FailWrites = false
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	rn.Advance(rd)
	if rn.HasReady() {
		t.Fatal("node still has ready work after retry and advance")
	}
	persisted, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("record %s not stored after retry", ref)
	}
	if persisted.Status != StatusExecuted {
		t.Fatalf("stored status = %s, want executed", persisted.Status)
	}
	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	status := restarted.Status()
	if len(status.Executed) != 1 || status.Executed[0] != ref {
		t.Fatalf("restart executed refs = %#v, want %s", status.Executed, ref)
	}
	if restarted.HasReady() {
		t.Fatal("restart re-emitted durable command")
	}
}

func TestFiveNodePartitionHealConverges(t *testing.T) {
	s := newSimCluster(t, 5, true)
	majority := []ReplicaID{1, 2, 3}
	minority := []ReplicaID{4, 5}
	for _, a := range majority {
		for _, b := range minority {
			s.drop[[2]ReplicaID{a, b}] = true
			s.drop[[2]ReplicaID{b, a}] = true
		}
	}

	cmd := Command{ID: CommandID{Client: 9, Sequence: 1}, Payload: []byte("majority"), ConflictKeys: [][]byte{[]byte("majority")}}
	ref, err := s.nodes[1].Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(2)
	for _, id := range majority {
		if !committedPayload(s.apps[id], ref, cmd.Payload) {
			t.Fatalf("majority node %d did not apply %s while minority was isolated: %#v", id, ref, s.apps[id])
		}
	}
	for _, id := range minority {
		if committedPayload(s.apps[id], ref, cmd.Payload) {
			t.Fatalf("minority node %d applied %s before heal: %#v", id, ref, s.apps[id])
		}
	}

	s.drop = map[[2]ReplicaID]bool{}
	s.tickAll(6)
	want := refs(s.apps[1])
	for _, id := range s.ids() {
		if got := refs(s.apps[id]); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d refs = %v, want %v", id, got, want)
		}
		if !committedPayload(s.apps[id], ref, cmd.Payload) {
			t.Fatalf("node %d did not converge on %s: %#v", id, ref, s.apps[id])
		}
	}
}

func committedPayload(app []CommittedCommand, ref InstanceRef, payload []byte) bool {
	for _, c := range app {
		if c.Ref == ref && bytes.Equal(c.Command.Payload, payload) {
			return true
		}
	}
	return false
}

func refs(cmds []CommittedCommand) []InstanceRef {
	out := make([]InstanceRef, len(cmds))
	for i := range cmds {
		out[i] = cmds[i].Ref
	}
	return out
}
