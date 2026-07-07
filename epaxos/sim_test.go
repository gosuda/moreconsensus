package epaxos

import (
	"bytes"
	"errors"
	"fmt"
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

func (s *simCluster) drain() {
	for round := 0; round < 1000; round++ {
		progress := false
		for id, rn := range s.nodes {
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
		for _, rn := range s.nodes {
			rn.Tick()
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
			for id := range s.nodes {
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
	for id := range s.nodes {
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

func refs(cmds []CommittedCommand) []InstanceRef {
	out := make([]InstanceRef, len(cmds))
	for i := range cmds {
		out[i] = cmds[i].Ref
	}
	return out
}
