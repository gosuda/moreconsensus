package epaxos

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

type stressRNG struct {
	state uint64
}

func newStressRNG(seed uint64) *stressRNG {
	return &stressRNG{state: seed}
}

func (r *stressRNG) next() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *stressRNG) intn(n int) int {
	return int(r.next() % uint64(n)) //nolint:gosec // G115: test harness converts bounded int index/count
}

type stressProposal struct {
	ref InstanceRef
	cmd Command
}

type stressTransportCluster struct {
	t       *testing.T
	ids     []ReplicaID
	nodes   map[ReplicaID]*RawNode
	stores  map[ReplicaID]*MemoryStorage
	apps    map[ReplicaID][]ApplyCommand
	pending []Message
	rng     *stressRNG
	opt     bool
}

func newStressTransportCluster(t *testing.T, size int, seed uint64, opt bool) *stressTransportCluster {
	t.Helper()
	ids := makeIDs(size)
	s := &stressTransportCluster{
		t:      t,
		ids:    ids,
		nodes:  make(map[ReplicaID]*RawNode, size),
		stores: make(map[ReplicaID]*MemoryStorage, size),
		apps:   make(map[ReplicaID][]ApplyCommand, size),
		rng:    newStressRNG(seed),
		opt:    opt,
	}
	for _, id := range ids {
		st := NewMemoryStorage()
		rn, err := NewRawNode(Config{ID: id, Voters: ids, Storage: st, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: opt, TimeOptimizationTicks: 1, MaxReadyMessages: 2})
		if err != nil {
			t.Fatalf("new node %d: %v", id, err)
		}
		s.nodes[id] = rn
		s.stores[id] = st
	}
	return s
}

func (s *stressTransportCluster) propose(node ReplicaID, cmd Command) stressProposal {
	s.t.Helper()
	ref, err := s.nodes[node].Propose(cmd)
	if err != nil {
		s.t.Fatalf("propose on node %d: %v", node, err)
	}
	return stressProposal{ref: ref, cmd: cmd.Clone()}
}

func (s *stressTransportCluster) readyIDs() []ReplicaID {
	ids := make([]ReplicaID, 0, len(s.ids))
	for _, id := range s.ids {
		if s.nodes[id].HasReady() {
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *stressTransportCluster) captureReady(id ReplicaID) {
	s.t.Helper()
	rn := s.nodes[id]
	rd := rn.Ready()
	if err := s.stores[id].ApplyReady(rd); err != nil {
		s.t.Fatalf("apply ready %d: %v", id, err)
	}
	s.apps[id] = append(s.apps[id], rd.Apply...)
	s.pending = append(s.pending, rd.Messages...)
	if err := rn.Advance(rd); err != nil {
		s.t.Fatalf("advance %d: %v", id, err)
	}
}

func (s *stressTransportCluster) captureRandomReady() bool {
	ids := s.readyIDs()
	if len(ids) == 0 {
		return false
	}
	s.captureReady(ids[s.rng.intn(len(ids))])
	return true
}

func (s *stressTransportCluster) deliver(m Message) {
	s.t.Helper()
	to := s.nodes[m.To]
	if to == nil {
		return
	}
	if err := to.Step(canonicalTestMessage(m)); err != nil && !errors.Is(err, ErrMessageRejected) { s.t.Fatalf("step %s %d->%d: %v; message=%#v", m.Type, m.From, m.To, err, m)
 }
}

func (s *stressTransportCluster) takePending() (Message, bool) {
	if len(s.pending) == 0 {
		return Message{}, false
	}
	idx := s.rng.intn(len(s.pending))
	m := s.pending[idx]
	last := len(s.pending) - 1
	s.pending[idx] = s.pending[last]
	s.pending = s.pending[:last]
	return m, true
}

func (s *stressTransportCluster) randomTransportStep() bool {
	m, ok := s.takePending()
	if !ok {
		return false
	}
	switch s.rng.intn(8) {
	case 0:
		return true
	case 1:
		s.pending = append(s.pending, m)
		return true
	case 2:
		s.deliver(m)
		s.deliver(m)
		return true
	default:
		s.deliver(m)
		return true
	}
}

func (s *stressTransportCluster) tickSome() {
	if s.rng.intn(3) == 0 {
		for _, id := range s.ids {
			if err := s.nodes[id].Tick(); err != nil {
				s.t.Fatal(err)
			}
		}
		return
	}
	if err := s.nodes[s.ids[s.rng.intn(len(s.ids))]].Tick(); err != nil {
		s.t.Fatal(err)
	}
}

func (s *stressTransportCluster) restart(id ReplicaID) {
	s.t.Helper()
	for s.nodes[id].HasReady() {
		s.captureReady(id)
	}
	rn, err := NewRawNode(Config{ID: id, Voters: s.ids, Storage: s.stores[id], RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: s.opt, TimeOptimizationTicks: 1, MaxReadyMessages: 2})
	if err != nil {
		s.t.Fatalf("restart node %d: %v", id, err)
	}
	s.nodes[id] = rn
}

func (s *stressTransportCluster) churn(steps int) {
	for range steps {
		switch s.rng.intn(12) {
		case 0, 1, 2, 3:
			if s.captureRandomReady() {
				continue
			}
		case 4, 5, 6, 7, 8:
			if s.randomTransportStep() {
				continue
			}
		case 9:
			s.tickSome()
			continue
		case 10:
			s.restart(s.ids[s.rng.intn(len(s.ids))])
			continue
		}
		s.tickSome()
	}
}

func (s *stressTransportCluster) driveUntilResolved(proposals []stressProposal) {
	s.t.Helper()
	for range 4000 {
		for _, id := range s.readyIDs() {
			s.captureReady(id)
		}
		if s.resolvedAll(proposals) {
			return
		}
		if m, ok := s.takePending(); ok {
			s.deliver(m)
			continue
		}
		for _, id := range s.ids {
			if err := s.nodes[id].Tick(); err != nil {
				s.t.Fatal(err)
			}
		}
	}
	s.t.Fatalf("cluster did not resolve %d proposed instances: counts=%#v pending=%d", len(proposals), s.appCounts(), len(s.pending))
}

func (s *stressTransportCluster) resolvedAll(proposals []stressProposal) bool {
	for _, id := range s.ids {
		for _, proposal := range proposals {
			inst := s.nodes[id].instances[proposal.ref]
			if inst == nil || inst.rec.Status != StatusExecuted {
				return false
			}
		}
	}
	return true
}

func (s *stressTransportCluster) appCounts() map[ReplicaID]int {
	counts := make(map[ReplicaID]int, len(s.ids))
	for _, id := range s.ids {
		counts[id] = len(s.apps[id])
	}
	return counts
}

func TestDeterministicRandomizedCoreSimulationConverges(t *testing.T) {
	const baseSeed uint64 = 0x5eed1234c0ffee
	tests := []struct {
		name string
		size int
		seed uint64
	}{
		{name: "n=3", size: 3, seed: baseSeed},
		{name: "n=5", size: 5, seed: baseSeed ^ 0x0505050505050505},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStressTransportCluster(t, tt.size, tt.seed, true)
			proposals := make([]stressProposal, 0, tt.size*4)
			for i := range tt.size * 4 {
				node := s.ids[s.rng.intn(len(s.ids))]
				cmd := stressCommand(tt.size, i, s.rng)
				proposals = append(proposals, s.propose(node, cmd))
				s.churn(8 + s.rng.intn(12))
				if i == tt.size || i == tt.size*3 {
					s.restart(s.ids[s.rng.intn(len(s.ids))])
				}
			}
			s.churn(200)
			s.driveUntilResolved(proposals)
			assertStressConvergence(t, s, proposals)
		})
	}
}

func stressCommand(size, index int, rng *stressRNG) Command {
	keys := [][]byte{[]byte(fmt.Sprintf("k-%d", rng.intn(size)))}
	if index%3 == 0 {
		keys = [][]byte{[]byte("hot")}
	}
	if index%5 == 0 {
		keys = append(keys, []byte(fmt.Sprintf("pair-%d", index%2)))
	}
	return Command{
		ID:        CommandID{Client: uint64(100 + index%size), Sequence: uint64(index + 1)}, //nolint:gosec // G115: test harness converts bounded int index/count
		Payload:   []byte(fmt.Sprintf("cmd-%02d", index)),
		Footprint: Footprint{Points: keys},
	}
}

func assertStressConvergence(t *testing.T, s *stressTransportCluster, proposals []stressProposal) {
	t.Helper()
	resolvedRefs := make([]InstanceRef, 0, len(proposals))
	chosen := make([]stressProposal, 0, len(proposals))
	for _, proposal := range proposals {
		resolvedRefs = append(resolvedRefs, proposal.ref)
		base := s.nodes[s.ids[0]].instances[proposal.ref]
		if base == nil || base.rec.Status != StatusExecuted {
			t.Fatalf("base node did not execute proposed instance %s: %#v", proposal.ref, base)
		}
		baseRecord, ok := s.stores[s.ids[0]].Instance(proposal.ref)
		if !ok || baseRecord.Status < StatusCommitted {
			t.Fatalf("base node lacks durable committed record %s: %#v", proposal.ref, baseRecord)
		}
		for _, id := range s.ids[1:] {
			inst := s.nodes[id].instances[proposal.ref]
			if inst == nil || inst.rec.Status != StatusExecuted {
				t.Fatalf("node %d did not execute %s: %#v", id, proposal.ref, inst)
			}
			record, found := s.stores[id].Instance(proposal.ref)
			if !found || record.Status < StatusCommitted || !sameValueTuple(record, baseRecord) {
				t.Fatalf("node %d durable decision %#v for %s, want base tuple %#v", id, record, proposal.ref, baseRecord)
			}
		}
		switch {
		case commandEqual(baseRecord.Command, proposal.cmd):
			chosen = append(chosen, proposal)
		case baseRecord.Kind != EntryNoop:
			t.Fatalf("proposed instance %s chose unexpected command %#v instead of %#v or no-op", proposal.ref, baseRecord.Command, proposal.cmd)
		}
	}
	proposals = chosen
	wantByID := make(map[CommandID]stressProposal, len(proposals))
	for _, p := range proposals {
		wantByID[p.cmd.ID] = p
	}
	baseOrder := commandIndexes(s.apps[s.ids[0]])
	for _, id := range s.ids {
		app := s.apps[id]
		if len(app) != len(proposals) {
			t.Fatalf("node %d applied %d commands, want %d: %#v", id, len(app), len(proposals), app)
		}
		gotByID := make(map[CommandID]ApplyCommand, len(app))
		for _, c := range app {
			if _, ok := gotByID[c.Command.ID]; ok {
				t.Fatalf("node %d applied command id %#v more than once", id, c.Command.ID)
			}
			want, ok := wantByID[c.Command.ID]
			if !ok {
				t.Fatalf("node %d applied unexpected command id %#v", id, c.Command.ID)
			}
			if c.Ref != want.ref {
				t.Fatalf("node %d command %#v ref = %s, want %s", id, c.Command.ID, c.Ref, want.ref)
			}
			if !sameCommandBytes(c.Command, want.cmd) {
				t.Fatalf("node %d command %#v bytes = %#v, want %#v", id, c.Command.ID, c.Command, want.cmd)
			}
			gotByID[c.Command.ID] = c
		}
		status := s.nodes[id].Status()
		if !sameExecutedRefs(status.Executed, resolvedRefs) {
			t.Fatalf("node %d executed refs = %v, want all resolved refs %v", id, status.Executed, resolvedRefs)
		}
		for _, p := range proposals {
			stored, ok := s.stores[id].Instance(p.ref)
			if !ok {
				t.Fatalf("node %d missing durable record %s", id, p.ref)
			}
			if stored.Status < StatusCommitted || !sameCommandBytes(stored.Command, p.cmd) {
				t.Fatalf("node %d durable record for %s = %#v, want chosen command %#v", id, p.ref, stored, p.cmd)
			}
		}
		order := commandIndexes(app)
		for i := range len(proposals) {
			for j := i + 1; j < len(proposals); j++ {
				left := proposals[i].cmd
				right := proposals[j].cmd
				if !left.ConflictsWith(right) {
					continue
				}
				baseLess := baseOrder[left.ID] < baseOrder[right.ID]
				gotLess := order[left.ID] < order[right.ID]
				if gotLess != baseLess {
					t.Fatalf("node %d conflict order for %#v and %#v differs from node %d: node positions %d/%d, base positions %d/%d, final tuples=%#v", id, left.ID, right.ID, s.ids[0], order[left.ID], order[right.ID], baseOrder[left.ID], baseOrder[right.ID], stressTuplePairs(s, wantByID[left.ID].ref, wantByID[right.ID].ref))
				}
			}
		}
	}
}

type stressTuple struct {
	Ref  InstanceRef
	ID   CommandID
	Seq  uint64
	Deps []InstanceNum
}

func stressTuplePairs(s *stressTransportCluster, left, right InstanceRef) map[ReplicaID][2]stressTuple {
	pairs := make(map[ReplicaID][2]stressTuple, len(s.ids))
	for _, id := range s.ids {
		leftRecord := s.nodes[id].instances[left].rec
		rightRecord := s.nodes[id].instances[right].rec
		pairs[id] = [2]stressTuple{
			{Ref: leftRecord.Ref, ID: leftRecord.Command.ID, Seq: leftRecord.Seq, Deps: append([]InstanceNum(nil), leftRecord.Deps...)},
			{Ref: rightRecord.Ref, ID: rightRecord.Command.ID, Seq: rightRecord.Seq, Deps: append([]InstanceNum(nil), rightRecord.Deps...)},
		}
	}
	return pairs
}

func commandIndexes(app []ApplyCommand) map[CommandID]int {
	idx := make(map[CommandID]int, len(app))
	for i, c := range app {
		idx[c.Command.ID] = i
	}
	return idx
}

func sameCommandBytes(got, want Command) bool {
	if got.ID != want.ID || !bytes.Equal(got.Payload, want.Payload) || !footprintEqual(got.Footprint, want.Footprint) || !bytes.Equal(got.CycleKey, want.CycleKey) {
		return false
	}
	for i := range got.Footprint.Points {
		if !bytes.Equal(got.Footprint.Points[i], want.Footprint.Points[i]) {
			return false
		}
	}
	return true
}

func sameExecutedRefs(got, want []InstanceRef) bool {
	if len(got) != len(want) {
		return false
	}
	got = append([]InstanceRef(nil), got...)
	want = append([]InstanceRef(nil), want...)
	sortRefs(got)
	sortRefs(want)
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
