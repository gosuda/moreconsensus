package epaxos

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"testing"
)

type simCluster struct {
	t       *testing.T
	nodes   map[ReplicaID]*RawNode
	stores  map[ReplicaID]*MemoryStorage
	apps    map[ReplicaID][]CommittedCommand
	drop    map[[2]ReplicaID]bool
	paused  map[ReplicaID]bool
	delayed []Message
	opt     bool
}

func newSimCluster(t *testing.T, n int, opt bool) *simCluster {
	t.Helper()
	ids := makeIDs(n)
	s := &simCluster{t: t, nodes: make(map[ReplicaID]*RawNode), stores: make(map[ReplicaID]*MemoryStorage), apps: make(map[ReplicaID][]CommittedCommand), drop: make(map[[2]ReplicaID]bool), paused: make(map[ReplicaID]bool), opt: opt}
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

func newCertifiedBootstrapSimCluster(t *testing.T, voters int) (*simCluster, ClusterID, []VoterIdentity, []ed25519.PrivateKey) {
	t.Helper()
	ids := makeIDs(voters)
	cluster := ClusterID{0x51, byte(voters)}
	identities := make([]VoterIdentity, voters+1)
	privateKeys := make([]ed25519.PrivateKey, voters+1)
	for i := range identities {
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = byte(i + 41)
		privateKeys[i] = ed25519.NewKeyFromSeed(seed)
		identities[i] = VoterIdentity{
			Replica: ReplicaID(i + 1), Incarnation: 1,
			VerifyKey: append([]byte(nil), privateKeys[i].Public().(ed25519.PublicKey)...),
		}
	}
	s := &simCluster{
		t: t, nodes: make(map[ReplicaID]*RawNode), stores: make(map[ReplicaID]*MemoryStorage),
		apps: make(map[ReplicaID][]CommittedCommand), drop: make(map[[2]ReplicaID]bool),
		paused: make(map[ReplicaID]bool),
	}
	base := ConfState{ID: 1, Voters: ids}
	for _, id := range ids {
		store := NewMemoryStorage()
		store.Hard = HardState{Conf: base.Clone()}
		store.ConfigHistory = []ConfigHistoryEntry{{Conf: base.Clone()}}
		store.AllocatorFloor = 1
		store.LocalVoterState = LocalVoterState{
			Cluster: cluster, Identity: identities[id-1].Clone(), Conf: base.Clone(),
			Status: LocalVoterStatusEligible, AllocatorFloor: 1,
		}
		node, err := NewRawNode(Config{
			ID: id, Voters: ids, Cluster: cluster, LocalIdentity: identities[id-1],
			VoterIdentities:     cloneVoterIdentities(identities[:voters]),
			BootstrapPrivateKey: privateKeys[id-1], Storage: store,
			RetryTicks: 2, RecoveryTicks: 5,
		})
		if err != nil {
			t.Fatalf("new certified node %d: %v", id, err)
		}
		s.nodes[id] = node
		s.stores[id] = store
	}
	return s, cluster, identities, privateKeys
}

func persistBootstrapSimNode(t *testing.T, s *simCluster, id ReplicaID) []BootstrapMessage {
	t.Helper()
	var messages []BootstrapMessage
	for attempts := 0; attempts < 16 && s.nodes[id].HasReady(); attempts++ {
		rd := s.nodes[id].Ready()
		if len(rd.Messages) != 0 || len(rd.Committed) != 0 {
			t.Fatalf("bootstrap persistence for node %d unexpectedly contained EPaxos output: %#v", id, rd)
		}
		if err := s.stores[id].ApplyReady(rd); err != nil {
			t.Fatalf("persist bootstrap node %d: %v", id, err)
		}
		for _, message := range rd.BootstrapMessages {
			messages = append(messages, message.Clone())
		}
		if err := s.nodes[id].Advance(rd); err != nil {
			t.Fatalf("advance bootstrap node %d: %v", id, err)
		}
	}
	if s.nodes[id].HasReady() {
		t.Fatalf("bootstrap persistence for node %d did not quiesce", id)
	}
	return messages
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
		if len(s.delayed) != 0 {
			blocked := s.delayed[:0]
			for _, m := range s.delayed {
				if s.deliver(m) {
					progress = true
				} else {
					blocked = append(blocked, m)
				}
			}
			s.delayed = blocked
		}
		for _, id := range s.ids() {
			if s.paused[id] {
				continue
			}
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
				if !s.deliver(m) {
					s.delayed = append(s.delayed, m)
				}
			}
			if err := rn.Advance(rd); err != nil {
				s.t.Fatalf("advance %d: %v", id, err)
			}
		}
		if !progress {
			return
		}
	}
	s.t.Fatalf("simulation did not quiesce")
}

func (s *simCluster) deliver(m Message) bool {
	s.t.Helper()
	if s.drop[[2]ReplicaID{m.From, m.To}] {
		return true
	}
	if s.paused[m.From] || s.paused[m.To] {
		return false
	}
	if to := s.nodes[m.To]; to != nil {
		if err := to.Step(m); err != nil && !errors.Is(err, ErrMessageRejected) {
			s.t.Fatalf("step %s %d->%d: %v", m.Type, m.From, m.To, err)
		}
	}
	return true
}

func (s *simCluster) tickAll(n int) {
	for range n {
		for _, id := range s.ids() {
			if !s.paused[id] {
				s.nodes[id].Tick()
			}
		}
		s.drain()
	}
}

func (s *simCluster) tickOnly(id ReplicaID, n int) {
	s.t.Helper()
	if s.paused[id] {
		s.t.Fatalf("tickOnly called on paused node %d", id)
	}
	for range n {
		s.nodes[id].Tick()
		s.drain()
	}
}

func (s *simCluster) tickBurst(id ReplicaID, n int) {
	s.t.Helper()
	if s.paused[id] {
		s.t.Fatalf("tickBurst called on paused node %d", id)
	}
	for range n {
		s.nodes[id].Tick()
	}
	s.drain()
}

func (s *simCluster) pause(id ReplicaID) {
	s.paused[id] = true
}

func (s *simCluster) resume(id ReplicaID) {
	delete(s.paused, id)
}

func (s *simCluster) omit(id ReplicaID) {
	for _, other := range s.ids() {
		if other == id {
			continue
		}
		s.drop[[2]ReplicaID{id, other}] = true
		s.drop[[2]ReplicaID{other, id}] = true
	}
}

func (s *simCluster) heal(id ReplicaID) {
	for _, other := range s.ids() {
		delete(s.drop, [2]ReplicaID{id, other})
		delete(s.drop, [2]ReplicaID{other, id})
	}
}

func (s *simCluster) restart(id ReplicaID, st *MemoryStorage) {
	s.t.Helper()
	s.stores[id] = st
	rn, err := NewRawNode(Config{ID: id, Voters: makeIDs(len(s.nodes)), Storage: st, RetryTicks: 2, RecoveryTicks: 5, TimeOptimization: s.opt, TimeOptimizationTicks: 1})
	if err != nil {
		s.t.Fatalf("restart node %d: %v", id, err)
	}
	s.nodes[id] = rn
}

func cloneMemoryStorage(st *MemoryStorage) *MemoryStorage {
	out := &MemoryStorage{
		Hard:       HardState{Conf: st.Hard.Conf.Clone(), Tick: st.Hard.Tick},
		Configs:    make([]ConfState, len(st.Configs)),
		Records:    make(map[InstanceRef]InstanceRecord, len(st.Records)),
		FailWrites: st.FailWrites,
	}
	for i := range st.Configs {
		out.Configs[i] = st.Configs[i].Clone()
	}
	for ref, rec := range st.Records {
		out.Records[ref] = rec.Clone()
	}
	return out
}

func cloneCommittedCommands(cmds []CommittedCommand) []CommittedCommand {
	out := make([]CommittedCommand, len(cmds))
	for i := range cmds {
		out[i] = cmds[i].Clone()
	}
	return out
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
	if err := s.nodes[1].Advance(rd); err != nil {
		t.Fatal(err)
	}
	var decoded Message
	if err := DecodeMessage([]byte{1, 2, 3}, &decoded); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("malformed decode err=%v", err)
	}
}

func TestDuplicateInboundPreAcceptAndAcceptDoNotQueueDuplicateRecords(t *testing.T) {
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	command := Command{ID: CommandID{Client: 10, Sequence: 20}, Payload: []byte("duplicate-inbound"), ConflictKeys: [][]byte{[]byte("duplicate-key")}}
	tests := []struct {
		name       string
		msg        Message
		wantStatus Status
	}{
		{
			name: "preaccept",
			msg: Message{
				Type:    MsgPreAccept,
				From:    1,
				To:      2,
				Ref:     ref,
				Ballot:  Ballot{Replica: 1},
				Seq:     1,
				Deps:    []InstanceNum{0, 0, 0},
				Command: command,
			},
			wantStatus: StatusPreAccepted,
		},
		{
			name: "accept",
			msg: Message{
				Type:    MsgAccept,
				From:    1,
				To:      2,
				Ref:     ref,
				Ballot:  Ballot{Replica: 1},
				Seq:     1,
				Deps:    []InstanceNum{0, 0, 0},
				Command: command,
			},
			wantStatus: StatusAccepted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store})
			if err != nil {
				t.Fatal(err)
			}
			tt.msg.Checksum = ChecksumMessage(tt.msg)

			if err := rn.Step(tt.msg); err != nil {
				t.Fatalf("first Step(%s) err=%v", tt.msg.Type, err)
			}
			first := rn.Ready()
			if len(first.Records) != 1 || first.Records[0].Ref != ref || first.Records[0].Status != tt.wantStatus {
				t.Fatalf("first duplicate target ready records = %#v, want exactly one %s record for %s", first.Records, tt.wantStatus, ref)
			}
			if err := store.ApplyReady(first); err != nil {
				t.Fatal(err)
			}
			advanceOK(t, rn, first)

			if err := rn.Step(tt.msg); err != nil {
				t.Fatalf("duplicate Step(%s) err=%v", tt.msg.Type, err)
			}
			duplicate := rn.Ready()
			if len(duplicate.Records) != 0 {
				t.Fatalf("duplicate %s ready records = %#v, want no durable records for unchanged %s", tt.msg.Type, duplicate.Records, ref)
			}
			if !duplicate.Empty() {
				if err := store.ApplyReady(duplicate); err != nil {
					t.Fatal(err)
				}
				advanceOK(t, rn, duplicate)
			}
		})
	}
}

func TestCodecChecksumZeroCopy(t *testing.T) {
	m := Message{Type: MsgCommit, From: 1,
		To:           2,
		Ref:          InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:       Ballot{Replica: 1},
		RecordBallot: Ballot{Replica: 1},
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		Command:      Command{ID: CommandID{Client: 7, Sequence: 9}, Payload: []byte("payload-alpha"), ConflictKeys: [][]byte{[]byte("conflict-key-beta")}}}
	buf, err := EncodeMessage(make([]byte, 0, 128), m)
	if err != nil {
		t.Fatal(err)
	}
	var out Message
	if err := DecodeMessage(buf, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Command.Payload, []byte("payload-alpha")) {
		t.Fatal("payload mismatch")
	}
	payloadOffset := bytes.Index(buf, []byte("payload-alpha"))
	if payloadOffset < 0 {
		t.Fatal("encoded payload bytes not found")
	}
	keyOffset := bytes.Index(buf, []byte("conflict-key-beta"))
	if keyOffset < 0 {
		t.Fatal("encoded conflict key bytes not found")
	}
	buf[payloadOffset] = 'P'
	buf[keyOffset] = 'C'
	if !bytes.Equal(out.Command.Payload, []byte("Payload-alpha")) {
		t.Fatalf("decoded payload does not alias encoded buffer: %q", out.Command.Payload)
	}
	if len(out.Command.ConflictKeys) != 1 || !bytes.Equal(out.Command.ConflictKeys[0], []byte("Conflict-key-beta")) {
		t.Fatalf("decoded conflict key does not alias encoded buffer: %q", out.Command.ConflictKeys)
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
	s, cluster, identities, privateKeys := newCertifiedBootstrapSimCluster(t, 3)
	target := identities[3].Clone()
	request := PrepareVoterRequest{
		Cluster: cluster, Plan: BootstrapID{0x73, 0x69, 0x6d}, Base: s.nodes[1].Status().Conf,
		OldVoters: cloneVoterIdentities(identities[:3]), Target: target, Source: 1,
		SourceDigest: StateDigest{1}, ReleaseDigest: StateDigest{2},
		TargetAllocatorFloor: 1, TimingDigest: StateDigest{3},
	}
	plan, err := s.nodes[1].PrepareVoter(request)
	if err != nil {
		t.Fatal(err)
	}
	if s.nodes[1].Status().Conf.Contains(target.Replica) {
		t.Fatal("prepared target became a voter")
	}
	s.drain()

	sealAcks := make([]BootstrapMessage, 0, 3)
	for _, id := range []ReplicaID{1, 2, 3} {
		if err := s.nodes[id].BeginVoterSeal(plan); err != nil {
			t.Fatalf("BeginVoterSeal node %d: %v", id, err)
		}
		sealAcks = append(sealAcks, persistBootstrapSimNode(t, s, id)...)
	}
	for _, ack := range sealAcks[:slowQuorumSize(len(plan.Request.Base.Voters))] {
		if err := s.nodes[1].StepBootstrap(ack); err != nil {
			t.Fatalf("SealAck from %d: %v", ack.From, err)
		}
	}
	seal := s.nodes[1].BootstrapStatus().Plans[0].SealCertificate
	if VerifySealCertificate(plan, seal) != nil {
		t.Fatal("owner did not form a valid seal certificate")
	}
	persistBootstrapSimNode(t, s, 1)
	for _, id := range []ReplicaID{2, 3} {
		if err := s.nodes[id].ApplySealCertificate(seal); err != nil {
			t.Fatalf("ApplySealCertificate node %d: %v", id, err)
		}
		persistBootstrapSimNode(t, s, id)
	}
	for _, id := range []ReplicaID{1, 2, 3} {
		if closure := s.nodes[id].BootstrapClosure(plan); !closure.Complete {
			t.Fatalf("node %d closure=%#v", id, closure)
		}
	}

	descriptor := standaloneSnapshotDescriptor(plan, seal)
	descriptorDigest := DigestSnapshotDescriptor(descriptor)
	attestations := make([]VoterAttestation, 2)
	for i := range attestations {
		attestations[i], err = SignVoterAttestation(descriptorDigest, identities[i], privateKeys[i])
		if err != nil {
			t.Fatal(err)
		}
		payload, marshalErr := marshalBootstrapCanonical(snapshotVotePayload{Descriptor: descriptor, Attestation: attestations[i]})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		message, signErr := SignBootstrapMessage(BootstrapMessage{
			Type: BootstrapMsgSnapshotVote, Cluster: cluster, Plan: plan.Request.Plan,
			From: identities[i].Replica, FromIncarnation: identities[i].Incarnation, To: 1,
			BaseID: plan.Request.Base.ID, BaseDigest: plan.RequestDigest, Payload: payload,
		}, identities[i], privateKeys[i])
		if signErr != nil {
			t.Fatal(signErr)
		}
		if err := s.nodes[1].StepBootstrap(message); err != nil {
			t.Fatalf("SnapshotVote from %d: %v", message.From, err)
		}
	}
	persistBootstrapSimNode(t, s, 1)
	snapshot := s.nodes[1].BootstrapStatus().Plans[0].SnapshotCertificate
	if VerifySnapshotCertificate(plan, snapshot) != nil {
		t.Fatal("owner did not durably retain a valid snapshot certificate")
	}

	proof, err := SignVoterReadyProof(VoterReadyProof{
		Cluster: cluster, Plan: plan.Request.Plan, Target: target,
		SnapshotDigest: snapshot.Digest, InstalledStateDigest: descriptor.InstalledStateDigest,
		AllocatorFloor: descriptor.TargetAllocatorFloor, TOQClosedThrough: descriptor.TOQClosedThrough,
	}, privateKeys[3])
	if err != nil {
		t.Fatal(err)
	}
	targetStore := NewMemoryStorage()
	targetStore.Hard = HardState{Conf: plan.Request.Base.Clone()}
	targetStore.ConfigHistory = []ConfigHistoryEntry{{Conf: plan.Request.Base.Clone()}}
	targetStore.AllocatorFloor = proof.AllocatorFloor
	targetStore.LocalVoterState = LocalVoterState{
		Cluster: cluster, Identity: target.Clone(), Conf: plan.Request.Base.Clone(),
		Status: LocalVoterStatusStaged, Plan: plan.Request.Plan, AllocatorFloor: proof.AllocatorFloor,
	}
	if _, err := NewRawNode(Config{
		ID: target.Replica, Voters: plan.Request.Base.Voters, Cluster: cluster,
		LocalIdentity: target, VoterIdentities: cloneVoterIdentities(identities[:3]),
		BootstrapPrivateKey: privateKeys[3], Storage: targetStore,
	}); !errors.Is(err, ErrBootstrapEligibility) {
		t.Fatalf("staged target startup err=%v, want ErrBootstrapEligibility", err)
	}

	readyAcks := make([]BootstrapMessage, 0, 2)
	installPayload, err := marshalBootstrapCanonical(installProofPayload{Snapshot: snapshot, Proof: proof})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []ReplicaID{1, 2} {
		message, signErr := SignBootstrapMessage(BootstrapMessage{
			Type: BootstrapMsgInstallProof, Cluster: cluster, Plan: plan.Request.Plan,
			From: target.Replica, FromIncarnation: target.Incarnation, To: id,
			BaseID: plan.Request.Base.ID, BaseDigest: plan.RequestDigest, Payload: installPayload,
		}, target, privateKeys[3])
		if signErr != nil {
			t.Fatal(signErr)
		}
		if err := s.nodes[id].StepBootstrap(message); err != nil {
			t.Fatalf("InstallProof node %d: %v", id, err)
		}
		readyAcks = append(readyAcks, persistBootstrapSimNode(t, s, id)...)
	}
	for _, ack := range readyAcks {
		if err := s.nodes[1].StepBootstrap(ack); err != nil {
			t.Fatalf("ReadyAck from %d: %v", ack.From, err)
		}
	}
	persistBootstrapSimNode(t, s, 1)
	readyCertificate := s.nodes[1].BootstrapStatus().Plans[0].ReadyCertificate
	if VerifyReadyCertificate(plan, snapshot, readyCertificate) != nil {
		t.Fatal("owner did not durably retain a valid ready certificate")
	}

	if _, err := s.nodes[1].ActivateVoter(plan, snapshot, readyCertificate); err != nil {
		t.Fatal(err)
	}
	if targetStore.LocalVoterState.Status != LocalVoterStatusStaged {
		t.Fatal("target became eligible before certified activation was durable")
	}
	s.drain()
	for _, id := range []ReplicaID{1, 2, 3} {
		if !confStateEqual(s.nodes[id].Status().Conf, plan.Successor) {
			t.Fatalf("node %d config=%#v, want certified successor %#v", id, s.nodes[id].Status().Conf, plan.Successor)
		}
	}

	activated := s.nodes[1].BootstrapStatus().Plans[0]
	local := LocalVoterState{
		Cluster: cluster, Identity: target.Clone(), Conf: plan.Successor.Clone(),
		Status: LocalVoterStatusEligible, Plan: plan.Request.Plan,
		InstalledDigest: descriptor.InstalledStateDigest, AllocatorFloor: proof.AllocatorFloor,
		TOQClosedThrough: proof.TOQClosedThrough,
	}
	targetActivation := Ready{
		HardState: HardState{Conf: plan.Successor.Clone()},
		ConfigHistory: []ConfigHistoryEntry{{
			Conf: plan.Successor.Clone(), AppliedRef: plan.Reservations.Activate,
			IdentityDigest: bootstrapIdentityDigest(plan),
		}},
		BootstrapRecords: []BootstrapRecord{activated}, LocalVoterState: &local,
		AllocatorFloor: proof.AllocatorFloor, MustSync: true,
	}
	if err := targetStore.ApplyReady(targetActivation); err != nil {
		t.Fatalf("persist target activation: %v", err)
	}
	targetNode, err := NewRawNode(Config{
		ID: target.Replica, Voters: plan.Request.Base.Voters, Cluster: cluster,
		LocalIdentity: target, VoterIdentities: cloneVoterIdentities(identities),
		BootstrapPrivateKey: privateKeys[3], Storage: targetStore,
	})
	if err != nil {
		t.Fatalf("start durably activated target: %v", err)
	}
	targetRef, err := targetNode.Propose(Command{Payload: []byte("target-after-activation"), ConflictKeys: [][]byte{[]byte("target")}})
	if err != nil || targetRef.Conf != plan.Successor.ID {
		t.Fatalf("activated target proposal ref=%s err=%v", targetRef, err)
	}

	m := GetMessage()
	m.Type = MsgCommit
	PutMessage(m)
	c := GetCommand()
	c.Payload = []byte("owned")
	PutCommand(c)
}

func TestQuorumTables(t *testing.T) {
	tests := []struct {
		n          int
		slow       int
		fast       int
		tryWitness int
	}{
		{n: 1, slow: 1, fast: 1, tryWitness: 1},
		{n: 2, slow: 2, fast: 2, tryWitness: 2},
		{n: 3, slow: 2, fast: 2, tryWitness: 1},
		{n: 4, slow: 3, fast: 4, tryWitness: 3},
		{n: 5, slow: 3, fast: 3, tryWitness: 1},
		{n: 6, slow: 4, fast: 5, tryWitness: 3},
		{n: 7, slow: 4, fast: 5, tryWitness: 2},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("n=%d", tt.n), func(t *testing.T) {
			slow, err := SlowQuorum(tt.n)
			if err != nil {
				t.Fatal(err)
			}
			fast, err := FastQuorum(tt.n)
			if err != nil {
				t.Fatal(err)
			}
			tryWitness, err := TryWitnessQuorum(tt.n)
			if err != nil {
				t.Fatal(err)
			}
			q, err := newQuorum(makeIDs(tt.n))
			if err != nil {
				t.Fatal(err)
			}
			if slow != tt.slow || fast != tt.fast || tryWitness != tt.tryWitness || q.tryWitnessQuorum() != tt.tryWitness {
				t.Fatalf("n=%d slow=%d fast=%d tryWitness=%d q.tryWitness=%d, want slow=%d fast=%d tryWitness=%d",
					tt.n, slow, fast, tryWitness, q.tryWitnessQuorum(), tt.slow, tt.fast, tt.tryWitness)
			}
			if got := fast + slow - tt.n; got != tryWitness {
				t.Fatalf("n=%d fast/slow intersection=%d, want try witness quorum %d", tt.n, got, tryWitness)
			}
		})
	}
	for _, fn := range []struct {
		name string
		call func(int) (int, error)
	}{
		{name: "slow", call: SlowQuorum},
		{name: "fast", call: FastQuorum},
		{name: "tryWitness", call: TryWitnessQuorum},
	} {
		for _, n := range []int{0, 8} {
			if _, err := fn.call(n); err == nil {
				t.Fatalf("%s quorum accepted invalid cluster size %d", fn.name, n)
			}
		}
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
	rn.installInstance(&instance{rec: InstanceRecord{Ref: a, Status: StatusCommitted, Seq: 7, Deps: []InstanceNum{1, 0, 0}, Command: Command{Payload: []byte("a")}}})
	rn.installInstance(&instance{rec: InstanceRecord{Ref: b, Status: StatusCommitted, Seq: 7, Deps: []InstanceNum{0, 1, 0}, Command: Command{Payload: []byte("b")}}})
	rn.installInstance(&instance{rec: InstanceRecord{Ref: c, Status: StatusCommitted, Seq: 7, Deps: []InstanceNum{0, 0, 1}, Command: Command{Payload: []byte("c")}}})

	view := rn.newExecutionView()
	comps := rn.executionComponents(&view)
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

func TestExecutionComponentsWaitForInactiveDependencyRefs(t *testing.T) {
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
			rn.installInstance(&instance{rec: InstanceRecord{Ref: x, Status: StatusCommitted, Seq: 2, Deps: []InstanceNum{0, 1, 0}, Command: Command{Payload: []byte("x")}}})
			if tt.dep != nil {
				rn.installInstance(tt.dep)
			}

			view := rn.newExecutionView()
			comps := rn.executionComponents(&view)
			if len(comps) != 1 || len(comps[0]) != 1 || comps[0][0] != x {
				t.Fatalf("component with inactive dependency = %#v, want unresolved %s component", comps, x)
			}
			var candidates []recoveryCandidate
			if rn.componentReady(&view, comps[0], &candidates) {
				t.Fatalf("component with inactive dependency %s was ready: %#v", y, comps)
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

func TestRemoveVoterConfChangeKeepsOldInFlightInstancePinned(t *testing.T) {
	s := newSimCluster(t, 4, false)
	oldCmd := Command{ID: CommandID{Client: 70, Sequence: 1}, Payload: []byte("old-inflight-across-removal"), ConflictKeys: [][]byte{[]byte("old-inflight-across-removal")}}
	oldRef, err := s.nodes[1].Propose(oldCmd)
	if err != nil {
		t.Fatal(err)
	}
	if oldRef.Conf != 1 {
		t.Fatalf("old proposal used config %d, want 1", oldRef.Conf)
	}
	oldReady := s.nodes[1].Ready()
	if len(oldReady.Committed) != 0 {
		t.Fatalf("old proposal committed before any quorum response: %#v", oldReady.Committed)
	}
	var oldPreAccepts []Message
	foundOldRecord := false
	for _, rec := range oldReady.Records {
		if rec.Ref != oldRef {
			continue
		}
		foundOldRecord = true
		if len(rec.Deps) != 4 {
			t.Fatalf("old record deps width = %d, want old config width 4: %#v", len(rec.Deps), rec)
		}
	}
	if !foundOldRecord {
		t.Fatalf("ready did not contain old record %s: %#v", oldRef, oldReady.Records)
	}
	for _, m := range oldReady.Messages {
		if m.Ref == oldRef && m.Type == MsgPreAccept {
			oldPreAccepts = append(oldPreAccepts, m.Clone())
		}
	}
	if len(oldPreAccepts) != 3 {
		t.Fatalf("old proposal sent %d pre-accepts, want one per remote old voter: %#v", len(oldPreAccepts), oldReady.Messages)
	}
	if err := s.stores[1].ApplyReady(oldReady); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, s.nodes[1], oldReady)

	s.pause(1)
	confRef, err := s.nodes[2].ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 4})
	if err != nil {
		t.Fatal(err)
	}
	if confRef.Conf != 1 {
		t.Fatalf("config change used config %d, want 1", confRef.Conf)
	}
	s.drain()
	s.tickAll(2)
	for _, id := range []ReplicaID{2, 3, 4} {
		conf := s.nodes[id].Status().Conf
		if conf.ID != 2 || conf.Contains(4) || !conf.Contains(1) || !conf.Contains(2) || !conf.Contains(3) {
			t.Fatalf("node %d config after voter removal = %#v", id, conf)
		}
		requireCommittedPayloadCount(t, s.apps[id], id, oldRef, oldCmd.Payload, 0)
	}

	var delayedRemoval []Message
	for _, message := range s.delayed {
		if message.Ref == confRef {
			delayedRemoval = append(delayedRemoval, message.Clone())
		}
	}
	if len(delayedRemoval) == 0 {
		t.Fatalf("paused node had no delayed removal traffic; delayed=%#v", s.delayed)
	}

	s.resume(1)
	for _, m := range oldPreAccepts {
		if !s.deliver(m) {
			t.Fatalf("saved old pre-accept to node %d was unexpectedly blocked", m.To)
		}
	}
	s.drain()

	for _, id := range s.ids() {
		conf := s.nodes[id].Status().Conf
		if conf.ID != 2 || conf.Contains(4) || !conf.Contains(1) || !conf.Contains(2) || !conf.Contains(3) {
			t.Fatalf("node %d config after replaying delayed removal = %#v; delayed removal=%#v stored removal=%#v stored old=%#v", id, conf, delayedRemoval, s.stores[id].Records[confRef], s.stores[id].Records[oldRef])
		}
		oldRec, ok := s.stores[id].Records[oldRef]
		if !ok {
			t.Fatalf("node %d did not retain old instance %s", id, oldRef)
		}
		if oldRec.Ref.Conf != 1 || len(oldRec.Deps) != 4 {
			t.Fatalf("node %d old record after removal = %#v, want config 1 deps width 4", id, oldRec)
		}
		requireCommittedPayloadCount(t, s.apps[id], id, oldRef, oldCmd.Payload, 1)
	}

	removedCmd := Command{ID: CommandID{Client: 70, Sequence: 3}, Payload: []byte("removed-node-proposal"), ConflictKeys: [][]byte{[]byte("removed-node-proposal")}}
	if _, err := s.nodes[4].Propose(removedCmd); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("removed voter Propose error = %v, want %v", err, ErrMessageRejected)
	}
	if _, err := s.nodes[4].ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4}); !errors.Is(err, ErrVoterCertificateRequired) {
		t.Fatalf("removed voter ProposeConfChange error = %v, want %v", err, ErrVoterCertificateRequired)
	}
	if s.nodes[4].HasReady() {
		t.Fatal("removed voter proposal rejection created Ready work")
	}

	newCmd := Command{ID: CommandID{Client: 70, Sequence: 2}, Payload: []byte("new-after-removal"), ConflictKeys: [][]byte{[]byte("new-after-removal")}}
	newRef, err := s.nodes[2].Propose(newCmd)
	if err != nil {
		t.Fatal(err)
	}
	if newRef.Conf != 2 {
		t.Fatalf("new proposal used config %d, want 2", newRef.Conf)
	}
	s.drain()
	newRec, ok := s.stores[2].Records[newRef]
	if !ok {
		t.Fatalf("node 2 did not store new instance %s", newRef)
	}
	if len(newRec.Deps) != 3 {
		t.Fatalf("new record deps width = %d, want new config width 3: %#v", len(newRec.Deps), newRec)
	}
	for _, id := range []ReplicaID{1, 2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, newRef, newCmd.Payload, 1)
	}
	requireCommittedPayloadCount(t, s.apps[4], 4, newRef, newCmd.Payload, 0)
	if rec, ok := s.stores[4].Records[newRef]; ok {
		t.Fatalf("removed voter stored new-config instance %s: %#v", newRef, rec)
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
	if !rn.HasReady() {
		t.Fatal("failed write hid outstanding ready before retry")
	}
	requireSameReady(t, rn.Ready(), rd)

	store.FailWrites = false
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(rd); err != nil {
		t.Fatal(err)
	}
	executedReady := rn.Ready()
	if len(executedReady.Committed) != 0 || !readyHasStatus(executedReady, ref, StatusExecuted) {
		t.Fatalf("executed ready for %s = %#v", ref, executedReady)
	}
	if err := store.ApplyReady(executedReady); err != nil {
		t.Fatal(err)
	}
	if err := rn.Advance(executedReady); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatal("node still has ready work after executed record")
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

func TestPausedSlowNodeQueuesDeliveryAndReadyUntilResume(t *testing.T) {
	s := newSimCluster(t, 3, false)
	s.pause(3)

	cmd := Command{ID: CommandID{Client: 10, Sequence: 1}, Payload: []byte("slow-node"), ConflictKeys: [][]byte{[]byte("slow-node")}}
	ref, err := s.nodes[1].Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(3)
	for _, id := range []ReplicaID{1, 2} {
		requireCommittedPayloadCount(t, s.apps[id], id, ref, cmd.Payload, 1)
	}
	requireCommittedPayloadCount(t, s.apps[3], 3, ref, cmd.Payload, 0)
	if len(s.delayed) == 0 {
		t.Fatal("paused node did not hold any in-flight messages")
	}

	s.resume(3)
	s.tickAll(6)
	for _, id := range s.ids() {
		requireCommittedPayloadCount(t, s.apps[id], id, ref, cmd.Payload, 1)
	}
	requireConvergedRefs(t, s)
}

func TestSingleNodeOmissionDropsInboundOutboundThenHealConverges(t *testing.T) {
	s := newSimCluster(t, 5, true)
	s.omit(5)

	cmd := Command{ID: CommandID{Client: 11, Sequence: 1}, Payload: []byte("omitted-node"), ConflictKeys: [][]byte{[]byte("omitted-node")}}
	ref, err := s.nodes[1].Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(3)
	for _, id := range []ReplicaID{1, 2, 3, 4} {
		requireCommittedPayloadCount(t, s.apps[id], id, ref, cmd.Payload, 1)
	}
	requireCommittedPayloadCount(t, s.apps[5], 5, ref, cmd.Payload, 0)

	s.heal(5)
	s.tickAll(6)
	for _, id := range s.ids() {
		requireCommittedPayloadCount(t, s.apps[id], id, ref, cmd.Payload, 1)
	}
	requireConvergedRefs(t, s)
}

func TestSustainedQuorumProgressWhileSingleNodeUnavailableThenCatchUp(t *testing.T) {
	tests := []struct {
		name        string
		unavailable func(*simCluster, ReplicaID)
		available   func(*simCluster, ReplicaID)
	}{
		{
			name:        "omitted",
			unavailable: (*simCluster).omit,
			available:   (*simCluster).heal,
		},
		{
			name:        "paused",
			unavailable: (*simCluster).pause,
			available:   (*simCluster).resume,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newSimCluster(t, 5, true)
			const slow ReplicaID = 5
			active := []ReplicaID{1, 2, 3, 4}
			tc.unavailable(s, slow)

			type proposal struct {
				ref InstanceRef
				cmd Command
			}
			proposals := make([]proposal, 0, 4)
			for i, proposer := range active {
				cmd := Command{
					ID:           CommandID{Client: uint64(proposer), Sequence: uint64(i + 1)},
					Payload:      []byte(fmt.Sprintf("%s-progress-%d", tc.name, i+1)),
					ConflictKeys: [][]byte{[]byte("sustained-progress")},
				}
				ref, err := s.nodes[proposer].Propose(cmd)
				if err != nil {
					t.Fatal(err)
				}
				proposals = append(proposals, proposal{ref: ref, cmd: cmd})
				s.drain()
				s.tickAll(2)
			}

			for _, p := range proposals {
				for _, id := range active {
					requireCommittedPayloadCount(t, s.apps[id], id, p.ref, p.cmd.Payload, 1)
				}
				requireCommittedPayloadCount(t, s.apps[slow], slow, p.ref, p.cmd.Payload, 0)
			}

			tc.available(s, slow)
			s.tickAll(12)
			for _, p := range proposals {
				for _, id := range s.ids() {
					requireCommittedPayloadCount(t, s.apps[id], id, p.ref, p.cmd.Payload, 1)
				}
			}
			requireConvergedRefs(t, s)
		})
	}
}

func TestPausedClockDoesNotTickOrProcessReadyUntilResume(t *testing.T) {
	s := newSimCluster(t, 3, false)
	s.pause(2)
	pausedTick := s.nodes[2].Status().Tick

	cmd := Command{ID: CommandID{Client: 12, Sequence: 1}, Payload: []byte("clock-pause"), ConflictKeys: [][]byte{[]byte("clock-pause")}}
	ref, err := s.nodes[1].Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	for range 7 {
		s.nodes[1].Tick()
		s.nodes[3].Tick()
		s.drain()
	}
	if got := s.nodes[2].Status().Tick; got != pausedTick {
		t.Fatalf("paused node tick advanced from %d to %d", pausedTick, got)
	}
	for _, id := range []ReplicaID{1, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, ref, cmd.Payload, 1)
	}
	requireCommittedPayloadCount(t, s.apps[2], 2, ref, cmd.Payload, 0)

	s.resume(2)
	s.tickAll(6)
	for _, id := range s.ids() {
		requireCommittedPayloadCount(t, s.apps[id], id, ref, cmd.Payload, 1)
	}
	requireConvergedRefs(t, s)
}

func TestUnevenLogicalTickSkewAndBurstConvergesWithoutDuplicates(t *testing.T) {
	s := newSimCluster(t, 5, true)
	first := Command{ID: CommandID{Client: 13, Sequence: 1}, Payload: []byte("skew-a"), ConflictKeys: [][]byte{[]byte("skew")}}
	second := Command{ID: CommandID{Client: 14, Sequence: 1}, Payload: []byte("skew-b"), ConflictKeys: [][]byte{[]byte("skew")}}
	firstRef, err := s.nodes[1].Propose(first)
	if err != nil {
		t.Fatal(err)
	}
	secondRef, err := s.nodes[2].Propose(second)
	if err != nil {
		t.Fatal(err)
	}

	s.tickBurst(1, 5)
	s.tickOnly(3, 2)
	s.tickBurst(5, 8)
	s.tickAll(8)

	for _, id := range s.ids() {
		requireCommittedPayloadCount(t, s.apps[id], id, firstRef, first.Payload, 1)
		requireCommittedPayloadCount(t, s.apps[id], id, secondRef, second.Payload, 1)
	}
	requireConvergedRefs(t, s)
}

func TestRolledBackNodeCatchesUpFromQuorumWithoutDuplicateApply(t *testing.T) {
	s := newSimCluster(t, 3, false)
	first := Command{ID: CommandID{Client: 15, Sequence: 1}, Payload: []byte("before-rollback"), ConflictKeys: [][]byte{[]byte("rollback")}}
	firstRef, err := s.nodes[1].Propose(first)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	for _, id := range s.ids() {
		requireCommittedPayloadCount(t, s.apps[id], id, firstRef, first.Payload, 1)
	}
	rolledBackStore := cloneMemoryStorage(s.stores[3])

	s.omit(3)
	second := Command{ID: CommandID{Client: 15, Sequence: 2}, Payload: []byte("after-rollback"), ConflictKeys: [][]byte{[]byte("rollback")}}
	secondRef, err := s.nodes[1].Propose(second)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(3)
	for _, id := range []ReplicaID{1, 2} {
		requireCommittedPayloadCount(t, s.apps[id], id, secondRef, second.Payload, 1)
	}
	requireCommittedPayloadCount(t, s.apps[3], 3, secondRef, second.Payload, 0)

	s.restart(3, rolledBackStore)
	if s.nodes[3].HasReady() {
		t.Fatalf("rollback restart re-emitted already executed work: %#v", s.nodes[3].Ready())
	}
	s.heal(3)
	s.tickAll(6)
	for _, id := range s.ids() {
		requireCommittedPayloadCount(t, s.apps[id], id, firstRef, first.Payload, 1)
		requireCommittedPayloadCount(t, s.apps[id], id, secondRef, second.Payload, 1)
	}
	requireConvergedRefs(t, s)
}

func TestStorageWireRestartUpgradeRollbackSimulationConvergesWithoutDuplicateApply(t *testing.T) {
	s := newSimCluster(t, 5, false)
	rollbackID := ReplicaID(5)
	conflictKey := []byte("storage-wire-upgrade-rollback")
	var canaries []struct {
		ref InstanceRef
		cmd Command
	}
	nextSequence := uint64(1)

	proposeCanary := func(proposer ReplicaID, payload string) (InstanceRef, Command) {
		s.t.Helper()
		cmd := Command{
			ID:           CommandID{Client: 210, Sequence: nextSequence},
			Payload:      []byte(payload),
			ConflictKeys: [][]byte{conflictKey},
		}
		nextSequence++
		ref, err := s.nodes[proposer].Propose(cmd)
		if err != nil {
			s.t.Fatalf("propose %q from node %d: %v", payload, proposer, err)
		}
		return ref, cmd
	}
	remember := func(ref InstanceRef, cmd Command) {
		canaries = append(canaries, struct {
			ref InstanceRef
			cmd Command
		}{ref: ref, cmd: cmd})
	}
	liveProposer := func(restarting ReplicaID) ReplicaID {
		if restarting == 1 {
			return 2
		}
		return 1
	}
	requireAllCanariesOnceEverywhere := func() {
		s.t.Helper()
		for _, id := range s.ids() {
			requireNoDuplicateCommittedRefs(t, s.apps[id], id)
			for _, canary := range canaries {
				requireCommittedPayloadCount(t, s.apps[id], id, canary.ref, canary.cmd.Payload, 1)
			}
		}
		requireConvergedRefs(t, s)
	}

	beforeRef, beforeCmd := proposeCanary(1, "before-restart-checkpoint")
	s.tickAll(30)
	remember(beforeRef, beforeCmd)
	requireAllCanariesOnceEverywhere()
	rollbackStoreCheckpoint := cloneMemoryStorage(s.stores[rollbackID])
	rollbackAppCheckpoint := cloneCommittedCommands(s.apps[rollbackID])

	for _, restarting := range s.ids() {
		s.pause(restarting)
		ref, cmd := proposeCanary(liveProposer(restarting), fmt.Sprintf("during-restart-%d", restarting))
		s.tickAll(30)
		for _, id := range s.ids() {
			want := 1
			if id == restarting {
				want = 0
			}
			requireCommittedPayloadCount(t, s.apps[id], id, ref, cmd.Payload, want)
		}

		s.restart(restarting, cloneMemoryStorage(s.stores[restarting]))
		s.resume(restarting)
		s.tickAll(30)
		remember(ref, cmd)
		requireAllCanariesOnceEverywhere()
	}

	afterRef, afterCmd := proposeCanary(1, "after-rolling-restarts")
	s.tickAll(30)
	remember(afterRef, afterCmd)
	requireAllCanariesOnceEverywhere()

	s.pause(rollbackID)
	s.restart(rollbackID, cloneMemoryStorage(rollbackStoreCheckpoint))
	s.apps[rollbackID] = cloneCommittedCommands(rollbackAppCheckpoint)
	rollbackWindowRef, rollbackWindowCmd := proposeCanary(1, "during-storage-rollback")
	s.tickAll(30)
	for _, id := range s.ids() {
		want := 1
		if id == rollbackID {
			want = 0
		}
		requireCommittedPayloadCount(t, s.apps[id], id, rollbackWindowRef, rollbackWindowCmd.Payload, want)
	}

	s.resume(rollbackID)
	s.tickAll(30)
	remember(rollbackWindowRef, rollbackWindowCmd)
	requireAllCanariesOnceEverywhere()
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

func requireConvergedRefs(t *testing.T, s *simCluster) {
	t.Helper()
	want := refs(s.apps[1])
	for _, id := range s.ids() {
		if got := refs(s.apps[id]); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d refs = %v, want %v", id, got, want)
		}
	}
}

func requireCommittedPayloadCount(t *testing.T, app []CommittedCommand, id ReplicaID, ref InstanceRef, payload []byte, want int) {
	t.Helper()
	if got := committedPayloadCount(app, ref, payload); got != want {
		t.Fatalf("node %d applied %s with payload %q %d times, want %d: %#v", id, ref, payload, got, want, app)
	}
}

func requireNoDuplicateCommittedRefs(t *testing.T, app []CommittedCommand, id ReplicaID) {
	t.Helper()
	seen := make(map[InstanceRef]struct{}, len(app))
	for _, cmd := range app {
		if _, ok := seen[cmd.Ref]; ok {
			t.Fatalf("node %d applied %s more than once: %#v", id, cmd.Ref, app)
		}
		seen[cmd.Ref] = struct{}{}
	}
}

func committedPayloadCount(app []CommittedCommand, ref InstanceRef, payload []byte) int {
	var count int
	for _, c := range app {
		if c.Ref == ref && bytes.Equal(c.Command.Payload, payload) {
			count++
		}
	}
	return count
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

func TestIterativeExecutionComponentsMatchExplicitPrefixOracle(t *testing.T) {
	refs := []InstanceRef{
		{Conf: 1, Replica: 1, Instance: 1},
		{Conf: 1, Replica: 2, Instance: 1},
		{Conf: 1, Replica: 3, Instance: 1},
	}
	for mask := range 1 << 9 {
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
		if err != nil {
			t.Fatal(err)
		}
		for from, ref := range refs {
			deps := make([]InstanceNum, 3)
			for to := range refs {
				if mask&(1<<(from*3+to)) != 0 {
					deps[to] = 1
				}
			}
			rn.installInstance(&instance{rec: InstanceRecord{Ref: ref, Status: StatusCommitted, Seq: 1, Deps: deps, Command: Command{Kind: CommandNoop}}})
		}
		view := rn.newExecutionView()
		components := rn.executionComponents(&view)
		componentOf := make(map[InstanceRef]int, len(refs))
		for componentIndex, component := range components {
			for _, ref := range component {
				componentOf[ref] = componentIndex
			}
		}
		reachable := [3][3]bool{}
		for from := range refs {
			reachable[from][from] = true
			for to := range refs {
				if from != to && mask&(1<<(from*3+to)) != 0 {
					reachable[from][to] = true
				}
			}
		}
		for via := range refs {
			for from := range refs {
				for to := range refs {
					reachable[from][to] = reachable[from][to] || (reachable[from][via] && reachable[via][to])
				}
			}
		}
		for left := range refs {
			for right := range refs {
				wantSame := reachable[left][right] && reachable[right][left]
				gotSame := componentOf[refs[left]] == componentOf[refs[right]]
				if gotSame != wantSame {
					t.Fatalf("mask=%09b pair=%s/%s same-component=%v want %v components=%v", mask, refs[left], refs[right], gotSame, wantSame, components)
				}
			}
		}
	}
}

func TestSparseMaxPrefixBuildsMaterializedSCCButBlocksOnFirstHole(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	max := ^InstanceNum(0)
	left := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
	right := InstanceRef{Conf: 1, Replica: 2, Instance: max}
	rn.installInstance(&instance{rec: InstanceRecord{Ref: left, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{0, max, 0}, Command: Command{Kind: CommandNoop}}})
	rn.installInstance(&instance{rec: InstanceRecord{Ref: right, Status: StatusCommitted, Seq: 1, Deps: []InstanceNum{1, 0, 0}, Command: Command{Kind: CommandNoop}}})
	view := rn.newExecutionView()
	components := rn.executionComponents(&view)
	if len(components) != 1 || len(components[0]) != 2 {
		t.Fatalf("sparse Max materialized SCC = %v, want one two-member component", components)
	}
	var candidates []recoveryCandidate
	if rn.componentReady(&view, components[0], &candidates) {
		t.Fatal("sparse Max SCC crossed unknown lower prefix hole")
	}
	want := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
	found := false
	for _, candidate := range candidates {
		if candidate.ref == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("sparse Max SCC candidates = %v, want first exact blocker %s", candidates, want)
	}
}
