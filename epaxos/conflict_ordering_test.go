package epaxos

import (
	"errors"
	"testing"
)

func bootstrapOrderingUser(client uint64, payload string) Command {
	return Command{
		ID:           CommandID{Client: client, Sequence: 1},
		Payload:      []byte(payload),
		ConflictKeys: [][]byte{[]byte("bootstrap-order")},
	}
}

func TestMembershipUserOrderingAcrossFence(t *testing.T) {
	t.Run("user before membership", func(t *testing.T) {
		fixture := newBootstrapTestFixture(t, 1, 1)
		userRef, err := fixture.node.Propose(bootstrapOrderingUser(1, "user-before-membership"))
		if err != nil {
			t.Fatal(err)
		}
		persistBootstrapReady(t, fixture)

		plan, err := fixture.node.PrepareVoter(fixture.request())
		if err != nil {
			t.Fatal(err)
		}
		membership := fixture.node.instances[plan.Reservations.Prepare]
		if membership == nil {
			t.Fatalf("missing membership prepare instance %s", plan.Reservations.Prepare)
		}
		if !membership.rec.Command.ConflictsWith(fixture.node.instances[userRef].rec.Command) {
			t.Fatal("membership and user commands were not classified as conflicting")
		}
		if membership.rec.Deps[0] < userRef.Instance {
			t.Fatalf("membership deps=%v, want user instance %d", membership.rec.Deps, userRef.Instance)
		}
	})

	t.Run("membership before user", func(t *testing.T) {
		fixture := newBootstrapTestFixture(t, 1, 1)
		plan := prepareBootstrapPlan(t, fixture)
		membershipRef := plan.Reservations.Prepare

		userRef, err := fixture.node.Propose(bootstrapOrderingUser(2, "user-after-membership"))
		if err != nil {
			t.Fatal(err)
		}
		user := fixture.node.instances[userRef]
		if user == nil {
			t.Fatalf("missing user instance %s", userRef)
		}
		membership := fixture.node.instances[membershipRef]
		if membership == nil {
			t.Fatalf("missing membership instance %s", membershipRef)
		}
		if !user.rec.Command.ConflictsWith(membership.rec.Command) {
			t.Fatal("user and membership commands were not classified as conflicting")
		}
		if user.rec.Deps[0] < membershipRef.Instance {
			t.Fatalf("user deps=%v, want membership instance %d", user.rec.Deps, membershipRef.Instance)
		}
	})
}

func TestConflictIndexScopesConfigurationsAndExcludedLaneMaximum(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	conf2 := rn.q.conf.Clone()
	conf2.ID = 2
	rn.confHistory[conf2.ID] = conf2
	key := []byte("same-key")
	records := []InstanceRecord{
		{
			Ref:     InstanceRef{Replica: 2, Instance: 1, Conf: 1},
			Status:  StatusPreAccepted,
			Seq:     2,
			Deps:    rn.q.deps(),
			Command: Command{Payload: []byte("conf-one"), ConflictKeys: [][]byte{key}},
		},
		{
			Ref:     InstanceRef{Replica: 2, Instance: 1, Conf: 2},
			Status:  StatusPreAccepted,
			Seq:     3,
			Deps:    rn.q.deps(),
			Command: Command{Payload: []byte("conf-two-old"), ConflictKeys: [][]byte{key}},
		},
		{
			Ref:     InstanceRef{Replica: 2, Instance: 2, Conf: 2},
			Status:  StatusPreAccepted,
			Seq:     4,
			Deps:    rn.q.deps(),
			Command: Command{Payload: []byte("conf-two-latest"), ConflictKeys: [][]byte{key}},
		},
	}
	for _, rec := range records {
		rn.instances[rec.Ref] = &instance{rec: rec, phase: phasePreAccept}
		rn.indexConflicts(rec)
	}
	if got := rn.conflictIndex(1, key)[instanceLane{conf: 1, replica: 2}]; got != records[0].Ref {
		t.Fatalf("configuration 1 index=%s, want %s", got, records[0].Ref)
	}
	if got := rn.conflictIndex(2, key)[instanceLane{conf: 2, replica: 2}]; got != records[2].Ref {
		t.Fatalf("configuration 2 index=%s, want latest %s", got, records[2].Ref)
	}

	newCommand := Command{Payload: []byte("new"), ConflictKeys: [][]byte{key}}
	attrs := rn.computeAttrs(newCommand, InstanceRef{Replica: 1, Instance: 9, Conf: 2})
	if attrs.Deps[1] != records[2].Ref.Instance {
		t.Fatalf("configuration-scoped deps=%v, want configuration 2 latest instance %d", attrs.Deps, records[2].Ref.Instance)
	}
	existingAttrs := rn.computeAttrs(newCommand, records[2].Ref)
	if existingAttrs.Deps[1] != records[1].Ref.Instance {
		t.Fatalf("excluded-lane fallback deps=%v, want prior instance %d", existingAttrs.Deps, records[1].Ref.Instance)
	}
}

func TestBootstrapUserOrderConvergesAfterDelayedDelivery(t *testing.T) {
	s, cluster, identities := newCertifiedBootstrapSimCluster(t, 3)
	request := PrepareVoterRequest{
		Cluster:              cluster,
		Plan:                 BootstrapID{0x6f, 0x72, 0x64},
		Base:                 s.nodes[1].Status().Conf,
		OldVoters:            cloneVoterIdentities(identities[:3]),
		Target:               identities[3].Clone(),
		Source:               1,
		SourceDigest:         StateDigest{1},
		ReleaseDigest:        StateDigest{2},
		TargetAllocatorFloor: 1,
		TimingDigest:         StateDigest{3},
	}
	plan, err := s.nodes[1].PrepareVoter(request)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()

	prepareRef := plan.Reservations.Prepare
	for _, id := range []ReplicaID{1, 2, 3} {
		if inst := s.nodes[id].instances[prepareRef]; inst == nil || inst.rec.Status != StatusExecuted {
			t.Fatalf("node %d bootstrap prepare=%#v, want executed control record", id, inst)
		}
	}

	s.paused[3] = true
	user := bootstrapOrderingUser(3, "delayed-user")
	userRef, err := s.nodes[1].Propose(user)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	if len(s.apps[1]) != 1 || len(s.apps[2]) != 1 || len(s.apps[3]) != 0 {
		t.Fatalf("initial delayed user application = %#v", s.apps)
	}
	delayedUser := append([]Message(nil), s.delayed...)
	s.delayed = nil
	if _, ok := s.nodes[3].instances[userRef]; ok {
		t.Fatal("node 3 observed delayed user before the fence")
	}

	fenceAcks := make([]BootstrapMessage, 0, 3)
	for _, id := range []ReplicaID{1, 2, 3} {
		if err := s.nodes[id].BeginVoterFence(plan); err != nil {
			t.Fatalf("BeginVoterFence node %d: %v", id, err)
		}
		fenceAcks = append(fenceAcks, persistBootstrapSimNode(t, s, id)...)
	}
	for _, ack := range fenceAcks[:slowQuorumSize(3)] {
		if err := s.nodes[1].StepBootstrapAuthenticated(ack, ack.From, ack.FromIncarnation); err != nil {
			t.Fatalf("FenceAck from %d: %v", ack.From, err)
		}
	}
	fenceQuorum := s.nodes[1].BootstrapStatus().Plans[0].FenceQuorum
	if err := ValidateFenceQuorum(plan, fenceQuorum); err != nil {
		t.Fatalf("invalid fence quorum: %v", err)
	}
	persistBootstrapSimNode(t, s, 1)
	if err := s.nodes[2].ApplyFenceQuorum(fenceQuorum); err != nil {
		t.Fatal(err)
	}
	persistBootstrapSimNode(t, s, 2)

	s.paused[3] = false
	if err := s.nodes[3].ApplyFenceQuorum(fenceQuorum); err != nil {
		t.Fatal(err)
	}
	persistBootstrapSimNode(t, s, 3)
	for _, message := range delayedUser {
		err := s.nodes[message.To].Step(message)
		if err != nil && !errors.Is(err, ErrBootstrapFenced) && !errors.Is(err, ErrBootstrapContradiction) {
			t.Fatalf("delayed post-fence message %s %d->%d err=%v", message.Type, message.From, message.To, err)
		}
	}
	s.drain()

	for _, id := range []ReplicaID{1, 2, 3} {
		closure := s.nodes[id].BootstrapClosure(plan)
		if !closure.Complete {
			t.Fatalf("node %d bootstrap closure=%#v", id, closure)
		}
		if len(s.apps[id]) != 1 || s.apps[id][0].Ref != userRef || !commandEqual(s.apps[id][0].Command, user) {
			t.Fatalf("node %d application output=%#v, want one delayed user command", id, s.apps[id])
		}
		membership := s.nodes[id].instances[prepareRef]
		if membership == nil || membership.rec.Status != StatusExecuted {
			t.Fatalf("node %d membership prepare=%#v, want executed control record", id, membership)
		}
	}
}
