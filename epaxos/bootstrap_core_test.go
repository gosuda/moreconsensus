package epaxos

import (
	"errors"
	"testing"
)

type bootstrapTestFixture struct {
	node       *RawNode
	store      *MemoryStorage
	cluster    ClusterID
	identities []VoterIdentity
	target     VoterIdentity
	planID     BootstrapID
}

func newBootstrapTestFixture(t *testing.T, voters int, local ReplicaID) bootstrapTestFixture {
	t.Helper()
	conf := ConfState{ID: 1, Voters: makeIDs(voters)}
	identities := make([]VoterIdentity, voters)
	for i := range identities {
		identities[i] = VoterIdentity{Replica: ReplicaID(i + 1), Incarnation: 1}
	}
	target := VoterIdentity{Replica: ReplicaID(voters + 1), Incarnation: 1}
	cluster := ClusterID{1, 2, 3}
	planID := BootstrapID{9, byte(voters), byte(local)}
	store := NewMemoryStorage()
	store.Hard = HardState{Conf: conf.Clone()}
	store.ConfigHistory = []ConfigHistoryEntry{{Conf: conf.Clone()}}
	store.LocalVoterState = LocalVoterState{
		Cluster: cluster, Identity: identities[local-1].Clone(), Conf: conf.Clone(),
		Status: LocalVoterStatusEligible, AllocatorFloor: 1,
	}
	node, err := NewRawNode(Config{
		ID: local, Voters: conf.Voters, Cluster: cluster,
		LocalIdentity: identities[local-1], VoterIdentities: identities,
		Storage: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return bootstrapTestFixture{node: node, store: store, cluster: cluster, identities: identities, target: target, planID: planID}
}

func (f bootstrapTestFixture) request() PrepareVoterRequest {
	return PrepareVoterRequest{
		Cluster: f.cluster, Plan: f.planID, Base: f.node.Status().Conf,
		OldVoters: cloneVoterIdentities(f.identities), Target: f.target.Clone(), Source: 1,
		SourceDigest: StateDigest{1}, ReleaseDigest: StateDigest{2},
		TargetAllocatorFloor: 1, TimingDigest: StateDigest{3},
	}
}

func persistBootstrapReady(t *testing.T, f bootstrapTestFixture) Ready {
	t.Helper()
	if !f.node.HasReady() {
		t.Fatal("expected Ready")
	}
	rd := f.node.Ready()
	if err := f.store.ApplyReady(rd); err != nil {
		t.Fatalf("ApplyReady: %v", err)
	}
	if err := f.node.Advance(rd); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	return rd
}

func prepareBootstrapPlan(t *testing.T, f bootstrapTestFixture) VoterPlan {
	t.Helper()
	plan, err := f.node.PrepareVoter(f.request())
	if err != nil {
		t.Fatalf("PrepareVoter: %v", err)
	}
	persistBootstrapReady(t, f)
	return plan
}

func fenceBootstrapPlan(t *testing.T, f bootstrapTestFixture, plan VoterPlan) FenceQuorum {
	t.Helper()
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatalf("BeginVoterFence: %v", err)
	}
	persistBootstrapReady(t, f)
	out := persistBootstrapReady(t, f)
	if len(out.BootstrapMessages) != 1 || out.BootstrapMessages[0].Type != BootstrapMsgFenceAck {
		t.Fatalf("post-durable FenceAck=%#v", out.BootstrapMessages)
	}
	fence := f.node.BootstrapStatus().Plans[0].LocalFence
	quorum, err := BuildFenceQuorum(plan, []LocalAdmissionFence{fence})
	if err != nil {
		t.Fatalf("BuildFenceQuorum: %v", err)
	}
	if err := f.node.ApplyFenceQuorum(quorum); err != nil {
		t.Fatalf("ApplyFenceQuorum: %v", err)
	}
	persistBootstrapReady(t, f)
	return quorum
}

func certifyBootstrapSnapshot(t *testing.T, f bootstrapTestFixture, plan VoterPlan, fence FenceQuorum) SnapshotCertificate {
	t.Helper()
	descriptor := SnapshotDescriptor{
		Cluster: f.cluster, Plan: plan.Request.Plan, Base: plan.Request.Base.Clone(), Successor: plan.Successor.Clone(),
		Target: plan.Request.Target.Clone(), Source: plan.Request.Source, Reservations: plan.Reservations,
		FenceDigest: fence.Digest, Frontier: fence.Frontier.Clone(), ManifestDigest: StateDigest{4},
		DeltaFirst: 1, DeltaLast: 1, DeltaRoot: StateDigest{5}, ApplicationDigest: StateDigest{6},
		IdempotencyDigest: StateDigest{7}, ConfigHistoryDigest: StateDigest{8}, InstanceDigest: StateDigest{9},
		HardStateDigest: StateDigest{10}, CompactionDigest: StateDigest{11},
		InstalledStateDigest: StateDigest{13}, TargetAllocatorFloor: 1,
		TOQClosedThrough: plan.Request.TOQClosedThrough,
		TimingDigest:     plan.Request.TimingDigest, ReleaseDigest: plan.Request.ReleaseDigest,
	}
	digest := DigestSnapshotDescriptor(descriptor)
	acknowledgement, err := BuildVoterAcknowledgement(digest, f.identities[0])
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := BuildSnapshotCertificate(plan, descriptor, []VoterAcknowledgement{acknowledgement})
	if err != nil {
		t.Fatalf("BuildSnapshotCertificate: %v", err)
	}
	payload, err := marshalBootstrapCanonical(snapshotVotePayload{Descriptor: descriptor, Acknowledgement: acknowledgement})
	if err != nil {
		t.Fatal(err)
	}
	message, err := BuildBootstrapMessage(BootstrapMessage{
		Type: BootstrapMsgSnapshotVote, Cluster: f.cluster, Plan: plan.Request.Plan,
		From: 1, FromIncarnation: 1, To: f.node.id, BaseID: plan.Request.Base.ID,
		BaseDigest: plan.RequestDigest, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.node.StepBootstrapAuthenticated(message, message.From, message.FromIncarnation); err != nil {
		t.Fatalf("StepBootstrapAuthenticated SnapshotVote: %v", err)
	}
	persistBootstrapReady(t, f)
	return certificate
}

func readyBootstrapTarget(t *testing.T, f bootstrapTestFixture, plan VoterPlan, snapshot SnapshotCertificate) ReadyCertificate {
	t.Helper()
	proof, err := BuildVoterReadyProof(VoterReadyProof{
		Cluster: f.cluster, Plan: plan.Request.Plan, Target: plan.Request.Target.Clone(),
		SnapshotDigest: snapshot.Digest, InstalledStateDigest: snapshot.Descriptor.InstalledStateDigest,
		AllocatorFloor:   snapshot.Descriptor.TargetAllocatorFloor,
		TOQClosedThrough: snapshot.Descriptor.TOQClosedThrough,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.node.RecordTargetReady(plan, proof); err != nil {
		t.Fatalf("RecordTargetReady: %v", err)
	}
	persistBootstrapReady(t, f)
	persistBootstrapReady(t, f)
	acknowledgement, err := BuildVoterAcknowledgement(DigestVoterReadyProof(proof), f.identities[0])
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := BuildReadyCertificate(plan, proof, []VoterAcknowledgement{acknowledgement})
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func TestPrepareVoterLeavesTargetNonVotingAndOldConfigAvailable(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if plan.Successor.Contains(f.target.Replica) == false || f.node.Status().Conf.Contains(f.target.Replica) {
		t.Fatalf("prepare changed voting config: current=%#v successor=%#v", f.node.Status().Conf, plan.Successor)
	}
	if _, err := f.node.Propose(Command{Payload: []byte("old-config-live")}); err != nil {
		t.Fatalf("old config unavailable while preparing: %v", err)
	}
}

func TestPrepareVoterAtomicallyReservesUniqueConsecutiveControlRefsAndAdvancesFloor(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan, err := f.node.PrepareVoter(f.request())
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Reservations.ValidFor(plan.Request.Base) || f.node.nextInstance != plan.Reservations.Abort.Instance+1 {
		t.Fatalf("reservations=%#v next=%d", plan.Reservations, f.node.nextInstance)
	}
	before := f.node.Ready()
	bad := newBootstrapTestFixture(t, 1, 1)
	bad.node.nextInstance = ^InstanceNum(0) - 1
	if _, err := bad.node.PrepareVoter(bad.request()); !errors.Is(err, ErrInstanceExhausted) {
		t.Fatalf("overflow err=%v", err)
	}
	if bad.node.HasReady() {
		t.Fatal("overflow exposed Ready mutation")
	}
	if len(before.BootstrapRecords) == 0 || before.AllocatorFloor != plan.Reservations.Abort.Instance+1 {
		t.Fatalf("atomic durable reservation Ready=%#v", before)
	}
}

func TestLegacyAddVoterRequiresCertificateAndStutters(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())
	before := rn.Status()
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4}); !errors.Is(err, ErrVoterCertificateRequired) {
		t.Fatalf("legacy add err=%v", err)
	}
	if rn.HasReady() || rn.nextInstance != 1 || !confStateEqual(before.Conf, rn.Status().Conf) {
		t.Fatalf("legacy add mutated node: before=%#v after=%#v", before, rn.Status())
	}
}

func TestTargetCannotVoteUntilActivationAndLocalEligibilityAreDurable(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	fence := fenceBootstrapPlan(t, f, plan)
	if closure := f.node.BootstrapClosure(plan); !closure.Complete {
		t.Fatalf("closure=%#v", closure)
	}
	snapshot := certifyBootstrapSnapshot(t, f, plan, fence)
	ready := readyBootstrapTarget(t, f, plan, snapshot)
	if f.node.Status().Conf.Contains(f.target.Replica) {
		t.Fatal("target voted before Activate")
	}
	ref, err := f.node.ActivateVoter(plan, snapshot, ready)
	if err != nil {
		t.Fatalf("ActivateVoter: %v", err)
	}
	if ref != plan.Reservations.Activate {
		t.Fatalf("activate ref=%s want=%s", ref, plan.Reservations.Activate)
	}
	rd := f.node.Ready()
	if !rd.MustSync || rd.LocalVoterState == nil || len(rd.BootstrapRecords) == 0 || len(rd.ConfigHistory) == 0 {
		t.Fatalf("activation durability batch=%#v", rd)
	}
	if err := f.store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := f.node.Advance(rd); err != nil {
		t.Fatal(err)
	}
	if !f.node.Status().Conf.Contains(f.target.Replica) || f.store.Hard.Conf.ID != plan.Successor.ID {
		t.Fatalf("activation did not install successor: node=%#v store=%#v", f.node.Status().Conf, f.store.Hard)
	}
}
