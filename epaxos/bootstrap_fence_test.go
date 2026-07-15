package epaxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func standaloneBootstrapPlan(t *testing.T, voters int) (bootstrapTestFixture, VoterPlan) {
	t.Helper()
	f := newBootstrapTestFixture(t, voters, 1)
	request := f.request()
	ids := append([]ReplicaID(nil), request.Base.Voters...)
	ids = append(ids, request.Target.Replica)
	plan := VoterPlan{
		Request: request,
		Reservations: ControlReservations{
			Prepare:  InstanceRef{Replica: 1, Instance: 1, Conf: request.Base.ID},
			Activate: InstanceRef{Replica: 1, Instance: 2, Conf: request.Base.ID},
			Abort:    InstanceRef{Replica: 1, Instance: 3, Conf: request.Base.ID},
		},
		Successor: ConfState{ID: request.Base.ID + 1, Voters: ids},
	}
	plan.RequestDigest = DigestVoterPlan(plan)
	if err := validateVoterPlan(plan); err != nil {
		t.Fatalf("standalone plan: %v", err)
	}
	return f, plan
}

func standaloneFrontier(plan VoterPlan, observed InstanceNum) BootstrapFrontier {
	frontier := BootstrapFrontier{Conf: plan.Request.Base.ID, Lanes: make([]BootstrapLaneFrontier, len(plan.Request.Base.Voters))}
	for i, voter := range plan.Request.Base.Voters {
		frontier.Lanes[i] = BootstrapLaneFrontier{Replica: voter, ObservedThrough: observed, CommittedThrough: observed, ExecutedThrough: observed}
		if observed != 0 {
			frontier.Lanes[i].Sparse = []InstanceNum{observed}
		}
	}
	return frontier
}

func standaloneSnapshotDescriptor(plan VoterPlan, fence FenceQuorum) SnapshotDescriptor {
	return SnapshotDescriptor{
		Cluster: plan.Request.Cluster, Plan: plan.Request.Plan, Base: plan.Request.Base.Clone(),
		Successor: plan.Successor.Clone(), Target: plan.Request.Target.Clone(), Source: plan.Request.Source,
		Reservations: plan.Reservations, FenceDigest: fence.Digest, Frontier: fence.Frontier.Clone(),
		ManifestDigest: StateDigest{1}, DeltaRoot: StateDigest{2}, ApplicationDigest: StateDigest{3},
		IdempotencyDigest: StateDigest{4}, ConfigHistoryDigest: StateDigest{5}, InstanceDigest: StateDigest{6},
		HardStateDigest: StateDigest{7}, CompactionDigest: StateDigest{8}, InstalledStateDigest: StateDigest{9},
		TargetAllocatorFloor: plan.Request.TargetAllocatorFloor, TOQClosedThrough: plan.Request.TOQClosedThrough,
		TimingDigest: plan.Request.TimingDigest, ReleaseDigest: plan.Request.ReleaseDigest,
	}
}

func standaloneAdmissionFence(t *testing.T, plan VoterPlan, identity VoterIdentity, frontier BootstrapFrontier) LocalAdmissionFence {
	t.Helper()
	fence, err := BuildLocalAdmissionFence(LocalAdmissionFence{
		Plan: plan.Request.Plan, Base: plan.Request.Base.Clone(), Voter: identity.Clone(),
		Reservations: plan.Reservations, Frontier: frontier,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fence
}

func TestPrepareVoterIdempotencyAndConcurrentPlanExclusion(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	request := f.request()
	first, err := f.node.PrepareVoter(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.node.PrepareVoter(request)
	if err != nil || !voterPlanEqual(first, second) {
		t.Fatalf("idempotent retry plan=%#v err=%v", second, err)
	}
	before := f.node.Ready()
	request.Plan[0]++
	if _, err := f.node.PrepareVoter(request); !errors.Is(err, ErrBootstrapBusy) {
		t.Fatalf("concurrent plan err=%v", err)
	}
	after := f.node.Ready()
	if len(after.BootstrapRecords) != len(before.BootstrapRecords) || after.AllocatorFloor != before.AllocatorFloor {
		t.Fatalf("busy plan mutated Ready: before=%#v after=%#v", before, after)
	}
}

func TestFenceAckEmittedOnlyAfterBootstrapReadyPersistence(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatal(err)
	}
	rd := f.node.Ready()
	if len(rd.BootstrapMessages) != 0 || !rd.MustSync || len(rd.BootstrapRecords) == 0 {
		t.Fatalf("pre-persistence fence Ready=%#v", rd)
	}
	if err := f.store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if len(f.node.Ready().BootstrapMessages) != 0 {
		t.Fatal("FenceAck visible before Advance")
	}
	if err := f.node.Advance(rd); err != nil {
		t.Fatal(err)
	}
	out := f.node.Ready()
	if out.MustSync || len(out.BootstrapMessages) != 1 || out.BootstrapMessages[0].Type != BootstrapMsgFenceAck {
		t.Fatalf("post-persistence Ready=%#v", out)
	}
}

func TestFenceRejectsOrdinaryPreAcceptAcceptPrepareAboveEveryLaneFrontier(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatal(err)
	}
	above := plan.Reservations.Abort.Instance + 1
	messages := []Message{
		{Type: MsgPreAccept, From: 1, To: 1, Ref: InstanceRef{Replica: 1, Instance: above, Conf: 1}, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: []InstanceNum{0}, Command: Command{Payload: []byte("blocked")}},
		{Type: MsgAccept, From: 1, To: 1, Ref: InstanceRef{Replica: 1, Instance: above, Conf: 1}, Ballot: Ballot{Epoch: 1, Replica: 1}, Seq: 1, Deps: []InstanceNum{0}, Command: Command{Payload: []byte("blocked")}},
		{Type: MsgPrepare, From: 1, To: 1, Ref: InstanceRef{Replica: 1, Instance: above, Conf: 1}, Ballot: Ballot{Epoch: 1, Replica: 1}},
	}
	beforeInstances := len(f.node.instances)
	beforeReady := f.node.Ready()
	for _, message := range messages {
		if err := f.node.Step(message); !errors.Is(err, ErrBootstrapFenced) {
			t.Fatalf("%s above fence err=%v", message.Type, err)
		}
	}
	afterReady := f.node.Ready()
	if len(f.node.instances) != beforeInstances || len(afterReady.Records) != len(beforeReady.Records) || len(afterReady.Messages) != len(beforeReady.Messages) {
		t.Fatal("fenced rejection mutated records or responses")
	}
}

func TestFenceAllowsOnlyMatchingPreFenceRetryOrRecoveryBelowFrontier(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	userRef, err := f.node.Propose(Command{Payload: []byte("before-fence"), ConflictKeys: [][]byte{[]byte("k")}})
	if err != nil {
		t.Fatal(err)
	}
	persistBootstrapReady(t, f)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatal(err)
	}
	stored, ok := f.store.Instance(userRef)
	if !ok {
		t.Fatalf("durable pre-fence record %s not found", userRef)
	}
	retry := Message{Type: MsgPreAccept, From: 1, To: 1, Ref: userRef, Ballot: stored.Ballot, Seq: stored.Seq, Deps: stored.Deps, Command: stored.Command}
	if err := f.node.Step(retry); err != nil {
		t.Fatalf("matching pre-fence retry: %v", err)
	}
	loadReady := f.node.Ready()
	if len(loadReady.RecordLoads) != 1 || loadReady.RecordLoads[0] != userRef {
		t.Fatalf("matching retry Ready=%#v, want one record load", loadReady)
	}
	if err := provideRecordLoadsFromStore(f.node, f.store, loadReady); err != nil {
		t.Fatalf("matching retry restore: %v", err)
	}
	if err := f.node.Advance(loadReady); err != nil {
		t.Fatal(err)
	}
	for f.node.HasReady() {
		persistBootstrapReady(t, f)
	}

	retry.Command.Payload = []byte("different")
	if err := f.node.Step(retry); err != nil {
		t.Fatalf("different retry deferred error=%v, want asynchronous validation", err)
	}
	loadReady = f.node.Ready()
	if err := provideRecordLoadsFromStore(f.node, f.store, loadReady); !errors.Is(err, ErrBootstrapFenced) {
		t.Fatalf("different retry replay err=%v, want ErrBootstrapFenced", err)
	}
	if err := f.node.Advance(loadReady); err != nil {
		t.Fatal(err)
	}
	recovery := Message{Type: MsgPrepare, From: 1, To: 1, Ref: userRef, Ballot: Ballot{Epoch: 1, Replica: 1}}
	if err := f.node.Step(recovery); err != nil {
		t.Fatalf("old-config recovery below frontier: %v", err)
	}
}

func TestReservedControlRefsRejectUserNoopWrongPlanAndSwappedExitCommands(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatal(err)
	}
	wrong := []Command{{Payload: []byte("user")}, {Kind: CommandNoop}}
	abort, err := encodeMembershipCommand(membershipCommandWire{Operation: membershipAbort, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	wrong = append(wrong, abort)
	for _, command := range wrong {
		message := Message{Type: MsgPreAccept, From: 1, To: 1, Ref: plan.Reservations.Activate, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: []InstanceNum{0}, Command: command}
		if err := f.node.Step(message); !errors.Is(err, ErrBootstrapControl) {
			t.Fatalf("control command %#v err=%v", command, err)
		}
	}
}

func TestCanonicalFenceFrontierIsComponentwiseUnionAndReservationsAreOnlyHoles(t *testing.T) {
	f, plan := standaloneBootstrapPlan(t, 3)
	left := standaloneFrontier(plan, 0)
	left.Lanes[0] = BootstrapLaneFrontier{Replica: 1, ObservedThrough: 5, CommittedThrough: 3, ExecutedThrough: 2, Sparse: []InstanceNum{1, 5}}
	right := standaloneFrontier(plan, 0)
	right.Lanes[0] = BootstrapLaneFrontier{Replica: 1, ObservedThrough: 7, CommittedThrough: 4, ExecutedThrough: 4, Sparse: []InstanceNum{2, 7}}
	right.Lanes[1] = BootstrapLaneFrontier{Replica: 2, ObservedThrough: 9, CommittedThrough: 8, ExecutedThrough: 8, Sparse: []InstanceNum{9}}
	fences := []LocalAdmissionFence{
		standaloneAdmissionFence(t, plan, f.identities[0], left),
		standaloneAdmissionFence(t, plan, f.identities[1], right),
	}
	quorum, err := BuildFenceQuorum(plan, fences)
	if err != nil {
		t.Fatal(err)
	}
	lane1, _ := quorum.Frontier.lane(1)
	lane2, _ := quorum.Frontier.lane(2)
	if lane1.ObservedThrough != 7 || lane1.CommittedThrough != 4 || lane1.ExecutedThrough != 4 ||
		!instanceNumsEqual(lane1.Sparse, []InstanceNum{1, 2, 5, 7}) || lane2.ObservedThrough != 9 {
		t.Fatalf("union frontier=%#v", quorum.Frontier)
	}
	if !quorum.Reservations.ValidFor(plan.Request.Base) {
		t.Fatalf("lost reservations: %#v", quorum.Reservations)
	}
}
func TestSnapshotVoteCannotCountBeforeFenceQuorumIsDurable(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatal(err)
	}
	fence := f.node.BootstrapStatus().Plans[0].LocalFence
	quorum, err := BuildFenceQuorum(plan, []LocalAdmissionFence{fence})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := standaloneSnapshotDescriptor(plan, quorum)
	acknowledgement, err := BuildVoterAcknowledgement(DigestSnapshotDescriptor(descriptor), f.identities[0])
	if err != nil {
		t.Fatal(err)
	}
	payload, err := marshalBootstrapCanonical(snapshotVotePayload{Descriptor: descriptor, Acknowledgement: acknowledgement})
	if err != nil {
		t.Fatal(err)
	}
	message, err := BuildBootstrapMessage(BootstrapMessage{
		Type: BootstrapMsgSnapshotVote, Cluster: f.cluster, Plan: plan.Request.Plan,
		From: 1, FromIncarnation: 1, To: 1, BaseID: plan.Request.Base.ID,
		BaseDigest: plan.RequestDigest, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.node.StepBootstrapAuthenticated(message, message.From, message.FromIncarnation); !errors.Is(err, ErrBootstrapClosure) {
		t.Fatalf("pre-quorum snapshot vote err=%v", err)
	}
	record := f.node.BootstrapStatus().Plans[0]
	if record.FenceQuorum.Digest != (StateDigest{}) || record.SnapshotCertificate.Digest != (StateDigest{}) {
		t.Fatalf("pre-quorum snapshot vote mutated durable state: %#v", record)
	}
}

func TestSnapshotVoteRequiresEveryUncompactedSlotThroughUnionFrontierResolved(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatal(err)
	}
	persistBootstrapReady(t, f)
	persistBootstrapReady(t, f)
	frontier := BootstrapFrontier{Conf: 1, Lanes: []BootstrapLaneFrontier{{Replica: 1, ObservedThrough: 100, Sparse: []InstanceNum{plan.Reservations.Prepare.Instance, 100}}}}
	fence := standaloneAdmissionFence(t, plan, f.identities[0], frontier)
	quorum, err := BuildFenceQuorum(plan, []LocalAdmissionFence{fence})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.node.ApplyFenceQuorum(quorum); err != nil {
		t.Fatal(err)
	}
	closure := f.node.BootstrapClosure(plan)
	if closure.Complete || len(closure.Missing) != 97 ||
		closure.Missing[0].Instance != plan.Reservations.Abort.Instance+1 ||
		closure.Missing[len(closure.Missing)-1].Instance != 100 {
		t.Fatalf("closure=%#v", closure)
	}
	oversized := BootstrapFrontier{
		Conf:  plan.Request.Base.ID,
		Lanes: []BootstrapLaneFrontier{{Replica: 1, ObservedThrough: ^InstanceNum(0), Sparse: []InstanceNum{^InstanceNum(0)}}},
	}
	oversizedFence := standaloneAdmissionFence(t, plan, f.identities[0], oversized)
	if _, err := BuildFenceQuorum(plan, []LocalAdmissionFence{oversizedFence}); !errors.Is(err, ErrBootstrapBounds) {
		t.Fatalf("unbounded closure frontier err=%v", err)
	}
}

func TestSnapshotCertificateRequiresUniqueExactOldQuorum(t *testing.T) {
	f, plan := standaloneBootstrapPlan(t, 3)
	frontier := standaloneFrontier(plan, 0)
	fence1 := standaloneAdmissionFence(t, plan, f.identities[0], frontier)
	fence2 := standaloneAdmissionFence(t, plan, f.identities[1], frontier)
	fenceQuorum, err := BuildFenceQuorum(plan, []LocalAdmissionFence{fence1, fence2})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := standaloneSnapshotDescriptor(plan, fenceQuorum)
	digest := DigestSnapshotDescriptor(descriptor)
	a1, _ := BuildVoterAcknowledgement(digest, f.identities[0])
	a2, _ := BuildVoterAcknowledgement(digest, f.identities[1])
	a3, _ := BuildVoterAcknowledgement(digest, f.identities[2])
	if _, err := BuildSnapshotCertificate(plan, descriptor, []VoterAcknowledgement{a1, a2}); err != nil {
		t.Fatalf("valid exact quorum: %v", err)
	}
	for name, acknowledgements := range map[string][]VoterAcknowledgement{
		"below quorum":   {a1},
		"duplicate":      {a1, a1},
		"target":         {a1, {Voter: f.target, AttestedDigest: digest}},
		"missing source": {a2, a3},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildSnapshotCertificate(plan, descriptor, acknowledgements); !errors.Is(err, ErrBootstrapCertificate) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	badDigest := a2
	badDigest.AttestedDigest[0] ^= 1
	if _, err := BuildSnapshotCertificate(plan, descriptor, []VoterAcknowledgement{a1, badDigest}); !errors.Is(err, ErrBootstrapCertificate) {
		t.Fatalf("bad acknowledgement err=%v", err)
	}
	wrongConfig := descriptor.Clone()
	wrongConfig.Base.Voters = []ReplicaID{1, 3}
	if _, err := BuildSnapshotCertificate(plan, wrongConfig, []VoterAcknowledgement{a1, a3}); err == nil {
		t.Fatal("wrong-config snapshot certificate accepted")
	}
}

func TestActivateRejectsMissingStaleOrMismatchedInstallProof(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	fence := fenceBootstrapPlan(t, f, plan)
	snapshot := certifyBootstrapSnapshot(t, f, plan, fence)
	ready := readyBootstrapTarget(t, f, plan, snapshot)
	ready.Proof.SnapshotDigest[0] ^= 1
	if _, err := f.node.ActivateVoter(plan, snapshot, ready); !errors.Is(err, ErrBootstrapCertificate) {
		t.Fatalf("mismatched proof err=%v", err)
	}
	if f.node.Status().Conf.ID != plan.Request.Base.ID {
		t.Fatal("rejected activation changed configuration")
	}
}

func TestReadyProofQuorumReplicationSurvivesOriginalCoordinatorCrash(t *testing.T) {
	f, plan := standaloneBootstrapPlan(t, 3)
	frontier := standaloneFrontier(plan, 0)
	fences := []LocalAdmissionFence{
		standaloneAdmissionFence(t, plan, f.identities[0], frontier),
		standaloneAdmissionFence(t, plan, f.identities[1], frontier),
	}
	fenceQuorum, _ := BuildFenceQuorum(plan, fences)
	descriptor := standaloneSnapshotDescriptor(plan, fenceQuorum)
	digest := DigestSnapshotDescriptor(descriptor)
	a1, _ := BuildVoterAcknowledgement(digest, f.identities[0])
	a2, _ := BuildVoterAcknowledgement(digest, f.identities[1])
	snapshot, _ := BuildSnapshotCertificate(plan, descriptor, []VoterAcknowledgement{a1, a2})
	proof, _ := BuildVoterReadyProof(VoterReadyProof{Cluster: f.cluster, Plan: plan.Request.Plan, Target: f.target, SnapshotDigest: snapshot.Digest, InstalledStateDigest: descriptor.InstalledStateDigest, AllocatorFloor: 1})
	proofDigest := DigestVoterReadyProof(proof)
	v2, _ := BuildVoterAcknowledgement(proofDigest, f.identities[1])
	v3, _ := BuildVoterAcknowledgement(proofDigest, f.identities[2])
	ready, err := BuildReadyCertificate(plan, proof, []VoterAcknowledgement{v2, v3})
	if err != nil || ValidateReadyCertificate(plan, snapshot, ready) != nil {
		t.Fatalf("coordinator-independent Ready certificate err=%v cert=%#v", err, ready)
	}
	if ready.Acknowledgements[0].Voter.Replica == plan.Reservations.Prepare.Replica {
		t.Fatal("test accidentally retained original coordinator")
	}
}

func TestFencingLayersRejectFoldedLoadAndStaleBootstrapAuth(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	current := VoterIdentity{Replica: 1, Incarnation: 2}
	f.identities[0] = current
	f.node.voterIdentities[1] = current
	f.node.localIdentity = current
	f.node.localVoter.Identity = current
	f.node.durableLocalVoter.Identity = current
	f.store.LocalVoterState.Identity = current
	plan := prepareBootstrapPlan(t, f)
	ref := InstanceRef{Conf: 1, Replica: 1, Instance: 5}
	rec := checkedRecord(InstanceRecord{
		Ref: ref, Status: StatusCommitted, Seq: 3, Ballot: Ballot{Replica: 1},
		Deps: f.node.q.deps(), Command: Command{Payload: []byte("chosen"), ConflictKeys: [][]byte{[]byte("k")}},
	})
	foldTestRef(&f.node.engine, rec)
	if err := f.node.BeginVoterFence(plan); err != nil {
		t.Fatal(err)
	}
	before := f.node.Ready()
	late := Message{Type: MsgPrepare, From: 1, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}}
	if err := f.node.Step(late); !errors.Is(err, ErrBootstrapFenced) {
		t.Fatalf("fenced folded message err=%v", err)
	}
	// Ordinary messages carry no incarnation. This folded prepare has no
	// resident payload-absent instance, so it does not defer a record load;
	// closed-config admission then rejects it.
	afterFoldedMessage := f.node.Ready()
	if len(f.node.pendingRecordLoads) != 0 || len(afterFoldedMessage.RecordLoads) != len(before.RecordLoads) {
		t.Fatalf("fenced folded message queued record load: pending=%#v before=%#v after=%#v", f.node.pendingRecordLoads, before.RecordLoads, afterFoldedMessage.RecordLoads)
	}
	// Bootstrap control traffic is the separate authenticated-incarnation path.
	message, err := BuildBootstrapMessage(BootstrapMessage{
		Type: BootstrapMsgReadyQuery, Cluster: f.cluster, Plan: plan.Request.Plan,
		From: 1, FromIncarnation: 1, To: 1, BaseID: plan.Request.Base.ID,
		BaseDigest: plan.RequestDigest, Payload: []byte("{}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.node.StepBootstrapAuthenticated(message, 1, 1); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("stale authenticated incarnation err=%v", err)
	}
	after := f.node.Ready()
	if len(f.node.pendingRecordLoads) != 0 || len(after.RecordLoads) != len(afterFoldedMessage.RecordLoads) {
		t.Fatalf("stale bootstrap sender queued record load: pending=%#v before=%#v after=%#v", f.node.pendingRecordLoads, afterFoldedMessage.RecordLoads, after.RecordLoads)
	}
}

func TestBootstrapEnvelopeAndChunkValidationIsCanonicalBoundedAndIdempotent(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	beforeStatus := f.node.BootstrapStatus()
	message, err := BuildBootstrapMessage(BootstrapMessage{Type: BootstrapMsgReadyQuery, Cluster: f.cluster, Plan: plan.Request.Plan, From: 1, FromIncarnation: 1, To: 1, BaseID: plan.Request.Base.ID, BaseDigest: plan.RequestDigest, Payload: []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeBootstrapMessage(nil, message)
	if err != nil {
		t.Fatal(err)
	}
	var decoded BootstrapMessage
	if err := DecodeBootstrapMessage(encoded, &decoded); err != nil || ValidateBootstrapMessage(decoded) != nil {
		t.Fatalf("round trip err=%v decoded=%#v", err, decoded)
	}
	if err := f.node.StepBootstrapAuthenticated(message, 2, 1); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("spoofed authenticated replica err=%v", err)
	}
	if err := f.node.StepBootstrapAuthenticated(message, 1, 2); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("spoofed authenticated incarnation err=%v", err)
	}
	afterStatus := f.node.BootstrapStatus()
	if !reflect.DeepEqual(beforeStatus, afterStatus) {
		t.Fatalf("spoofed bootstrap message mutated state: before=%#v after=%#v", beforeStatus, afterStatus)
	}
	if !bytes.Equal(decoded.Payload, message.Payload) {
		t.Fatal("decoded envelope payload mismatch")
	}
	if err := DecodeBootstrapMessage(append(encoded, 0), &decoded); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("trailing envelope err=%v", err)
	}
	chunk, err := BuildBootstrapChunk(BootstrapChunk{Cluster: f.cluster, Plan: f.planID, From: 1, FromIncarnation: 1, To: 2, Manifest: StateDigest{2}, Index: 1, Offset: 0, Total: 3, Payload: []byte("abc")})
	if err != nil {
		t.Fatal(err)
	}
	set := BootstrapChunkSet{Limit: 4}
	if err := set.AddAuthenticated(chunk, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := set.AddAuthenticated(chunk, 1, 1); err != nil {
		t.Fatalf("exact duplicate: %v", err)
	}
	beforeChunks := len(set.chunks)
	if err := set.AddAuthenticated(chunk, 2, 1); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("spoofed chunk replica err=%v", err)
	}
	if err := set.AddAuthenticated(chunk, 1, 2); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("spoofed chunk incarnation err=%v", err)
	}
	if len(set.chunks) != beforeChunks {
		t.Fatalf("spoofed chunk mutated set: before=%d after=%d", beforeChunks, len(set.chunks))
	}
	conflictOffset := chunk.Clone()
	conflictOffset.Offset = 1
	conflictOffset.Payload = []byte("ab")
	conflictOffset.PayloadDigest = StateDigest{}
	conflictOffset, err = BuildBootstrapChunk(conflictOffset)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.AddAuthenticated(conflictOffset, 1, 1); !errors.Is(err, ErrBootstrapChunkConflict) {
		t.Fatalf("same-index offset conflict err=%v", err)
	}
	conflictTotal := chunk.Clone()
	conflictTotal.Total = 4
	conflictTotal.PayloadDigest = StateDigest{}
	conflictTotal, err = BuildBootstrapChunk(conflictTotal)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.AddAuthenticated(conflictTotal, 1, 1); !errors.Is(err, ErrBootstrapChunkConflict) {
		t.Fatalf("same-index total conflict err=%v", err)
	}
}

func TestDecodeBootstrapMessageRejectsOversizedType(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	message, err := BuildBootstrapMessage(BootstrapMessage{
		Type: BootstrapMsgReadyQuery, Cluster: f.cluster, Plan: plan.Request.Plan,
		From: 1, FromIncarnation: 1, To: 1, BaseID: plan.Request.Base.ID,
		BaseDigest: plan.RequestDigest, Payload: []byte("{}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeBootstrapMessage(nil, message)
	if err != nil {
		t.Fatal(err)
	}
	// Replace the type uvarint (immediately after magic+version) with 257 so it
	// would truncate to BootstrapMsgFenceRequest without range checking.
	frame := append([]byte(nil), encoded[:len(bootstrapWireMagic)]...)
	// re-encode version then oversized type then rest after original version+type
	// Parse original after magic: version uvarint then type uvarint.
	rest := encoded[len(bootstrapWireMagic):]
	_, nVer := binary.Uvarint(rest)
	if nVer <= 0 {
		t.Fatal("version uvarint")
	}
	_, nType := binary.Uvarint(rest[nVer:])
	if nType <= 0 {
		t.Fatal("type uvarint")
	}
	frame = append(frame, rest[:nVer]...)
	frame = binary.AppendUvarint(frame, 257)
	frame = append(frame, rest[nVer+nType:]...)
	var got BootstrapMessage
	if err := DecodeBootstrapMessage(frame, &got); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("oversized bootstrap type err=%v, want ErrInvalidBootstrapMessage; got=%#v", err, got)
	}
	if got.Type != 0 || got.From != 0 || len(got.Payload) != 0 {
		t.Fatalf("failed decode left residue: %#v", got)
	}
}
