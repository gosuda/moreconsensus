package epaxos

import (
	"bytes"
	"crypto/ed25519"
	"errors"
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

func standaloneSnapshotDescriptor(plan VoterPlan, seal SealCertificate) SnapshotDescriptor {
	return SnapshotDescriptor{
		Cluster: plan.Request.Cluster, Plan: plan.Request.Plan, Base: plan.Request.Base.Clone(),
		Successor: plan.Successor.Clone(), Target: plan.Request.Target.Clone(), Source: plan.Request.Source,
		Reservations: plan.Reservations, SealDigest: seal.Digest, Frontier: seal.Frontier.Clone(),
		ManifestDigest: StateDigest{1}, DeltaRoot: StateDigest{2}, ApplicationDigest: StateDigest{3},
		IdempotencyDigest: StateDigest{4}, ConfigHistoryDigest: StateDigest{5}, InstanceDigest: StateDigest{6},
		HardStateDigest: StateDigest{7}, CompactionDigest: StateDigest{8}, InstalledStateDigest: StateDigest{9},
		TargetAllocatorFloor: plan.Request.TargetAllocatorFloor, TOQClosedThrough: plan.Request.TOQClosedThrough,
		TimingDigest: plan.Request.TimingDigest, ReleaseDigest: plan.Request.ReleaseDigest,
	}
}

func signedStandaloneSeal(t *testing.T, plan VoterPlan, identity VoterIdentity, key ed25519.PrivateKey, frontier BootstrapFrontier) LocalSeal {
	t.Helper()
	seal, err := SignLocalSeal(LocalSeal{
		Plan: plan.Request.Plan, Base: plan.Request.Base.Clone(), Signer: identity.Clone(),
		Reservations: plan.Reservations, Frontier: frontier,
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	return seal
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

func TestSealAckEmittedOnlyAfterBootstrapReadyPersistence(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterSeal(plan); err != nil {
		t.Fatal(err)
	}
	rd := f.node.Ready()
	if len(rd.BootstrapMessages) != 0 || !rd.MustSync || len(rd.BootstrapRecords) == 0 {
		t.Fatalf("pre-persistence seal Ready=%#v", rd)
	}
	if err := f.store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if len(f.node.Ready().BootstrapMessages) != 0 {
		t.Fatal("SealAck visible before Advance")
	}
	if err := f.node.Advance(rd); err != nil {
		t.Fatal(err)
	}
	out := f.node.Ready()
	if out.MustSync || len(out.BootstrapMessages) != 1 || out.BootstrapMessages[0].Type != BootstrapMsgSealAck {
		t.Fatalf("post-persistence Ready=%#v", out)
	}
}

func TestSealFenceRejectsOrdinaryPreAcceptAcceptPrepareAboveEveryLaneFrontier(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterSeal(plan); err != nil {
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
		if err := f.node.Step(message); !errors.Is(err, ErrBootstrapSealed) {
			t.Fatalf("%s above fence err=%v", message.Type, err)
		}
	}
	afterReady := f.node.Ready()
	if len(f.node.instances) != beforeInstances || len(afterReady.Records) != len(beforeReady.Records) || len(afterReady.Messages) != len(beforeReady.Messages) {
		t.Fatal("sealed rejection mutated records or responses")
	}
}

func TestSealFenceAllowsOnlyMatchingPreSealRetryOrRecoveryBelowFrontier(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	userRef, err := f.node.Propose(Command{Payload: []byte("before-seal"), ConflictKeys: [][]byte{[]byte("k")}})
	if err != nil {
		t.Fatal(err)
	}
	persistBootstrapReady(t, f)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterSeal(plan); err != nil {
		t.Fatal(err)
	}
	stored := f.node.instances[userRef].rec.Clone()
	retry := Message{Type: MsgPreAccept, From: 1, To: 1, Ref: userRef, Ballot: stored.Ballot, Seq: stored.Seq, Deps: stored.Deps, Command: stored.Command}
	if err := f.node.Step(retry); err != nil {
		t.Fatalf("matching pre-seal retry: %v", err)
	}
	retry.Command.Payload = []byte("different")
	if err := f.node.Step(retry); !errors.Is(err, ErrBootstrapSealed) {
		t.Fatalf("different retry err=%v", err)
	}
	recovery := Message{Type: MsgPrepare, From: 1, To: 1, Ref: userRef, Ballot: Ballot{Epoch: 1, Replica: 1}}
	if err := f.node.Step(recovery); err != nil {
		t.Fatalf("old-config recovery below frontier: %v", err)
	}
}

func TestReservedControlRefsRejectUserNoopWrongPlanAndSwappedExitCommands(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterSeal(plan); err != nil {
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

func TestCanonicalSealFrontierIsComponentwiseUnionAndReservationsAreOnlyHoles(t *testing.T) {
	f, plan := standaloneBootstrapPlan(t, 3)
	left := standaloneFrontier(plan, 0)
	left.Lanes[0] = BootstrapLaneFrontier{Replica: 1, ObservedThrough: 5, CommittedThrough: 3, ExecutedThrough: 2, Sparse: []InstanceNum{1, 5}}
	right := standaloneFrontier(plan, 0)
	right.Lanes[0] = BootstrapLaneFrontier{Replica: 1, ObservedThrough: 7, CommittedThrough: 4, ExecutedThrough: 4, Sparse: []InstanceNum{2, 7}}
	right.Lanes[1] = BootstrapLaneFrontier{Replica: 2, ObservedThrough: 9, CommittedThrough: 8, ExecutedThrough: 8, Sparse: []InstanceNum{9}}
	seals := []LocalSeal{
		signedStandaloneSeal(t, plan, f.identities[0], f.private[0], left),
		signedStandaloneSeal(t, plan, f.identities[1], f.private[1], right),
	}
	certificate, err := BuildSealCertificate(plan, seals)
	if err != nil {
		t.Fatal(err)
	}
	lane1, _ := certificate.Frontier.lane(1)
	lane2, _ := certificate.Frontier.lane(2)
	if lane1.ObservedThrough != 7 || lane1.CommittedThrough != 4 || lane1.ExecutedThrough != 4 ||
		!instanceNumsEqual(lane1.Sparse, []InstanceNum{1, 2, 5, 7}) || lane2.ObservedThrough != 9 {
		t.Fatalf("union frontier=%#v", certificate.Frontier)
	}
	if !certificate.Reservations.ValidFor(plan.Request.Base) {
		t.Fatalf("lost reservations: %#v", certificate.Reservations)
	}
}

func TestSnapshotVoteRequiresEveryUncompactedSlotThroughUnionFrontierResolved(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	if err := f.node.BeginVoterSeal(plan); err != nil {
		t.Fatal(err)
	}
	persistBootstrapReady(t, f)
	persistBootstrapReady(t, f)
	frontier := BootstrapFrontier{Conf: 1, Lanes: []BootstrapLaneFrontier{{Replica: 1, ObservedThrough: 100, Sparse: []InstanceNum{plan.Reservations.Prepare.Instance, 100}}}}
	seal := signedStandaloneSeal(t, plan, f.identities[0], f.private[0], frontier)
	certificate, err := BuildSealCertificate(plan, []LocalSeal{seal})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.node.ApplySealCertificate(certificate); err != nil {
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
	oversizedSeal := signedStandaloneSeal(t, plan, f.identities[0], f.private[0], oversized)
	if _, err := BuildSealCertificate(plan, []LocalSeal{oversizedSeal}); !errors.Is(err, ErrBootstrapBounds) {
		t.Fatalf("unbounded closure frontier err=%v", err)
	}
}

func TestSnapshotCertificateRequiresUniqueExactOldQuorum(t *testing.T) {
	f, plan := standaloneBootstrapPlan(t, 3)
	frontier := standaloneFrontier(plan, 0)
	seal1 := signedStandaloneSeal(t, plan, f.identities[0], f.private[0], frontier)
	seal2 := signedStandaloneSeal(t, plan, f.identities[1], f.private[1], frontier)
	sealCert, err := BuildSealCertificate(plan, []LocalSeal{seal1, seal2})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := standaloneSnapshotDescriptor(plan, sealCert)
	digest := DigestSnapshotDescriptor(descriptor)
	a1, _ := SignVoterAttestation(digest, f.identities[0], f.private[0])
	a2, _ := SignVoterAttestation(digest, f.identities[1], f.private[1])
	a3, _ := SignVoterAttestation(digest, f.identities[2], f.private[2])
	if _, err := BuildSnapshotCertificate(plan, descriptor, []VoterAttestation{a1, a2}); err != nil {
		t.Fatalf("valid exact quorum: %v", err)
	}
	for name, attestations := range map[string][]VoterAttestation{
		"below quorum":   {a1},
		"duplicate":      {a1, a1},
		"target":         {a1, {Signer: f.target, AttestedDigest: digest, Signature: make([]byte, ed25519.SignatureSize)}},
		"missing source": {a2, a3},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildSnapshotCertificate(plan, descriptor, attestations); !errors.Is(err, ErrBootstrapCertificate) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	a2.Signature[0] ^= 1
	if _, err := BuildSnapshotCertificate(plan, descriptor, []VoterAttestation{a1, a2}); !errors.Is(err, ErrBootstrapCertificate) {
		t.Fatalf("bad signature err=%v", err)
	}
	wrongConfig := descriptor.Clone()
	wrongConfig.Base.Voters = []ReplicaID{1, 3}
	if _, err := BuildSnapshotCertificate(plan, wrongConfig, []VoterAttestation{a1, a3}); err == nil {
		t.Fatal("wrong-config snapshot certificate accepted")
	}
}

func TestActivateRejectsMissingStaleOrMismatchedInstallProof(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, f)
	seal := sealBootstrapPlan(t, f, plan)
	snapshot := certifyBootstrapSnapshot(t, f, plan, seal)
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
	seals := []LocalSeal{signedStandaloneSeal(t, plan, f.identities[0], f.private[0], frontier), signedStandaloneSeal(t, plan, f.identities[1], f.private[1], frontier)}
	sealCert, _ := BuildSealCertificate(plan, seals)
	descriptor := standaloneSnapshotDescriptor(plan, sealCert)
	digest := DigestSnapshotDescriptor(descriptor)
	a1, _ := SignVoterAttestation(digest, f.identities[0], f.private[0])
	a2, _ := SignVoterAttestation(digest, f.identities[1], f.private[1])
	snapshot, _ := BuildSnapshotCertificate(plan, descriptor, []VoterAttestation{a1, a2})
	proof, _ := SignVoterReadyProof(VoterReadyProof{Cluster: f.cluster, Plan: plan.Request.Plan, Target: f.target, SnapshotDigest: snapshot.Digest, InstalledStateDigest: descriptor.InstalledStateDigest, AllocatorFloor: 1}, f.targetKey)
	proofDigest := DigestVoterReadyProof(proof)
	v2, _ := SignVoterAttestation(proofDigest, f.identities[1], f.private[1])
	v3, _ := SignVoterAttestation(proofDigest, f.identities[2], f.private[2])
	ready, err := BuildReadyCertificate(plan, proof, []VoterAttestation{v2, v3})
	if err != nil || VerifyReadyCertificate(plan, snapshot, ready) != nil {
		t.Fatalf("coordinator-independent Ready certificate err=%v cert=%#v", err, ready)
	}
	if ready.Attestations[0].Signer.Replica == plan.Reservations.Prepare.Replica {
		t.Fatal("test accidentally retained original coordinator")
	}
}

func TestBootstrapEnvelopeAndChunkValidationIsCanonicalBoundedAndIdempotent(t *testing.T) {
	f := newBootstrapTestFixture(t, 1, 1)
	message, err := SignBootstrapMessage(BootstrapMessage{Type: BootstrapMsgReadyQuery, Cluster: f.cluster, Plan: f.planID, From: 1, FromIncarnation: 1, To: 1, BaseID: 1, BaseDigest: StateDigest{1}, Payload: []byte("{}")}, f.identities[0], f.private[0])
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeBootstrapMessage(nil, message)
	if err != nil {
		t.Fatal(err)
	}
	var decoded BootstrapMessage
	if err := DecodeBootstrapMessage(encoded, &decoded); err != nil || VerifyBootstrapMessage(decoded, f.identities[0]) != nil {
		t.Fatalf("round trip err=%v decoded=%#v", err, decoded)
	}
	if !bytes.Equal(decoded.Payload, message.Payload) {
		t.Fatal("decoded envelope payload mismatch")
	}
	if err := DecodeBootstrapMessage(append(encoded, 0), &decoded); !errors.Is(err, ErrInvalidBootstrapMessage) {
		t.Fatalf("trailing envelope err=%v", err)
	}
	chunk, err := SignBootstrapChunk(BootstrapChunk{Cluster: f.cluster, Plan: f.planID, From: 1, FromIncarnation: 1, To: 2, Manifest: StateDigest{2}, Index: 1, Offset: 0, Total: 3, Payload: []byte("abc")}, f.identities[0], f.private[0])
	if err != nil {
		t.Fatal(err)
	}
	set := BootstrapChunkSet{Limit: 3}
	if err := set.Add(chunk, f.identities[0]); err != nil {
		t.Fatal(err)
	}
	if err := set.Add(chunk, f.identities[0]); err != nil {
		t.Fatalf("exact duplicate: %v", err)
	}
	conflict := chunk.Clone()
	conflict.Index = 2
	conflict.PayloadDigest = StateDigest{}
	conflict.Signature = nil
	conflict.Payload = []byte("x")
	conflict.Offset = 1
	conflict, _ = SignBootstrapChunk(conflict, f.identities[0], f.private[0])
	if err := set.Add(conflict, f.identities[0]); !errors.Is(err, ErrBootstrapChunkConflict) {
		t.Fatalf("overlap err=%v", err)
	}
}
