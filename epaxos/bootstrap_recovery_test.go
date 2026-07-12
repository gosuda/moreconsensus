package epaxos

import (
	"errors"
	"reflect"
	"testing"
)

func drainBootstrapProtocolReady(t *testing.T, node *RawNode, store *MemoryStorage) []Message {
	t.Helper()
	var messages []Message
	for attempts := 0; attempts < 16 && node.HasReady(); attempts++ {
		rd := node.Ready()
		messages = append(messages, rd.Messages...)
		if err := store.ApplyReady(rd); err != nil {
			t.Fatalf("ApplyReady: %v", err)
		}
		if err := node.Advance(rd); err != nil {
			t.Fatalf("Advance: %v", err)
		}
	}
	return messages
}

func bootstrapMessageOfType(t *testing.T, messages []Message, typ MessageType, ref InstanceRef) Message {
	t.Helper()
	for _, message := range messages {
		if message.Type == typ && message.Ref == ref {
			return message.Clone()
		}
	}
	t.Fatalf("missing %s for %s in %#v", typ, ref, messages)
	return Message{}
}

func preparedBootstrapRecord(plan VoterPlan) BootstrapRecord {
	record := BootstrapRecord{Plan: plan.Clone(), Phase: BootstrapPhasePrepared}
	record.Digest = DigestBootstrapRecord(record)
	return record
}

func newBootstrapNodeFromRecord(t *testing.T, fixture bootstrapTestFixture, local ReplicaID, record BootstrapRecord) (*RawNode, *MemoryStorage) {
	t.Helper()
	store := NewMemoryStorage()
	store.Hard = HardState{Conf: record.Plan.Request.Base.Clone()}
	store.ConfigHistory = []ConfigHistoryEntry{{Conf: record.Plan.Request.Base.Clone()}}
	store.BootstrapRecords = []BootstrapRecord{record.Clone()}
	store.AllocatorFloor = record.Plan.Reservations.Abort.Instance + 1
	store.LocalVoterState = LocalVoterState{
		Cluster: fixture.cluster, Identity: fixture.identities[local-1].Clone(),
		Conf: record.Plan.Request.Base.Clone(), Status: LocalVoterStatusEligible,
		AllocatorFloor: store.AllocatorFloor,
	}
	node, err := NewRawNode(Config{
		ID: local, Voters: record.Plan.Request.Base.Voters, Cluster: fixture.cluster,
		LocalIdentity: fixture.identities[local-1], VoterIdentities: fixture.identities, Storage: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return node, store
}

func TestAllNoneReservedAbortRecoverySelectsAbortNotNoop(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	if err := fixture.node.RecoverVoterControl(plan, BootstrapExitAbort); err != nil {
		t.Fatalf("RecoverVoterControl abort: %v", err)
	}
	drainBootstrapProtocolReady(t, fixture.node, fixture.store)
	record, ok := fixture.store.Instance(plan.Reservations.Abort)
	if !ok || record.Status != StatusExecuted || record.Command.Kind != CommandMembership ||
		record.MembershipResult.Outcome != BootstrapOutcomeAborted {
		t.Fatalf("recovered abort record=%#v present=%v", record, ok)
	}
	wire, err := decodeMembershipCommand(record.Command)
	if err != nil || wire.Operation != membershipAbort {
		t.Fatalf("recovered all-none value=%#v err=%v", wire, err)
	}
	if fixture.node.Status().Conf.ID != plan.Request.Base.ID || len(fixture.node.BootstrapStatus().Closed) != 0 {
		t.Fatalf("abort changed config or left fence: status=%#v bootstrap=%#v", fixture.node.Status(), fixture.node.BootstrapStatus())
	}
}

func TestAllNoneActivateRequiresDurableReadyCertificate(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	before := fixture.node.Ready()
	if err := fixture.node.RecoverVoterControl(plan, BootstrapExitActivate); !errors.Is(err, ErrBootstrapSnapshot) {
		t.Fatalf("all-none Activate err=%v, want %v", err, ErrBootstrapSnapshot)
	}
	if _, exists := fixture.node.instances[plan.Reservations.Activate]; exists {
		t.Fatal("certificate-free Activate recovery materialized its reserved ref")
	}
	after := fixture.node.Ready()
	if len(after.Records) != len(before.Records) || len(after.Messages) != len(before.Messages) {
		t.Fatalf("rejected Activate recovery mutated Ready: before=%#v after=%#v", before, after)
	}
}

func TestReadyCertificateRequiresDurableTargetReadyRecord(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	fence := fenceBootstrapPlan(t, fixture, plan)
	snapshot := certifyBootstrapSnapshot(t, fixture, plan, fence)
	ready := readyBootstrapTarget(t, fixture, plan, snapshot)
	state := fixture.node.bootstrapPlans[plan.Request.Plan]
	record := state.record.Clone()
	record.Phase = BootstrapPhaseFinalizing
	record.SnapshotCertificate = snapshot.Clone()
	record.TargetReady = TargetReadyRecord{Plan: plan.Request.Plan, Proof: ready.Proof.Clone()}
	record.ReadyCertificate = ready.Clone()
	record.Digest = DigestBootstrapRecord(record)
	if err := validateBootstrapRecord(record); err != nil {
		t.Fatalf("valid durable target-ready record rejected: %v", err)
	}
	record.TargetReady = TargetReadyRecord{}
	record.Digest = DigestBootstrapRecord(record)
	if !errors.Is(validateBootstrapRecord(record), ErrBootstrapCertificate) {
		t.Fatal("ready certificate accepted without durable target-ready record")
	}
}

func TestBootstrapRecordRejectsUndigestedOptionalProofPayloads(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	fence := fenceBootstrapPlan(t, fixture, plan)
	snapshot := certifyBootstrapSnapshot(t, fixture, plan, fence)
	ready := readyBootstrapTarget(t, fixture, plan, snapshot)
	state := fixture.node.bootstrapPlans[plan.Request.Plan]
	valid := state.record.Clone()
	valid.Phase = BootstrapPhaseFinalizing
	valid.SnapshotCertificate = snapshot.Clone()
	valid.TargetReady = TargetReadyRecord{Plan: plan.Request.Plan, Proof: ready.Proof.Clone()}
	valid.ReadyCertificate = ready.Clone()
	valid.Digest = DigestBootstrapRecord(valid)
	if err := validateBootstrapRecord(valid); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*BootstrapRecord)
	}{
		{name: "local fence", mutate: func(record *BootstrapRecord) {
			record.LocalFence.Digest = StateDigest{}
		}},
		{name: "fence quorum", mutate: func(record *BootstrapRecord) {
			record.FenceQuorum.Digest = StateDigest{}
		}},
		{name: "snapshot certificate", mutate: func(record *BootstrapRecord) {
			record.SnapshotCertificate.Digest = StateDigest{}
		}},
		{name: "ready certificate", mutate: func(record *BootstrapRecord) {
			record.ReadyCertificate.Digest = StateDigest{}
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			mutated := valid.Clone()
			testCase.mutate(&mutated)
			mutated.Digest = DigestBootstrapRecord(mutated)
			if !errors.Is(validateBootstrapRecord(mutated), ErrBootstrapCertificate) {
				t.Fatal("undigested optional proof payload accepted")
			}
		})
	}
}

func TestReadyResponsePersistsTargetReadyProofBeforeFinalizing(t *testing.T) {
	fixture, plan := standaloneBootstrapPlan(t, 3)
	frontier := standaloneFrontier(plan, 0)
	fence1 := standaloneAdmissionFence(t, plan, fixture.identities[0], frontier)
	fence2 := standaloneAdmissionFence(t, plan, fixture.identities[1], frontier)
	fenceQuorum, err := BuildFenceQuorum(plan, []LocalAdmissionFence{fence1, fence2})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := standaloneSnapshotDescriptor(plan, fenceQuorum)
	snapshotDigest := DigestSnapshotDescriptor(descriptor)
	ack1, err := BuildVoterAcknowledgement(snapshotDigest, fixture.identities[0])
	if err != nil {
		t.Fatal(err)
	}
	ack2, err := BuildVoterAcknowledgement(snapshotDigest, fixture.identities[1])
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := BuildSnapshotCertificate(plan, descriptor, []VoterAcknowledgement{ack1, ack2})
	if err != nil {
		t.Fatal(err)
	}
	proof, err := BuildVoterReadyProof(VoterReadyProof{
		Cluster: fixture.cluster, Plan: plan.Request.Plan, Target: plan.Request.Target.Clone(),
		SnapshotDigest: snapshot.Digest, InstalledStateDigest: descriptor.InstalledStateDigest,
		AllocatorFloor: descriptor.TargetAllocatorFloor, TOQClosedThrough: descriptor.TOQClosedThrough,
	})
	if err != nil {
		t.Fatal(err)
	}
	readyDigest := DigestVoterReadyProof(proof)
	readyAck1, err := BuildVoterAcknowledgement(readyDigest, fixture.identities[0])
	if err != nil {
		t.Fatal(err)
	}
	readyAck2, err := BuildVoterAcknowledgement(readyDigest, fixture.identities[1])
	if err != nil {
		t.Fatal(err)
	}
	ready, err := BuildReadyCertificate(plan, proof, []VoterAcknowledgement{readyAck1, readyAck2})
	if err != nil {
		t.Fatal(err)
	}
	requesterRecord := preparedBootstrapRecord(plan)
	requesterRecord.Phase = BootstrapPhaseCertified
	requesterRecord.FenceQuorum = fenceQuorum.Clone()
	requesterRecord.SnapshotCertificate = snapshot.Clone()
	requesterRecord.Digest = DigestBootstrapRecord(requesterRecord)
	responderRecord := requesterRecord.Clone()
	responderRecord.Phase = BootstrapPhaseFinalizing
	responderRecord.TargetReady = TargetReadyRecord{Plan: plan.Request.Plan, Proof: proof.Clone()}
	responderRecord.ReadyCertificate = ready.Clone()
	responderRecord.Digest = DigestBootstrapRecord(responderRecord)
	requester, requesterStore := newBootstrapNodeFromRecord(t, fixture, 2, requesterRecord)
	responder, responderStore := newBootstrapNodeFromRecord(t, fixture, 1, responderRecord)

	query, err := BuildBootstrapMessage(BootstrapMessage{
		Type: BootstrapMsgReadyQuery, Cluster: fixture.cluster, Plan: plan.Request.Plan,
		From: 2, FromIncarnation: 1, To: 1, BaseID: plan.Request.Base.ID,
		BaseDigest: plan.RequestDigest, Payload: []byte("{}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := responder.StepBootstrapAuthenticated(query, query.From, query.FromIncarnation); err != nil {
		t.Fatalf("ReadyQuery: %v", err)
	}
	responderReady := responder.Ready()
	if len(responderReady.BootstrapMessages) != 1 || responderReady.BootstrapMessages[0].Type != BootstrapMsgReadyResponse {
		t.Fatalf("ReadyQuery response=%#v", responderReady)
	}
	response := responderReady.BootstrapMessages[0]
	if err := responderStore.ApplyReady(responderReady); err != nil {
		t.Fatal(err)
	}
	if err := responder.Advance(responderReady); err != nil {
		t.Fatal(err)
	}
	if err := requester.StepBootstrapAuthenticated(response, response.From, response.FromIncarnation); err != nil {
		t.Fatalf("ReadyResponse: %v", err)
	}
	requesterReady := requester.Ready()
	if !requesterReady.MustSync || len(requesterReady.BootstrapRecords) != 1 {
		t.Fatalf("ReadyResponse Ready=%#v", requesterReady)
	}
	durable := requesterReady.BootstrapRecords[0]
	if durable.TargetReady.Plan != plan.Request.Plan ||
		DigestVoterReadyProof(durable.TargetReady.Proof) != DigestVoterReadyProof(ready.Proof) ||
		durable.ReadyCertificate.Digest != ready.Digest ||
		validateBootstrapRecord(durable) != nil {
		t.Fatalf("ReadyResponse durable record=%#v", durable)
	}
	if err := requesterStore.ApplyReady(requesterReady); err != nil {
		t.Fatal(err)
	}
	if err := requester.Advance(requesterReady); err != nil {
		t.Fatal(err)
	}
	persisted := requester.BootstrapStatus().Plans[0]
	if persisted.TargetReady.Plan != plan.Request.Plan || persisted.ReadyCertificate.Digest != ready.Digest {
		t.Fatalf("persisted ReadyResponse record=%#v", persisted)
	}
	if err := requester.StepBootstrapAuthenticated(response, response.From, response.FromIncarnation); err != nil {
		t.Fatalf("duplicate ReadyResponse: %v", err)
	}
	if requester.HasReady() {
		t.Fatal("duplicate ReadyResponse enqueued durable work")
	}
}

func TestControlRecoveryUsesOldPerInstanceBallotsAndQuorumAfterOwnerCrash(t *testing.T) {
	fixture, plan := standaloneBootstrapPlan(t, 3)
	node, store := newBootstrapNodeFromRecord(t, fixture, 2, preparedBootstrapRecord(plan))
	if err := node.RecoverVoterControl(plan, BootstrapExitAbort); err != nil {
		t.Fatal(err)
	}
	messages := drainBootstrapProtocolReady(t, node, store)
	prepare := bootstrapMessageOfType(t, messages, MsgPrepare, plan.Reservations.Abort)
	if !prepare.Ballot.IsRecovery() || prepare.Ballot.Replica != 2 || prepare.Ref.Conf != plan.Request.Base.ID {
		t.Fatalf("recovery Prepare=%#v", prepare)
	}
	response := Message{
		Type: MsgPrepareResp, From: 1, To: 2, Ref: prepare.Ref, Ballot: prepare.Ballot,
		RecordStatus: StatusNone, Deps: make([]InstanceNum, len(plan.Request.Base.Voters)),
	}
	if err := node.Step(response); err != nil {
		t.Fatal(err)
	}
	messages = drainBootstrapProtocolReady(t, node, store)
	accept := bootstrapMessageOfType(t, messages, MsgAccept, plan.Reservations.Abort)
	if accept.Ballot != prepare.Ballot || accept.Command.Kind != CommandMembership {
		t.Fatalf("old-config Accept=%#v", accept)
	}
	acceptResponse := Message{
		Type: MsgAcceptResp, From: 1, To: 2, Ref: accept.Ref, Ballot: accept.Ballot,
		RecordBallot: accept.Ballot, Seq: accept.Seq, Deps: append([]InstanceNum(nil), accept.Deps...),
		RecordStatus: StatusAccepted,
	}
	if err := node.Step(acceptResponse); err != nil {
		t.Fatal(err)
	}
	drainBootstrapProtocolReady(t, node, store)
	record, ok := store.Instance(plan.Reservations.Abort)
	if !ok || record.Status != StatusExecuted || record.MembershipResult.Outcome != BootstrapOutcomeAborted {
		t.Fatalf("old-quorum recovery record=%#v present=%v", record, ok)
	}
	if _, exists := store.Records[plan.Reservations.Activate]; exists {
		t.Fatal("Abort recovery consumed the distinct Activate ballot/ref")
	}
}

func installCommittedBootstrapControl(node *RawNode, ref InstanceRef, command Command) {
	record := InstanceRecord{
		Ref: ref, Ballot: Ballot{Replica: ref.Replica}, RecordBallot: Ballot{Replica: ref.Replica},
		Status: StatusCommitted, Seq: 1, Deps: node.depsForConf(ref.Conf), Command: command.Clone(),
		TimingDomain: TimingDomainUntimed,
	}
	record.Checksum = ChecksumRecord(record)
	node.installInstance(&instance{rec: record, phase: phaseCommitted})
}

func runBootstrapExitOrder(t *testing.T, reverse bool) (MembershipResult, MembershipResult, ConfState) {
	t.Helper()
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	fence := fenceBootstrapPlan(t, fixture, plan)
	snapshot := certifyBootstrapSnapshot(t, fixture, plan, fence)
	ready := readyBootstrapTarget(t, fixture, plan, snapshot)
	state := fixture.node.bootstrapPlans[plan.Request.Plan]
	state.record.Phase = BootstrapPhaseFinalizing
	state.record.SnapshotCertificate = snapshot.Clone()
	state.record.TargetReady = TargetReadyRecord{Plan: plan.Request.Plan, Proof: ready.Proof.Clone()}
	state.record.ReadyCertificate = ready.Clone()
	state.record.Digest = DigestBootstrapRecord(state.record)
	state.durablePhase = state.record.Phase
	state.durableDigest = state.record.Digest
	activate, err := encodeMembershipCommand(membershipCommandWire{
		Operation: membershipActivate, Plan: plan.Clone(), Snapshot: snapshot.Clone(), Ready: ready.Clone(), FenceDigest: fence.Digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	abort, err := encodeMembershipCommand(membershipCommandWire{Operation: membershipAbort, Plan: plan.Clone(), FenceDigest: fence.Digest})
	if err != nil {
		t.Fatal(err)
	}
	if reverse {
		installCommittedBootstrapControl(fixture.node, plan.Reservations.Abort, abort)
		installCommittedBootstrapControl(fixture.node, plan.Reservations.Activate, activate)
	} else {
		installCommittedBootstrapControl(fixture.node, plan.Reservations.Activate, activate)
		installCommittedBootstrapControl(fixture.node, plan.Reservations.Abort, abort)
	}
	fixture.node.tryExecute()
	return fixture.node.instances[plan.Reservations.Activate].rec.MembershipResult,
		fixture.node.instances[plan.Reservations.Abort].rec.MembershipResult, fixture.node.Status().Conf
}

func TestActivateAndAbortHaveOneDeterministicTerminalWinner(t *testing.T) {
	activate, abort, conf := runBootstrapExitOrder(t, false)
	if activate.Outcome != BootstrapOutcomeActivated || abort.Outcome != BootstrapOutcomeRejectedSuperseded || conf.ID != 2 {
		t.Fatalf("terminal outcomes activate=%#v abort=%#v conf=%#v", activate, abort, conf)
	}
}

func TestBootstrapReadyCausallyFencesSuccessorMessages(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	fence := fenceBootstrapPlan(t, fixture, plan)
	snapshot := certifyBootstrapSnapshot(t, fixture, plan, fence)
	ready := readyBootstrapTarget(t, fixture, plan, snapshot)
	if _, err := fixture.node.ActivateVoter(plan, snapshot, ready); err != nil {
		t.Fatal(err)
	}
	message := Message{
		Type: MsgPrepare, From: fixture.target.Replica, To: 1,
		Ref:    InstanceRef{Replica: fixture.target.Replica, Instance: 1, Conf: plan.Successor.ID},
		Ballot: Ballot{Epoch: 1, Replica: fixture.target.Replica},
	}
	if err := fixture.node.Step(message); !errors.Is(err, ErrBootstrapEligibility) {
		t.Fatalf("successor message before activation durability err=%v", err)
	}
	rd := fixture.node.Ready()
	if rd.LocalVoterState == nil || !rd.MustSync {
		t.Fatalf("activation Ready=%#v", rd)
	}
	if err := fixture.store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := fixture.node.Advance(rd); err != nil {
		t.Fatal(err)
	}
	if err := fixture.node.Step(message); err != nil {
		t.Fatalf("successor message after activation durability: %v", err)
	}
}

func restartBootstrapFixture(t *testing.T, fixture bootstrapTestFixture, voters []ReplicaID, identities []VoterIdentity) *RawNode {
	t.Helper()
	node, err := NewRawNode(Config{
		ID: 1, Voters: voters, Cluster: fixture.cluster, LocalIdentity: fixture.identities[0],
		VoterIdentities: identities, Storage: fixture.store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func TestBootstrapReplayRestoresPreparedFencedActivatedAndAbortedStates(t *testing.T) {
	t.Run("prepared", func(t *testing.T) {
		fixture := newBootstrapTestFixture(t, 1, 1)
		plan := prepareBootstrapPlan(t, fixture)
		restarted := restartBootstrapFixture(t, fixture, plan.Request.Base.Voters, fixture.identities)
		status := restarted.BootstrapStatus()
		if len(status.Plans) != 1 || status.Plans[0].Phase != BootstrapPhasePrepared || status.Plans[0].Plan.Reservations != plan.Reservations {
			t.Fatalf("prepared replay=%#v", status)
		}
	})
	t.Run("fenced", func(t *testing.T) {
		fixture := newBootstrapTestFixture(t, 1, 1)
		plan := prepareBootstrapPlan(t, fixture)
		fenceBootstrapPlan(t, fixture, plan)
		restarted := restartBootstrapFixture(t, fixture, plan.Request.Base.Voters, fixture.identities)
		status := restarted.BootstrapStatus()
		if len(status.Plans) != 1 || status.Plans[0].Phase != BootstrapPhaseFenced || len(status.Closed) != 1 {
			t.Fatalf("fenced replay=%#v", status)
		}
	})
	t.Run("activated", func(t *testing.T) {
		fixture := newBootstrapTestFixture(t, 1, 1)
		plan := prepareBootstrapPlan(t, fixture)
		fence := fenceBootstrapPlan(t, fixture, plan)
		snapshot := certifyBootstrapSnapshot(t, fixture, plan, fence)
		ready := readyBootstrapTarget(t, fixture, plan, snapshot)
		if _, err := fixture.node.ActivateVoter(plan, snapshot, ready); err != nil {
			t.Fatal(err)
		}
		drainBootstrapProtocolReady(t, fixture.node, fixture.store)
		identities := append(cloneVoterIdentities(fixture.identities), fixture.target.Clone())
		restarted := restartBootstrapFixture(t, fixture, plan.Request.Base.Voters, identities)
		status := restarted.BootstrapStatus()
		if len(status.Plans) != 1 || status.Plans[0].Phase != BootstrapPhaseActivated || restarted.Status().Conf.ID != plan.Successor.ID || len(status.Closed) != 1 {
			t.Fatalf("activated replay=%#v conf=%#v", status, restarted.Status().Conf)
		}
	})
	t.Run("aborted", func(t *testing.T) {
		fixture := newBootstrapTestFixture(t, 1, 1)
		plan := prepareBootstrapPlan(t, fixture)
		if err := fixture.node.RecoverVoterControl(plan, BootstrapExitAbort); err != nil {
			t.Fatal(err)
		}
		drainBootstrapProtocolReady(t, fixture.node, fixture.store)
		restarted := restartBootstrapFixture(t, fixture, plan.Request.Base.Voters, fixture.identities)
		status := restarted.BootstrapStatus()
		if len(status.Plans) != 1 || status.Plans[0].Phase != BootstrapPhaseAborted || len(status.Closed) != 0 || restarted.Status().Conf.ID != plan.Request.Base.ID {
			t.Fatalf("aborted replay=%#v conf=%#v", status, restarted.Status().Conf)
		}
	})
}

func TestAddedVoterNeverCountsForOldPinnedRecoveryAfterCertifiedActivation(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	fence := fenceBootstrapPlan(t, fixture, plan)
	snapshot := certifyBootstrapSnapshot(t, fixture, plan, fence)
	ready := readyBootstrapTarget(t, fixture, plan, snapshot)
	if _, err := fixture.node.ActivateVoter(plan, snapshot, ready); err != nil {
		t.Fatal(err)
	}
	drainBootstrapProtocolReady(t, fixture.node, fixture.store)
	activated := fixture.node.BootstrapStatus().Plans[0]
	targetStore := NewMemoryStorage()
	targetStore.Hard = HardState{Conf: plan.Successor.Clone()}
	targetStore.ConfigHistory = []ConfigHistoryEntry{
		{Conf: plan.Request.Base.Clone()},
		{Conf: plan.Successor.Clone(), AppliedRef: plan.Reservations.Activate, IdentityDigest: bootstrapIdentityDigest(plan)},
	}
	targetStore.BootstrapRecords = []BootstrapRecord{activated.Clone()}
	targetStore.AllocatorFloor = ready.Proof.AllocatorFloor
	targetStore.LocalVoterState = LocalVoterState{
		Cluster: fixture.cluster, Identity: fixture.target.Clone(), Conf: plan.Successor.Clone(),
		Status: LocalVoterStatusEligible, Plan: plan.Request.Plan,
		InstalledDigest: snapshot.Descriptor.InstalledStateDigest, AllocatorFloor: ready.Proof.AllocatorFloor,
	}
	identities := append(cloneVoterIdentities(fixture.identities), fixture.target.Clone())
	target, err := NewRawNode(Config{
		ID: fixture.target.Replica, Voters: plan.Request.Base.Voters, Cluster: fixture.cluster,
		LocalIdentity: fixture.target, VoterIdentities: identities, Storage: targetStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	old := Message{
		Type: MsgPrepare, From: 1, To: fixture.target.Replica,
		Ref:    InstanceRef{Replica: 1, Instance: 50, Conf: plan.Request.Base.ID},
		Ballot: Ballot{Epoch: 1, Replica: 1},
	}
	if err := target.Step(old); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("added voter accepted old-config vote: %v", err)
	}
	if target.HasReady() {
		t.Fatalf("rejected old-config vote created Ready: %#v", target.Ready())
	}
}

func TestRemovedCertifiedVoterStillServesPinnedHistoricalRecovery(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 3, 3)
	result := fixture.node.applyConfChange(
		InstanceRef{Replica: 1, Instance: 10, Conf: 1},
		confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}),
	)
	if result.Outcome != ConfChangeApplied || result.Conf.Contains(3) {
		t.Fatalf("remove result=%#v", result)
	}
	rd := fixture.node.Ready()
	if rd.LocalVoterState == nil || rd.LocalVoterState.Status != LocalVoterStatusIneligible {
		t.Fatalf("removal Ready lacks durable ineligibility: %#v", rd)
	}
	if err := fixture.store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if err := fixture.node.Advance(rd); err != nil {
		t.Fatal(err)
	}
	message := Message{
		Type: MsgPrepare, From: 1, To: 3,
		Ref:    InstanceRef{Replica: 1, Instance: 50, Conf: 1},
		Ballot: Ballot{Epoch: 1, Replica: 1},
	}
	if err := fixture.node.Step(message); err != nil {
		t.Fatalf("removed voter rejected historical recovery: %v", err)
	}
	if !fixture.node.HasReady() {
		t.Fatal("historical recovery produced no durable promise/response")
	}
	if _, err := fixture.node.Propose(Command{Payload: []byte("new-work")}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("removed voter proposed new work: %v", err)
	}
}

func TestNewRawNodeRejectsCurrentMembershipWithoutDurableEligibility(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	store := NewMemoryStorage()
	store.Hard = HardState{Conf: ConfState{ID: 1, Voters: []ReplicaID{1}}}
	store.ConfigHistory = []ConfigHistoryEntry{{Conf: store.Hard.Conf.Clone()}}
	if _, err := NewRawNode(Config{
		ID: 1, Voters: []ReplicaID{1}, Cluster: fixture.cluster,
		LocalIdentity: fixture.identities[0], VoterIdentities: fixture.identities, Storage: store,
	}); !errors.Is(err, ErrBootstrapEligibility) {
		t.Fatalf("startup without durable eligibility err=%v", err)
	}
}

func TestCertifiedNextInstanceFloorIsMonotonicSparseAndExhaustionSafe(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	fixture.node.nextInstance = 50
	fixture.node.allocatorFloor = 50
	plan, err := fixture.node.PrepareVoter(fixture.request())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Reservations.Prepare.Instance != 50 || fixture.node.nextInstance != 53 || fixture.node.allocatorFloor != 53 {
		t.Fatalf("reservations=%#v next=%d floor=%d", plan.Reservations, fixture.node.nextInstance, fixture.node.allocatorFloor)
	}
	drainBootstrapProtocolReady(t, fixture.node, fixture.store)
	restarted := restartBootstrapFixture(t, fixture, plan.Request.Base.Voters, fixture.identities)
	if restarted.nextInstance < 53 || restarted.allocatorFloor < 53 {
		t.Fatalf("restarted next=%d floor=%d", restarted.nextInstance, restarted.allocatorFloor)
	}

	exhausted := newBootstrapTestFixture(t, 1, 1)
	exhausted.node.nextInstance = ^InstanceNum(0) - 2
	exhausted.node.allocatorFloor = exhausted.node.nextInstance
	if _, err := exhausted.node.PrepareVoter(exhausted.request()); !errors.Is(err, ErrInstanceExhausted) {
		t.Fatalf("exhausted Prepare err=%v", err)
	}
	if exhausted.node.HasReady() || len(exhausted.node.bootstrapPlans) != 0 || exhausted.node.nextInstance != ^InstanceNum(0)-2 {
		t.Fatal("exhausted reservation mutated node")
	}

	badFloor := newBootstrapTestFixture(t, 1, 1)
	request := badFloor.request()
	request.TargetAllocatorFloor = ^InstanceNum(0)
	if _, err := badFloor.node.PrepareVoter(request); err == nil {
		t.Fatal("MaxUint64 target allocator floor was accepted")
	}
	if badFloor.node.HasReady() {
		t.Fatal("rejected target allocator floor created Ready")
	}
}

func TestPrepareVoterReaddRequiresNewIncarnationAndSafeFloor(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	fixture.node.voterIdentities[fixture.target.Replica] = fixture.target.Clone()
	historical := InstanceRef{Replica: fixture.target.Replica, Instance: 9, Conf: 1}
	fixture.node.instances[historical] = &instance{rec: InstanceRecord{Ref: historical, Status: StatusExecuted}}

	request := fixture.request()
	if _, err := fixture.node.PrepareVoter(request); !errors.Is(err, ErrBootstrapEligibility) {
		t.Fatalf("same-incarnation re-add err=%v", err)
	}
	if fixture.node.HasReady() {
		t.Fatal("rejected same-incarnation re-add mutated Ready")
	}

	request.Target.Incarnation++
	request.TargetAllocatorFloor = historical.Instance
	if _, err := fixture.node.PrepareVoter(request); !errors.Is(err, ErrInstanceExhausted) {
		t.Fatalf("unsafe re-add allocator floor err=%v", err)
	}
	if fixture.node.HasReady() {
		t.Fatal("rejected re-add allocator floor mutated Ready")
	}

	request.TargetAllocatorFloor = historical.Instance + 1
	if _, err := fixture.node.PrepareVoter(request); err != nil {
		t.Fatalf("safe re-add rejected: %v", err)
	}
}

func newTOQBootstrapFixture(t *testing.T) bootstrapTestFixture {
	t.Helper()
	fixture := newBootstrapTestFixture(t, 1, 1)
	base := fixture.node.Status().Conf
	runtime := TOQRuntimeConfig{Conf: base.Clone(), OneWayDelay: map[ReplicaID]uint64{1: 0}}
	node, err := NewRawNode(Config{
		ID: 1, Voters: base.Voters, Cluster: fixture.cluster,
		LocalIdentity: fixture.identities[0], VoterIdentities: fixture.identities, Storage: fixture.store,
		TOQ: true, TOQClock: func() uint64 { return 10 }, TOQRuntime: &runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.node = node
	return fixture
}

func TestBootstrapTOQRuntimeAndClosedFloorMustMatchSuccessor(t *testing.T) {
	fixture := newTOQBootstrapFixture(t)
	request := fixture.request()
	request.TOQ = true
	request.SuccessorTOQ = TOQRuntimeConfig{
		Conf:        ConfState{ID: request.Base.ID + 1, Voters: []ReplicaID{1, fixture.target.Replica}},
		OneWayDelay: map[ReplicaID]uint64{1: 0, fixture.target.Replica: 1},
	}
	if _, err := fixture.node.PrepareVoter(request); err != nil {
		t.Fatalf("valid TOQ bootstrap: %v", err)
	}

	mismatch := newTOQBootstrapFixture(t)
	bad := mismatch.request()
	bad.TOQ = true
	bad.TOQClosedThrough = 1
	bad.SuccessorTOQ = request.SuccessorTOQ.Clone()
	if _, err := mismatch.node.PrepareVoter(bad); !errors.Is(err, ErrBootstrapSnapshot) {
		t.Fatalf("closed-floor mismatch err=%v", err)
	}
	if mismatch.node.HasReady() {
		t.Fatal("TOQ floor mismatch mutated Ready")
	}

	missingRuntime := newTOQBootstrapFixture(t)
	bad = missingRuntime.request()
	bad.TOQ = true
	if _, err := missingRuntime.node.PrepareVoter(bad); err == nil {
		t.Fatal("missing successor TOQ runtime accepted")
	}
	if missingRuntime.node.HasReady() {
		t.Fatal("missing successor TOQ runtime mutated Ready")
	}
}

func TestMemoryStorageBootstrapBatchValidationIsAtomic(t *testing.T) {
	fixture := newBootstrapTestFixture(t, 1, 1)
	plan := prepareBootstrapPlan(t, fixture)
	before := fixture.store.deepClone()
	record := fixture.store.BootstrapRecords[0].Clone()
	record.Phase = BootstrapPhaseLocalFenced
	record.Digest = DigestBootstrapRecord(record)
	err := fixture.store.ApplyReady(Ready{
		BootstrapRecords: []BootstrapRecord{record},
		AllocatorFloor:   plan.Reservations.Abort.Instance,
		MustSync:         true,
	})
	if err == nil {
		t.Fatal("invalid bootstrap batch accepted")
	}
	if !reflect.DeepEqual(before, fixture.store) {
		t.Fatalf("invalid bootstrap batch partially mutated storage: before=%#v after=%#v", before, fixture.store)
	}

	activatedFixture := newBootstrapTestFixture(t, 1, 1)
	activatedPlan := prepareBootstrapPlan(t, activatedFixture)
	fence := fenceBootstrapPlan(t, activatedFixture, activatedPlan)
	snapshot := certifyBootstrapSnapshot(t, activatedFixture, activatedPlan, fence)
	ready := readyBootstrapTarget(t, activatedFixture, activatedPlan, snapshot)
	if _, err := activatedFixture.node.ActivateVoter(activatedPlan, snapshot, ready); err != nil {
		t.Fatal(err)
	}
	drainBootstrapProtocolReady(t, activatedFixture.node, activatedFixture.store)
	activated := activatedFixture.node.BootstrapStatus().Plans[0]

	incomplete := NewMemoryStorage()
	incomplete.Hard = HardState{Conf: activatedPlan.Request.Base.Clone()}
	incomplete.ConfigHistory = []ConfigHistoryEntry{{Conf: activatedPlan.Request.Base.Clone()}}
	incomplete.AllocatorFloor = 1
	incomplete.LocalVoterState = LocalVoterState{
		Cluster: activatedFixture.cluster, Identity: activatedFixture.identities[0].Clone(),
		Conf: activatedPlan.Request.Base.Clone(), Status: LocalVoterStatusEligible, AllocatorFloor: 1,
	}
	incompleteBefore, err := incomplete.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if err := incomplete.ApplyReady(Ready{BootstrapRecords: []BootstrapRecord{activated}, MustSync: true}); err == nil {
		t.Fatal("activated bootstrap record persisted without its causal configuration batch")
	}
	incompleteAfter, stateErr := incomplete.InitialState()
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if !reflect.DeepEqual(incompleteBefore, incompleteAfter) || len(incomplete.Records) != 0 {
		t.Fatalf("causally incomplete activation partially mutated storage: before=%#v after=%#v", incompleteBefore, incompleteAfter)
	}
}

func TestDueRecoveryTimersShareBoundedFairDriveBudget(t *testing.T) {
	node, err := NewRawNode(Config{
		ID: 1, Voters: []ReplicaID{1}, RecoveryTicks: 1,
		MaxDependencyRecoveriesPerDrive: 2, MaxConcurrentRecoveries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for instanceNumber := InstanceNum(1); instanceNumber <= 5; instanceNumber++ {
		record := InstanceRecord{
			Ref:  InstanceRef{Replica: 1, Instance: instanceNumber, Conf: 1},
			Deps: []InstanceNum{0}, TimingDomain: TimingDomainUntimed,
		}
		record.Checksum = ChecksumRecord(record)
		inst := &instance{rec: record, phase: phaseIdle}
		node.installInstance(inst)
		if err := node.scheduleRecovery(inst); err != nil {
			t.Fatal(err)
		}
	}
	for tick, wantExecuted := range []int{2, 4, 5} {
		if err := node.Tick(); err != nil {
			t.Fatalf("Tick %d: %v", tick+1, err)
		}
		executed := 0
		for instanceNumber := InstanceNum(1); instanceNumber <= 5; instanceNumber++ {
			if node.instances[InstanceRef{Replica: 1, Instance: instanceNumber, Conf: 1}].rec.Status == StatusExecuted {
				executed++
			}
		}
		if executed != wantExecuted {
			t.Fatalf("Tick %d executed %d recoveries, want %d", tick+1, executed, wantExecuted)
		}
	}
}

func TestLiveOldQuorumAfterFencerCrashesCanFinalizeButCannotChooseOrdinaryWork(t *testing.T) {
	for voters := 3; voters <= 6; voters++ {
		t.Run(string(rune('0'+voters)), func(t *testing.T) {
			fixture, plan := standaloneBootstrapPlan(t, voters)
			frontier := standaloneFrontier(plan, 0)
			quorum := slowQuorumSize(voters)
			fences := make([]LocalAdmissionFence, quorum)
			fenceSet := make(map[ReplicaID]struct{})
			for i := 0; i < quorum; i++ {
				fences[i] = standaloneAdmissionFence(t, plan, fixture.identities[i], frontier)
				fenceSet[fixture.identities[i].Replica] = struct{}{}
			}
			quorumCertificate, err := BuildFenceQuorum(plan, fences)
			if err != nil {
				t.Fatal(err)
			}
			for mask := 0; mask < 1<<voters; mask++ {
				if bitsSet(mask) != quorum {
					continue
				}
				intersects := false
				for index := 0; index < voters; index++ {
					if mask&(1<<index) != 0 {
						_, intersects = fenceSet[ReplicaID(index+1)]
						if intersects {
							break
						}
					}
				}
				if !intersects {
					t.Fatalf("live old quorum %#x misses fence quorum", mask)
				}
			}
			state := preparedBootstrapRecord(plan)
			if err := fixture.node.restoreBootstrapRecord(state); err != nil {
				t.Fatal(err)
			}
			if err := fixture.node.ApplyFenceQuorum(quorumCertificate); err != nil {
				t.Fatal(err)
			}
			ordinary := Message{
				Type: MsgPreAccept, From: 1, To: 1,
				Ref:    InstanceRef{Replica: 1, Instance: plan.Reservations.Abort.Instance + 1, Conf: plan.Request.Base.ID},
				Ballot: Ballot{Replica: 1}, Seq: 1, Deps: make([]InstanceNum, voters),
				Command: Command{Payload: []byte("post-fence")},
			}
			if err := fixture.node.Step(ordinary); !errors.Is(err, ErrBootstrapFenced) {
				t.Fatalf("post-fence ordinary admission err=%v", err)
			}
			if _, err := fixture.node.AbortVoter(plan); err != nil {
				t.Fatalf("old-config exit could not be proposed: %v", err)
			}
		})
	}
}

func bitsSet(value int) int {
	count := 0
	for value != 0 {
		value &= value - 1
		count++
	}
	return count
}

func TestActivateAbortRaceReplaysOneTerminalExitAcrossAllDeliveryOrders(t *testing.T) {
	forwardActivate, forwardAbort, forwardConf := runBootstrapExitOrder(t, false)
	reverseActivate, reverseAbort, reverseConf := runBootstrapExitOrder(t, true)
	if !membershipResultEqual(forwardActivate, reverseActivate) || !membershipResultEqual(forwardAbort, reverseAbort) ||
		!confStateEqual(forwardConf, reverseConf) || forwardActivate.Outcome != BootstrapOutcomeActivated ||
		forwardAbort.Outcome != BootstrapOutcomeRejectedSuperseded {
		t.Fatalf("delivery-order divergence: forward=(%#v,%#v,%#v) reverse=(%#v,%#v,%#v)",
			forwardActivate, forwardAbort, forwardConf, reverseActivate, reverseAbort, reverseConf)
	}
}
