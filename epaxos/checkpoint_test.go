package epaxos

import (
	"bytes"
	"errors"
	"slices"
	"testing"
)

func advanceCheckpointReady(t *testing.T, node *RawNode, storage *MemoryStorage, ready Ready) {
	t.Helper()
	if err := storage.ApplyReady(ready); err != nil {
		t.Fatal(err)
	}
	if err := node.Advance(ready); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointCertifiedCompactionLifecycle(t *testing.T) {
	storage := NewMemoryStorage()
	node, err := NewRawNode(Config{
		ID: 1, Voters: []ReplicaID{1}, Storage: storage,
		RetainExecutedPerLane: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if node.HasReady() {
		advanceCheckpointReady(t, node, storage, node.Ready())
	}

	command := Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("value"), Footprint: Footprint{Points: [][]byte{[]byte("key")}}}
	ref, err := node.Propose(command)
	if err != nil {
		t.Fatal(err)
	}
	ready := node.Ready()
	if len(ready.Apply) != 1 || ready.Apply[0].Ref != ref {
		t.Fatalf("proposal Ready.Apply=%#v, want %s", ready.Apply, ref)
	}
	advanceCheckpointReady(t, node, storage, ready)

	var request CheckpointRequest
	for step := 0; step < 8 && request.ID == (CheckpointID{}); step++ {
		if err := node.Tick(); err != nil {
			t.Fatal(err)
		}
		ready = node.Ready()
		if ready.Checkpoint != nil {
			request = ready.Checkpoint.Clone()
			break
		}
		advanceCheckpointReady(t, node, storage, ready)
	}
	if request.ID == (CheckpointID{}) {
		t.Fatal("checkpoint request was not emitted")
	}
	var applicationDigest StateDigest
	applicationDigest[0] = 1
	result := CheckpointResult{ID: request.ID, ApplicationSnapshot: []byte("snapshot-handle"), ApplicationDigest: applicationDigest}
	if err := node.ProvideCheckpoint(result); err != nil {
		t.Fatal(err)
	}
	if err := node.ProvideCheckpoint(result); err != nil {
		t.Fatalf("exact duplicate checkpoint result: %v", err)
	}
	conflict := result
	conflict.ApplicationDigest[1] = 1
	if err := node.ProvideCheckpoint(conflict); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("conflicting checkpoint result err=%v, want mismatch", err)
	}
	advanceCheckpointReady(t, node, storage, ready)

	var prepared, certified, compacted bool
	for step := 0; step < 8 && node.HasReady(); step++ {
		ready = node.Ready()
		if ready.Snapshot != nil {
			if ready.Snapshot.Mode != SnapshotPersistLocal {
				t.Fatalf("local checkpoint snapshot mode=%d", ready.Snapshot.Mode)
			}
			prepared = true
			certified = certified || checkpointCertified(ready.Snapshot.Checkpoint)
		}
		if len(ready.Compact) != 0 {
			if !certified {
				t.Fatal("compaction emitted before durable certificate")
			}
			compacted = true
		}
		advanceCheckpointReady(t, node, storage, ready)
	}
	if !prepared || !certified || !compacted {
		t.Fatalf("checkpoint lifecycle prepared=%t certified=%t compacted=%t", prepared, certified, compacted)
	}
	if !checkpointCertified(storage.Checkpoint) || !executionFrontierEqual(storage.Checkpoint.CompactedThrough, storage.Checkpoint.Descriptor.Through) {
		t.Fatalf("durable checkpoint=%#v, want certified compacted frontier", storage.Checkpoint)
	}
	if _, ok := storage.Records[ref]; ok {
		t.Fatalf("compacted application record %s remains durable", ref)
	}
	if _, found, err := storage.LoadInstance(ref); err != nil || found {
		t.Fatalf("compacted LoadInstance found=%t err=%v", found, err)
	}
}

func TestCheckpointCertificateMismatchAndMessageIncarnation(t *testing.T) {
	descriptor := CheckpointDescriptor{
		Conf:   ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}},
		Voters: []VoterIdentity{{Replica: 1, Incarnation: 10}, {Replica: 2, Incarnation: 20}, {Replica: 3, Incarnation: 30}},
	}
	digest := DigestCheckpointDescriptor(descriptor)
	acks := []VoterAcknowledgement{
		{Voter: descriptor.Voters[0], AttestedDigest: digest},
		{Voter: descriptor.Voters[1], AttestedDigest: digest},
	}
	certificate, err := BuildCheckpointCertificate(descriptor, acks)
	if err != nil {
		t.Fatal(err)
	}
	if certificate.DescriptorDigest != digest || certificate.Checksum != DigestCheckpointCertificate(certificate) {
		t.Fatal("certificate did not bind the exact descriptor digest")
	}
	acks[1].AttestedDigest[0] ^= 1
	if _, err := BuildCheckpointCertificate(descriptor, acks); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("mismatched attestation err=%v, want invalid checkpoint", err)
	}

	cluster := ClusterID{1}
	storage := NewMemoryStorage()
	identities := []VoterIdentity{{Replica: 1, Incarnation: 10}, {Replica: 2, Incarnation: 20}, {Replica: 3, Incarnation: 30}}
	storage.LocalVoterState = LocalVoterState{
		Cluster: cluster, Identity: identities[0],
		Conf:   ConfState{ID: 1, Voters: []ReplicaID{1, 2, 3}},
		Status: LocalVoterStatusEligible, AllocatorFloor: 1,
	}
	node, err := NewRawNode(Config{
		ID: 1, Voters: []ReplicaID{1, 2, 3}, Storage: storage, Cluster: cluster,
		LocalIdentity: identities[0], VoterIdentities: identities,
	})
	if err != nil {
		t.Fatal(err)
	}
	if node.HasReady() {
		advanceCheckpointReady(t, node, storage, node.Ready())
	}
	message := Message{
		Type: MsgPreAccept, From: 2, To: 1, FromIncarnation: 19, ToIncarnation: 10,
		Ref: InstanceRef{Conf: 1, Replica: 2, Instance: 1}, Ballot: Ballot{Replica: 2},
		Kind: EntryCommand, Seq: 1,
		Command: Command{ID: CommandID{Client: 2, Sequence: 1}, Footprint: Footprint{All: true}},
		Deps:    []InstanceNum{0, 0, 0},
	}
	message.Checksum = ChecksumMessage(message)
	before := node.Status()
	if err := node.Step(message); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("wrong-incarnation Step err=%v, want rejected", err)
	}
	after := node.Status()
	if len(after.Instances) != len(before.Instances) || !slices.Equal(after.Conf.Voters, before.Conf.Voters) {
		t.Fatal("wrong-incarnation message mutated protocol state")
	}
}

func TestCheckpointSchedulerKeepsOnePendingLocalBarrier(t *testing.T) {
	node, err := NewRawNode(Config{
		ID: 1, Voters: []ReplicaID{1, 2, 3},
		RetainExecutedPerLane: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	lane := instanceLane{conf: 1, replica: 1}
	node.executed.through[lane] = 5
	node.durableExecuted.through[lane] = 5
	node.committedThrough[lane] = 5
	node.nextInstance = 6

	if err := node.maybeScheduleCheckpointBarrier(); err != nil {
		t.Fatal(err)
	}
	firstNext := node.nextInstance
	var barriers int
	for _, inst := range node.instances {
		if inst != nil && inst.rec.Kind == EntryCheckpoint {
			barriers++
		}
	}
	if barriers != 1 {
		t.Fatalf("scheduled barriers=%d, want one", barriers)
	}
	if err := node.maybeScheduleCheckpointBarrier(); err != nil {
		t.Fatal(err)
	}
	if node.nextInstance != firstNext {
		t.Fatalf("next instance advanced from %d to %d for duplicate pending barrier", firstNext, node.nextInstance)
	}
}

func TestConcurrentCheckpointBarriersExecuteAsOneCycle(t *testing.T) {
	node, err := NewRawNode(Config{
		ID: 1, Voters: []ReplicaID{1, 2, 3},
		RetainExecutedPerLane: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	lane := instanceLane{conf: 1, replica: 1}
	node.executed.through[lane] = 12
	node.durableExecuted.through[lane] = 12
	node.committedThrough[lane] = 12

	records := []InstanceRecord{
		{
			Ref:    InstanceRef{Conf: 1, Replica: 1, Instance: 13},
			Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1},
			Status: StatusCommitted, Seq: 14, Deps: []InstanceNum{12, 1, 0},
			Kind: EntryCheckpoint, ProtocolControl: append([]byte(nil), checkpointBarrierPayload...),
		},
		{
			Ref:    InstanceRef{Conf: 1, Replica: 2, Instance: 1},
			Ballot: Ballot{Replica: 2}, RecordBallot: Ballot{Replica: 2},
			Status: StatusCommitted, Seq: 15, Deps: []InstanceNum{13, 0, 1},
			Kind: EntryCheckpoint, ProtocolControl: append([]byte(nil), checkpointBarrierPayload...),
		},
		{
			Ref:    InstanceRef{Conf: 1, Replica: 3, Instance: 1},
			Ballot: Ballot{Replica: 3}, RecordBallot: Ballot{Replica: 3},
			Status: StatusCommitted, Seq: 16, Deps: []InstanceNum{13, 1, 0},
			Kind: EntryCheckpoint, ProtocolControl: append([]byte(nil), checkpointBarrierPayload...),
		},
	}
	for i := range records {
		records[i].Checksum = ChecksumRecord(records[i])
		node.installInstance(&instance{rec: records[i]})
	}

	node.tryExecute()
	for _, record := range records {
		inst := node.instances[record.Ref]
		if inst == nil || inst.rec.Status != StatusExecuted {
			t.Fatalf("barrier %s status=%v, want executed", record.Ref, inst)
		}
	}
	wantBarrier := records[len(records)-1].Ref
	if node.checkpointRound == nil || node.checkpointRound.barrier != wantBarrier {
		t.Fatalf("checkpoint round=%#v, want barrier %s", node.checkpointRound, wantBarrier)
	}
}

func completeSingleReplicaCheckpoint(t *testing.T, node *RawNode, storage *MemoryStorage, sequence byte) Checkpoint {
	t.Helper()
	previousID := storage.Checkpoint.Descriptor.ID
	command := Command{
		ID: CommandID{Client: 9, Sequence: uint64(sequence)}, Payload: []byte{sequence},
		Footprint: Footprint{Points: [][]byte{[]byte("successive-checkpoint")}},
	}
	if _, err := node.Propose(command); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if !node.HasReady() {
			if err := node.Tick(); err != nil {
				t.Fatal(err)
			}
		}
		if !node.HasReady() {
			continue
		}
		ready := node.Ready()
		if ready.Checkpoint != nil {
			var digest StateDigest
			digest[0] = sequence
			if err := node.ProvideCheckpoint(CheckpointResult{
				ID: ready.Checkpoint.ID, ApplicationSnapshot: []byte{sequence},
				ApplicationDigest: digest,
			}); err != nil {
				t.Fatal(err)
			}
		}
		advanceCheckpointReady(t, node, storage, ready)
		checkpoint := storage.Checkpoint
		if checkpoint.Descriptor.ID != previousID && checkpointCertified(checkpoint) &&
			executionFrontierEqual(checkpoint.CompactedThrough, checkpoint.Descriptor.Through) {
			return checkpoint.Clone()
		}
	}
	t.Fatal("checkpoint did not certify and compact")
	return Checkpoint{}
}

func TestSuccessiveCheckpointPreservesCompactionFloorAndIgnoresOlderOffer(t *testing.T) {
	storage := NewMemoryStorage()
	node, err := NewRawNode(Config{
		ID: 1, Voters: []ReplicaID{1}, Storage: storage,
		RetainExecutedPerLane: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if node.HasReady() {
		advanceCheckpointReady(t, node, storage, node.Ready())
	}
	first := completeSingleReplicaCheckpoint(t, node, storage, 1)
	second := completeSingleReplicaCheckpoint(t, node, storage, 2)
	if first.Descriptor.ID == second.Descriptor.ID {
		t.Fatal("successive checkpoints reused an ID")
	}
	if frontierRegressesExecution(first.CompactedThrough, second.CompactedThrough) {
		t.Fatalf("second compaction frontier regressed: first=%#v second=%#v", first.CompactedThrough, second.CompactedThrough)
	}

	payload, err := encodeCheckpointControl(checkpointControlWire{
		Type: checkpointControlOffer, Checkpoint: first,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.handleCheckpointControl(Message{
		Type: MsgCheckpointOffer, From: 1, To: 1, Kind: EntryCheckpoint,
		ProtocolControl: payload,
	}); err != nil {
		t.Fatalf("stale checkpoint offer: %v", err)
	}
	if node.checkpointRound != nil {
		t.Fatalf("stale checkpoint offer started round %#v", node.checkpointRound)
	}
	if storage.Checkpoint.Descriptor.ID != second.Descriptor.ID {
		t.Fatal("stale checkpoint offer replaced durable checkpoint")
	}
	if node.HasReady() {
		ready := node.Ready()
		if ready.Snapshot != nil {
			t.Fatalf("stale checkpoint offer emitted snapshot %#v", ready.Snapshot)
		}
	}
}

func TestCheckpointPeerHandoffPreservesReceiverCompactionFloor(t *testing.T) {
	conf := ConfState{ID: 1, Voters: []ReplicaID{1, 2}}
	receiverCheckpoint := testCertifiedCheckpointWithFloor(t, conf, 3, 2, 3, 2, 1)
	senderCheckpoint := testCertifiedCheckpointWithFloor(t, conf, 5, 4, 1, 1, 2)

	sender, err := NewRawNode(Config{ID: 1, Voters: conf.Voters})
	if err != nil {
		t.Fatal(err)
	}
	sender.durableCheckpoint = senderCheckpoint.Clone()

	for _, test := range []struct {
		name          string
		messageType   MessageType
		controlType   checkpointControlType
		snapshotMode  SnapshotMode
		matchingRound bool
	}{
		{
			name:        "matching certificate",
			messageType: MsgCheckpointCertificate, controlType: checkpointControlCertificate,
			snapshotMode: SnapshotPersistLocal, matchingRound: true,
		},
		{
			name:        "snapshot offer",
			messageType: MsgCheckpointOffer, controlType: checkpointControlOffer,
			snapshotMode: SnapshotInstall,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			storage := NewMemoryStorage()
			storage.Hard = HardState{Conf: conf.Clone()}
			storage.Checkpoint = receiverCheckpoint.Clone()
			receiver, err := NewRawNode(Config{ID: 2, Voters: conf.Voters, Storage: storage})
			if err != nil {
				t.Fatal(err)
			}
			for step := 0; receiver.HasReady(); step++ {
				if step >= 8 {
					t.Fatal("receiver startup did not quiesce")
				}
				advanceCheckpointReady(t, receiver, storage, receiver.Ready())
			}

			localSnapshot := []byte("receiver-local-snapshot")
			if test.matchingRound {
				prepared := sender.durableCheckpoint.Clone()
				prepared.Certificate = CheckpointCertificate{}
				prepared.ApplicationSnapshot = bytes.Clone(localSnapshot)
				prepared.CompactedThrough = receiver.durableCheckpoint.CompactedThrough.Clone()
				prepared.Checksum = DigestCheckpoint(prepared)
				receiver.checkpointRound = &checkpointRound{
					barrier: prepared.Descriptor.Barrier,
					request: CheckpointRequest{
						ID: prepared.Descriptor.ID, Through: prepared.Descriptor.Through.Clone(),
					},
					checkpoint: prepared,
					votes:      make(map[ReplicaID]VoterAcknowledgement),
				}
			}

			payload, err := encodeCheckpointControl(checkpointControlWire{
				Type: test.controlType, Checkpoint: sender.durableCheckpoint,
			})
			if err != nil {
				t.Fatal(err)
			}
			message := Message{
				Type: test.messageType, From: 1, To: 2,
				FromIncarnation: 1, ToIncarnation: 1,
				Ref:    sender.durableCheckpoint.Descriptor.Barrier,
				Ballot: Ballot{Replica: sender.durableCheckpoint.Descriptor.Barrier.Replica},
				Kind:   EntryCheckpoint, ProtocolControl: payload,
			}
			message.Checksum = ChecksumMessage(message)
			if err := receiver.Step(message); err != nil {
				t.Fatal(err)
			}
			if !receiver.HasReady() {
				t.Fatal("checkpoint handoff produced no Ready")
			}
			ready := receiver.Ready()
			if ready.Snapshot == nil || ready.Snapshot.Mode != test.snapshotMode {
				t.Fatalf("snapshot=%#v, want mode %d", ready.Snapshot, test.snapshotMode)
			}
			localized := ready.Snapshot.Checkpoint
			if !checkpointDescriptorEqual(localized.Descriptor, sender.durableCheckpoint.Descriptor) ||
				!checkpointCertificateEqual(localized.Certificate, sender.durableCheckpoint.Certificate) {
				t.Fatal("localization changed certificate-attested checkpoint state")
			}
			if !executionFrontierEqual(localized.CompactedThrough, receiverCheckpoint.CompactedThrough) {
				t.Fatalf("localized floor=%#v, want receiver floor %#v", localized.CompactedThrough, receiverCheckpoint.CompactedThrough)
			}
			if localized.Checksum != DigestCheckpoint(localized) {
				t.Fatal("localized checkpoint checksum was not recomputed")
			}
			if test.matchingRound && !bytes.Equal(localized.ApplicationSnapshot, localSnapshot) {
				t.Fatalf("certificate snapshot handle=%q, want receiver-local handle", localized.ApplicationSnapshot)
			}
			if err := storage.ApplyReady(ready); err != nil {
				t.Fatalf("persist localized checkpoint: %v", err)
			}
			if !executionFrontierEqual(storage.Checkpoint.CompactedThrough, receiverCheckpoint.CompactedThrough) {
				t.Fatalf("durable floor=%#v, want preserved receiver floor %#v", storage.Checkpoint.CompactedThrough, receiverCheckpoint.CompactedThrough)
			}
		})
	}
}

func testCertifiedCheckpointWithFloor(
	t *testing.T,
	conf ConfState,
	replicaOneThrough, replicaTwoThrough InstanceNum,
	replicaOneFloor, replicaTwoFloor InstanceNum,
	marker byte,
) Checkpoint {
	t.Helper()
	through := ExecutionFrontier{Configs: []ExecutionConfigFrontier{{
		Conf: conf.ID,
		Lanes: []ExecutionLaneFrontier{
			{Replica: conf.Voters[0], Through: replicaOneThrough},
			{Replica: conf.Voters[1], Through: replicaTwoThrough},
		},
	}}}
	compactedThrough := ExecutionFrontier{Configs: []ExecutionConfigFrontier{{
		Conf: conf.ID,
		Lanes: []ExecutionLaneFrontier{
			{Replica: conf.Voters[0], Through: replicaOneFloor},
			{Replica: conf.Voters[1], Through: replicaTwoFloor},
		},
	}}}
	barrierDigest := StateDigest{marker}
	fences := []AllocatorFence{
		{Conf: conf.ID, Replica: conf.Voters[0], Next: replicaOneThrough + 1},
		{Conf: conf.ID, Replica: conf.Voters[1], Next: replicaTwoThrough + 1},
	}
	identities := []VoterIdentity{
		{Replica: conf.Voters[0], Incarnation: 1},
		{Replica: conf.Voters[1], Incarnation: 1},
	}
	descriptor := CheckpointDescriptor{
		ID:                 deriveCheckpointID(barrierDigest, through),
		Conf:               conf.Clone(),
		Voters:             identities,
		Barrier:            InstanceRef{Conf: conf.ID, Replica: conf.Voters[0], Instance: replicaOneThrough},
		BarrierTupleDigest: barrierDigest,
		Through:            through,
		AllocatorFences:    fences,
		ApplicationDigest:  StateDigest{marker},
	}
	descriptor.ProtocolStateDigest = checkpointProtocolStateDigest(
		map[ConfID]ConfState{conf.ID: conf},
		descriptor.Through,
		descriptor.BarrierTupleDigest,
		descriptor.AllocatorFences,
	)
	digest := DigestCheckpointDescriptor(descriptor)
	acknowledgements := make([]VoterAcknowledgement, len(identities))
	for index, identity := range identities {
		acknowledgements[index] = VoterAcknowledgement{Voter: identity, AttestedDigest: digest}
	}
	certificate, err := BuildCheckpointCertificate(descriptor, acknowledgements)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := Checkpoint{
		Descriptor:          descriptor,
		Certificate:         certificate,
		ApplicationSnapshot: []byte{marker},
		CompactedThrough:    compactedThrough,
	}
	checkpoint.Checksum = DigestCheckpoint(checkpoint)
	return checkpoint
}
