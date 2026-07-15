package epaxos

import (
	"bytes"
	"fmt"
	"sort"
)

var checkpointBarrierPayload = []byte("epaxos/checkpoint-barrier/v1")

func (n *RawNode) maybeScheduleCheckpointBarrier() error {
	if n == nil || n.retainExecutedPerLane <= 0 || n.executionPaused || n.checkpointRound != nil || n.pendingConf {
		return nil
	}
	for ref, inst := range n.instances {
		if ref.Replica == n.id && inst != nil && inst.rec.Kind == EntryCheckpoint &&
			!n.executed.contains(ref) {
			return nil
		}
	}
	base := n.durableCheckpoint.Descriptor.Through
	due := false
	for confID, conf := range n.confHistory {
		for _, voter := range conf.Voters {
			lane := instanceLane{conf: confID, replica: voter}
			through := n.durableExecuted.prefix(lane)
			baseThrough := executionFrontierLane(base, lane)
			if through < baseThrough || through-baseThrough < InstanceNum(n.retainExecutedPerLane) { //nolint:gosec // configured bound is validated.
				continue
			}
			due = true
			break
		}
		if due {
			break
		}
	}
	if !due {
		return nil
	}
	if err := n.requireLocalVoter(); err != nil {
		return err
	}
	_, err := n.proposeEntry(EntryCheckpoint, Command{}, ConfChange{}, checkpointBarrierPayload)
	return err
}

func executionFrontierLane(frontier ExecutionFrontier, lane instanceLane) InstanceNum {
	for _, config := range frontier.Configs {
		if config.Conf != lane.conf {
			continue
		}
		for _, candidate := range config.Lanes {
			if candidate.Replica == lane.replica {
				return candidate.Through
			}
		}
	}
	return 0
}

func (n *RawNode) currentExecutionFrontier() ExecutionFrontier {
	ids := make([]ConfID, 0, len(n.confHistory))
	for id := range n.confHistory {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	frontier := ExecutionFrontier{Configs: make([]ExecutionConfigFrontier, 0, len(ids))}
	for _, id := range ids {
		conf := n.confHistory[id]
		config := ExecutionConfigFrontier{Conf: id, Lanes: make([]ExecutionLaneFrontier, len(conf.Voters))}
		for i, voter := range conf.Voters {
			config.Lanes[i] = ExecutionLaneFrontier{
				Replica: voter,
				Through: n.executed.prefix(instanceLane{conf: id, replica: voter}),
			}
		}
		frontier.Configs = append(frontier.Configs, config)
	}
	return frontier
}

func (n *RawNode) checkpointComponentClosed(component []InstanceRef) bool {
	members := make(map[InstanceRef]struct{}, len(component))
	for _, ref := range component {
		members[ref] = struct{}{}
	}
	checkPrefix := func(base InstanceRef, lane instanceLane, through InstanceNum, allowKnownAfter bool) bool {
		for instance := InstanceNum(1); instance <= through; instance++ {
			ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
			_, componentMember := members[ref]
			knownAfter := false
			if allowKnownAfter {
				baseInst := n.instances[base]
				dependencyInst := n.instances[ref]
				knownAfter = baseInst != nil && baseInst.rec.Kind != EntryCheckpoint &&
					dependencyInst != nil && dependencyInst.rec.Kind != EntryCheckpoint &&
					n.dependencyKnownAfter(base, ref, StatusCommitted)
			}
			if componentMember || n.executed.contains(ref) || knownAfter {
				if instance == ^InstanceNum(0) {
					break
				}
				continue
			}
			n.maybeStartDependencyRecovery(ref)
			return false
		}
		return true
	}
	closed := true
	for _, ref := range component {
		if !checkPrefix(ref, laneFor(ref), ref.Instance, false) {
			closed = false
		}
		inst := n.instances[ref]
		if inst == nil {
			closed = false
			continue
		}
		conf := n.confFor(ref.Conf)
		for slot, dependency := range inst.rec.Deps {
			if dependency == 0 || slot >= len(conf.Voters) {
				continue
			}
			lane := instanceLane{conf: ref.Conf, replica: conf.Voters[slot]}
			if !checkPrefix(ref, lane, dependency, true) {
				closed = false
			}
		}
	}
	return closed
}

func (n *RawNode) beginCheckpoint(barrier InstanceRecord) {
	through := n.currentExecutionFrontier()
	tuple := checkpointBarrierTupleDigest(barrier)
	request := CheckpointRequest{ID: deriveCheckpointID(tuple, through), Through: through}
	if n.checkpointRound != nil && n.checkpointRound.request.ID == request.ID {
		return
	}
	n.checkpointRound = &checkpointRound{
		barrier: barrier.Ref,
		request: request,
		votes:   make(map[ReplicaID]VoterAcknowledgement),
	}
	n.executionPaused = true
	target := n.readyTarget()
	cloned := request.Clone()
	target.Checkpoint = &cloned
}

// ProvideCheckpoint supplies opaque application snapshot metadata for the one
// outstanding Ready.Checkpoint request. Exact duplicate results are idempotent.
func (n *RawNode) ProvideCheckpoint(result CheckpointResult) error {
	if n == nil || n.checkpointRound == nil {
		return ErrUnrequestedCheckpoint
	}
	round := n.checkpointRound
	if result.ID != round.request.ID {
		return ErrCheckpointMismatch
	}
	if result.ApplicationDigest == (StateDigest{}) || len(result.ApplicationSnapshot) == 0 ||
		len(result.ApplicationSnapshot) > maxApplicationSnapshotHandle {
		return ErrInvalidCheckpoint
	}
	if round.result.ID != (CheckpointID{}) {
		if round.result.ID == result.ID && round.result.ApplicationDigest == result.ApplicationDigest &&
			bytes.Equal(round.result.ApplicationSnapshot, result.ApplicationSnapshot) {
			return nil
		}
		return ErrCheckpointMismatch
	}
	barrier := n.instances[round.barrier]
	if barrier == nil || barrier.rec.Status != StatusExecuted || barrier.rec.Kind != EntryCheckpoint {
		return ErrInvalidCheckpoint
	}
	conf, ok := n.confHistory[barrier.rec.Ref.Conf]
	if !ok || n.pendingConf {
		return ErrInvalidCheckpoint
	}
	identities := make([]VoterIdentity, len(conf.Voters))
	for i, voter := range conf.Voters {
		identity, exists := n.voterIdentities[voter]
		if !exists {
			identity = VoterIdentity{Replica: voter, Incarnation: 1}
		}
		identities[i] = identity.Clone()
	}
	fences := make([]AllocatorFence, 0)
	for _, config := range round.request.Through.Configs {
		for _, lane := range config.Lanes {
			next, ok := instanceSuccessor(lane.Through)
			if !ok {
				return ErrInstanceExhausted
			}
			if config.Conf == n.q.conf.ID && lane.Replica == n.id && n.nextInstance > next {
				next = n.nextInstance
			}
			fences = append(fences, AllocatorFence{Conf: config.Conf, Replica: lane.Replica, Next: next})
		}
	}
	tuple := checkpointBarrierTupleDigest(barrier.rec)
	descriptor := CheckpointDescriptor{
		ID: round.request.ID, Cluster: n.cluster, Conf: conf.Clone(), Voters: identities,
		Barrier: barrier.rec.Ref, BarrierTupleDigest: tuple, Through: round.request.Through.Clone(),
		AllocatorFences: fences, ApplicationDigest: result.ApplicationDigest,
	}
	descriptor.ProtocolStateDigest = checkpointProtocolStateDigest(n.confHistory, descriptor.Through, tuple, fences)
	checkpoint := Checkpoint{
		Descriptor: descriptor, ApplicationSnapshot: bytes.Clone(result.ApplicationSnapshot),
		CompactedThrough: n.durableCheckpoint.CompactedThrough.Clone(),
	}
	checkpoint.Checksum = DigestCheckpoint(checkpoint)
	round.result = CheckpointResult{ID: result.ID, ApplicationSnapshot: bytes.Clone(result.ApplicationSnapshot), ApplicationDigest: result.ApplicationDigest}
	round.checkpoint = checkpoint.Clone()
	target := n.readyTarget()
	snapshot := Snapshot{Mode: SnapshotPersistLocal, Checkpoint: checkpoint}
	target.Snapshot = &snapshot
	target.MustSync = true
	return nil
}

func (n *RawNode) restorePreparedCheckpoint(checkpoint Checkpoint) error {
	if checkpoint.Empty() || checkpointCertified(checkpoint) {
		return nil
	}
	barrier := n.instances[checkpoint.Descriptor.Barrier]
	if barrier == nil || barrier.rec.Status != StatusExecuted || barrier.rec.Kind != EntryCheckpoint ||
		checkpointBarrierTupleDigest(barrier.rec) != checkpoint.Descriptor.BarrierTupleDigest {
		return ErrInvalidCheckpoint
	}
	request := CheckpointRequest{ID: checkpoint.Descriptor.ID, Through: checkpoint.Descriptor.Through.Clone()}
	round := &checkpointRound{
		barrier: checkpoint.Descriptor.Barrier, request: request,
		result: CheckpointResult{
			ID: request.ID, ApplicationSnapshot: bytes.Clone(checkpoint.ApplicationSnapshot),
			ApplicationDigest: checkpoint.Descriptor.ApplicationDigest,
		},
		checkpoint: checkpoint.Clone(), votes: make(map[ReplicaID]VoterAcknowledgement),
		preparedDurable: true,
	}
	digest := DigestCheckpointDescriptor(checkpoint.Descriptor)
	identity := n.voterIdentity(n.id)
	found := false
	for _, voter := range checkpoint.Descriptor.Voters {
		if voterIdentityEqual(voter, identity) {
			found = true
			break
		}
	}
	if !found {
		return ErrInvalidCheckpoint
	}
	vote := VoterAcknowledgement{Voter: identity, AttestedDigest: digest}
	round.votes[n.id] = vote
	n.checkpointRound = round
	n.executionPaused = true
	n.broadcastCheckpointControl(MsgCheckpointVote, checkpointControlWire{
		Type: checkpointControlVote, Descriptor: checkpoint.Descriptor, Vote: vote,
	})
	n.maybeCertifyCheckpoint()
	return nil
}

func (n *RawNode) checkpointSnapshotAdvanced(snapshot Snapshot) {
	round := n.checkpointRound
	if round == nil || snapshot.Checkpoint.Descriptor.ID != round.request.ID {
		return
	}
	round.checkpoint = snapshot.Checkpoint.Clone()
	if !checkpointCertified(round.checkpoint) {
		if round.preparedDurable {
			return
		}
		round.preparedDurable = true
		digest := DigestCheckpointDescriptor(round.checkpoint.Descriptor)
		identity := n.localIdentity
		if !identity.valid() {
			identity = VoterIdentity{Replica: n.id, Incarnation: n.voterIncarnation(n.id)}
		}
		vote := VoterAcknowledgement{Voter: identity.Clone(), AttestedDigest: digest}
		round.votes[n.id] = vote
		n.broadcastCheckpointControl(MsgCheckpointVote, checkpointControlWire{Type: checkpointControlVote, Descriptor: round.checkpoint.Descriptor, Vote: vote})
		n.maybeCertifyCheckpoint()
		return
	}
	if round.certificateDurable {
		return
	}
	round.certificateDurable = true
	n.durableCheckpoint = round.checkpoint.Clone()
	n.broadcastCheckpointControl(MsgCheckpointCertificate, checkpointControlWire{Type: checkpointControlCertificate, Checkpoint: round.checkpoint})
	n.broadcastCheckpointControl(MsgCheckpointOffer, checkpointControlWire{Type: checkpointControlOffer, Checkpoint: round.checkpoint})
	target := n.readyTarget()
	target.Compact = canonicalCompactionRanges(round.checkpoint)
	target.MustSync = true
	n.checkpointCompacting = true
}

func (n *RawNode) broadcastCheckpointControl(typ MessageType, control checkpointControlWire) {
	payload, err := encodeCheckpointControl(control)
	if err != nil {
		return
	}
	barrier := n.instances[n.checkpointRound.barrier]
	ballot := Ballot{Replica: n.checkpointRound.barrier.Replica}
	if barrier != nil {
		ballot = barrier.rec.RecordBallot
	}
	for _, voter := range n.votersForConf(n.checkpointRound.barrier.Conf) {
		if voter == n.id {
			continue
		}
		n.enqueueMessage(Message{Type: typ, From: n.id, To: voter, Ref: n.checkpointRound.barrier,
			Ballot: ballot, Kind: EntryCheckpoint, ProtocolControl: payload})
	}
}

func (n *RawNode) retryCheckpointControl() {
	round := n.checkpointRound
	if round == nil || !round.preparedDurable || checkpointCertified(round.checkpoint) {
		return
	}
	vote, ok := round.votes[n.id]
	if !ok {
		return
	}
	n.broadcastCheckpointControl(MsgCheckpointVote, checkpointControlWire{
		Type: checkpointControlVote, Descriptor: round.checkpoint.Descriptor, Vote: vote,
	})
}

func (n *RawNode) maybeCertifyCheckpoint() {
	round := n.checkpointRound
	if round == nil || !round.preparedDurable || checkpointCertified(round.checkpoint) {
		return
	}
	acks := make([]VoterAcknowledgement, 0, len(round.votes))
	for _, voter := range round.checkpoint.Descriptor.Conf.Voters {
		if acknowledgement, ok := round.votes[voter]; ok {
			acks = append(acks, acknowledgement)
		}
	}
	if len(acks) < slowQuorumSize(len(round.checkpoint.Descriptor.Conf.Voters)) {
		return
	}
	acks = acks[:slowQuorumSize(len(round.checkpoint.Descriptor.Conf.Voters))]
	certificate, err := BuildCheckpointCertificate(round.checkpoint.Descriptor, acks)
	if err != nil {
		return
	}
	round.checkpoint.Certificate = certificate
	round.checkpoint.Checksum = DigestCheckpoint(round.checkpoint)
	target := n.readyTarget()
	snapshot := Snapshot{Mode: SnapshotPersistLocal, Checkpoint: round.checkpoint.Clone()}
	target.Snapshot = &snapshot
	target.MustSync = true
}

func (n *RawNode) localizeIncomingCheckpoint(checkpoint Checkpoint, applicationSnapshot []byte) (Checkpoint, error) {
	checkpoint.ApplicationSnapshot = bytes.Clone(applicationSnapshot)
	checkpoint.CompactedThrough = n.durableCheckpoint.CompactedThrough.Clone()
	checkpoint.Checksum = DigestCheckpoint(checkpoint)
	if err := validateCheckpoint(checkpoint, n.confHistory); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

func (n *RawNode) handleCheckpointControl(message Message) error {
	wire, err := decodeCheckpointControl(message.ProtocolControl)
	if err != nil {
		return err
	}
	switch message.Type {
	case MsgCheckpointVote:
		if wire.Vote.Voter.Replica != message.From ||
			wire.Vote.AttestedDigest != DigestCheckpointDescriptor(wire.Descriptor) {
			return ErrCheckpointMismatch
		}
		expected := n.voterIdentity(message.From)
		if !voterIdentityEqual(expected, wire.Vote.Voter) {
			return ErrMessageRejected
		}
		if n.durableCheckpointCovers(wire.Descriptor) {
			n.enqueueLatestCheckpointCertificate(message.From)
			n.enqueueLatestCheckpointOffer(message.From)
			return nil
		}
		round := n.checkpointRound
		if round == nil || !checkpointDescriptorEqual(round.checkpoint.Descriptor, wire.Descriptor) {
			return nil
		}
		if existing, ok := round.votes[message.From]; ok && existing != wire.Vote {
			return ErrCheckpointMismatch
		}
		round.votes[message.From] = wire.Vote
		n.maybeCertifyCheckpoint()
		return nil
	case MsgCheckpointCertificate, MsgCheckpointOffer:
		checkpoint := wire.Checkpoint.Clone()
		if err := validateCheckpoint(checkpoint, n.confHistory); err != nil {
			return err
		}
		if checkpoint.Descriptor.Cluster != n.cluster {
			return ErrCheckpointMismatch
		}
		if checkpointCertified(n.durableCheckpoint) &&
			n.durableCheckpoint.Descriptor.ID == checkpoint.Descriptor.ID {
			return nil
		}
		if len(n.durableCheckpoint.CompactedThrough.Configs) != 0 &&
			!frontierRegressesExecution(checkpoint.Descriptor.Through, n.durableCheckpoint.CompactedThrough) {
			return nil
		}
		if checkpointCertified(n.durableCheckpoint) &&
			!frontierRegressesExecution(checkpoint.Descriptor.Through, n.durableCheckpoint.Descriptor.Through) {
			return nil
		}
		if n.checkpointRound != nil && n.checkpointRound.request.ID == checkpoint.Descriptor.ID &&
			checkpointDescriptorEqual(n.checkpointRound.checkpoint.Descriptor, checkpoint.Descriptor) {
			checkpoint, err = n.localizeIncomingCheckpoint(
				checkpoint,
				n.checkpointRound.checkpoint.ApplicationSnapshot,
			)
			if err != nil {
				return err
			}
			n.checkpointRound.checkpoint = checkpoint.Clone()
			target := n.readyTarget()
			snapshot := Snapshot{Mode: SnapshotPersistLocal, Checkpoint: checkpoint}
			target.Snapshot = &snapshot
			target.MustSync = true
			return nil
		}
		if message.Type != MsgCheckpointOffer {
			return nil
		}
		checkpoint, err = n.localizeIncomingCheckpoint(checkpoint, checkpoint.ApplicationSnapshot)
		if err != nil {
			return err
		}
		n.executionPaused = true
		n.checkpointRound = &checkpointRound{
			barrier:    checkpoint.Descriptor.Barrier,
			request:    CheckpointRequest{ID: checkpoint.Descriptor.ID, Through: checkpoint.Descriptor.Through.Clone()},
			checkpoint: checkpoint.Clone(), votes: make(map[ReplicaID]VoterAcknowledgement),
		}
		target := n.readyTarget()
		snapshot := Snapshot{Mode: SnapshotInstall, Checkpoint: checkpoint}
		target.Snapshot = &snapshot
		target.MustSync = true
		return nil
	case MsgPreAccept, MsgPreAcceptResp, MsgAccept, MsgAcceptResp, MsgCommit, MsgPrepare, MsgPrepareResp,
		MsgTryPreAccept, MsgTryPreAcceptResp, MsgEvidence, MsgEvidenceResp:
		return fmt.Errorf("%w: ordinary message passed as checkpoint control", ErrInvalidCheckpoint)
	default:
		return fmt.Errorf("%w: unknown checkpoint control", ErrInvalidCheckpoint)
	}
}

func (n *RawNode) voterIdentity(replica ReplicaID) VoterIdentity {
	if identity, ok := n.voterIdentities[replica]; ok {
		return identity.Clone()
	}
	return VoterIdentity{Replica: replica, Incarnation: 1}
}

func (n *RawNode) compactCheckpointMetadata() {
	checkpoint := n.durableCheckpoint
	if !checkpointCertified(checkpoint) {
		return
	}
	barrierRef := checkpoint.Descriptor.Barrier
	var barrierRecord InstanceRecord
	if barrier := n.instances[barrierRef]; barrier != nil {
		barrierRecord = barrier.rec.Clone()
	}
	if barrierRecord.Ref.IsZero() {
		barrierRecord = InstanceRecord{
			Ref:             checkpoint.Descriptor.Barrier,
			Ballot:          Ballot{Replica: checkpoint.Descriptor.Barrier.Replica},
			RecordBallot:    Ballot{Replica: checkpoint.Descriptor.Barrier.Replica},
			Status:          StatusExecuted,
			Deps:            make([]InstanceNum, len(checkpoint.Descriptor.Conf.Voters)),
			Kind:            EntryCheckpoint,
			ProtocolControl: append([]byte(nil), checkpointBarrierPayload...),
		}
		barrierRecord.Checksum = ChecksumRecord(barrierRecord)
	}
	retained := make([]*instance, 0, len(n.instances))
	for ref, inst := range n.instances {
		if inst == nil {
			continue
		}
		if frontierCovers(checkpoint.CompactedThrough, ref) {
			if ref == barrierRef {
				continue
			}
			if inst.payloadAbsent && n.payloadStubInstances > 0 {
				n.payloadStubInstances--
			}
			delete(n.instances, ref)
			continue
		}
		retained = append(retained, inst)
	}
	n.engine = conflictEngine{}
	n.executed = newExecutedTracker()
	n.durableExecuted = newExecutedTracker()
	n.committedThrough = make(map[instanceLane]InstanceNum)
	for _, config := range checkpoint.CompactedThrough.Configs {
		for _, lane := range config.Lanes {
			key := instanceLane{conf: config.Conf, replica: lane.Replica}
			n.executed.through[key] = lane.Through
			n.durableExecuted.through[key] = lane.Through
			n.committedThrough[key] = lane.Through
		}
	}
	if !barrierRecord.Ref.IsZero() {
		barrierRecord.Status = StatusExecuted
		barrierRecord.Checksum = ChecksumRecord(barrierRecord)
		inst := n.instances[barrierRef]
		if inst == nil {
			inst = &instance{rec: barrierRecord, phase: phaseCommitted}
			n.instances[barrierRef] = inst
		}
		n.engine.apply(nil, barrierRecord)
	}
	for _, inst := range retained {
		n.engine.apply(nil, inst.rec)
		if inst.rec.Status >= StatusCommitted {
			n.noteCommitted(inst.rec.Ref)
		}
		if inst.rec.Status == StatusExecuted {
			n.executed.add(inst.rec.Ref)
			n.durableExecuted.add(inst.rec.Ref)
		}
	}
}

func (n *RawNode) enqueueLatestCheckpointCertificate(to ReplicaID) {
	n.enqueueLatestCheckpointControl(to, MsgCheckpointCertificate, checkpointControlCertificate)
}

func (n *RawNode) enqueueLatestCheckpointOffer(to ReplicaID) {
	n.enqueueLatestCheckpointControl(to, MsgCheckpointOffer, checkpointControlOffer)
}

func (n *RawNode) enqueueLatestCheckpointControl(to ReplicaID, messageType MessageType, controlType checkpointControlType) {
	checkpoint := n.durableCheckpoint
	if !checkpointCertified(checkpoint) || len(checkpoint.ApplicationSnapshot) == 0 {
		return
	}
	payload, err := encodeCheckpointControl(checkpointControlWire{Type: controlType, Checkpoint: checkpoint})
	if err != nil {
		return
	}
	barrier := checkpoint.Descriptor.Barrier
	n.enqueueMessage(Message{Type: messageType, From: n.id, To: to, Ref: barrier,
		Ballot: Ballot{Replica: barrier.Replica}, Kind: EntryCheckpoint, ProtocolControl: payload})
}

func (n *RawNode) durableCheckpointCovers(descriptor CheckpointDescriptor) bool {
	checkpoint := n.durableCheckpoint
	return checkpointCertified(checkpoint) &&
		descriptor.Cluster == n.cluster &&
		descriptor.ID == deriveCheckpointID(descriptor.BarrierTupleDigest, descriptor.Through) &&
		!frontierRegressesExecution(descriptor.Through, checkpoint.Descriptor.Through)
}
