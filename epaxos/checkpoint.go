package epaxos

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
)

const (
	maxApplicationSnapshotHandle = 64 << 10
	checkpointWireVersion        = 1
)

// CheckpointID is the content-derived identity of a checkpoint barrier and frontier.
type CheckpointID [32]byte

// ExecutionFrontier records the exact executed prefix for each configuration lane.
type ExecutionFrontier struct {
	Configs []ExecutionConfigFrontier
}

// ExecutionConfigFrontier groups executed lane frontiers by configuration.
type ExecutionConfigFrontier struct {
	Conf  ConfID
	Lanes []ExecutionLaneFrontier
}

// ExecutionLaneFrontier records the executed prefix for one replica lane.
type ExecutionLaneFrontier struct {
	Replica ReplicaID
	Through InstanceNum
}

// Clone returns a deep copy of the execution frontier.
func (f ExecutionFrontier) Clone() ExecutionFrontier {
	out := ExecutionFrontier{Configs: make([]ExecutionConfigFrontier, len(f.Configs))}
	for i := range f.Configs {
		out.Configs[i].Conf = f.Configs[i].Conf
		out.Configs[i].Lanes = append([]ExecutionLaneFrontier(nil), f.Configs[i].Lanes...)
	}
	return out
}

func executionFrontierEqual(a, b ExecutionFrontier) bool {
	if len(a.Configs) != len(b.Configs) {
		return false
	}
	for i := range a.Configs {
		if a.Configs[i].Conf != b.Configs[i].Conf || len(a.Configs[i].Lanes) != len(b.Configs[i].Lanes) {
			return false
		}
		for j := range a.Configs[i].Lanes {
			if a.Configs[i].Lanes[j] != b.Configs[i].Lanes[j] {
				return false
			}
		}
	}
	return true
}

func validateExecutionFrontier(f ExecutionFrontier, history map[ConfID]ConfState) error {
	for i, config := range f.Configs {
		if config.Conf == 0 || i > 0 && f.Configs[i-1].Conf >= config.Conf {
			return ErrInvalidCheckpoint
		}
		conf, ok := history[config.Conf]
		if !ok || len(conf.Voters) != len(config.Lanes) {
			return ErrInvalidCheckpoint
		}
		for lane := range config.Lanes {
			if config.Lanes[lane].Replica != conf.Voters[lane] {
				return ErrInvalidCheckpoint
			}
		}
	}
	return nil
}

func frontierCovers(frontier ExecutionFrontier, ref InstanceRef) bool {
	for _, config := range frontier.Configs {
		if config.Conf != ref.Conf {
			continue
		}
		for _, lane := range config.Lanes {
			if lane.Replica == ref.Replica {
				return ref.Instance <= lane.Through
			}
		}
	}
	return false
}

func frontierRegressesExecution(current, next ExecutionFrontier) bool {
	for _, config := range current.Configs {
		for _, lane := range config.Lanes {
			found := false
			for _, nextConfig := range next.Configs {
				if nextConfig.Conf != config.Conf {
					continue
				}
				for _, nextLane := range nextConfig.Lanes {
					if nextLane.Replica == lane.Replica {
						found = nextLane.Through >= lane.Through
					}
				}
			}
			if !found && lane.Through != 0 {
				return true
			}
		}
	}
	return false
}

// AllocatorFence records the first instance number that remains allocatable after a checkpoint.
type AllocatorFence struct {
	Conf    ConfID
	Replica ReplicaID
	Next    InstanceNum
}

// CheckpointDescriptor binds a barrier, execution frontier, allocator fences, and state digests.
type CheckpointDescriptor struct {
	ID                  CheckpointID
	Cluster             ClusterID
	Conf                ConfState
	Voters              []VoterIdentity
	Barrier             InstanceRef
	BarrierTupleDigest  StateDigest
	Through             ExecutionFrontier
	AllocatorFences     []AllocatorFence
	ApplicationDigest   StateDigest
	ProtocolStateDigest StateDigest
}

// Clone returns a deep copy of the checkpoint descriptor.
func (d CheckpointDescriptor) Clone() CheckpointDescriptor {
	d.Conf = d.Conf.Clone()
	d.Voters = append([]VoterIdentity(nil), d.Voters...)
	d.Through = d.Through.Clone()
	d.AllocatorFences = append([]AllocatorFence(nil), d.AllocatorFences...)
	return d
}

// CheckpointCertificate is a slow-quorum attestation of one checkpoint descriptor.
type CheckpointCertificate struct {
	DescriptorDigest StateDigest
	Acknowledgements []VoterAcknowledgement
	Checksum         StateDigest
}

// Clone returns a deep copy of the checkpoint certificate.
func (c CheckpointCertificate) Clone() CheckpointCertificate {
	c.Acknowledgements = cloneVoterAcknowledgements(c.Acknowledgements)
	return c
}

// Empty reports whether the certificate contains no attestation.
func (c CheckpointCertificate) Empty() bool {
	return c.DescriptorDigest == (StateDigest{}) && len(c.Acknowledgements) == 0 && c.Checksum == (StateDigest{})
}

// Checkpoint is a certified protocol and application snapshot boundary.
type Checkpoint struct {
	Descriptor          CheckpointDescriptor
	Certificate         CheckpointCertificate
	ApplicationSnapshot []byte
	CompactedThrough    ExecutionFrontier
	Checksum            StateDigest
}

// Clone returns a deep copy of the checkpoint.
func (c Checkpoint) Clone() Checkpoint {
	c.Descriptor = c.Descriptor.Clone()
	c.Certificate = c.Certificate.Clone()
	c.ApplicationSnapshot = bytes.Clone(c.ApplicationSnapshot)
	c.CompactedThrough = c.CompactedThrough.Clone()
	return c
}

// Empty reports whether the checkpoint has no identity.
func (c Checkpoint) Empty() bool { return c.Descriptor.ID == (CheckpointID{}) }

func checkpointCertified(checkpoint Checkpoint) bool {
	return !checkpoint.Empty() && !checkpoint.Certificate.Empty()
}

func snapshotEqual(a, b Snapshot) bool {
	return a.Mode == b.Mode && checkpointEqual(a.Checkpoint, b.Checkpoint)
}

func checkpointEqual(a, b Checkpoint) bool {
	return a.Checksum == b.Checksum &&
		bytes.Equal(a.ApplicationSnapshot, b.ApplicationSnapshot) &&
		executionFrontierEqual(a.CompactedThrough, b.CompactedThrough) &&
		checkpointDescriptorEqual(a.Descriptor, b.Descriptor) &&
		checkpointCertificateEqual(a.Certificate, b.Certificate)
}

func checkpointDescriptorEqual(a, b CheckpointDescriptor) bool {
	if a.ID != b.ID || a.Cluster != b.Cluster || !confStateEqual(a.Conf, b.Conf) ||
		a.Barrier != b.Barrier || a.BarrierTupleDigest != b.BarrierTupleDigest ||
		a.ApplicationDigest != b.ApplicationDigest || a.ProtocolStateDigest != b.ProtocolStateDigest ||
		!executionFrontierEqual(a.Through, b.Through) ||
		len(a.Voters) != len(b.Voters) || len(a.AllocatorFences) != len(b.AllocatorFences) {
		return false
	}
	for i := range a.Voters {
		if !voterIdentityEqual(a.Voters[i], b.Voters[i]) {
			return false
		}
	}
	for i := range a.AllocatorFences {
		if a.AllocatorFences[i] != b.AllocatorFences[i] {
			return false
		}
	}
	return true
}

func checkpointCertificateEqual(a, b CheckpointCertificate) bool {
	if a.DescriptorDigest != b.DescriptorDigest || a.Checksum != b.Checksum ||
		len(a.Acknowledgements) != len(b.Acknowledgements) {
		return false
	}
	for i := range a.Acknowledgements {
		if a.Acknowledgements[i].AttestedDigest != b.Acknowledgements[i].AttestedDigest ||
			!voterIdentityEqual(a.Acknowledgements[i].Voter, b.Acknowledgements[i].Voter) {
			return false
		}
	}
	return true
}

type checkpointRound struct {
	barrier            InstanceRef
	request            CheckpointRequest
	result             CheckpointResult
	checkpoint         Checkpoint
	votes              map[ReplicaID]VoterAcknowledgement
	preparedDurable    bool
	certificateDurable bool
}

func checkpointBarrierTupleDigest(record InstanceRecord) StateDigest {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte("epaxos/checkpoint-barrier-tuple/v1"))
	writeRef(h, record.Ref)
	writeBallot(h, record.RecordBallot)
	writeUint64(h, record.Seq)
	writeUint64(h, uint64(len(record.Deps)))
	for _, dependency := range record.Deps {
		writeUint64(h, uint64(dependency))
	}
	writeEntryValue(h, record.Kind, record.Command, record.ConfChange, record.ProtocolControl)
	return StateDigest(sumHash(h))
}

func checkpointProtocolStateDigest(history map[ConfID]ConfState, through ExecutionFrontier, barrier StateDigest, fences []AllocatorFence) StateDigest {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte("epaxos/checkpoint-protocol-state/v1"))
	configs := make([]ConfID, 0, len(history))
	included := make(map[ConfID]struct{}, len(through.Configs))
	for _, config := range through.Configs {
		included[config.Conf] = struct{}{}
	}
	for id := range history {
		if _, ok := included[id]; ok {
			configs = append(configs, id)
		}
	}
	sort.Slice(configs, func(i, j int) bool { return configs[i] < configs[j] })
	writeUint64(h, uint64(len(configs)))
	for _, id := range configs {
		conf := history[id]
		writeUint64(h, uint64(conf.ID))
		writeUint64(h, uint64(len(conf.Voters)))
		for _, voter := range conf.Voters {
			writeUint64(h, uint64(voter))
		}
	}
	writeBytes(h, barrier[:])
	frontier := digestExecutionFrontier("epaxos/checkpoint-frontier/v1", through)
	writeBytes(h, frontier[:])
	writeUint64(h, uint64(len(fences)))
	for _, fence := range fences {
		writeUint64(h, uint64(fence.Conf))
		writeUint64(h, uint64(fence.Replica))
		writeUint64(h, uint64(fence.Next))
	}
	return StateDigest(sumHash(h))
}

func checkpointRequestEqual(a, b CheckpointRequest) bool {
	return a.ID == b.ID && executionFrontierEqual(a.Through, b.Through)
}

func canonicalCompactionRanges(checkpoint Checkpoint) []CompactionRange {
	if !checkpointCertified(checkpoint) {
		return nil
	}
	var ranges []CompactionRange
	for _, config := range checkpoint.Descriptor.Through.Configs {
		for _, lane := range config.Lanes {
			if lane.Through == 0 {
				continue
			}
			ranges = append(ranges, CompactionRange{
				Checkpoint: checkpoint.Descriptor.ID,
				Conf:       config.Conf,
				Replica:    lane.Replica,
				Through:    lane.Through,
			})
		}
	}
	return ranges
}

// SnapshotMode identifies whether Ready persists a local snapshot or installs a remote one.
type SnapshotMode uint8

// Snapshot modes distinguish local persistence from remote installation.
const (
	// SnapshotPersistLocal persists a locally prepared certified checkpoint.
	SnapshotPersistLocal SnapshotMode = iota + 1
	// SnapshotInstall installs a verified checkpoint received from a peer.
	SnapshotInstall
)

// Snapshot carries a checkpoint and its persistence mode through Ready.
type Snapshot struct {
	Mode       SnapshotMode
	Checkpoint Checkpoint
}

// Clone returns a deep copy of the snapshot.
func (s Snapshot) Clone() Snapshot {
	s.Checkpoint = s.Checkpoint.Clone()
	return s
}

// CompactionRange authorizes deletion through one certified replica-lane frontier.
type CompactionRange struct {
	Checkpoint CheckpointID
	Conf       ConfID
	Replica    ReplicaID
	Through    InstanceNum
}

// CheckpointRequest asks the application to snapshot at an exact execution frontier.
type CheckpointRequest struct {
	ID      CheckpointID
	Through ExecutionFrontier
}

// Clone returns a deep copy of the checkpoint request.
func (r CheckpointRequest) Clone() CheckpointRequest {
	r.Through = r.Through.Clone()
	return r
}

// CheckpointResult returns the application's opaque snapshot handle and state digest.
type CheckpointResult struct {
	ID                  CheckpointID
	ApplicationSnapshot []byte
	ApplicationDigest   StateDigest
}

// Checkpoint validation and lifecycle errors.
var (
	// ErrInvalidCheckpoint reports malformed or uncertified checkpoint state.
	ErrInvalidCheckpoint = errors.New("epaxos: invalid checkpoint")
	// ErrUnrequestedCheckpoint reports an application result without a pending request.
	ErrUnrequestedCheckpoint = errors.New("epaxos: unrequested checkpoint")
	// ErrCheckpointMismatch reports a result or attestation for a different checkpoint.
	ErrCheckpointMismatch = errors.New("epaxos: checkpoint mismatch")
)

func digestExecutionFrontier(hashDomain string, frontier ExecutionFrontier) StateDigest {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte(hashDomain))
	writeUint64(h, uint64(len(frontier.Configs)))
	for _, config := range frontier.Configs {
		writeUint64(h, uint64(config.Conf))
		writeUint64(h, uint64(len(config.Lanes)))
		for _, lane := range config.Lanes {
			writeUint64(h, uint64(lane.Replica))
			writeUint64(h, uint64(lane.Through))
		}
	}
	return StateDigest(sumHash(h))
}

func deriveCheckpointID(barrier StateDigest, frontier ExecutionFrontier) CheckpointID {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte("epaxos/checkpoint-id/v1"))
	writeBytes(h, barrier[:])
	frontierDigest := digestExecutionFrontier("epaxos/checkpoint-frontier/v1", frontier)
	writeBytes(h, frontierDigest[:])
	return CheckpointID(sumHash(h))
}

// DigestCheckpointDescriptor returns the canonical descriptor digest.
func DigestCheckpointDescriptor(descriptor CheckpointDescriptor) StateDigest {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte("epaxos/checkpoint-descriptor/v1"))
	writeBytes(h, descriptor.ID[:])
	writeBytes(h, descriptor.Cluster[:])
	writeUint64(h, uint64(descriptor.Conf.ID))
	writeUint64(h, uint64(len(descriptor.Conf.Voters)))
	for _, voter := range descriptor.Conf.Voters {
		writeUint64(h, uint64(voter))
	}
	writeUint64(h, uint64(len(descriptor.Voters)))
	for _, voter := range descriptor.Voters {
		writeUint64(h, uint64(voter.Replica))
		writeUint64(h, voter.Incarnation)
	}
	writeRef(h, descriptor.Barrier)
	writeBytes(h, descriptor.BarrierTupleDigest[:])
	frontier := digestExecutionFrontier("epaxos/checkpoint-frontier/v1", descriptor.Through)
	writeBytes(h, frontier[:])
	writeUint64(h, uint64(len(descriptor.AllocatorFences)))
	for _, fence := range descriptor.AllocatorFences {
		writeUint64(h, uint64(fence.Conf))
		writeUint64(h, uint64(fence.Replica))
		writeUint64(h, uint64(fence.Next))
	}
	writeBytes(h, descriptor.ApplicationDigest[:])
	writeBytes(h, descriptor.ProtocolStateDigest[:])
	return StateDigest(sumHash(h))
}

// DigestCheckpointCertificate returns the canonical certificate digest.
func DigestCheckpointCertificate(certificate CheckpointCertificate) StateDigest {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte("epaxos/checkpoint-certificate/v1"))
	writeBytes(h, certificate.DescriptorDigest[:])
	writeUint64(h, uint64(len(certificate.Acknowledgements)))
	for _, acknowledgement := range certificate.Acknowledgements {
		writeUint64(h, uint64(acknowledgement.Voter.Replica))
		writeUint64(h, acknowledgement.Voter.Incarnation)
		writeBytes(h, acknowledgement.AttestedDigest[:])
	}
	return StateDigest(sumHash(h))
}

// DigestCheckpoint returns the canonical digest of the complete checkpoint state.
func DigestCheckpoint(checkpoint Checkpoint) StateDigest {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte("epaxos/checkpoint-state/v1"))
	descriptor := DigestCheckpointDescriptor(checkpoint.Descriptor)
	writeBytes(h, descriptor[:])
	certificate := DigestCheckpointCertificate(checkpoint.Certificate)
	writeBytes(h, certificate[:])
	writeBytes(h, checkpoint.ApplicationSnapshot)
	compacted := digestExecutionFrontier("epaxos/checkpoint-compacted/v1", checkpoint.CompactedThrough)
	writeBytes(h, compacted[:])
	return StateDigest(sumHash(h))
}

// BuildCheckpointCertificate validates and canonicalizes a slow-quorum attestation set.
func BuildCheckpointCertificate(descriptor CheckpointDescriptor, acknowledgements []VoterAcknowledgement) (CheckpointCertificate, error) {
	digest := DigestCheckpointDescriptor(descriptor)
	acknowledgements = cloneVoterAcknowledgements(acknowledgements)
	sort.Slice(acknowledgements, func(i, j int) bool { return acknowledgements[i].Voter.Replica < acknowledgements[j].Voter.Replica })
	if len(acknowledgements) != slowQuorumSize(len(descriptor.Conf.Voters)) {
		return CheckpointCertificate{}, ErrInvalidCheckpoint
	}
	for i, acknowledgement := range acknowledgements {
		if i > 0 && acknowledgements[i-1].Voter.Replica >= acknowledgement.Voter.Replica || acknowledgement.AttestedDigest != digest {
			return CheckpointCertificate{}, ErrInvalidCheckpoint
		}
		found := false
		for _, identity := range descriptor.Voters {
			if voterIdentityEqual(identity, acknowledgement.Voter) {
				found = true
				break
			}
		}
		if !found {
			return CheckpointCertificate{}, ErrInvalidCheckpoint
		}
	}
	certificate := CheckpointCertificate{DescriptorDigest: digest, Acknowledgements: acknowledgements}
	certificate.Checksum = DigestCheckpointCertificate(certificate)
	return certificate, nil
}

func validateCheckpoint(checkpoint Checkpoint, history map[ConfID]ConfState) error {
	if checkpoint.Empty() {
		return nil
	}
	d := checkpoint.Descriptor
	if d.ApplicationDigest == (StateDigest{}) || d.Barrier.IsZero() || d.BarrierTupleDigest == (StateDigest{}) ||
		d.ProtocolStateDigest == (StateDigest{}) || deriveCheckpointID(d.BarrierTupleDigest, d.Through) != d.ID ||
		len(checkpoint.ApplicationSnapshot) == 0 || len(checkpoint.ApplicationSnapshot) > maxApplicationSnapshotHandle ||
		checkpoint.Checksum != DigestCheckpoint(checkpoint) {
		return ErrInvalidCheckpoint
	}
	if err := validateExecutionFrontier(d.Through, history); err != nil {
		return err
	}
	conf, ok := history[d.Conf.ID]
	if !ok || !confStateEqual(conf, d.Conf) || d.Barrier.Conf != d.Conf.ID ||
		len(d.Voters) != len(d.Conf.Voters) {
		return ErrInvalidCheckpoint
	}
	for i, voter := range d.Conf.Voters {
		if d.Voters[i].Replica != voter || !d.Voters[i].valid() ||
			(i > 0 && d.Voters[i-1].Replica >= d.Voters[i].Replica) {
			return ErrInvalidCheckpoint
		}
	}
	fenceIndex := 0
	for _, config := range d.Through.Configs {
		for _, lane := range config.Lanes {
			if fenceIndex >= len(d.AllocatorFences) {
				return ErrInvalidCheckpoint
			}
			fence := d.AllocatorFences[fenceIndex]
			minimum, exists := instanceSuccessor(lane.Through)
			if !exists || fence.Conf != config.Conf || fence.Replica != lane.Replica || fence.Next < minimum {
				return ErrInvalidCheckpoint
			}
			fenceIndex++
		}
	}
	if fenceIndex != len(d.AllocatorFences) ||
		d.ProtocolStateDigest != checkpointProtocolStateDigest(history, d.Through, d.BarrierTupleDigest, d.AllocatorFences) {
		return ErrInvalidCheckpoint
	}
	if len(checkpoint.CompactedThrough.Configs) != 0 &&
		frontierRegressesExecution(checkpoint.CompactedThrough, d.Through) {
		return ErrInvalidCheckpoint
	}
	if !checkpoint.Certificate.Empty() {
		built, err := BuildCheckpointCertificate(d, checkpoint.Certificate.Acknowledgements)
		if err != nil || built.Checksum != checkpoint.Certificate.Checksum {
			return ErrInvalidCheckpoint
		}
	}
	return nil
}

type checkpointControlType uint8

const (
	checkpointControlVote checkpointControlType = iota + 1
	checkpointControlCertificate
	checkpointControlOffer
)

type checkpointControlWire struct {
	Version    uint8
	Type       checkpointControlType
	Descriptor CheckpointDescriptor
	Vote       VoterAcknowledgement
	Checkpoint Checkpoint
}

func encodeCheckpointControl(wire checkpointControlWire) ([]byte, error) {
	wire.Version = checkpointWireVersion
	return json.Marshal(wire)
}

func decodeCheckpointControl(payload []byte) (checkpointControlWire, error) {
	var wire checkpointControlWire
	if len(payload) == 0 || len(payload) > maxWireProtocolControl || json.Unmarshal(payload, &wire) != nil || wire.Version != checkpointWireVersion {
		return checkpointControlWire{}, ErrInvalidCheckpoint
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, payload) {
		return checkpointControlWire{}, ErrInvalidCheckpoint
	}
	return wire, nil
}

func validateCheckpointControlMessage(message Message) error {
	if message.Kind != EntryCheckpoint || !commandEmpty(message.Command) ||
		message.ConfChange != (ConfChange{}) || message.RecordBallot != (Ballot{}) ||
		message.Seq != 0 || len(message.Deps) != 0 || message.AcceptSeq != 0 ||
		len(message.AcceptDeps) != 0 || len(message.AcceptEvidence) != 0 ||
		message.ProcessAt != 0 || message.TOQ || message.Reject ||
		message.RejectHint != (Ballot{}) || message.RejectReason != RejectNone ||
		message.ConflictRef != (InstanceRef{}) || message.ConflictStatus != StatusNone ||
		message.RecordStatus != StatusNone || message.FastPathEligible ||
		message.DepsCommitted != 0 || message.IgnoreDependency != (TryPreAcceptIgnore{}) {
		return ErrInvalidMessage
	}
	wire, err := decodeCheckpointControl(message.ProtocolControl)
	if err != nil {
		return ErrInvalidMessage
	}
	switch message.Type {
	case MsgCheckpointVote:
		if wire.Type != checkpointControlVote || wire.Descriptor.ID == (CheckpointID{}) ||
			wire.Vote.Voter.Replica != message.From ||
			wire.Vote.AttestedDigest != DigestCheckpointDescriptor(wire.Descriptor) ||
			!wire.Checkpoint.Empty() {
			return ErrInvalidMessage
		}
	case MsgCheckpointCertificate:
		if wire.Type != checkpointControlCertificate || !checkpointCertified(wire.Checkpoint) ||
			wire.Checkpoint.Descriptor.ID == (CheckpointID{}) {
			return ErrInvalidMessage
		}
	case MsgCheckpointOffer:
		if wire.Type != checkpointControlOffer || !checkpointCertified(wire.Checkpoint) ||
			wire.Checkpoint.Descriptor.ID == (CheckpointID{}) ||
			len(wire.Checkpoint.ApplicationSnapshot) == 0 {
			return ErrInvalidMessage
		}
	case MsgPreAccept, MsgPreAcceptResp, MsgAccept, MsgAcceptResp, MsgCommit, MsgPrepare, MsgPrepareResp,
		MsgTryPreAccept, MsgTryPreAcceptResp, MsgEvidence, MsgEvidenceResp:
		return ErrInvalidMessage
	default:
		return ErrInvalidMessage
	}
	return nil
}

// CheckpointOffer returns the certified checkpoint carried by a snapshot offer.
// Embeddings use it to materialize the opaque application snapshot before Step.
func CheckpointOffer(message Message) (Checkpoint, bool, error) {
	if message.Type != MsgCheckpointOffer {
		return Checkpoint{}, false, nil
	}
	if err := validateCheckpointControlMessage(message); err != nil {
		return Checkpoint{}, false, err
	}
	wire, err := decodeCheckpointControl(message.ProtocolControl)
	if err != nil || wire.Type != checkpointControlOffer {
		return Checkpoint{}, false, ErrInvalidMessage
	}
	return wire.Checkpoint.Clone(), true, nil
}
