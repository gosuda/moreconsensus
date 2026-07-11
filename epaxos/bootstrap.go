package epaxos

import (
	"bytes"
	"container/heap"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

const (
	bootstrapWireVersion       = 1
	maxBootstrapPayload        = 1 << 20
	maxBootstrapChunk          = 1 << 20
	maxBootstrapTransfer       = 1 << 34
	maxBootstrapChunks         = 1 << 16
	maxBootstrapSigners        = 7
	maxBootstrapFrontierLanes  = 7
	maxBootstrapSparseRefs     = 1 << 16
	maxBootstrapHistoryEntries = 1 << 16
)

var bootstrapWireMagic = [...]byte{'E', 'P', 'X', 'B', 'O', 'O', 'T', '1'}
var bootstrapChunkMagic = [...]byte{'E', 'P', 'X', 'C', 'H', 'N', 'K', '1'}

// ClusterID identifies one independently provisioned consensus cluster.
type ClusterID [32]byte

// BootstrapID identifies one immutable voter bootstrap plan.
type BootstrapID [32]byte

// StateDigest authenticates a canonical state projection.
type StateDigest [32]byte

// VoterIdentity binds a replica number to one externally fenced incarnation and
// its Ed25519 verification key.
type VoterIdentity struct {
	Replica     ReplicaID
	Incarnation uint64
	VerifyKey   []byte
}

// Clone returns an ownership-independent identity.
func (v VoterIdentity) Clone() VoterIdentity {
	v.VerifyKey = append([]byte(nil), v.VerifyKey...)
	return v
}

func (v VoterIdentity) valid() bool {
	return v.Replica != 0 && v.Incarnation != 0 && len(v.VerifyKey) == ed25519.PublicKeySize
}

// ControlReservations are the consecutive old-configuration refs owned by the
// Prepare coordinator. They are never available to ordinary proposals.
type ControlReservations struct {
	Prepare  InstanceRef
	Activate InstanceRef
	Abort    InstanceRef
}

// ValidFor reports whether the refs are consecutive and pinned to base.
func (r ControlReservations) ValidFor(base ConfState) bool {
	if r.Prepare.Replica == 0 || r.Prepare.Instance == 0 || r.Prepare.Conf != base.ID ||
		r.Activate.Replica != r.Prepare.Replica || r.Abort.Replica != r.Prepare.Replica ||
		r.Activate.Conf != base.ID || r.Abort.Conf != base.ID ||
		r.Prepare.Instance >= ^InstanceNum(0)-1 {
		return false
	}
	return r.Activate.Instance == r.Prepare.Instance+1 && r.Abort.Instance == r.Prepare.Instance+2
}

// BootstrapLaneFrontier is the exact retained evidence for one old-config lane.
// Sparse is sorted and unique and contains every retained ref above the compacted
// executed prefix. Until certified compaction summaries exist, the compacted
// prefix must remain zero.
type BootstrapLaneFrontier struct {
	Replica                  ReplicaID
	ObservedThrough          InstanceNum
	CommittedThrough         InstanceNum
	ExecutedThrough          InstanceNum
	CompactedExecutedThrough InstanceNum
	Sparse                   []InstanceNum
}

// Clone returns an ownership-independent frontier lane.
func (l BootstrapLaneFrontier) Clone() BootstrapLaneFrontier {
	l.Sparse = append([]InstanceNum(nil), l.Sparse...)
	return l
}

// BootstrapFrontier is a canonical per-lane sparse closure frontier.
type BootstrapFrontier struct {
	Conf  ConfID
	Lanes []BootstrapLaneFrontier
}

// Clone returns an ownership-independent frontier.
func (f BootstrapFrontier) Clone() BootstrapFrontier {
	out := BootstrapFrontier{Conf: f.Conf, Lanes: make([]BootstrapLaneFrontier, len(f.Lanes))}
	for i := range f.Lanes {
		out.Lanes[i] = f.Lanes[i].Clone()
	}
	return out
}

func (f BootstrapFrontier) lane(replica ReplicaID) (BootstrapLaneFrontier, bool) {
	i := sort.Search(len(f.Lanes), func(i int) bool { return f.Lanes[i].Replica >= replica })
	if i >= len(f.Lanes) || f.Lanes[i].Replica != replica {
		return BootstrapLaneFrontier{}, false
	}
	return f.Lanes[i], true
}

func validateBootstrapFrontier(f BootstrapFrontier, base ConfState) error {
	if f.Conf != base.ID || len(f.Lanes) != len(base.Voters) || len(f.Lanes) > maxBootstrapFrontierLanes {
		return ErrBootstrapCertificate
	}
	totalSparse := 0
	totalCoverage := 0
	for i, voter := range base.Voters {
		lane := f.Lanes[i]
		if lane.Replica != voter || lane.CompactedExecutedThrough > lane.ExecutedThrough ||
			lane.ExecutedThrough > lane.ObservedThrough || lane.CommittedThrough > lane.ObservedThrough {
			return ErrBootstrapCertificate
		}
		if lane.CompactedExecutedThrough != 0 {
			return ErrBootstrapCompactionEvidence
		}
		coverage := lane.ObservedThrough - lane.CompactedExecutedThrough
		if coverage > InstanceNum(maxBootstrapSparseRefs-totalCoverage) {
			return ErrBootstrapBounds
		}
		totalCoverage += int(coverage)
		for j, instance := range lane.Sparse {
			if instance == 0 || instance <= lane.CompactedExecutedThrough || instance > lane.ObservedThrough ||
				(j > 0 && lane.Sparse[j-1] >= instance) {
				return ErrBootstrapCertificate
			}
		}
		totalSparse += len(lane.Sparse)
		if totalSparse > maxBootstrapSparseRefs {
			return ErrBootstrapBounds
		}
	}
	return nil
}

// PrepareVoterRequest describes one immutable additive C to C+target plan.
type PrepareVoterRequest struct {
	Cluster              ClusterID
	Plan                 BootstrapID
	Base                 ConfState
	OldVoters            []VoterIdentity
	Target               VoterIdentity
	Source               ReplicaID
	SourceDigest         StateDigest
	ReleaseDigest        StateDigest
	TargetAllocatorFloor InstanceNum
	TOQ                  bool
	SuccessorTOQ         TOQRuntimeConfig
	TOQClosedThrough     uint64
	TimingDigest         StateDigest
}

// Clone returns an ownership-independent request.
func (r PrepareVoterRequest) Clone() PrepareVoterRequest {
	out := r
	out.Base = r.Base.Clone()
	out.OldVoters = cloneVoterIdentities(r.OldVoters)
	out.Target = r.Target.Clone()
	out.SuccessorTOQ = r.SuccessorTOQ.Clone()
	return out
}

// VoterPlan is the canonical durable result of PrepareVoter.
type VoterPlan struct {
	Request       PrepareVoterRequest
	Reservations  ControlReservations
	Successor     ConfState
	RequestDigest StateDigest
}

// Clone returns an ownership-independent plan.
func (p VoterPlan) Clone() VoterPlan {
	p.Request = p.Request.Clone()
	p.Successor = p.Successor.Clone()
	return p
}

// LocalSeal is a signer's durable admission fence and sparse evidence.
type LocalSeal struct {
	Plan         BootstrapID
	Base         ConfState
	Signer       VoterIdentity
	Reservations ControlReservations
	Frontier     BootstrapFrontier
	Digest       StateDigest
	Signature    []byte
}

// Clone returns an ownership-independent seal.
func (s LocalSeal) Clone() LocalSeal {
	s.Base = s.Base.Clone()
	s.Signer = s.Signer.Clone()
	s.Frontier = s.Frontier.Clone()
	s.Signature = append([]byte(nil), s.Signature...)
	return s
}

// SealCertificate is an exact old slow quorum of identical-plan local seals
// together with their deterministic componentwise union frontier.
type SealCertificate struct {
	Plan         BootstrapID
	Base         ConfState
	Reservations ControlReservations
	Frontier     BootstrapFrontier
	Seals        []LocalSeal
	Digest       StateDigest
}

// Clone returns an ownership-independent certificate.
func (c SealCertificate) Clone() SealCertificate {
	c.Base = c.Base.Clone()
	c.Frontier = c.Frontier.Clone()
	out := make([]LocalSeal, len(c.Seals))
	for i := range c.Seals {
		out[i] = c.Seals[i].Clone()
	}
	c.Seals = out
	return c
}

// SnapshotDescriptor binds the finite sealed state installed by the target.
type SnapshotDescriptor struct {
	Cluster              ClusterID
	Plan                 BootstrapID
	Base                 ConfState
	Successor            ConfState
	Target               VoterIdentity
	Source               ReplicaID
	Reservations         ControlReservations
	SealDigest           StateDigest
	Frontier             BootstrapFrontier
	ManifestDigest       StateDigest
	DeltaFirst           uint64
	DeltaLast            uint64
	DeltaRoot            StateDigest
	ApplicationDigest    StateDigest
	IdempotencyDigest    StateDigest
	ConfigHistoryDigest  StateDigest
	InstanceDigest       StateDigest
	HardStateDigest      StateDigest
	CompactionDigest     StateDigest
	InstalledStateDigest StateDigest
	TargetAllocatorFloor InstanceNum
	TOQClosedThrough     uint64
	TOQRuntimeDigest     StateDigest
	TimingDigest         StateDigest
	ReleaseDigest        StateDigest
}

// Clone returns an ownership-independent descriptor.
func (d SnapshotDescriptor) Clone() SnapshotDescriptor {
	d.Base = d.Base.Clone()
	d.Successor = d.Successor.Clone()
	d.Target = d.Target.Clone()
	d.Frontier = d.Frontier.Clone()
	return d
}

// VoterAttestation is one old voter signature over an exact digest.
type VoterAttestation struct {
	Signer         VoterIdentity
	AttestedDigest StateDigest
	Signature      []byte
}

// Clone returns an ownership-independent attestation.
func (a VoterAttestation) Clone() VoterAttestation {
	a.Signer = a.Signer.Clone()
	a.Signature = append([]byte(nil), a.Signature...)
	return a
}

// SnapshotCertificate is an exact old slow quorum certificate over Descriptor.
type SnapshotCertificate struct {
	Descriptor   SnapshotDescriptor
	Attestations []VoterAttestation
	Digest       StateDigest
}

// Clone returns an ownership-independent certificate.
func (c SnapshotCertificate) Clone() SnapshotCertificate {
	c.Descriptor = c.Descriptor.Clone()
	c.Attestations = cloneVoterAttestations(c.Attestations)
	return c
}

// VoterReadyProof is signed only by the staged target after exact install.
type VoterReadyProof struct {
	Cluster              ClusterID
	Plan                 BootstrapID
	Target               VoterIdentity
	SnapshotDigest       StateDigest
	InstalledStateDigest StateDigest
	AllocatorFloor       InstanceNum
	TOQClosedThrough     uint64
	Signature            []byte
}

// Clone returns an ownership-independent proof.
func (p VoterReadyProof) Clone() VoterReadyProof {
	p.Target = p.Target.Clone()
	p.Signature = append([]byte(nil), p.Signature...)
	return p
}

// TargetReadyRecord is the durable locally replicated target proof.
type TargetReadyRecord struct {
	Plan  BootstrapID
	Proof VoterReadyProof
}

// Clone returns an ownership-independent record.
func (r TargetReadyRecord) Clone() TargetReadyRecord {
	r.Proof = r.Proof.Clone()
	return r
}

// ReadyCertificate is an exact old slow quorum certificate that the target
// proof was durably recorded.
type ReadyCertificate struct {
	Plan         BootstrapID
	Proof        VoterReadyProof
	Attestations []VoterAttestation
	Digest       StateDigest
}

// Clone returns an ownership-independent certificate.
func (c ReadyCertificate) Clone() ReadyCertificate {
	c.Proof = c.Proof.Clone()
	c.Attestations = cloneVoterAttestations(c.Attestations)
	return c
}

// BootstrapPhase is the durable phase of a voter plan.
type BootstrapPhase uint8

const (
	BootstrapPhaseUnspecified BootstrapPhase = iota
	BootstrapPhasePreparing
	BootstrapPhasePrepared
	BootstrapPhaseLocalSealed
	BootstrapPhaseSealed
	BootstrapPhaseCertified
	BootstrapPhaseTargetReady
	BootstrapPhaseFinalizing
	BootstrapPhaseActivated
	BootstrapPhaseAborted
)

// BootstrapOutcome is a terminal replicated membership-control outcome.
type BootstrapOutcome uint8

const (
	BootstrapOutcomeUnspecified BootstrapOutcome = iota
	BootstrapOutcomeActivated
	BootstrapOutcomeAborted
	BootstrapOutcomeRejectedSuperseded
	BootstrapOutcomeRejectedInvalid
)

// MembershipResult is authenticated in the terminal instance record.
type MembershipResult struct {
	Plan              BootstrapID
	Outcome           BootstrapOutcome
	ExitRef           InstanceRef
	CertificateDigest StateDigest
	Successor         ConfState
}

// Clone returns an ownership-independent result.
func (r MembershipResult) Clone() MembershipResult {
	r.Successor = r.Successor.Clone()
	return r
}

// ClosedConfig permanently pins delayed traffic to its certified old frontier.
type ClosedConfig struct {
	Conf              ConfState
	Frontier          BootstrapFrontier
	Reservations      ControlReservations
	CertificateDigest StateDigest
}

// Clone returns an ownership-independent closed configuration.
func (c ClosedConfig) Clone() ClosedConfig {
	c.Conf = c.Conf.Clone()
	c.Frontier = c.Frontier.Clone()
	return c
}

// BootstrapRecord is the atomic durable projection of one plan phase.
type BootstrapRecord struct {
	Plan                VoterPlan
	Phase               BootstrapPhase
	Outcome             BootstrapOutcome
	LocalSeal           LocalSeal
	SealCertificate     SealCertificate
	SnapshotCertificate SnapshotCertificate
	TargetReady         TargetReadyRecord
	ReadyCertificate    ReadyCertificate
	TerminalRef         InstanceRef
	Closed              ClosedConfig
	Digest              StateDigest
}

// Clone returns an ownership-independent record.
func (r BootstrapRecord) Clone() BootstrapRecord {
	r.Plan = r.Plan.Clone()
	r.LocalSeal = r.LocalSeal.Clone()
	r.SealCertificate = r.SealCertificate.Clone()
	r.SnapshotCertificate = r.SnapshotCertificate.Clone()
	r.TargetReady = r.TargetReady.Clone()
	r.ReadyCertificate = r.ReadyCertificate.Clone()
	r.Closed = r.Closed.Clone()
	return r
}

// LocalVoterStatus is the durable local admission state.
type LocalVoterStatus uint8

const (
	LocalVoterStatusUnspecified LocalVoterStatus = iota
	LocalVoterStatusStaged
	LocalVoterStatusEligible
	LocalVoterStatusIneligible
)

// LocalVoterState gates every local vote, proposal, recovery, and allocation.
type LocalVoterState struct {
	Cluster          ClusterID
	Identity         VoterIdentity
	Conf             ConfState
	Status           LocalVoterStatus
	Plan             BootstrapID
	InstalledDigest  StateDigest
	AllocatorFloor   InstanceNum
	TOQClosedThrough uint64
}

// Clone returns an ownership-independent local voter state.
func (s LocalVoterState) Clone() LocalVoterState {
	s.Identity = s.Identity.Clone()
	s.Conf = s.Conf.Clone()
	return s
}

// IsEligible reports whether the exact local identity may participate in Conf.
func (s LocalVoterState) IsEligible(replica ReplicaID, conf ConfState) bool {
	return s.Status == LocalVoterStatusEligible && s.Identity.Replica == replica &&
		s.Conf.ID == conf.ID && sameReplicaIDs(s.Conf.Voters, conf.Voters)
}

// ConfigHistoryEntry is an exact durable configuration winner.
type ConfigHistoryEntry struct {
	Conf           ConfState
	AppliedRef     InstanceRef
	IdentityDigest StateDigest
}

// Clone returns an ownership-independent history entry.
func (e ConfigHistoryEntry) Clone() ConfigHistoryEntry {
	e.Conf = e.Conf.Clone()
	return e
}

// FrontierUpdate is an atomic durable frontier and allocator projection.
type FrontierUpdate struct {
	Frontier         BootstrapFrontier
	AllocatorFloor   InstanceNum
	TOQClosedThrough uint64
	EvidenceDigest   StateDigest
}

// Clone returns an ownership-independent update.
func (u FrontierUpdate) Clone() FrontierUpdate {
	u.Frontier = u.Frontier.Clone()
	return u
}

// StorageState is the complete initial durable consensus projection.
type StorageState struct {
	HardState        HardState
	ConfigHistory    []ConfigHistoryEntry
	BootstrapRecords []BootstrapRecord
	LocalVoterState  LocalVoterState
	Frontiers        []FrontierUpdate
	AllocatorFloor   InstanceNum
	TOQClosedThrough uint64
}

// Clone returns an ownership-independent storage state.
func (s StorageState) Clone() StorageState {
	out := s
	out.HardState = s.HardState.Clone()
	out.ConfigHistory = cloneConfigHistory(s.ConfigHistory)
	out.BootstrapRecords = cloneBootstrapRecords(s.BootstrapRecords)
	out.LocalVoterState = s.LocalVoterState.Clone()
	out.Frontiers = cloneFrontierUpdates(s.Frontiers)
	return out
}

// BootstrapClosureStatus reports exact sparse unresolved obligations.
type BootstrapClosureStatus struct {
	Plan     BootstrapID
	Complete bool
	Missing  []InstanceRef
	Retained int
}

// BootstrapStatusSnapshot is a copy-only view of all core bootstrap state.
type BootstrapStatusSnapshot struct {
	LocalVoter LocalVoterState
	Plans      []BootstrapRecord
	Closed     []ClosedConfig
}

// BootstrapExit selects one reserved terminal control ref for recovery.
type BootstrapExit uint8

const (
	BootstrapExitActivate BootstrapExit = iota + 1
	BootstrapExitAbort
)

// BootstrapMessageType identifies an authenticated out-of-band bootstrap message.
type BootstrapMessageType uint8

const (
	BootstrapMsgSealRequest BootstrapMessageType = iota + 1
	BootstrapMsgSealAck
	BootstrapMsgCanonicalSeal
	BootstrapMsgSnapshotVote
	BootstrapMsgInstallProof
	BootstrapMsgReadyAck
	BootstrapMsgReadyQuery
	BootstrapMsgReadyResponse
	BootstrapMsgActivationNotice
)

// BootstrapMessage is separate from the EPaxos Message transport.
type BootstrapMessage struct {
	Type            BootstrapMessageType
	Cluster         ClusterID
	Plan            BootstrapID
	From            ReplicaID
	FromIncarnation uint64
	To              ReplicaID
	BaseID          ConfID
	BaseDigest      StateDigest
	Payload         []byte
	PayloadDigest   StateDigest
	Signature       []byte
}

// Clone returns an ownership-independent message.
func (m BootstrapMessage) Clone() BootstrapMessage {
	m.Payload = append([]byte(nil), m.Payload...)
	m.Signature = append([]byte(nil), m.Signature...)
	return m
}

// BootstrapChunk is one independently authenticated snapshot stream frame.
type BootstrapChunk struct {
	Cluster         ClusterID
	Plan            BootstrapID
	From            ReplicaID
	FromIncarnation uint64
	To              ReplicaID
	Manifest        StateDigest
	Index           uint64
	Offset          uint64
	Total           uint64
	Payload         []byte
	PayloadDigest   StateDigest
	Signature       []byte
}

// Clone returns an ownership-independent chunk.
func (c BootstrapChunk) Clone() BootstrapChunk {
	c.Payload = append([]byte(nil), c.Payload...)
	c.Signature = append([]byte(nil), c.Signature...)
	return c
}

// BootstrapChunkSet validates idempotent, non-overlapping chunks under a fixed quota.
type BootstrapChunkSet struct {
	Total  uint64
	Limit  uint64
	chunks map[uint64]BootstrapChunk
}

// Add validates and retains one chunk. Exact duplicates stutter.
func (s *BootstrapChunkSet) Add(chunk BootstrapChunk, identity VoterIdentity) error {
	if err := VerifyBootstrapChunk(chunk, identity); err != nil {
		return err
	}
	limit := s.Limit
	if limit == 0 || limit > maxBootstrapTransfer {
		limit = maxBootstrapTransfer
	}
	if chunk.Index >= maxBootstrapChunks || chunk.Total == 0 || chunk.Total > limit || chunk.Offset > chunk.Total ||
		uint64(len(chunk.Payload)) > chunk.Total-chunk.Offset {
		return ErrBootstrapBounds
	}
	if s.Total != 0 && s.Total != chunk.Total {
		return ErrBootstrapChunkConflict
	}
	if s.chunks == nil {
		s.chunks = make(map[uint64]BootstrapChunk)
	}
	if existing, ok := s.chunks[chunk.Index]; ok {
		if bootstrapChunkEqual(existing, chunk) {
			return nil
		}
		return ErrBootstrapChunkConflict
	}
	end := chunk.Offset + uint64(len(chunk.Payload))
	for _, existing := range s.chunks {
		existingEnd := existing.Offset + uint64(len(existing.Payload))
		if chunk.Offset < existingEnd && existing.Offset < end {
			return ErrBootstrapChunkConflict
		}
	}
	if len(s.chunks) >= maxBootstrapChunks {
		return ErrBootstrapBounds
	}
	s.Total = chunk.Total
	s.chunks[chunk.Index] = chunk.Clone()
	return nil
}

func bootstrapChunkEqual(a, b BootstrapChunk) bool {
	return a.Cluster == b.Cluster && a.Plan == b.Plan && a.From == b.From &&
		a.FromIncarnation == b.FromIncarnation && a.To == b.To && a.Manifest == b.Manifest &&
		a.Index == b.Index && a.Offset == b.Offset && a.Total == b.Total &&
		a.PayloadDigest == b.PayloadDigest && bytes.Equal(a.Payload, b.Payload) && bytes.Equal(a.Signature, b.Signature)
}

func cloneVoterIdentities(in []VoterIdentity) []VoterIdentity {
	out := make([]VoterIdentity, len(in))
	for i := range in {
		out[i] = in[i].Clone()
	}
	return out
}

func cloneVoterAttestations(in []VoterAttestation) []VoterAttestation {
	out := make([]VoterAttestation, len(in))
	for i := range in {
		out[i] = in[i].Clone()
	}
	return out
}

func cloneConfigHistory(in []ConfigHistoryEntry) []ConfigHistoryEntry {
	out := make([]ConfigHistoryEntry, len(in))
	for i := range in {
		out[i] = in[i].Clone()
	}
	return out
}

func cloneBootstrapRecords(in []BootstrapRecord) []BootstrapRecord {
	out := make([]BootstrapRecord, len(in))
	for i := range in {
		out[i] = in[i].Clone()
	}
	return out
}

func cloneFrontierUpdates(in []FrontierUpdate) []FrontierUpdate {
	out := make([]FrontierUpdate, len(in))
	for i := range in {
		out[i] = in[i].Clone()
	}
	return out
}

func appendFixed(dst []byte, value [32]byte) []byte { return append(dst, value[:]...) }

func appendRef(dst []byte, ref InstanceRef) []byte {
	dst = binary.AppendUvarint(dst, uint64(ref.Replica))
	dst = binary.AppendUvarint(dst, uint64(ref.Instance))
	return binary.AppendUvarint(dst, uint64(ref.Conf))
}

func appendConf(dst []byte, conf ConfState) []byte {
	dst = binary.AppendUvarint(dst, uint64(conf.ID))
	dst = binary.AppendUvarint(dst, uint64(len(conf.Voters)))
	for _, voter := range conf.Voters {
		dst = binary.AppendUvarint(dst, uint64(voter))
	}
	return dst
}

func appendIdentity(dst []byte, identity VoterIdentity) []byte {
	dst = binary.AppendUvarint(dst, uint64(identity.Replica))
	dst = binary.AppendUvarint(dst, identity.Incarnation)
	dst = binary.AppendUvarint(dst, uint64(len(identity.VerifyKey)))
	return append(dst, identity.VerifyKey...)
}

func appendReservations(dst []byte, refs ControlReservations) []byte {
	dst = appendRef(dst, refs.Prepare)
	dst = appendRef(dst, refs.Activate)
	return appendRef(dst, refs.Abort)
}

func appendFrontier(dst []byte, frontier BootstrapFrontier) []byte {
	dst = binary.AppendUvarint(dst, uint64(frontier.Conf))
	dst = binary.AppendUvarint(dst, uint64(len(frontier.Lanes)))
	for _, lane := range frontier.Lanes {
		dst = binary.AppendUvarint(dst, uint64(lane.Replica))
		dst = binary.AppendUvarint(dst, uint64(lane.ObservedThrough))
		dst = binary.AppendUvarint(dst, uint64(lane.CommittedThrough))
		dst = binary.AppendUvarint(dst, uint64(lane.ExecutedThrough))
		dst = binary.AppendUvarint(dst, uint64(lane.CompactedExecutedThrough))
		dst = binary.AppendUvarint(dst, uint64(len(lane.Sparse)))
		for _, instance := range lane.Sparse {
			dst = binary.AppendUvarint(dst, uint64(instance))
		}
	}
	return dst
}

func appendTOQRuntime(dst []byte, runtime TOQRuntimeConfig) []byte {
	dst = appendConf(dst, runtime.Conf)
	ids := make([]ReplicaID, 0, len(runtime.OneWayDelay))
	for id := range runtime.OneWayDelay {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	dst = binary.AppendUvarint(dst, uint64(len(ids)))
	for _, id := range ids {
		dst = binary.AppendUvarint(dst, uint64(id))
		dst = binary.AppendUvarint(dst, runtime.OneWayDelay[id])
	}
	dst = binary.AppendUvarint(dst, uint64(len(runtime.SyncGroup)))
	for _, id := range runtime.SyncGroup {
		dst = binary.AppendUvarint(dst, uint64(id))
	}
	return dst
}

func appendPrepareRequest(dst []byte, request PrepareVoterRequest) []byte {
	dst = appendFixed(dst, [32]byte(request.Cluster))
	dst = appendFixed(dst, [32]byte(request.Plan))
	dst = appendConf(dst, request.Base)
	dst = binary.AppendUvarint(dst, uint64(len(request.OldVoters)))
	for _, identity := range request.OldVoters {
		dst = appendIdentity(dst, identity)
	}
	dst = appendIdentity(dst, request.Target)
	dst = binary.AppendUvarint(dst, uint64(request.Source))
	dst = appendFixed(dst, [32]byte(request.SourceDigest))
	dst = appendFixed(dst, [32]byte(request.ReleaseDigest))
	dst = binary.AppendUvarint(dst, uint64(request.TargetAllocatorFloor))
	if request.TOQ {
		dst = append(dst, 1)
		dst = appendTOQRuntime(dst, request.SuccessorTOQ)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, request.TOQClosedThrough)
	return appendFixed(dst, [32]byte(request.TimingDigest))
}

func appendVoterPlan(dst []byte, plan VoterPlan) []byte {
	dst = appendPrepareRequest(dst, plan.Request)
	dst = appendReservations(dst, plan.Reservations)
	dst = appendConf(dst, plan.Successor)
	return appendFixed(dst, [32]byte(plan.RequestDigest))
}

func appendSnapshotDescriptor(dst []byte, descriptor SnapshotDescriptor) []byte {
	dst = appendFixed(dst, [32]byte(descriptor.Cluster))
	dst = appendFixed(dst, [32]byte(descriptor.Plan))
	dst = appendConf(dst, descriptor.Base)
	dst = appendConf(dst, descriptor.Successor)
	dst = appendIdentity(dst, descriptor.Target)
	dst = binary.AppendUvarint(dst, uint64(descriptor.Source))
	dst = appendReservations(dst, descriptor.Reservations)
	dst = appendFixed(dst, [32]byte(descriptor.SealDigest))
	dst = appendFrontier(dst, descriptor.Frontier)
	dst = appendFixed(dst, [32]byte(descriptor.ManifestDigest))
	dst = binary.AppendUvarint(dst, descriptor.DeltaFirst)
	dst = binary.AppendUvarint(dst, descriptor.DeltaLast)
	dst = appendFixed(dst, [32]byte(descriptor.DeltaRoot))
	dst = appendFixed(dst, [32]byte(descriptor.ApplicationDigest))
	dst = appendFixed(dst, [32]byte(descriptor.IdempotencyDigest))
	dst = appendFixed(dst, [32]byte(descriptor.ConfigHistoryDigest))
	dst = appendFixed(dst, [32]byte(descriptor.InstanceDigest))
	dst = appendFixed(dst, [32]byte(descriptor.HardStateDigest))
	dst = appendFixed(dst, [32]byte(descriptor.CompactionDigest))
	dst = appendFixed(dst, [32]byte(descriptor.InstalledStateDigest))
	dst = binary.AppendUvarint(dst, uint64(descriptor.TargetAllocatorFloor))
	dst = binary.AppendUvarint(dst, descriptor.TOQClosedThrough)
	dst = appendFixed(dst, [32]byte(descriptor.TOQRuntimeDigest))
	dst = appendFixed(dst, [32]byte(descriptor.TimingDigest))
	return appendFixed(dst, [32]byte(descriptor.ReleaseDigest))
}

func appendAttestation(dst []byte, attestation VoterAttestation) []byte {
	dst = appendIdentity(dst, attestation.Signer)
	dst = appendFixed(dst, [32]byte(attestation.AttestedDigest))
	dst = binary.AppendUvarint(dst, uint64(len(attestation.Signature)))
	return append(dst, attestation.Signature...)
}

func appendReadyProof(dst []byte, proof VoterReadyProof, signature bool) []byte {
	dst = appendFixed(dst, [32]byte(proof.Cluster))
	dst = appendFixed(dst, [32]byte(proof.Plan))
	dst = appendIdentity(dst, proof.Target)
	dst = appendFixed(dst, [32]byte(proof.SnapshotDigest))
	dst = appendFixed(dst, [32]byte(proof.InstalledStateDigest))
	dst = binary.AppendUvarint(dst, uint64(proof.AllocatorFloor))
	dst = binary.AppendUvarint(dst, proof.TOQClosedThrough)
	if signature {
		dst = binary.AppendUvarint(dst, uint64(len(proof.Signature)))
		dst = append(dst, proof.Signature...)
	}
	return dst
}

func domainDigest(domain string, canonical []byte) StateDigest {
	h := getHash()
	defer putHash(h)
	writeBytes(h, []byte(domain))
	writeBytes(h, canonical)
	return StateDigest(sumHash(h))
}

// DigestVoterPlan returns the canonical plan digest.
func DigestVoterPlan(plan VoterPlan) StateDigest {
	copyPlan := plan.Clone()
	copyPlan.RequestDigest = StateDigest{}
	return domainDigest("epaxos/bootstrap/plan/v1", appendVoterPlan(nil, copyPlan))
}

// DigestLocalSeal returns the digest signed by a seal signer.
func DigestLocalSeal(seal LocalSeal) StateDigest {
	canonical := appendFixed(nil, [32]byte(seal.Plan))
	canonical = appendConf(canonical, seal.Base)
	canonical = appendIdentity(canonical, seal.Signer)
	canonical = appendReservations(canonical, seal.Reservations)
	canonical = appendFrontier(canonical, seal.Frontier)
	return domainDigest("epaxos/bootstrap/local-seal/v1", canonical)
}

// DigestSealCertificate returns the canonical certificate digest.
func DigestSealCertificate(certificate SealCertificate) StateDigest {
	canonical := appendFixed(nil, [32]byte(certificate.Plan))
	canonical = appendConf(canonical, certificate.Base)
	canonical = appendReservations(canonical, certificate.Reservations)
	canonical = appendFrontier(canonical, certificate.Frontier)
	canonical = binary.AppendUvarint(canonical, uint64(len(certificate.Seals)))
	for _, seal := range certificate.Seals {
		canonical = appendFixed(canonical, [32]byte(seal.Digest))
		canonical = binary.AppendUvarint(canonical, uint64(len(seal.Signature)))
		canonical = append(canonical, seal.Signature...)
	}
	return domainDigest("epaxos/bootstrap/seal-certificate/v1", canonical)
}

// DigestSnapshotDescriptor returns the canonical descriptor digest.
func DigestSnapshotDescriptor(descriptor SnapshotDescriptor) StateDigest {
	return domainDigest("epaxos/bootstrap/snapshot/v1", appendSnapshotDescriptor(nil, descriptor))
}

// DigestSnapshotCertificate returns the canonical certificate digest.
func DigestSnapshotCertificate(certificate SnapshotCertificate) StateDigest {
	canonical := appendSnapshotDescriptor(nil, certificate.Descriptor)
	canonical = binary.AppendUvarint(canonical, uint64(len(certificate.Attestations)))
	for _, attestation := range certificate.Attestations {
		canonical = appendAttestation(canonical, attestation)
	}
	return domainDigest("epaxos/bootstrap/snapshot-certificate/v1", canonical)
}

// DigestVoterReadyProof returns the target proof digest.
func DigestVoterReadyProof(proof VoterReadyProof) StateDigest {
	return domainDigest("epaxos/bootstrap/target-ready/v1", appendReadyProof(nil, proof, false))
}

// DigestReadyCertificate returns the canonical ready certificate digest.
func DigestReadyCertificate(certificate ReadyCertificate) StateDigest {
	canonical := appendFixed(nil, [32]byte(certificate.Plan))
	canonical = appendReadyProof(canonical, certificate.Proof, true)
	canonical = binary.AppendUvarint(canonical, uint64(len(certificate.Attestations)))
	for _, attestation := range certificate.Attestations {
		canonical = appendAttestation(canonical, attestation)
	}
	return domainDigest("epaxos/bootstrap/ready-certificate/v1", canonical)
}

func publicKeyMatches(identity VoterIdentity, private ed25519.PrivateKey) bool {
	if !identity.valid() || len(private) != ed25519.PrivateKeySize {
		return false
	}
	public, ok := private.Public().(ed25519.PublicKey)
	return ok && bytes.Equal(public, identity.VerifyKey)
}

// SignLocalSeal signs a canonical local seal.
func SignLocalSeal(seal LocalSeal, private ed25519.PrivateKey) (LocalSeal, error) {
	if !publicKeyMatches(seal.Signer, private) {
		return LocalSeal{}, ErrBootstrapEligibility
	}
	seal.Digest = DigestLocalSeal(seal)
	seal.Signature = ed25519.Sign(private, seal.Digest[:])
	return seal, nil
}

// VerifyLocalSeal verifies one canonical local seal.
func VerifyLocalSeal(seal LocalSeal) error {
	if !seal.Signer.valid() || len(seal.Signature) != ed25519.SignatureSize || seal.Digest != DigestLocalSeal(seal) ||
		!ed25519.Verify(ed25519.PublicKey(seal.Signer.VerifyKey), seal.Digest[:], seal.Signature) {
		return ErrBootstrapCertificate
	}
	return nil
}

func identityFor(plan VoterPlan, replica ReplicaID) (VoterIdentity, bool) {
	for _, identity := range plan.Request.OldVoters {
		if identity.Replica == replica {
			return identity, true
		}
	}
	return VoterIdentity{}, false
}

func validatePlanIdentities(plan VoterPlan) error {
	if len(plan.Request.OldVoters) != len(plan.Request.Base.Voters) || len(plan.Request.OldVoters) > maxBootstrapSigners {
		return ErrBootstrapCertificate
	}
	for i, voter := range plan.Request.Base.Voters {
		identity := plan.Request.OldVoters[i]
		if identity.Replica != voter || !identity.valid() || (i > 0 && plan.Request.OldVoters[i-1].Replica >= identity.Replica) {
			return ErrBootstrapCertificate
		}
	}
	return nil
}

func unionSealFrontier(base ConfState, seals []LocalSeal) (BootstrapFrontier, error) {
	frontier := BootstrapFrontier{Conf: base.ID, Lanes: make([]BootstrapLaneFrontier, len(base.Voters))}
	for i, voter := range base.Voters {
		frontier.Lanes[i].Replica = voter
	}
	for _, seal := range seals {
		if err := validateBootstrapFrontier(seal.Frontier, base); err != nil {
			return BootstrapFrontier{}, err
		}
		for i, lane := range seal.Frontier.Lanes {
			out := &frontier.Lanes[i]
			if lane.ObservedThrough > out.ObservedThrough {
				out.ObservedThrough = lane.ObservedThrough
			}
			if lane.CommittedThrough > out.CommittedThrough {
				out.CommittedThrough = lane.CommittedThrough
			}
			if lane.ExecutedThrough > out.ExecutedThrough {
				out.ExecutedThrough = lane.ExecutedThrough
			}
			out.Sparse = append(out.Sparse, lane.Sparse...)
		}
	}
	for i := range frontier.Lanes {
		sort.Slice(frontier.Lanes[i].Sparse, func(a, b int) bool { return frontier.Lanes[i].Sparse[a] < frontier.Lanes[i].Sparse[b] })
		sparse := frontier.Lanes[i].Sparse[:0]
		for _, instance := range frontier.Lanes[i].Sparse {
			if len(sparse) == 0 || sparse[len(sparse)-1] != instance {
				sparse = append(sparse, instance)
			}
		}
		frontier.Lanes[i].Sparse = sparse
	}
	if err := validateBootstrapFrontier(frontier, base); err != nil {
		return BootstrapFrontier{}, err
	}
	return frontier, nil
}

// BuildSealCertificate verifies and canonicalizes exactly one old slow quorum.
func BuildSealCertificate(plan VoterPlan, seals []LocalSeal) (SealCertificate, error) {
	if err := validateVoterPlan(plan); err != nil {
		return SealCertificate{}, err
	}
	quorum := slowQuorumSize(len(plan.Request.Base.Voters))
	if len(seals) != quorum || len(seals) > maxBootstrapSigners {
		return SealCertificate{}, ErrBootstrapCertificate
	}
	seals = append([]LocalSeal(nil), seals...)
	sort.Slice(seals, func(i, j int) bool { return seals[i].Signer.Replica < seals[j].Signer.Replica })
	for i := range seals {
		seal := seals[i]
		identity, ok := identityFor(plan, seal.Signer.Replica)
		if !ok || !voterIdentityEqual(identity, seal.Signer) || (i > 0 && seals[i-1].Signer.Replica == seal.Signer.Replica) ||
			seal.Plan != plan.Request.Plan || !confStateEqual(seal.Base, plan.Request.Base) || seal.Reservations != plan.Reservations {
			return SealCertificate{}, ErrBootstrapCertificate
		}
		if err := VerifyLocalSeal(seal); err != nil {
			return SealCertificate{}, err
		}
	}
	frontier, err := unionSealFrontier(plan.Request.Base, seals)
	if err != nil {
		return SealCertificate{}, err
	}
	certificate := SealCertificate{Plan: plan.Request.Plan, Base: plan.Request.Base.Clone(), Reservations: plan.Reservations, Frontier: frontier, Seals: seals}
	certificate.Digest = DigestSealCertificate(certificate)
	return certificate, nil
}

// VerifySealCertificate verifies exact old quorum, signer uniqueness, union, and digest.
func VerifySealCertificate(plan VoterPlan, certificate SealCertificate) error {
	canonical, err := BuildSealCertificate(plan, certificate.Seals)
	if err != nil {
		return err
	}
	if canonical.Digest != certificate.Digest || !frontierEqual(canonical.Frontier, certificate.Frontier) ||
		canonical.Plan != certificate.Plan || !confStateEqual(canonical.Base, certificate.Base) || canonical.Reservations != certificate.Reservations {
		return ErrBootstrapCertificate
	}
	return nil
}

// SignVoterAttestation signs one exact digest.
func SignVoterAttestation(digest StateDigest, signer VoterIdentity, private ed25519.PrivateKey) (VoterAttestation, error) {
	if !publicKeyMatches(signer, private) {
		return VoterAttestation{}, ErrBootstrapEligibility
	}
	return VoterAttestation{Signer: signer.Clone(), AttestedDigest: digest, Signature: ed25519.Sign(private, digest[:])}, nil
}

func verifyAttestations(plan VoterPlan, digest StateDigest, attestations []VoterAttestation, requireSource bool) error {
	if len(attestations) != slowQuorumSize(len(plan.Request.Base.Voters)) || len(attestations) > maxBootstrapSigners {
		return ErrBootstrapCertificate
	}
	seenSource := false
	last := ReplicaID(0)
	for _, attestation := range attestations {
		identity, ok := identityFor(plan, attestation.Signer.Replica)
		if !ok || !voterIdentityEqual(identity, attestation.Signer) || attestation.Signer.Replica <= last ||
			attestation.AttestedDigest != digest || len(attestation.Signature) != ed25519.SignatureSize ||
			!ed25519.Verify(ed25519.PublicKey(attestation.Signer.VerifyKey), digest[:], attestation.Signature) {
			return ErrBootstrapCertificate
		}
		last = attestation.Signer.Replica
		seenSource = seenSource || attestation.Signer.Replica == plan.Request.Source
	}
	if requireSource && !seenSource {
		return ErrBootstrapCertificate
	}
	return nil
}

// BuildSnapshotCertificate verifies and canonicalizes a descriptor quorum.
func BuildSnapshotCertificate(plan VoterPlan, descriptor SnapshotDescriptor, attestations []VoterAttestation) (SnapshotCertificate, error) {
	if err := validateSnapshotDescriptor(plan, descriptor); err != nil {
		return SnapshotCertificate{}, err
	}
	attestations = append([]VoterAttestation(nil), attestations...)
	sort.Slice(attestations, func(i, j int) bool { return attestations[i].Signer.Replica < attestations[j].Signer.Replica })
	digest := DigestSnapshotDescriptor(descriptor)
	if err := verifyAttestations(plan, digest, attestations, true); err != nil {
		return SnapshotCertificate{}, err
	}
	certificate := SnapshotCertificate{Descriptor: descriptor.Clone(), Attestations: attestations}
	certificate.Digest = DigestSnapshotCertificate(certificate)
	return certificate, nil
}

// VerifySnapshotCertificate verifies an exact old quorum snapshot certificate.
func VerifySnapshotCertificate(plan VoterPlan, certificate SnapshotCertificate) error {
	canonical, err := BuildSnapshotCertificate(plan, certificate.Descriptor, certificate.Attestations)
	if err != nil {
		return err
	}
	if canonical.Digest != certificate.Digest {
		return ErrBootstrapCertificate
	}
	return nil
}

// SignVoterReadyProof signs an exact staged-target install proof.
func SignVoterReadyProof(proof VoterReadyProof, private ed25519.PrivateKey) (VoterReadyProof, error) {
	if !publicKeyMatches(proof.Target, private) {
		return VoterReadyProof{}, ErrBootstrapEligibility
	}
	digest := DigestVoterReadyProof(proof)
	proof.Signature = ed25519.Sign(private, digest[:])
	return proof, nil
}

// VerifyVoterReadyProof verifies target identity, install binding, floor, and signature.
func VerifyVoterReadyProof(plan VoterPlan, snapshot SnapshotCertificate, proof VoterReadyProof) error {
	if proof.Cluster != plan.Request.Cluster || proof.Plan != plan.Request.Plan || !voterIdentityEqual(proof.Target, plan.Request.Target) ||
		proof.SnapshotDigest != snapshot.Digest || proof.InstalledStateDigest == (StateDigest{}) ||
		proof.InstalledStateDigest != snapshot.Descriptor.InstalledStateDigest ||
		proof.AllocatorFloor != snapshot.Descriptor.TargetAllocatorFloor ||
		proof.TOQClosedThrough != snapshot.Descriptor.TOQClosedThrough || proof.AllocatorFloor == 0 ||
		len(proof.Signature) != ed25519.SignatureSize {
		return ErrBootstrapSnapshot
	}
	digest := DigestVoterReadyProof(proof)
	if !ed25519.Verify(ed25519.PublicKey(proof.Target.VerifyKey), digest[:], proof.Signature) {
		return ErrBootstrapCertificate
	}
	return nil
}

// BuildReadyCertificate verifies and canonicalizes an exact old slow quorum.
func BuildReadyCertificate(plan VoterPlan, proof VoterReadyProof, attestations []VoterAttestation) (ReadyCertificate, error) {
	proofDigest := DigestVoterReadyProof(proof)
	if proof.Cluster != plan.Request.Cluster || proof.Plan != plan.Request.Plan ||
		!voterIdentityEqual(proof.Target, plan.Request.Target) ||
		proof.SnapshotDigest == (StateDigest{}) || proof.InstalledStateDigest == (StateDigest{}) ||
		proof.AllocatorFloor != plan.Request.TargetAllocatorFloor || proof.AllocatorFloor == 0 ||
		len(proof.Signature) != ed25519.SignatureSize ||
		!ed25519.Verify(ed25519.PublicKey(proof.Target.VerifyKey), proofDigest[:], proof.Signature) {
		return ReadyCertificate{}, ErrBootstrapCertificate
	}
	attestations = append([]VoterAttestation(nil), attestations...)
	sort.Slice(attestations, func(i, j int) bool { return attestations[i].Signer.Replica < attestations[j].Signer.Replica })
	if err := verifyAttestations(plan, proofDigest, attestations, false); err != nil {
		return ReadyCertificate{}, err
	}
	certificate := ReadyCertificate{Plan: plan.Request.Plan, Proof: proof.Clone(), Attestations: attestations}
	certificate.Digest = DigestReadyCertificate(certificate)
	return certificate, nil
}

// VerifyReadyCertificate verifies an exact old quorum ready certificate.
func VerifyReadyCertificate(plan VoterPlan, snapshot SnapshotCertificate, certificate ReadyCertificate) error {
	if certificate.Plan != plan.Request.Plan || VerifyVoterReadyProof(plan, snapshot, certificate.Proof) != nil {
		return ErrBootstrapCertificate
	}
	canonical, err := BuildReadyCertificate(plan, certificate.Proof, certificate.Attestations)
	if err != nil || canonical.Digest != certificate.Digest {
		return ErrBootstrapCertificate
	}
	return nil
}

func validateSnapshotDescriptor(plan VoterPlan, descriptor SnapshotDescriptor) error {
	if descriptor.Cluster != plan.Request.Cluster || descriptor.Plan != plan.Request.Plan ||
		!confStateEqual(descriptor.Base, plan.Request.Base) || !confStateEqual(descriptor.Successor, plan.Successor) ||
		!voterIdentityEqual(descriptor.Target, plan.Request.Target) || descriptor.Source != plan.Request.Source ||
		descriptor.Reservations != plan.Reservations || descriptor.TargetAllocatorFloor != plan.Request.TargetAllocatorFloor ||
		descriptor.ReleaseDigest != plan.Request.ReleaseDigest || descriptor.TimingDigest != plan.Request.TimingDigest ||
		descriptor.SealDigest == (StateDigest{}) || descriptor.ManifestDigest == (StateDigest{}) ||
		descriptor.DeltaRoot == (StateDigest{}) || descriptor.ApplicationDigest == (StateDigest{}) ||
		descriptor.IdempotencyDigest == (StateDigest{}) || descriptor.ConfigHistoryDigest == (StateDigest{}) ||
		descriptor.InstanceDigest == (StateDigest{}) || descriptor.HardStateDigest == (StateDigest{}) ||
		descriptor.CompactionDigest == (StateDigest{}) || descriptor.InstalledStateDigest == (StateDigest{}) ||
		(descriptor.DeltaFirst == 0) != (descriptor.DeltaLast == 0) ||
		descriptor.DeltaLast < descriptor.DeltaFirst || descriptor.TOQClosedThrough != plan.Request.TOQClosedThrough {
		return ErrBootstrapSnapshot
	}
	if err := validateBootstrapFrontier(descriptor.Frontier, plan.Request.Base); err != nil {
		return err
	}
	if plan.Request.TOQ {
		expectedRuntime := domainDigest("epaxos/bootstrap/toq-runtime/v1", appendTOQRuntime(nil, plan.Request.SuccessorTOQ))
		if descriptor.TOQRuntimeDigest != expectedRuntime {
			return ErrBootstrapSnapshot
		}
	} else if descriptor.TOQRuntimeDigest != (StateDigest{}) {
		return ErrBootstrapSnapshot
	}
	return nil
}

func voterIdentityEqual(a, b VoterIdentity) bool {
	return a.Replica == b.Replica && a.Incarnation == b.Incarnation && bytes.Equal(a.VerifyKey, b.VerifyKey)
}

func confStateEqual(a, b ConfState) bool { return a.ID == b.ID && sameReplicaIDs(a.Voters, b.Voters) }

func frontierEqual(a, b BootstrapFrontier) bool {
	if a.Conf != b.Conf || len(a.Lanes) != len(b.Lanes) {
		return false
	}
	for i := range a.Lanes {
		x, y := a.Lanes[i], b.Lanes[i]
		if x.Replica != y.Replica || x.ObservedThrough != y.ObservedThrough || x.CommittedThrough != y.CommittedThrough ||
			x.ExecutedThrough != y.ExecutedThrough || x.CompactedExecutedThrough != y.CompactedExecutedThrough || !instanceNumsEqual(x.Sparse, y.Sparse) {
			return false
		}
	}
	return true
}

// DigestBootstrapRecord returns the canonical durable phase-record digest.
func DigestBootstrapRecord(record BootstrapRecord) StateDigest {
	canonical := appendVoterPlan(nil, record.Plan)
	canonical = append(canonical, byte(record.Phase), byte(record.Outcome))
	canonical = appendFixed(canonical, [32]byte(record.LocalSeal.Digest))
	canonical = appendFixed(canonical, [32]byte(record.SealCertificate.Digest))
	canonical = appendFixed(canonical, [32]byte(record.SnapshotCertificate.Digest))
	canonical = appendFixed(canonical, [32]byte(DigestVoterReadyProof(record.TargetReady.Proof)))
	canonical = appendFixed(canonical, [32]byte(record.ReadyCertificate.Digest))
	canonical = appendRef(canonical, record.TerminalRef)
	canonical = appendConf(canonical, record.Closed.Conf)
	canonical = appendFrontier(canonical, record.Closed.Frontier)
	canonical = appendReservations(canonical, record.Closed.Reservations)
	canonical = appendFixed(canonical, [32]byte(record.Closed.CertificateDigest))
	return domainDigest("epaxos/bootstrap/record/v1", canonical)
}

func validateVoterPlan(plan VoterPlan) error {
	request := plan.Request
	baseQuorum, baseErr := newQuorum(request.Base.Voters)
	successorQuorum, successorErr := newQuorum(plan.Successor.Voters)
	if request.Cluster == (ClusterID{}) || request.Plan == (BootstrapID{}) || request.Base.ID == 0 ||
		request.Base.ID == ^ConfID(0) || len(request.Base.Voters) == 0 || len(request.Base.Voters) >= 7 ||
		baseErr != nil || !sameReplicaIDs(baseQuorum.conf.Voters, request.Base.Voters) ||
		successorErr != nil || !sameReplicaIDs(successorQuorum.conf.Voters, plan.Successor.Voters) ||
		!request.Target.valid() || request.Base.Contains(request.Target.Replica) || !request.Base.Contains(request.Source) ||
		request.SourceDigest == (StateDigest{}) || request.ReleaseDigest == (StateDigest{}) ||
		request.TimingDigest == (StateDigest{}) ||
		request.TargetAllocatorFloor == 0 || request.TargetAllocatorFloor == ^InstanceNum(0) ||
		!plan.Reservations.ValidFor(request.Base) || plan.RequestDigest != DigestVoterPlan(plan) ||
		plan.Successor.ID != request.Base.ID+1 || len(plan.Successor.Voters) != len(request.Base.Voters)+1 {
		return ErrBootstrapCertificate
	}
	if err := validatePlanIdentities(plan); err != nil {
		return err
	}
	voters := append([]ReplicaID(nil), request.Base.Voters...)
	voters = append(voters, request.Target.Replica)
	sort.Slice(voters, func(i, j int) bool { return voters[i] < voters[j] })
	if !sameReplicaIDs(voters, plan.Successor.Voters) {
		return ErrBootstrapCertificate
	}
	if request.TOQ {
		if !confStateEqual(request.SuccessorTOQ.Conf, plan.Successor) {
			return ErrBootstrapCertificate
		}
		if _, err := normalizeTOQRuntime(request.SuccessorTOQ, plan.Reservations.Prepare.Replica); err != nil {
			return ErrBootstrapCertificate
		}
	} else if !confStateIsZero(request.SuccessorTOQ.Conf) || len(request.SuccessorTOQ.OneWayDelay) != 0 ||
		len(request.SuccessorTOQ.SyncGroup) != 0 || request.TOQClosedThrough != 0 {
		return ErrBootstrapCertificate
	}
	return nil
}

func validateBootstrapRecord(record BootstrapRecord) error {
	if err := validateVoterPlan(record.Plan); err != nil {
		return err
	}
	if record.Phase < BootstrapPhasePreparing || record.Phase > BootstrapPhaseAborted ||
		record.Outcome > BootstrapOutcomeRejectedInvalid || record.Digest != DigestBootstrapRecord(record) {
		return fmt.Errorf("%w: malformed bootstrap record", ErrInvalidConfig)
	}
	if record.LocalSeal.Digest != (StateDigest{}) {
		identity, ok := identityFor(record.Plan, record.LocalSeal.Signer.Replica)
		if !ok || !voterIdentityEqual(identity, record.LocalSeal.Signer) ||
			record.LocalSeal.Plan != record.Plan.Request.Plan ||
			!confStateEqual(record.LocalSeal.Base, record.Plan.Request.Base) ||
			record.LocalSeal.Reservations != record.Plan.Reservations ||
			VerifyLocalSeal(record.LocalSeal) != nil ||
			validateBootstrapFrontier(record.LocalSeal.Frontier, record.Plan.Request.Base) != nil {
			return ErrBootstrapCertificate
		}
	}
	if record.SealCertificate.Digest != (StateDigest{}) {
		if err := VerifySealCertificate(record.Plan, record.SealCertificate); err != nil {
			return err
		}
	}
	if record.SnapshotCertificate.Digest != (StateDigest{}) {
		if err := VerifySnapshotCertificate(record.Plan, record.SnapshotCertificate); err != nil {
			return err
		}
		if record.SealCertificate.Digest != (StateDigest{}) &&
			record.SnapshotCertificate.Descriptor.SealDigest != record.SealCertificate.Digest {
			return ErrBootstrapCertificate
		}
	}
	if len(record.TargetReady.Proof.Signature) != 0 {
		if record.TargetReady.Plan != record.Plan.Request.Plan ||
			record.SnapshotCertificate.Digest == (StateDigest{}) ||
			VerifyVoterReadyProof(record.Plan, record.SnapshotCertificate, record.TargetReady.Proof) != nil {
			return ErrBootstrapCertificate
		}
	} else if record.TargetReady.Plan != (BootstrapID{}) {
		return ErrBootstrapCertificate
	}
	if record.ReadyCertificate.Digest != (StateDigest{}) {
		if record.SnapshotCertificate.Digest == (StateDigest{}) ||
			VerifyReadyCertificate(record.Plan, record.SnapshotCertificate, record.ReadyCertificate) != nil ||
			(len(record.TargetReady.Proof.Signature) != 0 &&
				DigestVoterReadyProof(record.TargetReady.Proof) != DigestVoterReadyProof(record.ReadyCertificate.Proof)) {
			return ErrBootstrapCertificate
		}
	}
	if !confStateIsZero(record.Closed.Conf) {
		if !confStateEqual(record.Closed.Conf, record.Plan.Request.Base) ||
			record.Closed.Reservations != record.Plan.Reservations ||
			validateBootstrapFrontier(record.Closed.Frontier, record.Plan.Request.Base) != nil {
			return ErrBootstrapCertificate
		}
		if record.Closed.CertificateDigest != (StateDigest{}) {
			if record.SealCertificate.Digest != (StateDigest{}) &&
				record.Closed.CertificateDigest != record.SealCertificate.Digest {
				return ErrBootstrapCertificate
			}
			if record.SnapshotCertificate.Digest != (StateDigest{}) &&
				record.Closed.CertificateDigest != record.SnapshotCertificate.Descriptor.SealDigest {
				return ErrBootstrapCertificate
			}
		}
	} else if !frontierEqual(record.Closed.Frontier, BootstrapFrontier{}) ||
		record.Closed.Reservations != (ControlReservations{}) ||
		record.Closed.CertificateDigest != (StateDigest{}) {
		return ErrBootstrapCertificate
	}
	if record.Phase == BootstrapPhaseLocalSealed && record.LocalSeal.Digest == (StateDigest{}) {
		return ErrBootstrapCertificate
	}
	if record.Phase >= BootstrapPhaseSealed && record.Phase <= BootstrapPhaseTargetReady &&
		record.SealCertificate.Digest == (StateDigest{}) {
		return ErrBootstrapCertificate
	}
	if record.Phase >= BootstrapPhaseCertified && record.Phase <= BootstrapPhaseTargetReady &&
		record.SnapshotCertificate.Digest == (StateDigest{}) {
		return ErrBootstrapCertificate
	}
	if record.Phase == BootstrapPhaseTargetReady && len(record.TargetReady.Proof.Signature) == 0 {
		return ErrBootstrapCertificate
	}
	switch record.Phase {
	case BootstrapPhaseActivated:
		if record.Outcome != BootstrapOutcomeActivated || record.TerminalRef != record.Plan.Reservations.Activate ||
			record.ReadyCertificate.Digest == (StateDigest{}) ||
			!confStateEqual(record.Closed.Conf, record.Plan.Request.Base) ||
			!frontierEqual(record.Closed.Frontier, record.SnapshotCertificate.Descriptor.Frontier) ||
			record.Closed.CertificateDigest != record.SnapshotCertificate.Descriptor.SealDigest {
			return ErrBootstrapControl
		}
	case BootstrapPhaseAborted:
		if record.Outcome != BootstrapOutcomeAborted || record.TerminalRef != record.Plan.Reservations.Abort {
			return ErrBootstrapControl
		}
	default:
		if record.Outcome != BootstrapOutcomeUnspecified || !record.TerminalRef.IsZero() {
			return ErrBootstrapControl
		}
	}
	return nil
}

func voterPlanEqual(a, b VoterPlan) bool {
	return a.RequestDigest == b.RequestDigest && a.Reservations == b.Reservations &&
		confStateEqual(a.Successor, b.Successor) && DigestVoterPlan(a) == DigestVoterPlan(b)
}

func bootstrapRecordEqual(a, b BootstrapRecord) bool {
	return a.Digest == b.Digest && DigestBootstrapRecord(a) == DigestBootstrapRecord(b)
}

func membershipResultEqual(a, b MembershipResult) bool {
	return a.Plan == b.Plan && a.Outcome == b.Outcome && a.ExitRef == b.ExitRef &&
		a.CertificateDigest == b.CertificateDigest && confStateEqual(a.Successor, b.Successor)
}

func validateMembershipResult(record InstanceRecord) error {
	result := record.MembershipResult
	if record.Command.Kind != CommandMembership || record.Status != StatusExecuted {
		if !membershipResultAbsent(record) {
			return fmt.Errorf("%w: membership result outside executed membership command", ErrInvalidConfig)
		}
		return nil
	}
	if result.Outcome == BootstrapOutcomeUnspecified {
		if !membershipResultAbsent(record) {
			return fmt.Errorf("%w: partial membership result", ErrInvalidConfig)
		}
		return nil
	}
	if result.Plan == (BootstrapID{}) || result.ExitRef != record.Ref {
		return fmt.Errorf("%w: membership result exit mismatch", ErrInvalidConfig)
	}
	switch result.Outcome {
	case BootstrapOutcomeActivated:
		if result.CertificateDigest == (StateDigest{}) || confStateIsZero(result.Successor) ||
			record.ConfChangeResult.Outcome != ConfChangeApplied ||
			!confStateEqual(record.ConfChangeResult.Conf, result.Successor) {
			return fmt.Errorf("%w: invalid activated membership result", ErrInvalidConfig)
		}
	case BootstrapOutcomeAborted, BootstrapOutcomeRejectedSuperseded, BootstrapOutcomeRejectedInvalid:
		if !confStateIsZero(result.Successor) || record.ConfChangeResult.Outcome == ConfChangeApplied {
			return fmt.Errorf("%w: rejected membership result has successor", ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("%w: unknown membership outcome", ErrInvalidConfig)
	}
	return nil
}

func bootstrapMessageSigningBytes(message BootstrapMessage) ([]byte, error) {
	if message.Type < BootstrapMsgSealRequest || message.Type > BootstrapMsgActivationNotice ||
		message.Cluster == (ClusterID{}) || message.Plan == (BootstrapID{}) ||
		message.From == 0 || message.To == 0 || message.FromIncarnation == 0 ||
		message.BaseID == 0 || message.BaseDigest == (StateDigest{}) || len(message.Payload) > maxBootstrapPayload {
		return nil, ErrInvalidBootstrapMessage
	}
	digest := domainDigest("epaxos/bootstrap/payload/v1", message.Payload)
	if message.PayloadDigest != (StateDigest{}) && message.PayloadDigest != digest {
		return nil, ErrInvalidBootstrapMessage
	}
	message.PayloadDigest = digest
	out := append([]byte(nil), bootstrapWireMagic[:]...)
	out = binary.AppendUvarint(out, bootstrapWireVersion)
	out = binary.AppendUvarint(out, uint64(message.Type))
	out = appendFixed(out, [32]byte(message.Cluster))
	out = appendFixed(out, [32]byte(message.Plan))
	out = binary.AppendUvarint(out, uint64(message.From))
	out = binary.AppendUvarint(out, message.FromIncarnation)
	out = binary.AppendUvarint(out, uint64(message.To))
	out = binary.AppendUvarint(out, uint64(message.BaseID))
	out = appendFixed(out, [32]byte(message.BaseDigest))
	out = binary.AppendUvarint(out, uint64(len(message.Payload)))
	out = append(out, message.Payload...)
	out = appendFixed(out, [32]byte(message.PayloadDigest))
	return out, nil
}

// SignBootstrapMessage authenticates an out-of-band bootstrap envelope.
func SignBootstrapMessage(message BootstrapMessage, signer VoterIdentity, private ed25519.PrivateKey) (BootstrapMessage, error) {
	if message.From != signer.Replica || message.FromIncarnation != signer.Incarnation || !publicKeyMatches(signer, private) {
		return BootstrapMessage{}, ErrBootstrapEligibility
	}
	canonical, err := bootstrapMessageSigningBytes(message)
	if err != nil {
		return BootstrapMessage{}, err
	}
	message.PayloadDigest = domainDigest("epaxos/bootstrap/payload/v1", message.Payload)
	message.Signature = ed25519.Sign(private, canonical)
	return message, nil
}

// VerifyBootstrapMessage verifies an envelope against its exact sender identity.
func VerifyBootstrapMessage(message BootstrapMessage, signer VoterIdentity) error {
	if !signer.valid() || message.From != signer.Replica || message.FromIncarnation != signer.Incarnation ||
		len(message.Signature) != ed25519.SignatureSize {
		return ErrInvalidBootstrapMessage
	}
	canonical, err := bootstrapMessageSigningBytes(message)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(signer.VerifyKey), canonical, message.Signature) {
		return ErrInvalidBootstrapMessage
	}
	return nil
}

// EncodeBootstrapMessage appends the strict canonical envelope.
func EncodeBootstrapMessage(dst []byte, message BootstrapMessage) ([]byte, error) {
	canonical, err := bootstrapMessageSigningBytes(message)
	if err != nil || len(message.Signature) != ed25519.SignatureSize {
		return dst, ErrInvalidBootstrapMessage
	}
	dst = append(dst, canonical...)
	dst = append(dst, message.Signature...)
	return dst, nil
}

// DecodeBootstrapMessage decodes one complete strict envelope.
func DecodeBootstrapMessage(src []byte, message *BootstrapMessage) error {
	*message = BootstrapMessage{}
	if len(src) < len(bootstrapWireMagic)+ed25519.SignatureSize || !bytes.Equal(src[:len(bootstrapWireMagic)], bootstrapWireMagic[:]) {
		return ErrInvalidBootstrapMessage
	}
	p := bootstrapParser{b: src[len(bootstrapWireMagic):]}
	if p.uvarint() != bootstrapWireVersion {
		return ErrInvalidBootstrapMessage
	}
	message.Type = BootstrapMessageType(p.uvarint())
	p.fixed((*[32]byte)(&message.Cluster))
	p.fixed((*[32]byte)(&message.Plan))
	message.From = ReplicaID(p.uvarint())
	message.FromIncarnation = p.uvarint()
	message.To = ReplicaID(p.uvarint())
	message.BaseID = ConfID(p.uvarint())
	p.fixed((*[32]byte)(&message.BaseDigest))
	message.Payload = p.bytes(maxBootstrapPayload)
	p.fixed((*[32]byte)(&message.PayloadDigest))
	if p.err || len(p.b) != ed25519.SignatureSize {
		*message = BootstrapMessage{}
		return ErrInvalidBootstrapMessage
	}
	message.Signature = append([]byte(nil), p.b...)
	if _, err := bootstrapMessageSigningBytes(*message); err != nil {
		*message = BootstrapMessage{}
		return err
	}
	return nil
}

func bootstrapChunkSigningBytes(chunk BootstrapChunk) ([]byte, error) {
	if chunk.Cluster == (ClusterID{}) || chunk.Plan == (BootstrapID{}) || chunk.Manifest == (StateDigest{}) ||
		chunk.From == 0 || chunk.To == 0 || chunk.FromIncarnation == 0 || chunk.Index >= maxBootstrapChunks ||
		len(chunk.Payload) == 0 || len(chunk.Payload) > maxBootstrapChunk || chunk.Total == 0 ||
		chunk.Total > maxBootstrapTransfer || chunk.Offset > chunk.Total ||
		uint64(len(chunk.Payload)) > chunk.Total-chunk.Offset {
		return nil, ErrBootstrapBounds
	}
	digest := domainDigest("epaxos/bootstrap/chunk-payload/v1", chunk.Payload)
	if chunk.PayloadDigest != (StateDigest{}) && chunk.PayloadDigest != digest {
		return nil, ErrBootstrapChunkConflict
	}
	chunk.PayloadDigest = digest
	out := append([]byte(nil), bootstrapChunkMagic[:]...)
	out = binary.AppendUvarint(out, bootstrapWireVersion)
	out = appendFixed(out, [32]byte(chunk.Cluster))
	out = appendFixed(out, [32]byte(chunk.Plan))
	out = binary.AppendUvarint(out, uint64(chunk.From))
	out = binary.AppendUvarint(out, chunk.FromIncarnation)
	out = binary.AppendUvarint(out, uint64(chunk.To))
	out = appendFixed(out, [32]byte(chunk.Manifest))
	out = binary.AppendUvarint(out, chunk.Index)
	out = binary.AppendUvarint(out, chunk.Offset)
	out = binary.AppendUvarint(out, chunk.Total)
	out = binary.AppendUvarint(out, uint64(len(chunk.Payload)))
	out = append(out, chunk.Payload...)
	out = appendFixed(out, [32]byte(chunk.PayloadDigest))
	return out, nil
}

// SignBootstrapChunk authenticates one chunk frame.
func SignBootstrapChunk(chunk BootstrapChunk, signer VoterIdentity, private ed25519.PrivateKey) (BootstrapChunk, error) {
	if chunk.From != signer.Replica || chunk.FromIncarnation != signer.Incarnation || !publicKeyMatches(signer, private) {
		return BootstrapChunk{}, ErrBootstrapEligibility
	}
	canonical, err := bootstrapChunkSigningBytes(chunk)
	if err != nil {
		return BootstrapChunk{}, err
	}
	chunk.PayloadDigest = domainDigest("epaxos/bootstrap/chunk-payload/v1", chunk.Payload)
	chunk.Signature = ed25519.Sign(private, canonical)
	return chunk, nil
}

// VerifyBootstrapChunk verifies one complete chunk frame.
func VerifyBootstrapChunk(chunk BootstrapChunk, signer VoterIdentity) error {
	if !signer.valid() || chunk.From != signer.Replica || chunk.FromIncarnation != signer.Incarnation ||
		len(chunk.Signature) != ed25519.SignatureSize {
		return ErrInvalidBootstrapMessage
	}
	canonical, err := bootstrapChunkSigningBytes(chunk)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(signer.VerifyKey), canonical, chunk.Signature) {
		return ErrInvalidBootstrapMessage
	}
	return nil
}

// EncodeBootstrapChunk appends one strict chunk frame.
func EncodeBootstrapChunk(dst []byte, chunk BootstrapChunk) ([]byte, error) {
	canonical, err := bootstrapChunkSigningBytes(chunk)
	if err != nil || len(chunk.Signature) != ed25519.SignatureSize {
		return dst, ErrInvalidBootstrapMessage
	}
	dst = append(dst, canonical...)
	dst = append(dst, chunk.Signature...)
	return dst, nil
}

// DecodeBootstrapChunk decodes one complete strict chunk frame.
func DecodeBootstrapChunk(src []byte, chunk *BootstrapChunk) error {
	*chunk = BootstrapChunk{}
	if len(src) < len(bootstrapChunkMagic)+ed25519.SignatureSize || !bytes.Equal(src[:len(bootstrapChunkMagic)], bootstrapChunkMagic[:]) {
		return ErrInvalidBootstrapMessage
	}
	p := bootstrapParser{b: src[len(bootstrapChunkMagic):]}
	if p.uvarint() != bootstrapWireVersion {
		return ErrInvalidBootstrapMessage
	}
	p.fixed((*[32]byte)(&chunk.Cluster))
	p.fixed((*[32]byte)(&chunk.Plan))
	chunk.From = ReplicaID(p.uvarint())
	chunk.FromIncarnation = p.uvarint()
	chunk.To = ReplicaID(p.uvarint())
	p.fixed((*[32]byte)(&chunk.Manifest))
	chunk.Index = p.uvarint()
	chunk.Offset = p.uvarint()
	chunk.Total = p.uvarint()
	chunk.Payload = p.bytes(maxBootstrapChunk)
	p.fixed((*[32]byte)(&chunk.PayloadDigest))
	if p.err || len(p.b) != ed25519.SignatureSize {
		*chunk = BootstrapChunk{}
		return ErrInvalidBootstrapMessage
	}
	chunk.Signature = append([]byte(nil), p.b...)
	if _, err := bootstrapChunkSigningBytes(*chunk); err != nil {
		*chunk = BootstrapChunk{}
		return err
	}
	return nil
}

type bootstrapParser struct {
	b   []byte
	err bool
}

func (p *bootstrapParser) uvarint() uint64 {
	if p.err {
		return 0
	}
	value, n := binary.Uvarint(p.b)
	if n <= 0 {
		p.err = true
		return 0
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], value) != n || !bytes.Equal(p.b[:n], canonical[:n]) {
		p.err = true
		return 0
	}
	p.b = p.b[n:]
	return value
}

func (p *bootstrapParser) fixed(dst *[32]byte) {
	if p.err || len(p.b) < len(dst) {
		p.err = true
		return
	}
	copy(dst[:], p.b[:len(dst)])
	p.b = p.b[len(dst):]
}

func (p *bootstrapParser) bytes(max int) []byte {
	length := p.uvarint()
	if p.err || length > uint64(max) || length > uint64(len(p.b)) {
		p.err = true
		return nil
	}
	out := append([]byte(nil), p.b[:length]...)
	p.b = p.b[length:]
	return out
}

type bootstrapState struct {
	record         BootstrapRecord
	sealAcks       map[ReplicaID]LocalSeal
	durablePhase   BootstrapPhase
	durableDigest  StateDigest
	snapshotDigest StateDigest
	readyDigest    StateDigest
	snapshotVotes  map[ReplicaID]VoterAttestation
	readyVotes     map[ReplicaID]VoterAttestation
}

type bootstrapDurabilityAction struct {
	plan         BootstrapID
	recordDigest StateDigest
	message      BootstrapMessage
	unseal       bool
}

type membershipOperation uint8

const (
	membershipPrepare membershipOperation = iota + 1
	membershipActivate
	membershipAbort
)

type membershipCommandWire struct {
	Version    uint8
	Operation  membershipOperation
	Plan       VoterPlan
	Snapshot   SnapshotCertificate
	Ready      ReadyCertificate
	SealDigest StateDigest
}

type snapshotVotePayload struct {
	Descriptor  SnapshotDescriptor
	Attestation VoterAttestation
}

type installProofPayload struct {
	Snapshot SnapshotCertificate
	Proof    VoterReadyProof
}

func marshalBootstrapCanonical(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maxBootstrapPayload {
		return nil, ErrBootstrapBounds
	}
	return encoded, nil
}

func unmarshalBootstrapCanonical(encoded []byte, value any) error {
	if len(encoded) == 0 || len(encoded) > maxBootstrapPayload {
		return ErrBootstrapBounds
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return ErrInvalidBootstrapMessage
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ErrInvalidBootstrapMessage
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return ErrInvalidBootstrapMessage
	}
	return nil
}

func encodeMembershipCommand(wire membershipCommandWire) (Command, error) {
	wire.Version = bootstrapWireVersion
	payload, err := marshalBootstrapCanonical(wire)
	if err != nil {
		return Command{}, err
	}
	return Command{Kind: CommandMembership, Payload: payload, ConflictKeys: [][]byte{[]byte("\xffmembership")}}, nil
}

func decodeMembershipCommand(command Command) (membershipCommandWire, error) {
	if command.Kind != CommandMembership || command.ID != (CommandID{}) || len(command.ConflictKeys) != 1 ||
		!bytes.Equal(command.ConflictKeys[0], []byte("\xffmembership")) {
		return membershipCommandWire{}, ErrBootstrapControl
	}
	var wire membershipCommandWire
	if err := unmarshalBootstrapCanonical(command.Payload, &wire); err != nil || wire.Version != bootstrapWireVersion ||
		wire.Operation < membershipPrepare || wire.Operation > membershipAbort {
		return membershipCommandWire{}, ErrBootstrapControl
	}
	if err := validateVoterPlan(wire.Plan); err != nil {
		return membershipCommandWire{}, err
	}
	switch wire.Operation {
	case membershipPrepare:
		if wire.Snapshot.Digest != (StateDigest{}) || wire.Ready.Digest != (StateDigest{}) || wire.SealDigest != (StateDigest{}) {
			return membershipCommandWire{}, ErrBootstrapControl
		}
	case membershipActivate:
		if wire.Snapshot.Digest == (StateDigest{}) || wire.Ready.Digest == (StateDigest{}) || wire.SealDigest == (StateDigest{}) {
			return membershipCommandWire{}, ErrBootstrapControl
		}
	case membershipAbort:
		if wire.Snapshot.Digest != (StateDigest{}) || wire.Ready.Digest != (StateDigest{}) {
			return membershipCommandWire{}, ErrBootstrapControl
		}
	}
	return wire, nil
}

func membershipCommandValidForRef(command Command, ref InstanceRef, conf ConfState) bool {
	wire, err := decodeMembershipCommand(command)
	if err != nil || !confStateEqual(wire.Plan.Request.Base, conf) {
		return false
	}
	switch wire.Operation {
	case membershipPrepare:
		return ref == wire.Plan.Reservations.Prepare
	case membershipActivate:
		return ref == wire.Plan.Reservations.Activate
	case membershipAbort:
		return ref == wire.Plan.Reservations.Abort
	default:
		return false
	}
}

func (n *RawNode) restoreBootstrapRecord(record BootstrapRecord) error {
	if err := validateBootstrapRecord(record); err != nil {
		return err
	}
	id := record.Plan.Request.Plan
	if existing := n.bootstrapPlans[id]; existing != nil {
		if !bootstrapRecordEqual(existing.record, record) {
			return fmt.Errorf("%w: conflicting bootstrap replay", ErrInvalidConfig)
		}
		return nil
	}
	if record.Phase != BootstrapPhaseAborted {
		if other, exists := n.bootstrapByBase[record.Plan.Request.Base.ID]; exists && other != id {
			return fmt.Errorf("%w: multiple bootstrap plans for one base", ErrInvalidConfig)
		}
	}
	state := &bootstrapState{
		record:        record.Clone(),
		sealAcks:      make(map[ReplicaID]LocalSeal),
		durablePhase:  record.Phase,
		durableDigest: record.Digest,
		snapshotVotes: make(map[ReplicaID]VoterAttestation),
		readyVotes:    make(map[ReplicaID]VoterAttestation),
	}
	if record.SnapshotCertificate.Digest != (StateDigest{}) {
		state.snapshotDigest = DigestSnapshotDescriptor(record.SnapshotCertificate.Descriptor)
	}
	if record.ReadyCertificate.Digest != (StateDigest{}) {
		state.readyDigest = DigestVoterReadyProof(record.ReadyCertificate.Proof)
	}
	n.bootstrapPlans[id] = state
	if record.Phase != BootstrapPhaseAborted {
		n.bootstrapByBase[record.Plan.Request.Base.ID] = id
	}
	if record.Phase >= BootstrapPhaseLocalSealed && record.Phase != BootstrapPhaseAborted {
		closed := record.Closed.Clone()
		if confStateIsZero(closed.Conf) {
			closed = ClosedConfig{
				Conf:         record.Plan.Request.Base.Clone(),
				Frontier:     record.LocalSeal.Frontier.Clone(),
				Reservations: record.Plan.Reservations,
			}
		}
		if record.SealCertificate.Digest != (StateDigest{}) {
			closed.Frontier = record.SealCertificate.Frontier.Clone()
			closed.CertificateDigest = record.SealCertificate.Digest
		}
		n.closedConfigs[closed.Conf.ID] = closed
	}
	if floor, ok := instanceSuccessor(record.Plan.Reservations.Abort.Instance); ok && floor > n.nextInstance &&
		record.Plan.Reservations.Abort.Replica == n.id {
		n.nextInstance = floor
		n.allocatorFloor = maxInstanceNum(n.allocatorFloor, floor)
	}
	return nil
}

func (n *RawNode) enqueueBootstrapRecord(state *bootstrapState) {
	state.record.Digest = DigestBootstrapRecord(state.record)
	target := n.readyTarget()
	target.BootstrapRecords = append(target.BootstrapRecords, state.record.Clone())
	target.MustSync = true
}

func (n *RawNode) enqueueFrontierUpdate(frontier BootstrapFrontier) {
	update := FrontierUpdate{
		Frontier:         frontier.Clone(),
		AllocatorFloor:   n.allocatorFloor,
		TOQClosedThrough: n.toqClosedThrough,
	}
	update.EvidenceDigest = domainDigest("epaxos/bootstrap/frontier/v1", appendFrontier(nil, update.Frontier))
	target := n.readyTarget()
	target.FrontierUpdates = append(target.FrontierUpdates, update)
	target.MustSync = true
}

func (n *RawNode) enqueueBootstrapAfterAdvance(state *bootstrapState, message BootstrapMessage, unseal bool) {
	n.bootstrapDurability = append(n.bootstrapDurability, bootstrapDurabilityAction{
		plan:         state.record.Plan.Request.Plan,
		recordDigest: state.record.Digest,
		message:      message.Clone(),
		unseal:       unseal,
	})
}

func (n *RawNode) prepareBootstrapMessage(messageType BootstrapMessageType, state *bootstrapState, to ReplicaID, payload any) (BootstrapMessage, error) {
	if !n.localIdentity.valid() || len(n.bootstrapPrivateKey) != ed25519.PrivateKeySize {
		return BootstrapMessage{}, ErrBootstrapEligibility
	}
	encoded, err := marshalBootstrapCanonical(payload)
	if err != nil {
		return BootstrapMessage{}, err
	}
	message := BootstrapMessage{
		Type:            messageType,
		Cluster:         state.record.Plan.Request.Cluster,
		Plan:            state.record.Plan.Request.Plan,
		From:            n.id,
		FromIncarnation: n.localIdentity.Incarnation,
		To:              to,
		BaseID:          state.record.Plan.Request.Base.ID,
		BaseDigest:      state.record.Plan.RequestDigest,
		Payload:         encoded,
	}
	return SignBootstrapMessage(message, n.localIdentity, ed25519.PrivateKey(n.bootstrapPrivateKey))
}

func (n *RawNode) validatePrepareRequest(request PrepareVoterRequest) (VoterPlan, error) {
	if request.Cluster == (ClusterID{}) || request.Cluster != n.cluster || request.Plan == (BootstrapID{}) ||
		!confStateEqual(request.Base, n.q.conf) || n.q.conf.ID == ^ConfID(0) || len(n.q.conf.Voters) >= 7 ||
		request.Target.Replica == 0 || n.q.conf.Contains(request.Target.Replica) ||
		request.TargetAllocatorFloor == 0 || request.TargetAllocatorFloor == ^InstanceNum(0) {
		return VoterPlan{}, ErrBootstrapStale
	}
	if previous, historical := n.voterIdentities[request.Target.Replica]; historical {
		if request.Target.Incarnation <= previous.Incarnation {
			return VoterPlan{}, ErrBootstrapEligibility
		}
		var highest InstanceNum
		for ref := range n.instances {
			if ref.Replica == request.Target.Replica && ref.Instance > highest {
				highest = ref.Instance
			}
		}
		floor, ok := instanceSuccessor(highest)
		if !ok || request.TargetAllocatorFloor < floor {
			return VoterPlan{}, ErrInstanceExhausted
		}
	}
	if len(request.OldVoters) == 0 {
		request.OldVoters = make([]VoterIdentity, 0, len(request.Base.Voters))
		for _, voter := range request.Base.Voters {
			identity, ok := n.voterIdentities[voter]
			if !ok {
				return VoterPlan{}, ErrBootstrapEligibility
			}
			request.OldVoters = append(request.OldVoters, identity.Clone())
		}
	}
	if request.Source == 0 {
		request.Source = n.id
	}
	voters := append([]ReplicaID(nil), request.Base.Voters...)
	voters = append(voters, request.Target.Replica)
	sort.Slice(voters, func(i, j int) bool { return voters[i] < voters[j] })
	plan := VoterPlan{
		Request:   request.Clone(),
		Successor: ConfState{ID: request.Base.ID + 1, Voters: voters},
	}
	if n.nextInstance == 0 || n.nextInstance > ^InstanceNum(0)-2 {
		return VoterPlan{}, ErrInstanceExhausted
	}
	plan.Reservations = ControlReservations{
		Prepare:  InstanceRef{Replica: n.id, Instance: n.nextInstance, Conf: request.Base.ID},
		Activate: InstanceRef{Replica: n.id, Instance: n.nextInstance + 1, Conf: request.Base.ID},
		Abort:    InstanceRef{Replica: n.id, Instance: n.nextInstance + 2, Conf: request.Base.ID},
	}
	if _, exists := n.instances[plan.Reservations.Prepare]; exists {
		return VoterPlan{}, ErrBootstrapControl
	}
	if _, exists := n.instances[plan.Reservations.Activate]; exists {
		return VoterPlan{}, ErrBootstrapControl
	}
	if _, exists := n.instances[plan.Reservations.Abort]; exists {
		return VoterPlan{}, ErrBootstrapControl
	}
	plan.RequestDigest = DigestVoterPlan(plan)
	if err := validateVoterPlan(plan); err != nil {
		return VoterPlan{}, err
	}
	if n.toqEnabled != request.TOQ {
		return VoterPlan{}, ErrBootstrapSnapshot
	}
	if request.TOQ {
		if n.toqActive == nil || request.TOQClosedThrough != n.toqClosedThrough {
			return VoterPlan{}, ErrBootstrapSnapshot
		}
	}
	return plan, nil
}

// PrepareVoter atomically reserves the three control refs and proposes Prepare
// without changing membership or target eligibility.
func (n *RawNode) PrepareVoter(request PrepareVoterRequest) (VoterPlan, error) {
	if err := n.requireLocalVoter(); err != nil {
		return VoterPlan{}, err
	}
	if n.cluster == (ClusterID{}) {
		return VoterPlan{}, ErrBootstrapEligibility
	}
	if existing := n.bootstrapPlans[request.Plan]; existing != nil {
		if existing.record.Phase == BootstrapPhaseAborted {
			return VoterPlan{}, ErrBootstrapAborted
		}
		candidate := existing.record.Plan.Clone()
		candidate.Request = request.Clone()
		if len(candidate.Request.OldVoters) == 0 {
			candidate.Request.OldVoters = cloneVoterIdentities(existing.record.Plan.Request.OldVoters)
		}
		if candidate.Request.Source == 0 {
			candidate.Request.Source = n.id
		}
		candidate.RequestDigest = DigestVoterPlan(candidate)
		if err := validateVoterPlan(candidate); err != nil || !voterPlanEqual(candidate, existing.record.Plan) {
			return VoterPlan{}, ErrBootstrapBusy
		}
		return existing.record.Plan.Clone(), nil
	}
	if _, busy := n.bootstrapByBase[n.q.conf.ID]; busy || n.pendingConf {
		return VoterPlan{}, ErrBootstrapBusy
	}
	if _, err := checkedLogicalDeadline(n.tick, n.retryTicks); err != nil {
		return VoterPlan{}, err
	}
	plan, err := n.validatePrepareRequest(request)
	if err != nil {
		return VoterPlan{}, err
	}
	command, err := encodeMembershipCommand(membershipCommandWire{Operation: membershipPrepare, Plan: plan})
	if err != nil {
		return VoterPlan{}, err
	}
	floor, ok := instanceSuccessor(plan.Reservations.Abort.Instance)
	if !ok {
		return VoterPlan{}, ErrInstanceExhausted
	}
	state := &bootstrapState{
		record:   BootstrapRecord{Plan: plan.Clone(), Phase: BootstrapPhasePreparing},
		sealAcks: make(map[ReplicaID]LocalSeal), snapshotVotes: make(map[ReplicaID]VoterAttestation), readyVotes: make(map[ReplicaID]VoterAttestation),
	}
	n.nextInstance = floor
	n.allocatorFloor = maxInstanceNum(n.allocatorFloor, floor)
	n.bootstrapPlans[request.Plan] = state
	n.bootstrapByBase[request.Base.ID] = request.Plan
	n.enqueueBootstrapRecord(state)
	target := n.readyTarget()
	target.AllocatorFloor = n.allocatorFloor
	target.MustSync = true
	n.beginDrive()
	defer n.endDrive()
	if err := n.proposeMembershipAt(plan.Reservations.Prepare, command); err != nil {
		delete(n.bootstrapPlans, request.Plan)
		delete(n.bootstrapByBase, request.Base.ID)
		n.nextInstance = plan.Reservations.Prepare.Instance
		n.allocatorFloor = plan.Reservations.Prepare.Instance
		return VoterPlan{}, err
	}
	return plan.Clone(), nil
}

func (n *RawNode) proposeMembershipAt(ref InstanceRef, command Command) error {
	if !membershipCommandValidForRef(command, ref, n.confFor(ref.Conf)) {
		return ErrBootstrapControl
	}
	if existing := n.instances[ref]; existing != nil {
		if commandEqual(existing.rec.Command, command) {
			return nil
		}
		return ErrBootstrapControl
	}
	attrs := n.computeAttrs(command, ref)
	record := InstanceRecord{
		Ref: ref, Ballot: Ballot{Replica: ref.Replica}, RecordBallot: Ballot{Replica: ref.Replica},
		Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: command.Clone(),
		FastPathEligible: true, TimingDomain: TimingDomainUntimed,
	}
	record.Checksum = ChecksumRecord(record)
	inst := &instance{rec: record, phase: phasePreAccept, preOK: getAttrVoteSet(), createdTick: n.tick}
	vote, ok := newAttrVote(n.confFor(ref.Conf), record.Seq, record.Deps, n.committedDepsMask(record.Attributes(), ref.Conf), true)
	if !ok || !inst.preOK.add(n.confFor(ref.Conf), n.id, vote) {
		putAttrVoteSet(inst.preOK)
		return ErrInvalidConfig
	}
	n.installInstance(inst)
	n.indexConflicts(record)
	n.enqueueRecord(record)
	if n.slowQuorumForConf(ref.Conf) == 1 {
		n.commit(inst, record.Attributes())
		return nil
	}
	n.broadcastPreAccept(inst)
	return n.schedule(inst, timerPreAccept, n.retryTicks)
}

func (n *RawNode) bootstrapLocalFrontier(base ConfState) (BootstrapFrontier, error) {
	frontier := BootstrapFrontier{Conf: base.ID, Lanes: make([]BootstrapLaneFrontier, len(base.Voters))}
	totalSparse := 0
	for i, voter := range base.Voters {
		laneKey := instanceLane{conf: base.ID, replica: voter}
		lane := BootstrapLaneFrontier{
			Replica:          voter,
			CommittedThrough: n.committedThrough[laneKey],
			ExecutedThrough:  n.executed.prefix(laneKey),
		}
		for ref := range n.instances {
			if ref.Conf != base.ID || ref.Replica != voter {
				continue
			}
			if totalSparse >= maxBootstrapSparseRefs {
				return BootstrapFrontier{}, ErrBootstrapBounds
			}
			totalSparse++
			if ref.Instance > lane.ObservedThrough {
				lane.ObservedThrough = ref.Instance
			}
			lane.Sparse = append(lane.Sparse, ref.Instance)
		}
		sort.Slice(lane.Sparse, func(i, j int) bool { return lane.Sparse[i] < lane.Sparse[j] })
		frontier.Lanes[i] = lane
	}
	if err := validateBootstrapFrontier(frontier, base); err != nil {
		return BootstrapFrontier{}, err
	}
	return frontier, nil
}

// BeginVoterSeal installs the local fence immediately and emits SealAck only
// after the resulting bootstrap Ready record is Advanced.
func (n *RawNode) BeginVoterSeal(plan VoterPlan) error {
	state := n.bootstrapPlans[plan.Request.Plan]
	if state == nil || !voterPlanEqual(state.record.Plan, plan) {
		return ErrBootstrapStale
	}
	if state.record.Phase >= BootstrapPhaseLocalSealed {
		return nil
	}
	if state.durablePhase < BootstrapPhasePrepared {
		return ErrBootstrapClosure
	}
	if !n.localIdentity.valid() || len(n.bootstrapPrivateKey) != ed25519.PrivateKeySize {
		return ErrBootstrapEligibility
	}
	frontier, err := n.bootstrapLocalFrontier(plan.Request.Base)
	if err != nil {
		return err
	}
	seal, err := SignLocalSeal(LocalSeal{
		Plan: plan.Request.Plan, Base: plan.Request.Base.Clone(), Signer: n.localIdentity.Clone(),
		Reservations: plan.Reservations, Frontier: frontier,
	}, ed25519.PrivateKey(n.bootstrapPrivateKey))
	if err != nil {
		return err
	}
	message, err := n.prepareBootstrapMessage(BootstrapMsgSealAck, state, plan.Reservations.Prepare.Replica, seal)
	if err != nil {
		return err
	}
	state.record.Phase = BootstrapPhaseLocalSealed
	state.record.LocalSeal = seal.Clone()
	state.record.Closed = ClosedConfig{Conf: plan.Request.Base.Clone(), Frontier: frontier.Clone(), Reservations: plan.Reservations}
	n.closedConfigs[plan.Request.Base.ID] = state.record.Closed.Clone()
	n.enqueueBootstrapRecord(state)
	n.enqueueFrontierUpdate(frontier)
	n.enqueueBootstrapAfterAdvance(state, message, false)
	return nil
}

func bootstrapClosureRefs(plan VoterPlan, frontier BootstrapFrontier) ([]InstanceRef, error) {
	if err := validateBootstrapFrontier(frontier, plan.Request.Base); err != nil {
		return nil, err
	}
	refs := make([]InstanceRef, 0)
	for _, lane := range frontier.Lanes {
		instance, ok := instanceSuccessor(lane.CompactedExecutedThrough)
		for ok && instance <= lane.ObservedThrough {
			ref := InstanceRef{Replica: lane.Replica, Instance: instance, Conf: plan.Request.Base.ID}
			if ref != plan.Reservations.Activate && ref != plan.Reservations.Abort {
				refs = append(refs, ref)
			}
			instance, ok = instanceSuccessor(instance)
		}
	}
	return refs, nil
}

func (n *RawNode) scheduleBootstrapClosure(plan VoterPlan, frontier BootstrapFrontier) error {
	refs, err := bootstrapClosureRefs(plan, frontier)
	if err != nil {
		return err
	}
	needsRecovery := false
	for _, ref := range refs {
		inst := n.instances[ref]
		if inst == nil || inst.rec.Status < StatusCommitted {
			needsRecovery = true
			break
		}
	}
	var deadline uint64
	if needsRecovery {
		deadline, err = checkedLogicalDeadline(n.tick, n.recoveryTicks)
		if err != nil {
			return err
		}
	}
	pending := make(map[InstanceRef]*instance)
	for _, ref := range refs {
		inst := n.instances[ref]
		if inst == nil {
			record := InstanceRecord{
				Ref: ref, Deps: n.depsForConf(ref.Conf), TimingDomain: TimingDomainUntimed,
			}
			record.Checksum = ChecksumRecord(record)
			inst = &instance{rec: record, phase: phaseIdle}
			n.installInstance(inst)
			n.enqueueRecord(record)
		}
		if inst.rec.Status >= StatusCommitted || inst.phase == phasePrepare || inst.waitDeadline > n.tick {
			continue
		}
		inst.waitDeadline = deadline
		pending[ref] = inst
	}
	if len(pending) == 0 {
		return nil
	}
	for i := range n.timers {
		timer := &n.timers[i]
		inst := pending[timer.ref]
		if timer.kind != timerPrepare || inst == nil {
			continue
		}
		*timer = makeBootstrapRecoveryTimer(deadline, inst)
		delete(pending, timer.ref)
	}
	for _, inst := range pending {
		n.timers = append(n.timers, makeBootstrapRecoveryTimer(deadline, inst))
	}
	for i := range n.timers {
		n.timers[i].index = i
	}
	heap.Init(&n.timers)
	return nil
}

func makeBootstrapRecoveryTimer(deadline uint64, inst *instance) timer {
	return timer{
		deadline: deadline, ref: inst.rec.Ref, kind: timerPrepare,
		gen: inst.generation, index: -1,
	}
}

// ApplySealCertificate verifies an exact old quorum and installs its union fence.
func (n *RawNode) ApplySealCertificate(certificate SealCertificate) error {
	state := n.bootstrapPlans[certificate.Plan]
	if state == nil {
		return ErrBootstrapStale
	}
	if err := VerifySealCertificate(state.record.Plan, certificate); err != nil {
		return err
	}
	if state.record.SealCertificate.Digest != (StateDigest{}) {
		if state.record.SealCertificate.Digest == certificate.Digest {
			return nil
		}
		return ErrBootstrapCertificate
	}
	if state.record.Phase < BootstrapPhasePrepared {
		return ErrBootstrapNotSealed
	}
	var canonicalMessages []BootstrapMessage
	if n.id == state.record.Plan.Reservations.Prepare.Replica {
		for _, voter := range state.record.Plan.Request.Base.Voters {
			if voter == n.id {
				continue
			}
			message, err := n.prepareBootstrapMessage(BootstrapMsgCanonicalSeal, state, voter, certificate)
			if err != nil {
				return err
			}
			canonicalMessages = append(canonicalMessages, message)
		}
	}
	if err := n.scheduleBootstrapClosure(state.record.Plan, certificate.Frontier); err != nil {
		return err
	}
	state.record.Phase = BootstrapPhaseSealed
	state.record.SealCertificate = certificate.Clone()
	state.record.Closed = ClosedConfig{
		Conf: state.record.Plan.Request.Base.Clone(), Frontier: certificate.Frontier.Clone(),
		Reservations: state.record.Plan.Reservations, CertificateDigest: certificate.Digest,
	}
	n.closedConfigs[certificate.Base.ID] = state.record.Closed.Clone()
	n.enqueueBootstrapRecord(state)
	n.enqueueFrontierUpdate(certificate.Frontier)
	for _, message := range canonicalMessages {
		n.enqueueBootstrapAfterAdvance(state, message, false)
	}
	return nil
}

// BootstrapClosure reports exact retained unresolved refs without scanning dense
// numeric prefixes.
func (n *RawNode) BootstrapClosure(plan VoterPlan) BootstrapClosureStatus {
	status := BootstrapClosureStatus{Plan: plan.Request.Plan}
	state := n.bootstrapPlans[plan.Request.Plan]
	if state == nil || !voterPlanEqual(state.record.Plan, plan) || state.record.SealCertificate.Digest == (StateDigest{}) {
		return status
	}
	refs, err := bootstrapClosureRefs(plan, state.record.SealCertificate.Frontier)
	if err != nil {
		return status
	}
	status.Retained = len(refs)
	for _, ref := range refs {
		inst := n.instances[ref]
		if inst == nil || inst.rec.Status != StatusExecuted {
			status.Missing = append(status.Missing, ref)
		}
	}
	status.Complete = len(status.Missing) == 0
	return status
}

// RecordTargetReady durably records a verified target proof before ReadyAck.
func (n *RawNode) RecordTargetReady(plan VoterPlan, proof VoterReadyProof) error {
	state := n.bootstrapPlans[plan.Request.Plan]
	if state == nil || !voterPlanEqual(state.record.Plan, plan) {
		return ErrBootstrapStale
	}
	if state.record.SnapshotCertificate.Digest == (StateDigest{}) {
		return ErrBootstrapSnapshot
	}
	if err := VerifyVoterReadyProof(plan, state.record.SnapshotCertificate, proof); err != nil {
		return err
	}
	if state.record.TargetReady.Proof.Signature != nil {
		if DigestVoterReadyProof(state.record.TargetReady.Proof) == DigestVoterReadyProof(proof) {
			return nil
		}
		return ErrBootstrapSnapshot
	}
	attestation, err := SignVoterAttestation(DigestVoterReadyProof(proof), n.localIdentity, ed25519.PrivateKey(n.bootstrapPrivateKey))
	if err != nil {
		return err
	}
	message, err := n.prepareBootstrapMessage(BootstrapMsgReadyAck, state, plan.Reservations.Prepare.Replica, attestation)
	if err != nil {
		return err
	}
	state.record.Phase = BootstrapPhaseTargetReady
	state.record.TargetReady = TargetReadyRecord{Plan: plan.Request.Plan, Proof: proof.Clone()}
	n.enqueueBootstrapRecord(state)
	n.enqueueBootstrapAfterAdvance(state, message, false)
	return nil
}

// ActivateVoter proposes the sole certified additive successor at ActivateRef.
func (n *RawNode) ActivateVoter(plan VoterPlan, snapshot SnapshotCertificate, ready ReadyCertificate) (InstanceRef, error) {
	state := n.bootstrapPlans[plan.Request.Plan]
	if state == nil || !voterPlanEqual(state.record.Plan, plan) || !confStateEqual(n.q.conf, plan.Request.Base) {
		return InstanceRef{}, ErrBootstrapStale
	}
	if state.record.Phase == BootstrapPhaseAborted {
		return InstanceRef{}, ErrBootstrapAborted
	}
	if closure := n.BootstrapClosure(plan); !closure.Complete {
		return InstanceRef{}, ErrBootstrapClosure
	}
	if err := VerifySnapshotCertificate(plan, snapshot); err != nil ||
		snapshot.Descriptor.SealDigest != state.record.SealCertificate.Digest ||
		(state.record.SnapshotCertificate.Digest != (StateDigest{}) &&
			state.record.SnapshotCertificate.Digest != snapshot.Digest) {
		return InstanceRef{}, ErrBootstrapSnapshot
	}
	if err := VerifyReadyCertificate(plan, snapshot, ready); err != nil ||
		(state.record.ReadyCertificate.Digest != (StateDigest{}) &&
			state.record.ReadyCertificate.Digest != ready.Digest) {
		return InstanceRef{}, ErrBootstrapCertificate
	}
	if plan.Reservations.Activate.Replica != n.id {
		return InstanceRef{}, ErrBootstrapControl
	}
	wire := membershipCommandWire{
		Operation: membershipActivate, Plan: plan.Clone(), Snapshot: snapshot.Clone(),
		Ready: ready.Clone(), SealDigest: state.record.SealCertificate.Digest,
	}
	command, err := encodeMembershipCommand(wire)
	if err != nil {
		return InstanceRef{}, err
	}
	state.record.Phase = BootstrapPhaseFinalizing
	state.record.SnapshotCertificate = snapshot.Clone()
	state.record.TargetReady = TargetReadyRecord{Plan: plan.Request.Plan, Proof: ready.Proof.Clone()}
	state.record.ReadyCertificate = ready.Clone()
	n.enqueueBootstrapRecord(state)
	n.beginDrive()
	defer n.endDrive()
	if err := n.proposeMembershipAt(plan.Reservations.Activate, command); err != nil {
		return InstanceRef{}, err
	}
	return plan.Reservations.Activate, nil
}

// AbortVoter proposes the sole safe pre-activation rollback at AbortRef.
func (n *RawNode) AbortVoter(plan VoterPlan) (InstanceRef, error) {
	state := n.bootstrapPlans[plan.Request.Plan]
	if state == nil || !voterPlanEqual(state.record.Plan, plan) {
		return InstanceRef{}, ErrBootstrapStale
	}
	if state.record.Phase == BootstrapPhaseActivated {
		return InstanceRef{}, ErrBootstrapControl
	}
	if state.record.Phase == BootstrapPhaseAborted {
		return plan.Reservations.Abort, nil
	}
	if plan.Reservations.Abort.Replica != n.id {
		return InstanceRef{}, ErrBootstrapControl
	}
	if state.durablePhase < BootstrapPhasePrepared {
		return InstanceRef{}, ErrBootstrapClosure
	}
	sealDigest := state.record.SealCertificate.Digest
	command, err := encodeMembershipCommand(membershipCommandWire{
		Operation: membershipAbort, Plan: plan.Clone(), SealDigest: sealDigest,
	})
	if err != nil {
		return InstanceRef{}, err
	}
	state.record.Phase = BootstrapPhaseFinalizing
	n.enqueueBootstrapRecord(state)
	n.beginDrive()
	defer n.endDrive()
	if err := n.proposeMembershipAt(plan.Reservations.Abort, command); err != nil {
		return InstanceRef{}, err
	}
	return plan.Reservations.Abort, nil
}

// RecoverVoterControl starts ordinary old-config per-instance recovery for one
// reserved exit. It never introduces a global ballot.
func (n *RawNode) RecoverVoterControl(plan VoterPlan, exit BootstrapExit) error {
	if err := n.requireLocalVoter(); err != nil {
		return err
	}
	state := n.bootstrapPlans[plan.Request.Plan]
	if state == nil || !voterPlanEqual(state.record.Plan, plan) {
		return ErrBootstrapStale
	}
	if state.durablePhase < BootstrapPhasePrepared {
		return ErrBootstrapClosure
	}
	ref := plan.Reservations.Activate
	if exit == BootstrapExitAbort {
		ref = plan.Reservations.Abort
	} else if exit != BootstrapExitActivate {
		return ErrBootstrapControl
	}
	if exit == BootstrapExitActivate && (state.record.SnapshotCertificate.Digest == (StateDigest{}) || state.record.ReadyCertificate.Digest == (StateDigest{})) {
		return ErrBootstrapSnapshot
	}
	inst := n.instances[ref]
	if inst == nil {
		record := InstanceRecord{Ref: ref, Deps: n.depsForConf(ref.Conf), TimingDomain: TimingDomainUntimed}
		record.Checksum = ChecksumRecord(record)
		inst = &instance{rec: record, phase: phaseIdle}
		n.installInstance(inst)
		n.enqueueRecord(record)
	}
	if inst.rec.Status >= StatusCommitted {
		return nil
	}
	n.beginDrive()
	defer n.endDrive()
	return n.startPrepare(inst)
}

// BootstrapStatus returns a copy-only complete bootstrap diagnostic view.
func (n *RawNode) BootstrapStatus() BootstrapStatusSnapshot {
	snapshot := BootstrapStatusSnapshot{LocalVoter: n.localVoter.Clone()}
	ids := make([]BootstrapID, 0, len(n.bootstrapPlans))
	for id := range n.bootstrapPlans {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	for _, id := range ids {
		snapshot.Plans = append(snapshot.Plans, n.bootstrapPlans[id].record.Clone())
	}
	confIDs := make([]ConfID, 0, len(n.closedConfigs))
	for id := range n.closedConfigs {
		confIDs = append(confIDs, id)
	}
	sort.Slice(confIDs, func(i, j int) bool { return confIDs[i] < confIDs[j] })
	for _, id := range confIDs {
		snapshot.Closed = append(snapshot.Closed, n.closedConfigs[id].Clone())
	}
	return snapshot
}

func (n *RawNode) bootstrapMessageIdentity(state *bootstrapState, message BootstrapMessage) (VoterIdentity, bool) {
	if message.Type == BootstrapMsgInstallProof && message.From == state.record.Plan.Request.Target.Replica {
		return state.record.Plan.Request.Target.Clone(), true
	}
	return identityFor(state.record.Plan, message.From)
}

func verifyBootstrapAttestation(plan VoterPlan, digest StateDigest, attestation VoterAttestation) error {
	identity, ok := identityFor(plan, attestation.Signer.Replica)
	if !ok || !voterIdentityEqual(identity, attestation.Signer) ||
		attestation.AttestedDigest != digest || len(attestation.Signature) != ed25519.SignatureSize ||
		!ed25519.Verify(ed25519.PublicKey(attestation.Signer.VerifyKey), digest[:], attestation.Signature) {
		return ErrBootstrapCertificate
	}
	return nil
}

// StepBootstrap applies one authenticated out-of-band bootstrap message.
func (n *RawNode) StepBootstrap(message BootstrapMessage) error {
	if message.To != n.id || message.Cluster != n.cluster {
		return ErrInvalidBootstrapMessage
	}
	state := n.bootstrapPlans[message.Plan]
	if state == nil {
		return ErrBootstrapStale
	}
	identity, ok := n.bootstrapMessageIdentity(state, message)
	if !ok || VerifyBootstrapMessage(message, identity) != nil || message.BaseID != state.record.Plan.Request.Base.ID ||
		message.BaseDigest != state.record.Plan.RequestDigest {
		return ErrInvalidBootstrapMessage
	}
	if state.record.Phase == BootstrapPhaseAborted {
		return ErrBootstrapAborted
	}
	if state.record.Phase == BootstrapPhaseActivated && message.Type != BootstrapMsgReadyQuery &&
		message.Type != BootstrapMsgActivationNotice {
		return ErrBootstrapControl
	}
	switch message.Type {
	case BootstrapMsgSealRequest:
		var plan VoterPlan
		if err := unmarshalBootstrapCanonical(message.Payload, &plan); err != nil || !voterPlanEqual(plan, state.record.Plan) {
			return ErrInvalidBootstrapMessage
		}
		return n.BeginVoterSeal(plan)
	case BootstrapMsgSealAck:
		var seal LocalSeal
		if err := unmarshalBootstrapCanonical(message.Payload, &seal); err != nil ||
			seal.Signer.Replica != message.From || VerifyLocalSeal(seal) != nil ||
			seal.Plan != state.record.Plan.Request.Plan ||
			!confStateEqual(seal.Base, state.record.Plan.Request.Base) ||
			seal.Reservations != state.record.Plan.Reservations ||
			validateBootstrapFrontier(seal.Frontier, state.record.Plan.Request.Base) != nil {
			return ErrInvalidBootstrapMessage
		}
		if state.record.SealCertificate.Digest != (StateDigest{}) {
			for _, certified := range state.record.SealCertificate.Seals {
				if certified.Signer.Replica == message.From && certified.Digest == seal.Digest {
					return nil
				}
			}
			return ErrBootstrapCertificate
		}
		if existing, duplicate := state.sealAcks[message.From]; duplicate {
			if existing.Digest == seal.Digest {
				return nil
			}
			return ErrBootstrapCertificate
		}
		state.sealAcks[message.From] = seal.Clone()
		if len(state.sealAcks) < slowQuorumSize(len(state.record.Plan.Request.Base.Voters)) {
			return nil
		}
		seals := make([]LocalSeal, 0, len(state.sealAcks))
		for _, candidate := range state.sealAcks {
			seals = append(seals, candidate.Clone())
		}
		certificate, err := BuildSealCertificate(state.record.Plan, seals)
		if err != nil {
			return err
		}
		return n.ApplySealCertificate(certificate)
	case BootstrapMsgCanonicalSeal:
		var certificate SealCertificate
		if err := unmarshalBootstrapCanonical(message.Payload, &certificate); err != nil {
			return err
		}
		return n.ApplySealCertificate(certificate)
	case BootstrapMsgSnapshotVote:
		if closure := n.BootstrapClosure(state.record.Plan); !closure.Complete {
			return ErrBootstrapClosure
		}
		var vote snapshotVotePayload
		var digest StateDigest
		if err := unmarshalBootstrapCanonical(message.Payload, &vote); err != nil {
			return ErrInvalidBootstrapMessage
		}
		digest = DigestSnapshotDescriptor(vote.Descriptor)
		if vote.Attestation.Signer.Replica != message.From ||
			verifyBootstrapAttestation(state.record.Plan, digest, vote.Attestation) != nil {
			return ErrInvalidBootstrapMessage
		}
		if err := validateSnapshotDescriptor(state.record.Plan, vote.Descriptor); err != nil ||
			vote.Descriptor.SealDigest != state.record.SealCertificate.Digest {
			return err
		}
		if state.record.SnapshotCertificate.Digest != (StateDigest{}) {
			if DigestSnapshotDescriptor(state.record.SnapshotCertificate.Descriptor) == digest {
				for _, certified := range state.record.SnapshotCertificate.Attestations {
					if certified.Signer.Replica == message.From &&
						bytes.Equal(certified.Signature, vote.Attestation.Signature) {
						return nil
					}
				}
			}
			return ErrBootstrapCertificate
		}
		if state.snapshotDigest != (StateDigest{}) && state.snapshotDigest != digest {
			return ErrBootstrapCertificate
		}
		state.snapshotDigest = digest
		if existing, duplicate := state.snapshotVotes[message.From]; duplicate {
			if existing.AttestedDigest == vote.Attestation.AttestedDigest && bytes.Equal(existing.Signature, vote.Attestation.Signature) {
				return nil
			}
			return ErrBootstrapCertificate
		}
		state.snapshotVotes[message.From] = vote.Attestation.Clone()
		if len(state.snapshotVotes) < slowQuorumSize(len(state.record.Plan.Request.Base.Voters)) {
			return nil
		}
		attestations := make([]VoterAttestation, 0, len(state.snapshotVotes))
		for _, candidate := range state.snapshotVotes {
			attestations = append(attestations, candidate.Clone())
		}
		certificate, err := BuildSnapshotCertificate(state.record.Plan, vote.Descriptor, attestations)
		if err != nil {
			return err
		}
		state.record.Phase = BootstrapPhaseCertified
		state.record.SnapshotCertificate = certificate
		n.enqueueBootstrapRecord(state)
		return nil
	case BootstrapMsgInstallProof:
		if closure := n.BootstrapClosure(state.record.Plan); !closure.Complete {
			return ErrBootstrapClosure
		}
		var install installProofPayload
		if err := unmarshalBootstrapCanonical(message.Payload, &install); err != nil ||
			VerifySnapshotCertificate(state.record.Plan, install.Snapshot) != nil ||
			state.record.SealCertificate.Digest == (StateDigest{}) ||
			install.Snapshot.Descriptor.SealDigest != state.record.SealCertificate.Digest ||
			VerifyVoterReadyProof(state.record.Plan, install.Snapshot, install.Proof) != nil {
			return ErrInvalidBootstrapMessage
		}
		if state.record.SnapshotCertificate.Digest == (StateDigest{}) {
			state.record.Phase = BootstrapPhaseCertified
			state.record.SnapshotCertificate = install.Snapshot.Clone()
			state.snapshotDigest = DigestSnapshotDescriptor(install.Snapshot.Descriptor)
			n.enqueueBootstrapRecord(state)
		} else if state.record.SnapshotCertificate.Digest != install.Snapshot.Digest {
			return ErrBootstrapSnapshot
		}
		return n.RecordTargetReady(state.record.Plan, install.Proof)
	case BootstrapMsgReadyAck:
		if state.record.TargetReady.Proof.Signature == nil {
			return ErrBootstrapSnapshot
		}
		var attestation VoterAttestation
		digest := DigestVoterReadyProof(state.record.TargetReady.Proof)
		if err := unmarshalBootstrapCanonical(message.Payload, &attestation); err != nil ||
			attestation.Signer.Replica != message.From ||
			verifyBootstrapAttestation(state.record.Plan, digest, attestation) != nil {
			return ErrInvalidBootstrapMessage
		}
		if state.record.ReadyCertificate.Digest != (StateDigest{}) {
			for _, certified := range state.record.ReadyCertificate.Attestations {
				if certified.Signer.Replica == message.From && bytes.Equal(certified.Signature, attestation.Signature) {
					return nil
				}
			}
			return ErrBootstrapCertificate
		}
		if state.readyDigest != (StateDigest{}) && state.readyDigest != digest {
			return ErrBootstrapCertificate
		}
		state.readyDigest = digest
		if existing, duplicate := state.readyVotes[message.From]; duplicate {
			if existing.AttestedDigest == attestation.AttestedDigest && bytes.Equal(existing.Signature, attestation.Signature) {
				return nil
			}
			return ErrBootstrapCertificate
		}
		state.readyVotes[message.From] = attestation.Clone()
		if len(state.readyVotes) < slowQuorumSize(len(state.record.Plan.Request.Base.Voters)) {
			return nil
		}
		attestations := make([]VoterAttestation, 0, len(state.readyVotes))
		for _, candidate := range state.readyVotes {
			attestations = append(attestations, candidate.Clone())
		}
		certificate, err := BuildReadyCertificate(state.record.Plan, state.record.TargetReady.Proof, attestations)
		if err != nil {
			return err
		}
		state.record.Phase = BootstrapPhaseFinalizing
		state.record.ReadyCertificate = certificate
		n.enqueueBootstrapRecord(state)
		return nil
	case BootstrapMsgReadyQuery:
		if state.record.ReadyCertificate.Digest == (StateDigest{}) ||
			state.durableDigest != state.record.Digest {
			return ErrBootstrapSnapshot
		}
		response, err := n.prepareBootstrapMessage(BootstrapMsgReadyResponse, state, message.From, state.record.ReadyCertificate)
		if err != nil {
			return err
		}
		n.readyTarget().BootstrapMessages = append(n.readyTarget().BootstrapMessages, response)
		return nil
	case BootstrapMsgReadyResponse:
		var certificate ReadyCertificate
		if err := unmarshalBootstrapCanonical(message.Payload, &certificate); err != nil ||
			VerifyReadyCertificate(state.record.Plan, state.record.SnapshotCertificate, certificate) != nil {
			return ErrInvalidBootstrapMessage
		}
		if state.record.ReadyCertificate.Digest != (StateDigest{}) && state.record.ReadyCertificate.Digest != certificate.Digest {
			return ErrBootstrapCertificate
		}
		state.record.Phase = BootstrapPhaseFinalizing
		state.record.ReadyCertificate = certificate.Clone()
		state.readyDigest = DigestVoterReadyProof(certificate.Proof)
		n.enqueueBootstrapRecord(state)
		return nil
	case BootstrapMsgActivationNotice:
		var result MembershipResult
		if err := unmarshalBootstrapCanonical(message.Payload, &result); err != nil ||
			state.record.Phase != BootstrapPhaseActivated ||
			result.Plan != state.record.Plan.Request.Plan ||
			result.Outcome != BootstrapOutcomeActivated ||
			result.ExitRef != state.record.Plan.Reservations.Activate ||
			result.CertificateDigest != state.record.ReadyCertificate.Digest ||
			!confStateEqual(result.Successor, state.record.Plan.Successor) {
			return ErrBootstrapControl
		}
		return nil
	default:
		return ErrInvalidBootstrapMessage
	}
}

func (n *RawNode) findBootstrapControl(ref InstanceRef) (*bootstrapState, membershipOperation, bool) {
	for _, state := range n.bootstrapPlans {
		switch ref {
		case state.record.Plan.Reservations.Prepare:
			return state, membershipPrepare, true
		case state.record.Plan.Reservations.Activate:
			return state, membershipActivate, true
		case state.record.Plan.Reservations.Abort:
			return state, membershipAbort, true
		}
	}
	return nil, 0, false
}

func bootstrapIdentityDigest(plan VoterPlan) StateDigest {
	identities := cloneVoterIdentities(plan.Request.OldVoters)
	identities = append(identities, plan.Request.Target.Clone())
	sort.Slice(identities, func(i, j int) bool { return identities[i].Replica < identities[j].Replica })
	canonical := appendConf(nil, plan.Successor)
	canonical = binary.AppendUvarint(canonical, uint64(len(identities)))
	for _, identity := range identities {
		canonical = appendIdentity(canonical, identity)
	}
	return domainDigest("epaxos/bootstrap/identity-set/v1", canonical)
}

func (n *RawNode) admitWhileSealed(message Message) error {
	closed, sealed := n.closedConfigs[message.Ref.Conf]
	if !sealed {
		return nil
	}
	if state, operation, control := n.findBootstrapControl(message.Ref); control {
		switch message.Type {
		case MsgPrepare:
			if !message.Ballot.IsRecovery() {
				return ErrBootstrapControl
			}
			return nil
		case MsgPreAcceptResp, MsgPrepareResp, MsgAcceptResp:
			if message.Command.Kind == CommandUser && len(message.Command.Payload) == 0 {
				return nil
			}
		}
		wire, err := decodeMembershipCommand(message.Command)
		if err != nil || wire.Operation != operation || wire.Plan.Request.Plan != state.record.Plan.Request.Plan {
			return ErrBootstrapControl
		}
		return nil
	}
	lane, ok := closed.Frontier.lane(message.Ref.Replica)
	if !ok {
		return ErrBootstrapSealed
	}
	if message.Ref.Instance > lane.ObservedThrough {
		if message.Type == MsgCommit {
			return ErrBootstrapContradiction
		}
		return ErrBootstrapSealed
	}
	inst := n.instances[message.Ref]
	switch message.Type {
	case MsgPreAccept:
		if inst == nil || !phaseMessageValueEqual(inst.rec, message) {
			return ErrBootstrapSealed
		}
	case MsgAccept:
		if !message.Ballot.IsRecovery() && (inst == nil || !phaseMessageValueEqual(inst.rec, message)) {
			return ErrBootstrapSealed
		}
	case MsgPrepare:
		if !message.Ballot.IsRecovery() {
			return ErrBootstrapSealed
		}
	case MsgCommit:
		if inst == nil {
			return ErrBootstrapContradiction
		}
	}
	return nil
}

func (n *RawNode) membershipAllNoneCommand(ref InstanceRef) (Command, bool) {
	state, operation, ok := n.findBootstrapControl(ref)
	if !ok || (operation != membershipActivate && operation != membershipAbort) ||
		state.durableDigest != state.record.Digest {
		return Command{}, false
	}
	wire := membershipCommandWire{Operation: operation, Plan: state.record.Plan.Clone(), SealDigest: state.record.SealCertificate.Digest}
	if operation == membershipActivate {
		if state.record.SnapshotCertificate.Digest == (StateDigest{}) || state.record.ReadyCertificate.Digest == (StateDigest{}) {
			return Command{}, false
		}
		wire.Snapshot = state.record.SnapshotCertificate.Clone()
		wire.Ready = state.record.ReadyCertificate.Clone()
	}
	command, err := encodeMembershipCommand(wire)
	return command, err == nil
}

func (n *RawNode) applyMembershipControl(ref InstanceRef, command Command) (MembershipResult, ConfChangeResult) {
	wire, err := decodeMembershipCommand(command)
	if err != nil || !membershipCommandValidForRef(command, ref, n.confFor(ref.Conf)) {
		return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedInvalid, ExitRef: ref}, ConfChangeResult{}
	}
	state := n.bootstrapPlans[wire.Plan.Request.Plan]
	if state == nil && wire.Operation == membershipPrepare {
		record := BootstrapRecord{Plan: wire.Plan.Clone(), Phase: BootstrapPhasePreparing}
		record.Digest = DigestBootstrapRecord(record)
		if n.restoreBootstrapRecord(record) != nil {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedInvalid, ExitRef: ref}, ConfChangeResult{}
		}
		state = n.bootstrapPlans[wire.Plan.Request.Plan]
	}
	if state == nil || !voterPlanEqual(state.record.Plan, wire.Plan) {
		return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedInvalid, ExitRef: ref}, ConfChangeResult{}
	}
	switch wire.Operation {
	case membershipPrepare:
		if state.record.Phase < BootstrapPhasePrepared {
			state.record.Phase = BootstrapPhasePrepared
			n.enqueueBootstrapRecord(state)
		}
		return MembershipResult{}, ConfChangeResult{}
	case membershipActivate:
		if state.record.Outcome != BootstrapOutcomeUnspecified {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedSuperseded, ExitRef: ref}, ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}
		}
		if VerifySnapshotCertificate(wire.Plan, wire.Snapshot) != nil ||
			VerifyReadyCertificate(wire.Plan, wire.Snapshot, wire.Ready) != nil ||
			wire.SealDigest == (StateDigest{}) ||
			(state.record.SealCertificate.Digest != (StateDigest{}) &&
				wire.SealDigest != state.record.SealCertificate.Digest) ||
			wire.Snapshot.Descriptor.SealDigest != wire.SealDigest {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedInvalid, ExitRef: ref}, ConfChangeResult{Outcome: ConfChangeRejectedInvalid}
		}
		if _, exists := n.appliedConfByBase[ref.Conf]; exists {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedSuperseded, ExitRef: ref}, ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}
		}
		successor := wire.Plan.Successor.Clone()
		if existing, exists := n.confHistory[successor.ID]; exists && !confStateEqual(existing, successor) {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedSuperseded, ExitRef: ref}, ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}
		}
		q, qerr := newQuorum(successor.Voters)
		if qerr != nil {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedInvalid, ExitRef: ref}, ConfChangeResult{Outcome: ConfChangeRejectedInvalid}
		}
		q.conf.ID = successor.ID
		n.confHistory[successor.ID] = successor.Clone()
		n.appliedConfByBase[ref.Conf] = ref
		n.voterIdentities[wire.Plan.Request.Target.Replica] = wire.Plan.Request.Target.Clone()
		state.record.Phase = BootstrapPhaseActivated
		state.record.Outcome = BootstrapOutcomeActivated
		state.record.TerminalRef = ref
		state.record.SnapshotCertificate = wire.Snapshot.Clone()
		state.record.TargetReady = TargetReadyRecord{Plan: wire.Plan.Request.Plan, Proof: wire.Ready.Proof.Clone()}
		state.record.ReadyCertificate = wire.Ready.Clone()
		state.record.Closed = ClosedConfig{
			Conf: wire.Plan.Request.Base.Clone(), Frontier: wire.Snapshot.Descriptor.Frontier.Clone(),
			Reservations: wire.Plan.Reservations, CertificateDigest: wire.SealDigest,
		}
		n.closedConfigs[ref.Conf] = state.record.Closed.Clone()
		target := n.readyTarget()
		target.ConfigHistory = append(target.ConfigHistory, ConfigHistoryEntry{
			Conf: successor.Clone(), AppliedRef: ref, IdentityDigest: bootstrapIdentityDigest(wire.Plan),
		})
		if ref.Conf == n.q.conf.ID {
			n.q = q
			n.activateTOQRuntime(successor)
			n.currentHardState.Conf = successor.Clone()
			n.localVoter.Conf = successor.Clone()
			n.localVoter.Status = LocalVoterStatusEligible
			n.localVoter.Plan = wire.Plan.Request.Plan
			n.localVoter.AllocatorFloor = maxInstanceNum(n.localVoter.AllocatorFloor, n.allocatorFloor)
			if wire.Snapshot.Descriptor.TOQClosedThrough > n.localVoter.TOQClosedThrough {
				n.localVoter.TOQClosedThrough = wire.Snapshot.Descriptor.TOQClosedThrough
			}
			local := n.localVoter.Clone()
			target.LocalVoterState = &local
		}
		target.MustSync = true
		n.enqueueBootstrapRecord(state)
		result := MembershipResult{
			Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeActivated, ExitRef: ref,
			CertificateDigest: wire.Ready.Digest, Successor: successor.Clone(),
		}
		if notice, noticeErr := n.prepareBootstrapMessage(
			BootstrapMsgActivationNotice, state, wire.Plan.Request.Target.Replica, result,
		); noticeErr == nil {
			n.enqueueBootstrapAfterAdvance(state, notice, false)
		}
		return result, ConfChangeResult{Outcome: ConfChangeApplied, Conf: successor}
	case membershipAbort:
		if state.record.Outcome != BootstrapOutcomeUnspecified {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedSuperseded, ExitRef: ref}, ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}
		}
		if state.record.SealCertificate.Digest != (StateDigest{}) && wire.SealDigest != state.record.SealCertificate.Digest {
			return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedInvalid, ExitRef: ref}, ConfChangeResult{Outcome: ConfChangeRejectedInvalid}
		}
		state.record.Phase = BootstrapPhaseAborted
		state.record.Outcome = BootstrapOutcomeAborted
		state.record.TerminalRef = ref
		n.enqueueBootstrapRecord(state)
		n.enqueueBootstrapAfterAdvance(state, BootstrapMessage{}, true)
		return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeAborted, ExitRef: ref, CertificateDigest: wire.SealDigest}, ConfChangeResult{}
	default:
		return MembershipResult{Plan: wire.Plan.Request.Plan, Outcome: BootstrapOutcomeRejectedInvalid, ExitRef: ref}, ConfChangeResult{}
	}
}

func configHistoryEntryEqual(a, b ConfigHistoryEntry) bool {
	return confStateEqual(a.Conf, b.Conf) && a.AppliedRef == b.AppliedRef && a.IdentityDigest == b.IdentityDigest
}

func localVoterStateEqual(a, b LocalVoterState) bool {
	return a.Cluster == b.Cluster && voterIdentityEqual(a.Identity, b.Identity) && confStateEqual(a.Conf, b.Conf) &&
		a.Status == b.Status && a.Plan == b.Plan && a.InstalledDigest == b.InstalledDigest &&
		a.AllocatorFloor == b.AllocatorFloor && a.TOQClosedThrough == b.TOQClosedThrough
}

func frontierUpdateEqual(a, b FrontierUpdate) bool {
	return frontierEqual(a.Frontier, b.Frontier) && a.AllocatorFloor == b.AllocatorFloor &&
		a.TOQClosedThrough == b.TOQClosedThrough && a.EvidenceDigest == b.EvidenceDigest
}

func bootstrapMessageEqual(a, b BootstrapMessage) bool {
	return a.Type == b.Type && a.Cluster == b.Cluster && a.Plan == b.Plan && a.From == b.From &&
		a.FromIncarnation == b.FromIncarnation && a.To == b.To && a.BaseID == b.BaseID &&
		a.BaseDigest == b.BaseDigest && a.PayloadDigest == b.PayloadDigest &&
		bytes.Equal(a.Payload, b.Payload) && bytes.Equal(a.Signature, b.Signature)
}

func (n *RawNode) applyBootstrapDurability(records []BootstrapRecord) {
	if len(records) == 0 {
		return
	}
	acked := make(map[BootstrapID]map[StateDigest]struct{})
	for _, record := range records {
		if state := n.bootstrapPlans[record.Plan.Request.Plan]; state != nil && record.Phase >= state.durablePhase {
			state.durablePhase = record.Phase
			state.durableDigest = record.Digest
		}
		byDigest := acked[record.Plan.Request.Plan]
		if byDigest == nil {
			byDigest = make(map[StateDigest]struct{})
			acked[record.Plan.Request.Plan] = byDigest
		}
		byDigest[record.Digest] = struct{}{}
	}
	if len(n.bootstrapDurability) == 0 {
		return
	}
	remaining := n.bootstrapDurability[:0]
	for _, action := range n.bootstrapDurability {
		if _, ok := acked[action.plan][action.recordDigest]; !ok {
			remaining = append(remaining, action)
			continue
		}
		if action.unseal {
			if state := n.bootstrapPlans[action.plan]; state != nil {
				delete(n.closedConfigs, state.record.Plan.Request.Base.ID)
				delete(n.bootstrapByBase, state.record.Plan.Request.Base.ID)
			}
		}
		if action.message.Type != 0 {
			target := n.readyTarget()
			target.BootstrapMessages = append(target.BootstrapMessages, action.message.Clone())
		}
	}
	clear(n.bootstrapDurability[len(remaining):])
	n.bootstrapDurability = remaining
}

var (
	// ErrVoterCertificateRequired rejects certificate-free additive membership.
	ErrVoterCertificateRequired = errors.New("epaxos: voter certificate required")
	// ErrBootstrapEligibility reports absent or mismatched durable voter eligibility.
	ErrBootstrapEligibility = errors.New("epaxos: bootstrap voter ineligible")
	// ErrBootstrapBusy reports a conflicting plan or membership operation.
	ErrBootstrapBusy = errors.New("epaxos: bootstrap plan busy")
	// ErrBootstrapStale reports a plan whose exact base is no longer current.
	ErrBootstrapStale = errors.New("epaxos: bootstrap plan stale")
	// ErrBootstrapNotSealed reports an operation attempted before canonical seal.
	ErrBootstrapNotSealed = errors.New("epaxos: bootstrap plan not sealed")
	// ErrBootstrapClosure reports unresolved old-configuration closure obligations.
	ErrBootstrapClosure = errors.New("epaxos: bootstrap closure incomplete")
	// ErrBootstrapCertificate reports malformed, noncanonical, or insufficient evidence.
	ErrBootstrapCertificate = errors.New("epaxos: invalid bootstrap certificate")
	// ErrBootstrapControl reports an invalid reserved-ref control value.
	ErrBootstrapControl = errors.New("epaxos: invalid bootstrap control")
	// ErrBootstrapAborted reports use of a terminally aborted plan.
	ErrBootstrapAborted = errors.New("epaxos: bootstrap plan aborted")
	// ErrBootstrapSnapshot reports a stale or mismatched installed snapshot.
	ErrBootstrapSnapshot = errors.New("epaxos: invalid bootstrap snapshot")
	// ErrBootstrapSealed reports ordinary old-config traffic refused by the fence.
	ErrBootstrapSealed = errors.New("epaxos: configuration sealed")
	// ErrBootstrapContradiction reports a commit above a certified frontier.
	ErrBootstrapContradiction = errors.New("epaxos: bootstrap frontier contradiction")
	// ErrBootstrapBounds reports an input exceeding a protocol bound.
	ErrBootstrapBounds = errors.New("epaxos: bootstrap input exceeds bound")
	// ErrBootstrapChunkConflict reports overlapping or different duplicate chunks.
	ErrBootstrapChunkConflict = errors.New("epaxos: bootstrap chunk conflict")
	// ErrBootstrapCompactionEvidence reports certification after evidence deletion.
	ErrBootstrapCompactionEvidence = errors.New("epaxos: bootstrap compaction evidence unavailable")
	// ErrInvalidBootstrapMessage reports a malformed or unauthenticated envelope.
	ErrInvalidBootstrapMessage = errors.New("epaxos: invalid bootstrap message")
)

func bootstrapError(base error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", base, fmt.Sprintf(format, args...))
}
