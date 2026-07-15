package epaxos

import (
	"errors"
	"fmt"
	"sort"
	"unsafe"
)

func cloneSliceInto[T any](dst, src []T) []T {
	source := src
	sameStorage := slicesSameStorageAndShape(dst, source)
	if len(source) == 0 {
		if !sameStorage {
			clear(dst[:cap(dst)])
		}
		return dst[:0]
	}
	if cap(dst) < len(source) {
		dst = make([]T, len(source))
	} else {
		dst = dst[:len(source)]
	}
	copy(dst, source)
	if !sameStorage {
		clear(dst[len(source):cap(dst)])
	}
	return dst
}

func slicesSameStorageAndShape[T any](left, right []T) bool {
	if len(left) != len(right) || cap(left) != cap(right) {
		return false
	}
	if cap(left) == 0 {
		return true
	}
	return unsafe.SliceData(left[:cap(left)]) == unsafe.SliceData(right[:cap(right)]) //nolint:gosec // G103: unsafe pointer arithmetic is confined to overlap detection.
}

func slicesPartiallyOverlap[T any](dst, src []T) bool {
	if len(src) == 0 || cap(dst) < len(src) {
		return false
	}
	size := unsafe.Sizeof(*new(T))
	if size == 0 {
		return false
	}
	dstStart := uintptr(unsafe.Pointer(unsafe.SliceData(dst[:cap(dst)]))) //nolint:gosec // G103: unsafe pointer arithmetic is confined to overlap detection.
	srcStart := uintptr(unsafe.Pointer(unsafe.SliceData(src)))            //nolint:gosec // G103: unsafe pointer arithmetic is confined to overlap detection.
	if dstStart == srcStart {
		return false
	}
	dstEnd := dstStart + uintptr(len(src))*size
	srcEnd := srcStart + uintptr(len(src))*size
	return dstStart < srcEnd && srcStart < dstEnd
}

func cloneByteSlicesInto(dst, src [][]byte) [][]byte {
	source := src
	if slicesPartiallyOverlap(dst, source) {
		snapshot := make([][]byte, len(source))
		for i := range source {
			snapshot[i] = append([]byte(nil), source[i]...)
		}
		source = snapshot
	}
	if cap(dst) < len(source) {
		grown := make([][]byte, len(source))
		copy(grown, dst)
		dst = grown
	} else {
		dst = dst[:len(source)]
	}
	for i := range source {
		dst[i] = cloneSliceInto(dst[i], source[i])
	}
	full := dst[:cap(dst)]
	for i := len(source); i < len(full); i++ {
		full[i] = nil
	}
	return dst
}

// ReplicaID identifies a replica inside a configuration.
type ReplicaID uint64

// InstanceNum identifies one instance in a replica-local EPaxos instance space.
type InstanceNum uint64

// ConfID identifies the configuration that owns an instance.
type ConfID uint64

// InstanceRef is the globally unique name of an EPaxos instance.
type InstanceRef struct {
	Replica  ReplicaID
	Instance InstanceNum
	Conf     ConfID
}

// IsZero reports whether the reference names no instance.
func (r InstanceRef) IsZero() bool { return r.Replica == 0 && r.Instance == 0 && r.Conf == 0 }

// String returns a deterministic human-readable instance reference.
func (r InstanceRef) String() string { return fmt.Sprintf("%d.%d@%d", r.Replica, r.Instance, r.Conf) }

// Ballot is the EPaxos promise/accept ballot.
type Ballot struct {
	Epoch   uint64
	Number  uint64
	Replica ReplicaID
}

// Less reports whether b is ordered before other.
func (b Ballot) Less(other Ballot) bool {
	if b.Epoch != other.Epoch {
		return b.Epoch < other.Epoch
	}
	if b.Number != other.Number {
		return b.Number < other.Number
	}
	return b.Replica < other.Replica
}

// IsInitialFor reports whether b is replica's exact configuration-epoch-zero
// initial ballot. A nonzero Epoch is always a recovery ballot even when Number
// is zero.
func (b Ballot) IsInitialFor(replica ReplicaID) bool {
	return replica != 0 && b.Epoch == 0 && b.Number == 0 && b.Replica == replica
}

// IsRecovery reports whether b is a non-initial, nonzero ballot.
func (b Ballot) IsRecovery() bool {
	return b != (Ballot{}) && (b.Epoch != 0 || b.Number != 0)
}

// Next returns a ballot owned by replica and strictly greater than b.
// It carries a saturated Number into the next Epoch and fails when the
// complete ballot counter space is exhausted.
func (b Ballot) Next(replica ReplicaID) (Ballot, error) {
	if b.Number != ^uint64(0) {
		return Ballot{Epoch: b.Epoch, Number: b.Number + 1, Replica: replica}, nil
	}
	if b.Epoch == ^uint64(0) {
		return Ballot{}, ErrBallotExhausted
	}
	return Ballot{Epoch: b.Epoch + 1, Replica: replica}, nil
}

// TimingDomain identifies the clock domain of an instance's ProcessAt value.
// Numeric timestamps are comparable only within the same nonzero domain.
type TimingDomain uint8

const (
	// TimingDomainUntimed means ProcessAt is zero and has no clock semantics.
	TimingDomainUntimed TimingDomain = iota
	// TimingDomainLogical means ProcessAt uses durable logical ticks.
	TimingDomainLogical
	// TimingDomainTOQ means ProcessAt uses the caller-sampled TOQ clock.
	TimingDomainTOQ
)

// Status is the durable status of an EPaxos instance.
type Status uint8

const (
	// StatusNone means the instance is unknown.
	StatusNone Status = iota
	// StatusPreAccepted means the pre-accept phase stored attributes.
	StatusPreAccepted
	// StatusAccepted means the slow accept phase stored attributes.
	StatusAccepted
	// StatusCommitted means the value and attributes are final.
	StatusCommitted
	// StatusExecuted means the command has been emitted to the application.
	StatusExecuted
)

// String returns a stable status name.
func (s Status) String() string {
	switch s {
	case StatusNone:
		return "none"
	case StatusPreAccepted:
		return "pre-accepted"
	case StatusAccepted:
		return "accepted"
	case StatusCommitted:
		return "committed"
	case StatusExecuted:
		return "executed"
	default:
		return "unknown"
	}
}

// EntryKind discriminates application values from protocol-only entries.
type EntryKind uint8

const (
	// EntryCommand carries an opaque application command.
	EntryCommand EntryKind = iota + 1
	// EntryNoop carries no application or protocol value.
	EntryNoop
	// EntryConfChange carries a legacy validated configuration change.
	EntryConfChange
	// EntryMembership carries membership protocol control.
	EntryMembership
	// EntryCheckpoint carries certified checkpoint protocol control.
	EntryCheckpoint
)

// CommandID is an application-supplied id used for duplicate detection by clients.
type CommandID struct {
	Client   uint64
	Sequence uint64
}

// Command is an opaque application value proposed through EPaxos.
//
// Footprint is correctness metadata, not an optimization hint. Payload,
// CommandID, and CycleKey are opaque to the core. Propose canonicalizes the
// footprint and clones all referenced bytes unless ZeroCopyProposals is set.
type Command struct {
	ID        CommandID
	Payload   []byte
	Footprint Footprint
	CycleKey  []byte
}

// Reset releases references held by the command so it can be reused.
func (c *Command) Reset() {
	c.ID = CommandID{}
	c.Payload = nil
	c.Footprint = Footprint{}
	c.CycleKey = nil
}

// CloneInto deep-copies c into dst while reusing destination capacity.
func (c Command) CloneInto(dst *Command) {
	payload := cloneSliceInto(dst.Payload, c.Payload)
	footprint := dst.Footprint
	cloneFootprintInto(&footprint, c.Footprint)
	cycleKey := cloneSliceInto(dst.CycleKey, c.CycleKey)
	*dst = Command{ID: c.ID, Payload: payload, Footprint: footprint, CycleKey: cycleKey}
}

// Clone returns a deep copy of the command.
func (c Command) Clone() Command {
	var out Command
	c.CloneInto(&out)
	return out
}

// Borrow returns c unchanged. The caller transfers ownership of all referenced
// buffers for the lifetime documented by Config.ZeroCopyProposals.
func (c Command) Borrow() Command { return c }

// ConflictsWith reports whether two application commands overlap.
func (c Command) ConflictsWith(other Command) bool {
	return footprintsConflict(c.Footprint, other.Footprint)
}

func commandEmpty(c Command) bool {
	return c.ID == (CommandID{}) && len(c.Payload) == 0 && len(c.Footprint.Points) == 0 &&
		len(c.Footprint.Spans) == 0 && !c.Footprint.All && len(c.CycleKey) == 0
}

func entryValueCanonical(kind EntryKind, command Command, change ConfChange, control []byte) bool {
	changeEmpty := change == (ConfChange{})
	switch kind {
	case 0:
		return commandEmpty(command) && changeEmpty && len(control) == 0
	case EntryCommand:
		return changeEmpty && len(control) == 0 && footprintCanonical(command.Footprint) &&
			len(command.CycleKey) <= maxWireCycleKeyBytes
	case EntryNoop:
		return commandEmpty(command) && changeEmpty && len(control) == 0
	case EntryConfChange:
		return commandEmpty(command) && len(control) == 0 &&
			(change.Type == ConfChangeAddVoter || change.Type == ConfChangeRemoveVoter) && change.Replica != 0
	case EntryMembership, EntryCheckpoint:
		return commandEmpty(command) && changeEmpty && len(control) != 0 && len(control) <= maxWireProtocolControl
	default:
		return false
	}
}

// ConfChangeType identifies the membership operation in a configuration command.
type ConfChangeType uint8

const (
	// ConfChangeAddVoter adds a voting replica to future instances.
	ConfChangeAddVoter ConfChangeType = iota + 1
	// ConfChangeRemoveVoter removes a voting replica from future instances.
	ConfChangeRemoveVoter
)

// ConfChange is encoded into a EntryConfChange command by ProposeConfChange.
type ConfChange struct {
	Type    ConfChangeType
	Replica ReplicaID
}

// ConfChangeOutcome is the terminal replicated result of an executed
// configuration command.
type ConfChangeOutcome uint8

const (
	// ConfChangeOutcomeUnspecified is valid only before a configuration command
	// executes. Executed configuration records must carry a terminal outcome.
	ConfChangeOutcomeUnspecified ConfChangeOutcome = iota
	// ConfChangeApplied means the command installed ConfChangeResult.Conf.
	ConfChangeApplied
	// ConfChangeRejectedSuperseded means another command already applied the
	// transition from this instance's pinned base configuration.
	ConfChangeRejectedSuperseded
	// ConfChangeRejectedInvalid means the durable command was invalid for its
	// pinned base configuration.
	ConfChangeRejectedInvalid
)

// ConfChangeResult is the checksummed terminal result of an executed
// configuration command. Conf is populated only for ConfChangeApplied.
type ConfChangeResult struct {
	Outcome ConfChangeOutcome
	Conf    ConfState
}

// ConfState is a deterministic snapshot of voters at a configuration id.
type ConfState struct {
	ID     ConfID
	Voters []ReplicaID
}

// CloneInto deep-copies c into dst while reusing destination capacity.
func (c ConfState) CloneInto(dst *ConfState) {
	voters := cloneSliceInto(dst.Voters, c.Voters)
	*dst = ConfState{ID: c.ID, Voters: voters}
}

// Clone returns a deep copy of the configuration state.
func (c ConfState) Clone() ConfState {
	var out ConfState
	c.CloneInto(&out)
	return out
}

// Contains reports whether id is a voter in the configuration.
func (c ConfState) Contains(id ReplicaID) bool {
	_, ok := c.Index(id)
	return ok
}

// Index returns the stable dependency-vector index for a replica.
func (c ConfState) Index(id ReplicaID) (int, bool) {
	i := sort.Search(len(c.Voters), func(i int) bool { return c.Voters[i] >= id })
	return i, i < len(c.Voters) && c.Voters[i] == id
}

// TOQRuntimeConfig binds TOQ delay and synchronization-group inputs to one
// exact configuration epoch. The node clones every supplied slice and map.
type TOQRuntimeConfig struct {
	Conf        ConfState
	OneWayDelay map[ReplicaID]uint64
	// SyncGroup is the group used when choosing the maximum delay. Empty means
	// every voter in Conf.
	SyncGroup []ReplicaID
}

// CloneInto deep-copies c into dst while reusing destination capacity.
func (c TOQRuntimeConfig) CloneInto(dst *TOQRuntimeConfig) {
	conf := dst.Conf
	c.Conf.CloneInto(&conf)
	delays := dst.OneWayDelay
	if delays == nil {
		delays = make(map[ReplicaID]uint64, len(c.OneWayDelay))
	} else {
		clear(delays)
	}
	for id, delay := range c.OneWayDelay {
		delays[id] = delay
	}
	group := cloneSliceInto(dst.SyncGroup, c.SyncGroup)
	*dst = TOQRuntimeConfig{Conf: conf, OneWayDelay: delays, SyncGroup: group}
}

// Clone returns a deep copy of the runtime configuration.
func (c TOQRuntimeConfig) Clone() TOQRuntimeConfig {
	var out TOQRuntimeConfig
	c.CloneInto(&out)
	return out
}

// Config configures a RawNode.
type Config struct {
	// ID is the local replica id and must be present in Voters.
	ID ReplicaID
	// Voters is the complete voting configuration for the initial cluster.
	Voters []ReplicaID
	// Cluster identifies the externally provisioned bootstrap trust domain. The
	// zero value selects legacy genesis behavior and cannot prepare new voters.
	Cluster ClusterID
	// LocalIdentity is the exact locally fenced voter incarnation.
	LocalIdentity VoterIdentity
	// VoterIdentities supplies canonical externally authenticated incarnations for Voters.
	VoterIdentities []VoterIdentity
	// Storage persists hard state and instance records. A nil Storage selects
	// an in-memory implementation.
	Storage Storage
	// RetryTicks is the logical tick interval for protocol retransmission.
	RetryTicks uint64
	// RecoveryTicks is the logical tick interval for recovery attempts.
	RecoveryTicks uint64
	// TimeOptimization enables deterministic logical ProcessAt timing for
	// PreAccept handling and a bounded logical fast-wait before slow accept.
	TimeOptimization bool
	// TimeOptimizationTicks is the logical ProcessAt offset and fast-wait duration.
	TimeOptimizationTicks uint64
	// TOQ enables EPaxos Revisited Timestamp-Ordered Queueing. It is a separate
	// mode from TimeOptimization and requires an explicitly sampled clock plus
	// conservative delay-bound inputs from the embedding application.
	TOQ bool
	// TOQRuntime binds operational timing inputs to an exact configuration.
	// Nil is valid and leaves local TOQ proposal capability unavailable until
	// RefreshTOQConfig installs a matching entry.
	TOQRuntime *TOQRuntimeConfig
	// LegacyProcessAtDomain explicitly assigns one domain while upgrading
	// nonpending durable records written before TimingDomain was authenticated.
	// Nil fails closed; a pointer may explicitly select Untimed, Logical, or TOQ.
	LegacyProcessAtDomain *TimingDomain
	// MaxDeferredPreAccepts bounds retained future timed PreAccept messages
	// across the logical and TOQ domains. Zero selects a conservative default.
	MaxDeferredPreAccepts int
	// MaxDeferredRecordLoads bounds deferred messages waiting on async record
	// loads for folded instances. Zero selects the package default of 256.
	MaxDeferredRecordLoads int
	// RetainExecutedPerLane is the per-lane resident tail of executed instances
	// kept after fold (0 selects default 1024).
	RetainExecutedPerLane int
	// MaxResidentInstances fails local Propose when engine.residentCount exceeds
	// the bound (0 = unlimited).
	MaxResidentInstances int
	// ZeroCopyProposals borrows already-canonical Payload, Footprint, and
	// CycleKey buffers. The caller must not mutate or reuse borrowed buffers
	// while they remain observable through Ready or Status. Canonicalization
	// that sorts, merges, deduplicates, or removes resources produces owned data.
	ZeroCopyProposals bool
	// MaxFootprintPoints bounds raw point count on local proposals.
	MaxFootprintPoints int
	// MaxFootprintSpans bounds raw span count on local proposals.
	MaxFootprintSpans int
	// MaxFootprintBytes bounds total raw footprint endpoint bytes.
	MaxFootprintBytes int
	// MaxCycleKeyBytes bounds replicated SCC tie-break metadata.
	MaxCycleKeyBytes int
	// MaxReadyMessages caps Ready.Messages without capping durable records or
	// committed application commands. A value less than one leaves messages
	// uncapped.
	MaxReadyMessages int
	// MaxDependencyRecoveriesPerDrive caps new exact dependency recoveries
	// started across one outer Propose, Step, Tick, ProcessTOQ, or startup
	// drive. Zero selects a conservative default.
	MaxDependencyRecoveriesPerDrive int
	// MaxConcurrentRecoveries caps unfinished ballot-based recoveries
	// coordinated locally before dependency closure starts another. Zero
	// selects a conservative default.
	MaxConcurrentRecoveries int
}

// HardState is the durable node-level state loaded before instances.
type HardState struct {
	Conf ConfState
	Tick uint64
}

// Empty reports whether h contains no durable update.
func (h HardState) Empty() bool {
	return h.Conf.ID == 0 && len(h.Conf.Voters) == 0 && h.Tick == 0
}

// Equal reports whether two hard states match exactly.
func (h HardState) Equal(other HardState) bool {
	if h.Tick != other.Tick || h.Conf.ID != other.Conf.ID || len(h.Conf.Voters) != len(other.Conf.Voters) {
		return false
	}
	for i := range h.Conf.Voters {
		if h.Conf.Voters[i] != other.Conf.Voters[i] {
			return false
		}
	}
	return true
}

// CloneInto deep-copies h into dst while reusing destination capacity.
func (h HardState) CloneInto(dst *HardState) {
	conf := dst.Conf
	h.Conf.CloneInto(&conf)
	*dst = HardState{Conf: conf, Tick: h.Tick}
}

// Clone returns a deep copy of the hard state.
func (h HardState) Clone() HardState {
	var out HardState
	h.CloneInto(&out)
	return out
}

// InstanceRecord is the durable value for one EPaxos instance.
type InstanceRecord struct {
	Ref    InstanceRef
	Ballot Ballot
	// RecordBallot is the ballot at which Seq/Deps/Command were last chosen as
	// a preaccepted, accepted, or committed value. Prepare promises may advance
	// Ballot without changing RecordBallot.
	RecordBallot Ballot
	Status       Status
	Seq          uint64
	Deps         []InstanceNum
	// AcceptSeq and AcceptDeps are recovery-only Accept-Deps evidence from the
	// paper/TR optimized recovery path. They record dependencies carried on
	// Accept/AcceptReply without changing the chosen execution attributes in
	// Seq/Deps.
	AcceptSeq  uint64
	AcceptDeps []InstanceNum
	// AcceptEvidence preserves original Accept/AcceptReply senders. It is tied
	// to this record's current value tuple and must be cleared when that tuple changes.
	AcceptEvidence   []AcceptEvidence
	Kind             EntryKind
	ConfChange       ConfChange
	ProtocolControl  []byte
	Command          Command
	FastPathEligible bool
	// ProcessAt is pinned when an instance is created or first received.
	// TOQ mode interprets it only in the caller-sampled physical domain;
	// TimeOptimization interprets it only in the durable logical-tick domain.
	// Phase changes never recompute it or compare values across clock domains.
	ProcessAt uint64
	// TimingDomain authenticates the clock domain of ProcessAt. Untimed
	// requires ProcessAt zero; Logical and TOQ require it nonzero.
	TimingDomain TimingDomain
	// TOQPending means the command has been durably accepted by the local API but
	// has not yet assigned its originator dependencies at ProcessAt. It must not
	// be indexed, recovered, or counted as an ordinary PreAccepted tuple.
	TOQPending bool
	// ConfChangeResult is the terminal replicated outcome of a configuration
	// command. It is zero before execution and for non-configuration commands.
	ConfChangeResult ConfChangeResult
	// MembershipResult is the terminal certified voter-control outcome. It is
	// zero before execution and on non-membership records.
	MembershipResult MembershipResult
	Checksum         [32]byte
}

// CloneInto deep-copies r into dst while reusing destination capacity.
func (r InstanceRecord) CloneInto(dst *InstanceRecord) {
	deps := cloneSliceInto(dst.Deps, r.Deps)
	acceptDeps := cloneSliceInto(dst.AcceptDeps, r.AcceptDeps)
	acceptEvidence := cloneAcceptEvidenceInto(dst.AcceptEvidence, r.AcceptEvidence)
	command := dst.Command
	r.Command.CloneInto(&command)
	control := cloneSliceInto(dst.ProtocolControl, r.ProtocolControl)
	conf := dst.ConfChangeResult.Conf
	r.ConfChangeResult.Conf.CloneInto(&conf)
	membership := r.MembershipResult.Clone()
	*dst = r
	dst.Deps = deps
	dst.AcceptDeps = acceptDeps
	dst.AcceptEvidence = acceptEvidence
	dst.ProtocolControl = control
	dst.Command = command
	dst.ConfChangeResult.Conf = conf
	dst.MembershipResult = membership
}

// Clone returns a deep copy of the instance record.
func (r InstanceRecord) Clone() InstanceRecord {
	var out InstanceRecord
	r.CloneInto(&out)
	return out
}

// Attributes returns the ordering attributes carried by the record.
func (r InstanceRecord) Attributes() Attributes {
	return Attributes{Seq: r.Seq, Deps: append([]InstanceNum(nil), r.Deps...)}
}

// AcceptAttributes returns the recovery-only Accept-Deps evidence carried by
// this record. The boolean is false for records with no separately recorded
// Accept-Deps evidence.
func (r InstanceRecord) AcceptAttributes() (Attributes, bool) {
	if r.AcceptSeq == 0 || len(r.AcceptDeps) == 0 {
		return Attributes{}, false
	}
	return Attributes{Seq: r.AcceptSeq, Deps: append([]InstanceNum(nil), r.AcceptDeps...)}, true
}

// Attributes are EPaxos sequence/dependency ordering metadata.
type Attributes struct {
	Seq  uint64
	Deps []InstanceNum
}

// CloneInto deep-copies a into dst while reusing destination capacity.
func (a Attributes) CloneInto(dst *Attributes) {
	deps := cloneSliceInto(dst.Deps, a.Deps)
	*dst = Attributes{Seq: a.Seq, Deps: deps}
}

// Clone returns a deep copy of the attributes.
func (a Attributes) Clone() Attributes {
	var out Attributes
	a.CloneInto(&out)
	return out
}

// Equal reports whether two attribute sets match exactly.
func (a Attributes) Equal(b Attributes) bool {
	if a.Seq != b.Seq || len(a.Deps) != len(b.Deps) {
		return false
	}
	for i := range a.Deps {
		if a.Deps[i] != b.Deps[i] {
			return false
		}
	}
	return true
}

// ApplyCommand is emitted after dependency closure. Applications must apply
// Ready.Apply strictly in slice order.
type ApplyCommand struct {
	Ref     InstanceRef
	Seq     uint64
	Deps    []InstanceNum
	Command Command
}

// CloneInto deep-copies c into dst while reusing destination capacity.
func (c ApplyCommand) CloneInto(dst *ApplyCommand) {
	deps := cloneSliceInto(dst.Deps, c.Deps)
	command := dst.Command
	c.Command.CloneInto(&command)
	*dst = c
	dst.Deps = deps
	dst.Command = command
}

// Clone returns a deep copy of the apply command.
func (c ApplyCommand) Clone() ApplyCommand {
	var out ApplyCommand
	c.CloneInto(&out)
	return out
}

// RecordLoadResult is the embedding's response to a Ready.RecordLoads request.
type RecordLoadResult struct {
	Ref    InstanceRef
	Record InstanceRecord
	Found  bool
}

// Ready batches deterministic effects for the embedding. Apply is already
// dependency-ordered and must not be reordered or applied in parallel.
type Ready struct {
	HardState         HardState
	ConfigHistory     []ConfigHistoryEntry
	Records           []InstanceRecord
	BootstrapRecords  []BootstrapRecord
	LocalVoterState   *LocalVoterState
	FrontierUpdates   []FrontierUpdate
	AllocatorFloor    InstanceNum
	Messages          []Message
	BootstrapMessages []BootstrapMessage
	Apply             []ApplyCommand
	Snapshot          *Snapshot
	Checkpoint        *CheckpointRequest
	Compact           []CompactionRange
	// RecordLoads requests durable records for folded refs. Sorted,
	// deduplicated, and stable until Advance.
	RecordLoads []InstanceRef
	MustSync    bool
}

// Empty reports whether the ready batch contains no work.
func (r Ready) Empty() bool {
	return r.HardState.Empty() && len(r.ConfigHistory) == 0 && len(r.Records) == 0 &&
		len(r.BootstrapRecords) == 0 && r.LocalVoterState == nil && len(r.FrontierUpdates) == 0 &&
		r.AllocatorFloor == 0 && len(r.Messages) == 0 && len(r.BootstrapMessages) == 0 &&
		len(r.Apply) == 0 && len(r.RecordLoads) == 0 && r.Snapshot == nil && r.Checkpoint == nil &&
		len(r.Compact) == 0 && !r.MustSync
}

// CloneInto deep-copies r into dst while reusing destination capacity.
func (r Ready) CloneInto(dst *Ready) {
	hardState := dst.HardState
	r.HardState.CloneInto(&hardState)
	sourceHistory := r.ConfigHistory
	if slicesPartiallyOverlap(dst.ConfigHistory, sourceHistory) {
		sourceHistory = cloneConfigHistory(sourceHistory)
	}
	history := dst.ConfigHistory
	if cap(history) < len(sourceHistory) {
		history = make([]ConfigHistoryEntry, len(sourceHistory))
	} else {
		history = history[:len(sourceHistory)]
	}
	for i := range sourceHistory {
		history[i] = sourceHistory[i].Clone()
	}
	clear(history[len(history):cap(history)])

	sourceRecords := r.Records
	if slicesPartiallyOverlap(dst.Records, sourceRecords) {
		snapshot := make([]InstanceRecord, len(sourceRecords))
		for i := range sourceRecords {
			sourceRecords[i].CloneInto(&snapshot[i])
		}
		sourceRecords = snapshot
	}
	records := dst.Records
	if cap(records) < len(sourceRecords) {
		records = make([]InstanceRecord, len(sourceRecords))
	} else {
		records = records[:len(sourceRecords)]
	}
	for i := range sourceRecords {
		sourceRecords[i].CloneInto(&records[i])
	}
	fullRecords := records[:cap(records)]
	for i := len(records); i < len(fullRecords); i++ {
		fullRecords[i] = InstanceRecord{}
	}
	sourceBootstrapRecords := r.BootstrapRecords
	if slicesPartiallyOverlap(dst.BootstrapRecords, sourceBootstrapRecords) {
		sourceBootstrapRecords = cloneBootstrapRecords(sourceBootstrapRecords)
	}
	bootstrapRecords := dst.BootstrapRecords
	if cap(bootstrapRecords) < len(sourceBootstrapRecords) {
		bootstrapRecords = make([]BootstrapRecord, len(sourceBootstrapRecords))
	} else {
		bootstrapRecords = bootstrapRecords[:len(sourceBootstrapRecords)]
	}
	for i := range sourceBootstrapRecords {
		bootstrapRecords[i] = sourceBootstrapRecords[i].Clone()
	}
	clear(bootstrapRecords[len(bootstrapRecords):cap(bootstrapRecords)])

	var localVoterState *LocalVoterState
	if r.LocalVoterState != nil {
		state := r.LocalVoterState.Clone()
		localVoterState = &state
	}

	sourceFrontiers := r.FrontierUpdates
	if slicesPartiallyOverlap(dst.FrontierUpdates, sourceFrontiers) {
		sourceFrontiers = cloneFrontierUpdates(sourceFrontiers)
	}
	frontiers := dst.FrontierUpdates
	if cap(frontiers) < len(sourceFrontiers) {
		frontiers = make([]FrontierUpdate, len(sourceFrontiers))
	} else {
		frontiers = frontiers[:len(sourceFrontiers)]
	}
	for i := range sourceFrontiers {
		frontiers[i] = sourceFrontiers[i].Clone()
	}
	clear(frontiers[len(frontiers):cap(frontiers)])

	sourceMessages := r.Messages
	if slicesPartiallyOverlap(dst.Messages, sourceMessages) {
		snapshot := make([]Message, len(sourceMessages))
		for i := range sourceMessages {
			sourceMessages[i].CloneInto(&snapshot[i])
		}
		sourceMessages = snapshot
	}
	messages := dst.Messages
	if cap(messages) < len(sourceMessages) {
		messages = make([]Message, len(sourceMessages))
	} else {
		messages = messages[:len(sourceMessages)]
	}
	for i := range sourceMessages {
		sourceMessages[i].CloneInto(&messages[i])
	}
	fullMessages := messages[:cap(messages)]
	for i := len(messages); i < len(fullMessages); i++ {
		fullMessages[i] = Message{}
	}
	sourceBootstrapMessages := r.BootstrapMessages
	if slicesPartiallyOverlap(dst.BootstrapMessages, sourceBootstrapMessages) {
		snapshot := make([]BootstrapMessage, len(sourceBootstrapMessages))
		for i := range sourceBootstrapMessages {
			snapshot[i] = sourceBootstrapMessages[i].Clone()
		}
		sourceBootstrapMessages = snapshot
	}
	bootstrapMessages := dst.BootstrapMessages
	if cap(bootstrapMessages) < len(sourceBootstrapMessages) {
		bootstrapMessages = make([]BootstrapMessage, len(sourceBootstrapMessages))
	} else {
		bootstrapMessages = bootstrapMessages[:len(sourceBootstrapMessages)]
	}
	for i := range sourceBootstrapMessages {
		bootstrapMessages[i] = sourceBootstrapMessages[i].Clone()
	}
	clear(bootstrapMessages[len(bootstrapMessages):cap(bootstrapMessages)])

	sourceApply := r.Apply
	if slicesPartiallyOverlap(dst.Apply, sourceApply) {
		snapshot := make([]ApplyCommand, len(sourceApply))
		for i := range sourceApply {
			sourceApply[i].CloneInto(&snapshot[i])
		}
		sourceApply = snapshot
	}
	apply := dst.Apply
	if cap(apply) < len(sourceApply) {
		apply = make([]ApplyCommand, len(sourceApply))
	} else {
		apply = apply[:len(sourceApply)]
	}
	for i := range sourceApply {
		sourceApply[i].CloneInto(&apply[i])
	}
	fullApply := apply[:cap(apply)]
	for i := len(apply); i < len(fullApply); i++ {
		fullApply[i] = ApplyCommand{}
	}

	sourceLoads := r.RecordLoads
	if slicesPartiallyOverlap(dst.RecordLoads, sourceLoads) {
		sourceLoads = append([]InstanceRef(nil), sourceLoads...)
	}
	recordLoads := dst.RecordLoads
	if cap(recordLoads) < len(sourceLoads) {
		recordLoads = make([]InstanceRef, len(sourceLoads))
	} else {
		recordLoads = recordLoads[:len(sourceLoads)]
	}
	copy(recordLoads, sourceLoads)
	clear(recordLoads[len(recordLoads):cap(recordLoads)])

	var snapshotWork *Snapshot
	if r.Snapshot != nil {
		cloned := r.Snapshot.Clone()
		snapshotWork = &cloned
	}
	var checkpointRequest *CheckpointRequest
	if r.Checkpoint != nil {
		cloned := r.Checkpoint.Clone()
		checkpointRequest = &cloned
	}
	compact := cloneSliceInto(dst.Compact, r.Compact)

	*dst = Ready{
		HardState:         hardState,
		ConfigHistory:     history,
		Records:           records,
		BootstrapRecords:  bootstrapRecords,
		LocalVoterState:   localVoterState,
		FrontierUpdates:   frontiers,
		AllocatorFloor:    r.AllocatorFloor,
		Messages:          messages,
		BootstrapMessages: bootstrapMessages,
		Apply:             apply,
		Snapshot:          snapshotWork,
		Checkpoint:        checkpointRequest,
		Compact:           compact,
		RecordLoads:       recordLoads,
		MustSync:          r.MustSync,
	}
}

// Clone returns a deep copy of the Ready batch.
func (r Ready) Clone() Ready {
	var out Ready
	r.CloneInto(&out)
	return out
}

// Release clears references and capacities held by a caller-owned Ready. It is
// cleanup only: Ready values are not pooled or returned to package ownership.
func (r *Ready) Release() {
	for i := range r.Records {
		r.Records[i] = InstanceRecord{}
	}
	for i := range r.ConfigHistory {
		r.ConfigHistory[i] = ConfigHistoryEntry{}
	}
	for i := range r.Messages {
		r.Messages[i].Reset()
	}
	for i := range r.BootstrapRecords {
		r.BootstrapRecords[i] = BootstrapRecord{}
	}
	if r.LocalVoterState != nil {
		*r.LocalVoterState = LocalVoterState{}
	}
	for i := range r.FrontierUpdates {
		r.FrontierUpdates[i] = FrontierUpdate{}
	}
	for i := range r.BootstrapMessages {
		r.BootstrapMessages[i] = BootstrapMessage{}
	}
	for i := range r.Apply {
		r.Apply[i] = ApplyCommand{}
	}
	if r.Snapshot != nil {
		*r.Snapshot = Snapshot{}
	}
	if r.Checkpoint != nil {
		*r.Checkpoint = CheckpointRequest{}
	}
	clear(r.Compact)
	r.HardState = HardState{}
	r.Records = nil
	r.ConfigHistory = nil
	r.Messages = nil
	r.BootstrapRecords = nil
	r.LocalVoterState = nil
	r.FrontierUpdates = nil
	r.AllocatorFloor = 0
	r.Apply = nil
	r.BootstrapMessages = nil
	r.Snapshot = nil
	r.Checkpoint = nil
	r.Compact = nil
	r.MustSync = false
}

// RuntimeStats is an allocation-free snapshot of bounded runtime ownership.
// Ready record/message counts include bootstrap records/messages in their
// respective totals. Ages are measured only in deterministic logical ticks.
type RuntimeStats struct {
	ResidentInstances        int
	ExecutedRefs             int
	DeferredPreAccepts       int
	ActiveRecoveries         int
	FrozenReadyRecords       int
	FrozenReadyMessages      int
	PendingReadyRecords      int
	PendingReadyMessages     int
	NextReadyRecords         int
	NextReadyMessages        int
	OldestUnexecutedAgeTicks uint64
	// RecordLoadMisses counts ProvideRecordLoad results with Found=false.
	RecordLoadMisses uint64
	// PayloadStubInstances counts residents with dropped command payloads.
	PayloadStubInstances uint64
	// FoldedInstances counts instances folded out of the resident map.
	FoldedInstances uint64
}

// StatusSnapshot is a copy-only view of node state for diagnostics and tests.
// Executed instance records omit Command.Payload; durable storage remains the
// authority for full executed commands.
type StatusSnapshot struct {
	ID        ReplicaID
	Tick      uint64
	Conf      ConfState
	Instances []InstanceRecord
	Executed  []InstanceRef
	// TOQAvailable reports whether this TOQ-configured node can originate new
	// local proposals with the current configuration.
	TOQAvailable bool
}

var (
	// ErrInvalidConfig reports an unusable node configuration.
	ErrInvalidConfig = errors.New("epaxos: invalid config")
	// ErrInvalidMessage reports a malformed transport message.
	ErrInvalidMessage = errors.New("epaxos: invalid message")
	// ErrChecksumMismatch reports a failed BLAKE3 checksum validation.
	ErrChecksumMismatch = errors.New("epaxos: checksum mismatch")
	// ErrMessageRejected reports a stale, duplicate, or non-local message.
	ErrMessageRejected = errors.New("epaxos: message rejected")
	// ErrTOQConfigUnavailable reports that local TOQ runtime inputs do not cover
	// the exact current configuration. It is retryable after RefreshTOQConfig.
	ErrTOQConfigUnavailable = errors.New("epaxos: TOQ config unavailable")
	// ErrTOQClockRollback reports a caller clock sample below the last accepted
	// TOQ sample or reconstructed durable closed-bucket floor.
	ErrTOQClockRollback = errors.New("epaxos: TOQ clock rollback")
	// ErrTOQTimestampOverflow reports an unrepresentable TOQ timestamp bound.
	ErrTOQTimestampOverflow = errors.New("epaxos: TOQ timestamp overflow")
	// ErrLogicalTimeExhausted reports an unrepresentable logical tick or timer deadline.
	ErrLogicalTimeExhausted = errors.New("epaxos: logical time exhausted")
	// ErrDeferredQueueFull reports that the bounded timed PreAccept queue is full.
	ErrDeferredQueueFull = errors.New("epaxos: deferred PreAccept queue full")
	// ErrDeferredRecordLoadFull reports that the deferred record-load queue is full.
	ErrDeferredRecordLoadFull = errors.New("epaxos: deferred record load queue full")
	// ErrUnrequestedRecordLoad reports ProvideRecordLoad for a ref not pending.
	ErrUnrequestedRecordLoad = errors.New("epaxos: unrequested record load")
	// ErrInvalidRecord reports a record that fails checksum validation.
	ErrInvalidRecord = errors.New("epaxos: invalid record")
	// ErrResidentInstancesExceeded reports Propose backpressure when the resident set is too large.
	ErrResidentInstancesExceeded = errors.New("epaxos: resident instances exceed configured bound")
	// ErrBallotExhausted reports that no strictly greater ballot is representable.
	ErrBallotExhausted = errors.New("epaxos: ballot exhausted")
	// ErrInstanceExhausted reports that no further local instance number is representable.
	ErrInstanceExhausted = errors.New("epaxos: local instance number exhausted")
	// ErrUnknownInstance reports a request for an instance not known locally.
	ErrUnknownInstance = errors.New("epaxos: unknown instance")
	// ErrInvalidReady reports an acknowledgement that does not match the outstanding Ready batch.
	ErrInvalidReady = errors.New("epaxos: invalid ready")
)
