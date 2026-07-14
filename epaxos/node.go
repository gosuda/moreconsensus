package epaxos

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

type phase uint8

const (
	phaseIdle phase = iota
	phasePreAccept
	phaseAccept
	phasePrepare
	phaseTryPreAccept
	phaseCommitted
)

type tryCandidate struct {
	rec    InstanceRecord
	voters voterMask
}

type tryEvidenceKey struct {
	target   InstanceRef
	conflict InstanceRef
	ballot   Ballot
}

type instance struct {
	rec             InstanceRecord
	phase           phase
	preOK           *attrVoteSet
	accOK           voterMask
	prepareOK       *recordVoteSet
	tryOK           voterMask
	tryDeferred     map[InstanceRef]struct{}
	prepareEvidence *recordVoteSet
	tryIgnored      map[InstanceRef]struct{}
	createdTick     uint64
	waitDeadline    uint64
	processAt       uint64
	generation      uint64
}

type timerKind uint8

const (
	timerPreAccept timerKind = iota + 1
	timerAccept
	timerPrepare
	timerTryPreAccept
	timerFastWait
)

type timer struct {
	deadline uint64
	ref      InstanceRef
	kind     timerKind
	gen      uint64
	index    int
}

type preAcceptDomain uint8

const (
	preAcceptLogical preAcceptDomain = iota + 1
	preAcceptTOQ
)

type deferredPreAcceptKey struct {
	domain preAcceptDomain
	ref    InstanceRef
	ballot Ballot
	from   ReplicaID
}

type recordLoadWait struct {
	messages []Message
	// requested is true once the ref has been placed into a Ready.RecordLoads batch.
	requested bool
}

type deferredPreAccept struct {
	key     deferredPreAcceptKey
	message Message
	index   int
}

type deferredPreAcceptHeap []*deferredPreAccept

func (h deferredPreAcceptHeap) Len() int { return len(h) }
func (h deferredPreAcceptHeap) Less(i, j int) bool {
	a, b := h[i].message, h[j].message
	if preAcceptMessageLess(a, b) {
		return true
	}
	if preAcceptMessageLess(b, a) {
		return false
	}
	if a.Ballot.Epoch != b.Ballot.Epoch {
		return a.Ballot.Epoch < b.Ballot.Epoch
	}
	if a.Ballot.Number != b.Ballot.Number {
		return a.Ballot.Number < b.Ballot.Number
	}
	return a.Ballot.Replica < b.Ballot.Replica
}
func (h deferredPreAcceptHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *deferredPreAcceptHeap) Push(x any) {
	entry := x.(*deferredPreAccept)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *deferredPreAcceptHeap) Pop() any {
	old := *h
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	entry.index = -1
	*h = old[:last]
	return entry
}

type toqRuntime struct {
	conf   ConfState
	delays map[ReplicaID]uint64
	group  []ReplicaID
}

type timerHeap []timer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline != h[j].deadline {
		return h[i].deadline < h[j].deadline
	}
	if h[i].ref.Conf != h[j].ref.Conf {
		return h[i].ref.Conf < h[j].ref.Conf
	}
	if h[i].ref.Replica != h[j].ref.Replica {
		return h[i].ref.Replica < h[j].ref.Replica
	}
	if h[i].ref.Instance != h[j].ref.Instance {
		return h[i].ref.Instance < h[j].ref.Instance
	}
	if h[i].kind != h[j].kind {
		return h[i].kind < h[j].kind
	}
	return h[i].gen < h[j].gen
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *timerHeap) Push(x any) {
	entry := x.(timer)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *timerHeap) Pop() any {
	old := *h
	last := len(old) - 1
	entry := old[last]
	old[last] = timer{}
	entry.index = -1
	*h = old[:last]
	return entry
}

// RawNode is a deterministic raft-like EPaxos state machine.
type RawNode struct {
	id                  ReplicaID
	q                   quorum
	confHistory         map[ConfID]ConfState
	storage             Storage
	zeroCopy            bool
	cluster             ClusterID
	localIdentity       VoterIdentity
	voterIdentities     map[ReplicaID]VoterIdentity
	localVoter          LocalVoterState
	durableLocalVoter   LocalVoterState
	bootstrapPlans      map[BootstrapID]*bootstrapState
	bootstrapByBase     map[ConfID]BootstrapID
	closedConfigs       map[ConfID]ClosedConfig
	allocatorFloor      InstanceNum
	bootstrapDurability []bootstrapDurabilityAction

	retryTicks            uint64
	recoveryTicks         uint64
	timeOptimization      bool
	timeOptimizationTicks uint64
	toqEnabled            bool
	toqClock              func() uint64
	toqRuntimes           map[ConfID]toqRuntime
	toqActive             *toqRuntime
	toqSeen               bool
	toqLastNow            uint64
	toqClosed             bool
	toqClosedThrough      uint64
	maxDeferredPreAccepts int
	maxReadyMessages      int

	tick                            uint64
	currentHardState                HardState
	acknowledgedHardState           HardState
	nextInstance                    InstanceNum
	instances                       map[InstanceRef]*instance
	engine                          conflictEngine
	executed                        executedTracker
	pendingConf                     bool
	appliedConfByBase               map[ConfID]InstanceRef
	timers                          timerHeap
	pendingReady                    Ready
	nextReady                       Ready
	frozenReady                     Ready
	awaitAdvance                    bool
	logicalPreAccepts               deferredPreAcceptHeap
	toqPreAccepts                   deferredPreAcceptHeap
	deferredIndex                   map[deferredPreAcceptKey]*deferredPreAccept
	// pendingRecordLoads defers inbound messages until ProvideRecordLoad for a folded ref.
	pendingRecordLoads              map[InstanceRef]*recordLoadWait
	pendingRecordLoadMessages       int
	maxDeferredRecordLoads          int
	recordLoadMisses                uint64
	retainExecutedPerLane           int
	maxResidentInstances            int
	payloadStubInstances            uint64
	foldedInstances                 uint64
	tryEvidenceChecks               map[tryEvidenceKey]map[ReplicaID]InstanceRecord
	committedThrough                map[instanceLane]InstanceNum
	maxDependencyRecoveriesPerDrive int
	maxConcurrentRecoveries         int
	dependencyRecoveryStartsLeft    int
	driveDepth                      int
	lastRecoverySource              recoverySource
	hasRecoverySource               bool
	executionWorkspace              executionWorkspace
}

func timingDomainFromMessage(message Message) TimingDomain {
	if message.TOQ {
		return TimingDomainTOQ
	}
	if message.ProcessAt != 0 {
		return TimingDomainLogical
	}
	return TimingDomainUntimed
}

func recordUsesTOQ(record InstanceRecord) bool {
	return record.TimingDomain == TimingDomainTOQ
}

func (n *RawNode) timingDomainEnabled(domain TimingDomain) bool {
	switch domain {
	case TimingDomainUntimed:
		return !n.timeOptimization && !n.toqEnabled
	case TimingDomainLogical:
		return n.timeOptimization
	case TimingDomainTOQ:
		return n.toqEnabled
	default:
		return false
	}
}

func messageCarriesValue(message Message) bool {
	switch message.Type {
	case MsgPreAccept, MsgAccept, MsgCommit, MsgTryPreAccept:
		return true
	case MsgPrepareResp, MsgEvidenceResp:
		return message.RecordStatus >= StatusPreAccepted
	case MsgTryPreAcceptResp:
		return (!message.Reject && message.RecordStatus >= StatusPreAccepted) ||
			(message.Reject && message.RejectReason == RejectAcceptedTarget)
	case MsgPreAcceptResp, MsgAcceptResp, MsgPrepare, MsgEvidence:
		fallthrough
	default:
		return false
	}
}

func (n *RawNode) messageTimingDomainEnabled(message Message) bool {
	domain := timingDomainFromMessage(message)
	if n.timingDomainEnabled(domain) || message.Command.Kind == CommandNoop || message.Command.Kind == CommandMembership {
		return true
	}
	if domain != TimingDomainUntimed {
		return false
	}
	switch message.Type {
	case MsgAccept, MsgCommit:
	case MsgPreAccept, MsgPreAcceptResp, MsgAcceptResp, MsgPrepare, MsgPrepareResp, MsgTryPreAccept, MsgTryPreAcceptResp, MsgEvidence, MsgEvidenceResp:
		fallthrough
	default:
		return false
	}
	current := n.instances[message.Ref]
	return current != nil &&
		current.rec.TimingDomain != TimingDomainUntimed &&
		commandEqual(current.rec.Command, message.Command)
}

func configureTOQ(cfg Config) (func() uint64, error) {
	if !cfg.TOQ {
		if cfg.TOQClock != nil || cfg.TOQRuntime != nil {
			return nil, fmt.Errorf("%w: TOQ inputs require TOQ mode", ErrInvalidConfig)
		}
		return nil, nil
	}
	if cfg.TimeOptimization {
		return nil, fmt.Errorf("%w: TOQ and TimeOptimization are separate modes", ErrInvalidConfig)
	}
	if cfg.TOQClock == nil {
		return nil, fmt.Errorf("%w: TOQ requires a caller-managed clock", ErrInvalidConfig)
	}
	return cfg.TOQClock, nil
}

func normalizeTOQRuntime(runtime TOQRuntimeConfig, local ReplicaID) (toqRuntime, error) {
	if runtime.Conf.ID == 0 {
		return toqRuntime{}, fmt.Errorf("%w: TOQ runtime configuration id is zero", ErrInvalidConfig)
	}
	q, err := newQuorum(runtime.Conf.Voters)
	if err != nil {
		return toqRuntime{}, err
	}
	q.conf.ID = runtime.Conf.ID
	group := append([]ReplicaID(nil), runtime.SyncGroup...)
	if len(group) == 0 {
		group = append(group, q.conf.Voters...)
	}
	delays := make(map[ReplicaID]uint64, len(runtime.OneWayDelay)+1)
	for id, delay := range runtime.OneWayDelay {
		if !q.contains(id) {
			return toqRuntime{}, fmt.Errorf("%w: TOQ delay contains non-voter %d", ErrInvalidConfig, id)
		}
		delays[id] = delay
	}
	seen := make(map[ReplicaID]struct{}, len(group))
	for _, id := range group {
		if !q.contains(id) {
			return toqRuntime{}, fmt.Errorf("%w: TOQ sync group contains non-voter %d", ErrInvalidConfig, id)
		}
		if _, duplicate := seen[id]; duplicate {
			return toqRuntime{}, fmt.Errorf("%w: TOQ sync group duplicates voter %d", ErrInvalidConfig, id)
		}
		seen[id] = struct{}{}
		if _, ok := delays[id]; !ok {
			if id != local {
				return toqRuntime{}, fmt.Errorf("%w: TOQ sync group is missing delay for voter %d", ErrInvalidConfig, id)
			}
			delays[id] = 0
		}
	}
	return toqRuntime{conf: q.conf.Clone(), delays: delays, group: group}, nil
}

func (n *RawNode) activateTOQRuntime(conf ConfState) {
	if !n.toqEnabled {
		return
	}
	runtime, ok := n.toqRuntimes[conf.ID]
	if !ok || !sameReplicaIDs(runtime.conf.Voters, conf.Voters) {
		n.toqActive = nil
		return
	}
	active := runtime
	n.toqActive = &active
}

// RefreshTOQConfig validates, clones, and stages exact timing inputs for one
// configuration. Refreshing operational data does not mutate consensus state.
func (n *RawNode) RefreshTOQConfig(runtime TOQRuntimeConfig) error {
	if !n.toqEnabled {
		return fmt.Errorf("%w: node is not in TOQ mode", ErrInvalidConfig)
	}
	normalized, err := normalizeTOQRuntime(runtime, n.id)
	if err != nil {
		return err
	}
	if known, ok := n.confHistory[normalized.conf.ID]; ok {
		if !sameReplicaIDs(known.Voters, normalized.conf.Voters) {
			return fmt.Errorf("%w: %w: voters do not match known configuration %d", ErrMessageRejected, ErrTOQConfigUnavailable, normalized.conf.ID)
		}
	} else if normalized.conf.ID <= n.q.conf.ID {
		return fmt.Errorf("%w: %w: unknown non-future configuration %d", ErrMessageRejected, ErrTOQConfigUnavailable, normalized.conf.ID)
	}
	n.toqRuntimes[normalized.conf.ID] = normalized
	if normalized.conf.ID == n.q.conf.ID {
		n.activateTOQRuntime(n.q.conf)
	}
	return nil
}

func checkedLogicalAdd(now, after uint64) (uint64, error) {
	if ^uint64(0)-now < after {
		return 0, ErrLogicalTimeExhausted
	}
	return now + after, nil
}

func checkedTOQAdd(now, delay uint64) (uint64, error) {
	if ^uint64(0)-now < delay {
		return 0, ErrTOQTimestampOverflow
	}
	return now + delay, nil
}

func checkedLogicalDeadline(now, after uint64) (uint64, error) {
	if after == 0 {
		after = 1
	}
	return checkedLogicalAdd(now, after)
}

func (n *RawNode) sampleTOQClock() (uint64, error) {
	now := n.toqClock()
	if n.toqSeen && now < n.toqLastNow {
		return 0, ErrTOQClockRollback
	}
	return now, nil
}

func (n *RawNode) nextTOQProcessAt(now uint64) (uint64, error) {
	if n.toqActive == nil {
		return 0, fmt.Errorf("%w: %w", ErrMessageRejected, ErrTOQConfigUnavailable)
	}
	maxDelay := uint64(0)
	for _, id := range n.toqActive.group {
		if delay := n.toqActive.delays[id]; delay > maxDelay {
			maxDelay = delay
		}
	}
	processAt, err := checkedTOQAdd(now, maxDelay)
	if err != nil {
		return 0, err
	}
	if n.toqClosed && processAt <= n.toqClosedThrough {
		processAt, err = checkedTOQAdd(n.toqClosedThrough, 1)
		if err != nil {
			return 0, err
		}
	}
	return processAt, nil
}

func (n *RawNode) localTOQPreAcceptMessage(inst *instance) Message {
	return Message{Type: MsgPreAccept, From: n.id, To: n.id, Ref: inst.rec.Ref, ProcessAt: inst.rec.ProcessAt, TOQ: true, Ballot: inst.rec.Ballot, Command: inst.rec.Command.Borrow(), RecordStatus: inst.rec.Status}
}

func saturatingSeqIncrement(value uint64) uint64 {
	if value == ^uint64(0) {
		return value
	}
	return value + 1
}

func recordHasValue(status Status) bool {
	return status >= StatusPreAccepted
}

func recordTupleBallot(rec InstanceRecord) Ballot {
	return rec.RecordBallot
}

func commandValidForConfiguration(command Command, ref InstanceRef, conf ConfState) bool {
	switch command.Kind {
	case CommandUser:
		return true
	case CommandNoop:
		return command.ID == (CommandID{}) && len(command.Payload) == 0 && len(command.ConflictKeys) == 0
	case CommandConfChange:
		if ref.Conf == ^ConfID(0) || len(command.Payload) != 9 {
			return false
		}
		change := ConfChange{
			Type:    ConfChangeType(command.Payload[0]),
			Replica: ReplicaID(binary.LittleEndian.Uint64(command.Payload[1:])),
		}
		_, err := confChangeQuorumFrom(conf, change)
		return err == nil
	case CommandMembership:
		return membershipCommandValidForRef(command, ref, conf)
	default:
		return false
	}
}

func confStateIsZero(conf ConfState) bool {
	return conf.ID == 0 && len(conf.Voters) == 0
}

func validateConfChangeResult(rec InstanceRecord) error {
	invalid := func(reason string) error {
		return fmt.Errorf("%w: durable record %s %s", ErrInvalidConfig, rec.Ref, reason)
	}
	result := rec.ConfChangeResult
	if (rec.Command.Kind != CommandConfChange && rec.Command.Kind != CommandMembership) || rec.Status != StatusExecuted {
		if result.Outcome != ConfChangeOutcomeUnspecified || !confStateIsZero(result.Conf) {
			return invalid("has a configuration result outside an executed configuration command")
		}
		return nil
	}
	if rec.Command.Kind == CommandMembership {
		if err := validateMembershipResult(rec); err != nil {
			return err
		}
		if rec.MembershipResult.Outcome == BootstrapOutcomeUnspecified {
			if result.Outcome != ConfChangeOutcomeUnspecified || !confStateIsZero(result.Conf) {
				return invalid("nonterminal membership command has a configuration result")
			}
			return nil
		}
		if rec.MembershipResult.Outcome == BootstrapOutcomeAborted {
			if result.Outcome != ConfChangeOutcomeUnspecified || !confStateIsZero(result.Conf) {
				return invalid("aborted membership command changed configuration")
			}
			return nil
		}
	}
	switch result.Outcome {
	case ConfChangeApplied:
		if rec.Ref.Conf == ^ConfID(0) || result.Conf.ID != rec.Ref.Conf+1 {
			return invalid("has an invalid applied successor id")
		}
		q, err := newQuorum(result.Conf.Voters)
		if err != nil || !sameReplicaIDs(q.conf.Voters, result.Conf.Voters) {
			return invalid("has non-canonical applied successor voters")
		}
	case ConfChangeRejectedSuperseded, ConfChangeRejectedInvalid:
		if !confStateIsZero(result.Conf) {
			return invalid("has configuration voters on a rejected result")
		}
	case ConfChangeOutcomeUnspecified:
		fallthrough
	default:
		return invalid("has no terminal configuration result")
	}
	return nil
}

func validateStoredInstanceRecord(rec InstanceRecord, conf ConfState) error {
	invalid := func(reason string) error {
		return fmt.Errorf("%w: durable record %s %s", ErrInvalidConfig, rec.Ref, reason)
	}
	if rec.Ref.Replica == 0 || rec.Ref.Instance == 0 || rec.Ref.Conf == 0 {
		return invalid("has an incomplete reference")
	}
	if conf.ID != rec.Ref.Conf || !conf.Contains(rec.Ref.Replica) {
		return invalid("is outside its pinned configuration")
	}
	if rec.Status > StatusExecuted {
		return invalid("has an unknown status")
	}
	if err := validateMembershipResult(rec); err != nil {
		return err
	}
	if !recordTimingInvariant(rec) {
		return invalid("has invalid timing metadata")
	}
	commandValid := commandValidForConfiguration(rec.Command, rec.Ref, conf)
	if !commandValid && (rec.Command.Kind != CommandConfChange ||
		rec.Status != StatusExecuted ||
		rec.ConfChangeResult.Outcome != ConfChangeRejectedInvalid) {
		return invalid("has an invalid command")
	}
	if err := validateConfChangeResult(rec); err != nil {
		return err
	}
	if len(rec.Deps) != len(conf.Voters) {
		return invalid("has the wrong dependency width")
	}
	validBallot := func(ballot Ballot, allowZero bool) bool {
		if ballot == (Ballot{}) {
			return allowZero
		}
		return ballot.Replica != 0 && conf.Contains(ballot.Replica)
	}
	if !validBallot(rec.Ballot, rec.Status == StatusNone && !rec.TOQPending) {
		return invalid("has an invalid promise ballot")
	}
	if rec.Status >= StatusPreAccepted {
		if rec.Seq == 0 || !validBallot(rec.RecordBallot, false) || rec.Ballot.Less(rec.RecordBallot) {
			return invalid("has an invalid chosen tuple")
		}
	} else if rec.RecordBallot != (Ballot{}) || rec.Seq != 0 {
		return invalid("has value metadata without a value")
	}
	hasAcceptAttrs := rec.AcceptSeq != 0 || len(rec.AcceptDeps) != 0
	if (rec.AcceptSeq == 0) != (len(rec.AcceptDeps) == 0) || (len(rec.AcceptDeps) != 0 && len(rec.AcceptDeps) != len(conf.Voters)) {
		return invalid("has malformed accept attributes")
	}
	if rec.Status < StatusAccepted && (hasAcceptAttrs || len(rec.AcceptEvidence) != 0) {
		return invalid("has accept evidence before Accepted")
	}
	if len(rec.AcceptEvidence) > len(conf.Voters) {
		return invalid("has too many sender accept evidence entries")
	}
	seenEvidence := make(map[ReplicaID]struct{}, len(rec.AcceptEvidence))
	for _, evidence := range rec.AcceptEvidence {
		if evidence.Sender == 0 || !conf.Contains(evidence.Sender) || evidence.Seq == 0 || len(evidence.Deps) != len(conf.Voters) {
			return invalid("has malformed sender accept evidence")
		}
		if _, duplicate := seenEvidence[evidence.Sender]; duplicate {
			return invalid("has duplicate sender accept evidence")
		}
		seenEvidence[evidence.Sender] = struct{}{}
	}
	if rec.FastPathEligible && (rec.Status != StatusPreAccepted || !rec.RecordBallot.IsInitialFor(rec.Ref.Replica)) {
		return invalid("has impossible fast-path eligibility")
	}
	if rec.TOQPending {
		if rec.Status != StatusNone || !rec.Ballot.IsInitialFor(rec.Ref.Replica) || rec.FastPathEligible || hasAcceptAttrs || len(rec.AcceptEvidence) != 0 || rec.Command.Kind == CommandNoop {
			return invalid("has malformed TOQ-pending state")
		}
	} else if rec.Status == StatusNone {
		if rec.ProcessAt != 0 || rec.FastPathEligible || rec.Command.ID != (CommandID{}) || rec.Command.Kind != CommandUser || len(rec.Command.Payload) != 0 || len(rec.Command.ConflictKeys) != 0 {
			return invalid("has a value without durable status")
		}
	}
	return nil
}

// NewRawNode constructs a deterministic EPaxos node from config and storage.
func NewRawNode(cfg Config) (*RawNode, error) {
	q, err := newQuorum(cfg.Voters)
	if err != nil {
		return nil, err
	}
	if !q.contains(cfg.ID) && cfg.Cluster == (ClusterID{}) {
		return nil, fmt.Errorf("%w: local id is not a voter", ErrInvalidConfig)
	}
	if cfg.RetryTicks == 0 {
		cfg.RetryTicks = 3
	}
	if cfg.RecoveryTicks == 0 {
		var err error
		cfg.RecoveryTicks, err = checkedLogicalAdd(cfg.RetryTicks, cfg.RetryTicks)
		if err == nil {
			cfg.RecoveryTicks, err = checkedLogicalAdd(cfg.RecoveryTicks, cfg.RetryTicks)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: default RecoveryTicks overflows", ErrInvalidConfig)
		}
	}
	if cfg.TimeOptimizationTicks == 0 {
		cfg.TimeOptimizationTicks = 1
	}
	if cfg.MaxDeferredPreAccepts < 0 {
		return nil, fmt.Errorf("%w: negative deferred PreAccept limit", ErrInvalidConfig)
	}
	if cfg.MaxDeferredPreAccepts == 0 {
		cfg.MaxDeferredPreAccepts = 4096
	}
	if cfg.MaxDeferredRecordLoads < 0 {
		return nil, fmt.Errorf("%w: MaxDeferredRecordLoads must be non-negative", ErrInvalidConfig)
	}
	if cfg.MaxDeferredRecordLoads == 0 {
		cfg.MaxDeferredRecordLoads = 256
	}
	if cfg.RetainExecutedPerLane < 0 {
		return nil, fmt.Errorf("%w: RetainExecutedPerLane must be non-negative", ErrInvalidConfig)
	}
	if cfg.RetainExecutedPerLane == 0 {
		cfg.RetainExecutedPerLane = 1024
	}
	if cfg.MaxResidentInstances < 0 {
		return nil, fmt.Errorf("%w: MaxResidentInstances must be non-negative", ErrInvalidConfig)
	}
	if cfg.LegacyProcessAtDomain != nil && *cfg.LegacyProcessAtDomain > TimingDomainTOQ {
		return nil, fmt.Errorf("%w: unknown legacy ProcessAt timing domain", ErrInvalidConfig)
	}
	if cfg.MaxDependencyRecoveriesPerDrive < 0 {
		return nil, fmt.Errorf("%w: negative dependency recovery drive limit", ErrInvalidConfig)
	}
	if cfg.MaxDependencyRecoveriesPerDrive == 0 {
		cfg.MaxDependencyRecoveriesPerDrive = defaultMaxDependencyRecoveriesPerDrive
	}
	if cfg.MaxConcurrentRecoveries < 0 {
		return nil, fmt.Errorf("%w: negative concurrent recovery limit", ErrInvalidConfig)
	}
	if cfg.MaxConcurrentRecoveries == 0 {
		cfg.MaxConcurrentRecoveries = defaultMaxConcurrentRecoveries
	}
	toqClock, err := configureTOQ(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
	}
	n := &RawNode{
		id:                              cfg.ID,
		q:                               q,
		storage:                         cfg.Storage,
		zeroCopy:                        cfg.ZeroCopyProposals,
		cluster:                         cfg.Cluster,
		localIdentity:                   cfg.LocalIdentity.Clone(),
		voterIdentities:                 make(map[ReplicaID]VoterIdentity),
		retryTicks:                      cfg.RetryTicks,
		recoveryTicks:                   cfg.RecoveryTicks,
		timeOptimization:                cfg.TimeOptimization,
		timeOptimizationTicks:           cfg.TimeOptimizationTicks,
		toqEnabled:                      cfg.TOQ,
		toqClock:                        toqClock,
		toqRuntimes:                     make(map[ConfID]toqRuntime),
		maxDeferredPreAccepts:           cfg.MaxDeferredPreAccepts,
		maxDeferredRecordLoads:          cfg.MaxDeferredRecordLoads,
		retainExecutedPerLane:           cfg.RetainExecutedPerLane,
		maxResidentInstances:            cfg.MaxResidentInstances,
		pendingRecordLoads:              make(map[InstanceRef]*recordLoadWait),
		maxReadyMessages:                cfg.MaxReadyMessages,
		maxDependencyRecoveriesPerDrive: cfg.MaxDependencyRecoveriesPerDrive,
		maxConcurrentRecoveries:         cfg.MaxConcurrentRecoveries,
		nextInstance:                    1,
		instances:                       make(map[InstanceRef]*instance),
		executed:                        newExecutedTracker(),
		committedThrough:                make(map[instanceLane]InstanceNum),
		appliedConfByBase:               make(map[ConfID]InstanceRef),
		confHistory:                     make(map[ConfID]ConfState),
		deferredIndex:                   make(map[deferredPreAcceptKey]*deferredPreAccept),
		bootstrapPlans:                  make(map[BootstrapID]*bootstrapState),
		bootstrapByBase:                 make(map[ConfID]BootstrapID),
		closedConfigs:                   make(map[ConfID]ClosedConfig),
	}
	q.conf.ID = normalizeConfID(q.conf.ID)
	n.confHistory[q.conf.ID] = q.conf.Clone()
	state, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, err
	}
	hs := state.HardState.Clone()
	legacyHardState := hs.Empty()
	if !hs.Empty() {
		if hs.Conf.ID == 0 || len(hs.Conf.Voters) == 0 {
			return nil, fmt.Errorf("%w: durable hard state has incomplete configuration", ErrInvalidConfig)
		}
		loaded, err := newQuorum(hs.Conf.Voters)
		if err != nil {
			return nil, err
		}
		loaded.conf.ID = normalizeConfID(hs.Conf.ID)
		if err := n.rememberConf(loaded.conf); err != nil {
			return nil, err
		}
		n.q = loaded
		hs.Conf = loaded.conf.Clone()
	}
	for _, entry := range state.ConfigHistory {
		stored := entry.Conf
		loaded, err := newQuorum(stored.Voters)
		if err != nil {
			return nil, err
		}
		loaded.conf.ID = normalizeConfID(stored.ID)
		if err := n.rememberConf(loaded.conf); err != nil {
			return nil, err
		}
		if !entry.AppliedRef.IsZero() {
			if existing := n.appliedConfByBase[entry.AppliedRef.Conf]; !existing.IsZero() && existing != entry.AppliedRef {
				return nil, fmt.Errorf("%w: conflicting configuration winner for base %d", ErrInvalidConfig, entry.AppliedRef.Conf)
			}
			n.appliedConfByBase[entry.AppliedRef.Conf] = entry.AppliedRef
		}
		if loaded.conf.ID > n.q.conf.ID {
			n.q = loaded
		}
	}
	n.q.conf.ID = normalizeConfID(n.q.conf.ID)
	n.tick = hs.Tick
	n.allocatorFloor = state.AllocatorFloor
	if n.allocatorFloor != 0 && n.allocatorFloor > n.nextInstance {
		n.nextInstance = n.allocatorFloor
	}
	n.toqClosedThrough = state.TOQClosedThrough
	n.toqClosed = state.TOQClosedThrough != 0
	loadedRefs := make(map[InstanceRef]struct{})
	loadedRecords := make([]InstanceRecord, 0)
	if cfg.Cluster != (ClusterID{}) {
		if !cfg.LocalIdentity.valid() || cfg.LocalIdentity.Replica != cfg.ID ||
			len(cfg.VoterIdentities) != len(n.q.conf.Voters) {
			return nil, fmt.Errorf("%w: complete current voter identities required", ErrInvalidConfig)
		}
		for i, voter := range n.q.conf.Voters {
			identity := cfg.VoterIdentities[i]
			if identity.Replica != voter || !identity.valid() || (i > 0 && cfg.VoterIdentities[i-1].Replica >= identity.Replica) {
				return nil, fmt.Errorf("%w: noncanonical voter identities", ErrInvalidConfig)
			}
			n.voterIdentities[voter] = identity.Clone()
		}
		if n.q.conf.Contains(cfg.ID) {
			if identity, ok := n.voterIdentities[cfg.ID]; !ok || !voterIdentityEqual(identity, cfg.LocalIdentity) {
				return nil, fmt.Errorf("%w: local identity is not canonical for current membership", ErrInvalidConfig)
			}
		}
		n.localVoter = state.LocalVoterState.Clone()
		if !n.localVoter.IsEligible(cfg.ID, n.q.conf) || n.localVoter.Cluster != cfg.Cluster ||
			!voterIdentityEqual(n.localVoter.Identity, cfg.LocalIdentity) {
			return nil, fmt.Errorf("%w: current membership lacks durable local eligibility", ErrBootstrapEligibility)
		}
	} else {
		n.localIdentity = VoterIdentity{Replica: cfg.ID, Incarnation: 1}
		n.localVoter = LocalVoterState{
			Identity:       n.localIdentity.Clone(),
			Conf:           n.q.conf.Clone(),
			Status:         LocalVoterStatusEligible,
			AllocatorFloor: n.nextInstance,
		}
	}
	n.durableLocalVoter = n.localVoter.Clone()
	if state.LocalVoterState.AllocatorFloor > n.nextInstance {
		n.nextInstance = state.LocalVoterState.AllocatorFloor
	}
	for _, record := range state.BootstrapRecords {
		if err := n.restoreBootstrapRecord(record); err != nil {
			return nil, err
		}
	}

	if err := cfg.Storage.LoadInstances(func(rec InstanceRecord) error {
		if rec.TimingDomain > TimingDomainTOQ {
			return fmt.Errorf("%w: durable record %s has unknown timing domain", ErrInvalidConfig, rec.Ref)
		}
		if verifyCanonicalRecordChecksumBytes(rec) {
			if !recordTimingInvariant(rec) {
				return fmt.Errorf("%w: durable record %s has invalid timing metadata", ErrInvalidConfig, rec.Ref)
			}
		} else {
			if !VerifyRecordChecksumWithoutMembershipResult(rec) &&
				!VerifyRecordChecksumWithoutTimingDomain(rec) && !VerifyRecordChecksumWithoutConfChangeResult(rec) {
				return ErrChecksumMismatch
			}
			if rec.Command.Kind == CommandNoop { //nolint:gocritic // ifElseChain: status dispatch keeps explicit ordering
				if cfg.LegacyProcessAtDomain == nil {
					return fmt.Errorf("%w: durable no-op record %s has ambiguous legacy timing domain", ErrInvalidConfig, rec.Ref)
				}
				legacyDomain := *cfg.LegacyProcessAtDomain
				legacyPending := rec.TOQPending
				if (legacyPending && legacyDomain != TimingDomainTOQ) ||
					(!legacyPending && rec.ProcessAt != 0 && legacyDomain == TimingDomainUntimed) {
					return fmt.Errorf("%w: durable no-op record %s conflicts with legacy timing policy", ErrInvalidConfig, rec.Ref)
				}
				if legacyPending {
					if rec.Status != StatusNone || !rec.Ballot.IsInitialFor(rec.Ref.Replica) || rec.RecordBallot != (Ballot{}) || rec.Seq != 0 {
						return fmt.Errorf("%w: durable no-op record %s has malformed legacy pending state", ErrInvalidConfig, rec.Ref)
					}
					rec.Status = StatusPreAccepted
					rec.RecordBallot = rec.Ballot
					rec.Seq = 1
					for i := range rec.Deps {
						rec.Deps[i] = 0
					}
					rec.FastPathEligible = true
				}
				rec.ProcessAt = 0
				rec.TimingDomain = TimingDomainUntimed
				rec.TOQPending = false
			} else if rec.TOQPending {
				rec.TimingDomain = TimingDomainTOQ
			} else {
				if cfg.LegacyProcessAtDomain == nil {
					return fmt.Errorf("%w: durable record %s has ambiguous legacy timing domain", ErrInvalidConfig, rec.Ref)
				}
				rec.TimingDomain = *cfg.LegacyProcessAtDomain
			}
			if !recordTimingInvariant(rec) {
				return fmt.Errorf("%w: durable record %s has invalid migrated timing metadata", ErrInvalidConfig, rec.Ref)
			}
			rec.Checksum = ChecksumRecord(rec)
			n.enqueueRecord(rec)
		}
		if _, duplicate := loadedRefs[rec.Ref]; duplicate {
			return fmt.Errorf("%w: duplicate durable record %s", ErrInvalidConfig, rec.Ref)
		}
		loadedRefs[rec.Ref] = struct{}{}
		loadedRecords = append(loadedRecords, rec.Clone())
		return nil
	}); err != nil {
		return nil, err
	}
	if legacyHardState {
		for _, rec := range loadedRecords {
			if rec.TimingDomain == TimingDomainLogical && rec.ProcessAt > n.tick {
				n.tick = rec.ProcessAt
			}
		}
	}
	if err := n.replayExecutedConfigRecords(loadedRecords); err != nil {
		return nil, err
	}
	if n.cluster == (ClusterID{}) {
		n.localVoter.Conf = n.q.conf.Clone()
		n.localVoter.Status = LocalVoterStatusEligible
	}
	sort.Slice(loadedRecords, func(i, j int) bool {
		return lessRef(loadedRecords[i].Ref, loadedRecords[j].Ref)
	})
	if cfg.TOQRuntime != nil {
		if err := n.RefreshTOQConfig(*cfg.TOQRuntime); err != nil {
			return nil, err
		}
	} else {
		n.activateTOQRuntime(n.q.conf)
	}
	for _, rec := range loadedRecords {
		conf, ok := n.confHistory[rec.Ref.Conf]
		if !ok {
			return nil, fmt.Errorf("%w: durable record %s references unknown configuration", ErrInvalidConfig, rec.Ref)
		}
		if err := validateStoredInstanceRecord(rec, conf); err != nil {
			return nil, err
		}
		if rec.Command.Kind != CommandNoop && rec.Command.Kind != CommandMembership && rec.Status >= StatusPreAccepted && !n.timingDomainEnabled(rec.TimingDomain) {
			return nil, fmt.Errorf("%w: durable record %s uses an incompatible timing domain", ErrInvalidConfig, rec.Ref)
		}
		if rec.TOQPending {
			if !n.toqEnabled {
				return nil, fmt.Errorf("%w: TOQ-pending record requires TOQ", ErrInvalidConfig)
			}
			if rec.Ref.Replica != n.id {
				return nil, fmt.Errorf("%w: foreign TOQ-pending record %s", ErrInvalidConfig, rec.Ref)
			}
		}
		phase := phaseFromStatus(rec.Status)
		if rec.TOQPending {
			phase = phasePreAccept
		} else if rec.Status < StatusCommitted && rec.Ballot.IsRecovery() && rec.Ballot.Replica == n.id {
			phase = phasePrepare
		}
		inst := &instance{rec: rec.Clone(), phase: phase, processAt: rec.ProcessAt}
		n.seedLocalRestartVote(inst)
		n.installInstance(inst)
		n.observeInstanceRef(rec.Ref)
		if rec.Ref.Replica == n.id && rec.Ref.Instance >= n.nextInstance {
			n.observeInstanceRef(rec.Ref)
		}
		if rec.Status >= StatusPreAccepted && !rec.TOQPending {
			if rec.TimingDomain == TimingDomainTOQ && n.toqEnabled && (!n.toqClosed || rec.ProcessAt > n.toqClosedThrough) {
				n.toqClosed = true
				n.toqClosedThrough = rec.ProcessAt
				n.toqSeen = true
				n.toqLastNow = rec.ProcessAt
			}
		}
		n.markPendingConf(rec)
	}
	heap.Init(&n.timers)
	heap.Init(&n.logicalPreAccepts)
	heap.Init(&n.toqPreAccepts)
	restartTimerError := func(err error) error {
		if errors.Is(err, ErrLogicalTimeExhausted) && n.tick == ^uint64(0) {
			return nil
		}
		return err
	}
	for _, rec := range loadedRecords {
		inst := n.instances[rec.Ref]
		if inst == nil || inst.rec.Status >= StatusCommitted {
			continue
		}
		if inst.rec.TOQPending {
			if inst.rec.Ref.Replica == n.id {
				if _, err := n.admitDeferredPreAccept(preAcceptTOQ, n.localTOQPreAcceptMessage(inst)); err != nil {
					return nil, err
				}
				if err := restartTimerError(n.schedule(inst, timerPreAccept, n.retryTicks)); err != nil {
					return nil, err
				}
			}
			continue
		}
		switch inst.phase {
		case phaseIdle:
			if inst.rec.Status == StatusNone && inst.rec.Ref.Replica == n.id {
				if err := restartTimerError(n.startPrepare(inst)); err != nil {
					return nil, err
				}
			} else if err := restartTimerError(n.scheduleRecovery(inst)); err != nil {
				return nil, err
			}
		case phasePreAccept:
			if inst.rec.Ref.Replica == n.id && inst.rec.Ballot.IsInitialFor(n.id) {
				if err := restartTimerError(n.schedule(inst, timerPreAccept, n.retryTicks)); err != nil {
					return nil, err
				}
			} else if err := restartTimerError(n.scheduleRecovery(inst)); err != nil {
				return nil, err
			}
		case phaseAccept:
			if inst.rec.Ref.Replica == n.id && inst.rec.Ballot.Replica == n.id {
				if err := restartTimerError(n.schedule(inst, timerAccept, n.retryTicks)); err != nil {
					return nil, err
				}
			} else if err := restartTimerError(n.scheduleRecovery(inst)); err != nil {
				return nil, err
			}
		case phasePrepare:
			if err := restartTimerError(n.schedule(inst, timerPrepare, n.recoveryTicks)); err != nil {
				return nil, err
			}
		case phaseTryPreAccept, phaseCommitted:
		}
	}
	n.beginDrive()
	n.tryExecute()
	n.endDrive()
	n.currentHardState = HardState{Conf: n.q.conf.Clone(), Tick: n.tick}
	n.acknowledgedHardState = hs.Clone()
	if n.allocatorFloor == 0 || n.nextInstance > n.allocatorFloor {
		n.allocatorFloor = n.nextInstance
	}
	return n, nil
}

func normalizeConfID(id ConfID) ConfID {
	if id == 0 {
		return 1
	}
	return id
}

func (n *RawNode) rememberConf(conf ConfState) error {
	conf.ID = normalizeConfID(conf.ID)
	if existing, ok := n.confHistory[conf.ID]; ok {
		if !sameReplicaIDs(existing.Voters, conf.Voters) {
			return fmt.Errorf("%w: conflicting voters for config %d", ErrInvalidConfig, conf.ID)
		}
		return nil
	}
	n.confHistory[conf.ID] = conf.Clone()
	return nil
}

func sameReplicaIDs(a, b []ReplicaID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] { //nolint:gosec // G602: index is guarded by matching slice lengths.
			return false
		}
	}
	return true
}

func phaseFromStatus(s Status) phase {
	switch s {
	case StatusPreAccepted:
		return phasePreAccept
	case StatusAccepted:
		return phaseAccept
	case StatusCommitted, StatusExecuted:
		return phaseCommitted
	case StatusNone:
		fallthrough
	default:
		return phaseIdle
	}
}

func (n *RawNode) observeInstanceRef(ref InstanceRef) {
	if ref.Replica != n.id || n.nextInstance == 0 || ref.Instance < n.nextInstance {
		return
	}
	next, ok := instanceSuccessor(ref.Instance)
	if !ok {
		n.nextInstance = 0
		return
	}
	n.nextInstance = next
}

func (n *RawNode) confFor(id ConfID) ConfState {
	conf, _ := n.lookupConf(id)
	return conf
}

func (n *RawNode) lookupConf(id ConfID) (ConfState, bool) {
	conf, ok := n.confHistory[id]
	return conf, ok
}

func (n *RawNode) depsForConf(id ConfID) []InstanceNum {
	return make([]InstanceNum, len(n.confFor(id).Voters))
}

func (n *RawNode) seedLocalRestartVote(inst *instance) {
	if inst == nil || !n.confFor(inst.rec.Ref.Conf).Contains(n.id) {
		return
	}
	switch inst.phase {
	case phasePreAccept:
		if inst.rec.Ref.Replica != n.id ||
			inst.rec.Status != StatusPreAccepted ||
			inst.rec.TOQPending ||
			!inst.rec.Ballot.IsInitialFor(n.id) {
			return
		}
		if inst.preOK == nil {
			inst.preOK = getAttrVoteSet()
		}
		attrs := inst.rec.Attributes()
		// Restart cannot reconstruct volatile fast-quorum dependency coverage; count the durable owner vote only for the slow path.
		vote, ok := newAttrVote(n.confFor(inst.rec.Ref.Conf), attrs.Seq, attrs.Deps, 0, false)
		if ok {
			inst.preOK.add(n.confFor(inst.rec.Ref.Conf), n.id, vote)
		}
	case phaseAccept:
		if inst.rec.Ref.Replica != n.id ||
			inst.rec.Status != StatusAccepted ||
			inst.rec.Ballot.Replica != n.id {
			return
		}
		inst.accOK.add(n.confFor(inst.rec.Ref.Conf), n.id)
	case phasePrepare:
		if inst.rec.Status >= StatusCommitted ||
			!inst.rec.Ballot.IsRecovery() ||
			inst.rec.Ballot.Replica != n.id {
			return
		}
		inst.prepareOK = getRecordVoteSet()
		inst.prepareOK.add(n.confFor(inst.rec.Ref.Conf), n.id, inst.rec)
	case phaseIdle, phaseTryPreAccept, phaseCommitted:
	}
}

func (n *RawNode) votersForConf(id ConfID) []ReplicaID {
	return n.confFor(id).Voters
}

func (n *RawNode) slowQuorumForConf(id ConfID) int {
	return slowQuorumSize(len(n.votersForConf(id)))
}

func (n *RawNode) fastQuorumForConf(id ConfID) int {
	return fastQuorumSize(len(n.votersForConf(id)))
}

func (n *RawNode) tryWitnessQuorumForConf(id ConfID) int {
	return tryWitnessQuorumSize(len(n.votersForConf(id)))
}

func (n *RawNode) ensureRecordDeps(rec *InstanceRecord) bool {
	width := len(n.confFor(rec.Ref.Conf).Voters)
	if len(rec.Deps) == width {
		return false
	}
	deps := make([]InstanceNum, width)
	copy(deps, rec.Deps)
	rec.Deps = deps
	return true
}

func confChangeSuccessor(rec InstanceRecord, base ConfState) (ConfState, bool) {
	if !commandValidForConfiguration(rec.Command, rec.Ref, base) {
		return ConfState{}, false
	}
	cc := ConfChange{
		Type:    ConfChangeType(rec.Command.Payload[0]),
		Replica: ReplicaID(binary.LittleEndian.Uint64(rec.Command.Payload[1:])),
	}
	q, err := confChangeQuorumFrom(base, cc)
	if err != nil {
		return ConfState{}, false
	}
	q.conf.ID = rec.Ref.Conf + 1
	return q.conf, true
}

func (n *RawNode) rememberAppliedConf(ref InstanceRef, successor ConfState) error {
	if existing, duplicate := n.appliedConfByBase[ref.Conf]; duplicate {
		if existing == ref {
			return n.rememberConf(successor)
		}
		return fmt.Errorf("%w: multiple applied configuration commands for base %d", ErrInvalidConfig, ref.Conf)
	}
	if err := n.rememberConf(successor); err != nil {
		return err
	}
	n.appliedConfByBase[ref.Conf] = ref
	return nil
}

func legacyConfigExecutionOrder(records []InstanceRecord, indices []int, base ConfState) []int {
	ordered := append([]int(nil), indices...)
	sort.Slice(ordered, func(i, j int) bool {
		return lessRef(records[ordered[i]].Ref, records[ordered[j]].Ref)
	})
	byRef := make(map[InstanceRef]int, len(ordered))
	for _, recordIndex := range ordered {
		byRef[records[recordIndex].Ref] = recordIndex
	}
	dependsOn := func(from, to InstanceRecord) bool {
		if from.Ref == to.Ref {
			return false
		}
		slot, ok := base.Index(to.Ref.Replica)
		return ok && slot < len(from.Deps) && from.Deps[slot] >= to.Ref.Instance
	}
	dependencyKnownAfter := func(from, dependency InstanceRecord) bool {
		return dependency.Seq > from.Seq && dependsOn(dependency, from)
	}

	index := make(map[int]int, len(ordered))
	low := make(map[int]int, len(ordered))
	onStack := make(map[int]bool, len(ordered))
	stack := make([]int, 0, len(ordered))
	components := make([][]int, 0, len(ordered))
	next := 0
	var visit func(int)
	visit = func(recordIndex int) {
		index[recordIndex] = next
		low[recordIndex] = next
		next++
		stack = append(stack, recordIndex)
		onStack[recordIndex] = true
		from := records[recordIndex]
		for _, dependencyIndex := range ordered {
			dependency := records[dependencyIndex]
			if _, candidate := byRef[dependency.Ref]; !candidate ||
				!dependsOn(from, dependency) ||
				dependencyKnownAfter(from, dependency) {
				continue
			}
			if _, seen := index[dependencyIndex]; !seen {
				visit(dependencyIndex)
				if low[dependencyIndex] < low[recordIndex] {
					low[recordIndex] = low[dependencyIndex]
				}
			} else if onStack[dependencyIndex] && index[dependencyIndex] < low[recordIndex] {
				low[recordIndex] = index[dependencyIndex]
			}
		}
		if low[recordIndex] != index[recordIndex] {
			return
		}
		component := make([]int, 0, 1)
		for {
			member := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[member] = false
			component = append(component, member)
			if member == recordIndex {
				break
			}
		}
		sort.Slice(component, func(i, j int) bool {
			left, right := records[component[i]], records[component[j]]
			if left.Seq != right.Seq {
				return left.Seq < right.Seq
			}
			return lessRef(left.Ref, right.Ref)
		})
		components = append(components, component)
	}
	for _, recordIndex := range ordered {
		if _, seen := index[recordIndex]; !seen {
			visit(recordIndex)
		}
	}
	result := make([]int, 0, len(ordered))
	for _, component := range components {
		result = append(result, component...)
	}
	return result
}

func (n *RawNode) replayExecutedConfigRecords(records []InstanceRecord) error {
	byBase := make(map[ConfID][]int)
	bases := make([]ConfID, 0)
	for i := range records {
		rec := records[i]
		if rec.Status != StatusExecuted || rec.Command.Kind != CommandConfChange {
			continue
		}
		if _, exists := byBase[rec.Ref.Conf]; !exists {
			bases = append(bases, rec.Ref.Conf)
		}
		byBase[rec.Ref.Conf] = append(byBase[rec.Ref.Conf], i)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	for _, baseID := range bases {
		base, known := n.confHistory[baseID]
		if !known {
			return fmt.Errorf("%w: executed configuration command references unknown base %d", ErrInvalidConfig, baseID)
		}
		indices := byBase[baseID]
		sort.Slice(indices, func(i, j int) bool {
			return lessRef(records[indices[i]].Ref, records[indices[j]].Ref)
		})
		appliedIndex := -1
		legacyIndices := make([]int, 0)
		for _, index := range indices {
			rec := records[index]
			switch rec.ConfChangeResult.Outcome {
			case ConfChangeApplied:
				if appliedIndex >= 0 {
					return fmt.Errorf("%w: multiple applied configuration commands for base %d", ErrInvalidConfig, baseID)
				}
				appliedIndex = index
			case ConfChangeRejectedSuperseded, ConfChangeRejectedInvalid:
				if err := validateConfChangeResult(rec); err != nil {
					return err
				}
			case ConfChangeOutcomeUnspecified:
				if !confStateIsZero(rec.ConfChangeResult.Conf) {
					return fmt.Errorf("%w: legacy configuration outcome for %s carries a successor", ErrInvalidConfig, rec.Ref)
				}
				legacyIndices = append(legacyIndices, index)
			default:
				return fmt.Errorf("%w: unknown configuration outcome for %s", ErrInvalidConfig, rec.Ref)
			}
		}

		if appliedIndex >= 0 {
			rec := records[appliedIndex]
			if err := validateConfChangeResult(rec); err != nil {
				return err
			}
			successor, valid := confChangeSuccessor(rec, base)
			if !valid || successor.ID != rec.ConfChangeResult.Conf.ID ||
				!sameReplicaIDs(successor.Voters, rec.ConfChangeResult.Conf.Voters) {
				return fmt.Errorf("%w: applied configuration result for %s does not match its command", ErrInvalidConfig, rec.Ref)
			}
			if err := n.rememberAppliedConf(rec.Ref, successor); err != nil {
				return err
			}
		}

		for _, legacyIndex := range legacyConfigExecutionOrder(records, legacyIndices, base) {
			rec := &records[legacyIndex]
			successor, valid := confChangeSuccessor(*rec, base)
			switch {
			case !valid:
				rec.ConfChangeResult = ConfChangeResult{Outcome: ConfChangeRejectedInvalid}
			case n.appliedConfByBase[baseID] != (InstanceRef{}):
				rec.ConfChangeResult = ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}
			default:
				if existing, exists := n.confHistory[successor.ID]; exists &&
					!sameReplicaIDs(existing.Voters, successor.Voters) {
					return fmt.Errorf("%w: legacy configuration command for %s conflicts with stored successor", ErrInvalidConfig, rec.Ref)
				}
				rec.ConfChangeResult = ConfChangeResult{Outcome: ConfChangeApplied, Conf: successor.Clone()}
				if err := n.rememberAppliedConf(rec.Ref, successor); err != nil {
					return err
				}
			}
			rec.Checksum = ChecksumRecord(*rec)
			n.enqueueRecord(*rec)
		}

		for _, index := range indices {
			rec := records[index]
			successor, valid := confChangeSuccessor(rec, base)
			switch rec.ConfChangeResult.Outcome {
			case ConfChangeRejectedInvalid:
				if valid {
					return fmt.Errorf("%w: rejected-invalid configuration command %s is valid", ErrInvalidConfig, rec.Ref)
				}
			case ConfChangeRejectedSuperseded:
				if !valid {
					return fmt.Errorf("%w: superseded configuration command %s is invalid", ErrInvalidConfig, rec.Ref)
				}
				_, applied := n.appliedConfByBase[baseID]
				_, checkpoint := n.confHistory[successor.ID]
				if !applied && !checkpoint {
					return fmt.Errorf("%w: superseded configuration command %s has no applied successor", ErrInvalidConfig, rec.Ref)
				}
			case ConfChangeOutcomeUnspecified, ConfChangeApplied:
			}
		}
	}

	var highest ConfState
	for _, conf := range n.confHistory {
		if conf.ID > highest.ID {
			highest = conf
		}
	}
	if highest.ID != 0 {
		q, err := newQuorum(highest.Voters)
		if err != nil {
			return err
		}
		q.conf.ID = highest.ID
		n.q = q
	}
	return nil
}

func (n *RawNode) markPendingConf(rec InstanceRecord) {
	if rec.Command.Kind == CommandConfChange && rec.Status < StatusExecuted && (rec.Status >= StatusPreAccepted || rec.TOQPending) {
		n.pendingConf = true
	}
}

func (n *RawNode) refreshPendingConf() {
	n.pendingConf = false
	for ref, inst := range n.instances {
		if n.executed.contains(ref) {
			continue
		}
		n.markPendingConf(inst.rec)
		if n.pendingConf {
			return
		}
	}
}

// Tick advances deterministic logical time by one tick and processes due timers.
// If a due recovery would exhaust the ballot space, Tick stutters without
// changing logical time, timers, instances, or Ready work.
func (n *RawNode) Tick() error {
	next, err := checkedLogicalAdd(n.tick, 1)
	if err != nil {
		return err
	}
	if n.recoveryWouldExhaustAt(next) {
		return ErrBallotExhausted
	}
	if err := n.preflightDueTimers(next); err != nil {
		return err
	}
	if err := n.preflightDeferredPreAccepts(preAcceptLogical, next, next); err != nil {
		return err
	}
	n.beginDrive()
	defer n.endDrive()
	n.tick = next
	n.currentHardState.Tick = next
	if n.recoveryTicks > 0 && n.tick%n.recoveryTicks == 0 {
		n.rebroadcastCommits()
	}
	firstErr := n.processDeferredPreAccepts(preAcceptLogical, n.tick)
	for n.timers.Len() > 0 {
		t := n.timers[0]
		if t.deadline > n.tick {
			break
		}
		heap.Pop(&n.timers)
		inst := n.instances[t.ref]
		if inst == nil || inst.generation != t.gen || inst.phase == phaseCommitted {
			continue
		}
		if err := n.onTimer(inst, t.kind); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	n.tryExecute()
	return firstErr
}

func (n *RawNode) deferPreAccept(m Message) (bool, error) {
	if !m.Ballot.IsInitialFor(m.Ref.Replica) {
		return false, nil
	}
	if current := n.instances[m.Ref]; current != nil && !n.wouldReplaceInstance(m, current, true) {
		return false, nil
	}
	switch {
	case m.TOQ:
		if n.toqClosed && m.ProcessAt <= n.toqClosedThrough {
			return false, nil
		}
		return n.admitDeferredPreAccept(preAcceptTOQ, m)
	case n.timeOptimization && m.ProcessAt > n.tick:
		return n.admitDeferredPreAccept(preAcceptLogical, m)
	default:
		return false, nil
	}
}

func (n *RawNode) preAcceptWouldDefer(m Message) bool {
	if m.Type != MsgPreAccept || !m.Ballot.IsInitialFor(m.Ref.Replica) {
		return false
	}
	if m.TOQ {
		return !n.toqClosed || m.ProcessAt > n.toqClosedThrough
	}
	return n.timeOptimization && m.ProcessAt > n.tick
}

func deferredMessageEqual(a, b Message) bool {
	a.Checksum = [32]byte{}
	b.Checksum = [32]byte{}
	return messageEqual(a, b)
}

func (n *RawNode) admitDeferredPreAccept(domain preAcceptDomain, m Message) (bool, error) {
	key := deferredPreAcceptKey{domain: domain, ref: m.Ref, ballot: m.Ballot, from: m.From}
	if existing := n.deferredIndex[key]; existing != nil {
		if deferredMessageEqual(existing.message, m) {
			return true, nil
		}
		return false, fmt.Errorf("%w: conflicting deferred PreAccept for %s", ErrInvalidMessage, m.Ref)
	}
	if len(n.deferredIndex) >= n.maxDeferredPreAccepts {
		return false, fmt.Errorf("%w: %w", ErrMessageRejected, ErrDeferredQueueFull)
	}
	entry := &deferredPreAccept{key: key, message: m.Clone(), index: -1}
	n.deferredIndex[key] = entry
	if domain == preAcceptTOQ {
		heap.Push(&n.toqPreAccepts, entry)
	} else {
		heap.Push(&n.logicalPreAccepts, entry)
	}
	return true, nil
}

func (n *RawNode) removeDeferredPreAccepts(ref InstanceRef) {
	for key, entry := range n.deferredIndex {
		if key.ref != ref {
			continue
		}
		queue := &n.logicalPreAccepts
		if key.domain == preAcceptTOQ {
			queue = &n.toqPreAccepts
		}
		if entry.index >= 0 {
			heap.Remove(queue, entry.index)
		}
		delete(n.deferredIndex, key)
		entry.message.Reset()
	}
}
func preAcceptOrderBefore(aProcessAt uint64, aRef InstanceRef, bProcessAt uint64, bRef InstanceRef) bool {
	if aProcessAt != bProcessAt {
		return aProcessAt < bProcessAt
	}
	if aRef.Conf != bRef.Conf {
		return aRef.Conf < bRef.Conf
	}
	if aRef.Replica != bRef.Replica {
		return aRef.Replica < bRef.Replica
	}
	return aRef.Instance < bRef.Instance
}

func preAcceptMessageLess(a, b Message) bool {
	if preAcceptOrderBefore(a.ProcessAt, a.Ref, b.ProcessAt, b.Ref) {
		return true
	}
	if preAcceptOrderBefore(b.ProcessAt, b.Ref, a.ProcessAt, a.Ref) {
		return false
	}
	return a.From < b.From
}

func (n *RawNode) preAcceptVotesWouldAdvance(inst *instance) (bool, bool) {
	if inst == nil || inst.rec.TOQPending {
		return false, false
	}
	if _, ok := n.fastCommitAttrsPreAccept(inst); ok {
		return true, false
	}
	if inst.preOK.len() < n.slowQuorumForConf(inst.rec.Ref.Conf) {
		return false, false
	}
	if inst.preOK.len() >= n.fastQuorumForConf(inst.rec.Ref.Conf) || !n.canStillFastCommitPreAccept(inst) {
		return true, n.slowQuorumForConf(inst.rec.Ref.Conf) > 1
	}
	return false, false
}

func (n *RawNode) localTOQPreAcceptWouldAdvance(m Message, inst *instance) (bool, bool) {
	if inst == nil || inst.phase != phasePreAccept || !inst.rec.TOQPending {
		return false, false
	}
	temp := *inst
	temp.rec = inst.rec.Clone()
	attrs := n.computeAttrsAt(temp.rec.Command, temp.rec.Ref, m.ProcessAt, true)
	temp.rec.Status = StatusPreAccepted
	temp.rec.Seq = attrs.Seq
	temp.rec.Deps = attrs.Deps
	temp.rec.FastPathEligible = true
	temp.rec.ProcessAt = m.ProcessAt
	temp.rec.TimingDomain = TimingDomainTOQ
	temp.rec.TOQPending = false
	temp.rec.RecordBallot = temp.rec.Ballot
	var tempVotes attrVoteSet
	if inst.preOK != nil {
		tempVotes = *inst.preOK
	}
	temp.preOK = &tempVotes
	vote, ok := newAttrVote(n.confFor(temp.rec.Ref.Conf), temp.rec.Seq, temp.rec.Deps, n.committedDepsMask(temp.rec.Attributes(), temp.rec.Ref.Conf), true)
	if !ok || !temp.preOK.add(n.confFor(temp.rec.Ref.Conf), n.id, vote) {
		return false, false
	}
	if n.slowQuorumForConf(temp.rec.Ref.Conf) == 1 {
		return true, false
	}
	return n.preAcceptVotesWouldAdvance(&temp)
}

func (n *RawNode) preflightDeferredPreAccepts(domain preAcceptDomain, now, logicalNow uint64) error {
	queue := &n.logicalPreAccepts
	if domain == preAcceptTOQ {
		queue = &n.toqPreAccepts
	}
	for _, entry := range *queue {
		m := entry.message
		if m.ProcessAt > now {
			continue
		}
		inst := n.instances[m.Ref]
		if n.deferredPreAcceptStateError(m) != nil {
			continue
		}
		if m.TOQ && m.From == n.id && m.To == n.id && m.Ref.Replica == n.id {
			advance, retryTimer := n.localTOQPreAcceptWouldAdvance(m, inst)
			if !advance {
				continue
			}
			if retryTimer {
				if _, err := checkedLogicalDeadline(logicalNow, n.retryTicks); err != nil {
					return err
				}
			}
			continue
		}
		if !n.wouldReplaceInstance(m, inst, true) {
			continue
		}
		if _, err := checkedLogicalDeadline(logicalNow, n.recoveryTicks); err != nil {
			return err
		}
	}
	return nil
}

func (n *RawNode) removeDeferredPreAcceptEntry(entry *deferredPreAccept) {
	if entry == nil || n.deferredIndex[entry.key] != entry {
		return
	}
	queue := &n.logicalPreAccepts
	if entry.key.domain == preAcceptTOQ {
		queue = &n.toqPreAccepts
	}
	if entry.index >= 0 {
		heap.Remove(queue, entry.index)
	}
	delete(n.deferredIndex, entry.key)
	entry.message.Reset()
}

func (n *RawNode) detachDeferredPreAcceptEntry(entry *deferredPreAccept) (Message, bool) {
	if entry == nil || n.deferredIndex[entry.key] != entry {
		return Message{}, false
	}
	queue := &n.logicalPreAccepts
	if entry.key.domain == preAcceptTOQ {
		queue = &n.toqPreAccepts
	}
	if entry.index >= 0 {
		heap.Remove(queue, entry.index)
	}
	delete(n.deferredIndex, entry.key)
	message := entry.message
	entry.message = Message{}
	return message, true
}

func (n *RawNode) deferredPreAcceptStateError(m Message) error {
	inst := n.instances[m.Ref]
	if m.TOQ && m.From == n.id && m.To == n.id && m.Ref.Replica == n.id {
		advance, _ := n.localTOQPreAcceptWouldAdvance(m, inst)
		if advance && inst != nil && inst.generation == ^uint64(0) {
			return ErrLogicalTimeExhausted
		}
		return nil
	}
	if inst == nil {
		return nil
	}
	if inst.rec.Status >= StatusCommitted {
		if recordTupleBallot(inst.rec) == m.Ballot &&
			(!commandEqual(inst.rec.Command, m.Command) || inst.rec.ProcessAt != m.ProcessAt || inst.rec.TimingDomain != timingDomainFromMessage(m)) {
			return fmt.Errorf("%w: conflicting committed PreAccept retry for %s", ErrInvalidMessage, m.Ref)
		}
		return nil
	}
	if m.Ballot == inst.rec.Ballot &&
		(!commandEqual(inst.rec.Command, m.Command) || inst.rec.ProcessAt != m.ProcessAt || inst.rec.TimingDomain != timingDomainFromMessage(m)) {
		return fmt.Errorf("%w: conflicting same-ballot PreAccept for %s", ErrInvalidMessage, m.Ref)
	}
	if n.wouldReplaceInstance(m, inst, true) && inst.generation == ^uint64(0) {
		return ErrLogicalTimeExhausted
	}
	return nil
}

func (n *RawNode) processDeferredPreAccepts(domain preAcceptDomain, now uint64) error {
	queue := &n.logicalPreAccepts
	if domain == preAcceptTOQ {
		queue = &n.toqPreAccepts
	}
	var firstErr error
	for queue.Len() > 0 && (*queue)[0].message.ProcessAt <= now {
		entry := (*queue)[0]
		m, ok := n.detachDeferredPreAcceptEntry(entry)
		if !ok {
			continue
		}
		err := n.deferredPreAcceptStateError(m)
		if err == nil {
			if m.TOQ && m.From == n.id && m.To == n.id && m.Ref.Replica == n.id {
				n.handleLocalTOQPreAccept(m)
			} else {
				err = n.handlePreAccept(m, true)
			}
		}
		m.Reset()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ProcessTOQ samples the caller clock once, processes every queued TOQ
// PreAccept at or below that sample in total order, and closes those buckets.
// Tick and Step never call this method implicitly.
func (n *RawNode) ProcessTOQ() error {
	if !n.toqEnabled {
		return fmt.Errorf("%w: node is not in TOQ mode", ErrInvalidConfig)
	}
	now := n.toqClock()
	if n.toqSeen && now < n.toqLastNow {
		return ErrTOQClockRollback
	}
	if err := n.preflightDeferredPreAccepts(preAcceptTOQ, now, n.tick); err != nil {
		return err
	}
	n.beginDrive()
	defer n.endDrive()
	n.toqSeen = true
	n.toqLastNow = now
	firstErr := n.processDeferredPreAccepts(preAcceptTOQ, now)
	if !n.toqClosed || now > n.toqClosedThrough {
		n.toqClosed = true
		n.toqClosedThrough = now
	}
	n.tryExecute()
	return firstErr
}

func (n *RawNode) handleLocalTOQPreAccept(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePreAccept || !inst.rec.TOQPending {
		return
	}
	attrs := n.computeAttrsAt(inst.rec.Command, inst.rec.Ref, m.ProcessAt, true)
	next := inst.rec.Clone()
	next.Status = StatusPreAccepted
	next.Seq = attrs.Seq
	next.Deps = append([]InstanceNum(nil), attrs.Deps...)
	next.FastPathEligible = true
	next.ProcessAt = m.ProcessAt
	next.TOQPending = false
	next.RecordBallot = next.Ballot
	next.Checksum = ChecksumRecord(next)
	n.setInstanceRecord(inst, next)
	inst.processAt = m.ProcessAt
	if inst.preOK == nil {
		inst.preOK = getAttrVoteSet()
	}
	vote, ok := newAttrVote(n.confFor(inst.rec.Ref.Conf), inst.rec.Seq, inst.rec.Deps, n.committedDepsMask(inst.rec.Attributes(), inst.rec.Ref.Conf), inst.rec.FastPathEligible)
	if !ok || !inst.preOK.add(n.confFor(inst.rec.Ref.Conf), n.id, vote) {
		return
	}
	n.enqueueRecord(inst.rec)
	if n.slowQuorumForConf(inst.rec.Ref.Conf) == 1 {
		n.commit(inst, inst.rec.Attributes())
		return
	}
	n.maybeFinalizePreAccept(inst)
}

func (n *RawNode) requireLocalVoter() error {
	eligibility := n.durableLocalVoter
	if n.cluster == (ClusterID{}) {
		eligibility = n.localVoter
	}
	if n.q.contains(n.id) && eligibility.IsEligible(n.id, n.q.conf) {
		return nil
	}
	return fmt.Errorf("%w: %w: local replica %d is not durably eligible in config %d", ErrMessageRejected, ErrBootstrapEligibility, n.id, n.q.conf.ID)
}

func (n *RawNode) requireLocalVoterForConf(conf ConfState) error {
	eligibility := n.durableLocalVoter
	if n.cluster == (ClusterID{}) {
		eligibility = n.localVoter
	}
	identityMatches := eligibility.Identity.Replica == n.id
	if n.cluster != (ClusterID{}) {
		identityMatches = identityMatches &&
			eligibility.Cluster == n.cluster &&
			voterIdentityEqual(eligibility.Identity, n.localIdentity)
	}
	eligible := conf.Contains(n.id) && identityMatches &&
		(eligibility.IsEligible(n.id, conf) ||
			(conf.ID < eligibility.Conf.ID &&
				(eligibility.Status == LocalVoterStatusEligible ||
					eligibility.Status == LocalVoterStatusIneligible)))
	if eligible {
		return nil
	}
	return fmt.Errorf("%w: %w: local replica %d is not durably eligible in config %d", ErrMessageRejected, ErrBootstrapEligibility, n.id, conf.ID)
}

// Propose creates a new local EPaxos instance for an application command.
func (n *RawNode) Propose(cmd Command) (InstanceRef, error) {
	if n.maxResidentInstances > 0 && n.engine.residentCount() >= n.maxResidentInstances {
		return InstanceRef{}, ErrResidentInstancesExceeded
	}
	if cmd.Kind == CommandConfChange {
		return InstanceRef{}, fmt.Errorf("%w: use ProposeConfChange", ErrInvalidConfig)
	}
	if cmd.Kind == CommandMembership {
		return InstanceRef{}, fmt.Errorf("%w: use voter bootstrap API", ErrInvalidConfig)
	}
	if err := n.requireLocalVoter(); err != nil {
		return InstanceRef{}, err
	}
	if n.pendingConf && cmd.Kind != CommandNoop {
		return InstanceRef{}, fmt.Errorf("%w: configuration change pending", ErrMessageRejected)
	}
	if n.toqEnabled && n.toqActive == nil && cmd.Kind != CommandNoop {
		return InstanceRef{}, fmt.Errorf("%w: %w", ErrMessageRejected, ErrTOQConfigUnavailable)
	}
	ref := InstanceRef{Conf: n.q.conf.ID}
	if !commandValidForConfiguration(cmd, ref, n.q.conf) {
		return InstanceRef{}, fmt.Errorf("%w: invalid command kind or no-op payload", ErrInvalidConfig)
	}
	if !n.zeroCopy {
		cmd = cmd.Clone()
	}
	n.beginDrive()
	defer n.endDrive()
	return n.propose(cmd)
}

// ProposeConfChange proposes a validated membership change encoded as a consensus command.
func (n *RawNode) ProposeConfChange(cc ConfChange) (InstanceRef, error) {
	if n.maxResidentInstances > 0 && n.engine.residentCount() >= n.maxResidentInstances {
		return InstanceRef{}, ErrResidentInstancesExceeded
	}
	if cc.Type == ConfChangeAddVoter {
		return InstanceRef{}, ErrVoterCertificateRequired
	}
	if err := n.requireLocalVoter(); err != nil {
		return InstanceRef{}, err
	}
	if n.pendingConf {
		return InstanceRef{}, fmt.Errorf("%w: configuration change already pending", ErrMessageRejected)
	}
	if n.toqEnabled && n.toqActive == nil {
		return InstanceRef{}, fmt.Errorf("%w: %w", ErrMessageRejected, ErrTOQConfigUnavailable)
	}
	if n.q.conf.ID == ^ConfID(0) {
		return InstanceRef{}, fmt.Errorf("%w: configuration id exhausted", ErrInvalidConfig)
	}
	if _, err := n.confChangeQuorum(cc); err != nil {
		return InstanceRef{}, err
	}
	var payload [9]byte
	payload[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(payload[1:], uint64(cc.Replica))
	cmd := Command{Kind: CommandConfChange, Payload: append([]byte(nil), payload[:]...), ConflictKeys: [][]byte{[]byte("\xffconf")}}
	n.beginDrive()
	defer n.endDrive()
	n.pendingConf = true
	ref, err := n.propose(cmd)
	if err != nil {
		n.pendingConf = false
		return InstanceRef{}, err
	}
	return ref, nil
}

func (n *RawNode) confChangeQuorum(cc ConfChange) (quorum, error) {
	return confChangeQuorumFrom(n.q.conf, cc)
}

func confChangeQuorumFrom(conf ConfState, cc ConfChange) (quorum, error) {
	voters := append([]ReplicaID(nil), conf.Voters...)
	switch cc.Type {
	case ConfChangeAddVoter:
		if cc.Replica == 0 {
			return quorum{}, fmt.Errorf("%w: configuration change replica zero", ErrInvalidConfig)
		}
		if conf.Contains(cc.Replica) {
			return quorum{}, fmt.Errorf("%w: voter already exists", ErrInvalidConfig)
		}
		voters = append(voters, cc.Replica)
	case ConfChangeRemoveVoter:
		if !conf.Contains(cc.Replica) {
			return quorum{}, fmt.Errorf("%w: voter not found", ErrInvalidConfig)
		}
		filtered := voters[:0]
		for _, id := range voters {
			if id != cc.Replica {
				filtered = append(filtered, id)
			}
		}
		voters = filtered
	default:
		return quorum{}, fmt.Errorf("%w: unknown configuration change", ErrInvalidConfig)
	}
	q, err := newQuorum(voters)
	if err != nil {
		return quorum{}, err
	}
	return q, nil
}

func (n *RawNode) propose(cmd Command) (InstanceRef, error) {
	ref, nextInstance, err := n.nextLocalRef()
	if err != nil {
		return InstanceRef{}, err
	}
	if _, err := checkedLogicalDeadline(n.tick, n.retryTicks); err != nil {
		return InstanceRef{}, err
	}
	processAt := uint64(0)
	toqNow := uint64(0)
	if n.toqEnabled && cmd.Kind != CommandNoop {
		if len(n.deferredIndex) >= n.maxDeferredPreAccepts {
			return InstanceRef{}, fmt.Errorf("%w: %w", ErrMessageRejected, ErrDeferredQueueFull)
		}
		toqNow, err = n.sampleTOQClock()
		if err != nil {
			return InstanceRef{}, err
		}
		processAt, err = n.nextTOQProcessAt(toqNow)
		if err != nil {
			return InstanceRef{}, err
		}
	} else if n.timeOptimization && cmd.Kind != CommandNoop {
		processAt, err = checkedLogicalAdd(n.tick, n.timeOptimizationTicks)
		if err != nil {
			return InstanceRef{}, err
		}
	}

	if n.toqEnabled && cmd.Kind != CommandNoop {
		rec := InstanceRecord{Ref: ref, Ballot: Ballot{Replica: n.id}, Status: StatusNone, Seq: 0, Deps: n.depsForConf(ref.Conf), Command: cmd, ProcessAt: processAt, TimingDomain: TimingDomainTOQ, TOQPending: true}
		rec.Checksum = ChecksumRecord(rec)
		inst := &instance{rec: rec, phase: phasePreAccept, preOK: getAttrVoteSet(), createdTick: n.tick, processAt: processAt}
		if _, err := n.admitDeferredPreAccept(preAcceptTOQ, n.localTOQPreAcceptMessage(inst)); err != nil {
			putAttrVoteSet(inst.preOK)
			return InstanceRef{}, err
		}
		n.toqSeen = true
		n.toqLastNow = toqNow
		n.nextInstance = nextInstance
		n.installInstance(inst)
		n.enqueueRecord(rec)
		n.broadcastPreAccept(inst)
		if err := n.schedule(inst, timerPreAccept, n.retryTicks); err != nil {
			return InstanceRef{}, err
		}
		return ref, nil
	}

	n.nextInstance = nextInstance

	attrs := n.computeAttrsAt(cmd, ref, processAt, n.timeOptimization && processAt > 0)
	timingDomain := TimingDomainUntimed
	if processAt != 0 {
		timingDomain = TimingDomainLogical
	}
	rec := InstanceRecord{Ref: ref, Ballot: Ballot{Replica: n.id}, RecordBallot: Ballot{Replica: n.id}, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd, FastPathEligible: true, ProcessAt: processAt, TimingDomain: timingDomain}
	rec.Checksum = ChecksumRecord(rec)
	inst := &instance{rec: rec, phase: phasePreAccept, preOK: getAttrVoteSet(), createdTick: n.tick, processAt: processAt}
	vote, ok := newAttrVote(n.confFor(ref.Conf), rec.Seq, rec.Deps, n.committedDepsMask(rec.Attributes(), ref.Conf), rec.FastPathEligible)
	if !ok || !inst.preOK.add(n.confFor(ref.Conf), n.id, vote) {
		putAttrVoteSet(inst.preOK)
		return InstanceRef{}, ErrInvalidConfig
	}
	n.installInstance(inst)
	n.enqueueRecord(rec)
	if n.slowQuorumForConf(ref.Conf) == 1 {
		n.commit(inst, rec.Attributes())
		return ref, nil
	}
	n.broadcastPreAccept(inst)
	if err := n.schedule(inst, timerPreAccept, n.retryTicks); err != nil {
		return InstanceRef{}, err
	}
	return ref, nil
}

func (n *RawNode) nextLocalRef() (InstanceRef, InstanceNum, error) {
	candidate := n.nextInstance
	if candidate == 0 {
		return InstanceRef{}, 0, ErrInstanceExhausted
	}
	for {
		ref := InstanceRef{Replica: n.id, Instance: candidate, Conf: n.q.conf.ID}
		if _, exists := n.instances[ref]; !exists {
			if candidate == ^InstanceNum(0) {
				return ref, 0, nil
			}
			return ref, candidate + 1, nil
		}
		if candidate == ^InstanceNum(0) {
			return InstanceRef{}, 0, ErrInstanceExhausted
		}
		candidate++
	}
}

// Step applies one validated transport message to the local node. It copies
// command bytes before storing them, so callers may reuse decode buffers after
// Step returns.
func (n *RawNode) Step(m Message) error {
	if m.To != n.id {
		return ErrMessageRejected
	}
	conf, ok := n.confHistory[m.Ref.Conf]
	if !ok {
		return ErrMessageRejected
	}
	if err := m.Validate(conf); err != nil {
		return err
	}
	if !commandValidForConfiguration(m.Command, m.Ref, conf) {
		if _, _, control := n.findBootstrapControl(m.Ref); control {
			if _, fenced := n.closedConfigs[m.Ref.Conf]; fenced {
				return ErrBootstrapControl
			}
		}
		return ErrInvalidMessage
	}
	if m.Checksum != ([32]byte{}) && !VerifyMessageChecksum(m) {
		return ErrChecksumMismatch
	}
	if err := n.requireLocalVoterForConf(conf); err != nil {
		return err
	}
	if err := n.admitWhileFenced(m); err != nil {
		return err
	}
	if messageCarriesValue(m) && !n.messageTimingDomainEnabled(m) {
		return ErrMessageRejected
	}
	if m.Type == MsgPreAccept && m.Command.Kind != CommandNoop && m.Command.Kind != CommandMembership && m.Ballot.IsInitialFor(m.Ref.Replica) {
		switch timingDomainFromMessage(m) {
		case TimingDomainTOQ:
			if !n.toqEnabled {
				return ErrMessageRejected
			}
		case TimingDomainLogical:
			if !n.timeOptimization {
				return ErrMessageRejected
			}
		case TimingDomainUntimed:
			if n.toqEnabled {
				return ErrMessageRejected
			}
		}
	}
	if err := n.preflightTimingStep(m); err != nil {
		return err
	}
	if err := n.preflightRecoveryStep(m); err != nil {
		return err
	}
	n.beginDrive()
	defer n.endDrive()
	switch m.Type {
	case MsgPreAccept:
		deferred, err := n.deferPreAccept(m)
		if err != nil {
			return err
		}
		if deferred {
			return nil
		}
		return n.handlePreAccept(m, false)
	case MsgPreAcceptResp:
		return n.handlePreAcceptResp(m)
	case MsgAccept:
		return n.handleAccept(m)
	case MsgAcceptResp:
		return n.handleAcceptResp(m)
	case MsgCommit:
		return n.handleCommit(m)
	case MsgPrepare:
		return n.handlePrepare(m)
	case MsgPrepareResp:
		return n.handlePrepareResp(m)
	case MsgTryPreAccept:
		return n.handleTryPreAccept(m)
	case MsgTryPreAcceptResp:
		return n.handleTryPreAcceptResp(m)
	case MsgEvidence:
		return n.handleEvidence(m)
	case MsgEvidenceResp:
		n.handleEvidenceResp(m)
	}
	return nil
}

// HasReady reports whether Ready has unacknowledged work.
func (n *RawNode) HasReady() bool {
	return n.awaitAdvance || !n.pendingReady.Empty() || !n.currentHardState.Equal(n.acknowledgedHardState)
}

// IsExecuted reports whether ref has executed without allocating a status copy.
func (n *RawNode) IsExecuted(ref InstanceRef) bool {
	return n != nil && n.executed.contains(ref)
}

// RuntimeStats returns an allocation-free snapshot of resident runtime state.
func (n *RawNode) RuntimeStats() RuntimeStats {
	if n == nil {
		return RuntimeStats{}
	}
	stats := RuntimeStats{
		ResidentInstances:    len(n.instances),
		ExecutedRefs:         len(n.executed.exact),
		DeferredPreAccepts:   len(n.deferredIndex),
		ActiveRecoveries:     n.activeRecoveryCount(),
		FrozenReadyRecords:   len(n.frozenReady.Records) + len(n.frozenReady.BootstrapRecords),
		FrozenReadyMessages:  len(n.frozenReady.Messages) + len(n.frozenReady.BootstrapMessages),
		RecordLoadMisses:     n.recordLoadMisses,
		PayloadStubInstances: n.payloadStubInstances,
		FoldedInstances:      n.foldedInstances,
		PendingReadyRecords:  len(n.pendingReady.Records) + len(n.pendingReady.BootstrapRecords),
		PendingReadyMessages: len(n.pendingReady.Messages) + len(n.pendingReady.BootstrapMessages),
		NextReadyRecords:     len(n.nextReady.Records) + len(n.nextReady.BootstrapRecords),
		NextReadyMessages:    len(n.nextReady.Messages) + len(n.nextReady.BootstrapMessages),
	}
	for ref, inst := range n.instances {
		if inst == nil || n.executed.contains(ref) || n.tick < inst.createdTick {
			continue
		}
		age := n.tick - inst.createdTick
		if age > stats.OldestUnexecutedAgeTicks {
			stats.OldestUnexecutedAgeTicks = age
		}
	}
	return stats
}

// Ready returns a safe ownership-independent copy of the next frozen batch.
// Until the entire frozen view is acknowledged, repeated calls return only
// the unacknowledged part of that view. Later work remains isolated.
func (n *RawNode) Ready() Ready {
	var rd Ready
	_ = n.ReadyInto(&rd)
	return rd
}

// ReadyInto freezes the same batch as Ready and clones it into caller-owned
// capacity. It does not acknowledge work. dst must remain caller-owned and may
// be reused for identical retries or later batches.
func (n *RawNode) ReadyInto(dst *Ready) error {
	if dst == nil {
		return ErrInvalidReady
	}
	if !n.awaitAdvance {
		if n.pendingReady.Empty() && n.currentHardState.Equal(n.acknowledgedHardState) {
			(Ready{}).CloneInto(dst)
			return nil
		}
		n.freezeReady()
	}
	n.frozenReady.CloneInto(dst)
	return nil
}

func (n *RawNode) freezeReady() {
	n.frozenReady = n.pendingReady
	n.pendingReady = Ready{}
	if !n.currentHardState.Equal(n.acknowledgedHardState) {
		n.frozenReady.HardState = n.currentHardState.Clone()
	}
	if n.maxReadyMessages > 0 && len(n.frozenReady.Messages) > n.maxReadyMessages {
		messages := n.frozenReady.Messages
		n.frozenReady.Messages = messages[:n.maxReadyMessages:n.maxReadyMessages]
		n.pendingReady.Messages = messages[n.maxReadyMessages:]
	}
	n.frozenReady.MustSync = readyHasDurableUpdate(n.frozenReady)
	n.pendingReady.MustSync = readyHasDurableUpdate(n.pendingReady)
	n.awaitAdvance = true
}

func consumeReadyPrefix[T any](items []T, count int) []T {
	if count <= 0 {
		return items
	}
	if count >= len(items) {
		clear(items)
		return items[:0]
	}
	remaining := copy(items, items[count:])
	clear(items[remaining:])
	return items[:remaining]
}

func readyHasDurableUpdate(rd Ready) bool {
	return !rd.HardState.Empty() || len(rd.ConfigHistory) > 0 || len(rd.Records) > 0 ||
		len(rd.BootstrapRecords) > 0 || rd.LocalVoterState != nil || len(rd.FrontierUpdates) > 0 ||
		rd.AllocatorFloor != 0
}

// Advance acknowledges a prefix of the frozen Ready batch or returns ErrInvalidReady without mutating node state.
func (n *RawNode) Advance(rd Ready) error {
	if !n.awaitAdvance {
		if rd.Empty() {
			return nil
		}
		return ErrInvalidReady
	}
	if err := n.validateReadyAck(rd); err != nil {
		return err
	}
	ackedCommitted := len(rd.Committed)
	if !rd.HardState.Empty() {
		n.acknowledgedHardState = rd.HardState.Clone()
		n.frozenReady.HardState = HardState{}
	}
	n.frozenReady.ConfigHistory = consumeReadyPrefix(n.frozenReady.ConfigHistory, len(rd.ConfigHistory))
	n.frozenReady.Records = consumeReadyPrefix(n.frozenReady.Records, len(rd.Records))
	n.frozenReady.BootstrapRecords = consumeReadyPrefix(n.frozenReady.BootstrapRecords, len(rd.BootstrapRecords))
	if rd.LocalVoterState != nil {
		n.durableLocalVoter = rd.LocalVoterState.Clone()
		n.frozenReady.LocalVoterState = nil
	}
	n.frozenReady.FrontierUpdates = consumeReadyPrefix(n.frozenReady.FrontierUpdates, len(rd.FrontierUpdates))
	if rd.AllocatorFloor != 0 {
		n.frozenReady.AllocatorFloor = 0
	}
	n.frozenReady.Messages = consumeReadyPrefix(n.frozenReady.Messages, len(rd.Messages))
	n.frozenReady.BootstrapMessages = consumeReadyPrefix(n.frozenReady.BootstrapMessages, len(rd.BootstrapMessages))
	n.frozenReady.Committed = consumeReadyPrefix(n.frozenReady.Committed, len(rd.Committed))
	n.frozenReady.RecordLoads = consumeReadyPrefix(n.frozenReady.RecordLoads, len(rd.RecordLoads))
	n.frozenReady.MustSync = readyHasDurableUpdate(n.frozenReady)

	n.enqueueExecutedRecords(rd.Committed[:ackedCommitted])
	n.applyBootstrapDurability(rd.BootstrapRecords)
	n.retireExecuted()

	if n.frozenReady.Empty() {
		n.awaitAdvance = false
		n.recycleFrozenReady()
		n.mergeNextReady()
	}
	return nil
}

func (n *RawNode) validateReadyAck(rd Ready) error {
	if rd.Empty() || rd.MustSync != readyHasDurableUpdate(rd) {
		return ErrInvalidReady
	}
	hasPayload := len(rd.ConfigHistory) > 0 || len(rd.Records) > 0 || len(rd.BootstrapRecords) > 0 ||
		rd.LocalVoterState != nil || len(rd.FrontierUpdates) > 0 || rd.AllocatorFloor != 0 ||
		len(rd.Messages) > 0 || len(rd.BootstrapMessages) > 0 || len(rd.Committed) > 0
	if !rd.HardState.Empty() {
		if n.frozenReady.HardState.Empty() || !rd.HardState.Equal(n.frozenReady.HardState) {
			return ErrInvalidReady
		}
	} else if !n.frozenReady.HardState.Empty() && hasPayload {
		return ErrInvalidReady
	}
	if len(rd.ConfigHistory) > len(n.frozenReady.ConfigHistory) ||
		len(rd.Records) > len(n.frozenReady.Records) ||
		len(rd.BootstrapRecords) > len(n.frozenReady.BootstrapRecords) ||
		len(rd.FrontierUpdates) > len(n.frozenReady.FrontierUpdates) ||
		len(rd.Messages) > len(n.frozenReady.Messages) ||
		len(rd.BootstrapMessages) > len(n.frozenReady.BootstrapMessages) ||
		len(rd.Committed) > len(n.frozenReady.Committed) ||
		len(rd.RecordLoads) > len(n.frozenReady.RecordLoads) ||
		(rd.LocalVoterState != nil && n.frozenReady.LocalVoterState == nil) ||
		(rd.AllocatorFloor != 0 && rd.AllocatorFloor != n.frozenReady.AllocatorFloor) {
		return ErrInvalidReady
	}
	if !rd.HardState.Empty() && rd.HardState.Conf.ID > n.acknowledgedHardState.Conf.ID {
		requiredRecords := 0
		membershipSuccessor := false
		for i, rec := range n.frozenReady.Records {
			if rec.Status != StatusExecuted || rec.ConfChangeResult.Outcome != ConfChangeApplied {
				continue
			}
			successorID := rec.ConfChangeResult.Conf.ID
			if successorID > n.acknowledgedHardState.Conf.ID && successorID <= rd.HardState.Conf.ID {
				requiredRecords = i + 1
				membershipSuccessor = membershipSuccessor || rec.Command.Kind == CommandMembership
			}
		}
		if len(rd.Records) < requiredRecords {
			return ErrInvalidReady
		}
		if membershipSuccessor && (len(rd.ConfigHistory) != len(n.frozenReady.ConfigHistory) ||
			len(rd.BootstrapRecords) != len(n.frozenReady.BootstrapRecords) ||
			rd.LocalVoterState == nil) {
			return ErrInvalidReady
		}
	}
	if len(rd.Messages) > 0 || len(rd.BootstrapMessages) > 0 || len(rd.Committed) > 0 {
		if len(rd.ConfigHistory) != len(n.frozenReady.ConfigHistory) ||
			len(rd.Records) != len(n.frozenReady.Records) ||
			len(rd.BootstrapRecords) != len(n.frozenReady.BootstrapRecords) ||
			len(rd.FrontierUpdates) != len(n.frozenReady.FrontierUpdates) ||
			(n.frozenReady.LocalVoterState != nil && rd.LocalVoterState == nil) ||
			(n.frozenReady.AllocatorFloor != 0 && rd.AllocatorFloor == 0) {
			return ErrInvalidReady
		}
	}
	for i := range rd.ConfigHistory {
		if !configHistoryEntryEqual(rd.ConfigHistory[i], n.frozenReady.ConfigHistory[i]) {
			return ErrInvalidReady
		}
	}
	for i := range rd.Records {
		if !instanceRecordEqual(rd.Records[i], n.frozenReady.Records[i]) {
			return ErrInvalidReady
		}
	}
	for i := range rd.BootstrapRecords {
		if !bootstrapRecordEqual(rd.BootstrapRecords[i], n.frozenReady.BootstrapRecords[i]) {
			return ErrInvalidReady
		}
	}
	if rd.LocalVoterState != nil && !localVoterStateEqual(*rd.LocalVoterState, *n.frozenReady.LocalVoterState) {
		return ErrInvalidReady
	}
	for i := range rd.FrontierUpdates {
		if !frontierUpdateEqual(rd.FrontierUpdates[i], n.frozenReady.FrontierUpdates[i]) {
			return ErrInvalidReady
		}
	}
	for i := range rd.Messages {
		if !messageEqual(rd.Messages[i], n.frozenReady.Messages[i]) {
			return ErrInvalidReady
		}
	}
	for i := range rd.BootstrapMessages {
		if !bootstrapMessageEqual(rd.BootstrapMessages[i], n.frozenReady.BootstrapMessages[i]) {
			return ErrInvalidReady
		}
	}
	for i := range rd.Committed {
		if !committedCommandEqual(rd.Committed[i], n.frozenReady.Committed[i]) {
			return ErrInvalidReady
		}
	}
	return nil
}

func acceptEvidenceEqual(a, b []AcceptEvidence) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Sender != b[i].Sender || a[i].Seq != b[i].Seq || !instanceNumsEqual(a[i].Deps, b[i].Deps) {
			return false
		}
	}
	return true
}

func instanceRecordEqual(a, b InstanceRecord) bool {
	return a.Ref == b.Ref &&
		a.Ballot == b.Ballot &&
		a.RecordBallot == b.RecordBallot &&
		a.Status == b.Status &&
		a.Seq == b.Seq &&
		a.AcceptSeq == b.AcceptSeq &&
		a.FastPathEligible == b.FastPathEligible &&
		a.ProcessAt == b.ProcessAt &&
		a.TimingDomain == b.TimingDomain &&
		a.TOQPending == b.TOQPending &&
		a.ConfChangeResult.Outcome == b.ConfChangeResult.Outcome &&
		membershipResultEqual(a.MembershipResult, b.MembershipResult) &&
		a.ConfChangeResult.Conf.ID == b.ConfChangeResult.Conf.ID &&
		sameReplicaIDs(a.ConfChangeResult.Conf.Voters, b.ConfChangeResult.Conf.Voters) &&
		a.Checksum == b.Checksum &&
		instanceNumsEqual(a.Deps, b.Deps) &&
		instanceNumsEqual(a.AcceptDeps, b.AcceptDeps) &&
		acceptEvidenceEqual(a.AcceptEvidence, b.AcceptEvidence) &&
		commandEqual(a.Command, b.Command)
}

func messageEqual(a, b Message) bool {
	return a.Type == b.Type &&
		a.From == b.From &&
		a.To == b.To &&
		a.Ref == b.Ref &&
		a.Ballot == b.Ballot &&
		a.Seq == b.Seq &&
		a.RecordBallot == b.RecordBallot &&
		a.AcceptSeq == b.AcceptSeq &&
		a.Reject == b.Reject &&
		a.RejectHint == b.RejectHint &&
		a.RejectReason == b.RejectReason &&
		a.ConflictRef == b.ConflictRef &&
		a.ConflictStatus == b.ConflictStatus &&
		a.FastPathEligible == b.FastPathEligible &&
		a.DepsCommitted == b.DepsCommitted &&
		a.RecordStatus == b.RecordStatus &&
		a.Checksum == b.Checksum &&
		a.IgnoreDependency == b.IgnoreDependency &&
		instanceNumsEqual(a.Deps, b.Deps) &&
		instanceNumsEqual(a.AcceptDeps, b.AcceptDeps) &&
		acceptEvidenceEqual(a.AcceptEvidence, b.AcceptEvidence) &&
		a.ProcessAt == b.ProcessAt &&
		a.TOQ == b.TOQ &&
		commandEqual(a.Command, b.Command)
}

func committedCommandEqual(a, b CommittedCommand) bool {
	return a.Ref == b.Ref &&
		a.Seq == b.Seq &&
		instanceNumsEqual(a.Deps, b.Deps) &&
		commandEqual(a.Command, b.Command)
}

func commandEqual(a, b Command) bool {
	if a.ID != b.ID || a.Kind != b.Kind || !bytes.Equal(a.Payload, b.Payload) || len(a.ConflictKeys) != len(b.ConflictKeys) {
		return false
	}
	for i := range a.ConflictKeys {
		if !bytes.Equal(a.ConflictKeys[i], b.ConflictKeys[i]) {
			return false
		}
	}
	return true
}

func instanceNumsEqual(a, b []InstanceNum) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (n *RawNode) enqueueExecutedRecords(committed []CommittedCommand) {
	for _, c := range committed {
		inst := n.instances[c.Ref]
		if inst == nil || inst.rec.Status != StatusExecuted {
			continue
		}
		n.enqueueRecord(inst.rec)
	}
}

// Status returns a copy-only diagnostic snapshot of node state.
func (n *RawNode) Status() StatusSnapshot {
	s := StatusSnapshot{ID: n.id, Tick: n.tick, Conf: n.q.conf.Clone()}
	s.TOQAvailable = n.toqEnabled && n.toqActive != nil
	refs := make([]InstanceRef, 0, len(n.instances))
	for ref := range n.instances {
		refs = append(refs, ref)
	}
	sortRefs(refs)
	for _, ref := range refs {
		s.Instances = append(s.Instances, n.instances[ref].rec.Clone())
		if n.executed.contains(ref) {
			s.Executed = append(s.Executed, ref)
		}
	}
	return s
}

func (n *RawNode) preAcceptRecord(m Message, ordered bool) (InstanceRecord, bool) {
	attrs := n.computeAttrsAt(m.Command, m.Ref, m.ProcessAt, ordered && (m.TOQ || (n.timeOptimization && m.ProcessAt > 0)))
	if !m.TOQ {
		attrs = mergeAttrs(attrs, m.Attributes())
	}
	defaultBallot := m.Ballot.IsInitialFor(m.Ref.Replica)
	timedPreAccept := m.TOQ || (n.timeOptimization && m.ProcessAt > 0)
	liveFastPathEligible := defaultBallot && (!timedPreAccept || ordered)
	rec := InstanceRecord{
		Ref:              m.Ref,
		Ballot:           m.Ballot,
		RecordBallot:     m.Ballot,
		Status:           StatusPreAccepted,
		Seq:              attrs.Seq,
		Deps:             attrs.Deps,
		Command:          inboundCommand(m.Command),
		FastPathEligible: liveFastPathEligible && (m.TOQ || m.Attributes().Equal(attrs)),
		ProcessAt:        m.ProcessAt,
		TimingDomain:     timingDomainFromMessage(m),
	}
	rec.Checksum = ChecksumRecord(rec)
	return rec, liveFastPathEligible
}

func (n *RawNode) handlePreAccept(m Message, ordered bool) error {
	rec, liveFastPathEligible := n.preAcceptRecord(m, ordered)
	old := n.instances[m.Ref]
	if old != nil {
		if old.rec.Status >= StatusCommitted {
			if recordTupleBallot(old.rec) == m.Ballot && (!commandEqual(old.rec.Command, m.Command) || old.rec.ProcessAt != m.ProcessAt || old.rec.TimingDomain != timingDomainFromMessage(m)) {
				return fmt.Errorf("%w: conflicting committed PreAccept retry for %s", ErrInvalidMessage, m.Ref)
			}
			n.sendCommitTo(m.From, old.rec)
			return nil
		}
		if m.Ballot.Less(old.rec.Ballot) {
			n.sendReject(MsgPreAcceptResp, m.From, m.Ref, old.rec.Ballot)
			return nil
		}
		if m.Ballot == old.rec.Ballot &&
			(!commandEqual(old.rec.Command, m.Command) || old.rec.ProcessAt != m.ProcessAt || old.rec.TimingDomain != timingDomainFromMessage(m)) {
			return fmt.Errorf("%w: conflicting same-ballot PreAccept for %s", ErrInvalidMessage, m.Ref)
		}
		if old.rec.Status == StatusAccepted {
			if m.Ballot == old.rec.Ballot {
				resp := Message{Type: MsgPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: old.rec.Ballot, Seq: old.rec.Seq, Deps: old.rec.Deps}
				n.enqueueMessage(resp)
			}
			return nil
		}
		if old.rec.Status == StatusPreAccepted && m.Ballot == old.rec.Ballot && old.rec.TimingDomain == timingDomainFromMessage(m) {
			resp := Message{Type: MsgPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: old.rec.Ballot, Seq: old.rec.Seq, Deps: old.rec.Deps, FastPathEligible: old.rec.FastPathEligible, DepsCommitted: n.committedDepsMask(old.rec.Attributes(), m.Ref.Conf)}
			n.enqueueMessage(resp)
			return nil
		}
	}

	if old != nil && old.rec.Status == StatusPreAccepted && instanceRecordEqual(old.rec, rec) {
		resp := Message{Type: MsgPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, FastPathEligible: liveFastPathEligible, DepsCommitted: n.committedDepsMask(rec.Attributes(), m.Ref.Conf)}
		n.enqueueMessage(resp)
		return nil
	}
	generation := uint64(0)
	if old != nil {
		generation = old.generation + 1
	}
	n.removeDeferredPreAccepts(rec.Ref)
	n.observeInstanceRef(rec.Ref)
	inst := &instance{rec: rec, phase: phaseIdle, processAt: m.ProcessAt, generation: generation}
	n.installInstance(inst)
	n.enqueueRecord(rec)
	n.markPendingConf(rec)
	if err := n.scheduleRecovery(inst); err != nil {
		return err
	}
	resp := Message{Type: MsgPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, FastPathEligible: liveFastPathEligible, DepsCommitted: n.committedDepsMask(rec.Attributes(), m.Ref.Conf)}
	n.enqueueMessage(resp)
	return nil
}

func (n *RawNode) handlePreAcceptResp(m Message) error {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePreAccept || m.Ref.Replica != n.id {
		return nil
	}
	if inst.rec.Ballot.Replica != n.id {
		return nil
	}
	if m.Reject {
		if !inst.rec.Ballot.Less(m.RejectHint) {
			return nil
		}
		return n.startPrepareFrom(inst, m.RejectHint)
	}
	if m.Ballot != inst.rec.Ballot {
		return nil
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	if inst.preOK == nil {
		inst.preOK = getAttrVoteSet()
	}
	if inst.preOK.has(conf, m.From) {
		return nil
	}
	vote, ok := newAttrVote(conf, m.Seq, m.Deps, m.DepsCommitted, m.FastPathEligible)
	if !ok || !inst.preOK.add(conf, m.From, vote) {
		return nil
	}
	n.maybeFinalizePreAccept(inst)
	return nil
}

func (n *RawNode) maybeFinalizePreAccept(inst *instance) {
	if inst == nil || inst.rec.TOQPending {
		return
	}
	attrs := attrsFromVotes(n.confFor(inst.rec.Ref.Conf), inst.preOK)
	if fastAttrs, ok := n.fastCommitAttrsPreAccept(inst); ok {
		n.commit(inst, fastAttrs)
		return
	}
	slowQuorum := n.slowQuorumForConf(inst.rec.Ref.Conf)
	if inst.preOK.len() < slowQuorum {
		return
	}
	if inst.preOK.len() >= n.fastQuorumForConf(inst.rec.Ref.Conf) {
		n.startAccept(inst, attrs)
		return
	}
	if !n.canStillFastCommitPreAccept(inst) {
		n.startAccept(inst, attrs)
		return
	}
	if n.timeOptimization {
		deadline := inst.rec.ProcessAt
		if n.tick < deadline && inst.preOK.len() < len(n.confFor(inst.rec.Ref.Conf).Voters) {
			inst.waitDeadline = deadline
			_ = n.schedule(inst, timerFastWait, deadline-n.tick)
			return
		}
	}
}

func (n *RawNode) canStillFastCommitPreAccept(inst *instance) bool {
	if inst == nil ||
		!inst.rec.FastPathEligible ||
		!inst.rec.Ballot.IsInitialFor(inst.rec.Ref.Replica) {
		return false
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	unanswered := len(conf.Voters) - inst.preOK.len()
	if unanswered < 0 {
		unanswered = 0
	}
	fastQuorum := n.fastQuorumForConf(inst.rec.Ref.Conf)
	localAttrs := inst.rec.Attributes()
	canReach := func(candidate Attributes) bool {
		if !candidate.Equal(localAttrs) && !attrsCover(localAttrs, candidate) {
			return false
		}
		count := 1
		inst.preOK.each(conf, func(id ReplicaID, vote *attrVote) bool {
			if id != n.id && vote.fastPathEligible && vote.attributes().Equal(candidate) {
				count++
			}
			return true
		})
		return count+unanswered >= fastQuorum
	}
	if canReach(localAttrs) {
		return true
	}
	reachable := false
	inst.preOK.each(conf, func(id ReplicaID, vote *attrVote) bool {
		if id != n.id && vote.fastPathEligible && canReach(vote.attributes()) {
			reachable = true
			return false
		}
		return true
	})
	return reachable
}

func phaseMessageTimingEqual(rec InstanceRecord, m Message) bool {
	if rec.ProcessAt == m.ProcessAt && rec.TimingDomain == timingDomainFromMessage(m) {
		return true
	}
	return (m.Type == MsgAccept || m.Type == MsgCommit) &&
		m.ProcessAt == 0 &&
		!m.TOQ &&
		rec.ProcessAt != 0 &&
		rec.TimingDomain != TimingDomainUntimed
}

func phaseMessageTupleEqual(rec InstanceRecord, m Message) bool {
	return rec.Seq == m.Seq &&
		instanceNumsEqual(rec.Deps, m.Deps) &&
		commandEqual(rec.Command, m.Command) &&
		phaseMessageTimingEqual(rec, m)
}

func phaseMessageValueEqual(rec InstanceRecord, m Message) bool {
	return commandEqual(rec.Command, m.Command) && phaseMessageTimingEqual(rec, m)
}

func (n *RawNode) acceptRecord(m Message, old *instance) InstanceRecord {
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.Ballot, Status: StatusAccepted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: inboundCommand(m.Command), ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
	if old != nil && commandEqual(old.rec.Command, rec.Command) {
		if rec.ProcessAt == 0 && old.rec.ProcessAt != 0 {
			rec.ProcessAt = old.rec.ProcessAt
			rec.TimingDomain = old.rec.TimingDomain
		} else if rec.ProcessAt == old.rec.ProcessAt && old.rec.TimingDomain != TimingDomainUntimed {
			rec.TimingDomain = old.rec.TimingDomain
		}
	}
	acceptAttrs := mergeAttrs(acceptEvidenceFromMessage(m), n.computeAttrs(m.Command, m.Ref))
	if old != nil && old.rec.Status == StatusAccepted && old.rec.RecordBallot == rec.RecordBallot && old.rec.Seq == rec.Seq && instanceNumsEqual(old.rec.Deps, rec.Deps) && commandEqual(old.rec.Command, rec.Command) {
		if oldAttrs, ok := old.rec.AcceptAttributes(); ok {
			acceptAttrs = mergeAttrs(acceptAttrs, oldAttrs)
		}
		rec.AcceptEvidence = mergeAcceptEvidenceEntries(rec.AcceptEvidence, old.rec.AcceptEvidence)
	}
	mergeAcceptEvidence(&rec, acceptAttrs)
	mergeSenderAcceptEvidence(&rec, m.AcceptEvidence)
	mergeOwnAcceptEvidence(&rec, n.id, acceptAttrs)
	rec.Checksum = ChecksumRecord(rec)
	return rec
}

func (n *RawNode) handleAccept(m Message) error {
	old := n.instances[m.Ref]
	if old != nil {
		if old.rec.Status >= StatusCommitted {
			if recordTupleBallot(old.rec) == m.Ballot && !phaseMessageTupleEqual(old.rec, m) {
				return fmt.Errorf("%w: conflicting committed Accept retry for %s", ErrInvalidMessage, m.Ref)
			}
			n.sendCommitTo(m.From, old.rec)
			return nil
		}
		if m.Ballot.Less(old.rec.Ballot) {
			n.sendReject(MsgAcceptResp, m.From, m.Ref, old.rec.Ballot)
			return nil
		}
		if old.rec.Status == StatusAccepted && m.Ballot == old.rec.Ballot && old.rec.RecordBallot == m.Ballot {
			if !phaseMessageTupleEqual(old.rec, m) {
				return fmt.Errorf("%w: conflicting same-ballot Accept for %s", ErrInvalidMessage, m.Ref)
			}
			resp := Message{Type: MsgAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: old.rec.Ballot, RecordBallot: old.rec.RecordBallot, Seq: old.rec.Seq, Deps: old.rec.Deps, AcceptSeq: old.rec.AcceptSeq, AcceptDeps: old.rec.AcceptDeps, AcceptEvidence: old.rec.AcceptEvidence, RecordStatus: old.rec.Status}
			n.enqueueMessage(resp)
			return nil
		}
	}
	if old != nil && old.rec.Status == StatusPreAccepted && old.rec.RecordBallot == m.Ballot && !phaseMessageValueEqual(old.rec, m) {
		return fmt.Errorf("%w: conflicting same-ballot Accept promotion for %s", ErrInvalidMessage, m.Ref)
	}
	n.removeDeferredPreAccepts(m.Ref)
	rec := n.acceptRecord(m, old)
	generation := uint64(0)
	if old != nil {
		generation = old.generation + 1
	}
	n.observeInstanceRef(rec.Ref)
	inst := &instance{rec: rec, phase: phaseIdle, processAt: rec.ProcessAt, generation: generation}
	n.installInstance(inst)
	n.enqueueRecord(rec)
	n.markPendingConf(rec)
	if err := n.scheduleRecovery(inst); err != nil {
		return err
	}
	resp := Message{Type: MsgAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, RecordBallot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps, AcceptSeq: rec.AcceptSeq, AcceptDeps: rec.AcceptDeps, AcceptEvidence: rec.AcceptEvidence, RecordStatus: rec.Status}
	n.enqueueMessage(resp)
	return nil
}

func (n *RawNode) handleAcceptResp(m Message) error {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phaseAccept || !n.coordinatesInstance(inst) {
		return nil
	}
	if m.Reject {
		if !inst.rec.Ballot.Less(m.RejectHint) {
			return nil
		}
		return n.startPrepareFrom(inst, m.RejectHint)
	}
	if m.Ballot != inst.rec.Ballot ||
		m.RecordBallot != inst.rec.RecordBallot ||
		m.RecordStatus != StatusAccepted ||
		m.Seq != inst.rec.Seq ||
		!instanceNumsEqual(m.Deps, inst.rec.Deps) {
		return nil
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	if inst.accOK.has(conf, m.From) {
		return nil
	}
	if !inst.accOK.add(conf, m.From) {
		return nil
	}
	changed := mergeAcceptEvidence(&inst.rec, acceptEvidenceFromMessage(m))
	if mergeSenderAcceptEvidence(&inst.rec, m.AcceptEvidence) {
		changed = true
	}
	if changed {
		inst.rec.Checksum = ChecksumRecord(inst.rec)
		n.enqueueRecord(inst.rec)
	}
	if inst.accOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
		n.commit(inst, inst.rec.Attributes())
	}
	return nil
}

func (n *RawNode) handleCommit(m Message) error {
	old := n.instances[m.Ref]
	if old != nil && old.rec.Status >= StatusCommitted {
		if recordTupleBallot(old.rec) == m.RecordBallot && !phaseMessageTupleEqual(old.rec, m) {
			return fmt.Errorf("%w: conflicting committed Commit retry for %s", ErrInvalidMessage, m.Ref)
		}
		return nil
	}
	if old != nil && old.rec.RecordBallot == m.RecordBallot {
		switch {
		case old.rec.Status >= StatusAccepted && !phaseMessageTupleEqual(old.rec, m):
			return fmt.Errorf("%w: conflicting same-ballot Commit for %s", ErrInvalidMessage, m.Ref)
		case old.rec.Status == StatusPreAccepted && !phaseMessageValueEqual(old.rec, m):
			return fmt.Errorf("%w: conflicting same-ballot Commit promotion for %s", ErrInvalidMessage, m.Ref)
		}
	}
	n.removeDeferredPreAccepts(m.Ref)
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.RecordBallot, Status: StatusCommitted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: inboundCommand(m.Command), ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
	if old != nil && rec.Ballot.Less(old.rec.Ballot) {
		rec.Ballot = old.rec.Ballot
	}
	if old != nil && commandEqual(old.rec.Command, rec.Command) {
		if rec.ProcessAt == 0 && old.rec.ProcessAt != 0 {
			rec.ProcessAt = old.rec.ProcessAt
			rec.TimingDomain = old.rec.TimingDomain
		} else if rec.ProcessAt == old.rec.ProcessAt && old.rec.TimingDomain != TimingDomainUntimed {
			rec.TimingDomain = old.rec.TimingDomain
		}
	}
	if rec.RecordBallot == (Ballot{}) {
		return nil
	}
	var acceptAttrs Attributes
	hasAcceptAttrs := false
	if attrs, ok := m.AcceptAttributes(); ok {
		acceptAttrs = attrs
		hasAcceptAttrs = true
	}
	if old != nil {
		if attrs, ok := old.rec.AcceptAttributes(); ok {
			if hasAcceptAttrs {
				acceptAttrs = mergeAttrs(acceptAttrs, attrs)
			} else {
				acceptAttrs = attrs
				hasAcceptAttrs = true
			}
		}
	}
	if old != nil && old.rec.RecordBallot == rec.RecordBallot && old.rec.Seq == rec.Seq && instanceNumsEqual(old.rec.Deps, rec.Deps) && commandEqual(old.rec.Command, rec.Command) {
		rec.AcceptEvidence = mergeAcceptEvidenceEntries(rec.AcceptEvidence, old.rec.AcceptEvidence)
	}
	if hasAcceptAttrs {
		mergeAcceptEvidence(&rec, acceptAttrs)
	}
	rec.Checksum = ChecksumRecord(rec)
	n.observeInstanceRef(rec.Ref)
	n.installInstance(&instance{rec: rec, phase: phaseCommitted, processAt: rec.ProcessAt})
	n.enqueueRecord(rec)
	n.markPendingConf(rec)
	n.tryExecute()
	return nil
}

func (n *RawNode) handlePrepare(m Message) error {
	resp := Message{Type: MsgPrepareResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: m.Ballot}
	if n.needsRecordLoad(m.Ref) {
		return n.deferRecordLoad(m.Ref, m)
	}
	inst := n.instances[m.Ref]
	if inst != nil && inst.rec.TOQPending {
		return nil
	}
	if inst != nil && m.Ballot.Less(inst.rec.Ballot) {
		n.sendReject(MsgPrepareResp, m.From, m.Ref, inst.rec.Ballot)
		return nil
	}
	n.removeDeferredPreAccepts(m.Ref)
	deadline := uint64(0)
	needsWait := m.Ballot.IsRecovery() && m.Ballot.Replica != n.id &&
		(inst == nil || inst.rec.Ballot.Less(m.Ballot))
	if needsWait {
		var err error
		deadline, err = checkedLogicalDeadline(n.tick, n.recoveryTicks)
		if err != nil {
			return err
		}
	}
	if inst == nil {
		rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, Status: StatusNone, Deps: n.depsForConf(m.Ref.Conf)}
		rec.Checksum = ChecksumRecord(rec)
		inst = &instance{rec: rec, phase: phaseIdle}
		n.installInstance(inst)
		n.observeInstanceRef(rec.Ref)
		if needsWait {
			inst.waitDeadline = deadline
			if err := n.schedule(inst, timerPrepare, n.recoveryTicks); err != nil {
				return err
			}
		}
		n.enqueueRecord(rec)
	} else {
		changed := false
		if inst.rec.Ballot.Less(m.Ballot) {
			inst.rec.Ballot = m.Ballot
			changed = true
			if m.Ballot.Replica != n.id {
				n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
				n.cancelTimersForRef(inst.rec.Ref)
				releaseInstanceVolatile(inst)
				inst.phase = phaseIdle
				inst.generation++
				inst.waitDeadline = deadline
				if err := n.schedule(inst, timerPrepare, n.recoveryTicks); err != nil {
					return err
				}
			}
		}
		if n.ensureRecordDeps(&inst.rec) {
			changed = true
		}
		if changed {
			inst.rec.Checksum = ChecksumRecord(inst.rec)
			n.enqueueRecord(inst.rec)
		}
	}
	resp.Ballot = inst.rec.Ballot
	resp.RecordBallot = recordTupleBallot(inst.rec)
	resp.Seq = inst.rec.Seq
	resp.Deps = inst.rec.Deps
	resp.Command = inst.rec.Command.Borrow()
	resp.RecordStatus = inst.rec.Status
	resp.FastPathEligible = inst.rec.FastPathEligible
	resp.AcceptSeq = inst.rec.AcceptSeq
	resp.AcceptDeps = inst.rec.AcceptDeps
	resp.AcceptEvidence = inst.rec.AcceptEvidence
	resp.ProcessAt = inst.rec.ProcessAt
	resp.TOQ = recordUsesTOQ(inst.rec)
	n.enqueueMessage(resp)
	return nil
}

func (n *RawNode) handleEvidence(m Message) error {
	if n.needsRecordLoad(m.Ref) {
		return n.deferRecordLoad(m.Ref, m)
	}
	resp := Message{Type: MsgEvidenceResp, From: n.id, To: m.From, Ref: m.Ref, ConflictRef: m.ConflictRef, Ballot: m.Ballot, Deps: n.depsForConf(m.Ref.Conf), RecordStatus: StatusNone}
	inst := n.instances[m.Ref]
	if inst != nil && !inst.rec.TOQPending {
		deps := inst.rec.Deps
		conf := n.confFor(m.Ref.Conf)
		if len(deps) < len(conf.Voters) {
			expanded := make([]InstanceNum, len(conf.Voters))
			copy(expanded, deps)
			deps = expanded
		}
		resp.RecordBallot = recordTupleBallot(inst.rec)
		resp.Seq = inst.rec.Seq
		resp.Deps = deps
		resp.Command = inst.rec.Command.Borrow()
		resp.RecordStatus = inst.rec.Status
		resp.FastPathEligible = inst.rec.FastPathEligible
		resp.AcceptSeq = inst.rec.AcceptSeq
		resp.AcceptDeps = inst.rec.AcceptDeps
		resp.AcceptEvidence = inst.rec.AcceptEvidence
		resp.ProcessAt = inst.rec.ProcessAt
		resp.TOQ = recordUsesTOQ(inst.rec)
	}
	n.enqueueMessage(resp)
	return nil
}

func (n *RawNode) handleEvidenceResp(m Message) {
	key := tryEvidenceKey{target: m.ConflictRef, conflict: m.Ref, ballot: m.Ballot}
	records, ok := n.tryEvidenceChecks[key]
	if !ok {
		return
	}
	inst := n.instances[key.target]
	if inst == nil || inst.phase != phaseTryPreAccept || !n.coordinatesInstance(inst) || inst.rec.Ballot != key.ballot {
		n.deleteTryEvidenceCheck(key)
		return
	}
	if _, duplicate := records[m.From]; duplicate {
		return
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.RecordBallot, Status: m.RecordStatus, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptSeq: m.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), m.AcceptDeps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: m.Command.Clone(), FastPathEligible: m.FastPathEligible, ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
	records[m.From] = rec
	n.resolveTryEvidenceCheck(key)
}

func prepareResponseRecord(inst *instance, conf ConfState, id ReplicaID) InstanceRecord {
	if rec, ok := inst.prepareOK.get(conf, id); ok {
		return *rec
	}
	rec, _ := inst.prepareEvidence.get(conf, id)
	if rec == nil {
		return InstanceRecord{}
	}
	return *rec
}

func (n *RawNode) handlePrepareResp(m Message) error {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePrepare || !inst.rec.Ballot.IsRecovery() || inst.rec.Ballot.Replica != n.id {
		return nil
	}
	if m.Reject {
		if !inst.rec.Ballot.Less(m.RejectHint) {
			return nil
		}
		return n.startPrepareFrom(inst, m.RejectHint)
	}
	if m.Ballot.Less(inst.rec.Ballot) {
		return nil
	}
	if inst.rec.Ballot.Less(m.Ballot) {
		return n.startPrepareFrom(inst, m.Ballot)
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	if inst.prepareOK == nil {
		inst.prepareOK = getRecordVoteSet()
	}
	if _, duplicate := inst.prepareOK.get(conf, m.From); duplicate {
		return nil
	}
	recordBallot := m.RecordBallot
	if recordHasValue(m.RecordStatus) && recordBallot == (Ballot{}) {
		return nil
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: recordBallot, Status: m.RecordStatus, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptSeq: m.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), m.AcceptDeps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: m.Command.Clone(), FastPathEligible: m.FastPathEligible, ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
	n.ensureRecordDeps(&rec)
	rec.Checksum = ChecksumRecord(rec)
	if !inst.prepareOK.add(conf, m.From, rec) {
		return nil
	}
	if inst.prepareOK.len() < n.slowQuorumForConf(inst.rec.Ref.Conf) {
		return nil
	}

	var responderStorage [7]ReplicaID
	responders := responderStorage[:0]
	for _, id := range conf.Voters {
		if _, ok := inst.prepareOK.get(conf, id); ok {
			responders = append(responders, id)
			continue
		}
		if _, ok := inst.prepareEvidence.get(conf, id); ok {
			responders = append(responders, id)
		}
	}

	for _, id := range responders {
		rec := prepareResponseRecord(inst, conf, id)
		if rec.Status >= StatusCommitted {
			next := rec.Clone()
			next.Status = StatusCommitted
			next.Checksum = ChecksumRecord(next)
			n.setInstanceRecord(inst, next)
			n.commit(inst, inst.rec.Attributes())
			return nil
		}
	}
	var chosen InstanceRecord
	attrs := Attributes{}
	foundAccepted := false
	var highestAccepted Ballot
	for _, id := range responders {
		rec := prepareResponseRecord(inst, conf, id)
		if rec.Status != StatusAccepted {
			continue
		}
		if recBallot := recordTupleBallot(rec); !foundAccepted || highestAccepted.Less(recBallot) {
			chosen = rec
			attrs = rec.Attributes()
			highestAccepted = recordTupleBallot(rec)
			foundAccepted = true
			continue
		}
		if recordTupleBallot(rec) == highestAccepted && commandEqual(chosen.Command, rec.Command) {
			attrs = mergeAttrs(attrs, rec.Attributes())
		}
	}
	if foundAccepted {
		next := inst.rec.Clone()
		next.Command = chosen.Command.Clone()
		next.ProcessAt = chosen.ProcessAt
		next.TimingDomain = chosen.TimingDomain
		next.Checksum = ChecksumRecord(next)
		n.setInstanceRecord(inst, next)
		inst.processAt = chosen.ProcessAt
		n.startAccept(inst, attrs)
		return nil
	}

	attrs = Attributes{}
	foundPreAccepted := false
	var highestPreAccepted Ballot
	for _, id := range responders {
		rec := prepareResponseRecord(inst, conf, id)
		if rec.Status != StatusPreAccepted {
			continue
		}
		recBallot := recordTupleBallot(rec)
		if !foundPreAccepted || highestPreAccepted.Less(recBallot) {
			highestPreAccepted = recBallot
			foundPreAccepted = true
		}
	}
	foundPreAccepted = false
	var candidates []tryCandidate
	for _, id := range responders {
		rec := prepareResponseRecord(inst, conf, id)
		if rec.Status != StatusPreAccepted || recordTupleBallot(rec) != highestPreAccepted {
			continue
		}
		attrs = mergeAttrs(attrs, rec.Attributes())
		if !foundPreAccepted {
			chosen = rec
			foundPreAccepted = true
		}
		if rec.FastPathEligible {
			candidates = addTryCandidate(candidates, conf, rec, id)
		}
	}
	for _, candidate := range candidates {
		if candidate.voters.len() >= n.tryWitnessQuorumForConf(inst.rec.Ref.Conf) {
			next := inst.rec.Clone()
			next.Command = candidate.rec.Command.Clone()
			next.ProcessAt = candidate.rec.ProcessAt
			next.TimingDomain = candidate.rec.TimingDomain
			next.Checksum = ChecksumRecord(next)
			n.setInstanceRecord(inst, next)
			inst.processAt = candidate.rec.ProcessAt
			n.startTryPreAccept(inst, candidate.rec.Attributes(), candidate.voters, true)
			return nil
		}
	}
	if foundPreAccepted {
		next := inst.rec.Clone()
		next.Command = chosen.Command.Clone()
		next.ProcessAt = chosen.ProcessAt
		next.TimingDomain = chosen.TimingDomain
		next.Checksum = ChecksumRecord(next)
		n.setInstanceRecord(inst, next)
		inst.processAt = chosen.ProcessAt
		attrs = mergeAttrs(attrs, n.computeAttrs(inst.rec.Command, inst.rec.Ref))
		n.startAccept(inst, attrs)
		return nil
	}

	next := inst.rec.Clone()
	if _, _, control := n.findBootstrapControl(inst.rec.Ref); control {
		command, ok := n.membershipAllNoneCommand(inst.rec.Ref)
		if !ok {
			return ErrBootstrapControl
		}
		next.Command = command
	} else {
		next.Command = Command{Kind: CommandNoop}
	}
	next.ProcessAt = 0
	next.TimingDomain = TimingDomainUntimed
	next.TOQPending = false
	next.Checksum = ChecksumRecord(next)
	n.setInstanceRecord(inst, next)
	inst.processAt = 0
	n.startAccept(inst, Attributes{Seq: 1, Deps: n.depsForConf(m.Ref.Conf)})
	return nil
}

func acceptEvidenceFromMessage(m Message) Attributes {
	if attrs, ok := m.AcceptAttributes(); ok {
		return attrs
	}
	return m.Attributes()
}

func mergeAcceptEvidence(rec *InstanceRecord, attrs Attributes) bool {
	if attrs.Seq == 0 || len(attrs.Deps) == 0 {
		return false
	}
	if existing, ok := rec.AcceptAttributes(); ok {
		attrs = mergeAttrs(existing, attrs)
	}
	if rec.AcceptSeq == attrs.Seq && instanceNumsEqual(rec.AcceptDeps, attrs.Deps) {
		return false
	}
	rec.AcceptSeq = attrs.Seq
	rec.AcceptDeps = append([]InstanceNum(nil), attrs.Deps...)
	return true
}

func mergeSenderAcceptEvidence(rec *InstanceRecord, evidence []AcceptEvidence) bool {
	if len(evidence) == 0 {
		return false
	}
	merged := mergeAcceptEvidenceEntries(rec.AcceptEvidence, evidence)
	if acceptEvidenceEqual(rec.AcceptEvidence, merged) {
		return false
	}
	rec.AcceptEvidence = merged
	return true
}

func mergeOwnAcceptEvidence(rec *InstanceRecord, sender ReplicaID, attrs Attributes) bool {
	if sender == 0 || attrs.Seq == 0 || len(attrs.Deps) == 0 {
		return false
	}
	return mergeSenderAcceptEvidence(rec, []AcceptEvidence{{Sender: sender, Seq: attrs.Seq, Deps: attrs.Deps}})
}

func clearRecordAcceptEvidence(rec *InstanceRecord) {
	rec.AcceptSeq = 0
	rec.AcceptDeps = nil
	rec.AcceptEvidence = nil
}

func releaseInstanceVolatile(inst *instance) {
	if inst == nil {
		return
	}
	putAttrVoteSet(inst.preOK)
	putRecordVoteSet(inst.prepareOK)
	putRecordVoteSet(inst.prepareEvidence)
	clear(inst.tryDeferred)
	clear(inst.tryIgnored)
	inst.preOK = nil
	inst.accOK = 0
	inst.prepareOK = nil
	inst.prepareEvidence = nil
	inst.tryOK = 0
	inst.tryDeferred = nil
	inst.tryIgnored = nil
	// createdTick spans phase changes and remains the logical age origin.
	inst.waitDeadline = 0
	inst.processAt = inst.rec.ProcessAt
}

func (n *RawNode) startAccept(inst *instance, attrs Attributes) {
	if inst.phase == phaseCommitted {
		return
	}
	n.cancelTimersForRef(inst.rec.Ref)
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	n.removeDeferredPreAccepts(inst.rec.Ref)
	releaseInstanceVolatile(inst)
	next := inst.rec.Clone()
	clearRecordAcceptEvidence(&next)
	next.Status = StatusAccepted
	next.Seq = attrs.Seq
	next.Deps = append([]InstanceNum(nil), attrs.Deps...)
	next.FastPathEligible = false
	next.TOQPending = false
	next.RecordBallot = next.Ballot
	n.ensureRecordDeps(&next)
	evidenceAttrs := mergeAttrs(next.Attributes(), n.computeAttrs(next.Command, next.Ref))
	mergeAcceptEvidence(&next, evidenceAttrs)
	mergeOwnAcceptEvidence(&next, n.id, evidenceAttrs)
	next.Checksum = ChecksumRecord(next)
	n.setInstanceRecord(inst, next)
	inst.phase = phaseAccept
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	inst.accOK.add(n.confFor(inst.rec.Ref.Conf), n.id)
	inst.generation++
	n.enqueueRecord(inst.rec)
	if n.slowQuorumForConf(inst.rec.Ref.Conf) == 1 {
		n.commit(inst, attrs)
		return
	}
	n.broadcast(MsgAccept, inst.rec)
	if err := n.schedule(inst, timerAccept, n.retryTicks); err != nil {
		panic(err) // Tick prevents logical-clock exhaustion before scheduling.
	}
}

func (n *RawNode) startTryPreAccept(inst *instance, attrs Attributes, witnesses voterMask, countInitialLeader bool) {
	if inst.phase == phaseCommitted {
		return
	}
	n.cancelTimersForRef(inst.rec.Ref)
	conf := n.confFor(inst.rec.Ref.Conf)
	localPrepareWitness := false
	if rec, ok := inst.prepareOK.get(conf, n.id); ok {
		localPrepareWitness = rec.FastPathEligible && commandEqual(rec.Command, inst.rec.Command) && rec.Attributes().Equal(attrs)
	}
	n.removeDeferredPreAccepts(inst.rec.Ref)
	releaseInstanceVolatile(inst)
	next := inst.rec.Clone()
	next.Status = StatusPreAccepted
	next.Seq = attrs.Seq
	next.Deps = append([]InstanceNum(nil), attrs.Deps...)
	next.FastPathEligible = false
	next.TOQPending = false
	next.RecordBallot = next.Ballot
	clearRecordAcceptEvidence(&next)
	next.Checksum = ChecksumRecord(next)
	n.setInstanceRecord(inst, next)
	inst.phase = phaseTryPreAccept
	inst.tryDeferred = nil
	inst.tryIgnored = nil
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	inst.tryOK = witnesses
	if countInitialLeader {
		inst.tryOK.add(conf, inst.rec.Ref.Replica)
	}
	if localPrepareWitness {
		inst.tryOK.add(conf, n.id)
	}
	inst.generation++
	n.enqueueRecord(inst.rec)
	if inst.tryOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
		n.startAccept(inst, attrs)
		return
	}
	n.broadcastTryPreAccept(inst)
	if err := n.schedule(inst, timerTryPreAccept, n.retryTicks); err != nil {
		panic(err) // Tick prevents logical-clock exhaustion before scheduling.
	}
}

func (n *RawNode) handleTryPreAccept(m Message) error {
	old := n.instances[m.Ref]
	if old != nil {
		if old.rec.Status >= StatusCommitted {
			if recordTupleBallot(old.rec) == m.Ballot && !phaseMessageTupleEqual(old.rec, m) {
				return fmt.Errorf("%w: conflicting committed TryPreAccept retry for %s", ErrInvalidMessage, m.Ref)
			}
			n.sendCommitTo(m.From, old.rec)
			return nil
		}
		if m.Ballot.Less(old.rec.Ballot) {
			n.sendReject(MsgTryPreAcceptResp, m.From, m.Ref, old.rec.Ballot)
			return nil
		}
		if old.rec.Status == StatusAccepted {
			if m.Ballot == old.rec.Ballot && old.rec.RecordBallot == m.Ballot && !phaseMessageTupleEqual(old.rec, m) {
				return fmt.Errorf("%w: conflicting same-ballot TryPreAccept for %s", ErrInvalidMessage, m.Ref)
			}
			n.removeDeferredPreAccepts(m.Ref)
			if old.rec.Ballot.Less(m.Ballot) {
				old.rec.Ballot = m.Ballot
				old.rec.Checksum = ChecksumRecord(old.rec)
				if m.Ballot.Replica != n.id {
					n.cancelTimersForRef(old.rec.Ref)
					releaseInstanceVolatile(old)
					old.phase = phaseIdle
					old.generation++
					deadline, err := checkedLogicalDeadline(n.tick, n.recoveryTicks)
					if err != nil {
						return err
					}
					old.waitDeadline = deadline
					if err := n.schedule(old, timerPrepare, n.recoveryTicks); err != nil {
						return err
					}
				}
				n.enqueueRecord(old.rec)
			}
			resp := Message{
				Type:           MsgTryPreAcceptResp,
				From:           n.id,
				To:             m.From,
				Ref:            m.Ref,
				Ballot:         m.Ballot,
				RecordBallot:   old.rec.RecordBallot,
				Seq:            old.rec.Seq,
				Deps:           old.rec.Deps,
				AcceptSeq:      old.rec.AcceptSeq,
				AcceptDeps:     old.rec.AcceptDeps,
				AcceptEvidence: old.rec.AcceptEvidence,
				Command:        old.rec.Command.Borrow(),
				Reject:         true,
				RejectReason:   RejectAcceptedTarget,
				RecordStatus:   StatusAccepted,
				ProcessAt:      old.rec.ProcessAt,
				TOQ:            recordUsesTOQ(old.rec),
			}
			n.enqueueMessage(resp)
			return nil
		}
		if old.rec.Status == StatusPreAccepted && m.Ballot == old.rec.Ballot && old.rec.RecordBallot == m.Ballot {
			if !phaseMessageTupleEqual(old.rec, m) {
				return fmt.Errorf("%w: conflicting same-ballot TryPreAccept for %s", ErrInvalidMessage, m.Ref)
			}
			resp := Message{Type: MsgTryPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: old.rec.Ballot, Seq: old.rec.Seq, Deps: old.rec.Deps, RecordStatus: old.rec.Status, ProcessAt: old.rec.ProcessAt, TOQ: recordUsesTOQ(old.rec)}
			n.enqueueMessage(resp)
			return nil
		}
	}
	if conflictRef, conflictStatus, ok := n.tryPreAcceptConflict(m); ok {
		reason := RejectUncommittedConflict
		if conflictStatus >= StatusCommitted {
			reason = RejectCommittedConflict
		}
		resp := Message{Type: MsgTryPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: m.Ballot, Reject: true, RejectReason: reason, ConflictRef: conflictRef, ConflictStatus: conflictStatus, Deps: n.depsForConf(m.Ref.Conf)}
		n.enqueueMessage(resp)
		return nil
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.Ballot, Status: StatusPreAccepted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: inboundCommand(m.Command), ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
	rec.Checksum = ChecksumRecord(rec)
	if old != nil && old.rec.Status == StatusPreAccepted && instanceRecordEqual(old.rec, rec) {
		resp := Message{Type: MsgTryPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, RecordStatus: rec.Status, ProcessAt: rec.ProcessAt, TOQ: recordUsesTOQ(rec)}
		n.enqueueMessage(resp)
		return nil
	}
	n.removeDeferredPreAccepts(rec.Ref)
	n.observeInstanceRef(rec.Ref)
	generation := uint64(0)
	if old != nil {
		generation = old.generation + 1
	}
	inst := &instance{rec: rec, phase: phaseIdle, generation: generation}
	n.installInstance(inst)
	n.enqueueRecord(rec)
	n.markPendingConf(rec)
	if err := n.scheduleRecovery(inst); err != nil {
		return err
	}
	resp := Message{Type: MsgTryPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, RecordStatus: rec.Status, ProcessAt: rec.ProcessAt, TOQ: recordUsesTOQ(rec)}
	n.enqueueMessage(resp)
	return nil
}

func (n *RawNode) handleTryPreAcceptResp(m Message) error {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phaseTryPreAccept || !n.coordinatesInstance(inst) {
		return nil
	}
	if m.Reject {
		switch m.RejectReason {
		case RejectStaleBallot, RejectNone:
			if inst.rec.Ballot.Less(m.RejectHint) {
				return n.startPrepareFrom(inst, m.RejectHint)
			}
		case RejectAcceptedTarget:
			if m.Ballot != inst.rec.Ballot {
				return nil
			}
			evidence := InstanceRecord{
				Ref:            m.Ref,
				Ballot:         m.Ballot,
				RecordBallot:   m.RecordBallot,
				Status:         StatusAccepted,
				Seq:            m.Seq,
				Deps:           append([]InstanceNum(nil), m.Deps...),
				AcceptSeq:      m.AcceptSeq,
				AcceptDeps:     append([]InstanceNum(nil), m.AcceptDeps...),
				AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence),
				Command:        m.Command.Clone(),
				ProcessAt:      m.ProcessAt,
				TimingDomain:   timingDomainFromMessage(m),
			}
			evidence.Checksum = ChecksumRecord(evidence)
			if err := n.startPrepare(inst); err != nil {
				return err
			}
			if inst.prepareEvidence == nil {
				inst.prepareEvidence = getRecordVoteSet()
			}
			inst.prepareEvidence.add(n.confFor(inst.rec.Ref.Conf), m.From, evidence)
		case RejectCommittedConflict:
			if m.ConflictStatus >= StatusCommitted && n.maybeStartTryEvidenceCheck(inst, m.ConflictRef) {
				return nil
			}
			n.startAcceptAfterTryCommittedConflict(inst, m.ConflictRef)
		case RejectUncommittedConflict:
			if !m.ConflictRef.IsZero() {
				if n.tryConflictForcesSlowAccept(inst, m.ConflictRef) {
					attrs := n.computeAttrs(inst.rec.Command, inst.rec.Ref)
					n.startAccept(inst, mergeAttrs(attrs, inst.rec.Attributes()))
					return nil
				}
				if inst.tryDeferred == nil {
					inst.tryDeferred = make(map[InstanceRef]struct{}, 1)
				}
				inst.tryDeferred[m.ConflictRef] = struct{}{}
				n.maybeStartDependencyRecovery(m.ConflictRef)
			}
		}
		return nil
	}
	if m.Ballot != inst.rec.Ballot ||
		m.RecordStatus != StatusPreAccepted ||
		m.Seq != inst.rec.Seq ||
		!instanceNumsEqual(m.Deps, inst.rec.Deps) {
		return nil
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	if inst.tryOK.has(conf, m.From) {
		return nil
	}
	if !inst.tryOK.add(conf, m.From) {
		return nil
	}
	if inst.tryOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
		n.startAccept(inst, inst.rec.Attributes())
	}
	return nil
}

func (n *RawNode) startAcceptAfterTryCommittedConflict(inst *instance, conflictRef InstanceRef) {
	attrs := mergeAttrs(n.computeAttrs(inst.rec.Command, inst.rec.Ref), inst.rec.Attributes())
	attrs = n.attrsWithConflictDependency(attrs, conflictRef, inst.rec.Ref.Conf)
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	n.startAccept(inst, attrs)
}

func (n *RawNode) startAcceptAfterTryCommittedConflictTuple(inst *instance, conflict InstanceRecord) {
	attrs := mergeAttrs(n.computeAttrs(inst.rec.Command, inst.rec.Ref), inst.rec.Attributes())
	attrs = n.attrsWithConflictDependencySeq(attrs, conflict.Ref, conflict.Seq, inst.rec.Ref.Conf)
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	n.startAccept(inst, attrs)
}

func (n *RawNode) maybeStartTryEvidenceCheck(inst *instance, conflictRef InstanceRef) bool {
	if inst == nil || conflictRef.IsZero() || conflictRef.Conf != inst.rec.Ref.Conf {
		return false
	}
	if !n.attrsDependsOn(inst.rec.Attributes(), conflictRef, inst.rec.Ref.Conf) {
		return false
	}
	if len(inst.tryIgnored) > 0 || n.faultTolerance(inst.rec.Ref.Conf) <= 0 {
		return false
	}
	key := tryEvidenceKey{target: inst.rec.Ref, conflict: conflictRef, ballot: inst.rec.Ballot}
	for existingKey, records := range n.tryEvidenceChecks {
		if existingKey.target != inst.rec.Ref {
			continue
		}
		if existingKey == key {
			return true
		}
		tuple, tupleOK := n.committedConflictTuple(existingKey.conflict, records)
		if tupleOK {
			tuple = tuple.Clone()
		}
		n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
		if tupleOK {
			n.startAcceptAfterTryCommittedConflictTuple(inst, tuple)
		} else {
			n.startAcceptAfterTryCommittedConflict(inst, existingKey.conflict)
		}
		return true
	}
	if len(n.tryEvidenceChecks) >= n.maxConcurrentRecoveries {
		n.startAcceptAfterTryCommittedConflict(inst, conflictRef)
		return true
	}
	if n.tryEvidenceChecks == nil {
		n.tryEvidenceChecks = make(map[tryEvidenceKey]map[ReplicaID]InstanceRecord, 1)
	}
	n.tryEvidenceChecks[key] = make(map[ReplicaID]InstanceRecord, n.faultTolerance(inst.rec.Ref.Conf))
	n.broadcastTryEvidenceCheck(inst, conflictRef)
	return true
}

func (n *RawNode) broadcastTryEvidenceCheck(inst *instance, conflictRef InstanceRef) {
	for _, to := range n.confFor(inst.rec.Ref.Conf).Voters {
		if to == n.id {
			continue
		}
		m := Message{Type: MsgEvidence, From: n.id, To: to, Ref: conflictRef, ConflictRef: inst.rec.Ref, Ballot: inst.rec.Ballot}
		n.enqueueMessage(m)
	}
}

func (n *RawNode) deleteTryEvidenceCheck(key tryEvidenceKey) {
	if records, ok := n.tryEvidenceChecks[key]; ok {
		clearTryEvidenceRecords(records)
		delete(n.tryEvidenceChecks, key)
	}
	if len(n.tryEvidenceChecks) == 0 {
		n.tryEvidenceChecks = nil
	}
}

func (n *RawNode) resolveTryEvidenceCheck(key tryEvidenceKey) {
	records, ok := n.tryEvidenceChecks[key]
	if !ok {
		return
	}
	inst := n.instances[key.target]
	if inst == nil || inst.phase != phaseTryPreAccept || !n.coordinatesInstance(inst) || inst.rec.Ballot != key.ballot {
		n.deleteTryEvidenceCheck(key)
		return
	}
	authorized, failClosed := n.tryEvidenceDecision(inst, key, records)
	if !authorized && !failClosed {
		return
	}
	var tuple InstanceRecord
	tupleOK := false
	if failClosed {
		tuple, tupleOK = n.committedConflictTuple(key.conflict, records)
		if tupleOK {
			tuple = tuple.Clone()
		}
	}
	n.deleteTryEvidenceCheck(key)
	if failClosed {
		if tupleOK {
			n.startAcceptAfterTryCommittedConflictTuple(inst, tuple)
		} else {
			n.startAcceptAfterTryCommittedConflict(inst, key.conflict)
		}
		return
	}
	inst.tryIgnored = map[InstanceRef]struct{}{key.conflict: {}}
	n.broadcastTryPreAcceptIgnore(inst, key.conflict)
}

func (n *RawNode) tryEvidenceDecision(inst *instance, key tryEvidenceKey, records map[ReplicaID]InstanceRecord) (bool, bool) {
	f := n.faultTolerance(inst.rec.Ref.Conf)
	tuple, tupleOK := n.committedConflictTuple(key.conflict, records)
	if !tupleOK {
		if n.allRemoteEvidenceResponses(inst.rec.Ref.Conf, records) {
			return false, true
		}
		return false, false
	}
	ids := make([]ReplicaID, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	checked := 0
	for _, id := range ids {
		if id == n.id {
			continue
		}
		failClosed, usable := n.tryEvidenceRecordDecision(inst, tuple, records[id])
		if failClosed {
			return false, true
		}
		if usable {
			checked++
		}
	}
	if checked >= f {
		return true, false
	}
	if n.allRemoteEvidenceResponses(inst.rec.Ref.Conf, records) {
		return false, true
	}
	return false, false
}

func (n *RawNode) committedConflictTuple(conflictRef InstanceRef, records map[ReplicaID]InstanceRecord) (InstanceRecord, bool) {
	if inst := n.instances[conflictRef]; inst != nil && inst.rec.Status >= StatusCommitted {
		return inst.rec, true
	}
	ids := make([]ReplicaID, 0, len(records))
	for id := range records {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		rec := records[id]
		if rec.Status >= StatusCommitted {
			return rec, true
		}
	}
	return InstanceRecord{}, false
}

func (n *RawNode) tryEvidenceRecordDecision(inst *instance, tuple InstanceRecord, rec InstanceRecord) (bool, bool) {
	if rec.Ref != tuple.Ref {
		return true, false
	}
	if rec.Status == StatusNone {
		return false, false
	}
	if rec.RecordBallot == (Ballot{}) || !sameValueTuple(rec, tuple) || len(rec.AcceptEvidence) == 0 {
		return true, false
	}
	seen := make(map[ReplicaID]AcceptEvidence, len(rec.AcceptEvidence))
	for _, ev := range rec.AcceptEvidence {
		if ev.Sender == 0 || !n.confFor(tuple.Ref.Conf).Contains(ev.Sender) || ev.Seq == 0 || len(ev.Deps) != len(n.confFor(tuple.Ref.Conf).Voters) {
			return true, false
		}
		if existing, ok := seen[ev.Sender]; ok {
			if existing.Seq != ev.Seq || !instanceNumsEqual(existing.Deps, ev.Deps) {
				return true, false
			}
			continue
		}
		seen[ev.Sender] = ev
		if n.leaderMustBeInCandidateFastQuorum(inst, ev.Sender) && !n.attrsDependsOn(Attributes{Seq: ev.Seq, Deps: ev.Deps}, inst.rec.Ref, tuple.Ref.Conf) {
			return true, false
		}
	}
	return false, true
}

func sameValueTuple(a, b InstanceRecord) bool {
	return a.Ref == b.Ref && a.Seq == b.Seq && a.ProcessAt == b.ProcessAt && a.TimingDomain == b.TimingDomain && instanceNumsEqual(a.Deps, b.Deps) && commandEqual(a.Command, b.Command)
}

func (n *RawNode) allRemoteEvidenceResponses(confID ConfID, records map[ReplicaID]InstanceRecord) bool {
	remote := 0
	for _, id := range n.confFor(confID).Voters {
		if id != n.id {
			remote++
		}
	}
	return len(records) >= remote
}

func (n *RawNode) faultTolerance(confID ConfID) int {
	conf := n.confFor(confID)
	q, err := newQuorum(conf.Voters)
	if err != nil {
		return 0
	}
	return len(conf.Voters) - q.slowQuorum()
}

func (n *RawNode) failPendingTryEvidenceCheck(inst *instance) bool {
	if inst == nil || len(n.tryEvidenceChecks) == 0 {
		return false
	}
	keys := make([]tryEvidenceKey, 0, len(n.tryEvidenceChecks))
	for key := range n.tryEvidenceChecks {
		if key.target != inst.rec.Ref {
			continue
		}
		if key.ballot != inst.rec.Ballot {
			n.deleteTryEvidenceCheck(key)
			continue
		}
		keys = append(keys, key)
	}
	if len(n.tryEvidenceChecks) == 0 {
		n.tryEvidenceChecks = nil
	}
	if len(keys) == 0 {
		return false
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].conflict.Replica != keys[j].conflict.Replica {
			return keys[i].conflict.Replica < keys[j].conflict.Replica
		}
		if keys[i].conflict.Instance != keys[j].conflict.Instance {
			return keys[i].conflict.Instance < keys[j].conflict.Instance
		}
		return keys[i].conflict.Conf < keys[j].conflict.Conf
	})
	n.startAcceptAfterTryCommittedConflict(inst, keys[0].conflict)
	return true
}

func (n *RawNode) dropTryEvidenceChecksForTarget(target InstanceRef) {
	for key, records := range n.tryEvidenceChecks {
		if key.target != target {
			continue
		}
		clearTryEvidenceRecords(records)
		delete(n.tryEvidenceChecks, key)
	}
	if len(n.tryEvidenceChecks) == 0 {
		n.tryEvidenceChecks = nil
	}
}
func clearTryEvidenceRecords(records map[ReplicaID]InstanceRecord) {
	for id := range records {
		records[id] = InstanceRecord{}
		delete(records, id)
	}
}
func (n *RawNode) tryPreAcceptConflict(m Message) (InstanceRef, Status, bool) {
	candidate := InstanceRecord{Ref: m.Ref, Seq: m.Seq, Deps: m.Deps, Command: m.Command}
	var conflictRef InstanceRef
	conflictStatus := StatusNone
	considerConflict := func(ref InstanceRef, status Status) {
		if conflictRef.IsZero() || lessRef(ref, conflictRef) {
			conflictRef = ref
			conflictStatus = status
		}
	}
	considerRetired := func(lane instanceLane, instance InstanceNum) {
		if instance == 0 {
			return
		}
		ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
		if ref != m.Ref && !n.attrsDependsOn(candidate.Attributes(), ref, m.Ref.Conf) {
			considerConflict(ref, StatusExecuted)
		}
	}
	considerRecord := func(ref InstanceRef, other *instance) {
		if other == nil || ref == m.Ref || other.rec.Status == StatusNone || !commandsConflict(candidate.Command, other.rec.Command) {
			return
		}
		candidateDepends := n.attrsDependsOn(candidate.Attributes(), ref, m.Ref.Conf)
		otherAttrs := other.rec.Attributes()
		if n.attrsDependsOn(otherAttrs, m.Ref, other.rec.Ref.Conf) {
			return
		}
		if !candidateDepends {
			considerConflict(ref, other.rec.Status)
			return
		}
		if other.rec.Status >= StatusCommitted && m.IgnoreDependency.Ref == ref {
			return
		}
		if other.rec.Status >= StatusCommitted {
			if n.recordAcceptEvidenceDependsOn(other.rec, m.Ref) {
				return
			}
		} else if acceptAttrs, ok := other.rec.AcceptAttributes(); ok && n.attrsDependsOn(acceptAttrs, m.Ref, other.rec.Ref.Conf) {
			return
		}
		sameLeaderPreAccepted := ref.Replica == m.Ref.Replica && other.rec.Status == StatusPreAccepted
		if otherAttrs.Seq >= candidate.Seq && !sameLeaderPreAccepted {
			considerConflict(ref, other.rec.Status)
		}
	}

	if candidate.Command.Kind != CommandNoop {
		if commandHasGlobalConflictScope(candidate.Command.Kind) {
			n.engine.lanes(m.Ref.Conf, func(lane instanceLane) bool {
				resident, retired := n.engine.maxEligibleAny(lane)
				considerRetired(lane, retired)
				n.engine.walkDesc(lane, resident, func(instance InstanceNum, slot laneSlot) bool {
					if !slot.eligible() {
						return true
					}
					ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
					considerRecord(ref, n.instances[ref])
					return true
				})
				return true
			})
		} else {
			n.engine.keyLaneSet(m.Ref.Conf, candidate.Command.ConflictKeys, func(lane instanceLane) bool {
				for _, key := range candidate.Command.ConflictKeys {
					resident, retired := n.engine.keyMax(m.Ref.Conf, key, lane)
					considerRetired(lane, retired)
					n.engine.walkKeyDesc(m.Ref.Conf, key, lane, resident, func(instance InstanceNum, slot laneSlot) bool {
						if !slot.eligible() {
							return true
						}
						ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
						considerRecord(ref, n.instances[ref])
						return true
					})
				}
				return true
			})
			n.engine.lanes(m.Ref.Conf, func(lane instanceLane) bool {
				resident, retired := n.engine.globalMax(lane)
				considerRetired(lane, retired)
				n.engine.walkGlobalDesc(lane, resident, func(instance InstanceNum, _ laneSlot) bool {
					ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
					considerRecord(ref, n.instances[ref])
					return true
				})
				return true
			})
		}
	}
	return conflictRef, conflictStatus, !conflictRef.IsZero()
}


func (n *RawNode) attrsDependsOn(attrs Attributes, dep InstanceRef, confID ConfID) bool {
	if dep.Conf != confID || dep.Replica == 0 {
		return false
	}
	idx, ok := n.confFor(confID).Index(dep.Replica)
	if !ok || idx >= len(attrs.Deps) {
		return false
	}
	return attrs.Deps[idx] >= dep.Instance
}

func (n *RawNode) recordAcceptEvidenceDependsOn(rec InstanceRecord, dep InstanceRef) bool {
	seen := make(map[ReplicaID]AcceptEvidence, len(rec.AcceptEvidence))
	covers := false
	for _, ev := range rec.AcceptEvidence {
		if ev.Sender == 0 {
			continue
		}
		if existing, ok := seen[ev.Sender]; ok {
			if existing.Seq != ev.Seq || !instanceNumsEqual(existing.Deps, ev.Deps) {
				return false
			}
			continue
		}
		seen[ev.Sender] = ev
		if n.attrsDependsOn(Attributes{Seq: ev.Seq, Deps: ev.Deps}, dep, rec.Ref.Conf) {
			covers = true
		}
	}
	return covers
}

func (n *RawNode) attrsWithConflictDependency(attrs Attributes, dep InstanceRef, confID ConfID) Attributes {
	conflictSeq := uint64(0)
	if inst := n.instances[dep]; inst != nil && inst.rec.Status != StatusNone && inst.rec.Command.Kind != CommandNoop {
		conflictSeq = inst.rec.Seq
	}
	return n.attrsWithConflictDependencySeq(attrs, dep, conflictSeq, confID)
}

func (n *RawNode) attrsWithConflictDependencySeq(attrs Attributes, dep InstanceRef, conflictSeq uint64, confID ConfID) Attributes {
	if dep.IsZero() || dep.Conf != confID {
		return attrs
	}
	conf := n.confFor(confID)
	idx, ok := conf.Index(dep.Replica)
	if !ok {
		return attrs
	}
	if len(attrs.Deps) < len(conf.Voters) {
		deps := make([]InstanceNum, len(conf.Voters))
		copy(deps, attrs.Deps)
		attrs.Deps = deps
	}
	if dep.Instance > attrs.Deps[idx] {
		attrs.Deps[idx] = dep.Instance
	}
	if conflictSeq >= attrs.Seq {
		attrs.Seq = conflictSeq + 1
	}
	if attrs.Seq == 0 {
		attrs.Seq = 1
	}
	return attrs
}

func (n *RawNode) hasMatchingInitialLeaderTryRecord(inst *instance, attrs Attributes) bool {
	if inst == nil || inst.rec.Ref.Replica == 0 {
		return false
	}
	rec, ok := inst.prepareOK.get(n.confFor(inst.rec.Ref.Conf), inst.rec.Ref.Replica)
	return ok &&
		rec.Status == StatusPreAccepted &&
		rec.FastPathEligible &&
		commandEqual(rec.Command, inst.rec.Command) &&
		rec.Attributes().Equal(attrs)
}

func (n *RawNode) tryConflictForcesSlowAccept(inst *instance, conflictRef InstanceRef) bool {
	if inst == nil || conflictRef.IsZero() {
		return false
	}
	if !n.attrsDependsOn(inst.rec.Attributes(), conflictRef, inst.rec.Ref.Conf) && n.leaderMustBeInCandidateFastQuorum(inst, conflictRef.Replica) {
		return true
	}
	return n.hasDeferredCommandLeaderInFastQuorum(inst)
}

func (n *RawNode) hasDeferredCommandLeaderInFastQuorum(inst *instance) bool {
	current := inst.rec.Ref
	refs := make([]InstanceRef, 0, len(n.instances))
	for ref := range n.instances {
		refs = append(refs, ref)
	}
	sortRefs(refs)
	for _, ref := range refs {
		other := n.instances[ref]
		if other == nil || other == inst || other.tryDeferred == nil {
			continue
		}
		if _, ok := other.tryDeferred[current]; ok && n.leaderMustBeInCandidateFastQuorum(inst, ref.Replica) {
			return true
		}
	}
	return false
}

func (n *RawNode) leaderMustBeInCandidateFastQuorum(inst *instance, leader ReplicaID) bool {
	if inst == nil || leader == 0 {
		return false
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	if !conf.Contains(leader) {
		return false
	}
	possible := 0
	leaderPossible := false
	for _, id := range conf.Voters {
		if n.replicaCouldBeInCandidateFastQuorum(inst, id) {
			possible++
			if id == leader {
				leaderPossible = true
			}
		}
	}
	if !leaderPossible {
		return false
	}
	q, err := newQuorum(conf.Voters)
	if err != nil {
		return false
	}
	return possible-1 < q.fastQuorum()
}

func (n *RawNode) replicaCouldBeInCandidateFastQuorum(inst *instance, id ReplicaID) bool {
	rec, ok := inst.prepareOK.get(n.confFor(inst.rec.Ref.Conf), id)
	if !ok {
		return true
	}
	return rec.Status == StatusPreAccepted &&
		rec.FastPathEligible &&
		commandEqual(rec.Command, inst.rec.Command) &&
		rec.Attributes().Equal(inst.rec.Attributes())
}

func (n *RawNode) startPrepare(inst *instance) error {
	return n.startPrepareFrom(inst, inst.rec.Ballot)
}

func (n *RawNode) startPrepareFrom(inst *instance, base Ballot) error {
	if base.Less(inst.rec.Ballot) {
		base = inst.rec.Ballot
	}
	next, err := base.Next(n.id)
	if err != nil {
		return err
	}
	if _, err := checkedLogicalDeadline(n.tick, n.recoveryTicks); err != nil {
		return err
	}
	if inst.generation == ^uint64(0) {
		return ErrLogicalTimeExhausted
	}
	n.cancelTimersForRef(inst.rec.Ref)
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	releaseInstanceVolatile(inst)
	inst.waitDeadline = 0
	inst.phase = phasePrepare
	n.ensureRecordDeps(&inst.rec)
	inst.tryDeferred = nil
	inst.rec.Ballot = next
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	inst.prepareOK = getRecordVoteSet()
	if !inst.prepareOK.add(n.confFor(inst.rec.Ref.Conf), n.id, inst.rec) {
		return ErrInvalidConfig
	}
	inst.generation++
	n.enqueueRecord(inst.rec)
	if n.slowQuorumForConf(inst.rec.Ref.Conf) == 1 {
		next := inst.rec.Clone()
		if _, _, isControl := n.findBootstrapControl(inst.rec.Ref); isControl {
			command, ok := n.membershipAllNoneCommand(inst.rec.Ref)
			if !ok {
				return ErrBootstrapControl
			}
			next.Command = command
		} else {
			next.Command = Command{Kind: CommandNoop}
		}
		next.ProcessAt = 0
		next.TimingDomain = TimingDomainUntimed
		next.TOQPending = false
		next.Checksum = ChecksumRecord(next)
		n.setInstanceRecord(inst, next)
		inst.processAt = 0
		n.startAccept(inst, Attributes{Seq: 1, Deps: n.depsForConf(inst.rec.Ref.Conf)})
		return nil
	}
	n.broadcastPrepare(inst.rec)
	return n.schedule(inst, timerPrepare, n.recoveryTicks)
}

func (n *RawNode) coordinatesInstance(inst *instance) bool {
	return inst != nil && (inst.rec.Ref.Replica == n.id || (inst.rec.Ballot.IsRecovery() && inst.rec.Ballot.Replica == n.id))
}

func (n *RawNode) recoveryCoordinatorAt(ref InstanceRef, tick uint64) ReplicaID {
	conf := n.confFor(ref.Conf)
	candidates := make([]ReplicaID, 0, len(conf.Voters))
	for _, id := range conf.Voters {
		if id != ref.Replica {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		if len(conf.Voters) == 1 {
			return conf.Voters[0]
		}
		return 0
	}
	round := uint64(0)
	if tick > 0 && n.recoveryTicks > 0 {
		round = (tick - 1) / n.recoveryTicks
	}
	return candidates[round%uint64(len(candidates))]
}

func (n *RawNode) recoveryCoordinator(ref InstanceRef) ReplicaID {
	return n.recoveryCoordinatorAt(ref, n.tick)
}

func (n *RawNode) shouldCoordinateRecovery(ref InstanceRef) bool {
	return n.recoveryCoordinator(ref) == n.id
}

func (n *RawNode) recoveryWouldExhaustAt(tick uint64) bool {
	starts := n.maxDependencyRecoveriesPerDrive
	if available := n.maxConcurrentRecoveries - n.activeRecoveryCount(); starts > available {
		starts = available
	}
	if starts <= 0 {
		return false
	}
	due := make(timerHeap, 0, starts)
	for _, t := range n.timers {
		if t.deadline > tick || t.kind != timerPrepare {
			continue
		}
		inst := n.instances[t.ref]
		if inst == nil ||
			inst.generation != t.gen ||
			inst.phase == phaseCommitted ||
			inst.phase == phasePrepare ||
			inst.rec.Status >= StatusCommitted ||
			n.recoveryCoordinatorAt(inst.rec.Ref, tick) != n.id ||
			(inst.rec.Ballot.IsRecovery() && inst.rec.Ballot.Replica != n.id && tick < inst.waitDeadline) {
			continue
		}
		due = append(due, t)
	}
	sort.Slice(due, func(i, j int) bool { return due.Less(i, j) })
	if len(due) > starts {
		due = due[:starts]
	}
	for _, t := range due {
		if _, err := n.instances[t.ref].rec.Ballot.Next(n.id); err != nil {
			return true
		}
	}
	return false
}

func (n *RawNode) timerTransition(t timer, inst *instance, next uint64) (uint64, uint64, bool) {
	switch t.kind {
	case timerFastWait:
		if inst.phase == phasePreAccept &&
			!inst.rec.TOQPending &&
			(inst.waitDeadline == 0 || next >= inst.waitDeadline) &&
			inst.preOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
			delta := n.startAcceptGenerationDelta(inst)
			return delta, n.retryTicks, n.slowQuorumForConf(inst.rec.Ref.Conf) > 1
		}
	case timerPreAccept:
		if inst.phase != phasePreAccept {
			return 0, 0, false
		}
		if !inst.rec.TOQPending &&
			(inst.waitDeadline == 0 || next >= inst.waitDeadline) &&
			inst.preOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
			delta := n.startAcceptGenerationDelta(inst)
			return delta, n.retryTicks, n.slowQuorumForConf(inst.rec.Ref.Conf) > 1
		}
		return 0, n.retryTicks, next != ^uint64(0)
	case timerAccept:
		if inst.phase == phaseAccept {
			return 0, n.retryTicks, next != ^uint64(0)
		}
	case timerTryPreAccept:
		if inst.phase != phaseTryPreAccept {
			return 0, 0, false
		}
		pendingEvidence := false
		for key := range n.tryEvidenceChecks {
			if key.target == inst.rec.Ref && key.ballot == inst.rec.Ballot {
				pendingEvidence = true
				break
			}
		}
		if pendingEvidence {
			delta := n.startAcceptGenerationDelta(inst)
			return delta, n.retryTicks, n.slowQuorumForConf(inst.rec.Ref.Conf) > 1
		}
		return 0, n.retryTicks, next != ^uint64(0)
	case timerPrepare:
		if inst.phase == phasePrepare {
			return 0, n.recoveryTicks, next != ^uint64(0)
		}
		if inst.rec.Status >= StatusCommitted ||
			(inst.rec.Status != StatusNone && inst.rec.Status != StatusPreAccepted && inst.rec.Status != StatusAccepted) {
			return 0, 0, false
		}
		promised := inst.rec.Ballot.IsRecovery() && inst.rec.Ballot.Replica != n.id && next < inst.waitDeadline
		if n.recoveryCoordinatorAt(inst.rec.Ref, next) == n.id && !promised {
			return 1, n.recoveryTicks, next != ^uint64(0)
		}
		if inst.waitDeadline <= next {
			return 0, n.recoveryTicks, next != ^uint64(0)
		}
	}
	return 0, 0, false
}

func (n *RawNode) preflightDueTimers(next uint64) error {
	for _, t := range n.timers {
		if t.deadline > next {
			continue
		}
		inst := n.instances[t.ref]
		if inst == nil || inst.generation != t.gen || inst.phase == phaseCommitted {
			continue
		}
		delta, after, schedules := n.timerTransition(t, inst, next)
		if delta > ^uint64(0)-inst.generation {
			return ErrLogicalTimeExhausted
		}
		if schedules {
			if _, err := checkedLogicalDeadline(next, after); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *RawNode) scheduleRecovery(inst *instance) error {
	if inst == nil || inst.rec.Status >= StatusCommitted || inst.phase == phasePrepare {
		return nil
	}
	if inst.waitDeadline > n.tick {
		return nil
	}
	deadline, err := checkedLogicalDeadline(n.tick, n.recoveryTicks)
	if err != nil {
		return err
	}
	if err := n.schedule(inst, timerPrepare, n.recoveryTicks); err != nil {
		return err
	}
	inst.waitDeadline = deadline
	return nil
}

func (n *RawNode) promisedToOtherCoordinator(inst *instance) bool {
	return inst != nil && inst.rec.Ballot.IsRecovery() && inst.rec.Ballot.Replica != n.id && n.tick < inst.waitDeadline
}

func (n *RawNode) cancelTimersForRef(ref InstanceRef) {
	for i := len(n.timers) - 1; i >= 0; i-- {
		if n.timers[i].ref == ref {
			heap.Remove(&n.timers, i)
		}
	}
}

func (n *RawNode) commit(inst *instance, attrs Attributes) {
	if inst.phase == phaseCommitted && inst.rec.Status >= StatusCommitted {
		return
	}
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	n.cancelTimersForRef(inst.rec.Ref)
	n.removeDeferredPreAccepts(inst.rec.Ref)
	if inst.rec.Seq != attrs.Seq || !instanceNumsEqual(inst.rec.Deps, attrs.Deps) {
		clearRecordAcceptEvidence(&inst.rec)
	}
	next := inst.rec.Clone()
	next.Status = StatusCommitted
	next.Seq = attrs.Seq
	next.Deps = append([]InstanceNum(nil), attrs.Deps...)
	next.FastPathEligible = false
	next.TOQPending = false
	next.Checksum = ChecksumRecord(next)
	n.setInstanceRecord(inst, next)
	inst.phase = phaseCommitted
	n.noteCommitted(inst.rec.Ref)
	inst.generation++
	n.enqueueRecord(inst.rec)
	n.broadcast(MsgCommit, inst.rec)
	releaseInstanceVolatile(inst)
	n.tryExecute()
}

func (n *RawNode) onTimer(inst *instance, kind timerKind) error {
	switch kind {
	case timerFastWait:
		if inst.phase == phasePreAccept && !inst.rec.TOQPending && (inst.waitDeadline == 0 || n.tick >= inst.waitDeadline) && inst.preOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
			n.startAccept(inst, attrsFromVotes(n.confFor(inst.rec.Ref.Conf), inst.preOK))
		}
	case timerPreAccept:
		if inst.phase == phasePreAccept {
			if !inst.rec.TOQPending && (inst.waitDeadline == 0 || n.tick >= inst.waitDeadline) && inst.preOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
				n.startAccept(inst, attrsFromVotes(n.confFor(inst.rec.Ref.Conf), inst.preOK))
				return nil
			}
			n.broadcastPreAccept(inst)
			if n.tick != ^uint64(0) {
				return n.schedule(inst, timerPreAccept, n.retryTicks)
			}
		}
	case timerAccept:
		if inst.phase == phaseAccept {
			n.broadcast(MsgAccept, inst.rec)
			if n.tick != ^uint64(0) {
				return n.schedule(inst, timerAccept, n.retryTicks)
			}
		}
	case timerTryPreAccept:
		if inst.phase == phaseTryPreAccept {
			if n.failPendingTryEvidenceCheck(inst) {
				return nil
			}
			n.broadcastTryPreAccept(inst)
			if n.tick != ^uint64(0) {
				return n.schedule(inst, timerTryPreAccept, n.retryTicks)
			}
		}
	case timerPrepare:
		if inst.phase == phasePrepare {
			n.broadcastPrepare(inst.rec)
			if n.tick != ^uint64(0) {
				return n.schedule(inst, timerPrepare, n.recoveryTicks)
			}
			return nil
		}
		if inst.rec.Status < StatusCommitted && (inst.rec.Status == StatusNone || inst.rec.Status == StatusPreAccepted || inst.rec.Status == StatusAccepted) {
			if n.shouldCoordinateRecovery(inst.rec.Ref) && !n.promisedToOtherCoordinator(inst) {
				if n.dependencyRecoveryStartsLeft <= 0 || n.activeRecoveryCount() >= n.maxConcurrentRecoveries {
					return n.scheduleRecovery(inst)
				}
				if err := n.startPrepare(inst); err != nil {
					return err
				}
				n.dependencyRecoveryStartsLeft--
				return nil
			}
			return n.scheduleRecovery(inst)
		}
	}
	return nil
}

func (n *RawNode) schedule(inst *instance, kind timerKind, after uint64) error {
	deadline, err := checkedLogicalDeadline(n.tick, after)
	if err != nil {
		return err
	}
	for i := range n.timers {
		if n.timers[i].ref != inst.rec.Ref || n.timers[i].kind != kind {
			continue
		}
		n.timers[i] = timer{deadline: deadline, ref: inst.rec.Ref, kind: kind, gen: inst.generation, index: i}
		heap.Fix(&n.timers, i)
		return nil
	}
	heap.Push(&n.timers, timer{deadline: deadline, ref: inst.rec.Ref, kind: kind, gen: inst.generation})
	return nil
}

func (n *RawNode) broadcastPreAccept(inst *instance) {
	processAt := inst.processAt
	toq := recordUsesTOQ(inst.rec) && inst.rec.Ref.Replica == n.id && inst.rec.Ballot.IsInitialFor(inst.rec.Ref.Replica)
	for _, to := range n.votersForConf(inst.rec.Ref.Conf) {
		if to == n.id {
			continue
		}
		m := Message{Type: MsgPreAccept, From: n.id, To: to, Ref: inst.rec.Ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: inst.rec.Deps, Command: inst.rec.Command.Borrow(), ProcessAt: processAt, TOQ: recordUsesTOQ(inst.rec)}
		if toq {
			m.TOQ = true
			m.Seq = 0
			m.Deps = nil
		}
		n.enqueueMessage(m)
	}
}

func (n *RawNode) broadcastPrepare(rec InstanceRecord) {
	for _, to := range n.votersForConf(rec.Ref.Conf) {
		if to == n.id {
			continue
		}
		n.enqueueMessage(Message{Type: MsgPrepare, From: n.id, To: to, Ref: rec.Ref, Ballot: rec.Ballot})
	}
}

func (n *RawNode) broadcastTryPreAccept(inst *instance) {
	if len(inst.tryIgnored) == 0 {
		n.broadcast(MsgTryPreAccept, inst.rec)
		return
	}
	refs := make([]InstanceRef, 0, len(inst.tryIgnored))
	for ref := range inst.tryIgnored {
		refs = append(refs, ref)
	}
	sortRefs(refs)
	for _, ref := range refs {
		n.broadcastTryPreAcceptIgnore(inst, ref)
	}
}

func (n *RawNode) broadcastTryPreAcceptIgnore(inst *instance, ignore InstanceRef) {
	for _, to := range n.votersForConf(inst.rec.Ref.Conf) {
		if to == n.id {
			continue
		}
		m := Message{Type: MsgTryPreAccept, From: n.id, To: to, Ref: inst.rec.Ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: inst.rec.Deps, IgnoreDependency: TryPreAcceptIgnore{Ref: ignore}, Command: inst.rec.Command.Borrow(), ProcessAt: inst.rec.ProcessAt, TOQ: recordUsesTOQ(inst.rec)}
		n.enqueueMessage(m)
	}
}

func (n *RawNode) broadcast(t MessageType, rec InstanceRecord) {
	for _, to := range n.votersForConf(rec.Ref.Conf) {
		if to == n.id {
			continue
		}
		m := Message{Type: t, From: n.id, To: to, Ref: rec.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, Command: rec.Command.Borrow(), ProcessAt: rec.ProcessAt, TOQ: recordUsesTOQ(rec)}
		if t == MsgCommit {
			m.RecordBallot = recordTupleBallot(rec)
		}
		if t == MsgAccept || t == MsgCommit {
			m.AcceptSeq = rec.AcceptSeq
			m.AcceptDeps = rec.AcceptDeps
			m.AcceptEvidence = rec.AcceptEvidence
		}
		n.enqueueMessage(m)
	}
}

func (n *RawNode) sendReject(t MessageType, to ReplicaID, ref InstanceRef, hint Ballot) {
	reason := RejectNone
	if t == MsgTryPreAcceptResp {
		reason = RejectStaleBallot
	}
	m := Message{Type: t, From: n.id, To: to, Ref: ref, Ballot: hint, Reject: true, RejectReason: reason, RejectHint: hint, Deps: n.depsForConf(ref.Conf)}
	n.enqueueMessage(m)
}

func (n *RawNode) sendCommitTo(to ReplicaID, rec InstanceRecord) {
	m := Message{Type: MsgCommit, From: n.id, To: to, Ref: rec.Ref, Ballot: rec.Ballot, RecordBallot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps, AcceptSeq: rec.AcceptSeq, AcceptDeps: rec.AcceptDeps, AcceptEvidence: rec.AcceptEvidence, Command: rec.Command.Borrow(), ProcessAt: rec.ProcessAt, TOQ: recordUsesTOQ(rec)}
	n.enqueueMessage(m)
}

func (n *RawNode) readyTarget() *Ready {
	if n.awaitAdvance {
		return &n.nextReady
	}
	return &n.pendingReady
}

func mergeReadyQueue[T any](dst, src []T) ([]T, []T) {
	if len(src) == 0 {
		return dst, src
	}
	dst = append(dst, src...)
	clear(src)
	return dst, src[:0]
}

func reuseClearedReadyQueue[T any](dst, cleared []T) []T {
	if len(dst) == 0 && cap(cleared) > cap(dst) {
		return cleared[:0]
	}
	return dst
}

func (n *RawNode) recycleFrozenReady() {
	n.pendingReady.ConfigHistory = reuseClearedReadyQueue(n.pendingReady.ConfigHistory, n.frozenReady.ConfigHistory)
	n.pendingReady.Records = reuseClearedReadyQueue(n.pendingReady.Records, n.frozenReady.Records)
	n.pendingReady.BootstrapRecords = reuseClearedReadyQueue(n.pendingReady.BootstrapRecords, n.frozenReady.BootstrapRecords)
	n.pendingReady.FrontierUpdates = reuseClearedReadyQueue(n.pendingReady.FrontierUpdates, n.frozenReady.FrontierUpdates)
	n.pendingReady.Messages = reuseClearedReadyQueue(n.pendingReady.Messages, n.frozenReady.Messages)
	n.pendingReady.BootstrapMessages = reuseClearedReadyQueue(n.pendingReady.BootstrapMessages, n.frozenReady.BootstrapMessages)
	n.pendingReady.Committed = reuseClearedReadyQueue(n.pendingReady.Committed, n.frozenReady.Committed)
	n.frozenReady = Ready{}
}

func (n *RawNode) mergeNextReady() {
	n.pendingReady.ConfigHistory, n.nextReady.ConfigHistory = mergeReadyQueue(n.pendingReady.ConfigHistory, n.nextReady.ConfigHistory)
	n.pendingReady.Records, n.nextReady.Records = mergeReadyQueue(n.pendingReady.Records, n.nextReady.Records)
	n.pendingReady.BootstrapRecords, n.nextReady.BootstrapRecords = mergeReadyQueue(n.pendingReady.BootstrapRecords, n.nextReady.BootstrapRecords)
	if n.nextReady.LocalVoterState != nil {
		state := n.nextReady.LocalVoterState.Clone()
		n.pendingReady.LocalVoterState = &state
		n.nextReady.LocalVoterState = nil
	}
	n.pendingReady.FrontierUpdates, n.nextReady.FrontierUpdates = mergeReadyQueue(n.pendingReady.FrontierUpdates, n.nextReady.FrontierUpdates)
	n.pendingReady.AllocatorFloor = maxInstanceNum(n.pendingReady.AllocatorFloor, n.nextReady.AllocatorFloor)
	n.nextReady.AllocatorFloor = 0
	n.pendingReady.Messages, n.nextReady.Messages = mergeReadyQueue(n.pendingReady.Messages, n.nextReady.Messages)
	n.pendingReady.BootstrapMessages, n.nextReady.BootstrapMessages = mergeReadyQueue(n.pendingReady.BootstrapMessages, n.nextReady.BootstrapMessages)
	n.pendingReady.Committed, n.nextReady.Committed = mergeReadyQueue(n.pendingReady.Committed, n.nextReady.Committed)
	n.pendingReady.MustSync = readyHasDurableUpdate(n.pendingReady)
	n.nextReady.MustSync = false
}

func (n *RawNode) enqueueRecord(rec InstanceRecord) {
	if rec.Checksum == ([32]byte{}) {
		rec.Checksum = ChecksumRecord(rec)
	}
	target := n.readyTarget()
	target.Records = append(target.Records, n.readyRecord(rec))
	target.MustSync = true
}

func (n *RawNode) enqueueMessage(m Message) {
	if len(m.Deps) == 0 && (m.Type == MsgPreAcceptResp || m.Type == MsgAcceptResp || m.Type == MsgPrepareResp || m.Type == MsgTryPreAcceptResp || m.Type == MsgEvidenceResp) {
		m.Deps = n.depsForConf(m.Ref.Conf)
	}
	m.Checksum = ChecksumMessage(m)
	target := n.readyTarget()
	target.Messages = append(target.Messages, m.Clone())
}

func (n *RawNode) enqueueCommitted(c CommittedCommand) {
	target := n.readyTarget()
	target.Committed = append(target.Committed, c.Clone())
}

func (n *RawNode) readyRecord(rec InstanceRecord) InstanceRecord {
	out := rec
	out.ConfChangeResult.Conf = rec.ConfChangeResult.Conf.Clone()
	out.MembershipResult.Successor = rec.MembershipResult.Successor.Clone()
	out.Deps = append([]InstanceNum(nil), rec.Deps...)
	out.AcceptDeps = append([]InstanceNum(nil), rec.AcceptDeps...)
	out.AcceptEvidence = cloneAcceptEvidence(rec.AcceptEvidence)
	if n.zeroCopy {
		out.Command = rec.Command.Borrow()
	} else {
		out.Command = rec.Command.Clone()
	}
	return out
}

func inboundCommand(cmd Command) Command { return cmd.Clone() }


func (n *RawNode) needsRecordLoad(ref InstanceRef) bool {
	if ref.IsZero() || ref.Instance == 0 {
		return false
	}
	if n.instances[ref] != nil {
		return false
	}
	lane := laneFor(ref)
	return ref.Instance <= n.engine.foldedThrough(lane)
}

// deferRecordLoad queues m until ProvideRecordLoad(ref). Returns false if capacity exceeded.
func (n *RawNode) deferRecordLoad(ref InstanceRef, m Message) error {
	if n.pendingRecordLoads == nil {
		n.pendingRecordLoads = make(map[InstanceRef]*recordLoadWait)
	}
	wait := n.pendingRecordLoads[ref]
	if wait == nil {
		if n.pendingRecordLoadMessages >= n.maxDeferredRecordLoads {
			return fmt.Errorf("%w: %w", ErrMessageRejected, ErrDeferredRecordLoadFull)
		}
		wait = &recordLoadWait{}
		n.pendingRecordLoads[ref] = wait
		// enqueue request into next Ready batch
		target := n.readyTarget()
		target.RecordLoads = append(target.RecordLoads, ref)
		// keep sorted unique later at Ready() freeze; also sort now for stability
		sortRefs(target.RecordLoads)
		// dedup adjacent
		out := target.RecordLoads[:0]
		var last InstanceRef
		for i, r := range target.RecordLoads {
			if i == 0 || r != last {
				out = append(out, r)
				last = r
			}
		}
		target.RecordLoads = out
		wait.requested = true
	}
	if n.pendingRecordLoadMessages >= n.maxDeferredRecordLoads {
		return fmt.Errorf("%w: %w", ErrMessageRejected, ErrDeferredRecordLoadFull)
	}
	wait.messages = append(wait.messages, m.Clone())
	n.pendingRecordLoadMessages++
	return nil
}

// ProvideRecordLoad supplies a durable record (or miss) for a prior RecordLoads request.
func (n *RawNode) ProvideRecordLoad(res RecordLoadResult) error {
	wait := n.pendingRecordLoads[res.Ref]
	if wait == nil {
		return ErrUnrequestedRecordLoad
	}
	if !res.Found {
		n.recordLoadMisses++
		n.pendingRecordLoadMessages -= len(wait.messages)
		delete(n.pendingRecordLoads, res.Ref)
		return nil
	}
	rec := res.Record
	if rec.Ref != res.Ref && !res.Ref.IsZero() {
		// allow Ref on result to define
		if rec.Ref.IsZero() {
			rec.Ref = res.Ref
		}
	}
	if err := validateRecordChecksum(rec); err != nil {
		return err
	}
	// install through chokepoint
	inst := n.instances[rec.Ref]
	if inst == nil {
		inst = &instance{rec: rec, phase: phaseFromStatus(rec.Status)}
		n.installInstance(inst)
	} else {
		n.setInstanceRecord(inst, rec)
	}
	msgs := wait.messages
	n.pendingRecordLoadMessages -= len(msgs)
	delete(n.pendingRecordLoads, res.Ref)
	for _, msg := range msgs {
		if err := n.Step(msg); err != nil {
			return err
		}
	}
	n.maybeRefoldLoaded(rec.Ref)
	return nil
}

func (n *RawNode) maybeRefoldLoaded(ref InstanceRef) {
	inst := n.instances[ref]
	if inst == nil || inst.rec.Status != StatusExecuted {
		return
	}
	if ref.Instance > n.engine.foldedThrough(laneFor(ref)) {
		return
	}
	rec := inst.rec.Clone()
	n.engine.foldRecord(rec)
	delete(n.instances, ref)
	n.foldedInstances++
}

func validateRecordChecksum(rec InstanceRecord) error {
	if rec.Checksum == ([32]byte{}) {
		return ErrInvalidRecord
	}
	want := ChecksumRecord(rec)
	if rec.Checksum != want {
		return ErrInvalidRecord
	}
	return nil
}

func (n *RawNode) setInstanceRecord(inst *instance, rec InstanceRecord) {
	previous := inst.rec.Clone()
	n.engine.apply(&previous, rec)
	inst.rec = rec
}

func (n *RawNode) computeAttrs(cmd Command, exclude InstanceRef) Attributes {
	return n.computeAttrsAt(cmd, exclude, 0, false)
}

// VisitConflicts yields resident in-flight instances that conflict with cmd.
// Folded history is not enumerated. yield returns false to stop early.
// The walk is designed to avoid heap allocation in the steady state.
func (n *RawNode) VisitConflicts(cmd Command, yield func(InstanceRef, Status) bool) {
	if n == nil || yield == nil || cmd.Kind == CommandNoop {
		return
	}
	confID := n.currentHardState.Conf.ID
	stop := false
	visit := func(ref InstanceRef, status Status) bool {
		if !yield(ref, status) {
			stop = true
			return false
		}
		return true
	}
	if commandHasGlobalConflictScope(cmd.Kind) {
		n.engine.lanes(confID, func(lane instanceLane) bool {
			if stop {
				return false
			}
			resident, _ := n.engine.maxEligibleAny(lane)
			n.engine.walkDesc(lane, resident, func(instance InstanceNum, slot laneSlot) bool {
				if stop || !slot.eligible() {
					return !stop
				}
				ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
				inst := n.instances[ref]
				if inst == nil || !commandsConflict(cmd, inst.rec.Command) {
					return true
				}
				return visit(ref, inst.rec.Status)
			})
			return !stop
		})
		return
	}
	n.engine.keyLaneSet(confID, cmd.ConflictKeys, func(lane instanceLane) bool {
		if stop {
			return false
		}
		for _, key := range cmd.ConflictKeys {
			if stop {
				break
			}
			resident, _ := n.engine.keyMax(confID, key, lane)
			n.engine.walkKeyDesc(confID, key, lane, resident, func(instance InstanceNum, slot laneSlot) bool {
				if stop || !slot.eligible() {
					return !stop
				}
				ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
				inst := n.instances[ref]
				if inst == nil || !commandsConflict(cmd, inst.rec.Command) {
					return true
				}
				return visit(ref, inst.rec.Status)
			})
		}
		return !stop
	})
	if stop {
		return
	}
	n.engine.lanes(confID, func(lane instanceLane) bool {
		if stop {
			return false
		}
		resident, _ := n.engine.globalMax(lane)
		n.engine.walkGlobalDesc(lane, resident, func(instance InstanceNum, _ laneSlot) bool {
			if stop {
				return false
			}
			ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
			inst := n.instances[ref]
			if inst == nil || !commandsConflict(cmd, inst.rec.Command) {
				return true
			}
			return visit(ref, inst.rec.Status)
		})
		return !stop
	})
}

func (n *RawNode) computeAttrsAt(cmd Command, exclude InstanceRef, processAt uint64, timedPreAccept bool) Attributes {
	conf := n.confFor(exclude.Conf)
	deps := make([]InstanceNum, len(conf.Voters))
	if cmd.Kind == CommandNoop {
		return Attributes{Seq: 1, Deps: deps}
	}

	addRef := func(ref InstanceRef, rec InstanceRecord) bool {
		if ref == exclude || rec.TOQPending || rec.Status == StatusNone || rec.Command.Kind == CommandNoop || ref.Conf != exclude.Conf {
			return false
		}
		if !commandsConflict(cmd, rec.Command) {
			return false
		}
		if n.skipTimedConflict(cmd, exclude, processAt, timedPreAccept, ref, rec) {
			return false
		}
		idx, ok := conf.Index(ref.Replica)
		if !ok {
			return false
		}
		deps[idx] = max(deps[idx], ref.Instance)
		return true
	}
	addRetired := func(lane instanceLane, instance InstanceNum) {
		if instance == 0 || lane.conf != exclude.Conf {
			return
		}
		idx, ok := conf.Index(lane.replica)
		if ok {
			deps[idx] = max(deps[idx], instance)
		}
	}
	// Key-scoped: walk only that key's postings descending from its resident max.
	walkKey := func(lane instanceLane, key []byte, from InstanceNum) {
		if from == 0 || lane.conf != exclude.Conf {
			return
		}
		n.engine.walkKeyDesc(exclude.Conf, key, lane, from, func(instance InstanceNum, slot laneSlot) bool {
			if !slot.eligible() {
				return true
			}
			ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
			inst := n.instances[ref]
			if inst == nil {
				return true
			}
			_ = addRef(ref, inst.rec)
			// continue: need max across candidates; do not stop at first
			return true
		})
	}
	walkGlobal := func(lane instanceLane, from InstanceNum) {
		if from == 0 || lane.conf != exclude.Conf {
			return
		}
		n.engine.walkGlobalDesc(lane, from, func(instance InstanceNum, _ laneSlot) bool {
			ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
			inst := n.instances[ref]
			if inst == nil {
				addRetired(lane, instance)
				return true
			}
			_ = addRef(ref, inst.rec)
			return true
		})
	}

	if commandHasGlobalConflictScope(cmd.Kind) {
		n.engine.lanes(exclude.Conf, func(lane instanceLane) bool {
			resident, retired := n.engine.maxEligibleAny(lane)
			// Global-scope commands depend on any eligible conflict, not only global-flagged records.
			if resident != 0 {
				n.engine.walkDesc(lane, resident, func(instance InstanceNum, slot laneSlot) bool {
					if !slot.eligible() {
						return true
					}
					ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: instance}
					inst := n.instances[ref]
					if inst == nil {
						return true
					}
					_ = addRef(ref, inst.rec)
					return true
				})
			}
			addRetired(lane, retired)
			return true
		})
	} else {
		n.engine.keyLaneSet(exclude.Conf, cmd.ConflictKeys, func(lane instanceLane) bool {
			for _, key := range cmd.ConflictKeys {
				resident, retired := n.engine.keyMax(exclude.Conf, key, lane)
				walkKey(lane, key, resident)
				addRetired(lane, retired)
			}
			return true
		})
		n.engine.lanes(exclude.Conf, func(lane instanceLane) bool {
			resident, retired := n.engine.globalMax(lane)
			walkGlobal(lane, resident)
			addRetired(lane, retired)
			return true
		})
	}

	seq := uint64(1)
	for idx, through := range deps {
		if through == 0 {
			continue
		}
		lane := instanceLane{conf: exclude.Conf, replica: conf.Voters[idx]}
		seq = max(seq, saturatingSeqIncrement(n.engine.prefixMaxSeq(lane, through)))
	}
	return Attributes{Seq: seq, Deps: deps}
}


func (n *RawNode) skipTimedConflict(cmd Command, candidate InstanceRef, processAt uint64, timedPreAccept bool, ref InstanceRef, rec InstanceRecord) bool {
	if !timedPreAccept || cmd.Kind != CommandUser || rec.Command.Kind != CommandUser {
		return false
	}
	candidateDomain := TimingDomainLogical
	if n.toqEnabled {
		candidateDomain = TimingDomainTOQ
	}
	inst := n.instances[ref]
	if inst == nil || inst.rec.Status != StatusPreAccepted ||
		inst.rec.TimingDomain == TimingDomainUntimed ||
		inst.rec.TimingDomain != candidateDomain {
		return false
	}
	return preAcceptOrderBefore(processAt, candidate, inst.rec.ProcessAt, ref)
}

func (n *RawNode) fastCommitAttrsPreAccept(inst *instance) (Attributes, bool) {
	localAttrs := inst.rec.Attributes()
	if n.canFastCommitPreAccept(inst, localAttrs) {
		return localAttrs, true
	}
	if !inst.rec.Ballot.IsInitialFor(inst.rec.Ref.Replica) {
		return Attributes{}, false
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	for _, id := range conf.Voters {
		if id == n.id {
			continue
		}
		vote, ok := inst.preOK.get(conf, id)
		if !ok {
			continue
		}
		attrs := vote.attributes()
		if attrs.Equal(localAttrs) || !attrsCover(localAttrs, attrs) {
			continue
		}
		if n.canFastCommitPreAccept(inst, attrs) {
			return attrs, true
		}
	}
	return Attributes{}, false
}

func (n *RawNode) canFastCommitPreAccept(inst *instance, attrs Attributes) bool {
	if !inst.rec.FastPathEligible || !inst.rec.Ballot.IsInitialFor(inst.rec.Ref.Replica) {
		return false
	}
	localAttrs := inst.rec.Attributes()
	if !attrs.Equal(localAttrs) && !attrsCover(localAttrs, attrs) {
		return false
	}
	count := 1
	covered := n.committedDepsMask(attrs, inst.rec.Ref.Conf)
	conf := n.confFor(inst.rec.Ref.Conf)
	inst.preOK.each(conf, func(id ReplicaID, vote *attrVote) bool {
		if id != n.id && vote.attributes().Equal(attrs) && vote.fastPathEligible {
			count++
			covered |= vote.depsCommitted
		}
		return true
	})
	return count >= n.fastQuorumForConf(inst.rec.Ref.Conf) && dependencyMask(attrs.Deps)&^covered == 0
}

func attrsCover(base, candidate Attributes) bool {
	if candidate.Seq < base.Seq || len(candidate.Deps) < len(base.Deps) {
		return false
	}
	for i, dep := range base.Deps {
		if candidate.Deps[i] < dep {
			return false
		}
	}
	return true
}

func dependencyMask(deps []InstanceNum) uint64 {
	var mask uint64
	for i, dep := range deps {
		if dep != 0 {
			mask |= uint64(1) << uint(i)
		}
	}
	return mask
}

func (n *RawNode) committedDepsMask(attrs Attributes, confID ConfID) uint64 {
	conf := n.confFor(confID)
	var mask uint64
	for i, dep := range attrs.Deps {
		if dep == 0 || i >= len(conf.Voters) {
			continue
		}
		if n.committedPrefix(conf.Voters[i], dep, confID) {
			mask |= uint64(1) << uint(i)
		}
	}
	return mask
}
func (n *RawNode) committedPrefix(replica ReplicaID, through InstanceNum, confID ConfID) bool {
	return through == 0 || n.committedThrough[instanceLane{conf: confID, replica: replica}] >= through
}

func addTryCandidate(candidates []tryCandidate, conf ConfState, rec InstanceRecord, voter ReplicaID) []tryCandidate {
	for i := range candidates {
		if candidates[i].rec.ProcessAt == rec.ProcessAt && candidates[i].rec.TimingDomain == rec.TimingDomain && commandEqual(candidates[i].rec.Command, rec.Command) && candidates[i].rec.Attributes().Equal(rec.Attributes()) {
			candidates[i].voters.add(conf, voter)
			return candidates
		}
	}
	candidate := tryCandidate{rec: rec.Clone()}
	candidate.voters.add(conf, voter)
	return append(candidates, candidate)
}

func attrsFromVotes(conf ConfState, votes *attrVoteSet) Attributes {
	var out Attributes
	votes.each(conf, func(_ ReplicaID, vote *attrVote) bool {
		out = mergeAttrs(out, vote.attributes())
		return true
	})
	return out
}

func mergeAttrs(a, b Attributes) Attributes {
	out := a.Clone()
	if b.Seq > out.Seq {
		out.Seq = b.Seq
	}
	if len(b.Deps) > len(out.Deps) {
		grown := make([]InstanceNum, len(b.Deps))
		copy(grown, out.Deps)
		out.Deps = grown
	}
	for i, dep := range b.Deps {
		if dep > out.Deps[i] {
			out.Deps[i] = dep
		}
	}
	if out.Seq == 0 {
		out.Seq = 1
	}
	return out
}

func lessRef(a, b InstanceRef) bool {
	if a.Conf != b.Conf {
		return a.Conf < b.Conf
	}
	if a.Replica != b.Replica {
		return a.Replica < b.Replica
	}
	return a.Instance < b.Instance
}

func (n *RawNode) rebroadcastCommits() {
	refs := make([]InstanceRef, 0, len(n.instances))
	for ref, inst := range n.instances {
		if inst.rec.Status >= StatusCommitted {
			refs = append(refs, ref)
		}
	}
	sortRefs(refs)
	for _, ref := range refs {
		n.broadcast(MsgCommit, n.instances[ref].rec)
	}
}

func (n *RawNode) wouldReplaceInstance(m Message, current *instance, ordered bool) bool {
	switch m.Type {
	case MsgPreAccept:
		if current == nil {
			return true
		}
		if current.rec.Status >= StatusCommitted || m.Ballot.Less(current.rec.Ballot) {
			return false
		}
		if m.Ballot == current.rec.Ballot &&
			(!commandEqual(current.rec.Command, m.Command) || current.rec.ProcessAt != m.ProcessAt || current.rec.TimingDomain != timingDomainFromMessage(m)) {
			return false
		}
		if current.rec.Status == StatusAccepted {
			return false
		}
		if current.rec.Status == StatusPreAccepted && m.Ballot == current.rec.Ballot && current.rec.TimingDomain == timingDomainFromMessage(m) {
			return false
		}
		rec, _ := n.preAcceptRecord(m, ordered)
		return current.rec.Status != StatusPreAccepted || !instanceRecordEqual(current.rec, rec)
	case MsgAccept:
		if current == nil {
			return true
		}
		if current.rec.Status >= StatusCommitted || m.Ballot.Less(current.rec.Ballot) {
			return false
		}
		if current.rec.Status == StatusPreAccepted && current.rec.RecordBallot == m.Ballot && !phaseMessageValueEqual(current.rec, m) {
			return false
		}
		if current.rec.Status == StatusAccepted && m.Ballot == current.rec.Ballot && current.rec.RecordBallot == m.Ballot {
			return false
		}
		rec := n.acceptRecord(m, current)
		return current.rec.Status != StatusAccepted || !instanceRecordEqual(current.rec, rec)
	case MsgPreAcceptResp, MsgAcceptResp, MsgCommit, MsgPrepare, MsgPrepareResp, MsgTryPreAccept, MsgTryPreAcceptResp, MsgEvidence, MsgEvidenceResp:
		fallthrough
	default:
		return false
	}
}

func (n *RawNode) tryCommittedConflictResponseWouldAdvance(inst *instance, conflictRef InstanceRef) bool {
	if conflictRef.IsZero() ||
		conflictRef.Conf != inst.rec.Ref.Conf ||
		!n.attrsDependsOn(inst.rec.Attributes(), conflictRef, inst.rec.Ref.Conf) ||
		len(inst.tryIgnored) > 0 ||
		n.faultTolerance(inst.rec.Ref.Conf) <= 0 {
		return true
	}
	key := tryEvidenceKey{target: inst.rec.Ref, conflict: conflictRef, ballot: inst.rec.Ballot}
	for existingKey := range n.tryEvidenceChecks {
		if existingKey.target != inst.rec.Ref {
			continue
		}
		return existingKey != key
	}
	return len(n.tryEvidenceChecks) >= n.maxConcurrentRecoveries
}

func (n *RawNode) startAcceptGenerationDelta(inst *instance) uint64 {
	if inst != nil && n.slowQuorumForConf(inst.rec.Ref.Conf) == 1 {
		return 2
	}
	return 1
}

func (n *RawNode) prepareResponseGenerationDelta(m Message, inst *instance) uint64 {
	if inst == nil || m.Reject || inst.rec.Ballot.Less(m.Ballot) {
		return 1
	}
	temp := *inst
	conf := n.confFor(inst.rec.Ref.Conf)
	var tempPrepare recordVoteSet
	if inst.prepareOK != nil {
		tempPrepare = *inst.prepareOK
	}
	temp.prepareOK = &tempPrepare
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.RecordBallot, Status: m.RecordStatus, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptSeq: m.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), m.AcceptDeps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: m.Command.Clone(), FastPathEligible: m.FastPathEligible, ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
	n.ensureRecordDeps(&rec)
	temp.prepareOK.add(conf, m.From, rec)
	var responderStorage [7]ReplicaID
	responders := responderStorage[:0]
	for _, id := range conf.Voters {
		if _, ok := temp.prepareOK.get(conf, id); ok {
			responders = append(responders, id)
			continue
		}
		if _, ok := temp.prepareEvidence.get(conf, id); ok {
			responders = append(responders, id)
		}
	}
	for _, id := range responders {
		if prepareResponseRecord(&temp, conf, id).Status >= StatusCommitted {
			return 1
		}
	}
	for _, id := range responders {
		if prepareResponseRecord(&temp, conf, id).Status == StatusAccepted {
			return n.startAcceptGenerationDelta(inst)
		}
	}
	foundPreAccepted := false
	var highestPreAccepted Ballot
	for _, id := range responders {
		rec := prepareResponseRecord(&temp, conf, id)
		if rec.Status != StatusPreAccepted {
			continue
		}
		if ballot := recordTupleBallot(rec); !foundPreAccepted || highestPreAccepted.Less(ballot) {
			highestPreAccepted = ballot
			foundPreAccepted = true
		}
	}
	if !foundPreAccepted {
		return n.startAcceptGenerationDelta(inst)
	}
	var candidates []tryCandidate
	for _, id := range responders {
		rec := prepareResponseRecord(&temp, conf, id)
		if rec.Status == StatusPreAccepted && recordTupleBallot(rec) == highestPreAccepted && rec.FastPathEligible {
			candidates = addTryCandidate(candidates, conf, rec, id)
		}
	}
	for _, candidate := range candidates {
		if candidate.voters.len() < n.tryWitnessQuorumForConf(inst.rec.Ref.Conf) {
			continue
		}
		tryOK := candidate.voters
		tryOK.add(conf, inst.rec.Ref.Replica)
		if local, ok := temp.prepareOK.get(conf, n.id); ok &&
			local.FastPathEligible &&
			commandEqual(local.Command, candidate.rec.Command) &&
			local.Attributes().Equal(candidate.rec.Attributes()) {
			tryOK.add(conf, n.id)
		}
		delta := uint64(1)
		if tryOK.len() >= n.slowQuorumForConf(inst.rec.Ref.Conf) {
			delta += n.startAcceptGenerationDelta(inst)
		}
		return delta
	}
	return n.startAcceptGenerationDelta(inst)
}

func (n *RawNode) responseTimingTransition(m Message) (*instance, bool, bool, bool) {
	inst := n.instances[m.Ref]
	switch m.Type {
	case MsgPreAcceptResp:
		if inst == nil || inst.phase != phasePreAccept || m.Ref.Replica != n.id || inst.rec.Ballot.Replica != n.id {
			return inst, false, false, false
		}
		if m.Reject {
			advance := inst.rec.Ballot.Less(m.RejectHint)
			return inst, advance, false, advance
		}
		if m.Ballot != inst.rec.Ballot {
			return inst, false, false, false
		}
		conf := n.confFor(inst.rec.Ref.Conf)
		if inst.preOK.has(conf, m.From) {
			return inst, false, false, false
		}
		temp := *inst
		var tempVotes attrVoteSet
		if inst.preOK != nil {
			tempVotes = *inst.preOK
		}
		temp.preOK = &tempVotes
		vote, ok := newAttrVote(conf, m.Seq, m.Deps, m.DepsCommitted, m.FastPathEligible)
		if !ok || !temp.preOK.add(conf, m.From, vote) {
			return inst, false, false, false
		}
		advance, retry := n.preAcceptVotesWouldAdvance(&temp)
		return inst, advance, retry, false
	case MsgAcceptResp:
		if inst == nil || inst.phase != phaseAccept || !n.coordinatesInstance(inst) {
			return inst, false, false, false
		}
		if m.Reject {
			advance := inst.rec.Ballot.Less(m.RejectHint)
			return inst, advance, false, advance
		}
		if m.Ballot != inst.rec.Ballot ||
			m.RecordBallot != inst.rec.RecordBallot ||
			m.RecordStatus != StatusAccepted ||
			m.Seq != inst.rec.Seq ||
			!instanceNumsEqual(m.Deps, inst.rec.Deps) {
			return inst, false, false, false
		}
		conf := n.confFor(inst.rec.Ref.Conf)
		if inst.accOK.has(conf, m.From) {
			return inst, false, false, false
		}
		return inst, inst.accOK.len()+1 >= n.slowQuorumForConf(inst.rec.Ref.Conf), false, false
	case MsgPrepareResp:
		if inst == nil || inst.phase != phasePrepare || !inst.rec.Ballot.IsRecovery() || inst.rec.Ballot.Replica != n.id {
			return inst, false, false, false
		}
		if m.Reject {
			advance := inst.rec.Ballot.Less(m.RejectHint)
			return inst, advance, false, advance
		}
		if m.Ballot.Less(inst.rec.Ballot) {
			return inst, false, false, false
		}
		if inst.rec.Ballot.Less(m.Ballot) {
			return inst, true, false, true
		}
		conf := n.confFor(inst.rec.Ref.Conf)
		if _, duplicate := inst.prepareOK.get(conf, m.From); duplicate {
			return inst, false, false, false
		}
		if recordHasValue(m.RecordStatus) && m.RecordBallot == (Ballot{}) {
			return inst, false, false, false
		}
		if inst.prepareOK.len()+1 < n.slowQuorumForConf(inst.rec.Ref.Conf) {
			return inst, false, false, false
		}
		retry := n.slowQuorumForConf(inst.rec.Ref.Conf) > 1
		if m.RecordStatus >= StatusCommitted {
			retry = false
		}
		if retry {
			inst.prepareOK.each(conf, func(_ ReplicaID, rec *InstanceRecord) bool {
				if rec.Status >= StatusCommitted {
					retry = false
					return false
				}
				return true
			})
		}
		if retry {
			inst.prepareEvidence.each(conf, func(_ ReplicaID, rec *InstanceRecord) bool {
				if rec.Status >= StatusCommitted {
					retry = false
					return false
				}
				return true
			})
		}
		return inst, true, retry, false
	case MsgTryPreAcceptResp:
		if inst == nil || inst.phase != phaseTryPreAccept || !n.coordinatesInstance(inst) {
			return inst, false, false, false
		}
		if m.Reject {
			switch m.RejectReason {
			case RejectStaleBallot, RejectNone:
				advance := inst.rec.Ballot.Less(m.RejectHint)
				return inst, advance, false, advance
			case RejectAcceptedTarget:
				advance := m.Ballot == inst.rec.Ballot
				return inst, advance, false, advance
			case RejectCommittedConflict:
				advance := m.ConflictStatus < StatusCommitted || n.tryCommittedConflictResponseWouldAdvance(inst, m.ConflictRef)
				return inst, advance, advance, false
			case RejectUncommittedConflict:
				advance := !m.ConflictRef.IsZero() && n.tryConflictForcesSlowAccept(inst, m.ConflictRef)
				return inst, advance, advance, false
			}
			return inst, false, false, false
		}
		if m.Ballot != inst.rec.Ballot ||
			m.RecordStatus != StatusPreAccepted ||
			m.Seq != inst.rec.Seq ||
			!instanceNumsEqual(m.Deps, inst.rec.Deps) {
			return inst, false, false, false
		}
		conf := n.confFor(inst.rec.Ref.Conf)
		if inst.tryOK.has(conf, m.From) {
			return inst, false, false, false
		}
		advance := inst.tryOK.len()+1 >= n.slowQuorumForConf(inst.rec.Ref.Conf)
		return inst, advance, advance, false
	case MsgEvidenceResp:
		key := tryEvidenceKey{target: m.ConflictRef, conflict: m.Ref, ballot: m.Ballot}
		records, ok := n.tryEvidenceChecks[key]
		if !ok {
			return nil, false, false, false
		}
		inst = n.instances[key.target]
		if inst == nil || inst.phase != phaseTryPreAccept || !n.coordinatesInstance(inst) || inst.rec.Ballot != key.ballot {
			return inst, false, false, false
		}
		if _, duplicate := records[m.From]; duplicate {
			return inst, false, false, false
		}
		prospective := make(map[ReplicaID]InstanceRecord, len(records)+1)
		for id, rec := range records {
			prospective[id] = rec
		}
		prospective[m.From] = InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.RecordBallot, Status: m.RecordStatus, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptSeq: m.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), m.AcceptDeps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: m.Command.Clone(), FastPathEligible: m.FastPathEligible, ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
		_, failClosed := n.tryEvidenceDecision(inst, key, prospective)
		return inst, failClosed, failClosed, false
	case MsgPreAccept, MsgAccept, MsgCommit, MsgPrepare, MsgTryPreAccept, MsgEvidence:
		fallthrough
	default:
		return inst, false, false, false
	}
}

func (n *RawNode) preflightTimingStep(m Message) error {
	if n.preAcceptWouldDefer(m) {
		return nil
	}
	inst := n.instances[m.Ref]
	advanceGeneration := false
	retryTimer := false
	recoveryTimer := false
	switch m.Type {
	case MsgPreAccept, MsgAccept:
		recoveryTimer = n.wouldReplaceInstance(m, inst, false)
		advanceGeneration = inst != nil && recoveryTimer
	case MsgPrepare:
		recoveryTimer = (inst == nil || !inst.rec.TOQPending) &&
			m.Ballot.IsRecovery() && m.Ballot.Replica != n.id &&
			(inst == nil || inst.rec.Ballot.Less(m.Ballot))
		advanceGeneration = inst != nil &&
			!inst.rec.TOQPending &&
			inst.rec.Ballot.Less(m.Ballot) &&
			m.Ballot.Replica != n.id
	case MsgTryPreAccept:
		switch {
		case inst == nil:
			recoveryTimer = true
		case inst.rec.Status >= StatusCommitted || m.Ballot.Less(inst.rec.Ballot):
		case inst.rec.Status == StatusAccepted:
			advanceGeneration = inst.rec.Ballot.Less(m.Ballot) && m.Ballot.Replica != n.id
			recoveryTimer = advanceGeneration
		case inst.rec.Status == StatusPreAccepted && m.Ballot == inst.rec.Ballot && inst.rec.RecordBallot == m.Ballot:
		default:
			if _, _, conflict := n.tryPreAcceptConflict(m); !conflict {
				rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.Ballot, Status: StatusPreAccepted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: inboundCommand(m.Command), ProcessAt: m.ProcessAt, TimingDomain: timingDomainFromMessage(m)}
				rec.Checksum = ChecksumRecord(rec)
				advanceGeneration = inst.rec.Status != StatusPreAccepted || !instanceRecordEqual(inst.rec, rec)
				recoveryTimer = advanceGeneration
			}
		}
	case MsgPreAcceptResp, MsgAcceptResp, MsgPrepareResp, MsgTryPreAcceptResp, MsgEvidenceResp:
		inst, advanceGeneration, retryTimer, recoveryTimer = n.responseTimingTransition(m)
	case MsgCommit, MsgEvidence:
	}
	generationDelta := uint64(0)
	if advanceGeneration {
		generationDelta = 1
		switch m.Type {
		case MsgPrepareResp:
			generationDelta = n.prepareResponseGenerationDelta(m, inst)
		case MsgPreAcceptResp, MsgTryPreAcceptResp, MsgEvidenceResp:
			if retryTimer {
				generationDelta = n.startAcceptGenerationDelta(inst)
			}
		case MsgPreAccept, MsgAccept, MsgAcceptResp, MsgCommit, MsgPrepare, MsgTryPreAccept, MsgEvidence:
		}
	}
	if !advanceGeneration && !retryTimer && !recoveryTimer {
		return nil
	}
	if inst != nil && generationDelta > ^uint64(0)-inst.generation {
		return ErrLogicalTimeExhausted
	}
	if retryTimer {
		if _, err := checkedLogicalDeadline(n.tick, n.retryTicks); err != nil {
			return err
		}
	}
	if recoveryTimer {
		if _, err := checkedLogicalDeadline(n.tick, n.recoveryTicks); err != nil {
			return err
		}
	}
	return nil
}

func (n *RawNode) preflightRecoveryStep(m Message) error {
	inst := n.instances[m.Ref]
	if inst == nil {
		return nil
	}
	base := Ballot{}
	switch m.Type {
	case MsgPreAcceptResp:
		if m.Reject &&
			inst.phase == phasePreAccept &&
			m.Ref.Replica == n.id &&
			inst.rec.Ballot.Replica == n.id &&
			inst.rec.Ballot.Less(m.RejectHint) {
			base = m.RejectHint
		}
	case MsgAcceptResp:
		if m.Reject &&
			inst.phase == phaseAccept &&
			n.coordinatesInstance(inst) &&
			inst.rec.Ballot.Less(m.RejectHint) {
			base = m.RejectHint
		}
	case MsgPrepareResp:
		if inst.phase == phasePrepare &&
			inst.rec.Ballot.IsRecovery() &&
			inst.rec.Ballot.Replica == n.id {
			if m.Reject && inst.rec.Ballot.Less(m.RejectHint) {
				base = m.RejectHint
			} else if !m.Reject && inst.rec.Ballot.Less(m.Ballot) {
				base = m.Ballot
			}
		}
	case MsgTryPreAcceptResp:
		if inst.phase == phaseTryPreAccept && n.coordinatesInstance(inst) {
			if (m.RejectReason == RejectStaleBallot || m.RejectReason == RejectNone) &&
				m.Reject &&
				inst.rec.Ballot.Less(m.RejectHint) {
				base = m.RejectHint
			} else if m.Reject &&
				m.RejectReason == RejectAcceptedTarget &&
				m.Ballot == inst.rec.Ballot {
				base = inst.rec.Ballot
			}
		}
	case MsgPreAccept, MsgAccept, MsgCommit, MsgPrepare, MsgTryPreAccept, MsgEvidence, MsgEvidenceResp:
	}
	if base == (Ballot{}) {
		return nil
	}
	_, err := base.Next(n.id)
	return err
}

func (n *RawNode) dependencyKnownAfter(base, other InstanceRef, minStatus Status) bool {
	if base.Conf != other.Conf {
		return false
	}
	baseInst := n.instances[base]
	otherInst := n.instances[other]
	if baseInst == nil || otherInst == nil || otherInst.rec.Status < minStatus {
		return false
	}
	if otherInst.rec.Seq <= baseInst.rec.Seq {
		return false
	}
	conf := n.confFor(base.Conf)
	idx, ok := conf.Index(base.Replica)
	if !ok || idx >= len(otherInst.rec.Deps) {
		return false
	}
	return otherInst.rec.Deps[idx] >= base.Instance
}

func (n *RawNode) applyConfChange(ref InstanceRef, cmd Command) ConfChangeResult {
	defer n.refreshPendingConf()
	base, known := n.confHistory[ref.Conf]
	if !known {
		return ConfChangeResult{Outcome: ConfChangeRejectedInvalid}
	}
	rec := InstanceRecord{Ref: ref, Command: cmd}
	successor, valid := confChangeSuccessor(rec, base)
	if !valid {
		return ConfChangeResult{Outcome: ConfChangeRejectedInvalid}
	}
	if _, applied := n.appliedConfByBase[ref.Conf]; applied {
		return ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}
	}
	if _, exists := n.confHistory[successor.ID]; exists {
		return ConfChangeResult{Outcome: ConfChangeRejectedSuperseded}
	}
	q, err := newQuorum(successor.Voters)
	if err != nil {
		return ConfChangeResult{Outcome: ConfChangeRejectedInvalid}
	}
	q.conf.ID = successor.ID
	n.confHistory[successor.ID] = successor.Clone()
	n.appliedConfByBase[ref.Conf] = ref
	if ref.Conf == n.q.conf.ID {
		n.q = q
		n.activateTOQRuntime(successor)
		n.currentHardState.Conf = successor.Clone()
		n.localVoter.Conf = successor.Clone()
		if successor.Contains(n.id) {
			n.localVoter.Status = LocalVoterStatusEligible
		} else {
			n.localVoter.Status = LocalVoterStatusIneligible
		}
		if n.cluster != (ClusterID{}) {
			local := n.localVoter.Clone()
			target := n.readyTarget()
			target.LocalVoterState = &local
			target.ConfigHistory = append(target.ConfigHistory, ConfigHistoryEntry{Conf: successor.Clone(), AppliedRef: ref})
			target.MustSync = true
		}
	}
	return ConfChangeResult{Outcome: ConfChangeApplied, Conf: successor.Clone()}
}
