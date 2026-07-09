package epaxos

import (
	"bytes"
	"container/heap"
	"encoding/binary"
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

type attrVote struct {
	seq              uint64
	deps             []InstanceNum
	depsCommitted    uint64
	fastPathEligible bool
}

type tryCandidate struct {
	rec    InstanceRecord
	voters map[ReplicaID]struct{}
}

type tryEvidenceKey struct {
	target   InstanceRef
	conflict InstanceRef
}

type instance struct {
	rec          InstanceRecord
	phase        phase
	preOK        map[ReplicaID]attrVote
	accOK        map[ReplicaID]struct{}
	prepareOK    map[ReplicaID]InstanceRecord
	tryOK        map[ReplicaID]struct{}
	tryDeferred  map[InstanceRef]struct{}
	tryIgnored   map[InstanceRef]struct{}
	createdTick  uint64
	waitDeadline uint64
	processAt    uint64
	generation   uint64
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

type timerHeap []timer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline != h[j].deadline {
		return h[i].deadline < h[j].deadline
	}
	if h[i].ref.Replica != h[j].ref.Replica {
		return h[i].ref.Replica < h[j].ref.Replica
	}
	if h[i].ref.Instance != h[j].ref.Instance {
		return h[i].ref.Instance < h[j].ref.Instance
	}
	return h[i].kind < h[j].kind
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(timer)) }
func (h *timerHeap) Pop() any     { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// RawNode is a deterministic raft-like EPaxos state machine.
type RawNode struct {
	id          ReplicaID
	q           quorum
	confHistory map[ConfID]ConfState
	storage     Storage
	zeroCopy    bool

	retryTicks            uint64
	recoveryTicks         uint64
	timeOptimization      bool
	timeOptimizationTicks uint64
	toqEnabled            bool
	toqClock              func() uint64
	toqOneWayDelay        map[ReplicaID]uint64
	toqSyncGroup          []ReplicaID
	maxReadyMessages      int

	tick              uint64
	nextInstance      InstanceNum
	instances         map[InstanceRef]*instance
	conflicts         map[string]map[ReplicaID]InstanceRef
	executed          map[InstanceRef]struct{}
	pendingConf       bool
	timers            timerHeap
	pendingReady      Ready
	awaitAdvance      bool
	delayedPreAccepts []Message
	tryEvidenceChecks map[tryEvidenceKey]map[ReplicaID]InstanceRecord
}

func configureTOQ(cfg Config, q quorum) (func() uint64, map[ReplicaID]uint64, []ReplicaID, error) {
	if !cfg.TOQ {
		return nil, nil, nil, nil
	}
	if cfg.TimeOptimization {
		return nil, nil, nil, fmt.Errorf("%w: TOQ and TimeOptimization are separate modes", ErrInvalidConfig)
	}
	if cfg.TOQClock == nil {
		return nil, nil, nil, fmt.Errorf("%w: TOQ requires a synchronized clock", ErrInvalidConfig)
	}
	group := append([]ReplicaID(nil), cfg.TOQSyncGroup...)
	if len(group) == 0 {
		group = append(group, q.conf.Voters...)
	}
	delays := make(map[ReplicaID]uint64, len(group))
	seen := make(map[ReplicaID]struct{}, len(group))
	for _, id := range group {
		if !q.contains(id) {
			return nil, nil, nil, fmt.Errorf("%w: TOQ sync group contains non-voter", ErrInvalidConfig)
		}
		if _, ok := seen[id]; ok {
			return nil, nil, nil, fmt.Errorf("%w: TOQ sync group duplicate voter", ErrInvalidConfig)
		}
		seen[id] = struct{}{}
		delay, ok := cfg.TOQOneWayDelay[id]
		if !ok && id != cfg.ID {
			return nil, nil, nil, fmt.Errorf("%w: TOQ missing one-way delay", ErrInvalidConfig)
		}
		delays[id] = delay
	}
	return cfg.TOQClock, delays, group, nil
}
func (n *RawNode) preAcceptOrderingEnabled() bool { return n.timeOptimization || n.toqEnabled }

func (n *RawNode) preAcceptNow() uint64 {
	if n.toqEnabled {
		return n.toqClock()
	}
	return n.tick
}

func (n *RawNode) nextTOQProcessAt() uint64 {
	now := n.toqClock()
	processAt := now
	for _, id := range n.toqSyncGroup {
		candidate := now + n.toqOneWayDelay[id]
		if candidate > processAt {
			processAt = candidate
		}
	}
	return processAt
}
func (n *RawNode) localTOQPreAcceptMessage(inst *instance) Message {
	return Message{Type: MsgPreAccept, From: n.id, To: n.id, Ref: inst.rec.Ref, ProcessAt: inst.rec.ProcessAt, TOQ: true, Ballot: inst.rec.Ballot, Command: inst.rec.Command.Borrow(), RecordStatus: inst.rec.Status}
}

func recordHasValue(status Status) bool {
	return status >= StatusPreAccepted
}

func recordTupleBallot(rec InstanceRecord) Ballot {
	return rec.RecordBallot
}

// NewRawNode constructs a deterministic EPaxos node from config and storage.
func NewRawNode(cfg Config) (*RawNode, error) {
	q, err := newQuorum(cfg.Voters)
	if err != nil {
		return nil, err
	}
	if !q.contains(cfg.ID) {
		return nil, fmt.Errorf("%w: local id is not a voter", ErrInvalidConfig)
	}
	if cfg.RetryTicks == 0 {
		cfg.RetryTicks = 3
	}
	if cfg.RecoveryTicks == 0 {
		cfg.RecoveryTicks = cfg.RetryTicks * 3
	}
	if cfg.TimeOptimizationTicks == 0 {
		cfg.TimeOptimizationTicks = 1
	}
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
	}
	n := &RawNode{
		id:                    cfg.ID,
		q:                     q,
		storage:               cfg.Storage,
		zeroCopy:              cfg.ZeroCopyProposals,
		retryTicks:            cfg.RetryTicks,
		recoveryTicks:         cfg.RecoveryTicks,
		timeOptimization:      cfg.TimeOptimization,
		timeOptimizationTicks: cfg.TimeOptimizationTicks,
		maxReadyMessages:      cfg.MaxReadyMessages,
		nextInstance:          1,
		instances:             make(map[InstanceRef]*instance),
		conflicts:             make(map[string]map[ReplicaID]InstanceRef),
		executed:              make(map[InstanceRef]struct{}),
		confHistory:           make(map[ConfID]ConfState),
	}
	hs, configs, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, err
	}
	if hs.Conf.ID != 0 && len(hs.Conf.Voters) > 0 {
		loaded, err := newQuorum(hs.Conf.Voters)
		if err != nil {
			return nil, err
		}
		loaded.conf.ID = hs.Conf.ID
		n.q = loaded
		n.confHistory[loaded.conf.ID] = loaded.conf.Clone()
	}
	if len(configs) > 0 {
		last := configs[len(configs)-1]
		loaded, err := newQuorum(last.Voters)
		if err != nil {
			return nil, err
		}
		loaded.conf.ID = last.ID
		n.confHistory[loaded.conf.ID] = loaded.conf.Clone()
		n.q = loaded
	}
	if n.q.conf.ID == 0 {
		n.q.conf.ID = 1
	}
	n.confHistory[n.q.conf.ID] = n.q.conf.Clone()
	toqClock, toqOneWayDelay, toqSyncGroup, err := configureTOQ(cfg, n.q)
	if err != nil {
		return nil, err
	}
	n.toqEnabled = cfg.TOQ
	n.toqClock = toqClock
	n.toqOneWayDelay = toqOneWayDelay
	n.toqSyncGroup = toqSyncGroup
	n.tick = hs.Tick
	if err := cfg.Storage.LoadInstances(func(rec InstanceRecord) error {
		if !VerifyRecordChecksum(rec) {
			return ErrChecksumMismatch
		}
		phase := phaseFromStatus(rec.Status)
		if rec.TOQPending {
			if !n.toqEnabled {
				return fmt.Errorf("%w: TOQ-pending record requires TOQ", ErrInvalidConfig)
			}
			phase = phaseIdle
			if rec.Ref.Replica == n.id {
				phase = phasePreAccept
			}
		} else if rec.Status < StatusCommitted && rec.Ballot.Number > 0 && rec.Ballot.Replica == n.id {
			phase = phasePrepare
		}
		n.instances[rec.Ref] = &instance{rec: rec.Clone(), phase: phase, processAt: rec.ProcessAt}
		if rec.Ref.Replica == n.id && rec.Ref.Instance >= n.nextInstance {
			n.nextInstance = rec.Ref.Instance + 1
		}
		if rec.Status >= StatusPreAccepted && !rec.TOQPending {
			n.indexConflicts(rec)
		}
		if rec.Status == StatusExecuted {
			n.executed[rec.Ref] = struct{}{}
		}
		n.markPendingConf(rec)
		return nil
	}); err != nil {
		return nil, err
	}
	heap.Init(&n.timers)
	n.replayExecutedConfig()
	for _, inst := range n.instances {
		if inst.rec.Status >= StatusCommitted {
			continue
		}
		if inst.rec.TOQPending {
			if inst.rec.Ref.Replica == n.id {
				n.delayedPreAccepts = append(n.delayedPreAccepts, n.localTOQPreAcceptMessage(inst))
				n.schedule(inst, timerPreAccept, n.retryTicks)
			}
			continue
		}
		switch inst.phase {
		case phasePreAccept:
			if inst.rec.Ref.Replica == n.id && inst.rec.Ballot.Number == 0 && inst.rec.Ballot.Replica == n.id {
				n.schedule(inst, timerPreAccept, n.retryTicks)
			} else if n.shouldCoordinateRecovery(inst.rec.Ref) {
				n.schedule(inst, timerPrepare, n.recoveryTicks)
			}
		case phaseAccept:
			if inst.rec.Ref.Replica == n.id && inst.rec.Ballot.Replica == n.id {
				n.schedule(inst, timerAccept, n.retryTicks)
			} else if n.shouldCoordinateRecovery(inst.rec.Ref) {
				n.schedule(inst, timerPrepare, n.recoveryTicks)
			}
		case phasePrepare:
			n.schedule(inst, timerPrepare, n.recoveryTicks)
		}
	}
	n.processDuePreAccepts()
	n.tryExecute()
	return n, nil
}

func phaseFromStatus(s Status) phase {
	switch s {
	case StatusPreAccepted:
		return phasePreAccept
	case StatusAccepted:
		return phaseAccept
	case StatusCommitted, StatusExecuted:
		return phaseCommitted
	default:
		return phaseIdle
	}
}

func (n *RawNode) confFor(id ConfID) ConfState {
	if conf, ok := n.confHistory[id]; ok {
		return conf
	}
	return n.q.conf
}

func (n *RawNode) depsForConf(id ConfID) []InstanceNum {
	return make([]InstanceNum, len(n.confFor(id).Voters))
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

func (n *RawNode) replayExecutedConfig() {
	refs := make([]InstanceRef, 0, len(n.instances))
	for ref, inst := range n.instances {
		if inst.rec.Status == StatusExecuted && inst.rec.Command.Kind == CommandConfChange {
			refs = append(refs, ref)
		}
	}
	sortRefs(refs)
	for _, ref := range refs {
		n.applyConfChange(ref, n.instances[ref].rec.Command)
	}
}

func (n *RawNode) markPendingConf(rec InstanceRecord) {
	if rec.Command.Kind == CommandConfChange && rec.Status < StatusExecuted && (rec.Status >= StatusPreAccepted || rec.TOQPending) {
		n.pendingConf = true
	}
}

func (n *RawNode) refreshPendingConf() {
	n.pendingConf = false
	for ref, inst := range n.instances {
		if _, executed := n.executed[ref]; executed {
			continue
		}
		n.markPendingConf(inst.rec)
		if n.pendingConf {
			return
		}
	}
}

// Tick advances deterministic logical time by one tick and processes due timers.
func (n *RawNode) Tick() {
	n.tick++
	if n.recoveryTicks > 0 && n.tick%n.recoveryTicks == 0 {
		n.rebroadcastCommits()
	}
	n.processDuePreAccepts()
	for n.timers.Len() > 0 {
		t := n.timers[0]
		if t.deadline > n.tick {
			return
		}
		heap.Pop(&n.timers)
		inst := n.instances[t.ref]
		if inst == nil || inst.generation != t.gen || inst.phase == phaseCommitted {
			continue
		}
		n.onTimer(inst, t.kind)
	}
}

func (n *RawNode) deferPreAccept(m Message) bool {
	if !n.preAcceptOrderingEnabled() || m.ProcessAt == 0 || m.ProcessAt <= n.preAcceptNow() {
		return false
	}
	if m.Ballot.Number != 0 || m.Ballot.Replica != m.Ref.Replica {
		return false
	}
	n.delayedPreAccepts = append(n.delayedPreAccepts, m.Clone())
	return true
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

func (n *RawNode) processDuePreAccepts() {
	if len(n.delayedPreAccepts) == 0 {
		return
	}
	sort.Slice(n.delayedPreAccepts, func(i, j int) bool {
		return preAcceptMessageLess(n.delayedPreAccepts[i], n.delayedPreAccepts[j])
	})
	now := n.preAcceptNow()
	due := 0
	for due < len(n.delayedPreAccepts) && n.delayedPreAccepts[due].ProcessAt <= now {
		due++
	}
	for i := 0; i < due; i++ {
		m := n.delayedPreAccepts[i]
		if m.TOQ && m.From == n.id && m.To == n.id && m.Ref.Replica == n.id {
			n.handleLocalTOQPreAccept(m)
			continue
		}
		n.handlePreAccept(m)
	}
	copy(n.delayedPreAccepts, n.delayedPreAccepts[due:])
	tail := n.delayedPreAccepts[len(n.delayedPreAccepts)-due:]
	for i := range tail {
		tail[i].Reset()
	}
	n.delayedPreAccepts = n.delayedPreAccepts[:len(n.delayedPreAccepts)-due]
}

func (n *RawNode) handleLocalTOQPreAccept(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePreAccept || !inst.rec.TOQPending {
		return
	}
	attrs := n.computeAttrsAt(inst.rec.Command, inst.rec.Ref, m.ProcessAt, true)
	inst.rec.Status = StatusPreAccepted
	inst.rec.Seq = attrs.Seq
	inst.rec.Deps = append([]InstanceNum(nil), attrs.Deps...)
	inst.rec.FastPathEligible = true
	inst.rec.ProcessAt = m.ProcessAt
	inst.rec.TOQPending = false
	inst.processAt = m.ProcessAt
	inst.rec.RecordBallot = inst.rec.Ballot
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	n.indexConflicts(inst.rec)
	if inst.preOK == nil {
		inst.preOK = make(map[ReplicaID]attrVote, n.q.fastQuorum())
	}
	inst.preOK[n.id] = attrVote{seq: inst.rec.Seq, deps: append([]InstanceNum(nil), inst.rec.Deps...), depsCommitted: n.committedDepsMask(inst.rec.Attributes(), inst.rec.Ref.Conf), fastPathEligible: inst.rec.FastPathEligible}
	n.enqueueRecord(inst.rec)
	if n.q.slowQuorum() == 1 {
		n.commit(inst, inst.rec.Attributes())
		return
	}
	n.maybeFinalizePreAccept(inst)
}

// Propose creates a new local EPaxos instance for an application command.
func (n *RawNode) Propose(cmd Command) (InstanceRef, error) {
	if cmd.Kind == CommandConfChange {
		return InstanceRef{}, fmt.Errorf("%w: use ProposeConfChange", ErrInvalidConfig)
	}
	if n.pendingConf && cmd.Kind != CommandNoop {
		return InstanceRef{}, fmt.Errorf("%w: configuration change pending", ErrMessageRejected)
	}
	if !n.zeroCopy {
		cmd = cmd.Clone()
	}
	if cmd.Kind != CommandNoop {
		cmd.Kind = CommandUser
	}
	return n.propose(cmd), nil
}

// ProposeConfChange proposes a validated membership change encoded as a consensus command.
func (n *RawNode) ProposeConfChange(cc ConfChange) (InstanceRef, error) {
	if n.pendingConf {
		return InstanceRef{}, fmt.Errorf("%w: configuration change already pending", ErrMessageRejected)
	}
	if _, err := n.confChangeQuorum(cc); err != nil {
		return InstanceRef{}, err
	}
	var payload [9]byte
	payload[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(payload[1:], uint64(cc.Replica))
	cmd := Command{Kind: CommandConfChange, Payload: append([]byte(nil), payload[:]...), ConflictKeys: [][]byte{[]byte("\xffconf")}}
	n.pendingConf = true
	ref := n.propose(cmd)
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

func (n *RawNode) propose(cmd Command) InstanceRef {
	ref := InstanceRef{Replica: n.id, Instance: n.nextInstance, Conf: n.q.conf.ID}
	n.nextInstance++
	processAt := uint64(0)
	if n.toqEnabled {
		processAt = n.nextTOQProcessAt()
		rec := InstanceRecord{Ref: ref, Ballot: Ballot{Replica: n.id}, Status: StatusNone, Seq: 0, Deps: n.depsForConf(ref.Conf), Command: cmd, ProcessAt: processAt, TOQPending: true}
		rec.Checksum = ChecksumRecord(rec)
		inst := &instance{rec: rec, phase: phasePreAccept, preOK: make(map[ReplicaID]attrVote, n.q.fastQuorum()), createdTick: n.tick, processAt: processAt}
		n.instances[ref] = inst
		n.enqueueRecord(rec)
		n.delayedPreAccepts = append(n.delayedPreAccepts, n.localTOQPreAcceptMessage(inst))
		n.broadcastPreAccept(inst)
		n.schedule(inst, timerPreAccept, n.retryTicks)
		n.processDuePreAccepts()
		return ref
	}
	if n.timeOptimization {
		processAt = n.tick + n.timeOptimizationTicks
	}
	attrs := n.computeAttrsAt(cmd, ref, processAt, n.timeOptimization && processAt > 0)
	rec := InstanceRecord{Ref: ref, Ballot: Ballot{Replica: n.id}, RecordBallot: Ballot{Replica: n.id}, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd, FastPathEligible: true, ProcessAt: processAt}
	rec.Checksum = ChecksumRecord(rec)
	inst := &instance{rec: rec, phase: phasePreAccept, preOK: map[ReplicaID]attrVote{n.id: {seq: rec.Seq, deps: append([]InstanceNum(nil), rec.Deps...), depsCommitted: n.committedDepsMask(rec.Attributes(), ref.Conf), fastPathEligible: rec.FastPathEligible}}, createdTick: n.tick, processAt: processAt}
	n.instances[ref] = inst
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	if n.q.slowQuorum() == 1 {
		n.commit(inst, rec.Attributes())
		return ref
	}
	n.broadcastPreAccept(inst)
	n.schedule(inst, timerPreAccept, n.retryTicks)
	return ref
}

// Step applies one validated transport message to the local node. It copies
// command bytes before storing them, so callers may reuse decode buffers after
// Step returns.
func (n *RawNode) Step(m Message) error {
	if m.To != n.id {
		return ErrMessageRejected
	}
	if err := m.Validate(n.confFor(m.Ref.Conf)); err != nil {
		return err
	}
	if m.Checksum != ([32]byte{}) && !VerifyMessageChecksum(m) {
		return ErrChecksumMismatch
	}
	if m.TOQ && !n.toqEnabled {
		return ErrMessageRejected
	}
	if n.toqEnabled && m.Type == MsgPreAccept && m.ProcessAt > 0 && !m.TOQ {
		return ErrMessageRejected
	}
	n.processDuePreAccepts()
	switch m.Type {
	case MsgPreAccept:
		if n.deferPreAccept(m) {
			return nil
		}
		n.handlePreAccept(m)
	case MsgPreAcceptResp:
		n.handlePreAcceptResp(m)
	case MsgAccept:
		n.handleAccept(m)
	case MsgAcceptResp:
		n.handleAcceptResp(m)
	case MsgCommit:
		n.handleCommit(m)
	case MsgPrepare:
		n.handlePrepare(m)
	case MsgPrepareResp:
		n.handlePrepareResp(m)
	case MsgTryPreAccept:
		n.handleTryPreAccept(m)
	case MsgTryPreAcceptResp:
		n.handleTryPreAcceptResp(m)
	case MsgEvidence:
		n.handleEvidence(m)
	case MsgEvidenceResp:
		n.handleEvidenceResp(m)
	}
	return nil
}

func (n *RawNode) HasReady() bool { return !n.pendingReady.Empty() }

// Ready returns the next batch of persistent records, messages, and commands.
// Until Advance acknowledges a prefix, repeated Ready calls return the same
// outstanding batch so callers can retry failed durable or application work.
func (n *RawNode) Ready() Ready {
	if n.pendingReady.Empty() {
		return Ready{}
	}
	rd := Ready{MustSync: n.pendingReady.MustSync}
	rd.Records = make([]InstanceRecord, len(n.pendingReady.Records))
	for i := range n.pendingReady.Records {
		rd.Records[i] = n.readyRecord(n.pendingReady.Records[i])
	}
	messageCount := len(n.pendingReady.Messages)
	if n.maxReadyMessages > 0 && messageCount > n.maxReadyMessages {
		messageCount = n.maxReadyMessages
	}
	rd.Messages = make([]Message, messageCount)
	for i := range rd.Messages {
		rd.Messages[i] = n.pendingReady.Messages[i].Clone()
	}
	rd.Committed = make([]CommittedCommand, len(n.pendingReady.Committed))
	for i := range n.pendingReady.Committed {
		rd.Committed[i] = n.pendingReady.Committed[i].Clone()
	}
	n.awaitAdvance = true
	return rd
}

// Advance acknowledges a prefix of the outstanding Ready batch or returns ErrInvalidReady without mutating node state.
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
	if len(rd.Records) >= len(n.pendingReady.Records) {
		n.pendingReady.Records = nil
	} else {
		n.pendingReady.Records = n.pendingReady.Records[len(rd.Records):]
	}
	if len(rd.Messages) >= len(n.pendingReady.Messages) {
		n.pendingReady.Messages = nil
	} else {
		n.pendingReady.Messages = n.pendingReady.Messages[len(rd.Messages):]
	}
	if len(rd.Committed) >= len(n.pendingReady.Committed) {
		n.pendingReady.Committed = nil
	} else {
		n.pendingReady.Committed = n.pendingReady.Committed[len(rd.Committed):]
	}
	n.pendingReady.MustSync = len(n.pendingReady.Records) > 0
	n.enqueueExecutedRecords(rd.Committed[:ackedCommitted])
	n.awaitAdvance = false
	return nil
}

func (n *RawNode) validateReadyAck(rd Ready) error {
	if len(rd.Records) == 0 && len(rd.Messages) == 0 && len(rd.Committed) == 0 {
		return ErrInvalidReady
	}
	if rd.MustSync != n.pendingReady.MustSync {
		return ErrInvalidReady
	}
	if len(rd.Records) > len(n.pendingReady.Records) || len(rd.Committed) > len(n.pendingReady.Committed) {
		return ErrInvalidReady
	}
	visibleMessages := len(n.pendingReady.Messages)
	if n.maxReadyMessages > 0 && visibleMessages > n.maxReadyMessages {
		visibleMessages = n.maxReadyMessages
	}
	if len(rd.Messages) > visibleMessages {
		return ErrInvalidReady
	}
	if (len(rd.Messages) > 0 || len(rd.Committed) > 0) && len(rd.Records) != len(n.pendingReady.Records) {
		return ErrInvalidReady
	}
	for i := range rd.Records {
		if !instanceRecordEqual(rd.Records[i], n.readyRecord(n.pendingReady.Records[i])) {
			return ErrInvalidReady
		}
	}
	for i := range rd.Messages {
		if !messageEqual(rd.Messages[i], n.pendingReady.Messages[i].Clone()) {
			return ErrInvalidReady
		}
	}
	for i := range rd.Committed {
		if !committedCommandEqual(rd.Committed[i], n.pendingReady.Committed[i].Clone()) {
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
		a.TOQPending == b.TOQPending &&
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
	refs := make([]InstanceRef, 0, len(n.instances))
	for ref := range n.instances {
		refs = append(refs, ref)
	}
	sortRefs(refs)
	for _, ref := range refs {
		s.Instances = append(s.Instances, n.instances[ref].rec.Clone())
		if _, ok := n.executed[ref]; ok {
			s.Executed = append(s.Executed, ref)
		}
	}
	return s
}

func (n *RawNode) handlePreAccept(m Message) {
	attrs := n.computeAttrsAt(m.Command, m.Ref, m.ProcessAt, m.TOQ || (n.timeOptimization && m.ProcessAt > 0))
	if !m.TOQ {
		attrs = mergeAttrs(attrs, m.Attributes())
	}
	defaultBallot := m.Ballot.Number == 0 && m.Ballot.Replica == m.Ref.Replica
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.Ballot, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: inboundCommand(m.Command), FastPathEligible: defaultBallot && (m.TOQ || attrs.Equal(m.Attributes())), ProcessAt: m.ProcessAt}
	old := n.instances[m.Ref]
	if old != nil {
		if old.rec.Status >= StatusCommitted {
			n.sendCommitTo(m.From, old.rec)
			return
		}
		if m.Ballot.Less(old.rec.Ballot) {
			n.sendReject(MsgPreAcceptResp, m.From, m.Ref, old.rec.Ballot)
			return
		}
	}
	rec.Checksum = ChecksumRecord(rec)
	if old != nil && old.rec.Status == StatusPreAccepted && instanceRecordEqual(old.rec, rec) {
		resp := Message{Type: MsgPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, RecordStatus: rec.Status, FastPathEligible: old.rec.FastPathEligible, DepsCommitted: n.committedDepsMask(rec.Attributes(), m.Ref.Conf)}
		n.enqueueMessage(resp)
		return
	}
	inst := &instance{rec: rec, phase: phaseIdle, processAt: m.ProcessAt}
	n.instances[m.Ref] = inst
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	n.markPendingConf(rec)
	if n.shouldCoordinateRecovery(rec.Ref) {
		n.schedule(inst, timerPrepare, n.recoveryTicks)
	}
	resp := Message{Type: MsgPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, RecordStatus: rec.Status, FastPathEligible: rec.FastPathEligible, DepsCommitted: n.committedDepsMask(rec.Attributes(), m.Ref.Conf)}
	n.enqueueMessage(resp)
}

func (n *RawNode) handlePreAcceptResp(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePreAccept || m.Ref.Replica != n.id {
		return
	}
	if m.Reject {
		if !inst.rec.Ballot.Less(m.RejectHint) {
			return
		}
		inst.rec.Ballot = m.RejectHint.Next(n.id)
		n.startPrepare(inst)
		return
	}
	if inst.preOK == nil {
		inst.preOK = make(map[ReplicaID]attrVote, n.q.fastQuorum())
	}
	if _, duplicate := inst.preOK[m.From]; duplicate {
		return
	}
	inst.preOK[m.From] = attrVote{seq: m.Seq, deps: append([]InstanceNum(nil), m.Deps...), depsCommitted: m.DepsCommitted, fastPathEligible: m.FastPathEligible}
	n.maybeFinalizePreAccept(inst)
}

func (n *RawNode) maybeFinalizePreAccept(inst *instance) {
	if inst == nil || inst.rec.TOQPending {
		return
	}
	attrs := attrsFromVotes(inst.preOK)
	if fastAttrs, ok := n.fastCommitAttrsPreAccept(inst); ok {
		n.commit(inst, fastAttrs)
		return
	}
	if len(inst.preOK) >= n.q.slowQuorum() {
		deadline := inst.createdTick + n.timeOptimizationTicks
		if n.timeOptimization && n.tick < deadline && len(inst.preOK) < len(n.confFor(inst.rec.Ref.Conf).Voters) {
			inst.waitDeadline = deadline
			n.schedule(inst, timerFastWait, deadline-n.tick)
			return
		}
		n.startAccept(inst, attrs)
	}
}

func (n *RawNode) handleAccept(m Message) {
	old := n.instances[m.Ref]
	if old != nil {
		if old.rec.Status >= StatusCommitted {
			n.sendCommitTo(m.From, old.rec)
			return
		}
		if m.Ballot.Less(old.rec.Ballot) {
			n.sendReject(MsgAcceptResp, m.From, m.Ref, old.rec.Ballot)
			return
		}
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.Ballot, Status: StatusAccepted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: inboundCommand(m.Command)}
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
	if old != nil && old.rec.Status == StatusAccepted && instanceRecordEqual(old.rec, rec) {
		resp := Message{Type: MsgAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, RecordBallot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps, AcceptSeq: rec.AcceptSeq, AcceptDeps: rec.AcceptDeps, AcceptEvidence: rec.AcceptEvidence, RecordStatus: rec.Status}
		n.enqueueMessage(resp)
		return
	}
	inst := &instance{rec: rec, phase: phaseIdle}
	n.instances[m.Ref] = inst
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	n.markPendingConf(rec)
	if n.shouldCoordinateRecovery(rec.Ref) {
		n.schedule(inst, timerPrepare, n.recoveryTicks)
	}
	resp := Message{Type: MsgAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, RecordBallot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps, AcceptSeq: rec.AcceptSeq, AcceptDeps: rec.AcceptDeps, AcceptEvidence: rec.AcceptEvidence, RecordStatus: rec.Status}
	n.enqueueMessage(resp)
}

func (n *RawNode) handleAcceptResp(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phaseAccept || !n.coordinatesInstance(inst) {
		return
	}
	if m.Reject {
		if !inst.rec.Ballot.Less(m.RejectHint) {
			return
		}
		inst.rec.Ballot = m.RejectHint.Next(n.id)
		n.startPrepare(inst)
		return
	}
	if m.Ballot != inst.rec.Ballot {
		return
	}
	if inst.accOK == nil {
		inst.accOK = make(map[ReplicaID]struct{}, n.q.slowQuorum())
	}
	if _, duplicate := inst.accOK[m.From]; duplicate {
		return
	}
	inst.accOK[m.From] = struct{}{}
	changed := mergeAcceptEvidence(&inst.rec, acceptEvidenceFromMessage(m))
	if mergeSenderAcceptEvidence(&inst.rec, m.AcceptEvidence) {
		changed = true
	}
	if changed {
		inst.rec.Checksum = ChecksumRecord(inst.rec)
		n.enqueueRecord(inst.rec)
	}
	if len(inst.accOK) >= n.q.slowQuorum() {
		n.commit(inst, inst.rec.Attributes())
	}
}

func (n *RawNode) handleCommit(m Message) {
	old := n.instances[m.Ref]
	if old != nil && old.rec.Status >= StatusCommitted {
		return
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.RecordBallot, Status: StatusCommitted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: inboundCommand(m.Command)}
	if rec.RecordBallot == (Ballot{}) {
		return
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
	n.instances[m.Ref] = &instance{rec: rec, phase: phaseCommitted}
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	n.markPendingConf(rec)
	n.tryExecute()
}

func (n *RawNode) handlePrepare(m Message) {
	resp := Message{Type: MsgPrepareResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: m.Ballot}
	inst := n.instances[m.Ref]
	if inst != nil && inst.rec.TOQPending {
		n.processDuePreAccepts()
		inst = n.instances[m.Ref]
		if inst != nil && inst.rec.TOQPending {
			return
		}
	}
	if inst != nil && m.Ballot.Less(inst.rec.Ballot) {
		n.sendReject(MsgPrepareResp, m.From, m.Ref, inst.rec.Ballot)
		return
	}
	if inst == nil {
		rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, Status: StatusNone, Deps: n.depsForConf(m.Ref.Conf)}
		rec.Checksum = ChecksumRecord(rec)
		inst = &instance{rec: rec, phase: phaseIdle}
		n.instances[m.Ref] = inst
		if m.Ballot.Number > 0 && m.Ballot.Replica != n.id {
			inst.waitDeadline = n.tick + n.recoveryTicks
		}
		n.enqueueRecord(rec)
	} else {
		changed := false
		if inst.rec.Ballot.Less(m.Ballot) {
			inst.rec.Ballot = m.Ballot
			changed = true
			if m.Ballot.Replica != n.id {
				inst.phase = phaseIdle
				inst.preOK = nil
				inst.accOK = nil
				inst.prepareOK = nil
				inst.generation++
				inst.waitDeadline = n.tick + n.recoveryTicks
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
	n.enqueueMessage(resp)
}

func (n *RawNode) handleEvidence(m Message) {
	resp := Message{Type: MsgEvidenceResp, From: n.id, To: m.From, Ref: m.Ref, ConflictRef: m.ConflictRef, Ballot: m.Ballot, Deps: n.depsForConf(m.Ref.Conf), RecordStatus: StatusNone}
	inst := n.instances[m.Ref]
	if inst != nil && inst.rec.TOQPending {
		n.processDuePreAccepts()
		inst = n.instances[m.Ref]
	}
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
	}
	n.enqueueMessage(resp)
}

func (n *RawNode) handleEvidenceResp(m Message) {
	key := tryEvidenceKey{target: m.ConflictRef, conflict: m.Ref}
	records, ok := n.tryEvidenceChecks[key]
	if !ok {
		return
	}
	if _, duplicate := records[m.From]; duplicate {
		return
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.RecordBallot, Status: m.RecordStatus, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptSeq: m.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), m.AcceptDeps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: m.Command.Clone(), FastPathEligible: m.FastPathEligible}
	records[m.From] = rec
	n.resolveTryEvidenceCheck(key)
}

func (n *RawNode) handlePrepareResp(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePrepare || inst.rec.Ballot.Number == 0 || inst.rec.Ballot.Replica != n.id {
		return
	}
	if m.Reject {
		if !inst.rec.Ballot.Less(m.RejectHint) {
			return
		}
		inst.rec.Ballot = m.RejectHint
		n.startPrepare(inst)
		return
	}
	if m.Ballot.Less(inst.rec.Ballot) {
		return
	}
	if inst.rec.Ballot.Less(m.Ballot) {
		inst.rec.Ballot = m.Ballot
		n.startPrepare(inst)
		return
	}
	if inst.prepareOK == nil {
		inst.prepareOK = make(map[ReplicaID]InstanceRecord, n.q.slowQuorum())
	}
	if _, duplicate := inst.prepareOK[m.From]; duplicate {
		return
	}
	recordBallot := m.RecordBallot
	if recordHasValue(m.RecordStatus) && recordBallot == (Ballot{}) {
		return
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: recordBallot, Status: m.RecordStatus, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), AcceptSeq: m.AcceptSeq, AcceptDeps: append([]InstanceNum(nil), m.AcceptDeps...), AcceptEvidence: cloneAcceptEvidence(m.AcceptEvidence), Command: m.Command.Clone(), FastPathEligible: m.FastPathEligible}
	n.ensureRecordDeps(&rec)
	rec.Checksum = ChecksumRecord(rec)
	inst.prepareOK[m.From] = rec
	if len(inst.prepareOK) < n.q.slowQuorum() {
		return
	}

	responders := make([]ReplicaID, 0, len(inst.prepareOK))
	for id := range inst.prepareOK {
		responders = append(responders, id)
	}
	sort.Slice(responders, func(i, j int) bool { return responders[i] < responders[j] })

	for _, id := range responders {
		rec := inst.prepareOK[id]
		if rec.Status == StatusCommitted {
			inst.rec = rec.Clone()
			inst.rec.Status = StatusCommitted
			inst.rec.Checksum = ChecksumRecord(inst.rec)
			n.commit(inst, inst.rec.Attributes())
			return
		}
	}
	var chosen InstanceRecord
	attrs := Attributes{}
	foundAccepted := false
	var highestAccepted Ballot
	for _, id := range responders {
		rec := inst.prepareOK[id]
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
		inst.rec.Command = chosen.Command.Clone()
		n.startAccept(inst, attrs)
		return
	}

	attrs = Attributes{}
	foundPreAccepted := false
	var highestPreAccepted Ballot
	for _, id := range responders {
		rec := inst.prepareOK[id]
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
		rec := inst.prepareOK[id]
		if rec.Status != StatusPreAccepted || recordTupleBallot(rec) != highestPreAccepted {
			continue
		}
		attrs = mergeAttrs(attrs, rec.Attributes())
		if !foundPreAccepted {
			chosen = rec
			foundPreAccepted = true
		}
		if rec.FastPathEligible {
			candidates = addTryCandidate(candidates, rec, id)
		}
	}
	for _, candidate := range candidates {
		if len(candidate.voters) >= n.q.tryWitnessQuorum() {
			inst.rec.Command = candidate.rec.Command.Clone()
			n.startTryPreAccept(inst, candidate.rec.Attributes(), candidate.voters)
			return
		}
	}
	if foundPreAccepted {
		inst.rec.Command = chosen.Command.Clone()
		n.startAccept(inst, attrs)
		return
	}

	inst.rec.Command = Command{Kind: CommandNoop}
	n.startAccept(inst, Attributes{Seq: 1, Deps: n.depsForConf(m.Ref.Conf)})
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

func (n *RawNode) startAccept(inst *instance, attrs Attributes) {
	if inst.phase == phaseCommitted {
		return
	}
	clearRecordAcceptEvidence(&inst.rec)
	inst.phase = phaseAccept
	inst.rec.Status = StatusAccepted
	inst.rec.Seq = attrs.Seq
	inst.rec.Deps = append([]InstanceNum(nil), attrs.Deps...)
	inst.rec.FastPathEligible = false
	inst.rec.TOQPending = false
	inst.rec.RecordBallot = inst.rec.Ballot
	n.ensureRecordDeps(&inst.rec)
	evidenceAttrs := mergeAttrs(inst.rec.Attributes(), n.computeAttrs(inst.rec.Command, inst.rec.Ref))
	mergeAcceptEvidence(&inst.rec, evidenceAttrs)
	mergeOwnAcceptEvidence(&inst.rec, n.id, evidenceAttrs)
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	inst.accOK = map[ReplicaID]struct{}{n.id: {}}
	inst.generation++
	n.enqueueRecord(inst.rec)
	if n.q.slowQuorum() == 1 {
		n.commit(inst, attrs)
		return
	}
	n.broadcast(MsgAccept, inst.rec)
	n.schedule(inst, timerAccept, n.retryTicks)
}

func (n *RawNode) startTryPreAccept(inst *instance, attrs Attributes, witnesses map[ReplicaID]struct{}) {
	if inst.phase == phaseCommitted {
		return
	}
	inst.phase = phaseTryPreAccept
	inst.rec.Status = StatusPreAccepted
	inst.rec.Seq = attrs.Seq
	inst.rec.Deps = append([]InstanceNum(nil), attrs.Deps...)
	inst.rec.FastPathEligible = false
	inst.rec.TOQPending = false
	inst.rec.RecordBallot = inst.rec.Ballot
	clearRecordAcceptEvidence(&inst.rec)
	inst.tryDeferred = nil
	inst.tryIgnored = nil
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	inst.tryOK = make(map[ReplicaID]struct{}, len(witnesses)+1)
	for id := range witnesses {
		inst.tryOK[id] = struct{}{}
	}
	if n.canCountInitialLeaderForTry(inst) {
		inst.tryOK[inst.rec.Ref.Replica] = struct{}{}
	}
	if rec, ok := inst.prepareOK[n.id]; ok && rec.FastPathEligible && commandEqual(rec.Command, inst.rec.Command) && rec.Attributes().Equal(inst.rec.Attributes()) {
		inst.tryOK[n.id] = struct{}{}
	}
	inst.generation++
	n.enqueueRecord(inst.rec)
	if len(inst.tryOK) >= n.q.slowQuorum() {
		n.startAccept(inst, attrs)
		return
	}
	n.broadcastTryPreAccept(inst)
	n.schedule(inst, timerTryPreAccept, n.retryTicks)
}

func (n *RawNode) handleTryPreAccept(m Message) {
	old := n.instances[m.Ref]
	if old != nil {
		if old.rec.Status >= StatusCommitted {
			n.sendCommitTo(m.From, old.rec)
			return
		}
		if m.Ballot.Less(old.rec.Ballot) {
			n.sendReject(MsgTryPreAcceptResp, m.From, m.Ref, old.rec.Ballot)
			return
		}
	}
	if conflictRef, conflictStatus, ok := n.tryPreAcceptConflict(m); ok {
		reason := RejectUncommittedConflict
		if conflictStatus >= StatusCommitted {
			reason = RejectCommittedConflict
		}
		resp := Message{Type: MsgTryPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: m.Ballot, Reject: true, RejectReason: reason, ConflictRef: conflictRef, ConflictStatus: conflictStatus, Deps: n.depsForConf(m.Ref.Conf)}
		n.enqueueMessage(resp)
		return
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, RecordBallot: m.Ballot, Status: StatusPreAccepted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: inboundCommand(m.Command)}
	rec.Checksum = ChecksumRecord(rec)
	if old != nil && old.rec.Status == StatusPreAccepted && instanceRecordEqual(old.rec, rec) {
		resp := Message{Type: MsgTryPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, RecordBallot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps, RecordStatus: rec.Status}
		n.enqueueMessage(resp)
		return
	}
	inst := &instance{rec: rec, phase: phaseIdle}
	n.instances[m.Ref] = inst
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	resp := Message{Type: MsgTryPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: rec.Deps, RecordStatus: rec.Status}
	n.enqueueMessage(resp)
}

func (n *RawNode) handleTryPreAcceptResp(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phaseTryPreAccept || !n.coordinatesInstance(inst) {
		return
	}
	if m.Reject {
		switch m.RejectReason {
		case RejectStaleBallot, RejectNone:
			if inst.rec.Ballot.Less(m.RejectHint) {
				inst.rec.Ballot = m.RejectHint
				n.startPrepare(inst)
			}
		case RejectCommittedConflict:
			if m.ConflictStatus >= StatusCommitted && n.maybeStartTryEvidenceCheck(inst, m.ConflictRef) {
				return
			}
			n.startAcceptAfterTryCommittedConflict(inst, m.ConflictRef)
		case RejectUncommittedConflict:
			if !m.ConflictRef.IsZero() {
				if n.tryConflictForcesSlowAccept(inst, m.ConflictRef) {
					attrs := n.computeAttrs(inst.rec.Command, inst.rec.Ref)
					n.startAccept(inst, mergeAttrs(attrs, inst.rec.Attributes()))
					return
				}
				if inst.tryDeferred == nil {
					inst.tryDeferred = make(map[InstanceRef]struct{}, 1)
				}
				inst.tryDeferred[m.ConflictRef] = struct{}{}
				n.ensureDependencyRecovery(m.ConflictRef)
			}
		}
		return
	}
	if m.Ballot.Less(inst.rec.Ballot) {
		return
	}
	if inst.tryOK == nil {
		inst.tryOK = make(map[ReplicaID]struct{}, n.q.slowQuorum())
	}
	if _, duplicate := inst.tryOK[m.From]; duplicate {
		return
	}
	inst.tryOK[m.From] = struct{}{}
	if len(inst.tryOK) >= n.q.slowQuorum() {
		n.startAccept(inst, inst.rec.Attributes())
	}
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
	if len(inst.tryIgnored) > 0 {
		return false
	}
	if n.faultTolerance(inst.rec.Ref.Conf) <= 0 {
		return false
	}
	key := tryEvidenceKey{target: inst.rec.Ref, conflict: conflictRef}
	if n.tryEvidenceChecks == nil {
		n.tryEvidenceChecks = make(map[tryEvidenceKey]map[ReplicaID]InstanceRecord, 1)
	}
	if _, ok := n.tryEvidenceChecks[key]; ok {
		return true
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

func (n *RawNode) resolveTryEvidenceCheck(key tryEvidenceKey) {
	records, ok := n.tryEvidenceChecks[key]
	if !ok {
		return
	}
	inst := n.instances[key.target]
	if inst == nil || inst.phase != phaseTryPreAccept || !n.coordinatesInstance(inst) {
		delete(n.tryEvidenceChecks, key)
		return
	}
	authorized, failClosed := n.tryEvidenceDecision(inst, key, records)
	if !authorized && !failClosed {
		return
	}
	delete(n.tryEvidenceChecks, key)
	if len(n.tryEvidenceChecks) == 0 {
		n.tryEvidenceChecks = nil
	}
	if failClosed {
		if tuple, ok := n.committedConflictTuple(key.conflict, records); ok {
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
	return a.Ref == b.Ref && a.Seq == b.Seq && instanceNumsEqual(a.Deps, b.Deps) && commandEqual(a.Command, b.Command)
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
		if key.target == inst.rec.Ref {
			keys = append(keys, key)
		}
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
	for key := range n.tryEvidenceChecks {
		if key.target == target {
			delete(n.tryEvidenceChecks, key)
		}
	}
	if len(n.tryEvidenceChecks) == 0 {
		n.tryEvidenceChecks = nil
	}
}
func (n *RawNode) tryPreAcceptConflict(m Message) (InstanceRef, Status, bool) {
	candidate := InstanceRecord{Ref: m.Ref, Seq: m.Seq, Deps: m.Deps, Command: m.Command}
	refs := make([]InstanceRef, 0, len(n.instances))
	for ref := range n.instances {
		refs = append(refs, ref)
	}
	sortRefs(refs)
	for _, ref := range refs {
		other := n.instances[ref]
		if ref == m.Ref || other.rec.Status == StatusNone || !candidate.Command.ConflictsWith(other.rec.Command) {
			continue
		}
		candidateDepends := n.attrsDependsOn(candidate.Attributes(), ref, m.Ref.Conf)
		otherAttrs := other.rec.Attributes()
		otherDepends := n.attrsDependsOn(otherAttrs, m.Ref, other.rec.Ref.Conf)
		if otherDepends {
			continue
		}
		if !candidateDepends {
			return ref, other.rec.Status, true
		}
		if other.rec.Status >= StatusCommitted && m.IgnoreDependency.Ref == ref {
			continue
		}
		if other.rec.Status >= StatusCommitted {
			if n.recordAcceptEvidenceDependsOn(other.rec, m.Ref) {
				continue
			}
		} else if acceptAttrs, ok := other.rec.AcceptAttributes(); ok && n.attrsDependsOn(acceptAttrs, m.Ref, other.rec.Ref.Conf) {
			continue
		}
		sameLeaderPreAccepted := ref.Replica == m.Ref.Replica && other.rec.Status == StatusPreAccepted
		if otherAttrs.Seq >= candidate.Seq && !sameLeaderPreAccepted {
			return ref, other.rec.Status, true
		}
	}
	return InstanceRef{}, StatusNone, false
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

func (n *RawNode) canCountInitialLeaderForTry(inst *instance) bool {
	if inst == nil || inst.rec.Ref.Replica == 0 {
		return false
	}
	rec, ok := inst.prepareOK[inst.rec.Ref.Replica]
	if !ok {
		return n.confFor(inst.rec.Ref.Conf).Contains(inst.rec.Ref.Replica)
	}
	return rec.Status == StatusPreAccepted &&
		rec.FastPathEligible &&
		commandEqual(rec.Command, inst.rec.Command) &&
		rec.Attributes().Equal(inst.rec.Attributes())
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
	rec, ok := inst.prepareOK[id]
	if !ok {
		return true
	}
	return rec.Status == StatusPreAccepted &&
		rec.FastPathEligible &&
		commandEqual(rec.Command, inst.rec.Command) &&
		rec.Attributes().Equal(inst.rec.Attributes())
}

func (n *RawNode) startPrepare(inst *instance) {
	inst.phase = phasePrepare
	n.ensureRecordDeps(&inst.rec)
	previous := inst.rec.Clone()
	inst.tryDeferred = nil
	inst.rec.Ballot = inst.rec.Ballot.Next(n.id)
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	inst.prepareOK = map[ReplicaID]InstanceRecord{n.id: previous}
	inst.generation++
	n.enqueueRecord(inst.rec)
	n.broadcast(MsgPrepare, inst.rec)
	n.schedule(inst, timerPrepare, n.recoveryTicks)
}

func (n *RawNode) coordinatesInstance(inst *instance) bool {
	return inst != nil && (inst.rec.Ref.Replica == n.id || (inst.rec.Ballot.Number > 0 && inst.rec.Ballot.Replica == n.id))
}

func (n *RawNode) recoveryCoordinator(ref InstanceRef) ReplicaID {
	conf := n.confFor(ref.Conf)
	for _, id := range conf.Voters {
		if id != ref.Replica {
			return id
		}
	}
	if len(conf.Voters) == 1 {
		return conf.Voters[0]
	}
	return 0
}

func (n *RawNode) shouldCoordinateRecovery(ref InstanceRef) bool {
	return n.recoveryCoordinator(ref) == n.id
}

func (n *RawNode) promisedToOtherCoordinator(inst *instance) bool {
	return inst != nil && inst.rec.Ballot.Number > 0 && inst.rec.Ballot.Replica != n.id && n.tick < inst.waitDeadline
}

func (n *RawNode) commit(inst *instance, attrs Attributes) {
	if inst.phase == phaseCommitted && inst.rec.Status >= StatusCommitted {
		return
	}
	if inst.rec.Seq != attrs.Seq || !instanceNumsEqual(inst.rec.Deps, attrs.Deps) {
		clearRecordAcceptEvidence(&inst.rec)
	}
	inst.phase = phaseCommitted
	inst.rec.Status = StatusCommitted
	inst.rec.Seq = attrs.Seq
	inst.rec.Deps = append([]InstanceNum(nil), attrs.Deps...)
	inst.rec.FastPathEligible = false
	inst.rec.TOQPending = false
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	inst.generation++
	n.indexConflicts(inst.rec)
	n.enqueueRecord(inst.rec)
	n.broadcast(MsgCommit, inst.rec)
	n.tryExecute()
}

func (n *RawNode) onTimer(inst *instance, kind timerKind) {
	switch kind {
	case timerFastWait:
		if inst.phase == phasePreAccept && len(inst.preOK) >= n.q.slowQuorum() {
			n.startAccept(inst, attrsFromVotes(inst.preOK))
		}
	case timerPreAccept:
		if inst.phase == phasePreAccept {
			n.broadcastPreAccept(inst)
			n.schedule(inst, timerPreAccept, n.retryTicks)
		}
	case timerAccept:
		if inst.phase == phaseAccept {
			n.broadcast(MsgAccept, inst.rec)
			n.schedule(inst, timerAccept, n.retryTicks)
		}
	case timerTryPreAccept:
		if inst.phase == phaseTryPreAccept {
			if n.failPendingTryEvidenceCheck(inst) {
				return
			}
			n.broadcastTryPreAccept(inst)
			n.schedule(inst, timerTryPreAccept, n.retryTicks)
		}
	case timerPrepare:
		if inst.phase == phasePrepare {
			n.broadcast(MsgPrepare, inst.rec)
			n.schedule(inst, timerPrepare, n.recoveryTicks)
			return
		}
		if inst.rec.Status < StatusCommitted && (inst.rec.Status == StatusNone || inst.rec.Status == StatusPreAccepted || inst.rec.Status == StatusAccepted) && n.shouldCoordinateRecovery(inst.rec.Ref) && !n.promisedToOtherCoordinator(inst) {
			n.startPrepare(inst)
		}
	}
}

func (n *RawNode) schedule(inst *instance, kind timerKind, after uint64) {
	if after == 0 {
		after = 1
	}
	heap.Push(&n.timers, timer{deadline: n.tick + after, ref: inst.rec.Ref, kind: kind, gen: inst.generation})
}

func (n *RawNode) broadcastPreAccept(inst *instance) {
	processAt := inst.processAt
	toq := n.toqEnabled && inst.rec.Ref.Replica == n.id && inst.rec.Ballot.Number == 0 && inst.rec.Ballot.Replica == inst.rec.Ref.Replica
	for _, to := range n.q.conf.Voters {
		if to == n.id {
			continue
		}
		m := Message{Type: MsgPreAccept, From: n.id, To: to, Ref: inst.rec.Ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: inst.rec.Deps, Command: inst.rec.Command.Borrow(), RecordStatus: inst.rec.Status, ProcessAt: processAt}
		if toq {
			m.TOQ = true
			m.Seq = 0
			m.Deps = nil
		}
		n.enqueueMessage(m)
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
	for _, to := range n.q.conf.Voters {
		if to == n.id {
			continue
		}
		m := Message{Type: MsgTryPreAccept, From: n.id, To: to, Ref: inst.rec.Ref, Ballot: inst.rec.Ballot, RecordBallot: recordTupleBallot(inst.rec), Seq: inst.rec.Seq, Deps: inst.rec.Deps, IgnoreDependency: TryPreAcceptIgnore{Ref: ignore}, Command: inst.rec.Command.Borrow(), RecordStatus: inst.rec.Status}
		n.enqueueMessage(m)
	}
}

func (n *RawNode) broadcast(t MessageType, rec InstanceRecord) {
	for _, to := range n.q.conf.Voters {
		if to == n.id {
			continue
		}
		m := Message{Type: t, From: n.id, To: to, Ref: rec.Ref, Ballot: rec.Ballot, RecordBallot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps, Command: rec.Command.Borrow(), RecordStatus: rec.Status}
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
	m := Message{Type: t, From: n.id, To: to, Ref: ref, Ballot: hint, Reject: true, RejectReason: reason, RejectHint: hint, Deps: n.q.deps()}
	n.enqueueMessage(m)
}

func (n *RawNode) sendCommitTo(to ReplicaID, rec InstanceRecord) {
	m := Message{Type: MsgCommit, From: n.id, To: to, Ref: rec.Ref, Ballot: rec.Ballot, RecordBallot: recordTupleBallot(rec), Seq: rec.Seq, Deps: rec.Deps, AcceptSeq: rec.AcceptSeq, AcceptDeps: rec.AcceptDeps, AcceptEvidence: rec.AcceptEvidence, Command: rec.Command.Borrow(), RecordStatus: rec.Status}
	n.enqueueMessage(m)
}

func (n *RawNode) enqueueRecord(rec InstanceRecord) {
	if rec.Checksum == ([32]byte{}) {
		rec.Checksum = ChecksumRecord(rec)
	}
	n.pendingReady.Records = append(n.pendingReady.Records, n.readyRecord(rec))
	n.pendingReady.MustSync = true
}

func (n *RawNode) enqueueMessage(m Message) {
	if len(m.Deps) == 0 && (m.Type == MsgPreAcceptResp || m.Type == MsgAcceptResp || m.Type == MsgPrepareResp || m.Type == MsgTryPreAcceptResp || m.Type == MsgEvidenceResp) {
		m.Deps = n.q.deps()
	}
	m.Checksum = ChecksumMessage(m)
	n.pendingReady.Messages = append(n.pendingReady.Messages, m.Clone())
}

func (n *RawNode) enqueueCommitted(c CommittedCommand) {
	n.pendingReady.Committed = append(n.pendingReady.Committed, c.Clone())
}

func (n *RawNode) readyRecord(rec InstanceRecord) InstanceRecord {
	out := rec
	out.Deps = append([]InstanceNum(nil), rec.Deps...)
	if n.zeroCopy {
		out.Command = rec.Command.Borrow()
	} else {
		out.Command = rec.Command.Clone()
	}
	return out
}

func inboundCommand(cmd Command) Command { return cmd.Clone() }

func (n *RawNode) computeAttrs(cmd Command, exclude InstanceRef) Attributes {
	return n.computeAttrsAt(cmd, exclude, 0, false)
}

func (n *RawNode) computeAttrsAt(cmd Command, exclude InstanceRef, processAt uint64, timedPreAccept bool) Attributes {
	conf := n.confFor(exclude.Conf)
	deps := make([]InstanceNum, len(conf.Voters))
	seq := uint64(1)
	addRef := func(ref InstanceRef, rec InstanceRecord) {
		if ref == exclude || rec.TOQPending || rec.Status == StatusNone || rec.Command.Kind == CommandNoop || ref.Conf != exclude.Conf {
			return
		}
		if n.skipTimedConflict(cmd, exclude, processAt, timedPreAccept, ref, rec) {
			return
		}
		if idx, ok := conf.Index(ref.Replica); ok && ref.Instance > deps[idx] {
			deps[idx] = ref.Instance
			if rec.Seq >= seq {
				seq = rec.Seq + 1
			}
		}
	}
	if cmd.Kind == CommandNoop {
		return Attributes{Seq: seq, Deps: deps}
	}
	if cmd.Kind == CommandConfChange {
		for ref, inst := range n.instances {
			addRef(ref, inst.rec)
		}
		return Attributes{Seq: seq, Deps: deps}
	}
	if timedPreAccept {
		for ref, inst := range n.instances {
			if cmd.ConflictsWith(inst.rec.Command) {
				addRef(ref, inst.rec)
			}
		}
	} else {
		for _, key := range cmd.ConflictKeys {
			for _, ref := range n.conflicts[string(key)] {
				if ref == exclude || ref.Conf != exclude.Conf {
					continue
				}
				if idx, ok := conf.Index(ref.Replica); ok && ref.Instance > deps[idx] {
					deps[idx] = ref.Instance
					if inst := n.instances[ref]; inst != nil && inst.rec.Seq >= seq {
						seq = inst.rec.Seq + 1
					}
				}
			}
		}
	}
	for ref, inst := range n.instances {
		if inst.rec.Command.Kind == CommandConfChange {
			addRef(ref, inst.rec)
		}
	}
	return Attributes{Seq: seq, Deps: deps}
}

func (n *RawNode) skipTimedConflict(cmd Command, candidate InstanceRef, processAt uint64, timedPreAccept bool, ref InstanceRef, rec InstanceRecord) bool {
	if !timedPreAccept || cmd.Kind != CommandUser || rec.Command.Kind != CommandUser {
		return false
	}
	inst := n.instances[ref]
	if inst == nil || inst.rec.Status != StatusPreAccepted {
		return false
	}
	if inst.processAt == 0 && !n.toqEnabled {
		return false
	}
	return preAcceptOrderBefore(processAt, candidate, inst.processAt, ref)
}

func (n *RawNode) fastCommitAttrsPreAccept(inst *instance) (Attributes, bool) {
	localAttrs := inst.rec.Attributes()
	if n.canFastCommitPreAccept(inst, localAttrs) {
		return localAttrs, true
	}
	if !n.preAcceptOrderingEnabled() || (inst.processAt == 0 && !n.toqEnabled) || inst.rec.Ballot.Number != 0 || inst.rec.Ballot.Replica != inst.rec.Ref.Replica {
		return Attributes{}, false
	}
	conf := n.confFor(inst.rec.Ref.Conf)
	for _, id := range conf.Voters {
		if id == n.id {
			continue
		}
		vote, ok := inst.preOK[id]
		if !ok {
			continue
		}
		attrs := Attributes{Seq: vote.seq, Deps: vote.deps}
		if attrs.Equal(localAttrs) {
			continue
		}
		if n.canFastCommitPreAccept(inst, attrs) {
			return attrs, true
		}
	}
	return Attributes{}, false
}

func (n *RawNode) canFastCommitPreAccept(inst *instance, attrs Attributes) bool {
	localAttrs := inst.rec.Attributes()
	localMatches := attrs.Equal(localAttrs)
	if !localMatches {
		if !n.preAcceptOrderingEnabled() || (inst.processAt == 0 && !n.toqEnabled) || inst.rec.Ballot.Number != 0 || inst.rec.Ballot.Replica != inst.rec.Ref.Replica {
			return false
		}
		if !attrsCover(localAttrs, attrs) {
			return false
		}
	}
	count := 0
	matchingRemotes := 0
	covered := uint64(0)
	if localMatches {
		if !inst.rec.FastPathEligible {
			return false
		}
		count = 1
		covered = n.committedDepsMask(attrs, inst.rec.Ref.Conf)
	} else {
		count = 1
		covered = n.committedDepsMask(attrs, inst.rec.Ref.Conf)
		if n.toqEnabled && !inst.rec.FastPathEligible {
			return false
		}
	}
	for id, vote := range inst.preOK {
		if id == n.id {
			continue
		}
		if !(Attributes{Seq: vote.seq, Deps: vote.deps}).Equal(attrs) {
			continue
		}
		if (localMatches || n.toqEnabled) && !vote.fastPathEligible {
			continue
		}
		count++
		matchingRemotes++
		covered |= vote.depsCommitted
	}
	if !localMatches && !n.toqEnabled && matchingRemotes < len(n.confFor(inst.rec.Ref.Conf).Voters)-1 {
		return false
	}
	return count >= n.q.fastQuorum() && dependencyMask(attrs.Deps)&^covered == 0
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
	for inst := InstanceNum(1); inst <= through; inst++ {
		ref := InstanceRef{Replica: replica, Instance: inst, Conf: confID}
		if existing := n.instances[ref]; existing == nil || existing.rec.Status < StatusCommitted {
			return false
		}
	}
	return true
}

func addTryCandidate(candidates []tryCandidate, rec InstanceRecord, voter ReplicaID) []tryCandidate {
	for i := range candidates {
		if commandEqual(candidates[i].rec.Command, rec.Command) && candidates[i].rec.Attributes().Equal(rec.Attributes()) {
			candidates[i].voters[voter] = struct{}{}
			return candidates
		}
	}
	return append(candidates, tryCandidate{rec: rec.Clone(), voters: map[ReplicaID]struct{}{voter: {}}})
}

func (n *RawNode) indexConflicts(rec InstanceRecord) {
	if rec.Command.Kind == CommandNoop {
		return
	}
	if rec.Command.Kind == CommandConfChange {
		return
	}
	for _, key := range rec.Command.ConflictKeys {
		byReplica := n.conflicts[string(key)]
		if byReplica == nil {
			byReplica = make(map[ReplicaID]InstanceRef)
			n.conflicts[string(key)] = byReplica
		}
		if old, ok := byReplica[rec.Ref.Replica]; !ok || lessRef(old, rec.Ref) {
			byReplica[rec.Ref.Replica] = rec.Ref
		}
	}
}

func attrsFromVotes(votes map[ReplicaID]attrVote) Attributes {
	var out Attributes
	for _, v := range votes {
		out = mergeAttrs(out, Attributes{Seq: v.seq, Deps: v.deps})
	}
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

func (n *RawNode) tryExecute() {
	for {
		progress := false
		components := n.executionComponents()
		for _, comp := range components {
			if n.componentReady(comp) {
				sort.Slice(comp, func(i, j int) bool {
					a := n.instances[comp[i]].rec
					b := n.instances[comp[j]].rec
					if a.Seq != b.Seq {
						return a.Seq < b.Seq
					}
					return lessRef(a.Ref, b.Ref)
				})
				for _, ref := range comp {
					inst := n.instances[ref]
					n.executed[ref] = struct{}{}
					inst.rec.Status = StatusExecuted
					inst.rec.Checksum = ChecksumRecord(inst.rec)
					switch inst.rec.Command.Kind {
					case CommandUser:
						n.enqueueCommitted(CommittedCommand{Ref: ref, Seq: inst.rec.Seq, Deps: append([]InstanceNum(nil), inst.rec.Deps...), Command: inst.rec.Command.Clone()})
					case CommandConfChange:
						n.applyConfChange(ref, inst.rec.Command)
						n.enqueueRecord(inst.rec)
					default:
						n.enqueueRecord(inst.rec)
					}
					progress = true
				}
			}
		}
		if !progress {
			return
		}
	}
}

func (n *RawNode) executionComponents() [][]InstanceRef {
	refs := make([]InstanceRef, 0)
	for ref, inst := range n.instances {
		if inst.rec.Status >= StatusCommitted {
			if _, ok := n.executed[ref]; !ok {
				refs = append(refs, ref)
			}
		}
	}
	sortRefs(refs)
	index := make(map[InstanceRef]int, len(refs))
	low := make(map[InstanceRef]int, len(refs))
	onStack := make(map[InstanceRef]bool, len(refs))
	stack := make([]InstanceRef, 0, len(refs))
	var comps [][]InstanceRef
	var next int
	var visit func(InstanceRef)
	visit = func(v InstanceRef) {
		index[v] = next
		low[v] = next
		next++
		stack = append(stack, v)
		onStack[v] = true
		for _, w := range n.dependencyRefs(v) {
			if _, executed := n.executed[w]; executed {
				continue
			}
			wi := n.instances[w]
			if wi == nil || wi.rec.Status < StatusCommitted {
				continue
			}
			if n.dependencyKnownAfter(v, w, StatusCommitted) {
				continue
			}
			if _, seen := index[w]; !seen {
				visit(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] && index[w] < low[v] {
				low[v] = index[w]
			}
		}
		if low[v] == index[v] {
			var comp []InstanceRef
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				comp = append(comp, w)
				if w == v {
					break
				}
			}
			comps = append(comps, comp)
		}
	}
	for _, ref := range refs {
		if _, ok := index[ref]; !ok {
			visit(ref)
		}
	}
	return comps
}

func (n *RawNode) componentReady(comp []InstanceRef) bool {
	inside := make(map[InstanceRef]struct{}, len(comp))
	for _, ref := range comp {
		inside[ref] = struct{}{}
	}
	for _, ref := range comp {
		if n.hasUnresolvedKnownConflict(ref, inside) {
			return false
		}
		for _, dep := range n.dependencyRefs(ref) {
			if _, ok := inside[dep]; ok {
				continue
			}
			if _, ok := n.executed[dep]; ok {
				continue
			}
			inst := n.instances[dep]
			if inst == nil || inst.rec.Status < StatusCommitted {
				n.ensureDependencyRecovery(dep)
				return false
			}
			if n.dependencyKnownAfter(ref, dep, StatusCommitted) {
				continue
			}
			return false
		}
	}
	return true
}

func (n *RawNode) ensureDependencyRecovery(ref InstanceRef) {
	if ref.IsZero() {
		return
	}
	if _, ok := n.executed[ref]; ok {
		return
	}
	inst := n.instances[ref]
	if inst == nil {
		rec := InstanceRecord{Ref: ref, Status: StatusNone, Deps: n.depsForConf(ref.Conf)}
		rec.Checksum = ChecksumRecord(rec)
		inst = &instance{rec: rec, phase: phaseIdle}
		n.instances[ref] = inst
	}
	if inst.rec.Status >= StatusCommitted {
		return
	}
	if (inst.phase == phasePreAccept || inst.phase == phaseAccept || inst.phase == phasePrepare) && n.coordinatesInstance(inst) {
		return
	}
	if n.promisedToOtherCoordinator(inst) {
		return
	}
	n.startPrepare(inst)
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

func (n *RawNode) hasUnresolvedKnownConflict(ref InstanceRef, inside map[InstanceRef]struct{}) bool {
	inst := n.instances[ref]
	if inst == nil {
		return false
	}
	for otherRef, other := range n.instances {
		if otherRef == ref {
			continue
		}
		if _, ok := inside[otherRef]; ok {
			continue
		}
		if _, ok := n.executed[otherRef]; ok {
			continue
		}
		if other.rec.Status == StatusNone || other.rec.Status >= StatusCommitted {
			continue
		}
		if inst.rec.Command.ConflictsWith(other.rec.Command) && !n.dependencyKnownAfter(ref, otherRef, StatusPreAccepted) {
			return true
		}
	}
	return false
}

func (n *RawNode) dependencyRefs(ref InstanceRef) []InstanceRef {
	inst := n.instances[ref]
	if inst == nil {
		return nil
	}
	conf := n.confFor(ref.Conf)
	out := make([]InstanceRef, 0, len(inst.rec.Deps))
	for i, dep := range inst.rec.Deps {
		if dep == 0 || i >= len(conf.Voters) {
			continue
		}
		replica := conf.Voters[i]
		for num := InstanceNum(1); ; num++ {
			other := InstanceRef{Replica: replica, Instance: num, Conf: ref.Conf}
			if other != ref {
				out = append(out, other)
			}
			if num == dep {
				break
			}
		}
	}
	sortRefs(out)
	return out
}

func (n *RawNode) applyConfChange(ref InstanceRef, cmd Command) {
	defer n.refreshPendingConf()
	if len(cmd.Payload) != 9 {
		return
	}
	cc := ConfChange{Type: ConfChangeType(cmd.Payload[0]), Replica: ReplicaID(binary.LittleEndian.Uint64(cmd.Payload[1:]))}
	base := n.confFor(ref.Conf)
	q, err := confChangeQuorumFrom(base, cc)
	if err != nil {
		return
	}
	q.conf.ID = ref.Conf + 1
	if _, exists := n.confHistory[q.conf.ID]; exists {
		return
	}
	n.confHistory[q.conf.ID] = q.conf.Clone()
	if ref.Conf == n.q.conf.ID {
		n.q = q
	}
}
