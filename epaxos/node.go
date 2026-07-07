package epaxos

import (
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
	phaseCommitted
)

type attrVote struct {
	seq  uint64
	deps []InstanceNum
}

type instance struct {
	rec          InstanceRecord
	phase        phase
	preOK        map[ReplicaID]attrVote
	accOK        map[ReplicaID]struct{}
	prepareOK    map[ReplicaID]InstanceRecord
	createdTick  uint64
	waitDeadline uint64
	generation   uint64
}

type timerKind uint8

const (
	timerPreAccept timerKind = iota + 1
	timerAccept
	timerPrepare
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
	maxReadyMessages      int

	tick         uint64
	nextInstance InstanceNum
	instances    map[InstanceRef]*instance
	conflicts    map[string]map[ReplicaID]InstanceRef
	executed     map[InstanceRef]struct{}
	pendingConf  bool
	timers       timerHeap
	pendingReady Ready
	awaitAdvance bool
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
	n.tick = hs.Tick
	if err := cfg.Storage.LoadInstances(func(rec InstanceRecord) error {
		if !VerifyRecordChecksum(rec) {
			return ErrChecksumMismatch
		}
		phase := phaseFromStatus(rec.Status)
		if rec.Ref.Replica == n.id && rec.Status < StatusCommitted && rec.Status != StatusNone && rec.Ballot.Number > 0 {
			phase = phasePrepare
		}
		n.instances[rec.Ref] = &instance{rec: rec.Clone(), phase: phase}
		if rec.Ref.Replica == n.id && rec.Ref.Instance >= n.nextInstance {
			n.nextInstance = rec.Ref.Instance + 1
		}
		if rec.Status >= StatusPreAccepted {
			n.indexConflicts(rec)
		}
		if rec.Status == StatusExecuted {
			n.executed[rec.Ref] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	heap.Init(&n.timers)
	n.replayExecutedConfig()
	for _, inst := range n.instances {
		if inst.rec.Ref.Replica != n.id || inst.rec.Status >= StatusCommitted {
			continue
		}
		switch inst.phase {
		case phasePreAccept:
			n.schedule(inst, timerPreAccept, n.retryTicks)
		case phaseAccept:
			n.schedule(inst, timerAccept, n.retryTicks)
		case phasePrepare:
			n.schedule(inst, timerPrepare, n.recoveryTicks)
		}
	}
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

func (n *RawNode) replayExecutedConfig() {
	refs := make([]InstanceRef, 0, len(n.instances))
	for ref, inst := range n.instances {
		if inst.rec.Status == StatusExecuted && inst.rec.Command.Kind == CommandConfChange {
			refs = append(refs, ref)
		}
	}
	sortRefs(refs)
	for _, ref := range refs {
		n.applyConfChange(n.instances[ref].rec.Command)
	}
}

// Tick advances deterministic logical time by one tick and processes due timers.
func (n *RawNode) Tick() {
	n.tick++
	if n.recoveryTicks > 0 && n.tick%n.recoveryTicks == 0 {
		n.rebroadcastCommits()
	}
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

// Propose creates a new local EPaxos instance for an application command.
func (n *RawNode) Propose(cmd Command) (InstanceRef, error) {
	if cmd.Kind == CommandConfChange {
		return InstanceRef{}, fmt.Errorf("%w: use ProposeConfChange", ErrInvalidConfig)
	}
	if !n.zeroCopy {
		cmd = cmd.Clone()
	}
	if cmd.Kind != CommandNoop {
		cmd.Kind = CommandUser
	}
	return n.propose(cmd)
}

// ProposeConfChange proposes a membership change encoded as a consensus command.
func (n *RawNode) ProposeConfChange(cc ConfChange) (InstanceRef, error) {
	if n.pendingConf {
		return InstanceRef{}, fmt.Errorf("%w: configuration change already pending", ErrMessageRejected)
	}
	var payload [9]byte
	payload[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(payload[1:], uint64(cc.Replica))
	cmd := Command{Kind: CommandConfChange, Payload: append([]byte(nil), payload[:]...), ConflictKeys: [][]byte{[]byte("\xffconf")}}
	ref, err := n.propose(cmd)
	if err == nil {
		n.pendingConf = true
	}
	return ref, err
}

func (n *RawNode) propose(cmd Command) (InstanceRef, error) {
	ref := InstanceRef{Replica: n.id, Instance: n.nextInstance, Conf: n.q.conf.ID}
	n.nextInstance++
	attrs := n.computeAttrs(cmd, ref)
	rec := InstanceRecord{Ref: ref, Ballot: Ballot{Replica: n.id}, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd}
	rec.Checksum = ChecksumRecord(rec)
	inst := &instance{rec: rec, phase: phasePreAccept, preOK: map[ReplicaID]attrVote{n.id: {seq: rec.Seq, deps: append([]InstanceNum(nil), rec.Deps...)}}, createdTick: n.tick}
	n.instances[ref] = inst
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	if n.q.slowQuorum() == 1 {
		n.commit(inst, rec.Attributes())
		return ref, nil
	}
	n.broadcast(MsgPreAccept, rec)
	n.schedule(inst, timerPreAccept, n.retryTicks)
	return ref, nil
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
	switch m.Type {
	case MsgPreAccept:
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
	}
	return nil
}

// HasReady reports whether Ready would return a non-empty batch.
func (n *RawNode) HasReady() bool { return !n.awaitAdvance && !n.pendingReady.Empty() }

// Ready returns the next batch of persistent records, messages, and commands.
func (n *RawNode) Ready() Ready {
	if n.awaitAdvance || n.pendingReady.Empty() {
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

// Advance acknowledges that the caller persisted and applied the Ready batch.
func (n *RawNode) Advance(rd Ready) {
	if !n.awaitAdvance {
		return
	}
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
	n.awaitAdvance = false
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
	attrs := n.computeAttrs(m.Command, m.Ref)
	attrs = mergeAttrs(attrs, m.Attributes())
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: inboundCommand(m.Command)}
	if old := n.instances[m.Ref]; old != nil {
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
	n.instances[m.Ref] = &instance{rec: rec, phase: phasePreAccept}
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	resp := Message{Type: MsgPreAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: append([]InstanceNum(nil), rec.Deps...), RecordStatus: rec.Status}
	n.enqueueMessage(resp)
}

func (n *RawNode) handlePreAcceptResp(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePreAccept || m.Ref.Replica != n.id {
		return
	}
	if m.Reject {
		if inst.rec.Ballot.Less(m.RejectHint) {
			inst.rec.Ballot = m.RejectHint.Next(n.id)
		}
		n.startPrepare(inst)
		return
	}
	if inst.preOK == nil {
		inst.preOK = make(map[ReplicaID]attrVote, n.q.fastQuorum())
	}
	if _, duplicate := inst.preOK[m.From]; duplicate {
		return
	}
	inst.preOK[m.From] = attrVote{seq: m.Seq, deps: append([]InstanceNum(nil), m.Deps...)}
	attrs := attrsFromVotes(inst.preOK)
	if len(inst.preOK) >= n.q.fastQuorum() && attrs.Equal(inst.rec.Attributes()) {
		n.commit(inst, attrs)
		return
	}
	if len(inst.preOK) >= n.q.slowQuorum() {
		if n.timeOptimization && len(inst.preOK) < n.q.fastQuorum() && n.tick < inst.createdTick+n.timeOptimizationTicks {
			inst.waitDeadline = inst.createdTick + n.timeOptimizationTicks
			n.schedule(inst, timerFastWait, inst.waitDeadline-n.tick)
			return
		}
		n.startAccept(inst, attrs)
	}
}

func (n *RawNode) handleAccept(m Message) {
	if old := n.instances[m.Ref]; old != nil {
		if old.rec.Status >= StatusCommitted {
			n.sendCommitTo(m.From, old.rec)
			return
		}
		if m.Ballot.Less(old.rec.Ballot) {
			n.sendReject(MsgAcceptResp, m.From, m.Ref, old.rec.Ballot)
			return
		}
	}
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, Status: StatusAccepted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: inboundCommand(m.Command)}
	rec.Checksum = ChecksumRecord(rec)
	n.instances[m.Ref] = &instance{rec: rec, phase: phaseAccept}
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	resp := Message{Type: MsgAcceptResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: append([]InstanceNum(nil), rec.Deps...), RecordStatus: rec.Status}
	n.enqueueMessage(resp)
}

func (n *RawNode) handleAcceptResp(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phaseAccept || m.Ref.Replica != n.id {
		return
	}
	if m.Reject {
		if inst.rec.Ballot.Less(m.RejectHint) {
			inst.rec.Ballot = m.RejectHint.Next(n.id)
		}
		n.startPrepare(inst)
		return
	}
	if inst.accOK == nil {
		inst.accOK = make(map[ReplicaID]struct{}, n.q.slowQuorum())
	}
	if _, duplicate := inst.accOK[m.From]; duplicate {
		return
	}
	inst.accOK[m.From] = struct{}{}
	if len(inst.accOK) >= n.q.slowQuorum() {
		n.commit(inst, inst.rec.Attributes())
	}
}

func (n *RawNode) handleCommit(m Message) {
	rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, Status: StatusCommitted, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: inboundCommand(m.Command)}
	rec.Checksum = ChecksumRecord(rec)
	old := n.instances[m.Ref]
	if old != nil && old.rec.Status >= StatusCommitted && old.rec.Checksum == rec.Checksum {
		return
	}
	n.instances[m.Ref] = &instance{rec: rec, phase: phaseCommitted}
	n.indexConflicts(rec)
	n.enqueueRecord(rec)
	n.tryExecute()
}

func (n *RawNode) handlePrepare(m Message) {
	resp := Message{Type: MsgPrepareResp, From: n.id, To: m.From, Ref: m.Ref, Ballot: m.Ballot}
	inst := n.instances[m.Ref]
	if inst != nil && m.Ballot.Less(inst.rec.Ballot) {
		n.sendReject(MsgPrepareResp, m.From, m.Ref, inst.rec.Ballot)
		return
	}
	if inst == nil {
		rec := InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, Status: StatusNone, Deps: make([]InstanceNum, len(n.confFor(m.Ref.Conf).Voters))}
		rec.Checksum = ChecksumRecord(rec)
		inst = &instance{rec: rec, phase: phaseIdle}
		n.instances[m.Ref] = inst
		n.enqueueRecord(rec)
	} else if inst.rec.Ballot.Less(m.Ballot) {
		inst.rec.Ballot = m.Ballot
		inst.rec.Checksum = ChecksumRecord(inst.rec)
		n.enqueueRecord(inst.rec)
	}
	resp.Ballot = inst.rec.Ballot
	resp.Seq = inst.rec.Seq
	resp.Deps = append([]InstanceNum(nil), inst.rec.Deps...)
	resp.Command = inst.rec.Command.Clone()
	resp.RecordStatus = inst.rec.Status
	n.enqueueMessage(resp)
}

func (n *RawNode) handlePrepareResp(m Message) {
	inst := n.instances[m.Ref]
	if inst == nil || inst.phase != phasePrepare || m.Ref.Replica != n.id {
		return
	}
	if inst.prepareOK == nil {
		inst.prepareOK = make(map[ReplicaID]InstanceRecord, n.q.slowQuorum())
	}
	if _, duplicate := inst.prepareOK[m.From]; duplicate {
		return
	}
	inst.prepareOK[m.From] = InstanceRecord{Ref: m.Ref, Ballot: m.Ballot, Status: m.RecordStatus, Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...), Command: m.Command.Clone()}
	if len(inst.prepareOK) < n.q.slowQuorum() {
		return
	}
	chosen := inst.rec
	attrs := inst.rec.Attributes()
	for _, rec := range inst.prepareOK {
		if rec.Status == StatusCommitted {
			chosen = rec
			chosen.Status = StatusCommitted
			chosen.Checksum = ChecksumRecord(chosen)
			inst.rec = chosen
			n.commit(inst, chosen.Attributes())
			return
		}
		if rec.Status >= StatusPreAccepted {
			attrs = mergeAttrs(attrs, rec.Attributes())
			if chosen.Ballot.Less(rec.Ballot) {
				chosen = rec
			}
		}
	}
	inst.rec.Command = chosen.Command.Clone()
	n.startAccept(inst, attrs)
}

func (n *RawNode) startAccept(inst *instance, attrs Attributes) {
	if inst.phase == phaseCommitted {
		return
	}
	inst.phase = phaseAccept
	inst.rec.Status = StatusAccepted
	inst.rec.Seq = attrs.Seq
	inst.rec.Deps = append([]InstanceNum(nil), attrs.Deps...)
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

func (n *RawNode) startPrepare(inst *instance) {
	inst.phase = phasePrepare
	inst.rec.Ballot = inst.rec.Ballot.Next(n.id)
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	inst.prepareOK = map[ReplicaID]InstanceRecord{n.id: inst.rec.Clone()}
	inst.generation++
	n.enqueueRecord(inst.rec)
	n.broadcast(MsgPrepare, inst.rec)
	n.schedule(inst, timerPrepare, n.recoveryTicks)
}

func (n *RawNode) commit(inst *instance, attrs Attributes) {
	if inst.phase == phaseCommitted && inst.rec.Status >= StatusCommitted {
		return
	}
	inst.phase = phaseCommitted
	inst.rec.Status = StatusCommitted
	inst.rec.Seq = attrs.Seq
	inst.rec.Deps = append([]InstanceNum(nil), attrs.Deps...)
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
			n.broadcast(MsgPreAccept, inst.rec)
			n.schedule(inst, timerPreAccept, n.retryTicks)
		}
	case timerAccept:
		if inst.phase == phaseAccept {
			n.broadcast(MsgAccept, inst.rec)
			n.schedule(inst, timerAccept, n.retryTicks)
		}
	case timerPrepare:
		if inst.phase == phasePrepare {
			n.broadcast(MsgPrepare, inst.rec)
			n.schedule(inst, timerPrepare, n.recoveryTicks)
		}
	}
}

func (n *RawNode) schedule(inst *instance, kind timerKind, after uint64) {
	if after == 0 {
		after = 1
	}
	heap.Push(&n.timers, timer{deadline: n.tick + after, ref: inst.rec.Ref, kind: kind, gen: inst.generation})
}

func (n *RawNode) broadcast(t MessageType, rec InstanceRecord) {
	for _, to := range n.q.conf.Voters {
		if to == n.id {
			continue
		}
		m := Message{Type: t, From: n.id, To: to, Ref: rec.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: append([]InstanceNum(nil), rec.Deps...), Command: rec.Command.Clone(), RecordStatus: rec.Status}
		n.enqueueMessage(m)
	}
}

func (n *RawNode) sendReject(t MessageType, to ReplicaID, ref InstanceRef, hint Ballot) {
	m := Message{Type: t, From: n.id, To: to, Ref: ref, Ballot: hint, Reject: true, RejectHint: hint, Deps: n.q.deps()}
	n.enqueueMessage(m)
}

func (n *RawNode) sendCommitTo(to ReplicaID, rec InstanceRecord) {
	m := Message{Type: MsgCommit, From: n.id, To: to, Ref: rec.Ref, Ballot: rec.Ballot, Seq: rec.Seq, Deps: append([]InstanceNum(nil), rec.Deps...), Command: rec.Command.Clone(), RecordStatus: rec.Status}
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
	if len(m.Deps) == 0 && (m.Type == MsgPreAcceptResp || m.Type == MsgAcceptResp || m.Type == MsgPrepareResp) {
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
	deps := n.q.deps()
	seq := uint64(1)
	if cmd.Kind == CommandNoop {
		return Attributes{Seq: seq, Deps: deps}
	}
	if cmd.Kind == CommandConfChange {
		for _, inst := range n.instances {
			if inst.rec.Ref == exclude || inst.rec.Status == StatusNone || inst.rec.Command.Kind == CommandNoop {
				continue
			}
			if idx, ok := n.q.depIndex(inst.rec.Ref.Replica); ok && inst.rec.Ref.Instance > deps[idx] {
				deps[idx] = inst.rec.Ref.Instance
				if inst.rec.Seq >= seq {
					seq = inst.rec.Seq + 1
				}
			}
		}
		return Attributes{Seq: seq, Deps: deps}
	}
	for _, key := range cmd.ConflictKeys {
		for _, ref := range n.conflicts[string(key)] {
			if ref == exclude {
				continue
			}
			if idx, ok := n.q.depIndex(ref.Replica); ok && ref.Instance > deps[idx] {
				deps[idx] = ref.Instance
				if inst := n.instances[ref]; inst != nil && inst.rec.Seq >= seq {
					seq = inst.rec.Seq + 1
				}
			}
		}
	}
	return Attributes{Seq: seq, Deps: deps}
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
					n.enqueueRecord(inst.rec)
					if inst.rec.Command.Kind == CommandConfChange {
						n.applyConfChange(inst.rec.Command)
					}
					if inst.rec.Command.Kind != CommandNoop {
						n.enqueueCommitted(CommittedCommand{Ref: ref, Seq: inst.rec.Seq, Deps: append([]InstanceNum(nil), inst.rec.Deps...), Command: inst.rec.Command.Clone()})
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
			if inst := n.instances[dep]; inst != nil && inst.rec.Status >= StatusCommitted {
				return false
			}
		}
	}
	return true
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
		if inst.rec.Command.ConflictsWith(other.rec.Command) {
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
		out = append(out, InstanceRef{Replica: conf.Voters[i], Instance: dep, Conf: ref.Conf})
	}
	return out
}

func (n *RawNode) applyConfChange(cmd Command) {
	defer func() { n.pendingConf = false }()
	if len(cmd.Payload) != 9 {
		return
	}
	cc := ConfChange{Type: ConfChangeType(cmd.Payload[0]), Replica: ReplicaID(binary.LittleEndian.Uint64(cmd.Payload[1:]))}
	voters := append([]ReplicaID(nil), n.q.conf.Voters...)
	switch cc.Type {
	case ConfChangeAddVoter:
		if cc.Replica == 0 {
			return
		}
		if _, ok := n.q.depIndex(cc.Replica); !ok {
			voters = append(voters, cc.Replica)
		}
	case ConfChangeRemoveVoter:
		filtered := voters[:0]
		for _, id := range voters {
			if id != cc.Replica {
				filtered = append(filtered, id)
			}
		}
		voters = filtered
	default:
		return
	}
	q, err := newQuorum(voters)
	if err != nil {
		return
	}
	q.conf.ID = n.q.conf.ID + 1
	n.confHistory[q.conf.ID] = q.conf.Clone()
	n.q = q
}
