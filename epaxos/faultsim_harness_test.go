package epaxos

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

var errFaultSimBackpressure = errors.New("faultsim deterministic backpressure")

const (
	faultActionPropose             = "propose"
	faultActionConfChange          = "conf-change"
	faultActionPump                = "pump-ready"
	faultActionDeliver             = "deliver"
	faultActionDrop                = "drop"
	faultActionDuplicate           = "duplicate"
	faultActionHold                = "hold"
	faultActionRelease             = "release"
	faultActionCorrupt             = "corrupt"
	faultActionPartition           = "partition"
	faultActionHeal                = "heal"
	faultActionCrash               = "crash"
	faultActionSnapshot            = "snapshot"
	faultActionStorageFailure      = "storage-failure"
	faultActionTick                = "tick"
	faultActionBurst               = "burst"
	faultActionPause               = "pause"
	faultActionResume              = "resume"
	faultActionSetClock            = "set-clock"
	faultActionProcessTOQ          = "process-toq"
	faultActionInjectMalformed     = "inject-malformed"
	faultActionNotApplicable       = "not-applicable"
	faultActionPrerequisiteMissing = "prerequisite-missing"
)

type faultCrashCut string

const (
	faultCrashBeforeReady              faultCrashCut = "before-ready"
	faultCrashAfterFrozenReady         faultCrashCut = "after-frozen-ready-before-persistence"
	faultCrashAfterPersistence         faultCrashCut = "after-records-hard-state-before-application"
	faultCrashAfterApplication         faultCrashCut = "after-application-before-advance"
	faultCrashAfterAdvance             faultCrashCut = "after-advance-before-executed-persistence"
	faultCrashAfterExecutedPersistence faultCrashCut = "after-executed-persistence"
)

type faultKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type faultClientOperation struct {
	Client   uint64    `json:"client"`
	Sequence uint64    `json:"sequence"`
	Kind     string    `json:"kind"`
	Reads    []string  `json:"reads,omitempty"`
	Writes   []faultKV `json:"writes,omitempty"`
	Deletes  []string  `json:"deletes,omitempty"`
}

func (o faultClientOperation) commandID() CommandID {
	return CommandID{Client: o.Client, Sequence: o.Sequence}
}

func (o faultClientOperation) clone() faultClientOperation {
	out := o
	out.Reads = append([]string(nil), o.Reads...)
	out.Writes = append([]faultKV(nil), o.Writes...)
	out.Deletes = append([]string(nil), o.Deletes...)
	return out
}

func (o faultClientOperation) command() (Command, error) {
	switch o.Kind {
	case "get":
		if len(o.Reads) != 1 || len(o.Writes) != 0 || len(o.Deletes) != 0 {
			return Command{}, fmt.Errorf("invalid get operation: %#v", o)
		}
	case "put":
		if len(o.Writes) != 1 || len(o.Reads) != 0 || len(o.Deletes) != 0 {
			return Command{}, fmt.Errorf("invalid put operation: %#v", o)
		}
	case "delete":
		if len(o.Deletes) != 1 || len(o.Reads) != 0 || len(o.Writes) != 0 {
			return Command{}, fmt.Errorf("invalid delete operation: %#v", o)
		}
	case "txn":
		if len(o.Reads)+len(o.Writes)+len(o.Deletes) == 0 {
			return Command{}, fmt.Errorf("empty transaction: %#v", o)
		}
	default:
		return Command{}, fmt.Errorf("unknown operation kind %q", o.Kind)
	}
	payload, err := json.Marshal(o)
	if err != nil {
		return Command{}, err
	}
	keys := make(map[string]struct{}, len(o.Reads)+len(o.Writes)+len(o.Deletes))
	for _, key := range o.Reads {
		keys[key] = struct{}{}
	}
	for _, write := range o.Writes {
		keys[write.Key] = struct{}{}
	}
	for _, key := range o.Deletes {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	conflicts := make([][]byte, len(ordered))
	for i, key := range ordered {
		conflicts[i] = []byte(key)
	}
	return Command{ID: o.commandID(), Payload: payload, ConflictKeys: conflicts}, nil
}

func faultDecodeOperation(cmd Command) (faultClientOperation, error) {
	var op faultClientOperation
	if cmd.Kind != CommandUser {
		return op, fmt.Errorf("command kind %d is not a client operation", cmd.Kind)
	}
	dec := json.NewDecoder(bytes.NewReader(cmd.Payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&op); err != nil {
		return op, err
	}
	if op.commandID() != cmd.ID {
		return op, fmt.Errorf("payload id %#v does not match command id %#v", op.commandID(), cmd.ID)
	}
	if _, err := op.command(); err != nil {
		return op, err
	}
	return op, nil
}

type faultOperationResponse struct {
	Reads []faultKV `json:"reads,omitempty"`
}

func (r faultOperationResponse) equal(other faultOperationResponse) bool {
	if len(r.Reads) != len(other.Reads) {
		return false
	}
	for i := range r.Reads {
		if r.Reads[i] != other.Reads[i] {
			return false
		}
	}
	return true
}

type faultApplicationEvent struct {
	Ref      InstanceRef            `json:"ref"`
	Command  Command                `json:"command"`
	Response faultOperationResponse `json:"response,omitempty"`
}

type faultApplicationImage struct {
	State   map[string]string
	Applied map[InstanceRef]faultApplicationEvent
	Log     []faultApplicationEvent
}

func newFaultApplicationImage() faultApplicationImage {
	return faultApplicationImage{State: make(map[string]string), Applied: make(map[InstanceRef]faultApplicationEvent)}
}

func (a faultApplicationImage) clone() faultApplicationImage {
	out := newFaultApplicationImage()
	for key, value := range a.State {
		out.State[key] = value
	}
	for ref, event := range a.Applied {
		out.Applied[ref] = faultCloneApplicationEvent(event)
	}
	out.Log = make([]faultApplicationEvent, len(a.Log))
	for i, event := range a.Log {
		out.Log[i] = faultCloneApplicationEvent(event)
	}
	return out
}

func faultCloneApplicationEvent(event faultApplicationEvent) faultApplicationEvent {
	out := event
	out.Command = event.Command.Clone()
	out.Response.Reads = append([]faultKV(nil), event.Response.Reads...)
	return out
}

func (a *faultApplicationImage) apply(committed []CommittedCommand) ([]faultApplicationEvent, error) {
	next := a.clone()
	newEvents := make([]faultApplicationEvent, 0, len(committed))
	for _, item := range committed {
		if existing, ok := next.Applied[item.Ref]; ok {
			if !faultSameCommand(existing.Command, item.Command) {
				return nil, fmt.Errorf("application replay for %s changed command", item.Ref)
			}
			continue
		}
		event := faultApplicationEvent{Ref: item.Ref, Command: item.Command.Clone()}
		if item.Command.Kind == CommandUser {
			op, err := faultDecodeOperation(item.Command)
			if err != nil {
				return nil, fmt.Errorf("decode committed operation %s: %w", item.Ref, err)
			}
			for _, key := range op.Reads {
				event.Response.Reads = append(event.Response.Reads, faultKV{Key: key, Value: next.State[key]})
			}
			for _, write := range op.Writes {
				next.State[write.Key] = write.Value
			}
			for _, key := range op.Deletes {
				delete(next.State, key)
			}
		}
		next.Applied[item.Ref] = event
		next.Log = append(next.Log, event)
		newEvents = append(newEvents, event)
	}
	*a = next
	return newEvents, nil
}

type faultHistoryOperation struct {
	Operation faultClientOperation   `json:"operation"`
	Proposer  ReplicaID              `json:"proposer"`
	Ref       InstanceRef            `json:"ref"`
	Invoke    uint64                 `json:"invoke"`
	Complete  uint64                 `json:"complete,omitempty"`
	Result    string                 `json:"result"`
	Response  faultOperationResponse `json:"response,omitempty"`
}

type faultSimConfig struct {
	Size                int    `json:"size"`
	Seed                uint64 `json:"seed"`
	TOQ                 bool   `json:"toq,omitempty"`
	MaxEnvelopes        int    `json:"max_envelopes"`
	MaxOutstanding      int    `json:"max_outstanding_clients"`
	MaxApplicationBatch int    `json:"max_application_batch"`
	MaxReadyMessages    int    `json:"max_ready_messages"`
}

func (c faultSimConfig) normalized() faultSimConfig {
	if c.MaxEnvelopes <= 0 {
		c.MaxEnvelopes = 4096
	}
	if c.MaxOutstanding <= 0 {
		c.MaxOutstanding = 32
	}
	if c.MaxApplicationBatch <= 0 {
		c.MaxApplicationBatch = 256
	}
	if c.MaxReadyMessages <= 0 {
		c.MaxReadyMessages = 16
	}
	return c
}

type faultEnvelopeState string

const (
	faultEnvelopeQueued   faultEnvelopeState = "queued"
	faultEnvelopeHeld     faultEnvelopeState = "held"
	faultEnvelopeConsumed faultEnvelopeState = "consumed"
)

type faultEnvelope struct {
	ID          uint64
	Parent      uint64
	From        ReplicaID
	To          ReplicaID
	CreatedStep uint64
	Attempt     uint64
	wire        []byte
}

func (e faultEnvelope) clone() faultEnvelope {
	out := e
	out.wire = append([]byte(nil), e.wire...)
	return out
}

func (e faultEnvelope) bytes() []byte { return append([]byte(nil), e.wire...) }

type faultSimAction struct {
	Kind       string                `json:"kind"`
	Node       ReplicaID             `json:"node,omitempty"`
	Envelope   uint64                `json:"envelope,omitempty"`
	From       ReplicaID             `json:"from,omitempty"`
	To         ReplicaID             `json:"to,omitempty"`
	Count      int                   `json:"count,omitempty"`
	Clock      uint64                `json:"clock,omitempty"`
	Cut        faultCrashCut         `json:"cut,omitempty"`
	Image      string                `json:"image,omitempty"`
	Name       string                `json:"name,omitempty"`
	Enabled    bool                  `json:"enabled,omitempty"`
	Mutation   string                `json:"mutation,omitempty"`
	Reason     string                `json:"reason,omitempty"`
	Data       []byte                `json:"data,omitempty"`
	Operation  *faultClientOperation `json:"operation,omitempty"`
	ConfChange *ConfChange           `json:"conf_change,omitempty"`
	Required   bool                  `json:"required,omitempty"`
}

func (a faultSimAction) clone() faultSimAction {
	out := a
	out.Data = append([]byte(nil), a.Data...)
	if a.Operation != nil {
		op := a.Operation.clone()
		out.Operation = &op
	}
	if a.ConfChange != nil {
		cc := *a.ConfChange
		out.ConfChange = &cc
	}
	return out
}

type faultActionReceipt struct {
	Index    int    `json:"index"`
	Step     uint64 `json:"step"`
	Kind     string `json:"kind"`
	Affected uint64 `json:"affected"`
	Rejected bool   `json:"rejected,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

type faultFailClosedCheck struct {
	Name string
	OK   bool
}

type faultDurableImage struct {
	Store *MemoryStorage
	App   faultApplicationImage
}

type faultReplica struct {
	node   *RawNode
	store  *MemoryStorage
	app    faultApplicationImage
	images map[string]faultDurableImage
	paused bool
}

type faultSimHarness struct {
	cfg               faultSimConfig
	ids               []ReplicaID
	replicas          map[ReplicaID]*faultReplica
	envelopes         map[uint64]faultEnvelope
	envelopeState     map[uint64]faultEnvelopeState
	partitions        map[[2]ReplicaID]bool
	clocks            map[ReplicaID]uint64
	nextEnvelope      uint64
	step              uint64
	actions           []faultSimAction
	receipts          []faultActionReceipt
	counters          map[string]uint64
	history           []faultHistoryOperation
	historyByID       map[CommandID]int
	outstanding       int
	failClosed        []faultFailClosedCheck
	lastProgressBound int
}

func newFaultSimHarness(cfg faultSimConfig) (*faultSimHarness, error) {
	cfg = cfg.normalized()
	if cfg.Size < 1 || cfg.Size > 7 {
		return nil, fmt.Errorf("faultsim size %d outside 1..7", cfg.Size)
	}
	h := &faultSimHarness{
		cfg:           cfg,
		ids:           makeIDs(cfg.Size),
		replicas:      make(map[ReplicaID]*faultReplica, cfg.Size),
		envelopes:     make(map[uint64]faultEnvelope),
		envelopeState: make(map[uint64]faultEnvelopeState),
		partitions:    make(map[[2]ReplicaID]bool),
		clocks:        make(map[ReplicaID]uint64, cfg.Size),
		counters:      make(map[string]uint64),
		historyByID:   make(map[CommandID]int),
	}
	for _, id := range h.ids {
		store := NewMemoryStorage()
		replica := &faultReplica{store: store, app: newFaultApplicationImage(), images: make(map[string]faultDurableImage)}
		h.replicas[id] = replica
		node, err := h.newRawNode(id, store)
		if err != nil {
			return nil, fmt.Errorf("new faultsim node %d: %w", id, err)
		}
		replica.node = node
		h.saveImage(id, "current")
	}
	return h, nil
}

func (h *faultSimHarness) newRawNode(id ReplicaID, store *MemoryStorage) (*RawNode, error) {
	cfg := Config{
		ID:               id,
		Voters:           h.ids,
		Storage:          store,
		RetryTicks:       2,
		RecoveryTicks:    5,
		MaxReadyMessages: h.cfg.MaxReadyMessages,
	}
	if h.cfg.TOQ {
		delays := make(map[ReplicaID]uint64, len(h.ids))
		for _, voter := range h.ids {
			delays[voter] = 0
		}
		cfg.TOQ = true
		cfg.TOQClock = func() uint64 { return h.clocks[id] }
		cfg.TOQRuntime = &TOQRuntimeConfig{Conf: ConfState{ID: 1, Voters: append([]ReplicaID(nil), h.ids...)}, OneWayDelay: delays}
	}
	return NewRawNode(cfg)
}

func faultCloneMemoryStorage(store *MemoryStorage) *MemoryStorage {
	out := &MemoryStorage{
		Hard:       store.Hard.Clone(),
		Configs:    make([]ConfState, len(store.Configs)),
		Records:    make(map[InstanceRef]InstanceRecord, len(store.Records)),
		FailWrites: store.FailWrites,
	}
	for i, conf := range store.Configs {
		out.Configs[i] = conf.Clone()
	}
	for ref, record := range store.Records {
		out.Records[ref] = record.Clone()
	}
	return out
}

func (h *faultSimHarness) saveImage(id ReplicaID, name string) {
	r := h.replicas[id]
	r.images[name] = faultDurableImage{Store: faultCloneMemoryStorage(r.store), App: r.app.clone()}
}

func (h *faultSimHarness) action(action faultSimAction) (faultActionReceipt, error) {
	h.step++
	receipt := faultActionReceipt{Index: len(h.actions), Step: h.step, Kind: action.Kind}
	err := h.execute(action, &receipt)
	if err != nil {
		receipt.Rejected = true
		receipt.Reason = err.Error()
	}
	h.actions = append(h.actions, action.clone())
	h.receipts = append(h.receipts, receipt)
	return receipt, err
}

func (h *faultSimHarness) execute(action faultSimAction, receipt *faultActionReceipt) error {
	switch action.Kind {
	case faultActionPropose:
		return h.executePropose(action, receipt)
	case faultActionConfChange:
		return h.executeConfChange(action, receipt)
	case faultActionPump:
		h.executePump(receipt)
		return nil
	case faultActionDeliver:
		return h.executeDeliver(action.Envelope, receipt)
	case faultActionDrop:
		return h.consumeEnvelope(action.Envelope, faultActionDrop, receipt)
	case faultActionDuplicate:
		return h.executeDuplicate(action, receipt)
	case faultActionHold:
		return h.transitionEnvelope(action.Envelope, faultEnvelopeQueued, faultEnvelopeHeld, faultActionHold, receipt)
	case faultActionRelease:
		return h.transitionEnvelope(action.Envelope, faultEnvelopeHeld, faultEnvelopeQueued, faultActionRelease, receipt)
	case faultActionCorrupt:
		return h.executeCorrupt(action, receipt)
	case faultActionPartition:
		return h.executePartition(action, true, receipt)
	case faultActionHeal:
		return h.executePartition(action, false, receipt)
	case faultActionCrash:
		return h.executeCrash(action, receipt)
	case faultActionSnapshot:
		if action.Name == "" {
			return errors.New("snapshot name is empty")
		}
		if h.replicas[action.Node] == nil {
			return fmt.Errorf("snapshot node %d does not exist", action.Node)
		}
		h.saveImage(action.Node, action.Name)
		receipt.Affected = 1
		return nil
	case faultActionStorageFailure:
		r := h.replicas[action.Node]
		if r == nil {
			return fmt.Errorf("storage node %d does not exist", action.Node)
		}
		r.store.FailWrites = action.Enabled
		h.saveImage(action.Node, "current")
		receipt.Affected = 1
		if action.Enabled {
			h.counters["storage-failure"]++
		}
		return nil
	case faultActionTick:
		return h.executeTick(action.Node, 1, receipt)
	case faultActionBurst:
		return h.executeTick(action.Node, action.Count, receipt)
	case faultActionPause:
		return h.executePause(action.Node, true, receipt)
	case faultActionResume:
		return h.executePause(action.Node, false, receipt)
	case faultActionSetClock:
		if h.replicas[action.Node] == nil {
			return fmt.Errorf("clock node %d does not exist", action.Node)
		}
		if action.Clock < h.clocks[action.Node] {
			h.counters["clock-rollback"]++
		}
		h.clocks[action.Node] = action.Clock
		receipt.Affected = 1
		return nil
	case faultActionProcessTOQ:
		r := h.replicas[action.Node]
		if r == nil {
			return fmt.Errorf("TOQ node %d does not exist", action.Node)
		}
		receipt.Affected = 1
		if err := r.node.ProcessTOQ(); err != nil {
			receipt.Rejected = true
			receipt.Reason = err.Error()
			if errors.Is(err, ErrTOQClockRollback) {
				h.counters["clock-rollback-rejected"]++
				return nil
			}
			return err
		}
		return nil
	case faultActionInjectMalformed:
		return h.executeMalformed(action, receipt)
	case faultActionNotApplicable:
		receipt.Affected = 1
		receipt.Detail = action.Reason
		h.counters["not-applicable"]++
		return nil
	case faultActionPrerequisiteMissing:
		receipt.Affected = 1
		receipt.Detail = action.Reason
		h.counters["prerequisite-missing"]++
		return nil
	default:
		return fmt.Errorf("unknown faultsim action %q", action.Kind)
	}
}

func (h *faultSimHarness) executePropose(action faultSimAction, receipt *faultActionReceipt) error {
	if action.Operation == nil {
		return errors.New("propose action lacks operation")
	}
	r := h.replicas[action.Node]
	if r == nil {
		return fmt.Errorf("proposer %d does not exist", action.Node)
	}
	if h.outstanding >= h.cfg.MaxOutstanding {
		receipt.Affected = 1
		receipt.Rejected = true
		receipt.Reason = errFaultSimBackpressure.Error()
		h.counters["overload"]++
		h.counters["backpressure"]++
		return nil
	}
	op := action.Operation.clone()
	if _, duplicate := h.historyByID[op.commandID()]; duplicate {
		return fmt.Errorf("duplicate client operation id %#v", op.commandID())
	}
	cmd, err := op.command()
	if err != nil {
		return err
	}
	ref, err := r.node.Propose(cmd)
	if err != nil {
		receipt.Affected = 1
		receipt.Rejected = true
		receipt.Reason = err.Error()
		return nil
	}
	history := faultHistoryOperation{Operation: op, Proposer: action.Node, Ref: ref, Invoke: h.step, Result: "unknown"}
	h.historyByID[op.commandID()] = len(h.history)
	h.history = append(h.history, history)
	h.outstanding++
	receipt.Affected = 1
	receipt.Detail = ref.String()
	return nil
}

func (h *faultSimHarness) executeConfChange(action faultSimAction, receipt *faultActionReceipt) error {
	if action.ConfChange == nil {
		return errors.New("conf-change action lacks change")
	}
	r := h.replicas[action.Node]
	if r == nil {
		return fmt.Errorf("configuration proposer %d does not exist", action.Node)
	}
	ref, err := r.node.ProposeConfChange(*action.ConfChange)
	if err != nil {
		receipt.Affected = 1
		receipt.Rejected = true
		receipt.Reason = err.Error()
		return nil
	}
	receipt.Affected = 1
	receipt.Detail = ref.String()
	h.counters["membership"]++
	return nil
}

func (h *faultSimHarness) executePump(receipt *faultActionReceipt) {
	var rejected []string
	for _, id := range h.ids {
		r := h.replicas[id]
		if r == nil || r.paused || !r.node.HasReady() {
			continue
		}
		rd := r.node.Ready()
		wires, err := h.encodeReadyMessages(rd.Messages)
		if err != nil {
			rejected = append(rejected, fmt.Sprintf("node %d encode: %v", id, err))
			receipt.Affected++
			continue
		}
		if h.inflightCount()+len(wires) > h.cfg.MaxEnvelopes {
			rejected = append(rejected, fmt.Sprintf("node %d envelope capacity: %v", id, errFaultSimBackpressure))
			h.counters["backpressure"]++
			receipt.Affected++
			continue
		}
		newCommitted := 0
		for _, item := range rd.Committed {
			if _, exists := r.app.Applied[item.Ref]; !exists {
				newCommitted++
			}
		}
		if newCommitted > h.cfg.MaxApplicationBatch {
			rejected = append(rejected, fmt.Sprintf("node %d application capacity: %v", id, errFaultSimBackpressure))
			h.counters["backpressure"]++
			receipt.Affected++
			continue
		}
		if err := r.store.ApplyReady(rd); err != nil {
			rejected = append(rejected, fmt.Sprintf("node %d storage: %v", id, err))
			h.counters["storage-rejection"]++
			receipt.Affected++
			continue
		}
		if err := provideRecordLoadsFromStore(r.node, r.store, rd); err != nil {
			rejected = append(rejected, fmt.Sprintf("node %d record load: %v", id, err))
			receipt.Affected++
			continue
		}
		events, err := r.app.apply(rd.Committed)
		if err != nil {
			rejected = append(rejected, fmt.Sprintf("node %d application: %v", id, err))
			receipt.Affected++
			continue
		}
		h.completeClientOperations(id, events)
		h.enqueueWires(id, rd.Messages, wires)
		if err := r.node.Advance(rd); err != nil {
			rejected = append(rejected, fmt.Sprintf("node %d advance: %v", id, err))
			receipt.Affected++
			continue
		}
		h.saveImage(id, "current")
		receipt.Affected++
	}
	if len(rejected) != 0 {
		receipt.Rejected = true
		receipt.Reason = strings.Join(rejected, "; ")
	}
}

func (h *faultSimHarness) encodeReadyMessages(messages []Message) ([][]byte, error) {
	wires := make([][]byte, len(messages))
	for i, message := range messages {
		wire, err := EncodeMessage(nil, message)
		if err != nil {
			return nil, err
		}
		wires[i] = wire
	}
	return wires, nil
}

func (h *faultSimHarness) enqueueWires(_ ReplicaID, messages []Message, wires [][]byte) {
	for i, message := range messages {
		h.nextEnvelope++
		h.envelopes[h.nextEnvelope] = faultEnvelope{
			ID:          h.nextEnvelope,
			From:        message.From,
			To:          message.To,
			CreatedStep: h.step,
			Attempt:     1,
			wire:        append([]byte(nil), wires[i]...),
		}
		h.envelopeState[h.nextEnvelope] = faultEnvelopeQueued
	}
}

func (h *faultSimHarness) completeClientOperations(id ReplicaID, events []faultApplicationEvent) {
	for _, event := range events {
		if event.Command.Kind != CommandUser {
			continue
		}
		index, ok := h.historyByID[event.Command.ID]
		if !ok {
			continue
		}
		history := &h.history[index]
		if history.Proposer != id || history.Complete != 0 {
			continue
		}
		history.Complete = h.step
		history.Result = "ok"
		history.Response = event.Response
		if h.outstanding > 0 {
			h.outstanding--
		}
	}
}

func (h *faultSimHarness) inflightCount() int {
	count := 0
	for _, state := range h.envelopeState {
		if state == faultEnvelopeQueued || state == faultEnvelopeHeld {
			count++
		}
	}
	return count
}

func (h *faultSimHarness) pendingEnvelopeIDs(includeHeld bool) []uint64 {
	ids := make([]uint64, 0, h.inflightCount())
	for id, state := range h.envelopeState {
		if state == faultEnvelopeQueued || (includeHeld && state == faultEnvelopeHeld) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (h *faultSimHarness) deliverableEnvelopeIDs() []uint64 {
	ids := h.pendingEnvelopeIDs(false)
	out := ids[:0]
	for _, id := range ids {
		envelope := h.envelopes[id]
		from := h.replicas[envelope.From]
		to := h.replicas[envelope.To]
		if h.partitions[[2]ReplicaID{envelope.From, envelope.To}] || from == nil || to == nil || from.paused || to.paused {
			continue
		}
		out = append(out, id)
	}
	return out
}

func (h *faultSimHarness) executeDeliver(id uint64, receipt *faultActionReceipt) error {
	envelope, ok := h.envelopes[id]
	if !ok || h.envelopeState[id] != faultEnvelopeQueued {
		return fmt.Errorf("envelope %d is not queued", id)
	}
	if h.partitions[[2]ReplicaID{envelope.From, envelope.To}] {
		receipt.Rejected = true
		receipt.Reason = fmt.Sprintf("directed partition %d->%d", envelope.From, envelope.To)
		return nil
	}
	from := h.replicas[envelope.From]
	to := h.replicas[envelope.To]
	if from == nil || to == nil {
		return fmt.Errorf("envelope %d endpoint is absent", id)
	}
	if from.paused || to.paused {
		receipt.Rejected = true
		receipt.Reason = "paused endpoint"
		return nil
	}
	pending := h.pendingEnvelopeIDs(false)
	if len(pending) > 0 && pending[0] != id {
		h.counters["reorder"]++
		receipt.Detail = fmt.Sprintf("delivered %d before %d", id, pending[0])
	}
	h.envelopeState[id] = faultEnvelopeConsumed
	receipt.Affected = 1
	h.counters["deliver"]++
	var message Message
	if err := DecodeMessage(envelope.wire, &message); err != nil {
		receipt.Rejected = true
		receipt.Reason = err.Error()
		h.counters["malformed"]++
		return nil
	}
	if err := to.node.Step(message); err != nil {
		receipt.Rejected = true
		receipt.Reason = err.Error()
		h.counters["delivery-rejected"]++
		return nil
	}
	return nil
}

func (h *faultSimHarness) consumeEnvelope(id uint64, counter string, receipt *faultActionReceipt) error {
	if _, ok := h.envelopes[id]; !ok || (h.envelopeState[id] != faultEnvelopeQueued && h.envelopeState[id] != faultEnvelopeHeld) {
		return fmt.Errorf("envelope %d is not pending", id)
	}
	h.envelopeState[id] = faultEnvelopeConsumed
	receipt.Affected = 1
	h.counters[counter]++
	return nil
}

func (h *faultSimHarness) transitionEnvelope(id uint64, from, to faultEnvelopeState, counter string, receipt *faultActionReceipt) error {
	if _, ok := h.envelopes[id]; !ok || h.envelopeState[id] != from {
		return fmt.Errorf("envelope %d state is %q, want %q", id, h.envelopeState[id], from)
	}
	h.envelopeState[id] = to
	receipt.Affected = 1
	h.counters[counter]++
	return nil
}

func (h *faultSimHarness) executeDuplicate(action faultSimAction, receipt *faultActionReceipt) error {
	envelope, ok := h.envelopes[action.Envelope]
	if !ok || h.envelopeState[action.Envelope] != faultEnvelopeQueued {
		return fmt.Errorf("envelope %d is not queued", action.Envelope)
	}
	count := action.Count
	if count <= 0 {
		count = 1
	}
	if h.inflightCount()+count > h.cfg.MaxEnvelopes {
		receipt.Affected = 1
		receipt.Rejected = true
		receipt.Reason = errFaultSimBackpressure.Error()
		h.counters["backpressure"]++
		return nil
	}
	for range count {
		h.nextEnvelope++
		copyEnvelope := envelope.clone()
		copyEnvelope.ID = h.nextEnvelope
		copyEnvelope.Parent = envelope.ID
		copyEnvelope.CreatedStep = h.step
		copyEnvelope.Attempt = envelope.Attempt + 1
		h.envelopes[copyEnvelope.ID] = copyEnvelope
		h.envelopeState[copyEnvelope.ID] = faultEnvelopeQueued
	}
	receipt.Affected = uint64(count)
	h.counters["duplicate"] += uint64(count)
	return nil
}

func (h *faultSimHarness) executeCorrupt(action faultSimAction, receipt *faultActionReceipt) error {
	envelope, ok := h.envelopes[action.Envelope]
	if !ok || h.envelopeState[action.Envelope] != faultEnvelopeQueued {
		return fmt.Errorf("envelope %d is not queued", action.Envelope)
	}
	wire := envelope.bytes()
	switch action.Mutation {
	case "", "flip-middle":
		if len(wire) == 0 {
			return errors.New("cannot flip empty envelope")
		}
		wire[len(wire)/2] ^= 0x80
	case "truncate":
		if len(wire) == 0 {
			return errors.New("cannot truncate empty envelope")
		}
		wire = wire[:len(wire)-1]
	default:
		return fmt.Errorf("unknown corruption mutation %q", action.Mutation)
	}
	h.envelopeState[action.Envelope] = faultEnvelopeConsumed
	h.nextEnvelope++
	corrupt := envelope.clone()
	corrupt.ID = h.nextEnvelope
	corrupt.Parent = envelope.ID
	corrupt.CreatedStep = h.step
	corrupt.Attempt = envelope.Attempt + 1
	corrupt.wire = wire
	h.envelopes[corrupt.ID] = corrupt
	h.envelopeState[corrupt.ID] = faultEnvelopeQueued
	receipt.Affected = 1
	receipt.Detail = strconv.FormatUint(corrupt.ID, 10)
	h.counters["corrupt"]++
	return nil
}

func (h *faultSimHarness) executePartition(action faultSimAction, enabled bool, receipt *faultActionReceipt) error {
	if action.From == action.To || h.replicas[action.From] == nil || h.replicas[action.To] == nil {
		return fmt.Errorf("invalid directed link %d->%d", action.From, action.To)
	}
	link := [2]ReplicaID{action.From, action.To}
	if enabled {
		h.partitions[link] = true
		h.counters["partition"]++
	} else {
		delete(h.partitions, link)
		h.counters["heal"]++
	}
	receipt.Affected = 1
	return nil
}

func (h *faultSimHarness) executePause(id ReplicaID, paused bool, receipt *faultActionReceipt) error {
	r := h.replicas[id]
	if r == nil {
		return fmt.Errorf("pause node %d does not exist", id)
	}
	r.paused = paused
	receipt.Affected = 1
	if paused {
		h.counters["pause"]++
	} else {
		h.counters["resume"]++
	}
	return nil
}

func (h *faultSimHarness) executeTick(id ReplicaID, count int, receipt *faultActionReceipt) error {
	r := h.replicas[id]
	if r == nil {
		return fmt.Errorf("tick node %d does not exist", id)
	}
	if r.paused {
		receipt.Rejected = true
		receipt.Reason = "node is paused"
		return nil
	}
	if count <= 0 {
		count = 1
	}
	for range count {
		if err := r.node.Tick(); err != nil {
			return err
		}
		receipt.Affected++
	}
	if count > 1 {
		h.counters["burst"] += uint64(count)
	} else {
		h.counters["tick"]++
	}
	return nil
}

func (h *faultSimHarness) executeMalformed(action faultSimAction, receipt *faultActionReceipt) error {
	if h.replicas[action.Node] == nil {
		return fmt.Errorf("malformed target %d does not exist", action.Node)
	}
	var message Message
	err := DecodeMessage(action.Data, &message)
	receipt.Affected = 1
	if err != nil {
		receipt.Rejected = true
		receipt.Reason = err.Error()
		h.counters["malformed"]++
		return nil
	}
	if err := h.replicas[action.Node].node.Step(message); err != nil {
		receipt.Rejected = true
		receipt.Reason = err.Error()
		h.counters["malformed"]++
		return nil
	}
	return errors.New("malformed injection was unexpectedly accepted")
}

func (h *faultSimHarness) executeCrash(action faultSimAction, receipt *faultActionReceipt) error {
	r := h.replicas[action.Node]
	if r == nil {
		return fmt.Errorf("crash node %d does not exist", action.Node)
	}
	imageName := action.Image
	if imageName == "" {
		imageName = "current"
	}
	if action.Image != "" {
		if _, ok := r.images[imageName]; !ok {
			return fmt.Errorf("durable image %q does not exist on node %d", imageName, action.Node)
		}
	}
	if action.Image == "" {
		switch action.Cut {
		case faultCrashBeforeReady:
		case faultCrashAfterFrozenReady:
			if r.node.HasReady() {
				_ = r.node.Ready()
			}
		case faultCrashAfterPersistence, faultCrashAfterApplication, faultCrashAfterAdvance, faultCrashAfterExecutedPersistence:
			if !r.node.HasReady() {
				return fmt.Errorf("crash cut %q requires Ready work", action.Cut)
			}
			rd := r.node.Ready()
			wires, err := h.encodeReadyMessages(rd.Messages)
			if err != nil {
				return err
			}
			if h.inflightCount()+len(wires) > h.cfg.MaxEnvelopes {
				receipt.Affected = 1
				receipt.Rejected = true
				receipt.Reason = errFaultSimBackpressure.Error()
				h.counters["backpressure"]++
				return nil
			}
			if err := r.store.ApplyReady(rd); err != nil {
				receipt.Affected = 1
				receipt.Rejected = true
				receipt.Reason = err.Error()
				h.counters["storage-rejection"]++
				return nil
			}
			h.saveImage(action.Node, "current")
			if action.Cut == faultCrashAfterPersistence {
				break
			}
			if err := provideRecordLoadsFromStore(r.node, r.store, rd); err != nil {
				return err
			}
			events, err := r.app.apply(rd.Committed)
			if err != nil {
				return err
			}
			h.completeClientOperations(action.Node, events)
			h.saveImage(action.Node, "current")
			if action.Cut == faultCrashAfterApplication {
				break
			}
			h.enqueueWires(action.Node, rd.Messages, wires)
			if err := r.node.Advance(rd); err != nil {
				return err
			}
			if action.Cut == faultCrashAfterAdvance {
				break
			}
			if r.node.HasReady() {
				executed := r.node.Ready()
				executedWires, err := h.encodeReadyMessages(executed.Messages)
				if err != nil {
					return err
				}
				if h.inflightCount()+len(executedWires) > h.cfg.MaxEnvelopes {
					return errFaultSimBackpressure
				}
				if err := r.store.ApplyReady(executed); err != nil {
					return err
				}
				if err := provideRecordLoadsFromStore(r.node, r.store, executed); err != nil {
					return err
				}
				events, err := r.app.apply(executed.Committed)
				if err != nil {
					return err
				}
				h.completeClientOperations(action.Node, events)
				h.enqueueWires(action.Node, executed.Messages, executedWires)
				h.saveImage(action.Node, "current")
			}
		case "":
			return errors.New("crash cut is empty")
		default:
			return fmt.Errorf("unknown crash cut %q", action.Cut)
		}
	}
	image, ok := r.images[imageName]
	if !ok {
		return fmt.Errorf("current durable image missing on node %d", action.Node)
	}
	store := faultCloneMemoryStorage(image.Store)
	app := image.App.clone()
	r.store = store
	r.app = app
	node, err := h.newRawNode(action.Node, store)
	if err != nil {
		return fmt.Errorf("restart node %d from %q: %w", action.Node, imageName, err)
	}
	r.node = node
	r.paused = false
	h.saveImage(action.Node, "current")
	receipt.Affected = 1
	receipt.Detail = string(action.Cut)
	h.counters["crash"]++
	return nil
}

func (h *faultSimHarness) refFor(id CommandID) (InstanceRef, bool) {
	index, ok := h.historyByID[id]
	if !ok {
		return InstanceRef{}, false
	}
	return h.history[index].Ref, true
}

func (h *faultSimHarness) appliedOn(id ReplicaID, refs []InstanceRef) bool {
	r := h.replicas[id]
	if r == nil {
		return false
	}
	for _, ref := range refs {
		if _, ok := r.app.Applied[ref]; !ok {
			return false
		}
	}
	return true
}

func (h *faultSimHarness) driveUntilApplied(refs []InstanceRef, nodes []ReplicaID, bound int) (int, error) {
	if bound <= 0 {
		return 0, errors.New("logical-step bound must be positive")
	}
	for round := 1; round <= bound; round++ {
		if h.cfg.TOQ {
			for _, id := range h.ids {
				if h.replicas[id].paused {
					continue
				}
				if _, err := h.action(faultSimAction{Kind: faultActionProcessTOQ, Node: id}); err != nil {
					return round, err
				}
			}
		}
		if _, err := h.action(faultSimAction{Kind: faultActionPump}); err != nil {
			return round, err
		}
		for _, envelopeID := range h.deliverableEnvelopeIDs() {
			if _, err := h.action(faultSimAction{Kind: faultActionDeliver, Envelope: envelopeID}); err != nil {
				return round, err
			}
		}
		if _, err := h.action(faultSimAction{Kind: faultActionPump}); err != nil {
			return round, err
		}
		complete := true
		for _, id := range nodes {
			if !h.appliedOn(id, refs) {
				complete = false
				break
			}
		}
		if complete {
			h.lastProgressBound = round
			return round, nil
		}
		for _, id := range h.ids {
			if h.replicas[id].paused {
				continue
			}
			if _, err := h.action(faultSimAction{Kind: faultActionTick, Node: id}); err != nil {
				return round, err
			}
		}
	}
	return bound, fmt.Errorf("refs %v did not apply on nodes %v within %d logical steps", refs, nodes, bound)
}

func (h *faultSimHarness) recordFailClosed(name string, nodes []ReplicaID, refs []InstanceRef) bool {
	ok := true
	for _, id := range nodes {
		for _, ref := range refs {
			if h.appliedOn(id, []InstanceRef{ref}) {
				ok = false
			}
		}
	}
	h.failClosed = append(h.failClosed, faultFailClosedCheck{Name: name, OK: ok})
	return ok
}

func faultSameCommand(a, b Command) bool {
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

type faultTerminalRecord struct {
	Node    ReplicaID
	Hard    HardState
	Config  []ConfState
	Records []InstanceRecord
	State   []faultKV
	Log     []faultApplicationEvent
}

type faultTerminalEnvelope struct {
	ID          uint64
	Parent      uint64
	From        ReplicaID
	To          ReplicaID
	CreatedStep uint64
	Attempt     uint64
	State       faultEnvelopeState
	WireHash    string
}

type faultTerminalState struct {
	Replicas   []faultTerminalRecord
	Envelopes  []faultTerminalEnvelope
	Partitions [][2]ReplicaID
	Clocks     []faultKV
	History    []faultHistoryOperation
}

func (h *faultSimHarness) terminalHash() string {
	terminal := faultTerminalState{}
	for _, id := range h.ids {
		r := h.replicas[id]
		record := faultTerminalRecord{Node: id, Hard: r.store.Hard.Clone()}
		record.Config = make([]ConfState, len(r.store.Configs))
		for i, conf := range r.store.Configs {
			record.Config[i] = conf.Clone()
		}
		refs := make([]InstanceRef, 0, len(r.store.Records))
		for ref := range r.store.Records {
			refs = append(refs, ref)
		}
		sortRefs(refs)
		for _, ref := range refs {
			record.Records = append(record.Records, r.store.Records[ref].Clone())
		}
		keys := make([]string, 0, len(r.app.State))
		for key := range r.app.State {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			record.State = append(record.State, faultKV{Key: key, Value: r.app.State[key]})
		}
		for _, event := range r.app.Log {
			record.Log = append(record.Log, faultCloneApplicationEvent(event))
		}
		terminal.Replicas = append(terminal.Replicas, record)
	}
	envelopeIDs := make([]uint64, 0, len(h.envelopes))
	for id := range h.envelopes {
		envelopeIDs = append(envelopeIDs, id)
	}
	sort.Slice(envelopeIDs, func(i, j int) bool { return envelopeIDs[i] < envelopeIDs[j] })
	for _, id := range envelopeIDs {
		envelope := h.envelopes[id]
		sum := sha256.Sum256(envelope.wire)
		terminal.Envelopes = append(terminal.Envelopes, faultTerminalEnvelope{
			ID: envelope.ID, Parent: envelope.Parent, From: envelope.From, To: envelope.To,
			CreatedStep: envelope.CreatedStep, Attempt: envelope.Attempt,
			State: h.envelopeState[id], WireHash: hex.EncodeToString(sum[:]),
		})
	}
	for link := range h.partitions {
		terminal.Partitions = append(terminal.Partitions, link)
	}
	sort.Slice(terminal.Partitions, func(i, j int) bool {
		if terminal.Partitions[i][0] != terminal.Partitions[j][0] {
			return terminal.Partitions[i][0] < terminal.Partitions[j][0]
		}
		return terminal.Partitions[i][1] < terminal.Partitions[j][1]
	})
	for _, id := range h.ids {
		terminal.Clocks = append(terminal.Clocks, faultKV{Key: strconv.FormatUint(uint64(id), 10), Value: strconv.FormatUint(h.clocks[id], 10)})
	}
	terminal.History = append([]faultHistoryOperation(nil), h.history...)
	data, err := json.Marshal(terminal)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
