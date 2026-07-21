package sim

import (
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gosuda.org/moreconsensus/epaxos"
)

const traceLimitMessage = "This trace is too large. Reset the lab to continue."

var keyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,15}$`)

type envelope struct {
	id      uint64
	message epaxos.Message
}

type commandEntry struct {
	id    epaxos.CommandID
	view  CommandView
	cycle []byte
}

type application struct {
	state   map[string]string
	applied []AppliedView
	seen    map[epaxos.CommandID]bool
}

type pathEvidence struct {
	slow     bool
	recovery bool
}

type machine struct {
	size       int
	voters     []epaxos.ReplicaID
	fastQuorum int
	slowQuorum int
	nodes      []*epaxos.RawNode
	stores     []*epaxos.MemoryStorage
	apps       []application
	paused     []bool
	delayed    map[[2]epaxos.ReplicaID]bool
	queue      []envelope
	nextID     uint64
	commands   []commandEntry
	paths      map[string]pathEvidence
}

// Session owns one deterministic, branchable simulator history.
type Session struct {
	size    int
	machine *machine
	history []Action
	cursor  int
	frame   Frame
}

// NewSession creates a bounded EPaxos simulation for three or five replicas.
// A size of zero selects the default three-replica lab.
func NewSession(size int) (*Session, error) {
	if size == 0 {
		size = 3
	}
	if size != 3 && size != 5 {
		return nil, actionError(CodeInvalidAction, "Choose a 3- or 5-replica cluster.")
	}
	m, err := newMachine(size)
	if err != nil {
		return nil, internalError("The EPaxos simulation could not start.", err)
	}
	s := &Session{size: size, machine: m}
	s.frame = m.frame(0, Action{}, nil)
	return s, nil
}

func newMachine(size int) (*machine, error) {
	fast, err := epaxos.FastQuorum(size)
	if err != nil {
		return nil, err
	}
	slow, err := epaxos.SlowQuorum(size)
	if err != nil {
		return nil, err
	}
	m := &machine{
		size:       size,
		fastQuorum: fast,
		slowQuorum: slow,
		voters:     make([]epaxos.ReplicaID, size),
		nodes:      make([]*epaxos.RawNode, size+1),
		stores:     make([]*epaxos.MemoryStorage, size+1),
		apps:       make([]application, size+1),
		paused:     make([]bool, size+1),
		delayed:    make(map[[2]epaxos.ReplicaID]bool),
		paths:      make(map[string]pathEvidence),
	}
	for i := range size {
		m.voters[i] = epaxos.ReplicaID(i + 1)
	}
	for i := 1; i <= size; i++ {
		store := epaxos.NewMemoryStorage()
		node, nodeErr := epaxos.NewRawNode(epaxos.Config{
			ID:                    epaxos.ReplicaID(i),
			Voters:                m.voters,
			Storage:               store,
			RetryTicks:            2,
			RecoveryTicks:         5,
			RetainExecutedPerLane: 64,
			MaxResidentInstances:  256,
			MaxReadyMessages:      64,
		})
		if nodeErr != nil {
			return nil, nodeErr
		}
		m.nodes[i] = node
		m.stores[i] = store
		m.apps[i] = application{state: make(map[string]string), seen: make(map[epaxos.CommandID]bool)}
	}
	var discarded []Event
	if err := m.drainReady(&discarded); err != nil {
		return nil, err
	}
	if len(m.queue) != 0 {
		return nil, errors.New("genesis emitted transport messages")
	}
	return m, nil
}

// Dispatch validates and applies one action. Invalid and blocked actions do not
// alter history. A failure after mutation reconstructs the last valid prefix.
func (s *Session) Dispatch(action Action) (frame Frame, err error) {
	if s == nil || s.machine == nil {
		return Frame{}, internalError("The EPaxos simulation is unavailable.", nil)
	}
	normalized, validationErr := s.machine.validate(action)
	if validationErr != nil {
		return Frame{}, validationErr
	}
	if s.cursor >= maxHistoryActions {
		return Frame{}, actionError(CodeLimit, traceLimitMessage)
	}
	prefix := s.cursor
	defer func() {
		if recovered := recover(); recovered != nil {
			rebuildErr := s.rebuild(prefix)
			cause := fmt.Errorf("panic: %v", recovered)
			if rebuildErr != nil {
				cause = errors.Join(cause, rebuildErr)
			}
			frame = Frame{}
			err = internalError("The EPaxos core rejected this transition. The last valid frame was restored.", cause)
		}
	}()

	frame, executionErr := s.machine.execute(normalized, prefix+1)
	if executionErr != nil {
		rebuildErr := s.rebuild(prefix)
		if rebuildErr != nil {
			return Frame{}, internalError("The EPaxos simulation could not restore its last valid frame.", errors.Join(executionErr, rebuildErr))
		}
		var coded interface{ Code() string }
		if errors.As(executionErr, &coded) && coded.Code() == CodeLimit {
			return Frame{}, executionErr
		}
		return Frame{}, internalError("The EPaxos core rejected this transition. The last valid frame was restored.", executionErr)
	}

	s.history = append(s.history[:prefix], normalized)
	s.cursor = prefix + 1
	s.frame = frame
	return frame, nil
}

// Seek reconstructs the session by replaying the recorded action prefix.
func (s *Session) Seek(index int) (Frame, error) {
	if s == nil || s.machine == nil {
		return Frame{}, internalError("The EPaxos simulation is unavailable.", nil)
	}
	if index < 0 || index > len(s.history) {
		return Frame{}, actionError(CodeInvalidAction, "That history frame does not exist.")
	}
	if err := s.rebuild(index); err != nil {
		return Frame{}, internalError("The EPaxos simulation could not replay that frame.", err)
	}
	return s.frame, nil
}

func (s *Session) rebuild(index int) error {
	m, err := newMachine(s.size)
	if err != nil {
		return err
	}
	frame := m.frame(0, Action{}, nil)
	for i := range index {
		frame, err = m.execute(s.history[i], i+1)
		if err != nil {
			return fmt.Errorf("replay action %d: %w", i+1, err)
		}
	}
	s.machine = m
	s.cursor = index
	s.frame = frame
	return nil
}

func (m *machine) validate(action Action) (Action, error) {
	invalidFields := func(replica, peer, envelope, key, value bool) bool {
		return (!replica && action.Replica != 0) || (!peer && action.Peer != 0) ||
			(!envelope && action.Envelope != "") || (!key && action.Key != "") || (!value && action.Value != "")
	}
	validReplica := func(id uint64) bool { return id >= 1 && id <= uint64(m.size) } //nolint:gosec // Session sizes are restricted to 3 or 5.

	switch action.Kind {
	case "propose":
		if invalidFields(true, false, false, true, true) || !validReplica(action.Replica) || action.Key == "" || action.Value == "" {
			return Action{}, actionError(CodeInvalidAction, "Choose a replica and enter a valid SET command.")
		}
		if m.paused[action.Replica] {
			return Action{}, actionError(CodeBlocked, "That replica is paused.")
		}
		if len(m.commands) >= maxProposals {
			return Action{}, actionError(CodeLimit, "This lab is full. Reset it to start a new trace.")
		}
		if !keyPattern.MatchString(action.Key) {
			return Action{}, actionError(CodeInvalidAction, "Keys must start with a lowercase letter and use at most 16 lowercase letters, digits, underscores, or hyphens.")
		}
		action.Value = strings.TrimSpace(action.Value)
		if len(action.Value) == 0 || len(action.Value) > 16 {
			return Action{}, actionError(CodeInvalidAction, "Values must contain 1 to 16 printable ASCII characters.")
		}
		for i := range len(action.Value) {
			if action.Value[i] < 0x20 || action.Value[i] > 0x7e {
				return Action{}, actionError(CodeInvalidAction, "Values must contain 1 to 16 printable ASCII characters.")
			}
		}
	case "deliver-next":
		if invalidFields(false, false, false, false, false) {
			return Action{}, actionError(CodeInvalidAction, "Deliver next does not accept additional fields.")
		}
		if _, ok := m.nextDeliverable(); !ok {
			return Action{}, actionError(CodeBlocked, "No queued message can be delivered.")
		}
	case "deliver":
		if invalidFields(false, false, true, false, false) || action.Envelope == "" {
			return Action{}, actionError(CodeInvalidAction, "Choose a queued message to deliver.")
		}
		index, parseErr := m.envelopeIndex(action.Envelope)
		if parseErr != nil {
			return Action{}, parseErr
		}
		if !m.deliverable(m.queue[index]) {
			return Action{}, actionError(CodeBlocked, "That message is blocked by a paused replica or delayed link.")
		}
	case "drop":
		if invalidFields(false, false, true, false, false) || action.Envelope == "" {
			return Action{}, actionError(CodeInvalidAction, "Choose a queued message to drop.")
		}
		if _, parseErr := m.envelopeIndex(action.Envelope); parseErr != nil {
			return Action{}, parseErr
		}
	case "pause":
		if invalidFields(true, false, false, false, false) || !validReplica(action.Replica) {
			return Action{}, actionError(CodeInvalidAction, "Choose a known replica to pause.")
		}
		if m.paused[action.Replica] {
			return Action{}, actionError(CodeInvalidAction, "That replica is already paused.")
		}
	case "resume":
		if invalidFields(true, false, false, false, false) || !validReplica(action.Replica) {
			return Action{}, actionError(CodeInvalidAction, "Choose a known replica to resume.")
		}
		if !m.paused[action.Replica] {
			return Action{}, actionError(CodeInvalidAction, "That replica is already running.")
		}
	case "delay-link", "heal-link":
		if invalidFields(true, true, false, false, false) || !validReplica(action.Replica) || !validReplica(action.Peer) || action.Replica == action.Peer {
			return Action{}, actionError(CodeInvalidAction, "Choose two different known replicas.")
		}
		key := linkKey(epaxos.ReplicaID(action.Replica), epaxos.ReplicaID(action.Peer))
		delayed := m.delayed[key]
		if action.Kind == "delay-link" && delayed {
			return Action{}, actionError(CodeInvalidAction, "That link is already delayed.")
		}
		if action.Kind == "heal-link" && !delayed {
			return Action{}, actionError(CodeInvalidAction, "That link is already healthy.")
		}
	case "tick":
		if invalidFields(true, false, false, false, false) || action.Replica > uint64(m.size) { //nolint:gosec // Session sizes are restricted to 3 or 5.
			return Action{}, actionError(CodeInvalidAction, "Choose a known replica to tick.")
		}
		if action.Replica != 0 && m.paused[action.Replica] {
			return Action{}, actionError(CodeBlocked, "A paused replica cannot tick.")
		}
		if action.Replica == 0 {
			active := false
			for id := 1; id <= m.size; id++ {
				active = active || !m.paused[id]
			}
			if !active {
				return Action{}, actionError(CodeBlocked, "Every replica is paused.")
			}
		}
	default:
		return Action{}, actionError(CodeInvalidAction, "That simulator action is not supported.")
	}
	return action, nil
}

func (m *machine) execute(action Action, index int) (Frame, error) {
	events := make([]Event, 0, 8)
	switch action.Kind {
	case "propose":
		if err := m.propose(action, &events); err != nil {
			return Frame{}, err
		}
	case "deliver-next":
		envelopeIndex, _ := m.nextDeliverable()
		if err := m.deliver(envelopeIndex, &events); err != nil {
			return Frame{}, err
		}
	case "deliver":
		envelopeIndex, _ := m.envelopeIndex(action.Envelope)
		if err := m.deliver(envelopeIndex, &events); err != nil {
			return Frame{}, err
		}
	case "drop":
		envelopeIndex, _ := m.envelopeIndex(action.Envelope)
		dropped := m.queue[envelopeIndex]
		view := m.messageView(dropped)
		m.queue = append(m.queue[:envelopeIndex], m.queue[envelopeIndex+1:]...)
		events = append(events, Event{Kind: "dropped", Message: &view, Detail: fmt.Sprintf("Dropped %s from R%d to R%d.", view.Type, view.From, view.To)})
	case "pause":
		m.paused[action.Replica] = true
		events = append(events, Event{Kind: "paused", Replica: action.Replica, Detail: fmt.Sprintf("R%d paused.", action.Replica)})
	case "resume":
		m.paused[action.Replica] = false
		events = append(events, Event{Kind: "resumed", Replica: action.Replica, Detail: fmt.Sprintf("R%d resumed.", action.Replica)})
	case "delay-link", "heal-link":
		key := linkKey(epaxos.ReplicaID(action.Replica), epaxos.ReplicaID(action.Peer))
		delayed := action.Kind == "delay-link"
		m.delayed[key] = delayed
		if !delayed {
			delete(m.delayed, key)
		}
		kind := "link-delayed"
		verb := "Delayed"
		if !delayed {
			kind = "link-healed"
			verb = "Healed"
		}
		events = append(events, Event{Kind: kind, Detail: fmt.Sprintf("%s the R%d ↔ R%d link.", verb, key[0], key[1])})
	case "tick":
		if action.Replica == 0 {
			for id := 1; id <= m.size; id++ {
				if m.paused[id] {
					continue
				}
				if err := m.nodes[id].Tick(); err != nil {
					return Frame{}, err
				}
				events = append(events, Event{Kind: "ticked", Replica: uint64(id), Detail: fmt.Sprintf("Advanced R%d by one logical tick.", id)}) //nolint:gosec // Replica indices are restricted to 1 through 5.
			}
		} else {
			id := int(action.Replica) //nolint:gosec // Validation restricts the replica to 1 through 5.
			if err := m.nodes[id].Tick(); err != nil {
				return Frame{}, err
			}
			events = append(events, Event{Kind: "ticked", Replica: action.Replica, Detail: fmt.Sprintf("Advanced R%d by one logical tick.", action.Replica)})
		}
		if err := m.drainReady(&events); err != nil {
			return Frame{}, err
		}
	}
	if len(m.queue) > maxQueuedMessages {
		return Frame{}, actionError(CodeLimit, traceLimitMessage)
	}
	return m.frame(index, action, events), nil
}

func (m *machine) propose(action Action, events *[]Event) error {
	sequence := uint64(len(m.commands) + 1)
	id := epaxos.CommandID{Client: 1, Sequence: sequence}
	cycle := make([]byte, 8)
	binary.BigEndian.PutUint64(cycle, sequence)
	command := epaxos.Command{
		ID:        id,
		Payload:   []byte(fmt.Sprintf("SET %s=%s", action.Key, action.Value)),
		Footprint: epaxos.Footprint{Points: [][]byte{[]byte(action.Key)}},
		CycleKey:  cycle,
	}
	entry := commandEntry{
		id:    id,
		view:  CommandView{ID: commandIDString(id), Key: action.Key, Value: action.Value, Order: sequence},
		cycle: append([]byte(nil), cycle...),
	}
	m.commands = append(m.commands, entry)
	ref, err := m.nodes[action.Replica].Propose(command)
	if err != nil {
		return err
	}
	m.commands[len(m.commands)-1].view.Ref = ref.String()
	view := m.commands[len(m.commands)-1].view
	*events = append(*events, Event{Kind: "proposed", Replica: action.Replica, Command: &view, Detail: fmt.Sprintf("R%d proposed SET %s = %s as %s.", action.Replica, action.Key, action.Value, ref)})
	return m.drainReady(events)
}

func (m *machine) deliver(index int, events *[]Event) error {
	delivered := m.queue[index]
	view := m.messageView(delivered)
	m.queue = append(m.queue[:index], m.queue[index+1:]...)
	*events = append(*events, Event{Kind: "delivered", Replica: view.To, Message: &view, Detail: fmt.Sprintf("Delivered %s from R%d to R%d.", view.Type, view.From, view.To)})
	err := m.nodes[view.To].Step(delivered.message)
	if errors.Is(err, epaxos.ErrMessageRejected) {
		*events = append(*events, Event{Kind: "ignored", Replica: view.To, Message: &view, Detail: "The stale or duplicate packet was safely ignored."})
		return nil
	}
	if err != nil {
		return err
	}
	return m.drainReady(events)
}

func (m *machine) drainReady(events *[]Event) error {
	for round := 0; round < 4096; round++ {
		progress := false
		for id := 1; id <= m.size; id++ {
			if m.paused[id] || !m.nodes[id].HasReady() {
				continue
			}
			progress = true
			ready := m.nodes[id].Ready()
			if err := rejectUnsupportedReady(ready); err != nil {
				return err
			}
			if err := m.stores[id].ApplyReady(ready); err != nil {
				return err
			}
			for i := range ready.Records {
				view := m.instanceView(ready.Records[i])
				*events = append(*events, Event{Kind: "persisted", Replica: uint64(id), Record: &view, Detail: fmt.Sprintf("R%d persisted %s as %s.", id, view.Ref, view.Status)})
			}
			for i := range ready.Messages {
				m.nextID++
				env := envelope{id: m.nextID, message: ready.Messages[i].Clone()}
				m.queue = append(m.queue, env)
				m.observeMessage(env.message)
				view := m.messageView(env)
				*events = append(*events, Event{Kind: "sent", Replica: uint64(id), Message: &view, Detail: fmt.Sprintf("R%d queued %s for R%d.", id, view.Type, view.To)})
				if len(m.queue) > maxQueuedMessages {
					return actionError(CodeLimit, traceLimitMessage)
				}
			}
			for i := range ready.Apply {
				if err := m.apply(id, ready.Apply[i], events); err != nil {
					return err
				}
			}
			if err := m.nodes[id].Advance(ready); err != nil {
				return err
			}
		}
		if !progress {
			return nil
		}
	}
	return errors.New("local Ready processing did not quiesce")
}

func rejectUnsupportedReady(ready epaxos.Ready) error {
	if len(ready.BootstrapRecords) != 0 || ready.LocalVoterState != nil || len(ready.BootstrapMessages) != 0 {
		return errors.New("bounded simulator received bootstrap work")
	}
	if ready.Snapshot != nil {
		return errors.New("bounded simulator received snapshot work")
	}
	if ready.Checkpoint != nil {
		return errors.New("bounded simulator received checkpoint work")
	}
	if len(ready.Compact) != 0 || len(ready.FrontierUpdates) != 0 {
		return errors.New("bounded simulator received compaction work")
	}
	if len(ready.RecordLoads) != 0 {
		return errors.New("bounded simulator received record-load work")
	}
	return nil
}

func (m *machine) apply(replica int, applied epaxos.ApplyCommand, events *[]Event) error {
	entry, ok := m.command(applied.Command.ID)
	if !ok {
		return fmt.Errorf("application command %s is not registered", commandIDString(applied.Command.ID))
	}
	app := &m.apps[replica]
	if app.seen[applied.Command.ID] {
		return fmt.Errorf("application command %s executed twice on replica %d", commandIDString(applied.Command.ID), replica)
	}
	app.seen[applied.Command.ID] = true
	app.state[entry.view.Key] = entry.view.Value
	view := AppliedView{Ref: applied.Ref.String(), Command: entry.view.ID, Key: entry.view.Key, Value: entry.view.Value, Order: entry.view.Order}
	app.applied = append(app.applied, view)
	command := entry.view
	*events = append(*events, Event{Kind: "applied", Replica: uint64(replica), Command: &command, Detail: fmt.Sprintf("R%d applied SET %s = %s.", replica, command.Key, command.Value)}) //nolint:gosec // Replica indices are restricted to 1 through 5.
	return nil
}

func (m *machine) command(id epaxos.CommandID) (commandEntry, bool) {
	if id.Client != 1 || id.Sequence == 0 || id.Sequence > uint64(len(m.commands)) {
		return commandEntry{}, false
	}
	entry := m.commands[id.Sequence-1]
	return entry, entry.id == id
}

func (m *machine) envelopeIndex(value string) (int, error) {
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != value {
		return 0, actionError(CodeInvalidAction, "That message envelope does not exist.")
	}
	index := sort.Search(len(m.queue), func(i int) bool { return m.queue[i].id >= id })
	if index == len(m.queue) || m.queue[index].id != id {
		return 0, actionError(CodeInvalidAction, "That message envelope does not exist.")
	}
	return index, nil
}

func (m *machine) nextDeliverable() (int, bool) {
	for i := range m.queue {
		if m.deliverable(m.queue[i]) {
			return i, true
		}
	}
	return 0, false
}

func (m *machine) deliverable(env envelope) bool {
	message := env.message
	return !m.paused[message.From] && !m.paused[message.To] && !m.delayed[linkKey(message.From, message.To)]
}

func linkKey(left, right epaxos.ReplicaID) [2]epaxos.ReplicaID {
	if left > right {
		left, right = right, left
	}
	return [2]epaxos.ReplicaID{left, right}
}

func (m *machine) observeMessage(message epaxos.Message) {
	ref := message.Ref.String()
	evidence := m.paths[ref]
	switch message.Type {
	case epaxos.MsgAccept, epaxos.MsgAcceptResp:
		evidence.slow = true
	case epaxos.MsgPrepare, epaxos.MsgPrepareResp, epaxos.MsgTryPreAccept, epaxos.MsgTryPreAcceptResp:
		evidence.recovery = true
	case epaxos.MsgPreAccept, epaxos.MsgPreAcceptResp, epaxos.MsgCommit, epaxos.MsgEvidence, epaxos.MsgEvidenceResp,
		epaxos.MsgCheckpointVote, epaxos.MsgCheckpointCertificate, epaxos.MsgCheckpointOffer:
		// These messages do not change path classification.
	}
	m.paths[ref] = evidence
}

func (m *machine) frame(index int, cause Action, events []Event) Frame {
	if events == nil {
		events = []Event{}
	}
	return Frame{Index: index, Cause: cause, Events: events, Snapshot: m.snapshot()}
}

func (m *machine) snapshot() Snapshot {
	snapshot := Snapshot{
		Cluster:  ClusterView{Size: m.size, FastQuorum: m.fastQuorum, SlowQuorum: m.slowQuorum},
		Replicas: make([]ReplicaView, 0, m.size),
		Messages: make([]MessageView, 0, len(m.queue)),
		Links:    make([]LinkView, 0, m.size*(m.size-1)/2),
		Commands: make([]CommandView, len(m.commands)),
	}
	for id := 1; id <= m.size; id++ {
		status := m.nodes[id].Status()
		instances := make([]InstanceView, len(status.Instances))
		for i := range status.Instances {
			instances[i] = m.instanceView(status.Instances[i])
		}
		sort.Slice(instances, func(i, j int) bool { return instances[i].Ref < instances[j].Ref })
		state := make([]StateView, 0, len(m.apps[id].state))
		for key, value := range m.apps[id].state {
			state = append(state, StateView{Key: key, Value: value})
		}
		sort.Slice(state, func(i, j int) bool { return state[i].Key < state[j].Key })
		applied := append([]AppliedView(nil), m.apps[id].applied...)
		if applied == nil {
			applied = []AppliedView{}
		}
		snapshot.Replicas = append(snapshot.Replicas, ReplicaView{
			ID:        uint64(id),
			Paused:    m.paused[id],
			Tick:      status.Tick,
			Instances: instances,
			Applied:   applied,
			State:     state,
		})
	}
	for i := range m.queue {
		snapshot.Messages = append(snapshot.Messages, m.messageView(m.queue[i]))
	}
	for left := 1; left <= m.size; left++ {
		for right := left + 1; right <= m.size; right++ {
			key := linkKey(epaxos.ReplicaID(left), epaxos.ReplicaID(right))
			snapshot.Links = append(snapshot.Links, LinkView{From: uint64(left), To: uint64(right), Delayed: m.delayed[key]})
		}
	}
	for i := range m.commands {
		snapshot.Commands[i] = m.commands[i].view
	}
	return snapshot
}

func (m *machine) instanceView(record epaxos.InstanceRecord) InstanceView {
	view := InstanceView{
		Ref:       record.Ref.String(),
		Status:    record.Status.String(),
		Seq:       record.Seq,
		DepVector: make([]uint64, len(record.Deps)),
		Edges:     m.expandDeps(record.Ref.Conf, record.Deps),
		Ballot:    BallotView{Epoch: record.Ballot.Epoch, Number: record.Ballot.Number, Replica: uint64(record.Ballot.Replica)},
		Path:      "PENDING",
	}
	for i := range record.Deps {
		view.DepVector[i] = uint64(record.Deps[i])
	}
	if entry, ok := m.command(record.Command.ID); ok {
		command := entry.view
		view.Command = &command
		view.Order = command.Order
	}
	evidence := m.paths[view.Ref]
	switch {
	case evidence.recovery:
		view.Path = "RECOVERY"
	case evidence.slow:
		view.Path = "SLOW"
	case record.Status == epaxos.StatusCommitted || record.Status == epaxos.StatusExecuted:
		view.Path = "FAST"
	}
	return view
}

func (m *machine) messageView(env envelope) MessageView {
	message := env.message
	return MessageView{
		ID:      strconv.FormatUint(env.id, 10),
		Type:    message.Type.String(),
		From:    uint64(message.From),
		To:      uint64(message.To),
		Ref:     message.Ref.String(),
		Seq:     message.Seq,
		Deps:    m.expandDeps(message.Ref.Conf, message.Deps),
		Blocked: !m.deliverable(env),
	}
}

func (m *machine) expandDeps(conf epaxos.ConfID, deps []epaxos.InstanceNum) []string {
	edges := make([]string, 0, len(deps))
	for i, instance := range deps {
		if instance == 0 || i >= len(m.voters) {
			continue
		}
		edges = append(edges, epaxos.InstanceRef{Replica: m.voters[i], Instance: instance, Conf: conf}.String())
	}
	return edges
}

func commandIDString(id epaxos.CommandID) string {
	return fmt.Sprintf("%d:%d", id.Client, id.Sequence)
}
