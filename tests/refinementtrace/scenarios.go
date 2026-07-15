package refinementtrace

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"gosuda.org/moreconsensus/epaxos"
)

type semanticTrace struct {
	id     string
	events []semanticEvent
}

type traceBuilder struct {
	id     string
	events []semanticEvent
}

func (b *traceBuilder) add(action string, event semanticEvent) error {
	if _, ok := actionAllowlist[action]; !ok {
		return fmt.Errorf("unmapped state-changing transition %q", action)
	}
	event.Schema = semanticSchema
	event.Sequence = len(b.events) + 1
	event.Action = action
	if event.Kind == "" {
		return fmt.Errorf("action %s has empty event kind", action)
	}
	if event.Result.Class == "" {
		event.Result = classifyError(nil)
	}
	b.events = append(b.events, event)
	return nil
}

func (b *traceBuilder) finish() semanticTrace {
	return semanticTrace{id: b.id, events: b.events}
}

type nodeHarness struct {
	id    epaxos.ReplicaID
	node  *epaxos.RawNode
	store *epaxos.MemoryStorage
	apps  []epaxos.ApplyCommand
	clock *uint64
	cfg   epaxos.Config
}

type clusterHarness struct {
	ids   []epaxos.ReplicaID
	nodes map[epaxos.ReplicaID]*nodeHarness
}

type phaseActions struct {
	ready   string
	persist string
	advance string
	apply   string
	codec   string
	step    string
	drop    string
	frozen  string
}

type phaseMetrics struct {
	messages  []epaxos.Message
	sawAccept map[epaxos.InstanceRef]bool
	statuses  map[epaxos.InstanceRef]map[epaxos.Status]bool
}

func captureScenarios() ([]semanticTrace, error) {
	captures := []func() (semanticTrace, error){captureNormalScenario, captureRecoveryScenario, captureTOQScenario, captureConfigScenario}
	out := make([]semanticTrace, 0, len(captures))
	for _, capture := range captures {
		trace, err := capture()
		if err != nil {
			return nil, err
		}
		out = append(out, trace)
	}
	return out, nil
}

func addScope(b *traceBuilder, action string, scenario string, bounds []string) error {
	return b.add(action, semanticEvent{
		Kind: "finite-scope", Result: classifyError(nil),
		Scope: &scopeView{
			Scenarios: []string{scenario}, Bounds: bounds,
			HiddenMetadata: []string{
				"record and message checksum bytes (only checksum validity is semantic)",
				"canonical codec byte contents and borrowed buffer identity (wire length and real encode/decode execution are recorded)",
				"slice capacities, pool identities, allocation addresses, Ready clone allocation identity",
			},
			ReadyIDPolicy:      "sha256 of canonical semantic Ready payload because public RawNode Ready exposes no numeric token",
			TransportPolicy:    "every delivered message is canonically EncodeMessage then DecodeMessage; decoded bytes remain live through Step",
			SemanticComparison: "each exact formal stage go_event commits by SHA-256 to its canonical mapped semantic events and the full ordered scenario; replay reconstructs fresh RawNodes and compares the exact export bytes",
			NonClaim:           "bounded representative scenarios only; this is not a claim of arbitrary Go implementation refinement",
		},
	})
}

func newClassicCluster(ids []epaxos.ReplicaID, retryTicks, recoveryTicks uint64) (*clusterHarness, error) {
	cluster := &clusterHarness{ids: append([]epaxos.ReplicaID(nil), ids...), nodes: make(map[epaxos.ReplicaID]*nodeHarness, len(ids))}
	for _, id := range ids {
		store := epaxos.NewMemoryStorage()
		cfg := epaxos.Config{ID: id, Voters: append([]epaxos.ReplicaID(nil), ids...), Storage: store, RetryTicks: retryTicks, RecoveryTicks: recoveryTicks}
		node, err := epaxos.NewRawNode(cfg)
		if err != nil {
			return nil, fmt.Errorf("new node %d: %w", id, err)
		}
		harness := &nodeHarness{id: id, node: node, store: store, cfg: cfg}
		if err := persistInitial(harness); err != nil {
			return nil, fmt.Errorf("persist initial node %d: %w", id, err)
		}
		cluster.nodes[id] = harness
	}
	return cluster, nil
}

func persistInitial(node *nodeHarness) error {
	if !node.node.HasReady() {
		return fmt.Errorf("fresh node %d omitted initial HardState Ready", node.id)
	}
	ready := node.node.Ready()
	if ready.HardState.Empty() || !ready.MustSync || len(ready.Records) != 0 || len(ready.Messages) != 0 || len(ready.Apply) != 0 {
		return fmt.Errorf("fresh node %d initial Ready is not exact HardState-only durability work", node.id)
	}
	if err := node.store.ApplyReady(ready); err != nil {
		return err
	}
	if err := node.node.Advance(ready); err != nil {
		return err
	}
	return nil
}

func addAPICall(b *traceBuilder, action, kind string, node *nodeHarness, input *inputView, pre snapshotView, callErr error, admission, drop, boundary string) error {
	post, err := snapshot(node.node, node.store)
	if err != nil {
		return err
	}
	return b.add(action, semanticEvent{
		Kind: kind, Node: uint64(node.id), Input: input, Result: classifyError(callErr),
		ResponseAdmission: admission, DropClassification: drop, Boundary: boundary, Pre: &pre, Post: &post,
	})
}

func consumeReady(b *traceBuilder, node *nodeHarness, actions phaseActions, freeze bool, metrics *phaseMetrics) ([]epaxos.Message, error) {
	pre, err := snapshot(node.node, node.store)
	if err != nil {
		return nil, err
	}
	ready := node.node.Ready()
	id, err := readyID(ready)
	if err != nil {
		return nil, err
	}
	post, err := snapshot(node.node, node.store)
	if err != nil {
		return nil, err
	}
	readySemantic := toReady(ready)
	if err := b.add(actions.ready, semanticEvent{
		Kind: "ready-observed", Node: uint64(node.id), Input: &inputView{Operation: "Ready"}, Result: classifyError(nil),
		ReadyID: id, Ready: &readySemantic, Pre: &pre, Post: &post,
	}); err != nil {
		return nil, err
	}
	if freeze {
		repeated := node.node.Ready()
		repeatedID, err := readyID(repeated)
		if err != nil {
			return nil, err
		}
		repeatedSemantic := toReady(repeated)
		frozenPre, err := snapshot(node.node, node.store)
		if err != nil {
			return nil, err
		}
		if repeatedID != id || !semanticJSONEqual(readySemantic, repeatedSemantic) {
			return nil, fmt.Errorf("node %d Ready changed before Advance", node.id)
		}
		if err := b.add(actions.frozen, semanticEvent{
			Kind: "frozen-ready-probe", Node: uint64(node.id), Input: &inputView{Operation: "Ready", ReadyID: id}, Result: classifyError(nil),
			ReadyID: repeatedID, Ready: &repeatedSemantic, Pre: &frozenPre, Post: &frozenPre,
		}); err != nil {
			return nil, err
		}
	}

	persistPre, err := snapshot(node.node, node.store)
	if err != nil {
		return nil, err
	}
	persistErr := node.store.ApplyReady(ready)
	persistPost, snapErr := snapshot(node.node, node.store)
	if snapErr != nil {
		return nil, snapErr
	}
	persistence := "success"
	if persistErr != nil {
		persistence = "failure"
	}
	if err := b.add(actions.persist, semanticEvent{
		Kind: "persistence-complete", Node: uint64(node.id), Input: &inputView{Operation: "MemoryStorage.ApplyReady", ReadyID: id},
		Result: classifyError(persistErr), ReadyID: id, Ready: &readySemantic, Persistence: persistence, Pre: &persistPre, Post: &persistPost,
	}); err != nil {
		return nil, err
	}
	if persistErr != nil {
		return nil, persistErr
	}

	messages := make([]epaxos.Message, len(ready.Messages))
	for i := range ready.Messages {
		messages[i] = ready.Messages[i].Clone()
		if metrics != nil {
			metrics.messages = append(metrics.messages, messages[i].Clone())
			if messages[i].Type == epaxos.MsgAccept {
				metrics.sawAccept[messages[i].Ref] = true
			}
		}
	}
	if metrics != nil {
		for _, record := range ready.Records {
			if metrics.statuses[record.Ref] == nil {
				metrics.statuses[record.Ref] = make(map[epaxos.Status]bool)
			}
			metrics.statuses[record.Ref][record.Status] = true
		}
	}
	if len(ready.Apply) != 0 {
		for _, committed := range ready.Apply {
			node.apps = append(node.apps, committed.Clone())
		}
		application := make([]committedView, len(ready.Apply))
		for i := range ready.Apply {
			application[i] = toCommitted(ready.Apply[i])
		}
		order := make([]refView, len(node.apps))
		for i := range node.apps {
			order[i] = toRef(node.apps[i].Ref)
		}
		appSnapshot, err := snapshot(node.node, node.store)
		if err != nil {
			return nil, err
		}
		if err := b.add(actions.apply, semanticEvent{
			Kind: "application-acknowledged", Node: uint64(node.id), Input: &inputView{Operation: "apply committed Ready entries", ReadyID: id},
			Result: classifyError(nil), ReadyID: id, Application: application, ExecutionOrder: order, Pre: &appSnapshot, Post: &appSnapshot,
		}); err != nil {
			return nil, err
		}
	}

	advancePre, err := snapshot(node.node, node.store)
	if err != nil {
		return nil, err
	}
	advanceErr := node.node.Advance(ready)
	advancePost, snapErr := snapshot(node.node, node.store)
	if snapErr != nil {
		return nil, snapErr
	}
	if err := b.add(actions.advance, semanticEvent{
		Kind: "advance-complete", Node: uint64(node.id), Input: &inputView{Operation: "RawNode.Advance", ReadyID: id}, Result: classifyError(advanceErr),
		ReadyID: id, AdvanceID: id, Pre: &advancePre, Post: &advancePost,
	}); err != nil {
		return nil, err
	}
	if advanceErr != nil {
		return nil, advanceErr
	}
	return messages, nil
}

func deliverMessage(b *traceBuilder, cluster *clusterHarness, message epaxos.Message, actions phaseActions, actionOverride, admission, drop string) (epaxos.Message, error) {
	wire, err := epaxos.EncodeMessage(nil, message)
	if err != nil {
		return epaxos.Message{}, err
	}
	var decoded epaxos.Message
	decodeErr := epaxos.DecodeMessage(wire, &decoded)
	if decodeErr != nil {
		return epaxos.Message{}, decodeErr
	}
	target := cluster.nodes[message.To]
	if target == nil {
		return epaxos.Message{}, fmt.Errorf("missing target node %d", message.To)
	}
	codecSnapshot, err := snapshot(target.node, target.store)
	if err != nil {
		return epaxos.Message{}, err
	}
	messageSemantic := toMessage(decoded)
	if err := b.add(actions.codec, semanticEvent{
		Kind: "canonical-message-roundtrip", Node: uint64(target.id), Input: &inputView{Operation: "EncodeMessage+DecodeMessage", Message: &messageSemantic},
		Result: classifyError(nil), Pre: &codecSnapshot, Post: &codecSnapshot,
	}); err != nil {
		return epaxos.Message{}, err
	}
	pre, err := snapshot(target.node, target.store)
	if err != nil {
		return epaxos.Message{}, err
	}
	stepErr := target.node.Step(decoded)
	action := actions.step
	if actionOverride != "" {
		action = actionOverride
	} else if stepErr != nil {
		action = actions.drop
	}
	if err := addAPICall(b, action, "message-step", target, &inputView{Operation: "RawNode.Step", Message: &messageSemantic}, pre, stepErr, admission, drop, ""); err != nil {
		return epaxos.Message{}, err
	}
	if stepErr != nil && !errors.Is(stepErr, epaxos.ErrMessageRejected) {
		return epaxos.Message{}, fmt.Errorf("%s %s %d->%d for %s: %w", action, decoded.Type, decoded.From, decoded.To, decoded.Ref, stepErr)
	}
	return decoded, nil
}

func pumpCluster(b *traceBuilder, cluster *clusterHarness, actions phaseActions, metrics *phaseMetrics, freezeFirst *bool) error {
	return pumpClusterQueued(b, cluster, actions, metrics, freezeFirst, nil)
}

func pumpClusterQueued(b *traceBuilder, cluster *clusterHarness, actions phaseActions, metrics *phaseMetrics, freezeFirst *bool, initial []epaxos.Message) error {
	queue := make([]epaxos.Message, len(initial))
	for i := range initial {
		queue[i] = initial[i].Clone()
	}
	for round := 0; round < 4000; round++ {
		progress := false
		for _, id := range cluster.ids {
			node := cluster.nodes[id]
			if !node.node.HasReady() {
				continue
			}
			freeze := freezeFirst != nil && !*freezeFirst
			messages, err := consumeReady(b, node, actions, freeze, metrics)
			if err != nil {
				return err
			}
			if freeze {
				*freezeFirst = true
			}
			queue = append(queue, messages...)
			progress = true
		}
		if len(queue) != 0 {
			message := queue[0]
			queue = queue[1:]
			if _, err := deliverMessage(b, cluster, message, actions, "", "admitted-or-idempotent", ""); err != nil {
				return err
			}
			progress = true
		}
		if !progress {
			return nil
		}
	}
	return fmt.Errorf("cluster did not quiesce within finite round bound")
}

func confEqual(a, b epaxos.ConfState) bool {
	if a.ID != b.ID || len(a.Voters) != len(b.Voters) {
		return false
	}
	for i := range a.Voters {
		if a.Voters[i] != b.Voters[i] {
			return false
		}
	}
	return true
}

func instanceNumsEqual(a, b []epaxos.InstanceNum) bool {
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

func applicationContains(apps []epaxos.ApplyCommand, ref epaxos.InstanceRef) bool {
	for _, committed := range apps {
		if committed.Ref == ref {
			return true
		}
	}
	return false
}

func recordNetworkDrops(b *traceBuilder, node *nodeHarness, action string, messages []epaxos.Message, retained map[epaxos.ReplicaID]bool, reason string) error {
	for _, message := range messages {
		if retained[message.To] {
			continue
		}
		messageSemantic := toMessage(message)
		state, err := snapshot(node.node, node.store)
		if err != nil {
			return err
		}
		if err := b.add(action, semanticEvent{
			Kind: "network-drop", Node: uint64(node.id),
			Input:  &inputView{Operation: "drop persisted outbound message", Message: &messageSemantic},
			Result: classifyError(nil), DropClassification: reason, Pre: &state, Post: &state,
		}); err != nil {
			return err
		}
	}
	return nil
}

func semanticJSONEqual(a, b any) bool {
	left, err1 := json.Marshal(a)
	right, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(left, right)
}

func snapshotEqual(a, b snapshotView) bool { return semanticJSONEqual(a, b) }

func captureNormalScenario() (semanticTrace, error) {
	b := &traceBuilder{id: "normal-fast-slow"}
	if err := addScope(b, "NormalCodecOnly", "classic fast path followed by conflicting concurrent proposals that enter slow Accept", []string{
		"three replicas {1,2,3}; configuration 1", "three user commands; fixed FIFO transport and ascending Ready scan", "no faults except final stale duplicate and wrong-target probes",
	}); err != nil {
		return semanticTrace{}, err
	}
	cluster, err := newClassicCluster([]epaxos.ReplicaID{1, 2, 3}, 2, 5)
	if err != nil {
		return semanticTrace{}, err
	}
	actions := phaseActions{
		ready: "NormalBuildReady", persist: "NormalPersistReady", advance: "NormalAdvance", apply: "NormalApply",
		codec: "NormalCodecOnly", step: "NormalRetrySend", drop: "NormalValidationDrop", frozen: "NormalFrozenReadyProbe",
	}
	metrics := &phaseMetrics{sawAccept: make(map[epaxos.InstanceRef]bool), statuses: make(map[epaxos.InstanceRef]map[epaxos.Status]bool)}
	fastCommand := epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 1}, Payload: []byte("fast"), Footprint: epaxos.Footprint{Points: [][]byte{[]byte("fast-key")}}}
	fastNode := cluster.nodes[1]
	pre, err := snapshot(fastNode.node, fastNode.store)
	if err != nil {
		return semanticTrace{}, err
	}
	fastRef, proposeErr := fastNode.node.Propose(fastCommand)
	refSemantic := toRef(fastRef)
	commandSemantic := toCommand(epaxos.EntryCommand, fastCommand)
	if err := addAPICall(b, "NormalPropose", "propose-fast", fastNode, &inputView{Operation: "RawNode.Propose", Ref: &refSemantic, Command: &commandSemantic}, pre, proposeErr, "", "", ""); err != nil {
		return semanticTrace{}, err
	}
	if proposeErr != nil {
		return semanticTrace{}, proposeErr
	}
	frozen := false
	if err := pumpCluster(b, cluster, actions, metrics, &frozen); err != nil {
		return semanticTrace{}, err
	}
	if metrics.sawAccept[fastRef] {
		return semanticTrace{}, fmt.Errorf("normal fast-path reference %s unexpectedly emitted Accept", fastRef)
	}
	if !metrics.statuses[fastRef][epaxos.StatusCommitted] && !metrics.statuses[fastRef][epaxos.StatusExecuted] {
		return semanticTrace{}, fmt.Errorf("normal fast-path reference %s did not commit", fastRef)
	}

	var slowRefs []epaxos.InstanceRef
	for _, id := range []epaxos.ReplicaID{2, 3} {
		node := cluster.nodes[id]
		command := epaxos.Command{ID: epaxos.CommandID{Client: uint64(id), Sequence: 2}, Payload: []byte(fmt.Sprintf("slow-%d", id)), Footprint: epaxos.Footprint{Points: [][]byte{[]byte("slow-key")}}}
		pre, err := snapshot(node.node, node.store)
		if err != nil {
			return semanticTrace{}, err
		}
		ref, callErr := node.node.Propose(command)
		refView := toRef(ref)
		commandView := toCommand(epaxos.EntryCommand, command)
		if err := addAPICall(b, "NormalPropose", "propose-conflicting", node, &inputView{Operation: "RawNode.Propose", Ref: &refView, Command: &commandView}, pre, callErr, "", "", ""); err != nil {
			return semanticTrace{}, err
		}
		if callErr != nil {
			return semanticTrace{}, callErr
		}
		slowRefs = append(slowRefs, ref)
	}
	if err := pumpCluster(b, cluster, actions, metrics, nil); err != nil {
		return semanticTrace{}, err
	}
	sawSlow := false
	for _, ref := range slowRefs {
		sawSlow = sawSlow || metrics.sawAccept[ref] || metrics.statuses[ref][epaxos.StatusAccepted]
	}
	if !sawSlow {
		return semanticTrace{}, fmt.Errorf("conflicting normal scenario did not exercise slow Accept")
	}
	for _, ref := range slowRefs {
		for _, id := range cluster.ids {
			node := cluster.nodes[id]
			record, ok := node.store.Instance(ref)
			if !ok || record.Status != epaxos.StatusExecuted || !applicationContains(node.apps, ref) {
				return semanticTrace{}, fmt.Errorf("slow-path reference %s on node %d status=%s durable=%v applied=%v, want executed and applied", ref, id, record.Status, ok, applicationContains(node.apps, ref))
			}
		}
	}

	var duplicate epaxos.Message
	for i := len(metrics.messages) - 1; i >= 0; i-- {
		if metrics.messages[i].Type == epaxos.MsgCommit {
			duplicate = metrics.messages[i].Clone()
			break
		}
	}
	if duplicate.Type == 0 {
		return semanticTrace{}, fmt.Errorf("normal scenario emitted no Commit for duplicate probe")
	}
	target := cluster.nodes[duplicate.To]
	beforeDuplicate, err := snapshot(target.node, target.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if _, err := deliverMessage(b, cluster, duplicate, actions, "NormalDuplicateDrop", "duplicate-stutter", "duplicate-commit"); err != nil {
		return semanticTrace{}, err
	}
	afterDuplicate, err := snapshot(target.node, target.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if !snapshotEqual(beforeDuplicate, afterDuplicate) {
		return semanticTrace{}, fmt.Errorf("duplicate normal Commit changed observable state")
	}

	wrong := duplicate.Clone()
	actualTarget := wrong.To
	for _, candidate := range cluster.ids {
		if candidate != actualTarget {
			wrong.To = candidate
			break
		}
	}
	wrong.Checksum = epaxos.ChecksumMessage(wrong)
	wrongWire, err := epaxos.EncodeMessage(nil, wrong)
	if err != nil {
		return semanticTrace{}, err
	}
	var wrongDecoded epaxos.Message
	if err := epaxos.DecodeMessage(wrongWire, &wrongDecoded); err != nil {
		return semanticTrace{}, err
	}
	wrongSemantic := toMessage(wrongDecoded)
	beforeWrong, err := snapshot(target.node, target.store)
	if err != nil {
		return semanticTrace{}, err
	}
	stepErr := target.node.Step(wrongDecoded)
	if !errors.Is(stepErr, epaxos.ErrMessageRejected) {
		if stepErr != nil {
			return semanticTrace{}, fmt.Errorf("wrong-target normal probe: %w, want message rejected", stepErr)
		}
		return semanticTrace{}, errors.New("wrong-target normal probe: nil error, want message rejected")
	}
	if err := addAPICall(b, "NormalValidationDrop", "wrong-target-step", target, &inputView{Operation: "RawNode.Step", Message: &wrongSemantic}, beforeWrong, stepErr, "rejected", "wrong-target", ""); err != nil {
		return semanticTrace{}, err
	}
	afterWrong, err := snapshot(target.node, target.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if !snapshotEqual(beforeWrong, afterWrong) {
		return semanticTrace{}, fmt.Errorf("wrong-target normal probe changed observable state")
	}
	return b.finish(), nil
}

func captureRecoveryScenario() (semanticTrace, error) {
	b := &traceBuilder{id: "recovery-response-restart"}
	if err := addScope(b, "RecoveryFrozenReadyProbe", "durable accepted tuple, crash/restart, recovery sender admission and response stutters", []string{
		"five replicas {1,2,3,4,5}; configuration 1; majority requires two remote responses plus local state", "one user instance; recovery tick interval 2", "duplicate, stale-ballot, and wrong-target Prepare responses are finite explicit probes",
	}); err != nil {
		return semanticTrace{}, err
	}
	cluster, err := newClassicCluster([]epaxos.ReplicaID{1, 2, 3, 4, 5}, 2, 2)
	if err != nil {
		return semanticTrace{}, err
	}
	owner := cluster.nodes[1]
	coordinator := cluster.nodes[2]
	seedCommand := epaxos.Command{ID: epaxos.CommandID{Client: 91, Sequence: 1}, Payload: []byte("recover"), Footprint: epaxos.Footprint{Points: [][]byte{[]byte("recovery-key")}}}
	pre, err := snapshot(owner.node, owner.store)
	if err != nil {
		return semanticTrace{}, err
	}
	ref, proposeErr := owner.node.Propose(seedCommand)
	refSemantic := toRef(ref)
	commandSemantic := toCommand(epaxos.EntryCommand, seedCommand)
	if err := addAPICall(b, "RecoverySeedAccepted", "seed-propose", owner, &inputView{Operation: "RawNode.Propose", Ref: &refSemantic, Command: &commandSemantic}, pre, proposeErr, "", "", ""); err != nil {
		return semanticTrace{}, err
	}
	if proposeErr != nil {
		return semanticTrace{}, proposeErr
	}
	seedActions := phaseActions{ready: "RecoveryBuildAcceptedReady", persist: "RecoveryPersistAccepted", advance: "RecoveryAdvanceAccepted", apply: "RecoveryApply", codec: "RecoveryFrozenReadyProbe", step: "RecoverySeedAccepted", drop: "RecoverySeedAccepted", frozen: "RecoveryFrozenReadyProbe"}
	seedMessages, err := consumeReady(b, owner, seedActions, true, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	for _, outbound := range seedMessages {
		outboundView := toMessage(outbound)
		dropSnapshot, err := snapshot(owner.node, owner.store)
		if err != nil {
			return semanticTrace{}, err
		}
		if err := b.add("RecoverySeedAccepted", semanticEvent{Kind: "network-drop", Node: uint64(owner.id), Input: &inputView{Operation: "drop outbound before peer delivery", Message: &outboundView}, Result: classifyError(nil), DropClassification: "fault-injected-loss", Pre: &dropSnapshot, Post: &dropSnapshot}); err != nil {
			return semanticTrace{}, err
		}
	}
	seedRecord, ok := owner.store.Instance(ref)
	if !ok {
		return semanticTrace{}, fmt.Errorf("seed record %s was not durable", ref)
	}
	accept := epaxos.Message{
		Type: epaxos.MsgAccept, From: 3, To: 2, FromIncarnation: 1, ToIncarnation: 1,
		Ref: ref, Ballot: epaxos.Ballot{Number: 1, Replica: 3},
		Seq: seedRecord.Seq, Deps: append([]epaxos.InstanceNum(nil), seedRecord.Deps...),
		Kind: seedRecord.Kind, ConfChange: seedRecord.ConfChange,
		ProtocolControl: append([]byte(nil), seedRecord.ProtocolControl...), Command: seedRecord.Command.Clone(),
	}
	accept.Checksum = epaxos.ChecksumMessage(accept)
	acceptActions := phaseActions{ready: "RecoveryBuildAcceptedReady", persist: "RecoveryPersistAccepted", advance: "RecoveryAdvanceAccepted", apply: "RecoveryApply", codec: "RecoveryBuildAcceptedReady", step: "RecoverySeedAccepted", drop: "RecoverySeedAccepted", frozen: "RecoveryFrozenReadyProbe"}
	if _, err := deliverMessage(b, cluster, accept, acceptActions, "RecoverySeedAccepted", "admitted", ""); err != nil {
		return semanticTrace{}, fmt.Errorf("seed Accept: %w", err)
	}
	acceptedMessages, err := consumeReady(b, coordinator, acceptActions, false, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := recordNetworkDrops(b, coordinator, "RecoveryAdvanceAccepted", acceptedMessages, nil, "fault-injected-post-accept-loss"); err != nil {
		return semanticTrace{}, err
	}
	acceptedRecord, ok := coordinator.store.Instance(ref)
	if !ok || acceptedRecord.Status != epaxos.StatusAccepted {
		return semanticTrace{}, fmt.Errorf("seed record %s status=%s ok=%v, want accepted", ref, acceptedRecord.Status, ok)
	}

	crashPre, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	restarted, restartErr := epaxos.NewRawNode(coordinator.cfg)
	if restartErr != nil {
		return semanticTrace{}, restartErr
	}
	coordinator.node = restarted
	crashPost, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := b.add("RecoveryCrash", semanticEvent{
		Kind: "crash-restart", Node: uint64(coordinator.id), Input: &inputView{Operation: "discard RawNode then NewRawNode with same MemoryStorage", Ref: &refSemantic},
		Result: classifyError(restartErr), Boundary: "after accepted Ready persistence and Advance; volatile node discarded; exact durable storage reused", Pre: &crashPre, Post: &crashPost,
	}); err != nil {
		return semanticTrace{}, err
	}
	if got, ok := coordinator.store.Instance(ref); !ok || got.Status != epaxos.StatusAccepted {
		return semanticTrace{}, fmt.Errorf("restart lost durable accepted record %s", ref)
	}

	recoveryActions := phaseActions{ready: "RecoveryBuildBallotReady", persist: "RecoveryPersistBallot", advance: "RecoveryAdvanceBallot", apply: "RecoveryApply", codec: "RecoveryBuildBallotReady", step: "RecoveryFirstSender", drop: "RecoveryWrongTargetDrop", frozen: "RecoveryFrozenReadyProbe"}
	var prepares []epaxos.Message
	var observedRecoveryMessages []string
	for tick := uint64(1); tick <= 8; tick++ {
		tickPre, err := snapshot(coordinator.node, coordinator.store)
		if err != nil {
			return semanticTrace{}, err
		}
		tickErr := coordinator.node.Tick()
		if err := addAPICall(b, "RecoveryStartBallot", "logical-tick", coordinator, &inputView{Operation: "RawNode.Tick", Tick: &tick}, tickPre, tickErr, "", "", ""); err != nil {
			return semanticTrace{}, err
		}
		if tickErr != nil {
			return semanticTrace{}, tickErr
		}
		if coordinator.node.HasReady() {
			messages, err := consumeReady(b, coordinator, recoveryActions, false, nil)
			if err != nil {
				return semanticTrace{}, err
			}
			for _, message := range messages {
				observedRecoveryMessages = append(observedRecoveryMessages, fmt.Sprintf("%s:%s", message.Type, message.Ref))
				if message.Type == epaxos.MsgPrepare && message.Ref == ref {
					prepares = append(prepares, message.Clone())
				}
			}
		}
		if len(prepares) >= 2 {
			break
		}
	}
	if len(prepares) < 2 {
		return semanticTrace{}, fmt.Errorf("recovery did not emit peer Prepare messages: %d; observed=%v; status=%#v", len(prepares), observedRecoveryMessages, coordinator.node.Status())
	}
	if err := recordNetworkDrops(b, coordinator, "RecoveryAdvanceBallot", prepares, map[epaxos.ReplicaID]bool{1: true, 3: true}, "bounded-unselected-prepare-loss"); err != nil {
		return semanticTrace{}, err
	}
	sort.Slice(prepares, func(i, j int) bool { return prepares[i].To < prepares[j].To })
	prepareBallot := prepares[0].Ballot

	responses := make(map[epaxos.ReplicaID]epaxos.Message)
	for _, peerID := range []epaxos.ReplicaID{1, 3} {
		var prepare epaxos.Message
		for _, candidate := range prepares {
			if candidate.To == peerID {
				prepare = candidate.Clone()
				break
			}
		}
		if prepare.Type == 0 {
			return semanticTrace{}, fmt.Errorf("missing Prepare for peer %d", peerID)
		}
		if _, err := deliverMessage(b, cluster, prepare, recoveryActions, "RecoveryBuildBallotReady", "admitted", ""); err != nil {
			return semanticTrace{}, err
		}
		peerMessages, err := consumeReady(b, cluster.nodes[peerID], recoveryActions, false, nil)
		if err != nil {
			return semanticTrace{}, err
		}
		for _, message := range peerMessages {
			if message.Type == epaxos.MsgPrepareResp && message.To == coordinator.id && message.Ref == ref {
				responses[peerID] = message.Clone()
			}
		}
	}
	if len(responses) != 2 {
		return semanticTrace{}, fmt.Errorf("recovery peers produced %d Prepare responses, want 2", len(responses))
	}

	if _, err := deliverMessage(b, cluster, responses[1], recoveryActions, "RecoveryFirstSender", "admitted-new-sender", ""); err != nil {
		return semanticTrace{}, err
	}
	beforeDuplicate, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if _, err := deliverMessage(b, cluster, responses[1], recoveryActions, "RecoveryDuplicateSender", "duplicate-stutter", "duplicate-sender"); err != nil {
		return semanticTrace{}, err
	}
	afterDuplicate, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if !snapshotEqual(beforeDuplicate, afterDuplicate) {
		return semanticTrace{}, fmt.Errorf("duplicate recovery sender changed public semantic state")
	}

	stale := responses[3].Clone()
	stale.Ballot = epaxos.Ballot{Number: 1, Replica: coordinator.id}
	stale.Checksum = epaxos.ChecksumMessage(stale)
	beforeStale, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if _, err := deliverMessage(b, cluster, stale, recoveryActions, "RecoveryStaleBallotDrop", "stale-stutter", "stale-ballot"); err != nil {
		return semanticTrace{}, err
	}
	afterStale, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if !snapshotEqual(beforeStale, afterStale) {
		return semanticTrace{}, fmt.Errorf("stale recovery response changed public semantic state")
	}

	wrongTarget := responses[3].Clone()
	wrongTarget.Ref.Instance += 99
	wrongTarget.Checksum = epaxos.ChecksumMessage(wrongTarget)
	beforeWrong, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if _, err := deliverMessage(b, cluster, wrongTarget, recoveryActions, "RecoveryWrongTargetDrop", "wrong-target-stutter", "wrong-target-ref"); err != nil {
		return semanticTrace{}, err
	}
	afterWrong, err := snapshot(coordinator.node, coordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if !snapshotEqual(beforeWrong, afterWrong) {
		return semanticTrace{}, fmt.Errorf("wrong-target recovery response changed public semantic state")
	}

	if _, err := deliverMessage(b, cluster, responses[3], recoveryActions, "RecoverySecondSender", "admitted-new-sender", ""); err != nil {
		return semanticTrace{}, err
	}
	if !coordinator.node.HasReady() {
		return semanticTrace{}, fmt.Errorf("second unique recovery sender produced no Accept Ready")
	}
	commitActions := phaseActions{ready: "RecoveryBuildCommitReady", persist: "RecoveryPersistCommit", advance: "RecoveryAdvanceCommit", apply: "RecoveryApply", codec: "RecoveryBuildCommitReady", step: "RecoverySecondSender", drop: "RecoveryWrongTargetDrop", frozen: "RecoveryFrozenReadyProbe"}
	acceptRound, err := consumeReady(b, coordinator, commitActions, false, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	var accepts []epaxos.Message
	for _, message := range acceptRound {
		if message.Type == epaxos.MsgAccept && message.Ref == ref {
			accepts = append(accepts, message.Clone())
		}
	}
	if len(accepts) < 2 {
		return semanticTrace{}, fmt.Errorf("recovery Accept round emitted %d messages", len(accepts))
	}
	if err := recordNetworkDrops(b, coordinator, "RecoveryAdvanceCommit", acceptRound, map[epaxos.ReplicaID]bool{1: true, 3: true}, "bounded-unselected-accept-loss"); err != nil {
		return semanticTrace{}, err
	}
	sort.Slice(accepts, func(i, j int) bool { return accepts[i].To < accepts[j].To })
	recoveryBallot := accepts[0].Ballot
	for _, message := range accepts[1:] {
		if message.Ballot != recoveryBallot {
			return semanticTrace{}, fmt.Errorf("recovery Accept messages used inconsistent ballots")
		}
	}
	acceptResponses := make(map[epaxos.ReplicaID]epaxos.Message)
	for _, peerID := range []epaxos.ReplicaID{1, 3} {
		var acceptMessage epaxos.Message
		for _, candidate := range accepts {
			if candidate.To == peerID {
				acceptMessage = candidate.Clone()
				break
			}
		}
		if _, err := deliverMessage(b, cluster, acceptMessage, commitActions, "RecoveryBuildCommitReady", "admitted", ""); err != nil {
			return semanticTrace{}, err
		}
		peerMessages, err := consumeReady(b, cluster.nodes[peerID], commitActions, false, nil)
		if err != nil {
			return semanticTrace{}, err
		}
		for _, message := range peerMessages {
			if message.Type == epaxos.MsgAcceptResp && message.To == coordinator.id && message.Ref == ref {
				acceptResponses[peerID] = message.Clone()
			}
		}
	}
	for _, peerID := range []epaxos.ReplicaID{1, 3} {
		if acceptResponses[peerID].Type == 0 {
			return semanticTrace{}, fmt.Errorf("peer %d produced no Accept response", peerID)
		}
		if _, err := deliverMessage(b, cluster, acceptResponses[peerID], commitActions, "RecoverySecondSender", "accept-response-admitted", ""); err != nil {
			return semanticTrace{}, err
		}
	}
	if !coordinator.node.HasReady() {
		return semanticTrace{}, fmt.Errorf("recovery Accept quorum produced no commit Ready")
	}
	commitMessages, err := consumeReady(b, coordinator, commitActions, false, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := recordNetworkDrops(b, coordinator, "RecoveryPersistCommit", commitMessages, nil, "bounded-undelivered-commit-loss"); err != nil {
		return semanticTrace{}, err
	}
	for coordinator.node.HasReady() {
		followupMessages, err := consumeReady(b, coordinator, commitActions, false, nil)
		if err != nil {
			return semanticTrace{}, err
		}
		if err := recordNetworkDrops(b, coordinator, "RecoveryAdvanceCommit", followupMessages, nil, "bounded-followup-loss"); err != nil {
			return semanticTrace{}, err
		}
	}
	finalRecord, ok := coordinator.store.Instance(ref)
	if !ok || finalRecord.Status != epaxos.StatusExecuted {
		return semanticTrace{}, fmt.Errorf("recovered record status=%s ok=%v, want executed", finalRecord.Status, ok)
	}
	if prepareBallot != responses[1].Ballot || prepareBallot != responses[3].Ballot {
		return semanticTrace{}, fmt.Errorf("recovery response ballots did not bind exact Prepare ballot")
	}
	if !semanticJSONEqual(toCommand(finalRecord.Kind, finalRecord.Command), toCommand(acceptedRecord.Kind, acceptedRecord.Command)) ||
		finalRecord.Seq != acceptedRecord.Seq ||
		!instanceNumsEqual(finalRecord.Deps, acceptedRecord.Deps) ||
		finalRecord.Ballot != recoveryBallot ||
		finalRecord.RecordBallot != recoveryBallot ||
		!applicationContains(coordinator.apps, ref) {
		return semanticTrace{}, fmt.Errorf("recovered tuple changed: accepted=%#v final=%#v applied=%v", acceptedRecord, finalRecord, applicationContains(coordinator.apps, ref))
	}
	return b.finish(), nil
}

func captureTOQScenario() (semanticTrace, error) {
	b := &traceBuilder{id: "toq-logical-processat"}
	if err := addScope(b, "TOQMaxTickDrop", "TOQ pending persistence, logical retry ticks, explicit due ProcessAt decision, and application", []string{
		"one replica {1}; configuration 1; caller TOQ clock starts at 7", "one-way delay bound 2 gives ProcessAt 9", "two durable logical Tick inputs precede the due ProcessTOQ call",
	}); err != nil {
		return semanticTrace{}, err
	}
	clock := uint64(7)
	store := epaxos.NewMemoryStorage()
	conf := epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1}}
	runtime := &epaxos.TOQRuntimeConfig{Conf: conf, OneWayDelay: map[epaxos.ReplicaID]uint64{1: 2}}
	cfg := epaxos.Config{ID: 1, Voters: conf.Voters, Storage: store, RetryTicks: 3, RecoveryTicks: 9, TOQ: true, TOQRuntime: runtime}
	rawNode, err := epaxos.NewRawNode(cfg)
	if err != nil {
		return semanticTrace{}, err
	}
	node := &nodeHarness{id: 1, node: rawNode, store: store, cfg: cfg, clock: &clock}
	if err := persistInitial(node); err != nil {
		return semanticTrace{}, err
	}
	command := epaxos.Command{ID: epaxos.CommandID{Client: 220, Sequence: 1}, Payload: []byte("toq"), Footprint: epaxos.Footprint{Points: [][]byte{[]byte("toq-key")}}}
	pre, err := snapshot(node.node, node.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := node.node.ProcessTOQ(clock); err != nil {
		return semanticTrace{}, err
	}
	ref, proposeErr := node.node.Propose(command)
	refSemantic := toRef(ref)
	commandSemantic := toCommand(epaxos.EntryCommand, command)
	if err := addAPICall(b, "TOQPropose", "toq-propose", node, &inputView{Operation: "RawNode.Propose", Ref: &refSemantic, Command: &commandSemantic, TOQClock: &clock}, pre, proposeErr, "", "", ""); err != nil {
		return semanticTrace{}, err
	}
	if proposeErr != nil {
		return semanticTrace{}, proposeErr
	}
	pendingActions := phaseActions{ready: "TOQBuildReady", persist: "TOQPersistPending", advance: "TOQAdvancePending", apply: "TOQApply", codec: "TOQBuildReady", step: "TOQEarlyDecisionDrop", drop: "TOQEarlyDecisionDrop", frozen: "TOQFrozenReadyProbe"}
	pendingMessages, err := consumeReady(b, node, pendingActions, true, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	if len(pendingMessages) != 0 {
		return semanticTrace{}, fmt.Errorf("single-voter pending TOQ Ready emitted transport messages: %v", pendingMessages)
	}
	pending, ok := node.store.Instance(ref)
	if !ok || !pending.TOQPending || pending.ProcessAt != 9 || pending.TimingDomain != epaxos.TimingDomainTOQ || pending.Status != epaxos.StatusNone {
		return semanticTrace{}, fmt.Errorf("durable TOQ pending record=%#v ok=%v, want pending at ProcessAt 9", pending, ok)
	}
	if len(node.apps) != 0 || applicationContains(node.apps, ref) {
		return semanticTrace{}, fmt.Errorf("pending TOQ reference %s reached application before ProcessAt", ref)
	}
	for _, executed := range node.node.Status().Executed {
		if executed == ref {
			return semanticTrace{}, fmt.Errorf("pending TOQ reference %s was marked executed before ProcessAt", ref)
		}
	}
	blockedSnapshot, err := snapshot(node.node, node.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := b.add("TOQPendingApplyBlocked", semanticEvent{
		Kind: "pending-application-blocked", Node: 1, Input: &inputView{Operation: "observe no Ready.Apply while durable TOQPending", Ref: &refSemantic, TOQClock: &clock},
		Result: classifyError(nil), ResponseAdmission: "pending-blocked", Pre: &blockedSnapshot, Post: &blockedSnapshot,
	}); err != nil {
		return semanticTrace{}, err
	}

	clock = 8
	earlyPre, err := snapshot(node.node, node.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if clock >= pending.ProcessAt || node.node.HasReady() {
		return semanticTrace{}, fmt.Errorf("early TOQ decision precondition clock=%d ProcessAt=%d ready=%v", clock, pending.ProcessAt, node.node.HasReady())
	}
	if err := b.add("TOQEarlyDecisionDrop", semanticEvent{
		Kind: "withhold-process-toq-before-due", Node: uint64(node.id),
		Input:  &inputView{Operation: "do not call RawNode.ProcessTOQ before ProcessAt", TOQClock: &clock, Ref: &refSemantic},
		Result: classifyError(nil), ResponseAdmission: "early-stutter", DropClassification: "before-process-at", Pre: &earlyPre, Post: &earlyPre,
	}); err != nil {
		return semanticTrace{}, err
	}

	tickActions := phaseActions{ready: "TOQFirstTick", persist: "TOQFirstTick", advance: "TOQFirstTick", apply: "TOQApply", codec: "TOQFirstTick", step: "TOQFirstTick", drop: "TOQMaxTickDrop", frozen: "TOQFrozenReadyProbe"}
	for logicalTick := uint64(1); logicalTick <= 2; logicalTick++ {
		tickPre, err := snapshot(node.node, node.store)
		if err != nil {
			return semanticTrace{}, err
		}
		tickErr := node.node.Tick()
		action := "TOQFirstTick"
		if logicalTick == 2 {
			action = "TOQDeadlineTick"
			tickActions.ready, tickActions.persist, tickActions.advance = action, action, action
		}
		if err := addAPICall(b, action, "logical-tick", node, &inputView{Operation: "RawNode.Tick", Tick: &logicalTick, TOQClock: &clock}, tickPre, tickErr, "", "", ""); err != nil {
			return semanticTrace{}, err
		}
		if tickErr != nil {
			return semanticTrace{}, tickErr
		}
		if _, err := consumeReady(b, node, tickActions, false, nil); err != nil {
			return semanticTrace{}, err
		}
	}

	clock = pending.ProcessAt
	duePre, err := snapshot(node.node, node.store)
	if err != nil {
		return semanticTrace{}, err
	}
	dueErr := node.node.ProcessTOQ(clock)
	if err := addAPICall(b, "TOQBuildAllowReady", "process-toq-due", node, &inputView{Operation: "RawNode.ProcessTOQ", TOQClock: &clock, Ref: &refSemantic}, duePre, dueErr, "due-admitted", "", ""); err != nil {
		return semanticTrace{}, err
	}
	if dueErr != nil {
		return semanticTrace{}, fmt.Errorf("due ProcessTOQ: %w", dueErr)
	}
	if !node.node.HasReady() {
		return semanticTrace{}, errors.New("due ProcessTOQ: node has no ready work")
	}
	allowActions := phaseActions{ready: "TOQBuildAllowReady", persist: "TOQPersistAllow", advance: "TOQAdvanceAllow", apply: "TOQApply", codec: "TOQBuildAllowReady", step: "TOQBuildAllowReady", drop: "TOQMaxTickDrop", frozen: "TOQFrozenReadyProbe"}
	if _, err := consumeReady(b, node, allowActions, false, nil); err != nil {
		return semanticTrace{}, err
	}
	for node.node.HasReady() {
		if _, err := consumeReady(b, node, allowActions, false, nil); err != nil {
			return semanticTrace{}, err
		}
	}
	final, ok := node.store.Instance(ref)
	if !ok || final.TOQPending || final.Status != epaxos.StatusExecuted || final.ProcessAt != 9 || final.TimingDomain != epaxos.TimingDomainTOQ {
		return semanticTrace{}, fmt.Errorf("final TOQ record=%#v ok=%v, want executed decision at ProcessAt 9", final, ok)
	}
	if len(node.apps) != 1 || node.apps[0].Ref != ref {
		return semanticTrace{}, fmt.Errorf("due TOQ decision applications=%v, want exactly %s", node.apps, ref)
	}
	maxPre, err := snapshot(node.node, node.store)
	if err != nil {
		return semanticTrace{}, err
	}
	maxErr := node.node.ProcessTOQ(clock)
	if err := addAPICall(b, "TOQMaxTickDrop", "process-toq-closed-bucket", node, &inputView{Operation: "RawNode.ProcessTOQ", TOQClock: &clock, Ref: &refSemantic}, maxPre, maxErr, "closed-bucket-stutter", "already-processed-clock", ""); err != nil {
		return semanticTrace{}, err
	}
	maxPost, err := snapshot(node.node, node.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if maxErr != nil {
		return semanticTrace{}, fmt.Errorf("closed TOQ bucket: %w", maxErr)
	}
	if !snapshotEqual(maxPre, maxPost) {
		return semanticTrace{}, errors.New("closed TOQ bucket did not stutter")
	}
	return b.finish(), nil
}

func captureConfigScenario() (semanticTrace, error) {
	b := &traceBuilder{id: "config-outcome-history"}
	if err := addScope(b, "ConfigDependencyBlocked", "applied removal outcome, exact durable configuration history, active recovery of an old-config Ref, and new-config execution", []string{
		"implementation bound is three replicas {1,2,3}; remove replica 3 to exact configuration 2 {1,2}", "one accepted no-op remains pinned to config 1 across the transition; one configuration command in config 1 and one user command in config 2", "public Message has no independent response-config field, so wrong configuration is represented by the same replica/instance coordinates under config 2", "fixed FIFO transport and ascending Ready scan",
	}); err != nil {
		return semanticTrace{}, err
	}
	cluster, err := newClassicCluster([]epaxos.ReplicaID{1, 2, 3}, 2, 5)
	if err != nil {
		return semanticTrace{}, err
	}
	leader := cluster.nodes[1]
	oldRef := epaxos.InstanceRef{Replica: 1, Instance: 50, Conf: 1}
	oldAccept := epaxos.Message{
		Type: epaxos.MsgAccept, From: 3, To: 2, FromIncarnation: 1, ToIncarnation: 1, Ref: oldRef,
		Ballot: epaxos.Ballot{Number: 1, Replica: 3}, Seq: 1,
		Deps: []epaxos.InstanceNum{0, 0, 0}, Kind: epaxos.EntryNoop,
	}
	oldAccept.Checksum = epaxos.ChecksumMessage(oldAccept)
	oldActions := phaseActions{
		ready: "ConfigBuildOldBallotReady", persist: "ConfigPersistOldBallot", advance: "ConfigAdvanceOldBallot", apply: "ConfigApplyA",
		codec: "ConfigBuildAReady", step: "ConfigProposeA", drop: "ConfigWrongConfDrop", frozen: "ConfigFrozenReadyProbe",
	}
	if _, err := deliverMessage(b, cluster, oldAccept, oldActions, "ConfigProposeA", "old-config-accepted", ""); err != nil {
		return semanticTrace{}, err
	}
	oldSeedMessages, err := consumeReady(b, cluster.nodes[2], oldActions, false, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := recordNetworkDrops(b, cluster.nodes[2], "ConfigAdvanceOldBallot", oldSeedMessages, nil, "bounded-old-accept-response-loss"); err != nil {
		return semanticTrace{}, err
	}
	oldSeed, ok := cluster.nodes[2].store.Instance(oldRef)
	if !ok || oldSeed.Status != epaxos.StatusAccepted || oldSeed.Ref.Conf != 1 {
		return semanticTrace{}, fmt.Errorf("old-config seed=%#v durable=%v, want accepted config-1 Ref", oldSeed, ok)
	}
	change := epaxos.ConfChange{Type: epaxos.ConfChangeRemoveVoter, Replica: 3}
	pre, err := snapshot(leader.node, leader.store)
	if err != nil {
		return semanticTrace{}, err
	}
	configRef, proposeErr := leader.node.ProposeConfChange(change)
	configRefView := toRef(configRef)
	changeView := confChangeView{Type: "remove-voter", Replica: 3}
	if err := addAPICall(b, "ConfigProposeA", "propose-config-a", leader, &inputView{Operation: "RawNode.ProposeConfChange", Ref: &configRefView, ConfChange: &changeView}, pre, proposeErr, "", "", ""); err != nil {
		return semanticTrace{}, err
	}
	if proposeErr != nil {
		return semanticTrace{}, proposeErr
	}
	configMetrics := &phaseMetrics{sawAccept: make(map[epaxos.InstanceRef]bool), statuses: make(map[epaxos.InstanceRef]map[epaxos.Status]bool)}
	configAActions := phaseActions{
		ready: "ConfigBuildAReady", persist: "ConfigPersistA", advance: "ConfigAdvanceA", apply: "ConfigApplyA",
		codec: "ConfigBuildAReady", step: "ConfigOldRefResponse", drop: "ConfigWrongConfDrop", frozen: "ConfigFrozenReadyProbe",
	}
	initialConfigMessages, err := consumeReady(b, leader, configAActions, false, configMetrics)
	if err != nil {
		return semanticTrace{}, err
	}
	configActions := phaseActions{ready: "ConfigBuildTransitionReady", persist: "ConfigPersistTransition", advance: "ConfigAdvanceTransition", apply: "ConfigApplyA", codec: "ConfigBuildTransitionReady", step: "ConfigOldRefResponse", drop: "ConfigWrongConfDrop", frozen: "ConfigFrozenReadyProbe"}
	frozen := false
	if err := pumpClusterQueued(b, cluster, configActions, configMetrics, &frozen, initialConfigMessages); err != nil {
		return semanticTrace{}, err
	}
	wantConfig := epaxos.ConfState{ID: 2, Voters: []epaxos.ReplicaID{1, 2}}
	for _, id := range cluster.ids {
		node := cluster.nodes[id]
		status := node.node.Status()
		if !confEqual(status.Conf, wantConfig) {
			return semanticTrace{}, fmt.Errorf("node %d active config=%#v, want %#v", id, status.Conf, wantConfig)
		}
		record, ok := node.store.Instance(configRef)
		if !ok || record.Ref.Conf != 1 || record.Status != epaxos.StatusExecuted || record.ConfChangeResult.Outcome != epaxos.ConfChangeApplied || !confEqual(record.ConfChangeResult.Conf, wantConfig) {
			return semanticTrace{}, fmt.Errorf("node %d config outcome record=%#v ok=%v", id, record, ok)
		}
		state, err := node.store.InitialState()
		if err != nil {
			return semanticTrace{}, err
		}
		initialConfig := epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1, 2, 3}}
		if len(state.ConfigHistory) != 2 ||
			!confEqual(state.ConfigHistory[0].Conf, initialConfig) ||
			state.ConfigHistory[0].AppliedRef != (epaxos.InstanceRef{}) ||
			!confEqual(state.ConfigHistory[1].Conf, wantConfig) ||
			state.ConfigHistory[1].AppliedRef != configRef {
			return semanticTrace{}, fmt.Errorf("node %d config history=%#v, want exact initial and applied successor", id, state.ConfigHistory)
		}
	}
	checkpoint, err := snapshot(leader.node, leader.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := b.add("ConfigApplyA", semanticEvent{Kind: "config-outcome-history-checkpoint", Node: 1, Input: &inputView{Operation: "observe terminal config outcome and MemoryStorage.InitialState history", Ref: &configRefView}, Result: classifyError(nil), ExecutionOrder: checkpoint.Node.Executed, Pre: &checkpoint, Post: &checkpoint}); err != nil {
		return semanticTrace{}, err
	}

	oldCoordinator := cluster.nodes[2]
	oldRecoveryActions := phaseActions{
		ready: "ConfigBuildOldBallotReady", persist: "ConfigPersistOldBallot", advance: "ConfigAdvanceOldBallot", apply: "ConfigApplyA",
		codec: "ConfigBuildOldBallotReady", step: "ConfigOldRefResponse", drop: "ConfigWrongConfDrop", frozen: "ConfigFrozenReadyProbe",
	}
	var oldPrepares []epaxos.Message
	for tick := uint64(1); tick <= 5; tick++ {
		tickPre, err := snapshot(oldCoordinator.node, oldCoordinator.store)
		if err != nil {
			return semanticTrace{}, err
		}
		tickErr := oldCoordinator.node.Tick()
		if err := addAPICall(b, "ConfigStartOldRecovery", "old-config-recovery-tick", oldCoordinator, &inputView{Operation: "RawNode.Tick", Tick: &tick, Ref: new(toRef(oldRef))}, tickPre, tickErr, "", "", ""); err != nil {
			return semanticTrace{}, err
		}
		if tickErr != nil {
			return semanticTrace{}, tickErr
		}
		if oldCoordinator.node.HasReady() {
			messages, err := consumeReady(b, oldCoordinator, oldRecoveryActions, false, nil)
			if err != nil {
				return semanticTrace{}, err
			}
			for _, message := range messages {
				if message.Type == epaxos.MsgPrepare && message.Ref == oldRef {
					oldPrepares = append(oldPrepares, message.Clone())
				}
			}
		}
	}
	if len(oldPrepares) != 2 {
		return semanticTrace{}, fmt.Errorf("old-config recovery Prepare messages=%v, want voters 1 and 3", oldPrepares)
	}
	if err := recordNetworkDrops(b, oldCoordinator, "ConfigAdvanceOldBallot", oldPrepares, map[epaxos.ReplicaID]bool{1: true}, "bounded-old-prepare-loss"); err != nil {
		return semanticTrace{}, err
	}
	var prepareToLeader epaxos.Message
	for _, message := range oldPrepares {
		if message.To == 1 {
			prepareToLeader = message.Clone()
		}
	}
	if prepareToLeader.Type == 0 {
		return semanticTrace{}, fmt.Errorf("old-config recovery omitted Prepare to voter 1")
	}
	if _, err := deliverMessage(b, cluster, prepareToLeader, oldRecoveryActions, "ConfigBuildOldBallotReady", "old-config-prepare-admitted", ""); err != nil {
		return semanticTrace{}, err
	}
	leaderResponses, err := consumeReady(b, leader, oldRecoveryActions, false, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	var oldResponse epaxos.Message
	for _, message := range leaderResponses {
		if message.Type == epaxos.MsgPrepareResp && message.Ref == oldRef && message.To == oldCoordinator.id {
			oldResponse = message.Clone()
		}
	}
	if oldResponse.Type == 0 {
		return semanticTrace{}, fmt.Errorf("old-config Prepare produced no exact response")
	}
	wrongConf := oldResponse.Clone()
	wrongConf.Ref.Conf = 2
	wrongConf.Deps = []epaxos.InstanceNum{0, 0}
	wrongConf.Checksum = epaxos.ChecksumMessage(wrongConf)
	wrongBefore, err := snapshot(oldCoordinator.node, oldCoordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if _, err := deliverMessage(b, cluster, wrongConf, oldRecoveryActions, "ConfigWrongConfDrop", "wrong-config-stutter", "same-coordinates-new-config-ref"); err != nil {
		return semanticTrace{}, err
	}
	wrongAfter, err := snapshot(oldCoordinator.node, oldCoordinator.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if !snapshotEqual(wrongBefore, wrongAfter) {
		return semanticTrace{}, fmt.Errorf("wrong-config response changed active old-ref recovery")
	}
	if _, err := deliverMessage(b, cluster, oldResponse, oldRecoveryActions, "ConfigOldRefResponse", "exact-old-config-response-admitted", ""); err != nil {
		return semanticTrace{}, err
	}
	if !oldCoordinator.node.HasReady() {
		return semanticTrace{}, fmt.Errorf("exact old-config response produced no Accept Ready")
	}
	oldAccepts, err := consumeReady(b, oldCoordinator, oldRecoveryActions, false, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := recordNetworkDrops(b, oldCoordinator, "ConfigAdvanceOldBallot", oldAccepts, map[epaxos.ReplicaID]bool{1: true}, "bounded-old-accept-loss"); err != nil {
		return semanticTrace{}, err
	}
	var acceptToLeader epaxos.Message
	for _, message := range oldAccepts {
		if message.Type == epaxos.MsgAccept && message.To == 1 && message.Ref == oldRef {
			acceptToLeader = message.Clone()
		}
	}
	if acceptToLeader.Type == 0 {
		return semanticTrace{}, fmt.Errorf("old-config recovery omitted Accept to voter 1")
	}
	if _, err := deliverMessage(b, cluster, acceptToLeader, oldRecoveryActions, "ConfigOldRefResponse", "old-config-accept-admitted", ""); err != nil {
		return semanticTrace{}, err
	}
	oldAcceptResponses, err := consumeReady(b, leader, oldRecoveryActions, false, nil)
	if err != nil {
		return semanticTrace{}, err
	}
	var acceptResponse epaxos.Message
	for _, message := range oldAcceptResponses {
		if message.Type == epaxos.MsgAcceptResp && message.To == oldCoordinator.id && message.Ref == oldRef {
			acceptResponse = message.Clone()
		}
	}
	if acceptResponse.Type == 0 {
		return semanticTrace{}, fmt.Errorf("old-config Accept produced no response")
	}
	if _, err := deliverMessage(b, cluster, acceptResponse, oldRecoveryActions, "ConfigOldRefResponse", "old-config-accept-response-admitted", ""); err != nil {
		return semanticTrace{}, err
	}
	for oldCoordinator.node.HasReady() {
		messages, err := consumeReady(b, oldCoordinator, oldRecoveryActions, false, nil)
		if err != nil {
			return semanticTrace{}, err
		}
		if err := recordNetworkDrops(b, oldCoordinator, "ConfigAdvanceOldBallot", messages, nil, "bounded-old-commit-loss"); err != nil {
			return semanticTrace{}, err
		}
	}
	recoveredOld, ok := oldCoordinator.store.Instance(oldRef)
	if !ok || recoveredOld.Status != epaxos.StatusExecuted || recoveredOld.Ref.Conf != 1 {
		return semanticTrace{}, fmt.Errorf("old-config recovery result=%#v durable=%v, want executed config-1 Ref", recoveredOld, ok)
	}

	userCommand := epaxos.Command{ID: epaxos.CommandID{Client: 301, Sequence: 1}, Payload: []byte("new-config"), Footprint: epaxos.Footprint{Points: [][]byte{[]byte("config-order-key")}}}
	userPre, err := snapshot(leader.node, leader.store)
	if err != nil {
		return semanticTrace{}, err
	}
	userRef, userErr := leader.node.Propose(userCommand)
	userRefView := toRef(userRef)
	userCommandView := toCommand(epaxos.EntryCommand, userCommand)
	if err := addAPICall(b, "ConfigProposeB", "propose-user-b", leader, &inputView{Operation: "RawNode.Propose", Ref: &userRefView, Command: &userCommandView}, userPre, userErr, "", "", ""); err != nil {
		return semanticTrace{}, err
	}
	if userErr != nil {
		return semanticTrace{}, userErr
	}
	if userRef.Conf != 2 || configRef.Conf != 1 {
		return semanticTrace{}, fmt.Errorf("configuration pinning A=%s B=%s, want configs 1 then 2", configRef, userRef)
	}
	userActions := phaseActions{ready: "ConfigBuildBReady", persist: "ConfigPersistB", advance: "ConfigAdvanceB", apply: "ConfigApplyB", codec: "ConfigDependencyBlocked", step: "ConfigBuildBReady", drop: "ConfigWrongConfDrop", frozen: "ConfigFrozenReadyProbe"}
	userMetrics := &phaseMetrics{sawAccept: make(map[epaxos.InstanceRef]bool), statuses: make(map[epaxos.InstanceRef]map[epaxos.Status]bool)}
	if err := pumpCluster(b, cluster, userActions, userMetrics, nil); err != nil {
		return semanticTrace{}, err
	}
	for _, id := range []epaxos.ReplicaID{1, 2} {
		record, ok := cluster.nodes[id].store.Instance(userRef)
		if !ok || record.Ref.Conf != 2 || record.Status != epaxos.StatusExecuted {
			return semanticTrace{}, fmt.Errorf("node %d new-config record=%#v ok=%v", id, record, ok)
		}
		status := cluster.nodes[id].node.Status()
		configIndex, userIndex := -1, -1
		for i, executed := range status.Executed {
			if executed == configRef {
				configIndex = i
			}
			if executed == userRef {
				userIndex = i
			}
		}
		if configIndex < 0 || userIndex < 0 || configIndex >= userIndex {
			return semanticTrace{}, fmt.Errorf("node %d execution order=%v does not place config A before user B", id, status.Executed)
		}
	}
	finalSnapshot, err := snapshot(leader.node, leader.store)
	if err != nil {
		return semanticTrace{}, err
	}
	if err := b.add("ConfigApplyB", semanticEvent{Kind: "configuration-pinning-final", Node: 1, Input: &inputView{Operation: "observe A@conf1 before B@conf2", Ref: &userRefView}, Result: classifyError(nil), ExecutionOrder: finalSnapshot.Node.Executed, Pre: &finalSnapshot, Post: &finalSnapshot}); err != nil {
		return semanticTrace{}, err
	}
	return b.finish(), nil
}
