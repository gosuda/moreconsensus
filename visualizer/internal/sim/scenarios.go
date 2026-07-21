package sim

import (
	"fmt"
	"strconv"
)

var scenarioCatalog = []ScenarioMeta{
	{
		ID:         "parallel",
		Title:      "Coordinate anywhere",
		Lede:       "Two replicas coordinate disjoint keys at the same time. Neither command waits for a global leader.",
		Completion: "CHECKED · 2 coordinators · 0 cross-dependencies",
	},
	{
		ID:         "fast-path",
		Title:      "One round to commit",
		Lede:       "Matching ordering attributes let a fast quorum commit directly after PreAccept—there is no Accept round.",
		Completion: "CHECKED · committed without Accept",
	},
	{
		ID:         "conflict-cycle",
		Title:      "Conflict, then converge",
		Lede:       "Concurrent writes become dependencies. A cycle is executed in the same deterministic order at every replica.",
		Completion: "CHECKED · one cycle · one apply order",
	},
	{
		ID:         "recovery",
		Title:      "Recovery has no owner",
		Lede:       "When an instance owner stops, another replica gathers evidence and finishes the chosen value.",
		Completion: "CHECKED · owner paused · value recovered",
	},
}

// Catalog returns the stable guided-scenario catalog in presentation order.
func Catalog() []ScenarioMeta {
	return append([]ScenarioMeta(nil), scenarioCatalog...)
}

// BuildScenario generates a guided trace by dispatching ordinary simulator actions.
func BuildScenario(id string) (ScenarioTrace, error) {
	var (
		trace ScenarioTrace
		err   error
	)
	switch id {
	case "parallel":
		trace, err = buildParallel()
	case "fast-path":
		trace, err = buildFastPath()
	case "conflict-cycle":
		trace, err = buildConflictCycle()
	case "recovery":
		trace, err = buildRecovery()
	default:
		return ScenarioTrace{}, actionError(CodeNotFound, "That guided scenario does not exist.")
	}
	if err != nil {
		return ScenarioTrace{}, internalError("The guided scenario could not be generated.", err)
	}
	return trace, nil
}

type scenarioBuilder struct {
	meta    ScenarioMeta
	session *Session
	frames  []Frame
	current Frame
}

func newScenarioBuilder(meta ScenarioMeta) (*scenarioBuilder, error) {
	session, err := NewSession(3)
	if err != nil {
		return nil, err
	}
	frame, err := session.Seek(0)
	if err != nil {
		return nil, err
	}
	frame.Focus = &Focus{Replica: 1}
	return &scenarioBuilder{meta: meta, session: session, frames: []Frame{frame}, current: frame}, nil
}

func (b *scenarioBuilder) dispatch(action Action) (Frame, error) {
	frame, err := b.session.Dispatch(action)
	if err != nil {
		return Frame{}, fmt.Errorf("dispatch %#v: %w", action, err)
	}
	frame.Focus = focusForFrame(frame)
	b.frames = append(b.frames, frame)
	b.current = frame
	return frame, nil
}

func (b *scenarioBuilder) deliver(messageType string, from, to uint64, ref string) (Frame, error) {
	for _, message := range b.current.Snapshot.Messages {
		if message.Type == messageType && message.From == from && message.To == to && message.Ref == ref {
			return b.dispatch(Action{Kind: "deliver", Envelope: message.ID})
		}
	}
	return Frame{}, fmt.Errorf("queued tuple %s R%d->R%d %s not found", messageType, from, to, ref)
}

func (b *scenarioBuilder) drain() error {
	for len(b.frames) < maxHistoryActions {
		var next *MessageView
		for _, message := range b.current.Snapshot.Messages {
			if !message.Blocked {
				selected := message
				next = &selected
				break
			}
		}
		if next == nil {
			return nil
		}
		if _, err := b.dispatch(Action{Kind: "deliver", Envelope: next.ID}); err != nil {
			return fmt.Errorf("drain %s R%d->R%d %s envelope %s: %w", next.Type, next.From, next.To, next.Ref, next.ID, err)
		}
	}
	return fmt.Errorf("scenario exceeded %d frames", maxHistoryActions)
}

func (b *scenarioBuilder) drainUntil(done func(Frame) bool) error {
	for len(b.frames) < maxHistoryActions {
		if done(b.current) {
			return nil
		}
		var next *MessageView
		for _, message := range b.current.Snapshot.Messages {
			if !message.Blocked && !messageTargetsExecuted(b.current, message) {
				selected := message
				next = &selected
				break
			}
		}
		if next == nil {
			return fmt.Errorf("scenario queue blocked before its invariant held")
		}
		if _, err := b.dispatch(Action{Kind: "deliver", Envelope: next.ID}); err != nil {
			return fmt.Errorf("drain %s R%d->R%d %s envelope %s: %w", next.Type, next.From, next.To, next.Ref, next.ID, err)
		}
	}
	return fmt.Errorf("scenario exceeded %d frames", maxHistoryActions)
}

func messageTargetsExecuted(frame Frame, message MessageView) bool {
	if message.To == 0 || message.To > uint64(len(frame.Snapshot.Replicas)) {
		return false
	}
	instance, ok := instanceByRef(frame.Snapshot.Replicas[message.To-1], message.Ref)
	return ok && instance.Status == "executed"
}

func (b *scenarioBuilder) ref(sequence int) (string, error) {
	id := "1:" + strconv.Itoa(sequence)
	for _, command := range b.current.Snapshot.Commands {
		if command.ID == id {
			return command.Ref, nil
		}
	}
	return "", fmt.Errorf("command %s has no instance ref", id)
}

func (b *scenarioBuilder) trace() ScenarioTrace {
	return ScenarioTrace{Scenario: b.meta, Frames: b.frames}
}

func buildParallel() (ScenarioTrace, error) {
	b, err := newScenarioBuilder(scenarioCatalog[0])
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "propose", Replica: 1, Key: "east", Value: "7"}); err != nil {
		return ScenarioTrace{}, err
	}
	eastRef, err := b.ref(1)
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "propose", Replica: 3, Key: "west", Value: "9"}); err != nil {
		return ScenarioTrace{}, err
	}
	westRef, err := b.ref(2)
	if err != nil {
		return ScenarioTrace{}, err
	}
	if err = b.drain(); err != nil {
		return ScenarioTrace{}, err
	}
	if err = requireExecutedEverywhere(b.current, eastRef, westRef); err != nil {
		return ScenarioTrace{}, err
	}
	if traceHasMessageType(b.frames, "accept") || traceHasMessageType(b.frames, "accept-resp") {
		return ScenarioTrace{}, fmt.Errorf("parallel trace entered Accept")
	}
	for _, replica := range b.current.Snapshot.Replicas {
		for _, instance := range replica.Instances {
			if instance.Ref == eastRef && contains(instance.Edges, westRef) {
				return ScenarioTrace{}, fmt.Errorf("%s depends on %s at R%d", eastRef, westRef, replica.ID)
			}
			if instance.Ref == westRef && contains(instance.Edges, eastRef) {
				return ScenarioTrace{}, fmt.Errorf("%s depends on %s at R%d", westRef, eastRef, replica.ID)
			}
		}
	}
	indices := []milestone{
		{index: 2, headline: "No permanent leader", explanation: "R1 and R3 each coordinate the command they received.", focus: Focus{Replica: 3, Ref: westRef}},
		{index: firstFrameWithBothRefs(b.frames, eastRef, westRef, 3), headline: "Different keys, no dependencies", explanation: "The point footprints do not overlap, so neither instance points to the other.", focus: Focus{Replica: 2, Ref: eastRef}},
		{index: firstFrameAllExecuted(b.frames, eastRef, westRef), headline: "Both take the fast path", explanation: "Each matching fast quorum commits without Accept.", focus: Focus{Replica: 1, Ref: eastRef}},
	}
	if err = applyMilestones(b.frames, indices); err != nil {
		return ScenarioTrace{}, err
	}
	return b.trace(), nil
}

func buildFastPath() (ScenarioTrace, error) {
	b, err := newScenarioBuilder(scenarioCatalog[1])
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "propose", Replica: 1, Key: "profile", Value: "ready"}); err != nil {
		return ScenarioTrace{}, err
	}
	ref, err := b.ref(1)
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept", 1, 2, ref); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept-resp", 2, 1, ref); err != nil {
		return ScenarioTrace{}, err
	}
	if err = b.drain(); err != nil {
		return ScenarioTrace{}, err
	}
	for _, required := range []string{"pre-accept", "pre-accept-resp", "commit"} {
		if !traceHasMessageType(b.frames, required) {
			return ScenarioTrace{}, fmt.Errorf("fast-path trace has no %s", required)
		}
	}
	if traceHasMessageType(b.frames, "accept") || traceHasMessageType(b.frames, "accept-resp") {
		return ScenarioTrace{}, fmt.Errorf("fast-path trace entered Accept")
	}
	if err = requireExecutedEverywhere(b.current, ref); err != nil {
		return ScenarioTrace{}, err
	}
	for _, replica := range b.current.Snapshot.Replicas {
		if value, ok := stateValue(replica, "profile"); !ok || value != "ready" {
			return ScenarioTrace{}, fmt.Errorf("replica %d profile state = %q, %v", replica.ID, value, ok)
		}
	}
	indices := []milestone{
		{index: 1, headline: "R1 owns this instance", explanation: "Ownership belongs to this command, not to a permanent leadership role.", focus: Focus{Replica: 1, Ref: ref}},
		{index: firstDeliveredType(b.frames, "pre-accept-resp", ref), headline: "A fast quorum matches", explanation: "R1 and R2 report the same sequence and dependencies.", focus: Focus{Replica: 1, Ref: ref}},
		{index: firstDeliveredType(b.frames, "commit", ref), headline: "Accept is skipped", explanation: "Commit follows PreAccept directly.", focus: Focus{Replica: 2, Ref: ref}},
	}
	if err = applyMilestones(b.frames, indices); err != nil {
		return ScenarioTrace{}, err
	}
	return b.trace(), nil
}

func buildConflictCycle() (ScenarioTrace, error) {
	b, err := newScenarioBuilder(scenarioCatalog[2])
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "propose", Replica: 1, Key: "cart", Value: "reserved"}); err != nil {
		return ScenarioTrace{}, err
	}
	firstRef, err := b.ref(1)
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "propose", Replica: 2, Key: "cart", Value: "paid"}); err != nil {
		return ScenarioTrace{}, err
	}
	secondRef, err := b.ref(2)
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept", 1, 2, firstRef); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept", 2, 1, secondRef); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept-resp", 2, 1, firstRef); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept-resp", 1, 2, secondRef); err != nil {
		return ScenarioTrace{}, err
	}
	if err = b.drainUntil(func(frame Frame) bool {
		return requireExecutedEverywhere(frame, firstRef, secondRef) == nil
	}); err != nil {
		return ScenarioTrace{}, err
	}
	if !traceHasMessageType(b.frames, "accept") || !traceHasMessageType(b.frames, "accept-resp") {
		return ScenarioTrace{}, fmt.Errorf("conflict trace did not complete Accept")
	}
	if err = requireExecutedEverywhere(b.current, firstRef, secondRef); err != nil {
		return ScenarioTrace{}, err
	}
	for _, replica := range b.current.Snapshot.Replicas {
		first, ok := instanceByRef(replica, firstRef)
		if !ok || !contains(first.Edges, secondRef) {
			return ScenarioTrace{}, fmt.Errorf("replica %d %s does not depend on %s", replica.ID, firstRef, secondRef)
		}
		second, ok := instanceByRef(replica, secondRef)
		if !ok || !contains(second.Edges, firstRef) {
			return ScenarioTrace{}, fmt.Errorf("replica %d %s does not depend on %s", replica.ID, secondRef, firstRef)
		}
		if value, exists := stateValue(replica, "cart"); !exists || value != "paid" {
			return ScenarioTrace{}, fmt.Errorf("replica %d cart state = %q, %v", replica.ID, value, exists)
		}
	}
	order := appliedRefs(b.current.Snapshot.Replicas[0])
	for _, replica := range b.current.Snapshot.Replicas[1:] {
		if !equalStrings(order, appliedRefs(replica)) {
			return ScenarioTrace{}, fmt.Errorf("replica %d apply order differs", replica.ID)
		}
	}
	indices := []milestone{
		{index: 2, headline: "Same key, different arrival order", explanation: "R1 and R2 each know a conflicting local command first.", focus: Focus{Replica: 2, Ref: secondRef}},
		{index: firstFrameWithReciprocalEdges(b.frames, firstRef, secondRef), headline: "Attributes diverge", explanation: "Each PreAccept reply adds the other instance as a dependency.", focus: Focus{Replica: 1, Ref: firstRef}},
		{index: firstSentType(b.frames, "accept", ""), headline: "Accept chooses merged attributes", explanation: "The coordinator takes the majority path when fast-path attributes differ.", focus: Focus{Replica: 1, Ref: firstRef}},
		{index: firstFrameAllExecuted(b.frames, firstRef, secondRef), headline: "The cycle becomes an order", explanation: "Seq, ORDER, then instance ref give every replica the same apply order.", focus: Focus{Replica: 2, Ref: secondRef}},
	}
	if err = applyMilestones(b.frames, indices); err != nil {
		return ScenarioTrace{}, err
	}
	return b.trace(), nil
}

func buildRecovery() (ScenarioTrace, error) {
	b, err := newScenarioBuilder(scenarioCatalog[3])
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "propose", Replica: 1, Key: "order", Value: "created"}); err != nil {
		return ScenarioTrace{}, err
	}
	firstRef, err := b.ref(1)
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept", 1, 2, firstRef); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept", 1, 3, firstRef); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "pause", Replica: 1}); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.dispatch(Action{Kind: "propose", Replica: 2, Key: "order", Value: "confirmed"}); err != nil {
		return ScenarioTrace{}, err
	}
	secondRef, err := b.ref(2)
	if err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept", 2, 3, secondRef); err != nil {
		return ScenarioTrace{}, err
	}
	if _, err = b.deliver("pre-accept-resp", 3, 2, secondRef); err != nil {
		return ScenarioTrace{}, err
	}
	for range 6 {
		if _, err = b.dispatch(Action{Kind: "tick", Replica: 2}); err != nil {
			return ScenarioTrace{}, err
		}
	}
	if err = b.drainUntil(func(frame Frame) bool {
		second := frame.Snapshot.Replicas[1]
		third := frame.Snapshot.Replicas[2]
		return countApplied(second, firstRef) == 1 && countApplied(second, secondRef) == 1 &&
			countApplied(third, firstRef) == 1 && countApplied(third, secondRef) == 1
	}); err != nil {
		return ScenarioTrace{}, err
	}
	beforeResume := len(b.frames) - 1
	if !traceHasMessageType(b.frames, "prepare") || !traceHasMessageType(b.frames, "prepare-resp") {
		return ScenarioTrace{}, fmt.Errorf("recovery trace emitted no Prepare exchange")
	}
	for _, replicaID := range []uint64{2, 3} {
		replica := b.current.Snapshot.Replicas[replicaID-1]
		if countApplied(replica, firstRef) != 1 || countApplied(replica, secondRef) != 1 {
			return ScenarioTrace{}, fmt.Errorf("replica %d did not apply each command once before owner return", replicaID)
		}
	}
	if _, err = b.dispatch(Action{Kind: "resume", Replica: 1}); err != nil {
		return ScenarioTrace{}, err
	}
	if err = b.drainUntil(func(frame Frame) bool {
		return requireExecutedEverywhere(frame, firstRef, secondRef) == nil
	}); err != nil {
		return ScenarioTrace{}, err
	}
	for _, replica := range b.current.Snapshot.Replicas {
		if countApplied(replica, firstRef) != 1 || countApplied(replica, secondRef) != 1 {
			return ScenarioTrace{}, fmt.Errorf("replica %d application counts are not exactly one", replica.ID)
		}
		if value, ok := stateValue(replica, "order"); !ok || value != "confirmed" {
			return ScenarioTrace{}, fmt.Errorf("replica %d order state = %q, %v", replica.ID, value, ok)
		}
	}
	indices := []milestone{
		{index: firstCause(b.frames, "pause"), headline: "The owner disappears", explanation: "R1 stops after other replicas persist its PreAccept.", focus: Focus{Replica: 1, Ref: firstRef}},
		{index: firstDependencyBlocked(b.frames, 2, secondRef, firstRef), headline: "A dependency blocks execution", explanation: "R2 cannot apply the conflicting successor first.", focus: Focus{Replica: 2, Ref: secondRef}},
		{index: firstSentType(b.frames, "prepare", firstRef), headline: "R2 raises a recovery ballot", explanation: "A healthy replica gathers Prepare evidence for the stalled instance.", focus: Focus{Replica: 2, Ref: firstRef}},
		{index: firstHealthyAppliedBoth(b.frames, beforeResume, firstRef, secondRef), headline: "The value survives its owner", explanation: "The quorum finishes both commands before R1 returns.", focus: Focus{Replica: 2, Ref: firstRef}},
	}
	if err = applyMilestones(b.frames, indices); err != nil {
		return ScenarioTrace{}, err
	}
	return b.trace(), nil
}

type milestone struct {
	index       int
	headline    string
	explanation string
	focus       Focus
}

func applyMilestones(frames []Frame, milestones []milestone) error {
	previous := 0
	for i, mark := range milestones {
		if mark.index <= 0 || mark.index >= len(frames) || mark.index < previous {
			return fmt.Errorf("milestone %d has invalid frame %d", i, mark.index)
		}
		end := len(frames)
		if i+1 < len(milestones) {
			end = milestones[i+1].index
		}
		for frameIndex := mark.index; frameIndex < end; frameIndex++ {
			frames[frameIndex].Headline = mark.headline
			frames[frameIndex].Explanation = mark.explanation
			focus := mark.focus
			frames[frameIndex].Focus = &focus
		}
		previous = mark.index
	}
	return nil
}

func focusForFrame(frame Frame) *Focus {
	focus := Focus{Replica: frame.Cause.Replica}
	for _, event := range frame.Events {
		if event.Replica != 0 {
			focus.Replica = event.Replica
		}
		if event.Message != nil {
			focus.Envelope = event.Message.ID
			focus.Ref = event.Message.Ref
		}
		if event.Record != nil {
			focus.Ref = event.Record.Ref
		}
		if event.Command != nil {
			focus.Ref = event.Command.Ref
		}
	}
	if focus.Replica == 0 && focus.Ref == "" && focus.Envelope == "" {
		focus.Replica = 1
	}
	return &focus
}

func traceHasMessageType(frames []Frame, messageType string) bool {
	return firstSentType(frames, messageType, "") > 0 || firstDeliveredType(frames, messageType, "") > 0
}

func firstSentType(frames []Frame, messageType, ref string) int {
	for index, frame := range frames {
		for _, event := range frame.Events {
			if event.Kind == "sent" && event.Message != nil && event.Message.Type == messageType && (ref == "" || event.Message.Ref == ref) {
				return index
			}
		}
	}
	return -1
}

func firstDeliveredType(frames []Frame, messageType, ref string) int {
	for index, frame := range frames {
		for _, event := range frame.Events {
			if event.Kind == "delivered" && event.Message != nil && event.Message.Type == messageType && (ref == "" || event.Message.Ref == ref) {
				return index
			}
		}
	}
	return -1
}

func firstCause(frames []Frame, kind string) int {
	for index, frame := range frames {
		if frame.Cause.Kind == kind {
			return index
		}
	}
	return -1
}

func firstFrameWithBothRefs(frames []Frame, firstRef, secondRef string, replicaID uint64) int {
	for index, frame := range frames {
		replica := frame.Snapshot.Replicas[replicaID-1]
		_, first := instanceByRef(replica, firstRef)
		_, second := instanceByRef(replica, secondRef)
		if first && second {
			return index
		}
	}
	return -1
}

func firstFrameWithReciprocalEdges(frames []Frame, firstRef, secondRef string) int {
	for index, frame := range frames {
		firstFound := false
		secondFound := false
		for _, replica := range frame.Snapshot.Replicas {
			if instance, ok := instanceByRef(replica, firstRef); ok && contains(instance.Edges, secondRef) {
				firstFound = true
			}
			if instance, ok := instanceByRef(replica, secondRef); ok && contains(instance.Edges, firstRef) {
				secondFound = true
			}
		}
		if firstFound && secondFound {
			return index
		}
	}
	return -1
}

func firstFrameAllExecuted(frames []Frame, refs ...string) int {
	for index, frame := range frames {
		if requireExecutedEverywhere(frame, refs...) == nil {
			return index
		}
	}
	return -1
}

func firstDependencyBlocked(frames []Frame, replicaID uint64, ref, dependency string) int {
	for index, frame := range frames {
		replica := frame.Snapshot.Replicas[replicaID-1]
		instance, ok := instanceByRef(replica, ref)
		if ok && contains(instance.Edges, dependency) && countApplied(replica, ref) == 0 {
			return index
		}
	}
	return -1
}

func firstHealthyAppliedBoth(frames []Frame, end int, firstRef, secondRef string) int {
	for index, frame := range frames {
		if index > end {
			break
		}
		second := frame.Snapshot.Replicas[1]
		third := frame.Snapshot.Replicas[2]
		if countApplied(second, firstRef) == 1 && countApplied(second, secondRef) == 1 &&
			countApplied(third, firstRef) == 1 && countApplied(third, secondRef) == 1 {
			return index
		}
	}
	return -1
}

func requireExecutedEverywhere(frame Frame, refs ...string) error {
	for _, replica := range frame.Snapshot.Replicas {
		for _, ref := range refs {
			if countApplied(replica, ref) != 1 {
				return fmt.Errorf("replica %d did not execute %s exactly once", replica.ID, ref)
			}
		}
	}
	return nil
}

func instanceByRef(replica ReplicaView, ref string) (InstanceView, bool) {
	for _, instance := range replica.Instances {
		if instance.Ref == ref {
			return instance, true
		}
	}
	return InstanceView{}, false
}

func countApplied(replica ReplicaView, ref string) int {
	count := 0
	for _, applied := range replica.Applied {
		if applied.Ref == ref {
			count++
		}
	}
	return count
}

func stateValue(replica ReplicaView, key string) (string, bool) {
	for _, item := range replica.State {
		if item.Key == key {
			return item.Value, true
		}
	}
	return "", false
}

func appliedRefs(replica ReplicaView) []string {
	refs := make([]string, len(replica.Applied))
	for i := range replica.Applied {
		refs[i] = replica.Applied[i].Ref
	}
	return refs
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
