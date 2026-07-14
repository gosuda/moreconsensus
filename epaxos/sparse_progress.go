package epaxos

import "sort"

const (
	defaultMaxDependencyRecoveriesPerDrive = 8
	defaultMaxConcurrentRecoveries         = 64
)

type instanceLane struct {
	conf    ConfID
	replica ReplicaID
}

func laneFor(ref InstanceRef) instanceLane {
	return instanceLane{conf: ref.Conf, replica: ref.Replica}
}

func instanceSuccessor(value InstanceNum) (InstanceNum, bool) {
	if value == ^InstanceNum(0) {
		return 0, false
	}
	return value + 1, true
}

type executedTracker struct {
	exact   map[InstanceRef]struct{}
	through map[instanceLane]InstanceNum
}

func newExecutedTracker() executedTracker {
	return executedTracker{
		exact:   make(map[InstanceRef]struct{}),
		through: make(map[instanceLane]InstanceNum),
	}
}

func (tracker *executedTracker) contains(ref InstanceRef) bool {
	if tracker == nil || tracker.exact == nil {
		return false
	}
	_, ok := tracker.exact[ref]
	return ok
}

func (tracker *executedTracker) prefix(lane instanceLane) InstanceNum {
	if tracker == nil || tracker.through == nil {
		return 0
	}
	return tracker.through[lane]
}

func (tracker *executedTracker) add(ref InstanceRef) {
	if tracker == nil || ref.IsZero() || ref.Instance == 0 {
		return
	}
	if tracker.exact == nil {
		tracker.exact = make(map[InstanceRef]struct{})
	}
	if tracker.through == nil {
		tracker.through = make(map[instanceLane]InstanceNum)
	}
	tracker.exact[ref] = struct{}{}
	lane := laneFor(ref)
	through := tracker.through[lane]
	for {
		next, ok := instanceSuccessor(through)
		if !ok {
			return
		}
		if _, present := tracker.exact[InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: next}]; !present {
			return
		}
		tracker.through[lane] = next
		through = next
	}
}

type refRange struct {
	first int
	last  int
}

type executionView struct {
	refs  []InstanceRef
	lanes map[instanceLane]refRange
}

type dependencyIter struct {
	n       *RawNode
	view    *executionView
	base    InstanceRef
	deps    []InstanceNum
	voters  []ReplicaID
	slot    int
	current int
	end     int
}

// dependencyRefs iterates only materialized records covered by base's compact
// dependency prefixes. Missing logical slots remain semantic obligations and
// are checked separately by firstPrefixBlocker; they are never synthesized here.
func (view *executionView) dependencyRefs(n *RawNode, base InstanceRef) dependencyIter {
	iter := dependencyIter{n: n, view: view, base: base}
	if inst := n.instances[base]; inst != nil {
		iter.deps = inst.rec.Deps
		iter.voters = n.confFor(base.Conf).Voters
	}
	return iter
}

func (iter *dependencyIter) next() (InstanceRef, bool) {
	for {
		for iter.current < iter.end {
			ref := iter.view.refs[iter.current]
			iter.current++
			if ref != iter.base {
				return ref, true
			}
		}
		if iter.slot >= len(iter.deps) || iter.slot >= len(iter.voters) {
			return InstanceRef{}, false
		}
		through := iter.deps[iter.slot]
		replica := iter.voters[iter.slot]
		iter.slot++
		if through == 0 {
			continue
		}
		window, ok := iter.view.lanes[instanceLane{conf: iter.base.Conf, replica: replica}]
		if !ok {
			continue
		}
		iter.current = window.first
		width := window.last - window.first
		iter.end = window.first + sort.Search(width, func(offset int) bool {
			return iter.view.refs[window.first+offset].Instance > through
		})
	}
}

type recoverySourceKind uint8

const (
	recoverySourcePrefix recoverySourceKind = iota + 1
	recoverySourceConflict
)

type recoverySource struct {
	kind     recoverySourceKind
	base     InstanceRef
	lane     instanceLane
	conflict InstanceRef
}

type recoveryCandidate struct {
	source recoverySource
	ref    InstanceRef
}

type recoveryResult uint8

const (
	recoveryResolved recoveryResult = iota
	recoveryInFlight
	recoveryDeferred
	recoveryStarted
)

type tarjanFrame struct {
	vertex       InstanceRef
	iter         dependencyIter
	entered      bool
	pendingChild InstanceRef
	hasChild     bool
}

type executionWorkspace struct {
	refs             []InstanceRef
	lanes            map[instanceLane]refRange
	index            map[InstanceRef]int
	low              map[InstanceRef]int
	onStack          map[InstanceRef]bool
	tarjanStack      []InstanceRef
	dfs              []tarjanFrame
	componentStorage []InstanceRef
	components       [][]InstanceRef
	inside           map[InstanceRef]struct{}
	readinessRefs    []InstanceRef
	candidates       []recoveryCandidate
}

func (n *RawNode) installInstance(inst *instance) {
	if inst == nil {
		return
	}
	replaced := n.instances[inst.rec.Ref]
	if replaced != nil && replaced != inst {
		n.cancelTimersForRef(inst.rec.Ref)
		releaseInstanceVolatile(replaced)
	}
	n.dropTryEvidenceChecksForTarget(inst.rec.Ref)
	var previous *InstanceRecord
	if replaced != nil {
		previous = &replaced.rec
	}
	n.engine.apply(previous, inst.rec)
	n.instances[inst.rec.Ref] = inst
	if inst.rec.Status >= StatusCommitted {
		n.noteCommitted(inst.rec.Ref)
		releaseInstanceVolatile(inst)
	}
	if inst.rec.Status == StatusExecuted {
		n.executed.add(inst.rec.Ref)
	}
}

func (n *RawNode) noteCommitted(ref InstanceRef) {
	if ref.IsZero() || ref.Instance == 0 {
		return
	}
	if n.committedThrough == nil {
		n.committedThrough = make(map[instanceLane]InstanceNum)
	}
	lane := laneFor(ref)
	through := n.committedThrough[lane]
	if ref.Instance <= through {
		return
	}
	next, ok := instanceSuccessor(through)
	if !ok || ref.Instance != next {
		return
	}
	for {
		candidate := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: next}
		inst := n.instances[candidate]
		if inst == nil || inst.rec.Status < StatusCommitted {
			return
		}
		n.committedThrough[lane] = next
		through = next
		next, ok = instanceSuccessor(through)
		if !ok {
			return
		}
	}
}

func (n *RawNode) newExecutionView() executionView {
	workspace := &n.executionWorkspace
	if cap(workspace.refs) < len(n.instances) {
		workspace.refs = make([]InstanceRef, 0, len(n.instances))
	} else {
		workspace.refs = workspace.refs[:0]
	}
	for ref := range n.instances {
		workspace.refs = append(workspace.refs, ref)
	}
	sortRefs(workspace.refs)
	if workspace.lanes == nil {
		workspace.lanes = make(map[instanceLane]refRange)
	} else {
		clear(workspace.lanes)
	}
	for first := 0; first < len(workspace.refs); {
		lane := laneFor(workspace.refs[first])
		last := first + 1
		for last < len(workspace.refs) && laneFor(workspace.refs[last]) == lane {
			last++
		}
		workspace.lanes[lane] = refRange{first: first, last: last}
		first = last
	}
	return executionView{refs: workspace.refs, lanes: workspace.lanes}
}

func (n *RawNode) executionComponents(view *executionView) [][]InstanceRef {
	workspace := &n.executionWorkspace
	if workspace.index == nil {
		workspace.index = make(map[InstanceRef]int)
		workspace.low = make(map[InstanceRef]int)
		workspace.onStack = make(map[InstanceRef]bool)
	} else {
		clear(workspace.index)
		clear(workspace.low)
		clear(workspace.onStack)
	}
	workspace.tarjanStack = workspace.tarjanStack[:0]
	clear(workspace.dfs[:cap(workspace.dfs)])
	workspace.dfs = workspace.dfs[:0]
	if cap(workspace.componentStorage) < len(view.refs) {
		workspace.componentStorage = make([]InstanceRef, 0, len(view.refs))
	} else {
		workspace.componentStorage = workspace.componentStorage[:0]
	}
	clear(workspace.components[:cap(workspace.components)])
	workspace.components = workspace.components[:0]

	nextIndex := 0
	for _, root := range view.refs {
		rootInst := n.instances[root]
		if rootInst == nil || rootInst.rec.Status < StatusCommitted || n.executed.contains(root) {
			continue
		}
		if _, seen := workspace.index[root]; seen {
			continue
		}
		workspace.dfs = append(workspace.dfs, tarjanFrame{vertex: root})
		for len(workspace.dfs) > 0 {
			frameIndex := len(workspace.dfs) - 1
			frame := &workspace.dfs[frameIndex]
			if !frame.entered {
				workspace.index[frame.vertex] = nextIndex
				workspace.low[frame.vertex] = nextIndex
				nextIndex++
				workspace.tarjanStack = append(workspace.tarjanStack, frame.vertex)
				workspace.onStack[frame.vertex] = true
				frame.iter = view.dependencyRefs(n, frame.vertex)
				frame.entered = true
			}
			if frame.hasChild {
				if workspace.low[frame.pendingChild] < workspace.low[frame.vertex] {
					workspace.low[frame.vertex] = workspace.low[frame.pendingChild]
				}
				frame.pendingChild = InstanceRef{}
				frame.hasChild = false
			}

			advanced := false
			for {
				dependency, ok := frame.iter.next()
				if !ok {
					break
				}
				if n.executed.contains(dependency) {
					continue
				}
				dependencyInst := n.instances[dependency]
				if dependencyInst == nil || dependencyInst.rec.Status < StatusCommitted || n.dependencyKnownAfter(frame.vertex, dependency, StatusCommitted) {
					continue
				}
				if _, seen := workspace.index[dependency]; !seen {
					frame.pendingChild = dependency
					frame.hasChild = true
					workspace.dfs = append(workspace.dfs, tarjanFrame{vertex: dependency})
					advanced = true
					break
				}
				if workspace.onStack[dependency] && workspace.index[dependency] < workspace.low[frame.vertex] {
					workspace.low[frame.vertex] = workspace.index[dependency]
				}
			}
			if advanced {
				continue
			}

			vertex := frame.vertex
			if workspace.low[vertex] == workspace.index[vertex] {
				first := len(workspace.componentStorage)
				for {
					last := len(workspace.tarjanStack) - 1
					member := workspace.tarjanStack[last]
					workspace.tarjanStack = workspace.tarjanStack[:last]
					workspace.onStack[member] = false
					workspace.componentStorage = append(workspace.componentStorage, member)
					if member == vertex {
						break
					}
				}
				workspace.components = append(workspace.components, workspace.componentStorage[first:])
			}
			workspace.dfs[frameIndex] = tarjanFrame{}
			workspace.dfs = workspace.dfs[:frameIndex]
		}
	}
	clear(workspace.dfs[:cap(workspace.dfs)])
	return workspace.components
}

func (n *RawNode) firstPrefixBlocker(base InstanceRef, lane instanceLane, through InstanceNum, inside map[InstanceRef]struct{}) (bool, InstanceRef) {
	if through == 0 || n.executed.prefix(lane) >= through {
		return false, InstanceRef{}
	}
	current, ok := instanceSuccessor(n.executed.prefix(lane))
	if !ok {
		return false, InstanceRef{}
	}
	for {
		ref := InstanceRef{Conf: lane.conf, Replica: lane.replica, Instance: current}
		discharged := ref == base
		if !discharged {
			_, discharged = inside[ref]
		}
		if !discharged {
			discharged = n.executed.contains(ref)
		}
		if !discharged {
			inst := n.instances[ref]
			if inst == nil || inst.rec.Status < StatusCommitted {
				return true, ref
			}
			if !n.dependencyKnownAfter(base, ref, StatusCommitted) {
				return true, InstanceRef{}
			}
		}
		if current == through {
			return false, InstanceRef{}
		}
		current++ // Safe because current < through.
	}
}

func (n *RawNode) componentReady(view *executionView, component []InstanceRef, candidates *[]recoveryCandidate) bool {
	workspace := &n.executionWorkspace
	if workspace.inside == nil {
		workspace.inside = make(map[InstanceRef]struct{}, len(component))
	} else {
		clear(workspace.inside)
	}
	for _, ref := range component {
		workspace.inside[ref] = struct{}{}
	}
	if cap(workspace.readinessRefs) < len(component) {
		workspace.readinessRefs = make([]InstanceRef, len(component))
	} else {
		workspace.readinessRefs = workspace.readinessRefs[:len(component)]
	}
	copy(workspace.readinessRefs, component)
	sortRefs(workspace.readinessRefs)

	ready := true
	for _, base := range workspace.readinessRefs {
		if n.hasUnresolvedKnownConflict(view, base, workspace.inside, candidates) {
			ready = false
		}
		inst := n.instances[base]
		if inst == nil {
			ready = false
			continue
		}
		conf := n.confFor(base.Conf)
		for slot, through := range inst.rec.Deps {
			if through == 0 || slot >= len(conf.Voters) {
				continue
			}
			lane := instanceLane{conf: base.Conf, replica: conf.Voters[slot]}
			blocked, recoverRef := n.firstPrefixBlocker(base, lane, through, workspace.inside)
			if !blocked {
				continue
			}
			ready = false
			if !recoverRef.IsZero() {
				*candidates = append(*candidates, recoveryCandidate{
					source: recoverySource{kind: recoverySourcePrefix, base: base, lane: lane},
					ref:    recoverRef,
				})
			}
		}
	}
	return ready
}

func (n *RawNode) hasUnresolvedKnownConflict(view *executionView, base InstanceRef, inside map[InstanceRef]struct{}, candidates *[]recoveryCandidate) bool {
	inst := n.instances[base]
	if inst == nil {
		return false
	}
	blocked := false
	for _, otherRef := range view.refs {
		if otherRef == base {
			continue
		}
		if _, member := inside[otherRef]; member || n.executed.contains(otherRef) {
			continue
		}
		other := n.instances[otherRef]
		if other == nil || other.rec.Status == StatusNone || other.rec.Status >= StatusCommitted {
			continue
		}
		if !commandsConflict(inst.rec.Command, other.rec.Command) || n.dependencyKnownAfter(base, otherRef, StatusPreAccepted) {
			continue
		}
		blocked = true
		*candidates = append(*candidates, recoveryCandidate{
			source: recoverySource{kind: recoverySourceConflict, base: base, conflict: otherRef},
			ref:    otherRef,
		})
	}
	return blocked
}

func recoverySourceLess(left, right recoverySource) bool {
	if left.kind != right.kind {
		return left.kind < right.kind
	}
	if left.base != right.base {
		return lessRef(left.base, right.base)
	}
	if left.lane != right.lane {
		if left.lane.conf != right.lane.conf {
			return left.lane.conf < right.lane.conf
		}
		return left.lane.replica < right.lane.replica
	}
	if left.conflict != right.conflict {
		return lessRef(left.conflict, right.conflict)
	}
	return false
}

func recoveryCandidateLess(left, right recoveryCandidate) bool {
	if left.source != right.source {
		return recoverySourceLess(left.source, right.source)
	}
	return lessRef(left.ref, right.ref)
}

func (n *RawNode) activeRecoveryCount() int {
	active := 0
	for _, inst := range n.instances {
		if inst.rec.Status < StatusCommitted && inst.rec.Ballot.IsRecovery() && inst.rec.Ballot.Replica == n.id {
			active++
		}
	}
	return active
}

func (n *RawNode) ensureDependencyRecovery(ref InstanceRef, mayStart bool) recoveryResult {
	if ref.IsZero() || ref.Instance == 0 || n.executed.contains(ref) {
		return recoveryResolved
	}
	inst := n.instances[ref]
	if inst != nil && inst.rec.Status >= StatusCommitted {
		return recoveryResolved
	}
	if inst != nil && (inst.phase == phasePreAccept || inst.phase == phaseAccept || inst.phase == phasePrepare || inst.phase == phaseTryPreAccept) && n.coordinatesInstance(inst) {
		return recoveryInFlight
	}
	if !mayStart {
		return recoveryDeferred
	}
	if inst != nil && n.promisedToOtherCoordinator(inst) {
		return recoveryDeferred
	}
	if !n.shouldCoordinateRecovery(ref) {
		if inst != nil {
			_ = n.scheduleRecovery(inst)
		}
		return recoveryDeferred
	}

	created := false
	if inst == nil {
		rec := InstanceRecord{Ref: ref, Status: StatusNone, Deps: n.depsForConf(ref.Conf)}
		rec.Checksum = ChecksumRecord(rec)
		inst = &instance{rec: rec, phase: phaseIdle}
		n.installInstance(inst)
		created = true
	}
	if err := n.startPrepare(inst); err != nil {
		if created {
			n.engine.remove(ref, inst.rec)
			delete(n.instances, ref)
		}
		return recoveryDeferred
	}
	n.observeInstanceRef(ref)
	return recoveryStarted
}

func (n *RawNode) scheduleDependencyRecoveries(candidates []recoveryCandidate) {
	if len(candidates) == 0 || n.dependencyRecoveryStartsLeft <= 0 || n.maxConcurrentRecoveries <= 0 {
		return
	}
	sort.Slice(candidates, func(i, j int) bool {
		return recoveryCandidateLess(candidates[i], candidates[j])
	})
	unique := candidates[:0]
	for _, candidate := range candidates {
		if len(unique) > 0 && unique[len(unique)-1] == candidate {
			continue
		}
		unique = append(unique, candidate)
	}
	start := 0
	if n.hasRecoverySource {
		start = sort.Search(len(unique), func(index int) bool {
			return recoverySourceLess(n.lastRecoverySource, unique[index].source)
		})
		if start == len(unique) {
			start = 0
		}
	}
	active := n.activeRecoveryCount()
	for offset := 0; offset < len(unique) && n.dependencyRecoveryStartsLeft > 0 && active < n.maxConcurrentRecoveries; offset++ {
		candidate := unique[(start+offset)%len(unique)]
		if n.ensureDependencyRecovery(candidate.ref, true) != recoveryStarted {
			continue
		}
		n.dependencyRecoveryStartsLeft--
		active++
		n.lastRecoverySource = candidate.source
		n.hasRecoverySource = true
	}
}

func (n *RawNode) maybeStartDependencyRecovery(ref InstanceRef) recoveryResult {
	ownedDrive := n.driveDepth == 0
	if ownedDrive {
		n.beginDrive()
		defer n.endDrive()
	}
	if n.dependencyRecoveryStartsLeft <= 0 || n.activeRecoveryCount() >= n.maxConcurrentRecoveries {
		return n.ensureDependencyRecovery(ref, false)
	}
	result := n.ensureDependencyRecovery(ref, true)
	if result == recoveryStarted {
		n.dependencyRecoveryStartsLeft--
	}
	return result
}

func (n *RawNode) beginDrive() {
	if n.driveDepth == 0 {
		n.dependencyRecoveryStartsLeft = n.maxDependencyRecoveriesPerDrive
	}
	n.driveDepth++
}

func (n *RawNode) endDrive() {
	if n.driveDepth > 0 {
		n.driveDepth--
	}
}

func (n *RawNode) tryExecute() {
	ownedDrive := n.driveDepth == 0
	if ownedDrive {
		n.beginDrive()
		defer n.endDrive()
	}
	view := n.newExecutionView()
	workspace := &n.executionWorkspace
	for {
		workspace.candidates = workspace.candidates[:0]
		progress := false
		components := n.executionComponents(&view)
		for _, component := range components {
			if !n.componentReady(&view, component, &workspace.candidates) {
				continue
			}
			sort.Slice(component, func(i, j int) bool {
				left := n.instances[component[i]].rec
				right := n.instances[component[j]].rec
				if left.Seq != right.Seq {
					return left.Seq < right.Seq
				}
				return lessRef(left.Ref, right.Ref)
			})
			for _, ref := range component {
				inst := n.instances[ref]
				if inst == nil || n.executed.contains(ref) {
					continue
				}
				n.executed.add(ref)
				rec := inst.rec
				rec.Status = StatusExecuted
				switch rec.Command.Kind {
				case CommandUser:
					rec.Checksum = ChecksumRecord(rec)
					n.setInstanceRecord(inst, rec)
					n.enqueueCommitted(CommittedCommand{Ref: ref, Seq: rec.Seq, Deps: append([]InstanceNum(nil), rec.Deps...), Command: rec.Command.Clone()})
				case CommandConfChange:
					rec.ConfChangeResult = n.applyConfChange(ref, rec.Command)
					rec.Checksum = ChecksumRecord(rec)
					n.setInstanceRecord(inst, rec)
					n.enqueueRecord(rec)
				case CommandMembership:
					rec.MembershipResult, rec.ConfChangeResult = n.applyMembershipControl(ref, rec.Command)
					rec.Checksum = ChecksumRecord(rec)
					n.setInstanceRecord(inst, rec)
					n.enqueueRecord(rec)
				case CommandNoop:
					fallthrough
				default:
					rec.Checksum = ChecksumRecord(rec)
					n.setInstanceRecord(inst, rec)
					n.enqueueRecord(rec)
				}
				releaseInstanceVolatile(inst)
				progress = true
			}
		}
		if progress {
			continue
		}
		n.scheduleDependencyRecoveries(workspace.candidates)
		return
	}
}
