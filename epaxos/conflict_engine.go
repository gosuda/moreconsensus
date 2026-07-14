package epaxos

import "fmt"

const (
	radixBits               = 6
	radixFanout             = 1 << radixBits
	maxRadixLevel           = 10
	retiredSeqBreakpointCap = 64
)

type laneSlotFlags uint8

const (
	laneSlotStatusNone laneSlotFlags = 1 << iota
	laneSlotTOQPending
	laneSlotNoop
	laneSlotGlobalScope
	laneSlotExecuted
)

type laneSlot struct {
	seq   uint64
	flags laneSlotFlags
}

func slotForRecord(rec InstanceRecord) laneSlot {
	var flags laneSlotFlags
	if rec.Status == StatusNone {
		flags |= laneSlotStatusNone
	}
	if rec.TOQPending {
		flags |= laneSlotTOQPending
	}
	if rec.Command.Kind == CommandNoop {
		flags |= laneSlotNoop
	}
	if commandHasGlobalConflictScope(rec.Command.Kind) {
		flags |= laneSlotGlobalScope
	}
	if rec.Status == StatusExecuted {
		flags |= laneSlotExecuted
	}
	return laneSlot{seq: rec.Seq, flags: flags}
}

func (s laneSlot) eligible() bool {
	const ineligible = laneSlotStatusNone | laneSlotTOQPending | laneSlotNoop
	return s.flags&ineligible == 0
}

func (s laneSlot) global() bool { return s.flags&laneSlotGlobalScope != 0 }

type radixLeaf struct {
	present uint64
	slots   [radixFanout]laneSlot
}

type radixChildren [radixFanout]*radixNode

type radixNode struct {
	level          uint8
	children       *radixChildren
	leaf           *radixLeaf
	count          int
	maxSeq         uint64
	maxEligibleAny InstanceNum
	globalMax      InstanceNum
}

type laneTree struct {
	root *radixNode
}

func radixLevel(value InstanceNum) uint8 {
	for level := uint8(0); level < maxRadixLevel; level++ {
		if uint64(value) < uint64(1)<<(radixBits*(level+1)) {
			return level
		}
	}
	return maxRadixLevel
}

func newRadixNode(level uint8) *radixNode {
	node := &radixNode{level: level}
	if level == 0 {
		node.leaf = &radixLeaf{}
	} else {
		node.children = &radixChildren{}
	}
	return node
}

func (t *laneTree) set(instance InstanceNum, slot laneSlot) {
	level := radixLevel(instance)
	if t.root == nil {
		t.root = newRadixNode(level)
	}
	for t.root.level < level {
		root := newRadixNode(t.root.level + 1)
		root.children[0] = t.root
		recomputeRadixNode(root, 0)
		t.root = root
	}
	setRadixSlot(t.root, instance, slot, 0)
}

func setRadixSlot(node *radixNode, instance InstanceNum, slot laneSlot, prefix InstanceNum) {
	if node.level == 0 {
		idx := uint(instance) & (radixFanout - 1)
		node.leaf.slots[idx] = slot
		node.leaf.present |= uint64(1) << idx
		recomputeRadixNode(node, prefix)
		return
	}
	shift := radixBits * node.level
	idx := uint(instance>>shift) & (radixFanout - 1)
	if node.children[idx] == nil {
		node.children[idx] = newRadixNode(node.level - 1)
	}
	childPrefix := prefix | InstanceNum(idx)<<shift
	setRadixSlot(node.children[idx], instance, slot, childPrefix)
	recomputeRadixNode(node, prefix)
}

func (t *laneTree) remove(instance InstanceNum) bool {
	if t.root == nil || radixLevel(instance) > t.root.level {
		return false
	}
	removed := removeRadixSlot(t.root, instance, 0)
	if !removed {
		return false
	}
	if t.root.count == 0 {
		t.root = nil
		return true
	}
	for t.root.level > 0 && t.root.children[0] != nil && t.root.count == t.root.children[0].count {
		t.root = t.root.children[0]
	}
	return true
}

func removeRadixSlot(node *radixNode, instance InstanceNum, prefix InstanceNum) bool {
	if node.level == 0 {
		idx := uint(instance) & (radixFanout - 1)
		bit := uint64(1) << idx
		if node.leaf.present&bit == 0 {
			return false
		}
		node.leaf.present &^= bit
		node.leaf.slots[idx] = laneSlot{}
		recomputeRadixNode(node, prefix)
		return true
	}
	shift := radixBits * node.level
	idx := uint(instance>>shift) & (radixFanout - 1)
	child := node.children[idx]
	if child == nil || !removeRadixSlot(child, instance, prefix|InstanceNum(idx)<<shift) {
		return false
	}
	if child.count == 0 {
		node.children[idx] = nil
	}
	recomputeRadixNode(node, prefix)
	return true
}

func recomputeRadixNode(node *radixNode, prefix InstanceNum) {
	node.count = 0
	node.maxSeq = 0
	node.maxEligibleAny = 0
	node.globalMax = 0
	if node.level == 0 {
		present := node.leaf.present
		for idx := uint(0); idx < radixFanout; idx++ {
			if present&(uint64(1)<<idx) == 0 {
				continue
			}
			node.count++
			slot := node.leaf.slots[idx]
			if !slot.eligible() {
				continue
			}
			instance := prefix | InstanceNum(idx)
			node.maxSeq = max(node.maxSeq, slot.seq)
			node.maxEligibleAny = max(node.maxEligibleAny, instance)
			if slot.global() {
				node.globalMax = max(node.globalMax, instance)
			}
		}
		return
	}
	for _, child := range node.children {
		if child == nil {
			continue
		}
		node.count += child.count
		node.maxSeq = max(node.maxSeq, child.maxSeq)
		node.maxEligibleAny = max(node.maxEligibleAny, child.maxEligibleAny)
		node.globalMax = max(node.globalMax, child.globalMax)
	}
}

func (t *laneTree) prefixMaxSeq(through InstanceNum) uint64 {
	if t.root == nil {
		return 0
	}
	return prefixMaxRadix(t.root, through, radixLevel(through) <= t.root.level)
}

func prefixMaxRadix(node *radixNode, through InstanceNum, limited bool) uint64 {
	if node == nil {
		return 0
	}
	if !limited {
		return node.maxSeq
	}
	if node.level == 0 {
		last := uint(through) & (radixFanout - 1)
		var result uint64
		for idx := uint(0); idx <= last; idx++ {
			if node.leaf.present&(uint64(1)<<idx) != 0 {
				slot := node.leaf.slots[idx]
				if slot.eligible() {
					result = max(result, slot.seq)
				}
			}
		}
		return result
	}
	shift := radixBits * node.level
	last := uint(through>>shift) & (radixFanout - 1)
	var result uint64
	for idx := uint(0); idx <= last; idx++ {
		result = max(result, prefixMaxRadix(node.children[idx], through, idx == last))
	}
	return result
}

func (t *laneTree) walkDesc(from InstanceNum, yield func(InstanceNum, laneSlot) bool) {
	if t.root == nil {
		return
	}
	walkRadixDesc(t.root, 0, from, radixLevel(from) <= t.root.level, yield)
}

func walkRadixDesc(node *radixNode, prefix, from InstanceNum, limited bool, yield func(InstanceNum, laneSlot) bool) bool {
	if node == nil {
		return true
	}
	if node.level == 0 {
		last := uint(radixFanout - 1)
		if limited {
			last = uint(from) & (radixFanout - 1)
		}
		for idx := int(last); idx >= 0; idx-- {
			if node.leaf.present&(uint64(1)<<uint(idx)) == 0 {
				continue
			}
			if !yield(prefix|InstanceNum(idx), node.leaf.slots[idx]) {
				return false
			}
		}
		return true
	}
	shift := radixBits * node.level
	last := uint(radixFanout - 1)
	if limited {
		last = uint(from>>shift) & (radixFanout - 1)
	}
	for idx := int(last); idx >= 0; idx-- {
		childLimited := limited && uint(idx) == last
		if !walkRadixDesc(node.children[idx], prefix|InstanceNum(idx)<<shift, from, childLimited, yield) {
			return false
		}
	}
	return true
}

// walkGlobalRadixDesc descends only subtrees that contain a global-eligible instance
// (node.globalMax != 0 and within the from prefix), so unrelated residents are not visited.
func walkGlobalRadixDesc(node *radixNode, prefix, from InstanceNum, limited bool, yield func(InstanceNum, laneSlot) bool) bool {
	if node == nil || node.globalMax == 0 {
		return true
	}
	if node.level == 0 {
		last := uint(radixFanout - 1)
		if limited {
			last = uint(from) & (radixFanout - 1)
		}
		for idx := int(last); idx >= 0; idx-- {
			if node.leaf.present&(uint64(1)<<uint(idx)) == 0 {
				continue
			}
			instance := prefix | InstanceNum(idx)
			if limited && instance > from {
				continue
			}
			slot := node.leaf.slots[idx]
			if !slot.global() || !slot.eligible() {
				continue
			}
			if !yield(instance, slot) {
				return false
			}
		}
		return true
	}
	shift := radixBits * node.level
	last := uint(radixFanout - 1)
	if limited {
		last = uint(from>>shift) & (radixFanout - 1)
	}
	for idx := int(last); idx >= 0; idx-- {
		child := node.children[idx]
		if child == nil || child.globalMax == 0 {
			continue
		}
		childPrefix := prefix | InstanceNum(idx)<<shift
		// Prune children whose entire key range is > from when limited.
		if limited && childPrefix > from {
			continue
		}
		childLimited := limited && uint(idx) == last
		if !walkGlobalRadixDesc(child, childPrefix, from, childLimited, yield) {
			return false
		}
	}
	return true
}

func (t *laneTree) slot(instance InstanceNum) (laneSlot, bool) {
	node := t.root
	if node == nil || radixLevel(instance) > node.level {
		return laneSlot{}, false
	}
	for node.level > 0 {
		idx := uint(instance>>(radixBits*node.level)) & (radixFanout - 1)
		node = node.children[idx]
		if node == nil {
			return laneSlot{}, false
		}
	}
	idx := uint(instance) & (radixFanout - 1)
	if node.leaf.present&(uint64(1)<<idx) == 0 {
		return laneSlot{}, false
	}
	return node.leaf.slots[idx], true
}

type postingLeaf struct {
	present uint64
}

type postingChildren [radixFanout]*postingNode

type postingNode struct {
	level    uint8
	children *postingChildren
	leaf     *postingLeaf
	count    int
	maximum  InstanceNum
}

type postingSet struct {
	root *postingNode
}

func newPostingNode(level uint8) *postingNode {
	node := &postingNode{level: level}
	if level == 0 {
		node.leaf = &postingLeaf{}
	} else {
		node.children = &postingChildren{}
	}
	return node
}

func (s *postingSet) insert(instance InstanceNum) {
	level := radixLevel(instance)
	if s.root == nil {
		s.root = newPostingNode(level)
	}
	for s.root.level < level {
		root := newPostingNode(s.root.level + 1)
		root.children[0] = s.root
		recomputePostingNode(root, 0)
		s.root = root
	}
	insertPosting(s.root, instance, 0)
}

func insertPosting(node *postingNode, instance, prefix InstanceNum) {
	if node.level == 0 {
		node.leaf.present |= uint64(1) << (uint(instance) & (radixFanout - 1))
		recomputePostingNode(node, prefix)
		return
	}
	shift := radixBits * node.level
	idx := uint(instance>>shift) & (radixFanout - 1)
	if node.children[idx] == nil {
		node.children[idx] = newPostingNode(node.level - 1)
	}
	insertPosting(node.children[idx], instance, prefix|InstanceNum(idx)<<shift)
	recomputePostingNode(node, prefix)
}

func (s *postingSet) remove(instance InstanceNum) bool {
	if s.root == nil || radixLevel(instance) > s.root.level || !removePosting(s.root, instance, 0) {
		return false
	}
	if s.root.count == 0 {
		s.root = nil
		return true
	}
	for s.root.level > 0 && s.root.children[0] != nil && s.root.count == s.root.children[0].count {
		s.root = s.root.children[0]
	}
	return true
}

func removePosting(node *postingNode, instance, prefix InstanceNum) bool {
	if node.level == 0 {
		idx := uint(instance) & (radixFanout - 1)
		bit := uint64(1) << idx
		if node.leaf.present&bit == 0 {
			return false
		}
		node.leaf.present &^= bit
		recomputePostingNode(node, prefix)
		return true
	}
	shift := radixBits * node.level
	idx := uint(instance>>shift) & (radixFanout - 1)
	child := node.children[idx]
	if child == nil || !removePosting(child, instance, prefix|InstanceNum(idx)<<shift) {
		return false
	}
	if child.count == 0 {
		node.children[idx] = nil
	}
	recomputePostingNode(node, prefix)
	return true
}

func recomputePostingNode(node *postingNode, prefix InstanceNum) {
	node.count = 0
	node.maximum = 0
	if node.level == 0 {
		for idx := uint(0); idx < radixFanout; idx++ {
			if node.leaf.present&(uint64(1)<<idx) != 0 {
				node.count++
				node.maximum = prefix | InstanceNum(idx)
			}
		}
		return
	}
	for _, child := range node.children {
		if child != nil {
			node.count += child.count
			node.maximum = max(node.maximum, child.maximum)
		}
	}
}

func (s *postingSet) max() InstanceNum {
	if s.root == nil {
		return 0
	}
	return s.root.maximum
}

func (s *postingSet) predecessor(from InstanceNum) (InstanceNum, bool) {
	if s.root == nil {
		return 0, false
	}
	var result InstanceNum
	found := false
	walkPostingDesc(s.root, 0, from, radixLevel(from) <= s.root.level, func(instance InstanceNum) bool {
		result, found = instance, true
		return false
	})
	return result, found
}

func (s *postingSet) contains(instance InstanceNum) bool {
	found, ok := s.predecessor(instance)
	return ok && found == instance
}

func walkPostingDesc(node *postingNode, prefix, from InstanceNum, limited bool, yield func(InstanceNum) bool) bool {
	if node == nil {
		return true
	}
	if node.level == 0 {
		last := uint(radixFanout - 1)
		if limited {
			last = uint(from) & (radixFanout - 1)
		}
		for idx := int(last); idx >= 0; idx-- {
			if node.leaf.present&(uint64(1)<<uint(idx)) != 0 && !yield(prefix|InstanceNum(idx)) {
				return false
			}
		}
		return true
	}
	shift := radixBits * node.level
	last := uint(radixFanout - 1)
	if limited {
		last = uint(from>>shift) & (radixFanout - 1)
	}
	for idx := int(last); idx >= 0; idx-- {
		if !walkPostingDesc(node.children[idx], prefix|InstanceNum(idx)<<shift, from, limited && uint(idx) == last, yield) {
			return false
		}
	}
	return true
}

type retiredSeqBreakpoint struct {
	through InstanceNum
	maxSeq  uint64
}

type laneIndex struct {
	resident           laneTree
	folded             InstanceNum
	retiredEligibleAny InstanceNum
	retiredGlobal      InstanceNum
	retiredSeq         []retiredSeqBreakpoint
	seqCompressed      bool
	pendingFold        postingSet
}

type keyLane struct {
	postings     postingSet
	retiredFloor InstanceNum
}

type keyLanes map[instanceLane]*keyLane

type conflictEngine struct {
	laneIndex map[instanceLane]*laneIndex
	byKey     map[ConfID]map[string]*keyLanes
	resident int
}

func (e *conflictEngine) ensureLane(lane instanceLane) *laneIndex {
	if e.laneIndex == nil {
		e.laneIndex = make(map[instanceLane]*laneIndex)
	}
	index := e.laneIndex[lane]
	if index == nil {
		index = &laneIndex{}
		e.laneIndex[lane] = index
	}
	return index
}

func (e *conflictEngine) apply(prev *InstanceRecord, rec InstanceRecord) {
	if prev != nil {
		e.remove(prev.Ref, *prev)
	}
	lane := laneFor(rec.Ref)
	index := e.ensureLane(lane)
	_, existed := index.resident.slot(rec.Ref.Instance)
	index.resident.set(rec.Ref.Instance, slotForRecord(rec))
	if !existed {
		e.resident++
	}
	if recordConflictEligible(rec) && !commandHasGlobalConflictScope(rec.Command.Kind) {
		for _, key := range rec.Command.ConflictKeys {
			e.ensureKeyLane(rec.Ref.Conf, key, lane).postings.insert(rec.Ref.Instance)
		}
	}
}

func recordConflictEligible(rec InstanceRecord) bool {
	return rec.Status != StatusNone && !rec.TOQPending && rec.Command.Kind != CommandNoop
}

func (e *conflictEngine) ensureKeyLane(conf ConfID, key []byte, lane instanceLane) *keyLane {
	if e.byKey == nil {
		e.byKey = make(map[ConfID]map[string]*keyLanes)
	}
	keys := e.byKey[conf]
	if keys == nil {
		keys = make(map[string]*keyLanes)
		e.byKey[conf] = keys
	}
	lanes := keys[string(key)]
	if lanes == nil {
		lanes = &keyLanes{}
		keys[string(key)] = lanes
	}
	entry := (*lanes)[lane]
	if entry == nil {
		entry = &keyLane{}
		(*lanes)[lane] = entry
	}
	return entry
}

func (e *conflictEngine) remove(ref InstanceRef, rec InstanceRecord) {
	lane := laneFor(ref)
	index := e.laneIndex[lane]
	if index == nil || !index.resident.remove(ref.Instance) {
		return
	}
	e.resident--
	if recordConflictEligible(rec) && !commandHasGlobalConflictScope(rec.Command.Kind) {
		for _, key := range rec.Command.ConflictKeys {
			e.removeKeyPosting(ref.Conf, key, lane, ref.Instance)
		}
	}
}

func (e *conflictEngine) removeKeyPosting(conf ConfID, key []byte, lane instanceLane, instance InstanceNum) {
	keys := e.byKey[conf]
	if keys == nil {
		return
	}
	lanes := keys[string(key)]
	if lanes == nil {
		return
	}
	entry := (*lanes)[lane]
	if entry == nil {
		return
	}
	entry.postings.remove(instance)
	if entry.postings.root == nil && entry.retiredFloor == 0 {
		delete(*lanes, lane)
	}
	if len(*lanes) == 0 {
		delete(keys, string(key))
	}
	if len(keys) == 0 {
		delete(e.byKey, conf)
	}
}

func (e *conflictEngine) prefixMaxSeq(lane instanceLane, through InstanceNum) uint64 {
	index := e.laneIndex[lane]
	if index == nil {
		return 0
	}
	return max(index.resident.prefixMaxSeq(through), index.retiredPrefixMaxSeq(through))
}

func (i *laneIndex) retiredPrefixMaxSeq(through InstanceNum) uint64 {
	if len(i.retiredSeq) == 0 {
		return 0
	}
	if through < i.retiredSeq[0].through {
		if i.seqCompressed {
			return i.retiredSeq[0].maxSeq
		}
		return 0
	}
	lo, hi := 0, len(i.retiredSeq)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if i.retiredSeq[mid].through <= through {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return i.retiredSeq[lo-1].maxSeq
}

func (e *conflictEngine) walkDesc(lane instanceLane, from InstanceNum, yield func(InstanceNum, laneSlot) bool) {
	if index := e.laneIndex[lane]; index != nil {
		index.resident.walkDesc(from, yield)
	}
}

func (e *conflictEngine) keyMax(conf ConfID, key []byte, lane instanceLane) (resident, retired InstanceNum) {
	keys := e.byKey[conf]
	if keys == nil {
		return 0, 0
	}
	lanes := keys[string(key)]
	if lanes == nil {
		return 0, 0
	}
	entry := (*lanes)[lane]
	if entry == nil {
		return 0, 0
	}
	return entry.postings.max(), entry.retiredFloor
}

// walkKeyDesc yields resident instances for one (conf,key,lane) in descending Instance order,
// only visiting that key's posting tree (not unrelated lane residents).
func (e *conflictEngine) walkKeyDesc(conf ConfID, key []byte, lane instanceLane, from InstanceNum, yield func(InstanceNum, laneSlot) bool) {
	keys := e.byKey[conf]
	if keys == nil {
		return
	}
	lanes := keys[string(key)]
	if lanes == nil {
		return
	}
	entry := (*lanes)[lane]
	if entry == nil || entry.postings.root == nil {
		return
	}
	index := e.laneIndex[lane]
	if index == nil {
		return
	}
	if from == 0 {
		from = entry.postings.max()
	}
	if from == 0 {
		return
	}
	walkPostingDesc(entry.postings.root, 0, from, true, func(instance InstanceNum) bool {
		slot, ok := index.resident.slot(instance)
		if !ok {
			return true
		}
		return yield(instance, slot)
	})
}

// walkGlobalDesc yields global-scope eligible residents for a lane descending from from.
// Descent prunes radix subtrees with globalMax==0 so unrelated residents are not visited.
func (e *conflictEngine) walkGlobalDesc(lane instanceLane, from InstanceNum, yield func(InstanceNum, laneSlot) bool) {
	index := e.laneIndex[lane]
	if index == nil || index.resident.root == nil || from == 0 {
		return
	}
	walkGlobalRadixDesc(index.resident.root, 0, from, radixLevel(from) <= index.resident.root.level, yield)
}

func (e *conflictEngine) keyLaneSet(conf ConfID, keys [][]byte, yield func(instanceLane) bool) {
	for lane := range e.laneIndex {
		if lane.conf != conf {
			continue
		}
		for _, key := range keys {
			byConf := e.byKey[conf]
			if byConf == nil {
				break
			}
			byLane := byConf[string(key)]
			if byLane == nil || (*byLane)[lane] == nil {
				continue
			}
			if !yield(lane) {
				return
			}
			break
		}
	}
}

func (e *conflictEngine) lanes(conf ConfID, yield func(instanceLane) bool) {
	for lane := range e.laneIndex {
		if lane.conf == conf && !yield(lane) {
			return
		}
	}
}

func (e *conflictEngine) maxEligibleAny(lane instanceLane) (resident, retired InstanceNum) {
	index := e.laneIndex[lane]
	if index == nil {
		return 0, 0
	}
	if index.resident.root != nil {
		resident = index.resident.root.maxEligibleAny
	}
	return resident, index.retiredEligibleAny
}

func (e *conflictEngine) globalMax(lane instanceLane) (resident, retired InstanceNum) {
	index := e.laneIndex[lane]
	if index == nil {
		return 0, 0
	}
	if index.resident.root != nil {
		resident = index.resident.root.globalMax
	}
	return resident, index.retiredGlobal
}

func (e *conflictEngine) foldRecord(rec InstanceRecord) {
	lane := laneFor(rec.Ref)
	index := e.ensureLane(lane)
	e.remove(rec.Ref, rec)
	if rec.Ref.Instance <= index.folded {
		return
	}
	index.pendingFold.insert(rec.Ref.Instance)
	if !recordConflictEligible(rec) {
		return
	}
	index.retiredEligibleAny = max(index.retiredEligibleAny, rec.Ref.Instance)
	if commandHasGlobalConflictScope(rec.Command.Kind) {
		index.retiredGlobal = max(index.retiredGlobal, rec.Ref.Instance)
	} else {
		for _, key := range rec.Command.ConflictKeys {
			entry := e.ensureKeyLane(rec.Ref.Conf, key, lane)
			entry.retiredFloor = max(entry.retiredFloor, rec.Ref.Instance)
		}
	}
	index.addRetiredSeq(rec.Ref.Instance, rec.Seq)
}

func (i *laneIndex) addRetiredSeq(through InstanceNum, seq uint64) {
	prior := i.retiredPrefixMaxSeq(through)
	if seq <= prior {
		return
	}
	pos := 0
	for pos < len(i.retiredSeq) && i.retiredSeq[pos].through < through {
		pos++
	}
	if pos < len(i.retiredSeq) && i.retiredSeq[pos].through == through {
		i.retiredSeq[pos].maxSeq = max(i.retiredSeq[pos].maxSeq, seq)
	} else {
		i.retiredSeq = append(i.retiredSeq, retiredSeqBreakpoint{})
		copy(i.retiredSeq[pos+1:], i.retiredSeq[pos:])
		i.retiredSeq[pos] = retiredSeqBreakpoint{through: through, maxSeq: seq}
	}
	end := pos + 1
	for end < len(i.retiredSeq) && i.retiredSeq[end].maxSeq <= i.retiredSeq[pos].maxSeq {
		end++
	}
	if end > pos+1 {
		copy(i.retiredSeq[pos+1:], i.retiredSeq[end:])
		i.retiredSeq = i.retiredSeq[:len(i.retiredSeq)-(end-pos-1)]
	}
	if len(i.retiredSeq) > retiredSeqBreakpointCap {
		drop := len(i.retiredSeq) - retiredSeqBreakpointCap
		copy(i.retiredSeq, i.retiredSeq[drop:])
		i.retiredSeq = i.retiredSeq[:retiredSeqBreakpointCap]
		i.seqCompressed = true
	}
}

func (e *conflictEngine) canAdvanceFold(lane instanceLane, through InstanceNum) bool {
	index := e.laneIndex[lane]
	if index == nil || through <= index.folded {
		return through == 0 || (index != nil && through == index.folded)
	}
	for instance := index.folded + 1; ; instance++ {
		if !index.pendingFold.contains(instance) {
			return false
		}
		if instance == through {
			return true
		}
	}
}

func (e *conflictEngine) advanceFold(lane instanceLane, through InstanceNum) {
	index := e.ensureLane(lane)
	if through < index.folded {
		panic("epaxos: conflict engine fold watermark regression")
	}
	if through == index.folded {
		return
	}
	for instance := index.folded + 1; ; instance++ {
		if !index.pendingFold.contains(instance) {
			panic("epaxos: conflict engine non-contiguous fold")
		}
		if instance == through {
			break
		}
	}
	for instance := index.folded + 1; ; instance++ {
		if !index.pendingFold.remove(instance) {
			panic("epaxos: conflict engine fold preflight changed")
		}
		if instance == through {
			break
		}
	}
	index.folded = through
}

func (e *conflictEngine) foldedThrough(lane instanceLane) InstanceNum {
	if index := e.laneIndex[lane]; index != nil {
		return index.folded
	}
	return 0
}

func (e *conflictEngine) residentCount() int { return e.resident }

func (e *conflictEngine) verify() error {
	resident := 0
	for lane, index := range e.laneIndex {
		if err := verifyRadixNode(index.resident.root, 0); err != nil {
			return fmt.Errorf("lane %v: %w", lane, err)
		}
		if err := verifyPostingNode(index.pendingFold.root, 0); err != nil {
			return fmt.Errorf("lane %v pending fold: %w", lane, err)
		}
		if index.resident.root != nil {
			resident += index.resident.root.count
		}
		if len(index.retiredSeq) > retiredSeqBreakpointCap {
			return fmt.Errorf("lane %v: retired seq breakpoint cap exceeded", lane)
		}
		for idx := 1; idx < len(index.retiredSeq); idx++ {
			if index.retiredSeq[idx-1].through >= index.retiredSeq[idx].through || index.retiredSeq[idx-1].maxSeq >= index.retiredSeq[idx].maxSeq {
				return fmt.Errorf("lane %v: retired seq breakpoints are not increasing", lane)
			}
		}
	}
	if resident != e.resident {
		return fmt.Errorf("resident count: aggregate=%d tracked=%d", resident, e.resident)
	}
	// Forward: every posting entry must name an eligible non-global resident.
	for conf, keys := range e.byKey {
		for key, lanes := range keys {
			for lane, entry := range *lanes {
				if lane.conf != conf {
					return fmt.Errorf("key %q: lane configuration mismatch", key)
				}
				if err := verifyPostingNode(entry.postings.root, 0); err != nil {
					return fmt.Errorf("key %q lane %v: %w", key, lane, err)
				}
				valid := true
				walkPostingDesc(entry.postings.root, 0, ^InstanceNum(0), false, func(instance InstanceNum) bool {
					slot, ok := e.laneIndex[lane].resident.slot(instance)
					if !ok || !slot.eligible() || slot.global() {
						valid = false
						return false
					}
					return true
				})
				if !valid {
					return fmt.Errorf("key %q lane %v: posting lacks eligible resident slot", key, lane)
				}
			}
		}
	}
	return nil
}

func verifyRadixNode(node *radixNode, prefix InstanceNum) error {
	if node == nil {
		return nil
	}
	copyNode := *node
	recomputeRadixNode(&copyNode, prefix)
	if copyNode.count != node.count || copyNode.maxSeq != node.maxSeq || copyNode.maxEligibleAny != node.maxEligibleAny || copyNode.globalMax != node.globalMax {
		return fmt.Errorf("radix aggregate mismatch at level %d", node.level)
	}
	if node.level == 0 {
		return nil
	}
	shift := radixBits * node.level
	for idx, child := range node.children {
		if child == nil {
			continue
		}
		if child.level+1 != node.level {
			return fmt.Errorf("radix level mismatch: parent=%d child=%d", node.level, child.level)
		}
		if err := verifyRadixNode(child, prefix|InstanceNum(idx)<<shift); err != nil {
			return err
		}
	}
	return nil
}

func verifyPostingNode(node *postingNode, prefix InstanceNum) error {
	if node == nil {
		return nil
	}
	copyNode := *node
	recomputePostingNode(&copyNode, prefix)
	if copyNode.count != node.count || copyNode.maximum != node.maximum {
		return fmt.Errorf("posting aggregate mismatch at level %d", node.level)
	}
	if node.level == 0 {
		return nil
	}
	shift := radixBits * node.level
	for idx, child := range node.children {
		if child == nil {
			continue
		}
		if child.level+1 != node.level {
			return fmt.Errorf("posting level mismatch: parent=%d child=%d", node.level, child.level)
		}
		if err := verifyPostingNode(child, prefix|InstanceNum(idx)<<shift); err != nil {
			return err
		}
	}
	return nil
}
