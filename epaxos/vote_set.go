package epaxos

import "sync"

type voterMask uint8

func (m voterMask) hasSlot(slot int) bool {
	return slot >= 0 && slot < 7 && m&(voterMask(1)<<slot) != 0
}

func (m *voterMask) add(conf ConfState, sender ReplicaID) bool {
	if m == nil || len(conf.Voters) < 1 || len(conf.Voters) > 7 {
		return false
	}
	slot, ok := conf.Index(sender)
	if !ok || m.hasSlot(slot) {
		return false
	}
	*m |= voterMask(1) << slot
	return true
}

func (m voterMask) has(conf ConfState, sender ReplicaID) bool {
	slot, ok := conf.Index(sender)
	return ok && m.hasSlot(slot)
}

func (m voterMask) len() int {
	count := 0
	for m != 0 {
		m &= m - 1
		count++
	}
	return count
}

type attrVote struct {
	seq              uint64
	deps             [7]InstanceNum
	width            uint8
	depsCommitted    uint64
	fastPathEligible bool
}

func newAttrVote(conf ConfState, seq uint64, deps []InstanceNum, depsCommitted uint64, fastPathEligible bool) (attrVote, bool) {
	if len(conf.Voters) < 1 || len(conf.Voters) > 7 || len(deps) != len(conf.Voters) {
		return attrVote{}, false
	}
	vote := attrVote{seq: seq, width: uint8(len(deps)), depsCommitted: depsCommitted, fastPathEligible: fastPathEligible}
	copy(vote.deps[:], deps)
	return vote, true
}

func (v *attrVote) reset() { *v = attrVote{} }

func (v *attrVote) attributes() Attributes {
	if v == nil || v.width == 0 || v.width > 7 {
		return Attributes{}
	}
	return Attributes{Seq: v.seq, Deps: v.deps[:v.width:v.width]}
}

type attrVoteSet struct {
	mask  voterMask
	votes [7]attrVote
}

func (s *attrVoteSet) add(conf ConfState, sender ReplicaID, vote attrVote) bool {
	if s == nil || int(vote.width) != len(conf.Voters) {
		return false
	}
	slot, ok := conf.Index(sender)
	if !ok || s.mask.hasSlot(slot) {
		return false
	}
	s.votes[slot] = vote
	s.mask |= voterMask(1) << slot
	return true
}

func (s *attrVoteSet) has(conf ConfState, sender ReplicaID) bool {
	return s != nil && s.mask.has(conf, sender)
}

func (s *attrVoteSet) get(conf ConfState, sender ReplicaID) (*attrVote, bool) {
	if s == nil {
		return nil, false
	}
	slot, ok := conf.Index(sender)
	if !ok || !s.mask.hasSlot(slot) {
		return nil, false
	}
	return &s.votes[slot], true
}

func (s *attrVoteSet) len() int {
	if s == nil {
		return 0
	}
	return s.mask.len()
}

func (s *attrVoteSet) each(conf ConfState, fn func(ReplicaID, *attrVote) bool) {
	if s == nil {
		return
	}
	for slot, sender := range conf.Voters {
		if s.mask.hasSlot(slot) && !fn(sender, &s.votes[slot]) {
			return
		}
	}
}

func (s *attrVoteSet) reset() {
	if s == nil {
		return
	}
	for slot := range s.votes {
		if s.mask.hasSlot(slot) {
			s.votes[slot].reset()
		}
	}
	s.mask = 0
}

type recordVoteSet struct {
	mask    voterMask
	records [7]InstanceRecord
}

func (s *recordVoteSet) add(conf ConfState, sender ReplicaID, record InstanceRecord) bool {
	if s == nil || len(conf.Voters) < 1 || len(conf.Voters) > 7 {
		return false
	}
	slot, ok := conf.Index(sender)
	if !ok || s.mask.hasSlot(slot) {
		return false
	}
	s.records[slot] = record.Clone()
	s.mask |= voterMask(1) << slot
	return true
}

func (s *recordVoteSet) get(conf ConfState, sender ReplicaID) (*InstanceRecord, bool) {
	if s == nil {
		return nil, false
	}
	slot, ok := conf.Index(sender)
	if !ok || !s.mask.hasSlot(slot) {
		return nil, false
	}
	return &s.records[slot], true
}

func (s *recordVoteSet) len() int {
	if s == nil {
		return 0
	}
	return s.mask.len()
}

func (s *recordVoteSet) each(conf ConfState, fn func(ReplicaID, *InstanceRecord) bool) {
	if s == nil {
		return
	}
	for slot, sender := range conf.Voters {
		if s.mask.hasSlot(slot) && !fn(sender, &s.records[slot]) {
			return
		}
	}
}

func (s *recordVoteSet) reset() {
	if s == nil {
		return
	}
	for slot := range s.records {
		if s.mask.hasSlot(slot) {
			s.records[slot] = InstanceRecord{}
		}
	}
	s.mask = 0
}

var attrVoteSetPool = sync.Pool{New: func() any { return new(attrVoteSet) }}
var recordVoteSetPool = sync.Pool{New: func() any { return new(recordVoteSet) }}

func getAttrVoteSet() *attrVoteSet {
	set := attrVoteSetPool.Get().(*attrVoteSet)
	set.reset()
	return set
}

func putAttrVoteSet(set *attrVoteSet) {
	if set == nil {
		return
	}
	set.reset()
	attrVoteSetPool.Put(set)
}

func getRecordVoteSet() *recordVoteSet {
	set := recordVoteSetPool.Get().(*recordVoteSet)
	set.reset()
	return set
}

func putRecordVoteSet(set *recordVoteSet) {
	if set == nil {
		return
	}
	set.reset()
	recordVoteSetPool.Put(set)
}
