package epaxos

import (
	"fmt"
	"sort"
)

type quorum struct {
	conf  ConfState
	index map[ReplicaID]int
	slow  int
	fast  int
}

func newQuorum(voters []ReplicaID) (quorum, error) {
	if len(voters) == 0 || len(voters) > 7 {
		return quorum{}, fmt.Errorf("%w: cluster size must be 1..7", ErrInvalidConfig)
	}
	vv := append([]ReplicaID(nil), voters...)
	sort.Slice(vv, func(i, j int) bool { return vv[i] < vv[j] })
	for i, id := range vv {
		if id == 0 {
			return quorum{}, fmt.Errorf("%w: replica id zero", ErrInvalidConfig)
		}
		if i > 0 && vv[i-1] == id {
			return quorum{}, fmt.Errorf("%w: duplicate replica", ErrInvalidConfig)
		}
	}
	idx := make(map[ReplicaID]int, len(vv))
	for i, id := range vv {
		idx[id] = i
	}
	n := len(vv)
	return quorum{conf: ConfState{ID: 1, Voters: vv}, index: idx, slow: n/2 + 1, fast: n - ((n - 1) / 4)}, nil
}

func (q quorum) contains(id ReplicaID) bool { _, ok := q.index[id]; return ok }

func (q quorum) depIndex(id ReplicaID) (int, bool) { i, ok := q.index[id]; return i, ok }

func (q quorum) deps() []InstanceNum { return make([]InstanceNum, len(q.conf.Voters)) }

func (q quorum) slowQuorum() int { return q.slow }

func (q quorum) fastQuorum() int { return q.fast }

// SlowQuorum returns the majority quorum size for n voters.
func SlowQuorum(n int) (int, error) {
	q, err := newQuorum(makeIDs(n))
	if err != nil {
		return 0, err
	}
	return q.slowQuorum(), nil
}

// FastQuorum returns the conservative EPaxos fast quorum size for n voters.
func FastQuorum(n int) (int, error) {
	q, err := newQuorum(makeIDs(n))
	if err != nil {
		return 0, err
	}
	return q.fastQuorum(), nil
}

func makeIDs(n int) []ReplicaID {
	ids := make([]ReplicaID, n)
	for i := range ids {
		ids[i] = ReplicaID(i + 1)
	}
	return ids
}
