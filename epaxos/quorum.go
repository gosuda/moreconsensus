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
	return quorum{conf: ConfState{ID: 1, Voters: vv}, index: idx, slow: slowQuorumSize(n), fast: fastQuorumSize(n)}, nil
}

func slowQuorumSize(n int) int { return n/2 + 1 }

func fastQuorumSize(n int) int {
	// Optimized EPaxos fast quorum from the paper/TR for odd N=2F+1:
	// F + floor((F+1)/2) total voters including the command leader. Even
	// cluster sizes keep the previous conservative quorum because the paper
	// proof assumes odd replica counts.
	fast := n - ((n - 1) / 4)
	if n%2 == 1 {
		f := n / 2
		fast = f + ((f + 1) / 2)
		if fast == 0 {
			fast = 1
		}
	}
	return fast
}

func tryWitnessQuorumSize(n int) int { return fastQuorumSize(n) + slowQuorumSize(n) - n }

func (q quorum) contains(id ReplicaID) bool { _, ok := q.index[id]; return ok }

func (q quorum) deps() []InstanceNum { return make([]InstanceNum, len(q.conf.Voters)) }

func (q quorum) slowQuorum() int { return q.slow }

func (q quorum) fastQuorum() int { return q.fast }

func (q quorum) tryWitnessQuorum() int { return tryWitnessQuorumSize(len(q.conf.Voters)) }

// SlowQuorum returns the majority quorum size for n voters.
func SlowQuorum(n int) (int, error) {
	if _, err := newQuorum(makeIDs(n)); err != nil {
		return 0, err
	}
	return slowQuorumSize(n), nil
}

// FastQuorum returns the optimized EPaxos fast quorum size for n voters.
func FastQuorum(n int) (int, error) {
	if _, err := newQuorum(makeIDs(n)); err != nil {
		return 0, err
	}
	return fastQuorumSize(n), nil
}

// TryWitnessQuorum returns the optimized fast/slow intersection threshold.
func TryWitnessQuorum(n int) (int, error) {
	if _, err := newQuorum(makeIDs(n)); err != nil {
		return 0, err
	}
	return tryWitnessQuorumSize(n), nil
}

func makeIDs(n int) []ReplicaID {
	ids := make([]ReplicaID, n)
	for i := range ids {
		ids[i] = ReplicaID(i + 1)
	}
	return ids
}
