package epaxos

import "testing"

type testAttrVote struct {
	seq              uint64
	deps             []InstanceNum
	depsCommitted    uint64
	fastPathEligible bool
}

func testAttrVoteSet(t testing.TB, conf ConfState, entries map[ReplicaID]testAttrVote) *attrVoteSet {
	t.Helper()
	set := new(attrVoteSet)
	for sender, entry := range entries {
		vote, ok := newAttrVote(conf, entry.seq, entry.deps, entry.depsCommitted, entry.fastPathEligible)
		if !ok || !set.add(conf, sender, vote) {
			t.Fatalf("invalid attribute vote fixture for sender %d", sender)
		}
	}
	return set
}

func testRecordVoteSet(t testing.TB, conf ConfState, entries map[ReplicaID]InstanceRecord) *recordVoteSet {
	t.Helper()
	set := new(recordVoteSet)
	for sender, record := range entries {
		if !set.add(conf, sender, record) {
			t.Fatalf("invalid record vote fixture for sender %d", sender)
		}
	}
	return set
}

func testVoterMask(t testing.TB, conf ConfState, voters ...ReplicaID) voterMask {
	t.Helper()
	var mask voterMask
	for _, voter := range voters {
		if !mask.add(conf, voter) {
			t.Fatalf("invalid voter mask fixture for sender %d", voter)
		}
	}
	return mask
}
