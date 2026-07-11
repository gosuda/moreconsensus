package epaxos

import "testing"

func TestFixedWidthVoteSetsForAllSupportedConfigurations(t *testing.T) {
	for voters := 1; voters <= 7; voters++ {
		t.Run(string(rune('0'+voters)), func(t *testing.T) {
			conf := ConfState{ID: ConfID(10 + voters), Voters: makeIDs(voters)}
			var attrs attrVoteSet
			var records recordVoteSet
			var mask voterMask
			for _, sender := range conf.Voters {
				deps := make([]InstanceNum, voters)
				deps[sender-1] = InstanceNum(sender)
				vote, ok := newAttrVote(conf, uint64(sender), deps, uint64(1)<<(sender-1), true)
				if !ok || !attrs.add(conf, sender, vote) || !records.add(conf, sender, InstanceRecord{Ref: InstanceRef{Conf: conf.ID, Replica: sender, Instance: 1}, Deps: deps}) || !mask.add(conf, sender) {
					t.Fatalf("failed to add sender %d", sender)
				}
				if attrs.add(conf, sender, vote) || records.add(conf, sender, InstanceRecord{}) || mask.add(conf, sender) {
					t.Fatalf("duplicate sender %d mutated first vote", sender)
				}
			}
			if attrs.len() != voters || records.len() != voters || mask.len() != voters {
				t.Fatalf("vote counts attrs=%d records=%d mask=%d, want %d", attrs.len(), records.len(), mask.len(), voters)
			}
			beforeAttrs, beforeRecords, beforeMask := attrs, records, mask
			invalidVote, _ := newAttrVote(conf, 1, make([]InstanceNum, voters), 0, true)
			if attrs.add(conf, 99, invalidVote) || records.add(conf, 99, InstanceRecord{}) || mask.add(conf, 99) {
				t.Fatal("out-of-configuration sender was accepted")
			}
			if attrs.mask != beforeAttrs.mask || records.mask != beforeRecords.mask || mask != beforeMask {
				t.Fatal("invalid sender mutated vote state")
			}
			seen := make([]ReplicaID, 0, voters)
			attrs.each(conf, func(sender ReplicaID, _ *attrVote) bool { seen = append(seen, sender); return true })
			for i, sender := range seen {
				if sender != conf.Voters[i] {
					t.Fatalf("iteration order=%v, want %v", seen, conf.Voters)
				}
			}
		})
	}
}

func TestVoteSetsUsePinnedHistoricalConfiguration(t *testing.T) {
	historical := ConfState{ID: 3, Voters: []ReplicaID{1, 3, 5}}
	current := ConfState{ID: 4, Voters: []ReplicaID{1, 2, 5}}
	vote, ok := newAttrVote(historical, 1, []InstanceNum{0, 0, 0}, 0, true)
	if !ok {
		t.Fatal("historical vote rejected")
	}
	var set attrVoteSet
	if !set.add(historical, 3, vote) || !set.has(historical, 3) {
		t.Fatal("historical voter was not indexed by pinned configuration")
	}
	if set.has(current, 3) || set.add(current, 3, vote) {
		t.Fatal("removed historical voter was resolved through current configuration")
	}
}

func TestVoteSetPoolsDeepResetActiveOwnership(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	attrs := getAttrVoteSet()
	vote, _ := newAttrVote(conf, 2, []InstanceNum{1, 2, 3}, 7, true)
	attrs.add(conf, 2, vote)
	putAttrVoteSet(attrs)
	attrs = getAttrVoteSet()
	if attrs.len() != 0 || attrs.votes[1] != (attrVote{}) {
		t.Fatalf("pooled attribute votes retained state: %#v", attrs)
	}
	putAttrVoteSet(attrs)

	records := getRecordVoteSet()
	records.add(conf, 2, InstanceRecord{Ref: InstanceRef{Conf: 1, Replica: 2, Instance: 1}, Deps: []InstanceNum{1, 2, 3}, Command: Command{Payload: []byte("secret")}})
	putRecordVoteSet(records)
	records = getRecordVoteSet()
	if records.len() != 0 || records.records[1].Deps != nil || records.records[1].Command.Payload != nil {
		t.Fatalf("pooled record votes retained references: %#v", records)
	}
	putRecordVoteSet(records)
}
