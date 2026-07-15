package epaxos

import "testing"

func TestLegacyRecordChecksumsRejectUnauthenticatedNewerFields(t *testing.T) {
	base := InstanceRecord{
		Ref:              InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:           Ballot{Replica: 1},
		RecordBallot:     Ballot{Replica: 1},
		Status:           StatusPreAccepted,
		Seq:              1,
		Deps:             []InstanceNum{0, 0, 0},
		Command:          Command{ID: CommandID{Client: 401, Sequence: 1}, Payload: []byte("legacy-checksum"), Footprint: Footprint{Points: [][]byte{[]byte("legacy-checksum-key")}}},
		FastPathEligible: true,
	}
	tests := []struct {
		name     string
		prepare  func(InstanceRecord) InstanceRecord
		verify   func(InstanceRecord) bool
		mutators []func(*InstanceRecord)
	}{
		{
			name: "without configuration outcome",
			prepare: func(r InstanceRecord) InstanceRecord {
				r.Checksum = checksumRecord(r, true, true, true, true, true, false)
				return r
			},
			verify: VerifyRecordChecksumWithoutConfChangeResult,
		},
		{
			name: "without sender evidence",
			prepare: func(r InstanceRecord) InstanceRecord {
				r.AcceptEvidence = nil
				r.Checksum = checksumRecord(r, true, true, true, true, false, false)
				return r
			},
			verify: VerifyRecordChecksumWithoutSenderAcceptEvidence,
			mutators: []func(*InstanceRecord){
				func(r *InstanceRecord) {
					r.AcceptEvidence = []AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{0, 0, 0}}}
				},
			},
		},
		{
			name: "without accept evidence",
			prepare: func(r InstanceRecord) InstanceRecord {
				r.RecordBallot = Ballot{}
				r.AcceptSeq = 0
				r.AcceptDeps = nil
				r.AcceptEvidence = nil
				r.Checksum = checksumRecord(r, true, true, false, false, false, false)
				return r
			},
			verify: VerifyRecordChecksumWithoutAcceptEvidence,
			mutators: []func(*InstanceRecord){
				func(r *InstanceRecord) { r.RecordBallot = Ballot{Replica: 1} },
				func(r *InstanceRecord) { r.AcceptSeq, r.AcceptDeps = 1, []InstanceNum{0, 0, 0} },
				func(r *InstanceRecord) {
					r.AcceptEvidence = []AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{0, 0, 0}}}
				},
			},
		},
		{
			name: "without record ballot",
			prepare: func(r InstanceRecord) InstanceRecord {
				r.RecordBallot = Ballot{}
				r.AcceptEvidence = nil
				r.Checksum = checksumRecord(r, true, true, true, false, false, false)
				return r
			},
			verify: VerifyRecordChecksumWithoutRecordBallot,
			mutators: []func(*InstanceRecord){
				func(r *InstanceRecord) { r.RecordBallot = Ballot{Replica: 1} },
				func(r *InstanceRecord) {
					r.AcceptEvidence = []AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{0, 0, 0}}}
				},
			},
		},
		{
			name: "without TOQ",
			prepare: func(r InstanceRecord) InstanceRecord {
				r.RecordBallot = Ballot{}
				r.AcceptSeq = 0
				r.AcceptDeps = nil
				r.AcceptEvidence = nil
				r.ProcessAt = 0
				r.TOQPending = false
				r.Checksum = checksumRecord(r, true, false, false, false, false, false)
				return r
			},
			verify: VerifyRecordChecksumWithoutTOQ,
			mutators: []func(*InstanceRecord){
				func(r *InstanceRecord) { r.ProcessAt = 9 },
				func(r *InstanceRecord) { r.TOQPending = true },
				func(r *InstanceRecord) { r.AcceptSeq, r.AcceptDeps = 1, []InstanceNum{0, 0, 0} },
			},
		},
		{
			name: "without fast path or TOQ",
			prepare: func(r InstanceRecord) InstanceRecord {
				r.RecordBallot = Ballot{}
				r.AcceptSeq = 0
				r.AcceptDeps = nil
				r.AcceptEvidence = nil
				r.FastPathEligible = false
				r.ProcessAt = 0
				r.TOQPending = false
				r.Checksum = checksumRecord(r, false, false, false, false, false, false)
				return r
			},
			verify: VerifyRecordChecksumWithoutFastPathOrTOQ,
			mutators: []func(*InstanceRecord){
				func(r *InstanceRecord) { r.FastPathEligible = true },
				func(r *InstanceRecord) { r.ProcessAt = 9 },
				func(r *InstanceRecord) { r.TOQPending = true },
			},
		},
	}
	poisonOutcome := func(r *InstanceRecord) {
		r.ConfChangeResult = ConfChangeResult{
			Outcome: ConfChangeApplied,
			Conf:    ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}},
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			legacy := tc.prepare(base.Clone())
			if !tc.verify(legacy) {
				t.Fatalf("authentic legacy checksum rejected: %#v", legacy)
			}
			mutators := append([]func(*InstanceRecord){poisonOutcome}, tc.mutators...)
			for i, mutate := range mutators {
				poisoned := legacy.Clone()
				mutate(&poisoned)
				if tc.verify(poisoned) {
					t.Fatalf("unauthenticated newer-field mutation %d accepted: %#v", i, poisoned)
				}
			}
		})
	}
}
