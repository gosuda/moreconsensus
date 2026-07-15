package epaxos

import (
	"errors"
	"testing"
)

func TestMessageValidateRejectsImpossiblePreAcceptedRecoveryEvidence(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	target := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	base := Message{
		From:             2,
		To:               1,
		Ref:              target,
		Ballot:           Ballot{Number: 1, Replica: 1},
		RecordBallot:     Ballot{Replica: 1},
		RecordStatus:     StatusPreAccepted,
		Seq:              2,
		Deps:             []InstanceNum{0, 1, 0},
		Kind:             EntryCommand,
		Command:          optimizedTestCommand("preaccepted-recovery-evidence", "preaccepted-recovery-evidence-key"),
		FastPathEligible: true,
	}
	for _, typ := range []MessageType{MsgPrepareResp, MsgEvidenceResp} {
		t.Run(typ.String(), func(t *testing.T) {
			valid := base.Clone()
			valid.Type = typ
			if typ == MsgEvidenceResp {
				valid.Ref = conflict
				valid.ConflictRef = target
				valid.RecordBallot = Ballot{Replica: conflict.Replica}
			}
			if err := valid.Validate(conf); err != nil {
				t.Fatalf("valid fast PreAccepted recovery evidence rejected: %v", err)
			}
			mutations := []func(*Message){
				func(m *Message) {
					m.AcceptSeq = 3
					m.AcceptDeps = []InstanceNum{0, 1, 0}
				},
				func(m *Message) {
					m.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 3, Deps: []InstanceNum{0, 1, 0}}}
				},
				func(m *Message) {
					m.RecordBallot = Ballot{Number: 1, Replica: m.Ref.Replica}
				},
				func(m *Message) {
					m.RecordBallot = Ballot{Replica: 3}
				},
			}
			for i, mutate := range mutations {
				poisoned := valid.Clone()
				mutate(&poisoned)
				if err := poisoned.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("impossible recovery evidence mutation %d err=%v, want ErrInvalidMessage: %#v", i, err, poisoned)
				}
			}
		})
	}
}
