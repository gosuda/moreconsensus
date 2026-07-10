package epaxos

import (
	"reflect"
	"testing"
)

func TestCloneIntoReusesCapacityWithoutAliasing(t *testing.T) {
	largeCommand := Command{
		ID:           CommandID{Client: 601, Sequence: 1},
		Payload:      []byte("large-command-payload"),
		ConflictKeys: [][]byte{[]byte("large-key-one"), []byte("large-key-two")},
	}
	smallCommand := Command{
		ID:           CommandID{Client: 601, Sequence: 2},
		Payload:      []byte("small"),
		ConflictKeys: [][]byte{[]byte("key")},
	}
	largeEvidence := []AcceptEvidence{
		{Sender: 1, Seq: 7, Deps: []InstanceNum{1, 2, 3, 4, 5}},
		{Sender: 2, Seq: 8, Deps: []InstanceNum{5, 4, 3, 2, 1}},
	}
	smallEvidence := []AcceptEvidence{{Sender: 2, Seq: 3, Deps: []InstanceNum{0, 1, 0}}}

	var commandDst Command
	largeCommand.CloneInto(&commandDst)
	var confDst ConfState
	(ConfState{ID: 9, Voters: []ReplicaID{1, 2, 3, 4, 5, 6, 7}}).CloneInto(&confDst)
	largeMessage := Message{Deps: []InstanceNum{1, 2, 3, 4, 5}, AcceptDeps: []InstanceNum{5, 4, 3, 2, 1}, AcceptEvidence: largeEvidence, Command: largeCommand}
	var messageDst Message
	largeMessage.CloneInto(&messageDst)
	largeRecord := InstanceRecord{
		Deps:           []InstanceNum{1, 2, 3, 4, 5},
		AcceptDeps:     []InstanceNum{5, 4, 3, 2, 1},
		AcceptEvidence: largeEvidence,
		Command:        largeCommand,
		ConfChangeResult: ConfChangeResult{
			Outcome: ConfChangeApplied,
			Conf:    ConfState{ID: 9, Voters: []ReplicaID{1, 2, 3, 4, 5, 6, 7}},
		},
	}
	var recordDst InstanceRecord
	largeRecord.CloneInto(&recordDst)
	var committedDst CommittedCommand
	(CommittedCommand{Deps: []InstanceNum{1, 2, 3, 4, 5}, Command: largeCommand}).CloneInto(&committedDst)
	var attrsDst Attributes
	(Attributes{Seq: 9, Deps: []InstanceNum{1, 2, 3, 4, 5}}).CloneInto(&attrsDst)

	smallConf := ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}}
	smallMessage := Message{Type: MsgAcceptResp, Deps: []InstanceNum{0, 1, 0}, AcceptDeps: []InstanceNum{0, 1, 0}, AcceptEvidence: smallEvidence, Command: smallCommand}
	smallRecord := InstanceRecord{
		Ref:            InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Deps:           []InstanceNum{0, 1, 0},
		AcceptDeps:     []InstanceNum{0, 1, 0},
		AcceptEvidence: smallEvidence,
		Command:        smallCommand,
		ConfChangeResult: ConfChangeResult{
			Outcome: ConfChangeApplied,
			Conf:    smallConf,
		},
	}
	smallCommitted := CommittedCommand{Ref: smallRecord.Ref, Seq: 3, Deps: []InstanceNum{0, 1, 0}, Command: smallCommand}
	smallAttrs := Attributes{Seq: 3, Deps: []InstanceNum{0, 1, 0}}

	allocs := testing.AllocsPerRun(1000, func() {
		smallCommand.CloneInto(&commandDst)
		smallConf.CloneInto(&confDst)
		smallMessage.CloneInto(&messageDst)
		smallRecord.CloneInto(&recordDst)
		smallCommitted.CloneInto(&committedDst)
		smallAttrs.CloneInto(&attrsDst)
	})
	if allocs != 0 {
		t.Fatalf("warmed CloneInto allocations=%v, want 0", allocs)
	}
	if !reflect.DeepEqual(commandDst, smallCommand) ||
		!reflect.DeepEqual(confDst, smallConf) ||
		!reflect.DeepEqual(messageDst, smallMessage) ||
		!reflect.DeepEqual(recordDst, smallRecord) ||
		!reflect.DeepEqual(committedDst, smallCommitted) ||
		!reflect.DeepEqual(attrsDst, smallAttrs) {
		t.Fatalf("CloneInto result mismatch: command=%#v conf=%#v message=%#v record=%#v committed=%#v attrs=%#v", commandDst, confDst, messageDst, recordDst, committedDst, attrsDst)
	}
	if commandDst.ConflictKeys[:cap(commandDst.ConflictKeys)][1] != nil {
		t.Fatal("Command.CloneInto retained an inactive conflict-key reference")
	}
	if !reflect.DeepEqual(messageDst.AcceptEvidence[:cap(messageDst.AcceptEvidence)][1], AcceptEvidence{}) ||
		!reflect.DeepEqual(recordDst.AcceptEvidence[:cap(recordDst.AcceptEvidence)][1], AcceptEvidence{}) {
		t.Fatal("CloneInto retained an inactive AcceptEvidence reference")
	}

	commandDst.Payload[0] = 'X'
	commandDst.ConflictKeys[0][0] = 'Y'
	messageDst.AcceptEvidence[0].Deps[0] = 99
	recordDst.ConfChangeResult.Conf.Voters[0] = 99
	if string(smallCommand.Payload) != "small" || string(smallCommand.ConflictKeys[0]) != "key" || smallEvidence[0].Deps[0] != 0 || smallConf.Voters[0] != 1 {
		t.Fatal("CloneInto destination mutation reached a source value")
	}
}
