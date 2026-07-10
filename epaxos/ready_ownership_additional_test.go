package epaxos

import (
	"errors"
	"reflect"
	"testing"
)

func TestReadyDeepIsolationAndAllocationFreeAcknowledgement(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 1, Instance: 70, Conf: 1}
	rec := InstanceRecord{
		Ref:          ref,
		Ballot:       Ballot{Number: 1, Replica: 1},
		RecordBallot: Ballot{Number: 1, Replica: 1},
		Status:       StatusAccepted,
		Seq:          4,
		Deps:         []InstanceNum{0, 2, 0},
		AcceptSeq:    5,
		AcceptDeps:   []InstanceNum{0, 3, 0},
		AcceptEvidence: []AcceptEvidence{{
			Sender: 2,
			Seq:    5,
			Deps:   []InstanceNum{0, 3, 0},
		}},
		Command: Command{
			ID:           CommandID{Client: 501, Sequence: 1},
			Payload:      []byte("ready-record-command"),
			ConflictKeys: [][]byte{[]byte("ready-record-key")},
		},
		ConfChangeResult: ConfChangeResult{
			Outcome: ConfChangeApplied,
			Conf:    ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}},
		},
	}
	rec.Checksum = ChecksumRecord(rec)
	rn.enqueueRecord(rec)
	rn.enqueueMessage(Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       rec.Ballot,
		RecordBallot: rec.RecordBallot,
		RecordStatus: StatusAccepted,
		Seq:          rec.Seq,
		Deps:         rec.Deps,
		AcceptEvidence: []AcceptEvidence{{
			Sender: 2,
			Seq:    5,
			Deps:   []InstanceNum{0, 3, 0},
		}},
	})
	rn.enqueueCommitted(CommittedCommand{Ref: ref, Seq: rec.Seq, Deps: rec.Deps, Command: rec.Command})

	mutated := rn.Ready()
	mutated.Records[0].Deps[0] = 99
	mutated.Records[0].AcceptDeps[0] = 99
	mutated.Records[0].AcceptEvidence[0].Deps[0] = 99
	mutated.Records[0].ConfChangeResult.Conf.Voters[0] = 99
	mutated.Records[0].Command.Payload[0] = 'X'
	mutated.Messages[0].Deps[0] = 99
	mutated.Messages[0].AcceptEvidence[0].Deps[0] = 99
	mutated.Committed[0].Deps[0] = 99
	mutated.Committed[0].Command.Payload[0] = 'Y'

	pendingRecord := rn.pendingReady.Records[0]
	if pendingRecord.Deps[0] != 0 ||
		pendingRecord.AcceptDeps[0] != 0 ||
		pendingRecord.AcceptEvidence[0].Deps[0] != 0 ||
		pendingRecord.ConfChangeResult.Conf.Voters[0] != 1 ||
		string(pendingRecord.Command.Payload) != "ready-record-command" {
		t.Fatalf("Ready record mutation reached pending state: %#v", pendingRecord)
	}
	if rn.pendingReady.Messages[0].Deps[0] != 0 || rn.pendingReady.Messages[0].AcceptEvidence[0].Deps[0] != 0 {
		t.Fatalf("Ready message mutation reached pending state: %#v", rn.pendingReady.Messages[0])
	}
	if rn.pendingReady.Committed[0].Deps[0] != 0 || string(rn.pendingReady.Committed[0].Command.Payload) != "ready-record-command" {
		t.Fatalf("Ready committed mutation reached pending state: %#v", rn.pendingReady.Committed[0])
	}
	if err := rn.Advance(mutated); !errors.Is(err, ErrInvalidReady) {
		t.Fatalf("Advance mutated Ready err=%v, want ErrInvalidReady", err)
	}

	canonical := rn.Ready()
	var validationErr error
	allocs := testing.AllocsPerRun(1000, func() {
		validationErr = rn.validateReadyAck(canonical)
	})
	if validationErr != nil {
		t.Fatalf("validateReadyAck canonical Ready: %v", validationErr)
	}
	if allocs != 0 {
		t.Fatalf("validateReadyAck allocations=%v, want 0", allocs)
	}
	recordCap := cap(rn.pendingReady.Records)
	messageCap := cap(rn.pendingReady.Messages)
	committedCap := cap(rn.pendingReady.Committed)
	if err := rn.Advance(canonical); err != nil {
		t.Fatal(err)
	}
	if len(rn.pendingReady.Records) != 0 || len(rn.pendingReady.Messages) != 0 || len(rn.pendingReady.Committed) != 0 {
		t.Fatalf("Advance left acknowledged entries: %#v", rn.pendingReady)
	}
	if cap(rn.pendingReady.Records) != recordCap || cap(rn.pendingReady.Messages) != messageCap || cap(rn.pendingReady.Committed) != committedCap {
		t.Fatalf("Advance discarded reusable queue capacity: records %d/%d messages %d/%d committed %d/%d", cap(rn.pendingReady.Records), recordCap, cap(rn.pendingReady.Messages), messageCap, cap(rn.pendingReady.Committed), committedCap)
	}
	if !reflect.DeepEqual(rn.pendingReady.Records[:cap(rn.pendingReady.Records)][0], InstanceRecord{}) ||
		!reflect.DeepEqual(rn.pendingReady.Messages[:cap(rn.pendingReady.Messages)][0], Message{}) ||
		!reflect.DeepEqual(rn.pendingReady.Committed[:cap(rn.pendingReady.Committed)][0], CommittedCommand{}) {
		t.Fatal("Advance retained acknowledged pointer-bearing queue entries")
	}
}
