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

	frozenRecord := rn.frozenReady.Records[0]
	if frozenRecord.Deps[0] != 0 ||
		frozenRecord.AcceptDeps[0] != 0 ||
		frozenRecord.AcceptEvidence[0].Deps[0] != 0 ||
		frozenRecord.ConfChangeResult.Conf.Voters[0] != 1 ||
		string(frozenRecord.Command.Payload) != "ready-record-command" {
		t.Fatalf("Ready record mutation reached frozen state: %#v", frozenRecord)
	}
	if rn.frozenReady.Messages[0].Deps[0] != 0 || rn.frozenReady.Messages[0].AcceptEvidence[0].Deps[0] != 0 {
		t.Fatalf("Ready message mutation reached frozen state: %#v", rn.frozenReady.Messages[0])
	}
	if rn.frozenReady.Committed[0].Deps[0] != 0 || string(rn.frozenReady.Committed[0].Command.Payload) != "ready-record-command" {
		t.Fatalf("Ready committed mutation reached frozen state: %#v", rn.frozenReady.Committed[0])
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
	recordCap := cap(rn.frozenReady.Records)
	messageCap := cap(rn.frozenReady.Messages)
	committedCap := cap(rn.frozenReady.Committed)
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

func TestReadyIntoOwnershipRetryAndInactiveClearing(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.ReadyInto(nil); !errors.Is(err, ErrInvalidReady) {
		t.Fatalf("ReadyInto(nil) err=%v, want %v", err, ErrInvalidReady)
	}
	if _, err := rn.Propose(Command{
		ID:           CommandID{Client: 900, Sequence: 1},
		Payload:      []byte("ready-into"),
		ConflictKeys: [][]byte{[]byte("ready-into-key")},
	}); err != nil {
		t.Fatal(err)
	}
	var first Ready
	if err := rn.ReadyInto(&first); err != nil {
		t.Fatal(err)
	}
	var retry Ready
	if err := rn.ReadyInto(&retry); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, retry) {
		t.Fatalf("ReadyInto retry changed frozen batch: first=%#v retry=%#v", first, retry)
	}
	first.Records[0].Command.Payload[0] = 'X'
	first.Messages[0].Command.Payload[0] = 'Y'
	var isolated Ready
	if err := rn.ReadyInto(&isolated); err != nil {
		t.Fatal(err)
	}
	if string(isolated.Records[0].Command.Payload) != "ready-into" ||
		string(isolated.Messages[0].Command.Payload) != "ready-into" {
		t.Fatalf("caller mutation reached frozen Ready: %#v", isolated)
	}

	var validationErr error
	allocs := testing.AllocsPerRun(1000, func() {
		validationErr = rn.ReadyInto(&retry)
	})
	if validationErr != nil {
		t.Fatal(validationErr)
	}
	if allocs != 0 {
		t.Fatalf("warmed fixed-shape ReadyInto retry allocations=%v, want 0", allocs)
	}
	if err := rn.Advance(isolated); err != nil {
		t.Fatal(err)
	}
	if err := rn.ReadyInto(&retry); err != nil {
		t.Fatal(err)
	}
	if !retry.Empty() {
		t.Fatalf("inactive ReadyInto retained references: %#v", retry)
	}
}

func TestReadyIntoCappedTailUsesDisjointWritableCapacity(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), MaxReadyMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rn.Propose(Command{
		ID:           CommandID{Client: 901, Sequence: 1},
		Payload:      []byte("ready-tail"),
		ConflictKeys: [][]byte{[]byte("ready-tail-key")},
	}); err != nil {
		t.Fatal(err)
	}
	var prefix Ready
	if err := rn.ReadyInto(&prefix); err != nil {
		t.Fatal(err)
	}
	if len(prefix.Messages) != 1 || cap(rn.frozenReady.Messages) != 1 || len(rn.pendingReady.Messages) != 1 {
		t.Fatalf("capped freeze prefix=%d/%d retained=%d", len(prefix.Messages), cap(rn.frozenReady.Messages), len(rn.pendingReady.Messages))
	}
	retainedTo := rn.pendingReady.Messages[0].To
	rn.nextReady.Messages = append(rn.nextReady.Messages, Message{To: 7})
	if rn.pendingReady.Messages[0].To != retainedTo {
		t.Fatal("next Ready append overwrote retained message tail")
	}
	if err := rn.Advance(prefix); err != nil {
		t.Fatal(err)
	}
	var tail Ready
	if err := rn.ReadyInto(&tail); err != nil {
		t.Fatal(err)
	}
	if len(tail.Messages) != 1 || tail.Messages[0].To != retainedTo {
		t.Fatalf("retained tail ordering changed: %#v", tail.Messages)
	}
}
