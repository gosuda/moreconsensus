package epaxos

import (
	"bytes"
	"errors"
	"testing"
)

func TestUserCommittedCommandQueuesExecutedRecordOnlyAfterAdvance(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{ID: CommandID{Client: 12, Sequence: 34}, Payload: []byte("durable-user"), ConflictKeys: [][]byte{[]byte("durable-key")}}
	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	committed := requireCommittedForRef(t, rd, ref)
	if committed.Command.ID != cmd.ID || !bytes.Equal(committed.Command.Payload, cmd.Payload) || committed.Command.Kind != CommandUser {
		t.Fatalf("committed command = %#v, want user command %#v", committed.Command, cmd)
	}
	if !readyHasStatus(rd, ref, StatusCommitted) {
		t.Fatalf("ready for %s did not persist committed record: %#v", ref, rd.Records)
	}
	if readyHasStatus(rd, ref, StatusExecuted) {
		t.Fatalf("ready for %s persisted executed record before Advance: %#v", ref, rd.Records)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if got := requireStoredStatus(t, store, ref); got != StatusCommitted {
		t.Fatalf("stored status after committed ready = %s, want committed", got)
	}

	advanceOK(t, rn, rd)
	next := rn.Ready()
	if len(next.Committed) != 0 {
		t.Fatalf("post-Advance ready emitted application commands: %#v", next.Committed)
	}
	if !readyHasStatus(next, ref, StatusExecuted) {
		t.Fatalf("post-Advance ready for %s did not persist executed record: %#v", ref, next.Records)
	}
	if err := store.ApplyReady(next); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, next)
	if got := requireStoredStatus(t, store, ref); got != StatusExecuted {
		t.Fatalf("stored status after executed ready = %s, want executed", got)
	}
}

func TestAdvanceRejectsEmptyAcknowledgementAndAcceptsOutstandingReady(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("empty-ack"), ConflictKeys: [][]byte{[]byte("empty-ack-key")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) == 0 || len(rd.Committed) != 1 || rd.Committed[0].Ref != ref {
		t.Fatalf("ready for %s = %#v", ref, rd)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}

	advanceInvalid(t, rn, Ready{})
	if readyHasStatus(rn.pendingReady, ref, StatusExecuted) {
		t.Fatalf("empty acknowledgement enqueued executed record for %s: %#v", ref, rn.pendingReady.Records)
	}

	advanceOK(t, rn, rd)
	next := rn.Ready()
	if len(next.Committed) != 0 || !readyHasStatus(next, ref, StatusExecuted) {
		t.Fatalf("ready after accepting outstanding batch for %s = %#v", ref, next)
	}
}

func TestAdvanceRejectsNonEmptyAcknowledgementWithoutOutstandingReady(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}

	advanceInvalid(t, rn, Ready{Records: []InstanceRecord{{Ref: InstanceRef{Replica: 1, Instance: 99, Conf: 1}}}, MustSync: true})

	ref, err := rn.Propose(Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("after-empty-window"), ConflictKeys: [][]byte{[]byte("after-empty-window-key")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) == 0 || rd.Records[0].Ref != ref {
		t.Fatalf("ready after rejected acknowledgement = %#v, want record for %s", rd, ref)
	}
	advanceOK(t, rn, rd)
	if rn.HasReady() {
		t.Fatalf("accepted ready for %s left pending work: %#v", ref, rn.Ready())
	}
}

func TestAdvanceRejectsStrictAcknowledgementMismatchesWithoutDroppingReady(t *testing.T) {
	tests := []struct {
		name       string
		newNode    func(*testing.T) (*RawNode, Ready)
		mismatched func(*RawNode, Ready) Ready
		verify     func(*testing.T, *RawNode, Ready)
	}{
		{
			name: "sync bit",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rn.Propose(Command{ID: CommandID{Client: 3, Sequence: 1}, Payload: []byte("sync-bit"), ConflictKeys: [][]byte{[]byte("sync-bit-key")}}); err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Records) == 0 || !rd.MustSync {
					t.Fatalf("ready lacks durable record requiring sync: %#v", rd)
				}
				return rn, rd
			},
			mismatched: func(_ *RawNode, rd Ready) Ready {
				bad := cloneReady(rd)
				bad.MustSync = !bad.MustSync
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, _ Ready) {
				if rn.HasReady() {
					t.Fatalf("accepted ready left pending work: %#v", rn.Ready())
				}
			},
		},
		{
			name: "message beyond visible prefix",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), MaxReadyMessages: 1})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rn.Propose(Command{ID: CommandID{Client: 4, Sequence: 1}, Payload: []byte("capped-message"), ConflictKeys: [][]byte{[]byte("capped-message-key")}}); err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Messages) != 1 || len(rn.pendingReady.Messages) != 2 {
					t.Fatalf("ready messages = %#v pending=%#v, want one visible from two pending", rd.Messages, rn.pendingReady.Messages)
				}
				return rn, rd
			},
			mismatched: func(rn *RawNode, rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Messages = append(bad.Messages, rn.pendingReady.Messages[1].Clone())
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, rd Ready) {
				tail := rn.Ready()
				if len(tail.Records) != 0 || len(tail.Committed) != 0 || len(tail.Messages) != 1 || tail.Messages[0].Ref != rd.Messages[0].Ref {
					t.Fatalf("ready after capped acknowledgement = %#v", tail)
				}
				advanceOK(t, rn, tail)
				if rn.HasReady() {
					t.Fatalf("accepted capped tail left pending work: %#v", rn.Ready())
				}
			},
		},
		{
			name: "dependency element",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rn.Propose(Command{ID: CommandID{Client: 5, Sequence: 1}, Payload: []byte("dep-element"), ConflictKeys: [][]byte{[]byte("dep-element-key")}}); err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Records) == 0 || len(rd.Records[0].Deps) == 0 {
					t.Fatalf("ready lacks dependency vector: %#v", rd)
				}
				return rn, rd
			},
			mismatched: func(_ *RawNode, rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Records[0].Deps[0]++
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, _ Ready) {
				if rn.HasReady() {
					t.Fatalf("accepted ready left pending work: %#v", rn.Ready())
				}
			},
		},
		{
			name: "dependency vector length",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rn.Propose(Command{ID: CommandID{Client: 6, Sequence: 1}, Payload: []byte("dep-width"), ConflictKeys: [][]byte{[]byte("dep-width-key")}}); err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Records) == 0 || len(rd.Records[0].Deps) == 0 {
					t.Fatalf("ready lacks dependency vector: %#v", rd)
				}
				return rn, rd
			},
			mismatched: func(_ *RawNode, rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Records[0].Deps = append(bad.Records[0].Deps, 1)
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, _ Ready) {
				if rn.HasReady() {
					t.Fatalf("accepted ready left pending work: %#v", rn.Ready())
				}
			},
		},
		{
			name: "command payload bytes",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rn.Propose(Command{ID: CommandID{Client: 7, Sequence: 1}, Payload: []byte("payload-match"), ConflictKeys: [][]byte{[]byte("payload-match-key")}}); err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Records) == 0 || len(rd.Records[0].Command.Payload) == 0 {
					t.Fatalf("ready lacks command payload: %#v", rd)
				}
				return rn, rd
			},
			mismatched: func(_ *RawNode, rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Records[0].Command.Payload[0] = 'P'
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, _ Ready) {
				if rn.HasReady() {
					t.Fatalf("accepted ready left pending work: %#v", rn.Ready())
				}
			},
		},
		{
			name: "conflict key bytes",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := rn.Propose(Command{ID: CommandID{Client: 6, Sequence: 1}, Payload: []byte("conflict-key"), ConflictKeys: [][]byte{[]byte("conflict-key-a")}}); err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Records) == 0 || len(rd.Records[0].Command.ConflictKeys) != 1 || len(rd.Records[0].Command.ConflictKeys[0]) == 0 {
					t.Fatalf("ready lacks command conflict key: %#v", rd)
				}
				return rn, rd
			},
			mismatched: func(_ *RawNode, rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Records[0].Command.ConflictKeys[0][0] = 'C'
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, _ Ready) {
				if rn.HasReady() {
					t.Fatalf("accepted ready left pending work: %#v", rn.Ready())
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, rd := tt.newNode(t)
			advanceInvalid(t, rn, tt.mismatched(rn, rd))
			advanceOK(t, rn, rd)
			tt.verify(t, rn, rd)
		})
	}
}

func TestAdvanceAcceptsRecordOnlyAcknowledgementWithoutExecution(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("record-only"), ConflictKeys: [][]byte{[]byte("record-only-key")}})
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) == 0 || len(rd.Committed) != 1 || rd.Committed[0].Ref != ref {
		t.Fatalf("ready for %s = %#v", ref, rd)
	}
	if err := store.ApplyReady(Ready{Records: rd.Records, MustSync: rd.MustSync}); err != nil {
		t.Fatal(err)
	}

	advanceOK(t, rn, Ready{Records: rd.Records, MustSync: rd.MustSync})
	if readyHasStatus(rn.pendingReady, ref, StatusExecuted) {
		t.Fatalf("record-only acknowledgement enqueued executed record for %s: %#v", ref, rn.pendingReady.Records)
	}

	committedOnly := rn.Ready()
	if len(committedOnly.Records) != 0 || len(committedOnly.Committed) != 1 || committedOnly.Committed[0].Ref != ref {
		t.Fatalf("ready after record-only acknowledgement for %s = %#v", ref, committedOnly)
	}
	advanceOK(t, rn, committedOnly)
	next := rn.Ready()
	if len(next.Committed) != 0 || !readyHasStatus(next, ref, StatusExecuted) {
		t.Fatalf("ready after committed acknowledgement for %s = %#v", ref, next)
	}
}

func TestAdvanceRejectsMessageOrCommittedBeforeRecordBarrier(t *testing.T) {
	tests := []struct {
		name       string
		newNode    func(*testing.T) (*RawNode, Ready)
		mismatched func(Ready) Ready
		verify     func(*testing.T, *RawNode)
	}{
		{
			name: "message",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				for seq := uint64(1); seq <= 2; seq++ {
					if _, err := rn.Propose(Command{ID: CommandID{Client: 20, Sequence: seq}, Payload: []byte("barrier-message"), ConflictKeys: [][]byte{[]byte("barrier-message-key")}}); err != nil {
						t.Fatal(err)
					}
				}
				rd := rn.Ready()
				if len(rd.Records) < 2 || len(rd.Messages) == 0 {
					t.Fatalf("ready lacks message barrier inputs: %#v", rd)
				}
				return rn, rd
			},
			mismatched: func(rd Ready) Ready {
				return Ready{Records: rd.Records[:1], Messages: rd.Messages[:1], MustSync: rd.MustSync}
			},
			verify: func(t *testing.T, rn *RawNode) {
				if rn.HasReady() {
					t.Fatalf("pending work remained after matching acknowledgement: %#v", rn.Ready())
				}
			},
		},
		{
			name: "committed",
			newNode: func(t *testing.T) (*RawNode, Ready) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1)})
				if err != nil {
					t.Fatal(err)
				}
				for seq := uint64(1); seq <= 2; seq++ {
					if _, err := rn.Propose(Command{ID: CommandID{Client: 21, Sequence: seq}, Payload: []byte("barrier-committed"), ConflictKeys: [][]byte{[]byte("barrier-committed-key")}}); err != nil {
						t.Fatal(err)
					}
				}
				rd := rn.Ready()
				if len(rd.Records) < 2 || len(rd.Committed) == 0 {
					t.Fatalf("ready lacks committed barrier inputs: %#v", rd)
				}
				return rn, rd
			},
			mismatched: func(rd Ready) Ready {
				return Ready{Records: rd.Records[:1], Committed: rd.Committed[:1], MustSync: rd.MustSync}
			},
			verify: func(t *testing.T, rn *RawNode) {
				next := rn.Ready()
				if len(next.Committed) != 0 || len(next.Records) != 2 {
					t.Fatalf("executed ready after matching acknowledgement = %#v", next)
				}
				for _, rec := range next.Records {
					if rec.Status != StatusExecuted {
						t.Fatalf("executed ready record = %#v, want executed", rec)
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, rd := tt.newNode(t)
			advanceInvalid(t, rn, tt.mismatched(rd))
			advanceOK(t, rn, rd)
			tt.verify(t, rn)
		})
	}
}

func TestAdvanceRejectsMutatedReadyItemsWithoutDroppingPendingWork(t *testing.T) {
	tests := []struct {
		name       string
		newNode    func(*testing.T) (*RawNode, Ready, InstanceRef)
		mismatched func(Ready) Ready
		verify     func(*testing.T, *RawNode, InstanceRef)
	}{
		{
			name: "record item",
			newNode: func(t *testing.T) (*RawNode, Ready, InstanceRef) {
				store := NewMemoryStorage()
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
				if err != nil {
					t.Fatal(err)
				}
				ref, err := rn.Propose(Command{ID: CommandID{Client: 3, Sequence: 1}, Payload: []byte("mutated-record"), ConflictKeys: [][]byte{[]byte("mutated-record-key")}})
				if err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if err := store.ApplyReady(rd); err != nil {
					t.Fatal(err)
				}
				return rn, rd, ref
			},
			mismatched: func(rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Records[0].Status = StatusAccepted
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, ref InstanceRef) {
				next := rn.Ready()
				if len(next.Committed) != 0 || !readyHasStatus(next, ref, StatusExecuted) {
					t.Fatalf("ready after matching acknowledgement for %s = %#v", ref, next)
				}
			},
		},
		{
			name: "message item",
			newNode: func(t *testing.T) (*RawNode, Ready, InstanceRef) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				ref, err := rn.Propose(Command{ID: CommandID{Client: 4, Sequence: 1}, Payload: []byte("mutated-message"), ConflictKeys: [][]byte{[]byte("mutated-message-key")}})
				if err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Messages) == 0 {
					t.Fatalf("ready for %s has no messages: %#v", ref, rd)
				}
				return rn, rd, ref
			},
			mismatched: func(rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Messages[0].To = 99
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, ref InstanceRef) {
				if rn.HasReady() {
					t.Fatalf("pending work remained after matching acknowledgement for %s: %#v", ref, rn.Ready())
				}
			},
		},
		{
			name: "committed item",
			newNode: func(t *testing.T) (*RawNode, Ready, InstanceRef) {
				store := NewMemoryStorage()
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
				if err != nil {
					t.Fatal(err)
				}
				ref, err := rn.Propose(Command{ID: CommandID{Client: 5, Sequence: 1}, Payload: []byte("mutated-committed"), ConflictKeys: [][]byte{[]byte("mutated-committed-key")}})
				if err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if err := store.ApplyReady(rd); err != nil {
					t.Fatal(err)
				}
				return rn, rd, ref
			},
			mismatched: func(rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Committed[0].Seq++
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, ref InstanceRef) {
				next := rn.Ready()
				if len(next.Committed) != 0 || !readyHasStatus(next, ref, StatusExecuted) {
					t.Fatalf("ready after matching acknowledgement for %s = %#v", ref, next)
				}
			},
		},
		{
			name: "longer record slice",
			newNode: func(t *testing.T) (*RawNode, Ready, InstanceRef) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				ref, err := rn.Propose(Command{ID: CommandID{Client: 6, Sequence: 1}, Payload: []byte("long-record"), ConflictKeys: [][]byte{[]byte("long-record-key")}})
				if err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Records) == 0 {
					t.Fatalf("ready for %s has no records: %#v", ref, rd)
				}
				return rn, rd, ref
			},
			mismatched: func(rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Records = append(bad.Records, bad.Records[0].Clone())
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, ref InstanceRef) {
				if rn.HasReady() {
					t.Fatalf("pending work remained after matching acknowledgement for %s: %#v", ref, rn.Ready())
				}
			},
		},
		{
			name: "longer message slice",
			newNode: func(t *testing.T) (*RawNode, Ready, InstanceRef) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
				if err != nil {
					t.Fatal(err)
				}
				ref, err := rn.Propose(Command{ID: CommandID{Client: 7, Sequence: 1}, Payload: []byte("long-message"), ConflictKeys: [][]byte{[]byte("long-message-key")}})
				if err != nil {
					t.Fatal(err)
				}
				rd := rn.Ready()
				if len(rd.Messages) == 0 {
					t.Fatalf("ready for %s has no messages: %#v", ref, rd)
				}
				return rn, rd, ref
			},
			mismatched: func(rd Ready) Ready {
				bad := cloneReady(rd)
				bad.Messages = append(bad.Messages, bad.Messages[0].Clone())
				return bad
			},
			verify: func(t *testing.T, rn *RawNode, ref InstanceRef) {
				if rn.HasReady() {
					t.Fatalf("pending work remained after matching acknowledgement for %s: %#v", ref, rn.Ready())
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, rd, ref := tt.newNode(t)
			advanceInvalid(t, rn, tt.mismatched(rd))
			advanceOK(t, rn, rd)
			tt.verify(t, rn, ref)
		})
	}
}

func TestAdvanceRejectsOverlongCommittedAcknowledgement(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{ID: CommandID{Client: 90, Sequence: 12}, Payload: []byte("capped-user"), ConflictKeys: [][]byte{[]byte("capped-key")}}
	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	if len(rd.Committed) != 1 || rd.Committed[0].Ref != ref {
		t.Fatalf("ready committed commands = %#v, want only %s", rd.Committed, ref)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}

	otherRef := InstanceRef{Replica: 1, Instance: ref.Instance + 10, Conf: 1}
	other := InstanceRecord{
		Ref:     otherRef,
		Status:  StatusExecuted,
		Seq:     99,
		Deps:    rn.q.deps(),
		Command: Command{ID: CommandID{Client: 91, Sequence: 13}, Payload: []byte("unrelated-user"), ConflictKeys: [][]byte{[]byte("unrelated-key")}},
	}
	other.Checksum = ChecksumRecord(other)
	rn.instances[otherRef] = &instance{rec: other, phase: phaseCommitted}

	ack := cloneReady(rd)
	ack.Committed = append(ack.Committed, CommittedCommand{
		Ref:     otherRef,
		Seq:     other.Seq,
		Deps:    append([]InstanceNum(nil), other.Deps...),
		Command: other.Command.Clone(),
	})
	advanceInvalid(t, rn, ack)

	advanceOK(t, rn, rd)
	next := rn.Ready()
	if len(next.Committed) != 0 {
		t.Fatalf("post-Advance ready emitted application commands: %#v", next.Committed)
	}
	if !readyHasStatus(next, ref, StatusExecuted) {
		t.Fatalf("post-Advance ready for %s did not persist executed record: %#v", ref, next.Records)
	}
	if readyHasStatus(next, otherRef, StatusExecuted) {
		t.Fatalf("post-Advance ready included unacknowledged executed record %s: %#v", otherRef, next.Records)
	}
	if err := store.ApplyReady(next); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Instance(otherRef); ok {
		t.Fatalf("stored unrelated executed record %s from an over-long acknowledgement", otherRef)
	}
}

func TestRestartWithOnlyCommittedRecordReemitsUserCommand(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{ID: CommandID{Client: 56, Sequence: 78}, Payload: []byte("replay-user"), ConflictKeys: [][]byte{[]byte("replay-key")}}
	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	if got := requireStoredStatus(t, store, ref); got != StatusCommitted {
		t.Fatalf("stored status before restart = %s, want committed", got)
	}

	restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	replayed := restarted.Ready()
	committed := requireCommittedForRef(t, replayed, ref)
	if committed.Command.ID != cmd.ID || committed.Command.Kind != CommandUser || !bytes.Equal(committed.Command.Payload, cmd.Payload) {
		t.Fatalf("replayed command = %#v, want %#v", committed.Command, cmd)
	}
	if len(committed.Command.ConflictKeys) != 1 || !bytes.Equal(committed.Command.ConflictKeys[0], cmd.ConflictKeys[0]) {
		t.Fatalf("replayed command keys = %#v, want %#v", committed.Command.ConflictKeys, cmd.ConflictKeys)
	}
	if readyHasStatus(replayed, ref, StatusExecuted) {
		t.Fatalf("restart ready for %s persisted executed record before Advance: %#v", ref, replayed.Records)
	}
}

func TestConfChangeExecutesWithoutApplicationCommit(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(1), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 2})
	if err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	if len(rd.Committed) != 0 {
		t.Fatalf("config change appeared as application command: %#v", rd.Committed)
	}
	if !readyHasStatus(rd, ref, StatusExecuted) {
		t.Fatalf("config change ready for %s did not persist executed record: %#v", ref, rd.Records)
	}
	conf := rn.Status().Conf
	if conf.ID != 2 || !conf.Contains(1) || !conf.Contains(2) {
		t.Fatalf("config after executed change = %#v, want voters 1 and 2 at id 2", conf)
	}
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
	if rn.HasReady() {
		t.Fatalf("config change left additional ready work: %#v", rn.Ready())
	}
	stored, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("missing stored config change record %s", ref)
	}
	if stored.Status != StatusExecuted || stored.Command.Kind != CommandConfChange {
		t.Fatalf("stored config change record = %#v, want executed config change", stored)
	}
}

func advanceOK(t *testing.T, rn *RawNode, rd Ready) {
	t.Helper()
	if err := rn.Advance(rd); err != nil {
		t.Fatalf("Advance(%#v) err=%v", rd, err)
	}
}

func advanceInvalid(t *testing.T, rn *RawNode, rd Ready) {
	t.Helper()
	if err := rn.Advance(rd); !errors.Is(err, ErrInvalidReady) {
		t.Fatalf("Advance(%#v) err=%v, want %v", rd, err, ErrInvalidReady)
	}
}

func cloneReady(rd Ready) Ready {
	out := Ready{MustSync: rd.MustSync}
	out.Records = make([]InstanceRecord, len(rd.Records))
	for i := range rd.Records {
		out.Records[i] = rd.Records[i].Clone()
	}
	out.Messages = make([]Message, len(rd.Messages))
	for i := range rd.Messages {
		out.Messages[i] = rd.Messages[i].Clone()
	}
	out.Committed = make([]CommittedCommand, len(rd.Committed))
	for i := range rd.Committed {
		out.Committed[i] = rd.Committed[i].Clone()
	}
	return out
}

func requireCommittedForRef(t *testing.T, rd Ready, ref InstanceRef) CommittedCommand {
	t.Helper()
	for _, c := range rd.Committed {
		if c.Ref == ref {
			return c
		}
	}
	t.Fatalf("ready did not include committed command %s: %#v", ref, rd.Committed)
	return CommittedCommand{}
}

func readyHasStatus(rd Ready, ref InstanceRef, status Status) bool {
	for _, rec := range rd.Records {
		if rec.Ref == ref && rec.Status == status {
			return true
		}
	}
	return false
}

func requireStoredStatus(t *testing.T, store *MemoryStorage, ref InstanceRef) Status {
	t.Helper()
	rec, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("missing stored record %s", ref)
	}
	return rec.Status
}
