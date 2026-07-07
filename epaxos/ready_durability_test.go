package epaxos

import (
	"bytes"
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

	rn.Advance(rd)
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
	rn.Advance(next)
	if got := requireStoredStatus(t, store, ref); got != StatusExecuted {
		t.Fatalf("stored status after executed ready = %s, want executed", got)
	}
}

func TestAdvanceCapsExecutedRecordAcknowledgementsToPendingCommitted(t *testing.T) {
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

	ack := rd
	ack.Committed = append(append([]CommittedCommand(nil), rd.Committed...), CommittedCommand{
		Ref:     otherRef,
		Seq:     other.Seq,
		Deps:    append([]InstanceNum(nil), other.Deps...),
		Command: other.Command.Clone(),
	})
	rn.Advance(ack)

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
	rn.Advance(rd)
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
