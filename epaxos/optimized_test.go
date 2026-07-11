package epaxos

import (
	"bytes"
	"errors"
	"testing"
)

func optimizedTestCommand(payload, key string) Command {
	return Command{ID: CommandID{Client: 100, Sequence: 1}, Payload: []byte(payload), ConflictKeys: [][]byte{[]byte(key)}}
}

func optimizedNewRawNode(t *testing.T, id ReplicaID, voters int) *RawNode {
	t.Helper()
	rn, err := NewRawNode(Config{ID: id, Voters: makeIDs(voters), RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

func optimizedApplyAndAdvance(t *testing.T, store *MemoryStorage, rn *RawNode, rd Ready) {
	t.Helper()
	if store != nil {
		if err := store.ApplyReady(rd); err != nil {
			t.Fatal(err)
		}
	}
	advanceOK(t, rn, rd)
}

func optimizedSeedRecord(t *testing.T, rn *RawNode, rec InstanceRecord) {
	t.Helper()
	if rec.Deps == nil {
		rec.Deps = rn.q.deps()
	}
	if rec.Status >= StatusPreAccepted && rec.RecordBallot == (Ballot{}) {
		rec.RecordBallot = rec.Ballot
	}
	if rec.TimingDomain == TimingDomainUntimed {
		if rn.toqEnabled && rec.Status >= StatusPreAccepted {
			rec.TimingDomain = TimingDomainTOQ
		} else if rn.timeOptimization && rec.ProcessAt != 0 {
			rec.TimingDomain = TimingDomainLogical
		}
	}
	rec.Checksum = ChecksumRecord(rec)
	rn.installInstance(&instance{rec: rec, phase: phaseFromStatus(rec.Status), processAt: rec.ProcessAt})
	if rec.Status >= StatusPreAccepted {
		rn.indexConflicts(rec)
	}
}

func optimizedRequireRecord(t *testing.T, rd Ready, ref InstanceRef) InstanceRecord {
	t.Helper()
	for _, rec := range rd.Records {
		if rec.Ref == ref {
			return rec
		}
	}
	t.Fatalf("ready records do not include %s: %#v", ref, rd.Records)
	return InstanceRecord{}
}

func optimizedRequireMessage(t *testing.T, messages []Message, typ MessageType, to ReplicaID) Message {
	t.Helper()
	for _, m := range messages {
		if m.Type == typ && (to == 0 || m.To == to) {
			return m
		}
	}
	t.Fatalf("messages do not include %s to %d: %#v", typ, to, messages)
	return Message{}
}

func optimizedRequireCanonicalInstanceRequest(t *testing.T, msg Message, rec InstanceRecord) {
	t.Helper()
	if msg.Ref != rec.Ref ||
		msg.Ballot != rec.Ballot ||
		msg.RecordBallot != (Ballot{}) ||
		msg.RecordStatus != StatusNone ||
		msg.Seq != rec.Seq ||
		!instanceNumsEqual(msg.Deps, rec.Deps) ||
		!commandEqual(msg.Command, rec.Command) {
		t.Fatalf("%s request = %#v, want ballot %#v, zero record metadata, seq %d deps %v command %#v", msg.Type, msg, rec.Ballot, rec.Seq, rec.Deps, rec.Command)
	}
}

func optimizedRequireCanonicalCommitRequest(t *testing.T, msg Message, rec InstanceRecord) {
	t.Helper()
	if msg.Ref != rec.Ref ||
		msg.Ballot != rec.Ballot ||
		msg.RecordBallot != recordTupleBallot(rec) ||
		msg.RecordBallot == (Ballot{}) ||
		msg.RecordStatus != StatusNone ||
		msg.Seq != rec.Seq ||
		!instanceNumsEqual(msg.Deps, rec.Deps) ||
		!commandEqual(msg.Command, rec.Command) {
		t.Fatalf("commit request = %#v, want ballot %#v, record ballot %#v, zero record status, seq %d deps %v command %#v", msg, rec.Ballot, recordTupleBallot(rec), rec.Seq, rec.Deps, rec.Command)
	}
}

func optimizedRequireCanonicalTryPreAcceptResp(t *testing.T, msg Message, ref InstanceRef, ballot Ballot, attrs Attributes) {
	t.Helper()
	if msg.Reject ||
		msg.Ref != ref ||
		msg.Ballot != ballot ||
		msg.RecordBallot != (Ballot{}) ||
		msg.RecordStatus != StatusPreAccepted ||
		msg.Seq != attrs.Seq ||
		!instanceNumsEqual(msg.Deps, attrs.Deps) ||
		!commandEqual(msg.Command, Command{}) {
		t.Fatalf("TryPreAccept response = %#v, want ballot %#v, preaccepted seq %d deps %v with zero record ballot and empty command", msg, ballot, attrs.Seq, attrs.Deps)
	}
}

func optimizedRequireCanonicalTryReject(t *testing.T, msg Message, ref InstanceRef, ballot, hint Ballot, reason RejectReason, conflictRef InstanceRef, conflictStatus Status, zeroDeps []InstanceNum) {
	t.Helper()
	if !msg.Reject ||
		msg.Ref != ref ||
		msg.Ballot != ballot ||
		msg.RejectHint != hint ||
		msg.RejectReason != reason ||
		msg.ConflictRef != conflictRef ||
		msg.ConflictStatus != conflictStatus ||
		msg.RecordBallot != (Ballot{}) ||
		msg.RecordStatus != StatusNone ||
		msg.Seq != 0 ||
		!instanceNumsEqual(msg.Deps, zeroDeps) ||
		!commandEqual(msg.Command, Command{}) ||
		msg.AcceptSeq != 0 ||
		len(msg.AcceptDeps) != 0 ||
		len(msg.AcceptEvidence) != 0 {
		t.Fatalf("TryPreAccept rejection = %#v, want ref %s ballot %#v hint %#v reason %v conflict %s/%s and zero tuple metadata", msg, ref, ballot, hint, reason, conflictRef, conflictStatus)
	}
}

func optimizedRequireNoMessageType(t *testing.T, messages []Message, typ MessageType) {
	t.Helper()
	for _, m := range messages {
		if m.Type == typ {
			t.Fatalf("unexpected %s message in %#v", typ, messages)
		}
	}
}

func optimizedStartRecovery(t *testing.T, voters int, ref InstanceRef) (*RawNode, *instance, Ballot) {
	t.Helper()
	rn := optimizedNewRawNode(t, 1, voters)
	inst := &instance{rec: InstanceRecord{Ref: ref, Status: StatusNone, Deps: rn.q.deps()}}
	rn.instances[ref] = inst
	rn.startPrepare(inst)
	ballot := inst.rec.Ballot
	rd := rn.Ready()
	advanceOK(t, rn, rd)
	return rn, inst, ballot
}

func TestOptimizedMessageValidationAndStrings(t *testing.T) {
	for _, tt := range []struct {
		typ  MessageType
		want string
	}{
		{typ: MsgTryPreAccept, want: "try-pre-accept"},
		{typ: MsgTryPreAcceptResp, want: "try-pre-accept-resp"},
	} {
		if got := tt.typ.String(); got != tt.want {
			t.Fatalf("%d.String() = %q, want %q", tt.typ, got, tt.want)
		}
	}

	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	ballot := Ballot{Number: 1, Replica: 1}
	cmd := optimizedTestCommand("try", "try-key")
	deps := []InstanceNum{0, 0, 0}

	validTry := Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: ref, Ballot: ballot, Seq: 1, Deps: deps, Command: cmd}
	if err := validTry.Validate(conf); err != nil {
		t.Fatalf("valid TryPreAccept rejected: %v", err)
	}
	validResp := Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: ballot, Seq: 1, Deps: deps, RecordStatus: StatusPreAccepted}
	if err := validResp.Validate(conf); err != nil {
		t.Fatalf("valid TryPreAcceptResp rejected: %v", err)
	}
	shortResp := validResp.Clone()
	shortResp.Deps = shortResp.Deps[:2]
	if err := shortResp.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("short non-reject TryPreAcceptResp deps err=%v, want ErrInvalidMessage", err)
	}
	overwideEvidence := validResp.Clone()
	overwideEvidence.DepsCommitted = 1 << uint(len(conf.Voters))
	if err := overwideEvidence.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("overwide DepsCommitted evidence err=%v, want ErrInvalidMessage", err)
	}

	rejectResp := Message{
		Type:           MsgTryPreAcceptResp,
		From:           2,
		To:             1,
		Ref:            ref,
		Ballot:         ballot,
		Deps:           []InstanceNum{0, 0, 0},
		Reject:         true,
		RejectReason:   RejectCommittedConflict,
		ConflictRef:    InstanceRef{Replica: 2, Instance: 7, Conf: 1},
		ConflictStatus: StatusCommitted,
	}
	if err := rejectResp.Validate(conf); err != nil {
		t.Fatalf("reject TryPreAcceptResp with metadata rejected: %v", err)
	}
	staleResp := Message{
		Type:         MsgTryPreAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       Ballot{Number: 2, Replica: 2},
		Deps:         []InstanceNum{0, 0, 0},
		Reject:       true,
		RejectReason: RejectStaleBallot,
		RejectHint:   Ballot{Number: 2, Replica: 2},
	}
	if err := staleResp.Validate(conf); err != nil {
		t.Fatalf("canonical stale TryPreAcceptResp rejected: %v", err)
	}
	encoded, err := EncodeMessage(nil, rejectResp)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Message
	if err := DecodeMessage(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Reject || decoded.RejectReason != RejectCommittedConflict || decoded.RejectHint != rejectResp.RejectHint || decoded.ConflictRef != rejectResp.ConflictRef || decoded.ConflictStatus != StatusCommitted {
		t.Fatalf("decoded reject metadata = %#v, want reason=%v hint=%#v conflict=%s status=%s", decoded, RejectCommittedConflict, rejectResp.RejectHint, rejectResp.ConflictRef, StatusCommitted)
	}
}

func TestFastPathEligibleMarksDefaultPreAcceptWitnesses(t *testing.T) {
	t.Run("local default proposal", func(t *testing.T) {
		rn := optimizedNewRawNode(t, 1, 3)
		ref, err := rn.Propose(optimizedTestCommand("local", "local-key"))
		if err != nil {
			t.Fatal(err)
		}
		inst := rn.instances[ref]
		if inst == nil {
			t.Fatalf("missing proposed instance %s", ref)
		}
		if !inst.rec.FastPathEligible {
			t.Fatalf("local default proposal record is not FastPathEligible: %#v", inst.rec)
		}
		rec := optimizedRequireRecord(t, rn.Ready(), ref)
		if !rec.FastPathEligible {
			t.Fatalf("local default proposal ready record is not FastPathEligible: %#v", rec)
		}
	})

	t.Run("follower exact default preaccept", func(t *testing.T) {
		rn := optimizedNewRawNode(t, 2, 3)
		ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		cmd := optimizedTestCommand("follower", "follower-key")
		if err := rn.Step(Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: cmd}); err != nil {
			t.Fatal(err)
		}
		rec := optimizedRequireRecord(t, rn.Ready(), ref)
		if !rec.FastPathEligible {
			t.Fatalf("exact default follower preaccept is not FastPathEligible: %#v", rec)
		}
	})

	t.Run("follower raises default-ballot attributes", func(t *testing.T) {
		rn := optimizedNewRawNode(t, 2, 3)
		conflict := InstanceRecord{Ref: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 2}, Status: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("existing", "shared-key"), FastPathEligible: true}
		optimizedSeedRecord(t, rn, conflict)
		ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		if err := rn.Step(Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("new", "shared-key")}); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		rec := optimizedRequireRecord(t, rd, ref)
		if rec.FastPathEligible || rec.Seq != 2 || rec.Deps[1] != 1 {
			t.Fatalf("updated follower preaccept record = %#v, want raised attrs without durable recovery eligibility", rec)
		}
		resp := optimizedRequireMessage(t, rd.Messages, MsgPreAcceptResp, 1)
		if !resp.FastPathEligible {
			t.Fatalf("updated follower live PreAccept response lost covering fast-path eligibility: %#v", resp)
		}
	})

	t.Run("non-default ballot", func(t *testing.T) {
		rn := optimizedNewRawNode(t, 2, 3)
		ref := InstanceRef{Replica: 1, Instance: 2, Conf: 1}
		if err := rn.Step(Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("recovery-preaccept", "recovery-preaccept-key")}); err != nil {
			t.Fatal(err)
		}
		rec := optimizedRequireRecord(t, rn.Ready(), ref)
		if rec.FastPathEligible {
			t.Fatalf("non-default-ballot preaccept marked FastPathEligible: %#v", rec)
		}
	})

	t.Run("accept and try-preaccept recovery paths", func(t *testing.T) {
		rn := optimizedNewRawNode(t, 2, 3)
		acceptRef := InstanceRef{Replica: 1, Instance: 3, Conf: 1}
		if err := rn.Step(Message{Type: MsgAccept, From: 1, To: 2, Ref: acceptRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("accept", "accept-key")}); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		acceptRec := optimizedRequireRecord(t, rd, acceptRef)
		if acceptRec.FastPathEligible {
			t.Fatalf("accept recovery path marked FastPathEligible: %#v", acceptRec)
		}
		advanceOK(t, rn, rd)

		tryRef := InstanceRef{Replica: 1, Instance: 4, Conf: 1}
		if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: tryRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("try", "try-fast-path-key")}); err != nil {
			t.Fatal(err)
		}
		if tryInst := rn.instances[tryRef]; tryInst != nil && tryInst.rec.FastPathEligible {
			t.Fatalf("try-preaccept recovery path marked FastPathEligible: %#v", tryInst.rec)
		}
		if rn.HasReady() {
			for _, rec := range rn.Ready().Records {
				if rec.Ref == tryRef && rec.FastPathEligible {
					t.Fatalf("try-preaccept ready record marked FastPathEligible: %#v", rec)
				}
			}
		}
	})
}

func TestOptimizedThreeNodeFastPathCommitsAfterOneRemoteMatch(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, TimeOptimization: true, TimeOptimizationTicks: 3})
	if err != nil {
		t.Fatal(err)
	}
	cmd := optimizedTestCommand("fast", "fast-key")
	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	optimizedApplyAndAdvance(t, store, rn, rd)
	inst := rn.instances[ref]
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: append([]InstanceNum(nil), inst.rec.Deps...), FastPathEligible: true}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseCommitted || inst.rec.Status != StatusExecuted {
		t.Fatalf("one matching response phase/status = %d/%s, want committed/executed under optimized 3-node quorum", inst.phase, inst.rec.Status)
	}
	rd = rn.Ready()
	readyRecord := optimizedRequireRecord(t, rd, ref)
	if readyRecord.Status != StatusCommitted {
		t.Fatalf("fast-path ready record = %#v, want committed", readyRecord)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		optimizedRequireCanonicalCommitRequest(t, msg, readyRecord)
	}
	committed := requireCommittedForRef(t, rd, ref)
	if committed.Command.ID != cmd.ID || !bytes.Equal(committed.Command.Payload, cmd.Payload) {
		t.Fatalf("committed command = %#v, want %#v", committed.Command, cmd)
	}
}

func TestOptimizedThreeNodeDivergentResponseFallsBackToAccept(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(optimizedTestCommand("slow", "slow-key"))
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	optimizedApplyAndAdvance(t, store, rn, rd)
	inst := rn.instances[ref]
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq + 1, Deps: append([]InstanceNum(nil), inst.rec.Deps...)}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("divergent response phase/status = %d/%s, want accept/accepted", inst.phase, inst.rec.Status)
	}
	rd = rn.Ready()
	accepted := optimizedRequireRecord(t, rd, ref)
	if accepted.Status != StatusAccepted || accepted.Seq != 2 {
		t.Fatalf("divergent response ready record = %#v, want accepted with merged seq 2", accepted)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgCommit)
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
		optimizedRequireCanonicalInstanceRequest(t, msg, accepted)
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("divergent response committed application commands: %#v", rd.Committed)
	}
}

func TestAcceptRecordsAndRepliesWithLocalConflictDeps(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	key := "accept-local-conflict-key"
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:     conflictRef,
		Ballot:  Ballot{Replica: 2},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("local-conflict", key),
	})

	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	chosenDeps := rn.q.deps()
	cmd := optimizedTestCommand("accept-target", key)
	if err := rn.Step(Message{
		Type:    MsgAccept,
		From:    1,
		To:      2,
		Ref:     ref,
		Ballot:  Ballot{Number: 1, Replica: 1},
		Seq:     1,
		Deps:    chosenDeps,
		Command: cmd,
	}); err != nil {
		t.Fatal(err)
	}

	wantEvidence := Attributes{Seq: 2, Deps: []InstanceNum{0, 1, 0}}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusAccepted || rec.Seq != 1 || !instanceNumsEqual(rec.Deps, chosenDeps) {
		t.Fatalf("accept record chosen attrs = %#v, want accepted seq 1 deps %v", rec, chosenDeps)
	}
	gotEvidence, ok := rec.AcceptAttributes()
	if !ok || gotEvidence.Seq != wantEvidence.Seq || !instanceNumsEqual(gotEvidence.Deps, wantEvidence.Deps) {
		t.Fatalf("accept record evidence = seq %d deps %v ok=%v, want seq %d deps %v", gotEvidence.Seq, gotEvidence.Deps, ok, wantEvidence.Seq, wantEvidence.Deps)
	}

	resp := optimizedRequireMessage(t, rd.Messages, MsgAcceptResp, 1)
	if resp.Reject ||
		resp.Ballot != rec.Ballot ||
		resp.RecordBallot != rec.RecordBallot ||
		resp.RecordStatus != StatusAccepted ||
		resp.Seq != 1 ||
		!instanceNumsEqual(resp.Deps, chosenDeps) ||
		!commandEqual(resp.Command, Command{}) {
		t.Fatalf("accept response chosen attrs = %#v, want ballot %#v, accepted seq 1 deps %v and empty command", resp, rec.Ballot, chosenDeps)
	}
	respEvidence, ok := resp.AcceptAttributes()
	if !ok || respEvidence.Seq != wantEvidence.Seq || !instanceNumsEqual(respEvidence.Deps, wantEvidence.Deps) {
		t.Fatalf("accept response evidence = seq %d deps %v ok=%v, want seq %d deps %v", respEvidence.Seq, respEvidence.Deps, ok, wantEvidence.Seq, wantEvidence.Deps)
	}
	optimizedApplyAndAdvance(t, store, rn, rd)
	stored, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("storage missing accepted record %s", ref)
	}
	storedEvidence, ok := stored.AcceptAttributes()
	if !ok || storedEvidence.Seq != wantEvidence.Seq || !instanceNumsEqual(storedEvidence.Deps, wantEvidence.Deps) {
		t.Fatalf("stored accept evidence = seq %d deps %v ok=%v, want seq %d deps %v", storedEvidence.Seq, storedEvidence.Deps, ok, wantEvidence.Seq, wantEvidence.Deps)
	}
}

func TestAcceptRespQuorumCommitsChosenAttrsAndRecordsDepsEvidence(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := rn.Propose(optimizedTestCommand("accept-resp-target", "accept-resp-key"))
	if err != nil {
		t.Fatal(err)
	}
	optimizedApplyAndAdvance(t, store, rn, rn.Ready())

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing proposed instance %s", ref)
	}
	chosen := inst.rec.Attributes()
	rn.startAccept(inst, chosen)
	acceptReady := rn.Ready()
	accepted := optimizedRequireRecord(t, acceptReady, ref)
	if accepted.Status != StatusAccepted || accepted.Seq != chosen.Seq || !instanceNumsEqual(accepted.Deps, chosen.Deps) {
		t.Fatalf("started accept record = %#v, want chosen attrs seq %d deps %v", accepted, chosen.Seq, chosen.Deps)
	}
	optimizedApplyAndAdvance(t, store, rn, acceptReady)

	evidence := Attributes{Seq: chosen.Seq + 1, Deps: []InstanceNum{0, 1, 0}}
	if err := rn.Step(Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       inst.rec.Ballot,
		RecordBallot: inst.rec.RecordBallot,
		Seq:          chosen.Seq,
		Deps:         append([]InstanceNum(nil), chosen.Deps...),
		AcceptSeq:    evidence.Seq,
		AcceptDeps:   append([]InstanceNum(nil), evidence.Deps...),
		RecordStatus: StatusAccepted,
	}); err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	var committed InstanceRecord
	var sawCommitted, sawEvidence bool
	for _, rec := range rd.Records {
		if rec.Ref != ref {
			continue
		}
		if rec.Status == StatusCommitted {
			committed = rec
			sawCommitted = true
		}
		if recEvidence, ok := rec.AcceptAttributes(); ok && recEvidence.Seq == evidence.Seq && instanceNumsEqual(recEvidence.Deps, evidence.Deps) {
			sawEvidence = true
		}
	}
	if !sawCommitted || committed.Seq != chosen.Seq || !instanceNumsEqual(committed.Deps, chosen.Deps) {
		t.Fatalf("committed record chosen attrs = %#v found=%v in records %#v, want committed seq %d deps %v", committed, sawCommitted, rd.Records, chosen.Seq, chosen.Deps)
	}
	if !sawEvidence {
		t.Fatalf("ready records did not durably carry accept evidence seq %d deps %v: %#v", evidence.Seq, evidence.Deps, rd.Records)
	}
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		optimizedRequireCanonicalCommitRequest(t, msg, committed)
		msgEvidence, ok := msg.AcceptAttributes()
		if !ok || msgEvidence.Seq != evidence.Seq || !instanceNumsEqual(msgEvidence.Deps, evidence.Deps) {
			t.Fatalf("commit message to %d evidence = seq %d deps %v ok=%v, want seq %d deps %v", to, msgEvidence.Seq, msgEvidence.Deps, ok, evidence.Seq, evidence.Deps)
		}
	}
	optimizedApplyAndAdvance(t, store, rn, rd)
	stored, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("storage missing committed record %s", ref)
	}
	storedEvidence, ok := stored.AcceptAttributes()
	if stored.Status != StatusCommitted || stored.Seq != chosen.Seq || !instanceNumsEqual(stored.Deps, chosen.Deps) || !ok || storedEvidence.Seq != evidence.Seq || !instanceNumsEqual(storedEvidence.Deps, evidence.Deps) {
		t.Fatalf("stored committed record = %#v with evidence seq %d deps %v ok=%v, want chosen seq %d deps %v and evidence seq %d deps %v", stored, storedEvidence.Seq, storedEvidence.Deps, ok, chosen.Seq, chosen.Deps, evidence.Seq, evidence.Deps)
	}
}

func TestTryPreAcceptRejectsStaleBallot(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	cmd := optimizedTestCommand("stale", "stale-key")
	higher := Ballot{Number: 2, Replica: 2}
	optimizedSeedRecord(t, rn, InstanceRecord{Ref: ref, Ballot: higher, Status: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: cmd, FastPathEligible: true})
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: cmd}); err != nil {
		t.Fatal(err)
	}
	resp := optimizedRequireMessage(t, rn.Ready().Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryReject(t, resp, ref, higher, higher, RejectStaleBallot, InstanceRef{}, StatusNone, rn.q.deps())
}

func TestTryPreAcceptUsesAcceptDepsEvidenceToAvoidConflictRejection(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	key := "try-accept-deps-evidence"
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	chosenDeps := rn.q.deps()
	acceptDeps := rn.q.deps()
	acceptDeps[0] = 1
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:        conflictRef,
		Ballot:     Ballot{Replica: 2},
		Status:     StatusAccepted,
		Seq:        1,
		Deps:       chosenDeps,
		AcceptSeq:  2,
		AcceptDeps: acceptDeps,
		Command:    optimizedTestCommand("accepted-conflict", key),
	})

	targetRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	targetDeps := rn.q.deps()
	targetDeps[1] = conflictRef.Instance
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: targetDeps, Command: optimizedTestCommand("target", key)}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryPreAcceptResp(t, resp, targetRef, Ballot{Number: 1, Replica: 1}, Attributes{Seq: 1, Deps: targetDeps})
	rec := optimizedRequireRecord(t, rd, targetRef)
	if rec.Status != StatusPreAccepted || rec.Seq != 1 || !instanceNumsEqual(rec.Deps, targetDeps) {
		t.Fatalf("TryPreAccept durable record = %#v, want target preaccepted with deps %v", rec, targetDeps)
	}
	conflict := rn.instances[conflictRef].rec
	if conflict.Seq != 1 || !instanceNumsEqual(conflict.Deps, chosenDeps) {
		t.Fatalf("conflicting record chosen attrs changed to seq %d deps %v, want seq 1 deps %v", conflict.Seq, conflict.Deps, chosenDeps)
	}
	evidence, ok := conflict.AcceptAttributes()
	if !ok || evidence.Seq != 2 || !instanceNumsEqual(evidence.Deps, acceptDeps) {
		t.Fatalf("conflicting record accept evidence = seq %d deps %v ok=%v, want seq 2 deps %v", evidence.Seq, evidence.Deps, ok, acceptDeps)
	}
}

func TestTryPreAcceptRejectsCommittedStaleDependencyWithoutAcceptDepsEvidence(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	key := "committed-stale-dependency-without-evidence"
	targetRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:     conflictRef,
		Ballot:  Ballot{Number: 1, Replica: 2},
		Status:  StatusCommitted,
		Seq:     2,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("committed-without-evidence", key),
	})

	targetDeps := rn.q.deps()
	targetDeps[1] = conflictRef.Instance
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: targetDeps, Command: optimizedTestCommand("target", key)}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryReject(t, resp, targetRef, Ballot{Number: 1, Replica: 1}, Ballot{}, RejectCommittedConflict, conflictRef, StatusCommitted, rn.q.deps())
	if inst := rn.instances[targetRef]; inst != nil && inst.rec.Status >= StatusPreAccepted {
		t.Fatalf("stale-dependency conflict stored target without Accept-Deps evidence: %#v", inst.rec)
	}
	if len(rd.Records) != 0 {
		t.Fatalf("stale-dependency conflict durably recorded target state: %#v", rd.Records)
	}
}

func TestTryPreAcceptRejectsCommittedConflict(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{Ref: conflictRef, Ballot: Ballot{Replica: 2}, Status: StatusCommitted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("committed-conflict", "shared-committed")})
	targetRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("target", "shared-committed")}); err != nil {
		t.Fatal(err)
	}
	resp := optimizedRequireMessage(t, rn.Ready().Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryReject(t, resp, targetRef, Ballot{Number: 1, Replica: 1}, Ballot{}, RejectCommittedConflict, conflictRef, StatusCommitted, rn.q.deps())
	if inst := rn.instances[targetRef]; inst != nil && inst.rec.Status >= StatusPreAccepted {
		t.Fatalf("unsafe committed conflict stored target instance: %#v", inst.rec)
	}
}

func TestTryPreAcceptDefersUncommittedConflict(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{Ref: conflictRef, Ballot: Ballot{Replica: 2}, Status: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("uncommitted-conflict", "shared-uncommitted"), FastPathEligible: true})
	targetRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("target", "shared-uncommitted")}); err != nil {
		t.Fatal(err)
	}
	resp := optimizedRequireMessage(t, rn.Ready().Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryReject(t, resp, targetRef, Ballot{Number: 1, Replica: 1}, Ballot{}, RejectUncommittedConflict, conflictRef, StatusPreAccepted, rn.q.deps())
	if inst := rn.instances[targetRef]; inst != nil && inst.rec.Status >= StatusPreAccepted {
		t.Fatalf("uncommitted conflict stored target instance instead of deferring: %#v", inst.rec)
	}
}

func TestTryPreAcceptStatusNoneQuorumStillStartsNoopAccept(t *testing.T) {
	ref := InstanceRef{Replica: 2, Instance: 9, Conf: 1}
	rn, inst, ballot := optimizedStartRecovery(t, 3, ref)
	if err := rn.Step(Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordStatus: StatusNone, Deps: rn.q.deps()}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted || inst.rec.Command.Kind != CommandNoop {
		t.Fatalf("StatusNone recovery phase/record = %d/%#v, want noop accept", inst.phase, inst.rec)
	}
	rd := rn.Ready()
	optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAccept)
	accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
	optimizedRequireCanonicalInstanceRequest(t, accept, inst.rec)
}

func TestTryPreAcceptRecoveryStartsAcceptAfterWitnessQuorum(t *testing.T) {
	t.Run("below optimized witness quorum takes the slow accept path", func(t *testing.T) {
		ref := InstanceRef{Replica: 2, Instance: 11, Conf: 1}
		rn, inst, ballot := optimizedStartRecovery(t, 7, ref)
		cmd := optimizedTestCommand("recover", "recover-key")
		attrs := Attributes{Seq: 1, Deps: rn.q.deps()}

		prepareResponses := []Message{
			{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordBallot: Ballot{Replica: ref.Replica}, RecordStatus: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd, FastPathEligible: true},
			{Type: MsgPrepareResp, From: 3, To: 1, Ref: ref, Ballot: ballot, RecordStatus: StatusNone, Deps: rn.q.deps()},
			{Type: MsgPrepareResp, From: 4, To: 1, Ref: ref, Ballot: ballot, RecordStatus: StatusNone, Deps: rn.q.deps()},
		}
		for _, resp := range prepareResponses {
			if err := rn.Step(resp); err != nil {
				t.Fatalf("step prepare response from %d: %v", resp.From, err)
			}
		}
		if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
			t.Fatalf("below-witness recovery phase/record = %d/%#v, want direct slow accept", inst.phase, inst.rec)
		}
		rd := rn.Ready()
		optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAccept)
		accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
		optimizedRequireCanonicalInstanceRequest(t, accept, inst.rec)
		if rec := optimizedRequireRecord(t, rd, ref); rec.Status != StatusAccepted || rec.FastPathEligible {
			t.Fatalf("below-witness recovery record = %#v, want non-fast-path accepted record", rec)
		}
	})

	t.Run("five-node optimized witness waits for try-preaccept OK quorum", func(t *testing.T) {
		ref := InstanceRef{Replica: 2, Instance: 12, Conf: 1}
		rn, inst, ballot := optimizedStartRecovery(t, 5, ref)
		cmd := optimizedTestCommand("recover", "recover-key")
		attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
		if witness := rn.q.tryWitnessQuorum(); witness != 1 {
			t.Fatalf("five-node optimized try witness quorum = %d, want 1", witness)
		}

		prepareResponses := []Message{
			{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordBallot: Ballot{Replica: ref.Replica}, RecordStatus: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd, FastPathEligible: true},
			{Type: MsgPrepareResp, From: 3, To: 1, Ref: ref, Ballot: ballot, RecordStatus: StatusNone, Deps: rn.q.deps()},
		}
		for _, resp := range prepareResponses {
			if err := rn.Step(resp); err != nil {
				t.Fatalf("step prepare response from %d: %v", resp.From, err)
			}
		}
		if inst.phase != phaseTryPreAccept {
			t.Fatalf("prepare quorum phase = %d, want try-preaccept after one optimized five-node conceptual witness", inst.phase)
		}
		rd := rn.Ready()
		try := optimizedRequireMessage(t, rd.Messages, MsgTryPreAccept, 4)
		optimizedRequireCanonicalInstanceRequest(t, try, inst.rec)
		for _, msg := range rd.Messages {
			if msg.Type == MsgTryPreAccept && msg.FastPathEligible {
				t.Fatalf("TryPreAccept message carried FastPathEligible marker: %#v", msg)
			}
		}
		advanceOK(t, rn, rd)

		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 3, To: 1, Ref: ref, Ballot: ballot, Seq: attrs.Seq, Deps: attrs.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseTryPreAccept || inst.tryOK.len() != 2 {
			t.Fatalf("after first actual witness phase/tryOK = %d/%#v, want try-preaccept with two total witnesses", inst.phase, inst.tryOK)
		}
		if rn.HasReady() {
			t.Fatalf("first actual TryPreAccept OK reached accept quorum early: %#v", rn.Ready())
		}

		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 4, To: 1, Ref: ref, Ballot: ballot, Seq: attrs.Seq, Deps: attrs.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
			t.Fatalf("after enough conceptual+actual witnesses phase/record = %d/%#v, want accept/accepted", inst.phase, inst.rec)
		}
		rd = rn.Ready()
		optimizedRequireNoMessageType(t, rd.Messages, MsgCommit)
		accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
		optimizedRequireCanonicalInstanceRequest(t, accept, inst.rec)
		if rec := optimizedRequireRecord(t, rd, ref); rec.Status != StatusAccepted || rec.FastPathEligible {
			t.Fatalf("five-node witness quorum recovery record = %#v, want non-fast-path accepted record", rec)
		}
	})
}

func TestPreparePromiseDoesNotOverwritePersistedRecordBallotAcrossRestart(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	ref := InstanceRef{Replica: 1, Instance: 30, Conf: 1}
	acceptedBallot := Ballot{Number: 2, Replica: 2}
	firstPromise := Ballot{Number: 7, Replica: 1}
	secondPromise := Ballot{Number: 8, Replica: 3}
	cmd := optimizedTestCommand("prepare-record-ballot", "prepare-record-ballot-key")
	deps := []InstanceNum{0, 4, 0}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:          ref,
		Ballot:       acceptedBallot,
		RecordBallot: acceptedBallot,
		Status:       StatusAccepted,
		Seq:          4,
		Deps:         deps,
		Command:      cmd,
	})

	if err := rn.Step(Message{Type: MsgPrepare, From: 1, To: 2, Ref: ref, Ballot: firstPromise}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	promised := optimizedRequireRecord(t, rd, ref)
	if promised.Ballot != firstPromise || promised.RecordBallot != acceptedBallot || promised.Status != StatusAccepted || promised.Seq != 4 || !instanceNumsEqual(promised.Deps, deps) {
		t.Fatalf("durable promise record = %#v, want promise ballot %#v and record ballot %#v with accepted attrs seq 4 deps %v", promised, firstPromise, acceptedBallot, deps)
	}
	resp := optimizedRequireMessage(t, rd.Messages, MsgPrepareResp, 1)
	if resp.Ballot != firstPromise {
		t.Fatalf("Prepare response promise ballot = %#v, want %#v", resp.Ballot, firstPromise)
	}
	if got := resp.RecordBallot; got != acceptedBallot {
		t.Fatalf("Prepare response record ballot = %#v, want previous accepted record ballot %#v; response=%#v", got, acceptedBallot, resp)
	}
	if resp.RecordStatus != StatusAccepted || resp.Seq != 4 || !instanceNumsEqual(resp.Deps, deps) || !commandEqual(resp.Command, cmd) {
		t.Fatalf("Prepare response accepted tuple = %#v, want status accepted seq 4 deps %v command %#v", resp, deps, cmd)
	}
	optimizedApplyAndAdvance(t, store, rn, rd)

	restarted, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), Storage: store, RetryTicks: 10, RecoveryTicks: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Step(Message{Type: MsgPrepare, From: 3, To: 2, Ref: ref, Ballot: secondPromise}); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	rePromised := optimizedRequireRecord(t, rd, ref)
	if rePromised.Ballot != secondPromise || rePromised.RecordBallot != acceptedBallot || rePromised.Status != StatusAccepted || rePromised.Seq != 4 || !instanceNumsEqual(rePromised.Deps, deps) {
		t.Fatalf("restarted durable promise record = %#v, want promise ballot %#v and original record ballot %#v with accepted attrs seq 4 deps %v", rePromised, secondPromise, acceptedBallot, deps)
	}
	resp = optimizedRequireMessage(t, rd.Messages, MsgPrepareResp, 3)
	if resp.Ballot != secondPromise {
		t.Fatalf("restarted Prepare response promise ballot = %#v, want %#v", resp.Ballot, secondPromise)
	}
	if got := resp.RecordBallot; got != acceptedBallot {
		t.Fatalf("restarted Prepare response record ballot = %#v, want original accepted record ballot %#v after persisted promise; response=%#v", got, acceptedBallot, resp)
	}
	if resp.RecordStatus != StatusAccepted || resp.Seq != 4 || !instanceNumsEqual(resp.Deps, deps) || !commandEqual(resp.Command, cmd) {
		t.Fatalf("restarted Prepare response accepted tuple = %#v, want status accepted seq 4 deps %v command %#v", resp, deps, cmd)
	}
}

func TestPrepareRecoveryChoosesHighestPersistedRecordBallot(t *testing.T) {
	ref := InstanceRef{Replica: 2, Instance: 31, Conf: 1}
	rn, inst, ballot := optimizedStartRecovery(t, 5, ref)
	lowCmd := optimizedTestCommand("low-record-ballot", "record-ballot-filter-key")
	highCmd := optimizedTestCommand("high-record-ballot", "record-ballot-filter-key")
	lowDeps := []InstanceNum{0, 7, 0, 0, 0}
	highDeps := []InstanceNum{0, 0, 3, 0, 0}
	lowRecordBallot := Ballot{Replica: ref.Replica}
	highRecordBallot := ballot
	low := Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordBallot: lowRecordBallot, RecordStatus: StatusAccepted, Seq: 2, Deps: lowDeps, Command: lowCmd}
	high := Message{Type: MsgPrepareResp, From: 3, To: 1, Ref: ref, Ballot: ballot, RecordBallot: highRecordBallot, RecordStatus: StatusAccepted, Seq: 5, Deps: highDeps, Command: highCmd}

	if err := rn.Step(low); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phasePrepare {
		t.Fatalf("single accepted Prepare response formed quorum: phase=%d", inst.phase)
	}
	if err := rn.Step(high); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("Prepare recovery phase/status = %d/%s, want accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	if inst.rec.Seq != high.Seq || !instanceNumsEqual(inst.rec.Deps, highDeps) || !commandEqual(inst.rec.Command, highCmd) {
		t.Fatalf("recovered accepted tuple = seq %d deps %v command %#v, want highest record-ballot tuple %v seq %d deps %v command %#v over lower tuple %v", inst.rec.Seq, inst.rec.Deps, inst.rec.Command, highRecordBallot, high.Seq, highDeps, highCmd, lowRecordBallot)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusAccepted || rec.Seq != high.Seq || !instanceNumsEqual(rec.Deps, highDeps) {
		t.Fatalf("durable recovered record = %#v, want accepted seq %d deps %v from highest persisted record ballot %v", rec, high.Seq, highDeps, highRecordBallot)
	}
	accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
	optimizedRequireCanonicalInstanceRequest(t, accept, rec)
}

func TestTryPreAcceptRecoveryCountsProvenInitialLeaderWhenOwnerIsStopped(t *testing.T) {
	ref := InstanceRef{Replica: 2, Instance: 32, Conf: 1}
	rn, inst, ballot := optimizedStartRecovery(t, 3, ref)
	cmd := optimizedTestCommand("implicit-leader-witness", "implicit-leader-witness-key")
	attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	resp := Message{Type: MsgPrepareResp, From: 3, To: 1, Ref: ref, Ballot: ballot, RecordBallot: Ballot{Replica: ref.Replica}, RecordStatus: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd, FastPathEligible: true}
	if err := rn.Step(resp); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("stopped-owner recovery phase/status = %d/%s, want accept/%s from one proven witness plus implicit initial leader; tryOK=%#v", inst.phase, inst.rec.Status, StatusAccepted, inst.tryOK)
	}
	rd := rn.Ready()
	optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAccept)
	accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	optimizedRequireCanonicalInstanceRequest(t, accept, inst.rec)
}

func optimizedInstallTryRecoveryCandidate(t *testing.T, rn *RawNode, ref InstanceRef, cmd Command, attrs Attributes, impossibleFastQuorumMembers ...ReplicaID) *instance {
	t.Helper()
	prepareOK := make(map[ReplicaID]InstanceRecord, len(impossibleFastQuorumMembers))
	for _, id := range impossibleFastQuorumMembers {
		prepareOK[id] = InstanceRecord{Ref: ref, Status: StatusNone, Deps: rn.q.deps()}
	}
	rec := InstanceRecord{Ref: ref, Ballot: Ballot{Number: 1, Replica: rn.id}, RecordBallot: Ballot{Number: 1, Replica: rn.id}, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd}
	rec.Checksum = ChecksumRecord(rec)
	inst := &instance{rec: rec, phase: phaseTryPreAccept, prepareOK: testRecordVoteSet(t, rn.q.conf, prepareOK)}
	rn.instances[ref] = inst
	return inst
}

func TestTryPreAcceptCommittedStaleDependencyEvidenceResendsIgnoreMarker(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 5)
	key := "committed-stale-evidence-resend-ignore"
	gammaRef := InstanceRef{Replica: 1, Instance: 50, Conf: 1}
	deltaRef := InstanceRef{Replica: 2, Instance: 7, Conf: 1}
	gammaDeps := rn.q.deps()
	gammaDeps[1] = deltaRef.Instance
	gammaAttrs := Attributes{Seq: 2, Deps: gammaDeps}
	deltaSeq := uint64(3)
	deltaBallot := Ballot{Number: 1, Replica: deltaRef.Replica}
	deltaChosenDeps := rn.q.deps()
	deltaCmd := optimizedTestCommand("delta", key)
	evidenceDeps := rn.q.deps()
	evidenceDeps[0] = gammaRef.Instance
	inst := optimizedInstallTryRecoveryCandidate(t, rn, gammaRef, optimizedTestCommand("gamma", key), gammaAttrs, 4, 5)

	if err := rn.Step(Message{
		Type:           MsgTryPreAcceptResp,
		From:           4,
		To:             1,
		Ref:            gammaRef,
		Ballot:         inst.rec.Ballot,
		Reject:         true,
		RejectReason:   RejectCommittedConflict,
		ConflictRef:    deltaRef,
		ConflictStatus: StatusCommitted,
		Deps:           rn.q.deps(),
	}); err != nil {
		t.Fatal(err)
	}

	if inst.phase != phaseTryPreAccept || inst.rec.Status != StatusPreAccepted {
		t.Fatalf("committed-conflict rejection phase/status = %d/%s, want try-preaccept/%s while checking conflict evidence", inst.phase, inst.rec.Status, StatusPreAccepted)
	}
	rd := rn.Ready()
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	optimizedRequireNoMessageType(t, rd.Messages, MsgPrepare)
	for _, to := range []ReplicaID{2, 3, 4, 5} {
		evidenceReq := optimizedRequireMessage(t, rd.Messages, MsgEvidence, to)
		if evidenceReq.Ref != deltaRef || evidenceReq.ConflictRef != gammaRef {
			t.Fatalf("evidence query to %d = %#v, want read-only query for committed conflict %s tied to recovery target %s", to, evidenceReq, deltaRef, gammaRef)
		}
	}
	if len(rd.Records) != 0 {
		t.Fatalf("read-only conflict evidence query durably recorded side effects: %#v", rd.Records)
	}
	advanceOK(t, rn, rd)

	if err := rn.Step(Message{
		Type:           MsgEvidenceResp,
		From:           2,
		To:             1,
		Ref:            deltaRef,
		ConflictRef:    gammaRef,
		Ballot:         inst.rec.Ballot,
		RecordBallot:   deltaBallot,
		RecordStatus:   StatusCommitted,
		Seq:            deltaSeq,
		Deps:           append([]InstanceNum(nil), deltaChosenDeps...),
		Command:        deltaCmd,
		AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: deltaSeq, Deps: append([]InstanceNum(nil), evidenceDeps...)}},
	}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseTryPreAccept || rn.HasReady() {
		t.Fatalf("single conflict evidence response completed recovery early: phase=%d ready=%#v", inst.phase, rn.Ready())
	}

	if err := rn.Step(Message{
		Type:           MsgEvidenceResp,
		From:           3,
		To:             1,
		Ref:            deltaRef,
		ConflictRef:    gammaRef,
		Ballot:         inst.rec.Ballot,
		RecordBallot:   deltaBallot,
		RecordStatus:   StatusCommitted,
		Seq:            deltaSeq,
		Deps:           append([]InstanceNum(nil), deltaChosenDeps...),
		Command:        deltaCmd,
		AcceptEvidence: []AcceptEvidence{{Sender: 3, Seq: deltaSeq, Deps: append([]InstanceNum(nil), evidenceDeps...)}},
	}); err != nil {
		t.Fatal(err)
	}

	if inst.phase != phaseTryPreAccept || inst.rec.Status != StatusPreAccepted {
		t.Fatalf("covered committed-conflict evidence phase/status = %d/%s, want try-preaccept/%s", inst.phase, inst.rec.Status, StatusPreAccepted)
	}
	rd = rn.Ready()
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	var resend Message
	for _, msg := range rd.Messages {
		if msg.Type == MsgTryPreAccept && msg.IgnoreDependency.Ref == deltaRef {
			resend = msg
			break
		}
	}
	if resend.Type != MsgTryPreAccept {
		t.Fatalf("covered committed-conflict evidence messages = %#v, want resent TryPreAccept with IgnoreDependency.Ref %s", rd.Messages, deltaRef)
	}
	optimizedRequireCanonicalInstanceRequest(t, resend, inst.rec)
	if resend.IgnoreDependency.Ref != deltaRef {
		t.Fatalf("ignore-marker resend = %#v, want ignored dependency %s", resend, deltaRef)
	}
}

func TestTryPreAcceptIgnoreMarkerAllowsCommittedStaleDependency(t *testing.T) {
	rn := optimizedNewRawNode(t, 4, 5)
	key := "committed-stale-ignore-marker"
	gammaRef := InstanceRef{Replica: 1, Instance: 51, Conf: 1}
	deltaRef := InstanceRef{Replica: 2, Instance: 8, Conf: 1}
	gammaDeps := rn.q.deps()
	gammaDeps[1] = deltaRef.Instance
	deltaSeq := uint64(3)
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:     deltaRef,
		Ballot:  Ballot{Number: 1, Replica: deltaRef.Replica},
		Status:  StatusCommitted,
		Seq:     deltaSeq,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("delta", key),
	})
	withoutMarker := Message{
		Type:    MsgTryPreAccept,
		From:    1,
		To:      4,
		Ref:     gammaRef,
		Ballot:  Ballot{Number: 1, Replica: 1},
		Seq:     2,
		Deps:    append([]InstanceNum(nil), gammaDeps...),
		Command: optimizedTestCommand("gamma", key),
	}

	if err := rn.Step(withoutMarker); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	reject := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryReject(t, reject, gammaRef, withoutMarker.Ballot, Ballot{}, RejectCommittedConflict, deltaRef, StatusCommitted, rn.q.deps())
	if len(rd.Records) != 0 {
		t.Fatalf("unmarked committed conflict persisted candidate records: %#v", rd.Records)
	}
	advanceOK(t, rn, rd)

	withMarker := withoutMarker.Clone()
	withMarker.IgnoreDependency.Ref = deltaRef
	if err := rn.Step(withMarker); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryPreAcceptResp(t, resp, gammaRef, withMarker.Ballot, Attributes{Seq: withMarker.Seq, Deps: gammaDeps})
	rec := optimizedRequireRecord(t, rd, gammaRef)
	if rec.Status != StatusPreAccepted || rec.Seq != withMarker.Seq || !instanceNumsEqual(rec.Deps, gammaDeps) {
		t.Fatalf("ignore-marker durable record = %#v, want candidate %s preaccepted at seq %d deps %v", rec, gammaRef, withMarker.Seq, gammaDeps)
	}
	if delta := rn.instances[deltaRef]; delta == nil || delta.rec.Status != StatusCommitted || delta.rec.Seq != deltaSeq {
		t.Fatalf("ignore-marker TryPreAccept changed committed conflict instance: %#v", delta)
	}
}

func TestTryPreAcceptCommittedStaleDependencyEvidenceOmittingCandidateFallsBackToAccept(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 5)
	key := "committed-stale-evidence-fail-closed"
	gammaRef := InstanceRef{Replica: 1, Instance: 52, Conf: 1}
	deltaRef := InstanceRef{Replica: 2, Instance: 9, Conf: 1}
	gammaDeps := rn.q.deps()
	gammaDeps[1] = deltaRef.Instance
	gammaAttrs := Attributes{Seq: 2, Deps: gammaDeps}
	deltaSeq := uint64(3)
	deltaBallot := Ballot{Number: 1, Replica: deltaRef.Replica}
	deltaChosenDeps := rn.q.deps()
	deltaCmd := optimizedTestCommand("delta", key)
	missingCandidateDeps := rn.q.deps()
	coveringDeps := rn.q.deps()
	coveringDeps[0] = gammaRef.Instance
	inst := optimizedInstallTryRecoveryCandidate(t, rn, gammaRef, optimizedTestCommand("gamma", key), gammaAttrs, 4, 5)

	if err := rn.Step(Message{
		Type:           MsgTryPreAcceptResp,
		From:           4,
		To:             1,
		Ref:            gammaRef,
		Ballot:         inst.rec.Ballot,
		Reject:         true,
		RejectReason:   RejectCommittedConflict,
		ConflictRef:    deltaRef,
		ConflictStatus: StatusCommitted,
		Deps:           rn.q.deps(),
	}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	optimizedRequireNoMessageType(t, rd.Messages, MsgPrepare)
	for _, to := range []ReplicaID{2, 3, 4, 5} {
		evidenceReq := optimizedRequireMessage(t, rd.Messages, MsgEvidence, to)
		if evidenceReq.Ref != deltaRef || evidenceReq.ConflictRef != gammaRef {
			t.Fatalf("evidence query to %d = %#v, want read-only query for committed conflict %s tied to recovery target %s", to, evidenceReq, deltaRef, gammaRef)
		}
	}
	if len(rd.Records) != 0 {
		t.Fatalf("read-only conflict evidence query durably recorded side effects: %#v", rd.Records)
	}
	advanceOK(t, rn, rd)

	if err := rn.Step(Message{
		Type:           MsgEvidenceResp,
		From:           3,
		To:             1,
		Ref:            deltaRef,
		ConflictRef:    gammaRef,
		Ballot:         inst.rec.Ballot,
		RecordBallot:   deltaBallot,
		RecordStatus:   StatusCommitted,
		Seq:            deltaSeq,
		Deps:           append([]InstanceNum(nil), deltaChosenDeps...),
		Command:        deltaCmd,
		AcceptEvidence: []AcceptEvidence{{Sender: 3, Seq: deltaSeq, Deps: append([]InstanceNum(nil), coveringDeps...)}},
	}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseTryPreAccept || rn.HasReady() {
		t.Fatalf("single covering conflict evidence response completed recovery early: phase=%d ready=%#v", inst.phase, rn.Ready())
	}

	if err := rn.Step(Message{
		Type:           MsgEvidenceResp,
		From:           2,
		To:             1,
		Ref:            deltaRef,
		ConflictRef:    gammaRef,
		Ballot:         inst.rec.Ballot,
		RecordBallot:   deltaBallot,
		RecordStatus:   StatusCommitted,
		Seq:            deltaSeq,
		Deps:           append([]InstanceNum(nil), deltaChosenDeps...),
		Command:        deltaCmd,
		AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: deltaSeq, Deps: append([]InstanceNum(nil), missingCandidateDeps...)}},
	}); err != nil {
		t.Fatal(err)
	}

	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("disqualifying committed-conflict evidence phase/status = %d/%s, want accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	rd = rn.Ready()
	accepted := optimizedRequireRecord(t, rd, gammaRef)
	if accepted.Status != StatusAccepted || accepted.FastPathEligible || !instanceNumsEqual(accepted.Deps, gammaDeps) {
		t.Fatalf("fail-closed accepted record = %#v, want non-fast slow accept with deps %v", accepted, gammaDeps)
	}
	accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
	optimizedRequireCanonicalInstanceRequest(t, accept, accepted)
	for _, msg := range rd.Messages {
		if msg.Type == MsgTryPreAccept && msg.IgnoreDependency.Ref == deltaRef {
			t.Fatalf("disqualifying evidence emitted unsafe ignore-marker TryPreAccept: %#v in %#v", msg, rd.Messages)
		}
	}
}

func TestTryPreAcceptUncommittedConflictLeaderInFastQuorumStartsAccept(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 5)
	currentRef := InstanceRef{Replica: 1, Instance: 41, Conf: 1}
	conflictRef := InstanceRef{Replica: 3, Instance: 7, Conf: 1}
	attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	inst := optimizedInstallTryRecoveryCandidate(t, rn, currentRef, optimizedTestCommand("leader-in-fast-quorum", "leader-in-fast-quorum-key"), attrs, 4, 5)

	if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: currentRef, Ballot: inst.rec.Ballot, Reject: true, RejectReason: RejectUncommittedConflict, ConflictRef: conflictRef, ConflictStatus: StatusPreAccepted, Deps: rn.q.deps()}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("leader-in-fast-quorum rejection phase/status = %d/%s, want accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, currentRef)
	if rec.Status != StatusAccepted || rec.FastPathEligible || !instanceNumsEqual(rec.Deps, attrs.Deps) {
		t.Fatalf("leader-in-fast-quorum accepted record = %#v, want non-fast accepted attrs %v", rec, attrs)
	}
	accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	optimizedRequireCanonicalInstanceRequest(t, accept, rec)
	optimizedRequireNoMessageType(t, rd.Messages, MsgPrepare)
	if _, ok := rn.instances[conflictRef]; ok {
		t.Fatalf("leader-in-fast-quorum rejection started dependency recovery for %s instead of accepting current candidate", conflictRef)
	}
}

func TestTryPreAcceptUncommittedConflictDefersAndRecoversConflict(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 5)
	currentRef := InstanceRef{Replica: 1, Instance: 42, Conf: 1}
	conflictRef := InstanceRef{Replica: 3, Instance: 8, Conf: 1}
	attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	inst := optimizedInstallTryRecoveryCandidate(t, rn, currentRef, optimizedTestCommand("ordinary-deferral", "ordinary-deferral-key"), attrs, 5)

	if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: currentRef, Ballot: inst.rec.Ballot, Reject: true, RejectReason: RejectUncommittedConflict, ConflictRef: conflictRef, ConflictStatus: StatusPreAccepted, Deps: rn.q.deps()}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseTryPreAccept || inst.rec.Status != StatusPreAccepted {
		t.Fatalf("ordinary uncommitted-conflict rejection phase/status = %d/%s, want try-preaccept/%s", inst.phase, inst.rec.Status, StatusPreAccepted)
	}
	if _, ok := inst.tryDeferred[conflictRef]; !ok {
		t.Fatalf("ordinary uncommitted-conflict rejection deferred set = %#v, want %s recorded", inst.tryDeferred, conflictRef)
	}
	conflict := rn.instances[conflictRef]
	if conflict == nil || conflict.phase != phasePrepare || conflict.rec.Status != StatusNone {
		t.Fatalf("ordinary uncommitted-conflict recovery instance = %#v, want prepare recovery for %s", conflict, conflictRef)
	}
	rd := rn.Ready()
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	prepare := optimizedRequireMessage(t, rd.Messages, MsgPrepare, 2)
	if prepare.Ref != conflictRef || prepare.Ballot.Replica != rn.id {
		t.Fatalf("ordinary uncommitted-conflict prepare = %#v, want recovery prepare for %s from coordinator %d", prepare, conflictRef, rn.id)
	}
	rec := optimizedRequireRecord(t, rd, conflictRef)
	if rec.Status != StatusNone || rec.Ballot.Replica != rn.id {
		t.Fatalf("ordinary uncommitted-conflict durable recovery record = %#v, want StatusNone promise for %s", rec, conflictRef)
	}
}

func TestTryPreAcceptDuplicateUncommittedConflictRejectionDoesNotRestartBlockerRecovery(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 5)
	currentRef := InstanceRef{Replica: 1, Instance: 44, Conf: 1}
	conflictRef := InstanceRef{Replica: 3, Instance: 10, Conf: 1}
	attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	inst := optimizedInstallTryRecoveryCandidate(t, rn, currentRef, optimizedTestCommand("duplicate-deferral", "duplicate-deferral-key"), attrs, 5)
	reject := Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: currentRef, Ballot: inst.rec.Ballot, Reject: true, RejectReason: RejectUncommittedConflict, ConflictRef: conflictRef, ConflictStatus: StatusPreAccepted, Deps: rn.q.deps()}

	if err := rn.Step(reject); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseTryPreAccept || inst.rec.Status != StatusPreAccepted {
		t.Fatalf("first uncommitted-conflict rejection phase/status = %d/%s, want try-preaccept/%s", inst.phase, inst.rec.Status, StatusPreAccepted)
	}
	if _, ok := inst.tryDeferred[conflictRef]; !ok || len(inst.tryDeferred) != 1 {
		t.Fatalf("first uncommitted-conflict rejection deferred set = %#v, want only %s recorded", inst.tryDeferred, conflictRef)
	}
	conflict := rn.instances[conflictRef]
	if conflict == nil || conflict.phase != phasePrepare || conflict.rec.Status != StatusNone {
		t.Fatalf("first uncommitted-conflict recovery instance = %#v, want prepare recovery for %s", conflict, conflictRef)
	}
	firstGeneration := conflict.generation
	rd := rn.Ready()
	optimizedRequireMessage(t, rd.Messages, MsgPrepare, 2)
	advanceOK(t, rn, rd)

	if err := rn.Step(reject); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseTryPreAccept || inst.rec.Status != StatusPreAccepted {
		t.Fatalf("duplicate uncommitted-conflict rejection changed current recovery to phase/status = %d/%s", inst.phase, inst.rec.Status)
	}
	if _, ok := inst.tryDeferred[conflictRef]; !ok || len(inst.tryDeferred) != 1 {
		t.Fatalf("duplicate uncommitted-conflict rejection deferred set = %#v, want only %s recorded", inst.tryDeferred, conflictRef)
	}
	if rn.instances[conflictRef] != conflict || conflict.phase != phasePrepare || conflict.generation != firstGeneration {
		t.Fatalf("duplicate uncommitted-conflict rejection restarted blocker recovery: instance=%#v generation=%d want original generation %d", rn.instances[conflictRef], conflict.generation, firstGeneration)
	}
	if rn.HasReady() {
		t.Fatalf("duplicate uncommitted-conflict rejection emitted duplicate recovery effects: %#v", rn.Ready())
	}
}

func TestTryPreAcceptDeferredCycleLeaderInFastQuorumStartsAccept(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 5)
	currentRef := InstanceRef{Replica: 1, Instance: 43, Conf: 1}
	deferredRef := InstanceRef{Replica: 3, Instance: 9, Conf: 1}
	currentDeps := rn.q.deps()
	deferredIdx, ok := rn.q.conf.Index(deferredRef.Replica)
	if !ok {
		t.Fatalf("deferred replica %d is not in test configuration", deferredRef.Replica)
	}
	currentDeps[deferredIdx] = deferredRef.Instance
	attrs := Attributes{Seq: 3, Deps: currentDeps}
	inst := optimizedInstallTryRecoveryCandidate(t, rn, currentRef, optimizedTestCommand("deferred-cycle-current", "deferred-cycle-key"), attrs, 4, 5)
	deferredDeps := rn.q.deps()
	currentIdx, ok := rn.q.conf.Index(currentRef.Replica)
	if !ok {
		t.Fatalf("current replica %d is not in test configuration", currentRef.Replica)
	}
	deferredDeps[currentIdx] = currentRef.Instance
	deferredRec := InstanceRecord{Ref: deferredRef, Ballot: Ballot{Number: 1, Replica: rn.id}, RecordBallot: Ballot{Number: 1, Replica: rn.id}, Status: StatusPreAccepted, Seq: 2, Deps: deferredDeps, Command: optimizedTestCommand("deferred-cycle-earlier", "deferred-cycle-key")}
	deferredRec.Checksum = ChecksumRecord(deferredRec)
	rn.instances[deferredRef] = &instance{rec: deferredRec, phase: phaseTryPreAccept, tryDeferred: map[InstanceRef]struct{}{currentRef: {}}}

	if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: currentRef, Ballot: inst.rec.Ballot, Reject: true, RejectReason: RejectUncommittedConflict, ConflictRef: deferredRef, ConflictStatus: StatusPreAccepted, Deps: rn.q.deps()}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("deferred-cycle rejection phase/status = %d/%s, want accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, currentRef)
	if rec.Status != StatusAccepted || rec.FastPathEligible || !instanceNumsEqual(rec.Deps, currentDeps) {
		t.Fatalf("deferred-cycle accepted record = %#v, want current candidate accepted with deps %v", rec, currentDeps)
	}
	accept := optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	optimizedRequireCanonicalInstanceRequest(t, accept, rec)
	optimizedRequireNoMessageType(t, rd.Messages, MsgPrepare)
}

func TestCommitPreservesAcceptDepsEvidenceForLaterTryPreAccept(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	key := "commit-preserves-accept-deps"
	targetRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	chosenDeps := rn.q.deps()
	acceptDeps := rn.q.deps()
	acceptDeps[0] = targetRef.Instance
	acceptEvidence := []AcceptEvidence{{Sender: 3, Seq: 2, Deps: acceptDeps}}
	conflictCmd := optimizedTestCommand("accepted-before-commit", key)
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:            conflictRef,
		Ballot:         Ballot{Number: 1, Replica: 2},
		Status:         StatusAccepted,
		Seq:            1,
		Deps:           chosenDeps,
		AcceptSeq:      2,
		AcceptDeps:     acceptDeps,
		AcceptEvidence: acceptEvidence,
		Command:        conflictCmd,
	})

	if err := rn.Step(Message{Type: MsgCommit, From: 2, To: 2, Ref: conflictRef, Ballot: Ballot{Number: 1, Replica: 2}, RecordBallot: Ballot{Number: 1, Replica: 2}, Seq: 1, Deps: chosenDeps, Command: conflictCmd}); err != nil {
		t.Fatal(err)
	}
	commitReady := rn.Ready()
	committed := optimizedRequireRecord(t, commitReady, conflictRef)
	evidence, ok := committed.AcceptAttributes()
	if committed.Status != StatusCommitted || !ok || evidence.Seq != 2 || !instanceNumsEqual(evidence.Deps, acceptDeps) {
		t.Fatalf("committed record after MsgCommit = %#v with evidence seq %d deps %v ok=%v, want committed with preserved evidence seq 2 deps %v", committed, evidence.Seq, evidence.Deps, ok, acceptDeps)
	}
	advanceOK(t, rn, commitReady)

	targetDeps := rn.q.deps()
	targetDeps[1] = conflictRef.Instance
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: targetDeps, Command: optimizedTestCommand("target-after-commit", key)}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	tryResp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryPreAcceptResp(t, tryResp, targetRef, Ballot{Number: 1, Replica: 1}, Attributes{Seq: 1, Deps: targetDeps})
	if rec := optimizedRequireRecord(t, rd, targetRef); rec.Status != StatusPreAccepted {
		t.Fatalf("TryPreAccept record after covered committed conflict = %#v, want preaccepted target", rec)
	}
}

func TestCommitMessageAcceptDepsEvidenceAllowsLaterTryPreAccept(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	key := "commit-message-accept-deps"
	targetRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	chosenDeps := rn.q.deps()
	acceptDeps := rn.q.deps()
	acceptDeps[0] = targetRef.Instance
	conflictCmd := optimizedTestCommand("committed-with-message-evidence", key)

	if err := rn.Step(Message{
		Type:           MsgCommit,
		From:           2,
		To:             2,
		Ref:            conflictRef,
		Ballot:         Ballot{Number: 1, Replica: 2},
		RecordBallot:   Ballot{Number: 1, Replica: 2},
		Seq:            1,
		Deps:           chosenDeps,
		AcceptSeq:      2,
		AcceptDeps:     append([]InstanceNum(nil), acceptDeps...),
		AcceptEvidence: []AcceptEvidence{{Sender: 3, Seq: 2, Deps: append([]InstanceNum(nil), acceptDeps...)}},
		Command:        conflictCmd,
	}); err != nil {
		t.Fatal(err)
	}
	commitReady := rn.Ready()
	committed := optimizedRequireRecord(t, commitReady, conflictRef)
	evidence, ok := committed.AcceptAttributes()
	if committed.Status != StatusCommitted || !ok || evidence.Seq != 2 || !instanceNumsEqual(evidence.Deps, acceptDeps) {
		t.Fatalf("committed record from MsgCommit = %#v with evidence seq %d deps %v ok=%v, want committed with message Accept-Deps seq 2 deps %v", committed, evidence.Seq, evidence.Deps, ok, acceptDeps)
	}
	advanceOK(t, rn, commitReady)

	targetDeps := rn.q.deps()
	targetDeps[1] = conflictRef.Instance
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: targetDeps, Command: optimizedTestCommand("target-after-commit-message", key)}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	optimizedRequireCanonicalTryPreAcceptResp(t, resp, targetRef, Ballot{Number: 1, Replica: 1}, Attributes{Seq: 1, Deps: targetDeps})
	rec := optimizedRequireRecord(t, rd, targetRef)
	if rec.Status != StatusPreAccepted || rec.Seq != 1 || !instanceNumsEqual(rec.Deps, targetDeps) {
		t.Fatalf("TryPreAccept record after message-evidence committed conflict = %#v, want preaccepted seq 1 deps %v", rec, targetDeps)
	}
	conflict := rn.instances[conflictRef].rec
	conflictEvidence, ok := conflict.AcceptAttributes()
	if !ok || conflictEvidence.Seq != 2 || !instanceNumsEqual(conflictEvidence.Deps, acceptDeps) {
		t.Fatalf("committed conflict evidence after TryPreAccept = seq %d deps %v ok=%v, want message-carried seq 2 deps %v", conflictEvidence.Seq, conflictEvidence.Deps, ok, acceptDeps)
	}
}

func TestTryPreAcceptAllowsConflictWhoseChosenAttrsDependOnCandidate(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	key := "chosen-attrs-depend-on-candidate"
	targetRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	deps := rn.q.deps()
	deps[0] = targetRef.Instance
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:     conflictRef,
		Ballot:  Ballot{Number: 1, Replica: 2},
		Status:  StatusCommitted,
		Seq:     2,
		Deps:    deps,
		Command: optimizedTestCommand("chosen-covered-conflict", key),
	})

	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("target", key)}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	rec := optimizedRequireRecord(t, rd, targetRef)
	optimizedRequireCanonicalTryPreAcceptResp(t, resp, targetRef, Ballot{Number: 1, Replica: 1}, rec.Attributes())
	if rec.Status != StatusPreAccepted {
		t.Fatalf("TryPreAccept record after chosen-covered conflict = %#v, want preaccepted target", rec)
	}
}

func TestTryPreAcceptAllowsSameLeaderDependentPreacceptedConflict(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
	key := "same-leader-dependent-preaccepted"
	earlierRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	targetRef := InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:              earlierRef,
		Ballot:           Ballot{Replica: 1},
		Status:           StatusPreAccepted,
		Seq:              1,
		Deps:             rn.q.deps(),
		Command:          optimizedTestCommand("same-leader-earlier", key),
		FastPathEligible: true,
	})
	targetDeps := rn.q.deps()
	targetDeps[0] = earlierRef.Instance

	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: targetDeps, Command: optimizedTestCommand("same-leader-target", key)}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	rec := optimizedRequireRecord(t, rd, targetRef)
	optimizedRequireCanonicalTryPreAcceptResp(t, resp, targetRef, Ballot{Number: 1, Replica: 1}, Attributes{Seq: 1, Deps: targetDeps})
	if rec.Status != StatusPreAccepted || rec.Seq != 1 || !instanceNumsEqual(rec.Deps, targetDeps) {
		t.Fatalf("same-leader dependent TryPreAccept record = %#v, want preaccepted seq 1 deps %v", rec, targetDeps)
	}
}
