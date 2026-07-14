package epaxos

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func protocolCoverageNewRawNode(t *testing.T, id ReplicaID, voters int) *RawNode {
	t.Helper()
	rn := optimizedNewRawNode(t, id, voters)
	protocolCoverageAdvanceHardStateOnly(t, rn, 0)
	return rn
}

func protocolCoverageAdvanceHardStateOnly(t *testing.T, rn *RawNode, wantTick uint64) {
	t.Helper()
	rd := rn.Ready()
	want := HardState{Conf: rn.Status().Conf, Tick: wantTick}
	if !rd.HardState.Equal(want) || !rd.MustSync ||
		len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("hard-state-only Ready = %#v, want %#v without protocol payload", rd, want)
	}
	advanceOK(t, rn, rd)
}

func TestEncodeMessagePreservesFastPathEligibilityWireMarker(t *testing.T) {
	for _, tt := range []struct {
		name string
		fast bool
	}{
		{name: "false marker", fast: false},
		{name: "true marker", fast: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
			msg := Message{Type: MsgTryPreAccept, From: 1,
				To:      2,
				Ref:     ref,
				Ballot:  Ballot{Number: 1, Replica: 1},
				Seq:     3,
				Deps:    []InstanceNum{0, 2, 0},
				Command: optimizedTestCommand("try-encode", "try-encode-key"), FastPathEligible: tt.fast,
				DepsCommitted: 0b101}
			encoded, err := EncodeMessage(nil, msg)
			if err != nil {
				t.Fatal(err)
			}
			var decoded Message
			if err := DecodeMessage(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.FastPathEligible != tt.fast {
				t.Fatalf("decoded TryPreAccept FastPathEligible = %v, want %v from wire marker: %#v", decoded.FastPathEligible, tt.fast, decoded)
			}
			if decoded.Type != msg.Type || decoded.Ref != msg.Ref || decoded.Ballot != msg.Ballot ||
				decoded.RecordBallot != (Ballot{}) || decoded.RecordStatus != StatusNone ||
				decoded.Seq != msg.Seq || !instanceNumsEqual(decoded.Deps, msg.Deps) ||
				!commandEqual(decoded.Command, msg.Command) || decoded.DepsCommitted != msg.DepsCommitted {
				t.Fatalf("decoded TryPreAccept = %#v, want protocol fields from %#v", decoded, msg)
			}
		})
	}
}

func TestPrepareExpandsStoredDependencyVectorBeforeResponding(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 2, 3)
	ref := InstanceRef{Replica: 1, Instance: 4, Conf: 1}
	cmd := optimizedTestCommand("prepare-expand", "prepare-expand-key")
	rec := checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Number: 1, Replica: 1},
		Status:  StatusPreAccepted,
		Seq:     2,
		Deps:    []InstanceNum{7},
		Command: cmd,
	})
	rn.instances[ref] = &instance{rec: rec, phase: phasePreAccept}

	if err := rn.Step(Message{Type: MsgPrepare, From: 1, To: 2, Ref: ref, Ballot: rec.Ballot}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	expanded := optimizedRequireRecord(t, rd, ref)
	if len(expanded.Deps) != 3 || expanded.Deps[0] != 7 || expanded.Deps[1] != 0 || expanded.Deps[2] != 0 {
		t.Fatalf("prepare did not durably expand stored deps: %#v", expanded)
	}
	resp := optimizedRequireMessage(t, rd.Messages, MsgPrepareResp, 1)
	if len(resp.Deps) != 3 || resp.Deps[0] != 7 || resp.RecordStatus != StatusPreAccepted {
		t.Fatalf("prepare response exposed unexpanded deps/status: %#v", resp)
	}
}

func TestStartTryPreAcceptDoesNotReopenCommittedInstance(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	ref := InstanceRef{Replica: 1, Instance: 5, Conf: 1}
	committed := checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Number: 2, Replica: 1},
		Status:  StatusCommitted,
		Seq:     4,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("committed", "committed-key"),
	})
	inst := &instance{rec: committed, phase: phaseCommitted, generation: 11}
	rn.instances[ref] = inst

	rn.startTryPreAccept(inst, Attributes{Seq: 9, Deps: []InstanceNum{3, 3, 3}}, testVoterMask(t, rn.q.conf, 2), true)
	if inst.phase != phaseCommitted || inst.rec.Status != StatusCommitted || inst.rec.Seq != committed.Seq || inst.generation != 11 {
		t.Fatalf("TryPreAccept reopened committed instance: phase=%d record=%#v generation=%d", inst.phase, inst.rec, inst.generation)
	}
	if rn.HasReady() {
		t.Fatalf("TryPreAccept on committed instance emitted duplicate effects: %#v", rn.Ready())
	}
}

func TestTryPreAcceptCommittedAndIdempotentFollowerPaths(t *testing.T) {
	t.Run("committed instance replies with commit only", func(t *testing.T) {
		rn := protocolCoverageNewRawNode(t, 2, 3)
		ref := InstanceRef{Replica: 1, Instance: 6, Conf: 1}
		cmd := optimizedTestCommand("already-committed", "already-committed-key")
		optimizedSeedRecord(t, rn, InstanceRecord{Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusCommitted, Seq: 2, Deps: rn.q.deps(), Command: cmd})

		if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 2, Deps: rn.q.deps(), Command: cmd}); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		commit := optimizedRequireMessage(t, rd.Messages, MsgCommit, 1)
		want := rn.instances[ref].rec
		if commit.From != 2 || commit.Ref != ref || commit.Ballot != want.Ballot ||
			commit.RecordBallot == (Ballot{}) || commit.RecordBallot != want.RecordBallot ||
			commit.RecordStatus != StatusNone || commit.Seq != want.Seq ||
			!instanceNumsEqual(commit.Deps, want.Deps) || !commandEqual(commit.Command, want.Command) {
			t.Fatalf("committed TryPreAccept reply = %#v, want canonical committed tuple %#v", commit, want)
		}
		optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAcceptResp)
		if len(rd.Records) != 0 {
			t.Fatalf("committed TryPreAccept rewrote durable state: %#v", rd.Records)
		}
	})

	t.Run("duplicate matching preaccept only re-acks", func(t *testing.T) {
		rn := protocolCoverageNewRawNode(t, 2, 3)
		ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
		cmd := optimizedTestCommand("idempotent", "idempotent-key")
		attrs := Attributes{Seq: 3, Deps: []InstanceNum{0, 1, 0}}
		optimizedSeedRecord(t, rn, InstanceRecord{Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd})

		if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd}); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
		if resp.From != 2 || resp.Ref != ref || resp.Ballot != (Ballot{Number: 1, Replica: 1}) ||
			resp.RecordBallot != (Ballot{}) || resp.Reject || resp.RecordStatus != StatusPreAccepted ||
			resp.Seq != attrs.Seq || !instanceNumsEqual(resp.Deps, attrs.Deps) ||
			!commandEqual(resp.Command, Command{}) {
			t.Fatalf("idempotent TryPreAccept response = %#v, want canonical matching preaccept ack", resp)
		}
		if len(rd.Records) != 0 {
			t.Fatalf("idempotent TryPreAccept duplicated durable record: %#v", rd.Records)
		}
	})
}

func TestTryPreAcceptResponseRecoveryBranches(t *testing.T) {
	t.Run("stale rejection with higher hint restarts prepare", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 1)
		hint := Ballot{Number: 7, Replica: 2}
		want, err := hint.Next(1)
		if err != nil {
			t.Fatal(err)
		}
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: hint, Reject: true, RejectReason: RejectStaleBallot, RejectHint: hint, Deps: rn.q.deps()}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phasePrepare || inst.rec.Ballot != want {
			t.Fatalf("stale TryPreAccept rejection phase/ballot = %d/%#v, want prepare at %#v", inst.phase, inst.rec.Ballot, want)
		}
		prepare := optimizedRequireMessage(t, rn.Ready().Messages, MsgPrepare, 2)
		if prepare.From != 1 || prepare.Ref != ref || prepare.Ballot != inst.rec.Ballot ||
			prepare.RecordBallot != (Ballot{}) || prepare.RecordStatus != StatusNone ||
			prepare.Seq != 0 || len(prepare.Deps) != 0 || !commandEqual(prepare.Command, Command{}) {
			t.Fatalf("stale TryPreAccept rejection Prepare = %#v, want minimal request at ballot %#v", prepare, inst.rec.Ballot)
		}
	})

	t.Run("committed conflict falls back to slow accept", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 1)
		conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
		if err := rn.Step(Message{
			Type:           MsgTryPreAcceptResp,
			From:           2,
			To:             1,
			Ref:            ref,
			Ballot:         inst.rec.Ballot,
			Deps:           rn.q.deps(),
			Reject:         true,
			RejectReason:   RejectCommittedConflict,
			ConflictRef:    conflictRef,
			ConflictStatus: StatusCommitted,
		}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted || inst.rec.FastPathEligible {
			t.Fatalf("committed-conflict TryPreAccept rejection phase/record = %d/%#v, want non-fast accepted", inst.phase, inst.rec)
		}
		accept := optimizedRequireMessage(t, rn.Ready().Messages, MsgAccept, 2)
		if accept.From != 1 || accept.Ref != ref || accept.Ballot != inst.rec.Ballot ||
			accept.RecordBallot != (Ballot{}) || accept.RecordStatus != StatusNone ||
			accept.Seq != inst.rec.Seq || !instanceNumsEqual(accept.Deps, inst.rec.Deps) ||
			!commandEqual(accept.Command, inst.rec.Command) {
			t.Fatalf("committed-conflict slow Accept = %#v, want canonical tuple %#v", accept, inst.rec)
		}
	})

	t.Run("older ballot response is ignored", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 3)
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 2, Replica: 1}, Seq: inst.rec.Seq, Deps: inst.rec.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseTryPreAccept || inst.tryOK.len() != 0 {
			t.Fatalf("older-ballot TryPreAccept response changed recovery: phase=%d tryOK=%#v", inst.phase, inst.tryOK)
		}
		if rn.HasReady() {
			t.Fatalf("older-ballot TryPreAccept response emitted effects: %#v", rn.Ready())
		}
	})

	t.Run("nil vote set records first response without quorum", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 1)
		inst.tryOK = 0
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: inst.rec.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseTryPreAccept || inst.tryOK.len() != 1 {
			t.Fatalf("first TryPreAccept response with nil vote set phase/tryOK = %d/%#v, want one recorded vote and no quorum", inst.phase, inst.tryOK)
		}
		if !inst.tryOK.has(rn.q.conf, 2) {
			t.Fatalf("first TryPreAccept response was not recorded: %#v", inst.tryOK)
		}
		if rn.HasReady() {
			t.Fatalf("single TryPreAccept response unexpectedly reached quorum: %#v", rn.Ready())
		}
	})

	t.Run("duplicate response does not count twice", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 1)
		inst.tryOK = testVoterMask(t, rn.q.conf, 2)
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: inst.rec.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseTryPreAccept || inst.tryOK.len() != 1 {
			t.Fatalf("duplicate TryPreAccept response changed quorum state: phase=%d tryOK=%#v", inst.phase, inst.tryOK)
		}
		if rn.HasReady() {
			t.Fatalf("duplicate TryPreAccept response emitted accept/records: %#v", rn.Ready())
		}
	})
}

func TestPreAcceptRespAllocatesNilVoteMapAndRecordsResponse(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	ref := InstanceRef{Replica: 1, Instance: 31, Conf: 1}
	rec := checkedRecord(InstanceRecord{
		Ref:              ref,
		Ballot:           Ballot{Number: 1, Replica: 1},
		Status:           StatusPreAccepted,
		Seq:              2,
		Deps:             rn.q.deps(),
		Command:          optimizedTestCommand("preaccept-nil-map", "preaccept-nil-map-key"),
		FastPathEligible: true,
	})
	inst := &instance{rec: rec, phase: phasePreAccept, preOK: nil}
	rn.instances[ref] = inst
	rn.indexConflicts(rec)

	respDeps := []InstanceNum{0, 1, 0}
	const depsCommitted uint64 = 0b101
	if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 2,
		To:     1,
		Ref:    ref,
		Ballot: rec.Ballot,
		Seq:    4,
		Deps:   respDeps, FastPathEligible: true,
		DepsCommitted: depsCommitted}); err != nil {
		t.Fatal(err)
	}
	if inst.preOK == nil {
		t.Fatal("PreAccept response left nil vote map")
	}
	vote, ok := inst.preOK.get(rn.q.conf, 2)
	if !ok {
		t.Fatalf("PreAccept response was not recorded: %#v", inst.preOK)
	}
	if vote.seq != 4 || !instanceNumsEqual(vote.attributes().Deps, respDeps) || vote.depsCommitted != depsCommitted || !vote.fastPathEligible {
		t.Fatalf("recorded PreAccept vote = %#v, want seq 4 deps %v depsCommitted %03b fast-path eligible", vote, respDeps, depsCommitted)
	}
	if got := inst.preOK.len(); got != 1 {
		t.Fatalf("PreAccept vote count = %d, want only the remote response recorded", got)
	}
	if inst.phase != phasePreAccept {
		t.Fatalf("single PreAccept response with nil vote map phase = %d, want preaccept before quorum", inst.phase)
	}
	if rn.HasReady() {
		t.Fatalf("single PreAccept response with nil vote map unexpectedly emitted ready work: %#v", rn.Ready())
	}
}

func TestTryPreAcceptCommittedConflictRejectionAddsSlowAcceptDependency(t *testing.T) {
	rn, inst, ref := protocolTryInstance(t, 3, 1)
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	conflictSeq := uint64(4)
	conflict := checkedRecord(InstanceRecord{
		Ref:     conflictRef,
		Ballot:  Ballot{Number: 1, Replica: 2},
		Status:  StatusCommitted,
		Seq:     conflictSeq,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("committed-conflict", "try-response-key"),
	})
	rn.instances[conflictRef] = &instance{rec: conflict, phase: phaseCommitted}

	if err := rn.Step(Message{
		Type:           MsgTryPreAcceptResp,
		From:           2,
		To:             1,
		Ref:            ref,
		Ballot:         inst.rec.Ballot,
		Reject:         true,
		RejectReason:   RejectCommittedConflict,
		ConflictRef:    conflictRef,
		ConflictStatus: StatusCommitted,
		Deps:           rn.q.deps(),
	}); err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	wantDeps := rn.q.deps()
	wantDeps[1] = conflictRef.Instance
	wantSeq := conflictSeq + 1
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusAccepted || rec.FastPathEligible || rec.Seq != wantSeq || !instanceNumsEqual(rec.Deps, wantDeps) {
		t.Fatalf("committed-conflict slow accept record = %#v, want non-fast accepted seq %d deps %v", rec, wantSeq, wantDeps)
	}
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
		if msg.From != 1 || msg.Ref != ref || msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
			msg.Seq != wantSeq || !instanceNumsEqual(msg.Deps, wantDeps) ||
			!commandEqual(msg.Command, inst.rec.Command) {
			t.Fatalf("committed-conflict slow accept message to %d = %#v, want canonical seq %d deps %v", to, msg, wantSeq, wantDeps)
		}
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("committed-conflict slow accept emitted committed commands: %#v", rd.Committed)
	}
}

func TestPrepareResponsePreservesOriginalAcceptEvidenceSender(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 2, 3)
	ref := InstanceRef{Replica: 1, Instance: 8, Conf: 1}
	reporter := rn.id
	originalSender := ReplicaID(3)
	evidenceDeps := []InstanceNum{0, 4, 0}
	rec := checkedRecord(InstanceRecord{
		Ref:            ref,
		Ballot:         Ballot{Number: 1, Replica: 1},
		RecordBallot:   Ballot{Number: 1, Replica: 1},
		Status:         StatusAccepted,
		Seq:            2,
		Deps:           rn.q.deps(),
		Command:        optimizedTestCommand("prepare-evidence-sender", "prepare-evidence-sender-key"),
		AcceptEvidence: []AcceptEvidence{{Sender: originalSender, Seq: 5, Deps: evidenceDeps}},
	})
	rn.instances[ref] = &instance{rec: rec, phase: phaseAccept}

	if err := rn.Step(Message{Type: MsgPrepare, From: 1, To: reporter, Ref: ref, Ballot: rec.Ballot}); err != nil {
		t.Fatal(err)
	}

	resp := optimizedRequireMessage(t, rn.Ready().Messages, MsgPrepareResp, 1)
	if len(resp.AcceptEvidence) != 1 {
		t.Fatalf("prepare response carried %d AcceptEvidence entries, want the original evidence from sender %d: %#v", len(resp.AcceptEvidence), originalSender, resp)
	}
	got := resp.AcceptEvidence[0]
	if got.Sender != originalSender || got.Sender == reporter || got.Seq != 5 || !instanceNumsEqual(got.Deps, evidenceDeps) {
		t.Fatalf("prepare reporter %d rewrote AcceptEvidence to %#v, want sender %d seq 5 deps %v", reporter, got, originalSender, evidenceDeps)
	}
}

func TestLegacyAggregateAcceptDepsCannotAuthorizeTryPreAcceptIgnoreMarker(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 2, 3)
	key := "legacy-aggregate-accept-deps-ignore"
	targetRef := InstanceRef{Replica: 1, Instance: 9, Conf: 1}
	conflictRef := InstanceRef{Replica: 2, Instance: 2, Conf: 1}
	chosenDeps := rn.q.deps()
	legacyAggregateDeps := rn.q.deps()
	legacyAggregateDeps[0] = targetRef.Instance
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:        conflictRef,
		Ballot:     Ballot{Number: 1, Replica: 2},
		Status:     StatusCommitted,
		Seq:        1,
		Deps:       chosenDeps,
		AcceptSeq:  6,
		AcceptDeps: legacyAggregateDeps,
		Command:    optimizedTestCommand("legacy-aggregate-conflict", key),
	})

	targetDeps := rn.q.deps()
	targetDeps[1] = conflictRef.Instance
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: targetRef, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: targetDeps, Command: optimizedTestCommand("target", key)}); err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
	if !resp.Reject || resp.RejectReason != RejectCommittedConflict || resp.ConflictRef != conflictRef || resp.ConflictStatus != StatusCommitted {
		t.Fatalf("legacy aggregate AcceptSeq/AcceptDeps TryPreAccept response = %#v, want committed conflict %s without aggregate evidence waiver", resp, conflictRef)
	}
	for _, rec := range rd.Records {
		if rec.Ref == targetRef && rec.Status >= StatusPreAccepted {
			t.Fatalf("legacy aggregate AcceptSeq/AcceptDeps persisted target record despite committed conflict: %#v", rec)
		}
	}
	if inst := rn.instances[targetRef]; inst != nil && inst.rec.Status >= StatusPreAccepted {
		t.Fatalf("legacy aggregate AcceptSeq/AcceptDeps stored target despite committed conflict: %#v", inst.rec)
	}
}

func TestDependencyPredicatesConservativelyRejectMissingConfigKnowledge(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	if rn.attrsDependsOn(Attributes{Seq: 1, Deps: []InstanceNum{0, 4, 0}}, InstanceRef{Replica: 2, Instance: 1, Conf: 2}, 1) {
		t.Fatal("dependency from another configuration was treated as known in this config")
	}
	if rn.attrsDependsOn(Attributes{Seq: 1, Deps: []InstanceNum{0, 4, 0}}, InstanceRef{Replica: 99, Instance: 1, Conf: 1}, 1) {
		t.Fatal("dependency for a non-voter replica was treated as known")
	}
	if rn.attrsDependsOn(Attributes{Seq: 1, Deps: []InstanceNum{0, 4}}, InstanceRef{Replica: 3, Instance: 1, Conf: 1}, 1) {
		t.Fatal("dependency beyond the available dependency vector was treated as known")
	}
	if !rn.attrsDependsOn(Attributes{Seq: 1, Deps: []InstanceNum{0, 4, 0}}, InstanceRef{Replica: 2, Instance: 4, Conf: 1}, 1) {
		t.Fatal("known dependency in the same config was not recognized")
	}
	if attrsCover(Attributes{Seq: 2, Deps: []InstanceNum{0, 1, 0}}, Attributes{Seq: 1, Deps: []InstanceNum{0, 1, 0}}) {
		t.Fatal("stale timed fast-path attrs covered newer local attrs")
	}
	if attrsCover(Attributes{Seq: 1, Deps: []InstanceNum{0, 1, 0}}, Attributes{Seq: 2, Deps: []InstanceNum{0, 1}}) {
		t.Fatal("short timed fast-path attrs covered wider local deps")
	}

	single := protocolCoverageNewRawNode(t, 1, 1)
	if got := single.recoveryCoordinator(InstanceRef{Replica: 1, Instance: 1, Conf: 1}); got != 1 {
		t.Fatalf("single-voter recovery coordinator = %d, want self", got)
	}
	rn.confHistory[9] = ConfState{ID: 9, Voters: []ReplicaID{1, 1}}
	if got := rn.recoveryCoordinator(InstanceRef{Replica: 1, Instance: 1, Conf: 9}); got != 0 {
		t.Fatalf("recovery coordinator with no non-owner voter = %d, want no coordinator", got)
	}
}

func TestTryPreAcceptTimerRebroadcastsOnlyWhileInTryPhase(t *testing.T) {
	rn, inst, ref := protocolTryInstance(t, 3, 1)
	if err := rn.onTimer(inst, timerTryPreAccept); err != nil {
		panic(err)
	}
	rd := rn.Ready()
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgTryPreAccept, to)
		if msg.From != 1 || msg.Ref != ref || msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
			msg.Seq != inst.rec.Seq || !instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
			!commandEqual(msg.Command, inst.rec.Command) || msg.FastPathEligible {
			t.Fatalf("TryPreAccept timer message to %d = %#v, want canonical non-fast recovery tuple %#v", to, msg, inst.rec)
		}
	}
	if len(rd.Records) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("TryPreAccept timer emitted durable/application effects: %#v", rd)
	}
}

func TestTryPreAcceptTimerDropsStaleEvidenceChecksBeforeRetry(t *testing.T) {
	rn, inst, ref := protocolTryInstance(t, 3, 3)
	staleTargetKey := tryEvidenceKey{
		target:   ref,
		conflict: InstanceRef{Replica: 2, Instance: 11, Conf: 1},
		ballot:   Ballot{Number: inst.rec.Ballot.Number - 1, Replica: inst.rec.Ballot.Replica},
	}
	unrelatedTargetKey := tryEvidenceKey{
		target:   InstanceRef{Replica: 3, Instance: 99, Conf: 1},
		conflict: InstanceRef{Replica: 2, Instance: 12, Conf: 1},
		ballot:   inst.rec.Ballot,
	}
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{
		staleTargetKey:     {},
		unrelatedTargetKey: {},
	}

	if err := rn.onTimer(inst, timerTryPreAccept); err != nil {
		panic(err)
	}
	if _, ok := rn.tryEvidenceChecks[staleTargetKey]; ok {
		t.Fatalf("TryPreAccept timer left stale evidence check for target: %#v", rn.tryEvidenceChecks)
	}
	if _, ok := rn.tryEvidenceChecks[unrelatedTargetKey]; !ok {
		t.Fatalf("TryPreAccept timer removed unrelated current-ballot evidence check: %#v", rn.tryEvidenceChecks)
	}
	if inst.phase != phaseTryPreAccept || inst.rec.Status != StatusPreAccepted {
		t.Fatalf("TryPreAccept timer phase/status = %d/%s, want try-pre-accept/%s", inst.phase, inst.rec.Status, StatusPreAccepted)
	}
	rd := rn.Ready()
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgTryPreAccept, to)
		if msg.From != 1 || msg.Ref != ref || msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
			msg.Seq != inst.rec.Seq || !instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
			!commandEqual(msg.Command, inst.rec.Command) || msg.FastPathEligible {
			t.Fatalf("TryPreAccept timer retry message to %d = %#v, want canonical recovery tuple %#v", to, msg, inst.rec)
		}
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	if len(rd.Records) != 0 || len(rd.Committed) != 0 || rd.MustSync {
		t.Fatalf("TryPreAccept timer retry emitted durable/application effects: %#v", rd)
	}
	advanceOK(t, rn, rd)
	if rn.HasReady() {
		t.Fatalf("accepted first TryPreAccept retry Ready left outstanding work: %#v", rn.Ready())
	}
	for tick := uint64(1); tick < rn.retryTicks; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		protocolCoverageAdvanceHardStateOnly(t, rn, tick)
	}

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	retry := rn.Ready()
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, retry.Messages, MsgTryPreAccept, to)
		if msg.From != 1 || msg.Ref != ref || msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
			msg.Seq != inst.rec.Seq || !instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
			!commandEqual(msg.Command, inst.rec.Command) || msg.FastPathEligible {
			t.Fatalf("rescheduled TryPreAccept retry message to %d = %#v, want canonical recovery tuple %#v", to, msg, inst.rec)
		}
	}
	optimizedRequireNoMessageType(t, retry.Messages, MsgAccept)
	if !retry.HardState.Equal(HardState{Conf: rn.Status().Conf, Tick: rn.tick}) ||
		len(retry.Records) != 0 || len(retry.Committed) != 0 || !retry.MustSync {
		t.Fatalf("rescheduled TryPreAccept retry did not carry only tick durability plus messages: %#v", retry)
	}
}

func TestEnsureDependencyRecoveryNoopsForAlreadyResolvedOrInFlightWork(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	rn.ensureDependencyRecovery(InstanceRef{}, false)
	if rn.HasReady() || len(rn.instances) != 0 {
		t.Fatalf("zero dependency started recovery: ready=%v instances=%#v", rn.HasReady(), rn.instances)
	}

	executedRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	rn.executed.add(executedRef)
	rn.ensureDependencyRecovery(executedRef, false)
	if _, ok := rn.instances[executedRef]; ok || rn.HasReady() {
		t.Fatalf("executed dependency restarted recovery: ready=%v instance=%#v", rn.HasReady(), rn.instances[executedRef])
	}

	committedRef := InstanceRef{Replica: 2, Instance: 2, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{Ref: committedRef, Ballot: Ballot{Number: 1, Replica: 2}, Status: StatusCommitted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("committed-dep", "committed-dep-key")})
	rn.ensureDependencyRecovery(committedRef, false)
	if rn.HasReady() {
		t.Fatalf("committed dependency restarted recovery: %#v", rn.Ready())
	}

	preparingRef := InstanceRef{Replica: 2, Instance: 3, Conf: 1}
	preparing := &instance{rec: checkedRecord(InstanceRecord{Ref: preparingRef, Ballot: Ballot{Number: 2, Replica: 1}, Status: StatusNone, Deps: rn.q.deps()}), phase: phasePrepare, generation: 5}
	rn.installInstance(preparing)
	rn.ensureDependencyRecovery(preparingRef, false)
	if preparing.phase != phasePrepare || preparing.generation != 5 || rn.HasReady() {
		t.Fatalf("in-flight local prepare was restarted: phase=%d generation=%d ready=%v", preparing.phase, preparing.generation, rn.HasReady())
	}
}

func TestDependencyKnownAfterRequiresBothRecordsAndMinimumStatus(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	base := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	other := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	if rn.dependencyKnownAfter(base, other, StatusCommitted) {
		t.Fatal("missing records made dependency appear known after base")
	}
	optimizedSeedRecord(t, rn, InstanceRecord{Ref: base, Status: StatusCommitted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("base", "dep-known-key")})
	optimizedSeedRecord(t, rn, InstanceRecord{Ref: other, Status: StatusPreAccepted, Seq: 3, Deps: []InstanceNum{1, 0, 0}, Command: optimizedTestCommand("other", "dep-known-key")})
	if rn.dependencyKnownAfter(base, other, StatusCommitted) {
		t.Fatal("preaccepted record satisfied committed dependency knowledge")
	}
}

func TestSenderPreservingEvidenceValidationAndMergeContracts(t *testing.T) {
	if MsgEvidence.String() != "evidence" {
		t.Fatalf("MsgEvidence.String() = %q, want evidence", MsgEvidence.String())
	}
	if MsgEvidenceResp.String() != "evidence-resp" {
		t.Fatalf("MsgEvidenceResp.String() = %q, want evidence-resp", MsgEvidenceResp.String())
	}

	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	deps := []InstanceNum{0, 1, 0}
	validEvidenceResp := Message{
		Type:           MsgEvidenceResp,
		From:           2,
		To:             1,
		Ref:            conflict,
		ConflictRef:    ref,
		Ballot:         Ballot{Number: 1, Replica: 1},
		RecordBallot:   Ballot{Number: 1, Replica: 2},
		RecordStatus:   StatusCommitted,
		Seq:            3,
		Deps:           append([]InstanceNum(nil), deps...),
		Command:        optimizedTestCommand("evidence-value", "evidence-key"),
		AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 4, Deps: append([]InstanceNum(nil), deps...)}},
	}
	if err := validEvidenceResp.Validate(conf); err != nil {
		t.Fatalf("valid MsgEvidenceResp rejected: %v", err)
	}
	duplicateSameSender := validEvidenceResp.Clone()
	duplicateSameSender.AcceptEvidence = append(duplicateSameSender.AcceptEvidence, AcceptEvidence{Sender: 2, Seq: 4, Deps: append([]InstanceNum(nil), deps...)})
	if err := duplicateSameSender.Validate(conf); err != nil {
		t.Fatalf("duplicate identical AcceptEvidence rejected: %v", err)
	}

	validEvidenceReq := Message{Type: MsgEvidence, From: 1, To: 2, Ref: conflict, ConflictRef: ref, Ballot: Ballot{Number: 1, Replica: 1}}
	if err := validEvidenceReq.Validate(conf); err != nil {
		t.Fatalf("valid read-only MsgEvidence rejected: %v", err)
	}

	for _, tc := range []struct {
		name string
		msg  Message
	}{
		{
			name: "evidence missing target conflict",
			msg:  Message{Type: MsgEvidence, From: 1, To: 2, Ref: conflict, Ballot: Ballot{Number: 1, Replica: 1}},
		},
		{
			name: "evidence cross configuration conflict",
			msg:  Message{Type: MsgEvidence, From: 1, To: 2, Ref: conflict, ConflictRef: InstanceRef{Replica: 1, Instance: 1, Conf: 2}, Ballot: Ballot{Number: 1, Replica: 1}},
		},
		{
			name: "accept evidence on preaccept",
			msg:  Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 2, Deps: append([]InstanceNum(nil), deps...), Command: optimizedTestCommand("preaccept", "evidence-key"), AcceptEvidence: []AcceptEvidence{{Sender: 1, Seq: 2, Deps: append([]InstanceNum(nil), deps...)}}},
		},
		{
			name: "accept evidence from non voter",
			msg: func() Message {
				msg := validEvidenceResp.Clone()
				msg.AcceptEvidence[0].Sender = 9
				return msg
			}(),
		},
		{
			name: "accept evidence without sender",
			msg: func() Message {
				msg := validEvidenceResp.Clone()
				msg.AcceptEvidence[0].Sender = 0
				return msg
			}(),
		},
		{
			name: "accept evidence without seq",
			msg: func() Message {
				msg := validEvidenceResp.Clone()
				msg.AcceptEvidence[0].Seq = 0
				return msg
			}(),
		},
		{
			name: "accept evidence short deps",
			msg: func() Message {
				msg := validEvidenceResp.Clone()
				msg.AcceptEvidence[0].Deps = msg.AcceptEvidence[0].Deps[:2]
				return msg
			}(),
		},
		{
			name: "duplicate sender conflicting tuple",
			msg: func() Message {
				msg := validEvidenceResp.Clone()
				msg.AcceptEvidence = append(msg.AcceptEvidence, AcceptEvidence{Sender: 2, Seq: 5, Deps: append([]InstanceNum(nil), deps...)})
				return msg
			}(),
		},
		{
			name: "ignore marker on evidence response",
			msg: func() Message {
				msg := validEvidenceResp.Clone()
				msg.IgnoreDependency.Ref = ref
				return msg
			}(),
		},
		{
			name: "ignore marker cross configuration",
			msg: Message{
				Type:             MsgTryPreAccept,
				From:             1,
				To:               2,
				Ref:              ref,
				Ballot:           Ballot{Number: 1, Replica: 1},
				Seq:              2,
				Deps:             append([]InstanceNum(nil), deps...),
				Command:          optimizedTestCommand("try-ignore", "evidence-key"),
				IgnoreDependency: TryPreAcceptIgnore{Ref: InstanceRef{Replica: 2, Instance: 1, Conf: 2}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.msg.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("Validate err=%v, want %v", err, ErrInvalidMessage)
			}
		})
	}

	merged := mergeAcceptEvidenceEntries(
		[]AcceptEvidence{{Sender: 1, Seq: 2, Deps: []InstanceNum{1, 0, 0}}},
		[]AcceptEvidence{
			{Sender: 0, Seq: 99, Deps: []InstanceNum{9, 9, 9}},
			{Sender: 1, Seq: 2, Deps: []InstanceNum{1, 0, 0}},
			{Sender: 1, Seq: 3, Deps: []InstanceNum{0, 4, 0}},
			{Sender: 2, Seq: 4, Deps: []InstanceNum{0, 3, 0}},
		},
	)
	if !acceptEvidenceEqual(merged, []AcceptEvidence{
		{Sender: 1, Seq: 3, Deps: []InstanceNum{1, 4, 0}},
		{Sender: 2, Seq: 4, Deps: []InstanceNum{0, 3, 0}},
	}) {
		t.Fatalf("merged sender evidence = %#v", merged)
	}
	mergedEvidenceResp := validEvidenceResp.Clone()
	mergedEvidenceResp.AcceptEvidence = cloneAcceptEvidence(merged)
	if err := mergedEvidenceResp.Validate(conf); err != nil {
		t.Fatalf("merged sender evidence response rejected: %v", err)
	}
	if acceptEvidenceEqual([]AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{1}}}, []AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{2}}}) {
		t.Fatal("AcceptEvidence equality ignored dependency differences")
	}
	if acceptEvidenceEqual([]AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{1}}}, []AcceptEvidence{{Sender: 2, Seq: 1, Deps: []InstanceNum{1}}}) {
		t.Fatal("AcceptEvidence equality ignored sender differences")
	}
}

func TestMessageValidateRejectsSemanticMalleability(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	deps := []InstanceNum{0, 0, 0}
	command := optimizedTestCommand("semantic-message", "semantic-message-key")
	valid := map[MessageType]Message{
		MsgPreAccept:        {Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: deps, Command: command},
		MsgPreAcceptResp:    {Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: deps},
		MsgAccept:           {Type: MsgAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: deps, Command: command},
		MsgAcceptResp:       {Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1}, Seq: 1, Deps: deps, RecordStatus: StatusAccepted},
		MsgCommit:           {Type: MsgCommit, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1}, Seq: 1, Deps: deps, Command: command},
		MsgPrepare:          {Type: MsgPrepare, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}},
		MsgPrepareResp:      {Type: MsgPrepareResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Deps: deps, RecordStatus: StatusNone},
		MsgTryPreAccept:     {Type: MsgTryPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: deps, Command: command},
		MsgTryPreAcceptResp: {Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 1, Deps: deps, RecordStatus: StatusPreAccepted},
		MsgEvidence:         {Type: MsgEvidence, From: 1, To: 2, Ref: ref, ConflictRef: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Ballot: Ballot{Number: 1, Replica: 1}},
		MsgEvidenceResp:     {Type: MsgEvidenceResp, From: 2, To: 1, Ref: ref, ConflictRef: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Ballot: Ballot{Number: 1, Replica: 1}, RecordBallot: Ballot{Number: 2, Replica: 2}, Seq: 1, Deps: deps, Command: command, RecordStatus: StatusCommitted},
	}
	for typ := MsgPreAccept; typ <= MsgEvidenceResp; typ++ {
		msg, ok := valid[typ]
		if !ok {
			t.Fatalf("missing valid fixture for %s", typ)
		}
		if err := msg.Validate(conf); err != nil {
			t.Fatalf("valid %s rejected: %v; message=%#v", typ, err, msg)
		}
	}
	for _, malformedConf := range []ConfState{
		{ID: 1, Voters: []ReplicaID{1, 2, 3, 4, 5, 6, 7, 8}},
		{ID: 1, Voters: []ReplicaID{1, 2, 2}},
		{ID: 1, Voters: []ReplicaID{0, 1, 2}},
	} {
		if err := valid[MsgCommit].Validate(malformedConf); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("Validate malformed configuration %#v err=%v, want ErrInvalidMessage", malformedConf, err)
		}
	}

	base := valid[MsgCommit]
	tests := []struct {
		name string
		msg  Message
		want error
	}{
		{name: "zero owner", msg: func() Message { msg := base; msg.Ref.Replica = 0; return msg }(), want: ErrInvalidMessage},
		{name: "zero instance", msg: func() Message { msg := base; msg.Ref.Instance = 0; return msg }(), want: ErrInvalidMessage},
		{name: "zero configuration", msg: func() Message { msg := base; msg.Ref.Conf = 0; return msg }(), want: ErrInvalidMessage},
		{name: "wrong configuration", msg: func() Message { msg := base; msg.Ref.Conf = 2; return msg }(), want: ErrInvalidMessage},
		{name: "non-voter owner", msg: func() Message { msg := base; msg.Ref.Replica = 9; return msg }(), want: ErrMessageRejected},
		{name: "non-voter ballot", msg: func() Message { msg := base; msg.Ballot.Replica = 9; return msg }(), want: ErrInvalidMessage},
		{name: "record ballot above promise", msg: func() Message {
			msg := base
			msg.RecordBallot = Ballot{Number: 2, Replica: 1}
			return msg
		}(), want: ErrInvalidMessage},
		{name: "invalid record status", msg: func() Message { msg := base; msg.RecordStatus = Status(255); return msg }(), want: ErrInvalidMessage},
		{name: "invalid conflict status", msg: func() Message { msg := base; msg.ConflictStatus = Status(255); return msg }(), want: ErrInvalidMessage},
		{name: "invalid command kind", msg: func() Message { msg := base; msg.Command.Kind = CommandKind(255); return msg }(), want: ErrInvalidMessage},
		{name: "zero commit sequence", msg: func() Message { msg := base; msg.Seq = 0; return msg }(), want: ErrInvalidMessage},
		{name: "partial conflict reference", msg: func() Message {
			msg := valid[MsgEvidence]
			msg.ConflictRef.Instance = 0
			return msg
		}(), want: ErrInvalidMessage},
		{name: "partial ignore reference", msg: func() Message {
			msg := valid[MsgTryPreAccept]
			msg.IgnoreDependency.Ref = InstanceRef{Replica: 2, Conf: 1}
			return msg
		}(), want: ErrInvalidMessage},
		{name: "conflict reject without conflict", msg: Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Deps: deps, Reject: true, RejectReason: RejectCommittedConflict}, want: ErrInvalidMessage},
		{name: "stale reject without hint", msg: Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Deps: deps, Reject: true, RejectReason: RejectStaleBallot}, want: ErrInvalidMessage},
		{name: "dependency evidence on commit", msg: func() Message { msg := base; msg.DepsCommitted = 1; return msg }(), want: ErrInvalidMessage},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.msg.Validate(conf); !errors.Is(err, tc.want) {
				t.Fatalf("Validate err=%v, want %v; message=%#v", err, tc.want, tc.msg)
			}
		})
	}
}

func TestMessageValidateHotPathAllocatesZero(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	msg := Message{
		Type:         MsgEvidenceResp,
		From:         2,
		To:           1,
		Ref:          InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		ConflictRef:  InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:       Ballot{Number: 1, Replica: 1},
		RecordBallot: Ballot{Number: 2, Replica: 2},
		RecordStatus: StatusCommitted,
		Seq:          2,
		Deps:         []InstanceNum{0, 1, 0},
		Command:      optimizedTestCommand("validation-allocation", "validation-allocation-key"),
		AcceptEvidence: []AcceptEvidence{{
			Sender: 2,
			Seq:    2,
			Deps:   []InstanceNum{0, 1, 0},
		}},
	}
	var validationErr error
	allocs := testing.AllocsPerRun(1000, func() {
		validationErr = msg.Validate(conf)
	})
	if validationErr != nil {
		t.Fatalf("Validate returned %v", validationErr)
	}
	if allocs != 0 {
		t.Fatalf("Validate allocations = %v, want 0", allocs)
	}
}

func TestMessageValidateRejectsCommittedFastPathRecoveryEvidence(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	target := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	base := Message{
		From:             2,
		To:               1,
		Ref:              target,
		Ballot:           Ballot{Number: 1, Replica: 1},
		RecordBallot:     Ballot{Replica: 2},
		RecordStatus:     StatusCommitted,
		Seq:              2,
		Deps:             []InstanceNum{0, 1, 0},
		Command:          optimizedTestCommand("committed-fast-evidence", "committed-fast-evidence-key"),
		FastPathEligible: true,
	}
	for _, typ := range []MessageType{MsgPrepareResp, MsgEvidenceResp} {
		msg := base.Clone()
		msg.Type = typ
		if typ == MsgEvidenceResp {
			msg.Ref = conflict
			msg.ConflictRef = target
		}
		if err := msg.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("%s committed fast-path evidence err=%v, want ErrInvalidMessage", typ, err)
		}
	}
}

func TestCodecRejectsMalformedSenderEvidenceWireFrames(t *testing.T) {
	base := Message{
		Type:           MsgEvidenceResp,
		From:           2,
		To:             1,
		Ref:            InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		ConflictRef:    InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:         Ballot{Number: 1, Replica: 1},
		RecordBallot:   Ballot{Number: 1, Replica: 2},
		RecordStatus:   StatusCommitted,
		Seq:            2,
		Deps:           []InstanceNum{0, 0, 0},
		Command:        optimizedTestCommand("codec-evidence", "codec-evidence-key"),
		AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 2, Deps: make([]InstanceNum, maxWireDeps+1)}},
	}
	prefix := []byte("caller-prefix")
	if got, err := EncodeMessage(prefix, base); !errors.Is(err, ErrInvalidMessage) || string(got) != string(prefix) {
		t.Fatalf("EncodeMessage overwide sender evidence deps got (%q, %v), want unchanged prefix and ErrInvalidMessage", got, err)
	}

	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{name: "too many evidence entries", frame: protocolMalformedEvidenceFrame(maxWireAcceptEvidence+1, 0, 0)},
		{name: "too many evidence deps", frame: protocolMalformedEvidenceFrame(1, maxWireDeps+1, 0)},
		{name: "too many command conflict keys after evidence", frame: protocolMalformedEvidenceFrame(0, 0, maxWireConflictKeys+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out Message
			if err := DecodeMessage(tc.frame, &out); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("DecodeMessage err=%v, want %v", err, ErrInvalidMessage)
			}
			if out.Type != 0 || len(out.AcceptEvidence) != 0 || len(out.Command.ConflictKeys) != 0 {
				t.Fatalf("decode failure retained stale data: %#v", out)
			}
		})
	}
}

func TestLegacyRecordBallotChecksumVerifierAcceptsOnlyLegacyLayout(t *testing.T) {
	rec := InstanceRecord{
		Ref:            InstanceRef{Replica: 1, Instance: 8, Conf: 1},
		Ballot:         Ballot{Epoch: 1, Number: 3, Replica: 1},
		RecordBallot:   Ballot{Epoch: 1, Number: 2, Replica: 2},
		Status:         StatusCommitted,
		Seq:            7,
		Deps:           []InstanceNum{0, 4, 0},
		AcceptSeq:      9,
		AcceptDeps:     []InstanceNum{0, 5, 0},
		AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 9, Deps: []InstanceNum{0, 5, 0}}},
		Command:        optimizedTestCommand("legacy-record-ballot", "legacy-record-ballot-key"),
	}
	legacy := rec.Clone()
	legacy.RecordBallot = Ballot{}
	legacy.AcceptEvidence = nil
	legacy.Checksum = checksumRecord(legacy, true, true, true, false, false, false)
	if VerifyRecordChecksum(legacy) {
		t.Fatalf("legacy checksum without durable record ballot was accepted as canonical: %#v", legacy)
	}
	if !VerifyRecordChecksumWithoutRecordBallot(legacy) {
		t.Fatalf("legacy checksum without durable record ballot was rejected: %#v", legacy)
	}
	canonical := rec.Clone()
	canonical.Checksum = ChecksumRecord(canonical)
	if VerifyRecordChecksumWithoutRecordBallot(canonical) {
		t.Fatalf("canonical checksum was accepted as legacy no-record-ballot layout: %#v", canonical)
	}
}

func TestEvidenceRequestIsReadOnlyAndPreservesSenderEvidence(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 2, 3)
	ref := InstanceRef{Replica: 1, Instance: 10, Conf: 1}
	target := InstanceRef{Replica: 3, Instance: 4, Conf: 1}
	acceptDeps := []InstanceNum{0, 0, target.Instance}
	rec := checkedRecord(InstanceRecord{
		Ref:            ref,
		Ballot:         Ballot{Number: 1, Replica: 1},
		RecordBallot:   Ballot{Number: 1, Replica: 1},
		Status:         StatusCommitted,
		Seq:            5,
		Deps:           []InstanceNum{7},
		AcceptSeq:      6,
		AcceptDeps:     append([]InstanceNum(nil), acceptDeps...),
		AcceptEvidence: []AcceptEvidence{{Sender: 3, Seq: 6, Deps: append([]InstanceNum(nil), acceptDeps...)}},
		Command:        optimizedTestCommand("evidence-read", "evidence-read-key"),
	})
	rn.instances[ref] = &instance{rec: rec, phase: phaseCommitted}

	if err := rn.Step(Message{Type: MsgEvidence, From: 1, To: 2, Ref: ref, ConflictRef: target, Ballot: Ballot{Number: 2, Replica: 1}}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgEvidenceResp, 1)
	if resp.Ref != ref || resp.ConflictRef != target || resp.RecordStatus != StatusCommitted || resp.RecordBallot != rec.RecordBallot || resp.Seq != rec.Seq {
		t.Fatalf("evidence response tuple = %#v, want committed record %s without changing the target link %s", resp, ref, target)
	}
	if len(resp.Deps) != 3 || resp.Deps[0] != 7 || resp.Deps[1] != 0 || resp.Deps[2] != 0 {
		t.Fatalf("evidence response deps = %v, want expanded copy of short stored deps", resp.Deps)
	}
	if len(resp.AcceptEvidence) != 1 || resp.AcceptEvidence[0].Sender != 3 || !instanceNumsEqual(resp.AcceptEvidence[0].Deps, acceptDeps) {
		t.Fatalf("evidence response rewrote sender-preserving AcceptEvidence: %#v", resp.AcceptEvidence)
	}
	if len(rd.Records) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("read-only MsgEvidence emitted durable or application effects: %#v", rd)
	}
	if got := rn.instances[ref].rec.Deps; len(got) != 1 || got[0] != 7 {
		t.Fatalf("read-only MsgEvidence mutated stored deps: %v", got)
	}
	advanceOK(t, rn, rd)

	pendingRef := InstanceRef{Replica: 1, Instance: 11, Conf: 1}
	pending := checkedRecord(InstanceRecord{
		Ref:        pendingRef,
		Ballot:     Ballot{Number: 1, Replica: 1},
		Status:     StatusPreAccepted,
		Seq:        3,
		Deps:       rn.q.deps(),
		Command:    optimizedTestCommand("pending-toq", "pending-toq-key"),
		TOQPending: true,
	})
	rn.instances[pendingRef] = &instance{rec: pending, phase: phasePreAccept}
	if err := rn.Step(Message{Type: MsgEvidence, From: 1, To: 2, Ref: pendingRef, ConflictRef: target, Ballot: Ballot{Number: 2, Replica: 1}}); err != nil {
		t.Fatal(err)
	}
	pendingResp := optimizedRequireMessage(t, rn.Ready().Messages, MsgEvidenceResp, 1)
	if pendingResp.RecordStatus != StatusNone || pendingResp.Seq != 0 || len(pendingResp.Command.Payload) != 0 {
		t.Fatalf("TOQ-pending evidence response exposed an unprocessed value: %#v", pendingResp)
	}
}

func TestEvidenceResponsesIgnoreStaleMismatchedAndDuplicateSenders(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 5)
	target := InstanceRef{Replica: 1, Instance: 30, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 7, Conf: 1}
	deps := rn.q.deps()
	deps[1] = conflict.Instance
	ballot := Ballot{Number: 1, Replica: 1}
	rn.instances[target] = &instance{rec: checkedRecord(InstanceRecord{Ref: target, Ballot: ballot, Status: StatusPreAccepted, Seq: 2, Deps: deps, Command: optimizedTestCommand("evidence-response-target", "evidence-response-key")}), phase: phaseTryPreAccept}
	key := tryEvidenceKey{target: target, conflict: conflict, ballot: ballot}
	records := make(map[ReplicaID]InstanceRecord)
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{key: records}

	mismatched := Message{Type: MsgEvidenceResp, From: 2, To: 1, Ref: InstanceRef{Replica: 3, Instance: 7, Conf: 1}, ConflictRef: target, Ballot: ballot, Deps: rn.q.deps(), RecordStatus: StatusNone}
	if err := rn.Step(mismatched); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("mismatched evidence response was recorded: %#v", records)
	}

	valid := Message{Type: MsgEvidenceResp, From: 2, To: 1, Ref: conflict, ConflictRef: target, Ballot: ballot, Deps: rn.q.deps(), RecordStatus: StatusNone}
	if err := rn.Step(valid); err != nil {
		t.Fatal(err)
	}
	if got := records[2]; got.Ref != conflict || got.Status != StatusNone {
		t.Fatalf("valid evidence response record = %#v, want none record for %s", got, conflict)
	}
	duplicate := valid.Clone()
	duplicate.Seq = 99
	duplicate.RecordStatus = StatusCommitted
	duplicate.RecordBallot = Ballot{Number: 1, Replica: 2}
	duplicate.Command = optimizedTestCommand("duplicate", "evidence-response-key")
	if err := rn.Step(duplicate); err != nil {
		t.Fatal(err)
	}
	if got := records[2]; got.Seq != 0 || got.Status != StatusNone {
		t.Fatalf("duplicate evidence response overwrote first sender record: %#v", got)
	}
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{key: records}
	records[3] = InstanceRecord{Ref: conflict, Status: StatusNone}
	rn.handleEvidenceResp(Message{From: 3, Ref: conflict, ConflictRef: target, Ballot: ballot, Seq: 123, RecordStatus: StatusCommitted})
	if got := records[3]; got.Seq != 0 || got.Status != StatusNone {
		t.Fatalf("direct duplicate evidence response overwrote first sender record: %#v", got)
	}
}

func TestEvidenceStaleDuplicateCommittedTupleFallsBackToSlowAccept(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	target := InstanceRef{Replica: 1, Instance: 31, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 8, Conf: 1}
	deps := rn.q.deps()
	deps[1] = conflict.Instance
	inst := &instance{
		rec: checkedRecord(InstanceRecord{
			Ref:     target,
			Ballot:  Ballot{Number: 1, Replica: 1},
			Status:  StatusPreAccepted,
			Seq:     2,
			Deps:    deps,
			Command: optimizedTestCommand("stale-duplicate-target", "stale-duplicate-key"),
		}),
		phase: phaseTryPreAccept,
		tryOK: testVoterMask(t, rn.q.conf, 1),
	}
	rn.instances[target] = inst
	key := tryEvidenceKey{target: target, conflict: conflict, ballot: inst.rec.Ballot}
	records := make(map[ReplicaID]InstanceRecord)
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{key: records}

	tupleDeps := rn.q.deps()
	tupleDeps[1] = conflict.Instance
	evidenceDeps := rn.q.deps()
	evidenceDeps[0] = target.Instance
	oldBallot := Message{
		Type:           MsgEvidenceResp,
		From:           2,
		To:             1,
		Ref:            conflict,
		ConflictRef:    target,
		Ballot:         Ballot{Replica: inst.rec.Ballot.Replica},
		RecordBallot:   Ballot{Number: 1, Replica: 2},
		RecordStatus:   StatusCommitted,
		Seq:            4,
		Deps:           tupleDeps,
		Command:        optimizedTestCommand("old-ballot-conflict", "stale-duplicate-conflict-key"),
		AcceptSeq:      5,
		AcceptDeps:     evidenceDeps,
		AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 5, Deps: evidenceDeps}},
	}
	oldKey := tryEvidenceKey{target: target, conflict: conflict, ballot: oldBallot.Ballot}
	oldRecords := make(map[ReplicaID]InstanceRecord)
	rn.tryEvidenceChecks[oldKey] = oldRecords
	if err := rn.Step(oldBallot); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 || len(oldRecords) != 0 || inst.phase != phaseTryPreAccept {
		t.Fatalf("old-ballot evidence response affected live check: current=%#v old=%#v phase=%d", records, oldRecords, inst.phase)
	}
	if _, ok := rn.tryEvidenceChecks[oldKey]; ok {
		t.Fatalf("old-ballot evidence check was not deleted: %#v", rn.tryEvidenceChecks)
	}
	if _, ok := rn.tryEvidenceChecks[key]; !ok {
		t.Fatalf("old-ballot evidence response deleted current check: %#v", rn.tryEvidenceChecks)
	}

	stale := Message{Type: MsgEvidenceResp, From: 2, To: 1, Ref: conflict, ConflictRef: target, Ballot: inst.rec.Ballot, Deps: rn.q.deps(), RecordStatus: StatusNone}
	if err := rn.Step(stale); err != nil {
		t.Fatal(err)
	}

	fresh := stale.Clone()
	fresh.RecordBallot = oldBallot.RecordBallot
	fresh.RecordStatus = StatusCommitted
	fresh.Seq = oldBallot.Seq
	fresh.Deps = tupleDeps
	fresh.Command = optimizedTestCommand("stale-duplicate-conflict", "stale-duplicate-conflict-key")
	fresh.AcceptSeq = oldBallot.AcceptSeq
	fresh.AcceptDeps = evidenceDeps
	fresh.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 5, Deps: evidenceDeps}}
	if err := rn.Step(fresh); err != nil {
		t.Fatal(err)
	}
	if got := records[2]; got.Status != StatusNone || got.Seq != 0 {
		t.Fatalf("stale duplicate evidence response overwrote first sender record: %#v", got)
	}

	empty := Message{Type: MsgEvidenceResp, From: 3, To: 1, Ref: conflict, ConflictRef: target, Ballot: inst.rec.Ballot, Deps: rn.q.deps(), RecordStatus: StatusNone}
	if err := rn.Step(empty); err != nil {
		t.Fatal(err)
	}
	if got := records[2]; got.Status != StatusNone || got.Seq != 0 {
		t.Fatalf("resolved stale duplicate evidence response overwrote first sender record: %#v", got)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("stale duplicate evidence decision phase/status = %d/%s, want slow accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	if inst.rec.Deps[1] != conflict.Instance {
		t.Fatalf("accepted target deps = %v, want dependency on stale conflict %s", inst.rec.Deps, conflict)
	}
	if len(inst.tryIgnored) != 0 {
		t.Fatalf("stale duplicate evidence authorized ignore resend: %#v", inst.tryIgnored)
	}

	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, target)
	if rec.Status != StatusAccepted || rec.Deps[1] != conflict.Instance {
		t.Fatalf("stale duplicate evidence accepted record = %#v, want accepted record retaining conflict %s", rec, conflict)
	}
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
		if msg.From != 1 || msg.Ref != target || msg.Ballot != rec.Ballot ||
			msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
			msg.Seq != rec.Seq || !instanceNumsEqual(msg.Deps, rec.Deps) ||
			!commandEqual(msg.Command, rec.Command) {
			t.Fatalf("stale duplicate evidence Accept to %d = %#v, want canonical tuple %#v", to, msg, rec)
		}
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAccept)
	if rn.tryEvidenceChecks != nil {
		t.Fatalf("stale duplicate evidence left stale checks: %#v", rn.tryEvidenceChecks)
	}
}

func TestPendingEvidenceTimeoutFallsBackToSlowAccept(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	target := InstanceRef{Replica: 1, Instance: 40, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 6, Conf: 1}
	deps := rn.q.deps()
	deps[1] = conflict.Instance
	inst := &instance{
		rec: checkedRecord(InstanceRecord{
			Ref:     target,
			Ballot:  Ballot{Number: 1, Replica: 1},
			Status:  StatusPreAccepted,
			Seq:     2,
			Deps:    deps,
			Command: optimizedTestCommand("timeout-target", "timeout-key"),
		}),
		phase: phaseTryPreAccept,
		tryOK: testVoterMask(t, rn.q.conf, 1),
	}
	rn.instances[target] = inst
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{{target: target, conflict: conflict, ballot: inst.rec.Ballot}: {}}

	if err := rn.onTimer(inst, timerTryPreAccept); err != nil {
		panic(err)
	}
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("pending evidence timeout phase/status = %d/%s, want slow accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, target)
	if rec.Status != StatusAccepted || rec.FastPathEligible || rec.Deps[1] != conflict.Instance {
		t.Fatalf("timeout fallback record = %#v, want non-fast accept retaining conflict dependency %s", rec, conflict)
	}
	msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
	if msg.From != 1 || msg.Ref != target || msg.Ballot != rec.Ballot ||
		msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
		msg.Seq != rec.Seq || !instanceNumsEqual(msg.Deps, rec.Deps) ||
		!commandEqual(msg.Command, rec.Command) {
		t.Fatalf("timeout fallback Accept = %#v, want canonical tuple %#v", msg, rec)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAccept)
	if rn.tryEvidenceChecks != nil {
		t.Fatalf("timeout fallback left stale evidence checks: %#v", rn.tryEvidenceChecks)
	}
}

func TestTryEvidenceHelperBranchesFailClosedConservatively(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	target := InstanceRef{Replica: 1, Instance: 50, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 8, Conf: 1}
	deps := rn.q.deps()
	deps[1] = conflict.Instance
	inst := &instance{rec: checkedRecord(InstanceRecord{Ref: target, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusPreAccepted, Seq: 2, Deps: deps, Command: optimizedTestCommand("helper-target", "helper-key")}), phase: phaseTryPreAccept}
	rn.instances[target] = inst

	if rn.maybeStartTryEvidenceCheck(nil, conflict) {
		t.Fatal("nil recovery instance started evidence check")
	}
	if rn.maybeStartTryEvidenceCheck(inst, InstanceRef{}) {
		t.Fatal("zero conflict started evidence check")
	}
	if rn.maybeStartTryEvidenceCheck(inst, InstanceRef{Replica: 2, Instance: 8, Conf: 2}) {
		t.Fatal("cross-configuration conflict started evidence check")
	}
	noDep := *inst
	noDep.rec.Deps = rn.q.deps()
	if rn.maybeStartTryEvidenceCheck(&noDep, conflict) {
		t.Fatal("conflict outside candidate deps started evidence check")
	}
	inst.tryIgnored = map[InstanceRef]struct{}{conflict: {}}
	if rn.maybeStartTryEvidenceCheck(inst, conflict) {
		t.Fatal("candidate with existing ignore marker started nested evidence check")
	}
	inst.tryIgnored = nil

	single := protocolCoverageNewRawNode(t, 1, 1)
	singleRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	singleInst := &instance{rec: checkedRecord(InstanceRecord{Ref: singleRef, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusPreAccepted, Seq: 1, Deps: []InstanceNum{1}, Command: optimizedTestCommand("single", "helper-key")}), phase: phaseTryPreAccept}
	if single.maybeStartTryEvidenceCheck(singleInst, singleRef) {
		t.Fatal("single-voter configuration with f=0 started evidence check")
	}

	key := tryEvidenceKey{target: target, conflict: conflict, ballot: inst.rec.Ballot}
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{key: {}}
	if !rn.maybeStartTryEvidenceCheck(inst, conflict) {
		t.Fatal("existing evidence check was not treated as in-flight")
	}

	rn.resolveTryEvidenceCheck(tryEvidenceKey{target: target, conflict: InstanceRef{Replica: 3, Instance: 9, Conf: 1}})
	invalidTarget := InstanceRef{Replica: 1, Instance: 51, Conf: 1}
	invalidKey := tryEvidenceKey{target: invalidTarget, conflict: conflict}
	rn.tryEvidenceChecks[invalidKey] = map[ReplicaID]InstanceRecord{}
	rn.resolveTryEvidenceCheck(invalidKey)
	if _, ok := rn.tryEvidenceChecks[invalidKey]; ok {
		t.Fatalf("evidence check for missing target was not dropped: %#v", rn.tryEvidenceChecks)
	}

	failClosedTarget := InstanceRef{Replica: 1, Instance: 52, Conf: 1}
	failDeps := rn.q.deps()
	failDeps[1] = conflict.Instance
	failInst := &instance{rec: checkedRecord(InstanceRecord{Ref: failClosedTarget, Ballot: Ballot{Number: 2, Replica: 1}, Status: StatusPreAccepted, Seq: 2, Deps: failDeps, Command: optimizedTestCommand("fail-closed", "helper-key")}), phase: phaseTryPreAccept}
	rn.instances[failClosedTarget] = failInst
	failKey := tryEvidenceKey{target: failClosedTarget, conflict: conflict, ballot: failInst.rec.Ballot}
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{failKey: {2: {Ref: conflict, Status: StatusNone}, 3: {Ref: conflict, Status: StatusNone}}}
	rn.resolveTryEvidenceCheck(failKey)
	if failInst.phase != phaseAccept || failInst.rec.Status != StatusAccepted {
		t.Fatalf("all-remote no-tuple evidence decision phase/status = %d/%s, want fail-closed slow accept", failInst.phase, failInst.rec.Status)
	}
}

func TestTryEvidenceDecisionAndRecordValidationBranches(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 5)
	target := InstanceRef{Replica: 1, Instance: 60, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 10, Conf: 1}
	targetDeps := rn.q.deps()
	targetDeps[1] = conflict.Instance
	inst := &instance{rec: checkedRecord(InstanceRecord{Ref: target, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusPreAccepted, Seq: 2, Deps: targetDeps, Command: optimizedTestCommand("decision-target", "decision-key")}), phase: phaseTryPreAccept}
	rn.instances[target] = inst
	key := tryEvidenceKey{target: target, conflict: conflict, ballot: inst.rec.Ballot}
	if authorized, failClosed := rn.tryEvidenceDecision(inst, key, map[ReplicaID]InstanceRecord{2: {Ref: conflict, Status: StatusNone}}); authorized || failClosed {
		t.Fatalf("partial evidence without committed tuple decision = authorized %v failClosed %v, want wait", authorized, failClosed)
	}
	allNone := map[ReplicaID]InstanceRecord{2: {Ref: conflict, Status: StatusNone}, 3: {Ref: conflict, Status: StatusNone}, 4: {Ref: conflict, Status: StatusNone}, 5: {Ref: conflict, Status: StatusNone}}
	if authorized, failClosed := rn.tryEvidenceDecision(inst, key, allNone); authorized || !failClosed {
		t.Fatalf("all-remote evidence without committed tuple decision = authorized %v failClosed %v, want fail closed", authorized, failClosed)
	}
	if _, ok := rn.committedConflictTuple(conflict, allNone); ok {
		t.Fatal("committedConflictTuple found a value in all-none evidence")
	}

	tupleDeps := rn.q.deps()
	tuple := checkedRecord(InstanceRecord{Ref: conflict, Ballot: Ballot{Number: 1, Replica: 2}, RecordBallot: Ballot{Number: 1, Replica: 2}, Status: StatusCommitted, Seq: 3, Deps: tupleDeps, Command: optimizedTestCommand("decision-conflict", "decision-key")})
	rn.instances[conflict] = &instance{rec: tuple, phase: phaseCommitted}
	if got, ok := rn.committedConflictTuple(conflict, nil); !ok || got.Ref != conflict || got.Seq != tuple.Seq {
		t.Fatalf("committedConflictTuple local record = %#v ok=%v, want %s", got, ok, conflict)
	}
	coveringDeps := rn.q.deps()
	coveringDeps[0] = target.Instance
	usable := tuple.Clone()
	usable.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 4, Deps: coveringDeps}}
	usableFrom3 := tuple.Clone()
	usableFrom3.AcceptEvidence = []AcceptEvidence{{Sender: 3, Seq: 4, Deps: coveringDeps}}
	if authorized, failClosed := rn.tryEvidenceDecision(inst, key, map[ReplicaID]InstanceRecord{1: {Ref: InstanceRef{Replica: 9, Instance: 9, Conf: 1}, Status: StatusCommitted}, 2: usable, 3: usableFrom3}); !authorized || failClosed {
		t.Fatalf("decision with self response plus f usable records = authorized %v failClosed %v, want authorized", authorized, failClosed)
	}

	rnFive := protocolCoverageNewRawNode(t, 1, 5)
	instFive := &instance{rec: inst.rec, phase: phaseTryPreAccept}
	rnFive.instances[conflict] = &instance{rec: tuple, phase: phaseCommitted}
	oneUsable := tuple.Clone()
	oneUsable.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 4, Deps: coveringDeps}}
	if authorized, failClosed := rnFive.tryEvidenceDecision(instFive, key, map[ReplicaID]InstanceRecord{2: oneUsable, 3: {Ref: conflict, Status: StatusNone}, 4: {Ref: conflict, Status: StatusNone}, 5: {Ref: conflict, Status: StatusNone}}); authorized || !failClosed {
		t.Fatalf("all-remote evidence below f decision = authorized %v failClosed %v, want fail closed", authorized, failClosed)
	}

	inst.prepareOK = testRecordVoteSet(t, rn.q.conf, map[ReplicaID]InstanceRecord{
		4: {Ref: target, Status: StatusPreAccepted, FastPathEligible: false},
		5: {Ref: target, Status: StatusPreAccepted, FastPathEligible: false},
	})

	for _, tc := range []struct {
		name           string
		rec            InstanceRecord
		wantFailClosed bool
		wantUsable     bool
	}{
		{name: "wrong ref", rec: InstanceRecord{Ref: target, Status: StatusCommitted}, wantFailClosed: true},
		{name: "none", rec: InstanceRecord{Ref: conflict, Status: StatusNone}},
		{name: "missing record ballot", rec: InstanceRecord{Ref: conflict, Status: StatusCommitted, Seq: tuple.Seq, Deps: tuple.Deps, Command: tuple.Command}, wantFailClosed: true},
		{name: "different value tuple", rec: checkedRecord(InstanceRecord{Ref: conflict, Ballot: tuple.Ballot, RecordBallot: tuple.RecordBallot, Status: StatusCommitted, Seq: tuple.Seq + 1, Deps: tuple.Deps, Command: tuple.Command}), wantFailClosed: true},
		{name: "no sender evidence", rec: checkedRecord(InstanceRecord{Ref: conflict, Ballot: tuple.Ballot, RecordBallot: tuple.RecordBallot, Status: StatusCommitted, Seq: tuple.Seq, Deps: tuple.Deps, Command: tuple.Command}), wantFailClosed: true},
		{name: "malformed sender evidence", rec: func() InstanceRecord {
			rec := tuple.Clone()
			rec.AcceptEvidence = []AcceptEvidence{{Sender: 0, Seq: 4, Deps: coveringDeps}}
			return rec
		}(), wantFailClosed: true},
		{name: "duplicate conflicting sender evidence", rec: func() InstanceRecord {
			rec := tuple.Clone()
			rec.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 4, Deps: coveringDeps}, {Sender: 2, Seq: 5, Deps: coveringDeps}}
			return rec
		}(), wantFailClosed: true},
		{name: "duplicate identical sender evidence", rec: func() InstanceRecord {
			rec := tuple.Clone()
			rec.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 4, Deps: coveringDeps}, {Sender: 2, Seq: 4, Deps: coveringDeps}}
			return rec
		}(), wantUsable: true},
		{name: "required sender evidence misses target", rec: func() InstanceRecord {
			rec := tuple.Clone()
			rec.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 4, Deps: rn.q.deps()}}
			return rec
		}(), wantFailClosed: true},
		{name: "usable sender evidence covers target", rec: usable, wantUsable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failClosed, usable := rn.tryEvidenceRecordDecision(inst, tuple, tc.rec)
			if failClosed != tc.wantFailClosed || usable != tc.wantUsable {
				t.Fatalf("tryEvidenceRecordDecision = failClosed %v usable %v, want %v/%v for %#v", failClosed, usable, tc.wantFailClosed, tc.wantUsable, tc.rec)
			}
		})
	}
}

func TestAttrsWithConflictDependencySeqFloorsWrappedMaxConflictSeq(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	conflict := InstanceRef{Replica: 2, Instance: 11, Conf: 1}
	attrs := Attributes{Seq: 0, Deps: rn.q.deps()}

	got := rn.attrsWithConflictDependencySeq(attrs, conflict, ^uint64(0), 1)

	if got.Seq != 1 {
		t.Fatalf("attrsWithConflictDependencySeq Seq = %d, want fail-safe floor 1 after max conflict sequence wraps", got.Seq)
	}
	if len(got.Deps) != len(attrs.Deps) {
		t.Fatalf("attrsWithConflictDependencySeq Deps len = %d, want %d", len(got.Deps), len(attrs.Deps))
	}
	conf := rn.confFor(conflict.Conf)
	idx, ok := conf.Index(conflict.Replica)
	if !ok {
		t.Fatalf("test conflict replica %d is not a voter in config %d", conflict.Replica, conflict.Conf)
	}
	if got.Deps[idx] != conflict.Instance {
		t.Fatalf("attrsWithConflictDependencySeq dependency at index %d = %d, want conflict instance %d", idx, got.Deps[idx], conflict.Instance)
	}
}

func TestEvidenceDependencyAndBroadcastHelperBranches(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	target := InstanceRef{Replica: 1, Instance: 70, Conf: 1}
	conflict := InstanceRef{Replica: 2, Instance: 11, Conf: 1}
	attrs := Attributes{Seq: 0, Deps: []InstanceNum{0}}
	unchanged := rn.attrsWithConflictDependencySeq(attrs, InstanceRef{Replica: 9, Instance: 1, Conf: 1}, 0, 1)
	if unchanged.Seq != 0 || len(unchanged.Deps) != 1 {
		t.Fatalf("unknown conflict replica changed attrs: %#v", unchanged)
	}
	expanded := rn.attrsWithConflictDependencySeq(attrs, conflict, 0, 1)
	if expanded.Seq != 1 || len(expanded.Deps) != 3 || expanded.Deps[1] != conflict.Instance {
		t.Fatalf("attrsWithConflictDependencySeq expanded = %#v, want seq floor and conflict dependency", expanded)
	}

	if rn.recordAcceptEvidenceDependsOn(InstanceRecord{Ref: conflict, AcceptEvidence: []AcceptEvidence{{Sender: 0, Seq: 9, Deps: []InstanceNum{target.Instance, 0, 0}}}}, target) {
		t.Fatal("zero-sender AcceptEvidence was allowed to prove dependency")
	}
	covering := []InstanceNum{target.Instance, 0, 0}
	if !rn.recordAcceptEvidenceDependsOn(InstanceRecord{Ref: conflict, AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 9, Deps: covering}, {Sender: 2, Seq: 9, Deps: covering}}}, target) {
		t.Fatal("duplicate identical sender evidence did not prove dependency")
	}
	if rn.recordAcceptEvidenceDependsOn(InstanceRecord{Ref: conflict, AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 9, Deps: covering}, {Sender: 2, Seq: 10, Deps: covering}}}, target) {
		t.Fatal("duplicate conflicting sender evidence proved dependency")
	}

	rec := InstanceRecord{}
	if mergeOwnAcceptEvidence(&rec, 0, Attributes{Seq: 1, Deps: rn.q.deps()}) {
		t.Fatal("zero sender merged own AcceptEvidence")
	}
	if mergeOwnAcceptEvidence(&rec, 2, Attributes{Seq: 0, Deps: rn.q.deps()}) {
		t.Fatal("zero seq merged own AcceptEvidence")
	}
	if mergeOwnAcceptEvidence(&rec, 2, Attributes{Seq: 1}) {
		t.Fatal("empty deps merged own AcceptEvidence")
	}

	if rn.tryConflictForcesSlowAccept(nil, conflict) {
		t.Fatal("nil TryPreAccept instance forced slow accept")
	}
	if rn.tryConflictForcesSlowAccept(&instance{}, InstanceRef{}) {
		t.Fatal("zero conflict forced slow accept")
	}
	if rn.leaderMustBeInCandidateFastQuorum(nil, 1) {
		t.Fatal("nil instance required a fast quorum leader")
	}
	inst := &instance{rec: checkedRecord(InstanceRecord{Ref: target, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("leader-helper", "leader-helper-key")}), phase: phaseTryPreAccept}
	if rn.leaderMustBeInCandidateFastQuorum(inst, 9) {
		t.Fatal("non-voter leader was required in candidate fast quorum")
	}
	rn.confHistory[9] = ConfState{ID: 9, Voters: []ReplicaID{1, 1}}
	if rn.faultTolerance(9) != 0 {
		t.Fatal("invalid historical configuration reported positive fault tolerance")
	}
	badConfInst := &instance{rec: InstanceRecord{Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 9}}}
	if rn.leaderMustBeInCandidateFastQuorum(badConfInst, 1) {
		t.Fatal("invalid historical configuration required a fast quorum leader")
	}

	inst.tryIgnored = map[InstanceRef]struct{}{
		{Replica: 3, Instance: 1, Conf: 1}: {},
		{Replica: 2, Instance: 9, Conf: 1}: {},
	}
	rn.broadcastTryPreAccept(inst)
	rd := rn.Ready()
	first := optimizedRequireMessage(t, rd.Messages, MsgTryPreAccept, 2)
	if first.From != 1 || first.Ref != target || first.Ballot != inst.rec.Ballot ||
		first.RecordBallot != (Ballot{}) || first.RecordStatus != StatusNone ||
		first.Seq != inst.rec.Seq || !instanceNumsEqual(first.Deps, inst.rec.Deps) ||
		!commandEqual(first.Command, inst.rec.Command) || first.FastPathEligible ||
		first.IgnoreDependency.Ref != (InstanceRef{Replica: 2, Instance: 9, Conf: 1}) {
		t.Fatalf("first ignored dependency broadcast = %#v, want canonical tuple with refs sorted by replica", first)
	}
	secondSeen := false
	for _, msg := range rd.Messages {
		if msg.Type == MsgTryPreAccept && msg.IgnoreDependency.Ref == (InstanceRef{Replica: 3, Instance: 1, Conf: 1}) {
			if msg.From != 1 || msg.Ref != target || msg.Ballot != inst.rec.Ballot ||
				msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
				msg.Seq != inst.rec.Seq || !instanceNumsEqual(msg.Deps, inst.rec.Deps) ||
				!commandEqual(msg.Command, inst.rec.Command) || msg.FastPathEligible {
				t.Fatalf("second ignored dependency broadcast = %#v, want canonical tuple %#v", msg, inst.rec)
			}
			secondSeen = true
		}
	}
	if !secondSeen {
		t.Fatalf("broadcastTryPreAccept did not send the second ignore marker: %#v", rd.Messages)
	}
}

func TestFailPendingEvidenceCheckSortsAndDropsByTarget(t *testing.T) {
	run := func(t *testing.T, conflicts []InstanceRef, want InstanceRef) {
		t.Helper()
		rn := protocolCoverageNewRawNode(t, 1, 3)
		target := InstanceRef{Replica: 1, Instance: 80, Conf: 1}
		inst := &instance{rec: checkedRecord(InstanceRecord{Ref: target, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusPreAccepted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("fail-pending", "fail-pending-key")}), phase: phaseTryPreAccept}
		rn.instances[target] = inst
		staleBallotKey := tryEvidenceKey{target: target, conflict: conflicts[0], ballot: Ballot{Number: inst.rec.Ballot.Number + 1, Replica: inst.rec.Ballot.Replica}}
		rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{staleBallotKey: {}}
		if rn.failPendingTryEvidenceCheck(inst) {
			t.Fatal("stale-ballot evidence check for target failed this recovery")
		}
		if rn.tryEvidenceChecks != nil {
			t.Fatalf("stale-ballot evidence check was not cleaned up: %#v", rn.tryEvidenceChecks)
		}
		otherTarget := InstanceRef{Replica: 1, Instance: 81, Conf: 1}
		rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{{target: otherTarget, conflict: conflicts[0], ballot: inst.rec.Ballot}: {}}
		if rn.failPendingTryEvidenceCheck(inst) {
			t.Fatal("evidence checks for another target failed this recovery")
		}
		for _, conflict := range conflicts {
			rn.tryEvidenceChecks[tryEvidenceKey{target: target, conflict: conflict, ballot: inst.rec.Ballot}] = map[ReplicaID]InstanceRecord{}
		}
		if !rn.failPendingTryEvidenceCheck(inst) {
			t.Fatal("pending evidence checks for target did not fail closed")
		}
		idx, ok := rn.q.conf.Index(want.Replica)
		if !ok {
			t.Fatalf("wanted conflict replica %d is not in the test config", want.Replica)
		}
		if inst.phase != phaseAccept || inst.rec.Deps[idx] != want.Instance {
			t.Fatalf("failed pending evidence picked record %#v, want dependency on lowest conflict %s", inst.rec, want)
		}
	}

	run(t, []InstanceRef{{Replica: 3, Instance: 1, Conf: 1}, {Replica: 2, Instance: 5, Conf: 1}, {Replica: 2, Instance: 4, Conf: 1}}, InstanceRef{Replica: 2, Instance: 4, Conf: 1})
	run(t, []InstanceRef{{Replica: 2, Instance: 4, Conf: 2}, {Replica: 2, Instance: 4, Conf: 1}}, InstanceRef{Replica: 2, Instance: 4, Conf: 1})

	rn := protocolCoverageNewRawNode(t, 1, 3)
	target := InstanceRef{Replica: 1, Instance: 82, Conf: 1}
	keep := tryEvidenceKey{target: InstanceRef{Replica: 1, Instance: 83, Conf: 1}, conflict: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, ballot: Ballot{Number: 1, Replica: 1}}
	drop := tryEvidenceKey{target: target, conflict: InstanceRef{Replica: 2, Instance: 2, Conf: 1}, ballot: Ballot{Number: 1, Replica: 1}}
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{keep: {}, drop: {}}
	rn.dropTryEvidenceChecksForTarget(target)
	if _, ok := rn.tryEvidenceChecks[drop]; ok {
		t.Fatalf("dropTryEvidenceChecksForTarget left target check: %#v", rn.tryEvidenceChecks)
	}
	if _, ok := rn.tryEvidenceChecks[keep]; !ok {
		t.Fatalf("dropTryEvidenceChecksForTarget removed unrelated check: %#v", rn.tryEvidenceChecks)
	}
}

func TestCommitAndPrepareResponsesWithoutRecordBallotFailClosed(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	ref := InstanceRef{Replica: 2, Instance: 90, Conf: 1}
	msg := Message{Type: MsgCommit, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("zero-record-ballot", "zero-record-ballot-key")}
	if err := rn.Step(msg); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("commit without RecordBallot error = %v, want ErrInvalidMessage", err)
	}
	if rn.HasReady() || rn.instances[ref] != nil {
		t.Fatalf("commit without RecordBallot changed state: ready=%v instance=%#v", rn.HasReady(), rn.instances[ref])
	}

	recoveryRef := InstanceRef{Replica: 1, Instance: 91, Conf: 1}
	recovery := &instance{rec: checkedRecord(InstanceRecord{Ref: recoveryRef, Ballot: Ballot{Number: 2, Replica: 1}, Status: StatusNone, Deps: rn.q.deps()}), phase: phasePrepare, prepareOK: new(recordVoteSet)}
	rn.instances[recoveryRef] = recovery
	before := recovery.rec.Clone()
	resp := Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: recoveryRef, Ballot: recovery.rec.Ballot, RecordStatus: StatusCommitted, Seq: 2, Deps: rn.q.deps(), Command: optimizedTestCommand("prepare-zero-record-ballot", "zero-record-ballot-key")}
	if err := rn.Step(resp); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("prepare response without RecordBallot error = %v, want ErrInvalidMessage", err)
	}
	if recovery.prepareOK.len() != 0 || recovery.phase != phasePrepare ||
		!instanceRecordEqual(recovery.rec, before) || rn.HasReady() {
		t.Fatalf("prepare response without RecordBallot changed state: phase=%d record=%#v prepareOK=%#v ready=%v", recovery.phase, recovery.rec, recovery.prepareOK, rn.HasReady())
	}
}

func protocolMalformedEvidenceFrame(evidence, evidenceDeps, conflictKeys uint64) []byte {
	buf := append([]byte(nil), wireMagic[:]...)
	for _, v := range []uint64{
		uint64(MsgEvidenceResp),
		2,
		1,
		2,
		1,
		1,
		0,
	} {
		buf = binary.AppendUvarint(buf, v)
	}
	buf = append(buf, 0)
	for _, v := range []uint64{
		0,
		1,
		2,
		0,
		1,
		2,
		3,
		3,
		0,
		0,
		0,
		0,
		0,
		evidence,
	} {
		buf = binary.AppendUvarint(buf, v)
	}
	if evidence <= maxWireAcceptEvidence {
		for range evidence {
			for _, v := range []uint64{2, 4, evidenceDeps} {
				buf = binary.AppendUvarint(buf, v)
			}
			if evidenceDeps <= maxWireDeps {
				for range evidenceDeps {
					buf = binary.AppendUvarint(buf, 0)
				}
			}
		}
		for _, v := range []uint64{
			0,
			0,
			0,
			100,
			1,
			uint64(CommandUser),
			0,
			conflictKeys,
		} {
			buf = binary.AppendUvarint(buf, v)
		}
		if conflictKeys <= maxWireConflictKeys {
			for range conflictKeys {
				buf = binary.AppendUvarint(buf, 0)
			}
			buf = append(buf, 0)
			for _, v := range []uint64{
				0,
				0,
				0,
				0,
				1,
				1,
				1,
				uint64(StatusCommitted),
			} {
				buf = binary.AppendUvarint(buf, v)
			}
			buf = append(buf, 0)
			buf = binary.AppendUvarint(buf, 0)
			buf = binary.AppendUvarint(buf, uint64(StatusCommitted))
		}
	}
	return append(buf, make([]byte, 32)...)
}

func protocolTryInstance(t *testing.T, voters int, ballotNumber uint64) (*RawNode, *instance, InstanceRef) {
	t.Helper()
	rn := protocolCoverageNewRawNode(t, 1, voters)
	ref := InstanceRef{Replica: 1, Instance: InstanceNum(20 + ballotNumber), Conf: 1}
	rec := checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Number: ballotNumber, Replica: 1},
		Status:  StatusPreAccepted,
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("try-response", "try-response-key"),
	})
	inst := &instance{rec: rec, phase: phaseTryPreAccept}
	rn.instances[ref] = inst
	return rn, inst, ref
}

func TestPrepareRecoveryPreservesExecutedChosenTuple(t *testing.T) {
	ref := InstanceRef{Replica: 2, Instance: 91, Conf: 1}
	rn, inst, ballot := optimizedStartRecovery(t, 3, ref)
	cmd := optimizedTestCommand("executed-prepare-value", "executed-prepare-key")
	valueBallot := Ballot{Replica: ref.Replica}
	if err := rn.Step(Message{
		Type:         MsgPrepareResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       ballot,
		RecordBallot: valueBallot,
		RecordStatus: StatusExecuted,
		Seq:          1,
		Deps:         rn.q.deps(),
		Command:      cmd,
	}); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseCommitted || !commandEqual(inst.rec.Command, cmd) {
		t.Fatalf("executed prepare evidence recovered phase/record = %d/%#v, want committed original value", inst.phase, inst.rec)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Status != StatusCommitted || !commandEqual(rec.Command, cmd) {
		t.Fatalf("executed prepare evidence durable record = %#v, want committed original value", rec)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		if msg.From != 1 || msg.Ref != ref || msg.Ballot != rec.Ballot ||
			msg.RecordBallot == (Ballot{}) || msg.RecordBallot != rec.RecordBallot ||
			msg.RecordStatus != StatusNone || msg.Seq != rec.Seq ||
			!instanceNumsEqual(msg.Deps, rec.Deps) || !commandEqual(msg.Command, rec.Command) {
			t.Fatalf("executed prepare evidence Commit to %d = %#v, want canonical tuple %#v", to, msg, rec)
		}
	}
}

func TestReorderedPreAcceptDoesNotDowngradeAcceptedRecord(t *testing.T) {
	for _, typ := range []MessageType{MsgPreAccept, MsgTryPreAccept} {
		t.Run(typ.String(), func(t *testing.T) {
			rn := protocolCoverageNewRawNode(t, 2, 3)
			ref := InstanceRef{Replica: 1, Instance: 92, Conf: 1}
			cmd := optimizedTestCommand("accepted-before-preaccept", "accepted-before-preaccept-key")
			ballot := Ballot{Replica: ref.Replica}
			if typ == MsgTryPreAccept {
				ballot.Number = 1
			}
			optimizedSeedRecord(t, rn, InstanceRecord{
				Ref:          ref,
				Ballot:       ballot,
				RecordBallot: ballot,
				Status:       StatusAccepted,
				Seq:          1,
				Deps:         rn.q.deps(),
				Command:      cmd,
			})
			if err := rn.Step(Message{
				Type:    typ,
				From:    1,
				To:      2,
				Ref:     ref,
				Ballot:  ballot,
				Seq:     1,
				Deps:    rn.q.deps(),
				Command: cmd,
			}); err != nil {
				t.Fatal(err)
			}
			got := rn.instances[ref]
			if got == nil || got.rec.Status != StatusAccepted || got.rec.RecordBallot != ballot || !commandEqual(got.rec.Command, cmd) {
				t.Fatalf("reordered %s downgraded accepted record: %#v", typ, got)
			}
			for _, rec := range rn.Ready().Records {
				if rec.Ref == ref && rec.Status < StatusAccepted {
					t.Fatalf("reordered %s emitted downgraded durable record %#v", typ, rec)
				}
			}
		})
	}
}

func TestNormalFastPathAdoptsCoveringRemoteTupleAtPaperQuorum(t *testing.T) {
	for _, voters := range []int{3, 5, 7} {
		t.Run(string(rune('0'+voters))+"-voters", func(t *testing.T) { //nolint:gosec // G115: conversion is bounded by protocol or test-fixture limits.
			rn := protocolCoverageNewRawNode(t, 1, voters)
			cmd := optimizedTestCommand("covering-normal-fast", "covering-normal-fast-key")
			ref, err := rn.Propose(cmd)
			if err != nil {
				t.Fatal(err)
			}
			advanceOK(t, rn, rn.Ready())
			inst := rn.instances[ref]
			if inst == nil {
				t.Fatalf("missing proposed instance %s", ref)
			}
			dependency := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
			optimizedSeedRecord(t, rn, InstanceRecord{
				Ref:          dependency,
				Ballot:       Ballot{Replica: dependency.Replica},
				RecordBallot: Ballot{Replica: dependency.Replica},
				Status:       StatusExecuted,
				Seq:          1,
				Deps:         rn.q.deps(),
				Command:      optimizedTestCommand("covering-normal-dependency", "covering-normal-fast-key"),
			})
			rn.executed.add(dependency)
			covering := inst.rec.Attributes()
			covering.Seq++
			covering.Deps[1] = dependency.Instance
			neededRemote := rn.fastQuorumForConf(ref.Conf) - 1
			for id := ReplicaID(2); id < ReplicaID(2+neededRemote); id++ { //nolint:gosec // G115: test harness converts bounded int index/count
				if err := rn.Step(Message{Type: MsgPreAcceptResp, From: id,
					To:     1,
					Ref:    ref,
					Ballot: inst.rec.Ballot,
					Seq:    covering.Seq,
					Deps:   append([]InstanceNum(nil), covering.Deps...), FastPathEligible: true,
					DepsCommitted: dependencyMask(covering.Deps)}); err != nil {
					t.Fatal(err)
				}
			}
			if inst.phase != phaseCommitted || inst.rec.Status < StatusCommitted || !inst.rec.Attributes().Equal(covering) {
				t.Fatalf("%d-voter covering fast path phase/record = %d/%#v, want committed attrs %#v", voters, inst.phase, inst.rec, covering)
			}
			rd := rn.Ready()
			optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
			if rec := optimizedRequireRecord(t, rd, ref); rec.Status != StatusCommitted || !rec.Attributes().Equal(covering) {
				t.Fatalf("%d-voter covering fast-path record = %#v, want committed %#v", voters, rec, covering)
			}
		})
	}
}

func TestRecoveryRecomputesAttrsForUnsafePreAcceptedCandidate(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	target := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	conflict := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	key := "recovery-recompute-key"
	targetCommand := optimizedTestCommand("recovery-recompute-target", key)
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:          conflict,
		Ballot:       Ballot{Replica: conflict.Replica},
		RecordBallot: Ballot{Replica: conflict.Replica},
		Status:       StatusCommitted,
		Seq:          2,
		Deps:         rn.q.deps(),
		Command:      optimizedTestCommand("recovery-recompute-conflict", key),
	})

	rec := InstanceRecord{Ref: target, Status: StatusNone, Deps: rn.q.deps()}
	rec.Checksum = ChecksumRecord(rec)
	inst := &instance{rec: rec, phase: phaseIdle}
	rn.instances[target] = inst
	if err := rn.startPrepare(inst); err != nil {
		panic(err)
	}
	if err := rn.Step(Message{
		Type:             MsgPrepareResp,
		From:             2,
		To:               1,
		Ref:              target,
		Ballot:           inst.rec.Ballot,
		RecordBallot:     Ballot{Replica: target.Replica},
		Seq:              1,
		Deps:             rn.q.deps(),
		Command:          targetCommand,
		RecordStatus:     StatusPreAccepted,
		FastPathEligible: false,
	}); err != nil {
		t.Fatal(err)
	}

	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("recovery phase/record = %d/%#v, want direct slow Accept after recomputing unsafe PreAccepted attrs", inst.phase, inst.rec)
	}
	if !commandEqual(inst.rec.Command, targetCommand) {
		t.Fatalf("recovered command = %#v, want %#v", inst.rec.Command, targetCommand)
	}
	if got := inst.rec.Deps[2]; got != conflict.Instance {
		t.Fatalf("recovered deps = %v, want dependency on %s", inst.rec.Deps, conflict)
	}
	if inst.rec.Seq <= 2 {
		t.Fatalf("recovered seq = %d, want greater than conflicting seq 2", inst.rec.Seq)
	}
}

func TestPromisedFollowerTakesOverAfterCoordinatorDisappears(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 3, 3)
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{
		Type:   MsgPrepare,
		From:   2,
		To:     3,
		Ref:    ref,
		Ballot: Ballot{Number: 1, Replica: 2},
	}); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())

	deadline := rn.recoveryTicks * 2
	for tick := uint64(1); tick < deadline; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		protocolCoverageAdvanceHardStateOnly(t, rn, tick)
	}
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if !rd.HardState.Equal(HardState{Conf: rn.Status().Conf, Tick: deadline}) || !rd.MustSync {
		t.Fatalf("takeover Ready hard state = %#v, want tick %d", rd.HardState, deadline)
	}
	prepare := optimizedRequireMessage(t, rd.Messages, MsgPrepare, 1)
	if prepare.From != 3 || prepare.Ref != ref || prepare.Ballot.Number <= 1 || prepare.Ballot.Replica != 3 ||
		prepare.RecordBallot != (Ballot{}) || prepare.RecordStatus != StatusNone ||
		prepare.Seq != 0 || len(prepare.Deps) != 0 || !commandEqual(prepare.Command, Command{}) {
		t.Fatalf("takeover Prepare = %#v, want minimal higher-ballot request from replica 3", prepare)
	}
}

func TestTryPreAcceptFollowerTakesOverAfterCoordinatorDisappears(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 3, 3)
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{Type: MsgTryPreAccept, From: 2,
		To:      3,
		Ref:     ref,
		Ballot:  Ballot{Number: 1, Replica: 2},
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("orphaned-try", "orphaned-try-key")}); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rn.Ready())

	deadline := rn.recoveryTicks * 2
	for tick := uint64(1); tick < deadline; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		protocolCoverageAdvanceHardStateOnly(t, rn, tick)
	}
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if !rd.HardState.Equal(HardState{Conf: rn.Status().Conf, Tick: deadline}) || !rd.MustSync {
		t.Fatalf("TryPreAccept takeover Ready hard state = %#v, want tick %d", rd.HardState, deadline)
	}
	prepare := optimizedRequireMessage(t, rd.Messages, MsgPrepare, 1)
	if prepare.From != 3 || prepare.Ref != ref || prepare.Ballot.Number <= 1 || prepare.Ballot.Replica != 3 ||
		prepare.RecordBallot != (Ballot{}) || prepare.RecordStatus != StatusNone ||
		prepare.Seq != 0 || len(prepare.Deps) != 0 || !commandEqual(prepare.Command, Command{}) {
		t.Fatalf("TryPreAccept takeover = %#v, want minimal higher-ballot request from replica 3", prepare)
	}
}

func TestLateTimedPreAcceptFallsBackToConservativeConflictOrdering(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 2, Voters: makeIDs(3), TimeOptimization: true, TimeOptimizationTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	protocolCoverageAdvanceHardStateOnly(t, rn, 0)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	protocolCoverageAdvanceHardStateOnly(t, rn, 1)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	protocolCoverageAdvanceHardStateOnly(t, rn, 2)

	key := "late-timed-conflict"
	laterRef := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	earlierRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	step := func(ref InstanceRef, from ReplicaID, payload string) {
		t.Helper()
		if err := rn.Step(Message{
			Type:      MsgPreAccept,
			From:      from,
			To:        2,
			Ref:       ref,
			ProcessAt: 1,
			Ballot:    Ballot{Replica: from},
			Seq:       1,
			Deps:      rn.q.deps(),
			Command:   optimizedTestCommand(payload, key),
		}); err != nil {
			t.Fatal(err)
		}
		advanceOK(t, rn, rn.Ready())
	}
	step(laterRef, 3, "late-higher-ref")
	step(earlierRef, 1, "late-lower-ref")

	earlier := rn.instances[earlierRef]
	if earlier == nil || earlier.rec.Deps[2] < laterRef.Instance || earlier.rec.Seq <= rn.instances[laterRef].rec.Seq {
		t.Fatalf("late earlier-order tuple = %#v, want conservative dependency on already processed %s", earlier, laterRef)
	}
}

func TestTimerSchedulingNearCounterLimitDoesNotWrapOrRefire(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	rec := InstanceRecord{
		Ref:              ref,
		Ballot:           Ballot{Replica: 1},
		RecordBallot:     Ballot{Replica: 1},
		Status:           StatusPreAccepted,
		Seq:              1,
		Deps:             rn.q.deps(),
		Command:          optimizedTestCommand("counter-limit", "counter-limit-key"),
		FastPathEligible: true,
	}
	rec.Checksum = ChecksumRecord(rec)
	inst := &instance{rec: rec, phase: phasePreAccept}
	rn.instances[ref] = inst
	rn.tick = ^uint64(0) - 1

	if err := rn.schedule(inst, timerPreAccept, 2); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("overflowing near-limit schedule err=%v, want %v", err, ErrLogicalTimeExhausted)
	}
	if len(rn.timers) != 0 {
		t.Fatalf("overflowing near-limit schedule inserted timers: %#v", rn.timers)
	}
	if err := rn.schedule(inst, timerPreAccept, 1); err != nil {
		t.Fatal(err)
	}
	if len(rn.timers) != 1 || rn.timers[0].deadline != ^uint64(0) {
		t.Fatalf("exact near-limit timer = %#v, want one timer at MaxUint64", rn.timers)
	}
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if !rd.HardState.Equal(HardState{Conf: rn.Status().Conf, Tick: ^uint64(0)}) || !rd.MustSync {
		t.Fatalf("counter-limit Ready hard state = %#v, want exact durable tick", rd.HardState)
	}
	if rn.tick != ^uint64(0) || len(rn.timers) != 0 {
		t.Fatalf("counter-limit tick/timers = %d/%#v, want saturated tick and no unrepresentable retry", rn.tick, rn.timers)
	}
}

func TestComputeAttrsUsesMaximumSequenceAcrossCompressedPrefix(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	highSeq := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	laterLowSeq := InstanceRef{Replica: 2, Instance: 2, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:          highSeq,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Status:       StatusCommitted,
		Seq:          10,
		Deps:         rn.q.deps(),
		Command:      Command{ConflictKeys: [][]byte{[]byte("compressed-prefix-key")}},
	})
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:          laterLowSeq,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Status:       StatusCommitted,
		Seq:          1,
		Deps:         rn.q.deps(),
		Command:      Command{ConflictKeys: [][]byte{[]byte("compressed-prefix-key")}},
	})
	cmd := Command{ConflictKeys: [][]byte{[]byte("compressed-prefix-key")}}
	attrs := rn.computeAttrs(cmd, InstanceRef{Replica: 1, Instance: 1, Conf: 1})
	if attrs.Deps[1] != laterLowSeq.Instance || attrs.Seq != 11 {
		t.Fatalf("compressed-prefix attrs = %#v, want replica-2 prefix %d and seq 11", attrs, laterLowSeq.Instance)
	}
	for i := range 32 {
		timed := rn.computeAttrsAt(cmd, InstanceRef{Replica: 1, Instance: InstanceNum(i + 2), Conf: 1}, 10, true)
		if timed.Deps[1] != laterLowSeq.Instance || timed.Seq != 11 {
			t.Fatalf("timed compressed-prefix attrs iteration %d = %#v, want replica-2 prefix %d and seq 11", i, timed, laterLowSeq.Instance)
		}
	}
}

func TestUnresolvedKnownConflictStartsRecoveryOnAnyHealthyReplica(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 3, 5)
	base := InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	blocker := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	key := "unresolved-known-conflict-recovery"
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:          base,
		Ballot:       Ballot{Replica: base.Replica},
		RecordBallot: Ballot{Replica: base.Replica},
		Status:       StatusCommitted,
		Seq:          1,
		Deps:         rn.q.deps(),
		Command:      optimizedTestCommand("base", key),
	})
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:              blocker,
		Ballot:           Ballot{Replica: blocker.Replica},
		RecordBallot:     Ballot{Replica: blocker.Replica},
		Status:           StatusPreAccepted,
		Seq:              1,
		Deps:             rn.q.deps(),
		Command:          optimizedTestCommand("blocker", key),
		FastPathEligible: true,
	})
	if rn.shouldCoordinateRecovery(blocker) {
		t.Fatalf("test replica unexpectedly selected as first recovery coordinator for %s", blocker)
	}
	rn.tryExecute()
	deadline := rn.recoveryTicks * 2
	var rd Ready
	for tick := uint64(1); tick <= deadline; tick++ {
		if err := rn.Tick(); err != nil {
			t.Fatal(err)
		}
		inst := rn.instances[blocker]
		if inst != nil && inst.phase == phasePrepare {
			rd = rn.Ready()
			wantHard := HardState{Conf: rn.Status().Conf, Tick: tick}
			if !rd.HardState.Equal(wantHard) || !rd.MustSync || len(rd.Committed) != 0 {
				t.Fatalf("fallback recovery Ready at tick %d = %#v", tick, rd)
			}
			break
		}
		if tick == rn.recoveryTicks {
			tickReady := rn.Ready()
			wantHard := HardState{Conf: rn.Status().Conf, Tick: tick}
			if !tickReady.HardState.Equal(wantHard) || !tickReady.MustSync ||
				len(tickReady.Records) != 0 || len(tickReady.Committed) != 0 ||
				len(tickReady.Messages) != len(rn.Status().Conf.Voters)-1 {
				t.Fatalf("committed-base retry Ready at tick %d = %#v", tick, tickReady)
			}
			for _, msg := range tickReady.Messages {
				if msg.Type != MsgCommit || msg.Ref != base {
					t.Fatalf("tick %d emitted non-base retry message: %#v", tick, msg)
				}
			}
			advanceOK(t, rn, tickReady)
			continue
		}
		protocolCoverageAdvanceHardStateOnly(t, rn, tick)
	}
	inst := rn.instances[blocker]
	if inst == nil || inst.phase != phasePrepare || inst.rec.Ballot.Replica != rn.id {
		t.Fatalf("healthy fallback replica did not start blocker recovery after coordinator rotation: %#v", inst)
	}
	for _, to := range []ReplicaID{1, 2, 4, 5} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgPrepare, to)
		if msg.From != 3 || msg.Ref != blocker || msg.Ballot != inst.rec.Ballot ||
			msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
			msg.Seq != 0 || len(msg.Deps) != 0 || !commandEqual(msg.Command, Command{}) {
			t.Fatalf("Prepare to %d = %#v, want minimal recovery request for %s at %#v", to, msg, blocker, inst.rec.Ballot)
		}
	}
}

func TestTryPreAcceptResponseRequiresCurrentBallotAndExactTuple(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*Message)
		wantErr error
	}{
		{
			name: "future ballot",
			mutate: func(m *Message) {
				m.Ballot.Number++
			},
		},
		{
			name: "different sequence",
			mutate: func(m *Message) {
				m.Seq++
			},
		},
		{
			name: "different dependencies",
			mutate: func(m *Message) {
				m.Deps[1] = 1
			},
		},
		{
			name: "wrong status",
			mutate: func(m *Message) {
				m.RecordStatus = StatusAccepted
			},
			wantErr: ErrInvalidMessage,
		},
		{
			name: "unexpected record ballot",
			mutate: func(m *Message) {
				m.RecordBallot = m.Ballot
			},
			wantErr: ErrInvalidMessage,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rn, inst, ref := protocolTryInstance(t, 3, 1)
			msg := Message{
				Type:         MsgTryPreAcceptResp,
				From:         2,
				To:           1,
				Ref:          ref,
				Ballot:       inst.rec.Ballot,
				RecordStatus: StatusPreAccepted,
				Seq:          inst.rec.Seq,
				Deps:         append([]InstanceNum(nil), inst.rec.Deps...),
			}
			tc.mutate(&msg)
			if err := rn.Step(msg); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Step error = %v, want %v for message %#v", err, tc.wantErr, msg)
			}
			if inst.tryOK.len() != 0 || inst.phase != phaseTryPreAccept || rn.HasReady() {
				t.Fatalf("invalid TryPreAccept response changed state: phase=%d tryOK=%#v ready=%#v", inst.phase, inst.tryOK, rn.Ready())
			}
		})
	}
}

func TestMessageValidateAndAcceptResponseExactTupleContract(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	ref := InstanceRef{Replica: 1, Instance: 93, Conf: 1}
	ballot := Ballot{Number: 1, Replica: 1}
	rec := checkedRecord(InstanceRecord{
		Ref:          ref,
		Ballot:       ballot,
		RecordBallot: ballot,
		Status:       StatusAccepted,
		Seq:          3,
		Deps:         []InstanceNum{0, 2, 0},
		Command:      optimizedTestCommand("accept-response-exact", "accept-response-key"),
	})
	inst := &instance{
		rec:   rec,
		phase: phaseAccept,
		accOK: testVoterMask(t, rn.q.conf, 1),
	}
	rn.instances[ref] = inst

	exact := Message{
		Type:         MsgAcceptResp,
		From:         2,
		To:           1,
		Ref:          ref,
		Ballot:       ballot,
		RecordBallot: ballot,
		RecordStatus: StatusAccepted,
		Seq:          rec.Seq,
		Deps:         append([]InstanceNum(nil), rec.Deps...),
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Message)
	}{
		{
			name: "different current ballot",
			mutate: func(m *Message) {
				m.Ballot.Number++
				m.RecordBallot = m.Ballot
			},
		},
		{
			name: "different sequence",
			mutate: func(m *Message) {
				m.Seq++
			},
		},
		{
			name: "different dependencies",
			mutate: func(m *Message) {
				m.Deps[0] = 1
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := exact.Clone()
			tc.mutate(&msg)
			if err := rn.Step(msg); err != nil {
				t.Fatalf("well-formed mismatched AcceptResp rejected before handler: %v", err)
			}
			if inst.phase != phaseAccept || inst.accOK.len() != 1 || rn.HasReady() {
				t.Fatalf("mismatched AcceptResp changed state: phase=%d accOK=%#v ready=%v", inst.phase, inst.accOK, rn.HasReady())
			}
		})
	}

	malformed := exact.Clone()
	malformed.RecordBallot.Number++
	if err := rn.Step(malformed); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("AcceptResp with non-echoed RecordBallot error = %v, want ErrInvalidMessage", err)
	}
	if inst.phase != phaseAccept || inst.accOK.len() != 1 || rn.HasReady() {
		t.Fatalf("malformed AcceptResp changed state: phase=%d accOK=%#v ready=%v", inst.phase, inst.accOK, rn.HasReady())
	}

	if err := rn.Step(exact); err != nil {
		t.Fatal(err)
	}
	if inst.phase != phaseCommitted || inst.accOK != 0 {
		t.Fatalf("exact AcceptResp phase/volatile votes = %d/%#v, want committed with votes released", inst.phase, inst.accOK)
	}
	rd := rn.Ready()
	var committed InstanceRecord
	for _, durable := range rd.Records {
		if durable.Ref == ref && durable.Status == StatusCommitted {
			committed = durable
			break
		}
	}
	if committed.Status != StatusCommitted || committed.Ballot != ballot ||
		committed.RecordBallot != ballot || committed.Seq != rec.Seq ||
		!instanceNumsEqual(committed.Deps, rec.Deps) || !commandEqual(committed.Command, rec.Command) {
		t.Fatalf("exact AcceptResp committed record = %#v, want chosen tuple %#v", committed, rec)
	}
}

func TestTimingDomainChecksumMutationIsDetected(t *testing.T) {
	rec := checkedRecord(InstanceRecord{
		Ref:          InstanceRef{Conf: 1, Replica: 1, Instance: 1},
		Ballot:       Ballot{Replica: 1},
		Status:       StatusCommitted,
		Seq:          1,
		Deps:         make([]InstanceNum, 3),
		Command:      optimizedTestCommand("timing-checksum", "timing-checksum-key"),
		ProcessAt:    0,
		TimingDomain: TimingDomainTOQ,
	})
	rec.Checksum = ChecksumRecord(rec)
	if !VerifyRecordChecksum(rec) {
		t.Fatal("canonical TOQ@0 checksum did not verify")
	}
	rec.TimingDomain = TimingDomainUntimed
	if VerifyRecordChecksum(rec) {
		t.Fatal("TimingDomain mutation was not authenticated")
	}
}

func TestTryEvidenceResolutionPreservesTupleBeforeOwnershipCleanup(t *testing.T) {
	rn, inst, target := protocolTryInstance(t, 3, 1)
	conflict := InstanceRef{Conf: 1, Replica: 2, Instance: 7}
	key := tryEvidenceKey{target: target, conflict: conflict, ballot: inst.rec.Ballot}
	payload := []byte("evidence-only-committed-tuple")
	tuple := checkedRecord(InstanceRecord{
		Ref:     conflict,
		Ballot:  Ballot{Replica: conflict.Replica},
		Status:  StatusCommitted,
		Seq:     7,
		Deps:    rn.q.deps(),
		Command: Command{ID: CommandID{Client: 1400, Sequence: 1}, Payload: payload},
	})
	records := map[ReplicaID]InstanceRecord{2: tuple}
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{key: records}
	rn.resolveTryEvidenceCheck(key)
	if inst.phase != phaseAccept {
		t.Fatalf("evidence-only committed tuple phase = %d, want Accept", inst.phase)
	}
	if inst.rec.Seq < tuple.Seq+1 {
		t.Fatalf("captured conflict Seq was lost: accepted Seq=%d conflict Seq=%d", inst.rec.Seq, tuple.Seq)
	}
	if len(rn.tryEvidenceChecks) != 0 || len(records) != 0 {
		t.Fatalf("resolved evidence retained maps: outer=%d nested=%d", len(rn.tryEvidenceChecks), len(records))
	}
}

func TestTryEvidenceAdmissionIsBoundedPerTargetAndGlobally(t *testing.T) {
	rn, inst, _ := protocolTryInstance(t, 3, 2)
	rn.maxConcurrentRecoveries = 1
	inst.rec.Deps = []InstanceNum{0, 20, 20}
	inst.rec.Checksum = ChecksumRecord(inst.rec)
	first := InstanceRef{Conf: 1, Replica: 2, Instance: 10}
	second := InstanceRef{Conf: 1, Replica: 3, Instance: 11}
	if !rn.maybeStartTryEvidenceCheck(inst, first) {
		t.Fatal("first covered conflict did not start evidence check")
	}
	if len(rn.tryEvidenceChecks) != 1 {
		t.Fatalf("first evidence check count = %d, want 1", len(rn.tryEvidenceChecks))
	}
	beforeMessages := len(rn.pendingReady.Messages)
	if !rn.maybeStartTryEvidenceCheck(inst, second) {
		t.Fatal("second covered conflict did not take deterministic fail-closed path")
	}
	if inst.phase != phaseAccept || len(rn.tryEvidenceChecks) != 0 {
		t.Fatalf("second conflict phase/checks = %d/%d, want Accept/0", inst.phase, len(rn.tryEvidenceChecks))
	}
	if got := len(rn.pendingReady.Messages) - beforeMessages; got != len(rn.q.conf.Voters)-1 {
		t.Fatalf("second conflict emitted %d new messages, want one bounded Accept broadcast of %d", got, len(rn.q.conf.Voters)-1)
	}

	other, otherInst, _ := protocolTryInstance(t, 3, 3)
	other.maxConcurrentRecoveries = 1
	otherInst.rec.Deps = []InstanceNum{0, 20, 20}
	otherInst.rec.Checksum = ChecksumRecord(otherInst.rec)
	other.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{
		{target: InstanceRef{Conf: 1, Replica: 1, Instance: 999}, conflict: first, ballot: Ballot{Number: 1, Replica: 1}}: {},
	}
	if !other.maybeStartTryEvidenceCheck(otherInst, second) || otherInst.phase != phaseAccept {
		t.Fatalf("global evidence cap did not fail closed: phase=%d checks=%d", otherInst.phase, len(other.tryEvidenceChecks))
	}
}

func TestTerminalCommitReleasesInstanceVolatileOwnership(t *testing.T) {
	rn := protocolCoverageNewRawNode(t, 1, 3)
	ref := InstanceRef{Conf: 1, Replica: 1, Instance: 40}
	rec := checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("volatile-terminal", "volatile-terminal-key"),
	})
	inst := &instance{
		rec:   rec,
		phase: phaseAccept,
		preOK: testAttrVoteSet(t, rn.q.conf, map[ReplicaID]testAttrVote{
			1: {seq: 1, deps: []InstanceNum{1, 2, 3}},
		}),
		accOK:           testVoterMask(t, rn.q.conf, 1),
		prepareOK:       testRecordVoteSet(t, rn.q.conf, map[ReplicaID]InstanceRecord{1: rec.Clone()}),
		prepareEvidence: testRecordVoteSet(t, rn.q.conf, map[ReplicaID]InstanceRecord{2: rec.Clone()}),
		tryOK:           testVoterMask(t, rn.q.conf, 1),
		tryDeferred:     map[InstanceRef]struct{}{{Conf: 1, Replica: 2, Instance: 1}: {}},
		tryIgnored:      map[InstanceRef]struct{}{{Conf: 1, Replica: 3, Instance: 1}: {}},
		waitDeadline:    99,
	}
	rn.installInstance(inst)
	rn.commit(inst, rec.Attributes())
	if inst.preOK != nil || inst.accOK != 0 || inst.prepareOK != nil || inst.prepareEvidence != nil ||
		inst.tryOK != 0 || inst.tryDeferred != nil || inst.tryIgnored != nil || inst.waitDeadline != 0 {
		t.Fatalf("terminal instance retained volatile ownership: %#v", inst)
	}
}

func TestTryPreAcceptRetainsRequiredLocalPrepareWitness(t *testing.T) {
	for _, voters := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("n=%d", voters), func(t *testing.T) {
			rn := protocolCoverageNewRawNode(t, 1, voters)
			ref := InstanceRef{Conf: 1, Replica: 2, Instance: 50}
			attrs := Attributes{Seq: 4, Deps: make([]InstanceNum, voters)}
			command := optimizedTestCommand("local-prepare-witness", "local-prepare-witness-key")
			rec := checkedRecord(InstanceRecord{
				Ref:     ref,
				Ballot:  Ballot{Number: 1, Replica: 1},
				Status:  StatusPreAccepted,
				Seq:     attrs.Seq,
				Deps:    attrs.Deps,
				Command: command,
			})
			local := rec.Clone()
			local.FastPathEligible = true
			inst := &instance{
				rec:       rec,
				phase:     phasePrepare,
				prepareOK: testRecordVoteSet(t, rn.q.conf, map[ReplicaID]InstanceRecord{1: local}),
			}
			rn.installInstance(inst)
			var witnesses voterMask
			for id := ReplicaID(3); witnesses.len() < rn.slowQuorumForConf(1)-1; id++ {
				if !witnesses.add(rn.q.conf, id) {
					t.Fatalf("invalid witness %d", id)
				}
			}
			rn.startTryPreAccept(inst, attrs, witnesses, false)
			if inst.phase != phaseAccept {
				t.Fatalf("N=%d local Prepare witness lost: phase=%d tryOK=%v", voters, inst.phase, inst.tryOK)
			}
		})
	}
}

func TestValueBearingMessagesRejectIncompatibleTimingDomains(t *testing.T) {
	build := func(kind MessageType, toq bool) Message {
		ref := InstanceRef{Conf: 1, Replica: 2, Instance: InstanceNum(kind)}
		command := optimizedTestCommand("wrong-domain", "wrong-domain-key")
		requestBallot := Ballot{Number: 1, Replica: 2}
		responseBallot := Ballot{Number: 1, Replica: 1}
		message := Message{
			Type:      kind,
			From:      2,
			To:        1,
			Ref:       ref,
			Ballot:    requestBallot,
			Seq:       1,
			Deps:      make([]InstanceNum, 3),
			Command:   command,
			ProcessAt: 9,
			TOQ:       toq,
		}
		switch kind {
		case MsgCommit:
			message.RecordBallot = requestBallot
		case MsgPrepareResp:
			message.Ballot = responseBallot
			message.RecordBallot = Ballot{Replica: ref.Replica}
			message.RecordStatus = StatusAccepted
		case MsgEvidenceResp:
			message.Ballot = responseBallot
			message.RecordBallot = Ballot{Replica: ref.Replica}
			message.RecordStatus = StatusAccepted
			message.ConflictRef = InstanceRef{Conf: 1, Replica: 1, Instance: 99}
		case MsgPreAccept, MsgPreAcceptResp, MsgAccept, MsgAcceptResp, MsgPrepare, MsgTryPreAccept, MsgTryPreAcceptResp, MsgEvidence:
		}
		return message
	}
	for _, tc := range []struct {
		name   string
		config Config
		toq    bool
	}{
		{name: "toq-value-on-logical-node", config: Config{ID: 1, Voters: makeIDs(3), TimeOptimization: true}, toq: true},
		{name: "logical-value-on-toq-node-without-runtime", config: Config{ID: 1, Voters: makeIDs(3), TOQ: true, TOQClock: func() uint64 { return 0 }}, toq: false},
	} {
		for _, kind := range []MessageType{MsgAccept, MsgCommit, MsgPrepareResp, MsgTryPreAccept, MsgEvidenceResp} {
			t.Run(tc.name+"/"+kind.String(), func(t *testing.T) {
				rn, err := NewRawNode(tc.config)
				if err != nil {
					t.Fatal(err)
				}
				message := build(kind, tc.toq)
				if err := message.Validate(rn.q.conf); err != nil {
					t.Fatalf("test message is malformed before domain gate: %v", err)
				}
				before := snapshotRemainingNodeProtocol(rn)
				if err := rn.Step(message); !errors.Is(err, ErrMessageRejected) {
					t.Fatalf("wrong-domain %s err=%v, want ErrMessageRejected", kind, err)
				}
				requireRemainingNodeProtocolUnchanged(t, rn, before)
			})
		}
	}
}

func TestRestartRejectsIncompatibleNonNoopTimingDomain(t *testing.T) {
	record := func(domain TimingDomain, kind CommandKind) InstanceRecord {
		ref := InstanceRef{Conf: 1, Replica: 2, Instance: 1}
		processAt := uint64(9)
		if domain == TimingDomainUntimed {
			processAt = 0
		}
		rec := checkedRecord(InstanceRecord{
			Ref:          ref,
			Ballot:       Ballot{Replica: 2},
			Status:       StatusAccepted,
			Seq:          1,
			Deps:         make([]InstanceNum, 3),
			Command:      Command{ID: CommandID{Client: 1500, Sequence: 1}, Kind: kind, Payload: []byte("restart-domain")},
			ProcessAt:    processAt,
			TimingDomain: domain,
		})
		if kind == CommandNoop {
			rec.Command = Command{Kind: CommandNoop}
		}
		rec.Checksum = ChecksumRecord(rec)
		return rec
	}
	makeStore := func(rec InstanceRecord) *MemoryStorage {
		store := NewMemoryStorage()
		store.Hard = HardState{Conf: ConfState{ID: 1, Voters: makeIDs(3)}}
		store.Records[rec.Ref] = rec.Clone()
		return store
	}
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), TimeOptimization: true, Storage: makeStore(record(TimingDomainTOQ, CommandUser))}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("logical reopen of TOQ value err=%v, want ErrInvalidConfig", err)
	}
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), TOQ: true, TOQClock: func() uint64 { return 0 }, Storage: makeStore(record(TimingDomainLogical, CommandUser))}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("TOQ reopen of logical value err=%v, want ErrInvalidConfig", err)
	}
	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: makeStore(record(TimingDomainTOQ, CommandNoop))}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("timing-tagged no-op restart err=%v, want ErrInvalidConfig", err)
	}
}

func TestTimedModesProposeCanonicalUntimedNoop(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  func(*MemoryStorage, *int) Config
	}{
		{
			name: "logical",
			cfg: func(store *MemoryStorage, _ *int) Config {
				return Config{ID: 1, Voters: makeIDs(3), Storage: store, TimeOptimization: true, TimeOptimizationTicks: 5}
			},
		},
		{
			name: "TOQ",
			cfg: func(store *MemoryStorage, reads *int) Config {
				conf := ConfState{ID: 1, Voters: makeIDs(3)}
				return Config{
					ID:         1,
					Voters:     conf.Voters,
					Storage:    store,
					TOQ:        true,
					TOQClock:   func() uint64 { (*reads)++; return 50 },
					TOQRuntime: &TOQRuntimeConfig{Conf: conf, OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 1, 3: 1}},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStorage()
			reads := 0
			cfg := tc.cfg(store, &reads)
			rn, err := NewRawNode(cfg)
			if err != nil {
				t.Fatal(err)
			}
			initial := rn.Ready()
			if err := store.ApplyReady(initial); err != nil {
				t.Fatal(err)
			}
			advanceOK(t, rn, initial)
			ref, err := rn.Propose(Command{Kind: CommandNoop})
			if err != nil {
				t.Fatal(err)
			}
			rd := rn.Ready()
			if len(rd.Records) != 1 || rd.Records[0].Ref != ref {
				t.Fatalf("no-op Ready records = %#v, want one record for %s", rd.Records, ref)
			}
			rec := rd.Records[0]
			if rec.Command.Kind != CommandNoop || rec.ProcessAt != 0 || rec.TimingDomain != TimingDomainUntimed || rec.TOQPending || !VerifyRecordChecksum(rec) {
				t.Fatalf("timed-mode no-op record is not canonical untimed: %#v", rec)
			}
			var responses []Message
			for _, message := range rd.Messages {
				if message.Command.Kind != CommandNoop || message.ProcessAt != 0 || message.TOQ {
					t.Fatalf("timed-mode no-op message is not canonical untimed: %#v", message)
				}
				if err := message.Validate(rn.q.conf); err != nil {
					t.Fatalf("canonical no-op message failed validation: %v", err)
				}
				followerCfg := cfg
				followerCfg.ID = message.To
				followerCfg.Storage = NewMemoryStorage()
				follower, err := NewRawNode(followerCfg)
				if err != nil {
					t.Fatal(err)
				}
				advanceOK(t, follower, follower.Ready())
				if err := follower.Step(message); err != nil {
					t.Fatalf("follower %d rejected canonical timed-mode no-op: %v", message.To, err)
				}
				followerReady := follower.Ready()
				if len(followerReady.Records) != 1 || !VerifyRecordChecksum(followerReady.Records[0]) || followerReady.Records[0].TimingDomain != TimingDomainUntimed {
					t.Fatalf("follower %d no-op Ready = %#v", message.To, followerReady)
				}
				responses = append(responses, followerReady.Messages...)
			}
			if len(rn.deferredIndex) != 0 {
				t.Fatalf("canonical no-op entered timed deferred queue: %#v", rn.deferredIndex)
			}
			if tc.name == "TOQ" && reads != 0 {
				t.Fatalf("TOQ no-op sampled caller clock %d times, want zero", reads)
			}
			if err := store.ApplyReady(rd); err != nil {
				t.Fatal(err)
			}
			advanceOK(t, rn, rd)
			for _, response := range responses {
				if err := rn.Step(response); err != nil {
					t.Fatalf("owner rejected no-op response: %v", err)
				}
			}
			if rn.HasReady() {
				committed := rn.Ready()
				for _, record := range committed.Records {
					if record.Command.Kind == CommandNoop && (record.ProcessAt != 0 || record.TimingDomain != TimingDomainUntimed || record.TOQPending || !VerifyRecordChecksum(record)) {
						t.Fatalf("committed no-op record lost canonical timing: %#v", record)
					}
				}
				if err := store.ApplyReady(committed); err != nil {
					t.Fatal(err)
				}
				advanceOK(t, rn, committed)
			}
			restarted, err := NewRawNode(tc.cfg(store, &reads))
			if err != nil {
				t.Fatalf("restart with canonical no-op: %v", err)
			}
			got := restarted.instances[ref]
			if got == nil || got.rec.Command.Kind != CommandNoop || got.rec.ProcessAt != 0 || got.rec.TimingDomain != TimingDomainUntimed || got.rec.TOQPending {
				t.Fatalf("restarted canonical no-op = %#v", got)
			}
		})
	}
}

func TestTimedNoopMessagesRejectWithoutMutation(t *testing.T) {
	build := func(kind MessageType, toq bool) Message {
		ref := InstanceRef{Conf: 1, Replica: 2, Instance: InstanceNum(kind)}
		requestBallot := Ballot{Number: 1, Replica: 2}
		responseBallot := Ballot{Number: 1, Replica: 1}
		message := Message{Type: kind, From: 2, To: 1, Ref: ref, Ballot: requestBallot, Seq: 1, Deps: make([]InstanceNum, 3), Command: Command{Kind: CommandNoop}, ProcessAt: 9, TOQ: toq}
		switch kind {
		case MsgCommit:
			message.RecordBallot = requestBallot
		case MsgPrepareResp:
			message.Ballot = responseBallot
			message.RecordBallot = Ballot{Replica: ref.Replica}
			message.RecordStatus = StatusAccepted
		case MsgEvidenceResp:
			message.Ballot = responseBallot
			message.RecordBallot = Ballot{Replica: ref.Replica}
			message.RecordStatus = StatusAccepted
			message.ConflictRef = InstanceRef{Conf: 1, Replica: 1, Instance: 99}
		case MsgPreAccept, MsgPreAcceptResp, MsgAccept, MsgAcceptResp, MsgPrepare, MsgTryPreAccept, MsgTryPreAcceptResp, MsgEvidence:
		}
		return message
	}
	for _, toq := range []bool{false, true} {
		for _, kind := range []MessageType{MsgAccept, MsgCommit, MsgPrepareResp, MsgTryPreAccept, MsgEvidenceResp} {
			t.Run(fmt.Sprintf("toq=%v/%s", toq, kind), func(t *testing.T) {
				rn := protocolCoverageNewRawNode(t, 1, 3)
				message := build(kind, toq)
				before := snapshotRemainingNodeProtocol(rn)
				if err := rn.Step(message); !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("timed no-op %s error = %v, want ErrInvalidMessage", kind, err)
				}
				requireRemainingNodeProtocolUnchanged(t, rn, before)
			})
		}
	}
}

func TestTimedModesRejectFreshUntimedValueAndPreserveLegacyContinuation(t *testing.T) {
	for _, domain := range []TimingDomain{TimingDomainLogical, TimingDomainTOQ} {
		for _, kind := range []MessageType{MsgAccept, MsgCommit} {
			t.Run(fmt.Sprintf("%v/%s", domain, kind), func(t *testing.T) {
				cfg := Config{ID: 1, Voters: makeIDs(3)}
				if domain == TimingDomainLogical {
					cfg.TimeOptimization = true
				} else {
					cfg.TOQ = true
					cfg.TOQClock = func() uint64 { return 0 }
				}
				fresh, err := NewRawNode(cfg)
				if err != nil {
					t.Fatal(err)
				}
				protocolCoverageAdvanceHardStateOnly(t, fresh, 0)
				ref := InstanceRef{Conf: 1, Replica: 2, Instance: InstanceNum(kind)}
				ballot := Ballot{Number: 1, Replica: 2}
				command := optimizedTestCommand("legacy-zero", "legacy-zero-key")
				message := Message{Type: kind, From: 2, To: 1, Ref: ref, Ballot: ballot, Seq: 1, Deps: make([]InstanceNum, 3), Command: command}
				if kind == MsgCommit {
					message.RecordBallot = ballot
				}
				before := snapshotRemainingNodeProtocol(fresh)
				if err := fresh.Step(message); !errors.Is(err, ErrMessageRejected) {
					t.Fatalf("fresh timed node accepted Untimed %s: %v", kind, err)
				}
				requireRemainingNodeProtocolUnchanged(t, fresh, before)

				continued, err := NewRawNode(cfg)
				if err != nil {
					t.Fatal(err)
				}
				protocolCoverageAdvanceHardStateOnly(t, continued, 0)
				oldStatus := StatusPreAccepted
				if kind == MsgCommit {
					oldStatus = StatusAccepted
				}
				old := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: oldStatus, Seq: 1, Deps: make([]InstanceNum, 3), Command: command, ProcessAt: 9, TimingDomain: domain})
				continued.installInstance(&instance{rec: old, phase: phaseIdle})
				if err := continued.Step(message); err != nil {
					t.Fatalf("legacy zero-timing continuation failed: %v", err)
				}
				got := continued.instances[ref].rec
				if got.ProcessAt != 9 || got.TimingDomain != domain {
					t.Fatalf("legacy continuation lost pinned timing: %#v", got)
				}
			})
		}
	}
}

func TestSameBallotTimedPhaseConflictsRejectWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   MessageType
		status Status
		phase  phase
	}{
		{name: "Accept duplicate", kind: MsgAccept, status: StatusAccepted, phase: phaseIdle},
		{name: "Accept promotion", kind: MsgAccept, status: StatusPreAccepted, phase: phaseIdle},
		{name: "Commit duplicate", kind: MsgCommit, status: StatusCommitted, phase: phaseCommitted},
		{name: "Commit promotion", kind: MsgCommit, status: StatusPreAccepted, phase: phaseIdle},
		{name: "TryPreAccept duplicate", kind: MsgTryPreAccept, status: StatusPreAccepted, phase: phaseIdle},
	} {
		for _, mutation := range []string{"ProcessAt", "domain"} {
			t.Run(tc.name+"/"+mutation, func(t *testing.T) {
				rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), TimeOptimization: true})
				if err != nil {
					t.Fatal(err)
				}
				protocolCoverageAdvanceHardStateOnly(t, rn, 0)
				ref := InstanceRef{Conf: 1, Replica: 2, Instance: InstanceNum(tc.kind)}
				ballot := Ballot{Number: 1, Replica: 2}
				command := optimizedTestCommand("pinned-phase", "pinned-phase-key")
				rec := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: tc.status, Seq: 1, Deps: make([]InstanceNum, 3), Command: command, ProcessAt: 100, TimingDomain: TimingDomainLogical})
				inst := &instance{rec: rec, phase: tc.phase, generation: ^uint64(0)}
				rn.installInstance(inst)
				message := Message{Type: tc.kind, From: 2, To: 1, Ref: ref, Ballot: ballot, Seq: rec.Seq, Deps: append([]InstanceNum(nil), rec.Deps...), Command: command.Clone(), ProcessAt: rec.ProcessAt}
				if tc.kind == MsgCommit {
					message.RecordBallot = ballot
				}
				wantErr := ErrInvalidMessage
				if mutation == "ProcessAt" {
					message.ProcessAt--
				} else {
					message.TOQ = true
					wantErr = ErrMessageRejected
				}
				before := rec.Clone()
				if err := rn.Step(message); !errors.Is(err, wantErr) {
					t.Fatalf("%s conflict error = %v, want %v", mutation, err, wantErr)
				}
				if rn.instances[ref] != inst || inst.generation != ^uint64(0) || !instanceRecordEqual(inst.rec, before) || rn.HasReady() {
					t.Fatalf("%s conflict mutated pinned phase: instance=%#v Ready=%#v", mutation, rn.instances[ref], rn.Ready())
				}
			})
		}
	}

	t.Run("PreAccepted promotion may evolve attributes", func(t *testing.T) {
		rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), TimeOptimization: true})
		if err != nil {
			t.Fatal(err)
		}
		protocolCoverageAdvanceHardStateOnly(t, rn, 0)
		ref := InstanceRef{Conf: 1, Replica: 2, Instance: 99}
		ballot := Ballot{Number: 1, Replica: 2}
		command := optimizedTestCommand("promotion", "promotion-key")
		rec := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: command, ProcessAt: 100, TimingDomain: TimingDomainLogical})
		rn.installInstance(&instance{rec: rec, phase: phaseIdle})
		deps := []InstanceNum{0, 0, 1}
		message := Message{Type: MsgAccept, From: 2, To: 1, Ref: ref, Ballot: ballot, Seq: 2, Deps: deps, Command: command, ProcessAt: 100}
		if err := rn.Step(message); err != nil {
			t.Fatalf("valid same-value promotion: %v", err)
		}
		got := rn.instances[ref].rec
		if got.Status != StatusAccepted || got.Seq != 2 || !instanceNumsEqual(got.Deps, deps) || got.ProcessAt != 100 || got.TimingDomain != TimingDomainLogical {
			t.Fatalf("same-value promotion = %#v", got)
		}
	})
}

func TestIgnoredResponsesStutterAtGenerationAndTickBoundary(t *testing.T) {
	type setupFunc func(*testing.T) (*RawNode, *instance, Message)
	cases := map[string]setupFunc{
		"PreAcceptResp wrong ballot": func(t *testing.T) (*RawNode, *instance, Message) {
			rn := protocolCoverageNewRawNode(t, 1, 3)
			ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
			ballot := Ballot{Number: 1, Replica: 1}
			rec := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("ignored", "ignored-key")})
			inst := &instance{rec: rec, phase: phasePreAccept, preOK: testAttrVoteSet(t, rn.q.conf, map[ReplicaID]testAttrVote{1: {seq: 1, deps: make([]InstanceNum, 3)}})}
			rn.installInstance(inst)
			return rn, inst, Message{Type: MsgPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Replica: 1}, Seq: 1, Deps: make([]InstanceNum, 3)}
		},
		"AcceptResp wrong tuple": func(t *testing.T) (*RawNode, *instance, Message) {
			rn := protocolCoverageNewRawNode(t, 1, 3)
			ref := InstanceRef{Conf: 1, Replica: 1, Instance: 2}
			ballot := Ballot{Number: 1, Replica: 1}
			rec := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: StatusAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("ignored", "ignored-key")})
			inst := &instance{rec: rec, phase: phaseAccept, accOK: testVoterMask(t, rn.q.conf, 1)}
			rn.installInstance(inst)
			return rn, inst, Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordBallot: ballot, RecordStatus: StatusAccepted, Seq: 2, Deps: make([]InstanceNum, 3)}
		},
		"PrepareResp stale ballot": func(t *testing.T) (*RawNode, *instance, Message) {
			rn := protocolCoverageNewRawNode(t, 1, 3)
			ref := InstanceRef{Conf: 1, Replica: 2, Instance: 3}
			ballot := Ballot{Number: 2, Replica: 1}
			rec := InstanceRecord{Ref: ref, Ballot: ballot, Status: StatusNone, Deps: make([]InstanceNum, 3)}
			rec.Checksum = ChecksumRecord(rec)
			inst := &instance{rec: rec, phase: phasePrepare, prepareOK: testRecordVoteSet(t, rn.q.conf, map[ReplicaID]InstanceRecord{1: rec})}
			rn.installInstance(inst)
			return rn, inst, Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, RecordStatus: StatusNone, Deps: make([]InstanceNum, 3)}
		},
		"TryPreAcceptResp wrong tuple": func(t *testing.T) (*RawNode, *instance, Message) {
			rn := protocolCoverageNewRawNode(t, 1, 3)
			ref := InstanceRef{Conf: 1, Replica: 2, Instance: 4}
			ballot := Ballot{Number: 1, Replica: 1}
			rec := checkedRecord(InstanceRecord{Ref: ref, Ballot: ballot, RecordBallot: ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("ignored", "ignored-key")})
			inst := &instance{rec: rec, phase: phaseTryPreAccept, tryOK: testVoterMask(t, rn.q.conf, 1)}
			rn.installInstance(inst)
			return rn, inst, Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordStatus: StatusPreAccepted, Seq: 2, Deps: make([]InstanceNum, 3)}
		},
		"EvidenceResp missing check": func(t *testing.T) (*RawNode, *instance, Message) {
			rn := protocolCoverageNewRawNode(t, 1, 3)
			target := InstanceRef{Conf: 1, Replica: 1, Instance: 5}
			ballot := Ballot{Number: 1, Replica: 1}
			rec := checkedRecord(InstanceRecord{Ref: target, Ballot: ballot, RecordBallot: ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("ignored", "ignored-key")})
			inst := &instance{rec: rec, phase: phaseTryPreAccept}
			rn.installInstance(inst)
			conflict := InstanceRef{Conf: 1, Replica: 2, Instance: 6}
			return rn, inst, Message{Type: MsgEvidenceResp, From: 2, To: 1, Ref: conflict, ConflictRef: target, Ballot: ballot, RecordStatus: StatusNone, Deps: make([]InstanceNum, 3)}
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			rn, inst, message := setup(t)
			inst.generation = ^uint64(0)
			rn.tick = ^uint64(0)
			rn.currentHardState.Tick = rn.tick
			rn.acknowledgedHardState = rn.currentHardState.Clone()
			before := inst.rec.Clone()
			if err := rn.Step(message); err != nil {
				t.Fatalf("ignored response returned exhaustion/error: %v", err)
			}
			if inst.generation != ^uint64(0) || !instanceRecordEqual(inst.rec, before) || rn.HasReady() {
				t.Fatalf("ignored response mutated state: instance=%#v Ready=%#v", inst, rn.Ready())
			}
		})
	}
}

func TestTOQPendingUserSupersededByCanonicalRecoveryNoop(t *testing.T) {
	for _, kind := range []MessageType{MsgAccept, MsgCommit} {
		t.Run(kind.String(), func(t *testing.T) {
			store := NewMemoryStorage()
			conf := ConfState{ID: 1, Voters: makeIDs(3)}
			cfg := Config{ID: 1, Voters: conf.Voters, Storage: store, TOQ: true, TOQClock: func() uint64 { return 50 }, TOQRuntime: &TOQRuntimeConfig{Conf: conf, OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 1, 3: 1}}}
			rn, err := NewRawNode(cfg)
			if err != nil {
				t.Fatal(err)
			}
			initial := rn.Ready()
			if err := store.ApplyReady(initial); err != nil {
				t.Fatal(err)
			}
			advanceOK(t, rn, initial)
			ref := InstanceRef{Conf: 1, Replica: 1, Instance: 1}
			pending := InstanceRecord{Ref: ref, Ballot: Ballot{Replica: 1}, Status: StatusNone, Deps: make([]InstanceNum, 3), Command: optimizedTestCommand("pending-user", "pending-user-key"), ProcessAt: 60, TimingDomain: TimingDomainTOQ, TOQPending: true}
			pending.Checksum = ChecksumRecord(pending)
			rn.installInstance(&instance{rec: pending, phase: phasePreAccept, processAt: pending.ProcessAt})
			ballot := Ballot{Number: 1, Replica: 2}
			message := Message{Type: kind, From: 2, To: 1, Ref: ref, Ballot: ballot, Seq: 1, Deps: make([]InstanceNum, 3), Command: Command{Kind: CommandNoop}}
			if kind == MsgCommit {
				message.RecordBallot = ballot
			}
			if err := rn.Step(message); err != nil {
				t.Fatal(err)
			}
			got := rn.instances[ref].rec
			if got.Command.Kind != CommandNoop || got.ProcessAt != 0 || got.TimingDomain != TimingDomainUntimed || got.TOQPending || !VerifyRecordChecksum(got) {
				t.Fatalf("recovered no-op inherited pending user timing: %#v", got)
			}
			rd := rn.Ready()
			if kind == MsgAccept {
				if err := store.ApplyReady(rd); err != nil {
					t.Fatal(err)
				}
				advanceOK(t, rn, rd)
				commit := Message{Type: MsgCommit, From: 2, To: 1, Ref: ref, Ballot: ballot, RecordBallot: ballot, Seq: 1, Deps: make([]InstanceNum, 3), Command: Command{Kind: CommandNoop}}
				if err := rn.Step(commit); err != nil {
					t.Fatal(err)
				}
				rd = rn.Ready()
			}
			for _, record := range rd.Records {
				if record.Ref == ref && !VerifyRecordChecksum(record) {
					t.Fatalf("recovered no-op emitted invalid Ready record: %#v", record)
				}
			}
			if err := store.ApplyReady(rd); err != nil {
				t.Fatal(err)
			}
			advanceOK(t, rn, rd)
			if terminal := rn.instances[ref]; terminal == nil || terminal.rec.Status != StatusExecuted || terminal.rec.Command.Kind != CommandNoop {
				t.Fatalf("recovered no-op did not reach terminal execution: %#v", terminal)
			}
			terminal := rn.instances[ref]
			duplicate := Message{Type: MsgPreAccept, From: 1, To: 1, Ref: ref, Ballot: Ballot{Replica: 1}, ProcessAt: pending.ProcessAt, TOQ: true, Command: pending.Command}
			if err := rn.Step(duplicate); err != nil {
				t.Fatalf("late pending-value duplicate: %v", err)
			}
			if rn.instances[ref] != terminal || terminal.rec.Command.Kind != CommandNoop || terminal.rec.Status != StatusExecuted {
				t.Fatalf("late pending-value duplicate resurrected original value: %#v", rn.instances[ref])
			}
			duplicateReady := rn.Ready()
			if len(duplicateReady.Records) != 0 || len(duplicateReady.Messages) != 1 || duplicateReady.Messages[0].Type != MsgCommit || duplicateReady.Messages[0].Command.Kind != CommandNoop {
				t.Fatalf("late pending-value duplicate response = %#v, want canonical no-op Commit only", duplicateReady)
			}
			restarted, err := NewRawNode(cfg)
			if err != nil {
				t.Fatalf("restart after pending-user/no-op recovery: %v", err)
			}
			restartedRecord := restarted.instances[ref]
			if restartedRecord == nil || restartedRecord.rec.Command.Kind != CommandNoop || restartedRecord.rec.TimingDomain != TimingDomainUntimed || restartedRecord.rec.ProcessAt != 0 {
				t.Fatalf("restarted recovered no-op = %#v", restartedRecord)
			}
		})
	}
}

func TestTOQUnavailableRuntimeStillAllowsCanonicalNoop(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), TOQ: true, TOQClock: func() uint64 { return 99 }})
	if err != nil {
		t.Fatal(err)
	}
	protocolCoverageAdvanceHardStateOnly(t, rn, 0)
	ref, err := rn.Propose(Command{Kind: CommandNoop})
	if err != nil {
		t.Fatalf("canonical no-op with unavailable TOQ runtime: %v", err)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	if rec.Command.Kind != CommandNoop || rec.ProcessAt != 0 || rec.TimingDomain != TimingDomainUntimed || rec.TOQPending || !VerifyRecordChecksum(rec) {
		t.Fatalf("no-runtime TOQ no-op record = %#v", rec)
	}
	for _, message := range rd.Messages {
		if message.ProcessAt != 0 || message.TOQ || message.Command.Kind != CommandNoop {
			t.Fatalf("no-runtime TOQ no-op message = %#v", message)
		}
	}
}
