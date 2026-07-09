package epaxos

import (
	"encoding/binary"
	"errors"
	"testing"
)

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
			msg := Message{
				Type:             MsgTryPreAccept,
				From:             1,
				To:               2,
				Ref:              ref,
				Ballot:           Ballot{Number: 1, Replica: 1},
				Seq:              3,
				Deps:             []InstanceNum{0, 2, 0},
				Command:          optimizedTestCommand("try-encode", "try-encode-key"),
				RecordStatus:     StatusPreAccepted,
				FastPathEligible: tt.fast,
				DepsCommitted:    0b101,
			}
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
			if decoded.Type != msg.Type || decoded.Ref != msg.Ref || decoded.Seq != msg.Seq || decoded.RecordStatus != msg.RecordStatus || decoded.DepsCommitted != msg.DepsCommitted {
				t.Fatalf("decoded TryPreAccept = %#v, want protocol fields from %#v", decoded, msg)
			}
		})
	}
}

func TestPrepareExpandsStoredDependencyVectorBeforeResponding(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
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
	rn := optimizedNewRawNode(t, 1, 3)
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

	rn.startTryPreAccept(inst, Attributes{Seq: 9, Deps: []InstanceNum{3, 3, 3}}, map[ReplicaID]struct{}{2: {}})
	if inst.phase != phaseCommitted || inst.rec.Status != StatusCommitted || inst.rec.Seq != committed.Seq || inst.generation != 11 {
		t.Fatalf("TryPreAccept reopened committed instance: phase=%d record=%#v generation=%d", inst.phase, inst.rec, inst.generation)
	}
	if rn.HasReady() {
		t.Fatalf("TryPreAccept on committed instance emitted duplicate effects: %#v", rn.Ready())
	}
}

func TestTryPreAcceptCommittedAndIdempotentFollowerPaths(t *testing.T) {
	t.Run("committed instance replies with commit only", func(t *testing.T) {
		rn := optimizedNewRawNode(t, 2, 3)
		ref := InstanceRef{Replica: 1, Instance: 6, Conf: 1}
		cmd := optimizedTestCommand("already-committed", "already-committed-key")
		optimizedSeedRecord(t, rn, InstanceRecord{Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusCommitted, Seq: 2, Deps: rn.q.deps(), Command: cmd})

		if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: 2, Deps: rn.q.deps(), Command: cmd}); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		commit := optimizedRequireMessage(t, rd.Messages, MsgCommit, 1)
		if commit.Ref != ref || commit.RecordStatus != StatusCommitted || commit.Seq != 2 {
			t.Fatalf("committed TryPreAccept reply = %#v, want committed record", commit)
		}
		optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAcceptResp)
		if len(rd.Records) != 0 {
			t.Fatalf("committed TryPreAccept rewrote durable state: %#v", rd.Records)
		}
	})

	t.Run("duplicate matching preaccept only re-acks", func(t *testing.T) {
		rn := optimizedNewRawNode(t, 2, 3)
		ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
		cmd := optimizedTestCommand("idempotent", "idempotent-key")
		attrs := Attributes{Seq: 3, Deps: []InstanceNum{0, 1, 0}}
		optimizedSeedRecord(t, rn, InstanceRecord{Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Status: StatusPreAccepted, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd})

		if err := rn.Step(Message{Type: MsgTryPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Number: 1, Replica: 1}, Seq: attrs.Seq, Deps: attrs.Deps, Command: cmd}); err != nil {
			t.Fatal(err)
		}
		rd := rn.Ready()
		resp := optimizedRequireMessage(t, rd.Messages, MsgTryPreAcceptResp, 1)
		if resp.Reject || resp.RecordStatus != StatusPreAccepted || resp.Seq != attrs.Seq || len(resp.Deps) != len(attrs.Deps) {
			t.Fatalf("idempotent TryPreAccept response = %#v, want matching preaccept ack", resp)
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
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Reject: true, RejectReason: RejectStaleBallot, RejectHint: hint, Deps: rn.q.deps()}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phasePrepare || inst.rec.Ballot != hint.Next(1) {
			t.Fatalf("stale TryPreAccept rejection phase/ballot = %d/%#v, want prepare at %#v", inst.phase, inst.rec.Ballot, hint.Next(1))
		}
		optimizedRequireMessage(t, rn.Ready().Messages, MsgPrepare, 2)
	})

	t.Run("committed conflict falls back to slow accept", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 1)
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Reject: true, RejectReason: RejectCommittedConflict, Deps: rn.q.deps()}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted || inst.rec.FastPathEligible {
			t.Fatalf("committed-conflict TryPreAccept rejection phase/record = %d/%#v, want non-fast accepted", inst.phase, inst.rec)
		}
		optimizedRequireMessage(t, rn.Ready().Messages, MsgAccept, 2)
	})

	t.Run("older ballot response is ignored", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 3)
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 2, Replica: 1}, Seq: inst.rec.Seq, Deps: inst.rec.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseTryPreAccept || len(inst.tryOK) != 0 {
			t.Fatalf("older-ballot TryPreAccept response changed recovery: phase=%d tryOK=%#v", inst.phase, inst.tryOK)
		}
		if rn.HasReady() {
			t.Fatalf("older-ballot TryPreAccept response emitted effects: %#v", rn.Ready())
		}
	})

	t.Run("nil vote set records first response without quorum", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 1)
		inst.tryOK = nil
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: inst.rec.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseTryPreAccept || len(inst.tryOK) != 1 {
			t.Fatalf("first TryPreAccept response with nil vote set phase/tryOK = %d/%#v, want one recorded vote and no quorum", inst.phase, inst.tryOK)
		}
		if _, ok := inst.tryOK[2]; !ok {
			t.Fatalf("first TryPreAccept response was not recorded: %#v", inst.tryOK)
		}
		if rn.HasReady() {
			t.Fatalf("single TryPreAccept response unexpectedly reached quorum: %#v", rn.Ready())
		}
	})

	t.Run("duplicate response does not count twice", func(t *testing.T) {
		rn, inst, ref := protocolTryInstance(t, 3, 1)
		inst.tryOK = map[ReplicaID]struct{}{2: {}}
		if err := rn.Step(Message{Type: MsgTryPreAcceptResp, From: 2, To: 1, Ref: ref, Ballot: inst.rec.Ballot, Seq: inst.rec.Seq, Deps: inst.rec.Deps, RecordStatus: StatusPreAccepted}); err != nil {
			t.Fatal(err)
		}
		if inst.phase != phaseTryPreAccept || len(inst.tryOK) != 1 {
			t.Fatalf("duplicate TryPreAccept response changed quorum state: phase=%d tryOK=%#v", inst.phase, inst.tryOK)
		}
		if rn.HasReady() {
			t.Fatalf("duplicate TryPreAccept response emitted accept/records: %#v", rn.Ready())
		}
	})
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
		if msg.Ref != ref || msg.RecordStatus != StatusAccepted || msg.Seq != wantSeq || !instanceNumsEqual(msg.Deps, wantDeps) {
			t.Fatalf("committed-conflict slow accept message to %d = %#v, want accepted seq %d deps %v", to, msg, wantSeq, wantDeps)
		}
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("committed-conflict slow accept emitted committed commands: %#v", rd.Committed)
	}
}

func TestPrepareResponsePreservesOriginalAcceptEvidenceSender(t *testing.T) {
	rn := optimizedNewRawNode(t, 2, 3)
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
	rn := optimizedNewRawNode(t, 2, 3)
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
	rn := optimizedNewRawNode(t, 1, 3)
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

	single := optimizedNewRawNode(t, 1, 1)
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
	rn.onTimer(inst, timerTryPreAccept)
	rd := rn.Ready()
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgTryPreAccept, to)
		if msg.Ref != ref || msg.RecordStatus != StatusPreAccepted || msg.FastPathEligible {
			t.Fatalf("TryPreAccept timer message to %d = %#v, want non-fast preaccepted recovery record", to, msg)
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

	rn.onTimer(inst, timerTryPreAccept)
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
		if msg.Ref != ref || msg.RecordStatus != StatusPreAccepted || msg.FastPathEligible {
			t.Fatalf("TryPreAccept timer retry message to %d = %#v, want target preaccepted recovery record", to, msg)
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
		rn.Tick()
		if rn.HasReady() {
			t.Fatalf("rescheduled TryPreAccept retry fired after %d ticks, before deadline: %#v", tick, rn.Ready())
		}
	}

	rn.Tick()
	retry := rn.Ready()
	for _, to := range []ReplicaID{2, 3} {
		msg := optimizedRequireMessage(t, retry.Messages, MsgTryPreAccept, to)
		if msg.Ref != ref || msg.RecordStatus != StatusPreAccepted || msg.FastPathEligible {
			t.Fatalf("rescheduled TryPreAccept retry message to %d = %#v, want target preaccepted recovery record", to, msg)
		}
	}
	optimizedRequireNoMessageType(t, retry.Messages, MsgAccept)
	if len(retry.Records) != 0 || len(retry.Committed) != 0 || retry.MustSync {
		t.Fatalf("rescheduled TryPreAccept retry emitted durable/application effects: %#v", retry)
	}
}

func TestEnsureDependencyRecoveryNoopsForAlreadyResolvedOrInFlightWork(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 3)
	rn.ensureDependencyRecovery(InstanceRef{})
	if rn.HasReady() || len(rn.instances) != 0 {
		t.Fatalf("zero dependency started recovery: ready=%v instances=%#v", rn.HasReady(), rn.instances)
	}

	executedRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	rn.executed[executedRef] = struct{}{}
	rn.ensureDependencyRecovery(executedRef)
	if _, ok := rn.instances[executedRef]; ok || rn.HasReady() {
		t.Fatalf("executed dependency restarted recovery: ready=%v instance=%#v", rn.HasReady(), rn.instances[executedRef])
	}

	committedRef := InstanceRef{Replica: 2, Instance: 2, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{Ref: committedRef, Ballot: Ballot{Number: 1, Replica: 2}, Status: StatusCommitted, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("committed-dep", "committed-dep-key")})
	rn.ensureDependencyRecovery(committedRef)
	if rn.HasReady() {
		t.Fatalf("committed dependency restarted recovery: %#v", rn.Ready())
	}

	preparingRef := InstanceRef{Replica: 2, Instance: 3, Conf: 1}
	preparing := &instance{rec: checkedRecord(InstanceRecord{Ref: preparingRef, Ballot: Ballot{Number: 2, Replica: 1}, Status: StatusNone, Deps: rn.q.deps()}), phase: phasePrepare, generation: 5}
	rn.instances[preparingRef] = preparing
	rn.ensureDependencyRecovery(preparingRef)
	if preparing.phase != phasePrepare || preparing.generation != 5 || rn.HasReady() {
		t.Fatalf("in-flight local prepare was restarted: phase=%d generation=%d ready=%v", preparing.phase, preparing.generation, rn.HasReady())
	}
}

func TestDependencyKnownAfterRequiresBothRecordsAndMinimumStatus(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 3)
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
			{Sender: 1, Seq: 3, Deps: []InstanceNum{2, 0, 0}},
			{Sender: 2, Seq: 4, Deps: []InstanceNum{0, 3, 0}},
		},
	)
	if !acceptEvidenceEqual(merged, []AcceptEvidence{
		{Sender: 1, Seq: 2, Deps: []InstanceNum{1, 0, 0}},
		{Sender: 1, Seq: 3, Deps: []InstanceNum{2, 0, 0}},
		{Sender: 2, Seq: 4, Deps: []InstanceNum{0, 3, 0}},
	}) {
		t.Fatalf("merged sender evidence = %#v", merged)
	}
	if acceptEvidenceEqual([]AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{1}}}, []AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{2}}}) {
		t.Fatal("AcceptEvidence equality ignored dependency differences")
	}
	if acceptEvidenceEqual([]AcceptEvidence{{Sender: 1, Seq: 1, Deps: []InstanceNum{1}}}, []AcceptEvidence{{Sender: 2, Seq: 1, Deps: []InstanceNum{1}}}) {
		t.Fatal("AcceptEvidence equality ignored sender differences")
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
	legacy.Checksum = checksumRecord(legacy, true, true, true, false, false)
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
	rn := optimizedNewRawNode(t, 2, 3)
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
	rn := optimizedNewRawNode(t, 1, 5)
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
	rn := optimizedNewRawNode(t, 1, 3)
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
		tryOK: map[ReplicaID]struct{}{
			1: {},
		},
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
	optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
	optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAccept)
	if rn.tryEvidenceChecks != nil {
		t.Fatalf("stale duplicate evidence left stale checks: %#v", rn.tryEvidenceChecks)
	}
}

func TestPendingEvidenceTimeoutFallsBackToSlowAccept(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 3)
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
		tryOK: map[ReplicaID]struct{}{1: {}},
	}
	rn.instances[target] = inst
	rn.tryEvidenceChecks = map[tryEvidenceKey]map[ReplicaID]InstanceRecord{{target: target, conflict: conflict, ballot: inst.rec.Ballot}: {}}

	rn.onTimer(inst, timerTryPreAccept)
	if inst.phase != phaseAccept || inst.rec.Status != StatusAccepted {
		t.Fatalf("pending evidence timeout phase/status = %d/%s, want slow accept/%s", inst.phase, inst.rec.Status, StatusAccepted)
	}
	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, target)
	if rec.Status != StatusAccepted || rec.FastPathEligible || rec.Deps[1] != conflict.Instance {
		t.Fatalf("timeout fallback record = %#v, want non-fast accept retaining conflict dependency %s", rec, conflict)
	}
	optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
	optimizedRequireNoMessageType(t, rd.Messages, MsgTryPreAccept)
	if rn.tryEvidenceChecks != nil {
		t.Fatalf("timeout fallback left stale evidence checks: %#v", rn.tryEvidenceChecks)
	}
}

func TestTryEvidenceHelperBranchesFailClosedConservatively(t *testing.T) {
	rn := optimizedNewRawNode(t, 1, 3)
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

	single := optimizedNewRawNode(t, 1, 1)
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
	rn := optimizedNewRawNode(t, 1, 5)
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

	rnFive := optimizedNewRawNode(t, 1, 5)
	instFive := &instance{rec: inst.rec, phase: phaseTryPreAccept}
	rnFive.instances[conflict] = &instance{rec: tuple, phase: phaseCommitted}
	oneUsable := tuple.Clone()
	oneUsable.AcceptEvidence = []AcceptEvidence{{Sender: 2, Seq: 4, Deps: coveringDeps}}
	if authorized, failClosed := rnFive.tryEvidenceDecision(instFive, key, map[ReplicaID]InstanceRecord{2: oneUsable, 3: {Ref: conflict, Status: StatusNone}, 4: {Ref: conflict, Status: StatusNone}, 5: {Ref: conflict, Status: StatusNone}}); authorized || !failClosed {
		t.Fatalf("all-remote evidence below f decision = authorized %v failClosed %v, want fail closed", authorized, failClosed)
	}

	inst.prepareOK = map[ReplicaID]InstanceRecord{
		4: {Ref: target, Status: StatusPreAccepted, FastPathEligible: false},
		5: {Ref: target, Status: StatusPreAccepted, FastPathEligible: false},
	}

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
	rn := optimizedNewRawNode(t, 1, 3)
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
	rn := optimizedNewRawNode(t, 1, 3)
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

	if rn.canCountInitialLeaderForTry(nil) {
		t.Fatal("nil TryPreAccept instance counted initial leader")
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
	if first.IgnoreDependency.Ref != (InstanceRef{Replica: 2, Instance: 9, Conf: 1}) {
		t.Fatalf("first ignored dependency broadcast = %#v, want refs sorted by replica", first.IgnoreDependency.Ref)
	}
	secondSeen := false
	for _, msg := range rd.Messages {
		if msg.Type == MsgTryPreAccept && msg.IgnoreDependency.Ref == (InstanceRef{Replica: 3, Instance: 1, Conf: 1}) {
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
		rn := optimizedNewRawNode(t, 1, 3)
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

	rn := optimizedNewRawNode(t, 1, 3)
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
	rn := optimizedNewRawNode(t, 1, 3)
	ref := InstanceRef{Replica: 2, Instance: 90, Conf: 1}
	msg := Message{Type: MsgCommit, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}, Seq: 1, Deps: rn.q.deps(), Command: optimizedTestCommand("zero-record-ballot", "zero-record-ballot-key")}
	if err := rn.Step(msg); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() || rn.instances[ref] != nil {
		t.Fatalf("commit without RecordBallot changed state: ready=%v instance=%#v", rn.HasReady(), rn.instances[ref])
	}

	recoveryRef := InstanceRef{Replica: 1, Instance: 91, Conf: 1}
	recovery := &instance{rec: checkedRecord(InstanceRecord{Ref: recoveryRef, Ballot: Ballot{Number: 2, Replica: 1}, Status: StatusNone, Deps: rn.q.deps()}), phase: phasePrepare, prepareOK: map[ReplicaID]InstanceRecord{}}
	rn.instances[recoveryRef] = recovery
	resp := Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: recoveryRef, Ballot: recovery.rec.Ballot, RecordStatus: StatusCommitted, Seq: 2, Deps: rn.q.deps(), Command: optimizedTestCommand("prepare-zero-record-ballot", "zero-record-ballot-key")}
	if err := rn.Step(resp); err != nil {
		t.Fatal(err)
	}
	if len(recovery.prepareOK) != 0 || rn.HasReady() {
		t.Fatalf("prepare response without RecordBallot was accepted: prepareOK=%#v ready=%v", recovery.prepareOK, rn.HasReady())
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
	rn := optimizedNewRawNode(t, 1, voters)
	ref := InstanceRef{Replica: 1, Instance: InstanceNum(20 + ballotNumber), Conf: 1}
	rec := checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Number: ballotNumber, Replica: 1},
		Status:  StatusPreAccepted,
		Seq:     1,
		Deps:    rn.q.deps(),
		Command: optimizedTestCommand("try-response", "try-response-key"),
	})
	inst := &instance{rec: rec, phase: phaseTryPreAccept, tryOK: map[ReplicaID]struct{}{}}
	rn.instances[ref] = inst
	return rn, inst, ref
}
