package epaxos

import (
	"errors"
	"reflect"
	"testing"
)

func recoveryBoundaryNode(t *testing.T, id ReplicaID, voters int, storage Storage) *RawNode {
	t.Helper()
	rn, err := NewRawNode(Config{
		ID:            id,
		Voters:        makeIDs(voters),
		Storage:       storage,
		RetryTicks:    2,
		RecoveryTicks: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

func cloneRecoveryBoundaryInstance(inst *instance) instance {
	out := *inst
	out.rec = inst.rec.Clone()
	if inst.preOK != nil {
		out.preOK = make(map[ReplicaID]attrVote, len(inst.preOK))
		for id, vote := range inst.preOK {
			vote.deps = append([]InstanceNum(nil), vote.deps...)
			out.preOK[id] = vote
		}
	}
	if inst.accOK != nil {
		out.accOK = make(map[ReplicaID]struct{}, len(inst.accOK))
		for id := range inst.accOK {
			out.accOK[id] = struct{}{}
		}
	}
	cloneRecords := func(records map[ReplicaID]InstanceRecord) map[ReplicaID]InstanceRecord {
		if records == nil {
			return nil
		}
		cloned := make(map[ReplicaID]InstanceRecord, len(records))
		for id, rec := range records {
			cloned[id] = rec.Clone()
		}
		return cloned
	}
	out.prepareOK = cloneRecords(inst.prepareOK)
	out.prepareEvidence = cloneRecords(inst.prepareEvidence)
	if inst.tryOK != nil {
		out.tryOK = make(map[ReplicaID]struct{}, len(inst.tryOK))
		for id := range inst.tryOK {
			out.tryOK[id] = struct{}{}
		}
	}
	if inst.tryDeferred != nil {
		out.tryDeferred = make(map[InstanceRef]struct{}, len(inst.tryDeferred))
		for ref := range inst.tryDeferred {
			out.tryDeferred[ref] = struct{}{}
		}
	}
	if inst.tryIgnored != nil {
		out.tryIgnored = make(map[InstanceRef]struct{}, len(inst.tryIgnored))
		for ref := range inst.tryIgnored {
			out.tryIgnored[ref] = struct{}{}
		}
	}
	return out
}

type recoveryBoundarySnapshot struct {
	tick             uint64
	currentHardState HardState
	instance         instance
	timers           timerHeap
	pendingReady     Ready
	nextReady        Ready
	frozenReady      Ready
	awaitAdvance     bool
	delayed          []Message
}

func snapshotRecoveryBoundary(rn *RawNode, inst *instance) recoveryBoundarySnapshot {
	delayed := make([]Message, 0, len(rn.deferredIndex))
	for _, entry := range rn.logicalPreAccepts {
		delayed = append(delayed, entry.message.Clone())
	}
	for _, entry := range rn.toqPreAccepts {
		delayed = append(delayed, entry.message.Clone())
	}
	return recoveryBoundarySnapshot{
		tick:             rn.tick,
		currentHardState: rn.currentHardState.Clone(),
		instance:         cloneRecoveryBoundaryInstance(inst),
		timers:           append(timerHeap(nil), rn.timers...),
		pendingReady:     rn.pendingReady.Clone(),
		nextReady:        rn.nextReady.Clone(),
		frozenReady:      rn.frozenReady.Clone(),
		awaitAdvance:     rn.awaitAdvance,
		delayed:          delayed,
	}
}

func TestBallotNextCheckedBoundaries(t *testing.T) {
	tests := []struct {
		name string
		from Ballot
		want Ballot
	}{
		{
			name: "normal increment",
			from: Ballot{Epoch: 3, Number: 8, Replica: 2},
			want: Ballot{Epoch: 3, Number: 9, Replica: 1},
		},
		{
			name: "number carry",
			from: Ballot{Epoch: 3, Number: ^uint64(0), Replica: 2},
			want: Ballot{Epoch: 4, Number: 0, Replica: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.from.Next(1)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want || !tt.from.Less(got) || got == (Ballot{}) || got.IsInitialFor(1) {
				t.Fatalf("Next(%#v) = %#v, want %#v strictly greater and non-initial", tt.from, got, tt.want)
			}
		})
	}

	max := Ballot{Epoch: ^uint64(0), Number: ^uint64(0), Replica: 2}
	got, err := max.Next(1)
	if !errors.Is(err, ErrBallotExhausted) || got != (Ballot{}) {
		t.Fatalf("absolute exhaustion = %#v, %v, want zero, %v", got, err, ErrBallotExhausted)
	}
}

func recoveryBoundaryRejectFixture(t *testing.T, typ MessageType, hint Ballot) (*RawNode, *instance, Message) {
	t.Helper()
	rn := recoveryBoundaryNode(t, 1, 3, nil)
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	cmd := Command{ID: CommandID{Client: 501, Sequence: 1}, Payload: []byte("reject-boundary"), ConflictKeys: [][]byte{[]byte("reject-boundary-key")}}
	rec := checkedRecord(InstanceRecord{
		Ref:          ref,
		Ballot:       Ballot{Number: 1, Replica: 1},
		RecordBallot: Ballot{Number: 1, Replica: 1},
		Status:       StatusPreAccepted,
		Seq:          1,
		Deps:         rn.q.deps(),
		Command:      cmd,
	})
	inst := &instance{rec: rec}
	switch typ {
	case MsgPreAcceptResp:
		inst.phase = phasePreAccept
	case MsgAcceptResp:
		inst.phase = phaseAccept
		inst.rec.Status = StatusAccepted
		inst.rec.Checksum = ChecksumRecord(inst.rec)
	case MsgPrepareResp:
		inst.phase = phasePrepare
		inst.prepareOK = map[ReplicaID]InstanceRecord{1: inst.rec.Clone()}
	default:
		t.Fatalf("unsupported reject fixture %s", typ)
	}
	rn.instances[ref] = inst
	msg := Message{
		Type:       typ,
		From:       2,
		To:         1,
		Ref:        ref,
		Ballot:     hint,
		Deps:       rn.q.deps(),
		Reject:     true,
		RejectHint: hint,
	}
	return rn, inst, msg
}

func TestRejectPathsIncrementObservedBallotExactlyOnce(t *testing.T) {
	for _, typ := range []MessageType{MsgPreAcceptResp, MsgAcceptResp, MsgPrepareResp} {
		t.Run(typ.String(), func(t *testing.T) {
			hint := Ballot{Epoch: 4, Number: 9, Replica: 2}
			rn, inst, msg := recoveryBoundaryRejectFixture(t, typ, hint)
			want, err := hint.Next(1)
			if err != nil {
				t.Fatal(err)
			}
			if err := rn.Step(msg); err != nil {
				t.Fatal(err)
			}
			if inst.phase != phasePrepare || inst.rec.Ballot != want {
				t.Fatalf("phase/ballot after reject = %d/%#v, want Prepare/%#v", inst.phase, inst.rec.Ballot, want)
			}
		})
	}
}

func TestStepBallotExhaustionIsTypedAndHasNoEffect(t *testing.T) {
	max := Ballot{Epoch: ^uint64(0), Number: ^uint64(0), Replica: 2}
	for _, typ := range []MessageType{MsgPreAcceptResp, MsgAcceptResp, MsgPrepareResp} {
		t.Run(typ.String(), func(t *testing.T) {
			rn, inst, msg := recoveryBoundaryRejectFixture(t, typ, max)
			before := snapshotRecoveryBoundary(rn, inst)
			err := rn.Step(msg)
			if !errors.Is(err, ErrBallotExhausted) {
				t.Fatalf("Step error = %v, want %v", err, ErrBallotExhausted)
			}
			after := snapshotRecoveryBoundary(rn, inst)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("Step mutated exhausted recovery\nbefore: %#v\nafter:  %#v", before, after)
			}
		})
	}
	t.Run("accepted TryPreAccept response", func(t *testing.T) {
		rn := recoveryBoundaryNode(t, 1, 3, nil)
		ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
		maxTry := Ballot{Epoch: ^uint64(0), Number: ^uint64(0), Replica: 1}
		inst := &instance{rec: checkedRecord(InstanceRecord{
			Ref:          ref,
			Ballot:       maxTry,
			RecordBallot: maxTry,
			Status:       StatusPreAccepted,
			Seq:          1,
			Deps:         rn.q.deps(),
			Command:      Command{Kind: CommandNoop},
		}), phase: phaseTryPreAccept}
		rn.instances[ref] = inst
		msg := Message{
			Type:         MsgTryPreAcceptResp,
			From:         2,
			To:           1,
			Ref:          ref,
			Ballot:       maxTry,
			RecordBallot: Ballot{Replica: ref.Replica},
			Seq:          1,
			Deps:         rn.q.deps(),
			Command:      Command{Kind: CommandNoop},
			Reject:       true,
			RejectReason: RejectAcceptedTarget,
			RecordStatus: StatusAccepted,
		}
		before := snapshotRecoveryBoundary(rn, inst)
		err := rn.Step(msg)
		if !errors.Is(err, ErrBallotExhausted) {
			t.Fatalf("Step error = %v, want %v", err, ErrBallotExhausted)
		}
		after := snapshotRecoveryBoundary(rn, inst)
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("Step mutated exhausted Accepted-target recovery\nbefore: %#v\nafter:  %#v", before, after)
		}
	})
}

func TestTickBallotExhaustionStuttersWithoutEffects(t *testing.T) {
	rn := recoveryBoundaryNode(t, 2, 3, nil)
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	inst := &instance{rec: checkedRecord(InstanceRecord{
		Ref:          ref,
		Ballot:       Ballot{Epoch: ^uint64(0), Number: ^uint64(0), Replica: 3},
		RecordBallot: Ballot{Replica: 1},
		Status:       StatusPreAccepted,
		Seq:          1,
		Deps:         rn.q.deps(),
		Command:      Command{Kind: CommandNoop},
	}), phase: phaseIdle}
	rn.instances[ref] = inst
	rn.schedule(inst, timerPrepare, 1)
	before := snapshotRecoveryBoundary(rn, inst)

	err := rn.Tick()
	if !errors.Is(err, ErrBallotExhausted) {
		t.Fatalf("Tick error = %v, want %v", err, ErrBallotExhausted)
	}
	after := snapshotRecoveryBoundary(rn, inst)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("Tick mutated exhausted recovery\nbefore: %#v\nafter:  %#v", before, after)
	}
}

func TestEpochCarryBallotIsRecoveryAcrossValidationRestartAndFastGuard(t *testing.T) {
	ballot := Ballot{Epoch: 1, Number: 0, Replica: 1}
	if !ballot.IsRecovery() || ballot.IsInitialFor(1) {
		t.Fatalf("carried ballot classified as initial: %#v", ballot)
	}
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	prepare := Message{Type: MsgPrepare, From: 1, To: 2, Ref: ref, Ballot: ballot}
	if err := prepare.Validate(conf); err != nil {
		t.Fatalf("carried Prepare rejected: %v", err)
	}

	store := NewMemoryStorage()
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:              ref,
		Ballot:           ballot,
		RecordBallot:     Ballot{Replica: ref.Replica},
		Status:           StatusPreAccepted,
		Seq:              1,
		Deps:             make([]InstanceNum, 3),
		Command:          Command{Kind: CommandNoop},
		FastPathEligible: true,
	})
	restarted := recoveryBoundaryNode(t, 1, 3, store)
	inst := restarted.instances[ref]
	if inst == nil || inst.phase != phasePrepare || len(inst.prepareOK) != 1 {
		t.Fatalf("carried recovery restart = %#v, want Prepare with durable self response", inst)
	}
	if restarted.canStillFastCommitPreAccept(inst) {
		t.Fatal("carried recovery ballot passed initial fast-path guard")
	}

	follower := recoveryBoundaryNode(t, 3, 3, nil)
	preAccept := Message{
		Type:    MsgPreAccept,
		From:    1,
		To:      3,
		Ref:     ref,
		Ballot:  ballot,
		Seq:     1,
		Deps:    follower.q.deps(),
		Command: Command{Kind: CommandNoop},
	}
	if err := follower.Step(preAccept); err != nil {
		t.Fatalf("carried recovery PreAccept rejected: %v", err)
	}
	if got := follower.instances[ref].rec; got.FastPathEligible || got.Ballot != ballot {
		t.Fatalf("carried recovery PreAccept record = %#v, want non-fast recovery", got)
	}
}

func TestRestartReconstructsDurablePrepareSelfVoteAtMaximumFailures(t *testing.T) {
	for _, voters := range []int{3, 5, 7} {
		t.Run(string(rune('0'+voters))+" voters", func(t *testing.T) {
			store := NewMemoryStorage()
			rn := recoveryBoundaryNode(t, 1, voters, store)
			ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
			cmd := Command{ID: CommandID{Client: 600 + uint64(voters), Sequence: 1}, Payload: []byte("restart-prepare"), ConflictKeys: [][]byte{[]byte("restart-prepare-key")}}
			inst := &instance{rec: checkedRecord(InstanceRecord{
				Ref:          ref,
				Ballot:       Ballot{Replica: ref.Replica},
				RecordBallot: Ballot{Replica: ref.Replica},
				Status:       StatusPreAccepted,
				Seq:          1,
				Deps:         rn.q.deps(),
				Command:      cmd,
			}), phase: phaseIdle}
			rn.instances[ref] = inst
			if err := rn.startPrepare(inst); err != nil {
				t.Fatal(err)
			}
			startedBallot := inst.rec.Ballot
			rd := rn.Ready()
			if err := store.ApplyReady(rd); err != nil {
				t.Fatal(err)
			}
			if err := rn.Advance(rd); err != nil {
				t.Fatal(err)
			}

			restarted := recoveryBoundaryNode(t, 1, voters, store)
			recovered := restarted.instances[ref]
			if recovered == nil || recovered.phase != phasePrepare || recovered.rec.Ballot != startedBallot {
				t.Fatalf("restart phase/ballot = %#v, want Prepare/%#v", recovered, startedBallot)
			}
			self, ok := recovered.prepareOK[1]
			if !ok || self.Ballot != startedBallot || self.RecordBallot != (Ballot{Replica: ref.Replica}) || self.Status != StatusPreAccepted || !commandEqual(self.Command, cmd) {
				t.Fatalf("reconstructed self Prepare response = %#v", self)
			}

			failures := (voters - 1) / 2
			for offset := range failures {
				from := ReplicaID(2 + offset)
				if err := restarted.Step(Message{
					Type:         MsgPrepareResp,
					From:         from,
					To:           1,
					Ref:          ref,
					Ballot:       startedBallot,
					Deps:         restarted.q.deps(),
					RecordStatus: StatusNone,
				}); err != nil {
					t.Fatalf("Prepare response from %d: %v", from, err)
				}
			}
			if recovered.phase != phaseAccept || recovered.rec.Status != StatusAccepted || !commandEqual(recovered.rec.Command, cmd) {
				t.Fatalf("recovery after maximum failures = phase %d record %#v", recovered.phase, recovered.rec)
			}
			for offset := range failures {
				from := ReplicaID(2 + offset)
				if err := restarted.Step(Message{
					Type:         MsgAcceptResp,
					From:         from,
					To:           1,
					Ref:          ref,
					Ballot:       recovered.rec.Ballot,
					RecordBallot: recovered.rec.RecordBallot,
					Seq:           recovered.rec.Seq,
					Deps:          append([]InstanceNum(nil), recovered.rec.Deps...),
					RecordStatus: StatusAccepted,
				}); err != nil {
					t.Fatalf("Accept response from %d: %v", from, err)
				}
			}
			if recovered.phase != phaseCommitted || recovered.rec.Status < StatusCommitted {
				t.Fatalf("recovery did not commit with %d failures: phase %d status %s", failures, recovered.phase, recovered.rec.Status)
			}
		})
	}
}

func findRecoveryBoundaryMessage(t *testing.T, messages []Message, typ MessageType, to ReplicaID) Message {
	t.Helper()
	for _, msg := range messages {
		if msg.Type == typ && msg.To == to {
			return msg
		}
	}
	t.Fatalf("missing %s to %d in %#v", typ, to, messages)
	return Message{}
}

func TestTryPreAcceptAcceptedTargetRestartsPrepareAndCompletes(t *testing.T) {
	for _, voters := range []int{3, 5, 7} {
		for _, same := range []bool{true, false} {
			name := "different tuple"
			if same {
				name = "same tuple"
			}
			t.Run(string(rune('0'+voters))+" voters/"+name, func(t *testing.T) {
				coordinator := recoveryBoundaryNode(t, 1, voters, nil)
				follower := recoveryBoundaryNode(t, 2, voters, nil)
				ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
				tryBallot := Ballot{Number: 1, Replica: 1}
				candidate := Command{ID: CommandID{Client: 701, Sequence: 1}, Payload: []byte("candidate"), ConflictKeys: [][]byte{[]byte("accepted-target-key")}}
				accepted := candidate.Clone()
				if !same {
					accepted = Command{ID: CommandID{Client: 702, Sequence: 1}, Payload: []byte("accepted"), ConflictKeys: [][]byte{[]byte("accepted-target-key")}}
				}
				candidateDeps := coordinator.q.deps()
				candidateInst := &instance{rec: checkedRecord(InstanceRecord{
					Ref:              ref,
					Ballot:           tryBallot,
					RecordBallot:     tryBallot,
					Status:           StatusPreAccepted,
					Seq:              3,
					Deps:             candidateDeps,
					Command:          candidate,
					FastPathEligible: false,
				}), phase: phaseTryPreAccept, tryOK: map[ReplicaID]struct{}{1: {}}}
				coordinator.instances[ref] = candidateInst

				acceptedDeps := follower.q.deps()
				acceptedDeps[0] = 1
				acceptedRecordBallot := Ballot{Replica: ref.Replica}
				acceptedRec := checkedRecord(InstanceRecord{
					Ref:            ref,
					Ballot:         acceptedRecordBallot,
					RecordBallot:   acceptedRecordBallot,
					Status:         StatusAccepted,
					Seq:            7,
					Deps:           acceptedDeps,
					AcceptSeq:      8,
					AcceptDeps:     append([]InstanceNum(nil), acceptedDeps...),
					AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 8, Deps: append([]InstanceNum(nil), acceptedDeps...)}},
					Command:        accepted,
				})
				acceptedInst := &instance{rec: acceptedRec, phase: phaseIdle}
				follower.instances[ref] = acceptedInst

				if err := follower.Step(Message{
					Type:    MsgTryPreAccept,
					From:    1,
					To:      2,
					Ref:     ref,
					Ballot:  tryBallot,
					Seq:     candidateInst.rec.Seq,
					Deps:    append([]InstanceNum(nil), candidateInst.rec.Deps...),
					Command: candidate,
				}); err != nil {
					t.Fatal(err)
				}
				if acceptedInst.rec.Status != StatusAccepted ||
					acceptedInst.rec.RecordBallot != acceptedRecordBallot ||
					acceptedInst.rec.Seq != acceptedRec.Seq ||
					!instanceNumsEqual(acceptedInst.rec.Deps, acceptedRec.Deps) ||
					!commandEqual(acceptedInst.rec.Command, accepted) ||
					acceptedInst.rec.Ballot != tryBallot {
					t.Fatalf("TryPreAccept overwrote Accepted target: before %#v after %#v", acceptedRec, acceptedInst.rec)
				}
				followerReady := follower.Ready()
				resp := findRecoveryBoundaryMessage(t, followerReady.Messages, MsgTryPreAcceptResp, 1)
				if !resp.Reject || resp.RejectReason != RejectAcceptedTarget || resp.Ballot != tryBallot ||
					resp.RejectHint != (Ballot{}) || resp.ConflictRef != (InstanceRef{}) || resp.ConflictStatus != StatusNone ||
					resp.RecordBallot != acceptedRecordBallot || resp.RecordStatus != StatusAccepted || resp.Seq != acceptedRec.Seq ||
					!instanceNumsEqual(resp.Deps, acceptedRec.Deps) || !commandEqual(resp.Command, accepted) ||
					resp.AcceptSeq != acceptedRec.AcceptSeq || !instanceNumsEqual(resp.AcceptDeps, acceptedRec.AcceptDeps) ||
					!acceptEvidenceEqual(resp.AcceptEvidence, acceptedRec.AcceptEvidence) {
					t.Fatalf("Accepted-target rejection is not canonical: %#v", resp)
				}
				if err := resp.Validate(coordinator.q.conf); err != nil {
					t.Fatalf("canonical Accepted-target rejection invalid: %v", err)
				}

				if err := coordinator.Step(resp); err != nil {
					t.Fatal(err)
				}
				wantPrepare, err := tryBallot.Next(1)
				if err != nil {
					t.Fatal(err)
				}
				if candidateInst.phase != phasePrepare || candidateInst.rec.Ballot != wantPrepare || len(candidateInst.prepareOK) != 1 {
					t.Fatalf("Accepted-target response phase/ballot/votes = %d/%#v/%#v", candidateInst.phase, candidateInst.rec.Ballot, candidateInst.prepareOK)
				}
				if evidence, ok := candidateInst.prepareEvidence[2]; !ok || evidence.Status != StatusAccepted || evidence.RecordBallot != acceptedRecordBallot || !commandEqual(evidence.Command, accepted) {
					t.Fatalf("Accepted-target evidence not retained outside Prepare quorum: %#v", candidateInst.prepareEvidence)
				}
				beforeDuplicate := snapshotRecoveryBoundary(coordinator, candidateInst)
				if err := coordinator.Step(resp); err != nil {
					t.Fatal(err)
				}
				if afterDuplicate := snapshotRecoveryBoundary(coordinator, candidateInst); !reflect.DeepEqual(afterDuplicate, beforeDuplicate) {
					t.Fatalf("duplicate Accepted-target rejection changed coordinator\nbefore: %#v\nafter:  %#v", beforeDuplicate, afterDuplicate)
				}

				failures := (voters - 1) / 2
				for offset := range failures {
					from := ReplicaID(3 + offset)
					if err := coordinator.Step(Message{
						Type:         MsgPrepareResp,
						From:         from,
						To:           1,
						Ref:          ref,
						Ballot:       wantPrepare,
						Deps:         coordinator.q.deps(),
						RecordStatus: StatusNone,
					}); err != nil {
						t.Fatalf("Prepare response from surviving replica %d: %v", from, err)
					}
				}
				if candidateInst.phase != phaseAccept || candidateInst.rec.Status != StatusAccepted ||
					!commandEqual(candidateInst.rec.Command, accepted) || candidateInst.rec.Seq != acceptedRec.Seq ||
					!instanceNumsEqual(candidateInst.rec.Deps, acceptedRec.Deps) {
					t.Fatalf("higher Prepare did not adopt Accepted target: phase %d record %#v", candidateInst.phase, candidateInst.rec)
				}
				for offset := range failures {
					from := ReplicaID(3 + offset)
					if err := coordinator.Step(Message{
						Type:         MsgAcceptResp,
						From:         from,
						To:           1,
						Ref:          ref,
						Ballot:       candidateInst.rec.Ballot,
						RecordBallot: candidateInst.rec.RecordBallot,
						Seq:           candidateInst.rec.Seq,
						Deps:          append([]InstanceNum(nil), candidateInst.rec.Deps...),
						RecordStatus: StatusAccepted,
					}); err != nil {
						t.Fatalf("Accept response from surviving replica %d: %v", from, err)
					}
				}
				if candidateInst.phase != phaseCommitted || candidateInst.rec.Status < StatusCommitted {
					t.Fatalf("Accepted-target recovery did not commit with %d failures: phase %d status %s", failures, candidateInst.phase, candidateInst.rec.Status)
				}
			})
		}
	}
}

func TestAcceptedTargetRejectValidationIsExact(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	ballot := Ballot{Number: 2, Replica: 1}
	canonical := Message{
		Type:           MsgTryPreAcceptResp,
		From:           2,
		To:             1,
		Ref:            ref,
		Ballot:         ballot,
		RecordBallot:   Ballot{Replica: 2},
		Seq:            3,
		Deps:           []InstanceNum{0, 1, 0},
		AcceptSeq:      4,
		AcceptDeps:     []InstanceNum{0, 1, 0},
		AcceptEvidence: []AcceptEvidence{{Sender: 2, Seq: 4, Deps: []InstanceNum{0, 1, 0}}},
		Command:        Command{ID: CommandID{Client: 800, Sequence: 1}, Payload: []byte("accepted")},
		Reject:         true,
		RejectReason:   RejectAcceptedTarget,
		RecordStatus:   StatusAccepted,
	}
	if err := canonical.Validate(conf); err != nil {
		t.Fatalf("canonical rejection invalid: %v", err)
	}
	encoded, err := EncodeMessage(nil, canonical)
	if err != nil {
		t.Fatalf("encode canonical rejection: %v", err)
	}
	var decoded Message
	if err := DecodeMessage(encoded, &decoded); err != nil {
		t.Fatalf("decode canonical rejection: %v", err)
	}
	wireWant := canonical.Clone()
	wireWant.Checksum = ChecksumMessage(wireWant)
	if !messageEqual(decoded, wireWant) {
		t.Fatalf("Accepted-target rejection wire round trip = %#v, want %#v", decoded, wireWant)
	}

	mutations := []func(*Message){
		func(m *Message) { m.RejectHint = m.Ballot },
		func(m *Message) { m.RecordBallot = Ballot{} },
		func(m *Message) { m.RecordStatus = StatusPreAccepted },
		func(m *Message) { m.Seq = 0 },
		func(m *Message) { m.Deps = m.Deps[:2] },
		func(m *Message) { m.To = 2 },
		func(m *Message) { m.Ballot = Ballot{Replica: 1} },
		func(m *Message) {
			m.RejectReason = RejectStaleBallot
			m.RejectHint = m.Ballot
		},
		func(m *Message) {
			m.RejectReason = RejectCommittedConflict
			m.ConflictRef = InstanceRef{Replica: 3, Instance: 1, Conf: 1}
			m.ConflictStatus = StatusCommitted
		},
	}
	for i, mutate := range mutations {
		bad := canonical.Clone()
		mutate(&bad)
		if err := bad.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("mutation %d error = %v, want %v: %#v", i, err, ErrInvalidMessage, bad)
		}
	}
}
