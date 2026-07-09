package epaxos

import (
	"bytes"
	"errors"
	"testing"
)

func TestDependencyClosureStartsPrefixRecoveryBeforeExecution(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}

	dependentRef := InstanceRef{Replica: 1, Instance: 3, Conf: 1}
	foreignPrefixRefs := []InstanceRef{
		{Replica: 2, Instance: 1, Conf: 1},
		{Replica: 2, Instance: 2, Conf: 1},
	}
	dependent := Command{ID: CommandID{Client: 10, Sequence: 1}, Payload: []byte("dependent"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{
		Type:         MsgCommit,
		From:         2,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 2},
		RecordBallot: Ballot{Replica: 2},
		Seq:          3,
		Deps:         []InstanceNum{0, 2, 0},
		Command:      dependent,
		RecordStatus: StatusCommitted,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatal(err)
	}
	recoveryRequireDependencyRefs(t, rn.dependencyRefs(dependentRef), foreignPrefixRefs)

	rd := rn.Ready()
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before dependency closure: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryRequirePrepareRefs(t, rd.Messages, foreignPrefixRefs[:1])
	recoveryRequireNoPrepareRefs(t, rd.Messages, []InstanceRef{
		{Replica: 1, Instance: 1, Conf: 1},
		{Replica: 1, Instance: 2, Conf: 1},
	})
}

func TestForeignMissingDependencyRecoveryNoopsAndReleasesDependent(t *testing.T) {
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
	if err != nil {
		t.Fatal(err)
	}

	missingRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	dependentRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	dependent := Command{ID: CommandID{Client: 20, Sequence: 1}, Payload: []byte("after-noop"), ConflictKeys: [][]byte{[]byte("k")}}
	commit := Message{
		Type:         MsgCommit,
		From:         3,
		To:           1,
		Ref:          dependentRef,
		Ballot:       Ballot{Replica: 3},
		RecordBallot: Ballot{Replica: 3},
		Seq:          2,
		Deps:         []InstanceNum{0, 1, 0},
		Command:      dependent,
		RecordStatus: StatusCommitted,
	}
	if err := rn.Step(commit); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before missing dependency recovery: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryRequirePrepareRefs(t, rd.Messages, []InstanceRef{missingRef})
	recoveryApplyReady(t, store, rn, rd)

	ballot := Ballot{Number: 1, Replica: 1}
	resp := Message{Type: MsgPrepareResp, From: 2, To: 1, Ref: missingRef, Ballot: ballot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0}}
	if err := rn.Step(resp); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	recoveryRequireRecord(t, rd.Records, missingRef, StatusAccepted, CommandNoop)
	recoveryRequireMessages(t, rd.Messages, MsgAccept, missingRef, CommandNoop)
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
		t.Fatalf("dependent command executed before noop accept quorum: count=%d committed=%#v", got, rd.Committed)
	}
	recoveryApplyReady(t, store, rn, rd)

	acceptResp := Message{Type: MsgAcceptResp, From: 2, To: 1, Ref: missingRef, Ballot: ballot, Seq: 1, Deps: []InstanceNum{0, 0, 0}, RecordStatus: StatusAccepted, Command: Command{Kind: CommandNoop}}
	if err := rn.Step(acceptResp); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	recoveryRequireRecord(t, rd.Records, missingRef, StatusExecuted, CommandNoop)
	if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 1 {
		t.Fatalf("dependent command release count=%d, want 1; committed=%#v", got, rd.Committed)
	}
	for _, c := range rd.Committed {
		if c.Ref == missingRef || c.Command.Kind == CommandNoop {
			t.Fatalf("noop recovery emitted application command: %#v", rd.Committed)
		}
	}
	recoveryApplyReady(t, store, rn, rd)

	if rn.HasReady() {
		rd = rn.Ready()
		if got := recoveryCommittedPayloadCount(rd.Committed, dependentRef, dependent.Payload); got != 0 {
			t.Fatalf("dependent command emitted again after recovery: count=%d ready=%#v", got, rd)
		}
		recoveryApplyReady(t, store, rn, rd)
	}
}

func TestRestartResumesForeignRecoveryBallot(t *testing.T) {
	for _, tt := range []struct {
		name string
		st   Status
		cmd  Command
	}{
		{name: "none", st: StatusNone, cmd: Command{Kind: CommandNoop}},
		{name: "accepted", st: StatusAccepted, cmd: Command{Kind: CommandNoop}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStorage()
			ref := InstanceRef{Replica: 2, Instance: 7, Conf: 1}
			store.Records[ref] = checkedRecord(InstanceRecord{
				Ref:     ref,
				Ballot:  Ballot{Number: 1, Replica: 1},
				Status:  tt.st,
				Seq:     1,
				Deps:    []InstanceNum{0, 0, 0},
				Command: tt.cmd,
			})
			restarted, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store, RetryTicks: 2, RecoveryTicks: 5})
			if err != nil {
				t.Fatal(err)
			}
			if restarted.HasReady() {
				t.Fatalf("restart emitted work before recovery deadline: %#v", restarted.Ready())
			}
			for tick := 1; tick < 5; tick++ {
				restarted.Tick()
				if restarted.HasReady() {
					t.Fatalf("recovery fired after %d ticks, before deadline", tick)
				}
			}
			restarted.Tick()
			rd := restarted.Ready()
			recoveryRequirePrepareRefs(t, rd.Messages, []InstanceRef{ref})
			for _, m := range rd.Messages {
				if m.Type != MsgPrepare || m.Ref != ref {
					continue
				}
				if m.From != 1 || (m.To != 2 && m.To != 3) {
					t.Fatalf("foreign recovery prepare = %#v, want from local node to peers", m)
				}
			}
		})
	}
}

func TestOldConfigRecoveryUsesPinnedVotersAfterRemoval(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}, Storage: store, RetryTicks: 2, RecoveryTicks: 4})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery fired after %d ticks, before deadline", tick)
		}
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargets(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3, 4})
	recoveryApplyReady(t, store, restarted, rd)

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery reached slow quorum without removed voter 4: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted duplicate voter 3 prepare response as removed voter 4: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargets(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3, 4})
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	recoveryApplyReady(t, store, restarted, rd)

	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 4)
	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery committed without removed voter 4 accept response: %#v", restarted.Ready())
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted duplicate voter 3 accept response as removed voter 4: %#v", restarted.Ready())
	}
	if err := restarted.Step(acceptResp(4)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargets(t, rd.Messages, MsgCommit, ref, []ReplicaID{1, 3, 4})
	recoveryRequireRecord(t, rd.Records, ref, StatusExecuted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestOldConfigRecoveryUsesPinnedVotersAfterAddition(t *testing.T) {
	store := NewMemoryStorage()
	store.Configs = []ConfState{{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}}}
	ref := InstanceRef{Replica: 1, Instance: 7, Conf: 1}
	store.Records[ref] = checkedRecord(InstanceRecord{
		Ref:     ref,
		Ballot:  Ballot{Replica: 1},
		Status:  StatusAccepted,
		Seq:     1,
		Deps:    []InstanceNum{0, 0, 0},
		Command: Command{Kind: CommandNoop},
	})

	restarted, err := NewRawNode(Config{ID: 2, Voters: []ReplicaID{1, 2, 3}, Storage: store, RetryTicks: 2, RecoveryTicks: 4})
	if err != nil {
		t.Fatal(err)
	}
	assertConfState(t, restarted.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}})
	if restarted.HasReady() {
		t.Fatalf("restart emitted work before old-config recovery deadline: %#v", restarted.Ready())
	}
	for tick := 1; tick < 4; tick++ {
		restarted.Tick()
		if restarted.HasReady() {
			t.Fatalf("old-config recovery fired after %d ticks, before deadline", tick)
		}
	}

	restarted.Tick()
	rd := restarted.Ready()
	prepareBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgPrepare, ref, []ReplicaID{1, 3}, 3)
	recoveryApplyReady(t, store, restarted, rd)

	prepareResp := func(from ReplicaID) Message {
		return Message{Type: MsgPrepareResp, From: from, To: 2, Ref: ref, Ballot: prepareBallot, RecordStatus: StatusNone, Deps: []InstanceNum{0, 0, 0}}
	}
	if err := restarted.Step(prepareResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted newly added voter 4 prepare response: %#v", restarted.Ready())
	}
	if err := restarted.Step(prepareResp(3)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	acceptBallot := recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgAccept, ref, []ReplicaID{1, 3}, 3)
	recoveryRequireRecord(t, rd.Records, ref, StatusAccepted, CommandNoop)
	recoveryApplyReady(t, store, restarted, rd)

	acceptMsg := optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	acceptResp := func(from ReplicaID) Message {
		return Message{Type: MsgAcceptResp, From: from, To: 2, Ref: ref, Ballot: acceptBallot, Seq: acceptMsg.Seq, Deps: append([]InstanceNum(nil), acceptMsg.Deps...), RecordStatus: StatusAccepted}
	}
	if err := restarted.Step(acceptResp(4)); err != nil && !errors.Is(err, ErrMessageRejected) {
		t.Fatal(err)
	}
	if restarted.HasReady() {
		t.Fatalf("old-config recovery counted newly added voter 4 accept response: %#v", restarted.Ready())
	}
	if err := restarted.Step(acceptResp(3)); err != nil {
		t.Fatal(err)
	}
	rd = restarted.Ready()
	recoveryRequireMessageTargetsWithDepsWidth(t, rd.Messages, MsgCommit, ref, []ReplicaID{1, 3}, 3)
	recoveryRequireRecord(t, rd.Records, ref, StatusExecuted, CommandNoop)
	if len(rd.Committed) != 0 {
		t.Fatalf("old-config noop recovery emitted application commands: %#v", rd.Committed)
	}
	recoveryApplyReady(t, store, restarted, rd)
}

func TestSimulatorNonOwnerRecoversPreAcceptedDependency(t *testing.T) {
	s := newSimCluster(t, 3, false)
	first := Command{ID: CommandID{Client: 30, Sequence: 1}, Payload: []byte("owner-command"), ConflictKeys: [][]byte{[]byte("shared")}}
	firstRef, err := s.nodes[1].Propose(first)
	if err != nil {
		t.Fatal(err)
	}
	ownerReady := s.nodes[1].Ready()
	if err := s.stores[1].ApplyReady(ownerReady); err != nil {
		t.Fatal(err)
	}
	for _, m := range ownerReady.Messages {
		if m.Type == MsgPreAccept && !s.deliver(m) {
			t.Fatalf("preaccept to follower was unexpectedly delayed: %#v", m)
		}
	}
	advanceOK(t, s.nodes[1], ownerReady)

	s.pause(1)
	s.drain()

	second := Command{ID: CommandID{Client: 31, Sequence: 1}, Payload: []byte("follower-command"), ConflictKeys: [][]byte{[]byte("shared")}}
	secondRef, err := s.nodes[2].Propose(second)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickOnly(2, 6)

	for _, id := range []ReplicaID{2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, firstRef, first.Payload, 1)
		requireCommittedPayloadCount(t, s.apps[id], id, secondRef, second.Payload, 1)
	}
}

func TestSimulatorIsolatedLocalTimeoutRecoversAfterConflictingQuorumCommit(t *testing.T) {
	s := newSimCluster(t, 3, true)
	key := []byte("current-canary-recovery")

	s.omit(1)
	isolated := Command{ID: CommandID{Client: 1, Sequence: 1}, Payload: []byte("isolated-local"), ConflictKeys: [][]byte{key}}
	isolatedRef, err := s.nodes[1].Propose(isolated)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickOnly(1, 6)
	if committedPayload(s.apps[1], isolatedRef, isolated.Payload) {
		t.Fatalf("isolated replica applied %s without quorum before heal: %#v", isolatedRef, s.apps[1])
	}

	quorum := Command{ID: CommandID{Client: 2, Sequence: 1}, Payload: []byte("quorum-commit"), ConflictKeys: [][]byte{key}}
	quorumRef, err := s.nodes[2].Propose(quorum)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(6)
	for _, id := range []ReplicaID{2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, quorumRef, quorum.Payload, 1)
	}
	if committedPayload(s.apps[1], quorumRef, quorum.Payload) {
		t.Fatalf("isolated replica applied quorum commit %s before transport healed: %#v", quorumRef, s.apps[1])
	}

	s.heal(1)
	s.tickAll(20)
	requireCommittedPayloadCount(t, s.apps[1], 1, quorumRef, quorum.Payload, 1)

	barrier := Command{ID: CommandID{Client: 1, Sequence: 2}, ConflictKeys: [][]byte{key}}
	barrierRef, err := s.nodes[1].Propose(barrier)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	s.tickAll(20)
	requireCommittedPayloadCount(t, s.apps[1], 1, barrierRef, barrier.Payload, 1)
	requireCommittedPayloadCount(t, s.apps[1], 1, isolatedRef, isolated.Payload, 1)
	recoveryRequireAppliedOrder(t, s.apps[1], 1, quorumRef, isolatedRef, barrierRef)
	requireNoDuplicateCommittedRefs(t, s.apps[1], 1)
}

func TestSimulatorRestoredLocalStorageAdvancesPastLearnedLocalCommit(t *testing.T) {
	s := newSimCluster(t, 3, false)
	key := []byte("rollback-local-key")

	beforeCheckpoint := Command{ID: CommandID{Client: 40, Sequence: 1}, Payload: []byte("before-checkpoint"), ConflictKeys: [][]byte{key}}
	beforeCheckpointRef, err := s.nodes[3].Propose(beforeCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	requireCommittedPayloadCount(t, s.apps[3], 3, beforeCheckpointRef, beforeCheckpoint.Payload, 1)

	checkpointStore := cloneMemoryStorage(s.stores[3])
	checkpointApp := cloneCommittedCommands(s.apps[3])

	learned := Command{ID: CommandID{Client: 40, Sequence: 2}, Payload: []byte("learned-after-checkpoint"), ConflictKeys: [][]byte{key}}
	learnedRef, err := s.nodes[3].Propose(learned)
	if err != nil {
		t.Fatal(err)
	}
	s.drain()
	if learnedRef.Replica != 3 || learnedRef.Instance <= beforeCheckpointRef.Instance {
		t.Fatalf("post-checkpoint proposal ref=%s, want local instance after %s", learnedRef, beforeCheckpointRef)
	}
	for _, id := range []ReplicaID{1, 2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, learnedRef, learned.Payload, 1)
	}

	quorumRecords := make(map[ReplicaID]InstanceRecord, 2)
	for _, id := range []ReplicaID{1, 2} {
		rec, ok := s.stores[id].Records[learnedRef]
		if !ok || rec.Status < StatusCommitted {
			t.Fatalf("node %d storage record for %s = %#v, ok=%v; want committed quorum record", id, learnedRef, rec, ok)
		}
		quorumRecords[id] = rec
	}

	s.restart(3, checkpointStore)
	s.apps[3] = checkpointApp
	requireCommittedPayloadCount(t, s.apps[3], 3, learnedRef, learned.Payload, 0)

	for _, id := range []ReplicaID{1, 2} {
		if !s.deliver(recoveryCommitMessageFromRecord(id, 3, quorumRecords[id])) {
			t.Fatalf("commit message for %s from node %d to restored node 3 was unexpectedly delayed", learnedRef, id)
		}
	}
	s.drain()
	requireCommittedPayloadCount(t, s.apps[3], 3, learnedRef, learned.Payload, 1)
	requireNoDuplicateCommittedRefs(t, s.apps[3], 3)

	afterCatchUp := Command{ID: CommandID{Client: 40, Sequence: 3}, Payload: []byte("after-catch-up"), ConflictKeys: [][]byte{key}}
	afterCatchUpRef, err := s.nodes[3].Propose(afterCatchUp)
	if err != nil {
		t.Fatal(err)
	}
	if afterCatchUpRef.Replica != 3 || afterCatchUpRef.Instance <= learnedRef.Instance {
		t.Fatalf("proposal after learning local commit ref=%s, want local instance greater than learned ref %s", afterCatchUpRef, learnedRef)
	}
	s.drain()
	for _, id := range []ReplicaID{1, 2, 3} {
		requireCommittedPayloadCount(t, s.apps[id], id, afterCatchUpRef, afterCatchUp.Payload, 1)
		recoveryRequireAppliedOrder(t, s.apps[id], id, learnedRef, afterCatchUpRef)
		requireNoDuplicateCommittedRefs(t, s.apps[id], id)
	}
}

func recoveryCommitMessageFromRecord(from, to ReplicaID, rec InstanceRecord) Message {
	return Message{
		Type:           MsgCommit,
		From:           from,
		To:             to,
		Ref:            rec.Ref,
		Ballot:         rec.Ballot,
		RecordBallot:   recordTupleBallot(rec),
		Seq:            rec.Seq,
		Deps:           append([]InstanceNum(nil), rec.Deps...),
		AcceptSeq:      rec.AcceptSeq,
		AcceptDeps:     append([]InstanceNum(nil), rec.AcceptDeps...),
		AcceptEvidence: cloneAcceptEvidence(rec.AcceptEvidence),
		Command:        rec.Command.Clone(),
		RecordStatus:   StatusCommitted,
	}
}

func recoveryRequireAppliedOrder(t *testing.T, app []CommittedCommand, id ReplicaID, want ...InstanceRef) {
	t.Helper()
	positions := make(map[InstanceRef]int, len(app))
	for i, c := range app {
		if _, ok := positions[c.Ref]; !ok {
			positions[c.Ref] = i
		}
	}
	previous := -1
	for _, ref := range want {
		pos, ok := positions[ref]
		if !ok {
			t.Fatalf("node %d applied refs %v, missing %s", id, refs(app), ref)
		}
		if pos <= previous {
			t.Fatalf("node %d applied refs %v, want ordered subsequence %v", id, refs(app), want)
		}
		previous = pos
	}
}

func recoveryApplyReady(t *testing.T, store *MemoryStorage, rn *RawNode, rd Ready) {
	t.Helper()
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
}

func recoveryRequirePrepareRefs(t *testing.T, messages []Message, want []InstanceRef) {
	t.Helper()
	for _, ref := range want {
		count := 0
		for _, m := range messages {
			if m.Type == MsgPrepare && m.Ref == ref {
				count++
				if m.From == m.To {
					t.Fatalf("prepare for %s sent to self: %#v", ref, m)
				}
				if m.Ballot.Number == 0 || m.Ballot.Replica == 0 {
					t.Fatalf("prepare for %s has no recovery ballot: %#v", ref, m)
				}
			}
		}
		if count != 2 {
			t.Fatalf("prepare messages for %s = %d, want 2; messages=%#v", ref, count, messages)
		}
	}
}

func recoveryRequireMessageTargets(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, want []ReplicaID) Ballot {
	t.Helper()
	return recoveryRequireMessageTargetsWithDepsWidth(t, messages, typ, ref, want, 4)
}

func recoveryRequireMessageTargetsWithDepsWidth(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, want []ReplicaID, depsWidth int) Ballot {
	t.Helper()
	seen := make(map[ReplicaID]Message, len(want))
	for _, m := range messages {
		if m.Type != typ || m.Ref != ref {
			continue
		}
		if m.From == m.To {
			t.Fatalf("%s for %s sent to self: %#v", typ, ref, m)
		}
		seen[m.To] = m
		if m.Command.Kind != CommandNoop {
			t.Fatalf("%s for %s command kind = %v, want %v: %#v", typ, ref, m.Command.Kind, CommandNoop, m)
		}
		if len(m.Deps) != depsWidth {
			t.Fatalf("%s for %s deps width = %d, want old config width %d: %#v", typ, ref, len(m.Deps), depsWidth, m)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("%s messages for %s targets = %v, want %v; messages=%#v", typ, ref, messageTargets(seen), want, messages)
	}
	var ballot Ballot
	for _, to := range want {
		m, ok := seen[to]
		if !ok {
			t.Fatalf("%s messages for %s targets = %v, want %v; messages=%#v", typ, ref, messageTargets(seen), want, messages)
		}
		if m.Ballot.Number == 0 || m.Ballot.Replica == 0 {
			t.Fatalf("%s for %s has no recovery ballot: %#v", typ, ref, m)
		}
		if ballot == (Ballot{}) {
			ballot = m.Ballot
		} else if m.Ballot != ballot {
			t.Fatalf("%s messages for %s used different ballots: %#v", typ, ref, messages)
		}
	}
	return ballot
}

func messageTargets(messages map[ReplicaID]Message) []ReplicaID {
	targets := make([]ReplicaID, 0, len(messages))
	for to := range messages {
		targets = append(targets, to)
	}
	for i := 1; i < len(targets); i++ {
		for j := i; j > 0 && targets[j] < targets[j-1]; j-- {
			targets[j], targets[j-1] = targets[j-1], targets[j]
		}
	}
	return targets
}

func recoveryRequireDependencyRefs(t *testing.T, got []InstanceRef, want []InstanceRef) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("dependency refs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dependency refs = %v, want %v", got, want)
		}
	}
}

func recoveryRequireNoPrepareRefs(t *testing.T, messages []Message, refs []InstanceRef) {
	t.Helper()
	for _, ref := range refs {
		for _, m := range messages {
			if m.Type == MsgPrepare && m.Ref == ref {
				t.Fatalf("unexpected prepare for %s in %#v", ref, messages)
			}
		}
	}
}

func recoveryRequireMessages(t *testing.T, messages []Message, typ MessageType, ref InstanceRef, kind CommandKind) {
	t.Helper()
	count := 0
	for _, m := range messages {
		if m.Type != typ || m.Ref != ref {
			continue
		}
		count++
		if m.Command.Kind != kind {
			t.Fatalf("%s message for %s command kind = %v, want %v: %#v", typ, ref, m.Command.Kind, kind, m)
		}
	}
	if count != 2 {
		t.Fatalf("%s messages for %s = %d, want 2; messages=%#v", typ, ref, count, messages)
	}
}

func recoveryRequireRecord(t *testing.T, records []InstanceRecord, ref InstanceRef, status Status, kind CommandKind) {
	t.Helper()
	for _, rec := range records {
		if rec.Ref != ref || rec.Status != status {
			continue
		}
		if rec.Command.Kind != kind {
			t.Fatalf("record %s/%s command kind = %v, want %v: %#v", ref, status, rec.Command.Kind, kind, rec)
		}
		return
	}
	t.Fatalf("missing %s record for %s with command kind %v: %#v", status, ref, kind, records)
}

func recoveryCommittedPayloadCount(committed []CommittedCommand, ref InstanceRef, payload []byte) int {
	count := 0
	for _, c := range committed {
		if c.Ref == ref && bytes.Equal(c.Command.Payload, payload) {
			count++
		}
	}
	return count
}
