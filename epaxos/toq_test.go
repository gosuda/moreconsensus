package epaxos

import (
	"errors"
	"testing"
)

func toqTestCommand(client uint64, payload, key string) Command {
	return Command{ID: CommandID{Client: client, Sequence: 1}, Payload: []byte(payload), ConflictKeys: [][]byte{[]byte(key)}}
}

func newTOQTestRawNode(t *testing.T, id ReplicaID, voters int, clock *uint64, store *MemoryStorage, syncGroup []ReplicaID, delays map[ReplicaID]uint64) *RawNode {
	t.Helper()
	if store == nil {
		store = NewMemoryStorage()
	}
	rn, err := NewRawNode(Config{
		ID:             id,
		Voters:         makeIDs(voters),
		Storage:        store,
		RetryTicks:     2,
		RecoveryTicks:  10,
		TOQ:            true,
		TOQClock:       func() uint64 { return *clock },
		TOQOneWayDelay: delays,
		TOQSyncGroup:   syncGroup,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

func requireTOQPreAccepts(t *testing.T, messages []Message, ref InstanceRef, processAt uint64, wantTo []ReplicaID) {
	t.Helper()
	if len(messages) != len(wantTo) {
		t.Fatalf("TOQ PreAccept messages = %#v, want %d messages", messages, len(wantTo))
	}
	seen := make(map[ReplicaID]struct{}, len(messages))
	for _, msg := range messages {
		if msg.Type != MsgPreAccept || msg.From != ref.Replica || msg.Ref != ref || msg.ProcessAt != processAt {
			t.Fatalf("TOQ PreAccept message = %#v, want ProcessAt %d for %s from %d", msg, processAt, ref, ref.Replica)
		}
		if !msg.TOQ || msg.Seq != 0 || len(msg.Deps) != 0 {
			t.Fatalf("TOQ PreAccept message attrs = %#v, want TOQ=true, Seq=0, empty deps", msg)
		}
		seen[msg.To] = struct{}{}
	}
	for _, to := range wantTo {
		if _, ok := seen[to]; !ok {
			t.Fatalf("TOQ PreAccept recipients = %v, missing %d from %#v", seen, to, messages)
		}
	}
}

func applyAndAdvanceTOQReady(t *testing.T, store *MemoryStorage, rn *RawNode, rd Ready) {
	t.Helper()
	if err := store.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	advanceOK(t, rn, rd)
}

func TestTOQImmediateProcessAtProposeAssignsLocalAttrsAndValidatesMessages(t *testing.T) {
	clock := uint64(0)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
	cmd := toqTestCommand(211, "toq-immediate", "toq-immediate-key")

	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 2 {
		t.Fatalf("immediate TOQ ready records = %#v, want pending record followed by assigned attrs", rd.Records)
	}
	pending := rd.Records[0]
	if pending.Ref != ref || pending.Status != StatusNone || !pending.TOQPending || pending.ProcessAt != 0 || pending.Seq != 0 {
		t.Fatalf("immediate TOQ pending record = %#v, want StatusNone TOQPending at ProcessAt 0", pending)
	}
	assigned := rd.Records[1]
	if assigned.Ref != ref || assigned.Status != StatusPreAccepted || assigned.TOQPending || assigned.ProcessAt != 0 || assigned.Seq != 1 || !assigned.FastPathEligible {
		t.Fatalf("immediate TOQ assigned record = %#v, want preaccepted fast-path attrs at ProcessAt 0", assigned)
	}
	if !instanceNumsEqual(assigned.Deps, []InstanceNum{0, 0, 0}) {
		t.Fatalf("immediate TOQ assigned deps = %v, want no conflicts", assigned.Deps)
	}
	requireTOQPreAccepts(t, rd.Messages, ref, 0, []ReplicaID{2, 3})
	for _, msg := range rd.Messages {
		if err := msg.Validate(rn.q.conf); err != nil {
			t.Fatalf("immediate TOQ PreAccept message %#v failed validation: %v", msg, err)
		}
	}
	inst := rn.instances[ref]
	if inst == nil || inst.rec.TOQPending || inst.rec.Status != StatusPreAccepted || inst.rec.ProcessAt != 0 || len(inst.preOK) != 1 {
		t.Fatalf("local immediate TOQ instance = %#v, want assigned local preaccept vote at ProcessAt 0", inst)
	}
	if got := rn.conflicts["toq-immediate-key"][1]; got != ref {
		t.Fatalf("immediate TOQ command conflict index = %s, want %s", got, ref)
	}

	applyAndAdvanceTOQReady(t, store, rn, rd)
	stored, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("missing persisted immediate TOQ record for %s", ref)
	}
	if !instanceRecordEqual(stored, assigned) {
		t.Fatalf("persisted immediate TOQ record = %#v, want final assigned record %#v", stored, assigned)
	}
}

func TestTOQImmediateProcessAtSkipsLaterSameTimestampConflict(t *testing.T) {
	clock := uint64(0)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
	laterRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:              laterRef,
		Ballot:           Ballot{Replica: 2},
		Status:           StatusPreAccepted,
		Seq:              5,
		Deps:             rn.q.deps(),
		Command:          toqTestCommand(213, "toq-zero-later-conflict", "toq-zero-order-key"),
		ProcessAt:        0,
		FastPathEligible: true,
	})

	ref, err := rn.Propose(toqTestCommand(214, "toq-zero-candidate", "toq-zero-order-key"))
	if err != nil {
		t.Fatal(err)
	}
	if !preAcceptOrderBefore(0, ref, 0, laterRef) {
		t.Fatalf("test setup candidate ref %s must sort before existing same-timestamp conflict %s", ref, laterRef)
	}
	rd := rn.Ready()
	if len(rd.Records) != 2 {
		t.Fatalf("immediate TOQ ready records = %#v, want pending record followed by assigned attrs", rd.Records)
	}
	assigned := rd.Records[1]
	if assigned.Status != StatusPreAccepted || assigned.TOQPending || assigned.ProcessAt != 0 || !assigned.FastPathEligible {
		t.Fatalf("immediate TOQ zero-timestamp assignment = %#v, want ordinary fast-path preaccepted record at ProcessAt 0", assigned)
	}
	if assigned.Seq != 1 || !instanceNumsEqual(assigned.Deps, []InstanceNum{0, 0, 0}) {
		t.Fatalf("immediate TOQ zero-timestamp attrs = seq %d deps %v, want no dependency on later same-timestamp conflict %s", assigned.Seq, assigned.Deps, laterRef)
	}
	if byReplica := rn.conflicts["toq-zero-order-key"]; len(byReplica) != 2 || byReplica[1] != ref || byReplica[2] != laterRef {
		t.Fatalf("TOQ zero-timestamp conflict index = %#v, want candidate %s and later conflict %s indexed after assignment", byReplica, ref, laterRef)
	}
}

func TestTOQProposePersistsPendingAndSendsExplicitMessages(t *testing.T) {
	clock := uint64(10)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, []ReplicaID{1, 3}, map[ReplicaID]uint64{1: 2, 2: 99, 3: 7})
	cmd := toqTestCommand(201, "toq-pending", "toq-pending-key")

	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	processAt := uint64(17)
	rd := rn.Ready()
	if len(rd.Records) != 1 {
		t.Fatalf("initial TOQ ready records = %#v, want one pending record", rd.Records)
	}
	pending := rd.Records[0]
	if pending.Ref != ref || pending.Status != StatusNone || !pending.TOQPending || pending.ProcessAt != processAt || pending.Seq != 0 {
		t.Fatalf("initial TOQ record = %#v, want StatusNone TOQPending at ProcessAt %d", pending, processAt)
	}
	if !instanceNumsEqual(pending.Deps, []InstanceNum{0, 0, 0}) {
		t.Fatalf("initial TOQ pending deps = %v, want zero-width vector for durability only", pending.Deps)
	}
	requireTOQPreAccepts(t, rd.Messages, ref, processAt, []ReplicaID{2, 3})

	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing local TOQ instance %s", ref)
	}
	if inst.rec.Status != StatusNone || !inst.rec.TOQPending || len(inst.preOK) != 0 {
		t.Fatalf("local TOQ instance before ProcessAt = phase %d record %#v preOK %#v, want pending without local preaccept vote", inst.phase, inst.rec, inst.preOK)
	}
	if byReplica := rn.conflicts["toq-pending-key"]; len(byReplica) != 0 {
		t.Fatalf("TOQ pending command was indexed as an ordinary conflict before ProcessAt: %#v", byReplica)
	}

	applyAndAdvanceTOQReady(t, store, rn, rd)
	stored, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("missing durable TOQ pending record for %s", ref)
	}
	if stored.Status != StatusNone || !stored.TOQPending || stored.ProcessAt != processAt {
		t.Fatalf("stored TOQ record = %#v, want durable pending at ProcessAt %d", stored, processAt)
	}
}

func TestTOQProcessAtAssignsLocalAttrsAndRetriesStayExplicit(t *testing.T) {
	clock := uint64(5)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 4, 3: 6})
	cmd := toqTestCommand(202, "toq-assign", "toq-assign-key")

	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	processAt := uint64(11)
	requireTOQPreAccepts(t, initial.Messages, ref, processAt, []ReplicaID{2, 3})
	applyAndAdvanceTOQReady(t, store, rn, initial)

	clock = processAt
	rn.Tick()
	assignedReady := rn.Ready()
	assigned := optimizedRequireRecord(t, assignedReady, ref)
	if assigned.Status != StatusPreAccepted || assigned.TOQPending || assigned.ProcessAt != processAt || assigned.Seq != 1 || !assigned.FastPathEligible {
		t.Fatalf("TOQ ProcessAt assignment record = %#v, want preaccepted fast-path attrs at ProcessAt %d", assigned, processAt)
	}
	if !instanceNumsEqual(assigned.Deps, []InstanceNum{0, 0, 0}) {
		t.Fatalf("TOQ ProcessAt deps = %v, want no conflicts", assigned.Deps)
	}
	inst := rn.instances[ref]
	if inst == nil || inst.rec.TOQPending || inst.rec.Status != StatusPreAccepted || len(inst.preOK) != 1 {
		t.Fatalf("local TOQ instance after ProcessAt = %#v preOK %#v, want one local preaccept vote", inst, inst.preOK)
	}
	if got := rn.conflicts["toq-assign-key"][1]; got != ref {
		t.Fatalf("TOQ command conflict index after ProcessAt = %s, want %s", got, ref)
	}
	applyAndAdvanceTOQReady(t, store, rn, assignedReady)

	rn.Tick()
	retry := rn.Ready()
	if len(retry.Records) != 0 || len(retry.Committed) != 0 {
		t.Fatalf("TOQ PreAccept retry changed durable/application work: records=%#v committed=%#v", retry.Records, retry.Committed)
	}
	requireTOQPreAccepts(t, retry.Messages, ref, processAt, []ReplicaID{2, 3})
}

func TestTOQPendingLocalRestartWaitsAndAssignsAtProcessAt(t *testing.T) {
	clock := uint64(40)
	store := NewMemoryStorage()
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	conflict := InstanceRecord{
		Ref:              conflictRef,
		Ballot:           Ballot{Replica: 2},
		RecordBallot:     Ballot{Replica: 2},
		Status:           StatusPreAccepted,
		Seq:              3,
		Deps:             []InstanceNum{0, 0, 0},
		Command:          toqTestCommand(209, "toq-restart-conflict", "toq-restart-key"),
		FastPathEligible: true,
	}
	conflict.Checksum = ChecksumRecord(conflict)
	store.Records[conflictRef] = conflict

	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 4, 3: 7})
	cmd := toqTestCommand(210, "toq-restart-pending", "toq-restart-key")

	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	processAt := uint64(47)
	initial := rn.Ready()
	pending := optimizedRequireRecord(t, initial, ref)
	if pending.Status != StatusNone || !pending.TOQPending || pending.ProcessAt != processAt || !commandEqual(pending.Command, cmd) {
		t.Fatalf("initial TOQ pending record = %#v, want durable pending command at ProcessAt %d", pending, processAt)
	}
	requireTOQPreAccepts(t, initial.Messages, ref, processAt, []ReplicaID{2, 3})
	applyAndAdvanceTOQReady(t, store, rn, initial)

	clock = processAt - 1
	restarted := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 4, 3: 7})
	if restarted.HasReady() {
		t.Fatalf("restart before TOQ ProcessAt produced ready work: %#v", restarted.Ready())
	}
	inst := restarted.instances[ref]
	if inst == nil {
		t.Fatalf("missing restarted local TOQ instance %s", ref)
	}
	if inst.phase != phasePreAccept || inst.rec.Status != StatusNone || !inst.rec.TOQPending || inst.rec.ProcessAt != processAt || !commandEqual(inst.rec.Command, cmd) || len(inst.preOK) != 0 {
		t.Fatalf("restarted local TOQ instance before ProcessAt = %#v preOK %#v, want pending command without local preaccept vote", inst, inst.preOK)
	}
	if byReplica := restarted.conflicts["toq-restart-key"]; len(byReplica) != 1 || byReplica[2] != conflictRef {
		t.Fatalf("restart conflict index before ProcessAt = %#v, want only preexisting conflict %s", byReplica, conflictRef)
	}

	restarted.Tick()
	if restarted.HasReady() {
		t.Fatalf("restart tick before retry produced ready work before ProcessAt: %#v", restarted.Ready())
	}
	restarted.Tick()
	beforeProcessAtRetry := restarted.Ready()
	if len(beforeProcessAtRetry.Records) != 0 || len(beforeProcessAtRetry.Committed) != 0 {
		t.Fatalf("TOQ retry before ProcessAt produced durable/application work: records=%#v committed=%#v", beforeProcessAtRetry.Records, beforeProcessAtRetry.Committed)
	}
	requireTOQPreAccepts(t, beforeProcessAtRetry.Messages, ref, processAt, []ReplicaID{2, 3})
	if inst.rec.Status != StatusNone || !inst.rec.TOQPending {
		t.Fatalf("TOQ retry before ProcessAt mutated pending record to %#v", inst.rec)
	}
	if byReplica := restarted.conflicts["toq-restart-key"]; len(byReplica) != 1 || byReplica[2] != conflictRef {
		t.Fatalf("retry before ProcessAt indexed pending command: %#v", byReplica)
	}
	applyAndAdvanceTOQReady(t, store, restarted, beforeProcessAtRetry)

	clock = processAt
	restarted.Tick()
	assignedReady := restarted.Ready()
	assigned := optimizedRequireRecord(t, assignedReady, ref)
	wantDeps := []InstanceNum{0, 1, 0}
	if assigned.Status != StatusPreAccepted || assigned.TOQPending || assigned.ProcessAt != processAt || assigned.Seq != 4 || !assigned.FastPathEligible {
		t.Fatalf("restarted TOQ ProcessAt assignment record = %#v, want ordinary preaccepted attrs at ProcessAt %d", assigned, processAt)
	}
	if !instanceNumsEqual(assigned.Deps, wantDeps) {
		t.Fatalf("restarted TOQ ProcessAt deps = %v, want dependency on %s", assigned.Deps, conflictRef)
	}
	if !commandEqual(assigned.Command, cmd) {
		t.Fatalf("restarted TOQ assignment command = %#v, want original %#v", assigned.Command, cmd)
	}
	if byReplica := restarted.conflicts["toq-restart-key"]; len(byReplica) != 2 || byReplica[1] != ref || byReplica[2] != conflictRef {
		t.Fatalf("conflict index after restarted TOQ assignment = %#v, want local %s plus preexisting %s", byReplica, ref, conflictRef)
	}
	applyAndAdvanceTOQReady(t, store, restarted, assignedReady)
	stored, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("missing persisted restarted TOQ assignment for %s", ref)
	}
	if !instanceRecordEqual(stored, assigned) {
		t.Fatalf("persisted restarted TOQ assignment = %#v, want %#v", stored, assigned)
	}

	restarted.Tick()
	retry := restarted.Ready()
	if len(retry.Records) != 0 || len(retry.Committed) != 0 {
		t.Fatalf("TOQ retry after assignment changed durable/application work: records=%#v committed=%#v", retry.Records, retry.Committed)
	}
	requireTOQPreAccepts(t, retry.Messages, ref, processAt, []ReplicaID{2, 3})
	for _, msg := range retry.Messages {
		if !commandEqual(msg.Command, cmd) {
			t.Fatalf("TOQ retry after restart command = %#v, want original %#v", msg.Command, cmd)
		}
	}
}

func TestTOQPendingConfChangeRestartRejectsNewProposalsBeforeProcessAt(t *testing.T) {
	clock := uint64(60)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 4, 3: 7})

	confRef, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 4})
	if err != nil {
		t.Fatal(err)
	}
	processAt := uint64(67)
	initial := rn.Ready()
	pending := optimizedRequireRecord(t, initial, confRef)
	if pending.Status != StatusNone || !pending.TOQPending || pending.ProcessAt != processAt || pending.Command.Kind != CommandConfChange {
		t.Fatalf("initial TOQ config-change record = %#v, want durable pending config change at ProcessAt %d", pending, processAt)
	}
	applyAndAdvanceTOQReady(t, store, rn, initial)
	stored, ok := store.Instance(confRef)
	if !ok {
		t.Fatalf("missing durable TOQ config-change record for %s", confRef)
	}
	if stored.Status != StatusNone || !stored.TOQPending || stored.ProcessAt != processAt || stored.Command.Kind != CommandConfChange {
		t.Fatalf("stored TOQ config-change record = %#v, want durable pending config change at ProcessAt %d", stored, processAt)
	}

	clock = processAt - 1
	restarted := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 4, 3: 7})
	inst := restarted.instances[confRef]
	if inst == nil || inst.rec.Status != StatusNone || !inst.rec.TOQPending || inst.rec.ProcessAt != processAt || inst.rec.Command.Kind != CommandConfChange {
		t.Fatalf("restarted TOQ config-change instance = %#v, want durable pending config change before ProcessAt", inst)
	}
	if _, err := restarted.Propose(toqTestCommand(212, "blocked-by-pending-conf", "blocked-by-pending-conf-key")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose with restarted pending TOQ config change err=%v, want %v", err, ErrMessageRejected)
	}
	if _, err := restarted.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 5}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("ProposeConfChange with restarted pending TOQ config change err=%v, want %v", err, ErrMessageRejected)
	}
}

func TestTOQReceiverComputesAttrsAtProcessAtFromFlaggedPreAccept(t *testing.T) {
	clock := uint64(50)
	rn := newTOQTestRawNode(t, 2, 3, &clock, nil, nil, map[ReplicaID]uint64{1: 5, 2: 0, 3: 5})
	conflictRef := InstanceRef{Replica: 2, Instance: 1, Conf: 1}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:              conflictRef,
		Ballot:           Ballot{Replica: 2},
		Status:           StatusPreAccepted,
		Seq:              3,
		Deps:             rn.q.deps(),
		Command:          toqTestCommand(203, "receiver-conflict", "shared-toq-receiver"),
		FastPathEligible: true,
	})

	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{
		Type:      MsgPreAccept,
		From:      1,
		To:        2,
		Ref:       ref,
		Ballot:    Ballot{Replica: 1},
		TOQ:       true,
		ProcessAt: clock,
		Seq:       0,
		Deps:      nil,
		Command:   toqTestCommand(204, "receiver-toq", "shared-toq-receiver"),
	}); err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	wantDeps := []InstanceNum{0, 1, 0}
	if rec.Status != StatusPreAccepted || rec.Seq != 4 || !instanceNumsEqual(rec.Deps, wantDeps) || !rec.FastPathEligible {
		t.Fatalf("receiver TOQ record = %#v, want computed attrs seq 4 deps %v and fast-path eligible", rec, wantDeps)
	}
	resp := optimizedRequireMessage(t, rd.Messages, MsgPreAcceptResp, 1)
	if resp.Seq != 4 || !instanceNumsEqual(resp.Deps, wantDeps) || !resp.FastPathEligible || resp.RecordStatus != StatusPreAccepted {
		t.Fatalf("receiver TOQ response = %#v, want computed fast-path attrs seq 4 deps %v", resp, wantDeps)
	}
}

func TestTOQFiveNodeFastCommitUsesOptimizedQuorumWithCoveringAttrs(t *testing.T) {
	clock := uint64(100)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 5, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 3, 3: 3, 4: 3, 5: 3})
	if slow, fast := rn.q.slowQuorum(), rn.q.fastQuorum(); slow != 3 || fast != 3 {
		t.Fatalf("five-node TOQ quorum slow=%d fast=%d, want both 3", slow, fast)
	}
	optimizedSeedRecord(t, rn, InstanceRecord{
		Ref:              InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot:           Ballot{Replica: 2},
		Status:           StatusPreAccepted,
		Seq:              1,
		Deps:             rn.q.deps(),
		Command:          toqTestCommand(205, "toq-fast-conflict", "toq-fast-key"),
		FastPathEligible: true,
	})
	cmd := toqTestCommand(206, "toq-fast", "toq-fast-key")

	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	processAt := uint64(103)
	requireTOQPreAccepts(t, initial.Messages, ref, processAt, []ReplicaID{2, 3, 4, 5})
	applyAndAdvanceTOQReady(t, store, rn, initial)

	clock = processAt
	rn.Tick()
	assignedReady := rn.Ready()
	assigned := optimizedRequireRecord(t, assignedReady, ref)
	localAttrs := Attributes{Seq: 2, Deps: []InstanceNum{0, 1, 0, 0, 0}}
	if assigned.Status != StatusPreAccepted || !assigned.FastPathEligible || !assigned.Attributes().Equal(localAttrs) {
		t.Fatalf("TOQ local delayed attrs = %#v, want %#v", assigned, localAttrs)
	}
	applyAndAdvanceTOQReady(t, store, rn, assignedReady)
	inst := rn.instances[ref]
	if inst == nil {
		t.Fatalf("missing TOQ instance %s", ref)
	}

	covering := Attributes{Seq: 3, Deps: []InstanceNum{0, 1, 1, 0, 0}}
	for _, from := range []ReplicaID{2, 3} {
		if err := rn.Step(Message{
			Type:             MsgPreAcceptResp,
			From:             from,
			To:               1,
			Ref:              ref,
			Ballot:           inst.rec.Ballot,
			Seq:              covering.Seq,
			Deps:             append([]InstanceNum(nil), covering.Deps...),
			RecordStatus:     StatusPreAccepted,
			FastPathEligible: true,
			DepsCommitted:    dependencyMask(covering.Deps),
		}); err != nil {
			t.Fatalf("step TOQ covering response from %d: %v", from, err)
		}
	}

	if inst.phase != phaseCommitted {
		t.Fatalf("TOQ phase after local plus two covering remote replies = %d, want committed", inst.phase)
	}
	rd := rn.Ready()
	committed := optimizedRequireRecord(t, rd, ref)
	if committed.Status != StatusCommitted || committed.Seq != covering.Seq || !instanceNumsEqual(committed.Deps, covering.Deps) {
		t.Fatalf("TOQ fast commit record = %#v, want committed covering attrs %#v", committed, covering)
	}
	optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
	for _, to := range []ReplicaID{2, 3, 4, 5} {
		msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
		if msg.Ref != ref || msg.RecordStatus != StatusCommitted || msg.Seq != covering.Seq || !instanceNumsEqual(msg.Deps, covering.Deps) {
			t.Fatalf("TOQ fast commit message to %d = %#v, want covering committed attrs %#v", to, msg, covering)
		}
	}
	if got := len(inst.preOK); got != rn.q.fastQuorum() {
		t.Fatalf("TOQ fast commit used %d PreAccept votes, want optimized quorum %d without all remote voters", got, rn.q.fastQuorum())
	}
}

func TestTOQThreeNodeFastCommitUsesOptimizedQuorumWithCoveringAttrs(t *testing.T) {
	startScenario := func(t *testing.T, client uint64, payload string) (*RawNode, *MemoryStorage, InstanceRef, *instance, Attributes) {
		t.Helper()
		clock := uint64(100)
		store := NewMemoryStorage()
		rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 3, 3: 3})
		if slow, fast := rn.q.slowQuorum(), rn.q.fastQuorum(); slow != 2 || fast != 2 {
			t.Fatalf("three-node TOQ quorum slow=%d fast=%d, want both 2", slow, fast)
		}
		optimizedSeedRecord(t, rn, InstanceRecord{
			Ref:              InstanceRef{Replica: 2, Instance: 1, Conf: 1},
			Ballot:           Ballot{Replica: 2},
			Status:           StatusPreAccepted,
			Seq:              1,
			Deps:             rn.q.deps(),
			Command:          toqTestCommand(client+1000, payload+"-conflict", payload+"-key"),
			FastPathEligible: true,
		})

		ref, err := rn.Propose(toqTestCommand(client, payload, payload+"-key"))
		if err != nil {
			t.Fatal(err)
		}
		initial := rn.Ready()
		processAt := uint64(103)
		requireTOQPreAccepts(t, initial.Messages, ref, processAt, []ReplicaID{2, 3})
		applyAndAdvanceTOQReady(t, store, rn, initial)

		clock = processAt
		rn.Tick()
		assignedReady := rn.Ready()
		assigned := optimizedRequireRecord(t, assignedReady, ref)
		localAttrs := Attributes{Seq: 2, Deps: []InstanceNum{0, 1, 0}}
		if assigned.Status != StatusPreAccepted || !assigned.FastPathEligible || !assigned.Attributes().Equal(localAttrs) {
			t.Fatalf("TOQ local delayed attrs = %#v, want %#v", assigned, localAttrs)
		}
		applyAndAdvanceTOQReady(t, store, rn, assignedReady)
		inst := rn.instances[ref]
		if inst == nil {
			t.Fatalf("missing TOQ instance %s", ref)
		}
		if got := len(inst.preOK); got != 1 {
			t.Fatalf("TOQ local assignment votes = %d, want exactly the local vote before remote responses", got)
		}
		return rn, store, ref, inst, localAttrs
	}

	t.Run("covering remote reply commits at local plus one boundary", func(t *testing.T) {
		rn, _, ref, inst, localAttrs := startScenario(t, 230, "toq-three-fast")
		covering := Attributes{Seq: localAttrs.Seq + 1, Deps: []InstanceNum{0, 1, 1}}
		if err := rn.Step(Message{
			Type:             MsgPreAcceptResp,
			From:             2,
			To:               1,
			Ref:              ref,
			Ballot:           inst.rec.Ballot,
			Seq:              covering.Seq,
			Deps:             append([]InstanceNum(nil), covering.Deps...),
			RecordStatus:     StatusPreAccepted,
			FastPathEligible: true,
			DepsCommitted:    dependencyMask(covering.Deps),
		}); err != nil {
			t.Fatalf("step TOQ covering response: %v", err)
		}

		if inst.phase != phaseCommitted {
			t.Fatalf("TOQ phase after local plus one covering remote reply = %d, want committed", inst.phase)
		}
		rd := rn.Ready()
		committed := optimizedRequireRecord(t, rd, ref)
		if committed.Status != StatusCommitted || committed.Seq != covering.Seq || !instanceNumsEqual(committed.Deps, covering.Deps) {
			t.Fatalf("TOQ three-node fast commit record = %#v, want committed covering attrs %#v", committed, covering)
		}
		optimizedRequireNoMessageType(t, rd.Messages, MsgAccept)
		for _, to := range []ReplicaID{2, 3} {
			msg := optimizedRequireMessage(t, rd.Messages, MsgCommit, to)
			if msg.Ref != ref || msg.RecordStatus != StatusCommitted || msg.Seq != covering.Seq || !instanceNumsEqual(msg.Deps, covering.Deps) {
				t.Fatalf("TOQ three-node fast commit message to %d = %#v, want covering committed attrs %#v", to, msg, covering)
			}
		}
		if got, fast, voters := len(inst.preOK), rn.q.fastQuorum(), len(rn.q.conf.Voters); got != fast || got == voters {
			t.Fatalf("TOQ three-node fast commit used %d PreAccept votes, want optimized quorum %d below all %d voters", got, fast, voters)
		}
	})

	t.Run("non-covering remote reply cannot fast commit at same boundary", func(t *testing.T) {
		rn, _, ref, inst, _ := startScenario(t, 231, "toq-three-non-covering")
		nonCovering := Attributes{Seq: 1, Deps: []InstanceNum{0, 0, 0}}
		if err := rn.Step(Message{
			Type:             MsgPreAcceptResp,
			From:             2,
			To:               1,
			Ref:              ref,
			Ballot:           inst.rec.Ballot,
			Seq:              nonCovering.Seq,
			Deps:             append([]InstanceNum(nil), nonCovering.Deps...),
			RecordStatus:     StatusPreAccepted,
			FastPathEligible: true,
			DepsCommitted:    dependencyMask(nonCovering.Deps),
		}); err != nil {
			t.Fatalf("step TOQ non-covering response: %v", err)
		}

		if inst.phase == phaseCommitted || inst.rec.Status == StatusCommitted {
			t.Fatalf("TOQ non-covering attrs fast-committed at local plus one boundary: phase=%d record=%#v", inst.phase, inst.rec)
		}
		rd := rn.Ready()
		accepted := optimizedRequireRecord(t, rd, ref)
		if accepted.Status != StatusAccepted {
			t.Fatalf("TOQ non-covering attrs record = %#v, want slow-path accepted rather than fast committed", accepted)
		}
		optimizedRequireNoMessageType(t, rd.Messages, MsgCommit)
		optimizedRequireMessage(t, rd.Messages, MsgAccept, 2)
		optimizedRequireMessage(t, rd.Messages, MsgAccept, 3)
	})
}

func TestTOQPrepareBeforeProcessAtWaitsForPendingAssignment(t *testing.T) {
	clock := uint64(20)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 5, 3: 5})
	cmd := toqTestCommand(207, "toq-prepare", "toq-prepare-key")

	ref, err := rn.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	processAt := uint64(25)
	applyAndAdvanceTOQReady(t, store, rn, initial)

	prepare := Message{Type: MsgPrepare, From: 2, To: 1, Ref: ref, Ballot: Ballot{Number: 1, Replica: 2}}
	if err := rn.Step(prepare); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatalf("Prepare before TOQ ProcessAt produced ready work: %#v", rn.Ready())
	}
	if inst := rn.instances[ref]; inst == nil || !inst.rec.TOQPending || inst.rec.Status != StatusNone || inst.rec.Command.ID != cmd.ID {
		t.Fatalf("TOQ pending command after early Prepare = %#v, want pending command retained", inst)
	}
	stored, ok := store.Instance(ref)
	if !ok || !stored.TOQPending || stored.Status != StatusNone || stored.Command.ID != cmd.ID {
		t.Fatalf("durable TOQ pending after early Prepare = %#v ok=%v, want pending command retained", stored, ok)
	}

	clock = processAt
	rn.Tick()
	assignedReady := rn.Ready()
	assigned := optimizedRequireRecord(t, assignedReady, ref)
	if assigned.Status != StatusPreAccepted || assigned.TOQPending || assigned.Command.ID != cmd.ID {
		t.Fatalf("TOQ assignment before Prepare recovery = %#v, want ordinary preaccepted command", assigned)
	}
	applyAndAdvanceTOQReady(t, store, rn, assignedReady)

	if err := rn.Step(prepare); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	resp := optimizedRequireMessage(t, rd.Messages, MsgPrepareResp, 2)
	if resp.RecordStatus != StatusPreAccepted || resp.Seq != assigned.Seq || !instanceNumsEqual(resp.Deps, assigned.Deps) || resp.Command.ID != cmd.ID {
		t.Fatalf("Prepare after TOQ ProcessAt response = %#v, want ordinary preaccepted tuple %#v", resp, assigned)
	}
	if resp.FastPathEligible != assigned.FastPathEligible {
		t.Fatalf("Prepare after TOQ ProcessAt FastPathEligible = %v, want %v", resp.FastPathEligible, assigned.FastPathEligible)
	}
}

func TestTOQMessageValidationRejectsInvalidFlagBoundaries(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	cmd := toqTestCommand(208, "toq-validate", "toq-validate-key")

	for _, tc := range []struct {
		name      string
		processAt uint64
	}{
		{name: "immediate-zero-process-at", processAt: 0},
		{name: "delayed-process-at", processAt: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valid := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, TOQ: true, ProcessAt: tc.processAt, Seq: 0, Deps: nil, Command: cmd}
			if err := valid.Validate(conf); err != nil {
				t.Fatalf("valid explicit TOQ PreAccept at ProcessAt %d rejected: %v", tc.processAt, err)
			}
		})
	}

	for _, typ := range []MessageType{MsgPreAcceptResp, MsgAccept, MsgCommit, MsgPrepare, MsgPrepareResp, MsgTryPreAccept, MsgTryPreAcceptResp} {
		t.Run(typ.String()+"/toq-flag", func(t *testing.T) {
			msg := Message{Type: typ, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, TOQ: true, ProcessAt: 9, Seq: 0, Deps: rnDepsForValidation(typ, conf), Command: cmd}
			if err := msg.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("Validate(%s with TOQ=true) err=%v, want %v", typ, err, ErrInvalidMessage)
			}
		})
	}

	for _, tc := range []struct {
		name      string
		processAt uint64
	}{
		{name: "legacy-empty-deps-immediate", processAt: 0},
		{name: "legacy-empty-deps-delayed", processAt: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacy := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, ProcessAt: tc.processAt, Seq: 0, Deps: nil, Command: cmd}
			if err := legacy.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("legacy unflagged Seq0/empty-deps PreAccept at ProcessAt %d err=%v, want %v", tc.processAt, err, ErrInvalidMessage)
			}
		})
	}
}

func TestFlaggedTOQPreAcceptRejectsLegacyAttrsButAllowsImmediateProcessAt(t *testing.T) {
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	ref := InstanceRef{Replica: 1, Instance: 2, Conf: 1}
	cmd := toqTestCommand(217, "toq-attr-validation", "toq-attr-validation-key")

	validImmediate := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, TOQ: true, ProcessAt: 0, Command: cmd}
	if err := validImmediate.Validate(conf); err != nil {
		t.Fatalf("flagged immediate TOQ PreAccept with empty attrs rejected: %v", err)
	}

	for _, tc := range []struct {
		name string
		msg  Message
	}{
		{
			name: "nonzero seq",
			msg:  Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, TOQ: true, ProcessAt: 0, Seq: 1, Command: cmd},
		},
		{
			name: "nonempty deps",
			msg:  Message{Type: MsgPreAccept, From: 1, To: 2, Ref: ref, Ballot: Ballot{Replica: 1}, TOQ: true, ProcessAt: 0, Deps: []InstanceNum{0, 0, 0}, Command: cmd},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.msg.Validate(conf); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("flagged TOQ PreAccept with %s err=%v, want %v", tc.name, err, ErrInvalidMessage)
			}
		})
	}
}

func TestTOQConfigValidationRejectsUnsafeClockAndSyncGroup(t *testing.T) {
	clock := uint64(10)
	validClock := func() uint64 { return clock }
	base := Config{
		ID:             1,
		Voters:         makeIDs(3),
		TOQ:            true,
		TOQClock:       validClock,
		TOQOneWayDelay: map[ReplicaID]uint64{1: 0, 2: 2, 3: 3},
	}

	for _, tc := range []struct {
		name   string
		mutate func(Config) Config
	}{
		{
			name: "time optimization cannot be combined with TOQ",
			mutate: func(cfg Config) Config {
				cfg.TimeOptimization = true
				return cfg
			},
		},
		{
			name: "missing synchronized clock",
			mutate: func(cfg Config) Config {
				cfg.TOQClock = nil
				return cfg
			},
		},
		{
			name: "sync group contains non-voter",
			mutate: func(cfg Config) Config {
				cfg.TOQSyncGroup = []ReplicaID{1, 4}
				cfg.TOQOneWayDelay[4] = 4
				return cfg
			},
		},
		{
			name: "sync group contains duplicate voter",
			mutate: func(cfg Config) Config {
				cfg.TOQSyncGroup = []ReplicaID{1, 1}
				return cfg
			},
		},
		{
			name: "missing non-local one-way delay",
			mutate: func(cfg Config) Config {
				cfg.TOQSyncGroup = []ReplicaID{1, 2}
				cfg.TOQOneWayDelay = map[ReplicaID]uint64{1: 0}
				return cfg
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRawNode(tc.mutate(base)); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewRawNode(%s) err=%v, want %v", tc.name, err, ErrInvalidConfig)
			}
		})
	}
}

func TestNewRawNodeRejectsTOQPendingRecordWhenTOQDisabled(t *testing.T) {
	rec := InstanceRecord{
		Ref:        InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:     Ballot{Replica: 1},
		Status:     StatusNone,
		Deps:       []InstanceNum{0, 0, 0},
		Command:    toqTestCommand(218, "toq-pending-without-config", "toq-pending-without-config-key"),
		ProcessAt:  20,
		TOQPending: true,
	}
	rec.Checksum = ChecksumRecord(rec)

	_, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: loadFailStorage{rec: rec}})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewRawNode loaded TOQPending record without Config.TOQ err=%v, want %v", err, ErrInvalidConfig)
	}
}

func TestTOQLocalSelfMessageIgnoresStaleAssignment(t *testing.T) {
	clock := uint64(10)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 4, 3: 7})
	ref, err := rn.Propose(toqTestCommand(219, "toq-stale-local", "toq-stale-local-key"))
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	inst := rn.instances[ref]
	if inst == nil || !inst.rec.TOQPending {
		t.Fatalf("test setup local TOQ instance = %#v, want pending", inst)
	}
	stale := rn.localTOQPreAcceptMessage(inst)
	applyAndAdvanceTOQReady(t, store, rn, initial)

	clock = stale.ProcessAt
	rn.Tick()
	assignedReady := rn.Ready()
	assigned := optimizedRequireRecord(t, assignedReady, ref)
	if assigned.TOQPending || assigned.Status != StatusPreAccepted {
		t.Fatalf("TOQ assignment record = %#v, want preaccepted non-pending record", assigned)
	}
	applyAndAdvanceTOQReady(t, store, rn, assignedReady)

	rn.handleLocalTOQPreAccept(stale)
	if rd := rn.Ready(); !rd.Empty() {
		t.Fatalf("stale local TOQ self-message produced new Ready work: %#v", rd)
	}
	if inst.rec.TOQPending || inst.rec.Status != StatusPreAccepted || inst.rec.ProcessAt != stale.ProcessAt {
		t.Fatalf("stale local TOQ self-message mutated assigned instance: %#v", inst.rec)
	}
}

func TestSingleVoterTOQProposalCommitsAfterLocalProcessAtAssignment(t *testing.T) {
	clock := uint64(7)
	rn := newTOQTestRawNode(t, 1, 1, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0})
	ref, err := rn.Propose(toqTestCommand(220, "toq-single-voter", "toq-single-voter-key"))
	if err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	if len(rd.Messages) != 0 {
		t.Fatalf("single-voter TOQ proposal sent peer messages: %#v", rd.Messages)
	}
	if len(rd.Committed) != 1 || rd.Committed[0].Ref != ref || string(rd.Committed[0].Command.Payload) != "toq-single-voter" {
		t.Fatalf("single-voter TOQ committed commands = %#v, want proposed command for %s", rd.Committed, ref)
	}
	var sawPending, sawPreAccepted, sawCommitted bool
	for _, rec := range rd.Records {
		if rec.Ref != ref {
			continue
		}
		sawPending = sawPending || rec.TOQPending
		sawPreAccepted = sawPreAccepted || rec.Status == StatusPreAccepted
		sawCommitted = sawCommitted || rec.Status == StatusCommitted
	}
	if !sawPending || !sawPreAccepted || !sawCommitted {
		t.Fatalf("single-voter TOQ records = %#v, want pending, local preaccept assignment, and commit", rd.Records)
	}
	if inst := rn.instances[ref]; inst == nil || inst.rec.Status != StatusExecuted || inst.rec.TOQPending {
		t.Fatalf("single-voter TOQ instance = %#v, want executed after immediate local commit", inst)
	}
}

func TestStepRejectsTOQFlagMismatchWithNodeMode(t *testing.T) {
	cmd := toqTestCommand(221, "toq-step-flag", "toq-step-flag-key")
	nonTOQ, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	flagged := Message{Type: MsgPreAccept, From: 2, To: 1, Ref: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 2}, TOQ: true, ProcessAt: 9, Command: cmd}
	if err := flagged.Validate(nonTOQ.q.conf); err != nil {
		t.Fatalf("test setup flagged TOQ message failed validation before Step mode check: %v", err)
	}
	if err := nonTOQ.Step(flagged); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("non-TOQ Step(flagged TOQ PreAccept) err=%v, want %v", err, ErrMessageRejected)
	}

	clock := uint64(1)
	toq := newTOQTestRawNode(t, 1, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 3, 3: 5})
	unflaggedTimed := Message{Type: MsgPreAccept, From: 2, To: 1, Ref: InstanceRef{Replica: 2, Instance: 2, Conf: 1}, Ballot: Ballot{Replica: 2}, ProcessAt: 9, Seq: 1, Deps: []InstanceNum{0, 0, 0}, Command: cmd}
	if err := unflaggedTimed.Validate(toq.q.conf); err != nil {
		t.Fatalf("test setup unflagged timed message failed validation before Step TOQ check: %v", err)
	}
	if err := toq.Step(unflaggedTimed); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("TOQ Step(unflagged timed PreAccept) err=%v, want %v", err, ErrMessageRejected)
	}
}

func TestMaybeFinalizePreAcceptLeavesPendingTOQUnfinalized(t *testing.T) {
	clock := uint64(3)
	rn := newTOQTestRawNode(t, 1, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 5, 3: 7})
	rn.maybeFinalizePreAccept(nil)
	if rd := rn.Ready(); !rd.Empty() {
		t.Fatalf("nil preaccept finalization produced Ready work: %#v", rd)
	}

	attrs := Attributes{Seq: 1, Deps: rn.q.deps()}
	inst := &instance{
		rec: InstanceRecord{
			Ref:              InstanceRef{Replica: 1, Instance: 9, Conf: 1},
			Ballot:           Ballot{Replica: 1},
			Status:           StatusNone,
			Seq:              attrs.Seq,
			Deps:             append([]InstanceNum(nil), attrs.Deps...),
			Command:          toqTestCommand(222, "toq-pending-finalize", "toq-pending-finalize-key"),
			FastPathEligible: true,
			ProcessAt:        15,
			TOQPending:       true,
		},
		phase:     phasePreAccept,
		processAt: 15,
		preOK: map[ReplicaID]attrVote{
			1: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
			2: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
			3: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
		},
	}
	rn.instances[inst.rec.Ref] = inst
	rn.maybeFinalizePreAccept(inst)
	if !inst.rec.TOQPending || inst.rec.Status != StatusNone {
		t.Fatalf("pending TOQ record finalized before ProcessAt assignment: %#v", inst.rec)
	}
	if rd := rn.Ready(); !rd.Empty() {
		t.Fatalf("pending TOQ finalization produced Ready work: %#v", rd)
	}
}

func rnDepsForValidation(typ MessageType, conf ConfState) []InstanceNum {
	switch typ {
	case MsgPreAcceptResp, MsgAccept, MsgCommit, MsgPrepareResp, MsgTryPreAccept, MsgTryPreAcceptResp:
		return make([]InstanceNum, len(conf.Voters))
	default:
		return nil
	}
}
