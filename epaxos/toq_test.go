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
	conf := ConfState{ID: 1, Voters: makeIDs(voters)}
	runtime := &TOQRuntimeConfig{Conf: conf, OneWayDelay: delays, SyncGroup: syncGroup}
	rn, err := NewRawNode(Config{
		ID:            id,
		Voters:        conf.Voters,
		Storage:       store,
		RetryTicks:    2,
		RecoveryTicks: 10,
		TOQ:           true,
		TOQClock:      func() uint64 { return *clock },
		TOQRuntime:    runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	applyInitialTOQHardState(t, store, rn)
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
		if msg.Ballot != (Ballot{Replica: ref.Replica}) ||
			msg.RecordBallot != (Ballot{}) ||
			msg.RecordStatus != StatusNone {
			t.Fatalf("TOQ PreAccept message metadata = %#v, want default owner ballot and zero record metadata", msg)
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

func applyInitialTOQHardState(t *testing.T, store *MemoryStorage, rn *RawNode) {
	t.Helper()
	if !rn.HasReady() {
		return
	}
	applyTOQHardStateOnly(t, store, rn, rn.tick)
}

func applyTOQHardStateOnly(t *testing.T, store *MemoryStorage, rn *RawNode, wantTick uint64) {
	t.Helper()
	rd := rn.Ready()
	if rd.HardState.Empty() || !rd.HardState.Equal(rn.currentHardState) || rd.HardState.Tick != wantTick || !rd.MustSync {
		t.Fatalf("TOQ hard-state-only Ready = %#v, want exact current hard state at tick %d", rd, wantTick)
	}
	if len(rd.Records) != 0 || len(rd.Messages) != 0 || len(rd.Committed) != 0 {
		t.Fatalf("TOQ hard-state-only Ready at tick %d carried protocol payload: %#v", wantTick, rd)
	}
	applyAndAdvanceTOQReady(t, store, rn, rd)
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
	if err := rn.ProcessTOQ(); err != nil {
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
	if inst == nil || inst.rec.TOQPending || inst.rec.Status != StatusPreAccepted || inst.rec.ProcessAt != 0 || inst.preOK.len() != 1 {
		t.Fatalf("local immediate TOQ instance = %#v, want assigned local preaccept vote at ProcessAt 0", inst)
	}
	if resident, _ := rn.engine.keyMax(1, []byte("toq-immediate-key"), instanceLane{conf: 1, replica: 1}); resident != ref.Instance {
		t.Fatalf("immediate TOQ command conflict index = %d, want %s", resident, ref)
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
	if err := rn.ProcessTOQ(); err != nil {
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
	if r1, _ := rn.engine.keyMax(ref.Conf, []byte("toq-zero-order-key"), instanceLane{conf: ref.Conf, replica: ref.Replica}); r1 != ref.Instance {
		t.Fatalf("TOQ zero-timestamp conflict index missing candidate %s (got %d)", ref, r1)
	}
	if r2, _ := rn.engine.keyMax(laterRef.Conf, []byte("toq-zero-order-key"), instanceLane{conf: laterRef.Conf, replica: laterRef.Replica}); r2 != laterRef.Instance {
		t.Fatalf("TOQ zero-timestamp conflict index missing later %s (got %d)", laterRef, r2)
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
	if inst.rec.Status != StatusNone || !inst.rec.TOQPending || inst.preOK.len() != 0 {
		t.Fatalf("local TOQ instance before ProcessAt = phase %d record %#v preOK %#v, want pending without local preaccept vote", inst.phase, inst.rec, inst.preOK)
	}
	if resident, _ := rn.engine.keyMax(inst.rec.Ref.Conf, []byte("toq-pending-key"), instanceLane{conf: inst.rec.Ref.Conf, replica: inst.rec.Ref.Replica}); resident != 0 {
		t.Fatalf("TOQ pending command was indexed as an ordinary conflict before ProcessAt: resident=%d", resident)
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
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
	assignedReady := rn.Ready()
	assigned := optimizedRequireRecord(t, assignedReady, ref)
	if assigned.Status != StatusPreAccepted || assigned.TOQPending || assigned.ProcessAt != processAt || assigned.Seq != 1 || !assigned.FastPathEligible {
		t.Fatalf("TOQ ProcessAt assignment record = %#v, want preaccepted fast-path attrs at ProcessAt %d", assigned, processAt)
	}
	if !instanceNumsEqual(assigned.Deps, []InstanceNum{0, 0, 0}) {
		t.Fatalf("TOQ ProcessAt deps = %v, want no conflicts", assigned.Deps)
	}
	inst := rn.instances[ref]
	if inst == nil || inst.rec.TOQPending || inst.rec.Status != StatusPreAccepted || inst.preOK.len() != 1 {
		t.Fatalf("local TOQ instance after ProcessAt = %#v preOK %#v, want one local preaccept vote", inst, inst.preOK)
	}
	if resident, _ := rn.engine.keyMax(1, []byte("toq-assign-key"), instanceLane{conf: 1, replica: 1}); resident != ref.Instance {
		t.Fatalf("TOQ command conflict index after ProcessAt = %d, want %s", resident, ref)
	}
	applyAndAdvanceTOQReady(t, store, rn, assignedReady)

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	applyTOQHardStateOnly(t, store, rn, 1)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	retry := rn.Ready()
	if len(retry.Records) != 0 || len(retry.Committed) != 0 {
		t.Fatalf("TOQ PreAccept retry changed durable/application work: records=%#v committed=%#v", retry.Records, retry.Committed)
	}
	requireTOQPreAccepts(t, retry.Messages, ref, processAt, []ReplicaID{2, 3})
}

func TestTOQPendingRetryCannotStartAcceptBeforeProcessAt(t *testing.T) {
	clock := uint64(5)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 4, 3: 7})

	ref, err := rn.Propose(toqTestCommand(213, "pending-retry", "pending-retry-key"))
	if err != nil {
		t.Fatal(err)
	}
	initial := rn.Ready()
	pending := optimizedRequireRecord(t, initial, ref)
	if !pending.TOQPending || pending.Status != StatusNone || pending.ProcessAt <= clock {
		t.Fatalf("initial record = %#v, want pending TOQ record after clock %d", pending, clock)
	}
	applyAndAdvanceTOQReady(t, store, rn, initial)

	inst := rn.instances[ref]
	for _, from := range []ReplicaID{2, 3} {
		resp := Message{Type: MsgPreAcceptResp, From: from,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    1,
			Deps:   []InstanceNum{0, 0, 0}, FastPathEligible: true}
		if err := rn.Step(resp); err != nil {
			t.Fatalf("step preaccept response from %d: %v", from, err)
		}
	}
	if got, want := inst.preOK.len(), rn.slowQuorumForConf(ref.Conf); got != want {
		t.Fatalf("preaccept votes = %d, want slow quorum %d", got, want)
	}

	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	applyTOQHardStateOnly(t, store, rn, 1)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	retry := rn.Ready()
	for _, msg := range retry.Messages {
		if msg.Type == MsgAccept {
			t.Fatalf("retry before ProcessAt emitted Accept: %#v", msg)
		}
	}
	requireTOQPreAccepts(t, retry.Messages, ref, pending.ProcessAt, []ReplicaID{2, 3})
	if inst.phase != phasePreAccept || inst.rec.Status != StatusNone || !inst.rec.TOQPending {
		t.Fatalf("retry before ProcessAt mutated pending instance: %#v", inst)
	}
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
		ProcessAt:        40,
		TimingDomain:     TimingDomainTOQ,
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
	if inst.phase != phasePreAccept || inst.rec.Status != StatusNone || !inst.rec.TOQPending || inst.rec.ProcessAt != processAt || !commandEqual(inst.rec.Command, cmd) || inst.preOK.len() != 0 {
		t.Fatalf("restarted local TOQ instance before ProcessAt = %#v preOK %#v, want pending command without local preaccept vote", inst, inst.preOK)
	}
	if r, _ := restarted.engine.keyMax(conflictRef.Conf, []byte("toq-restart-key"), instanceLane{conf: conflictRef.Conf, replica: conflictRef.Replica}); r != conflictRef.Instance {
		t.Fatalf("restart conflict index before ProcessAt missing preexisting %s (got %d)", conflictRef, r)
	}
	if r, _ := restarted.engine.keyMax(ref.Conf, []byte("toq-restart-key"), instanceLane{conf: ref.Conf, replica: ref.Replica}); r != 0 {
		t.Fatalf("restart conflict index before ProcessAt indexed pending command: resident=%d", r)
	}

	if err := restarted.Tick(); err != nil {
		t.Fatal(err)
	}
	applyTOQHardStateOnly(t, store, restarted, 1)
	if err := restarted.Tick(); err != nil {
		t.Fatal(err)
	}
	beforeProcessAtRetry := restarted.Ready()
	if len(beforeProcessAtRetry.Records) != 0 || len(beforeProcessAtRetry.Committed) != 0 {
		t.Fatalf("TOQ retry before ProcessAt produced durable/application work: records=%#v committed=%#v", beforeProcessAtRetry.Records, beforeProcessAtRetry.Committed)
	}
	requireTOQPreAccepts(t, beforeProcessAtRetry.Messages, ref, processAt, []ReplicaID{2, 3})
	if inst.rec.Status != StatusNone || !inst.rec.TOQPending {
		t.Fatalf("TOQ retry before ProcessAt mutated pending record to %#v", inst.rec)
	}
	if r, _ := restarted.engine.keyMax(ref.Conf, []byte("toq-restart-key"), instanceLane{conf: ref.Conf, replica: ref.Replica}); r != 0 {
		t.Fatalf("retry before ProcessAt indexed pending command: resident=%d", r)
	}
	applyAndAdvanceTOQReady(t, store, restarted, beforeProcessAtRetry)

	clock = processAt
	if err := restarted.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
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
	if r1, _ := restarted.engine.keyMax(ref.Conf, []byte("toq-restart-key"), instanceLane{conf: ref.Conf, replica: ref.Replica}); r1 != ref.Instance {
		t.Fatalf("conflict index after restarted TOQ assignment missing local %s (got %d)", ref, r1)
	}
	if r2, _ := restarted.engine.keyMax(conflictRef.Conf, []byte("toq-restart-key"), instanceLane{conf: conflictRef.Conf, replica: conflictRef.Replica}); r2 != conflictRef.Instance {
		t.Fatalf("conflict index after restarted TOQ assignment missing preexisting %s (got %d)", conflictRef, r2)
	}
	applyAndAdvanceTOQReady(t, store, restarted, assignedReady)
	stored, ok := store.Instance(ref)
	if !ok {
		t.Fatalf("missing persisted restarted TOQ assignment for %s", ref)
	}
	if !instanceRecordEqual(stored, assigned) {
		t.Fatalf("persisted restarted TOQ assignment = %#v, want %#v", stored, assigned)
	}

	if err := restarted.Tick(); err != nil {
		t.Fatal(err)
	}
	applyTOQHardStateOnly(t, store, restarted, 3)
	if err := restarted.Tick(); err != nil {
		t.Fatal(err)
	}
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

	confRef, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3})
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
	before := snapshotRemainingNodeProtocol(restarted)
	if _, err := restarted.Propose(toqTestCommand(212, "blocked-by-pending-conf", "blocked-by-pending-conf-key")); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("Propose with restarted pending TOQ config change err=%v, want %v", err, ErrMessageRejected)
	}
	if _, err := restarted.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 2}); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("ProposeConfChange with restarted pending TOQ config change err=%v, want %v", err, ErrMessageRejected)
	}
	requireRemainingNodeProtocolUnchanged(t, restarted, before)
}

func TestTOQConfigReplayRestartUsesReplayedCurrentVoters(t *testing.T) {
	newReplayStore := func() *MemoryStorage {
		store := NewMemoryStorage()
		addRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		removeRef := InstanceRef{Replica: 2, Instance: 1, Conf: 2}

		store.Records[addRef] = checkedRecord(InstanceRecord{
			Ref:              addRef,
			Ballot:           Ballot{Replica: 1},
			Status:           StatusExecuted,
			Seq:              1,
			Deps:             []InstanceNum{0, 0, 0},
			Command:          confChangeCommand(ConfChange{Type: ConfChangeAddVoter, Replica: 4}),
			FastPathEligible: true,
			ProcessAt:        1,
			TimingDomain:     TimingDomainTOQ,
		})
		store.Records[removeRef] = checkedRecord(InstanceRecord{
			Ref:              removeRef,
			Ballot:           Ballot{Replica: 2},
			Status:           StatusExecuted,
			Seq:              1,
			Deps:             []InstanceNum{0, 0, 0, 0},
			Command:          confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3}),
			FastPathEligible: true,
			ProcessAt:        2,
			TimingDomain:     TimingDomainTOQ,
		})
		return store
	}
	clock := uint64(80)
	newConfig := func(store *MemoryStorage) Config {
		return Config{
			ID:            1,
			Voters:        makeIDs(3),
			Storage:       store,
			RetryTicks:    2,
			RecoveryTicks: 10,
			TOQ:           true,
			TOQClock:      func() uint64 { return clock },
			TOQRuntime: &TOQRuntimeConfig{
				Conf:        ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}},
				OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 5, 4: 9},
			},
		}
	}

	t.Run("implicit sync group uses replayed current voters", func(t *testing.T) {
		restarted, err := NewRawNode(newConfig(newReplayStore()))
		if err != nil {
			t.Fatal(err)
		}
		assertConfState(t, restarted.Status().Conf, ConfState{ID: 3, Voters: []ReplicaID{1, 2, 4}})
		if restarted.toqActive == nil || !sameReplicaIDs(restarted.toqActive.group, []ReplicaID{1, 2, 4}) {
			t.Fatalf("restarted TOQ sync group = %#v, want replayed current voters [1 2 4]", restarted.toqActive)
		}
		for id, want := range map[ReplicaID]uint64{1: 0, 2: 5, 4: 9} {
			if got, ok := restarted.toqActive.delays[id]; !ok || got != want {
				t.Fatalf("restarted TOQ delay for voter %d = %d, ok=%v; want %d", id, got, ok, want)
			}
		}
		if _, ok := restarted.toqActive.delays[3]; ok {
			t.Fatalf("restarted TOQ delay state included removed voter 3: %#v", restarted.toqActive.delays)
		}
	})

	t.Run("implicit sync group requires delay for replayed remote voter", func(t *testing.T) {
		cfg := newConfig(newReplayStore())
		cfg.TOQRuntime.OneWayDelay = map[ReplicaID]uint64{1: 0, 2: 5}
		_, err := NewRawNode(cfg)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewRawNode with missing delay for replayed voter 4 err=%v, want %v", err, ErrInvalidConfig)
		}
	})

	t.Run("explicit stale sync group rejects removed voter", func(t *testing.T) {
		cfg := newConfig(newReplayStore())
		cfg.TOQRuntime.SyncGroup = []ReplicaID{1, 2, 3}
		cfg.TOQRuntime.OneWayDelay[3] = 7
		_, err := NewRawNode(cfg)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewRawNode with stale TOQ sync group %v err=%v, want %v", cfg.TOQRuntime.SyncGroup, err, ErrInvalidConfig)
		}
	})
}

func TestTOQAppliedSuccessorWithoutRuntimeInputsRejectsLocalProposals(t *testing.T) {
	clock := uint64(90)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 2, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 7})

	confRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{
		Type: MsgCommit, From: 1, To: 1, Ref: confRef,
		Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1},
		Seq: 1, Deps: []InstanceNum{0, 0}, ProcessAt: clock, TOQ: true,
		Command: confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 2}),
	}); err != nil {
		t.Fatal(err)
	}
	assertConfState(t, rn.Status().Conf, ConfState{ID: 2, Voters: []ReplicaID{1}})
	if rn.toqActive != nil || rn.Status().TOQAvailable {
		t.Fatalf("successor TOQ availability active/status=%#v/%v, want unavailable", rn.toqActive, rn.Status().TOQAvailable)
	}
	rd := rn.Ready()
	var executed InstanceRecord
	for _, rec := range rd.Records {
		if rec.Ref == confRef && rec.Status == StatusExecuted {
			executed = rec
		}
	}
	if executed.ConfChangeResult.Outcome != ConfChangeApplied {
		t.Fatalf("executed config result=%#v, want Applied despite unavailable successor TOQ", executed.ConfChangeResult)
	}
	assertConfState(t, executed.ConfChangeResult.Conf, ConfState{ID: 2, Voters: []ReplicaID{1}})
	applyAndAdvanceTOQReady(t, store, rn, rd)
	if rn.HasReady() {
		t.Fatalf("config Ready remained after Advance: %#v", rn.Ready())
	}

	before := snapshotRemainingNodeProtocol(rn)
	if _, err := rn.Propose(toqTestCommand(220, "toq-unavailable", "toq-unavailable-key")); !errors.Is(err, ErrTOQConfigUnavailable) {
		t.Fatalf("Propose with unavailable successor TOQ err=%v, want %v", err, ErrTOQConfigUnavailable)
	}
	if _, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, Replica: 3}); !errors.Is(err, ErrVoterCertificateRequired) {
		t.Fatalf("legacy AddVoter with unavailable successor TOQ err=%v, want %v", err, ErrVoterCertificateRequired)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)
}

func TestTOQAppliedSuccessorUsesPreprovisionedImplicitBound(t *testing.T) {
	clock := uint64(100)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 2, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 7})
	if err := rn.RefreshTOQConfig(TOQRuntimeConfig{
		Conf:        ConfState{ID: 2, Voters: []ReplicaID{1}},
		OneWayDelay: map[ReplicaID]uint64{1: 0},
	}); err != nil {
		t.Fatal(err)
	}

	confRef := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	if err := rn.Step(Message{
		Type: MsgCommit, From: 1, To: 1, Ref: confRef,
		Ballot: Ballot{Replica: 1}, RecordBallot: Ballot{Replica: 1},
		Seq: 1, Deps: []InstanceNum{0, 0}, ProcessAt: clock, TOQ: true,
		Command: confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 2}),
	}); err != nil {
		t.Fatal(err)
	}
	if rn.toqActive == nil || !rn.Status().TOQAvailable || !sameReplicaIDs(rn.toqActive.group, []ReplicaID{1}) {
		t.Fatalf("preprovisioned successor TOQ active/status=%#v/%v", rn.toqActive, rn.Status().TOQAvailable)
	}
	oldRuntime, oldOK := rn.toqRuntimes[1]
	newRuntime, newOK := rn.toqRuntimes[2]
	if !oldOK || !newOK || !sameReplicaIDs(oldRuntime.conf.Voters, []ReplicaID{1, 2}) ||
		!sameReplicaIDs(oldRuntime.group, []ReplicaID{1, 2}) ||
		!sameReplicaIDs(newRuntime.conf.Voters, []ReplicaID{1}) ||
		!sameReplicaIDs(newRuntime.group, []ReplicaID{1}) {
		t.Fatalf("per-configuration runtime history mixed epochs: old=%#v/%v new=%#v/%v", oldRuntime, oldOK, newRuntime, newOK)
	}
	applyAndAdvanceTOQReady(t, store, rn, rn.Ready())

	ref, err := rn.Propose(toqTestCommand(221, "toq-successor", "toq-successor-key"))
	if err != nil {
		t.Fatal(err)
	}
	if ref.Conf != 2 {
		t.Fatalf("successor TOQ proposal ref=%s, want configuration 2", ref)
	}
	rec := rn.instances[ref].rec
	if !rec.TOQPending || rec.ProcessAt != clock {
		t.Fatalf("successor TOQ record=%#v, want pending at %d", rec, clock)
	}
}

func TestTOQAppliedSuccessorRemovingExplicitGroupMemberRejectsLocalProposals(t *testing.T) {
	clock := uint64(110)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, []ReplicaID{1, 2}, map[ReplicaID]uint64{1: 0, 2: 4})
	ref := InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	rec := checkedRecord(InstanceRecord{
		Ref:          ref,
		Ballot:       Ballot{Replica: 1},
		RecordBallot: Ballot{Replica: 1},
		Status:       StatusCommitted,
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		Command:      confChangeCommand(ConfChange{Type: ConfChangeRemoveVoter, Replica: 2}),
	})
	rn.instances[ref] = &instance{rec: rec, phase: phaseCommitted}
	rn.markPendingConf(rec)
	rn.tryExecute()

	want := ConfState{ID: 2, Voters: []ReplicaID{1, 3}}
	assertConfState(t, rn.Status().Conf, want)
	result := rn.instances[ref].rec.ConfChangeResult
	if result.Outcome != ConfChangeApplied {
		t.Fatalf("explicit-group removal result=%#v, want Applied", result)
	}
	assertConfState(t, result.Conf, want)
	if rn.toqActive != nil || rn.Status().TOQAvailable {
		t.Fatalf("removed explicit-group member left TOQ available: active/status=%#v/%v", rn.toqActive, rn.Status().TOQAvailable)
	}
	applyAndAdvanceTOQReady(t, store, rn, rn.Ready())
	if _, err := rn.Propose(toqTestCommand(222, "toq-explicit-unavailable", "toq-explicit-unavailable-key")); !errors.Is(err, ErrTOQConfigUnavailable) {
		t.Fatalf("Propose after explicit-group member removal err=%v, want %v", err, ErrTOQConfigUnavailable)
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
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	rec := optimizedRequireRecord(t, rd, ref)
	wantDeps := []InstanceNum{0, 1, 0}
	if rec.Status != StatusPreAccepted || rec.Seq != 4 || !instanceNumsEqual(rec.Deps, wantDeps) || !rec.FastPathEligible {
		t.Fatalf("receiver timely TOQ record = %#v, want computed fast attrs seq 4 deps %v", rec, wantDeps)
	}
	resp := optimizedRequireMessage(t, rd.Messages, MsgPreAcceptResp, 1)
	if resp.Ref != ref || resp.Ballot != (Ballot{Replica: 1}) || resp.RecordBallot != (Ballot{}) || resp.RecordStatus != StatusNone ||
		resp.Seq != 4 || !instanceNumsEqual(resp.Deps, wantDeps) || !resp.FastPathEligible {
		t.Fatalf("receiver timely TOQ response = %#v, want canonical metadata and computed fast attrs seq 4 deps %v", resp, wantDeps)
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
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
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
		if err := rn.Step(Message{Type: MsgPreAcceptResp, From: from,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    covering.Seq,
			Deps:   append([]InstanceNum(nil), covering.Deps...), FastPathEligible: true,
			DepsCommitted: dependencyMask(covering.Deps)}); err != nil {
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
		if msg.Ref != ref || msg.Ballot != committed.Ballot || msg.RecordBallot == (Ballot{}) || msg.RecordBallot != committed.RecordBallot ||
			msg.RecordStatus != StatusNone || msg.Seq != covering.Seq || !instanceNumsEqual(msg.Deps, covering.Deps) ||
			!commandEqual(msg.Command, committed.Command) {
			t.Fatalf("TOQ fast commit message to %d = %#v, want canonical metadata and covering committed tuple %#v", to, msg, covering)
		}
	}
	if inst.preOK != nil {
		t.Fatalf("TOQ committed instance retained PreAccept votes: %#v", inst.preOK)
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
		if err := rn.ProcessTOQ(); err != nil {
			t.Fatal(err)
		}
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
		if got := inst.preOK.len(); got != 1 {
			t.Fatalf("TOQ local assignment votes = %d, want exactly the local vote before remote responses", got)
		}
		return rn, store, ref, inst, localAttrs
	}

	t.Run("covering remote reply commits at local plus one boundary", func(t *testing.T) {
		rn, _, ref, inst, localAttrs := startScenario(t, 230, "toq-three-fast")
		covering := Attributes{Seq: localAttrs.Seq + 1, Deps: []InstanceNum{0, 1, 1}}
		if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 2,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    covering.Seq,
			Deps:   append([]InstanceNum(nil), covering.Deps...), FastPathEligible: true,
			DepsCommitted: dependencyMask(covering.Deps)}); err != nil {
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
			if msg.Ref != ref || msg.Ballot != committed.Ballot || msg.RecordBallot == (Ballot{}) || msg.RecordBallot != committed.RecordBallot ||
				msg.RecordStatus != StatusNone || msg.Seq != covering.Seq || !instanceNumsEqual(msg.Deps, covering.Deps) ||
				!commandEqual(msg.Command, committed.Command) {
				t.Fatalf("TOQ three-node fast commit message to %d = %#v, want canonical metadata and covering committed tuple %#v", to, msg, covering)
			}
		}
		if inst.preOK != nil {
			t.Fatalf("TOQ three-node committed instance retained PreAccept votes: %#v", inst.preOK)
		}
	})

	t.Run("non-covering remote reply cannot fast commit at same boundary", func(t *testing.T) {
		rn, _, ref, inst, _ := startScenario(t, 231, "toq-three-non-covering")
		nonCovering := Attributes{Seq: 1, Deps: []InstanceNum{0, 0, 0}}
		if err := rn.Step(Message{Type: MsgPreAcceptResp, From: 2,
			To:     1,
			Ref:    ref,
			Ballot: inst.rec.Ballot,
			Seq:    nonCovering.Seq,
			Deps:   append([]InstanceNum(nil), nonCovering.Deps...), FastPathEligible: true,
			DepsCommitted: dependencyMask(nonCovering.Deps)}); err != nil {
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
		for _, to := range []ReplicaID{2, 3} {
			msg := optimizedRequireMessage(t, rd.Messages, MsgAccept, to)
			if msg.Ref != ref || msg.Ballot != accepted.Ballot || msg.RecordBallot != (Ballot{}) || msg.RecordStatus != StatusNone ||
				msg.Seq != accepted.Seq || !instanceNumsEqual(msg.Deps, accepted.Deps) ||
				!commandEqual(msg.Command, accepted.Command) || msg.FastPathEligible != accepted.FastPathEligible {
				t.Fatalf("TOQ slow-path Accept message to %d = %#v, want canonical accepted tuple %#v", to, msg, accepted)
			}
		}
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
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
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
		ID:       1,
		Voters:   makeIDs(3),
		TOQ:      true,
		TOQClock: validClock,
		TOQRuntime: &TOQRuntimeConfig{
			Conf:        ConfState{ID: 1, Voters: makeIDs(3)},
			OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 2, 3: 3},
		},
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
				cfg.TOQRuntime.SyncGroup = []ReplicaID{1, 4}
				cfg.TOQRuntime.OneWayDelay[4] = 4
				return cfg
			},
		},
		{
			name: "sync group contains duplicate voter",
			mutate: func(cfg Config) Config {
				cfg.TOQRuntime.SyncGroup = []ReplicaID{1, 1}
				return cfg
			},
		},
		{
			name: "missing non-local one-way delay",
			mutate: func(cfg Config) Config {
				cfg.TOQRuntime.SyncGroup = []ReplicaID{1, 2}
				cfg.TOQRuntime.OneWayDelay = map[ReplicaID]uint64{1: 0}
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
		Ref:          InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot:       Ballot{Replica: 1},
		Status:       StatusNone,
		Deps:         []InstanceNum{0, 0, 0},
		Command:      toqTestCommand(218, "toq-pending-without-config", "toq-pending-without-config-key"),
		ProcessAt:    20,
		TimingDomain: TimingDomainTOQ,
		TOQPending:   true,
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
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
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
	if err := rn.ProcessTOQ(); err != nil {
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
	remainingApplyHardStateOnly(t, nonTOQ, 0)
	before := snapshotRemainingNodeProtocol(nonTOQ)
	flagged := Message{Type: MsgPreAccept, From: 2, To: 1, Ref: InstanceRef{Replica: 2, Instance: 1, Conf: 1}, Ballot: Ballot{Replica: 2}, TOQ: true, ProcessAt: 9, Command: cmd}
	if err := flagged.Validate(nonTOQ.q.conf); err != nil {
		t.Fatalf("test setup flagged TOQ message failed validation before Step mode check: %v", err)
	}
	if err := nonTOQ.Step(flagged); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("non-TOQ Step(flagged TOQ PreAccept) err=%v, want %v", err, ErrMessageRejected)
	}
	requireRemainingNodeProtocolUnchanged(t, nonTOQ, before)

	clock := uint64(1)
	toq := newTOQTestRawNode(t, 1, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 3, 3: 5})
	before = snapshotRemainingNodeProtocol(toq)
	unflaggedTimed := Message{Type: MsgPreAccept, From: 2, To: 1, Ref: InstanceRef{Replica: 2, Instance: 2, Conf: 1}, Ballot: Ballot{Replica: 2}, ProcessAt: 9, Seq: 1, Deps: []InstanceNum{0, 0, 0}, Command: cmd}
	if err := unflaggedTimed.Validate(toq.q.conf); err != nil {
		t.Fatalf("test setup unflagged timed message failed validation before Step TOQ check: %v", err)
	}
	if err := toq.Step(unflaggedTimed); !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("TOQ Step(unflagged timed PreAccept) err=%v, want %v", err, ErrMessageRejected)
	}
	requireRemainingNodeProtocolUnchanged(t, toq, before)
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
		preOK: testAttrVoteSet(t, rn.q.conf, map[ReplicaID]testAttrVote{
			1: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
			2: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
			3: {seq: attrs.Seq, deps: append([]InstanceNum(nil), attrs.Deps...), fastPathEligible: true},
		}),
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

func TestTOQExplicitSamplingRollbackAndClosedBoundary(t *testing.T) {
	clock := uint64(10)
	reads := 0
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{
		ID:            2,
		Voters:        makeIDs(3),
		Storage:       store,
		RetryTicks:    2,
		RecoveryTicks: 10,
		TOQ:           true,
		TOQClock: func() uint64 {
			reads++
			return clock
		},
		TOQRuntime: &TOQRuntimeConfig{
			Conf:        ConfState{ID: 1, Voters: makeIDs(3)},
			OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 0, 3: 0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reads != 0 {
		t.Fatalf("NewRawNode sampled TOQ clock %d times, want zero", reads)
	}
	applyInitialTOQHardState(t, store, rn)
	if err := rn.Tick(); err != nil {
		t.Fatal(err)
	}
	if reads != 0 {
		t.Fatalf("Tick sampled TOQ clock %d times, want zero", reads)
	}
	applyTOQHardStateOnly(t, store, rn, 1)

	future := Message{
		Type:      MsgPreAccept,
		From:      1,
		To:        2,
		Ref:       InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		ProcessAt: 20,
		TOQ:       true,
		Ballot:    Ballot{Replica: 1},
		Command:   toqTestCommand(401, "explicit-sample", "explicit-sample-key"),
	}
	if err := rn.Step(future); err != nil {
		t.Fatal(err)
	}
	if reads != 0 {
		t.Fatalf("Step sampled TOQ clock %d times, want zero", reads)
	}
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
	if reads != 1 || !rn.toqClosed || rn.toqClosedThrough != 10 {
		t.Fatalf("ProcessTOQ sample/closure reads=%d closed=%v through=%d, want 1/true/10", reads, rn.toqClosed, rn.toqClosedThrough)
	}

	before := snapshotRemainingNodeProtocol(rn)
	clock = 9
	if err := rn.ProcessTOQ(); !errors.Is(err, ErrTOQClockRollback) {
		t.Fatalf("ProcessTOQ rollback err=%v, want %v", err, ErrTOQClockRollback)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)

	clock = 10
	ref, err := rn.Propose(toqTestCommand(402, "closed-successor", "closed-successor-key"))
	if err != nil {
		t.Fatal(err)
	}
	if reads != 3 {
		t.Fatalf("proposal clock reads=%d, want rollback attempt plus one proposal sample", reads)
	}
	if got := rn.instances[ref].rec.ProcessAt; got != 11 {
		t.Fatalf("proposal after closed bucket ProcessAt=%d, want 11", got)
	}
}

func TestTOQTimestampOverflowRejectsProtocolMutation(t *testing.T) {
	clock := ^uint64(0)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 1, 3, &clock, store, []ReplicaID{1, 2, 3}, map[ReplicaID]uint64{1: 0, 2: 1, 3: 0})
	next := rn.nextInstance
	if _, err := rn.Propose(toqTestCommand(403, "overflow", "overflow-key")); !errors.Is(err, ErrTOQTimestampOverflow) {
		t.Fatalf("overflow proposal err=%v, want %v", err, ErrTOQTimestampOverflow)
	}
	if rn.nextInstance != next || len(rn.instances) != 0 || len(rn.timers) != 0 || len(rn.deferredIndex) != 0 || rn.HasReady() {
		t.Fatalf("overflow proposal mutated protocol state: next=%d instances=%d timers=%d deferred=%d ready=%#v", rn.nextInstance, len(rn.instances), len(rn.timers), len(rn.deferredIndex), rn.Ready())
	}
	if rn.toqSeen || rn.toqLastNow != 0 {
		t.Fatalf("overflow proposal mutated monotonic observation: seen=%v last=%d", rn.toqSeen, rn.toqLastNow)
	}

	zeroDelay := newTOQTestRawNode(t, 1, 1, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0})
	if err := zeroDelay.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
	if _, err := zeroDelay.Propose(toqTestCommand(404, "closed-max", "closed-max-key")); !errors.Is(err, ErrTOQTimestampOverflow) {
		t.Fatalf("proposal after closing MaxUint64 err=%v, want %v", err, ErrTOQTimestampOverflow)
	}
}

func TestProcessTOQLogicalOverflowIsAtomic(t *testing.T) {
	clock := uint64(10)
	rn := newTOQTestRawNode(t, 2, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
	message := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, ProcessAt: 20, TOQ: true, Ballot: Ballot{Replica: 1}, Command: toqTestCommand(413, "toq-logical-overflow", "toq-logical-overflow-key")}
	if err := rn.Step(message); err != nil {
		t.Fatal(err)
	}
	rn.tick = ^uint64(0)
	rn.currentHardState.Tick = rn.tick
	rn.acknowledgedHardState = rn.currentHardState.Clone()
	clock = 20
	before := snapshotRemainingNodeProtocol(rn)
	if err := rn.ProcessTOQ(); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("ProcessTOQ at exhausted logical time err=%v, want %v", err, ErrLogicalTimeExhausted)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)
}

func TestDeferredTOQUniqueConflictAndBound(t *testing.T) {
	clock := uint64(10)
	store := NewMemoryStorage()
	rn, err := NewRawNode(Config{
		ID:                    2,
		Voters:                makeIDs(3),
		Storage:               store,
		RetryTicks:            2,
		RecoveryTicks:         10,
		TOQ:                   true,
		TOQClock:              func() uint64 { return clock },
		TOQRuntime:            &TOQRuntimeConfig{Conf: ConfState{ID: 1, Voters: makeIDs(3)}, OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 0, 3: 0}},
		MaxDeferredPreAccepts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	applyInitialTOQHardState(t, store, rn)
	original := Message{
		Type:      MsgPreAccept,
		From:      1,
		To:        2,
		Ref:       InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		ProcessAt: 20,
		TOQ:       true,
		Ballot:    Ballot{Replica: 1},
		Command:   toqTestCommand(405, "first", "dedup-key"),
	}
	if err := rn.Step(original); err != nil {
		t.Fatal(err)
	}
	duplicate := original.Clone()
	duplicate.Checksum = ChecksumMessage(duplicate)
	if err := rn.Step(duplicate); err != nil {
		t.Fatalf("exact duplicate with canonical checksum err=%v", err)
	}
	if len(rn.deferredIndex) != 1 || rn.toqPreAccepts.Len() != 1 {
		t.Fatalf("exact duplicate queue size index/heap=%d/%d, want 1/1", len(rn.deferredIndex), rn.toqPreAccepts.Len())
	}
	conflicting := original.Clone()
	conflicting.Command = toqTestCommand(406, "conflict", "dedup-key")
	if err := rn.Step(conflicting); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("conflicting duplicate err=%v, want %v", err, ErrInvalidMessage)
	}
	second := original.Clone()
	second.From = 3
	second.Ref = InstanceRef{Replica: 3, Instance: 1, Conf: 1}
	second.Ballot = Ballot{Replica: 3}
	if err := rn.Step(second); !errors.Is(err, ErrDeferredQueueFull) || !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("bounded queue err=%v, want ErrDeferredQueueFull and ErrMessageRejected", err)
	}
	if len(rn.deferredIndex) != 1 || !deferredMessageEqual(rn.toqPreAccepts[0].message, original) {
		t.Fatalf("conflict/bound replaced first queue entry: %#v", rn.toqPreAccepts)
	}

	clock = 20
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Records) != 1 || rd.Records[0].Ref != original.Ref || len(rd.Messages) != 1 {
		t.Fatalf("deduplicated close Ready records/messages=%#v/%#v, want one original response", rd.Records, rd.Messages)
	}
}

func TestTOQEqualTimestampTotalOrderAndLateFallback(t *testing.T) {
	run := func(reverse bool) Ready {
		clock := uint64(10)
		rn := newTOQTestRawNode(t, 2, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
		first := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, ProcessAt: 20, TOQ: true, Ballot: Ballot{Replica: 1}, Command: toqTestCommand(407, "a", "equal-order-key")}
		second := Message{Type: MsgPreAccept, From: 3, To: 2, Ref: InstanceRef{Replica: 3, Instance: 1, Conf: 1}, ProcessAt: 20, TOQ: true, Ballot: Ballot{Replica: 3}, Command: toqTestCommand(408, "b", "equal-order-key")}
		messages := []Message{first, second}
		if reverse {
			messages[0], messages[1] = messages[1], messages[0]
		}
		for _, message := range messages {
			if err := rn.Step(message); err != nil {
				t.Fatal(err)
			}
		}
		clock = 20
		if err := rn.ProcessTOQ(); err != nil {
			t.Fatal(err)
		}
		return rn.Ready()
	}
	forward := run(false)
	reverse := run(true)
	if len(forward.Records) != 2 || len(reverse.Records) != 2 {
		t.Fatalf("equal-time Ready record counts forward/reverse=%d/%d, want 2/2", len(forward.Records), len(reverse.Records))
	}
	for i := range forward.Records {
		if forward.Records[i].Ref != reverse.Records[i].Ref ||
			!forward.Records[i].Attributes().Equal(reverse.Records[i].Attributes()) {
			t.Fatalf("equal-time order differs at %d: forward=%#v reverse=%#v", i, forward.Records[i], reverse.Records[i])
		}
	}
	if forward.Records[0].Ref.Replica != 1 || forward.Records[1].Ref.Replica != 3 ||
		forward.Records[1].Seq != 2 || !instanceNumsEqual(forward.Records[1].Deps, []InstanceNum{1, 0, 0}) {
		t.Fatalf("equal-time total order records=%#v, want replica 1 then replica 3 depending on it", forward.Records)
	}

	clock := uint64(20)
	late := newTOQTestRawNode(t, 2, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
	if err := late.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
	message := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 2, Conf: 1}, ProcessAt: 20, TOQ: true, Ballot: Ballot{Replica: 1}, Command: toqTestCommand(409, "late", "late-key")}
	if err := late.Step(message); err != nil {
		t.Fatal(err)
	}
	rd := late.Ready()
	if len(rd.Records) != 1 || rd.Records[0].FastPathEligible || len(rd.Messages) != 1 || rd.Messages[0].FastPathEligible {
		t.Fatalf("late equal-time Ready=%#v, want persisted/responded conservative non-fast tuple", rd)
	}
}

func TestTOQRestartRejectsRollbackBelowProcessedFloor(t *testing.T) {
	clock := uint64(30)
	store := NewMemoryStorage()
	rn := newTOQTestRawNode(t, 2, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
	message := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Replica: 1, Instance: 1, Conf: 1}, ProcessAt: 30, TOQ: true, Ballot: Ballot{Replica: 1}, Command: toqTestCommand(410, "restart-floor", "restart-floor-key")}
	if err := rn.Step(message); err != nil {
		t.Fatal(err)
	}
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatal(err)
	}
	assigned := rn.instances[message.Ref].rec
	recoveryBallot := Ballot{Number: 1, Replica: 3}
	if err := rn.Step(Message{Type: MsgAccept, From: 3, To: 2, Ref: message.Ref, ProcessAt: 30, TOQ: true, Ballot: recoveryBallot, Seq: assigned.Seq, Deps: assigned.Deps, Command: assigned.Command}); err != nil {
		t.Fatal(err)
	}
	if got := rn.instances[message.Ref].rec.ProcessAt; got != 30 {
		t.Fatalf("Accept discarded ProcessAt: got %d, want 30", got)
	}
	if err := rn.Step(Message{Type: MsgCommit, From: 3, To: 2, Ref: message.Ref, ProcessAt: 30, TOQ: true, Ballot: recoveryBallot, RecordBallot: recoveryBallot, Seq: assigned.Seq, Deps: assigned.Deps, Command: assigned.Command}); err != nil {
		t.Fatal(err)
	}
	if got := rn.instances[message.Ref].rec.ProcessAt; got != 30 {
		t.Fatalf("Commit discarded ProcessAt: got %d, want 30", got)
	}
	applyAndAdvanceTOQReady(t, store, rn, rn.Ready())
	if rn.HasReady() {
		applyAndAdvanceTOQReady(t, store, rn, rn.Ready())
	}

	clock = 29
	restarted := newTOQTestRawNode(t, 2, 3, &clock, store, nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
	before := snapshotRemainingNodeProtocol(restarted)
	if err := restarted.ProcessTOQ(); !errors.Is(err, ErrTOQClockRollback) {
		t.Fatalf("restart rollback err=%v, want %v", err, ErrTOQClockRollback)
	}
	requireRemainingNodeProtocolUnchanged(t, restarted, before)
}

func TestTOQRuntimeRefreshClonesAndMatchesExactEpoch(t *testing.T) {
	clock := uint64(5)
	rn := newTOQTestRawNode(t, 1, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 2, 3: 3})
	conf := ConfState{ID: 1, Voters: makeIDs(3)}
	delays := map[ReplicaID]uint64{1: 0, 2: 7}
	group := []ReplicaID{1, 2}
	if err := rn.RefreshTOQConfig(TOQRuntimeConfig{Conf: conf, OneWayDelay: delays, SyncGroup: group}); err != nil {
		t.Fatal(err)
	}
	conf.Voters[0] = 99
	delays[2] = 99
	group[0] = 3
	if rn.toqActive == nil || !sameReplicaIDs(rn.toqActive.conf.Voters, makeIDs(3)) ||
		!sameReplicaIDs(rn.toqActive.group, []ReplicaID{1, 2}) || rn.toqActive.delays[2] != 7 {
		t.Fatalf("runtime refresh retained caller buffers: %#v", rn.toqActive)
	}

	before := snapshotRemainingNodeProtocol(rn)
	err := rn.RefreshTOQConfig(TOQRuntimeConfig{
		Conf:        ConfState{ID: 1, Voters: []ReplicaID{1, 2}},
		OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 1},
	})
	if !errors.Is(err, ErrTOQConfigUnavailable) || !errors.Is(err, ErrMessageRejected) {
		t.Fatalf("mismatched known runtime err=%v, want ErrTOQConfigUnavailable and ErrMessageRejected", err)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)

	if err := rn.RefreshTOQConfig(TOQRuntimeConfig{
		Conf:        ConfState{ID: 2, Voters: []ReplicaID{1, 2, 3, 4}},
		OneWayDelay: map[ReplicaID]uint64{1: 0, 2: 2, 3: 3},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("future runtime missing remote bound err=%v, want %v", err, ErrInvalidConfig)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)
}

func rnDepsForValidation(typ MessageType, conf ConfState) []InstanceNum {
	switch typ {
	case MsgPreAcceptResp, MsgAccept, MsgCommit, MsgPrepareResp, MsgTryPreAccept, MsgTryPreAcceptResp:
		return make([]InstanceNum, len(conf.Voters))
	case MsgPreAccept, MsgAcceptResp, MsgPrepare, MsgEvidence, MsgEvidenceResp:
		fallthrough
	default:
		return nil
	}
}

func TestTOQDeferredGenerationPoisonDoesNotBlockLaterDuePreAccept(t *testing.T) {
	clock := uint64(10)
	rn := newTOQTestRawNode(t, 2, 3, &clock, NewMemoryStorage(), nil, map[ReplicaID]uint64{1: 0, 2: 0, 3: 0})
	first := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Conf: 1, Replica: 1, Instance: 1}, Ballot: Ballot{Replica: 1}, ProcessAt: 20, TOQ: true, Command: toqTestCommand(420, "poison-a", "poison-a-key")}
	second := Message{Type: MsgPreAccept, From: 3, To: 2, Ref: InstanceRef{Conf: 1, Replica: 3, Instance: 1}, Ballot: Ballot{Replica: 3}, ProcessAt: 20, TOQ: true, Command: toqTestCommand(421, "valid-b", "valid-b-key")}
	if err := rn.Step(first); err != nil {
		t.Fatal(err)
	}
	if err := rn.Step(second); err != nil {
		t.Fatal(err)
	}
	firstKey := deferredPreAcceptKey{domain: preAcceptTOQ, ref: first.Ref, ballot: first.Ballot, from: first.From}
	firstEntry := rn.deferredIndex[firstKey]
	rec := checkedRecord(InstanceRecord{Ref: first.Ref, Ballot: Ballot{}, RecordBallot: first.Ballot, Status: StatusPreAccepted, Seq: 1, Deps: make([]InstanceNum, 3), Command: first.Command.Clone(), ProcessAt: 19, TimingDomain: TimingDomainTOQ})
	poison := &instance{rec: rec, phase: phaseIdle, generation: ^uint64(0)}
	rn.installInstance(poison)
	clock = 20
	if err := rn.ProcessTOQ(); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("poisoned ProcessTOQ error = %v, want %v", err, ErrLogicalTimeExhausted)
	}
	if len(rn.deferredIndex) != 0 || firstEntry == nil || firstEntry.index != -1 {
		t.Fatalf("poisoned TOQ entry not cleared: entry=%#v deferred=%d", firstEntry, len(rn.deferredIndex))
	}
	if rn.instances[first.Ref] != poison {
		t.Fatalf("poisoned TOQ entry replaced current state: %#v", rn.instances[first.Ref])
	}
	if valid := rn.instances[second.Ref]; valid == nil || valid.rec.Status != StatusPreAccepted || valid.rec.TimingDomain != TimingDomainTOQ {
		t.Fatalf("later valid TOQ entry did not process: %#v", valid)
	}
	if !rn.toqClosed || rn.toqClosedThrough != 20 {
		t.Fatalf("poisoned ProcessTOQ did not close sampled bucket: closed=%v through=%d", rn.toqClosed, rn.toqClosedThrough)
	}
	late := Message{Type: MsgPreAccept, From: 1, To: 2, Ref: InstanceRef{Conf: 1, Replica: 1, Instance: 2}, Ballot: Ballot{Replica: 1}, ProcessAt: 20, TOQ: true, Command: toqTestCommand(422, "late-same-time", "late-same-time-key")}
	if err := rn.Step(late); err != nil {
		t.Fatal(err)
	}
	if len(rn.deferredIndex) != 0 {
		t.Fatalf("late same-time TOQ message re-entered closed bucket: %#v", rn.deferredIndex)
	}
	if got := rn.instances[late.Ref]; got == nil || got.rec.FastPathEligible {
		t.Fatalf("late same-time TOQ message regained fast eligibility: %#v", got)
	}
	if err := rn.ProcessTOQ(); err != nil {
		t.Fatalf("later ProcessTOQ repeated poison error: %v", err)
	}
}
