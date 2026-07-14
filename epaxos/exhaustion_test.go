package epaxos

import (
	"errors"
	"reflect"
	"testing"
)

func TestLocalInstanceNumberExhaustionNeverWraps(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3)})
	if err != nil {
		t.Fatal(err)
	}
	rn.nextInstance = ^InstanceNum(0)

	last, err := rn.Propose(Command{
		ID:           CommandID{Client: 301, Sequence: 1},
		Payload:      []byte("last-local-instance"),
		ConflictKeys: [][]byte{[]byte("last-local-instance-key")},
	})
	if err != nil {
		t.Fatalf("Propose final representable instance: %v", err)
	}
	if last.Instance != ^InstanceNum(0) || last.Replica != 1 || last.Conf != 1 {
		t.Fatalf("final local ref = %s, want 1.%d@1", last, ^InstanceNum(0))
	}
	instancesBefore := len(rn.instances)
	readyBefore := rn.Ready()

	got, err := rn.Propose(Command{
		ID:           CommandID{Client: 301, Sequence: 2},
		Payload:      []byte("must-not-wrap"),
		ConflictKeys: [][]byte{[]byte("last-local-instance-key")},
	})
	if !errors.Is(err, ErrInstanceExhausted) {
		t.Fatalf("Propose after final instance err=%v, want ErrInstanceExhausted", err)
	}
	if got != (InstanceRef{}) {
		t.Fatalf("exhausted Propose ref=%s, want zero", got)
	}
	if rn.nextInstance != 0 || len(rn.instances) != instancesBefore {
		t.Fatalf("exhausted Propose mutated allocator/state: next=%d instances=%d want next=0 instances=%d", rn.nextInstance, len(rn.instances), instancesBefore)
	}
	if !reflect.DeepEqual(rn.Ready(), readyBefore) {
		t.Fatalf("exhausted Propose changed Ready state: before=%#v after=%#v", readyBefore, rn.Ready())
	}

	confRef, err := rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveVoter, Replica: 3})
	if !errors.Is(err, ErrInstanceExhausted) {
		t.Fatalf("ProposeConfChange after final instance err=%v, want ErrInstanceExhausted", err)
	}
	if confRef != (InstanceRef{}) || rn.pendingConf {
		t.Fatalf("exhausted ProposeConfChange ref/pending=%s/%v, want zero/false", confRef, rn.pendingConf)
	}
}

func TestRestartAtMaximumLocalInstanceRemainsExhausted(t *testing.T) {
	store := NewMemoryStorage()
	ref := InstanceRef{Replica: 1, Instance: ^InstanceNum(0), Conf: 1}
	rec := InstanceRecord{
		Ref:          ref,
		Ballot:       Ballot{Replica: 1},
		RecordBallot: Ballot{Replica: 1},
		Status:       StatusCommitted,
		Seq:          1,
		Deps:         []InstanceNum{0, 0, 0},
		Command:      Command{Kind: CommandNoop},
	}
	rec.Checksum = ChecksumRecord(rec)
	store.Records[ref] = rec

	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	if rn.nextInstance != 0 {
		t.Fatalf("restart next instance=%d, want exhausted sentinel 0", rn.nextInstance)
	}
	got, err := rn.Propose(Command{
		ID:           CommandID{Client: 302, Sequence: 1},
		Payload:      []byte("restart-must-not-wrap"),
		ConflictKeys: [][]byte{[]byte("restart-must-not-wrap-key")},
	})
	if !errors.Is(err, ErrInstanceExhausted) || got != (InstanceRef{}) {
		t.Fatalf("restart exhausted Propose ref/err=%s/%v, want zero/ErrInstanceExhausted", got, err)
	}
	if _, exists := rn.instances[InstanceRef{Replica: 1, Instance: 1, Conf: 1}]; exists {
		t.Fatal("restart exhaustion wrapped and reused local instance 1")
	}
}

func TestRecoveryTicksDefaultMultiplicationOverflow(t *testing.T) {
	maxTick := ^uint64(0)
	exact := maxTick / 3
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: exact})
	if err != nil {
		t.Fatalf("exact representable default RecoveryTicks: %v", err)
	}
	if rn.recoveryTicks != exact*3 {
		t.Fatalf("default RecoveryTicks=%d, want %d", rn.recoveryTicks, exact*3)
	}

	if _, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: exact + 1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("overflowing default RecoveryTicks err=%v, want %v", err, ErrInvalidConfig)
	}
}

func TestTimeOptimizationProcessAtCheckedBeforeMutation(t *testing.T) {
	maxTick := ^uint64(0)
	exact, err := NewRawNode(Config{
		ID:                    1,
		Voters:                makeIDs(3),
		RetryTicks:            1,
		RecoveryTicks:         1,
		TimeOptimization:      true,
		TimeOptimizationTicks: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, exact, 0)
	exact.tick = maxTick - 2
	exact.currentHardState.Tick = exact.tick
	exact.acknowledgedHardState = exact.currentHardState.Clone()
	ref, err := exact.Propose(Command{ID: CommandID{Client: 303, Sequence: 1}, Payload: []byte("exact-max"), ConflictKeys: [][]byte{[]byte("exact-max-key")}})
	if err != nil {
		t.Fatalf("exact-Max ProcessAt proposal: %v", err)
	}
	if got := exact.instances[ref].rec.ProcessAt; got != maxTick {
		t.Fatalf("exact-Max ProcessAt=%d, want %d", got, maxTick)
	}

	overflow, err := NewRawNode(Config{
		ID:                    1,
		Voters:                makeIDs(3),
		RetryTicks:            1,
		RecoveryTicks:         1,
		TimeOptimization:      true,
		TimeOptimizationTicks: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, overflow, 0)
	overflow.tick = maxTick - 2
	overflow.currentHardState.Tick = overflow.tick
	overflow.acknowledgedHardState = overflow.currentHardState.Clone()
	before := snapshotRemainingNodeProtocol(overflow)
	if _, err := overflow.Propose(Command{ID: CommandID{Client: 304, Sequence: 1}, Payload: []byte("overflow"), ConflictKeys: [][]byte{[]byte("overflow-key")}}); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("overflowing ProcessAt err=%v, want %v", err, ErrLogicalTimeExhausted)
	}
	requireRemainingNodeProtocolUnchanged(t, overflow, before)
}

func TestTickAtMaximumIsAtomicError(t *testing.T) {
	rn, err := NewRawNode(Config{ID: 1, Voters: makeIDs(3), RetryTicks: 1, RecoveryTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	remainingApplyHardStateOnly(t, rn, 0)
	rn.tick = ^uint64(0)
	rn.currentHardState.Tick = rn.tick
	rn.acknowledgedHardState = rn.currentHardState.Clone()
	before := snapshotRemainingNodeProtocol(rn)
	if err := rn.Tick(); !errors.Is(err, ErrLogicalTimeExhausted) {
		t.Fatalf("Tick at MaxUint64 err=%v, want %v", err, ErrLogicalTimeExhausted)
	}
	requireRemainingNodeProtocolUnchanged(t, rn, before)
}
