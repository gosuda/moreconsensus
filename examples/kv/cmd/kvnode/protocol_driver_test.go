//go:build kvnode

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"gosuda.org/moreconsensus/epaxos"
)

type manualLogicalTickSource struct {
	pulses chan struct{}
	stop   chan struct{}
}

func newManualLogicalTickSource() *manualLogicalTickSource {
	return &manualLogicalTickSource{pulses: make(chan struct{}, 8), stop: make(chan struct{})}
}

func (s *manualLogicalTickSource) C() <-chan struct{} { return s.pulses }

func (s *manualLogicalTickSource) Stop() {
	select {
	case <-s.stop:
		return
	default:
		close(s.stop)
		close(s.pulses)
	}
}

func (s *manualLogicalTickSource) Pulse() { s.pulses <- struct{}{} }

type failingReadyApplier struct{ err error }

func (a failingReadyApplier) ApplyReady(epaxos.Ready) error { return a.err }

func (a failingReadyApplier) LoadInstance(epaxos.InstanceRef) (epaxos.InstanceRecord, bool, error) {
	return epaxos.InstanceRecord{}, false, a.err
}

func TestProtocolLoopConsumesManualLogicalPulses(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2, 3})
	source := newManualLogicalTickSource()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.protocolLoop(ctx, source)
		close(done)
	}()

	s.mu.Lock()
	ref, err := s.node.Propose(epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 1}, ConflictKeys: [][]byte{[]byte("tick")}})
	if err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	if err := s.drainLocked(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	before := s.node.RuntimeStats().OldestUnexecutedAgeTicks
	s.mu.Unlock()

	source.Pulse()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		after := s.node.RuntimeStats().OldestUnexecutedAgeTicks
		s.mu.Unlock()
		if after > before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("logical pulse did not age %s beyond %d ticks", ref, before)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

func TestProposalWaiterCancellationRemovesOnlyMatchingWaiter(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2, 3})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- s.proposeAndWait(ctx, epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 2}, ConflictKeys: [][]byte{[]byte("cancel")}})
	}()

	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		registered := len(s.waiters) == 1
		s.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("proposal waiter was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("proposal result=%v, want context cancellation", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiters) != 0 {
		t.Fatalf("waiters retained after cancellation: %d", len(s.waiters))
	}
}

func TestProtocolTerminalErrorStopsTransitionsAndCompletesWaiters(t *testing.T) {
	s := newTestClusterService(t, []epaxos.ReplicaID{1, 2, 3})
	applyErr := errors.New("durable apply failed")
	s.ready = failingReadyApplier{err: applyErr}
	waiter := &proposalWaiter{result: make(chan error, 1)}

	s.mu.Lock()
	ref, err := s.node.Propose(epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 3}, ConflictKeys: [][]byte{[]byte("terminal")}})
	if err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.waiters = map[epaxos.InstanceRef]*proposalWaiter{ref: waiter}
	s.protocolPulseLocked()
	terminal := s.terminalErr
	before := s.node.RuntimeStats()
	s.protocolPulseLocked()
	after := s.node.RuntimeStats()
	s.mu.Unlock()

	if !errors.Is(terminal, applyErr) {
		t.Fatalf("terminal error=%v, want %v", terminal, applyErr)
	}
	if got := <-waiter.result; got != terminal {
		t.Fatalf("waiter error=%v, want exact terminal error %v", got, terminal)
	}
	if before != after {
		t.Fatalf("terminal protocol pulse mutated runtime stats: before=%+v after=%+v", before, after)
	}
}
