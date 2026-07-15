//go:build kvnode

package main

import (
	"net/http"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

func TestReadyLargerThanQueueAdvancesInCappedPrefixes(t *testing.T) {
	db, err := kv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	node, err := epaxos.NewRawNode(epaxos.Config{
		ID:               1,
		Voters:           []epaxos.ReplicaID{1, 2, 3},
		Storage:          db.EPaxosStorage(),
		RetryTicks:       2,
		RecoveryTicks:    5,
		MaxReadyMessages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	s := &service{
		id:            1,
		node:          node,
		ready:         db,
		db:            db,
		peers:         map[epaxos.ReplicaID]string{1: "http://peer-1", 2: "http://peer-2", 3: "http://peer-3"},
		client:        &http.Client{},
		sendq:         make(chan *outboundFrame, 1),
		retryCapacity: 1,
		waiters:       make(map[epaxos.InstanceRef]*proposalWaiter),
	}
	s.mu.Lock()
	if _, err := s.node.Propose(epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 1}, Footprint: epaxos.Footprint{Points: [][]byte{[]byte("capped")}}}); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	if err := s.drainLocked(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	if !s.admissionBlocked || len(s.sendq) != 1 {
		s.mu.Unlock()
		t.Fatalf("first prefix blocked=%v queue=%d, want blocked with one admitted frame", s.admissionBlocked, len(s.sendq))
	}
	first := <-s.sendq
	s.finishOutboundFrame(first, false)
	if err := s.drainLocked(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	if s.admissionBlocked || len(s.sendq) != 1 || s.node.HasReady() {
		s.mu.Unlock()
		t.Fatalf("second prefix blocked=%v queue=%d ready=%v", s.admissionBlocked, len(s.sendq), s.node.HasReady())
	}
	second := <-s.sendq
	s.finishOutboundFrame(second, false)
	s.mu.Unlock()
}
