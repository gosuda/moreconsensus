//go:build kvnode

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
)

func TestRetentionThresholdsPreserveSettlementAndFailClosedAtLimit(t *testing.T) {
	s := newTestService(t)
	s.retention.maxResidentInstances = 10
	for sequence := uint64(1); sequence <= 8; sequence++ {
		if err := s.proposeAndWait(context.Background(), epaxos.Command{
			ID:           epaxos.CommandID{Client: 1, Sequence: sequence},
			ConflictKeys: [][]byte{[]byte(fmt.Sprintf("retention-%d", sequence))},
		}); err != nil {
			t.Fatalf("proposal %d: %v", sequence, err)
		}
	}
	s.mu.Lock()
	s.evaluateRetentionLocked()
	if s.retentionLevel != 1 {
		t.Fatalf("80%% retention level=%d, want warning", s.retentionLevel)
	}
	s.mu.Unlock()
	ready := httptest.NewRecorder()
	s.handleReady(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("80%% readiness status=%d body=%q", ready.Code, ready.Body.String())
	}

	if err := s.proposeAndWait(context.Background(), epaxos.Command{
		ID:           epaxos.CommandID{Client: 1, Sequence: 9},
		ConflictKeys: [][]byte{[]byte("retention-9")},
	}); err != nil {
		t.Fatalf("proposal crossing 90%% threshold did not settle: %v", err)
	}
	pressure := httptest.NewRecorder()
	s.handleReady(pressure, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if pressure.Code != http.StatusServiceUnavailable || pressure.Body.String() != "retention-pressure\n" {
		t.Fatalf("90%% readiness status=%d body=%q", pressure.Code, pressure.Body.String())
	}
	if err := s.proposeAndWait(context.Background(), epaxos.Command{
		ID:           epaxos.CommandID{Client: 1, Sequence: 10},
		ConflictKeys: [][]byte{[]byte("retention-10")},
	}); !errors.Is(err, errRetentionPressure) {
		t.Fatalf("proposal above 90%% error=%v, want retention pressure", err)
	}

	s.mu.Lock()
	beforeTick := s.node.Status().Tick
	s.protocolPulseLocked()
	afterTick := s.node.Status().Tick
	if afterTick != beforeTick+1 {
		s.mu.Unlock()
		t.Fatalf("pressure settlement tick=%d, want %d", afterTick, beforeTick+1)
	}
	s.retention.maxResidentInstances = 9
	s.evaluateRetentionLocked()
	terminalTick := s.node.Status().Tick
	s.protocolPulseLocked()
	stoppedTick := s.node.Status().Tick
	s.mu.Unlock()
	if !errors.Is(s.terminalErr, errRetentionLimit) || s.terminalReason != "retention-limit" {
		t.Fatalf("terminal error=%v reason=%q", s.terminalErr, s.terminalReason)
	}
	if stoppedTick != terminalTick {
		t.Fatalf("terminal pulse advanced tick %d -> %d", terminalTick, stoppedTick)
	}
	limited := httptest.NewRecorder()
	s.handleReady(limited, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if limited.Code != http.StatusServiceUnavailable || limited.Body.String() != "retention-limit\n" {
		t.Fatalf("limit readiness status=%d body=%q", limited.Code, limited.Body.String())
	}
}

func TestDurableRecordRetentionCountTriggersStartupEquivalentLimit(t *testing.T) {
	s := newTestService(t)
	if err := s.proposeAndWait(context.Background(), epaxos.Command{
		ID:           epaxos.CommandID{Client: 1, Sequence: 1},
		ConflictKeys: [][]byte{[]byte("durable-limit")},
	}); err != nil {
		t.Fatal(err)
	}
	if count := s.db.StorageStats().DurableInstanceRecords; count == 0 {
		t.Fatal("proposal did not create a durable record")
	}
	s.mu.Lock()
	s.retention.maxDurableRecords = 1
	s.evaluateRetentionLocked()
	s.mu.Unlock()
	if !errors.Is(s.terminalErr, errRetentionLimit) {
		t.Fatalf("durable record terminal error=%v", s.terminalErr)
	}
	metrics := httptest.NewRecorder()
	s.handleMetrics(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, metric := range []string{"kvnode_retention_level 3", "kvnode_storage_durable_instance_records 1"} {
		if !strings.Contains(metrics.Body.String(), metric) {
			t.Fatalf("metrics missing %q:\n%s", metric, metrics.Body.String())
		}
	}
}
