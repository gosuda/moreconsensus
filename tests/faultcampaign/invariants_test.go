package main

import (
	"path/filepath"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
	"gosuda.org/moreconsensus/examples/kv"
)

func TestDurableInvariantCheckerChosenApplicationAndConflictOrder(t *testing.T) {
	nodes := []*nodeProcess{
		{id: 1, dataDir: filepath.Join(t.TempDir(), "node-1")},
		{id: 2, dataDir: filepath.Join(t.TempDir(), "node-2")},
	}
	records := orderedInvariantRecords("one", "two")
	for _, node := range nodes {
		writeInvariantRecords(t, node.dataDir, records)
	}
	result := inspectDurableCluster(nodes, 2)
	if !result.Valid {
		t.Fatalf("durable checker rejected ordered records: %#v", result)
	}
	if result.ExecutedMutationCount != 2 || result.ConflictPairs != 1 || result.ChosenHash == "" || len(result.NodeHashes) != 2 {
		t.Fatalf("durable checker result=%#v", result)
	}
}

func TestDurableInvariantCheckerRejectsDivergentChosenTuple(t *testing.T) {
	nodes := []*nodeProcess{
		{id: 1, dataDir: filepath.Join(t.TempDir(), "node-1")},
		{id: 2, dataDir: filepath.Join(t.TempDir(), "node-2")},
	}
	writeInvariantRecords(t, nodes[0].dataDir, orderedInvariantRecords("one", "two"))
	writeInvariantRecords(t, nodes[1].dataDir, orderedInvariantRecords("one", "divergent"))
	result := inspectDurableCluster(nodes, 2)
	if result.Valid || result.Error == "" {
		t.Fatalf("durable checker accepted divergent chosen tuple: %#v", result)
	}
}

func TestDurableInvariantCheckerRejectsUnorderedConflictsAndDuplicateApplicationID(t *testing.T) {
	t.Run("unordered conflict", func(t *testing.T) {
		nodes := []*nodeProcess{{id: 1, dataDir: filepath.Join(t.TempDir(), "node-1")}}
		records := orderedInvariantRecords("one", "two")
		records[1].Deps[0] = 0
		records[1].Checksum = epaxos.ChecksumRecord(records[1])
		writeInvariantRecords(t, nodes[0].dataDir, records)
		result := inspectDurableCluster(nodes, 2)
		if result.Valid || result.Error == "" {
			t.Fatalf("durable checker accepted unordered conflicts: %#v", result)
		}
	})
	t.Run("duplicate command ID", func(t *testing.T) {
		nodes := []*nodeProcess{{id: 1, dataDir: filepath.Join(t.TempDir(), "node-1")}}
		records := orderedInvariantRecords("one", "two")
		records[1].Command.ID = records[0].Command.ID
		records[1].Checksum = epaxos.ChecksumRecord(records[1])
		writeInvariantRecords(t, nodes[0].dataDir, records)
		result := inspectDurableCluster(nodes, 2)
		if result.Valid || result.Error == "" {
			t.Fatalf("durable checker accepted duplicate application ID: %#v", result)
		}
	})
}

func TestDependsTransitivelyUsesHistoricalVoterOrderingAndImplicitPrefix(t *testing.T) {
	from := epaxos.InstanceRef{Replica: 4, Instance: 7, Conf: 1}
	records := map[epaxos.InstanceRef]epaxos.InstanceRecord{
		from: {Ref: from, Deps: []epaxos.InstanceNum{5, 3}},
	}
	configs := map[epaxos.ConfID]epaxos.ConfState{
		1: {ID: 1, Voters: []epaxos.ReplicaID{4, 2}},
	}
	for _, target := range []epaxos.InstanceRef{
		{Replica: 4, Instance: 1, Conf: 1},
		{Replica: 2, Instance: 2, Conf: 1},
	} {
		if !dependsTransitively(from, target, records, configs, make(map[epaxos.InstanceRef]struct{})) {
			t.Fatalf("%s did not include historical-slot dependency prefix through %s", from, target)
		}
	}
	if dependsTransitively(from, epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, records, configs, make(map[epaxos.InstanceRef]struct{})) {
		t.Fatal("dependency traversal used numeric replica indexing instead of historical voter ordering")
	}
	if dependsTransitively(from, epaxos.InstanceRef{Replica: 4, Instance: 1, Conf: 1}, records, nil, make(map[epaxos.InstanceRef]struct{})) {
		t.Fatal("dependency traversal accepted a missing historical configuration")
	}
}

func TestSameDurableConfigurationsRejectsReorderedVoters(t *testing.T) {
	left := map[epaxos.ConfID]epaxos.ConfState{1: {ID: 1, Voters: []epaxos.ReplicaID{4, 2}}}
	right := map[epaxos.ConfID]epaxos.ConfState{1: {ID: 1, Voters: []epaxos.ReplicaID{2, 4}}}
	if sameDurableConfigurations(left, right) {
		t.Fatal("durable configuration comparison accepted reordered voters")
	}
}

func orderedInvariantRecords(first, second string) []epaxos.InstanceRecord {
	firstRecord := epaxos.InstanceRecord{
		Ref:    epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Ballot: epaxos.Ballot{Replica: 1}, RecordBallot: epaxos.Ballot{Replica: 1},
		Status: epaxos.StatusExecuted, Seq: 1, Deps: []epaxos.InstanceNum{0, 0},
		Command: epaxos.Command{ID: epaxos.CommandID{Client: 1, Sequence: 1}, Payload: []byte(first), ConflictKeys: [][]byte{[]byte("shared")}},
	}
	firstRecord.Checksum = epaxos.ChecksumRecord(firstRecord)
	secondRecord := epaxos.InstanceRecord{
		Ref:    epaxos.InstanceRef{Replica: 2, Instance: 1, Conf: 1},
		Ballot: epaxos.Ballot{Replica: 2}, RecordBallot: epaxos.Ballot{Replica: 2},
		Status: epaxos.StatusExecuted, Seq: 2, Deps: []epaxos.InstanceNum{1, 0},
		Command: epaxos.Command{ID: epaxos.CommandID{Client: 2, Sequence: 1}, Payload: []byte(second), ConflictKeys: [][]byte{[]byte("shared")}},
	}
	secondRecord.Checksum = epaxos.ChecksumRecord(secondRecord)
	return []epaxos.InstanceRecord{firstRecord, secondRecord}
}

func writeInvariantRecords(t *testing.T, dataDir string, records []epaxos.InstanceRecord) {
	t.Helper()
	database, err := kv.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	hard := epaxos.HardState{Conf: epaxos.ConfState{ID: 1, Voters: []epaxos.ReplicaID{1, 2}}}
	if err := database.EPaxosStorage().ApplyReady(epaxos.Ready{HardState: hard, Records: records, MustSync: true}); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
