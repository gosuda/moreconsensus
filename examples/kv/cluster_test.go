package kv

import (
	"fmt"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
)

func TestClusterReplicatesThroughEPaxos(t *testing.T) {
	paths := make([]string, 3)
	for i := range paths {
		paths[i] = t.TempDir()
	}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	if err := cluster.Put([]byte("shared"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	for id := epaxos.ReplicaID(1); id <= 3; id++ {
		value, ok, err := cluster.Get(id, []byte("shared"))
		if err != nil || !ok || string(value) != "value" {
			t.Fatalf("replica %d value=%q ok=%v err=%v", id, value, ok, err)
		}
	}
	if err := cluster.Delete([]byte("shared")); err != nil {
		t.Fatal(err)
	}
	for id := epaxos.ReplicaID(1); id <= 3; id++ {
		_, ok, err := cluster.Get(id, []byte("shared"))
		if err != nil || ok {
			t.Fatalf("replica %d deleted ok=%v err=%v", id, ok, err)
		}
	}
}

func TestOpenClusterRejectsInvalidSizes(t *testing.T) {
	if _, err := OpenCluster(nil); err == nil {
		t.Fatal("expected empty cluster rejection")
	}
	paths := make([]string, 8)
	for i := range paths {
		paths[i] = fmt.Sprintf("unused-%d", i)
	}
	if _, err := OpenCluster(paths); err == nil {
		t.Fatal("expected large cluster rejection")
	}
}

func TestClusterDrainQuiescesWithoutAdvancingLogicalTime(t *testing.T) {
	paths := make([]string, 3)
	for i := range paths {
		paths[i] = t.TempDir()
	}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()

	before := make(map[epaxos.ReplicaID]uint64, len(cluster.Nodes))
	for id, node := range cluster.Nodes {
		before[id] = node.Status().Tick
	}
	if err := cluster.Drain(); err != nil {
		t.Fatal(err)
	}
	for id, node := range cluster.Nodes {
		if got := node.Status().Tick; got != before[id] {
			t.Fatalf("Drain advanced replica %d logical tick from %d to %d", id, before[id], got)
		}
	}
}
