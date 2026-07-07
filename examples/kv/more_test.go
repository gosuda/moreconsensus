package kv

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

func TestKVErrorAndPrefixBranches(t *testing.T) {
	if _, err := Open("/dev/null/not-a-directory"); err == nil {
		t.Fatal("expected open error")
	}
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: epaxos.Command{Kind: epaxos.CommandNoop}}); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: epaxos.Command{Payload: []byte{opPut, 9}}}); err == nil {
		t.Fatal("expected malformed command")
	}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: epaxos.Command{Payload: []byte{99, 0, 0}}}); err == nil {
		t.Fatal("expected unknown operation")
	}
	if _, ok, err := db.Get([]byte("missing")); err != nil || ok {
		t.Fatalf("missing get ok=%v err=%v", ok, err)
	}
	if err := db.PutVersion([]byte("pre-a"), []byte("1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("pre-b"), []byte("2"), 2); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("other"), []byte("3"), 3); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("pre-")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 2 {
		t.Fatalf("prefix scan len=%d", len(scan))
	}
	all, err := db.Scan(ScanOptions{})
	if err != nil || len(all) < 3 {
		t.Fatalf("full scan len=%d err=%v", len(all), err)
	}
	if _, _, ok := DecodeDataKey([]byte("bad"), 1); ok {
		t.Fatal("short key decoded")
	}
	badSep := append(EncodeUserPrefix(nil, 1, []byte("no-separator")), make([]byte, 8)...)
	if _, _, ok := DecodeDataKey(badSep, 1); ok {
		t.Fatal("bad separator decoded")
	}
	if limit := prefixLimit([]byte{0xff, 0xff}); limit != nil {
		t.Fatalf("prefix limit=%x", limit)
	}
	p := parser{err: true}
	if p.bytes() != nil {
		t.Fatal("errored parser returned bytes")
	}
	p = parser{b: []byte{0xff}}
	if p.bytes() != nil || !p.err {
		t.Fatal("bad varint did not fail")
	}
	if !IsNotFound(pebble.ErrNotFound) || IsNotFound(errors.New("x")) {
		t.Fatal("not-found helper failed")
	}
}

func TestClusterErrorBranches(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	cluster, err := OpenCluster(paths)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cluster.Get(99, []byte("x")); err == nil {
		t.Fatal("expected unknown replica")
	}
	cluster.Stores[1].FailWrites = true
	if err := cluster.Put([]byte("x"), []byte("y")); err == nil {
		t.Fatal("expected storage failure")
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
	cluster, err = OpenCluster([]string{t.TempDir(), t.TempDir(), t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	if err := cluster.Nodes[1].Step(epaxos.Message{Type: epaxos.MsgCommit, From: 2, To: 1, Ref: epaxos.InstanceRef{Replica: 2, Instance: 90, Conf: 1}, Deps: []epaxos.InstanceNum{0, 0, 0}, Command: epaxos.Command{Payload: []byte{opPut, 9}}}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drain(); err == nil {
		t.Fatal("expected malformed apply failure")
	}
}

func TestOpenClusterPartialFailureClosesOpenedDBs(t *testing.T) {
	first := t.TempDir()
	_, err := OpenCluster([]string{first, "/dev/null/not-a-directory"})
	if err == nil {
		t.Fatal("expected partial open failure")
	}
	db, err := Open(first)
	if err != nil {
		t.Fatalf("first database remained locked after partial failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClusterDeletePropagatesDurableStorageFailure(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	cluster.Stores[1].FailWrites = true
	if err := cluster.Delete([]byte("x")); err == nil {
		t.Fatal("expected storage failure")
	}
}

func TestIteratorFactoryErrorsPropagate(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	sentinel := errors.New("iterator")
	db.newIter = func(*pebble.IterOptions) (*pebble.Iterator, error) {
		return nil, sentinel
	}
	if _, _, err := db.Get([]byte("x")); !errors.Is(err, sentinel) {
		t.Fatalf("get err=%v", err)
	}
	if _, err := db.Scan(ScanOptions{}); !errors.Is(err, sentinel) {
		t.Fatalf("scan err=%v", err)
	}
}

func TestOpenClusterRawNodeFailureClosesOpenedDBs(t *testing.T) {
	first := t.TempDir()
	sentinel := errors.New("raw node")
	_, err := openCluster([]string{first}, func(epaxos.Config) (*epaxos.RawNode, error) {
		return nil, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	db, err := Open(first)
	if err != nil {
		t.Fatalf("first database remained locked after raw node failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClusterProposalAndTransportErrorBranches(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	if err := cluster.proposeAndDrain(epaxos.Command{Kind: epaxos.CommandConfChange}); !errors.Is(err, epaxos.ErrInvalidConfig) {
		t.Fatalf("proposal err=%v", err)
	}
	if err := cluster.deliver(epaxos.Message{To: 99}); err != nil {
		t.Fatalf("unknown target err=%v", err)
	}
	if err := cluster.deliver(epaxos.Message{Type: epaxos.MsgPreAccept, From: 99, To: 1, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}}); err != nil {
		t.Fatalf("rejected sender err=%v", err)
	}
	if err := cluster.deliver(epaxos.Message{From: 1, To: 1, Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}}); !errors.Is(err, epaxos.ErrInvalidMessage) {
		t.Fatalf("invalid transport err=%v", err)
	}
	if err := cluster.drainWithLimit(0); err == nil {
		t.Fatal("expected non-quiescence error")
	}
}

func TestDrainReturnsDeliveryFailure(t *testing.T) {
	cluster, err := OpenCluster([]string{t.TempDir(), t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	sentinel := errors.New("delivery")
	cluster.deliverMessage = func(epaxos.Message) error {
		return sentinel
	}
	if _, err := cluster.Nodes[1].Propose(CommandForPut(1, 1, []byte("x"), []byte("y"))); err != nil {
		t.Fatal(err)
	}
	if err := cluster.drainWithLimit(1); !errors.Is(err, sentinel) {
		t.Fatalf("drain err=%v", err)
	}
}
