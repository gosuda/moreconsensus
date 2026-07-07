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
	badSep := append(EncodeUserPrefix(nil, 1, nil), make([]byte, 8)...)
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

func TestOpenClusterPartialFailureCloses(t *testing.T) {
	_, err := OpenCluster([]string{t.TempDir(), "/dev/null/not-a-directory"})
	if err == nil {
		t.Fatal("expected partial open failure")
	}
}
