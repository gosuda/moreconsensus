package kv

import (
	"bytes"
	"testing"

	"gosuda.org/moreconsensus/epaxos"
)

func TestRecordFormatOrdersNewestFirst(t *testing.T) {
	old := EncodeDataKey(nil, 1, []byte("k"), 10)
	newer := EncodeDataKey(nil, 1, []byte("k"), 11)
	if bytes.Compare(newer, old) >= 0 {
		t.Fatalf("newer key must sort first: %x >= %x", newer, old)
	}
	user, ts, ok := DecodeDataKey(newer, 1)
	if !ok || string(user) != "k" || ts != 11 {
		t.Fatalf("decode %q %d %v", user, ts, ok)
	}
}

func TestPutGetScanAndApplyCommitted(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	cmd := epaxos.CommittedCommand{Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command: CommandForPut(1, 1, []byte("alpha"), []byte("one"))}
	if err := db.ApplyCommitted(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = epaxos.CommittedCommand{Ref: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Command: CommandForPut(1, 2, []byte("beta"), []byte("two"))}
	if err := db.ApplyCommitted(cmd); err != nil {
		t.Fatal(err)
	}
	v, ok, err := db.Get([]byte("alpha"))
	if err != nil || !ok || string(v) != "one" {
		t.Fatalf("get value=%q ok=%v err=%v", v, ok, err)
	}
	scan, err := db.Scan(ScanOptions{Start: []byte("a"), End: []byte("z"), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 2 || string(scan[0].Key) != "alpha" || string(scan[1].Key) != "beta" {
		t.Fatalf("scan %#v", scan)
	}
	rev, err := db.Scan(ScanOptions{Start: []byte("a"), End: []byte("z"), Reverse: true, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rev) != 1 || string(rev[0].Key) != "beta" {
		t.Fatalf("reverse scan %#v", rev)
	}
	cmd = epaxos.CommittedCommand{Ref: epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 1}, Command: CommandForDelete(1, 3, []byte("alpha"))}
	if err := db.ApplyCommitted(cmd); err != nil {
		t.Fatal(err)
	}
	_, ok, err = db.Get([]byte("alpha"))
	if err != nil || ok {
		t.Fatalf("deleted ok=%v err=%v", ok, err)
	}
}
