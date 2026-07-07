package kv

import (
	"errors"
	"strings"
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

func TestParserUvarintAlreadyErroredReturnsZero(t *testing.T) {
	p := parser{b: []byte{1, 'x'}, err: true}
	if got := p.uvarint(); got != 0 {
		t.Fatalf("uvarint=%d", got)
	}
	if !p.err {
		t.Fatal("parser error state cleared")
	}
	if len(p.b) != 2 || p.b[0] != 1 || p.b[1] != 'x' {
		t.Fatalf("parser consumed input %x", p.b)
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

func TestCommandForTxnAppliesPutsThenDeleteAndPut(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	first := epaxos.CommittedCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 10, Conf: 1},
		Command: CommandForTxn(7, 1, []TxnOp{{Key: []byte("alpha"), Value: []byte("one")}, {Key: []byte("beta"), Value: []byte("two")}}),
	}
	if err := db.ApplyCommitted(first); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Start: []byte("a"), End: []byte("z")})
	if err != nil {
		t.Fatal(err)
	}
	wantTime := (uint64(1) << 56) | 10
	if len(scan) != 2 ||
		string(scan[0].Key) != "alpha" || string(scan[0].Value) != "one" || scan[0].Time != wantTime ||
		string(scan[1].Key) != "beta" || string(scan[1].Value) != "two" || scan[1].Time != wantTime {
		t.Fatalf("first transaction scan %#v", scan)
	}

	second := epaxos.CommittedCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 11, Conf: 1},
		Command: CommandForTxn(7, 2, []TxnOp{{Delete: true, Key: []byte("alpha")}, {Key: []byte("gamma"), Value: []byte("three")}}),
	}
	if err := db.ApplyCommitted(second); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.Get([]byte("alpha")); err != nil || ok {
		t.Fatalf("deleted key ok=%v err=%v", ok, err)
	}
	v, ok, err := db.Get([]byte("beta"))
	if err != nil || !ok || string(v) != "two" {
		t.Fatalf("beta value=%q ok=%v err=%v", v, ok, err)
	}
	v, ok, err = db.Get([]byte("gamma"))
	if err != nil || !ok || string(v) != "three" {
		t.Fatalf("gamma value=%q ok=%v err=%v", v, ok, err)
	}
}

func TestTxnPayloadRejectsMalformedInputsAndKeepsBatchAtomic(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{name: "bad count varint", payload: []byte{opTxn, 0xff}, wantErr: "kv: malformed command"},
		{name: "missing transaction operation", payload: []byte{opTxn, 1}, wantErr: "kv: malformed command"},
		{name: "unknown transaction operation", payload: []byte{opTxn, 1, 99, 0, 0}, wantErr: "kv: unknown transaction op 99"},
		{name: "extra byte after operations", payload: []byte{opTxn, 0, 0}, wantErr: "kv: malformed command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			err = db.ApplyCommitted(epaxos.CommittedCommand{
				Ref:     epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
				Command: epaxos.Command{Payload: tt.payload},
			})
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("err=%v want %q", err, tt.wantErr)
			}
		})
	}

	t.Run("unknown operation leaves earlier write unapplied", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		payload := []byte{opTxn, 2, opPut}
		payload = appendKVFields(payload, []byte("alpha"), []byte("one"))
		payload = append(payload, 99)
		payload = appendKVFields(payload, []byte("beta"), nil)
		err = db.ApplyCommitted(epaxos.CommittedCommand{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1},
			Command: epaxos.Command{Payload: payload},
		})
		if err == nil || !strings.Contains(err.Error(), "unknown transaction op") {
			t.Fatalf("err=%v", err)
		}
		if _, ok, err := db.Get([]byte("alpha")); err != nil || ok {
			t.Fatalf("partial write ok=%v err=%v", ok, err)
		}
	})
}

func TestTxnBatchSetReturnsIndexErrorForPutAndDelete(t *testing.T) {
	tests := []struct {
		name string
		op   TxnOp
	}{
		{name: "put", op: TxnOp{Key: []byte("alpha"), Value: []byte("one")}},
		{name: "delete", op: TxnOp{Delete: true, Key: []byte("alpha")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()

			setErr := errors.New("batch set failed")
			db.newBatch = func() txnBatch {
				return txnBatchWithSetErr{err: setErr}
			}

			err = db.ApplyCommitted(epaxos.CommittedCommand{
				Ref:     epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 1},
				Command: CommandForTxn(7, 3, []TxnOp{tt.op}),
			})
			if !errors.Is(err, setErr) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

type txnBatchWithSetErr struct {
	err error
}

func (b txnBatchWithSetErr) Set(_, _ []byte, _ *pebble.WriteOptions) error {
	return b.err
}

func (txnBatchWithSetErr) Commit(*pebble.WriteOptions) error {
	return nil
}

func (txnBatchWithSetErr) Close() error {
	return nil
}

func TestPutVersionSameTimestampKeepsLaterValue(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("collision"), []byte("first"), 44); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("collision"), []byte("second"), 44); err != nil {
		t.Fatal(err)
	}
	v, ok, err := db.Get([]byte("collision"))
	if err != nil || !ok || string(v) != "second" {
		t.Fatalf("value=%q ok=%v err=%v", v, ok, err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("collision")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Value) != "second" {
		t.Fatalf("scan %#v", scan)
	}
}

func TestPebbleDBPersistsVersionWriteAcrossReopen(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("durable"), []byte("kept"), 88); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	v, ok, err := db.Get([]byte("durable"))
	if err != nil || !ok || string(v) != "kept" {
		t.Fatalf("value=%q ok=%v err=%v", v, ok, err)
	}
}

func TestReverseScanUsesNewestVersionForRepeatedKey(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("scan-b"), []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("scan-b"), []byte("new"), 2); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("scan-c"), []byte("later"), 3); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range scan {
		if string(kv.Key) == "scan-b" {
			if string(kv.Value) != "new" {
				t.Fatalf("reverse scan returned repeated key value %q in %#v", kv.Value, scan)
			}
			return
		}
	}
	t.Fatalf("reverse scan missed repeated key in %#v", scan)
}
