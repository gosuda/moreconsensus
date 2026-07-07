package kv

import (
	"encoding/binary"
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

func TestOpenLoadIteratorFactoryErrorClosesPebbleDB(t *testing.T) {
	path := t.TempDir()
	sentinel := errors.New("load iterator")

	_, err := open(path, func(*pebble.DB) func(*pebble.IterOptions) (kvIterator, error) {
		return func(*pebble.IterOptions) (kvIterator, error) {
			return nil, sentinel
		}
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("database remained locked after load iterator failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenLoadIteratorTerminalErrorClosesPebbleDB(t *testing.T) {
	path := t.TempDir()
	sentinel := errors.New("load scan")
	iter := &terminalErrorIterator{err: sentinel}

	_, err := open(path, func(*pebble.DB) func(*pebble.IterOptions) (kvIterator, error) {
		return func(*pebble.IterOptions) (kvIterator, error) {
			return iter, nil
		}
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if !iter.closed {
		t.Fatal("load iterator was not closed")
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("database remained locked after load scan failure: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNextRecordTimeStartsZeroValuedDBAtOne(t *testing.T) {
	db := &DB{}
	if got := db.nextRecordTime(); got != 1 {
		t.Fatalf("first timestamp=%d, want 1", got)
	}
	if got := db.nextRecordTime(); got != 1 {
		t.Fatalf("pending timestamp advanced to %d before a successful write", got)
	}
}

func TestVersionWriteErrorsDoNotAdvanceNextAutomaticTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		write func(*DB) error
	}{
		{
			name: "put",
			write: func(db *DB) error {
				return db.PutVersion([]byte("closed-put"), []byte("value"), 99)
			},
		},
		{
			name: "delete",
			write: func(db *DB) error {
				return db.DeleteVersion([]byte("closed-delete"), 99)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			pebbleDB, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pebbleDB.Close() }()
			db = &DB{pebble: pebbleDB, cf: 1, nextTime: 7}

			if err := tt.write(db); !errors.Is(err, pebble.ErrReadOnly) {
				t.Fatalf("err=%v, want read-only write failure", err)
			}
			if got := db.nextRecordTime(); got != 7 {
				t.Fatalf("next automatic timestamp advanced to %d after failed explicit write", got)
			}
		})
	}
}

func TestIteratorFactoryErrorsPropagate(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	sentinel := errors.New("iterator")
	db.newIter = func(*pebble.IterOptions) (kvIterator, error) {
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
	wantTime := uint64(1)
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

func TestTxnBatchCommitErrorIsReturned(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	commitErr := errors.New("batch commit failed")
	db.newBatch = func() txnBatch {
		return txnBatchWithCommitErr{err: commitErr}
	}

	err = db.ApplyCommitted(epaxos.CommittedCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 4, Conf: 1},
		Command: CommandForTxn(7, 4, []TxnOp{{Key: []byte("alpha"), Value: []byte("one")}}),
	})
	if !errors.Is(err, commitErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestApplyReadyPersistsExecutedRecordAndKVAcrossReopen(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	node, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: db.EPaxosStorage()})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := node.Propose(CommandForPut(9, 1, []byte("ready-key"), []byte("ready-value")))
	if err != nil {
		t.Fatal(err)
	}
	rd := node.Ready()
	if len(rd.Committed) != 1 || rd.Committed[0].Ref != ref {
		t.Fatalf("ready committed = %#v, want one command for %s", rd.Committed, ref)
	}
	if err := db.ApplyReady(rd); err != nil {
		t.Fatal(err)
	}
	node.Advance(rd)
	executedReady := node.Ready()
	if len(executedReady.Committed) != 0 {
		t.Fatalf("post-advance ready committed = %#v, want no replayed command", executedReady.Committed)
	}
	if len(executedReady.Records) != 1 || executedReady.Records[0].Ref != ref || executedReady.Records[0].Status != epaxos.StatusExecuted {
		t.Fatalf("post-advance ready records = %#v, want one executed record for %s", executedReady.Records, ref)
	}
	if err := db.ApplyReady(executedReady); err != nil {
		t.Fatal(err)
	}
	node.Advance(executedReady)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	value, ok, err := db.Get([]byte("ready-key"))
	if err != nil || !ok || string(value) != "ready-value" {
		t.Fatalf("reopened value=%q ok=%v err=%v", value, ok, err)
	}
	restarted, err := epaxos.NewRawNode(epaxos.Config{ID: 1, Voters: []epaxos.ReplicaID{1}, Storage: db.EPaxosStorage()})
	if err != nil {
		t.Fatal(err)
	}
	if !hasExecutedRef(restarted.Status().Executed, ref) {
		t.Fatalf("restarted executed refs = %#v, want %s", restarted.Status().Executed, ref)
	}
	if replay := restarted.Ready(); !replay.Empty() {
		t.Fatalf("restart produced Ready for an already executed command: %#v", replay)
	}
}

func TestPebbleStorageLoadInstancesDecodesDurableRecordOrder(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records := []epaxos.InstanceRecord{
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 3, Instance: 2, Conf: 2},
			Ballot:  epaxos.Ballot{Epoch: 4, Number: 5, Replica: 3},
			Status:  epaxos.StatusAccepted,
			Seq:     8,
			Deps:    []epaxos.InstanceNum{1, 0, 2},
			Command: CommandForPut(20, 1, []byte("late"), []byte("value-late")),
		}),
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 2, Instance: 9, Conf: 1},
			Ballot:  epaxos.Ballot{Epoch: 1, Number: 7, Replica: 2},
			Status:  epaxos.StatusCommitted,
			Seq:     6,
			Deps:    []epaxos.InstanceNum{0, 9, 1},
			Command: CommandForDelete(20, 2, []byte("middle")),
		}),
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 4, Conf: 1},
			Ballot:  epaxos.Ballot{Epoch: 2, Number: 1, Replica: 1},
			Status:  epaxos.StatusPreAccepted,
			Seq:     5,
			Deps:    []epaxos.InstanceNum{4, 0, 0},
			Command: CommandForTxn(20, 3, []TxnOp{{Key: []byte("first"), Value: []byte("value-first")}, {Delete: true, Key: []byte("gone")}}),
		}),
		checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 2},
			Ballot:  epaxos.Ballot{Epoch: 3, Number: 2, Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     7,
			Deps:    []epaxos.InstanceNum{0, 1, 3},
			Command: CommandForPut(20, 4, []byte("third"), []byte("value-third")),
		}),
	}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: records}); err != nil {
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
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantRefs := []epaxos.InstanceRef{
		{Replica: 1, Instance: 4, Conf: 1},
		{Replica: 2, Instance: 9, Conf: 1},
		{Replica: 1, Instance: 3, Conf: 2},
		{Replica: 3, Instance: 2, Conf: 2},
	}
	if len(loaded) != len(wantRefs) {
		t.Fatalf("loaded %d records, want %d: %#v", len(loaded), len(wantRefs), loaded)
	}
	for i, want := range wantRefs {
		if loaded[i].Ref != want {
			t.Fatalf("loaded[%d] ref = %s, want %s; all records %#v", i, loaded[i].Ref, want, loaded)
		}
		if !epaxos.VerifyRecordChecksum(loaded[i]) {
			t.Fatalf("loaded[%d] checksum was not preserved: %#v", i, loaded[i])
		}
	}
	if loaded[0].Status != epaxos.StatusPreAccepted || loaded[0].Seq != 5 || len(loaded[0].Deps) != 3 || loaded[0].Deps[0] != 4 {
		t.Fatalf("first loaded record lost attributes: %#v", loaded[0])
	}
	if string(loaded[0].Command.ConflictKeys[0]) != "first" || string(loaded[0].Command.ConflictKeys[1]) != "gone" {
		t.Fatalf("transaction conflict keys not decoded: %#v", loaded[0].Command.ConflictKeys)
	}
	if loaded[3].Ballot.Number != 5 || string(loaded[3].Command.Payload) != string(CommandForPut(20, 1, []byte("late"), []byte("value-late")).Payload) {
		t.Fatalf("last loaded record lost ballot or payload: %#v", loaded[3])
	}
}

func TestApplyReadyDoesNotPersistExecutedRecordWhenKVStagingFails(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
	cmd := CommandForPut(31, 1, []byte("atomic-key"), []byte("atomic-value"))
	rec := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     ref,
		Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
		Status:  epaxos.StatusExecuted,
		Seq:     1,
		Deps:    []epaxos.InstanceNum{0},
		Command: cmd,
	})
	setErr := errors.New("kv stage failed")
	db.newBatch = func() txnBatch {
		return &failOnSetBatch{inner: db.pebble.NewBatch(), failAt: 2, err: setErr}
	}
	err = db.ApplyReady(epaxos.Ready{
		Records:   []epaxos.InstanceRecord{rec},
		Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: rec.Seq, Deps: rec.Deps, Command: cmd}},
	})
	if !errors.Is(err, setErr) {
		t.Fatalf("err=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("failed ApplyReady persisted consensus records: %#v", loaded)
	}
	if value, ok, err := db.Get([]byte("atomic-key")); err != nil || ok {
		t.Fatalf("failed ApplyReady value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestPebbleStorageLoadInstancesReturnsIteratorDecodeAndCallbackErrors(t *testing.T) {
	t.Run("iterator creation", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		sentinel := errors.New("storage iterator")
		oldNewIter := newPebbleStorageIter
		var sawRecordBounds bool
		newPebbleStorageIter = func(_ *pebble.DB, opts *pebble.IterOptions) (kvIterator, error) {
			sawRecordBounds = len(opts.LowerBound) == 2 && opts.LowerBound[0] == epaxosStorePrefix && opts.LowerBound[1] == epaxosRecordEntry
			return nil, sentinel
		}
		t.Cleanup(func() { newPebbleStorageIter = oldNewIter })
		err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error {
			t.Fatal("callback must not run when iterator creation fails")
			return nil
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err=%v", err)
		}
		if !sawRecordBounds {
			t.Fatal("iterator was not scoped to durable record keys")
		}
	})

	t.Run("decode failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		ref := epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}
		if err := db.pebble.Set(epaxosRecordKey(ref), []byte{epaxosRecordCodec + 1}, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		err = db.EPaxosStorage().LoadInstances(func(epaxos.InstanceRecord) error {
			t.Fatal("callback must not run for a malformed durable record")
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "bad epaxos record version") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("callback failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		rec := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     epaxos.InstanceRef{Replica: 2, Instance: 3, Conf: 1},
			Ballot:  epaxos.Ballot{Number: 1, Replica: 2},
			Status:  epaxos.StatusCommitted,
			Seq:     4,
			Deps:    []epaxos.InstanceNum{0, 3},
			Command: CommandForPut(41, 1, []byte("callback-key"), []byte("callback-value")),
		})
		if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{rec}}); err != nil {
			t.Fatal(err)
		}
		sentinel := errors.New("load callback")
		var seen []epaxos.InstanceRef
		err = db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
			seen = append(seen, rec.Ref)
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err=%v", err)
		}
		if len(seen) != 1 || seen[0] != rec.Ref {
			t.Fatalf("callback refs=%#v, want only %s", seen, rec.Ref)
		}
	})
}

func TestPebbleStorageApplyReadyRejectsBadRecordsAndLeavesStorageUnchanged(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{}); err != nil {
		t.Fatalf("empty ready err=%v", err)
	}
	bad := epaxos.InstanceRecord{Ref: epaxos.InstanceRef{Replica: 1, Instance: 9, Conf: 1}, Checksum: [32]byte{1}}
	if err := db.EPaxosStorage().ApplyReady(epaxos.Ready{Records: []epaxos.InstanceRecord{bad}}); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("err=%v", err)
	}
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("invalid record advanced durable storage: %#v", loaded)
	}
}

func TestDBApplyReadyErrorPathsDoNotAdvanceDurableOrKVState(t *testing.T) {
	t.Run("empty ready bypasses batch", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 12
		db.newBatch = func() txnBatch {
			t.Fatal("empty ready must not allocate a write batch")
			return txnBatchWithSetErr{err: errors.New("unreachable")}
		}
		if err := db.ApplyReady(epaxos.Ready{}); err != nil {
			t.Fatalf("empty ready err=%v", err)
		}
		if got := db.nextRecordTime(); got != 12 {
			t.Fatalf("next automatic timestamp advanced to %d for empty ready", got)
		}
	})

	t.Run("invalid consensus record", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 5
		bad := epaxos.InstanceRecord{Ref: epaxos.InstanceRef{Replica: 1, Instance: 10, Conf: 1}, Checksum: [32]byte{1}}
		cmd := CommandForPut(42, 1, []byte("bad-ready"), []byte("value"))
		err = db.ApplyReady(epaxos.Ready{
			Records:   []epaxos.InstanceRecord{bad},
			Committed: []epaxos.CommittedCommand{{Ref: bad.Ref, Command: cmd}},
		})
		if !errors.Is(err, epaxos.ErrChecksumMismatch) {
			t.Fatalf("err=%v", err)
		}
		assertNoEPaxosRecords(t, db)
		if value, ok, err := db.Get([]byte("bad-ready")); err != nil || ok {
			t.Fatalf("invalid record applied value=%q ok=%v err=%v", value, ok, err)
		}
		if got := db.nextRecordTime(); got != 5 {
			t.Fatalf("next automatic timestamp advanced to %d after invalid record", got)
		}
	})

	t.Run("batch commit failure", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 7
		commitErr := errors.New("ready commit failed")
		db.newBatch = func() txnBatch {
			return &commitErrBatch{inner: db.pebble.NewBatch(), err: commitErr}
		}
		ref := epaxos.InstanceRef{Replica: 1, Instance: 11, Conf: 1}
		cmd := CommandForPut(42, 2, []byte("commit-ready"), []byte("value"))
		rec := checkedKVRecord(epaxos.InstanceRecord{
			Ref:     ref,
			Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
			Status:  epaxos.StatusExecuted,
			Seq:     1,
			Deps:    []epaxos.InstanceNum{0},
			Command: cmd,
		})
		err = db.ApplyReady(epaxos.Ready{
			Records:   []epaxos.InstanceRecord{rec},
			Committed: []epaxos.CommittedCommand{{Ref: ref, Seq: rec.Seq, Deps: rec.Deps, Command: cmd}},
		})
		if !errors.Is(err, commitErr) {
			t.Fatalf("err=%v", err)
		}
		assertNoEPaxosRecords(t, db)
		if value, ok, err := db.Get([]byte("commit-ready")); err != nil || ok {
			t.Fatalf("failed commit value=%q ok=%v err=%v", value, ok, err)
		}
		if got := db.nextRecordTime(); got != 7 {
			t.Fatalf("next automatic timestamp advanced to %d after failed ready commit", got)
		}
	})
}

func TestStageEPaxosRecordsRejectsInvalidChecksumBeforeWriting(t *testing.T) {
	setErr := errors.New("record set failed")
	bad := epaxos.InstanceRecord{Ref: epaxos.InstanceRef{Replica: 1, Instance: 12, Conf: 1}, Checksum: [32]byte{1}}
	if err := stageEPaxosRecords(txnBatchWithSetErr{err: setErr}, []epaxos.InstanceRecord{bad}); !errors.Is(err, epaxos.ErrChecksumMismatch) {
		t.Fatalf("err=%v", err)
	}
	good := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 13, Conf: 1},
		Ballot:  epaxos.Ballot{Number: 1, Replica: 1},
		Status:  epaxos.StatusAccepted,
		Seq:     2,
		Deps:    []epaxos.InstanceNum{0},
		Command: CommandForDelete(43, 1, []byte("stage")),
	})
	if err := stageEPaxosRecords(txnBatchWithSetErr{err: setErr}, []epaxos.InstanceRecord{good}); !errors.Is(err, setErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestDecodeEPaxosRecordRejectsMalformedRecords(t *testing.T) {
	valid := checkedKVRecord(epaxos.InstanceRecord{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 14, Conf: 1},
		Ballot:  epaxos.Ballot{Epoch: 1, Number: 2, Replica: 1},
		Status:  epaxos.StatusCommitted,
		Seq:     3,
		Deps:    []epaxos.InstanceNum{0},
		Command: CommandForPut(44, 1, []byte("decode"), []byte("value")),
	})
	corruptChecksum := encodeEPaxosRecord(valid)
	corruptChecksum[len(corruptChecksum)-1] ^= 0x80
	tests := []struct {
		name    string
		record  []byte
		wantErr string
		wantIs  error
	}{
		{name: "bad version", record: []byte{epaxosRecordCodec + 1}, wantErr: "kv: bad epaxos record version"},
		{name: "excessive dependency count", record: malformedEPaxosRecord(129, 0, true), wantErr: "kv: bad epaxos dependency count"},
		{name: "excessive conflict key count", record: malformedEPaxosRecord(0, 129, true), wantErr: "kv: bad epaxos conflict-key count"},
		{name: "short checksum", record: malformedEPaxosRecord(0, 0, false), wantErr: "kv: malformed epaxos record"},
		{name: "extra bytes", record: append(encodeEPaxosRecord(valid), 1), wantErr: "kv: malformed epaxos record"},
		{name: "checksum mismatch", record: corruptChecksum, wantIs: epaxos.ErrChecksumMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeEPaxosRecord(tt.record)
			switch {
			case tt.wantIs != nil:
				if !errors.Is(err, tt.wantIs) {
					t.Fatalf("err=%v", err)
				}
			case err == nil || err.Error() != tt.wantErr:
				t.Fatalf("err=%v want %q", err, tt.wantErr)
			}
		})
	}
}

func TestDirectVersionAndDeleteErrorBranchesPreserveAutomaticTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		write func(*DB) error
	}{
		{
			name: "put set",
			write: func(db *DB) error {
				return db.PutVersion([]byte("direct-put"), []byte("value"), 20)
			},
		},
		{
			name: "delete set",
			write: func(db *DB) error {
				return db.DeleteVersion([]byte("direct-delete"), 20)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			db.nextTime = 9
			setErr := errors.New("direct set failed")
			db.newBatch = func() txnBatch {
				return txnBatchWithSetErr{err: setErr}
			}
			if err := tt.write(db); !errors.Is(err, setErr) {
				t.Fatalf("err=%v", err)
			}
			if got := db.nextRecordTime(); got != 9 {
				t.Fatalf("next automatic timestamp advanced to %d after failed %s", got, tt.name)
			}
		})
	}
}

func TestApplyCommittedEmptyTxnAndDeleteSetErrorDoNotAdvanceTimestamp(t *testing.T) {
	t.Run("empty transaction", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 6
		commitErr := errors.New("empty transaction commit")
		db.newBatch = func() txnBatch {
			return txnBatchWithCommitErr{err: commitErr}
		}
		if err := db.ApplyCommitted(epaxos.CommittedCommand{Command: CommandForTxn(45, 1, nil)}); err != nil {
			t.Fatalf("empty transaction err=%v", err)
		}
		if got := db.nextRecordTime(); got != 6 {
			t.Fatalf("next automatic timestamp advanced to %d for empty transaction", got)
		}
	})

	t.Run("delete set error", func(t *testing.T) {
		db, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.nextTime = 8
		setErr := errors.New("delete set failed")
		db.newBatch = func() txnBatch {
			return txnBatchWithSetErr{err: setErr}
		}
		err = db.ApplyCommitted(epaxos.CommittedCommand{
			Ref:     epaxos.InstanceRef{Replica: 1, Instance: 15, Conf: 1},
			Command: CommandForDelete(45, 2, []byte("delete-set")),
		})
		if !errors.Is(err, setErr) {
			t.Fatalf("err=%v", err)
		}
		if value, ok, err := db.Get([]byte("delete-set")); err != nil || ok {
			t.Fatalf("failed delete write value=%q ok=%v err=%v", value, ok, err)
		}
		if got := db.nextRecordTime(); got != 8 {
			t.Fatalf("next automatic timestamp advanced to %d after delete set failure", got)
		}
	})
}

func assertNoEPaxosRecords(t *testing.T, db *DB) {
	t.Helper()
	var loaded []epaxos.InstanceRecord
	if err := db.EPaxosStorage().LoadInstances(func(rec epaxos.InstanceRecord) error {
		loaded = append(loaded, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("durable records advanced: %#v", loaded)
	}
}

func malformedEPaxosRecord(deps, keys uint64, withChecksum bool) []byte {
	out := []byte{epaxosRecordCodec}
	for range 8 {
		out = binary.AppendUvarint(out, 1)
	}
	out = binary.AppendUvarint(out, deps)
	for i := uint64(0); i < deps && i < 128; i++ {
		out = binary.AppendUvarint(out, 1)
	}
	for range 3 {
		out = binary.AppendUvarint(out, 1)
	}
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, keys)
	for i := uint64(0); i < keys && i < 128; i++ {
		out = binary.AppendUvarint(out, 0)
	}
	if withChecksum {
		out = append(out, make([]byte, 32)...)
	}
	return out
}

type commitErrBatch struct {
	inner txnBatch
	err   error
}

func (b *commitErrBatch) Set(key, value []byte, opts *pebble.WriteOptions) error {
	return b.inner.Set(key, value, opts)
}

func (b *commitErrBatch) Commit(*pebble.WriteOptions) error {
	return b.err
}

func (b *commitErrBatch) Close() error {
	return b.inner.Close()
}

type terminalErrorIterator struct {
	err    error
	closed bool
}

func (i *terminalErrorIterator) First() bool {
	return false
}

func (i *terminalErrorIterator) Next() bool {
	return false
}

func (i *terminalErrorIterator) Key() []byte {
	return nil
}

func (i *terminalErrorIterator) Value() []byte {
	return nil
}

func (i *terminalErrorIterator) Error() error {
	return i.err
}

func (i *terminalErrorIterator) Close() error {
	i.closed = true
	return nil
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

type txnBatchWithCommitErr struct {
	err error
}

func (txnBatchWithCommitErr) Set(_, _ []byte, _ *pebble.WriteOptions) error {
	return nil
}

func (b txnBatchWithCommitErr) Commit(*pebble.WriteOptions) error {
	return b.err
}

func (txnBatchWithCommitErr) Close() error {
	return nil
}

type failOnSetBatch struct {
	inner  txnBatch
	failAt int
	seen   int
	err    error
}

func (b *failOnSetBatch) Set(key, value []byte, opts *pebble.WriteOptions) error {
	b.seen++
	if b.seen == b.failAt {
		return b.err
	}
	return b.inner.Set(key, value, opts)
}

func (b *failOnSetBatch) Commit(opts *pebble.WriteOptions) error {
	return b.inner.Commit(opts)
}

func (b *failOnSetBatch) Close() error {
	return b.inner.Close()
}

func checkedKVRecord(rec epaxos.InstanceRecord) epaxos.InstanceRecord {
	rec.Checksum = epaxos.ChecksumRecord(rec)
	return rec
}

func hasExecutedRef(refs []epaxos.InstanceRef, want epaxos.InstanceRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
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

func TestScanOmitsOlderValueHiddenByNewerDelete(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("scan-alive"), []byte("kept"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("scan-gone"), []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteVersion([]byte("scan-gone"), 2); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("scan-")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Key) != "scan-alive" || string(scan[0].Value) != "kept" || scan[0].Time != 1 {
		t.Fatalf("scan %#v", scan)
	}
}

func TestApplyCommittedChangesPersistAcrossReopen(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	commands := []epaxos.CommittedCommand{
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command: CommandForPut(7, 1, []byte("applied-put"), []byte("one"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Command: CommandForPut(7, 2, []byte("applied-deleted"), []byte("before"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 1}, Command: CommandForDelete(7, 3, []byte("applied-deleted"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 4, Conf: 1}, Command: CommandForPut(7, 4, []byte("txn-deleted"), []byte("before"))},
		{Ref: epaxos.InstanceRef{Replica: 1, Instance: 5, Conf: 1}, Command: CommandForTxn(7, 5, []TxnOp{
			{Key: []byte("txn-put"), Value: []byte("two")},
			{Delete: true, Key: []byte("txn-deleted")},
		})},
	}
	for _, cmd := range commands {
		if err := db.ApplyCommitted(cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	value, ok, err := db.Get([]byte("applied-put"))
	if err != nil || !ok || string(value) != "one" {
		t.Fatalf("applied-put value=%q ok=%v err=%v", value, ok, err)
	}
	if _, ok, err := db.Get([]byte("applied-deleted")); err != nil || ok {
		t.Fatalf("applied-deleted ok=%v err=%v", ok, err)
	}
	value, ok, err = db.Get([]byte("txn-put"))
	if err != nil || !ok || string(value) != "two" {
		t.Fatalf("txn-put value=%q ok=%v err=%v", value, ok, err)
	}
	if _, ok, err := db.Get([]byte("txn-deleted")); err != nil || ok {
		t.Fatalf("txn-deleted ok=%v err=%v", ok, err)
	}
}

func TestApplyCommittedSameRefSameKeyKeepsLaterValue(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ref := epaxos.InstanceRef{Replica: 1, Instance: 44, Conf: 1}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Ref: ref, Command: CommandForPut(7, 1, []byte("same-ref"), []byte("first"))}); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyCommitted(epaxos.CommittedCommand{Ref: ref, Command: CommandForPut(7, 2, []byte("same-ref"), []byte("second"))}); err != nil {
		t.Fatal(err)
	}
	value, ok, err := db.Get([]byte("same-ref"))
	if err != nil || !ok || string(value) != "second" {
		t.Fatalf("value=%q ok=%v err=%v", value, ok, err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("same-ref")})
	if err != nil {
		t.Fatal(err)
	}
	wantTime := uint64(2)
	if len(scan) != 1 || string(scan[0].Value) != "second" || scan[0].Time != wantTime {
		t.Fatalf("scan %#v", scan)
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
