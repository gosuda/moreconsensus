package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
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

func TestEmbeddedSeparatorUserKeysAreRejected(t *testing.T) {
	embeddedKey := []byte("a\x00x")
	cases := []struct {
		name  string
		write func(*DB) error
	}{
		{
			name: "put version",
			write: func(db *DB) error {
				return db.PutVersion(embeddedKey, []byte("embedded"), 2)
			},
		},
		{
			name: "delete version",
			write: func(db *DB) error {
				return db.DeleteVersion(embeddedKey, 2)
			},
		},
		{
			name: "committed put",
			write: func(db *DB) error {
				return db.ApplyCommitted(epaxos.ApplyCommand{
					Ref:     epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1},
					Command: CommandForPut(1, 2, embeddedKey, []byte("embedded")),
				})
			},
		},
		{
			name: "committed delete",
			write: func(db *DB) error {
				return db.ApplyCommitted(epaxos.ApplyCommand{
					Ref:     epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1},
					Command: CommandForDelete(1, 2, embeddedKey),
				})
			},
		},
		{
			name: "transaction put",
			write: func(db *DB) error {
				return db.ApplyCommitted(epaxos.ApplyCommand{
					Ref: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1},
					Command: CommandForTxn(1, 2, []TxnOp{
						{Key: []byte("a"), Value: []byte("txn-overwrite")},
						{Key: embeddedKey, Value: []byte("embedded")},
					}),
				})
			},
		},
		{
			name: "transaction delete",
			write: func(db *DB) error {
				return db.ApplyCommitted(epaxos.ApplyCommand{
					Ref: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1},
					Command: CommandForTxn(1, 2, []TxnOp{
						{Key: []byte("a"), Value: []byte("txn-overwrite")},
						{Delete: true, Key: embeddedKey},
					}),
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()

			if err := db.PutVersion([]byte("a"), []byte("short"), 1); err != nil {
				t.Fatal(err)
			}
			if err := tc.write(db); err == nil {
				t.Fatalf("embedded-separator key %q was accepted", embeddedKey)
			}

			value, ok, err := db.Get([]byte("a"))
			if err != nil || !ok || string(value) != "short" {
				t.Fatalf("short key value=%q ok=%v err=%v", value, ok, err)
			}
			scan, err := db.Scan(ScanOptions{Start: []byte("a"), End: []byte("b")})
			if err != nil {
				t.Fatal(err)
			}
			if len(scan) != 1 || string(scan[0].Key) != "a" || string(scan[0].Value) != "short" || scan[0].Time != 1 {
				t.Fatalf("scan=%#v", scan)
			}
		})
	}
}

func TestStageVersionHelpersRejectInvalidKeysBeforeBatchMutation(t *testing.T) {
	invalidKeys := []struct {
		name string
		key  []byte
	}{
		{name: "empty", key: nil},
		{name: "embedded separator", key: []byte("a\x00x")},
	}
	helpers := []struct {
		name  string
		stage func(txnBatch, []byte) error
	}{
		{
			name: "put",
			stage: func(batch txnBatch, key []byte) error {
				return stagePutVersion(batch, 1, key, []byte("value"), 7)
			},
		},
		{
			name: "delete",
			stage: func(batch txnBatch, key []byte) error {
				return stageDeleteVersion(batch, 1, key, 7)
			},
		},
	}

	for _, invalid := range invalidKeys {
		for _, helper := range helpers {
			t.Run(invalid.name+" "+helper.name, func(t *testing.T) {
				batch := &recordingTxnBatch{}
				err := helper.stage(batch, invalid.key)
				if !errors.Is(err, errInvalidKey) {
					t.Fatalf("err=%v, want invalid key", err)
				}
				if batch.sets != 0 {
					t.Fatalf("invalid key reached batch Set %d time(s)", batch.sets)
				}
			})
		}
	}
}

func TestGetRejectsInvalidKeys(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{name: "empty", key: nil},
		{name: "embedded separator", key: []byte("a\x00x")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, ok, err := db.Get(tc.key)
			if !errors.Is(err, errInvalidKey) {
				t.Fatalf("err=%v, want invalid key", err)
			}
			if ok || value != nil {
				t.Fatalf("invalid get returned value=%q ok=%v", value, ok)
			}
		})
	}
}

func TestPutGetScanAndApplyCommitted(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	cmd := epaxos.ApplyCommand{Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command: CommandForPut(1, 1, []byte("alpha"), []byte("one"))}
	if err := db.ApplyCommitted(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = epaxos.ApplyCommand{Ref: epaxos.InstanceRef{Replica: 1, Instance: 2, Conf: 1}, Command: CommandForPut(1, 2, []byte("beta"), []byte("two"))}
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
	limited, err := db.Scan(ScanOptions{Start: []byte("a"), End: []byte("z"), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 || string(limited[0].Key) != "alpha" {
		t.Fatalf("limited scan %#v", limited)
	}
	rev, err := db.Scan(ScanOptions{Start: []byte("a"), End: []byte("z"), Reverse: true, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rev) != 1 || string(rev[0].Key) != "beta" {
		t.Fatalf("reverse scan %#v", rev)
	}
	cmd = epaxos.ApplyCommand{Ref: epaxos.InstanceRef{Replica: 1, Instance: 3, Conf: 1}, Command: CommandForDelete(1, 3, []byte("alpha"))}
	if err := db.ApplyCommitted(cmd); err != nil {
		t.Fatal(err)
	}
	_, ok, err = db.Get([]byte("alpha"))
	if err != nil || ok {
		t.Fatalf("deleted ok=%v err=%v", ok, err)
	}
}

func TestApplyCommittedUsesApplyOrderInsteadOfInstanceRefOrder(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	first := epaxos.ApplyCommand{
		Ref:     epaxos.InstanceRef{Replica: 2, Instance: 99, Conf: 1},
		Command: CommandForPut(2, 99, []byte("shared"), []byte("first-applied")),
	}
	second := epaxos.ApplyCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Command: CommandForPut(1, 1, []byte("shared"), []byte("second-applied")),
	}
	if err := db.ApplyCommitted(first); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyCommitted(second); err != nil {
		t.Fatal(err)
	}

	value, ok, err := db.Get([]byte("shared"))
	if err != nil || !ok || string(value) != "second-applied" {
		t.Fatalf("get value=%q ok=%v err=%v", value, ok, err)
	}
	scan, err := db.Scan(ScanOptions{Prefix: []byte("shared")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 ||
		string(scan[0].Key) != "shared" ||
		string(scan[0].Value) != "second-applied" ||
		scan[0].Time != 2 {
		t.Fatalf("scan=%#v", scan)
	}
}

func TestTransactionCommandAppliesPutAndDeleteAtomically(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("gone"), []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	cmd := epaxos.ApplyCommand{
		Ref: epaxos.InstanceRef{Replica: 1, Instance: 9, Conf: 1},
		Command: CommandForTxn(7, 11, []TxnOp{
			{Key: []byte("alpha"), Value: []byte("one")},
			{Delete: true, Key: []byte("gone")},
		}),
	}
	if err := db.ApplyCommitted(cmd); err != nil {
		t.Fatal(err)
	}
	value, ok, err := db.Get([]byte("alpha"))
	if err != nil || !ok || string(value) != "one" {
		t.Fatalf("alpha value=%q ok=%v err=%v", value, ok, err)
	}
	if _, ok, err := db.Get([]byte("gone")); err != nil || ok {
		t.Fatalf("gone ok=%v err=%v", ok, err)
	}
	alpha, err := db.Scan(ScanOptions{Prefix: []byte("alpha")})
	if err != nil {
		t.Fatal(err)
	}
	if len(alpha) != 1 || string(alpha[0].Value) != "one" || alpha[0].Time != 2 {
		t.Fatalf("alpha scan=%#v", alpha)
	}
}

func TestCommandForTxnDeduplicatesFootprintPointsAndKeepsPayloadOps(t *testing.T) {
	cmd := CommandForTxn(7, 12, []TxnOp{
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("beta"), Value: []byte("two")},
		{Delete: true, Key: []byte("alpha")},
		{Key: []byte("alpha"), Value: []byte("three")},
	})
	if len(cmd.Footprint.Points) != 2 ||
		string(cmd.Footprint.Points[0]) != "alpha" ||
		string(cmd.Footprint.Points[1]) != "beta" {
		t.Fatalf("conflict keys=%q", cmd.Footprint.Points)
	}
	wantPayload := []byte{
		opTxn, 4,
		opPut, 5, 'a', 'l', 'p', 'h', 'a', 3, 'o', 'n', 'e',
		opPut, 4, 'b', 'e', 't', 'a', 3, 't', 'w', 'o',
		opDelete, 5, 'a', 'l', 'p', 'h', 'a', 0,
		opPut, 5, 'a', 'l', 'p', 'h', 'a', 5, 't', 'h', 'r', 'e', 'e',
	}
	if !bytes.Equal(cmd.Payload, wantPayload) {
		t.Fatalf("payload=%v, want %v", cmd.Payload, wantPayload)
	}

	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.ApplyCommitted(epaxos.ApplyCommand{Command: cmd}); err != nil {
		t.Fatal(err)
	}
	value, ok, err := db.Get([]byte("alpha"))
	if err != nil || !ok || string(value) != "three" {
		t.Fatalf("alpha value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = db.Get([]byte("beta"))
	if err != nil || !ok || string(value) != "two" {
		t.Fatalf("beta value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestDeleteCommandRejectsMalformedPayload(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	err = db.ApplyCommitted(epaxos.ApplyCommand{
		Ref:     epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1},
		Command: epaxos.Command{Payload: []byte{opDelete, 1, 'k'}},
	})
	if err == nil || !strings.Contains(err.Error(), "malformed command") {
		t.Fatalf("err=%v, want malformed command", err)
	}
}

func TestTransactionRejectsMalformedPayloads(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	cases := []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "bad count", payload: []byte{opTxn, 0xff}, want: "malformed command"},
		{name: "missing op", payload: []byte{opTxn, 1}, want: "malformed command"},
		{name: "truncated fields", payload: appendTxnPayload(1, opPut, []byte{9}), want: "malformed command"},
		{name: "unknown op", payload: appendTxnPayload(1, 99, appendKVFields(nil, []byte("k"), []byte("v"))), want: "unknown transaction op 99"},
		{name: "trailing bytes", payload: []byte{opTxn, 0, 1}, want: "malformed command"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := db.ApplyCommitted(epaxos.ApplyCommand{Ref: epaxos.InstanceRef{Replica: 1, Instance: 1, Conf: 1}, Command: epaxos.Command{Payload: tc.payload}})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestScanIgnoresMalformedInternalRecordsAndKeepsNewestUserVersion(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PutVersion([]byte("stable"), []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if err := db.PutVersion([]byte("stable"), []byte("new"), 2); err != nil {
		t.Fatal(err)
	}
	badKey := append(EncodeUserPrefix(nil, 1, []byte("stable-bad-record-without-separator")), make([]byte, 8)...)
	if err := db.pebble.Set(badKey, []byte{valueRecord, 'x'}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	scan, err := db.Scan(ScanOptions{Start: []byte("stable"), End: []byte("stablez")})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan) != 1 || string(scan[0].Key) != "stable" || string(scan[0].Value) != "new" || scan[0].Time != 2 {
		t.Fatalf("scan=%#v", scan)
	}
}

func appendTxnPayload(count uint64, op byte, rest []byte) []byte {
	out := []byte{opTxn}
	out = binary.AppendUvarint(out, count)
	out = append(out, op)
	return append(out, rest...)
}

type recordingTxnBatch struct {
	sets int
}

func (b *recordingTxnBatch) Set(_, _ []byte, _ *pebble.WriteOptions) error {
	b.sets++
	return errors.New("recording batch Set should not be called for invalid keys")
}

func (*recordingTxnBatch) Commit(*pebble.WriteOptions) error {
	return nil
}

func (*recordingTxnBatch) Close() error {
	return nil
}
