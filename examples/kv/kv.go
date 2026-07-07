// Package kv provides a small distributed key-value example backed by Pebble.
package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

const (
	recordPrefix byte = 'd'
	valueRecord  byte = 1
	deleteRecord byte = 2
	opPut        byte = 1
	opDelete     byte = 2
)

// DB stores EPaxos-applied commands in a Pebble database.
type DB struct {
	pebble *pebble.DB
	cf     uint32
}

// KV is one key-value pair returned by scans.
type KV struct {
	Key   []byte
	Value []byte
	Time  uint64
}

// ScanOptions controls range scans.
type ScanOptions struct {
	Start   []byte
	End     []byte
	Prefix  []byte
	Limit   int
	Reverse bool
}

// Open opens a Pebble-backed example database.
func Open(path string) (*DB, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{pebble: db, cf: 1}, nil
}

// Close closes the underlying Pebble database.
func (db *DB) Close() error { return db.pebble.Close() }

// ApplyCommitted applies one committed EPaxos command to the key-value store.
func (db *DB) ApplyCommitted(cmd epaxos.CommittedCommand) error {
	if cmd.Command.Kind == epaxos.CommandNoop || len(cmd.Command.Payload) == 0 {
		return nil
	}
	op := cmd.Command.Payload[0]
	p := parser{b: cmd.Command.Payload[1:]}
	key := p.bytes()
	value := p.bytes()
	if p.err {
		return fmt.Errorf("kv: malformed command")
	}
	ts := (uint64(cmd.Ref.Replica) << 56) | uint64(cmd.Ref.Instance)
	switch op {
	case opPut:
		return db.PutVersion(key, value, ts)
	case opDelete:
		return db.DeleteVersion(key, ts)
	default:
		return fmt.Errorf("kv: unknown op %d", op)
	}
}

// PutVersion writes one version using the example's MyRocks-like record format.
func (db *DB) PutVersion(key, value []byte, ts uint64) error {
	k := EncodeDataKey(nil, db.cf, key, ts)
	v := append([]byte{valueRecord}, value...)
	return db.pebble.Set(k, v, pebble.Sync)
}

// DeleteVersion writes a tombstone version for key.
func (db *DB) DeleteVersion(key []byte, ts uint64) error {
	k := EncodeDataKey(nil, db.cf, key, ts)
	return db.pebble.Set(k, []byte{deleteRecord}, pebble.Sync)
}

// Get returns the newest live value for key.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	prefix := EncodeUserPrefix(nil, db.cf, key)
	upper := prefixLimit(prefix)
	iter, err := db.pebble.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = iter.Close() }()
	if !iter.First() {
		return nil, false, iter.Error()
	}
	v := iter.Value()
	if len(v) == 0 || v[0] == deleteRecord {
		return nil, false, nil
	}
	return append([]byte(nil), v[1:]...), true, nil
}

// Scan returns the newest live version for each key in the requested range.
func (db *DB) Scan(opt ScanOptions) ([]KV, error) {
	lowerUser := opt.Start
	upperUser := opt.End
	if len(opt.Prefix) > 0 {
		lowerUser = opt.Prefix
		upperUser = prefixLimit(opt.Prefix)
	}
	lower := EncodeUserPrefix(nil, db.cf, lowerUser)
	upper := EncodeUserPrefix(nil, db.cf, upperUser)
	if len(upperUser) == 0 {
		upper = prefixLimit(EncodeUserPrefix(nil, db.cf, nil))
	}
	iter, err := db.pebble.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	seen := make(map[string]struct{})
	out := make([]KV, 0)
	limit := opt.Limit
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	valid := false
	if opt.Reverse {
		valid = iter.Last()
	} else {
		valid = iter.First()
	}
	for valid && len(out) < limit {
		uk, ts, ok := DecodeDataKey(iter.Key(), db.cf)
		if ok {
			id := string(uk)
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
				v := iter.Value()
				if len(v) > 0 && v[0] == valueRecord {
					out = append(out, KV{Key: append([]byte(nil), uk...), Value: append([]byte(nil), v[1:]...), Time: ts})
				}
			}
		}
		if opt.Reverse {
			valid = iter.Prev()
		} else {
			valid = iter.Next()
		}
	}
	return out, iter.Error()
}

// CommandForPut encodes a deterministic EPaxos command for a key update.
func CommandForPut(client, seq uint64, key, value []byte) epaxos.Command {
	payload := appendKVCommand(opPut, key, value)
	return epaxos.Command{ID: epaxos.CommandID{Client: client, Sequence: seq}, Payload: payload, ConflictKeys: [][]byte{append([]byte(nil), key...)}}
}

// CommandForDelete encodes a deterministic EPaxos command for a key delete.
func CommandForDelete(client, seq uint64, key []byte) epaxos.Command {
	payload := appendKVCommand(opDelete, key, nil)
	return epaxos.Command{ID: epaxos.CommandID{Client: client, Sequence: seq}, Payload: payload, ConflictKeys: [][]byte{append([]byte(nil), key...)}}
}

// EncodeDataKey appends a MyRocks-like data key to dst.
func EncodeDataKey(dst []byte, cf uint32, user []byte, ts uint64) []byte {
	dst = EncodeUserPrefix(dst, cf, user)
	dst = append(dst, 0)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ^ts)
	return append(dst, b[:]...)
}

// EncodeUserPrefix appends the record prefix, column family, and user key.
func EncodeUserPrefix(dst []byte, cf uint32, user []byte) []byte {
	dst = append(dst, recordPrefix)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], cf)
	dst = append(dst, b[:]...)
	return append(dst, user...)
}

// DecodeDataKey decodes a key produced by EncodeDataKey.
func DecodeDataKey(k []byte, cf uint32) ([]byte, uint64, bool) {
	if len(k) < 1+4+1+8 || k[0] != recordPrefix || binary.BigEndian.Uint32(k[1:5]) != cf {
		return nil, 0, false
	}
	sep := bytes.LastIndexByte(k[:len(k)-8], 0)
	if sep < 5 {
		return nil, 0, false
	}
	user := k[5:sep]
	ts := ^binary.BigEndian.Uint64(k[len(k)-8:])
	return user, ts, true
}

func appendKVCommand(op byte, key, value []byte) []byte {
	out := []byte{op}
	out = binary.AppendUvarint(out, uint64(len(key)))
	out = append(out, key...)
	out = binary.AppendUvarint(out, uint64(len(value)))
	out = append(out, value...)
	return out
}

func prefixLimit(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

type parser struct {
	b   []byte
	err bool
}

func (p *parser) bytes() []byte {
	if p.err {
		return nil
	}
	n, used := binary.Uvarint(p.b)
	if used <= 0 || n > uint64(len(p.b[used:])) {
		p.err = true
		return nil
	}
	p.b = p.b[used:]
	out := p.b[:n]
	p.b = p.b[n:]
	return out
}

// IsNotFound reports whether an error represents a missing key in the example API.
func IsNotFound(err error) bool { return errors.Is(err, pebble.ErrNotFound) }
