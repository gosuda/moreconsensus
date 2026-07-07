package kv

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

const (
	epaxosStorePrefix byte = 'e'
	epaxosRecordEntry byte = 'r'
	epaxosRecordCodec byte = 1
)

// PebbleStorage persists EPaxOS instance records inside the example Pebble database.
type PebbleStorage struct {
	pebble *pebble.DB
}

var newPebbleStorageIter = func(db *pebble.DB, opts *pebble.IterOptions) (kvIterator, error) {
	return db.NewIter(opts)
}

// EPaxosStorage returns durable EPaxOS storage backed by the same Pebble database.
func (db *DB) EPaxosStorage() *PebbleStorage {
	return &PebbleStorage{pebble: db.pebble}
}

// InitialState returns the stored hard state and configuration history.
func (s *PebbleStorage) InitialState() (epaxos.HardState, []epaxos.ConfState, error) {
	return epaxos.HardState{}, nil, nil
}

// LoadInstances loads persisted EPAXOS records in deterministic instance order.
func (s *PebbleStorage) LoadInstances(fn func(epaxos.InstanceRecord) error) error {
	lower := []byte{epaxosStorePrefix, epaxosRecordEntry}
	iter, err := newPebbleStorageIter(s.pebble, &pebble.IterOptions{LowerBound: lower, UpperBound: prefixLimit(lower)})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	for valid := iter.First(); valid; valid = iter.Next() {
		rec, err := decodeEPaxosRecord(iter.Value())
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return iter.Error()
}

// ApplyReady persists the durable records in rd without applying commands.
func (s *PebbleStorage) ApplyReady(rd epaxos.Ready) error {
	if len(rd.Records) == 0 {
		return nil
	}
	batch := s.pebble.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := stageEPaxosRecords(batch, rd.Records); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

// ApplyReady atomically persists EPAXOS records and applies committed KV commands.
func (db *DB) ApplyReady(rd epaxos.Ready) error {
	if len(rd.Records) == 0 && len(rd.Committed) == 0 {
		return nil
	}
	batch := db.newWriteBatch()
	defer func() { _ = batch.Close() }()
	if err := stageEPaxosRecords(batch, rd.Records); err != nil {
		return err
	}
	next := db.nextRecordTime()
	for _, committed := range rd.Committed {
		if _, err := db.stageCommitted(batch, committed, &next); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	db.nextTime = next
	return nil
}

func stageEPaxosRecords(batch txnBatch, records []epaxos.InstanceRecord) error {
	for _, rec := range records {
		if !epaxos.VerifyRecordChecksum(rec) {
			return epaxos.ErrChecksumMismatch
		}
		if err := batch.Set(epaxosRecordKey(rec.Ref), encodeEPaxosRecord(rec), nil); err != nil {
			return err
		}
	}
	return nil
}

func epaxosRecordKey(ref epaxos.InstanceRef) []byte {
	out := make([]byte, 2, 26)
	out[0] = epaxosStorePrefix
	out[1] = epaxosRecordEntry
	out = binary.BigEndian.AppendUint64(out, uint64(ref.Conf))
	out = binary.BigEndian.AppendUint64(out, uint64(ref.Replica))
	out = binary.BigEndian.AppendUint64(out, uint64(ref.Instance))
	return out
}

func encodeEPaxosRecord(rec epaxos.InstanceRecord) []byte {
	out := []byte{epaxosRecordCodec}
	out = binary.AppendUvarint(out, uint64(rec.Ref.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Instance))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Conf))
	out = binary.AppendUvarint(out, rec.Ballot.Epoch)
	out = binary.AppendUvarint(out, rec.Ballot.Number)
	out = binary.AppendUvarint(out, uint64(rec.Ballot.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Status))
	out = binary.AppendUvarint(out, rec.Seq)
	out = binary.AppendUvarint(out, uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, rec.Command.ID.Client)
	out = binary.AppendUvarint(out, rec.Command.ID.Sequence)
	out = binary.AppendUvarint(out, uint64(rec.Command.Kind))
	out = binary.AppendUvarint(out, uint64(len(rec.Command.Payload)))
	out = append(out, rec.Command.Payload...)
	out = binary.AppendUvarint(out, uint64(len(rec.Command.ConflictKeys)))
	for _, key := range rec.Command.ConflictKeys {
		out = binary.AppendUvarint(out, uint64(len(key)))
		out = append(out, key...)
	}
	out = append(out, rec.Checksum[:]...)
	return out
}

func decodeEPaxosRecord(src []byte) (epaxos.InstanceRecord, error) {
	p := parser{b: src}
	if p.byte() != epaxosRecordCodec {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos record version")
	}
	rec := epaxos.InstanceRecord{}
	rec.Ref = epaxos.InstanceRef{Replica: epaxos.ReplicaID(p.uvarint()), Instance: epaxos.InstanceNum(p.uvarint()), Conf: epaxos.ConfID(p.uvarint())}
	rec.Ballot = epaxos.Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: epaxos.ReplicaID(p.uvarint())}
	rec.Status = epaxos.Status(p.uvarint())
	rec.Seq = p.uvarint()
	deps := p.uvarint()
	if deps > 128 {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos dependency count")
	}
	rec.Deps = make([]epaxos.InstanceNum, int(deps))
	for i := range rec.Deps {
		rec.Deps[i] = epaxos.InstanceNum(p.uvarint())
	}
	rec.Command = epaxos.Command{ID: epaxos.CommandID{Client: p.uvarint(), Sequence: p.uvarint()}, Kind: epaxos.CommandKind(p.uvarint())}
	rec.Command.Payload = append([]byte(nil), p.bytes()...)
	keys := p.uvarint()
	if keys > 128 {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos conflict-key count")
	}
	rec.Command.ConflictKeys = make([][]byte, int(keys))
	for i := range rec.Command.ConflictKeys {
		rec.Command.ConflictKeys[i] = append([]byte(nil), p.bytes()...)
	}
	if len(p.b) < len(rec.Checksum) {
		p.err = true
	} else {
		copy(rec.Checksum[:], p.b[:len(rec.Checksum)])
		p.b = p.b[len(rec.Checksum):]
	}
	if p.err || len(p.b) != 0 {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: malformed epaxos record")
	}
	if !epaxos.VerifyRecordChecksum(rec) {
		return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
	}
	return rec, nil
}
