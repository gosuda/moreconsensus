package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

const (
	epaxosStorePrefix  byte = 'e'
	epaxosRecordEntry  byte = 'r'
	epaxosAppliedEntry byte = 'a'
	epaxosRecordCodec  byte = 6
)

// PebbleStorage persists EPaxOS instance records inside the example Pebble database.
type PebbleStorage struct {
	pebble *pebble.DB
}

var newPebbleStorageIter = func(db *pebble.DB, opts *pebble.IterOptions) (kvIterator, error) {
	return db.NewIter(opts)
}

type appliedMarkerLookup func(epaxos.InstanceRef) (io.Closer, error)

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
		keyRef, ok := decodeEPaxosRecordKey(iter.Key())
		if !ok {
			return fmt.Errorf("kv: malformed epaxos record key")
		}
		rec, err := decodeEPaxosRecord(iter.Value())
		if err != nil {
			return err
		}
		if rec.Ref != keyRef {
			return fmt.Errorf("kv: epaxos record key/value ref mismatch")
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
		done, err := db.appliedReadyCommand(committed.Ref)
		if err != nil {
			return err
		}
		if done {
			continue
		}
		if _, err := db.stageCommitted(batch, committed, &next); err != nil {
			return err
		}
		if err := batch.Set(epaxosAppliedKey(committed.Ref), []byte{1}, nil); err != nil {
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

func decodeEPaxosRecordKey(key []byte) (epaxos.InstanceRef, bool) {
	return decodeEPaxosStoreRefKey(key, epaxosRecordEntry)
}

func decodeEPaxosAppliedKey(key []byte) (epaxos.InstanceRef, bool) {
	return decodeEPaxosStoreRefKey(key, epaxosAppliedEntry)
}

func decodeEPaxosStoreRefKey(key []byte, entry byte) (epaxos.InstanceRef, bool) {
	if len(key) != 26 || key[0] != epaxosStorePrefix || key[1] != entry {
		return epaxos.InstanceRef{}, false
	}
	return epaxos.InstanceRef{
		Conf:     epaxos.ConfID(binary.BigEndian.Uint64(key[2:10])),
		Replica:  epaxos.ReplicaID(binary.BigEndian.Uint64(key[10:18])),
		Instance: epaxos.InstanceNum(binary.BigEndian.Uint64(key[18:26])),
	}, true
}

func epaxosAppliedKey(ref epaxos.InstanceRef) []byte {
	out := make([]byte, 2, 26)
	out[0] = epaxosStorePrefix
	out[1] = epaxosAppliedEntry
	out = binary.BigEndian.AppendUint64(out, uint64(ref.Conf))
	out = binary.BigEndian.AppendUint64(out, uint64(ref.Replica))
	out = binary.BigEndian.AppendUint64(out, uint64(ref.Instance))
	return out
}

func (db *DB) appliedReadyCommand(ref epaxos.InstanceRef) (applied bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			applied = false
			err = fmt.Errorf("kv: applied marker read failed: %v", r)
		}
	}()
	closer, err := db.getAppliedMarker(ref)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = closer.Close()
	return true, nil
}

func (db *DB) getAppliedMarker(ref epaxos.InstanceRef) (io.Closer, error) {
	if db.lookupAppliedMarker != nil {
		return db.lookupAppliedMarker(ref)
	}
	_, closer, err := db.pebble.Get(epaxosAppliedKey(ref))
	return closer, err
}

func encodeEPaxosRecord(rec epaxos.InstanceRecord) []byte {
	out := []byte{epaxosRecordCodec}
	out = binary.AppendUvarint(out, uint64(rec.Ref.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Instance))
	out = binary.AppendUvarint(out, uint64(rec.Ref.Conf))
	out = binary.AppendUvarint(out, rec.Ballot.Epoch)
	out = binary.AppendUvarint(out, rec.Ballot.Number)
	out = binary.AppendUvarint(out, uint64(rec.Ballot.Replica))
	out = binary.AppendUvarint(out, rec.RecordBallot.Epoch)
	out = binary.AppendUvarint(out, rec.RecordBallot.Number)
	out = binary.AppendUvarint(out, uint64(rec.RecordBallot.Replica))
	out = binary.AppendUvarint(out, uint64(rec.Status))
	out = binary.AppendUvarint(out, rec.Seq)
	out = binary.AppendUvarint(out, uint64(len(rec.Deps)))
	for _, dep := range rec.Deps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, rec.AcceptSeq)
	out = binary.AppendUvarint(out, uint64(len(rec.AcceptDeps)))
	for _, dep := range rec.AcceptDeps {
		out = binary.AppendUvarint(out, uint64(dep))
	}
	out = binary.AppendUvarint(out, uint64(len(rec.AcceptEvidence)))
	for _, ev := range rec.AcceptEvidence {
		out = binary.AppendUvarint(out, uint64(ev.Sender))
		out = binary.AppendUvarint(out, ev.Seq)
		out = binary.AppendUvarint(out, uint64(len(ev.Deps)))
		for _, dep := range ev.Deps {
			out = binary.AppendUvarint(out, uint64(dep))
		}
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
	out = binary.AppendUvarint(out, rec.ProcessAt)
	if rec.TOQPending {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	if rec.FastPathEligible {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	out = append(out, rec.Checksum[:]...)
	return out
}

func decodeEPaxosRecord(src []byte) (epaxos.InstanceRecord, error) {
	p := parser{b: src}
	version := p.byte()
	if version < 1 || version > epaxosRecordCodec {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos record version")
	}
	rec := epaxos.InstanceRecord{}
	rec.Ref = epaxos.InstanceRef{Replica: epaxos.ReplicaID(p.uvarint()), Instance: epaxos.InstanceNum(p.uvarint()), Conf: epaxos.ConfID(p.uvarint())}
	rec.Ballot = epaxos.Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: epaxos.ReplicaID(p.uvarint())}
	if version >= 5 {
		rec.RecordBallot = epaxos.Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: epaxos.ReplicaID(p.uvarint())}
	}
	rec.Status = epaxos.Status(p.uvarint())
	if version < 5 && rec.Status != epaxos.StatusNone {
		rec.RecordBallot = rec.Ballot
	}
	rec.Seq = p.uvarint()
	deps := p.uvarint()
	if deps > 128 {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos dependency count")
	}
	rec.Deps = make([]epaxos.InstanceNum, int(deps))
	for i := range rec.Deps {
		rec.Deps[i] = epaxos.InstanceNum(p.uvarint())
	}
	if version >= 4 {
		rec.AcceptSeq = p.uvarint()
		acceptDeps := p.uvarint()
		if acceptDeps > 128 {
			return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos accept dependency count")
		}
		rec.AcceptDeps = make([]epaxos.InstanceNum, int(acceptDeps))
		for i := range rec.AcceptDeps {
			rec.AcceptDeps[i] = epaxos.InstanceNum(p.uvarint())
		}
	}
	if version >= 6 {
		evidence := p.uvarint()
		if evidence > 128 {
			return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos accept evidence count")
		}
		rec.AcceptEvidence = make([]epaxos.AcceptEvidence, int(evidence))
		seenEvidence := make(map[epaxos.ReplicaID]struct{}, int(evidence))
		for i := range rec.AcceptEvidence {
			sender := epaxos.ReplicaID(p.uvarint())
			if sender == 0 {
				return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos accept evidence sender")
			}
			if _, ok := seenEvidence[sender]; ok {
				return epaxos.InstanceRecord{}, fmt.Errorf("kv: duplicate epaxos accept evidence sender")
			}
			seenEvidence[sender] = struct{}{}
			rec.AcceptEvidence[i].Sender = sender
			rec.AcceptEvidence[i].Seq = p.uvarint()
			deps := p.uvarint()
			if deps > 128 {
				return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos accept evidence dependency count")
			}
			rec.AcceptEvidence[i].Deps = make([]epaxos.InstanceNum, int(deps))
			for j := range rec.AcceptEvidence[i].Deps {
				rec.AcceptEvidence[i].Deps[j] = epaxos.InstanceNum(p.uvarint())
			}
		}
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
	if version >= 3 {
		rec.ProcessAt = p.uvarint()
		rec.TOQPending = p.byte() == 1
		rec.FastPathEligible = p.byte() == 1
	} else if version >= 2 {
		rec.FastPathEligible = p.byte() == 1
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
	if epaxos.VerifyRecordChecksum(rec) {
		return rec, nil
	}
	if version < 6 {
		if epaxos.VerifyRecordChecksumWithoutSenderAcceptEvidence(rec) {
			rec.Checksum = epaxos.ChecksumRecord(rec)
			return rec, nil
		}
	}
	if version < 5 {
		if epaxos.VerifyRecordChecksumWithoutRecordBallot(rec) {
			rec.Checksum = epaxos.ChecksumRecord(rec)
			return rec, nil
		}
	}
	if version < 4 {
		if epaxos.VerifyRecordChecksumWithoutAcceptEvidence(rec) {
			rec.Checksum = epaxos.ChecksumRecord(rec)
			return rec, nil
		}
	}
	if version == 1 {
		rec.FastPathEligible = true
		if epaxos.VerifyRecordChecksum(rec) {
			return rec, nil
		}
		rec.FastPathEligible = false
	}
	if version < 3 {
		if epaxos.VerifyRecordChecksumWithoutTOQ(rec) {
			rec.Checksum = epaxos.ChecksumRecord(rec)
			return rec, nil
		}
		if version == 1 {
			rec.FastPathEligible = true
			if epaxos.VerifyRecordChecksumWithoutTOQ(rec) {
				rec.Checksum = epaxos.ChecksumRecord(rec)
				return rec, nil
			}
			rec.FastPathEligible = false
			if epaxos.VerifyRecordChecksumWithoutFastPathOrTOQ(rec) {
				rec.Checksum = epaxos.ChecksumRecord(rec)
				return rec, nil
			}
		}
	}
	return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
}
