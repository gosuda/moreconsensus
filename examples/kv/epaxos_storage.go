package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"
	"gosuda.org/moreconsensus/epaxos"
)

const (
	epaxosStorePrefix             byte = 'e'
	epaxosRecordEntry             byte = 'r'
	epaxosAppliedEntry            byte = 'a'
	epaxosHardStateEntry          byte = 'h'
	epaxosRecordCodec             byte = 8
	epaxosHardStateCodec          byte = 1
	epaxosHardStateChecksumSize        = 32
	epaxosHardStateChecksumDomain      = "gosuda.org/moreconsensus/examples/kv/epaxos-hard-state/v1"
)

var epaxosHardStateKey = []byte{epaxosStorePrefix, epaxosHardStateEntry}

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
	hardState, _, err := loadEPaxosHardState(s.pebble)
	return hardState, nil, err
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
	if len(rd.Records) == 0 && rd.HardState.Empty() {
		return nil
	}
	hardState, err := prepareEPaxosHardState(s.pebble, rd.HardState)
	if err != nil {
		return err
	}
	batch := s.pebble.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := stageEPaxosRecords(batch, rd.Records); err != nil {
		return err
	}
	if hardState != nil {
		if err := batch.Set(epaxosHardStateKey, hardState, nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

// ApplyReady atomically persists EPAXOS records and applies committed KV commands.
func (db *DB) ApplyReady(rd epaxos.Ready) error {
	if len(rd.Records) == 0 && len(rd.Committed) == 0 && rd.HardState.Empty() {
		return nil
	}
	hardState, err := prepareEPaxosHardState(db.pebble, rd.HardState)
	if err != nil {
		return err
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
	if hardState != nil {
		if err := batch.Set(epaxosHardStateKey, hardState, nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	db.nextTime = next
	return nil
}

func prepareEPaxosHardState(pebbleDB *pebble.DB, next epaxos.HardState) ([]byte, error) {
	if next.Empty() {
		return nil, nil
	}
	if err := validateEPaxosHardState(next); err != nil {
		return nil, err
	}
	current, found, err := loadEPaxosHardState(pebbleDB)
	if err != nil {
		return nil, err
	}
	if found {
		if next.Tick < current.Tick {
			return nil, fmt.Errorf("%w: hard-state tick regression from %d to %d", epaxos.ErrInvalidConfig, current.Tick, next.Tick)
		}
		if next.Conf.ID < current.Conf.ID {
			return nil, fmt.Errorf("%w: hard-state configuration regression from %d to %d", epaxos.ErrInvalidConfig, current.Conf.ID, next.Conf.ID)
		}
		if next.Conf.ID == current.Conf.ID && !sameEPaxosVoters(next.Conf.Voters, current.Conf.Voters) {
			return nil, fmt.Errorf("%w: conflicting hard-state voters for configuration %d", epaxos.ErrInvalidConfig, next.Conf.ID)
		}
	}
	return encodeEPaxosHardState(next), nil
}

func validateEPaxosHardState(hardState epaxos.HardState) error {
	if hardState.Empty() {
		return nil
	}
	if hardState.Conf.ID == 0 {
		return fmt.Errorf("%w: hard state requires a nonzero configuration id", epaxos.ErrInvalidConfig)
	}
	if len(hardState.Conf.Voters) < 1 || len(hardState.Conf.Voters) > 7 {
		return fmt.Errorf("%w: hard-state voter count must be 1..7", epaxos.ErrInvalidConfig)
	}
	var previous epaxos.ReplicaID
	for _, voter := range hardState.Conf.Voters {
		if voter == 0 {
			return fmt.Errorf("%w: hard-state voter must be nonzero", epaxos.ErrInvalidConfig)
		}
		if voter <= previous {
			return fmt.Errorf("%w: hard-state voters must be sorted and unique", epaxos.ErrInvalidConfig)
		}
		previous = voter
	}
	return nil
}

func sameEPaxosVoters(left, right []epaxos.ReplicaID) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func encodeEPaxosHardState(hardState epaxos.HardState) []byte {
	voterCount := len(hardState.Conf.Voters)
	payloadSize := 18 + voterCount*8
	out := make([]byte, payloadSize+epaxosHardStateChecksumSize)
	out[0] = epaxosHardStateCodec
	binary.BigEndian.PutUint64(out[1:9], uint64(hardState.Conf.ID))
	out[9] = byte(voterCount)
	offset := 10
	for _, voter := range hardState.Conf.Voters {
		binary.BigEndian.PutUint64(out[offset:offset+8], uint64(voter))
		offset += 8
	}
	binary.BigEndian.PutUint64(out[offset:offset+8], hardState.Tick)
	checksum := checksumEPaxosHardState(out[:payloadSize])
	copy(out[payloadSize:], checksum[:])
	return out
}

func decodeEPaxosHardState(src []byte) (epaxos.HardState, error) {
	if len(src) == 0 {
		return epaxos.HardState{}, fmt.Errorf("kv: malformed epaxos hard state")
	}
	if src[0] != epaxosHardStateCodec {
		return epaxos.HardState{}, fmt.Errorf("kv: bad epaxos hard-state version %d", src[0])
	}
	if len(src) < 10 {
		return epaxos.HardState{}, fmt.Errorf("kv: malformed epaxos hard state")
	}
	voterCount := int(src[9])
	if voterCount < 1 || voterCount > 7 {
		return epaxos.HardState{}, fmt.Errorf("%w: hard-state voter count must be 1..7", epaxos.ErrInvalidConfig)
	}
	payloadSize := 18 + voterCount*8
	encodedSize := payloadSize + epaxosHardStateChecksumSize
	if len(src) < encodedSize {
		return epaxos.HardState{}, fmt.Errorf("kv: malformed epaxos hard state")
	}
	if len(src) > encodedSize {
		return epaxos.HardState{}, fmt.Errorf("kv: trailing bytes in epaxos hard state")
	}
	checksum := checksumEPaxosHardState(src[:payloadSize])
	var storedChecksum [epaxosHardStateChecksumSize]byte
	copy(storedChecksum[:], src[payloadSize:])
	if checksum != storedChecksum {
		return epaxos.HardState{}, epaxos.ErrChecksumMismatch
	}
	hardState := epaxos.HardState{
		Conf: epaxos.ConfState{
			ID:     epaxos.ConfID(binary.BigEndian.Uint64(src[1:9])),
			Voters: make([]epaxos.ReplicaID, voterCount),
		},
	}
	offset := 10
	for i := range hardState.Conf.Voters {
		hardState.Conf.Voters[i] = epaxos.ReplicaID(binary.BigEndian.Uint64(src[offset : offset+8]))
		offset += 8
	}
	hardState.Tick = binary.BigEndian.Uint64(src[offset : offset+8])
	if err := validateEPaxosHardState(hardState); err != nil {
		return epaxos.HardState{}, err
	}
	return hardState, nil
}

func checksumEPaxosHardState(payload []byte) [epaxosHardStateChecksumSize]byte {
	hasher := blake3.NewDeriveKey(epaxosHardStateChecksumDomain)
	_, _ = hasher.Write(payload)
	var out [epaxosHardStateChecksumSize]byte
	sum := hasher.Sum(out[:0])
	copy(out[:], sum)
	return out
}

func loadEPaxosHardState(pebbleDB *pebble.DB) (epaxos.HardState, bool, error) {
	value, closer, err := pebbleDB.Get(epaxosHardStateKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return epaxos.HardState{}, false, nil
	}
	if err != nil {
		return epaxos.HardState{}, false, err
	}
	hardState, decodeErr := decodeEPaxosHardState(value)
	closeErr := closer.Close()
	if decodeErr != nil {
		return epaxos.HardState{}, false, decodeErr
	}
	if closeErr != nil {
		return epaxos.HardState{}, false, closeErr
	}
	return hardState, true, nil
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
	out = append(out, byte(rec.TimingDomain))
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
	out = binary.AppendUvarint(out, uint64(rec.ConfChangeResult.Outcome))
	out = binary.AppendUvarint(out, uint64(rec.ConfChangeResult.Conf.ID))
	out = binary.AppendUvarint(out, uint64(len(rec.ConfChangeResult.Conf.Voters)))
	for _, voter := range rec.ConfChangeResult.Conf.Voters {
		out = binary.AppendUvarint(out, uint64(voter))
	}
	out = append(out, rec.Checksum[:]...)
	return out
}

type epaxosRecordParser struct {
	parser
	canonical bool
}

func (p *epaxosRecordParser) uvarint() uint64 {
	if p.err {
		return 0
	}
	value, used := binary.Uvarint(p.b)
	if used <= 0 {
		p.err = true
		return 0
	}
	if p.canonical {
		var encoded [binary.MaxVarintLen64]byte
		if binary.PutUvarint(encoded[:], value) != used {
			p.err = true
			return 0
		}
	}
	p.b = p.b[used:]
	return value
}

func (p *epaxosRecordParser) bytes() []byte {
	length := p.uvarint()
	if p.err || length > uint64(len(p.b)) {
		p.err = true
		return nil
	}
	out := p.b[:length]
	p.b = p.b[length:]
	return out
}

func decodeEPaxosRecord(src []byte) (epaxos.InstanceRecord, error) {
	p := epaxosRecordParser{parser: parser{b: src}}
	version := p.byte()
	p.canonical = version >= 8
	if version < 1 || version > epaxosRecordCodec {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos record version")
	}
	rec := epaxos.InstanceRecord{}
	rec.Ref = epaxos.InstanceRef{Replica: epaxos.ReplicaID(p.uvarint()), Instance: epaxos.InstanceNum(p.uvarint()), Conf: epaxos.ConfID(p.uvarint())}
	rec.Ballot = epaxos.Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: epaxos.ReplicaID(p.uvarint())}
	if version >= 5 {
		rec.RecordBallot = epaxos.Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: epaxos.ReplicaID(p.uvarint())}
	}
	statusValue := p.uvarint()
	if version >= 8 && statusValue > uint64(epaxos.StatusExecuted) {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos record status")
	}
	rec.Status = epaxos.Status(statusValue)
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
	commandClient := p.uvarint()
	commandSequence := p.uvarint()
	commandKind := p.uvarint()
	if version >= 8 && commandKind > uint64(epaxos.CommandConfChange) {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos command kind")
	}
	rec.Command = epaxos.Command{ID: epaxos.CommandID{Client: commandClient, Sequence: commandSequence}, Kind: epaxos.CommandKind(commandKind)}
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
		if version >= 8 {
			rec.TimingDomain = epaxos.TimingDomain(p.byte())
			if rec.TimingDomain > epaxos.TimingDomainTOQ {
				return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos timing domain")
			}
		}
		toqPending := p.byte()
		fastPathEligible := p.byte()
		if version >= 8 && (toqPending > 1 || fastPathEligible > 1) {
			return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos record flags")
		}
		rec.TOQPending = toqPending == 1
		rec.FastPathEligible = fastPathEligible == 1
		if version >= 8 {
			switch rec.TimingDomain {
			case epaxos.TimingDomainUntimed:
				if rec.ProcessAt != 0 || rec.TOQPending {
					return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos timing metadata")
				}
			case epaxos.TimingDomainLogical:
				if rec.ProcessAt == 0 || rec.TOQPending {
					return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos timing metadata")
				}
			}
		}
	} else if version >= 2 {
		rec.FastPathEligible = p.byte() == 1
	}
	if version >= 7 {
		outcomeValue := p.uvarint()
		if outcomeValue > uint64(epaxos.ConfChangeRejectedInvalid) {
			return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos conf change outcome")
		}
		rec.ConfChangeResult.Outcome = epaxos.ConfChangeOutcome(outcomeValue)
		rec.ConfChangeResult.Conf.ID = epaxos.ConfID(p.uvarint())
		voters := p.uvarint()
		if voters > 7 {
			return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos conf change voter count")
		}
		if voters > 0 {
			rec.ConfChangeResult.Conf.Voters = make([]epaxos.ReplicaID, int(voters))
			var previous epaxos.ReplicaID
			for i := range rec.ConfChangeResult.Conf.Voters {
				voter := epaxos.ReplicaID(p.uvarint())
				if voter == 0 || voter <= previous {
					return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos conf change voter order")
				}
				rec.ConfChangeResult.Conf.Voters[i] = voter
				previous = voter
			}
		}
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
	if version == epaxosRecordCodec {
		if epaxos.VerifyRecordChecksum(rec) {
			return rec, nil
		}
		return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
	}
	if version == 7 {
		if epaxos.VerifyRecordChecksumWithoutTimingDomain(rec) {
			return rec, nil
		}
		return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
	}
	if version == 6 {
		if epaxos.VerifyRecordChecksumWithoutConfChangeResult(rec) {
			rec.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(rec)
			return rec, nil
		}
		return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
	}
	if version < 6 && epaxos.VerifyRecordChecksumWithoutTimingDomain(rec) {
		return rec, nil
	}
	if version < 6 {
		if epaxos.VerifyRecordChecksumWithoutSenderAcceptEvidence(rec) {
			rec.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(rec)
			return rec, nil
		}
	}
	if version < 5 {
		legacy := rec
		legacy.RecordBallot = epaxos.Ballot{}
		if epaxos.VerifyRecordChecksumWithoutRecordBallot(legacy) {
			rec.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(rec)
			return rec, nil
		}
	}
	if version < 4 {
		legacy := rec
		legacy.RecordBallot = epaxos.Ballot{}
		if epaxos.VerifyRecordChecksumWithoutAcceptEvidence(legacy) {
			rec.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(rec)
			return rec, nil
		}
	}
	if version == 1 {
		rec.FastPathEligible = true
		if epaxos.VerifyRecordChecksumWithoutTimingDomain(rec) {
			return rec, nil
		}
		rec.FastPathEligible = false
	}
	if version < 3 {
		legacy := rec
		legacy.RecordBallot = epaxos.Ballot{}
		if epaxos.VerifyRecordChecksumWithoutTOQ(legacy) {
			rec.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(rec)
			return rec, nil
		}
		if version == 1 {
			legacy.FastPathEligible = true
			if epaxos.VerifyRecordChecksumWithoutTOQ(legacy) {
				rec.FastPathEligible = true
				rec.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(rec)
				return rec, nil
			}
			legacy.FastPathEligible = false
			if epaxos.VerifyRecordChecksumWithoutFastPathOrTOQ(legacy) {
				rec.Checksum = epaxos.ChecksumRecordWithoutTimingDomain(rec)
				return rec, nil
			}
		}
	}
	return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
}
