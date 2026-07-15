package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"
	"gosuda.org/moreconsensus/epaxos"
)

const (
	epaxosStorePrefix                   byte = 'e'
	epaxosRecordEntry                   byte = 'r'
	epaxosAppliedEntry                  byte = 'a' // legacy instance marker, retained for migration
	epaxosHardStateEntry                byte = 'h'
	epaxosRecordCodec                   byte = 9
	epaxosHardStateCodec                byte = 1
	epaxosCheckpointEntry               byte = 'c'
	epaxosCheckpointCodec               byte = 1
	epaxosCommandResultEntry            byte = 'd'
	epaxosCommandResultCodec            byte = 1
	epaxosApplicationSnapshotEntry      byte = 's'
	epaxosApplicationSnapshotCodec      byte = 1
	epaxosApplicationSnapshotHandleSize      = len(epaxos.StateDigest{}) + 8
	epaxosApplicationSnapshotDomain          = "gosuda.org/moreconsensus/examples/kv/application-snapshot/v1"
	epaxosCommandDigestDomain                = "gosuda.org/moreconsensus/examples/kv/epaxos-command/v1"
	epaxosHardStateChecksumSize              = 32
	epaxosHardStateChecksumDomain            = "gosuda.org/moreconsensus/examples/kv/epaxos-hard-state/v1"
)

var epaxosHardStateKey = []byte{epaxosStorePrefix, epaxosHardStateEntry}
var epaxosCheckpointKey = []byte{epaxosStorePrefix, epaxosCheckpointEntry}

// PebbleStorage persists EPaxOS instance records inside the example Pebble database.
type PebbleStorage struct {
	db     *DB
	pebble *pebble.DB
}

var newPebbleStorageIter = func(db *pebble.DB, opts *pebble.IterOptions) (kvIterator, error) {
	return db.NewIter(opts)
}

type appliedMarkerLookup func(epaxos.InstanceRef) (io.Closer, error)

// EPaxosStorage returns durable EPaxOS storage backed by the same Pebble database.
func (db *DB) EPaxosStorage() *PebbleStorage {
	return &PebbleStorage{db: db, pebble: db.pebble}
}

func applicationSnapshotKey(handle []byte) []byte {
	out := []byte{epaxosStorePrefix, epaxosApplicationSnapshotEntry}
	return append(out, handle...)
}

func applicationStateKey(key []byte) bool {
	return len(key) != 0 && (key[0] == recordPrefix ||
		len(key) >= 2 && key[0] == epaxosStorePrefix &&
			(key[1] == epaxosCommandResultEntry || key[1] == epaxosAppliedEntry))
}

func digestApplicationSnapshot(bundle []byte) epaxos.StateDigest {
	hasher := blake3.NewDeriveKey(epaxosApplicationSnapshotDomain)
	_, _ = hasher.Write(bundle)
	var digest epaxos.StateDigest
	copy(digest[:], hasher.Sum(nil))
	return digest
}
func encodeApplicationSnapshotHandle(digest epaxos.StateDigest, size uint64) []byte {
	handle := make([]byte, epaxosApplicationSnapshotHandleSize)
	copy(handle, digest[:])
	binary.BigEndian.PutUint64(handle[len(digest):], size)
	return handle
}

func decodeApplicationSnapshotHandle(handle []byte) (epaxos.StateDigest, uint64, error) {
	if len(handle) != epaxosApplicationSnapshotHandleSize {
		return epaxos.StateDigest{}, 0, epaxos.ErrInvalidCheckpoint
	}
	var digest epaxos.StateDigest
	copy(digest[:], handle[:len(digest)])
	return digest, binary.BigEndian.Uint64(handle[len(digest):]), nil
}

// ApplicationSnapshotBundleSize returns the authenticated transfer size carried
// by an opaque example-KV application snapshot handle.
func ApplicationSnapshotBundleSize(handle []byte) (uint64, error) {
	_, size, err := decodeApplicationSnapshotHandle(handle)
	return size, err
}

func encodeApplicationSnapshot(entries [][2][]byte) []byte {
	out := []byte{epaxosApplicationSnapshotCodec}
	out = binary.AppendUvarint(out, uint64(len(entries)))
	for _, entry := range entries {
		out = appendLengthBytes(out, entry[0])
		out = appendLengthBytes(out, entry[1])
	}
	return out
}

func decodeApplicationSnapshot(bundle []byte) ([][2][]byte, error) {
	if len(bundle) < 2 || bundle[0] != epaxosApplicationSnapshotCodec {
		return nil, fmt.Errorf("kv: malformed application snapshot")
	}
	p := epaxosRecordParser{parser: parser{b: bundle[1:]}, canonical: true}
	count := p.uvarint()
	if count > uint64(len(bundle)) {
		return nil, fmt.Errorf("kv: malformed application snapshot")
	}
	entries := make([][2][]byte, count)
	for i := range entries {
		entries[i] = [2][]byte{bytes.Clone(p.bytes()), bytes.Clone(p.bytes())}
		if p.err || !applicationStateKey(entries[i][0]) ||
			i > 0 && bytes.Compare(entries[i-1][0], entries[i][0]) >= 0 {
			return nil, fmt.Errorf("kv: malformed application snapshot")
		}
	}
	if p.err || len(p.b) != 0 {
		return nil, fmt.Errorf("kv: malformed application snapshot")
	}
	return entries, nil
}

// CreateApplicationCheckpoint captures application state and durable command
// results at the exact Ready.Checkpoint cut.
func (db *DB) CreateApplicationCheckpoint(request epaxos.CheckpointRequest) (epaxos.CheckpointResult, error) {
	db.durableMu.Lock()
	defer db.durableMu.Unlock()
	iter, err := db.pebble.NewIter(nil)
	if err != nil {
		return epaxos.CheckpointResult{}, err
	}
	var entries [][2][]byte
	for valid := iter.First(); valid; valid = iter.Next() {
		if applicationStateKey(iter.Key()) {
			entries = append(entries, [2][]byte{bytes.Clone(iter.Key()), bytes.Clone(iter.Value())})
		}
	}
	iterErr := iter.Error()
	closeErr := iter.Close()
	if iterErr != nil {
		return epaxos.CheckpointResult{}, iterErr
	}
	if closeErr != nil {
		return epaxos.CheckpointResult{}, closeErr
	}
	bundle := encodeApplicationSnapshot(entries)
	digest := digestApplicationSnapshot(bundle)
	handle := encodeApplicationSnapshotHandle(digest, uint64(len(bundle)))
	if err := db.pebble.Set(applicationSnapshotKey(handle), bundle, pebble.Sync); err != nil {
		return epaxos.CheckpointResult{}, err
	}
	return epaxos.CheckpointResult{
		ID: request.ID, ApplicationSnapshot: handle, ApplicationDigest: digest,
	}, nil
}

// ApplicationSnapshotBundle returns a verified content-addressed snapshot bundle.
func (db *DB) ApplicationSnapshotBundle(handle []byte) ([]byte, error) {
	wantDigest, wantSize, err := decodeApplicationSnapshotHandle(handle)
	if err != nil {
		return nil, err
	}
	value, closer, err := db.pebble.Get(applicationSnapshotKey(handle))
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	bundle := bytes.Clone(value)
	if uint64(len(bundle)) != wantSize || digestApplicationSnapshot(bundle) != wantDigest {
		return nil, epaxos.ErrChecksumMismatch
	}
	if _, err := decodeApplicationSnapshot(bundle); err != nil {
		return nil, err
	}
	return bundle, nil
}

// MaterializeApplicationSnapshot verifies and publishes a fetched bundle.
func (db *DB) MaterializeApplicationSnapshot(handle, bundle []byte) error {
	wantDigest, wantSize, err := decodeApplicationSnapshotHandle(handle)
	if err != nil {
		return err
	}
	if uint64(len(bundle)) != wantSize || digestApplicationSnapshot(bundle) != wantDigest {
		return epaxos.ErrChecksumMismatch
	}
	if _, err := decodeApplicationSnapshot(bundle); err != nil {
		return err
	}
	return db.pebble.Set(applicationSnapshotKey(handle), bundle, pebble.Sync)
}

func (db *DB) validateApplicationSnapshot(checkpoint epaxos.Checkpoint) ([][2][]byte, error) {
	handle := checkpoint.ApplicationSnapshot
	handleDigest, _, err := decodeApplicationSnapshotHandle(handle)
	if err != nil || checkpoint.Descriptor.ApplicationDigest == (epaxos.StateDigest{}) ||
		handleDigest != checkpoint.Descriptor.ApplicationDigest {
		return nil, epaxos.ErrInvalidCheckpoint
	}
	bundle, err := db.ApplicationSnapshotBundle(handle)
	if err != nil {
		return nil, err
	}
	if digestApplicationSnapshot(bundle) != checkpoint.Descriptor.ApplicationDigest {
		return nil, epaxos.ErrChecksumMismatch
	}
	return decodeApplicationSnapshot(bundle)
}

func (db *DB) installApplicationSnapshot(checkpoint epaxos.Checkpoint) error {
	entries, err := db.validateApplicationSnapshot(checkpoint)
	if err != nil {
		return err
	}
	batch := db.pebble.NewBatch()
	defer func() { _ = batch.Close() }()
	iter, err := db.pebble.NewIter(nil)
	if err != nil {
		return err
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		if applicationStateKey(iter.Key()) {
			if err := batch.Delete(bytes.Clone(iter.Key()), nil); err != nil {
				_ = iter.Close()
				return err
			}
		}
	}
	iterErr := iter.Error()
	closeErr := iter.Close()
	if iterErr != nil {
		return iterErr
	}
	if closeErr != nil {
		return closeErr
	}
	for _, entry := range entries {
		if err := batch.Set(entry[0], entry[1], nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	next, err := db.loadNextTime()
	if err == nil {
		db.nextTime = next
	}
	return err
}

// InitialState returns the complete stored durable consensus projection.
func (s *PebbleStorage) InitialState() (epaxos.StorageState, error) {
	hardState, _, err := loadEPaxosHardState(s.pebble)
	if err != nil {
		return epaxos.StorageState{}, err
	}
	return epaxos.StorageState{HardState: hardState}, nil
}

// LoadCheckpoint returns the durable protocol checkpoint metadata.
func (s *PebbleStorage) LoadCheckpoint() (epaxos.Checkpoint, error) {
	value, closer, err := s.pebble.Get(epaxosCheckpointKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return epaxos.Checkpoint{}, nil
	}
	if err != nil {
		return epaxos.Checkpoint{}, err
	}
	defer func() { _ = closer.Close() }()
	return decodeEPaxosCheckpoint(value)
}

// LoadInstances loads persisted EPAXOS records in deterministic instance order.
func (s *PebbleStorage) LoadInstances(after epaxos.ExecutionFrontier, fn func(epaxos.InstanceRecord) error) error {
	lower := []byte{epaxosStorePrefix, epaxosRecordEntry}
	iter, err := newPebbleStorageIter(s.pebble, &pebble.IterOptions{LowerBound: lower, UpperBound: prefixLimit(lower)})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	var migrations *pebble.Batch
	defer func() {
		if migrations != nil {
			_ = migrations.Close()
		}
	}()
	for valid := iter.First(); valid; {
		keyRef, ok := decodeEPaxosRecordKey(iter.Key())
		if !ok {
			return fmt.Errorf("kv: malformed epaxos record key")
		}
		through := executionFrontierThrough(after, keyRef.Conf, keyRef.Replica)
		if keyRef.Instance <= through {
			if through == epaxos.InstanceNum(^uint64(0)) {
				return fmt.Errorf("kv: compacted epaxos frontier overflows instance space")
			}
			seek := epaxosRecordKey(epaxos.InstanceRef{
				Conf: keyRef.Conf, Replica: keyRef.Replica, Instance: through + 1,
			})
			valid = iter.SeekGE(seek)
			continue
		}
		value := iter.Value()
		rec, err := decodeEPaxosRecord(value)
		if err != nil {
			return err
		}
		if rec.Ref != keyRef {
			return fmt.Errorf("kv: epaxos record key/value ref mismatch")
		}
		if len(value) > 0 && value[0] == 8 {
			if migrations == nil {
				migrations = s.pebble.NewBatch()
			}
			if err := migrations.Set(epaxosRecordKey(rec.Ref), encodeEPaxosRecord(rec), nil); err != nil {
				return err
			}
		}
		if err := fn(rec); err != nil {
			return err
		}
		valid = iter.Next()
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if migrations != nil {
		return migrations.Commit(pebble.Sync)
	}
	return nil
}

// LoadInstance returns one persisted EPaxos record for a Ready.RecordLoads request.
func (s *PebbleStorage) LoadInstance(ref epaxos.InstanceRef) (epaxos.InstanceRecord, bool, error) {
	checkpoint, err := s.LoadCheckpoint()
	if err != nil {
		return epaxos.InstanceRecord{}, false, err
	}
	if executionFrontierCovers(checkpoint.CompactedThrough, ref) {
		return epaxos.InstanceRecord{}, false, nil
	}
	value, closer, err := s.pebble.Get(epaxosRecordKey(ref))
	if errors.Is(err, pebble.ErrNotFound) {
		return epaxos.InstanceRecord{}, false, nil
	}
	if err != nil {
		return epaxos.InstanceRecord{}, false, err
	}
	defer func() { _ = closer.Close() }()
	rec, err := decodeEPaxosRecord(value)
	if err != nil {
		return epaxos.InstanceRecord{}, false, err
	}
	if rec.Ref != ref {
		return epaxos.InstanceRecord{}, false, fmt.Errorf("kv: epaxos record key/value ref mismatch")
	}
	return rec, true, nil
}

// NextCommandSequence returns one greater than the largest durable sequence for client.
func (db *DB) NextCommandSequence(client uint64) (uint64, error) {
	prefix := epaxosCommandResultClientPrefix(client)
	iter, err := newPebbleStorageIter(db.pebble, &pebble.IterOptions{LowerBound: prefix, UpperBound: prefixLimit(prefix)})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()
	next := uint64(1)
	for valid := iter.First(); valid; valid = iter.Next() {
		id, ok := decodeEPaxosCommandResultKey(iter.Key())
		if !ok || id.Client != client {
			return 0, fmt.Errorf("kv: malformed command-result key")
		}
		if _, _, err := decodeEPaxosCommandResult(iter.Value()); err != nil {
			return 0, err
		}
		if id.Sequence == ^uint64(0) {
			return 0, fmt.Errorf("kv: command sequence exhausted for client %d", client)
		}
		next = id.Sequence + 1
	}
	return next, iter.Error()
}

func epaxosCommandResultClientPrefix(client uint64) []byte {
	out := []byte{epaxosStorePrefix, epaxosCommandResultEntry}
	return binary.BigEndian.AppendUint64(out, client)
}

func epaxosCommandResultKey(id epaxos.CommandID) []byte {
	out := epaxosCommandResultClientPrefix(id.Client)
	return binary.BigEndian.AppendUint64(out, id.Sequence)
}

func decodeEPaxosCommandResultKey(key []byte) (epaxos.CommandID, bool) {
	if len(key) != 18 || key[0] != epaxosStorePrefix || key[1] != epaxosCommandResultEntry {
		return epaxos.CommandID{}, false
	}
	return epaxos.CommandID{
		Client:   binary.BigEndian.Uint64(key[2:10]),
		Sequence: binary.BigEndian.Uint64(key[10:18]),
	}, true
}

func digestEPaxosCommand(command epaxos.Command) (epaxos.StateDigest, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return epaxos.StateDigest{}, err
	}
	hasher := blake3.NewDeriveKey(epaxosCommandDigestDomain)
	_, _ = hasher.Write(encoded)
	var digest epaxos.StateDigest
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func encodeEPaxosCommandResult(digest epaxos.StateDigest, response []byte) []byte {
	out := make([]byte, 1, 1+len(digest)+binary.MaxVarintLen64+len(response))
	out[0] = epaxosCommandResultCodec
	out = append(out, digest[:]...)
	out = binary.AppendUvarint(out, uint64(len(response)))
	return append(out, response...)
}

func decodeEPaxosCommandResult(encoded []byte) (epaxos.StateDigest, []byte, error) {
	if len(encoded) < 1+32+1 || encoded[0] != epaxosCommandResultCodec {
		return epaxos.StateDigest{}, nil, fmt.Errorf("kv: malformed command result")
	}
	var digest epaxos.StateDigest
	copy(digest[:], encoded[1:33])
	length, used := binary.Uvarint(encoded[33:])
	if used <= 0 {
		return epaxos.StateDigest{}, nil, fmt.Errorf("kv: malformed command result")
	}
	remaining := len(encoded) - 33 - used
	//nolint:gosec // G115: remaining is nonnegative and no larger than the encoded slice.
	if length != uint64(remaining) {
		return epaxos.StateDigest{}, nil, fmt.Errorf("kv: malformed command result")
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], length) != used {
		return epaxos.StateDigest{}, nil, fmt.Errorf("kv: noncanonical command result")
	}
	return digest, bytes.Clone(encoded[33+used:]), nil
}

// CommandResult returns the durable response for an applied command ID.
func (db *DB) CommandResult(id epaxos.CommandID) ([]byte, bool, error) {
	value, closer, err := db.pebble.Get(epaxosCommandResultKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = closer.Close() }()
	_, response, err := decodeEPaxosCommandResult(value)
	return response, err == nil, err
}

// ApplyReady persists the durable records in rd without applying commands.
func (s *PebbleStorage) ApplyReady(rd epaxos.Ready) error {
	if len(rd.Records) == 0 && rd.HardState.Empty() && rd.Snapshot == nil && len(rd.Compact) == 0 {
		return nil
	}
	s.db.durableMu.Lock()
	defer s.db.durableMu.Unlock()
	hardState, err := prepareEPaxosHardState(s.pebble, rd.HardState)
	if err != nil {
		return err
	}
	batch := s.pebble.NewBatch()
	defer func() { _ = batch.Close() }()
	_, err = stageAndCountEPaxosRecords(batch, rd.Records, s.db.durableRefs)
	if err != nil {
		return err
	}
	compacted, err := stageEPaxosCheckpoint(batch, s.pebble, rd)
	if err != nil {
		return err
	}
	if hardState != nil {
		if err := batch.Set(epaxosHardStateKey, hardState, nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	addDurableRecords(s.db.durableRefs, rd.Records)
	removeDurableRecords(s.db.durableRefs, compacted)
	s.db.durableRecords.Store(uint64(len(s.db.durableRefs)))
	return nil
}

// ApplyReady atomically persists EPAXOS records and applies committed KV commands.
func (db *DB) ApplyReady(rd epaxos.Ready) error {
	if len(rd.Records) == 0 && len(rd.Apply) == 0 && rd.HardState.Empty() &&
		rd.Snapshot == nil && len(rd.Compact) == 0 {
		return nil
	}
	db.durableMu.Lock()
	defer db.durableMu.Unlock()
	if rd.Snapshot != nil {
		switch rd.Snapshot.Mode {
		case epaxos.SnapshotPersistLocal, epaxos.SnapshotInstall:
		default:
			return epaxos.ErrInvalidCheckpoint
		}
		if _, err := db.validateApplicationSnapshot(rd.Snapshot.Checkpoint); err != nil {
			return err
		}
	}

	var compacted []epaxos.InstanceRef
	if len(rd.Records) != 0 || !rd.HardState.Empty() || rd.Snapshot != nil || len(rd.Compact) != 0 {
		hardState, err := prepareEPaxosHardState(db.pebble, rd.HardState)
		if err != nil {
			return err
		}
		batch := db.newWriteBatch()
		defer func() { _ = batch.Close() }()
		if _, err = stageAndCountEPaxosRecords(batch, rd.Records, db.durableRefs); err != nil {
			return err
		}
		compacted, err = stageEPaxosCheckpoint(batch, db.pebble, rd)
		if err != nil {
			return err
		}
		if hardState != nil {
			if err := batch.Set(epaxosHardStateKey, hardState, nil); err != nil {
				return err
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		addDurableRecords(db.durableRefs, rd.Records)
		removeDurableRecords(db.durableRefs, compacted)
		db.durableRecords.Store(uint64(len(db.durableRefs)))
	}

	if rd.Snapshot != nil && rd.Snapshot.Mode == epaxos.SnapshotInstall {
		if err := db.installApplicationSnapshot(rd.Snapshot.Checkpoint); err != nil {
			return err
		}
	}

	for _, command := range rd.Apply {
		if err := db.applyReadyCommand(command); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) applyReadyCommand(command epaxos.ApplyCommand) error {
	id := command.Command.ID
	if id == (epaxos.CommandID{}) {
		return fmt.Errorf("kv: applied command requires a nonzero command ID")
	}
	digest, err := digestEPaxosCommand(command.Command)
	if err != nil {
		return err
	}
	value, closer, err := db.pebble.Get(epaxosCommandResultKey(id))
	if err == nil {
		storedDigest, _, decodeErr := decodeEPaxosCommandResult(value)
		closeErr := closer.Close()
		if decodeErr != nil {
			return decodeErr
		}
		if closeErr != nil {
			return closeErr
		}
		if storedDigest != digest {
			return fmt.Errorf("kv: command ID reused with a different command")
		}
		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	legacyApplied, err := db.appliedReadyCommand(command.Ref)
	if err != nil {
		return err
	}
	var response []byte
	batch := db.newWriteBatch()
	defer func() { _ = batch.Close() }()
	next := db.nextRecordTime()
	advancesTime := false
	if !legacyApplied {
		var handled bool
		response, handled, err = db.executeOrderedRead(command.Command)
		if err != nil {
			return err
		}
		if !handled {
			if _, err := db.stageCommitted(batch, command, &next); err != nil {
				return err
			}
			advancesTime = true
		}
	}
	if err := batch.Set(epaxosCommandResultKey(id), encodeEPaxosCommandResult(digest, response), nil); err != nil {
		return err
	}
	if !command.Ref.IsZero() {
		if err := batch.Set(epaxosAppliedKey(command.Ref), []byte{1}, nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	if advancesTime {
		db.nextTime = next
	}
	return nil
}

// LoadInstance returns one persisted EPaxos record for a Ready.RecordLoads request.
func (db *DB) LoadInstance(ref epaxos.InstanceRef) (epaxos.InstanceRecord, bool, error) {
	return db.EPaxosStorage().LoadInstance(ref)
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
	out[9] = byte(voterCount) //nolint:gosec // voterCount is validated to be between 1 and 7 by validateEPaxosHardState
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

func stageAndCountEPaxosRecords(batch txnBatch, records []epaxos.InstanceRecord, durableRefs map[epaxos.InstanceRef]struct{}) (uint64, error) {
	newRecords := uint64(0)
	var key [26]byte
	encoded := make([]byte, 0, 128)
	for index, rec := range records {
		if !epaxos.VerifyRecordChecksum(rec) {
			return 0, epaxos.ErrChecksumMismatch
		}
		encodeEPaxosStoreRefKey(key[:], epaxosRecordEntry, rec.Ref)
		if _, exists := durableRefs[rec.Ref]; !exists {
			duplicate := false
			for previous := 0; previous < index; previous++ {
				if records[previous].Ref == rec.Ref { //nolint:gosec // previous is bounded by index, which is strictly less than len(records)
					duplicate = true
					break
				}
			}
			if !duplicate {
				newRecords++
			}
		}
		encoded = appendEPaxosRecord(encoded[:0], rec)
		if err := batch.Set(key[:], encoded, nil); err != nil {
			return 0, err
		}
	}
	return newRecords, nil
}

func addDurableRecords(durableRefs map[epaxos.InstanceRef]struct{}, records []epaxos.InstanceRecord) {
	for _, record := range records {
		durableRefs[record.Ref] = struct{}{}
	}
}

func executionFrontierCovers(frontier epaxos.ExecutionFrontier, ref epaxos.InstanceRef) bool {
	return ref.Instance <= executionFrontierThrough(frontier, ref.Conf, ref.Replica)
}

func executionFrontierThrough(frontier epaxos.ExecutionFrontier, conf epaxos.ConfID, replica epaxos.ReplicaID) epaxos.InstanceNum {
	for _, config := range frontier.Configs {
		if config.Conf != conf {
			continue
		}
		for _, lane := range config.Lanes {
			if lane.Replica == replica {
				return lane.Through
			}
		}
	}
	return 0
}

func removeDurableRecords(durableRefs map[epaxos.InstanceRef]struct{}, compacted []epaxos.InstanceRef) {
	for _, ref := range compacted {
		delete(durableRefs, ref)
	}
}

func encodeEPaxosCheckpoint(checkpoint epaxos.Checkpoint) ([]byte, error) {
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, err
	}
	return append([]byte{epaxosCheckpointCodec}, payload...), nil
}

func decodeEPaxosCheckpoint(src []byte) (epaxos.Checkpoint, error) {
	if len(src) < 2 || src[0] != epaxosCheckpointCodec {
		return epaxos.Checkpoint{}, fmt.Errorf("kv: bad epaxos checkpoint version")
	}
	var checkpoint epaxos.Checkpoint
	decoder := json.NewDecoder(bytes.NewReader(src[1:]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return epaxos.Checkpoint{}, fmt.Errorf("kv: malformed epaxos checkpoint: %w", err)
	}
	canonical, err := json.Marshal(checkpoint)
	if err != nil || !bytes.Equal(canonical, src[1:]) || checkpoint.Empty() ||
		checkpoint.Checksum != epaxos.DigestCheckpoint(checkpoint) {
		return epaxos.Checkpoint{}, fmt.Errorf("kv: malformed epaxos checkpoint")
	}
	return checkpoint, nil
}

func stageEPaxosCheckpoint(batch txnBatch, pebbleDB *pebble.DB, rd epaxos.Ready) ([]epaxos.InstanceRef, error) {
	if rd.Snapshot == nil && len(rd.Compact) == 0 {
		return nil, nil
	}
	current, found, err := loadEPaxosCheckpoint(pebbleDB)
	if err != nil {
		return nil, err
	}
	if !found {
		current = epaxos.Checkpoint{}
	}
	if rd.Snapshot != nil {
		current = rd.Snapshot.Checkpoint.Clone()
		if current.Empty() || current.Checksum != epaxos.DigestCheckpoint(current) {
			return nil, epaxos.ErrInvalidCheckpoint
		}
	}
	var compacted []epaxos.InstanceRef
	if len(rd.Compact) > 0 {
		if current.Empty() || current.Certificate.Empty() ||
			current.Certificate.DescriptorDigest != epaxos.DigestCheckpointDescriptor(current.Descriptor) ||
			current.Certificate.Checksum != epaxos.DigestCheckpointCertificate(current.Certificate) {
			return nil, epaxos.ErrInvalidCheckpoint
		}
		expected := make([]epaxos.CompactionRange, 0)
		for _, config := range current.Descriptor.Through.Configs {
			for _, lane := range config.Lanes {
				if lane.Through != 0 {
					expected = append(expected, epaxos.CompactionRange{
						Checkpoint: current.Descriptor.ID,
						Conf:       config.Conf, Replica: lane.Replica, Through: lane.Through,
					})
				}
			}
		}
		if len(expected) != len(rd.Compact) {
			return nil, epaxos.ErrInvalidCheckpoint
		}
		deleter, ok := batch.(interface {
			Delete([]byte, *pebble.WriteOptions) error
		})
		if !ok {
			return nil, fmt.Errorf("kv: write batch does not support compaction")
		}
		for i := range expected {
			if expected[i] != rd.Compact[i] {
				return nil, epaxos.ErrInvalidCheckpoint
			}
			for instance := epaxos.InstanceNum(1); instance <= expected[i].Through; instance++ {
				ref := epaxos.InstanceRef{Conf: expected[i].Conf, Replica: expected[i].Replica, Instance: instance}
				if err := deleter.Delete(epaxosRecordKey(ref), nil); err != nil {
					return nil, err
				}
				compacted = append(compacted, ref)
				if instance == ^epaxos.InstanceNum(0) {
					break
				}
			}
		}
		current.CompactedThrough = current.Descriptor.Through.Clone()
		current.Checksum = epaxos.DigestCheckpoint(current)
	}
	encoded, err := encodeEPaxosCheckpoint(current)
	if err != nil {
		return nil, err
	}
	if err := batch.Set(epaxosCheckpointKey, encoded, nil); err != nil {
		return nil, err
	}
	return compacted, nil
}

func loadEPaxosCheckpoint(pebbleDB *pebble.DB) (epaxos.Checkpoint, bool, error) {
	value, closer, err := pebbleDB.Get(epaxosCheckpointKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return epaxos.Checkpoint{}, false, nil
	}
	if err != nil {
		return epaxos.Checkpoint{}, false, err
	}
	defer func() { _ = closer.Close() }()
	checkpoint, err := decodeEPaxosCheckpoint(value)
	return checkpoint, err == nil, err
}

func epaxosRecordKey(ref epaxos.InstanceRef) []byte {
	out := make([]byte, 26)
	encodeEPaxosStoreRefKey(out, epaxosRecordEntry, ref)
	return out
}

func encodeEPaxosStoreRefKey(out []byte, entry byte, ref epaxos.InstanceRef) {
	out[0] = epaxosStorePrefix
	out[1] = entry
	binary.BigEndian.PutUint64(out[2:10], uint64(ref.Conf))
	binary.BigEndian.PutUint64(out[10:18], uint64(ref.Replica))
	binary.BigEndian.PutUint64(out[18:26], uint64(ref.Instance))
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
	return appendEPaxosRecord(nil, rec)
}

func appendEPaxosRecord(out []byte, rec epaxos.InstanceRecord) []byte {
	payload, err := json.Marshal(rec)
	if err != nil {
		panic(fmt.Sprintf("kv: encode epaxos record: %v", err))
	}
	out = append(out, epaxosRecordCodec)
	return append(out, payload...)
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
	if version == epaxosRecordCodec {
		var rec epaxos.InstanceRecord
		decoder := json.NewDecoder(bytes.NewReader(p.b))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&rec); err != nil {
			return epaxos.InstanceRecord{}, fmt.Errorf("kv: malformed epaxos record: %w", err)
		}
		canonical, err := json.Marshal(rec)
		if err != nil || !bytes.Equal(canonical, p.b) || !epaxos.VerifyRecordChecksum(rec) {
			return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
		}
		return rec, nil
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
	if statusValue > 255 {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos record status")
	}
	rec.Status = epaxos.Status(statusValue) //nolint:gosec // statusValue is verified to fit in uint8
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
	if commandKind > 3 {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos command kind")
	}
	payload := append([]byte(nil), p.bytes()...)
	keys := p.uvarint()
	if keys > 128 {
		return epaxos.InstanceRecord{}, fmt.Errorf("kv: bad epaxos conflict-key count")
	}
	points := make([][]byte, int(keys))
	for i := range points {
		points[i] = append([]byte(nil), p.bytes()...)
	}
	switch commandKind {
	case 0:
		rec.Kind = epaxos.EntryCommand
		rec.Command = epaxos.Command{
			ID:        epaxos.CommandID{Client: commandClient, Sequence: commandSequence},
			Payload:   payload,
			Footprint: epaxos.Footprint{Points: points, All: len(points) == 0},
		}
	case 1:
		rec.Kind = epaxos.EntryNoop
	case 2:
		if len(payload) != 9 {
			return epaxos.InstanceRecord{}, fmt.Errorf("kv: malformed legacy configuration command")
		}
		rec.Kind = epaxos.EntryConfChange
		rec.ConfChange = epaxos.ConfChange{
			Type:    epaxos.ConfChangeType(payload[0]),
			Replica: epaxos.ReplicaID(binary.LittleEndian.Uint64(payload[1:])),
		}
	case 3:
		rec.Kind = epaxos.EntryMembership
		rec.ProtocolControl = payload
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
			case epaxos.TimingDomainTOQ:
				// TimingDomainTOQ permits all ProcessAt/pending combinations; no validation required.
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
	finalizeMigration := func(record epaxos.InstanceRecord) (epaxos.InstanceRecord, error) {
		if record.Kind == epaxos.EntryCommand {
			var canonical epaxos.Footprint
			if err := epaxos.CanonicalizeFootprint(&canonical, record.Command.Footprint); err != nil {
				return epaxos.InstanceRecord{}, err
			}
			record.Command.Footprint = canonical
		}
		record.Checksum = epaxos.ChecksumRecord(record)
		return record, nil
	}
	if version == 8 {
		if epaxos.VerifyRecordChecksumWithoutMembershipResult(rec) {
			return finalizeMigration(rec)
		}
		return epaxos.InstanceRecord{}, epaxos.ErrChecksumMismatch
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
