package kv

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/pebble"
	"gosuda.org/moreconsensus/epaxos"
)

var (
	checkpointStat      = os.Stat
	checkpointMkdirAll  = os.MkdirAll
	checkpointMkdirTemp = os.MkdirTemp
	checkpointRemove    = os.Remove
	checkpointRemoveAll = os.RemoveAll
	checkpointRename    = os.Rename
	checkpointWalkDir   = filepath.WalkDir
	checkpointRel       = filepath.Rel
	checkpointOpen      = os.Open
	checkpointOpenFile  = os.OpenFile
	checkpointSyncFile  = func(file *os.File) error { return file.Sync() }
)

type checkpointIterator interface {
	First() bool
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

var checkpointNewIter = func(db *pebble.DB, opts *pebble.IterOptions) (checkpointIterator, error) {
	return db.NewIter(opts)
}

// Checkpoint writes a flushed Pebble checkpoint for this database.
// The checkpoint is a complete backup candidate for the example KV node and
// includes EPaxos durable records, applied markers, and versioned KV data.
func (db *DB) Checkpoint(path string) error {
	if path == "" {
		return fmt.Errorf("kv: checkpoint path must be non-empty")
	}
	return db.pebble.Checkpoint(path, pebble.WithFlushedWAL())
}

// VerifyCheckpoint opens checkpointDir read-only and checks that persisted
// EPaxos records authenticate, applied markers have committed-or-executed
// user-command source records, executed user records have applied markers, and
// versioned KV rows match the timestamp-ordered effects of the applied EPaxos
// commands. It does not inspect or mutate the live data directory.
func VerifyCheckpoint(checkpointDir string) error {
	if checkpointDir == "" {
		return fmt.Errorf("kv: checkpoint path must be non-empty")
	}
	pebbleDB, err := pebble.Open(checkpointDir, &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
	if err != nil {
		return err
	}
	db := &DB{pebble: pebbleDB, cf: 1}
	defer func() { _ = db.Close() }()
	_, err = verifyCheckpointDB(db)
	return err
}

type checkpointState struct {
	records     map[epaxos.InstanceRef]epaxos.InstanceRecord
	applied     map[epaxos.InstanceRef]struct{}
	writeGroups map[epaxos.InstanceRef]string
}

type checkpointDataGroup struct {
	timestamp uint64
	key       string
}

func verifyCheckpointDB(db *DB) (checkpointState, error) {
	state := checkpointState{
		records:     make(map[epaxos.InstanceRef]epaxos.InstanceRecord),
		applied:     make(map[epaxos.InstanceRef]struct{}),
		writeGroups: make(map[epaxos.InstanceRef]string),
	}
	if err := loadCheckpointRecords(db.pebble, state.records); err != nil {
		return checkpointState{}, err
	}
	actualGroups, actualCounts, err := loadCheckpointDataGroups(db.pebble)
	if err != nil {
		return checkpointState{}, err
	}
	expectedCounts := make(map[string]int)
	candidates := make(map[string][]epaxos.InstanceRef)
	if err := loadCheckpointAppliedMarkers(db.pebble, state, expectedCounts, candidates); err != nil {
		return checkpointState{}, err
	}
	for ref, rec := range state.records {
		if rec.Status == epaxos.StatusExecuted && rec.Command.Kind == epaxos.CommandUser {
			if _, ok := state.applied[ref]; !ok {
				return checkpointState{}, fmt.Errorf("kv: executed user epaxos record %s missing applied marker", ref)
			}
		}
	}
	if !checkpointCountsEqual(actualCounts, expectedCounts) {
		return checkpointState{}, fmt.Errorf("kv: checkpoint data rows do not match applied epaxos commands")
	}
	for key := range candidates {
		sort.Slice(candidates[key], func(i, j int) bool {
			return checkpointRefLess(candidates[key][i], candidates[key][j])
		})
	}
	if err := assignCheckpointTimestamps(actualGroups, candidates, state); err != nil {
		return checkpointState{}, err
	}
	return state, nil
}

func loadCheckpointRecords(pebbleDB *pebble.DB, records map[epaxos.InstanceRef]epaxos.InstanceRecord) error {
	lower := []byte{epaxosStorePrefix, epaxosRecordEntry}
	iter, err := checkpointNewIter(pebbleDB, &pebble.IterOptions{LowerBound: lower, UpperBound: prefixLimit(lower)})
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
		if _, ok := records[rec.Ref]; ok {
			return fmt.Errorf("kv: duplicate epaxos record %s", rec.Ref)
		}
		records[rec.Ref] = rec
	}
	return iter.Error()
}

func loadCheckpointAppliedMarkers(pebbleDB *pebble.DB, state checkpointState, expectedCounts map[string]int, candidates map[string][]epaxos.InstanceRef) error {
	lower := []byte{epaxosStorePrefix, epaxosAppliedEntry}
	iter, err := checkpointNewIter(pebbleDB, &pebble.IterOptions{LowerBound: lower, UpperBound: prefixLimit(lower)})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	for valid := iter.First(); valid; valid = iter.Next() {
		ref, ok := decodeEPaxosAppliedKey(iter.Key())
		if !ok {
			return fmt.Errorf("kv: malformed epaxos applied marker key")
		}
		value := iter.Value()
		if len(value) != 1 || value[0] != 1 {
			return fmt.Errorf("kv: malformed epaxos applied marker %s", ref)
		}
		if _, ok := state.applied[ref]; ok {
			return fmt.Errorf("kv: duplicate epaxos applied marker %s", ref)
		}
		rec, ok := state.records[ref]
		if !ok {
			return fmt.Errorf("kv: applied marker %s has no epaxos record", ref)
		}
		if rec.Status < epaxos.StatusCommitted {
			return fmt.Errorf("kv: applied marker %s references uncommitted epaxos record", ref)
		}
		state.applied[ref] = struct{}{}
		group, writes, err := checkpointCommandGroup(rec.Command)
		if err != nil {
			return fmt.Errorf("kv: applied command %s is invalid: %w", ref, err)
		}
		if writes {
			state.writeGroups[ref] = group
			expectedCounts[group]++
			candidates[group] = append(candidates[group], ref)
		}
	}
	return iter.Error()
}

func loadCheckpointDataGroups(pebbleDB *pebble.DB) ([]checkpointDataGroup, map[string]int, error) {
	lower := []byte{recordPrefix}
	iter, err := checkpointNewIter(pebbleDB, &pebble.IterOptions{LowerBound: lower, UpperBound: prefixLimit(lower)})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = iter.Close() }()
	byTimestamp := make(map[uint64][]string)
	for valid := iter.First(); valid; valid = iter.Next() {
		key, ts, ok := DecodeDataKey(iter.Key(), 1)
		if !ok {
			return nil, nil, fmt.Errorf("kv: malformed data key in checkpoint")
		}
		if err := ValidateKey(key); err != nil {
			return nil, nil, err
		}
		atom, err := checkpointValueAtom(key, iter.Value())
		if err != nil {
			return nil, nil, err
		}
		byTimestamp[ts] = append(byTimestamp[ts], atom)
	}
	if err := iter.Error(); err != nil {
		return nil, nil, err
	}
	timestamps := make([]uint64, 0, len(byTimestamp))
	for ts := range byTimestamp {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
	for i, ts := range timestamps {
		want := uint64(i + 1)
		if ts != want {
			return nil, nil, fmt.Errorf("kv: checkpoint data timestamp %d is not dense position %d", ts, want)
		}
	}
	groups := make([]checkpointDataGroup, 0, len(timestamps))
	counts := make(map[string]int, len(timestamps))
	for _, ts := range timestamps {
		key := checkpointGroupKey(byTimestamp[ts])
		groups = append(groups, checkpointDataGroup{timestamp: ts, key: key})
		counts[key]++
	}
	return groups, counts, nil
}

func checkpointValueAtom(key, value []byte) (string, error) {
	if len(value) == 0 {
		return "", fmt.Errorf("kv: empty data value in checkpoint")
	}
	switch value[0] {
	case valueRecord:
		return checkpointAtomKey(valueRecord, key, value[1:]), nil
	case deleteRecord:
		if len(value) != 1 {
			return "", fmt.Errorf("kv: malformed delete value in checkpoint")
		}
		return checkpointAtomKey(deleteRecord, key, nil), nil
	default:
		return "", fmt.Errorf("kv: unknown data value kind %d in checkpoint", value[0])
	}
}

func checkpointCommandGroup(cmd epaxos.Command) (string, bool, error) {
	if cmd.Kind == epaxos.CommandNoop || len(cmd.Payload) == 0 {
		return "", false, nil
	}
	p := parser{b: cmd.Payload[1:]}
	switch cmd.Payload[0] {
	case opPut:
		key := p.bytes()
		value := p.bytes()
		if p.err || len(p.b) != 0 {
			return "", false, fmt.Errorf("kv: malformed command")
		}
		if err := ValidateKey(key); err != nil {
			return "", false, err
		}
		return checkpointGroupKey([]string{checkpointAtomKey(valueRecord, key, value)}), true, nil
	case opDelete:
		key := p.bytes()
		_ = p.bytes()
		if p.err || len(p.b) != 0 {
			return "", false, fmt.Errorf("kv: malformed command")
		}
		if err := ValidateKey(key); err != nil {
			return "", false, err
		}
		return checkpointGroupKey([]string{checkpointAtomKey(deleteRecord, key, nil)}), true, nil
	case opTxn:
		count := p.uvarint()
		if p.err {
			return "", false, fmt.Errorf("kv: malformed command")
		}
		final := make(map[string]string)
		for i := uint64(0); i < count; i++ {
			op := p.byte()
			key := p.bytes()
			value := p.bytes()
			if p.err {
				return "", false, fmt.Errorf("kv: malformed command")
			}
			if err := ValidateKey(key); err != nil {
				return "", false, err
			}
			switch op {
			case opPut:
				final[string(key)] = checkpointAtomKey(valueRecord, key, value)
			case opDelete:
				final[string(key)] = checkpointAtomKey(deleteRecord, key, nil)
			default:
				return "", false, fmt.Errorf("kv: unknown transaction op %d", op)
			}
		}
		if len(p.b) != 0 {
			return "", false, fmt.Errorf("kv: malformed command")
		}
		if len(final) == 0 {
			return "", false, nil
		}
		atoms := make([]string, 0, len(final))
		for _, atom := range final {
			atoms = append(atoms, atom)
		}
		return checkpointGroupKey(atoms), true, nil
	default:
		return "", false, fmt.Errorf("kv: unknown op %d", cmd.Payload[0])
	}
}

func checkpointAtomKey(kind byte, key, value []byte) string {
	out := make([]byte, 0, 1+8+len(key)+8+len(value))
	out = append(out, kind)
	out = binary.BigEndian.AppendUint64(out, uint64(len(key)))
	out = append(out, key...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(value)))
	out = append(out, value...)
	return string(out)
}

func checkpointGroupKey(atoms []string) string {
	sort.Strings(atoms)
	var size int
	for _, atom := range atoms {
		size += 8 + len(atom)
	}
	out := make([]byte, 0, size)
	for _, atom := range atoms {
		out = binary.BigEndian.AppendUint64(out, uint64(len(atom)))
		out = append(out, atom...)
	}
	return string(out)
}

func checkpointCountsEqual(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func assignCheckpointTimestamps(groups []checkpointDataGroup, candidates map[string][]epaxos.InstanceRef, state checkpointState) error {
	assigned := make(map[epaxos.InstanceRef]uint64, len(groups))
	used := make(map[epaxos.InstanceRef]struct{}, len(groups))
	for _, group := range groups {
		refs := candidates[group.key]
		selected := -1
		for i, ref := range refs {
			if _, ok := used[ref]; ok {
				continue
			}
			if checkpointDependenciesAssigned(state.records[ref], assigned, state.writeGroups) {
				selected = i
				break
			}
		}
		if selected < 0 {
			return fmt.Errorf("kv: checkpoint timestamp %d has no dependency-satisfied applied command", group.timestamp)
		}
		ref := refs[selected]
		used[ref] = struct{}{}
		assigned[ref] = group.timestamp
	}
	if len(used) != len(state.writeGroups) {
		return fmt.Errorf("kv: checkpoint applied command count does not match data timestamps")
	}
	return nil
}

func checkpointDependenciesAssigned(rec epaxos.InstanceRecord, assigned map[epaxos.InstanceRef]uint64, writeGroups map[epaxos.InstanceRef]string) bool {
	for i, dep := range rec.Deps {
		if dep == 0 {
			continue
		}
		replica := epaxos.ReplicaID(i + 1)
		for instance := epaxos.InstanceNum(1); instance <= dep; instance++ {
			depRef := epaxos.InstanceRef{Replica: replica, Instance: instance, Conf: rec.Ref.Conf}
			if depRef == rec.Ref {
				continue
			}
			if _, writes := writeGroups[depRef]; !writes {
				continue
			}
			if _, ok := assigned[depRef]; !ok {
				return false
			}
		}
	}
	return true
}

func checkpointRefLess(left, right epaxos.InstanceRef) bool {
	if left.Conf != right.Conf {
		return left.Conf < right.Conf
	}
	if left.Replica != right.Replica {
		return left.Replica < right.Replica
	}
	return left.Instance < right.Instance
}

// RepairFromCheckpoint authenticates checkpointDir's persisted EPaxos records
// and then atomically replaces dataDir using RestoreCheckpoint. This is an
// explicit offline operator repair path: normal Open/OpenCluster still fail
// closed on corrupt live EPaxos records, and this function never recomputes
// checksums or deletes individual records from the corrupt directory.
func RepairFromCheckpoint(dataDir, checkpointDir string) error {
	if err := VerifyCheckpoint(checkpointDir); err != nil {
		return fmt.Errorf("kv: checkpoint verification failed: %w", err)
	}
	return RestoreCheckpoint(dataDir, checkpointDir)
}

// RestoreCheckpoint replaces dataDir with a copy of checkpointDir.
// Callers must stop the KV node and close any DB already using dataDir before
// calling RestoreCheckpoint. The old data directory is first moved aside so a
// failed final rename can roll it back. RestoreCheckpoint only copies bytes; use
// RepairFromCheckpoint when the checkpoint must be verified before repair.
func RestoreCheckpoint(dataDir, checkpointDir string) error {
	if dataDir == "" || checkpointDir == "" {
		return fmt.Errorf("kv: data and checkpoint paths must be non-empty")
	}
	cleanData := filepath.Clean(dataDir)
	if cleanData == "." || cleanData == string(os.PathSeparator) {
		return fmt.Errorf("kv: refusing to replace broad data directory %q", dataDir)
	}
	checkpointInfo, err := checkpointStat(checkpointDir)
	if err != nil {
		return err
	}
	if !checkpointInfo.IsDir() {
		return fmt.Errorf("kv: checkpoint path %q is not a directory", checkpointDir)
	}
	parent := filepath.Dir(cleanData)
	if err := checkpointMkdirAll(parent, 0o700); err != nil {
		return err
	}
	tmp, err := checkpointMkdirTemp(parent, ".restore-*")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = checkpointRemoveAll(tmp)
		}
	}()
	if err := copyCheckpointDir(checkpointDir, tmp); err != nil {
		return err
	}
	backup, err := checkpointMkdirTemp(parent, filepath.Base(cleanData)+".pre-restore-*")
	if err != nil {
		return err
	}
	if err := checkpointRemove(backup); err != nil {
		return err
	}
	oldMoved := false
	if _, err := checkpointStat(cleanData); err == nil {
		if err := checkpointRename(cleanData, backup); err != nil {
			return err
		}
		oldMoved = true
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := checkpointRename(tmp, cleanData); err != nil {
		if oldMoved {
			if rollbackErr := checkpointRename(backup, cleanData); rollbackErr != nil {
				return fmt.Errorf("kv: restore rename failed: %v; rollback failed: %w", err, rollbackErr)
			}
		}
		return err
	}
	committed = true
	if oldMoved {
		return checkpointRemoveAll(backup)
	}
	return nil
}

func copyCheckpointDir(src, dst string) error {
	return checkpointWalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := checkpointRel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		out := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(out, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("kv: checkpoint entry %q is not a regular file", path)
		}
		return copyCheckpointFile(path, out, info.Mode().Perm())
	})
}

func copyCheckpointFile(src, dst string, mode fs.FileMode) error {
	in, err := checkpointOpen(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := checkpointOpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := checkpointSyncFile(out); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
