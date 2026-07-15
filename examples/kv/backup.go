package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"
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

const (
	checkpointManifestName                = "CHECKPOINT-MANIFEST"
	checkpointManifestVersion        byte = 1
	checkpointManifestMaxSize             = 64 << 20
	checkpointManifestMaxFiles            = 1 << 20
	checkpointManifestMaxField            = 4096
	checkpointManifestChecksumSize        = 32
	checkpointManifestDomain              = "gosuda.org/moreconsensus/examples/kv/checkpoint-manifest/v1"
	checkpointManifestIdentityDomain      = "gosuda.org/moreconsensus/examples/kv/checkpoint-identity/v1"
	checkpointFileChecksumDomain          = "gosuda.org/moreconsensus/examples/kv/checkpoint-file/v1"
	checkpointSemanticDigestDomain        = "gosuda.org/moreconsensus/examples/kv/checkpoint-semantic/v1"
	checkpointHardStateDigestDomain       = "gosuda.org/moreconsensus/examples/kv/checkpoint-hard-state/v1"
)

var checkpointManifestMagic = [8]byte{'K', 'V', 'C', 'P', 'M', 'F', 'S', 'T'}

// CheckpointMetadata binds optional source and release identity to a manifest.
type CheckpointMetadata struct {
	SourceIdentity  string
	ReleaseChecksum string
}

// CheckpointInfo is authenticated by a current checkpoint manifest. Legacy
// verification returns Format "legacy" and CurrentTarget false.
type CheckpointInfo struct {
	Format              string
	ManifestVersion     uint8
	ManifestIdentity    string
	SemanticStateDigest string
	RecordCount         uint64
	AppliedCount        uint64
	HardStateDigest     string
	SourceIdentity      string
	ReleaseChecksum     string
	CurrentTarget       bool
}

type checkpointManifest struct {
	sourceIdentity  string
	releaseChecksum string
	semanticDigest  [32]byte
	recordCount     uint64
	appliedCount    uint64
	hardStateDigest [32]byte
	files           []checkpointManifestFile
	checksum        [checkpointManifestChecksumSize]byte
}

type checkpointManifestFile struct {
	path   string
	size   uint64
	digest [32]byte
}

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

// Checkpoint writes a flushed Pebble checkpoint and deterministic manifest.
func (db *DB) Checkpoint(checkpointDir string) error {
	_, err := db.CheckpointWithMetadata(checkpointDir, CheckpointMetadata{})
	return err
}

// CheckpointWithMetadata writes a current checkpoint whose manifest binds all
// regular Pebble files, semantic state, durable hard state, and source inputs.
func (db *DB) CheckpointWithMetadata(checkpointDir string, metadata CheckpointMetadata) (CheckpointInfo, error) {
	if checkpointDir == "" {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint path must be non-empty")
	}
	if err := validateCheckpointMetadata(metadata); err != nil {
		return CheckpointInfo{}, err
	}
	if err := db.pebble.Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
		return CheckpointInfo{}, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = checkpointRemoveAll(checkpointDir)
		}
	}()
	state, err := verifyCheckpointSemantic(checkpointDir, true)
	if err != nil {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint semantic verification failed: %w", err)
	}
	files, err := collectCheckpointFiles(checkpointDir, true)
	if err != nil {
		return CheckpointInfo{}, err
	}
	manifest := checkpointManifest{
		sourceIdentity:  metadata.SourceIdentity,
		releaseChecksum: metadata.ReleaseChecksum,
		semanticDigest:  state.semanticDigest,
		recordCount:     uint64(len(state.records)),
		appliedCount:    uint64(len(state.applied)),
		hardStateDigest: state.hardStateDigest,
		files:           files,
	}
	encoded := encodeCheckpointManifest(manifest)
	if err := writeCheckpointManifest(checkpointDir, encoded); err != nil {
		return CheckpointInfo{}, err
	}
	info, err := VerifyCheckpointWithInfo(checkpointDir)
	if err != nil {
		return CheckpointInfo{}, fmt.Errorf("kv: created checkpoint failed verification: %w", err)
	}
	complete = true
	return info, nil
}

// VerifyCheckpoint verifies a current manifest-bound checkpoint. Legacy
// checkpoints are accepted only by VerifyLegacyCheckpoint.
func VerifyCheckpoint(checkpointDir string) error {
	_, err := VerifyCheckpointWithInfo(checkpointDir)
	return err
}

// VerifyCheckpointWithInfo verifies a current checkpoint and returns its
// immutable manifest identity and authenticated semantic summary.
func VerifyCheckpointWithInfo(checkpointDir string) (CheckpointInfo, error) {
	if checkpointDir == "" {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint path must be non-empty")
	}
	manifest, encoded, err := readCheckpointManifest(checkpointDir)
	if err != nil {
		return CheckpointInfo{}, err
	}
	if err := validateCheckpointTree(checkpointDir); err != nil {
		return CheckpointInfo{}, err
	}
	state, err := verifyCheckpointSemantic(checkpointDir, true)
	if err != nil {
		return CheckpointInfo{}, err
	}
	if manifest.hardStateDigest != state.hardStateDigest {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint hard-state digest mismatch")
	}
	if manifest.semanticDigest != state.semanticDigest {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint semantic state digest mismatch")
	}
	if manifest.recordCount != uint64(len(state.records)) {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint record count mismatch: manifest=%d actual=%d", manifest.recordCount, len(state.records))
	}
	if manifest.appliedCount != uint64(len(state.applied)) {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint applied-marker count mismatch: manifest=%d actual=%d", manifest.appliedCount, len(state.applied))
	}
	files, err := collectCheckpointFiles(checkpointDir, true)
	if err != nil {
		return CheckpointInfo{}, err
	}
	if err := compareCheckpointFiles(manifest.files, files); err != nil {
		return CheckpointInfo{}, err
	}
	identity := checkpointDigest(checkpointManifestIdentityDomain, encoded)
	return checkpointInfoFromState(manifest, state, hex.EncodeToString(identity[:]), true), nil
}

// VerifyLegacyCheckpoint performs the old semantic read without making a
// current-target or manifest-authentication claim.
func VerifyLegacyCheckpoint(checkpointDir string) error {
	_, err := VerifyLegacyCheckpointWithInfo(checkpointDir)
	return err
}

// VerifyLegacyCheckpointWithInfo explicitly verifies a checkpoint that predates
// the durable hard-state and manifest contract.
func VerifyLegacyCheckpointWithInfo(checkpointDir string) (CheckpointInfo, error) {
	if checkpointDir == "" {
		return CheckpointInfo{}, fmt.Errorf("kv: checkpoint path must be non-empty")
	}
	manifestPath := filepath.Join(checkpointDir, checkpointManifestName)
	if _, err := os.Lstat(manifestPath); err == nil {
		return CheckpointInfo{}, fmt.Errorf("kv: legacy verification refuses a current checkpoint manifest")
	} else if !os.IsNotExist(err) {
		return CheckpointInfo{}, err
	}
	if err := validateCheckpointTree(checkpointDir); err != nil {
		return CheckpointInfo{}, err
	}
	state, err := verifyCheckpointSemantic(checkpointDir, false)
	if err != nil {
		return CheckpointInfo{}, err
	}
	hardStateDigest := ""
	if state.hardStateFound {
		hardStateDigest = hex.EncodeToString(state.hardStateDigest[:])
	}
	return CheckpointInfo{
		Format:              "legacy",
		SemanticStateDigest: hex.EncodeToString(state.semanticDigest[:]),
		RecordCount:         uint64(len(state.records)),
		AppliedCount:        uint64(len(state.applied)),
		HardStateDigest:     hardStateDigest,
		CurrentTarget:       false,
	}, nil
}

type checkpointState struct {
	records         map[epaxos.InstanceRef]epaxos.InstanceRecord
	applied         map[epaxos.InstanceRef]struct{}
	writeGroups     map[epaxos.InstanceRef]string
	hardState       epaxos.HardState
	hardStateFound  bool
	hardStateDigest [32]byte
	semanticDigest  [32]byte
	configHistory   map[epaxos.ConfID]epaxos.ConfState
}

type checkpointDataGroup struct {
	timestamp uint64
	key       string
}

func verifyCheckpointSemantic(checkpointDir string, requireHardState bool) (checkpointState, error) {
	pebbleDB, err := pebble.Open(checkpointDir, &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
	if err != nil {
		return checkpointState{}, err
	}
	db := &DB{pebble: pebbleDB, cf: 1}
	state, verifyErr := verifyCheckpointDBMode(db, requireHardState)
	closeErr := db.Close()
	if verifyErr != nil {
		return checkpointState{}, verifyErr
	}
	if closeErr != nil {
		return checkpointState{}, closeErr
	}
	return state, nil
}

func verifyCheckpointDB(db *DB) (checkpointState, error) {
	return verifyCheckpointDBMode(db, true)
}

func verifyCheckpointDBMode(db *DB, requireHardState bool) (checkpointState, error) {
	state := checkpointState{
		records:     make(map[epaxos.InstanceRef]epaxos.InstanceRecord),
		applied:     make(map[epaxos.InstanceRef]struct{}),
		writeGroups: make(map[epaxos.InstanceRef]string),
	}
	hardState, encodedHardState, found, err := loadCheckpointHardState(db.pebble)
	if err != nil {
		return checkpointState{}, err
	}
	if requireHardState && !found {
		return checkpointState{}, fmt.Errorf("kv: current checkpoint omitted epaxos hard state")
	}
	state.hardState = hardState
	state.hardStateFound = found
	if found {
		state.hardStateDigest = checkpointDigest(checkpointHardStateDigestDomain, encodedHardState)
	}
	if err := loadCheckpointRecords(db.pebble, state.records); err != nil {
		return checkpointState{}, err
	}
	if found {
		if err := verifyCheckpointConfigurationHistory(&state); err != nil {
			return checkpointState{}, err
		}
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
		if rec.Status == epaxos.StatusExecuted && rec.Kind == epaxos.EntryCommand {
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
	state.semanticDigest, err = checkpointDatabaseDigest(db.pebble)
	if err != nil {
		return checkpointState{}, err
	}
	return state, nil
}

func loadCheckpointHardState(pebbleDB *pebble.DB) (epaxos.HardState, []byte, bool, error) {
	lower := []byte{epaxosStorePrefix, epaxosHardStateEntry}
	iter, err := checkpointNewIter(pebbleDB, &pebble.IterOptions{LowerBound: lower, UpperBound: prefixLimit(lower)})
	if err != nil {
		return epaxos.HardState{}, nil, false, err
	}
	defer func() { _ = iter.Close() }()
	var encoded []byte
	for valid := iter.First(); valid; valid = iter.Next() {
		if !bytes.Equal(iter.Key(), epaxosHardStateKey) {
			return epaxos.HardState{}, nil, false, fmt.Errorf("kv: malformed singleton epaxos hard-state key")
		}
		if encoded != nil {
			return epaxos.HardState{}, nil, false, fmt.Errorf("kv: duplicate singleton epaxos hard state")
		}
		encoded = append([]byte(nil), iter.Value()...)
	}
	if err := iter.Error(); err != nil {
		return epaxos.HardState{}, nil, false, err
	}
	if encoded == nil {
		return epaxos.HardState{}, nil, false, nil
	}
	hardState, err := decodeEPaxosHardState(encoded)
	if err != nil {
		return epaxos.HardState{}, nil, false, err
	}
	return hardState, encoded, true, nil
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
		if rec.Kind != epaxos.EntryCommand {
			return fmt.Errorf("kv: applied marker %s references a non-user epaxos record", ref)
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
	if len(cmd.Payload) == 0 {
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
	order := checkpointDependencyOrderForState(state)
	used := make(map[epaxos.InstanceRef]struct{}, len(groups))
	for _, group := range groups {
		refs := candidates[group.key]
		selected := -1
		for i, ref := range refs {
			if _, ok := used[ref]; ok {
				continue
			}
			if checkpointDependenciesAssignedForStateWithOrder(state.records[ref], assigned, state, order) {
				selected = i
				break
			}
		}
		if selected < 0 {
			return fmt.Errorf("kv: checkpoint timestamp %d has no dependency-satisfied applied command among candidates %v", group.timestamp, refs)
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

func checkpointDependenciesAssignedForState(rec epaxos.InstanceRecord, assigned map[epaxos.InstanceRef]uint64, state checkpointState) bool {
	return checkpointDependenciesAssignedForStateWithOrder(rec, assigned, state, nil)
}

type checkpointDependencyOrder struct {
	component    map[epaxos.InstanceRef]int
	dependencies map[int]map[int]struct{}
}

func checkpointDependenciesAssignedForStateWithOrder(rec epaxos.InstanceRecord, assigned map[epaxos.InstanceRef]uint64, state checkpointState, order *checkpointDependencyOrder) bool {
	conf, known := state.configHistory[rec.Ref.Conf]
	if known {
		for i := len(conf.Voters); i < len(rec.Deps); i++ {
			if rec.Deps[i] != 0 {
				return false
			}
		}
	}
	for depRef := range state.writeGroups {
		if depRef == rec.Ref {
			continue
		}
		required := checkpointRecordDependsOn(rec, depRef, state)
		if order != nil {
			component, currentKnown := order.component[rec.Ref]
			dependencyComponent, dependencyKnown := order.component[depRef]
			if currentKnown && dependencyKnown {
				switch component {
				case dependencyComponent:
					required = checkpointExecutionLess(state.records[depRef], rec)
				default:
					_, required = order.dependencies[component][dependencyComponent]
				}
			}
		}
		if !required {
			continue
		}
		if _, ok := assigned[depRef]; !ok {
			return false
		}
	}
	return true
}

func checkpointRecordDependsOn(rec epaxos.InstanceRecord, depRef epaxos.InstanceRef, state checkpointState) bool {
	if depRef == rec.Ref || depRef.Conf != rec.Ref.Conf || depRef.Replica == 0 || depRef.Instance == 0 {
		return false
	}
	if conf, known := state.configHistory[rec.Ref.Conf]; known {
		slot, ok := conf.Index(depRef.Replica)
		return ok && slot < len(rec.Deps) && depRef.Instance <= rec.Deps[slot]
	}
	slot := uint64(depRef.Replica - 1)
	return slot < uint64(len(rec.Deps)) && depRef.Instance <= rec.Deps[slot]
}

func checkpointExecutionLess(left, right epaxos.InstanceRecord) bool {
	if left.Seq != right.Seq {
		return left.Seq < right.Seq
	}
	return checkpointRefLess(left.Ref, right.Ref)
}

func checkpointDependencyOrderForState(state checkpointState) *checkpointDependencyOrder {
	refs := make([]epaxos.InstanceRef, 0, len(state.records))
	for ref, rec := range state.records {
		_, applied := state.applied[ref]
		if rec.Status == epaxos.StatusExecuted || applied {
			refs = append(refs, ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return checkpointRefLess(refs[i], refs[j]) })
	indices := make(map[epaxos.InstanceRef]int, len(refs))
	lowlinks := make(map[epaxos.InstanceRef]int, len(refs))
	onStack := make(map[epaxos.InstanceRef]bool, len(refs))
	stack := make([]epaxos.InstanceRef, 0, len(refs))
	components := make(map[epaxos.InstanceRef]int, len(refs))
	nextIndex := 0
	nextComponent := 0
	var visit func(epaxos.InstanceRef)
	visit = func(ref epaxos.InstanceRef) {
		nextIndex++
		indices[ref] = nextIndex
		lowlinks[ref] = nextIndex
		stack = append(stack, ref)
		onStack[ref] = true
		rec := state.records[ref]
		for _, dependency := range refs {
			if !checkpointRecordDependsOn(rec, dependency, state) {
				continue
			}
			if indices[dependency] == 0 {
				visit(dependency)
				if lowlinks[dependency] < lowlinks[ref] {
					lowlinks[ref] = lowlinks[dependency]
				}
			} else if onStack[dependency] && indices[dependency] < lowlinks[ref] {
				lowlinks[ref] = indices[dependency]
			}
		}
		if lowlinks[ref] != indices[ref] {
			return
		}
		nextComponent++
		for {
			last := len(stack) - 1
			member := stack[last]
			stack = stack[:last]
			onStack[member] = false
			components[member] = nextComponent
			if member == ref {
				return
			}
		}
	}
	for _, ref := range refs {
		if indices[ref] == 0 {
			visit(ref)
		}
	}

	direct := make(map[int]map[int]struct{}, nextComponent)
	for _, ref := range refs {
		component := components[ref]
		for _, dependency := range refs {
			dependencyComponent := components[dependency]
			if component == dependencyComponent || !checkpointRecordDependsOn(state.records[ref], dependency, state) {
				continue
			}
			if direct[component] == nil {
				direct[component] = make(map[int]struct{})
			}
			direct[component][dependencyComponent] = struct{}{}
		}
	}
	dependencies := make(map[int]map[int]struct{}, nextComponent)
	for component := 1; component <= nextComponent; component++ {
		reachable := make(map[int]struct{})
		var walk func(int)
		walk = func(current int) {
			for dependency := range direct[current] {
				if _, seen := reachable[dependency]; seen {
					continue
				}
				reachable[dependency] = struct{}{}
				walk(dependency)
			}
		}
		walk(component)
		dependencies[component] = reachable
	}
	return &checkpointDependencyOrder{component: components, dependencies: dependencies}
}

func checkpointDependenciesAssigned(rec epaxos.InstanceRecord, assigned map[epaxos.InstanceRef]uint64, writeGroups map[epaxos.InstanceRef]string) bool {
	for depRef := range writeGroups {
		if depRef == rec.Ref || depRef.Conf != rec.Ref.Conf || depRef.Replica == 0 || depRef.Instance == 0 {
			continue
		}
		slot := uint64(depRef.Replica - 1)
		if slot >= uint64(len(rec.Deps)) || depRef.Instance > rec.Deps[slot] {
			continue
		}
		if _, ok := assigned[depRef]; !ok {
			return false
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

func verifyCheckpointConfigurationHistory(state *checkpointState) error {
	current := state.hardState.Conf
	if uint64(current.ID) > uint64(len(state.records))+1 {
		return fmt.Errorf("%w: current hard-state configuration %d skips causal applied outcomes", epaxos.ErrInvalidConfig, current.ID)
	}
	applied := make(map[epaxos.ConfID]epaxos.InstanceRecord)
	for _, rec := range state.records {
		result := rec.ConfChangeResult
		if rec.Kind != epaxos.EntryConfChange || rec.Status != epaxos.StatusExecuted {
			if result.Outcome != epaxos.ConfChangeOutcomeUnspecified || !checkpointConfStateZero(result.Conf) {
				return fmt.Errorf("%w: durable record %s has a configuration outcome outside an executed configuration command", epaxos.ErrInvalidConfig, rec.Ref)
			}
			continue
		}
		if result.Outcome == epaxos.ConfChangeOutcomeUnspecified {
			return fmt.Errorf("%w: executed configuration record %s omitted its causal outcome", epaxos.ErrInvalidConfig, rec.Ref)
		}
		if result.Outcome != epaxos.ConfChangeApplied {
			continue
		}
		if rec.Ref.Conf == ^epaxos.ConfID(0) || result.Conf.ID != rec.Ref.Conf+1 {
			return fmt.Errorf("%w: applied configuration record %s has invalid successor id", epaxos.ErrInvalidConfig, rec.Ref)
		}
		if err := validateEPaxosHardState(epaxos.HardState{Conf: result.Conf}); err != nil {
			return err
		}
		if _, duplicate := applied[rec.Ref.Conf]; duplicate {
			return fmt.Errorf("%w: multiple applied configuration outcomes for base %d", epaxos.ErrInvalidConfig, rec.Ref.Conf)
		}
		applied[rec.Ref.Conf] = rec
	}

	history := make(map[epaxos.ConfID]epaxos.ConfState)
	history[current.ID] = current.Clone()
	for successorID := current.ID; successorID > 1; successorID-- {
		baseID := successorID - 1
		rec, ok := applied[baseID]
		if !ok {
			return fmt.Errorf("%w: current hard-state configuration %d skips causal applied outcome from %d", epaxos.ErrInvalidConfig, current.ID, baseID)
		}
		successor := history[successorID]
		if !checkpointSameConf(rec.ConfChangeResult.Conf, successor) {
			return fmt.Errorf("%w: applied configuration outcome for %s conflicts with known successor %d", epaxos.ErrInvalidConfig, rec.Ref, successorID)
		}
		base, ok := checkpointAppliedPredecessor(rec, successor)
		if !ok {
			return fmt.Errorf("%w: applied configuration outcome for %s is incompatible with its command", epaxos.ErrInvalidConfig, rec.Ref)
		}
		history[baseID] = base
	}
	for baseID, rec := range applied {
		if baseID >= current.ID || history[baseID].ID == 0 {
			return fmt.Errorf("%w: current hard-state configuration %d regresses behind applied outcome %s", epaxos.ErrInvalidConfig, current.ID, rec.Ref)
		}
	}

	for _, rec := range state.records {
		conf, known := history[rec.Ref.Conf]
		if !known {
			return fmt.Errorf("%w: durable record %s references unknown configuration", epaxos.ErrInvalidConfig, rec.Ref)
		}
		if rec.Ref.Replica == 0 || rec.Ref.Instance == 0 || !conf.Contains(rec.Ref.Replica) {
			return fmt.Errorf("%w: durable record %s is not owned by its pinned configuration", epaxos.ErrInvalidConfig, rec.Ref)
		}
		if rec.Kind < epaxos.EntryCommand || rec.Kind > epaxos.EntryCheckpoint {
			return fmt.Errorf("%w: durable record %s has unknown entry kind %d", epaxos.ErrInvalidConfig, rec.Ref, rec.Kind)
		}
		if len(rec.Deps) != len(conf.Voters) {
			return fmt.Errorf("%w: durable record %s has dependency width %d for %d voters", epaxos.ErrInvalidConfig, rec.Ref, len(rec.Deps), len(conf.Voters))
		}
		if rec.Status > epaxos.StatusExecuted {
			return fmt.Errorf("%w: durable record %s has unknown status %d", epaxos.ErrInvalidConfig, rec.Ref, rec.Status)
		}
		if rec.Status >= epaxos.StatusPreAccepted && rec.Seq == 0 {
			return fmt.Errorf("%w: durable record %s has zero sequence with a durable value", epaxos.ErrInvalidConfig, rec.Ref)
		}
		if !checkpointBallotCompatible(rec.Ballot, conf, rec.Status == epaxos.StatusNone) ||
			!checkpointBallotCompatible(rec.RecordBallot, conf, rec.Status < epaxos.StatusPreAccepted) {
			return fmt.Errorf("%w: durable record %s has a ballot outside its pinned configuration", epaxos.ErrInvalidConfig, rec.Ref)
		}
		if (rec.AcceptSeq == 0) != (len(rec.AcceptDeps) == 0) ||
			(len(rec.AcceptDeps) != 0 && len(rec.AcceptDeps) != len(conf.Voters)) {
			return fmt.Errorf("%w: durable record %s has invalid accept dependency metadata", epaxos.ErrInvalidConfig, rec.Ref)
		}
		if rec.Status < epaxos.StatusAccepted && (rec.AcceptSeq != 0 || len(rec.AcceptEvidence) != 0) {
			return fmt.Errorf("%w: durable record %s has accept evidence before accepted status", epaxos.ErrInvalidConfig, rec.Ref)
		}
		seenEvidence := make(map[epaxos.ReplicaID]struct{}, len(rec.AcceptEvidence))
		for _, evidence := range rec.AcceptEvidence {
			if evidence.Sender == 0 || !conf.Contains(evidence.Sender) || evidence.Seq == 0 || len(evidence.Deps) != len(conf.Voters) {
				return fmt.Errorf("%w: durable record %s has accept evidence outside its pinned configuration", epaxos.ErrInvalidConfig, rec.Ref)
			}
			if _, duplicate := seenEvidence[evidence.Sender]; duplicate {
				return fmt.Errorf("%w: durable record %s has duplicate accept evidence sender %d", epaxos.ErrInvalidConfig, rec.Ref, evidence.Sender)
			}
			seenEvidence[evidence.Sender] = struct{}{}
		}
		if err := verifyCheckpointConfigRecord(rec, conf, applied); err != nil {
			return err
		}
	}
	state.configHistory = history
	return nil
}

func verifyCheckpointConfigRecord(rec epaxos.InstanceRecord, base epaxos.ConfState, applied map[epaxos.ConfID]epaxos.InstanceRecord) error {
	if rec.Kind != epaxos.EntryConfChange {
		return nil
	}
	successor, valid := checkpointConfigSuccessor(base, rec.ConfChange)
	result := rec.ConfChangeResult
	if rec.Status != epaxos.StatusExecuted {
		if !valid {
			return fmt.Errorf("%w: durable configuration record %s has an invalid command", epaxos.ErrInvalidConfig, rec.Ref)
		}
		return nil
	}
	switch result.Outcome {
	case epaxos.ConfChangeOutcomeUnspecified:
		return fmt.Errorf("%w: configuration record %s has unspecified outcome", epaxos.ErrInvalidConfig, rec.Ref)
	case epaxos.ConfChangeApplied:
		if !valid || !checkpointSameConf(successor, result.Conf) {
			return fmt.Errorf("%w: applied configuration outcome for %s does not match its command", epaxos.ErrInvalidConfig, rec.Ref)
		}
	case epaxos.ConfChangeRejectedInvalid:
		if valid || !checkpointConfStateZero(result.Conf) {
			return fmt.Errorf("%w: rejected-invalid configuration outcome for %s is inconsistent", epaxos.ErrInvalidConfig, rec.Ref)
		}
	case epaxos.ConfChangeRejectedSuperseded:
		winner, exists := applied[rec.Ref.Conf]
		if !valid || !checkpointConfStateZero(result.Conf) || !exists || winner.Ref == rec.Ref {
			return fmt.Errorf("%w: superseded configuration outcome for %s has no compatible applied successor", epaxos.ErrInvalidConfig, rec.Ref)
		}
	default:
		return fmt.Errorf("%w: configuration record %s has unknown outcome %d", epaxos.ErrInvalidConfig, rec.Ref, result.Outcome)
	}
	return nil
}

func checkpointAppliedPredecessor(rec epaxos.InstanceRecord, successor epaxos.ConfState) (epaxos.ConfState, bool) {
	changeType, replica, ok := checkpointDecodeConfChange(rec.ConfChange)
	if !ok {
		return epaxos.ConfState{}, false
	}
	base := epaxos.ConfState{ID: rec.Ref.Conf, Voters: append([]epaxos.ReplicaID(nil), successor.Voters...)}
	index := sort.Search(len(base.Voters), func(i int) bool { return base.Voters[i] >= replica })
	switch changeType {
	case epaxos.ConfChangeAddVoter:
		if index >= len(base.Voters) || base.Voters[index] != replica {
			return epaxos.ConfState{}, false
		}
		base.Voters = append(base.Voters[:index], base.Voters[index+1:]...)
	case epaxos.ConfChangeRemoveVoter:
		if index < len(base.Voters) && base.Voters[index] == replica {
			return epaxos.ConfState{}, false
		}
		base.Voters = append(base.Voters, 0)
		copy(base.Voters[index+1:], base.Voters[index:])
		base.Voters[index] = replica
	default:
		return epaxos.ConfState{}, false
	}
	if err := validateEPaxosHardState(epaxos.HardState{Conf: base}); err != nil {
		return epaxos.ConfState{}, false
	}
	replayed, valid := checkpointConfigSuccessor(base, rec.ConfChange)
	return base, valid && checkpointSameConf(replayed, successor)
}

func checkpointConfigSuccessor(base epaxos.ConfState, change epaxos.ConfChange) (epaxos.ConfState, bool) {
	changeType, replica, ok := checkpointDecodeConfChange(change)
	if !ok || replica == 0 {
		return epaxos.ConfState{}, false
	}
	voters := append([]epaxos.ReplicaID(nil), base.Voters...)
	index := sort.Search(len(voters), func(i int) bool { return voters[i] >= replica })
	switch changeType {
	case epaxos.ConfChangeAddVoter:
		if index < len(voters) && voters[index] == replica {
			return epaxos.ConfState{}, false
		}
		voters = append(voters, 0)
		copy(voters[index+1:], voters[index:])
		voters[index] = replica
	case epaxos.ConfChangeRemoveVoter:
		if index >= len(voters) || voters[index] != replica {
			return epaxos.ConfState{}, false
		}
		voters = append(voters[:index], voters[index+1:]...)
	default:
		return epaxos.ConfState{}, false
	}
	successor := epaxos.ConfState{ID: base.ID + 1, Voters: voters}
	if err := validateEPaxosHardState(epaxos.HardState{Conf: successor}); err != nil {
		return epaxos.ConfState{}, false
	}
	return successor, true
}

func checkpointDecodeConfChange(change epaxos.ConfChange) (epaxos.ConfChangeType, epaxos.ReplicaID, bool) {
	if change.Type != epaxos.ConfChangeAddVoter && change.Type != epaxos.ConfChangeRemoveVoter || change.Replica == 0 {
		return 0, 0, false
	}
	return change.Type, change.Replica, true
}

func checkpointBallotCompatible(ballot epaxos.Ballot, conf epaxos.ConfState, allowZero bool) bool {
	if ballot == (epaxos.Ballot{}) {
		return allowZero
	}
	return ballot.Replica != 0 && conf.Contains(ballot.Replica)
}

func checkpointConfStateZero(conf epaxos.ConfState) bool {
	return conf.ID == 0 && len(conf.Voters) == 0
}

func checkpointSameConf(left, right epaxos.ConfState) bool {
	return left.ID == right.ID && sameEPaxosVoters(left.Voters, right.Voters)
}

func validateCheckpointMetadata(metadata CheckpointMetadata) error {
	if err := validateCheckpointMetadataField("source identity", metadata.SourceIdentity); err != nil {
		return err
	}
	return validateCheckpointMetadataField("release checksum", metadata.ReleaseChecksum)
}

func validateCheckpointMetadataField(name, value string) error {
	if len(value) > checkpointManifestMaxField || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("kv: invalid checkpoint %s", name)
	}
	return nil
}

func checkpointDatabaseDigest(pebbleDB *pebble.DB) ([32]byte, error) {
	hasher := blake3.NewDeriveKey(checkpointSemanticDigestDomain)
	iter, err := checkpointNewIter(pebbleDB, &pebble.IterOptions{})
	if err != nil {
		return [32]byte{}, err
	}
	defer func() { _ = iter.Close() }()
	var length [8]byte
	for valid := iter.First(); valid; valid = iter.Next() {
		key, value := iter.Key(), iter.Value()
		binary.BigEndian.PutUint64(length[:], uint64(len(key)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write(key)
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write(value)
	}
	if err := iter.Error(); err != nil {
		return [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(digest[:0]))
	return digest, nil
}

func checkpointDigest(domain string, data []byte) [32]byte {
	hasher := blake3.NewDeriveKey(domain)
	_, _ = hasher.Write(data)
	var digest [32]byte
	copy(digest[:], hasher.Sum(digest[:0]))
	return digest
}

func encodeCheckpointManifest(manifest checkpointManifest) []byte {
	size := len(checkpointManifestMagic) + 1 + 4 + len(manifest.sourceIdentity) + 4 + len(manifest.releaseChecksum) +
		32 + 8 + 8 + 32 + 4 + checkpointManifestChecksumSize
	for _, file := range manifest.files {
		size += 4 + len(file.path) + 8 + len(file.digest)
	}
	out := make([]byte, 0, size)
	out = append(out, checkpointManifestMagic[:]...)
	out = append(out, checkpointManifestVersion)
	out = checkpointAppendString(out, manifest.sourceIdentity)
	out = checkpointAppendString(out, manifest.releaseChecksum)
	out = append(out, manifest.semanticDigest[:]...)
	out = binary.BigEndian.AppendUint64(out, manifest.recordCount)
	out = binary.BigEndian.AppendUint64(out, manifest.appliedCount)
	out = append(out, manifest.hardStateDigest[:]...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(manifest.files))) //nolint:gosec // files count is verified to be within uint32 range by collectCheckpointFiles and verification checks
	for _, file := range manifest.files {
		out = checkpointAppendString(out, file.path)
		out = binary.BigEndian.AppendUint64(out, file.size)
		out = append(out, file.digest[:]...)
	}
	checksum := checkpointDigest(checkpointManifestDomain, out)
	out = append(out, checksum[:]...)
	return out
}

func checkpointAppendString(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value))) //nolint:gosec // string length is bounded by checkpointManifestMaxField via validateCheckpointRelativePath and metadata validation
	return append(dst, value...)
}

type checkpointManifestParser struct {
	b   []byte
	err bool
}

func (p *checkpointManifestParser) bytes(size int) []byte {
	if p.err || size < 0 || len(p.b) < size {
		p.err = true
		return nil
	}
	value := p.b[:size]
	p.b = p.b[size:]
	return value
}

func (p *checkpointManifestParser) uint32() uint32 {
	value := p.bytes(4)
	if value == nil {
		return 0
	}
	return binary.BigEndian.Uint32(value)
}

func (p *checkpointManifestParser) uint64() uint64 {
	value := p.bytes(8)
	if value == nil {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

func (p *checkpointManifestParser) string(maxLen int) string {
	if maxLen < 0 {
		p.err = true
		return ""
	}
	size := p.uint32()
	if p.err || size > uint32(maxLen) { //nolint:gosec // maxLen is verified non-negative and is bounded by checkpointManifestMaxField
		p.err = true
		return ""
	}
	value := p.bytes(int(size))
	if value == nil || !utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0 {
		p.err = true
		return ""
	}
	return string(value)
}

func decodeCheckpointManifest(encoded []byte) (checkpointManifest, error) {
	minimum := len(checkpointManifestMagic) + 1 + 4 + 4 + 32 + 8 + 8 + 32 + 4 + checkpointManifestChecksumSize
	if len(encoded) < minimum || len(encoded) > checkpointManifestMaxSize {
		return checkpointManifest{}, fmt.Errorf("kv: malformed checkpoint manifest")
	}
	if !bytes.Equal(encoded[:len(checkpointManifestMagic)], checkpointManifestMagic[:]) {
		return checkpointManifest{}, fmt.Errorf("kv: malformed checkpoint manifest magic")
	}
	if encoded[len(checkpointManifestMagic)] != checkpointManifestVersion {
		return checkpointManifest{}, fmt.Errorf("kv: unknown checkpoint manifest version %d", encoded[len(checkpointManifestMagic)])
	}
	payload := encoded[:len(encoded)-checkpointManifestChecksumSize]
	wantChecksum := checkpointDigest(checkpointManifestDomain, payload)
	var storedChecksum [checkpointManifestChecksumSize]byte
	copy(storedChecksum[:], encoded[len(payload):])
	if wantChecksum != storedChecksum {
		return checkpointManifest{}, fmt.Errorf("kv: checkpoint manifest checksum mismatch")
	}
	parser := checkpointManifestParser{b: payload[len(checkpointManifestMagic)+1:]}
	manifest := checkpointManifest{
		sourceIdentity:  parser.string(checkpointManifestMaxField),
		releaseChecksum: parser.string(checkpointManifestMaxField),
	}
	copy(manifest.semanticDigest[:], parser.bytes(len(manifest.semanticDigest)))
	manifest.recordCount = parser.uint64()
	manifest.appliedCount = parser.uint64()
	copy(manifest.hardStateDigest[:], parser.bytes(len(manifest.hardStateDigest)))
	fileCount := parser.uint32()
	const minimumManifestFileEntry = 4 + 8 + 32
	if parser.err || fileCount > checkpointManifestMaxFiles ||
		uint64(fileCount)*minimumManifestFileEntry > uint64(len(parser.b)) {
		return checkpointManifest{}, fmt.Errorf("kv: malformed checkpoint manifest")
	}
	manifest.files = make([]checkpointManifestFile, int(fileCount))
	previous := ""
	for i := range manifest.files {
		file := &manifest.files[i]
		file.path = parser.string(checkpointManifestMaxField)
		file.size = parser.uint64()
		copy(file.digest[:], parser.bytes(len(file.digest)))
		if parser.err {
			return checkpointManifest{}, fmt.Errorf("kv: malformed checkpoint manifest")
		}
		if err := validateCheckpointRelativePath(file.path); err != nil {
			return checkpointManifest{}, err
		}
		if i > 0 && file.path <= previous {
			if file.path == previous {
				return checkpointManifest{}, fmt.Errorf("kv: duplicate checkpoint manifest entry %q", file.path)
			}
			return checkpointManifest{}, fmt.Errorf("kv: checkpoint manifest entries are not sorted")
		}
		previous = file.path
	}
	if parser.err || len(parser.b) != 0 {
		return checkpointManifest{}, fmt.Errorf("kv: trailing or malformed checkpoint manifest bytes")
	}
	manifest.checksum = storedChecksum
	return manifest, nil
}

func readCheckpointManifest(checkpointDir string) (checkpointManifest, []byte, error) {
	rootInfo, err := os.Lstat(checkpointDir)
	if err != nil {
		return checkpointManifest{}, nil, err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return checkpointManifest{}, nil, fmt.Errorf("kv: checkpoint path %q is not a directory", checkpointDir)
	}
	manifestPath := filepath.Join(checkpointDir, checkpointManifestName)
	info, err := os.Lstat(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return checkpointManifest{}, nil, fmt.Errorf("kv: current checkpoint manifest is missing; use explicit legacy verification for a legacy checkpoint")
	}
	if err != nil {
		return checkpointManifest{}, nil, err
	}
	if !info.Mode().IsRegular() {
		return checkpointManifest{}, nil, fmt.Errorf("kv: checkpoint manifest is not a regular file")
	}
	if info.Size() < 0 || info.Size() > checkpointManifestMaxSize {
		return checkpointManifest{}, nil, fmt.Errorf("kv: checkpoint manifest has invalid size")
	}
	encoded, err := os.ReadFile(manifestPath) //nolint:gosec // manifestPath is a controlled configuration path
	if err != nil {
		return checkpointManifest{}, nil, err
	}
	manifest, err := decodeCheckpointManifest(encoded)
	if err != nil {
		return checkpointManifest{}, nil, err
	}
	return manifest, encoded, nil
}

func writeCheckpointManifest(checkpointDir string, encoded []byte) error {
	manifestPath := filepath.Join(checkpointDir, checkpointManifestName)
	file, err := checkpointOpenFile(manifestPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return err
	}
	if err := checkpointSyncFile(file); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	dir, err := checkpointOpen(checkpointDir)
	if err != nil {
		return err
	}
	if err := checkpointSyncFile(dir); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func validateCheckpointTree(checkpointDir string) error {
	return checkpointWalkDir(checkpointDir, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("kv: checkpoint entry %q is a symlink", filePath)
		}
		rel, err := checkpointRel(checkpointDir, filePath)
		if err != nil {
			return err
		}
		if rel == "." {
			if !entry.IsDir() {
				return fmt.Errorf("kv: checkpoint path %q is not a directory", checkpointDir)
			}
			return nil
		}
		relative := filepath.ToSlash(rel)
		if relative != checkpointManifestName {
			if err := validateCheckpointRelativePath(relative); err != nil {
				return err
			}
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("kv: checkpoint entry %q is not a regular file", filePath)
		}
		return nil
	})
}

func validateCheckpointRelativePath(relative string) error {
	if relative == "" || len(relative) > checkpointManifestMaxField || relative == "." || relative == checkpointManifestName ||
		strings.Contains(relative, "\\") || strings.IndexByte(relative, 0) >= 0 ||
		!utf8.ValidString(relative) || path.IsAbs(relative) || path.Clean(relative) != relative ||
		relative == ".." || strings.HasPrefix(relative, "../") {
		return fmt.Errorf("kv: invalid checkpoint manifest path %q", relative)
	}
	return nil
}

func collectCheckpointFiles(checkpointDir string, hashFiles bool) ([]checkpointManifestFile, error) {
	files := make([]checkpointManifestFile, 0)
	err := checkpointWalkDir(checkpointDir, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("kv: checkpoint entry %q is a symlink", filePath)
		}
		rel, err := checkpointRel(checkpointDir, filePath)
		if err != nil {
			return err
		}
		if rel == "." || entry.IsDir() {
			return nil
		}
		relative := filepath.ToSlash(rel)
		if relative == checkpointManifestName {
			return nil
		}
		if err := validateCheckpointRelativePath(relative); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("kv: checkpoint entry %q is not a regular file", filePath)
		}
		sz := info.Size()
		if sz < 0 {
			return fmt.Errorf("kv: checkpoint entry %q has negative size %d", filePath, sz)
		}
		if len(files) >= checkpointManifestMaxFiles {
			return fmt.Errorf("kv: checkpoint file count exceeds max limit %d", checkpointManifestMaxFiles)
		}
		file := checkpointManifestFile{path: relative, size: uint64(sz)} //nolint:gosec // file size is verified non-negative
		if hashFiles {
			digest, size, err := checkpointFileDigest(filePath)
			if err != nil {
				return err
			}
			if size != file.size {
				return fmt.Errorf("kv: checkpoint file %q changed while hashing", relative)
			}
			file.digest = digest
		}
		files = append(files, file)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	return files, nil
}

func checkpointFileDigest(filePath string) ([32]byte, uint64, error) {
	file, err := checkpointOpen(filePath)
	if err != nil {
		return [32]byte{}, 0, err
	}
	defer func() { _ = file.Close() }()
	hasher := blake3.NewDeriveKey(checkpointFileChecksumDomain)
	size, err := io.Copy(hasher, file)
	if err != nil {
		return [32]byte{}, 0, err
	}
	if size < 0 {
		return [32]byte{}, 0, fmt.Errorf("kv: negative file size while hashing %q", filePath)
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(digest[:0]))
	return digest, uint64(size), nil //nolint:gosec // file size is verified non-negative
}

func compareCheckpointFiles(manifestFiles, actualFiles []checkpointManifestFile) error {
	if len(manifestFiles) != len(actualFiles) {
		return fmt.Errorf("kv: checkpoint manifest file count mismatch: manifest=%d actual=%d", len(manifestFiles), len(actualFiles))
	}
	for i := range manifestFiles {
		want, got := manifestFiles[i], actualFiles[i]
		if want.path != got.path {
			return fmt.Errorf("kv: checkpoint manifest file mismatch: manifest=%q actual=%q", want.path, got.path)
		}
		if want.size != got.size || want.digest != got.digest {
			return fmt.Errorf("kv: checkpoint file checksum mismatch for %q", want.path)
		}
	}
	return nil
}

func checkpointInfoFromState(manifest checkpointManifest, state checkpointState, identity string, current bool) CheckpointInfo {
	return CheckpointInfo{
		Format:              "manifest-v1",
		ManifestVersion:     checkpointManifestVersion,
		ManifestIdentity:    identity,
		SemanticStateDigest: hex.EncodeToString(state.semanticDigest[:]),
		RecordCount:         uint64(len(state.records)),
		AppliedCount:        uint64(len(state.applied)),
		HardStateDigest:     hex.EncodeToString(state.hardStateDigest[:]),
		SourceIdentity:      manifest.sourceIdentity,
		ReleaseChecksum:     manifest.releaseChecksum,
		CurrentTarget:       current,
	}
}

// RepairFromCheckpoint verifies a staged byte-for-byte copy before atomically
// replacing dataDir. It never recomputes or repairs individual records.
func RepairFromCheckpoint(dataDir, checkpointDir string) error {
	if err := restoreCheckpointDirectory(dataDir, checkpointDir, VerifyCheckpoint); err != nil {
		return fmt.Errorf("kv: checkpoint repair failed: %w", err)
	}
	return nil
}

// RestoreCheckpoint replaces dataDir only after a staged copy passes current
// manifest and semantic verification. The original directory is not moved
// until verification succeeds.
func RestoreCheckpoint(dataDir, checkpointDir string) error {
	return restoreCheckpointDirectory(dataDir, checkpointDir, VerifyCheckpoint)
}

func restoreCheckpointDirectory(dataDir, checkpointDir string, verify func(string) error) error {
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
	if verify != nil {
		if err := verify(tmp); err != nil {
			return fmt.Errorf("kv: staged checkpoint verification failed: %w", err)
		}
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
				return fmt.Errorf("kv: restore rename failed: %w; rollback failed: %w", err, rollbackErr)
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
	return checkpointWalkDir(src, func(filePath string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("kv: checkpoint entry %q is not a regular file", filePath)
		}
		rel, err := checkpointRel(src, filePath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		relative := filepath.ToSlash(rel)
		if relative != checkpointManifestName {
			if err := validateCheckpointRelativePath(relative); err != nil {
				return err
			}
		} else if filepath.Clean(rel) != rel {
			return fmt.Errorf("kv: invalid checkpoint manifest path %q", rel)
		}
		out := filepath.Join(dst, filepath.FromSlash(relative))
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(out, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("kv: checkpoint entry %q is not a regular file", filePath)
		}
		return copyCheckpointFile(filePath, out, info.Mode().Perm())
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
