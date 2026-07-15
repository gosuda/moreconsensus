package epaxos

import (
	"bytes"
	"fmt"
	"sort"
)

// Storage exposes startup checkpoint-plus-delta state and exact record loads.
type Storage interface {
	InitialState() (StorageState, error)
	LoadCheckpoint() (Checkpoint, error)
	LoadInstances(after ExecutionFrontier, yield func(InstanceRecord) error) error
	LoadInstance(ref InstanceRef) (InstanceRecord, bool, error)
}

// MemoryStorage is a deterministic in-memory storage implementation for tests and examples.
type MemoryStorage struct {
	Hard HardState
	// Configs mirrors the historical configuration snapshots retained by older
	// embeddings. ConfigHistory is authoritative when populated.
	Configs          []ConfState
	ConfigHistory    []ConfigHistoryEntry
	BootstrapRecords []BootstrapRecord
	LocalVoterState  LocalVoterState
	Frontiers        []FrontierUpdate
	AllocatorFloor   InstanceNum
	TOQClosedThrough uint64
	Records          map[InstanceRef]InstanceRecord
	Checkpoint       Checkpoint
	FailWrites       bool
}

// NewMemoryStorage returns an empty deterministic in-memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{Records: make(map[InstanceRef]InstanceRecord)}
}

// InitialState returns an ownership-independent complete durable state.
func (m *MemoryStorage) InitialState() (StorageState, error) {
	state, err := m.storageState()
	if err != nil {
		return StorageState{}, err
	}
	return state, nil
}

func (m *MemoryStorage) storageState() (StorageState, error) {
	history := cloneConfigHistory(m.ConfigHistory)
	for i := range history {
		history[i].Conf.ID = normalizeConfID(history[i].Conf.ID)
	}
	for _, conf := range m.Configs {
		conf.ID = normalizeConfID(conf.ID)
		entry := ConfigHistoryEntry{Conf: conf.Clone()}
		found := false
		for i := range history {
			if history[i].Conf.ID != conf.ID {
				continue
			}
			if !confStateEqual(history[i].Conf, conf) {
				return StorageState{}, fmt.Errorf("%w: conflicting stored configuration %d", ErrInvalidConfig, conf.ID)
			}
			found = true
			break
		}
		if !found {
			history = append(history, entry)
		}
	}
	sort.Slice(history, func(i, j int) bool { return history[i].Conf.ID < history[j].Conf.ID })
	state := StorageState{
		HardState:        m.Hard.Clone(),
		ConfigHistory:    history,
		BootstrapRecords: cloneBootstrapRecords(m.BootstrapRecords),
		LocalVoterState:  m.LocalVoterState.Clone(),
		Frontiers:        cloneFrontierUpdates(m.Frontiers),
		AllocatorFloor:   m.AllocatorFloor,
		TOQClosedThrough: m.TOQClosedThrough,
	}
	if err := validateStorageState(state); err != nil {
		return StorageState{}, err
	}
	return state, nil
}

// LoadCheckpoint returns an ownership-independent durable checkpoint.
func (m *MemoryStorage) LoadCheckpoint() (Checkpoint, error) {
	return m.Checkpoint.Clone(), nil
}

// LoadInstances iterates over records strictly after the compacted frontier.
func (m *MemoryStorage) LoadInstances(after ExecutionFrontier, yield func(InstanceRecord) error) error {
	refs := make([]InstanceRef, 0, len(m.Records))
	for ref := range m.Records {
		if !frontierCovers(after, ref) {
			refs = append(refs, ref)
		}
	}
	sortRefs(refs)
	for _, ref := range refs {
		rec := m.Records[ref].Clone()
		if !verifyCanonicalRecordChecksumBytes(rec) &&
			!VerifyRecordChecksumWithoutMembershipResult(rec) &&
			!VerifyRecordChecksumWithoutTimingDomain(rec) &&
			!VerifyRecordChecksumWithoutConfChangeResult(rec) {
			return ErrChecksumMismatch
		}
		if err := yield(rec); err != nil {
			return err
		}
	}
	return nil
}

// LoadInstance returns one non-compacted durable record.
func (m *MemoryStorage) LoadInstance(ref InstanceRef) (InstanceRecord, bool, error) {
	if frontierCovers(m.Checkpoint.CompactedThrough, ref) {
		return InstanceRecord{}, false, nil
	}
	rec, ok := m.Records[ref]
	return rec.Clone(), ok, nil
}

// ApplyReady durably applies a Ready batch atomically in persistence order.
func (m *MemoryStorage) ApplyReady(rd Ready) error {
	if m.FailWrites {
		return fmt.Errorf("%w: memory storage write failure", ErrMessageRejected)
	}
	candidate := m.deepClone()
	if err := candidate.applyReadyValidated(rd); err != nil {
		return err
	}
	failWrites := m.FailWrites
	*m = *candidate
	m.FailWrites = failWrites
	return nil
}

func (m *MemoryStorage) deepClone() *MemoryStorage {
	out := &MemoryStorage{
		Hard:             m.Hard.Clone(),
		Configs:          make([]ConfState, len(m.Configs)),
		ConfigHistory:    cloneConfigHistory(m.ConfigHistory),
		BootstrapRecords: cloneBootstrapRecords(m.BootstrapRecords),
		LocalVoterState:  m.LocalVoterState.Clone(),
		Frontiers:        cloneFrontierUpdates(m.Frontiers),
		AllocatorFloor:   m.AllocatorFloor,
		TOQClosedThrough: m.TOQClosedThrough,
		Checkpoint:       m.Checkpoint.Clone(),
		Records:          make(map[InstanceRef]InstanceRecord, len(m.Records)),
		FailWrites:       m.FailWrites,
	}
	for i := range m.Configs {
		out.Configs[i] = m.Configs[i].Clone()
	}
	for ref, record := range m.Records {
		out.Records[ref] = record.Clone()
	}
	return out
}

func (m *MemoryStorage) applyReadyValidated(rd Ready) error {
	if err := validateCompleteHardState(m.Hard); err != nil {
		return err
	}
	if err := validateCompleteHardState(rd.HardState); err != nil {
		return err
	}
	state, err := m.storageState()
	if err != nil {
		return err
	}

	history := cloneConfigHistory(state.ConfigHistory)
	remember := func(entry ConfigHistoryEntry) error {
		if err := validateConfigHistoryEntry(entry); err != nil {
			return err
		}
		i := sort.Search(len(history), func(i int) bool { return history[i].Conf.ID >= entry.Conf.ID })
		if i < len(history) && history[i].Conf.ID == entry.Conf.ID {
			if !configHistoryEntryCompatible(history[i], entry) {
				return fmt.Errorf("%w: conflicting stored configuration %d", ErrInvalidConfig, entry.Conf.ID)
			}
			merged := history[i].Clone()
			if merged.AppliedRef.IsZero() && !entry.AppliedRef.IsZero() {
				merged.AppliedRef = entry.AppliedRef
			}
			if merged.IdentityDigest == (StateDigest{}) && entry.IdentityDigest != (StateDigest{}) {
				merged.IdentityDigest = entry.IdentityDigest
			}
			history[i] = merged
			return nil
		}
		history = append(history, ConfigHistoryEntry{})
		copy(history[i+1:], history[i:])
		history[i] = entry.Clone()
		return nil
	}

	if !m.Hard.Empty() {
		if err := remember(ConfigHistoryEntry{Conf: m.Hard.Conf}); err != nil {
			return err
		}
	}
	if len(rd.ConfigHistory) > maxBootstrapHistoryEntries {
		return ErrBootstrapBounds
	}
	for i, entry := range rd.ConfigHistory {
		if i > 0 && rd.ConfigHistory[i-1].Conf.ID >= entry.Conf.ID {
			return fmt.Errorf("%w: unordered ready configuration history", ErrInvalidConfig)
		}
		if err := remember(entry); err != nil {
			return err
		}
	}
	if !rd.HardState.Empty() {
		if !m.Hard.Empty() {
			if rd.HardState.Tick < m.Hard.Tick {
				return fmt.Errorf("%w: hard-state tick regression from %d to %d", ErrInvalidConfig, m.Hard.Tick, rd.HardState.Tick)
			}
			if rd.HardState.Conf.ID < m.Hard.Conf.ID {
				return fmt.Errorf("%w: hard-state configuration regression from %d to %d", ErrInvalidConfig, m.Hard.Conf.ID, rd.HardState.Conf.ID)
			}
		}
		if err := remember(ConfigHistoryEntry{Conf: rd.HardState.Conf}); err != nil {
			return err
		}
	}

	lastRecord := make(map[InstanceRef]int, len(rd.Records))
	for i, rec := range rd.Records {
		if frontierCovers(m.Checkpoint.CompactedThrough, rec.Ref) {
			return fmt.Errorf("%w: record %s is below compacted frontier", ErrInvalidCheckpoint, rec.Ref)
		}
		if !VerifyRecordChecksum(rec) {
			return ErrChecksumMismatch
		}
		lastRecord[rec.Ref] = i
	}

	for index, rec := range rd.Records {
		if lastRecord[rec.Ref] != index {
			continue
		}
		if current, ok := m.Records[rec.Ref]; ok {
			if rec.Status < current.Status || (!current.Ballot.Less(rec.Ballot) && current.Ballot != rec.Ballot) {
				return fmt.Errorf("%w: durable record regression at %s (status %s -> %s, ballot %#v -> %#v)",
					ErrInvalidConfig, rec.Ref, current.Status, rec.Status, current.Ballot, rec.Ballot)
			}
			if current.Status == StatusExecuted && !instanceRecordEqual(current, rec) &&
				!terminalRecordPromiseUpdate(current, rec) && !terminalRecordMigration(current, rec) {
				return fmt.Errorf("%w: conflicting terminal record at %s: current=%#v next=%#v",
					ErrInvalidConfig, rec.Ref, current, rec)
			}
		}
		if rec.ConfChangeResult.Outcome == ConfChangeApplied {
			if err := validateConfChangeResult(rec); err != nil {
				return err
			}
			if err := remember(ConfigHistoryEntry{Conf: rec.ConfChangeResult.Conf, AppliedRef: rec.Ref}); err != nil {
				return err
			}
		}
		if err := validateMembershipResult(rec); err != nil {
			return err
		}
	}

	bootstrap := cloneBootstrapRecords(m.BootstrapRecords)
	for _, record := range rd.BootstrapRecords {
		if err := validateBootstrapRecord(record); err != nil {
			return err
		}
		i := sort.Search(len(bootstrap), func(i int) bool {
			return bytes.Compare(bootstrap[i].Plan.Request.Plan[:], record.Plan.Request.Plan[:]) >= 0
		})
		if i < len(bootstrap) && bootstrap[i].Plan.Request.Plan == record.Plan.Request.Plan {
			if record.Phase < bootstrap[i].Phase || !voterPlanEqual(record.Plan, bootstrap[i].Plan) {
				return fmt.Errorf("%w: bootstrap phase regression or plan conflict", ErrInvalidConfig)
			}
			if record.Phase == bootstrap[i].Phase && !bootstrapRecordEqual(record, bootstrap[i]) {
				return fmt.Errorf("%w: conflicting duplicate bootstrap record", ErrInvalidConfig)
			}
			bootstrap[i] = record.Clone()
			continue
		}
		bootstrap = append(bootstrap, BootstrapRecord{})
		copy(bootstrap[i+1:], bootstrap[i:])
		bootstrap[i] = record.Clone()
	}

	local := m.LocalVoterState.Clone()
	if rd.LocalVoterState != nil {
		if err := validateLocalVoterTransition(local, *rd.LocalVoterState); err != nil {
			return err
		}
		local = rd.LocalVoterState.Clone()
	}

	frontiers := cloneFrontierUpdates(m.Frontiers)
	allocatorFloor := m.AllocatorFloor
	toqFloor := m.TOQClosedThrough
	if rd.AllocatorFloor != 0 {
		if allocatorFloor != 0 && rd.AllocatorFloor < allocatorFloor {
			return fmt.Errorf("%w: allocator floor regression", ErrInvalidConfig)
		}
		allocatorFloor = rd.AllocatorFloor
	}
	configForFrontier := func(id ConfID) (ConfState, bool) {
		i := sort.Search(len(history), func(i int) bool { return history[i].Conf.ID >= id })
		if i < len(history) && history[i].Conf.ID == id {
			return history[i].Conf, true
		}
		if !m.Hard.Empty() && m.Hard.Conf.ID == id {
			return m.Hard.Conf, true
		}
		if !rd.HardState.Empty() && rd.HardState.Conf.ID == id {
			return rd.HardState.Conf, true
		}
		return ConfState{}, false
	}
	for _, update := range rd.FrontierUpdates {
		if update.AllocatorFloor != 0 && update.AllocatorFloor < allocatorFloor {
			return fmt.Errorf("%w: frontier allocator floor regression", ErrInvalidConfig)
		}
		if update.TOQClosedThrough < toqFloor {
			return fmt.Errorf("%w: TOQ closed floor regression", ErrInvalidConfig)
		}
		conf, ok := configForFrontier(update.Frontier.Conf)
		if !ok || validateBootstrapFrontier(update.Frontier, conf) != nil ||
			update.EvidenceDigest != domainDigest("epaxos/bootstrap/frontier/v1", appendFrontier(nil, update.Frontier)) {
			return fmt.Errorf("%w: malformed frontier evidence", ErrInvalidConfig)
		}
		allocatorFloor = maxInstanceNum(allocatorFloor, update.AllocatorFloor)
		toqFloor = update.TOQClosedThrough
		replaced := false
		for i := range frontiers {
			if frontiers[i].Frontier.Conf != update.Frontier.Conf {
				continue
			}
			if frontierRegresses(frontiers[i].Frontier, update.Frontier) {
				return fmt.Errorf("%w: frontier regression", ErrInvalidConfig)
			}
			frontiers[i] = update.Clone()
			replaced = true
			break
		}
		if !replaced {
			frontiers = append(frontiers, update.Clone())
		}
	}
	sort.Slice(frontiers, func(i, j int) bool { return frontiers[i].Frontier.Conf < frontiers[j].Frontier.Conf })

	candidate := StorageState{
		HardState:        m.Hard,
		ConfigHistory:    history,
		BootstrapRecords: bootstrap,
		LocalVoterState:  local,
		Frontiers:        frontiers,
		AllocatorFloor:   allocatorFloor,
		TOQClosedThrough: toqFloor,
	}
	if !rd.HardState.Empty() {
		candidate.HardState = rd.HardState.Clone()
	}
	if err := validateStorageState(candidate); err != nil {
		return err
	}

	checkpoint := m.Checkpoint.Clone()
	historyByID := make(map[ConfID]ConfState, len(history))
	for _, entry := range history {
		historyByID[entry.Conf.ID] = entry.Conf.Clone()
	}
	if rd.Snapshot != nil {
		if rd.Snapshot.Mode != SnapshotPersistLocal && rd.Snapshot.Mode != SnapshotInstall {
			return ErrInvalidCheckpoint
		}
		next := rd.Snapshot.Checkpoint.Clone()
		if rd.Snapshot.Mode == SnapshotInstall && !checkpointCertified(next) {
			return ErrInvalidCheckpoint
		}
		if err := validateCheckpoint(next, historyByID); err != nil {
			return err
		}
		if !checkpoint.Empty() {
			if frontierRegressesExecution(checkpoint.Descriptor.Through, next.Descriptor.Through) ||
				frontierRegressesExecution(checkpoint.CompactedThrough, next.CompactedThrough) {
				return ErrInvalidCheckpoint
			}
			if checkpoint.Descriptor.ID == next.Descriptor.ID &&
				checkpointCertified(checkpoint) && !checkpointCertified(next) {
				return ErrInvalidCheckpoint
			}
		}
		checkpoint = next
	}
	expectedCompaction := canonicalCompactionRanges(checkpoint)
	if len(rd.Compact) > 0 {
		if !checkpointCertified(checkpoint) || len(rd.Compact) != len(expectedCompaction) {
			return ErrInvalidCheckpoint
		}
		for i := range rd.Compact {
			if rd.Compact[i] != expectedCompaction[i] {
				return ErrInvalidCheckpoint
			}
		}
		checkpoint.CompactedThrough = checkpoint.Descriptor.Through.Clone()
		checkpoint.Checksum = DigestCheckpoint(checkpoint)
	}

	m.Hard = candidate.HardState.Clone()
	m.ConfigHistory = cloneConfigHistory(candidate.ConfigHistory)
	m.Configs = m.Configs[:0]
	for _, entry := range candidate.ConfigHistory {
		if entry.Conf.ID != 1 || !entry.AppliedRef.IsZero() {
			m.Configs = append(m.Configs, entry.Conf.Clone())
		}
	}
	m.BootstrapRecords = cloneBootstrapRecords(candidate.BootstrapRecords)
	m.LocalVoterState = candidate.LocalVoterState.Clone()
	m.Frontiers = cloneFrontierUpdates(candidate.Frontiers)
	m.AllocatorFloor = candidate.AllocatorFloor
	m.TOQClosedThrough = candidate.TOQClosedThrough
	if m.Records == nil {
		m.Records = make(map[InstanceRef]InstanceRecord)
	}
	for _, rec := range rd.Records {
		m.Records[rec.Ref] = rec.Clone()
	}
	m.Checkpoint = checkpoint.Clone()
	if len(rd.Compact) > 0 {
		for ref := range m.Records {
			if frontierCovers(m.Checkpoint.CompactedThrough, ref) {
				delete(m.Records, ref)
			}
		}
	}
	return nil
}

func terminalRecordPromiseUpdate(current, next InstanceRecord) bool {
	if current.Status != StatusExecuted || next.Status != StatusExecuted || !current.Ballot.Less(next.Ballot) {
		return false
	}
	expected := current.Clone()
	expected.Ballot = next.Ballot
	expected.Checksum = next.Checksum
	return instanceRecordEqual(expected, next)
}

func terminalRecordMigration(current, next InstanceRecord) bool {
	if current.Ref != next.Ref || current.Status != StatusExecuted || next.Status != StatusExecuted ||
		current.Ballot != next.Ballot || current.RecordBallot != next.RecordBallot ||
		current.Seq != next.Seq || !instanceNumsEqual(current.Deps, next.Deps) ||
		!commandEqual(current.Command, next.Command) ||
		current.ConfChangeResult.Outcome != ConfChangeOutcomeUnspecified ||
		!confStateIsZero(current.ConfChangeResult.Conf) || !membershipResultAbsent(current) {
		return false
	}
	return true
}

func validateStorageState(state StorageState) error {
	if err := validateCompleteHardState(state.HardState); err != nil {
		return err
	}
	if len(state.ConfigHistory) > maxBootstrapHistoryEntries || len(state.BootstrapRecords) > maxBootstrapHistoryEntries {
		return ErrBootstrapBounds
	}
	for i, entry := range state.ConfigHistory {
		if i > 0 && state.ConfigHistory[i-1].Conf.ID >= entry.Conf.ID {
			return fmt.Errorf("%w: duplicate or unordered configuration history", ErrInvalidConfig)
		}
		if err := validateConfigHistoryEntry(entry); err != nil {
			return err
		}
	}
	lastPlan := BootstrapID{}
	for i, record := range state.BootstrapRecords {
		if i > 0 && bytes.Compare(lastPlan[:], record.Plan.Request.Plan[:]) >= 0 {
			return fmt.Errorf("%w: duplicate or unordered bootstrap records", ErrInvalidConfig)
		}
		if err := validateBootstrapRecord(record); err != nil {
			return err
		}
		lastPlan = record.Plan.Request.Plan
		if record.Phase == BootstrapPhaseActivated {
			foundSuccessor := false
			for _, entry := range state.ConfigHistory {
				if confStateEqual(entry.Conf, record.Plan.Successor) &&
					entry.AppliedRef == record.Plan.Reservations.Activate &&
					entry.IdentityDigest == bootstrapIdentityDigest(record.Plan) {
					foundSuccessor = true
					break
				}
			}
			if !foundSuccessor || (!state.HardState.Empty() && state.HardState.Conf.ID < record.Plan.Successor.ID) {
				return fmt.Errorf("%w: activated bootstrap lacks causal configuration durability", ErrInvalidConfig)
			}
		}
	}
	if state.LocalVoterState.Status != LocalVoterStatusUnspecified {
		if err := validateLocalVoterState(state.LocalVoterState); err != nil {
			return err
		}
	}
	if state.LocalVoterState.Status != LocalVoterStatusUnspecified &&
		!state.HardState.Empty() &&
		!confStateEqual(state.LocalVoterState.Conf, state.HardState.Conf) {
		return fmt.Errorf("%w: local voter state and hard state disagree", ErrInvalidConfig)
	}
	for i, update := range state.Frontiers {
		if i > 0 && state.Frontiers[i-1].Frontier.Conf >= update.Frontier.Conf {
			return fmt.Errorf("%w: duplicate or unordered frontier updates", ErrInvalidConfig)
		}
		var conf ConfState
		for _, entry := range state.ConfigHistory {
			if entry.Conf.ID == update.Frontier.Conf {
				conf = entry.Conf
				break
			}
		}
		if confStateIsZero(conf) && state.HardState.Conf.ID == update.Frontier.Conf {
			conf = state.HardState.Conf
		}
		if confStateIsZero(conf) || validateBootstrapFrontier(update.Frontier, conf) != nil ||
			update.EvidenceDigest != domainDigest("epaxos/bootstrap/frontier/v1", appendFrontier(nil, update.Frontier)) {
			return fmt.Errorf("%w: malformed stored frontier evidence", ErrInvalidConfig)
		}
	}
	return nil
}

func validateConfigHistoryEntry(entry ConfigHistoryEntry) error {
	if entry.Conf.ID == 0 || len(entry.Conf.Voters) == 0 {
		return fmt.Errorf("%w: incomplete configuration history entry", ErrInvalidConfig)
	}
	q, err := newQuorum(entry.Conf.Voters)
	if err != nil || !sameReplicaIDs(q.conf.Voters, entry.Conf.Voters) {
		return fmt.Errorf("%w: noncanonical configuration history entry", ErrInvalidConfig)
	}
	if !entry.AppliedRef.IsZero() && (entry.AppliedRef.Conf == ^ConfID(0) || entry.AppliedRef.Conf+1 != entry.Conf.ID) {
		return fmt.Errorf("%w: configuration history winner mismatch", ErrInvalidConfig)
	}
	return nil
}

func configHistoryEntryCompatible(a, b ConfigHistoryEntry) bool {
	if !confStateEqual(a.Conf, b.Conf) ||
		(a.IdentityDigest != (StateDigest{}) && b.IdentityDigest != (StateDigest{}) && a.IdentityDigest != b.IdentityDigest) {
		return false
	}
	return a.AppliedRef.IsZero() || b.AppliedRef.IsZero() || a.AppliedRef == b.AppliedRef
}

func validateCompleteHardState(h HardState) error {
	if h.Empty() {
		return nil
	}
	if h.Conf.ID == 0 || len(h.Conf.Voters) == 0 {
		return fmt.Errorf("%w: hard state requires a complete configuration", ErrInvalidConfig)
	}
	q, err := newQuorum(h.Conf.Voters)
	if err != nil {
		return err
	}
	if !sameReplicaIDs(q.conf.Voters, h.Conf.Voters) {
		return fmt.Errorf("%w: hard-state voters must be sorted", ErrInvalidConfig)
	}
	return nil
}

func validateLocalVoterState(state LocalVoterState) error {
	if state.Status < LocalVoterStatusStaged || state.Status > LocalVoterStatusIneligible ||
		state.Cluster == (ClusterID{}) || !state.Identity.valid() || state.Conf.ID == 0 ||
		state.AllocatorFloor == 0 {
		return fmt.Errorf("%w: malformed local voter state", ErrInvalidConfig)
	}
	q, err := newQuorum(state.Conf.Voters)
	if err != nil || !sameReplicaIDs(q.conf.Voters, state.Conf.Voters) {
		return fmt.Errorf("%w: noncanonical local voter configuration", ErrInvalidConfig)
	}
	contains := state.Conf.Contains(state.Identity.Replica)
	switch state.Status {
	case LocalVoterStatusStaged:
		if contains || state.Plan == (BootstrapID{}) {
			return fmt.Errorf("%w: staged voter is present or lacks a plan", ErrInvalidConfig)
		}
	case LocalVoterStatusEligible:
		if !contains {
			return fmt.Errorf("%w: eligible voter is absent", ErrInvalidConfig)
		}
	case LocalVoterStatusIneligible:
		if contains {
			return fmt.Errorf("%w: ineligible voter remains present", ErrInvalidConfig)
		}
	case LocalVoterStatusUnspecified:
	}
	return nil
}

func validateLocalVoterTransition(current, next LocalVoterState) error {
	if err := validateLocalVoterState(next); err != nil {
		return err
	}
	if current.Status == LocalVoterStatusUnspecified {
		return nil
	}
	if err := validateLocalVoterState(current); err != nil {
		return err
	}
	if current.Cluster != next.Cluster || current.Identity.Replica != next.Identity.Replica ||
		next.Identity.Incarnation < current.Identity.Incarnation || next.Conf.ID < current.Conf.ID ||
		next.AllocatorFloor < current.AllocatorFloor || next.TOQClosedThrough < current.TOQClosedThrough {
		return fmt.Errorf("%w: local voter state regression", ErrInvalidConfig)
	}
	if current.Status == LocalVoterStatusEligible &&
		(next.Conf.ID == current.Conf.ID || next.Status == LocalVoterStatusStaged) &&
		next.Status != LocalVoterStatusEligible {
		return fmt.Errorf("%w: local voter eligibility revoked without successor", ErrInvalidConfig)
	}
	if current.Status == LocalVoterStatusIneligible && next.Status == LocalVoterStatusEligible &&
		next.Identity.Incarnation == current.Identity.Incarnation {
		return fmt.Errorf("%w: ineligible voter reactivated without a new incarnation", ErrInvalidConfig)
	}
	return nil
}

func frontierRegresses(current, next BootstrapFrontier) bool {
	if current.Conf != next.Conf || len(current.Lanes) != len(next.Lanes) {
		return true
	}
	for i := range current.Lanes {
		a, b := current.Lanes[i], next.Lanes[i]
		if a.Replica != b.Replica || b.ObservedThrough < a.ObservedThrough || b.CommittedThrough < a.CommittedThrough ||
			b.ExecutedThrough < a.ExecutedThrough || b.CompactedExecutedThrough < a.CompactedExecutedThrough {
			return true
		}
		nextIndex := 0
		for _, instance := range a.Sparse {
			for nextIndex < len(b.Sparse) && b.Sparse[nextIndex] < instance {
				nextIndex++
			}
			if nextIndex == len(b.Sparse) || b.Sparse[nextIndex] != instance {
				return true
			}
		}
	}
	return false
}

func maxInstanceNum(a, b InstanceNum) InstanceNum {
	if a > b {
		return a
	}
	return b
}

// Instance returns a copy of a durable record.
func (m *MemoryStorage) Instance(ref InstanceRef) (InstanceRecord, bool) {
	rec, ok := m.Records[ref]
	return rec.Clone(), ok
}

func sortRefs(refs []InstanceRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Conf != refs[j].Conf {
			return refs[i].Conf < refs[j].Conf
		}
		if refs[i].Replica != refs[j].Replica {
			return refs[i].Replica < refs[j].Replica
		}
		return refs[i].Instance < refs[j].Instance
	})
}
