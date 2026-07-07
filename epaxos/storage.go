package epaxos

import (
	"fmt"
	"sort"
)

// Storage loads durable state for a RawNode.
type Storage interface {
	InitialState() (HardState, []ConfState, error)
	LoadInstances(func(InstanceRecord) error) error
}

// MemoryStorage is a deterministic in-memory storage implementation for tests and examples.
type MemoryStorage struct {
	Hard       HardState
	Configs    []ConfState
	Records    map[InstanceRef]InstanceRecord
	FailWrites bool
}

// NewMemoryStorage returns an empty deterministic in-memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{Records: make(map[InstanceRef]InstanceRecord)}
}

// InitialState returns copies of the durable hard state and configuration history.
func (m *MemoryStorage) InitialState() (HardState, []ConfState, error) {
	configs := make([]ConfState, len(m.Configs))
	for i := range m.Configs {
		configs[i] = m.Configs[i].Clone()
	}
	return HardState{Conf: m.Hard.Conf.Clone(), Tick: m.Hard.Tick}, configs, nil
}

// LoadInstances iterates over durable records in deterministic order.
func (m *MemoryStorage) LoadInstances(fn func(InstanceRecord) error) error {
	refs := make([]InstanceRef, 0, len(m.Records))
	for ref := range m.Records {
		refs = append(refs, ref)
	}
	sortRefs(refs)
	for _, ref := range refs {
		rec := m.Records[ref].Clone()
		if !VerifyRecordChecksum(rec) {
			return ErrChecksumMismatch
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// ApplyReady durably applies a Ready batch in the required persistence order.
func (m *MemoryStorage) ApplyReady(rd Ready) error {
	if m.FailWrites {
		return fmt.Errorf("%w: memory storage write failure", ErrMessageRejected)
	}
	if m.Records == nil {
		m.Records = make(map[InstanceRef]InstanceRecord)
	}
	for _, rec := range rd.Records {
		if !VerifyRecordChecksum(rec) {
			return ErrChecksumMismatch
		}
		m.Records[rec.Ref] = rec.Clone()
		if rec.Ref.Conf >= m.Hard.Conf.ID {
			m.Hard.Conf.ID = rec.Ref.Conf
		}
	}
	return nil
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
