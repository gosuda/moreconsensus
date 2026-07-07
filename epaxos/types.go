// Package epaxos implements a deterministic, library-embedded EPaxos core.
package epaxos

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

// ReplicaID identifies a replica inside a configuration.
type ReplicaID uint64

// InstanceNum identifies one instance in a replica-local EPaxos instance space.
type InstanceNum uint64

// ConfID identifies the configuration that owns an instance.
type ConfID uint64

// InstanceRef is the globally unique name of an EPaxos instance.
type InstanceRef struct {
	Replica  ReplicaID
	Instance InstanceNum
	Conf     ConfID
}

// IsZero reports whether the reference names no instance.
func (r InstanceRef) IsZero() bool { return r.Replica == 0 && r.Instance == 0 && r.Conf == 0 }

// String returns a deterministic human-readable instance reference.
func (r InstanceRef) String() string { return fmt.Sprintf("%d.%d@%d", r.Replica, r.Instance, r.Conf) }

// Ballot is the EPaxos promise/accept ballot.
type Ballot struct {
	Epoch   uint64
	Number  uint64
	Replica ReplicaID
}

// Less reports whether b is ordered before other.
func (b Ballot) Less(other Ballot) bool {
	if b.Epoch != other.Epoch {
		return b.Epoch < other.Epoch
	}
	if b.Number != other.Number {
		return b.Number < other.Number
	}
	return b.Replica < other.Replica
}

// Next returns a ballot owned by replica and greater than b.
func (b Ballot) Next(replica ReplicaID) Ballot {
	return Ballot{Epoch: b.Epoch, Number: b.Number + 1, Replica: replica}
}

// Status is the durable status of an EPaxos instance.
type Status uint8

const (
	// StatusNone means the instance is unknown.
	StatusNone Status = iota
	// StatusPreAccepted means the pre-accept phase stored attributes.
	StatusPreAccepted
	// StatusAccepted means the slow accept phase stored attributes.
	StatusAccepted
	// StatusCommitted means the value and attributes are final.
	StatusCommitted
	// StatusExecuted means the command has been emitted to the application.
	StatusExecuted
)

// String returns a stable status name.
func (s Status) String() string {
	switch s {
	case StatusNone:
		return "none"
	case StatusPreAccepted:
		return "pre-accepted"
	case StatusAccepted:
		return "accepted"
	case StatusCommitted:
		return "committed"
	case StatusExecuted:
		return "executed"
	default:
		return "unknown"
	}
}

// CommandKind describes how the EPaxos core should treat a command.
type CommandKind uint8

const (
	// CommandUser is an application command.
	CommandUser CommandKind = iota
	// CommandNoop is a recovery command with no application effect.
	CommandNoop
	// CommandConfChange is a membership-change command.
	CommandConfChange
)

// CommandID is an application-supplied id used for duplicate detection by clients.
type CommandID struct {
	Client   uint64
	Sequence uint64
}

// Command is the application value proposed through EPaxos.
//
// Payload is opaque to the consensus core. ConflictKeys define the command's
// commutativity relation: two user commands conflict when any conflict key is
// byte-identical. Configuration changes conflict with every command. The core
// never mutates Payload or ConflictKeys. Propose clones these slices unless
// Config.ZeroCopyProposals is true.
type Command struct {
	ID           CommandID
	Kind         CommandKind
	Payload      []byte
	ConflictKeys [][]byte
}

// Reset releases references held by the command so it can be reused by a caller.
func (c *Command) Reset() {
	c.ID = CommandID{}
	c.Kind = CommandUser
	c.Payload = nil
	c.ConflictKeys = nil
}

// Clone returns a deep copy of the command.
func (c Command) Clone() Command {
	out := Command{ID: c.ID, Kind: c.Kind}
	if len(c.Payload) > 0 {
		out.Payload = append([]byte(nil), c.Payload...)
	}
	if len(c.ConflictKeys) > 0 {
		out.ConflictKeys = make([][]byte, len(c.ConflictKeys))
		for i := range c.ConflictKeys {
			out.ConflictKeys[i] = append([]byte(nil), c.ConflictKeys[i]...)
		}
	}
	return out
}

// Borrow returns the command unchanged when the caller transfers ownership of
// its Payload and ConflictKeys slices to the node.
func (c Command) Borrow() Command { return c }

// ConflictsWith reports whether two commands must be ordered by dependencies.
func (c Command) ConflictsWith(other Command) bool {
	if c.Kind == CommandNoop || other.Kind == CommandNoop {
		return false
	}
	if c.Kind == CommandConfChange || other.Kind == CommandConfChange {
		return true
	}
	for _, a := range c.ConflictKeys {
		for _, b := range other.ConflictKeys {
			if bytes.Equal(a, b) {
				return true
			}
		}
	}
	return false
}

// ConfChangeType identifies the membership operation in a configuration command.
type ConfChangeType uint8

const (
	// ConfChangeAddVoter adds a voting replica to future instances.
	ConfChangeAddVoter ConfChangeType = iota + 1
	// ConfChangeRemoveVoter removes a voting replica from future instances.
	ConfChangeRemoveVoter
)

// ConfChange is encoded into a CommandConfChange command by ProposeConfChange.
type ConfChange struct {
	Type    ConfChangeType
	Replica ReplicaID
}

// ConfState is a deterministic snapshot of voters at a configuration id.
type ConfState struct {
	ID     ConfID
	Voters []ReplicaID
}

// Clone returns a deep copy of the configuration state.
func (c ConfState) Clone() ConfState {
	out := ConfState{ID: c.ID}
	out.Voters = append(out.Voters, c.Voters...)
	return out
}

// Contains reports whether id is a voter in the configuration.
func (c ConfState) Contains(id ReplicaID) bool {
	_, ok := c.Index(id)
	return ok
}

// Index returns the stable dependency-vector index for a replica.
func (c ConfState) Index(id ReplicaID) (int, bool) {
	i := sort.Search(len(c.Voters), func(i int) bool { return c.Voters[i] >= id })
	return i, i < len(c.Voters) && c.Voters[i] == id
}

// Config configures a RawNode.
type Config struct {
	// ID is the local replica id and must be present in Voters.
	ID ReplicaID
	// Voters is the complete voting configuration for the initial cluster.
	Voters []ReplicaID
	// Storage persists hard state and instance records. A nil Storage selects
	// an in-memory implementation.
	Storage Storage
	// RetryTicks is the logical tick interval for protocol retransmission.
	RetryTicks uint64
	// RecoveryTicks is the logical tick interval for recovery attempts.
	RecoveryTicks uint64
	// TimeOptimization enables the EPaxos Revisited fast-wait optimization.
	TimeOptimization bool
	// TimeOptimizationTicks is the logical fast-wait duration before slow accept.
	TimeOptimizationTicks uint64
	// ZeroCopyProposals makes Propose retain command Payload and ConflictKeys
	// slices instead of cloning them. When true, the caller transfers ownership
	// of those slices and must not mutate or reuse them while they remain
	// observable through Ready or Status.
	ZeroCopyProposals bool
	// MaxReadyMessages caps Ready.Messages without capping durable records or
	// committed application commands. A value less than one leaves messages
	// uncapped.
	MaxReadyMessages int
}

// HardState is the durable node-level state loaded before instances.
type HardState struct {
	Conf ConfState
	Tick uint64
}

// InstanceRecord is the durable value for one EPaxos instance.
type InstanceRecord struct {
	Ref      InstanceRef
	Ballot   Ballot
	Status   Status
	Seq      uint64
	Deps     []InstanceNum
	Command  Command
	Checksum [32]byte
}

// Clone returns a deep copy of the instance record.
func (r InstanceRecord) Clone() InstanceRecord {
	out := r
	out.Deps = append([]InstanceNum(nil), r.Deps...)
	out.Command = r.Command.Clone()
	return out
}

// Attributes returns the ordering attributes carried by the record.
func (r InstanceRecord) Attributes() Attributes {
	return Attributes{Seq: r.Seq, Deps: append([]InstanceNum(nil), r.Deps...)}
}

// Attributes are EPaxos sequence/dependency ordering metadata.
type Attributes struct {
	Seq  uint64
	Deps []InstanceNum
}

// Clone returns a deep copy of the attributes.
func (a Attributes) Clone() Attributes {
	return Attributes{Seq: a.Seq, Deps: append([]InstanceNum(nil), a.Deps...)}
}

// Equal reports whether two attribute sets match exactly.
func (a Attributes) Equal(b Attributes) bool {
	if a.Seq != b.Seq || len(a.Deps) != len(b.Deps) {
		return false
	}
	for i := range a.Deps {
		if a.Deps[i] != b.Deps[i] {
			return false
		}
	}
	return true
}

// CommittedCommand is emitted to the application after dependency ordering closes.
type CommittedCommand struct {
	Ref     InstanceRef
	Seq     uint64
	Deps    []InstanceNum
	Command Command
}

// Clone returns a deep copy of the committed command.
func (c CommittedCommand) Clone() CommittedCommand {
	out := c
	out.Deps = append([]InstanceNum(nil), c.Deps...)
	out.Command = c.Command.Clone()
	return out
}

// Ready batches records to persist, messages to send, and commands to apply.
type Ready struct {
	Records   []InstanceRecord
	Messages  []Message
	Committed []CommittedCommand
	MustSync  bool
}

// Empty reports whether the ready batch contains no work.
func (r Ready) Empty() bool {
	return len(r.Records) == 0 && len(r.Messages) == 0 && len(r.Committed) == 0 && !r.MustSync
}

// Release clears slice headers so a Ready value can be reused by the caller.
func (r *Ready) Release() {
	for i := range r.Records {
		r.Records[i] = InstanceRecord{}
	}
	for i := range r.Messages {
		r.Messages[i].Reset()
	}
	for i := range r.Committed {
		r.Committed[i] = CommittedCommand{}
	}
	r.Records = nil
	r.Messages = nil
	r.Committed = nil
	r.MustSync = false
}

// StatusSnapshot is a copy-only view of node state for diagnostics and tests.
type StatusSnapshot struct {
	ID        ReplicaID
	Tick      uint64
	Conf      ConfState
	Instances []InstanceRecord
	Executed  []InstanceRef
}

var (
	// ErrInvalidConfig reports an unusable node configuration.
	ErrInvalidConfig = errors.New("epaxos: invalid config")
	// ErrInvalidMessage reports a malformed transport message.
	ErrInvalidMessage = errors.New("epaxos: invalid message")
	// ErrChecksumMismatch reports a failed BLAKE3 checksum validation.
	ErrChecksumMismatch = errors.New("epaxos: checksum mismatch")
	// ErrMessageRejected reports a stale, duplicate, or non-local message.
	ErrMessageRejected = errors.New("epaxos: message rejected")
	// ErrUnknownInstance reports a request for an instance not known locally.
	ErrUnknownInstance = errors.New("epaxos: unknown instance")
)
