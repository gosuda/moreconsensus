package epaxos

// MessageType identifies an EPaxos transport message.
type MessageType uint8

const (
	// MsgPreAccept starts the EPaxos fast path.
	MsgPreAccept MessageType = iota + 1
	// MsgPreAcceptResp answers MsgPreAccept with local attributes.
	MsgPreAcceptResp
	// MsgAccept starts or continues the slow path.
	MsgAccept
	// MsgAcceptResp answers MsgAccept.
	MsgAcceptResp
	// MsgCommit announces a committed value and attributes.
	MsgCommit
	// MsgPrepare starts recovery for a stalled instance.
	MsgPrepare
	// MsgPrepareResp returns the highest local state for recovery.
	MsgPrepareResp
)

// String returns a stable message type name.
func (t MessageType) String() string {
	switch t {
	case MsgPreAccept:
		return "pre-accept"
	case MsgPreAcceptResp:
		return "pre-accept-resp"
	case MsgAccept:
		return "accept"
	case MsgAcceptResp:
		return "accept-resp"
	case MsgCommit:
		return "commit"
	case MsgPrepare:
		return "prepare"
	case MsgPrepareResp:
		return "prepare-resp"
	default:
		return "unknown"
	}
}

// Message is the wire-level EPaxos transport value.
type Message struct {
	Type         MessageType
	From         ReplicaID
	To           ReplicaID
	Ref          InstanceRef
	Ballot       Ballot
	Seq          uint64
	Deps         []InstanceNum
	Command      Command
	Reject       bool
	RejectHint   Ballot
	RecordStatus Status
	Checksum     [32]byte
}

// Reset releases references held by the message so it can be reused.
func (m *Message) Reset() {
	*m = Message{}
}

// Clone returns a deep copy of the message.
func (m Message) Clone() Message {
	out := m
	out.Deps = append([]InstanceNum(nil), m.Deps...)
	out.Command = m.Command.Clone()
	return out
}

// Attributes returns the message ordering attributes.
func (m Message) Attributes() Attributes {
	return Attributes{Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...)}
}

// Validate checks basic envelope constraints against the supplied configuration.
func (m Message) Validate(conf ConfState) error {
	if m.Type < MsgPreAccept || m.Type > MsgPrepareResp {
		return ErrInvalidMessage
	}
	if m.From == 0 || m.To == 0 || m.Ref.IsZero() {
		return ErrInvalidMessage
	}
	if !conf.Contains(m.From) || !conf.Contains(m.To) {
		return ErrMessageRejected
	}
	if !m.Reject && (m.Type == MsgPreAccept || m.Type == MsgPreAcceptResp || m.Type == MsgAccept || m.Type == MsgCommit || m.Type == MsgPrepareResp) {
		if len(m.Deps) != len(conf.Voters) {
			return ErrInvalidMessage
		}
	}
	return nil
}
