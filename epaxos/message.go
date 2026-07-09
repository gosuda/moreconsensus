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
	// MsgTryPreAccept validates a possible optimized fast-path tuple during recovery.
	MsgTryPreAccept
	// MsgTryPreAcceptResp answers MsgTryPreAccept.
	MsgTryPreAcceptResp
	// MsgEvidence reads a committed conflict instance's recovery evidence without
	// changing ballots or promises.
	MsgEvidence
	// MsgEvidenceResp answers MsgEvidence with the local record, if any.
	MsgEvidenceResp
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
	case MsgTryPreAccept:
		return "try-pre-accept"
	case MsgTryPreAcceptResp:
		return "try-pre-accept-resp"
	case MsgEvidence:
		return "evidence"
	case MsgEvidenceResp:
		return "evidence-resp"
	default:
		return "unknown"
	}
}

// RejectReason classifies TryPreAccept recovery rejections.
type RejectReason uint8

const (
	RejectNone RejectReason = iota
	RejectStaleBallot
	RejectCommittedConflict
	RejectUncommittedConflict
)

// AcceptEvidence records recovery-only Accept-Deps evidence together with the
// original Accept or AcceptReply sender. The evidence is tied to the containing
// instance tuple; callers must not reinterpret it for a different value.
type AcceptEvidence struct {
	Sender ReplicaID
	Seq    uint64
	Deps   []InstanceNum
}

// TryPreAcceptIgnore describes a committed dependency a recovery coordinator
// has proven safe to ignore for one TryPreAccept resend. The zero value means no
// ignore marker.
type TryPreAcceptIgnore struct {
	Ref InstanceRef
}

func cloneAcceptEvidence(in []AcceptEvidence) []AcceptEvidence {
	out := append([]AcceptEvidence(nil), in...)
	for i := range out {
		out[i].Deps = append([]InstanceNum(nil), out[i].Deps...)
	}
	return out
}

func mergeAcceptEvidenceEntries(dst, src []AcceptEvidence) []AcceptEvidence {
	out := cloneAcceptEvidence(dst)
	for _, ev := range src {
		if ev.Sender == 0 {
			continue
		}
		duplicate := false
		for _, existing := range out {
			if existing.Sender != ev.Sender {
				continue
			}
			duplicate = true
			if existing.Seq != ev.Seq || !instanceNumsEqual(existing.Deps, ev.Deps) {
				out = append(out, AcceptEvidence{Sender: ev.Sender, Seq: ev.Seq, Deps: append([]InstanceNum(nil), ev.Deps...)})
			}
			break
		}
		if duplicate {
			continue
		}
		out = append(out, AcceptEvidence{Sender: ev.Sender, Seq: ev.Seq, Deps: append([]InstanceNum(nil), ev.Deps...)})
	}
	return out
}

// Message is the wire-level EPaxos transport value.
type Message struct {
	Type MessageType
	From ReplicaID
	To   ReplicaID
	Ref  InstanceRef
	// ProcessAt is the logical or TOQ timestamp at or after which a receiver
	// should process MsgPreAccept. Zero means process immediately.
	ProcessAt uint64
	// TOQ marks an EPaxos Revisited Timestamp-Ordered Queueing PreAccept. TOQ
	// PreAccepts intentionally carry Seq=0 and no deps; receivers compute attrs
	// at ProcessAt instead of merging legacy proposer attrs.
	TOQ    bool
	Ballot Ballot
	// RecordBallot is the pre-promise durable ballot for MsgPrepareResp
	// recovery evidence. Ballot remains the promise/reject ballot.
	RecordBallot Ballot
	Seq          uint64
	Deps         []InstanceNum
	// AcceptSeq/AcceptDeps are legacy aggregate recovery evidence. AcceptEvidence
	// preserves original Accept/AcceptReply senders for sender-sensitive recovery.
	AcceptSeq        uint64
	AcceptDeps       []InstanceNum
	AcceptEvidence   []AcceptEvidence
	IgnoreDependency TryPreAcceptIgnore
	Command          Command
	Reject           bool
	RejectReason     RejectReason
	RejectHint       Ballot
	ConflictRef      InstanceRef
	ConflictStatus   Status
	FastPathEligible bool
	// DepsCommitted is a bitset over Deps/Voters. Bit i means this sender has
	// durably recorded every implicit dependency through Deps[i] as committed or executed.
	DepsCommitted uint64
	RecordStatus  Status
	Checksum      [32]byte
}

// Reset releases references held by the message so it can be reused.
func (m *Message) Reset() {
	*m = Message{}
}

// Clone returns a deep copy of the message.
func (m Message) Clone() Message {
	out := m
	out.Deps = append([]InstanceNum(nil), m.Deps...)
	out.AcceptDeps = append([]InstanceNum(nil), m.AcceptDeps...)
	out.AcceptEvidence = cloneAcceptEvidence(m.AcceptEvidence)
	out.Command = m.Command.Clone()
	return out
}

// Attributes returns the message ordering attributes.
func (m Message) Attributes() Attributes {
	return Attributes{Seq: m.Seq, Deps: append([]InstanceNum(nil), m.Deps...)}
}

// AcceptAttributes returns recovery-only Accept-Deps evidence carried by the
// message. When no separate evidence is present, callers should fall back to
// Attributes for the message's chosen attributes.
func (m Message) AcceptAttributes() (Attributes, bool) {
	if m.AcceptSeq == 0 || len(m.AcceptDeps) == 0 {
		return Attributes{}, false
	}
	return Attributes{Seq: m.AcceptSeq, Deps: append([]InstanceNum(nil), m.AcceptDeps...)}, true
}

// Validate checks basic envelope constraints against the supplied configuration.
func (m Message) Validate(conf ConfState) error {
	if m.Type < MsgPreAccept || m.Type > MsgEvidenceResp {
		return ErrInvalidMessage
	}
	if m.TOQ && m.Type != MsgPreAccept {
		return ErrInvalidMessage
	}
	if m.From == 0 || m.To == 0 || m.Ref.IsZero() {
		return ErrInvalidMessage
	}
	if !conf.Contains(m.From) || !conf.Contains(m.To) {
		return ErrMessageRejected
	}
	if (m.Type == MsgEvidence || m.Type == MsgEvidenceResp) && (m.ConflictRef.IsZero() || m.ConflictRef.Conf != m.Ref.Conf || !conf.Contains(m.ConflictRef.Replica)) {
		return ErrInvalidMessage
	}
	if !m.Reject && (m.Type == MsgPreAccept || m.Type == MsgPreAcceptResp || m.Type == MsgAccept || m.Type == MsgCommit || m.Type == MsgPrepareResp || m.Type == MsgTryPreAccept || m.Type == MsgTryPreAcceptResp || m.Type == MsgEvidenceResp) {
		if m.Type == MsgPreAccept && m.TOQ {
			if m.Seq != 0 || len(m.Deps) != 0 {
				return ErrInvalidMessage
			}
		} else if len(m.Deps) != len(conf.Voters) {
			return ErrInvalidMessage
		}
	}
	if (m.AcceptSeq == 0) != (len(m.AcceptDeps) == 0) {
		return ErrInvalidMessage
	}
	if len(m.AcceptDeps) > 0 && len(m.AcceptDeps) != len(conf.Voters) {
		return ErrInvalidMessage
	}
	if len(m.AcceptEvidence) > 0 {
		switch m.Type {
		case MsgAccept, MsgAcceptResp, MsgCommit, MsgPrepareResp, MsgTryPreAcceptResp, MsgEvidenceResp:
		default:
			return ErrInvalidMessage
		}
		seen := make(map[ReplicaID]AcceptEvidence, len(m.AcceptEvidence))
		for _, ev := range m.AcceptEvidence {
			if ev.Sender == 0 || !conf.Contains(ev.Sender) || ev.Seq == 0 || len(ev.Deps) != len(conf.Voters) {
				return ErrInvalidMessage
			}
			if existing, ok := seen[ev.Sender]; ok {
				if existing.Seq != ev.Seq || !instanceNumsEqual(existing.Deps, ev.Deps) {
					return ErrInvalidMessage
				}
				continue
			}
			seen[ev.Sender] = ev
		}
	}
	if m.IgnoreDependency != (TryPreAcceptIgnore{}) {
		if m.Type != MsgTryPreAccept || m.IgnoreDependency.Ref.Conf != m.Ref.Conf || !conf.Contains(m.IgnoreDependency.Ref.Replica) {
			return ErrInvalidMessage
		}
	}
	if m.DepsCommitted>>uint(len(conf.Voters)) != 0 {
		return ErrInvalidMessage
	}
	return nil
}
