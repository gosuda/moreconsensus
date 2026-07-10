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
	// RejectAcceptedTarget means TryPreAccept reached a replica that already
	// durably accepted the target instance. The rejection carries that exact
	// accepted tuple so the coordinator can abandon Try and restart Prepare.
	RejectAcceptedTarget
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

func cloneAcceptEvidenceInto(dst, src []AcceptEvidence) []AcceptEvidence {
	source := src
	if slicesPartiallyOverlap(dst, source) {
		snapshot := make([]AcceptEvidence, len(source))
		for i := range source {
			snapshot[i] = AcceptEvidence{
				Sender: source[i].Sender,
				Seq:    source[i].Seq,
				Deps:   append([]InstanceNum(nil), source[i].Deps...),
			}
		}
		source = snapshot
	}
	if cap(dst) < len(source) {
		grown := make([]AcceptEvidence, len(source))
		copy(grown, dst)
		dst = grown
	} else {
		dst = dst[:len(source)]
	}
	for i := range source {
		deps := cloneSliceInto(dst[i].Deps, source[i].Deps)
		dst[i] = AcceptEvidence{Sender: source[i].Sender, Seq: source[i].Seq, Deps: deps}
	}
	full := dst[:cap(dst)]
	for i := len(source); i < len(full); i++ {
		full[i] = AcceptEvidence{}
	}
	return dst
}

func cloneAcceptEvidence(in []AcceptEvidence) []AcceptEvidence {
	return cloneAcceptEvidenceInto(nil, in)
}

func mergeAcceptEvidenceEntries(dst, src []AcceptEvidence) []AcceptEvidence {
	out := cloneAcceptEvidence(dst)
	for _, ev := range src {
		if ev.Sender == 0 {
			continue
		}
		merged := false
		for i, existing := range out {
			if existing.Sender != ev.Sender {
				continue
			}
			attrs := mergeAttrs(
				Attributes{Seq: existing.Seq, Deps: existing.Deps},
				Attributes{Seq: ev.Seq, Deps: ev.Deps},
			)
			if out[i].Seq != attrs.Seq || !instanceNumsEqual(out[i].Deps, attrs.Deps) {
				out[i].Seq = attrs.Seq
				out[i].Deps = append([]InstanceNum(nil), attrs.Deps...)
			}
			merged = true
			break
		}
		if merged {
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
	// ProcessAt is pinned by the instance owner. Its clock domain is encoded
	// by ProcessAt together with TOQ and is authenticated by the checksum.
	ProcessAt uint64
	// TOQ marks a nonzero ProcessAt as caller-sampled TOQ time. False marks
	// nonzero ProcessAt as logical time. On an initial TOQ PreAccept, Seq and
	// Deps remain empty until ProcessTOQ closes the timestamp bucket.
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

// CloneInto deep-copies m into dst while reusing destination capacity.
func (m Message) CloneInto(dst *Message) {
	deps := cloneSliceInto(dst.Deps, m.Deps)
	acceptDeps := cloneSliceInto(dst.AcceptDeps, m.AcceptDeps)
	acceptEvidence := cloneAcceptEvidenceInto(dst.AcceptEvidence, m.AcceptEvidence)
	command := dst.Command
	m.Command.CloneInto(&command)
	*dst = m
	dst.Deps = deps
	dst.AcceptDeps = acceptDeps
	dst.AcceptEvidence = acceptEvidence
	dst.Command = command
}

// Clone returns a deep copy of the message.
func (m Message) Clone() Message {
	var out Message
	m.CloneInto(&out)
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
	if len(conf.Voters) == 0 || len(conf.Voters) > 7 {
		return ErrInvalidMessage
	}
	for i, voter := range conf.Voters {
		if voter == 0 || (i > 0 && conf.Voters[i-1] >= voter) {
			return ErrInvalidMessage
		}
	}
	if conf.ID == 0 ||
		m.Ref.Replica == 0 ||
		m.Ref.Instance == 0 ||
		m.Ref.Conf == 0 ||
		m.Ref.Conf != conf.ID {
		return ErrInvalidMessage
	}
	if m.From == 0 || m.To == 0 {
		return ErrInvalidMessage
	}
	if !conf.Contains(m.From) || !conf.Contains(m.To) || !conf.Contains(m.Ref.Replica) {
		return ErrMessageRejected
	}
	if m.Command.Kind > CommandMembership ||
		m.RecordStatus > StatusExecuted ||
		m.ConflictStatus > StatusExecuted ||
		m.RejectReason > RejectAcceptedTarget {
		return ErrInvalidMessage
	}
	if m.Command.Kind == CommandNoop && (m.ProcessAt != 0 || m.TOQ) {
		return ErrInvalidMessage
	}
	validBallot := func(ballot Ballot) bool {
		return ballot == (Ballot{}) || (ballot.Replica != 0 && conf.Contains(ballot.Replica))
	}
	if !validBallot(m.Ballot) || !validBallot(m.RecordBallot) || !validBallot(m.RejectHint) {
		return ErrInvalidMessage
	}
	if m.Type != MsgEvidenceResp && m.Ballot != (Ballot{}) && m.RecordBallot != (Ballot{}) && m.Ballot.Less(m.RecordBallot) {
		return ErrInvalidMessage
	}
	if m.Ballot == (Ballot{}) {
		return ErrInvalidMessage
	}
	switch m.Type {
	case MsgPreAccept:
		if m.Ballot.IsInitialFor(m.Ref.Replica) {
			if m.From != m.Ref.Replica {
				return ErrInvalidMessage
			}
		} else if !m.Ballot.IsRecovery() || m.From != m.Ballot.Replica {
			return ErrInvalidMessage
		}
	case MsgAccept, MsgPrepare, MsgTryPreAccept, MsgEvidence:
		if m.From != m.Ballot.Replica {
			return ErrInvalidMessage
		}
	}
	validRef := func(ref InstanceRef) bool {
		return ref.Replica != 0 && ref.Instance != 0 && ref.Conf == conf.ID && conf.Contains(ref.Replica)
	}
	conflictPresent := m.ConflictRef != (InstanceRef{})
	if conflictPresent && !validRef(m.ConflictRef) {
		return ErrInvalidMessage
	}
	if m.Type == MsgEvidence || m.Type == MsgEvidenceResp {
		if !conflictPresent {
			return ErrInvalidMessage
		}
	} else if conflictPresent && !(m.Type == MsgTryPreAcceptResp && m.Reject) {
		return ErrInvalidMessage
	}
	if m.IgnoreDependency != (TryPreAcceptIgnore{}) {
		if m.Type != MsgTryPreAccept || m.Reject || !validRef(m.IgnoreDependency.Ref) {
			return ErrInvalidMessage
		}
	}
	isResponse := m.Type == MsgPreAcceptResp ||
		m.Type == MsgAcceptResp ||
		m.Type == MsgPrepareResp ||
		m.Type == MsgTryPreAcceptResp ||
		m.Type == MsgEvidenceResp
	if isResponse && !m.Reject && m.To != m.Ballot.Replica {
		return ErrInvalidMessage
	}
	if m.Reject {
		if !isResponse || m.Type == MsgEvidenceResp {
			return ErrInvalidMessage
		}
		if m.Type != MsgTryPreAcceptResp {
			if m.RejectReason != RejectNone || conflictPresent || m.ConflictStatus != StatusNone {
				return ErrInvalidMessage
			}
		} else {
			switch m.RejectReason {
			case RejectNone, RejectStaleBallot:
				if m.RejectHint == (Ballot{}) || conflictPresent || m.ConflictStatus != StatusNone {
					return ErrInvalidMessage
				}
			case RejectCommittedConflict:
				if !conflictPresent || m.ConflictStatus < StatusCommitted || m.RejectHint != (Ballot{}) {
					return ErrInvalidMessage
				}
			case RejectUncommittedConflict:
				if !conflictPresent || (m.ConflictStatus != StatusPreAccepted && m.ConflictStatus != StatusAccepted) || m.RejectHint != (Ballot{}) {
					return ErrInvalidMessage
				}
			case RejectAcceptedTarget:
				if conflictPresent || m.ConflictStatus != StatusNone || m.RejectHint != (Ballot{}) {
					return ErrInvalidMessage
				}
			default:
				return ErrInvalidMessage
			}
		}
	} else if m.RejectReason != RejectNone || m.RejectHint != (Ballot{}) || (m.Type != MsgEvidence && m.Type != MsgEvidenceResp && m.ConflictStatus != StatusNone) {
		return ErrInvalidMessage
	}
	if m.TOQ {
		if m.Type == MsgPreAccept && m.Ballot.IsInitialFor(m.Ref.Replica) {
			if m.From != m.Ref.Replica ||
				m.RecordBallot != (Ballot{}) ||
				m.RecordStatus != StatusNone {
				return ErrInvalidMessage
			}
		} else {
			switch m.Type {
			case MsgAccept, MsgCommit, MsgPrepareResp, MsgTryPreAccept, MsgEvidenceResp:
			case MsgTryPreAcceptResp:
				if !m.Reject || m.RejectReason != RejectAcceptedTarget {
					return ErrInvalidMessage
				}
			default:
				return ErrInvalidMessage
			}
		}
	}
	if !m.Reject && (m.Type == MsgPreAccept || m.Type == MsgPreAcceptResp || m.Type == MsgAccept || m.Type == MsgCommit || m.Type == MsgPrepareResp || m.Type == MsgTryPreAccept || m.Type == MsgTryPreAcceptResp || m.Type == MsgEvidenceResp) {
		if m.Type == MsgPreAccept && m.TOQ && m.Ballot.IsInitialFor(m.Ref.Replica) {
			if m.Seq != 0 || len(m.Deps) != 0 {
				return ErrInvalidMessage
			}
		} else if len(m.Deps) != len(conf.Voters) {
			return ErrInvalidMessage
		}
	}
	if !m.Reject {
		switch m.Type {
		case MsgPreAccept:
			if !m.TOQ && m.Seq == 0 {
				return ErrInvalidMessage
			}
		case MsgAccept, MsgCommit, MsgTryPreAccept:
			if m.Seq == 0 {
				return ErrInvalidMessage
			}
		case MsgPreAcceptResp:
			if m.Seq == 0 {
				return ErrInvalidMessage
			}
		case MsgTryPreAcceptResp:
			if m.Seq == 0 || m.RecordStatus != StatusPreAccepted {
				return ErrInvalidMessage
			}
		case MsgPrepareResp, MsgEvidenceResp:
			if m.RecordStatus >= StatusPreAccepted && m.Seq == 0 {
				return ErrInvalidMessage
			}
		}
	}
	if (m.AcceptSeq == 0) != (len(m.AcceptDeps) == 0) {
		return ErrInvalidMessage
	}
	if len(m.AcceptDeps) > 0 && len(m.AcceptDeps) != len(conf.Voters) {
		return ErrInvalidMessage
	}
	if len(m.AcceptEvidence) > len(conf.Voters) {
		return ErrInvalidMessage
	}
	if len(m.AcceptEvidence) > 0 {
		switch m.Type {
		case MsgAccept, MsgAcceptResp, MsgCommit, MsgPrepareResp, MsgEvidenceResp:
		case MsgTryPreAcceptResp:
			if !m.Reject || m.RejectReason != RejectAcceptedTarget {
				return ErrInvalidMessage
			}
		default:
			return ErrInvalidMessage
		}
		var seen uint64
		var evidenceSeq [7]uint64
		var evidenceDeps [7][]InstanceNum
		for _, evidence := range m.AcceptEvidence {
			index, ok := conf.Index(evidence.Sender)
			if !ok || evidence.Seq == 0 || len(evidence.Deps) != len(conf.Voters) {
				return ErrInvalidMessage
			}
			bit := uint64(1) << uint(index)
			if seen&bit != 0 {
				if evidenceSeq[index] != evidence.Seq || !instanceNumsEqual(evidenceDeps[index], evidence.Deps) {
					return ErrInvalidMessage
				}
				continue
			}
			evidenceSeq[index] = evidence.Seq
			evidenceDeps[index] = evidence.Deps
			seen |= bit
		}
	}
	hasAcceptMetadata := m.AcceptSeq != 0 || len(m.AcceptDeps) != 0 || len(m.AcceptEvidence) != 0
	switch m.Type {
	case MsgAccept, MsgAcceptResp, MsgCommit, MsgPrepareResp, MsgEvidenceResp:
	case MsgTryPreAcceptResp:
		if hasAcceptMetadata && (!m.Reject || m.RejectReason != RejectAcceptedTarget) {
			return ErrInvalidMessage
		}
	default:
		if hasAcceptMetadata {
			return ErrInvalidMessage
		}
	}
	if (m.Type == MsgPrepareResp || m.Type == MsgEvidenceResp) &&
		m.RecordStatus < StatusAccepted &&
		hasAcceptMetadata {
		return ErrInvalidMessage
	}
	if m.ProcessAt != 0 {
		switch m.Type {
		case MsgPreAccept, MsgAccept, MsgCommit, MsgPrepareResp, MsgTryPreAccept, MsgTryPreAcceptResp, MsgEvidenceResp:
		default:
			return ErrInvalidMessage
		}
	}
	if m.FastPathEligible {
		switch m.Type {
		case MsgPreAcceptResp:
			if m.Reject || m.RecordStatus != StatusNone {
				return ErrInvalidMessage
			}
		case MsgPrepareResp, MsgEvidenceResp:
			if m.Reject ||
				m.RecordStatus != StatusPreAccepted ||
				m.RecordBallot != (Ballot{Replica: m.Ref.Replica}) {
				return ErrInvalidMessage
			}
		default:
			return ErrInvalidMessage
		}
	}
	if m.DepsCommitted != 0 && (m.Type != MsgPreAcceptResp || m.Reject || m.RecordStatus != StatusNone) {
		return ErrInvalidMessage
	}
	commandEmpty := m.Command.ID == (CommandID{}) &&
		m.Command.Kind == CommandUser &&
		len(m.Command.Payload) == 0 &&
		len(m.Command.ConflictKeys) == 0
	depsAllZero := func() bool {
		for _, dep := range m.Deps {
			if dep != 0 {
				return false
			}
		}
		return true
	}
	if m.Reject {
		timedAcceptedTarget := m.Type == MsgTryPreAcceptResp && m.RejectReason == RejectAcceptedTarget
		if m.FastPathEligible ||
			m.DepsCommitted != 0 ||
			(!timedAcceptedTarget && (m.ProcessAt != 0 || m.TOQ)) ||
			m.IgnoreDependency != (TryPreAcceptIgnore{}) {
			return ErrInvalidMessage
		}
		if m.Type == MsgTryPreAcceptResp && m.RejectReason == RejectAcceptedTarget {
			if !m.Ballot.IsRecovery() ||
				m.To != m.Ballot.Replica ||
				m.RejectHint != (Ballot{}) ||
				conflictPresent ||
				m.ConflictStatus != StatusNone ||
				m.RecordBallot == (Ballot{}) ||
				m.RecordStatus != StatusAccepted ||
				m.Seq == 0 ||
				len(m.Deps) != len(conf.Voters) {
				return ErrInvalidMessage
			}
			return nil
		}
		if len(m.Deps) != len(conf.Voters) ||
			!depsAllZero() ||
			m.RecordBallot != (Ballot{}) ||
			m.Seq != 0 ||
			!commandEmpty ||
			m.RecordStatus != StatusNone ||
			hasAcceptMetadata {
			return ErrInvalidMessage
		}
		switch m.Type {
		case MsgPreAcceptResp, MsgAcceptResp, MsgPrepareResp:
			if m.RejectReason != RejectNone || m.RejectHint == (Ballot{}) || m.Ballot != m.RejectHint {
				return ErrInvalidMessage
			}
		case MsgTryPreAcceptResp:
			switch m.RejectReason {
			case RejectStaleBallot:
				if m.RejectHint == (Ballot{}) || m.Ballot != m.RejectHint {
					return ErrInvalidMessage
				}
			case RejectCommittedConflict, RejectUncommittedConflict:
				if m.RejectHint != (Ballot{}) {
					return ErrInvalidMessage
				}
			default:
				return ErrInvalidMessage
			}
		default:
			return ErrInvalidMessage
		}
		return nil
	}
	switch m.Type {
	case MsgPreAccept:
		if m.RecordBallot != (Ballot{}) ||
			m.RecordStatus != StatusNone ||
			m.FastPathEligible ||
			m.DepsCommitted != 0 {
			return ErrInvalidMessage
		}
	case MsgPreAcceptResp:
		if m.RecordBallot != (Ballot{}) ||
			m.RecordStatus != StatusNone ||
			!commandEmpty ||
			hasAcceptMetadata {
			return ErrInvalidMessage
		}
	case MsgAccept:
		if m.RecordStatus != StatusNone || m.RecordBallot != (Ballot{}) {
			return ErrInvalidMessage
		}
	case MsgAcceptResp:
		if m.RecordStatus != StatusAccepted ||
			m.RecordBallot == (Ballot{}) ||
			m.RecordBallot != m.Ballot ||
			m.Seq == 0 ||
			len(m.Deps) != len(conf.Voters) ||
			!commandEmpty {
			return ErrInvalidMessage
		}
	case MsgCommit:
		if m.RecordStatus != StatusNone || m.RecordBallot == (Ballot{}) {
			return ErrInvalidMessage
		}
	case MsgPrepare:
		if !m.Ballot.IsRecovery() ||
			m.RecordBallot != (Ballot{}) ||
			m.RecordStatus != StatusNone ||
			m.Seq != 0 ||
			len(m.Deps) != 0 ||
			!commandEmpty ||
			hasAcceptMetadata ||
			m.FastPathEligible ||
			m.DepsCommitted != 0 {
			return ErrInvalidMessage
		}
	case MsgPrepareResp:
		if !m.Ballot.IsRecovery() {
			return ErrInvalidMessage
		}
		if m.RecordStatus == StatusNone {
			if m.RecordBallot != (Ballot{}) ||
				m.Seq != 0 ||
				!depsAllZero() ||
				!commandEmpty ||
				hasAcceptMetadata ||
				m.FastPathEligible {
				return ErrInvalidMessage
			}
		} else if m.RecordBallot == (Ballot{}) || m.Seq == 0 {
			return ErrInvalidMessage
		}
	case MsgTryPreAccept:
		if !m.Ballot.IsRecovery() ||
			m.RecordStatus != StatusNone ||
			m.RecordBallot != (Ballot{}) {
			return ErrInvalidMessage
		}
	case MsgTryPreAcceptResp:
		if m.RecordStatus != StatusPreAccepted ||
			m.RecordBallot != (Ballot{}) ||
			!commandEmpty {
			return ErrInvalidMessage
		}
	case MsgEvidence:
		if m.RecordBallot != (Ballot{}) ||
			m.Seq != 0 ||
			len(m.Deps) != 0 ||
			!commandEmpty ||
			m.RecordStatus != StatusNone ||
			hasAcceptMetadata ||
			m.FastPathEligible {
			return ErrInvalidMessage
		}
	case MsgEvidenceResp:
		if m.RecordStatus == StatusNone {
			if m.RecordBallot != (Ballot{}) ||
				m.Seq != 0 ||
				!depsAllZero() ||
				!commandEmpty ||
				hasAcceptMetadata ||
				m.FastPathEligible {
				return ErrInvalidMessage
			}
		} else if m.RecordBallot == (Ballot{}) || m.Seq == 0 {
			return ErrInvalidMessage
		}
	}
	if m.DepsCommitted>>uint(len(conf.Voters)) != 0 || (m.DepsCommitted != 0 && m.Type != MsgPreAcceptResp) {
		return ErrInvalidMessage
	}
	return nil
}
