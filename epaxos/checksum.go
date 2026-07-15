package epaxos

import (
	"encoding/binary"
	"sync"

	"github.com/zeebo/blake3"
)

var checksumPool = sync.Pool{New: func() any { return blake3.New() }}

func getHash() *blake3.Hasher {
	h := checksumPool.Get().(*blake3.Hasher)
	h.Reset()
	return h
}

func putHash(h *blake3.Hasher) { checksumPool.Put(h) }

func writeByte(h *blake3.Hasher, v byte) {
	var b [1]byte
	b[0] = v
	_, _ = h.Write(b[:])
}

func writeUint64(h *blake3.Hasher, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, _ = h.Write(b[:])
}

func writeBytes(h *blake3.Hasher, b []byte) {
	writeUint64(h, uint64(len(b)))
	_, _ = h.Write(b)
}

func writeFootprint(h *blake3.Hasher, f Footprint) {
	if f.All {
		writeByte(h, 1)
	} else {
		writeByte(h, 0)
	}
	writeUint64(h, uint64(len(f.Points)))
	for _, point := range f.Points {
		writeBytes(h, point)
	}
	writeUint64(h, uint64(len(f.Spans)))
	for _, span := range f.Spans {
		writeBytes(h, span.Start)
		writeBytes(h, span.End)
	}
}

func writeCommand(h *blake3.Hasher, c Command) {
	writeUint64(h, c.ID.Client)
	writeUint64(h, c.ID.Sequence)
	writeBytes(h, c.Payload)
	writeFootprint(h, c.Footprint)
	writeBytes(h, c.CycleKey)
}

func writeEntryValue(h *blake3.Hasher, kind EntryKind, command Command, change ConfChange, control []byte) {
	writeByte(h, byte(kind))
	writeByte(h, byte(change.Type))
	writeUint64(h, uint64(change.Replica))
	writeBytes(h, control)
	writeCommand(h, command)
}

func writeLegacyCommand(h *blake3.Hasher, r InstanceRecord) {
	var kind byte
	var id CommandID
	var payload []byte
	var points [][]byte
	switch r.Kind {
	case EntryCommand:
		kind, id, payload, points = 0, r.Command.ID, r.Command.Payload, r.Command.Footprint.Points
	case EntryNoop:
		kind = 1
	case EntryConfChange:
		kind = 2
		payload = make([]byte, 9)
		payload[0] = byte(r.ConfChange.Type)
		binary.LittleEndian.PutUint64(payload[1:], uint64(r.ConfChange.Replica))
	case EntryMembership:
		kind, payload = 3, r.ProtocolControl
	case EntryCheckpoint:
		kind, payload = byte(EntryCheckpoint), r.ProtocolControl
	default:
		kind = byte(r.Kind)
	}
	writeUint64(h, id.Client)
	writeUint64(h, id.Sequence)
	writeByte(h, kind)
	writeBytes(h, payload)
	writeUint64(h, uint64(len(points)))
	for _, point := range points {
		writeBytes(h, point)
	}
}

func writeRef(h *blake3.Hasher, r InstanceRef) {
	writeUint64(h, uint64(r.Replica))
	writeUint64(h, uint64(r.Instance))
	writeUint64(h, uint64(r.Conf))
}

func writeBallot(h *blake3.Hasher, b Ballot) {
	writeUint64(h, b.Epoch)
	writeUint64(h, b.Number)
	writeUint64(h, uint64(b.Replica))
}

func sumHash(h *blake3.Hasher) [32]byte {
	var out [32]byte
	s := h.Sum(out[:0])
	copy(out[:], s)
	return out
}

// ChecksumRecord returns the canonical BLAKE3 checksum for a record.
func ChecksumRecord(r InstanceRecord) [32]byte {
	return checksumRecordLayout(r, true, true, true, true, true, true, true, true)
}

// ChecksumRecordWithoutMembershipResult returns the immediately previous
// durable record layout. It is exposed only for exact storage migration.
func ChecksumRecordWithoutMembershipResult(r InstanceRecord) [32]byte {
	return checksumLegacyRecordLayout(r, true, true, true, true, true, true, true)
}

// ChecksumRecordWithoutTimingDomain returns the older pre-timing durable
// record layout. It is exposed only for exact storage migration.
func ChecksumRecordWithoutTimingDomain(r InstanceRecord) [32]byte {
	return checksumLegacyRecordLayout(r, true, true, true, true, true, true, false)
}

// checksumRecord retains the pre-TimingDomain durable checksum layout for
// older exact-layout migration verifiers.
func checksumRecord(r InstanceRecord, includeFastPath, includeTOQ, includeAcceptEvidence, includeRecordBallot, includeSenderEvidence, includeConfChangeResult bool) [32]byte {
	return checksumLegacyRecordLayout(r, includeFastPath, includeTOQ, includeAcceptEvidence,
		includeRecordBallot, includeSenderEvidence, includeConfChangeResult, false)
}

func checksumLegacyRecordLayout(r InstanceRecord, includeFastPath, includeTOQ, includeAcceptEvidence, includeRecordBallot, includeSenderEvidence, includeConfChangeResult, includeTimingDomain bool) [32]byte {
	h := getHash()
	defer putHash(h)
	writeRef(h, r.Ref)
	writeBallot(h, r.Ballot)
	if includeRecordBallot {
		writeBallot(h, r.RecordBallot)
	}
	writeByte(h, byte(r.Status))
	writeUint64(h, r.Seq)
	writeUint64(h, uint64(len(r.Deps)))
	for _, dep := range r.Deps {
		writeUint64(h, uint64(dep))
	}
	if includeAcceptEvidence {
		writeUint64(h, r.AcceptSeq)
		writeUint64(h, uint64(len(r.AcceptDeps)))
		for _, dep := range r.AcceptDeps {
			writeUint64(h, uint64(dep))
		}
	}
	if includeSenderEvidence {
		writeUint64(h, uint64(len(r.AcceptEvidence)))
		for _, evidence := range r.AcceptEvidence {
			writeUint64(h, uint64(evidence.Sender))
			writeUint64(h, evidence.Seq)
			writeUint64(h, uint64(len(evidence.Deps)))
			for _, dep := range evidence.Deps {
				writeUint64(h, uint64(dep))
			}
		}
	}
	writeLegacyCommand(h, r)
	if includeFastPath {
		if r.FastPathEligible {
			writeByte(h, 1)
		} else {
			writeByte(h, 0)
		}
	}
	if includeTOQ {
		writeUint64(h, r.ProcessAt)
		if r.TOQPending {
			writeByte(h, 1)
		} else {
			writeByte(h, 0)
		}
	}
	if includeTimingDomain {
		writeByte(h, byte(r.TimingDomain))
	}
	if includeConfChangeResult {
		writeByte(h, byte(r.ConfChangeResult.Outcome))
		writeUint64(h, uint64(r.ConfChangeResult.Conf.ID))
		writeUint64(h, uint64(len(r.ConfChangeResult.Conf.Voters)))
		for _, voter := range r.ConfChangeResult.Conf.Voters {
			writeUint64(h, uint64(voter))
		}
	}
	return sumHash(h)
}

func checksumRecordLayout(r InstanceRecord, includeFastPath, includeTOQ, includeAcceptEvidence, includeRecordBallot, includeSenderEvidence, includeConfChangeResult, includeTimingDomain, includeMembershipResult bool) [32]byte {
	h := getHash()
	defer putHash(h)
	writeRef(h, r.Ref)
	writeBallot(h, r.Ballot)
	if includeRecordBallot {
		writeBallot(h, r.RecordBallot)
	}
	writeByte(h, byte(r.Status))
	writeUint64(h, r.Seq)
	writeUint64(h, uint64(len(r.Deps)))
	for _, dep := range r.Deps {
		writeUint64(h, uint64(dep))
	}
	if includeAcceptEvidence {
		writeUint64(h, r.AcceptSeq)
		writeUint64(h, uint64(len(r.AcceptDeps)))
		for _, dep := range r.AcceptDeps {
			writeUint64(h, uint64(dep))
		}
	}
	if includeSenderEvidence {
		writeUint64(h, uint64(len(r.AcceptEvidence)))
		for _, ev := range r.AcceptEvidence {
			writeUint64(h, uint64(ev.Sender))
			writeUint64(h, ev.Seq)
			writeUint64(h, uint64(len(ev.Deps)))
			for _, dep := range ev.Deps {
				writeUint64(h, uint64(dep))
			}
		}
	}
	writeEntryValue(h, r.Kind, r.Command, r.ConfChange, r.ProtocolControl)
	if includeFastPath {
		if r.FastPathEligible {
			writeByte(h, 1)
		} else {
			writeByte(h, 0)
		}
	}
	if includeTOQ {
		writeUint64(h, r.ProcessAt)
		if r.TOQPending {
			writeByte(h, 1)
		} else {
			writeByte(h, 0)
		}
	}
	if includeTimingDomain {
		writeByte(h, byte(r.TimingDomain))
	}
	if includeConfChangeResult {
		writeByte(h, byte(r.ConfChangeResult.Outcome))
		writeUint64(h, uint64(r.ConfChangeResult.Conf.ID))
		writeUint64(h, uint64(len(r.ConfChangeResult.Conf.Voters)))
		for _, voter := range r.ConfChangeResult.Conf.Voters {
			writeUint64(h, uint64(voter))
		}
	}
	if includeMembershipResult {
		writeBytes(h, r.MembershipResult.Plan[:])
		writeByte(h, byte(r.MembershipResult.Outcome))
		writeRef(h, r.MembershipResult.ExitRef)
		writeBytes(h, r.MembershipResult.CertificateDigest[:])
		writeUint64(h, uint64(r.MembershipResult.Successor.ID))
		writeUint64(h, uint64(len(r.MembershipResult.Successor.Voters)))
		for _, voter := range r.MembershipResult.Successor.Voters {
			writeUint64(h, uint64(voter))
		}
	}
	return sumHash(h)
}

func recordTimingInvariant(r InstanceRecord) bool {
	if r.Kind == EntryNoop {
		return r.ProcessAt == 0 && r.TimingDomain == TimingDomainUntimed && !r.TOQPending
	}
	switch r.TimingDomain {
	case TimingDomainUntimed:
		return r.ProcessAt == 0 && !r.TOQPending
	case TimingDomainLogical:
		return r.ProcessAt != 0 && !r.TOQPending
	case TimingDomainTOQ:
		return true
	default:
		return false
	}
}

func recordChecksumInvariant(r InstanceRecord) bool {
	if r.Status >= StatusPreAccepted {
		return r.RecordBallot != (Ballot{})
	}
	return r.RecordBallot == (Ballot{})
}

func confChangeResultAbsent(r InstanceRecord) bool {
	return r.ConfChangeResult.Outcome == ConfChangeOutcomeUnspecified &&
		r.ConfChangeResult.Conf.ID == 0 &&
		len(r.ConfChangeResult.Conf.Voters) == 0
}

func membershipResultAbsent(r InstanceRecord) bool {
	return r.MembershipResult.Plan == (BootstrapID{}) &&
		r.MembershipResult.Outcome == BootstrapOutcomeUnspecified &&
		r.MembershipResult.ExitRef.IsZero() &&
		r.MembershipResult.CertificateDigest == (StateDigest{}) &&
		confStateIsZero(r.MembershipResult.Successor)
}

func acceptEvidenceAbsent(r InstanceRecord) bool {
	return r.AcceptSeq == 0 && len(r.AcceptDeps) == 0 && len(r.AcceptEvidence) == 0
}

func verifyCanonicalRecordChecksumBytes(r InstanceRecord) bool {
	return recordChecksumInvariant(r) && r.Checksum == ChecksumRecord(r)
}

// VerifyRecordChecksum reports whether the record checksum matches its content.
func VerifyRecordChecksum(r InstanceRecord) bool {
	return recordTimingInvariant(r) && verifyCanonicalRecordChecksumBytes(r)
}

// VerifyRecordChecksumWithoutMembershipResult reports whether a record matches
// the immediately previous canonical layout.
func VerifyRecordChecksumWithoutMembershipResult(r InstanceRecord) bool {
	return membershipResultAbsent(r) &&
		recordTimingInvariant(r) &&
		recordChecksumInvariant(r) &&
		r.Checksum == ChecksumRecordWithoutMembershipResult(r)
}

// VerifyRecordChecksumWithoutTimingDomain reports whether a record matches the
// immediately previous canonical layout that authenticated every current field
// except TimingDomain.
func VerifyRecordChecksumWithoutTimingDomain(r InstanceRecord) bool {
	return r.TimingDomain == TimingDomainUntimed &&
		recordChecksumInvariant(r) &&
		r.Checksum == ChecksumRecordWithoutTimingDomain(r)
}

// VerifyRecordChecksumWithoutConfChangeResult reports whether a record matches
// the checksum format used before durable configuration-command outcomes.
func VerifyRecordChecksumWithoutConfChangeResult(r InstanceRecord) bool {
	return r.TimingDomain == TimingDomainUntimed &&
		confChangeResultAbsent(r) &&
		recordChecksumInvariant(r) &&
		r.Checksum == checksumRecord(r, true, true, true, true, true, false)
}

// VerifyRecordChecksumWithoutAcceptEvidence reports whether a record matches
// the checksum format used before durable Accept-Deps recovery evidence was
// added. It exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutAcceptEvidence(r InstanceRecord) bool {
	return r.TimingDomain == TimingDomainUntimed &&
		confChangeResultAbsent(r) &&
		r.RecordBallot == (Ballot{}) &&
		acceptEvidenceAbsent(r) &&
		r.Checksum == checksumRecord(r, true, true, false, false, false, false)
}

// VerifyRecordChecksumWithoutSenderAcceptEvidence reports whether a record
// matches the checksum format used before sender-preserving Accept-Deps evidence.
// It exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutSenderAcceptEvidence(r InstanceRecord) bool {
	return r.TimingDomain == TimingDomainUntimed &&
		confChangeResultAbsent(r) &&
		len(r.AcceptEvidence) == 0 &&
		recordChecksumInvariant(r) &&
		r.Checksum == checksumRecord(r, true, true, true, true, false, false)
}

// VerifyRecordChecksumWithoutRecordBallot reports whether a record matches
// the checksum format used before durable record/value ballots were added. It
// exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutRecordBallot(r InstanceRecord) bool {
	return r.TimingDomain == TimingDomainUntimed &&
		confChangeResultAbsent(r) &&
		r.RecordBallot == (Ballot{}) &&
		len(r.AcceptEvidence) == 0 &&
		r.Checksum == checksumRecord(r, true, true, true, false, false, false)
}

// VerifyRecordChecksumWithoutTOQ reports whether a record matches the checksum
// format used before durable TOQ ProcessAt/pending metadata was added. It exists
// only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutTOQ(r InstanceRecord) bool {
	return r.TimingDomain == TimingDomainUntimed &&
		confChangeResultAbsent(r) &&
		r.RecordBallot == (Ballot{}) &&
		acceptEvidenceAbsent(r) &&
		r.ProcessAt == 0 &&
		!r.TOQPending &&
		r.Checksum == checksumRecord(r, true, false, false, false, false, false)
}

// VerifyRecordChecksumWithoutFastPathOrTOQ reports whether a record matches the
// checksum format used before durable fast-path and TOQ metadata were added. It
// exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutFastPathOrTOQ(r InstanceRecord) bool {
	return r.TimingDomain == TimingDomainUntimed &&
		confChangeResultAbsent(r) &&
		r.RecordBallot == (Ballot{}) &&
		acceptEvidenceAbsent(r) &&
		!r.FastPathEligible &&
		r.ProcessAt == 0 &&
		!r.TOQPending &&
		r.Checksum == checksumRecord(r, false, false, false, false, false, false)
}

// ChecksumMessage returns the canonical BLAKE3 checksum for a message.
func ChecksumMessage(m Message) [32]byte {
	h := getHash()
	defer putHash(h)
	writeByte(h, byte(m.Type))
	writeUint64(h, uint64(m.From))
	writeUint64(h, uint64(m.To))
	writeUint64(h, m.FromIncarnation)
	writeUint64(h, m.ToIncarnation)
	writeRef(h, m.Ref)
	writeUint64(h, m.ProcessAt)
	if m.TOQ {
		writeByte(h, 1)
	} else {
		writeByte(h, 0)
	}
	writeBallot(h, m.Ballot)
	writeBallot(h, m.RecordBallot)
	writeUint64(h, m.Seq)
	writeUint64(h, uint64(len(m.Deps)))
	for _, dep := range m.Deps {
		writeUint64(h, uint64(dep))
	}
	writeUint64(h, m.AcceptSeq)
	writeUint64(h, uint64(len(m.AcceptDeps)))
	for _, dep := range m.AcceptDeps {
		writeUint64(h, uint64(dep))
	}
	writeUint64(h, uint64(len(m.AcceptEvidence)))
	for _, ev := range m.AcceptEvidence {
		writeUint64(h, uint64(ev.Sender))
		writeUint64(h, ev.Seq)
		writeUint64(h, uint64(len(ev.Deps)))
		for _, dep := range ev.Deps {
			writeUint64(h, uint64(dep))
		}
	}
	writeRef(h, m.IgnoreDependency.Ref)
	writeEntryValue(h, m.Kind, m.Command, m.ConfChange, m.ProtocolControl)
	if m.Reject {
		writeByte(h, 1)
	} else {
		writeByte(h, 0)
	}
	writeBallot(h, m.RejectHint)
	writeByte(h, byte(m.RejectReason))
	writeRef(h, m.ConflictRef)
	writeByte(h, byte(m.ConflictStatus))
	if m.FastPathEligible {
		writeByte(h, 1)
	} else {
		writeByte(h, 0)
	}
	writeUint64(h, m.DepsCommitted)
	writeByte(h, byte(m.RecordStatus))
	return sumHash(h)
}

// VerifyMessageChecksum reports whether the message checksum matches its content.
func VerifyMessageChecksum(m Message) bool { return m.Checksum == ChecksumMessage(m) }
