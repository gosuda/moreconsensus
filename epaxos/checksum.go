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

func writeCommand(h *blake3.Hasher, c Command) {
	writeUint64(h, c.ID.Client)
	writeUint64(h, c.ID.Sequence)
	writeByte(h, byte(c.Kind))
	writeBytes(h, c.Payload)
	writeUint64(h, uint64(len(c.ConflictKeys)))
	for _, key := range c.ConflictKeys {
		writeBytes(h, key)
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
	return checksumRecord(r, true, true, true, true, true)
}

func checksumRecord(r InstanceRecord, includeFastPath, includeTOQ, includeAcceptEvidence, includeRecordBallot, includeSenderEvidence bool) [32]byte {
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
	writeCommand(h, r.Command)
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
	return sumHash(h)
}

func recordChecksumInvariant(r InstanceRecord) bool {
	if r.Status >= StatusPreAccepted {
		return r.RecordBallot != (Ballot{})
	}
	return r.RecordBallot == (Ballot{})
}

// VerifyRecordChecksum reports whether the record checksum matches its content.
func VerifyRecordChecksum(r InstanceRecord) bool {
	return recordChecksumInvariant(r) && r.Checksum == ChecksumRecord(r)
}

// VerifyRecordChecksumWithoutAcceptEvidence reports whether a record matches
// the checksum format used before durable Accept-Deps recovery evidence was
// added. It exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutAcceptEvidence(r InstanceRecord) bool {
	return r.Checksum == checksumRecord(r, true, true, false, false, false)
}

// VerifyRecordChecksumWithoutSenderAcceptEvidence reports whether a record
// matches the checksum format used before sender-preserving Accept-Deps evidence.
// It exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutSenderAcceptEvidence(r InstanceRecord) bool {
	return recordChecksumInvariant(r) && r.Checksum == checksumRecord(r, true, true, true, true, false)
}

// VerifyRecordChecksumWithoutRecordBallot reports whether a record matches
// the checksum format used before durable record/value ballots were added. It
// exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutRecordBallot(r InstanceRecord) bool {
	return r.Checksum == checksumRecord(r, true, true, true, false, false)
}

// VerifyRecordChecksumWithoutTOQ reports whether a record matches the checksum
// format used before durable TOQ ProcessAt/pending metadata was added. It exists
// only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutTOQ(r InstanceRecord) bool {
	return r.Checksum == checksumRecord(r, true, false, false, false, false)
}

// VerifyRecordChecksumWithoutFastPathOrTOQ reports whether a record matches the
// checksum format used before durable fast-path and TOQ metadata were added. It
// exists only for decoding older on-disk example-KV records.
func VerifyRecordChecksumWithoutFastPathOrTOQ(r InstanceRecord) bool {
	return r.Checksum == checksumRecord(r, false, false, false, false, false)
}

// ChecksumMessage returns the canonical BLAKE3 checksum for a message.
func ChecksumMessage(m Message) [32]byte {
	h := getHash()
	defer putHash(h)
	writeByte(h, byte(m.Type))
	writeUint64(h, uint64(m.From))
	writeUint64(h, uint64(m.To))
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
	writeCommand(h, m.Command)
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
