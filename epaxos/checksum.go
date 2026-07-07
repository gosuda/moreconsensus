package epaxos

import (
	"encoding/binary"
	"hash"
	"sync"

	"github.com/zeebo/blake3"
)

var checksumPool = sync.Pool{New: func() any { return blake3.New() }}

func getHash() hash.Hash {
	h := checksumPool.Get().(hash.Hash)
	h.Reset()
	return h
}

func putHash(h hash.Hash) { checksumPool.Put(h) }

func writeByte(h hash.Hash, v byte) { _, _ = h.Write([]byte{v}) }

func writeUint64(h hash.Hash, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, _ = h.Write(b[:])
}

func writeBytes(h hash.Hash, b []byte) {
	writeUint64(h, uint64(len(b)))
	_, _ = h.Write(b)
}

func writeCommand(h hash.Hash, c Command) {
	writeUint64(h, c.ID.Client)
	writeUint64(h, c.ID.Sequence)
	writeByte(h, byte(c.Kind))
	writeBytes(h, c.Payload)
	writeUint64(h, uint64(len(c.ConflictKeys)))
	for _, key := range c.ConflictKeys {
		writeBytes(h, key)
	}
}

func writeRef(h hash.Hash, r InstanceRef) {
	writeUint64(h, uint64(r.Replica))
	writeUint64(h, uint64(r.Instance))
	writeUint64(h, uint64(r.Conf))
}

func writeBallot(h hash.Hash, b Ballot) {
	writeUint64(h, b.Epoch)
	writeUint64(h, b.Number)
	writeUint64(h, uint64(b.Replica))
}

func sumHash(h hash.Hash) [32]byte {
	var out [32]byte
	s := h.Sum(out[:0])
	copy(out[:], s)
	return out
}

// ChecksumRecord returns the canonical BLAKE3 checksum for a record.
func ChecksumRecord(r InstanceRecord) [32]byte {
	h := getHash()
	defer putHash(h)
	writeRef(h, r.Ref)
	writeBallot(h, r.Ballot)
	writeByte(h, byte(r.Status))
	writeUint64(h, r.Seq)
	writeUint64(h, uint64(len(r.Deps)))
	for _, dep := range r.Deps {
		writeUint64(h, uint64(dep))
	}
	writeCommand(h, r.Command)
	return sumHash(h)
}

// VerifyRecordChecksum reports whether the record checksum matches its content.
func VerifyRecordChecksum(r InstanceRecord) bool { return r.Checksum == ChecksumRecord(r) }

// ChecksumMessage returns the canonical BLAKE3 checksum for a message.
func ChecksumMessage(m Message) [32]byte {
	h := getHash()
	defer putHash(h)
	writeByte(h, byte(m.Type))
	writeUint64(h, uint64(m.From))
	writeUint64(h, uint64(m.To))
	writeRef(h, m.Ref)
	writeBallot(h, m.Ballot)
	writeUint64(h, m.Seq)
	writeUint64(h, uint64(len(m.Deps)))
	for _, dep := range m.Deps {
		writeUint64(h, uint64(dep))
	}
	writeCommand(h, m.Command)
	if m.Reject {
		writeByte(h, 1)
	} else {
		writeByte(h, 0)
	}
	writeBallot(h, m.RejectHint)
	writeByte(h, byte(m.RecordStatus))
	return sumHash(h)
}

// VerifyMessageChecksum reports whether the message checksum matches its content.
func VerifyMessageChecksum(m Message) bool { return m.Checksum == ChecksumMessage(m) }
