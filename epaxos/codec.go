package epaxos

import (
	"bytes"
	"encoding/binary"
)

var wireMagic = [...]byte{'M', 'E', 'P', '1'}

// DecodeScratch owns reusable metadata buffers for DecodeMessageWithScratch.
//
// The decoded message's Deps and ConflictKeys slice header alias these buffers
// until the scratch is reused. Command payload and conflict-key bytes still
// alias the input buffer.
type DecodeScratch struct {
	Deps         []InstanceNum
	ConflictKeys [][]byte
}

// Reset clears references retained by the scratch while keeping buffer capacity.
func (s *DecodeScratch) Reset() {
	for i := range s.ConflictKeys {
		s.ConflictKeys[i] = nil
	}
	s.Deps = s.Deps[:0]
	s.ConflictKeys = s.ConflictKeys[:0]
}

func (s *DecodeScratch) deps(n int) []InstanceNum {
	if cap(s.Deps) < n {
		s.Deps = make([]InstanceNum, n)
	} else {
		s.Deps = s.Deps[:n]
	}
	return s.Deps
}

func (s *DecodeScratch) conflictKeys(n int) [][]byte {
	if cap(s.ConflictKeys) < n {
		for i := range s.ConflictKeys {
			s.ConflictKeys[i] = nil
		}
		s.ConflictKeys = make([][]byte, n)
	} else {
		for i := n; i < len(s.ConflictKeys); i++ {
			s.ConflictKeys[i] = nil
		}
		s.ConflictKeys = s.ConflictKeys[:n]
	}
	return s.ConflictKeys
}

// EncodeMessage appends the canonical wire representation of m to dst.
func EncodeMessage(dst []byte, m Message) ([]byte, error) {
	m.Checksum = ChecksumMessage(m)
	dst = append(dst, wireMagic[:]...)
	dst = binary.AppendUvarint(dst, uint64(m.Type))
	dst = binary.AppendUvarint(dst, uint64(m.From))
	dst = binary.AppendUvarint(dst, uint64(m.To))
	dst = binary.AppendUvarint(dst, uint64(m.Ref.Replica))
	dst = binary.AppendUvarint(dst, uint64(m.Ref.Instance))
	dst = binary.AppendUvarint(dst, uint64(m.Ref.Conf))
	dst = binary.AppendUvarint(dst, m.Ballot.Epoch)
	dst = binary.AppendUvarint(dst, m.Ballot.Number)
	dst = binary.AppendUvarint(dst, uint64(m.Ballot.Replica))
	dst = binary.AppendUvarint(dst, m.Seq)
	dst = binary.AppendUvarint(dst, uint64(len(m.Deps)))
	for _, dep := range m.Deps {
		dst = binary.AppendUvarint(dst, uint64(dep))
	}
	dst = appendCommand(dst, m.Command)
	if m.Reject {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, m.RejectHint.Epoch)
	dst = binary.AppendUvarint(dst, m.RejectHint.Number)
	dst = binary.AppendUvarint(dst, uint64(m.RejectHint.Replica))
	dst = binary.AppendUvarint(dst, uint64(m.RecordStatus))
	dst = append(dst, m.Checksum[:]...)
	return dst, nil
}

// DecodeMessage decodes a message. Command payload and conflict-key byte slices
// alias src; decoded metadata slices such as Deps are allocated.
func DecodeMessage(src []byte, m *Message) error {
	return decodeMessage(src, m, nil)
}

// DecodeMessageWithScratch decodes a message using scratch-owned metadata buffers.
//
// A nil scratch behaves like DecodeMessage. With a non-nil scratch, the decoded
// Deps and ConflictKeys slice header are valid until the scratch is reused.
// Command payload and conflict-key bytes alias src.
func DecodeMessageWithScratch(src []byte, m *Message, scratch *DecodeScratch) error {
	return decodeMessage(src, m, scratch)
}

func decodeMessage(src []byte, m *Message, scratch *DecodeScratch) error {
	m.Reset()
	if len(src) < len(wireMagic)+32 || !bytes.Equal(src[:len(wireMagic)], wireMagic[:]) {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	p := parser{b: src[len(wireMagic) : len(src)-32], scratch: scratch}
	m.Type = MessageType(p.uvarint())
	m.From = ReplicaID(p.uvarint())
	m.To = ReplicaID(p.uvarint())
	m.Ref = InstanceRef{Replica: ReplicaID(p.uvarint()), Instance: InstanceNum(p.uvarint()), Conf: ConfID(p.uvarint())}
	m.Ballot = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.Seq = p.uvarint()
	deps := p.uvarint()
	if deps > 128 {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	if scratch != nil {
		m.Deps = scratch.deps(int(deps))
	} else {
		m.Deps = make([]InstanceNum, int(deps))
	}
	for i := range m.Deps {
		m.Deps[i] = InstanceNum(p.uvarint())
	}
	m.Command = p.command()
	m.Reject = p.byte() == 1
	m.RejectHint = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.RecordStatus = Status(p.uvarint())
	if p.err || len(p.b) != 0 {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	copy(m.Checksum[:], src[len(src)-32:])
	if !VerifyMessageChecksum(*m) {
		return decodeMessageError(m, scratch, ErrChecksumMismatch)
	}
	return nil
}

func decodeMessageError(m *Message, scratch *DecodeScratch, err error) error {
	m.Reset()
	if scratch != nil {
		scratch.Reset()
	}
	return err
}

func appendCommand(dst []byte, c Command) []byte {
	dst = binary.AppendUvarint(dst, c.ID.Client)
	dst = binary.AppendUvarint(dst, c.ID.Sequence)
	dst = binary.AppendUvarint(dst, uint64(c.Kind))
	dst = binary.AppendUvarint(dst, uint64(len(c.Payload)))
	dst = append(dst, c.Payload...)
	dst = binary.AppendUvarint(dst, uint64(len(c.ConflictKeys)))
	for _, key := range c.ConflictKeys {
		dst = binary.AppendUvarint(dst, uint64(len(key)))
		dst = append(dst, key...)
	}
	return dst
}

type parser struct {
	b       []byte
	err     bool
	scratch *DecodeScratch
}

func (p *parser) uvarint() uint64 {
	if p.err {
		return 0
	}
	v, n := binary.Uvarint(p.b)
	if n <= 0 {
		p.err = true
		return 0
	}
	p.b = p.b[n:]
	return v
}

func (p *parser) byte() byte {
	if len(p.b) == 0 {
		p.err = true
		return 0
	}
	v := p.b[0]
	p.b = p.b[1:]
	return v
}

func (p *parser) bytes() []byte {
	n := p.uvarint()
	if n > uint64(len(p.b)) {
		p.err = true
		return nil
	}
	out := p.b[:n]
	p.b = p.b[n:]
	return out
}

func (p *parser) command() Command {
	c := Command{ID: CommandID{Client: p.uvarint(), Sequence: p.uvarint()}, Kind: CommandKind(p.uvarint())}
	c.Payload = p.bytes()
	keys := p.uvarint()
	if keys > 128 {
		p.err = true
		return c
	}
	if p.scratch != nil {
		c.ConflictKeys = p.scratch.conflictKeys(int(keys))
	} else {
		c.ConflictKeys = make([][]byte, int(keys))
	}
	for i := range c.ConflictKeys {
		c.ConflictKeys[i] = p.bytes()
	}
	return c
}
