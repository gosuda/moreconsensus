package epaxos

import (
	"bytes"
	"encoding/binary"
)

var wireMagic = [...]byte{'M', 'E', 'P', '2'}

const (
	maxWireDeps           = 128
	maxWireConflictKeys   = 128
	maxWireAcceptEvidence = 128
	maxWireCommandPayload  = 1 << 20
	maxWireConflictKey     = 1 << 16
)

// DecodeScratch owns reusable metadata buffers for DecodeMessageWithScratch.
//
// The decoded message's Deps, AcceptDeps, AcceptEvidence, and ConflictKeys
// slice headers alias these buffers until the scratch is reused.
// AcceptEvidence dependency slices partition AcceptEvidenceDeps. Command payload
// and conflict-key bytes still alias the input buffer.
type DecodeScratch struct {
	Deps               []InstanceNum
	AcceptDeps         []InstanceNum
	AcceptEvidence     []AcceptEvidence
	AcceptEvidenceDeps []InstanceNum
	ConflictKeys       [][]byte
}

// Reset clears references retained by the scratch while keeping buffer capacity.
func (s *DecodeScratch) Reset() {
	clear(s.AcceptEvidence[:cap(s.AcceptEvidence)])
	clear(s.ConflictKeys[:cap(s.ConflictKeys)])
	s.Deps = s.Deps[:0]
	s.AcceptDeps = s.AcceptDeps[:0]
	s.AcceptEvidence = s.AcceptEvidence[:0]
	s.AcceptEvidenceDeps = s.AcceptEvidenceDeps[:0]
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

func (s *DecodeScratch) acceptDeps(n int) []InstanceNum {
	if cap(s.AcceptDeps) < n {
		s.AcceptDeps = make([]InstanceNum, n)
	} else {
		s.AcceptDeps = s.AcceptDeps[:n]
	}
	return s.AcceptDeps
}

func (s *DecodeScratch) acceptEvidence(n int) []AcceptEvidence {
	if cap(s.AcceptEvidence) < n {
		clear(s.AcceptEvidence[:cap(s.AcceptEvidence)])
		s.AcceptEvidence = make([]AcceptEvidence, n)
	} else {
		clear(s.AcceptEvidence[n:cap(s.AcceptEvidence)])
		s.AcceptEvidence = s.AcceptEvidence[:n]
	}
	return s.AcceptEvidence
}

func (s *DecodeScratch) acceptEvidenceDeps(n int) []InstanceNum {
	if cap(s.AcceptEvidenceDeps) < n {
		s.AcceptEvidenceDeps = make([]InstanceNum, n)
	} else {
		s.AcceptEvidenceDeps = s.AcceptEvidenceDeps[:n]
	}
	return s.AcceptEvidenceDeps
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
	if len(m.Deps) > maxWireDeps || len(m.AcceptDeps) > maxWireDeps || len(m.AcceptEvidence) > maxWireAcceptEvidence ||
		len(m.Command.Payload) > maxWireCommandPayload || len(m.Command.ConflictKeys) > maxWireConflictKeys {
		return dst, ErrInvalidMessage
	}
	for _, key := range m.Command.ConflictKeys {
		if len(key) > maxWireConflictKey {
			return dst, ErrInvalidMessage
		}
	}
	for _, ev := range m.AcceptEvidence {
		if len(ev.Deps) > maxWireDeps {
			return dst, ErrInvalidMessage
		}
	}
	m.Checksum = ChecksumMessage(m)
	dst = append(dst, wireMagic[:]...)
	dst = binary.AppendUvarint(dst, uint64(m.Type))
	dst = binary.AppendUvarint(dst, uint64(m.From))
	dst = binary.AppendUvarint(dst, uint64(m.To))
	dst = binary.AppendUvarint(dst, uint64(m.Ref.Replica))
	dst = binary.AppendUvarint(dst, uint64(m.Ref.Instance))
	dst = binary.AppendUvarint(dst, uint64(m.Ref.Conf))
	dst = binary.AppendUvarint(dst, m.ProcessAt)
	if m.TOQ {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, m.Ballot.Epoch)
	dst = binary.AppendUvarint(dst, m.Ballot.Number)
	dst = binary.AppendUvarint(dst, uint64(m.Ballot.Replica))
	dst = binary.AppendUvarint(dst, m.RecordBallot.Epoch)
	dst = binary.AppendUvarint(dst, m.RecordBallot.Number)
	dst = binary.AppendUvarint(dst, uint64(m.RecordBallot.Replica))
	dst = binary.AppendUvarint(dst, m.Seq)
	dst = binary.AppendUvarint(dst, uint64(len(m.Deps)))
	for _, dep := range m.Deps {
		dst = binary.AppendUvarint(dst, uint64(dep))
	}
	dst = binary.AppendUvarint(dst, m.AcceptSeq)
	dst = binary.AppendUvarint(dst, uint64(len(m.AcceptDeps)))
	for _, dep := range m.AcceptDeps {
		dst = binary.AppendUvarint(dst, uint64(dep))
	}
	dst = binary.AppendUvarint(dst, uint64(len(m.AcceptEvidence)))
	for _, ev := range m.AcceptEvidence {
		dst = binary.AppendUvarint(dst, uint64(ev.Sender))
		dst = binary.AppendUvarint(dst, ev.Seq)
		dst = binary.AppendUvarint(dst, uint64(len(ev.Deps)))
		for _, dep := range ev.Deps {
			dst = binary.AppendUvarint(dst, uint64(dep))
		}
	}
	dst = binary.AppendUvarint(dst, uint64(m.IgnoreDependency.Ref.Replica))
	dst = binary.AppendUvarint(dst, uint64(m.IgnoreDependency.Ref.Instance))
	dst = binary.AppendUvarint(dst, uint64(m.IgnoreDependency.Ref.Conf))
	dst = appendCommand(dst, m.Command)
	if m.Reject {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, m.RejectHint.Epoch)
	dst = binary.AppendUvarint(dst, m.RejectHint.Number)
	dst = binary.AppendUvarint(dst, uint64(m.RejectHint.Replica))
	dst = binary.AppendUvarint(dst, uint64(m.RejectReason))
	dst = binary.AppendUvarint(dst, uint64(m.ConflictRef.Replica))
	dst = binary.AppendUvarint(dst, uint64(m.ConflictRef.Instance))
	dst = binary.AppendUvarint(dst, uint64(m.ConflictRef.Conf))
	dst = binary.AppendUvarint(dst, uint64(m.ConflictStatus))
	if m.FastPathEligible {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, m.DepsCommitted)
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
// Deps, AcceptDeps, AcceptEvidence, and ConflictKeys slice headers are valid
// until the scratch is reused. Command payload and conflict-key bytes alias src.
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
	m.ProcessAt = p.uvarint()
	m.TOQ = p.byte() == 1
	m.Ballot = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.RecordBallot = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.Seq = p.uvarint()
	deps := p.uvarint()
	if deps > maxWireDeps {
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
	m.AcceptSeq = p.uvarint()
	acceptDeps := p.uvarint()
	if acceptDeps > maxWireDeps {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	if scratch != nil {
		m.AcceptDeps = scratch.acceptDeps(int(acceptDeps))
	} else {
		m.AcceptDeps = make([]InstanceNum, int(acceptDeps))
	}
	for i := range m.AcceptDeps {
		m.AcceptDeps[i] = InstanceNum(p.uvarint())
	}
	evidence := p.uvarint()
	if evidence > maxWireAcceptEvidence {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	evidenceCount := int(evidence)
	evidenceDeps, ok := scanAcceptEvidence(p, evidenceCount)
	if !ok {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	var evidenceArena []InstanceNum
	if scratch != nil {
		m.AcceptEvidence = scratch.acceptEvidence(evidenceCount)
		evidenceArena = scratch.acceptEvidenceDeps(evidenceDeps)
	} else if evidenceCount > 0 {
		m.AcceptEvidence = make([]AcceptEvidence, evidenceCount)
	}
	evidenceOffset := 0
	for i := range m.AcceptEvidence {
		m.AcceptEvidence[i].Sender = ReplicaID(p.uvarint())
		m.AcceptEvidence[i].Seq = p.uvarint()
		deps := int(p.uvarint())
		if scratch != nil {
			end := evidenceOffset + deps
			m.AcceptEvidence[i].Deps = evidenceArena[evidenceOffset:end:end]
			evidenceOffset = end
		} else {
			m.AcceptEvidence[i].Deps = make([]InstanceNum, deps)
		}
		for j := range m.AcceptEvidence[i].Deps {
			m.AcceptEvidence[i].Deps[j] = InstanceNum(p.uvarint())
		}
	}
	m.IgnoreDependency.Ref = InstanceRef{Replica: ReplicaID(p.uvarint()), Instance: InstanceNum(p.uvarint()), Conf: ConfID(p.uvarint())}
	m.Command = p.command()
	m.Reject = p.byte() == 1
	m.RejectHint = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.RejectReason = RejectReason(p.uvarint())
	m.ConflictRef = InstanceRef{Replica: ReplicaID(p.uvarint()), Instance: InstanceNum(p.uvarint()), Conf: ConfID(p.uvarint())}
	m.ConflictStatus = Status(p.uvarint())
	m.FastPathEligible = p.byte() == 1
	m.DepsCommitted = p.uvarint()
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

func scanAcceptEvidence(p parser, count int) (int, bool) {
	totalDeps := 0
	for range count {
		p.uvarint()
		p.uvarint()
		deps := p.uvarint()
		if p.err || deps > maxWireDeps {
			return 0, false
		}
		totalDeps += int(deps)
		for range deps {
			p.uvarint()
		}
		if p.err {
			return 0, false
		}
	}
	return totalDeps, true
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

func (p *parser) bytesBound(max uint64) []byte {
	n := p.uvarint()
	if n > max || n > uint64(len(p.b)) {
		p.err = true
		return nil
	}
	out := p.b[:n]
	p.b = p.b[n:]
	return out
}
func (p *parser) bytes() []byte {
	return p.bytesBound(^uint64(0))
}


func (p *parser) command() Command {
	c := Command{ID: CommandID{Client: p.uvarint(), Sequence: p.uvarint()}, Kind: CommandKind(p.uvarint())}
	c.Payload = p.bytesBound(maxWireCommandPayload)
	keys := p.uvarint()
	if keys > maxWireConflictKeys {
		p.err = true
		return c
	}
	if p.scratch != nil {
		c.ConflictKeys = p.scratch.conflictKeys(int(keys))
	} else {
		c.ConflictKeys = make([][]byte, int(keys))
	}
	for i := range c.ConflictKeys {
		c.ConflictKeys[i] = p.bytesBound(maxWireConflictKey)
	}
	return c
}
