package epaxos

import "encoding/binary"

var wireMagic = [...]byte{'M', 'E', 'P', '1'}

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

// DecodeMessage decodes a message using zero-copy slices backed by src.
func DecodeMessage(src []byte, m *Message) error {
	m.Reset()
	if len(src) < len(wireMagic)+32 || string(src[:len(wireMagic)]) != string(wireMagic[:]) {
		return ErrInvalidMessage
	}
	p := parser{b: src[len(wireMagic) : len(src)-32]}
	m.Type = MessageType(p.uvarint())
	m.From = ReplicaID(p.uvarint())
	m.To = ReplicaID(p.uvarint())
	m.Ref = InstanceRef{Replica: ReplicaID(p.uvarint()), Instance: InstanceNum(p.uvarint()), Conf: ConfID(p.uvarint())}
	m.Ballot = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.Seq = p.uvarint()
	deps := p.uvarint()
	if deps > 128 {
		return ErrInvalidMessage
	}
	m.Deps = make([]InstanceNum, int(deps))
	for i := range m.Deps {
		m.Deps[i] = InstanceNum(p.uvarint())
	}
	m.Command = p.command()
	m.Reject = p.byte() == 1
	m.RejectHint = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.RecordStatus = Status(p.uvarint())
	if p.err || len(p.b) != 0 {
		return ErrInvalidMessage
	}
	copy(m.Checksum[:], src[len(src)-32:])
	if !VerifyMessageChecksum(*m) {
		return ErrChecksumMismatch
	}
	return nil
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
	b   []byte
	err bool
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
	c.ConflictKeys = make([][]byte, int(keys))
	for i := range c.ConflictKeys {
		c.ConflictKeys[i] = p.bytes()
	}
	return c
}
