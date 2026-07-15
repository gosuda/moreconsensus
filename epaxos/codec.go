package epaxos

import (
	"bytes"
	"encoding/binary"
)

var wireMagic = [...]byte{'M', 'E', 'P', '3'}

const (
	maxWireDeps            = 128
	maxWireAcceptEvidence  = 128
	maxWireCommandPayload  = 1 << 20
	maxWireProtocolControl = 1 << 20
)

// DecodeScratch owns reusable metadata buffers for DecodeMessageWithScratch.
// Decoded metadata slice headers alias these buffers until reuse; endpoint,
// payload, cycle-key, and protocol-control bytes alias the input.
type DecodeScratch struct {
	Deps               []InstanceNum
	AcceptDeps         []InstanceNum
	AcceptEvidence     []AcceptEvidence
	AcceptEvidenceDeps []InstanceNum
	Points             [][]byte
	Spans              []Span
}

// Reset clears references retained by the scratch while keeping buffer capacity.
func (s *DecodeScratch) Reset() {
	clear(s.AcceptEvidence[:cap(s.AcceptEvidence)])
	clear(s.Points[:cap(s.Points)])
	clear(s.Spans[:cap(s.Spans)])
	s.Deps = s.Deps[:0]
	s.AcceptDeps = s.AcceptDeps[:0]
	s.AcceptEvidence = s.AcceptEvidence[:0]
	s.AcceptEvidenceDeps = s.AcceptEvidenceDeps[:0]
	s.Points = s.Points[:0]
	s.Spans = s.Spans[:0]
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

func (s *DecodeScratch) points(n int) [][]byte {
	if cap(s.Points) < n {
		clear(s.Points[:cap(s.Points)])
		s.Points = make([][]byte, n)
	} else {
		clear(s.Points[n:cap(s.Points)])
		s.Points = s.Points[:n]
	}
	return s.Points
}

func (s *DecodeScratch) spans(n int) []Span {
	if cap(s.Spans) < n {
		clear(s.Spans[:cap(s.Spans)])
		s.Spans = make([]Span, n)
	} else {
		clear(s.Spans[n:cap(s.Spans)])
		s.Spans = s.Spans[:n]
	}
	return s.Spans
}

// EncodeMessage appends the canonical wire representation of m to dst.
func EncodeMessage(dst []byte, m Message) ([]byte, error) {
	if len(m.Deps) > maxWireDeps || len(m.AcceptDeps) > maxWireDeps || len(m.AcceptEvidence) > maxWireAcceptEvidence ||
		len(m.Command.Payload) > maxWireCommandPayload || len(m.Command.CycleKey) > maxWireCycleKeyBytes ||
		len(m.ProtocolControl) > maxWireProtocolControl || !entryValueCanonical(m.Kind, m.Command, m.ConfChange, m.ProtocolControl) {
		return dst, ErrInvalidMessage
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
	dst = binary.AppendUvarint(dst, m.FromIncarnation)
	dst = binary.AppendUvarint(dst, m.ToIncarnation)
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
	dst = binary.AppendUvarint(dst, uint64(m.Kind))
	dst = binary.AppendUvarint(dst, uint64(m.ConfChange.Type))
	dst = binary.AppendUvarint(dst, uint64(m.ConfChange.Replica))
	dst = binary.AppendUvarint(dst, uint64(len(m.ProtocolControl)))
	dst = append(dst, m.ProtocolControl...)
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
// alias src and expire when src is mutated or released; decoded metadata slices
// such as Deps are allocated. Step copies any data it retains before returning.
func DecodeMessage(src []byte, m *Message) error {
	return decodeMessage(src, m, nil)
}

// DecodeMessageWithScratch decodes a message using scratch-owned metadata buffers.
//
// A nil scratch behaves like DecodeMessage. With a non-nil scratch, decoded
// Deps, AcceptDeps, AcceptEvidence, and Footprint point/span slice headers expire
// when scratch is reset, returned to its pool, or reused. Command payload and
// footprint endpoint bytes alias src and expire when src is mutated or released.
// Step copies any data retained beyond the synchronous call before returning.
func DecodeMessageWithScratch(src []byte, m *Message, scratch *DecodeScratch) error {
	if scratch == nil {
		return decodeMessage(src, m, nil)
	}
	original := *scratch
	if err := decodeMessage(src, m, scratch); err != nil {
		scratch.Reset()
		*scratch = original
		scratch.Reset()
		return err
	}
	return nil
}

func decodeMessage(src []byte, m *Message, scratch *DecodeScratch) error {
	m.Reset()
	if len(src) < len(wireMagic)+32 || !bytes.Equal(src[:len(wireMagic)], wireMagic[:]) {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	p := parser{b: src[len(wireMagic) : len(src)-32], scratch: scratch}
	if scratch != nil && !scanMessageBody(p) {
		return decodeMessageError(m, scratch, ErrInvalidMessage)
	}
	m.Type = MessageType(p.uvarint8())
	m.From = ReplicaID(p.uvarint())
	m.To = ReplicaID(p.uvarint())
	m.FromIncarnation = p.uvarint()
	m.ToIncarnation = p.uvarint()
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
		depsCount := p.uvarint()
		if p.err || depsCount > maxWireDeps {
			return decodeMessageError(m, scratch, ErrInvalidMessage)
		}
		deps := int(depsCount)
		if scratch != nil {
			end := evidenceOffset + deps
			if end > len(evidenceArena) {
				return decodeMessageError(m, scratch, ErrInvalidMessage)
			}
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
	m.Kind = EntryKind(p.uvarint8())
	m.ConfChange = ConfChange{Type: ConfChangeType(p.uvarint8()), Replica: ReplicaID(p.uvarint())}
	m.ProtocolControl = p.bytesBound(maxWireProtocolControl)
	m.Command = p.command()
	m.Reject = p.byte() == 1
	m.RejectHint = Ballot{Epoch: p.uvarint(), Number: p.uvarint(), Replica: ReplicaID(p.uvarint())}
	m.RejectReason = RejectReason(p.uvarint8())
	m.ConflictRef = InstanceRef{Replica: ReplicaID(p.uvarint()), Instance: InstanceNum(p.uvarint()), Conf: ConfID(p.uvarint())}
	m.ConflictStatus = Status(p.uvarint8())
	m.FastPathEligible = p.byte() == 1
	m.DepsCommitted = p.uvarint()
	m.RecordStatus = Status(p.uvarint8())
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
func scanMessageBody(p parser) bool {
	p.scratch = nil
	p.uvarint8()
	p.uvarint()
	p.uvarint()
	p.uvarint()
	p.uvarint()
	for range 3 {
		p.uvarint()
	}
	p.uvarint()
	p.byte()
	for range 7 {
		p.uvarint()
	}
	deps := p.uvarint()
	if deps > maxWireDeps {
		return false
	}
	for range deps {
		p.uvarint()
	}
	p.uvarint()
	acceptDeps := p.uvarint()
	if acceptDeps > maxWireDeps {
		return false
	}
	for range acceptDeps {
		p.uvarint()
	}
	evidence := p.uvarint()
	if evidence > maxWireAcceptEvidence {
		return false
	}
	for range evidence {
		p.uvarint()
		p.uvarint()
		evidenceDeps := p.uvarint()
		if evidenceDeps > maxWireDeps {
			return false
		}
		for range evidenceDeps {
			p.uvarint()
		}
	}
	for range 3 {
		p.uvarint()
	}
	p.uvarint8()
	p.uvarint8()
	p.uvarint()
	p.bytesBound(maxWireProtocolControl)
	if !scanCommand(&p) {
		return false
	}
	p.byte()
	for range 3 {
		p.uvarint()
	}
	p.uvarint8()
	for range 3 {
		p.uvarint()
	}
	p.uvarint8()
	p.byte()
	p.uvarint()
	p.uvarint8()
	return !p.err && len(p.b) == 0
}

func scanCommand(p *parser) bool {
	client := p.uvarint()
	sequence := p.uvarint()
	payload := p.bytesBound(maxWireCommandPayload)
	all := p.byte() == 1
	pointCount := p.uvarint()
	if p.err || pointCount > maxWireFootprintPoints {
		return false
	}
	var pointStorage [maxWireFootprintPoints][]byte
	points := pointStorage[:int(pointCount)]
	total := uint64(0)
	for i := range points {
		points[i] = p.bytesBound(maxWireFootprintBytes)
		total += uint64(len(points[i]))
	}
	spanCount := p.uvarint()
	if p.err || spanCount > maxWireFootprintSpans {
		return false
	}
	var spanStorage [maxWireFootprintSpans]Span
	spans := spanStorage[:int(spanCount)]
	for i := range spans {
		spans[i].Start = p.bytesBound(maxWireFootprintBytes)
		spans[i].End = p.bytesBound(maxWireFootprintBytes)
		total += uint64(len(spans[i].Start) + len(spans[i].End))
	}
	cycleKey := p.bytesBound(maxWireCycleKeyBytes)
	if p.err || total > maxWireFootprintBytes {
		return false
	}
	if client == 0 && sequence == 0 && len(payload) == 0 && !all && len(points) == 0 && len(spans) == 0 && len(cycleKey) == 0 {
		return true
	}
	return footprintCanonical(Footprint{Points: points, Spans: spans, All: all})
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
	dst = binary.AppendUvarint(dst, uint64(len(c.Payload)))
	dst = append(dst, c.Payload...)
	if c.Footprint.All {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.AppendUvarint(dst, uint64(len(c.Footprint.Points)))
	for _, point := range c.Footprint.Points {
		dst = binary.AppendUvarint(dst, uint64(len(point)))
		dst = append(dst, point...)
	}
	dst = binary.AppendUvarint(dst, uint64(len(c.Footprint.Spans)))
	for _, span := range c.Footprint.Spans {
		dst = binary.AppendUvarint(dst, uint64(len(span.Start)))
		dst = append(dst, span.Start...)
		dst = binary.AppendUvarint(dst, uint64(len(span.End)))
		dst = append(dst, span.End...)
	}
	dst = binary.AppendUvarint(dst, uint64(len(c.CycleKey)))
	dst = append(dst, c.CycleKey...)
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

func (p *parser) uvarint8() uint8 {
	v := p.uvarint()
	if v > uint64(^uint8(0)) {
		p.err = true
		return 0
	}
	return uint8(v)
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

func (p *parser) bytesBound(maxLen uint64) []byte {
	n := p.uvarint()
	if n > maxLen || n > uint64(len(p.b)) {
		p.err = true
		return nil
	}
	if n == 0 {
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
	c := Command{ID: CommandID{Client: p.uvarint(), Sequence: p.uvarint()}}
	c.Payload = p.bytesBound(maxWireCommandPayload)
	c.Footprint.All = p.byte() == 1
	points := p.uvarint()
	if points > maxWireFootprintPoints {
		p.err = true
		return c
	}
	if points != 0 {
		if p.scratch != nil {
			c.Footprint.Points = p.scratch.points(int(points))
		} else {
			c.Footprint.Points = make([][]byte, int(points))
		}
	}
	total := uint64(0)
	for i := range c.Footprint.Points {
		c.Footprint.Points[i] = p.bytesBound(maxWireFootprintBytes)
		total += uint64(len(c.Footprint.Points[i]))
	}
	spans := p.uvarint()
	if spans > maxWireFootprintSpans {
		p.err = true
		return c
	}
	if spans != 0 {
		if p.scratch != nil {
			c.Footprint.Spans = p.scratch.spans(int(spans))
		} else {
			c.Footprint.Spans = make([]Span, int(spans))
		}
	}
	for i := range c.Footprint.Spans {
		c.Footprint.Spans[i].Start = p.bytesBound(maxWireFootprintBytes)
		c.Footprint.Spans[i].End = p.bytesBound(maxWireFootprintBytes)
		total += uint64(len(c.Footprint.Spans[i].Start) + len(c.Footprint.Spans[i].End))
	}
	if total > maxWireFootprintBytes {
		p.err = true
	}
	c.CycleKey = p.bytesBound(maxWireCycleKeyBytes)
	if !p.err && !commandEmpty(c) && !footprintCanonical(c.Footprint) {
		p.err = true
	}
	return c
}
