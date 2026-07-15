package epaxos

import "sync"

var messagePool = sync.Pool{New: func() any { return new(Message) }}
var commandPool = sync.Pool{New: func() any { return new(Command) }}
var decodeScratchPool = sync.Pool{New: func() any { return new(DecodeScratch) }}

const (
	maxPooledDependencyWidth          = 7
	maxPooledEvidenceEntries          = 7
	maxPooledFootprintPoints          = 128
	maxPooledPayloadBytes             = 64 << 10
	maxPooledFootprintPointBytes      = 64 << 10
	maxPooledFootprintPointArenaBytes = 256 << 10
	maxPooledEvidenceDeps             = maxPooledDependencyWidth * maxPooledEvidenceEntries
)

func resetSliceForPool[T any](items []T, maxCapacity int) []T {
	if cap(items) > maxCapacity {
		clear(items)
		return nil
	}
	clear(items[:cap(items)])
	return items[:0]
}

func resetFootprintPointsForPool(items [][]byte) [][]byte {
	full := items[:cap(items)]
	drop := cap(items) > maxPooledFootprintPoints
	aggregate := 0
	for i := range full {
		key := full[i]
		clear(key[:cap(key)])
		if cap(key) > maxPooledFootprintPointBytes || aggregate > maxPooledFootprintPointArenaBytes-cap(key) {
			drop = true
		}
		aggregate += cap(key)
	}
	if aggregate > maxPooledFootprintPointArenaBytes {
		drop = true
	}
	if drop {
		for i := range full {
			full[i] = nil
		}
		return nil
	}
	for i := range full {
		full[i] = full[i][:0]
	}
	return full[:0]
}

func resetAcceptEvidenceForPool(items []AcceptEvidence) []AcceptEvidence {
	full := items[:cap(items)]
	if cap(items) > maxPooledEvidenceEntries {
		for i := range full {
			clear(full[i].Deps)
			full[i] = AcceptEvidence{}
		}
		return nil
	}
	for i := range full {
		deps := resetSliceForPool(full[i].Deps, maxPooledDependencyWidth)
		full[i] = AcceptEvidence{Deps: deps}
	}
	return full[:0]
}

func resetCommandForPool(c *Command) {
	payload := resetSliceForPool(c.Payload, maxPooledPayloadBytes)
	points := resetFootprintPointsForPool(c.Footprint.Points)
	spans := c.Footprint.Spans
	for i := range spans[:cap(spans)] {
		clear(spans[:cap(spans)][i].Start)
		clear(spans[:cap(spans)][i].End)
		spans[:cap(spans)][i] = Span{}
	}
	if cap(spans) > maxWireFootprintSpans {
		spans = nil
	} else {
		spans = spans[:0]
	}
	cycleKey := resetSliceForPool(c.CycleKey, maxWireCycleKeyBytes)
	*c = Command{Payload: payload, Footprint: Footprint{Points: points, Spans: spans}, CycleKey: cycleKey}
}

func resetMessageForPool(m *Message) {
	deps := resetSliceForPool(m.Deps, maxPooledDependencyWidth)
	acceptDeps := resetSliceForPool(m.AcceptDeps, maxPooledDependencyWidth)
	acceptEvidence := resetAcceptEvidenceForPool(m.AcceptEvidence)
	control := resetSliceForPool(m.ProtocolControl, maxWireProtocolControl)
	command := m.Command
	resetCommandForPool(&command)
	*m = Message{
		Deps: deps, AcceptDeps: acceptDeps, AcceptEvidence: acceptEvidence,
		ProtocolControl: control, Command: command,
	}
}

// GetMessage returns a logically zero message with bounded reusable capacities.
func GetMessage() *Message {
	return messagePool.Get().(*Message)
}

// PutMessage transfers the message and its nested buffers to the package pool.
// The caller must retain no aliases. Data is cleared; oversized buffers are dropped.
func PutMessage(m *Message) {
	if m == nil {
		return
	}
	resetMessageForPool(m)
	messagePool.Put(m)
}

// GetCommand returns a logically zero command with bounded reusable capacities.
func GetCommand() *Command {
	return commandPool.Get().(*Command)
}

// PutCommand transfers the command and its nested buffers to the package pool.
// The caller must retain no aliases. Data is cleared; oversized buffers are dropped.
func PutCommand(c *Command) {
	if c == nil {
		return
	}
	resetCommandForPool(c)
	commandPool.Put(c)
}

// GetDecodeScratch returns cleared caller-owned decode scratch from the package pool.
// The caller must return it only after every decoded Message view has expired.
func GetDecodeScratch() *DecodeScratch {
	s := decodeScratchPool.Get().(*DecodeScratch)
	s.Reset()
	return s
}

// PutDecodeScratch clears and returns decode scratch to the package pool.
// Oversized arenas are discarded so malformed traffic cannot pin unbounded memory.
func PutDecodeScratch(s *DecodeScratch) {
	if s == nil {
		return
	}
	s.Reset()
	s.Deps = resetSliceForPool(s.Deps, maxWireDeps)
	s.AcceptDeps = resetSliceForPool(s.AcceptDeps, maxWireDeps)
	s.AcceptEvidence = resetSliceForPool(s.AcceptEvidence, maxWireAcceptEvidence)
	s.AcceptEvidenceDeps = resetSliceForPool(s.AcceptEvidenceDeps, maxPooledEvidenceDeps)
	s.Points = resetSliceForPool(s.Points, maxWireFootprintPoints)
	s.Spans = resetSliceForPool(s.Spans, maxWireFootprintSpans)
	decodeScratchPool.Put(s)
}
