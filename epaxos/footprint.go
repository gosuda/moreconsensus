package epaxos

import (
	"bytes"
	"errors"
	"sort"
)

const (
	defaultMaxFootprintPoints = 128
	defaultMaxFootprintSpans  = 128
	defaultMaxFootprintBytes  = 1 << 20
	defaultMaxCycleKeyBytes   = 64 << 10

	maxWireFootprintPoints = defaultMaxFootprintPoints
	maxWireFootprintSpans  = defaultMaxFootprintSpans
	maxWireFootprintBytes  = defaultMaxFootprintBytes
	maxWireCycleKeyBytes   = defaultMaxCycleKeyBytes
)

// ErrInvalidFootprint reports a malformed or non-canonicalizable command footprint.
var ErrInvalidFootprint = errors.New("epaxos: invalid footprint")

// Span is a half-open logical resource interval [Start, End). Endpoints are
// compared lexicographically with bytes.Compare.
type Span struct {
	Start []byte
	End   []byte
}

// Footprint declares every logical resource touched by an application command.
// All is the explicit EPaxos-group-wide scope.
type Footprint struct {
	Points [][]byte
	Spans  []Span
	All    bool
}

type footprintLimits struct {
	points int
	spans  int
	bytes  int
}

func wireFootprintLimits() footprintLimits {
	return footprintLimits{points: maxWireFootprintPoints, spans: maxWireFootprintSpans, bytes: maxWireFootprintBytes}
}

// CanonicalizeFootprint validates, canonicalizes, and deep-copies src into dst.
// Points are sorted and deduplicated, overlapping or adjacent spans are merged,
// and points covered by spans are removed. The zero footprint is invalid.
func CanonicalizeFootprint(dst *Footprint, src Footprint) error {
	if dst == nil {
		return ErrInvalidFootprint
	}
	return canonicalizeFootprint(dst, src, wireFootprintLimits(), false)
}

func canonicalizeFootprint(dst *Footprint, src Footprint, limits footprintLimits, borrowCanonical bool) error {
	if dst == nil || limits.points < 0 || limits.spans < 0 || limits.bytes < 0 ||
		len(src.Points) > limits.points || len(src.Spans) > limits.spans {
		return ErrInvalidFootprint
	}
	total := 0
	addBytes := func(n int) bool {
		if n < 0 || total > limits.bytes-n {
			return false
		}
		total += n
		return true
	}
	for _, point := range src.Points {
		if !addBytes(len(point)) {
			return ErrInvalidFootprint
		}
	}
	for _, span := range src.Spans {
		if bytes.Compare(span.Start, span.End) >= 0 || !addBytes(len(span.Start)) || !addBytes(len(span.End)) {
			return ErrInvalidFootprint
		}
	}
	if !src.All && len(src.Points) == 0 && len(src.Spans) == 0 {
		return ErrInvalidFootprint
	}
	if src.All {
		*dst = Footprint{All: true}
		return nil
	}

	canonical := footprintCanonical(src)
	if canonical && borrowCanonical {
		*dst = src
		return nil
	}

	points := make([][]byte, len(src.Points))
	for i := range src.Points {
		points[i] = bytes.Clone(src.Points[i])
	}
	sort.Slice(points, func(i, j int) bool { return bytes.Compare(points[i], points[j]) < 0 })
	outPoints := points[:0]
	for _, point := range points {
		if len(outPoints) == 0 || !bytes.Equal(outPoints[len(outPoints)-1], point) {
			outPoints = append(outPoints, point)
		}
	}

	spans := make([]Span, len(src.Spans))
	for i := range src.Spans {
		spans[i] = Span{Start: bytes.Clone(src.Spans[i].Start), End: bytes.Clone(src.Spans[i].End)}
	}
	sort.Slice(spans, func(i, j int) bool {
		if cmp := bytes.Compare(spans[i].Start, spans[j].Start); cmp != 0 {
			return cmp < 0
		}
		return bytes.Compare(spans[i].End, spans[j].End) < 0
	})
	outSpans := spans[:0]
	for _, span := range spans {
		if len(outSpans) == 0 || bytes.Compare(outSpans[len(outSpans)-1].End, span.Start) < 0 {
			outSpans = append(outSpans, span)
			continue
		}
		last := &outSpans[len(outSpans)-1]
		if bytes.Compare(last.End, span.End) < 0 {
			last.End = span.End
		}
	}

	filtered := outPoints[:0]
	spanIndex := 0
	for _, point := range outPoints {
		for spanIndex < len(outSpans) && bytes.Compare(outSpans[spanIndex].End, point) <= 0 {
			spanIndex++
		}
		if spanIndex < len(outSpans) && bytes.Compare(outSpans[spanIndex].Start, point) <= 0 && bytes.Compare(point, outSpans[spanIndex].End) < 0 {
			continue
		}
		filtered = append(filtered, point)
	}
	clear(outPoints[len(filtered):])
	*dst = Footprint{Points: filtered, Spans: outSpans}
	return nil
}

func footprintCanonical(f Footprint) bool {
	if f.All {
		return len(f.Points) == 0 && len(f.Spans) == 0
	}
	if len(f.Points) == 0 && len(f.Spans) == 0 {
		return false
	}
	for i, span := range f.Spans {
		if bytes.Compare(span.Start, span.End) >= 0 {
			return false
		}
		if i > 0 {
			prev := f.Spans[i-1]
			if bytes.Compare(prev.Start, span.Start) >= 0 || bytes.Compare(prev.End, span.Start) >= 0 {
				return false
			}
		}
	}
	spanIndex := 0
	for i, point := range f.Points {
		if i > 0 && bytes.Compare(f.Points[i-1], point) >= 0 {
			return false
		}
		for spanIndex < len(f.Spans) && bytes.Compare(f.Spans[spanIndex].End, point) <= 0 {
			spanIndex++
		}
		if spanIndex < len(f.Spans) && bytes.Compare(f.Spans[spanIndex].Start, point) <= 0 && bytes.Compare(point, f.Spans[spanIndex].End) < 0 {
			return false
		}
	}
	return true
}

func cloneFootprintInto(dst *Footprint, src Footprint) {
	points := cloneByteSlicesInto(dst.Points, src.Points)
	spans := dst.Spans
	if cap(spans) < len(src.Spans) {
		spans = make([]Span, len(src.Spans))
	} else {
		spans = spans[:len(src.Spans)]
	}
	for i := range src.Spans {
		spans[i].Start = cloneSliceInto(spans[i].Start, src.Spans[i].Start)
		spans[i].End = cloneSliceInto(spans[i].End, src.Spans[i].End)
	}
	clear(spans[len(src.Spans):cap(spans)])
	*dst = Footprint{Points: points, Spans: spans, All: src.All}
}

func footprintEqual(a, b Footprint) bool {
	if a.All != b.All || len(a.Points) != len(b.Points) || len(a.Spans) != len(b.Spans) {
		return false
	}
	for i := range a.Points {
		if !bytes.Equal(a.Points[i], b.Points[i]) {
			return false
		}
	}
	for i := range a.Spans {
		if !bytes.Equal(a.Spans[i].Start, b.Spans[i].Start) || !bytes.Equal(a.Spans[i].End, b.Spans[i].End) {
			return false
		}
	}
	return true
}

func footprintsConflict(a, b Footprint) bool {
	if a.All || b.All {
		return true
	}
	for _, ap := range a.Points {
		for _, bp := range b.Points {
			if bytes.Equal(ap, bp) {
				return true
			}
		}
		for _, bs := range b.Spans {
			if bytes.Compare(bs.Start, ap) <= 0 && bytes.Compare(ap, bs.End) < 0 {
				return true
			}
		}
	}
	for _, bp := range b.Points {
		for _, as := range a.Spans {
			if bytes.Compare(as.Start, bp) <= 0 && bytes.Compare(bp, as.End) < 0 {
				return true
			}
		}
	}
	for _, as := range a.Spans {
		for _, bs := range b.Spans {
			if bytes.Compare(as.Start, bs.End) < 0 && bytes.Compare(bs.Start, as.End) < 0 {
				return true
			}
		}
	}
	return false
}
