// Package model holds the types shared across ingest, the ring, the hub and
// the detector.
//
// Kept deliberately small and dependency-free. Everything downstream imports
// it, so anything that grows here grows everywhere.
package model

import (
	"sort"
	"strings"
)

// SeriesID is an interned handle for a label set.
//
// The entire point of interning: a label set is sent once and referenced by
// a uint32 thereafter. At ~9 bytes per point on the wire, carrying full
// labels on every sample would be 150-400 bytes instead.
type SeriesID uint32

// Point is one observation.
//
// Two timestamps, deliberately:
//
//   - OriginTsMs is stamped at observation, by the agent. It is what makes
//     end-to-end latency measurable (§3.5) and what charts are drawn
//     against.
//   - IngestTsMs is stamped at the gateway. It is the only one guaranteed
//     monotonic, because agent clocks skew.
//
// Storing only one of them means either unmeasurable latency or
// non-monotonic ordering. Both are needed.
type Point struct {
	Series     SeriesID
	OriginTsMs int64
	IngestTsMs int64
	Value      float32
}

// Value is float32, not float64.
//
// Percentages, rates and byte counts do not need 53 bits of mantissa, and
// f32 halves the payload. Where full precision genuinely matters (monetary
// counters, say) the metric should be scaled at the source instead.

// Labels is a metric's identity: a name plus key/value pairs.
type Labels struct {
	Name  string
	Pairs map[string]string
}

// Key returns a stable, canonical string for this label set.
//
// Sorted, because Go's map iteration order is randomised per run. Building
// the key from unsorted iteration produces a *different* key for the same
// label set on every process start, which silently multiplies the series
// count — an especially nasty cardinality bug because it only appears under
// restart, not in tests that run one process.
func (l Labels) Key() string {
	if len(l.Pairs) == 0 {
		return l.Name
	}
	keys := make([]string, 0, len(l.Pairs))
	for k := range l.Pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(l.Name)
	for _, k := range keys {
		b.WriteByte(0x1f) // unit separator: cannot appear in a label name
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(l.Pairs[k])
	}
	return b.String()
}

// Clone returns a deep copy, so a caller cannot mutate a stored label set.
func (l Labels) Clone() Labels {
	out := Labels{Name: l.Name}
	if l.Pairs != nil {
		out.Pairs = make(map[string]string, len(l.Pairs))
		for k, v := range l.Pairs {
			out.Pairs[k] = v
		}
	}
	return out
}

// Bucket is one rolled-up interval.
//
// Five values, not one. Averaging a one-second 100% CPU spike into a
// one-minute bucket alongside 59 seconds at 20% reads as 21.3% — the
// incident becomes invisible. For a monitoring tool that is a correctness
// failure, not a fidelity trade-off (§8.3).
type Bucket struct {
	StartMs int64
	Min     float64
	Max     float64
	Sum     float64
	Count   int64
}

// Avg is derived rather than stored, so it can never disagree with Sum/Count.
func (b Bucket) Avg() float64 {
	if b.Count == 0 {
		return 0
	}
	return b.Sum / float64(b.Count)
}

// Merge combines two buckets covering the same interval.
func (b Bucket) Merge(o Bucket) Bucket {
	if b.Count == 0 {
		return o
	}
	if o.Count == 0 {
		return b
	}
	return Bucket{
		StartMs: b.StartMs,
		Min:     min(b.Min, o.Min),
		Max:     max(b.Max, o.Max),
		Sum:     b.Sum + o.Sum,
		Count:   b.Count + o.Count,
	}
}
