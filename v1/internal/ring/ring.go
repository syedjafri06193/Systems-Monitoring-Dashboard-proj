// Package ring holds the last N points per series in memory.
//
// This is the live source of truth. New subscribers get their snapshot from
// here rather than from the TSDB, which makes subscription setup fast and
// keeps query load off the database — and the detector reads from here too,
// so alert evaluation never depends on what any browser happens to be
// watching.
package ring

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// seriesRing is a fixed-size circular buffer for one series.
//
// Fixed-size and pre-allocated: the whole point of the hot tier is bounded
// memory. A growable buffer under a metric that suddenly reports faster is
// how an in-memory cache becomes an OOM.
type seriesRing struct {
	mu   sync.RWMutex
	ts   []int64
	vals []float32
	head int // next write position
	n    int // number of valid points, <= len(ts)
}

func newSeriesRing(capacity int) *seriesRing {
	return &seriesRing{
		ts:   make([]int64, capacity),
		vals: make([]float32, capacity),
	}
}

func (s *seriesRing) append(ts int64, v float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ts[s.head] = ts
	s.vals[s.head] = v
	s.head = (s.head + 1) % len(s.ts)
	if s.n < len(s.ts) {
		s.n++
	}
}

// rangeInto appends points with from <= ts <= to.
//
// Inclusive on both ends, and that is load-bearing for the snapshot
// protocol: the client discards buffered live points with ts <= watermark,
// so the snapshot must include the watermark point itself or it is lost by
// both paths.
func (s *seriesRing) rangeInto(dst []model.Point, id model.SeriesID, from, to int64) []model.Point {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Walk oldest to newest. The oldest entry is at head when the buffer
	// has wrapped, and at 0 when it has not.
	start := 0
	if s.n == len(s.ts) {
		start = s.head
	}
	for i := 0; i < s.n; i++ {
		idx := (start + i) % len(s.ts)
		t := s.ts[idx]
		if t < from || t > to {
			continue
		}
		dst = append(dst, model.Point{Series: id, OriginTsMs: t, Value: s.vals[idx]})
	}
	return dst
}

// latest returns the most recently appended point.
func (s *seriesRing) latest() (model.Point, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.n == 0 {
		return model.Point{}, false
	}
	idx := (s.head - 1 + len(s.ts)) % len(s.ts)
	return model.Point{OriginTsMs: s.ts[idx], Value: s.vals[idx]}, true
}

func (s *seriesRing) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.n
}

// Ring is the whole hot tier: a bounded ring per series.
type Ring struct {
	mu     sync.RWMutex
	series map[model.SeriesID]*seriesRing
	cap    int

	// watermark is the newest timestamp fully committed and visible.
	//
	// Snapshots read this BEFORE scanning (§7.2). Taking it after the scan
	// means a point written mid-scan could be in neither the snapshot nor
	// the live stream; taking it before means such a point is strictly
	// above the watermark and will arrive live.
	watermark atomic.Int64

	appended atomic.Uint64
}

// New returns a ring holding capacity points per series.
//
// At 5 Hz, 15 minutes is 4500 points per series. That is the tier-0
// retention from §8.2: enough to serve any live view's snapshot, short
// enough that a thousand series costs ~36 MB rather than gigabytes.
func New(capacity int) *Ring {
	if capacity < 1 {
		capacity = 1
	}
	return &Ring{
		series: make(map[model.SeriesID]*seriesRing),
		cap:    capacity,
	}
}

// Append adds a point and advances the watermark.
func (r *Ring) Append(id model.SeriesID, ts int64, v float32) {
	r.mu.RLock()
	s, ok := r.series[id]
	r.mu.RUnlock()

	if !ok {
		r.mu.Lock()
		// Re-check: two goroutines can both miss the read lookup for the
		// same new series, and the second would otherwise discard the
		// first's buffer along with any points already in it.
		if s, ok = r.series[id]; !ok {
			s = newSeriesRing(r.cap)
			r.series[id] = s
		}
		r.mu.Unlock()
	}
	s.append(ts, v)
	r.appended.Add(1)

	// Monotonic advance only. A late point must not move the watermark
	// backwards: doing so would place the snapshot cut behind data already
	// delivered live, and the client would re-apply it as a duplicate.
	for {
		cur := r.watermark.Load()
		if ts <= cur || r.watermark.CompareAndSwap(cur, ts) {
			break
		}
	}
}

// CommittedWatermark is the newest timestamp visible to a snapshot.
func (r *Ring) CommittedWatermark() int64 { return r.watermark.Load() }

// Range returns points for the given series within [from, to] inclusive.
//
// Results are sorted by timestamp then series id, so a snapshot is
// deterministic and two clients requesting the same window receive
// byte-identical data — which makes the protocol testable.
func (r *Ring) Range(ids []model.SeriesID, from, to int64) []model.Point {
	if from > to {
		return nil
	}

	r.mu.RLock()
	rings := make([]*seriesRing, 0, len(ids))
	kept := make([]model.SeriesID, 0, len(ids))
	for _, id := range ids {
		if s, ok := r.series[id]; ok {
			rings = append(rings, s)
			kept = append(kept, id)
		}
	}
	r.mu.RUnlock()

	var out []model.Point
	for i, s := range rings {
		out = s.rangeInto(out, kept[i], from, to)
	}

	sort.Slice(out, func(a, b int) bool {
		if out[a].OriginTsMs != out[b].OriginTsMs {
			return out[a].OriginTsMs < out[b].OriginTsMs
		}
		return out[a].Series < out[b].Series
	})
	return out
}

// Window returns the most recent n points for one series, oldest first.
//
// The detector's input. Returning a copy rather than a view because the
// detector holds it across a baseline computation while the ring keeps
// being written.
func (r *Ring) Window(id model.SeriesID, n int) []float64 {
	r.mu.RLock()
	s, ok := r.series[id]
	r.mu.RUnlock()
	if !ok {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.n == 0 {
		return nil
	}
	if n > s.n {
		n = s.n
	}

	out := make([]float64, 0, n)
	// The most recent n points end at head-1.
	start := (s.head - n + len(s.ts)) % len(s.ts)
	for i := 0; i < n; i++ {
		out = append(out, float64(s.vals[(start+i)%len(s.ts)]))
	}
	return out
}

// Latest returns the newest point for a series.
func (r *Ring) Latest(id model.SeriesID) (model.Point, bool) {
	r.mu.RLock()
	s, ok := r.series[id]
	r.mu.RUnlock()
	if !ok {
		return model.Point{}, false
	}
	p, ok := s.latest()
	p.Series = id
	return p, ok
}

// SeriesIDs lists every series with data.
func (r *Ring) SeriesIDs() []model.SeriesID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]model.SeriesID, 0, len(r.series))
	for id := range r.series {
		out = append(out, id)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

// Stats for meta-monitoring (§11.1).
type Stats struct {
	SeriesCount  int
	PointCount   int
	Appended     uint64
	WatermarkMs  int64
	CapPerSeries int
}

func (r *Ring) Stats() Stats {
	r.mu.RLock()
	rings := make([]*seriesRing, 0, len(r.series))
	for _, s := range r.series {
		rings = append(rings, s)
	}
	n := len(r.series)
	r.mu.RUnlock()

	total := 0
	for _, s := range rings {
		total += s.count()
	}
	return Stats{
		SeriesCount:  n,
		PointCount:   total,
		Appended:     r.appended.Load(),
		WatermarkMs:  r.watermark.Load(),
		CapPerSeries: r.cap,
	}
}
