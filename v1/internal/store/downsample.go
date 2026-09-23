// Package store handles aggregation, tiering and viewport downsampling.
package store

import (
	"math"
	"sort"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// Downsampling to the viewport.
//
// A 1000-pixel-wide chart cannot display more than 1000 x-positions.
// Sending 100,000 points to draw 1,000 columns wastes bandwidth, decode
// time and render time for zero visible benefit.
//
// The algorithm choice is not a detail:
//
//	naive averaging  HIDES SPIKES. Never use alone for monitoring.
//	min/max          preserves extremes; renders as a band. THE DEFAULT.
//	LTTB             preserves visual shape with one point per bucket.
//	last             correct for gauges and state-like series.
//
// Min/max is the right default here because the entire purpose of the tool
// is noticing that something spiked.

// Algorithm selects a downsampler.
type Algorithm uint8

const (
	MinMax Algorithm = iota
	LTTB
	LastValue
	Average
)

func (a Algorithm) String() string {
	switch a {
	case MinMax:
		return "minmax"
	case LTTB:
		return "lttb"
	case LastValue:
		return "last"
	case Average:
		return "average"
	default:
		return "unknown"
	}
}

// Band is one rendered column: a min-max extent plus a central value.
//
// Two values per column, not one. A one-second burst to 100% CPU averaged
// into a one-minute bucket alongside 59 seconds at 20% reads as 21.3% --
// the incident becomes invisible, and for a monitoring tool that is a
// correctness failure rather than a fidelity trade-off.
type Band struct {
	TsMs  int64
	Min   float64
	Max   float64
	Avg   float64
	Count int
}

// DownsampleMinMax reduces points to at most `columns` bands.
//
// Returns fewer bands than columns when the input is sparse; empty buckets
// are omitted rather than emitted as zeros. A zero-valued gap is
// indistinguishable from a real zero, which for a metric like error rate is
// exactly the wrong thing to draw.
func DownsampleMinMax(points []model.Point, from, to int64, columns int) []Band {
	if len(points) == 0 || columns < 1 || from > to {
		return nil
	}
	if to == from {
		to = from + 1
	}

	width := float64(to-from) / float64(columns)
	if width <= 0 {
		width = 1
	}

	bands := make([]Band, columns)
	for i := range bands {
		bands[i].Min = math.Inf(1)
		bands[i].Max = math.Inf(-1)
		bands[i].TsMs = from + int64(float64(i)*width)
	}

	for _, p := range points {
		if p.OriginTsMs < from || p.OriginTsMs > to {
			continue
		}
		v := float64(p.Value)
		if math.IsNaN(v) {
			continue
		}
		i := int(float64(p.OriginTsMs-from) / width)
		if i >= columns {
			// The final point lands exactly on `to`, which would index one
			// past the end. Clamping keeps it in the last column rather
			// than dropping the most recent sample -- the one the viewer
			// is most likely to be looking at.
			i = columns - 1
		}
		b := &bands[i]
		if v < b.Min {
			b.Min = v
		}
		if v > b.Max {
			b.Max = v
		}
		b.Avg += v
		b.Count++
	}

	out := make([]Band, 0, columns)
	for _, b := range bands {
		if b.Count == 0 {
			continue
		}
		b.Avg /= float64(b.Count)
		out = append(out, b)
	}
	return out
}

// DownsampleLTTB implements Largest Triangle Three Buckets.
//
// Preserves the *visual shape* of a series with one point per bucket, which
// is what you want when a band would be visual noise -- a single clean line
// on an overview chart.
//
// It is not a substitute for min/max on a monitoring chart: LTTB keeps the
// points that contribute most to the silhouette, which usually includes a
// spike but is not guaranteed to.
func DownsampleLTTB(points []model.Point, threshold int) []model.Point {
	n := len(points)
	if threshold < 3 || n <= threshold {
		return points
	}

	out := make([]model.Point, 0, threshold)
	// The first and last points are always kept: they anchor the line to
	// the real extent of the data.
	out = append(out, points[0])

	// Bucket size excluding the two fixed endpoints.
	every := float64(n-2) / float64(threshold-2)

	a := 0 // index of the previously selected point
	for i := 0; i < threshold-2; i++ {
		// Average of the *next* bucket, forming the far vertex of the
		// triangle.
		avgStart := int(float64(i+1)*every) + 1
		avgEnd := int(float64(i+2)*every) + 1
		if avgEnd > n {
			avgEnd = n
		}
		if avgStart >= avgEnd {
			avgStart = avgEnd - 1
		}

		var avgTs, avgVal float64
		for j := avgStart; j < avgEnd; j++ {
			avgTs += float64(points[j].OriginTsMs)
			avgVal += float64(points[j].Value)
		}
		cnt := float64(avgEnd - avgStart)
		avgTs /= cnt
		avgVal /= cnt

		rangeStart := int(float64(i)*every) + 1
		rangeEnd := int(float64(i+1)*every) + 1
		if rangeEnd > n-1 {
			rangeEnd = n - 1
		}

		pointATs := float64(points[a].OriginTsMs)
		pointAVal := float64(points[a].Value)

		maxArea := -1.0
		best := rangeStart
		for j := rangeStart; j < rangeEnd; j++ {
			area := math.Abs(
				(pointATs-avgTs)*(float64(points[j].Value)-pointAVal)-
					(pointATs-float64(points[j].OriginTsMs))*(avgVal-pointAVal),
			) * 0.5
			if area > maxArea {
				maxArea = area
				best = j
			}
		}

		out = append(out, points[best])
		a = best
	}

	out = append(out, points[n-1])
	return out
}

// DownsampleLast keeps the last value in each bucket.
//
// Correct for gauges and state-like series -- a status enum, a
// configuration value, a connection count. Averaging those produces a
// number the metric never held, and min/max produces a band that implies a
// range that never existed.
func DownsampleLast(points []model.Point, from, to int64, columns int) []model.Point {
	if len(points) == 0 || columns < 1 || from > to {
		return nil
	}
	if to == from {
		to = from + 1
	}
	width := float64(to-from) / float64(columns)

	type slot struct {
		p  model.Point
		ok bool
	}
	slots := make([]slot, columns)

	for _, p := range points {
		if p.OriginTsMs < from || p.OriginTsMs > to {
			continue
		}
		i := int(float64(p.OriginTsMs-from) / width)
		if i >= columns {
			i = columns - 1
		}
		if !slots[i].ok || p.OriginTsMs >= slots[i].p.OriginTsMs {
			slots[i] = slot{p: p, ok: true}
		}
	}

	out := make([]model.Point, 0, columns)
	for _, s := range slots {
		if s.ok {
			out = append(out, s.p)
		}
	}
	return out
}

// Rollup aggregates points into fixed-width buckets.
//
// Every bucket stores five values -- min, max, sum, count and the derived
// average -- because storing the average alone throws away exactly the
// information a monitoring tool exists to surface.
func Rollup(points []model.Point, bucketMs int64) []model.Bucket {
	if len(points) == 0 || bucketMs < 1 {
		return nil
	}

	byBucket := make(map[int64]*model.Bucket)
	for _, p := range points {
		v := float64(p.Value)
		if math.IsNaN(v) {
			continue
		}
		// Floor division that works for negative timestamps too: Go's %
		// keeps the dividend's sign, so a naive ts-(ts%w) puts pre-epoch
		// points in the bucket above them.
		start := floorDiv(p.OriginTsMs, bucketMs) * bucketMs

		b, ok := byBucket[start]
		if !ok {
			b = &model.Bucket{StartMs: start, Min: v, Max: v}
			byBucket[start] = b
		}
		if v < b.Min {
			b.Min = v
		}
		if v > b.Max {
			b.Max = v
		}
		b.Sum += v
		b.Count++
	}

	out := make([]model.Bucket, 0, len(byBucket))
	for _, b := range byBucket {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartMs < out[j].StartMs })
	return out
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// MergeRollups combines two aligned rollup series.
//
// What a materialized view does on insert: the 1-minute rollup is built
// from the 1-second one rather than rescanning raw data. Min and max
// compose; an average does not, which is the other reason to store sum and
// count rather than the quotient.
func MergeRollups(a, b []model.Bucket) []model.Bucket {
	byStart := make(map[int64]model.Bucket, len(a)+len(b))
	for _, x := range a {
		byStart[x.StartMs] = x
	}
	for _, x := range b {
		if prev, ok := byStart[x.StartMs]; ok {
			byStart[x.StartMs] = prev.Merge(x)
		} else {
			byStart[x.StartMs] = x
		}
	}
	out := make([]model.Bucket, 0, len(byStart))
	for _, v := range byStart {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartMs < out[j].StartMs })
	return out
}
