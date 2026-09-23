package store

import (
	"fmt"
	"time"
)

// Tiering.
//
//	0 hot     native (200 ms)  15 min   in-memory ring on the gateway
//	1 raw     1 s              24 h     TSDB
//	2 rollup  1 min            30 d     TSDB, materialized view
//	3 rollup  1 h              1 y      TSDB, materialized view
//
// The query router picks a tier from the requested range and the target
// resolution, and the governing rule is that it must never return more
// points than the chart can physically draw.

type Tier uint8

const (
	TierHot Tier = iota
	TierRaw
	TierMinute
	TierHour
)

func (t Tier) String() string {
	switch t {
	case TierHot:
		return "hot"
	case TierRaw:
		return "raw"
	case TierMinute:
		return "1m"
	case TierHour:
		return "1h"
	default:
		return "unknown"
	}
}

// Step is the tier's native resolution.
func (t Tier) Step() time.Duration {
	switch t {
	case TierHot:
		return 200 * time.Millisecond
	case TierRaw:
		return time.Second
	case TierMinute:
		return time.Minute
	default:
		return time.Hour
	}
}

// Retention is how far back the tier holds data.
func (t Tier) Retention() time.Duration {
	switch t {
	case TierHot:
		return 15 * time.Minute
	case TierRaw:
		return 24 * time.Hour
	case TierMinute:
		return 30 * 24 * time.Hour
	default:
		return 365 * 24 * time.Hour
	}
}

// HotRetention is exported because the ring's capacity is derived from it.
const HotRetention = 15 * time.Minute

// SelectTier chooses the cheapest tier that can serve a request.
//
// `now` is a parameter rather than time.Now() so the routing is testable
// without sleeping, and so a replay can route exactly as the live system
// would have at the time.
func SelectTier(from, to, now time.Time, pixelWidth int) Tier {
	if pixelWidth < 1 {
		pixelWidth = 1
	}
	span := to.Sub(from)
	if span <= 0 {
		span = time.Millisecond
	}

	// Never return more points than the chart can physically draw.
	targetStep := span / time.Duration(pixelWidth)
	age := now.Sub(from)

	// The retention comparisons are inclusive. "Last 24 hours" is the most
	// common dashboard range and it lands exactly on the raw tier's
	// retention boundary; an exclusive comparison would silently serve it
	// from the 1-minute tier, costing resolution on the query people run
	// most. At the boundary the oldest points may be mid-expiry, which
	// shows up as a slightly short range rather than as wrong data.
	switch {
	case age <= HotRetention && targetStep < time.Second:
		return TierHot
	case targetStep < time.Minute && age <= TierRaw.Retention():
		return TierRaw
	case targetStep < time.Hour && age <= TierMinute.Retention():
		return TierMinute
	default:
		return TierHour
	}
}

// EstimatedPoints is how many points a tier would return for a range.
//
// Used to reject a query that would be absurd before running it. A user who
// asks for a year at 200 ms resolution has made a mistake, and the useful
// response is to say so rather than to spend four minutes producing 157
// million points nobody can render.
func EstimatedPoints(t Tier, from, to time.Time, seriesCount int) int64 {
	span := to.Sub(from)
	if span <= 0 || seriesCount < 1 {
		return 0
	}
	return int64(span/t.Step()) * int64(seriesCount)
}

// MaxQueryPoints bounds a single query's result.
const MaxQueryPoints = 5_000_000

// ValidateQuery rejects a request that cannot be served sensibly.
func ValidateQuery(t Tier, from, to time.Time, seriesCount, pixelWidth int) error {
	if !to.After(from) {
		return fmt.Errorf("store: range end %v is not after start %v", to, from)
	}
	if n := EstimatedPoints(t, from, to, seriesCount); n > MaxQueryPoints {
		return fmt.Errorf(
			"store: query would read ~%d points from the %s tier (limit %d); "+
				"narrow the range, reduce the series count, or accept a coarser tier",
			n, t, MaxQueryPoints)
	}
	return nil
}

// TargetColumns is how many x-positions to downsample to.
//
// Capped at the pixel width because more is invisible, and floored at a
// small number so a sparkline still shows shape.
func TargetColumns(pixelWidth int) int {
	if pixelWidth < 16 {
		return 16
	}
	if pixelWidth > 4096 {
		return 4096
	}
	return pixelWidth
}
