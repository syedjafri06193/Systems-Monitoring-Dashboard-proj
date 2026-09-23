package detect

import (
	"sort"
)

// Seasonality.
//
// CPU at 3 a.m. is not CPU at 2 p.m. A detector with a rolling one-hour
// baseline alerts every single morning when traffic ramps, and after a week
// of that nobody reads its alerts.
//
// Of the available approaches, same-time-last-week has by far the best
// value for the effort: it handles both daily and weekly patterns with a
// lookup and a median, and -- crucially -- it degrades sensibly to the
// rolling window when there is not enough history yet, which is the state
// every new deployment is in for its first fortnight.

const (
	// MsPerDay and MsPerWeek, as the slot arithmetic needs them.
	MsPerDay  int64 = 24 * 60 * 60 * 1000
	MsPerWeek int64 = 7 * MsPerDay
)

// SlotOf returns the wall-clock slot index for a timestamp.
//
// Slots are fixed-width buckets within a week, so "the same time last week"
// is a subtraction rather than a calendar computation.
//
// Note the deliberate use of UTC-based arithmetic: doing this in local time
// means the slot index shifts by an hour twice a year, and a detector that
// silently loses its baseline on the DST boundary is a genuinely nasty
// thing to debug at 2 a.m. on a Sunday. The cost is that a daily pattern
// anchored to local business hours drifts by an hour across DST, which is a
// far smaller error than losing the baseline entirely.
func SlotOf(tsMs int64, slotWidthMs int64) int64 {
	if slotWidthMs < 1 {
		slotWidthMs = 1
	}
	within := ((tsMs % MsPerWeek) + MsPerWeek) % MsPerWeek
	return within / slotWidthMs
}

// SeasonalStore keeps per-slot history across weeks.
//
// Bounded by construction: slots-per-week x weeks-retained values. At a
// 5-minute slot width and 4 weeks that is 2016 x 4 floats per series, which
// is affordable for the series anyone actually alerts on.
type SeasonalStore struct {
	slotWidthMs int64
	weeks       int
	// history[slot] holds up to `weeks` observations, oldest first.
	history map[int64][]float64
}

func NewSeasonalStore(slotWidthMs int64, weeks int) *SeasonalStore {
	if slotWidthMs < 1 {
		slotWidthMs = 5 * 60 * 1000
	}
	if weeks < 1 {
		weeks = 4
	}
	return &SeasonalStore{
		slotWidthMs: slotWidthMs,
		weeks:       weeks,
		history:     make(map[int64][]float64),
	}
}

// Observe records a value for the slot containing tsMs.
func (s *SeasonalStore) Observe(tsMs int64, v float64) {
	slot := SlotOf(tsMs, s.slotWidthMs)
	h := append(s.history[slot], v)
	if len(h) > s.weeks {
		h = h[len(h)-s.weeks:]
	}
	s.history[slot] = h
}

// Baseline returns the seasonal expectation for a timestamp.
//
// The median across the previous k weeks' observations of this slot, not
// the mean: one bad week -- an incident, a load test, a deploy that ran
// long -- should not move the baseline for the same hour every subsequent
// week.
//
// The second return is false when there is not enough history, which is the
// caller's signal to fall back to the rolling window.
func (s *SeasonalStore) Baseline(tsMs int64, minWeeks int) (Baseline, bool) {
	if minWeeks < 1 {
		minWeeks = 2
	}
	slot := SlotOf(tsMs, s.slotWidthMs)
	h := s.history[slot]
	if len(h) < minWeeks {
		return Baseline{}, false
	}

	vals := append([]float64(nil), h...)
	sort.Float64s(vals)
	center := medianSorted(vals)

	dev := make([]float64, len(vals))
	for i, v := range vals {
		dev[i] = absf(v - center)
	}
	sort.Float64s(dev)

	return Baseline{
		Center: center,
		Spread: MADScale * medianSorted(dev),
		N:      len(vals),
	}, true
}

// SlotCount is the number of distinct slots with history, for diagnostics.
func (s *SeasonalStore) SlotCount() int { return len(s.history) }

// Combine blends a seasonal baseline with a rolling one.
//
// The seasonal center is the right expectation -- it is what this hour
// normally looks like. But the seasonal *spread*, computed across only a
// handful of weekly samples, is a poor scale estimate: with 4 weeks of
// history it is the MAD of 4 numbers.
//
// So take the center from the seasonal estimate and the spread from
// whichever is larger. Using the smaller would make the detector more
// sensitive than either estimate alone justifies, which is the wrong
// direction to be wrong in when the cost of a false positive is someone's
// sleep.
func Combine(seasonal, rolling Baseline) Baseline {
	if !seasonal.Valid() {
		return rolling
	}
	if !rolling.Valid() {
		return seasonal
	}
	spread := seasonal.Spread
	if rolling.Spread > spread {
		spread = rolling.Spread
	}
	return Baseline{Center: seasonal.Center, Spread: spread, N: seasonal.N + rolling.N}
}

func absf(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
