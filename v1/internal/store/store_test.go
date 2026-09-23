package store

import (
	"math"
	"testing"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

func pts(base int64, stepMs int64, vals ...float32) []model.Point {
	out := make([]model.Point, len(vals))
	for i, v := range vals {
		out[i] = model.Point{Series: 1, OriginTsMs: base + int64(i)*stepMs, Value: v}
	}
	return out
}

// --- the spike-visibility property -----------------------------------------

func TestAveragingHidesASpikeAndMinMaxDoesNot(t *testing.T) {
	// The §8.3 argument, made executable. A one-second burst to 100% CPU
	// averaged into a one-minute bucket alongside 59 seconds at 20% reads
	// as 21.3% -- the incident is invisible, and someone looking at the
	// dashboard concludes nothing happened.
	vals := make([]float32, 60)
	for i := range vals {
		vals[i] = 20
	}
	vals[30] = 100 // the one-second spike

	points := pts(0, 1000, vals...)
	buckets := Rollup(points, 60_000)

	if len(buckets) != 1 {
		t.Fatalf("got %d buckets", len(buckets))
	}
	b := buckets[0]

	t.Logf("one-minute bucket: avg=%.1f%% min=%.0f%% max=%.0f%%", b.Avg(), b.Min, b.Max)

	if math.Abs(b.Avg()-21.33) > 0.1 {
		t.Fatalf("avg = %.2f, expected ~21.33", b.Avg())
	}
	// The average alone would report a quiet minute.
	if b.Avg() > 30 {
		t.Fatal("this fixture no longer demonstrates the problem")
	}
	// Max preserves the incident.
	if b.Max != 100 {
		t.Fatalf("max = %v, want 100 -- the spike was lost", b.Max)
	}
}

func TestSpikeSurvivesDownsamplingToAThirtyDayChart(t *testing.T) {
	// The M6 acceptance criterion: "a one-second spike is still visible on
	// a 30-day chart, rendered as a band."
	const day = 24 * 60 * 60 * 1000
	const span = 30 * day

	// A month of one-minute rollups sitting at 20%, with one minute whose
	// max hit 100 because of a one-second burst.
	var points []model.Point
	for ts := int64(0); ts < span; ts += 60_000 {
		v := float32(20)
		if ts == 15*day {
			v = 100
		}
		points = append(points, model.Point{Series: 1, OriginTsMs: ts, Value: v})
	}

	bands := DownsampleMinMax(points, 0, span, 1000)
	if len(bands) == 0 {
		t.Fatal("no bands")
	}

	found := false
	for _, b := range bands {
		if b.Max >= 100 {
			found = true
			// And the band is tall: min-to-max spans the incident.
			if b.Min >= 100 {
				t.Fatal("band collapsed to a point; the surrounding data was lost")
			}
		}
	}
	if !found {
		t.Fatal("the spike vanished when downsampled to 1000 columns")
	}
}

func TestNaiveAveragingLosesTheSpikeAtTheSameScale(t *testing.T) {
	// The control: this is what the default would do if it averaged.
	const day = 24 * 60 * 60 * 1000
	const span = 30 * day

	var points []model.Point
	for ts := int64(0); ts < span; ts += 60_000 {
		v := float32(20)
		if ts == 15*day {
			v = 100
		}
		points = append(points, model.Point{Series: 1, OriginTsMs: ts, Value: v})
	}

	bands := DownsampleMinMax(points, 0, span, 1000)
	maxAvg := 0.0
	for _, b := range bands {
		if b.Avg > maxAvg {
			maxAvg = b.Avg
		}
	}
	t.Logf("highest column average across a 30-day chart: %.2f%% (the spike was 100%%)", maxAvg)
	if maxAvg > 25 {
		t.Fatal("this fixture no longer demonstrates that averaging hides the spike")
	}
}

// --- rollup ----------------------------------------------------------------

func TestRollupStoresFiveValues(t *testing.T) {
	b := Rollup(pts(0, 100, 1, 5, 3), 1000)
	if len(b) != 1 {
		t.Fatalf("got %d buckets", len(b))
	}
	if b[0].Min != 1 || b[0].Max != 5 || b[0].Sum != 9 || b[0].Count != 3 {
		t.Fatalf("got %+v", b[0])
	}
	if b[0].Avg() != 3 {
		t.Fatalf("avg = %v", b[0].Avg())
	}
}

func TestRollupBucketsAreAligned(t *testing.T) {
	// Buckets must start on multiples of the width, or two rollups of the
	// same data computed at different offsets disagree.
	b := Rollup(pts(1234, 100, 1, 2, 3), 1000)
	for _, x := range b {
		if x.StartMs%1000 != 0 {
			t.Fatalf("unaligned bucket start %d", x.StartMs)
		}
	}
}

func TestRollupHandlesNegativeTimestamps(t *testing.T) {
	// Go's % keeps the dividend's sign, so a naive ts-(ts%w) puts
	// pre-epoch points in the bucket above them.
	b := Rollup([]model.Point{{OriginTsMs: -1500, Value: 1}}, 1000)
	if len(b) != 1 {
		t.Fatalf("got %d buckets", len(b))
	}
	if b[0].StartMs != -2000 {
		t.Fatalf("bucket start = %d, want -2000", b[0].StartMs)
	}
}

func TestRollupSkipsNaN(t *testing.T) {
	b := Rollup(pts(0, 100, 1, float32(math.NaN()), 3), 1000)
	if b[0].Count != 2 || math.IsNaN(b[0].Sum) {
		t.Fatalf("NaN leaked into the rollup: %+v", b[0])
	}
}

func TestRollupIsSorted(t *testing.T) {
	points := []model.Point{
		{OriginTsMs: 5000, Value: 1},
		{OriginTsMs: 1000, Value: 2},
		{OriginTsMs: 3000, Value: 3},
	}
	b := Rollup(points, 1000)
	for i := 1; i < len(b); i++ {
		if b[i].StartMs < b[i-1].StartMs {
			t.Fatal("rollup not sorted")
		}
	}
}

func TestMergeRollupsComposes(t *testing.T) {
	// Min and max compose; an average does not. This is the other reason
	// to store sum and count rather than the quotient -- a 1-minute rollup
	// is built from the 1-second one, not from raw data.
	a := []model.Bucket{{StartMs: 0, Min: 5, Max: 10, Sum: 30, Count: 4}}
	b := []model.Bucket{{StartMs: 0, Min: 1, Max: 100, Sum: 200, Count: 4}}

	m := MergeRollups(a, b)
	if len(m) != 1 {
		t.Fatalf("got %d buckets", len(m))
	}
	if m[0].Min != 1 || m[0].Max != 100 {
		t.Fatalf("extremes did not compose: %+v", m[0])
	}
	if m[0].Avg() != 28.75 {
		t.Fatalf("avg = %v, want 28.75 (230/8)", m[0].Avg())
	}
}

func TestMergeRollupsKeepsDisjointBuckets(t *testing.T) {
	a := []model.Bucket{{StartMs: 0, Min: 1, Max: 1, Sum: 1, Count: 1}}
	b := []model.Bucket{{StartMs: 1000, Min: 2, Max: 2, Sum: 2, Count: 1}}
	if m := MergeRollups(a, b); len(m) != 2 {
		t.Fatalf("got %d buckets", len(m))
	}
}

func TestEmptyBucketMerge(t *testing.T) {
	full := model.Bucket{StartMs: 0, Min: 1, Max: 2, Sum: 3, Count: 2}
	if got := full.Merge(model.Bucket{}); got != full {
		t.Fatalf("merging an empty bucket changed the result: %+v", got)
	}
	if got := (model.Bucket{}).Merge(full); got != full {
		t.Fatalf("merging into an empty bucket lost data: %+v", got)
	}
}

// --- downsampling ----------------------------------------------------------

func TestDownsampleRespectsColumnLimit(t *testing.T) {
	var points []model.Point
	for i := 0; i < 100000; i++ {
		points = append(points, model.Point{OriginTsMs: int64(i), Value: float32(i)})
	}
	bands := DownsampleMinMax(points, 0, 100000, 1000)
	if len(bands) > 1000 {
		t.Fatalf("got %d bands for 1000 columns", len(bands))
	}
}

func TestDownsampleOmitsEmptyBucketsRatherThanZeroing(t *testing.T) {
	// A zero-valued gap is indistinguishable from a real zero, which for
	// error rate is exactly the wrong thing to draw.
	points := []model.Point{
		{OriginTsMs: 0, Value: 5},
		{OriginTsMs: 9000, Value: 5},
	}
	bands := DownsampleMinMax(points, 0, 10000, 10)
	if len(bands) != 2 {
		t.Fatalf("got %d bands, want 2 (the rest are empty)", len(bands))
	}
	for _, b := range bands {
		if b.Count == 0 {
			t.Fatal("emitted an empty band")
		}
	}
}

func TestDownsampleKeepsTheFinalPoint(t *testing.T) {
	// The point landing exactly on `to` would index one past the end. It
	// is also the most recent sample, which is the one the viewer is most
	// likely to be looking at.
	points := []model.Point{
		{OriginTsMs: 0, Value: 1},
		{OriginTsMs: 1000, Value: 99},
	}
	bands := DownsampleMinMax(points, 0, 1000, 10)
	found := false
	for _, b := range bands {
		if b.Max == 99 {
			found = true
		}
	}
	if !found {
		t.Fatal("the point at the range end was dropped")
	}
}

func TestDownsampleIgnoresOutOfRangePoints(t *testing.T) {
	points := []model.Point{
		{OriginTsMs: -5000, Value: 999},
		{OriginTsMs: 500, Value: 5},
		{OriginTsMs: 50000, Value: 999},
	}
	bands := DownsampleMinMax(points, 0, 1000, 10)
	for _, b := range bands {
		if b.Max == 999 {
			t.Fatal("an out-of-range point was included")
		}
	}
}

func TestDownsampleEdgeCases(t *testing.T) {
	if got := DownsampleMinMax(nil, 0, 100, 10); got != nil {
		t.Fatal("nil input produced bands")
	}
	if got := DownsampleMinMax(pts(0, 1, 1), 0, 100, 0); got != nil {
		t.Fatal("zero columns produced bands")
	}
	if got := DownsampleMinMax(pts(0, 1, 1), 100, 0, 10); got != nil {
		t.Fatal("inverted range produced bands")
	}
	// A zero-width range must not divide by zero.
	if got := DownsampleMinMax([]model.Point{{OriginTsMs: 5, Value: 1}}, 5, 5, 10); len(got) != 1 {
		t.Fatalf("zero-width range gave %d bands", len(got))
	}
}

func TestLTTBKeepsEndpointsAndCount(t *testing.T) {
	var points []model.Point
	for i := 0; i < 1000; i++ {
		points = append(points, model.Point{OriginTsMs: int64(i), Value: float32(math.Sin(float64(i) / 20))})
	}
	out := DownsampleLTTB(points, 100)

	if len(out) != 100 {
		t.Fatalf("got %d points, want 100", len(out))
	}
	if out[0] != points[0] {
		t.Fatal("first point not preserved")
	}
	if out[len(out)-1] != points[len(points)-1] {
		t.Fatal("last point not preserved")
	}
}

func TestLTTBPreservesOrder(t *testing.T) {
	var points []model.Point
	for i := 0; i < 500; i++ {
		points = append(points, model.Point{OriginTsMs: int64(i), Value: float32(i % 17)})
	}
	out := DownsampleLTTB(points, 50)
	for i := 1; i < len(out); i++ {
		if out[i].OriginTsMs <= out[i-1].OriginTsMs {
			t.Fatalf("out of order at %d", i)
		}
	}
}

func TestLTTBPassesThroughWhenUnderThreshold(t *testing.T) {
	points := pts(0, 100, 1, 2, 3)
	if got := DownsampleLTTB(points, 100); len(got) != 3 {
		t.Fatalf("got %d", len(got))
	}
	if got := DownsampleLTTB(points, 2); len(got) != 3 {
		t.Fatalf("threshold below 3 should pass through, got %d", len(got))
	}
}

func TestLTTBKeepsAProminentSpike(t *testing.T) {
	// LTTB is not a substitute for min/max, but it should keep a spike
	// that dominates the silhouette.
	points := make([]model.Point, 1000)
	for i := range points {
		points[i] = model.Point{OriginTsMs: int64(i), Value: 10}
	}
	points[500].Value = 1000

	out := DownsampleLTTB(points, 50)
	found := false
	for _, p := range out {
		if p.Value == 1000 {
			found = true
		}
	}
	if !found {
		t.Fatal("LTTB dropped a dominant spike")
	}
}

func TestDownsampleLastIsCorrectForGauges(t *testing.T) {
	// Averaging a status enum produces a number the metric never held;
	// min/max implies a range that never existed.
	points := pts(0, 100, 1, 1, 2, 2, 3)
	out := DownsampleLast(points, 0, 500, 5)
	for _, p := range out {
		if p.Value != 1 && p.Value != 2 && p.Value != 3 {
			t.Fatalf("invented a value the gauge never held: %v", p.Value)
		}
	}
}

func TestDownsampleLastTakesTheLatestInEachBucket(t *testing.T) {
	points := pts(0, 100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	out := DownsampleLast(points, 0, 1000, 2)
	if len(out) != 2 {
		t.Fatalf("got %d", len(out))
	}
	if out[0].Value != 5 {
		t.Fatalf("first bucket kept %v, want the last value 5", out[0].Value)
	}
}

func TestAlgorithmStringsAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for _, a := range []Algorithm{MinMax, LTTB, LastValue, Average} {
		if seen[a.String()] {
			t.Fatalf("duplicate %q", a.String())
		}
		seen[a.String()] = true
	}
}

// --- tier routing ----------------------------------------------------------

func TestSelectTierByRangeAndResolution(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	cases := []struct {
		name string
		from time.Duration // ago
		to   time.Duration // ago
		px   int
		want Tier
	}{
		{"live tail", 5 * time.Minute, 0, 1000, TierHot},
		{"last hour", time.Hour, 0, 1000, TierRaw},
		// 24 h across 1000 px is 86 s per pixel, so the 1-minute tier is
		// the right answer: the raw tier would return 86x more points
		// than the chart can draw.
		{"last day", 24 * time.Hour, 0, 1000, TierMinute},
		// The same range on a much wider chart does want raw.
		{"last day, wide chart", 24 * time.Hour, 0, 20000, TierRaw},
		{"last week", 7 * 24 * time.Hour, 0, 1000, TierMinute},
		{"last year", 365 * 24 * time.Hour, 0, 1000, TierHour},
	}

	for _, c := range cases {
		got := SelectTier(now.Add(-c.from), now.Add(-c.to), now, c.px)
		if got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestSelectTierNeverExceedsPixelWidth(t *testing.T) {
	// The governing rule: never return more points than the chart can
	// physically draw.
	now := time.Unix(1_700_000_000, 0)
	for _, span := range []time.Duration{
		time.Minute, time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 365 * 24 * time.Hour,
	} {
		const px = 1000
		from := now.Add(-span)
		tier := SelectTier(from, now, now, px)
		points := EstimatedPoints(tier, from, now, 1)
		// One series should not blow the viewport by more than a small
		// factor; downsampling handles the rest.
		if points > px*20 {
			t.Errorf("span %v routed to %v giving %d points for %d pixels", span, tier, points, px)
		}
	}
}

func TestSelectTierRespectsHotRetention(t *testing.T) {
	// A high-resolution request older than the ring's retention cannot be
	// served from memory, however fine the target step.
	now := time.Unix(1_700_000_000, 0)
	old := now.Add(-time.Hour)
	if got := SelectTier(old, old.Add(time.Second), now, 1000); got == TierHot {
		t.Fatal("routed a request older than hot retention to the hot tier")
	}
}

func TestSelectTierHandlesDegenerateInput(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	// Zero pixel width and an inverted range must not divide by zero.
	_ = SelectTier(now, now, now, 0)
	_ = SelectTier(now, now.Add(-time.Hour), now, 1000)
}

func TestValidateQueryRejectsAbsurdRequests(t *testing.T) {
	// A year at 200 ms is a mistake, and the useful response is to say so
	// rather than to spend four minutes producing 157 million points.
	now := time.Unix(1_700_000_000, 0)
	from := now.Add(-365 * 24 * time.Hour)

	if err := ValidateQuery(TierHot, from, now, 1, 1000); err == nil {
		t.Fatal("accepted a year at hot resolution")
	} else {
		t.Logf("rejected with: %v", err)
	}

	if err := ValidateQuery(TierHour, from, now, 1, 1000); err != nil {
		t.Fatalf("rejected a reasonable query: %v", err)
	}
}

func TestValidateQueryRejectsInvertedRange(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	if err := ValidateQuery(TierRaw, now, now.Add(-time.Hour), 1, 1000); err == nil {
		t.Fatal("accepted an inverted range")
	}
}

func TestEstimatedPointsScalesWithSeries(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	one := EstimatedPoints(TierRaw, now.Add(-time.Hour), now, 1)
	ten := EstimatedPoints(TierRaw, now.Add(-time.Hour), now, 10)
	if ten != one*10 {
		t.Fatalf("%d vs %d", ten, one)
	}
}

func TestTargetColumnsIsClamped(t *testing.T) {
	if got := TargetColumns(0); got < 16 {
		t.Fatalf("got %d", got)
	}
	if got := TargetColumns(100000); got > 4096 {
		t.Fatalf("got %d", got)
	}
	if got := TargetColumns(800); got != 800 {
		t.Fatalf("got %d", got)
	}
}

func TestTierStepsAndRetentionsAreOrdered(t *testing.T) {
	tiers := []Tier{TierHot, TierRaw, TierMinute, TierHour}
	for i := 1; i < len(tiers); i++ {
		if tiers[i].Step() <= tiers[i-1].Step() {
			t.Fatalf("%v step is not coarser than %v", tiers[i], tiers[i-1])
		}
		if tiers[i].Retention() <= tiers[i-1].Retention() {
			t.Fatalf("%v retention is not longer than %v", tiers[i], tiers[i-1])
		}
	}
}
