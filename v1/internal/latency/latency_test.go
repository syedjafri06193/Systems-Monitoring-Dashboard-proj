package latency

import (
	"math"
	"sync"
	"testing"
	"time"
)

// --- the argument for the architecture -------------------------------------

func TestScrapeAtFifteenSecondsCannotBeSubSecond(t *testing.T) {
	// The finding that reshapes the project: sub-second latency is a
	// collection-architecture problem, not a transport problem. However
	// fast the WebSocket, a 15-second scrape has a median staleness of
	// 7.5 seconds.
	s := Scenario{"scrape", 15 * time.Second, 100}

	if math.Abs(s.P50()-7600) > 50 {
		t.Fatalf("P50 = %.0f ms, expected ~7600", s.P50())
	}
	if s.MeetsSubSecond() {
		t.Fatal("a 15 s scrape claimed to meet sub-second P99")
	}
	t.Logf("15 s scrape: P50 %.0f ms, P99 %.0f ms", s.P50(), s.P99())
}

func TestOneHertzMakesP50ButMissesP99(t *testing.T) {
	// The subtle row of the table, and the reason the answer is 5 Hz
	// rather than 1 Hz: at 1 Hz the collection interval alone can
	// contribute a full second in the tail.
	s := Scenario{"1 Hz", time.Second, 100}

	if s.P50() > 1000 {
		t.Fatalf("P50 = %.0f ms; 1 Hz should make the median comfortably", s.P50())
	}
	if s.MeetsSubSecond() {
		t.Fatalf("1 Hz claimed sub-second P99 (%.0f ms); it should miss", s.P99())
	}
	t.Logf("1 Hz push: P50 %.0f ms, P99 %.0f ms", s.P50(), s.P99())
}

func TestFiveHertzMeetsTheSLO(t *testing.T) {
	s := Scenario{"5 Hz", 200 * time.Millisecond, 100}
	if !s.MeetsSubSecond() {
		t.Fatalf("5 Hz missed sub-second P99: %.0f ms", s.P99())
	}
	t.Logf("5 Hz push: P50 %.0f ms, P99 %.0f ms", s.P50(), s.P99())
}

func TestTheStandardScenarioTable(t *testing.T) {
	// The whole §3.2 table in one place. This is the argument for the
	// architecture and it should be re-derivable rather than quoted.
	for _, s := range StandardScenarios() {
		t.Logf("%-24s P50 %7.0f ms  P99 %7.0f ms  sub-second: %v",
			s.Name, s.P50(), s.P99(), s.MeetsSubSecond())
	}

	scenarios := StandardScenarios()
	if scenarios[0].MeetsSubSecond() || scenarios[1].MeetsSubSecond() {
		t.Fatal("a slower-than-5-Hz scenario claimed to meet the SLO")
	}
	if !scenarios[2].MeetsSubSecond() {
		t.Fatal("5 Hz did not meet the SLO")
	}
}

func TestRequiredCollectionIntervalSolvesForTheTarget(t *testing.T) {
	// Decide the collection frequency *from* the P99 requirement, not the
	// other way around.
	got := RequiredCollectionInterval(1000, 100)
	t.Logf("to meet a 1 s P99 with 100 ms of other overhead: collect every %v", got)

	if got > time.Second {
		t.Fatalf("required interval %v is not fast enough", got)
	}
	// And the answer must actually satisfy the constraint it solved for.
	s := Scenario{"solved", got, 100}
	if !s.MeetsSubSecond() {
		t.Fatalf("the solved interval %v gives P99 %.0f ms", got, s.P99())
	}
}

func TestImpossibleTargetReturnsZero(t *testing.T) {
	// A 50 ms target with 100 ms of fixed overhead cannot be met at any
	// collection frequency, and saying so beats returning a negative
	// duration.
	if got := RequiredCollectionInterval(50, 100); got != 0 {
		t.Fatalf("got %v for an unachievable target", got)
	}
}

func TestFiveHertzCostsSeventyFiveTimesTheData(t *testing.T) {
	// §3.3: nothing is free. This is the number that justifies
	// demand-driven collection.
	scrape := ComputeVolume(15*time.Second, 1000, 16, 1.4)
	push := ComputeVolume(200*time.Millisecond, 1000, 16, 1.4)

	ratio := float64(push.PointsPerDay) / float64(scrape.PointsPerDay)
	t.Logf("15 s scrape: %d pts/series/day, %.0f MB/day raw, %.0f MB/day compressed",
		scrape.PointsPerSeriesPerDay,
		float64(scrape.RawBytesPerDay)/1e6, float64(scrape.CompressedBytesPerDay)/1e6)
	t.Logf("5 Hz push:   %d pts/series/day, %.1f GB/day raw, %.0f MB/day compressed",
		push.PointsPerSeriesPerDay,
		float64(push.RawBytesPerDay)/1e9, float64(push.CompressedBytesPerDay)/1e6)
	t.Logf("ratio: %.0fx", ratio)

	if math.Abs(ratio-75) > 1 {
		t.Fatalf("ratio = %.1f, expected 75x", ratio)
	}
	if scrape.PointsPerSeriesPerDay != 5760 {
		t.Fatalf("scrape points/series/day = %d, expected 5760", scrape.PointsPerSeriesPerDay)
	}
	if push.PointsPerSeriesPerDay != 432_000 {
		t.Fatalf("push points/series/day = %d, expected 432000", push.PointsPerSeriesPerDay)
	}
}

func TestComputeVolumeRejectsDegenerateInput(t *testing.T) {
	if got := ComputeVolume(0, 100, 16, 1.4); got.PointsPerDay != 0 {
		t.Fatal("zero interval produced a volume")
	}
	if got := ComputeVolume(time.Second, 0, 16, 1.4); got.PointsPerDay != 0 {
		t.Fatal("zero series produced a volume")
	}
}

// --- histogram -------------------------------------------------------------

func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram(10000)
	for i := 1; i <= 1000; i++ {
		h.Observe(float64(i))
	}
	s := h.Summary()

	if s.Count != 1000 {
		t.Fatalf("count = %d", s.Count)
	}
	if s.P50 < 495 || s.P50 > 505 {
		t.Fatalf("p50 = %v", s.P50)
	}
	if s.P99 < 985 || s.P99 > 995 {
		t.Fatalf("p99 = %v", s.P99)
	}
	if s.Min != 1 || s.Max != 1000 {
		t.Fatalf("min/max = %v/%v", s.Min, s.Max)
	}
}

func TestMeanHidesTheTailThatPercentilesShow(t *testing.T) {
	// Why the summary reports a distribution rather than a number: nine
	// fast frames and one slow one average to something comfortable while
	// the viewer sees a stutter.
	// 2% slow, not 1%: exactly 1% sits on the p99 boundary, where the
	// 99th percentile of 1000 samples is the 990th value and correctly
	// reports the fast number. A tail thinner than 1-in-100 is invisible
	// to p99 by definition, which is itself worth knowing when choosing
	// which percentile to alert on.
	h := NewHistogram(10000)
	for i := 0; i < 980; i++ {
		h.Observe(30)
	}
	for i := 0; i < 20; i++ {
		h.Observe(5000)
	}
	s := h.Summary()

	t.Logf("mean %.0f ms hides a p99 of %.0f ms", s.Mean, s.P99)
	if s.Mean > 200 {
		t.Fatal("this fixture no longer demonstrates the point")
	}
	if s.P99 < 1000 {
		t.Fatalf("p99 = %v, expected the tail to show", s.P99)
	}
	if s.MeetsSubSecondSLO() {
		t.Fatal("claimed to meet the SLO with a 5 s tail")
	}
}

func TestHistogramIgnoresNaN(t *testing.T) {
	h := NewHistogram(100)
	h.Observe(math.NaN())
	h.Observe(10)
	s := h.Summary()
	if s.Count != 1 || math.IsNaN(s.Mean) {
		t.Fatalf("NaN leaked in: %+v", s)
	}
}

func TestHistogramBoundsMemory(t *testing.T) {
	h := NewHistogram(100)
	for i := 0; i < 100_000; i++ {
		h.Observe(float64(i))
	}
	h.mu.Lock()
	n := len(h.samples)
	h.mu.Unlock()

	if n > 100 {
		t.Fatalf("retained %d samples against a cap of 100", n)
	}
	// Count and extremes still reflect everything observed.
	s := h.Summary()
	if s.Count != 100_000 {
		t.Fatalf("count = %d", s.Count)
	}
	if s.Max != 99_999 {
		t.Fatalf("max = %v", s.Max)
	}
}

func TestHistogramKeepsSamplingAfterTheCap(t *testing.T) {
	// A long-running process must not report its first 100 samples
	// forever; the distribution has to keep tracking reality.
	h := NewHistogram(100)
	for i := 0; i < 100; i++ {
		h.Observe(10)
	}
	for i := 0; i < 10_000; i++ {
		h.Observe(500)
	}
	if p := h.Summary().P50; p < 400 {
		t.Fatalf("p50 = %v; the reservoir froze at startup values", p)
	}
}

func TestEmptyHistogram(t *testing.T) {
	s := NewHistogram(100).Summary()
	if s.Count != 0 || s.P99 != 0 {
		t.Fatalf("got %+v", s)
	}
	if s.MeetsSubSecondSLO() {
		t.Fatal("an empty histogram claimed to meet the SLO")
	}
}

func TestHistogramReset(t *testing.T) {
	h := NewHistogram(100)
	h.Observe(5)
	h.Reset()
	if h.Summary().Count != 0 {
		t.Fatal("reset left samples")
	}
}

func TestHistogramIsConcurrencySafe(t *testing.T) {
	h := NewHistogram(1000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				h.Observe(float64(i))
				if i%100 == 0 {
					h.Summary()
				}
			}
		}()
	}
	wg.Wait()
	if got := h.Summary().Count; got != 16000 {
		t.Fatalf("count = %d, want 16000", got)
	}
}

// --- budget ----------------------------------------------------------------

func TestBudgetNamesTheDominantStage(t *testing.T) {
	// The output that changes behaviour. The document's whole argument is
	// that people optimise the wrong term, so the instrumentation must say
	// which term is the big one.
	b := NewBudget(1000)
	for i := 0; i < 100; i++ {
		b.Observe(StageCollection, 7500) // the 15 s scrape
		b.Observe(StageDecode, 3)
		b.Observe(StageNetworkOut, 20)
		b.Observe(StageFrame, 16)
		b.ObserveEndToEnd(7539)
	}

	stage, s := b.Dominant()
	if stage != StageCollection {
		t.Fatalf("dominant stage = %v, want collection", stage)
	}
	t.Logf("dominant: %v at p99 %.0f ms", stage, s.P99)
	t.Logf("\n%s", b.Report())
}

func TestBudgetDominantIgnoresUnobservedStages(t *testing.T) {
	b := NewBudget(100)
	b.Observe(StageDecode, 5)
	stage, s := b.Dominant()
	if stage != StageDecode || s.Count != 1 {
		t.Fatalf("got %v %+v", stage, s)
	}
}

func TestEmptyBudgetReportsNothing(t *testing.T) {
	b := NewBudget(100)
	_, s := b.Dominant()
	if s.Count != 0 {
		t.Fatal("empty budget named a dominant stage")
	}
}

func TestStageStringsAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for i := Stage(0); i < numStages; i++ {
		if seen[i.String()] {
			t.Fatalf("duplicate stage string %q", i.String())
		}
		seen[i.String()] = true
	}
}

func TestBudgetIgnoresOutOfRangeStage(t *testing.T) {
	b := NewBudget(100)
	b.Observe(Stage(200), 5)
	if s := b.Stage(Stage(200)); s.Count != 0 {
		t.Fatal("an out-of-range stage recorded a sample")
	}
}
