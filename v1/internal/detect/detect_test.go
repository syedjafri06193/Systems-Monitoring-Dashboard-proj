package detect

import (
	"math"
	"math/rand"
	"testing"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// --- robust statistics -----------------------------------------------------

func TestRobustBaselineOnCleanData(t *testing.T) {
	// For normal data, 1.4826 x MAD should approximate sigma. Without the
	// rescaling a MAD z-score is on a different scale from a sigma one and
	// every threshold anyone has intuition about is wrong by a third.
	rng := rand.New(rand.NewSource(42))
	w := make([]float64, 5000)
	for i := range w {
		w[i] = 100 + rng.NormFloat64()*10
	}

	b := RobustBaseline(w)
	if math.Abs(b.Center-100) > 0.5 {
		t.Fatalf("center = %.3f, want ~100", b.Center)
	}
	if math.Abs(b.Spread-10) > 0.5 {
		t.Fatalf("spread = %.3f, want ~10 (sigma)", b.Spread)
	}
}

func TestOneOutlierMasksTheAnomalyWithStdDevButNotWithMAD(t *testing.T) {
	// The headline property, and the reason the detector does not use
	// standard deviation.
	//
	// Standard deviation has a breakdown point of zero: one extreme value
	// inflates it arbitrarily, so the spike raises the threshold and masks
	// what comes after it.
	//
	// The masking is about *scale*, and showing it needs a realistic
	// fixture. A constant series plus one outlier does not demonstrate it
	// (a second spike of the same size still breaches even an inflated
	// sigma, and a constant series has zero MAD anyway). What it actually
	// costs is the ability to see a normal-sized anomaly afterwards.
	rng := rand.New(rand.NewSource(11))
	w := make([]float64, 200)
	for i := range w {
		w[i] = 50 + rng.NormFloat64()*2
	}
	w[100] = 5000 // one earlier incident, sitting in the baseline window

	robust := RobustBaseline(w)
	naive := StdBaseline(w)

	// A genuine anomaly afterwards: 65 is over 7 real sigmas from a
	// series that normally sits at 50 +/- 2.
	const probe = 65.0
	robustZ := RobustZ(probe, robust)
	naiveZ := RobustZ(probe, naive)

	t.Logf("baseline polluted by one outlier: MAD spread=%.2f z=%.1f | stddev spread=%.1f z=%.3f",
		robust.Spread, robustZ, naive.Spread, naiveZ)

	if robust.Spread > 4 {
		t.Fatalf("MAD was corrupted by the outlier: spread=%.2f", robust.Spread)
	}
	if naive.Spread < 100 {
		t.Fatalf("expected stddev to be inflated, got %.2f", naive.Spread)
	}
	if robustZ < 5 {
		t.Fatalf("MAD failed to flag a real anomaly: z=%.2f", robustZ)
	}
	if naiveZ > 1 {
		t.Fatalf("expected stddev to mask the anomaly, but z=%.2f", naiveZ)
	}
}

func TestMADSurvivesHalfTheDataBeingGarbage(t *testing.T) {
	// The 50% breakdown point, tested at the edge.
	w := make([]float64, 100)
	for i := range w {
		if i < 49 {
			w[i] = 1e9 // garbage
		} else {
			w[i] = 50
		}
	}
	b := RobustBaseline(w)
	if math.Abs(b.Center-50) > 1e-9 {
		t.Fatalf("center = %v, want 50 with 49%% garbage", b.Center)
	}
}

func TestFlatSeriesProducesNoAnomalies(t *testing.T) {
	// The guard that matters more than it looks. A constant series has a
	// MAD of exactly zero; without the guard the next differing sample
	// divides by ~0, becomes infinitely anomalous, and the detector alerts
	// forever. A real and common cause of alert storms.
	w := make([]float64, 100)
	for i := range w {
		w[i] = 0 // a gauge sitting at zero
	}
	b := RobustBaseline(w)

	if b.Valid() {
		t.Fatal("a flat series produced a valid baseline")
	}
	for _, probe := range []float64{0, 1, -1, 1e9, math.MaxFloat64} {
		z := RobustZ(probe, b)
		if z != 0 {
			t.Fatalf("flat baseline scored %v as z=%v, want 0", probe, z)
		}
		if math.IsInf(z, 0) || math.IsNaN(z) {
			t.Fatalf("flat baseline produced non-finite z for %v", probe)
		}
	}
}

func TestNearlyFlatSeriesIsAlsoGuarded(t *testing.T) {
	// A counter that has moved by one ULP is flat for practical purposes,
	// and must not become infinitely sensitive.
	w := make([]float64, 100)
	for i := range w {
		w[i] = 1.0
	}
	w[0] = 1.0 + 1e-15
	b := RobustBaseline(w)
	if z := RobustZ(2.0, b); math.IsInf(z, 0) || math.Abs(z) > 1e12 {
		t.Fatalf("near-flat baseline gave z=%v", z)
	}
}

func TestBaselineIgnoresNaN(t *testing.T) {
	// A metric can legitimately produce NaN -- a rate over a zero
	// interval. Leaving it in makes the whole baseline NaN, every z-score
	// NaN, and every comparison false: a detector that silently stops
	// detecting.
	w := []float64{10, 11, math.NaN(), 9, 10, math.NaN(), 11}
	b := RobustBaseline(w)
	if math.IsNaN(b.Center) || math.IsNaN(b.Spread) {
		t.Fatalf("NaN leaked into the baseline: %+v", b)
	}
	if b.N != 5 {
		t.Fatalf("N = %d, want 5 non-NaN samples", b.N)
	}
}

func TestBaselineOfAllNaNIsInvalid(t *testing.T) {
	b := RobustBaseline([]float64{math.NaN(), math.NaN()})
	if b.Valid() {
		t.Fatal("all-NaN window produced a valid baseline")
	}
}

func TestBaselineDoesNotMutateCaller(t *testing.T) {
	// The caller's slice is usually a ring-buffer window it still needs in
	// order; sorting in place would silently scramble it.
	w := []float64{5, 1, 4, 2, 3}
	orig := append([]float64(nil), w...)
	RobustBaseline(w)
	for i := range w {
		if w[i] != orig[i] {
			t.Fatalf("baseline sorted the caller's slice: %v", w)
		}
	}
}

func TestEmptyWindow(t *testing.T) {
	if b := RobustBaseline(nil); b.Valid() {
		t.Fatal("empty window produced a valid baseline")
	}
	if z := RobustZ(5, Baseline{}); z != 0 {
		t.Fatalf("z on an empty baseline = %v", z)
	}
}

func TestMedianInterpolatesForEvenCounts(t *testing.T) {
	// A baseline that jumps when the window size changes parity is a
	// confusing thing to debug.
	if got := RobustBaseline([]float64{1, 2, 3, 4}).Center; got != 2.5 {
		t.Fatalf("even-count median = %v, want 2.5", got)
	}
	if got := RobustBaseline([]float64{1, 2, 3}).Center; got != 2 {
		t.Fatalf("odd-count median = %v, want 2", got)
	}
}

func TestRobustZSign(t *testing.T) {
	b := RobustBaseline([]float64{10, 10, 11, 9, 10, 11, 9, 10})
	if RobustZ(20, b) <= 0 {
		t.Fatal("a value above center scored non-positive")
	}
	if RobustZ(0, b) >= 0 {
		t.Fatal("a value below center scored non-negative")
	}
}

// --- the multiple-comparisons arithmetic -----------------------------------

func TestThreeSigmaFlagsPointTwoSevenPercent(t *testing.T) {
	// The definition, not a bug. This is where the 233,000 comes from.
	p := TwoTailedNormalP(3)
	if math.Abs(p-0.0027) > 0.0001 {
		t.Fatalf("P(|Z|>3) = %.5f, want ~0.0027", p)
	}
}

func TestTheQuarterMillionFalseAlertsPerDay(t *testing.T) {
	// §10.2's table, as executable arithmetic. This number justifies the
	// entire alerting design, so it should be re-derivable rather than
	// quoted.
	const series = 1000
	const evalsPerDay = 86400
	p := TwoTailedNormalP(3)

	got := ExpectedFalseAlertsPerDay(series, p, evalsPerDay, 1)
	t.Logf("1000 series, 3 sigma, 1 Hz, no hysteresis: %.0f false alerts/day", got)
	if got < 200_000 || got > 260_000 {
		t.Fatalf("expected ~233,000, got %.0f", got)
	}
}

func TestHysteresisCollapsesTheFalseAlertRate(t *testing.T) {
	// The table from §10.2: k=1 ~233,000, k=2 ~630, k=3 ~1.7.
	const series = 1000
	const evalsPerDay = 86400
	p := TwoTailedNormalP(3)

	k1 := ExpectedFalseAlertsPerDay(series, p, evalsPerDay, 1)
	k2 := ExpectedFalseAlertsPerDay(series, p, evalsPerDay, 2)
	k3 := ExpectedFalseAlertsPerDay(series, p, evalsPerDay, 3)

	t.Logf("false alerts/day: k=1 %.0f | k=2 %.1f | k=3 %.2f", k1, k2, k3)

	if k2 < 500 || k2 > 800 {
		t.Fatalf("k=2 gave %.1f, expected ~630", k2)
	}
	if k3 < 1 || k3 > 3 {
		t.Fatalf("k=3 gave %.2f, expected ~1.7", k3)
	}
}

// --- hysteresis ------------------------------------------------------------

func TestHysteresisRequiresConsecutiveBreaches(t *testing.T) {
	h := NewHysteresis(Config{ForCount: 3})

	for i := 1; i <= 2; i++ {
		r := h.Observe(1, true, int64(i)*1000)
		if r.Transition != TransitionNone {
			t.Fatalf("fired after %d breaches", i)
		}
		if r.State != StatePending {
			t.Fatalf("state after %d breaches = %v", i, r.State)
		}
	}

	r := h.Observe(1, true, 3000)
	if r.Transition != TransitionFired {
		t.Fatal("did not fire on the third consecutive breach")
	}
	if r.State != StateFiring {
		t.Fatalf("state = %v", r.State)
	}
}

func TestHysteresisResetsOnAnyGap(t *testing.T) {
	// The case it exists for: a false positive that dies quietly.
	h := NewHysteresis(Config{ForCount: 3})
	h.Observe(1, true, 1000)
	h.Observe(1, true, 2000)
	h.Observe(1, false, 3000) // one clear evaluation
	h.Observe(1, true, 4000)
	r := h.Observe(1, true, 5000)

	if r.Transition == TransitionFired {
		t.Fatal("fired despite the run being broken")
	}
}

func TestHysteresisPendingResolvesSilently(t *testing.T) {
	// Never fired, so there is nothing to resolve and nothing to notify.
	h := NewHysteresis(Config{ForCount: 3})
	h.Observe(1, true, 1000)
	r := h.Observe(1, false, 2000)

	if r.Transition != TransitionNone {
		t.Fatalf("a never-fired alert emitted %v", r.Transition)
	}
	if r.State != StateOK {
		t.Fatalf("state = %v", r.State)
	}
}

func TestHysteresisFiresOnlyOnce(t *testing.T) {
	// Dedup at the source: a condition that stays true must notify once,
	// not once per evaluation.
	h := NewHysteresis(Config{ForCount: 2})
	fired := 0
	for i := 1; i <= 50; i++ {
		if h.Observe(1, true, int64(i)*1000).Transition == TransitionFired {
			fired++
		}
	}
	if fired != 1 {
		t.Fatalf("fired %d times for one ongoing condition", fired)
	}
}

func TestHysteresisResolvesWithItsOwnHysteresis(t *testing.T) {
	// An alert that resolves the instant the condition clears flaps, and a
	// flapping alert trains people to ignore alerts.
	h := NewHysteresis(Config{ForCount: 2, ClearCount: 3})
	h.Observe(1, true, 1000)
	h.Observe(1, true, 2000) // fires

	for i := 1; i <= 2; i++ {
		r := h.Observe(1, false, int64(2000+i*1000))
		if r.Transition == TransitionResolved {
			t.Fatalf("resolved after only %d clear evaluations", i)
		}
		if r.State != StateResolving {
			t.Fatalf("state = %v", r.State)
		}
	}

	r := h.Observe(1, false, 6000)
	if r.Transition != TransitionResolved {
		t.Fatal("did not resolve after ClearCount clear evaluations")
	}
	if r.State != StateOK {
		t.Fatalf("state = %v", r.State)
	}
}

func TestHysteresisDoesNotRefireWhenResolutionIsInterrupted(t *testing.T) {
	// A condition that flickers while resolving is still the *same*
	// ongoing condition. Sending a second "fired" is exactly the flapping
	// this design prevents.
	h := NewHysteresis(Config{ForCount: 2, ClearCount: 3})
	h.Observe(1, true, 1000)
	h.Observe(1, true, 2000) // fires

	h.Observe(1, false, 3000) // resolving
	r := h.Observe(1, true, 4000)

	if r.Transition != TransitionNone {
		t.Fatalf("re-notified on an interrupted resolution: %v", r.Transition)
	}
	if r.State != StateFiring {
		t.Fatalf("state = %v, want firing", r.State)
	}
}

func TestHysteresisDefaultsClearToForCount(t *testing.T) {
	// Asymmetric hysteresis is a deliberate choice, so it must be asked
	// for rather than happening by accident.
	h := NewHysteresis(Config{ForCount: 3})
	for i := 1; i <= 3; i++ {
		h.Observe(1, true, int64(i)*1000)
	}
	for i := 1; i <= 2; i++ {
		if h.Observe(1, false, int64(3000+i*1000)).Transition == TransitionResolved {
			t.Fatalf("resolved after %d clear evaluations, want 3", i)
		}
	}
	if h.Observe(1, false, 7000).Transition != TransitionResolved {
		t.Fatal("did not resolve on the third clear evaluation")
	}
}

func TestHysteresisForCountOneFiresImmediately(t *testing.T) {
	h := NewHysteresis(Config{ForCount: 1})
	if h.Observe(1, true, 1000).Transition != TransitionFired {
		t.Fatal("ForCount=1 did not fire on the first breach")
	}
}

func TestHysteresisZeroConfigIsSane(t *testing.T) {
	// A zero-valued config must not mean "never fire" or divide by zero.
	h := NewHysteresis(Config{})
	if h.Observe(1, true, 1000).Transition != TransitionFired {
		t.Fatal("zero config never fires")
	}
}

func TestHysteresisReportsFirstBreachNotFireTime(t *testing.T) {
	// "High since 03:14" is far more useful in a notification than
	// "alerted at 03:17".
	h := NewHysteresis(Config{ForCount: 3})
	h.Observe(1, true, 1000)
	h.Observe(1, true, 2000)
	r := h.Observe(1, true, 3000)

	if r.FirstBreachMs != 1000 {
		t.Fatalf("FirstBreachMs = %d, want 1000", r.FirstBreachMs)
	}
}

func TestHysteresisTracksSeriesIndependently(t *testing.T) {
	h := NewHysteresis(Config{ForCount: 2})
	h.Observe(1, true, 1000)
	h.Observe(2, true, 1000)
	h.Observe(1, false, 2000)
	r2 := h.Observe(2, true, 2000)

	if r2.Transition != TransitionFired {
		t.Fatal("series 2 did not fire")
	}
	if h.State(1) != StateOK {
		t.Fatalf("series 1 state = %v", h.State(1))
	}
}

func TestHysteresisFiringList(t *testing.T) {
	h := NewHysteresis(Config{ForCount: 1})
	h.Observe(1, true, 1000)
	h.Observe(2, true, 1000)
	h.Observe(3, false, 1000)

	firing := h.Firing()
	if len(firing) != 2 {
		t.Fatalf("firing = %v", firing)
	}
}

func TestHysteresisForgetPreventsUnboundedGrowth(t *testing.T) {
	// A fleet with churn would otherwise accumulate one map entry per
	// series ever seen, in the one process that must stay up during an
	// incident.
	h := NewHysteresis(Config{ForCount: 1})
	for i := 1; i <= 1000; i++ {
		h.Observe(model.SeriesID(i), false, 1000)
	}
	if h.Len() != 1000 {
		t.Fatalf("len = %d", h.Len())
	}
	for i := 1; i <= 1000; i++ {
		h.Forget(model.SeriesID(i))
	}
	if h.Len() != 0 {
		t.Fatalf("len after forgetting = %d", h.Len())
	}
}

func TestStateAndTransitionStringsAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range []State{StateOK, StatePending, StateFiring, StateResolving} {
		if seen[s.String()] {
			t.Fatalf("duplicate state string %q", s.String())
		}
		seen[s.String()] = true
	}
}

// --- hysteresis against simulated noise ------------------------------------

func TestHysteresisSuppressesNoiseInSimulation(t *testing.T) {
	// The arithmetic says k=3 collapses the false-alert rate. This checks
	// it empirically on pure Gaussian noise, which is the "perfectly
	// normal, perfectly healthy series" the §10.2 table describes.
	rng := rand.New(rand.NewSource(7))
	const series = 200
	const evaluations = 5000

	count := func(k int) int {
		h := NewHysteresis(Config{ForCount: k})
		fired := 0
		for eval := 0; eval < evaluations; eval++ {
			for s := 1; s <= series; s++ {
				z := rng.NormFloat64()
				if h.Observe(model.SeriesID(s), math.Abs(z) > 3, int64(eval)).Transition == TransitionFired {
					fired++
				}
			}
		}
		return fired
	}

	k1 := count(1)
	k3 := count(3)
	t.Logf("simulated false alerts over %d evaluations x %d series: k=1 %d, k=3 %d", evaluations, series, k1, k3)

	if k1 < 1000 {
		t.Fatalf("expected many false alerts without hysteresis, got %d", k1)
	}
	if k3 > k1/100 {
		t.Fatalf("k=3 gave %d false alerts against %d for k=1; expected a ~100x reduction", k3, k1)
	}
}

// --- seasonality -----------------------------------------------------------

func TestSlotOfIsStableAcrossWeeks(t *testing.T) {
	const slot = 5 * 60 * 1000 // 5 minutes
	base := int64(1_700_000_000_000)
	if SlotOf(base, slot) != SlotOf(base+MsPerWeek, slot) {
		t.Fatal("the same wall-clock time in two weeks landed in different slots")
	}
	if SlotOf(base, slot) == SlotOf(base+MsPerDay, slot) {
		t.Fatal("the same time on two different days landed in the same slot")
	}
}

func TestSlotOfHandlesNegativeTimestamps(t *testing.T) {
	// Go's % keeps the sign of the dividend, so a pre-epoch timestamp
	// would produce a negative slot index and a map lookup that never
	// matches anything.
	if s := SlotOf(-1000, 60000); s < 0 {
		t.Fatalf("negative slot index %d", s)
	}
}

func TestSeasonalBaselineNeedsEnoughWeeks(t *testing.T) {
	// The degradation path. Every new deployment spends its first
	// fortnight here, and it must fall back rather than refuse to work.
	s := NewSeasonalStore(5*60*1000, 4)
	base := int64(1_700_000_000_000)

	s.Observe(base, 100)
	if _, ok := s.Baseline(base, 2); ok {
		t.Fatal("produced a baseline from one week of history")
	}

	s.Observe(base+MsPerWeek, 102)
	if _, ok := s.Baseline(base+2*MsPerWeek, 2); !ok {
		t.Fatal("refused a baseline with two weeks of history")
	}
}

func TestSeasonalBaselineUsesMedianAcrossWeeks(t *testing.T) {
	// One bad week -- an incident, a load test, a deploy that ran long --
	// must not move the baseline for the same hour every subsequent week.
	s := NewSeasonalStore(5*60*1000, 4)
	base := int64(1_700_000_000_000)

	for w, v := range []float64{100, 102, 5000, 101} { // week 2 was an incident
		s.Observe(base+int64(w)*MsPerWeek, v)
	}

	b, ok := s.Baseline(base+4*MsPerWeek, 2)
	if !ok {
		t.Fatal("no baseline")
	}
	if b.Center > 200 {
		t.Fatalf("one bad week moved the center to %.1f", b.Center)
	}
}

func TestSeasonalHistoryIsBounded(t *testing.T) {
	s := NewSeasonalStore(5*60*1000, 4)
	base := int64(1_700_000_000_000)
	for w := 0; w < 100; w++ {
		s.Observe(base+int64(w)*MsPerWeek, float64(w))
	}
	b, _ := s.Baseline(base, 2)
	if b.N > 4 {
		t.Fatalf("retained %d weeks, want at most 4", b.N)
	}
	// And the retained weeks are the most recent ones.
	if b.Center < 95 {
		t.Fatalf("center %.1f suggests old weeks were kept", b.Center)
	}
}

func TestSeasonalDistinguishesTimeOfDay(t *testing.T) {
	// The whole motivation: CPU at 3 a.m. is not CPU at 2 p.m. A rolling
	// baseline alerts every morning when traffic ramps.
	s := NewSeasonalStore(60*60*1000, 4)
	base := int64(1_700_000_000_000)

	for w := 0; w < 3; w++ {
		off := int64(w) * MsPerWeek
		s.Observe(base+off, 10)                // quiet hour
		s.Observe(base+off+12*60*60*1000, 900) // busy hour
	}

	quiet, _ := s.Baseline(base+3*MsPerWeek, 2)
	busy, _ := s.Baseline(base+3*MsPerWeek+12*60*60*1000, 2)

	if quiet.Center > 100 || busy.Center < 500 {
		t.Fatalf("seasonal slots did not separate: quiet=%.1f busy=%.1f", quiet.Center, busy.Center)
	}
}

func TestCombineTakesSeasonalCenterAndLargerSpread(t *testing.T) {
	// The seasonal center is the right expectation. Its spread, computed
	// from four weekly samples, is a poor scale estimate -- so take
	// whichever spread is larger. Being more sensitive than either
	// estimate alone justifies is the wrong direction to err when the cost
	// is someone's sleep.
	seasonal := Baseline{Center: 500, Spread: 2, N: 4}
	rolling := Baseline{Center: 100, Spread: 20, N: 600}

	c := Combine(seasonal, rolling)
	if c.Center != 500 {
		t.Fatalf("center = %v, want the seasonal one", c.Center)
	}
	if c.Spread != 20 {
		t.Fatalf("spread = %v, want the larger (20)", c.Spread)
	}
}

func TestCombineFallsBackWhenOneSideIsInvalid(t *testing.T) {
	rolling := Baseline{Center: 100, Spread: 20, N: 600}
	if got := Combine(Baseline{}, rolling); got != rolling {
		t.Fatalf("did not fall back to rolling: %+v", got)
	}
	seasonal := Baseline{Center: 500, Spread: 5, N: 4}
	if got := Combine(seasonal, Baseline{}); got != seasonal {
		t.Fatalf("did not fall back to seasonal: %+v", got)
	}
}

func TestQuantile(t *testing.T) {
	vals := []float64{5, 1, 4, 2, 3}
	if got := Quantile(vals, 0); got != 1 {
		t.Fatalf("q0 = %v", got)
	}
	if got := Quantile(vals, 1); got != 5 {
		t.Fatalf("q1 = %v", got)
	}
	if got := Quantile(vals, 0.5); got != 3 {
		t.Fatalf("q50 = %v", got)
	}
	if got := Quantile(nil, 0.5); got != 0 {
		t.Fatalf("q of empty = %v", got)
	}
}
