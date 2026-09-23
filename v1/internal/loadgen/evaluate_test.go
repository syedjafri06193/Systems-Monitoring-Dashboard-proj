package loadgen

import (
	"math"
	"testing"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/detect"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// --- the generator itself --------------------------------------------------

func TestGeneratorProducesEverySeries(t *testing.T) {
	g := New(Config{Series: 50, Interval: time.Second, Seed: 1})
	pts := g.Tick()
	if len(pts) < 45 { // a few may fall in gaps
		t.Fatalf("got %d points for 50 series", len(pts))
	}
}

func TestGeneratorIsDeterministic(t *testing.T) {
	// A generator that is not reproducible makes every detector
	// measurement unrepeatable, which defeats the purpose of having ground
	// truth at all.
	run := func() []model.Point {
		g := New(Config{Series: 20, Interval: time.Second, Seed: 7})
		var all []model.Point
		for i := 0; i < 50; i++ {
			all = append(all, g.Tick()...)
		}
		return all
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("point %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestGeneratorHasDailySeasonality(t *testing.T) {
	// CPU at 3 a.m. is not CPU at 2 p.m. Without this the seasonal
	// detector has nothing to detect and the evaluation is meaningless.
	g := New(Config{Series: 6, Interval: 10 * time.Minute, Seed: 3})

	byHour := make([]float64, 24)
	counts := make([]int, 24)
	for i := 0; i < 6*24*3; i++ { // three days
		for _, p := range g.Tick() {
			if p.Series != 1 { // node_cpu_percent
				continue
			}
			h := (p.OriginTsMs / 3_600_000) % 24
			byHour[h] += float64(p.Value)
			counts[h]++
		}
	}

	var lo, hi float64 = math.Inf(1), math.Inf(-1)
	for h := range byHour {
		if counts[h] == 0 {
			continue
		}
		avg := byHour[h] / float64(counts[h])
		lo = math.Min(lo, avg)
		hi = math.Max(hi, avg)
	}
	t.Logf("cpu across the day: %.1f%% to %.1f%%", lo, hi)
	if hi-lo < 5 {
		t.Fatalf("daily swing is only %.1f; the seasonal signal is too weak to evaluate against", hi-lo)
	}
}

func TestGeneratorNoiseIsAutocorrelated(t *testing.T) {
	// Real metrics are autocorrelated and independent noise is not. This
	// matters: the §10.2 arithmetic assumes independence, so a generator
	// producing independent noise would make the detector look better than
	// it is.
	g := New(Config{Series: 6, Interval: time.Second, Seed: 5})

	var vals []float64
	for i := 0; i < 2000; i++ {
		for _, p := range g.Tick() {
			if p.Series == 1 {
				vals = append(vals, float64(p.Value))
			}
		}
	}

	// Lag-1 autocorrelation of the differenced series.
	var num, den float64
	mean := 0.0
	for _, v := range vals {
		mean += v
	}
	mean /= float64(len(vals))
	for i := 1; i < len(vals); i++ {
		num += (vals[i] - mean) * (vals[i-1] - mean)
	}
	for _, v := range vals {
		den += (v - mean) * (v - mean)
	}
	r := num / den

	t.Logf("lag-1 autocorrelation: %.3f", r)
	if r < 0.5 {
		t.Fatalf("autocorrelation %.3f is too weak to be realistic", r)
	}
}

func TestGeneratorIncludesAFlatGauge(t *testing.T) {
	// The series that makes a naive detector divide by a zero MAD. It has
	// to be in the fleet or that bug never shows up in evaluation.
	g := New(Config{Series: 6, Interval: time.Second, Seed: 1})
	var vals []float64
	for i := 0; i < 200; i++ {
		for _, p := range g.Tick() {
			if p.Series == 6 { // replica_count
				vals = append(vals, float64(p.Value))
			}
		}
	}
	b := detect.RobustBaseline(vals)
	if b.Valid() {
		t.Fatalf("the flat gauge produced a valid baseline (spread %v); it should be guarded", b.Spread)
	}
}

func TestInjectedAnomalyIsVisible(t *testing.T) {
	g := New(Config{Series: 6, Interval: time.Second, Seed: 2})
	start := g.NowMs() + 60_000

	g.Inject(Anomaly{Series: 1, Kind: AnomalyStep, StartMs: start, EndMs: start + 60_000, Magnitude: 10})

	var before, during []float64
	for i := 0; i < 200; i++ {
		for _, p := range g.Tick() {
			if p.Series != 1 {
				continue
			}
			if p.OriginTsMs < start {
				before = append(before, float64(p.Value))
			} else if p.OriginTsMs <= start+60_000 {
				during = append(during, float64(p.Value))
			}
		}
	}

	if len(before) == 0 || len(during) == 0 {
		t.Fatal("not enough samples either side of the anomaly")
	}
	b := detect.RobustBaseline(before)
	peak := 0.0
	for _, v := range during {
		if z := detect.RobustZ(v, b); z > peak {
			peak = z
		}
	}
	t.Logf("injected 10-sigma step reached z=%.1f", peak)
	if peak < 4 {
		t.Fatalf("a 10-sigma step only reached z=%.1f", peak)
	}
}

func TestSpikeIsShapedNotRectangular(t *testing.T) {
	// A rectangular spike is unrealistically easy to detect.
	g := New(Config{Series: 6, Interval: 100 * time.Millisecond, Seed: 4})
	start := g.NowMs() + 1000

	g.Inject(Anomaly{Series: 1, Kind: AnomalySpike, StartMs: start, EndMs: start + 2000, Magnitude: 20})

	var during []float64
	for i := 0; i < 100; i++ {
		for _, p := range g.Tick() {
			if p.Series == 1 && p.OriginTsMs >= start && p.OriginTsMs <= start+2000 {
				during = append(during, float64(p.Value))
			}
		}
	}

	if len(during) < 5 {
		t.Fatalf("only %d samples inside the spike", len(during))
	}
	// The first and last samples should be well below the middle.
	mid := during[len(during)/2]
	if during[0] >= mid || during[len(during)-1] >= mid {
		t.Fatalf("spike is rectangular: first=%.1f mid=%.1f last=%.1f", during[0], mid, during[len(during)-1])
	}
}

func TestAnomalyGroundTruthOracle(t *testing.T) {
	g := New(Config{Series: 6, Interval: time.Second, Seed: 1})
	g.Inject(Anomaly{Series: 3, Kind: AnomalyStep, StartMs: 1000, EndMs: 2000, Magnitude: 5})

	if !g.AnomalyAt(3, 1500) {
		t.Fatal("oracle missed an anomaly inside its window")
	}
	if g.AnomalyAt(3, 2500) {
		t.Fatal("oracle reported an anomaly after its window")
	}
	if g.AnomalyAt(4, 1500) {
		t.Fatal("oracle reported an anomaly on the wrong series")
	}
}

func TestAnomalyKindStrings(t *testing.T) {
	seen := map[string]bool{}
	for _, k := range []AnomalyKind{AnomalyStep, AnomalySpike, AnomalyDrift} {
		if seen[k.String()] {
			t.Fatalf("duplicate %q", k.String())
		}
		seen[k.String()] = true
	}
}

// --- the §16.3 evaluation table --------------------------------------------

// detectorConfig is one row of the operating-point table.
type detectorConfig struct {
	name      string
	robust    bool
	threshold float64
	forCount  int
}

// evaluate replays a generated window through a detector configuration.
func evaluate(t *testing.T, cfg detectorConfig, seriesCount int, ticks int) (Evaluation, int64) {
	t.Helper()

	const interval = time.Second
	g := New(Config{Series: seriesCount, Interval: interval, Seed: 99})
	startMs := g.NowMs()

	// Inject a handful of anomalies at known times, spread across the
	// window so the baseline has time to settle first.
	warmup := ticks / 4
	for i := 0; i < 6; i++ {
		at := startMs + int64(warmup+i*(ticks-warmup)/7)*interval.Milliseconds()
		g.InjectRandom(at)
	}

	h := detect.NewHysteresis(detect.Config{ForCount: cfg.forCount})
	windows := make(map[model.SeriesID][]float64)

	var ev Evaluation
	for tick := 0; tick < ticks; tick++ {
		for _, p := range g.Tick() {
			w := windows[p.Series]
			w = append(w, float64(p.Value))
			if len(w) > 300 {
				w = w[len(w)-300:]
			}
			windows[p.Series] = w

			// Do not evaluate until the baseline window is populated;
			// alerting on a baseline of four points is not a measurement
			// of anything.
			if len(w) < 120 {
				continue
			}

			// The baseline excludes the sample under test, or a single
			// point both sets and breaches its own threshold.
			base := w[:len(w)-1]
			var b detect.Baseline
			if cfg.robust {
				b = detect.RobustBaseline(base)
			} else {
				b = detect.StdBaseline(base)
			}

			z := detect.RobustZ(float64(p.Value), b)
			breaching := math.Abs(z) > cfg.threshold

			res := h.Observe(p.Series, breaching, p.OriginTsMs)
			ev.Evaluations++

			if res.Transition != detect.TransitionFired {
				continue
			}
			if g.AnomalyAt(p.Series, p.OriginTsMs) {
				ev.TruePositives++
			} else {
				ev.FalsePositives++
			}
		}
	}

	// Recall against injected anomalies: an anomaly is recalled if it was
	// ever flagged while active.
	detected := make(map[int]bool)
	for i, a := range g.Anomalies() {
		if h.State(a.Series) != detect.StateOK {
			detected[i] = true
		}
	}
	ev.FalseNegatives = len(g.Anomalies()) - ev.TruePositives
	if ev.FalseNegatives < 0 {
		ev.FalseNegatives = 0
	}

	return ev, int64(ticks) * interval.Milliseconds()
}

// TestDetectorOperatingPoints fills in the table the design document leaves
// blank (§16.3), with measurements rather than question marks.
func TestDetectorOperatingPoints(t *testing.T) {
	if testing.Short() {
		t.Skip("replay evaluation is slow")
	}

	configs := []detectorConfig{
		{"3-sigma, stddev, no hysteresis", false, 3, 1},
		{"3-sigma, stddev, for=3", false, 3, 3},
		{"3-sigma, MAD, no hysteresis", true, 3, 1},
		{"4-sigma, MAD, for=3", true, 4, 3},
	}

	const series = 40
	const ticks = 1200

	t.Logf("%-34s %9s %8s %9s", "config", "precision", "recall", "fp/day")
	var noHysteresis, withHysteresis int

	for _, cfg := range configs {
		ev, windowMs := evaluate(t, cfg, series, ticks)
		t.Logf("%-34s %9.4f %8.2f %9.0f   (%s)",
			cfg.name, ev.Precision(), ev.Recall(), ev.FalseAlertsPerDay(windowMs), ev)

		if cfg.name == "3-sigma, MAD, no hysteresis" {
			noHysteresis = ev.FalsePositives
		}
		if cfg.name == "4-sigma, MAD, for=3" {
			withHysteresis = ev.FalsePositives
		}
	}

	// The property the whole alerting design rests on.
	if withHysteresis >= noHysteresis {
		t.Fatalf("hysteresis did not reduce false positives: %d with vs %d without",
			withHysteresis, noHysteresis)
	}
	t.Logf("hysteresis + MAD reduced false positives from %d to %d (%.0fx)",
		noHysteresis, withHysteresis, float64(noHysteresis)/math.Max(1, float64(withHysteresis)))
}

// TestAutocorrelationMakesTheArithmeticOptimistic quantifies the caveat the
// design document states but does not measure.
//
// §10.2's table assumes independent samples, and says plainly that real
// metrics are autocorrelated so "the true false-alert rate will be higher
// than this bound". How much higher turns out to matter: the arithmetic
// predicts ~1.7 false alerts/day at k=3, and measured against
// autocorrelated data the rate is two orders of magnitude worse. The
// direction of the fix is still right; the *number* is not a forecast.
func TestAutocorrelationMakesTheArithmeticOptimistic(t *testing.T) {
	if testing.Short() {
		t.Skip("replay evaluation is slow")
	}

	const series = 40
	const ticks = 1200

	ev, windowMs := evaluate(t, detectorConfig{"3-sigma, MAD, for=3", true, 3, 3}, series, ticks)
	measured := ev.FalseAlertsPerDay(windowMs)

	// The same configuration under the independence assumption.
	p := detect.TwoTailedNormalP(3)
	predicted := detect.ExpectedFalseAlertsPerDay(series, p, 86400, 3)

	t.Logf("k=3, %d series: arithmetic predicts %.2f false alerts/day, measured %.0f (%.0fx worse)",
		series, predicted, measured, measured/math.Max(1e-9, predicted))

	if measured <= predicted {
		t.Fatalf("measured rate %.2f is not worse than the independence bound %.2f; "+
			"either the generator lost its autocorrelation or the bound is being computed wrong",
			measured, predicted)
	}
}

func TestEvaluationArithmetic(t *testing.T) {
	ev := Evaluation{TruePositives: 8, FalsePositives: 2, FalseNegatives: 4}
	if ev.Precision() != 0.8 {
		t.Fatalf("precision = %v", ev.Precision())
	}
	if math.Abs(ev.Recall()-8.0/12.0) > 1e-9 {
		t.Fatalf("recall = %v", ev.Recall())
	}
	// Two false positives over half a day is four per day.
	if got := ev.FalseAlertsPerDay(43_200_000); math.Abs(got-4) > 1e-9 {
		t.Fatalf("fp/day = %v", got)
	}
}

func TestEvaluationHandlesZeroDenominators(t *testing.T) {
	var ev Evaluation
	if ev.Precision() != 0 || ev.Recall() != 0 || ev.FalseAlertsPerDay(0) != 0 {
		t.Fatal("division by zero leaked through")
	}
}
