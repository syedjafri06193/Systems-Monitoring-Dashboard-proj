// Package loadgen produces synthetic metrics with injectable anomalies.
//
// A first-class component, not a test fixture. You cannot develop or
// evaluate any of this against three hand-typed metrics: the browser limits
// only appear at a thousand series, the conflation policy only matters
// under pressure, and the detector cannot be tuned without ground truth.
//
// The ground truth is the part that makes §16.3 possible. Without knowing
// where the anomalies actually are, evaluating a detector is eyeballing
// charts and guessing.
package loadgen

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// AnomalyKind is one of the three shapes worth injecting.
type AnomalyKind uint8

const (
	// AnomalyStep is a level shift that persists: a config change, a
	// traffic rebalance, a leak beginning.
	AnomalyStep AnomalyKind = iota
	// AnomalySpike is a brief excursion: a GC pause, a burst of work.
	AnomalySpike
	// AnomalyDrift is a gradual ramp, which is the hardest of the three to
	// detect and the one a rolling baseline silently absorbs.
	AnomalyDrift
)

func (k AnomalyKind) String() string {
	switch k {
	case AnomalyStep:
		return "step"
	case AnomalySpike:
		return "spike"
	case AnomalyDrift:
		return "drift"
	default:
		return "unknown"
	}
}

// Anomaly is an injected event with known ground truth.
type Anomaly struct {
	Series    model.SeriesID
	Kind      AnomalyKind
	StartMs   int64
	EndMs     int64
	Magnitude float64 // multiples of the series' own noise sigma
}

// Contains reports whether a timestamp falls inside the anomaly.
func (a Anomaly) Contains(tsMs int64) bool {
	return tsMs >= a.StartMs && tsMs <= a.EndMs
}

// SeriesSpec describes one synthetic series.
type SeriesSpec struct {
	ID     model.SeriesID
	Labels model.Labels
	// Base is the value the series sits at with no seasonality.
	Base float64
	// DailyAmplitude is the peak-to-trough swing of the daily cycle,
	// relative to Base. Real infrastructure metrics have one and a
	// detector that ignores it alerts every morning.
	DailyAmplitude float64
	// WeeklyAmplitude captures the weekday/weekend difference.
	WeeklyAmplitude float64
	// NoiseSigma is the standard deviation of the per-sample noise.
	NoiseSigma float64
	// Autocorrelation makes consecutive samples related, which real
	// metrics are and independent noise is not. This matters for detector
	// evaluation: the §10.2 arithmetic assumes independence, and
	// autocorrelated data produces a *higher* false-alert rate than the
	// arithmetic predicts. A generator producing independent noise would
	// make the detector look better than it is.
	Autocorrelation float64
	// GapProbability is the chance a sample is missing entirely.
	GapProbability float64
}

// Config controls generation.
type Config struct {
	Series   int
	Interval time.Duration
	Seed     int64
	// StartMs anchors the seasonal phase.
	StartMs int64
	// AnomalyRate is the expected number of anomalies per series per day.
	AnomalyRate float64
}

func DefaultConfig() Config {
	return Config{
		Series:      1000,
		Interval:    200 * time.Millisecond,
		Seed:        1,
		StartMs:     time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
		AnomalyRate: 2,
	}
}

// Generator produces points and remembers where the anomalies are.
type Generator struct {
	cfg     Config
	rng     *rand.Rand
	specs   []SeriesSpec
	anoms   []Anomaly
	lastVal []float64
	nowMs   int64
}

// New builds a generator with a realistic fleet of series.
func New(cfg Config) *Generator {
	if cfg.Series < 1 {
		cfg.Series = 1
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 200 * time.Millisecond
	}
	rng := rand.New(rand.NewSource(cfg.Seed))

	g := &Generator{
		cfg:     cfg,
		rng:     rng,
		specs:   make([]SeriesSpec, 0, cfg.Series),
		lastVal: make([]float64, cfg.Series+1),
		nowMs:   cfg.StartMs,
	}

	// A realistic mix rather than a thousand copies of one shape. The
	// detector's false-positive rate depends heavily on the mix: flat
	// gauges and spiky counters fail in different ways.
	kinds := []struct {
		name            string
		base            float64
		daily           float64
		weekly          float64
		sigma           float64
		autocorrelation float64
	}{
		{"node_cpu_percent", 35, 25, 8, 3.0, 0.85},
		{"node_memory_percent", 60, 5, 2, 1.0, 0.95},
		{"http_requests_per_second", 800, 600, 200, 40, 0.7},
		{"http_request_duration_p99_ms", 120, 40, 10, 15, 0.6},
		{"disk_io_ops", 400, 300, 100, 50, 0.5},
		// A deliberately flat gauge. This is the series that makes a naive
		// detector divide by a zero MAD.
		{"replica_count", 3, 0, 0, 0, 0.99},
	}

	for i := 0; i < cfg.Series; i++ {
		k := kinds[i%len(kinds)]
		g.specs = append(g.specs, SeriesSpec{
			ID: model.SeriesID(i + 1),
			Labels: model.Labels{
				Name: k.name,
				Pairs: map[string]string{
					"instance": fmt.Sprintf("node-%03d", i/len(kinds)),
					"dc":       []string{"eu-west", "us-east", "ap-south"}[i%3],
					"rack":     fmt.Sprintf("r%d", (i/len(kinds))%8),
				},
			},
			Base:            k.base,
			DailyAmplitude:  k.daily,
			WeeklyAmplitude: k.weekly,
			NoiseSigma:      k.sigma,
			Autocorrelation: k.autocorrelation,
			GapProbability:  0.001,
		})
		g.lastVal[i+1] = k.base
	}

	return g
}

// Specs exposes the series definitions, for registering them.
func (g *Generator) Specs() []SeriesSpec { return g.specs }

// Anomalies returns the ground truth injected so far.
func (g *Generator) Anomalies() []Anomaly { return g.anoms }

// Inject schedules an anomaly with known ground truth.
func (g *Generator) Inject(a Anomaly) { g.anoms = append(g.anoms, a) }

// InjectRandom schedules one anomaly of a random kind on a random series.
func (g *Generator) InjectRandom(startMs int64) Anomaly {
	spec := g.specs[g.rng.Intn(len(g.specs))]
	kind := AnomalyKind(g.rng.Intn(3))

	var dur int64
	switch kind {
	case AnomalySpike:
		dur = int64(1000 + g.rng.Intn(4000)) // 1-5 s
	case AnomalyStep:
		dur = int64(60_000 + g.rng.Intn(600_000)) // 1-11 min
	default:
		dur = int64(300_000 + g.rng.Intn(1_800_000)) // 5-35 min
	}

	a := Anomaly{
		Series:    spec.ID,
		Kind:      kind,
		StartMs:   startMs,
		EndMs:     startMs + dur,
		Magnitude: 4 + g.rng.Float64()*8, // 4-12 sigma
	}
	g.Inject(a)
	return a
}

// Tick produces one interval's worth of points and advances the clock.
//
// Returns points for every series, minus any that fell in a gap.
func (g *Generator) Tick() []model.Point {
	ts := g.nowMs
	g.nowMs += g.cfg.Interval.Milliseconds()

	out := make([]model.Point, 0, len(g.specs))
	for i := range g.specs {
		spec := &g.specs[i]
		if spec.GapProbability > 0 && g.rng.Float64() < spec.GapProbability {
			continue
		}
		v := g.value(spec, ts)
		out = append(out, model.Point{
			Series:     spec.ID,
			OriginTsMs: ts,
			IngestTsMs: ts,
			Value:      float32(v),
		})
	}
	return out
}

// NowMs is the generator's current timestamp.
func (g *Generator) NowMs() int64 { return g.nowMs }

// value computes one sample: seasonality + autocorrelated noise + anomaly.
func (g *Generator) value(spec *SeriesSpec, tsMs int64) float64 {
	base := spec.Base + g.seasonal(spec, tsMs)

	// Autocorrelated noise: an AR(1) process. Real metrics are not
	// independent from sample to sample, and a generator producing
	// independent noise makes the detector look better than it is.
	prev := g.lastVal[spec.ID]
	innovation := g.rng.NormFloat64() * spec.NoiseSigma * math.Sqrt(1-spec.Autocorrelation*spec.Autocorrelation)
	v := base + spec.Autocorrelation*(prev-base) + innovation
	g.lastVal[spec.ID] = v

	// Apply any active anomaly on top.
	for _, a := range g.anoms {
		if a.Series != spec.ID || !a.Contains(tsMs) {
			continue
		}
		// Magnitude is in multiples of the series' own sigma, so an
		// anomaly means the same thing on a metric that sits at 3 and one
		// that sits at 800.
		amp := a.Magnitude * spec.NoiseSigma
		if spec.NoiseSigma == 0 {
			// A flat gauge has no sigma to scale against; use a unit step
			// so an anomaly on it is still expressible.
			amp = a.Magnitude
		}
		switch a.Kind {
		case AnomalyStep:
			v += amp
		case AnomalySpike:
			// Shaped rather than rectangular: a real spike has an attack
			// and a decay, and a rectangular one is unrealistically easy
			// to detect.
			frac := float64(tsMs-a.StartMs) / float64(max64(1, a.EndMs-a.StartMs))
			v += amp * math.Sin(frac*math.Pi)
		case AnomalyDrift:
			frac := float64(tsMs-a.StartMs) / float64(max64(1, a.EndMs-a.StartMs))
			v += amp * frac
		}
	}

	return v
}

// seasonal is the daily and weekly cycle.
func (g *Generator) seasonal(spec *SeriesSpec, tsMs int64) float64 {
	day := float64((tsMs%86_400_000)+86_400_000) / 86_400_000
	week := float64((tsMs%604_800_000)+604_800_000) / 604_800_000

	// Daily: peaks mid-afternoon, troughs pre-dawn.
	daily := spec.DailyAmplitude * 0.5 * math.Sin((day-0.25)*2*math.Pi)
	// Weekly: weekdays busier than weekends.
	weekly := spec.WeeklyAmplitude * 0.5 * math.Sin(week*2*math.Pi)
	return daily + weekly
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// --- detector evaluation ---------------------------------------------------

// Evaluation is the §16.3 precision/recall measurement.
type Evaluation struct {
	TruePositives  int
	FalsePositives int
	FalseNegatives int
	Evaluations    int
}

func (e Evaluation) Precision() float64 {
	d := e.TruePositives + e.FalsePositives
	if d == 0 {
		return 0
	}
	return float64(e.TruePositives) / float64(d)
}

func (e Evaluation) Recall() float64 {
	d := e.TruePositives + e.FalseNegatives
	if d == 0 {
		return 0
	}
	return float64(e.TruePositives) / float64(d)
}

// FalseAlertsPerDay extrapolates from the evaluated window.
func (e Evaluation) FalseAlertsPerDay(windowMs int64) float64 {
	if windowMs <= 0 {
		return 0
	}
	days := float64(windowMs) / 86_400_000
	if days == 0 {
		return 0
	}
	return float64(e.FalsePositives) / days
}

func (e Evaluation) String() string {
	return fmt.Sprintf("precision=%.3f recall=%.3f tp=%d fp=%d fn=%d",
		e.Precision(), e.Recall(), e.TruePositives, e.FalsePositives, e.FalseNegatives)
}

// AnomalyAt reports whether any injected anomaly covers a series at a time.
//
// The ground-truth oracle. Everything in §16.3 is measured against this.
func (g *Generator) AnomalyAt(id model.SeriesID, tsMs int64) bool {
	for _, a := range g.anoms {
		if a.Series == id && a.Contains(tsMs) {
			return true
		}
	}
	return false
}
