// Package latency instruments the end-to-end budget.
//
// The centrepiece of the project, and the thing to build before any
// optimisation. Without a measured budget you will spend a week shaving
// 5 ms off the decoder while the collection interval contributes seven
// seconds.
//
// The only measurement that means anything is origin-to-render: stamp the
// timestamp where the event was observed, compare it where the pixel is
// drawn. Everything else is a stage breakdown for finding which term
// dominates.
package latency

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Stage names the segments of the path from §3.1.
type Stage uint8

const (
	StageCollection Stage = iota // event occurs -> collector observes
	StageBatch                   // agent batches and flushes
	StageNetworkIn               // agent -> ingest
	StageIngest                  // parse, validate, route
	StageFanout                  // pub/sub to subscribers
	StageQueue                   // server send queue wait
	StageNetworkOut              // server -> browser
	StageDecode                  // client decode
	StageFrame                   // wait for the next animation frame
	numStages
)

func (s Stage) String() string {
	switch s {
	case StageCollection:
		return "collection"
	case StageBatch:
		return "batch"
	case StageNetworkIn:
		return "network_in"
	case StageIngest:
		return "ingest"
	case StageFanout:
		return "fanout"
	case StageQueue:
		return "queue"
	case StageNetworkOut:
		return "network_out"
	case StageDecode:
		return "decode"
	case StageFrame:
		return "frame"
	default:
		return "unknown"
	}
}

// Histogram records a latency distribution.
//
// Reservoir-free: it keeps every sample up to a cap, because the whole
// point is the *tail*, and a sampling scheme that loses the tail measures
// the wrong thing. The cap bounds memory; once reached, samples are
// replaced at random so the distribution stays representative rather than
// freezing at startup.
type Histogram struct {
	mu      sync.Mutex
	samples []float64
	cap     int
	count   uint64
	sum     float64
	min     float64
	max     float64
	// rng state, kept local so the histogram does not perturb global
	// randomness and stays deterministic in tests.
	seed uint64
}

func NewHistogram(capacity int) *Histogram {
	if capacity < 16 {
		capacity = 16
	}
	return &Histogram{
		samples: make([]float64, 0, capacity),
		cap:     capacity,
		min:     math.Inf(1),
		max:     math.Inf(-1),
		seed:    0x2545F4914F6CDD1D,
	}
}

// Observe records one measurement in milliseconds.
func (h *Histogram) Observe(ms float64) {
	if math.IsNaN(ms) {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++
	h.sum += ms
	if ms < h.min {
		h.min = ms
	}
	if ms > h.max {
		h.max = ms
	}

	if len(h.samples) < h.cap {
		h.samples = append(h.samples, ms)
		return
	}
	// Reservoir replacement, so a long-running process does not report the
	// first 100k samples forever.
	h.seed ^= h.seed << 13
	h.seed ^= h.seed >> 7
	h.seed ^= h.seed << 17
	idx := int(h.seed % uint64(h.cap))
	h.samples[idx] = ms
}

// Summary is a distribution, not a single number.
//
// "Report the full distribution" is the instruction, and the reason is that
// a mean hides exactly the problem: nine fast frames and one slow one
// average to something comfortable while the viewer sees a stutter.
type Summary struct {
	Count uint64
	Min   float64
	Max   float64
	Mean  float64
	P50   float64
	P95   float64
	P99   float64
}

func (s Summary) String() string {
	return fmt.Sprintf("n=%d p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms",
		s.Count, s.P50, s.P95, s.P99, s.Max)
}

// MeetsSubSecondSLO is the project's stated target: sub-second P99.
func (s Summary) MeetsSubSecondSLO() bool { return s.Count > 0 && s.P99 < 1000 }

func (h *Histogram) Summary() Summary {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.count == 0 {
		return Summary{}
	}
	s := append([]float64(nil), h.samples...)
	sort.Float64s(s)

	return Summary{
		Count: h.count,
		Min:   h.min,
		Max:   h.max,
		Mean:  h.sum / float64(h.count),
		P50:   percentile(s, 0.50),
		P95:   percentile(s, 0.95),
		P99:   percentile(s, 0.99),
	}
}

func (h *Histogram) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples = h.samples[:0]
	h.count, h.sum = 0, 0
	h.min, h.max = math.Inf(1), math.Inf(-1)
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	// Nearest-rank. Interpolating between samples invents a latency no
	// request experienced, which for a tail statistic is the wrong kind of
	// tidy.
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// Budget is a per-stage breakdown.
type Budget struct {
	stages [numStages]*Histogram
	total  *Histogram
}

func NewBudget(capacity int) *Budget {
	b := &Budget{total: NewHistogram(capacity)}
	for i := range b.stages {
		b.stages[i] = NewHistogram(capacity)
	}
	return b
}

func (b *Budget) Observe(s Stage, ms float64) {
	if int(s) < len(b.stages) {
		b.stages[s].Observe(ms)
	}
}

// ObserveEndToEnd records the only measurement that means anything.
func (b *Budget) ObserveEndToEnd(ms float64) { b.total.Observe(ms) }

func (b *Budget) Stage(s Stage) Summary {
	if int(s) >= len(b.stages) {
		return Summary{}
	}
	return b.stages[s].Summary()
}

func (b *Budget) Total() Summary { return b.total.Summary() }

// Dominant returns the stage with the largest P99 contribution.
//
// This is the output that changes behaviour. The design document's whole
// argument is that people optimise the wrong term; naming the dominant one
// is how the instrumentation earns its place.
func (b *Budget) Dominant() (Stage, Summary) {
	var best Stage
	var bestS Summary
	for i := range b.stages {
		s := b.stages[i].Summary()
		if s.Count > 0 && s.P99 > bestS.P99 {
			best, bestS = Stage(i), s
		}
	}
	return best, bestS
}

// Report renders the breakdown.
func (b *Budget) Report() string {
	var sb []byte
	sb = append(sb, "latency budget\n"...)
	for i := range b.stages {
		s := b.stages[i].Summary()
		if s.Count == 0 {
			continue
		}
		sb = append(sb, fmt.Sprintf("  %-12s %s\n", Stage(i), s)...)
	}
	total := b.total.Summary()
	sb = append(sb, fmt.Sprintf("  %-12s %s\n", "END-TO-END", total)...)
	if dom, ds := b.Dominant(); ds.Count > 0 {
		sb = append(sb, fmt.Sprintf("  dominant stage: %s (p99 %.1f ms)\n", dom, ds.P99)...)
	}
	return string(sb)
}

// --- the scenario arithmetic -----------------------------------------------

// Scenario is one row of the §3.2 table.
type Scenario struct {
	Name               string
	CollectionInterval time.Duration
	// OtherMs is everything except the collection interval.
	OtherMs float64
}

// P50 is the median end-to-end latency.
//
// Half the collection interval, because an event is equally likely to occur
// anywhere within it.
func (s Scenario) P50() float64 {
	return float64(s.CollectionInterval.Milliseconds())/2 + s.OtherMs
}

// P99 is the tail.
//
// Nearly a full collection interval: the worst case is an event occurring
// immediately after a collection, which then waits the whole interval.
func (s Scenario) P99() float64 {
	return float64(s.CollectionInterval.Milliseconds())*0.99 + s.OtherMs*2
}

func (s Scenario) MeetsSubSecond() bool { return s.P99() < 1000 }

// StandardScenarios is the table that argues for the whole architecture.
//
// Worth having as code: the conclusion -- that sub-second P99 requires
// collection at roughly 5 Hz or faster -- is quantitative, and anyone
// proposing a different collection interval should be able to re-derive it.
func StandardScenarios() []Scenario {
	return []Scenario{
		{"prometheus scrape @ 15s", 15 * time.Second, 100},
		{"push @ 1 Hz", time.Second, 100},
		{"push @ 5 Hz", 200 * time.Millisecond, 100},
	}
}

// RequiredCollectionInterval solves for the interval that meets a P99 target.
func RequiredCollectionInterval(targetP99Ms, otherMs float64) time.Duration {
	budget := targetP99Ms - otherMs*2
	if budget <= 0 {
		return 0
	}
	return time.Duration(budget/0.99) * time.Millisecond
}

// --- the cost of frequency -------------------------------------------------

// VolumeCost is the §3.3 arithmetic: what 5 Hz actually costs.
type VolumeCost struct {
	PointsPerSeriesPerDay int64
	PointsPerDay          int64
	RawBytesPerDay        int64
	CompressedBytesPerDay int64
}

// ComputeVolume for a collection interval and series count.
//
// bytesPerPoint is the uncompressed wire cost (~16 B in a TSDB row) and
// compressedBytesPerPoint the Gorilla figure (~1.4 B). Both are parameters
// rather than constants because they are measurable, and this function
// exists so the measurement can feed the capacity argument directly.
func ComputeVolume(interval time.Duration, series int, bytesPerPoint, compressedBytesPerPoint float64) VolumeCost {
	if interval <= 0 || series < 1 {
		return VolumeCost{}
	}
	perSeries := int64(24 * time.Hour / interval)
	total := perSeries * int64(series)
	return VolumeCost{
		PointsPerSeriesPerDay: perSeries,
		PointsPerDay:          total,
		RawBytesPerDay:        int64(float64(total) * bytesPerPoint),
		CompressedBytesPerDay: int64(float64(total) * compressedBytesPerPoint),
	}
}
