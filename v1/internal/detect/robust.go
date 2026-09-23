// Package detect turns a stream of values into candidate anomalies.
//
// Detecting that a number moved is easy. Not drowning people in alerts is
// the entire engineering problem, and the failure mode is quantifiable:
// 1000 series at a 3-sigma threshold evaluated at 1 Hz produces roughly
// 233,000 false alerts per day from a perfectly healthy fleet. That is not
// a bug in the threshold, it is the definition of 3 sigma.
//
// Two mechanisms do almost all the work: robust statistics (this file) and
// hysteresis (hysteresis.go).
package detect

import (
	"math"
	"sort"
)

// MADScale rescales the median absolute deviation to be a consistent
// estimator of sigma for normally distributed data.
//
// 1/Phi^-1(3/4) = 1.4826. Without it a MAD-based z-score is on a different
// scale from a sigma-based one, and every threshold anyone has intuition
// about would be wrong by a third.
const MADScale = 1.4826

// MinSpread guards against a degenerate baseline.
//
// A series that has been constant -- a gauge sitting at zero, a counter
// that has not moved, a metric reporting a hard-coded value -- has a MAD of
// exactly zero. Without this guard the next sample that differs at all
// divides by ~0 and becomes infinitely anomalous, and the detector emits an
// alert for every subsequent evaluation forever. This is a real and common
// cause of alert storms in homegrown detectors, and it costs one comparison
// to prevent.
const MinSpread = 1e-9

// Baseline is a robust estimate of where a series normally sits.
type Baseline struct {
	Center float64
	Spread float64
	// N is the sample count the estimate was built from. A baseline from
	// three points is not worth alerting on, and the caller needs to be
	// able to tell.
	N int
}

// Valid reports whether the baseline is usable for scoring.
func (b Baseline) Valid() bool { return b.N > 0 && b.Spread >= MinSpread }

// RobustBaseline estimates center and spread in a way a spike cannot corrupt.
//
// Standard deviation has a **breakdown point of zero**: a single extreme
// outlier inflates it arbitrarily. That is precisely backwards for anomaly
// detection -- the spike you want to catch raises your threshold and masks
// itself. A series sitting at 50 with one sample at 5000 gets a sigma large
// enough that nothing will ever look anomalous again.
//
// The median absolute deviation has a 50% breakdown point: half the data
// can be garbage and the estimate holds.
func RobustBaseline(window []float64) Baseline {
	if len(window) == 0 {
		return Baseline{}
	}

	// Copy before sorting: the caller's slice is often a ring-buffer
	// window it still needs in order.
	s := make([]float64, 0, len(window))
	for _, v := range window {
		// NaN poisons both sort order and the median. A metric can
		// legitimately produce NaN (a rate over a zero interval), and
		// leaving it in makes the whole baseline NaN, which then makes
		// every z-score NaN and every comparison false -- a detector that
		// silently stops detecting.
		if math.IsNaN(v) {
			continue
		}
		s = append(s, v)
	}
	if len(s) == 0 {
		return Baseline{}
	}
	sort.Float64s(s)
	center := medianSorted(s)

	dev := make([]float64, len(s))
	for i, v := range s {
		dev[i] = math.Abs(v - center)
	}
	sort.Float64s(dev)

	return Baseline{
		Center: center,
		Spread: MADScale * medianSorted(dev),
		N:      len(s),
	}
}

// StdBaseline is the naive alternative, kept for comparison.
//
// Exported because the README and the detector evaluation table both
// compare against it, and because having both side by side is the clearest
// way to show what the breakdown point means.
func StdBaseline(window []float64) Baseline {
	if len(window) == 0 {
		return Baseline{}
	}
	var sum float64
	n := 0
	for _, v := range window {
		if math.IsNaN(v) {
			continue
		}
		sum += v
		n++
	}
	if n == 0 {
		return Baseline{}
	}
	mean := sum / float64(n)

	var ss float64
	for _, v := range window {
		if math.IsNaN(v) {
			continue
		}
		d := v - mean
		ss += d * d
	}
	return Baseline{Center: mean, Spread: math.Sqrt(ss / float64(n)), N: n}
}

// RobustZ scores a value against a baseline.
//
// Returns 0 for a degenerate baseline rather than +Inf. Zero means "no
// evidence of an anomaly", which is the correct reading of a flat series:
// there is no spread to be unusual relative to.
func RobustZ(v float64, b Baseline) float64 {
	if !b.Valid() {
		return 0
	}
	if math.IsNaN(v) {
		return 0
	}
	return (v - b.Center) / b.Spread
}

func medianSorted(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	// Interpolating for even counts rather than taking the lower element.
	// With small windows the difference moves the threshold noticeably,
	// and a baseline that jumps when the window size changes parity is a
	// confusing thing to debug.
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// Quantile returns the q-quantile of an unsorted slice, by nearest rank.
func Quantile(vals []float64, q float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	if q <= 0 {
		return s[0]
	}
	if q >= 1 {
		return s[len(s)-1]
	}
	i := int(q * float64(len(s)))
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

// ExpectedFalseAlertsPerDay is the arithmetic from §10.2.
//
// Worth having as code rather than as a comment, because it is the number
// that justifies the entire alerting design and it should be re-derivable
// for any configuration someone proposes.
//
// The independence assumption is stated in the doc and is wrong in the
// helpful direction to be honest about: real metrics are autocorrelated, so
// consecutive breaches are not independent and the true rate is *higher*
// than this bound. It is a floor, not a forecast.
func ExpectedFalseAlertsPerDay(series int, pFlag float64, evaluationsPerDay int, consecutive int) float64 {
	if consecutive < 1 {
		consecutive = 1
	}
	pRun := math.Pow(pFlag, float64(consecutive))
	return float64(series) * pRun * float64(evaluationsPerDay)
}

// TwoTailedNormalP is P(|Z| > z) for a standard normal.
//
// Used to derive the flag probability for a given sigma threshold: 3 sigma
// gives 0.0027, which is where the 233,000 comes from.
func TwoTailedNormalP(z float64) float64 {
	return math.Erfc(math.Abs(z) / math.Sqrt2)
}
