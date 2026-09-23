// Package cardinality enforces series limits at ingest.
//
// Series count is the *product* of label cardinalities. One label carrying
// a user id, a request id, or a raw URL path makes it unbounded, and an
// unbounded series count kills every TSDB there is. This is the single most
// common way Prometheus deployments fall over.
//
// Enforcement belongs at ingest, where refusing is still possible. Once the
// series exists in the database, the damage is done and the only remedies
// are expensive.
package cardinality

import (
	"fmt"
	"sync"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// Limits bound what a single metric may produce.
type Limits struct {
	// PerMetric is the maximum distinct series for one metric name.
	PerMetric int
	// PerLabel is the maximum distinct values for one label on one metric.
	// This is the limit that actually catches the dangerous case: a
	// request-id label blows this long before the series total, because
	// every request is a new value.
	PerLabel int
	// MaxLabelValueLen rejects values that are obviously not labels --
	// a stack trace, a URL with a query string, a serialised payload.
	MaxLabelValueLen int
	// MaxLabelsPerSeries bounds the label count itself.
	MaxLabelsPerSeries int
}

// DefaultLimits are deliberately generous but finite.
//
// Finite is the point. A limit set to "unlimited because we don't know yet"
// is the same as no limit, and the failure it permits takes down the
// database rather than one metric.
func DefaultLimits() Limits {
	return Limits{
		PerMetric:          10_000,
		PerLabel:           1_000,
		MaxLabelValueLen:   256,
		MaxLabelsPerSeries: 32,
	}
}

// RejectReason says which rule fired, so the metric naming it is actionable.
type RejectReason string

const (
	ReasonMetricCardinality RejectReason = "metric_cardinality"
	ReasonLabelCardinality  RejectReason = "label_cardinality"
	ReasonLabelValueTooLong RejectReason = "label_value_too_long"
	ReasonTooManyLabels     RejectReason = "too_many_labels"
)

// Error carries the reason and the offending label.
//
// Naming the label is the whole value of rejecting loudly. "Cardinality
// exceeded" sends someone on an hour's hunt; "cardinality exceeded on label
// request_id" is fixed in a minute by the person who added it.
type Error struct {
	Reason RejectReason
	Metric string
	Label  string
	Detail string
}

func (e *Error) Error() string {
	if e.Label != "" {
		return fmt.Sprintf("cardinality: %s rejected: %s on label %q (%s)", e.Metric, e.Reason, e.Label, e.Detail)
	}
	return fmt.Sprintf("cardinality: %s rejected: %s (%s)", e.Metric, e.Reason, e.Detail)
}

// metricState tracks one metric's observed cardinality.
type metricState struct {
	series      map[string]struct{}
	labelValues map[string]map[string]struct{}
}

// Limiter enforces Limits across metrics.
type Limiter struct {
	mu      sync.Mutex
	limits  Limits
	metrics map[string]*metricState

	// rejected counts by (metric, reason) so the dashboard can show which
	// metric is misbehaving without anyone grepping logs.
	rejected map[string]uint64
	accepted uint64
}

func New(limits Limits) *Limiter {
	if limits.PerMetric < 1 {
		limits.PerMetric = DefaultLimits().PerMetric
	}
	if limits.PerLabel < 1 {
		limits.PerLabel = DefaultLimits().PerLabel
	}
	if limits.MaxLabelValueLen < 1 {
		limits.MaxLabelValueLen = DefaultLimits().MaxLabelValueLen
	}
	if limits.MaxLabelsPerSeries < 1 {
		limits.MaxLabelsPerSeries = DefaultLimits().MaxLabelsPerSeries
	}
	return &Limiter{
		limits:   limits,
		metrics:  make(map[string]*metricState),
		rejected: make(map[string]uint64),
	}
}

// Check admits or rejects a label set.
//
// Admitting also *records* it, because cardinality is about the set of
// distinct series seen, not about any single sample. A check that did not
// record would admit an unbounded stream of distinct series forever.
func (l *Limiter) Check(labels model.Labels) error {
	if n := len(labels.Pairs); n > l.limits.MaxLabelsPerSeries {
		return l.reject(labels.Name, "", ReasonTooManyLabels,
			fmt.Sprintf("%d labels, limit %d", n, l.limits.MaxLabelsPerSeries))
	}

	// Length is checked before any state is touched: a 2 MB label value is
	// not something to record and then reject.
	for k, v := range labels.Pairs {
		if len(v) > l.limits.MaxLabelValueLen {
			return l.reject(labels.Name, k, ReasonLabelValueTooLong,
				fmt.Sprintf("%d bytes, limit %d", len(v), l.limits.MaxLabelValueLen))
		}
	}

	key := labels.Key()

	l.mu.Lock()
	defer l.mu.Unlock()

	m, ok := l.metrics[labels.Name]
	if !ok {
		m = &metricState{
			series:      make(map[string]struct{}),
			labelValues: make(map[string]map[string]struct{}),
		}
		l.metrics[labels.Name] = m
	}

	// An already-known series is always admitted. Re-checking limits for a
	// series that exists would start rejecting established metrics the
	// moment a limit was lowered, which is the opposite of useful.
	if _, seen := m.series[key]; seen {
		l.accepted++
		return nil
	}

	// Per-label check first. This is the one that catches the dangerous
	// case early: a request-id label blows the per-label limit long before
	// the series total, and reporting *which* label is what makes the
	// rejection actionable.
	for k, v := range labels.Pairs {
		vals := m.labelValues[k]
		if vals == nil {
			continue
		}
		if _, known := vals[v]; known {
			continue
		}
		if len(vals) >= l.limits.PerLabel {
			l.rejected[labels.Name+"/"+string(ReasonLabelCardinality)+":"+k]++
			return &Error{
				Reason: ReasonLabelCardinality,
				Metric: labels.Name,
				Label:  k,
				Detail: fmt.Sprintf("%d distinct values, limit %d", len(vals), l.limits.PerLabel),
			}
		}
	}

	if len(m.series) >= l.limits.PerMetric {
		l.rejected[labels.Name+"/"+string(ReasonMetricCardinality)]++
		return &Error{
			Reason: ReasonMetricCardinality,
			Metric: labels.Name,
			Detail: fmt.Sprintf("%d series, limit %d", len(m.series), l.limits.PerMetric),
		}
	}

	// Commit.
	m.series[key] = struct{}{}
	for k, v := range labels.Pairs {
		vals := m.labelValues[k]
		if vals == nil {
			vals = make(map[string]struct{})
			m.labelValues[k] = vals
		}
		vals[v] = struct{}{}
	}
	l.accepted++
	return nil
}

func (l *Limiter) reject(metric, label string, reason RejectReason, detail string) error {
	l.mu.Lock()
	k := metric + "/" + string(reason)
	if label != "" {
		k += ":" + label
	}
	l.rejected[k]++
	l.mu.Unlock()
	return &Error{Reason: reason, Metric: metric, Label: label, Detail: detail}
}

// Stats for meta-monitoring.
type Stats struct {
	Metrics  int
	Series   int
	Accepted uint64
	Rejected map[string]uint64
}

func (l *Limiter) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()

	total := 0
	for _, m := range l.metrics {
		total += len(m.series)
	}
	rej := make(map[string]uint64, len(l.rejected))
	for k, v := range l.rejected {
		rej[k] = v
	}
	return Stats{Metrics: len(l.metrics), Series: total, Accepted: l.accepted, Rejected: rej}
}

// SeriesCount returns the current series count for one metric.
func (l *Limiter) SeriesCount(metric string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m, ok := l.metrics[metric]; ok {
		return len(m.series)
	}
	return 0
}

// LabelValueCount returns distinct values seen for one label on one metric.
func (l *Limiter) LabelValueCount(metric, label string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m, ok := l.metrics[metric]; ok {
		return len(m.labelValues[label])
	}
	return 0
}

// TopLabels ranks a metric's labels by distinct value count.
//
// The first thing anyone wants when a cardinality alert fires: not "this
// metric has too many series" but "this metric has too many series
// *because of this label*".
func (l *Limiter) TopLabels(metric string, n int) []LabelCount {
	l.mu.Lock()
	defer l.mu.Unlock()

	m, ok := l.metrics[metric]
	if !ok {
		return nil
	}
	out := make([]LabelCount, 0, len(m.labelValues))
	for k, vals := range m.labelValues {
		out = append(out, LabelCount{Label: k, Distinct: len(vals)})
	}
	sortLabelCounts(out)
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}

type LabelCount struct {
	Label    string
	Distinct int
}

func sortLabelCounts(s []LabelCount) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0; j-- {
			if s[j].Distinct > s[j-1].Distinct ||
				(s[j].Distinct == s[j-1].Distinct && s[j].Label < s[j-1].Label) {
				s[j], s[j-1] = s[j-1], s[j]
				continue
			}
			break
		}
	}
}
