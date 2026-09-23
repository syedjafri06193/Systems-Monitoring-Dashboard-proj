package cardinality

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

func lbl(name string, kv ...string) model.Labels {
	l := model.Labels{Name: name}
	if len(kv) > 0 {
		l.Pairs = map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			l.Pairs[kv[i]] = kv[i+1]
		}
	}
	return l
}

func TestAcceptsWithinLimits(t *testing.T) {
	l := New(Limits{PerMetric: 100, PerLabel: 10})
	for i := 0; i < 10; i++ {
		if err := l.Check(lbl("cpu", "host", fmt.Sprintf("h%d", i))); err != nil {
			t.Fatalf("rejected %d: %v", i, err)
		}
	}
	if l.SeriesCount("cpu") != 10 {
		t.Fatalf("series = %d", l.SeriesCount("cpu"))
	}
}

func TestCardinalityBombIsRejected(t *testing.T) {
	// The §16.2 chaos test: 100k label values must be rejected at ingest
	// with a metric naming the label, and the TSDB unaffected.
	l := New(Limits{PerMetric: 100_000, PerLabel: 1000})

	var rejected int
	var firstErr error
	for i := 0; i < 100_000; i++ {
		err := l.Check(lbl("http_requests", "request_id", fmt.Sprintf("req-%d", i)))
		if err != nil {
			rejected++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if rejected == 0 {
		t.Fatal("a 100k-value label was accepted")
	}
	if l.LabelValueCount("http_requests", "request_id") > 1000 {
		t.Fatalf("recorded %d distinct values past the limit", l.LabelValueCount("http_requests", "request_id"))
	}

	// The error must name the offending label, or the person who added it
	// spends an hour hunting instead of a minute fixing.
	var ce *Error
	if !errors.As(firstErr, &ce) {
		t.Fatalf("error is not a cardinality.Error: %v", firstErr)
	}
	if ce.Label != "request_id" {
		t.Fatalf("error blamed label %q, want request_id", ce.Label)
	}
	if !strings.Contains(ce.Error(), "request_id") {
		t.Fatalf("error message does not name the label: %s", ce.Error())
	}
	t.Logf("rejected with: %v", firstErr)
}

func TestPerLabelLimitCatchesTheDangerousCaseFirst(t *testing.T) {
	// A request-id label blows the per-label limit long before the series
	// total, and reporting which label is what makes it actionable.
	l := New(Limits{PerMetric: 1_000_000, PerLabel: 50})

	var err error
	for i := 0; i < 500 && err == nil; i++ {
		err = l.Check(lbl("m", "request_id", fmt.Sprintf("r%d", i)))
	}
	if err == nil {
		t.Fatal("no rejection")
	}
	var ce *Error
	errors.As(err, &ce)
	if ce.Reason != ReasonLabelCardinality {
		t.Fatalf("reason = %v, want label_cardinality", ce.Reason)
	}
}

func TestPerMetricLimit(t *testing.T) {
	// Many labels each within their own limit can still multiply out past
	// the metric's total, because series count is the *product* of label
	// cardinalities.
	l := New(Limits{PerMetric: 20, PerLabel: 1000})

	var err error
	for i := 0; i < 100 && err == nil; i++ {
		err = l.Check(lbl("m", "a", fmt.Sprintf("%d", i%10), "b", fmt.Sprintf("%d", i/10)))
	}
	if err == nil {
		t.Fatal("no rejection past PerMetric")
	}
	var ce *Error
	errors.As(err, &ce)
	if ce.Reason != ReasonMetricCardinality {
		t.Fatalf("reason = %v", ce.Reason)
	}
}

func TestKnownSeriesIsAlwaysAdmitted(t *testing.T) {
	// Re-checking limits for an existing series would start rejecting
	// established metrics the moment a limit was lowered, which is the
	// opposite of useful.
	l := New(Limits{PerMetric: 2, PerLabel: 10})
	l.Check(lbl("m", "h", "a"))
	l.Check(lbl("m", "h", "b"))

	// Full. A new series is rejected...
	if err := l.Check(lbl("m", "h", "c")); err == nil {
		t.Fatal("accepted a third series past the limit")
	}
	// ...but the existing ones keep flowing.
	for i := 0; i < 100; i++ {
		if err := l.Check(lbl("m", "h", "a")); err != nil {
			t.Fatalf("an established series was rejected: %v", err)
		}
	}
}

func TestLabelValueLengthIsCheckedBeforeRecording(t *testing.T) {
	// A 2 MB label value -- a stack trace, a serialised payload -- is not
	// something to record and then reject.
	l := New(Limits{PerMetric: 100, PerLabel: 100, MaxLabelValueLen: 64})
	huge := strings.Repeat("x", 5000)

	err := l.Check(lbl("m", "trace", huge))
	if err == nil {
		t.Fatal("accepted a 5000-byte label value")
	}
	var ce *Error
	errors.As(err, &ce)
	if ce.Reason != ReasonLabelValueTooLong {
		t.Fatalf("reason = %v", ce.Reason)
	}
	if l.LabelValueCount("m", "trace") != 0 {
		t.Fatal("the oversized value was recorded before being rejected")
	}
}

func TestTooManyLabels(t *testing.T) {
	l := New(Limits{PerMetric: 100, PerLabel: 100, MaxLabelsPerSeries: 3})
	kv := []string{"a", "1", "b", "2", "c", "3", "d", "4"}
	err := l.Check(lbl("m", kv...))
	if err == nil {
		t.Fatal("accepted 4 labels against a limit of 3")
	}
	var ce *Error
	errors.As(err, &ce)
	if ce.Reason != ReasonTooManyLabels {
		t.Fatalf("reason = %v", ce.Reason)
	}
}

func TestRejectionIsCounted(t *testing.T) {
	// Reject loudly: a silent drop is almost as bad as accepting.
	l := New(Limits{PerMetric: 1, PerLabel: 1})
	l.Check(lbl("m", "h", "a"))
	l.Check(lbl("m", "h", "b"))
	l.Check(lbl("m", "h", "c"))

	s := l.Stats()
	total := uint64(0)
	for _, v := range s.Rejected {
		total += v
	}
	if total < 2 {
		t.Fatalf("rejections not counted: %+v", s.Rejected)
	}
	if s.Accepted != 1 {
		t.Fatalf("accepted = %d", s.Accepted)
	}
}

func TestTopLabelsRanksTheCulprit(t *testing.T) {
	// The first thing anyone wants when a cardinality alert fires is not
	// "this metric has too many series" but "because of this label".
	l := New(Limits{PerMetric: 100_000, PerLabel: 100_000})
	for i := 0; i < 500; i++ {
		l.Check(lbl("m",
			"env", "prod", // 1 value
			"host", fmt.Sprintf("h%d", i%5), // 5 values
			"request_id", fmt.Sprintf("r%d", i), // 500 values
		))
	}

	top := l.TopLabels("m", 3)
	if len(top) == 0 {
		t.Fatal("no labels ranked")
	}
	if top[0].Label != "request_id" {
		t.Fatalf("top label = %q with %d values; want request_id", top[0].Label, top[0].Distinct)
	}
	t.Logf("top labels: %+v", top)
}

func TestTopLabelsOfUnknownMetric(t *testing.T) {
	if got := New(DefaultLimits()).TopLabels("nope", 5); got != nil {
		t.Fatalf("got %v", got)
	}
}

func TestZeroLimitsFallBackToDefaults(t *testing.T) {
	// A limit set to "unlimited because we don't know yet" is the same as
	// no limit, and the failure it permits takes down the database.
	l := New(Limits{})
	for i := 0; i < 20_000; i++ {
		if err := l.Check(lbl("m", "h", fmt.Sprintf("%d", i))); err != nil {
			t.Logf("rejected at %d: %v", i, err)
			return
		}
	}
	t.Fatal("zero limits behaved as unlimited")
}

func TestMetricsAreIndependent(t *testing.T) {
	l := New(Limits{PerMetric: 2, PerLabel: 10})
	l.Check(lbl("a", "h", "1"))
	l.Check(lbl("a", "h", "2"))
	if err := l.Check(lbl("b", "h", "1")); err != nil {
		t.Fatalf("one metric's limit affected another: %v", err)
	}
}

func TestUnlabelledMetricIsFine(t *testing.T) {
	l := New(DefaultLimits())
	if err := l.Check(lbl("up")); err != nil {
		t.Fatalf("rejected an unlabelled metric: %v", err)
	}
}

func TestConcurrentChecksAreSafe(t *testing.T) {
	// Run under -race. Ingest is concurrent by design.
	l := New(Limits{PerMetric: 10_000, PerLabel: 1000})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				l.Check(lbl("m", "host", fmt.Sprintf("h%d", i%200)))
				l.Stats()
				l.SeriesCount("m")
			}
		}(g)
	}
	wg.Wait()

	// 200 distinct hosts, whatever the interleaving.
	if got := l.SeriesCount("m"); got != 200 {
		t.Fatalf("series = %d, want 200", got)
	}
}
