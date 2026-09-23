package hub

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ring"
)

var t0 = time.Unix(1_700_000_000, 0)

func pt(id model.SeriesID, ts int64, v float32) model.Point {
	return model.Point{Series: id, OriginTsMs: ts, Value: v}
}

// --- conflating queue ------------------------------------------------------

func TestQueueDeliversInOrder(t *testing.T) {
	q := NewConflatingQueue(10)
	for i := 1; i <= 3; i++ {
		q.Push(pt(model.SeriesID(i), int64(i)*100, float32(i)), t0)
	}
	got := q.Drain(nil, 10)
	if len(got) != 3 {
		t.Fatalf("got %d", len(got))
	}
	for i, p := range got {
		if p.Series != model.SeriesID(i+1) {
			t.Fatalf("out of order at %d: %+v", i, p)
		}
	}
}

func TestQueueConflatesBySeries(t *testing.T) {
	// The core policy. For a dashboard only the latest value per series
	// matters, so a second point for a series already pending replaces it
	// rather than queueing behind it.
	q := NewConflatingQueue(10)
	q.Push(pt(1, 100, 1), t0)
	q.Push(pt(1, 200, 2), t0)
	q.Push(pt(1, 300, 3), t0)

	got := q.Drain(nil, 10)
	if len(got) != 1 {
		t.Fatalf("got %d points, want 1 conflated", len(got))
	}
	if got[0].Value != 3 || got[0].OriginTsMs != 300 {
		t.Fatalf("kept the wrong value: %+v", got[0])
	}
	if s := q.Stats(); s.Conflated != 2 {
		t.Fatalf("conflated = %d, want 2", s.Conflated)
	}
}

func TestQueueKeepsTheNewerOfTwoConflatedPoints(t *testing.T) {
	// Points arrive out of order from different agents. Overwriting a
	// newer value with an older one makes the chart jump backwards.
	q := NewConflatingQueue(10)
	q.Push(pt(1, 500, 5), t0)
	q.Push(pt(1, 200, 2), t0) // older, arrives second

	got := q.Drain(nil, 10)
	if got[0].OriginTsMs != 500 || got[0].Value != 5 {
		t.Fatalf("older point overwrote newer: %+v", got[0])
	}
}

func TestQueueMemoryIsBoundedBySeriesCount(t *testing.T) {
	// The property the whole design exists for: one slow client cannot
	// grow the gateway's memory. A thousand updates to ten series must
	// occupy ten slots, not a thousand.
	q := NewConflatingQueue(100)
	for i := 0; i < 10000; i++ {
		q.Push(pt(model.SeriesID(i%10+1), int64(i), float32(i)), t0)
	}
	if d := q.Len(); d != 10 {
		t.Fatalf("queue depth %d, want 10 distinct series", d)
	}
	if s := q.Stats(); s.MaxDepth > 10 {
		t.Fatalf("max depth reached %d", s.MaxDepth)
	}
}

func TestQueueDropsOnlyWhenFullOfDistinctSeries(t *testing.T) {
	// Dropping is the genuinely-full case: there is nothing to conflate
	// against because every slot holds a different series.
	q := NewConflatingQueue(3)
	for i := 1; i <= 3; i++ {
		if !q.Push(pt(model.SeriesID(i), 100, 1), t0) {
			t.Fatalf("push %d rejected while space remained", i)
		}
	}
	if q.Push(pt(4, 100, 1), t0) {
		t.Fatal("fourth distinct series accepted into a queue of 3")
	}
	if s := q.Stats(); s.Dropped != 1 {
		t.Fatalf("dropped = %d", s.Dropped)
	}

	// But a repeat of an already-pending series still succeeds, because it
	// conflates rather than adding to the queue.
	if !q.Push(pt(2, 200, 9), t0) {
		t.Fatal("conflation rejected on a full queue")
	}
}

func TestQueueDrainRespectsLimit(t *testing.T) {
	q := NewConflatingQueue(100)
	for i := 1; i <= 20; i++ {
		q.Push(pt(model.SeriesID(i), 100, 1), t0)
	}
	got := q.Drain(nil, 5)
	if len(got) != 5 {
		t.Fatalf("drained %d, want 5", len(got))
	}
	if q.Len() != 15 {
		t.Fatalf("%d left, want 15", q.Len())
	}
}

func TestQueueDrainIsFairAcrossSeries(t *testing.T) {
	// A map alone has randomised iteration order, so under sustained
	// pressure some series would be delivered far less often than others
	// for no reason the user could see. Insertion order fixes that.
	q := NewConflatingQueue(100)
	for i := 1; i <= 10; i++ {
		q.Push(pt(model.SeriesID(i), 100, 1), t0)
	}
	got := q.Drain(nil, 4)
	for i, p := range got {
		if p.Series != model.SeriesID(i+1) {
			t.Fatalf("unfair drain order: %+v at %d", p, i)
		}
	}
}

func TestQueueDrainReusesBuffer(t *testing.T) {
	q := NewConflatingQueue(100)
	buf := make([]model.Point, 0, 64)
	for round := 0; round < 3; round++ {
		for i := 1; i <= 10; i++ {
			q.Push(pt(model.SeriesID(i), int64(round), 1), t0)
		}
		buf = q.Drain(buf, 100)
		if len(buf) != 10 {
			t.Fatalf("round %d drained %d", round, len(buf))
		}
		if cap(buf) != 64 {
			t.Fatalf("drain reallocated: cap %d", cap(buf))
		}
	}
}

func TestQueueDrainEmpty(t *testing.T) {
	q := NewConflatingQueue(10)
	if got := q.Drain(nil, 10); len(got) != 0 {
		t.Fatalf("got %d from an empty queue", len(got))
	}
}

func TestQueueOrderListDoesNotGrowUnbounded(t *testing.T) {
	// The order slice is compacted on drain. Without that it would grow by
	// one entry per enqueue forever — a slow memory leak on a connection
	// that is otherwise behaving perfectly.
	q := NewConflatingQueue(50)
	buf := make([]model.Point, 0, 64)
	for round := 0; round < 1000; round++ {
		for i := 1; i <= 20; i++ {
			q.Push(pt(model.SeriesID(i), int64(round), 1), t0)
		}
		buf = q.Drain(buf, 100)
	}
	q.mu.Lock()
	orderLen := len(q.order)
	q.mu.Unlock()
	if orderLen > 50 {
		t.Fatalf("order list grew to %d entries", orderLen)
	}
}

func TestQueueStuckDetection(t *testing.T) {
	// A connection stuck full for ~30 s is closed: a client that is not
	// reading is not watching, and its buffer costs everyone else.
	q := NewConflatingQueue(2)
	q.Push(pt(1, 100, 1), t0)
	q.Push(pt(2, 100, 1), t0)

	if d := q.StuckFor(t0); d != 0 {
		t.Fatalf("not yet full-and-rejected, got %v", d)
	}

	q.Push(pt(3, 100, 1), t0) // rejected; marks fullSince
	if d := q.StuckFor(t0.Add(45 * time.Second)); d != 45*time.Second {
		t.Fatalf("stuck for %v", d)
	}
}

func TestQueueStuckClearsOnDrain(t *testing.T) {
	q := NewConflatingQueue(2)
	q.Push(pt(1, 100, 1), t0)
	q.Push(pt(2, 100, 1), t0)
	q.Push(pt(3, 100, 1), t0) // rejected

	q.Drain(nil, 1)
	if d := q.StuckFor(t0.Add(time.Minute)); d != 0 {
		t.Fatalf("still reported stuck after draining: %v", d)
	}
}

func TestConflationRateIsReportable(t *testing.T) {
	// A high conflation rate is the first thing anyone wants during a "why
	// is my chart choppy" investigation.
	q := NewConflatingQueue(10)
	q.Push(pt(1, 100, 1), t0)
	for i := 0; i < 9; i++ {
		q.Push(pt(1, int64(200+i), 1), t0)
	}
	s := q.Stats()
	if got := s.ConflationRate(); got < 0.89 || got > 0.91 {
		t.Fatalf("conflation rate = %.3f, want ~0.9", got)
	}
}

func TestConflationRateOfIdleQueueIsZero(t *testing.T) {
	if got := NewConflatingQueue(10).Stats().ConflationRate(); got != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestQueueIsSafeUnderConcurrency(t *testing.T) {
	// Run under -race. Ingest pushes from the fanout goroutine while the
	// connection's writer drains.
	q := NewConflatingQueue(64)
	done := make(chan struct{})

	// Producers and the drainer use separate wait groups: waiting on one
	// group that also contains the drainer deadlocks, because the drainer
	// only exits once done is closed.
	var producers, drainer sync.WaitGroup

	for g := 0; g < 4; g++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := 0; i < 5000; i++ {
				q.Push(pt(model.SeriesID(i%100+1), int64(i), float32(i)), t0)
			}
		}()
	}

	drainer.Add(1)
	go func() {
		defer drainer.Done()
		buf := make([]model.Point, 0, 64)
		for {
			select {
			case <-done:
				return
			default:
			}
			buf = q.Drain(buf, 16)
			q.Stats()
			q.StuckFor(t0)
		}
	}()

	producers.Wait()
	close(done)
	drainer.Wait()

	if q.Len() > 64 {
		t.Fatalf("queue exceeded its bound: %d", q.Len())
	}
}

// --- snapshot boundary -----------------------------------------------------

func TestSnapshotReturnsExplicitWatermark(t *testing.T) {
	// The server must return the cut point. Without it the client is
	// guessing where history ends, and "use the timestamp of the last
	// snapshot point" is wrong whenever a series has no recent data.
	r := ring.New(100)
	r.Append(1, 1000, 1)
	r.Append(1, 2000, 2)
	r.Append(2, 5000, 3) // a different series is newer

	snap, err := TakeSnapshot(r, []model.SeriesID{1}, 0, 9999)
	if err != nil {
		t.Fatal(err)
	}
	// The watermark is the ring's, not series 1's last point — series 1
	// has been idle since 2000 but the stream is live to 5000.
	if snap.Header.WatermarkTsMs != 5000 {
		t.Fatalf("watermark = %d, want 5000", snap.Header.WatermarkTsMs)
	}
}

func TestSnapshotClampsRequestToWatermark(t *testing.T) {
	// A client asking for data up to "now" is asking for a future the
	// server cannot promise. Answering up to the watermark and saying so
	// is honest; the rest arrives live.
	r := ring.New(100)
	r.Append(1, 1000, 1)

	snap, _ := TakeSnapshot(r, []model.SeriesID{1}, 0, 1_000_000)
	if snap.Header.WatermarkTsMs != 1000 {
		t.Fatalf("watermark = %d", snap.Header.WatermarkTsMs)
	}
	for _, p := range snap.Points {
		if p.OriginTsMs > snap.Header.WatermarkTsMs {
			t.Fatalf("snapshot contains a point above its own watermark: %+v", p)
		}
	}
}

func TestSnapshotRejectsInvertedRange(t *testing.T) {
	r := ring.New(10)
	if _, err := TakeSnapshot(r, []model.SeriesID{1}, 500, 100); err != ErrInvalidRange {
		t.Fatalf("got %v", err)
	}
}

func TestSnapshotOfEmptyRingIsEmptyNotAnError(t *testing.T) {
	// Normal at startup, and the client must render an empty chart rather
	// than an error.
	snap, err := TakeSnapshot(ring.New(10), []model.SeriesID{1}, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Points) != 0 || snap.Header.SeriesCount != 0 {
		t.Fatalf("got %+v", snap.Header)
	}
}

func TestSnapshotSeriesCountIsWhatWasFoundNotWhatWasAsked(t *testing.T) {
	// "You have no data for 3 of my 10 series" and "your request was
	// wrong" are different problems, and the client can only tell them
	// apart if this counts what was found.
	r := ring.New(10)
	r.Append(1, 100, 1)

	snap, _ := TakeSnapshot(r, []model.SeriesID{1, 2, 3}, 0, 1000)
	if snap.Header.SeriesCount != 1 {
		t.Fatalf("series count = %d, want 1", snap.Header.SeriesCount)
	}
}

func TestReconcileDiscardsAtOrBelowWatermark(t *testing.T) {
	// The inclusive boundary is the whole rule. A point exactly *at* the
	// watermark is already in the snapshot, so keeping it duplicates.
	snap := &Snapshot{
		Header: SnapshotHeaderAt(1000),
		Points: []model.Point{pt(1, 800, 1), pt(1, 1000, 2)},
	}
	buffered := []model.Point{
		pt(1, 900, 9),  // below: already in the snapshot
		pt(1, 1000, 9), // exactly at: already in the snapshot
		pt(1, 1200, 3), // above: genuinely new
	}
	got := Reconcile(snap, buffered)

	if len(got) != 3 {
		t.Fatalf("got %d points, want 3", len(got))
	}
	if got[2].OriginTsMs != 1200 || got[2].Value != 3 {
		t.Fatalf("wrong point kept: %+v", got[2])
	}
}

func TestReconcileWithNothingBuffered(t *testing.T) {
	snap := &Snapshot{Header: SnapshotHeaderAt(1000), Points: []model.Point{pt(1, 500, 1)}}
	if got := Reconcile(snap, nil); len(got) != 1 {
		t.Fatalf("got %d", len(got))
	}
}

// injectingSource appends a point *during* the Range scan.
//
// This is the interleaving that the watermark ordering exists to handle,
// and it is the one that is almost impossible to hit reliably by racing
// goroutines. Forcing it deterministically turns a flaky timing test into
// a proof.
type injectingSource struct {
	r       *ring.Ring
	inject  func()
	watched bool
}

func (s *injectingSource) CommittedWatermark() int64 { return s.r.CommittedWatermark() }

func (s *injectingSource) Range(ids []model.SeriesID, from, to int64) []model.Point {
	out := s.r.Range(ids, from, to)
	// The write lands after the scan has read its data but before the
	// caller sees the result.
	if s.inject != nil && !s.watched {
		s.watched = true
		s.inject()
	}
	return out
}

func TestWatermarkReadBeforeScanLosesNothing(t *testing.T) {
	// The §7.2 correctness argument, made concrete.
	//
	// A point written during the snapshot scan is strictly above the
	// watermark that was read before it. It is therefore absent from the
	// snapshot AND retained from the live buffer — delivered exactly once.
	r := ring.New(1000)
	for ts := int64(1000); ts <= 1010; ts++ {
		r.Append(1, ts, float32(ts))
	}

	const injectedTs = 1011
	src := &injectingSource{r: r, inject: func() { r.Append(1, injectedTs, injectedTs) }}

	snap, err := TakeSnapshot(src, []model.SeriesID{1}, 0, 1<<40)
	if err != nil {
		t.Fatal(err)
	}

	// The live subscription saw the injected point, as it would in reality.
	buffered := []model.Point{pt(1, injectedTs, injectedTs)}
	merged := Reconcile(snap, buffered)

	count := 0
	for _, p := range merged {
		if p.OriginTsMs == injectedTs {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("point written during the scan appears %d times, want exactly 1", count)
	}
	if snap.Header.WatermarkTsMs != 1010 {
		t.Fatalf("watermark = %d, want the value read before the scan (1010)", snap.Header.WatermarkTsMs)
	}
}

func TestWatermarkReadAfterScanWouldLoseAPoint(t *testing.T) {
	// The bug the ordering prevents, demonstrated rather than asserted.
	//
	// Reading the watermark AFTER the scan makes it include the injected
	// point's timestamp — so the client discards that point from its live
	// buffer, while the scan never returned it. It is in neither path: a
	// silent hole that looks like a quiet period.
	r := ring.New(1000)
	for ts := int64(1000); ts <= 1010; ts++ {
		r.Append(1, ts, float32(ts))
	}

	const injectedTs = 1011
	points := r.Range([]model.SeriesID{1}, 0, 1<<40) // the scan, before the write
	r.Append(1, injectedTs, injectedTs)              // written during the scan
	wrongWatermark := r.CommittedWatermark()         // read after: now 1011

	snap := &Snapshot{Header: SnapshotHeaderAt(wrongWatermark), Points: points}
	merged := Reconcile(snap, []model.Point{pt(1, injectedTs, injectedTs)})

	for _, p := range merged {
		if p.OriginTsMs == injectedTs {
			t.Fatal("expected the wrong ordering to lose the point; it did not, so this test no longer proves anything")
		}
	}
}

// TestNoGapsOrDuplicatesUnderContinuousIngest is the M5 acceptance test:
// "a test that continuously loads the page against a running stream
// produces charts with no gaps and no duplicate timestamps, over hundreds
// of iterations."
func TestNoGapsOrDuplicatesUnderContinuousIngest(t *testing.T) {
	iterations := 300
	if testing.Short() {
		iterations = 20
	}

	for iter := 0; iter < iterations; iter++ {
		r := ring.New(20000)
		var wg sync.WaitGroup
		stop := make(chan struct{})

		// Ingest, running continuously.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ts := int64(1000); ; ts++ {
				select {
				case <-stop:
					return
				default:
				}
				r.Append(1, ts, float32(ts))
			}
		}()

		// Let history accumulate.
		for r.CommittedWatermark() < 1100 {
			runtime.Gosched()
		}

		// §7.2 step 1: subscribe. Everything from here is buffered, not
		// rendered. The buffer advances a cursor rather than rescanning,
		// so the collector stays O(new points).
		subscribeAt := r.CommittedWatermark()
		var mu sync.Mutex
		buffered := make([]model.Point, 0, 4096)

		wg.Add(1)
		go func() {
			defer wg.Done()
			cursor := subscribeAt
			for {
				select {
				case <-stop:
					// Final drain of anything appended before the stop.
					if wm := r.CommittedWatermark(); wm > cursor {
						pts := r.Range([]model.SeriesID{1}, cursor+1, wm)
						mu.Lock()
						buffered = append(buffered, pts...)
						mu.Unlock()
					}
					return
				default:
				}
				if wm := r.CommittedWatermark(); wm > cursor {
					pts := r.Range([]model.SeriesID{1}, cursor+1, wm)
					mu.Lock()
					buffered = append(buffered, pts...)
					mu.Unlock()
					cursor = wm
				} else {
					runtime.Gosched()
				}
			}
		}()

		// §7.2 steps 2-3: snapshot with an explicit watermark, taken while
		// writes continue.
		for i := rand.Intn(50); i > 0; i-- {
			runtime.Gosched()
		}
		snap, err := TakeSnapshot(r, []model.SeriesID{1}, 0, 1<<40)
		if err != nil {
			t.Fatal(err)
		}

		close(stop)
		wg.Wait()

		// §7.2 steps 4-6: render the snapshot, drain the buffer discarding
		// ts <= watermark, go live.
		mu.Lock()
		merged := Reconcile(snap, buffered)
		mu.Unlock()

		seen := make(map[int64]int, len(merged))
		var lo, hi int64 = 1 << 62, -1
		for _, p := range merged {
			seen[p.OriginTsMs]++
			if p.OriginTsMs < lo {
				lo = p.OriginTsMs
			}
			if p.OriginTsMs > hi {
				hi = p.OriginTsMs
			}
		}

		for ts, n := range seen {
			if n != 1 {
				t.Fatalf("iteration %d: timestamp %d appears %d times", iter, ts, n)
			}
		}
		for ts := lo; ts <= hi; ttsNext(&ts) {
			if seen[ts] == 0 {
				t.Fatalf("iteration %d: gap at %d (covered %d..%d)", iter, ts, lo, hi)
			}
		}
	}
}

// ttsNext exists only so the gap loop reads as a scan rather than a
// fencepost puzzle.
func ttsNext(ts *int64) { *ts++ }

func TestReasonStringsAreDistinct(t *testing.T) {
	// All four cases take the identical code path; the reason exists only
	// for metrics. Four hand-written variants would be four off-by-ones.
	seen := map[string]bool{}
	for _, r := range []Reason{ReasonInitialLoad, ReasonReconnect, ReasonAddSeries, ReasonWidenRange} {
		s := r.String()
		if seen[s] {
			t.Fatalf("duplicate reason string %q", s)
		}
		seen[s] = true
	}
}
