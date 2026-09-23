package ring

import (
	"sync"
	"testing"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

func TestAppendAndRange(t *testing.T) {
	r := New(100)
	for i := 0; i < 10; i++ {
		r.Append(1, int64(i)*200, float32(i))
	}
	got := r.Range([]model.SeriesID{1}, 0, 2000)
	if len(got) != 10 {
		t.Fatalf("got %d points", len(got))
	}
	for i, p := range got {
		if p.OriginTsMs != int64(i)*200 || p.Value != float32(i) {
			t.Fatalf("point %d: %+v", i, p)
		}
	}
}

func TestRangeIsInclusiveAtBothEnds(t *testing.T) {
	// Load-bearing for the snapshot protocol. The client discards buffered
	// points with ts <= watermark, so the snapshot must contain the
	// watermark point itself or it is lost by both paths.
	r := New(10)
	for i := 0; i < 5; i++ {
		r.Append(1, int64(i)*100, float32(i))
	}
	got := r.Range([]model.SeriesID{1}, 100, 300)
	if len(got) != 3 {
		t.Fatalf("got %d points, want 3 (100, 200, 300 inclusive)", len(got))
	}
	if got[0].OriginTsMs != 100 || got[2].OriginTsMs != 300 {
		t.Fatalf("boundaries: %d..%d", got[0].OriginTsMs, got[2].OriginTsMs)
	}
}

func TestRingEvictsOldestWhenFull(t *testing.T) {
	r := New(5)
	for i := 0; i < 12; i++ {
		r.Append(1, int64(i)*100, float32(i))
	}
	got := r.Range([]model.SeriesID{1}, 0, 100000)
	if len(got) != 5 {
		t.Fatalf("got %d points, want capacity 5", len(got))
	}
	// Points 7..11 survive.
	if got[0].OriginTsMs != 700 || got[4].OriginTsMs != 1100 {
		t.Fatalf("kept the wrong window: %d..%d", got[0].OriginTsMs, got[4].OriginTsMs)
	}
}

func TestRingMemoryIsBounded(t *testing.T) {
	// The whole reason the hot tier is a ring. A growable buffer under a
	// metric that suddenly reports faster turns a cache into an OOM.
	r := New(50)
	for i := 0; i < 100000; i++ {
		r.Append(1, int64(i), 1)
	}
	if n := len(r.Range([]model.SeriesID{1}, 0, 1<<40)); n != 50 {
		t.Fatalf("ring holds %d points after 100k appends", n)
	}
}

func TestRangeSortsDeterministically(t *testing.T) {
	// Two clients requesting the same window must receive identical data,
	// or the protocol is not testable.
	r := New(100)
	for i := 0; i < 5; i++ {
		r.Append(3, int64(i)*100, 3)
		r.Append(1, int64(i)*100, 1)
		r.Append(2, int64(i)*100, 2)
	}
	a := r.Range([]model.SeriesID{1, 2, 3}, 0, 10000)
	b := r.Range([]model.SeriesID{3, 2, 1}, 0, 10000)

	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("point %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
	// Sorted by ts, then series id.
	for i := 1; i < len(a); i++ {
		if a[i].OriginTsMs < a[i-1].OriginTsMs {
			t.Fatalf("not sorted by ts at %d", i)
		}
		if a[i].OriginTsMs == a[i-1].OriginTsMs && a[i].Series < a[i-1].Series {
			t.Fatalf("not sorted by series at %d", i)
		}
	}
}

func TestRangeIgnoresUnknownSeries(t *testing.T) {
	r := New(10)
	r.Append(1, 100, 1)
	got := r.Range([]model.SeriesID{1, 999}, 0, 1000)
	if len(got) != 1 {
		t.Fatalf("got %d points", len(got))
	}
}

func TestRangeRejectsInvertedWindow(t *testing.T) {
	r := New(10)
	r.Append(1, 100, 1)
	if got := r.Range([]model.SeriesID{1}, 500, 100); got != nil {
		t.Fatalf("inverted range returned %d points", len(got))
	}
}

// --- watermark -------------------------------------------------------------

func TestWatermarkTracksNewestTimestamp(t *testing.T) {
	r := New(10)
	r.Append(1, 1000, 1)
	r.Append(2, 2000, 2)
	if got := r.CommittedWatermark(); got != 2000 {
		t.Fatalf("watermark = %d", got)
	}
}

func TestWatermarkNeverGoesBackwards(t *testing.T) {
	// A late point must not move the cut behind data already delivered
	// live — the client would re-apply it as a duplicate.
	r := New(10)
	r.Append(1, 5000, 1)
	r.Append(1, 1000, 2) // late arrival
	if got := r.CommittedWatermark(); got != 5000 {
		t.Fatalf("watermark moved backwards to %d", got)
	}
}

func TestWatermarkIsMonotonicUnderConcurrency(t *testing.T) {
	// The compare-and-swap loop. A plain store would let two goroutines
	// racing with timestamps 900 and 1000 leave the watermark at 900.
	r := New(1000)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				r.Append(model.SeriesID(g%4+1), int64(i), 1)
			}
		}(g)
	}
	wg.Wait()
	if got := r.CommittedWatermark(); got != 499 {
		t.Fatalf("watermark = %d, want 499", got)
	}
}

func TestWatermarkStartsAtZero(t *testing.T) {
	if got := New(10).CommittedWatermark(); got != 0 {
		t.Fatalf("empty ring watermark = %d", got)
	}
}

// --- window (detector input) ----------------------------------------------

func TestWindowReturnsMostRecentPointsOldestFirst(t *testing.T) {
	r := New(100)
	for i := 0; i < 20; i++ {
		r.Append(1, int64(i)*100, float32(i))
	}
	w := r.Window(1, 5)
	if len(w) != 5 {
		t.Fatalf("got %d values", len(w))
	}
	for i, want := range []float64{15, 16, 17, 18, 19} {
		if w[i] != want {
			t.Fatalf("window[%d] = %v want %v", i, w[i], want)
		}
	}
}

func TestWindowClampsToAvailable(t *testing.T) {
	r := New(100)
	r.Append(1, 0, 1)
	r.Append(1, 100, 2)
	if w := r.Window(1, 50); len(w) != 2 {
		t.Fatalf("got %d values", len(w))
	}
}

func TestWindowAcrossTheWrapPoint(t *testing.T) {
	// The off-by-one that a non-wrapped test never reaches.
	r := New(5)
	for i := 0; i < 8; i++ {
		r.Append(1, int64(i)*100, float32(i))
	}
	w := r.Window(5, 5)
	_ = w
	got := r.Window(1, 5)
	for i, want := range []float64{3, 4, 5, 6, 7} {
		if got[i] != want {
			t.Fatalf("window[%d] = %v want %v (full window: %v)", i, got[i], want, got)
		}
	}
}

func TestWindowOfUnknownSeriesIsEmpty(t *testing.T) {
	if w := New(10).Window(42, 5); w != nil {
		t.Fatalf("got %v", w)
	}
}

func TestWindowCopiesRatherThanAliases(t *testing.T) {
	// The detector holds the window across a baseline computation while
	// the ring keeps being written.
	r := New(10)
	for i := 0; i < 5; i++ {
		r.Append(1, int64(i), float32(i))
	}
	w := r.Window(1, 5)
	for i := 0; i < 5; i++ {
		r.Append(1, int64(100+i), 999)
	}
	if w[0] != 0 || w[4] != 4 {
		t.Fatalf("window was mutated by later appends: %v", w)
	}
}

// --- latest ----------------------------------------------------------------

func TestLatest(t *testing.T) {
	r := New(10)
	for i := 0; i < 7; i++ {
		r.Append(1, int64(i)*100, float32(i))
	}
	p, ok := r.Latest(1)
	if !ok || p.OriginTsMs != 600 || p.Value != 6 || p.Series != 1 {
		t.Fatalf("got %+v ok=%v", p, ok)
	}
}

func TestLatestAcrossWrap(t *testing.T) {
	r := New(3)
	for i := 0; i < 10; i++ {
		r.Append(1, int64(i)*100, float32(i))
	}
	p, _ := r.Latest(1)
	if p.OriginTsMs != 900 {
		t.Fatalf("got %d", p.OriginTsMs)
	}
}

func TestLatestOfUnknownSeries(t *testing.T) {
	if _, ok := New(10).Latest(42); ok {
		t.Fatal("unknown series reported a latest point")
	}
}

// --- concurrency -----------------------------------------------------------

func TestConcurrentAppendCreatesOneBufferPerSeries(t *testing.T) {
	// Two goroutines can both miss the read-locked lookup for the same new
	// series. Without the re-check under the write lock, the second
	// discards the first's buffer along with any points in it.
	const goroutines = 32
	const perGoroutine = 100
	// Capacity must exceed the total written, or eviction — which is
	// correct behaviour — would look like the lost-buffer bug under test.
	r := New(goroutines * perGoroutine)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := 0; i < perGoroutine; i++ {
				r.Append(1, int64(g*perGoroutine+i), float32(g))
			}
		}(g)
	}
	close(start)
	wg.Wait()

	if n := len(r.Range([]model.SeriesID{1}, 0, 1<<40)); n != goroutines*perGoroutine {
		t.Fatalf("lost points to a racing buffer creation: got %d want %d", n, goroutines*perGoroutine)
	}
}

func TestConcurrentReadWhileWriting(t *testing.T) {
	// Run under -race. A snapshot scan runs concurrently with ingest by
	// design; the reader must never see a torn point.
	r := New(500)
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			r.Append(model.SeriesID(i%8+1), int64(i), float32(i))
		}
	}()

	for i := 0; i < 2000; i++ {
		ids := []model.SeriesID{1, 2, 3, 4, 5, 6, 7, 8}
		pts := r.Range(ids, 0, 1<<40)
		for _, p := range pts {
			// A torn read would pair one point's timestamp with another's
			// value; the generator writes ts == value.
			if int64(p.Value) != p.OriginTsMs {
				t.Errorf("torn point: ts=%d value=%v", p.OriginTsMs, p.Value)
				break
			}
		}
		r.Window(1, 50)
		r.Latest(3)
		r.Stats()
	}
	close(done)
	wg.Wait()
}

// --- stats -----------------------------------------------------------------

func TestStats(t *testing.T) {
	r := New(10)
	for i := 0; i < 15; i++ {
		r.Append(1, int64(i), 1)
	}
	r.Append(2, 100, 1)

	s := r.Stats()
	if s.SeriesCount != 2 {
		t.Fatalf("series = %d", s.SeriesCount)
	}
	if s.PointCount != 11 { // 10 retained + 1
		t.Fatalf("points = %d", s.PointCount)
	}
	if s.Appended != 16 {
		t.Fatalf("appended = %d", s.Appended)
	}
	if s.WatermarkMs != 100 {
		t.Fatalf("watermark = %d", s.WatermarkMs)
	}
}

func TestSeriesIDsAreSorted(t *testing.T) {
	r := New(10)
	for _, id := range []model.SeriesID{5, 1, 9, 3} {
		r.Append(id, 1, 1)
	}
	got := r.SeriesIDs()
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("not sorted: %v", got)
		}
	}
}

func TestZeroCapacityIsClampedNotFatal(t *testing.T) {
	r := New(0)
	r.Append(1, 100, 1)
	if n := len(r.Range([]model.SeriesID{1}, 0, 1000)); n != 1 {
		t.Fatalf("got %d points", n)
	}
}
