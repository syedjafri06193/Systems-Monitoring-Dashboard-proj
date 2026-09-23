// Command agent collects local metrics and pushes them to ingest.
//
// The design rules it embodies (§5.3):
//
//   - Batch with a deadline: flush at N points or T milliseconds,
//     whichever comes first. T is a direct line item in the latency
//     budget.
//   - Stamp origin_ts at observation, not at flush, or end-to-end latency
//     is unmeasurable.
//   - Bound the buffer. If ingest is unreachable the agent must drop and
//     count, not grow until the node OOMs. An observability agent that
//     takes down the host it monitors is a genuinely bad outcome and has
//     happened to real products.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

func main() {
	var (
		flushEvery  = flag.Duration("flush-interval", 50*time.Millisecond, "batch deadline")
		flushPoints = flag.Int("flush-points", 500, "batch size")
		bufferCap   = flag.Int("buffer", 10000, "bounded local buffer")
		interval    = flag.Duration("interval", 200*time.Millisecond, "collection interval")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	logger.Info("agent starting",
		"interval", *interval, "flush_interval", *flushEvery,
		"flush_points", *flushPoints, "buffer", *bufferCap)

	b := newBoundedBuffer(*bufferCap)

	// Collection. In a real agent this reads /proc, an exporter, or an
	// in-process registry; the shape is what matters here.
	go func() {
		t := time.NewTicker(*interval)
		defer t.Stop()
		for range t.C {
			now := time.Now().UnixMilli()
			// origin_ts is stamped HERE, at observation, not at flush.
			b.push(model.Point{Series: 1, OriginTsMs: now, Value: 1})
		}
	}()

	// Flush with a deadline.
	t := time.NewTicker(*flushEvery)
	defer t.Stop()
	for range t.C {
		batch := b.drain(*flushPoints)
		if len(batch) == 0 {
			continue
		}
		if dropped := b.dropped(); dropped > 0 {
			logger.Warn("agent dropped points; ingest may be unreachable", "dropped", dropped)
		}
		fmt.Printf("flush %d points, first origin_ts=%d\n", len(batch), batch[0].OriginTsMs)
	}
}

// boundedBuffer drops rather than growing.
type boundedBuffer struct {
	mu      sync.Mutex
	points  []model.Point
	cap     int
	dropCnt uint64
}

func newBoundedBuffer(capacity int) *boundedBuffer {
	return &boundedBuffer{points: make([]model.Point, 0, capacity), cap: capacity}
}

func (b *boundedBuffer) push(p model.Point) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.points) >= b.cap {
		// Drop and count. Growing here is how an observability agent takes
		// down the host it is supposed to be watching.
		b.dropCnt++
		return
	}
	b.points = append(b.points, p)
}

func (b *boundedBuffer) drain(n int) []model.Point {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > len(b.points) {
		n = len(b.points)
	}
	out := make([]model.Point, n)
	copy(out, b.points[:n])
	b.points = append(b.points[:0], b.points[n:]...)
	return out
}

func (b *boundedBuffer) dropped() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropCnt
}
