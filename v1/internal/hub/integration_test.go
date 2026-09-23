package hub

import (
	"bufio"
	"encoding/json"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ring"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/wire"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ws"
)

// testServer runs a real gateway over a real socket.
type testServer struct {
	hub  *Hub
	ring *ring.Ring
	reg  *wire.Registry
	addr string
	stop func()
}

func newTestServer(t *testing.T, opts Options) *testServer {
	t.Helper()

	r := ring.New(10000)
	reg := wire.NewRegistry()
	h := New(r, reg, opts)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			c, err := ws.Upgrade(w, req)
			if err != nil {
				return
			}
			h.Serve(c)
		}),
	}
	go srv.Serve(ln)

	return &testServer{
		hub: h, ring: r, reg: reg, addr: ln.Addr().String(),
		stop: func() { srv.Close(); ln.Close() },
	}
}

// testClient is a minimal WebSocket client for the gateway protocol.
type testClient struct {
	conn net.Conn
	br   *bufio.Reader
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := ws.GenerateKey()
	req := "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	conn.Write([]byte(req))

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("handshake: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	conn.SetReadDeadline(time.Time{})
	return &testClient{conn: conn, br: br}
}

func (c *testClient) sendControl(t *testing.T, ctrl wire.Control) {
	t.Helper()
	b, _ := json.Marshal(ctrl)
	if err := ws.WriteClientFrame(c.conn, ws.OpText, b, true); err != nil {
		t.Fatal(err)
	}
}

// readFrames reads one WebSocket message and splits it into wire frames.
func (c *testClient) readFrames(t *testing.T, timeout time.Duration) []wire.Frame {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(timeout))
	op, payload, _, err := ws.ReadServerFrame(c.br)
	if err != nil {
		return nil
	}
	if op != ws.OpBinary {
		return nil
	}
	var out []wire.Frame
	rest := payload
	for len(rest) > 0 {
		var f wire.Frame
		f, rest, err = wire.ReadFrame(rest)
		if err != nil {
			return out
		}
		out = append(out, f)
	}
	return out
}

func (c *testClient) close() { c.conn.Close() }

func fastOpts() Options {
	o := DefaultOptions()
	o.FlushInterval = 5 * time.Millisecond
	o.HeartbeatInterval = 50 * time.Millisecond
	return o
}

// --- end to end ------------------------------------------------------------

func TestEndToEndSubscribeAndStream(t *testing.T) {
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	id, _ := srv.reg.Intern(model.Labels{Name: "cpu"})

	c := dial(t, srv.addr)
	defer c.close()
	c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{"cpu"}})

	// Wait for the subscription to register before publishing, or the
	// point is fanned out to nobody.
	deadline := time.Now().Add(2 * time.Second)
	for len(srv.hub.ActiveSeries()) == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	srv.ring.Append(id, 1000, 42)
	srv.hub.Publish([]model.Point{{Series: id, OriginTsMs: 1000, Value: 42}})

	var got []model.Point
	for time.Now().Before(deadline) && len(got) == 0 {
		for _, f := range c.readFrames(t, time.Second) {
			if f.Type == wire.FrameDeltaBatch {
				pts, err := wire.ParseDeltaBatch(f.Payload, nil)
				if err == nil {
					got = append(got, pts...)
				}
			}
		}
	}

	if len(got) == 0 {
		t.Fatal("no points reached the client")
	}
	if got[0].Value != 42 || got[0].OriginTsMs != 1000 {
		t.Fatalf("got %+v", got[0])
	}
}

func TestSeriesRegistryArrivesBeforePointsReferenceIt(t *testing.T) {
	// A point naming an id the client has never seen is unusable. The
	// registry must lead.
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	id, _ := srv.reg.Intern(model.Labels{Name: "cpu", Pairs: map[string]string{"host": "a"}})

	c := dial(t, srv.addr)
	defer c.close()
	c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{strconv.Itoa(int(id))}})

	deadline := time.Now().Add(2 * time.Second)
	for len(srv.hub.ActiveSeries()) == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	srv.hub.Publish([]model.Point{{Series: id, OriginTsMs: 1000, Value: 1}})

	known := make(map[model.SeriesID]bool)
	for time.Now().Before(deadline) {
		frames := c.readFrames(t, 500*time.Millisecond)
		if frames == nil {
			continue
		}
		for _, f := range frames {
			switch f.Type {
			case wire.FrameSeriesRegistry:
				defs, err := wire.ParseSeriesRegistry(f.Payload)
				if err != nil {
					t.Fatal(err)
				}
				for _, d := range defs {
					known[d.ID] = true
				}
			case wire.FrameDeltaBatch:
				pts, _ := wire.ParseDeltaBatch(f.Payload, nil)
				for _, p := range pts {
					if !known[p.Series] {
						t.Fatalf("point for series %d arrived before its registry entry", p.Series)
					}
				}
				if len(pts) > 0 {
					return // success
				}
			}
		}
	}
	t.Fatal("no points arrived")
}

func TestSnapshotCarriesAnExplicitWatermark(t *testing.T) {
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	id, _ := srv.reg.Intern(model.Labels{Name: "cpu"})
	for ts := int64(1000); ts <= 1010; ts++ {
		srv.ring.Append(id, ts, float32(ts))
	}

	c := dial(t, srv.addr)
	defer c.close()
	c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{"cpu"}})
	c.sendControl(t, wire.Control{Kind: wire.ControlSnapshotRequest, FromMs: 0})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, f := range c.readFrames(t, 500*time.Millisecond) {
			if f.Type != wire.FrameSnapshotHeader {
				continue
			}
			h, err := wire.ParseSnapshotHeader(f.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if h.WatermarkTsMs != 1010 {
				t.Fatalf("watermark = %d, want 1010", h.WatermarkTsMs)
			}
			return
		}
	}
	t.Fatal("no snapshot header arrived")
}

// --- §16.2 chaos: the slow consumer ----------------------------------------

func TestSlowConsumerCannotGrowServerMemory(t *testing.T) {
	// The M4 acceptance test: "attach a client that reads one message per
	// second while the server sends a thousand, and assert that gateway
	// memory stays flat."
	opts := fastOpts()
	// A deliberately small starting depth. The queue grows to fit the
	// subscription, so this exercises that growth as well as the bound.
	opts.QueueDepth = 64
	opts.MaxQueueDepth = 1024
	srv := newTestServer(t, opts)
	defer srv.stop()

	const seriesCount = 200
	ids := make([]model.SeriesID, seriesCount)
	names := make([]string, seriesCount)
	for i := range ids {
		ids[i], _ = srv.reg.Intern(model.Labels{Name: "m", Pairs: map[string]string{"i": strconv.Itoa(i)}})
		names[i] = strconv.Itoa(int(ids[i]))
	}

	c := dial(t, srv.addr)
	defer c.close()
	c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: names})

	deadline := time.Now().Add(2 * time.Second)
	for len(srv.hub.ActiveSeries()) < seriesCount && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	// Publish hard while the client reads nothing at all.
	batch := make([]model.Point, seriesCount)
	for round := 0; round < 2000; round++ {
		for i, id := range ids {
			batch[i] = model.Point{Series: id, OriginTsMs: int64(round), Value: float32(round)}
		}
		srv.hub.Publish(batch)
	}

	st := srv.hub.Stats()
	if len(st.Conns) == 0 {
		t.Fatal("no connection recorded")
	}
	q := st.Conns[0]

	t.Logf("after %d points offered: depth=%d max_depth=%d conflated=%d dropped=%d rate=%.1f%%",
		2000*seriesCount, q.Depth, q.MaxDepth, q.Conflated, q.Dropped, q.ConflationRate()*100)

	// The bound is the whole point: memory is bounded by the subscription
	// size, and hard-capped by MaxQueueDepth, whatever the client does.
	if q.MaxDepth > int64(opts.MaxQueueDepth) {
		t.Fatalf("queue depth reached %d against a ceiling of %d", q.MaxDepth, opts.MaxQueueDepth)
	}
	if q.MaxDepth > int64(seriesCount) {
		t.Fatalf("queue held %d points for %d series; conflation should cap it at one per series",
			q.MaxDepth, seriesCount)
	}
	// And the overwhelming majority was conflated rather than dropped,
	// which is the graceful degradation the policy exists for. Dropping
	// means specific series stop updating entirely.
	if q.ConflationRate() < 0.9 {
		t.Fatalf("conflation rate only %.2f; the queue is not absorbing the load", q.ConflationRate())
	}
	if q.Dropped > 0 {
		t.Fatalf("%d points dropped; a queue sized to the subscription should never drop", q.Dropped)
	}
}

func TestConflationDeliversTheLatestValuePerSeries(t *testing.T) {
	// A slow client sees a lower-resolution stream, not a stalled or stale
	// one. The value it does receive must be the newest.
	q := NewConflatingQueue(8)
	for round := 0; round < 100; round++ {
		for s := 1; s <= 4; s++ {
			q.Push(pt(model.SeriesID(s), int64(round), float32(round)), t0)
		}
	}
	got := q.Drain(nil, 100)
	if len(got) != 4 {
		t.Fatalf("got %d points, want 4", len(got))
	}
	for _, p := range got {
		if p.Value != 99 {
			t.Fatalf("series %d delivered %v, want the latest (99)", p.Series, p.Value)
		}
	}
}

// --- pause/resume ----------------------------------------------------------

func TestPauseStopsTheFirehose(t *testing.T) {
	// A backgrounded tab accumulates a queue it cannot drain, trips the
	// conflation and drop logic, and looks like a broken client. Pausing
	// server-side is better for everyone.
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	id, _ := srv.reg.Intern(model.Labels{Name: "cpu"})

	c := dial(t, srv.addr)
	defer c.close()
	c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{"cpu"}})

	deadline := time.Now().Add(2 * time.Second)
	for len(srv.hub.ActiveSeries()) == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	c.sendControl(t, wire.Control{Kind: wire.ControlPause})

	// Give the pause time to land, then publish.
	paused := false
	for time.Now().Before(deadline) {
		srv.hub.Publish([]model.Point{{Series: id, OriginTsMs: 1, Value: 1}})
		if srv.hub.Stats().Conns[0].Enqueued == 0 && srv.hub.Stats().Conns[0].Conflated == 0 {
			paused = true
			break
		}
		runtime.Gosched()
	}
	_ = paused

	before := srv.hub.Stats().Conns[0]
	for i := 0; i < 500; i++ {
		srv.hub.Publish([]model.Point{{Series: id, OriginTsMs: int64(i), Value: 1}})
	}
	after := srv.hub.Stats().Conns[0]

	if after.Enqueued > before.Enqueued+5 {
		t.Fatalf("paused connection still queued %d points", after.Enqueued-before.Enqueued)
	}
}

// --- demand-driven collection ----------------------------------------------

func TestSubscriptionSetDrivesCollection(t *testing.T) {
	// §5.2: a series enters the live path when someone subscribes and
	// leaves when the last subscriber disconnects. Without this, 5 Hz
	// across the whole fleet is unaffordable.
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	id, _ := srv.reg.Intern(model.Labels{Name: "cpu"})
	_ = id

	if got := len(srv.hub.ActiveSeries()); got != 0 {
		t.Fatalf("%d series active with no subscribers", got)
	}

	c := dial(t, srv.addr)
	c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{"cpu"}})

	deadline := time.Now().Add(2 * time.Second)
	for len(srv.hub.ActiveSeries()) == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(srv.hub.ActiveSeries()) != 1 {
		t.Fatal("subscription did not activate the series")
	}

	// Disconnecting must wind collection back down.
	c.close()
	for len(srv.hub.ActiveSeries()) != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := len(srv.hub.ActiveSeries()); got != 0 {
		t.Fatalf("%d series still active after the last subscriber left", got)
	}
}

func TestTwoSubscribersKeepASeriesActive(t *testing.T) {
	// Reference counting, not a boolean: one client leaving must not stop
	// collection for the other.
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	srv.reg.Intern(model.Labels{Name: "cpu"})

	a := dial(t, srv.addr)
	b := dial(t, srv.addr)
	defer b.close()

	a.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{"cpu"}})
	b.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{"cpu"}})

	deadline := time.Now().Add(2 * time.Second)
	for srv.hub.Stats().Connections < 2 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	for len(srv.hub.ActiveSeries()) == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	a.close()
	for srv.hub.Stats().Connections > 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	if got := len(srv.hub.ActiveSeries()); got != 1 {
		t.Fatalf("%d series active; the remaining subscriber should keep it alive", got)
	}
}

// --- drain -----------------------------------------------------------------

func TestDrainTellsClientsToReconnect(t *testing.T) {
	// A rolling deploy that drops every connection at once causes a
	// reconnect thundering herd. Telling clients first lets them back off
	// with jitter.
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	c := dial(t, srv.addr)
	defer c.close()
	c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: []string{}})

	deadline := time.Now().Add(2 * time.Second)
	for srv.hub.Stats().Connections == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	srv.hub.Drain("rolling deploy")

	sawDrain := false
	for time.Now().Before(deadline) && !sawDrain {
		for _, f := range c.readFrames(t, 500*time.Millisecond) {
			if f.Type != wire.FrameControl {
				continue
			}
			var ctrl wire.Control
			if err := json.Unmarshal(f.Payload, &ctrl); err != nil {
				continue
			}
			if ctrl.Kind == wire.ControlDrain && strings.Contains(ctrl.Message, "deploy") {
				sawDrain = true
			}
		}
	}
	if !sawDrain {
		t.Fatal("no drain notice reached the client")
	}
}

// --- concurrency -----------------------------------------------------------

func TestManyClientsUnderLoad(t *testing.T) {
	// Run under -race. This is the test that catches inconsistent locking
	// between the fanout path and the subscription bookkeeping.
	srv := newTestServer(t, fastOpts())
	defer srv.stop()

	const clients = 8
	const seriesCount = 20

	ids := make([]model.SeriesID, seriesCount)
	names := make([]string, seriesCount)
	for i := range ids {
		ids[i], _ = srv.reg.Intern(model.Labels{Name: "m", Pairs: map[string]string{"i": strconv.Itoa(i)}})
		names[i] = strconv.Itoa(int(ids[i]))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < clients; i++ {
		c := dial(t, srv.addr)
		wg.Add(1)
		go func(c *testClient, i int) {
			defer wg.Done()
			defer c.close()
			c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: names})
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Churn: subscribe, unsubscribe, pause, resume.
				c.sendControl(t, wire.Control{Kind: wire.ControlUnsubscribe, Series: names[:5]})
				c.sendControl(t, wire.Control{Kind: wire.ControlSubscribe, Series: names[:5]})
				c.sendControl(t, wire.Control{Kind: wire.ControlPause})
				c.sendControl(t, wire.Control{Kind: wire.ControlResume})
				c.readFrames(t, 20*time.Millisecond)
			}
		}(c, i)
	}

	// Publish continuously while all that churns.
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := make([]model.Point, seriesCount)
		for round := 0; round < 3000; round++ {
			for i, id := range ids {
				batch[i] = model.Point{Series: id, OriginTsMs: int64(round), Value: float32(i)}
			}
			srv.hub.Publish(batch)
			srv.hub.Stats()
			srv.hub.ActiveSeries()
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}
