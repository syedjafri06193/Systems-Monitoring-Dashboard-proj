package hub

import (
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ring"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/wire"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ws"
)

// Options tune the gateway's connection handling.
type Options struct {
	// QueueDepth is the initial per-connection bound. It grows to fit the
	// subscription (see ConflatingQueue.SetCapacity) up to MaxQueueDepth.
	QueueDepth int
	// MaxQueueDepth is the absolute ceiling, against a client that
	// subscribes to an unreasonable number of series. Memory per
	// connection never exceeds MaxQueueDepth x sizeof(Point).
	MaxQueueDepth int
	// FlushInterval batches sends. A direct latency term, so it is small:
	// 20 ms adds at most 20 ms and cuts the frame count by an order of
	// magnitude at 5 Hz across many series.
	FlushInterval time.Duration
	// HeartbeatInterval must sit well inside the shortest proxy idle
	// timeout in the path. Many load balancers default to 60 s.
	HeartbeatInterval time.Duration
	// StuckTimeout closes a connection whose queue has been full this
	// long. A client that is not reading is not watching.
	StuckTimeout time.Duration
	// WriteTimeout bounds a single write. The queue bounds memory; this
	// bounds time, because a client that stopped reading blocks the
	// writer indefinitely once the kernel buffer fills.
	WriteTimeout time.Duration
	Logger       *slog.Logger
}

func DefaultOptions() Options {
	return Options{
		QueueDepth:        1024,
		MaxQueueDepth:     65536,
		FlushInterval:     20 * time.Millisecond,
		HeartbeatInterval: 15 * time.Second,
		StuckTimeout:      30 * time.Second,
		WriteTimeout:      10 * time.Second,
		Logger:            slog.Default(),
	}
}

// Hub fans points out to subscribed connections.
type Hub struct {
	opts     Options
	ring     *ring.Ring
	registry *wire.Registry

	mu    sync.RWMutex
	conns map[*Conn]struct{}

	// subscribed counts subscribers per series, which is what makes
	// collection demand-driven: a series with no subscribers falls back to
	// the 15 s path, and 5 Hz is paid only for what someone is watching.
	subscribed map[model.SeriesID]int

	connected    atomic.Int64
	pointsFanned atomic.Uint64
}

func New(r *ring.Ring, reg *wire.Registry, opts Options) *Hub {
	if opts.QueueDepth < 1 {
		opts = DefaultOptions()
	}
	if opts.MaxQueueDepth < opts.QueueDepth {
		opts.MaxQueueDepth = DefaultOptions().MaxQueueDepth
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Hub{
		opts:       opts,
		ring:       r,
		registry:   reg,
		conns:      make(map[*Conn]struct{}),
		subscribed: make(map[model.SeriesID]int),
	}
}

// Publish fans a batch out to every interested connection.
//
// Non-blocking by construction: each connection's queue either accepts,
// conflates, or drops. A slow client can never slow down ingest, which is
// the whole reason the queue is per-connection rather than shared.
func (h *Hub) Publish(points []model.Point) {
	now := time.Now()

	h.mu.RLock()
	conns := make([]*Conn, 0, len(h.conns))
	for c := range h.conns {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		for _, p := range points {
			if c.Subscribed(p.Series) {
				c.queue.Push(p, now)
			}
		}
	}
	h.pointsFanned.Add(uint64(len(points)))
}

// ActiveSeries lists series with at least one subscriber.
//
// Published back toward the collectors: this is the demand-driven
// collection set from §5.2.
func (h *Hub) ActiveSeries() []model.SeriesID {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]model.SeriesID, 0, len(h.subscribed))
	for id, n := range h.subscribed {
		if n > 0 {
			out = append(out, id)
		}
	}
	return out
}

func (h *Hub) addConn(c *Conn) {
	h.mu.Lock()
	h.conns[c] = struct{}{}
	h.mu.Unlock()
	h.connected.Add(1)
}

// Lock ordering throughout: h.mu before c.mu, never the reverse.
//
// The connection's subscription set is guarded by c.mu alone -- the fanout
// path reads it on every point and must not contend on the hub-wide lock --
// while the hub's per-series subscriber counts are guarded by h.mu. Both are
// touched together here, so a single consistent order is what keeps this
// deadlock-free.
func (h *Hub) removeConn(c *Conn) {
	h.mu.Lock()
	delete(h.conns, c)

	c.mu.Lock()
	// Release this connection's interest so collection can wind back down.
	for id := range c.subs {
		h.subscribed[id]--
		if h.subscribed[id] <= 0 {
			delete(h.subscribed, id)
		}
	}
	c.mu.Unlock()

	h.mu.Unlock()
	h.connected.Add(-1)
}

func (h *Hub) subscribe(c *Conn, ids []model.SeriesID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range ids {
		if _, already := c.subs[id]; already {
			continue
		}
		c.subs[id] = struct{}{}
		h.subscribed[id]++
	}
}

func (h *Hub) unsubscribe(c *Conn, ids []model.SeriesID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range ids {
		if _, had := c.subs[id]; !had {
			continue
		}
		delete(c.subs, id)
		h.subscribed[id]--
		if h.subscribed[id] <= 0 {
			delete(h.subscribed, id)
		}
	}
}

// Stats for meta-monitoring (§11.1): monitor the monitor.
type Stats struct {
	Connections  int64
	ActiveSeries int
	PointsFanned uint64
	Ring         ring.Stats
	Conns        []QueueStats
}

func (h *Hub) Stats() Stats {
	h.mu.RLock()
	qs := make([]QueueStats, 0, len(h.conns))
	for c := range h.conns {
		qs = append(qs, c.queue.Stats())
	}
	active := len(h.subscribed)
	h.mu.RUnlock()

	return Stats{
		Connections:  h.connected.Load(),
		ActiveSeries: active,
		PointsFanned: h.pointsFanned.Load(),
		Ring:         h.ring.Stats(),
		Conns:        qs,
	}
}

// Drain tells every client to reconnect, then closes.
//
// A rolling deploy that simply drops every connection causes a reconnect
// thundering herd. Telling clients to go first lets them back off with
// jitter (§6.5) before the socket disappears.
func (h *Hub) Drain(reason string) {
	h.mu.RLock()
	conns := make([]*Conn, 0, len(h.conns))
	for c := range h.conns {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		c.sendControl(wire.Control{Kind: wire.ControlDrain, Message: reason})
		_ = c.ws.WriteClose(ws.CloseDraining, reason)
		c.Close()
	}
}

// Conn is one subscribed WebSocket client.
type Conn struct {
	hub   *Hub
	ws    *ws.Conn
	queue *ConflatingQueue

	mu   sync.RWMutex
	subs map[model.SeriesID]struct{}
	// paused is set when the tab is hidden. A backgrounded tab accumulates
	// a queue it cannot drain, trips the conflation and drop logic, and
	// looks like a broken client; pausing server-side is better for
	// everyone.
	paused atomic.Bool
	// sentUpTo is the highest series id whose definition this connection
	// has received, so registry updates are a range rather than a diff.
	//
	// Atomic because sendRegistry runs from both the read loop (when a
	// subscription arrives) and the write loop (before every batch, so a
	// point never references an id the client cannot name).
	sentUpTo atomic.Uint32

	closeOnce sync.Once
	done      chan struct{}
}

// Serve runs one connection until it closes.
func (h *Hub) Serve(c *ws.Conn) {
	conn := &Conn{
		hub:   h,
		ws:    c,
		queue: NewConflatingQueue(h.opts.QueueDepth),
		subs:  make(map[model.SeriesID]struct{}),
		done:  make(chan struct{}),
	}
	h.addConn(conn)
	defer func() {
		h.removeConn(conn)
		conn.Close()
	}()

	go conn.writeLoop()
	conn.readLoop()
}

func (c *Conn) Subscribed(id model.SeriesID) bool {
	if c.paused.Load() {
		return false
	}
	c.mu.RLock()
	_, ok := c.subs[id]
	c.mu.RUnlock()
	return ok
}

func (c *Conn) readLoop() {
	for {
		msg, err := c.ws.Read()
		if err != nil {
			return
		}
		if msg.Opcode != ws.OpText && msg.Opcode != ws.OpBinary {
			continue
		}

		var ctrl wire.Control
		if err := json.Unmarshal(msg.Data, &ctrl); err != nil {
			c.hub.opts.Logger.Warn("bad control frame", "err", err)
			continue
		}
		c.handleControl(ctrl)
	}
}

func (c *Conn) handleControl(ctrl wire.Control) {
	switch ctrl.Kind {
	case wire.ControlSubscribe:
		ids := c.resolve(ctrl.Series)
		c.hub.subscribe(c, ids)
		// Grow the send queue to fit the subscription, up to the
		// configured ceiling. A queue smaller than the subscription drops
		// the same series forever rather than degrading everyone's
		// resolution -- see ConflatingQueue.SetCapacity.
		c.mu.RLock()
		n := len(c.subs)
		c.mu.RUnlock()
		if n > c.hub.opts.MaxQueueDepth {
			n = c.hub.opts.MaxQueueDepth
		}
		c.queue.SetCapacity(n)
		c.sendRegistry()

	case wire.ControlUnsubscribe:
		c.hub.unsubscribe(c, c.resolve(ctrl.Series))

	case wire.ControlSnapshotRequest:
		c.sendSnapshot(ctrl)

	case wire.ControlPause:
		c.paused.Store(true)

	case wire.ControlResume:
		c.paused.Store(false)
		// Resuming is a reconnect in miniature, and it goes through the
		// same §7 path: the client reports what it last saw and gets the
		// gap, rather than a fresh subscription that would leave a hole.
		if ctrl.LastSeenMs > 0 {
			c.sendSnapshot(wire.Control{
				Series: ctrl.Series,
				FromMs: ctrl.LastSeenMs + 1,
				ToMs:   0,
			})
		}
	}
}

func (c *Conn) resolve(names []string) []model.SeriesID {
	out := make([]model.SeriesID, 0, len(names))
	for _, n := range names {
		if id, ok := c.hub.registry.ID(model.Labels{Name: n}); ok {
			out = append(out, id)
			continue
		}
		// Also accept a raw numeric id, which is what the client uses once
		// it has the registry.
		if id, ok := parseID(n); ok {
			out = append(out, id)
		}
	}
	return out
}

func parseID(s string) (model.SeriesID, bool) {
	var n uint64
	if len(s) == 0 {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + uint64(s[i]-'0')
		if n > 1<<32 {
			return 0, false
		}
	}
	return model.SeriesID(n), true
}

func (c *Conn) sendRegistry() {
	max := c.hub.registry.MaxID()
	sent := model.SeriesID(c.sentUpTo.Load())
	if max <= sent {
		return
	}
	defs := c.hub.registry.DefsSince(sent)
	if len(defs) == 0 {
		return
	}
	buf, err := wire.AppendSeriesRegistry(nil, defs)
	if err != nil {
		return
	}
	if err := c.writeFrame(buf); err != nil {
		return
	}
	// Advance monotonically. Two goroutines can both send a registry
	// update; the later one must not lower the watermark and cause the
	// earlier definitions to be resent forever.
	for {
		cur := c.sentUpTo.Load()
		if uint32(max) <= cur || c.sentUpTo.CompareAndSwap(cur, uint32(max)) {
			break
		}
	}
}

// sendSnapshot runs the §7 handshake for every one of its four cases.
func (c *Conn) sendSnapshot(ctrl wire.Control) {
	c.mu.RLock()
	ids := make([]model.SeriesID, 0, len(c.subs))
	for id := range c.subs {
		ids = append(ids, id)
	}
	c.mu.RUnlock()

	to := ctrl.ToMs
	if to == 0 {
		to = 1<<62 - 1 // clamped to the watermark inside TakeSnapshot
	}

	snap, err := TakeSnapshot(c.hub.ring, ids, ctrl.FromMs, to)
	if err != nil {
		c.sendControl(wire.Control{Kind: wire.ControlError, Message: err.Error()})
		return
	}

	// Header first, carrying the explicit watermark. The client needs the
	// cut point before the data so it can start discarding immediately.
	buf, err := wire.AppendSnapshotHeader(nil, snap.Header)
	if err != nil {
		return
	}
	buf, err = wire.AppendDeltaBatch(buf, snap.Points)
	if err != nil {
		return
	}
	_ = c.writeFrame(buf)
}

func (c *Conn) sendControl(ctrl wire.Control) {
	payload, err := json.Marshal(ctrl)
	if err != nil {
		return
	}
	buf, err := wire.AppendFrame(nil, wire.FrameControl, payload)
	if err != nil {
		return
	}
	_ = c.writeFrame(buf)
}

func (c *Conn) writeFrame(b []byte) error {
	if c.hub.opts.WriteTimeout > 0 {
		_ = c.ws.SetWriteDeadline(time.Now().Add(c.hub.opts.WriteTimeout))
	}
	return c.ws.WriteBinary(b)
}

func (c *Conn) writeLoop() {
	flush := time.NewTicker(c.hub.opts.FlushInterval)
	defer flush.Stop()
	heartbeat := time.NewTicker(c.hub.opts.HeartbeatInterval)
	defer heartbeat.Stop()

	batch := make([]model.Point, 0, 512)
	var buf []byte

	for {
		select {
		case <-c.done:
			return
		case <-c.ws.Done():
			return

		case <-heartbeat.C:
			buf, _ = wire.AppendHeartbeat(buf[:0], time.Now().UnixMilli())
			if err := c.writeFrame(buf); err != nil {
				c.Close()
				return
			}

		case now := <-flush.C:
			// Close a client that has been unable to keep up for long
			// enough that it cannot be watching.
			if d := c.queue.StuckFor(now); d > c.hub.opts.StuckTimeout {
				st := c.queue.Stats()
				c.hub.opts.Logger.Warn("closing stuck connection",
					"stuck_for", d, "dropped", st.Dropped, "conflated", st.Conflated)
				_ = c.ws.WriteClose(ws.CloseSlowConsumer, "send queue full for "+d.String())
				c.Close()
				return
			}

			batch = c.queue.Drain(batch, 2048)
			if len(batch) == 0 {
				continue
			}

			// Any series interned since the last send must be named before
			// a point references it.
			c.sendRegistry()

			var err error
			buf, err = wire.AppendDeltaBatch(buf[:0], batch)
			if err != nil {
				continue
			}
			if err := c.writeFrame(buf); err != nil {
				c.Close()
				return
			}
		}
	}
}

func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.ws.Close()
	})
}

// QueueStats exposes this connection's backpressure state.
func (c *Conn) QueueStats() QueueStats { return c.queue.Stats() }
