// Command gateway is the WebSocket stream server.
//
// It owns the hot ring, the subscription set and the per-connection
// conflating queues. Ingest feeds it; browsers subscribe to it; the
// detector reads the same ring.
//
// Run it with the built-in load generator to see the whole path without
// deploying anything:
//
//	go run ./cmd/gateway -loadgen -series 1000 -hz 5
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/cardinality"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/hub"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/loadgen"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ring"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/store"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/wire"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ws"
)

func main() {
	var (
		addr       = flag.String("addr", ":8080", "listen address")
		useLoadgen = flag.Bool("loadgen", false, "run the synthetic load generator")
		series     = flag.Int("series", 1000, "loadgen series count")
		hz         = flag.Float64("hz", 5, "loadgen collection frequency")
		webDir     = flag.String("web", "web/dist", "directory of static assets to serve")
		verbose    = flag.Bool("v", false, "debug logging")
		origins    = flag.String("allowed-origins", "", "comma-separated extra origins allowed to open /stream (same-origin is always allowed; \"*\" allows any)")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	// The ring holds 15 minutes at the collection frequency. Sized from
	// the tier definition rather than hard-coded, so changing the hot
	// retention changes one number.
	capacity := int(store.HotRetention.Seconds() * *hz)
	if capacity < 64 {
		capacity = 64
	}
	r := ring.New(capacity)
	registry := wire.NewRegistry()
	limiter := cardinality.New(cardinality.DefaultLimits())

	opts := hub.DefaultOptions()
	opts.Logger = logger
	h := hub.New(r, registry, opts)

	var allowedOrigins []string
	for _, o := range strings.Split(*origins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowedOrigins = append(allowedOrigins, o)
		}
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/stream", func(w http.ResponseWriter, req *http.Request) {
		c, err := ws.UpgradeWithOrigins(w, req, allowedOrigins)
		if err != nil {
			logger.Warn("upgrade failed", "err", err, "remote", req.RemoteAddr)
			status := http.StatusBadRequest
			if errors.Is(err, ws.ErrBadOrigin) {
				status = http.StatusForbidden
			}
			http.Error(w, err.Error(), status)
			return
		}
		logger.Debug("client connected", "remote", c.RemoteAddr())
		h.Serve(c)
		logger.Debug("client disconnected", "remote", c.RemoteAddr())
	})

	// Meta-monitoring (§11.1): monitor the monitor. Ingest rate,
	// connection count, queue depths and conflation rate are the first
	// things anyone wants during a "why is my chart choppy" investigation.
	mux.HandleFunc("/stats", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"hub":         h.Stats(),
			"cardinality": limiter.Stats(),
			"registry":    registry.Len(),
		})
	})

	// The dead-man's switch (§11.1). An alert rule that fires constantly,
	// wired to an external service that pages when it *stops* arriving.
	// This is the only way to detect that the alerting pipeline itself is
	// dead, because a silent alerting system looks exactly like a healthy
	// fleet.
	mux.HandleFunc("/watchdog", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"alert": "Watchdog",
			"state": "firing",
			"note":  "this always fires; page when it stops arriving",
			"ts":    time.Now().UnixMilli(),
		})
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	if *webDir != "" {
		if _, err := os.Stat(*webDir); err == nil {
			mux.Handle("/", http.FileServer(http.Dir(*webDir)))
			logger.Info("serving static assets", "dir", *webDir)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if *useLoadgen {
		go runLoadgen(ctx, h, r, registry, limiter, *series, *hz, logger)
	}

	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
		// No ReadTimeout: a WebSocket connection is long-lived by design
		// and a read timeout would kill every one of them. Idleness is
		// handled by the application-level heartbeat instead, which is
		// also what keeps proxies from closing the connection.
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("gateway listening", "addr", *addr, "ring_capacity", capacity)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listen failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()

	// Graceful drain (§11.4): tell clients to reconnect, with jitter,
	// before the socket disappears. A rolling deploy that drops every
	// connection simultaneously causes a reconnect thundering herd.
	logger.Info("draining connections")
	h.Drain("gateway shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	logger.Info("gateway stopped")
}

// runLoadgen feeds the gateway from the synthetic generator.
func runLoadgen(
	ctx context.Context,
	h *hub.Hub,
	r *ring.Ring,
	registry *wire.Registry,
	limiter *cardinality.Limiter,
	series int,
	hz float64,
	logger *slog.Logger,
) {
	cfg := loadgen.DefaultConfig()
	cfg.Series = series
	cfg.Interval = time.Duration(float64(time.Second) / hz)
	cfg.StartMs = time.Now().UnixMilli()

	g := loadgen.New(cfg)

	// Intern every series up front so ids are stable and the registry is
	// complete before any point references one.
	idOf := make(map[model.SeriesID]model.SeriesID, series)
	for _, spec := range g.Specs() {
		if err := limiter.Check(spec.Labels); err != nil {
			logger.Warn("loadgen series rejected", "err", err)
			continue
		}
		id, _ := registry.Intern(spec.Labels)
		idOf[spec.ID] = id
	}
	logger.Info("loadgen started", "series", registry.Len(), "interval", cfg.Interval)

	// Inject anomalies with known ground truth, so the detector has
	// something real to find.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				a := g.InjectRandom(time.Now().UnixMilli() + 1000)
				logger.Info("injected anomaly", "series", a.Series, "kind", a.Kind,
					"magnitude", a.Magnitude)
			}
		}
	}()

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	batch := make([]model.Point, 0, series)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			batch = batch[:0]
			now := time.Now().UnixMilli()
			for _, p := range g.Tick() {
				id, ok := idOf[p.Series]
				if !ok {
					continue
				}
				// origin_ts is stamped at observation and ingest_ts here,
				// which is what makes the end-to-end measurement possible
				// and keeps ordering monotonic despite agent clock skew.
				p.Series = id
				p.OriginTsMs = now
				p.IngestTsMs = now
				batch = append(batch, p)
				r.Append(id, p.OriginTsMs, p.Value)
			}
			h.Publish(batch)
		}
	}
}
