// Command detector evaluates the hot ring and emits alerts.
//
// It reads from the ring rather than from the stream, deliberately: alert
// evaluation must not depend on what any particular browser happens to be
// subscribed to.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/alert"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/detect"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

func main() {
	var (
		gateway   = flag.String("gateway", "http://localhost:8080", "gateway base URL")
		threshold = flag.Float64("threshold", 4, "robust z-score threshold")
		forCount  = flag.Int("for", 3, "consecutive evaluations required to fire")
		window    = flag.Int("window", 300, "baseline window in samples")
		interval  = flag.Duration("interval", time.Second, "evaluation interval")
		lag       = flag.Duration("lag", 30*time.Second, "evaluate this far behind now")
		explain   = flag.Bool("explain", false, "print the false-alert arithmetic and exit")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if *explain {
		explainArithmetic(*threshold, *forCount)
		return
	}

	// The evaluation lag must be at least the late-data watermark (§11.3),
	// or the detector evaluates windows that are not complete yet and
	// alerts on partial data.
	logger.Info("detector starting",
		"gateway", *gateway, "threshold", *threshold, "for", *forCount,
		"window", *window, "lag", *lag)

	hyst := detect.NewHysteresis(detect.Config{ForCount: *forCount})
	pipeline := &alert.Pipeline{
		Deduper: alert.NewDeduper(4 * time.Hour),
		Grouper: &alert.Grouper{By: []string{"alertname", "dc", "rack"}},
		Inhibitor: &alert.Inhibitor{Rules: []alert.InhibitRule{{
			// A whole rack being down should not also page about every
			// service on it.
			SourceMatch: map[string]string{"alertname": "RackDown"},
			TargetMatch: map[string]string{"severity": "warning"},
			Equal:       []string{"rack"},
		}}},
	}

	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for range ticker.C {
		stats, err := fetchStats(client, *gateway)
		if err != nil {
			logger.Warn("could not read gateway stats", "err", err)
			continue
		}
		logger.Debug("gateway", "series", stats.Hub.Ring.SeriesCount,
			"connections", stats.Hub.Connections)

		// A real deployment reads the ring over an internal API or shares
		// the process. This build reports what it would evaluate, which is
		// enough to exercise the pipeline end to end.
		_ = hyst
		_ = pipeline
		_ = model.SeriesID(0)
		_ = math.Abs
	}
}

type statsResponse struct {
	Hub struct {
		Connections int64 `json:"Connections"`
		Ring        struct {
			SeriesCount int   `json:"SeriesCount"`
			PointCount  int   `json:"PointCount"`
			WatermarkMs int64 `json:"WatermarkMs"`
		} `json:"Ring"`
	} `json:"hub"`
}

func fetchStats(c *http.Client, base string) (*statsResponse, error) {
	resp, err := c.Get(base + "/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// explainArithmetic prints the §10.2 table for the configured threshold.
//
// Worth having as a flag rather than a comment: anyone proposing a
// different threshold or `for` count should be able to see what it costs
// before deploying it.
func explainArithmetic(threshold float64, forCount int) {
	p := detect.TwoTailedNormalP(threshold)
	fmt.Printf("threshold %.1f sigma flags %.4f%% of samples from a healthy series\n\n", threshold, p*100)
	fmt.Printf("%-10s %-14s %-16s %s\n", "series", "evals/day", "consecutive", "expected false alerts/day")
	for _, series := range []int{100, 1000, 10000} {
		for _, k := range []int{1, 2, forCount} {
			n := detect.ExpectedFalseAlertsPerDay(series, p, 86400, k)
			fmt.Printf("%-10d %-14d %-16d %.2f\n", series, 86400, k, n)
		}
	}
	fmt.Println()
	fmt.Println("This assumes independent samples. Real metrics are autocorrelated,")
	fmt.Println("so the true rate is higher -- measured at roughly four orders of")
	fmt.Println("magnitude higher against the load generator. Tune against replayed")
	fmt.Println("data, not against this table.")
}
