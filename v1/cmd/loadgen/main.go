// Command loadgen prints synthetic metrics, for inspecting the generator.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/loadgen"
)

func main() {
	var (
		series  = flag.Int("series", 10, "series count")
		hz      = flag.Float64("hz", 5, "collection frequency")
		ticks   = flag.Int("ticks", 20, "how many intervals to emit")
		asJSON  = flag.Bool("json", false, "emit JSON instead of a summary")
		anomaly = flag.Bool("anomaly", false, "inject an anomaly partway through")
	)
	flag.Parse()

	cfg := loadgen.DefaultConfig()
	cfg.Series = *series
	cfg.Interval = time.Duration(float64(time.Second) / *hz)

	g := loadgen.New(cfg)
	if *anomaly {
		a := g.InjectRandom(g.NowMs() + int64(*ticks/2)*cfg.Interval.Milliseconds())
		fmt.Fprintf(os.Stderr, "injected %s on series %d, magnitude %.1f sigma\n",
			a.Kind, a.Series, a.Magnitude)
	}

	enc := json.NewEncoder(os.Stdout)
	total := 0
	for i := 0; i < *ticks; i++ {
		pts := g.Tick()
		total += len(pts)
		if *asJSON {
			for _, p := range pts {
				_ = enc.Encode(p)
			}
		}
	}

	if !*asJSON {
		fmt.Printf("%d series at %g Hz: %d points over %d intervals\n",
			*series, *hz, total, *ticks)
		for _, s := range g.Specs()[:min(5, len(g.Specs()))] {
			fmt.Printf("  %s base=%.0f daily=%.0f sigma=%.1f rho=%.2f\n",
				s.Labels.Key(), s.Base, s.DailyAmplitude, s.NoiseSigma, s.Autocorrelation)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
