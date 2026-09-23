package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/wire"
)

type fixture struct {
	Name  string `json:"name"`
	Bytes []int  `json:"bytes"`
	Note  string `json:"note"`
}

func b(x []byte) []int {
	out := make([]int, len(x))
	for i, v := range x {
		out[i] = int(v)
	}
	return out
}

func main() {
	var fixtures []fixture

	reg, _ := wire.AppendSeriesRegistry(nil, []wire.SeriesDef{
		{ID: 1, Labels: model.Labels{Name: "node_cpu_percent", Pairs: map[string]string{"host": "a", "dc": "eu"}}},
		{ID: 7, Labels: model.Labels{Name: "up"}},
	})
	fixtures = append(fixtures, fixture{"series_registry", b(reg), "2 series, one with labels one without"})

	hdr, _ := wire.AppendSnapshotHeader(nil, wire.SnapshotHeader{WatermarkTsMs: 1700000123456, SeriesCount: 3})
	fixtures = append(fixtures, fixture{"snapshot_header", b(hdr), "watermark 1700000123456, 3 series"})

	base := int64(1700000000000)
	batch, _ := wire.AppendDeltaBatch(nil, []model.Point{
		{Series: 1, OriginTsMs: base, Value: 1.5},
		{Series: 2, OriginTsMs: base + 200, Value: -3.25},
		{Series: 1, OriginTsMs: base + 100000, Value: 0},
	})
	fixtures = append(fixtures, fixture{"delta_batch", b(batch), "3 points, one with a multi-byte varint delta"})

	hb, _ := wire.AppendHeartbeat(nil, 1700000000000)
	fixtures = append(fixtures, fixture{"heartbeat", b(hb), "server ts 1700000000000"})

	// Several frames coalesced into one message.
	var multi []byte
	multi = append(multi, reg...)
	multi = append(multi, hdr...)
	multi = append(multi, batch...)
	fixtures = append(fixtures, fixture{"coalesced", b(multi), "registry + snapshot header + delta batch in one message"})

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(fixtures); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
