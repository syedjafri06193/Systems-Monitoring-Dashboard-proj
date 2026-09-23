package hub

import (
	"errors"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/ring"
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/wire"
)

// The snapshot/stream boundary.
//
// A chart is part history and part live stream, and joining them without
// gaps or duplicates is the subtlest correctness problem in the system.
// Both obvious orderings are wrong:
//
//   - Query then subscribe leaves a GAP: everything between the query's cut
//     and the subscription's start is lost, and it looks like a quiet
//     period rather than a bug.
//   - Subscribe then query DUPLICATES: points appear twice, and rate
//     calculations double-count.
//
// The correct procedure is subscribe -> buffer -> snapshot-with-watermark
// -> reconcile, and the load-bearing detail is that the watermark is read
// *before* the scan.
//
// This file implements it once. Four hand-written variants for initial
// load, reconnect, series addition and range widening would be four
// different off-by-one bugs, so all four route through Snapshot.

// ErrInvalidRange is returned when from is after to.
var ErrInvalidRange = errors.New("hub: snapshot range is inverted")

// Snapshot is history up to an explicit cut point.
type Snapshot struct {
	Header wire.SnapshotHeader
	Points []model.Point
}

// Source is what a snapshot reads from.
//
// An interface so the hot ring and the TSDB are interchangeable: the tier
// router (§8.2) picks one by time range, and the protocol does not care
// which answered.
type Source interface {
	CommittedWatermark() int64
	Range(ids []model.SeriesID, from, to int64) []model.Point
}

// Compile-time check that the hot ring satisfies it.
var _ Source = (*ring.Ring)(nil)

// TakeSnapshot reads history for ids within [from, to].
//
// The watermark is read BEFORE the range scan, and that ordering is the
// whole correctness argument:
//
//   - Read it first, and any point written during the scan has a timestamp
//     strictly above the watermark. It is therefore not in the snapshot,
//     and the client will not discard it from the live buffer — so it
//     arrives exactly once, live.
//   - Read it after, and a point written mid-scan may have been missed by
//     the scan while still landing at or below the watermark. The client
//     discards it from the buffer too, and it is in neither path. A silent
//     hole.
func TakeSnapshot(src Source, ids []model.SeriesID, from, to int64) (*Snapshot, error) {
	if from > to {
		return nil, ErrInvalidRange
	}

	watermark := src.CommittedWatermark()

	// Clamp the requested end to the watermark. A client asking for data up
	// to "now" is asking for a future the server cannot promise; answering
	// with everything up to the watermark and saying so is honest, and the
	// rest arrives on the live stream.
	if to > watermark {
		to = watermark
	}

	var points []model.Point
	if from <= to {
		points = src.Range(ids, from, to)
	}

	// SeriesCount is the number of series actually represented, not the
	// number requested. A client can tell "you have no data for 3 of my 10
	// series" from "your request was wrong", which are different problems.
	seen := make(map[model.SeriesID]struct{}, len(ids))
	for _, p := range points {
		seen[p.Series] = struct{}{}
	}

	return &Snapshot{
		Header: wire.SnapshotHeader{
			WatermarkTsMs: watermark,
			SeriesCount:   uint32(len(seen)),
		},
		Points: points,
	}, nil
}

// Reconcile merges buffered live points into a snapshot.
//
// This is the client half, implemented server-side too because it is the
// part worth testing exhaustively and because the same logic serves the
// gateway's own replay path.
//
// The rule is a single comparison: discard any buffered point at or below
// the watermark, because the snapshot already contains it. Note the
// inclusive boundary — a point exactly *at* the watermark is in the
// snapshot, so keeping it would duplicate.
func Reconcile(snap *Snapshot, buffered []model.Point) []model.Point {
	out := make([]model.Point, 0, len(snap.Points)+len(buffered))
	out = append(out, snap.Points...)
	for _, p := range buffered {
		if p.OriginTsMs > snap.Header.WatermarkTsMs {
			out = append(out, p)
		}
	}
	return out
}

// SubscriptionRequest is one client's interest.
type SubscriptionRequest struct {
	SeriesIDs []model.SeriesID
	FromMs    int64
	ToMs      int64
}

// Reason identifies which of the four cases produced a snapshot.
//
// Carried only for metrics and logging — every reason takes the identical
// code path, which is the point.
type Reason uint8

const (
	ReasonInitialLoad Reason = iota
	ReasonReconnect
	ReasonAddSeries
	ReasonWidenRange
)

func (r Reason) String() string {
	switch r {
	case ReasonInitialLoad:
		return "initial_load"
	case ReasonReconnect:
		return "reconnect"
	case ReasonAddSeries:
		return "add_series"
	case ReasonWidenRange:
		return "widen_range"
	default:
		return "unknown"
	}
}

// SnapshotHeaderAt is a test and replay helper: a header with only a
// watermark set.
func SnapshotHeaderAt(watermarkMs int64) wire.SnapshotHeader {
	return wire.SnapshotHeader{WatermarkTsMs: watermarkMs}
}
