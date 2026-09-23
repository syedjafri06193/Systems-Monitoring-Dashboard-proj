package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// The frame format.
//
//	Frame header (5 bytes)
//	  u8   type
//	  u32  payload length
//
// A length-prefixed header rather than a self-describing format, because
// the decoder runs in a Web Worker on every message at up to 5 Hz per
// series. Anything requiring a parse to find the next frame boundary costs
// more than the bytes it saves.
type FrameType uint8

const (
	FrameSeriesRegistry FrameType = 0x01
	FrameSnapshotHeader FrameType = 0x02
	FrameDeltaBatch     FrameType = 0x03
	FrameHeartbeat      FrameType = 0x04
	FrameControl        FrameType = 0x05
	FrameSnapshotData   FrameType = 0x06
)

func (t FrameType) String() string {
	switch t {
	case FrameSeriesRegistry:
		return "SERIES_REGISTRY"
	case FrameSnapshotHeader:
		return "SNAPSHOT_HEADER"
	case FrameDeltaBatch:
		return "DELTA_BATCH"
	case FrameHeartbeat:
		return "HEARTBEAT"
	case FrameControl:
		return "CONTROL"
	case FrameSnapshotData:
		return "SNAPSHOT_DATA"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02x)", uint8(t))
	}
}

const HeaderLen = 5

// MaxPayloadLen bounds what a single frame may claim.
//
// Without a cap, a corrupt or hostile 4-byte length prefix of 0xFFFFFFFF
// makes the reader allocate 4 GB before it discovers the frame is 20 bytes
// long. This is the standard length-prefix denial of service and it costs
// one comparison to prevent.
const MaxPayloadLen = 16 << 20 // 16 MiB

var (
	ErrShortFrame    = errors.New("wire: frame shorter than its header")
	ErrPayloadTooBig = errors.New("wire: frame payload exceeds the maximum")
	ErrTruncated     = errors.New("wire: frame payload truncated")
)

// AppendFrame writes a header and payload to dst.
//
// Appends to a caller-owned buffer rather than allocating, because this runs
// once per batch per connection and a per-frame allocation shows up directly
// in the gateway's GC pressure — which is a latency term (§3.4).
func AppendFrame(dst []byte, t FrameType, payload []byte) ([]byte, error) {
	if len(payload) > MaxPayloadLen {
		return nil, ErrPayloadTooBig
	}
	dst = append(dst, byte(t))
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(payload)))
	return append(dst, payload...), nil
}

// Frame is one decoded frame, with payload aliasing the input buffer.
type Frame struct {
	Type    FrameType
	Payload []byte
}

// ReadFrame decodes one frame from b and returns the remainder.
//
// Payload aliases b rather than copying. Callers that retain a payload past
// the life of the read buffer must copy it themselves; the alternative is a
// copy on every frame, which at thousands of frames per second is the exact
// allocation the format exists to avoid.
func ReadFrame(b []byte) (Frame, []byte, error) {
	if len(b) < HeaderLen {
		return Frame{}, nil, ErrShortFrame
	}
	t := FrameType(b[0])
	n := binary.BigEndian.Uint32(b[1:5])
	if n > MaxPayloadLen {
		return Frame{}, nil, ErrPayloadTooBig
	}
	// The addition cannot overflow: n is bounded above by MaxPayloadLen.
	if len(b) < HeaderLen+int(n) {
		return Frame{}, nil, ErrTruncated
	}
	return Frame{Type: t, Payload: b[HeaderLen : HeaderLen+n]}, b[HeaderLen+int(n):], nil
}

// --- SERIES_REGISTRY -------------------------------------------------------

// SeriesDef maps an interned id to its label set.
type SeriesDef struct {
	ID     model.SeriesID
	Labels model.Labels
}

// AppendSeriesRegistry encodes id -> labels mappings.
//
//	u32  count
//	[ u32 series_id, u16 label_bytes, <name \x1f k=v \x1f k=v ...> ] x count
//
// The label blob reuses model.Labels.Key()'s canonical form, so the same
// label set always produces the same bytes. That makes the registry frame
// deterministic and therefore diffable and cacheable.
func AppendSeriesRegistry(dst []byte, defs []SeriesDef) ([]byte, error) {
	payload := make([]byte, 0, 64*len(defs))
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(defs)))
	for _, d := range defs {
		key := d.Labels.Key()
		if len(key) > 0xFFFF {
			return nil, fmt.Errorf("wire: label set for series %d is %d bytes, over the 64KiB frame limit", d.ID, len(key))
		}
		payload = binary.BigEndian.AppendUint32(payload, uint32(d.ID))
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(key)))
		payload = append(payload, key...)
	}
	return AppendFrame(dst, FrameSeriesRegistry, payload)
}

func ParseSeriesRegistry(payload []byte) ([]SeriesDef, error) {
	if len(payload) < 4 {
		return nil, ErrTruncated
	}
	count := binary.BigEndian.Uint32(payload[:4])
	// Each entry is at least 6 bytes, so a count claiming more entries than
	// the payload could possibly hold is corrupt. Checking before
	// allocating stops a small hostile frame from reserving a huge slice.
	if uint64(count)*6 > uint64(len(payload)) {
		return nil, ErrTruncated
	}
	p := payload[4:]

	defs := make([]SeriesDef, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(p) < 6 {
			return nil, ErrTruncated
		}
		id := model.SeriesID(binary.BigEndian.Uint32(p[:4]))
		n := int(binary.BigEndian.Uint16(p[4:6]))
		p = p[6:]
		if len(p) < n {
			return nil, ErrTruncated
		}
		labels, err := parseLabelKey(string(p[:n]))
		if err != nil {
			return nil, err
		}
		p = p[n:]
		defs = append(defs, SeriesDef{ID: id, Labels: labels})
	}
	return defs, nil
}

func parseLabelKey(key string) (model.Labels, error) {
	parts := splitByte(key, 0x1f)
	l := model.Labels{Name: parts[0]}
	if len(parts) > 1 {
		l.Pairs = make(map[string]string, len(parts)-1)
		for _, kv := range parts[1:] {
			eq := -1
			for i := 0; i < len(kv); i++ {
				if kv[i] == '=' {
					eq = i
					break
				}
			}
			if eq < 0 {
				return model.Labels{}, fmt.Errorf("wire: malformed label pair %q", kv)
			}
			l.Pairs[kv[:eq]] = kv[eq+1:]
		}
	}
	return l, nil
}

func splitByte(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// --- SNAPSHOT_HEADER -------------------------------------------------------

// SnapshotHeader carries the cut point between history and live data.
//
// Every point in the accompanying snapshot has ts <= WatermarkTsMs. The
// client discards any buffered live point at or below it.
//
// The watermark is explicit rather than inferred. "Use the timestamp of the
// last snapshot point" is wrong whenever a series has no recent data: a
// series idle for ten minutes would place the cut ten minutes back, and
// every live point since would be re-applied as a duplicate.
type SnapshotHeader struct {
	WatermarkTsMs int64
	SeriesCount   uint32
}

func AppendSnapshotHeader(dst []byte, h SnapshotHeader) ([]byte, error) {
	var payload [12]byte
	binary.BigEndian.PutUint64(payload[0:8], uint64(h.WatermarkTsMs))
	binary.BigEndian.PutUint32(payload[8:12], h.SeriesCount)
	return AppendFrame(dst, FrameSnapshotHeader, payload[:])
}

func ParseSnapshotHeader(payload []byte) (SnapshotHeader, error) {
	if len(payload) < 12 {
		return SnapshotHeader{}, ErrTruncated
	}
	return SnapshotHeader{
		WatermarkTsMs: int64(binary.BigEndian.Uint64(payload[0:8])),
		SeriesCount:   binary.BigEndian.Uint32(payload[8:12]),
	}, nil
}

// --- DELTA_BATCH -----------------------------------------------------------

// AppendDeltaBatch encodes a batch of points.
//
//	u64  base_ts_ms
//	u32  count
//	[ u32 series_id, varint ts_delta_ms, f32 value ] x count
//
// Timestamps are deltas from a per-batch base, which is what keeps them to
// one or two bytes. Values are f32: infrastructure metrics are percentages,
// rates and counts, and f32 halves the payload against f64.
func AppendDeltaBatch(dst []byte, points []model.Point) ([]byte, error) {
	if len(points) == 0 {
		return AppendFrame(dst, FrameDeltaBatch, nil)
	}

	// The base is the minimum rather than the first point's timestamp, so
	// every delta is non-negative and the varint stays unsigned. Using the
	// first point instead would make an out-of-order batch produce negative
	// deltas that a uvarint silently encodes as enormous positive ones.
	base := points[0].OriginTsMs
	for _, p := range points[1:] {
		if p.OriginTsMs < base {
			base = p.OriginTsMs
		}
	}

	payload := make([]byte, 0, 12+len(points)*10)
	payload = binary.BigEndian.AppendUint64(payload, uint64(base))
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(points)))

	var scratch [binary.MaxVarintLen64]byte
	for _, p := range points {
		payload = binary.BigEndian.AppendUint32(payload, uint32(p.Series))
		n := binary.PutUvarint(scratch[:], uint64(p.OriginTsMs-base))
		payload = append(payload, scratch[:n]...)
		payload = binary.BigEndian.AppendUint32(payload, float32bits(p.Value))
	}
	return AppendFrame(dst, FrameDeltaBatch, payload)
}

// ParseDeltaBatch decodes into dst, which may be reused across calls.
func ParseDeltaBatch(payload []byte, dst []model.Point) ([]model.Point, error) {
	dst = dst[:0]
	if len(payload) == 0 {
		return dst, nil
	}
	if len(payload) < 12 {
		return nil, ErrTruncated
	}
	base := int64(binary.BigEndian.Uint64(payload[:8]))
	count := binary.BigEndian.Uint32(payload[8:12])
	// Minimum 9 bytes per point (4 id + 1 varint + 4 value).
	if uint64(count)*9 > uint64(len(payload)) {
		return nil, ErrTruncated
	}
	p := payload[12:]

	for i := uint32(0); i < count; i++ {
		if len(p) < 4 {
			return nil, ErrTruncated
		}
		id := model.SeriesID(binary.BigEndian.Uint32(p[:4]))
		p = p[4:]

		delta, n := binary.Uvarint(p)
		if n <= 0 {
			return nil, ErrTruncated
		}
		p = p[n:]

		if len(p) < 4 {
			return nil, ErrTruncated
		}
		v := float32frombits(binary.BigEndian.Uint32(p[:4]))
		p = p[4:]

		dst = append(dst, model.Point{
			Series:     id,
			OriginTsMs: base + int64(delta),
			Value:      v,
		})
	}
	return dst, nil
}

// --- HEARTBEAT -------------------------------------------------------------

// AppendHeartbeat carries the server's clock.
//
// The timestamp is not decoration. It lets the client estimate clock offset
// (§3.5 measures latency across two clocks) and detect a stalled stream: no
// data frames *and* no heartbeat means the connection is dead even though
// TCP has not noticed.
func AppendHeartbeat(dst []byte, serverTsMs int64) ([]byte, error) {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], uint64(serverTsMs))
	return AppendFrame(dst, FrameHeartbeat, payload[:])
}

func ParseHeartbeat(payload []byte) (int64, error) {
	if len(payload) < 8 {
		return 0, ErrTruncated
	}
	return int64(binary.BigEndian.Uint64(payload[:8])), nil
}

// --- CONTROL ---------------------------------------------------------------

type ControlKind uint8

const (
	ControlSubscribe ControlKind = iota + 1
	ControlUnsubscribe
	ControlSnapshotRequest
	ControlPause
	ControlResume
	ControlConflationNotice
	ControlError
	ControlDrain
)

// Control is the low-rate bidirectional channel.
//
// JSON-encoded, deliberately, unlike everything else here. Control frames
// are rare (a subscription change, a pause, an error) and their schema
// changes far more often than the data path's. Paying a few hundred bytes
// for a self-describing format on a frame that arrives once a minute buys
// forward and backward compatibility where it actually matters.
type Control struct {
	Kind ControlKind `json:"kind"`
	// Series names for subscribe/unsubscribe.
	Series []string `json:"series,omitempty"`
	FromMs int64    `json:"from_ms,omitempty"`
	ToMs   int64    `json:"to_ms,omitempty"`
	// LastSeenMs drives the reconnect backfill (§6.5).
	LastSeenMs int64 `json:"last_seen_ms,omitempty"`
	// Conflated and Dropped report what the connection lost, so a client
	// can tell a choppy chart from a broken one.
	Conflated uint64 `json:"conflated,omitempty"`
	Dropped   uint64 `json:"dropped,omitempty"`
	Message   string `json:"message,omitempty"`
}

func float32bits(f float32) uint32 { return math.Float32bits(f) }

func float32frombits(u uint32) float32 { return math.Float32frombits(u) }
