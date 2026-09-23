package wire

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

func labels(name string, kv ...string) model.Labels {
	l := model.Labels{Name: name}
	if len(kv) > 0 {
		l.Pairs = map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			l.Pairs[kv[i]] = kv[i+1]
		}
	}
	return l
}

// --- framing ---------------------------------------------------------------

func TestFrameRoundTrip(t *testing.T) {
	buf, err := AppendFrame(nil, FrameHeartbeat, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	f, rest, err := ReadFrame(buf)
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != FrameHeartbeat || string(f.Payload) != "hello" {
		t.Fatalf("got %v %q", f.Type, f.Payload)
	}
	if len(rest) != 0 {
		t.Fatalf("expected no remainder, got %d bytes", len(rest))
	}
}

func TestMultipleFramesInOneBuffer(t *testing.T) {
	// Several frames coalesce into one WebSocket message routinely; the
	// reader must walk them rather than assuming one frame per message.
	var buf []byte
	buf, _ = AppendFrame(buf, FrameHeartbeat, []byte("a"))
	buf, _ = AppendFrame(buf, FrameControl, []byte("bb"))
	buf, _ = AppendFrame(buf, FrameDeltaBatch, []byte("ccc"))

	var types []FrameType
	rest := buf
	for len(rest) > 0 {
		var f Frame
		var err error
		f, rest, err = ReadFrame(rest)
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, f.Type)
	}
	if len(types) != 3 || types[0] != FrameHeartbeat || types[2] != FrameDeltaBatch {
		t.Fatalf("got %v", types)
	}
}

func TestEmptyPayloadIsValid(t *testing.T) {
	buf, err := AppendFrame(nil, FrameControl, nil)
	if err != nil {
		t.Fatal(err)
	}
	f, _, err := ReadFrame(buf)
	if err != nil || f.Type != FrameControl || len(f.Payload) != 0 {
		t.Fatalf("got %v %v %v", f.Type, f.Payload, err)
	}
}

func TestRejectsAbsurdLengthPrefixWithoutAllocating(t *testing.T) {
	// The classic length-prefix denial of service: a 5-byte frame claiming
	// a 4 GB payload. Rejecting on the claim rather than on the read is
	// what stops the allocation.
	hdr := make([]byte, 5)
	hdr[0] = byte(FrameDeltaBatch)
	binary.BigEndian.PutUint32(hdr[1:], 0xFFFFFFFF)

	if _, _, err := ReadFrame(hdr); err != ErrPayloadTooBig {
		t.Fatalf("expected ErrPayloadTooBig, got %v", err)
	}
}

func TestTruncatedFrameErrors(t *testing.T) {
	buf, _ := AppendFrame(nil, FrameHeartbeat, []byte("12345678"))
	for cut := 0; cut < len(buf); cut++ {
		if _, _, err := ReadFrame(buf[:cut]); err == nil {
			t.Fatalf("%d-byte prefix decoded without error", cut)
		}
	}
}

func TestUnknownFrameTypeIsReadable(t *testing.T) {
	// Forward compatibility: a client must be able to skip a frame type it
	// does not know, which requires the header to parse independently of
	// the type.
	buf, _ := AppendFrame(nil, FrameType(0x7F), []byte("future"))
	f, rest, err := ReadFrame(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || string(f.Payload) != "future" {
		t.Fatalf("got %q rest=%d", f.Payload, len(rest))
	}
	if f.Type.String() != "UNKNOWN(0x7f)" {
		t.Fatalf("String() = %q", f.Type.String())
	}
}

// --- series registry frame -------------------------------------------------

func TestSeriesRegistryRoundTrip(t *testing.T) {
	defs := []SeriesDef{
		{ID: 1, Labels: labels("node_cpu_seconds_total", "instance", "node-42", "cpu", "7", "mode", "user")},
		{ID: 2, Labels: labels("up")},
		{ID: 99, Labels: labels("http_requests_total", "code", "200")},
	}
	buf, err := AppendSeriesRegistry(nil, defs)
	if err != nil {
		t.Fatal(err)
	}
	f, _, err := ReadFrame(buf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseSeriesRegistry(f.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d defs", len(got))
	}
	for i := range defs {
		if got[i].ID != defs[i].ID {
			t.Fatalf("id[%d]: %d != %d", i, got[i].ID, defs[i].ID)
		}
		if got[i].Labels.Key() != defs[i].Labels.Key() {
			t.Fatalf("labels[%d]: %q != %q", i, got[i].Labels.Key(), defs[i].Labels.Key())
		}
	}
}

func TestSeriesRegistryEmpty(t *testing.T) {
	buf, _ := AppendSeriesRegistry(nil, nil)
	f, _, _ := ReadFrame(buf)
	got, err := ParseSeriesRegistry(f.Payload)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestSeriesRegistryRejectsLyingCount(t *testing.T) {
	// A count of 100000 in an 8-byte payload must not make the parser
	// allocate a 100000-element slice before discovering the truth.
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload[:4], 100000)
	if _, err := ParseSeriesRegistry(payload); err != ErrTruncated {
		t.Fatalf("expected ErrTruncated, got %v", err)
	}
}

func TestSeriesRegistryTruncated(t *testing.T) {
	defs := []SeriesDef{{ID: 1, Labels: labels("x", "a", "b")}}
	buf, _ := AppendSeriesRegistry(nil, defs)
	f, _, _ := ReadFrame(buf)
	for cut := 0; cut < len(f.Payload); cut++ {
		if _, err := ParseSeriesRegistry(f.Payload[:cut]); err == nil {
			t.Fatalf("%d-byte payload parsed without error", cut)
		}
	}
}

// --- delta batch -----------------------------------------------------------

func TestDeltaBatchRoundTrip(t *testing.T) {
	base := int64(1_700_000_000_000)
	pts := []model.Point{
		{Series: 1, OriginTsMs: base, Value: 1.5},
		{Series: 2, OriginTsMs: base + 200, Value: -3.25},
		{Series: 1, OriginTsMs: base + 400, Value: 99999.5},
	}
	buf, err := AppendDeltaBatch(nil, pts)
	if err != nil {
		t.Fatal(err)
	}
	f, _, _ := ReadFrame(buf)
	got, err := ParseDeltaBatch(f.Payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(pts) {
		t.Fatalf("got %d points", len(got))
	}
	for i := range pts {
		if got[i].Series != pts[i].Series || got[i].OriginTsMs != pts[i].OriginTsMs || got[i].Value != pts[i].Value {
			t.Fatalf("point %d: got %+v want %+v", i, got[i], pts[i])
		}
	}
}

func TestDeltaBatchHandlesOutOfOrderPoints(t *testing.T) {
	// The base is the minimum, not the first point's timestamp. Taking the
	// first would make an out-of-order batch produce a negative delta that
	// a uvarint silently encodes as an enormous positive one — the
	// timestamps would come back thousands of years in the future.
	base := int64(1_700_000_000_000)
	pts := []model.Point{
		{Series: 1, OriginTsMs: base + 1000, Value: 1},
		{Series: 2, OriginTsMs: base, Value: 2}, // earlier than the first
		{Series: 3, OriginTsMs: base + 500, Value: 3},
	}
	buf, _ := AppendDeltaBatch(nil, pts)
	f, _, _ := ReadFrame(buf)
	got, err := ParseDeltaBatch(f.Payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range pts {
		if got[i].OriginTsMs != pts[i].OriginTsMs {
			t.Fatalf("point %d: ts %d want %d", i, got[i].OriginTsMs, pts[i].OriginTsMs)
		}
	}
}

func TestDeltaBatchEmpty(t *testing.T) {
	buf, _ := AppendDeltaBatch(nil, nil)
	f, _, _ := ReadFrame(buf)
	got, err := ParseDeltaBatch(f.Payload, nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestDeltaBatchReusesDestination(t *testing.T) {
	// The decoder runs per message in a worker; allocating a fresh slice
	// per batch is exactly the GC pressure the binary format exists to
	// avoid.
	pts := []model.Point{{Series: 1, OriginTsMs: 1000, Value: 1}}
	buf, _ := AppendDeltaBatch(nil, pts)
	f, _, _ := ReadFrame(buf)

	dst := make([]model.Point, 0, 16)
	got1, _ := ParseDeltaBatch(f.Payload, dst)
	got2, _ := ParseDeltaBatch(f.Payload, got1)
	if len(got2) != 1 {
		t.Fatalf("reuse produced %d points", len(got2))
	}
	if cap(got2) != cap(dst) {
		t.Fatalf("reuse reallocated: cap %d -> %d", cap(dst), cap(got2))
	}
}

func TestDeltaBatchPreservesFloat32Exactly(t *testing.T) {
	vals := []float32{0, -0, 1, -1, math.MaxFloat32, math.SmallestNonzeroFloat32,
		float32(math.Inf(1)), float32(math.NaN()), 0.1, -123456.75}
	pts := make([]model.Point, len(vals))
	for i, v := range vals {
		pts[i] = model.Point{Series: model.SeriesID(i + 1), OriginTsMs: int64(i) * 200, Value: v}
	}
	buf, _ := AppendDeltaBatch(nil, pts)
	f, _, _ := ReadFrame(buf)
	got, err := ParseDeltaBatch(f.Payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range vals {
		if math.Float32bits(got[i].Value) != math.Float32bits(vals[i]) {
			t.Fatalf("value %d: bits %x want %x", i, math.Float32bits(got[i].Value), math.Float32bits(vals[i]))
		}
	}
}

func TestDeltaBatchRejectsLyingCount(t *testing.T) {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[8:12], 1_000_000)
	if _, err := ParseDeltaBatch(payload, nil); err != ErrTruncated {
		t.Fatalf("expected ErrTruncated, got %v", err)
	}
}

func TestDeltaBatchTruncated(t *testing.T) {
	pts := []model.Point{
		{Series: 1, OriginTsMs: 1000, Value: 1},
		{Series: 2, OriginTsMs: 1200, Value: 2},
	}
	buf, _ := AppendDeltaBatch(nil, pts)
	f, _, _ := ReadFrame(buf)
	for cut := 0; cut < len(f.Payload); cut++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on %d-byte payload: %v", cut, r)
				}
			}()
			_, _ = ParseDeltaBatch(f.Payload[:cut], nil)
		}()
	}
}

func TestDeltaBatchBytesPerPoint(t *testing.T) {
	// The document claims ~9 B/point for this format against 150-400 for
	// JSON with full labels. The claim holds, but it is a property of the
	// *batch window*, not of the format: the timestamp is a uvarint delta
	// from the batch base, so it costs one byte only while the batch spans
	// under 128 ms. That is exactly what the 50 ms agent flush deadline
	// (§5.3) produces, so the figure is right for the system as designed —
	// but a batch accumulated over a longer window pays 2-3 bytes per
	// timestamp instead. Both are measured here.
	base := int64(1_700_000_000_000)

	measure := func(n int, spacingMs int64) float64 {
		pts := make([]model.Point, n)
		for i := range pts {
			pts[i] = model.Point{
				Series:     model.SeriesID(1 + i%50),
				OriginTsMs: base + int64(i)*spacingMs,
				Value:      float32(i) * 0.5,
			}
		}
		buf, _ := AppendDeltaBatch(nil, pts)
		return float64(len(buf)) / float64(n)
	}

	// 50 ms flush: 250 series sampled within one window, near-zero spread.
	flush := measure(250, 0)
	// A batch spanning 100 s, which the design never produces but which
	// shows where the varint widens.
	wide := measure(500, 200)

	t.Logf("delta batch: %.2f B/pt (50 ms flush window), %.2f B/pt (100 s span)", flush, wide)

	if flush > 9.5 {
		t.Fatalf("expected ~9 B/pt within a flush window, got %.2f", flush)
	}
	if wide > 12 {
		t.Fatalf("wide batch unexpectedly large: %.2f", wide)
	}
}

// --- snapshot header -------------------------------------------------------

func TestSnapshotHeaderRoundTrip(t *testing.T) {
	h := SnapshotHeader{WatermarkTsMs: 1_700_000_123_456, SeriesCount: 17}
	buf, _ := AppendSnapshotHeader(nil, h)
	f, _, _ := ReadFrame(buf)
	got, err := ParseSnapshotHeader(f.Payload)
	if err != nil || got != h {
		t.Fatalf("got %+v want %+v (%v)", got, h, err)
	}
}

func TestSnapshotHeaderHandlesNegativeWatermark(t *testing.T) {
	// Not realistic as wall-clock, but the sign must survive the uint64
	// round-trip or a test harness using small timestamps sees nonsense.
	h := SnapshotHeader{WatermarkTsMs: -5000, SeriesCount: 1}
	buf, _ := AppendSnapshotHeader(nil, h)
	f, _, _ := ReadFrame(buf)
	got, _ := ParseSnapshotHeader(f.Payload)
	if got.WatermarkTsMs != -5000 {
		t.Fatalf("got %d", got.WatermarkTsMs)
	}
}

func TestSnapshotHeaderTruncated(t *testing.T) {
	if _, err := ParseSnapshotHeader(make([]byte, 11)); err != ErrTruncated {
		t.Fatalf("expected ErrTruncated, got %v", err)
	}
}

// --- heartbeat -------------------------------------------------------------

func TestHeartbeatRoundTrip(t *testing.T) {
	buf, _ := AppendHeartbeat(nil, 1_700_000_000_000)
	f, _, _ := ReadFrame(buf)
	ts, err := ParseHeartbeat(f.Payload)
	if err != nil || ts != 1_700_000_000_000 {
		t.Fatalf("got %d %v", ts, err)
	}
}

// --- registry --------------------------------------------------------------

func TestRegistryInterns(t *testing.T) {
	r := NewRegistry()
	l := labels("cpu", "host", "a")

	id1, isNew1 := r.Intern(l)
	id2, isNew2 := r.Intern(l)

	if !isNew1 {
		t.Fatal("first intern should report new")
	}
	if isNew2 {
		t.Fatal("second intern should not report new")
	}
	if id1 != id2 {
		t.Fatalf("same labels got different ids: %d and %d", id1, id2)
	}
}

func TestRegistryNeverAssignsZero(t *testing.T) {
	// Zero means "no series". If it were assignable, a zero-valued Point
	// would silently refer to a real series instead of being detectably
	// invalid.
	r := NewRegistry()
	id, _ := r.Intern(labels("first"))
	if id == 0 {
		t.Fatal("registry assigned id 0")
	}
	if _, ok := r.Lookup(0); ok {
		t.Fatal("id 0 resolved to a series")
	}
}

func TestRegistryIsOrderIndependent(t *testing.T) {
	// Go randomises map iteration order per run. A label key built from
	// unsorted iteration produces a different key on every process start,
	// silently multiplying the series count — and only under restart, so
	// a single-process test would never catch it.
	r := NewRegistry()
	a := model.Labels{Name: "m", Pairs: map[string]string{"z": "1", "a": "2", "m": "3"}}
	b := model.Labels{Name: "m", Pairs: map[string]string{"m": "3", "a": "2", "z": "1"}}

	idA, _ := r.Intern(a)
	idB, isNew := r.Intern(b)
	if idA != idB || isNew {
		t.Fatalf("same label set interned twice: %d vs %d (new=%v)", idA, idB, isNew)
	}
	if r.Len() != 1 {
		t.Fatalf("registry holds %d series, want 1", r.Len())
	}
}

func TestRegistryDistinguishesLabelSets(t *testing.T) {
	r := NewRegistry()
	id1, _ := r.Intern(labels("m", "host", "a"))
	id2, _ := r.Intern(labels("m", "host", "b"))
	id3, _ := r.Intern(labels("m"))
	if id1 == id2 || id2 == id3 || id1 == id3 {
		t.Fatalf("distinct label sets collided: %d %d %d", id1, id2, id3)
	}
	if r.Len() != 3 {
		t.Fatalf("len = %d", r.Len())
	}
}

func TestRegistryLookupRoundTrips(t *testing.T) {
	r := NewRegistry()
	l := labels("m", "host", "a", "dc", "eu")
	id, _ := r.Intern(l)

	got, ok := r.Lookup(id)
	if !ok {
		t.Fatal("lookup failed")
	}
	if got.Key() != l.Key() {
		t.Fatalf("got %q want %q", got.Key(), l.Key())
	}
}

func TestRegistryLookupRejectsOutOfRange(t *testing.T) {
	r := NewRegistry()
	r.Intern(labels("m"))
	if _, ok := r.Lookup(999); ok {
		t.Fatal("out-of-range id resolved")
	}
}

func TestRegistryClonesLabels(t *testing.T) {
	// A caller reusing its label map across metrics would otherwise mutate
	// what the registry stored, and every interned series would end up
	// with the last caller's labels.
	r := NewRegistry()
	pairs := map[string]string{"host": "a"}
	l := model.Labels{Name: "m", Pairs: pairs}
	id, _ := r.Intern(l)

	pairs["host"] = "MUTATED"

	got, _ := r.Lookup(id)
	if got.Pairs["host"] != "a" {
		t.Fatalf("registry stored an aliased map: host=%q", got.Pairs["host"])
	}
}

func TestRegistryTakeDirty(t *testing.T) {
	r := NewRegistry()
	r.Intern(labels("a"))
	r.Intern(labels("b"))

	first := r.TakeDirty()
	if len(first) != 2 {
		t.Fatalf("got %d dirty", len(first))
	}
	if second := r.TakeDirty(); second != nil {
		t.Fatalf("dirty list not cleared: %v", second)
	}

	r.Intern(labels("c"))
	if third := r.TakeDirty(); len(third) != 1 {
		t.Fatalf("got %d dirty after a new intern", len(third))
	}
}

func TestRegistryDefsSince(t *testing.T) {
	// What a reconnecting client needs: it reports the highest id it knows
	// and gets exactly the gap, because ids are monotonic.
	r := NewRegistry()
	for i := 0; i < 5; i++ {
		r.Intern(labels("m", "i", string(rune('a'+i))))
	}

	if got := r.DefsSince(0); len(got) != 5 {
		t.Fatalf("since 0: got %d", len(got))
	}
	if got := r.DefsSince(3); len(got) != 2 {
		t.Fatalf("since 3: got %d", len(got))
	}
	if got := r.DefsSince(5); got != nil {
		t.Fatalf("since 5: got %v", got)
	}
	if got := r.DefsSince(999); got != nil {
		t.Fatalf("since a future id: got %v", got)
	}
}

func TestRegistryConcurrentInternAssignsOneID(t *testing.T) {
	// Two goroutines can both miss the read-locked lookup for the same new
	// series. Without the re-check under the write lock they assign two
	// ids to one label set, silently doubling the series count.
	r := NewRegistry()
	const goroutines = 64

	var wg sync.WaitGroup
	ids := make([]model.SeriesID, goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i], _ = r.Intern(labels("contended", "x", "1"))
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 1; i < goroutines; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("goroutine %d got id %d, goroutine 0 got %d", i, ids[i], ids[0])
		}
	}
	if r.Len() != 1 {
		t.Fatalf("registry holds %d series after concurrent interning, want 1", r.Len())
	}
}

func TestRegistryConcurrentMixedAccess(t *testing.T) {
	// Run under -race: interning while looking up and listing must not
	// corrupt the id->labels slice.
	r := NewRegistry()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 500; i++ {
				switch rng.Intn(4) {
				case 0:
					r.Intern(labels("m", "k", string(rune('a'+rng.Intn(20)))))
				case 1:
					r.Lookup(model.SeriesID(rng.Intn(25)))
				case 2:
					r.DefsSince(model.SeriesID(rng.Intn(10)))
				default:
					r.Len()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestClientRegistryRejectsUnknownID(t *testing.T) {
	// The client must never invent an id. A point for an unknown series is
	// a protocol error worth reporting, not a series to name arbitrarily.
	c := NewClientRegistry()
	c.Apply([]SeriesDef{{ID: 7, Labels: labels("m")}})

	if _, ok := c.Lookup(7); !ok {
		t.Fatal("known id not found")
	}
	if _, ok := c.Lookup(8); ok {
		t.Fatal("unknown id resolved")
	}
}

// --- interning win ---------------------------------------------------------

func TestInterningBeatsInlineLabels(t *testing.T) {
	// The reason the registry exists. A realistic label set repeated on
	// every sample is the difference between ~9 B/pt and ~80.
	l := labels("node_cpu_seconds_total", "instance", "node-42.eu-west-1.example.com",
		"cpu", "7", "mode", "user", "job", "node-exporter")
	inline := len(l.Key())

	pts := make([]model.Point, 1000)
	for i := range pts {
		pts[i] = model.Point{Series: 1, OriginTsMs: int64(i) * 200, Value: float32(i)}
	}
	buf, _ := AppendDeltaBatch(nil, pts)
	interned := float64(len(buf)) / float64(len(pts))

	t.Logf("inline labels: %d B/pt, interned: %.1f B/pt (%.0fx)", inline, interned, float64(inline)/interned)
	if float64(inline)/interned < 5 {
		t.Fatalf("interning gained only %.1fx", float64(inline)/interned)
	}
}
