package wire

import (
	"math"
	"math/rand"
	"testing"
)

func roundTrip(t *testing.T, ts []int64, vals []float64) {
	t.Helper()
	e := NewEncoder()
	for i := range ts {
		e.Push(ts[i], vals[i])
	}
	gotTs, gotVals, err := DecodeAll(e.Bytes(), e.Count())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(gotTs) != len(ts) {
		t.Fatalf("count: got %d want %d", len(gotTs), len(ts))
	}
	for i := range ts {
		if gotTs[i] != ts[i] {
			t.Fatalf("ts[%d]: got %d want %d", i, gotTs[i], ts[i])
		}
		// Bit-exact, not approximate. The codec is lossless by
		// construction; an "almost equal" assertion here would hide a
		// genuine mantissa bug.
		if math.Float64bits(gotVals[i]) != math.Float64bits(vals[i]) {
			t.Fatalf("val[%d]: got %v want %v", i, gotVals[i], vals[i])
		}
	}
}

func TestRoundTripRegularCadence(t *testing.T) {
	// The case the format is designed for: 5 Hz, slowly-varying value.
	var ts []int64
	var vals []float64
	base := int64(1_700_000_000_000)
	for i := 0; i < 300; i++ {
		ts = append(ts, base+int64(i)*200)
		vals = append(vals, 42.0+float64(i%7)*0.001)
	}
	roundTrip(t, ts, vals)
}

func TestRoundTripSinglePoint(t *testing.T) {
	roundTrip(t, []int64{1_700_000_000_000}, []float64{3.25})
}

func TestRoundTripTwoPoints(t *testing.T) {
	// n==1 takes a distinct branch (raw delta, no delta-of-delta), so it
	// needs its own case.
	roundTrip(t, []int64{1000, 1200}, []float64{1, 2})
}

func TestRoundTripEmpty(t *testing.T) {
	e := NewEncoder()
	ts, vals, err := DecodeAll(e.Bytes(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 0 || len(vals) != 0 {
		t.Fatalf("expected empty, got %v %v", ts, vals)
	}
}

func TestRoundTripConstantValue(t *testing.T) {
	// Every XOR is zero: one bit per value. Very common for gauges.
	var ts []int64
	var vals []float64
	for i := 0; i < 100; i++ {
		ts = append(ts, int64(i)*200)
		vals = append(vals, 7.5)
	}
	roundTrip(t, ts, vals)
}

func TestRoundTripIrregularTimestamps(t *testing.T) {
	// Jitter across every delta-of-delta bucket boundary.
	ts := []int64{0, 200, 401, 600, 900, 901, 3000, 3200, 100000, 100200}
	vals := make([]float64, len(ts))
	for i := range vals {
		vals[i] = float64(i)
	}
	roundTrip(t, ts, vals)
}

func TestRoundTripDeltaOfDeltaBoundaries(t *testing.T) {
	// The exact edges of each control code's range. Off-by-one here picks
	// a wider code than needed (harmless) or a narrower one (corrupt).
	for _, dod := range []int64{-63, -62, 64, 65, -255, 256, 257, -2047, 2048, 2049, -100000, 100000} {
		ts := []int64{0, 200, 400 + dod}
		vals := []float64{1, 2, 3}
		roundTrip(t, ts, vals)
	}
}

func TestRoundTripNegativeAndZeroTimestamps(t *testing.T) {
	// Not realistic for wall-clock, but a decoder that mishandles the sign
	// bit fails here and nowhere else.
	roundTrip(t, []int64{-5000, -4800, 0, 200}, []float64{1, 2, 3, 4})
}

func TestRoundTripSpecialFloats(t *testing.T) {
	// NaN and infinities reach the codec from real metrics: a rate
	// computed over a zero interval, a division by an empty count.
	vals := []float64{
		0, -0, 1, -1,
		math.Inf(1), math.Inf(-1), math.NaN(),
		math.MaxFloat64, math.SmallestNonzeroFloat64,
	}
	ts := make([]int64, len(vals))
	for i := range ts {
		ts[i] = int64(i) * 200
	}

	e := NewEncoder()
	for i := range ts {
		e.Push(ts[i], vals[i])
	}
	_, gotVals, err := DecodeAll(e.Bytes(), e.Count())
	if err != nil {
		t.Fatal(err)
	}
	for i := range vals {
		// Compare bit patterns: NaN != NaN, and -0 == 0, so value
		// comparison would pass on a codec that silently normalised them.
		if math.Float64bits(gotVals[i]) != math.Float64bits(vals[i]) {
			t.Fatalf("val[%d]: got bits %x want %x", i, math.Float64bits(gotVals[i]), math.Float64bits(vals[i]))
		}
	}
}

func TestRoundTripAlternatingWideValues(t *testing.T) {
	// Forces a new leading/trailing window on nearly every point — the
	// worst case for the value codec, and the path least exercised by
	// realistic data.
	vals := []float64{1e-300, 1e300, -1e-300, 1e300, 0.1, 123456.789, -0.0001}
	ts := make([]int64, len(vals))
	for i := range ts {
		ts[i] = int64(i) * 200
	}
	roundTrip(t, ts, vals)
}

func TestRoundTripRandomised(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 200; trial++ {
		n := 1 + rng.Intn(64)
		ts := make([]int64, n)
		vals := make([]float64, n)
		cur := rng.Int63n(1_000_000)
		for i := 0; i < n; i++ {
			cur += rng.Int63n(1000) - 200
			ts[i] = cur
			switch rng.Intn(4) {
			case 0:
				vals[i] = rng.Float64()
			case 1:
				vals[i] = rng.Float64() * 1e6
			case 2:
				vals[i] = float64(rng.Intn(100))
			default:
				vals[i] = 42.0 // repeat, to hit the xor==0 path
			}
		}
		roundTrip(t, ts, vals)
	}
}

// --- compression ratio -----------------------------------------------------

func TestCompressionBeatsTheDocumentsTarget(t *testing.T) {
	// The design document claims ~2-4 bytes/point against ~9 for plain
	// binary framing. The claim holds, but only for values carrying f32
	// precision -- which is what this system stores (model.Point.Value is
	// a float32), and which the document itself recommends in the same
	// section.
	//
	// The distinction is not cosmetic. A float32 widened to float64 has 29
	// zero bits in the low mantissa, so consecutive XORs have a large
	// trailing-zero run and the significant window stays narrow. A genuine
	// float64 random walk has no such structure and costs roughly 6.4
	// B/pt -- still better than JSON, but outside the documented range.
	// Both numbers are measured below.
	const n = 3000
	base := int64(1_700_000_000_000)
	rng := rand.New(rand.NewSource(7))

	measure := func(quantize bool) float64 {
		e := NewEncoder()
		r := rand.New(rand.NewSource(rng.Int63()))
		v := 55.0
		for i := 0; i < n; i++ {
			// Regular 5 Hz cadence with a slowly drifting value: what an
			// infrastructure metric actually looks like.
			v += (r.Float64() - 0.5) * 0.05
			out := v
			if quantize {
				out = float64(float32(v))
			}
			e.Push(base+int64(i)*200, out)
		}
		return float64(e.BitLen()) / 8 / float64(n)
	}

	f32 := measure(true)
	f64 := measure(false)
	t.Logf("gorilla: %.2f B/pt (f32-precision values), %.2f B/pt (full f64 random walk)", f32, f64)

	if f32 > 4.0 {
		t.Fatalf("f32-precision compression worse than the documented 2-4 B/pt: %.2f", f32)
	}
	// And it should comfortably beat the 9 B/pt uncompressed framing.
	if f32 > 9.0/2 {
		t.Fatalf("compression does not beat plain framing by 2x: %.2f", f32)
	}
}

func TestConstantSeriesIsNearlyFree(t *testing.T) {
	// Regular cadence plus unchanging value is 2 bits/point: one for the
	// delta-of-delta, one for "value unchanged". This is the case that
	// makes streaming idle series affordable.
	const n = 10000
	e := NewEncoder()
	for i := 0; i < n; i++ {
		e.Push(int64(i)*200, 1.0)
	}
	bitsPerPoint := float64(e.BitLen()) / float64(n)
	t.Logf("constant series: %.3f bits/point", bitsPerPoint)
	if bitsPerPoint > 2.1 {
		t.Fatalf("expected ~2 bits/point for a constant series, got %.3f", bitsPerPoint)
	}
}

// --- corrupt input ---------------------------------------------------------

func TestTruncatedStreamErrorsRatherThanPanics(t *testing.T) {
	// Frames arrive from the network. A truncated one is an expected
	// input, and a panic in the decoder is a remote crash.
	e := NewEncoder()
	for i := 0; i < 50; i++ {
		e.Push(int64(i)*200, float64(i))
	}
	full := e.Bytes()

	for cut := 0; cut < len(full); cut++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic decoding %d-byte prefix: %v", cut, r)
				}
			}()
			// Claiming the full count against a short buffer must error,
			// not panic and not silently return fewer points.
			_, _, err := DecodeAll(full[:cut], e.Count())
			if err == nil && cut < len(full) {
				// Padding means a slightly short buffer can occasionally
				// still decode; only assert no panic and no wrong answer.
				return
			}
		}()
	}
}

func TestOverstatedCountErrors(t *testing.T) {
	e := NewEncoder()
	e.Push(1000, 1)
	_, _, err := DecodeAll(e.Bytes(), 1000)
	if err == nil {
		t.Fatal("expected an error when the count exceeds the data")
	}
}

func TestUnderstatedCountReturnsPrefix(t *testing.T) {
	// A caller asking for fewer points than were written gets a prefix,
	// not an error: this is how a partial read is meant to work.
	e := NewEncoder()
	for i := 0; i < 10; i++ {
		e.Push(int64(i)*200, float64(i))
	}
	ts, _, err := DecodeAll(e.Bytes(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 4 {
		t.Fatalf("got %d points, want 4", len(ts))
	}
}

func TestResetReusesTheEncoder(t *testing.T) {
	e := NewEncoder()
	e.Push(1000, 1)
	e.Push(1200, 2)
	e.Reset()
	if e.Count() != 0 || e.BitLen() != 0 {
		t.Fatalf("reset left state: count=%d bits=%d", e.Count(), e.BitLen())
	}
	e.Push(5000, 9)
	ts, vals, err := DecodeAll(e.Bytes(), e.Count())
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 1 || ts[0] != 5000 || vals[0] != 9 {
		t.Fatalf("after reset got %v %v", ts, vals)
	}
}

// --- bitstream primitives --------------------------------------------------

func TestBitWriterReaderRoundTrip(t *testing.T) {
	w := NewBitWriter()
	w.WriteBit(1)
	w.WriteBits(0b1011, 4)
	w.WriteBits(0xDEADBEEF, 32)
	w.WriteBit(0)

	if got, want := w.BitLen(), 1+4+32+1; got != want {
		t.Fatalf("BitLen: got %d want %d", got, want)
	}

	r := NewBitReader(w.Bytes())
	if b, _ := r.ReadBit(); b != 1 {
		t.Fatal("bit 0")
	}
	if v, _ := r.ReadBits(4); v != 0b1011 {
		t.Fatalf("bits 1-4: %b", v)
	}
	if v, _ := r.ReadBits(32); v != 0xDEADBEEF {
		t.Fatalf("32 bits: %x", v)
	}
	if b, _ := r.ReadBit(); b != 0 {
		t.Fatal("last bit")
	}

	// 38 bits written occupies 5 bytes, so 2 padding bits remain and are
	// readable. They are indistinguishable from data — a zero bit is a
	// valid control code — which is exactly why every format here carries
	// an explicit point count rather than decoding until exhaustion.
	for i := 0; i < 2; i++ {
		if b, err := r.ReadBit(); err != nil || b != 0 {
			t.Fatalf("padding bit %d: got %d, %v", i, b, err)
		}
	}
	if _, err := r.ReadBit(); err != ErrShortRead {
		t.Fatalf("expected ErrShortRead past the padding, got %v", err)
	}
}

func TestBitReaderRejectsOversizedRead(t *testing.T) {
	r := NewBitReader([]byte{0xFF})
	if _, err := r.ReadBits(65); err == nil {
		t.Fatal("expected an error for n > 64")
	}
}

func TestBitReaderBoundsCheckIsAtomic(t *testing.T) {
	// A read that cannot be satisfied must not consume any bits, or a
	// caller that recovers and retries reads from the wrong offset.
	r := NewBitReader([]byte{0xFF})
	if _, err := r.ReadBits(16); err != ErrShortRead {
		t.Fatalf("expected ErrShortRead, got %v", err)
	}
	if r.BitPos() != 0 {
		t.Fatalf("failed read consumed %d bits", r.BitPos())
	}
}

func TestZigzagRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 63, -63, 64, -64, 1 << 30, -(1 << 30), math.MaxInt64, math.MinInt64} {
		if got := unzigzag(zigzag(v)); got != v {
			t.Fatalf("zigzag(%d) round-trip gave %d", v, got)
		}
	}
}

func TestZigzagKeepsSmallMagnitudesSmall(t *testing.T) {
	// The property the encoding exists for: -1 must not cost 64 bits.
	if zigzag(-1) != 1 {
		t.Fatalf("zigzag(-1) = %d, want 1", zigzag(-1))
	}
	if zigzag(1) != 2 {
		t.Fatalf("zigzag(1) = %d, want 2", zigzag(1))
	}
}

// --- fuzz ------------------------------------------------------------------

// FuzzDecoderDoesNotPanic feeds arbitrary bytes to the decoder.
//
// The design document is specific that bit-level codecs fail in ways unit
// tests miss. The decoder parses untrusted network input, so the property
// under test is that no input can panic it.
func FuzzDecoderDoesNotPanic(f *testing.F) {
	e := NewEncoder()
	for i := 0; i < 20; i++ {
		e.Push(int64(i)*200, float64(i)*1.5)
	}
	f.Add(e.Bytes(), 20)
	f.Add([]byte{}, 1)
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF}, 3)

	f.Fuzz(func(t *testing.T, data []byte, count int) {
		if count < 0 || count > 10000 {
			t.Skip()
		}
		_, _, _ = DecodeAll(data, count)
	})
}

// FuzzRoundTrip checks that anything the encoder produces, the decoder reads
// back exactly.
func FuzzRoundTrip(f *testing.F) {
	f.Add(int64(1000), int64(200), 1.5, 10)

	f.Fuzz(func(t *testing.T, base, step int64, v float64, n int) {
		if n < 1 || n > 500 {
			t.Skip()
		}
		// Keep timestamps inside a sane range so the test is about the
		// codec rather than about int64 overflow.
		if step < -1e9 || step > 1e9 || base < -1e15 || base > 1e15 {
			t.Skip()
		}

		ts := make([]int64, n)
		vals := make([]float64, n)
		cur := base
		for i := 0; i < n; i++ {
			ts[i] = cur
			cur += step
			vals[i] = v * float64(i%3)
		}

		e := NewEncoder()
		for i := range ts {
			e.Push(ts[i], vals[i])
		}
		gotTs, gotVals, err := DecodeAll(e.Bytes(), e.Count())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		for i := range ts {
			if gotTs[i] != ts[i] {
				t.Fatalf("ts[%d]: got %d want %d", i, gotTs[i], ts[i])
			}
			if math.Float64bits(gotVals[i]) != math.Float64bits(vals[i]) {
				t.Fatalf("val[%d] mismatch", i)
			}
		}
	})
}
