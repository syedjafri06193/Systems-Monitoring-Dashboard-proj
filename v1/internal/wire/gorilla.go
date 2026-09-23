package wire

import (
	"errors"
	"math"
)

// Gorilla compression, from Pelkonen et al., VLDB 2015.
//
// Two independent ideas, applied to the two halves of a point:
//
//   - Timestamps use delta-of-delta. Metrics arrive on a regular cadence,
//     so the second difference is almost always exactly zero, and zero
//     costs a single bit.
//   - Values use XOR against the previous value. A metric that changes
//     slowly produces a XOR with many leading and trailing zero bits, and
//     only the meaningful middle is written.
//
// Together these take a point from ~9 bytes (the plain binary framing) to
// roughly 2-4 bytes on realistic data.

// Timestamp control codes. Variable-length prefixes, shortest for the most
// common case.
//
//	0       delta-of-delta == 0        1 bit
//	10      7-bit zigzag dod           9 bits
//	110     9-bit                      12 bits
//	1110    12-bit                     16 bits
//	11110   32-bit                     37 bits
//	11111   64-bit escape              69 bits
const (
	dodZero = iota
	dod7
	dod9
	dod12
	dod32
)

// Encoder writes a stream of points for a single series.
//
// Per series, not per connection: the compression works by exploiting
// regularity within one metric's history. Interleaving series would break
// both the timestamp cadence and the value correlation, and produce output
// larger than uncompressed.
type Encoder struct {
	bw *BitWriter

	n         int
	prevTs    int64
	prevDelta int64

	prevVal      uint64
	prevLeading  uint8
	prevTrailing uint8
	// hasPrevWindow tracks whether prevLeading/prevTrailing are meaningful.
	// Initialising leading to 0 instead would make the first XOR believe it
	// can reuse a window that was never established.
	hasPrevWindow bool
}

func NewEncoder() *Encoder {
	return &Encoder{bw: NewBitWriter()}
}

// Push appends one point.
func (e *Encoder) Push(ts int64, v float64) {
	e.encodeTimestamp(ts)
	e.encodeValue(v)
	e.n++
}

func (e *Encoder) encodeTimestamp(ts int64) {
	if e.n == 0 {
		// The first timestamp is written in full. There is nothing to
		// delta against, and truncating it would limit the usable epoch.
		e.bw.WriteBits(uint64(ts), 64)
		e.prevTs = ts
		return
	}

	delta := ts - e.prevTs
	if e.n == 1 {
		// The first delta has no previous delta, so it is written directly.
		// 32 bits of zigzag covers ±24 days of gap, far beyond anything a
		// live stream will see.
		e.bw.WriteBits(zigzag(delta), 32)
		e.prevTs, e.prevDelta = ts, delta
		return
	}

	// The ranges below are for *zigzag* encoding, and differ by one from
	// the ones usually quoted.
	//
	// The Gorilla paper's ranges ([-63,64], [-255,256], [-2047,2048]) are
	// for storing the delta-of-delta as a plain signed value. Zigzag maps
	// [-64,63] onto [0,127], so combining the paper's ranges with zigzag
	// overflows the field: zigzag(64) is 128, which does not fit in 7 bits
	// and is silently truncated to 0. The result is a corrupt timestamp,
	// but only for the handful of dod values that land exactly on the
	// boundary — which is why it survives casual testing.
	dod := delta - e.prevDelta
	switch {
	case dod == 0:
		e.bw.WriteBit(0)
	case dod >= -64 && dod <= 63:
		e.bw.WriteBits(0b10, 2)
		e.bw.WriteBits(zigzag(dod), 7)
	case dod >= -256 && dod <= 255:
		e.bw.WriteBits(0b110, 3)
		e.bw.WriteBits(zigzag(dod), 9)
	case dod >= -2048 && dod <= 2047:
		e.bw.WriteBits(0b1110, 4)
		e.bw.WriteBits(zigzag(dod), 12)
	case zigzag(dod) <= math.MaxUint32:
		e.bw.WriteBits(0b11110, 5)
		e.bw.WriteBits(zigzag(dod), 32)
	default:
		// Beyond 32 bits of zigzag is a gap of more than ~24 days between
		// consecutive points. The paper stops at 32 bits and silently
		// corrupts past it; a 64-bit escape costs one extra prefix bit on
		// a case that never occurs in practice, and makes the codec total
		// rather than almost-total. Fuzzing finds the difference
		// immediately.
		e.bw.WriteBits(0b11111, 5)
		e.bw.WriteBits(zigzag(dod), 64)
	}

	e.prevTs, e.prevDelta = ts, delta
}

func (e *Encoder) encodeValue(v float64) {
	bits := math.Float64bits(v)

	if e.n == 0 {
		e.bw.WriteBits(bits, 64)
		e.prevVal = bits
		return
	}

	xor := bits ^ e.prevVal
	e.prevVal = bits

	if xor == 0 {
		// Identical to the previous value. Extremely common for gauges and
		// for any metric sampled faster than it changes.
		e.bw.WriteBit(0)
		return
	}
	e.bw.WriteBit(1)

	leading := uint8(leadingZeros64(xor))
	trailing := uint8(trailingZeros64(xor))

	// The leading-zero count is stored in 5 bits, so it cannot exceed 31.
	// Clamping rather than widening the field is what the paper does; the
	// cost is a few wasted bits on values whose XOR has >31 leading zeros.
	if leading > 31 {
		leading = 31
	}

	if e.hasPrevWindow && leading >= e.prevLeading && trailing >= e.prevTrailing {
		// The meaningful bits fit inside the previous window, so the window
		// itself does not need re-sending.
		e.bw.WriteBit(0)
		sigbits := 64 - e.prevLeading - e.prevTrailing
		e.bw.WriteBits(xor>>e.prevTrailing, sigbits)
		return
	}

	e.bw.WriteBit(1)
	sigbits := 64 - leading - trailing
	e.bw.WriteBits(uint64(leading), 5)
	// sigbits is 1..64, stored in 6 bits as 0..63. 64 would overflow the
	// field, and it cannot occur: xor != 0 here, so at least one bit is
	// zero on one end... except when xor has no leading or trailing zeros
	// at all, which gives exactly 64. Storing sigbits-1 avoids the
	// collision between 0 and 64 that a naive encoding has.
	e.bw.WriteBits(uint64(sigbits-1), 6)
	e.bw.WriteBits(xor>>trailing, sigbits)

	e.prevLeading, e.prevTrailing = leading, trailing
	e.hasPrevWindow = true
}

// Bytes returns the compressed stream.
func (e *Encoder) Bytes() []byte { return e.bw.Bytes() }

// Count is the number of points encoded. A decoder needs it, because the
// stream is bit-padded and cannot self-terminate.
func (e *Encoder) Count() int { return e.n }

// BitLen is the exact compressed size in bits, for measuring bytes/point.
func (e *Encoder) BitLen() int { return e.bw.BitLen() }

func (e *Encoder) Reset() {
	e.bw.Reset()
	e.n = 0
	e.prevTs, e.prevDelta = 0, 0
	e.prevVal = 0
	e.prevLeading, e.prevTrailing = 0, 0
	e.hasPrevWindow = false
}

// Decoder reverses Encoder.
type Decoder struct {
	br *BitReader

	n         int
	count     int
	prevTs    int64
	prevDelta int64

	prevVal       uint64
	prevLeading   uint8
	prevTrailing  uint8
	hasPrevWindow bool
}

// NewDecoder reads count points from b.
//
// The count is required rather than inferred. The bitstream is padded to a
// byte boundary, so up to 7 trailing zero bits remain, and a zero bit is a
// valid "delta-of-delta is zero" control code — a decoder that read until
// exhaustion would emit up to 7 phantom points.
func NewDecoder(b []byte, count int) *Decoder {
	return &Decoder{br: NewBitReader(b), count: count}
}

var errCorrupt = errors.New("wire: corrupt gorilla stream")

// Next returns the next point, or false when the stream is exhausted.
func (d *Decoder) Next() (ts int64, v float64, ok bool, err error) {
	if d.n >= d.count {
		return 0, 0, false, nil
	}

	ts, err = d.decodeTimestamp()
	if err != nil {
		return 0, 0, false, err
	}
	v, err = d.decodeValue()
	if err != nil {
		return 0, 0, false, err
	}
	d.n++
	return ts, v, true, nil
}

func (d *Decoder) decodeTimestamp() (int64, error) {
	if d.n == 0 {
		raw, err := d.br.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.prevTs = int64(raw)
		return d.prevTs, nil
	}

	if d.n == 1 {
		raw, err := d.br.ReadBits(32)
		if err != nil {
			return 0, err
		}
		d.prevDelta = unzigzag(raw)
		d.prevTs += d.prevDelta
		return d.prevTs, nil
	}

	// Count the leading ones to identify the control code. Every code
	// except the longest is terminated by a zero bit, so five ones in a
	// row is unambiguous and needs no terminator.
	var nbits uint8
	var width uint8
	for nbits = 0; nbits < 5; nbits++ {
		b, err := d.br.ReadBit()
		if err != nil {
			return 0, err
		}
		if b == 0 {
			break
		}
	}

	switch nbits {
	case 0:
		width = 0 // dod == 0
	case 1:
		width = 7
	case 2:
		width = 9
	case 3:
		width = 12
	case 4:
		width = 32
	case 5:
		width = 64
	default:
		return 0, errCorrupt
	}

	var dod int64
	if width > 0 {
		raw, err := d.br.ReadBits(width)
		if err != nil {
			return 0, err
		}
		dod = unzigzag(raw)
	}

	d.prevDelta += dod
	d.prevTs += d.prevDelta
	return d.prevTs, nil
}

func (d *Decoder) decodeValue() (float64, error) {
	if d.n == 0 {
		raw, err := d.br.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.prevVal = raw
		return math.Float64frombits(raw), nil
	}

	changed, err := d.br.ReadBit()
	if err != nil {
		return 0, err
	}
	if changed == 0 {
		return math.Float64frombits(d.prevVal), nil
	}

	newWindow, err := d.br.ReadBit()
	if err != nil {
		return 0, err
	}

	if newWindow == 1 {
		lead, err := d.br.ReadBits(5)
		if err != nil {
			return 0, err
		}
		sig, err := d.br.ReadBits(6)
		if err != nil {
			return 0, err
		}
		d.prevLeading = uint8(lead)
		// Encoder wrote sigbits-1.
		sigbits := uint8(sig) + 1
		d.prevTrailing = 64 - d.prevLeading - sigbits
		d.hasPrevWindow = true
	} else if !d.hasPrevWindow {
		// "Reuse the previous window" when none has been established. Only
		// reachable from a corrupt or truncated stream; without this check
		// it would silently decode garbage.
		return 0, errCorrupt
	}

	sigbits := 64 - d.prevLeading - d.prevTrailing
	raw, err := d.br.ReadBits(sigbits)
	if err != nil {
		return 0, err
	}

	bits := d.prevVal ^ (raw << d.prevTrailing)
	d.prevVal = bits
	return math.Float64frombits(bits), nil
}

// DecodeAll is the common case: decode a whole block into slices.
func DecodeAll(b []byte, count int) (ts []int64, vals []float64, err error) {
	d := NewDecoder(b, count)
	ts = make([]int64, 0, count)
	vals = make([]float64, 0, count)
	for {
		t, v, ok, err := d.Next()
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			break
		}
		ts = append(ts, t)
		vals = append(vals, v)
	}
	return ts, vals, nil
}

func leadingZeros64(v uint64) int {
	n := 0
	for i := 63; i >= 0; i-- {
		if v&(1<<uint(i)) != 0 {
			return n
		}
		n++
	}
	return 64
}

func trailingZeros64(v uint64) int {
	if v == 0 {
		return 64
	}
	n := 0
	for i := 0; i < 64; i++ {
		if v&(1<<uint(i)) != 0 {
			return n
		}
		n++
	}
	return 64
}
