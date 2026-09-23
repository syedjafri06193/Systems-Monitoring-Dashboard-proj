package wire

import "errors"

// ErrShortRead is returned when a reader runs past the end of its buffer.
//
// A distinct error rather than a panic: bit-level decoders are fed bytes
// from the network, and a truncated or corrupt frame is an expected input,
// not a programming mistake.
var ErrShortRead = errors.New("wire: read past end of bitstream")

// BitWriter appends individual bits, MSB first within each byte.
//
// MSB-first matters and is not arbitrary: the Gorilla control codes are
// variable-length prefixes (0, 10, 110, 1110, 1111), and prefix decoding
// only works if bits come off the stream in the order they went on.
type BitWriter struct {
	buf []byte
	// count of bits already used in the final byte, 0-7.
	nbits uint8
}

func NewBitWriter() *BitWriter {
	return &BitWriter{buf: make([]byte, 0, 256)}
}

// WriteBit appends a single bit.
func (w *BitWriter) WriteBit(b uint8) {
	if w.nbits == 0 {
		w.buf = append(w.buf, 0)
	}
	if b != 0 {
		w.buf[len(w.buf)-1] |= 1 << (7 - w.nbits)
	}
	w.nbits = (w.nbits + 1) & 7
}

// WriteBits appends the low n bits of v, most significant first.
func (w *BitWriter) WriteBits(v uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		w.WriteBit(uint8((v >> uint(i)) & 1))
	}
}

// Bytes returns the written stream. The final byte is zero-padded.
//
// The padding is why a decoder cannot simply read until it runs out of
// bits: up to 7 trailing zero bits are indistinguishable from data. Every
// format here therefore carries an explicit count.
func (w *BitWriter) Bytes() []byte { return w.buf }

// BitLen is the number of bits written, which is not len(Bytes())*8.
func (w *BitWriter) BitLen() int {
	if w.nbits == 0 {
		return len(w.buf) * 8
	}
	return (len(w.buf)-1)*8 + int(w.nbits)
}

func (w *BitWriter) Reset() {
	w.buf = w.buf[:0]
	w.nbits = 0
}

// BitReader reads bits MSB first.
type BitReader struct {
	buf []byte
	pos int // bit position
}

func NewBitReader(b []byte) *BitReader { return &BitReader{buf: b} }

// ReadBit returns the next bit.
func (r *BitReader) ReadBit() (uint8, error) {
	if r.pos >= len(r.buf)*8 {
		return 0, ErrShortRead
	}
	b := (r.buf[r.pos>>3] >> (7 - uint(r.pos&7))) & 1
	r.pos++
	return b, nil
}

// ReadBits returns the next n bits as the low bits of a uint64.
func (r *BitReader) ReadBits(n uint8) (uint64, error) {
	// Bounds-check once rather than per bit: a corrupt length prefix
	// claiming 64 bits when 3 remain should fail immediately, not after 3
	// successful reads leave the caller with a half-built value.
	if n > 64 {
		return 0, errors.New("wire: ReadBits n > 64")
	}
	if r.pos+int(n) > len(r.buf)*8 {
		return 0, ErrShortRead
	}
	var v uint64
	for i := uint8(0); i < n; i++ {
		b := (r.buf[r.pos>>3] >> (7 - uint(r.pos&7))) & 1
		v = v<<1 | uint64(b)
		r.pos++
	}
	return v, nil
}

// BitPos is the current read offset in bits.
func (r *BitReader) BitPos() int { return r.pos }

// zigzag maps signed to unsigned so small magnitudes stay small.
//
// Without it, -1 as a two's-complement uint64 is all ones and costs the
// full width in a variable-length encoding — which would defeat the entire
// point of delta-of-delta, since roughly half of all deltas are negative.
func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func unzigzag(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }
