// Package ws is a minimal RFC 6455 WebSocket server.
//
// Written against the standard library rather than pulled in, because the
// parts this project needs are small and the parts it does not need
// (extensions, permessage-deflate, client mode) are most of a general
// library. Compression in particular is actively unwanted here: the wire
// format is already binary and delta-encoded, so deflate would spend CPU on
// every frame to save very little, and it would add a latency term to a
// budget the whole project is about minimising.
package ws

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// magicGUID is from RFC 6455 section 1.3. It exists so that a cached HTTP
// response cannot be mistaken for a successful handshake.
const magicGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Opcode is a WebSocket frame type.
type Opcode uint8

const (
	OpContinuation Opcode = 0x0
	OpText         Opcode = 0x1
	OpBinary       Opcode = 0x2
	OpClose        Opcode = 0x8
	OpPing         Opcode = 0x9
	OpPong         Opcode = 0xA
)

// Close codes from RFC 6455 section 7.4.1, plus the ones this uses.
const (
	CloseNormal          = 1000
	CloseGoingAway       = 1001
	CloseProtocolError   = 1002
	CloseUnsupportedData = 1003
	CloseMessageTooBig   = 1009
	CloseInternalError   = 1011
	// CloseSlowConsumer is in the private-use range. A connection stuck
	// full for 30 s is closed with this, so the client can distinguish
	// "you were too slow" from a network failure and back off rather than
	// immediately reconnecting into the same condition.
	CloseSlowConsumer = 4000
	// CloseDraining tells clients to reconnect elsewhere, with jitter.
	CloseDraining = 4001
)

// MaxMessageSize bounds an inbound message.
//
// Client messages here are control frames -- a subscribe, a pause -- which
// are hundreds of bytes. Anything larger is either a bug or an attempt to
// exhaust memory, and a bounded reader is the only defence.
const MaxMessageSize = 1 << 20 // 1 MiB

var (
	ErrNotWebSocket   = errors.New("ws: not a websocket upgrade request")
	ErrMessageTooBig  = errors.New("ws: message exceeds the maximum size")
	ErrUnmaskedClient = errors.New("ws: client frame was not masked")
	ErrReservedBits   = errors.New("ws: reserved bits set without a negotiated extension")
	ErrBadCloseCode   = errors.New("ws: invalid close code")
	ErrControlTooLong = errors.New("ws: control frame payload exceeds 125 bytes")
	ErrFragmentedCtrl = errors.New("ws: control frames may not be fragmented")
)

// Accept computes the Sec-WebSocket-Accept response header.
func Accept(key string) string {
	h := sha1.New()
	io.WriteString(h, key)
	io.WriteString(h, magicGUID)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// IsUpgrade reports whether a request is a WebSocket handshake.
//
// The Connection header is a comma-separated list and may contain more than
// "Upgrade" -- some proxies add "keep-alive" -- so it needs a token search
// rather than an equality check. That detail is the single most common
// reason a hand-rolled handshake works locally and fails behind a load
// balancer.
func IsUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, tok := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(tok), "upgrade") {
			return true
		}
	}
	return false
}

// Conn is an accepted WebSocket connection.
type Conn struct {
	conn net.Conn
	br   *bufio.Reader

	// writeMu serialises writes. WebSocket frames must not interleave on
	// the wire, and the gateway writes from both the fanout goroutine and
	// the heartbeat ticker.
	writeMu sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}
}

// Upgrade performs the handshake and hijacks the connection.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if !IsUpgrade(r) {
		return nil, ErrNotWebSocket
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, fmt.Errorf("ws: missing Sec-WebSocket-Key")
	}
	if v := r.Header.Get("Sec-WebSocket-Version"); v != "13" {
		return nil, fmt.Errorf("ws: unsupported version %q", v)
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("ws: response writer does not support hijacking")
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + Accept(key) + "\r\n\r\n"

	if _, err := conn.Write([]byte(resp)); err != nil {
		conn.Close()
		return nil, err
	}

	// TCP_NODELAY. Nagle's algorithm can add up to 40 ms waiting to
	// coalesce small writes, which is 20% of the entire latency budget for
	// a one-line change.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	return &Conn{conn: conn, br: brw.Reader, closed: make(chan struct{})}, nil
}

// Message is one received WebSocket message.
type Message struct {
	Opcode Opcode
	Data   []byte
}

// Read returns the next non-control message.
//
// Ping frames are answered transparently, because a client that pings and
// gets no pong concludes the connection is dead -- and answering pings is
// not something a caller should have to remember.
func (c *Conn) Read() (Message, error) {
	var fragments []byte
	var fragOp Opcode
	total := 0

	for {
		f, err := c.readFrame()
		if err != nil {
			return Message{}, err
		}

		switch f.opcode {
		case OpPing:
			if err := c.write(OpPong, f.payload); err != nil {
				return Message{}, err
			}
			continue
		case OpPong:
			continue
		case OpClose:
			code := CloseNormal
			if len(f.payload) >= 2 {
				code = int(binary.BigEndian.Uint16(f.payload[:2]))
			}
			_ = c.WriteClose(code, "")
			return Message{}, fmt.Errorf("ws: closed by peer with code %d", code)
		}

		total += len(f.payload)
		if total > MaxMessageSize {
			_ = c.WriteClose(CloseMessageTooBig, "message too large")
			return Message{}, ErrMessageTooBig
		}

		if f.opcode != OpContinuation {
			fragOp = f.opcode
			fragments = append(fragments[:0], f.payload...)
		} else {
			fragments = append(fragments, f.payload...)
		}

		if f.fin {
			return Message{Opcode: fragOp, Data: fragments}, nil
		}
	}
}

type frame struct {
	fin     bool
	opcode  Opcode
	payload []byte
}

func (c *Conn) readFrame() (frame, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		return frame{}, err
	}

	fin := hdr[0]&0x80 != 0
	// Reserved bits must be zero unless an extension negotiated them, and
	// none is negotiated here. Accepting them silently would misparse a
	// compressed frame as raw data.
	if hdr[0]&0x70 != 0 {
		return frame{}, ErrReservedBits
	}
	opcode := Opcode(hdr[0] & 0x0F)
	masked := hdr[1]&0x80 != 0
	length := int(hdr[1] & 0x7F)

	isControl := opcode&0x8 != 0
	if isControl {
		if !fin {
			return frame{}, ErrFragmentedCtrl
		}
		if length > 125 {
			return frame{}, ErrControlTooLong
		}
	}

	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return frame{}, err
		}
		length = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return frame{}, err
		}
		n := binary.BigEndian.Uint64(ext[:])
		// Bound before allocating: a 64-bit length is otherwise a
		// trivial way to ask the server for 16 exabytes.
		if n > MaxMessageSize {
			return frame{}, ErrMessageTooBig
		}
		length = int(n)
	}

	// Every client-to-server frame MUST be masked (RFC 6455 5.1). An
	// unmasked one means a broken or hostile client, and accepting it
	// leaves the payload XORed with nothing.
	if !masked {
		return frame{}, ErrUnmaskedClient
	}

	var maskKey [4]byte
	if _, err := io.ReadFull(c.br, maskKey[:]); err != nil {
		return frame{}, err
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return frame{}, err
	}
	for i := range payload {
		payload[i] ^= maskKey[i&3]
	}

	return frame{fin: fin, opcode: opcode, payload: payload}, nil
}

// WriteBinary sends a binary message.
func (c *Conn) WriteBinary(b []byte) error { return c.write(OpBinary, b) }

// WriteText sends a text message.
func (c *Conn) WriteText(s string) error { return c.write(OpText, []byte(s)) }

// WritePing sends a ping.
//
// The application-level heartbeat is what keeps a load balancer from
// killing an idle connection: many default to a 60 s idle timeout, so the
// gateway pings every 15-20 s, comfortably inside it.
func (c *Conn) WritePing(b []byte) error { return c.write(OpPing, b) }

// WriteClose sends a close frame with a code and reason.
func (c *Conn) WriteClose(code int, reason string) error {
	if code < 1000 || code > 4999 {
		return ErrBadCloseCode
	}
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload[:2], uint16(code))
	copy(payload[2:], reason)
	return c.write(OpClose, payload)
}

func (c *Conn) write(op Opcode, payload []byte) error {
	select {
	case <-c.closed:
		return net.ErrClosed
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	var hdr [10]byte
	hdr[0] = 0x80 | byte(op) // FIN
	n := len(payload)

	var hdrLen int
	switch {
	case n < 126:
		hdr[1] = byte(n)
		hdrLen = 2
	case n <= 0xFFFF:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		hdrLen = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		hdrLen = 10
	}
	// Server-to-client frames are never masked.

	if _, err := c.conn.Write(hdr[:hdrLen]); err != nil {
		return err
	}
	if n > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// SetWriteDeadline bounds how long a write may block.
//
// Without it, a client that has stopped reading blocks the writer
// goroutine indefinitely -- the kernel send buffer fills and Write never
// returns. The conflating queue bounds *memory*; this bounds *time*.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

func (c *Conn) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

func (c *Conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// Close closes the connection once.
func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.conn.Close()
	})
	return err
}

// Done is closed when the connection is closed.
func (c *Conn) Done() <-chan struct{} { return c.closed }

// GenerateKey produces a Sec-WebSocket-Key, for tests and for a client.
func GenerateKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
}

// WriteClientFrame writes a masked frame, which is what a client must send.
//
// Exported for tests: exercising the server's unmasking path requires
// producing correctly masked frames, and a test that wrote unmasked ones
// would be testing a code path no real client uses.
func WriteClientFrame(w io.Writer, op Opcode, payload []byte, fin bool) error {
	var hdr [14]byte
	hdr[0] = byte(op)
	if fin {
		hdr[0] |= 0x80
	}
	n := len(payload)

	var hdrLen int
	switch {
	case n < 126:
		hdr[1] = 0x80 | byte(n)
		hdrLen = 2
	case n <= 0xFFFF:
		hdr[1] = 0x80 | 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		hdrLen = 4
	default:
		hdr[1] = 0x80 | 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		hdrLen = 10
	}

	var key [4]byte
	if _, err := rand.Read(key[:]); err != nil {
		return err
	}
	copy(hdr[hdrLen:hdrLen+4], key[:])
	hdrLen += 4

	if _, err := w.Write(hdr[:hdrLen]); err != nil {
		return err
	}

	masked := make([]byte, n)
	for i := range payload {
		masked[i] = payload[i] ^ key[i&3]
	}
	_, err := w.Write(masked)
	return err
}

// ReadServerFrame reads an unmasked server frame, for tests and a client.
func ReadServerFrame(r io.Reader) (Opcode, []byte, bool, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, false, err
	}
	fin := hdr[0]&0x80 != 0
	op := Opcode(hdr[0] & 0x0F)
	masked := hdr[1]&0x80 != 0
	length := int(hdr[1] & 0x7F)

	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, false, err
		}
		length = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, false, err
		}
		length = int(binary.BigEndian.Uint64(ext[:]))
	}

	var key [4]byte
	if masked {
		if _, err := io.ReadFull(r, key[:]); err != nil {
			return 0, nil, false, err
		}
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, false, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i&3]
		}
	}
	return op, payload, fin, nil
}
