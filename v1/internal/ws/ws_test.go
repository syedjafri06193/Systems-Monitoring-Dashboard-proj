package ws

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAcceptMatchesTheRFCExample(t *testing.T) {
	// RFC 6455 section 1.3's worked example. Getting this wrong makes
	// every browser refuse the connection with an opaque error.
	if got := Accept("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("Accept = %q", got)
	}
}

func TestIsUpgradeHandlesAConnectionHeaderList(t *testing.T) {
	// The single most common reason a hand-rolled handshake works locally
	// and fails behind a load balancer: Connection is a comma-separated
	// list and proxies add tokens to it.
	cases := []struct {
		conn string
		want bool
	}{
		{"Upgrade", true},
		{"upgrade", true},
		{"keep-alive, Upgrade", true},
		{"Upgrade, keep-alive", true},
		{" Upgrade ", true},
		{"keep-alive", false},
		{"", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Upgrade", "websocket")
		r.Header.Set("Connection", c.conn)
		if got := IsUpgrade(r); got != c.want {
			t.Errorf("Connection %q: got %v want %v", c.conn, got, c.want)
		}
	}
}

func TestIsUpgradeRequiresWebSocketToken(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Upgrade", "h2c")
	r.Header.Set("Connection", "Upgrade")
	if IsUpgrade(r) {
		t.Fatal("accepted a non-websocket upgrade")
	}
}

// --- end-to-end over a real socket -----------------------------------------

// serve starts a server that upgrades and hands the Conn to fn.
func serve(t *testing.T, fn func(*Conn)) (*clientConn, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := Upgrade(w, r)
			if err != nil {
				t.Errorf("upgrade: %v", err)
				return
			}
			fn(c)
		}),
	}
	go srv.Serve(ln)

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	key, _ := GenerateKey()
	req := "GET / HTTP/1.1\r\n" +
		"Host: x\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := raw.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	// Read the 101 response through a buffered reader, line by line.
	//
	// Reading into a fixed buffer would swallow the first WebSocket frame
	// along with the headers, because the server can write both before the
	// client reads either -- which is exactly what a 5 Hz stream does.
	raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(raw)
	var sb strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading handshake response: %v", err)
		}
		sb.WriteString(line)
		if line == "\r\n" {
			break
		}
	}
	raw.SetReadDeadline(time.Time{})

	resp := sb.String()
	if !strings.HasPrefix(resp, "HTTP/1.1 101") {
		t.Fatalf("handshake response: %q", resp)
	}
	if !strings.Contains(resp, "Sec-WebSocket-Accept: "+Accept(key)) {
		t.Fatalf("wrong accept header: %q", resp)
	}

	return &clientConn{Conn: raw, r: br}, func() {
		raw.Close()
		ln.Close()
		srv.Close()
	}
}

// clientConn pairs the socket with the buffered reader that holds any
// bytes read past the handshake.
type clientConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *clientConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func TestBinaryRoundTrip(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	raw, cleanup := serve(t, func(c *Conn) {
		defer wg.Done()
		msg, err := c.Read()
		if err != nil {
			t.Errorf("read: %v", err)
			return
		}
		if msg.Opcode != OpBinary {
			t.Errorf("opcode = %v", msg.Opcode)
		}
		if err := c.WriteBinary(append([]byte("echo:"), msg.Data...)); err != nil {
			t.Errorf("write: %v", err)
		}
	})
	defer cleanup()

	if err := WriteClientFrame(raw, OpBinary, []byte("hello"), true); err != nil {
		t.Fatal(err)
	}

	raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	op, payload, fin, err := ReadServerFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpBinary || !fin || string(payload) != "echo:hello" {
		t.Fatalf("got op=%v fin=%v payload=%q", op, fin, payload)
	}
	wg.Wait()
}

func TestServerFramesAreNotMasked(t *testing.T) {
	// RFC 6455 5.1: a server must not mask. A browser closes the
	// connection with a protocol error if it does.
	raw, cleanup := serve(t, func(c *Conn) {
		c.WriteBinary([]byte("data"))
	})
	defer cleanup()

	raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hdr [2]byte
	if _, err := raw.Read(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if hdr[1]&0x80 != 0 {
		t.Fatal("server set the mask bit")
	}
}

func TestUnmaskedClientFrameIsRejected(t *testing.T) {
	// Every client-to-server frame MUST be masked. Accepting an unmasked
	// one leaves the payload XORed with nothing, and a client sending them
	// is broken or hostile.
	errCh := make(chan error, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		_, err := c.Read()
		errCh <- err
	})
	defer cleanup()

	// A hand-built unmasked binary frame.
	raw.Write([]byte{0x82, 0x04, 'd', 'a', 't', 'a'})

	select {
	case err := <-errCh:
		if err != ErrUnmaskedClient {
			t.Fatalf("got %v, want ErrUnmaskedClient", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server accepted an unmasked frame")
	}
}

func TestReservedBitsAreRejected(t *testing.T) {
	// No extension is negotiated, so reserved bits set means a compressed
	// frame the server would otherwise misparse as raw data.
	errCh := make(chan error, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		_, err := c.Read()
		errCh <- err
	})
	defer cleanup()

	// RSV1 set, masked, empty payload.
	raw.Write([]byte{0xC2, 0x80, 0, 0, 0, 0})

	select {
	case err := <-errCh:
		if err != ErrReservedBits {
			t.Fatalf("got %v, want ErrReservedBits", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server accepted reserved bits")
	}
}

func TestPingIsAnsweredTransparently(t *testing.T) {
	// A client that pings and gets no pong concludes the connection is
	// dead, and answering pings is not something a caller should have to
	// remember.
	raw, cleanup := serve(t, func(c *Conn) {
		c.Read() // blocks; the ping is handled inside
	})
	defer cleanup()

	if err := WriteClientFrame(raw, OpPing, []byte("ping!"), true); err != nil {
		t.Fatal(err)
	}

	raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	op, payload, _, err := ReadServerFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpPong {
		t.Fatalf("op = %v, want pong", op)
	}
	if string(payload) != "ping!" {
		t.Fatalf("pong payload = %q; RFC requires echoing the ping body", payload)
	}
}

func TestFragmentedMessageIsReassembled(t *testing.T) {
	msgCh := make(chan Message, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		m, err := c.Read()
		if err != nil {
			t.Errorf("read: %v", err)
			return
		}
		msgCh <- m
	})
	defer cleanup()

	WriteClientFrame(raw, OpBinary, []byte("frag"), false)
	WriteClientFrame(raw, OpContinuation, []byte("ment"), false)
	WriteClientFrame(raw, OpContinuation, []byte("ed"), true)

	select {
	case m := <-msgCh:
		if string(m.Data) != "fragmented" {
			t.Fatalf("got %q", m.Data)
		}
		if m.Opcode != OpBinary {
			t.Fatalf("opcode = %v; the first fragment's opcode must be kept", m.Opcode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestPingBetweenFragmentsIsHandled(t *testing.T) {
	// Control frames may be interleaved into a fragmented message. A
	// reader that treats them as continuation data corrupts the message.
	msgCh := make(chan Message, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		m, err := c.Read()
		if err != nil {
			t.Errorf("read: %v", err)
			return
		}
		msgCh <- m
	})
	defer cleanup()

	WriteClientFrame(raw, OpBinary, []byte("ab"), false)
	WriteClientFrame(raw, OpPing, []byte("p"), true)
	WriteClientFrame(raw, OpContinuation, []byte("cd"), true)

	select {
	case m := <-msgCh:
		if string(m.Data) != "abcd" {
			t.Fatalf("got %q; a ping corrupted the message", m.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestFragmentedControlFrameIsRejected(t *testing.T) {
	errCh := make(chan error, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		_, err := c.Read()
		errCh <- err
	})
	defer cleanup()

	WriteClientFrame(raw, OpPing, []byte("x"), false) // FIN clear on a control frame

	select {
	case err := <-errCh:
		if err != ErrFragmentedCtrl {
			t.Fatalf("got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("accepted a fragmented control frame")
	}
}

func TestOversizedLengthIsRejectedBeforeAllocating(t *testing.T) {
	// A 64-bit length field is otherwise a trivial way to ask the server
	// for 16 exabytes.
	errCh := make(chan error, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		_, err := c.Read()
		errCh <- err
	})
	defer cleanup()

	hdr := make([]byte, 14)
	hdr[0] = 0x82       // FIN + binary
	hdr[1] = 0x80 | 127 // masked, 64-bit length
	binary.BigEndian.PutUint64(hdr[2:10], 1<<62)
	raw.Write(hdr)

	select {
	case err := <-errCh:
		if err != ErrMessageTooBig {
			t.Fatalf("got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server accepted a 4-exabyte length")
	}
}

func TestLargeMessageUsesExtendedLength(t *testing.T) {
	// The 126 and 127 length encodings must round-trip, or any payload
	// over 125 bytes is silently truncated.
	for _, size := range []int{125, 126, 1000, 70000} {
		raw, cleanup := serve(t, func(c *Conn) {
			c.WriteBinary(bytes.Repeat([]byte{0xAB}, size))
		})

		raw.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, payload, _, err := ReadServerFrame(raw)
		if err != nil {
			cleanup()
			t.Fatalf("size %d: %v", size, err)
		}
		if len(payload) != size {
			cleanup()
			t.Fatalf("size %d: got %d bytes", size, len(payload))
		}
		cleanup()
	}
}

func TestCloseCarriesCodeAndReason(t *testing.T) {
	// A connection stuck full is closed with an explicit code, so the
	// client can tell "you were too slow" from a network failure and back
	// off rather than reconnecting into the same condition.
	raw, cleanup := serve(t, func(c *Conn) {
		c.WriteClose(CloseSlowConsumer, "queue full for 30s")
	})
	defer cleanup()

	raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	op, payload, _, err := ReadServerFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpClose {
		t.Fatalf("op = %v", op)
	}
	code := binary.BigEndian.Uint16(payload[:2])
	if code != CloseSlowConsumer {
		t.Fatalf("code = %d", code)
	}
	if string(payload[2:]) != "queue full for 30s" {
		t.Fatalf("reason = %q", payload[2:])
	}
}

func TestInvalidCloseCodeIsRejected(t *testing.T) {
	c := &Conn{closed: make(chan struct{})}
	if err := c.WriteClose(1, "x"); err != ErrBadCloseCode {
		t.Fatalf("got %v", err)
	}
	if err := c.WriteClose(9999, "x"); err != ErrBadCloseCode {
		t.Fatalf("got %v", err)
	}
}

func TestConcurrentWritesDoNotInterleave(t *testing.T) {
	// WebSocket frames must not interleave on the wire, and the gateway
	// writes from both the fanout goroutine and the heartbeat ticker.
	const writers = 8
	const each = 50

	raw, cleanup := serve(t, func(c *Conn) {
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				payload := bytes.Repeat([]byte{byte(w)}, 200)
				for i := 0; i < each; i++ {
					if err := c.WriteBinary(payload); err != nil {
						return
					}
				}
			}(w)
		}
		wg.Wait()
	})
	defer cleanup()

	raw.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < writers*each; i++ {
		_, payload, _, err := ReadServerFrame(raw)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if len(payload) != 200 {
			t.Fatalf("frame %d has %d bytes", i, len(payload))
		}
		// Every byte must come from one writer.
		for _, b := range payload {
			if b != payload[0] {
				t.Fatalf("frame %d interleaved two writers' payloads", i)
			}
		}
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	c := &Conn{closed: make(chan struct{})}
	close(c.closed)
	if err := c.WriteBinary([]byte("x")); err == nil {
		t.Fatal("write succeeded after close")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			defer c.Close()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c := &Conn{conn: nc, closed: make(chan struct{})}
	c.Close()
	c.Close() // must not panic on a double close of the channel
}

func TestUpgradeRejectsNonWebSocket(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	if _, err := Upgrade(w, r); err != ErrNotWebSocket {
		t.Fatalf("got %v", err)
	}
}

func TestUpgradeRejectsWrongVersion(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Sec-WebSocket-Key", "x")
	r.Header.Set("Sec-WebSocket-Version", "8")
	if _, err := Upgrade(w, r); err == nil {
		t.Fatal("accepted version 8")
	}
}

func TestUnexpectedContinuationIsRejected(t *testing.T) {
	errCh := make(chan error, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		_, err := c.Read()
		errCh <- err
	})
	defer cleanup()

	WriteClientFrame(raw, OpContinuation, []byte("orphan"), true)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrBadFragment) {
			t.Fatalf("err = %v, want ErrBadFragment", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestNewDataFrameMidMessageIsRejected(t *testing.T) {
	errCh := make(chan error, 1)
	raw, cleanup := serve(t, func(c *Conn) {
		_, err := c.Read()
		errCh <- err
	})
	defer cleanup()

	WriteClientFrame(raw, OpBinary, []byte("ab"), false)
	WriteClientFrame(raw, OpText, []byte("cd"), true)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrBadFragment) {
			t.Fatalf("err = %v, want ErrBadFragment", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestCheckOrigin(t *testing.T) {
	cases := []struct {
		origin, host string
		allowed      []string
		want         bool
	}{
		{"", "dash:8080", nil, true},                               // not a browser
		{"http://dash:8080", "dash:8080", nil, true},               // same origin
		{"https://DASH:8080", "dash:8080", nil, true},              // host compare is case-insensitive
		{"https://evil.example", "dash:8080", nil, false},          // cross-site hijack
		{"https://evil.example", "dash:8080", []string{"*"}, true}, // explicitly opened up
		{"https://ops.example", "dash:8080", []string{"https://ops.example/"}, true},
		{"null", "dash:8080", nil, false}, // sandboxed iframe / file://
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "http://"+c.host+"/stream", nil)
		r.Host = c.host
		if c.origin != "" {
			r.Header.Set("Origin", c.origin)
		}
		if got := CheckOrigin(r, c.allowed); got != c.want {
			t.Errorf("origin=%q host=%q allowed=%v: got %v, want %v", c.origin, c.host, c.allowed, got, c.want)
		}
	}
}
