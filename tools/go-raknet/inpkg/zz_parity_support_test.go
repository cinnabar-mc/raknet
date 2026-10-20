//go:build raknetparity

package raknet

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal"
)

// parityRepoPath resolves a path in the Rust repository that owns the shared fixtures.
func parityRepoPath(tb testing.TB, parts ...string) string {
	tb.Helper()
	root := os.Getenv("RAKNET_REPO_ROOT")
	if root == "" {
		tb.Skip("missing fixture RAKNET_REPO_ROOT: run through `go run ./cmd/inpkg`")
	}
	return filepath.Join(append([]string{root}, parts...)...)
}

// parityReadRecords reads a fixture of u32 little-endian length-prefixed records.
func parityReadRecords(tb testing.TB, name string) [][]byte {
	tb.Helper()
	data, err := os.ReadFile(parityRepoPath(tb, "testdata", "bench", name))
	if err != nil {
		tb.Fatalf("missing fixture testdata/bench/%s: %v (regenerate with `go run ./cmd/inpkg -run TestParityGolden -update`)", name, err)
	}
	var records [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			tb.Fatalf("truncated record header in %s", name)
		}
		n := int(binary.LittleEndian.Uint32(data))
		data = data[4:]
		if len(data) < n {
			tb.Fatalf("truncated record in %s", name)
		}
		records = append(records, data[:n:n])
		data = data[n:]
	}
	return records
}

// parityEncodeRecords is the inverse of parityReadRecords.
func parityEncodeRecords(records [][]byte) []byte {
	var out []byte
	for _, record := range records {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(record)))
		out = append(out, record...)
	}
	return out
}

// parityPayload matches raknet::test_support::pattern and cmd/peer's Pattern.
func parityPayload(n int, salt int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + salt*7 + 1)
	}
	if n > 0 {
		b[0] = 0xfe
	}
	return b
}

// parityPacketConn is an in-memory socket: reads come from a channel and writes are handed to sink.
type parityPacketConn struct {
	local, remote net.Addr
	reads         chan parityInbound
	sink          func([]byte, net.Addr)
	closed        chan struct{}
	once          sync.Once
}

type parityInbound struct {
	data []byte
	addr net.Addr
}

func newParityPacketConn(local, remote net.Addr, sink func([]byte, net.Addr)) *parityPacketConn {
	return &parityPacketConn{local: local, remote: remote, reads: make(chan parityInbound, 1024), sink: sink, closed: make(chan struct{})}
}

func (c *parityPacketConn) deliver(data []byte, from net.Addr) {
	select {
	case c.reads <- parityInbound{bytes.Clone(data), from}:
	case <-c.closed:
	}
}

func (c *parityPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	select {
	case d := <-c.reads:
		return copy(b, d.data), d.addr, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *parityPacketConn) Read(b []byte) (int, error) {
	n, _, err := c.ReadFrom(b)
	return n, err
}

func (c *parityPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	if c.sink != nil {
		c.sink(bytes.Clone(b), addr)
	}
	return len(b), nil
}

func (c *parityPacketConn) Write(b []byte) (int, error)      { return c.WriteTo(b, c.remote) }
func (c *parityPacketConn) Close() error                     { c.once.Do(func() { close(c.closed) }); return nil }
func (c *parityPacketConn) LocalAddr() net.Addr              { return c.local }
func (c *parityPacketConn) RemoteAddr() net.Addr             { return c.remote }
func (c *parityPacketConn) SetDeadline(time.Time) error      { return nil }
func (c *parityPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *parityPacketConn) SetWriteDeadline(time.Time) error { return nil }

// parityDialer hands a dialer the prepared in-memory connection.
type parityDialer struct{ conn net.Conn }

func (d parityDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return d.conn, nil
}

func parityDiscardLog() *slog.Logger { return slog.New(internal.DiscardHandler{}) }

// parityTranscript records timestamped datagrams in both directions.
type parityTranscript struct {
	mu    sync.Mutex
	start time.Time
	lines []string
	// maskCookie hides cookies, which a listener derives from a random salt.
	maskCookie bool
}

func (t *parityTranscript) add(dir string, data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, fmt.Sprintf("%s %d %s", dir, time.Since(t.start).Milliseconds(), t.mask(data)))
}

// mask renders data as hex with "xx" in place of random GUIDs, cookies and clock readings.
func (t *parityTranscript) mask(data []byte) string {
	masked := make([]bool, len(data))
	hide := func(from, to int) {
		for i := max(from, 0); i < min(to, len(data)); i++ {
			masked[i] = true
		}
	}
	payload := func(offset int, b []byte) {
		if len(b) == 0 {
			return
		}
		switch b[0] {
		case 0x09: // GUID and request time.
			hide(offset+1, offset+17)
		case 0x10, 0x13: // Trailing ping and pong times.
			hide(offset+len(b)-16, offset+len(b))
		case 0x00:
			hide(offset+1, offset+9)
		case 0x03:
			hide(offset+1, offset+17)
		}
	}
	switch {
	case len(data) == 0:
	case data[0] == 0x06 && t.maskCookie && len(data) > 29 && data[25] == 1:
		hide(26, 30)
	case data[0] == 0x07:
		if t.maskCookie && len(data) == 39 {
			hide(17, 21)
		}
		if !t.maskCookie {
			hide(len(data)-8, len(data)) // Dialers generate a random GUID.
		}
	case data[0]&bitFlagDatagram != 0 && data[0]&(bitFlagACK|bitFlagNACK) == 0 && len(data) >= 4:
		offset := 4
		for offset < len(data) {
			var pk packet
			n, err := pk.read(data[offset:])
			if err != nil {
				break
			}
			payload(offset+n-len(pk.content), pk.content)
			offset += n
		}
	}
	out := hex.EncodeToString(data)
	var b strings.Builder
	for i := range data {
		if masked[i] {
			b.WriteString("xx")
		} else {
			b.WriteString(out[2*i : 2*i+2])
		}
	}
	return b.String()
}

func (t *parityTranscript) text(header string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return header + strings.Join(t.lines, "\n") + "\n"
}

// parityCheckGolden compares or rewrites a committed golden fixture.
func parityCheckGolden(t *testing.T, rel string, got string) {
	t.Helper()
	path := parityRepoPath(t, rel)
	if *parityUpdate {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden %s: %v (regenerate with -update)", rel, err)
	}
	if string(want) != got {
		t.Fatalf("golden %s is stale; regenerate with `go run ./cmd/inpkg -run TestParityGolden -update`\n--- got ---\n%s", rel, got)
	}
}

// parityCheckRecords compares or rewrites a committed bench input.
func parityCheckRecords(t *testing.T, name string, records [][]byte) {
	t.Helper()
	path := parityRepoPath(t, "testdata", "bench", name)
	data := parityEncodeRecords(records)
	if *parityUpdate {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(want, data) {
		t.Fatalf("bench input testdata/bench/%s is stale or missing; regenerate with -update", name)
	}
}
