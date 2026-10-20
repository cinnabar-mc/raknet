// Command peer runs one go-raknet connection for Rust interop tests.
//
// It listens or dials, prints `listening <addr>` (listen mode) and `connected <addr>`,
// then reads commands from stdin and reports events on stdout, one per line:
//
//	send <size> <count>  send count messages; message k of a run is pattern(size, k)
//	close                close the connection gracefully
//	exit                 exit immediately, without a disconnect notification
//
//	recv <size> ok       message k arrived intact; k counts only these
//	unexpected <size> <hex>  a message that is not pattern(size, k), with its first bytes
//	sent <count>         a send command finished queueing
//	closed <ms>          the connection ended, ms after the last received message
//	error <text>         a command or connection failed
package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sandertv/go-raknet"
)

func main() {
	mode := flag.String("mode", "listen", "listen or dial")
	addr := flag.String("addr", "127.0.0.1:0", "listen address, or the address to dial")
	verbose := flag.Bool("v", false, "log go-raknet errors to stderr")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if *verbose {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	out := &events{w: bufio.NewWriter(os.Stdout)}
	conn, err := open(*mode, *addr, logger, out)
	if err != nil {
		out.emit("error %v", err)
		os.Exit(1)
	}
	out.emit("connected %s", conn.RemoteAddr())
	go receive(conn, out)
	commands(conn, out)
}

// events serializes stdout lines between the reader and command goroutines.
type events struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func (e *events) emit(format string, args ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Fprintf(e.w, format+"\n", args...)
	_ = e.w.Flush()
}

func open(mode, addr string, logger *slog.Logger, out *events) (*raknet.Conn, error) {
	switch mode {
	case "listen":
		listener, err := raknet.ListenConfig{ErrorLog: logger}.Listen(addr)
		if err != nil {
			return nil, err
		}
		out.emit("listening %s", listener.Addr())
		conn, err := listener.Accept()
		if err != nil {
			return nil, err
		}
		return conn.(*raknet.Conn), nil
	case "dial":
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return raknet.Dialer{ErrorLog: logger}.DialContext(ctx, addr)
	}
	return nil, fmt.Errorf("unknown mode %q", mode)
}

// Pattern fills message k of a run; both test suites generate and verify the same bytes.
// The first byte is the game-packet ID, so no message reads as a RakNet control packet.
func Pattern(size, k int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*31 + k*7 + 1)
	}
	if size > 0 {
		b[0] = 0xfe
	}
	return b
}

func receive(conn *raknet.Conn, out *events) {
	last := time.Now()
	for k := 0; ; {
		message, err := conn.ReadPacket()
		if err != nil {
			out.emit("closed %d", time.Since(last).Milliseconds())
			return
		}
		last = time.Now()
		if !bytes.Equal(message, Pattern(len(message), k)) {
			out.emit("unexpected %d %x", len(message), message[:min(len(message), 4)])
			continue
		}
		k++
		out.emit("recv %d ok", len(message))
	}
}

func commands(conn *raknet.Conn, out *events) {
	sent := 0
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "send":
			var size, count int
			if len(fields) != 3 {
				out.emit("error usage: send <size> <count>")
				continue
			}
			fmt.Sscan(fields[1], &size)
			fmt.Sscan(fields[2], &count)
			for range count {
				if _, err := conn.Write(Pattern(size, sent)); err != nil {
					out.emit("error send: %v", err)
					break
				}
				sent++
			}
			out.emit("sent %d", count)
		case "close":
			_ = conn.Close()
		case "exit":
			os.Exit(0)
		default:
			out.emit("error unknown command %q", fields[0])
		}
	}
	// The test closed stdin: leave without a disconnect notification.
	os.Exit(0)
}
