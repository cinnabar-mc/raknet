package goraknet_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/sandertv/go-raknet"
)

// loopbackPair connects a go-raknet dialer to a listener on IPv4 loopback.
func loopbackPair(b *testing.B) (client, server *raknet.Conn) {
	listener, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan *raknet.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn.(*raknet.Conn)
		}
		close(accepted)
	}()
	client, err = raknet.Dialer{ErrorLog: slog.New(slog.DiscardHandler)}.Dial(listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	server = <-accepted
	if server == nil {
		b.Fatal("accept failed")
	}
	b.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

// BenchmarkLoopback matches raknet-tokio/benches/loopback.rs.
func BenchmarkLoopback(b *testing.B) {
	b.Run("rtt_64b", func(b *testing.B) {
		client, server := loopbackPair(b)
		go echo(server)
		message := bytes.Repeat([]byte{0xfe}, 64)
		b.SetBytes(int64(len(message)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := client.Write(message); err != nil {
				b.Fatal(err)
			}
			if _, err := client.ReadPacket(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("message_2mib", func(b *testing.B) {
		client, server := loopbackPair(b)
		go func() {
			for {
				message, err := server.ReadPacket()
				if err != nil {
					return
				}
				if _, err := server.Write(message[:1]); err != nil {
					return
				}
			}
		}()
		message := bytes.Repeat([]byte{0xfe}, 2<<20)
		b.SetBytes(int64(len(message)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := client.Write(message); err != nil {
				b.Fatal(err)
			}
			if _, err := client.ReadPacket(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func echo(conn *raknet.Conn) {
	for {
		message, err := conn.ReadPacket()
		if err != nil {
			return
		}
		if _, err := conn.Write(message); err != nil {
			return
		}
	}
}
