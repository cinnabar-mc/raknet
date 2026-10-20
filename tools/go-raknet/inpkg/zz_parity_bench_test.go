//go:build raknetparity

package raknet

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// Benchmark names match the criterion `group/function/case` ids in raknet/benches.

var parityFrameInputs = []string{"unreliable_64", "reliable_ordered_1024", "split_1400"}

func BenchmarkFrame(b *testing.B) {
	for _, name := range parityFrameInputs {
		input := parityReadRecords(b, "frame_"+name+".bin")[0]
		b.Run("parse/"+name, func(b *testing.B) {
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			var pk packet
			for b.Loop() {
				if _, err := pk.read(input); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("serialize/"+name, func(b *testing.B) {
			var pk packet
			if _, err := pk.read(input); err != nil {
				b.Fatal(err)
			}
			buf := bytes.NewBuffer(make([]byte, 0, 1500))
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			for b.Loop() {
				buf.Reset()
				pk.write(buf)
			}
		})
	}
}

var parityFramesetInputs = []string{"bundle_16x64", "single_1400"}

// BenchmarkFrameset measures the datagram body loop conn.go runs per received or sent datagram.
func BenchmarkFrameset(b *testing.B) {
	for _, name := range parityFramesetInputs {
		input := parityReadRecords(b, "frameset_"+name+".bin")[0]
		b.Run("parse/"+name, func(b *testing.B) {
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			pk := new(packet)
			for b.Loop() {
				if input[0]&bitFlagDatagram == 0 || len(input) < 4 {
					b.Fatal("not a datagram")
				}
				_ = loadUint24(input[1:])
				rest := input[4:]
				for len(rest) > 0 {
					n, err := pk.read(rest)
					if err != nil {
						b.Fatal(err)
					}
					rest = rest[n:]
				}
			}
		})
		b.Run("serialize/"+name, func(b *testing.B) {
			pks := parityDatagramPayloads(input)
			seq := uint24(loadUint24(input[1:]))
			buf := bytes.NewBuffer(make([]byte, 0, 1500))
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			for b.Loop() {
				buf.Reset()
				buf.WriteByte(input[0])
				writeUint24(buf, seq)
				for i := range pks {
					pks[i].write(buf)
				}
			}
		})
	}
}

var parityAckInputs = []string{"ack_ranges_1", "ack_ranges_512", "ack_singles_2048", "nack_singles_2048"}

// BenchmarkAck encodes a sequence set into MTU-bounded datagrams and decodes them back.
func BenchmarkAck(b *testing.B) {
	for _, name := range parityAckInputs {
		datagrams := parityReadRecords(b, name+".bin")
		var seqs []uint24
		total := 0
		for _, d := range datagrams {
			ack := &acknowledgement{}
			if err := ack.read(d[1:]); err != nil {
				b.Fatal(err)
			}
			seqs = append(seqs, ack.packets...)
			total += len(d)
		}
		flag := datagrams[0][0] &^ bitFlagDatagram
		b.Run("decode/"+name, func(b *testing.B) {
			b.SetBytes(int64(total))
			b.ReportAllocs()
			for b.Loop() {
				for _, d := range datagrams {
					ack := &acknowledgement{}
					if err := ack.read(d[1:]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		b.Run("encode/"+name, func(b *testing.B) {
			buf := bytes.NewBuffer(make([]byte, 0, 1500))
			scratch := make([]uint24, len(seqs))
			b.SetBytes(int64(total))
			b.ReportAllocs()
			for b.Loop() {
				// sendAcknowledgement consumes its slice, so each pass starts from a fresh copy.
				copy(scratch, seqs)
				ack := &acknowledgement{packets: scratch}
				for len(ack.packets) != 0 {
					buf.WriteByte(flag | bitFlagDatagram)
					n := ack.write(buf, 1492-28)
					ack.packets = ack.packets[n:]
					buf.Reset()
				}
			}
		})
	}
}

// paritySink is a socket that keeps only the sequence numbers of written data datagrams.
type paritySink struct {
	mu   sync.Mutex
	seqs []uint24
}

func (s *paritySink) WriteTo(b []byte, _ net.Addr) (int, error) {
	if len(b) >= 4 && b[0]&bitFlagDatagram != 0 && b[0]&(bitFlagACK|bitFlagNACK) == 0 {
		s.mu.Lock()
		s.seqs = append(s.seqs, loadUint24(b[1:]))
		s.mu.Unlock()
	}
	return len(b), nil
}

func (s *paritySink) take() []uint24 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seqs := s.seqs
	s.seqs = nil
	return seqs
}

func (*paritySink) ReadFrom([]byte) (int, net.Addr, error) { select {} }
func (*paritySink) Close() error                           { return nil }
func (*paritySink) LocalAddr() net.Addr                    { return parityClientAddr }
func (*paritySink) SetDeadline(time.Time) error            { return nil }
func (*paritySink) SetReadDeadline(time.Time) error        { return nil }
func (*paritySink) SetWriteDeadline(time.Time) error       { return nil }

func parityBenchConn(b *testing.B) (*Conn, *paritySink) {
	sink := &paritySink{}
	conn := newConn(sink, parityServerAddr, 1492, dialerConnectionHandler{l: parityDiscardLog()})
	b.Cleanup(conn.closeImmediately)
	return conn, sink
}

// paritySplitFragment is the payload each inbound fragment carries in both suites.
const paritySplitFragment = 1440

// paritySplitTemplate builds n fragment datagrams whose counters each iteration patches in place.
func paritySplitTemplate(n int) [][]byte {
	datagrams := make([][]byte, n)
	for i := range n {
		datagrams[i] = parityDatagram(bitFlagDatagram|bitFlagNeedsBAndAS, 0, packet{
			reliability: reliabilityReliableOrdered, split: true, splitCount: uint32(n), splitIndex: uint32(i),
			content: parityPayload(paritySplitFragment, i),
		})
	}
	return datagrams
}

// parityPatchSplit rewrites a template fragment's datagram, message, order and split counters.
func parityPatchSplit(d []byte, seq, msg, order uint32, id uint16) {
	put24 := func(at int, v uint32) { d[at], d[at+1], d[at+2] = byte(v), byte(v>>8), byte(v>>16) }
	put24(1, seq)
	put24(7, msg)
	put24(10, order)
	d[18], d[19] = byte(id>>8), byte(id)
}

func BenchmarkSplit(b *testing.B) {
	for _, n := range paritySplitCounts {
		b.Run(fmt.Sprintf("reassemble/fragments_%d", n), func(b *testing.B) {
			conn, _ := parityBenchConn(b)
			datagrams := paritySplitTemplate(n)
			var seq, msg, order uint32
			ctx := context.Background()
			b.SetBytes(int64(n * paritySplitFragment))
			b.ReportAllocs()
			for b.Loop() {
				for _, d := range datagrams {
					parityPatchSplit(d, seq&0xffffff, msg&0xffffff, order&0xffffff, uint16(order))
					seq++
					msg++
					if err := conn.receive(d); err != nil {
						b.Fatal(err)
					}
				}
				order++
				if message, ok := conn.packets.Recv(ctx); !ok || len(message) != n*paritySplitFragment {
					b.Fatalf("reassembled %d bytes", len(message))
				}
			}
		})
	}
}

// paritySendQueued are the application backlogs the scheduler benchmark drains.
var paritySendQueued = []int{64, 512, 4096}

// BenchmarkSend queues a backlog of 200 byte reliable ordered messages, then
// drains it through send passes that are acknowledged immediately.
func BenchmarkSend(b *testing.B) {
	message := parityPayload(200, 9)
	for _, n := range paritySendQueued {
		b.Run(fmt.Sprintf("schedule/queued_%d", n), func(b *testing.B) {
			conn, sink := parityBenchConn(b)
			ack := bytes.NewBuffer(make([]byte, 0, 1500))
			b.SetBytes(int64(n * len(message)))
			b.ReportAllocs()
			for b.Loop() {
				conn.mu.Lock()
				for range n {
					if _, err := conn.write(message, reliabilityReliableOrdered, false); err != nil {
						b.Fatal(err)
					}
				}
				conn.mu.Unlock()
				for {
					conn.update(time.Now())
					if seqs := sink.take(); len(seqs) != 0 {
						a := &acknowledgement{packets: seqs}
						for len(a.packets) != 0 {
							ack.Reset()
							k := a.write(ack, conn.effectiveMTU())
							a.packets = a.packets[k:]
							if err := conn.handleACK(ack.Bytes()); err != nil {
								b.Fatal(err)
							}
						}
					}
					conn.mu.Lock()
					idle := len(conn.sendQueue) == 0 && len(conn.retransmission.unacknowledged) == 0
					conn.mu.Unlock()
					if idle {
						break
					}
				}
			}
		})
	}
}
