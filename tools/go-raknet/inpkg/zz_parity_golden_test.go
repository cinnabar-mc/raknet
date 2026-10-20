//go:build raknetparity

//go:debug asynctimerchan=0

package raknet

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

var parityUpdate = flag.Bool("update", false, "rewrite committed golden and bench fixtures")

const parityGoldenHeader = "# Generated from go-raknet by tools/go-raknet: `go run ./cmd/inpkg -run TestParityGolden -update`.\n"

var (
	parityClientAddr = &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2).To4(), Port: 50000}
	parityServerAddr = &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1).To4(), Port: 19132}
)

const (
	parityServerGUID = int64(0x0102030405060708)
	parityCookie     = uint32(0x0a0b0c0d)
	parityLatency    = time.Millisecond
)

// TestParityGoldenCodec records how go-raknet encodes and decodes each codec case.
func TestParityGoldenCodec(t *testing.T) {
	var out strings.Builder
	out.WriteString(parityGoldenHeader)
	out.WriteString("# kind name mode hex => go outcome\n")
	for _, c := range parityCodecCases() {
		fmt.Fprintf(&out, "%s %s %s %s => %s\n", c.kind, c.name, c.mode, hex.EncodeToString(c.data), parityDecode(c.kind, c.data))
	}
	parityCheckGolden(t, "testdata/golden/codec.txt", out.String())
}

type parityCodecCase struct {
	kind, name, mode string
	data             []byte
}

func parityFrame(pk packet) []byte {
	var buf bytes.Buffer
	pk.write(&buf)
	return buf.Bytes()
}

func parityDatagram(header byte, seq uint24, pks ...packet) []byte {
	var buf bytes.Buffer
	buf.WriteByte(header)
	writeUint24(&buf, seq)
	for _, pk := range pks {
		pk.write(&buf)
	}
	return buf.Bytes()
}

func parityAck(flag byte, seqs []uint24) [][]byte {
	var datagrams [][]byte
	ack := &acknowledgement{packets: append([]uint24(nil), seqs...)}
	for len(ack.packets) != 0 {
		var buf bytes.Buffer
		buf.WriteByte(flag | bitFlagDatagram)
		n := ack.write(&buf, 1492-28)
		ack.packets = ack.packets[n:]
		datagrams = append(datagrams, buf.Bytes())
	}
	return datagrams
}

func parityMarshal(pk interface{ MarshalBinary() ([]byte, error) }) []byte {
	b, _ := pk.MarshalBinary()
	return b
}

func parityAddrBytes(addr netip.AddrPort) []byte {
	sys := message.NewSystemAddresses(addr)
	pk := &message.NewIncomingConnection{ServerAddress: addr, SystemAddresses: sys}
	b := parityMarshal(pk)
	// The server address directly follows the message ID.
	if addr.Addr().Is4() {
		return b[1:8]
	}
	return b[1:30]
}

// parityCodecCases lists every committed codec fixture; mode is roundtrip or decode.
func parityCodecCases() []parityCodecCase {
	payload := []byte("parity")
	v4 := netip.MustParseAddrPort("127.0.0.1:19132")
	v6 := netip.MustParseAddrPort("[::1]:19132")
	server := netip.MustParseAddrPort("10.0.0.1:19132")
	client := netip.MustParseAddrPort("10.0.0.2:50000")
	cases := []parityCodecCase{
		{"frame", "unreliable", "roundtrip", parityFrame(packet{reliability: reliabilityUnreliable, content: payload})},
		{"frame", "unreliable_sequenced", "roundtrip", parityFrame(packet{reliability: reliabilityUnreliableSequenced, sequenceIndex: 4, orderIndex: 9, content: payload})},
		{"frame", "reliable", "roundtrip", parityFrame(packet{reliability: reliabilityReliable, messageIndex: 7, content: payload})},
		{"frame", "reliable_ordered", "roundtrip", parityFrame(packet{reliability: reliabilityReliableOrdered, messageIndex: 1, orderIndex: 2, content: payload})},
		{"frame", "reliable_sequenced", "roundtrip", parityFrame(packet{reliability: reliabilityReliableSequenced, messageIndex: 3, sequenceIndex: 5, orderIndex: 6, content: payload})},
		{"frame", "u24_max_indices", "roundtrip", parityFrame(packet{reliability: reliabilityReliableOrdered, messageIndex: 0xffffff, orderIndex: 0xfffffe, content: payload})},
		{"frame", "split_header", "roundtrip", parityFrame(packet{reliability: reliabilityReliableOrdered, messageIndex: 9, orderIndex: 1, split: true, splitCount: 3, splitID: 7, splitIndex: 1, content: payload})},
		{"frame", "split_header_max_count", "roundtrip", parityFrame(packet{reliability: reliabilityReliableOrdered, split: true, splitCount: 8192, splitID: 0xffff, splitIndex: 8191, content: payload})},
		{"frame", "split_header_count_zero", "decode", parityFrame(packet{reliability: reliabilityReliableOrdered, split: true, splitCount: 0, splitID: 2, splitIndex: 0, content: payload})},
		{"frame", "empty_payload", "roundtrip", parityFrame(packet{reliability: reliabilityReliableOrdered, messageIndex: 4, orderIndex: 4})},
		{"frame", "bit_length_not_byte_aligned", "decode", []byte{0x00, 0x00, 0x43, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"frame", "truncated_payload", "decode", []byte{0x00, 0x00, 0x50, 1, 2, 3}},
		{"frame", "truncated_header", "decode", []byte{0x60, 0x00, 0x30, 0x01}},
		{"datagram", "bundle", "roundtrip", parityDatagram(bitFlagDatagram|bitFlagNeedsBAndAS, 5,
			packet{reliability: reliabilityReliableOrdered, messageIndex: 0, orderIndex: 0, content: payload},
			packet{reliability: reliabilityUnreliable, content: []byte{0x00, 1, 2, 3, 4, 5, 6, 7, 8}},
			packet{reliability: reliabilityReliable, messageIndex: 1, content: payload})},
		{"datagram", "continuous_send", "roundtrip", parityDatagram(bitFlagDatagram|bitFlagContinuousSend, 0xffffff,
			packet{reliability: reliabilityReliableOrdered, messageIndex: 2, orderIndex: 1, content: payload})},
		{"datagram", "no_frames", "roundtrip", parityDatagram(bitFlagDatagram, 1)},
		{"datagram", "trailing_garbage", "decode", append(parityDatagram(bitFlagDatagram, 2, packet{reliability: reliabilityUnreliable, content: payload}), 0x60)},
		{"datagram", "short_header", "decode", []byte{bitFlagDatagram, 1}},
		{"ack", "single", "roundtrip", parityAck(bitFlagACK, []uint24{5})[0]},
		{"ack", "range", "roundtrip", parityAck(bitFlagACK, []uint24{5, 6, 7, 8})[0]},
		{"ack", "mixed", "roundtrip", parityAck(bitFlagACK, []uint24{1, 3, 4, 5, 9, 0xffffff})[0]},
		{"ack", "many_ranges", "roundtrip", parityAck(bitFlagACK, parityRanges(300, 2, 5))[0]},
		{"ack", "nack_mixed", "roundtrip", parityAck(bitFlagNACK, []uint24{2, 10, 11, 12})[0]},
		{"ack", "max_entries", "decode", []byte{bitFlagDatagram | bitFlagACK, 0, 1, packetRange, 0, 0, 0, 0xff, 0x1f, 0}},
		{"ack", "over_max_entries", "decode", []byte{bitFlagDatagram | bitFlagACK, 0, 1, packetRange, 0, 0, 0, 0x00, 0x20, 0}},
		{"ack", "reversed_range", "decode", []byte{bitFlagDatagram | bitFlagACK, 0, 1, packetRange, 9, 0, 0, 3, 0, 0}},
		{"ack", "unknown_record_type", "decode", []byte{bitFlagDatagram | bitFlagACK, 0, 1, 2, 9, 0, 0}},
		{"ack", "empty", "decode", []byte{bitFlagDatagram | bitFlagACK, 0, 0}},
		{"ack", "truncated", "decode", []byte{bitFlagDatagram | bitFlagACK, 0, 2, packetSingle, 9, 0, 0}},
		{"addr", "ipv4", "roundtrip", parityAddrBytes(v4)},
		{"addr", "ipv6", "roundtrip", parityAddrBytes(v6)},
		{"addr", "ipv4_family_zero", "decode", append([]byte{0}, parityAddrBytes(v4)[1:]...)},
		{"addr", "unknown_family", "decode", append([]byte{9}, parityAddrBytes(v6)[1:]...)},
		{"cra", "local_ipv4", "roundtrip", parityMarshal(&message.ConnectionRequestAccepted{ClientAddress: client, SystemAddresses: message.NewLocalSystemAddresses(server), PingTime: 11, PongTime: 22})},
		{"cra", "local_ipv6", "roundtrip", parityMarshal(&message.ConnectionRequestAccepted{ClientAddress: v6, SystemIndex: 3, SystemAddresses: message.NewLocalSystemAddresses(v6), PingTime: 11, PongTime: 22})},
		{"cra", "ten_addresses", "decode", parityAddressList(message.IDConnectionRequestAccepted, true, 10, 4)},
		{"cra", "twenty_one_addresses", "decode", parityAddressList(message.IDConnectionRequestAccepted, true, 21, 4)},
		{"cra", "unknown_family_address", "decode", parityAddressList(message.IDConnectionRequestAccepted, true, 2, 9)},
		{"nic", "local_ipv4", "roundtrip", parityMarshal(&message.NewIncomingConnection{ServerAddress: server, SystemAddresses: message.NewSystemAddresses(client), PingTime: 33, PongTime: 44})},
		{"nic", "twenty_one_addresses", "decode", parityAddressList(message.IDNewIncomingConnection, false, 21, 4)},
		{"pong", "with_data", "roundtrip", parityMarshal(&message.UnconnectedPong{PingTime: 99, ServerGUID: parityServerGUID, Data: []byte("MCPE;parity;")})},
		{"pong", "empty_data", "roundtrip", parityMarshal(&message.UnconnectedPong{PingTime: 99, ServerGUID: parityServerGUID})},
		{"pong", "bds_no_length_field", "decode", parityMarshal(&message.UnconnectedPong{PingTime: 99, ServerGUID: parityServerGUID})[:33]},
		{"pong", "bad_magic", "decode", parityBadMagicPong()},
		{"pong", "truncated_data", "decode", parityMarshal(&message.UnconnectedPong{PingTime: 99, ServerGUID: parityServerGUID, Data: []byte("MCPE")})[:37]},
	}
	return cases
}

func parityBadMagicPong() []byte {
	b := parityMarshal(&message.UnconnectedPong{PingTime: 1, ServerGUID: 2, Data: []byte("x")})
	b[17] ^= 0xff
	return b
}

// parityRanges builds count ranges of width seqs, each starting stride apart.
func parityRanges(count, width, stride int) []uint24 {
	var seqs []uint24
	for i := range count {
		for j := range width {
			seqs = append(seqs, uint24(i*stride+j))
		}
	}
	return seqs
}

// parityAddressList builds a handshake body with count addresses of the given family byte.
func parityAddressList(id byte, systemIndex bool, count int, family byte) []byte {
	out := []byte{id}
	out = append(out, parityAddrBytes(netip.MustParseAddrPort("10.0.0.2:50000"))...)
	if systemIndex {
		out = append(out, 0, 0)
	}
	for i := range count {
		addr := parityAddrBytes(netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(i)}), uint16(1000+i)))
		if family != 4 {
			addr = parityAddrBytes(netip.MustParseAddrPort("[2001:db8::1]:1000"))
			addr[0] = family
		}
		out = append(out, addr...)
	}
	out = binary.BigEndian.AppendUint64(out, 55)
	return binary.BigEndian.AppendUint64(out, 66)
}

// parityDecode renders go-raknet's interpretation of one fixture.
func parityDecode(kind string, b []byte) (outcome string) {
	defer func() {
		if r := recover(); r != nil {
			outcome = "panic"
		}
	}()
	switch kind {
	case "frame":
		var pk packet
		n, err := pk.read(b)
		if err != nil {
			return "err"
		}
		return fmt.Sprintf("ok n=%d %s", n, parityRenderFrame(&pk))
	case "datagram":
		if len(b) < 4 || b[0]&bitFlagDatagram == 0 {
			return "err"
		}
		flags, seq := b[0], loadUint24(b[1:])
		var frames []string
		rest := b[4:]
		for len(rest) > 0 {
			var pk packet
			n, err := pk.read(rest)
			if err != nil {
				return "err"
			}
			frames = append(frames, parityRenderFrame(&pk))
			rest = rest[n:]
		}
		return fmt.Sprintf("ok flags=%02x seq=%d frames=[%s]", flags, seq, strings.Join(frames, "; "))
	case "ack":
		nack := 0
		switch {
		case b[0]&bitFlagACK != 0:
		case b[0]&bitFlagNACK != 0:
			nack = 1
		default:
			return "err"
		}
		ack := &acknowledgement{}
		if err := ack.read(b[1:]); err != nil {
			return "err"
		}
		return fmt.Sprintf("ok nack=%d seqs=%s", nack, parityRenderSeqs(ack.packets))
	case "addr":
		// The address codec is private to the message package; decode it as a NIC's leading field.
		n := 29
		if b[0] == 4 || b[0] == 0 {
			n = 7
		}
		var pk message.NewIncomingConnection
		if err := pk.UnmarshalBinary(append(bytes.Clone(b[:n]), make([]byte, 16)...)); err != nil {
			return "err"
		}
		return fmt.Sprintf("ok n=%d addr=%s", n, pk.ServerAddress)
	case "cra":
		var pk message.ConnectionRequestAccepted
		if b[0] != message.IDConnectionRequestAccepted || pk.UnmarshalBinary(b[1:]) != nil {
			return "err"
		}
		return fmt.Sprintf("ok client=%s index=%d sys=[%s] ping=%d pong=%d", pk.ClientAddress, pk.SystemIndex, parityRenderAddrs(pk.SystemAddresses[:]), pk.PingTime, pk.PongTime)
	case "nic":
		var pk message.NewIncomingConnection
		if b[0] != message.IDNewIncomingConnection || pk.UnmarshalBinary(b[1:]) != nil {
			return "err"
		}
		return fmt.Sprintf("ok server=%s sys=[%s] ping=%d pong=%d", pk.ServerAddress, parityRenderAddrs(pk.SystemAddresses[:]), pk.PingTime, pk.PongTime)
	case "pong":
		var pk message.UnconnectedPong
		if b[0] != message.IDUnconnectedPong || pk.UnmarshalBinary(b[1:]) != nil {
			return "err"
		}
		return fmt.Sprintf("ok ping=%d guid=%d data=%s", pk.PingTime, pk.ServerGUID, hex.EncodeToString(pk.Data))
	}
	panic("unknown kind " + kind)
}

func parityRenderFrame(pk *packet) string {
	split := 0
	if pk.split {
		split = 1
	}
	return fmt.Sprintf("rel=%d msg=%d seq=%d ord=%d split=%d:%d/%d/%d payload=%s",
		pk.reliability, pk.messageIndex, pk.sequenceIndex, pk.orderIndex, split, pk.splitCount, pk.splitID, pk.splitIndex, hex.EncodeToString(pk.content))
}

// parityRenderSeqs compresses sorted sequence numbers into first-last runs.
func parityRenderSeqs(seqs []uint24) string {
	if len(seqs) == 0 {
		return "-"
	}
	var parts []string
	start, end := seqs[0], seqs[0]
	flush := func() {
		if start == end {
			parts = append(parts, fmt.Sprint(start))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", start, end))
		}
	}
	for _, s := range seqs[1:] {
		if s == end+1 {
			end = s
			continue
		}
		flush()
		start, end = s, s
	}
	flush()
	return strings.Join(parts, ",")
}

// parityRenderAddrs lists decoded addresses, omitting the unset tail of a short list.
func parityRenderAddrs(addrs []netip.AddrPort) string {
	last := len(addrs)
	for last > 0 && !addrs[last-1].IsValid() {
		last--
	}
	parts := make([]string, last)
	for i, a := range addrs[:last] {
		parts[i] = a.String()
	}
	return strings.Join(parts, ",")
}

// paritySplitCounts are the reassembly sizes both benchmark suites cover.
var paritySplitCounts = []int{1, 16, 256, 2048, 8192}

// TestParityGoldenBenchInputs records the shared benchmark inputs.
func TestParityGoldenBenchInputs(t *testing.T) {
	for _, in := range parityBenchInputs() {
		parityCheckRecords(t, in.name, in.records)
	}
}

type parityBenchInput struct {
	name    string
	records [][]byte
}

func parityBenchInputs() []parityBenchInput {
	frames := []packet{}
	for i := range 16 {
		frames = append(frames, packet{reliability: reliabilityReliableOrdered, messageIndex: uint24(i), orderIndex: uint24(i), content: parityPayload(64, i)})
	}
	return []parityBenchInput{
		{"frame_unreliable_64.bin", [][]byte{parityFrame(packet{reliability: reliabilityUnreliable, content: parityPayload(64, 0)})}},
		{"frame_reliable_ordered_1024.bin", [][]byte{parityFrame(packet{reliability: reliabilityReliableOrdered, messageIndex: 77, orderIndex: 12, content: parityPayload(1024, 1)})}},
		{"frame_split_1400.bin", [][]byte{parityFrame(packet{reliability: reliabilityReliableOrdered, messageIndex: 78, orderIndex: 13, split: true, splitCount: 40, splitID: 3, splitIndex: 17, content: parityPayload(1400, 2)})}},
		{"frameset_bundle_16x64.bin", [][]byte{parityDatagram(bitFlagDatagram|bitFlagNeedsBAndAS, 100, frames...)}},
		{"frameset_single_1400.bin", [][]byte{parityDatagram(bitFlagDatagram|bitFlagNeedsBAndAS, 101, packet{reliability: reliabilityReliableOrdered, messageIndex: 5, orderIndex: 5, content: parityPayload(1400, 3)})}},
		{"ack_ranges_1.bin", parityAck(bitFlagACK, parityRanges(1, 1000, 1000))},
		{"ack_ranges_512.bin", parityAck(bitFlagACK, parityRanges(512, 2, 3))},
		{"ack_singles_2048.bin", parityAck(bitFlagACK, parityRanges(2048, 1, 2))},
		{"nack_singles_2048.bin", parityAck(bitFlagNACK, parityRanges(2048, 1, 2))},
	}
}

// TestParityGoldenHandshake records go-raknet's handshake datagrams against scripted peers.
func TestParityGoldenHandshake(t *testing.T) {
	for _, scenario := range []parityDialScenario{
		{name: "dial_ladder_cookie", security: true, maxProbe: 1200},
		{name: "dial_plain", maxProbe: 1492},
		{name: "dial_broken_reply_mtu", maxProbe: 1492, brokenFirst: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var text string
			synctest.Test(t, func(t *testing.T) { text = parityRecordDial(t, scenario) })
			parityCheckGolden(t, "testdata/golden/handshake_"+scenario.name+".txt", text)
		})
	}
	for _, scenario := range []parityListenScenario{
		{name: "listen_plain"},
		{name: "listen_cookie", cookies: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var text string
			synctest.Test(t, func(t *testing.T) { text = parityRecordListen(t, scenario) })
			parityCheckGolden(t, "testdata/golden/handshake_"+scenario.name+".txt", text)
		})
	}
}

type parityDialScenario struct {
	name        string
	security    bool
	maxProbe    uint16 // Request 1 probes above this MTU are dropped.
	brokenFirst bool   // The first answered probe gets a Reply 1 with an out-of-range MTU.
}

// parityScript reacts to one peer's datagrams; replies carry the index of the non-ACK datagram that triggered them.
type parityScript struct {
	transcript *parityTranscript
	deliver    func([]byte)
	mu         sync.Mutex
	trigger    int
	link       parityLink
	seq        uint24
	msgIndex   uint24
	done       chan struct{}
	doneOnce   sync.Once
}

func (s *parityScript) reply(data []byte) {
	trigger := s.trigger
	s.link.send(func() {
		s.transcript.addReply(trigger, data)
		s.deliver(data)
	})
}

// parityLink delivers datagrams after the link latency in exactly the order they were sent.
type parityLink struct {
	mu, delivering sync.Mutex
	queue          []func()
}

func (l *parityLink) send(fn func()) {
	l.mu.Lock()
	l.queue = append(l.queue, fn)
	l.mu.Unlock()
	time.AfterFunc(parityLatency, l.pop)
}

func (l *parityLink) pop() {
	l.delivering.Lock()
	defer l.delivering.Unlock()
	l.mu.Lock()
	fn := l.queue[0]
	l.queue = l.queue[1:]
	l.mu.Unlock()
	fn()
}

func (s *parityScript) datagram(pks ...packet) []byte {
	seq := s.seq.Inc()
	for i := range pks {
		if pks[i].reliability.reliable() {
			pks[i].messageIndex = s.msgIndex.Inc()
		}
	}
	return parityDatagram(bitFlagDatagram|bitFlagNeedsBAndAS, seq, pks...)
}

func (t *parityTranscript) addReply(trigger int, data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, fmt.Sprintf("s2c %d %d %s", time.Since(t.start).Milliseconds(), trigger, t.mask(data)))
}

// isAckDatagram reports whether b is a pure ACK or NACK, which replays do not align on.
func isAckDatagram(b []byte) bool {
	return len(b) > 0 && b[0]&bitFlagDatagram != 0 && b[0]&(bitFlagACK|bitFlagNACK) != 0
}

// parityDatagramPayloads decodes the frames of one connected datagram.
func parityDatagramPayloads(b []byte) []packet {
	var pks []packet
	rest := b[4:]
	for len(rest) > 0 {
		var pk packet
		n, err := pk.read(rest)
		if err != nil {
			return pks
		}
		pks = append(pks, pk)
		rest = rest[n:]
	}
	return pks
}

func parityRecordDial(t *testing.T, scenario parityDialScenario) string {
	transcript := &parityTranscript{start: time.Now()}
	script := &parityScript{transcript: transcript, done: make(chan struct{})}
	brokenSent := false
	var conn *parityPacketConn
	conn = newParityPacketConn(parityClientAddr, parityServerAddr, func(b []byte, _ net.Addr) {
		script.mu.Lock()
		defer script.mu.Unlock()
		select {
		case <-script.done:
			return
		default:
		}
		transcript.add("c2s", b)
		if !isAckDatagram(b) {
			defer func() { script.trigger++ }()
		}
		switch {
		case b[0] == message.IDOpenConnectionRequest1:
			mtu := uint16(len(b) + 28)
			if mtu > scenario.maxProbe {
				return
			}
			reply := &message.OpenConnectionReply1{ServerGUID: parityServerGUID, ServerHasSecurity: scenario.security, Cookie: parityCookie, MTU: mtu}
			if scenario.brokenFirst && !brokenSent {
				brokenSent = true
				reply.MTU = 0xffff
			}
			script.reply(parityMarshal(reply))
		case b[0] == message.IDOpenConnectionRequest2:
			pk := &message.OpenConnectionRequest2{ServerHasSecurity: scenario.security}
			if pk.UnmarshalBinary(b[1:]) != nil || pk.MTU == 0xffff {
				return
			}
			client, _ := netip.ParseAddrPort(parityClientAddr.String())
			script.reply(parityMarshal(&message.OpenConnectionReply2{ServerGUID: parityServerGUID, ClientAddress: client, MTU: pk.MTU}))
		case b[0]&bitFlagDatagram != 0 && !isAckDatagram(b):
			script.reply(parityAck(bitFlagACK, []uint24{loadUint24(b[1:])})[0])
			for _, pk := range parityDatagramPayloads(b) {
				switch pk.content[0] {
				case message.IDConnectionRequest:
					req := &message.ConnectionRequest{}
					_ = req.UnmarshalBinary(pk.content[1:])
					client, _ := netip.ParseAddrPort(parityClientAddr.String())
					server, _ := netip.ParseAddrPort(parityServerAddr.String())
					cra := parityMarshal(&message.ConnectionRequestAccepted{ClientAddress: client, SystemAddresses: message.NewLocalSystemAddresses(server), PingTime: req.RequestTime, PongTime: 1000})
					script.reply(script.datagram(packet{reliability: reliabilityReliableOrdered, orderIndex: 0, content: cra}))
				case message.IDNewIncomingConnection:
					time.AfterFunc(100*time.Millisecond, func() { script.doneOnce.Do(func() { close(script.done) }) })
				}
			}
		}
	})
	script.deliver = func(b []byte) { conn.deliver(b, parityServerAddr) }

	dialer := Dialer{UpstreamDialer: parityDialer{conn: conn}, ErrorLog: parityDiscardLog()}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rak, err := dialer.DialContext(ctx, parityServerAddr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	<-script.done
	rak.closeImmediately()
	_ = conn.Close()
	return transcript.text(parityGoldenHeader + "# scenario " + scenario.name + "\n# c2s <ms> <hex> | s2c <ms> <trigger> <hex>\n")
}

type parityListenScenario struct {
	name    string
	cookies bool
}

func parityRecordListen(t *testing.T, scenario parityListenScenario) string {
	transcript := &parityTranscript{start: time.Now(), maskCookie: true}
	script := &parityScript{transcript: transcript, done: make(chan struct{})}
	var conn *parityPacketConn
	clientGUID := int64(-0x1122334455667788)
	sendClient := func(b []byte) {
		transcript.add("c2s", b)
		if !isAckDatagram(b) {
			script.trigger++
		}
		conn.deliver(b, parityClientAddr)
	}
	// Server output arrives at the scripted client after the link latency.
	conn = newParityPacketConn(parityServerAddr, nil, func(b []byte, _ net.Addr) {
		script.mu.Lock()
		trigger := script.trigger - 1
		script.mu.Unlock()
		script.link.send(func() {
			script.mu.Lock()
			defer script.mu.Unlock()
			select {
			case <-script.done:
				return
			default:
			}
			transcript.addReply(trigger, b)
			switch {
			case b[0] == message.IDOpenConnectionReply1:
				reply := &message.OpenConnectionReply1{}
				_ = reply.UnmarshalBinary(b[1:])
				server, _ := netip.ParseAddrPort(parityServerAddr.String())
				sendClient(parityMarshal(&message.OpenConnectionRequest2{ServerAddress: server, MTU: reply.MTU, ClientGUID: clientGUID, ServerHasSecurity: reply.ServerHasSecurity, Cookie: reply.Cookie}))
			case b[0] == message.IDOpenConnectionReply2:
				sendClient(script.datagram(packet{reliability: reliabilityReliableOrdered, content: parityMarshal(&message.ConnectionRequest{ClientGUID: clientGUID, RequestTime: 500})}))
			case b[0]&bitFlagDatagram != 0 && !isAckDatagram(b):
				sendClient(parityAck(bitFlagACK, []uint24{loadUint24(b[1:])})[0])
				for _, pk := range parityDatagramPayloads(b) {
					if pk.content[0] == message.IDConnectionRequestAccepted {
						server, _ := netip.ParseAddrPort(parityServerAddr.String())
						sendClient(script.datagram(packet{reliability: reliabilityReliableOrdered, orderIndex: 1, content: parityMarshal(&message.NewIncomingConnection{ServerAddress: server, SystemAddresses: message.NewSystemAddresses(netip.MustParseAddrPort(parityClientAddr.String())), PingTime: 1000, PongTime: 1001})}))
						time.AfterFunc(100*time.Millisecond, func() { script.doneOnce.Do(func() { close(script.done) }) })
					}
				}
			}
		})
	})
	listener, err := ListenConfig{UpstreamPacketListener: parityListenPacket{conn}, ServerID: parityServerGUID, DisableCookies: !scenario.cookies, ErrorLog: parityDiscardLog()}.Listen(parityServerAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	script.mu.Lock()
	sendClient(parityMarshal(&message.OpenConnectionRequest1{ClientProtocol: protocolVersion, MTU: 1492}))
	script.mu.Unlock()
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	<-script.done
	accepted.(*Conn).closeImmediately()
	_ = listener.Close()
	return transcript.text(parityGoldenHeader + "# scenario " + scenario.name + "\n# c2s <ms> <hex> | s2c <ms> <trigger> <hex>\n")
}

type parityListenPacket struct{ conn *parityPacketConn }

func (l parityListenPacket) ListenPacket(string, string) (net.PacketConn, error) { return l.conn, nil }
