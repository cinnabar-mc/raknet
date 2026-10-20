//! Session receive and send-scheduling costs; ids match the Go suite in tools/go-raknet.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use raknet::prelude::*;
use raknet::protocol::codec::RakCodec;
use raknet::protocol::packets::ack::Ack;
use raknet::protocol::packets::frame_set::FrameSet;
use raknet::protocol::types::frame::Frame;
use raknet::test_support::{negotiated_pair, pattern};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Payload each inbound fragment carries in both suites.
const SPLIT_FRAGMENT: usize = 1440;
const SPLIT_COUNTS: [usize; 5] = [1, 16, 256, 2048, 8192];
const SEND_QUEUED: [usize; 3] = [64, 512, 4096];

/// Counters the next inbound datagram must continue from.
struct Next {
    seq: u32,
    msg: u32,
    order: u32,
}

/// Negotiates a session and learns the peer's next datagram, reliable and order counters.
fn established() -> (RakSession<Established>, Instant, Next) {
    let client: SocketAddr = "10.0.0.2:50000".parse().unwrap();
    let server: SocketAddr = "10.0.0.1:19132".parse().unwrap();
    let now = Instant::now();
    let (mut local, mut remote) = negotiated_pair(client, server, now, RakSessionConfig::default());
    remote
        .send(
            Box::new([0xfe]),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    let mut next = None;
    while let Some(output) = remote.poll() {
        let RakSessionOutput::Datagram(bytes, _) = output else {
            continue;
        };
        if data_sequence(&bytes).is_some() {
            let set = FrameSet::deserialize(&mut &bytes[..]).unwrap();
            if let Some(frame) = set.frames.iter().find(|f| f.payload[..] == [0xfe]) {
                next = Some(Next {
                    seq: set.sequence + 1,
                    msg: frame.reliable_index + 1,
                    order: frame.order_index + 1,
                });
            }
        }
        local.handle(RakSessionInput::Datagram(bytes, now)).unwrap();
    }
    local.tick(now).unwrap();
    while local.poll().is_some() {}
    (local, now, next.expect("the probe message was sent"))
}

/// Builds n fragment datagrams whose counters each iteration patches in place.
fn split_template(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut frame = Frame::new(
                RakReliability::ReliableOrdered,
                pattern(SPLIT_FRAGMENT, i).into(),
            );
            frame.split_size = n as u32;
            frame.split_index = i as u32;
            let mut buf = Vec::new();
            FrameSet::new(0, vec![frame], false, true, false)
                .serialize(&mut buf)
                .unwrap();
            buf
        })
        .collect()
}

/// Rewrites a template fragment's datagram, message, order and split counters.
fn patch_split(d: &mut [u8], seq: u32, msg: u32, order: u32, id: u16) {
    d[1..4].copy_from_slice(&seq.to_le_bytes()[..3]);
    d[7..10].copy_from_slice(&msg.to_le_bytes()[..3]);
    d[10..13].copy_from_slice(&order.to_le_bytes()[..3]);
    d[18..20].copy_from_slice(&id.to_be_bytes());
}

fn split(c: &mut Criterion) {
    let mut group = c.benchmark_group("split");
    for n in SPLIT_COUNTS {
        group.throughput(Throughput::Bytes((n * SPLIT_FRAGMENT) as u64));
        group.bench_function(
            BenchmarkId::new("reassemble", format!("fragments_{n}")),
            |b| {
                let (mut session, now, next) = established();
                let mut datagrams = split_template(n);
                let (mut seq, mut msg, mut order) = (next.seq, next.msg, next.order);
                b.iter(|| {
                    for d in &mut datagrams {
                        patch_split(
                            d,
                            seq & 0xff_ffff,
                            msg & 0xff_ffff,
                            order & 0xff_ffff,
                            order as u16,
                        );
                        seq += 1;
                        msg += 1;
                        session
                            .handle(RakSessionInput::Datagram(d.as_slice().into(), now))
                            .unwrap();
                    }
                    order += 1;
                    let mut delivered = 0;
                    while let Some(output) = session.poll() {
                        if let RakSessionOutput::Packet(packet) = output {
                            delivered = packet.len();
                        }
                    }
                    assert_eq!(delivered, n * SPLIT_FRAGMENT);
                    // Acknowledge the run, as Go's send loop does on its own ticker.
                    session.tick(now).unwrap();
                    while session.poll().is_some() {}
                })
            },
        );
    }
    group.finish();
}

/// Returns the datagram sequence of a data datagram, skipping ACKs and NACKs.
fn data_sequence(datagram: &[u8]) -> Option<u32> {
    (datagram.len() >= 4 && datagram[0] & 0x80 != 0 && datagram[0] & 0x60 == 0)
        .then(|| u32::from_le_bytes([datagram[1], datagram[2], datagram[3], 0]))
}

fn send(c: &mut Criterion) {
    let mut group = c.benchmark_group("send");
    let message = pattern(200, 9);
    for n in SEND_QUEUED {
        group.throughput(Throughput::Bytes((n * message.len()) as u64));
        group.bench_function(BenchmarkId::new("schedule", format!("queued_{n}")), |b| {
            let (mut session, start, _) = established();
            let mtu = session.mtu() as usize;
            let mut now = start;
            let mut ack = Vec::with_capacity(1500);
            b.iter(|| {
                for _ in 0..n {
                    session
                        .send(
                            message.as_slice().into(),
                            RakReliability::ReliableOrdered,
                            RakPriority::Normal,
                            now,
                        )
                        .unwrap();
                }
                for pass in 0.. {
                    assert!(pass < 1_000_000, "send queue did not drain");
                    now += Duration::from_millis(1);
                    session.tick(now).unwrap();
                    let mut seqs = Vec::new();
                    while let Some(output) = session.poll() {
                        if let RakSessionOutput::Datagram(bytes, _) = output {
                            seqs.extend(data_sequence(&bytes));
                        }
                    }
                    for set in Ack::split(seqs, false, mtu) {
                        ack.clear();
                        set.serialize(&mut ack).unwrap();
                        session
                            .handle(RakSessionInput::Datagram(ack.as_slice().into(), now))
                            .unwrap();
                    }
                    if session.outbound_bytes() == 0 {
                        break;
                    }
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, split, send);
criterion_main!(benches);
