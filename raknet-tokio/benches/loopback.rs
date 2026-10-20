//! Loopback round trip and bulk transfer between Tokio endpoints; ids match the Go suite in tools/go-raknet.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use raknet_tokio::prelude::*;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

/// Starts a server whose sessions reply to each message with `reply(len)` bytes, then connects to it.
async fn pair(
    reply: fn(usize) -> usize,
) -> (
    RakClient<RunningClient>,
    RakServer<RunningServer>,
    RakSession<Established>,
) {
    // The adapter reports no bound port, so reserve a free one first.
    let addr: SocketAddr = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|s| s.local_addr())
        .unwrap();
    // go-raknet has no per-address datagram rate limit; the default one ends a bulk loopback session.
    let mut server = RakServer::new(addr, |c| {
        c.max_connections = 1;
        c.online_packet_limit = i32::MAX;
    })
    .start()
    .await
    .unwrap();
    let client = RakClient::new(|_| {}).start().await.unwrap();
    let (session, accepted) = tokio::join!(client.connect(addr), server.accept());
    let mut accepted = accepted.unwrap();
    tokio::spawn(async move {
        while let Ok(message) = accepted.recv::<Box<[u8]>>().await {
            let body = message[..reply(message.len())].to_vec();
            if accepted
                .send(body, RakReliability::ReliableOrdered, RakPriority::Normal)
                .await
                .is_err()
            {
                return;
            }
        }
    });
    (client, server, session.unwrap())
}

fn loopback(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("loopback");
    group.measurement_time(Duration::from_secs(10));
    for (name, size, reply) in [
        ("rtt_64b", 64, (|len| len) as fn(usize) -> usize),
        ("message_2mib", 2 << 20, |_| 1),
    ] {
        let (_client, _server, mut session) = runtime.block_on(pair(reply));
        let message = vec![0xfe_u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter_custom(|iters| {
                runtime.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        session
                            .send(
                                message.clone(),
                                RakReliability::ReliableOrdered,
                                RakPriority::Normal,
                            )
                            .await
                            .unwrap();
                        session.recv::<Box<[u8]>>().await.unwrap();
                    }
                    start.elapsed()
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, loopback);
criterion_main!(benches);
