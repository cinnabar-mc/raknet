//! Wire codec costs; ids match the Go suite in tools/go-raknet.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use raknet::protocol::codec::RakCodec;
use raknet::protocol::packets::ack::Ack;
use raknet::protocol::packets::frame_set::FrameSet;
use raknet::protocol::types::frame::Frame;
use raknet::test_support::bench_input;
use std::hint::black_box;

/// Effective MTU both suites bound ACK datagrams by: 1492 minus IPv4 and UDP headers.
const ACK_MTU: usize = 1492 - 28;

fn frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame");
    for name in ["unreliable_64", "reliable_ordered_1024", "split_1400"] {
        let input = bench_input(&format!("frame_{name}.bin")).remove(0);
        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_function(BenchmarkId::new("parse", name), |b| {
            b.iter(|| Frame::deserialize(&mut black_box(input.as_slice())).unwrap())
        });
        let decoded = Frame::deserialize(&mut input.as_slice()).unwrap();
        let mut buf = Vec::with_capacity(1500);
        group.bench_function(BenchmarkId::new("serialize", name), |b| {
            b.iter(|| {
                buf.clear();
                black_box(&decoded).serialize(&mut buf).unwrap();
            })
        });
    }
    group.finish();
}

fn frameset(c: &mut Criterion) {
    let mut group = c.benchmark_group("frameset");
    for name in ["bundle_16x64", "single_1400"] {
        let input = bench_input(&format!("frameset_{name}.bin")).remove(0);
        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_function(BenchmarkId::new("parse", name), |b| {
            b.iter(|| FrameSet::deserialize(&mut black_box(input.as_slice())).unwrap())
        });
        let decoded = FrameSet::deserialize(&mut input.as_slice()).unwrap();
        let mut buf = Vec::with_capacity(1500);
        group.bench_function(BenchmarkId::new("serialize", name), |b| {
            b.iter(|| {
                buf.clear();
                black_box(&decoded).serialize(&mut buf).unwrap();
            })
        });
    }
    group.finish();
}

fn ack(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack");
    for name in [
        "ack_ranges_1",
        "ack_ranges_512",
        "ack_singles_2048",
        "nack_singles_2048",
    ] {
        let datagrams = bench_input(&format!("{name}.bin"));
        let total: usize = datagrams.iter().map(Vec::len).sum();
        let decoded: Vec<Ack> = datagrams
            .iter()
            .map(|d| Ack::deserialize(&mut d.as_slice()).unwrap())
            .collect();
        let is_nack = decoded[0].is_nack;
        let seqs: Vec<u32> = decoded.into_iter().flat_map(|a| a.sequences).collect();
        group.throughput(Throughput::Bytes(total as u64));
        group.bench_function(BenchmarkId::new("decode", name), |b| {
            b.iter(|| {
                for d in &datagrams {
                    black_box(Ack::deserialize(&mut d.as_slice()).unwrap());
                }
            })
        });
        let mut buf = Vec::with_capacity(1500);
        group.bench_function(BenchmarkId::new("encode", name), |b| {
            b.iter(|| {
                for ack in Ack::split(black_box(&seqs).clone(), is_nack, ACK_MTU) {
                    buf.clear();
                    ack.serialize(&mut buf).unwrap();
                }
            })
        });
    }
    group.finish();
}

criterion_group!(benches, frame, frameset, ack);
criterion_main!(benches);
