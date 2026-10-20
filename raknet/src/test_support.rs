//! Fixtures shared by benchmarks and the cross-implementation tests.

use crate::prelude::*;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Returns the repository's committed `testdata` directory.
pub fn testdata_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata")
}

/// Reads a fixture of u32 little-endian length-prefixed records.
///
/// # Panics
/// Panics with the regeneration command when the fixture is missing or truncated.
pub fn read_records(path: &Path) -> Vec<Vec<u8>> {
    let data = std::fs::read(path).unwrap_or_else(|error| {
        panic!(
            "missing fixture {}: {error}; regenerate with `go run ./cmd/inpkg -run TestParityGolden -update` in tools/go-raknet",
            path.display()
        )
    });
    let mut rest = data.as_slice();
    let mut records = Vec::new();
    while !rest.is_empty() {
        let (len, tail) = rest.split_at(4);
        let len = u32::from_le_bytes(len.try_into().expect("four bytes")) as usize;
        assert!(tail.len() >= len, "truncated record in {}", path.display());
        records.push(tail[..len].to_vec());
        rest = &tail[len..];
    }
    records
}

/// Reads a shared benchmark input from `testdata/bench`.
pub fn bench_input(name: &str) -> Vec<Vec<u8>> {
    read_records(&testdata_dir().join("bench").join(name))
}

/// Fills message `k` of a run; the Go peer and benchmarks generate the same bytes.
///
/// The first byte is the game-packet ID, so no message reads as a RakNet control packet.
pub fn pattern(size: usize, k: usize) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..size)
        .map(|i| {
            (i.wrapping_mul(31)
                .wrapping_add(k.wrapping_mul(7))
                .wrapping_add(1)) as u8
        })
        .collect();
    if let Some(first) = bytes.first_mut() {
        *first = 0xfe;
    }
    bytes
}

/// Completes a public client/server handshake with in-memory delivery at one instant.
///
/// # Panics
/// Panics when negotiation does not finish within a fixed amount of work.
pub fn negotiated_pair(
    client_addr: SocketAddr,
    server_addr: SocketAddr,
    now: Instant,
    config: RakSessionConfig,
) -> (RakSession<Established>, RakSession<Established>) {
    let mut client = RakClient::new(
        RakClientConfig {
            session: config.clone(),
            ..Default::default()
        },
        RakClock::new(now),
    )
    .unwrap()
    .connect(server_addr, client_addr, ClientGuid::from_raw(1 << 63), now)
    .unwrap();
    let mut server = RakServer::new(
        RakServerConfig {
            session: config,
            ..Default::default()
        },
        server_addr,
        RakClock::new(now),
        now,
    )
    .unwrap();
    let mut local: Option<RakSession<Established>> = None;
    let mut remote: Option<RakSession<Established>> = None;
    for _ in 0..128 {
        let mut moved = false;
        while let Some(output) = client.poll() {
            match output {
                RakClientOutput::MtuProbe { id, datagram, .. } => {
                    client
                        .handle(RakClientInput::MtuProbeResult {
                            id,
                            now,
                            elapsed: Duration::ZERO,
                            too_large: false,
                        })
                        .unwrap();
                    server
                        .handle(RakServerInput::Datagram(datagram, client_addr, now))
                        .unwrap();
                    moved = true;
                }
                RakClientOutput::SocketDatagram(bytes, _) => {
                    server
                        .handle(RakServerInput::Datagram(bytes, client_addr, now))
                        .unwrap();
                    moved = true;
                }
                RakClientOutput::SessionConnected(session) => local = Some(session),
                RakClientOutput::SessionDatagram(bytes) => {
                    let session = local.as_mut().expect("handoff precedes session datagrams");
                    session
                        .handle(RakSessionInput::Datagram(bytes, now))
                        .unwrap();
                }
                RakClientOutput::SessionClosing(_) => panic!("a fresh handshake cannot close"),
                RakClientOutput::Wait(_) | RakClientOutput::Pong { .. } => {}
            }
        }
        while let Some(output) = server.poll() {
            match output {
                RakServerOutput::SocketDatagram(bytes, _) => {
                    client
                        .handle(RakClientInput::Datagram(bytes, server_addr, now))
                        .unwrap();
                    moved = true;
                }
                RakServerOutput::SessionDatagram(bytes, _) => {
                    let session = remote.as_mut().expect("handoff precedes session datagrams");
                    session
                        .handle(RakSessionInput::Datagram(bytes, now))
                        .unwrap();
                }
                RakServerOutput::SessionConnected(session) => remote = Some(session),
                RakServerOutput::SessionClosing(_) => panic!("a fresh handshake cannot close"),
            }
        }
        for (session, to_server) in [(&mut local, true), (&mut remote, false)] {
            let Some(session) = session else { continue };
            session.tick(now).unwrap();
            while let Some(output) = session.poll() {
                if let RakSessionOutput::Datagram(bytes, _) = output {
                    if to_server {
                        server
                            .handle(RakServerInput::Datagram(bytes, client_addr, now))
                            .unwrap();
                    } else {
                        client
                            .handle(RakClientInput::Datagram(bytes, server_addr, now))
                            .unwrap();
                    }
                    moved = true;
                }
            }
        }
        if !moved {
            match (local, remote) {
                (Some(local), Some(remote)) => return (local, remote),
                (l, r) => (local, remote) = (l, r),
            }
        }
    }
    panic!("negotiation did not complete within the fixed work bound");
}
