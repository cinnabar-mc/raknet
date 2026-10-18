//! Private fixtures establish sessions through the real public protocol API.

use raknet::prelude::*;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Completes both public handshake directions with deterministic in-memory delivery.
pub(super) fn negotiated_pair(
    local: SocketAddr,
    remote: SocketAddr,
    now: Instant,
    origin: Instant,
    config: RakSessionConfig,
) -> (RakSession<Established>, RakSession<Established>) {
    let mut client = RakClient::new(
        RakClientConfig {
            session: config.clone(),
            ..Default::default()
        },
        RakClock::new(origin),
    )
    .unwrap()
    .connect(remote, local, ClientGuid::from_raw(1 << 63), now)
    .unwrap();
    let mut server = RakServer::new(
        RakServerConfig {
            session: config,
            ..Default::default()
        },
        remote,
        RakClock::new(origin),
        now,
    )
    .unwrap();
    let mut local_session: Option<RakSession<Established>> = None;
    let mut remote_session: Option<RakSession<Established>> = None;
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
                        .handle(RakServerInput::Datagram(datagram, local, now))
                        .unwrap();
                    moved = true;
                }
                RakClientOutput::SocketDatagram(bytes, _) => {
                    server
                        .handle(RakServerInput::Datagram(bytes, local, now))
                        .unwrap();
                    moved = true;
                }
                RakClientOutput::SessionConnected(session) => local_session = Some(session),
                RakClientOutput::SessionDatagram(bytes) => {
                    local_session
                        .as_mut()
                        .unwrap()
                        .handle(RakSessionInput::Datagram(bytes, now))
                        .unwrap();
                }
                RakClientOutput::SessionClosing(_) => {
                    panic!("a fresh handshake cannot hand off a closing session")
                }
                RakClientOutput::Wait(_) | RakClientOutput::Pong { .. } => {}
            }
        }
        while let Some(output) = server.poll() {
            match output {
                RakServerOutput::SocketDatagram(bytes, _) => {
                    client
                        .handle(RakClientInput::Datagram(bytes, remote, now))
                        .unwrap();
                    moved = true;
                }
                RakServerOutput::SessionDatagram(bytes, _) => {
                    remote_session
                        .as_mut()
                        .unwrap()
                        .handle(RakSessionInput::Datagram(bytes, now))
                        .unwrap();
                }
                RakServerOutput::SessionConnected(session) => remote_session = Some(session),
                RakServerOutput::SessionClosing(_) => {
                    panic!("a fresh handshake cannot hand off a closing session")
                }
            }
        }
        if let Some(session) = &mut local_session {
            while let Some(output) = session.poll() {
                if let RakSessionOutput::Datagram(bytes, _) = output {
                    server
                        .handle(RakServerInput::Datagram(bytes, local, now))
                        .unwrap();
                    moved = true;
                }
            }
        }
        if let Some(session) = &mut remote_session {
            while let Some(output) = session.poll() {
                if let RakSessionOutput::Datagram(bytes, _) = output {
                    client
                        .handle(RakClientInput::Datagram(bytes, remote, now))
                        .unwrap();
                    moved = true;
                }
            }
        }
        if !moved && local_session.is_some() && remote_session.is_some() {
            return (local_session.unwrap(), remote_session.unwrap());
        }
    }
    panic!("public negotiation did not complete within the controlled work bound");
}
