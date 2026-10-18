//! Loss witnesses drive actual public endpoint owners before the session handoff.

use crate::prelude::*;
use crate::protocol::codec::RakCodec;
use crate::protocol::packets::frame_set::FrameSet;
use crate::util::{flags, packet_id};
use std::io::Cursor;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

type Pair = (RakSession<Established>, RakSession<Established>, Instant);

/// Drops one actual reliable control datagram, without manufacturing protocol input.
fn drop_first_control(bytes: &[u8], wanted: u8, dropped: &mut bool) -> bool {
    if *dropped
        || bytes.first().is_none_or(|header| {
            header & flags::VALID == 0 || header & (flags::ACK | flags::NACK) != 0
        })
    {
        return false;
    }
    let frames = FrameSet::deserialize(&mut Cursor::new(bytes)).unwrap();
    if let Some(frame) = frames
        .frames
        .iter()
        .find(|frame| frame.payload.first() == Some(&wanted))
    {
        assert_eq!(frame.reliability, RakReliability::ReliableOrdered);
        *dropped = true;
        return true;
    }
    false
}

/// Runs both public endpoints under one caller clock and drops only the selected wire control.
fn negotiate_after_loss(autoflush: bool, lost_control: u8) -> Option<Pair> {
    let start = crate::clock::test_now();
    let local: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    let remote: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let config = RakClientConfig {
        session: RakSessionConfig {
            autoflush,
            ..Default::default()
        },
        ..Default::default()
    };
    let deadline = start + config.conn_attempt_timeout;
    let step = config.conn_attempt_interval;
    let mut server = RakServer::new(
        RakServerConfig {
            session: config.session.clone(),
            ..Default::default()
        },
        remote,
        RakClock::new(start),
        start,
    )
    .unwrap();
    let mut client = RakClient::new(config, RakClock::new(start))
        .unwrap()
        .connect(remote, local, ClientGuid::from_raw(1 << 63), start)
        .unwrap();
    let mut local_session: Option<RakSession<Established>> = None;
    let mut remote_session: Option<RakSession<Established>> = None;
    let mut dropped = false;
    let mut now = start;
    while now < deadline {
        client.handle(RakClientInput::Update(now)).unwrap();
        server.handle(RakServerInput::Update(now)).unwrap();
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
                        if !drop_first_control(&bytes, lost_control, &mut dropped) {
                            server
                                .handle(RakServerInput::Datagram(bytes, local, now))
                                .unwrap();
                        }
                        moved = true;
                    }
                    RakClientOutput::SessionConnected(session) => {
                        assert!(local_session.replace(session).is_none());
                    }
                    RakClientOutput::SessionDatagram(bytes) => local_session
                        .as_mut()
                        .unwrap()
                        .handle(RakSessionInput::Datagram(bytes, now))
                        .unwrap(),
                    RakClientOutput::SessionClosing(_) | RakClientOutput::Pong { .. } => {
                        panic!("fresh negotiation cannot yield a closing owner or discovery pong")
                    }
                    RakClientOutput::Wait(_) => {}
                }
            }
            while let Some(output) = server.poll() {
                match output {
                    RakServerOutput::SocketDatagram(bytes, _) => {
                        if !drop_first_control(&bytes, lost_control, &mut dropped) {
                            client
                                .handle(RakClientInput::Datagram(bytes, remote, now))
                                .unwrap();
                        }
                        moved = true;
                    }
                    RakServerOutput::SessionConnected(session) => {
                        assert!(remote_session.replace(session).is_none());
                    }
                    RakServerOutput::SessionDatagram(bytes, _) => remote_session
                        .as_mut()
                        .unwrap()
                        .handle(RakSessionInput::Datagram(bytes, now))
                        .unwrap(),
                    RakServerOutput::SessionClosing(_) => {
                        panic!("fresh negotiation cannot hand off a closing owner")
                    }
                }
            }
            if let Some(session) = &mut local_session {
                session.tick(now).unwrap();
                while let Some(output) = session.poll() {
                    match output {
                        RakSessionOutput::Datagram(bytes, _) => {
                            server
                                .handle(RakServerInput::Datagram(bytes, local, now))
                                .unwrap();
                            moved = true;
                        }
                        RakSessionOutput::Wait(_) => {}
                        output => panic!("unexpected established output: {output:?}"),
                    }
                }
            }
            if let Some(session) = &mut remote_session {
                session.tick(now).unwrap();
                while let Some(output) = session.poll() {
                    match output {
                        RakSessionOutput::Datagram(bytes, _) => {
                            client
                                .handle(RakClientInput::Datagram(bytes, remote, now))
                                .unwrap();
                            moved = true;
                        }
                        RakSessionOutput::Wait(_) => {}
                        output => panic!("unexpected established output: {output:?}"),
                    }
                }
            }
            if !moved {
                match (local_session, remote_session) {
                    (Some(local), Some(remote)) => {
                        assert!(dropped, "the actual selected reliable control must be lost");
                        return Some((local, remote, now));
                    }
                    (local, remote) => {
                        local_session = local;
                        remote_session = remote;
                    }
                }
                break;
            }
        }
        now += step;
    }
    assert!(dropped, "the actual selected reliable control must be lost");
    None
}

/// Confirms manual application delivery still needs the real handed-off tick capability.
fn assert_manual_application_flush(mut session: RakSession<Established>, now: Instant) {
    assert!(!session.snapshot(now).unwrap().config().autoflush);
    session
        .send(
            Box::from([0xfe, 1]),
            RakReliability::ReliableOrdered,
            RakPriority::Normal,
            now,
        )
        .unwrap();
    session.handle(RakSessionInput::Update(now)).unwrap();
    while let Some(output) = session.poll() {
        assert!(!matches!(output, RakSessionOutput::Datagram(..)));
    }
    session.tick(now).unwrap();
    assert!(
        std::iter::from_fn(|| session.poll())
            .any(|output| { matches!(output, RakSessionOutput::Datagram(..)) })
    );
}

#[test]
fn manual_pending_client_retries_a_lost_connection_request() {
    assert!(negotiate_after_loss(true, packet_id::CONNECTION_REQUEST).is_some());
    let (local, remote, now) = negotiate_after_loss(false, packet_id::CONNECTION_REQUEST)
        .expect("endpoint Update must advance its private manual-mode handshake retransmission");
    assert_manual_application_flush(local, now);
    assert_manual_application_flush(remote, now);
}

#[test]
fn manual_pending_server_retries_a_lost_connection_acceptance() {
    assert!(negotiate_after_loss(true, packet_id::CONNECTION_REQUEST_ACCEPTED).is_some());
    let (local, remote, now) = negotiate_after_loss(false, packet_id::CONNECTION_REQUEST_ACCEPTED)
        .expect("endpoint Update must advance its private manual-mode handshake retransmission");
    assert_manual_application_flush(local, now);
    assert_manual_application_flush(remote, now);
}
