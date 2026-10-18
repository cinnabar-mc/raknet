use super::*;
use crate::protocol::packets::frame_set::FrameSet;
use crate::protocol::types::frame::Frame;

const PEER: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 19132);
const LOCAL: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 54321);

/// Completes offline negotiation through the public inputs with instantaneous writes.
fn start_reliable_handshake(client: &mut RakClient, now: Instant) {
    client
        .handle(RakClientInput::Connect {
            guid: crate::client::TEST_CLIENT_GUID,
            remote: PEER,
            local: LOCAL,
            now,
        })
        .unwrap();
    while let Some(output) = client.poll() {
        if let RakClientOutput::MtuProbe { id, .. } = output {
            client
                .handle(RakClientInput::MtuProbeResult {
                    id,
                    now,
                    elapsed: Duration::ZERO,
                    too_large: false,
                })
                .unwrap();
        }
    }
    for packet in [
        OpenConnectionReply1 {
            guid: 1,
            cookie: None,
            mtu: 1492,
        }
        .serialized()
        .unwrap(),
        OpenConnectionReply2::new(1, PEER, 1492, false)
            .serialized()
            .unwrap(),
    ] {
        client
            .handle(RakClientInput::Datagram(packet, PEER, now))
            .unwrap();
        while client.poll().is_some() {}
    }
}

/// Creates a client waiting for reliable connection acceptance at a fixed domain time.
fn negotiating(config: RakClientConfig) -> (RakClient, Instant) {
    let now = crate::clock::test_now() + Duration::from_secs(1000);
    let mut client = RakClient::new(
        config,
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    start_reliable_handshake(&mut client, now);
    (client, now)
}

/// Encodes consecutive reliable ordered peer messages into one datagram.
fn datagram(sequence: u32, first_order: u32, payloads: Vec<Box<[u8]>>) -> Box<[u8]> {
    let frames = payloads
        .into_iter()
        .enumerate()
        .map(|(offset, payload)| {
            let mut frame = Frame::new(RakReliability::ReliableOrdered, payload);
            frame.reliable_index = first_order + offset as u32;
            frame.order_index = first_order + offset as u32;
            frame
        })
        .collect();
    FrameSet::new(sequence, frames, false, false, false)
        .serialized()
        .unwrap()
}

/// Encodes a valid acceptance control with a short system-address list.
fn accepted() -> Box<[u8]> {
    ConnectionRequestAccepted {
        client_address: LOCAL,
        system_index: 0,
        system_addresses: Vec::new(),
        request_timestamp: 42,
        timestamp: 100,
    }
    .serialized()
    .unwrap()
}

#[test]
fn early_packets_survive_handoff_in_fifo_after_the_incoming_control_is_sent() {
    let (mut client, now) = negotiating(RakClientConfig::default());
    client
        .handle(RakClientInput::Datagram(
            datagram(0, 0, vec![Box::new([0xfe, 1])]),
            PEER,
            now,
        ))
        .unwrap();
    while let Some(output) = client.poll() {
        assert!(!matches!(output, RakClientOutput::SessionConnected(_)));
    }
    client
        .handle(RakClientInput::Update(now + Duration::from_millis(100)))
        .unwrap();
    while client.poll().is_some() {}
    client
        .handle(RakClientInput::Datagram(
            datagram(
                1,
                1,
                vec![
                    Box::new([0xfe, 2]),
                    Box::new([packet_id::CONNECTION_REQUEST]),
                    Box::new([packet_id::NEW_INCOMING_CONNECTION]),
                    accepted(),
                    Box::new([0xfe, 3]),
                ],
            ),
            PEER,
            now + Duration::from_millis(100),
        ))
        .unwrap();
    let mut incoming_sent = false;
    let mut session = None;
    while let Some(output) = client.poll() {
        match output {
            RakClientOutput::SocketDatagram(bytes, _) if bytes[0] & 0xe0 == 0x80 => {
                let set = FrameSet::deserialize(&mut Cursor::new(bytes.as_ref())).unwrap();
                incoming_sent |= set
                    .frames
                    .iter()
                    .any(|frame| frame.payload[0] == packet_id::NEW_INCOMING_CONNECTION);
            }
            RakClientOutput::SessionConnected(connected) => {
                assert!(
                    incoming_sent,
                    "NewIncomingConnection must precede session exposure"
                );
                session = Some(connected);
            }
            _ => {}
        }
    }
    let mut session = session.expect("accepted handshake transfers its owner");
    // A later datagram must follow the packets retained before ownership transfer.
    session
        .handle(RakSessionInput::Datagram(
            datagram(2, 6, vec![Box::new([0xfe, 4])]),
            now + Duration::from_millis(101),
        ))
        .unwrap();
    let mut packets = Vec::new();
    while let Some(output) = session.poll() {
        if let RakSessionOutput::Packet(packet) = output {
            packets.push(packet);
        }
    }
    assert_eq!(
        packets,
        [1, 2, 3, 4].map(|index| Box::from([0xfe, index].as_slice()))
    );
}

#[test]
fn lost_connection_probe_emits_a_reliable_ping_instead_of_an_application_packet() {
    let now = crate::clock::test_now() + Duration::from_secs(1000);
    let mut session = RakSession::new(
        RakSessionId(0),
        PEER,
        1,
        1492,
        now,
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );
    session
        .handle(RakSessionInput::Datagram(
            datagram(0, 0, vec![Box::new([0x04])]),
            now,
        ))
        .unwrap();
    let mut ping = None;
    while let Some(output) = session.poll() {
        match output {
            RakSessionOutput::Packet(_) => panic!("liveness control leaked into application data"),
            RakSessionOutput::Datagram(bytes, _) if bytes[0] & 0xe0 == 0x80 => {
                let set = FrameSet::deserialize(&mut Cursor::new(bytes.as_ref())).unwrap();
                ping = set
                    .frames
                    .into_iter()
                    .find(|frame| frame.payload[0] == packet_id::CONNECTED_PING);
            }
            _ => {}
        }
    }
    assert_eq!(
        ping.expect("liveness control triggers a ping").reliability,
        RakReliability::ReliableOrdered
    );
    session.handle(RakSessionInput::Disconnect(now)).unwrap();
    session
        .handle(RakSessionInput::Datagram(
            datagram(1, 1, vec![Box::new([0x04])]),
            now,
        ))
        .unwrap();
    assert_eq!(session.get_state(), RakSessionState::Closing);
}

#[test]
fn accepted_then_closed_handoff_keeps_the_packet_prefix_and_terminal_event() {
    let (mut client, now) = negotiating(RakClientConfig::default());
    client
        .handle(RakClientInput::Datagram(
            datagram(
                0,
                0,
                vec![
                    accepted(),
                    Box::new([0xfe, 1]),
                    Box::new([packet_id::DISCONNECT]),
                    Box::new([0xfe, 2]),
                    crate::protocol::packets::connected_ping::ConnectedPing { timestamp: 777 }
                        .serialized()
                        .unwrap(),
                ],
            ),
            PEER,
            now,
        ))
        .unwrap();
    let mut session = None;
    while let Some(output) = client.poll() {
        match output {
            RakClientOutput::SessionConnected(connected) => session = Some(connected),
            RakClientOutput::SocketDatagram(bytes, _) if bytes[0] & 0xe0 == 0x80 => {
                let set = FrameSet::deserialize(&mut Cursor::new(bytes.as_ref())).unwrap();
                assert!(
                    set.frames
                        .iter()
                        .all(|frame| frame.payload[0] != packet_id::CONNECTED_PONG),
                    "controls after peer close must not receive a response"
                );
            }
            _ => {}
        }
    }
    let mut session = session.expect("acceptance transfers the terminal session");
    assert_eq!(session.get_state(), RakSessionState::Disconnected);
    assert!(
        matches!(session.poll(), Some(RakSessionOutput::Packet(packet)) if packet.as_ref() == [0xfe, 1])
    );
    assert!(matches!(
        session.poll(),
        Some(RakSessionOutput::Disconnected(
            _,
            crate::prelude::RakDisconnectReason::Remote
        ))
    ));
    assert!(session.poll().is_none());
}

#[test]
fn handshake_limits_and_cancellation_discard_pending_packets_before_reconnect() {
    for (bytes, messages, cancel) in [(4, 8, false), (8, 2, false), (4, 8, true)] {
        let (mut client, now) = negotiating(RakClientConfig {
            max_handshake_bytes: bytes,
            max_handshake_messages: messages,
            ..RakClientConfig::default()
        });
        client
            .handle(RakClientInput::Datagram(
                datagram(0, 0, vec![Box::new([0xfe, 1]), Box::new([0xfe, 2])]),
                PEER,
                now,
            ))
            .unwrap();
        if cancel {
            client.handle(RakClientInput::Disconnect).unwrap();
        } else {
            let result = client.handle(RakClientInput::Datagram(
                datagram(1, 2, vec![Box::new([0xfe, 3])]),
                PEER,
                now,
            ));
            assert!(matches!(
                result,
                Err(RakClientError::HandshakeReceiveBufferExceeded)
            ));
        }
        assert!(!client.is_connecting());
        while let Some(output) = client.poll() {
            assert!(!matches!(output, RakClientOutput::SessionConnected(_)));
        }
        let later = now + Duration::from_secs(1);
        start_reliable_handshake(&mut client, later);
        client
            .handle(RakClientInput::Datagram(
                datagram(0, 0, vec![accepted(), Box::new([0xfe, 9])]),
                PEER,
                later,
            ))
            .unwrap();
        let mut received = Vec::new();
        while let Some(output) = client.poll() {
            if let RakClientOutput::SessionConnected(mut session) = output {
                while let Some(output) = session.poll() {
                    if let RakSessionOutput::Packet(packet) = output {
                        received.push(packet);
                    }
                }
            }
        }
        assert_eq!(received, vec![Box::from([0xfe, 9].as_slice())]);
    }
}
