use super::*;
use crate::util::flags;

/// Returns the first probe's payload length for a fresh synthetic connection.
fn request_1_len(addr: SocketAddr) -> u16 {
    let mut client = RakClient::new(RakClientConfig::default());
    client
        .handle(RakClientInput::Connect(addr, domain_time()))
        .unwrap();
    match client.poll().unwrap() {
        RakClientOutput::MtuProbe { datagram, .. } => u16::try_from(datagram.len()).unwrap(),
        output => panic!("expected an MTU probe, got {output:?}"),
    }
}

/// Supplies a reproducible domain timestamp without reading the clock.
fn domain_time() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1000)
}

#[test]
fn request_1_leaves_room_for_the_ip_and_udp_headers() {
    let v4 = request_1_len("127.0.0.1:19132".parse().unwrap());
    let v6 = request_1_len("[::1]:19132".parse().unwrap());

    assert_eq!(v4 + 20 + 8, constants::MAX_MTU_SIZE);
    assert_eq!(v6 + 40 + 8, constants::MAX_MTU_SIZE);
}

#[test]
fn overall_connect_timeout_fails_regardless_of_attempts() {
    let mut client = RakClient::new(RakClientConfig {
        conn_attempt_timeout: Duration::from_millis(100),
        conn_attempt_interval: Duration::from_millis(1000),
        conn_attempt_max: 100,
        ..RakClientConfig::default()
    });

    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let start = domain_time();

    client.handle(RakClientInput::Connect(addr, start)).unwrap();

    let result = client.handle(RakClientInput::Update(start + Duration::from_millis(200)));

    assert!(matches!(result, Err(RakClientError::ConnectionFailed)));
}

fn last_wait(client: &mut RakClient) -> Option<Duration> {
    let mut wait = None;
    while let Some(out) = client.poll() {
        if let RakClientOutput::Wait(duration) = out {
            wait = Some(duration);
        }
    }
    wait
}

#[test]
fn connected_client_schedules_a_wait_instead_of_spinning() {
    let mut client = RakClient::new(RakClientConfig::default());
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let start = domain_time();

    client.handle(RakClientInput::Connect(addr, start)).unwrap();
    last_wait(&mut client);
    client.state = RakClientState::HandshakeCompleted(addr);

    for elapsed in [Duration::from_secs(2), Duration::from_secs(20)] {
        let result = client.handle(RakClientInput::Update(start + elapsed));
        assert!(
            result.is_ok(),
            "update {elapsed:?} after connecting failed: {result:?}"
        );

        let wait = last_wait(&mut client);
        assert!(
            wait.is_some_and(|w| w > Duration::ZERO),
            "update {elapsed:?} after connecting scheduled {wait:?}"
        );
    }
}

#[test]
fn failed_connect_schedules_a_wait_instead_of_spinning() {
    let mut client = RakClient::new(RakClientConfig::default());
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let start = domain_time();

    client.handle(RakClientInput::Connect(addr, start)).unwrap();
    last_wait(&mut client);

    let failed_at = start + client.config.conn_attempt_timeout;
    let result = client.handle(RakClientInput::Update(failed_at));
    assert!(matches!(result, Err(RakClientError::ConnectionFailed)));
    assert!(last_wait(&mut client).is_some_and(|w| w > Duration::ZERO));

    client
        .handle(RakClientInput::Update(failed_at + Duration::from_secs(1)))
        .unwrap();
    assert!(last_wait(&mut client).is_some_and(|w| w > Duration::ZERO));
}

#[test]
fn disconnect_input_allows_reconnect() {
    let mut client = RakClient::new(RakClientConfig::default());
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();

    client.state = RakClientState::HandshakeCompleted(addr);

    let result = client.handle(RakClientInput::Connect(addr, domain_time()));
    assert!(matches!(result, Err(RakClientError::AlreadyConnected)));
    assert!(matches!(
        client.state,
        RakClientState::HandshakeCompleted(_)
    ));

    client.handle(RakClientInput::Disconnect).unwrap();
    assert!(matches!(client.state, RakClientState::Unconnected));

    client
        .handle(RakClientInput::Connect(addr, domain_time()))
        .unwrap();
    assert!(matches!(client.state, RakClientState::Handshake1(_)));
}

fn encoded(packet: impl RakCodec) -> Box<[u8]> {
    packet.serialized().unwrap()
}

fn reply_1(mtu: u16) -> Box<[u8]> {
    encoded(OpenConnectionReply1 {
        guid: 1,
        cookie: None,
        mtu,
    })
}

fn reply_2(addr: SocketAddr, mtu: u16) -> Box<[u8]> {
    encoded(OpenConnectionReply2::new(1, addr, mtu, false))
}

#[test]
fn reply_2_above_the_maximum_mtu_is_clamped() {
    let mut client = RakClient::new(RakClientConfig::default());
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    client.state = RakClientState::Handshake2(addr);

    client
        .handle(RakClientInput::Datagram(
            reply_2(addr, u16::MAX),
            addr,
            domain_time(),
        ))
        .unwrap();

    assert_eq!(client.mtu, constants::MAX_MTU_SIZE);
    while let Some(out) = client.poll() {
        if let RakClientOutput::SocketDatagram(buf, _) = out {
            assert!(buf.len() <= constants::MAX_MTU_SIZE as usize);
        }
    }
}

#[test]
fn handshake_session_resends_connection_request() {
    let mut client = RakClient::new(RakClientConfig::default());
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let t0 = domain_time();

    client.handle(RakClientInput::Connect(addr, t0)).unwrap();
    client
        .handle(RakClientInput::Datagram(reply_1(1200), addr, t0))
        .unwrap();
    client
        .handle(RakClientInput::Datagram(reply_2(addr, 1200), addr, t0))
        .unwrap();
    while client.poll().is_some() {}

    client
        .handle(RakClientInput::Update(t0 + Duration::from_millis(2100)))
        .unwrap();

    let resent = std::iter::from_fn(|| client.poll()).any(|out| {
        matches!(out, RakClientOutput::SocketDatagram(buf, _)
            if buf.first().is_some_and(|b| b & flags::VALID != 0 && b & flags::ACK == 0))
    });
    assert!(
        resent,
        "handshake session never resent the ConnectionRequest"
    );
}

#[test]
fn failed_connect_resets_and_schedules_a_wait() {
    let mut client = RakClient::new(RakClientConfig {
        conn_attempt_timeout: Duration::from_millis(100),
        ..RakClientConfig::default()
    });
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let start = domain_time();

    client.handle(RakClientInput::Connect(addr, start)).unwrap();
    last_wait(&mut client);

    let result = client.handle(RakClientInput::Update(start + Duration::from_millis(200)));
    assert!(matches!(result, Err(RakClientError::ConnectionFailed)));
    assert!(last_wait(&mut client).is_some(), "no wait after failure");
    assert!(matches!(client.state, RakClientState::Unconnected));
    assert!(!client.is_connecting());

    let again = client.handle(RakClientInput::Update(start + Duration::from_millis(300)));
    assert!(again.is_ok(), "update after failure failed: {again:?}");
}

#[test]
fn incompatible_protocol_resets_state() {
    let mut client = RakClient::new(RakClientConfig::default());
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let now = domain_time();

    client.handle(RakClientInput::Connect(addr, now)).unwrap();
    last_wait(&mut client);

    let result = client.handle(RakClientInput::Datagram(
        vec![packet_id::INCOMPATIBLE_PROTOCOL].into(),
        addr,
        now,
    ));

    assert!(matches!(result, Err(RakClientError::IncompatibleProtocol)));
    assert!(matches!(client.state, RakClientState::Unconnected));
    assert!(last_wait(&mut client).is_some());
}

#[test]
fn connect_while_connecting_is_rejected() {
    let mut client = RakClient::new(RakClientConfig::default());
    let addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let now = domain_time();

    client.handle(RakClientInput::Connect(addr, now)).unwrap();
    let result = client.handle(RakClientInput::Connect(addr, now));

    assert!(
        matches!(result, Err(RakClientError::AlreadyConnected)),
        "got {result:?}"
    );
}
