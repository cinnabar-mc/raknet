use super::*;
use std::net::SocketAddrV4;

const PEER: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 19132));

/// Collects complete wire outputs as an instantaneous synthetic socket would.
fn collect(client: &mut RakClient, now: SystemTime) -> Vec<Box<[u8]>> {
    let mut sent = Vec::new();
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
                sent.push(datagram);
            }
            RakClientOutput::SocketDatagram(bytes, _) => sent.push(bytes),
            _ => {}
        }
    }
    sent
}

/// Delivers one encoded peer packet and returns the client's immediate wire response.
fn deliver(client: &mut RakClient, packet: impl RakCodec, now: SystemTime) -> Vec<Box<[u8]>> {
    client
        .handle(RakClientInput::Datagram(
            packet.serialized().unwrap(),
            PEER,
            now,
        ))
        .unwrap();
    collect(client, now)
}

/// Starts a client at a fixed domain time, with no real sockets or clock reads.
fn begin(cap: u16) -> (RakClient, SystemTime) {
    let mut client = RakClient::new(RakClientConfig {
        max_mtu_size: cap,
        ..RakClientConfig::default()
    });
    let now = UNIX_EPOCH + Duration::from_secs(1000);
    client
        .handle(RakClientInput::Connect {
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now,
        })
        .unwrap();
    assert_eq!(
        collect(&mut client, now)[0][0],
        packet_id::OPEN_CONNECTION_REQUEST_1
    );
    (client, now)
}

#[test]
fn reply_one_keeps_probes_running_and_probe_exhaustion_keeps_request_two_alive() {
    let (mut client, start) = begin(constants::MAX_MTU_SIZE);
    let grant = OpenConnectionReply1 {
        guid: 1,
        cookie: Some(7),
        mtu: constants::MAX_MTU_SIZE,
    };
    let sent = deliver(&mut client, grant, start);
    assert_eq!(sent[0][0], packet_id::OPEN_CONNECTION_REQUEST_2);

    let mut probes = Vec::new();
    for tick in 1..=14 {
        let now = start + Duration::from_millis(tick * 500);
        client.handle(RakClientInput::Update(now)).unwrap();
        let sent = collect(&mut client, now);
        assert!(
            sent.iter()
                .any(|bytes| bytes[0] == packet_id::OPEN_CONNECTION_REQUEST_2)
        );
        probes.extend(
            sent.into_iter()
                .filter(|bytes| bytes[0] == packet_id::OPEN_CONNECTION_REQUEST_1)
                .map(|bytes| bytes.len()),
        );
    }
    assert_eq!(
        probes,
        [vec![1464; 3], vec![1172; 4], vec![548; 5]].concat()
    );
    assert!(client.is_connecting());
    assert!(matches!(
        client.handle(RakClientInput::Update(start + Duration::from_secs(10))),
        Err(RakClientError::ConnectionFailed)
    ));
}

#[test]
fn protected_challenge_and_rotated_cookie_never_become_an_unvalidated_connected_budget() {
    let (mut client, start) = begin(1400);
    let challenge = OpenConnectionReply1 {
        guid: 0,
        cookie: Some(17),
        mtu: 24,
    };
    let sent = deliver(&mut client, challenge, start);
    let request = OpenConnectionRequest2::deserialize(&mut Cursor::new(&sent[0])).unwrap();
    assert_eq!((request.mtu, request.cookie), (24, Some(17)));
    assert!(matches!(client.state, RakClientState::Handshake1 { .. }));
    assert!(
        deliver(
            &mut client,
            OpenConnectionReply2::new(1, PEER, 1200, false),
            start,
        )
        .is_empty(),
        "a challenge alone does not authorize Reply 2"
    );
    assert!(client.session.is_none());

    let at = start + Duration::from_millis(500);
    client.handle(RakClientInput::Update(at)).unwrap();
    assert!(
        collect(&mut client, at)
            .iter()
            .any(|bytes| bytes[0] == packet_id::OPEN_CONNECTION_REQUEST_1)
    );

    for (elapsed, mtu, cookie) in [(550, 1400, 18), (560, 1200, 19)] {
        let sent = deliver(
            &mut client,
            OpenConnectionReply1 {
                guid: 1,
                cookie: Some(cookie),
                mtu,
            },
            start + Duration::from_millis(elapsed),
        );
        let request = OpenConnectionRequest2::deserialize(&mut Cursor::new(&sent[0])).unwrap();
        assert_eq!((request.mtu, request.cookie), (mtu, Some(cookie)));
    }
    let sent = deliver(
        &mut client,
        OpenConnectionReply1 {
            guid: 1,
            cookie: Some(20),
            mtu: 1492,
        },
        start + Duration::from_millis(570),
    );
    assert!(
        sent.is_empty(),
        "a valid grant above the caller's cap must be ignored"
    );

    let mut malformed = OpenConnectionReply1 {
        guid: 1,
        cookie: Some(21),
        mtu: 1300,
    }
    .serialized()
    .unwrap();
    malformed[1] ^= 0xff;
    client
        .handle(RakClientInput::Datagram(
            malformed,
            PEER,
            start + Duration::from_millis(580),
        ))
        .unwrap();
    assert!(collect(&mut client, start).is_empty());

    let at = start + Duration::from_millis(1060);
    client.handle(RakClientInput::Update(at)).unwrap();
    let sent = collect(&mut client, at);
    let request = sent
        .iter()
        .find(|bytes| bytes[0] == packet_id::OPEN_CONNECTION_REQUEST_2)
        .unwrap();
    let request = OpenConnectionRequest2::deserialize(&mut Cursor::new(request)).unwrap();
    assert_eq!((request.mtu, request.cookie), (1200, Some(19)));
}

#[test]
fn reply_two_selects_the_final_budget_independently_of_reply_one() {
    for (grant, expected) in [(1280, 1280), (900, 900), (20, 576), (u16::MAX, 1400)] {
        let (mut client, now) = begin(1400);
        assert!(client.is_offline_negotiating());
        deliver(
            &mut client,
            OpenConnectionReply1 {
                guid: 1,
                cookie: None,
                mtu: 1200,
            },
            now,
        );
        assert!(client.is_offline_negotiating());
        deliver(
            &mut client,
            OpenConnectionReply2::new(1, PEER, grant, false),
            now,
        );
        assert_eq!(client.mtu, expected);
        assert!(client.session.is_some());
        assert!(!client.is_offline_negotiating());
        assert!(client.is_connecting());
    }
}

#[test]
fn an_unsupported_caller_cap_fails_before_emitting_a_probe() {
    let mut client = RakClient::new(RakClientConfig {
        max_mtu_size: 400,
        ..RakClientConfig::default()
    });
    let now = UNIX_EPOCH + Duration::from_secs(1000);
    assert!(
        client
            .handle(RakClientInput::Connect {
                remote: PEER,
                local: SocketAddr::new(PEER.ip(), 54321),
                now
            })
            .is_err()
    );
    assert!(collect(&mut client, now).is_empty());
    assert!(!client.is_connecting());
}

/// Takes the single outstanding probe without completing its synthetic socket write.
fn pending_probe(client: &mut RakClient) -> Option<(MtuProbeId, usize)> {
    let mut probe = None;
    while let Some(output) = client.poll() {
        if let RakClientOutput::MtuProbe { id, datagram, .. } = output {
            assert!(probe.is_none(), "more than one probe write was outstanding");
            probe = Some((id, datagram.len()));
        }
    }
    probe
}

#[test]
fn write_results_preserve_preparation_deadlines_and_the_integer_slow_write_boundary() {
    for (preparation, elapsed, too_large, payload_len, retry_ms) in [
        (Duration::from_millis(101), Duration::ZERO, false, 1464, 500),
        (Duration::ZERO, Duration::from_millis(100), false, 1464, 500),
        (
            Duration::ZERO,
            Duration::from_micros(100_999),
            false,
            1464,
            500,
        ),
        (Duration::ZERO, Duration::from_millis(101), false, 548, 111),
        (Duration::ZERO, Duration::from_millis(101), true, 1172, 111),
    ] {
        let mut client = RakClient::new(RakClientConfig::default());
        let start = UNIX_EPOCH + Duration::from_secs(1000);
        client
            .handle(RakClientInput::Connect {
                remote: PEER,
                local: SocketAddr::new(PEER.ip(), 54321),
                now: start,
            })
            .unwrap();
        let (id, _) = pending_probe(&mut client).unwrap();
        client
            .handle(RakClientInput::MtuProbeResult {
                id,
                now: start + preparation + elapsed,
                elapsed,
                too_large,
            })
            .unwrap();
        assert!(pending_probe(&mut client).is_none());
        let retry = start + Duration::from_millis(retry_ms);
        client
            .handle(RakClientInput::Update(retry - Duration::from_millis(1)))
            .unwrap();
        assert!(pending_probe(&mut client).is_none());
        client.handle(RakClientInput::Update(retry)).unwrap();
        assert_eq!(pending_probe(&mut client).unwrap().1, payload_len);
    }

    let mut client = RakClient::new(RakClientConfig::default());
    let mut now = UNIX_EPOCH + Duration::from_secs(1000);
    client
        .handle(RakClientInput::Connect {
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now,
        })
        .unwrap();
    for (payload_len, too_large) in [(1464, true), (1172, true), (548, true), (548, false)] {
        let (id, len) = pending_probe(&mut client).unwrap();
        assert_eq!(len, payload_len);
        client
            .handle(RakClientInput::MtuProbeResult {
                id,
                now,
                elapsed: Duration::ZERO,
                too_large,
            })
            .unwrap();
        now += Duration::from_millis(10);
        client.handle(RakClientInput::Update(now)).unwrap();
    }
    assert!(
        pending_probe(&mut client).is_none(),
        "a successful final retry must finish the probe ladder"
    );
}

#[test]
fn stale_write_completions_do_not_advance_reconnects_or_restart_completed_negotiation() {
    let mut client = RakClient::new(RakClientConfig::default());
    let first = UNIX_EPOCH + Duration::from_secs(1000);
    client
        .handle(RakClientInput::Connect {
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now: first,
        })
        .unwrap();
    let (old, _) = pending_probe(&mut client).unwrap();
    client.handle(RakClientInput::Disconnect).unwrap();
    let start = first + Duration::from_secs(1);
    client
        .handle(RakClientInput::Connect {
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now: start,
        })
        .unwrap();
    let (current, _) = pending_probe(&mut client).unwrap();
    assert_ne!(old, current);
    client
        .handle(RakClientInput::MtuProbeResult {
            id: old,
            now: first,
            elapsed: Duration::ZERO,
            too_large: false,
        })
        .unwrap();
    assert!(
        client.poll().is_none(),
        "stale completion emitted a new timer"
    );
    client
        .handle(RakClientInput::MtuProbeResult {
            id: old,
            now: start,
            elapsed: Duration::from_millis(101),
            too_large: true,
        })
        .unwrap();
    let at = start + Duration::from_millis(500);
    client.handle(RakClientInput::Update(at)).unwrap();
    assert!(
        pending_probe(&mut client).is_none(),
        "the new write still awaits its own completion"
    );
    client
        .handle(RakClientInput::MtuProbeResult {
            id: current,
            now: at,
            elapsed: Duration::ZERO,
            too_large: false,
        })
        .unwrap();
    let at = at + Duration::from_millis(10);
    client.handle(RakClientInput::Update(at)).unwrap();
    let (pending, len) = pending_probe(&mut client).unwrap();
    assert_eq!(len, 1464);
    deliver(
        &mut client,
        OpenConnectionReply1 {
            guid: 1,
            cookie: Some(7),
            mtu: 1200,
        },
        at,
    );
    deliver(
        &mut client,
        OpenConnectionReply2::new(1, PEER, 1200, false),
        at,
    );
    client
        .handle(RakClientInput::MtuProbeResult {
            id: pending,
            now: at,
            elapsed: Duration::ZERO,
            too_large: true,
        })
        .unwrap();
    client
        .handle(RakClientInput::Update(at + Duration::from_secs(3)))
        .unwrap();
    assert!(
        std::iter::from_fn(|| client.poll()).all(|output| match output {
            RakClientOutput::MtuProbe { .. } => false,
            RakClientOutput::SocketDatagram(bytes, _) =>
                bytes[0] != packet_id::OPEN_CONNECTION_REQUEST_2,
            _ => true,
        })
    );
}

#[test]
fn late_grants_replace_the_exhausted_probe_wake_and_wake_the_connected_handshake() {
    let (mut client, start) = begin(constants::MAX_MTU_SIZE);
    for tick in 1..=12 {
        let now = start + Duration::from_millis(tick * 500);
        client.handle(RakClientInput::Update(now)).unwrap();
        collect(&mut client, now);
    }
    let late = start + Duration::from_millis(6100);
    client
        .handle(RakClientInput::Datagram(
            OpenConnectionReply1 {
                guid: 1,
                cookie: None,
                mtu: 1200,
            }
            .serialized()
            .unwrap(),
            PEER,
            late,
        ))
        .unwrap();
    let outputs: Vec<_> = std::iter::from_fn(|| client.poll()).collect();
    assert!(
        outputs.iter().any(|output| matches!(output,
            RakClientOutput::Wait(wait) if *wait <= Duration::from_millis(500)
        )),
        "a late grant left its Request 2 retry asleep"
    );
    client
        .handle(RakClientInput::Datagram(
            OpenConnectionReply2::new(1, PEER, 1200, false)
                .serialized()
                .unwrap(),
            PEER,
            late,
        ))
        .unwrap();
    assert!(
        std::iter::from_fn(|| client.poll()).any(|output| matches!(output,
            RakClientOutput::Wait(wait) if wait < Duration::from_secs(1)
        )),
        "the connected handshake needs its own timer wake"
    );
}

#[test]
fn cancellation_drops_unclaimed_session_handoffs() {
    let mut client = RakClient::new(RakClientConfig::default());
    let now = UNIX_EPOCH + Duration::from_secs(1000);
    client
        .adopt(RakSession::new(
            RakSessionId(1),
            PEER,
            1,
            constants::MAX_MTU_SIZE,
            now,
            |_| {},
        ))
        .unwrap();
    client.handle(RakClientInput::Disconnect).unwrap();
    assert!(
        client.poll().is_none(),
        "a cancelled session must not be handed to its consumer"
    );
}
