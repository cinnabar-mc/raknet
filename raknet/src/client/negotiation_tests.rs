use super::*;
use std::net::SocketAddrV4;

const PEER: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 19132));

/// Collects complete wire outputs as an instantaneous synthetic socket would.
fn collect(client: &mut ClientEngine, now: Instant) -> Vec<Box<[u8]>> {
    let mut sent = Vec::new();
    while let Some(output) = client.poll() {
        match output {
            ClientOutput::MtuProbe { id, datagram, .. } => {
                client
                    .handle(ClientInput::MtuProbeResult {
                        id,
                        now,
                        elapsed: Duration::ZERO,
                        too_large: false,
                    })
                    .unwrap();
                sent.push(datagram);
            }
            ClientOutput::SocketDatagram(bytes, _) => sent.push(bytes),
            _ => {}
        }
    }
    sent
}

/// Delivers one encoded peer packet and returns the client's immediate wire response.
fn deliver(client: &mut ClientEngine, packet: impl RakCodec, now: Instant) -> Vec<Box<[u8]>> {
    client
        .handle(ClientInput::Datagram(
            packet.serialized().unwrap(),
            PEER,
            now,
        ))
        .unwrap();
    collect(client, now)
}

/// Starts a client at a fixed domain time, with no real sockets or clock reads.
fn begin(cap: u16) -> (ClientEngine, Instant) {
    let mut client = ClientEngine::new(
        RakClientConfig {
            max_mtu_size: cap,
            ..RakClientConfig::default()
        },
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    let now = crate::clock::test_now() + Duration::from_secs(1000);
    client
        .handle(ClientInput::Connect {
            guid: crate::client::TEST_CLIENT_GUID,
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
        client.handle(ClientInput::Update(now)).unwrap();
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
        client.handle(ClientInput::Update(start + Duration::from_secs(10))),
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
    assert!(matches!(client.state, RakClientState::Handshake1(_)));
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
    client.handle(ClientInput::Update(at)).unwrap();
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
        .handle(ClientInput::Datagram(
            malformed,
            PEER,
            start + Duration::from_millis(580),
        ))
        .unwrap();
    assert!(collect(&mut client, start).is_empty());

    let at = start + Duration::from_millis(1060);
    client.handle(ClientInput::Update(at)).unwrap();
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
    let mut client = ClientEngine::new(
        RakClientConfig {
            max_mtu_size: 400,
            ..RakClientConfig::default()
        },
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    let now = crate::clock::test_now() + Duration::from_secs(1000);
    assert!(
        client
            .handle(ClientInput::Connect {
                guid: crate::client::TEST_CLIENT_GUID,
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
fn pending_probe(client: &mut ClientEngine) -> Option<(MtuProbeId, usize)> {
    let mut probe = None;
    while let Some(output) = client.poll() {
        if let ClientOutput::MtuProbe { id, datagram, .. } = output {
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
        let mut client = ClientEngine::new(
            RakClientConfig::default(),
            crate::clock::RakClock::new(crate::clock::test_now()),
        );
        let start = crate::clock::test_now() + Duration::from_secs(1000);
        client
            .handle(ClientInput::Connect {
                guid: crate::client::TEST_CLIENT_GUID,
                remote: PEER,
                local: SocketAddr::new(PEER.ip(), 54321),
                now: start,
            })
            .unwrap();
        let (id, _) = pending_probe(&mut client).unwrap();
        client
            .handle(ClientInput::MtuProbeResult {
                id,
                now: start + preparation + elapsed,
                elapsed,
                too_large,
            })
            .unwrap();
        assert!(pending_probe(&mut client).is_none());
        let retry = start + Duration::from_millis(retry_ms);
        client
            .handle(ClientInput::Update(retry - Duration::from_millis(1)))
            .unwrap();
        assert!(pending_probe(&mut client).is_none());
        client.handle(ClientInput::Update(retry)).unwrap();
        assert_eq!(pending_probe(&mut client).unwrap().1, payload_len);
    }

    let mut client = ClientEngine::new(
        RakClientConfig::default(),
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    let mut now = crate::clock::test_now() + Duration::from_secs(1000);
    client
        .handle(ClientInput::Connect {
            guid: crate::client::TEST_CLIENT_GUID,
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now,
        })
        .unwrap();
    for (payload_len, too_large) in [(1464, true), (1172, true), (548, true), (548, false)] {
        let (id, len) = pending_probe(&mut client).unwrap();
        assert_eq!(len, payload_len);
        client
            .handle(ClientInput::MtuProbeResult {
                id,
                now,
                elapsed: Duration::ZERO,
                too_large,
            })
            .unwrap();
        now += Duration::from_millis(10);
        client.handle(ClientInput::Update(now)).unwrap();
    }
    assert!(
        pending_probe(&mut client).is_none(),
        "a successful final retry must finish the probe ladder"
    );
}

#[test]
fn stale_write_completions_do_not_advance_reconnects_or_restart_completed_negotiation() {
    let mut client = ClientEngine::new(
        RakClientConfig::default(),
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    let first = crate::clock::test_now() + Duration::from_secs(1000);
    client
        .handle(ClientInput::Connect {
            guid: crate::client::TEST_CLIENT_GUID,
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now: first,
        })
        .unwrap();
    let (old, _) = pending_probe(&mut client).unwrap();
    client.handle(ClientInput::Disconnect).unwrap();
    let start = first + Duration::from_secs(1);
    client
        .handle(ClientInput::Connect {
            guid: crate::client::TEST_CLIENT_GUID,
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now: start,
        })
        .unwrap();
    let (current, _) = pending_probe(&mut client).unwrap();
    assert_ne!(old, current);
    client
        .handle(ClientInput::MtuProbeResult {
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
        .handle(ClientInput::MtuProbeResult {
            id: old,
            now: start,
            elapsed: Duration::from_millis(101),
            too_large: true,
        })
        .unwrap();
    let at = start + Duration::from_millis(500);
    client.handle(ClientInput::Update(at)).unwrap();
    assert!(
        pending_probe(&mut client).is_none(),
        "the new write still awaits its own completion"
    );
    client
        .handle(ClientInput::MtuProbeResult {
            id: current,
            now: at,
            elapsed: Duration::ZERO,
            too_large: false,
        })
        .unwrap();
    let at = at + Duration::from_millis(10);
    client.handle(ClientInput::Update(at)).unwrap();
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
        .handle(ClientInput::MtuProbeResult {
            id: pending,
            now: at,
            elapsed: Duration::ZERO,
            too_large: true,
        })
        .unwrap();
    client
        .handle(ClientInput::Update(at + Duration::from_secs(3)))
        .unwrap();
    assert!(
        std::iter::from_fn(|| client.poll()).all(|output| match output {
            ClientOutput::MtuProbe { .. } => false,
            ClientOutput::SocketDatagram(bytes, _) =>
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
        client.handle(ClientInput::Update(now)).unwrap();
        collect(&mut client, now);
    }
    let late = start + Duration::from_millis(6100);
    client
        .handle(ClientInput::Datagram(
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
            ClientOutput::Wait(wait) if *wait <= Duration::from_millis(500)
        )),
        "a late grant left its Request 2 retry asleep"
    );
    client
        .handle(ClientInput::Datagram(
            OpenConnectionReply2::new(1, PEER, 1200, false)
                .serialized()
                .unwrap(),
            PEER,
            late,
        ))
        .unwrap();
    assert!(
        std::iter::from_fn(|| client.poll()).any(|output| matches!(output,
            ClientOutput::Wait(wait) if wait < Duration::from_secs(1)
        )),
        "the connected handshake needs its own timer wake"
    );
}

#[test]
fn cancellation_drops_unclaimed_session_handoffs() {
    let mut client = ClientEngine::new(
        RakClientConfig::default(),
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    let now = crate::clock::test_now() + Duration::from_secs(1000);
    client
        .adopt(SessionEngine::new(
            RakSessionId(1),
            PEER,
            1,
            constants::MAX_MTU_SIZE,
            now,
            crate::clock::RakClock::new(crate::clock::test_now()),
            Default::default(),
        ))
        .unwrap();
    client.handle(ClientInput::Disconnect).unwrap();
    assert!(
        client.poll().is_none(),
        "a cancelled session must not be handed to its consumer"
    );
}

#[test]
fn identity_stays_with_each_attempt_through_challenges_retries_session_and_reconnect() {
    let mut client = ClientEngine::new(
        RakClientConfig::default(),
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    let first = TEST_CLIENT_GUID;
    let second = ClientGuid::from_raw(first.as_u64() ^ 1);
    let origin = crate::clock::test_now() + Duration::from_secs(1000);
    for (attempt, guid) in [first, second].into_iter().enumerate() {
        let now = origin + Duration::from_secs(attempt as u64);
        client
            .handle(ClientInput::Connect {
                remote: PEER,
                local: SocketAddr::new(PEER.ip(), 54321),
                guid,
                now,
            })
            .unwrap();
        collect(&mut client, now);
        for (server_guid, grant, cookie) in [(0, 20, 7), (1, 1200, 8), (1, 900, 9)] {
            let sent = deliver(
                &mut client,
                OpenConnectionReply1 {
                    guid: server_guid,
                    cookie: Some(cookie),
                    mtu: grant,
                },
                now,
            );
            let request = sent
                .iter()
                .find(|bytes| bytes[0] == packet_id::OPEN_CONNECTION_REQUEST_2)
                .unwrap();
            let request =
                OpenConnectionRequest2::deserialize(&mut Cursor::new(request.as_ref())).unwrap();
            assert_eq!(request.client, guid.as_u64());
            assert_eq!(request.cookie, Some(cookie));
        }
        let later = now + Duration::from_millis(500);
        client.handle(ClientInput::Update(later)).unwrap();
        let sent = collect(&mut client, later);
        let retry = sent
            .iter()
            .find(|bytes| bytes[0] == packet_id::OPEN_CONNECTION_REQUEST_2)
            .unwrap();
        assert_eq!(
            OpenConnectionRequest2::deserialize(&mut Cursor::new(retry.as_ref()))
                .unwrap()
                .client,
            guid.as_u64()
        );
        let connected = deliver(
            &mut client,
            OpenConnectionReply2::new(1, PEER, 900, false),
            later,
        );
        let data = connected
            .iter()
            .find(|bytes| bytes[0] & 0xe0 == 0x80)
            .unwrap();
        let set = crate::protocol::packets::frame_set::FrameSet::deserialize(&mut Cursor::new(
            data.as_ref(),
        ))
        .unwrap();
        let request = set
            .frames
            .iter()
            .find(|frame| frame.payload[0] == packet_id::CONNECTION_REQUEST)
            .unwrap();
        assert_eq!(
            ConnectionRequest::deserialize(&mut Cursor::new(request.payload.as_ref()))
                .unwrap()
                .client_guid,
            guid.as_u64()
        );
        assert_eq!(client.session.as_ref().unwrap().guid, guid.as_u64());
        client.handle(ClientInput::Disconnect).unwrap();
    }
    // Explicit custom wire identities are kept separate from generated defaults.
    let ping_guid = ClientGuid::from_raw(1234);
    client
        .handle(ClientInput::Ping {
            remote: PEER,
            guid: ping_guid,
            now: origin + Duration::from_secs(2),
        })
        .unwrap();
    let sent = collect(&mut client, origin + Duration::from_secs(2));
    let ping = sent
        .iter()
        .find(|bytes| bytes[0] == packet_id::UNCONNECTED_PING)
        .unwrap();
    assert_eq!(
        UnconnectedPing::deserialize(&mut Cursor::new(ping.as_ref()))
            .unwrap()
            .client,
        ping_guid.as_u64()
    );
}

#[test]
fn final_mtu_size_errors_exhaust_the_original_probe_budget() {
    let mut client = ClientEngine::new(
        RakClientConfig::default(),
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    let mut now = crate::clock::test_now() + Duration::from_secs(1000);
    client
        .handle(ClientInput::Connect {
            guid: crate::client::TEST_CLIENT_GUID,
            remote: PEER,
            local: SocketAddr::new(PEER.ip(), 54321),
            now,
        })
        .unwrap();
    for _ in 0..=constants::MTU_SIZES.len() {
        let (id, _) = pending_probe(&mut client).unwrap();
        client
            .handle(ClientInput::MtuProbeResult {
                id,
                now,
                elapsed: Duration::ZERO,
                too_large: true,
            })
            .unwrap();
        now += Duration::from_millis(10);
        client.handle(ClientInput::Update(now)).unwrap();
    }
    assert!(
        pending_probe(&mut client).is_none(),
        "a final too-large completion must exhaust the same finite ladder as a successful write"
    );
}
