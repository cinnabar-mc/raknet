use super::*;

fn drain(server: &mut RakServer) -> Vec<RakServerOutput> {
    let mut out = Vec::new();
    while let Some(o) = server.poll() {
        out.push(o);
    }
    out
}

fn first_byte(output: &RakServerOutput) -> Option<u8> {
    match output {
        RakServerOutput::SocketDatagram(buf, _) => buf.first().copied(),
        _ => None,
    }
}

fn request_1(protocol: u8) -> Box<[u8]> {
    let req = OpenConnectionRequest1 { protocol, mtu: 100 };
    req.serialized().unwrap()
}

fn reply_cookie(server: &mut RakServer) -> Option<i32> {
    drain(server).into_iter().find_map(|out| match out {
        RakServerOutput::SocketDatagram(buf, _)
            if buf.first() == Some(&packet_id::OPEN_CONNECTION_REPLY_1) =>
        {
            OpenConnectionReply1::deserialize(&mut buf.as_ref())
                .unwrap()
                .cookie
        }
        _ => None,
    })
}

fn request_2(server_addr: SocketAddr, client: u64, cookie: Option<i32>) -> Box<[u8]> {
    let req = OpenConnectionRequest2 {
        cookie,
        addr: server_addr,
        mtu: constants::MIN_MTU_SIZE,
        client,
    };
    req.serialized().unwrap()
}

use crate::client::RakClient;
use crate::client::config::RakClientConfig;
use crate::client::input::RakClientInput;
use crate::client::output::RakClientOutput;

/// Completes a synthetic probe write before delivering its bytes to the test server.
fn client_datagram(
    client: &mut RakClient,
    output: RakClientOutput,
    now: Instant,
) -> Option<Box<[u8]>> {
    match output {
        RakClientOutput::SocketDatagram(bytes, _) => Some(bytes),
        RakClientOutput::MtuProbe { id, datagram, .. } => {
            client
                .handle(RakClientInput::MtuProbeResult {
                    id,
                    now,
                    elapsed: Duration::ZERO,
                    too_large: false,
                })
                .unwrap();
            Some(datagram)
        }
        _ => None,
    }
}

fn connect_all(
    server: &mut RakServer,
    server_addr: SocketAddr,
    client_addrs: &[SocketAddr],
    now: Instant,
) -> Vec<RakSession> {
    let mut clients: Vec<(SocketAddr, Option<RakClient>)> = client_addrs
        .iter()
        .map(|&addr| {
            let mut client = RakClient::new(
                RakClientConfig::default(),
                crate::clock::RakClock::new(crate::clock::test_now()),
            );
            client
                .handle(RakClientInput::Connect {
                    guid: crate::client::TEST_CLIENT_GUID,
                    remote: server_addr,
                    local: SocketAddr::new(server_addr.ip(), 54321),
                    now,
                })
                .unwrap();
            (addr, Some(client))
        })
        .collect();

    let mut connected = Vec::new();
    loop {
        let mut sent = false;
        for (addr, client) in &mut clients {
            let Some(client) = client else { continue };
            while let Some(out) = client.poll() {
                if let Some(buf) = client_datagram(client, out, now) {
                    sent = true;
                    let _ = server.handle(RakServerInput::Datagram(buf, *addr, now));
                }
            }
        }
        if !sent {
            return connected;
        }

        for out in drain(server) {
            match out {
                RakServerOutput::SocketDatagram(buf, to) => {
                    let Some((_, slot)) = clients.iter_mut().find(|(addr, _)| *addr == to) else {
                        continue;
                    };
                    if let Some(client) = slot
                        && client
                            .handle(RakClientInput::Datagram(buf, server_addr, now))
                            .is_err()
                    {
                        *slot = None;
                    }
                }
                RakServerOutput::SessionConnected(session) => connected.push(*session),
                _ => {}
            }
        }
    }
}

fn connect(
    server: &mut RakServer,
    server_addr: SocketAddr,
    client_addr: SocketAddr,
    now: Instant,
) -> Option<RakSession> {
    connect_all(server, server_addr, &[client_addr], now).pop()
}

fn open_pending(server: &mut RakServer, server_addr: SocketAddr, client_addr: SocketAddr) {
    let now = crate::clock::test_now();
    server
        .handle(RakServerInput::Datagram(
            request_1(constants::PROTOCOL),
            client_addr,
            now,
        ))
        .unwrap();
    let cookie = reply_cookie(server);
    server
        .handle(RakServerInput::Datagram(
            request_2(server_addr, 1, cookie),
            client_addr,
            now,
        ))
        .unwrap();
}

#[test]
fn a_client_completes_the_handshake() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );

    let session = connect(
        &mut server,
        server_addr,
        "127.0.0.1:2".parse().unwrap(),
        crate::clock::test_now(),
    );

    assert!(session.is_some());
}

fn online_datagram() -> Box<[u8]> {
    Box::new([flags::VALID, 0, 0, 0])
}

fn session_datagrams(server: &mut RakServer) -> usize {
    drain(server)
        .iter()
        .filter(|o| matches!(o, RakServerOutput::SessionDatagram(..)))
        .count()
}

#[test]
fn a_flood_of_online_datagrams_blocks_the_ip_for_a_while() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();
    let flooder: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let bystander: SocketAddr = "127.0.0.2:1".parse().unwrap();
    connect_all(&mut server, server_addr, &[flooder, bystander], now);
    drain(&mut server);
    let now = now + Duration::from_secs(1);

    let limit = server.config.online_packet_limit as usize;
    for _ in 0..limit * 2 {
        server
            .handle(RakServerInput::Datagram(online_datagram(), flooder, now))
            .unwrap();
    }
    assert_eq!(session_datagrams(&mut server), limit);

    let later = now + Duration::from_secs(1);
    for addr in [flooder, bystander] {
        server
            .handle(RakServerInput::Datagram(online_datagram(), addr, later))
            .unwrap();
    }
    assert_eq!(
        session_datagrams(&mut server),
        1,
        "only the bystander gets through"
    );

    let unblocked = now + server.config.block_duration;
    server
        .handle(RakServerInput::Datagram(
            online_datagram(),
            flooder,
            unblocked,
        ))
        .unwrap();
    assert_eq!(session_datagrams(&mut server), 1);
}

#[test]
fn expired_blocks_are_pruned_on_update() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    server
        .handle(RakServerInput::Block(
            "127.0.0.9".parse().unwrap(),
            Duration::from_secs(1),
            now,
        ))
        .unwrap();
    server
        .handle(RakServerInput::Update(now + Duration::from_secs(2)))
        .unwrap();

    assert!(server.blocked.is_empty());
}

#[test]
fn a_blocked_ip_is_ignored_until_unblocked() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();
    let client: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let ping = || {
        let mut buf = Vec::new();
        UnconnectedPing {
            timestamp: 0,
            client: 1,
        }
        .serialize(&mut buf)
        .unwrap();
        buf.into_boxed_slice()
    };

    server
        .handle(RakServerInput::Block(
            client.ip(),
            Duration::from_secs(60),
            now,
        ))
        .unwrap();
    server
        .handle(RakServerInput::Datagram(ping(), client, now))
        .unwrap();
    assert!(drain(&mut server).is_empty());

    server.handle(RakServerInput::Unblock(client.ip())).unwrap();
    server
        .handle(RakServerInput::Datagram(ping(), client, now))
        .unwrap();
    assert!(
        drain(&mut server)
            .iter()
            .any(|o| first_byte(o) == Some(packet_id::UNCONNECTED_PONG))
    );
}

#[test]
fn established_connections_never_exceed_max() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig {
            max_connections: 1,
            ..Default::default()
        },
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    let clients = [
        "127.0.0.1:1".parse().unwrap(),
        "127.0.0.1:2".parse().unwrap(),
    ];

    let sessions = connect_all(&mut server, server_addr, &clients, now);

    assert_eq!(sessions.len(), 1);
    assert_eq!(server.session_map.len(), 1);
    assert!(server.session_pending.is_empty());
}

#[test]
fn pending_session_closed_during_handshake_frees_its_slot() {
    use crate::protocol::packets::frame_set::FrameSet;
    use crate::protocol::types::frame::Frame;

    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );

    open_pending(&mut server, server_addr, client_addr);
    drain(&mut server);

    let mut frame = Frame::new(RakReliability::ReliableOrdered, Box::new([0xFE]));
    frame.order_index = 100_000;
    let set = FrameSet::new(0, vec![frame], false, true, false);

    server
        .handle(RakServerInput::Datagram(
            set.serialized().unwrap(),
            client_addr,
            crate::clock::test_now(),
        ))
        .unwrap();

    assert!(!server.session_pending.contains_key(&client_addr));
    assert!(!server.session_map.contains_key(&client_addr));
}

fn cookie_for(server: &mut RakServer, client_addr: SocketAddr, now: Instant) -> Option<i32> {
    server
        .handle(RakServerInput::Datagram(
            request_1(constants::PROTOCOL),
            client_addr,
            now,
        ))
        .unwrap();
    reply_cookie(server)
}

fn accepts_request_2(
    server: &mut RakServer,
    server_addr: SocketAddr,
    client_addr: SocketAddr,
    cookie: Option<i32>,
    now: Instant,
) -> bool {
    server
        .handle(RakServerInput::Datagram(
            request_2(server_addr, 1, cookie),
            client_addr,
            now,
        ))
        .unwrap();
    drain(server)
        .iter()
        .any(|o| first_byte(o) == Some(packet_id::OPEN_CONNECTION_REPLY_2))
        && server.session_pending.contains_key(&client_addr)
}

#[test]
fn reply_1_carries_a_cookie_by_default() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );

    let cookie = cookie_for(
        &mut server,
        "127.0.0.1:1".parse().unwrap(),
        crate::clock::test_now(),
    );

    assert!(cookie.is_some());
}

#[test]
fn request_2_without_a_cookie_is_ignored() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    cookie_for(&mut server, client_addr, now);

    assert!(!accepts_request_2(
        &mut server,
        server_addr,
        client_addr,
        None,
        now
    ));
}

#[test]
fn request_2_with_another_addresses_cookie_is_ignored() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let spoofed_addr: SocketAddr = "127.0.0.2:1".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    let cookie = cookie_for(&mut server, client_addr, now);

    assert!(!accepts_request_2(
        &mut server,
        server_addr,
        spoofed_addr,
        cookie,
        now
    ));
    assert!(accepts_request_2(
        &mut server,
        server_addr,
        client_addr,
        cookie,
        now
    ));
}

#[test]
fn cookie_survives_one_rotation_but_not_two() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    let recent: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let cookie = cookie_for(&mut server, recent, now);
    let later = now + COOKIE_ROTATION_INTERVAL + COOKIE_ROTATION_INTERVAL / 2;
    assert!(accepts_request_2(
        &mut server,
        server_addr,
        recent,
        cookie,
        later
    ));

    let stale: SocketAddr = "127.0.0.1:2".parse().unwrap();
    let cookie = cookie_for(&mut server, stale, later);
    let much_later = later + COOKIE_ROTATION_INTERVAL * 2;
    assert!(!accepts_request_2(
        &mut server,
        server_addr,
        stale,
        cookie,
        much_later
    ));
}

#[test]
fn cookies_can_be_disabled() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig {
            cookies: false,
            ..Default::default()
        },
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    assert_eq!(cookie_for(&mut server, client_addr, now), None);
    assert!(accepts_request_2(
        &mut server,
        server_addr,
        client_addr,
        None,
        now
    ));
}

#[test]
fn rate_limits_offline_packets_per_ip() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

    let mut server = RakServer::new(
        RakServerConfig {
            packet_limit: 2,
            ..Default::default()
        },
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );

    let ping = UnconnectedPing {
        timestamp: 0,
        client: 1,
    };

    let buf = ping.serialized().unwrap();

    let now = crate::clock::test_now();
    for _ in 0..5 {
        server
            .handle(RakServerInput::Datagram(buf.clone(), client_addr, now))
            .unwrap();
    }

    assert_eq!(drain(&mut server).len(), 2);
}

#[test]
fn rate_limit_is_shared_by_every_port_of_an_ip() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();

    let mut server = RakServer::new(
        RakServerConfig {
            packet_limit: 2,
            ..Default::default()
        },
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );

    let ping = UnconnectedPing {
        timestamp: 0,
        client: 1,
    };

    let buf = ping.serialized().unwrap();

    let now = crate::clock::test_now();
    for port in 1..=5 {
        let client_addr = SocketAddr::from(([127, 0, 0, 1], port));
        server
            .handle(RakServerInput::Datagram(buf.clone(), client_addr, now))
            .unwrap();
    }

    assert_eq!(drain(&mut server).len(), 2);
}

#[test]
fn already_connected_reply_on_duplicate_request() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    server
        .handle(RakServerInput::Datagram(
            request_1(constants::PROTOCOL),
            client_addr,
            now,
        ))
        .unwrap();
    let cookie = reply_cookie(&mut server);

    server
        .handle(RakServerInput::Datagram(
            request_2(server_addr, 1, cookie),
            client_addr,
            now,
        ))
        .unwrap();
    drain(&mut server);

    server
        .handle(RakServerInput::Datagram(
            request_2(server_addr, 1, cookie),
            client_addr,
            now,
        ))
        .unwrap();

    let outputs = drain(&mut server);
    assert!(
        outputs
            .iter()
            .any(|o| first_byte(o) == Some(packet_id::ALREADY_CONNECTED))
    );
}

#[test]
fn ip_recently_connected_reply_after_disconnect() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    server
        .handle(RakServerInput::Datagram(
            request_1(constants::PROTOCOL),
            client_addr,
            now,
        ))
        .unwrap();
    let cookie = reply_cookie(&mut server);

    server
        .handle(RakServerInput::Datagram(
            request_2(server_addr, 1, cookie),
            client_addr,
            now,
        ))
        .unwrap();
    drain(&mut server);

    let id = *server.session_map.get(&client_addr).unwrap();
    server
        .handle(RakServerInput::RemoveSession(id, now))
        .unwrap();

    server
        .handle(RakServerInput::Datagram(
            request_1(constants::PROTOCOL),
            client_addr,
            now,
        ))
        .unwrap();

    let outputs = drain(&mut server);
    assert!(
        outputs
            .iter()
            .any(|o| first_byte(o) == Some(packet_id::IP_RECENTLY_CONNECTED))
    );
}

#[test]
fn adopted_session_continues_on_a_second_server() {
    let peer_addr: SocketAddr = "127.0.0.1:40000".parse().unwrap();

    // A session whose sequence state has advanced past its initial values.
    let mut server_a = RakServer::new(
        RakServerConfig::default(),
        "127.0.0.1:19160".parse().unwrap(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let id_on_a = RakSessionId(7);
    server_a.session_map.insert(peer_addr, id_on_a);
    server_a.session_addr.insert(id_on_a, peer_addr);

    let mut session = RakSession::new(
        id_on_a,
        peer_addr,
        0xDEAD_BEEF,
        constants::MAX_MTU_SIZE,
        crate::clock::test_now(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );
    let now = crate::clock::test_now();
    session
        .handle(RakSessionInput::Send(
            b"hello".to_vec().into_boxed_slice(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        ))
        .unwrap();
    let before_seq = session.outbound_rel;
    assert!(
        before_seq > 0,
        "sending a reliable frame must advance the sequence counter"
    );

    // A separate instance whose id counter is already past server A's, so reusing
    // the incoming id would collide.
    let mut server_b = RakServer::new(
        RakServerConfig::default(),
        "127.0.0.1:19161".parse().unwrap(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    server_b.session_id = RakSessionId(id_on_a.0 + 100);

    server_b.adopt(session);

    let outputs = drain(&mut server_b);
    let adopted = outputs.into_iter().find_map(|o| match o {
        RakServerOutput::SessionConnected(session) => Some(*session),
        _ => None,
    });
    let adopted = adopted.expect("adopt must emit SessionConnected");

    assert_eq!(
        adopted.outbound_rel, before_seq,
        "sequence state must carry over unchanged"
    );
    assert_ne!(
        adopted.id, id_on_a,
        "a colliding id from another server must not be reused as-is"
    );
    assert_eq!(
        *server_b.session_map.get(&peer_addr).unwrap(),
        adopted.id,
        "future datagrams from the peer must route to the adopted session's new id"
    );

    server_b
        .handle(RakServerInput::Datagram(
            b"\xffnoise".to_vec().into_boxed_slice(),
            peer_addr,
            now,
        ))
        .unwrap();
    let routed = drain(&mut server_b);
    assert!(
        routed
            .iter()
            .any(|o| matches!(o, RakServerOutput::SessionDatagram(_, id) if *id == adopted.id)),
        "a datagram from the adopted session's peer must be routed by its new id"
    );
}

#[test]
fn evicts_stale_pending_connection() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();

    server
        .handle(RakServerInput::Datagram(
            request_1(constants::PROTOCOL),
            client_addr,
            now,
        ))
        .unwrap();
    let cookie = reply_cookie(&mut server);

    server
        .handle(RakServerInput::Datagram(
            request_2(server_addr, 1, cookie),
            client_addr,
            now,
        ))
        .unwrap();
    drain(&mut server);

    assert!(server.session_pending.contains_key(&client_addr));

    let later = now + PENDING_CONNECTION_TIMEOUT;
    server.handle(RakServerInput::Update(later)).unwrap();

    assert!(!server.session_pending.contains_key(&client_addr));
    assert!(!server.session_map.contains_key(&client_addr));
}

#[test]
fn pending_session_acks_and_resends_connection_request_accepted() {
    let server_addr: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:2".parse().unwrap();
    let mut server = RakServer::new(
        RakServerConfig::default(),
        server_addr,
        crate::clock::RakClock::new(crate::clock::test_now()),
        crate::clock::test_now(),
    );
    let now = crate::clock::test_now();
    let mut client = RakClient::new(
        RakClientConfig::default(),
        crate::clock::RakClock::new(crate::clock::test_now()),
    );
    client
        .handle(RakClientInput::Connect {
            guid: crate::client::TEST_CLIENT_GUID,
            remote: server_addr,
            local: SocketAddr::new(server_addr.ip(), 54321),
            now,
        })
        .unwrap();

    loop {
        let mut sent = false;
        while let Some(out) = client.poll() {
            if let Some(buf) = client_datagram(&mut client, out, now) {
                sent = true;
                server
                    .handle(RakServerInput::Datagram(buf, client_addr, now))
                    .unwrap();
            }
        }
        if !sent {
            break;
        }
        for out in drain(&mut server) {
            if let RakServerOutput::SocketDatagram(buf, _) = out
                && buf.first().is_some_and(|b| b & flags::VALID == 0)
            {
                client
                    .handle(RakClientInput::Datagram(buf, server_addr, now))
                    .unwrap();
            }
        }
    }
    assert!(server.session_pending.contains_key(&client_addr));

    server
        .handle(RakServerInput::Update(now + Duration::from_millis(20)))
        .unwrap();
    let acked = drain(&mut server).iter().any(|out| {
        matches!(out, RakServerOutput::SocketDatagram(buf, _)
            if buf.first().is_some_and(|b| b & flags::ACK != 0))
    });
    assert!(
        acked,
        "pending session never acknowledged the ConnectionRequest"
    );

    server
        .handle(RakServerInput::Update(now + Duration::from_millis(2100)))
        .unwrap();
    let resent = drain(&mut server).iter().any(|out| {
        matches!(out, RakServerOutput::SocketDatagram(buf, _)
            if buf.first().is_some_and(|b| b & flags::VALID != 0 && b & flags::ACK == 0 && b & flags::NACK == 0))
    });
    assert!(
        resent,
        "pending session never resent ConnectionRequestAccepted"
    );
}

fn accepted_reply_to(client_addr: SocketAddr) -> ConnectionRequestAccepted {
    use crate::protocol::packets::frame_set::FrameSet;

    let now = crate::clock::test_now();
    let mut session = RakSession::new(
        RakSessionId(0),
        client_addr,
        0,
        constants::MAX_MTU_SIZE,
        crate::clock::test_now(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );
    let request = ConnectionRequest {
        client_guid: 1,
        client_timestamp: 2,
        security: false,
    };
    let mut buf = Vec::new();
    request.serialize(&mut buf).unwrap();
    RakServer::handle_connection_request(&mut session, client_addr, &mut Cursor::new(&buf), now)
        .unwrap();
    session.tick(now).unwrap();

    std::iter::from_fn(|| session.poll())
        .find_map(|out| match out {
            RakSessionOutput::Datagram(buf, _) => Some(buf),
            _ => None,
        })
        .map(|buf| FrameSet::deserialize(&mut Cursor::new(buf.as_ref())).unwrap())
        .map(|set| {
            ConnectionRequestAccepted::deserialize(&mut set.frames[0].payload.as_ref()).unwrap()
        })
        .unwrap()
}

#[test]
fn connection_request_accepted_carries_ten_system_addresses_of_the_client_family() {
    for client_addr in ["127.0.0.1:2", "[::1]:2"] {
        let client_addr: SocketAddr = client_addr.parse().unwrap();
        let accepted = accepted_reply_to(client_addr);

        assert_eq!(accepted.client_address, client_addr);
        assert_eq!(accepted.system_addresses.len(), 10);
        assert!(
            accepted
                .system_addresses
                .iter()
                .all(|addr| addr.is_ipv4() == client_addr.is_ipv4())
        );
        assert_eq!(accepted.request_timestamp, 2);
    }
}
