//! Real frame-driven peers retain finite application delivery through retirement.

use super::*;
use crate::client::RakClient;
use raknet::protocol::codec::RakCodec;

pub(crate) const RECEIVE_BYTES: usize = 512;
const BODY_BYTES: usize = RECEIVE_BYTES / 4;
const BODY_COUNT: usize = RECEIVE_BYTES / BODY_BYTES + 2;
pub(crate) const PROGRESS_WATCHDOG: usize = 1024;

/// Binds the original public accepting owner and reads its actual ephemeral endpoint.
pub(crate) fn server(max_connections: usize) -> (RakServer, SocketAddr) {
    let server = RakServer::new("127.0.0.1:0".parse().unwrap(), |config| {
        config.max_connections = max_connections;
        config.session.max_queued_bytes = RECEIVE_BYTES.try_into().unwrap();
    })
    .unwrap();
    let address = server.socket.local_addr().unwrap();
    (server, address)
}

/// Uses the same valid small byte policy on the actual public client edge.
pub(crate) fn client() -> RakClient {
    RakClient::new(|config| {
        config.session.max_queued_bytes = RECEIVE_BYTES.try_into().unwrap();
    })
    .unwrap()
}

/// Drives the existing nonblocking socket owners without replacing their clock or sleeping.
pub(crate) fn step(server: &mut RakServer, clients: &mut [&mut RakClient]) {
    server.update();
    for client in clients {
        client.update();
    }
}

#[derive(Clone, Copy)]
enum Attempt {
    Connected(RakSessionId),
    Refused,
    Stalled,
}

/// Observes only the public handshake's actual connected capability or completed refusal.
fn connect(server: &mut RakServer, address: SocketAddr, client: &mut RakClient) -> Attempt {
    client.idle().unwrap().connect(address).unwrap();
    let mut id = None;
    for _ in 0..PROGRESS_WATCHDOG {
        step(server, &mut [&mut *client]);
        while let Some(event) = server.next_event() {
            if let RakServerEvent::SessionConnected { id: connected, .. } = event {
                id = Some(connected);
            }
        }
        while client.next_event().is_some() {}
        if let Some(id) = id
            && client.connected().is_some()
            && server.connected(id).is_some()
        {
            return Attempt::Connected(id);
        }
        if client.idle().is_some() {
            return Attempt::Refused;
        }
    }
    Attempt::Stalled
}

/// Requires the original bidirectional handshake instead of constructing a session phase.
fn established(
    server: &mut RakServer,
    address: SocketAddr,
    client: &mut RakClient,
) -> RakSessionId {
    match connect(server, address, client) {
        Attempt::Connected(id) => id,
        Attempt::Refused | Attempt::Stalled => panic!("setup: original loopback handshake failed"),
    }
}

#[test]
fn normal_sends_reach_both_sockets_without_an_update() {
    let (mut server, address) = server(1);
    let mut client = client();
    let id = established(&mut server, address, &mut client);
    let marker = Box::<[u8]>::from(&b"\xfenormal send without a frame update"[..]);
    client
        .connected()
        .unwrap()
        .send(
            marker.clone(),
            RakReliability::ReliableOrdered,
            RakPriority::Normal,
        )
        .unwrap();
    let mut buffer = [0; 1492];
    let (len, _) = server
        .socket
        .recv_from(&mut buffer)
        .expect("client sent UDP");
    let set =
        raknet::protocol::packets::frame_set::FrameSet::deserialize(&mut &buffer[..len]).unwrap();
    assert!(set.frames.iter().any(|frame| frame.payload == marker));

    server
        .connected(id)
        .unwrap()
        .send(
            marker.clone(),
            RakReliability::ReliableOrdered,
            RakPriority::Normal,
        )
        .unwrap();
    let len = crate::client::bounded_delivery_tests::receive_datagram(&client, &mut buffer)
        .expect("server sent UDP");
    let set =
        raknet::protocol::packets::frame_set::FrameSet::deserialize(&mut &buffer[..len]).unwrap();
    assert!(set.frames.iter().any(|frame| frame.payload == marker));
}

/// Sends through the live public capability with reliable wire ordering.
fn send(client: &mut RakClient, bytes: &[u8]) {
    client
        .connected()
        .unwrap()
        .send(
            Box::<[u8]>::from(bytes),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
        )
        .unwrap();
}

type Echo = (Box<[u8]>, Box<[u8]>);

/// Echoes one real application message while another peer's receive remains untouched.
fn exchange(
    server: &mut RakServer,
    client: &mut RakClient,
    id: RakSessionId,
    mut other: Option<&mut RakClient>,
    marker: &[u8],
) -> Option<Echo> {
    send(client, marker);
    let mut request = None;
    for _ in 0..PROGRESS_WATCHDOG {
        server.update();
        if let Some(other) = other.as_deref_mut() {
            other.update();
        }
        client.update();
        if request.is_none()
            && let Some(bytes) = server.connected(id).and_then(|mut peer| peer.recv())
        {
            server
                .connected(id)
                .unwrap()
                .send(
                    bytes.clone(),
                    RakReliability::ReliableOrdered,
                    RakPriority::Immediate,
                )
                .unwrap();
            request = Some(bytes);
        }
        if let Some(bytes) = client.recv() {
            return Some((request.expect("the echo follows the actual request"), bytes));
        }
    }
    None
}

/// Distinct valid application bodies make both retained prefixes observable.
fn bodies() -> Vec<Box<[u8]>> {
    (0..BODY_COUNT)
        .map(|index| {
            let mut body = vec![index as u8; BODY_BYTES];
            body[0] = 0xfe;
            body.into_boxed_slice()
        })
        .collect()
}

#[test]
fn stalled_bevy_receivers_retain_a_bounded_prefix_without_retiring_another_peer() {
    let (mut server, address) = server(2);
    let mut stalled = client();
    let stalled_id = established(&mut server, address, &mut stalled);
    let mut healthy = client();
    let healthy_id = established(&mut server, address, &mut healthy);
    let expected = bodies();
    for body in &expected {
        send(&mut stalled, body);
        server
            .connected(stalled_id)
            .unwrap()
            .send(
                body.clone(),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .unwrap();
    }

    let marker = b"\xfehealthy before draining the stalled peer";
    let healthy_before = exchange(
        &mut server,
        &mut healthy,
        healthy_id,
        Some(&mut stalled),
        marker,
    );
    let server_retired = server.connected(stalled_id).is_none();
    let client_retired = stalled.connected().is_none();
    let server_prefix: Vec<_> = std::iter::from_fn(|| server.recv()).collect();
    let client_prefix: Vec<_> = std::iter::from_fn(|| stalled.recv()).collect();

    let after_marker = b"\xfehealthy after the original receive budget was released";
    let healthy_after = exchange(
        &mut server,
        &mut healthy,
        healthy_id,
        Some(&mut stalled),
        after_marker,
    );
    let late_server: Vec<_> = std::iter::from_fn(|| server.recv()).collect();
    let late_client: Vec<_> = std::iter::from_fn(|| stalled.recv()).collect();
    let server_events: Vec<_> = std::iter::from_fn(|| server.next_event()).collect();
    let client_events: Vec<_> = std::iter::from_fn(|| stalled.next_event()).collect();
    drop((stalled, healthy, server));

    let (request, response) = healthy_before.expect("setup: independent peer did not echo");
    assert_eq!(request.as_ref(), marker);
    assert_eq!(response.as_ref(), marker);
    let (request, response) = healthy_after.expect("setup: independent peer stopped after receive");
    assert_eq!(request.as_ref(), after_marker);
    assert_eq!(response.as_ref(), after_marker);
    assert!(
        server_retired && client_retired,
        "stalled application receive did not retire the original finite delivery owner"
    );
    assert_eq!(client_prefix, expected[..RECEIVE_BYTES / BODY_BYTES]);
    assert_eq!(
        server_prefix,
        expected[..RECEIVE_BYTES / BODY_BYTES]
            .iter()
            .cloned()
            .map(|body| (stalled_id, body))
            .collect::<Vec<_>>()
    );
    assert!(late_server.is_empty() && late_client.is_empty());
    assert_eq!(
        server_events
            .iter()
            .filter(|event| matches!(event, RakServerEvent::SessionDisconnected { id, .. } if *id == stalled_id))
            .count(),
        1
    );
    assert_eq!(
        client_events
            .iter()
            .filter(|event| matches!(event, crate::client::RakClientEvent::Disconnected { .. }))
            .count(),
        1
    );
}

#[test]
fn an_unread_retired_bevy_peer_keeps_its_original_admission_slot() {
    let (mut server, address) = server(1);
    let mut first = client();
    let first_id = established(&mut server, address, &mut first);
    let retained = Box::<[u8]>::from(&b"\xfeoriginal unread message"[..]);
    send(&mut first, &retained);
    let mut received = false;
    for _ in 0..PROGRESS_WATCHDOG {
        step(&mut server, &mut [&mut first]);
        if server.received.iter().any(|(id, _)| *id == first_id) {
            received = true;
            break;
        }
    }
    drop(first);
    let mut retired = false;
    for _ in 0..PROGRESS_WATCHDOG {
        server.update();
        if server.connected(first_id).is_none() {
            retired = true;
            break;
        }
    }
    let terminal_count = std::iter::from_fn(|| server.next_event())
        .filter(|event| matches!(event, RakServerEvent::SessionDisconnected { id, .. } if *id == first_id))
        .count();

    let mut next = client();
    let before_drain = connect(&mut server, address, &mut next);
    let original_prefix: Vec<_> = std::iter::from_fn(|| server.recv()).collect();
    let after_drain = match before_drain {
        Attempt::Connected(id) => Attempt::Connected(id),
        Attempt::Refused => connect(&mut server, address, &mut next),
        Attempt::Stalled => Attempt::Stalled,
    };
    let marker = b"\xfeadmitted after the original prefix drained";
    let subsequent = match after_drain {
        Attempt::Connected(id) => exchange(&mut server, &mut next, id, None, marker),
        Attempt::Refused | Attempt::Stalled => None,
    };
    drop((next, server));

    assert!(
        received && retired,
        "setup: original data/retirement did not complete"
    );
    assert_eq!(terminal_count, 1);
    assert_eq!(original_prefix, [(first_id, retained)]);
    let (request, response) = subsequent.expect("setup: post-drain peer could not echo");
    assert_eq!(request.as_ref(), marker);
    assert_eq!(response.as_ref(), marker);
    assert!(
        matches!(before_drain, Attempt::Refused),
        "retired peer released its admission slot while original accepted data was still unread"
    );
}
