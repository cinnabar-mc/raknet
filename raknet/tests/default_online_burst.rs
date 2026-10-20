//! Established application traffic passes through the original default server route.

use raknet::prelude::*;
use std::net::SocketAddr;
use std::time::Instant;

#[path = "support/negotiated.rs"]
mod negotiated;

/// Exchanges the original sessions' wire output at one caller-selected instant.
fn exchange(
    client: &mut RakSession<Established>,
    peer: &mut RakSession<Established>,
    server: &mut RakServer,
    now: Instant,
    received: &mut Vec<Box<[u8]>>,
) -> usize {
    let mut datagrams = 0;
    for _ in 0..16 {
        let mut moved = false;
        client.tick(now).unwrap();
        while let Some(output) = client.poll() {
            match output {
                RakSessionOutput::Datagram(bytes, destination) => {
                    assert_eq!(destination, client.addr());
                    server
                        .handle(RakServerInput::Datagram(bytes, peer.addr(), now))
                        .unwrap();
                    datagrams += 1;
                    moved = true;
                }
                RakSessionOutput::Wait(_) => {}
                output => panic!("unexpected client output: {output:?}"),
            }
        }
        while let Some(output) = server.poll() {
            match output {
                RakServerOutput::SessionDatagram(bytes, id) => {
                    assert_eq!(id, peer.id());
                    peer.handle(RakSessionInput::Datagram(bytes, now)).unwrap();
                }
                output => panic!("unexpected established route output: {output:?}"),
            }
        }
        peer.tick(now).unwrap();
        while let Some(output) = peer.poll() {
            match output {
                RakSessionOutput::Datagram(bytes, destination) => {
                    assert_eq!(destination, peer.addr());
                    client
                        .handle(RakSessionInput::Datagram(bytes, now))
                        .unwrap();
                    moved = true;
                }
                RakSessionOutput::Packet(bytes) => received.push(bytes),
                RakSessionOutput::Wait(_) => {}
                output => panic!("unexpected peer output: {output:?}"),
            }
        }
        if !moved {
            return datagrams;
        }
    }
    panic!("a small reliable exchange did not quiesce within its work bound");
}

#[test]
fn a_default_server_preserves_an_established_reliable_application_burst() {
    let now = Instant::now();
    let local: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    let remote: SocketAddr = "127.0.0.1:19132".parse().unwrap();
    let (mut client, peer) =
        negotiated::negotiated_pair(local, remote, now, now, Default::default());
    let mut server = RakServer::new(Default::default(), remote, RakClock::new(now), now).unwrap();
    let id = server.adopt(peer).unwrap();
    let Some(RakServerOutput::SessionConnected(mut peer)) = server.poll() else {
        panic!("the actual negotiated owner must be handed off after adoption");
    };
    assert_eq!(peer.id(), id);
    let mut received = Vec::new();
    let mut datagrams = 0;
    let expected: Vec<Box<[u8]>> = (u8::MIN..=u8::MAX)
        .map(|sequence| Box::<[u8]>::from([0xfe, sequence]))
        .collect();
    for bytes in &expected {
        client
            .send(
                bytes.clone(),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
                now,
            )
            .unwrap();
        datagrams += exchange(&mut client, &mut peer, &mut server, now, &mut received);
    }
    let first_delivered = received.first() == expected.first();
    let connected =
        client.state() == RakSessionState::Connected && peer.state() == RakSessionState::Connected;

    // Drain both original abort owners even when the endpoint rejected later wire input.
    let mut client = client.abort(now).unwrap();
    let mut peer = peer.abort(now).unwrap();
    while client.poll().is_some() {}
    while peer.poll().is_some() {}
    assert!(client.finish().is_ok());
    assert!(peer.finish().is_ok());
    server
        .handle(RakServerInput::RemoveSession(id, now))
        .unwrap();
    assert!(server.poll().is_none());
    drop(server);

    assert!(
        first_delivered,
        "the genuine established route must deliver data"
    );
    assert!(
        connected,
        "the protocol peers must remain established during the burst"
    );
    assert_eq!(
        received, expected,
        "the default endpoint discarded a valid established application burst ({datagrams} original datagrams emitted)"
    );
}
