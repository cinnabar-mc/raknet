//! Trusted handoff admission uses the serving owner's actual capacity and route.

use raknet::prelude::*;
use std::net::SocketAddr;
use std::time::Instant;

#[path = "support/negotiated.rs"]
mod negotiated;

/// Obtains both original owners through the existing public wire handshake.
fn pair(peer: u16, now: Instant) -> (RakSession<Established>, RakSession<Established>) {
    negotiated::negotiated_pair(
        "127.0.0.1:19132".parse().unwrap(),
        SocketAddr::from(([127, 0, 0, 1], peer)),
        now,
        now,
        Default::default(),
    )
}

/// Uses the configured public server, without fabricating admitted routes.
fn server(max_connections: usize, now: Instant) -> RakServer {
    RakServer::new(
        RakServerConfig {
            max_connections,
            ..Default::default()
        },
        "127.0.0.1:19132".parse().unwrap(),
        RakClock::new(now),
        now,
    )
    .unwrap()
}

/// Takes only the actual handoff returned by a successful established adoption.
fn established(server: &mut RakServer) -> RakSession<Established> {
    match server.poll().expect("the real adoption produced a handoff") {
        RakServerOutput::SessionConnected(session) => session,
        other => panic!("unexpected handoff: {other:?}"),
    }
}

/// Confirms that a recovered genuine owner still sends through its original peer.
fn delivers(mut session: RakSession<Established>, mut peer: RakSession<Established>, now: Instant) {
    let marker = b"\xfeoriginal rejected-owner capability";
    session
        .send(
            marker.to_vec().into_boxed_slice(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    while let Some(output) = session.poll() {
        if let RakSessionOutput::Datagram(bytes, _) = output {
            peer.handle(RakSessionInput::Datagram(bytes, now)).unwrap();
        }
    }
    let mut delivered = Vec::new();
    while let Some(output) = peer.poll() {
        if let RakSessionOutput::Packet(bytes) = output {
            delivered.push(bytes);
        }
    }
    drop((session, peer));
    assert_eq!(delivered, [marker.to_vec().into_boxed_slice()]);
}

#[test]
fn adoption_respects_zero_and_full_capacity_without_consuming_rejected_phases() {
    let now = Instant::now();
    let mut refused = Vec::new();
    for limit in [0, 1] {
        let mut server = server(limit, now);
        let guard = if limit != 0 {
            let (original, peer) = pair(19133, now);
            server.adopt(original).unwrap();
            Some((established(&mut server), peer))
        } else {
            None
        };
        let (candidate, peer) = pair(19134, now);
        let (rejected, recovered) = match server.adopt(candidate) {
            Err(error) => {
                let (session, cause) = error.into_parts();
                assert!(matches!(cause, RakServerError::RefusingConnection(_)));
                (true, session)
            }
            Ok(_) => (false, established(&mut server)),
        };
        delivers(recovered, peer, now);
        refused.push(rejected);
        drop((guard, server));
    }

    let mut server = server(0, now);
    let (mut candidate, peer) = pair(19135, now);
    candidate
        .send(
            Box::from(&b"\xfeoriginal pending reliable tail"[..]),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    let mut candidate = candidate.close(now).unwrap();
    while candidate.poll().is_some() {}
    let pending = candidate.outbound_bytes();
    let (rejected, recovered) = match server.adopt(candidate) {
        Err(error) => {
            let (session, cause) = error.into_parts();
            assert!(matches!(cause, RakServerError::RefusingConnection(_)));
            (true, session)
        }
        Ok(_) => match server
            .poll()
            .expect("the original closing handoff is owned")
        {
            RakServerOutput::SessionClosing(session) => (false, session),
            other => panic!("closing adoption changed its phase: {other:?}"),
        },
    };
    let closing_preserved = recovered.state() == RakSessionState::Closing
        && pending != 0
        && recovered.outbound_bytes() == pending;
    refused.push(rejected);
    drop((recovered, peer, server));
    assert!(
        closing_preserved,
        "rejected Closing must retain its original reliable work"
    );
    assert!(
        refused.into_iter().all(|refused| refused),
        "trusted adoption bypassed the same configured zero/full serving capacity"
    );
}

#[test]
fn duplicate_adoption_preserves_the_original_route_and_owned_handoff() {
    let now = Instant::now();
    let mut server = server(2, now);
    let (original, mut peer) = pair(19136, now);
    let duplicate = match original
        .snapshot(now)
        .unwrap()
        .restore(now, Default::default())
        .unwrap()
    {
        RestoredSession::Established(session) => session,
        other => panic!("a genuine established capture changed phase: {other:?}"),
    };
    let first_id = server.adopt(original).unwrap();
    let mut original = established(&mut server);
    let rejected = match server.adopt(duplicate) {
        Err(error) => {
            let (duplicate, cause) = error.into_parts();
            assert!(matches!(cause, RakServerError::RefusingConnection(_)));
            drop(duplicate);
            true
        }
        Ok(_) => {
            drop(established(&mut server));
            false
        }
    };
    let marker = b"\xfeoriginal route after duplicate handoff";
    peer.send(
        marker.to_vec().into_boxed_slice(),
        RakReliability::ReliableOrdered,
        RakPriority::Immediate,
        now,
    )
    .unwrap();
    while let Some(output) = peer.poll() {
        if let RakSessionOutput::Datagram(bytes, _) = output {
            server
                .handle(RakServerInput::Datagram(bytes, original.addr(), now))
                .unwrap();
        }
    }
    let mut routes = Vec::new();
    while let Some(output) = server.poll() {
        if let RakServerOutput::SessionDatagram(bytes, id) = output {
            routes.push(id);
            original
                .handle(RakSessionInput::Datagram(bytes, now))
                .unwrap();
        }
    }
    let mut delivered = Vec::new();
    while let Some(output) = original.poll() {
        if let RakSessionOutput::Packet(bytes) = output {
            delivered.push(bytes);
        }
    }
    drop((original, peer, server));
    assert_eq!(delivered, [marker.to_vec().into_boxed_slice()]);
    assert!(
        !routes.is_empty(),
        "the actual generated peer datagram must route"
    );
    assert!(
        rejected && routes.into_iter().all(|id| id == first_id),
        "duplicate trusted adoption replaced the existing exact session route"
    );
}
