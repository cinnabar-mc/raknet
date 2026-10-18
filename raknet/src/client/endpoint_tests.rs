use super::*;
use crate::protocol::packets::frame_set::FrameSet;
use std::net::{Ipv6Addr, SocketAddrV6};

#[test]
fn incoming_connection_advertises_the_bound_endpoint_and_twenty_system_addresses() {
    for (remote, local) in [
        ("192.0.2.1:19132", "192.0.2.2:54321"),
        ("[2001:db8::1]:19132", "[2001:db8::2]:54321"),
    ] {
        let remote: SocketAddr = remote.parse().unwrap();
        let local: SocketAddr = local.parse().unwrap();
        let now = crate::clock::test_now() + Duration::from_secs(1000);
        let mut session = SessionEngine::new(
            RakSessionId(0),
            remote,
            1,
            1492,
            now,
            crate::clock::RakClock::new(crate::clock::test_now()),
            Default::default(),
        );
        let accepted = ConnectionRequestAccepted {
            client_address: "198.51.100.3:60000".parse().unwrap(),
            system_index: 0,
            system_addresses: Vec::new(),
            request_timestamp: 42,
            timestamp: 100,
        }
        .serialized()
        .unwrap();
        ClientEngine::handle_connection_request_accepted(
            &mut session,
            remote,
            local,
            &mut Cursor::new(accepted.as_ref()),
            now,
        )
        .unwrap();
        let mut packets = Vec::new();
        while let Some(output) = session.poll() {
            if let RakSessionOutput::Datagram(bytes, _) = output {
                packets.extend(
                    FrameSet::deserialize(&mut Cursor::new(bytes.as_ref()))
                        .unwrap()
                        .frames,
                );
            }
        }
        let incoming =
            NewIncomingConnection::deserialize(&mut Cursor::new(packets[0].payload.as_ref()))
                .unwrap();
        assert_eq!(incoming.server_address, remote);
        assert_eq!(incoming.internal_addresses.first(), Some(&local));
        assert_eq!(incoming.internal_addresses.len(), 20);
        assert!(
            incoming.internal_addresses[1..]
                .iter()
                .all(|addr| *addr == SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        );
        assert_eq!(incoming.incoming_timestamp, 100);
        assert_eq!(incoming.server_timestamp, 1_000_000);
    }
}

#[test]
fn wire_addresses_unmap_ipv4_and_omit_ipv6_routing_metadata() {
    let mapped: SocketAddr = "[::ffff:127.0.0.1]:19132".parse().unwrap();
    assert_eq!(
        mapped.serialized().unwrap().as_ref(),
        &[4, 0x80, 0xff, 0xff, 0xfe, 0x4a, 0xbc]
    );
    let scoped = SocketAddr::V6(SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        19132,
        0x01020304,
        7,
    ));
    let mut expected = vec![6, 23, 0, 0x4a, 0xbc, 0, 0, 0, 0];
    expected.extend_from_slice(&"fe80::1".parse::<Ipv6Addr>().unwrap().octets());
    expected.extend_from_slice(&[0; 4]);
    assert_eq!(scoped.serialized().unwrap().as_ref(), expected);
    assert_eq!(canonical_socket_addr(scoped), scoped);
    let mut received = expected;
    received[5..9].copy_from_slice(&0x01020304_u32.to_be_bytes());
    received[25..29].copy_from_slice(&7_u32.to_be_bytes());
    assert_eq!(
        SocketAddr::deserialize(&mut Cursor::new(received)).unwrap(),
        SocketAddr::new(scoped.ip(), scoped.port())
    );
    assert_eq!(mapped.size_hint(), 7);
    assert_eq!(
        SocketAddr::deserialize(&mut Cursor::new([0, 0xff, 0xff, 0xff, 0xff, 0, 0])).unwrap(),
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
    );
}

#[test]
fn accepted_address_lists_tolerate_short_lists_but_reject_an_extra_entry() {
    for (count, system_address) in [
        (0, "0.0.0.0:0"),
        (10, "0.0.0.0:0"),
        (20, "0.0.0.0:0"),
        (20, "[::]:0"),
    ] {
        let packet = ConnectionRequestAccepted {
            client_address: "[::1]:19132".parse().unwrap(),
            system_index: 0,
            system_addresses: vec![system_address.parse().unwrap(); count],
            request_timestamp: 42,
            timestamp: 100,
        }
        .serialized()
        .unwrap();
        let decoded =
            ConnectionRequestAccepted::deserialize(&mut Cursor::new(packet.as_ref())).unwrap();
        assert_eq!(decoded.system_addresses.len(), count);
        assert_eq!((decoded.request_timestamp, decoded.timestamp), (42, 100));
        let incoming = NewIncomingConnection {
            server_address: decoded.client_address,
            internal_addresses: decoded.system_addresses,
            incoming_timestamp: 42,
            server_timestamp: 100,
        }
        .serialized()
        .unwrap();
        let decoded =
            NewIncomingConnection::deserialize(&mut Cursor::new(incoming.as_ref())).unwrap();
        assert_eq!(decoded.internal_addresses.len(), count);
        assert_eq!(
            (decoded.incoming_timestamp, decoded.server_timestamp),
            (42, 100)
        );
        assert!(
            NewIncomingConnection::deserialize(&mut Cursor::new(&incoming[..incoming.len() - 1]))
                .is_err()
        );
        if count == 20 {
            let mut excessive_incoming = incoming.to_vec();
            let at = excessive_incoming.len() - 16;
            excessive_incoming.splice(at..at, [4, 0xff, 0xff, 0xff, 0xff, 0, 0]);
            assert!(
                NewIncomingConnection::deserialize(&mut Cursor::new(excessive_incoming)).is_err()
            );
            let mut oversized = packet.to_vec();
            let at = oversized.len() - 16;
            oversized.splice(at..at, [4, 0xff, 0xff, 0xff, 0xff, 0, 0]);
            assert!(ConnectionRequestAccepted::deserialize(&mut Cursor::new(oversized)).is_err());
        }
    }
}
