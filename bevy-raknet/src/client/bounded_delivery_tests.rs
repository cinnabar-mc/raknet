//! Discovery replies remain bounded without being confused with game delivery.

use crate::server::bounded_delivery_tests::{
    PROGRESS_WATCHDOG, RECEIVE_BYTES, client, server, step,
};

/// Borrows the original client socket without driving session timers.
pub(crate) fn socket(client: &super::RakClient) -> &std::net::UdpSocket {
    &client.socket
}

#[test]
fn unread_bevy_discovery_pongs_keep_only_the_bounded_accepted_prefix() {
    let (mut server, address) = server(1);
    let mut client = client();
    let mut replies = Vec::new();
    for index in 0_u8..3 {
        let message = vec![index; RECEIVE_BYTES / 2 + 1].into_boxed_slice();
        server.set_message(message.clone());
        let before = (client.pongs.len(), client.dropped_pongs);
        client.ping(address);
        let mut completed = false;
        for _ in 0..PROGRESS_WATCHDOG {
            step(&mut server, &mut [&mut client]);
            if (client.pongs.len(), client.dropped_pongs) != before {
                completed = true;
                break;
            }
            std::thread::yield_now();
        }
        assert!(
            completed,
            "setup: the actual discovery reply did not arrive"
        );
        replies.push(message);
    }

    // This smaller reply fits beside the original retained reply and observes later socket work.
    let marker = Box::<[u8]>::from(&b"done"[..]);
    server.set_message(marker.clone());
    client.ping(address);
    let mut completed = false;
    for _ in 0..PROGRESS_WATCHDOG {
        step(&mut server, &mut [&mut client]);
        if client
            .pongs
            .iter()
            .any(|(_, message, _)| message == &marker)
        {
            completed = true;
            break;
        }
    }
    let received: Vec<_> = std::iter::from_fn(|| client.recv_pong())
        .map(|(peer, message, _)| (peer, message))
        .collect();
    let idle = client.idle().is_some();
    drop((client, server));

    assert!(
        completed,
        "setup: the actual discovery marker did not arrive"
    );
    assert!(idle);
    assert_eq!(
        received,
        [(address, replies.remove(0)), (address, marker)],
        "unread discovery replies exceeded the configured retained byte budget"
    );
}
