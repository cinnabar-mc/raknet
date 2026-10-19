//! Real endpoint command owners remain bounded before their actors consume queued requests.

use crate::prelude::{RakClient, RakServer};
use raknet::prelude::constants;
use std::net::{Ipv4Addr, SocketAddr};
use std::task::Poll;
use tokio::net::UdpSocket;

#[derive(Debug)]
pub struct QueueState {
    pub(crate) queued: usize,
    pub(crate) address: Option<SocketAddr>,
}

#[tokio::test(flavor = "current_thread")]
async fn running_endpoint_command_admission_is_bounded_before_actor_progress() {
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let silent_address = silent.local_addr().unwrap();
    let mut server = RakServer::new((Ipv4Addr::LOCALHOST, 0).into(), |_| {})
        .start()
        .await
        .unwrap();

    let observed = async {
        let client = RakClient::new(|_| {})
            .start()
            .await
            .map_err(|error| format!("client startup: {error:?}"))?;
        let exercise = async {
            // The probes are first in each real FIFO; no actor can run during these sync calls.
            let server_front = server.command_queue_probe();
            let client_front = client.command_queue_probe();
            for limit in 0..=constants::MAX_RECEIVE_MESSAGES {
                let _ = server.set_max_connections(limit);
            }
            let mut pings: Vec<_> = (0..=constants::MAX_RECEIVE_MESSAGES)
                .map(|_| Box::pin(client.ping(silent_address)))
                .collect();
            let mut unexpected_success = false;
            std::future::poll_fn(|context| {
                for ping in &mut pings {
                    if let Poll::Ready(Ok(_)) = std::future::Future::poll(ping.as_mut(), context) {
                        unexpected_success = true;
                    }
                }
                Poll::Ready(())
            })
            .await;

            let server_queued = server_front
                .await
                .map_err(|_| "original server queue probe closed".to_owned())?;
            let client_queued = client_front
                .await
                .map_err(|_| "original client queue probe closed".to_owned())?;
            drop(pings);
            server
                .command_queue_probe()
                .await
                .map_err(|_| "original server drain fence closed".to_owned())?;
            client
                .command_queue_probe()
                .await
                .map_err(|_| "original client drain fence closed".to_owned())?;

            let marker = Box::<[u8]>::from(&b"healthy original endpoint after command drain"[..]);
            server
                .set_message(marker.clone())
                .map_err(|_| "fresh advertisement was not admitted".to_owned())?;
            server
                .command_queue_probe()
                .await
                .map_err(|_| "original advertisement fence closed".to_owned())?;
            let address = server_queued
                .address
                .ok_or_else(|| "original bound server endpoint unavailable".to_owned())?;
            let (response, _) = client
                .ping(address)
                .await
                .map_err(|error| format!("fresh original peer ping: {error}"))?;
            Ok::<_, String>((
                server_queued,
                client_queued,
                unexpected_success,
                marker,
                response,
            ))
        }
        .await;
        client.stop().await;
        exercise
    }
    .await;
    server.stop().await;
    drop(silent);

    let (server_queued, client_queued, unexpected_success, marker, response) =
        observed.expect("setup: original endpoint actors and fresh peer must complete");
    assert!(
        !unexpected_success,
        "the silent owned socket must not reply"
    );
    assert_eq!(response, marker);
    assert!(
        server_queued.queued <= constants::MAX_RECEIVE_MESSAGES
            && client_queued.queued <= constants::MAX_RECEIVE_MESSAGES,
        "original endpoint command admission exceeded its finite count before actor progress: \
         server={server_queued:?}, client={client_queued:?}"
    );
}
