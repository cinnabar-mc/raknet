//! Observes only the original running endpoint's actual pending-ping metadata.

use super::*;
use raknet::prelude::{
    RakServer as CoreServer, RakServerConfig, RakServerInput, RakServerOutput, Sans,
};
use std::task::Poll;

#[derive(Debug)]
pub struct PingState {
    pub(super) rows: usize,
    pub(super) requests: usize,
    pub(super) canceled: usize,
}

/// Fences previously admitted commands and reads the same actor-local map without changing it.
async fn inspect(client: &RakClient<RunningClient>) -> Result<PingState, String> {
    let (send, receive) = oneshot::channel();
    client
        .state
        .msg_tx
        .send(RakClientMsg::InspectPings(send))
        .await
        .map_err(|_| "original client command owner closed".to_owned())?;
    receive
        .await
        .map_err(|_| "original client inspection did not complete".to_owned())
}

/// Completes the public ping using the original server codec and this same owned socket.
async fn complete_ping(
    client: &RakClient<RunningClient>,
    socket: &UdpSocket,
    message: Box<[u8]>,
) -> Result<Box<[u8]>, String> {
    let address = socket.local_addr().map_err(|error| error.to_string())?;
    let now = Instant::now().into_std();
    let mut server = CoreServer::new(
        RakServerConfig {
            message,
            ..Default::default()
        },
        address,
        RakClock::new(now),
        now,
    )
    .map_err(|error| error.to_string())?;
    let ping = client.ping(address);
    tokio::pin!(ping);
    let mut buffer = vec![0; constants::MAX_MTU_SIZE as usize];
    loop {
        tokio::select! {
            result = &mut ping => return result.map(|(body, _)| body)
                .map_err(|error| format!("healthy original ping: {error}")),
            received = socket.recv_from(&mut buffer) => {
                let (length, source) = received.map_err(|error| error.to_string())?;
                server.handle(RakServerInput::Datagram(
                    buffer[..length].into(), source, Instant::now().into_std(),
                )).map_err(|error| error.to_string())?;
                while let Some(output) = server.poll() {
                    if let RakServerOutput::SocketDatagram(body, destination) = output {
                        socket.send_to(&body, destination).await
                            .map_err(|error| error.to_string())?;
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pending_ping_metadata_is_bounded_and_canceled_addresses_are_reaped() {
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let other_silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let healthy = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let first_address = silent.local_addr().unwrap();
    let second_address = other_silent.local_addr().unwrap();
    let client = RakClient::new(|_| {}).start().await.unwrap();
    let marker = Box::<[u8]>::from(&b"original healthy ping after cancellation"[..]);

    let observed = async {
        let mut requests: Vec<_> = (0..=constants::MAX_RECEIVE_MESSAGES)
            .map(|_| Box::pin(client.ping(first_address)))
            .collect();
        requests.push(Box::pin(client.ping(second_address)));
        let mut unexpected_success = false;
        // These are caller-owned waits. The actual endpoint actor has not been yielded to yet.
        std::future::poll_fn(|context| {
            for request in &mut requests {
                if let Poll::Ready(Ok(_)) = std::future::Future::poll(request.as_mut(), context) {
                    unexpected_success = true;
                }
            }
            Poll::Ready(())
        })
        .await;
        let admitted = inspect(&client).await?;
        drop(requests);
        let canceled = inspect(&client).await?;
        let response = complete_ping(&client, &healthy, marker.clone()).await?;
        let after_healthy = inspect(&client).await?;
        Ok::<_, String>((
            unexpected_success,
            admitted,
            canceled,
            response,
            after_healthy,
        ))
    }
    .await;

    // Complete the original owned actor before any behavioral verdict; no stop future is canceled.
    client.stop().await;
    drop((silent, other_silent, healthy));

    let (unexpected_success, admitted, canceled, response, after_healthy) =
        observed.expect("setup: original running client and healthy peer must complete");
    assert!(
        !unexpected_success,
        "silent owned sockets must not answer a ping"
    );
    assert_eq!(response, marker);
    assert!(
        admitted.requests <= constants::MAX_RECEIVE_MESSAGES
            && after_healthy.rows == 0
            && after_healthy.requests == 0
            && after_healthy.canceled == 0,
        "endpoint retained unbounded or canceled ping metadata after commands drained: \
         admitted={admitted:?}, canceled={canceled:?}, after healthy={after_healthy:?}"
    );
}
