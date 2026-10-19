//! Actual running controls preserve rejected ownership, ordered progress and full-queue stop.

use super::*;
use crate::client::RakClient;
use std::net::Ipv4Addr;

/// Observes the original bounded FIFO after all earlier controls were consumed.
async fn fence(
    server: &RakServer<RunningServer>,
) -> Result<crate::endpoint_command_tests::QueueState, String> {
    let (send, receive) = tokio::sync::oneshot::channel();
    server
        .state
        .msg_tx
        .send(RakServerMsg::InspectCommands(send))
        .await
        .map_err(|_| "original server command owner closed".to_owned())?;
    receive
        .await
        .map_err(|_| "original server fence closed".to_owned())
}

#[tokio::test(flavor = "current_thread")]
async fn full_controls_return_owned_input_and_preserve_the_last_admitted_advertisement() {
    let mut configured = RakServer::new((Ipv4Addr::LOCALHOST, 0).into(), |_| {});
    configured.set_message(Box::<[u8]>::from(&b"configured before binding"[..]));
    let mut server = configured.start().await.unwrap();
    let observed = async {
        let mut last = Vec::new();
        for index in 0..constants::MAX_RECEIVE_MESSAGES {
            let message = format!("original ordered advertisement {index}").into_bytes();
            server.set_message(message.clone())
                .map_err(|_| "valid count rejected before the original FIFO was full".to_owned())?;
            last = message;
        }
        let rejected = b"caller still owns this advertisement".to_vec();
        let original = rejected.as_ptr();
        let recovered = server.set_message(rejected);
        let owned_full = matches!(&recovered, Err(TrySendError::Full(value)) if std::ptr::eq(value.as_ptr(), original)
            && value.as_slice() == b"caller still owns this advertisement");
        let blocked_ip = Ipv4Addr::LOCALHOST.into();
        let count_full = matches!(server.set_max_connections(0), Err(TrySendError::Full(0)));
        let block_full = matches!(server.block(blocked_ip, Duration::ZERO), Err(TrySendError::Full((ip, duration)))
            if ip == blocked_ip && duration == Duration::ZERO);
        let unblock_full = matches!(server.unblock(blocked_ip), Err(TrySendError::Full(ip)) if ip == blocked_ip);
        let address = fence(&server).await?.address
            .ok_or_else(|| "original bound server address unavailable".to_owned())?;
        let client = RakClient::new(|_| {}).start().await
            .map_err(|error| format!("client startup: {error:?}"))?;
        let reply = client.ping(address).await;
        client.stop().await;
        let (body, _) = reply.map_err(|error| format!("original ordered reply: {error}"))?;
        Ok::<_, String>((owned_full, count_full, block_full, unblock_full, body, last))
    }.await;
    server.stop().await;

    let (owned_full, count_full, block_full, unblock_full, body, last) =
        observed.expect("setup: original public owners must make progress after Full");
    assert!(owned_full && count_full && block_full && unblock_full);
    assert_eq!(body.as_ref(), last.as_slice());
}

#[tokio::test(flavor = "current_thread")]
async fn full_endpoint_fifo_cannot_lose_stop_or_reopen_control_admission() {
    let mut server = RakServer::new((Ipv4Addr::LOCALHOST, 0).into(), |_| {})
        .start()
        .await
        .unwrap();
    let mut admitted = true;
    for value in 0..constants::MAX_RECEIVE_MESSAGES {
        admitted &= server.set_max_connections(value).is_ok();
    }
    let full = matches!(server.set_max_connections(0), Err(TrySendError::Full(0)));
    // This is the exact production stop owner used by both consuming stop and resource Drop.
    let stop = server.state.stop.clone();
    stop.request();
    let first = stop.deadline();
    let closed_input = b"unaccepted after original stop".to_vec();
    let original = closed_input.as_ptr();
    let closed = matches!(server.set_message(closed_input), Err(TrySendError::Closed(value))
        if std::ptr::eq(value.as_ptr(), original) && value.as_slice() == b"unaccepted after original stop");
    stop.request();
    let repeated = stop.deadline();
    server.stop().await;

    assert!(admitted && full && closed);
    assert!(first.is_some());
    assert_eq!(first, repeated);
}
