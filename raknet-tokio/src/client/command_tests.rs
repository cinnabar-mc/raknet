//! Capacity waits belong to original public requests, including their timeout and cancellation.

use super::*;
use std::task::Poll;

#[tokio::test(start_paused = true)]
async fn ping_timeout_includes_waiting_for_original_command_capacity() {
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let address = silent.local_addr().unwrap();
    let client = RakClient::new(|_| {}).start().await.unwrap();
    // This finite caller poll batch must exhaust real capacity, not its cooperative task budget.
    let mut queued: Vec<_> = (0..constants::MAX_RECEIVE_MESSAGES)
        .map(|_| Box::pin(client.ping(address)))
        .collect();
    tokio::task::unconstrained(std::future::poll_fn(|context| {
        for request in &mut queued {
            let _ = std::future::Future::poll(request.as_mut(), context);
        }
        Poll::Ready(())
    }))
    .await;
    let full = client.state.msg_tx.capacity() == 0;
    let mut waiting = Box::pin(client.ping(address));
    let pending = tokio::task::unconstrained(std::future::poll_fn(|context| {
        Poll::Ready(std::future::Future::poll(waiting.as_mut(), context).is_pending())
    }))
    .await;
    drop(queued);
    tokio::time::advance(PING_TIMEOUT).await;
    let reply = waiting.await;
    client.stop().await;
    drop(silent);

    assert!(full && pending);
    assert!(matches!(reply, Err(RakClientError::Timeout)));
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_capacity_wait_releases_before_the_same_full_owner_stops() {
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let address = silent.local_addr().unwrap();
    let client = RakClient::new(|_| {}).start().await.unwrap();
    // This finite caller poll batch must exhaust real capacity, not its cooperative task budget.
    let mut queued: Vec<_> = (0..constants::MAX_RECEIVE_MESSAGES)
        .map(|_| Box::pin(client.ping(address)))
        .collect();
    tokio::task::unconstrained(std::future::poll_fn(|context| {
        for request in &mut queued {
            let _ = std::future::Future::poll(request.as_mut(), context);
        }
        Poll::Ready(())
    }))
    .await;
    let full = client.state.msg_tx.capacity() == 0;
    let mut canceled = Box::pin(client.ping(address));
    let pending = tokio::task::unconstrained(std::future::poll_fn(|context| {
        Poll::Ready(std::future::Future::poll(canceled.as_mut(), context).is_pending())
    }))
    .await;
    drop(canceled);
    let unchanged = client.state.msg_tx.capacity() == 0;
    let mut stopped = Box::pin(client.ping(address));
    let stop_pending = tokio::task::unconstrained(std::future::poll_fn(|context| {
        Poll::Ready(std::future::Future::poll(stopped.as_mut(), context).is_pending())
    }))
    .await;
    client.state.stop.request();
    let result = stopped.await;
    drop(queued);
    client.stop().await;
    drop(silent);

    assert!(full && pending && unchanged && stop_pending);
    assert!(matches!(result, Err(RakClientError::Closed)));
}
