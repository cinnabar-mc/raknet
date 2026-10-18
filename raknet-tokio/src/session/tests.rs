use super::*;
use std::time::Duration;

#[tokio::test]
async fn max_message_len_comes_from_the_session_config() {
    let now = Instant::now().into_std();
    let (session, _) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        RakSessionConfig {
            max_queued_bytes: 1 << 20,
            ..Default::default()
        },
    );
    let (datagram_tx, _datagram_rx) = unbounded_channel();
    let (disconnect_tx, _disconnect_rx) = unbounded_channel();
    let expected = session.max_message_len();

    let (session, _input_tx) = spawn_established(session, datagram_tx, disconnect_tx);

    assert_eq!(session.max_message_len(), expected);
}

#[tokio::test]
async fn a_session_whose_packets_are_never_read_is_closed() {
    let now = Instant::now().into_std();
    let (local, mut peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        Default::default(),
    );
    let expected_id = local.id();
    let (datagram_tx, _datagram_rx) = unbounded_channel();
    let (disconnect_tx, mut disconnect_rx) = unbounded_channel();
    let (_session, input_tx) = spawn_established(local, datagram_tx, disconnect_tx);

    let now = Instant::now().into_std();
    for _ in 0..SESSION_QUEUE_CAPACITY + 64 {
        peer.send(
            Box::new([0xFE, 1]),
            RakReliability::Unreliable,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
        while let Some(out) = peer.poll() {
            if let RakSessionOutput::Datagram(buf, _) = out
                && input_tx
                    .send(SessionEvent::Io(RakSessionInput::Datagram(buf, now)))
                    .await
                    .is_err()
            {
                break;
            }
        }
    }

    let closed = tokio::time::timeout(Duration::from_secs(5), disconnect_rx.recv()).await;
    assert!(matches!(closed, Ok(Some(id)) if id == expected_id));
}

/// A rejected real actor command preserves its input prefix and repeatable terminal receive.
#[tokio::test]
async fn rejected_close_after_actor_retirement_drains_prefix_and_stays_closed() {
    let now = Instant::now().into_std();
    let (local, mut peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        Default::default(),
    );
    let id = local.id();
    let (datagram_tx, _datagram_rx) = unbounded_channel();
    let (disconnect_tx, mut disconnect_rx) = unbounded_channel();
    let (session, input_tx) = spawn_established(local, datagram_tx, disconnect_tx);
    let prefix = Box::<[u8]>::from(&b"\xfeaccepted before retirement"[..]);
    peer.send(
        prefix.clone(),
        RakReliability::ReliableOrdered,
        RakPriority::Immediate,
        now,
    )
    .unwrap();
    while let Some(output) = peer.poll() {
        if let RakSessionOutput::Datagram(bytes, _) = output {
            input_tx
                .send(SessionEvent::Io(RakSessionInput::Datagram(bytes, now)))
                .await
                .unwrap();
        }
    }
    // The original actor processes queued input before channel closure aborts its core owner.
    drop(input_tx);
    tokio::time::timeout(Duration::from_secs(5), session.msg_tx.closed())
        .await
        .expect("the actual actor retires after its input closes");
    assert_eq!(disconnect_rx.recv().await, Some(id));
    let Err(failed) = session.close() else {
        panic!("a retired actor cannot accept another close command")
    };
    let (mut closing, error) = failed.into_parts();
    assert!(matches!(error, RakSessionError::Closed));
    assert_eq!(closing.recv::<Box<[u8]>>().await.unwrap(), prefix);
    for _ in 0..2 {
        assert!(matches!(
            closing.recv::<Box<[u8]>>().await,
            Err(RakSessionError::Closed)
        ));
    }
    assert_eq!(closing.finish().unwrap().id(), id);
}
