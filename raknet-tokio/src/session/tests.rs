use super::*;

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
