use super::*;

/// Exercises the public adapter against the original genuinely negotiated snapshot policy.
async fn reject_manual_flush_snapshot(snapshot: RakSessionSnapshot) {
    let client = RakClient::new(|_| {}).start().await.unwrap();
    let result = client.adopt(snapshot, Duration::ZERO).await;
    let rejected = result.is_err();
    drop(result);
    client.stop().await;
    assert!(
        rejected,
        "automatic client adopted manual flushing without a tick capability"
    );
}

#[tokio::test]
async fn automatic_client_rejects_established_manual_flush_snapshot() {
    let (_output, established, _) = crate::test_support::manual_flush_snapshots();
    reject_manual_flush_snapshot(established).await;
}

#[tokio::test]
async fn automatic_client_rejects_closing_manual_flush_snapshot() {
    let (_output, _, closing) = crate::test_support::manual_flush_snapshots();
    reject_manual_flush_snapshot(closing).await;
}

#[tokio::test]
async fn rejected_snapshot_retains_original_policy_before_time_restoration() {
    let (_output, established, closing) = crate::test_support::manual_flush_snapshots();
    let client = RakClient::new(|_| {}).start().await.unwrap();
    for original in [established, closing] {
        let uptime = original.captured_uptime_ms();
        let Err(ClientAdoptError::UnsupportedConfiguration { snapshot, cause }) =
            client.adopt(original, Duration::MAX).await
        else {
            panic!("manual policy must return its capture before invalid time restoration")
        };
        assert_eq!(cause.kind(), std::io::ErrorKind::InvalidInput);
        assert!(!snapshot.config().autoflush);
        assert_eq!(snapshot.captured_uptime_ms(), uptime);
        assert!(
            snapshot
                .restore(Instant::now().into_std(), Duration::ZERO)
                .is_ok()
        );
    }
    client.stop().await;
}

#[tokio::test(start_paused = true)]
async fn adopting_a_snapshot_preserves_time_spent_waiting_for_the_actor() {
    let now = Instant::now().into_std();
    let (original, _) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now - Duration::from_secs(5),
        Default::default(),
    );
    let snapshot = original.snapshot(now).unwrap();
    let (msg_tx, mut msg_rx) = channel(constants::MAX_RECEIVE_MESSAGES);
    let client = RakClient {
        state: RunningClient {
            handle: Some(tokio::spawn(async {})),
            msg_tx,
            stop: Arc::new(StopState::default()),
        },
    };
    let adopting =
        tokio::spawn(async move { client.adopt(snapshot, Duration::from_millis(100)).await });
    let message = msg_rx.recv().await.unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    let RakClientMsg::Adopt(session, reply) = message else {
        panic!("adoption must deliver a session to the actor");
    };
    let CoreRestoredSession::Established(session) = session else {
        panic!("established capture must preserve phase");
    };
    assert_eq!(
        session
            .snapshot(Instant::now().into_std())
            .unwrap()
            .captured_uptime_ms(),
        8100
    );
    let _ = reply.send(Err(RakClientError::Closed));
    assert!(matches!(
        adopting.await.unwrap(),
        Err(ClientAdoptError::Session(RakClientError::Closed))
    ));
}

#[tokio::test]
async fn failed_ping_identity_reaches_the_reply_without_blocking_the_next_ping() {
    let origin = Instant::now().into_std();
    let clock = RakClock::new(origin);
    let mut core = raknet::prelude::RakClient::new(RakClientConfig::default(), clock).unwrap();
    let remote = SocketAddr::from((Ipv4Addr::LOCALHOST, 19132));
    let now = origin + Duration::from_secs(1);
    let (sender, reply) = oneshot::channel();
    assert!(
        prepare_ping(
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "entropy unavailable"
            )),
            remote,
            now,
            clock,
            sender,
        )
        .is_none()
    );
    let Err(RakClientError::IdentityGeneration(error)) = reply.await.unwrap() else {
        panic!("the entropy error must reach the ping caller");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "entropy unavailable");
    assert!(
        core.poll().is_none(),
        "failed identity must not send anything"
    );

    let (sender, reply) = oneshot::channel();
    let (guid, ping) =
        prepare_ping(Ok(ClientGuid::from_raw(7)), remote, now, clock, sender).unwrap();
    core.ping(remote, guid, now).unwrap();
    assert!(
        matches!(core.poll(), Some(RakClientOutput::SocketDatagram(_, peer)) if peer == remote)
    );
    ping.sender
        .send(Ok((Box::from(*b"ok"), Duration::ZERO)))
        .unwrap();
    assert_eq!(reply.await.unwrap().unwrap().0.as_ref(), b"ok");
}

async fn fake_server(reply_after: Option<Duration>) -> SocketAddr {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = socket.local_addr().unwrap();

    tokio::spawn(async move {
        let mut buf = [0u8; 64];
        loop {
            let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                return;
            };
            let Some(delay) = reply_after else {
                continue;
            };
            if len < 9 {
                continue;
            }

            let mut pong = vec![0x1C];
            pong.extend_from_slice(&buf[1..9]);
            pong.extend_from_slice(&[0u8; 8]);
            pong.extend_from_slice(&constants::MAGIC);
            pong.extend_from_slice(&2u16.to_be_bytes());
            pong.extend_from_slice(b"hi");

            sleep(delay).await;
            let _ = socket.send_to(&pong, from).await;
        }
    });

    addr
}

#[tokio::test]
async fn ping_latency_is_the_round_trip_time() {
    let server = fake_server(Some(Duration::from_millis(50))).await;
    let client = RakClient::new(|_| {});
    let client = client.start().await.unwrap();

    let (message, latency) = client.ping(server).await.unwrap();

    assert_eq!(message.as_ref(), b"hi");
    assert!(
        latency >= Duration::from_millis(40) && latency < Duration::from_millis(500),
        "latency was {latency:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn ping_to_a_silent_server_times_out() {
    let server = fake_server(None).await;
    let client = RakClient::new(|_| {});
    let client = client.start().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(60), client.ping(server))
        .await
        .expect("ping never gave up");

    assert!(
        matches!(result, Err(RakClientError::Timeout)),
        "got {result:?}"
    );
}

#[tokio::test]
#[ignore]
async fn ping() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_test_writer()
        .compact()
        .try_init();

    let client = RakClient::new(|_| {});

    let client = client.start().await.unwrap();

    loop {
        let pong = client
            .ping("127.0.0.1:19132".parse().unwrap())
            .await
            .unwrap();
        debug!(
            "ponged in {}ms with message: {:?}",
            pong.1.as_millis(),
            String::from_utf8_lossy(&pong.0)
        );
    }
}

#[tokio::test]
async fn connect_over_ipv6_sends_open_connection_request_1() {
    let Ok(listener) = UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)).await else {
        return;
    };
    let addr = listener.local_addr().unwrap();

    let client = RakClient::new(|_| {});
    let client = client.start().await.unwrap();

    let connect = client.connect(addr);
    tokio::pin!(connect);
    let mut buf = [0u8; 2048];

    tokio::select! {
        _ = &mut connect => panic!("connect resolved without a server"),
        received = timeout(Duration::from_secs(2), listener.recv_from(&mut buf)) => {
            let (len, _) = received
                .expect("no datagram reached the IPv6 listener")
                .unwrap();
            assert!(len > 0);
            assert_eq!(buf[0], 0x05);
        }
    }
}
